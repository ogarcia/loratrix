//! Lean [`MatrixSink`] over the Matrix client-server API.
//!
//! Plain HTTPS and JSON against three endpoints: join a room, long-poll `/sync`,
//! send a message. This implementation cannot talk to encrypted rooms; that is
//! what the planned `e2ee` feature is for.
//!
//! Two details are worth knowing. Config may name a room by alias, while `/sync`
//! only ever speaks room ids, so joining is also what resolves the two and the
//! sink keeps the translation both ways — the router only ever sees the name it
//! was configured with.
//!
//! And **the sync filter is an optimization, never a guarantee**. It asks for the
//! bridged rooms only, messages only, and no history on the first request, which
//! on a homeserver that honours it saves a great deal of traffic. Not every one
//! does: conduwuit ignores `/sync` filters completely, inline or registered, and
//! answers with the last ten events of every joined room regardless. So the two
//! things that matter for correctness — starting from now rather than from
//! history, and only relaying rooms we bridge — are done here in code, and the
//! filter is sent on top of that.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{RwLock, mpsc};
use tokio::time::sleep;
use tracing::{debug, info, trace, warn};

use crate::config::MatrixConfig;
use crate::matrix::{MatrixMessage, MatrixSink, RoomId};

/// How long the server may hold a `/sync` open waiting for something to happen.
const SYNC_TIMEOUT: Duration = Duration::from_secs(30);
/// Client-side ceiling, above the server's own, so a connection that has died
/// without saying so is noticed rather than waited on forever.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Backoff between failed syncs, doubling from the first to the second.
const RETRY_DELAY_MIN: Duration = Duration::from_secs(1);
const RETRY_DELAY_MAX: Duration = Duration::from_secs(60);

/// Message types worth relaying. A notice is what other bridges speak, and an
/// emote is someone talking in the third person, which still reads as a message.
const RELAYED_MSGTYPES: [&str; 3] = ["m.text", "m.notice", "m.emote"];

/// The two directions of the same mapping, filled in as rooms are joined.
#[derive(Default)]
struct Rooms {
    /// What config called a room -> the id the server uses.
    to_server: HashMap<String, String>,
    /// And back again, for events arriving from `/sync`.
    to_config: HashMap<String, String>,
}

pub struct HttpSink {
    client: reqwest::Client,
    /// Homeserver base URL without a trailing slash.
    homeserver: String,
    user_id: String,
    access_token: String,
    /// Monotonic part of the transaction id, so retries stay idempotent.
    txn_counter: AtomicU64,
    txn_epoch: u64,
    rooms: RwLock<Rooms>,
}

impl HttpSink {
    pub fn new(config: &MatrixConfig) -> Result<Self> {
        let access_token = config.resolve_token()?;
        let homeserver = config.homeserver.trim_end_matches('/').to_owned();
        if !homeserver.starts_with("http://") && !homeserver.starts_with("https://") {
            bail!(
                "matrix.homeserver must include a scheme, got {:?}",
                config.homeserver
            );
        }

        Ok(Self {
            client: reqwest::Client::builder()
                .user_agent(concat!("meshtrix/", env!("CARGO_PKG_VERSION")))
                .timeout(REQUEST_TIMEOUT)
                .build()
                .context("building the HTTP client")?,
            homeserver,
            user_id: config.user_id.clone(),
            access_token,
            txn_counter: AtomicU64::new(0),
            txn_epoch: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |since| since.as_secs()),
            rooms: RwLock::new(Rooms::default()),
        })
    }

    fn next_txn_id(&self) -> String {
        let counter = self.txn_counter.fetch_add(1, Ordering::Relaxed);
        format!("meshtrix-{}-{counter}", self.txn_epoch)
    }

    /// The sync filter: only the bridged rooms, only messages, and none of the
    /// presence, receipts and account data that would otherwise make up most of
    /// the traffic. `history` of zero asks for a position in the stream without
    /// any of the events leading up to it.
    async fn filter(&self, history: u8) -> String {
        let known = self.rooms.read().await;
        let rooms: Vec<&String> = known.to_server.values().collect();
        json!({
            "presence": { "not_types": ["*"] },
            "account_data": { "not_types": ["*"] },
            "room": {
                "rooms": rooms,
                "ephemeral": { "not_types": ["*"] },
                "account_data": { "not_types": ["*"] },
                "state": { "not_types": ["*"] },
                "timeline": { "limit": history, "types": ["m.room.message"] },
            },
        })
        .to_string()
    }

    /// One `/sync` request.
    async fn sync(
        &self,
        since: Option<&str>,
        filter: &str,
        wait: Duration,
    ) -> Result<SyncResponse> {
        let url = format!("{}/_matrix/client/v3/sync", self.homeserver);
        let mut query: Vec<(&str, String)> = vec![
            ("timeout", wait.as_millis().to_string()),
            ("filter", filter.to_owned()),
        ];
        if let Some(since) = since {
            query.push(("since", since.to_owned()));
        }

        let response = self
            .client
            .get(&url)
            .bearer_auth(&self.access_token)
            .query(&query)
            .send()
            .await
            .context("syncing")?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("sync failed with {status}: {body}");
        }
        response.json().await.context("reading the sync response")
    }

    /// Turn a timeline event into something worth relaying.
    fn message(&self, room: &str, event: &TimelineEvent) -> Option<MatrixMessage> {
        if event.kind != "m.room.message" {
            return None;
        }
        // Our own echo. Dropped here as well as in the router, because there is
        // no sense carrying it any further.
        if event.sender == self.user_id {
            return None;
        }

        let msgtype = event.content.msgtype.as_deref()?;
        if !RELAYED_MSGTYPES.contains(&msgtype) {
            trace!(room, msgtype, "not a kind of message we relay");
            return None;
        }
        let body = event.content.body.as_deref()?;
        if body.is_empty() {
            return None;
        }

        Some(MatrixMessage {
            room_id: room.to_owned(),
            sender: event.sender.clone(),
            body: body.to_owned(),
        })
    }

    /// Room id as config named it, which is what the router's tables are keyed
    /// by, or `None` for a room we do not bridge.
    ///
    /// This is the client-side room filter. The request asks the server for these
    /// rooms and no others, but a server that ignores filters will send the lot,
    /// and none of it may reach a mesh.
    async fn configured_room(&self, room: &str) -> Option<String> {
        self.rooms.read().await.to_config.get(room).cloned()
    }

    /// Room id as the server knows it.
    async fn as_server(&self, room: &str) -> String {
        self.rooms
            .read()
            .await
            .to_server
            .get(room)
            .cloned()
            .unwrap_or_else(|| room.to_owned())
    }
}

#[async_trait]
impl MatrixSink for HttpSink {
    fn user_id(&self) -> &str {
        &self.user_id
    }

    async fn join_rooms(&self, rooms: &[RoomId]) -> Result<()> {
        for room in rooms {
            let url = format!(
                "{}/_matrix/client/v3/join/{}",
                self.homeserver,
                encode_path_segment(room)
            );
            let response = self
                .client
                .post(&url)
                .bearer_auth(&self.access_token)
                .json(&json!({}))
                .send()
                .await
                .with_context(|| format!("joining {room}"))?;

            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                bail!("joining {room} failed with {status}: {body}");
            }

            // The answer is where an alias becomes an id, which is the only form
            // /sync will ever use.
            let joined: JoinResponse = response
                .json()
                .await
                .with_context(|| format!("reading the response to joining {room}"))?;
            if joined.room_id == *room {
                info!(room = %room, "joined room");
            } else {
                info!(room = %room, id = %joined.room_id, "joined room, resolved");
            }

            let mut known = self.rooms.write().await;
            known.to_server.insert(room.clone(), joined.room_id.clone());
            known.to_config.insert(joined.room_id, room.clone());
        }
        Ok(())
    }

    async fn run(&self, tx: mpsc::Sender<MatrixMessage>) -> Result<()> {
        // A position in the stream, and nothing else kept. The filter asks for no
        // history, but a server that ignores filters answers with the tail of
        // every room, and relaying that would read yesterday's conversation out
        // to the mesh on every restart. So whatever comes back is dropped: it is
        // the discarding, not the asking, that makes this safe.
        let primed = self
            .sync(None, &self.filter(0).await, Duration::ZERO)
            .await
            .context("priming the sync stream")?;
        let ignored: usize = primed
            .rooms
            .join
            .values()
            .map(|room| room.timeline.events.len())
            .sum();
        if ignored > 0 {
            debug!(
                ignored,
                "primed; the homeserver sent history despite being asked not to"
            );
        } else {
            debug!("sync stream primed");
        }
        let mut since = primed.next_batch;

        let filter = self.filter(50).await;
        let mut delay = RETRY_DELAY_MIN;

        loop {
            let response = match self.sync(Some(&since), &filter, SYNC_TIMEOUT).await {
                Ok(response) => {
                    delay = RETRY_DELAY_MIN;
                    response
                }
                Err(error) => {
                    warn!("{error:#}; retrying in {delay:?}");
                    sleep(delay).await;
                    delay = (delay * 2).min(RETRY_DELAY_MAX);
                    continue;
                }
            };

            for (room, joined) in &response.rooms.join {
                let Some(configured) = self.configured_room(room).await else {
                    trace!(room, "not a room we bridge");
                    continue;
                };
                for event in &joined.timeline.events {
                    if let Some(message) = self.message(&configured, event)
                        && tx.send(message).await.is_err()
                    {
                        return Ok(());
                    }
                }
            }

            since = response.next_batch;
        }
    }

    async fn send_text(&self, room_id: &str, body: &str) -> Result<()> {
        let url = format!(
            "{}/_matrix/client/v3/rooms/{}/send/m.room.message/{}",
            self.homeserver,
            encode_path_segment(&self.as_server(room_id).await),
            encode_path_segment(&self.next_txn_id()),
        );
        let response = self
            .client
            .put(&url)
            .bearer_auth(&self.access_token)
            .json(&json!({ "msgtype": "m.text", "body": body }))
            .send()
            .await
            .with_context(|| format!("sending to {room_id}"))?;

        let status = response.status();
        if !status.is_success() {
            let error = response.text().await.unwrap_or_default();
            bail!("sending to {room_id} failed with {status}: {error}");
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct JoinResponse {
    room_id: String,
}

#[derive(Deserialize)]
struct SyncResponse {
    next_batch: String,
    #[serde(default)]
    rooms: SyncRooms,
}

#[derive(Default, Deserialize)]
struct SyncRooms {
    #[serde(default)]
    join: HashMap<String, JoinedRoom>,
}

#[derive(Default, Deserialize)]
struct JoinedRoom {
    #[serde(default)]
    timeline: Timeline,
}

#[derive(Default, Deserialize)]
struct Timeline {
    #[serde(default)]
    events: Vec<TimelineEvent>,
}

#[derive(Deserialize)]
struct TimelineEvent {
    #[serde(rename = "type")]
    kind: String,
    sender: String,
    #[serde(default)]
    content: EventContent,
}

#[derive(Default, Deserialize)]
struct EventContent {
    #[serde(default)]
    msgtype: Option<String>,
    #[serde(default)]
    body: Option<String>,
}

/// Percent-encode a single URL path segment.
///
/// Room ids carry `!`, `:` and `$`, none of which may travel raw in a path.
/// Everything outside the RFC 3986 unreserved set is escaped, which is stricter
/// than required and therefore always safe.
fn encode_path_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(char::from(byte));
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::Mutex as SyncMutex;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// A homeserver, near enough. It answers a queue of canned bodies in order
    /// and records what it was asked, which is all these tests need — and less
    /// than a mock HTTP crate would cost to carry.
    struct FakeHomeserver {
        addr: SocketAddr,
        requests: Arc<SyncMutex<Vec<String>>>,
    }

    impl FakeHomeserver {
        async fn start(replies: Vec<(u16, String)>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("a loopback port");
            let addr = listener.local_addr().expect("a bound address");
            let requests = Arc::new(SyncMutex::new(Vec::new()));

            let recorded = requests.clone();
            tokio::spawn(async move {
                let mut replies = replies.into_iter();
                loop {
                    let Ok((mut socket, _)) = listener.accept().await else {
                        return;
                    };

                    // Enough of HTTP to serve one request per connection: read
                    // until the headers end, then whatever body they promised.
                    let mut raw = Vec::new();
                    let mut buffer = [0u8; 4096];
                    let head_end = loop {
                        let Ok(read) = socket.read(&mut buffer).await else {
                            return;
                        };
                        if read == 0 {
                            break None;
                        }
                        raw.extend_from_slice(&buffer[..read]);
                        if let Some(at) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                            break Some(at + 4);
                        }
                    };
                    let Some(head_end) = head_end else { continue };

                    let head = String::from_utf8_lossy(&raw[..head_end]).into_owned();
                    let length: usize = head
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("content-length: ")
                                .or_else(|| line.strip_prefix("Content-Length: "))
                        })
                        .and_then(|value| value.trim().parse().ok())
                        .unwrap_or(0);
                    while raw.len() < head_end + length {
                        let Ok(read) = socket.read(&mut buffer).await else {
                            return;
                        };
                        if read == 0 {
                            break;
                        }
                        raw.extend_from_slice(&buffer[..read]);
                    }
                    recorded
                        .lock()
                        .expect("uncontended")
                        .push(String::from_utf8_lossy(&raw).into_owned());

                    let (status, body) = replies
                        .next()
                        .unwrap_or_else(|| (200, r#"{"next_batch":"idle"}"#.to_owned()));
                    let response = format!(
                        "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                }
            });

            Self { addr, requests }
        }

        fn sink(&self) -> HttpSink {
            HttpSink::new(&MatrixConfig {
                homeserver: format!("http://{}", self.addr),
                user_id: "@relay:example.org".to_owned(),
                access_token: Some("secret".to_owned()),
                access_token_file: None,
            })
            .expect("a valid config")
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().expect("uncontended").clone()
        }
    }

    fn ok(body: &str) -> (u16, String) {
        (200, body.to_owned())
    }

    #[test]
    fn encodes_room_ids_and_aliases() {
        assert_eq!(
            encode_path_segment("!abc:example.org"),
            "%21abc%3Aexample.org"
        );
        assert_eq!(
            encode_path_segment("#alias:example.org"),
            "%23alias%3Aexample.org"
        );
        assert_eq!(encode_path_segment("plain-id_1.0~x"), "plain-id_1.0~x");
    }

    #[test]
    fn encodes_non_ascii_as_utf8_bytes() {
        assert_eq!(encode_path_segment("ñ"), "%C3%B1");
    }

    fn config(homeserver: &str) -> MatrixConfig {
        MatrixConfig {
            homeserver: homeserver.to_owned(),
            user_id: "@relay:example.org".to_owned(),
            access_token: Some("secret".to_owned()),
            access_token_file: None,
        }
    }

    #[test]
    fn requires_a_scheme_on_the_homeserver_url() {
        assert!(HttpSink::new(&config("matrix.example.org")).is_err());
        assert!(HttpSink::new(&config("https://matrix.example.org")).is_ok());
    }

    #[test]
    fn strips_a_trailing_slash_from_the_homeserver_url() {
        let sink = HttpSink::new(&config("https://matrix.example.org/")).expect("valid");
        assert_eq!(sink.homeserver, "https://matrix.example.org");
    }

    #[test]
    fn transaction_ids_are_unique() {
        let sink = HttpSink::new(&config("https://matrix.example.org")).expect("valid");
        assert_ne!(sink.next_txn_id(), sink.next_txn_id());
    }

    #[tokio::test]
    async fn sends_a_message_as_m_text() {
        let server = FakeHomeserver::start(vec![ok(r#"{"event_id":"$1"}"#)]).await;
        server
            .sink()
            .send_text("!room:example.org", "hola")
            .await
            .expect("the send should succeed");

        let request = &server.requests()[0];
        assert!(
            request.starts_with(
                "PUT /_matrix/client/v3/rooms/%21room%3Aexample.org/send/m.room.message/"
            ),
            "{request}"
        );
        assert!(
            request.contains("authorization: Bearer secret"),
            "{request}"
        );
        assert!(request.contains(r#""msgtype":"m.text""#), "{request}");
        assert!(request.contains(r#""body":"hola""#), "{request}");
    }

    #[tokio::test]
    async fn reports_what_the_server_said_when_a_send_fails() {
        let server =
            FakeHomeserver::start(vec![(403, r#"{"errcode":"M_FORBIDDEN"}"#.to_owned())]).await;
        let error = server
            .sink()
            .send_text("!room:example.org", "hola")
            .await
            .expect_err("a rejected send must fail");

        let error = error.to_string();
        assert!(error.contains("403"), "{error}");
        assert!(error.contains("M_FORBIDDEN"), "{error}");
    }

    #[tokio::test]
    async fn joining_resolves_an_alias_and_remembers_it_both_ways() {
        let server = FakeHomeserver::start(vec![ok(r#"{"room_id":"!real:example.org"}"#)]).await;
        let sink = server.sink();
        sink.join_rooms(&["#alias:example.org".to_owned()])
            .await
            .expect("joining should succeed");

        // Outbound: what config called it becomes what the server calls it.
        assert_eq!(
            sink.as_server("#alias:example.org").await,
            "!real:example.org"
        );
        // Inbound: and back again, so the router recognizes its own room.
        assert_eq!(
            sink.configured_room("!real:example.org").await.as_deref(),
            Some("#alias:example.org")
        );
        assert_eq!(
            sink.configured_room("!never-joined:example.org").await,
            None
        );
    }

    #[tokio::test]
    async fn a_send_after_joining_goes_to_the_resolved_room() {
        let server = FakeHomeserver::start(vec![
            ok(r#"{"room_id":"!real:example.org"}"#),
            ok(r#"{"event_id":"$1"}"#),
        ])
        .await;
        let sink = server.sink();
        sink.join_rooms(&["#alias:example.org".to_owned()])
            .await
            .expect("joining should succeed");
        sink.send_text("#alias:example.org", "hola")
            .await
            .expect("the send should succeed");

        let request = &server.requests()[1];
        assert!(
            request.contains("/rooms/%21real%3Aexample.org/send/"),
            "the alias must not travel to the send endpoint: {request}"
        );
    }

    /// The whole loop: prime, then one sync carrying two events.
    #[tokio::test]
    async fn relays_a_message_from_a_bridged_room() {
        let timeline = r#"{
            "next_batch": "s2",
            "rooms": { "join": { "!room:example.org": { "timeline": { "events": [
                { "type": "m.room.message", "sender": "@relay:example.org",
                  "content": { "msgtype": "m.text", "body": "our own echo" } },
                { "type": "m.room.message", "sender": "@alice:example.org",
                  "content": { "msgtype": "m.text", "body": "hola" } }
            ] } } } }
        }"#;
        let server = FakeHomeserver::start(vec![
            ok(r#"{"room_id":"!room:example.org"}"#),
            ok(r#"{"next_batch":"s1"}"#),
            ok(timeline),
        ])
        .await;
        let sink = server.sink();
        sink.join_rooms(&["!room:example.org".to_owned()])
            .await
            .expect("joining should succeed");

        let (tx, mut rx) = mpsc::channel(4);
        let running = tokio::spawn(async move { sink.run(tx).await });

        let message = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a message should arrive")
            .expect("the channel stays open");
        assert_eq!(message.room_id, "!room:example.org");
        assert_eq!(message.sender, "@alice:example.org");
        assert_eq!(message.body, "hola");

        drop(rx);
        let _ = tokio::time::timeout(Duration::from_secs(5), running).await;

        // The priming request asks for a position and no history, and every
        // request after it carries the position the one before returned.
        let requests = server.requests();
        assert!(requests[1].contains("timeout=0"), "{}", requests[1]);
        assert!(requests[1].contains("limit%22%3A0"), "{}", requests[1]);
        assert!(!requests[1].contains("since="), "{}", requests[1]);
        assert!(requests[2].contains("since=s1"), "{}", requests[2]);
    }

    /// A homeserver that ignores the filter, which is not hypothetical: conduwuit
    /// does exactly this, inline filter or registered one alike. It answers the
    /// priming request with the tail of every room, and relaying that would read
    /// yesterday's conversation out to the mesh on every restart.
    #[tokio::test]
    async fn history_the_server_sends_anyway_is_not_relayed() {
        let history = r#"{
            "next_batch": "s1",
            "rooms": { "join": { "!room:example.org": { "timeline": { "events": [
                { "type": "m.room.message", "sender": "@alice:example.org",
                  "content": { "msgtype": "m.text", "body": "said yesterday" } }
            ] } } } }
        }"#;
        let live = r#"{
            "next_batch": "s2",
            "rooms": { "join": { "!room:example.org": { "timeline": { "events": [
                { "type": "m.room.message", "sender": "@alice:example.org",
                  "content": { "msgtype": "m.text", "body": "said just now" } }
            ] } } } }
        }"#;
        let server = FakeHomeserver::start(vec![
            ok(r#"{"room_id":"!room:example.org"}"#),
            ok(history),
            ok(live),
        ])
        .await;
        let sink = server.sink();
        sink.join_rooms(&["!room:example.org".to_owned()])
            .await
            .expect("joining should succeed");

        let (tx, mut rx) = mpsc::channel(4);
        tokio::spawn(async move { sink.run(tx).await });

        let message = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a message should arrive")
            .expect("the channel stays open");
        assert_eq!(
            message.body, "said just now",
            "the primed batch must be dropped whatever the server chose to send"
        );
    }

    /// The other half of not trusting the filter: it asks for the bridged rooms,
    /// and a server may send others regardless.
    #[tokio::test]
    async fn events_from_a_room_we_do_not_bridge_are_ignored() {
        let both = r#"{
            "next_batch": "s2",
            "rooms": { "join": {
                "!elsewhere:example.org": { "timeline": { "events": [
                    { "type": "m.room.message", "sender": "@alice:example.org",
                      "content": { "msgtype": "m.text", "body": "not our room" } }
                ] } },
                "!room:example.org": { "timeline": { "events": [
                    { "type": "m.room.message", "sender": "@alice:example.org",
                      "content": { "msgtype": "m.text", "body": "ours" } }
                ] } }
            } }
        }"#;
        let server = FakeHomeserver::start(vec![
            ok(r#"{"room_id":"!room:example.org"}"#),
            ok(r#"{"next_batch":"s1"}"#),
            ok(both),
        ])
        .await;
        let sink = server.sink();
        sink.join_rooms(&["!room:example.org".to_owned()])
            .await
            .expect("joining should succeed");

        let (tx, mut rx) = mpsc::channel(4);
        tokio::spawn(async move { sink.run(tx).await });

        let message = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a message should arrive")
            .expect("the channel stays open");
        assert_eq!(message.body, "ours");
        assert_eq!(message.room_id, "!room:example.org");

        // And nothing else follows it.
        assert!(
            tokio::time::timeout(Duration::from_millis(500), rx.recv())
                .await
                .is_err(),
            "the unbridged room must not be relayed"
        );
    }

    #[tokio::test]
    async fn ignores_events_that_are_not_relayable_messages() {
        let sink = HttpSink::new(&config("https://matrix.example.org")).expect("valid");

        let cases = [
            (
                r#"{"type":"m.reaction","sender":"@a:x","content":{}}"#,
                "a reaction",
            ),
            (
                r#"{"type":"m.room.message","sender":"@relay:example.org","content":{"msgtype":"m.text","body":"hi"}}"#,
                "our own message",
            ),
            (
                r#"{"type":"m.room.message","sender":"@a:x","content":{"msgtype":"m.image","body":"cat.png"}}"#,
                "an image",
            ),
            (
                r#"{"type":"m.room.message","sender":"@a:x","content":{"msgtype":"m.text","body":""}}"#,
                "an empty body",
            ),
            (
                r#"{"type":"m.room.message","sender":"@a:x","content":{}}"#,
                "a redacted message",
            ),
        ];

        for (raw, what) in cases {
            let event: TimelineEvent = serde_json::from_str(raw).expect("valid json");
            assert!(
                sink.message("!room:example.org", &event).is_none(),
                "{what} must not be relayed"
            );
        }
    }

    #[tokio::test]
    async fn relays_notices_and_emotes_as_well_as_text() {
        let sink = HttpSink::new(&config("https://matrix.example.org")).expect("valid");

        for msgtype in ["m.text", "m.notice", "m.emote"] {
            let raw = format!(
                r#"{{"type":"m.room.message","sender":"@a:x","content":{{"msgtype":"{msgtype}","body":"hola"}}}}"#
            );
            let event: TimelineEvent = serde_json::from_str(&raw).expect("valid json");
            let message = sink
                .message("!room:example.org", &event)
                .unwrap_or_else(|| panic!("{msgtype} should be relayed"));
            assert_eq!(message.body, "hola");
        }
    }

    /// The real thing, which is the only way to find out whether a homeserver
    /// agrees with our reading of the spec. Ignored because it needs credentials;
    /// point it at some and run it explicitly:
    ///
    /// ```sh
    /// MESHTRIX_MATRIX_CREDENTIALS=/tmp/credentials.json \
    /// MESHTRIX_MATRIX_ROOMS='!one,!two' \
    ///     cargo test --all-features -- --ignored --nocapture real_homeserver
    /// ```
    ///
    /// It joins and listens. Nothing is sent, so no room sees anything.
    #[tokio::test]
    #[ignore = "needs a real homeserver; set MESHTRIX_MATRIX_CREDENTIALS"]
    async fn talks_to_a_real_homeserver() {
        let Ok(path) = std::env::var("MESHTRIX_MATRIX_CREDENTIALS") else {
            panic!("set MESHTRIX_MATRIX_CREDENTIALS to a json file with a token in it");
        };
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        #[derive(Deserialize)]
        struct Credentials {
            homeserver: String,
            user_id: String,
            access_token: String,
        }
        let raw = std::fs::read_to_string(&path).expect("the credentials file should be readable");
        let credentials: Credentials =
            serde_json::from_str(&raw).expect("the credentials file should be json");

        println!("as {} on {}", credentials.user_id, credentials.homeserver);
        let sink = HttpSink::new(&MatrixConfig {
            homeserver: credentials.homeserver,
            user_id: credentials.user_id,
            access_token: Some(credentials.access_token),
            access_token_file: None,
        })
        .expect("a valid config");

        let rooms: Vec<String> = std::env::var("MESHTRIX_MATRIX_ROOMS")
            .unwrap_or_default()
            .split(',')
            .filter(|room| !room.is_empty())
            .map(str::to_owned)
            .collect();
        assert!(
            !rooms.is_empty(),
            "set MESHTRIX_MATRIX_ROOMS to a comma separated list of rooms"
        );

        sink.join_rooms(&rooms)
            .await
            .expect("the relay account should be able to join its rooms");

        // Real events, parsed by the real structs. Long-polling only proves the
        // stream works; this is what proves we read what a homeserver actually
        // sends, without waiting for somebody to say something. Still read-only:
        // history is what the room already holds.
        if let Some(history) = std::env::var("MESHTRIX_MATRIX_HISTORY")
            .ok()
            .and_then(|count| count.parse().ok())
        {
            let response = sink
                .sync(None, &sink.filter(history).await, Duration::ZERO)
                .await
                .expect("a sync asking for history should succeed");
            let mut events = 0;
            let mut relayed = 0;
            for (room, joined) in &response.rooms.join {
                let configured = sink
                    .configured_room(room)
                    .await
                    .unwrap_or_else(|| room.clone());
                for event in &joined.timeline.events {
                    events += 1;
                    match sink.message(&configured, event) {
                        Some(message) => {
                            relayed += 1;
                            println!("would relay {message:?}");
                        }
                        None => println!("would skip a {} from {}", event.kind, event.sender),
                    }
                }
            }
            println!("{events} real event(s) parsed, {relayed} of them relayable");
            assert!(
                events > 0,
                "asked for history and got nothing; either the rooms are empty \
                 or the filter is wrong"
            );
        }

        let (tx, mut rx) = mpsc::channel(32);
        let listen: u64 = std::env::var("MESHTRIX_MATRIX_LISTEN_SECS")
            .ok()
            .and_then(|secs| secs.parse().ok())
            .unwrap_or(30);
        println!("listening {listen}s");

        let running = tokio::spawn(async move { sink.run(tx).await });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(listen);
        let mut seen = 0;
        while let Ok(Some(message)) = tokio::time::timeout_at(deadline, rx.recv()).await {
            println!("received {message:?}");
            seen += 1;
        }
        running.abort();

        println!("{seen} message(s) arrived while listening");
    }

    #[tokio::test]
    async fn a_failed_sync_is_retried_rather_than_fatal() {
        let server = FakeHomeserver::start(vec![
            ok(r#"{"room_id":"!room:example.org"}"#),
            ok(r#"{"next_batch":"s1"}"#),
            (502, "bad gateway".to_owned()),
            ok(r#"{
                "next_batch": "s3",
                "rooms": { "join": { "!room:example.org": { "timeline": { "events": [
                    { "type": "m.room.message", "sender": "@alice:example.org",
                      "content": { "msgtype": "m.text", "body": "after the error" } }
                ] } } } }
            }"#),
        ])
        .await;
        let sink = server.sink();
        sink.join_rooms(&["!room:example.org".to_owned()])
            .await
            .expect("joining should succeed");

        let (tx, mut rx) = mpsc::channel(4);
        tokio::spawn(async move { sink.run(tx).await });

        let message = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("the loop should survive the error")
            .expect("the channel stays open");
        assert_eq!(message.body, "after the error");
    }
}
