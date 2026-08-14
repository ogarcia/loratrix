//! Lean [`MatrixSink`] over the Matrix client-server API.
//!
//! Plain HTTPS and JSON against three endpoints. This implementation cannot
//! talk to encrypted rooms; that is what the planned `e2ee` feature is for.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::json;
use tokio::sync::mpsc;
use tracing::info;

use crate::config::MatrixConfig;
use crate::matrix::{MatrixMessage, MatrixSink, RoomId};

pub struct HttpSink {
    client: reqwest::Client,
    /// Homeserver base URL without a trailing slash.
    homeserver: String,
    user_id: String,
    access_token: String,
    /// Monotonic part of the transaction id, so retries stay idempotent.
    txn_counter: AtomicU64,
    txn_epoch: u64,
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
                .build()
                .context("building the HTTP client")?,
            homeserver,
            user_id: config.user_id.clone(),
            access_token,
            txn_counter: AtomicU64::new(0),
            txn_epoch: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |since| since.as_secs()),
        })
    }

    fn next_txn_id(&self) -> String {
        let counter = self.txn_counter.fetch_add(1, Ordering::Relaxed);
        format!("meshtrix-{}-{counter}", self.txn_epoch)
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
            info!(room = %room, "joined room");
        }
        Ok(())
    }

    async fn run(&self, _tx: mpsc::Sender<MatrixMessage>) -> Result<()> {
        // Next step: long-poll /sync with a filter limited to the bridged rooms,
        // discard the events already present at the first sync, and forward
        // m.room.message events from other senders.
        bail!("the Matrix /sync loop is not implemented yet")
    }

    async fn send_text(&self, room_id: &str, body: &str) -> Result<()> {
        let url = format!(
            "{}/_matrix/client/v3/rooms/{}/send/m.room.message/{}",
            self.homeserver,
            encode_path_segment(room_id),
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
    use super::*;

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
}
