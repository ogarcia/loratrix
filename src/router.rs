//! Message routing between mesh networks and Matrix rooms.
//!
//! The routing tables are built once from config and then only read, so the
//! only mutable state is the duplicate-suppression cache.
//!
//! A room with several routes bridges those meshes to each other as well as to
//! Matrix. That fan-out happens here rather than by looping back through
//! Matrix, because the Matrix-to-mesh direction ignores the relay's own events
//! and would therefore never deliver them.
//!
//! Transmit pacing is not enforced here: each backend owns its own duty-cycle
//! and firmware limits, so the router hands messages over and lets the backend
//! decide when they hit the air.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Result, bail};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::config::{Config, RelayConfig, Route};
use crate::dedupe::{Dedupe, message_key};
use crate::matrix::{MatrixMessage, MatrixSink, RoomId};
use crate::mesh::{IncomingMesh, MeshBackend, NetworkId, Origin};
use crate::prefix::{self, Vars};

/// A DM inbox room fed by one or more networks.
struct DmTarget {
    room: RoomId,
    /// Whether to name the network in the prefix, which is only useful when the
    /// room mixes DMs from several networks.
    tag_network: bool,
}

pub struct Router {
    backends: HashMap<NetworkId, Arc<dyn MeshBackend>>,
    matrix: Arc<dyn MatrixSink>,
    relay: RelayConfig,
    /// network -> channel -> room, for the mesh-to-Matrix direction.
    inbound: HashMap<NetworkId, HashMap<u8, RoomId>>,
    /// room -> routes, for the Matrix-to-mesh direction and for fan-out.
    outbound: HashMap<RoomId, Vec<Route>>,
    /// network -> DM inbox rooms mirroring its direct messages.
    dm_inboxes: HashMap<NetworkId, Vec<DmTarget>>,
    /// Rooms that only mirror DMs and therefore never send to a mesh.
    read_only: HashSet<RoomId>,
    dedupe: Mutex<Dedupe>,
}

impl Router {
    pub fn build(
        config: &Config,
        backends: Vec<Arc<dyn MeshBackend>>,
        matrix: Arc<dyn MatrixSink>,
    ) -> Result<Self> {
        let backends: HashMap<NetworkId, Arc<dyn MeshBackend>> = backends
            .into_iter()
            .map(|backend| (backend.id().to_owned(), backend))
            .collect();

        let mut inbound: HashMap<NetworkId, HashMap<u8, RoomId>> = HashMap::new();
        let mut outbound: HashMap<RoomId, Vec<Route>> = HashMap::new();
        let mut dm_inboxes: HashMap<NetworkId, Vec<DmTarget>> = HashMap::new();
        let mut read_only = HashSet::new();

        for room in &config.rooms {
            for route in &room.routes {
                require_backend(&backends, config, &route.network)?;
                inbound
                    .entry(route.network.clone())
                    .or_default()
                    .insert(route.channel, room.id.clone());
            }
            if !room.routes.is_empty() {
                outbound.insert(room.id.clone(), room.routes.clone());
            }

            let dm_networks = room.dm_networks();
            let tag_network = dm_networks.len() > 1;
            for network in dm_networks {
                require_backend(&backends, config, network)?;
                dm_inboxes
                    .entry(network.clone())
                    .or_default()
                    .push(DmTarget {
                        room: room.id.clone(),
                        tag_network,
                    });
            }
            if room.is_dm_inbox() {
                read_only.insert(room.id.clone());
            }
        }

        Ok(Self {
            backends,
            matrix,
            relay: config.relay.clone(),
            inbound,
            outbound,
            dm_inboxes,
            read_only,
            dedupe: Mutex::new(Dedupe::new(config.relay.dedupe_window())),
        })
    }

    /// Every room the relay has to be a member of.
    pub fn rooms(&self) -> Vec<RoomId> {
        let mut rooms: Vec<RoomId> = self
            .outbound
            .keys()
            .chain(self.read_only.iter())
            .cloned()
            .collect();
        rooms.sort_unstable();
        rooms.dedup();
        rooms
    }

    /// Human-readable routing plan, for `--check`.
    pub fn plan(&self) -> String {
        let mut lines = Vec::new();
        let mut rooms: Vec<&RoomId> = self.outbound.keys().collect();
        rooms.sort_unstable();

        for room in rooms {
            let routes = &self.outbound[room];
            let bridge = if routes.len() > 1 {
                "  (bridged: messages are also forwarded between these meshes)"
            } else {
                ""
            };
            lines.push(format!("room {room}{bridge}"));
            for route in routes {
                let detail = self.backends.get(&route.network).map_or_else(
                    || "  <no backend>".to_owned(),
                    |backend| {
                        format!(
                            "{}, {} B payload, {:?} min gap",
                            backend.kind(),
                            backend.max_payload(),
                            backend.min_send_interval(),
                        )
                    },
                );
                lines.push(format!(
                    "  <-> {} channel {} [{detail}]",
                    route.network, route.channel
                ));
            }
        }

        let mut dm_networks: Vec<&NetworkId> = self.dm_inboxes.keys().collect();
        dm_networks.sort_unstable();
        for network in dm_networks {
            for target in &self.dm_inboxes[network] {
                lines.push(format!(
                    "room {} <-- {} direct messages (read-only)",
                    target.room, network
                ));
            }
        }

        lines.join("\n")
    }

    /// Handle one message received from a mesh network.
    pub async fn handle_mesh(&self, message: IncomingMesh, now: Instant) {
        let key = message_key(&message.network, message.channel(), &message.text);
        if !self.dedupe.lock().await.admit(key, now) {
            debug!(
                network = %message.network,
                "dropping duplicate or echoed mesh message"
            );
            return;
        }

        match &message.origin {
            Origin::Channel(channel) => self.relay_channel(&message, *channel, now).await,
            Origin::Direct(peer) => self.relay_direct(&message, peer).await,
        }
    }

    async fn relay_channel(&self, message: &IncomingMesh, channel: u8, now: Instant) {
        let Some(room) = self
            .inbound
            .get(&message.network)
            .and_then(|channels| channels.get(&channel))
        else {
            debug!(
                network = %message.network,
                channel,
                "no room bridged to this channel, dropping"
            );
            return;
        };

        let name = self.sender_name(message);
        let vars = Vars {
            name: &name,
            network: &message.network,
            network_tag: "",
            channel: Some(channel),
        };

        let body = format!(
            "{}{}",
            prefix::render(&self.relay.mesh_to_matrix_prefix, &vars),
            message.text
        );
        if let Err(error) = self.matrix.send_text(room, &body).await {
            warn!(room = %room, %error, "failed to relay a mesh message to Matrix");
        }

        self.fan_out(message, room, &name, now).await;
    }

    /// Forward a mesh message to the other meshes sharing its room.
    async fn fan_out(&self, message: &IncomingMesh, room: &str, name: &str, now: Instant) {
        let Some(routes) = self.outbound.get(room) else {
            return;
        };

        for route in routes {
            if route.network == message.network {
                continue;
            }
            let Some(backend) = self.backends.get(&route.network) else {
                continue;
            };

            let vars = Vars {
                name,
                network: &message.network,
                network_tag: "",
                channel: Some(route.channel),
            };
            let payload = prefix::fit(
                &prefix::render(&self.relay.mesh_to_mesh_prefix, &vars),
                &message.text,
                backend.max_payload(),
            );

            // Remember before sending: whatever comes back carrying this exact
            // text on this channel is our own echo.
            self.dedupe.lock().await.remember(
                message_key(&route.network, Some(route.channel), &payload),
                now,
            );

            if let Err(error) = backend.send_channel(route.channel, &payload).await {
                warn!(
                    network = %route.network,
                    channel = route.channel,
                    %error,
                    "failed to fan a mesh message out to another mesh"
                );
            }
        }
    }

    async fn relay_direct(&self, message: &IncomingMesh, peer: &crate::mesh::PeerId) {
        let Some(targets) = self.dm_inboxes.get(&message.network) else {
            debug!(
                network = %message.network,
                peer = %peer,
                "no DM inbox for this network, dropping"
            );
            return;
        };

        // A DM always carries identity, but fall back to the peer address if the
        // name is not in the backend's cache yet.
        let name = match self.sender_display_name(message) {
            Some(name) => name,
            None => self
                .backends
                .get(&message.network)
                .and_then(|backend| backend.resolve_name(peer))
                .unwrap_or_else(|| peer.to_string()),
        };

        for target in targets {
            let tag = if target.tag_network {
                format!("[{}]", message.network)
            } else {
                String::new()
            };
            let vars = Vars {
                name: &name,
                network: &message.network,
                network_tag: &tag,
                channel: None,
            };
            let body = format!(
                "{}{}",
                prefix::render(&self.relay.dm_prefix, &vars),
                message.text
            );
            if let Err(error) = self.matrix.send_text(&target.room, &body).await {
                warn!(room = %target.room, %error, "failed to mirror a direct message");
            }
        }
    }

    /// Handle one message observed in a bridged Matrix room.
    pub async fn handle_matrix(&self, message: MatrixMessage, now: Instant) {
        if message.sender == self.matrix.user_id() {
            return;
        }
        if self.read_only.contains(&message.room_id) {
            debug!(
                room = %message.room_id,
                "ignoring a message written in a read-only DM inbox"
            );
            return;
        }
        let Some(routes) = self.outbound.get(&message.room_id) else {
            debug!(room = %message.room_id, "message from an unbridged room, dropping");
            return;
        };

        let name = matrix_localpart(&message.sender);
        for route in routes {
            let Some(backend) = self.backends.get(&route.network) else {
                continue;
            };

            let vars = Vars {
                name,
                network: &route.network,
                network_tag: "",
                channel: Some(route.channel),
            };
            let payload = prefix::fit(
                &prefix::render(&self.relay.matrix_to_mesh_prefix, &vars),
                &message.body,
                backend.max_payload(),
            );

            self.dedupe.lock().await.remember(
                message_key(&route.network, Some(route.channel), &payload),
                now,
            );

            if let Err(error) = backend.send_channel(route.channel, &payload).await {
                warn!(
                    network = %route.network,
                    channel = route.channel,
                    %error,
                    "failed to relay a Matrix message to the mesh"
                );
            }
        }
    }

    fn sender_display_name(&self, message: &IncomingMesh) -> Option<String> {
        message
            .sender
            .as_ref()
            .and_then(|sender| sender.display_name().map(str::to_owned))
    }

    /// Sender name to put in a prefix, falling back to the configured stand-in
    /// when the protocol did not carry one.
    fn sender_name(&self, message: &IncomingMesh) -> String {
        self.sender_display_name(message)
            .unwrap_or_else(|| self.relay.unknown_sender_name.clone())
    }
}

/// Fail with an actionable message when a configured network has no backend,
/// which usually means the build lacks the corresponding feature.
fn require_backend(
    backends: &HashMap<NetworkId, Arc<dyn MeshBackend>>,
    config: &Config,
    network: &str,
) -> Result<()> {
    if backends.contains_key(network) {
        return Ok(());
    }
    let kind = config
        .networks
        .iter()
        .find(|candidate| candidate.name == network)
        .map(|candidate| candidate.kind);
    match kind {
        Some(kind) => bail!(
            "network {network:?} is {kind:?}, but this build has no backend for it; \
             rebuild with the matching cargo feature enabled"
        ),
        None => bail!("network {network:?} has no backend"),
    }
}

/// `@alice:example.org` -> `alice`. Display names would need another API call
/// and a cache; the localpart is stable and good enough for a mesh prefix.
fn matrix_localpart(user_id: &str) -> &str {
    user_id
        .strip_prefix('@')
        .unwrap_or(user_id)
        .split_once(':')
        .map_or(user_id, |(localpart, _)| localpart)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::sync::mpsc;

    use super::*;
    use crate::mesh::{MeshKind, PeerId, Sender};

    /// Records what it was asked to transmit instead of touching a radio.
    struct FakeBackend {
        id: String,
        kind: MeshKind,
        max_payload: usize,
        sent: Mutex<Vec<(u8, String)>>,
    }

    impl FakeBackend {
        fn new(id: &str, kind: MeshKind) -> Arc<Self> {
            Arc::new(Self {
                id: id.to_owned(),
                kind,
                max_payload: 200,
                sent: Mutex::new(Vec::new()),
            })
        }

        async fn sent(&self) -> Vec<(u8, String)> {
            self.sent.lock().await.clone()
        }
    }

    #[async_trait]
    impl MeshBackend for FakeBackend {
        fn id(&self) -> &str {
            &self.id
        }

        fn kind(&self) -> MeshKind {
            self.kind
        }

        fn max_payload(&self) -> usize {
            self.max_payload
        }

        fn min_send_interval(&self) -> Duration {
            Duration::from_secs(2)
        }

        async fn run(&self, _tx: mpsc::Sender<IncomingMesh>) -> Result<()> {
            Ok(())
        }

        async fn send_channel(&self, channel: u8, text: &str) -> Result<()> {
            self.sent.lock().await.push((channel, text.to_owned()));
            Ok(())
        }

        async fn send_direct(&self, _peer: &PeerId, _text: &str) -> Result<()> {
            Ok(())
        }

        fn resolve_name(&self, _peer: &PeerId) -> Option<String> {
            None
        }
    }

    struct FakeSink {
        sent: Mutex<Vec<(String, String)>>,
    }

    impl FakeSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                sent: Mutex::new(Vec::new()),
            })
        }

        async fn sent(&self) -> Vec<(String, String)> {
            self.sent.lock().await.clone()
        }
    }

    #[async_trait]
    impl MatrixSink for FakeSink {
        fn user_id(&self) -> &str {
            "@relay:example.org"
        }

        async fn join_rooms(&self, _rooms: &[RoomId]) -> Result<()> {
            Ok(())
        }

        async fn run(&self, _tx: mpsc::Sender<MatrixMessage>) -> Result<()> {
            Ok(())
        }

        async fn send_text(&self, room_id: &str, body: &str) -> Result<()> {
            self.sent
                .lock()
                .await
                .push((room_id.to_owned(), body.to_owned()));
            Ok(())
        }
    }

    fn config(rooms: &str) -> Config {
        let raw = format!(
            r#"
            [matrix]
            homeserver = "https://example.org"
            user_id = "@relay:example.org"
            access_token = "secret"

            [[networks]]
            name = "lora"
            kind = "meshtastic"
            transport = {{ type = "tcp", host = "192.168.1.41" }}

            [[networks]]
            name = "mc"
            kind = "meshcore"
            transport = {{ type = "tcp", host = "192.168.1.161" }}

            {rooms}
            "#
        );
        let config: Config = toml::from_str(&raw).expect("test config parses");
        config.validate().expect("test config is valid");
        config
    }

    const ONE_ROOM: &str = r#"
        [[rooms]]
        id = "!room:example.org"
        routes = [{ network = "lora", channel = 0 }]
    "#;

    const BRIDGED_ROOM: &str = r#"
        [[rooms]]
        id = "!room:example.org"
        routes = [
          { network = "lora", channel = 0 },
          { network = "mc", channel = 0 },
        ]
    "#;

    struct Harness {
        router: Router,
        lora: Arc<FakeBackend>,
        mc: Arc<FakeBackend>,
        sink: Arc<FakeSink>,
    }

    fn harness(rooms: &str) -> Harness {
        let lora = FakeBackend::new("lora", MeshKind::Meshtastic);
        let mc = FakeBackend::new("mc", MeshKind::MeshCore);
        let sink = FakeSink::new();
        let router = Router::build(&config(rooms), vec![lora.clone(), mc.clone()], sink.clone())
            .expect("router builds");
        Harness {
            router,
            lora,
            mc,
            sink,
        }
    }

    fn from_mesh(network: &str, channel: u8, name: Option<&str>, text: &str) -> IncomingMesh {
        IncomingMesh {
            network: network.to_owned(),
            origin: Origin::Channel(channel),
            sender: name.map(|name| Sender {
                id: Some(PeerId::Node(1)),
                long_name: Some(name.to_owned()),
                short_name: None,
            }),
            text: text.to_owned(),
        }
    }

    #[tokio::test]
    async fn relays_a_mesh_message_into_its_room() {
        let h = harness(ONE_ROOM);
        h.router
            .handle_mesh(from_mesh("lora", 0, Some("Alice"), "hola"), Instant::now())
            .await;

        assert_eq!(
            h.sink.sent().await,
            [(
                "!room:example.org".to_owned(),
                "[Alice/lora]: hola".to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn relays_a_matrix_message_to_the_mesh() {
        let h = harness(ONE_ROOM);
        h.router
            .handle_matrix(
                MatrixMessage {
                    room_id: "!room:example.org".to_owned(),
                    sender: "@bob:example.org".to_owned(),
                    body: "hola".to_owned(),
                },
                Instant::now(),
            )
            .await;

        assert_eq!(h.lora.sent().await, [(0, "bob[M]: hola".to_owned())]);
    }

    #[tokio::test]
    async fn ignores_its_own_matrix_events() {
        let h = harness(ONE_ROOM);
        h.router
            .handle_matrix(
                MatrixMessage {
                    room_id: "!room:example.org".to_owned(),
                    sender: "@relay:example.org".to_owned(),
                    body: "[Alice/lora]: hola".to_owned(),
                },
                Instant::now(),
            )
            .await;

        assert!(
            h.lora.sent().await.is_empty(),
            "the relay must not echo itself back to the mesh"
        );
    }

    /// What a real MeshCore channel message ends up looking like in Matrix, once
    /// the backend has lifted the name out of the text. Word for word what came
    /// off the Galicia channel.
    #[tokio::test]
    async fn a_meshcore_channel_message_reads_like_any_other() {
        let h = harness(ONE_ROOM);
        h.router
            .handle_mesh(
                from_mesh(
                    "lora",
                    0,
                    Some("EA1HYZ 📟"),
                    "@[Solo probando] hola buenos días",
                ),
                Instant::now(),
            )
            .await;

        assert_eq!(
            h.sink.sent().await,
            [(
                "!room:example.org".to_owned(),
                "[EA1HYZ 📟/lora]: @[Solo probando] hola buenos días".to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn uses_the_stand_in_name_when_the_protocol_carries_none() {
        // Every MeshCore channel message arrives without sender identity.
        let h = harness(ONE_ROOM);
        h.router
            .handle_mesh(from_mesh("lora", 0, None, "Alice: hola"), Instant::now())
            .await;

        assert_eq!(
            h.sink.sent().await,
            [(
                "!room:example.org".to_owned(),
                "[unknown/lora]: Alice: hola".to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn drops_duplicate_mesh_messages_inside_the_window() {
        let h = harness(ONE_ROOM);
        let now = Instant::now();
        let message = from_mesh("lora", 0, Some("Alice"), "hola");

        h.router.handle_mesh(message.clone(), now).await;
        h.router
            .handle_mesh(message, now + Duration::from_secs(1))
            .await;

        assert_eq!(
            h.sink.sent().await.len(),
            1,
            "the duplicate must be dropped"
        );
    }

    #[tokio::test]
    async fn drops_mesh_messages_from_unbridged_channels() {
        let h = harness(ONE_ROOM);
        h.router
            .handle_mesh(from_mesh("lora", 7, Some("Alice"), "hola"), Instant::now())
            .await;

        assert!(h.sink.sent().await.is_empty());
    }

    #[tokio::test]
    async fn fans_a_mesh_message_out_to_the_other_mesh_in_the_room() {
        let h = harness(BRIDGED_ROOM);
        h.router
            .handle_mesh(from_mesh("lora", 0, Some("Alice"), "hola"), Instant::now())
            .await;

        assert_eq!(
            h.sink.sent().await,
            [(
                "!room:example.org".to_owned(),
                "[Alice/lora]: hola".to_owned()
            )]
        );
        assert_eq!(
            h.mc.sent().await,
            [(0, "Alice[lora]: hola".to_owned())],
            "the other mesh must receive it with an origin prefix"
        );
        assert!(
            h.lora.sent().await.is_empty(),
            "it must not be sent back to the mesh it came from"
        );
    }

    #[tokio::test]
    async fn a_fanned_out_message_does_not_come_back() {
        let h = harness(BRIDGED_ROOM);
        let now = Instant::now();
        h.router
            .handle_mesh(from_mesh("lora", 0, Some("Alice"), "hola"), now)
            .await;

        // The second mesh echoes what we just transmitted, with no sender
        // identity, exactly as MeshCore would.
        let echo = from_mesh("mc", 0, None, "Alice[lora]: hola");
        h.router
            .handle_mesh(echo, now + Duration::from_secs(1))
            .await;

        assert_eq!(
            h.sink.sent().await.len(),
            1,
            "the echo must not be relayed a second time"
        );
        assert_eq!(
            h.lora.sent().await.len(),
            0,
            "and must not loop back to the origin mesh"
        );
    }

    #[tokio::test]
    async fn a_matrix_message_to_a_bridged_room_reaches_every_mesh_once() {
        let h = harness(BRIDGED_ROOM);
        let now = Instant::now();
        h.router
            .handle_matrix(
                MatrixMessage {
                    room_id: "!room:example.org".to_owned(),
                    sender: "@bob:example.org".to_owned(),
                    body: "hola".to_owned(),
                },
                now,
            )
            .await;

        assert_eq!(h.lora.sent().await, [(0, "bob[M]: hola".to_owned())]);
        assert_eq!(h.mc.sent().await, [(0, "bob[M]: hola".to_owned())]);

        // And the mesh echo of what we just sent is suppressed.
        h.router
            .handle_mesh(
                from_mesh("mc", 0, None, "bob[M]: hola"),
                now + Duration::from_secs(1),
            )
            .await;
        assert!(h.sink.sent().await.is_empty());
    }

    const DM_ROOMS: &str = r#"
        [[rooms]]
        id = "!room:example.org"
        routes = [{ network = "lora", channel = 0 }]

        [[rooms]]
        id = "!dm:example.org"
        dm_inbox = ["lora", "mc"]

        [[rooms]]
        id = "!dm-mc:example.org"
        dm_inbox = "mc"
    "#;

    fn direct(network: &str, name: Option<&str>, text: &str) -> IncomingMesh {
        IncomingMesh {
            network: network.to_owned(),
            origin: Origin::Direct(PeerId::Node(0x1a2b_3c4d)),
            sender: name.map(|name| Sender {
                id: Some(PeerId::Node(0x1a2b_3c4d)),
                long_name: Some(name.to_owned()),
                short_name: None,
            }),
            text: text.to_owned(),
        }
    }

    #[tokio::test]
    async fn mirrors_direct_messages_into_every_listening_inbox() {
        let h = harness(DM_ROOMS);
        h.router
            .handle_mesh(direct("mc", Some("Alice"), "estas ahi?"), Instant::now())
            .await;

        let mut sent = h.sink.sent().await;
        sent.sort();
        assert_eq!(
            sent,
            [
                (
                    "!dm-mc:example.org".to_owned(),
                    "[DM] Alice: estas ahi?".to_owned()
                ),
                (
                    "!dm:example.org".to_owned(),
                    "[DM][mc] Alice: estas ahi?".to_owned()
                ),
            ],
            "the network tag appears only where the room mixes networks"
        );
    }

    #[tokio::test]
    async fn falls_back_to_the_peer_address_for_unnamed_direct_messages() {
        let h = harness(DM_ROOMS);
        h.router
            .handle_mesh(direct("mc", None, "hola"), Instant::now())
            .await;

        assert!(
            h.sink
                .sent()
                .await
                .iter()
                .all(|(_, body)| body.contains("!1a2b3c4d")),
            "an unresolvable sender must still be identified by address"
        );
    }

    #[tokio::test]
    async fn never_sends_a_message_written_in_a_dm_inbox_to_the_mesh() {
        let h = harness(DM_ROOMS);
        h.router
            .handle_matrix(
                MatrixMessage {
                    room_id: "!dm:example.org".to_owned(),
                    sender: "@bob:example.org".to_owned(),
                    body: "can you hear me?".to_owned(),
                },
                Instant::now(),
            )
            .await;

        assert!(h.lora.sent().await.is_empty());
        assert!(h.mc.sent().await.is_empty());
    }

    #[tokio::test]
    async fn drops_direct_messages_from_networks_without_an_inbox() {
        let h = harness(ONE_ROOM);
        h.router
            .handle_mesh(direct("lora", Some("Alice"), "hola"), Instant::now())
            .await;

        assert!(h.sink.sent().await.is_empty());
    }

    #[tokio::test]
    async fn truncates_payloads_to_each_backend_budget() {
        let h = harness(ONE_ROOM);
        let body = "x".repeat(500);
        h.router
            .handle_matrix(
                MatrixMessage {
                    room_id: "!room:example.org".to_owned(),
                    sender: "@bob:example.org".to_owned(),
                    body,
                },
                Instant::now(),
            )
            .await;

        let sent = h.lora.sent().await;
        assert_eq!(sent[0].1.len(), 200);
        assert!(sent[0].1.starts_with("bob[M]: "));
    }

    #[test]
    fn lists_every_room_it_must_join() {
        let h = harness(DM_ROOMS);
        assert_eq!(
            h.router.rooms(),
            ["!dm-mc:example.org", "!dm:example.org", "!room:example.org"]
        );
    }

    #[test]
    fn refuses_to_build_when_a_configured_network_has_no_backend() {
        let sink = FakeSink::new();
        let only_lora = FakeBackend::new("lora", MeshKind::Meshtastic);
        let error = Router::build(&config(BRIDGED_ROOM), vec![only_lora], sink)
            .err()
            .expect("a missing backend must be fatal");
        assert!(error.to_string().contains("cargo feature"), "{error}");
    }

    #[test]
    fn extracts_the_localpart_of_a_user_id() {
        assert_eq!(matrix_localpart("@alice:example.org"), "alice");
        assert_eq!(matrix_localpart("alice"), "alice");
    }

    #[test]
    fn describes_the_routing_plan() {
        let plan = harness(BRIDGED_ROOM).router.plan();
        assert!(plan.contains("bridged"), "{plan}");
        assert!(plan.contains("lora channel 0"), "{plan}");
        assert!(plan.contains("mc channel 0"), "{plan}");
    }
}
