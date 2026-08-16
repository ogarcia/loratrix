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
use tracing::{debug, info, warn};

use crate::config::{Config, RelayConfig, Route};
use crate::dedupe::{Dedupe, message_key};
use crate::matrix::{MatrixMessage, MatrixSink, RoomId};
use crate::mesh::{IncomingMesh, MeshBackend, MeshKind, MessageRef, NetworkId, Origin, Sender};
use crate::prefix::{self, Vars};
use crate::replies::Replies;

/// The name placeholders a template can ask for, worked out once from whatever
/// the protocol actually carried.
///
/// `long` and `short` are left empty when they were not given, which for MeshCore
/// is always the case for `short`. `name` is the one that never comes out blank:
/// the long name, else the short one, else the configured stand-in. That is the
/// whole reason for having a template per protocol — `{short}` is only worth
/// writing where there is one.
#[derive(Default)]
struct Names {
    name: String,
    long: String,
    short: String,
}

impl Names {
    /// A Matrix sender, of whom we know one name and call it all three. Fetching
    /// a display name would be another API call and a cache; the localpart is
    /// stable and short, which is what a mesh payload wants.
    fn of_matrix_user(localpart: &str) -> Self {
        Self {
            name: localpart.to_owned(),
            long: localpart.to_owned(),
            short: localpart.to_owned(),
        }
    }

    fn vars<'a>(&'a self, network: &'a str, network_tag: &'a str, channel: Option<u8>) -> Vars<'a> {
        Vars {
            name: &self.name,
            long: &self.long,
            short: &self.short,
            network,
            network_tag,
            channel,
        }
    }
}

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
    /// Which mesh message each relayed Matrix event is, so that a reply can be
    /// relayed as one.
    replies: Mutex<Replies>,
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

        let router = Self {
            backends,
            matrix,
            relay: config.relay.clone(),
            inbound,
            outbound,
            dm_inboxes,
            read_only,
            dedupe: Mutex::new(Dedupe::new(config.relay.dedupe_window())),
            replies: Mutex::new(Replies::new(config.relay.reply_window())),
        };
        router.log_prefixes();
        Ok(router)
    }

    /// Say which templates are actually going to be used, for each protocol that
    /// is configured.
    ///
    /// Resolved rather than as written, because that is where the surprises are: a
    /// per-protocol override that did not apply, or the general one winning when
    /// an override was meant to. Quoted, because a trailing space is part of a
    /// prefix and an empty template is a deliberate choice, and neither shows up
    /// otherwise.
    fn log_prefixes(&self) {
        for kind in [MeshKind::Meshtastic, MeshKind::MeshCore] {
            if !self.backends.values().any(|backend| backend.kind() == kind) {
                continue;
            }
            info!(
                protocol = %kind,
                to_matrix = ?self.relay.mesh_to_matrix(Some(kind)),
                from_matrix = ?self.relay.matrix_to_mesh(Some(kind)),
                from_other_mesh = ?self.relay.mesh_to_mesh(Some(kind)),
                direct_to_matrix = ?self.relay.dm(Some(kind)),
                "prefixes"
            );
        }
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

        let names = self.names_of(message.sender.as_ref());
        let template = self.relay.mesh_to_matrix(self.kind_of(&message.network));
        let body = format!(
            "{}{}",
            prefix::render(template, &names.vars(&message.network, "", Some(channel))),
            message.text
        );

        let event = match self.matrix.send_text(room, &body).await {
            Ok(event) => {
                info!(
                    network = %message.network,
                    channel,
                    room = %room,
                    sender = %names.name,
                    text = %message.text,
                    "mesh -> matrix"
                );
                // Now the two names for this one message are both known, so an
                // answer written in Matrix can point back at what the mesh calls
                // it.
                if let Some(reference) = message.reference.clone() {
                    self.replies
                        .lock()
                        .await
                        .remember(&event, &message.network, reference, now);
                }
                Some(event)
            }
            Err(error) => {
                warn!(room = %room, %error, "failed to relay a mesh message to Matrix");
                None
            }
        };

        self.fan_out(message, room, &names, event.as_deref(), now)
            .await;
    }

    /// Forward a mesh message to the other meshes sharing its room.
    ///
    /// `event` is what Matrix called this message, where it got that far: the
    /// copy on each other mesh is the same message, and a reply to the Matrix
    /// event has to be able to find whichever copy shares its network.
    async fn fan_out(
        &self,
        message: &IncomingMesh,
        room: &str,
        names: &Names,
        event: Option<&str>,
        now: Instant,
    ) {
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

            let template = self.relay.mesh_to_mesh(Some(backend.kind()));
            let payload = prefix::fit(
                &prefix::render(
                    template,
                    &names.vars(&message.network, "", Some(route.channel)),
                ),
                &message.text,
                backend.max_payload(),
            );

            // Remember before sending: whatever comes back carrying this exact
            // text on this channel is our own echo.
            self.dedupe.lock().await.remember(
                message_key(&route.network, Some(route.channel), &payload),
                now,
            );

            match backend.send_channel(route.channel, &payload, None).await {
                Ok(sent) => {
                    info!(
                        from = %message.network,
                        network = %route.network,
                        channel = route.channel,
                        text = %payload,
                        "mesh -> mesh"
                    );
                    if let (Some(event), Some(id)) = (event, sent) {
                        self.replies
                            .lock()
                            .await
                            .remember(event, &route.network, id, now);
                    }
                }
                Err(error) => warn!(
                    network = %route.network,
                    channel = route.channel,
                    %error,
                    "failed to fan a mesh message out to another mesh"
                ),
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

        // A DM always carries identity, but fall back to the backend's cache and
        // then to the peer address if no name came with it.
        let mut names = self.names_of(message.sender.as_ref());
        if self.sender_display_name(message).is_none() {
            names.name = self
                .backends
                .get(&message.network)
                .and_then(|backend| backend.resolve_name(peer))
                .unwrap_or_else(|| peer.to_string());
        }
        let template = self.relay.dm(self.kind_of(&message.network));

        for target in targets {
            let tag = if target.tag_network {
                format!("[{}]", message.network)
            } else {
                String::new()
            };
            let body = format!(
                "{}{}",
                prefix::render(template, &names.vars(&message.network, &tag, None)),
                message.text
            );
            match self.matrix.send_text(&target.room, &body).await {
                // Who and where, but not what. A direct message was addressed
                // to this node and to nobody else, and a log is a place text
                // outlives the room it was meant for.
                Ok(_) => info!(
                    network = %message.network,
                    room = %target.room,
                    sender = %names.name,
                    bytes = message.text.len(),
                    "direct message -> matrix"
                ),
                Err(error) => {
                    warn!(room = %target.room, %error, "failed to mirror a direct message")
                }
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

        let names = Names::of_matrix_user(matrix_localpart(&message.sender));
        for route in routes {
            let Some(backend) = self.backends.get(&route.network) else {
                continue;
            };

            // What the answered event is called on *this* mesh. A reply to
            // something we no longer remember, or that never reached this mesh,
            // goes out as an ordinary message: the answer still arrives, it just
            // arrives without its thread.
            let reply_to = match &message.reply_to {
                Some(event) => self
                    .replies
                    .lock()
                    .await
                    .message_for(event, &route.network, now),
                None => None,
            };
            if message.reply_to.is_some() && reply_to.is_none() {
                debug!(
                    room = %message.room_id,
                    network = %route.network,
                    "a reply to something this mesh does not know, sending it plainly"
                );
            }

            // Where the protocol writes the relation into the text, it is written
            // here rather than in the backend: the marker spends payload, and the
            // budget is applied a couple of lines below. Where it does, the
            // backend is then told nothing — the relation has already been said,
            // and saying it twice is how a message ends up with two of them.
            let marker = reply_to.as_ref().and_then(|to| backend.reply_marker(to));
            let body = match &marker {
                Some(marker) => format!("{marker}{}", message.body),
                None => message.body.clone(),
            };
            let in_packet = marker.is_none().then(|| reply_to.clone()).flatten();
            let template = self.relay.matrix_to_mesh(Some(backend.kind()));
            let payload = prefix::fit(
                &prefix::render(
                    template,
                    &names.vars(&route.network, "", Some(route.channel)),
                ),
                &body,
                backend.max_payload(),
            );

            self.dedupe.lock().await.remember(
                message_key(&route.network, Some(route.channel), &payload),
                now,
            );

            match backend
                .send_channel(route.channel, &payload, in_packet)
                .await
            {
                Ok(sent) => {
                    info!(
                        room = %message.room_id,
                        network = %route.network,
                        channel = route.channel,
                        sender = %names.name,
                        // The payload as it went out, prefix and truncation included,
                        // which is what a mesh client will have seen.
                        text = %payload,
                        reply_to = reply_to.as_ref().map(MessageRef::to_string),
                        "matrix -> mesh"
                    );
                    // And this message becomes something that can be replied to
                    // in turn.
                    if let Some(reference) = sent {
                        self.replies.lock().await.remember(
                            &message.event_id,
                            &route.network,
                            reference,
                            now,
                        );
                    }
                }
                Err(error) => warn!(
                    network = %route.network,
                    channel = route.channel,
                    %error,
                    "failed to relay a Matrix message to the mesh"
                ),
            }
        }
    }

    /// Pick apart what a sender gave us into the three name placeholders.
    fn names_of(&self, sender: Option<&Sender>) -> Names {
        Names {
            name: sender
                .and_then(|sender| sender.display_name().map(str::to_owned))
                .unwrap_or_else(|| self.relay.unknown_sender_name.clone()),
            long: sender
                .and_then(|sender| sender.long_name.clone())
                .unwrap_or_default(),
            short: sender
                .and_then(|sender| sender.short_name.clone())
                .unwrap_or_default(),
        }
    }

    fn sender_display_name(&self, message: &IncomingMesh) -> Option<String> {
        message
            .sender
            .as_ref()
            .and_then(|sender| sender.display_name().map(str::to_owned))
    }

    /// Which protocol a network speaks, for choosing a per-protocol template.
    fn kind_of(&self, network: &str) -> Option<MeshKind> {
        self.backends.get(network).map(|backend| backend.kind())
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
    ///
    /// It answers about threading the way the protocol it is standing in for
    /// does, because that is the part under test: a Meshtastic fake hands back
    /// packet ids counting from one, a MeshCore one reads the name back out of
    /// the text exactly as the real backend does.
    struct FakeBackend {
        id: String,
        kind: MeshKind,
        max_payload: usize,
        sent: Mutex<Vec<(u8, String, Option<MessageRef>)>>,
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

        /// What went out, without the threading, which most tests do not care
        /// about.
        async fn sent(&self) -> Vec<(u8, String)> {
            self.sent
                .lock()
                .await
                .iter()
                .map(|(channel, text, _)| (*channel, text.clone()))
                .collect()
        }

        /// What each of those was a reply to.
        async fn replied_to(&self) -> Vec<Option<MessageRef>> {
            self.sent
                .lock()
                .await
                .iter()
                .map(|(_, _, reply_to)| reply_to.clone())
                .collect()
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

        fn reply_marker(&self, to: &MessageRef) -> Option<String> {
            match (self.kind, to) {
                (MeshKind::MeshCore, MessageRef::Author(name)) => Some(format!("@[{name}] ")),
                _ => None,
            }
        }

        async fn send_channel(
            &self,
            channel: u8,
            text: &str,
            reply_to: Option<MessageRef>,
        ) -> Result<Option<MessageRef>> {
            let mut sent = self.sent.lock().await;
            sent.push((channel, text.to_owned(), reply_to));
            Ok(match self.kind {
                MeshKind::Meshtastic => Some(MessageRef::Packet(
                    u32::try_from(sent.len()).expect("few sends in a test"),
                )),
                // The name is whatever the prefix template wrote in, which is
                // what a MeshCore client would show and therefore answer.
                MeshKind::MeshCore => text
                    .split_once(": ")
                    .map(|(name, _)| MessageRef::Author(name.to_owned())),
            })
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

        /// The id this fake would have given the nth message it posted, counting
        /// from one, so a test can reply to something it made the relay say.
        fn event_id(nth: usize) -> String {
            format!("$event{nth}")
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

        async fn send_text(&self, room_id: &str, body: &str) -> Result<String> {
            let mut sent = self.sent.lock().await;
            sent.push((room_id.to_owned(), body.to_owned()));
            Ok(Self::event_id(sent.len()))
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

    const MESHCORE_ROOM: &str = r#"
        [[rooms]]
        id = "!room:example.org"
        routes = [{ network = "mc", channel = 0 }]
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

    /// The packet id every `from_mesh` message carries, which is what a reply to
    /// it has to name.
    fn mesh_packet() -> MessageRef {
        MessageRef::Packet(0xbeef)
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
            reference: Some(mesh_packet()),
        }
    }

    /// A MeshCore channel message, which points at its author rather than at a
    /// packet, because that is all its protocol says about itself.
    fn from_meshcore(network: &str, channel: u8, name: &str, text: &str) -> IncomingMesh {
        IncomingMesh {
            reference: Some(MessageRef::Author(name.to_owned())),
            ..from_mesh(network, channel, Some(name), text)
        }
    }

    fn from_matrix(room: &str, sender: &str, body: &str) -> MatrixMessage {
        MatrixMessage {
            room_id: room.to_owned(),
            sender: sender.to_owned(),
            body: body.to_owned(),
            event_id: "$typed:example.org".to_owned(),
            reply_to: None,
        }
    }

    /// The same, but answering a Matrix event.
    fn replying_to(event: &str, room: &str, sender: &str, body: &str) -> MatrixMessage {
        MatrixMessage {
            reply_to: Some(event.to_owned()),
            ..from_matrix(room, sender, body)
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
                from_matrix("!room:example.org", "@bob:example.org", "hola"),
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
                from_matrix(
                    "!room:example.org",
                    "@relay:example.org",
                    "[Alice/lora]: hola",
                ),
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

    /// The reason per-protocol templates exist: `{short}` is a Meshtastic thing,
    /// and asking for it on a MeshCore channel would leave a hole.
    #[tokio::test]
    async fn a_per_protocol_template_wins_over_the_general_one() {
        let mut config = config(BRIDGED_ROOM);
        config.relay.mesh_to_matrix_prefix = "[{name}/{network}]: ".to_owned();
        config.relay.meshtastic_to_matrix_prefix = Some("[{short}/{long}]: ".to_owned());

        let lora = FakeBackend::new("lora", MeshKind::Meshtastic);
        let mc = FakeBackend::new("mc", MeshKind::MeshCore);
        let sink = FakeSink::new();
        let router = Router::build(&config, vec![lora.clone(), mc.clone()], sink.clone())
            .expect("router builds");

        // Meshtastic: the override, with both names.
        router
            .handle_mesh(
                IncomingMesh {
                    network: "lora".to_owned(),
                    origin: Origin::Channel(0),
                    sender: Some(Sender {
                        id: Some(PeerId::Node(7)),
                        long_name: Some("Alice Cooper".to_owned()),
                        short_name: Some("ALCO".to_owned()),
                    }),
                    text: "hola".to_owned(),
                    reference: Some(mesh_packet()),
                },
                Instant::now(),
            )
            .await;

        // MeshCore: no override set, so the general template.
        router
            .handle_mesh(
                IncomingMesh {
                    network: "mc".to_owned(),
                    origin: Origin::Channel(0),
                    sender: Some(Sender {
                        id: None,
                        long_name: Some("EA1HYZ".to_owned()),
                        short_name: None,
                    }),
                    text: "bos dias".to_owned(),
                    reference: Some(MessageRef::Author("EA1HYZ".to_owned())),
                },
                Instant::now(),
            )
            .await;

        let bodies: Vec<String> = sink
            .sent()
            .await
            .into_iter()
            .map(|(_, body)| body)
            .collect();
        assert_eq!(
            bodies,
            [
                "[ALCO/Alice Cooper]: hola".to_owned(),
                "[EA1HYZ/mc]: bos dias".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn a_per_protocol_template_applies_to_the_mesh_direction_too() {
        let mut config = config(BRIDGED_ROOM);
        config.relay.matrix_to_mesh_prefix = "{name5}[M]: ".to_owned();
        config.relay.matrix_to_meshcore_prefix = Some("{name}: ".to_owned());

        let lora = FakeBackend::new("lora", MeshKind::Meshtastic);
        let mc = FakeBackend::new("mc", MeshKind::MeshCore);
        let sink = FakeSink::new();
        let router =
            Router::build(&config, vec![lora.clone(), mc.clone()], sink).expect("router builds");

        router
            .handle_matrix(
                from_matrix("!room:example.org", "@roberto:example.org", "hola"),
                Instant::now(),
            )
            .await;

        assert_eq!(lora.sent().await, [(0, "rober[M]: hola".to_owned())]);
        assert_eq!(
            mc.sent().await,
            [(0, "roberto: hola".to_owned())],
            "MeshCore clients expect a name and a colon, so its template can say so"
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

    /// The whole point, in the direction the user tried: answer in Matrix
    /// something a Meshtastic node said, and it goes back as a reply to that
    /// packet rather than as a remark nobody can place.
    #[tokio::test]
    async fn a_matrix_reply_answers_the_packet_it_replies_to() {
        let h = harness(ONE_ROOM);
        let now = Instant::now();
        h.router
            .handle_mesh(from_mesh("lora", 0, Some("Alice"), "hola"), now)
            .await;

        h.router
            .handle_matrix(
                replying_to(
                    &FakeSink::event_id(1),
                    "!room:example.org",
                    "@bob:example.org",
                    "buenas",
                ),
                now,
            )
            .await;

        assert_eq!(h.lora.sent().await, [(0, "bob[M]: buenas".to_owned())]);
        assert_eq!(
            h.lora.replied_to().await,
            [Some(mesh_packet())],
            "the packet the mesh message arrived as is what the reply must name"
        );
    }

    /// MeshCore has no field for it, so the relation is written into the text —
    /// after the sender's name, which is where its clients put it.
    #[tokio::test]
    async fn a_matrix_reply_on_meshcore_is_written_into_the_text() {
        let mut config = config(MESHCORE_ROOM);
        config.relay.matrix_to_mesh_prefix = "{name}: ".to_owned();

        let mc = FakeBackend::new("mc", MeshKind::MeshCore);
        let sink = FakeSink::new();
        let router = Router::build(&config, vec![mc.clone()], sink).expect("router builds");

        let now = Instant::now();
        router
            .handle_mesh(from_meshcore("mc", 0, "ALICE", "Hola a todos"), now)
            .await;
        router
            .handle_matrix(
                replying_to(
                    &FakeSink::event_id(1),
                    "!room:example.org",
                    "@bob:example.org",
                    "Hola Alice",
                ),
                now,
            )
            .await;

        assert_eq!(
            mc.sent().await,
            [(0, "bob: @[ALICE] Hola Alice".to_owned())],
            "exactly what a MeshCore client writes when it answers somebody"
        );
        assert_eq!(
            mc.replied_to().await,
            [None],
            "the backend is told nothing: the marker is already in the text"
        );
    }

    /// The marker is part of the payload, so it has to be counted before the
    /// text is cut — otherwise a reply loses exactly the room the marker takes.
    #[tokio::test]
    async fn a_meshcore_reply_marker_is_charged_against_the_payload() {
        let mut config = config(MESHCORE_ROOM);
        config.relay.matrix_to_mesh_prefix = "{name}: ".to_owned();

        let mc = FakeBackend::new("mc", MeshKind::MeshCore);
        let sink = FakeSink::new();
        let router = Router::build(&config, vec![mc.clone()], sink).expect("router builds");

        let now = Instant::now();
        router
            .handle_mesh(from_meshcore("mc", 0, "ALICE", "Hola a todos"), now)
            .await;
        router
            .handle_matrix(
                replying_to(
                    &FakeSink::event_id(1),
                    "!room:example.org",
                    "@bob:example.org",
                    &"x".repeat(500),
                ),
                now,
            )
            .await;

        let sent = mc.sent().await;
        assert_eq!(sent[0].1.len(), 200, "the budget is the budget");
        assert!(sent[0].1.starts_with("bob: @[ALICE] x"));
    }

    /// A reply to something older than the window, or from before a restart, is
    /// still delivered. It just arrives as an ordinary message.
    #[tokio::test]
    async fn a_reply_to_something_forgotten_still_gets_through() {
        let h = harness(ONE_ROOM);
        h.router
            .handle_matrix(
                replying_to(
                    "$long-gone:example.org",
                    "!room:example.org",
                    "@bob:example.org",
                    "buenas",
                ),
                Instant::now(),
            )
            .await;

        assert_eq!(h.lora.sent().await, [(0, "bob[M]: buenas".to_owned())]);
        assert_eq!(h.lora.replied_to().await, [None]);
    }

    /// Answering something that was itself typed in Matrix: the relay sent that
    /// one, so it knows which packet it became.
    #[tokio::test]
    async fn a_reply_to_a_matrix_message_names_the_packet_the_relay_sent() {
        let h = harness(ONE_ROOM);
        let now = Instant::now();
        h.router
            .handle_matrix(
                from_matrix("!room:example.org", "@alice:example.org", "hola"),
                now,
            )
            .await;

        h.router
            .handle_matrix(
                replying_to(
                    "$typed:example.org",
                    "!room:example.org",
                    "@bob:example.org",
                    "buenas",
                ),
                now,
            )
            .await;

        assert_eq!(
            h.lora.replied_to().await,
            [None, Some(MessageRef::Packet(1))],
            "the second must answer the packet the first went out as"
        );
    }

    /// One Matrix event bridged to two meshes is two different packets, and each
    /// reply has to name the one on its own mesh.
    #[tokio::test]
    async fn a_reply_in_a_bridged_room_names_the_right_message_on_each_mesh() {
        let h = harness(BRIDGED_ROOM);
        let now = Instant::now();
        // Said on lora, mirrored to Matrix, and fanned out to mc.
        h.router
            .handle_mesh(from_mesh("lora", 0, Some("Alice"), "hola"), now)
            .await;

        h.router
            .handle_matrix(
                replying_to(
                    &FakeSink::event_id(1),
                    "!room:example.org",
                    "@bob:example.org",
                    "buenas",
                ),
                now,
            )
            .await;

        assert_eq!(
            h.lora.replied_to().await,
            [Some(mesh_packet())],
            "on lora, the packet it originally arrived as"
        );
        assert_eq!(
            h.mc.replied_to().await,
            [None, None],
            "on mc the relation is in the text, so neither send tells the backend"
        );
        assert_eq!(
            h.mc.sent().await[1].1,
            "bob[M]: @[Alice[lora]] buenas".to_owned(),
            "and it points at the name the fanned-out copy went out under"
        );
    }

    #[tokio::test]
    async fn a_matrix_message_to_a_bridged_room_reaches_every_mesh_once() {
        let h = harness(BRIDGED_ROOM);
        let now = Instant::now();
        h.router
            .handle_matrix(
                from_matrix("!room:example.org", "@bob:example.org", "hola"),
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
            reference: None,
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
                from_matrix("!dm:example.org", "@bob:example.org", "can you hear me?"),
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
                from_matrix("!room:example.org", "@bob:example.org", &body),
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
