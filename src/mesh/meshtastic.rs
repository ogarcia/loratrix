//! Meshtastic backend.
//!
//! The easier of the two protocols to read: a packet says who it is from, and the
//! radio hands over its whole node database on connecting, so a node number can
//! be turned into a name without guessing. What arrives is protobuf rather than
//! text, which means a packet has to be the right kind before it is a message at
//! all — anything that is not `TEXT_MESSAGE_APP` is somebody's telemetry, position
//! or routing traffic, and none of that belongs on Matrix.
//!
//! Whether a message is a broadcast or a direct one is a matter of who it is
//! addressed to, which is why the radio's own node number has to be learnt before
//! either can be told apart.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::RwLock as SyncRwLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use meshtastic::api::{ConnectedStreamApi, StreamApi};
use meshtastic::packet::{PacketDestination, PacketReceiver, PacketRouter};
use meshtastic::protobufs;
use meshtastic::types::{MeshChannel, NodeId};
use meshtastic::utils;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Instant, sleep, sleep_until};
use tracing::{debug, info, trace, warn};

use crate::config::{NetworkConfig, NetworkKind, Transport};
use crate::mesh::{IncomingMesh, MeshBackend, MeshKind, NetworkId, Origin, PeerId, Sender};

/// Every node, rather than one of them. Meshtastic has no constant for it.
const BROADCAST_ADDR: u32 = 0xffff_ffff;

/// Firmware refuses to transmit faster than this, so it is a floor rather than a
/// preference.
const MIN_SEND_INTERVAL: Duration = Duration::from_secs(2);

/// Reconnection backoff, doubling from the first to the second.
const RECONNECT_DELAY_MIN: Duration = Duration::from_secs(1);
const RECONNECT_DELAY_MAX: Duration = Duration::from_secs(60);

/// Where the radio is and how to reach it.
#[derive(Clone, Debug)]
enum Link {
    Tcp { host: String, port: u16 },
    Serial { port: String, baud: u32 },
}

impl std::fmt::Display for Link {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Link::Tcp { host, port } => write!(f, "{host}:{port}"),
            Link::Serial { port, baud } => write!(f, "{port}@{baud}"),
        }
    }
}

/// Whether the pump wants another connection or the relay is going away.
enum Outcome {
    Reconnect,
    Stop,
}

/// The crate's send path insists on a router to hand packets to, so that a client
/// can see its own traffic echoed back. We route in [`crate::router`] from what
/// the listener gives us, so the echo is dropped and this answers only the one
/// question that has to be answered: which node we are, which the radio stamps on
/// everything we send.
struct OwnRouter(NodeId);

impl PacketRouter<(), Infallible> for OwnRouter {
    fn handle_packet_from_radio(
        &mut self,
        _packet: protobufs::FromRadio,
    ) -> Result<(), Infallible> {
        Ok(())
    }

    fn handle_mesh_packet(&mut self, _packet: protobufs::MeshPacket) -> Result<(), Infallible> {
        Ok(())
    }

    fn source_node_id(&self) -> NodeId {
        self.0
    }
}

pub struct MeshtasticBackend {
    id: NetworkId,
    link: Link,
    /// Long and short names from the radio's node database, by node number.
    /// In memory only, by design: it is rebuilt on every connection anyway.
    names: SyncRwLock<HashMap<u32, Sender>>,
    /// The radio's own node number, which is how a direct message is recognized.
    /// Zero until the radio says, which is not a valid node number.
    my_node_num: AtomicU32,
    /// The configured connection, replaced on every reconnect. Sending needs it
    /// mutably, and sends can arrive while there is none.
    api: Mutex<Option<ConnectedStreamApi>>,
    /// Earliest instant at which the next transmission may go out.
    next_send: Mutex<Option<Instant>>,
}

impl MeshtasticBackend {
    pub fn new(network: &NetworkConfig) -> Result<Self> {
        let link = match &network.transport {
            Transport::Tcp { host, .. } => Link::Tcp {
                host: host.clone(),
                port: network
                    .transport
                    .tcp_port(NetworkKind::Meshtastic)
                    .expect("a TCP transport always resolves a port"),
            },
            Transport::Serial { port, baud } => Link::Serial {
                port: port.clone(),
                baud: *baud,
            },
            Transport::Ble { .. } => {
                if cfg!(feature = "ble") {
                    bail!(
                        "network {:?} uses a BLE transport, which is not implemented yet",
                        network.name
                    );
                }
                bail!(
                    "network {:?} uses a BLE transport, but this build lacks the `ble` feature",
                    network.name
                );
            }
        };

        Ok(Self {
            id: network.name.clone(),
            link,
            names: SyncRwLock::new(HashMap::new()),
            my_node_num: AtomicU32::new(0),
            api: Mutex::new(None),
            next_send: Mutex::new(None),
        })
    }

    /// Open the link and ask the radio to configure the session, which is what
    /// makes it start reporting.
    async fn connect(&self) -> Result<(PacketReceiver, ConnectedStreamApi)> {
        let api = StreamApi::new();
        let (listener, connected) = match &self.link {
            Link::Tcp { host, port } => {
                let stream = utils::stream::build_tcp_stream(format!("{host}:{port}"))
                    .await
                    .with_context(|| format!("connecting to {}", self.link))?;
                api.connect(stream).await
            }
            Link::Serial { port, baud } => {
                let stream =
                    utils::stream::build_serial_stream(port.clone(), Some(*baud), None, None)
                        .with_context(|| format!("opening {}", self.link))?;
                api.connect(stream).await
            }
        };

        let configured = connected
            .configure(utils::generate_rand_id())
            .await
            .context("configuring the session")?;
        Ok((listener, configured))
    }

    /// Connect, then forward messages until the link drops.
    async fn pump(&self, tx: &mpsc::Sender<IncomingMesh>) -> Result<Outcome> {
        let (mut listener, api) = self.connect().await?;
        info!(network = %self.id, link = %self.link, "connected");

        // The node database arrives unasked as part of configuring, so the names
        // are in hand before the first message needs one. Cleared first, in case
        // the radio is not the one we last spoke to.
        self.forget_names();
        *self.api.lock().await = Some(api);

        let outcome = loop {
            let Some(packet) = listener.recv().await else {
                warn!(network = %self.id, "the radio closed the link");
                break Outcome::Reconnect;
            };
            if let Some(message) = self.absorb(packet)
                && tx.send(message).await.is_err()
            {
                break Outcome::Stop;
            }
        };

        // Taken back before disconnecting, so a send already waiting on the lock
        // finds nothing rather than a closed stream.
        let api = self.api.lock().await.take();
        if let Some(api) = api
            && let Err(error) = api.disconnect().await
        {
            debug!(network = %self.id, %error, "disconnecting was not clean");
        }

        Ok(outcome)
    }

    /// Take in one packet from the radio, learning from it and returning whatever
    /// part of it the router should see.
    fn absorb(&self, packet: protobufs::FromRadio) -> Option<IncomingMesh> {
        use protobufs::from_radio::PayloadVariant;

        match packet.payload_variant? {
            PayloadVariant::MyInfo(info) => {
                // Which node we are, which is the only way to tell a message
                // addressed to us from one addressed to everybody.
                self.my_node_num.store(info.my_node_num, Ordering::Relaxed);
                info!(network = %self.id, node = %PeerId::Node(info.my_node_num), "this is us");
                None
            }
            PayloadVariant::NodeInfo(node) => {
                self.remember_name(&node);
                None
            }
            PayloadVariant::ConfigCompleteId(_) => {
                let known = self.names.read().map(|names| names.len()).unwrap_or(0);
                info!(network = %self.id, nodes = known, "node database loaded");
                None
            }
            PayloadVariant::Packet(packet) => self.message(packet),
            other => {
                trace!(network = %self.id, variant = ?std::mem::discriminant(&other), "ignored");
                None
            }
        }
    }

    /// A mesh packet, if it turns out to be a text message meant for us.
    fn message(&self, packet: protobufs::MeshPacket) -> Option<IncomingMesh> {
        use protobufs::mesh_packet::PayloadVariant;

        let data = match packet.payload_variant? {
            PayloadVariant::Decoded(data) => data,
            // A channel we hold no key for. Nothing to relay, and nothing wrong.
            PayloadVariant::Encrypted(_) => {
                trace!(network = %self.id, from = packet.from, "encrypted, no key");
                return None;
            }
        };

        if data.portnum != protobufs::PortNum::TextMessageApp as i32 {
            trace!(network = %self.id, portnum = data.portnum, "not a text message");
            return None;
        }

        // Text on this port is UTF-8 by convention. A packet that is not is
        // corrupt or from something that is not a chat client, and guessing at
        // the bytes would put mojibake on Matrix.
        let text = match String::from_utf8(data.payload) {
            Ok(text) if !text.is_empty() => text,
            Ok(_) => return None,
            Err(error) => {
                debug!(network = %self.id, from = packet.from, %error, "text was not utf-8");
                return None;
            }
        };

        let origin = if packet.to == BROADCAST_ADDR {
            Origin::Channel(u8::try_from(packet.channel).ok()?)
        } else if packet.to == self.my_node_num.load(Ordering::Relaxed) {
            Origin::Direct(PeerId::Node(packet.from))
        } else {
            // Somebody else's private conversation, which this radio only saw in
            // passing. Not ours to repeat.
            trace!(network = %self.id, to = packet.to, "addressed to another node");
            return None;
        };

        Some(IncomingMesh {
            network: self.id.clone(),
            origin,
            sender: Some(self.sender_of(packet.from)),
            text,
        })
    }

    /// What we know about a node, which may be no more than its number.
    fn sender_of(&self, num: u32) -> Sender {
        self.names
            .read()
            .ok()
            .and_then(|names| names.get(&num).cloned())
            .unwrap_or(Sender {
                id: Some(PeerId::Node(num)),
                long_name: None,
                short_name: None,
            })
    }

    fn remember_name(&self, node: &protobufs::NodeInfo) {
        let Some(user) = node.user.as_ref() else {
            return;
        };
        let sender = Sender {
            id: Some(PeerId::Node(node.num)),
            long_name: Some(user.long_name.clone()),
            short_name: Some(user.short_name.clone()),
        };
        match self.names.write() {
            Ok(mut names) => {
                names.insert(node.num, sender);
            }
            Err(_) => warn!(network = %self.id, "the name cache lock is poisoned"),
        }
    }

    fn forget_names(&self) {
        match self.names.write() {
            Ok(mut names) => names.clear(),
            Err(_) => warn!(network = %self.id, "the name cache lock is poisoned"),
        }
    }

    /// Wait until this network's turn to transmit comes round. Done before
    /// reaching for the connection, so the wait is not spent holding it.
    async fn pace(&self) {
        let mut next_send = self.next_send.lock().await;
        if let Some(at) = *next_send {
            sleep_until(at).await;
        }
        *next_send = Some(Instant::now() + MIN_SEND_INTERVAL);
    }

    /// Hand text to the radio, addressed as told.
    async fn transmit(
        &self,
        destination: PacketDestination,
        channel: u8,
        text: &str,
    ) -> Result<()> {
        let channel = MeshChannel::new(u32::from(channel)).map_err(|error| {
            anyhow::anyhow!("channel {channel} is not one a radio has: {error}")
        })?;

        self.pace().await;

        let mut held = self.api.lock().await;
        let api = match held.as_mut() {
            Some(api) => api,
            None => bail!("network {:?} is not connected", self.id),
        };
        let mut router = OwnRouter(NodeId::new(self.my_node_num.load(Ordering::Relaxed)));

        // No acknowledgement asked for: a broadcast cannot be acknowledged in any
        // useful way, and for a direct message the answer would arrive long after
        // the relay had stopped caring.
        api.send_text(&mut router, text.to_owned(), destination, false, channel)
            .await
            .with_context(|| format!("sending on {}", self.id))
    }
}

#[async_trait]
impl MeshBackend for MeshtasticBackend {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> MeshKind {
        MeshKind::Meshtastic
    }

    /// Straight from the protobuf definitions, so it tracks the protocol rather
    /// than a guess.
    fn max_payload(&self) -> usize {
        protobufs::Constants::DataPayloadLen as usize
    }

    fn min_send_interval(&self) -> Duration {
        MIN_SEND_INTERVAL
    }

    async fn run(&self, tx: mpsc::Sender<IncomingMesh>) -> Result<()> {
        let mut delay = RECONNECT_DELAY_MIN;

        loop {
            match self.pump(&tx).await {
                Ok(Outcome::Stop) => return Ok(()),
                Ok(Outcome::Reconnect) => delay = RECONNECT_DELAY_MIN,
                Err(error) => {
                    warn!(
                        network = %self.id,
                        link = %self.link,
                        "{error:#}; retrying in {delay:?}"
                    );
                    sleep(delay).await;
                    delay = (delay * 2).min(RECONNECT_DELAY_MAX);
                    continue;
                }
            }
            sleep(delay).await;
        }
    }

    async fn send_channel(&self, channel: u8, text: &str) -> Result<()> {
        self.transmit(PacketDestination::Broadcast, channel, text)
            .await
    }

    async fn send_direct(&self, peer: &PeerId, text: &str) -> Result<()> {
        let PeerId::Node(num) = peer else {
            bail!("{peer} is not a Meshtastic address");
        };
        // The primary channel, which is where a direct message goes: the address
        // is what makes it private, not the channel.
        self.transmit(PacketDestination::Node(NodeId::new(*num)), 0, text)
            .await
    }

    fn resolve_name(&self, peer: &PeerId) -> Option<String> {
        let PeerId::Node(num) = peer else {
            return None;
        };
        self.names
            .read()
            .ok()?
            .get(num)
            .and_then(|sender| sender.display_name().map(str::to_owned))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn network(transport: Transport) -> NetworkConfig {
        NetworkConfig {
            name: "lora".to_owned(),
            kind: NetworkKind::Meshtastic,
            transport,
        }
    }

    fn backend(transport: Transport) -> MeshtasticBackend {
        MeshtasticBackend::new(&network(transport)).expect("valid transport")
    }

    fn offline_backend() -> MeshtasticBackend {
        backend(Transport::Tcp {
            host: "radio".to_owned(),
            port: None,
        })
    }

    /// A text message packet, shaped as the radio sends one.
    fn text_packet(from: u32, to: u32, channel: u32, text: &str) -> protobufs::MeshPacket {
        protobufs::MeshPacket {
            from,
            to,
            channel,
            payload_variant: Some(protobufs::mesh_packet::PayloadVariant::Decoded(
                protobufs::Data {
                    portnum: protobufs::PortNum::TextMessageApp as i32,
                    payload: text.as_bytes().to_vec(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    fn node_info(num: u32, long: &str, short: &str) -> protobufs::NodeInfo {
        protobufs::NodeInfo {
            num,
            user: Some(protobufs::User {
                id: format!("!{num:08x}"),
                long_name: long.to_owned(),
                short_name: short.to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn resolves_the_default_tcp_port() {
        assert_eq!(
            backend(Transport::Tcp {
                host: "192.168.1.41".to_owned(),
                port: None,
            })
            .link
            .to_string(),
            "192.168.1.41:4403"
        );
    }

    #[test]
    fn honours_an_explicit_tcp_port() {
        assert_eq!(
            backend(Transport::Tcp {
                host: "radio".to_owned(),
                port: Some(4404),
            })
            .link
            .to_string(),
            "radio:4404"
        );
    }

    #[test]
    fn takes_the_payload_budget_from_the_protocol() {
        // 233 today; the point is that it comes from the protobufs, not from us.
        assert_eq!(offline_backend().max_payload(), 233);
    }

    #[test]
    fn a_broadcast_becomes_a_channel_message() {
        let backend = offline_backend();
        backend.remember_name(&node_info(7, "Alice Cooper", "ALCO"));

        let message = backend
            .message(text_packet(7, BROADCAST_ADDR, 2, "bos dias"))
            .expect("a broadcast text message must be relayed");

        assert_eq!(message.network, "lora");
        assert_eq!(message.origin, Origin::Channel(2));
        assert_eq!(message.text, "bos dias");

        // Unlike MeshCore, the sender is on the wire and the name comes from the
        // radio's own database.
        let sender = message
            .sender
            .expect("a Meshtastic packet says who sent it");
        assert_eq!(sender.id, Some(PeerId::Node(7)));
        assert_eq!(sender.display_name(), Some("Alice Cooper"));
    }

    #[test]
    fn a_message_addressed_to_us_becomes_a_direct_one() {
        let backend = offline_backend();
        backend.my_node_num.store(42, Ordering::Relaxed);

        let message = backend
            .message(text_packet(7, 42, 0, "estas ahi?"))
            .expect("a message to us must be relayed");
        assert_eq!(message.origin, Origin::Direct(PeerId::Node(7)));
    }

    #[test]
    fn a_message_addressed_to_somebody_else_is_left_alone() {
        let backend = offline_backend();
        backend.my_node_num.store(42, Ordering::Relaxed);

        assert!(
            backend.message(text_packet(7, 99, 0, "private")).is_none(),
            "a conversation this radio only overheard is not ours to repeat"
        );
    }

    #[test]
    fn an_unknown_sender_is_carried_by_number() {
        let backend = offline_backend();
        let message = backend
            .message(text_packet(0x1a2b_3c4d, BROADCAST_ADDR, 0, "hola"))
            .expect("relayed even with nobody to name");

        let sender = message.sender.expect("the number is always there");
        assert_eq!(sender.id, Some(PeerId::Node(0x1a2b_3c4d)));
        assert_eq!(sender.display_name(), None);
    }

    #[test]
    fn only_text_messages_are_relayed() {
        let backend = offline_backend();

        let mut telemetry = text_packet(7, BROADCAST_ADDR, 0, "not really text");
        if let Some(protobufs::mesh_packet::PayloadVariant::Decoded(data)) =
            telemetry.payload_variant.as_mut()
        {
            data.portnum = protobufs::PortNum::TelemetryApp as i32;
        }
        assert!(
            backend.message(telemetry).is_none(),
            "telemetry is not chat"
        );

        let encrypted = protobufs::MeshPacket {
            from: 7,
            to: BROADCAST_ADDR,
            payload_variant: Some(protobufs::mesh_packet::PayloadVariant::Encrypted(vec![
                1, 2, 3,
            ])),
            ..Default::default()
        };
        assert!(
            backend.message(encrypted).is_none(),
            "a channel we hold no key for is nothing to relay"
        );

        let empty = text_packet(7, BROADCAST_ADDR, 0, "");
        assert!(
            backend.message(empty).is_none(),
            "an empty body is not news"
        );
    }

    #[test]
    fn refuses_text_that_is_not_utf8() {
        let backend = offline_backend();
        let mut packet = text_packet(7, BROADCAST_ADDR, 0, "");
        if let Some(protobufs::mesh_packet::PayloadVariant::Decoded(data)) =
            packet.payload_variant.as_mut()
        {
            data.payload = vec![0xff, 0xfe];
        }
        assert!(
            backend.message(packet).is_none(),
            "guessing at the bytes would put mojibake on Matrix"
        );
    }

    #[test]
    fn learns_who_it_is_and_who_else_is_out_there() {
        let backend = offline_backend();

        backend.absorb(protobufs::FromRadio {
            payload_variant: Some(protobufs::from_radio::PayloadVariant::MyInfo(
                protobufs::MyNodeInfo {
                    my_node_num: 42,
                    ..Default::default()
                },
            )),
            ..Default::default()
        });
        assert_eq!(backend.my_node_num.load(Ordering::Relaxed), 42);

        backend.absorb(protobufs::FromRadio {
            payload_variant: Some(protobufs::from_radio::PayloadVariant::NodeInfo(node_info(
                7, "Alice", "AL",
            ))),
            ..Default::default()
        });
        assert_eq!(
            backend.resolve_name(&PeerId::Node(7)),
            Some("Alice".to_owned())
        );
        assert_eq!(backend.resolve_name(&PeerId::Node(8)), None);
        assert_eq!(backend.resolve_name(&PeerId::KeyPrefix([0; 6])), None);
    }

    #[test]
    fn a_reconnect_forgets_the_last_radio_node_database() {
        let backend = offline_backend();
        backend.remember_name(&node_info(7, "Alice", "AL"));
        backend.forget_names();
        assert_eq!(backend.resolve_name(&PeerId::Node(7)), None);
    }

    #[tokio::test]
    async fn refuses_to_send_while_disconnected() {
        let error = offline_backend()
            .send_channel(0, "hola")
            .await
            .expect_err("sending without a link must fail");
        assert!(error.to_string().contains("not connected"), "{error}");
    }

    #[tokio::test]
    async fn refuses_a_channel_no_radio_has() {
        // Meshtastic has eight, numbered nought to seven.
        let error = offline_backend()
            .send_channel(9, "hola")
            .await
            .expect_err("channel nine must be refused");
        assert!(error.to_string().contains("not one a radio has"), "{error}");
    }

    #[tokio::test]
    async fn refuses_a_direct_message_to_a_meshcore_address() {
        let error = offline_backend()
            .send_direct(&PeerId::KeyPrefix([0; 6]), "hola")
            .await
            .expect_err("a key prefix is not a Meshtastic address");
        assert!(
            error.to_string().contains("not a Meshtastic address"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn paces_transmissions() {
        let backend = offline_backend();
        tokio::time::pause();
        let started = Instant::now();

        backend.pace().await;
        assert_eq!(started.elapsed(), Duration::ZERO);

        backend.pace().await;
        assert!(
            started.elapsed() >= MIN_SEND_INTERVAL,
            "the second wait must respect the interval, waited {:?}",
            started.elapsed()
        );
    }

    #[test]
    #[cfg(not(feature = "ble"))]
    fn rejects_ble_without_the_feature() {
        let error = MeshtasticBackend::new(&network(Transport::Ble {
            address: "AA:BB:CC:DD:EE:FF".to_owned(),
        }))
        .err()
        .expect("BLE must be refused in a build without the feature");
        assert!(error.to_string().contains("`ble` feature"), "{error}");
    }

    /// Hardware in the loop. Ignored because it needs a radio; point it at one and
    /// run it explicitly:
    ///
    /// ```sh
    /// MESHTRIX_MESHTASTIC_HOST=192.168.1.41 cargo test --all-features -- \
    ///     --ignored --nocapture meshtastic_radio
    /// ```
    ///
    /// It only listens. Nothing is transmitted, so running it costs no airtime.
    #[tokio::test]
    #[ignore = "needs a real Meshtastic radio; set MESHTRIX_MESHTASTIC_HOST"]
    async fn talks_to_a_real_meshtastic_radio() {
        let Some(host) = std::env::var_os("MESHTRIX_MESHTASTIC_HOST") else {
            // Skipped rather than failed: running the ignored tests with only one
            // kind of radio to hand should not look like a broken relay.
            println!("MESHTRIX_MESHTASTIC_HOST is unset, so there is no radio to talk to");
            return;
        };
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
        let backend = backend(Transport::Tcp {
            host: host.to_string_lossy().into_owned(),
            port: None,
        });

        let (tx, mut rx) = mpsc::channel(16);
        let listener = tokio::spawn(async move {
            let mut seen = 0;
            while let Some(message) = rx.recv().await {
                println!("received {message:?}");
                seen += 1;
            }
            seen
        });

        let listen = std::env::var("MESHTRIX_MESHTASTIC_LISTEN_SECS")
            .ok()
            .and_then(|secs| secs.parse().ok())
            .unwrap_or(30);
        println!("listening for {listen}s");
        match tokio::time::timeout(Duration::from_secs(listen), backend.pump(&tx)).await {
            Err(_elapsed) => {}
            Ok(Ok(Outcome::Reconnect)) => panic!("the link dropped before the test was done"),
            Ok(Ok(Outcome::Stop)) => panic!("the receiving end went away"),
            Ok(Err(error)) => panic!("{error:#}"),
        }

        let names = backend.names.read().expect("uncontended").clone();
        let sample: Vec<Option<&str>> = names.values().take(5).map(Sender::display_name).collect();
        println!("{} node(s) known, for example {sample:?}", names.len());
        println!(
            "we are {}",
            PeerId::Node(backend.my_node_num.load(Ordering::Relaxed))
        );

        drop(tx);
        let seen = listener.await.expect("the listener does not panic");
        println!("{seen} message(s) relayed while listening");

        // Learning our own node number is what proves the session was configured:
        // the radio sends it unasked, and nothing else arrives before it.
        assert_ne!(
            backend.my_node_num.load(Ordering::Relaxed),
            0,
            "the radio should have told us which node it is"
        );
    }
}
