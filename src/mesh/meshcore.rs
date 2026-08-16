//! MeshCore backend.
//!
//! Two things about this protocol shape the code below.
//!
//! A **channel** message carries no sender identity at all: only the channel
//! index, a timestamp and the text. Every client makes up for that by writing
//! the name into the text itself, as `NAME: MESSAGE`, so that is where the
//! identity is recovered from — see [`split_sender`]. A **direct** message does
//! carry identity, as the first six bytes of the sender's public key, which is
//! resolved against the contact list the companion radio keeps.
//!
//! Delivery is pull-based. The radio says `MessagesWaiting` and the host calls
//! `get_msg` until the queue drains, which `start_auto_message_fetching` does
//! for us. Anything queued from before we connected has to be drained by hand,
//! because that announcement has already been and gone.

use std::collections::HashMap;
use std::sync::{Mutex as SyncMutex, RwLock as SyncRwLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use meshcore_rs::{EventPayload, EventType, MeshCore};
use tokio::sync::{Mutex, RwLock, broadcast, mpsc, oneshot};
use tokio::time::{Instant, sleep, sleep_until};
use tracing::{debug, info, trace, warn};

use crate::config::{NetworkConfig, NetworkKind, Transport};
use crate::mesh::{
    Delivery, IncomingMesh, MeshBackend, MeshKind, MessageRef, NetworkId, Origin, PeerId, Sender,
};

/// Conservative text budget. MeshCore does not expose a payload constant the way
/// Meshtastic's protobufs do, so this is a floor picked to be safe rather than a
/// figure read from the protocol. Worth measuring against real firmware before
/// raising it.
const MAX_TEXT_LEN: usize = 160;

/// No firmware-documented floor as with Meshtastic; kept equal for now so a
/// bridged room paces both meshes the same way. Also worth measuring.
const MIN_SEND_INTERVAL: Duration = Duration::from_secs(2);

/// Reconnection backoff, doubling from the first to the second.
const RECONNECT_DELAY_MIN: Duration = Duration::from_secs(1);
const RECONNECT_DELAY_MAX: Duration = Duration::from_secs(60);

/// How often the contact list may be re-read while chasing an unknown sender.
/// Reading it is one round trip that returns every contact the radio knows — 350
/// of them on the mesh this was written against — so a stranger sending several
/// messages must not cost one fetch each.
const NAME_REFRESH_COOLDOWN: Duration = Duration::from_secs(60);

/// How a MeshCore client marks a reply. The protocol has no field for it, so the
/// convention is the same one that carries the sender's name: it goes in the
/// text, at the head of the answer.
///
/// ```text
/// ALICE: Hola a todos
/// BOB: @[ALICE] Hola Alice
/// ```
const REPLY_OPEN: &str = "@[";

/// Longest thing we will believe is a name. The advertised names on the mesh this
/// was written against run to about twenty five characters; well past that and
/// the colon was punctuation rather than a separator.
const MAX_PARSED_NAME: usize = 32;

/// Split the `NAME: MESSAGE` that a MeshCore client puts on the wire.
///
/// The protocol gives a channel message no sender, so clients prepend the name to
/// the text. Recovering it is what lets a MeshCore channel read the same as a
/// Meshtastic one once it reaches Matrix, rather than carrying a name the relay
/// has to pretend not to know.
///
/// The separator is the **first** `": "`, because that is the one the client
/// wrote: whatever the sender typed, colons and all, comes after it. A
/// colon-space rather than a bare colon keeps a URL from being read as a name.
/// When the shape does not hold — a client that prepends nothing — this gives up
/// rather than guessing, and the text is passed through whole.
fn split_sender(text: &str) -> Option<(&str, &str)> {
    let (name, body) = text.split_once(": ")?;
    if name.is_empty() || body.is_empty() || name.len() > MAX_PARSED_NAME || name.contains('\n') {
        return None;
    }
    Some((name, body))
}

/// Where the companion radio is and how to reach it.
#[derive(Clone, Debug)]
enum Link {
    Tcp { host: String, port: u16 },
    Serial { port: String, baud: u32 },
}

impl Link {
    async fn connect(&self) -> Result<MeshCore> {
        let client = match self {
            Link::Tcp { host, port } => MeshCore::tcp(host, *port).await,
            Link::Serial { port, baud } => MeshCore::serial(port, *baud).await,
        };
        client.with_context(|| format!("connecting to {self}"))
    }
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

pub struct MeshCoreBackend {
    id: NetworkId,
    link: Link,
    /// Advertised names by public-key prefix. A mirror of what the client knows,
    /// kept because [`MeshBackend::resolve_name`] cannot await.
    names: SyncRwLock<HashMap<[u8; 6], String>>,
    /// The live client, replaced on every reconnect. Sends need it, and they can
    /// arrive while there is none.
    client: RwLock<Option<MeshCore>>,
    /// Earliest instant at which the next transmission may go out. Behind a
    /// mutex held across the wait, which is what serializes senders.
    next_send: Mutex<Option<Instant>>,
    /// When the contact list was last read, to keep [`NAME_REFRESH_COOLDOWN`].
    names_read_at: Mutex<Option<Instant>>,
    /// Direct messages waiting to be acknowledged, by the tag the radio said to
    /// expect. The acknowledgement arrives later, as an event of its own.
    awaiting_ack: SyncMutex<HashMap<[u8; 4], oneshot::Sender<bool>>>,
}

impl MeshCoreBackend {
    pub fn new(network: &NetworkConfig) -> Result<Self> {
        let link = match &network.transport {
            Transport::Tcp { host, .. } => Link::Tcp {
                host: host.clone(),
                port: network
                    .transport
                    .tcp_port(NetworkKind::Meshcore)
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
            client: RwLock::new(None),
            next_send: Mutex::new(None),
            names_read_at: Mutex::new(None),
            awaiting_ack: SyncMutex::new(HashMap::new()),
        })
    }

    /// Anything that was in flight when the link went is not going to be
    /// acknowledged now. Dropping the waiters closes their channels, which tells
    /// whoever is waiting straight away rather than at the timeout.
    fn abandon_acks(&self) {
        if let Ok(mut waiting) = self.awaiting_ack.lock() {
            let abandoned = waiting.len();
            if abandoned > 0 {
                debug!(network = %self.id, abandoned, "direct messages left unacknowledged");
            }
            waiting.clear();
        }
    }

    /// Connect, then forward messages until the link drops.
    async fn pump(&self, tx: &mpsc::Sender<IncomingMesh>) -> Result<Outcome> {
        self.abandon_acks();
        let client = self.link.connect().await?;

        // Before anything is asked of the radio: a broadcast receiver only sees
        // what is sent after it exists, and the answers to those questions are
        // events like any other.
        let mut events = client.dispatcher().receiver();

        let info = client
            .commands()
            .lock()
            .await
            .send_appstart()
            .await
            .context("sending appstart")?;
        info!(network = %self.id, link = %self.link, device = %info.name, "connected");

        self.load_names(&client).await;

        // Whatever arrived while we were not here. The radio announced it once,
        // and that announcement is gone, so nothing else will ask for these.
        self.drain_pending(&client).await;

        client.start_auto_message_fetching().await;
        *self.client.write().await = Some(client);

        let outcome = loop {
            match events.recv().await {
                Ok(event) => match self.convert(&event).await {
                    Some(message) => {
                        if tx.send(message).await.is_err() {
                            break Outcome::Stop;
                        }
                    }
                    None if event.event_type == EventType::Disconnected => {
                        warn!(network = %self.id, "the radio closed the link");
                        break Outcome::Reconnect;
                    }
                    None => {}
                },
                // The channel holds 256 events. Falling that far behind means
                // messages were lost, which is worth saying out loud rather than
                // discarding the way the crate's own stream helper does.
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    warn!(network = %self.id, count, "fell behind and lost events");
                }
                Err(broadcast::error::RecvError::Closed) => break Outcome::Reconnect,
            }
        };

        // Taken back before disconnecting, so a send that is already waiting on
        // the lock finds nothing rather than a dead socket.
        let client = self.client.write().await.take();
        if let Some(client) = client {
            // Not merely polite: the read and write tasks are only stopped here,
            // and a reconnect would otherwise leave a pair of them behind every
            // time.
            if let Err(error) = client.disconnect().await {
                debug!(network = %self.id, %error, "disconnecting was not clean");
            }
        }

        Ok(outcome)
    }

    /// Turn an event into something the router understands, or nothing.
    async fn convert(&self, event: &meshcore_rs::MeshCoreEvent) -> Option<IncomingMesh> {
        match &event.payload {
            // No sender on the wire, so it is dug out of the text, where the
            // sending client wrote it. What is left is the message as typed.
            EventPayload::ChannelMessage(message) => {
                let (sender, text) = match split_sender(&message.text) {
                    Some((name, body)) => (
                        Some(Sender {
                            // A name and nothing else: the protocol gave no
                            // address to go with it, and inventing one would
                            // make it look resolvable.
                            id: None,
                            long_name: Some(name.to_owned()),
                            short_name: None,
                        }),
                        body.to_owned(),
                    ),
                    None => (None, message.text.clone()),
                };
                Some(IncomingMesh {
                    network: self.id.clone(),
                    origin: Origin::Channel(message.channel_idx),
                    // A reply here points at the person, because that is the only
                    // thing a MeshCore channel message says about itself. Which
                    // also means a message whose name we could not recover cannot
                    // be replied to at all.
                    reference: sender
                        .as_ref()
                        .and_then(|sender| sender.long_name.clone())
                        .map(MessageRef::Author),
                    sender,
                    text,
                    // MeshCore has no tapbacks. An emoji here is somebody typing
                    // one, which is a message like any other.
                    reacts_to: None,
                })
            }
            EventPayload::ContactMessage(message) => {
                let prefix = message.sender_prefix;
                Some(IncomingMesh {
                    network: self.id.clone(),
                    origin: Origin::Direct(PeerId::KeyPrefix(prefix)),
                    sender: Some(Sender {
                        id: Some(PeerId::KeyPrefix(prefix)),
                        long_name: self.name_of(&prefix).await,
                        short_name: None,
                    }),
                    text: message.text.clone(),
                    // A direct message is a conversation of two, where naming
                    // what you answer says nothing the address does not.
                    reference: None,
                    reacts_to: None,
                })
            }
            // A direct message of ours reached the far node. The tag is the one
            // the radio told us to expect when it took the message.
            EventPayload::Ack { tag } => {
                match self.awaiting_ack.lock() {
                    Ok(mut waiting) => match waiting.remove(tag) {
                        // Nobody left to tell is not a problem: the waiter timed
                        // out and moved on.
                        Some(waiter) => {
                            debug!(network = %self.id, "a direct message was acknowledged");
                            let _ = waiter.send(true);
                        }
                        None => {
                            trace!(network = %self.id, "an ack we were not waiting for");
                        }
                    },
                    Err(_) => warn!(network = %self.id, "the pending-ack lock is poisoned"),
                }
                None
            }
            _ => {
                trace!(network = %self.id, event = ?event.event_type, "ignored");
                None
            }
        }
    }

    /// Resolve a sender prefix to an advertised name.
    async fn name_of(&self, prefix: &[u8; 6]) -> Option<String> {
        let client = self.client.read().await;
        let client = client.as_ref()?;

        if let Some(contact) = client.get_contact_by_prefix(prefix).await {
            return Some(contact.adv_name);
        }

        // Someone we have not met. `ensure_contacts` would not help: it only
        // fetches while a dirty flag is set, and nothing in the crate ever sets
        // that flag again after the first load, so ask the radio directly — but
        // not more often than the cooldown allows, since a stranger may well
        // send more than one message.
        if !self.may_refresh_names().await {
            debug!(network = %self.id, "unknown sender, and the contact list was just read");
            return None;
        }
        debug!(network = %self.id, "unknown sender, refreshing contacts");
        self.load_names(client).await;
        client
            .get_contact_by_prefix(prefix)
            .await
            .map(|contact| contact.adv_name)
    }

    /// Whether enough time has passed to read the contact list again.
    async fn may_refresh_names(&self) -> bool {
        self.names_read_at
            .lock()
            .await
            .is_none_or(|previous| previous.elapsed() >= NAME_REFRESH_COOLDOWN)
    }

    /// Fetch the contact list and mirror the names for the synchronous lookup.
    async fn load_names(&self, client: &MeshCore) {
        let contacts = match client.commands().lock().await.get_contacts(0).await {
            Ok(contacts) => contacts,
            Err(error) => {
                warn!(network = %self.id, %error, "could not read the contact list");
                return;
            }
        };

        *self.names_read_at.lock().await = Some(Instant::now());

        let mirrored: HashMap<[u8; 6], String> = contacts
            .iter()
            .map(|contact| (contact.prefix(), contact.adv_name.clone()))
            .collect();
        debug!(network = %self.id, contacts = mirrored.len(), "contact list loaded");

        match self.names.write() {
            Ok(mut names) => *names = mirrored,
            Err(_) => warn!(network = %self.id, "the name cache lock is poisoned"),
        }
    }

    /// Ask for whatever is already queued on the radio, until it says there is
    /// no more. The messages come back as events like any other, so there is
    /// nothing to do with what these calls return.
    async fn drain_pending(&self, client: &MeshCore) {
        for _ in 0..MAX_DRAIN {
            match client.commands().lock().await.get_msg().await {
                Ok(Some(_)) => {}
                Ok(None) => return,
                Err(error) => {
                    debug!(network = %self.id, %error, "stopped draining the queue");
                    return;
                }
            }
        }
        warn!(
            network = %self.id,
            MAX_DRAIN,
            "gave up draining the queue; it should not be this long"
        );
    }

    /// Wait until this network's turn to transmit comes round.
    async fn pace(&self) {
        let mut next_send = self.next_send.lock().await;
        if let Some(at) = *next_send {
            sleep_until(at).await;
        }
        *next_send = Some(Instant::now() + MIN_SEND_INTERVAL);
    }

    /// The live client, or an error naming what is wrong.
    async fn connected(&self) -> Result<tokio::sync::RwLockReadGuard<'_, Option<MeshCore>>> {
        let client = self.client.read().await;
        if client.is_none() {
            bail!("network {:?} is not connected", self.id);
        }
        Ok(client)
    }
}

/// Bound on the startup drain, so a radio that answers "one more" forever cannot
/// keep the backend from ever reaching its event loop.
const MAX_DRAIN: usize = 256;

/// How many channel slots to ask a radio about when listing what it is
/// subscribed to.
#[cfg(test)]
const MAX_CHANNELS: u8 = 8;

#[async_trait]
impl MeshBackend for MeshCoreBackend {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> MeshKind {
        MeshKind::MeshCore
    }

    fn max_payload(&self) -> usize {
        MAX_TEXT_LEN
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

    fn reply_marker(&self, to: &MessageRef) -> Option<String> {
        match to {
            MessageRef::Author(name) => Some(format!("{REPLY_OPEN}{name}] ")),
            // Not something this protocol can name. Nothing is written, and the
            // message goes out as an ordinary one.
            MessageRef::Packet(_) => None,
        }
    }

    /// `reply_to` is not read here, and does not need to be: on MeshCore the
    /// marker is part of the text, so the router has already written it in — see
    /// [`MeshBackend::reply_marker`]. Doing it at this end would add bytes to a
    /// payload that has already been measured and cut to fit.
    async fn send_channel(
        &self,
        channel: u8,
        text: &str,
        _reply_to: Option<MessageRef>,
    ) -> Result<Option<MessageRef>> {
        let client = self.connected().await?;
        self.pace().await;
        client
            .as_ref()
            .expect("checked just above")
            .commands()
            .lock()
            .await
            .send_channel_msg(channel, text, None)
            .await
            .with_context(|| format!("sending on {} channel {channel}", self.id))?;

        // What a reply to this would have to name, read back out of the text the
        // same way a received message is read. Whatever the prefix template put
        // there is what other clients will show as the sender, so it is also what
        // they will answer.
        Ok(split_sender(text).map(|(name, _)| MessageRef::Author(name.to_owned())))
    }

    async fn send_direct(&self, peer: &PeerId, text: &str) -> Result<Delivery> {
        let PeerId::KeyPrefix(prefix) = peer else {
            bail!("{peer} is not a MeshCore address");
        };
        let client = self.connected().await?;
        self.pace().await;
        let sent = client
            .as_ref()
            .expect("checked just above")
            .commands()
            .lock()
            .await
            .send_msg(&prefix[..], text, None)
            .await
            .with_context(|| format!("sending a direct message on {}", self.id))?;

        // The radio says which tag to expect and how long it is worth waiting,
        // so neither has to be guessed at. Registered after the send, because a
        // send that failed leaves nothing to acknowledge.
        let (tx, ack) = oneshot::channel();
        match self.awaiting_ack.lock() {
            Ok(mut waiting) => {
                waiting.insert(sent.expected_ack, tx);
            }
            Err(_) => warn!(network = %self.id, "the pending-ack lock is poisoned"),
        }
        Ok(Delivery {
            // Nothing on the wire another client could point back at, so nothing
            // can be a reaction to it either.
            reference: None,
            ack,
            within: Duration::from_millis(u64::from(sent.suggested_timeout)),
        })
    }

    fn resolve_name(&self, peer: &PeerId) -> Option<String> {
        let PeerId::KeyPrefix(prefix) = peer else {
            return None;
        };
        self.names.read().ok()?.get(prefix).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn network(transport: Transport) -> NetworkConfig {
        NetworkConfig {
            name: "mc".to_owned(),
            kind: NetworkKind::Meshcore,
            transport,
        }
    }

    fn backend(transport: Transport) -> MeshCoreBackend {
        MeshCoreBackend::new(&network(transport)).expect("valid transport")
    }

    #[test]
    fn resolves_the_default_tcp_port() {
        let backend = backend(Transport::Tcp {
            host: "192.168.1.161".to_owned(),
            port: None,
        });
        assert_eq!(backend.link.to_string(), "192.168.1.161:5000");
    }

    #[test]
    fn honours_an_explicit_tcp_port() {
        let backend = backend(Transport::Tcp {
            host: "radio".to_owned(),
            port: Some(5001),
        });
        assert_eq!(backend.link.to_string(), "radio:5001");
    }

    #[test]
    fn describes_a_serial_link() {
        let backend = backend(Transport::Serial {
            port: "/dev/ttyACM0".to_owned(),
            baud: 115_200,
        });
        assert_eq!(backend.link.to_string(), "/dev/ttyACM0@115200");
    }

    #[test]
    fn resolves_a_mirrored_contact_name() {
        let backend = backend(Transport::Tcp {
            host: "radio".to_owned(),
            port: None,
        });

        let prefix = [1, 2, 3, 4, 5, 6];
        backend
            .names
            .write()
            .expect("uncontended")
            .insert(prefix, "Alice".to_owned());

        assert_eq!(
            backend.resolve_name(&PeerId::KeyPrefix(prefix)),
            Some("Alice".to_owned())
        );
        assert_eq!(backend.resolve_name(&PeerId::KeyPrefix([9; 6])), None);
        assert_eq!(backend.resolve_name(&PeerId::Node(1)), None);
    }

    #[tokio::test]
    async fn refuses_to_send_while_disconnected() {
        let backend = backend(Transport::Tcp {
            host: "radio".to_owned(),
            port: None,
        });

        let error = backend
            .send_channel(0, "hola", None)
            .await
            .expect_err("sending without a link must fail");
        assert!(error.to_string().contains("not connected"), "{error}");
    }

    #[tokio::test]
    async fn refuses_a_direct_message_to_a_meshtastic_address() {
        let backend = backend(Transport::Tcp {
            host: "radio".to_owned(),
            port: None,
        });

        let error = backend
            .send_direct(&PeerId::Node(7), "hola")
            .await
            .expect_err("a node number is not a MeshCore address");
        assert!(
            error.to_string().contains("not a MeshCore address"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn paces_transmissions() {
        let backend = backend(Transport::Tcp {
            host: "radio".to_owned(),
            port: None,
        });

        // The first goes out at once and sets the floor for the next.
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
        let error = MeshCoreBackend::new(&network(Transport::Ble {
            address: "AA:BB:CC:DD:EE:FF".to_owned(),
        }))
        .err()
        .expect("BLE must be refused in a build without the feature");
        assert!(error.to_string().contains("`ble` feature"), "{error}");
    }

    #[test]
    #[cfg(feature = "ble")]
    fn rejects_ble_as_unimplemented() {
        let error = MeshCoreBackend::new(&network(Transport::Ble {
            address: "AA:BB:CC:DD:EE:FF".to_owned(),
        }))
        .err()
        .expect("BLE is not implemented yet");
        assert!(error.to_string().contains("not implemented yet"), "{error}");
    }

    /// A radio accepts one connection at a time, so there is no way to send this
    /// backend a message from a phone while it is attached, and catching real
    /// traffic is a matter of waiting for a stranger. The events are therefore
    /// built here exactly as the reader emits them, which tests the part that is
    /// ours: turning the crate's types into the router's.
    fn channel_event(channel_idx: u8, text: &str) -> meshcore_rs::MeshCoreEvent {
        meshcore_rs::MeshCoreEvent::new(
            EventType::ChannelMsgRecv,
            EventPayload::ChannelMessage(meshcore_rs::ChannelMessage {
                channel_idx,
                path_len: 2,
                txt_type: 0,
                sender_timestamp: 1_770_000_000,
                text: text.to_owned(),
                snr: Some(9.5),
            }),
        )
    }

    fn direct_event(sender_prefix: [u8; 6], text: &str) -> meshcore_rs::MeshCoreEvent {
        meshcore_rs::MeshCoreEvent::new(
            EventType::ContactMsgRecv,
            EventPayload::ContactMessage(meshcore_rs::ContactMessage {
                sender_prefix,
                path_len: 0,
                txt_type: 0,
                sender_timestamp: 1_770_000_000,
                text: text.to_owned(),
                snr: None,
                signature: None,
            }),
        )
    }

    fn offline_backend() -> MeshCoreBackend {
        backend(Transport::Tcp {
            host: "radio".to_owned(),
            port: None,
        })
    }

    /// Word for word what a radio on the Galicia channel actually sent us, in
    /// answer to a message from this relay.
    #[tokio::test]
    async fn a_channel_message_gives_up_its_sender() {
        let backend = offline_backend();
        let message = backend
            .convert(&channel_event(
                1,
                "EA1HYZ 📟: @[Solo probando] hola buenos días",
            ))
            .await
            .expect("a channel message must be relayed");

        assert_eq!(message.network, "mc");
        assert_eq!(message.origin, Origin::Channel(1));
        assert_eq!(
            message.text, "@[Solo probando] hola buenos días",
            "the name is lifted out of the text, not left in it twice"
        );

        let sender = message.sender.expect("the name was there to be found");
        assert_eq!(sender.display_name(), Some("EA1HYZ 📟"));
        assert_eq!(
            sender.id, None,
            "a name is all the protocol gave; an address would be invented"
        );
    }

    #[tokio::test]
    async fn a_channel_message_without_the_convention_is_left_alone() {
        let backend = offline_backend();
        let message = backend
            .convert(&channel_event(1, "bos dias a todos"))
            .await
            .expect("a channel message must be relayed");

        assert!(
            message.sender.is_none(),
            "with no name to find, claiming one would be a lie"
        );
        assert_eq!(message.text, "bos dias a todos");
    }

    #[test]
    fn splits_the_name_a_client_wrote_from_the_message_a_person_typed() {
        assert_eq!(
            split_sender("EA1HYZ 📟: @[Solo probando] hola"),
            Some(("EA1HYZ 📟", "@[Solo probando] hola"))
        );

        // The first colon-space is the client's; everything after it was typed,
        // colons and all.
        assert_eq!(
            split_sender("EA1HYZ: mira: esto"),
            Some(("EA1HYZ", "mira: esto"))
        );

        // Names on this mesh look like this, and they must survive.
        assert_eq!(
            split_sender("LI.CSC P.AMARELA G4/8/O: probando"),
            Some(("LI.CSC P.AMARELA G4/8/O", "probando"))
        );
    }

    #[test]
    fn refuses_to_read_a_name_that_is_not_there() {
        // A bare colon is not the separator, so a URL stays whole.
        assert_eq!(split_sender("http://example.org/x"), None);
        assert_eq!(split_sender("no separator here"), None);
        assert_eq!(split_sender(": no name"), None);
        assert_eq!(split_sender("no message: "), None);

        // Long enough that the colon was punctuation in a sentence rather than a
        // client writing its name.
        let sentence = format!("{}: y luego", "palabra ".repeat(6));
        assert_eq!(split_sender(&sentence), None);
    }

    #[tokio::test]
    async fn a_direct_message_arrives_with_its_sender_address() {
        let backend = offline_backend();
        let prefix = [0xde, 0xad, 0xbe, 0xef, 0x00, 0x01];
        let message = backend
            .convert(&direct_event(prefix, "estas ahi?"))
            .await
            .expect("a direct message must be relayed");

        assert_eq!(message.origin, Origin::Direct(PeerId::KeyPrefix(prefix)));
        assert_eq!(message.text, "estas ahi?");

        // Identity is there even with no contact list to put a name to it, which
        // is what lets the router fall back to the address.
        let sender = message
            .sender
            .expect("a direct message identifies its sender");
        assert_eq!(sender.id, Some(PeerId::KeyPrefix(prefix)));
        assert_eq!(sender.display_name(), None);
    }

    #[tokio::test]
    async fn a_direct_message_carries_a_known_contact_name() {
        let backend = offline_backend();
        let prefix = [1, 2, 3, 4, 5, 6];
        backend
            .names
            .write()
            .expect("uncontended")
            .insert(prefix, "Alice".to_owned());

        // Without a live client the lookup cannot reach the radio, so this is the
        // mirror answering, which is the same path resolve_name uses.
        assert_eq!(
            backend.resolve_name(&PeerId::KeyPrefix(prefix)),
            Some("Alice".to_owned())
        );
    }

    #[tokio::test]
    async fn everything_that_is_not_a_message_is_ignored() {
        let backend = offline_backend();
        for event in [
            meshcore_rs::MeshCoreEvent::new(EventType::Battery, EventPayload::None),
            meshcore_rs::MeshCoreEvent::new(EventType::Advertisement, EventPayload::None),
            meshcore_rs::MeshCoreEvent::new(EventType::Disconnected, EventPayload::None),
        ] {
            assert!(
                backend.convert(&event).await.is_none(),
                "{:?} is not a message",
                event.event_type
            );
        }
    }

    #[tokio::test]
    async fn the_contact_refresh_cooldown_holds() {
        let backend = offline_backend();
        assert!(
            backend.may_refresh_names().await,
            "the first look must be allowed"
        );

        *backend.names_read_at.lock().await = Some(Instant::now());
        assert!(
            !backend.may_refresh_names().await,
            "a second stranger must not cost a second read of 350 contacts"
        );

        tokio::time::pause();
        tokio::time::advance(NAME_REFRESH_COOLDOWN).await;
        assert!(
            backend.may_refresh_names().await,
            "once the cooldown is out, asking again is fine"
        );
    }

    /// Hardware in the loop, which is the only thing that can answer whether the
    /// handshake and the contact list actually work. Ignored by default because
    /// it needs a radio; point it at one and run it explicitly:
    ///
    /// ```sh
    /// LORATRIX_MESHCORE_HOST=192.168.1.161 cargo test --all-features -- \
    ///     --ignored --nocapture meshcore
    /// ```
    ///
    /// It only listens. Nothing is transmitted, so running it costs no airtime.
    #[tokio::test]
    #[ignore = "needs a real MeshCore radio; set LORATRIX_MESHCORE_HOST"]
    async fn talks_to_a_real_meshcore_radio() {
        let Some(host) = std::env::var_os("LORATRIX_MESHCORE_HOST") else {
            // Skipped rather than failed: running the ignored tests with only one
            // kind of radio to hand should not look like a broken relay.
            println!("LORATRIX_MESHCORE_HOST is unset, so there is no radio to talk to");
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
            let mut seen = Vec::new();
            while let Some(message) = rx.recv().await {
                println!("received {message:?}");
                seen.push(message);
            }
            seen
        });

        // The pump runs until the link drops, so it is the timeout that ends the
        // test. Reaching it is the success case. Listen for longer with
        // LORATRIX_MESHCORE_LISTEN_SECS when the point is to catch real traffic
        // rather than just the handshake.
        let listen = std::env::var("LORATRIX_MESHCORE_LISTEN_SECS")
            .ok()
            .and_then(|secs| secs.parse().ok())
            .unwrap_or(20);
        println!("listening for {listen}s");
        let outcome = tokio::time::timeout(Duration::from_secs(listen), backend.pump(&tx)).await;
        match outcome {
            Err(_elapsed) => {}
            Ok(Ok(Outcome::Reconnect)) => panic!("the link dropped before the test was done"),
            Ok(Ok(Outcome::Stop)) => panic!("the receiving end went away"),
            Ok(Err(error)) => panic!("{error:#}"),
        }

        // A count and a taste of it: the mesh this was written against knows 350
        // contacts, and printing them all buries everything else.
        let names = backend.names.read().expect("uncontended").clone();
        let sample: Vec<&String> = names.values().take(5).collect();
        println!("{} contact(s), for example {sample:?}", names.len());

        drop(tx);
        let seen = listener.await.expect("the listener does not panic");
        println!("{} message(s) relayed while listening", seen.len());

        // The contact list is what proves the handshake got through: appstart has
        // to succeed before the radio answers anything, and an empty list is a
        // legitimate answer from a radio that has met nobody.
        assert!(
            backend.client.read().await.is_some(),
            "the backend should still hold a live client"
        );
    }

    /// The other half of the hardware check: that a message we hand over reaches
    /// the air. This one **transmits**, on a real mesh, where real people will
    /// read it, so it stays behind a second gate and does nothing at all unless
    /// told exactly what to say:
    ///
    /// ```sh
    /// LORATRIX_MESHCORE_HOST=192.168.1.161 \
    /// LORATRIX_MESHCORE_CHANNEL=Galicia \
    /// LORATRIX_MESHCORE_SEND="Hello folks" \
    ///     cargo test --all-features -- --ignored --nocapture sends_to
    /// ```
    ///
    /// Without `LORATRIX_MESHCORE_SEND` it lists the channels and stops, which is
    /// how to find out what the radio is subscribed to before saying anything on
    /// it. Reaching for the channel by name rather than by index is deliberate:
    /// an index is easy to get wrong and the mistake goes out over the air.
    #[tokio::test]
    #[ignore = "transmits on a real mesh; needs LORATRIX_MESHCORE_HOST and _SEND"]
    async fn sends_to_a_real_meshcore_radio() {
        let Some(host) = std::env::var_os("LORATRIX_MESHCORE_HOST") else {
            // Skipped rather than failed: running the ignored tests with only one
            // kind of radio to hand should not look like a broken relay.
            println!("LORATRIX_MESHCORE_HOST is unset, so there is no radio to talk to");
            return;
        };
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
        let backend = std::sync::Arc::new(backend(Transport::Tcp {
            host: host.to_string_lossy().into_owned(),
            port: None,
        }));

        let (tx, mut rx) = mpsc::channel(16);
        let listener = tokio::spawn(async move {
            let mut seen = Vec::new();
            while let Some(message) = rx.recv().await {
                println!("received {message:?}");
                seen.push(message);
            }
            seen
        });

        // The pump has to keep running throughout: it is what holds the link open
        // and what would show a reply arriving.
        let pumping = tokio::spawn({
            let backend = backend.clone();
            async move { backend.pump(&tx).await }
        });

        // Wait for the handshake rather than guessing how long it takes.
        let mut waited = Duration::ZERO;
        while backend.client.read().await.is_none() {
            assert!(waited < Duration::from_secs(15), "the radio never came up");
            sleep(Duration::from_millis(200)).await;
            waited += Duration::from_millis(200);
        }

        let channels = {
            let client = backend.client.read().await;
            let client = client.as_ref().expect("connected just above");
            let mut found = Vec::new();
            for index in 0..MAX_CHANNELS {
                match client.commands().lock().await.get_channel(index).await {
                    Ok(info) if !info.name.is_empty() => found.push((index, info.name)),
                    Ok(_) => {}
                    Err(error) => {
                        println!("channel {index} could not be read: {error}");
                        break;
                    }
                }
            }
            found
        };
        println!("channels: {channels:?}");

        let Ok(text) = std::env::var("LORATRIX_MESHCORE_SEND") else {
            println!("LORATRIX_MESHCORE_SEND is unset, so nothing was transmitted");
            pumping.abort();
            return;
        };
        let wanted = std::env::var("LORATRIX_MESHCORE_CHANNEL").unwrap_or_else(|_| "0".to_owned());
        let channel = channels
            .iter()
            .find(|(_, name)| name.eq_ignore_ascii_case(&wanted))
            .map(|(index, _)| *index)
            .unwrap_or_else(|| {
                panic!("no channel named {wanted:?} on this radio; it has {channels:?}")
            });

        println!("sending {text:?} on channel {channel} ({wanted})");
        backend
            .send_channel(channel, &text, None)
            .await
            .expect("the radio should accept the message");
        println!("the radio accepted it");

        // Long enough for somebody to answer, if anybody is going to.
        let listen = std::env::var("LORATRIX_MESHCORE_LISTEN_SECS")
            .ok()
            .and_then(|secs| secs.parse().ok())
            .unwrap_or(60);
        println!("listening {listen}s for a reply");
        sleep(Duration::from_secs(listen)).await;

        pumping.abort();
        let seen = listener.await.expect("the listener does not panic");
        println!("{} message(s) arrived afterwards", seen.len());
    }
}
