//! The mesh-side abstraction.
//!
//! Meshtastic and MeshCore differ in ways that leak into any honest
//! abstraction, so the trait normalizes events rather than pretending one
//! protocol looks like the other:
//!
//! * **Sender identity.** Meshtastic packets carry a node number that resolves
//!   against the node database. MeshCore channel messages carry *no* sender
//!   identity at all — the name travels inside the message text by client
//!   convention. Hence [`IncomingMesh::sender`] is an [`Option`].
//! * **Delivery model.** Meshtastic pushes packets; MeshCore signals
//!   `MessagesWaiting` and the host pulls until the queue drains. Both are
//!   hidden inside [`MeshBackend::run`].
//! * **Payload budget and pacing** differ per protocol, so they are per-backend
//!   values rather than global constants.

use std::fmt;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

#[cfg(feature = "meshcore")]
pub mod meshcore;
#[cfg(feature = "meshtastic")]
pub mod meshtastic;

/// Logical name of a configured network, as written in `[[networks]].name`.
pub type NetworkId = String;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "each variant is only constructed by its own backend, which may be feature-disabled"
)]
pub enum MeshKind {
    Meshtastic,
    MeshCore,
}

impl fmt::Display for MeshKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MeshKind::Meshtastic => f.write_str("meshtastic"),
            MeshKind::MeshCore => f.write_str("meshcore"),
        }
    }
}

/// How a peer is addressed, which is protocol specific.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[allow(
    dead_code,
    reason = "each variant belongs to one backend, and a build may not have it"
)]
pub enum PeerId {
    /// Meshtastic node number.
    Node(u32),
    /// First six bytes of a MeshCore contact's public key, which is what the
    /// companion protocol puts on the wire.
    KeyPrefix([u8; 6]),
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerId::Node(num) => write!(f, "!{num:08x}"),
            PeerId::KeyPrefix(prefix) => {
                for byte in prefix {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
        }
    }
}

/// How a reply names the message it answers, which the two protocols do
/// differently — the same split as sender identity, and for the same reason.
///
/// Meshtastic has a field for it: every packet carries a 32-bit id and a reply
/// quotes it. MeshCore has nothing of the kind on the wire, so its clients do
/// what they already do for the sender's name and put it in the text, writing
/// `@[NAME]` at the head of the answer:
///
/// ```text
/// ALICE: Hola a todos
/// BOB: @[ALICE] Hola Alice
/// ```
///
/// Which means what MeshCore points at is the person rather than the message. It
/// is what the protocol gives us, and it is what every client there understands.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[allow(
    dead_code,
    reason = "each variant belongs to one backend, and a build may not have it"
)]
pub enum MessageRef {
    /// A Meshtastic packet id.
    Packet(u32),
    /// The name a MeshCore client wrote into the text, which is all a reply
    /// there has to point at.
    Author(String),
}

impl fmt::Display for MessageRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageRef::Packet(id) => write!(f, "{id:08x}"),
            MessageRef::Author(name) => f.write_str(name),
        }
    }
}

/// A direct message handed to a radio, and the promise of hearing whether it
/// arrived.
///
/// Both protocols acknowledge a direct message, and neither does it in the
/// answer to the send: the acknowledgement comes back over the air later and
/// arrives through the receive loop like anything else. So a send reports what
/// to wait for rather than what happened.
#[derive(Debug)]
pub struct Delivery {
    /// What the message went out as, so a reaction to it can be recognized when
    /// it comes back. `None` on a protocol with nothing to name.
    pub reference: Option<MessageRef>,
    /// `true` once the far node has acknowledged. `false` if the mesh said
    /// outright that it could not be delivered, which is worth knowing early
    /// rather than waiting out the timeout for.
    ///
    /// A dropped sender — the link went down, or the radio was replaced — closes
    /// the channel, and an answer that never comes reads the same as one that
    /// was never going to.
    pub ack: oneshot::Receiver<bool>,
    /// How long to wait before calling it lost. Asked of the radio where the
    /// protocol says (MeshCore suggests one per message), and a fixed value
    /// where it does not.
    pub within: Duration,
}

/// Where a received message came from.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "only a backend constructs these, and a build may have none that does yet"
)]
pub enum Origin {
    /// A broadcast channel, identified by its index on the radio.
    Channel(u8),
    /// A direct message addressed to the relay's own node.
    Direct(PeerId),
}

/// What we know about who sent a message. Every field is optional because the
/// two protocols expose different amounts of it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Sender {
    pub id: Option<PeerId>,
    pub long_name: Option<String>,
    pub short_name: Option<String>,
}

impl Sender {
    /// Best available human-readable name.
    ///
    /// Radios that were never configured report an empty long name, so an empty
    /// candidate has to fall through to the next one rather than win.
    pub fn display_name(&self) -> Option<&str> {
        [self.long_name.as_deref(), self.short_name.as_deref()]
            .into_iter()
            .flatten()
            .find(|name| !name.is_empty())
    }
}

/// A message received from a mesh network, normalized across protocols.
#[derive(Clone, Debug)]
pub struct IncomingMesh {
    pub network: NetworkId,
    pub origin: Origin,
    /// `None` when the protocol does not carry sender identity, which is the
    /// case for every MeshCore channel message.
    pub sender: Option<Sender>,
    pub text: String,
    /// What a reply to this message would have to name. `None` when there is
    /// nothing to name it by, which on MeshCore is any message whose sender we
    /// could not recover from the text.
    pub reference: Option<MessageRef>,
    /// Set when this is not a message at all but a reaction to one, naming what
    /// it is a reaction to. The text is then the emoji itself.
    ///
    /// Only Meshtastic has these: a tapback is an ordinary text packet with
    /// `emoji` set, which is the only thing telling it apart from somebody
    /// typing a thumbs-up on its own.
    pub reacts_to: Option<MessageRef>,
}

impl IncomingMesh {
    pub fn channel(&self) -> Option<u8> {
        match self.origin {
            Origin::Channel(channel) => Some(channel),
            Origin::Direct(_) => None,
        }
    }
}

/// One mesh network the relay is attached to.
///
/// Implementations own their transport, including reconnection: [`run`] is
/// expected to keep trying rather than returning on a dropped link.
///
/// [`run`]: MeshBackend::run
#[async_trait]
pub trait MeshBackend: Send + Sync {
    /// Configured name of this network.
    fn id(&self) -> &str;

    fn kind(&self) -> MeshKind;

    /// Largest text payload, in bytes, that a single message may carry.
    fn max_payload(&self) -> usize;

    /// Minimum gap between transmissions, to respect firmware and duty-cycle
    /// limits.
    fn min_send_interval(&self) -> Duration;

    /// Connect and pump received messages into `tx` until cancelled.
    async fn run(&self, tx: mpsc::Sender<IncomingMesh>) -> Result<()>;

    /// How this protocol writes "in reply to" *inside the text*, where it writes
    /// it there at all.
    ///
    /// MeshCore does, because it has nowhere else to put it. The marker is
    /// therefore part of the payload and has to be applied by the router, before
    /// the text is measured against [`max_payload`] — otherwise a reply is
    /// truncated by exactly the room the marker takes.
    ///
    /// Meshtastic answers [`None`]: its reply travels in a field of the packet,
    /// which is what `reply_to` is for and costs the text nothing.
    ///
    /// [`max_payload`]: MeshBackend::max_payload
    fn reply_marker(&self, _to: &MessageRef) -> Option<String> {
        None
    }

    /// Broadcast on a channel, optionally as a reply to a message already there.
    ///
    /// Returns what a reply to *this* message would have to name, so the router
    /// can tie it to the Matrix event it came from. A backend asked for a reply
    /// it cannot express sends the text plainly rather than failing: a message
    /// delivered without its thread beats no message at all.
    async fn send_channel(
        &self,
        channel: u8,
        text: &str,
        reply_to: Option<MessageRef>,
    ) -> Result<Option<MessageRef>>;

    /// Send a direct message to a peer.
    ///
    /// This is what an answer written in a DM inbox goes out as. No reply
    /// relation is expressed: a direct message is a conversation of two, where
    /// naming what you answer says nothing the address does not.
    ///
    /// Returns what to wait on to find out whether it arrived. See [`Delivery`].
    async fn send_direct(&self, peer: &PeerId, text: &str) -> Result<Delivery>;

    /// Resolve a peer to a display name from whatever cache the backend keeps.
    fn resolve_name(&self, peer: &PeerId) -> Option<String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_peer_ids_per_protocol() {
        assert_eq!(PeerId::Node(0x1a2b_3c4d).to_string(), "!1a2b3c4d");
        assert_eq!(
            PeerId::KeyPrefix([0xde, 0xad, 0xbe, 0xef, 0x00, 0x01]).to_string(),
            "deadbeef0001"
        );
    }

    #[test]
    fn display_name_prefers_long_and_skips_empty() {
        let sender = Sender {
            id: None,
            long_name: Some("Alice Cooper".to_owned()),
            short_name: Some("ALCO".to_owned()),
        };
        assert_eq!(sender.display_name(), Some("Alice Cooper"));

        let short_only = Sender {
            long_name: Some(String::new()),
            ..sender
        };
        assert_eq!(short_only.display_name(), Some("ALCO"));

        assert_eq!(Sender::default().display_name(), None);
    }

    #[test]
    fn channel_is_none_for_direct_messages() {
        let direct = IncomingMesh {
            network: "lora".to_owned(),
            origin: Origin::Direct(PeerId::Node(7)),
            sender: None,
            text: "hi".to_owned(),
            reference: None,
            reacts_to: None,
        };
        assert_eq!(direct.channel(), None);

        let broadcast = IncomingMesh {
            origin: Origin::Channel(3),
            ..direct
        };
        assert_eq!(broadcast.channel(), Some(3));
    }
}
