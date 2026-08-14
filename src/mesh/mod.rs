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
use tokio::sync::mpsc;

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
// Tests construct every variant; the production paths that will are the backend
// packet loops, still to be written.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "variants are constructed by the backend packet loops, still to be written"
    )
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

/// Where a received message came from.
#[derive(Clone, Debug, PartialEq, Eq)]
// Tests construct every variant; the production paths that will are the backend
// packet loops, still to be written.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "variants are constructed by the backend packet loops, still to be written"
    )
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

    /// Broadcast on a channel.
    async fn send_channel(&self, channel: u8, text: &str) -> Result<()>;

    /// Send a direct message to a peer.
    ///
    /// Unused while DM inboxes are read-only, but both protocols support it, so
    /// the trait keeps the door open for an explicit reply syntax later.
    #[expect(dead_code, reason = "reserved for replying to direct messages")]
    async fn send_direct(&self, peer: &PeerId, text: &str) -> Result<()>;

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
        };
        assert_eq!(direct.channel(), None);

        let broadcast = IncomingMesh {
            origin: Origin::Channel(3),
            ..direct
        };
        assert_eq!(broadcast.channel(), Some(3));
    }
}
