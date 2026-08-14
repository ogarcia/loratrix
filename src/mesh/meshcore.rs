//! MeshCore backend.
//!
//! Status: skeleton. What is settled here is the shape of the backend and the
//! contact cache, which is the only way a MeshCore direct message can be
//! attributed: the wire carries just the first six bytes of the sender's public
//! key, resolved against the contact list.
//!
//! Channel messages are a different story — they carry no sender identity at
//! all, so the backend reports `sender: None` and lets the router decide what
//! to show.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

use anyhow::{Result, bail};
use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::config::{NetworkConfig, NetworkKind, Transport};
use crate::mesh::{IncomingMesh, MeshBackend, MeshKind, NetworkId, PeerId};

/// Conservative text budget. MeshCore does not expose a payload constant the way
/// Meshtastic's protobufs do, so this is a floor picked to be safe rather than a
/// figure read from the protocol. Worth measuring against real firmware before
/// raising it.
const MAX_TEXT_LEN: usize = 160;

/// No firmware-documented floor as with Meshtastic; kept equal for now so a
/// bridged room paces both meshes the same way. Also worth measuring.
const MIN_SEND_INTERVAL: Duration = Duration::from_secs(2);

pub struct MeshCoreBackend {
    id: NetworkId,
    target: String,
    /// Advertised names keyed by public-key prefix, from `get_contacts`.
    contacts: RwLock<HashMap<[u8; 6], String>>,
}

impl MeshCoreBackend {
    pub fn new(network: &NetworkConfig) -> Result<Self> {
        let target = match &network.transport {
            Transport::Tcp { host, .. } => {
                let port = network
                    .transport
                    .tcp_port(NetworkKind::Meshcore)
                    .expect("a TCP transport always resolves a port");
                format!("{host}:{port}")
            }
            Transport::Serial { port, baud } => format!("{port}@{baud}"),
            Transport::Ble { address } => {
                if !cfg!(feature = "ble") {
                    bail!(
                        "network {:?} uses a BLE transport, but this build lacks the `ble` feature",
                        network.name
                    );
                }
                address.clone()
            }
        };

        Ok(Self {
            id: network.name.clone(),
            target,
            contacts: RwLock::new(HashMap::new()),
        })
    }
}

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

    async fn run(&self, _tx: mpsc::Sender<IncomingMesh>) -> Result<()> {
        // Next step: `MeshCore::tcp`/`serial`, `send_appstart`, prime `contacts`
        // from `get_contacts`, then `start_auto_message_fetching` and forward
        // ChannelMsgRecv and ContactMsgRecv events, reconnecting with backoff.
        bail!(
            "the MeshCore backend is not implemented yet (network {:?}, target {})",
            self.id,
            self.target
        )
    }

    async fn send_channel(&self, _channel: u8, _text: &str) -> Result<()> {
        bail!("the MeshCore backend is not implemented yet")
    }

    async fn send_direct(&self, _peer: &PeerId, _text: &str) -> Result<()> {
        bail!("the MeshCore backend is not implemented yet")
    }

    fn resolve_name(&self, peer: &PeerId) -> Option<String> {
        let PeerId::KeyPrefix(prefix) = peer else {
            return None;
        };
        self.contacts.read().ok()?.get(prefix).cloned()
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

    #[test]
    fn resolves_the_default_tcp_port() {
        let backend = MeshCoreBackend::new(&network(Transport::Tcp {
            host: "192.168.1.161".to_owned(),
            port: None,
        }))
        .expect("valid transport");
        assert_eq!(backend.target, "192.168.1.161:5000");
    }

    #[test]
    fn resolves_a_cached_contact_name() {
        let backend = MeshCoreBackend::new(&network(Transport::Tcp {
            host: "radio".to_owned(),
            port: None,
        }))
        .expect("valid transport");

        let prefix = [1, 2, 3, 4, 5, 6];
        backend
            .contacts
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
}
