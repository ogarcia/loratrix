//! Meshtastic backend.
//!
//! Status: skeleton. The connection and packet loop are the next step; what is
//! settled here is the shape of the backend, the payload budget and the name
//! cache the packet loop will fill from the radio's node database.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

use anyhow::{Result, bail};
use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::config::{NetworkConfig, NetworkKind, Transport};
use crate::mesh::{IncomingMesh, MeshBackend, MeshKind, NetworkId, PeerId, Sender};

/// Firmware refuses to transmit faster than this, so it is a floor rather than a
/// preference.
const MIN_SEND_INTERVAL: Duration = Duration::from_secs(2);

pub struct MeshtasticBackend {
    id: NetworkId,
    target: String,
    /// Long and short names from the radio's node database, keyed by node
    /// number. Filled by the packet loop; in-memory only, by design.
    nodes: RwLock<HashMap<u32, Sender>>,
}

impl MeshtasticBackend {
    pub fn new(network: &NetworkConfig) -> Result<Self> {
        let target = match &network.transport {
            Transport::Tcp { host, .. } => {
                let port = network
                    .transport
                    .tcp_port(NetworkKind::Meshtastic)
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
            nodes: RwLock::new(HashMap::new()),
        })
    }
}

/// Largest payload a data packet may carry, straight from the protobuf
/// definitions so it tracks the protocol rather than a guess.
fn max_text_len() -> usize {
    meshtastic::protobufs::Constants::DataPayloadLen as usize
}

#[async_trait]
impl MeshBackend for MeshtasticBackend {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> MeshKind {
        MeshKind::Meshtastic
    }

    fn max_payload(&self) -> usize {
        max_text_len()
    }

    fn min_send_interval(&self) -> Duration {
        MIN_SEND_INTERVAL
    }

    async fn run(&self, _tx: mpsc::Sender<IncomingMesh>) -> Result<()> {
        // Next step: build the stream, `StreamApi::connect` + `configure`, then
        // forward TEXT_MESSAGE_APP packets and keep `nodes` up to date from
        // NodeInfo packets, reconnecting with backoff.
        bail!(
            "the Meshtastic backend is not implemented yet (network {:?}, target {})",
            self.id,
            self.target
        )
    }

    async fn send_channel(&self, _channel: u8, _text: &str) -> Result<()> {
        bail!("the Meshtastic backend is not implemented yet")
    }

    async fn send_direct(&self, _peer: &PeerId, _text: &str) -> Result<()> {
        bail!("the Meshtastic backend is not implemented yet")
    }

    fn resolve_name(&self, peer: &PeerId) -> Option<String> {
        let PeerId::Node(num) = peer else {
            return None;
        };
        self.nodes
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

    #[test]
    fn resolves_the_default_tcp_port() {
        let backend = MeshtasticBackend::new(&network(Transport::Tcp {
            host: "192.168.1.41".to_owned(),
            port: None,
        }))
        .expect("valid transport");
        assert_eq!(backend.target, "192.168.1.41:4403");
    }

    #[test]
    fn honours_an_explicit_tcp_port() {
        let backend = MeshtasticBackend::new(&network(Transport::Tcp {
            host: "radio".to_owned(),
            port: Some(4404),
        }))
        .expect("valid transport");
        assert_eq!(backend.target, "radio:4404");
    }

    #[test]
    fn takes_the_payload_budget_from_the_protocol() {
        // 233 today; the point is that it comes from the protobufs, not from us.
        assert_eq!(max_text_len(), 233);
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

    #[test]
    fn resolve_name_returns_none_for_a_meshcore_peer() {
        let backend = MeshtasticBackend::new(&network(Transport::Tcp {
            host: "radio".to_owned(),
            port: None,
        }))
        .expect("valid transport");
        assert_eq!(backend.resolve_name(&PeerId::KeyPrefix([0; 6])), None);
    }
}
