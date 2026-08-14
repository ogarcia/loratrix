//! Configuration file parsing and validation.
//!
//! The config is TOML. Every network is named, and rooms reference networks by
//! name, so a room can bridge several meshes at once (see [`RoomConfig`]).

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::mesh::MeshKind;

/// Default TCP port used by both Meshtastic and MeshCore companion radios.
const DEFAULT_MESHTASTIC_TCP_PORT: u16 = 4403;
const DEFAULT_MESHCORE_TCP_PORT: u16 = 5000;
/// Default baud rate for USB serial companion links.
const DEFAULT_SERIAL_BAUD: u32 = 115_200;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub matrix: MatrixConfig,
    #[serde(default)]
    pub relay: RelayConfig,
    pub networks: Vec<NetworkConfig>,
    pub rooms: Vec<RoomConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatrixConfig {
    /// Base URL of the homeserver, e.g. `https://matrix.example.org`.
    pub homeserver: String,
    /// Full user id of the relay account, e.g. `@relay:example.org`.
    pub user_id: String,
    /// Access token, given inline. Mutually exclusive with `access_token_file`.
    #[serde(default)]
    pub access_token: Option<String>,
    /// Path to a file holding the access token. Preferred over the inline form
    /// so the token does not sit in a world-readable config.
    #[serde(default)]
    pub access_token_file: Option<PathBuf>,
}

impl MatrixConfig {
    /// Resolve the access token from whichever source is configured.
    pub fn resolve_token(&self) -> Result<String> {
        match (&self.access_token, &self.access_token_file) {
            (Some(token), None) => Ok(token.clone()),
            (None, Some(path)) => {
                let path = expand_tilde(path);
                let raw = std::fs::read_to_string(&path).with_context(|| {
                    format!("reading matrix.access_token_file {}", path.display())
                })?;
                let token = raw.trim().to_owned();
                if token.is_empty() {
                    bail!("matrix.access_token_file {} is empty", path.display());
                }
                Ok(token)
            }
            (Some(_), Some(_)) => {
                bail!("set either matrix.access_token or matrix.access_token_file, not both")
            }
            (None, None) => {
                bail!("one of matrix.access_token or matrix.access_token_file is required")
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayConfig {
    /// How long a relayed message stays in the duplicate-suppression cache.
    /// This is what stops a message fanned out to a second mesh from coming
    /// back, and what absorbs MQTT duplicates on Meshtastic.
    #[serde(default = "default_dedupe_window_secs")]
    pub dedupe_window_secs: u64,
    /// Prefix prepended to mesh messages on their way into Matrix. Applies to
    /// both protocols; the two below override it for one of them.
    #[serde(default = "default_mesh_to_matrix_prefix")]
    pub mesh_to_matrix_prefix: String,
    #[serde(default)]
    pub meshtastic_to_matrix_prefix: Option<String>,
    #[serde(default)]
    pub meshcore_to_matrix_prefix: Option<String>,

    /// Prefix prepended to Matrix messages on their way into a mesh channel.
    #[serde(default = "default_matrix_to_mesh_prefix")]
    pub matrix_to_mesh_prefix: String,
    #[serde(default)]
    pub matrix_to_meshtastic_prefix: Option<String>,
    #[serde(default)]
    pub matrix_to_meshcore_prefix: Option<String>,

    /// Prefix used when fanning a mesh message out to another mesh, which
    /// happens when one room has more than one route. Kept short because it
    /// spends a LoRa payload budget rather than a Matrix one.
    #[serde(default = "default_mesh_to_mesh_prefix")]
    pub mesh_to_mesh_prefix: String,
    #[serde(default)]
    pub meshtastic_to_meshcore_prefix: Option<String>,
    #[serde(default)]
    pub meshcore_to_meshtastic_prefix: Option<String>,

    /// Prefix used for direct messages landing in a read-only DM inbox room.
    #[serde(default = "default_dm_prefix")]
    pub dm_prefix: String,
    #[serde(default)]
    pub meshtastic_dm_prefix: Option<String>,
    #[serde(default)]
    pub meshcore_dm_prefix: Option<String>,
    /// Stand-in for `{name}` when the protocol did not tell us who sent the
    /// message, which is every MeshCore channel message. The real name is
    /// usually inside the text itself.
    #[serde(default = "default_unknown_sender_name")]
    pub unknown_sender_name: String,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            dedupe_window_secs: default_dedupe_window_secs(),
            mesh_to_matrix_prefix: default_mesh_to_matrix_prefix(),
            meshtastic_to_matrix_prefix: None,
            meshcore_to_matrix_prefix: None,
            matrix_to_mesh_prefix: default_matrix_to_mesh_prefix(),
            matrix_to_meshtastic_prefix: None,
            matrix_to_meshcore_prefix: None,
            mesh_to_mesh_prefix: default_mesh_to_mesh_prefix(),
            meshtastic_to_meshcore_prefix: None,
            meshcore_to_meshtastic_prefix: None,
            dm_prefix: default_dm_prefix(),
            meshtastic_dm_prefix: None,
            meshcore_dm_prefix: None,
            unknown_sender_name: default_unknown_sender_name(),
        }
    }
}

impl RelayConfig {
    pub fn dedupe_window(&self) -> Duration {
        Duration::from_secs(self.dedupe_window_secs)
    }

    /// Template for a mesh message going to Matrix.
    ///
    /// Each direction has a general template and a per-protocol override, and the
    /// override wins where it is set. That exists because the two protocols do not
    /// carry the same things: only Meshtastic has a short name, so a template
    /// using `{short}` is only worth writing for one of them.
    pub fn mesh_to_matrix(&self, from: Option<MeshKind>) -> &str {
        self.per_kind(
            from,
            &self.meshtastic_to_matrix_prefix,
            &self.meshcore_to_matrix_prefix,
            &self.mesh_to_matrix_prefix,
        )
    }

    /// Template for a Matrix message going to a mesh channel.
    pub fn matrix_to_mesh(&self, to: Option<MeshKind>) -> &str {
        self.per_kind(
            to,
            &self.matrix_to_meshtastic_prefix,
            &self.matrix_to_meshcore_prefix,
            &self.matrix_to_mesh_prefix,
        )
    }

    /// Template for a direct message going to its inbox room.
    pub fn dm(&self, from: Option<MeshKind>) -> &str {
        self.per_kind(
            from,
            &self.meshtastic_dm_prefix,
            &self.meshcore_dm_prefix,
            &self.dm_prefix,
        )
    }

    /// Template for a mesh message fanned out to the other mesh in its room.
    /// Named for where it is going, since that is whose payload it spends.
    pub fn mesh_to_mesh(&self, to: Option<MeshKind>) -> &str {
        self.per_kind(
            to,
            // Going *to* Meshtastic means it came from MeshCore, there being two.
            &self.meshcore_to_meshtastic_prefix,
            &self.meshtastic_to_meshcore_prefix,
            &self.mesh_to_mesh_prefix,
        )
    }

    /// `None` means the protocol is not known, which leaves the general template.
    fn per_kind<'a>(
        &self,
        kind: Option<MeshKind>,
        meshtastic: &'a Option<String>,
        meshcore: &'a Option<String>,
        general: &'a str,
    ) -> &'a str {
        match kind {
            Some(MeshKind::Meshtastic) => meshtastic.as_deref(),
            Some(MeshKind::MeshCore) => meshcore.as_deref(),
            None => None,
        }
        .unwrap_or(general)
    }
}

fn default_dedupe_window_secs() -> u64 {
    30
}

fn default_mesh_to_matrix_prefix() -> String {
    "[{name}/{network}]: ".to_owned()
}

fn default_matrix_to_mesh_prefix() -> String {
    "{name5}[M]: ".to_owned()
}

fn default_mesh_to_mesh_prefix() -> String {
    "{name5}[{network4}]: ".to_owned()
}

fn default_dm_prefix() -> String {
    "[DM]{network_tag} {name}: ".to_owned()
}

fn default_unknown_sender_name() -> String {
    "unknown".to_owned()
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NetworkKind {
    Meshtastic,
    Meshcore,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    /// Logical name, used by rooms to reference this network and shown in
    /// prefixes. Must be unique.
    pub name: String,
    pub kind: NetworkKind,
    pub transport: Transport,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum Transport {
    Tcp {
        host: String,
        #[serde(default)]
        port: Option<u16>,
    },
    Serial {
        port: String,
        #[serde(default = "default_serial_baud")]
        baud: u32,
    },
    /// Only usable in builds with the `ble` feature.
    Ble { address: String },
}

fn default_serial_baud() -> u32 {
    DEFAULT_SERIAL_BAUD
}

impl Transport {
    /// Reject a transport that parses but names nowhere to connect to.
    fn validate(&self, network: &str) -> Result<()> {
        let missing = match self {
            Transport::Tcp { host, .. } if host.is_empty() => "a TCP host",
            Transport::Serial { port, .. } if port.is_empty() => "a serial port",
            Transport::Ble { address } if address.is_empty() => "a BLE address",
            _ => return Ok(()),
        };
        bail!("network {network:?} is missing {missing}")
    }

    /// TCP port to dial, falling back to the per-protocol default.
    pub fn tcp_port(&self, kind: NetworkKind) -> Option<u16> {
        match self {
            Transport::Tcp { port, .. } => Some(port.unwrap_or(match kind {
                NetworkKind::Meshtastic => DEFAULT_MESHTASTIC_TCP_PORT,
                NetworkKind::Meshcore => DEFAULT_MESHCORE_TCP_PORT,
            })),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoomConfig {
    /// Matrix room id or alias.
    pub id: String,
    /// Mesh channels bridged to this room. More than one route turns the room
    /// into a mesh-to-mesh bridge: the router fans each message out to the
    /// other routes as well as to Matrix.
    #[serde(default)]
    pub routes: Vec<Route>,
    /// Networks whose direct messages are mirrored into this room. A DM inbox
    /// is read-only: nothing written in the room is sent back to the mesh.
    #[serde(default)]
    pub dm_inbox: Option<NetworkList>,
}

impl RoomConfig {
    /// Networks feeding this room's DM inbox, empty when it is not one.
    pub fn dm_networks(&self) -> &[String] {
        self.dm_inbox.as_ref().map_or(&[], NetworkList::as_slice)
    }

    pub fn is_dm_inbox(&self) -> bool {
        !self.dm_networks().is_empty()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    /// Name of a network declared in `[[networks]]`.
    pub network: String,
    pub channel: u8,
}

/// Accepts either a single network name or a list of them, so both
/// `dm_inbox = "lora"` and `dm_inbox = ["lora", "mc"]` parse.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum NetworkList {
    One(String),
    Many(Vec<String>),
}

impl NetworkList {
    pub fn as_slice(&self) -> &[String] {
        match self {
            NetworkList::One(name) => std::slice::from_ref(name),
            NetworkList::Many(names) => names,
        }
    }
}

impl Config {
    /// Read and validate a config file.
    pub fn load(path: &Path) -> Result<Self> {
        let path = expand_tilde(path);
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let config: Config =
            toml::from_str(&raw).with_context(|| format!("parsing config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    /// Reject configs that parse but cannot describe a working relay.
    pub fn validate(&self) -> Result<()> {
        if self.networks.is_empty() {
            bail!("at least one [[networks]] entry is required");
        }
        if self.rooms.is_empty() {
            bail!("at least one [[rooms]] entry is required");
        }
        if !self.matrix.user_id.starts_with('@') {
            bail!(
                "matrix.user_id must be a full user id like @relay:example.org, got {:?}",
                self.matrix.user_id
            );
        }

        let mut seen_networks = HashSet::new();
        for network in &self.networks {
            if network.name.is_empty() {
                bail!("network names must not be empty");
            }
            if !seen_networks.insert(network.name.as_str()) {
                bail!("duplicate network name {:?}", network.name);
            }
            network.transport.validate(&network.name)?;
        }

        let mut seen_rooms = HashSet::new();
        // A mesh channel may feed only one room, so the mesh-to-Matrix
        // direction is unambiguous.
        let mut seen_channels = HashSet::new();
        for room in &self.rooms {
            if !seen_rooms.insert(room.id.as_str()) {
                bail!("duplicate room id {:?}", room.id);
            }
            if room.routes.is_empty() && !room.is_dm_inbox() {
                bail!("room {:?} has neither routes nor dm_inbox", room.id);
            }
            if !room.routes.is_empty() && room.is_dm_inbox() {
                bail!(
                    "room {:?} mixes routes with dm_inbox; a DM inbox is read-only, \
                     so give it its own room",
                    room.id
                );
            }

            for route in &room.routes {
                if !seen_networks.contains(route.network.as_str()) {
                    bail!(
                        "room {:?} routes to unknown network {:?}",
                        room.id,
                        route.network
                    );
                }
                if !seen_channels.insert((route.network.as_str(), route.channel)) {
                    bail!(
                        "network {:?} channel {} is routed to more than one room",
                        route.network,
                        route.channel
                    );
                }
            }

            let mut seen_dm = HashSet::new();
            for name in room.dm_networks() {
                if !seen_networks.contains(name.as_str()) {
                    bail!(
                        "room {:?} has unknown network {:?} in dm_inbox",
                        room.id,
                        name
                    );
                }
                if !seen_dm.insert(name.as_str()) {
                    bail!(
                        "room {:?} lists network {:?} twice in dm_inbox",
                        room.id,
                        name
                    );
                }
            }
        }

        Ok(())
    }
}

/// Expand a leading `~/` against `$HOME`. Enough for config paths, and cheaper
/// than a dependency.
fn expand_tilde(path: &Path) -> PathBuf {
    let Ok(stripped) = path.strip_prefix("~") else {
        return path.to_path_buf();
    };
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(stripped),
        None => path.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
        [matrix]
        homeserver = "https://example.org"
        user_id = "@relay:example.org"
        access_token = "secret"

        [[networks]]
        name = "lora"
        kind = "meshtastic"
        transport = { type = "tcp", host = "192.168.1.41" }

        [[rooms]]
        id = "!room:example.org"
        routes = [{ network = "lora", channel = 0 }]
    "#;

    fn parse(raw: &str) -> Result<Config> {
        let config: Config = toml::from_str(raw)?;
        config.validate()?;
        Ok(config)
    }

    #[test]
    fn parses_a_minimal_config() {
        let config = parse(MINIMAL).expect("minimal config should be valid");
        assert_eq!(config.networks.len(), 1);
        assert_eq!(config.rooms[0].routes[0].channel, 0);
        assert_eq!(config.relay.dedupe_window(), Duration::from_secs(30));
    }

    #[test]
    fn applies_per_protocol_default_tcp_ports() {
        let config = parse(MINIMAL).expect("valid");
        let transport = &config.networks[0].transport;
        assert_eq!(transport.tcp_port(NetworkKind::Meshtastic), Some(4403));
        assert_eq!(transport.tcp_port(NetworkKind::Meshcore), Some(5000));
    }

    #[test]
    fn accepts_dm_inbox_as_string_or_list() {
        let one = format!("{MINIMAL}\n[[rooms]]\nid = \"!dm:example.org\"\ndm_inbox = \"lora\"\n");
        assert_eq!(parse(&one).expect("valid").rooms[1].dm_networks(), ["lora"]);

        let many =
            format!("{MINIMAL}\n[[rooms]]\nid = \"!dm:example.org\"\ndm_inbox = [\"lora\"]\n");
        assert_eq!(
            parse(&many).expect("valid").rooms[1].dm_networks(),
            ["lora"]
        );
    }

    #[test]
    fn accepts_a_room_bridging_two_networks() {
        let raw = r#"
            [matrix]
            homeserver = "https://example.org"
            user_id = "@relay:example.org"
            access_token = "secret"

            [[networks]]
            name = "lora"
            kind = "meshtastic"
            transport = { type = "tcp", host = "192.168.1.41" }

            [[networks]]
            name = "mc"
            kind = "meshcore"
            transport = { type = "tcp", host = "192.168.1.161" }

            [[rooms]]
            id = "!room:example.org"
            routes = [
              { network = "lora", channel = 0 },
              { network = "mc", channel = 0 },
            ]
        "#;
        assert_eq!(parse(raw).expect("valid").rooms[0].routes.len(), 2);
    }

    #[test]
    fn rejects_duplicate_network_names() {
        let raw = MINIMAL.replace("[[rooms]]", "[[networks]]\nname = \"lora\"\nkind = \"meshcore\"\ntransport = { type = \"tcp\", host = \"h\" }\n\n[[rooms]]");
        let error = parse(&raw).expect_err("duplicate names must be rejected");
        assert!(
            error.to_string().contains("duplicate network name"),
            "{error}"
        );
    }

    #[test]
    fn rejects_a_transport_with_nowhere_to_connect_to() {
        for (transport, missing) in [
            (r#"{ type = "tcp", host = "" }"#, "a TCP host"),
            (r#"{ type = "serial", port = "" }"#, "a serial port"),
            (r#"{ type = "ble", address = "" }"#, "a BLE address"),
        ] {
            let raw = MINIMAL.replace(
                r#"transport = { type = "tcp", host = "192.168.1.41" }"#,
                &format!("transport = {transport}"),
            );
            let error = parse(&raw).expect_err("an empty target must be rejected");
            assert!(error.to_string().contains(missing), "{error}");
        }
    }

    #[test]
    fn rejects_routes_to_unknown_networks() {
        let raw = MINIMAL.replace("network = \"lora\"", "network = \"nope\"");
        let error = parse(&raw).expect_err("unknown network must be rejected");
        assert!(error.to_string().contains("unknown network"), "{error}");
    }

    #[test]
    fn rejects_one_channel_feeding_two_rooms() {
        let raw = format!(
            "{MINIMAL}\n[[rooms]]\nid = \"!other:example.org\"\nroutes = [{{ network = \"lora\", channel = 0 }}]\n"
        );
        let error = parse(&raw).expect_err("ambiguous mesh-to-matrix route must be rejected");
        assert!(error.to_string().contains("more than one room"), "{error}");
    }

    #[test]
    fn rejects_mixing_routes_and_dm_inbox() {
        let raw = format!("{MINIMAL}\ndm_inbox = \"lora\"\n");
        let error = parse(&raw).expect_err("a room cannot be both");
        assert!(error.to_string().contains("read-only"), "{error}");
    }

    #[test]
    fn rejects_unknown_config_keys() {
        let raw = format!("{MINIMAL}\n[[rooms]]\nid = \"!x:example.org\"\ntypo = 1\n");
        assert!(parse(&raw).is_err(), "unknown keys must be rejected");
    }

    #[test]
    fn requires_exactly_one_token_source() {
        let both = MatrixConfig {
            homeserver: "https://example.org".to_owned(),
            user_id: "@relay:example.org".to_owned(),
            access_token: Some("a".to_owned()),
            access_token_file: Some(PathBuf::from("/tmp/token")),
        };
        assert!(both.resolve_token().is_err());

        let neither = MatrixConfig {
            access_token: None,
            access_token_file: None,
            ..both.clone()
        };
        assert!(neither.resolve_token().is_err());

        assert_eq!(
            MatrixConfig {
                access_token_file: None,
                ..both
            }
            .resolve_token()
            .expect("inline token resolves"),
            "a"
        );
    }
}
