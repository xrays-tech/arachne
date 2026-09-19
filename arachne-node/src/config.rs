//! TOML config loading and validation for the Arachne node.
//!
//! The schema mirrors propsol §7 / §3.1 naming. Parsing is done at the
//! boundary: a `Config` is a **trusted, validated** value (parse, don't
//! validate) — once constructed, internal code never re-checks it. Every
//! validation failure is reported as a `ConfigError`, never a panic.
//!
//! # Profile
//!
//! A `profile` (`"lan"` / `"wan"`, default `"lan"`) selects the propsol §7
//! preset. Any individual field may be overridden with a top-level key of the
//! same name (e.g. `heartbeat_interval_ms`). The profile plus overrides is
//! folded into a [`ProfileConfig`] and **validated at parse time** — an
//! invalid combination (e.g. `rpc_timeout_ms >= election_timeout_ms`) is
//! rejected before any node is built.
//!
//! # Raft id mapping
//!
//! `node_id` is mapped to its raft id as its 1-based position in
//! `initial_cluster`. This is a *bootstrap* mapping (propsol §5.7): the
//! persisted `String` ↔ `u64` mapping is a P4-flagged open item, so the index
//! order of `initial_cluster` is the single source of truth here. The position
//! matches on the node id — the part of the entry before any `=` — so an entry
//! with an inline address (`"n1=127.0.0.1:7000"`) maps to the same id as a
//! bare `"n1"`. M0 uses a single-node cluster, so the mapping is `n1 → 1`.
//!
//! # Cluster addresses
//!
//! Each `initial_cluster` entry is either a bare `"<node_id>"` or
//! `"<node_id>=<ip:port>"`. The bare form is sufficient for the node itself;
//! every *other* member must carry an explicit `id=addr` so that it is
//! reachable.
//!
//! The parsed `Config` exposes `addresses: HashMap<NodeId, SocketAddr>`, which
//! maps **every** member to its listen address under this invariant:
//!
//! * The self entry is **always** `listen`. If the self entry carries an
//!   inline address, it must equal `listen`, else `SelfAddressMismatch`.
//! * Every non-self member **requires** an inline address, else
//!   `MissingMemberAddress`.
//!
//! A single-node cluster (`initial_cluster = ["n1"]`) therefore stays valid
//! with no inline address (the l4/ scripts rely on this), while a multi-node
//! cluster must spell out each peer's address.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use arachne::NodeId;
use arachne::{Profile, ProfileConfig};
use serde::Deserialize;

/// The parsed and validated node configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// The cluster identifier.
    pub cluster_id: String,
    /// This node's identifier.
    pub node_id: NodeId,
    /// This node's deterministic raft id (index in `initial_cluster` + 1).
    pub raft_id: u64,
    /// The raft listener address (reserved for the M1 tonic transport; unused
    /// by the M0 placeholder transport).
    pub listen: SocketAddr,
    /// The on-disk WAL data directory.
    pub data_dir: PathBuf,
    /// The HTTP listener address for `/readyz` and `/metrics`.
    pub http_listen: SocketAddr,
    /// The bootstrap cluster membership (this node must be a member).
    pub initial_cluster: Vec<NodeId>,
    /// Every cluster member mapped to its listen address. The self entry is
    /// always `listen`; every other member carries the address declared in
    /// `initial_cluster` (see the module docs' "Cluster addresses"). A peer's
    /// inline address must be a concrete `ip:port` (a usable dial target): the
    /// self entry may be `:0` (its real bound port is learned at bind), but a
    /// peer `:0` would be a useless dial target.
    pub addresses: HashMap<NodeId, SocketAddr>,
    /// The selected propsol §7 preset (`lan` / `wan`).
    pub profile: Profile,
    /// The full, validated tuning table (profile + overrides).
    pub profile_config: ProfileConfig,
    /// The effective raft heartbeat interval in milliseconds (profile + any
    /// override). Drives the node's tick period.
    pub heartbeat_interval_ms: u64,
    /// The effective raft election timeout in milliseconds (profile + any
    /// override).
    pub election_timeout_ms: u64,
}

const DEFAULT_PROFILE: &str = "lan";

/// Raw, unvalidated TOML shape.
#[derive(Deserialize)]
struct RawConfig {
    cluster_id: String,
    node_id: String,
    listen: String,
    data_dir: String,
    http_listen: String,
    initial_cluster: Vec<String>,
    #[serde(default = "default_profile")]
    profile: String,
    /// Per-field overrides; absent (None) means "use the profile value".
    #[serde(default)]
    heartbeat_interval_ms: Option<u64>,
    #[serde(default)]
    election_timeout_ms: Option<u64>,
    #[serde(default)]
    rpc_timeout_ms: Option<u64>,
}

fn default_profile() -> String {
    DEFAULT_PROFILE.to_string()
}

/// Errors from parsing or validating a node config.
#[derive(Debug)]
pub enum ConfigError {
    /// The TOML could not be deserialized (missing field, wrong type, …).
    Parse(String),
    /// A required string field was empty.
    EmptyField { field: &'static str },
    /// A `node_id` or an `initial_cluster` member is not a valid node
    /// identifier (it failed `NodeId::try_new`, i.e. it is empty or otherwise
    /// malformed). Distinct from [`ConfigError::EmptyField`], which is reserved
    /// for the explicitly-emptied top-level fields.
    InvalidNodeId { value: String },
    /// `initial_cluster` had no entries.
    EmptyCluster,
    /// `initial_cluster` lists the same node more than once (two entries would
    /// collide on the same raft id under the index-based bootstrap mapping).
    DuplicateClusterMember { value: NodeId },
    /// `node_id` is not a member of `initial_cluster`.
    NodeNotInCluster { node: NodeId },
    /// An address field was not a valid `IP:port` (only IP literals parse).
    BadAddress { field: &'static str, value: String },
    /// An `initial_cluster` entry had an inline `id=addr` whose address part
    /// was not a valid `IP:port` (only IP literals parse).
    BadClusterAddress { value: String },
    /// A non-self cluster member had no inline `id=addr`; every member other
    /// than this node must be reachable, so each requires an explicit address.
    MissingMemberAddress { node: NodeId },
    /// The self entry in `initial_cluster` carried an inline address that did
    /// not equal `listen` (the self address is always `listen`).
    SelfAddressMismatch { listen: SocketAddr, configured: SocketAddr },
    /// The `profile` field was not `lan` or `wan`.
    UnknownProfile { value: String },
    /// A profile field (or a profile + override combination) failed validation.
    ProfileInvalid { message: String },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Parse(e) => write!(f, "invalid config: {e}"),
            ConfigError::EmptyField { field } => {
                write!(f, "config field `{field}` must be non-empty")
            }
            ConfigError::InvalidNodeId { value } => {
                write!(f, "invalid node id: `{value}` (a node id must be a non-empty string)")
            }
            ConfigError::EmptyCluster => write!(f, "`initial_cluster` must contain at least one node"),
            ConfigError::DuplicateClusterMember { value } => {
                write!(f, "`initial_cluster` lists the same node more than once: `{value}`")
            }
            ConfigError::NodeNotInCluster { node } => {
                write!(f, "`node_id` ({node}) must be a member of `initial_cluster`")
            }
            ConfigError::BadAddress { field, value } => {
                write!(f, "config field `{field}` is not a valid `IP:port`: {value}")
            }
            ConfigError::BadClusterAddress { value } => {
                write!(f, "`initial_cluster` entry has a bad `IP:port` address: `{value}`")
            }
            ConfigError::MissingMemberAddress { node } => {
                write!(f, "cluster member `{node}` has no address (a non-self member must be `id=addr`)")
            }
            ConfigError::SelfAddressMismatch { listen, configured } => {
                write!(
                    f,
                    "self address `{configured}` in `initial_cluster` must equal `listen` (`{listen}`)"
                )
            }
            ConfigError::UnknownProfile { value } => {
                write!(f, "unknown profile `{value}` (expected `lan` or `wan`)")
            }
            ConfigError::ProfileInvalid { message } => write!(f, "invalid profile config: {message}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<toml::de::Error> for ConfigError {
    fn from(e: toml::de::Error) -> Self {
        ConfigError::Parse(e.to_string())
    }
}

/// Parse and validate a node config from a TOML string.
pub fn parse_config(toml_text: &str) -> Result<Config, ConfigError> {
    let raw: RawConfig = toml::from_str(toml_text)?;

    // Non-empty required string fields (early exit on the first failure).
    if raw.cluster_id.is_empty() {
        return Err(ConfigError::EmptyField { field: "cluster_id" });
    }
    if raw.node_id.is_empty() {
        return Err(ConfigError::EmptyField { field: "node_id" });
    }
    if raw.data_dir.is_empty() {
        return Err(ConfigError::EmptyField { field: "data_dir" });
    }
    if raw.initial_cluster.is_empty() {
        return Err(ConfigError::EmptyCluster);
    }

    let node_id = NodeId::try_new(raw.node_id.clone())
        .map_err(|_| ConfigError::InvalidNodeId { value: raw.node_id.clone() })?;

    let listen = parse_addr("listen", &raw.listen)?;
    let http_listen = parse_addr("http_listen", &raw.http_listen)?;

    // Parse each `initial_cluster` entry into `(node_id, optional inline
    // address)`. An entry is either `"<node_id>"` or `"<node_id>=<ip:port>"`.
    let members: Vec<(NodeId, Option<SocketAddr>)> = raw
        .initial_cluster
        .iter()
        .map(|entry| parse_cluster_entry(entry))
        .collect::<Result<Vec<_>, _>>()?;

    // Deterministic raft id: the node's 1-based position in `initial_cluster`
    // (propsol §5.7 bootstrap mapping — see the module docs). The position
    // matches on the node id, so an inline address does not shift it.
    let raft_id = members
        .iter()
        .position(|(id, _)| id == &node_id)
        .map(|i| (i + 1) as u64)
        .ok_or_else(|| ConfigError::NodeNotInCluster { node: node_id.clone() })?;

    // Reject duplicate members: two entries for the same node would collide on
    // the same raft id under the index-based bootstrap mapping.
    {
        let mut seen = std::collections::HashSet::new();
        for (id, _) in &members {
            if !seen.insert(id.clone()) {
                return Err(ConfigError::DuplicateClusterMember { value: id.clone() });
            }
        }
    }

    // Build the member → address map (the invariant described in the module
    // docs). The self entry is always `listen`; every other member requires an
    // explicit inline address so that it is reachable.
    let mut addresses: HashMap<NodeId, SocketAddr> = HashMap::new();
    for (id, addr) in &members {
        if id == &node_id {
            if let Some(configured) = addr {
                if *configured != listen {
                    return Err(ConfigError::SelfAddressMismatch {
                        listen,
                        configured: *configured,
                    });
                }
            }
            addresses.insert(id.clone(), listen);
        } else {
            let configured =
                addr.ok_or_else(|| ConfigError::MissingMemberAddress { node: id.clone() })?;
            addresses.insert(id.clone(), configured);
        }
    }

    // Select the profile preset and apply per-field overrides, then validate
    // the folded result at the boundary (parse, don't validate).
    let profile = Profile::parse_name(&raw.profile)
        .map_err(|e| ConfigError::UnknownProfile { value: e.to_string() })?;
    let mut profile_config = profile.config();
    if let Some(v) = raw.heartbeat_interval_ms {
        profile_config.heartbeat_interval_ms = v;
    }
    if let Some(v) = raw.election_timeout_ms {
        profile_config.election_timeout_ms = v;
    }
    if let Some(v) = raw.rpc_timeout_ms {
        profile_config.rpc_timeout_ms = v;
    }
    profile_config
        .validate()
        .map_err(|e| ConfigError::ProfileInvalid { message: e.to_string() })?;

    let initial_cluster: Vec<NodeId> = members.iter().map(|(id, _)| id.clone()).collect();

    Ok(Config {
        cluster_id: raw.cluster_id,
        node_id,
        raft_id,
        listen,
        data_dir: PathBuf::from(raw.data_dir),
        http_listen,
        initial_cluster,
        addresses,
        profile,
        heartbeat_interval_ms: profile_config.heartbeat_interval_ms,
        election_timeout_ms: profile_config.election_timeout_ms,
        profile_config,
    })
}

/// Parse a single `initial_cluster` entry into `(node_id, optional address)`.
///
/// An entry is either a bare `"<node_id>"` (no inline address) or
/// `"<node_id>=<ip:port>"`. The entry is split on the first `=`; the node id
/// (before it) is validated with [`NodeId::try_new`] and the address part (if
/// present) must parse as an IP-literal `IP:port`.
fn parse_cluster_entry(entry: &str) -> Result<(NodeId, Option<SocketAddr>), ConfigError> {
    match entry.split_once('=') {
        None => {
            let id = NodeId::try_new(entry.to_string())
                .map_err(|_| ConfigError::InvalidNodeId { value: entry.to_string() })?;
            Ok((id, None))
        }
        Some((id_part, addr_part)) => {
            let id = NodeId::try_new(id_part.to_string())
                .map_err(|_| ConfigError::InvalidNodeId { value: id_part.to_string() })?;
            let addr = addr_part
                .parse::<SocketAddr>()
                .map_err(|_| ConfigError::BadClusterAddress { value: entry.to_string() })?;
            Ok((id, Some(addr)))
        }
    }
}

fn parse_addr(field: &'static str, value: &str) -> Result<SocketAddr, ConfigError> {
    value
        .parse::<SocketAddr>()
        .map_err(|_| ConfigError::BadAddress { field, value: value.to_string() })
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
        cluster_id = "demo"
        node_id = "n1"
        listen = "127.0.0.1:7000"
        data_dir = "/var/lib/arachne"
        http_listen = "127.0.0.1:8000"
        initial_cluster = ["n1"]
    "#;

    #[test]
    fn parses_a_valid_config_with_defaults() {
        let config = parse_config(VALID).expect("valid config");
        assert_eq!(config.cluster_id, "demo");
        assert_eq!(config.node_id.as_str(), "n1");
        assert_eq!(config.raft_id, 1);
        assert_eq!(config.data_dir, std::path::Path::new("/var/lib/arachne"));
        assert_eq!(config.listen, "127.0.0.1:7000".parse().expect("addr"));
        assert_eq!(config.http_listen, "127.0.0.1:8000".parse().expect("addr"));
        assert_eq!(config.initial_cluster.len(), 1);
        // The single-node cluster maps to exactly {n1: listen}.
        assert_eq!(config.addresses.len(), 1);
        assert_eq!(
            config.addresses[&NodeId::new("n1")],
            "127.0.0.1:7000".parse().expect("addr")
        );
        // No `profile` key defaults to `lan`; the effective tick timings are
        // the Lan preset values (propsol §7).
        assert_eq!(config.profile, Profile::Lan);
        assert_eq!(config.heartbeat_interval_ms, 100);
        assert_eq!(config.election_timeout_ms, 1000);
    }

    #[test]
    fn honors_explicit_profile_overrides() {
        let toml = format!(
            "heartbeat_interval_ms = 25\nelection_timeout_ms = 250\nrpc_timeout_ms = 100\n{VALID}"
        );
        let config = parse_config(&toml).expect("config with overrides");
        assert_eq!(config.heartbeat_interval_ms, 25);
        assert_eq!(config.election_timeout_ms, 250);
        assert_eq!(config.profile_config.rpc_timeout_ms, 100);
    }

    #[test]
    fn maps_node_id_to_its_cluster_index() {
        let toml = VALID
            .replace(
                "initial_cluster = [\"n1\"]",
                "initial_cluster = [\"a=127.0.0.1:7001\", \"n1=127.0.0.1:7002\", \"c=127.0.0.1:7003\"]",
            )
            .replace("listen = \"127.0.0.1:7000\"", "listen = \"127.0.0.1:7002\"");
        let config = parse_config(&toml).expect("config");
        assert_eq!(config.raft_id, 2, "n1 is the second (1-based) member");
        // The self entry (n1) uses `listen` (7002); every peer carries its
        // declared address.
        assert_eq!(config.addresses.len(), 3);
        assert_eq!(
            config.addresses[&NodeId::new("a")],
            "127.0.0.1:7001".parse().expect("addr")
        );
        assert_eq!(
            config.addresses[&NodeId::new("n1")],
            "127.0.0.1:7002".parse().expect("addr")
        );
        assert_eq!(
            config.addresses[&NodeId::new("c")],
            "127.0.0.1:7003".parse().expect("addr")
        );
    }

    #[test]
    fn parses_a_multi_node_cluster_with_addresses() {
        let toml = r#"
            cluster_id = "demo"
            node_id = "n1"
            listen = "127.0.0.1:7000"
            data_dir = "/var/lib/arachne"
            http_listen = "127.0.0.1:8000"
            initial_cluster = ["n1=127.0.0.1:7000", "n2=127.0.0.1:7001", "n3=127.0.0.1:7002"]
        "#;
        let config = parse_config(toml).expect("multi-node config");
        assert_eq!(config.raft_id, 1);
        assert_eq!(config.initial_cluster.len(), 3);
        assert_eq!(config.addresses.len(), 3);
        assert_eq!(
            config.addresses[&NodeId::new("n1")],
            "127.0.0.1:7000".parse().expect("addr")
        );
        assert_eq!(
            config.addresses[&NodeId::new("n2")],
            "127.0.0.1:7001".parse().expect("addr")
        );
        assert_eq!(
            config.addresses[&NodeId::new("n3")],
            "127.0.0.1:7002".parse().expect("addr")
        );
    }

    #[test]
    fn single_node_without_address_is_valid() {
        // A bare self entry (no `id=addr`) is valid: the self address is
        // always `listen`, so no inline address is required for a single node.
        let config = parse_config(VALID).expect("single node, no address");
        assert_eq!(config.initial_cluster.len(), 1);
        assert_eq!(config.addresses.len(), 1);
        assert_eq!(config.addresses[&config.node_id], config.listen);
    }

    #[test]
    fn rejects_a_missing_node_id() {
        let toml = VALID.replace("node_id = \"n1\"\n", "");
        let err = parse_config(&toml).expect_err("missing node_id must fail");
        assert!(err.to_string().contains("node_id"), "error: {err}");
    }

    #[test]
    fn rejects_a_node_not_in_the_cluster() {
        let toml = VALID.replace("node_id = \"n1\"", "node_id = \"ghost\"");
        let err = parse_config(&toml).expect_err("node not in cluster must fail");
        assert!(
            matches!(err, ConfigError::NodeNotInCluster { .. }),
            "error: {err}"
        );
    }

    #[test]
    fn rejects_an_empty_cluster() {
        let toml = VALID.replace("initial_cluster = [\"n1\"]", "initial_cluster = []");
        let err = parse_config(&toml).expect_err("empty cluster must fail");
        assert!(matches!(err, ConfigError::EmptyCluster), "error: {err}");
    }

    #[test]
    fn rejects_an_unparseable_address() {
        let toml = VALID.replace("listen = \"127.0.0.1:7000\"", "listen = \"not-an-addr\"");
        let err = parse_config(&toml).expect_err("bad listen address must fail");
        assert!(
            matches!(err, ConfigError::BadAddress { .. }),
            "error: {err}"
        );
        // Only IP literals parse, so the message says IP:port (not host:port).
        assert!(err.to_string().contains("IP:port"), "error: {err}");
    }

    #[test]
    fn rejects_a_duplicate_cluster_member() {
        let toml = VALID.replace("initial_cluster = [\"n1\"]", "initial_cluster = [\"n1\", \"n1\"]");
        let err = parse_config(&toml).expect_err("duplicate member must fail");
        assert!(
            matches!(err, ConfigError::DuplicateClusterMember { .. }),
            "error: {err}"
        );
    }

    #[test]
    fn rejects_an_invalid_cluster_member_id() {
        let toml = VALID.replace("initial_cluster = [\"n1\"]", "initial_cluster = [\"n1\", \"\"]");
        let err = parse_config(&toml).expect_err("empty member id must fail");
        assert!(
            matches!(err, ConfigError::InvalidNodeId { .. }),
            "error: {err}"
        );
    }

    #[test]
    fn rejects_a_missing_peer_address() {
        let toml = VALID.replace(
            "initial_cluster = [\"n1\"]",
            "initial_cluster = [\"n1\", \"n2\"]",
        );
        let err = parse_config(&toml).expect_err("a peer without an address must fail");
        assert!(
            matches!(err, ConfigError::MissingMemberAddress { .. }),
            "error: {err}"
        );
    }

    #[test]
    fn rejects_self_address_mismatching_listen() {
        let toml = VALID.replace(
            "initial_cluster = [\"n1\"]",
            "initial_cluster = [\"n1=127.0.0.1:9999\"]",
        );
        let err = parse_config(&toml).expect_err("self address != listen must fail");
        assert!(
            matches!(err, ConfigError::SelfAddressMismatch { .. }),
            "error: {err}"
        );
    }

    #[test]
    fn rejects_a_bad_cluster_address() {
        let toml = VALID.replace(
            "initial_cluster = [\"n1\"]",
            "initial_cluster = [\"n1\", \"n2=not-an-addr\"]",
        );
        let err = parse_config(&toml).expect_err("a bad cluster address must fail");
        assert!(
            matches!(err, ConfigError::BadClusterAddress { .. }),
            "error: {err}"
        );
        // Only IP literals parse, so the message says IP:port (not host:port).
        assert!(err.to_string().contains("IP:port"), "error: {err}");
    }

    #[test]
    fn rejects_an_empty_field() {
        let toml = VALID.replace("cluster_id = \"demo\"", "cluster_id = \"\"");
        let err = parse_config(&toml).expect_err("empty cluster_id must fail");
        assert!(
            matches!(err, ConfigError::EmptyField { field: "cluster_id" }),
            "error: {err}"
        );
    }

    #[test]
    fn profile_wan_yields_wan_defaults() {
        let toml = format!("profile = \"wan\"\n{VALID}");
        let config = parse_config(&toml).expect("wan profile");
        assert_eq!(config.profile, Profile::Wan);
        let pc = &config.profile_config;
        assert_eq!(pc.heartbeat_interval_ms, 500);
        assert_eq!(pc.election_timeout_ms, 2500);
        assert_eq!(pc.rpc_timeout_ms, 2000);
        assert_eq!(pc.max_inflight_bytes, 1024 * 1024);
        assert_eq!(pc.snapshot_threshold_bytes, 16 * 1024 * 1024);
        // Q2: read_index_timeout = 2x election timeout.
        assert_eq!(pc.read_index_timeout_ms, 5000);
        // Effective tick fields mirror the profile.
        assert_eq!(config.heartbeat_interval_ms, 500);
        assert_eq!(config.election_timeout_ms, 2500);
    }

    #[test]
    fn profile_override_wins_over_preset() {
        let toml = format!("profile = \"wan\"\nelection_timeout_ms = 3000\n{VALID}");
        let config = parse_config(&toml).expect("override config");
        assert_eq!(config.profile, Profile::Wan);
        // The override wins over the Wan preset's 2500.
        assert_eq!(config.profile_config.election_timeout_ms, 3000);
        // An untouched preset field is preserved.
        assert_eq!(config.profile_config.heartbeat_interval_ms, 500);
    }

    #[test]
    fn rejects_an_unknown_profile() {
        let toml = format!("profile = \"mars\"\n{VALID}");
        let err = parse_config(&toml).expect_err("unknown profile must fail");
        assert!(
            matches!(err, ConfigError::UnknownProfile { .. }),
            "error: {err}"
        );
    }

    #[test]
    fn rejects_an_invalid_profile_combination() {
        // `rpc_timeout_ms` must be `< election_timeout_ms`; a value at/above
        // the heartbeat (and thus the election) is rejected at parse time.
        let toml = format!("profile = \"wan\"\nrpc_timeout_ms = 3000\n{VALID}");
        let err = parse_config(&toml).expect_err("rpc >= election must be rejected");
        assert!(
            matches!(err, ConfigError::ProfileInvalid { .. }),
            "error: {err}"
        );
        assert!(
            err.to_string().contains("rpc_timeout_ms"),
            "error: {err}"
        );
    }
}
