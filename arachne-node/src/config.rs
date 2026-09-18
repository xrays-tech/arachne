//! TOML config loading and validation for the Arachne node.
//!
//! The schema mirrors propsol §7 / §3.1 naming. Parsing is done at the
//! boundary: a `Config` is a **trusted, validated** value (parse, don't
//! validate) — once constructed, internal code never re-checks it. Every
//! validation failure is reported as a `ConfigError`, never a panic.
//!
//! # Raft id mapping
//!
//! `node_id` is mapped to its raft [`RaftId`] as its 1-based position in
//! `initial_cluster`. This is a *bootstrap* mapping (propsol §5.7): the
//! persisted `String` ↔ `u64` mapping is a P4-flagged open item, so the index
//! order of `initial_cluster` is the single source of truth here. M0 uses a
//! single-node cluster, so the mapping is `n1 → 1`.

use std::net::SocketAddr;
use std::path::PathBuf;

use arachne::NodeId;
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
    /// The raft tick interval in milliseconds (profile override).
    pub heartbeat_interval_ms: u64,
    /// The raft election timeout in milliseconds (profile override).
    pub election_timeout_ms: u64,
}

const DEFAULT_HEARTBEAT_MS: u64 = 100;
const DEFAULT_ELECTION_MS: u64 = 1000;

/// Raw, unvalidated TOML shape.
#[derive(Deserialize)]
struct RawConfig {
    cluster_id: String,
    node_id: String,
    listen: String,
    data_dir: String,
    http_listen: String,
    initial_cluster: Vec<String>,
    #[serde(default = "default_heartbeat")]
    heartbeat_interval_ms: u64,
    #[serde(default = "default_election")]
    election_timeout_ms: u64,
}

fn default_heartbeat() -> u64 {
    DEFAULT_HEARTBEAT_MS
}

fn default_election() -> u64 {
    DEFAULT_ELECTION_MS
}

/// Errors from parsing or validating a node config.
#[derive(Debug)]
pub enum ConfigError {
    /// The TOML could not be deserialized (missing field, wrong type, …).
    Parse(String),
    /// A required string field was empty.
    EmptyField { field: &'static str },
    /// `initial_cluster` had no entries.
    EmptyCluster,
    /// `node_id` is not a member of `initial_cluster`.
    NodeNotInCluster { node: NodeId },
    /// An address field was not a valid `host:port`.
    BadAddress { field: &'static str, value: String },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Parse(e) => write!(f, "invalid config: {e}"),
            ConfigError::EmptyField { field } => {
                write!(f, "config field `{field}` must be non-empty")
            }
            ConfigError::EmptyCluster => write!(f, "`initial_cluster` must contain at least one node"),
            ConfigError::NodeNotInCluster { node } => {
                write!(f, "`node_id` ({node}) must be a member of `initial_cluster`")
            }
            ConfigError::BadAddress { field, value } => {
                write!(f, "config field `{field}` is not a valid `host:port`: {value}")
            }
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
        .map_err(|_| ConfigError::EmptyField { field: "node_id" })?;

    // Deterministic raft id: the node's 1-based position in `initial_cluster`
    // (propsol §5.7 bootstrap mapping — see the module docs).
    let raft_id = raw
        .initial_cluster
        .iter()
        .position(|id| id == &raw.node_id)
        .map(|i| (i + 1) as u64)
        .ok_or_else(|| ConfigError::NodeNotInCluster { node: node_id.clone() })?;

    let listen = parse_addr("listen", &raw.listen)?;
    let http_listen = parse_addr("http_listen", &raw.http_listen)?;

    let initial_cluster = raw
        .initial_cluster
        .iter()
        .map(NodeId::try_new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ConfigError::EmptyField { field: "initial_cluster" })?;

    Ok(Config {
        cluster_id: raw.cluster_id,
        node_id,
        raft_id,
        listen,
        data_dir: PathBuf::from(raw.data_dir),
        http_listen,
        initial_cluster,
        heartbeat_interval_ms: raw.heartbeat_interval_ms,
        election_timeout_ms: raw.election_timeout_ms,
    })
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
        // Optional profile overrides default to the documented values.
        assert_eq!(config.heartbeat_interval_ms, DEFAULT_HEARTBEAT_MS);
        assert_eq!(config.election_timeout_ms, DEFAULT_ELECTION_MS);
    }

    #[test]
    fn honors_explicit_profile_overrides() {
        let toml = format!("heartbeat_interval_ms = 25\nelection_timeout_ms = 250\n{VALID}");
        let config = parse_config(&toml).expect("config with overrides");
        assert_eq!(config.heartbeat_interval_ms, 25);
        assert_eq!(config.election_timeout_ms, 250);
    }

    #[test]
    fn maps_node_id_to_its_cluster_index() {
        let toml = VALID.replace("initial_cluster = [\"n1\"]", "initial_cluster = [\"a\", \"n1\", \"c\"]");
        let config = parse_config(&toml).expect("config");
        assert_eq!(config.raft_id, 2, "n1 is the second (1-based) member");
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
}
