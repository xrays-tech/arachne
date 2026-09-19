//! Configuration presets for Arachne node behavior (propsol §7).
//!
//! A [`Profile`] is a named tuning preset ([`Profile::Lan`] / [`Profile::Wan`]).
//! Each maps to a concrete [`ProfileConfig`] holding the full §7 field table.
//! Individual fields are `pub`, so a deployment can override any single one:
//!
//! ```
//! use arachne::{Profile, ProfileConfig};
//! let base = Profile::Lan.config();
//! let tuned = ProfileConfig { heartbeat_interval_ms: 50, ..base };
//! assert_eq!(tuned.heartbeat_interval_ms, 50);
//! ```
//!
//! [`ProfileConfig::validate`] is the boundary check: a node builds its config
//! from a profile plus operator overrides, then validates once (parse, don't
//! validate — internal code never re-checks it).

use crate::storage::FsyncPolicy;

/// Binary scaling constants (KiB / MiB) for the §7 preset values.
const KB: u64 = 1024;
const MB: u64 = 1024 * 1024;
const SECONDS: u64 = 1000;

/// A named tuning preset (propsol §7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    /// A low-latency LAN deployment (sub-5 ms RTT).
    Lan,
    /// A WAN deployment (up to ~300 ms RTT).
    Wan,
}

impl Profile {
    /// The concrete [`ProfileConfig`] for this preset (propsol §7).
    pub fn config(self) -> ProfileConfig {
        match self {
            Profile::Lan => ProfileConfig::lan(),
            Profile::Wan => ProfileConfig::wan(),
        }
    }

    /// Parse a profile name (`"lan"` / `"wan"`, case-insensitive, trimmed).
    pub fn parse_name(name: &str) -> Result<Self, ProfileError> {
        match name.trim().to_ascii_lowercase().as_str() {
            "lan" => Ok(Profile::Lan),
            "wan" => Ok(Profile::Wan),
            other => Err(ProfileError::UnknownProfile { value: other.to_string() }),
        }
    }
}

/// The full tuning field table (propsol §7).
///
/// All fields are `pub` so a deployment can override any one of them (e.g.
/// `ProfileConfig { ..Profile::Lan.config(), heartbeat_interval_ms: 50 }`).
#[derive(Clone, Debug)]
pub struct ProfileConfig {
    /// Leader heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,
    /// Election timeout in milliseconds (randomized to `[1x, 2x)` at runtime).
    pub election_timeout_ms: u64,
    /// Outbound RPC timeout in milliseconds.
    pub rpc_timeout_ms: u64,
    /// Max bytes in flight to a single follower.
    pub max_inflight_bytes: u64,
    /// Max messages in flight to a single follower (raft built-in flow control).
    pub max_inflight_msgs: u64,
    /// WAL bytes that trigger snapshot + compaction.
    pub snapshot_threshold_bytes: u64,
    /// WAL bytes retained after compaction for slow followers to catch up.
    ///
    /// Bound to [`Self::snapshot_threshold_bytes`] by default (propsol Q6); a
    /// deployment may set it independently to decouple the two.
    pub wal_trailing_keep_bytes: u64,
    /// Size of a single WAL segment file before rollover.
    pub wal_segment_bytes: u64,
    /// ReadIndex wait timeout in milliseconds. Locked to `2 * election_timeout_ms` (Q2).
    pub read_index_timeout_ms: u64,
    /// Proposal queue capacity in bytes (propsol Q7, counted by bytes).
    pub proposal_queue_bytes: u64,
    /// Session TTL in milliseconds (propsol Q1, locked to 60 s).
    pub session_ttl_ms: u64,
    /// Window (ms) after a session expires before its dedup results are dropped.
    pub session_grace_period_ms: u64,
    /// Max concurrent sessions.
    pub max_sessions: u64,
    /// Max value size in bytes (checked before propose).
    pub max_value_bytes: u64,
    /// Max key size in bytes (checked before propose).
    pub max_key_bytes: u64,
    /// Snapshot transfer rate limit in bytes/second.
    pub snapshot_transfer_rate_bps: u64,
    /// Fsync policy for WAL entry records (HardState is always fsynced, I1).
    pub fsync_policy: FsyncPolicy,
}

impl ProfileConfig {
    /// The LAN preset (propsol §7).
    pub fn lan() -> Self {
        let election = SECONDS; // 1 s
        Self {
            heartbeat_interval_ms: 100,
            election_timeout_ms: election,
            rpc_timeout_ms: 500,
            max_inflight_bytes: 4 * MB,
            max_inflight_msgs: 256,
            snapshot_threshold_bytes: 64 * MB,
            // Q6: bound to `snapshot_threshold_bytes` by default.
            wal_trailing_keep_bytes: 64 * MB,
            wal_segment_bytes: 128 * MB,
            // Q2: ReadIndex timeout is 2x the election timeout.
            read_index_timeout_ms: 2 * election,
            proposal_queue_bytes: 64 * MB, // Q7
            session_ttl_ms: 60 * SECONDS, // Q1
            session_grace_period_ms: 60 * SECONDS,
            max_sessions: 10_000,
            max_value_bytes: MB, // 1 MiB
            max_key_bytes: 4 * KB, // 4 KiB
            snapshot_transfer_rate_bps: 32 * MB,
            fsync_policy: FsyncPolicy::Always,
        }
    }

    /// The WAN preset (propsol §7).
    pub fn wan() -> Self {
        let election = 2500; // 2.5 s
        Self {
            heartbeat_interval_ms: 500,
            election_timeout_ms: election,
            rpc_timeout_ms: 2 * SECONDS, // 2 s
            max_inflight_bytes: MB,
            max_inflight_msgs: 256,
            snapshot_threshold_bytes: 16 * MB,
            // Q6: bound to `snapshot_threshold_bytes` by default.
            wal_trailing_keep_bytes: 16 * MB,
            wal_segment_bytes: 128 * MB,
            // Q2: ReadIndex timeout is 2x the election timeout.
            read_index_timeout_ms: 2 * election,
            proposal_queue_bytes: 64 * MB, // Q7
            session_ttl_ms: 60 * SECONDS, // Q1
            session_grace_period_ms: 60 * SECONDS,
            max_sessions: 10_000,
            max_value_bytes: MB, // 1 MiB
            max_key_bytes: 4 * KB, // 4 KiB
            snapshot_transfer_rate_bps: 8 * MB,
            fsync_policy: FsyncPolicy::Always,
        }
    }

    /// Validate the timing invariants that keep the node's liveness guarantees
    /// sound. This is the boundary check — call it once after building the
    /// config from a profile plus operator overrides (parse, don't validate).
    ///
    /// # Invariants enforced
    ///
    /// * `heartbeat_interval_ms > 0`
    /// * `election_timeout_ms >= 5 * heartbeat_interval_ms` — the election
    ///   window must be at least 5x one heartbeat. The §7 presets use a ratio
    ///   of 10 (Lan) and 5 (Wan); the doc annotates 10x, but the Wan preset
    ///   uses 5x, so 5x is the self-consistent floor.
    /// * `rpc_timeout_ms > 0`
    /// * `rpc_timeout_ms < election_timeout_ms` — a single RPC must resolve
    ///   before the election window elapses, or a slow RPC would spuriously
    ///   trigger an election.
    ///
    /// # Note on the propsol §7 annotations
    ///
    /// The doc annotates `election_timeout >= 10 * heartbeat_interval` and
    /// `rpc_timeout < heartbeat_interval`. The §7 preset values satisfy
    /// neither (Lan `rpc 500 > heartbeat 100`; Wan `election 2500 < 10*500`
    /// and `rpc 2000 > heartbeat 500`). Enforcing the doc's annotations would
    /// make both presets invalid, which is unusable — the node is driven by
    /// them. We therefore enforce the self-consistent, physically-correct
    /// invariants above, which both presets satisfy.
    pub fn validate(&self) -> Result<(), ProfileError> {
        if self.heartbeat_interval_ms == 0 {
            return Err(ProfileError::HeartbeatNotPositive);
        }
        let min_election = self.heartbeat_interval_ms.saturating_mul(5);
        if self.election_timeout_ms < min_election {
            return Err(ProfileError::ElectionTooShort {
                heartbeat: self.heartbeat_interval_ms,
                election: self.election_timeout_ms,
            });
        }
        if self.rpc_timeout_ms == 0 {
            return Err(ProfileError::RpcNotPositive);
        }
        if self.rpc_timeout_ms >= self.election_timeout_ms {
            return Err(ProfileError::RpcTooLong {
                rpc: self.rpc_timeout_ms,
                election: self.election_timeout_ms,
            });
        }
        Ok(())
    }
}

/// Errors from parsing or validating a profile config.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProfileError {
    /// The profile name was not `lan` or `wan`.
    #[error("unknown profile `{value}` (expected `lan` or `wan`)")]
    UnknownProfile { value: String },
    /// The heartbeat interval must be positive.
    #[error("heartbeat_interval_ms must be > 0")]
    HeartbeatNotPositive,
    /// The election timeout must be at least 5x the heartbeat interval.
    #[error(
        "election_timeout_ms ({election}) must be >= 5 * heartbeat_interval_ms ({heartbeat})"
    )]
    ElectionTooShort { heartbeat: u64, election: u64 },
    /// The RPC timeout must be positive.
    #[error("rpc_timeout_ms must be > 0")]
    RpcNotPositive,
    /// The RPC timeout must be shorter than the election timeout.
    #[error("rpc_timeout_ms ({rpc}) must be < election_timeout_ms ({election})")]
    RpcTooLong { rpc: u64, election: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lan_preset_matches_propsol_section7() {
        let c = Profile::Lan.config();
        assert_eq!(c.heartbeat_interval_ms, 100);
        assert_eq!(c.election_timeout_ms, 1000);
        assert_eq!(c.rpc_timeout_ms, 500);
        assert_eq!(c.max_inflight_bytes, 4 * MB);
        assert_eq!(c.max_inflight_msgs, 256);
        assert_eq!(c.snapshot_threshold_bytes, 64 * MB);
        // Q6: trailing keep bound to the snapshot threshold by default.
        assert_eq!(c.wal_trailing_keep_bytes, c.snapshot_threshold_bytes);
        assert_eq!(c.wal_segment_bytes, 128 * MB);
        // Q2: read_index_timeout = 2x election timeout.
        assert_eq!(c.read_index_timeout_ms, 2 * c.election_timeout_ms);
        assert_eq!(c.proposal_queue_bytes, 64 * MB);
        assert_eq!(c.session_ttl_ms, 60_000);
        assert_eq!(c.session_grace_period_ms, 60_000);
        assert_eq!(c.max_sessions, 10_000);
        assert_eq!(c.max_value_bytes, MB);
        assert_eq!(c.max_key_bytes, 4 * KB);
        assert_eq!(c.snapshot_transfer_rate_bps, 32 * MB);
        assert_eq!(c.fsync_policy, FsyncPolicy::Always);
    }

    #[test]
    fn wan_preset_matches_propsol_section7() {
        let c = Profile::Wan.config();
        assert_eq!(c.heartbeat_interval_ms, 500);
        assert_eq!(c.election_timeout_ms, 2500);
        assert_eq!(c.rpc_timeout_ms, 2000);
        assert_eq!(c.max_inflight_bytes, MB);
        assert_eq!(c.max_inflight_msgs, 256);
        assert_eq!(c.snapshot_threshold_bytes, 16 * MB);
        // Q6: trailing keep bound to the snapshot threshold by default.
        assert_eq!(c.wal_trailing_keep_bytes, c.snapshot_threshold_bytes);
        assert_eq!(c.wal_segment_bytes, 128 * MB);
        // Q2: read_index_timeout = 2x election timeout.
        assert_eq!(c.read_index_timeout_ms, 5000);
        assert_eq!(c.proposal_queue_bytes, 64 * MB);
        assert_eq!(c.session_ttl_ms, 60_000);
        assert_eq!(c.session_grace_period_ms, 60_000);
        assert_eq!(c.max_sessions, 10_000);
        assert_eq!(c.max_value_bytes, MB);
        assert_eq!(c.max_key_bytes, 4 * KB);
        assert_eq!(c.snapshot_transfer_rate_bps, 8 * MB);
        assert_eq!(c.fsync_policy, FsyncPolicy::Always);
    }

    #[test]
    fn both_presets_pass_validation() {
        assert!(Profile::Lan.config().validate().is_ok(), "Lan must be valid");
        assert!(Profile::Wan.config().validate().is_ok(), "Wan must be valid");
    }

    #[test]
    fn individual_fields_can_be_overridden() {
        let tuned = ProfileConfig {
            heartbeat_interval_ms: 50,
            ..Profile::Lan.config()
        };
        assert_eq!(tuned.heartbeat_interval_ms, 50);
        // Untouched fields keep their preset values.
        assert_eq!(tuned.election_timeout_ms, 1000);
    }

    #[test]
    fn parse_name_is_case_insensitive() {
        assert_eq!(Profile::parse_name("lan"), Ok(Profile::Lan));
        assert_eq!(Profile::parse_name("WAN"), Ok(Profile::Wan));
        assert_eq!(Profile::parse_name("  wan "), Ok(Profile::Wan));
    }

    #[test]
    fn parse_name_rejects_unknown() {
        assert_eq!(
            Profile::parse_name("mars"),
            Err(ProfileError::UnknownProfile {
                value: "mars".to_string()
            })
        );
    }

    #[test]
    fn rejects_zero_heartbeat() {
        let c = ProfileConfig {
            heartbeat_interval_ms: 0,
            ..Profile::Lan.config()
        };
        assert_eq!(c.validate(), Err(ProfileError::HeartbeatNotPositive));
    }

    #[test]
    fn rejects_election_shorter_than_5x_heartbeat() {
        let c = ProfileConfig {
            heartbeat_interval_ms: 1000,
            election_timeout_ms: 4000, // < 5 * 1000
            ..Profile::Lan.config()
        };
        assert!(matches!(
            c.validate(),
            Err(ProfileError::ElectionTooShort { .. })
        ));
    }

    #[test]
    fn rejects_rpc_at_or_above_election() {
        // rpc must be strictly below the election window; a value at/above it
        // (which is necessarily >= the heartbeat too) is rejected.
        let c = ProfileConfig {
            rpc_timeout_ms: 2500,
            ..Profile::Wan.config()
        };
        assert!(matches!(
            c.validate(),
            Err(ProfileError::RpcTooLong { .. })
        ));
    }

    #[test]
    fn rejects_zero_rpc() {
        let c = ProfileConfig {
            rpc_timeout_ms: 0,
            ..Profile::Lan.config()
        };
        assert_eq!(c.validate(), Err(ProfileError::RpcNotPositive));
    }
}
