//! A tiny thread-safe metrics registry rendered as Prometheus text.
//!
//! This lives in the core (not the node binary) because the node runtime actor
//! (see [`crate::runtime`]) owns an [`Arc<Metrics>`] and refreshes it every
//! drive cycle, while the node's HTTP `/metrics` handler (on another thread)
//! reads it. Keeping a single registry here means the binary and the lib share
//! exactly one implementation — the binary re-exports it (see `arachne-node`).
//!
//! Deliberately dependency-free: a handful of atomic gauges updated from the
//! drive loop and read by the HTTP handler.

use std::sync::atomic::{AtomicU64, Ordering};

/// The node's gauges (propsol §8 subset for M1).
#[derive(Debug, Default)]
pub struct Metrics {
    term: AtomicU64,
    leader_id: AtomicU64,
    commit_index: AtomicU64,
    applied_index: AtomicU64,
    is_leader: AtomicU64,
    dropped_sends: AtomicU64,
    /// Total number of ReadIndex reads that timed out after one retry.
    read_index_timeout_total: AtomicU64,
    /// Number of ReadIndex reads currently pending confirmation/apply.
    read_index_pending: AtomicU64,
}

impl Metrics {
    /// A fresh registry with all gauges at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the current raft term.
    pub fn set_term(&self, value: u64) {
        self.term.store(value, Ordering::Relaxed);
    }

    /// Record the current leader's raft id (0 = unknown).
    pub fn set_leader_id(&self, value: u64) {
        self.leader_id.store(value, Ordering::Relaxed);
    }

    /// Record the highest committed log index.
    pub fn set_commit_index(&self, value: u64) {
        self.commit_index.store(value, Ordering::Relaxed);
    }

    /// Record the highest applied log index.
    pub fn set_applied_index(&self, value: u64) {
        self.applied_index.store(value, Ordering::Relaxed);
    }

    /// Record whether this node is the leader.
    pub fn set_is_leader(&self, value: bool) {
        self.is_leader.store(u64::from(value), Ordering::Relaxed);
    }

    /// Record the number of outbound raft messages dropped due to a failed
    /// transport send (see `RaftNode::dropped_send_count`).
    pub fn set_dropped_sends(&self, value: u64) {
        self.dropped_sends.store(value, Ordering::Relaxed);
    }

    /// Increment the ReadIndex timeout counter (a read timed out after its
    /// single retry, propsol §5.4).
    pub fn inc_read_index_timeout(&self) {
        self.read_index_timeout_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the number of ReadIndex reads currently pending confirmation or
    /// apply.
    pub fn set_read_index_pending(&self, value: u64) {
        self.read_index_pending.store(value, Ordering::Relaxed);
    }

    /// Whether this node currently believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed) == 1
    }

    /// Whether a leader is known (the node is "ready").
    ///
    /// A node is ready to serve once it knows its cluster's leader — even if it
    /// is itself a follower (in which case writes/linearizable reads redirect).
    pub fn is_ready(&self) -> bool {
        self.leader_id.load(Ordering::Relaxed) != 0
    }

    /// The current leader's raft id (0 = unknown).
    pub fn leader_id(&self) -> u64 {
        self.leader_id.load(Ordering::Relaxed)
    }

    /// The number of outbound raft messages dropped due to a failed transport
    /// send.
    pub fn dropped_sends(&self) -> u64 {
        self.dropped_sends.load(Ordering::Relaxed)
    }

    /// Render every gauge in the Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut out = String::new();
        gauge(
            &mut out,
            "arachne_term",
            "Current raft term.",
            self.term.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "arachne_leader_id",
            "Current leader raft id (0 = unknown).",
            self.leader_id.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "arachne_commit_index",
            "Highest committed log index.",
            self.commit_index.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "arachne_applied_index",
            "Highest applied log index.",
            self.applied_index.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "arachne_is_leader",
            "1 if this node is the leader, else 0.",
            self.is_leader.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "arachne_dropped_sends",
            "Outbound raft messages dropped due to a failed transport send.",
            self.dropped_sends.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "arachne_read_index_timeout_total",
            "ReadIndex reads that timed out after one retry.",
            self.read_index_timeout_total.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "arachne_read_index_pending",
            "ReadIndex reads currently pending confirmation or apply.",
            self.read_index_pending.load(Ordering::Relaxed),
        );
        out
    }
}

fn gauge(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push_str(" gauge\n");
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push_str(" counter\n");
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_every_gauge_with_help_and_type() {
        let m = Metrics::new();
        m.set_term(3);
        m.set_leader_id(1);
        m.set_commit_index(7);
        m.set_applied_index(6);
        m.set_is_leader(true);
        m.set_dropped_sends(4);
        m.inc_read_index_timeout();
        m.inc_read_index_timeout();
        m.set_read_index_pending(5);
        let text = m.render();
        for name in [
            "arachne_term",
            "arachne_leader_id",
            "arachne_commit_index",
            "arachne_applied_index",
            "arachne_is_leader",
            "arachne_dropped_sends",
            "arachne_read_index_timeout_total",
            "arachne_read_index_pending",
        ] {
            assert!(text.contains(name), "missing {name}");
        }
        assert!(text.contains("# TYPE arachne_term gauge"));
        assert!(text.contains("arachne_term 3"));
        assert!(text.contains("arachne_is_leader 1"));
        assert!(text.contains("arachne_dropped_sends 4"));
        assert!(text.contains("# TYPE arachne_read_index_timeout_total counter"));
        assert!(text.contains("arachne_read_index_timeout_total 2"));
        assert!(text.contains("# TYPE arachne_read_index_pending gauge"));
        assert!(text.contains("arachne_read_index_pending 5"));
        assert!(m.is_leader());
        assert_eq!(m.leader_id(), 1);
        assert_eq!(m.dropped_sends(), 4);
    }

    #[test]
    fn defaults_are_zero() {
        let text = Metrics::new().render();
        assert!(text.contains("arachne_term 0"));
        assert!(text.contains("arachne_is_leader 0"));
        assert!(text.contains("arachne_dropped_sends 0"));
    }

    #[test]
    fn is_ready_tracks_leader_known() {
        let m = Metrics::new();
        assert!(!m.is_ready(), "no leader yet");
        m.set_leader_id(2);
        assert!(m.is_ready(), "a leader is now known");
    }
}
