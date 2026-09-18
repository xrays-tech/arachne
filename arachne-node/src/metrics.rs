//! A tiny thread-safe metrics registry rendered as Prometheus text.
//!
//! Deliberately dependency-free: the node exposes a handful of gauges updated
//! from the drive loop and read by the HTTP handler on another thread.

use std::sync::atomic::{AtomicU64, Ordering};

/// The node's gauges (propsol §8 subset for M0).
#[derive(Debug, Default)]
pub struct Metrics {
    term: AtomicU64,
    leader_id: AtomicU64,
    commit_index: AtomicU64,
    applied_index: AtomicU64,
    is_leader: AtomicU64,
    dropped_sends: AtomicU64,
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

    /// Whether this node currently believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed) == 1
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
        let text = m.render();
        for name in [
            "arachne_term",
            "arachne_leader_id",
            "arachne_commit_index",
            "arachne_applied_index",
            "arachne_is_leader",
            "arachne_dropped_sends",
        ] {
            assert!(text.contains(name), "missing {name}");
        }
        assert!(text.contains("# TYPE arachne_term gauge"));
        assert!(text.contains("arachne_term 3"));
        assert!(text.contains("arachne_is_leader 1"));
        assert!(text.contains("arachne_dropped_sends 4"));
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
}
