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
    /// Bytes the durable WAL occupies on disk.
    wal_bytes: AtomicU64,
    /// Wall-clock milliseconds the most recent local snapshot took
    /// (propsol §8: the >1s budget alarm, Q4).
    snapshot_last_duration_ms: AtomicU64,
    /// Size in bytes of the most recent locally created snapshot.
    snapshot_last_size_bytes: AtomicU64,
    /// Snapshots created locally.
    snapshots_created_total: AtomicU64,
    /// Snapshots received from a leader and installed.
    snapshots_installed_total: AtomicU64,
    /// Entries committed but not yet applied (the apply task's backlog).
    apply_lag: AtomicU64,
    /// Bytes committed but not yet applied (the backpressure signal, Q7).
    apply_backlog_bytes: AtomicU64,
    /// Proposals rejected with `Busy` because the apply backlog was over the
    /// byte bound (propsol v0.2.11 N).
    proposal_busy_total: AtomicU64,
    /// Local snapshots that exceeded their duration budget (propsol §8.2 Q4:
    /// `> 1s` warns — snapshot creation blocks apply).
    snapshot_slow_total: AtomicU64,
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

    /// Record the on-disk WAL size.
    pub fn set_wal_bytes(&self, value: u64) {
        self.wal_bytes.store(value, Ordering::Relaxed);
    }

    /// Record how long the most recent local snapshot took, and its size.
    pub fn set_snapshot_last(&self, duration_ms: u64, size_bytes: u64) {
        self.snapshot_last_duration_ms
            .store(duration_ms, Ordering::Relaxed);
        self.snapshot_last_size_bytes
            .store(size_bytes, Ordering::Relaxed);
    }

    /// Increment the locally-created snapshot counter.
    pub fn inc_snapshots_created(&self) {
        self.snapshots_created_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the installed-snapshot counter.
    pub fn inc_snapshots_installed(&self) {
        self.snapshots_installed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record the apply backlog: entries (and bytes) committed but not applied.
    pub fn set_apply_backlog(&self, entries: u64, bytes: u64) {
        self.apply_lag.store(entries, Ordering::Relaxed);
        self.apply_backlog_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Increment the counter of proposals rejected by apply backpressure.
    pub fn inc_proposal_busy(&self) {
        self.proposal_busy_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Entries committed but not yet applied.
    pub fn apply_lag(&self) -> u64 {
        self.apply_lag.load(Ordering::Relaxed)
    }

    /// Bytes committed but not yet applied.
    pub fn apply_backlog_bytes(&self) -> u64 {
        self.apply_backlog_bytes.load(Ordering::Relaxed)
    }

    /// Proposals rejected with `Busy` by apply backpressure.
    pub fn proposal_busy_total(&self) -> u64 {
        self.proposal_busy_total.load(Ordering::Relaxed)
    }

    /// Count a local snapshot that blew its duration budget (§8.2).
    pub fn inc_snapshot_slow(&self) {
        self.snapshot_slow_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Local snapshots that exceeded their duration budget.
    pub fn snapshot_slow_total(&self) -> u64 {
        self.snapshot_slow_total.load(Ordering::Relaxed)
    }

    /// The on-disk WAL size in bytes.
    pub fn wal_bytes(&self) -> u64 {
        self.wal_bytes.load(Ordering::Relaxed)
    }

    /// The highest committed log index.
    pub fn commit_index(&self) -> u64 {
        self.commit_index.load(Ordering::Relaxed)
    }

    /// The highest applied log index.
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Relaxed)
    }

    /// Snapshots created locally.
    pub fn snapshots_created_total(&self) -> u64 {
        self.snapshots_created_total.load(Ordering::Relaxed)
    }

    /// Snapshots received from a leader and installed.
    pub fn snapshots_installed_total(&self) -> u64 {
        self.snapshots_installed_total.load(Ordering::Relaxed)
    }

    /// Wall-clock milliseconds the most recent local snapshot took.
    pub fn snapshot_last_duration_ms(&self) -> u64 {
        self.snapshot_last_duration_ms.load(Ordering::Relaxed)
    }

    /// Size in bytes of the most recently created local snapshot.
    pub fn snapshot_last_size_bytes(&self) -> u64 {
        self.snapshot_last_size_bytes.load(Ordering::Relaxed)
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
        gauge(
            &mut out,
            "arachne_wal_bytes",
            "Bytes the durable WAL occupies on disk.",
            self.wal_bytes.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "arachne_snapshot_last_duration_ms",
            "Wall-clock milliseconds the most recent local snapshot took.",
            self.snapshot_last_duration_ms.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "arachne_snapshot_last_size_bytes",
            "Size in bytes of the most recent locally created snapshot.",
            self.snapshot_last_size_bytes.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "arachne_snapshots_created_total",
            "Snapshots created locally.",
            self.snapshots_created_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "arachne_snapshots_installed_total",
            "Snapshots received from a leader and installed.",
            self.snapshots_installed_total.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "arachne_apply_lag",
            "Entries committed but not yet applied to the state machine.",
            self.apply_lag.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "arachne_apply_backlog_bytes",
            "Bytes committed but not yet applied (the proposal backpressure signal).",
            self.apply_backlog_bytes.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "arachne_proposal_busy_total",
            "Proposals rejected with Busy because the apply backlog was too deep.",
            self.proposal_busy_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "arachne_snapshot_slow_total",
            "Local snapshots that exceeded the 1s budget (they block apply).",
            self.snapshot_slow_total.load(Ordering::Relaxed),
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
        m.set_wal_bytes(4096);
        m.set_snapshot_last(12, 1024);
        m.inc_snapshots_created();
        m.inc_snapshots_installed();
        m.inc_snapshots_installed();
        m.set_apply_backlog(7, 2048);
        m.inc_proposal_busy();
        m.inc_snapshot_slow();
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
            "arachne_wal_bytes",
            "arachne_snapshot_last_duration_ms",
            "arachne_snapshot_last_size_bytes",
            "arachne_snapshots_created_total",
            "arachne_snapshots_installed_total",
            "arachne_apply_lag",
            "arachne_apply_backlog_bytes",
            "arachne_proposal_busy_total",
            "arachne_snapshot_slow_total",
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
        assert!(text.contains("# TYPE arachne_wal_bytes gauge"));
        assert!(text.contains("arachne_wal_bytes 4096"));
        assert!(text.contains("arachne_snapshot_last_duration_ms 12"));
        assert!(text.contains("arachne_snapshot_last_size_bytes 1024"));
        assert!(text.contains("# TYPE arachne_snapshots_created_total counter"));
        assert!(text.contains("arachne_snapshots_created_total 1"));
        assert!(text.contains("arachne_snapshots_installed_total 2"));
        assert!(text.contains("arachne_apply_lag 7"));
        assert!(text.contains("arachne_apply_backlog_bytes 2048"));
        assert!(text.contains("arachne_proposal_busy_total 1"));
        assert!(text.contains("# TYPE arachne_snapshot_slow_total counter"));
        assert!(text.contains("arachne_snapshot_slow_total 1"));
        assert_eq!(m.apply_lag(), 7);
        assert_eq!(m.apply_backlog_bytes(), 2048);
        assert_eq!(m.proposal_busy_total(), 1);
        assert_eq!(m.wal_bytes(), 4096);
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
