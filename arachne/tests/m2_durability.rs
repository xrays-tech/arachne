//! M2 — storage fault injection and durability-ledger reconciliation in an L2
//! scenario.
//!
//! Three nodes over the in-memory transport, each with its real [`WalStorage`]
//! wrapped in [`FaultyStorage`] and watched by a [`DurabilityLedger`]. Every
//! outbound raft message is reconciled **at send time** against that node's
//! ledger, which closes the whole of INV1 (test-plan §7) at the transport
//! boundary:
//!
//! * **I2/I4** — every entry the message carries is already fsynced;
//! * **I1** — the message's `term` does not exceed the sender's highest
//!   *persisted* term (the `Storage` seam has no HardState-fsync callback, so
//!   [`FaultyStorage`] records every successful `set_hard_state`; the WAL fsyncs
//!   inside that call, so a recorded state is durable).
//!
//! `m0_inv1_ordering.rs` proves the entry half (I2/I4) for a healthy 2-node
//! cluster. This file adds the HardState half (I1), runs the storage through
//! [`FaultyStorage`], and exercises a crash/restart and an injected fsync
//! failure.
//!
//! Scenarios
//! * a follower crash + restart keeps INV1 true and preserves committed entries
//!   (INV2's committed half);
//! * a failing `sync_entries` **fail-stops** the node instead of letting it
//!   propagate unsynced entries — across the whole run, no entry-carrying
//!   message is ever sent and no entry fsync is ever recorded;
//! * negative controls prove both detectors are non-vacuous.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::consensus::RaftNode;
use arachne::state_machine::KvStateMachine;
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{
    FsyncObserver, LogIndex, NodeId, RaftId, StateMachine, Transport, TransportFactory,
    TransportMessage,
};
use arachne_testsupport::{
    block_on, DurabilityLedger, FaultSchedule, FaultyStorage, InMemoryRx, InMemoryTransportFactory,
    InMemoryTx, TransportError,
};
use protobuf::Message as _;
use raft::eraftpb::{Entry, Message as RaftMessage, MessageType};
use slog::{o, Drain, Logger};

/// A node whose storage has faults injected and whose outbound messages are
/// reconciled against a durability ledger.
type TestNode = RaftNode<FaultyStorage<WalStorage>, DurabilityTx, InMemoryRx>;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-m2-dur-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn node_id(i: RaftId) -> NodeId {
    NodeId::from(format!("n{i}"))
}

fn parse_raft_id(id: &NodeId) -> RaftId {
    id.as_str()
        .trim_start_matches('n')
        .parse::<RaftId>()
        .expect("harness node ids are `n<raft_id>`")
}

fn peers_of(self_id: RaftId, n: RaftId) -> HashMap<RaftId, NodeId> {
    (1..=n)
        .filter(|j| *j != self_id)
        .map(|j| (j, node_id(j)))
        .collect()
}

// ---------------------------------------------------------------------------
// Per-node observation counters
// ---------------------------------------------------------------------------

/// Observe-only counters for one node's outbound traffic and INV1 violations.
#[derive(Default)]
struct NodeCounters {
    sends: AtomicU64,
    entry_sends: AtomicU64,
    /// Messages carrying an entry the sender had not fsynced (I2/I4 violation).
    entry_violations: AtomicU64,
    /// Messages whose term exceeded the sender's persisted term (I1 violation).
    term_violations: AtomicU64,
}

impl NodeCounters {
    fn get(&self, field: &AtomicU64) -> u64 {
        field.load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------------------
// Durability-checking transport
// ---------------------------------------------------------------------------

/// Wraps the in-memory transport and reconciles every outbound raft message
/// against the sender's [`DurabilityLedger`] **before** delegating the send.
///
/// The check never panics inside the async `send` (fail-loud discipline): it
/// records counters that the test asserts on after the run.
struct DurabilityTx {
    inner: InMemoryTx,
    ledger: Arc<DurabilityLedger>,
    counters: Arc<NodeCounters>,
}

impl DurabilityTx {
    fn check_and_record(&self, msg: &TransportMessage) {
        let TransportMessage::Raft(bytes) = msg else {
            return;
        };
        let mut raft_msg = RaftMessage::default();
        if raft_msg.merge_from_bytes(bytes).is_err() {
            return; // undecodable → the core's codec rejects it anyway
        }
        self.counters.sends.fetch_add(1, Ordering::SeqCst);

        // INV1 (I1): a *term-changing* message must not carry a term beyond the
        // sender's durable HardState. Pre-vote is the deliberate exception: it
        // probes a prospective term and must NOT persist it (that is the whole
        // point of pre-vote), so it is exempt.
        let msg_type = raft_msg.get_msg_type();
        let is_pre_vote = matches!(
            msg_type,
            MessageType::MsgRequestPreVote | MessageType::MsgRequestPreVoteResponse
        );
        let term = raft_msg.get_term();
        if !is_pre_vote && term > self.ledger.max_persisted_term() {
            self.counters.term_violations.fetch_add(1, Ordering::SeqCst);
        }

        // INV1 (I2/I4): every carried entry must already be fsynced.
        let entries = raft_msg.get_entries();
        if entries.is_empty() {
            return;
        }
        self.counters.entry_sends.fetch_add(1, Ordering::SeqCst);
        let max_index = entries.iter().map(|e| e.get_index()).max().unwrap_or(0);
        if max_index >= 1 && !self.ledger.entries_cover(1, max_index) {
            self.counters.entry_violations.fetch_add(1, Ordering::SeqCst);
        }
    }
}

// `DurabilityTx` holds non-`Debug` state; the node's error type needs `Debug`.
impl std::fmt::Debug for DurabilityTx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurabilityTx").finish_non_exhaustive()
    }
}

impl Transport for DurabilityTx {
    type Error = TransportError;

    fn send(
        &self,
        to: NodeId,
        msg: TransportMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.check_and_record(&msg);
        let result = block_on(self.inner.send(to, msg));
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

struct Cluster {
    n: RaftId,
    /// The shared in-memory switch. A bounce must reuse it: a fresh factory
    /// would put the restarted node on a different network.
    factory: InMemoryTransportFactory,
    dirs: Vec<PathBuf>,
    ledgers: Vec<Arc<DurabilityLedger>>,
    counters: Vec<Arc<NodeCounters>>,
    schedules: Vec<FaultSchedule>,
    nodes: Vec<Option<TestNode>>,
    sms: Vec<KvStateMachine>,
    /// Per-node log of `(index, data)` applied to the state machine, in order.
    /// Cleared on a bounce (volatile state is lost and replayed from the WAL).
    committed: Vec<Vec<(LogIndex, Vec<u8>)>>,
    /// Nodes that fail-stopped (`step` returned an error): no longer driven and
    /// no longer delivered to, like a dead actor.
    dead: Vec<bool>,
    /// The first `step` error each node produced, if any.
    step_errors: Vec<Option<String>>,
}

impl Cluster {
    fn new(n: RaftId) -> Self {
        Self::with_schedules(n, vec![FaultSchedule::default(); n as usize])
    }

    fn with_schedules(n: RaftId, schedules: Vec<FaultSchedule>) -> Self {
        let mut c = Self {
            n,
            factory: InMemoryTransportFactory::new(),
            dirs: Vec::new(),
            ledgers: (0..n).map(|_| Arc::new(DurabilityLedger::new())).collect(),
            counters: (0..n).map(|_| Arc::new(NodeCounters::default())).collect(),
            schedules,
            nodes: Vec::new(),
            sms: (0..n).map(|_| KvStateMachine::new()).collect(),
            committed: (0..n).map(|_| Vec::new()).collect(),
            dead: vec![false; n as usize],
            step_errors: vec![None; n as usize],
        };
        for i in 1..=n {
            c.dirs.push(temp_dir(&format!("n{i}")));
        }
        for i in 1..=n {
            let node = c.build_node(i);
            c.nodes.push(Some(node));
        }
        c
    }

    /// Open node `i`'s WAL (same directory, same ledger — durable state
    /// persists across a bounce) and assemble a fresh `RaftNode`.
    fn build_node(&self, i: RaftId) -> TestNode {
        let dir = &self.dirs[(i - 1) as usize];
        let ledger = Arc::clone(&self.ledgers[(i - 1) as usize]);
        let wal = WalStorage::open(
            dir,
            WalOptions {
                cluster_id: "m2-dur".into(),
                node_id: format!("n{i}"),
                config: WalConfig {
                    fsync_policy: FsyncPolicy::Always,
                    segment_bytes: 1 << 20,
                },
                created_at_millis: 1_700_000_000_000,
                fsync_observer: Some(ledger.clone()),
            },
        )
        .expect("open wal");
        let store = FaultyStorage::with_ledger(
            wal,
            self.schedules[(i - 1) as usize].clone(),
            ledger.clone(),
        );
        let (tx, rx) = self.factory.create(node_id(i));
        let tx = DurabilityTx {
            inner: tx,
            ledger,
            counters: Arc::clone(&self.counters[(i - 1) as usize]),
        };
        RaftNode::new(i, peers_of(i, self.n), store, tx, rx, 0, &logger()).expect("build node")
    }

    /// One round: tick+step every live node, then drain and deliver inbound
    /// messages to live targets.
    fn round(&mut self) {
        for i in 0..self.nodes.len() {
            if self.dead[i] {
                continue;
            }
            let Some(node) = self.nodes[i].as_mut() else {
                continue;
            };
            node.tick();
            match block_on(node.step()) {
                Ok(outcome) => {
                    for (idx, _kind, data) in outcome.committed {
                        self.sms[i].apply(idx, &data).expect("apply");
                        self.committed[i].push((idx, data));
                    }
                    node.advance_apply();
                }
                Err(e) => {
                    self.dead[i] = true;
                    self.step_errors[i] = Some(e.to_string());
                }
            }
        }

        let mut outbound: Vec<(RaftId, RaftId, TransportMessage)> = Vec::new();
        for i in 0..self.nodes.len() {
            if self.dead[i] {
                continue;
            }
            let Some(node) = self.nodes[i].as_mut() else {
                continue;
            };
            loop {
                match node.rx().try_recv() {
                    Ok(Some((from, msg))) => {
                        outbound.push((parse_raft_id(&from), (i + 1) as RaftId, msg))
                    }
                    Ok(None) | Err(_) => break,
                }
            }
        }
        for (from, to, msg) in outbound {
            if self.dead[(to - 1) as usize] {
                continue;
            }
            if let Some(node) = self.nodes[(to - 1) as usize].as_mut() {
                let _ = node.on_message(from, msg);
            }
        }
    }

    /// Crash node `i`: task death, WAL directory retained.
    fn crash(&mut self, i: RaftId) {
        self.nodes[(i - 1) as usize] = None;
    }

    /// Restart node `i` from its WAL with volatile state lost.
    fn bounce(&mut self, i: RaftId) {
        let node = self.build_node(i);
        let idx = (i - 1) as usize;
        self.nodes[idx] = Some(node);
        self.sms[idx] = KvStateMachine::new();
        self.committed[idx].clear();
        self.dead[idx] = false;
        self.step_errors[idx] = None;
    }

    /// The target node's durable commit index (`HardState.commit` in memory —
    /// durable because `set_hard_state` fsyncs before returning).
    fn durable_commit(&self, i: RaftId) -> LogIndex {
        self.nodes[(i - 1) as usize]
            .as_ref()
            .expect("node present")
            .hard_state()
            .commit
    }

    /// The target node's applied log as `(index, data)` pairs, in commit order.
    fn committed_log(&self, i: RaftId) -> &[(LogIndex, Vec<u8>)] {
        &self.committed[(i - 1) as usize]
    }

    /// The target node's state-machine snapshot (INV3's comparison unit).
    fn snapshot(&self, i: RaftId) -> Vec<u8> {
        self.sms[(i - 1) as usize].snapshot().expect("snapshot")
    }

    /// Tick+step+apply node `i` **without delivering any message**: this is the
    /// pure WAL-replay path (a restarted node replaying its durable log).
    fn drive_local(&mut self, i: RaftId, rounds: usize) {
        let idx = (i - 1) as usize;
        for _ in 0..rounds {
            if self.dead[idx] {
                return;
            }
            let Some(node) = self.nodes[idx].as_mut() else {
                return;
            };
            node.tick();
            match block_on(node.step()) {
                Ok(outcome) => {
                    for (ix, _kind, data) in outcome.committed {
                        self.sms[idx].apply(ix, &data).expect("apply");
                        self.committed[idx].push((ix, data));
                    }
                    node.advance_apply();
                }
                Err(e) => {
                    self.dead[idx] = true;
                    self.step_errors[idx] = Some(e.to_string());
                }
            }
        }
    }

    /// Assert every live node's state-machine snapshot is byte-identical (INV3).
    fn assert_converged(&self) {
        let snaps: Vec<(RaftId, Vec<u8>)> = (1..=self.n)
            .filter(|&i| self.nodes[(i - 1) as usize].is_some())
            .map(|i| (i, self.snapshot(i)))
            .collect();
        for w in snaps.windows(2) {
            assert_eq!(
                w[0].1, w[1].1,
                "INV3 violated: nodes {} and {} disagree on state",
                w[0].0, w[1].0
            );
        }
    }

    /// Arm the `fault-injection` hook for `stage` on this thread, then
    /// tick+step node `i` exactly once, catching the injected crash.
    ///
    /// Returns `true` if the node stopped at the armed stage. The node is left
    /// in whatever in-memory state it had (the caller drops it, modelling a
    /// crash); nothing is applied, because the crash precedes the caller's
    /// apply step.
    #[cfg(feature = "fault-injection")]
    fn drive_one_armed(&mut self, i: RaftId, stage: arachne::fault_injection::Stage) -> bool {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        let idx = (i - 1) as usize;
        arachne::fault_injection::arm(stage);
        let node = self.nodes[idx].as_mut().expect("node present");
        node.tick();
        match catch_unwind(AssertUnwindSafe(|| block_on(node.step()))) {
            Err(_) => true,
            Ok(Ok(outcome)) => {
                for (ix, _kind, data) in outcome.committed {
                    self.sms[idx].apply(ix, &data).expect("apply");
                    self.committed[idx].push((ix, data));
                }
                self.nodes[idx].as_mut().expect("node present").advance_apply();
                arachne::fault_injection::disarm();
                false
            }
            Ok(Err(e)) => {
                self.dead[idx] = true;
                self.step_errors[idx] = Some(e.to_string());
                arachne::fault_injection::disarm();
                false
            }
        }
    }

    fn leader(&self) -> Option<RaftId> {
        (1..=self.n).find(|&i| {
            self.nodes[(i - 1) as usize]
                .as_ref()
                .is_some_and(|n| n.leader_id() == i)
        })
    }

    fn sm(&self, i: RaftId) -> &KvStateMachine {
        &self.sms[(i - 1) as usize]
    }

    fn total(&self, f: impl Fn(&NodeCounters) -> &AtomicU64) -> u64 {
        self.counters.iter().map(|c| c.get(f(c))).sum()
    }

    fn cleanup(&mut self) {
        self.nodes.iter_mut().for_each(|n| *n = None);
        for dir in &self.dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// Elect a leader, committing on the way.
fn elect(c: &mut Cluster) -> RaftId {
    for _ in 0..600 {
        c.round();
        if let Some(l) = c.leader() {
            return l;
        }
    }
    panic!("no leader elected within the round budget");
}

/// Commit `put(k, v)` on the leader and drive until it is applied there.
fn commit_put(c: &mut Cluster, leader: RaftId, seq: RaftId) {
    let cmd = KvStateMachine::encode_put(1, seq, b"k", &v_bytes(seq as u8));
    c.nodes[(leader - 1) as usize]
        .as_mut()
        .expect("leader present")
        .propose(&cmd)
        .expect("leader accepts the put");
    for _ in 0..400 {
        c.round();
        if c.sm(leader).get(b"k").expect("sm get").is_some() {
            break;
        }
    }
    assert!(
        c.sm(leader).get(b"k").expect("sm get").is_some(),
        "the leader must apply the put"
    );
}

fn v_bytes(v: u8) -> Vec<u8> {
    vec![v]
}

// ---------------------------------------------------------------------------
// INV1 over a crash + restart
// ---------------------------------------------------------------------------

/// INV1 holds across a follower crash and restart, and the committed entries
/// survive (INV2's committed half).
#[test]
fn inv1_holds_across_a_crash_and_restart() {
    let mut c = Cluster::new(3);
    let leader = elect(&mut c);

    commit_put(&mut c, leader, 1);

    // Crash a follower and commit another write on the remaining majority.
    let follower = (1..=3).find(|&i| i != leader).expect("a follower exists");
    c.crash(follower);
    commit_put(&mut c, leader, 2);

    // Restart the follower: it must recover committed entries from its WAL and
    // rejoin. INV1 must hold for every message it (and everyone) sends.
    c.bounce(follower);
    for _ in 0..600 {
        c.round();
        if c.sm(follower).get(b"k").expect("sm get").is_some() {
            break;
        }
    }
    assert!(
        c.sm(follower).get(b"k").expect("sm get").is_some(),
        "the restarted follower must recover the committed value"
    );

    // Non-vacuity: entry- and term-carrying traffic really happened.
    let entry_sends = c.total(|c| &c.entry_sends);
    assert!(
        entry_sends >= 1,
        "expected entry-carrying sends, got {entry_sends}"
    );
    assert!(c.total(|c| &c.sends) >= 1, "expected some sends");

    // INV1: neither half was ever violated.
    assert_eq!(
        c.total(|c| &c.entry_violations),
        0,
        "INV1 (I2/I4): an entry was sent before it was fsynced"
    );
    assert_eq!(
        c.total(|c| &c.term_violations),
        0,
        "INV1 (I1): a term was propagated before its HardState was persisted"
    );

    // The ledger of the restarted follower covers what it applied.
    let applied = c.nodes[(follower - 1) as usize]
        .as_ref()
        .expect("follower present")
        .applied_index();
    assert!(applied >= 1, "the follower must have applied entries");
    assert!(
        c.ledgers[(follower - 1) as usize].entries_cover(1, applied),
        "the follower's entry durability must cover [1, {applied}]"
    );

    c.cleanup();
}

// ---------------------------------------------------------------------------
// A failing fsync must fail-stop, never propagate
// ---------------------------------------------------------------------------

/// With `sync_entries` failing on every node, no node can ever make an entry
/// durable — so no entry may ever be propagated, and the first attempt must
/// fail-stop rather than send anyway.
#[test]
fn failing_fsync_fail_stops_and_never_propagates_entries() {
    let schedule = FaultSchedule {
        fail_sync_entries_at: Some(1),
        ..Default::default()
    };
    let mut c = Cluster::with_schedules(3, vec![schedule; 3]);

    // Drive well past an election and a no-op append.
    for _ in 0..800 {
        c.round();
    }

    // Some node must have fail-stopped on the injected failure.
    let errors: Vec<&String> = c.step_errors.iter().flatten().collect();
    assert!(
        !errors.is_empty(),
        "the injected sync_entries failure must surface as a step error (fail-stop)"
    );
    // Prove the failure really came from the fsync path: no entry ever became
    // durable anywhere (a HardState fsync may have recorded an event, but it
    // cannot have covered an entry, since the first entry sync always failed).
    for (i, ledger) in c.ledgers.iter().enumerate() {
        assert!(
            !ledger.entries_cover(1, 1),
            "node {} made an entry durable despite the injected failure",
            i + 1
        );
    }
    eprintln!("fail-stop errors: {errors:?}");

    // And nothing unsynced was ever put on the wire.
    assert_eq!(
        c.total(|c| &c.entry_sends),
        0,
        "no entry-carrying message may be sent when no entry could be fsynced"
    );
    assert_eq!(
        c.total(|c| &c.entry_violations),
        0,
        "INV1 (I2/I4) must not be violated even under fsync failure"
    );

    c.cleanup();
}

// ---------------------------------------------------------------------------
// INV2: crash sweep — restart replays the committed prefix deterministically
// ---------------------------------------------------------------------------

/// IN2: crash a node at several points in the run, restart it on its WAL, and
/// drive **pure replay** (no delivery). The pre-crash applied prefix must be
/// reproduced byte-for-byte and never lost, the durable commit must not
/// regress, and (when the replay lands on the same prefix) the state machine
/// must be byte-identical — INV3's determinism. The node then rejoins and every
/// node converges on one state.
///
/// The crash points are harness-visible boundaries around `RaftNode::step`
/// (before the step, between the step and applying its committed entries, and
/// after applying), swept over both the leader and a follower. A precise
/// in-`step` injection point (between persist and deliver) would need a
/// production hook; this covers the boundaries a harness can express.
#[test]
fn inv2_crash_sweep_replays_the_committed_prefix() {
    // (crash target role, extra rounds driven after the first commit)
    let cases: &[(&str, usize)] = &[
        ("leader", 0),
        ("leader", 40),
        ("follower", 0),
        ("follower", 40),
        ("follower", 160),
    ];

    for (role, extra_rounds) in cases {
        let mut c = Cluster::new(3);
        let leader = elect(&mut c);
        let target = if *role == "leader" {
            leader
        } else {
            (1..=3).find(|&i| i != leader).expect("a follower exists")
        };
        let tag = format!("role={role}, extra_rounds={extra_rounds}");

        // Commit a first write so a durable committed prefix exists, then drive
        // the requested extra rounds (the crash point moves later each case).
        commit_put(&mut c, leader, 1);
        for _ in 0..*extra_rounds {
            c.round();
        }

        // Capture the target's pre-crash **durable** state.
        //
        // `RaftNode::hard_state().commit` is raft's *in-memory* commit and can
        // lead the durable commit until the next `step()` persists it (the
        // client-ack path is safe: the runtime applies and acks only after
        // `step` persisted the commit). INV2's floor is the **persisted** commit,
        // which the durability ledger records.
        let persisted_commit = c.ledgers[(target - 1) as usize].persisted_commit();
        let before_applied = c.committed_log(target).to_vec();
        let durable_prefix: Vec<(LogIndex, Vec<u8>)> = before_applied
            .iter()
            .filter(|(ix, _)| *ix <= persisted_commit)
            .cloned()
            .collect();
        assert!(
            !durable_prefix.is_empty(),
            "the target must have a durable committed prefix [{tag}], ledger={:?}",
            c.ledgers[(target - 1) as usize]
                .hard_states()
                .iter()
                .map(|h| (h.term, h.commit))
                .collect::<Vec<_>>()
        );

        // Crash it; the survivors keep making progress on their quorum.
        c.crash(target);
        for _ in 0..200 {
            c.round();
        }

        // Restart on the WAL. Immediately after the bounce, raft's commit is the
        // one recovered from disk — before any replay advances it.
        c.bounce(target);
        let recovered_commit = c.durable_commit(target);
        assert!(
            recovered_commit >= persisted_commit,
            "INV2 violated: the persisted commit was not recovered ({persisted_commit} -> {recovered_commit}) [{tag}]"
        );

        // Replay the WAL with **no delivery** (the pure recovery path).
        c.drive_local(target, 400);
        let after_applied = c.committed_log(target);

        // INV2: no durably committed entry is silently lost — the durable
        // prefix is reproduced, in order and content-identical.
        assert!(
            after_applied.len() >= durable_prefix.len(),
            "INV2 violated: WAL replay lost durably committed entries ({} -> {}) [{tag}]",
            durable_prefix.len(),
            after_applied.len()
        );
        for (i, expected) in durable_prefix.iter().enumerate() {
            assert_eq!(
                after_applied[i], *expected,
                "INV2 violated: replay changed durable entry {i} [{tag}]"
            );
        }

        // Rejoin: drive the whole cluster and assert convergence (INV3/INV8).
        for _ in 0..800 {
            c.round();
        }
        c.assert_converged();

        c.cleanup();
    }
}

// ---------------------------------------------------------------------------
// INV2: precise ready-stage crash injection (feature `fault-injection`)
// ---------------------------------------------------------------------------

/// IN2 with a **precise** crash point (propsol v0.2.9 L): arm the runtime hook for
/// a `RaftNode::step` stage boundary and crash the target exactly there —
/// `AfterPersist` (entries + HardState durable, nothing sent yet) or
/// `AfterDeliver` (messages sent, not yet applied). Swept over the leader and a
/// follower.
///
/// After the crash the node is restarted on its WAL and driven with **no
/// delivery** (pure replay). INV2 requires that everything up to the durably
/// persisted `HardState.commit` is present, so:
/// * the commit recovered from disk is >= the commit persisted before the crash;
/// * the replayed state machine has applied at least that commit;
/// * a crash at the *leader* — which had acked the write — never loses the acked
///   value;
/// * after rejoining, every node converges on one state.
///
/// Gated on `--features fault-injection` (not a default feature).
#[cfg(feature = "fault-injection")]
#[test]
fn inv2_precise_ready_stage_crash_replays_the_durable_prefix() {
    use arachne::fault_injection::Stage;

    let cases: &[(&str, Stage)] = &[
        ("leader", Stage::AfterPersist),
        ("leader", Stage::AfterDeliver),
        ("follower", Stage::AfterPersist),
        ("follower", Stage::AfterDeliver),
    ];

    for (role, stage) in cases {
        let mut c = Cluster::new(3);
        let leader = elect(&mut c);
        let target = if *role == "leader" {
            leader
        } else {
            (1..=3).find(|&i| i != leader).expect("a follower exists")
        };
        let tag = format!("role={role}, stage={stage:?}");

        // Ack one write so a durably committed prefix exists.
        commit_put(&mut c, leader, 1);

        // Crash the target at the armed stage boundary inside `step`.
        let crashed = c.drive_one_armed(target, *stage);
        assert!(
            crashed,
            "the fault-injection hook must stop the node at {stage:?} [{tag}]"
        );
        let persisted_at_crash = c.ledgers[(target - 1) as usize].persisted_commit();

        // Drop the node (crash), then restart it on its WAL.
        c.crash(target);
        c.bounce(target);

        let recovered_commit = c.durable_commit(target);
        assert!(
            recovered_commit >= persisted_at_crash,
            "INV2 violated: the persisted commit was not recovered ({persisted_at_crash} -> {recovered_commit}) [{tag}]"
        );

        // Pure WAL replay (no delivery).
        c.drive_local(target, 400);

        // INV2: apply keeps up with the recovered commit.
        let applied = c.nodes[(target - 1) as usize]
            .as_ref()
            .expect("node present")
            .applied_index();
        assert!(
            applied >= recovered_commit,
            "INV2 violated: applied ({applied}) < recovered commit ({recovered_commit}) [{tag}]"
        );

        // A crash at the leader — which had acked the write — must never lose it.
        if *role == "leader" {
            assert_eq!(
                c.sm(target).get(b"k").expect("sm get"),
                Some(v_bytes(1)),
                "INV2 violated: the leader lost its acked write across the crash [{tag}]"
            );
        }

        // Rejoin and converge (INV3).
        for _ in 0..800 {
            c.round();
        }
        c.assert_converged();
        for i in 1..=3 {
            assert_eq!(
                c.sm(i).get(b"k").expect("sm get"),
                Some(v_bytes(1)),
                "every node must hold the acked value after convergence [{tag}]"
            );
        }

        c.cleanup();
    }
}

// ---------------------------------------------------------------------------
// Negative controls (non-vacuity of both detectors)
// ---------------------------------------------------------------------------

/// Build a `DurabilityTx` over one in-memory endpoint with the given ledger.
fn probe_tx(ledger: Arc<DurabilityLedger>) -> (DurabilityTx, Arc<NodeCounters>) {
    let factory = InMemoryTransportFactory::new();
    let (inner, _rx) = factory.create(node_id(1));
    let counters = Arc::new(NodeCounters::default());
    (
        DurabilityTx {
            inner,
            ledger,
            counters: Arc::clone(&counters),
        },
        counters,
    )
}

fn raft_msg(term: u64, entries: Vec<(u64, u64)>) -> Vec<u8> {
    let mut msg = RaftMessage::default();
    msg.set_msg_type(MessageType::MsgAppend);
    msg.set_from(1);
    msg.set_to(2);
    msg.set_term(term);
    let ents: Vec<Entry> = entries
        .into_iter()
        .map(|(index, term)| {
            let mut e = Entry::default();
            e.set_index(index);
            e.set_term(term);
            e.set_data(format!("payload-{index}").into_bytes().into());
            e
        })
        .collect();
    msg.set_entries(ents.into());
    msg.write_to_bytes().expect("encode")
}

/// The I2/I4 detector fires when an entry is sent beyond the durable range.
#[test]
fn entry_detector_flags_an_unsynced_entry() {
    let ledger = Arc::new(DurabilityLedger::new());
    ledger.on_segment_fsynced(1, 3); // durable through 3
    ledger.record_hard_state(1, Some(1), 0); // term 1 is durable, so I1 is satisfied
    let (tx, counters) = probe_tx(ledger);

    let _ = block_on(tx.send(
        node_id(2),
        TransportMessage::Raft(raft_msg(1, vec![(4, 1), (5, 1)])),
    ));
    assert_eq!(counters.entry_sends.load(Ordering::SeqCst), 1);
    assert_eq!(
        counters.entry_violations.load(Ordering::SeqCst),
        1,
        "entries beyond the durable range must be flagged"
    );
    assert_eq!(
        counters.term_violations.load(Ordering::SeqCst),
        0,
        "the term is persisted, so I1 must not fire"
    );
}

/// The I1 detector fires when a term is propagated before its HardState is
/// durable — even when the carried entries are durable.
#[test]
fn term_detector_flags_an_unpersisted_term() {
    let ledger = Arc::new(DurabilityLedger::new());
    ledger.on_segment_fsynced(1, 5);
    ledger.record_hard_state(1, Some(1), 0); // persisted term 1 only
    let (tx, counters) = probe_tx(ledger);

    let _ = block_on(tx.send(
        node_id(2),
        TransportMessage::Raft(raft_msg(2, vec![(4, 1)])),
    ));
    assert_eq!(counters.entry_sends.load(Ordering::SeqCst), 1);
    assert_eq!(
        counters.entry_violations.load(Ordering::SeqCst),
        0,
        "the entries are durable, so I2/I4 must not fire"
    );
    assert_eq!(
        counters.term_violations.load(Ordering::SeqCst),
        1,
        "term 2 propagated while only term 1 is durable must be flagged"
    );
}

/// No false alarms: a message within both watermarks is not flagged.
#[test]
fn detectors_pass_an_in_range_message() {
    let ledger = Arc::new(DurabilityLedger::new());
    ledger.on_segment_fsynced(1, 5);
    ledger.record_hard_state(2, Some(1), 5);
    let (tx, counters) = probe_tx(ledger);

    let _ = block_on(tx.send(
        node_id(2),
        TransportMessage::Raft(raft_msg(2, vec![(4, 1), (5, 1)])),
    ));
    assert_eq!(counters.entry_sends.load(Ordering::SeqCst), 1);
    assert_eq!(counters.entry_violations.load(Ordering::SeqCst), 0);
    assert_eq!(counters.term_violations.load(Ordering::SeqCst), 0);
}
