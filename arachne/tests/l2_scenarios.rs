//! L2 deterministic scenario harness (M1 (C) stage 3c) over the **in-memory**
//! transport.
//!
//! The transport seam (`arachne-transport-tonic`) keeps the real tonic path
//! available; the deterministic real-tonic-on-turmoil track stalled in stage 3b
//! (see handoff). Per decision, L2 scenarios run here on the in-memory
//! transport, where the harness fully controls message delivery — which is what
//! lets us inject partitions/crashes deterministically and still assert the
//! raft invariants (INV7/8/9) and, later, INV4 linearizability via the
//! ClientOracle.
//!
//! Everything is synchronous and clock-free: the harness owns a logical round
//! counter, drives `RaftNode::tick`/`step`, and delivers (or drops) each queued
//! message itself. Same seed + same schedule ⇒ byte-identical trace.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use arachne::consensus::RaftNode;
use arachne::state_machine::KvStateMachine;
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{NodeId, RaftId, StateMachine, TransportFactory};
use arachne_testsupport::{
    block_on, check_linearizable, CallId, CheckOutcome, ClientId, History, InMemoryRx,
    InMemoryTransportFactory, InMemoryTx, KvState, Op, OpResult, SeqNo, ValueId,
};
use raft::{clear_election_rng_seed, set_election_rng_seed};
use slog::{o, Drain, Logger};

type TestNode = RaftNode<WalStorage, InMemoryTx, InMemoryRx>;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-l2-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
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

fn wal_opts(tag: &str) -> WalOptions {
    WalOptions {
        cluster_id: "l2-mem".into(),
        node_id: tag.into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 1 << 20,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

/// Fault policy: a set of nodes that cannot exchange messages with the rest
/// (a partition isolating them). Deterministic and harness-owned.
#[derive(Default, Clone)]
struct Faults {
    isolated: BTreeSet<RaftId>,
    /// Dropped in exactly one direction (`from` -> `to`).
    oneway: BTreeSet<(RaftId, RaftId)>,
}

impl Faults {
    fn isolate(&mut self, id: RaftId) {
        self.isolated.insert(id);
    }

    /// Drop messages in exactly one direction (`from` -> `to`).
    fn oneway(&mut self, from: RaftId, to: RaftId) {
        self.oneway.insert((from, to));
    }

    /// `true` if a message from `from` to `to` must be dropped.
    fn drops(&self, from: RaftId, to: RaftId) -> bool {
        self.isolated.contains(&from) != self.isolated.contains(&to)
            || self.oneway.contains(&(from, to))
    }
}

/// A deterministic 3-node cluster with harness-controlled delivery.
struct Cluster {
    n: RaftId,
    dirs: Vec<PathBuf>,
    nodes: Vec<Option<TestNode>>,
    sms: Vec<KvStateMachine>,
    /// Per-node committed log `(index, data)` in apply order.
    committed: Vec<Vec<(RaftId, Vec<u8>)>>,
    /// Read states emitted across the run: `(node, request_ctx, read_index)`.
    read_states: Vec<(RaftId, Vec<u8>, arachne::LogIndex)>,
    faults: Faults,
    /// `(term, leader)` observations across the whole run, for INV7.
    leader_obs: Vec<(u64, RaftId)>,
}

impl Cluster {
    fn new(n: RaftId) -> Self {
        let factory = InMemoryTransportFactory::new();
        let mut dirs = Vec::new();
        let mut nodes = Vec::new();
        let mut sms = Vec::new();
        let mut committed = Vec::new();

        for i in 1..=n {
            let dir = temp_dir(&format!("n{i}"));
            let wal = WalStorage::open(&dir, wal_opts(&format!("n{i}"))).expect("open wal");
            let (tx, rx) = factory.create(node_id(i));
            let node = RaftNode::new(i, peers_of(i, n), wal, tx, rx, 0, &logger())
                .expect("build node");
            dirs.push(dir);
            nodes.push(Some(node));
            sms.push(KvStateMachine::new());
            committed.push(Vec::new());
        }

        Self {
            n,
            dirs,
            nodes,
            sms,
            committed,
            read_states: Vec::new(),
            faults: Faults::default(),
            leader_obs: Vec::new(),
        }
    }

    /// One logical round: tick+drive every live node, then deliver queued
    /// messages subject to the fault policy.
    fn round(&mut self) {
        for i in 0..self.nodes.len() {
            if let Some(node) = self.nodes[i].as_mut() {
                node.tick();
                let outcome = block_on(node.step()).expect("step");
                for (ctx, index) in &outcome.read_states {
                    self.read_states.push(((i + 1) as RaftId, ctx.clone(), *index));
                }
                for (idx, data) in outcome.committed {
                    self.sms[i].apply(idx, &data).expect("apply");
                    self.committed[i].push((idx, data));
                }
                node.advance_apply();
            }
        }

        // Drain every node's outbound queue, then deliver per policy.
        let mut outbound: Vec<(RaftId, RaftId, arachne::TransportMessage)> = Vec::new();
        for i in 0..self.nodes.len() {
            if let Some(node) = self.nodes[i].as_mut() {
                loop {
                    match node.rx().try_recv() {
                        Ok(Some((from, msg))) => {
                            outbound.push((parse_raft_id(&from), (i + 1) as RaftId, msg))
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
            }
        }
        for (from, to, msg) in outbound {
            if self.faults.drops(from, to) {
                continue;
            }
            if let Some(target) = self.nodes[(to - 1) as usize].as_mut() {
                let _ = target.on_message(from, msg);
            }
        }

        // Record (term, leader) for INV7.
        for slot in self.nodes.iter() {
            if let Some(node) = slot {
                let obs = (node.hard_state().term, node.leader_id());
                if obs.1 != 0 {
                    self.leader_obs.push(obs);
                }
            }
        }
    }

    /// Tick+step only node `i` with NO message delivery, returning the read
    /// states it emits locally. Isolates "does this node serve ReadIndex itself?".
    fn step_local(&mut self, i: usize) -> Vec<(Vec<u8>, arachne::LogIndex)> {
        let node = self.nodes[i].as_mut().expect("node present");
        node.tick();
        let out = block_on(node.step()).expect("step");
        let reads = out.read_states;
        for (idx, data) in out.committed {
            self.sms[i].apply(idx, &data).expect("apply");
            self.committed[i].push((idx, data));
        }
        node.advance_apply();
        reads
    }

    fn leader_of(&self, id: RaftId) -> bool {
        self.nodes[(id - 1) as usize]
            .as_ref()
            .is_some_and(|n| n.leader_id() == id)
    }

    fn any_leader(&self) -> Option<RaftId> {
        (1..=self.n).find(|&i| self.leader_of(i))
    }

    fn propose(&mut self, leader: RaftId, key: &[u8], val: &[u8], seq: RaftId) -> bool {
        let cmd = KvStateMachine::encode_put(1, seq, key, val);
        self.nodes[(leader - 1) as usize]
            .as_mut()
            .expect("leader present")
            .propose(&cmd)
            .is_ok()
    }

    fn cleanup(&mut self) {
        self.nodes.iter_mut().for_each(|n| *n = None);
        for dir in &self.dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// INV7: each term has at most one leader across the whole observed run.
fn assert_single_leader_per_term(obs: &[(u64, RaftId)]) {
    let mut by_term: BTreeMap<u64, RaftId> = BTreeMap::new();
    for &(term, leader) in obs {
        if let Some(prev) = by_term.get(&term) {
            assert_eq!(
                *prev, leader,
                "INV7 violated: term {term} had leaders {prev} and {leader}"
            );
        } else {
            by_term.insert(term, leader);
        }
    }
}

// ---------------------------------------------------------------------------
// Scenario S02 (2+1 partition) + INV7 + double-run determinism
// ---------------------------------------------------------------------------

/// RAII guard that seeds the (stage-2 patched) raft election RNG and clears it
/// on drop, so a run is reproducible and the seed cannot leak to other tests.
struct ElectionSeed;

impl ElectionSeed {
    fn enter(seed: u64) -> Self {
        set_election_rng_seed(seed);
        Self
    }
}

impl Drop for ElectionSeed {
    fn drop(&mut self) {
        clear_election_rng_seed();
    }
}

/// The byte-comparable outcome of one scenario run.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    leader_trace: Vec<(u64, RaftId)>,
    committed: Vec<Vec<(RaftId, Vec<u8>)>>,
}

/// Elect a leader, committing on the way; panics if none within the budget.
fn elect(c: &mut Cluster) -> RaftId {
    for _ in 0..600 {
        c.round();
        if let Some(l) = c.any_leader() {
            return l;
        }
    }
    panic!("no leader elected within the round budget");
}

/// S02: elect a leader, commit a first write, isolate a follower (2+1 split),
/// then commit a second write on the majority. Returns the observed
/// `(term, leader)` trace and each node's committed log.
fn run_s02(seed: u64) -> Outcome {
    let _seed = ElectionSeed::enter(seed);
    let mut c = Cluster::new(3);

    let leader = elect(&mut c);
    assert!(c.propose(leader, b"k0", b"v0", 1), "first propose must be accepted");
    for _ in 0..300 {
        c.round();
    }

    // Isolate a follower: the leader + the other follower keep a majority.
    let victim = (1..=3).find(|&i| i != leader).expect("a follower exists");
    c.faults.isolate(victim);
    let victim_len_before = c.committed[(victim - 1) as usize].len();

    assert!(
        c.propose(leader, b"k1", b"v1", 2),
        "majority-side propose must be accepted"
    );
    for _ in 0..400 {
        c.round();
    }

    // The partition must be OBSERVED: the isolated follower did not receive the
    // majority-side write, while both majority nodes did.
    assert_eq!(
        c.committed[(victim - 1) as usize].len(),
        victim_len_before,
        "isolated follower must not receive the majority-side write"
    );
    assert!(
        (1..=3)
            .filter(|&i| i != victim)
            .all(|i| c.committed[(i - 1) as usize].len() > victim_len_before),
        "both majority nodes must commit the second write"
    );

    assert_single_leader_per_term(&c.leader_obs);
    // Non-vacuity: every node committed the first write; the majority also
    // committed the second, so every node's committed log is non-empty.
    assert!(
        c.committed.iter().all(|log| !log.is_empty()),
        "all nodes must have committed at least the first write: {:?}",
        c.committed
    );

    let out = Outcome {
        leader_trace: c.leader_obs.clone(),
        committed: c.committed.clone(),
    };
    c.cleanup();
    out
}

#[test]
fn s02_partition_preserves_single_leader_per_term() {
    let out = run_s02(0x5020_0001);
    // A leader was elected (trace non-empty) and the run committed work.
    assert!(!out.leader_trace.is_empty(), "a leader must appear in the trace");
    assert!(out.committed.iter().all(|log| !log.is_empty()));
}

#[test]
fn double_run_same_seed_is_deterministic() {
    let a = run_s02(0x5020_0001);
    let b = run_s02(0x5020_0001);
    assert_eq!(a.leader_trace, b.leader_trace, "same seed ⇒ same leader trace");
    assert_eq!(a.committed, b.committed, "same seed ⇒ same committed logs");
}

// ---------------------------------------------------------------------------
// INV8 / INV9 / INV3 helpers and scenario S01 (leader crash + revive)
// ---------------------------------------------------------------------------

/// INV8: any two nodes agree on the `(index, data)` of every log index they
/// both applied.
fn assert_log_matching(committed: &[Vec<(arachne::LogIndex, Vec<u8>)>]) {
    let maps: Vec<BTreeMap<arachne::LogIndex, Vec<u8>>> = committed
        .iter()
        .map(|log| log.iter().cloned().collect())
        .collect();
    for a in 0..maps.len() {
        for b in (a + 1)..maps.len() {
            for (idx, data_a) in &maps[a] {
                if let Some(data_b) = maps[b].get(idx) {
                    assert_eq!(
                        data_a, data_b,
                        "INV8 violated: nodes {} and {} disagree at index {idx}",
                        a + 1,
                        b + 1
                    );
                }
            }
        }
    }
}

/// INV3: nodes that applied the same committed prefix must have byte-identical
/// state-machine snapshots.
fn assert_state_agreement(c: &Cluster, nodes: &[RaftId]) {
    let snaps: Vec<Vec<u8>> = nodes
        .iter()
        .map(|&id| c.sms[(id - 1) as usize].snapshot().expect("snapshot"))
        .collect();
    for w in snaps.windows(2) {
        assert_eq!(w[0], w[1], "INV3 violated: state snapshots disagree");
    }
}

/// S01: elect a leader, commit, crash it, elect a new leader among the
/// survivors, then revive the old leader. Asserts INV7 (single leader per term)
/// and INV9 (the new leader holds every previously acked entry), and that all
/// nodes converge to identical logs (INV8) and state (INV3).
#[test]
fn s01_leader_crash_revive_preserves_invariants() {
    let _seed = ElectionSeed::enter(0x5010_0001);
    let mut c = Cluster::new(3);

    // 1. Elect and commit an acked entry.
    let old_leader = elect(&mut c);
    // Drive a few more rounds so the acked entry is definitely committed.
    assert!(c.propose(old_leader, b"k0", b"v0", 1));
    for _ in 0..300 {
        c.round();
    }
    let acked_index = c.committed[(old_leader - 1) as usize]
        .last()
        .map(|(i, _)| *i)
        .expect("leader committed the first entry");

    // 2. Crash the leader: isolate it fully (no messages in or out).
    c.faults.isolate(old_leader);

    // 3. The two survivors must elect a new leader.
    let mut new_leader = 0;
    for _ in 0..600 {
        c.round();
        if let Some(l) = c.any_leader() {
            if l != old_leader {
                new_leader = l;
                break;
            }
        }
    }
    assert!(new_leader != 0, "survivors must elect a new leader");

    // INV9: the new leader holds the previously acked entry.
    let holds = c.committed[(new_leader - 1) as usize]
        .iter()
        .any(|(i, _)| *i == acked_index);
    assert!(holds, "INV9 violated: new leader is missing the acked entry");

    // 4. Revive the old leader; drive to convergence.
    c.faults.isolated.clear();
    for _ in 0..800 {
        c.round();
    }

    // INV7 across the whole run (no term with two leaders).
    assert_single_leader_per_term(&c.leader_obs);
    // INV8: all three nodes agree on overlapping log indices.
    assert_log_matching(&c.committed);
    // INV3: nodes that caught up share a byte-identical state snapshot.
    assert_state_agreement(&c, &[1, 2, 3]);

    c.cleanup();
}

// ---------------------------------------------------------------------------
// INV4: linearizability via the ClientOracle + the self-built checker
// ---------------------------------------------------------------------------

fn vbytes(v: ValueId) -> Vec<u8> {
    v.0.to_be_bytes().to_vec()
}

fn parse_v(b: &[u8]) -> Option<ValueId> {
    <[u8; 8]>::try_from(b).ok().map(|a| ValueId(u64::from_be_bytes(a)))
}

/// INV4 (M1 acceptance ④): under an S02 partition, a client's `put`/`get`
/// history must be **linearizable** — verified independently by the oracle
/// (its always-on checks) and by the self-built Wing–Gong checker.
#[test]
fn inv4_client_history_is_linearizable_under_partition() {
    let _seed = ElectionSeed::enter(0x1_4A4);
    let mut c = Cluster::new(3);
    let leader = elect(&mut c);

    // Partition a follower; the leader + the other follower keep a majority, so
    // client writes keep committing (a non-trivial linearization under fault).
    let victim = (1..=3).find(|&i| i != leader).expect("a follower exists");
    c.faults.isolate(victim);
    let victim_len_before = c.committed[(victim - 1) as usize].len();

    let mut h = History::new();
    let mut ts = 0u64;
    let mut version = 0u64;

    for round in 0..5u64 {
        // --- Put(k, version) ---------------------------------------------
        version += 1;
        let value = ValueId(version);
        let put_call = CallId(round * 2 + 1);
        let put_seq = SeqNo(round * 2 + 1);
        h.invoke(
            put_call,
            ClientId(0),
            put_seq,
            Op::Put {
                key: b"k".to_vec(),
                value,
            },
            ts,
        );
        ts += 1;
        assert!(
            c.propose(leader, b"k", &vbytes(value), round * 2 + 1),
            "leader accepts the put"
        );
        for _ in 0..400 {
            c.round();
        }
        let seen = c.sms[(leader - 1) as usize].get(b"k").expect("sm get");
        assert_eq!(
            seen.as_deref().and_then(parse_v),
            Some(value),
            "leader must have applied the put"
        );
        h.complete(put_call, ts, OpResult::Ok(None));
        ts += 1;

        // --- Get(k) ------------------------------------------------------
        let get_call = CallId(round * 2 + 2);
        let get_seq = SeqNo(round * 2 + 2);
        h.invoke(get_call, ClientId(0), get_seq, Op::Get { key: b"k".to_vec() }, ts);
        ts += 1;
        let read = c.sms[(leader - 1) as usize].get(b"k").expect("sm get");
        h.complete(get_call, ts, OpResult::Ok(read.as_deref().and_then(parse_v)));
        ts += 1;
    }

    // The partition is OBSERVED: the isolated follower never saw any
    // partition-era write, while the majority committed them all.
    assert_eq!(
        c.committed[(victim - 1) as usize].len(),
        victim_len_before,
        "isolated follower must not observe the partition-era writes"
    );

    // Oracle: all always-on checks (phantom / one-log-id / RYW / monotonic /
    // well-formed).
    let report = h.check();
    assert!(report.passed(), "oracle failures: {}", report.render());

    // Checker: complete linearizability over the reduced history.
    let outcome = check_linearizable(&h, &KvState::new());
    assert!(
        matches!(outcome, CheckOutcome::Linearizable),
        "checker verdict: {outcome:?}"
    );

    c.cleanup();
}

// ---------------------------------------------------------------------------
// S16 (asymmetric partition) + INV4 across a mid-history failover
// ---------------------------------------------------------------------------

/// Commit `put(key, value)` on `leader` and assert the leader applied it.
fn commit_put(c: &mut Cluster, leader: RaftId, key: &[u8], value: ValueId, seq: RaftId) {
    assert!(
        c.propose(leader, key, &vbytes(value), seq),
        "leader accepts the put"
    );
    for _ in 0..400 {
        c.round();
    }
    assert_eq!(
        c.sms[(leader - 1) as usize]
            .get(key)
            .expect("sm get")
            .as_deref()
            .and_then(parse_v),
        Some(value),
        "leader must have applied the put"
    );
}

/// Advance rounds until a node other than `avoid` reports itself leader.
fn wait_new_leader(c: &mut Cluster, avoid: RaftId) -> RaftId {
    for _ in 0..1500 {
        c.round();
        if let Some(l) = (1..=c.n).find(|&i| i != avoid && c.leader_of(i)) {
            return l;
        }
    }
    panic!("no new leader after failover");
}

/// S16: drop every follower→leader message. The leader keeps sending but never
/// hears acknowledgements, so CheckQuorum must step it down; INV7 still holds.
#[test]
fn s16_asymmetric_partition_steps_down_leader() {
    let _seed = ElectionSeed::enter(0x5160_0001);
    let mut c = Cluster::new(3);
    let leader = elect(&mut c);

    for f in 1..=3 {
        if f != leader {
            c.faults.oneway(f, leader);
        }
    }
    for _ in 0..1500 {
        c.round();
    }

    assert!(
        !c.leader_of(leader),
        "an asymmetric partition must step the leader down (CheckQuorum)"
    );
    assert!(
        c.any_leader().is_some(),
        "the survivors must elect a new leader after the step-down"
    );
    assert_single_leader_per_term(&c.leader_obs);
    c.cleanup();
}

/// INV4 across a mid-history failover: isolate the leader, let the survivors
/// elect a new one, redirect the client, and require the whole history to stay
/// linearizable (oracle + checker).
#[test]
fn inv4_failover_mid_history_is_linearizable() {
    let _seed = ElectionSeed::enter(0x1_4F0);
    let mut c = Cluster::new(3);
    let mut leader = elect(&mut c);
    let initial_leader = leader;

    let mut h = History::new();
    let mut ts = 0u64;
    let mut version = 0u64;

    for round in 0..6u64 {
        if round == 2 {
            c.faults.isolate(leader);
            leader = wait_new_leader(&mut c, leader);
        }

        version += 1;
        let value = ValueId(version);
        let put_call = CallId(round * 2 + 1);
        h.invoke(
            put_call,
            ClientId(0),
            SeqNo(round * 2 + 1),
            Op::Put { key: b"k".to_vec(), value },
            ts,
        );
        ts += 1;
        commit_put(&mut c, leader, b"k", value, round * 2 + 1);
        h.complete(put_call, ts, OpResult::Ok(None));
        ts += 1;

        let get_call = CallId(round * 2 + 2);
        h.invoke(get_call, ClientId(0), SeqNo(round * 2 + 2), Op::Get { key: b"k".to_vec() }, ts);
        ts += 1;
        let read = c.sms[(leader - 1) as usize].get(b"k").expect("sm get");
        h.complete(get_call, ts, OpResult::Ok(read.as_deref().and_then(parse_v)));
        ts += 1;
    }

    // Observational (system-derived, not counter bookkeeping): the trace must
    // show a leader other than the initial one after the failover.
    assert!(
        c.leader_obs.iter().any(|&(_, l)| l != initial_leader),
        "a new leader must appear in the observed trace after the failover"
    );
    let report = h.check();
    assert!(report.passed(), "oracle failures after failover: {}", report.render());
    let outcome = check_linearizable(&h, &KvState::new());
    assert!(
        matches!(outcome, CheckOutcome::Linearizable),
        "checker after failover: {outcome:?}"
    );

    c.cleanup();
}

// ---------------------------------------------------------------------------
// Increment 3: concurrent (overlapping) ops + transport-level ReadIndex
// ---------------------------------------------------------------------------

/// INV4 with two clients whose operations overlap in real time — this exercises
/// the checker's concurrency machinery (prior histories were sequential).
#[test]
fn inv4_two_clients_overlapping_ops_linearizable() {
    let _seed = ElectionSeed::enter(0x1_4C0);
    let mut c = Cluster::new(3);
    let leader = elect(&mut c);

    let mut h = History::new();
    let mut ts = 0u64;
    let mut version = 0u64;

    for r in 0..4u64 {
        version += 1;
        let value = ValueId(version);
        let put_call = CallId(r * 4 + 1);
        let get_call = CallId(r * 4 + 2);

        // Invoke BOTH before completing either ⇒ real-time overlap.
        h.invoke(put_call, ClientId(0), SeqNo(r), Op::Put { key: b"k".to_vec(), value }, ts);
        ts += 1;
        h.invoke(get_call, ClientId(1), SeqNo(r), Op::Get { key: b"k".to_vec() }, ts);
        ts += 1;

        // The put commits; the concurrent get then observes it.
        commit_put(&mut c, leader, b"k", value, r * 4 + 1);
        let read = c.sms[(leader - 1) as usize].get(b"k").expect("sm get");
        h.complete(put_call, ts, OpResult::Ok(None));
        ts += 1;
        h.complete(get_call, ts, OpResult::Ok(read.as_deref().and_then(parse_v)));
        ts += 1;
    }

    let report = h.check();
    assert!(report.passed(), "oracle failures (2 clients): {}", report.render());
    let outcome = check_linearizable(&h, &KvState::new());
    assert!(
        matches!(outcome, CheckOutcome::Linearizable),
        "checker (2 clients): {outcome:?}"
    );
    c.cleanup();
}

/// ReadIndex is served only on the leader: the leader emits a read state for its
/// request, while a follower (which forwards the request) emits none locally.
#[test]
fn read_index_is_served_only_on_the_leader() {
    let _seed = ElectionSeed::enter(0x1_4D0);
    let mut c = Cluster::new(3);
    let leader = elect(&mut c);
    // Ensure the leader has committed in its term (ReadIndex is ignored until
    // then).
    commit_put(&mut c, leader, b"k", ValueId(1), 1);

    let leader_ctx = vec![1u8; 8];
    c.nodes[(leader - 1) as usize]
        .as_mut()
        .expect("leader present")
        .read_index(leader_ctx.clone());
    for _ in 0..200 {
        c.round();
    }
    assert!(
        c.read_states
            .iter()
            .any(|(n, ctx, _)| *n == leader && *ctx == leader_ctx),
        "the leader must emit a read state for its ReadIndex request"
    );

    let follower = (1..=3).find(|&i| i != leader).expect("a follower exists");
    let follower_ctx = vec![2u8; 8];
    c.nodes[(follower - 1) as usize]
        .as_mut()
        .expect("follower present")
        .read_index(follower_ctx.clone());
    // Without delivery the follower cannot complete a quorum round, so it must
    // not serve the read itself — only the leader's round yields a read state.
    let mut served_locally = false;
    for _ in 0..20 {
        for (ctx, _) in c.step_local((follower - 1) as usize) {
            if ctx == follower_ctx {
                served_locally = true;
            }
        }
    }
    assert!(!served_locally, "a follower must not serve ReadIndex itself");

    c.cleanup();
}

// ---------------------------------------------------------------------------
// Increment 4: multi-key histories (phantom check) + overlap under partition
// ---------------------------------------------------------------------------

/// INV4 over multiple keys: writes/reads alternate between two keys, plus a read
/// of a never-written key (must be `None`) — this exercises the oracle's
/// per-key phantom-value check, not just a single key.
#[test]
fn inv4_multi_key_history_linearizable() {
    let _seed = ElectionSeed::enter(0x1_4E0);
    let mut c = Cluster::new(3);
    let leader = elect(&mut c);

    let mut h = History::new();
    let mut ts = 0u64;
    let mut version = 0u64;
    let keys: [&[u8]; 2] = [b"a", b"b"];

    for r in 0..4u64 {
        version += 1;
        let value = ValueId(version);
        let key = keys[(r % 2) as usize];
        let put_call = CallId(r * 2 + 1);
        let get_call = CallId(r * 2 + 2);

        h.invoke(put_call, ClientId(0), SeqNo(r * 2 + 1), Op::Put { key: key.to_vec(), value }, ts);
        ts += 1;
        commit_put(&mut c, leader, key, value, r * 2 + 1);
        h.complete(put_call, ts, OpResult::Ok(None));
        ts += 1;

        h.invoke(get_call, ClientId(0), SeqNo(r * 2 + 2), Op::Get { key: key.to_vec() }, ts);
        ts += 1;
        let read = c.sms[(leader - 1) as usize].get(key).expect("sm get");
        h.complete(get_call, ts, OpResult::Ok(read.as_deref().and_then(parse_v)));
        ts += 1;
    }

    // A read of a never-written key must be None (no phantom).
    let absent_call = CallId(99);
    h.invoke(absent_call, ClientId(0), SeqNo(99), Op::Get { key: b"c".to_vec() }, ts);
    ts += 1;
    let read = c.sms[(leader - 1) as usize].get(b"c").expect("sm get");
    assert_eq!(read, None, "a never-written key must read None");
    h.complete(absent_call, ts, OpResult::Ok(None));

    let report = h.check();
    assert!(report.passed(), "oracle failures (multi-key): {}", report.render());
    let outcome = check_linearizable(&h, &KvState::new());
    assert!(
        matches!(outcome, CheckOutcome::Linearizable),
        "checker (multi-key): {outcome:?}"
    );
    c.cleanup();
}

/// INV4 with two clients whose operations overlap **during** a 2+1 partition:
/// the majority keeps serving, the isolated follower is starved, and the history
/// must still be linearizable.
#[test]
fn inv4_concurrent_ops_during_partition_linearizable() {
    let _seed = ElectionSeed::enter(0x1_4F1);
    let mut c = Cluster::new(3);
    let leader = elect(&mut c);
    let victim = (1..=3).find(|&i| i != leader).expect("a follower exists");
    c.faults.isolate(victim);
    let victim_len_before = c.committed[(victim - 1) as usize].len();

    let mut h = History::new();
    let mut ts = 0u64;
    let mut version = 0u64;

    for r in 0..4u64 {
        version += 1;
        let value = ValueId(version);
        let put_call = CallId(r * 4 + 1);
        let get_call = CallId(r * 4 + 2);

        h.invoke(put_call, ClientId(0), SeqNo(r), Op::Put { key: b"k".to_vec(), value }, ts);
        ts += 1;
        h.invoke(get_call, ClientId(1), SeqNo(r), Op::Get { key: b"k".to_vec() }, ts);
        ts += 1;

        commit_put(&mut c, leader, b"k", value, r * 4 + 1);
        let read = c.sms[(leader - 1) as usize].get(b"k").expect("sm get");
        h.complete(put_call, ts, OpResult::Ok(None));
        ts += 1;
        h.complete(get_call, ts, OpResult::Ok(read.as_deref().and_then(parse_v)));
        ts += 1;
    }

    assert_eq!(
        c.committed[(victim - 1) as usize].len(),
        victim_len_before,
        "the isolated follower must stay starved"
    );
    let report = h.check();
    assert!(report.passed(), "oracle failures (overlap+partition): {}", report.render());
    let outcome = check_linearizable(&h, &KvState::new());
    assert!(
        matches!(outcome, CheckOutcome::Linearizable),
        "checker (overlap+partition): {outcome:?}"
    );
    c.cleanup();
}

/// INV4 where a client operation's real-time interval **spans a leadership
/// change**: the put/get are invoked before the failover and complete after it,
/// on the new leader. Exercises the interplay of concurrency and failover.
#[test]
fn inv4_concurrent_ops_spanning_failover_linearizable() {
    let _seed = ElectionSeed::enter(0x1_500);
    let mut c = Cluster::new(3);
    let mut leader = elect(&mut c);
    let initial_leader = leader;

    // A pre-failover committed entry on a separate key, to pin retention across
    // the leadership change (the recorded history only covers key "k").
    commit_put(&mut c, leader, b"old", ValueId(1000), 1000);

    let mut h = History::new();
    let mut ts = 0u64;
    let mut version = 0u64;

    for r in 0..5u64 {
        version += 1;
        let value = ValueId(version);
        let put_call = CallId(r * 4 + 1);
        let get_call = CallId(r * 4 + 2);

        // Invoke both ops...
        h.invoke(put_call, ClientId(0), SeqNo(r), Op::Put { key: b"k".to_vec(), value }, ts);
        ts += 1;
        h.invoke(get_call, ClientId(1), SeqNo(r), Op::Get { key: b"k".to_vec() }, ts);
        ts += 1;

        // ...then, mid-op, the leader fails over; the ops complete on the new
        // leader (their intervals span the leadership change).
        if r == 2 {
            c.faults.isolate(leader);
            leader = wait_new_leader(&mut c, leader);
            // INV: the new leader retains the pre-failover committed entry.
            assert_eq!(
                c.sms[(leader - 1) as usize].get(b"old").expect("sm get"),
                Some(vbytes(ValueId(1000))),
                "the new leader must retain the pre-failover committed entry"
            );
        }

        commit_put(&mut c, leader, b"k", value, r * 4 + 1);
        let read = c.sms[(leader - 1) as usize].get(b"k").expect("sm get");
        h.complete(put_call, ts, OpResult::Ok(None));
        ts += 1;
        h.complete(get_call, ts, OpResult::Ok(read.as_deref().and_then(parse_v)));
        ts += 1;
    }

    assert!(
        c.leader_obs.iter().any(|&(_, l)| l != initial_leader),
        "the trace must show a leader change caused by the failover"
    );
    assert_single_leader_per_term(&c.leader_obs);
    let report = h.check();
    assert!(report.passed(), "oracle failures (spanning failover): {}", report.render());
    let outcome = check_linearizable(&h, &KvState::new());
    assert!(
        matches!(outcome, CheckOutcome::Linearizable),
        "checker (spanning failover): {outcome:?}"
    );
    c.cleanup();
}
