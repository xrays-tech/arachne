//! M1 (C) Stage 2 — D-S1 double-run determinism canary (test-plan §5 E3).
//!
//! The consensus core (`raft` 0.7.0) randomizes each node's election timeout
//! with the process-wide, unseedable `thread_rng`. That jitter is an entropy
//! source that breaks the L2 same-seed double-run gate: two runs of the same
//! schedule could elect different leaders at different ticks, so no byte-
//! identical repro is provable.
//!
//! This canary is the *gate* for the D-S1 `[patch.crates-io]` RNG hook
//! (`third_party/raft/ARACHNE-PATCH.md`): it proves that, once a seed is set
//! via `raft::set_election_rng_seed`, a fixed schedule of rounds over a 3-node
//! cluster reproduces **exactly**, run after run. It is the smallest thing that
//! would fail if the election RNG were not injectable.
//!
//! # What is checked
//!
//! 1. **Double-run determinism**: the *same* seed drives two independent
//!    3-node clusters (real [`WalStorage`], in-memory transport, the same
//!    known-good `RaftNode` harness as `m0_determinism.rs`) through an
//!    identical 60-round schedule; the per-round, per-node
//!    `(term, leader, commit_index)` trace must be byte-identical, and a stable
//!    leader must be elected (non-vacuity).
//! 2. **The seed matters**: two *different* fixed seeds yield different
//!    per-node `randomized_election_timeout()` vectors, read directly from a
//!    raw `Raft` (asserted deterministically — never against the unseeded
//!    `thread_rng`, which would be flaky).
//!
//! # Determinism / gates
//!
//! Plain synchronous `#[test]` — no tokio, no threads, no wall clock, no
//! real-time or network APIs; time is advanced purely by logical tick counts
//! (`scripts/check-entropy.sh` Gate C). A small RAII guard clears the seed on
//! drop so a leaked seed cannot leak into other tests on the same worker
//! thread.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use arachne::consensus::RaftNode;
use arachne::state_machine::KvStateMachine;
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{NodeId, RaftId, StateMachine, TransportFactory, TransportMessage};
use arachne_testsupport::{block_on, InMemoryRx, InMemoryTransportFactory, InMemoryTx};
use raft::prelude::{ConfState, Config, Raft};
use raft::storage::MemStorage;
use raft::{clear_election_rng_seed, set_election_rng_seed};
use slog::{o, Drain, Logger};

type TestNode = RaftNode<WalStorage, InMemoryTx, InMemoryRx>;

const N: u64 = 3;
const ROUNDS: usize = 60;

/// Fixed seeds for the two independent runs (arbitrary distinct constants).
const SEED_A: u64 = 0x2026_0919_1c2b_3d4e;
const SEED_B: u64 = 0x9e37_79b9_7f4a_7c15;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-detcanary-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn wal_opts(node: &str) -> WalOptions {
    WalOptions {
        cluster_id: "detcanary".into(),
        node_id: node.into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 1 << 20,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
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

/// RAII seed scope: sets the D-S1 base seed on creation and clears it on drop,
/// so a test that panics mid-run cannot leave a seed that perturbs a later test
/// sharing this worker thread.
struct SeedScope;

impl SeedScope {
    fn enter(seed: u64) -> Self {
        set_election_rng_seed(seed);
        Self
    }
}

impl Drop for SeedScope {
    fn drop(&mut self) {
        clear_election_rng_seed();
    }
}

/// A 3-node cluster of [`RaftNode`] over the in-memory transport — the same
/// known-good harness pattern as `m0_determinism.rs`.
struct Cluster {
    dirs: Vec<PathBuf>,
    nodes: Vec<Option<TestNode>>,
    sms: Vec<KvStateMachine>,
}

impl Cluster {
    fn new() -> Self {
        // The factory is dropped at the end of this constructor; the transport
        // stays connected because every node's outbound half holds an `Arc` to
        // the shared switch.
        let factory = InMemoryTransportFactory::new();
        let mut dirs = Vec::new();
        let mut nodes = Vec::new();
        let mut sms = Vec::new();

        for i in 1..=N {
            let dir = temp_dir(&format!("n{i}"));
            let wal = WalStorage::open(&dir, wal_opts(&format!("n{i}"))).expect("open wal");
            let (tx, rx) = factory.create(node_id(i));
            let node = RaftNode::new(i, peers_of(i, N), wal, tx, rx, 0, &logger())
                .expect("build node");
            dirs.push(dir);
            nodes.push(Some(node));
            sms.push(KvStateMachine::new());
        }

        Self { dirs, nodes, sms }
    }

    /// One round: drive every node (tick + step + advance_apply, applying
    /// committed entries to the state machine), then deliver each node's queued
    /// inbound raft messages. Fully deterministic: no message delay, no
    /// concurrency — the same seed therefore yields the same trajectory.
    fn round(&mut self) {
        for i in 0..self.nodes.len() {
            if let Some(node) = self.nodes[i].as_mut() {
                node.tick();
                let entries = block_on(node.step()).expect("step").committed;
                for (idx, _kind, data) in entries {
                    self.sms[i].apply(idx, &data).expect("apply");
                }
                node.advance_apply();
            }
        }

        for i in 0..self.nodes.len() {
            let mut inbox: Vec<(RaftId, TransportMessage)> = Vec::new();
            if let Some(node) = self.nodes[i].as_mut() {
                loop {
                    match node.rx().try_recv() {
                        Ok(Some((from, msg))) => inbox.push((parse_raft_id(&from), msg)),
                        Ok(None) | Err(_) => break,
                    }
                }
            }
            if let Some(node) = self.nodes[i].as_mut() {
                for (from, msg) in inbox {
                    let _ = node.on_message(from, msg);
                }
            }
        }
    }

    /// Per-node `(term, leader_id, commit_index)` at the current round.
    fn snapshot(&self) -> Vec<(u64, u64, u64)> {
        self.nodes
            .iter()
            .map(|n| {
                let node = n.as_ref().expect("node present");
                (
                    node.hard_state().term,
                    node.leader_id(),
                    node.hard_state().commit,
                )
            })
            .collect()
    }

    /// Drive `rounds` rounds and return the full `(term, leader, commit)` trace.
    fn run(&mut self, rounds: usize) -> Vec<Vec<(u64, u64, u64)>> {
        let mut trace = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            self.round();
            trace.push(self.snapshot());
        }
        trace
    }

    /// Tear down: drop the nodes (releasing WAL locks) and remove the dirs.
    fn cleanup(&mut self) {
        self.nodes.iter_mut().for_each(|n| *n = None);
        for dir in &self.dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// Run one fixed 60-round schedule on a fresh cluster under `seed`, returning
/// its `(term, leader, commit)` trace. A pure function of the (fixed) schedule
/// + seed: same inputs, same bytes, run after run.
fn run_once(seed: u64) -> Vec<Vec<(u64, u64, u64)>> {
    let _seed = SeedScope::enter(seed);
    let mut cluster = Cluster::new();
    let trace = cluster.run(ROUNDS);
    cluster.cleanup();
    trace
}

/// The per-node `randomized_election_timeout()` drawn under the currently set
/// seed, for a standalone raw `Raft` per id (1..=3). A wide range makes two
/// different seeds easy to distinguish deterministically.
fn election_timeouts() -> Vec<u64> {
    let logger = logger();
    let voters: Vec<u64> = (1..=N).collect();
    (1..=N)
        .map(|id| {
            let config = Config {
                id,
                election_tick: 10,
                heartbeat_tick: 2,
                min_election_tick: 10,
                max_election_tick: 100,
                ..Config::default()
            };
            config.validate().expect("config must be valid");
            let storage =
                MemStorage::new_with_conf_state(ConfState::from((voters.iter().copied(), Vec::<u64>::new())));
            let mut raft = Raft::new(&config, storage, &logger).expect("build raft");
            raft.reset_randomized_election_timeout();
            raft.randomized_election_timeout() as u64
        })
        .collect()
}

/// D-S1 gate (primary): the same seed, run twice, must reproduce an identical
/// `(term, leader, commit)` trace across the whole schedule, and a stable
/// leader must be elected (non-vacuity).
#[test]
fn double_run_same_seed_is_deterministic() {
    let trace_a = run_once(SEED_A);
    let trace_b = run_once(SEED_A);

    assert_eq!(
        trace_a, trace_b,
        "the same seed must reproduce an identical (term, leader, commit) trace"
    );

    // Non-vacuity: by the end of the window every node agrees on a single,
    // non-zero leader (a real, stable leader was elected — not 60 rounds of an
    // all-idle or leaderless cluster).
    let last = trace_a.last().expect("trace is non-empty");
    let leaders: Vec<u64> = last.iter().map(|t| t.1).collect();
    assert!(
        leaders.iter().all(|&l| l != 0 && l == leaders[0]),
        "all nodes must agree on a single leader by the end (last: {last:?})"
    );
    // And terms advanced past the initial term (an election actually happened).
    assert!(
        last.iter().any(|t| t.0 != 0),
        "a term must have been elected (last: {last:?})"
    );
}

/// D-S1 gate (secondary): two different fixed seeds must yield different
/// per-node election-timeout vectors. Asserted directly (deterministic), never
/// against the unseeded `thread_rng`.
#[test]
fn different_seeds_change_election_timeouts() {
    let a = {
        let _seed = SeedScope::enter(SEED_A);
        election_timeouts()
    };
    let b = {
        let _seed = SeedScope::enter(SEED_B);
        election_timeouts()
    };

    assert!(
        a.iter().zip(&b).any(|(x, y)| x != y),
        "two different seeds must yield different per-node election timeouts: seed_a={a:?} seed_b={b:?}"
    );
    // Sanity: every drawn timeout lies within the configured [10, 100) range.
    assert!(a.iter().all(|&t| (10..100).contains(&t)), "timeouts must be in [10,100): {a:?}");
    assert!(b.iter().all(|&t| (10..100).contains(&t)), "timeouts must be in [10,100): {b:?}");
}
