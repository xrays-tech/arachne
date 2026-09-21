//! M0 "双跑复现门禁" — deterministic double-run reproducibility gate.
//!
//! The core must be a pure function of its inputs: given the same scenario,
//! two independent runs must produce **byte-identical** outcomes. Any
//! divergence is a determinism leak (an entropy source: RNG, wall clock,
//! thread scheduling, or map-iteration order) and is treated as a P0 bug.
//!
//! # What is compared
//!
//! Two fresh 3-node clusters (real [`WalStorage`], in-memory transport, no
//! tokio, no threads) run the same fixed scenario — elect a leader, propose
//! three commands, drive to full commit — and we assert that across runs:
//!
//! 1. every node's **committed log** (the `(index, data)` prefix it applied)
//!    is byte-identical;
//! 2. every node's **state-machine snapshot** (KV + session table, `Vec<u8>`)
//!    is byte-identical;
//! 3. every node's **final commit index** is identical.
//!
//! # A note on the raft election RNG (E3)
//!
//! `raft` 0.7 randomizes the *election timeout* via `thread_rng`
//! (`reset_randomized_election_timeout`). This affects *which* node wins the
//! election and *when*, but **not** the committed outcomes: with a stable
//! single-term cluster (heartbeats every tick keep the leader in power), the
//! replicated log — one term's no-op followed by the three commands — is
//! identical regardless of which node became leader. The gate therefore
//! compares *outcomes* (log, state, commit index), which is the level at
//! which determinism must hold. If a future change introduces entropy that
//! affects outcomes (e.g. a wall clock in the apply path, or map-iteration
//! order in message emission), the two runs diverge and this test fails.
//!
//! Deterministic and in-process: no threads, no wall clock, no tokio, and no
//! real-time/network APIs (the absence is enforced by `scripts/check-entropy.sh`
//! Gate C).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use arachne::consensus::RaftNode;
use arachne::state_machine::KvStateMachine;
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{LogIndex, NodeId, RaftId, StateMachine, TransportFactory, TransportMessage};
use arachne_testsupport::{block_on, InMemoryRx, InMemoryTransportFactory, InMemoryTx};
use slog::{o, Drain, Logger};

type TestNode = RaftNode<WalStorage, InMemoryTx, InMemoryRx>;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-m0-det-{tag}-{}-{n}",
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
        cluster_id: "m0-det".into(),
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

/// The fixed commands the scenario proposes (in this order).
const KEYS: [&[u8]; 3] = [b"k1", b"k2", b"k3"];
const VALS: [&[u8]; 3] = [b"v1", b"v2", b"v3"];

// ---------------------------------------------------------------------------
// Three-node cluster (same harness pattern as `m0_three_node`)
// ---------------------------------------------------------------------------

struct Cluster {
    dirs: Vec<PathBuf>,
    nodes: Vec<Option<TestNode>>,
    sms: Vec<KvStateMachine>,
    /// Per-node committed log: `(index, data)` entries applied in order.
    committed: Vec<Vec<(RaftId, Vec<u8>)>>,
}

impl Cluster {
    fn new(n: u64) -> Self {
        // The in-memory transport factory is dropped at the end of this
        // constructor; the transport stays connected because every node's
        // outbound half holds an `Arc` to the shared switch.
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
            dirs,
            nodes,
            sms,
            committed,
        }
    }

    /// One round: drive every live node, then deliver queued inbound messages.
    fn round(&mut self) {
        for i in 0..self.nodes.len() {
            if let Some(node) = self.nodes[i].as_mut() {
                node.tick();
                let entries = block_on(node.step()).expect("step").committed;
                for (idx, _kind, data) in entries {
                    self.sms[i].apply(idx, &data).expect("apply");
                    self.committed[i].push((idx, data));
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

    fn leader_index(&self) -> Option<usize> {
        self.nodes.iter().enumerate().find_map(|(i, n)| {
            n.as_ref().and_then(|node| {
                let id = i as RaftId + 1;
                (node.leader_id() == id).then_some(i)
            })
        })
    }

    /// True once every node's state machine holds all three commands.
    fn all_applied(&self) -> bool {
        self.sms.iter().all(|sm| {
            KEYS.iter().zip(VALS.iter()).all(|(k, v)| sm.get(k).expect("get") == Some(v.to_vec()))
        })
    }

    /// Tear down: drop the nodes (releasing WAL locks) and remove the dirs.
    fn cleanup(&mut self) {
        self.nodes.iter_mut().for_each(|n| *n = None);
        for dir in &self.dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

// ---------------------------------------------------------------------------
// Scenario + outcome
// ---------------------------------------------------------------------------

/// The byte-comparable outcome of one scenario run.
#[derive(Debug, PartialEq, Eq)]
struct ScenarioOutcome {
    /// Per-node committed log: `(index, data)` entries, in apply order.
    committed: Vec<Vec<(RaftId, Vec<u8>)>>,
    /// Per-node state-machine snapshot (KV + session table), as opaque bytes.
    snapshots: Vec<Vec<u8>>,
    /// Per-node final commit index.
    commit_indices: Vec<LogIndex>,
}

/// Run the fixed 3-node scenario once on a fresh cluster and return its
/// outcome. This is a pure function of the (fixed) scenario inputs: same
/// inputs, same bytes, run after run.
fn run_scenario() -> ScenarioOutcome {
    let mut c = Cluster::new(3);

    // Phase 1: elect a leader (driven to completion; cap 300 ticks).
    for _ in 0..300 {
        c.round();
        if c.leader_index().is_some() {
            break;
        }
    }
    let li = c.leader_index().expect("scenario must elect a leader");

    // Phase 2: propose the fixed commands immediately (all in term 1).
    for (seq, (key, val)) in KEYS.iter().zip(VALS.iter()).enumerate() {
        let cmd = KvStateMachine::encode_put(1, (seq + 1) as RaftId, key, val);
        c.nodes[li].as_mut().expect("leader present").propose(&cmd).expect("propose");
    }

    // Phase 3: drive to full commit on every node.
    for _ in 0..400 {
        c.round();
        if c.all_applied() {
            break;
        }
    }
    assert!(c.all_applied(), "scenario must converge to full commit");

    let committed = c.committed.clone();
    let snapshots = c.sms.iter().map(|sm| sm.snapshot().expect("snapshot")).collect();
    let commit_indices = c
        .nodes
        .iter()
        .map(|n| n.as_ref().expect("node present").hard_state().commit)
        .collect();

    c.cleanup();
    ScenarioOutcome {
        committed,
        snapshots,
        commit_indices,
    }
}

// ---------------------------------------------------------------------------
// Double-run gate
// ---------------------------------------------------------------------------

/// The M0 双跑复现门禁: two independent runs of the same fixed scenario must
/// produce byte-identical outcomes (committed logs, state-machine snapshots,
/// and final commit indices). Divergence is a determinism leak = P0.
#[test]
fn double_run_is_byte_identical() {
    let run1 = run_scenario();
    let run2 = run_scenario();

    // 1. Committed logs, byte-identical per node.
    assert_eq!(
        run1.committed, run2.committed,
        "committed logs must be byte-identical across runs"
    );

    // 2. State-machine snapshots, byte-identical per node.
    assert_eq!(
        run1.snapshots, run2.snapshots,
        "state-machine snapshots must be byte-identical across runs"
    );

    // 3. Final commit indices, identical per node.
    assert_eq!(
        run1.commit_indices, run2.commit_indices,
        "final commit indices must be identical across runs"
    );

    // Non-vacuity: the scenario did real work (not a trivially-empty run).
    assert!(
        run1.commit_indices.iter().all(|&ci| ci >= 3),
        "each node must have committed the three commands (commit indices: {:?})",
        run1.commit_indices
    );
    assert!(
        run1.snapshots.iter().all(|s| !s.is_empty()),
        "state-machine snapshots must be non-empty (real work was applied)"
    );
    // Every node must agree on the same committed log (consensus).
    assert!(
        run1.committed.iter().all(|log| *log == run1.committed[0]),
        "all nodes must have identical committed logs (consensus)"
    );
}
