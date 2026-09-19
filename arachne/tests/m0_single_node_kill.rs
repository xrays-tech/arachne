//! M0 acceptance ① (INV2): single-node kill-at-each-`Ready`-step sweep.
//!
//! For every step count `k` in `0..=MAX_STEPS`, build a fresh single-node
//! cluster over a real `WalStorage`, drive `k` `Ready` cycles, kill the node
//! (drop it, releasing the WAL dir lock), reopen the WAL, and assert:
//!   (a) the durable log covers every applied index, byte-identical to the
//!       pre-kill applied prefix — i.e. **no committed/applied entry is lost**;
//!   (b) replaying the recovered prefix into a fresh state machine reproduces
//!       the pre-kill state machine exactly; and
//!   (c) after restart, the node **re-reaches** the pre-kill commit index.
//!
//! The raft commit index itself is deliberately **not** required to be durable
//! (raft re-derives it on restart, as (c) demonstrates); the entries are what
//! must survive (invariants I2/I4).
//!
//! Deterministic and in-process: no threads, no wall clock, no tokio.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use arachne::consensus::RaftNode;
use arachne::state_machine::KvStateMachine;
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{LogEntry, NodeId, RaftId, StateMachine, Storage, TransportFactory};
use arachne_testsupport::{block_on, InMemoryRx, InMemoryTransportFactory, InMemoryTx};
use slog::{o, Drain, Logger};

type TestNode = RaftNode<WalStorage, InMemoryTx, InMemoryRx>;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-m0-single-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn wal_opts() -> WalOptions {
    WalOptions {
        cluster_id: "m0".into(),
        node_id: "n1".into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 1 << 20,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

/// Single-node cluster: voter set `{1}` (bootstrap adds self).
fn build(dir: &PathBuf, factory: &InMemoryTransportFactory, applied: u64) -> TestNode {
    let wal = WalStorage::open(dir, wal_opts()).expect("open wal");
    let (tx, rx) = factory.create(NodeId::from("n1"));
    RaftNode::new(1, HashMap::new(), wal, tx, rx, applied, &logger()).expect("build node")
}

/// All durable log entries `[first_index, last_index]`.
fn durable_entries(wal: &WalStorage) -> Vec<LogEntry> {
    let first = wal.first_index().expect("first_index");
    let last = wal.last_index().expect("last_index");
    if last < first {
        return Vec::new();
    }
    wal.entries(first, last + 1, None).expect("entries")
}

#[test]
fn single_node_kill_at_each_ready_step() {
    const MAX_STEPS: usize = 40;

    for k in 0..=MAX_STEPS {
        let dir = temp_dir();
        let factory = InMemoryTransportFactory::new();
        let mut node = build(&dir, &factory, 0);
        let mut sm = KvStateMachine::new();
        let mut committed: Vec<(RaftId, Vec<u8>)> = Vec::new();

        for round in 0..k {
            node.tick();
            let entries = block_on(node.step()).expect("step").committed;
            for (idx, data) in entries {
                sm.apply(idx, &data).expect("apply");
                committed.push((idx, data));
            }
            node.advance_apply();
            if node.leader_id() == 1 {
                let cmd = KvStateMachine::encode_put(
                    1,
                    round as u64 + 1,
                    b"k",
                    format!("v{round}").as_bytes(),
                );
                let _ = node.propose(&cmd);
            }
        }

        let commit_before = node.hard_state().commit;
        let applied_before = sm.applied_index();
        let sm_before = sm.snapshot().expect("snapshot");

        // Kill: dropping the node drops the `WalStorage` (releasing the lock).
        drop(node);

        // Restart step 1: reopen and inspect the durable log.
        let wal = WalStorage::open(&dir, wal_opts()).expect("reopen wal");
        let durable = durable_entries(&wal);
        // The durable commit index may be lower than what we observed in
        // memory (raft does not need it durable — it re-derives it). `applied`
        // must never exceed `committed`, so restart at the durable commit.
        let durable_commit = wal.initial_state().expect("state").hard_state.commit;
        assert!(
            durable.len() as u64 >= applied_before,
            "k={k}: durable log shorter than applied ({} < {applied_before})",
            durable.len()
        );
        for (i, entry) in durable.iter().take(applied_before as usize).enumerate() {
            assert_eq!(entry.index, committed[i].0, "k={k}: index mismatch at {i}");
            assert_eq!(entry.data, committed[i].1, "k={k}: data mismatch at {i}");
        }
        drop(wal);

        // Restart step 2: rebuild the node from the recovered WAL, replay the
        // applied prefix, and confirm it re-reaches the pre-kill commit.
        let factory2 = InMemoryTransportFactory::new();
        let mut node2 = build(&dir, &factory2, applied_before.min(durable_commit));
        let mut sm2 = KvStateMachine::new();
        for entry in durable.iter().take(applied_before as usize) {
            sm2.apply(entry.index, &entry.data).expect("apply recovered");
        }
        assert_eq!(
            sm2.snapshot().expect("snapshot"),
            sm_before,
            "k={k}: recovered state machine must match the pre-kill one"
        );

        for _ in 0..200 {
            node2.tick();
            let _ = block_on(node2.step()).expect("step");
            node2.advance_apply();
            if node2.hard_state().commit >= commit_before {
                break;
            }
        }
        assert!(
            node2.hard_state().commit >= commit_before,
            "k={k}: recovered node did not re-reach commit {commit_before} \
             (got {})",
            node2.hard_state().commit
        );

        drop(node2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
