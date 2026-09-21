//! M0 acceptance ① (INV2) — three-node L1 cluster over the in-memory transport.
//!
//! Elects a leader, commits a write, then kills the leader (drops its
//! `RaftNode`, releasing the WAL dir lock), reopens its WAL and asserts the
//! durable log still covers the leader's applied prefix byte-for-byte. The node
//! is then rebuilt from the recovered WAL and the cluster must re-reach the
//! pre-kill commit index (recovery/liveness).
//!
//! Deterministic, in-process, no tokio. `RaftNode::step` treats undeliverable
//! messages as non-fatal (raft retransmits), so a dead peer cannot stall the
//! survivors.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use arachne::consensus::RaftNode;
use arachne::state_machine::KvStateMachine;
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{
    LogEntry, NodeId, RaftId, StateMachine, Storage, TransportFactory, TransportMessage,
};
use arachne_testsupport::{block_on, InMemoryRx, InMemoryTransportFactory, InMemoryTx};
use slog::{o, Drain, Logger};

type TestNode = RaftNode<WalStorage, InMemoryTx, InMemoryRx>;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-m0-3n-{tag}-{}-{n}",
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
        cluster_id: "m0".into(),
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

fn durable_log(wal: &WalStorage) -> (Vec<LogEntry>, u64) {
    let first = wal.first_index().expect("first_index");
    let last = wal.last_index().expect("last_index");
    let commit = wal.initial_state().expect("state").hard_state.commit;
    let entries = if last < first {
        Vec::new()
    } else {
        wal.entries(first, last + 1, None).expect("entries")
    };
    (entries, commit)
}

struct Cluster {
    factory: InMemoryTransportFactory,
    dirs: Vec<PathBuf>,
    nodes: Vec<Option<TestNode>>,
    sms: Vec<KvStateMachine>,
    committed: Vec<Vec<(RaftId, Vec<u8>)>>,
}

impl Cluster {
    fn new(n: u64) -> Self {
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
            factory,
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
}

#[test]
fn three_node_cluster_survives_leader_kill() {
    let mut c = Cluster::new(3);

    // Elect a leader.
    for _ in 0..300 {
        c.round();
        if c.leader_index().is_some() {
            break;
        }
    }
    let li = c.leader_index().expect("cluster must elect a leader");

    // Commit a write through the leader.
    let cmd = KvStateMachine::encode_put(1, 1, b"k", b"v");
    c.nodes[li].as_mut().unwrap().propose(&cmd).expect("propose");
    for _ in 0..300 {
        c.round();
        if c.sms[li].get(b"k").expect("get") == Some(b"v".to_vec()) {
            break;
        }
    }
    assert_eq!(c.sms[li].get(b"k").expect("get"), Some(b"v".to_vec()));
    let commit_before = c.nodes[li].as_ref().unwrap().hard_state().commit;
    let applied_before = c.sms[li].applied_index();
    let prefix = c.committed[li].clone();
    assert!(applied_before >= 1);

    // Kill the leader: dropping the node drops its `WalStorage` (frees the lock).
    c.nodes[li] = None;

    // Reopen its WAL and assert the durable log covers the applied prefix.
    let node_name = format!("n{}", li as RaftId + 1);
    let (durable, durable_commit) = {
        let wal = WalStorage::open(&c.dirs[li], wal_opts(&node_name)).expect("reopen wal");
        durable_log(&wal)
    };
    assert!(
        durable.len() as u64 >= applied_before,
        "durable log shorter than applied ({} < {applied_before})",
        durable.len()
    );
    for (i, entry) in durable.iter().take(applied_before as usize).enumerate() {
        assert_eq!(entry.index, prefix[i].0, "index mismatch at {i}");
        assert_eq!(entry.data, prefix[i].1, "data mismatch at {i}");
    }

    // Rebuild the node from the recovered WAL (`applied` never exceeds commit).
    let (tx, rx) = c.factory.create(node_id(li as RaftId + 1));
    let wal = WalStorage::open(&c.dirs[li], wal_opts(&node_name)).expect("reopen wal");
    let rebuilt = RaftNode::new(
        li as RaftId + 1,
        peers_of(li as RaftId + 1, 3),
        wal,
        tx,
        rx,
        applied_before.min(durable_commit),
        &logger(),
    )
    .expect("rebuild node");
    c.nodes[li] = Some(rebuilt);

    // The cluster must make progress again and re-reach the pre-kill commit.
    let mut recovered = false;
    for _ in 0..400 {
        c.round();
        if let Some(node) = c.nodes[li].as_ref() {
            if node.hard_state().commit >= commit_before {
                recovered = true;
                break;
            }
        }
    }
    assert!(
        recovered,
        "recovered cluster did not re-reach commit {commit_before}"
    );

    // Cleanup.
    c.nodes.iter_mut().for_each(|n| *n = None);
    for dir in &c.dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}
