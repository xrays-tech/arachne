//! Integration: restart safety when a durable snapshot is ahead of the raft
//! hard state (`dev-docs/arachne-kv-0.7.0-snapshot-hardstate-panic.md`).
//!
//! 0.7.0 could persist a snapshot at index N and then restart with
//! `hard_state.commit = N - 1`. Raft derives its committed index from the
//! snapshot (`raft_log.committed = N`) and its `load_state` rejects
//! `hs.commit < N` with a fatal panic. `WalStorage::open` now repairs that
//! crash window (the snapshot is authoritative), and `save_snapshot` keeps the
//! pair consistent for future snapshots.
//!
//! This test builds the exact crash window on a 3-node cluster: force a
//! snapshot at N on a node, kill it, reopen its WAL manually and rewind the
//! persisted hard state to N-1, then restart it. It must start without a panic
//! and converge.
//!
//! NOTE (entropy gates): `tests/` forbids real-time/network imports, so this
//! file uses `core::time::Duration` and no tokio select macros.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_kv::client::Handle;
use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::TransportFactory;
use arachne_kv::{
    ArachneError, HardState, Metrics, NodeId, Profile, ProfileConfig, Storage,
};
use arachne_kv_testsupport::InMemoryTransportFactory;
use slog::{o, Drain, Logger};

const N: u64 = 3;
const SEGMENT_BYTES: u64 = 8 * 1024;
const SNAPSHOT_THRESHOLD_BYTES: u64 = 4 * 1024;
const WRITES: u64 = 200;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-hardstate-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn node_id(i: u64) -> NodeId {
    NodeId::from(format!("n{i}"))
}

fn peers_of(self_id: u64, n: u64) -> HashMap<u64, NodeId> {
    (1..=n)
        .filter(|j| *j != self_id)
        .map(|j| (j, node_id(j)))
        .collect()
}

fn addresses(n: u64) -> HashMap<NodeId, SocketAddr> {
    (1..=n)
        .map(|i| {
            (
                node_id(i),
                SocketAddr::from(([127, 0, 0, 1], 7200 + i as u16)),
            )
        })
        .collect()
}

fn test_profile() -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 600,
        rpc_timeout_ms: 300,
        snapshot_threshold_bytes: SNAPSHOT_THRESHOLD_BYTES,
        ..Profile::Lan.config()
    }
}

fn wal_opts(i: u64) -> WalOptions {
    WalOptions {
        cluster_id: "snapshot-hardstate".into(),
        node_id: format!("n{i}"),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: SEGMENT_BYTES,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

struct Node {
    handle: Handle,
    metrics: Arc<Metrics>,
    dir: PathBuf,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Node {
    fn kill(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Open node `i`'s WAL, waiting for the killed predecessor to release the
/// data-dir lock.
async fn open_wal(dir: &Path, i: u64) -> WalStorage {
    let mut last = String::new();
    for _ in 0..300 {
        match WalStorage::open(dir, wal_opts(i)) {
            Ok(wal) => return wal,
            Err(e) => {
                last = e.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(10)).await;
            }
        }
    }
    panic!("node {i} could not reopen its WAL: {last}");
}

async fn spawn_node(
    i: u64,
    n: u64,
    dir: PathBuf,
    factory: &InMemoryTransportFactory,
    addresses: &HashMap<NodeId, SocketAddr>,
    profile: &ProfileConfig,
) -> Node {
    let wal = open_wal(&dir, i).await;
    let (tx, rx) = factory.create(node_id(i));
    let metrics = Arc::new(Metrics::new());
    let config = RuntimeConfig {
        self_raft_id: i,
        self_node_id: node_id(i),
        peers: peers_of(i, n),
        addresses: addresses.clone(),
        raft: RaftNodeConfig::from_profile(profile),
        profile: profile.clone(),
        metrics: Arc::clone(&metrics),
    };
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("build runtime");
    let task = tokio::spawn(runtime.run());
    Node {
        handle,
        metrics,
        dir,
        task: Some(task),
    }
}

async fn until_put(handle: &Handle, key: &[u8], value: &[u8]) -> Result<(), ArachneError> {
    for _ in 0..800 {
        match handle.put(key, value).await {
            Ok(()) => return Ok(()),
            Err(ArachneError::Timeout)
            | Err(ArachneError::QuorumUnavailable)
            | Err(ArachneError::NotLeader { .. }) => {
                tokio::time::sleep(core::time::Duration::from_millis(2)).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(ArachneError::Timeout)
}

async fn until_stale(handle: &Handle, key: &[u8]) -> Option<Vec<u8>> {
    for _ in 0..800 {
        match handle.get_stale(key).await {
            Ok(Some(value)) => return Some(value),
            Ok(None) | Err(_) => {
                tokio::time::sleep(core::time::Duration::from_millis(20)).await;
            }
        }
    }
    None
}

fn snapshot_files(dir: &Path) -> Vec<u64> {
    let mut found: Vec<u64> = std::fs::read_dir(dir)
        .expect("read data dir")
        .flatten()
        .filter_map(|e| {
            arachne_kv::storage::snapshot::parse_snapshot_file_name(
                &e.file_name().to_string_lossy(),
            )
        })
        .map(|(index, _term)| index)
        .collect();
    found.sort_unstable();
    found
}

async fn wait_for_leader(nodes: &[Node]) -> usize {
    for _ in 0..800 {
        if let Some(pos) = nodes.iter().position(|n| n.metrics.is_leader()) {
            return pos;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    panic!("no leader elected within the wait budget");
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i}").into_bytes()
}

fn value(i: u64) -> Vec<u8> {
    format!("v{i}").into_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_after_snapshot_with_lagging_hard_state_converges() {
    let profile = test_profile();
    let factory = InMemoryTransportFactory::new();
    let addresses = addresses(N);

    let mut nodes: Vec<Node> = Vec::new();
    for i in 1..=N {
        let dir = temp_dir(&format!("n{i}"));
        nodes.push(spawn_node(i, N, dir, &factory, &addresses, &profile).await);
    }
    for i in 0..N as usize {
        for j in 0..N as usize {
            if i != j {
                nodes[i].handle.register_peer(nodes[j].handle.clone());
            }
        }
    }

    let leader = wait_for_leader(&nodes).await;
    let leader_id = (leader + 1) as u64;
    let leader_handle = nodes[leader].handle.clone();

    // A key covered by the snapshot; a restarted node must see it immediately
    // after restoring.
    until_put(&leader_handle, &key(0), &value(0))
        .await
        .expect("the early write commits");

    // Drive the leader past the snapshot threshold so it writes a durable
    // snapshot at N and compacts the entries below it.
    for i in 1..=WRITES {
        until_put(&leader_handle, &key(i), &value(i))
            .await
            .expect("write commits with a live quorum");
    }
    assert!(
        nodes[leader].metrics.snapshots_created_total() > 0,
        "the leader must have created a snapshot"
    );
    assert!(
        !snapshot_files(&nodes[leader].dir).is_empty(),
        "the snapshot must be durable on disk"
    );

    // Kill the node that owns the snapshot, then rebuild the exact 0.7.0 crash
    // window by hand: snapshot at N, persisted hard-state commit N-1.
    let victim = leader;
    let victim_id = leader_id;
    nodes[victim].kill();
    tokio::time::sleep(core::time::Duration::from_millis(200)).await;

    let snapshot_index = {
        let mut wal = open_wal(&nodes[victim].dir, victim_id).await;
        let snapshot = wal
            .snapshot()
            .expect("read snapshot")
            .expect("the killed node must have a durable snapshot");
        let n = snapshot.meta.index;
        assert!(n > 0, "snapshot index must be a real applied index");

        let state = wal.initial_state().expect("initial state");
        assert!(
            state.hard_state.commit >= n,
            "save_snapshot must already have raised commit to the snapshot index"
        );
        // Rewind only the persisted commit, keeping term/vote: this is the
        // crash window between the snapshot file and the hard-state write.
        wal.set_hard_state(&HardState {
            term: state.hard_state.term,
            vote: state.hard_state.vote,
            commit: n - 1,
        })
        .expect("simulate the crash window");
        n
    };

    // Restart: `open` repairs commit to the snapshot index before raft builds
    // its log. The pre-fix code panics here (`hs.commit < raft_log.committed`).
    nodes[victim] = spawn_node(
        victim_id,
        N,
        nodes[victim].dir.clone(),
        &factory,
        &addresses,
        &profile,
    )
    .await;
    for j in 0..N as usize {
        if j != victim {
            nodes[victim]
                .handle
                .register_peer(nodes[j].handle.clone());
            nodes[j]
                .handle
                .register_peer(nodes[victim].handle.clone());
        }
    }

    // The restarted node restored the snapshot: the pre-snapshot key is there.
    assert_eq!(
        until_stale(&nodes[victim].handle, &key(0)).await.as_deref(),
        Some(value(0).as_slice()),
        "the restarted node must serve the key covered by the snapshot"
    );

    // It also rejoins and converges: a write committed after the restart is
    // visible on it.
    let writer = (victim + 1) % N as usize;
    let post_key = b"after-restart";
    until_put(&nodes[writer].handle, post_key, b"ok")
        .await
        .expect("the surviving cluster still commits writes");
    assert_eq!(
        until_stale(&nodes[victim].handle, post_key)
            .await
            .as_deref(),
        Some(b"ok".as_slice()),
        "the restarted node must catch up with a post-restart write"
    );
    assert!(
        nodes[victim].metrics.applied_index() >= snapshot_index,
        "the restarted node applied at least the snapshot"
    );

    let dirs: Vec<PathBuf> = nodes.iter().map(|n| n.dir.clone()).collect();
    for node in nodes.iter_mut() {
        node.kill();
    }
    tokio::time::sleep(core::time::Duration::from_millis(100)).await;
    for dir in dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}
