//! Integration: the `wal_trailing_keep` window (propsol §7 Q6, rev M).
//!
//! The leader snapshots and compacts continuously, but it must not compact so
//! far that a follower trailing by less than the window has to be caught up
//! with a snapshot transfer. This drives exactly that shape on a real 3-node
//! runtime:
//!
//! 1. three nodes elect a leader and commit enough writes to cross the snapshot
//!    threshold several times, so the leader snapshots and compacts;
//! 2. the leader's log directory must show that compaction really pruned (its
//!    oldest segment is gone), i.e. the window did not simply disable it;
//! 3. a follower is killed and the leader writes **less than the window** more;
//! 4. the follower restarts and catches up **from the log**: its `next_idx` is
//!    still above the watermark, so no snapshot is installed on it and its data
//!    directory never grows a snapshot file.
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`, so
//! this file times with `core::time::Duration` only.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::client::Handle;
use arachne::consensus::RaftNodeConfig;
use arachne::runtime::{Runtime, RuntimeConfig, RuntimeThread};
use arachne::storage::{parse_segment_name, FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::TransportFactory;
use arachne::{ArachneError, Metrics, NodeId, Profile, ProfileConfig};
use arachne_testsupport::InMemoryTransportFactory;
use slog::{o, Drain, Logger};

const N: u64 = 3;
/// Small segments so compaction actually has something to delete.
const SEGMENT_BYTES: u64 = 4 * 1024;
/// The snapshot trigger, crossed repeatedly by the writes below.
const SNAPSHOT_THRESHOLD_BYTES: u64 = 4 * 1024;
/// The trailing window: comfortably larger than one segment, so compaction
/// prunes the oldest segment but must stop before the reachable log falls
/// below this.
const TRAILING_KEEP_BYTES: u64 = 8 * 1024;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-keep-{tag}-{}-{n}", std::process::id()));
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
                SocketAddr::from(([127, 0, 0, 1], 7300 + i as u16)),
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
        cluster_id: "trailing-keep".into(),
        node_id: format!("n{i}"),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: SEGMENT_BYTES,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

/// Segment files present in a node's data directory, by first log index.
fn segment_indices(dir: &Path) -> Vec<u64> {
    let mut found: Vec<u64> = std::fs::read_dir(dir)
        .expect("read data dir")
        .flatten()
        .filter_map(|e| parse_segment_name(&e.file_name().to_string_lossy()).ok())
        .collect();
    found.sort_unstable();
    found
}

/// Snapshot indexes present in a node's data directory.
fn snapshot_files(dir: &Path) -> Vec<u64> {
    let mut found: Vec<u64> = std::fs::read_dir(dir)
        .expect("read data dir")
        .flatten()
        .filter_map(|e| {
            arachne::storage::snapshot::parse_snapshot_file_name(&e.file_name().to_string_lossy())
        })
        .map(|(index, _term)| index)
        .collect();
    found.sort_unstable();
    found
}

struct Node {
    handle: Handle,
    metrics: Arc<Metrics>,
    dir: PathBuf,
    runtime: Option<RuntimeThread>,
}

async fn open_wal(dir: &Path, i: u64) -> WalStorage {
    let mut last = String::new();
    for _ in 0..200 {
        match WalStorage::open(dir, wal_opts(i)) {
            Ok(mut wal) => {
                // Q6 window, as the node binary configures it.
                wal.set_trailing_keep_bytes(TRAILING_KEEP_BYTES);
                return wal;
            }
            Err(e) => {
                last = e.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(10)).await;
            }
        }
    }
    panic!("node {i} could not open its WAL: {last}");
}

async fn spawn_node(
    i: u64,
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
        peers: peers_of(i, N),
        addresses: addresses.clone(),
        raft: RaftNodeConfig::from_profile(profile),
        profile: profile.clone(),
        metrics: Arc::clone(&metrics),
    };
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("build runtime");
    let thread = runtime.spawn_dedicated().expect("spawn consensus thread");
    Node {
        handle,
        metrics,
        dir,
        runtime: Some(thread),
    }
}

async fn until_put(handle: &Handle, key: &[u8], value: &[u8]) -> Result<(), ArachneError> {
    for _ in 0..800 {
        match handle.put(key, value).await {
            Ok(()) => return Ok(()),
            Err(ArachneError::Timeout)
            | Err(ArachneError::QuorumUnavailable)
            | Err(ArachneError::NotLeader { .. })
            | Err(ArachneError::Busy) => {
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
            _ => tokio::time::sleep(core::time::Duration::from_millis(20)).await,
        }
    }
    None
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i}").into_bytes()
}

fn value(i: u64) -> Vec<u8> {
    format!("v{i}").into_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_inside_the_trailing_window_catches_up_from_the_log() {
    let profile = test_profile();
    let factory = InMemoryTransportFactory::new();
    let addresses = addresses(N);

    let mut nodes: Vec<Node> = Vec::new();
    for i in 1..=N {
        let dir = temp_dir(&format!("n{i}"));
        nodes.push(spawn_node(i, dir, &factory, &addresses, &profile).await);
    }
    for i in 0..N as usize {
        for j in 0..N as usize {
            if i != j {
                nodes[i].handle.register_peer(nodes[j].handle.clone());
            }
        }
    }

    // 1. A leader, agreed on by everyone, and some shared state.
    let mut leader = 0usize;
    for _ in 0..800 {
        if let Some(pos) = nodes.iter().position(|n| n.metrics.is_leader())
            && nodes.iter().all(|n| n.metrics.leader_id() != 0)
        {
            leader = pos;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    let leader_id = (leader + 1) as u64;
    let leader_handle = nodes[leader].handle.clone();
    for i in 0..20u64 {
        until_put(&leader_handle, &key(i), &value(i))
            .await
            .expect("the seed write commits");
    }

    // 2. Write past the snapshot threshold several times, so the leader
    //    snapshots and compacts while every follower is still caught up.
    for i in 20..320u64 {
        until_put(&leader_handle, &key(i), &value(i))
            .await
            .expect("the majority keeps committing");
    }

    // 3. The leader really did compact — otherwise this test would pass by
    //    having simply not pruned anything.
    let leader_segments = segment_indices(&nodes[leader].dir);
    assert!(
        *leader_segments.first().expect("segments") > 1,
        "compaction must have pruned the oldest segment, found {leader_segments:?}"
    );
    assert!(
        !snapshot_files(&nodes[leader].dir).is_empty(),
        "the leader must have taken snapshots while writing"
    );

    // 4. A follower now drops out, and the leader writes **less than the
    //    window** more: the follower's `next_idx` stays above the watermark.
    let victim = (1..=N)
        .position(|i| i != leader_id)
        .expect("a follower exists");
    let victim_id = (victim + 1) as u64;
    // Stop the actor for real: this releases its WAL (and the data-dir lock)
    // without the transport having to close.
    nodes[victim]
        .runtime
        .take()
        .expect("the victim is running")
        .shutdown();
    tokio::time::sleep(core::time::Duration::from_millis(150)).await;
    for i in 320..380u64 {
        until_put(&leader_handle, &key(i), &value(i))
            .await
            .expect("the majority keeps committing");
    }

    // 5. The follower restarts and must be served from the log: no snapshot
    //    transfer, so nothing is ever installed on it.
    let victim_dir = nodes[victim].dir.clone();
    nodes[victim] = spawn_node(victim_id, victim_dir, &factory, &addresses, &profile).await;
    for j in 0..N as usize {
        if j != victim {
            nodes[victim].handle.register_peer(nodes[j].handle.clone());
            nodes[j].handle.register_peer(nodes[victim].handle.clone());
        }
    }

    let newest = key(379);
    assert_eq!(
        until_stale(&nodes[victim].handle, &newest).await,
        Some(value(379)),
        "the follower must catch up on the newest write"
    );
    let early = nodes[victim]
        .handle
        .get_stale(&key(0))
        .await
        .expect("stale read")
        .expect("the early write must still be there");
    assert_eq!(early, value(0));

    assert_eq!(
        nodes[victim].metrics.snapshots_installed_total(),
        0,
        "a follower inside the trailing window must be caught up from the log"
    );
    // (The victim's directory may well hold snapshots of its own: every node
    // triggers its own local snapshots, and a restart *loads* one rather than
    // installing it. `snapshots_installed_total` is the signal that distinguishes
    // a transfer from a local snapshot.)

    let dirs: Vec<PathBuf> = nodes.iter().map(|n| n.dir.clone()).collect();
    for node in nodes.iter_mut() {
        if let Some(runtime) = node.runtime.take() {
            runtime.shutdown();
        }
    }
    for dir in dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}
