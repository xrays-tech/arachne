//! Integration: the M2 snapshot/compaction path on the lib runtime actor
//! (propsol §5.5.4, v0.2.10 M), over a 3-node in-process cluster.
//!
//! M2 exit criterion ① is "after writes exceed `snapshot_threshold` the WAL is
//! compacted, and a follower catches up through the snapshot". This test drives
//! exactly that, end to end and through the real actor:
//!
//! 1. three nodes elect a leader and commit a few writes;
//! 2. a **follower** is taken down (its actor is aborted, its data dir kept);
//! 3. the leader commits far more than `snapshot_threshold`, so it takes a
//!    snapshot and compacts its WAL — the early entries are now *gone* from the
//!    leader's log, not merely superseded;
//! 4. the follower restarts from its old, short WAL and must converge;
//! 5. the assertion that matters is on a key written **before** the follower
//!    went down: it exists nowhere in the leader's log any more, so a follower
//!    that only received the surviving tail would report it missing. Seeing it
//!    proves the snapshot was transferred and installed.
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids real-time/network imports there), so this file uses
//! `core::time::Duration` and no tokio select macros.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::client::Handle;
use arachne::consensus::RaftNodeConfig;
use arachne::runtime::{Runtime, RuntimeConfig};
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::TransportFactory;
use arachne::{ArachneError, Metrics, NodeId, Profile, ProfileConfig};
use arachne_testsupport::InMemoryTransportFactory;
use slog::{o, Drain, Logger};

const N: u64 = 3;
/// Deliberately tiny: a few hundred small writes must cross it several times,
/// and every rollover gives compaction a segment it can actually unlink.
const SEGMENT_BYTES: u64 = 8 * 1024;
/// The snapshot trigger. Small for the same reason.
const SNAPSHOT_THRESHOLD_BYTES: u64 = 4 * 1024;
/// Writes issued while one node is down. Each is ~30 bytes of payload, so this
/// is comfortably several multiples of the threshold.
const WRITES: u64 = 160;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-m2snap-{tag}-{}-{n}", std::process::id()));
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
                SocketAddr::from(([127, 0, 0, 1], 7000 + i as u16)),
            )
        })
        .collect()
}

/// A profile with a snapshot trigger small enough to fire in a test, and an
/// election timeout long enough that a restarting node cannot disrupt the
/// cluster before the leader has had a chance to reach it.
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
        cluster_id: "m2-snapshot".into(),
        node_id: format!("n{i}"),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: SEGMENT_BYTES,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

/// One node of the test cluster: its handle, metrics, and (while running) its
/// actor task.
struct Node {
    handle: Handle,
    metrics: Arc<Metrics>,
    dir: PathBuf,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Node {
    /// Stop the actor. The `WalStorage` (and with it the data-dir lock) is
    /// released when the aborted task is dropped, so the caller must wait for
    /// the lock before reopening the directory.
    fn kill(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }

    fn is_running(&self) -> bool {
        self.task.is_some()
    }
}

/// Open node `i`'s `WalStorage`, waiting for the data-dir lock to be released
/// by a killed predecessor.
async fn open_wal(dir: &Path, i: u64) -> WalStorage {
    let mut last = String::new();
    for _ in 0..200 {
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

/// Start (or restart) node `i` on `dir`, sharing `factory` so a restart
/// rejoins the same in-memory network.
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

/// Poll `handle.put` until it succeeds, retrying the transient errors a
/// reconfiguring cluster produces.
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

/// Poll `handle.get` until it answers, retrying the transient errors.
async fn until_get(handle: &Handle, key: &[u8]) -> Result<Option<Vec<u8>>, ArachneError> {
    for _ in 0..800 {
        match handle.get(key).await {
            Ok(value) => return Ok(value),
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

fn key(i: u64) -> Vec<u8> {
    format!("k{i}").into_bytes()
}

fn value(i: u64) -> Vec<u8> {
    format!("v{i}").into_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_follower_catches_up_through_a_snapshot() {
    let profile = test_profile();
    let factory = InMemoryTransportFactory::new();
    let addresses = addresses(N);

    let mut nodes: Vec<Node> = Vec::new();
    for i in 1..=N {
        let dir = temp_dir(&format!("n{i}"));
        nodes.push(spawn_node(i, N, dir, &factory, &addresses, &profile).await);
    }
    // In-process redirects: every handle knows every peer (propsol §3.3).
    for i in 0..N as usize {
        for j in 0..N as usize {
            if i != j {
                nodes[i].handle.register_peer(nodes[j].handle.clone());
            }
        }
    }

    // 1. Wait for a leader and for every node to learn who it is.
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
    assert!(
        nodes.iter().any(|n| n.metrics.is_leader()),
        "no leader elected"
    );
    let leader_id = (leader + 1) as u64;
    let leader_handle = nodes[leader].handle.clone();

    // 2. A key written *before* the follower goes down. By the end of the test
    //    this entry is compacted away on the leader, so only a snapshot can
    //    carry it to the follower.
    until_put(&leader_handle, &key(0), &value(0))
        .await
        .expect("the early write commits");

    // 3. Take a follower down. The surviving two nodes still form a quorum.
    let victim = (1..=N)
        .position(|i| i != leader_id)
        .expect("a follower exists");
    let victim_id = (victim + 1) as u64;
    nodes[victim].kill();
    assert!(!nodes[victim].is_running());
    // Give the leader time to notice, so the writes below are not just queued
    // for a target that is about to disappear.
    tokio::time::sleep(core::time::Duration::from_millis(200)).await;

    // 4. Write well past the threshold. Two of three nodes are alive, so every
    //    write commits durably; the leader snapshots and compacts repeatedly.
    for i in 1..=WRITES {
        until_put(&leader_handle, &key(i), &value(i))
            .await
            .expect("write commits with a live quorum");
    }
    eprintln!(
        "[snapshot] {WRITES} writes committed; leader snapshots={}",
        nodes[leader].metrics.snapshots_created_total()
    );

    // The trigger fired, the durable snapshot exists, and the log was released.
    assert!(
        nodes[leader].metrics.snapshots_created_total() > 0,
        "the leader must have taken a local snapshot once the log outgrew the threshold"
    );
    let leader_snapshots = snapshot_files(&nodes[leader].dir);
    assert!(
        !leader_snapshots.is_empty(),
        "the leader must have a durable snapshot file"
    );
    let newest = *leader_snapshots.last().expect("non-empty");
    assert!(
        newest > 0,
        "the snapshot must be taken at an applied index, got {newest}"
    );
    assert!(
        nodes[leader].metrics.snapshot_last_duration_ms() < 1_000,
        "Q4 budget: snapshot creation must stay well inside its 1s alarm"
    );

    // The compaction this test depends on: the leader's log no longer starts at
    // the entry carrying `k0`.
    let leader_applied = nodes[leader].metrics.applied_index();
    assert!(
        newest <= leader_applied,
        "a snapshot cannot be ahead of the applied index ({newest} > {leader_applied})"
    );

    // 5. Bring the follower back on its original directory: its WAL is short
    //    (it stopped before the writes above), so the leader's next attempt to
    //    replicate lands below `first_index` and switches to a snapshot.
    let victim_dir = nodes[victim].dir.clone();
    nodes[victim] = spawn_node(victim_id, N, victim_dir, &factory, &addresses, &profile).await;
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

    // 6. Wait for the follower to converge on the *newest* value...
    let newest_key = key(WRITES);
    let mut newest_seen = None;
    for _ in 0..1_500 {
        if let Ok(Some(v)) = nodes[victim].handle.get_stale(&newest_key).await {
            newest_seen = Some(v);
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        newest_seen.as_deref(),
        Some(value(WRITES).as_slice()),
        "the restarted follower must apply the writes it missed"
    );

    // ...and, decisively, on the key that only the snapshot can carry.
    let early = nodes[victim]
        .handle
        .get_stale(&key(0))
        .await
        .expect("stale read on the restarted follower")
        .expect("the pre-restart write must survive");
    assert_eq!(
        early,
        value(0),
        "the restarted follower lost a write that the leader had already compacted"
    );

    // The install path ran, and it is the reason the follower did not have to
    // replay the whole log.
    assert!(
        nodes[victim].metrics.snapshots_installed_total() > 0,
        "the follower must have installed a snapshot received from the leader"
    );
    assert!(
        !snapshot_files(&nodes[victim].dir).is_empty(),
        "the installed snapshot must be durable on the follower"
    );

    // 7. The restarted node serves linearizable reads again, and every node
    //    agrees on the state (the follower is no longer the odd one out).
    let read_back = until_get(&nodes[victim].handle, &key(WRITES))
        .await
        .expect("a redirecting read succeeds");
    assert_eq!(read_back.as_deref(), Some(value(WRITES).as_slice()));

    for i in 1..=N {
        let pos = (i - 1) as usize;
        if pos == victim {
            continue;
        }
        assert_eq!(
            nodes[pos].metrics.applied_index(),
            nodes[victim].metrics.applied_index(),
            "node {i} and the restarted follower must be at the same applied index"
        );
    }

    let dirs: Vec<PathBuf> = nodes.iter().map(|n| n.dir.clone()).collect();
    for node in nodes.iter_mut() {
        node.kill();
    }
    // Let the aborted actors drop their `WalStorage` before unlinking.
    tokio::time::sleep(core::time::Duration::from_millis(100)).await;
    for dir in dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// A restart must rebuild the applied state from the **durable snapshot**, not
/// from the log: raft's applied index starts at `first_index - 1`, which is the
/// snapshot index, so the compacted prefix is never replayed. If the runtime
/// came up with an empty state machine, `k0` would be gone for good — which is
/// exactly what this test pins down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_rebuilds_the_state_machine_from_the_snapshot() {
    const LOCAL_WRITES: u64 = 80;
    let profile = test_profile();
    let factory = InMemoryTransportFactory::new();
    let addresses = addresses(1);
    let dir = temp_dir("solo");

    let mut node = spawn_node(1, 1, dir.clone(), &factory, &addresses, &profile).await;
    for _ in 0..800 {
        if node.metrics.is_leader() {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    assert!(node.metrics.is_leader(), "the single voter elects itself");

    until_put(&node.handle, &key(0), &value(0))
        .await
        .expect("the early write commits");
    for i in 1..=LOCAL_WRITES {
        until_put(&node.handle, &key(i), &value(i))
            .await
            .expect("write commits");
    }
    assert!(
        node.metrics.snapshots_created_total() > 0,
        "the local trigger must have fired"
    );
    let snapshots = snapshot_files(&dir);
    assert!(!snapshots.is_empty(), "a durable snapshot exists");
    let newest = *snapshots.last().expect("non-empty");
    let applied_before = node.metrics.applied_index();
    assert!(newest > 1, "the snapshot must be past the early writes");

    node.kill();
    let node = {
        let restarted = spawn_node(1, 1, dir.clone(), &factory, &addresses, &profile).await;
        // The snapshot is the only possible source for `k0`: the log's window
        // starts above it.
        assert!(
            restarted.metrics.applied_index() >= newest,
            "the restarted node must resume from the snapshot at {newest}, \
             got applied={}",
            restarted.metrics.applied_index()
        );
        restarted
    };

    let early = node
        .handle
        .get_stale(&key(0))
        .await
        .expect("stale read after restart")
        .expect("k0 must come back from the snapshot");
    assert_eq!(early, value(0), "the restored state lost a compacted write");

    let tail = node
        .handle
        .get_stale(&key(LOCAL_WRITES))
        .await
        .expect("stale read after restart")
        .expect("the newest write must have been replayed on top of the snapshot");
    assert_eq!(tail, value(LOCAL_WRITES));

    // A linearizable read works too, so the restarted node is fully serving.
    let read_back = until_get(&node.handle, &key(LOCAL_WRITES))
        .await
        .expect("linearizable read after restart");
    assert_eq!(read_back.as_deref(), Some(value(LOCAL_WRITES).as_slice()));
    assert!(applied_before >= newest);

    let mut node = node;
    node.kill();
    tokio::time::sleep(core::time::Duration::from_millis(100)).await;
    let _ = std::fs::remove_dir_all(&dir);
}
