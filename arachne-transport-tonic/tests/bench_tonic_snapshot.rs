//! On-demand benchmark of the **real tonic** streamed-snapshot transfer (rev T).
//!
//! Same harness as `tests/tonic_streamed_snapshot.rs` (the acceptance test),
//! parameterised to time the transfer and run it at two pacing levels:
//!
//! * unlimited (`snapshot_rate_bps = 0`): what a local loopback transfer of a
//!   snapshot larger than `max_message_size` actually costs;
//! * rate-limited (`snapshot_rate_bps = 256 KiB/s`): the token bucket pacing
//!   on the same snapshot, so the transfer duration tracks bytes / rate.
//!
//! This is **not** a CI test (both tests are `#[ignore]`). Run it with:
//!
//! ```text
//! CARGO_TARGET_DIR=.dsh-target cargo test --release -p arachne-transport-tonic \
//!     --test bench_tonic_snapshot -- --ignored --nocapture
//! ```
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids the std clock there), so this file times with
//! `tokio::time::Instant` and uses no select macros.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::client::Handle;
use arachne::consensus::RaftNodeConfig;
use arachne::runtime::{Runtime, RuntimeConfig};
use arachne::storage::snapshot::snapshot_file_name;
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{ArachneError, Metrics, NodeId, Profile, ProfileConfig};
use arachne_seam::seam::TransportFactory;
use arachne_transport_tonic::snapshot::{SnapshotProvider, SnapshotReader};
use arachne_transport_tonic::TonicTransportFactory;
use slog::{o, Drain, Logger};
use tokio::time::Instant;

const CAP: usize = 64 * 1024;
const VALUE_BYTES: usize = 1024;
const WRITES: u64 = 384;
/// The paced run's token-bucket rate.
const RATE_BPS: u64 = 256 * 1024;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-tonicbench-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn nid(i: u64) -> NodeId {
    NodeId::from(format!("n{i}"))
}

fn profile() -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 600,
        rpc_timeout_ms: 300,
        snapshot_threshold_bytes: 4 * 1024,
        // No trailing window: the leader must not be able to serve the missed
        // log, so a snapshot is genuinely required.
        wal_trailing_keep_bytes: 0,
        ..Profile::Lan.config()
    }
}

fn wal_options(i: u64) -> WalOptions {
    WalOptions {
        cluster_id: "tonic-bench".into(),
        node_id: format!("n{i}"),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 8 * 1024,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i}").into_bytes()
}

fn value(i: u64) -> Vec<u8> {
    let mut v = format!("v{i}").into_bytes();
    v.resize(VALUE_BYTES, b'x');
    v
}

/// Serves this node's snapshots from its own data directory (as in the
/// acceptance test).
struct DirProvider {
    dir: PathBuf,
    served: Arc<AtomicU64>,
}

impl SnapshotProvider for DirProvider {
    fn open(&self, index: u64, term: u64) -> Option<SnapshotReader> {
        let path = self.dir.join(snapshot_file_name(index, term));
        let file = std::fs::File::open(&path).ok()?;
        let len = file.metadata().ok()?.len();
        self.served.fetch_add(1, Ordering::Relaxed);
        Some(SnapshotReader {
            len,
            reader: Box::new(file),
        })
    }
}

struct Node {
    handle: Handle,
    metrics: Arc<Metrics>,
    factory: TonicTransportFactory,
    task: Option<tokio::task::JoinHandle<()>>,
}

#[allow(clippy::too_many_arguments)]
async fn start_node(
    i: u64,
    dir: &Path,
    addr: SocketAddr,
    all: &HashMap<NodeId, SocketAddr>,
    learner: bool,
    served: Arc<AtomicU64>,
    rate_bps: u64,
) -> Node {
    let factory = TonicTransportFactory::new("tonic-bench", 1, 0, Vec::new(), all.clone());
    factory.max_message_size(CAP);
    factory.snapshot_rate_bps(rate_bps);
    factory.snapshot_provider(Arc::new(DirProvider {
        dir: dir.to_path_buf(),
        served,
    }));
    factory
        .start_with_bind(nid(i), addr)
        .await
        .expect("bind this node");
    let (tx, rx) = factory.create(nid(i));

    let wal = WalStorage::open(dir, wal_options(i)).expect("open wal");
    let metrics = Arc::new(Metrics::new());
    let peers = (1..=2u64).filter(|j| *j != i).map(|j| (j, nid(j))).collect();
    let mut raft = RaftNodeConfig::from_profile(&profile());
    raft.bootstrap_voters = Some(vec![1]);
    raft.join_as_learner = learner;
    let config = RuntimeConfig {
        self_raft_id: i,
        self_node_id: nid(i),
        peers,
        addresses: all.clone(),
        raft,
        profile: profile(),
        metrics: Arc::clone(&metrics),
    };
    let (runtime, handle) = match Runtime::new(config, wal, tx, rx, &logger()) {
        Ok(pair) => pair,
        Err(e) => panic!("node {i} could not start: {e}"),
    };
    Node {
        handle,
        metrics,
        factory,
        task: Some(tokio::spawn(runtime.run())),
    }
}

fn link(a: &Handle, b: &Handle) {
    a.register_peer(b.clone());
    b.register_peer(a.clone());
}

async fn until_put(handle: &Handle, k: &[u8], v: &[u8]) {
    for _ in 0..800 {
        match handle.put(k, v).await {
            Ok(()) => return,
            Err(ArachneError::Timeout)
            | Err(ArachneError::QuorumUnavailable)
            | Err(ArachneError::NotLeader { .. }) => {
                tokio::time::sleep(core::time::Duration::from_millis(5)).await;
            }
            Err(e) => panic!("write failed: {e}"),
        }
    }
    panic!("the write never went through");
}

fn biggest_snapshot(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .expect("read data dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("snapshot-") && name.ends_with(".snap")
        })
        .filter_map(|e| e.metadata().ok().map(|m| m.len()))
        .max()
        .unwrap_or(0)
}

/// One timed run: node 1 (sole voter) writes `WRITES` 1 KiB values past a 4 KiB
/// snapshot threshold with `wal_trailing_keep_bytes = 0`, then a late learner
/// (node 2) is started and timed until it catches up through the streamed
/// snapshot. Returns (snapshot bytes, catch-up seconds).
async fn run_case(tag: &str, rate_bps: u64) -> (u64, f64) {
    let served = Arc::new(AtomicU64::new(0));
    let a1 = std::net::TcpListener::bind("127.0.0.1:0").expect("probe");
    let addr1 = a1.local_addr().expect("addr");
    drop(a1);
    let a2 = std::net::TcpListener::bind("127.0.0.1:0").expect("probe");
    let addr2 = a2.local_addr().expect("addr");
    drop(a2);
    let mut all = HashMap::new();
    all.insert(nid(1), addr1);
    all.insert(nid(2), addr2);
    let dir1 = temp_dir("n1");

    let mut n1 = start_node(1, &dir1, addr1, &all, false, Arc::clone(&served), rate_bps).await;
    for _ in 0..400 {
        if n1.metrics.is_leader() {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(10)).await;
    }
    assert!(n1.metrics.is_leader(), "node 1 must elect itself");

    n1.handle.add_learner(2).await.expect("add learner");

    until_put(&n1.handle, &key(0), &value(0)).await;
    for i in 1..=WRITES {
        until_put(&n1.handle, &key(i), &value(i)).await;
    }
    let bytes = biggest_snapshot(&dir1);
    assert!(
        bytes > CAP as u64,
        "the snapshot must exceed the {CAP}-byte cap (it is {bytes})"
    );

    let dir2 = temp_dir("n2");
    let n2 = start_node(2, &dir2, addr2, &all, true, served, rate_bps).await;
    link(&n1.handle, &n2.handle);

    let t0 = Instant::now();
    let mut caught_up = false;
    for _ in 0..3_000 {
        if let Ok(Some(v)) = n2.handle.get_stale(&key(WRITES)).await
            && v == value(WRITES)
        {
            caught_up = true;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    let secs = t0.elapsed().as_secs_f64();
    assert!(
        caught_up,
        "learner must catch up in {tag} (installed={})",
        n2.metrics.snapshots_installed_total()
    );
    eprintln!(
        "[tonic-bench] {tag}: snapshot {bytes} bytes, catch-up {secs:.2}s = {:.1} KiB/s \
         (chunk = min(256 KiB, cap/2 = {} KiB))",
        bytes as f64 / 1024.0 / secs,
        CAP / 2 / 1024,
    );

    for node in [&mut n1, &mut { n2 }] {
        if let Some(task) = node.task.take() {
            task.abort();
        }
        node.factory.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&dir1);
    let _ = std::fs::remove_dir_all(&dir2);
    (bytes, secs)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn real_tonic_streamed_snapshot_benchmark() {
    eprintln!("[tonic-bench] === real tonic streamed snapshot (--release) ===");
    run_case("unlimited (rate=0)", 0).await;
    run_case(&format!("rate-limited ({RATE_BPS} B/s)"), RATE_BPS).await;
    eprintln!("[tonic-bench] === done ===");
}
