//! On-demand comprehensive benchmark of the arachne runtime (run with `--release`).
//!
//! This is **not** a CI test: every test here is `#[ignore]`, so the normal
//! workspace suite never executes it. Run it explicitly with:
//!
//! ```text
//! CARGO_TARGET_DIR=.dsh-target cargo test --release -p arachne \
//!     --test bench_runtime -- --ignored --nocapture
//! ```
//!
//! What it measures (all in-process, no network):
//! * the raft-core ceiling — `RawNode::propose` + `ready` on a single-node
//!   `MemStorage`, with no storage fsync and no transport (the vendored
//!   `third_party/raft` criterion benches are not in the lockfile, so this is
//!   the in-repo substitute);
//! * a 3-node in-memory cluster under `FsyncPolicy::Always` (the production
//!   default): first-leader-election time, write throughput + p99, weak-read
//!   (`get_stale`) throughput + p99, linearizable-read throughput + p99, and
//!   failover time (leader stops -> new quorum leader);
//! * the same 3-node cluster under `FsyncPolicy::BatchMs(10)` for the grouped
//!   flush effect on write throughput;
//! * an in-memory **streamed** snapshot catch-up (rev T): a late learner
//!   catching up through `with_snapshot_streaming`, reported as bytes / wall
//!   time (the transfer + install rate).
//!
//! Machines differ, so treat these as relative numbers. Write numbers are
//! dominated by the filesystem's full-durability flush (~10ms on this
//! machine's disk), exactly as the `read_latency` smoke gate documents.
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids the std clock there), so this file times with
//! `tokio::time::Instant` and uses no select macros.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::client::Handle;
use arachne::consensus::RaftNodeConfig;
use arachne::runtime::{Runtime, RuntimeConfig, RuntimeThread};
use arachne::storage::snapshot::snapshot_file_name;
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::TransportFactory;
use arachne::{ArachneError, Metrics, NodeId, Profile, ProfileConfig};
use arachne_testsupport::InMemoryTransportFactory;
use raft::eraftpb::ConfState;
use raft::storage::MemStorage;
use raft::{Config, RawNode};
use slog::{o, Drain, Logger};
use tokio::time::Instant;

/// One worker's worth of concurrent write pressure (like `read_latency.rs`).
const WRITERS: usize = 4;
/// How many sequential puts each worker attempts (per measured run).
const WRITES_PER_WORKER: usize = 40;
/// Sample counts for the read paths.
const WEAK_READ_SAMPLES: usize = 10_000;
const LINEAR_READ_SAMPLES: usize = 2_000;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-bench-{tag}-{}-{n}",
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
                SocketAddr::from(([127, 0, 0, 1], 8200 + i as u16)),
            )
        })
        .collect()
}

fn test_profile(wal_trailing_keep_bytes: u64) -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 500,
        rpc_timeout_ms: 300,
        wal_trailing_keep_bytes,
        ..Profile::Lan.config()
    }
}

fn wal_opts(i: u64, policy: FsyncPolicy) -> WalOptions {
    WalOptions {
        cluster_id: "bench".into(),
        node_id: format!("n{i}"),
        config: WalConfig {
            fsync_policy: policy,
            segment_bytes: 1 << 20,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

fn percentile(samples: &[u128], p: f64) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = ((sorted.len() as f64) * p).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index]
}

fn median(samples: &[u128]) -> u128 {
    percentile(samples, 0.50)
}

/// Blocks on a put, tolerating transient leadership churn.
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

/// A `n`-node in-memory cluster. `small` enables 4 KiB snapshot threshold +
/// zero trailing window so the log can be driven into snapshot territory.
struct Cluster {
    dirs: Vec<PathBuf>,
    handles: Vec<Handle>,
    metrics: Vec<Arc<Metrics>>,
    runtimes: Vec<RuntimeThread>,
}

async fn build_cluster(n: u64, policy: FsyncPolicy, small: bool) -> Cluster {
    let factory = InMemoryTransportFactory::new();
    let addresses = addresses(n);
    let profile = ProfileConfig {
        snapshot_threshold_bytes: if small { 4 * 1024 } else { 64 << 20 },
        wal_trailing_keep_bytes: if small { 0 } else { 64 << 20 },
        ..test_profile(0)
    };
    let mut dirs = Vec::new();
    let mut handles = Vec::new();
    let mut metrics = Vec::new();
    let mut runtimes = Vec::new();
    for i in 1..=n {
        let dir = temp_dir(&format!("n{i}"));
        let wal = WalStorage::open(&dir, wal_opts(i, policy)).expect("open wal");
        let (tx, rx) = factory.create(node_id(i));
        let m = Arc::new(Metrics::new());
        let config = RuntimeConfig {
            self_raft_id: i,
            self_node_id: node_id(i),
            peers: peers_of(i, n),
            addresses: addresses.clone(),
            raft: RaftNodeConfig::from_profile(&profile),
            profile: profile.clone(),
            metrics: Arc::clone(&m),
        };
        let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("runtime");
        runtimes.push(runtime.spawn_dedicated().expect("spawn consensus thread"));
        handles.push(handle);
        metrics.push(m);
        dirs.push(dir);
    }
    for i in 0..n as usize {
        for j in 0..n as usize {
            if i != j {
                handles[i].register_peer(handles[j].clone());
            }
        }
    }
    Cluster {
        dirs,
        handles,
        metrics,
        runtimes,
    }
}

impl Cluster {
    fn leader_pos(&self) -> usize {
        self.metrics
            .iter()
            .position(|m| m.is_leader())
            .expect("a leader must exist")
    }

    /// A quorum leader: at least one self-reported leader and no node that
    /// still thinks there is no leader.
    async fn wait_leader(&self) -> usize {
        for _ in 0..2_000 {
            if let Some(pos) = self.metrics.iter().position(|m| m.is_leader())
                && self.metrics.iter().all(|m| m.leader_id() != 0)
            {
                return pos;
            }
            tokio::time::sleep(core::time::Duration::from_millis(5)).await;
        }
        panic!("no quorum leader elected");
    }

    async fn write_throughput(&self, tag: &str) {
        let tag = tag.to_owned();
        let leader = self.leader_pos();
        let leader_handle = self.handles[leader].clone();
        until_put(&leader_handle, b"seed", b"v")
            .await
            .expect("the seed write commits");
        let samples: Arc<tokio::sync::Mutex<Vec<u128>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let started = Instant::now();
        let mut workers = Vec::new();
        for w in 0..WRITERS {
            let handle = leader_handle.clone();
            let shared = Arc::clone(&samples);
            let value = tag.clone();
            workers.push(tokio::spawn(async move {
                let mut ok = 0usize;
                for i in 0..WRITES_PER_WORKER {
                    let key = format!("{value}-{w}-{i}");
                    let t0 = Instant::now();
                    if until_put(&handle, key.as_bytes(), b"x").await.is_ok() {
                        ok += 1;
                        shared.lock().await.push(t0.elapsed().as_micros());
                    }
                }
                ok
            }));
        }
        let mut wrote = 0usize;
        for worker in workers {
            wrote += worker.await.expect("writer task");
        }
        let elapsed = started.elapsed();
        let all = samples.lock().await.clone();
        eprintln!(
            "[bench] write throughput ({tag}, FsyncPolicy): {:.0} ops/s, p50 {:.0}us p99 {:.0}us \
             ({} puts, {:.2}s)",
            wrote as f64 / elapsed.as_secs_f64(),
            median(&all),
            percentile(&all, 0.99),
            wrote,
            elapsed.as_secs_f64(),
        );
        assert!(
            wrote >= WRITERS * WRITES_PER_WORKER / 2,
            "the write storm failed: only {wrote} puts"
        );
    }

    async fn read_throughputs(&self, tag: &str) {
        let leader = self.leader_pos();
        let h = self.handles[leader].clone();

        let mut weak = Vec::with_capacity(WEAK_READ_SAMPLES);
        let t0 = Instant::now();
        for _ in 0..WEAK_READ_SAMPLES {
            let s = Instant::now();
            let _ = h.get_stale(b"seed").await;
            weak.push(s.elapsed().as_micros());
        }
        let wk = t0.elapsed().as_secs_f64();
        eprintln!(
            "[bench] weak-read throughput ({tag}): {:.0} ops/s, p50 {:.0}us p99 {:.0}us ({} reads)",
            WEAK_READ_SAMPLES as f64 / wk,
            median(&weak),
            percentile(&weak, 0.99),
            WEAK_READ_SAMPLES,
        );

        let mut lin = Vec::with_capacity(LINEAR_READ_SAMPLES);
        let t0 = Instant::now();
        for _ in 0..LINEAR_READ_SAMPLES {
            let s = Instant::now();
            if h.get(b"seed").await.is_ok() {
                lin.push(s.elapsed().as_micros());
            }
        }
        let lk = t0.elapsed().as_secs_f64();
        eprintln!(
            "[bench] linear-read throughput ({tag}): {:.0} ops/s, p50 {:.0}us p99 {:.0}us ({} reads)",
            LINEAR_READ_SAMPLES as f64 / lk,
            median(&lin),
            percentile(&lin, 0.99),
            lin.len(),
        );
    }

    /// Stop the current leader (drop its client handle + detach its thread) and
    /// time how long the remaining 3-node-configuration takes to agree on a new
    /// quorum leader.
    async fn failover(&mut self) {
        let victim = self.leader_pos();
        let victim_raft_id = victim as u64 + 1;
        let dead_handle = self.handles.remove(victim);
        let dead_rt = self.runtimes.remove(victim);
        self.metrics.remove(victim);
        drop(dead_handle);
        // A plain `drop` here would not stop the actor: every survivor holds a
        // clone of this node's `Handle` in its peer map (register_peer stores a
        // full handle, which owns the command-sender), so the command channel
        // never closes and the "dead" leader would keep heartbeating — no
        // re-election. `shutdown()` sends the explicit stop signal and joins.
        dead_rt.shutdown();
        let t0 = Instant::now();
        let mut ok = false;
        for _ in 0..2_000 {
            if self.metrics.iter().any(|m| m.is_leader())
                && self.metrics
                    .iter()
                    .all(|m| m.leader_id() != 0 && m.leader_id() != victim_raft_id as u64)
            {
                ok = true;
                break;
            }
            tokio::time::sleep(core::time::Duration::from_millis(5)).await;
        }
        if !ok {
            let states: Vec<(bool, u64)> = self
                .metrics
                .iter()
                .map(|m| (m.is_leader(), m.leader_id()))
                .collect();
            eprintln!("[bench] failover diag (no new leader): survivors={states:?}");
        }
        assert!(ok, "no new quorum leader after the leader stopped");
        eprintln!(
            "[bench] failover (leader n{victim_raft_id} stopped): {:.0} ms to new quorum leader",
            t0.elapsed().as_millis()
        );
    }

    /// Stop every node (explicit stop + join, so the WAL lock is released) and
    /// remove the temp data dirs.
    async fn teardown(&mut self) {
        for rt in self.runtimes.drain(..) {
            rt.shutdown();
        }
        self.handles.clear();
        tokio::time::sleep(core::time::Duration::from_millis(100)).await;
        for dir in self.dirs.drain(..) {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i}").into_bytes()
}

fn value(i: u64) -> Vec<u8> {
    let mut v = format!("v{i}").into_bytes();
    v.resize(1024, b'x');
    v
}

fn biggest_snapshot(dir: &std::path::Path) -> u64 {
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

/// The consensus-core ceiling: raft's own propose + ready processing on a
/// one-node `MemStorage`. No storage fsync, no transport, no apply — this is
/// the cheapest the core loop can be.
fn raw_node_micro() {
    let storage = MemStorage::new_with_conf_state(ConfState::from((vec![1], vec![])));
    let config = Config::new(1);
    let mut raw = RawNode::new(&config, storage, &logger()).expect("raw node");
    // A single-node cluster self-elects after ~election_tick ticks.
    for _ in 0..60 {
        raw.tick();
        while raw.has_ready() {
            let r = raw.ready();
            raw.advance(r);
        }
    }
    // Warmup so the first timed loop is not paying for allocation warm-up.
    for _ in 0..3_000 {
        raw.propose(vec![], vec![0u8; 8]).expect("propose");
        while raw.has_ready() {
            let r = raw.ready();
            raw.advance(r);
        }
    }
    for (label, payload, n) in [
        ("8B", vec![0u8; 8], 100_000u64),
        ("1KiB", vec![0u8; 1024], 20_000u64),
    ] {
        let t0 = Instant::now();
        for _ in 0..n {
            raw.propose(vec![], payload.clone()).expect("propose");
            while raw.has_ready() {
                let r = raw.ready();
                raw.advance(r);
            }
        }
        let el = t0.elapsed();
        eprintln!(
            "[bench] raft core propose+ready ({label}): {:.0} ns/op, {:.0} ops/s",
            el.as_nanos() as f64 / n as f64,
            n as f64 / el.as_secs_f64(),
        );
    }
}

/// In-memory **streamed** snapshot catch-up (rev T): nodes 1-3 run, node 4
/// joins late as a learner after the leader's log is compacted past what a
/// follower would need. The learner can only be caught up by the streamed
/// snapshot path (`with_snapshot_streaming` + a per-peer byte source).
/// Returns (snapshot bytes, catch-up seconds).
async fn streaming_catchup_bench() -> (u64, f64) {
    let factory = InMemoryTransportFactory::new().with_snapshot_streaming();
    let addresses = addresses(4);
    let profile = ProfileConfig {
        snapshot_threshold_bytes: 4 * 1024,
        wal_trailing_keep_bytes: 0,
        ..test_profile(0)
    };

    let mut dirs = Vec::new();
    let mut handles = Vec::new();
    let mut metrics = Vec::new();
    let mut runtimes = Vec::new();

    // Register node 4's transport halves up front (the switch must know it).
    let (tx4, rx4) = factory.create(node_id(4));
    for i in 1..=3u64 {
        let dir = temp_dir(&format!("s{i}"));
        let wal = WalStorage::open(&dir, wal_opts(i, FsyncPolicy::Always)).expect("open wal");
        let (tx, rx) = factory.create(node_id(i));
        let m = Arc::new(Metrics::new());
        let config = RuntimeConfig {
            self_raft_id: i,
            self_node_id: node_id(i),
            peers: peers_of(i, 4),
            addresses: addresses.clone(),
            raft: RaftNodeConfig::from_profile(&profile),
            profile: profile.clone(),
            metrics: Arc::clone(&m),
        };
        let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("runtime");
        runtimes.push(runtime.spawn_dedicated().expect("spawn consensus thread"));
        handles.push(handle);
        metrics.push(m);
        dirs.push(dir);
    }
    for i in 0..3usize {
        for j in 0..3usize {
            if i != j {
                handles[i].register_peer(handles[j].clone());
            }
        }
    }

    let mut leader = None;
    for _ in 0..2_000 {
        if let Some(pos) = metrics.iter().position(|m| m.is_leader())
            && metrics.iter().all(|m| m.leader_id() != 0)
        {
            leader = Some(pos);
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    let leader = leader.expect("a quorum leader");
    let leader_raft_id = leader as u64 + 1;
    let leader_handle = handles[leader].clone();
    let leader_dir = dirs[leader].clone();

    leader_handle
        .add_learner(4)
        .await
        .expect("learner must be added");
    for i in 1..=200u64 {
        until_put(&leader_handle, &key(i), &value(i))
            .await
            .expect("write during the catch-up setup");
    }
    let bytes = biggest_snapshot(&leader_dir);
    assert!(bytes > 0, "the leader must have produced a snapshot");

    // Serve the leader's snapshot directory as the learner's byte source.
    factory.set_snapshot_source(node_id(leader_raft_id), {
        let dir = leader_dir.clone();
        Arc::new(move |index, term| {
            std::fs::read(dir.join(snapshot_file_name(index, term))).ok()
        })
    });

    // Start the learner now: it can only be caught up through the snapshot.
    let dir4 = temp_dir("s4");
    let wal4 = WalStorage::open(&dir4, wal_opts(4, FsyncPolicy::Always)).expect("open wal");
    let m4 = Arc::new(Metrics::new());
    let mut raft4 = RaftNodeConfig::from_profile(&profile);
    raft4.bootstrap_voters = Some(vec![leader_raft_id]);
    raft4.join_as_learner = true;
    let config4 = RuntimeConfig {
        self_raft_id: 4,
        self_node_id: node_id(4),
        peers: peers_of(4, 4),
        addresses: addresses.clone(),
        raft: raft4,
        profile: profile.clone(),
        metrics: Arc::clone(&m4),
    };
    let (rt4, handle4) = Runtime::new(config4, wal4, tx4, rx4, &logger()).expect("runtime");
    for h in &handles {
        h.register_peer(handle4.clone());
        handle4.register_peer(h.clone());
    }
    let rt4 = rt4.spawn_dedicated().expect("spawn consensus thread");

    let t0 = Instant::now();
    let mut caught_up = false;
    for _ in 0..1_500 {
        if let Ok(Some(v)) = handle4.get_stale(&key(200)).await
            && v == value(200)
        {
            caught_up = true;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    let secs = t0.elapsed().as_secs_f64();
    assert!(
        caught_up,
        "the late learner must catch up through the streamed snapshot (installed={})",
        m4.snapshots_installed_total(),
    );
    eprintln!(
        "[bench] streamed snapshot catch-up (in-memory, learner n4): {} bytes in {:.2}s = \
         {:.1} KiB/s (snapshot {} installed: {})",
        bytes,
        secs,
        bytes as f64 / 1024.0 / secs,
        bytes,
        m4.snapshots_installed_total(),
    );

    drop(handle4);
    rt4.shutdown();
    for rt in runtimes.drain(..) {
        rt.shutdown();
    }
    handles.clear();
    tokio::time::sleep(core::time::Duration::from_millis(100)).await;
    for dir in dirs.into_iter().chain([dir4]) {
        let _ = std::fs::remove_dir_all(dir);
    }
    (bytes, secs)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn comprehensive_runtime_benchmark() {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    eprintln!(
        "[bench] === arachne comprehensive benchmark (--release, {cores} cores) ==="
    );
    raw_node_micro();

    // Cluster 1: the production default (per-write full fsync).
    let t0 = Instant::now();
    let mut c1 = build_cluster(3, FsyncPolicy::Always, false).await;
    let leader = c1.wait_leader().await;
    eprintln!(
        "[bench] 3-node first-leader-elected (Always): {:.0} ms (leader n{})",
        t0.elapsed().as_millis(),
        leader + 1,
    );
    c1.write_throughput("always").await;
    c1.read_throughputs("always").await;
    c1.failover().await;
    c1.teardown().await;

    // Cluster 2: grouped-flush policy, write throughput only.
    let mut c2 = build_cluster(3, FsyncPolicy::BatchMs(10), false).await;
    let t0 = Instant::now();
    let _leader2 = c2.wait_leader().await;
    eprintln!(
        "[bench] 3-node first-leader-elected (BatchMs10): {:.0} ms",
        t0.elapsed().as_millis(),
    );
    c2.write_throughput("batchms10").await;
    c2.teardown().await;

    // Streamed snapshot catch-up over the in-memory streaming path.
    streaming_catchup_bench().await;
    eprintln!("[bench] === done ===");
}
