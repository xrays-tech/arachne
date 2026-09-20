//! Integration: linearizable-read latency must not be dragged down by a write
//! storm (M2 acceptance ⑤, propsol §347 / §701 R6 / v0.2.11 N).
//!
//! This is a **smoke gate**, not an SLO: test-plan §309 keeps full p99 SLO
//! benchmarking for a separate workstream and asks for a threshold here.
//!
//! The references are measured in the same run rather than hard-coded, because
//! what dominates a storm here is the storage's full-durability flush:
//! `FsyncPolicy::Always` costs ≈10ms per write on this machine's filesystem, so
//! *everything* client-facing queues behind the write path. A read cannot be
//! faster than the loop can get to it, and an absolute p99 budget would only be
//! measuring the filesystem — it would fail on a slow disk and pass on a fast
//! one while saying nothing about the runtime.
//!
//! So the gate pins the property the separate apply task must provide:
//!
//! * a **weak read** (actor → apply task → reply: no quorum round, no apply
//!   wait) stays within a small factor of a **write** — if the apply task ever
//!   queued behind the write backlog, this is the first thing to blow up;
//! * a **linearizable read** stays within a small factor of the weak read. That
//!   gap is the ReadIndex round plus the wait for the read index to be applied,
//!   which is exactly where a lagging apply path would show up.
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids the std clock there, as a literal substring), so this file
//! times with `tokio::time::Instant` and uses no tokio select macros.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
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
use tokio::time::Instant;

const N: u64 = 3;
/// Idle and stormed read samples.
const GETS: usize = 120;
/// Concurrent writers during the storm.
const WRITERS: usize = 4;
/// Writes each storm writer attempts.
const WRITES_PER_WRITER: usize = 60;
/// A weak read under the storm may take this many times a write's p99. Both go
/// through the same actor loop, so this is "the apply path adds no queueing".
const WEAK_VS_WRITE_FACTOR: u128 = 4;
/// A linearizable read may take this many times a weak read's p99; the gap is
/// the ReadIndex round plus the wait for the read index to be applied.
const LINEARIZABLE_VS_WEAK_FACTOR: u128 = 3;
/// Allowance in microseconds on top of the scaled reference, for scheduling
/// noise the reference cannot capture.
const BUDGET_SLACK_US: u128 = 50_000;
/// Absolute ceiling: even a pathological run must not blow past this.
const ABSOLUTE_CEILING_US: u128 = 2_000_000;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-lat-{tag}-{}-{n}", std::process::id()));
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
        election_timeout_ms: 500,
        rpc_timeout_ms: 300,
        ..Profile::Lan.config()
    }
}

fn wal_opts(i: u64) -> WalOptions {
    WalOptions {
        cluster_id: "read-latency".into(),
        node_id: format!("n{i}"),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 1 << 20,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

/// Time one linearizable read, in microseconds. `None` for a transient failure
/// (the sample is skipped rather than counted as an outlier).
async fn timed_get(handle: &Handle, key: &[u8]) -> Option<u128> {
    let started = Instant::now();
    match handle.get(key).await {
        Ok(_) => Some(started.elapsed().as_micros()),
        Err(_) => None,
    }
}

/// The 99th percentile of `samples` (microseconds).
fn percentile_99(samples: &[u128]) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = ((sorted.len() as f64) * 0.99).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linearizable_read_latency_is_bounded_under_a_write_storm() {
    let profile = test_profile();
    let factory = InMemoryTransportFactory::new();
    let addresses = addresses(N);

    let mut handles: Vec<Handle> = Vec::new();
    let mut metrics: Vec<Arc<Metrics>> = Vec::new();
    let mut tasks = Vec::new();
    let mut dirs = Vec::new();
    for i in 1..=N {
        let dir = temp_dir(&format!("n{i}"));
        // The gate measures the **default** (synchronous) durability path: the
        // offloaded pipeline is opt-in, and measurement showed it does not move
        // these numbers — under `Always` the device flush is the bound, so
        // moving it off the actor changes where the wait happens, not how long
        // it is (see propsol v0.2.13 P and the handoff). Its value is that the
        // actor keeps ticking and serving while a slow disk is busy.
        // The gate measures the **default** (synchronous) durability path: the
        // offloaded pipeline is opt-in, and measurement showed it does not move
        // these numbers — under `Always` the device flush is the bound (see
        // propsol v0.2.13 P and the handoff). Its value is that the actor keeps
        // ticking and serving while a slow disk is busy.
        let wal = WalStorage::open(&dir, wal_opts(i)).expect("open wal");
        let (tx, rx) = factory.create(node_id(i));
        let m = Arc::new(Metrics::new());
        let config = RuntimeConfig {
            self_raft_id: i,
            self_node_id: node_id(i),
            peers: peers_of(i, N),
            addresses: addresses.clone(),
            raft: RaftNodeConfig::from_profile(&profile),
            profile: profile.clone(),
            metrics: Arc::clone(&m),
        };
        let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("runtime");
        tasks.push(tokio::spawn(runtime.run()));
        handles.push(handle);
        metrics.push(m);
        dirs.push(dir);
    }
    for i in 0..N as usize {
        for j in 0..N as usize {
            if i != j {
                handles[i].register_peer(handles[j].clone());
            }
        }
    }

    // Wait for a leader every node agrees on.
    let mut leader = 0usize;
    for _ in 0..800 {
        if let Some(pos) = metrics.iter().position(|m| m.is_leader())
            && metrics.iter().all(|m| m.leader_id() != 0)
        {
            leader = pos;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    assert!(metrics.iter().any(|m| m.is_leader()), "no leader elected");
    let leader_handle = handles[leader].clone();

    until_put(&leader_handle, b"hot", b"v")
        .await
        .expect("the seed write commits");

    // 1. Idle baseline on the same cluster.
    let mut baseline = Vec::with_capacity(GETS);
    for _ in 0..GETS {
        if let Some(us) = timed_get(&leader_handle, b"hot").await {
            baseline.push(us);
        }
    }
    let baseline_p99 = percentile_99(&baseline);
    // An idle cluster must answer a linearizable read in the low microseconds.
    // This assertion exists because it once did not: the read waited a whole
    // heartbeat interval for the ReadIndex round, because the in-memory test
    // transport never woke the receiving actor (see the handoff §1.12). The
    // relative budgets below could not see that, so it stayed hidden for
    // several commits.
    const IDLE_CEILING_US: u128 = 5_000;
    assert!(
        baseline_p99 <= IDLE_CEILING_US,
        "an idle linearizable read must not wait for a tick: idle p99 {baseline_p99}us"
    );
    assert!(
        baseline.len() >= GETS / 2,
        "the idle baseline produced too few samples ({}), the cluster is unhealthy",
        baseline.len()
    );

    // 2. The same reads while writers flood the log. Write latency is measured
    //    too: it is the *reference* for how quickly this node can get anything
    //    done at all, and it is what the stormed reads are compared against
    //    (an absolute p99 budget would really be measuring the machine's fsync
    //    cost, not the runtime's scheduling).
    let applied_before = metrics[leader].applied_index();
    let writes: Arc<tokio::sync::Mutex<Vec<u128>>> = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let mut writers = Vec::new();
    for w in 0..WRITERS {
        let handle = leader_handle.clone();
        let samples = Arc::clone(&writes);
        writers.push(tokio::spawn(async move {
            let mut ok = 0usize;
            for i in 0..WRITES_PER_WRITER {
                let key = format!("storm-{w}-{i}");
                let started = Instant::now();
                if until_put(&handle, key.as_bytes(), b"x").await.is_ok() {
                    ok += 1;
                    samples.lock().await.push(started.elapsed().as_micros());
                }
            }
            ok
        }));
    }
    let mut storm = Vec::with_capacity(GETS);
    let mut weak = Vec::with_capacity(GETS);
    for _ in 0..GETS {
        let started = Instant::now();
        let _ = leader_handle.get_stale(b"hot").await;
        weak.push(started.elapsed().as_micros());
        if let Some(us) = timed_get(&leader_handle, b"hot").await {
            storm.push(us);
        }
    }
    let mut wrote = 0usize;
    for writer in writers {
        wrote += writer.await.expect("writer task");
    }

    let storm_p99 = percentile_99(&storm);
    let weak_p99 = percentile_99(&weak);
    let write_p99 = percentile_99(&writes.lock().await);
    eprintln!(
        "[latency] idle p99={baseline_p99}us storm p99={storm_p99}us weak p99={weak_p99}us \
         write p99={write_p99}us ({} samples) writes={wrote} applied_lag={} backlog_bytes={}",
        storm.len(),
        metrics[leader].apply_lag(),
        metrics[leader].apply_backlog_bytes(),
    );

    // 3. The storm was real: those writes were committed and applied.
    assert!(
        wrote >= WRITERS * WRITES_PER_WRITER / 2,
        "the storm must actually write: only {wrote} writes succeeded"
    );
    let applied_after = metrics[leader].applied_index();
    assert!(
        applied_after >= applied_before + wrote as u64,
        "every acknowledged storm write must be applied ({applied_before} -> {applied_after}, {wrote} writes)"
    );
    assert!(
        storm.len() >= GETS / 2,
        "the stormed reads produced too few samples ({})",
        storm.len()
    );

    // 4. The bounds. The storm is deliberately brutal: `FsyncPolicy::Always`
    //    means every write costs a full-durability flush (≈10ms on this
    //    machine's filesystem), so *everything* client-facing queues behind the
    //    write path. What the apply task must guarantee is that it does not add
    //    to that queue: a weak read (actor → apply task → reply, no quorum
    //    round, no apply wait) must stay in the same league as a write, and a
    //    linearizable read must not be much worse than a weak one — that gap is
    //    exactly the ReadIndex round plus the wait for the read index to be
    //    applied, and with a lagging apply path it is where a read would blow
    //    up.
    let write_budget = write_p99
        .saturating_mul(WEAK_VS_WRITE_FACTOR)
        .max(baseline_p99 + BUDGET_SLACK_US);
    assert!(
        weak_p99 <= write_budget,
        "a weak read waited far longer than a write under the storm: weak {weak_p99}us, \
         write {write_p99}us, budget {write_budget}us (the apply path is queueing)"
    );
    let read_budget = weak_p99
        .saturating_mul(LINEARIZABLE_VS_WEAK_FACTOR)
        .max(write_p99 + BUDGET_SLACK_US);
    assert!(
        storm_p99 <= read_budget,
        "a linearizable read degraded far more than a weak one under the storm: \
         linearizable {storm_p99}us, weak {weak_p99}us, budget {read_budget}us"
    );
    assert!(
        storm_p99 <= ABSOLUTE_CEILING_US,
        "linearizable read p99 under a storm exceeded the absolute ceiling: {storm_p99}us"
    );

    for task in tasks {
        task.abort();
    }
    tokio::time::sleep(core::time::Duration::from_millis(100)).await;
    for dir in dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}
