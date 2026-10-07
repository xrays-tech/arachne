//! P4 4.1 gate: many **concurrent** linearizable reads all resolve within a
//! bounded deadline while the cluster is under a write storm.
//!
//! `read_latency` measures reads **sequentially** (one `get` after another)
//! against a storm. This file fires `READERS` reads *at the same time* against
//! the same storm. If the SM value-read ever slips back onto the actor's
//! current-thread loop, those reads serialize on that thread (each doing a
//! ReadIndex round + an `sm.get`) and the whole batch stalls behind each other
//! plus every write tick; the deadline blows. That stall is exactly the
//! regression this gate exists to catch.
//!
//! The seed key `b"seed"` is written once before the storm and never touched by
//! the storm writers, so every read returns the same stable `Some(b"v")`. The
//! storm writes to distinct `storm-{w}-{i}` keys (see `read_latency`), so it
//! keeps the log (and the actor) busy without moving the value a read expects.
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids the std clock there, as a literal substring), so this file
//! times with `tokio::time::Instant` and uses no tokio select macros.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use arachne_kv::client::Handle;
use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::TransportFactory;
use arachne_kv::{ArachneError, Metrics, NodeId, Profile, ProfileConfig};
use arachne_kv_testsupport::InMemoryTransportFactory;
use slog::{Discard, Drain, Logger, o};
use tokio::time::Instant;

const N: u64 = 3;
/// Concurrent linearizable readers fired at once during the storm.
const READERS: usize = 16;
/// Concurrent writers during the storm.
const WRITERS: usize = 4;
/// Writes each storm writer attempts.
const WRITES_PER_WRITER: usize = 60;
/// Every reader must finish within this wall-clock budget. Kept comfortably
/// above a healthy off-actor read (single-digit ms) but far below the ~10ms a
/// write would add per on-actor tick behind a full-durability flush: a storm
/// serialising 16 reads on the actor's current thread would blow past it.
const READ_BUDGET_US: u128 = 500_000;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-off-actor-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(Discard.fuse(), o!())
}

fn node_id(i: u64) -> NodeId {
    NodeId::from(format!("n{i}"))
}

fn peers_of(self_id: u64, n: u64) -> std::collections::HashMap<u64, NodeId> {
    (1..=n)
        .filter(|j| *j != self_id)
        .map(|j| (j, node_id(j)))
        .collect()
}

fn addresses(n: u64) -> std::collections::HashMap<NodeId, SocketAddr> {
    (1..=n)
        .map(|i| (
            node_id(i),
            SocketAddr::from(([127, 0, 0, 1], 7300 + i as u16)),
        ))
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
        cluster_id: "off-actor-concurrent".into(),
        node_id: format!("n{i}"),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 1 << 20,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

/// Fire all concurrent reads and return the set of completed latencies (us) plus
/// any failures.
async fn concurrent_reads(
    handle: &Handle,
    key: &[u8],
    readers: usize,
) -> (Vec<u128>, Vec<String>) {
    let mut handles = Vec::with_capacity(readers);
    for _i in 0..readers {
        let handle = handle.clone();
        let key = key.to_vec();
        handles.push(tokio::spawn(async move {
            let started = Instant::now();
            let (us, failure) = match handle.get(&key).await {
                Ok(Some(value)) if value == b"v" => {
                    (started.elapsed().as_micros(), None::<String>)
                }
                Ok(_) => (
                    started.elapsed().as_micros(),
                    Some("unexpected value".into()),
                ),
                Err(e) => (
                    started.elapsed().as_micros(),
                    Some(format!("get failed: {e}")),
                ),
            };
            (us, failure)
        }));
    }
    let mut latencies = Vec::with_capacity(readers);
    let mut failures = Vec::with_capacity(readers);
    for h in handles {
        let (us, failure) = h.await.expect("reader task panicked");
        latencies.push(us);
        if let Some(msg) = failure {
            failures.push(msg);
        }
    }
    (latencies, failures)
}

fn max_latency(latencies: &[u128]) -> u128 {
    latencies.iter().copied().max().unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_linear_reads_resolve_within_a_deadline_under_a_write_storm() -> Result<(), String> {
    let profile = test_profile();
    let factory = InMemoryTransportFactory::new();
    let addresses = addresses(N);

    let mut handles: Vec<Handle> = Vec::new();
    let mut metrics: Vec<Arc<Metrics>> = Vec::new();
    let mut runtimes: Vec<arachne_kv::RuntimeThread> = Vec::new();
    let mut dirs = Vec::new();
    for i in 1..=N {
        let dir = temp_dir(&format!("n{i}"));
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
        runtimes.push(runtime.spawn_dedicated().expect("spawn the consensus thread"));
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

    // Seed the stable key every read expects; let the cluster catch up.
    for _ in 0..800 {
        match leader_handle.put(b"seed", b"v").await {
            Ok(()) => break,
            Err(ArachneError::Timeout)
            | Err(ArachneError::QuorumUnavailable)
            | Err(ArachneError::NotLeader { .. })
            | Err(ArachneError::Busy) => {
                tokio::time::sleep(core::time::Duration::from_millis(2)).await;
            }
            Err(e) => return Err(format!("seed write failed: {e}")),
        }
    }
    tokio::time::sleep(core::time::Duration::from_millis(300)).await;

    // Start the write storm (distinct keys; never touches `b"seed"`).
    let mut writers = Vec::new();
    for w in 0..WRITERS {
        let handle = leader_handle.clone();
        writers.push(tokio::spawn(async move {
            for i in 0..WRITES_PER_WRITER {
                let key = format!("storm-{w}-{i}").into_bytes();
                let _ = handle.put(&key, b"x").await;
            }
        }));
    }

    // Fire all concurrent reads at once, during the storm.
    let (latencies, failures) = concurrent_reads(&leader_handle, b"seed", READERS).await;
    let max_us = max_latency(&latencies);
    let readers_count = READERS;
    let ok = READERS - failures.len();
    let failures_count = failures.len();
    let budget = READ_BUDGET_US;
    eprintln!(
        "[off-actor] concurrent reads {readers_count}: ok={ok}/{readers_count} max={max_us}us budget={budget}us failures={failures_count}",
    );

    assert!(
        failures.is_empty(),
        "some concurrent reads failed: {failures:?}",
    );
    assert!(
        latencies.len() == READERS,
        "only {len} of {readers_count} concurrent reads resolved",
        len = latencies.len(),
        readers_count = readers_count,
    );
    assert!(
        max_us <= READ_BUDGET_US,
        "the slowest concurrent read {max_us}us blew the {READ_BUDGET_US}us deadline \
         (a storm-serialised, on-actor read batch would)"
    );

    // Drop every handle (closes command channels -> ends the actors), release the
    // detached threads' WAL locks, then drop the WAL dirs.
    drop(leader_handle);
    for h in &handles {
        drop(h.clone());
    }
    drop(runtimes);
    tokio::time::sleep(core::time::Duration::from_millis(300)).await;
    for dir in dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
    Ok(())
}
