//! Integration: what the asynchronous durability pipeline is *for* — the
//! `slow_fsync` shape (propsol v0.2.13 P, §8.2).
//!
//! The pipeline does not make a device flush faster (measurement showed it does
//! not move p99 at all under `FsyncPolicy::Always`). Its value is that the actor
//! is not the thing waiting: while a slow disk is busy, the node keeps ticking,
//! heartbeating and serving.
//!
//! This test models a slow disk with a test-only flush delay inside the WAL
//! (feature `fault-injection`) and measures the same thing both ways:
//!
//! * **without** the pipeline the delay happens in the actor's own `fsync`, so
//!   a weak read issued during it waits for the device;
//! * **with** the pipeline the delay happens on the WAL's flusher thread, so the
//!   same read is served immediately.
//!
//! The assertion is therefore a contrast between the two runs, not an absolute
//! number: that is the property the pipeline was built for, and it is the one
//! `read_latency` cannot see.
#![cfg(feature = "fault-injection")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::consensus::RaftNodeConfig;
use arachne::runtime::{Runtime, RuntimeConfig};
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::TransportFactory;
use arachne::{Metrics, NodeId, Profile, ProfileConfig};
use arachne_testsupport::InMemoryTransportFactory;
use slog::{o, Drain, Logger};
use tokio::time::Instant;

/// A delay long enough that the difference cannot be scheduling noise.
const FLUSH_DELAY_MS: u64 = 400;
/// How many weak reads to sample while a write is being flushed.
const READS: usize = 40;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-slow-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn node_id(i: u64) -> NodeId {
    NodeId::from(format!("n{i}"))
}

fn wal_opts(i: u64) -> WalOptions {
    WalOptions {
        cluster_id: "slow-fsync".into(),
        node_id: format!("n{i}"),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 1 << 20,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

/// The worst weak-read latency observed while one write was being flushed, on a
/// single-node cluster with `FLUSH_DELAY_MS` of simulated disk latency.
async fn worst_read_during_a_slow_flush(offloaded: bool) -> u128 {
    let profile = ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 600,
        rpc_timeout_ms: 300,
        ..Profile::Lan.config()
    };
    let dir = temp_dir(if offloaded { "pipe" } else { "sync" });
    let mut wal = WalStorage::open(&dir, wal_opts(1)).expect("open wal");
    wal.set_flush_delay_ms(FLUSH_DELAY_MS);
    if offloaded {
        wal.enable_offloaded_durability().expect("enable offload");
    }
    let factory = InMemoryTransportFactory::new();
    let (tx, rx) = factory.create(node_id(1));
    let metrics = Arc::new(Metrics::new());
    let config = RuntimeConfig {
        self_raft_id: 1,
        self_node_id: node_id(1),
        peers: HashMap::new(),
        addresses: HashMap::new(),
        raft: RaftNodeConfig::from_profile(&profile),
        profile,
        metrics: Arc::clone(&metrics),
    };
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("runtime");
    let thread = runtime.spawn_dedicated().expect("spawn");

    // Elect, then commit one durable write so later reads have something to see.
    for _ in 0..400 {
        if metrics.is_leader() {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert!(metrics.is_leader(), "the single node must elect itself");
    let mut seeded = false;
    for _ in 0..200 {
        if handle.put(b"seed", b"v").await.is_ok() {
            seeded = true;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert!(seeded, "the seed write must commit");

    // Start a write and hammer weak reads while its flush is in the air. The
    // write is issued from another task so the reads race the flush.
    let writer = tokio::spawn({
        let handle = handle.clone();
        async move { handle.put(b"during", b"v").await }
    });
    // Let the write reach the storage (and its flush start).
    tokio::time::sleep(core::time::Duration::from_millis(50)).await;
    let mut worst = 0u128;
    for _ in 0..READS {
        let started = Instant::now();
        let _ = handle.get_stale(b"seed").await;
        worst = worst.max(started.elapsed().as_micros());
        tokio::time::sleep(core::time::Duration::from_millis(2)).await;
    }
    let _ = writer.await;

    drop(handle);
    thread.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
    worst
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pipeline_keeps_serving_while_a_slow_disk_flushes() {
    let without = worst_read_during_a_slow_flush(false).await;
    let with = worst_read_during_a_slow_flush(true).await;
    eprintln!(
        "[slow-fsync] worst weak read during a {FLUSH_DELAY_MS}ms flush: \
         synchronous {without}us, pipeline {with}us"
    );

    assert!(
        without >= FLUSH_DELAY_MS as u128 * 500,
        "without the pipeline the read must wait for the actor's flush (got {without}us)"
    );
    assert!(
        with < without / 10,
        "with the pipeline the actor must keep serving: {with}us vs {without}us"
    );
}
