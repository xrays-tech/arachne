//! Phase 4 load acceptance for `multi_get` (design §10).
//!
//! A single node serves a large exact-key workload with a bounded number of
//! batched commands. The tick is the read **command count**, not wall time:
//! `metrics.multi_get_commands()` must advance by exactly the number of
//! `multi_get` calls, proving the client does not fan a batch out into per-key
//! reads (Gate-1 Q7 requires an independent counter, not a self-referential
//! loop count).
//!
//! Two tests:
//! * default (fast): 4,096 keys × ~128 B in 8 seed batches and 8 read batches;
//! * `#[ignore]` acceptance: 24,000 keys × ~200 B, batches of 512 — the
//!   request's "≤ ~100 read commands vs 24,000".
//!
//! Run the big one explicitly:
//! `cargo test -p arachne-kv --test batch_read_load -- --ignored --nocapture`
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids real-time imports there), so this file uses
//! `core::time::Duration` and `tokio::time::Instant`, never `std::time`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_kv::client::Handle;
use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::{
    Metrics, MultiGetResult, NodeId, Profile, ProfileConfig, ReadConsistency, TransportFactory,
};
use arachne_kv_testsupport::InMemoryTransportFactory;
use slog::{o, Drain, Logger};

/// Entries per seed/read batch (design §10 suggests 256–512).
const BATCH: usize = 512;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-batch-load-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn profile() -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 200,
        rpc_timeout_ms: 100,
        ..Profile::Lan.config()
    }
}

/// A single-node runtime over the in-memory transport.
async fn single_node() -> (
    tokio::task::JoinHandle<()>,
    Handle,
    Arc<Metrics>,
    PathBuf,
) {
    let dir = temp_dir("solo");
    let wal = WalStorage::open(
        &dir,
        WalOptions {
            cluster_id: "batch-load".into(),
            node_id: "n1".into(),
            config: WalConfig {
                fsync_policy: FsyncPolicy::Always,
                segment_bytes: WalConfig::default().segment_bytes,
            },
            created_at_millis: 0,
            fsync_observer: None,
        },
    )
    .expect("open wal");

    let factory = InMemoryTransportFactory::new();
    let (tx, rx) = factory.create(NodeId::from("n1"));
    let metrics = Arc::new(Metrics::new());
    let profile = profile();
    let config = RuntimeConfig {
        self_raft_id: 1,
        self_node_id: NodeId::from("n1"),
        peers: HashMap::new(),
        addresses: HashMap::new(),
        raft: RaftNodeConfig::from_profile(&profile),
        profile,
        metrics: Arc::clone(&metrics),
    };
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("build runtime");
    let task = tokio::spawn(runtime.run());
    for _ in 0..400 {
        if metrics.is_leader() {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert!(metrics.is_leader(), "single node must elect itself");
    (task, handle, metrics, dir)
}

fn key(i: usize) -> Vec<u8> {
    format!("key{i:05}").into_bytes()
}

/// A value exactly `width` bytes long and unique per `i`.
fn value(i: usize, width: usize) -> Vec<u8> {
    let mut v = format!("value-{i:05}:").into_bytes();
    assert!(v.len() <= width, "value width must fit the prefix");
    v.resize(width, b'x');
    v
}

/// Seed all keys with one `multi_put` per batch.
async fn seed(handle: &Handle, keys: &[Vec<u8>], values: &[Vec<u8>]) {
    for start in (0..keys.len()).step_by(BATCH) {
        let end = (start + BATCH).min(keys.len());
        let entries: Vec<(&[u8], &[u8])> = (start..end)
            .map(|i| (keys[i].as_slice(), values[i].as_slice()))
            .collect();
        handle
            .multi_put(&entries)
            .await
            .expect("seed multi_put batch");
    }
}

/// Read all keys with one `multi_get(Stale)` per batch, verifying every value.
/// Returns the per-batch results so the caller can check index coherence.
async fn read_all(handle: &Handle, keys: &[Vec<u8>], values: &[Vec<u8>]) -> Vec<MultiGetResult> {
    let mut results = Vec::new();
    for start in (0..keys.len()).step_by(BATCH) {
        let end = (start + BATCH).min(keys.len());
        let key_refs: Vec<&[u8]> = (start..end).map(|i| keys[i].as_slice()).collect();
        let result = handle
            .multi_get(&key_refs, ReadConsistency::Stale)
            .await
            .expect("multi_get batch");
        assert_eq!(
            result.values.len(),
            end - start,
            "a batch must return exactly one value per requested key"
        );
        for (offset, got) in result.values.iter().enumerate() {
            let expected = values[start + offset].as_slice();
            assert_eq!(
                got.as_deref(),
                Some(expected),
                "wrong value for key {:?} (batch {start}..{end})",
                keys[start + offset]
            );
        }
        results.push(result);
    }
    results
}

/// No writes happen during the read, so every batch must observe the same
/// applied watermark, and it must equal the node's current applied index.
async fn assert_index_coherent(handle: &Handle, results: &[MultiGetResult]) {
    let first = results
        .first()
        .expect("at least one read batch")
        .applied_index;
    for result in results {
        assert_eq!(
            result.applied_index, first,
            "a read-only workload must observe one applied watermark across batches"
        );
        assert!(result.applied_index >= 1, "the seed writes applied");
    }
    let current = handle
        .multi_get(&[], ReadConsistency::Stale)
        .await
        .expect("empty batch reads the current applied index")
        .applied_index;
    assert_eq!(
        first, current,
        "the batch watermark must equal the node's applied index"
    );
}

/// Default acceptance: 4,096 keys in 8 seed + 8 read commands (≤ 10 reads).
#[tokio::test]
async fn batched_reads_cover_4096_keys_in_at_most_ten_commands() {
    const KEYS: usize = 4096;
    const VALUE_BYTES: usize = 128;

    let (task, handle, metrics, dir) = single_node().await;

    let keys: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    let values: Vec<Vec<u8>> = (0..KEYS).map(|i| value(i, VALUE_BYTES)).collect();

    seed(&handle, &keys, &values).await;

    let before = metrics.multi_get_commands();
    let results = read_all(&handle, &keys, &values).await;
    let delta = metrics.multi_get_commands() - before;

    let expected_calls = KEYS.div_ceil(BATCH) as u64;
    assert!(
        expected_calls <= 10,
        "the default workload must fit the ≤10 read-command budget ({expected_calls})"
    );
    assert_eq!(
        delta, expected_calls,
        "one handled command per batch: no per-key fan-out"
    );
    assert_index_coherent(&handle, &results).await;

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Acceptance (`#[ignore]`, slow): 24,000 keys × ~200 B, ≤ ~100 read commands.
///
/// `cargo test -p arachne-kv --test batch_read_load -- --ignored --nocapture`
#[tokio::test]
#[ignore = "24k-key load acceptance (run with --ignored --nocapture)"]
async fn batched_reads_cover_24000_keys_within_100_commands() {
    const KEYS: usize = 24_000;
    const VALUE_BYTES: usize = 200;
    const MAX_READ_COMMANDS: u64 = 100;

    let (task, handle, metrics, dir) = single_node().await;

    let keys: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    let values: Vec<Vec<u8>> = (0..KEYS).map(|i| value(i, VALUE_BYTES)).collect();

    let started = tokio::time::Instant::now();
    seed(&handle, &keys, &values).await;
    let seeded = started.elapsed();

    let before = metrics.multi_get_commands();
    let results = read_all(&handle, &keys, &values).await;
    let delta = metrics.multi_get_commands() - before;
    let elapsed = started.elapsed();

    let expected_calls = KEYS.div_ceil(BATCH) as u64;
    assert!(
        delta <= MAX_READ_COMMANDS,
        "24k keys must materialize in ≤ {MAX_READ_COMMANDS} read commands, used {delta}"
    );
    assert_eq!(
        delta, expected_calls,
        "one handled command per batch: no per-key fan-out"
    );
    assert_index_coherent(&handle, &results).await;

    println!(
        "batch_read_load 24k: seed={seeded:?}, seed+read={elapsed:?}, \
         read_commands={delta} (batches of {BATCH}, {VALUE_BYTES} B values)"
    );

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
