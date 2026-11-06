//! Integration (Phase 1): `Handle::multi_get` — batched exact-key reads with a
//! single applied index.
//!
//! Covers, over the in-memory transport (no tonic):
//! * `Stale` on a single node: order, duplicates, missing keys, empty batch,
//!   and `applied_index` coherence with `get_stale_range` / `get_stale_with_index`;
//! * `Linearizable` on the leader and follower redirect (in-process peers);
//! * a concurrent writer + `multi_get` reader: every returned value's origin
//!   index is `<=` the batch's `applied_index` (no torn read), and the value
//!   matches its key;
//! * the three distinct `InvalidArgument` over-limit paths (entry count,
//!   per-key size, request bytes) plus the post-read result-byte cap;
//! * `arachne_multi_get_commands_total` increments once per handled command.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arachne_kv::client::Handle;
use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::state_machine::kv::{
    MAX_MULTI_GET_ENTRIES, MAX_MULTI_GET_REQUEST_BYTES, MAX_MULTI_GET_RESULT_BYTES,
};
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::{
    ArachneError, Metrics, MultiGetResult, NodeId, Profile, ProfileConfig, ReadConsistency,
    TransportFactory,
};
use arachne_kv_testsupport::InMemoryTransportFactory;
use slog::{o, Drain, Logger};

static DIR: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let n = DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-multiget-{tag}-{}-{n}",
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

fn addresses(n: u64) -> HashMap<NodeId, std::net::SocketAddr> {
    (1..=n)
        .map(|i| (node_id(i), format!("127.0.0.1:700{i}").parse().expect("addr")))
        .collect()
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
    std::path::PathBuf,
) {
    let dir = temp_dir("single");
    let wal = WalStorage::open(
        &dir,
        WalOptions {
            cluster_id: "mg".into(),
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

/// Poll a transient "not ready yet" operation until it resolves.
async fn until_multi_get(
    handle: &Handle,
    keys: &[&[u8]],
    consistency: ReadConsistency,
) -> Result<MultiGetResult, ArachneError> {
    for _ in 0..600 {
        match handle.multi_get(keys, consistency).await {
            Ok(result) => return Ok(result),
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

/// `Stale` on a single node: order is preserved, duplicate keys repeat,
/// missing keys are `None`, an empty batch is legal, and the batch index is
/// coherent with the node's other consistent stale reads.
#[tokio::test]
async fn stale_multi_get_order_duplicates_missing_empty() {
    let (task, handle, _metrics, dir) = single_node().await;

    handle
        .multi_put(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")])
        .await
        .expect("multi_put");

    let result = handle
        .multi_get(&[b"b", b"missing", b"a", b"b"], ReadConsistency::Stale)
        .await
        .expect("multi_get");
    assert_eq!(
        result.values,
        vec![
            Some(b"2".to_vec()),
            None,
            Some(b"1".to_vec()),
            Some(b"2".to_vec()),
        ],
        "input order preserved; missing=None; duplicates repeat"
    );
    assert!(result.applied_index >= 1, "batch index must be populated");

    // The applied index is the node's current applied watermark: no writes
    // happened since, so the consistent range read observes the same value.
    let (_, range_applied, _) = handle
        .get_stale_range(b"", b"", 100)
        .await
        .expect("range read");
    assert_eq!(
        result.applied_index, range_applied,
        "multi_get's applied index must equal the node's applied watermark"
    );

    // Every returned value's origin is at or below the batch index.
    for (key, value) in [(b"a".as_slice(), 1u64), (b"b".as_slice(), 2), (b"c".as_slice(), 3)] {
        let (_, origin) = handle
            .get_stale_with_index(key)
            .await
            .expect("origin read")
            .expect("present");
        assert!(origin <= result.applied_index);
        assert!(value >= 1);
    }

    // Empty key set: empty values + the same applied watermark.
    let empty = handle
        .multi_get(&[], ReadConsistency::Stale)
        .await
        .expect("empty multi_get is legal");
    assert!(empty.values.is_empty());
    assert_eq!(empty.applied_index, result.applied_index);

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// `Linearizable` on the leader returns the same values as a linear `get`.
#[tokio::test]
async fn linearizable_multi_get_on_leader() {
    let (task, handle, _metrics, dir) = single_node().await;

    handle.put(b"x", b"ex").await.expect("put");
    handle.put(b"y", b"why").await.expect("put");

    let result = until_multi_get(&handle, &[b"y", b"x", b"z"], ReadConsistency::Linearizable)
        .await
        .expect("linearizable multi_get");
    assert_eq!(
        result.values,
        vec![Some(b"why".to_vec()), Some(b"ex".to_vec()), None]
    );
    assert_eq!(
        handle.get(b"x").await.expect("linear get"),
        Some(b"ex".to_vec()),
        "linearizable multi_get agrees with the linear point read"
    );

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- 3-node: follower redirects a Linearizable multi_get to the leader -------

struct Cluster {
    handles: Vec<Handle>,
    metrics: Vec<Arc<Metrics>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    dirs: Vec<std::path::PathBuf>,
}

impl Cluster {
    fn spawn(n: u64) -> Self {
        let profile = profile();
        let factory = InMemoryTransportFactory::new();
        let addrs = addresses(n);
        let metrics: Vec<Arc<Metrics>> = (0..n).map(|_| Arc::new(Metrics::new())).collect();
        let mut handles = Vec::new();
        let mut tasks = Vec::new();
        let mut dirs = Vec::new();

        for i in 1..=n {
            let dir = temp_dir(&format!("n{i}"));
            let wal = WalStorage::open(
                &dir,
                WalOptions {
                    cluster_id: "mg".into(),
                    node_id: format!("n{i}"),
                    config: WalConfig {
                        fsync_policy: FsyncPolicy::Always,
                        segment_bytes: WalConfig::default().segment_bytes,
                    },
                    created_at_millis: 0,
                    fsync_observer: None,
                },
            )
            .expect("open wal");
            let me = node_id(i);
            let (tx, rx) = factory.create(me.clone());
            let config = RuntimeConfig {
                self_raft_id: i,
                self_node_id: me,
                peers: peers_of(i, n),
                addresses: addrs.clone(),
                raft: RaftNodeConfig::from_profile(&profile),
                profile: profile.clone(),
                metrics: Arc::clone(&metrics[(i - 1) as usize]),
            };
            let (runtime, handle) =
                Runtime::new(config, wal, tx, rx, &logger()).expect("build runtime");
            tasks.push(tokio::spawn(runtime.run()));
            handles.push(handle);
            dirs.push(dir);
        }
        for i in 0..n as usize {
            for j in 0..n as usize {
                if i != j {
                    handles[i].register_peer(handles[j].clone());
                }
            }
        }
        Self {
            handles,
            metrics,
            tasks,
            dirs,
        }
    }

    async fn leader(&self) -> usize {
        for _ in 0..800 {
            let ids: Vec<u64> = self.metrics.iter().map(|m| m.leader_id()).collect();
            if ids[0] != 0 && ids.iter().all(|i| *i == ids[0]) {
                return (ids[0] - 1) as usize;
            }
            tokio::time::sleep(core::time::Duration::from_millis(5)).await;
        }
        panic!("the cluster never agreed on a leader");
    }

    async fn wait_value(&self, handle: &Handle, key: &[u8], expected: &[u8]) {
        for _ in 0..600 {
            if handle.get_stale(key).await.expect("stale read").as_deref() == Some(expected) {
                return;
            }
            tokio::time::sleep(core::time::Duration::from_millis(5)).await;
        }
        panic!("node never served {key:?}={expected:?}");
    }

    async fn shutdown(self) {
        for task in &self.tasks {
            task.abort();
        }
        for task in self.tasks {
            let _ = task.await;
        }
        for dir in &self.dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// A follower's `Linearizable` `multi_get` follows the leader hint to the
/// in-process leader and returns the leader's values; every node converges to
/// the same values via its own `Stale` reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_linearizable_multi_get_redirects_to_leader() {
    let cluster = Cluster::spawn(3);
    let leader = cluster.leader().await;
    let follower = (0..3usize).find(|i| *i != leader).expect("follower");

    cluster.handles[leader]
        .multi_put(&[(b"a", b"1"), (b"b", b"2")])
        .await
        .expect("multi_put on leader");
    for h in &cluster.handles {
        cluster.wait_value(h, b"a", b"1").await;
        cluster.wait_value(h, b"b", b"2").await;
    }

    let result = until_multi_get(
        &cluster.handles[follower],
        &[b"b", b"a", b"missing"],
        ReadConsistency::Linearizable,
    )
    .await
    .expect("follower linearizable multi_get must redirect and succeed");
    assert_eq!(
        result.values,
        vec![Some(b"2".to_vec()), Some(b"1".to_vec()), None]
    );

    // Stale from the same follower sees the same replicated state.
    let stale = cluster.handles[follower]
        .multi_get(&[b"a", b"b"], ReadConsistency::Stale)
        .await
        .expect("follower stale multi_get");
    assert_eq!(stale.values, vec![Some(b"1".to_vec()), Some(b"2".to_vec())]);

    cluster.shutdown().await;
}

// ---- concurrent writer + multi_get: no torn batch --------------------------

/// While a writer keeps replacing keys, every `multi_get` batch must satisfy:
/// each present value's **origin index** (recorded when the write completed) is
/// `<=` the batch's `applied_index`, and each value names its own key. This is
/// the single-read-lock + version-first invariant under concurrency.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writer_never_tears_a_multi_get_batch() {
    let (task, handle, _metrics, dir) = single_node().await;

    // Seed eight keys so the reader always has present values. Seed values obey
    // the same `<key>:` shape as the writer's values.
    let keys: Vec<String> = (0..8).map(|i| format!("k{i}")).collect();
    for key in &keys {
        handle
            .put(key.as_bytes(), format!("{key}:seed").as_bytes())
            .await
            .expect("seed put");
    }

    // value bytes -> the log index that first wrote that exact value.
    let origins: Arc<Mutex<HashMap<Vec<u8>, u64>>> = Arc::new(Mutex::new(HashMap::new()));

    let writer = {
        let handle = handle.clone();
        let keys = keys.clone();
        let origins = Arc::clone(&origins);
        tokio::spawn(async move {
            for seq in 0..160u64 {
                let key = keys[(seq as usize) % keys.len()].clone();
                // The value names its key, so the reader can detect a mismatch
                // (a torn batch would pair a key with another key's value).
                let value = format!("{key}:{seq}").into_bytes();
                handle.put(key.as_bytes(), &value).await.expect("writer put");
                if let Some((_, origin)) = handle
                    .get_stale_with_index(key.as_bytes())
                    .await
                    .expect("origin read")
                {
                    // Only record the mapping if the key still holds this exact
                    // value (a racing writer may have replaced it already).
                    if handle
                        .get_stale(key.as_bytes())
                        .await
                        .expect("value read")
                        .as_deref()
                        == Some(value.as_slice())
                    {
                        origins.lock().expect("origins lock").insert(value, origin);
                    }
                }
            }
        })
    };

    let reader = {
        let handle = handle.clone();
        let keys = keys.clone();
        let origins = Arc::clone(&origins);
        tokio::spawn(async move {
            let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
            let mut checked = 0usize;
            for _ in 0..400 {
                let result = handle
                    .multi_get(&key_refs, ReadConsistency::Stale)
                    .await
                    .expect("reader multi_get");
                for (key, value) in keys.iter().zip(result.values.iter()) {
                    let Some(value) = value else { continue };
                    let text = String::from_utf8(value.clone()).expect("utf8 value");
                    assert!(
                        text.starts_with(&format!("{key}:")),
                        "value {text:?} does not belong to key {key:?}"
                    );
                    let known = origins.lock().expect("origins lock").get(value).copied();
                    if let Some(origin) = known {
                        assert!(
                            origin <= result.applied_index,
                            "value at origin {origin} is newer than the batch's applied index {}",
                            result.applied_index
                        );
                        checked += 1;
                    }
                }
            }
            checked
        })
    };

    writer.await.expect("writer task");
    let checked = reader.await.expect("reader task");
    assert!(
        checked > 0,
        "the reader must have observed at least one known value"
    );

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- over-limit: three distinct InvalidArgument paths + result cap ----------

#[tokio::test]
async fn multi_get_rejects_each_over_limit_before_dispatch() {
    let (task, handle, _metrics, dir) = single_node().await;

    // 1) Entry count.
    let many: Vec<Vec<u8>> = (0..=MAX_MULTI_GET_ENTRIES)
        .map(|i| format!("k{i}").into_bytes())
        .collect();
    let many_refs: Vec<&[u8]> = many.iter().map(|k| k.as_slice()).collect();
    let err = handle
        .multi_get(&many_refs, ReadConsistency::Stale)
        .await
        .expect_err("entry count over the cap");
    match err {
        ArachneError::InvalidArgument(message) => {
            assert!(
                message.contains("MAX_MULTI_GET_ENTRIES"),
                "entry-count message must name its bound: {message}"
            );
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }

    // 2) Per-key size (> max_key_bytes = 4 KiB on Profile::Lan).
    let oversized = vec![0xAAu8; 4097];
    let err = handle
        .multi_get(&[oversized.as_slice()], ReadConsistency::Stale)
        .await
        .expect_err("oversized key");
    match err {
        ArachneError::InvalidArgument(message) => {
            assert!(
                message.contains("max_key_bytes"),
                "per-key message must name max_key_bytes: {message}"
            );
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }

    // 3) Total request key bytes: 2049 keys × 2048 B ≈ 4.2 MiB.
    let big: Vec<Vec<u8>> = (0..2049).map(|_| vec![0x11u8; 2048]).collect();
    let big_refs: Vec<&[u8]> = big.iter().map(|k| k.as_slice()).collect();
    let err = handle
        .multi_get(&big_refs, ReadConsistency::Stale)
        .await
        .expect_err("request bytes over the cap");
    match err {
        ArachneError::InvalidArgument(message) => {
            assert!(
                message.contains("MAX_MULTI_GET_REQUEST_BYTES"),
                "request-bytes message must name its bound: {message}"
            );
            assert!(
                (big.len() as u64) * 2048 > MAX_MULTI_GET_REQUEST_BYTES,
                "the test batch must actually exceed the request cap"
            );
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }

    // 4) Post-read result cap: five 1 MiB values (> 4 MiB total). Each write is
    //    within max_value_bytes and the multi_put cap, but the read is over.
    let value = vec![0x5Au8; 1024 * 1024];
    for i in 0..5u64 {
        handle
            .put(format!("v{i}").as_bytes(), &value)
            .await
            .expect("large put");
    }
    assert!(5 * value.len() as u64 > MAX_MULTI_GET_RESULT_BYTES);
    let result_keys: Vec<Vec<u8>> = (0..5u64).map(|i| format!("v{i}").into_bytes()).collect();
    let result_refs: Vec<&[u8]> = result_keys.iter().map(|k| k.as_slice()).collect();
    let err = handle
        .multi_get(&result_refs, ReadConsistency::Stale)
        .await
        .expect_err("result bytes over the cap");
    match err {
        ArachneError::InvalidArgument(message) => {
            assert!(
                message.contains("split the batch"),
                "result-cap message must tell the caller to split: {message}"
            );
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- Gate-1 Q7: independent command counter --------------------------------

/// `arachne_multi_get_commands_total` counts handled commands, not the caller's
/// loop count or the number of keys. N calls (different batch sizes) advance it
/// by exactly N.
#[tokio::test]
async fn multi_get_command_counter_increments_per_command() {
    let (task, handle, metrics, dir) = single_node().await;

    handle
        .multi_put(&[(b"a", b"1"), (b"b", b"2")])
        .await
        .expect("multi_put");

    let before = metrics.multi_get_commands();
    for keys in [
        vec![b"a".as_slice()],
        vec![b"a".as_slice(), b"b".as_slice()],
        vec![],
        vec![b"missing".as_slice()],
        vec![b"a".as_slice(), b"missing".as_slice()],
    ] {
        handle
            .multi_get(&keys, ReadConsistency::Stale)
            .await
            .expect("multi_get");
    }
    let after = metrics.multi_get_commands();
    assert_eq!(
        after,
        before + 5,
        "the counter must advance once per handled command (before={before}, after={after})"
    );

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
