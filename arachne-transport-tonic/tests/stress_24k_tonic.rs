//! Real-tonic 3-node, 24k-key materializer stress test.
//!
//! This is the integration-scale companion to the single-node
//! `batch_read_load` 24k acceptance and the real-tonic `three_node_client`
//! forward/redirect tests. It proves the gated materializer pattern end to end
//! over actual loopback gRPC:
//!
//! * three real `TonicTransportFactory` nodes (no in-process peer handles);
//! * 24,000 `entity/<i>` keys (47 `multi_put` batches of 512, ~200 B values);
//! * a `head` generation token written *after* the entities;
//! * 2 concurrent materializers per live node, each round doing
//!   `get_stale_with_index(head)` → local `wait_applied(head_index, 2s)` →
//!   47 stale `multi_get` batches, verifying every value;
//! * a leader kill after round 1, a bounded re-election, a new head generation
//!   written through a *follower* (so the real tonic `Forward` path is used,
//!   since no peer handles are registered), and rounds 2–3 on the two
//!   survivors only.
//!
//! The key non-functional assertion is the command budget: one materializer
//! reads 24,000 values in 47 handled commands (≤ 50), not 24,000.
//!
//! Run explicitly:
//! `cargo test -p arachne-kv-transport-tonic --test stress_24k_tonic -- --ignored --nocapture`
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids real-time imports there), so this file uses
//! `core::time::Duration` and `tokio::time::Instant`, never `std::time`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arachne_kv::client::Handle;
use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::{
    ArachneError, Metrics, NodeId, Profile, ProfileConfig, ReadConsistency, TransportFactory,
};
use arachne_kv_transport_tonic::TonicTransportFactory;
use slog::{Drain, Logger, o};
use tokio::sync::Mutex;

const NODES: usize = 3;
const KEY_COUNT: usize = 24_000;
const VALUE_BYTES: usize = 200;
const BATCH: usize = 512;
const MAX_MATERIALIZE_COMMANDS: u64 = 50;
const HEAD_KEY: &[u8] = b"head";

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-stress24k-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn node_id(i: usize) -> NodeId {
    NodeId::from(format!("n{}", i + 1))
}

fn profile() -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 200,
        rpc_timeout_ms: 100,
        ..Profile::Lan.config()
    }
}

fn entity_key(i: usize) -> Vec<u8> {
    format!("entity/{i:05}").into_bytes()
}

/// A value exactly `VALUE_BYTES` long and unique per `i`.
fn entity_value(i: usize) -> Vec<u8> {
    let mut value = format!("value-{i:05}:").into_bytes();
    assert!(
        value.len() <= VALUE_BYTES,
        "the value prefix must fit the requested width"
    );
    value.resize(VALUE_BYTES, b'x');
    value
}

fn head_value(generation: u64) -> Vec<u8> {
    format!("gen-{generation:010}").into_bytes()
}

fn is_transient(error: &ArachneError) -> bool {
    matches!(
        error,
        ArachneError::Timeout | ArachneError::QuorumUnavailable | ArachneError::NotLeader { .. }
    )
}

/// Retry a `multi_put` until the batch commits, absorbing the leadership
/// window errors the cluster can produce while it settles.
async fn until_multi_put(handle: &Handle, entries: &[(&[u8], &[u8])]) {
    let mut last = String::new();
    for _ in 0..600 {
        match handle.multi_put(entries).await {
            Ok(()) => return,
            Err(ref error) if is_transient(error) => {
                last = error.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(5)).await;
            }
            Err(error) => panic!("seed multi_put failed with a non-transient error: {error}"),
        }
    }
    panic!("seed multi_put never committed (last transient: {last})");
}

/// Retry an indexed single-key write until it commits and return its index.
async fn until_put_with_index(handle: &Handle, key: &[u8], value: &[u8]) -> u64 {
    let mut last = String::new();
    for _ in 0..600 {
        match handle.put_with_index(key, value).await {
            Ok(index) => return index,
            Err(ref error) if is_transient(error) => {
                last = error.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(5)).await;
            }
            Err(error) => panic!("put_with_index failed with a non-transient error: {error}"),
        }
    }
    panic!("put_with_index never committed (last transient: {last})");
}

/// Poll a local stale read until `key` has exactly `expected`.
async fn wait_stale_value(handle: &Handle, key: &[u8], expected: &[u8]) {
    for _ in 0..700 {
        match handle.get_stale(key).await {
            Ok(Some(ref value)) if value == expected => return,
            Ok(_) => tokio::time::sleep(core::time::Duration::from_millis(10)).await,
            Err(ref error) if is_transient(error) => {
                tokio::time::sleep(core::time::Duration::from_millis(10)).await
            }
            Err(error) => panic!("get_stale failed with a non-transient error: {error}"),
        }
    }
    panic!(
        "node {} never served key {:?} with the expected value",
        handle.node_id(),
        String::from_utf8_lossy(key)
    );
}

/// Poll a stale read until `key` is present, returning its `(value, origin)`.
async fn wait_stale_with_index(handle: &Handle, key: &[u8]) -> (Vec<u8>, u64) {
    for _ in 0..700 {
        match handle.get_stale_with_index(key).await {
            Ok(Some(pair)) => return pair,
            Ok(None) => tokio::time::sleep(core::time::Duration::from_millis(10)).await,
            Err(ref error) if is_transient(error) => {
                tokio::time::sleep(core::time::Duration::from_millis(10)).await
            }
            Err(error) => panic!("get_stale_with_index failed with a non-transient error: {error}"),
        }
    }
    panic!(
        "node {} never observed key {:?}",
        handle.node_id(),
        String::from_utf8_lossy(key)
    );
}

struct RoundRecord {
    node: usize,
    round: u64,
    task: usize,
    elapsed_ms: u128,
    wait_ms: u128,
    commands: u64,
    applied_index: u64,
}

/// One materializer pass: observe the head, wait locally for it to apply, then
/// materialize all 24k entities with 47 stale multi-get commands.
async fn materialize_round(
    node: usize,
    round: u64,
    task: usize,
    handle: Handle,
    metrics: Arc<Metrics>,
    read_lock: Arc<Mutex<()>>,
    expected_head: Arc<Vec<u8>>,
    keys: Arc<Vec<Vec<u8>>>,
    values: Arc<Vec<Vec<u8>>>,
) -> RoundRecord {
    let started = tokio::time::Instant::now();

    // 1. Read the head token and the index that must be applied locally.
    let (seen_head, head_index) = wait_stale_with_index(&handle, HEAD_KEY).await;
    assert_eq!(
        seen_head,
        *expected_head,
        "node {} round {round} task {task}: stale head token mismatch",
        handle.node_id()
    );
    assert!(head_index > 0, "a present head must have a nonzero origin");

    // 2. Local, quorum-free wait. The head was already read from this node's
    //    applied state, so this must resolve promptly (and never redirect).
    let wait_started = tokio::time::Instant::now();
    let observed = tokio::time::timeout(
        core::time::Duration::from_secs(3),
        handle.wait_applied(head_index, core::time::Duration::from_secs(2)),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "node {} round {round} task {task}: wait_applied hung past its 3 s safety bound",
            handle.node_id()
        )
    })
    .unwrap_or_else(|error| {
        panic!(
            "node {} round {round} task {task}: wait_applied({head_index}) failed: {error}",
            handle.node_id()
        )
    });
    let wait_ms = wait_started.elapsed().as_millis();
    assert!(
        observed >= head_index,
        "node {} round {round}: wait_applied observed {observed} < {head_index}",
        handle.node_id()
    );
    assert!(
        wait_ms <= 2_000,
        "node {} round {round}: wait_applied returned Ok after {wait_ms} ms, over its 2 s budget",
        handle.node_id()
    );

    // 3. Materialize every entity with stale batch reads. The per-node lock
    //    makes this task's `multi_get_commands` delta unambiguous.
    let _read_guard = read_lock.lock().await;
    let commands_before = metrics.multi_get_commands();
    let mut last_applied = 0u64;
    let mut batches = 0u64;
    for start in (0..keys.len()).step_by(BATCH) {
        let end = (start + BATCH).min(keys.len());
        let key_refs: Vec<&[u8]> = (start..end).map(|i| keys[i].as_slice()).collect();
        let result = handle
            .multi_get(&key_refs, ReadConsistency::Stale)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "node {} round {round} task {task}: multi_get batch {start}..{end} failed: \
                     {error}",
                    handle.node_id()
                )
            });
        assert_eq!(
            result.values.len(),
            end - start,
            "node {} round {round}: a batch must return one value per requested key",
            handle.node_id()
        );
        for (offset, got) in result.values.iter().enumerate() {
            assert_eq!(
                got.as_deref(),
                Some(values[start + offset].as_slice()),
                "node {} round {round} task {task}: wrong value for key {:?}",
                handle.node_id(),
                String::from_utf8_lossy(&keys[start + offset])
            );
        }
        assert!(
            result.applied_index >= head_index,
            "node {} round {round}: batch applied index {} is below the observed head {head_index}",
            handle.node_id(),
            result.applied_index
        );
        assert!(
            result.applied_index >= last_applied,
            "node {} round {round}: applied index went backwards ({} < {last_applied})",
            handle.node_id(),
            result.applied_index
        );
        last_applied = result.applied_index;
        batches += 1;
    }
    let commands = metrics.multi_get_commands() - commands_before;
    assert_eq!(
        batches,
        keys.len().div_ceil(BATCH) as u64,
        "the materializer must issue one command per batch"
    );
    assert!(
        commands >= batches,
        "node {} round {round}: {commands} commands for {batches} batches",
        handle.node_id()
    );
    assert!(
        commands <= MAX_MATERIALIZE_COMMANDS,
        "node {} round {round}: materializing 24k keys took {commands} commands (> \
         {MAX_MATERIALIZE_COMMANDS}); the batch path must not fan out per key",
        handle.node_id()
    );

    RoundRecord {
        node,
        round,
        task,
        elapsed_ms: started.elapsed().as_millis(),
        wait_ms,
        commands,
        applied_index: last_applied,
    }
}

/// Run one globally-phased round: two materializers per live node.
async fn run_round(
    round: u64,
    live: &[usize],
    expected_head: &Arc<Vec<u8>>,
    handles: &[Handle],
    metrics: &[Arc<Metrics>],
    read_locks: &[Arc<Mutex<()>>],
    keys: &Arc<Vec<Vec<u8>>>,
    values: &Arc<Vec<Vec<u8>>>,
) -> Vec<RoundRecord> {
    let mut tasks = Vec::new();
    for &node in live {
        for task in 0..2usize {
            tasks.push(tokio::spawn(materialize_round(
                node,
                round,
                task,
                handles[node].clone(),
                Arc::clone(&metrics[node]),
                Arc::clone(&read_locks[node]),
                Arc::clone(expected_head),
                Arc::clone(keys),
                Arc::clone(values),
            )));
        }
    }

    let mut records = Vec::with_capacity(tasks.len());
    for task in tasks {
        records.push(task.await.expect("a materializer task panicked"));
    }

    for record in &records {
        println!(
            "[stress] round={} node=n{} task={} ms={} wait_ms={} commands={} applied_index={}",
            record.round,
            record.node + 1,
            record.task,
            record.elapsed_ms,
            record.wait_ms,
            record.commands,
            record.applied_index,
        );
    }
    records
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "24k-key 3-node real-tonic stress (run with --ignored --nocapture)"]
async fn materializes_24k_keys_across_a_leader_failure_over_real_tonic() {
    let profile = profile();
    let cluster_id = "stress24k";

    let mut addresses: HashMap<NodeId, SocketAddr> = HashMap::new();
    for i in 0..NODES {
        addresses.insert(node_id(i), "127.0.0.1:0".parse().expect("addr"));
    }

    let factory = TonicTransportFactory::new(cluster_id, 1, 0, Vec::new(), addresses.clone());
    factory.start().await.expect("start transport");

    let metrics: Vec<Arc<Metrics>> = (0..NODES).map(|_| Arc::new(Metrics::new())).collect();
    let mut handles: Vec<Handle> = Vec::new();
    let mut tasks = Vec::new();
    let mut dirs = Vec::new();

    for i in 0..NODES {
        let dir = temp_dir(&format!("n{}", i + 1));
        let wal = WalStorage::open(
            &dir,
            WalOptions {
                cluster_id: cluster_id.into(),
                node_id: format!("n{}", i + 1),
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
        let peers = (0..NODES)
            .filter(|j| *j != i)
            .map(|j| ((j + 1) as u64, node_id(j)))
            .collect();
        let config = RuntimeConfig {
            self_raft_id: (i + 1) as u64,
            self_node_id: me.clone(),
            peers,
            addresses: addresses.clone(),
            raft: RaftNodeConfig::from_profile(&profile),
            profile: profile.clone(),
            metrics: Arc::clone(&metrics[i]),
        };
        let (runtime, handle) = match Runtime::new(config, wal, tx, rx, &logger()) {
            Ok(pair) => pair,
            Err(error) => panic!("runtime build failed: {error}"),
        };
        // Inbound `Forward` RPCs on this node execute against its runtime. This
        // is what lets a follower's redirect/write cross the real wire.
        factory.set_command_sink(me.clone(), runtime.command_sink());
        tasks.push(tokio::spawn(runtime.run()));
        handles.push(handle);
        dirs.push(dir);
    }

    // Deliberately no `register_peer`: a follower has no in-process shortcut to
    // the leader, so its forwarded writes/redirects must use the tonic path.
    // Local stale reads and `wait_applied` still go straight to its own actor.

    let keys: Arc<Vec<Vec<u8>>> = Arc::new((0..KEY_COUNT).map(entity_key).collect());
    let values: Arc<Vec<Vec<u8>>> = Arc::new((0..KEY_COUNT).map(entity_value).collect());
    // Per-node stale-read phases are serialized only for clean
    // per-materializer metric attribution; the two tasks per node still run
    // concurrently through the head read / `wait_applied` phase. A `multi_get`
    // counter is per node, so overlapping materializers would otherwise each
    // observe the other's commands and neither could prove the ≤ 50 bound.
    let read_locks: Vec<Arc<Mutex<()>>> = (0..NODES).map(|_| Arc::new(Mutex::new(()))).collect();

    let result = tokio::time::timeout(core::time::Duration::from_secs(170), async {
        // ---- election: a leader plus every follower knowing who it is ------
        let mut leader = None;
        for _ in 0..800 {
            let elected = metrics.iter().position(|m| m.is_leader());
            if let Some(pos) = elected
                && metrics.iter().all(|m| m.leader_id() != 0)
            {
                leader = Some(pos);
                break;
            }
            tokio::time::sleep(core::time::Duration::from_millis(20)).await;
        }
        let leader = leader.expect("the cluster must elect a leader all nodes know");
        println!("[stress] initial leader=n{}", leader + 1);

        // ---- seed 24k entities in 47 multi_put batches ---------------------
        let seed_started = tokio::time::Instant::now();
        for start in (0..KEY_COUNT).step_by(BATCH) {
            let end = (start + BATCH).min(KEY_COUNT);
            let entries: Vec<(&[u8], &[u8])> = (start..end)
                .map(|i| (keys[i].as_slice(), values[i].as_slice()))
                .collect();
            until_multi_put(&handles[leader], &entries).await;
        }
        let seed_elapsed = seed_started.elapsed();
        let max_entity_applied = metrics[leader].applied_index();

        // Every entity is now applied on the leader. A head write afterwards
        // must therefore carry a strictly larger index.
        let (last_entity, last_entity_origin) =
            wait_stale_with_index(&handles[leader], &keys[KEY_COUNT - 1]).await;
        assert_eq!(
            last_entity,
            values[KEY_COUNT - 1],
            "the last seeded entity must be readable on the leader"
        );

        let head_1 = head_value(1);
        let head_1_index = until_put_with_index(&handles[leader], HEAD_KEY, &head_1).await;
        assert!(
            head_1_index > 0,
            "the head generation index must be nonzero"
        );
        assert!(
            head_1_index > last_entity_origin,
            "head index {head_1_index} must be after the last entity index {last_entity_origin}"
        );
        assert!(
            head_1_index > max_entity_applied,
            "head index {head_1_index} must be after the leader applied watermark \
             {max_entity_applied} produced by the entity batches"
        );
        println!(
            "[stress] seeded {KEY_COUNT} entities in {} batches: seed={:?}, \
             last_entity_index={last_entity_origin}, head_index={head_1_index}",
            KEY_COUNT.div_ceil(BATCH),
            seed_elapsed
        );

        // ---- pre-round convergence: head + a spread of entities ------------
        let sample: Vec<usize> = vec![0, 1024, 4096, 8192, 12288, 16384, 20480, KEY_COUNT - 1];
        for node in 0..NODES {
            wait_stale_value(&handles[node], HEAD_KEY, &head_1).await;
            for &i in &sample {
                wait_stale_value(&handles[node], &keys[i], &values[i]).await;
            }
        }
        println!(
            "[stress] all 3 nodes converged on head + {} sampled entities",
            sample.len()
        );

        // ---- round 1: all three nodes, two materializers each --------------
        let live_1: Vec<usize> = (0..NODES).collect();
        let records_1 = run_round(
            1,
            &live_1,
            &Arc::new(head_1.clone()),
            &handles,
            &metrics,
            &read_locks,
            &keys,
            &values,
        )
        .await;
        for record in &records_1 {
            assert_eq!(record.round, 1);
        }

        // ---- fault: kill the leader, re-elect, write a new head ------------
        let killed = leader;
        tasks[killed].abort();
        let alive: Vec<usize> = (0..NODES).filter(|i| *i != killed).collect();
        assert_eq!(
            alive.len(),
            2,
            "exactly two nodes must survive the leader kill"
        );

        let fault_started = tokio::time::Instant::now();
        let mut new_leader = None;
        for _ in 0..250 {
            if let Some(pos) = alive.iter().copied().find(|i| metrics[*i].is_leader()) {
                new_leader = Some(pos);
                break;
            }
            tokio::time::sleep(core::time::Duration::from_millis(20)).await;
        }
        let new_leader =
            new_leader.expect("one of the two surviving nodes must become leader within ~5 s");
        assert_ne!(new_leader, killed, "the aborted node cannot lead again");
        println!(
            "[stress] killed leader=n{}; new leader=n{} after {:?}",
            killed + 1,
            new_leader + 1,
            fault_started.elapsed()
        );

        // The writing node is the *surviving follower*, not the new leader:
        // with no in-process peer handles this is a real tonic `Forward`.
        let writer = alive
            .iter()
            .copied()
            .find(|i| *i != new_leader)
            .expect("the surviving majority has one follower");

        // A redirect needs the follower to have learned the new leader.
        let expected_leader_id = node_id(new_leader);
        let mut hinted = false;
        for _ in 0..250 {
            if let Some((id, _)) = handles[writer].leader_hint().await
                && id == expected_leader_id
            {
                hinted = true;
                break;
            }
            tokio::time::sleep(core::time::Duration::from_millis(20)).await;
        }
        assert!(
            hinted,
            "the surviving follower must learn the new leader before its forwarded write"
        );

        let head_2 = head_value(2);
        let head_2_index = until_put_with_index(&handles[writer], HEAD_KEY, &head_2).await;
        assert!(
            head_2_index > head_1_index,
            "the new head generation must be later"
        );
        println!(
            "[stress] forwarded head generation 2 through follower n{}: index={head_2_index}",
            writer + 1
        );

        // ---- verify the survivors converged before rounds 2–3 --------------
        for &node in &alive {
            wait_stale_value(&handles[node], HEAD_KEY, &head_2).await;
            for &i in &sample {
                wait_stale_value(&handles[node], &keys[i], &values[i]).await;
            }
        }
        assert!(
            !alive.contains(&killed),
            "the killed node must not participate in the post-fault rounds"
        );

        // ---- rounds 2 and 3: survivors only, new head generation -----------
        for round in 2..=3u64 {
            let records = run_round(
                round,
                &alive,
                &Arc::new(head_2.clone()),
                &handles,
                &metrics,
                &read_locks,
                &keys,
                &values,
            )
            .await;
            for record in &records {
                assert_eq!(record.round, round);
                assert_ne!(record.node, killed, "the killed node must be excluded");
            }
        }

        println!(
            "[stress] complete: {KEY_COUNT} entities materialized by 2 tasks/node each round; \
             every round used ≤ {MAX_MATERIALIZE_COMMANDS} read commands"
        );
    })
    .await;

    // Always release the cluster, including on a failed/timed-out run.
    for task in &tasks {
        task.abort();
    }
    tokio::time::sleep(core::time::Duration::from_millis(100)).await;
    factory.shutdown().await;
    for dir in &dirs {
        let _ = std::fs::remove_dir_all(dir);
    }

    result.expect("the 24k stress test must finish within its 170 s bound");
}
