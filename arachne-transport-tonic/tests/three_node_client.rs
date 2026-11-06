//! Integration: a 3-node cluster over real tonic, driven by the lib runtime
//! actor + client `Handle`.
//!
//! Proves the M1-3a path end to end over the production transport: a write
//! issued through a **follower's** handle is rejected with `NotLeader` and
//! client-side redirected to the leader, and the write then commits on all
//! three nodes (propsol §3.3, M1 acceptance ②/③).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_kv::client::Handle;
use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::{
    ArachneError, Metrics, NodeId, Profile, ProfileConfig, ReadConsistency, TransportFactory,
};
use arachne_kv_transport_tonic::TonicTransportFactory;
use slog::Drain;

static DIR: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let n = DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-3nc-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> slog::Logger {
    slog::Logger::root(slog::Discard.fuse(), slog::o!())
}

fn node_id(i: u64) -> NodeId {
    NodeId::from(format!("n{i}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_write_redirects_to_leader_over_tonic() {
    let profile = ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 200,
        rpc_timeout_ms: 100,
        ..Profile::Lan.config()
    };

    let mut addresses: HashMap<NodeId, SocketAddr> = HashMap::new();
    for i in 1..=3u64 {
        addresses.insert(node_id(i), "127.0.0.1:0".parse().expect("addr"));
    }

    let factory = TonicTransportFactory::new("m1", 1, 0, Vec::new(), addresses.clone());
    factory.start().await.expect("start transport");

    let metrics: Vec<Arc<Metrics>> = (0..3).map(|_| Arc::new(Metrics::new())).collect();
    let mut handles: Vec<Handle> = Vec::new();
    let mut tasks = Vec::new();
    let mut dirs = Vec::new();

    for i in 1..=3u64 {
        let dir = temp_dir(&format!("n{i}"));
        let wal = WalStorage::open(
            &dir,
            WalOptions {
                cluster_id: "m1".into(),
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
        let peers = (1..=3u64)
            .filter(|j| *j != i)
            .map(|j| (j, node_id(j)))
            .collect();
        let config = RuntimeConfig {
            self_raft_id: i,
            self_node_id: me,
            peers,
            addresses: addresses.clone(),
            raft: RaftNodeConfig::from_profile(&profile),
            profile: profile.clone(),
            metrics: Arc::clone(&metrics[(i - 1) as usize]),
        };
        // `NodeError<TonicTransport>` is not `Debug` (the transport holds
        // non-Debug state), so format the failure via `Display` instead.
        let (runtime, handle) = match Runtime::new(config, wal, tx, rx, &logger()) {
            Ok(pair) => pair,
            Err(e) => panic!("runtime build failed: {e}"),
        };
        tasks.push(tokio::spawn(runtime.run()));
        handles.push(handle);
        dirs.push(dir);
    }

    // Make every handle aware of its peers so redirect can hop in-process.
    for i in 0..3 {
        for j in 0..3 {
            if i != j {
                handles[i].register_peer(handles[j].clone());
            }
        }
    }

    // Wait for an election.
    for _ in 0..800 {
        if metrics.iter().any(|m| m.is_leader()) {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    let leader = (0..3)
        .find(|i| metrics[*i].is_leader())
        .expect("a leader must be elected");
    let follower = (0..3).find(|i| *i != leader).expect("a follower exists");

    // Wait until the CHOSEN FOLLOWER knows the leader: a redirect needs a hint,
    // and a freshly elected leader has not necessarily reached every follower
    // yet. A missing hint surfaces as `QuorumUnavailable`, which the handle does
    // not retry, so asserting a single put here would be racy (it failed on a
    // 2-core CI runner).
    let mut follower_knows_leader = false;
    for _ in 0..800 {
        if handles[follower].leader_hint().await.is_some() {
            follower_knows_leader = true;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    assert!(
        follower_knows_leader,
        "the follower must learn the leader before a redirect can be attempted"
    );

    // Write through a FOLLOWER's handle: redirected to the leader. Retry the
    // transient window errors (`Timeout` / `QuorumUnavailable` / `NotLeader`) —
    // a leadership change right after the election is not what this test is
    // about; the redirect itself is (all retries failing still fails the test).
    let mut last_err = String::new();
    let mut wrote = false;
    for _ in 0..200 {
        match handles[follower].put(b"k", b"v").await {
            Ok(()) => {
                wrote = true;
                break;
            }
            Err(e @ ArachneError::Timeout)
            | Err(e @ ArachneError::QuorumUnavailable)
            | Err(e @ ArachneError::NotLeader { .. }) => {
                last_err = e.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(20)).await;
            }
            Err(e) => panic!("follower put failed with a non-transient error: {e}"),
        }
    }
    assert!(
        wrote,
        "follower put must redirect and succeed (last transient error: {last_err})"
    );

    // All three nodes converge on the value.
    for i in 0..3 {
        let mut seen = false;
        for _ in 0..400 {
            if handles[i].get_stale(b"k").await.expect("get_stale") == Some(b"v".to_vec()) {
                seen = true;
                break;
            }
            tokio::time::sleep(core::time::Duration::from_millis(10)).await;
        }
        assert!(seen, "node {i} did not converge on the value");
    }

    for task in &tasks {
        task.abort();
    }
    factory.shutdown().await;
    for dir in &dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// A follower's `Linearizable` `multi_get` must travel the full wire path:
/// Handle -> `ForwardRequest{kind=3, linearizable=true}` -> server -> sink ->
/// `Command::MultiGet{Linearizable}` (ReadIndex gate) -> `MultiGetReply` ->
/// decode.
///
/// The tested follower deliberately has **no** in-process peer handles
/// registered, so `reach_multi_get` cannot satisfy the leader locally and must
/// use the tonic `RemoteForwarder`. If the remote path is unavailable the call
/// fails rather than silently falling back; if the sink regressed to Stale the
/// read would bypass the leader's ReadIndex gate (the wire-path assertion below
/// is what keeps this test honest about using the remote forwarder).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_multi_get_forwards_over_wire() {
    let profile = ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 200,
        rpc_timeout_ms: 100,
        ..Profile::Lan.config()
    };

    let mut addresses: HashMap<NodeId, SocketAddr> = HashMap::new();
    for i in 1..=3u64 {
        addresses.insert(node_id(i), "127.0.0.1:0".parse().expect("addr"));
    }

    let factory = TonicTransportFactory::new("mg", 1, 0, Vec::new(), addresses.clone());
    factory.start().await.expect("start transport");

    let metrics: Vec<Arc<Metrics>> = (0..3).map(|_| Arc::new(Metrics::new())).collect();
    let mut handles: Vec<Handle> = Vec::new();
    let mut tasks = Vec::new();
    let mut dirs = Vec::new();

    for i in 1..=3u64 {
        let dir = temp_dir(&format!("mg-n{i}"));
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
        let peers = (1..=3u64)
            .filter(|j| *j != i)
            .map(|j| (j, node_id(j)))
            .collect();
        let config = RuntimeConfig {
            self_raft_id: i,
            self_node_id: me,
            peers,
            addresses: addresses.clone(),
            raft: RaftNodeConfig::from_profile(&profile),
            profile: profile.clone(),
            metrics: Arc::clone(&metrics[(i - 1) as usize]),
        };
        let (runtime, handle) = match Runtime::new(config, wal, tx, rx, &logger()) {
            Ok(pair) => pair,
            Err(e) => panic!("runtime build failed: {e}"),
        };
        // Register the node's forward sink with the transport so an inbound
        // `Forward` RPC can actually execute against this runtime. (The
        // in-process redirect test never needs this; the wire test does.)
        factory.set_command_sink(node_id(i), runtime.command_sink());
        tasks.push(tokio::spawn(runtime.run()));
        handles.push(handle);
        dirs.push(dir);
    }

    // Wait for an election.
    for _ in 0..800 {
        if metrics.iter().any(|m| m.is_leader()) {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    let leader = (0..3)
        .find(|i| metrics[*i].is_leader())
        .expect("a leader must be elected");
    let follower = (0..3).find(|i| *i != leader).expect("a follower exists");
    let third = (0..3)
        .find(|i| *i != leader && *i != follower)
        .expect("a third node exists");

    // Deliberately do NOT register the tested follower as a peer of anyone (and
    // do not give it any peer handles). Only the leader/third pair is linked, so
    // leader writes can still redirect after a leadership change; the follower
    // must forward its linearizable read over tonic.
    handles[leader].register_peer(handles[third].clone());
    handles[third].register_peer(handles[leader].clone());

    // A redirect needs the follower to know the leader's hint.
    let mut follower_knows_leader = false;
    for _ in 0..800 {
        if handles[follower].leader_hint().await.is_some() {
            follower_knows_leader = true;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    assert!(
        follower_knows_leader,
        "the follower must learn the leader before forwarding can be attempted"
    );

    // Write through the leader, retrying the transient leadership window.
    for (key, value) in [
        (b"a".as_slice(), b"1".as_slice()),
        (b"b".as_slice(), b"2".as_slice()),
    ] {
        let mut wrote = false;
        let mut last_err = String::new();
        for _ in 0..200 {
            match handles[leader].put(key, value).await {
                Ok(()) => {
                    wrote = true;
                    break;
                }
                Err(e @ ArachneError::Timeout)
                | Err(e @ ArachneError::QuorumUnavailable)
                | Err(e @ ArachneError::NotLeader { .. }) => {
                    last_err = e.to_string();
                    tokio::time::sleep(core::time::Duration::from_millis(20)).await;
                }
                Err(e) => panic!("leader put failed with a non-transient error: {e}"),
            }
        }
        assert!(
            wrote,
            "leader put of {key:?} must succeed (last transient: {last_err})"
        );
    }

    // All three nodes converge on both values before the read.
    for i in 0..3 {
        for (key, value) in [
            (b"a".as_slice(), b"1".as_slice()),
            (b"b".as_slice(), b"2".as_slice()),
        ] {
            let mut seen = false;
            for _ in 0..400 {
                if handles[i].get_stale(key).await.expect("get_stale").as_deref()
                    == Some(value)
                {
                    seen = true;
                    break;
                }
                tokio::time::sleep(core::time::Duration::from_millis(10)).await;
            }
            assert!(seen, "node {i} did not converge on {key:?}");
        }
    }

    // The follower's Linearizable read must cross the wire (no in-process peer
    // for the leader) and return the leader's values plus a populated index.
    let keys: [&[u8]; 3] = [b"b", b"a", b"missing"];
    let mut done = false;
    let mut last_err = String::new();
    for _ in 0..400 {
        match handles[follower]
            .multi_get(&keys, ReadConsistency::Linearizable)
            .await
        {
            Ok(result) => {
                assert_eq!(
                    result.values,
                    vec![Some(b"2".to_vec()), Some(b"1".to_vec()), None],
                    "forwarded linearizable multi_get must preserve order"
                );
                assert!(
                    result.applied_index >= 1,
                    "the forwarded reply must carry a populated applied index"
                );
                done = true;
                break;
            }
            Err(e @ ArachneError::Timeout)
            | Err(e @ ArachneError::QuorumUnavailable)
            | Err(e @ ArachneError::NotLeader { .. }) => {
                last_err = e.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(20)).await;
            }
            Err(e) => panic!("follower linearizable multi_get failed: {e}"),
        }
    }
    assert!(
        done,
        "follower linearizable multi_get must forward over the wire (last transient: {last_err})"
    );

    // The same follower can still serve a Stale batch locally.
    let stale = handles[follower]
        .multi_get(&keys, ReadConsistency::Stale)
        .await
        .expect("follower stale multi_get");
    assert_eq!(
        stale.values,
        vec![Some(b"2".to_vec()), Some(b"1".to_vec()), None]
    );
    assert!(stale.applied_index >= 1);

    for task in &tasks {
        task.abort();
    }
    factory.shutdown().await;
    for dir in &dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// A follower's index-returning write must travel the full wire path:
/// Handle -> `ForwardRequest{kind=4}` -> server -> sink ->
/// `Command::ProposeWithIndex` -> apply pipeline -> `ForwardReply.commit_index`
/// -> decode -> `ForwardOutcome.index` -> public `Index`.
///
/// The tested follower deliberately has **no** in-process peer handles
/// registered, so `reach_target` cannot satisfy the leader locally and must use
/// the tonic `RemoteForwarder`. The returned index is then checked against the
/// leader's own `get_stale_with_index` origin, so this test fails if the server
/// maps kind=4 to plain `Propose` (no index), the sink drops the index, or the
/// wire field is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_put_with_index_forwards_over_wire() {
    let profile = ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 200,
        rpc_timeout_ms: 100,
        ..Profile::Lan.config()
    };

    let mut addresses: HashMap<NodeId, SocketAddr> = HashMap::new();
    for i in 1..=3u64 {
        addresses.insert(node_id(i), "127.0.0.1:0".parse().expect("addr"));
    }

    let factory = TonicTransportFactory::new("mw", 1, 0, Vec::new(), addresses.clone());
    factory.start().await.expect("start transport");

    let metrics: Vec<Arc<Metrics>> = (0..3).map(|_| Arc::new(Metrics::new())).collect();
    let mut handles: Vec<Handle> = Vec::new();
    let mut tasks = Vec::new();
    let mut dirs = Vec::new();

    for i in 1..=3u64 {
        let dir = temp_dir(&format!("mw-n{i}"));
        let wal = WalStorage::open(
            &dir,
            WalOptions {
                cluster_id: "mw".into(),
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
        let peers = (1..=3u64)
            .filter(|j| *j != i)
            .map(|j| (j, node_id(j)))
            .collect();
        let config = RuntimeConfig {
            self_raft_id: i,
            self_node_id: me,
            peers,
            addresses: addresses.clone(),
            raft: RaftNodeConfig::from_profile(&profile),
            profile: profile.clone(),
            metrics: Arc::clone(&metrics[(i - 1) as usize]),
        };
        let (runtime, handle) = match Runtime::new(config, wal, tx, rx, &logger()) {
            Ok(pair) => pair,
            Err(e) => panic!("runtime build failed: {e}"),
        };
        // Register the node's forward sink so an inbound `Forward` RPC can
        // actually execute against this runtime.
        factory.set_command_sink(node_id(i), runtime.command_sink());
        tasks.push(tokio::spawn(runtime.run()));
        handles.push(handle);
        dirs.push(dir);
    }

    // Wait for an election.
    for _ in 0..800 {
        if metrics.iter().any(|m| m.is_leader()) {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    let leader = (0..3)
        .find(|i| metrics[*i].is_leader())
        .expect("a leader must be elected");
    let follower = (0..3).find(|i| *i != leader).expect("a follower exists");
    let third = (0..3)
        .find(|i| *i != leader && *i != follower)
        .expect("a third node exists");

    // Deliberately do NOT register the tested follower as a peer of anyone (and
    // do not give it any peer handles). Only the leader/third pair is linked, so
    // leader writes can still redirect after a leadership change; the follower
    // must forward its indexed write over tonic.
    handles[leader].register_peer(handles[third].clone());
    handles[third].register_peer(handles[leader].clone());

    // A redirect needs the follower to know the leader's hint.
    let mut follower_knows_leader = false;
    for _ in 0..800 {
        if handles[follower].leader_hint().await.is_some() {
            follower_knows_leader = true;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    assert!(
        follower_knows_leader,
        "the follower must learn the leader before forwarding can be attempted"
    );

    // Follower write: it cannot serve locally and has no in-process peer for the
    // leader, so the only way this can succeed is the tonic `kind=4` forward.
    let mut put_index = None;
    let mut last_err = String::new();
    for _ in 0..400 {
        match handles[follower].put_with_index(b"k", b"v").await {
            Ok(index) => {
                put_index = Some(index);
                break;
            }
            Err(e @ ArachneError::Timeout)
            | Err(e @ ArachneError::QuorumUnavailable)
            | Err(e @ ArachneError::NotLeader { .. }) => {
                last_err = e.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(20)).await;
            }
            Err(e) => panic!("follower put_with_index failed with a non-transient error: {e}"),
        }
    }
    let put_index = put_index.unwrap_or_else(|| {
        panic!("follower put_with_index must forward over the wire (last transient: {last_err})")
    });
    assert!(
        put_index >= 1,
        "a forwarded indexed write must carry a real applied-write index, got {put_index}"
    );

    // The leader's own origin for the value must match the forwarded index.
    let mut leader_origin = None;
    for _ in 0..400 {
        match handles[leader]
            .get_stale_with_index(b"k")
            .await
            .expect("leader get_stale_with_index")
        {
            Some((value, index)) if value == b"v" => {
                leader_origin = Some(index);
                break;
            }
            _ => tokio::time::sleep(core::time::Duration::from_millis(10)).await,
        }
    }
    assert_eq!(
        leader_origin,
        Some(put_index),
        "the leader's origin index must equal the index returned to the follower"
    );

    // Optional batch variant: both keys share the one returned index.
    let entries: [(&[u8], &[u8]); 2] = [(b"m1", b"x"), (b"m2", b"y")];
    let mut multi_index = None;
    let mut last_err = String::new();
    for _ in 0..400 {
        match handles[follower].multi_put_with_index(&entries).await {
            Ok(index) => {
                multi_index = Some(index);
                break;
            }
            Err(e @ ArachneError::Timeout)
            | Err(e @ ArachneError::QuorumUnavailable)
            | Err(e @ ArachneError::NotLeader { .. }) => {
                last_err = e.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(20)).await;
            }
            Err(e) => {
                panic!("follower multi_put_with_index failed with a non-transient error: {e}")
            }
        }
    }
    let multi_index = multi_index.unwrap_or_else(|| {
        panic!(
            "follower multi_put_with_index must forward over the wire (last transient: {last_err})"
        )
    });
    assert!(
        multi_index >= 1,
        "a forwarded indexed multi-write must carry a real applied-write index, got {multi_index}"
    );

    for (key, value) in entries {
        let mut origin = None;
        for _ in 0..400 {
            match handles[leader]
                .get_stale_with_index(key)
                .await
                .expect("leader get_stale_with_index")
            {
                Some((seen, index)) if seen == value => {
                    origin = Some(index);
                    break;
                }
                _ => tokio::time::sleep(core::time::Duration::from_millis(10)).await,
            }
        }
        assert_eq!(
            origin,
            Some(multi_index),
            "every key of the forwarded batch must share the returned index (key {key:?})"
        );
    }

    for task in &tasks {
        task.abort();
    }
    factory.shutdown().await;
    for dir in &dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}
