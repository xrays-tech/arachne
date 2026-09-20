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

use arachne::client::Handle;
use arachne::consensus::RaftNodeConfig;
use arachne::runtime::{Runtime, RuntimeConfig};
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{ArachneError, Metrics, NodeId, Profile, ProfileConfig, TransportFactory};
use arachne_transport_tonic::TonicTransportFactory;
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
