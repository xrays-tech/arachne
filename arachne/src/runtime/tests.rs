//! Runtime actor + `Handle` tests over the in-memory transport (dev-dependency
//! `arachne-testsupport`).
//!
//! * `single_node_put_then_get_stale` — one node elects itself leader; a `put`
//!   commits and is readable via `get_stale`.
//! * `follower_handle_redirects_to_leader` — three nodes over the in-memory
//!   transport elect a leader; a *follower's* `Handle` puts, the client-side
//!   redirect reaches the leader, and all three nodes apply the write.
//!
//! The in-memory transport is pull-based: a message is delivered once the
//! receiver is re-polled. The actor's tick interval re-polls the inbound branch
//! every heartbeat, so messages land within one tick (the test profile uses a
//! 10 ms heartbeat).

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use arachne_testsupport::InMemoryTransportFactory;
use slog::{o, Drain, Logger};
use tokio_util::sync::CancellationToken;

use super::Node;
use crate::client::{ArachneError, Handle};
use crate::profile::{Profile, ProfileConfig};
use crate::storage::{FsyncPolicy, WalConfig, WalOptions};
use crate::NodeId;

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

/// A fast test profile: 10 ms heartbeat, 100 ms election (5x, valid), 50 ms RPC.
fn test_profile() -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 100,
        rpc_timeout_ms: 50,
        ..Profile::Lan.config()
    }
}

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-rt-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn wal_opts(node: &str) -> WalOptions {
    WalOptions {
        cluster_id: "rt".into(),
        node_id: node.into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 1 << 20,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

fn node_id(i: u64) -> NodeId {
    NodeId::from(format!("n{i}"))
}

fn peers_of(self_raft_id: u64, n: u64) -> HashMap<u64, NodeId> {
    (1..=n)
        .filter(|j| *j != self_raft_id)
        .map(|j| (j, node_id(j)))
        .collect()
}

fn addresses(n: u64) -> HashMap<NodeId, SocketAddr> {
    (1..=n)
        .map(|i| {
            (
                node_id(i),
                format!("127.0.0.1:700{i}").parse().expect("addr"),
            )
        })
        .collect()
}

/// Poll `f` (a fresh future each call) until it returns `Ok`, retrying on the
/// "not ready yet" errors (timeout / no quorum / redirect). Bounded so a stuck
/// cluster fails loudly instead of hanging.
async fn until_ok<F, Fut>(mut f: F) -> Result<(), ArachneError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), ArachneError>>,
{
    for _ in 0..300 {
        match f().await {
            Ok(()) => return Ok(()),
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

/// Poll `handle`'s leader hint until a leader is known.
async fn until_leader_hint(handle: &Handle) -> Option<(NodeId, SocketAddr)> {
    for _ in 0..300 {
        if let Some(hint) = handle.leader_hint().await {
            return Some(hint);
        }
        tokio::time::sleep(core::time::Duration::from_millis(2)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_put_then_get_stale() {
    let dir = temp_dir("single");
    let factory = InMemoryTransportFactory::new();
    let (tx, rx) = factory.create(node_id(1));

    let node = Node::new(
        1,
        node_id(1),
        peers_of(1, 1),
        &dir,
        wal_opts("n1"),
        tx,
        rx,
        0,
        &test_profile(),
        addresses(1),
        &logger(),
    )
    .expect("node must assemble");
    let shutdown = CancellationToken::new();
    let handle = node.handle();
    let task = tokio::spawn(async move { node.run(&shutdown).await });

    // The single node elects itself leader, after which the put commits.
    until_ok(|| async { handle.put(b"k", b"v").await })
        .await
        .expect("put on the single node must succeed");
    assert_eq!(
        handle.get_stale(b"k").await.expect("get_stale"),
        Some(b"v".to_vec())
    );

    shutdown.cancel();
    let _ = task.await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_handle_redirects_to_leader() {
    const N: u64 = 3;
    let factory = InMemoryTransportFactory::new();

    let mut handles = Vec::new();
    let mut nodes = Vec::new();
    let mut dirs = Vec::new();
    for i in 1..=N {
        let dir = temp_dir(&format!("n{i}"));
        let (tx, rx) = factory.create(node_id(i));
        let node = Node::new(
            i,
            node_id(i),
            peers_of(i, N),
            &dir,
            wal_opts(&format!("n{i}")),
            tx,
            rx,
            0,
            &test_profile(),
            addresses(N),
            &logger(),
        )
        .expect("node must assemble");
        dirs.push(dir);
        handles.push(node.handle());
        nodes.push(node);
    }

    // Register in-process peers so a follower's handle can redirect to the
    // leader's handle (client-side redirect, propsol §3.3).
    for h in &handles {
        for other in &handles {
            if other.node_id() != h.node_id() {
                h.register_peer(other.clone());
            }
        }
    }

    let mut shutdowns = Vec::new();
    let mut tasks = Vec::new();
    for node in nodes {
        let shutdown = CancellationToken::new();
        let run_token = shutdown.clone();
        tasks.push(tokio::spawn(async move { node.run(&run_token).await }));
        shutdowns.push(shutdown);
    }

    // Wait for a leader to be known.
    let leader = until_leader_hint(&handles[0])
        .await
        .expect("the cluster must elect a leader");
    // Pick a follower handle (not the leader) to exercise the redirect path.
    let follower = handles
        .iter()
        .find(|h| *h.node_id() != leader.0)
        .expect("there must be a non-leader")
        .clone();

    // The follower's handle puts; the client-side redirect reaches the leader.
    until_ok(|| async { follower.put(b"k", b"v").await })
        .await
        .expect("redirected put must succeed");

    // All three nodes applied the write.
    for h in &handles {
        assert_eq!(
            h.get_stale(b"k").await.expect("get_stale"),
            Some(b"v".to_vec()),
            "every node's state machine must hold k => v"
        );
    }

    for s in &shutdowns {
        s.cancel();
    }
    for task in tasks {
        let _ = task.await;
    }
    for dir in dirs {
        let _ = std::fs::remove_dir_all(&dir);
    }
}
