//! Multi-node **client** test for remote (over-wire) forwarding.
//!
//! When a client issues `put`/`get` through a **follower's** `Handle`, that handle
//! receives a `NotLeader` and must follow the hint to the leader. In this suite the
//! three nodes are assembled in a single process but with **independent** handles
//! (no in-process peers registered), so a follower's redirect is forced over the
//! real tonic `Forward` RPC (the multi-process path) rather than in-process.
//! That exercises the §5 wiring: the leader's inbound gRPC service must carry the
//! node's [`ForwardCommandSink`], which `assemble_cluster` must register *before*
//! the transport starts serving. If the sink is not installed, the leader answers
//! `unavailable` and the follower's client sees an `Unrecoverable` error.
//!
//! **Why in one process?** The façade is a library — external process wrappers belong
//! to calling projects. The network is still real: each node binds its own loopback
//! port and serves a genuine tonic gRPC server, so a forward genuinely traverses the
//! `Forward` RPC across processes' worth of machinery.
//!
//! **Gate C (no real time in tests):** unique temp dirs come from `std::process::id()`
//! plus a process-local sequence counter; port pre-detection uses `std::net::TcpListener`
//! (Gate C forbids `std::time`/`tokio::net` in tests); every polling loop is bounded
//! by a fixed attempt count and a `tokio::time::sleep` interval.
//!
//! `assemble_cluster` is `async` (the gRPC servers run on the caller's tokio runtime);
//! each node's raft actor runs on its own dedicated OS thread.

#[cfg(feature = "transport-tonic")]
mod wire_forwarding {
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use arachne_kv::server::{AssembledClusterNode, ClusterConfig};
    use arachne_kv::NodeId;

    // Unique per-process sequence so `process::id()` (shared across all tests in one
    // binary) is disambiguated without reaching for real time.
    static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

    /// A fresh, unique temp dir for a single node/test.
    fn unique_tempdir() -> PathBuf {
        let seq = DIR_SEQ.fetch_add(1, Ordering::Relaxed);
        PathBuf::from(env::temp_dir())
            .join(format!("arachne-wire-{0}-{1}", std::process::id(), seq))
    }

    /// Bind `n` fresh, unique TCP ports on 127.0.0.1 (Gate C: no real time; the
    /// kernel supplies the concrete values). Uses `std::net` (sync) — Gate C only
    /// forbids `std::time` and `tokio::net` in tests — and is called synchronously
    /// from the async setup (a one-shot, cheap bind).
    fn prebind_ports(n: u32) -> Vec<SocketAddr> {
        let mut addrs = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")
                .expect("cannot bind a free loopback port");
            addrs.push(listener.local_addr().expect("listener has a local addr"));
        }
        addrs
    }

    const MAX_ATTEMPTS: u32 = 120; // 120 × 50ms = 6s budget (election + redirect)

    async fn poll() {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    /// Poll the given (alive) nodes until one serves a linearizable read (`get == Ok`);
    /// that node is the current leader. Returns its original index, or None on bound.
    async fn find_leader(alive: &[(usize, &AssembledClusterNode)]) -> Option<usize> {
        let mut last_err = String::new();
        for _ in 0..MAX_ATTEMPTS {
            for (idx, node) in alive.iter() {
                match node.handle.get(b"health").await {
                    Ok(_) => return Some(*idx),
                    Err(e) => last_err = format!("[{idx}] {e:?}"),
                }
            }
            poll().await;
        }
        eprintln!(
            "no live node served a read in {MAX_ATTEMPTS} attempts; last: {last_err}"
        );
        None
    }

    /// Poll a node's stale read until it equals `expected`.
    async fn wait_stale(node: &AssembledClusterNode, key: &[u8], expected: &[u8]) -> bool {
        for _ in 0..MAX_ATTEMPTS {
            match node.handle.get_stale(key).await {
                Ok(Some(v)) if v == expected => return true,
                _ => {}
            }
            poll().await;
        }
        false
    }

    /// Assemble 3 nodes with concrete pre-detected ports and unique temp dirs.
    /// Handles are **independent** (no `register_peer`), so a follower's redirect
    /// is forced over the wire — the multi-process forwarding path.
    async fn three_nodes() -> (Vec<AssembledClusterNode>, Vec<PathBuf>) {
        let ids: Vec<NodeId> = vec![NodeId::new("w1"), NodeId::new("w2"), NodeId::new("w3")];
        let ports = prebind_ports(3);
        let initial_cluster = ids.clone();

        let mut nodes = Vec::with_capacity(3);
        let mut dirs = Vec::with_capacity(3);
        for (i, id) in ids.iter().enumerate() {
            let dir = unique_tempdir();
            dirs.push(dir.clone());
            let cfg = ClusterConfig::member(
                "wire-forward".to_string(),
                id.clone(),
                ports[i],
                dir,
                initial_cluster.clone(),
                {
                    let mut m = HashMap::new();
                    for (j, nid) in ids.iter().enumerate() {
                        m.insert(nid.clone(), ports[j]);
                    }
                    m
                },
            );
            let node = arachne_kv::server::assemble_cluster(cfg)
                .await
                .unwrap_or_else(|e| panic!("assemble node {id}: {e:?}"));
            nodes.push(node);
        }
        (nodes, dirs)
    }

    /// Shutdown each node: stop the gRPC (idempotent, `&self`), then stop the actor
    /// thread (which *consumes* the `RuntimeThread`). The gRPC-first, thread-second
    /// order matches `Arachne::shutdown`.
    async fn shutdown_nodes(nodes: Vec<AssembledClusterNode>) {
        for node in nodes {
            node.tonic.shutdown().await;
            node.thread.shutdown();
        }
    }

    #[tokio::test]
    async fn follower_put_forwards_over_wire() {
        let (nodes, dirs) = three_nodes().await;
        let alive: Vec<(usize, &AssembledClusterNode)> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (i, n))
            .collect();
        let leader = find_leader(&alive)
            .await
            .expect("a leader must be elected in the 3-node cluster");
        // A follower is any node that is not the leader.
        let follower = nodes
            .iter()
            .enumerate()
            .find(|(i, _)| *i != leader)
            .expect("with 3 nodes there is a follower distinct from the leader");
        let follower_idx = follower.0;

        // A linearizable write driven through the *follower's* handle. Since that
        // handle has no in-process peer for the leader, the redirect is forced over
        // the wire (`Forward` RPC), exercising the sink-before-start §5 wiring.
        nodes[follower_idx]
            .handle
            .put(b"wire", b"value").await
            .expect("follower put must succeed after following the leader hint over the wire");

        // Cross-node replication via stale reads on all 3 nodes (independent of
        // which node led).
        for (i, node) in nodes.iter().enumerate() {
            assert!(
                wait_stale(node, b"wire", b"value").await,
                "node {i} should see the replicated value via get_stale"
            );
        }
        println!(
            "[wire] leader {leader}, follower {follower_idx}; put forwarded over the wire"
        );

        shutdown_nodes(nodes).await;
        for d in &dirs {
            let _ = fs::remove_dir_all(d);
        }
    }

    #[tokio::test]
    async fn follower_get_forwards_over_wire() {
        let (nodes, dirs) = three_nodes().await;
        let alive: Vec<(usize, &AssembledClusterNode)> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (i, n))
            .collect();
        let leader = find_leader(&alive)
            .await
            .expect("a leader must be elected in the 3-node cluster");
        let follower_idx = nodes
            .iter()
            .enumerate()
            .find(|(i, _)| *i != leader)
            .map(|(i, _)| i)
            .expect("with 3 nodes there is a follower distinct from the leader");

        // Seed a value through the leader directly (the leader serves ReadIndex reads).
        nodes[leader]
            .handle
            .put(b"wire-read", b"seek").await
            .expect("leader put");
        assert!(
            nodes[leader]
                .handle
                .get_stale(b"wire-read")
                .await
                .is_ok(),
            "leader committed the seed value"
        );

        // A linearizable read driven through the *follower's* handle: the follower
        // redirects to the leader over the wire (ReadIndex read), which is what
        // makes it linearizable. Independent of which node led.
        let v = nodes[follower_idx]
            .handle
            .get(b"wire-read")
            .await
            .expect("follower get must succeed after following the leader hint over the wire");
        assert_eq!(
            v,
            Some(b"seek".to_vec()),
            "follower get should serve the seed value after an over-wire redirect"
        );

        shutdown_nodes(nodes).await;
        for d in &dirs {
            let _ = fs::remove_dir_all(d);
        }
    }
}

#[cfg(not(feature = "transport-tonic"))]
mod no_tonic {
    use std::path::PathBuf;
    use std::collections::HashMap;

    use arachne_kv::server::{Arachne, ArachneError, ClusterConfig};
    use arachne_kv::NodeId;

    #[tokio::test]
    async fn multi_node_requires_tonic_feature() {
        let n1 = NodeId::new("w1");
        let n2 = NodeId::new("w2");
        let cfg = ClusterConfig::member(
            "lean-cluster".to_string(),
            n1.clone(),
            "127.0.0.1:1".parse().expect("addr"),
            PathBuf::new(),
            vec![n1.clone(), n2.clone()],
            {
                let mut m = HashMap::new();
                m.insert(n1, "127.0.0.1:1".parse().unwrap());
                m.insert(n2, "127.0.0.1:2".parse().unwrap());
                m
            },
        );
        match Arachne::start(cfg) {
            Err(ArachneError::Unrecoverable(m)) => {
                assert_eq!(m, "multi-node requires the transport-tonic feature");
            }
            e => panic!("multi-node without tonic: expected Unrecoverable, got {e:?}"),
        }
    }
}
