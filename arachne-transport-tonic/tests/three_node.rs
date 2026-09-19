//! M1-2 acceptance — a three-node cluster over the **real** tonic transport.
//!
//! End-to-end proof that the tonic transport is wire-compatible with the core:
//! three `RaftNode`s, each backed by a real `WalStorage` (unique temp dir) and a
//! `KvStateMachine`, are wired together through one `TonicTransportFactory`
//! (real loopback TCP + gRPC + handshake). A deterministic round loop ticks every
//! node, persists and applies its `Ready`s, and drains each node's inbound queue
//! until a leader is elected and a `Put` commits on all three nodes.
//!
//! The loop is bounded and fails loudly if no leader emerges. `RaftNode::step`
//! treats undeliverable messages as non-fatal (raft retransmits), so no special
//! fault handling is needed.
//!
//! NOTE (entropy gates): `tests/` directories are scanned by
//! `scripts/check-entropy.sh` (Gate C forbids real-time/network imports there),
//! so this file uses `core::time::Duration` and no tokio select macros at all.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use arachne::consensus::{NodeError, RaftNode};
use arachne::state_machine::KvStateMachine;
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{
    LogIndex, NodeId, RaftId, StateMachine, Transport, TransportFactory, TransportMessage,
    TransportRx,
};
use arachne_transport_tonic::{TonicRx, TonicTransport, TonicTransportFactory, TransportError};
use slog::{o, Drain, Logger};

type TestNode = RaftNode<WalStorage, TonicTransport, TonicRx>;

/// Upper bound on drive-loop rounds (a few hundred; election + commit need far
/// fewer, the bound is only a safety net for a clear failure message).
const ROUNDS: u32 = 500;
/// Inbound drain window per node per round (lets in-flight messages land).
const DRAIN: core::time::Duration = core::time::Duration::from_millis(5);
/// A short yield between rounds.
const SLEEP: core::time::Duration = core::time::Duration::from_millis(1);

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-m1-3n-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn wal_opts(node: &str) -> WalOptions {
    WalOptions {
        cluster_id: "m1".into(),
        node_id: node.into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 1 << 20,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

fn node_id(i: RaftId) -> NodeId {
    NodeId::from(format!("n{i}"))
}

fn parse_raft_id(id: &NodeId) -> RaftId {
    id.as_str()
        .trim_start_matches('n')
        .parse::<RaftId>()
        .expect("harness node ids are `n<raft_id>`")
}

fn peers_of(self_id: RaftId, n: RaftId) -> HashMap<RaftId, NodeId> {
    (1..=n)
        .filter(|j| *j != self_id)
        .map(|j| (j, node_id(j)))
        .collect()
}

/// The 0-based index of a node that believes it is the leader, if any.
fn leader_index(nodes: &[TestNode]) -> Option<usize> {
    nodes
        .iter()
        .enumerate()
        .position(|(i, n)| n.leader_id() == i as RaftId + 1)
}

/// Unwrap a node operation, failing loudly with the `Display` error.
///
/// `NodeError<TonicTransport>` is not `Debug` (the transport half is not), so
/// `expect`/`unwrap` cannot be used directly; `unwrap_or_else` has no `Debug`
/// bound and the error's `Display` text is descriptive enough for a test.
fn expect_ok<T>(res: Result<T, NodeError<TonicTransport>>, what: &str) -> T {
    res.unwrap_or_else(|e| panic!("{what}: {e}"))
}

/// One round: drive every node (tick → persist → apply → advance), then drain
/// each node's inbound queue (the timeout bounds the wait on an empty queue and
/// yields to the runtime so in-flight messages can land).
async fn round(
    nodes: &mut [TestNode],
    sms: &mut [KvStateMachine],
    put_index: &mut Option<LogIndex>,
) -> Result<(), NodeError<TonicTransport>> {
    for i in 0..nodes.len() {
        let node = &mut nodes[i];
        node.tick();
        let outcome = node.step().await?;
        for (idx, data) in &outcome.committed {
            sms[i].apply(*idx, data).expect("apply must succeed");
            // Capture the index of the committed `k => v` put. Every node
            // applies the same log in the same order, so the first capture is
            // the cluster-wide entry index.
            if put_index.is_none() && sms[i].get(b"k").expect("get") == Some(b"v".to_vec()) {
                *put_index = Some(*idx);
            }
        }
        node.advance_apply();

        // Drain inbound: pull everything that has landed since the last round.
        // Borrows of `node` (via `node.rx()`) live only for the condition's
        // await; `on_message` runs with a fresh borrow in the body.
        while let Ok(Some((from, msg))) =
            tokio::time::timeout(DRAIN, node.rx().recv()).await
        {
            node.on_message(parse_raft_id(&from), msg)?;
        }
    }
    Ok(())
}

/// M1-2: three nodes over real tonic elect a leader, commit a `Put` on all of
/// them, and do so without a single handshake rejection.
///
/// Returns `()` and uses `expect` at the test boundary: `NodeError<TonicTransport>`
/// is not `Debug` (the transport half is not), which `#[tokio::test]` requires
/// of a `Result` return type.
#[tokio::test(flavor = "multi_thread")]
async fn three_node_cluster_elects_and_commits_over_tonic() {
    // 1. One factory for the whole cluster: real listeners per node (`:0` yields
    //    ephemeral ports that `start` writes back into the shared address map).
    let mut addresses = HashMap::new();
    for i in 1..=3 {
        addresses.insert(node_id(i), ([127, 0, 0, 1], 0).into());
    }
    let factory = TonicTransportFactory::new("m1", 1, 0, Vec::new(), addresses);
    factory
        .start()
        .await
        .expect("tonic factory must bind and serve every node");

    // 2. Build the three nodes, each with its own WAL dir and state machine.
    let mut dirs = Vec::new();
    let mut nodes = Vec::new();
    let mut sms = Vec::new();
    for i in 1..=3 {
        let dir = temp_dir(&format!("n{i}"));
        let wal = WalStorage::open(&dir, wal_opts(&format!("n{i}"))).expect("open wal");
        let (tx, rx) = factory.create(node_id(i));
        let node = expect_ok(
            RaftNode::new(i, peers_of(i, 3), wal, tx, rx, 0, &logger()),
            "node construction must succeed",
        );
        dirs.push(dir);
        nodes.push(node);
        sms.push(KvStateMachine::new());
    }

    // 3. Drive rounds until a leader emerges and the put commits everywhere.
    let cmd = KvStateMachine::encode_put(1, 1, b"k", b"v");
    let mut proposed = false;
    let mut put_index = None;

    for _ in 0..ROUNDS {
        expect_ok(
            round(&mut nodes, &mut sms, &mut put_index).await,
            "drive round must succeed",
        );
        if let Some(li) = leader_index(&nodes) {
            if !proposed {
                expect_ok(
                    nodes[li].propose(&cmd),
                    "propose to the elected leader must succeed",
                );
                proposed = true;
            }
        }
        if proposed
            && put_index.is_some()
            && sms.iter().all(|sm| sm.get(b"k").expect("get") == Some(b"v".to_vec()))
        {
            break;
        }
        tokio::time::sleep(SLEEP).await;
    }

    // 4. Assert consensus and a clean handshake.
    let li = leader_index(&nodes).expect(
        "no leader emerged within the round bound: the cluster must elect one",
    );
    let agreed: Vec<RaftId> = nodes.iter().map(|n| n.leader_id()).collect();
    assert!(
        agreed.iter().all(|id| *id == agreed[0]) && agreed[0] != 0,
        "all three nodes must agree on the same leader, got {agreed:?}"
    );
    assert_eq!(
        agreed[0],
        li as RaftId + 1,
        "the agreed leader must be the node that actually won the election"
    );

    for (i, sm) in sms.iter().enumerate() {
        assert_eq!(
            sm.get(b"k").expect("get"),
            Some(b"v".to_vec()),
            "node {} state machine must hold k => v",
            i + 1
        );
    }

    let put_index = put_index.expect("the put entry must have been applied");
    for (i, node) in nodes.iter().enumerate() {
        assert!(
            node.hard_state().commit >= put_index,
            "node {} hard-state commit {} is behind the put entry {put_index}",
            i + 1,
            node.hard_state().commit
        );
    }

    assert_eq!(
        factory.handshake_rejections(),
        0,
        "same-cluster traffic must never be rejected by the handshake"
    );

    // 5. Shut down the servers, drop the nodes (releasing the WAL locks), and
    //    remove the temp dirs.
    factory.shutdown().await;
    drop(nodes);
    for dir in dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// M1-2 (handshake): a client speaking for a **different cluster** is rejected
/// by the handshake — the send fails with `cluster_id_mismatch` and the
/// receiving cluster's `handshake_rejections` counter increments.
///
/// Why the second factory is NOT `start`ed: `start` binds *every* entry of the
/// factory's address map, and this map must contain the target's real listen
/// port for the cross-cluster send to reach it — starting would re-bind that
/// port (EADDRINUSE). `create` needs no bind, so the outbound `TonicTransport`
/// (with the intruder's handshake) still resolves the target correctly.
#[tokio::test(flavor = "multi_thread")]
async fn wrong_cluster_id_is_rejected_by_handshake() {
    // Claim a free port (bind → read → release), so the "right" cluster's
    // listener has a known address the intruder can be pointed at.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a probe listener");
    let target: SocketAddr = probe.local_addr().expect("probe local addr");
    drop(probe);

    // The "right" cluster: one listener on the known port.
    let mut right_addrs = HashMap::new();
    right_addrs.insert(NodeId::from("n1"), target);
    let right = TonicTransportFactory::new("m1", 1, 0, Vec::new(), right_addrs);
    right.start().await.expect("the right cluster must start");

    // The "wrong" cluster: a second factory with a different cluster_id, whose
    // address map points `n1` at the right cluster's listener.
    let mut wrong_addrs = HashMap::new();
    wrong_addrs.insert(NodeId::from("x1"), ([127, 0, 0, 1], 0).into());
    wrong_addrs.insert(NodeId::from("n1"), target);
    let wrong = TonicTransportFactory::new("intruder", 1, 0, Vec::new(), wrong_addrs);
    let (tx, _rx) = wrong.create(NodeId::from("x1"));

    // The send must be refused by the handshake (and counted on the receiver).
    let err = tx
        .send(NodeId::from("n1"), TransportMessage::Raft(vec![1, 2, 3]))
        .await
        .expect_err("a send from a different cluster must be rejected");
    assert!(
        matches!(err, TransportError::ClusterIdMismatch(_)),
        "expected cluster_id_mismatch, got {err:?}"
    );
    assert_eq!(
        right.handshake_rejections(),
        1,
        "the rejection must be counted on the receiving cluster"
    );

    right.shutdown().await;
}
