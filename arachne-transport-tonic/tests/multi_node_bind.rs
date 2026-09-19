//! M1 (task A): real multi-process tonic cluster — one factory per process,
//! each binding ONLY its own node.
//!
//! [`TonicTransportFactory::start`] binds *every* entry of a factory's address
//! map, so a second process holding the same map would hit `EADDRINUSE` on the
//! first node's port. [`TonicTransportFactory::start_with_bind`] binds exactly
//! one node — the caller's own — so two independent factories, each constructed
//! with the SAME full address map, can both start without colliding.
//!
//! This test simulates two OS processes: each claims its own ephemeral loopback
//! port, builds its own factory with the full map, binds only its own node, and
//! proves a real raft payload crosses the process boundary with the correct
//! sender tag and zero handshake rejections.
//!
//! NOTE (entropy gates): `tests/` directories are scanned by
//! `scripts/check-entropy.sh` (Gate C forbids real-time / network imports
//! there), so this file uses `core::time::Duration`, `std::net::TcpListener`,
//! and `tokio::time::timeout`, and never the real-time or tokio-net APIs the
//! gate forbids.

use std::collections::HashMap;
use std::net::SocketAddr;

use arachne::{NodeId, Transport, TransportFactory, TransportMessage, TransportRx};
use arachne_transport_tonic::TonicTransportFactory;

/// Bounded window for the cross-process send to land on the receiver.
const RECV_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(5);

/// Claim an ephemeral loopback port the way an independent OS process would:
/// bind `127.0.0.1:0`, read the assigned port, and release it. The port is free
/// again, so a later factory can bind it for its own node.
fn claim_ephemeral() -> SocketAddr {
    let probe = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind a probe listener");
    let addr = probe.local_addr().expect("probe local addr");
    drop(probe);
    addr
}

/// The full cluster address map, as both processes would be configured with it.
fn full_map(addr1: SocketAddr, addr2: SocketAddr) -> HashMap<NodeId, SocketAddr> {
    let mut m = HashMap::new();
    m.insert(NodeId::from("n1"), addr1);
    m.insert(NodeId::from("n2"), addr2);
    m
}

#[tokio::test(flavor = "multi_thread")]
async fn two_independent_factories_bind_only_their_own_node() {
    let n1 = NodeId::from("n1");
    let n2 = NodeId::from("n2");

    // Simulate two OS processes: each claims its own ephemeral loopback port.
    let addr1 = claim_ephemeral();
    let addr2 = claim_ephemeral();

    // Both factories are constructed with the SAME full address map, exactly as
    // two processes launched from the same cluster configuration would be.
    let factory_a = TonicTransportFactory::new("m1", 1, 0, Vec::new(), full_map(addr1, addr2));
    let factory_b = TonicTransportFactory::new("m1", 1, 0, Vec::new(), full_map(addr1, addr2));

    // Each process binds ONLY its own node. With the old `start()` the second
    // bind would EADDRINUSE on the first node's port; `start_with_bind` never
    // touches the other node's address, so both succeed.
    factory_a
        .start_with_bind(n1.clone(), addr1)
        .await
        .expect("factory_a must bind only its own node");
    factory_b
        .start_with_bind(n2.clone(), addr2)
        .await
        .expect("factory_b must bind only its own node");

    // Mint the transport halves: n1 (process A) sends, n2 (process B) receives.
    let (tx_a, _rx_a) = factory_a.create(n1.clone());
    let (_, mut rx_b) = factory_b.create(n2.clone());

    // Cross-process: A's n1 sends a raft payload to B's n2.
    tx_a
        .send(n2.clone(), TransportMessage::Raft(vec![1, 2, 3, 4]))
        .await
        .expect("the cross-process send must succeed");

    // B's n2 receives it within the bounded window, tagged with A's node id.
    let (from, msg) = tokio::time::timeout(RECV_TIMEOUT, rx_b.recv())
        .await
        .expect("the payload must land before the window closes")
        .expect("the inbound channel must still be open");
    assert_eq!(
        from,
        NodeId::from("n1"),
        "the payload must be tagged with the sender's node id"
    );
    assert_eq!(
        msg,
        TransportMessage::Raft(vec![1, 2, 3, 4]),
        "the payload must be the one that was sent"
    );

    // Same-cluster, same-version traffic must never be rejected by the
    // handshake.
    assert_eq!(factory_a.handshake_rejections(), 0);
    assert_eq!(factory_b.handshake_rejections(), 0);

    // Tear down both processes' servers.
    factory_a.shutdown().await;
    factory_b.shutdown().await;
}
