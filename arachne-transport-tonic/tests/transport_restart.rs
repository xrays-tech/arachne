//! Integration: a peer that restarts on the same address stays reachable.
//!
//! The transport caches one gRPC channel per peer. A restarted peer comes back
//! with a new listener behind the same address, so the cached channel has to
//! recover — otherwise every later send to that peer would fail for the life of
//! the *sending* process, and a restarted follower could never be caught up.
//! This test pins that property (verified in round 18 to hold on its own, without
//! any eviction logic on our side: tonic's channel reconnects).
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`, so
//! this file uses `core::time::Duration` and `tokio::time` only.

use std::collections::HashMap;
use std::net::SocketAddr;

use arachne_seam::seam::{Transport, TransportFactory, TransportMessage};
use arachne_seam::types::NodeId;
use arachne_transport_tonic::TonicTransportFactory;

fn claim_ephemeral() -> SocketAddr {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let addr = probe.local_addr().expect("probe addr");
    drop(probe);
    addr
}

/// A factory that binds **only** `node` (a single factory would bind every
/// address in its map) and keeps its inbound half alive.
async fn bind(node: &NodeId, addr: SocketAddr) -> (TonicTransportFactory, SocketAddr) {
    let mut map = HashMap::new();
    map.insert(node.clone(), addr);
    let factory = TonicTransportFactory::new("restart", 1, 0, Vec::new(), map);
    factory
        .start_with_bind(node.clone(), addr)
        .await
        .expect("bind");
    let (tx_keep, rx_keep) = factory.create(node.clone());
    // The halves must outlive this helper; leaking is enough for a test and
    // keeps the inbound channel open so the peer looks restarted, not gone.
    std::mem::forget(tx_keep);
    std::mem::forget(rx_keep);
    (factory, addr)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_peer_is_reachable_again() {
    let peer = NodeId::from("peer");
    let sender = NodeId::from("sender");
    let peer_addr = claim_ephemeral();
    let sender_addr = claim_ephemeral();

    // One long-lived sender: this is the situation a follower restart creates.
    let mut sender_map = HashMap::new();
    sender_map.insert(peer.clone(), peer_addr);
    sender_map.insert(sender.clone(), sender_addr);
    let sender_factory = TonicTransportFactory::new("restart", 1, 0, Vec::new(), sender_map);
    sender_factory
        .start_with_bind(sender.clone(), sender_addr)
        .await
        .expect("bind sender");
    let (tx, rx) = sender_factory.create(sender.clone());
    std::mem::forget(rx);

    let (first, _) = bind(&peer, peer_addr).await;
    tx.send(peer.clone(), TransportMessage::Raft(vec![1]))
        .await
        .expect("the first send must succeed and cache a channel");

    // Restart the peer on the same address.
    first.shutdown().await;
    let (second, _) = bind(&peer, peer_addr).await;

    // The first attempt after the restart may fail (that is what a stale
    // connection looks like); a retry must reach the new listener.
    let mut last = None;
    let mut delivered = false;
    for _ in 0..20 {
        match tx.send(peer.clone(), TransportMessage::Raft(vec![2])).await {
            Ok(()) => {
                delivered = true;
                break;
            }
            Err(e) => {
                last = Some(e.to_string());
                tokio::time::sleep(core::time::Duration::from_millis(50)).await;
            }
        }
    }
    assert!(
        delivered,
        "a restarted peer must be reachable again: {last:?}"
    );

    sender_factory.shutdown().await;
    second.shutdown().await;
}
