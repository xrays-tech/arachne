//! Integration: the streamed snapshot RPC (propsol rev T, v0.2.17).
//!
//! Before this RPC, a snapshot travelled as one raft `Message` — one gRPC
//! message — and the transport caps gRPC messages at 8 MiB while the snapshot
//! threshold is 64 MiB (Lan). Any snapshot above the cap was untransferable, so
//! a follower that fell behind `wal_trailing_keep` could never catch up. These
//! tests pin the replacement: the bytes arrive exactly (the receiver validates
//! the snapshot's own CRC, invariant I9), an unauthenticated caller gets none of
//! them, and a configured rate visibly paces the stream.
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids real-time / network imports there), so this file uses
//! `core::time::Duration` and `tokio::time` only — never `std::time` or
//! `tokio::net`.

use std::collections::HashMap;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;

use arachne_transport_tonic::proto::raft_transport_client::RaftTransportClient;
use arachne_transport_tonic::proto::{Hello, SnapshotRequest};
use arachne_transport_tonic::snapshot::{SnapshotProvider, SnapshotReader};
use arachne_transport_tonic::TonicTransportFactory;
use arachne_seam::types::NodeId;

/// The chunk size the server streams in (kept in step with `SNAPSHOT_CHUNK_BYTES`).
const CHUNK: usize = 256 * 1024;
/// The snapshot position the provider serves.
const INDEX: u64 = 7;
const TERM: u64 = 3;

/// Deterministic bytes, so a mismatch is easy to see.
fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Serves one fixed snapshot.
struct FixedProvider {
    // `Arc<[u8]>`, not `Arc<Vec<u8>>`: a cursor over it is a `Read` without
    // copying the snapshot for every fetch.
    bytes: Arc<[u8]>,
}

impl SnapshotProvider for FixedProvider {
    fn open(&self, index: u64, term: u64) -> Option<SnapshotReader> {
        if index != INDEX || term != TERM {
            return None;
        }
        Some(SnapshotReader {
            len: self.bytes.len() as u64,
            // The cursor reads through the `Arc` without copying the snapshot —
            // the provider hands out a reader, not a buffer.
            reader: Box::new(Cursor::new(Arc::clone(&self.bytes))),
        })
    }
}

fn claim_ephemeral() -> SocketAddr {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a probe listener");
    let addr = probe.local_addr().expect("probe local addr");
    drop(probe);
    addr
}

fn valid_hello() -> Hello {
    Hello {
        protocol_major: 1,
        protocol_minor: 0,
        cluster_id: "snap".to_string(),
        node_id: "n2".to_string(),
        feature_flags: Vec::new(),
    }
}

/// Start a single-node transport server serving `bytes` at `rate_bps`.
async fn start_server(
    bytes: Arc<[u8]>,
    rate_bps: u64,
) -> (SocketAddr, TonicTransportFactory) {
    let n1 = NodeId::from("n1");
    let addr = claim_ephemeral();
    let mut map = HashMap::new();
    map.insert(n1.clone(), addr);
    let factory = TonicTransportFactory::new("snap", 1, 0, Vec::new(), map);
    factory.snapshot_rate_bps(rate_bps);
    factory.snapshot_provider(Arc::new(FixedProvider { bytes }));
    factory
        .start_with_bind(n1, addr)
        .await
        .expect("the server must bind");
    (addr, factory)
}

async fn client(addr: SocketAddr) -> RaftTransportClient<tonic::transport::Channel> {
    RaftTransportClient::connect(format!("http://{addr}"))
        .await
        .expect("connect to the transport server")
}

/// Drain a snapshot stream into one buffer.
async fn collect(
    client: &mut RaftTransportClient<tonic::transport::Channel>,
    hello: Option<Hello>,
    index: u64,
    term: u64,
) -> Result<Vec<u8>, tonic::Status> {
    let request = SnapshotRequest { index, term, hello };
    let mut stream = client.fetch_snapshot(request).await?.into_inner();
    let mut out = Vec::new();
    while let Some(chunk) = stream.message().await? {
        out.extend_from_slice(&chunk.data);
    }
    Ok(out)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_streamed_snapshot_arrives_byte_for_byte() {
    // Larger than one chunk, so the receiver really does concatenate.
    let source: Arc<[u8]> = payload(2 * CHUNK + 1234).into();
    let (addr, factory) = start_server(Arc::clone(&source), 0).await;
    let mut client = client(addr).await;

    let got = collect(&mut client, Some(valid_hello()), INDEX, TERM)
        .await
        .expect("the fetch must succeed");
    assert_eq!(
        got.len(),
        source.len(),
        "every byte must arrive (a short stream would make the CRC check fail)"
    );
    assert_eq!(
        got.as_slice(),
        &source[..],
        "the streamed snapshot must be byte-for-byte identical to the source (I10)"
    );

    // A snapshot the node does not have is a clean NOT_FOUND, not an empty
    // stream that the follower would install as a valid snapshot.
    let status = collect(&mut client, Some(valid_hello()), INDEX + 1, TERM)
        .await
        .expect_err("an unknown snapshot must be refused");
    assert_eq!(status.code(), tonic::Code::NotFound);

    assert_eq!(factory.handshake_rejections(), 0);
    factory.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unauthenticated_caller_gets_no_snapshot_bytes() {
    let source: Arc<[u8]> = payload(CHUNK).into();
    let (addr, factory) = start_server(Arc::clone(&source), 0).await;
    let mut client = client(addr).await;

    // No handshake at all.
    let status = collect(&mut client, None, INDEX, TERM)
        .await
        .expect_err("a missing handshake must be refused before any byte is served");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);

    // A handshake from another cluster.
    let mut foreign = valid_hello();
    foreign.cluster_id = "other".to_string();
    let status = collect(&mut client, Some(foreign), INDEX, TERM)
        .await
        .expect_err("another cluster must not read our snapshots");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);

    // A handshake with an incompatible major version.
    let mut future = valid_hello();
    future.protocol_major = 99;
    let status = collect(&mut client, Some(future), INDEX, TERM)
        .await
        .expect_err("an incompatible protocol must not read our snapshots");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);

    // Each rejection is counted (the L3 surface for a misconfigured cluster).
    assert_eq!(
        factory.handshake_rejections(),
        3,
        "every refused fetch must be counted"
    );
    factory.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_configured_rate_paces_the_stream() {
    // 768 KiB = three chunks. At 256 KiB/s the first chunk is the bucket's
    // burst and the other two are paced ~1 s apart, so the transfer cannot
    // finish quickly; with pacing off (rate 0) the same bytes cross at once.
    // The contrast is the assertion — no wall-clock threshold is hard-coded
    // beyond the two windows.
    let source: Arc<[u8]> = payload(3 * CHUNK).into();

    let (slow_addr, slow_factory) = start_server(Arc::clone(&source), CHUNK as u64).await;
    let mut slow_client = client(slow_addr).await;
    let early = tokio::time::timeout(
        core::time::Duration::from_millis(500),
        collect(&mut slow_client, Some(valid_hello()), INDEX, TERM),
    )
    .await;
    assert!(
        early.is_err(),
        "a 768 KiB snapshot at 256 KiB/s must not complete within 500 ms"
    );
    // It does complete, just paced.
    let got = collect(&mut slow_client, Some(valid_hello()), INDEX, TERM)
        .await
        .expect("the paced transfer must still finish");
    assert_eq!(got.as_slice(), &source[..]);
    slow_factory.shutdown().await;

    let (fast_addr, fast_factory) = start_server(Arc::clone(&source), 0).await;
    let mut fast_client = client(fast_addr).await;
    let unlimited = tokio::time::timeout(
        core::time::Duration::from_millis(500),
        collect(&mut fast_client, Some(valid_hello()), INDEX, TERM),
    )
    .await
    .expect("with pacing off the same transfer must finish well inside 500 ms")
    .expect("and succeed");
    assert_eq!(unlimited.as_slice(), &source[..]);
    fast_factory.shutdown().await;
}
