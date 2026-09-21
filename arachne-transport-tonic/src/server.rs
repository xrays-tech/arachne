//! The tonic gRPC service: performs the handshake and forwards accepted
//! payloads to a node's inbound channel.
//!
//! The service is created per-listening-node by the
//! [`TonicTransportFactory`](crate::TonicTransportFactory). It holds the cluster
//! identity it validates against, the rejection counter, and the inbound sender
//! that feeds that node's [`TonicRx`](crate::TonicRx).

use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_seam::seam::TransportMessage;
use arachne_seam::types::NodeId;
use tokio::sync::mpsc::{error::TrySendError, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::error::ERR_HELLO_MISSING;
use crate::handshake::validate_hello;
use crate::proto::raft_transport_server::RaftTransport;
use crate::proto::{RaftEnvelope, SendReply, SnapshotChunk, SnapshotRequest};
use crate::snapshot::{RateLimiter, SnapshotProvider};

/// How much of a snapshot one streamed chunk carries.
///
/// A constant, not a knob: chunk size is a wire detail that both ends must
/// agree on implicitly (the receiver concatenates and checks the snapshot's own
/// CRC, so boundaries are meaningless). Smaller chunks pace more smoothly
/// against `snapshot_transfer_rate_bps`; larger ones cost fewer wakeups.
pub(crate) const SNAPSHOT_CHUNK_BYTES: usize = 256 * 1024;

/// The gRPC server half for one listening node.
pub struct RaftTransportService {
    cluster_id: String,
    protocol_major: u32,
    protocol_minor: u32,
    /// Cluster-wide counter of handshake rejections (shared across all nodes'
    /// servers so a single accessor reports the total).
    rejects: Arc<AtomicU64>,
    /// Feeds the owning node's inbound [`TonicRx`](crate::TonicRx). Bounded: a
    /// full queue answers `resource_exhausted` (raft retransmits) instead of
    /// blocking the handler or growing memory unboundedly.
    inbound: Sender<(NodeId, TransportMessage)>,
    /// Where snapshot bytes come from (rev T). `None` means this node cannot
    /// serve snapshots — the RPC answers `unavailable` rather than pretending.
    snapshot_provider: Option<Arc<dyn SnapshotProvider>>,
    /// Pacing for a snapshot stream, from `snapshot_transfer_rate_bps`
    /// (0 = unlimited).
    snapshot_rate_bps: u64,
}

impl RaftTransportService {
    /// Build the service for one listening node.
    pub(crate) fn new(
        cluster_id: String,
        protocol_major: u32,
        protocol_minor: u32,
        rejects: Arc<AtomicU64>,
        inbound: Sender<(NodeId, TransportMessage)>,
        snapshot_provider: Option<Arc<dyn SnapshotProvider>>,
        snapshot_rate_bps: u64,
    ) -> Self {
        Self {
            cluster_id,
            protocol_major,
            protocol_minor,
            rejects,
            inbound,
            snapshot_provider,
            snapshot_rate_bps,
        }
    }
}

/// Read `reader` in chunks, pace them, and push them into `tx`.
///
/// Ends silently when the client goes away (the send fails), which is the
/// correct behaviour for a cancelled stream: the follower will ask again.
async fn stream_snapshot(
    reader: crate::snapshot::SnapshotReader,
    rate_bps: u64,
    tx: Sender<Result<SnapshotChunk, Status>>,
) {
    let mut remaining = reader.len;
    let mut reader = reader.reader;
    let mut limiter = RateLimiter::new(rate_bps, SNAPSHOT_CHUNK_BYTES as u64);
    let mut buf = vec![0u8; SNAPSHOT_CHUNK_BYTES];

    while remaining > 0 {
        let want = remaining.min(SNAPSHOT_CHUNK_BYTES as u64) as usize;
        let mut filled = 0usize;
        // A single `read` may return short; loop until the chunk is full or the
        // snapshot ends early (which is a real error: the length lied).
        while filled < want {
            match reader.read(&mut buf[filled..want]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) => {
                    let _ = tx
                        .send(Err(Status::internal(format!("snapshot read failed: {e}"))))
                        .await;
                    return;
                }
            }
        }
        if filled == 0 {
            let _ = tx
                .send(Err(Status::internal(
                    "snapshot ended before its declared length",
                )))
                .await;
            return;
        }
        limiter.acquire(filled as u64).await;
        let chunk = SnapshotChunk {
            data: buf[..filled].to_vec(),
        };
        if tx.send(Ok(chunk)).await.is_err() {
            return;
        }
        remaining -= filled as u64;
    }
}

#[tonic::async_trait]
impl RaftTransport for RaftTransportService {
    type FetchSnapshotStream = ReceiverStream<Result<SnapshotChunk, Status>>;

    /// Stream one snapshot's bytes (rev T).
    ///
    /// The handshake is validated **before** the provider is consulted, so an
    /// unauthenticated caller cannot make this node read (or disclose) cluster
    /// data. The stream is produced by a task reading from the provider one
    /// chunk at a time and waiting on a token bucket between chunks, so neither
    /// the node's memory nor the link is taken over by a transfer.
    async fn fetch_snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::FetchSnapshotStream>, Status> {
        let request = request.into_inner();

        let Some(hello) = request.hello else {
            self.rejects.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied(ERR_HELLO_MISSING));
        };
        if let Some(code) = validate_hello(
            &hello,
            &self.cluster_id,
            self.protocol_major,
            self.protocol_minor,
        ) {
            self.rejects.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied(code));
        }

        let Some(provider) = self.snapshot_provider.clone() else {
            return Err(Status::unavailable(
                "snapshot streaming is not configured on this node",
            ));
        };
        let Some(reader) = provider.open(request.index, request.term) else {
            return Err(Status::not_found("no such snapshot"));
        };

        // A small channel: this is the transfer's back-pressure. If the client
        // stops reading (slow link, cancelled stream), the send blocks and the
        // reader task parks instead of buffering the snapshot in memory.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<SnapshotChunk, Status>>(2);
        let rate = self.snapshot_rate_bps;
        tokio::spawn(async move {
            stream_snapshot(reader, rate, tx).await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn send(&self, request: Request<RaftEnvelope>) -> Result<Response<SendReply>, Status> {
        let envelope = request.into_inner();

        // A missing handshake is rejected *unconditionally*: we never fall back
        // to a `Default` hello (an empty cluster_id / major 0 would otherwise be
        // accepted if the cluster were misconfigured that way).
        let Some(hello) = envelope.hello else {
            self.rejects.fetch_add(1, Ordering::Relaxed);
            return Ok(Response::new(SendReply {
                accepted: false,
                error_code: ERR_HELLO_MISSING.to_string(),
            }));
        };

        // Handshake first: reject (and count) before the payload is touched.
        if let Some(code) = validate_hello(&hello, &self.cluster_id, self.protocol_major, self.protocol_minor) {
            self.rejects.fetch_add(1, Ordering::Relaxed);
            return Ok(Response::new(SendReply {
                accepted: false,
                error_code: code.to_string(),
            }));
        }

        // The sender's identity comes from its handshake — the authoritative
        // tag the core trusts (never from the untrusted payload bytes).
        let sender = NodeId::new(hello.node_id);
        let payload = envelope.payload;

        // Bounded inbound: never block the gRPC handler on a slow consumer and
        // never grow memory unboundedly. `try_send` either succeeds, reports a
        // full queue (back-pressure → raft retransmits), or a closed channel
        // (the node is going down).
        match self.inbound.try_send((sender, TransportMessage::Raft(payload))) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                return Err(Status::resource_exhausted("inbound queue full"));
            }
            Err(TrySendError::Closed(_)) => {
                return Err(Status::unavailable("inbound channel closed"));
            }
        }

        Ok(Response::new(SendReply {
            accepted: true,
            error_code: String::new(),
        }))
    }
}
