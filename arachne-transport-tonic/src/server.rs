//! The tonic gRPC service: performs the handshake and forwards accepted
//! payloads to a node's inbound channel.
//!
//! The service is created per-listening-node by the
//! [`TonicTransportFactory`](crate::TonicTransportFactory). It holds the cluster
//! identity it validates against, the rejection counter, and the inbound sender
//! that feeds that node's [`TonicRx`](crate::TonicRx).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_seam::seam::TransportMessage;
use arachne_seam::types::NodeId;
use tokio::sync::mpsc::{error::TrySendError, Sender};
use tonic::{Request, Response, Status};

use crate::error::ERR_HELLO_MISSING;
use crate::handshake::validate_hello;
use crate::proto::raft_transport_server::RaftTransport;
use crate::proto::{RaftEnvelope, SendReply};

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
}

impl RaftTransportService {
    /// Build the service for one listening node.
    pub(crate) fn new(
        cluster_id: String,
        protocol_major: u32,
        protocol_minor: u32,
        rejects: Arc<AtomicU64>,
        inbound: Sender<(NodeId, TransportMessage)>,
    ) -> Self {
        Self {
            cluster_id,
            protocol_major,
            protocol_minor,
            rejects,
            inbound,
        }
    }
}

#[tonic::async_trait]
impl RaftTransport for RaftTransportService {
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
