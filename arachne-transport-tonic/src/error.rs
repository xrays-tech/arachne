//! Typed errors for the tonic transport, plus the stable wire error codes.
//!
//! A handshake rejection is *not* a gRPC transport failure: the server answers
//! the `Send` RPC normally, with a `SendReply { accepted: false, error_code }`.
//! The client maps that stable `error_code` back to a typed [`TransportError`]
//! variant here. Genuine transport failures (connection refused, RST, …) surface
//! as [`TransportError::Send`] wrapping a `tonic::Status`.

use arachne_seam::types::NodeId;
use thiserror::Error;
use tonic::Status;

/// Stable wire code: the sender's `cluster_id` does not match the receiver's.
pub const ERR_CLUSTER_ID_MISMATCH: &str = "cluster_id_mismatch";
/// Stable wire code: the sender's protocol version is incompatible.
pub const ERR_PROTOCOL_MISMATCH: &str = "protocol_mismatch";

/// Errors the tonic transport reports to the core.
///
/// The node treats a failed `send` as non-fatal (raft retransmits), so these
/// mostly matter for observability and for callers that send explicitly.
#[derive(Debug, Error)]
pub enum TransportError {
    /// The destination `NodeId` is not present in this transport's address map.
    #[error("unknown peer: {0}")]
    UnknownPeer(NodeId),

    /// The peer rejected the message because its `cluster_id` differs.
    #[error("cluster id mismatch (peer: {0})")]
    ClusterIdMismatch(String),

    /// The peer rejected the message because its protocol version is
    /// incompatible (major differs, or peer minor is newer than ours).
    #[error("protocol version mismatch")]
    ProtocolMismatch,

    /// The peer rejected the message with an unrecognized error code.
    #[error("send rejected by peer with code '{code}'")]
    Rejected { code: String },

    /// A genuine transport failure while performing the gRPC call.
    #[error("transport send failed: {0}")]
    Send(#[source] Status),

    /// The receiving side has closed its inbound channel (it is shutting down).
    #[error("inbound channel is closed")]
    InboundClosed,

    /// A `TransportMessage` variant this transport cannot carry.
    #[error("unsupported transport message variant")]
    UnsupportedMessage,

    /// The server failed to bind its listen address.
    #[error("failed to bind listen address: {0}")]
    Bind(#[source] std::io::Error),
}

impl TransportError {
    /// Map a peer-supplied wire `error_code` (from `SendReply`) to a typed
    /// variant. Unknown codes are preserved in [`TransportError::Rejected`].
    pub fn from_error_code(code: &str) -> Self {
        match code {
            ERR_CLUSTER_ID_MISMATCH => TransportError::ClusterIdMismatch(code.to_string()),
            ERR_PROTOCOL_MISMATCH => TransportError::ProtocolMismatch,
            other => TransportError::Rejected {
                code: other.to_string(),
            },
        }
    }
}
