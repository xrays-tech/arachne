//! The tonic gRPC service: performs the handshake and forwards accepted
//! payloads to a node's inbound channel.
//!
//! The service is created per-listening-node by the
//! [`TonicTransportFactory`](crate::TonicTransportFactory). It holds the cluster
//! identity it validates against, the rejection counter, and the inbound sender
//! that feeds that node's [`TonicRx`](crate::TonicRx).

use std::collections::HashMap;
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use arachne_kv_seam::seam::{ApplyOutcome, CommandSink, ForwardCommand, ForwardOutcome, TransportMessage};
use arachne_kv_seam::types::{LogIndex, NodeId};
use tokio::sync::mpsc::{error::TrySendError, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::error::ERR_HELLO_MISSING;
use crate::handshake::validate_hello;
use crate::proto::raft_transport_server::RaftTransport;
use crate::proto::{
    forward_result, BytesValue, CasFailed, ForwardReply, ForwardRequest, ForwardResult,
    MultiGetReply, OptionalBytesValue, RaftEnvelope, SendReply, SnapshotChunk, SnapshotRequest,
};
use crate::snapshot::{RateLimiter, SnapshotProvider};

/// The largest useful snapshot chunk.
///
/// An upper bound, not a knob: the actual chunk is
/// `min(this, max_message_size / 2)`, because a chunk is a gRPC message and one
/// above the configured cap cannot cross at all. (Sizing it independently of the
/// cap is what broke every streamed transfer on a transport configured below
/// 512 KiB.) Within that bound, smaller chunks pace more smoothly against
/// `snapshot_transfer_rate_bps`; larger ones cost fewer wakeups.
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
    /// Where forwarded client commands are run (propsol v0.2.19). A map keyed
    /// by node id, so the sink can be registered *after* the gRPC server is up
    /// (the facade registers it right after `Runtime::new`). A missing entry
    /// answers `unavailable` rather than hang the caller — the follower's client
    /// then falls back to the in-process / quorum-unavailable path.
    command_sinks: Arc<RwLock<HashMap<NodeId, Arc<dyn CommandSink>>>>,
    /// This service's owning node id (used to resolve the sink).
    node_id: NodeId,
    /// Pacing for a snapshot stream, from `snapshot_transfer_rate_bps`
    /// (0 = unlimited).
    snapshot_rate_bps: u64,
    /// How much of a snapshot one chunk may carry.
    ///
    /// Derived from the transport's message cap: a chunk is a gRPC message, so a
    /// chunk larger than `max_message_size` cannot cross at all — the server
    /// would fail to encode it and the client to decode it. (That is precisely
    /// how the original 256 KiB constant broke every streamed transfer on a
    /// transport configured below it.)
    snapshot_chunk_bytes: usize,
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
        command_sinks: Arc<RwLock<HashMap<NodeId, Arc<dyn CommandSink>>>>,
        node_id: NodeId,
        snapshot_rate_bps: u64,
        max_message_size: usize,
    ) -> Self {
        Self {
            cluster_id,
            protocol_major,
            protocol_minor,
            rejects,
            inbound,
            snapshot_provider,
            command_sinks,
            node_id,
            snapshot_rate_bps,
        // Half the cap leaves room for gRPC and protobuf framing, which are
        // part of the same message budget.
        snapshot_chunk_bytes: (max_message_size / 2).clamp(1, SNAPSHOT_CHUNK_BYTES),
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
    chunk_bytes: usize,
    tx: Sender<Result<SnapshotChunk, Status>>,
) {
    let mut remaining = reader.len;
    let mut reader = reader.reader;
    let mut limiter = RateLimiter::new(rate_bps, chunk_bytes as u64);
    let mut buf = vec![0u8; chunk_bytes];

    while remaining > 0 {
        let want = remaining.min(chunk_bytes as u64) as usize;
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
        let chunk_bytes = self.snapshot_chunk_bytes;
        tokio::spawn(async move {
            stream_snapshot(reader, rate, chunk_bytes, tx).await;
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

    /// Run a forwarded client command against this node's runtime (propsol
    /// v0.2.19). Mirrors [`send`] in that the handshake is validated first and
    /// the sender's identity comes from it; on top, the command is dispatched to
    /// the injected [`CommandSink`], and its outcome — or its (possibly fresh)
    /// leader hint — is returned over the wire.
    async fn forward(&self, request: Request<ForwardRequest>) -> Result<Response<ForwardReply>, Status> {
        let request = request.into_inner();
        let mut reply = ForwardReply {
            ok: false,
            error_code: String::new(),
            value: Vec::new(),
            leader_node_id: String::new(),
            leader_addr: String::new(),
            result: None,
            commit_index: None,
            multi_get: None,
        };

        // A missing handshake is rejected *unconditionally*, exactly as for `Send`.
        let Some(hello) = request.hello else {
            self.rejects.fetch_add(1, Ordering::Relaxed);
            reply.error_code = ERR_HELLO_MISSING.to_string();
            return Ok(Response::new(reply));
        };

        // Handshake first: reject (and count) before the command is touched.
        if let Some(code) = validate_hello(
            &hello,
            &self.cluster_id,
            self.protocol_major,
            self.protocol_minor,
        ) {
            self.rejects.fetch_add(1, Ordering::Relaxed);
            reply.error_code = code.to_string();
            return Ok(Response::new(reply));
        };

        // Look up the sink at request time (not start time) so the facade can
        // register it right after `Runtime::new`. A missing entry answers
        // `unavailable` rather than hang the caller.
        let sink = match self.command_sinks.read().unwrap().get(&self.node_id) {
            Some(sink) => sink.clone(),
            None => {
                reply.ok = false;
                reply.error_code = "unavailable".to_string();
                return Ok(Response::new(reply));
            }
        };
        // Build the seam command from the wire fields. The sender's
        // session envelope (`client_id` / `seq_no`) rides the original
        // values — never minted here — so the leader dedups idempotently
        // (propsol INV5).
        let forward_command = match request.kind {
            // 0 = propose (write): payload in `cmd`, session in `client_id`/`seq_no`.
            0 => ForwardCommand::Propose {
                cmd: request.cmd,
                client_id: request.client_id,
                seq_no: request.seq_no,
            },
            // 1 = read (ReadIndex); 2 = stale read. Both carry a key.
            1 => ForwardCommand::Read { key: request.key },
            2 => ForwardCommand::GetStale { key: request.key },
            // 3 = multi_get: the keys ride `keys` in request order and the
            // requested consistency rides `linearizable`.
            3 => ForwardCommand::MultiGet {
                keys: request.keys,
                linearizable: request.linearizable,
            },
            // 4 = propose_with_index: the same write payload/session fields as
            // kind=0, but the reply additionally carries the applied log index.
            4 => ForwardCommand::ProposeWithIndex {
                cmd: request.cmd,
                client_id: request.client_id,
                seq_no: request.seq_no,
            },
            // Any unknown kind is rejected *before* the sink runs, so
            // `unknown_kind` honestly means "nothing was executed".
            _ => {
                reply.ok = false;
                reply.error_code = "unknown_kind".to_string();
                return Ok(Response::new(reply));
            }
        };

        match sink.sink(forward_command).await {
            Ok(ForwardOutcome {
                value,
                leader_hint,
                result,
                multi,
                index,
            }) => {
                // A hint means this node is not the (current) leader:
                // report `not_leader` with a fresh hint so the caller can
                // re-redirect, rather than a bare failure.
                if let Some((leader_id, leader_addr)) = leader_hint {
                    reply.ok = false;
                    reply.error_code = "not_leader".to_string();
                    reply.leader_node_id = leader_id.as_str().to_string();
                    reply.leader_addr = leader_addr.to_string();
                } else {
                    reply.ok = true;
                    // `value` is the seam's `Option<Vec<u8>>` read result;
                    // the wire `ForwardReply.value` is a plain `Vec<u8>`,
                    // so unwrap to an empty vector when the read has no
                    // value.
                    reply.value = value.unwrap_or_default();
                    // M1: the apply outcome of a successful write rides the
                    // `result` envelope (an explicit-presence message, so "no
                    // result" for a read / old node is distinguishable from a
                    // genuinely empty value). `ApplyOutcome::Value(v)` is
                    // encoded with the envelope present — including an empty
                    // `v` — while `ApplyOutcome::None` (delete / no-op) is
                    // encoded with the envelope *absent*, which is also what a
                    // read and an old node produce. This is the documented
                    // collapse (seam `ForwardOutcome` doc): `None` and "no
                    // outcome" are indistinguishable over the wire.
                    reply.result = encode_forward_result(result);
                    // Indexed writes: the applied log index of an entry that
                    // established the write. Every other kind leaves it absent
                    // (`None`), which is also what an old node sends.
                    reply.commit_index = encode_commit_index(index);
                    // Batched reads: one entry per requested key plus the
                    // applied index the values were observed at. Every other
                    // kind leaves the field absent (`None`).
                    reply.multi_get = encode_multi_get(multi);
                }
            }
            // `Err(code)` carries the stable code produced by the sink
            // (`quorum_unavailable`, `timeout`, `busy`, `session_expired`,
            // `session_table_full`, `shutting_down`, …); `not_leader` is
            // always returned *with* a hint, so it never lands here.
            Err(code) => {
                reply.ok = false;
                reply.error_code = code;
            }
        }
        Ok(Response::new(reply))
    }
}

/// Encode a seam apply outcome into the wire `ForwardResult` envelope (M1).
///
/// Presence semantics: `Value(v)` — including an empty `v` — is encoded with
/// the envelope present; `ApplyOutcome::None` (delete / no-op) and "no
/// outcome" (a read) are both encoded as the *absent* envelope, which is the
/// documented collapse (see the seam `ForwardOutcome` doc). A `CasFailed`
/// outcome (M3) is a present envelope carrying the state the compare saw. The
/// match is written **exhaustively** over `ApplyOutcome`: a new variant fails
/// this match at compile time, forcing its wire encoding to be decided here
/// rather than silently collapsing into "no outcome".
fn encode_forward_result(result: Option<ApplyOutcome>) -> Option<ForwardResult> {
    match result {
        Some(ApplyOutcome::Value(v)) => Some(ForwardResult {
            kind: Some(forward_result::Kind::Value(v)),
        }),
        Some(ApplyOutcome::CasFailed {
            current_index,
            current_value,
        }) => Some(ForwardResult {
            kind: Some(forward_result::Kind::CasFailed(CasFailed {
                current_index,
                current_value: current_value.map(|data| BytesValue { data }),
            })),
        }),
        Some(ApplyOutcome::None) | None => None,
        // Unreachable in practice: a `Conflict` outcome is turned into
        // `Err(ArachneError::SessionConflict)` by the actor's write-back reply
        // (`PendingReply::send_applied`), so the command sink surfaces it as
        // `Err("session_conflict")` (the lossy wire code) and never builds a
        // `ForwardOutcome` carrying a `Conflict` outcome. Kept for exhaustiveness
        // (a new variant must be decided here explicitly, not silently collapse).
        Some(ApplyOutcome::Conflict) => None,
    }
}

/// Encode a seam write index into the wire `commit_index` field.
///
/// Presence is the contract: `Some(i)` becomes a present `optional uint64`
/// (a real applied-write index), while `None` stays absent so an old /
/// non-indexed reply is distinguishable from a fabricated `0`.
fn encode_commit_index(index: Option<LogIndex>) -> Option<u64> {
    index
}

/// Encode a seam batched-read outcome into the wire `MultiGetReply`.
///
/// An absent `multi` (a read/write, any failure, or a non-`multi_get` command)
/// stays absent on the wire. A present result becomes one `OptionalBytesValue`
/// per requested key, in order: `Some(data)` — including a present *empty*
/// value — keeps `data` present, while `None` leaves it absent. The wrapper's
/// message presence is what distinguishes those two cases on the wire.
fn encode_multi_get(multi: Option<(Vec<Option<Vec<u8>>>, LogIndex)>) -> Option<MultiGetReply> {
    multi.map(|(values, applied_index)| MultiGetReply {
        values: values
            .into_iter()
            .map(|data| OptionalBytesValue { data })
            .collect(),
        applied_index,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{forward_result, BytesValue, CasFailed, ForwardResult};

    /// Encoding preserves the presence distinction the decode side relies on:
    /// `Value(v)` and empty-`Value` are present; `CasFailed` is a present
    /// envelope whose branch carries the current state; `None` and "no
    /// outcome" are absent (and therefore indistinguishable over the wire, by
    /// design).
    #[test]
    fn encode_distinguishes_presence_shapes() {
        // A value outcome is a present envelope carrying the value.
        assert_eq!(
            encode_forward_result(Some(ApplyOutcome::Value(b"v".to_vec()))),
            Some(ForwardResult {
                kind: Some(forward_result::Kind::Value(b"v".to_vec()))
            })
        );
        // An empty value is still a present envelope (empty interior).
        assert_eq!(
            encode_forward_result(Some(ApplyOutcome::Value(Vec::new()))),
            Some(ForwardResult {
                kind: Some(forward_result::Kind::Value(Vec::new()))
            })
        );
        // A `CasFailed` outcome is a present envelope carrying the current
        // state (absent `current_value` = absent key).
        assert_eq!(
            encode_forward_result(Some(ApplyOutcome::CasFailed {
                current_index: 7,
                current_value: Some(b"cur".to_vec()),
            })),
            Some(ForwardResult {
                kind: Some(forward_result::Kind::CasFailed(CasFailed {
                    current_index: 7,
                    current_value: Some(BytesValue { data: b"cur".to_vec() }),
                }))
            })
        );
        // `None` outcome and absent outcome are both the absent envelope.
        assert_eq!(encode_forward_result(Some(ApplyOutcome::None)), None);
        assert_eq!(encode_forward_result(None), None);
    }

    /// All per-element presence shapes encode without loss: present non-empty,
    /// present empty (data present), and absent (data absent).
    #[test]
    fn encode_multi_get_preserves_presence_shapes() {
        // Present, non-empty.
        assert_eq!(
            encode_multi_get(Some((vec![Some(b"value".to_vec())], 11))),
            Some(MultiGetReply {
                values: vec![OptionalBytesValue {
                    data: Some(b"value".to_vec()),
                }],
                applied_index: 11,
            })
        );

        // Present but empty: `data` stays present so the decoder can tell it
        // apart from an absent key.
        assert_eq!(
            encode_multi_get(Some((vec![Some(Vec::new())], 12))),
            Some(MultiGetReply {
                values: vec![OptionalBytesValue {
                    data: Some(Vec::new()),
                }],
                applied_index: 12,
            })
        );

        // Absent key: the element exists, `data` does not.
        assert_eq!(
            encode_multi_get(Some((vec![None], 13))),
            Some(MultiGetReply {
                values: vec![OptionalBytesValue { data: None }],
                applied_index: 13,
            })
        );
    }

    /// A mixed batch encodes in request order with the applied index; missing
    /// keys become entries whose `data` is absent.
    #[test]
    fn encode_multi_get_preserves_mixed_batch_and_index() {
        assert_eq!(
            encode_multi_get(Some((
                vec![
                    Some(b"first".to_vec()),
                    None,
                    Some(Vec::new()),
                    Some(b"fourth".to_vec()),
                ],
                42,
            ))),
            Some(MultiGetReply {
                values: vec![
                    OptionalBytesValue {
                        data: Some(b"first".to_vec()),
                    },
                    OptionalBytesValue { data: None },
                    OptionalBytesValue {
                        data: Some(Vec::new()),
                    },
                    OptionalBytesValue {
                        data: Some(b"fourth".to_vec()),
                    },
                ],
                applied_index: 42,
            })
        );
    }

    /// An absent batched-read result stays absent on the wire.
    #[test]
    fn encode_multi_get_absent_is_absent() {
        assert_eq!(encode_multi_get(None), None);
    }

    /// `commit_index` presence round-trips: a real index is present, while
    /// `None` stays absent (never encoded as `0`).
    #[test]
    fn encode_commit_index_preserves_presence() {
        assert_eq!(encode_commit_index(Some(42)), Some(42));
        assert_eq!(encode_commit_index(None), None);
    }
}
