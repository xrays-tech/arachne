//! The outbound half of the tonic transport: [`TonicTransport`].
//!
//! A `TonicTransport` sends raft payloads to peers over plaintext gRPC. For each
//! destination it lazily opens a tonic [`Channel`] and reuses it for subsequent
//! sends. Every send carries the sender's handshake [`Hello`]; the result is
//! mapped to a typed [`TransportError`].
//!
//! Local sends (to this node's own [`NodeId`]) never touch the network — they are
//! resolved by the node itself.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};

use arachne_kv_seam::seam::{
    ApplyOutcome, ForwardCommand, ForwardOutcome, ForwardTransport, RemoteForwarder, Transport,
    TransportMessage,
};
use arachne_kv_seam::types::NodeId;
use tokio::io::AsyncWriteExt;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

use crate::error::TransportError;
use crate::factory::TransportConfig;
use crate::io::{TokioIoProvider, TransportIo};
use crate::proto::raft_transport_client::RaftTransportClient;
use crate::proto::{
    forward_result, ForwardReply, ForwardRequest, ForwardResult, Hello, RaftEnvelope, SnapshotRequest,
};
use crate::unlock;
use crate::unlock_read;

/// A cached channel to one peer, keyed by the peer's [`NodeId`].
type ChannelCache = HashMap<NodeId, Channel>;

/// The shared cluster `NodeId → SocketAddr` map. A `RwLock` because the factory
/// rewrites it at [`crate::TonicTransportFactory::start`] with the real
/// post-bind addresses (e.g. when `:0` allocated an ephemeral port).
type AddressMap = Arc<RwLock<HashMap<NodeId, SocketAddr>>>;

/// The outbound half for one node.
///
/// Generic over the I/O seam `Io` (default [`TokioIoProvider`], i.e. real tokio
/// TCP). The `connector` field is the seam's client-side dialer; in production
/// it is [`crate::io::TcpConnector`], and in M1 stage 3b a deterministic
/// simulator.
#[derive(Clone)]
pub struct TonicTransport<Io: TransportIo = TokioIoProvider> {
    /// The full cluster `NodeId → SocketAddr` map (shared across all nodes).
    addresses: AddressMap,
    /// This node's own id; sends to it are short-circuited (never go over the
    /// network — the node handles its own messages).
    self_id: NodeId,
    /// The sender's handshake, attached to every outbound envelope.
    hello: Hello,
    /// Lazily-opened channels, one per peer.
    channels: Arc<Mutex<ChannelCache>>,
    /// Client-side transport knobs (timeouts, keep-alive, message size),
    /// resolved from the factory's config at construction.
    config: TransportConfig,
    /// Whether this node can *serve* snapshots — i.e. whether the factory had a
    /// provider when this transport was minted (rev T).
    ///
    /// It gates the streaming capability deliberately: a node that could fetch
    /// but not serve would make the leader send metadata-only snapshots that its
    /// peers can never complete, so the safe default (no provider) keeps the
    /// pre-streaming path exactly as it was.
    serves_snapshots: bool,
    /// The client-side connector used to dial peers.
    connector: <Io as TransportIo>::Connector,
}

impl<Io: TransportIo> TonicTransport<Io> {
    /// Build a transport for the node `self_id`. `connector` is the client-side
    /// I/O connector (from the factory's seam), `addresses` the shared cluster
    /// address map, `hello` the sender's handshake, and `config` the resolved
    /// client-side transport knobs.
    pub(crate) fn new(
        connector: <Io as TransportIo>::Connector,
        addresses: AddressMap,
        self_id: NodeId,
        hello: Hello,
        config: TransportConfig,
        serves_snapshots: bool,
    ) -> Self {
        Self {
            addresses,
            self_id,
            hello,
            channels: Arc::new(Mutex::new(HashMap::new())),
            config,
            serves_snapshots,
            connector,
        }
    }
}

impl<Io: TransportIo> Transport for TonicTransport<Io> {
    type Error = TransportError;

    async fn send(&self, to: NodeId, msg: TransportMessage) -> Result<(), Self::Error> {
        // Early exit: a message addressed to ourselves is the node's to handle;
        // it must never traverse the wire.
        if to == self.self_id {
            return Ok(());
        }

        // Resolve the destination address. An unknown peer is a clear, typed
        // failure (the node keeps retransmitting; raft tolerates it).
        let Some(addr) = unlock_read(&self.addresses).get(&to).copied() else {
            return Err(TransportError::UnknownPeer(to));
        };

        let TransportMessage::Raft(payload) = msg else {
            return Err(TransportError::UnsupportedMessage);
        };

        let channel =
            get_or_connect::<Io>(&self.channels, &self.connector, &to, addr, self.config).await?;

        let envelope = RaftEnvelope {
            payload,
            hello: Some(self.hello.clone()),
        };
        // The per-request timeout lives on the channel (see `get_or_connect`),
        // so a hung peer cannot stall the core's `step()` — the `send` future
        // resolves to `DeadlineExceeded`, mapped to `TransportError::Send`
        // below. The message-size cap is set here so an oversized payload fails
        // fast on the client too (F3).
        let mut client = RaftTransportClient::new(channel)
            .max_decoding_message_size(self.config.max_message_size)
            .max_encoding_message_size(self.config.max_message_size);
        let response = client
            .send(tonic::Request::new(envelope))
            .await
            .map_err(TransportError::Send)?;

        let reply = response.into_inner();
        if reply.accepted {
            Ok(())
        } else {
            Err(TransportError::from_error_code(&reply.error_code))
        }
    }

    fn supports_snapshot_streaming(&self) -> bool {
        // Only when the node can also serve them (see `serves_snapshots`).
        self.serves_snapshots
    }

    /// Fetch a snapshot from `from` and write it to `dest` (propsol rev T).
    ///
    /// Deliberately **not** using the cached send channel: that channel carries
    /// a per-request timeout sized for one raft message (5s by default), and a
    /// paced snapshot of tens of megabytes legitimately takes far longer — the
    /// timeout would abort a healthy transfer. A snapshot fetch is rare and
    /// long-lived, so it opens its own connection (connect timeout and
    /// keep-alive still apply) rather than polluting the send path's cache.
    async fn fetch_snapshot(
        &self,
        from: NodeId,
        index: u64,
        term: u64,
        dest: &std::path::Path,
    ) -> Result<Option<u64>, Self::Error> {
        let Some(addr) = unlock_read(&self.addresses).get(&from).copied() else {
            return Err(TransportError::UnknownPeer(from));
        };

        let endpoint = Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| TransportError::SnapshotStream(e.to_string()))?
            .connect_timeout(self.config.connect_timeout)
            .http2_keep_alive_interval(self.config.keep_alive_interval)
            .keep_alive_timeout(self.config.keep_alive_timeout)
            .keep_alive_while_idle(true);
        let channel = endpoint
            .connect_with_connector(self.connector.clone())
            .await
            .map_err(|e| TransportError::SnapshotStream(e.to_string()))?;

        let mut client = RaftTransportClient::new(channel)
            .max_decoding_message_size(self.config.max_message_size);
        let request = SnapshotRequest {
            index,
            term,
            hello: Some(self.hello.clone()),
        };
        let mut stream = client
            .fetch_snapshot(tonic::Request::new(request))
            .await
            .map_err(TransportError::Send)?
            .into_inner();

        // Assemble into a fresh file. The caller validates the snapshot's own
        // CRC afterwards (I9) and only then installs it, so a truncated or
        // corrupted stream is caught rather than applied.
        let mut file = tokio::fs::File::create(dest)
            .await
            .map_err(|e| TransportError::SnapshotStream(e.to_string()))?;
        let mut written: u64 = 0;
        loop {
            match stream.message().await {
                Ok(Some(chunk)) => {
                    file.write_all(&chunk.data)
                        .await
                        .map_err(|e| TransportError::SnapshotStream(e.to_string()))?;
                    written += chunk.data.len() as u64;
                }
                Ok(None) => break,
                Err(status) => return Err(TransportError::Send(status)),
            }
        }
        file.sync_all()
            .await
            .map_err(|e| TransportError::SnapshotStream(e.to_string()))?;
        Ok(Some(written))
    }
}

/// Client-side forwarder for [`ForwardTransport`]: dials the leader over gRPC
/// and carries a forwarded client command to its runtime. It reuses the same
/// per-peer channel cache as the raft `send` path (same connector, same client
/// knobs, same `get_or_connect` channel reuse), so the forward is no more
/// expensive than a raft message.
struct Forwarder<Io: TransportIo> {
    /// A clone of the owning transport (shares the address map, channel cache,
    /// handshake and client knobs). The clone is `Clone` and cheap to make.
    transport: TonicTransport<Io>,
}

impl<Io: TransportIo> Forwarder<Io> {
    /// Carry `c` to the leader at `addr` (the hint), mapping the reply to a
    /// `ForwardOutcome`. A `not_leader` reply re-emits a *fresh* hint so the
    /// caller can re-redirect; any other stable code is returned as an `Err`.
    async fn do_forward(&self, to: NodeId, addr: SocketAddr, c: ForwardCommand) -> Result<ForwardOutcome, String> {
        // Build the wire request. The sender's handshake rides the outbound hello
        // (this node's handshake, unchanged — it identifies the *origin* of the
        // command); the leader's dedup uses `client_id`/`seq_no` from `c`, which
        // were minted by the originating client and never changed in transit.
        let mut req = ForwardRequest {
            kind: 0,
            cmd: Vec::new(),
            key: Vec::new(),
            client_id: 0,
            seq_no: 0,
            hello: Some(self.transport.hello.clone()),
        };
        match c {
            // 0 = propose (write).
            ForwardCommand::Propose { cmd, client_id, seq_no } => {
                req.kind = 0;
                req.cmd = cmd;
                req.client_id = client_id;
                req.seq_no = seq_no;
            }
            // 1 = read (ReadIndex).
            ForwardCommand::Read { key } => {
                req.kind = 1;
                req.key = key;
            }
            // 2 = stale read.
            ForwardCommand::GetStale { key } => {
                req.kind = 2;
                req.key = key;
            }
        }

        // Reuse the cached channel to the leader (keyed by `to`), or open a new
        // one. A `not_leader` hint always carries the leader's *current* address,
        // so dialing `addr` is safe even if the node's map is momentarily stale.
        let channel = get_or_connect::<Io>(
            &self.transport.channels,
            &self.transport.connector,
            &to,
            addr,
            self.transport.config,
        )
        .await
        .map_err(|e| e.to_string())?;

        let mut client = RaftTransportClient::new(channel)
            .max_decoding_message_size(self.transport.config.max_message_size)
            .max_encoding_message_size(self.transport.config.max_message_size);
        let response: tonic::Response<ForwardReply> = client
            .forward(Request::new(req))
            .await
            .map_err(|e: tonic::Status| e.to_string())?;

        let reply = response.into_inner();
        if reply.ok {
            // A successful forward: `value` is the read's answer (empty for a
            // write) and there is no new hint. The `result` envelope is the
            // write's apply outcome: its *presence* distinguishes an outcome
            // from its absence, and an empty interior `Value(vec![])` is a
            // genuine value outcome (a put of an empty value), not a collapse
            // with `None` — the server encodes `Value(v)` with the envelope
            // present and `None`/no-outcome as absent.
            let result = decode_forward_result(reply.result);
            Ok(ForwardOutcome {
                value: Some(reply.value),
                leader_hint: None,
                result,
            })
        } else if reply.error_code == "not_leader" {
            // The leader redirected: hand back a fresh hint so the caller can
            // re-feed it into its redirect loop. No value to carry.
            let leader_addr: SocketAddr = reply
                .leader_addr
                .parse()
                .map_err(|_| "bad leader address".to_string())?;
            Ok(ForwardOutcome {
                value: None,
                leader_hint: Some((NodeId::new(reply.leader_node_id), leader_addr)),
                result: None,
            })
        } else {
            // A stable code that carries no hint (quorum_unavailable, timeout,
            // busy, session_expired, session_table_full, shutting_down,
            // unavailable, …). Return it so the client maps it to its typed error.
            Err(reply.error_code)
        }
    }
}

impl<Io: TransportIo> RemoteForwarder for Forwarder<Io> {
    fn forward(
        &self,
        to: NodeId,
        addr: SocketAddr,
        c: ForwardCommand,
    ) -> Pin<Box<dyn Future<Output = Result<ForwardOutcome, String>> + Send + '_>> {
        let transport = self.transport.clone();
        Box::pin(async move {
            Forwarder { transport }.do_forward(to, addr, c).await
        })
    }
}

impl<Io: TransportIo> ForwardTransport for TonicTransport<Io> {
    /// This node *can* forward commands (it has a real gRPC client). The forwarder
    /// is minted on demand; it reuses the transport's shared channel cache.
    fn forwarder(&self) -> Option<Arc<dyn RemoteForwarder>> {
        Some(Arc::new(Forwarder {
            transport: self.clone(),
        }))
    }
}

/// Return a channel to `to` (at `addr`), reusing a cached one or opening a new
/// connection (and caching it) if none exists.
///
/// Every channel is built with a connect timeout, a per-request timeout, and
/// HTTP/2 keep-alive (F1): a peer that accepts the connection but then hangs
/// must not stall the drive loop — keep-alive detects a dead connection and the
/// per-request timeout bounds any single `send`.
///
/// The `Mutex` is only held across the synchronous map lookup/insert, never
/// across the connect `await`, so a slow connect cannot block other sends.
async fn get_or_connect<Io: TransportIo>(
    channels: &Mutex<ChannelCache>,
    connector: &Io::Connector,
    to: &NodeId,
    addr: SocketAddr,
    config: TransportConfig,
) -> Result<Channel, TransportError> {
    {
        let cache = unlock(channels);
        if let Some(channel) = cache.get(to) {
            return Ok(channel.clone());
        }
    }

    let endpoint = Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| TransportError::Send(tonic::Status::unavailable(e.to_string())))?
        // Bound the connect phase so a black-holed peer cannot wedge the
        // channel cache forever.
        .connect_timeout(config.connect_timeout)
        // Bound every `send`; a timeout surfaces as `DeadlineExceeded`.
        .timeout(config.request_timeout)
        // Detect and reap dead connections: ping when idle, and give up on a
        // connection whose last ping went unanswered.
        .http2_keep_alive_interval(config.keep_alive_interval)
        .keep_alive_timeout(config.keep_alive_timeout)
        .keep_alive_while_idle(true);
    let channel = endpoint
        .connect_with_connector(connector.clone())
        .await
        .map_err(|e| TransportError::Send(tonic::Status::unavailable(e.to_string())))?;

    // Best-effort cache; a racing insert of an equivalent channel is harmless
    // (tonic channels share their underlying connection and are cheap to drop).
    unlock(channels).entry(to.clone()).or_insert_with(|| channel.clone());
    Ok(channel)
}

/// Decode the wire `result` envelope back into the apply outcome it carried.
///
/// Presence semantics (M1/M3): an *absent* envelope — a read, an old node that
/// predates the field, or a `None` outcome — decodes to `None`; a *present*
/// envelope is `Value` of its interior (including empty) unless it carries the
/// M3 `CasFailed` branch, which decodes to the failed-CAS outcome.
fn decode_forward_result(result: Option<ForwardResult>) -> Option<ApplyOutcome> {
    match result {
        Some(r) => match r.kind {
            Some(forward_result::Kind::Value(v)) => Some(ApplyOutcome::Value(v)),
            Some(forward_result::Kind::CasFailed(cas)) => Some(ApplyOutcome::CasFailed {
                current_index: cas.current_index,
                current_value: cas.current_value.map(|b| b.data),
            }),
            // A present envelope with no branch set never leaves a current
            // encoder; decode it as "no outcome" rather than inventing one.
            None => None,
        },
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{forward_result, BytesValue, CasFailed, ForwardResult};

    /// The wire shapes are distinguishable: `Value(v)`, empty-`Value`,
    /// `CasFailed`, and absent (`None` / old node).
    #[test]
    fn decode_distinguishes_presence_shapes() {
        // A present envelope carrying a value.
        assert_eq!(
            decode_forward_result(Some(ForwardResult {
                kind: Some(forward_result::Kind::Value(b"v".to_vec()))
            })),
            Some(ApplyOutcome::Value(b"v".to_vec()))
        );
        // A present envelope carrying an *empty* value is `Value(vec![])`,
        // not a collapse with the absent case.
        assert_eq!(
            decode_forward_result(Some(ForwardResult {
                kind: Some(forward_result::Kind::Value(Vec::new()))
            })),
            Some(ApplyOutcome::Value(Vec::new()))
        );
        // A `CasFailed` envelope decodes to the failed outcome, carrying the
        // state the compare observed (absent value = absent key).
        assert_eq!(
            decode_forward_result(Some(ForwardResult {
                kind: Some(forward_result::Kind::CasFailed(CasFailed {
                    current_index: 7,
                    current_value: Some(BytesValue { data: b"cur".to_vec() }),
                }))
            })),
            Some(ApplyOutcome::CasFailed {
                current_index: 7,
                current_value: Some(b"cur".to_vec()),
            })
        );
        // An absent envelope (old node / read / `None` outcome) is `None`.
        assert_eq!(decode_forward_result(None), None);
    }

    /// The round-trip through the wire encoding is lossless for the shapes
    /// the forward path actually produces.
    #[test]
    fn decode_maps_internal_outcome_shapes() {
        assert_eq!(
            decode_forward_result(Some(ForwardResult {
                kind: Some(forward_result::Kind::Value(b"x".to_vec()))
            })),
            Some(ApplyOutcome::Value(b"x".to_vec()))
        );
    }
}
