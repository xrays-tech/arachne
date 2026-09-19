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
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

use arachne_seam::seam::{Transport, TransportMessage};
use arachne_seam::types::NodeId;
use tonic::transport::{Channel, Endpoint};

use crate::error::TransportError;
use crate::factory::TransportConfig;
use crate::io::{TokioIoProvider, TransportIo};
use crate::proto::raft_transport_client::RaftTransportClient;
use crate::proto::{Hello, RaftEnvelope};
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
    ) -> Self {
        Self {
            addresses,
            self_id,
            hello,
            channels: Arc::new(Mutex::new(HashMap::new())),
            config,
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
