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
#[derive(Clone)]
pub struct TonicTransport {
    /// The full cluster `NodeId → SocketAddr` map (shared across all nodes).
    addresses: AddressMap,
    /// This node's own id; sends to it are short-circuited (never go over the
    /// network — the node handles its own messages).
    self_id: NodeId,
    /// The sender's handshake, attached to every outbound envelope.
    hello: Hello,
    /// Lazily-opened channels, one per peer.
    channels: Arc<Mutex<ChannelCache>>,
}

impl TonicTransport {
    /// Build a transport for the node `self_id`. `addresses` is the shared
    /// cluster address map and `hello` the sender's handshake.
    pub(crate) fn new(addresses: AddressMap, self_id: NodeId, hello: Hello) -> Self {
        Self {
            addresses,
            self_id,
            hello,
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Transport for TonicTransport {
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

        let channel = get_or_connect(&self.channels, &to, addr).await?;

        let envelope = RaftEnvelope {
            payload,
            hello: Some(self.hello.clone()),
        };
        let mut client = RaftTransportClient::new(channel);
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
/// The `Mutex` is only held across the synchronous map lookup/insert, never
/// across the `connect().await`, so a slow connect cannot block other sends.
async fn get_or_connect(
    channels: &Mutex<ChannelCache>,
    to: &NodeId,
    addr: SocketAddr,
) -> Result<Channel, TransportError> {
    {
        let cache = unlock(channels);
        if let Some(channel) = cache.get(to) {
            return Ok(channel.clone());
        }
    }

    let endpoint = Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| TransportError::Send(tonic::Status::unavailable(e.to_string())))?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| TransportError::Send(tonic::Status::unavailable(e.to_string())))?;

    // Best-effort cache; a racing insert of an equivalent channel is harmless
    // (tonic channels share their underlying connection and are cheap to drop).
    unlock(channels).entry(to.clone()).or_insert_with(|| channel.clone());
    Ok(channel)
}
