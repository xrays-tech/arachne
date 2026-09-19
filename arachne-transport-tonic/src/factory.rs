//! [`TonicTransportFactory`]: mints the transport halves for each node of a
//! cluster and owns the shared cluster identity.
//!
//! One factory serves a whole cluster. It is built with the cluster identity
//! (cluster id, protocol version, feature flags) and a map of
//! `NodeId → listen address`. For every node it pre-creates an inbound channel:
//!
//! * [`create`](TransportFactory::create) hands a node its outbound
//!   [`TonicTransport`] (with the shared address map and its own handshake) and
//!   its inbound [`TonicRx`].
//! * [`start`](Self::start) binds one listener per node (a `:0` address yields
//!   an ephemeral port), records each node's *real* bound address in the shared
//!   map, and runs a gRPC server that validates the handshake and feeds that
//!   node's inbound channel.
//! * [`shutdown`](Self::shutdown) stops the servers and closes the inbound
//!   channels, so each [`TonicRx::recv`] resolves to `None`.
//!
//! The address map is shared (and updated by [`Self::start`] with the real
//! post-bind addresses), so transports always resolve peers to the address a
//! node actually listens on — even when `:0` was used to obtain an ephemeral
//! port.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use arachne_seam::seam::{TransportFactory, TransportMessage};
use arachne_seam::types::NodeId;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::error::TransportError;
use crate::handshake::build_hello;
use crate::proto::raft_transport_server::RaftTransportServer;
use crate::rx::TonicRx;
use crate::server::RaftTransportService;
use crate::transport::TonicTransport;
use crate::unlock_read;
use crate::unlock_write;

/// A message in the inbound channel: who sent it (the trusted handshake tag)
/// and the payload.
type Inbound = (NodeId, TransportMessage);

struct FactoryInner {
    cluster_id: String,
    protocol_major: u32,
    protocol_minor: u32,
    feature_flags: Vec<String>,
    /// Shared cluster address map. Populated at construction (possibly with
    /// `:0` placeholders) and updated by [`TonicTransportFactory::start`] with
    /// the real post-bind addresses.
    addresses: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
    /// Per-node inbound senders (used by each node's gRPC server).
    senders: Mutex<HashMap<NodeId, UnboundedSender<Inbound>>>,
    /// Per-node inbound receivers (handed out by `create`).
    receivers: Mutex<HashMap<NodeId, UnboundedReceiver<Inbound>>>,
    /// Cluster-wide handshake-rejection counter.
    rejects: Arc<AtomicU64>,
    /// Live server handles, set by `start`, consumed by `shutdown`.
    servers: Mutex<Option<Vec<CancellationToken>>>,
}

/// Mints the tonic transport halves for the nodes of a cluster.
#[derive(Clone)]
pub struct TonicTransportFactory {
    inner: Arc<FactoryInner>,
}

impl TonicTransportFactory {
    /// Create a factory for a cluster.
    ///
    /// `addresses` maps every node in the cluster to the address it should
    /// listen on (a `:0` port allocates an ephemeral one; the real address is
    /// learned at [`Self::start`]). An inbound channel is pre-created for each.
    pub fn new(
        cluster_id: impl Into<String>,
        protocol_major: u32,
        protocol_minor: u32,
        feature_flags: Vec<String>,
        addresses: HashMap<NodeId, SocketAddr>,
    ) -> Self {
        let cluster_id = cluster_id.into();
        // Pre-create an inbound channel for every node (before the map is
        // wrapped in the shared `RwLock`).
        let node_ids: Vec<NodeId> = addresses.keys().cloned().collect();
        let addresses = Arc::new(RwLock::new(addresses));

        let mut senders = HashMap::new();
        let mut receivers = HashMap::new();
        for node in node_ids {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Inbound>();
            senders.insert(node.clone(), tx);
            receivers.insert(node.clone(), rx);
        }

        Self {
            inner: Arc::new(FactoryInner {
                cluster_id,
                protocol_major,
                protocol_minor,
                feature_flags,
                addresses,
                senders: Mutex::new(senders),
                receivers: Mutex::new(receivers),
                rejects: Arc::new(AtomicU64::new(0)),
                servers: Mutex::new(None),
            }),
        }
    }

    /// Bind one listener per node and start serving.
    ///
    /// For each node the configured listen address is bound (a `:0` port
    /// resolves to an ephemeral one); the node's *real* bound address is written
    /// back into the shared map so peers resolve it correctly. A gRPC server is
    /// then spawned to validate the handshake and forward accepted payloads to
    /// the node's inbound channel.
    pub async fn start(&self) -> Result<(), TransportError> {
        // Snapshot the (possibly placeholder) listen addresses up front.
        let listen: Vec<(NodeId, SocketAddr)> = unlock_read(&self.inner.addresses)
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();

        let mut tokens = Vec::new();
        for (node, addr) in listen {
            let listener = tokio::net::TcpListener::bind(addr).await.map_err(TransportError::Bind)?;
            let real_addr = listener.local_addr().map_err(TransportError::Bind)?;

            // Record the real address so the shared map always reflects where a
            // node actually listens (this is what makes `:0` usable).
            unlock_write(&self.inner.addresses).insert(node.clone(), real_addr);

            // A node with no inbound channel cannot receive; skip it.
            let Some(sender) = crate::unlock(&self.inner.senders).get(&node).cloned() else {
                continue;
            };
            let service = RaftTransportService::new(
                self.inner.cluster_id.clone(),
                self.inner.protocol_major,
                self.inner.protocol_minor,
                Arc::clone(&self.inner.rejects),
                sender,
            );
            let cancel = CancellationToken::new();
            let server = Server::builder()
                .add_service(RaftTransportServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener));

            // Serve until the cancel token fires; then the server future is
            // dropped, stopping it and closing the listener.
            let server_cancel = cancel.clone();
            tokio::spawn(async move {
                tokio::select! {
                    biased;
                    _ = server => {}
                    _ = server_cancel.cancelled() => {}
                }
            });
            tokens.push(cancel);
        }
        *crate::unlock(&self.inner.servers) = Some(tokens);
        Ok(())
    }

    /// Gracefully stop every server and close the inbound channels, so each
    /// [`TonicRx::recv`] resolves to `None`.
    pub async fn shutdown(&self) {
        if let Some(tokens) = crate::unlock(&self.inner.servers).take() {
            for cancel in tokens {
                cancel.cancel();
            }
        }
        // Drop the factory's sender clones so every inbound channel closes.
        crate::unlock(&self.inner.senders).clear();
    }

    /// The number of handshake rejections (cluster-id or protocol mismatches)
    /// seen across the whole cluster.
    pub fn handshake_rejections(&self) -> u64 {
        self.inner.rejects.load(Ordering::SeqCst)
    }
}

impl TransportFactory for TonicTransportFactory {
    type Tx = TonicTransport;
    type Rx = TonicRx;

    fn create(&self, me: NodeId) -> (Self::Tx, Self::Rx) {
        let hello = build_hello(
            &self.inner.cluster_id,
            me.as_str(),
            self.inner.protocol_major,
            self.inner.protocol_minor,
            &self.inner.feature_flags,
        );
        let tx = TonicTransport::new(Arc::clone(&self.inner.addresses), me.clone(), hello);

        let rx = match crate::unlock(&self.inner.receivers).remove(&me) {
            Some(receiver) => TonicRx::new(receiver),
            // A node not present in the address map has no inbound channel to
            // receive on: hand it a channel that is already closed, so `recv`
            // reports `None` immediately and no messages can ever arrive.
            None => {
                let (_tx, receiver) = tokio::sync::mpsc::unbounded_channel();
                TonicRx::new(receiver)
            }
        };
        (tx, rx)
    }
}
