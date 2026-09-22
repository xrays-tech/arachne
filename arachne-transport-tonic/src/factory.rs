//! [`TonicTransportFactory`]: mints the transport halves for each node of a
//! cluster and owns the shared cluster identity.
//!
//! One factory serves a whole cluster. It is built with the cluster identity
//! (cluster id, protocol version, feature flags) and a map of
//! `NodeId → listen address`. For every node it pre-creates a **bounded**
//! inbound channel:
//!
//! * [`create`](TransportFactory::create) hands a node its outbound
//!   [`TonicTransport`] (with the shared address map, its own handshake, and
//!   the resolved transport knobs) and its inbound [`TonicRx`].
//! * [`start`](Self::start) binds one listener per node (a `:0` address yields
//!   an ephemeral port), records each node's *real* bound address in the shared
//!   map, and runs a gRPC server that validates the handshake and feeds that
//!   node's inbound channel (answering `resource_exhausted` when the queue is
//!   full rather than blocking).
//! * [`shutdown`](Self::shutdown) stops the servers and *awaits their teardown*
//!   so that, once it returns, every inbound sender is dropped and each
//!   [`TonicRx::recv`] resolves to `None`.
//!
//! The address map is shared (and updated by [`Self::start`] with the real
//! post-bind addresses), so transports always resolve peers to the address a
//! node actually listens on — even when `:0` was used to obtain an ephemeral
//! port.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use arachne_seam::seam::{TransportFactory, TransportMessage};
use arachne_seam::types::NodeId;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::error::TransportError;
use crate::handshake::build_hello;
use crate::io::{TokioIoProvider, TransportIo};
use crate::proto::raft_transport_server::RaftTransportServer;
use crate::rx::TonicRx;
use crate::snapshot::SnapshotProvider;
use crate::server::RaftTransportService;
use crate::transport::TonicTransport;
use crate::unlock;
use crate::unlock_read;
use crate::unlock_write;

/// A message in the inbound channel: who sent it (the trusted handshake tag)
/// and the payload.
type Inbound = (NodeId, TransportMessage);

/// Capacity of each node's inbound queue. A fast peer cannot push more than
/// this many payloads ahead of a slow node: beyond that the server answers
/// `resource_exhausted` (raft retransmits) instead of blocking or growing
/// memory unboundedly.
pub(crate) const INBOUND_QUEUE_CAPACITY: usize = 1024;

/// Documented client-side defaults (F1): bound the connect phase and every
/// `send` so a hung peer cannot stall the core's drive loop.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// HTTP/2 keep-alive: ping a connection every 30s of idle, and reap it if the
/// last ping goes unanswered within 10s (detects half-dead connections).
const DEFAULT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(10);
/// Default gRPC message cap (F3): 8 MiB — headroom above the Lan preset's
/// 4 MiB `max_size_per_msg` — applied to both decoding and encoding on client
/// and server. See `proto/raft.proto` for the coupling.
const DEFAULT_MAX_MESSAGE_SIZE: usize = 8 * 1024 * 1024;

/// Client- and server-side transport knobs, resolved from the factory's
/// overrides (documented defaults when unset). `Copy` so `create`/`start` can
/// snapshot it without holding the guard.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TransportConfig {
    /// Bound the connect phase (F1).
    pub connect_timeout: Duration,
    /// Bound every `send` — the per-request timeout (F1).
    pub request_timeout: Duration,
    /// HTTP/2 keep-alive ping interval (sent when idle) (F1).
    pub keep_alive_interval: Duration,
    /// Give up on a connection whose last keep-alive ping went unanswered (F1).
    pub keep_alive_timeout: Duration,
    /// Max gRPC message size (decoding and encoding, client and server) (F3).
    pub max_message_size: usize,
    /// Pacing for an outgoing snapshot stream, in bytes/second (rev T). 0 =
    /// unlimited, which is the pre-streaming behaviour and the default here:
    /// the value comes from the profile (`snapshot_transfer_rate_bps`), which
    /// only the caller holds.
    pub snapshot_rate_bps: u64,
}

/// A started server: the token that stops it and the task that owns it.
///
/// Keeping the [`tokio::task::JoinHandle`] (not just the token) lets
/// [`TonicTransportFactory::shutdown`] *await* each server's teardown, which is
/// what guarantees the server's inbound-sender clone is dropped before
/// `shutdown` returns (and thus that every `TonicRx::recv` resolves `None`).
struct ServerHandle {
    /// Cancels the server task.
    cancel: CancellationToken,
    /// Joining this proves the server task (and its inbound sender clone) has
    /// been dropped.
    task: tokio::task::JoinHandle<()>,
}

struct FactoryInner<Io: TransportIo> {
    /// The I/O seam (real tokio TCP in production; a simulator in M1 stage 3b).
    io: Io,
    cluster_id: String,
    protocol_major: u32,
    protocol_minor: u32,
    feature_flags: Vec<String>,
    /// Shared cluster address map. Populated at construction (possibly with
    /// `:0` placeholders) and updated by [`TonicTransportFactory::start`] with
    /// the real post-bind addresses.
    addresses: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
    /// Per-node inbound senders (used by each node's gRPC server). Bounded.
    senders: Mutex<HashMap<NodeId, Sender<Inbound>>>,
    /// Per-node inbound receivers (handed out by `create`). Bounded.
    receivers: Mutex<HashMap<NodeId, Receiver<Inbound>>>,
    /// Cluster-wide handshake-rejection counter.
    rejects: Arc<AtomicU64>,
    /// Where snapshot bytes come from, when this node serves them (rev T).
    /// Set once before `start`; `None` makes `FetchSnapshot` answer
    /// `unavailable` instead of pretending the node can serve snapshots.
    snapshot_provider: Mutex<Option<Arc<dyn SnapshotProvider>>>,
    /// Live server handles, set by `start`, consumed by `shutdown`.
    servers: Mutex<Option<Vec<ServerHandle>>>,
    /// Resolved transport knobs; the optional setters overwrite individual
    /// fields before `start`/`create`.
    config: Mutex<TransportConfig>,
}

/// Mints the tonic transport halves for the nodes of a cluster.
///
/// Generic over the I/O seam `Io` (default [`TokioIoProvider`], i.e. real tokio
/// TCP). M1 stage 3b injects a deterministic simulator here; production code
/// and the public API are unchanged because the default is `TokioIoProvider`.
#[derive(Clone)]
pub struct TonicTransportFactory<Io: TransportIo = TokioIoProvider> {
    inner: Arc<FactoryInner<Io>>,
}

impl TonicTransportFactory<TokioIoProvider> {
    /// Create a factory for a cluster using the default I/O provider
    /// ([`TokioIoProvider`], real tokio TCP).
    ///
    /// `addresses` maps every node in the cluster to the address it should
    /// listen on (a `:0` port allocates an ephemeral one; the real address is
    /// learned at [`Self::start`]). A bounded inbound channel is pre-created for
    /// each. Transport knobs start at their documented defaults; override them
    /// with the optional setters *before* calling [`Self::start`]/[`Self::create`].
    ///
    /// For a custom I/O seam (e.g. a deterministic simulator) use
    /// [`TonicTransportFactory::with_io`] instead.
    pub fn new(
        cluster_id: impl Into<String>,
        protocol_major: u32,
        protocol_minor: u32,
        feature_flags: Vec<String>,
        addresses: HashMap<NodeId, SocketAddr>,
    ) -> Self {
        Self::with_io(
            TokioIoProvider,
            cluster_id,
            protocol_major,
            protocol_minor,
            feature_flags,
            addresses,
        )
    }
}

impl<Io: TransportIo> TonicTransportFactory<Io> {
    /// Create a factory for a cluster with an explicit I/O provider.
    ///
    /// This is the seam-aware constructor: it takes the [`TransportIo`]
    /// implementation (real tokio TCP or a deterministic simulator) and threads
    /// it through to both the server listeners ([`Self::start`]) and the client
    /// connectors (`create` → [`TonicTransport`]).
    pub fn with_io(
        io: Io,
        cluster_id: impl Into<String>,
        protocol_major: u32,
        protocol_minor: u32,
        feature_flags: Vec<String>,
        addresses: HashMap<NodeId, SocketAddr>,
    ) -> Self {
        let cluster_id = cluster_id.into();
        // Pre-create a bounded inbound channel for every node (before the map is
        // wrapped in the shared `RwLock`).
        let node_ids: Vec<NodeId> = addresses.keys().cloned().collect();
        let addresses = Arc::new(RwLock::new(addresses));

        let mut senders = HashMap::new();
        let mut receivers = HashMap::new();
        for node in node_ids {
            let (tx, rx) = tokio::sync::mpsc::channel::<Inbound>(INBOUND_QUEUE_CAPACITY);
            senders.insert(node.clone(), tx);
            receivers.insert(node.clone(), rx);
        }

        Self {
            inner: Arc::new(FactoryInner {
                io,
                cluster_id,
                protocol_major,
                protocol_minor,
                feature_flags,
                addresses,
                senders: Mutex::new(senders),
                receivers: Mutex::new(receivers),
                rejects: Arc::new(AtomicU64::new(0)),
            snapshot_provider: Mutex::new(None),
                servers: Mutex::new(None),
                config: Mutex::new(TransportConfig {
                    connect_timeout: DEFAULT_CONNECT_TIMEOUT,
                    request_timeout: DEFAULT_REQUEST_TIMEOUT,
                    keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL,
                    keep_alive_timeout: DEFAULT_KEEP_ALIVE_TIMEOUT,
                    max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
                    snapshot_rate_bps: 0,
                }),
            }),
        }
    }

    /// Override the connect timeout (F1). Call before [`Self::start`]/[`Self::create`].
    pub fn connect_timeout(&self, timeout: Duration) -> &Self {
        unlock(&self.inner.config).connect_timeout = timeout;
        self
    }

    /// Override the per-request `send` timeout (F1). Call before [`Self::start`]/[`Self::create`].
    pub fn request_timeout(&self, timeout: Duration) -> &Self {
        unlock(&self.inner.config).request_timeout = timeout;
        self
    }

    /// Override the HTTP/2 keep-alive interval (F1). Call before [`Self::start`]/[`Self::create`].
    pub fn keep_alive_interval(&self, interval: Duration) -> &Self {
        unlock(&self.inner.config).keep_alive_interval = interval;
        self
    }

    /// Override the HTTP/2 keep-alive timeout (F1). Call before [`Self::start`]/[`Self::create`].
    pub fn keep_alive_timeout(&self, timeout: Duration) -> &Self {
        unlock(&self.inner.config).keep_alive_timeout = timeout;
        self
    }

    /// Override the gRPC message-size cap (F3). Applied to decoding *and*
    /// encoding on both the client and the server. Call before [`Self::start`]/[`Self::create`].
    /// Pace outgoing snapshot streams at `bytes_per_sec` (0 = unlimited).
    ///
    /// Comes from the profile's `snapshot_transfer_rate_bps`; the transport
    /// crate has no access to the profile, so the caller (the node binary)
    /// passes it down (rev T).
    pub fn snapshot_rate_bps(&self, bytes_per_sec: u64) -> &Self {
        unlock(&self.inner.config).snapshot_rate_bps = bytes_per_sec;
        self
    }

    /// Install the source of snapshot bytes this node serves (rev T).
    ///
    /// Must be called before [`start`](TonicTransportFactory::start): the gRPC
    /// service is built at start-up and captures the provider then. Without it,
    /// `FetchSnapshot` answers `unavailable` — a node that cannot serve
    /// snapshots must say so rather than hang a follower.
    pub fn snapshot_provider(&self, provider: Arc<dyn SnapshotProvider>) -> &Self {
        *unlock(&self.inner.snapshot_provider) = Some(provider);
        self
    }

    pub fn max_message_size(&self, size: usize) -> &Self {
        unlock(&self.inner.config).max_message_size = size;
        self
    }

    /// Bind one listener per node and start serving.
    ///
    /// Before binding, the cluster identity is validated (a non-empty
    /// `cluster_id` and a non-empty `node_id` for every node — an empty one
    /// would produce a meaningless, unrouteable handshake tag). If servers were
    /// already started by a previous [`Self::start`], they are stopped first.
    ///
    /// For each node the configured listen address is bound (a `:0` port
    /// resolves to an ephemeral one); the node's *real* bound address is written
    /// back into the shared map so peers resolve it correctly. A gRPC server is
    /// then spawned to validate the handshake and forward accepted payloads to
    /// the node's bounded inbound channel.
    ///
    /// Binding is done in a separate phase from starting servers, so a
    /// mid-start bind failure never leaves a partially-started server set
    /// behind (there is nothing to tear down — the already-bound listeners
    /// close on the early return).
    pub async fn start(&self) -> Result<(), TransportError> {
        // Fail fast on an invalid cluster identity. (Duplicate node ids are
        // already unrepresentable — `NodeId` is the address-map key — so only
        // emptiness needs checking here.)
        if self.inner.cluster_id.is_empty() {
            return Err(TransportError::InvalidClusterConfig {
                reason: "cluster_id must be non-empty".to_string(),
            });
        }

        // Snapshot the (possibly placeholder) listen addresses up front.
        let listen: Vec<(NodeId, SocketAddr)> = unlock_read(&self.inner.addresses)
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        if listen.is_empty() {
            // Nothing to bind (an outbound-only factory). Not an error.
            return Ok(());
        }
        for (node, _) in &listen {
            if node.as_str().is_empty() {
                return Err(TransportError::InvalidClusterConfig {
                    reason: "node id must be non-empty".to_string(),
                });
            }
        }

        self.start_targets(listen).await
    }

    /// Bind a single listener for `me` at `bind` and start serving it.
    ///
    /// This is the multi-process counterpart of [`Self::start`]: a process that
    /// is part of a cluster should bind **only its own node**, not the whole
    /// address map (which would `EADDRINUSE` against a peer that already holds
    /// that port). The caller supplies the concrete `bind` address for its own
    /// node; the real post-bind address is written back into the shared map, and
    /// a gRPC server is spawned to feed that node's inbound channel.
    ///
    /// `me` must be a node this factory was constructed with (it owns a
    /// pre-created inbound sender for exactly the nodes in its address map); any
    /// other identity is rejected as an invalid cluster configuration.
    pub async fn start_with_bind(&self, me: NodeId, bind: SocketAddr) -> Result<(), TransportError> {
        // Same cluster-identity validation as `start()`.
        if self.inner.cluster_id.is_empty() {
            return Err(TransportError::InvalidClusterConfig {
                reason: "cluster_id must be non-empty".to_string(),
            });
        }
        if me.as_str().is_empty() {
            return Err(TransportError::InvalidClusterConfig {
                reason: "node id must be non-empty".to_string(),
            });
        }
        // `me` must own a pre-created inbound sender; without one there is no
        // channel the server could feed.
        if !unlock(&self.inner.senders).contains_key(&me) {
            return Err(TransportError::InvalidClusterConfig {
                reason: format!("node '{}' is not part of this factory's cluster", me.as_str()),
            });
        }

        self.start_targets(vec![(me, bind)]).await
    }

    /// Shared serve logic behind [`Self::start`] and [`Self::start_with_bind`]:
    /// stop any previously-started servers, then bind the given
    /// `(node, address)` targets (phase 1), record their real post-bind
    /// addresses, and spawn a gRPC server per target (phase 2).
    ///
    /// Binding is done in a separate phase from starting servers, so a mid-start
    /// bind failure never leaves a partially-started server set behind (the
    /// already-bound listeners close on the early return — leak-free).
    async fn start_targets(&self, targets: Vec<(NodeId, SocketAddr)>) -> Result<(), TransportError> {
        // Double-start guard: stop any servers from a previous start before
        // binding again, so we never leak old servers or re-bind a port they
        // still hold. Awaiting the tasks also drops their inbound sender clones.
        if let Some(prev) = unlock(&self.inner.servers).take() {
            for handle in &prev {
                handle.cancel.cancel();
            }
            for handle in prev {
                let _ = handle.task.await;
            }
        }

        // Phase 1: bind every listener before starting any server. If a bind
        // fails, no server has started, so there is nothing to tear down — the
        // already-bound listeners close when `listeners` is dropped on the early
        // return (a mid-start bind failure is leak-free).
        let mut listeners = Vec::with_capacity(targets.len());
        for (node, addr) in &targets {
            let (listener, real_addr) = self
                .inner
                .io
                .bind(*addr)
                .await
                .map_err(TransportError::Bind)?;
            listeners.push((node.clone(), real_addr, listener));
        }

        // Phase 2: record the real addresses and start one server per node.
        let config = *unlock(&self.inner.config);
        let mut handles = Vec::with_capacity(listeners.len());
        for (node, real_addr, listener) in listeners {
            // Record the real address so the shared map always reflects where a
            // node actually listens (this is what makes `:0` usable).
            unlock_write(&self.inner.addresses).insert(node.clone(), real_addr);

            // A node with no inbound channel cannot receive; skip it (its bound
            // listener is dropped here).
            let Some(sender) = unlock(&self.inner.senders).get(&node).cloned() else {
                continue;
            };
            let service = RaftTransportService::new(
                self.inner.cluster_id.clone(),
                self.inner.protocol_major,
                self.inner.protocol_minor,
                Arc::clone(&self.inner.rejects),
                sender,
                unlock(&self.inner.snapshot_provider).clone(),
                config.snapshot_rate_bps,
                config.max_message_size,
            );
            let cancel = CancellationToken::new();
            let server = Server::builder()
                .add_service(
                    RaftTransportServer::new(service)
                        .max_decoding_message_size(config.max_message_size)
                        .max_encoding_message_size(config.max_message_size),
                )
                .serve_with_incoming(self.inner.io.incoming(listener));

            // Serve until the cancel token fires; then the server future is
            // dropped, stopping it and closing the listener.
            let server_cancel = cancel.clone();
            let task = tokio::spawn(async move {
                tokio::select! {
                    biased;
                    _ = server => {}
                    _ = server_cancel.cancelled() => {}
                }
            });
            handles.push(ServerHandle { cancel, task });
        }

        *unlock(&self.inner.servers) = Some(handles);
        Ok(())
    }

    /// Gracefully stop every server and close the inbound channels, so each
    /// [`TonicRx::recv`] resolves to `None` once this returns.
    ///
    /// Teardown semantics: every server is signalled to stop and *awaited*, so
    /// the inbound-sender clone held by each gRPC service is dropped; the
    /// factory's own sender clones are then dropped too. With every sender
    /// dropped, each bounded inbound channel is closed and drained, and every
    /// outstanding `TonicRx::recv` resolves to `None`.
    pub async fn shutdown(&self) {
        if let Some(handles) = unlock(&self.inner.servers).take() {
            // Signal every server to stop.
            for handle in &handles {
                handle.cancel.cancel();
            }
            // Await each server task to completion so its inbound-sender clone
            // (held by the gRPC service) is dropped before `shutdown` returns.
            // The task only exits via the cancel token (normal completion), so
            // a `JoinError` would only mean the task panicked mid-teardown;
            // the server is already stopping, so there is nothing to recover.
            for handle in handles {
                let _ = handle.task.await;
            }
        }
        // Drop the factory's sender clones so every inbound channel closes.
        unlock(&self.inner.senders).clear();
    }

    /// The number of handshake rejections (cluster-id, protocol, or missing
    /// handshake) seen across the whole cluster.
    pub fn handshake_rejections(&self) -> u64 {
        self.inner.rejects.load(Ordering::Relaxed)
    }
}

impl<Io: TransportIo> TransportFactory for TonicTransportFactory<Io> {
    type Tx = TonicTransport<Io>;
    type Rx = TonicRx;

    fn create(&self, me: NodeId) -> (Self::Tx, Self::Rx) {
        let hello = build_hello(
            &self.inner.cluster_id,
            me.as_str(),
            self.inner.protocol_major,
            self.inner.protocol_minor,
            &self.inner.feature_flags,
        );
        let config = *unlock(&self.inner.config);
        // A transport may fetch snapshots only if this node can serve them too:
        // mixing the two roles would let a leader send metadata-only snapshots
        // its peers could never complete (rev T).
        let serves_snapshots = unlock(&self.inner.snapshot_provider).is_some();
        let tx = TonicTransport::new(
            self.inner.io.connector(),
            Arc::clone(&self.inner.addresses),
            me.clone(),
            hello,
            config,
            serves_snapshots,
        );

        let rx = match unlock(&self.inner.receivers).remove(&me) {
            Some(receiver) => TonicRx::new(receiver),
            // A node not present in the address map has no inbound channel to
            // receive on: hand it a channel that is already closed, so `recv`
            // reports `None` immediately and no messages can ever arrive.
            None => {
                let (_tx, receiver) = tokio::sync::mpsc::channel::<Inbound>(INBOUND_QUEUE_CAPACITY);
                TonicRx::new(receiver)
            }
        };
        (tx, rx)
    }
}
