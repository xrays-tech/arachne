//! Assembly of an Arachne node over the lib runtime actor.
//!
//! The node is a thin wiring layer (D-ART): it opens the WAL, builds the lib
//! [`Runtime`] (WAL + state machine + raft node), spawns the actor, and exposes
//! a client [`Handle`]. All consensus/storage logic lives in the `arachne` lib,
//! so the process under test is the shipped path.
//!
//! M1: the node runs over the **real tonic transport**. One
//! [`TonicTransportFactory`] owns the cluster identity (cluster id, protocol
//! version) and the shared `NodeId -> SocketAddr` map; `open` binds **only this
//! process's own** listener ([`TonicTransportFactory::start_with_bind`]) and
//! mints this node's `(Tx, Rx)` halves. Each process therefore binds a single
//! port and reaches its peers through the shared address map — which is what
//! lets several processes form a real cluster.

use std::collections::HashMap;
use std::sync::Arc;

use arachne::client::Handle;
use arachne::consensus::RaftNodeConfig;
use arachne::runtime::{Runtime, RuntimeConfig};
use arachne::storage::{WalConfig, WalOptions, WalStorage};
use arachne::{Metrics, RaftId, StorageError, TransportFactory};
use arachne_transport_tonic::{TonicRx, TonicTransport, TonicTransportFactory, TransportError};
use slog::Logger;

use crate::config::Config;

/// The node runtime type for the real tonic transport.
pub type NodeRuntime = Runtime<TonicTransport, TonicRx>;

/// Wire-protocol major version this node speaks in the transport handshake.
///
/// A peer whose major differs is rejected (propsol §5.6). Kept in sync with the
/// `arachne-transport-tonic` factory defaults so a same-cluster handshake always
/// matches.
const PROTOCOL_MAJOR: u32 = 1;
/// Wire-protocol minor version this node speaks in the transport handshake.
///
/// A peer whose minor is *newer* than ours is rejected; equal or older is
/// accepted. `0` is the first (and current) revision of the M1 wire protocol.
const PROTOCOL_MINOR: u32 = 0;

/// Errors from assembling the node.
#[derive(Debug)]
pub enum NodeError {
    /// A durable-storage failure (open or I/O).
    Storage(StorageError),
    /// A runtime-assembly failure.
    Runtime(String),
    /// A transport failure (a `listen` bind failure, an invalid cluster
    /// configuration, …) raised while setting up the tonic transport.
    Transport(TransportError),
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeError::Storage(e) => write!(f, "storage error: {e}"),
            NodeError::Runtime(e) => write!(f, "runtime error: {e}"),
            NodeError::Transport(e) => write!(f, "transport error: {e}"),
        }
    }
}

impl std::error::Error for NodeError {}

impl From<StorageError> for NodeError {
    fn from(e: StorageError) -> Self {
        NodeError::Storage(e)
    }
}

impl From<TransportError> for NodeError {
    fn from(e: TransportError) -> Self {
        NodeError::Transport(e)
    }
}

/// A running Arachne node.
pub struct Arachne {
    /// The tonic transport factory; [`Arachne::shutdown`] stops its server and
    /// closes the inbound channels before the runtime actor is aborted.
    factory: TonicTransportFactory,
    task: arachne::RuntimeThread,
    handle: Handle,
    metrics: Arc<Metrics>,
}

impl Arachne {
    /// Open the node's WAL, bind its tonic listener, assemble the lib runtime,
    /// and spawn its actor.
    ///
    /// Both the WAL (`fsync_policy`, `segment_bytes`) and the raft tick/flow
    /// timings are driven by the validated [`arachne::ProfileConfig`] (propsol §7).
    /// The tonic factory binds **only** this node's `listen` address: in a
    /// multi-process cluster a process must never bind its peers' ports, so it
    /// calls `start_with_bind` (not `start`) for its own node id.
    pub async fn open(
        config: &Config,
        metrics: Arc<Metrics>,
        logger: &Logger,
    ) -> Result<Self, NodeError> {
        let profile = &config.profile_config;
        let wal = WalStorage::open(
            &config.data_dir,
            WalOptions {
                cluster_id: config.cluster_id.clone(),
                node_id: config.node_id.as_str().to_string(),
                config: WalConfig {
                    fsync_policy: profile.fsync_policy,
                    segment_bytes: profile.wal_segment_bytes,
                },
                created_at_millis: now_millis(),
                fsync_observer: None,
            },
        )?;

        // The real tonic transport: one factory owns the cluster identity and
        // the `NodeId -> SocketAddr` map and mints this node's halves.
        let factory = TonicTransportFactory::new(
            config.cluster_id.clone(),
            PROTOCOL_MAJOR,
            PROTOCOL_MINOR,
            Vec::new(),
            config.addresses.clone(),
        );
        // Bind ONLY this node's listener and start serving it. This node id is
        // guaranteed to be in the factory's address map: the map holds every
        // `initial_cluster` member, and this node is one of them.
        factory
            .start_with_bind(config.node_id.clone(), config.listen)
            .await
            .map_err(NodeError::Transport)?;
        let (tx, rx) = factory.create(config.node_id.clone());

        // Bootstrap peers: each OTHER member's raft id is its 1-based position
        // in `initial_cluster` — the same bootstrap mapping as `self_raft_id`.
        let mut peers = HashMap::new();
        for (i, id) in config.initial_cluster.iter().enumerate() {
            if *id != config.node_id {
                peers.insert((i + 1) as RaftId, id.clone());
            }
        }

        let runtime_config = RuntimeConfig {
            self_raft_id: config.raft_id,
            self_node_id: config.node_id.clone(),
            peers,
            addresses: config.addresses.clone(),
            raft: RaftNodeConfig::from_profile(profile),
            profile: profile.clone(),
            metrics: Arc::clone(&metrics),
        };

        // If runtime assembly fails, `start_with_bind` has already bound this
        // node's listener: shut the factory down before returning so the gRPC
        // server is torn down now rather than left running until process exit.
        let (runtime, handle) = match Runtime::new(runtime_config, wal, tx, rx, logger) {
            Ok(built) => built,
            Err(e) => {
                factory.shutdown().await;
                return Err(NodeError::Runtime(e.to_string()));
            }
        };

        // The actor does blocking `fsync`s, so it gets its own OS thread
        // instead of a shared tokio worker (see `Runtime::spawn_dedicated`).
        let task = runtime
            .spawn_dedicated()
            .map_err(|e| NodeError::Runtime(format!("cannot spawn the runtime thread: {e}")))?;
        Ok(Self {
            factory,
            task,
            handle,
            metrics,
        })
    }

    /// A cheap-`Clone` client handle for this node.
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }

    /// The shared metrics registry.
    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    /// Stop the node: shut the tonic factory down first (stopping the gRPC
    /// server and closing the inbound channels, so the runtime's inbound
    /// `recv` resolves to `None`), then abort the runtime actor (dropping the
    /// WAL and releasing the dir lock).
    pub async fn shutdown(self) {
        // Closing the factory closes the inbound stream, which ends the actor's
        // loop; join its thread off the async workers so the WAL (and its data
        // dir lock) is released before this returns.
        self.factory.shutdown().await;
        let _ = tokio::task::spawn_blocking(move || self.task.join()).await;
    }
}

/// The current wall-clock time in milliseconds, read once at the binary edge.
/// The core itself never reads a clock.
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
