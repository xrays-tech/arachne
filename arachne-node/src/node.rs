//! Assembly of an Arachne node over the lib runtime actor.
//!
//! The node is a thin wiring layer (D-ART): it opens the WAL, builds the lib
//! [`Runtime`] (WAL + state machine + raft node), spawns the actor, and exposes
//! a client [`Handle`]. All consensus/storage logic lives in the `arachne` lib,
//! so the process under test is the shipped path.
//!
//! M1-3a runs the single-node placeholder transport (no peers). The tonic
//! transport is wired for real clustering in a later increment.

use std::collections::HashMap;
use std::sync::Arc;

use arachne::client::Handle;
use arachne::consensus::RaftNodeConfig;
use arachne::runtime::{Runtime, RuntimeConfig};
use arachne::storage::{WalConfig, WalOptions, WalStorage};
use arachne::{Metrics, StorageError};
use slog::Logger;

use crate::config::Config;
use crate::transport::{PlaceholderRx, PlaceholderTx};

/// The node runtime type for the placeholder (no-peer) transport.
pub type NodeRuntime = Runtime<PlaceholderTx, PlaceholderRx>;

/// Errors from assembling the node.
#[derive(Debug)]
pub enum NodeError {
    /// A durable-storage failure (open or I/O).
    Storage(StorageError),
    /// A runtime-assembly failure.
    Runtime(String),
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeError::Storage(e) => write!(f, "storage error: {e}"),
            NodeError::Runtime(e) => write!(f, "runtime error: {e}"),
        }
    }
}

impl std::error::Error for NodeError {}

impl From<StorageError> for NodeError {
    fn from(e: StorageError) -> Self {
        NodeError::Storage(e)
    }
}

/// A running Arachne node.
pub struct Arachne {
    task: tokio::task::JoinHandle<()>,
    handle: Handle,
    metrics: Arc<Metrics>,
}

impl Arachne {
    /// Open the node's WAL, assemble the lib runtime, and spawn its actor.
    ///
    /// Both the WAL (`fsync_policy`, `segment_bytes`) and the raft tick/flow
    /// timings are driven by the validated [`arachne::ProfileConfig`] (propsol §7).
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

        // M1-3a: single node, no peers (bootstrap voters = {self}).
        let runtime_config = RuntimeConfig {
            self_raft_id: config.raft_id,
            self_node_id: config.node_id.clone(),
            peers: HashMap::new(),
            addresses: HashMap::new(),
            raft: RaftNodeConfig::from_profile(profile),
            profile: profile.clone(),
            metrics: Arc::clone(&metrics),
        };

        let (runtime, handle) = Runtime::new(
            runtime_config,
            wal,
            PlaceholderTx,
            PlaceholderRx,
            logger,
        )
        .map_err(|e| NodeError::Runtime(e.to_string()))?;

        let task = tokio::spawn(runtime.run());
        Ok(Self {
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

    /// Stop the runtime actor (dropping the WAL, releasing the dir lock).
    pub async fn shutdown(self) {
        self.task.abort();
        let _ = self.task.await;
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
