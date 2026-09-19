//! Assembly of a single-node Arachne node: WAL + KV state machine + `RaftNode`.
//!
//! M0 runs one node over the local placeholder transport (no peers). The drive
//! loop ticks raft, persists `Ready`s, applies committed entries to the state
//! machine, and refreshes the metrics registry.
//!
//! The real tonic transport arrives at M1; only [`Arachne::open`] changes then.

use std::collections::HashMap;
use std::sync::Arc;

use arachne::consensus::{RaftNode, RaftNodeConfig};
use arachne::state_machine::KvStateMachine;
use arachne::storage::{WalConfig, WalOptions, WalStorage};
use arachne::{StateMachine, StorageError};
use slog::Logger;

use crate::config::Config;
use crate::metrics::Metrics;
use crate::transport::{PlaceholderRx, PlaceholderTx};

/// The concrete raft node type for M0.
pub type Node = RaftNode<WalStorage, PlaceholderTx, PlaceholderRx>;

/// Errors from assembling or driving the node.
#[derive(Debug)]
pub enum NodeError {
    /// A durable-storage failure (open or I/O).
    Storage(StorageError),
    /// A raft-core failure.
    Raft(String),
    /// A state-machine apply failure (invariant violation — fail-stop).
    StateMachine(String),
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeError::Storage(e) => write!(f, "storage error: {e}"),
            NodeError::Raft(e) => write!(f, "raft error: {e}"),
            NodeError::StateMachine(e) => write!(f, "state machine error: {e}"),
        }
    }
}

impl std::error::Error for NodeError {}

impl From<StorageError> for NodeError {
    fn from(e: StorageError) -> Self {
        NodeError::Storage(e)
    }
}

/// A running single-node Arachne node.
pub struct Arachne {
    raft: Node,
    sm: KvStateMachine,
    metrics: Arc<Metrics>,
    raft_id: u64,
}

impl Arachne {
    /// Open the node's WAL and assemble the raft node. A fresh node starts with
    /// `applied = 0` (M0 does not yet replay a persisted state-machine snapshot).
    ///
    /// Both the WAL (`fsync_policy`, `segment_bytes`) and the raft tick/flow
    /// timings are driven by the validated [`ProfileConfig`] (propsol §7).
    pub fn open(config: &Config, metrics: Arc<Metrics>, logger: &Logger) -> Result<Self, NodeError> {
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

        let raft = RaftNode::new_with_config(
            config.raft_id,
            // M0 single-node: no peers. Bootstrap voters = {self}.
            HashMap::new(),
            wal,
            PlaceholderTx,
            PlaceholderRx,
            0,
            RaftNodeConfig::from_profile(profile),
            logger,
        )
        .map_err(|e| NodeError::Raft(e.to_string()))?;

        let node = Self {
            raft,
            sm: KvStateMachine::new(),
            metrics,
            raft_id: config.raft_id,
        };
        node.refresh_metrics();
        Ok(node)
    }

    /// One drive cycle: `tick`, persist a `Ready`, apply committed entries,
    /// advance the apply progress, and refresh metrics.
    pub async fn tick(&mut self) -> Result<(), NodeError> {
        self.raft.tick();
        let entries = self
            .raft
            .step()
            .await
            .map_err(|e| NodeError::Raft(e.to_string()))?;
        for (index, data) in entries {
            self.sm
                .apply(index, &data)
                .map_err(|e| NodeError::StateMachine(e.to_string()))?;
        }
        self.raft.advance_apply();
        self.refresh_metrics();
        Ok(())
    }

    /// Propose a command to the raft log (only meaningful on the leader).
    pub fn propose(&mut self, command: &[u8]) -> Result<(), NodeError> {
        self.raft
            .propose(command)
            .map_err(|e| NodeError::Raft(e.to_string()))
    }

    /// The highest applied log index.
    pub fn applied_index(&self) -> u64 {
        self.sm.applied_index()
    }

    /// Read a key from the applied state machine.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, NodeError> {
        self.sm
            .get(key)
            .map_err(|e| NodeError::StateMachine(e.to_string()))
    }

    /// Whether the node is ready to serve (a leader is known).
    pub fn is_ready(&self) -> bool {
        self.raft.leader_id() != 0
    }

    fn refresh_metrics(&self) {
        let hard_state = self.raft.hard_state();
        self.metrics.set_term(hard_state.term);
        self.metrics.set_commit_index(hard_state.commit);
        self.metrics.set_applied_index(self.sm.applied_index());
        self.metrics.set_leader_id(self.raft.leader_id());
        self.metrics
            .set_is_leader(self.raft.leader_id() == self.raft_id);
        // P4 gate: surface dropped sends so a dead transport is visible.
        self.metrics.set_dropped_sends(self.raft.dropped_send_count());
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
