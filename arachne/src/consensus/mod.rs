//! The consensus core: raft-rs integration.
//!
//! * [`raft_storage`] — adapter implementing `raft::storage::Storage` over
//!   `arachne_seam::Storage`.
//! * [`node`] — `RaftNode`: drives the `RawNode` Ready loop with the frozen
//!   persist/send ordering (I1–I4).

pub mod node;
pub mod raft_storage;

pub use node::{CommittedEntry, NodeError, RaftNode, RaftNodeConfig, conf_change_identity};
pub use raft_storage::RaftStorage;
