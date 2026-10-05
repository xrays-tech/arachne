//! The consensus core: raft-rs integration.
//!
//! * [`raft_storage`] — adapter implementing `raft_seedable::storage::Storage` over
//!   `arachne_kv_seam::Storage`.
//! * [`node`] — `RaftNode`: drives the `RawNode` Ready loop with the frozen
//!   persist/send ordering (I1–I4).

pub mod node;
pub mod raft_storage;

pub use node::{
    CommittedEntry, HeldSnapshot, NodeError, RaftNode, RaftNodeConfig, conf_change_identity,
    learner_caught_up,
};
pub use raft_storage::RaftStorage;
