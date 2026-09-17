//! Arachne seam: the leaf crate that holds the core value types and the
//! injectable seam traits.
//!
//! This crate is intentionally dependency-free. It contains:
//!
//! * [`types`] — the transport-agnostic value types (`NodeId`, `Timestamp`,
//!   `LogIndex`, `Term`) every part of the system shares.
//! * [`seam`] — the injectable seam traits (`Clock`, `Rng`, `Transport`,
//!   `TransportRx`, `TransportFactory`, `StateMachine`) that isolate all
//!   environment interaction behind swappable interfaces.
//!
//! Every other crate (the `arachne` core, `arachne-transport-tonic`,
//! `arachne-testsupport`, …) depends on **this** crate. Because it is a leaf
//! (it depends on nothing), it cannot participate in a dependency cycle: in
//! particular, `arachne-transport-tonic` can implement the seam traits here
//! without depending on `arachne`, which resolves the `arachne` <->
//! `arachne-transport-tonic` cycle that the feature-gated re-export would
//! otherwise create.

pub mod seam;
pub mod types;

pub use seam::{
    ApplyOutcome, Clock, Rng, StateMachine, Transport, TransportFactory, TransportMessage,
    TransportRx,
};
pub use types::{LogIndex, NodeId, NodeIdError, Term, Timestamp};
