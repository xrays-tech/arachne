//! Node metrics — re-exported from the `arachne` lib.
//!
//! The registry lives in the core (`arachne::Metrics`) so the runtime actor
//! updates it directly; this module keeps the node's `crate::metrics::Metrics`
//! import path stable.

pub use arachne::Metrics;
