//! Arachne node: a runnable single-node Arachne process.
//!
//! This crate is both a library and a binary. The library exposes the pieces
//! the binary (and the integration tests) compose:
//!
//! * [`config`] — TOML config parsing and validation.
//! * [`transport`] — a no-peer placeholder transport (M0); replaced by the
//!   tonic transport at M1.
//! * [`metrics`] — a lock-free, thread-safe metrics registry.
//! * [`http`] — a minimal, dependency-free HTTP server for `/readyz` and
//!   `/metrics`.
//! * [`node`] — assembly of a single-node Arachne node (WAL + state machine +
//!   `RaftNode`).
//!
//! The binary (`src/main.rs`) wires these together: it loads a TOML config,
//! opens a real WAL, drives the raft Ready loop on a tokio runtime, serves the
//! HTTP endpoints, and shuts down gracefully on SIGINT/SIGTERM.
//!
//! M0 runs over the local placeholder transport (no peers); the real tonic
//! transport lands at M1.

pub mod config;
pub mod http;
pub mod metrics;
pub mod node;
pub mod transport;
