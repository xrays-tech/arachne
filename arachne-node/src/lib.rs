//! Arachne node: a runnable single-node Arachne process.
//!
//! This crate is both a library and a binary. The library exposes the pieces
//! the binary (and the integration tests) compose:
//!
//! * [`config`] — TOML config parsing and validation.
//! * [`metrics`] — a lock-free, thread-safe metrics registry.
//! * [`http`] — a minimal, dependency-free HTTP server for `/readyz`,
//!   `/metrics`, and the KV read/write endpoints.
//! * [`node`] — assembly of an Arachne node (WAL + state machine + `RaftNode`)
//!   over the real tonic transport.
//! * [`force_recovery`] — the operator-gated `force-recovery` subcommand
//!   (propsol §6.1).
//!
//! The binary (`src/main.rs`) wires these together: it loads a TOML config,
//! opens a real WAL, binds this process's tonic listener, drives the raft
//! Ready loop on a tokio runtime, serves the HTTP endpoints, and shuts down
//! gracefully on SIGINT/SIGTERM.
//!
//! M1: the node runs over the real tonic transport (`arachne-transport-tonic`),
//! so each process can join a multi-process cluster — binding only its own
//! `listen` address and reaching peers through the shared `NodeId ->
//! SocketAddr` map.

pub mod config;
pub mod force_recovery;
pub mod http;
pub mod metrics;
pub mod node;
