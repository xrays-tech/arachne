//! Arachne tonic transport — the real network transport (M1).
//!
//! This crate is the ONLY crate in the workspace allowed to reference `tonic`,
//! `prost`, and (later, M4) `rustls` types. Every other crate talks to the
//! network exclusively through the transport-agnostic seam traits in the leaf
//! [`arachne_seam`] crate.
//!
//! # What this crate provides
//!
//! * [`TonicTransport`] — the outbound half ([`Transport`]): resolves a
//!   [`NodeId`] to a `SocketAddr`, lazily opens/reuses a tonic channel, attaches
//!   the handshake, and maps the result to a typed [`TransportError`].
//! * [`TonicRx`] — the inbound half ([`TransportRx`]): pulls accepted payloads
//!   off a channel; `None` once the transport is shut down.
//! * [`TonicTransportFactory`] — mints the `(Tx, Rx)` halves for a node
//!   ([`TransportFactory`]), owns the cluster identity (cluster id, protocol
//!   version), the `NodeId → SocketAddr` map, and a graceful `shutdown()`.
//! * The gRPC wire protocol ([`proto`]) and its server half, which performs the
//!   handshake and rejects (`cluster_id` / protocol mismatches) with a counter.
//!
//! # M1 scope
//!
//! M1 is **plaintext** gRPC — there is no TLS yet (mTLS lands at M4). The
//! handshake (protocol version, cluster id, node id) IS in M1: it rides on every
//! message and is validated on the receiving side, so a cross-cluster or
//! version-incompatible node is rejected with a counter (propsol §5.6).
//!
//! All tonic/rustls/prost types stay inside this crate; the public API below
//! exposes only `NodeId`, `TransportMessage`, and `SocketAddr`.

pub mod error;
pub mod factory;
// `handshake` and `server` are implementation details (only `factory` uses
// them) and are deliberately crate-private so the public surface stays
// `error` / `factory` / `io` / `rx` / `transport`.
mod handshake;
pub mod io;
pub mod rx;
pub mod snapshot;
mod server;
pub mod transport;

/// The generated gRPC wire types (client, server, and message structs) for
/// [`proto/raft.proto`](crate::proto::raft). These are the only tonic/prost
/// types in this crate's module tree.
pub mod proto {
    tonic::include_proto!("raft");
}

pub use error::TransportError;
pub use factory::TonicTransportFactory;
pub use io::{TokioIoProvider, TransportIo};
pub use rx::TonicRx;
pub use snapshot::{SnapshotProvider, SnapshotReader};
pub use transport::TonicTransport;

/// Recover a `Mutex` guard even from a poisoned lock.
///
/// A poisoned lock means a prior thread panicked while holding it. The guarded
/// data here is a simple `HashMap`, so recovering is safe (no invariant is
/// violated by the map itself). This keeps the transport from turning a stray
/// panic into a hard failure of every subsequent `send`/`recv`.
pub(crate) fn unlock<'a, T>(mutex: &'a std::sync::Mutex<T>) -> std::sync::MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Recover a read guard even from a poisoned lock (see [`unlock`]).
pub(crate) fn unlock_read<'a, T>(
    lock: &'a std::sync::RwLock<T>,
) -> std::sync::RwLockReadGuard<'a, T> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Recover a write guard even from a poisoned lock (see [`unlock`]).
pub(crate) fn unlock_write<'a, T>(
    lock: &'a std::sync::RwLock<T>,
) -> std::sync::RwLockWriteGuard<'a, T> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// The name of this transport (re-exported by `arachne` when its
/// `transport-tonic` feature is enabled).
pub fn transport_name() -> &'static str {
    "tonic"
}
