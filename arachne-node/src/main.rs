//! Arachne node binary.
//!
//! Scaffold only: prints the core version and the active transport name.
//! No argument parsing, consensus, storage, or networking yet (later phases).
//!
//! The `transport-tonic` feature is enabled **explicitly** in this crate's
//! manifest (the workspace-wide `arachne` dependency is lean by default), so
//! `arachne::transport_name` is available.

fn main() {
    println!("arachne {}", arachne::version());
    println!("transport: {}", arachne::transport_name());
}
