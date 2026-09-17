//! Arachne tonic transport.
//!
//! This crate is the ONLY crate in the workspace allowed to reference `tonic`
//! and `rustls` types. All other crates must interact with the network
//! exclusively through the transport-agnostic core (`arachne`) and its seam
//! traits (in the leaf `arachne-seam` crate).
//!
//! This crate depends on `arachne-seam` (NOT on `arachne`) so that its future
//! `impl arachne_seam::Transport` can be written here without reintroducing the
//! `arachne` <-> `arachne-transport-tonic` dependency cycle: `arachne`
//! feature-gatedly depends on this crate, while this crate depends only on the
//! leaf `arachne-seam`.
//!
//! No external dependencies (`tonic`, `rustls`) have been added yet — they land
//! in a later phase. Until then this crate only depends on `arachne-seam`, so
//! the whole workspace builds without network access.

/// Return the name of this transport.
pub fn transport_name() -> &'static str {
    "tonic"
}

#[cfg(test)]
mod tests {
    use super::transport_name;

    #[test]
    fn transport_name_is_tonic() {
        assert_eq!(transport_name(), "tonic");
    }
}
