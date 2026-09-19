//! Pure helpers for the transport handshake (propsol §5.6).
//!
//! Both functions here are pure and side-effect free: [`build_hello`] assembles
//! a sender's handshake, and [`validate_hello`] decides whether an inbound
//! handshake is acceptable. Keeping the validation pure makes it directly unit
//! testable and guarantees the server's accept/reject decision is deterministic.

use crate::error::{ERR_CLUSTER_ID_MISMATCH, ERR_PROTOCOL_MISMATCH};
use crate::proto::Hello;

/// Assemble the sender's [`Hello`] handshake.
///
/// `node_id` is the *sender's* identity; `cluster_id` and the protocol version
/// come from the cluster configuration. This is the single place where a
/// `Hello` is constructed, so the field ordering can never drift.
pub fn build_hello(
    cluster_id: &str,
    node_id: &str,
    protocol_major: u32,
    protocol_minor: u32,
    feature_flags: &[String],
) -> Hello {
    Hello {
        protocol_major,
        protocol_minor,
        cluster_id: cluster_id.to_string(),
        node_id: node_id.to_string(),
        feature_flags: feature_flags.to_vec(),
    }
}

/// Decide whether an inbound [`Hello`] is acceptable to a receiver.
///
/// Returns `None` when the handshake is accepted, or `Some(code)` with the
/// stable wire error code when it must be rejected:
///
/// * the sender's `cluster_id` must equal the receiver's, and
/// * the sender's `protocol_major` must equal the receiver's, and
/// * the sender's `protocol_minor` must be `<=` the receiver's (backward
///   compatible only — a *newer* peer is rejected).
pub fn validate_hello(
    hello: &Hello,
    cluster_id: &str,
    protocol_major: u32,
    protocol_minor: u32,
) -> Option<&'static str> {
    if hello.cluster_id != cluster_id {
        return Some(ERR_CLUSTER_ID_MISMATCH);
    }
    if hello.protocol_major != protocol_major {
        return Some(ERR_PROTOCOL_MISMATCH);
    }
    if hello.protocol_minor > protocol_minor {
        // Only backward compatible: a peer running a newer minor is rejected.
        return Some(ERR_PROTOCOL_MISMATCH);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hello(major: u32, minor: u32, cluster: &str, node: &str) -> Hello {
        build_hello(cluster, node, major, minor, &[])
    }

    #[test]
    fn build_hello_sets_all_fields() {
        let h = build_hello("c1", "n1", 1, 2, &["f".to_string()]);
        assert_eq!(h.protocol_major, 1);
        assert_eq!(h.protocol_minor, 2);
        assert_eq!(h.cluster_id, "c1");
        assert_eq!(h.node_id, "n1");
        assert_eq!(h.feature_flags, vec!["f".to_string()]);
    }

    #[test]
    fn accepts_matching_cluster_and_version() {
        assert_eq!(validate_hello(&hello(1, 0, "c", "n"), "c", 1, 3), None);
        assert_eq!(validate_hello(&hello(1, 3, "c", "n"), "c", 1, 3), None);
    }

    #[test]
    fn rejects_cluster_mismatch() {
        assert_eq!(
            validate_hello(&hello(1, 0, "other", "n"), "c", 1, 3),
            Some(ERR_CLUSTER_ID_MISMATCH)
        );
    }

    #[test]
    fn rejects_major_mismatch() {
        assert_eq!(
            validate_hello(&hello(2, 0, "c", "n"), "c", 1, 3),
            Some(ERR_PROTOCOL_MISMATCH)
        );
    }

    #[test]
    fn rejects_newer_peer_minor() {
        // peer minor 5 > self minor 3 → reject (backward compatible only).
        assert_eq!(
            validate_hello(&hello(1, 5, "c", "n"), "c", 1, 3),
            Some(ERR_PROTOCOL_MISMATCH)
        );
    }
}
