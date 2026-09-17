//! Common, transport-agnostic value types shared across the Arachne core and
//! its seam traits.
//!
//! These types carry no behaviour of their own beyond identity/ordering; they
//! exist so that every seam (clock, transport, state machine) speaks the same
//! vocabulary for *who* a node is and *when/where* in the log an event sits.

use std::fmt;

/// A timestamp in **milliseconds**.
///
/// Produced by a [`Clock`](crate::seam::Clock) implementation. The state
/// machine never reads the clock directly: it sees only timestamps that a
/// leader has already stamped onto committed log entries, so its view of time
/// stays deterministic. (The leader itself does read the clock to apply those
/// stamps — see the [`seam::Clock`](crate::seam::Clock) docs.)
pub type Timestamp = u64;

/// A position in the replicated log.
///
/// `0` denotes "no entry yet". Indices are contiguous and strictly increasing
/// per leader term.
pub type LogIndex = u64;

/// A raft term: a monotonically increasing epoch of leadership.
pub type Term = u64;

/// The identity of a node in the cluster.
///
/// A `NodeId` is a thin newtype wrapper around a `String`. It is cheap to
/// clone, hashable (usable as a map key), and totally ordered (usable in sets
/// and for deterministic tie-breaking).
///
/// There are two constructors:
/// * [`NodeId::new`] — infallible, performs **no validation** (accepts any
///   string, including the empty one). Use it in tests and scaffolding.
/// * [`NodeId::try_new`] — validating; rejects an empty identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(String);

/// An error returned by the validating [`NodeId`] constructor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeIdError {
    /// The identifier string was empty.
    Empty,
}

impl NodeId {
    /// Create a `NodeId` from anything that converts into a `String`.
    ///
    /// This is **infallible** and performs **no validation**: any string,
    /// including the empty string, is accepted. Use [`NodeId::try_new`] when a
    /// non-empty, validated identifier is required.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Create a `NodeId`, rejecting an empty identifier.
    ///
    /// Returns [`NodeIdError::Empty`] if `id` is the empty string; otherwise
    /// `Ok` with the new `NodeId`.
    pub fn try_new(id: impl Into<String>) -> Result<Self, NodeIdError> {
        let id = id.into();
        if id.is_empty() {
            return Err(NodeIdError::Empty);
        }
        Ok(Self(id))
    }

    /// Borrow the underlying identifier string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeIdError::Empty => write!(f, "node id must be a non-empty string"),
        }
    }
}

impl core::error::Error for NodeIdError {}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for NodeId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for NodeId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_and_as_str_roundtrip() {
        let id = NodeId::new("node-a");
        assert_eq!(id.as_str(), "node-a");
    }

    #[test]
    fn from_str_and_string_roundtrip() {
        let from_slice = NodeId::from("node-b");
        let from_owned = NodeId::from(String::from("node-b"));
        assert_eq!(from_slice, from_owned);
        assert_eq!(from_slice.as_str(), "node-b");
        assert_eq!(from_owned.as_str(), "node-b");
    }

    #[test]
    fn display_matches_as_str() {
        let id = NodeId::new("node-c");
        assert_eq!(id.to_string(), "node-c");
        assert_eq!(format!("{}", id), id.as_str());
    }

    #[test]
    fn ordering_is_lexicographic() {
        let a = NodeId::from("a");
        let b = NodeId::from("b");
        let z = NodeId::from("z");
        assert!(a < b);
        assert!(b < z);
        assert!(z > a);
        // Equal ids compare equal and hash identically.
        assert_eq!(a, NodeId::from("a"));
    }

    #[test]
    fn used_as_a_sorted_set_key() {
        use std::collections::BTreeSet;
        let mut set = BTreeSet::new();
        set.insert(NodeId::from("c"));
        set.insert(NodeId::from("a"));
        set.insert(NodeId::from("b"));
        // BTreeSet keeps its elements in `Ord` order.
        let ordered: Vec<&str> = set.iter().map(NodeId::as_str).collect();
        assert_eq!(ordered, vec!["a", "b", "c"]);
    }

    #[test]
    fn try_new_accepts_a_non_empty_id() {
        let id = NodeId::try_new("node-d").expect("non-empty id must be valid");
        assert_eq!(id.as_str(), "node-d");
        let from_owned = NodeId::try_new(String::from("node-e")).expect("non-empty id must be valid");
        assert_eq!(from_owned.as_str(), "node-e");
    }

    #[test]
    fn try_new_rejects_an_empty_id() {
        assert_eq!(NodeId::try_new("").err(), Some(NodeIdError::Empty));
        assert_eq!(
            NodeId::try_new(String::new()).err(),
            Some(NodeIdError::Empty)
        );
    }

    #[test]
    fn new_is_infallible_and_unvalidated() {
        // `new` accepts even the empty string — validation is `try_new`'s job.
        let id = NodeId::new("");
        assert_eq!(id.as_str(), "");
    }

    #[test]
    fn node_id_error_is_a_std_error() {
        // The error must be usable anywhere a `core::error::Error` is expected.
        fn assert_is_error<E: core::error::Error + Send + Sync + 'static>(e: E) -> String {
            e.to_string()
        }
        let msg = assert_is_error(NodeIdError::Empty);
        assert_eq!(msg, "node id must be a non-empty string");
        // Debug / Clone / PartialEq behave as a plain value type.
        assert_eq!(format!("{:?}", NodeIdError::Empty), "Empty");
        assert_eq!(NodeIdError::Empty.clone(), NodeIdError::Empty);
    }
}
