//! Serving this node's snapshots to followers (propsol rev T, T2b).
//!
//! A snapshot is far larger than the transport's gRPC message cap, so it does
//! not travel inside the raft message: the follower asks for it by position and
//! the bytes are streamed. The transport crate deliberately knows nothing about
//! our on-disk layout, so it asks a [`SnapshotProvider`] — this is the
//! production implementation, backed by the node's data directory.
//!
//! # The naming matters
//!
//! The request carries the snapshot's **bare** index and term (they come from
//! the snapshot metadata, not from a file name), while the file on disk is
//! zero-padded: `snapshot-<020>-<020>.snap`. [`snapshot_file_name`] performs
//! that mapping, and it is the same function the WAL uses, so the two cannot
//! drift.

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arachne::storage::snapshot::snapshot_file_name;
use arachne_transport_tonic::{SnapshotProvider, SnapshotReader};

/// Serves snapshots out of a node's data directory.
pub struct DataDirSnapshots {
    /// The data directory holding `snapshot-*.snap` files.
    data_dir: PathBuf,
}

impl DataDirSnapshots {
    /// A provider over `data_dir`, shared with the transport factory.
    pub fn new(data_dir: PathBuf) -> Arc<Self> {
        Arc::new(Self { data_dir })
    }
}

impl SnapshotProvider for DataDirSnapshots {
    fn open(&self, index: u64, term: u64) -> Option<SnapshotReader> {
        let path = self.data_dir.join(snapshot_file_name(index, term));
        let file = File::open(&path).ok()?;
        let len = file.metadata().ok()?.len();
        // A reader plus its exact length, captured together: if the snapshot is
        // compacted away between the two, the follower would otherwise see a
        // short read that looks like a complete transfer.
        Some(SnapshotReader {
            len,
            reader: Box::new(file),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn serves_a_snapshot_by_position_and_ignores_the_rest() {
        let dir = std::env::temp_dir().join(format!(
            "arachne-node-snapshots-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create data dir");

        // The name is the WAL's, so write through the same helper the WAL uses.
        let path = dir.join(snapshot_file_name(149, 1));
        let contents = b"state machine bytes";
        std::fs::write(&path, contents).expect("write snapshot");

        let provider = DataDirSnapshots::new(dir.clone());
        let mut reader = provider.open(149, 1).expect("the snapshot is served");
        assert_eq!(
            reader.len,
            contents.len() as u64,
            "the length must match the file"
        );
        let mut bytes = Vec::new();
        reader
            .reader
            .read_to_end(&mut bytes)
            .expect("read the snapshot");
        assert_eq!(bytes, b"state machine bytes");

        // Anything else is "no such snapshot" — a failed transfer the runtime
        // reports, never an empty snapshot it installs.
        assert!(provider.open(150, 1).is_none());
        assert!(provider.open(149, 2).is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
