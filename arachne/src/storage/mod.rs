//! The Arachne storage module: the seam types (re-exported from
//! [`arachne_seam::storage`]) plus the on-disk WAL format, segment
//! management, META file, and the WAL-backed [`WalStorage`] implementation.
//!
//! # Layout
//!
//! * [`format`] — the WAL record framing, record types, and typed
//!   encode/decode functions.
//! * [`segment`] — segment file naming, append, and read.
//! * [`meta`] — the cluster/node identity META file with atomic write.
//! * [`crc32c`] — the CRC32C (Castagnoli) checksum used for record and META
//!   integrity.
//! * [`wal`] — the WAL-backed [`WalStorage`]: crash-safe durable log with
//!   startup recovery, fsync policy, and fsync accounting.
//!
//! # Re-exported seam types
//!
//! The [`Storage`], [`LogEntry`], [`HardState`], and other value types from
//! [`arachne_seam::storage`] are re-exported here so that the public path
//! `arachne::storage::*` remains stable.

mod crc32c;
mod format;
mod meta;
mod segment;
mod wal;

// Re-export the seam's storage types so `arachne::storage::Storage` etc.
// continue to work.
pub use arachne_seam::storage::{
    ConfState, EntryType, HardState, LogEntry, RaftId, RaftState, Snapshot, SnapshotMeta,
    Storage, StorageError,
};

// Re-export the WAL format and I/O items.
pub use crate::storage::format::{
    decode_entry, decode_hard_state, decode_record, encode_entry, encode_hard_state,
    encode_record, DecodeError, RecordType, MAX_RECORD_BYTES,
};
pub use crate::storage::meta::{read_meta, write_meta, Meta, MetaError, FORMAT_VERSION};
pub use crate::storage::segment::{
    parse_segment_name, segment_name, segment_path, Segment, SegmentError,
    DEFAULT_SEGMENT_BYTES,
};
pub use crate::storage::crc32c::crc32c;

// Re-export the WAL-backed storage implementation.
pub use crate::storage::wal::{
    FsyncPolicy, StorageStats, WalConfig, WalOptions, WalStorage,
};
