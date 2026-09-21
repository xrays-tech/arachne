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
//! * [`membership`] — the durable `ConfState` a ConfChange produced, written
//!   atomically (propsol v0.2.16 rev S).
//! * [`snapshot`] — the on-disk snapshot file format (propsol §5.5.4).
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
mod membership;
mod meta;
mod segment;
pub mod snapshot;
mod wal;

// Re-export the seam's storage types so `arachne::storage::Storage` etc.
// continue to work.
pub use arachne_seam::storage::{
    ConfState, EntryType, FsyncObserver, HardState, LogEntry, RaftId, RaftState, Snapshot,
    SnapshotMeta, Storage, StorageError,
};

// Re-export the WAL format and I/O items.
//
// F5: only the items actually needed by the standalone `fuzz/` crate and
// integration tests are public. The rest are `#[doc(hidden)]` to keep the
// production API surface minimal while still allowing the fuzz harness to
// compile (it is a separate crate that cannot access `pub(crate)` items).
#[doc(hidden)]
pub use crate::storage::format::{
    decode_entry, decode_hard_state, decode_record, encode_entry, encode_hard_state,
    encode_record, DecodeError, RecordType, MAX_RECORD_BYTES,
};
#[doc(hidden)]
pub use crate::storage::meta::{read_meta, write_meta, Meta, MetaError, FORMAT_VERSION};
#[doc(hidden)]
pub use crate::storage::segment::{
    parse_segment_name, segment_name, segment_path, Segment, SegmentError,
    DEFAULT_SEGMENT_BYTES,
};
#[doc(hidden)]
pub use crate::storage::crc32c::crc32c;

// Re-export the WAL-backed storage implementation.
pub use crate::storage::wal::{
    ForceRecoveryReport, FsyncPolicy, StorageStats, WalConfig, WalOptions, WalStorage,
};
