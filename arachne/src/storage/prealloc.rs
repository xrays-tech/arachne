//! Platform-dispatched WAL segment extent pre-allocation (B4 / U2-bis).
//!
//! On Linux this calls `fallocate(2)` with the **`FALLOC_FL_KEEP_SIZE`** flag
//! to reserve the full segment extent for a newly-opened WAL segment file
//! **without changing `i_size` at all**: the file stays logically empty
//! (length 0) while the filesystem commits the 128 MiB of extents up front.
//! Subsequent appends then write into already-allocated space — no delayed
//! allocation and no extent-journal work on the data path, and the per-segment
//! `fdatasync(2)` (`sync_durable`) barrier degrades from "data + allocation +
//! journal" to "data + inode". One `fdatasync` per segment lifecycle, same
//! shape as etcd's `fileutil.Preallocate`.
//!
//! # Why recovery, tear detection and rollover need zero changes
//!
//! `FALLOC_FL_KEEP_SIZE` reserves extents only; it **never** extends `i_size`.
//! The five subsystems that read `i_size` are therefore byte-identical:
//!
//! | Subsystem | Source of truth | Effect of KEEP_SIZE |
//! |---|---|---|
//! | `recover_wal` (wal.rs) | `fs::read` up to `i_size` | reserved region is beyond `i_size` → unreadable; byte-identical |
//! | tear detection | `record_end > file_len` (file_len = i_size) | unchanged |
//! | `truncate_log_to` / `truncate_at` | `set_len` (downward) | unchanged (KEEP_SIZE extents discarded on truncate) |
//! | `size()` / `should_rollover` | `metadata().len()` | unchanged ("rollover always fires" never occurs) |
//! | `log_bytes` | `metadata().len()` | unchanged |
//!
//! # Failure policy (never fatal)
//!
//! Any `fallocate(2)` `Err` (EOPNOTSUPP, ENOSYS, EINVAL, ENOSPC, …) means
//! *"skip pre-allocation"*: behaviour is byte-identical to a no-op today. We
//! neither fail-open nor fail-close: surfacing ENOSPC early would change the
//! existing failure paths (a full disk keeps failing on the write /
//! `fdatasync` path, as before), so the failure surface is deliberately left
//! unchanged.
//!
//! # Cross-platform
//!
//! Only Linux has the cheap `fdatasync` + `fallocate` pairing. On every other
//! platform the non-Linux arm returns a compile-time `false` (that branch is
//! not even compiled), so this is an empty shell off Linux.
//!
//! # TDD state
//!
//! The two `fallocate`-behaviour tests (`fallocate_reserves_blocks_without_
//! changing_len`, `preallocate_unsupported_len_degrades_without_error`) are
//! `#[cfg(target_os = "linux")]`; the three logical-size / roundtrip tests in
//! `segment.rs` / `wal.rs` pass on every platform (they pin the "KEEP_SIZE
//! never moves i_size" invariant).

use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};

/// Count of skipped pre-allocation attempts. Only incremented on Linux when a
/// `fallocate(2)` call fails (non-Linux is a compile-time no-op and does not
/// touch the counter).
static PREALLOC_SKIPS: AtomicU64 = AtomicU64::new(0);

/// Reserve `len` bytes of extents for `file` (Linux `fallocate` with
/// `FALLOC_FL_KEEP_SIZE`).
///
/// Returns `true` when the extents were reserved; `false` when the call was
/// skipped (non-Linux, or `fallocate` failed — failure is never fatal; see the
/// module docs for the five `i_size`-based subsystems that stay byte-identical
/// and why the failure policy is conservative).
pub(crate) fn preallocate(file: &File, len: u64) -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;

        // `#define FALLOC_FL_KEEP_SIZE 0x01` (Linux 3.13+): with this flag
        // `fallocate(2)` reserves extents **without** growing `i_size` — the
        // file stays logically empty while the filesystem commits the extents
        // up front.
        const FALLOC_FL_KEEP_SIZE: i32 = 0x01;

        let fd = file.as_raw_fd();
        // libc 0.2 binding: `fallocate(fd, mode, offset, len) -> c_int`
        // (0 on success, -1 on failure, errno set on error). `len` is a `u64`
        // but the binding takes `off_t` (a signed `i64`); values beyond
        // `i64::MAX` cast negative → `EINVAL`, which the caller treats as a
        // graceful skip (see `preallocate_unsupported_len_degrades_without_error`).
        let ret = unsafe {
            libc::fallocate(fd, FALLOC_FL_KEEP_SIZE as libc::c_int, 0, len as i64)
        };
        if ret >= 0 {
            true
        } else {
            // Consume the set `errno` so it is not misread by a later syscall.
            let _ = std::io::Error::last_os_error();
            PREALLOC_SKIPS.fetch_add(1, Ordering::Relaxed);
            false
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Off-Linux the whole pre-allocation is an empty shell: no syscall, no
        // counter, behaviour identical to the current no-op.
        let _ = (file, len);
        false
    }
}

/// Number of skipped pre-allocation attempts (relaxed load).
pub(crate) fn skips() -> u64 {
    PREALLOC_SKIPS.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{OpenOptions, remove_file};
    use std::io::{Read, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Unique temp file path for a test (no external crates).
    fn temp_file() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "arachne-prealloc-test-{}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    // Linux-only: pins the actual `fallocate(FALLOC_FL_KEEP_SIZE)` semantics.
    // Reserves extents (st_blocks / `blocks()` jumps to > 0), `len()` stays 0,
    // and `preallocate` returns true.
    #[cfg(target_os = "linux")]
    #[test]
    fn fallocate_reserves_blocks_without_changing_len() {
        let path = temp_file();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("create");
        assert_eq!(file.metadata().expect("metadata").len(), 0);

        assert!(preallocate(&file, 4096), "must reserve extents");
        assert_eq!(
            file.metadata().expect("metadata").len(),
            0,
            "KEEP_SIZE must not change i_size (len stays 0)"
        );
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt;
            assert!(
                file.metadata().expect("metadata").blocks() > 0,
                "st_blocks must be > 0 after reserving 4096 bytes"
            );
        }
        let _ = remove_file(&path);
    }

    // Linux-only: a `u64` that overflows `off_t` (as i64) is EINVAL →
    // preallocate must degrade gracefully (false + counted skip) and the file
    // must remain fully usable (write + sync + read back).
    #[cfg(target_os = "linux")]
    #[test]
    fn preallocate_unsupported_len_degrades_without_error() {
        let path = temp_file();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .expect("create");

        let before = skips();
        let ok = preallocate(&file, u64::MAX);
        assert!(!ok, "preallocate(u64::MAX) must degrade (negative off_t = EINVAL)");
        assert_eq!(
            skips(),
            before + 1,
            "the failed pre-allocation must be counted in skips()"
        );

        file.write_all(b"degraded-payload").expect("write");
        file.sync_all().expect("sync_all");
        drop(file);

        let mut file = OpenOptions::new().read(true).open(&path).expect("reopen");
        let mut buf = String::new();
        file.read_to_string(&mut buf).expect("read back");
        assert_eq!(buf, "degraded-payload");

        let _ = remove_file(&path);
    }
}