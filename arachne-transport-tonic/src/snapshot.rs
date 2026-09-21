//! Serving snapshot bytes, and pacing that transfer (propsol rev T, v0.2.17).
//!
//! # Why the transport needs a provider
//!
//! The bytes of a snapshot live in the storage layer (`snapshot-<index>-<term>
//! .snap`), which this crate deliberately knows nothing about: it depends only
//! on the dependency-free `arachne-seam`. So the transport asks a
//! [`SnapshotProvider`] for `(index, term)` and streams whatever it is handed.
//! Whoever knows the layout (`arachne-node`, wired to the data directory)
//! supplies the implementation.
//!
//! # Why a reader, not a buffer
//!
//! A snapshot may be tens of megabytes (the Lan preset snapshots at 64 MiB of
//! WAL). Returning `Vec<u8>` would put the whole thing in memory on the serving
//! node for every transfer; a reader lets the server hold one chunk at a time.

use std::io::Read;
use std::time::Duration;

/// A snapshot opened for streaming: its exact length plus a reader over it.
///
/// The two travel together on purpose. `len` and the reader are captured in one
/// call, so a snapshot that is compacted away between "how big is it" and "give
/// me the bytes" cannot produce a short read that looks like a complete
/// transfer.
pub struct SnapshotReader {
    /// The snapshot's total length in bytes.
    pub len: u64,
    /// The snapshot's bytes, read on demand.
    pub reader: Box<dyn Read + Send>,
}

impl std::fmt::Debug for SnapshotReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotReader")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

/// Where a node's snapshot bytes come from (rev T).
///
/// Implementations must be cheap to call and must not block for long: the
/// server calls [`open`](SnapshotProvider::open) once per `FetchSnapshot`, then
/// streams from the returned reader.
pub trait SnapshotProvider: Send + Sync + 'static {
    /// Open the snapshot at `(index, term)`.
    ///
    /// `None` means "no such snapshot" — it was never created, or it has since
    /// been compacted away. The server answers `NOT_FOUND`; the follower
    /// retries on its next snapshot offer, which is the correct recovery (the
    /// leader will name a snapshot it still has).
    fn open(&self, index: u64, term: u64) -> Option<SnapshotReader>;
}

/// A byte-rate limiter: a token bucket that refills at `bytes_per_sec`.
///
/// A snapshot transfer competes with the replication traffic on the same link;
/// without pacing, one 64 MiB snapshot can starve the heartbeats that keep the
/// leader's quorum alive. The rate comes from the profile
/// (`snapshot_transfer_rate_bps`); `0` disables the limiter entirely, which is
/// the pre-rev-T behaviour and the default when nobody configures it.
pub(crate) struct RateLimiter {
    /// Refill rate in bytes/second; `0` = unlimited.
    bytes_per_sec: u64,
    /// Tokens available now, in bytes.
    allowance: f64,
    /// When `allowance` was last refilled.
    last: tokio::time::Instant,
    /// The largest burst allowed: one chunk, so a transfer starts immediately
    /// instead of idling for the first chunk's worth of tokens.
    burst: f64,
}

impl RateLimiter {
    /// A limiter at `bytes_per_sec` allowing an initial burst of `burst` bytes.
    pub(crate) fn new(bytes_per_sec: u64, burst: u64) -> Self {
        Self {
            bytes_per_sec,
            // Start with one burst available: the first chunk goes out at once.
            allowance: burst as f64,
            last: tokio::time::Instant::now(),
            burst: burst as f64,
        }
    }

    /// Wait until `bytes` may be sent.
    pub(crate) async fn acquire(&mut self, bytes: u64) {
        if self.bytes_per_sec == 0 {
            return;
        }
        let need = bytes as f64;
        // A chunk larger than the burst can never be satisfied by tokens alone;
        // pace it at the bucket's size instead of waiting forever.
        let need = need.min(self.burst).max(1.0);
        loop {
            let now = tokio::time::Instant::now();
            let elapsed = now.duration_since(self.last).as_secs_f64();
            self.allowance =
                (self.allowance + elapsed * self.bytes_per_sec as f64).min(self.burst);
            self.last = now;
            if self.allowance >= need {
                self.allowance -= need;
                return;
            }
            let missing = need - self.allowance;
            let wait = missing / self.bytes_per_sec as f64;
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_unlimited_limiter_never_waits() {
        let mut limiter = RateLimiter::new(0, 1024);
        // A transfer orders of magnitude larger than the burst still completes
        // immediately: `0` means "no pacing at all", not "a very high rate".
        tokio::time::timeout(Duration::from_millis(50), limiter.acquire(1 << 30))
            .await
            .expect("rate 0 must not pace anything at all");
    }

    #[tokio::test]
    async fn a_limited_limiter_paces_after_the_initial_burst() {
        // 64 KiB/s with a 1 KiB burst: the first 1 KiB is free, then it paces.
        let mut limiter = RateLimiter::new(64 * 1024, 1024);
        let started = tokio::time::Instant::now();
        // Three chunks of 1 KiB: one free, two paced at ~1/64 s each.
        for _ in 0..3 {
            limiter.acquire(1024).await;
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(20),
            "three chunks at 64 KiB/s must take measurable time, took {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "but not an absurd amount of time, took {elapsed:?}"
        );
    }
}
