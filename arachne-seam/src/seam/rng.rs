//! The randomness seam.
///
/// The core needs *some* randomness for things like randomized timeouts and
/// leader-election jitter. Real randomness would make execution
/// non-reproducible, so it is injected through this trait. Production supplies
/// a cryptographic RNG; tests and the simulator supply a seeded, deterministic
/// one.

/// A source of unsigned 64-bit random values.
///
/// # Contract
///
/// [`gen_range_u64`](Rng::gen_range_u64) returns a value uniformly distributed
/// in the half-open range `[low, high)`. The precondition is `low < high`;
/// implementations MUST NOT panic, even if the precondition is violated (the
/// caller is responsible for supplying a valid range).
///
/// Implementations are `Send + Sync` so a single RNG can be shared across the
/// tasks that make up a node.
pub trait Rng: Send + Sync + 'static {
    /// Return a value `x` such that `low <= x < high`.
    ///
    /// Requires `low < high`. Must not panic.
    fn gen_range_u64(&self, low: u64, high: u64) -> u64;
}
