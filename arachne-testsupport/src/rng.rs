//! A small, fully-deterministic PRNG ([`Rng`](arachne_seam::Rng) impl) written from
//! scratch with zero external dependencies.
//!
//! It uses the [SplitMix64](https://en.wikipedia.org/wiki/Random_number_generation#SplitMix64)
//! algorithm. For a fixed seed it always yields the exact same sequence, which
//! makes election timeouts and other randomized behaviour reproducible in tests
//! and the simulator.

use std::sync::atomic::{AtomicU64, Ordering};

use arachne_seam::Rng;

/// SplitMix64 increment (the "golden ratio" constant).
const SPLITMIX_INCREMENT: u64 = 0x9E37_79B9_7F4A_7C15;
/// SplitMix64 mixing constants.
const MIX_A: u64 = 0xBF58_476D_1CE4_E5B9;
const MIX_B: u64 = 0x94D0_49BB_1331_11EB;

/// A deterministic 64-bit PRNG. Identical seeds ⇒ identical output sequences.
#[derive(Debug)]
pub struct SeededRng {
    state: AtomicU64,
}

impl SeededRng {
    /// Create a PRNG seeded with `seed`.
    pub fn new(seed: u64) -> Self {
        Self {
            state: AtomicU64::new(seed),
        }
    }

    /// Advance the internal state and return the next pseudo-random u64.
    ///
    /// This is the low-level generator; [`gen_range_u64`](Rng::gen_range_u64)
    /// maps it into a caller-chosen range.
    fn next_u64(&self) -> u64 {
        // SplitMix64: mix the advanced state. `wrapping_*` keeps this total and
        // panic-free for every input.
        let mut z = self
            .state
            .fetch_add(SPLITMIX_INCREMENT, Ordering::SeqCst)
            .wrapping_add(SPLITMIX_INCREMENT);
        z = (z ^ (z >> 30)).wrapping_mul(MIX_A);
        z = (z ^ (z >> 27)).wrapping_mul(MIX_B);
        z ^ (z >> 31)
    }
}

impl Rng for SeededRng {
    fn gen_range_u64(&self, low: u64, high: u64) -> u64 {
        // Precondition: low < high. If it is violated (span <= 0) we return
        // `low` rather than panicking, per the Rng contract.
        let span = match high.checked_sub(low) {
            Some(s) if s > 0 => s,
            _ => return low,
        };
        // `v % span` lies in [0, span); adding it to `low` stays in [low, high)
        // and cannot overflow (the result is at most high - 1).
        let v = self.next_u64();
        low + (v % span)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DRAWS: usize = 20_000;

    #[test]
    fn same_seed_yields_identical_sequence() {
        let a = SeededRng::new(42);
        let b = SeededRng::new(42);
        for _ in 0..DRAWS {
            assert_eq!(a.gen_range_u64(0, u64::MAX), b.gen_range_u64(0, u64::MAX));
        }
    }

    #[test]
    fn different_seeds_differ() {
        // Probabilistic: with a wide range, two different seeds almost surely
        // produce different first values.
        let a = SeededRng::new(1);
        let b = SeededRng::new(2);
        assert_ne!(a.gen_range_u64(0, u64::MAX), b.gen_range_u64(0, u64::MAX));
    }

    #[test]
    fn values_are_in_range_for_many_draws() {
        let rng = SeededRng::new(1234);
        for (low, high) in [(0u64, 10u64), (100, 200), (0, u64::MAX), (5, 6)] {
            for _ in 0..DRAWS {
                let x = rng.gen_range_u64(low, high);
                assert!(x >= low, "x={x} below low={low}");
                assert!(x < high, "x={x} not below high={high}");
            }
        }
    }

    #[test]
    fn single_width_range_returns_the_only_value() {
        let rng = SeededRng::new(7);
        for _ in 0..DRAWS {
            assert_eq!(rng.gen_range_u64(99, 100), 99);
        }
    }

    #[test]
    fn degenerate_range_does_not_panic() {
        let rng = SeededRng::new(0);
        // low == high and low > high are precondition violations; the contract
        // requires "must not panic". We return `low`.
        assert_eq!(rng.gen_range_u64(5, 5), 5);
        assert_eq!(rng.gen_range_u64(10, 3), 10);
    }
}
