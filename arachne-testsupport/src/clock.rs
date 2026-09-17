//! A deterministic, manually-advanced [`Clock`](arachne_seam::Clock) for tests and
//! the simulator.
//!
//! It starts at a chosen millisecond and only moves forward when the test
//! explicitly advances it. Because there is no real time involved, execution is
//! fully reproducible.

use std::sync::atomic::{AtomicU64, Ordering};

use arachne_seam::{Clock, Timestamp};

/// A clock that only advances when the test says so.
///
/// The value is guaranteed **non-decreasing**:
/// * [`advance`](ManualClock::advance) always moves forward by `ms`.
/// * [`set`](ManualClock::set) moves the clock to `ms` *only if* `ms` is later
///   than the current value. Setting it *backwards* is silently **rejected**
///   (a no-op) so the monotonic invariant always holds.
#[derive(Debug)]
pub struct ManualClock {
    now: AtomicU64,
}

impl ManualClock {
    /// Create a clock whose initial reading is `start_ms`.
    pub fn new(start_ms: Timestamp) -> Self {
        Self {
            now: AtomicU64::new(start_ms),
        }
    }

    /// Move the clock forward by `ms` milliseconds and return the new reading.
    ///
    /// `ms == 0` leaves the clock unchanged.
    pub fn advance(&self, ms: Timestamp) -> Timestamp {
        let next = self.now.fetch_add(ms, Ordering::SeqCst) + ms;
        next
    }

    /// Move the clock to `ms`, but never backwards.
    ///
    /// If `ms` is greater than the current reading the clock becomes `ms`;
    /// otherwise the clock is left untouched (the backwards set is rejected).
    pub fn set(&self, ms: Timestamp) {
        // `fetch_max` atomically sets the value to max(current, ms), which is
        // exactly "move to `ms` but never backwards".
        self.now.fetch_max(ms, Ordering::SeqCst);
    }

    /// The current reading (milliseconds).
    pub fn current(&self) -> Timestamp {
        self.now.load(Ordering::SeqCst)
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Clock for ManualClock {
    fn now_millis(&self) -> Timestamp {
        self.now.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_at_the_given_reading() {
        let clock = ManualClock::new(1000);
        assert_eq!(clock.now_millis(), 1000);
    }

    #[test]
    fn is_monotonic_under_advance() {
        let clock = ManualClock::new(0);
        let a = clock.advance(10);
        let b = clock.advance(5);
        let c = clock.advance(0);
        assert_eq!(a, 10);
        assert_eq!(b, 15);
        // Advancing by zero must not move the clock backwards.
        assert_eq!(c, 15);
        assert!(a <= b && b <= c);
    }

    #[test]
    fn advance_values_are_exact() {
        let clock = ManualClock::new(100);
        assert_eq!(clock.advance(1), 101);
        assert_eq!(clock.advance(999), 1100);
        assert_eq!(clock.now_millis(), 1100);
    }

    #[test]
    fn set_moves_forward_and_returns_the_new_value_implicitly() {
        let clock = ManualClock::new(10);
        clock.set(50);
        assert_eq!(clock.now_millis(), 50);
    }

    #[test]
    fn set_backwards_is_rejected() {
        let clock = ManualClock::new(100);
        clock.set(50); // backwards: must be a no-op
        assert_eq!(clock.now_millis(), 100);
        clock.set(100); // equal: also a no-op
        assert_eq!(clock.now_millis(), 100);
    }

    #[test]
    fn set_forward_is_idempotent_and_monotonic() {
        let clock = ManualClock::new(0);
        clock.set(100);
        clock.set(100);
        clock.advance(5);
        assert_eq!(clock.now_millis(), 105);
        clock.set(50); // backwards again: no-op
        assert_eq!(clock.now_millis(), 105);
    }
}
