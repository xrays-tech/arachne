//! Arachne simulation binary.
//!
//! Scaffold only. The sim must stay deterministic and fully offline: it is
//! forbidden from reading real wall-clock time or performing network I/O
//! (enforced by `scripts/check-entropy.sh` Gate C). A virtual clock and a
//! virtual transport will be added in a later phase.
//!
//! Note: this file deliberately does not name the forbidden crates/modules in
//! text, because Gate C is a plain text scan of `*sim*` paths.

fn main() {
    println!("arachne-sim {}", arachne::version());
}
