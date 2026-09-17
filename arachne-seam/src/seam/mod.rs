//! Seam traits: the injectable boundaries between the deterministic core and
//! the (non-deterministic) environment.
//!
//! The Arachne core is pure and deterministic; every interaction with the
//! outside world — time, randomness, the network, and the data store — goes
//! through one of the traits in this module. Production supplies real
//! implementations (system clock, cryptographic RNG, tonic transport, real
//! storage); tests and the simulator supply deterministic ones (manual clock,
//! seeded RNG, in-memory transport, in-memory state machine). Both must be
//! swappable behind exactly these interfaces.
//!
//! See each submodule for the precise contract and its determinism rules.

mod clock;
mod rng;
mod state_machine;
mod transport;

pub use clock::Clock;
pub use rng::Rng;
pub use state_machine::{ApplyOutcome, StateMachine};
pub use transport::{Transport, TransportFactory, TransportMessage, TransportRx};
