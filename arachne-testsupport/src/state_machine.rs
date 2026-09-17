//! A trivial, deterministic [`StateMachine`](arachne_seam::StateMachine) for tests.
//!
//! It is a byte-keyed map plus a record of the highest index applied. It
//! exists to prove the `StateMachine` seam is implementable and to give the
//! core a concrete, deterministic store to test against. No external
//! dependencies.
//!
//! # Command encoding
//!
//! A command is a byte string holding zero or more length-prefixed
//! `(key, value)` pairs, each written as:
//!
//! ```text
//! [u32be key_len][key][u32be value_len][value]
//! ```
//!
//! Applying a command sets each key to its value and returns the value of the
//! last pair (or [`ApplyOutcome::None`](arachne_seam::ApplyOutcome::None) for an
//! empty command).
//!
//! # Snapshot encoding
//!
//! A snapshot captures the *entire* state and round-trips losslessly:
//!
//! ```text
//! [u64be applied]
//! [u32be entry_count]
//! [u32be key_len][key][u32be value_len][value]   (repeated, keys ascending)
//! ```
//!
//! Entries are serialized in ascending key order (the backing map is a
//! `BTreeMap`), so the encoding is deterministic: the same state always yields
//! the same bytes.

use std::collections::BTreeMap;
use std::io::{Cursor, Read};

use arachne_seam::{ApplyOutcome, LogIndex, StateMachine};

/// Errors a state machine can report.
///
/// Per the [`StateMachine::apply`](arachne_seam::StateMachine::apply) contract,
/// any of these is a **fail-stop** condition: the caller must abort, never
/// retry or degrade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SmError {
    /// The command bytes were not a valid length-prefixed pair encoding.
    MalformedCommand,
    /// The snapshot bytes were not a valid snapshot encoding.
    MalformedSnapshot,
    /// An entry was applied at an index other than `applied_index + 1`. This
    /// state machine enforces strictly sequential application.
    OutOfOrderIndex { expected: LogIndex, got: LogIndex },
}

impl std::fmt::Display for SmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SmError::MalformedCommand => write!(f, "malformed command encoding"),
            SmError::MalformedSnapshot => write!(f, "malformed snapshot encoding"),
            SmError::OutOfOrderIndex { expected, got } => {
                write!(f, "out-of-order apply: expected index {expected}, got {got}")
            }
        }
    }
}

impl std::error::Error for SmError {}

/// A deterministic byte-keyed value store.
#[derive(Debug, Default)]
pub struct InMemoryStateMachine {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    applied: LogIndex,
}

impl InMemoryStateMachine {
    /// Create an empty state machine with no applied entries.
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            applied: 0,
        }
    }

    /// The number of keys currently stored.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the store holds no keys.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl StateMachine for InMemoryStateMachine {
    type Error = SmError;

    fn apply(&mut self, index: LogIndex, command: &[u8]) -> Result<ApplyOutcome, Self::Error> {
        // Strict sequential-apply contract: the caller MUST apply entries at
        // exactly `applied_index + 1`. Any other index is an invariant
        // violation => fail-stop. `checked_add` keeps a machine already at
        // `u64::MAX` failing cleanly instead of overflowing.
        let expected = self.applied.checked_add(1).ok_or(SmError::OutOfOrderIndex {
            expected: LogIndex::MAX,
            got: index,
        })?;
        if index != expected {
            return Err(SmError::OutOfOrderIndex { expected, got: index });
        }

        let pairs = parse_command(command)?;
        let mut last_value: Option<Vec<u8>> = None;
        for (key, value) in pairs {
            last_value = Some(value.clone());
            self.data.insert(key, value);
        }
        self.applied = index;
        Ok(match last_value {
            Some(value) => ApplyOutcome::Value(value),
            None => ApplyOutcome::None,
        })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.data.get(key).cloned())
    }

    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(encode_snapshot(&self.data, self.applied))
    }

    fn restore(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        let (data, applied) = decode_snapshot(bytes)?;
        self.data = data;
        self.applied = applied;
        Ok(())
    }

    fn applied_index(&self) -> LogIndex {
        self.applied
    }
}

/// Parse a command into its (key, value) pairs.
///
/// Fails fast with [`SmError::MalformedCommand`] on any truncation or
/// length-prefix overflow.
fn parse_command(command: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SmError> {
    let mut cursor = Cursor::new(command);
    let mut pairs = Vec::new();
    loop {
        // A clean end of input means "no more pairs"; anything else is malformed.
        let key_len = match read_u32_len(&mut cursor) {
            Ok(len) => len,
            Err(Stop::End) => break,
            Err(Stop::Bad) => return Err(SmError::MalformedCommand),
        };
        let key = read_bytes(&mut cursor, key_len as usize)
            .map_err(|_| SmError::MalformedCommand)?;
        let value_len = read_u32_len(&mut cursor)
            .map_err(|_| SmError::MalformedCommand)?;
        let value = read_bytes(&mut cursor, value_len as usize)
            .map_err(|_| SmError::MalformedCommand)?;
        pairs.push((key, value));
    }
    Ok(pairs)
}

/// A snapshot of the whole state as deterministic bytes.
fn encode_snapshot(data: &BTreeMap<Vec<u8>, Vec<u8>>, applied: LogIndex) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        8 + 4 + data.len() * (4 + 4) + data.values().map(|v| 2 * v.len() + 8).sum::<usize>(),
    );
    write_u64(&mut out, applied);
    write_u32(&mut out, data.len() as u32);
    for (key, value) in data {
        write_u32(&mut out, key.len() as u32);
        out.extend_from_slice(key);
        write_u32(&mut out, value.len() as u32);
        out.extend_from_slice(value);
    }
    out
}

/// Restore a full state from a snapshot produced by [`encode_snapshot`].
fn decode_snapshot(bytes: &[u8]) -> Result<(BTreeMap<Vec<u8>, Vec<u8>>, LogIndex), SmError> {
    let mut cursor = Cursor::new(bytes);
    let applied = read_u64(&mut cursor).map_err(|_| SmError::MalformedSnapshot)?;
    let count = read_u32_len(&mut cursor).map_err(|_| SmError::MalformedSnapshot)?;
    let mut data = BTreeMap::new();
    for _ in 0..count {
        let key_len = read_u32_len(&mut cursor).map_err(|_| SmError::MalformedSnapshot)?;
        let key =
            read_bytes(&mut cursor, key_len as usize).map_err(|_| SmError::MalformedSnapshot)?;
        let value_len = read_u32_len(&mut cursor).map_err(|_| SmError::MalformedSnapshot)?;
        let value = read_bytes(&mut cursor, value_len as usize)
            .map_err(|_| SmError::MalformedSnapshot)?;
        data.insert(key, value);
    }
    // A well-formed snapshot has no trailing bytes; reject any stragglers.
    if cursor.position() != bytes.len() as u64 {
        return Err(SmError::MalformedSnapshot);
    }
    Ok((data, applied))
}

/// What a length-prefixed read hit: a clean end of input, or a truncation.
enum Stop {
    End,
    Bad,
}

/// Read a `u32` big-endian length from `cursor`.
///
/// Returns [`Stop::End`] when the input is exactly exhausted (no more pairs),
/// [`Stop::Bad`] when a length prefix is truncated (1–3 bytes left), and
/// `Ok(len)` otherwise.
fn read_u32_len(cursor: &mut Cursor<&[u8]>) -> Result<u32, Stop> {
    let remaining = cursor.get_ref().len() - cursor.position() as usize;
    if remaining == 0 {
        return Err(Stop::End);
    }
    if remaining < 4 {
        return Err(Stop::Bad);
    }
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf).map_err(|_| Stop::Bad)?;
    Ok(u32::from_be_bytes(buf))
}

/// Read exactly `len` bytes from `cursor`.
fn read_bytes(cursor: &mut Cursor<&[u8]>, len: usize) -> Result<Vec<u8>, Stop> {
    let remaining = cursor.get_ref().len() - cursor.position() as usize;
    if remaining < len {
        return Err(Stop::Bad);
    }
    let mut out = vec![0u8; len];
    cursor
        .read_exact(&mut out)
        .map(|_| out)
        .map_err(|_| Stop::Bad)
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> std::io::Result<u64> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf)?;
    Ok(u64::from_be_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a command from a slice of (key, value) pairs. Keys are `&str` for
    /// readability; values are `&[u8]`.
    fn command(pairs: &[(&str, &[u8])]) -> Vec<u8> {
        let mut out = Vec::new();
        for (k, v) in pairs {
            write_u32(&mut out, k.len() as u32);
            out.extend_from_slice(k.as_bytes());
            write_u32(&mut out, v.len() as u32);
            out.extend_from_slice(v);
        }
        out
    }

    #[test]
    fn apply_is_deterministic_same_sequence_same_snapshot() {
        let cmds = vec![
            command(&[("a", b"1")]),
            command(&[("b", b"2"), ("a", b"3")]),
            command(&[("c", b"4")]),
        ];

        let mut m1 = InMemoryStateMachine::new();
        let mut m2 = InMemoryStateMachine::new();
        for (i, cmd) in cmds.iter().enumerate() {
            let idx = (i as LogIndex) + 1;
            let o1 = m1.apply(idx, cmd).unwrap();
            let o2 = m2.apply(idx, cmd).unwrap();
            assert_eq!(o1, o2);
        }
        assert_eq!(m1.snapshot().unwrap(), m2.snapshot().unwrap());
    }

    #[test]
    fn applied_index_advances() {
        let mut m = InMemoryStateMachine::new();
        assert_eq!(m.applied_index(), 0);
        m.apply(1, &command(&[("a", b"1")])).unwrap();
        assert_eq!(m.applied_index(), 1);
        m.apply(2, &command(&[("b", b"2")])).unwrap();
        assert_eq!(m.applied_index(), 2);
    }

    #[test]
    fn apply_rejects_an_index_ahead_of_first() {
        // A fresh machine must be applied at index 1, not 2.
        let mut m = InMemoryStateMachine::new();
        assert_eq!(
            m.apply(2, &command(&[("a", b"1")])).unwrap_err(),
            SmError::OutOfOrderIndex {
                expected: 1,
                got: 2
            }
        );
        // The machine is unchanged by a rejected apply (fail-stop, no partial
        // mutation): it still expects index 1.
        assert_eq!(m.applied_index(), 0);
        assert_eq!(m.get(b"a").unwrap(), None);
    }

    #[test]
    fn apply_rejects_a_duplicate_index() {
        let mut m = InMemoryStateMachine::new();
        m.apply(1, &command(&[("a", b"1")])).unwrap();
        // Re-applying index 1 (already applied) is a violation.
        assert_eq!(
            m.apply(1, &command(&[("a", b"1")])).unwrap_err(),
            SmError::OutOfOrderIndex {
                expected: 2,
                got: 1
            }
        );
        // And applying index 0 (a rewind) is also a violation.
        assert_eq!(
            m.apply(0, &command(&[("b", b"2")])).unwrap_err(),
            SmError::OutOfOrderIndex {
                expected: 2,
                got: 0
            }
        );
        // State is unchanged by the rejected applies.
        assert_eq!(m.applied_index(), 1);
        assert_eq!(m.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(m.get(b"b").unwrap(), None);
    }

    #[test]
    fn apply_rejects_a_skipped_index() {
        let mut m = InMemoryStateMachine::new();
        m.apply(1, &command(&[("a", b"1")])).unwrap();
        // Jumping to index 3 (skipping 2) is a violation.
        assert_eq!(
            m.apply(3, &command(&[("c", b"3")])).unwrap_err(),
            SmError::OutOfOrderIndex {
                expected: 2,
                got: 3
            }
        );
        // The correct next index (2) still succeeds after the rejection.
        m.apply(2, &command(&[("b", b"2")])).unwrap();
        assert_eq!(m.applied_index(), 2);
    }

    #[test]
    fn snapshot_restore_roundtrips() {
        let mut m = InMemoryStateMachine::new();
        m.apply(1, &command(&[("a", b"1")])).unwrap();
        m.apply(2, &command(&[("b", b"2"), ("c", b"3")])).unwrap();

        let snap = m.snapshot().unwrap();
        let mut m2 = InMemoryStateMachine::new();
        m2.restore(&snap).unwrap();

        assert_eq!(m2.applied_index(), 2);
        assert_eq!(m2.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(m2.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(m2.get(b"c").unwrap(), Some(b"3".to_vec()));
        assert_eq!(m2.snapshot().unwrap(), snap);
    }

    #[test]
    fn get_returns_none_for_absent_key() {
        let mut m = InMemoryStateMachine::new();
        m.apply(1, &command(&[("a", b"1")])).unwrap();
        assert_eq!(m.get(b"missing").unwrap(), None);
        assert_eq!(m.get(b"a").unwrap(), Some(b"1".to_vec()));
    }

    #[test]
    fn empty_command_applies_to_none() {
        let mut m = InMemoryStateMachine::new();
        let outcome = m.apply(1, &[]).unwrap();
        assert_eq!(outcome, ApplyOutcome::None);
        assert_eq!(m.applied_index(), 1);
    }

    #[test]
    fn malformed_command_is_rejected() {
        let mut m = InMemoryStateMachine::new();
        // A length prefix claiming 10 bytes with none following.
        assert_eq!(m.apply(1, &[0, 0, 0, 10]).unwrap_err(), SmError::MalformedCommand);
    }

    #[test]
    fn malformed_snapshot_is_rejected() {
        let mut m = InMemoryStateMachine::new();
        // Trailing junk after a valid empty snapshot.
        let mut snap = encode_snapshot(&BTreeMap::new(), 0);
        snap.push(0xFF);
        assert_eq!(m.restore(&snap).unwrap_err(), SmError::MalformedSnapshot);
        // Too short: missing the count field.
        assert_eq!(m.restore(&[0u8, 0, 0, 0]).unwrap_err(), SmError::MalformedSnapshot);
    }
}
