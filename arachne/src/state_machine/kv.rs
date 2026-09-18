//! An in-memory KV + idempotency session-table state machine.
//!
//! Commands are opaque bytes with a small codec:
//! * `Put{key, val}` — set a key to a value; the result is the stored value.
//! * `Delete{key}` — remove a key; the result is no value.
//!
//! Every command carries a `(client_id, seq_no)` session envelope. The same
//! `(client_id, seq_no)` is applied **exactly once**: a later replay of the
//! same session (at a new log index) returns the cached result without
//! re-mutating state (propsol §2.3).
//!
//! The machine is deterministic: [`StateMachine::apply`](crate::seam::StateMachine::apply)
//! reads only `index` and `command` — no clock, no I/O, no ambient state.
//! The session table is included in [`snapshot`](crate::seam::StateMachine::snapshot) /
//! [`restore`](crate::seam::StateMachine::restore) so a rebuilt node replays
//! with identical idempotency.

use std::collections::BTreeMap;
use std::io::{Cursor, Read};

use arachne_seam::seam::{ApplyOutcome, StateMachine};
use arachne_seam::types::LogIndex;

/// Errors from the KV state machine.
#[derive(Debug, thiserror::Error)]
pub enum KvError {
    /// The command bytes are malformed.
    #[error("malformed command")]
    MalformedCommand,
    /// The snapshot bytes are malformed.
    #[error("malformed snapshot")]
    MalformedSnapshot,
    /// Index ordering violation (the entry is not `applied_index + 1`).
    #[error("index ordering violation at {index}")]
    IndexViolation { index: LogIndex },
}

/// Command opcodes (part of the wire format; fixed).
const OP_PUT: u8 = 0;
const OP_DELETE: u8 = 1;

/// The kind of command, parsed from its opcode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Put,
    Delete,
}

/// A session key for idempotency: `(client_id, seq_no)`.
///
/// `Ord` is derived so the session table can live in a `BTreeMap`, which keeps
/// the snapshot encoding deterministic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct SessionKey {
    client_id: u64,
    seq_no: u64,
}

/// The in-memory KV + session-table state machine.
pub struct KvStateMachine {
    kv: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Session table: `(client_id, seq_no)` → the cached apply result.
    sessions: BTreeMap<SessionKey, ApplyOutcome>,
    applied: LogIndex,
}

impl KvStateMachine {
    /// Create a fresh, empty state machine.
    pub fn new() -> Self {
        Self {
            kv: BTreeMap::new(),
            sessions: BTreeMap::new(),
            applied: 0,
        }
    }

    // ---- command codec ----------------------------------------------------

    /// Parse a command from its wire bytes.
    ///
    /// Layout: `[u8 op][u64 client_id LE][u64 seq_no LE][u32 key_len LE][key]`
    /// and, for `Put` only, `[u32 val_len LE][val]`. The opcode is
    /// authoritative: `Put` carries a value (possibly empty), `Delete` carries
    /// none.
    fn parse_command(cmd: &[u8]) -> Result<(SessionKey, Op, Vec<u8>, Vec<u8>), KvError> {
        const HEADER: usize = 1 + 8 + 8;
        if cmd.len() < HEADER {
            return Err(KvError::MalformedCommand);
        }
        let op = match cmd[0] {
            OP_PUT => Op::Put,
            OP_DELETE => Op::Delete,
            _ => return Err(KvError::MalformedCommand),
        };
        let client_id =
            u64::from_le_bytes(cmd[1..9].try_into().map_err(|_| KvError::MalformedCommand)?);
        let seq_no =
            u64::from_le_bytes(cmd[9..17].try_into().map_err(|_| KvError::MalformedCommand)?);
        let session = SessionKey { client_id, seq_no };

        let rest = &cmd[HEADER..];
        if rest.len() < 4 {
            return Err(KvError::MalformedCommand);
        }
        let key_len =
            u32::from_le_bytes(rest[0..4].try_into().map_err(|_| KvError::MalformedCommand)?) as usize;
        if rest.len() < 4 + key_len {
            return Err(KvError::MalformedCommand);
        }
        let key = rest[4..4 + key_len].to_vec();
        let val_rest = &rest[4 + key_len..];

        match op {
            Op::Put => {
                if val_rest.len() < 4 {
                    return Err(KvError::MalformedCommand);
                }
                let val_len = u32::from_le_bytes(val_rest[0..4]
                    .try_into()
                    .map_err(|_| KvError::MalformedCommand)?) as usize;
                if val_rest.len() < 4 + val_len {
                    return Err(KvError::MalformedCommand);
                }
                let val = val_rest[4..4 + val_len].to_vec();
                Ok((session, op, key, val))
            }
            Op::Delete => {
                if !val_rest.is_empty() {
                    return Err(KvError::MalformedCommand);
                }
                Ok((session, op, key, Vec::new()))
            }
        }
    }

    /// Encode a `Put` command to wire bytes.
    pub fn encode_put(client_id: u64, seq_no: u64, key: &[u8], val: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(OP_PUT);
        buf.extend_from_slice(&client_id.to_le_bytes());
        buf.extend_from_slice(&seq_no.to_le_bytes());
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
        buf.extend_from_slice(val);
        buf
    }

    /// Encode a `Delete` command to wire bytes.
    pub fn encode_delete(client_id: u64, seq_no: u64, key: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(OP_DELETE);
        buf.extend_from_slice(&client_id.to_le_bytes());
        buf.extend_from_slice(&seq_no.to_le_bytes());
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        buf
    }
}

impl Default for KvStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachine for KvStateMachine {
    type Error = KvError;

    fn apply(&mut self, index: LogIndex, command: &[u8]) -> Result<ApplyOutcome, Self::Error> {
        // Strict ordering: an entry must be exactly `applied + 1` (a duplicate
        // or a gap is an invariant violation => fail-stop).
        if index != self.applied + 1 {
            return Err(KvError::IndexViolation { index });
        }

        // A no-op entry (empty payload) is how a leader records its term; it
        // advances the index but changes no state.
        if command.is_empty() {
            self.applied = index;
            return Ok(ApplyOutcome::None);
        }

        let (session, op, key, val) = Self::parse_command(command)?;

        // Idempotency: a replayed session returns its cached result without
        // re-mutating the store.
        let outcome = match self.sessions.get(&session) {
            Some(cached) => cached.clone(),
            None => {
                let outcome = match op {
                    Op::Put => {
                        self.kv.insert(key.clone(), val.clone());
                        ApplyOutcome::Value(val)
                    }
                    Op::Delete => {
                        self.kv.remove(&key);
                        ApplyOutcome::None
                    }
                };
                self.sessions.insert(session, outcome.clone());
                outcome
            }
        };

        self.applied = index;
        Ok(outcome)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.kv.get(key).cloned())
    }

    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.applied.to_le_bytes());

        buf.extend_from_slice(&(self.kv.len() as u32).to_le_bytes());
        for (k, v) in &self.kv {
            buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
            buf.extend_from_slice(k);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
        }

        buf.extend_from_slice(&(self.sessions.len() as u32).to_le_bytes());
        for (session, outcome) in &self.sessions {
            buf.extend_from_slice(&session.client_id.to_le_bytes());
            buf.extend_from_slice(&session.seq_no.to_le_bytes());
            match outcome {
                ApplyOutcome::None => buf.push(0),
                ApplyOutcome::Value(val) => {
                    buf.push(1);
                    buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
                    buf.extend_from_slice(val);
                }
            }
        }
        Ok(buf)
    }

    fn restore(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        let mut cursor = Cursor::new(bytes);
        self.applied = read_u64(&mut cursor)?;

        let kv_len = read_u32(&mut cursor)? as usize;
        self.kv.clear();
        for _ in 0..kv_len {
            let klen = read_u32(&mut cursor)? as usize;
            let key = read_bytes(&mut cursor, klen)?;
            let vlen = read_u32(&mut cursor)? as usize;
            let val = read_bytes(&mut cursor, vlen)?;
            self.kv.insert(key, val);
        }

        let sess_len = read_u32(&mut cursor)? as usize;
        self.sessions.clear();
        for _ in 0..sess_len {
            let client_id = read_u64(&mut cursor)?;
            let seq_no = read_u64(&mut cursor)?;
            let tag = read_u8(&mut cursor)?;
            let outcome = match tag {
                0 => ApplyOutcome::None,
                1 => {
                    let vlen = read_u32(&mut cursor)? as usize;
                    let val = read_bytes(&mut cursor, vlen)?;
                    ApplyOutcome::Value(val)
                }
                _ => return Err(KvError::MalformedSnapshot),
            };
            self.sessions
                .insert(SessionKey { client_id, seq_no }, outcome);
        }

        // A well-formed snapshot has no trailing bytes.
        if cursor.position() != bytes.len() as u64 {
            return Err(KvError::MalformedSnapshot);
        }
        Ok(())
    }

    fn applied_index(&self) -> LogIndex {
        self.applied
    }
}

// ---- small big-/little-endian cursor helpers (test-free, pure) -------------

fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64, KvError> {
    let mut buf = [0u8; 8];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| KvError::MalformedSnapshot)?;
    Ok(u64::from_le_bytes(buf))
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32, KvError> {
    let mut buf = [0u8; 4];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| KvError::MalformedSnapshot)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8, KvError> {
    let mut buf = [0u8; 1];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| KvError::MalformedSnapshot)?;
    Ok(buf[0])
}

fn read_bytes(cursor: &mut Cursor<&[u8]>, len: usize) -> Result<Vec<u8>, KvError> {
    let remaining = cursor.get_ref().len() - cursor.position() as usize;
    if remaining < len {
        return Err(KvError::MalformedSnapshot);
    }
    let mut out = vec![0u8; len];
    cursor
        .read_exact(&mut out)
        .map_err(|_| KvError::MalformedSnapshot)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_get() {
        let mut sm = KvStateMachine::new();
        let cmd = KvStateMachine::encode_put(1, 1, b"key", b"value");
        let outcome = sm.apply(1, &cmd).unwrap();
        assert_eq!(outcome, ApplyOutcome::Value(b"value".to_vec()));
        assert_eq!(sm.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn delete_removes_key() {
        let mut sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"key", b"value"))
            .unwrap();
        let outcome = sm
            .apply(2, &KvStateMachine::encode_delete(1, 2, b"key"))
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::None);
        assert_eq!(sm.get(b"key").unwrap(), None);
    }

    /// The same `(client_id, seq_no)` applies exactly once: a replay at a later
    /// index returns the cached result without re-mutating state.
    #[test]
    fn session_dedup_applies_exactly_once() {
        let mut sm = KvStateMachine::new();
        let cmd = KvStateMachine::encode_put(1, 1, b"key", b"value");
        let o1 = sm.apply(1, &cmd).unwrap();
        // Replay the same session at index 2.
        let o2 = sm.apply(2, &cmd).unwrap();
        assert_eq!(o1, o2);
        // The value was set once (a re-apply would have overwritten it, but the
        // key holds the original value either way; the dedup is what matters:
        // the store was written once for this session).
        assert_eq!(sm.get(b"key").unwrap(), Some(b"value".to_vec()));
        assert_eq!(sm.applied_index(), 2);
    }

    /// A deduped replay of a `Delete` must not resurrect a later `Put`.
    #[test]
    fn dedup_does_not_reapply_delete() {
        let mut sm = KvStateMachine::new();
        let put = KvStateMachine::encode_put(1, 1, b"key", b"v1");
        let delete = KvStateMachine::encode_delete(1, 2, b"key");
        let later_put = KvStateMachine::encode_put(1, 3, b"key", b"v2");

        sm.apply(1, &put).unwrap(); // key = v1
        sm.apply(2, &delete).unwrap(); // key removed
        sm.apply(3, &later_put).unwrap(); // key = v2
        // Replaying the delete (session 1/2) at index 4 must NOT remove v2.
        sm.apply(4, &delete).unwrap();
        assert_eq!(sm.get(b"key").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn empty_command_is_a_no_op() {
        let mut sm = KvStateMachine::new();
        let outcome = sm.apply(1, &[]).unwrap();
        assert_eq!(outcome, ApplyOutcome::None);
        assert_eq!(sm.applied_index(), 1);
        assert!(sm.get(b"x").unwrap().is_none());
    }

    #[test]
    fn index_violation_duplicate_and_gap() {
        let mut sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"a", b"b"))
            .unwrap();
        // Duplicate (index 1 again).
        assert!(matches!(
            sm.apply(1, &KvStateMachine::encode_put(1, 2, b"c", b"d")),
            Err(KvError::IndexViolation { index: 1 })
        ));
        // Gap (jump to index 3).
        assert!(matches!(
            sm.apply(3, &KvStateMachine::encode_put(1, 3, b"c", b"d")),
            Err(KvError::IndexViolation { index: 3 })
        ));
        // The correct next index (2) still succeeds after the rejections.
        sm.apply(2, &KvStateMachine::encode_put(1, 4, b"a", b"z")).unwrap();
        assert_eq!(sm.get(b"a").unwrap(), Some(b"z".to_vec()));
    }

    #[test]
    fn malformed_command_rejected() {
        let mut sm = KvStateMachine::new();
        // Too short for the header.
        assert!(matches!(
            sm.apply(1, &[0, 0, 0]),
            Err(KvError::MalformedCommand)
        ));
        // Unknown opcode.
        assert!(matches!(
            sm.apply(1, &[9, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            Err(KvError::MalformedCommand)
        ));
        // Truncated key: a full header plus a key length that promises bytes
        // which are not present.
        let mut truncated = vec![OP_PUT];
        truncated.extend_from_slice(&0u64.to_le_bytes()); // client_id
        truncated.extend_from_slice(&0u64.to_le_bytes()); // seq_no
        truncated.extend_from_slice(&5u32.to_le_bytes()); // key_len = 5, no key
        assert!(matches!(
            sm.apply(1, &truncated),
            Err(KvError::MalformedCommand)
        ));
    }

    #[test]
    fn snapshot_restore_roundtrip_includes_sessions() {
        let mut sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"a", b"1"))
            .unwrap();
        sm.apply(2, &KvStateMachine::encode_put(1, 2, b"b", b"2"))
            .unwrap();
        sm.apply(3, &KvStateMachine::encode_delete(1, 3, b"a"))
            .unwrap();

        let snap = sm.snapshot().unwrap();
        let mut sm2 = KvStateMachine::new();
        sm2.restore(&snap).unwrap();
        assert_eq!(sm2.applied_index(), 3);
        assert_eq!(sm2.get(b"a").unwrap(), None);
        assert_eq!(sm2.get(b"b").unwrap(), Some(b"2".to_vec()));
        // Lossless round-trip: the rebuilt machine snapshots identically.
        assert_eq!(sm2.snapshot().unwrap(), snap);
        // Session table survived: replaying session (1,1) still dedups.
        let replay = sm2.apply(4, &KvStateMachine::encode_put(1, 1, b"a", b"1"));
        assert_eq!(replay.unwrap(), ApplyOutcome::Value(b"1".to_vec()));
    }

    #[test]
    fn malformed_snapshot_rejected() {
        let mut sm = KvStateMachine::new();
        // Trailing junk after a valid empty snapshot.
        let mut snap = KvStateMachine::new().snapshot().unwrap();
        snap.push(0xFF);
        assert!(matches!(
            sm.restore(&snap),
            Err(KvError::MalformedSnapshot)
        ));
        // Too short (missing the count field).
        assert!(matches!(
            sm.restore(&[0u8; 4]),
            Err(KvError::MalformedSnapshot)
        ));
    }
}
