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
use std::sync::{Arc, RwLock};

use arachne_kv_seam::seam::{ApplyOutcome, StateMachine};
use arachne_kv_seam::types::LogIndex;

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
    /// The store lock is poisoned (a thread panicked while holding it).
    /// Unrecoverable — fail-stop.
    #[error("state machine store lock poisoned")]
    PoisonedLock,
}

/// Command opcodes (part of the wire format; fixed).
const OP_PUT: u8 = 0;
const OP_DELETE: u8 = 1;
/// Session garbage collection (propsol v0.2.15 R2): prune the listed sessions.
///
/// The list is **explicit** rather than a cutoff timestamp: every replica must
/// remove exactly the same sessions, and a timestamp would have to come from the
/// leader's clock and could not be replayed.
const OP_SESSION_GC: u8 = 2;

/// The state-machine snapshot payload format version.
///
/// State-machine-internal; **independent of** the storage layer's
/// [`crate::storage::meta::FORMAT_VERSION`]. It is the first byte of the
/// snapshot payload (see [`snapshot`](StateMachine::snapshot)) and is
/// validated by [`decode_store`]: a payload with a wrong version byte is
/// rejected with [`KvError::MalformedSnapshot`] (fail-stop, no migration — a
/// v0 payload lacks this byte entirely, and fabricating `index 0` would be
/// an illegal public value that could be persisted and propagated).
const KV_SNAPSHOT_VERSION: u8 = 1;

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
///
/// `Clone` is cheap (clones the `Arc<RwLock<Store>>` reference, not the data)
/// so the machine can be shared across tasks (apply task + read path).
#[derive(Clone)]
pub struct KvStateMachine {
    /// Single lock over the store (map + sessions + version) — D-Arc. A writer
    /// holds the write lock and mutates the maps FIRST, then bumps `applied`
    /// LAST; a reader holds the read lock and reads `applied` FIRST, then the
    /// map (D-Ord). The `Arc` makes the machine shareable across tasks.
    store: Arc<RwLock<Store>>,
}

/// A single KV entry: the applied value plus its origin — the log index of
/// the entry that wrote this value. The index is stable across deduped
/// replays (a replay does not re-mutate the entry, so it keeps its original
/// origin) and is always `>= 1` for a present key (the first applied index
/// is 1; index `0` is reserved to mean "absent").
#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
    /// The applied value.
    val: Vec<u8>,
    /// The log index of the entry that wrote this value (value's origin).
    index: LogIndex,
}

/// The data guarded by the state machine's single lock.
struct Store {
    /// Monotonic applied-version. Must be bumped *after* the maps mutate
    /// (D-Ord) so a reader never sees `version >= C` with entry `C` missing.
    applied: LogIndex,
    kv: BTreeMap<Vec<u8>, Entry>,
    /// Session table: `(client_id, seq_no)` → the cached apply result.
    sessions: BTreeMap<SessionKey, ApplyOutcome>,
}

impl KvStateMachine {
    /// Create a fresh, empty state machine.
    pub fn new() -> Self {
        Self {
            store: Arc::new(RwLock::new(Store {
                applied: 0,
                kv: BTreeMap::new(),
                sessions: BTreeMap::new(),
            })),
        }
    }

    // Lock access is inlined per method below (no generic helper): a poisoned
    // lock means a thread panicked holding it, so the only correct response is
    // to fail-stop, which the `?`/`.expect` below express directly.

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

    /// The idempotency session `(client_id, seq_no)` a command belongs to.
    ///
    /// `None` for an empty (leader no-op) entry or a truncated command. The
    /// runtime uses this to attribute an entry it hands to the apply task back
    /// to the client proposal waiting on it (propsol v0.2.11 N).
    pub fn command_session(cmd: &[u8]) -> Option<(u64, u64)> {
        // `[op:1][client_id:8][seq_no:8]` — the same layout `parse_command`
        // validates, minus the payload.
        if !matches!(cmd.first(), Some(&OP_PUT) | Some(&OP_DELETE)) {
            return None;
        }
        let client_id = u64::from_le_bytes(cmd.get(1..9)?.try_into().ok()?);
        let seq_no = u64::from_le_bytes(cmd.get(9..17)?.try_into().ok()?);
        Some((client_id, seq_no))
    }

    /// Encode a session-GC command (propsol v0.2.15 R2).
    ///
    /// Layout: `[op:1][count:u32][(client_id:u64, seq_no:u64) × count]`. It
    /// carries no session header of its own, so
    /// [`command_session`](KvStateMachine::command_session) reports `None` for it
    /// and it never extends a session.
    pub fn encode_session_gc(sessions: &[(u64, u64)]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(5 + sessions.len() * 16);
        buf.push(OP_SESSION_GC);
        buf.extend_from_slice(&(sessions.len() as u32).to_le_bytes());
        for (client_id, seq_no) in sessions {
            buf.extend_from_slice(&client_id.to_le_bytes());
            buf.extend_from_slice(&seq_no.to_le_bytes());
        }
        buf
    }

    /// Decode a session-GC command, rejecting a malformed one rather than
    /// pruning a prefix of it.
    fn parse_session_gc(cmd: &[u8]) -> Result<Vec<SessionKey>, KvError> {
        if cmd.first() != Some(&OP_SESSION_GC) || cmd.len() < 5 {
            return Err(KvError::MalformedCommand);
        }
        let count = u32::from_le_bytes(
            cmd.get(1..5)
                .ok_or(KvError::MalformedCommand)?
                .try_into()
                .map_err(|_| KvError::MalformedCommand)?,
        ) as usize;
        let body = cmd.get(5..).ok_or(KvError::MalformedCommand)?;
        if body.len() != count * 16 {
            return Err(KvError::MalformedCommand);
        }
        let mut sessions = Vec::with_capacity(count);
        for pair in body.chunks_exact(16) {
            let client_id = u64::from_le_bytes(
                pair[0..8].try_into().map_err(|_| KvError::MalformedCommand)?,
            );
            let seq_no = u64::from_le_bytes(
                pair[8..16].try_into().map_err(|_| KvError::MalformedCommand)?,
            );
            sessions.push(SessionKey { client_id, seq_no });
        }
        Ok(sessions)
    }

    /// How many sessions the table holds (propsol §8 `session_count`).
    pub fn session_count(&self) -> usize {
        let guard = self
            .store
            .read()
            .expect("state machine read lock should never be poisoned");
        guard.sessions.len()
    }

    /// Whether the session `(client_id, seq_no)` has been applied, i.e. its
    /// result is cached in the session table.
    ///
    /// The node runtime uses this to detect that a proposed command has been
    /// committed and applied (the propose→commit→apply→reply path, propsol §3):
    /// once the command's session is cached, the write is durable and visible.
    /// A replayed (deduped) session is reported as applied as soon as it was
    /// first applied, which is exactly the semantics a waiting caller wants.
    pub fn applied_session(&self, client_id: u64, seq_no: u64) -> bool {
        let guard = self
            .store
            .read()
            .expect("state machine read lock should never be poisoned");
        guard.sessions.contains_key(&SessionKey { client_id, seq_no })
    }

    /// Read the applied version and the kv map under a *single* read lock,
    /// returning a coherent copy. This is the D-Arc invariant in one shot: the
    /// version and the map are always observed together (no torn read). The
    /// torn-read TDD test asserts the linear invariant on this atomic snapshot.
    #[cfg(test)]
    fn read_store_copy(&self) -> (LogIndex, BTreeMap<Vec<u8>, Entry>) {
        let guard = self
            .store
            .read()
            .expect("state machine read lock should never be poisoned");
        (guard.applied, guard.kv.clone())
    }
}

impl Default for KvStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachine for KvStateMachine {
    type Error = KvError;

    fn apply(&self, index: LogIndex, command: &[u8]) -> Result<ApplyOutcome, Self::Error> {
        // The write lock is held for the whole apply. We mutate the maps FIRST
        // and bump `applied` LAST (D-Ord), so a reader never sees `version >= C`
        // with entry `C` missing.
        let mut guard = self
            .store
            .write()
            .map_err(|_| KvError::PoisonedLock)?;
        // Strict ordering: an entry must be exactly `applied + 1` (a duplicate
        // or a gap is an invariant violation => fail-stop).
        if index != guard.applied + 1 {
            return Err(KvError::IndexViolation { index });
        }

        // A no-op entry (empty payload) is how a leader records its term; it
        // advances the index but changes no state.
        if command.is_empty() {
            guard.applied = index;
            return Ok(ApplyOutcome::None);
        }

        if command.first() == Some(&OP_SESSION_GC) {
            let sessions = Self::parse_session_gc(command)?;
            for session in sessions {
                guard.sessions.remove(&session);
            }
            guard.applied = index;
            return Ok(ApplyOutcome::None);
        }

        let (session, op, key, val) = Self::parse_command(command)?;

        // Idempotency: a replayed session returns its cached result without
        // re-mutating the store.
        let outcome = match guard.sessions.get(&session) {
            Some(cached) => cached.clone(),
            None => {
                let outcome = match op {
                    Op::Put => {
                        // D-Ord: the entry (with its origin index) lands in the
                        // map first; `applied` is bumped last. The origin index
                        // is this command's log index — stable across deduped
                        // replays, which take the `Some(cached)` arm and skip
                        // this write entirely.
                        guard.kv.insert(
                            key.clone(),
                            Entry { val: val.clone(), index },
                        );
                        ApplyOutcome::Value(val)
                    }
                    Op::Delete => {
                        guard.kv.remove(&key);
                        ApplyOutcome::None
                    }
                };
                guard.sessions.insert(session, outcome.clone());
                outcome
            }
        };

        // Version is bumped LAST (D-Ord).
        guard.applied = index;
        Ok(outcome)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        // Read lock; the version and the map are read atomically (D-Arc/D-Ord).
        // The version is read before the map, so a reader never sees a map entry
        // from a future version.
        let guard = self
            .store
            .read()
            .map_err(|_| KvError::PoisonedLock)?;
        // Read the version (D-Arc: version-first) then the map, atomically.
        let _version = guard.applied;
        Ok(guard.kv.get(key).map(|entry| entry.val.clone()))
    }

    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        // Read lock; the version and the maps are read atomically (D-Arc/D-Ord).
        let guard = self
            .store
            .read()
            .map_err(|_| KvError::PoisonedLock)?;
        let mut buf = Vec::new();
        // First byte: state-machine snapshot format version (KV_SNAPSHOT_VERSION).
        // Rest of the payload: applied (u64 LE), kv entries, sessions — the
        // kv entry now appends an 8-byte LE `index` (value's origin) after
        // `[klen][k][vlen][v]`; sessions are unchanged.
        buf.push(KV_SNAPSHOT_VERSION);
        buf.extend_from_slice(&guard.applied.to_le_bytes());

        buf.extend_from_slice(&(guard.kv.len() as u32).to_le_bytes());
        for (k, v) in &guard.kv {
            buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
            buf.extend_from_slice(k);
            buf.extend_from_slice(&(v.val.len() as u32).to_le_bytes());
            buf.extend_from_slice(&v.val);
            // Value's origin: the log index of the entry that wrote this value.
            buf.extend_from_slice(&v.index.to_le_bytes());
        }

        buf.extend_from_slice(&(guard.sessions.len() as u32).to_le_bytes());
        for (session, outcome) in &guard.sessions {
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

    fn restore(&self, bytes: &[u8]) -> Result<(), Self::Error> {
        // The parse is done *outside* the write lock (2.4): a large snapshot's
        // decode cost never blocks concurrent reads. The write lock is then
        // held only for the swap of the fully-decoded Store, bounding the
        // critical section to the assignment rather than the decode. `bytes`
        // is fully validated before any mutation, so a malformed snapshot is
        // rejected without touching state (fail-stop, no partial restore).
        let next = decode_store(bytes)?;
        {
            let mut guard = self
                .store
                .write()
                .map_err(|_| KvError::PoisonedLock)?;
            *guard = next;
        }
        Ok(())
    }

    fn applied_index(&self) -> LogIndex {
        let guard = self
            .store
            .read()
            .expect("state machine read lock should never be poisoned");
        guard.applied
    }
}

impl KvStateMachine {
    /// Read the applied value **and** its origin index under a *single* read
    /// lock (the same D-Arc read as `StateMachine::get`).
    ///
    /// Returns `(None, None)` when the key is absent, so the public API never
    /// exposes `index 0`. When present, returns `(Some(val), Some(i))` with
    /// `i >= 1` (the first applied index is 1); the value and its index are
    /// observed coherently so they can never disagree about origin.
    pub fn get_with_index(
        &self,
        key: &[u8],
    ) -> Result<(Option<Vec<u8>>, Option<LogIndex>), KvError> {
        let guard = self
            .store
            .read()
            .map_err(|_| KvError::PoisonedLock)?;
        // Read the version (D-Arc: version-first) then the map, atomically.
        let _version = guard.applied;
        match guard.kv.get(key) {
            None => Ok((None, None)),
            Some(e) => {
                // A present entry always carries its origin; index 0 can only
                // reach an Entry via a restore bug, which would mean the state
                // machine itself is corrupt.
                debug_assert!(
                    e.index >= 1,
                    "present entry {key:?} has index {}, invariant violation",
                    e.index
                );
                Ok((Some(e.val.clone()), Some(e.index)))
            }
        }
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

/// Decode a snapshot byte string into a fresh [`Store`], entirely outside the
/// write lock (called by [`restore`](StateMachine::restore)). A malformed
/// snapshot yields [`KvError::MalformedSnapshot`] without touching existing
/// state. Keeping this out of the critical section bounds the write-lock hold
/// to the swap itself, so a large snapshot restore does not tail concurrent
/// reads (2.4).
fn decode_store(bytes: &[u8]) -> Result<Store, KvError> {
    let mut cursor = Cursor::new(bytes);
    // First byte is the snapshot format version. It must match
    // KV_SNAPSHOT_VERSION exactly; anything else (an old v0 payload that
    // lacks this byte, a corrupted/foreign byte, etc.) is rejected with
    // MalformedSnapshot. Fail-stop: we never fabricate an index (e.g. 0)
    // to tolerate an old payload.
    let version = read_u8(&mut cursor)?;
    if version != KV_SNAPSHOT_VERSION {
        return Err(KvError::MalformedSnapshot);
    }
    let applied = read_u64(&mut cursor)?;

    let kv_len = read_u32(&mut cursor)? as usize;
    let mut kv = BTreeMap::new();
    for _ in 0..kv_len {
        let klen = read_u32(&mut cursor)? as usize;
        let key = read_bytes(&mut cursor, klen)?;
        let vlen = read_u32(&mut cursor)? as usize;
        let val = read_bytes(&mut cursor, vlen)?;
        // Value's origin, appended after the value (post-0.2.0 payload).
        let index = read_u64(&mut cursor)?;
        kv.insert(key, Entry { val, index });
    }

    let sess_len = read_u32(&mut cursor)? as usize;
    let mut sessions = BTreeMap::new();
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
        sessions.insert(SessionKey { client_id, seq_no }, outcome);
    }

    // A well-formed snapshot has no trailing bytes.
    if cursor.position() != bytes.len() as u64 {
        return Err(KvError::MalformedSnapshot);
    }
    Ok(Store { applied, kv, sessions })
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
    fn session_gc_prunes_exactly_the_listed_sessions() {
        let mut sm = KvStateMachine::new();
        // Two sessions, distinguished by seq_no.
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"a", b"1"))
            .unwrap();
        sm.apply(2, &KvStateMachine::encode_put(2, 1, b"b", b"2"))
            .unwrap();
        assert_eq!(sm.session_count(), 2);
        assert!(sm.applied_session(1, 1) && sm.applied_session(2, 1));

        // A GC entry removes only what it lists, and does not disturb the data.
        let gc = KvStateMachine::encode_session_gc(&[(1, 1), (9, 9)]);
        sm.apply(3, &gc).unwrap();
        assert_eq!(sm.session_count(), 1);
        assert!(!sm.applied_session(1, 1));
        assert!(sm.applied_session(2, 1), "unlisted sessions survive");
        assert_eq!(sm.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(sm.session_count(), 1);

        // A GC entry is not a session command, so it never extends one.
        assert_eq!(KvStateMachine::command_session(&gc), None);
        // Malformed GC entries are rejected rather than partially applied.
        assert!(sm.apply(4, &[OP_SESSION_GC]).is_err());
        assert!(sm.apply(4, &KvStateMachine::encode_session_gc(&[(1, 1)])[..10]).is_err());
    }

    #[test]
    fn command_session_extracts_the_idempotency_key() {
        let put = KvStateMachine::encode_put(7, 3, b"k", b"v");
        assert_eq!(KvStateMachine::command_session(&put), Some((7, 3)));
        let del = KvStateMachine::encode_delete(9, 4, b"k");
        assert_eq!(KvStateMachine::command_session(&del), Some((9, 4)));
        // A no-op entry (the leader's term record) has no session.
        assert_eq!(KvStateMachine::command_session(&[]), None);
        // So does a truncated command.
        assert_eq!(KvStateMachine::command_session(&[1, 2, 3]), None);
    }

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

    // -----------------------------------------------------------------------
    // 2.1 TDD: the state machine must be *shareable* (Send + Sync, Arc-backed
    // data) and torn-read free under concurrent access. One writer applies a
    // monotonic sequence while several readers sample version + map atomically
    // (a single read lock), and the linear invariant holds for every sample.
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn concurrent_shared_sm_never_serves_torn_read() {
        const n_entries: u64 = 1024;
        const num_readers: usize = 8;
        const reads_per_reader: u64 = 500;

        // Empty sm; the writer (below) applies 1..=n_entries over time, so
        // readers observe it in flight.
        let shared: Arc<KvStateMachine> = Arc::new(KvStateMachine::new());
        let mut handles = Vec::new();

        // Writer: applies 1..=n_entries monotonically (map-then-version).
        let writer = Arc::clone(&shared);
        handles.push(tokio::spawn(async move {
            for i in 1..=n_entries {
                writer
                    .apply(
                        i,
                        &KvStateMachine::encode_put(
                            1,
                            i,
                            format!("k{i}").as_bytes(),
                            format!("v{i}").as_bytes(),
                        ),
                    )
                    .unwrap();
            }
        }));

        // Each reader loops: atomically read (version, kv) under one read lock
        // via read_store_copy, then assert the no-torn-read invariant — for
        // every key k_i with i <= version, the value present is exactly "v_i".
        for _ in 0..num_readers {
            let shared = Arc::clone(&shared);
            handles.push(tokio::spawn(async move {
                for _ in 0..reads_per_reader {
                    let (version, kv) = shared.read_store_copy();
                    for i in 1..=version {
                        let kstr = format!("k{i}");
                        let got = kv
                            .get(kstr.as_bytes())
                            .expect("torn read: key missing from the map at this version");
                        assert_eq!(
                            got.val.as_slice(),
                            format!("v{i}").as_bytes(),
                            "torn read: version={version}, key k{i} = {:?}",
                            got.val.as_slice()
                        );
                    }
                }
            }));
        }

        // Drain all tasks (writer + readers).
        for handle in handles {
            handle.await.expect("spawned task panicked");
        }

        // Final: all keys hold their final values (writer reached n_entries).
        for i in 1..=n_entries {
            assert_eq!(
                shared.get(&format!("k{i}").as_bytes()).unwrap(),
                Some(format!("v{i}").as_bytes().to_vec()),
                "key k{i} wrong after write storm"
            );
        }
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

    // -----------------------------------------------------------------------
    // I1/I3/I4/I5/I6 — value origin (commit index) semantics, design doc §6.
    // -----------------------------------------------------------------------

    /// I1: a key's origin index tracks the entry that wrote the current value,
    /// strictly increasing across rewrites.
    #[test]
    fn value_origin_index_tracks_apply_index() {
        let sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"k", b"v1"))
            .unwrap();
        assert_eq!(
            sm.get_with_index(b"k").unwrap(),
            (Some(b"v1".to_vec()), Some(1))
        );
        sm.apply(2, &KvStateMachine::encode_put(1, 2, b"k", b"v2"))
            .unwrap();
        assert_eq!(
            sm.get_with_index(b"k").unwrap(),
            (Some(b"v2".to_vec()), Some(2))
        );
        sm.apply(3, &KvStateMachine::encode_put(1, 3, b"k", b"v3"))
            .unwrap();
        assert_eq!(
            sm.get_with_index(b"k").unwrap(),
            (Some(b"v3".to_vec()), Some(3))
        );
    }

    /// I3: a deduped replay (same `(client_id, seq_no)` at a later index) does
    /// not re-mutate the entry, so the origin index is preserved.
    #[test]
    fn deduped_replay_preserves_origin_index() {
        let sm = KvStateMachine::new();
        let cmd = KvStateMachine::encode_put(7, 1, b"k", b"v");
        sm.apply(1, &cmd).unwrap();
        sm.apply(2, &cmd).unwrap(); // replay of the same session, deduped
        assert_eq!(
            sm.get_with_index(b"k").unwrap(),
            (Some(b"v".to_vec()), Some(1)),
            "replay must not move the origin index"
        );
        assert_eq!(sm.applied_index(), 2);
    }

    /// I4: a snapshot → restore round-trip preserves each value's origin index.
    #[test]
    fn snapshot_roundtrip_preserves_origin_index() {
        let sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"a", b"1"))
            .unwrap();
        sm.apply(2, &KvStateMachine::encode_put(2, 1, b"b", b"2"))
            .unwrap();
        let snap = sm.snapshot().unwrap();
        let sm2 = KvStateMachine::new();
        sm2.restore(&snap).unwrap();
        assert_eq!(
            sm2.get_with_index(b"a").unwrap(),
            (Some(b"1".to_vec()), Some(1))
        );
        assert_eq!(
            sm2.get_with_index(b"b").unwrap(),
            (Some(b"2".to_vec()), Some(2))
        );
        // Lossless: re-snapshotting the restored machine yields identical bytes.
        assert_eq!(sm2.snapshot().unwrap(), snap);
    }

    /// I5: a legacy / unknown snapshot payload is **rejected** (fail-stop) —
    /// no fabricated origin is ever accepted, and `index 0` never surfaces.
    #[test]
    fn old_or_unknown_snapshot_payload_is_rejected() {
        let sm = KvStateMachine::new();
        // A pre-feature payload has no version byte: it begins with `applied`
        // (u64 LE). First byte 0x08 != KV_SNAPSHOT_VERSION, so it is rejected.
        let mut legacy = Vec::new();
        legacy.extend_from_slice(&8u64.to_le_bytes()); // "applied" = 8
        legacy.extend_from_slice(&0u32.to_le_bytes()); // kv count = 0
        legacy.extend_from_slice(&0u32.to_le_bytes()); // sessions count = 0
        assert!(matches!(
            sm.restore(&legacy),
            Err(KvError::MalformedSnapshot)
        ));

        // Any version byte other than the current one is rejected too.
        let mut wrong = vec![2u8]; // KV_SNAPSHOT_VERSION is 1
        wrong.extend_from_slice(&0u64.to_le_bytes());
        wrong.extend_from_slice(&0u32.to_le_bytes());
        wrong.extend_from_slice(&0u32.to_le_bytes());
        assert!(matches!(
            sm.restore(&wrong),
            Err(KvError::MalformedSnapshot)
        ));
        // State is untouched by a rejected restore.
        assert_eq!(sm.applied_index(), 0);
        assert!(sm.get_with_index(b"k").unwrap() == (None, None));
    }

    /// I6: delete makes the key absent (`(None, None)` — no index exposed);
    /// a later put gets a fresh, higher origin index.
    #[test]
    fn delete_makes_key_absent_and_reput_raises_index() {
        let sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"k", b"v1"))
            .unwrap();
        sm.apply(2, &KvStateMachine::encode_delete(1, 2, b"k"))
            .unwrap();
        assert_eq!(sm.get_with_index(b"k").unwrap(), (None, None));
        sm.apply(3, &KvStateMachine::encode_put(1, 3, b"k", b"v2"))
            .unwrap();
        assert_eq!(
            sm.get_with_index(b"k").unwrap(),
            (Some(b"v2".to_vec()), Some(3)),
            "re-put after delete must carry an index above the whole history"
        );
    }
}
