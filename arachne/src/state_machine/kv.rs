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
/// Atomic multi-key put (M2-P1B): one command writes a whole key set under
/// **one** log entry and one `(client_id, seq_no)` session, atomically (all or
/// nothing, single apply). Outcome is `None` (the write-back pipeline carries
/// the success; there is no value result).
const OP_MULTI_PUT: u8 = 3;
/// Compare-and-swap (M3): `compare(key, pred) → op(success|failure)` on a
/// **single key**. Success applies `op` (put/delete) and returns the put value /
/// `None`; predicate miss returns [`ApplyOutcome::CasFailed`] with the current
/// state — and the failed session is still recorded, so a replay returns the
/// cached failure without recomputing the compare.
const OP_CAS: u8 = 4;

/// The upper bound on a single `multi_put`'s total key+value payload, in bytes
/// (M2-P1B, §5.2). Enforced at the `Handle` boundary (`validate_multi_put`)
/// *before* propose, so an over-limit command never enters the log.
///
/// The bound is **deliberately below the transport's default max message size**
/// (8 MiB, `arachne-transport-tonic` `DEFAULT_MAX_MESSAGE_SIZE`): a raft entry
/// (and a forwarded command) travels as one whole gRPC message with the raft
/// framing on top, so a batch this size is guaranteed to replicate/forward
/// without exceeding the wire cap. Raising it requires raising the transport's
/// `max_message_size` to match.
pub const MAX_MULTI_PUT_TOTAL_BYTES: u64 = 4 * 1024 * 1024;

/// The largest `multi_put` entry count. Bounded so the encoded command (and the
/// lock hold during its single apply) stays proportional even before byte
/// checks run; `Handle::validate_multi_put` rejects beyond this.
pub const MAX_MULTI_PUT_ENTRIES: usize = 4096;

/// The largest number of entries a single range/prefix stale read will return,
/// regardless of the caller-supplied `limit` (M2-P1A, §4.2). The state machine
/// clamps to this so a caller can never ask it to clone the whole map under the
/// read lock (a `limit = usize::MAX` would otherwise drain memory and block
/// apply while holding the lock).
pub const MAX_STALE_RANGE_ENTRIES: usize = 10_000;

/// The state-machine snapshot payload format version.
///
/// State-machine-internal; **independent of** the storage layer's
/// [`crate::storage::meta::FORMAT_VERSION`]. It is the first byte of the
/// snapshot payload (see [`snapshot`](StateMachine::snapshot)) and is
/// validated by [`decode_store`]: a payload with a wrong version byte is
/// rejected with [`KvError::MalformedSnapshot`] (fail-stop, no migration — a
/// v0 payload lacks this byte entirely, and fabricating `index 0` would be
/// an illegal public value that could be persisted and propagated).
///
/// v2 (M3): the session-table tag space grew from `0/1` (`None`/`Value`) to
/// `0/1/2` (`+ CasFailed`) so a failed CAS outcome survives
/// snapshot→restore; the same single M3 format package bumps the storage
/// `FORMAT_VERSION` (§2.2).
const KV_SNAPSHOT_VERSION: u8 = 2;

/// The kind of command, parsed from its opcode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Put,
    Delete,
}

/// The compare predicate of a compare-and-swap (M3 §6.1).
///
/// Three predicates (oracle-locked: an index alone cannot express an absent
/// key):
/// 1. [`IndexEquals`](CasPred::IndexEquals) — the key's origin index equals
///    `index` (monotonic, no ABA, aligns with `get_stale_with_index`;
///    **recommended**). An absent key never matches; pair with
///    [`NotExists`](CasPred::NotExists) for create-if-absent.
/// 2. [`ValueEquals`](CasPred::ValueEquals) — the key's value equals `bytes`
///    (compatible-intuitive, internal second choice).
/// 3. [`NotExists`](CasPred::NotExists) — the key is absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CasPred {
    /// The key's origin index equals `>= 1`; an absent key never matches.
    IndexEquals(u64),
    /// The key's value equals `bytes`; an absent key never matches.
    ValueEquals(Vec<u8>),
    /// The key is absent (create-if-absent).
    NotExists,
}

/// The success operation of a compare-and-swap (M3 §6.1): what the command
/// applies when the predicate matches. The failure branch is always a no-op
/// (first version: single-key only, no nested/multi-branch txn).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CasOp {
    /// Write `value` to the key on success.
    Put(Vec<u8>),
    /// Delete the key on success.
    Delete,
}

/// The client-visible verdict of a compare-and-swap (M3 §6). `ArachneError`
/// is NOT used for a failed compare: a failed CAS is a legal *result*, not an
/// error (a caller must be able to tell "definitely did not apply" from
/// "result unknown"/Timeout). Carries the current state so the caller can
/// build the next attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CasResult {
    /// The predicate matched and the success operation applied.
    Applied,
    /// The predicate did not match; nothing was applied. Carries the state
    /// the compare observed for the retry loop.
    NotApplied {
        /// The current origin index (`0` when the key is absent).
        current_index: u64,
        /// The current value (`None` when the key is absent).
        current_value: Option<Vec<u8>>,
    },
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

    /// Encode an atomic multi-put command (M2-P1B).
    ///
    /// Layout: `[op:1][client_id:8][seq_no:8][count:u32][(klen:u32 key vlen:u32 val) × count]`
    /// — a single session envelope for the whole batch, one log entry, one
    /// apply. Callers validate the batch (per-key size, total bytes,
    /// entry count) *before* proposing (`Handle::validate_multi_put`);
    /// [`parse_multi_put`](KvStateMachine::parse_multi_put) re-checks the
    /// structural bounds defensively at apply time.
    pub fn encode_multi_put(client_id: u64, seq_no: u64, entries: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(17 + entries.len() * 8);
        buf.push(OP_MULTI_PUT);
        buf.extend_from_slice(&client_id.to_le_bytes());
        buf.extend_from_slice(&seq_no.to_le_bytes());
        buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for (key, val) in entries {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
            buf.extend_from_slice(val);
        }
        buf
    }

    /// Encode a compare-and-swap command (M3).
    ///
    /// Layout:
    /// `[op:1][client_id:8][seq_no:8][key_len:u32][key][pred_tag:1]`
    /// followed by the predicate payload and the success op:
    /// * `IndexEquals`: `[0][index:u64 LE]`
    /// * `ValueEquals`: `[1][len:u32][value]`
    /// * `NotExists`: `[2]`
    /// then `[op_tag:1]`: `[0][len:u32][value]` for `Put`, `[1]` for `Delete`.
    pub fn encode_cas(
        client_id: u64,
        seq_no: u64,
        key: &[u8],
        pred: &CasPred,
        success: &CasOp,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(OP_CAS);
        buf.extend_from_slice(&client_id.to_le_bytes());
        buf.extend_from_slice(&seq_no.to_le_bytes());
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        match pred {
            CasPred::IndexEquals(index) => {
                buf.push(0);
                buf.extend_from_slice(&index.to_le_bytes());
            }
            CasPred::ValueEquals(value) => {
                buf.push(1);
                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(value);
            }
            CasPred::NotExists => buf.push(2),
        }
        match success {
            CasOp::Put(value) => {
                buf.push(0);
                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(value);
            }
            CasOp::Delete => buf.push(1),
        }
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
        if !matches!(
            cmd.first(),
            Some(&OP_PUT) | Some(&OP_DELETE) | Some(&OP_MULTI_PUT) | Some(&OP_CAS)
        ) {
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

    /// Decode a multi-put command, rejecting a malformed one rather than
    /// partially applying a prefix of it.
    ///
    /// Layout (mirror of [`encode_multi_put`](KvStateMachine::encode_multi_put)):
    /// `[op:1][client_id:8][seq_no:8][count:u32][(klen:u32 key vlen:u32 val) × count]`.
    fn parse_multi_put(cmd: &[u8]) -> Result<(SessionKey, Vec<(Vec<u8>, Vec<u8>)>), KvError> {
        const HEADER: usize = 1 + 8 + 8 + 4; // op + cid + seq + count
        if cmd.first() != Some(&OP_MULTI_PUT) || cmd.len() < HEADER {
            return Err(KvError::MalformedCommand);
        }
        let client_id = u64::from_le_bytes(
            cmd.get(1..9)
                .ok_or(KvError::MalformedCommand)?
                .try_into()
                .map_err(|_| KvError::MalformedCommand)?,
        );
        let seq_no = u64::from_le_bytes(
            cmd.get(9..17)
                .ok_or(KvError::MalformedCommand)?
                .try_into()
                .map_err(|_| KvError::MalformedCommand)?,
        );
        let session = SessionKey { client_id, seq_no };
        let count = u32::from_le_bytes(
            cmd.get(17..21)
                .ok_or(KvError::MalformedCommand)?
                .try_into()
                .map_err(|_| KvError::MalformedCommand)?,
        ) as usize;
        if count > MAX_MULTI_PUT_ENTRIES {
            return Err(KvError::MalformedCommand);
        }
        let mut rest = cmd.get(21..).ok_or(KvError::MalformedCommand)?;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let klen = u32::from_le_bytes(
                rest.get(0..4)
                    .ok_or(KvError::MalformedCommand)?
                    .try_into()
                    .map_err(|_| KvError::MalformedCommand)?,
            ) as usize;
            rest = rest.get(4..).ok_or(KvError::MalformedCommand)?;
            let key = rest
                .get(..klen)
                .ok_or(KvError::MalformedCommand)?
                .to_vec();
            rest = rest.get(klen..).ok_or(KvError::MalformedCommand)?;
            let vlen = u32::from_le_bytes(
                rest.get(0..4)
                    .ok_or(KvError::MalformedCommand)?
                    .try_into()
                    .map_err(|_| KvError::MalformedCommand)?,
            ) as usize;
            rest = rest.get(4..).ok_or(KvError::MalformedCommand)?;
            let val = rest
                .get(..vlen)
                .ok_or(KvError::MalformedCommand)?
                .to_vec();
            rest = rest.get(vlen..).ok_or(KvError::MalformedCommand)?;
            entries.push((key, val));
        }
        if !rest.is_empty() {
            return Err(KvError::MalformedCommand);
        }
        Ok((session, entries))
    }

    /// Decode a compare-and-swap command, rejecting a malformed one rather
    /// than partially applying it.
    ///
    /// Layout (mirror of [`encode_cas`](KvStateMachine::encode_cas)):
    /// `[op:1][cid:8][seq:8][klen:u32][key]` + predicate + success op.
    fn parse_cas(cmd: &[u8]) -> Result<(SessionKey, Vec<u8>, CasPred, CasOp), KvError> {
        const HEADER: usize = 1 + 8 + 8; // op + cid + seq
        if cmd.first() != Some(&OP_CAS) || cmd.len() < HEADER {
            return Err(KvError::MalformedCommand);
        }
        let client_id = u64::from_le_bytes(
            cmd.get(1..9)
                .ok_or(KvError::MalformedCommand)?
                .try_into()
                .map_err(|_| KvError::MalformedCommand)?,
        );
        let seq_no = u64::from_le_bytes(
            cmd.get(9..17)
                .ok_or(KvError::MalformedCommand)?
                .try_into()
                .map_err(|_| KvError::MalformedCommand)?,
        );
        let session = SessionKey { client_id, seq_no };

        let mut rest = cmd.get(HEADER..).ok_or(KvError::MalformedCommand)?;
        let klen = u32::from_le_bytes(
            rest.get(0..4)
                .ok_or(KvError::MalformedCommand)?
                .try_into()
                .map_err(|_| KvError::MalformedCommand)?,
        ) as usize;
        rest = rest.get(4..).ok_or(KvError::MalformedCommand)?;
        let key = rest
            .get(..klen)
            .ok_or(KvError::MalformedCommand)?
            .to_vec();
        rest = rest.get(klen..).ok_or(KvError::MalformedCommand)?;

        let pred = match rest.first() {
            Some(0) => {
                let index = u64::from_le_bytes(
                    rest.get(1..9)
                        .ok_or(KvError::MalformedCommand)?
                        .try_into()
                        .map_err(|_| KvError::MalformedCommand)?,
                );
                rest = rest.get(9..).ok_or(KvError::MalformedCommand)?;
                CasPred::IndexEquals(index)
            }
            Some(1) => {
                let vlen = u32::from_le_bytes(
                    rest.get(1..5)
                        .ok_or(KvError::MalformedCommand)?
                        .try_into()
                        .map_err(|_| KvError::MalformedCommand)?,
                ) as usize;
                let value = rest
                    .get(5..5 + vlen)
                    .ok_or(KvError::MalformedCommand)?
                    .to_vec();
                rest = rest.get(5 + vlen..).ok_or(KvError::MalformedCommand)?;
                CasPred::ValueEquals(value)
            }
            Some(2) => {
                rest = rest.get(1..).ok_or(KvError::MalformedCommand)?;
                CasPred::NotExists
            }
            _ => return Err(KvError::MalformedCommand),
        };

        let success = match rest.first() {
            Some(0) => {
                let vlen = u32::from_le_bytes(
                    rest.get(1..5)
                        .ok_or(KvError::MalformedCommand)?
                        .try_into()
                        .map_err(|_| KvError::MalformedCommand)?,
                ) as usize;
                let value = rest
                    .get(5..5 + vlen)
                    .ok_or(KvError::MalformedCommand)?
                    .to_vec();
                rest = rest.get(5 + vlen..).ok_or(KvError::MalformedCommand)?;
                CasOp::Put(value)
            }
            Some(1) => {
                rest = rest.get(1..).ok_or(KvError::MalformedCommand)?;
                CasOp::Delete
            }
            _ => return Err(KvError::MalformedCommand),
        };
        if !rest.is_empty() {
            return Err(KvError::MalformedCommand);
        }
        Ok((session, key, pred, success))
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
    /// **Test-only helper.** The runtime does **not** use this to detect a
    /// proposed command's commit/apply — the M1 write-back pipeline carries
    /// each command's [`ApplyOutcome`] from the apply task to the actor
    /// directly, so a reply never consults this (a post-apply re-read could
    /// mis-read a concurrent same-batch write; design §9.1). Session-TTL and
    /// dedup tests use it to assert which sessions are cached.
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

        // Atomic multi-put (M2-P1B): one command applies a whole key set under
        // a single lock hold and a single `(client_id, seq_no)` session. All
        // entries share this command's log index (same atomic apply), so a
        // reader observes either all of them or none of them at one watermark.
        // A replayed session returns its cached `None` without re-mutating —
        // exactly-once, like the single-key ops.
        if command.first() == Some(&OP_MULTI_PUT) {
            let (session, entries) = Self::parse_multi_put(command)?;
            let cached = guard.sessions.get(&session).cloned();
            if cached.is_none() {
                for (key, val) in entries {
                    guard
                        .kv
                        .insert(key, Entry { val, index });
                }
                guard.sessions.insert(session, ApplyOutcome::None);
            }
            guard.applied = index;
            return Ok(ApplyOutcome::None);
        }

        // Compare-and-swap (M3 §6): `compare(key, pred) → op(success|failure)`.
        // A matching predicate applies `op` (put/delete, same apply index);
        // a miss produces `ApplyOutcome::CasFailed` carrying the state the
        // compare saw — and the FAILED session is still recorded, so a replay
        // returns the cached failure without recomputing the compare
        // (exactly-once: a re-evaluation could flip an originally-failed CAS
        // to success and break the "at most one outcome" invariant, §6.2.2).
        if command.first() == Some(&OP_CAS) {
            let (session, key, pred, success) = Self::parse_cas(command)?;
            let cached = guard.sessions.get(&session).cloned();
            let outcome = match cached {
                Some(cached) => cached,
                None => {
                    // Evaluate the predicate against the CURRENT stored state
                    // (still under the write lock; no torn read).
                    let current = guard.kv.get(&key);
                    let matched = match &pred {
                        CasPred::IndexEquals(want) => {
                            current.map(|e| e.index == *want).unwrap_or(false)
                        }
                        CasPred::ValueEquals(want) => {
                            current.map(|e| e.val == *want).unwrap_or(false)
                        }
                        CasPred::NotExists => current.is_none(),
                    };
                    let outcome = if matched {
                        match success {
                            CasOp::Put(value) => {
                                guard.kv.insert(key, Entry { val: value.clone(), index });
                                ApplyOutcome::Value(value)
                            }
                            CasOp::Delete => {
                                guard.kv.remove(&key);
                                ApplyOutcome::None
                            }
                        }
                    } else {
                        let (current_index, current_value) = match current {
                            Some(e) => (
                                e.index,
                                // Cloned current value for the client's retry.
                                Some(e.val.clone()),
                            ),
                            None => (0, None),
                        };
                        ApplyOutcome::CasFailed {
                            current_index,
                            current_value,
                        }
                    };
                    // Record BOTH outcomes — success and failure — in the
                    // session table (failure too: §6.2.2).
                    guard.sessions.insert(session, outcome.clone());
                    outcome
                }
            };
            guard.applied = index;
            return Ok(outcome);
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
                // M3: a failed CAS is a cached outcome and must survive the
                // snapshot→restore round-trip (KV_SNAPSHOT_VERSION 2). Layout:
                // `[tag=2][current_index:u64 LE][has_value:1][val]*`.
                ApplyOutcome::CasFailed {
                    current_index,
                    current_value,
                } => {
                    buf.push(2);
                    buf.extend_from_slice(&current_index.to_le_bytes());
                    match current_value {
                        Some(val) => {
                            buf.push(1);
                            buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
                            buf.extend_from_slice(val);
                        }
                        None => buf.push(0),
                    }
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

    /// Consistent **range** read (M2-P1A): every `(key, val)` in `[start, end)`
    /// plus the **applied index** observed under the same read lock.
    ///
    /// This is the D-Arc invariant applied to a range: a single lock hold
    /// yields a whole segment of the map that belongs to one applied
    /// watermark, so the caller can use the returned index as the atomic
    /// version of the entire result (no torn read across the range — the
    /// `[a,b)` half-open read is never a mix of two applied states).
    ///
    /// Semantics: `[start, end)` half-open; a **full** empty range is
    /// expressed as `start == end == 0x00`-free — see [`RangeBounds`]
    /// handling — but the convention here is: an empty `start` and empty
    /// `end` returns **all** keys. The caller computes a byte-prefix range by
    /// `start = prefix` and `end = prefix + 0x00` padded to the next byte
    /// (see `Handle::get_stale_prefix`).
    ///
    /// `limit` bounds the returned entry count; `truncated` reports whether
    /// more keys existed past the cut (M2-P1B §4.2: bounded first version —
    /// never return an unbounded wall of keys in one reply).
    pub fn get_range_with_index(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, LogIndex, bool), KvError> {
        let guard = self
            .store
            .read()
            .map_err(|_| KvError::PoisonedLock)?;
        // Read the version (D-Arc: version-first) then the range, atomically.
        let applied = guard.applied;
        // Server-side hard cap: never clone more than MAX_STALE_RANGE_ENTRIES
        // under the read lock, no matter what the caller asks for (§4.2).
        let limit = limit.min(MAX_STALE_RANGE_ENTRIES);
        let mut out = Vec::new();
        let mut truncated = false;
        // A reversed / degenerate `[start, end)` (start >= end, both non-empty)
        // is an empty range by definition. `BTreeMap::range` would panic on
        // `start > end`, so return the empty result instead (the Handle
        // rejects a clearly-reversed request with `InvalidArgument`; this is
        // the defensive state-machine floor).
        let reversed = !start.is_empty() && !end.is_empty() && start >= end;
        if !reversed {
            let range = if start.is_empty() && end.is_empty() {
                None
            } else {
                let lo = if start.is_empty() {
                    std::ops::Bound::Unbounded
                } else {
                    std::ops::Bound::Included(start.to_vec())
                };
                let hi = if end.is_empty() {
                    std::ops::Bound::Unbounded
                } else {
                    std::ops::Bound::Excluded(end.to_vec())
                };
                Some((lo, hi))
            };
            match range {
                Some((lo, hi)) => {
                    for (k, e) in guard.kv.range((lo, hi)) {
                        if out.len() >= limit {
                            truncated = true;
                            break;
                        }
                        out.push((k.clone(), e.val.clone()));
                    }
                }
                None => {
                    for (k, e) in &guard.kv {
                        if out.len() >= limit {
                            truncated = true;
                            break;
                        }
                        out.push((k.clone(), e.val.clone()));
                    }
                }
            }
        }
        Ok((out, applied, truncated))
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
            // M3: a cached failed CAS. Layout mirrors the encoder:
            // `[tag=2][current_index:u64][has_value:1][val]*` (KV_SNAPSHOT_VERSION 2).
            2 => {
                let current_index = read_u64(&mut cursor)?;
                let has_value = read_u8(&mut cursor)?;
                match has_value {
                    1 => {
                        let vlen = read_u32(&mut cursor)? as usize;
                        let val = read_bytes(&mut cursor, vlen)?;
                        ApplyOutcome::CasFailed {
                            current_index,
                            current_value: Some(val),
                        }
                    }
                    0 => ApplyOutcome::CasFailed {
                        current_index,
                        current_value: None,
                    },
                    _ => return Err(KvError::MalformedSnapshot),
                }
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
        let mut wrong = vec![3u8]; // KV_SNAPSHOT_VERSION is 2 (bumped at M3)
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

    // ---- M2-P1B: atomic multi-put -------------------------------------------

    /// A multi-put writes the whole batch atomically: every key lands under
    /// the **same** origin index (the command's log index).
    #[test]
    fn multi_put_writes_batch_atomically_with_shared_index() {
        let sm = KvStateMachine::new();
        // Advance the machine to index 6 (no-op term records) so the batch's
        // origin index is a non-trivial `7`.
        for i in 1..=6u64 {
            sm.apply(i, &[]).unwrap();
        }
        let entries: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
        let outcome = sm
            .apply(7, &KvStateMachine::encode_multi_put(1, 1, entries))
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::None);
        // Every key exists, all with origin index 7 (the batch's apply index).
        assert_eq!(sm.get_with_index(b"a").unwrap(), (Some(b"1".to_vec()), Some(7)));
        assert_eq!(sm.get_with_index(b"b").unwrap(), (Some(b"2".to_vec()), Some(7)));
        assert_eq!(sm.get_with_index(b"c").unwrap(), (Some(b"3".to_vec()), Some(7)));
    }

    /// Duplicate keys inside one batch: the later entry wins (documented
    /// semantic), and still a single origin index.
    #[test]
    fn multi_put_duplicate_keys_last_wins() {
        let sm = KvStateMachine::new();
        let entries: &[(&[u8], &[u8])] = &[(b"k", b"first"), (b"k", b"second")];
        sm.apply(1, &KvStateMachine::encode_multi_put(1, 1, entries))
            .unwrap();
        assert_eq!(
            sm.get_with_index(b"k").unwrap(),
            (Some(b"second".to_vec()), Some(1)),
            "a later duplicate key must overwrite the earlier one"
        );
    }

    /// Replaying the same `(client_id, seq_no)` returns the cached `None`
    /// outcome and does **not** re-apply the batch (exactly-once, like
    /// single-key writes).
    #[test]
    fn multi_put_replay_is_idempotent() {
        let sm = KvStateMachine::new();
        let cmd = KvStateMachine::encode_multi_put(42, 9, &[(b"a", b"1"), (b"b", b"2")]);
        sm.apply(1, &cmd).unwrap();
        // A replay at a new index with the *same* session returns the cache and
        // leaves the store at its original apply.
        let replay = sm.apply(2, &cmd).unwrap();
        assert_eq!(replay, ApplyOutcome::None);
        assert_eq!(sm.get_with_index(b"a").unwrap(), (Some(b"1".to_vec()), Some(1)));
        assert_eq!(sm.get_with_index(b"b").unwrap(), (Some(b"2".to_vec()), Some(1)));
    }

    /// Over-limit / malformed batches are rejected rather than partially
    /// applied: the count cap and the structural bounds are enforced at apply.
    #[test]
    fn multi_put_rejects_malformed_and_over_bounds() {
        // Count exceeding MAX_MULTI_PUT_ENTRIES must be rejected.
        let sm = KvStateMachine::new();
        let mut over = Vec::new();
        over.push(OP_MULTI_PUT);
        over.extend_from_slice(&1u64.to_le_bytes()); // client_id
        over.extend_from_slice(&1u64.to_le_bytes()); // seq_no
        over.extend_from_slice(&((MAX_MULTI_PUT_ENTRIES + 1) as u32).to_le_bytes());
        assert!(matches!(
            sm.apply(1, &over),
            Err(KvError::MalformedCommand)
        ));
        // A declared count that outruns the payload is malformed too. A
        // rejected batch does not advance `applied`, so this must also land at
        // the next expected index (1) on a fresh machine.
        let sm = KvStateMachine::new();
        let cmd = KvStateMachine::encode_multi_put(1, 2, &[(b"a", b"1")]);
        let cut = &cmd[..cmd.len() - 2];
        assert!(matches!(
            sm.apply(1, cut),
            Err(KvError::MalformedCommand)
        ));
        // A truncated command carrying the op byte only. A rejected batch does
        // not advance `applied`, so this lands at the next expected index (1)
        // on the same machine.
        assert!(matches!(
            sm.apply(1, &[OP_MULTI_PUT]),
            Err(KvError::MalformedCommand)
        ));
        // Nothing was applied by any rejected batch.
        assert_eq!(sm.applied_index(), 0);
        assert_eq!(sm.get_with_index(b"a").unwrap(), (None, None));
    }

    /// `command_session` recognizes a multi-put (same `(client_id, seq_no)`
    /// envelope layout) so the runtime can attribute it to a waiting proposal.
    #[test]
    fn multi_put_command_session_is_attributed() {
        let cmd = KvStateMachine::encode_multi_put(55, 3, &[(b"a", b"1")]);
        assert_eq!(KvStateMachine::command_session(&cmd), Some((55, 3)));
    }

    // ---- M2-P1A: consistent range read --------------------------------------

    /// An empty `start`+`end` is the full map; entries come back in byte order
    /// with the applied index.
    #[test]
    fn range_read_full_map_is_ordered_with_index() {
        let sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"b", b"2"))
            .unwrap();
        sm.apply(2, &KvStateMachine::encode_put(1, 2, b"a", b"1"))
            .unwrap();
        let (entries, index, truncated) = sm.get_range_with_index(b"", b"", 100).unwrap();
        assert_eq!(
            entries,
            vec![(b"a".to_vec(), b"1".to_vec()), (b"b".to_vec(), b"2".to_vec())],
            "full range must be byte-ordered"
        );
        assert_eq!(index, 2);
        assert!(!truncated);
    }

    /// Half-open `[a, b)` semantics: `a` included, `b` excluded; missing
    /// endpoints are unbounded (empty = unbounded).
    #[test]
    fn range_read_respects_half_open_bounds() {
        let sm = KvStateMachine::new();
        let keys: [&[u8]; 5] = [b"aa", b"ab", b"ac", b"b", b"ba"];
        for (i, k) in keys.into_iter().enumerate() {
            sm.apply((i + 1) as u64, &KvStateMachine::encode_put(1, (i + 1) as u64, k, b"v"))
                .unwrap();
        }
        // [ab, b) → ab, ac (aa < ab excluded; b excluded).
        let (entries, _, _) = sm.get_range_with_index(b"ab", b"b", 100).unwrap();
        assert_eq!(
            entries,
            vec![(b"ab".to_vec(), b"v".to_vec()), (b"ac".to_vec(), b"v".to_vec())]
        );
        // Unbounded lower: `[.., b)` = everything before `b`.
        let (entries, _, _) = sm.get_range_with_index(b"", b"b", 100).unwrap();
        assert_eq!(
            entries,
            vec![
                (b"aa".to_vec(), b"v".to_vec()),
                (b"ab".to_vec(), b"v".to_vec()),
                (b"ac".to_vec(), b"v".to_vec())
            ]
        );
        // Unbounded upper: `[b, ..)` = b and after.
        let (entries, _, _) = sm.get_range_with_index(b"b", b"", 100).unwrap();
        assert_eq!(
            entries,
            vec![(b"b".to_vec(), b"v".to_vec()), (b"ba".to_vec(), b"v".to_vec())]
        );
    }

    /// `limit` bounds the returned entries and sets `truncated`.
    #[test]
    fn range_read_honors_limit_and_truncation() {
        let sm = KvStateMachine::new();
        let keys: [&[u8]; 4] = [b"a", b"b", b"c", b"d"];
        for (i, k) in keys.into_iter().enumerate() {
            sm.apply((i + 1) as u64, &KvStateMachine::encode_put(1, (i + 1) as u64, k, b"v"))
                .unwrap();
        }
        let (entries, _, truncated) = sm.get_range_with_index(b"", b"", 2).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(truncated, "more keys existed past the limit");
        let (entries, _, truncated) = sm.get_range_with_index(b"", b"", 4).unwrap();
        assert_eq!(entries.len(), 4);
        assert!(!truncated);
    }

    /// A prefix range behaves like `[prefix, successor(prefix))` — keys that
    /// merely start with the prefix but extend it are included, longer-mismatch
    /// keys are not.
    #[test]
    fn range_read_prefix_semantics() {
        let sm = KvStateMachine::new();
        let keys: [&[u8]; 4] = [b"head", b"head\x00e1", b"head\x00e2", b"other"];
        for (i, k) in keys.into_iter().enumerate() {
            sm.apply((i + 1) as u64, &KvStateMachine::encode_put(1, (i + 1) as u64, k, b"v"))
                .unwrap();
        }
        // `[prefix, successor(prefix))` for the prefix `head\x00e1`: the
        // successor bumps the last non-0xFF byte (`e1` → `e2`) and truncates,
        // yielding exactly `head\x00e2`.
        let prefix = b"head\x00e1";
        let end = b"head\x00e2";
        let (entries, _, _) = sm.get_range_with_index(prefix, end, 100).unwrap();
        assert_eq!(entries, vec![(b"head\x00e1".to_vec(), b"v".to_vec())]);

        // The prefix `head` (no embedded null) reaches all `head*` keys: its
        // successor bumps the trailing `d` → `e`.
        let (entries, _, _) = sm.get_range_with_index(b"head", b"heae", 100).unwrap();
        assert_eq!(
            entries,
            vec![
                (b"head".to_vec(), b"v".to_vec()),
                (b"head\x00e1".to_vec(), b"v".to_vec()),
                (b"head\x00e2".to_vec(), b"v".to_vec())
            ]
        );
    }

    /// A range read observes a whole segment at one applied watermark — writing
    /// after the read does not tear the earlier result.
    #[test]
    fn range_read_index_advances_with_apply() {
        let sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"a", b"1"))
            .unwrap();
        let (_, index1, _) = sm.get_range_with_index(b"", b"", 100).unwrap();
        assert_eq!(index1, 1);
        sm.apply(2, &KvStateMachine::encode_put(1, 2, b"b", b"2"))
            .unwrap();
        let (_, index2, _) = sm.get_range_with_index(b"", b"", 100).unwrap();
        assert_eq!(index2, 2);
    }

    /// A reversed / degenerate `[start, end)` (start >= end, both non-empty)
    /// returns an empty result rather than panicking inside `BTreeMap::range`.
    #[test]
    fn range_read_reversed_returns_empty() {
        let sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"a", b"1"))
            .unwrap();
        // start == end is a legal empty range (no panic, nothing to return).
        let (entries, index, truncated) = sm.get_range_with_index(b"b", b"b", 10).unwrap();
        assert!(entries.is_empty());
        assert_eq!(index, 1);
        assert!(!truncated);
        // start > end must not panic; returns empty.
        let (entries, _, _) = sm.get_range_with_index(b"z", b"a", 10).unwrap();
        assert!(entries.is_empty());
    }

    /// The server-side cap `MAX_STALE_RANGE_ENTRIES` bounds the reply even when
    /// the caller passes an unbounded `limit`.
    #[test]
    fn range_read_is_capped_by_server_side_limit() {
        let sm = KvStateMachine::new();
        for i in 0..(MAX_STALE_RANGE_ENTRIES + 5) as u64 {
            sm.apply(
                i + 1,
                &KvStateMachine::encode_put(1, i + 1, format!("k{i:06}").as_bytes(), b"v"),
            )
            .unwrap();
        }
        let (entries, _, truncated) = sm
            .get_range_with_index(b"", b"", usize::MAX)
            .unwrap();
        assert!(
            entries.len() <= MAX_STALE_RANGE_ENTRIES,
            "server-side cap must clamp the reply, got {}",
            entries.len()
        );
        assert!(truncated, "more keys existed past the cap");
    }

    // ---- M3: compare-and-swap ----------------------------------------------

    /// `IndexEquals` matches the current origin index and applies the success
    /// op (recommended predicate, J3).
    #[test]
    fn cas_index_equals_hits_and_applies() {
        let sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"k", b"old"))
            .unwrap();
        // CAS on the current index: success, writes the new value.
        let outcome = sm
            .apply(
                2,
                &KvStateMachine::encode_cas(1, 2, b"k", &CasPred::IndexEquals(1), &CasOp::Put(b"new".to_vec())),
            )
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::Value(b"new".to_vec()));
        assert_eq!(sm.get_with_index(b"k").unwrap(), (Some(b"new".to_vec()), Some(2)));
    }

    /// `IndexEquals` on a *stale* index (the key moved) fails with `CasFailed`
    /// carrying the current state — and the compare is never re-applied.
    #[test]
    fn cas_index_equals_stale_fails_with_current_state() {
        let sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"k", b"v1"))
            .unwrap();
        sm.apply(2, &KvStateMachine::encode_put(1, 2, b"k", b"v2"))
            .unwrap();
        // Compare against index 1; the key now sits at index 2.
        let outcome = sm
            .apply(
                3,
                &KvStateMachine::encode_cas(1, 3, b"k", &CasPred::IndexEquals(1), &CasOp::Put(b"v3".to_vec())),
            )
            .unwrap();
        assert_eq!(
            outcome,
            ApplyOutcome::CasFailed {
                current_index: 2,
                current_value: Some(b"v2".to_vec()),
            }
        );
        // Nothing was written; the key is untouched.
        assert_eq!(sm.get_with_index(b"k").unwrap(), (Some(b"v2".to_vec()), Some(2)));
    }

    /// `NotExists` is create-if-absent: succeeds on an absent key, fails (with
    /// the now-present current state) on an existing one.
    #[test]
    fn cas_not_exists_creates_absent_but_rejects_present() {
        let sm = KvStateMachine::new();
        // Absent key: create succeeds.
        let outcome = sm
            .apply(
                1,
                &KvStateMachine::encode_cas(1, 1, b"k", &CasPred::NotExists, &CasOp::Put(b"v".to_vec())),
            )
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::Value(b"v".to_vec()));
        // Present key: create-if-absent fails, carrying the current value.
        let outcome = sm
            .apply(
                2,
                &KvStateMachine::encode_cas(1, 2, b"k", &CasPred::NotExists, &CasOp::Put(b"x".to_vec())),
            )
            .unwrap();
        assert_eq!(
            outcome,
            ApplyOutcome::CasFailed {
                current_index: 1,
                current_value: Some(b"v".to_vec()),
            }
        );
        assert_eq!(sm.get_with_index(b"k").unwrap(), (Some(b"v".to_vec()), Some(1)));
    }

    /// `ValueEquals` is the compatible-intuitive predicate: exact value match.
    #[test]
    fn cas_value_equals_hits_and_misses() {
        let sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"k", b"abc"))
            .unwrap();
        // Hit.
        let outcome = sm
            .apply(
                2,
                &KvStateMachine::encode_cas(1, 2, b"k", &CasPred::ValueEquals(b"abc".to_vec()), &CasOp::Put(b"def".to_vec())),
            )
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::Value(b"def".to_vec()));
        // Miss.
        let outcome = sm
            .apply(
                3,
                &KvStateMachine::encode_cas(1, 3, b"k", &CasPred::ValueEquals(b"nope".to_vec()), &CasOp::Put(b"?".to_vec())),
            )
            .unwrap();
        assert_eq!(
            outcome,
            ApplyOutcome::CasFailed {
                current_index: 2,
                current_value: Some(b"def".to_vec()),
            }
        );
    }

    /// A failed CAS records its session, so a replay returns the **cached**
    /// `CasFailed` without recomputing the compare (J3: exactly-once — a
    /// re-evaluation could flip an originally-failed CAS to success).
    #[test]
    fn cas_failure_is_cached_and_replay_returns_it() {
        let sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"k", b"v"))
            .unwrap();
        // A CAS that fails (stale index).
        let cmd = KvStateMachine::encode_cas(42, 7, b"k", &CasPred::IndexEquals(99), &CasOp::Put(b"x".to_vec()));
        let first = sm.apply(2, &cmd).unwrap();
        assert!(matches!(first, ApplyOutcome::CasFailed { .. }));
        // Replay the same (client_id, seq_no) at a new index: cached failure.
        let replay = sm.apply(3, &cmd).unwrap();
        assert_eq!(replay, first, "a replayed CAS returns its cached failure");
        assert_eq!(
            sm.get_with_index(b"k").unwrap(),
            (Some(b"v".to_vec()), Some(1)),
            "the key is untouched by the failed CAS (and its replay)"
        );
    }

    /// A failed CAS outcome survives a snapshot → restore round-trip
    /// (KV_SNAPSHOT_VERSION 2 encodes tag=2).
    #[test]
    fn cas_failure_survives_snapshot_restore() {
        let sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"k", b"v"))
            .unwrap();
        let cmd = KvStateMachine::encode_cas(42, 7, b"k", &CasPred::IndexEquals(99), &CasOp::Delete);
        sm.apply(2, &cmd).unwrap();

        let snap = sm.snapshot().unwrap();
        let sm2 = KvStateMachine::new();
        sm2.restore(&snap).unwrap();

        // The rebuilt machine snapshots to identical bytes *before* any new
        // apply (the restore alone must be lossless; a re-apply would move the
        // index and legitimately change the watermark in the bytes).
        assert_eq!(sm2.snapshot().unwrap(), snap);

        // The replayed session on the rebuilt machine returns the cached
        // CasFailed (the compare is not recomputed against current state) —
        // without re-mutating anything.
        let replay = sm2.apply(3, &cmd).unwrap();
        assert_eq!(
            replay,
            ApplyOutcome::CasFailed {
                current_index: 1,
                current_value: Some(b"v".to_vec()),
            }
        );
        // The key still holds the same value (no delete re-applied).
        assert_eq!(sm2.get_with_index(b"k").unwrap(), (Some(b"v".to_vec()), Some(1)));
    }

    /// `IndexEquals` after a delete → re-put: the new value has a fresh
    /// (higher) origin index, so a compare against the old index fails (I6
    /// style — the origin really tracks the writing entry).
    #[test]
    fn cas_index_equals_sees_reput_origin() {
        let sm = KvStateMachine::new();
        sm.apply(1, &KvStateMachine::encode_put(1, 1, b"k", b"v1"))
            .unwrap();
        sm.apply(2, &KvStateMachine::encode_delete(1, 2, b"k"))
            .unwrap();
        sm.apply(3, &KvStateMachine::encode_put(1, 3, b"k", b"v2"))
            .unwrap();
        // Compare against the *old* index (1): the key now lives at 3.
        let outcome = sm
            .apply(
                4,
                &KvStateMachine::encode_cas(1, 4, b"k", &CasPred::IndexEquals(1), &CasOp::Delete),
            )
            .unwrap();
        assert_eq!(
            outcome,
            ApplyOutcome::CasFailed {
                current_index: 3,
                current_value: Some(b"v2".to_vec()),
            }
        );
    }

    /// A CAS on an absent key with a value predicate fails, carrying an absent
    /// current state (`current_index: 0`, `current_value: None`).
    #[test]
    fn cas_value_equals_on_absent_key_reports_absent() {
        let sm = KvStateMachine::new();
        let outcome = sm
            .apply(
                1,
                &KvStateMachine::encode_cas(1, 1, b"nope", &CasPred::ValueEquals(b"x".to_vec()), &CasOp::Put(b"y".to_vec())),
            )
            .unwrap();
        assert_eq!(
            outcome,
            ApplyOutcome::CasFailed {
                current_index: 0,
                current_value: None,
            }
        );
    }

    /// `command_session` recognizes a CAS (same envelope layout).
    #[test]
    fn cas_command_session_is_attributed() {
        let cmd = KvStateMachine::encode_cas(55, 3, b"k", &CasPred::NotExists, &CasOp::Put(b"v".to_vec()));
        assert_eq!(KvStateMachine::command_session(&cmd), Some((55, 3)));
    }

    /// Malformed CAS payloads are rejected without partial application.
    #[test]
    fn malformed_cas_is_rejected() {
        let sm = KvStateMachine::new();
        assert!(matches!(
            sm.apply(1, &[OP_CAS]),
            Err(KvError::MalformedCommand)
        ));
        // A valid header but a missing predicate byte. (A rejected command
        // does not advance `applied`, so the next attempt is again at index 1.)
        let mut bad =
            Vec::from(&KvStateMachine::encode_cas(1, 1, b"k", &CasPred::NotExists, &CasOp::Put(b"v".to_vec()))[..]);
        let header_key = 1 + 8 + 8 + 4 + 1; // op + cid + seq + klen + k
        assert!(matches!(
            sm.apply(1, &bad[..header_key]),
            Err(KvError::MalformedCommand)
        ));
        bad.clear();
    }
}
