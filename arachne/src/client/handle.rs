//! The [`Handle`]: the cheap-`Clone` client handle (propsol §3.1/§3.3).
//!
//! A `Handle` is the client's entry point to one node's runtime actor. It is
//! cheap to clone (it wraps an `Arc`), so many tasks can share one handle. Each
//! handle owns:
//!
//! * a command channel to its node's runtime actor (the write/read path),
//! * a per-handle `client_id` and a monotonically increasing `seq_no` (the
//!   session envelope, propsol §2.3; full session dedup lands in M3),
//! * the in-process peer handles used for client-side redirect (propsol §3.3),
//! * the `NodeId → SocketAddr` address map used to build leader hints,
//! * the key/value size limits (validated before propose, propsol §3.2).
//!
//! # Redirect / retry (propsol §3.3)
//!
//! An operation that returns [`ArachneError::NotLeader`] is retried against the
//! hinted peer (if it is a known in-process peer) or, failing that, the next
//! known peer in deterministic order. Up to [`Handle::MAX_REDIRECTS`] (3)
//! redirects are followed, bounded by a total deadline of one election timeout,
//! after which the call returns [`ArachneError::Timeout`]. No leader known /
//! quorum lost returns [`ArachneError::QuorumUnavailable`].
//!
//! [`Handle::without_redirect`] yields a single-shot clone that skips the whole
//! redirect policy and returns `NotLeader{hint}` verbatim; the node's HTTP
//! surface uses it so a multi-process follower can answer `409` + leader hint
//! instead of collapsing to `QuorumUnavailable`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use crate::client::ArachneError;
use crate::runtime::Command;
use crate::state_machine::{
    CasOp, CasPred, CasResult, KvStateMachine, MAX_MULTI_PUT_ENTRIES, MAX_MULTI_PUT_TOTAL_BYTES,
    WatchEvent,
};
use crate::{
    ApplyOutcome, ForwardCommand, NodeId, ProfileConfig, RaftId, RemoteForwarder,
};
use raft_seedable::eraftpb::ConfChangeType;

/// The maximum number of client-side redirects (propsol §3.3: "default 3").
const MAX_REDIRECTS: u32 = 3;

/// The cheap-`Clone` client handle for one node.
#[derive(Clone)]
pub struct Handle {
    inner: Arc<HandleInner>,
    /// The redirect budget for **this** handle (a clone-local setting, not part
    /// of the shared [`HandleInner`]). [`Handle::MAX_REDIRECTS`] for a normal
    /// client handle; `0` for a single-shot handle ([`Handle::without_redirect`])
    /// that returns `NotLeader{hint}` verbatim.
    max_redirects: u32,
}

impl Handle {
    /// The maximum number of redirects followed for a single operation.
    pub const MAX_REDIRECTS: u32 = MAX_REDIRECTS;

    /// Create the local handle for a node (no peers registered yet).
    ///
    /// This is the entry point used by the node runtime
    /// ([`crate::runtime::Runtime::new`]); callers register peers with
    /// [`Handle::register_peer`] after all handles are built.
    pub(crate) fn new_local(
        self_id: NodeId,
        tx: mpsc::Sender<Command>,
        remote: Option<Arc<dyn RemoteForwarder>>,
        profile: &ProfileConfig,
    ) -> Self {
        Self {
            inner: Arc::new(HandleInner {
                self_id,
                client_id: next_client_id(),
                seq: AtomicU64::new(1),
                tx,
                peers: RwLock::new(HashMap::new()),
                max_key_bytes: profile.max_key_bytes,
                max_value_bytes: profile.max_value_bytes,
                // A write attempt is bounded by one election timeout: a write
                // that does not commit within one election window is stalled
                // (propsol §2.4 N3) and is reported as a Timeout.
                timeout: Duration::from_millis(profile.election_timeout_ms.max(1)),
                // A **read** must outlive the runtime actor's own ReadIndex
                // budget, or the client would give up while the actor was still
                // resolving the read. The actor waits `read_index_timeout`
                // (= 2 × election) per attempt and retries once, so the client's
                // bound is two such waits plus one election timeout of
                // scheduling margin.
                read_timeout: Duration::from_millis(
                    profile
                        .read_index_timeout_ms
                        .max(1)
                        .saturating_mul(2)
                        .saturating_add(profile.election_timeout_ms.max(1)),
                ),
                remote,
            }),
            max_redirects: MAX_REDIRECTS,
        }
    }

    /// A clone of this handle that performs a **single attempt** against this
    /// node and returns [`ArachneError::NotLeader`] (hint included) verbatim
    /// instead of following the hint to a peer (propsol §3.3).
    ///
    /// A node's own HTTP surface uses this. In a multi-process deployment there
    /// are no in-process peer handles to redirect to, so the leader hint must
    /// reach the external caller — the node renders it as `409 Conflict` with a
    /// leader body — rather than being collapsed into
    /// [`ArachneError::QuorumUnavailable`].
    pub fn without_redirect(&self) -> Handle {
        Handle {
            inner: Arc::clone(&self.inner),
            max_redirects: 0,
        }
    }

    /// Register an in-process peer handle, enabling client-side redirect to it.
    ///
    /// Call this for every peer once all handles are constructed. The peer's
    /// `NodeId` is taken from the peer handle itself.
    pub fn register_peer(&self, peer: Handle) {
        let mut peers = lock_write(&self.inner.peers);
        peers.insert(peer.inner.self_id.clone(), peer);
    }

    /// The `NodeId` this handle is bound to.
    pub fn node_id(&self) -> &NodeId {
        &self.inner.self_id
    }

    /// Linearizable write (propsol §2.1). Validates the key/value against the
    /// profile limits **before** proposing (fail-stop discipline, propsol §3.2),
    /// then proposes and waits for commit+apply (bounded → `Timeout`).
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), ArachneError> {
        self.validate_put(key, value)?;
        let seq_no = self.inner.seq.fetch_add(1, Ordering::SeqCst);
        let client_id = self.inner.client_id;
        let cmd = KvStateMachine::encode_put(client_id, seq_no, key, value);
        self.propose_with_redirect(cmd, client_id, seq_no)
            .await
            .map(|_| ())
    }

    /// Atomic multi-key write (M2-P1B): one command, one log entry, one session
    /// that sets **all** `entries` atomically (all or nothing, single apply).
    ///
    /// This is the "whole-tree replace" primitive: hydra re-sets every key of a
    /// tree in one propose instead of N single-key writes (N proposes → 1).
    /// Duplicate keys within one batch are legal; the later entry wins.
    ///
    /// Violations of the per-key size limits or the batch's total-byte bound
    /// are rejected with [`ArachneError::InvalidArgument`] *before* proposing
    /// (fail-stop discipline, propsol §3.2) — the batch never enters the log.
    pub async fn multi_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), ArachneError> {
        self.validate_multi_put(entries)?;
        let seq_no = self.inner.seq.fetch_add(1, Ordering::SeqCst);
        let client_id = self.inner.client_id;
        let cmd = KvStateMachine::encode_multi_put(client_id, seq_no, entries);
        self.propose_with_redirect(cmd, client_id, seq_no)
            .await
            .map(|_| ())
    }

    /// Compare-and-swap (M3 §6): `compare(key, pred) → op(success|failure)`
    /// under one log entry and one session.
    ///
    /// A matching predicate applies `success` (put/delete); a miss applies
    /// nothing and returns [`CasResult::NotApplied`] carrying the current
    /// state, so the caller can re-read and retry. A failed compare is a legal
    /// **result**, not an error — the caller can always distinguish "definitely
    /// did not apply" from "result unknown" (Timeout).
    ///
    /// Recommended predicate: [`CasPred::IndexEquals`] paired with
    /// `get_stale_with_index`'s origin (`stale read → CAS(i) → re-put`, the
    /// optimistic-concurrency loop); an absent key is expressed with
    /// [`CasPred::NotExists`]. This is a general RMW primitive — it does not
    /// paper over multi-writer interleavings across keys (those belong to a
    /// full txn, out of scope).
    pub async fn cas(
        &self,
        key: &[u8],
        pred: CasPred,
        success: CasOp,
    ) -> Result<CasResult, ArachneError> {
        self.validate_key(key)?;
        if let CasOp::Put(value) = &success
            && (value.len() as u64) > self.inner.max_value_bytes
        {
            return Err(ArachneError::InvalidArgument(format!(
                "cas value of {} bytes exceeds max_value_bytes ({})",
                value.len(),
                self.inner.max_value_bytes
            )));
        }
        // The `ValueEquals` predicate's payload rides the command too: it must
        // respect the same size discipline (propsol §3.2 / plan §2.3), or an
        // over-limit compare could reach the log and fail to replicate past the
        // transport's max_message_size (the same trap M2's multi-put total cap
        // closed).
        if let CasPred::ValueEquals(want) = &pred
            && (want.len() as u64) > self.inner.max_value_bytes
        {
            return Err(ArachneError::InvalidArgument(format!(
                "cas ValueEquals predicate of {} bytes exceeds max_value_bytes ({})",
                want.len(),
                self.inner.max_value_bytes
            )));
        }
        let seq_no = self.inner.seq.fetch_add(1, Ordering::SeqCst);
        let client_id = self.inner.client_id;
        let cmd = KvStateMachine::encode_cas(client_id, seq_no, key, &pred, &success);
        let outcome = self.propose_with_redirect(cmd, client_id, seq_no).await?;
        // Map the apply outcome to the public verdict. The M1 write-back
        // pipeline carries `CasFailed` here; a fresh retry loop reads the
        // current state off `NotApplied`.
        match outcome {
            ApplyOutcome::Value(_) | ApplyOutcome::None => Ok(CasResult::Applied),
            ApplyOutcome::CasFailed {
                current_index,
                current_value,
            } => Ok(CasResult::NotApplied {
                current_index,
                current_value,
            }),
        }
    }

    /// **Test-only** (feature `fault-injection`): propose a raw state-machine
    /// command under an explicit session.
    ///
    /// `put`/`delete` mint a fresh `seq_no` per call, so a *retry* — the one
    /// case session idempotency is about — cannot be reproduced through them.
    /// The session-TTL tests use this to send the same `(client_id, seq_no)`
    /// twice, through the real actor.
    #[cfg(feature = "fault-injection")]
    pub async fn propose_raw(
        &self,
        cmd: Vec<u8>,
        client_id: u64,
        seq_no: u64,
    ) -> Result<(), ArachneError> {
        self.propose_with_redirect(cmd, client_id, seq_no)
            .await
            .map(|_| ())
    }

    /// **M1 observability seam** (`#[doc(hidden)]`, temporary): propose `cmd`
    /// under an explicit session and return the applied [`ApplyOutcome`] the
    /// write-back pipeline carried back from the apply task, instead of
    /// discarding it like [`Handle::put`] does.
    ///
    /// M1's acceptance requires a test that can actually see the outcome (a
    /// `put` regression cannot: its outcome equals its input and the public API
    /// drops it). M3's CAS/multi-put replace this with permanent APIs.
    #[doc(hidden)]
    pub async fn propose_with_outcome(
        &self,
        cmd: Vec<u8>,
        client_id: u64,
        seq_no: u64,
    ) -> Result<ApplyOutcome, ArachneError> {
        self.propose_with_redirect(cmd, client_id, seq_no).await
    }

    /// Add `node_id` to the cluster as a **learner** (propsol §5.3).
    ///
    /// A learner receives the log but does not vote, so adding one cannot
    /// affect quorum. Promotion is a separate, explicit step: promote only once
    /// the learner has caught up (S3 enforces the lag gate).
    ///
    /// At most one membership change may be outstanding; a second one is
    /// rejected with [`ArachneError::ConfChangePending`]. Ordinary writes are
    /// unaffected.
    pub async fn add_learner(&self, node_id: RaftId) -> Result<(), ArachneError> {
        self.conf_change(ConfChangeType::AddLearnerNode, node_id)
            .await
    }

    /// Promote a learner to a voting member (propsol §5.3).
    ///
    /// See [`Handle::add_learner`] for the single-flight rule.
    pub async fn promote_learner(&self, node_id: RaftId) -> Result<(), ArachneError> {
        self.conf_change(ConfChangeType::AddNode, node_id).await
    }

    /// Remove a member (propsol §5.3).
    ///
    /// Removing the **current leader** is a two-step sequence and is performed
    /// here automatically: leadership is handed to another voter first, and the
    /// removal is then proposed by the new leader — after the transfer the node
    /// being removed is no longer able to propose anything, so this composition
    /// is necessarily client-side. If the transfer fails (no quorum, nobody to
    /// hand over to) the removal fails with that error and **nothing is
    /// proposed**: removing a leader without quorum is by design not possible.
    pub async fn remove_member(&self, node_id: RaftId) -> Result<(), ArachneError> {
        match self.conf_change(ConfChangeType::RemoveNode, node_id).await {
            Err(ArachneError::LeaderRemovalRequiresTransfer) => {
                self.hand_over_leadership().await?;
                self.conf_change(ConfChangeType::RemoveNode, node_id).await
            }
            other => other,
        }
    }

    /// The applied membership of this node: `(voters, learners)` (propsol
    /// §5.3, rev S).
    ///
    /// Local-state read: it answers on any node, leader or not, which is what
    /// an operator inspecting a cluster wants — and it cannot hang on a lost
    /// quorum.
    pub async fn membership(&self) -> Result<(Vec<RaftId>, Vec<RaftId>), ArachneError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.inner
            .tx
            .send(Command::Membership { ack: ack_tx })
            .await
            .map_err(|_| ArachneError::ShuttingDown)?;
        let deadline = Instant::now() + self.inner.timeout;
        self.await_oneshot(ack_rx, deadline).await?
    }

    /// Hand leadership to `node_id` (propsol §5.3; Q3 makes this public).
    ///
    /// Resolves once the leadership has **actually moved**, not when raft
    /// accepts the request: a node that acknowledged a transfer it never
    /// completed would be reporting progress it did not make.
    pub async fn transfer_leader(&self, node_id: RaftId) -> Result<(), ArachneError> {
        self.request_with_redirect(Request::TransferLeader {
            target: Some(node_id),
        })
        .await
        .map(|_| ())
    }

    /// Hand leadership to any other voter (propsol §5.3 hard constraint 3).
    async fn hand_over_leadership(&self) -> Result<(), ArachneError> {
        self.request_with_redirect(Request::TransferLeader { target: None })
            .await
            .map(|_| ())
    }

    /// Propose one membership change, following leader hints.
    async fn conf_change(
        &self,
        change_type: ConfChangeType,
        node_id: RaftId,
    ) -> Result<(), ArachneError> {
        self.request_with_redirect(Request::ConfChange {
            change_type,
            node_id,
        })
        .await
        .map(|_| ())
    }

    /// **Test-only** (feature `fault-injection`): propose a single-step
    /// membership change, going through the real redirect loop.
    ///
    /// Kept so M3's routing and gate tests can drive changes at a level below
    /// the public API (e.g. to fire one while another is in flight). Like
    /// [`Handle::propose_raw`] this is deliberately not part of the API.
    #[cfg(feature = "fault-injection")]
    pub async fn propose_conf_change_raw(
        &self,
        change_type: ConfChangeType,
        node_id: RaftId,
    ) -> Result<(), ArachneError> {
        self.conf_change(change_type, node_id).await
    }

    /// Linearizable delete (propsol §2.1). See [`Handle::put`].
    pub async fn delete(&self, key: &[u8]) -> Result<(), ArachneError> {
        self.validate_key(key)?;
        let seq_no = self.inner.seq.fetch_add(1, Ordering::SeqCst);
        let client_id = self.inner.client_id;
        let cmd = KvStateMachine::encode_delete(client_id, seq_no, key);
        self.propose_with_redirect(cmd, client_id, seq_no)
            .await
            .map(|_| ())
    }

    /// Linearizable read (propsol §2.1 / §5.4).
    ///
    /// This is a **ReadIndex** read: the request is redirected to the current
    /// leader (a non-leader returns [`ArachneError::NotLeader`], and the client
    /// follows the hint to an in-process peer or the seeds), which confirms it
    /// still leads with a quorum heartbeat round before serving the value once
    /// the read index is applied. It does not rely on a leader lease or clock
    /// drift, so it stays linearizable (propsol §1 — lease reads are forbidden
    /// in v1). Bounded by the operation deadline (`Timeout`).
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ArachneError> {
        self.validate_key(key)?;
        self.get_with_redirect(key).await
    }

    /// Arbitrary stale read (propsol §2.1 / N1): a direct read of this node's
    /// applied state machine. Works on any node (no redirect), may be stale,
    /// and is **not monotone** across calls.
    pub async fn get_stale(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ArachneError> {
        self.validate_key(key)?;
        let deadline = Instant::now() + self.inner.timeout;
        let (ack_tx, ack_rx) = oneshot::channel();
        self.inner
            .tx
            .send(Command::GetStale {
                key: key.to_vec(),
                ack: ack_tx,
            })
            .await
            .map_err(|_| ArachneError::ShuttingDown)?;
        self.await_oneshot(ack_rx, deadline).await?
    }

    /// Stale read of an **atomic prefix**: every `(key, value)` whose key has
    /// byte-prefix `prefix`, plus the **applied index** the whole segment was
    /// observed at (M2-P1A). The index is the atomic version of the result: the
    /// caller can treat `(keys, values, index)` as one consistent snapshot and
    /// order on `index` — e.g. batch-decision without a quorum round.
    ///
    /// Same contract as [`Handle::get_stale`]: local, weak, may be stale, no
    /// quorum. The prefix is a raw byte prefix (keyspace in byte-lexicographic
    /// order, as `BTreeMap` orders them); `[prefix, prefix + successor)` is the
    /// half-open range returned. `limit` bounds the returned entries; the
    /// `bool` is `true` when more keys existed past `limit` (truncated).
    pub async fn get_stale_prefix(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, u64, bool), ArachneError> {
        self.validate_key(prefix)?;
        // The byte-successor range end: `prefix` with the last byte carrying a
        // `0xFF` bumped, `0x00`-padded beyond, so [prefix, end) is exactly the
        // prefix's slice of the keyspace. A prefix that is all `0xFF` has no
        // successor — an empty `end` means unbounded in the state machine.
        let end = successor(prefix);
        self.get_stale_range(prefix, end.as_deref().unwrap_or(&[]), limit)
            .await
    }

    /// Stale read of a **half-open key range** `[start, end)` plus the applied
    /// index the segment was observed at (M2-P1A). Consistent: one read lock,
    /// no torn mix of two applied states. `start.is_empty()` = unbounded lower,
    /// `end.is_empty()` = unbounded upper (both empty = the whole map).
    ///
    /// Same contract as [`Handle::get_stale`]: local, weak, may be stale, no
    /// quorum. `limit` bounds the returned entries; the `bool` is `true` when
    /// more keys existed past `limit` (truncated). See
    /// [`Handle::get_stale_prefix`] for the prefix short-hand.
    #[allow(clippy::type_complexity)]
    pub async fn get_stale_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, u64, bool), ArachneError> {
        self.validate_key(start)?;
        self.validate_key(end)?;
        // A clearly-reversed `[start, end)` (start > end, both non-empty) has
        // no keys; reject it as an invalid argument instead of letting the
        // state machine hit `BTreeMap::range`'s start-greater-than-end panic
        // inside the read-reply task (which would surface as a misleading
        // `ShuttingDown` and, on abort builds, let a remote client crash the
        // node). `start == end` (both non-empty) is a legal empty range.
        if !start.is_empty() && !end.is_empty() && start > end {
            return Err(ArachneError::InvalidArgument(format!(
                "range start {:?} is greater than end {:?}",
                String::from_utf8_lossy(start),
                String::from_utf8_lossy(end)
            )));
        }
        let deadline = Instant::now() + self.inner.timeout;
        let (ack_tx, ack_rx) = oneshot::channel();
        self.inner
            .tx
            .send(Command::GetStaleRange {
                start: start.to_vec(),
                end: end.to_vec(),
                limit,
                ack: ack_tx,
            })
            .await
            .map_err(|_| ArachneError::ShuttingDown)?;
        self.await_oneshot(ack_rx, deadline).await?
    }

    /// Stale read plus the **origin index** of the returned value: the log
    /// index of the entry that wrote it, always `>= 1` for a present key.
    ///
    /// Same contract as [`Handle::get_stale`] (propsol §2.1 / N1): a direct
    /// local state-machine read on any node, no quorum, may be stale and is
    /// not monotone across calls — but the value and its origin are observed
    /// coherently, so an external client can order on the origin without a
    /// quorum round. Returns `Ok(None)` for an absent key.
    pub async fn get_stale_with_index(
        &self,
        key: &[u8],
    ) -> Result<Option<(Vec<u8>, u64)>, ArachneError> {
        self.validate_key(key)?;
        let deadline = Instant::now() + self.inner.timeout;
        let (ack_tx, ack_rx) = oneshot::channel();
        self.inner
            .tx
            .send(Command::GetStaleWithIndex {
                key: key.to_vec(),
                ack: ack_tx,
            })
            .await
            .map_err(|_| ArachneError::ShuttingDown)?;
        self.await_oneshot(ack_rx, deadline).await?
    }

    /// Open a **watch** over a byte prefix (M4/P2, design D3/D5): a consistent
    /// prefix snapshot at the current applied watermark + a stream of write-set
    /// events *after* that watermark.
    ///
    /// The consumer reconstructs the prefix state as `snapshot ∪ events`:
    /// every event has `event.index > snapshot_index`, so nothing is missed or
    /// duplicated between the two (a strict `>` filter; the snapshot itself
    /// already contains the entry at its own index). Events are ordered and
    /// monotone in `index`.
    ///
    /// The subscription's receiver is **bounded** (design D2): a consumer that
    /// does not drain fast enough is **disconnected** — the channel closes with
    /// no silent gap — and the caller must re-`watch` (re-snapshot). `limit`
    /// bounds the snapshot size; a prefix whose snapshot would exceed it is
    /// rejected (`Busy`) rather than returned truncated (a truncated snapshot
    /// would silently miss the older keys).
    pub async fn watch(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<WatchSubscription, ArachneError> {
        self.validate_key(prefix)?;
        let end = successor(prefix);
        let deadline = Instant::now() + self.inner.timeout;
        let (ack_tx, ack_rx) = oneshot::channel();
        self.inner
            .tx
            .send(Command::Watch {
                start: prefix.to_vec(),
                end: end.unwrap_or_default(),
                limit,
                ack: ack_tx,
            })
            .await
            .map_err(|_| ArachneError::ShuttingDown)?;
        let (snapshot, applied_index, truncated, events) =
            self.await_oneshot(ack_rx, deadline).await??;
        Ok(WatchSubscription {
            snapshot,
            applied_index,
            truncated,
            events,
        })
    }

    /// Report this node's current leader hint: a `(NodeId, SocketAddr)` if a
    /// leader is known, else `None` (no leader / quorum lost). Bounded by the
    /// operation deadline (a `None` reply means the node simply has no leader).
    pub async fn leader_hint(&self) -> Option<(NodeId, SocketAddr)> {
        let deadline = Instant::now() + self.inner.timeout;
        let (ack_tx, ack_rx) = oneshot::channel();
        if self
            .inner
            .tx
            .send(Command::LeaderHint { ack: ack_tx })
            .await
            .is_err()
        {
            return None;
        }
        self.await_oneshot(ack_rx, deadline)
            .await
            .ok()
            .flatten()
    }

    // ---- redirect (propsol §3.3) --------------------------------------------

    /// Map a stable wire error code (produced by the leader's command sink,
    /// [`crate::runtime::ForwardCommandSink::code`]) to the corresponding client
    /// error. The `not_leader` case never appears as a code: it is signalled by a
    /// `ForwardOutcome` carrying a fresh `leader_hint`, which drives re-redirect
    /// below.
    fn code_to_error(code: &str) -> ArachneError {
        match code {
            "quorum_unavailable" => ArachneError::QuorumUnavailable,
            "timeout" => ArachneError::Timeout,
            "busy" => ArachneError::Busy,
            "session_expired" => ArachneError::SessionExpired,
            "session_table_full" => ArachneError::SessionTableFull,
            "shutting_down" => ArachneError::ShuttingDown,
            // Anything else is an unrecoverable condition on the leader side
            // (unknown kind, unavailable sink, …).
            other => ArachneError::Unrecoverable(format!("forwarded request failed: {other}")),
        }
    }

    /// Forward a write request to the leader over the wire (propsol v0.2.19).
    ///
    /// Multi-process analogue of the in-process redirect: when the hinted leader
    /// is not a known in-process peer (a follower with no local peer handles),
    /// the command is carried to the leader's gRPC `Forward` RPC via the
    /// transport's [`RemoteForwarder`].
    ///
    /// Returns:
    /// - `Ok(Ok(()))`            — the forwarded write committed.
    /// - `Ok(Err(NotLeader{hint}))` — the leader re-redirected us (fresh hint;
    ///   the caller must re-feed it).
    /// - `Ok(Err(other))`         — the leader reported a stable failure code.
    /// - `Err(e)`                 — the forward itself failed (dial/connect/
    ///   timeout); treat as an unreachable leader.
    async fn forward_request(
        &self,
        remote: &Arc<dyn RemoteForwarder>,
        leader_id: NodeId,
        leader_addr: SocketAddr,
        req: &Request,
        deadline: Instant,
    ) -> Result<Result<ApplyOutcome, ArachneError>, ArachneError> {
        // Only writes are forwardable in M1. `ConfChange`/`TransferLeader` are
        // only ever proposed by the node itself to a known in-process peer,
        // so they never reach this path; refuse rather than hang if they do.
        let forward_cmd = match req {
            Request::Propose {
                cmd,
                client_id,
                seq_no,
            } => ForwardCommand::Propose {
                cmd: cmd.clone(),
                client_id: *client_id,
                seq_no: *seq_no,
            },
            _ => return Err(ArachneError::QuorumUnavailable),
        };

        match tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            remote.forward(leader_id, leader_addr, forward_cmd),
        ).await
        {
            Ok(Ok(outcome)) => {
                // A `not_leader` outcome carries a *fresh* hint (re-redirect);
                // otherwise the write succeeded, and the outcome is what the
                // target's apply task computed and wrote back (M1). A node
                // predating the outcome field returns `result: None`, which the
                // caller treats as `ApplyOutcome::None` (the M1 default).
                match outcome.leader_hint {
                    Some((id, a)) => Ok(Err(ArachneError::NotLeader {
                        leader_hint: Some((id, a)),
                    })),
                    None => Ok(Ok(outcome.result.unwrap_or(ApplyOutcome::None))),
                }
            }
            // A stable code from the leader's command sink (a non-`not_leader`
            // outcome never carries a hint).
            Ok(Err(code)) => Ok(Err(Handle::code_to_error(&code))),
            // Dial/connect/timeout: the leader is unreachable.
            Err(_) => Err(ArachneError::QuorumUnavailable),
        }
    }

    /// Reach `target_id` for `req`, choosing the in-process or wire path, and
    /// return the *inner* result the redirect loop routes (`Ok(outcome)` on
    /// success, `Err(e)` for the node's verdict) or a reachability error
    /// (`Err(e)` on the outer side, e.g. an unreachable leader or a gone peer).
    async fn reach_target(
        &self,
        target_id: &NodeId,
        wire_addr: Option<SocketAddr>,
        req: &Request,
        deadline: Instant,
    ) -> Result<Result<ApplyOutcome, ArachneError>, ArachneError> {
        // In-process: a known in-process peer (self or a registered peer).
        if self.handle_for(target_id).is_some() {
            return self.send_request(target_id, req, deadline).await;
        }
        // No in-process peer: forward to the leader over the wire (the multi-
        // process case). `wire_addr` is only `Some` when it came from a hint.
        if let (Some(remote), Some(addr)) = (&self.inner.remote, wire_addr) {
            return self
                .forward_request(remote, target_id.clone(), addr, req, deadline)
                .await;
        }
        // No path to the target: the cluster cannot be reached.
        Err(ArachneError::QuorumUnavailable)
    }

    /// Propose `cmd` under `(client_id, seq_no)` and return the applied
    /// [`ApplyOutcome`] the write-back pipeline carried from the apply task
    /// (M1). Public write methods discard the outcome and return `()`; the
    /// outcome is what M3's CAS surfaces.
    async fn propose_with_redirect(
        &self,
        cmd: Vec<u8>,
        client_id: u64,
        seq_no: u64,
    ) -> Result<ApplyOutcome, ArachneError> {
        self.request_with_redirect(Request::Propose {
            cmd,
            client_id,
            seq_no,
        })
        .await
    }

    /// Send `req` to the leader, following hints, under one deadline.
    ///
    /// Writes and membership changes differ only in which command they put on
    /// the actor's channel, so they share this loop: a follower answers
    /// `NotLeader{hint}` for both, and both must not treat an unreachable peer
    /// as "this node is shutting down".
    ///
    /// The hint drives the target to reach: in-process when it's a known peer,
    /// over the wire otherwise (the multi-process case). A fresh hint re-feeds
    /// the loop toward the current leader.
    ///
    /// Returns the applied [`ApplyOutcome`] for a write (M1 write-back). Reads
    /// and membership changes do not go through this path; the caller maps the
    /// outcome to its public shape.
    async fn request_with_redirect(
        &self,
        req: Request,
    ) -> Result<ApplyOutcome, ArachneError> {
        let deadline = Instant::now() + self.inner.timeout;
        let order = self.target_order();
        let mut pos = 0usize;
        let mut redirects = 0u32;
        let mut unreachable = 0u32;
        // The most recent known leader. While `None`, the loop walks the
        // in-process order (self, then peers); once set, it drives the target
        // to reach (via the wire if the leader is not an in-process peer).
        let mut hint: Option<(NodeId, SocketAddr)> = None;
        loop {
            // Target to reach: the latest known leader, else the next in-process
            // target. `wire_addr` is `Some` only when it came from a hint (i.e.
            // the target is reached over the wire).
            let (target_id, wire_addr) = match &hint {
                Some((id, a)) => (id.clone(), Some(a.clone())),
                None => (order[pos].clone(), None),
            };
            let result = match self
                .reach_target(&target_id, wire_addr, &req, deadline)
                .await
            {
                Ok(res) => res,
                // A *peer* that no longer accepts requests is a node that went
                // away, not this client's node shutting down. While walking the
                // in-process order, skip it; while following a hint, there is no
                // "next" node, so report the loss of quorum. (A closed channel on
                // `self` is a genuine shutdown and propagates unchanged.)
                Err(ArachneError::ShuttingDown) if target_id != self.inner.self_id => {
                    if hint.is_none() {
                        unreachable += 1;
                        if unreachable + 1 >= order.len() as u32 {
                            return Err(ArachneError::QuorumUnavailable);
                        }
                        pos = (pos + 1) % order.len();
                        continue;
                    } else {
                        return Err(ArachneError::QuorumUnavailable);
                    }
                }
                Err(e) => return Err(e),
            };
            match result {
                Ok(outcome) => return Ok(outcome),
                // Re-redirect to the (possibly fresh) leader the node named.
                Err(ArachneError::NotLeader { leader_hint }) => {
                    if self.max_redirects == 0 {
                        // Single-shot handle: surface the hint unchanged.
                        return Err(ArachneError::NotLeader { leader_hint });
                    }
                    hint = leader_hint;
                    pos = (pos + 1) % order.len();
                    redirects += 1;
                    if redirects > self.max_redirects {
                        return Err(ArachneError::Timeout);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn get_with_redirect(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ArachneError> {
        // Reads use the longer read budget (the actor's ReadIndex wait plus its
        // one retry); using the write budget here made the client give up before
        // the actor could resolve the read.
        let deadline = Instant::now() + self.inner.read_timeout;
        let order = self.target_order();
        let mut pos = 0usize;
        let mut redirects = 0u32;
        let mut unreachable = 0u32;
        // The most recent known leader (see `request_with_redirect`).
        let mut hint: Option<(NodeId, SocketAddr)> = None;
        loop {
            // Target to reach: the latest known leader, else the next in-process
            // target. `wire_addr` is `Some` only when it came from a hint.
            let (target_id, wire_addr) = match &hint {
                Some((id, a)) => (id.clone(), Some(a.clone())),
                None => (order[pos].clone(), None),
            };
            let result = match self
                .reach_get(&target_id, wire_addr, key, deadline)
                .await
            {
                Ok(res) => res,
                // A peer that cannot accept the request is unreachable; an
                // unreachable peer means the cluster cannot confirm a read index.
                Err(ArachneError::ShuttingDown) if target_id != self.inner.self_id => {
                    if hint.is_none() {
                        unreachable += 1;
                        if unreachable + 1 >= order.len() as u32 {
                            return Err(ArachneError::QuorumUnavailable);
                        }
                        pos = (pos + 1) % order.len();
                        continue;
                    } else {
                        return Err(ArachneError::QuorumUnavailable);
                    }
                }
                Err(e) => return Err(e),
            };
            match result {
                Ok(value) => return Ok(value),
                // Re-redirect to the (possibly fresh) leader the node named.
                Err(ArachneError::NotLeader { leader_hint }) => {
                    if self.max_redirects == 0 {
                        // Single-shot handle: surface the hint unchanged.
                        return Err(ArachneError::NotLeader { leader_hint });
                    }
                    hint = leader_hint;
                    pos = (pos + 1) % order.len();
                    redirects += 1;
                    if redirects > self.max_redirects {
                        return Err(ArachneError::Timeout);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// The ordered node list to try: self first, then peers in sorted order.
    fn target_order(&self) -> Vec<NodeId> {
        let mut v = vec![self.inner.self_id.clone()];
        let peers = lock_read(&self.inner.peers);
        let mut peer_ids: Vec<NodeId> = peers.keys().cloned().collect();
        peer_ids.sort();
        for id in peer_ids {
            if id != self.inner.self_id {
                v.push(id);
            }
        }
        v
    }

    /// Resolve a `NodeId` to its handle (self, or a registered peer), cloned —
    /// a `Handle` is a cheap `Arc` clone, so returning an owned copy is free.
    fn handle_for(&self, id: &NodeId) -> Option<Handle> {
        if id == &self.inner.self_id {
            return Some(self.clone());
        }
        lock_read(&self.inner.peers).get(id).cloned()
    }

    // ---- transport to the runtime actor ------------------------------------

    /// Put one request on one target's actor channel.
    ///
    /// The outer error is a transport/deadline failure of *this* client; the
    /// inner one is the target's verdict (which may be `NotLeader{hint}`, the
    /// signal the redirect loop follows). The `Ok` half of the inner result
    /// carries the applied outcome for a `Propose` (M1 write-back); membership
    /// changes have no outcome and map to [`ApplyOutcome::None`].
    async fn send_request(
        &self,
        target: &NodeId,
        req: &Request,
        deadline: Instant,
    ) -> Result<Result<ApplyOutcome, ArachneError>, ArachneError> {
        let Some(handle) = self.handle_for(target) else {
            return Err(ArachneError::QuorumUnavailable);
        };
        match req {
            Request::Propose {
                cmd,
                client_id,
                seq_no,
            } => {
                let (ack_tx, ack_rx) = oneshot::channel::<Result<ApplyOutcome, ArachneError>>();
                handle
                    .inner
                    .tx
                    .send(Command::Propose {
                        cmd: cmd.clone(),
                        client_id: *client_id,
                        seq_no: *seq_no,
                        ack: ack_tx,
                    })
                    .await
                    .map_err(|_| ArachneError::ShuttingDown)?;
                self.await_oneshot(ack_rx, deadline).await
            }
            Request::ConfChange {
                change_type,
                node_id,
            } => {
                let (ack_tx, ack_rx) = oneshot::channel::<Result<(), ArachneError>>();
                handle
                    .inner
                    .tx
                    .send(Command::ConfChange {
                        change_type: *change_type,
                        node_id: *node_id,
                        ack: ack_tx,
                    })
                    .await
                    .map_err(|_| ArachneError::ShuttingDown)?;
                self.await_oneshot(ack_rx, deadline)
                    .await
                    .map(|r| r.map(|()| ApplyOutcome::None))
            }
            Request::TransferLeader { target: to } => {
                let (ack_tx, ack_rx) = oneshot::channel::<Result<(), ArachneError>>();
                handle
                    .inner
                    .tx
                    .send(Command::TransferLeader {
                        target: *to,
                        ack: ack_tx,
                    })
                    .await
                    .map_err(|_| ArachneError::ShuttingDown)?;
                self.await_oneshot(ack_rx, deadline)
                    .await
                    .map(|r| r.map(|()| ApplyOutcome::None))
            }
        }
    }

    async fn send_get(
        &self,
        target: &NodeId,
        key: &[u8],
        deadline: Instant,
    ) -> Result<Result<Option<Vec<u8>>, ArachneError>, ArachneError> {
        let Some(handle) = self.handle_for(target) else {
            return Err(ArachneError::QuorumUnavailable);
        };
        let (ack_tx, ack_rx) = oneshot::channel();
        handle
            .inner
            .tx
            .send(Command::Read {
                key: key.to_vec(),
                ack: ack_tx,
            })
            .await
            .map_err(|_| ArachneError::ShuttingDown)?;
        self.await_oneshot(ack_rx, deadline).await
    }

    /// Forward a read to the leader over the wire (propsol v0.2.19).
    ///
    /// Multi-process analogue of the in-process read redirect. Returns:
    /// - `Ok(Ok(value))`        — the forwarded read returned `value`.
    /// - `Ok(Err(NotLeader{hint}))` — the leader re-redirected us (fresh hint).
    /// - `Ok(Err(other))`       — the leader reported a stable failure code.
    /// - `Err(e)`               — the forward itself failed; treat as unreachable.
    async fn forward_get(
        &self,
        remote: &Arc<dyn RemoteForwarder>,
        leader_id: NodeId,
        leader_addr: SocketAddr,
        key: &[u8],
        deadline: Instant,
    ) -> Result<Result<Option<Vec<u8>>, ArachneError>, ArachneError> {
        let forward_cmd = ForwardCommand::Read {
            key: key.to_vec(),
        };
        match tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            remote.forward(leader_id, leader_addr, forward_cmd),
        ).await
        {
            Ok(Ok(outcome)) => {
                // A `not_leader` outcome carries a *fresh* hint (re-redirect);
                // otherwise the read succeeded and the value is returned.
                match outcome.leader_hint {
                    Some((id, a)) => Ok(Err(ArachneError::NotLeader {
                        leader_hint: Some((id, a)),
                    })),
                    None => Ok(Ok(outcome.value)),
                }
            }
            // A stable code from the leader's command sink.
            Ok(Err(code)) => Ok(Err(Handle::code_to_error(&code))),
            // Dial/connect/timeout: the leader is unreachable.
            Err(_) => Err(ArachneError::QuorumUnavailable),
        }
    }

    /// Reach `target_id` for a read, choosing the in-process or wire path, and
    /// return the *inner* result the redirect loop routes (`Ok(value)` on success,
    /// `Err(e)` for the node's verdict) or a reachability error (`Err(e)` on the
    /// outer side).
    async fn reach_get(
        &self,
        target_id: &NodeId,
        wire_addr: Option<SocketAddr>,
        key: &[u8],
        deadline: Instant,
    ) -> Result<Result<Option<Vec<u8>>, ArachneError>, ArachneError> {
        // In-process: a known in-process peer (self or a registered peer).
        if self.handle_for(target_id).is_some() {
            return self.send_get(target_id, key, deadline).await;
        }
        // No in-process peer: forward to the leader over the wire.
        if let (Some(remote), Some(addr)) = (&self.inner.remote, wire_addr) {
            return self.forward_get(remote, target_id.clone(), addr, key, deadline).await;
        }
        // No path to the target: the cluster cannot be reached.
        Err(ArachneError::QuorumUnavailable)
    }

    /// Await a oneshot reply bounded by `deadline`. A dropped sender (the
    /// runtime actor stopped) is a shutdown; a timeout is a `Timeout`.
    async fn await_oneshot<T>(
        &self,
        rx: oneshot::Receiver<T>,
        deadline: Instant,
    ) -> Result<T, ArachneError> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining == Duration::ZERO {
            return Err(ArachneError::Timeout);
        }
        match tokio::time::timeout(remaining, rx).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_)) => Err(ArachneError::ShuttingDown),
            Err(_) => Err(ArachneError::Timeout),
        }
    }

    // ---- argument validation (fail-stop discipline, propsol §3.2) -----------

    fn validate_key(&self, key: &[u8]) -> Result<(), ArachneError> {
        if (key.len() as u64) > self.inner.max_key_bytes {
            return Err(ArachneError::InvalidArgument(format!(
                "key of {} bytes exceeds max_key_bytes ({})",
                key.len(),
                self.inner.max_key_bytes
            )));
        }
        Ok(())
    }

    fn validate_put(&self, key: &[u8], value: &[u8]) -> Result<(), ArachneError> {
        self.validate_key(key)?;
        if (value.len() as u64) > self.inner.max_value_bytes {
            return Err(ArachneError::InvalidArgument(format!(
                "value of {} bytes exceeds max_value_bytes ({})",
                value.len(),
                self.inner.max_value_bytes
            )));
        }
        Ok(())
    }

    /// Validate a `multi_put` batch against every bound **before** propose
    /// (fail-stop discipline, propsol §3.2): each key within `max_key_bytes`,
    /// each value within `max_value_bytes`, entry count within
    /// [`MAX_MULTI_PUT_ENTRIES`], and the batch's total key+value bytes within
    /// [`MAX_MULTI_PUT_TOTAL_BYTES`]. An over-limit batch is rejected with
    /// `InvalidArgument` and never enters the log (§5.2 backpressure + size
    /// caps).
    fn validate_multi_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), ArachneError> {
        if entries.is_empty() {
            return Err(ArachneError::InvalidArgument(
                "multi_put requires at least one entry".into(),
            ));
        }
        if entries.len() > MAX_MULTI_PUT_ENTRIES {
            return Err(ArachneError::InvalidArgument(format!(
                "multi_put of {} entries exceeds MAX_MULTI_PUT_ENTRIES ({})",
                entries.len(),
                MAX_MULTI_PUT_ENTRIES
            )));
        }
        let mut total: u64 = 0;
        for (key, value) in entries {
            self.validate_key(key)?;
            if (value.len() as u64) > self.inner.max_value_bytes {
                return Err(ArachneError::InvalidArgument(format!(
                    "multi_put value of {} bytes exceeds max_value_bytes ({})",
                    value.len(),
                    self.inner.max_value_bytes
                )));
            }
            total = total
                .saturating_add(key.len() as u64)
                .saturating_add(value.len() as u64);
        }
        if total > MAX_MULTI_PUT_TOTAL_BYTES {
            return Err(ArachneError::InvalidArgument(format!(
                "multi_put total {} bytes exceeds MAX_MULTI_PUT_TOTAL_BYTES ({})",
                total, MAX_MULTI_PUT_TOTAL_BYTES
            )));
        }
        Ok(())
    }
}

/// A request the redirect loop can carry to whichever node is the leader.
///
/// Writes and membership changes share one loop because they share one
/// contract: the leader accepts, a follower answers `NotLeader{hint}`, and the
/// caller's deadline covers the whole chase (propsol §3.3).
#[derive(Clone)]
enum Request {
    Propose {
        cmd: Vec<u8>,
        client_id: u64,
        seq_no: u64,
    },
    ConfChange {
        change_type: ConfChangeType,
        node_id: RaftId,
    },
    /// `None` = any other voter (used by the automatic leader removal).
    TransferLeader {
        target: Option<RaftId>,
    },
}

struct HandleInner {
    /// This handle's node identity (so it knows which target is "self").
    self_id: NodeId,
    /// The per-handle session identifier (propsol §2.3). Unique per process
    /// (see [`next_client_id`]).
    client_id: u64,
    /// Monotonically increasing per-handle sequence number.
    seq: AtomicU64,
    /// Command channel to this node's runtime actor.
    tx: mpsc::Sender<Command>,
    /// In-process peer handles, for client-side redirect (propsol §3.3).
    peers: RwLock<HashMap<NodeId, Handle>>,
    /// Key/value size limits (validated before propose).
    max_key_bytes: u64,
    max_value_bytes: u64,
    /// The total deadline for a single **write** operation (across redirects).
    timeout: Duration,
    /// The total deadline for a single **read** operation. Must exceed the
    /// runtime actor's ReadIndex budget (see [`Handle::new_local`]).
    read_timeout: Duration,
    /// The follower's remote forwarder (propsol v0.2.19). `None` for in-memory
    /// / peerless nodes, where the in-process redirect is the only path. When
    /// set, a `NotLeader` whose hinted leader is not a known in-process peer is
    /// forwarded over the wire instead of collapsing to `QuorumUnavailable`.
    remote: Option<Arc<dyn RemoteForwarder>>,
}

/// The lower bound of a [`watch`](Handle::watch) subscription (M4/P2, design
/// D5): the consistent prefix snapshot at its applied watermark plus the
/// bounded stream of write-set events that followed. The consumer rebuilds the
/// prefix state as `snapshot ∪ events` (every event has `index >
/// applied_index`, so nothing is missed or duplicated).
pub struct WatchSubscription {
    /// The prefix snapshot as of `applied_index` (`(key, value)` pairs in byte
    /// order), bounded by the `limit` passed to `watch`.
    pub snapshot: Vec<(Vec<u8>, Vec<u8>)>,
    /// The applied watermark the snapshot was taken at. All events in `events`
    /// have `index > applied_index`.
    pub applied_index: u64,
    /// Always `false` from `Handle::watch` — a truncated snapshot is rejected
    /// at registration (design D3) rather than returned incomplete. Kept on the
    /// type so the field is explicit and forward-compatible.
    pub truncated: bool,
    /// Ordered, monotone-in-`index` write-set events strictly after
    /// `applied_index`. A consumer that falls behind is **disconnected**: the
    /// channel closes (no silent gap) and the caller must re-`watch`.
    pub events: mpsc::Receiver<WatchEvent>,
}

impl std::fmt::Debug for WatchSubscription {
    /// Hand-written: `mpsc::Receiver` has no `Debug`; print the snapshot and
    /// watermark and note the receiver is omitted.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatchSubscription")
            .field("snapshot", &self.snapshot)
            .field("applied_index", &self.applied_index)
            .field("truncated", &self.truncated)
            .field("events", &"<mpsc::Receiver<WatchEvent>>")
            .finish()
    }
}

/// The next per-handle `client_id`: the process id in the high bits (so
/// distinct processes have distinct ids) and a per-process counter in the low
/// bits (so distinct handles within a process have distinct ids).
fn next_client_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let base = (std::process::id() as u64) << 32;
    base | COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Recover a read guard even from a poisoned lock (the guarded data is a simple
/// map; recovering is safe). Mirrors `arachne-transport-tonic::unlock_read`.
fn lock_read<'a, T>(lock: &'a RwLock<T>) -> RwLockReadGuard<'a, T> {
    match lock.read() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

/// Recover a write guard even from a poisoned lock.
fn lock_write<'a, T>(lock: &'a RwLock<T>) -> RwLockWriteGuard<'a, T> {
    match lock.write() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

/// The byte-successor of a prefix: the smallest key greater than every key
/// that has `prefix` as its byte prefix (M2-P1A §4.1).
///
/// `[prefix, successor(prefix))` is exactly the keyspace slice sharing
/// `prefix`. Computed by taking `prefix`, bumping the last byte that is not
/// `0xFF` by one and truncating after it; a prefix whose tail is all `0xFF`
/// has no successor in the byte-ordering and returns `None` (the range end is
/// then unbounded).
fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    for i in (0..prefix.len()).rev() {
        if prefix[i] != 0xFF {
            let mut out = prefix[..=i].to_vec();
            out[i] += 1;
            return Some(out);
        }
    }
    None
}
