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
use crate::state_machine::KvStateMachine;
use crate::{NodeId, ProfileConfig, RaftId};
use raft::eraftpb::ConfChangeType;

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
        self.propose_with_redirect(cmd, client_id, seq_no).await
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
    }

    /// Hand leadership to any other voter (propsol §5.3 hard constraint 3).
    async fn hand_over_leadership(&self) -> Result<(), ArachneError> {
        self.request_with_redirect(Request::TransferLeader { target: None })
            .await
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
        self.propose_with_redirect(cmd, client_id, seq_no).await
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

    async fn propose_with_redirect(
        &self,
        cmd: Vec<u8>,
        client_id: u64,
        seq_no: u64,
    ) -> Result<(), ArachneError> {
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
    async fn request_with_redirect(&self, req: Request) -> Result<(), ArachneError> {
        let deadline = Instant::now() + self.inner.timeout;
        let order = self.target_order();
        let mut pos = 0usize;
        let mut redirects = 0u32;
        let mut unreachable_peers = 0u32;
        loop {
            let target = order[pos].clone();
            let result = match self.send_request(&target, &req, deadline).await {
                Ok(result) => result,
                // A *peer* that no longer accepts requests is a node that went
                // away, not this client's node shutting down: keep looking, and
                // report the loss of quorum once every peer has failed. (A
                // closed channel on `self` is a genuine shutdown and is
                // propagated unchanged.)
                Err(ArachneError::ShuttingDown) if target != self.inner.self_id => {
                    unreachable_peers += 1;
                    if unreachable_peers + 1 >= order.len() as u32 {
                        return Err(ArachneError::QuorumUnavailable);
                    }
                    pos = (pos + 1) % order.len();
                    continue;
                }
                Err(e) => return Err(e),
            };
            match result {
                Ok(()) => return Ok(()),
                Err(ArachneError::NotLeader { leader_hint }) => {
                    if self.max_redirects == 0 {
                        // Single-shot handle: surface the hint unchanged.
                        return Err(ArachneError::NotLeader { leader_hint });
                    }
                    pos = self.redirect_pos(&order, pos, leader_hint)?;
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
        let mut unreachable_peers = 0u32;
        loop {
            let target = order[pos].clone();
            let result = match self.send_get(&target, key, deadline).await {
                Ok(result) => result,
                // See `propose_with_redirect`: a peer that cannot accept the
                // request is unreachable, and unreachable peers mean the
                // cluster cannot confirm a read index.
                Err(ArachneError::ShuttingDown) if target != self.inner.self_id => {
                    unreachable_peers += 1;
                    if unreachable_peers + 1 >= order.len() as u32 {
                        return Err(ArachneError::QuorumUnavailable);
                    }
                    pos = (pos + 1) % order.len();
                    continue;
                }
                Err(e) => return Err(e),
            };
            match result {
                Ok(value) => return Ok(value),
                Err(ArachneError::NotLeader { leader_hint }) => {
                    if self.max_redirects == 0 {
                        // Single-shot handle: surface the hint unchanged.
                        return Err(ArachneError::NotLeader { leader_hint });
                    }
                    pos = self.redirect_pos(&order, pos, leader_hint)?;
                    redirects += 1;
                    if redirects > self.max_redirects {
                        return Err(ArachneError::Timeout);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Resolve the next target position for a `NotLeader` hint: the hinted
    /// leader if it is a known peer, otherwise the next peer in deterministic
    /// order (the seeds-order fallback). No forward progress (no leader, or the
    /// hint cycles back to ourselves) is a quorum failure.
    fn redirect_pos(
        &self,
        order: &[NodeId],
        pos: usize,
        hint: Option<(NodeId, SocketAddr)>,
    ) -> Result<usize, ArachneError> {
        let Some((leader_id, _)) = hint else {
            // No leader is known (or the leader stepped down): quorum is lost.
            return Err(ArachneError::QuorumUnavailable);
        };
        let next = order
            .iter()
            .position(|id| id == &leader_id)
            .unwrap_or((pos + 1) % order.len());
        if next == pos {
            // The hint points back at ourselves: no forward progress.
            return Err(ArachneError::QuorumUnavailable);
        }
        Ok(next)
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
    /// signal the redirect loop follows).
    async fn send_request(
        &self,
        target: &NodeId,
        req: &Request,
        deadline: Instant,
    ) -> Result<Result<(), ArachneError>, ArachneError> {
        let Some(handle) = self.handle_for(target) else {
            return Err(ArachneError::QuorumUnavailable);
        };
        let (ack_tx, ack_rx) = oneshot::channel();
        match req {
            Request::Propose {
                cmd,
                client_id,
                seq_no,
            } => {
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
            }
            Request::ConfChange {
                change_type,
                node_id,
            } => {
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
            }
            Request::TransferLeader { target: to } => {
                handle
                    .inner
                    .tx
                    .send(Command::TransferLeader {
                        target: *to,
                        ack: ack_tx,
                    })
                    .await
                    .map_err(|_| ArachneError::ShuttingDown)?;
            }
        }
        self.await_oneshot(ack_rx, deadline).await
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
