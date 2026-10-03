//! In-process embedding facade for Arachne.
//!
//! This module exposes a minimal, zero-tokio, single-instance API that lets an
//! embedding process create one in-process Arachne node and call linearizable
//! writes/reads without ever touching an async runtime:
//!
//! ```rust,ignore
//! use arachne::server::{Arachne, ArachneError, WalConfig};
//!
//! // One node per process. The binding is decorative; the node lives for the
//! // whole process and is shut down explicitly.
//! let _server = Arachne::new(1, &data_dir, WalConfig::default())?;
//!
//! Arachne::set(b"key", b"value").await?;
//! let v = Arachne::get(b"key").await?;        // ReadIndex linearizable read
//! let v = Arachne::get_stale(b"key").await?;  // local (weak) read
//! Arachne::delete(b"key").await?;
//! Arachne::shutdown();
//! ```
//!
//! * **Single instance.** The facade keeps exactly one node per process in a
//!   global [`std::sync::Mutex<Option<_>>`]. A second `Arachne::new` returns
//!   `ArachneError::AlreadyInitialized`; static methods called before `new`
//!   or after `shutdown` return `ArachneError::NotInitialized`.
//! * **Hidden runtime.** The node runs on its own dedicated OS thread with a
//!   current-thread tokio runtime (see [`Runtime::spawn_dedicated`]). Callers
//!   never poll a runtime, hold a tokio context, or own the actor.
//! * **Singleton raft.** With no peers, the bootstrap voter set is `{self}`,
//!   so the node self-elects on its first tick and linear reads are
//!   quorum-free (the raft `is_singleton` path). `get` is served as soon as a
//!   value is applied.
//!
//! For multi-node clusters or advanced membership control, use the original
//! `arachne::runtime::Runtime` / `arachne::client::Handle` APIs directly.

pub use crate::client::ArachneError;
pub use crate::client::Handle;
pub use crate::storage::WalConfig;

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use arachne_seam::{NodeId, Transport, TransportMessage, TransportRx};

use crate::consensus::RaftNodeConfig;
use crate::metrics::Metrics;
use crate::profile::Profile;
use crate::runtime::{Runtime, RuntimeConfig, RuntimeThread};
use crate::storage::{WalOptions, WalStorage};

use slog::{o, Drain, Logger};

/// Global, single-instance holder for the embedded node.
///
/// A `Mutex` (not `OnceLock`) so [`Arachne::shutdown`] can *take* the node out
/// of the slot before joining the actor thread (which drops the WAL data-dir
/// lock). After a shutdown the slot is empty again.
static INSTANCE: Mutex<Option<FacadeState>> = Mutex::new(None);

/// What lives behind the static: the local [`Handle`] (cheap-`Clone` for each
/// static method) plus the owned [`RuntimeThread`] that keeps the node alive
/// and carries the stop signal for [`Arachne::shutdown`].
struct FacadeState {
    handle: Handle,
    thread: RuntimeThread,
}

/// Outbound transport for the singleton node: there are no peers to deliver to,
/// so every `send` resolves immediately to `Ok(())` and nothing is ever
/// transmitted. This is the in-process analogue of the single-node example's
/// peerless transport. On a singleton every send is already accounted for by the
/// node's dropped-sends logic and the `Result` is irrelevant.
#[derive(Debug, Clone)]
struct PeerlessTx;

impl Transport for PeerlessTx {
    type Error = PeerlessSendError;

    fn send(
        &self,
        _to: NodeId,
        _msg: TransportMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PeerlessSendError;

impl std::fmt::Display for PeerlessSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("no peers to deliver to in a singleton node")
    }
}

impl std::error::Error for PeerlessSendError {}

/// Inbound transport for the singleton node: no remote peers ever deliver a
/// message, so `recv` simply suspends forever (a `pending` future). This is the
/// safe out-of-network case: nothing is lost because nothing is ever expected.
#[derive(Debug)]
struct PeerlessRx;

impl TransportRx for PeerlessRx {
    fn recv(&mut self) -> impl Future<Output = Option<(NodeId, TransportMessage)>> + Send {
        std::future::pending()
    }
}

/// Zero-field marker for the embedded node.
///
/// The node itself lives in a global static; the marker exists only so that
/// `Arachne::new` has an owned return type and so the process can hold an
/// explicit "handle" that documents the binding. Dropping it is a no-op (the
/// node is *not* shut down) and is deliberately distinct from the explicit
/// [`Arachne::shutdown`].
#[derive(Debug, Clone, Copy)]
pub struct Arachne;

impl Arachne {
    /// Bootstrap the single in-process node.
    ///
    /// `data_dir` is created on demand; the WAL's data-dir lock is acquired and
    /// held for the node's lifetime. A second call returns
    /// [`ArachneError::AlreadyInitialized`].
    pub fn new(node_id: u64, data_dir: &Path, wal: WalConfig) -> Result<Arachne, ArachneError> {
        // The lock is held across the fully-synchronous assembly (WalStorage::open,
        // Runtime::new, spawn_dedicated). None of these await, so no deadlock;
        // the WAL data-dir lock is a separate lock and is acquired here too;
        // nothing below awaits, so it is never held across an await either.
        let mut guard = INSTANCE
            .lock()
            .map_err(|_| ArachneError::Unrecoverable("facade lock poisoned".into()))?;
        if guard.is_some() {
            return Err(ArachneError::AlreadyInitialized);
        }
        let state = assemble(node_id, data_dir, wal)?;
        *guard = Some(state);
        Ok(Arachne)
    }

    /// Propose a linearizable write. `set` is a `Propose` through the leader's
    /// `Command::Propose` path; on a singleton there is no quorum round to
    /// coordinate, so it is fast.
    pub async fn set(key: &[u8], value: &[u8]) -> Result<(), ArachneError> {
        let handle = Self::borrow().await?;
        handle.put(key, value).await
    }

    /// Linearizable point read. The leader is required, so this is served only
    /// after the node has elected itself (the pre-election window returns
    /// `NotLeader`/`QuorumUnavailable`, which callers poll until it becomes
    /// `Ok`).
    pub async fn get(key: &[u8]) -> Result<Option<Vec<u8>>, ArachneError> {
        let handle = Self::borrow().await?;
        handle.get(key).await
    }

    /// Local (weak) read. Reads the node's own commit state directly; available
    /// on a singleton even before the read-index round completes.
    pub async fn get_stale(key: &[u8]) -> Result<Option<Vec<u8>>, ArachneError> {
        let handle = Self::borrow().await?;
        handle.get_stale(key).await
    }

    /// Delete a key (propose an empty value).
    pub async fn delete(key: &[u8]) -> Result<(), ArachneError> {
        let handle = Self::borrow().await?;
        handle.delete(key).await
    }

    /// Escape hatch: return a clone of the local [`Handle`] so embedding code can
    /// do anything the full client API permits (reads, proposes, membership
    /// changes...). The clone is cheap (shared actor channel) and the node must
    /// still be initialized.
    pub async fn handle() -> Result<Handle, ArachneError> {
        Self::borrow().await
    }

    /// Shut down the node: takes the state out of the static (releasing the
    /// facade lock *before* joining, since the join blocks until the actor drops
    /// the WAL data-dir lock), then stops and joins the actor thread. Returns
    /// `NotInitialized` if there is nothing to shut down; a second call returns
    /// `NotInitialized` again.
    pub fn shutdown() -> Result<(), ArachneError> {
        let state = {
            let mut guard = INSTANCE
                .lock()
                .map_err(|_| ArachneError::Unrecoverable("facade lock poisoned".into()))?;
            guard.take().ok_or(ArachneError::NotInitialized)?
        };
        // `guard` (and thus the mutex lock) is dropped at end of block, so the
        // lock is released *before* the (slow) join below.
        state.thread.shutdown();
        Ok(())
    }

    /// Cheaply clone the node's [`Handle`] for a single static method.
    ///
    /// The handle's `tx` is an `Arc<HandleInner>`-backed clone, so cloning is
    /// a refcount bump; it is cloned inside the lock and the *awaiting* of the
    /// resulting command happens entirely outside the lock.
    async fn borrow() -> Result<Handle, ArachneError> {
        let handle = INSTANCE
            .lock()
            .map_err(|_| ArachneError::Unrecoverable("facade lock poisoned".into()))?
            .as_ref()
            .and_then(|s| Some(s.handle.clone()))
            .ok_or(ArachneError::NotInitialized)?;
        Ok(handle)
    }
}

/// Assemble one singleton node: configure it, open the WAL, build the node and
/// runtime, and spawn the actor thread. The caller holds the facade lock across
/// this fully-synchronous sequence.
fn assemble(node_id: u64, data_dir: &Path, wal: WalConfig) -> Result<FacadeState, ArachneError> {
    let profile = Profile::Lan.config();
    let raft = RaftNodeConfig::from_profile(&profile);

    // Production `server.rs` reading `SystemTime::now()` is legal (check-entropy
    // Gate C only constrains the sim/tests path).
    let created_at_millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| ArachneError::Unrecoverable("system clock before epoch".into()))?
        .as_millis() as u64;

    let opts = WalOptions {
        cluster_id: format!("arachne-{node_id}"),
        node_id: node_id.to_string(),
        config: wal,
        created_at_millis,
        fsync_observer: None,
    };
    let storage = WalStorage::open(data_dir, opts)
        .map_err(|e| ArachneError::Unrecoverable(format!("facade init: cannot open WAL: {e}")))?;

    let config = RuntimeConfig {
        self_raft_id: node_id,
        self_node_id: NodeId::new(node_id.to_string()),
        peers: HashMap::new(),
        addresses: HashMap::new(),
        raft,
        profile,
        metrics: Arc::new(Metrics::new()),
    };

    let logger = Logger::root(slog::Discard.fuse(), o!());

    let (runtime, handle) = Runtime::new(config, storage, PeerlessTx, PeerlessRx, &logger)
        .map_err(|e| ArachneError::Unrecoverable(format!("facade init: {e}")))
        ?;
    let thread = runtime
        .spawn_dedicated()
        .map_err(|e| ArachneError::Unrecoverable(format!("facade init: cannot spawn actor thread: {e}")))?;

    Ok(FacadeState { handle, thread })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `PeerlessTx::send` must resolve to `Ok(())` immediately.
    #[tokio::test]
    async fn peerless_tx_resolves_ok() {
        let tx: PeerlessTx = PeerlessTx;
        let val = tx
            .send(NodeId::new(1.to_string()), TransportMessage::Raft(Vec::new()))
            .await;
        assert_eq!(val, Ok(()));
    }
}
