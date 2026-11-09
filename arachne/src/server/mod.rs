//! In-process embedding facade for Arachne.
//!
//! This module exposes a single-instance API that lets an embedding process
//! create one in-process Arachne node per process and call linearizable
//! writes/reads without ever polling a runtime. One entry point, one
//! code path:
//!
//! * [`Arachne::start`] — the unified entry, driven by a [`ClusterConfig`].
//!   A real cluster passes [`ClusterConfig::member`] (a
//!   `TonicTransportFactory`-driven node whose gRPC server runs on a private
//!   multithread tokio runtime that lives for as long as the facade state is
//!   alive). A one-node cluster passes [`ClusterConfig::single_node`] (the
//!   zero-tokio, **peerless** path: one node, no peers, no listeners).
//!
//! * **Single instance.** The facade keeps exactly one node per process in a
//!   global [`std::sync::Mutex<Option<_>>`]. A second `Arachne::start`
//!   returns `ArachneError::AlreadyInitialized`; static
//!   methods called before init or after `shutdown` return
//!   `ArachneError::NotInitialized`.
//! * **Hidden runtime.** The node runs on its own dedicated OS thread with a
//!   current-thread tokio runtime (see [`Runtime::spawn_dedicated`]). Callers
//!   never poll a runtime, hold a tokio context, or own the actor. On the
//!   multi-node path the tonic listener additionally runs on a private
//!   multithread tokio runtime held by the facade state.
//! * **Peerless.** With no peers, the bootstrap voter set is `{self}`, so the
//!   node self-elects on its first tick and linear reads are quorum-free
//!   (raft's `is_singleton` path). `get` is served as soon as a value is
//!   applied.
//!
//! For advanced membership control or out-of-process control, use the original
//! `arachne_kv::runtime::Runtime` / `arachne_kv::client::Handle` APIs directly.

pub use crate::client::ArachneError;
pub use crate::client::Handle;
pub use crate::storage::WalConfig;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
#[cfg(feature = "transport-tonic")]
use std::sync::mpsc;
use std::time::SystemTime;

use arachne_kv_seam::{NodeId, RaftId, Transport, TransportMessage, TransportRx};
use arachne_kv_seam::ForwardTransport;
#[cfg(feature = "transport-tonic")]
use arachne_kv_seam::TransportFactory;

use crate::consensus::RaftNodeConfig;
use crate::metrics::Metrics;
use crate::profile::{Profile, ProfileConfig};
use crate::runtime::{Runtime, RuntimeConfig, RuntimeThread};
use crate::storage::{WalOptions, WalStorage};

use slog::{o, Drain, Logger};

// The snapshot provider is hoisted to the core (multi-node facade + node binary
// share one implementation). Only built when the tonic transport is on.
#[cfg(feature = "transport-tonic")]
pub mod snapshot_source;

#[cfg(feature = "transport-tonic")]
use arachne_kv_transport_tonic::{PROTOCOL_MAJOR, PROTOCOL_MINOR, TonicTransportFactory};

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
    /// The private multithread tokio runtime driving the tonic gRPC server
    /// (multi-node only). Held here so that runtime — and the worker threads
    /// on which the listener runs — outlives any caller thread. `None` on the
    /// peerless path.
    #[cfg(feature = "transport-tonic")]
    tonic: Option<TonicState>,
}

/// The tonic-side runtime and factory for a multi-node node, held by the facade.
#[cfg(feature = "transport-tonic")]
struct TonicState {
    rt: Arc<tokio::runtime::Runtime>,
    factory: TonicTransportFactory,
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

/// The peerless (singleton) node has no remote peers, so remote forwarding is
/// impossible. The default `forwarder()` implementation (returning `None`)
/// is what the client falls back to: it keeps its in-process redirect /
/// quorum-unavailable path, which is exactly right for a single node.
impl ForwardTransport for PeerlessTx {}

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
/// `Arachne::start` has an owned return type and so the process can hold an
/// explicit "handle" that documents the binding. Dropping it is a no-op (the
/// node is *not* shut down) and is deliberately distinct from the explicit
/// [`Arachne::shutdown`].
#[derive(Debug, Clone, Copy)]
pub struct Arachne;

impl Arachne {
    /// Bootstrap one in-process node from a [`ClusterConfig`].
    ///
    /// Arachne is a **distributed** engine: every entry point assembles a
    /// clustered node. A one-node cluster is expressed as
    /// [`ClusterConfig::single_node`] (`initial_cluster = [self]`, self-elects,
    /// linear reads are quorum-free); a real cluster uses
    /// [`ClusterConfig::member`].
    ///
    /// * Peerless (`ClusterConfig::single_node`): synchronous, on-caller-thread
    ///   assembly (no transport, no listeners).
    /// * Multi-node (`ClusterConfig::member`): synchronous *init* that returns
    ///   once the node is assembled. The gRPC listener runs on a private
    ///   multithread tokio runtime (spun up in a dedicated std thread) that the
    ///   facade state owns; callers never hold that runtime's context.
    ///
    /// A second call (while the first is alive) returns
    /// [`ArachneError::AlreadyInitialized`].
    pub fn start(config: ClusterConfig) -> Result<Arachne, ArachneError> {
        let mut guard = INSTANCE
            .lock()
            .map_err(|_| ArachneError::Unrecoverable("facade lock poisoned".into()))?;
        if guard.is_some() {
            return Err(ArachneError::AlreadyInitialized);
        }
        // The lock is held across the (fully) synchronous assembly. Multi-node
        // `start` blocks on the assembly thread until the listener is up and the
        // actor is spawned, then releases the lock before returning.
        let state = if config.is_peerless() {
            assemble_peerless(&config)?
        } else {
            start_multi_node(config)?
        };
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

    /// Local (weak) read plus the value's **origin index** (the log index of
    /// the entry that wrote it, always `>= 1` when present). Same cost and
    /// availability as [`Arachne::get_stale`]; `Ok(None)` when the key is
    /// absent.
    pub async fn get_stale_with_index(
        key: &[u8],
    ) -> Result<Option<(Vec<u8>, u64)>, ArachneError> {
        let handle = Self::borrow().await?;
        handle.get_stale_with_index(key).await
    }

    /// Batched exact-key read (`multi_get`): values in input order plus one
    /// applied index for the whole batch. Mirrors
    /// [`Handle::multi_get`](crate::client::Handle::multi_get) for the
    /// embedding facade.
    pub async fn multi_get(
        keys: &[&[u8]],
        consistency: crate::ReadConsistency,
    ) -> Result<crate::MultiGetResult, ArachneError> {
        let handle = Self::borrow().await?;
        handle.multi_get(keys, consistency).await
    }

    /// Wait until this node's state machine has applied at least `index`
    /// (Phase 3). Mirrors [`Handle::wait_applied`]: local only, no redirect and
    /// no quorum, so it works on a minority follower. `index == 0` is a no-op
    /// returning the current applied index.
    pub async fn wait_applied(
        index: crate::Index,
        timeout: std::time::Duration,
    ) -> Result<crate::Index, ArachneError> {
        let handle = Self::borrow().await?;
        handle.wait_applied(index, timeout).await
    }

    /// Linearizable write that returns the effective log index of the write
    /// (Phase 2). Mirrors [`Handle::put_with_index`].
    pub async fn put_with_index(key: &[u8], value: &[u8]) -> Result<u64, ArachneError> {
        let handle = Self::borrow().await?;
        handle.put_with_index(key, value).await
    }

    /// Delete a key (propose an empty value).
    pub async fn delete(key: &[u8]) -> Result<(), ArachneError> {
        let handle = Self::borrow().await?;
        handle.delete(key).await
    }

    /// Delete that returns the effective log index of the delete entry
    /// (Phase 2). Mirrors [`Handle::delete_with_index`].
    pub async fn delete_with_index(key: &[u8]) -> Result<u64, ArachneError> {
        let handle = Self::borrow().await?;
        handle.delete_with_index(key).await
    }

    /// Atomic multi-key write (M2-P1B): one command sets all `entries`
    /// atomically (all or nothing). Mirrors [`Handle::multi_put`] for the
    /// embedding facade — the "whole-tree replace" primitive for hydra.
    pub async fn multi_put(entries: &[(&[u8], &[u8])]) -> Result<(), ArachneError> {
        let handle = Self::borrow().await?;
        handle.multi_put(entries).await
    }

    /// Atomic multi-key write that returns the shared effective log index of
    /// the batch (Phase 2). Mirrors [`Handle::multi_put_with_index`].
    pub async fn multi_put_with_index(
        entries: &[(&[u8], &[u8])],
    ) -> Result<u64, ArachneError> {
        let handle = Self::borrow().await?;
        handle.multi_put_with_index(entries).await
    }

    /// Compare-and-swap (M3 §6): `compare(key, pred) → op(success|failure)`,
    /// returning the public verdict. Mirrors [`Handle::cas`] for the embedding
    /// facade.
    pub async fn cas(
        key: &[u8],
        pred: crate::CasPred,
        success: crate::CasOp,
    ) -> Result<crate::CasResult, ArachneError> {
        let handle = Self::borrow().await?;
        handle.cas(key, pred, success).await
    }

    /// Open a **watch** over a byte prefix (M4/P2): a consistent prefix
    /// snapshot at the current applied watermark + a bounded stream of
    /// write-set events after it. Mirrors [`Handle::watch`] for the embedding
    /// facade (design D5).
    pub async fn watch(
        prefix: &[u8],
        limit: usize,
    ) -> Result<crate::client::WatchSubscription, ArachneError> {
        let handle = Self::borrow().await?;
        handle.watch(prefix, limit).await
    }

    /// Consistent **prefix** stale read (M2-P1A): every `(key, value)` whose
    /// key has byte-prefix `prefix`, plus the applied index the whole segment
    /// was observed at. Mirrors [`Handle::get_stale_prefix`].
    #[allow(clippy::type_complexity)]
    pub async fn get_stale_prefix(
        prefix: &[u8],
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, u64, bool), ArachneError> {
        let handle = Self::borrow().await?;
        handle.get_stale_prefix(prefix, limit).await
    }

    /// Consistent **range** stale read (M2-P1A): everything in `[start, end)`
    /// plus the applied index the segment was observed at. Mirrors
    /// [`Handle::get_stale_range`].
    #[allow(clippy::type_complexity)]
    pub async fn get_stale_range(
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, u64, bool), ArachneError> {
        let handle = Self::borrow().await?;
        handle.get_stale_range(start, end, limit).await
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
    /// the WAL data-dir lock), stops the gRPC factory (multi-node) to free the
    /// listener, then stops and joins the actor thread. Returns `NotInitialized`
    /// if there is nothing to shut down; a second call returns `NotInitialized`
    /// again.
    pub fn shutdown() -> Result<(), ArachneError> {
        let state = {
            let mut guard = INSTANCE
                .lock()
                .map_err(|_| ArachneError::Unrecoverable("facade lock poisoned".into()))?;
            guard.take().ok_or(ArachneError::NotInitialized)?
        };
        // `guard` (and thus the mutex lock) is dropped at end of block, so the
        // lock is released *before* the (slow) join below.
        //
        // Multi-node: shut the gRPC factory down first so its server task and
        // listener are torn down and the inbound `rx` closes, *then* stop the
        // actor. The order matters: `factory.shutdown` closes the inbound stream
        // that the actor blocks on; releasing the actor first would race a
        // still-running server.
        #[cfg(feature = "transport-tonic")]
        {
            if let Some(tonic) = &state.tonic {
                tonic
                    .rt
                    .block_on(tonic.factory.shutdown());
            }
        }
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

/// Configuration for one node of a cluster (propsol §3.1 / §5.7).
///
/// This is the unified input to [`Arachne::start`]:
///
/// * **Peerless** (single node): use [`ClusterConfig::single_node`];
///   `initial_cluster` / `addresses` are empty and the node self-elects.
/// * **Multi-node**: use [`ClusterConfig::member`]; `initial_cluster` is the
///   full member list (this node must be a member), `addresses` maps *every*
///   member to its listen address (the self entry is this node's `listen`),
///   and each member's raft id is its 1-based position in `initial_cluster`.
///
/// No `Default` (a cluster needs an explicit identity); `Profile` / `WalConfig`
/// have sane defaults.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// The cluster identifier (handshake identity).
    pub cluster_id: String,
    /// This node's identifier.
    pub node_id: NodeId,
    /// This node's listen address. For the peerless path this is ignored (a
    /// `127.0.0.1:0` placeholder); for a multi-node path it is bound (a `:0`
    /// port allocates an ephemeral one). In the facade the address map must
    /// name every *peer*, and the self entry is this `listen`.
    pub listen: SocketAddr,
    /// The on-disk WAL data directory.
    pub data_dir: PathBuf,
    /// Bootstrap member list; this node must be a member. Its 1-based
    /// position is the node's deterministic raft id.
    pub initial_cluster: Vec<NodeId>,
    /// Every member mapped to its listen address; the self entry is `listen`,
    /// every other member carries the address under which it listens.
    pub addresses: HashMap<NodeId, SocketAddr>,
    /// Propsol §7 tuning preset.
    ///
    /// This remains the default/provenance: when [`Self::profile_config`] is
    /// `None`, the assembly converts this preset with [`Profile::config`].
    pub profile: Profile,
    /// Optional full [`ProfileConfig`] **override**.
    ///
    /// * `None` (the default) = use [`Self::profile`]'s preset.
    /// * `Some(pc)` = **wholesale replacement** of the preset — no per-field
    ///   merge with `profile`, which only remains as provenance. The override
    ///   takes precedence over `profile`.
    ///
    /// The effective profile is validated by [`Self::effective_profile`]
    /// before the WAL is opened, so an invalid override fails with
    /// [`ArachneError::InvalidConfig`] and opens nothing.
    ///
    /// Note: [`Self::wal`] is currently **ignored** by the facade assembly
    /// (the WAL options are derived from the effective profile). New storage
    /// knobs therefore flow through `profile_config` / `profile`, not `wal`.
    pub profile_config: Option<ProfileConfig>,
    /// WAL storage configuration.
    pub wal: WalConfig,
}

impl ClusterConfig {
    /// Build a peerless (single-node) config. `cluster_id` is auto-derived as
    /// `arachne-{node_id}`; `initial_cluster` / `addresses` are empty (the
    /// peerless marker), and `listen` is a harmless placeholder that is never
    /// bound.
    pub fn single_node(
        node_id: u64,
        data_dir: impl AsRef<Path>,
        wal: WalConfig,
    ) -> Self {
        let node_id = NodeId::new(node_id.to_string());
        Self {
            cluster_id: format!("arachne-{node_id}"),
            node_id,
            // Placeholder; the peerless path never binds it.
            listen: "127.0.0.1:0".parse().expect("placeholder address"),
            data_dir: data_dir.as_ref().to_path_buf(),
            initial_cluster: Vec::new(),
            addresses: HashMap::new(),
            profile: Profile::Lan,
            profile_config: None,
            wal,
        }
    }

    /// Build a multi-node member config. `listen` is bound (a `:0` port allocates
    /// an ephemeral one); `addresses` must name every member's listen address
    /// (the self entry is `listen`, every peer a concrete reachable address).
    pub fn member(
        cluster_id: String,
        node_id: NodeId,
        listen: SocketAddr,
        data_dir: PathBuf,
        initial_cluster: Vec<NodeId>,
        addresses: HashMap<NodeId, SocketAddr>,
    ) -> Self {
        Self {
            cluster_id,
            node_id,
            listen,
            data_dir,
            initial_cluster,
            addresses,
            profile: Profile::Lan,
            profile_config: None,
            wal: WalConfig::default(),
        }
    }

    /// True when the node has no peers to serve over (the peerless path).
    fn is_peerless(&self) -> bool {
        self.initial_cluster.len() <= 1
    }

    /// The node's deterministic raft id: for a peerless node it is `1` (a
    /// singleton has no peers to disambiguate against); for a multi-node node
    /// it is the 1-based position in `initial_cluster` (the deterministic
    /// bootstrap mapping the node binary and tests rely on).
    fn raft_id(&self) -> RaftId {
        if self.is_peerless() {
            1
        } else {
            self.initial_cluster
                .iter()
                .position(|id| *id == self.node_id)
                .map(|i| (i + 1) as RaftId)
                .unwrap_or(1)
        }
    }

    /// The unvalidated effective profile: the explicit override when present,
    /// otherwise the [`Self::profile`] preset.
    fn profile_config_or_preset(&self) -> ProfileConfig {
        self.profile_config
            .clone()
            .unwrap_or_else(|| self.profile.config())
    }

    /// The effective profile, validated at the assembly boundary.
    ///
    /// Returns the explicit [`Self::profile_config`] override, or the
    /// [`Self::profile`] preset when no override is set, after checking it
    /// with [`ProfileConfig::validate`]. An invalid override is reported as
    /// [`ArachneError::InvalidConfig`] **before** anything is opened.
    pub fn effective_profile(&self) -> Result<ProfileConfig, ArachneError> {
        let profile = self.profile_config_or_preset();
        profile
            .validate()
            .map_err(|e| ArachneError::InvalidConfig(format!("invalid profile config: {e}")))?;
        Ok(profile)
    }

    /// Set a **wholesale** profile override, replacing [`Self::profile`]'s
    /// preset for assembly.
    ///
    /// This does not merge with the preset: every field comes from
    /// `profile_config`. [`Self::profile`] remains as the default/provenance
    /// value used when no override is set. Validation happens later in
    /// [`Self::effective_profile`].
    pub fn with_profile_config(mut self, profile_config: ProfileConfig) -> Self {
        self.profile_config = Some(profile_config);
        self
    }

    /// Convenience override for [`ProfileConfig::snapshot_threshold_bytes`].
    ///
    /// Derives from the **current effective** profile (the existing override,
    /// or the [`Self::profile`] preset when none is set), changes only this
    /// field, and stores the result as the override — so it composes with the
    /// other convenience builders in either order.
    ///
    /// `0` disables the local snapshot trigger (the existing semantics).
    pub fn with_snapshot_threshold_bytes(mut self, bytes: u64) -> Self {
        let mut profile = self.profile_config_or_preset();
        profile.snapshot_threshold_bytes = bytes;
        self.profile_config = Some(profile);
        self
    }

    /// Convenience override for [`ProfileConfig::wal_trailing_keep_bytes`].
    ///
    /// Derives from the **current effective** profile and changes only this
    /// field, so chained builders retain each other's changes. `0` is the
    /// escape hatch that compacts everything covered by the snapshot (the
    /// existing semantics); a non-zero value retains that many WAL bytes
    /// behind the compaction watermark.
    pub fn with_wal_trailing_keep_bytes(mut self, bytes: u64) -> Self {
        let mut profile = self.profile_config_or_preset();
        profile.wal_trailing_keep_bytes = bytes;
        self.profile_config = Some(profile);
        self
    }
}

impl ClusterConfig {
    /// Current wall-clock time in milliseconds. Only read at the facade edge
    /// (production, not test/sim); a pre-epoch clock is impossible on any
    /// supported target so `unwrap_or` is a safe degenerate default.
    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// Open the WAL for one facade assembly path with the effective profile's
/// storage knobs.
///
/// Builds [`WalOptions`] from the profile (`fsync_policy`,
/// `wal_segment_bytes`), opens the storage, then applies
/// `wal_trailing_keep_bytes`.
///
/// **Ordering invariant:** [`WalStorage::open`] is recovery-only and never
/// compacts; compaction is runtime-driven after a snapshot. Setting the
/// trailing keep here, after open but before [`Runtime::new`], is therefore
/// equivalent to setting it at open time (no compaction can run in between).
/// This is why there is no `WalOptions.trailing_keep_bytes` /
/// `open_with_trailing_keep`.
///
/// `context` prefixes the error so each assembly path keeps its existing
/// diagnostics (`"facade init: ..."` / `"assemble_cluster: ..."`).
fn open_storage(
    context: &str,
    cluster_id: &str,
    node_id: &str,
    data_dir: &Path,
    profile: &ProfileConfig,
    created_at_millis: u64,
) -> Result<WalStorage, ArachneError> {
    let opts = WalOptions {
        cluster_id: cluster_id.to_string(),
        node_id: node_id.to_string(),
        config: WalConfig {
            fsync_policy: profile.fsync_policy,
            segment_bytes: profile.wal_segment_bytes,
        },
        created_at_millis,
        fsync_observer: None,
    };
    let mut storage = WalStorage::open(data_dir, opts)
        .map_err(|e| ArachneError::Unrecoverable(format!("{context}: cannot open WAL: {e}")))?;
    // See the ordering invariant above: no compaction can run between open and
    // runtime assembly, so applying the trailing keep here is safe.
    storage.set_trailing_keep_bytes(profile.wal_trailing_keep_bytes);
    Ok(storage)
}

/// Assemble one **peerless** (singleton) node: configure it, open the WAL,
/// build the node and runtime, and spawn the actor thread. The caller holds the
/// facade lock across this fully-synchronous sequence (no awaits, so no
/// deadlock).
fn assemble_peerless(config: &ClusterConfig) -> Result<FacadeState, ArachneError> {
    let profile = config.effective_profile()?;
    let raft = RaftNodeConfig::from_profile(&profile);

    let created_at_millis = ClusterConfig::now_millis();

    let storage = open_storage(
        "facade init",
        &config.cluster_id,
        &config.node_id.to_string(),
        &config.data_dir,
        &profile,
        created_at_millis,
    )?;

    let config = RuntimeConfig {
        self_raft_id: config.raft_id(),
        self_node_id: config.node_id.clone(),
        // Peerless: no peers; the bootstrap voter set is {self}.
        peers: HashMap::new(),
        addresses: config.addresses.clone(),
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

    Ok(FacadeState {
        handle,
        thread,
        #[cfg(feature = "transport-tonic")]
        tonic: None,
    })
}

#[cfg(feature = "transport-tonic")]
fn start_multi_node(config: ClusterConfig) -> Result<FacadeState, ArachneError> {
    // The async assembly (binding the gRPC listener, creating the transport
    // halves, building the runtime) must run on a *multithread* tokio runtime
    // that outlives the `start` call. We spin that runtime up in a dedicated
    // OS thread (via `std::thread::spawn` + `block_on`) so the caller thread
    // — which holds the facade lock and has no tokio context of its own —
    // can block without holding a tokio worker.
    //
    // `config` is owned here so the values the spawned thread needs escape to
    // a 'static'-sized closure (the thread outlives `start`). The &ClusterConfig
    // reference captured earlier would otherwise escape the function.
    let profile = config.effective_profile()?;
    let node_id = config.node_id;
    let listen = config.listen;
    let addresses = config.addresses;
    let initial_cluster = config.initial_cluster;
    let cluster_id = config.cluster_id;
    let data_dir = config.data_dir;
    let node_id_str = node_id.to_string();

    let factory = TonicTransportFactory::new(
        cluster_id.clone(),
        PROTOCOL_MAJOR,
        PROTOCOL_MINOR,
        Vec::new(),
        addresses.clone(),
    );
    factory.snapshot_provider(snapshot_source::DataDirSnapshots::new(data_dir.clone()));
    factory.snapshot_rate_bps(profile.snapshot_transfer_rate_bps);

    let mut wal = open_storage(
        "facade init",
        &cluster_id,
        &node_id_str,
        &data_dir,
        &profile,
        ClusterConfig::now_millis(),
    )?;
    wal.enable_offloaded_durability()
        .map_err(|e| ArachneError::Unrecoverable(format!("facade init: cannot enable offloaded durability: {e}")))?;

    let ch = mpsc::channel::<Result<(RuntimeThread, Handle, Arc<tokio::runtime::Runtime>, TonicTransportFactory), String>>();
    let tx = ch.0;

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build private multithread tokio runtime");
        let arc: Arc<tokio::runtime::Runtime> = Arc::new(rt);

        // The async closure must NOT capture `arc` (it is already borrowed by
        // `arc.block_on` as &self); instead it returns `(thread, handle,
        // factory)` and we wrap `arc` into the sent tuple below.
        //
        // A panic anywhere in **assembly** used to drop the channel, and the
        // caller turned that into a main-thread `RecvError` panic. Catch it
        // here and send a typed error instead: one bad node must not take down
        // the embedding process. This is defense in depth; the storage-level
        // hard-state repair prevents the known `hs.commit` panic upstream.
        //
        // Scope: this contains panics while the node is being assembled. A
        // panic later inside the dedicated actor thread is **not** contained by
        // this catch (pre-existing behavior; the actor runs on its own OS
        // thread after assembly returns).
        let assembly = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            arc.block_on(async move {
            // Bind *only* this node's listener and start serving it. The self
            // node id is guaranteed to be in the factory's address map (the
            // map holds every `initial_cluster` member, and this node is one
            // of them). `node_id.clone()` is passed (not moved) so the later
            // `factory.create` / `RuntimeConfig` can still use it.
            factory
                .start_with_bind(node_id.clone(), listen)
                .await
                .map_err(|e| format!("start_with_bind: {e}"))?;
            let (tx, rx) = factory.create(node_id.clone());

            // Bootstrap peers: each *other* member's raft id is its 1-based
            // position in `initial_cluster`.
            let mut peers: HashMap<RaftId, NodeId> = HashMap::new();
            for (i, id) in initial_cluster.iter().enumerate() {
                if *id != node_id {
                    peers.insert((i + 1) as RaftId, id.clone());
                }
            }

            let raft = RaftNodeConfig::from_profile(&profile);
            let runtime_config = RuntimeConfig {
                self_raft_id: (initial_cluster
                    .iter()
                    .position(|n| n == &node_id)
                    .unwrap_or(0) + 1)
                    as RaftId,
                self_node_id: node_id.clone(),
                peers,
                addresses,
                raft,
                profile: profile.clone(),
                metrics: Arc::new(Metrics::new()),
            };

            let logger = Logger::root(slog::Discard.fuse(), o!());

            let (runtime, handle) = match Runtime::new(runtime_config, wal, tx, rx, &logger) {
                Ok(built) => built,
                Err(e) => {
                    // If runtime assembly fails, `start_with_bind` has already
                    // bound this node's listener: shut the factory down before
                    // returning so the gRPC server is torn down now rather
                    // than left running until process exit.
                    factory.shutdown().await;
                    return Err(format!("facade init: {e}"));
                }
            };

            // Register this node's command sink so its gRPC `Forward` RPC can
            // run forwarded client commands against the runtime.
            factory.set_command_sink(node_id, runtime.command_sink());

            let thread = runtime
                .spawn_dedicated()
                .map_err(|e| format!("facade init: cannot spawn actor thread: {e}"))?;

            Ok((thread, handle, factory))
            })
        }));

        // A panic in assembly is a typed failure, not a dropped channel.
        let result = match assembly {
            Ok(result) => result,
            Err(payload) => Err(format!(
                "multi-node assembly panicked: {}",
                panic_payload_message(payload)
            )),
        };

        // Wrap the (now-untouched) `arc` around the result before sending.
        // The channel's type is Result<(RuntimeThread, Handle, Arc<Runtime>,
        // TonicTransportFactory), String>, so the order must match exactly.
        // The main thread is waiting; a send failure must not panic this thread.
        let _ = tx.send(
            result.map(|(thread, handle, factory)| (thread, handle, arc, factory)),
        );
    });

    let result = match ch.1.recv() {
        Ok(result) => result,
        Err(e) => {
            return Err(ArachneError::Unrecoverable(format!(
                "multi-node assembly thread exited without sending a result: {e}"
            )));
        }
    };
    match result {
        Ok((thread, handle, rt_arc, factory)) => {
            Ok(FacadeState {
                handle,
                thread,
                tonic: Some(TonicState { rt: rt_arc, factory }),
            })
        }
        Err(err) => Err(ArachneError::Unrecoverable(err)),
    }
}

/// Extract a human-readable message from a caught panic payload.
#[cfg(feature = "transport-tonic")]
fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// One assembled multi-node cluster member: the client [`Handle`], the dedicated
/// actor [`RuntimeThread`], and the [`TonicTransportFactory`] whose gRPC servers
/// run on the caller's tokio runtime (see [`assemble_cluster`]).
#[cfg(feature = "transport-tonic")]
pub struct AssembledClusterNode {
    pub handle: Handle,
    pub thread: RuntimeThread,
    pub tonic: TonicTransportFactory,
}

/// Assemble one multi-node cluster member **without** the single-instance global
/// ([`INSTANCE`]). The in-process counterpart of [`Arachne::start`]: it builds
/// the same tonic-backed node (factory + WAL + runtime + actor thread) but hands
/// the caller the pieces, so a single process can host several independent
/// nodes — which is what the cluster integration tests need.
///
/// The gRPC servers spawned by the factory run on the **caller's** tokio
/// runtime (this is an `async` function); the raft actor itself runs on a
/// dedicated OS thread, as with [`Arachne::start`].
///
/// On teardown await [`AssembledClusterNode::tonic`].shutdown and
/// call `thread.shutdown` to release the listener, inbound channels, and the
/// data-dir lock.
#[cfg(feature = "transport-tonic")]
pub async fn assemble_cluster(
    config: ClusterConfig,
) -> Result<AssembledClusterNode, ArachneError> {
    let profile = config.effective_profile()?;
    let cluster_id = config.cluster_id;
    let node_id = config.node_id;
    let listen = config.listen;
    let data_dir = config.data_dir.clone();
    let addresses = config.addresses.clone();
    let initial_cluster = config.initial_cluster;

    let factory = TonicTransportFactory::new(
        cluster_id.clone(),
        PROTOCOL_MAJOR,
        PROTOCOL_MINOR,
        Vec::new(),
        addresses.clone(),
    );
    factory
        .snapshot_provider(snapshot_source::DataDirSnapshots::new(data_dir.clone()));
    factory.snapshot_rate_bps(profile.snapshot_transfer_rate_bps);

    let mut wal = open_storage(
        "assemble_cluster",
        &cluster_id,
        &node_id.to_string(),
        &data_dir,
        &profile,
        ClusterConfig::now_millis(),
    )?;
    wal.enable_offloaded_durability()
        .map_err(|e| ArachneError::Unrecoverable(format!("assemble_cluster: cannot enable offloaded durability: {e}")))?;

    // Bind this node's own listener (writes its real bound address back into the
    // factory's shared map, so a `:0` self-entry becomes a usable dial target).
    factory
        .start_with_bind(node_id.clone(), listen)
        .await
        .map_err(|e| ArachneError::Unrecoverable(format!("assemble_cluster: start_with_bind: {e}")))?;

    let (tx, rx) = factory.create(node_id.clone());

    let mut peers: HashMap<RaftId, NodeId> = HashMap::new();
    for (i, id) in initial_cluster.iter().enumerate() {
        if *id != node_id {
            peers.insert((i + 1) as RaftId, id.clone());
        }
    }
    let self_raft_id = (initial_cluster
        .iter()
        .position(|n| n == &node_id)
        .unwrap_or(0)
        + 1)
        as RaftId;

    let raft = RaftNodeConfig::from_profile(&profile);
    let runtime_config = RuntimeConfig {
        self_raft_id,
        self_node_id: node_id.clone(),
        peers,
        addresses,
        raft,
        profile: profile.clone(),
        metrics: Arc::new(Metrics::new()),
    };

    let logger = Logger::root(slog::Discard.fuse(), o!());

    let (runtime, handle) = Runtime::new(runtime_config, wal, tx, rx, &logger)
        .map_err(|e| ArachneError::Unrecoverable(format!("assemble_cluster: {e}")))?;
    // Register this node's command sink with the transport so its gRPC
    // `Forward` RPC can run forwarded client commands against the runtime.
    // The sink is looked up at request time (not start time), so it can be
    // registered after `start_with_bind`.
    factory.set_command_sink(node_id, runtime.command_sink());
    let thread = runtime
        .spawn_dedicated()
        .map_err(|e| ArachneError::Unrecoverable(format!("assemble_cluster: cannot spawn actor thread: {e}")))?;

    Ok(AssembledClusterNode {
        handle,
        thread,
        tonic: factory,
    })
}

#[cfg(not(feature = "transport-tonic"))]
fn start_multi_node(_config: ClusterConfig) -> Result<FacadeState, ArachneError> {
    Err(ArachneError::Unrecoverable(
        "multi-node requires the transport-tonic feature".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arachne_kv_seam::Transport;

    /// `PeerlessTx::send` must resolve to `Ok(())` immediately.
    ///
    /// This test does *not* touch the global `INSTANCE` static (it exercises
    /// the in-process transport in isolation), so it is safe to run
    /// concurrently with the integration tests that own a real node.
    #[tokio::test]
    async fn peerless_tx_resolves_ok() {
        let tx: PeerlessTx = PeerlessTx;
        let val = tx
            .send(NodeId::new(1.to_string()), TransportMessage::Raft(Vec::new()))
            .await;
        assert_eq!(val, Ok(()));
    }

    /// The peerless path's raft id is the singleton marker `1` (a node with
    /// no peers has nothing to disambiguate against).
    #[test]
    fn peerless_raft_id_is_singleton_marker() {
        let cfg = ClusterConfig::single_node(5, "/tmp/arachne-peerless", WalConfig::default());
        assert_eq!(cfg.raft_id(), 1, "peerless node's raft id must be the singleton marker");
    }

    /// The multi-node path's raft id is the deterministic 1-based position in
    /// `initial_cluster` — the same mapping the node binary and tests rely on.
    #[test]
    fn member_raft_id_is_position_based() {
        let n1 = NodeId::new("n1");
        let n2 = NodeId::new("n2");
        let n3 = NodeId::new("n3");
        let listen = "127.0.0.1:7000".parse().unwrap();
        let base_initial = vec![n1.clone(), n2.clone(), n3.clone()];

        let cfg1 = ClusterConfig::member(
            "c".to_string(),
            n1.clone(),
            listen,
            PathBuf::new(),
            base_initial.clone(),
            HashMap::new(),
        );
        assert_eq!(cfg1.raft_id(), 1, "first member is raft id 1");

        let cfg2 = ClusterConfig::member(
            "c".to_string(),
            n2.clone(),
            listen,
            PathBuf::new(),
            base_initial.clone(),
            HashMap::new(),
        );
        assert_eq!(cfg2.raft_id(), 2, "second member is raft id 2");

        let cfg3 = ClusterConfig::member(
            "c".to_string(),
            n3.clone(),
            listen,
            PathBuf::new(),
            base_initial.clone(),
            HashMap::new(),
        );
        assert_eq!(cfg3.raft_id(), 3, "third member is raft id 3");
    }

    fn unique_temp_dir(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "arachne-server-{tag}-{}-{n}",
            std::process::id()
        ))
    }

    #[test]
    fn effective_profile_none_uses_lan_preset() {
        let cfg = ClusterConfig::single_node(1, "/tmp/arachne-eff-none", WalConfig::default());
        let effective = cfg.effective_profile().expect("preset must validate");
        let preset = Profile::Lan.config();
        assert_eq!(
            effective.snapshot_threshold_bytes,
            preset.snapshot_threshold_bytes,
            "None must use the profile preset"
        );
        assert_eq!(
            effective.wal_trailing_keep_bytes,
            preset.wal_trailing_keep_bytes,
            "None must use the profile preset"
        );
    }

    #[test]
    fn effective_profile_some_overrides_preset() {
        let mut override_profile = Profile::Lan.config();
        override_profile.snapshot_threshold_bytes = 256 * 1024 * 1024;
        override_profile.wal_trailing_keep_bytes = 256 * 1024 * 1024;

        let cfg = ClusterConfig::single_node(1, "/tmp/arachne-eff-some", WalConfig::default())
            .with_profile_config(override_profile);

        let effective = cfg.effective_profile().expect("override must validate");
        assert_eq!(effective.snapshot_threshold_bytes, 256 * 1024 * 1024);
        assert_eq!(effective.wal_trailing_keep_bytes, 256 * 1024 * 1024);
    }

    #[test]
    fn effective_profile_invalid_override_is_invalid_config() {
        let mut override_profile = Profile::Lan.config();
        // `rpc_timeout_ms` must be strictly below `election_timeout_ms`.
        override_profile.rpc_timeout_ms = override_profile.election_timeout_ms;

        let cfg = ClusterConfig::single_node(1, "/tmp/arachne-eff-invalid", WalConfig::default())
            .with_profile_config(override_profile);

        match cfg.effective_profile() {
            Err(ArachneError::InvalidConfig(message)) => {
                assert!(
                    message.contains("invalid profile config"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn convenience_builders_compose_from_effective_profile() {
        let cfg = ClusterConfig::single_node(1, "/tmp/arachne-builders", WalConfig::default())
            .with_snapshot_threshold_bytes(1)
            .with_wal_trailing_keep_bytes(2);

        let effective = cfg.effective_profile().expect("builder profile must validate");
        assert_eq!(effective.snapshot_threshold_bytes, 1);
        assert_eq!(
            effective.wal_trailing_keep_bytes, 2,
            "the second builder must retain the first builder's change"
        );
    }

    #[test]
    fn with_profile_config_is_wholesale_then_builders_compose() {
        let wan = Profile::Wan.config();
        let cfg = ClusterConfig::single_node(1, "/tmp/arachne-wholesale", WalConfig::default())
            .with_profile_config(wan.clone())
            .with_snapshot_threshold_bytes(123);

        let effective = cfg.effective_profile().expect("override must validate");
        assert_eq!(
            effective.heartbeat_interval_ms, wan.heartbeat_interval_ms,
            "with_profile_config must replace the preset wholesale, not merge it"
        );
        assert_eq!(
            effective.election_timeout_ms, wan.election_timeout_ms,
            "with_profile_config must replace the preset wholesale, not merge it"
        );
        assert_eq!(
            effective.snapshot_threshold_bytes, 123,
            "a later convenience builder must derive from the current effective profile"
        );
        assert_eq!(effective.wal_trailing_keep_bytes, wan.wal_trailing_keep_bytes);
    }

    #[test]
    fn open_storage_applies_trailing_keep() {
        let mut profile = Profile::Lan.config();
        profile.wal_trailing_keep_bytes = 12_345;
        let dir = unique_temp_dir("open-keep");
        {
            let storage = open_storage("test", "c", "n1", &dir, &profile, 1)
                .expect("open storage");
            assert_eq!(storage.trailing_keep_bytes(), 12_345);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_storage_zero_trailing_keep_is_escape_hatch() {
        let mut profile = Profile::Lan.config();
        profile.wal_trailing_keep_bytes = 0;
        let dir = unique_temp_dir("open-keep-zero");
        {
            let storage = open_storage("test", "c", "n1", &dir, &profile, 1)
                .expect("open storage");
            assert_eq!(
                storage.trailing_keep_bytes(),
                0,
                "0 must mean compact everything"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_storage_peerless_default_keeps_lan_64_mib() {
        // Regression for the old peerless path, which left the trailing keep at
        // its 0 default; the shared helper must apply the profile's value.
        let profile = Profile::Lan.config();
        let dir = unique_temp_dir("open-keep-default");
        {
            let storage = open_storage("test", "c", "n1", &dir, &profile, 1)
                .expect("open storage");
            assert_eq!(storage.trailing_keep_bytes(), 64 * 1024 * 1024);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
