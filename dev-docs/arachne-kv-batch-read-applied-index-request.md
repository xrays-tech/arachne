# Upstream request: batched exact-key reads and applied-index visibility

> 这份文件是给 `arachne-kv` 的上游开发文档（英文正文，可直接粘贴成 issue / PR 描述）。
> 背景来自 Hydra 的 24k 实体配置树、TOC 分片和 3 节点破坏性验收。
> 目标不是让 Arachne 理解 Hydra 的配置树，而是补齐通用 KV 缺少的**批量读**和**写入/应用索引可见性**。

---

## Title

Add `multi_get` and applied-index visibility primitives to `arachne-kv`

## Summary

Hydra keeps its control-plane config in `arachne-kv` as a content-addressed tree:

- `hydra/ctl/head` names the current tree;
- a manifest names 32 content-addressed TOC shards;
- each shard names content-addressed entity values;
- every node materializes the tree into local SQLite + an in-memory snapshot.

At 24k entities (4000 users + 4000 global roles + 16000 API keys), materialization currently needs up to
24,000 individual `get_stale` calls, and a follower can observe the new head before all entity values are
applied locally. We worked around this in Hydra with bounded parallelism, retries, and a force-publish,
but the clean fix belongs in the consensus/KV layer:

1. a **batched exact-key read** (`multi_get`) so 24k values are a few commands, not 24k;
2. a **commit/applied index returned by writes**;
3. a **local `wait_applied(index)`** primitive so a follower can wait until its state machine has caught
   up to the head it just observed.

The existing `watch` API is useful and we may adopt it later; this request is deliberately limited to the
smaller, generic primitives above.

## Current API gaps

`arachne-kv` 0.4.0 currently exposes:

- `get` — linearizable ReadIndex read
- `get_stale` — local, possibly stale, non-monotone read
- `get_stale_with_index` — stale value plus its origin index
- `get_stale_prefix` / `get_stale_range` — consistent prefix/range read with an applied index
- `multi_put` — atomic bounded batch write
- `watch` — consistent prefix snapshot + ordered write events

Missing for a large replicated tree:

- no exact-key batch read; `get_stale_range` cannot select an arbitrary set of entity keys;
- `put` / `multi_put` return `Result<(), ArachneError>`, so the caller cannot record where its write landed;
- no local “wait until applied index >= N” primitive; a follower can only poll `get_stale`;
- `get_stale` remains non-monotone, so a client cannot order two reads without an index.

## The ask

### 1. `multi_get` (highest priority)

A batched exact-key stale read, with a single applied index for the whole result.

```rust
pub type Index = u64;

pub enum ReadConsistency {
    /// Local state-machine read; no quorum; may be stale.
    Stale,
    /// Existing `get`/ReadIndex semantics; for completeness, not required by Hydra.
    Linearizable,
}

pub struct MultiGetResult {
    /// One entry per input key, in input order. `None` = key absent.
    pub values: Vec<Option<Vec<u8>>>,
    /// The state-machine applied index at which all values were read.
    pub applied_index: Index,
}

pub async fn multi_get(
    &self,
    keys: &[&[u8]],
    consistency: ReadConsistency,
) -> Result<MultiGetResult, ArachneError>;
```

Required semantics:

- all values are observed at one applied index; no torn mix from two apply states;
- `Stale` uses the local state machine and must not require a quorum;
- results preserve input order; duplicate input keys may return duplicate results (or be documented as
  deduplicated, but they must not reorder);
- an absent key is `None`, not an error;
- bounds are validated **before** dispatch:
  - max entries (suggest 4096, or a lower configurable value);
  - max total returned bytes (suggest 4 MiB, matching the write-side guard);
  - no silent truncation; an over-limit request is `InvalidArgument` or a documented `Busy`;
- a single key still obeys `max_value_bytes`.

Hydra would call this with batches of 256–512 entity keys. For 24k entities that is ~48–96 calls instead of
24,000.

### 2. Index-returning writes

Keep the existing methods and add index-bearing variants:

```rust
pub async fn put_with_index(
    &self,
    key: &[u8],
    value: &[u8],
) -> Result<Index, ArachneError>;

pub async fn multi_put_with_index(
    &self,
    entries: &[(&[u8], &[u8])],
) -> Result<Index, ArachneError>;

pub async fn delete_with_index(
    &self,
    key: &[u8],
) -> Result<Index, ArachneError>;
```

Alternatives are acceptable: a `WriteOutcome { index: Index }`, or changing the existing return type if
the version policy allows it. Additive methods are preferred.

Required semantics:

- the returned index is the log index of the applied entry containing the write;
- retries with the same `(client_id, seq_no)` session return the original index (idempotent);
- a write forwarded from a follower returns the leader’s committed index, not a local guess;
- the index is comparable across keys and nodes;
- no extra quorum round is required merely to obtain it.

### 3. `wait_applied(index, timeout)`

A local, no-quorum wait for this node’s state machine.

```rust
pub async fn wait_applied(
    &self,
    index: Index,
    timeout: std::time::Duration,
) -> Result<Index, ArachneError>;
```

Required semantics:

- if the local applied index is already `>= index`, return immediately with the current applied index;
- otherwise wake when apply advances past `index`; no busy spin;
- local only: no leader redirect, no quorum, must work on a minority follower;
- timeout returns a named `Timeout`/`Busy` error, not a silent success;
- cancellation-safe; dropping the future must not leak a waiter;
- `index == 0` is a no-op;
- must not hold the state-machine lock across await points.

This is the primitive that closes the “head visible, entity missing” race:

```text
head token + head_index = get_stale_with_index("hydra/ctl/head")
wait_applied(head_index, timeout)
read manifest + shards + entities with multi_get(..., Stale)
```

If the head entry is applied, every earlier entity/shard entry is applied too; the reader no longer needs
to retry a partially visible tree.

### 4. Existing `watch` (no change required)

Hydra may later use `watch` to keep a materializer cache instead of polling. We are not asking for a new
watch design in this request. Please keep the current documented guarantees:

- snapshot at `applied_index`;
- events strictly after the snapshot index;
- bounded channel; disconnect rather than silently drop;
- snapshot over `limit` is rejected, not truncated.

## Why not solve this in Hydra

We already have these workarounds, and they are not the right long-term shape:

| Workaround | Cost |
|---|---|
| 24,000 `get_stale` calls | 24k command/RPC round trips per materialization |
| 64-way parallel read | still hundreds of commands; local workaround, not a semantic guarantee |
| Retry only missing entities | hides replication lag; no way to know how long to wait |
| Force `reload_all_with(true)` after bulk key writes | masks an application-level reload issue; does not fix follower visibility |
| Raise `max_value_bytes` / `multi_put` limits | monolithic TOC still rewrites ~8 MiB per publish at 100k entities; worse than sharding |

Hydra’s TOC sharding and content-addressed manifest are correct application-level design and should stay
in Hydra. The missing pieces are generic KV capabilities.

## Hydra integration sketch

### Publish

```rust
let entity_index = multi_put_with_index(chunk).await?;
// ...
let final_index = multi_put_with_index(&[
    (manifest_key, manifest_bytes),
    (retain_key, retain_bytes),
    (head_key, head_token),
]).await?;
```

Hydra records `final_index` and can use it as the tree’s generation.

### Materialize

```rust
let (head, head_index) = current_head_with_index().await?;
wait_applied(head_index, MATERIALIZE_WAIT).await?;

let manifest = multi_get(&[manifest_key], Stale).await?;
let shard_values = multi_get(&shard_keys, Stale).await?;
let entity_values = multi_get(&entity_keys, Stale).await?;
// verify digests, merge, apply to SQLite
```

The entity values are immutable content-addressed values. Once the head index is applied locally, the
values cannot disappear or change; a later `multi_get` is safe.

## Acceptance criteria

### Functional

- `multi_get` returns one result per input key, in order; missing keys are `None`.
- All values in one result are observed at one `applied_index`.
- Over-limit entries/bytes are rejected before dispatch; no partial result.
- `multi_get` works on a minority follower (`Stale`; no quorum).
- `put_with_index` / `multi_put_with_index` / `delete_with_index` return a monotonically usable index.
- A retried session returns the original index.
- `wait_applied` returns immediately when already caught up, wakes on apply, times out cleanly, and is
  cancellation-safe.
- No state-machine lock is held across an await.

### Cluster / failure

- 3 nodes: write on a follower, read the index on the leader, `wait_applied` on the other follower.
- Kill the leader, elect a new one, repeat; indexes remain comparable.
- Minority follower: `multi_get(Stale)` and `wait_applied` still work; `wait_applied` times out rather than
  hanging.
- Concurrent writer while `multi_get` runs: no torn result; `applied_index` corresponds to the values.
- Existing `watch` and `get_stale_range` tests remain green.

### Load / performance

- A synthetic 24k-key tree, values ~100–500 bytes, `multi_get` batches of 256–512.
- Target: no more than ~100 read commands per materialization, versus 24,000 today.
- `wait_applied` on a lagging follower completes as soon as apply reaches the head, without polling at
  1 Hz.
- Hydra acceptance: 4000 users + 4000 roles + 16000 API keys, 3 nodes, leader/follower kill, Redis
  restart, ClickHouse outage, network partition, quorum loss; no missing entity warnings and all nodes
  report the same final counts within the propagation budget.

## Compatibility / rollout

- Prefer additive client methods; existing `get`/`get_stale`/`put`/`multi_put` behavior must not change.
- New protocol commands need a wire/version gate; old clients must fail loudly if a server does not
  support them, not silently fall back to one-by-one reads.
- `multi_get` must not become a second consistency model; `Stale` and `Linearizable` reuse the existing
  definitions.
- `wait_applied` is local and must not be confused with a quorum barrier.

## Non-goals

- We are not asking for MVCC or arbitrary historical reads.
- We are not asking to raise `max_value_bytes` or the 4 MiB `multi_put` limit.
- We are not asking Arachne to understand Hydra’s TOC, manifest, config tree, or SQLite schema.
- We are not asking to change `watch` in this request.

## Evidence from Hydra

- TOC sharding: 24k entities → 32 shards × ~33–34 KiB, total ~1.08 MB, so the TOC fits the 1 MiB value
  cap without raising `max_value_bytes`.
- Current materialization: 24k individual `get_stale` calls; 64-way parallel read plus missing-entity
  retry is a workaround.
- Observed failure: a follower sees the head but logs repeated
  `tree ... is missing entity user_api_key/...`; acceptance times out at 180 s, then a later admin write
  triggers a new head and all nodes converge to 16,001 keys. No permanent data loss, but poor propagation
  and a real visibility gap.
- Existing `get_stale_with_index` and `get_stale_range` are useful, but neither provides an exact-key
  batch read or a local applied-index wait.

## Suggested delivery order

1. `multi_get` + `MultiGetResult.applied_index` (largest immediate Hydra win).
2. `put_with_index` / `multi_put_with_index` / `delete_with_index`.
3. `wait_applied`.
4. Optional Hydra follow-up: adopt `watch` for cache-driven materialization.

If only (1) ships, Hydra can remove most of the parallel/retry code. If (2) and (3) ship, Hydra can also
remove the local-write-index guesswork and the missing-entity retry loop.
