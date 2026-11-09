# Upstream bug report: Arachne 0.7.0 restart panics when a snapshot is ahead of the raft hard state

> Arachne 0.7.0 / raft-seedable 0.7.0, Hydra's 3-node compose, large-config drill.
> This is a durability/restart bug, not a load-test-harness issue.

## Summary

After a large snapshot is written, restarting a node panics instead of recovering:

```text
thread '<unnamed>' panicked at raft-seedable-0.7.0/src/raft.rs:2839:
hs.commit 7457 is out of range [7458, 7458], raft_id: 1

thread 'main' panicked at arachne-kv-0.7.0/src/server/mod.rs:808:
multi-node assembly thread exited without sending a result: RecvError
```

The node crash-loops. The first symptom before the restart was the control plane
returning `ArachneError::ShuttingDown` to every management write/materialization
attempt; the restart then exposed the persisted inconsistency.

## Environment

- `arachne-kv 0.7.0` (`arachne-kv-seam 0.7.0`, `arachne-kv-transport-tonic 0.7.0`)
- `raft-seedable 0.7.0`
- Hydra `main` with the 3-node compose (`environment/docker-compose.local.yml`)
- Arachne profile: LAN preset with
  `snapshot_threshold_bytes = 256 MiB`
  `wal_trailing_keep_bytes = 256 MiB`
  (set through `ClusterConfig::with_snapshot_threshold_bytes` /
  `with_wal_trailing_keep_bytes`)

## Observed failure

The 200k drill (target: 35k users + 35k roles + 140k api-keys + 2k groups)
stopped early because the control plane became unavailable:

- roles bulk stopped at 33,000/35,000 on `503 config_not_published`
- keys bulk stopped at 92,000/140,000; `POST /api/v1/reload?force=1` answered
  `400 reload_failed` because the publish still failed
- `hydra_arachne_config_bytes` was 78,497,597 (~74.9 MiB)
- `hydra_arachne_leader_flips_total = 6`
- `hydra_arachne_publish_total{result="error"} = 25`
- `hydra_arachne_quorum_unavailable_total{op="publish"} = 2`

At 09:03:36 the node began answering:

```text
WARN hydra_server::cluster::arachne_store: control-plane head watch unavailable; using current_head_with_index fallback error=node is shutting down
WARN hydra: config materialization did not complete; will retry error=config head: arachne: ShuttingDown
```

The container did not restart at that point (`RestartCount=0`, `OOMKilled=false`);
the in-process Arachne runtime had stopped. Restarting the container then hit the
`hs.commit` panic above and entered a crash loop.

## Persisted-state evidence

`/app/data/arachne` on the affected node:

```text
snapshot-00000000000000006868-00000000000000000001.snap  247M
snapshot-00000000000000007458-00000000000000000009.snap  504M
wal-00000000000000006869.log                             130M
wal-00000000000000007268.log                             128M
```

The failing snapshot is at raft index **7458**; the persisted hard state reports
`commit = 7457`. `raft-seedable` treats that as an invalid range and panics.

The snapshot is also unexpectedly large: the current Hydra config tree is
~75–78 MiB, but the snapshot is **504 MiB** (6× the live tree). This may be a
separate snapshot-bloat issue (see below), but it makes the snapshot path much
more expensive and increases the chance of hitting the inconsistent state.

## Reproduction

```bash
# 3-node compose, arachne-kv 0.7.0
export HYDRA_ARACHNE_SNAPSHOT_THRESHOLD_MB=256

export USE_BULK=1
export SCALE_GROUPS=2000 SCALE_USERS=35000 SCALE_ROLES=35000 SCALE_KEYS=140000
python3 .acceptance/tenant_scale_stability.py

# once a large snapshot exists (the drill above creates one at 7458):
docker restart hydra-a
docker logs --since 5m hydra-a
```

The panic is reproducible on every restart of that data directory. The exact
full 212k rows do not need to be reached; the snapshot at 7458 + hard-state
commit 7457 is enough.

## Expected

1. Snapshot creation must persist a hard state whose `commit` is **at least**
   the snapshot index (or the snapshot install path must clamp/validate it).
2. Restart from a valid snapshot + WAL must not panic:
   - restore the snapshot at 7458;
   - replay/commit from the WAL;
   - a node rejoins and converges with its peers.
3. If the persisted state is genuinely inconsistent, Arachne must return an
   explicit recoverable error naming the indices, not panic in a detached
   assembly thread. A panic in the assembly thread currently takes down the
   whole process (`RecvError` → `main` panic → crash loop).

## Suspected area

One or more of:

1. **Snapshot/hard-state ordering**: a snapshot is taken at an applied index
   (7458) while the last persisted hard state is 7457; a crash between those
   writes leaves `snapshot_index > hard_state.commit`.
2. **raft-seedable range check**: `hs.commit` outside the log range panics;
   this should be a typed error, and Arachne should decide whether to recover
   (drop the snapshot and replay WAL) or fail closed with a named error.
3. **Assembly panic containment**: `start_multi_node`'s assembly thread can
   panic; the facade only sees `RecvError` and panics on the main thread.
   A panic in one node should not be the only signal.
4. **Snapshot bloat**: 504 MiB for a 78 MiB live tree suggests unreferenced
   content (old shards/versions) is being included. Hydra's GC keeps the current
   tree and 64 retained heads, but deliberately does not scan/delete old
   `cfg/shard/` values. If the snapshot includes that garbage, every snapshot
   is unnecessarily huge and slow.

## Impact

- Any node that restarts after a large snapshot can crash-loop and never
  rejoin. A single restart then becomes a permanent local data-dir failure.
- Hydra's 200k-scale target is blocked: the snapshot path is both expensive
  (504 MiB) and unsafe to restart.
- The data plane can keep serving last-known-good while the control plane is
  down, so the failure is not immediately user-visible — which makes it more
  important to detect and document.

## What we need

1. A fix ensuring `snapshot_index <= hard_state.commit` (or a documented,
   explicit recovery path when it is not).
2. A typed error instead of the `raft-seedable` panic, plus an Arachne-level
   decision: recover by dropping/replaying, or fail closed with a named error.
3. A regression test:
   - 3 nodes;
   - force a snapshot at index N;
   - persist hard state with `commit = N-1` (or crash between the two writes);
   - restart the node;
   - assert no panic, and either convergence or a documented safe refusal.
4. Clarify the snapshot contents/size and whether unreferenced shards/versions
   are expected to be included. If yes, provide shard/compaction GC; if no,
   that is the bloat bug.
5. Document the recovery contract: what an operator should do when a node
   reports the inconsistent state (wipe local Arachne data and rejoin from
   peers? replay WAL? which is safe?).

## Hydra-side status / temporary workaround

- Hydra stopped the crash-looping node; the other two nodes stay up and serve
  last-known-good data.
- For the test environment, the only reliable reset is
  `docker compose ... down -v` followed by `up -d` (the affected data dir is
  inconsistent).
- There is no safe production workaround until upstream decides the recovery
  semantics; restarting the node just repeats the panic.
- The 150k drill with a 256 MiB threshold passed before this restart test, so the
  write path is not the only concern: **the restart-after-large-snapshot path
  must be part of acceptance**.
