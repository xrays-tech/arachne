# Upstream request: expose `ProfileConfig` / `snapshot_threshold_bytes` on `ClusterConfig`

## Summary

`arachne-kv 0.6.0` exposes only the `Profile::Lan` / `Profile::Wan` presets on
`ClusterConfig` (`pub profile: Profile`). The server converts the preset with
`config.profile.config()`, and the LAN preset fixes:

```text
snapshot_threshold_bytes = 64 MiB
wal_trailing_keep_bytes  = 64 MiB
```

That threshold is too small for Hydra's current config tree. At **150k hierarchy
rows** the tree is **78,176,097 bytes (~74.5 MiB)**. With the 64 MiB trigger the
150k drill produces:

```text
hydra_arachne_leader_flips_total        11
hydra_arachne_publish_total{result="error"} 16
follower config propagation             > 180 s (test fails)
```

The failure mode is safe (no data loss; writes answer `503 config_not_published`
and the nodes eventually converge after another head), but snapshot creation
blocks apply and the leader loses quorum repeatedly.

## Experiment

A local patch added an override field:

```rust
pub struct ClusterConfig {
    pub profile: Profile,
    /// `None` = `profile.config()`.
    pub profile_override: Option<ProfileConfig>,
}
```

and used `profile_override.clone().unwrap_or_else(|| profile.config())` in the
peerless and multi-node assembly paths. Hydra set:

```text
snapshot_threshold_bytes = 256 MiB
wal_trailing_keep_bytes  = 256 MiB
```

The same 150k drill then **passed**:

```text
hydra_arachne_leader_flips_total        5
hydra_arachne_publish_total{result="error"} 5
follower config propagation             7 s
final counts                            u25000 / k100001 on all 3 nodes
```

So the snapshot threshold, not the data volume, is the cliff at this scale.

## Ask

Please expose one of:

1. `pub profile_config: ProfileConfig` (or an override alongside the preset) on
   `ClusterConfig`, or
2. a direct `snapshot_threshold_bytes` / `wal_trailing_keep_bytes` setter, or
3. a documented environment/config knob with the same effect.

The values must be applied before `WalStorage::open` / runtime assembly, and
`wal_trailing_keep_bytes` should be allowed to follow or exceed the threshold.
Please also document the trade-off: a larger threshold means less frequent
snapshots but more WAL retained; the snapshot itself is still the current state
size.

## Reproduction

```bash
export USE_BULK=1
export SCALE_GROUPS=1500 SCALE_USERS=25000 SCALE_ROLES=25000 SCALE_KEYS=100000
python3 .acceptance/tenant_scale_stability.py
```

- 64 MiB default: `/tmp/hydra-scale-150k-145642.log` (fails `bulk propagation`)
- 256 MiB override: `/tmp/hydra-scale-150k-snap256-150906.log` (passes)
