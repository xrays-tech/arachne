# 开发计划：在 `ClusterConfig` 暴露 `ProfileConfig` / snapshot threshold 覆盖

> 响应：`dev-docs/arachne-kv-snapshot-threshold-request.md`（Hydra 150k 配置树）。
> 状态：**Phase 1 已完成（fix-6）；Phase 2 已完成（node TOML + E2E + 文档 lane）；Phase 3 无新增版本 bump（HEAD `db75a96` 已是 0.6.0）**。
> 目标版本：`arachne-kv 0.6.0`（HEAD `db75a96` 已是 0.6.0；本 feature 不再 bump；无数据格式/线协议变更）。
> 关联：`dev-docs/plan-vs-etcd.md`（Profile/存储调参）、`dev-docs/runbook-format-v4-upgrade.md`（本次不涉及格式升级）。

---

## 1. 背景与目标

Hydra 150k 配置树约 **74.5 MiB**。`Profile::Lan` 固定：

```text
snapshot_threshold_bytes = 64 MiB
wal_trailing_keep_bytes  = 64 MiB
```

64 MiB 触发点导致 150k drill 频繁 snapshot/compact（snapshot 期间 apply 阻塞），
leader flips 11 次、publish error 16 次、follower 传播 >180s。Hydra 本地实验把
两者调到 256 MiB 后 drill 通过（flips 5、error 5、传播 7s）。

目标：

- 让 embedder 在 **`ClusterConfig`** 上提供完整的 `ProfileConfig`（或至少
  `snapshot_threshold_bytes` / `wal_trailing_keep_bytes`）覆盖；
- 覆盖值在 **WAL open / runtime assembly 之前**生效；
- `wal_trailing_keep_bytes` 允许跟随或大于 threshold；
- 文档化 trade-off：更大 threshold = 更少 snapshot、更多 WAL 保留；snapshot 体积
  仍是当前 state 大小，且单次 snapshot 仍会阻塞 apply（只是频率降低）。

非目标：

- 不改 snapshot/compaction 算法、不改数据格式、不改线协议；
- 不改 `Profile::Lan`/`Wan` 预设值（默认行为保持）；
- 不做自动调参/自适应 threshold；
- 不在本仓跑 Hydra 150k drill（下游验收）。

---

## 2. 现状与代码锚点

| 事实 | 落点 |
|---|---|
| `ClusterConfig` 只有 `pub profile: Profile`（preset），无 `ProfileConfig` 覆盖 | `arachne/src/server/mod.rs:406-436` |
| `single_node` / `member` 构造器都固定 `profile: Profile::Lan` | `server/mod.rs:435,457` |
| 装配路径三处都用 `config.profile.config()` | `assemble_peerless:516`、`start_multi_node:574`、`assemble_cluster:731` |
| 多节点路径 open 后设置 trailing keep | `start_multi_node:605`、`assemble_cluster:762` |
| **peerless 路径没有设置 trailing keep**（默认 0 = compact everything） | `assemble_peerless:515-560` |
| `WalStorage::open` 默认 `trailing_keep_bytes = 0`；有公开 setter/getter | `storage/wal.rs:527,1078,1083` |
| `ProfileConfig` 有 `snapshot_threshold_bytes` / `wal_trailing_keep_bytes`；`validate` 只校验 timing | `profile.rs:58-95,180-215` |
| `arachne-node` 已有 `profile_config: ProfileConfig`（preset + TOML timing 覆盖），`to_cluster_config` 只传 `profile`/`wal` | `arachne-node/src/config.rs:81-84,280-332` |
| `arachne-node` 的 `Arachne::open` 直接用 `config.profile_config`，并设置 trailing keep | `arachne-node/src/node.rs:98-111` |
| 现有一处 `ClusterConfig` 结构体字面量（需加 `profile_config`） | `arachne-node/src/config.rs:318`；测试里的 `ClusterConfig::member(...)` 是构造器调用，无需改 |
| `ClusterConfig.wal` 在 facade 装配中是 dead field | `server/mod.rs` 无 `config.wal` 引用；三处从 profile 派生 `WalConfig` |
| 既有 snapshot/trailing keep 测试走 `Runtime` 直连，不经 facade | `arachne/tests/m2_snapshot.rs`、`m2_trailing_keep.rs` |

---

## 3. API 方案（推荐 + 备选）

### 推荐：`ClusterConfig` 增加 `pub profile_config: Option<ProfileConfig>` + builder

```rust
pub struct ClusterConfig {
    // ...existing fields...
    /// `None` = `self.profile.config()`. Some = full override (already validated
    /// by the caller or validated at assembly).
    pub profile_config: Option<ProfileConfig>,
}

impl ClusterConfig {
    /// Set the full profile override (preset + any per-field overrides).
    pub fn with_profile_config(mut self, profile_config: ProfileConfig) -> Self;

    /// Convenience: derive from the preset, override only the snapshot trigger.
    pub fn with_snapshot_threshold_bytes(mut self, bytes: u64) -> Self;

    /// Convenience: derive from the preset, override only the trailing keep.
    pub fn with_wal_trailing_keep_bytes(mut self, bytes: u64) -> Self;
}
```

- **Builder 组合语义（Gate-1 must-fix）**：每个 convenience builder 从**当前 effective**
  profile 派生并折入 `profile_config`，链式调用不会互相覆盖
  （`with_snapshot_threshold_bytes(a).with_wal_trailing_keep_bytes(b)` 两者都保留）。
  `with_profile_config(pc)` 是**整体替换** preset（不合并），优先级高于 `profile`；
  `profile` 仅作为 `None` 时的默认/来源。加组合单测。
- `effective_profile(&self) -> ProfileConfig`：
  `self.profile_config.clone().unwrap_or_else(|| self.profile.config())`。
- 构造器 `single_node`/`member` 初始化为 `profile_config: None`。
- 这是**字段新增**：对 `ClusterConfig { ... }` 字面量是 breaking；0.6.0 允许，
  仓内所有字面量随本变更更新；下游若用 `single_node`/`member`/builder 不受影响。
- builder 让下游不必写 `Option` 或完整字面量。

**备选（若 Oracle 拒绝字段新增）**：

1. **builder-only + 私有字段**：`ClusterConfig` 增加私有 `profile_config`；外部
   字面量本来就不能构造（有私有字段），但 `single_node`/`member`/builder 可用；
   仍需改 `arachne-node` 的字面量为构造器。语义同推荐方案。
2. **`Profile::Custom(ProfileConfig)` 变体**：`ClusterConfig.profile` 保持单字段；
   但 `Profile` 会失去 `Copy`、破坏穷尽 match；不推荐。
3. **仅 env/global knob**：不改公开 struct；用
   `ARACHNE_SNAPSHOT_THRESHOLD_BYTES` / `ARACHNE_WAL_TRAILING_KEEP_BYTES`
   在装配时覆盖。类型安全性差、测试隔离差；仅作最后备选。

> 裁决点：是否接受 `ClusterConfig` 的 0.6.0 breaking 字段新增；还是采用
> builder-only/私有字段。

---

## 4. 装配与生效顺序

1. `effective_profile()` **先于** `WalOptions`/`WalStorage::open` 计算并
   `ProfileConfig::validate()`。
2. `snapshot_threshold_bytes` 进入 `RuntimeConfig.profile`，在 `Runtime::new` 前生效。
3. `wal_trailing_keep_bytes` 在 `WalStorage::open` 之后、`Runtime::new` / 任何
   compaction 之前通过 `set_trailing_keep_bytes` 应用。
   - WAL open 本身不做 compaction（compaction 由 runtime 的 `maybe_snapshot`
     驱动），因此“open 后、runtime 前”是安全的。
   - 若 Oracle 要求字面意义上的“open 前”，新增
     `WalOptions.trailing_keep_bytes` 字段或 `WalStorage::open_with_trailing_keep`
     （同样是 0.6.0 breaking；会扩大改动面）。
4. **peerless 路径无条件补齐** `wal.set_trailing_keep_bytes(profile.wal_trailing_keep_bytes)`。
   - 当前 peerless 默认 0（compact everything）；补齐后默认 64 MiB，单节点 WAL
     保留增加。这是有意的行为一致化，**不是能力增强**（peerless 无 follower，保留日志
     没有功能用途）；在 CHANGELOG 与 `single_node`/`assemble_peerless` rustdoc 中显著说明。
   - 显式 escape hatch：`with_wal_trailing_keep_bytes(0)` 恢复“compact everything”。
5. 建议把三处 `WalStorage::open` + setter 抽成
   `fn open_storage(...) -> Result<WalStorage, ArachneError>`
   （或 `open_wal`），统一 `WalOptions` 构造与 trailing keep 应用，避免再次漏路径。
6. `start_multi_node` 会 move `config` 字段；`effective_profile()` 必须在 move 前。
7. **Ordering invariant（Gate-1 must-fix）**：在 `open_storage` 注释中钉住
   “`WalStorage::open` 只做 recovery，不做 compaction；compaction 由 runtime 在
   snapshot 后驱动；因此在这里 set trailing keep 等价于 open 时设置”。不新增
   `WalOptions.trailing_keep_bytes` / `open_with_trailing_keep`。
8. **`ClusterConfig.wal` 在 facade 装配中是 dead field**（`server/mod.rs` 无
   `config.wal` 引用；三处从 profile 派生 `WalConfig`）。新 knob 只经 `profile_config`
   生效；不要顺手把 `wal` 接线改掉（另一个独立议题）。

---

## 5. 校验与错误

- `effective_profile()` 中调用 `ProfileConfig::validate()`；预设已有效，覆盖值可能无效
  （例如 `rpc_timeout_ms >= election_timeout_ms`）。
- 错误映射（Gate-1 裁决）：采用 **`ArachneError::InvalidConfig(String)`**（语义比
  `InvalidArgument` 准确；0.6.0 已允许枚举新增变体）。在 `WalStorage::open` 之前返回
  （不创建/不打开数据目录）。若团队坚持零新增变体，备选 `InvalidArgument`。
- 在新 knob 的 rustdoc 中写明：`snapshot_threshold_bytes == 0` 禁用本地 snapshot 触发；
  `wal_trailing_keep_bytes == 0` compact 掉 snapshot 覆盖的全部日志。
- `snapshot_threshold_bytes == 0`：保持既有语义 = 禁用本地 snapshot 触发。
- `wal_trailing_keep_bytes == 0`：保持既有语义 = compact 掉 snapshot 覆盖的全部日志。
- 不强制 `wal_trailing_keep_bytes <= snapshot_threshold_bytes`；请求明确允许跟随/大于。
- 文档需说明：`snapshot_threshold` 按“上次 snapshot 以来 apply 的字节数”触发，不是
  物理 WAL 大小；`wal_segment_bytes` 会对物理占用取整；disk 上界**近似**为
  `threshold + trailing_keep + snapshot_size`（记录开销使物理 WAL 字节大于 applied
  字节，且每 term 可能多出 ~1 segment 的取整 slack）。`threshold` 高于稳态写入量时
  snapshot 可能永不触发（WAL 由总写入量而非 threshold 约束）。

---

## 6. `arachne-node` TOML / 配置

`arachne-node` 已经有 `profile: Profile` + `profile_config: ProfileConfig`（preset +
timing 覆盖）。本计划增加：

```toml
# node.toml（可选；缺省 = profile 值）
snapshot_threshold_bytes = 268435456   # 256 MiB
wal_trailing_keep_bytes  = 268435456
```

- `RawConfig` 增加两个 `Option<u64>`；在 `parse_config` 中写入 `profile_config`；
  继续调用 `validate()`。
- `Config::to_cluster_config()` 传 `profile_config: Some(self.profile_config.clone())`
  （该配置已包含 preset + 覆盖）；`wal` 仍从 `profile_config` 派生。
- `arachne-node/src/node.rs` 的 `Arachne::open` 已直接使用 `config.profile_config`，
  无需改动（但需回归测试）。
- 文档：`arachne-node` 配置示例/README。

---

## 7. 测试计划

### 单元

- `effective_profile`：
  - `profile_config: None` -> `Profile::Lan.config()` / `Wan.config()`；
  - `Some` -> 覆盖值（含 256 MiB threshold/keep）；
  - `with_snapshot_threshold_bytes` / `with_wal_trailing_keep_bytes` 从**当前 effective**
    profile 派生；链式组合（snapshot then keep）两者都保留（Gate-1 must-fix）；
  - 无效覆盖 -> `InvalidConfig`，且错误发生在 WAL open 前。
- `open_storage` helper：temp dir 打开后
  `storage.trailing_keep_bytes() == profile.wal_trailing_keep_bytes`，覆盖
  peerless/multi-node 两条路径的共同逻辑。
- `with_wal_trailing_keep_bytes(0)` escape hatch：显式恢复 compact everything。
- peerless 默认回归：不传覆盖时，profile 的 64 MiB trailing keep 被应用。

### Facade / 装配

- **peerless snapshot threshold**：
  - 用 `with_profile_config` 设小 threshold（如 4 KiB），写足够数据，断言数据目录出现
    snapshot 文件；
  - 用大 threshold（如 64 MiB），写少量数据，断言无 snapshot；
  - 走 `Arachne::start(ClusterConfig::single_node(...))`，用
    `storage::snapshot::parse_snapshot_file_name` 检查文件；facade 单例，测试间
    `Arachne::shutdown()`。
- **peerless trailing keep（facade 级 E2E）**：
  - 用小 threshold + 较大 `wal_trailing_keep_bytes`，写/compact 后检查数据目录最老
    segment 按 keep 保留（复用 `m2_trailing_keep` 的目录检查逻辑，走 facade 级测试）；
  - 同时保留 `open_storage` helper 单测，证明装配路径把正确 profile 传入 helper。
- **multi-node**：
  - `assemble_cluster` 用 `with_profile_config` 设小 threshold；写数据后检查该节点数据
    目录 snapshot（一个测试即可；`snapshot_transfer_rate_bps` 无公开 getter，不做断言）。
- **校验**：无效覆盖的 `Arachne::start` 返回 `InvalidConfig`，且不创建 WAL 文件。
- **回归**：`server_facade`、`server_facade_cluster`、`facade_no_runtime`、
  `m2_snapshot`、`m2_trailing_keep`、`three_node_client`、`leader_hint_self` 全绿；
  更新所有 `ClusterConfig` 字面量。
- **下游验收**（不在本仓）：Hydra 150k drill，256 MiB override；期望 flips/errors
  下降、传播在预算内。上游只提供原语并文档化。

### 文档

- `ClusterConfig`/`ProfileConfig` rustdoc + builder 示例；
- `README`/`docs/api-reference.html` 的 `ClusterConfig` 段落 + node TOML；
- trade-off：threshold/keep/segment/snapshot 体积/阻塞 apply/disk 上界；
- `CHANGELOG` 0.6.0；migration note 只涉及**一处**仓内 `ClusterConfig` 字面量
  （`arachne-node/src/config.rs:318`）和下游字面量（需加 `profile_config: None`）；
  并映射 Hydra 本地实验的 `profile_override` 字段名到最终 `profile_config`。

---

## 8. 版本与兼容

- 目标 **0.6.0**（请求文档的版本）；无数据格式/线协议变更，旧目录可直接启动。
- 若采用公开字段新增：`ClusterConfig` 字面量 breaking；0.x minor 允许，但需在
  CHANGELOG 明确。builder 对 `single_node`/`member` 用户无感。
- 默认行为：`None` = 原 preset；`Lan`/`Wan` 值不变。
- peerless trailing keep 由 0 -> profile 默认（64 MiB）是行为变化；文档/CHANGELOG 明示。

---

## 9. 风险

| # | 风险 | 缓解 |
|---|---|---|
| R1 | `ClusterConfig` 字段新增破坏下游字面量 | 0.6.0 + builder + migration note；仓内全部更新 |
| R2 | peerless trailing keep 行为变化（0 -> 64 MiB）增加单节点 WAL 占用 | 文档/CHANGELOG 明示；可只对 `Some` 覆盖生效（Oracle 裁决） |
| R3 | 大 threshold/keep 增加磁盘占用 | 文档给出 `threshold + keep + snapshot` 上界；默认值不变 |
| R4 | `wal_segment_bytes` 128 MiB 取整，threshold 256 MiB 实际跨 ~2 segments | 文档说明物理取整；测试用小块验证逻辑 |
| R5 | 单次 snapshot 仍阻塞 apply（75 MiB 场景） | 明确“只降频率、不消除单次阻塞”；保留 `snapshot_slow_total` 告警 |
| R6 | `effective_profile` 校验时机/错误类型不当 | 在 WAL open 前校验；`InvalidArgument`（或 Oracle 选新变体） |
| R7 | 三处装配路径再次漏 set trailing keep | 抽 `open_storage` helper + 单测 |
| R8 | Hydra 150k 验收不在本仓 | 下游验收；上游提供原语 + 小规模行为测试 |

---

## 10. 交付阶段与门禁

- **Phase 1（核心 API/装配）**：✅ **已完成（fix-6）**。`ClusterConfig.profile_config` +
  builders + `effective_profile` + `open_storage` helper（peerless trailing keep 补齐）+
  校验 + 单元/helper 测试。
- **Phase 2（node TOML + 集成测试 + 文档）**：✅ **已完成（本 lane）**。
  `arachne-node` 两个 TOML 字段、`to_cluster_config`、facade/multi-node E2E、
  README/API reference/trade-off、`CHANGELOG` 0.6.0。
- **Phase 3（0.6.0 收口）**：0.6.0 已是基线（`db75a96`），**无需版本 bump 或
  lockfile 同步**；剩余全量回归与发布门禁。

---

## 11. Gate-1 裁决结果（已并入）

**Verdict: APPROVE WITH CHANGES**（ora-1，attempt 1/3）。

- **API**：接受公开 `profile_config: Option<ProfileConfig>` + builders（0.6.0 breaking，仓内只有 1 处字面量需改）；否决 `Profile::Custom` 与仅 env knob。
- **Peerless trailing keep**：无条件应用 profile（0 -> 64 MiB），CHANGELOG/rustdoc 显著说明；`with_wal_trailing_keep_bytes(0)` 是 escape hatch；保留日志在 peerless 无功能用途，只是入口一致性。
- **Ordering**：`open` 只做 recovery，不做 compaction；open 后、`Runtime::new` 前 set trailing keep 满足请求；不新增 `WalOptions.trailing_keep_bytes`。
- **错误**：采用 `ArachneError::InvalidConfig(String)`；0.6.0 可吸收枚举新增。
- **测试**：facade-level trailing-keep E2E + `open_storage` helper 单测；做 builder 组合、escape hatch、peerless 默认回归；不做 `snapshot_transfer_rate_bps` 可观测断言。
- **Node TOML**：两个可选字段符合现有 timing-override 模式；`to_cluster_config` 传 `Some(profile_config)`，`profile` 保留为来源/默认，不矛盾。
- **版本**：0.6.0；无格式/线协议变更。
- **必须修正已并入**：字面量范围（仅 1 处）、builder 组合语义、peerless 无条件 + escape hatch、ordering 注释、`ClusterConfig.wal` dead field 说明、trade-off 近似上界。
- **剩余（should-fix/defer）**：`InvalidConfig` 已采用；facade trailing-keep E2E 已列入测试；CHANGELOG 映射 Hydra `profile_override` -> `profile_config`。
