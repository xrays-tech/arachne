# Changelog / 变更说明

面向 `arachne-kv` 使用方的发布说明。内部设计/取舍见
`dev-docs/arachne-kv-batch-read-applied-index-design.md`，升级步骤见
`dev-docs/runbook-format-v4-upgrade.md`。

---

## 0.7.1 — 2026-11-09

主题：**P0 重启修复——快照 index 不得领先于持久化 hard state commit**。

### 修复

- `WalStorage::save_snapshot` 顺序：快照文件 durable（write/fsync/rename/dir-fsync）
  → **写 META** → 将持久化 hard state `commit` 提升到快照 index 并 fsync → 更新内存
  snapshot。消除 `snapshot_index > hard_state.commit` 的持久化状态；唯一剩余崩溃窗口
  `META=N / commit=N-1` 由 `open` 修复。
- `WalStorage::open`：`commit < snapshot_index` 时提升到 snapshot index 并持久化；
  新增 `StorageStats::hard_state_repairs()` 观测，`Runtime::new` 打 warn；
  `initial_state` 再做防御性 clamp；`commit > last_entry` 仍是硬错误。
- `start_multi_node`：装配线程 `catch_unwind`，panic → typed
  `ArachneError::Unrecoverable`；`RecvError` 不再 panic 调用线程。装配后的
  actor 线程 panic 仍是既有未隔离行为。
- 回归：3 节点 snapshot→kill→commit N-1→restart→converge；
  `install_snapshot` META=N/commit=N-1 崩溃窗口；save_snapshot bump / open repair /
  initial_state clamp；m2 snapshot/trailing keep；facade。

### 兼容性

- **无格式/线协议变更**；0.7.0 数据目录可直接启动 0.7.1，首次打开会修复滞后
  commit（`hard_state_repairs` + warn）。
- `StorageStats` 新增 `hard_state_repairs()` getter（additive；0.7.0 无该字段，无 break）。
- 0.7.1 为 patch。

### 已知限制

- 快照体积：KV 快照序列化整个 live map；Hydra 未删除旧的 content-addressed shard，
  504 MiB vs ~75 MiB live 属应用层保留，不是编码 bug。follow-up：Hydra GC（逐 key
  `delete`）或 Arachne range-delete API。快照越大，fsync/rename 崩溃窗口越宽，
  建议优先做 Hydra GC。

---

## 0.6.0 — 2026-11-09

主题：**session payload 冲突显式化**。

### `SessionConflict`（payload hash）

- 复用完全相同的 `(client_id, seq_no)` 但携带**不同 payload** 时，不再静默 dedup
  成旧结果，而是返回 `ArachneError::SessionConflict { client_id, seq_no }`。冲突命令
  **未执行**（known-not-applied）：原 session 的缓存结果、origin index 与 KV 状态均不变；
  同 payload 重试仍是真正的 dedup（exactly-once 保持）。
- state machine 的 `SessionRecord` 增加整条命令的 FNV-64 digest；冲突检查是纯确定性函数，
  返回 `ApplyOutcome::Conflict` 且不修改状态；applied 仍推进（index 连续）。
- **破坏性格式升级**：`KV_SNAPSHOT_VERSION` 3 → 4（digest 必须随快照恢复），storage
  `FORMAT_VERSION` 4 → 5；旧目录 fail-stop，步骤见
  `dev-docs/runbook-format-v5-upgrade.md`。

---

## 0.7.0 — 2026-11-09

主题：**`ClusterConfig` profile / snapshot-threshold 覆盖**。

### `ClusterConfig` profile / snapshot-threshold 覆盖

- `ClusterConfig` 新增 `pub profile_config: Option<ProfileConfig>`：`None` = 使用
  `profile` preset；`Some` = **整体替换** preset（不与 `profile` 逐字段合并），优先级
  高于 `profile`。
- 新增 builder：`with_profile_config`、`with_snapshot_threshold_bytes`、
  `with_wal_trailing_keep_bytes`。convenience builder 从**当前 effective** profile
  派生，链式组合互不覆盖。
- `effective_profile()` 在 WAL open 前校验；无效配置返回
  `ArachneError::InvalidConfig`，数据目录不会创建/打开。`ArachneError` 现共 **19**
  个变体（0.6.0 的 `SessionConflict` + 本版新增的 `InvalidConfig`）。
- **peerless 行为变化**：单节点路径现在也会应用 `wal_trailing_keep_bytes`（此前为
  0 = compact everything；现默认跟随 profile，Lan 为 64 MiB）。
  `with_wal_trailing_keep_bytes(0)` 恢复 compact-everything。peerless 无 follower，
  保留日志没有功能用途，只是入口一致性。
- `arachne-node` TOML 新增可选顶层字段 `snapshot_threshold_bytes` /
  `wal_trailing_keep_bytes`；`to_cluster_config` 把验证后的 `ProfileConfig`
  整体传给 facade。
- **调参 trade-off**：snapshot 触发按“上次 snapshot 以来 apply 的字节数”，不是物理
  WAL 大小；磁盘上界**近似**为 `threshold + trailing_keep + snapshot_size`（记录开销 +
  每 term 约 1 个 segment 的取整 slack 使物理 WAL 更大）。`snapshot_threshold_bytes
  == 0` 禁用本地 snapshot 触发；`wal_trailing_keep_bytes == 0` compact 掉 snapshot
  覆盖的全部日志。threshold 高于稳态写入量时 snapshot 可能永不触发（WAL 由总写入量
  而非 threshold 约束）。
- Hydra 150k 配置树（~74.5 MiB）：64 MiB 默认触发点下 drill 失败（leader flips 11、
  publish errors 16、follower 传播 >180 s）；256 MiB 覆盖后通过（flips 5、errors 5、
  传播 7 s）。注意：单次 snapshot 仍会阻塞 apply，本项只降低 snapshot **频率**，
  不消除单次阻塞。
- **迁移**：`ClusterConfig` 结构体字面量需补 `profile_config: None`（仓内仅 1 处，
  下游字面量同理）；使用 `single_node` / `member` / builder 的调用方无需改动。
  Hydra 本地实验的 `profile_override` 字段名对应最终的 `profile_config`。

---

## 0.5.1 — 2026-11-06

主题：**P0 修复——写入 index 回退 / 静默丢写（跨主机 `client_id` 冲突）**。

### P0 修复

- **node-scoped `client_id`**：不再使用 `std::process::id()`。`client_id` 由
  FNV-1a(`NodeId` + 进程内 counter) 生成，确定性、无 time/random/PID；不同
  host/container 即使 PID 相同也不会得到同一个 `client_id`。
- **序列续接**：`Runtime::new` 在启动时根据该 node 的 `client_id` 计算
  `initial_seq = max_session_seq + 1`，其中 `max_session_seq` 同时覆盖
  **快照中的 session 表**与**尚未被 actor 重放的 WAL tail**。重启后新的
  `(client_id, seq_no)` 不会与本节点**本地已持久化**（快照 + WAL tail）的旧
  session 重叠；因此不会把不同 payload 误判为 dedup 而返回旧 index / 静默丢弃新值。
- **回归保护**（`--features fault-injection`）：
  - `write_index_client_id` 用固定 `client_id` 在**同一数据目录**重启，分别
    覆盖「session 已在快照中」和「session 只在 WAL tail」两种路径；断言重启后
    首次 `put_with_index` 的 index 大于旧最大 index 且新 key 可见。
  - 新增 `client_id_for` 单元测试（同 counter 不同 node、同 node 不同 counter、
    确定性）与 tail 扫描解析单元测试。
  - 扩展 leader churn（3 节点）、quorum-loss（indexed 写入必须失败而非伪造
    index）与 real-tonic follower `multi_put_with_index` 测试。

### 残余风险（P1）

- **复制窗口内崩溃**：写入在本地状态机持久化之前崩溃时，结果仍可能是
  unknown（既有 `Timeout` 语义）；dedup 只保护已经进入本地 session 表的
  `(client_id, seq_no)`。
- **单节点磁盘丢失 / 旧数据目录 re-seed 到活跃集群**：节点身份或 session
  空间被重建后重新加入活跃集群，仍可能产生无法由本次修复覆盖的重叠；按
  runbook 做破坏性升级/替换，不要将旧目录重新接入。
- **完整性机制后续项**：本次不引入 payload hash / `SessionConflict`。后续
  版本承诺以 payload hash 作为 **completeness** 机制，为同一个
  `(client_id, seq_no)` 下不同 payload 的情况提供显式检测，而不是静默
  dedup。
- **运维缓解**：Hydra 侧的读回校验（read-back guard）仍是当前生产环境的
  运行期兜底；在完整性机制交付前不要移除。

---

## 0.5.0 — 2026-11-06

主题：**批量精确读 + 写入/应用索引可见性 + 本地 applied-index 等待**。
对应需求：上游 Hydra 24k 实体配置树的「head 可见但 entity 尚未 apply」竞态。

### 一句话总结

- 24k 个 entity 的物化从 **24,000 次单 key 读** 降到 **~47 次 `multi_get`**；
- 写入可以拿到**实际生效的 log index**，重放返回原始 index；
- follower 可以**本地等待**状态机追平它刚观察到的 head index，不再靠重试/轮询。

---

## 1. 新增 API

### 1.1 `multi_get` —— 精确 key 集合的批量读

```rust
use arachne_kv::{ReadConsistency, MultiGetResult};

pub async fn multi_get(
    &self,
    keys: &[&[u8]],
    consistency: ReadConsistency,
) -> Result<MultiGetResult, ArachneError>;

pub enum ReadConsistency {
    Stale,        // 本地状态机；无 quorum；少数派 follower 可用；可能陈旧
    Linearizable, // 既有 ReadIndex；leader + quorum
}

pub struct MultiGetResult {
    pub values: Vec<Option<Vec<u8>>>, // 与输入 keys 同序；None = 缺键
    pub applied_index: u64,           // 整批 values 被观察时的单一 applied index
}
```

语义：
- 整批 values 在**同一次读锁**下取得，`applied_index` 与 values 属于同一个状态，**无撕裂**；
- 输入顺序严格保留；重复 key 产生重复结果；缺键 = `None`，不是错误；空 `keys` 合法；
- 上限（**不截断**，超限返回 `InvalidArgument`）：
  - 最多 `4096` 个 key；
  - 每个 key ≤ `max_key_bytes`（默认 4 KiB）；
  - 请求 key 总字节 ≤ 4 MiB；
  - 返回 value 总字节 ≤ 4 MiB；
  - 在途 `multi_get` payload 共享 16 MiB 预算，超限返回可重试的 `Busy`。

### 1.2 写入返回 index

```rust
use arachne_kv::Index; // = u64

pub async fn put_with_index(&self, key: &[u8], value: &[u8]) -> Result<Index, ArachneError>;
pub async fn multi_put_with_index(&self, entries: &[(&[u8], &[u8])]) -> Result<Index, ArachneError>;
pub async fn delete_with_index(&self, key: &[u8]) -> Result<Index, ArachneError>;
```

语义：
- 返回**使本次写入实际生效**的 log entry index（不是「提交后回查当前值」的 index）；
- `multi_put_with_index` 整批共享一个 index（一次原子 apply）；
- `delete_with_index` 即使 key 不存在也返回该 delete entry 的 index；
- 同 `(client_id, seq_no)` 重放返回**原始 index**（session 记录持久化了 effective index）；
  - 该保证仅在 session TTL + grace 内；GC 后的重试是新 session，返回新 index；
- follower 转发的写入返回 **leader** 的 index，不是本地猜测；
- 旧 peer 不支持时返回 `Unsupported`（terminal，操作未执行），绝不伪造 index 0。

### 1.3 `wait_applied` —— 本地 applied-index 等待

```rust
use std::time::Duration;

pub async fn wait_applied(
    &self,
    index: Index,
    timeout: Duration,
) -> Result<Index, ArachneError>;
```

语义：
- 若本地**状态机** `applied >= index`，立即返回当前 applied（可能 ≥ index）；
- 否则在 apply 推进到 `index` 时被唤醒（事件驱动，无忙轮询）；
- 纯本地：不 redirect、不要求 quorum，**少数派 follower 可用**；
- 超时返回 `Timeout`；`index == 0` 为 no-op；`timeout == 0` 立即 `Timeout`；
- 取消安全：drop future 不会泄漏 waiter；
- **不是 quorum barrier**，不替代线性读。

### 1.4 新增错误变体

```rust
ArachneError::Unsupported(String)
```

- 对端不支持所请求的能力（旧 peer 的 `multi_get` / indexed write、wire `unknown_kind` 或缺字段）；
- **terminal**：操作未执行；对同一 peer 重试同一调用无益；
- `ArachneError` 现为 **17 个变体**；对 `ArachneError` 做穷尽 `match` 的下游代码需要新增一个分支。

---

## 2. 推荐用法（Hydra 形状）

### 2.1 发布

```rust
// 1) 先写实体/分片（content-addressed，幂等）
let entity_index = handle.multi_put_with_index(&entity_chunk).await?;

// 2) 最后写 manifest + head（同一事务性批次）
let final_index = handle.multi_put_with_index(&[
    (manifest_key, manifest_bytes),
    (retain_key, retain_bytes),
    (head_key, head_token),
]).await?;

// final_index 可作为整棵树的 generation
```

### 2.2 物化（关闭「head 可见、entity 缺失」竞态）

```rust
let (head, head_index) = handle
    .get_stale_with_index(head_key)
    .await?
    .expect("head exists");

// 等待本地状态机真正 apply 到 head index
handle.wait_applied(head_index, MATERIALIZE_WAIT).await?;

// head 已 apply ⇒ 更早写入的 entity/shard 也已 apply
let shards = handle.multi_get(&shard_keys, ReadConsistency::Stale).await?;
let entities = handle.multi_get(&entity_keys, ReadConsistency::Stale).await?;

// 校验 digest、合并、写入本地 SQLite
```

前提（必须由应用层保证）：**entity/shard 的写入先于 head**，因此它们的 log index 小于
`head_index`。Arachne 只保证「同一状态机按 log 顺序 apply」，不解释树/manifest/SQLite 语义。

---

## 3. 兼容性

### 3.1 API

- 既有 `put` / `get` / `get_stale` / `get_stale_with_index` / `multi_put` / `delete` /
  `cas` / `watch` / `get_stale_prefix` / `get_stale_range` 的签名与语义**不变**；
- 新 API 全部为 additive；
- crate 版本 `0.4.0 → 0.5.0`。

### 3.2 数据目录（破坏性）

- KV 快照版本 `2 → 3`：每条 session 记录新增 8 字节 effective index；
- storage `FORMAT_VERSION 3 → 4`；
- **没有新增命令 opcode**；WAL 命令流对 0.3.x 二进制仍可解析，破坏性来自 **session 快照格式 + 升级策略**；
- 旧 v3 数据目录在 0.5.0 启动打开时**直接 fail-stop**（格式不匹配）；
- 不允许「v3 目录原地升 v4」；不允许新 leader 向旧节点发送 v3 快照；
- 推荐：**整集群停机 → 备份旧目录 → 重播种/迁移 → 启动 0.5.0**；
- 回滚 = 恢复升级前整目录备份 + 旧二进制。

完整步骤：`dev-docs/runbook-format-v4-upgrade.md`。

### 3.3 线协议

- `PROTOCOL_MINOR` **保持 0**（不 bump；bump 会破坏滚动升级握手）；
- 新增 `Forward` kind：
  - `kind=3` = `multi_get`
  - `kind=4` = `ProposeWithIndex`
- 旧 peer：
  - 对未知 kind 在**执行前**返回 `unknown_kind`；
  - 对 index 写省略 `commit_index`；
  - 新客户端统一映射为 `Unsupported`，**绝不**静默回退为逐 key 读或伪造 index 0。
- `multi_get` 的 `Some(vec![])`（present empty）在 wire 上可区分于 `None`。

---

## 4. 错误与重试

| 错误 | 含义 | 建议 |
|---|---|---|
| `Timeout` | 有界等待超时；写的结果**未知** | 用同一 session 重试（幂等）；`wait_applied` 可重试 |
| `Busy` | 队列/预算满 | 稍后重试；`multi_get` 可拆小批 |
| `QuorumUnavailable` | 无 quorum，或目标 peer runtime 已不可用（stale hint） | 重试/换节点；`multi_get(Stale)` / `wait_applied` 仍可本地工作 |
| `NotLeader { hint }` | 不是 leader | 跟随 hint 重定向 |
| `Unsupported` | 旧 peer / 不支持该能力 | **terminal**；升级对端，不重试同一调用 |
| `InvalidArgument` | 超限或参数非法 | 拆批/修正参数；不会部分执行 |
| `Unrecoverable` | 致命存储/状态机错误 | fail-stop |

补充：0.5.0 修复了「stale hint 指向已退出 runtime 但 listener 仍存活」时被错误归类为
`Unrecoverable` 的问题；现在返回可重试的 `QuorumUnavailable`。

---

## 5. 性能与验证摘要

以下为本机 loopback 测试环境数据，**不是容量承诺**；生产请按自身硬件/网络复测。

| 场景 | 结果 |
|---|---|
| 单节点 24k keys（200 B，512/批） | **47 条读命令**（vs 24,000），~0.8 s |
| 真实 tonic 3 节点 24k | 每轮 47 条命令，15–38 ms；杀 leader 重选 273–379 ms；24k/24k 一致 |
| 10 分钟混合读写 + 故障（无 pause） | **1,393.8 ops/s**、**706.7 write ops/s**；423,928 writes / 836,263 ops；24k/24k 校验；**0 fatal / 0 Unrecoverable**；survivor applied 53,124 |
| 10 分钟 p50/p99 | write 15.5/66.1 ms；get 10.3/57.2 ms；get_stale 53 µs/7.3 ms；multi_get 315 µs/7.5 ms；wait_applied 39 µs/4.8 ms |

压测脚本（`#[ignore]`，按需运行）：

```bash
cargo test -p arachne-kv --test batch_read_load -- --ignored --nocapture
cargo test -p arachne-kv-transport-tonic --test stress_24k_tonic -- --ignored --nocapture
cargo test -p arachne-kv --test stress_24k_partition -- --ignored --nocapture
cargo test -p arachne-kv-transport-tonic --test stress_10min_mixed -- --ignored --nocapture
# 快速 smoke：ARACHNE_STRESS_SECS=30 ...
```

---

## 6. 已知限制 / 后续

- 本发布**未**包含：多小时 soak、`docker/bench` 1w/4w/8w 吞吐 A/B、bounded fuzz 实跑、
  Hydra 下游 4000 users + 4000 roles + 16000 API keys 的端到端验收（下游负责）。
- 10 分钟 soak 中 RSS 从 14 MiB 增至 267 MiB；主要是 24k 数据、持续写入的 WAL/快照、
  session 表与测试缓冲，**未证明是泄漏**；更长 soak 的内存曲线建议作为后续观察项。
- 内部测试加固 follow-up：3 节点快照恢复的 session index 传播、forwarded `multi_get`
  actor 侧限额复核、budget/waiter 饱和测试、`Many` 读的 INV14 l2 场景、ConfChange-index
  wait、写负载下 deadline 精度、wait + fail-stop。

---

## 7. 参考

- 设计：`dev-docs/arachne-kv-batch-read-applied-index-design.md`
- 开发计划：`dev-docs/plan-batch-read-applied-index.md`
- 升级 runbook：`dev-docs/runbook-format-v4-upgrade.md`
- API reference：`docs/api-reference.html`
- 教程：`docs/tutorial.html`
