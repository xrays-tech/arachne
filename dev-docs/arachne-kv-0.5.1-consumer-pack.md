# Arachne-KV 0.5.1 使用方交接包（单文件）

> 本文档把 0.5.1 交接所需的全部**使用方面文档**合并为一个文件，方便直接复制给下游 Agent。
>
> 已包含：
> 1. 开发交接简报
> 2. 完整集成规范（API / 语义 / 限制 / 示例）
> 3. 发布说明（0.5.1 + 0.5.0）
> 4. 存储格式 v4 升级 runbook
>
> 未包含：内部设计/开发计划（非使用方必需），以及 `docs/` 下的 HTML 站点版本（其内容已由集成规范覆盖）。
> 文中出现的 `dev-docs/...`、`CHANGELOG.md` 路径仅为来源标注；对应内容已在本文件对应章节。

## 目录

1. 开发交接简报
2. 完整集成规范（API / 语义 / 限制 / 示例）
3. 发布说明（0.5.1 + 0.5.0）
4. 存储格式 v4 升级 runbook

---

## 1. 开发交接简报

<!-- source: dev-docs/arachne-kv-0.5.1-handoff-brief.md -->

### Arachne-KV 0.5.1 —— 使用方 Agent 开发交接简报

> 受众：下游（Hydra 等）负责继续开发的 Agent。
> 目标：在**不读 Arachne 内部实现**的前提下，完成 0.5.1 接入、修复后的验收、
> 以及下一阶段开发。API 细节见 `dev-docs/arachne-kv-0.5-integration-guide.md`；
> 变更记录见 `CHANGELOG.md`。

- 版本：`arachne-kv 0.5.1`
- 提交：`eef45ba`（P0 fix）+ `054285e`（release bump），已 push 到 `origin/main`
- 0.5.0：`98a7ee5`（功能）；文档：`7a9ab43`、`f0d00dc`
- 数据格式：`FORMAT_VERSION = 4`，KV snapshot v3；**0.5.0 → 0.5.1 不再 bump 格式**

---

#### 1. 一句话状态

0.5.0 的 `multi_get` / index writes / `wait_applied` 已交付；0.5.1 修复了
**跨主机 `client_id` 冲突导致 `multi_put_with_index` 返回旧 index 并静默丢写**的 P0。
公开 API 不变；下游应升级到 `0.5.1`，保留 read-back guard，并按第 3 节补验收。

---

#### 2. 这次到底改了什么

##### 0.5.0（已可用）

- `multi_get(keys, Stale | Linearizable)`：整批共享一个 `applied_index`；24k entities
  可分 256–512/批，约 48–96 条命令（不再是 24,000 次 `get_stale`）。
- `put_with_index` / `multi_put_with_index` / `delete_with_index`：返回写入实际生效的
  log index；同 session 重放返回原始 index。
- `wait_applied(index, timeout)`：本地等待状态机追平；无 quorum，不是 barrier。
- 数据目录破坏性升级：v3 → v4；整集群停机 + 备份 + 重播种/迁移。

##### 0.5.1（P0 fix，必须升级）

- 根因：`client_id` 原本是 `PID << 32 | counter`。不同 host/container PID 相同
  → 同一个 `client_id`；`seq` 重启从 1 开始 → 新 payload 复用已提交的
  `(client_id, seq_no)` → 状态机 dedup，返回旧 index，新写入被静默丢弃。
- 修复：
  - `client_id` = FNV-1a(NodeId + 进程 counter)，不再用 PID；
  - `Runtime::new` 用「快照 session 表 + 完整 WAL tail」的 `max_session_seq + 1`
    作为 `Handle.seq` 起点，重启不会与本地已持久化的旧 session 重叠；
  - 不 bump 快照格式；0.5.0 目录可原地升级到 0.5.1（旧 PID client 的 session
    变为惰性，GC 后消失；新 client_id 不会命中）。
- 新增回归：同 client_id 跨真实磁盘重启（snapshot + WAL tail 两种路径）、
  3 节点 leader churn、quorum loss indexed writes、真实 tonic 转发 batches。

---

#### 3. 下游 Agent 现在要做的（按顺序）

##### 3.1 升级与保护

- [ ] `Cargo.toml` 改成 `arachne-kv = "0.5.1"`（或精确 `=0.5.1`）。
- [ ] **保留** read-back guard（在 payload-hash/`SessionConflict` 完整性机制交付前
      不要移除）：

```text
final_index = multi_put_with_index(...)
(head, head_index) = get_stale_with_index(ctl_head)

if head != expected_head || head_index < final_index:
    return 503 config_not_published
```

##### 3.2 发布 / 物化接入

- [ ] 发布：先写 entities/TOC shards（`multi_put_with_index`），最后在同一原子批次写
      manifest + retain + head；记录 `final_index` 作为树 generation。
- [ ] 物化：
  1. `get_stale_with_index(head)` 拿 `head_index`；
  2. `wait_applied(head_index, MATERIALIZE_WAIT)`；
  3. `multi_get(shard_keys/entity_keys, Stale)`，256–512/批；
  4. 校验 digest、合并、写本地 SQLite。
- [ ] 前提：entity/shard 必须先于 head 写入（entity index < head index）。
      Arachne 只保证按 log 顺序 apply，不解释树/manifest/SQLite。

##### 3.3 必须补的验收（3 节点真实集群）

- [ ] 24k entities 物化：读命令数 ≤ ~100；`wait_applied` 后无 missing entity。
- [ ] 写 head 到一个已知 index；kill leader，重选；从旧 leader/旧 follower 发
      `multi_put_with_index`；断言返回 index **大于**旧 head、所有节点最终同一新 head。
- [ ] quorum loss：`put_with_index` / `multi_put_with_index` / `delete_with_index`
      必须失败（`Timeout`/`NotLeader`/`QuorumUnavailable`），**不得** `Ok(index)`。
- [ ] 重启 v4 数据目录：index 不回退；同 session 重放返回原 index。
- [ ] follower 转发：真实 tonic，`multi_put_with_index` 返回 leader 的 index。

##### 3.4 下一阶段开发（建议）

- [ ] 把 24k 物化从「每次全量」改为增量：利用 `watch`（0.5.0 已交付）维护 cache；
      或按树 generation 做差分。
- [ ] 在 Hydra 侧记录 `final_index`，发布后异步等待所有 follower `wait_applied`
      再宣告 generation 生效。
- [ ] 为 read-back guard 增加指标/告警（`config_not_published` 计数），观察是否仍有
      `final_index < head_index`；若出现，收集 Arachne 侧 `client_id`/session 日志。

---

#### 4. API 速查

```rust
pub type Index = u64;

pub enum ReadConsistency { Stale, Linearizable }

pub struct MultiGetResult {
    pub values: Vec<Option<Vec<u8>>>, // 同序；None = 缺键
    pub applied_index: Index,
}

pub async fn multi_get(&self, keys: &[&[u8]], consistency: ReadConsistency)
    -> Result<MultiGetResult, ArachneError>;

pub async fn put_with_index(&self, key: &[u8], value: &[u8]) -> Result<Index, ArachneError>;
pub async fn multi_put_with_index(&self, entries: &[(&[u8], &[u8])]) -> Result<Index, ArachneError>;
pub async fn delete_with_index(&self, key: &[u8]) -> Result<Index, ArachneError>;

pub async fn wait_applied(&self, index: Index, timeout: std::time::Duration)
    -> Result<Index, ArachneError>;
```

- `multi_get` 上限：4096 keys / 每 key ≤ `max_key_bytes` / 请求 key 总字节 ≤ 4 MiB /
  返回 value 总字节 ≤ 4 MiB / 在途 16 MiB。超限 `InvalidArgument`，不截断；预算满 `Busy`。
- index 写 dedup：同 `(client_id, seq_no)` 返回原始 index（仅 session TTL+grace 内）；
  GC 后重试是新 session，返回新 index。
- `wait_applied`：本地、无 quorum、不是 barrier；已追平立即返回；超时 `Timeout`；
  `index == 0` no-op；`timeout == 0` 立即 `Timeout`。
- 旧 peer / 缺字段 → `ArachneError::Unsupported`（terminal，未执行，不重试）。

完整签名、分块 helper、重试策略见 `dev-docs/arachne-kv-0.5-integration-guide.md`。

---

#### 5. 错误/重试

| 错误 | 处理 |
|---|---|
| `Timeout` | 结果未知；写重试复用同一 session（幂等） |
| `Busy` | 稍后重试；`multi_get` 可拆批 |
| `QuorumUnavailable` | 重试/换节点；`multi_get(Stale)` / `wait_applied` 仍本地可用 |
| `NotLeader { hint }` | 跟随 hint 重定向 |
| `Unsupported` | **terminal**；升级对端，不重试 |
| `InvalidArgument` | 拆批/修正参数；不会部分执行 |
| `Unrecoverable` | fail-stop |

---

#### 6. 残余风险（P1，已记录在 CHANGELOG）

- **payload hash / `SessionConflict` 完整性机制**尚未交付；在它之前，read-back guard
  是运行期兜底。
- **复制窗口内崩溃**：写入在本地持久化前崩溃，结果仍可能 unknown（既有 `Timeout` 语义）。
- **单节点磁盘丢失 / 旧目录 re-seed 到活跃集群**：不要这样操作；按 runbook 做破坏性
  升级/替换。
- 10 分钟 soak 中 RSS 14 → 267 MiB（数据/WAL/快照/缓冲；未证明泄漏）；更长 soak 作为
  后续观察项。

---

#### 7. 上游验证摘要（loopback，非容量承诺）

- 24k entities：**47 条读命令**（512/批）。
- 真实 tonic 3 节点 24k：每轮 47 条命令，15–38 ms；杀 leader 重选 273–379 ms。
- 10 分钟混合读写 + 故障（无 pause）：**1,393.8 ops/s**、706.7 write ops/s；
  423,928 writes / 836,263 ops；24k/24k 校验；**0 fatal / 0 `Unrecoverable`**。
- 0.5.1 回归：`write_index_client_id` 2/2（snapshot+tail）、`write_index` 8/8、
  `quorum_loss` 1/1、`three_node_client` 4/4、`cargo test --workspace` EXIT 0。

---

#### 8. 参考

- 集成规范（完整 API/示例）：`dev-docs/arachne-kv-0.5-integration-guide.md`
- 发布说明：`CHANGELOG.md`
- 升级 runbook：`dev-docs/runbook-format-v4-upgrade.md`
- API reference：`docs/api-reference.html`
- 教程：`docs/tutorial.html`


---

## 2. 完整集成规范（API / 语义 / 限制 / 示例）

<!-- source: dev-docs/arachne-kv-0.5-integration-guide.md -->

### Arachne-KV 0.5.x 使用方集成规范（Standalone）

> 本文档是**独立**的：下游 Agent 只读本文件即可完成 `arachne-kv` 0.5.x 的接入、
> 升级与新能力使用，不需要读 Arachne 内部设计文档。
>
> 版本：`arachne-kv 0.5.0`（2026-11-06）
> 适用：Rust 下游（Hydra 等），单节点 facade 或 `Runtime` + `Handle` 多节点嵌入。
> 关联发布说明：`CHANGELOG.md`；升级 runbook：`dev-docs/runbook-format-v4-upgrade.md`。

---

#### 0. Agent 执行摘要（先读这一节）

你要接入的新能力只有三组：

1. **`multi_get`**：精确 key 集合的批量读，整批共享一个 `applied_index`。
2. **index-returning writes**：`put_with_index` / `multi_put_with_index` / `delete_with_index`，
   返回写入实际生效的 log index。
3. **`wait_applied`**：本地等待状态机 apply 到指定 index（无 quorum，不是 barrier）。

必须遵守的 6 条规则：

1. **不要**用 24,000 次 `get_stale` 物化大配置树；用 `multi_get(Stale)` 分批（256–512/批）。
2. 物化时先 `get_stale_with_index(head)` 拿 `head_index`，再 `wait_applied(head_index)`，
   最后 `multi_get(entities, Stale)`；不要靠轮询或缺失重试。
3. 发布时用 `multi_put_with_index` 的返回 index 作为树 generation；不要用本地猜测。
4. 不要把 entity index 与 head index 混比成「整树版本」；**head index 是树世代**，
   entity index 只是单 key 的来源/排序信息。
5. 旧 peer / 缺字段 → `ArachneError::Unsupported`；**terminal，不重试、不静默降级**。
6. 数据目录升级是**破坏性**的：整集群停机 → 备份 → 重播种/迁移；不支持原地滚动。

验收目标（下游自测）：
- 24k entities 物化 ≤ ~100 条读命令（256–512/批）；
- `wait_applied(head_index)` 后无 missing entity；
- 杀 leader/重选后 index 可比、已确认写入不丢；
- minority follower 上 `multi_get(Stale)` 与 `wait_applied` 仍可用。

---

#### 1. 依赖与启动

##### 1.1 Cargo

```toml
### 默认：带 tonic transport
arachne-kv = "0.5"

### 或 lean core（零外部依赖、无 transport）
### arachne-kv = { version = "0.5", default-features = false }
```

要求：Rust `1.93+`，edition 2024。

##### 1.2 单节点 facade（最简单）

```rust
use arachne_kv::server::{Arachne, ClusterConfig, WalConfig};
use arachne_kv::{ArachneError, ReadConsistency};
use std::time::Duration;

let _server = Arachne::start(ClusterConfig::single_node(
    1,                 // 1-based raft id
    "./arachne-data",  // data dir
    WalConfig::default(),
))?;

// 既有 API 不变
Arachne::set(b"k", b"v").await?;
let v = Arachne::get(b"k").await?;

// 新 API
let r = Arachne::multi_get(&[&b"k"[..], &b"missing"[..]], ReadConsistency::Stale).await?;
assert_eq!(r.values, vec![Some(b"v".to_vec()), None]);

let idx = Arachne::put_with_index(b"k2", b"v2").await?;
let applied = Arachne::wait_applied(idx, Duration::from_secs(2)).await?;
assert!(applied >= idx);
```

##### 1.3 多节点 `Runtime` + `Handle`

多节点用 `ClusterConfig::member(...)`（见 `README.md` / `arachne-node/src/node.rs`），
或直接用 `Runtime::new(...)` 得到 `Handle`。所有新方法都在 `Handle` 上：

```rust
let handle: arachne_kv::client::Handle = /* from Runtime or Arachne::handle() */;
```

`Handle` 是廉价 `Clone`，可跨 task 共享。

---

#### 2. API 契约

##### 2.1 公共类型

```rust
pub type Index = u64; // = LogIndex

pub enum ReadConsistency {
    Stale,        // 本地状态机；无 quorum；少数派 follower 可用；可能陈旧
    Linearizable, // 既有 ReadIndex；leader + quorum
}

pub struct MultiGetResult {
    pub values: Vec<Option<Vec<u8>>>, // 与输入 keys 同序；None = 缺键
    pub applied_index: Index,          // 整批 values 被观察时的单一 applied index
}
```

##### 2.2 `multi_get`

```rust
pub async fn multi_get(
    &self,
    keys: &[&[u8]],
    consistency: ReadConsistency,
) -> Result<MultiGetResult, ArachneError>;
```

语义：
- 整批 values 在**同一次读锁**下取得；`applied_index` 与 values 同源，**无撕裂**；
- 输入顺序严格保留；重复 key 产生重复结果；缺键 = `None`，不是错误；空 `keys` 合法；
- `Stale`：本地、无 quorum、少数派 follower 可用；
- `Linearizable`：leader + ReadIndex quorum；非 leader 会 `NotLeader` 重定向；
- 上限（**不截断**，超限 `InvalidArgument`）：
  - `keys.len() <= 4096`
  - 每个 key `<= max_key_bytes`（默认 4 KiB）
  - 请求 key 总字节 `<= 4 MiB`
  - 返回 value 总字节 `<= 4 MiB`
  - 在途 payload 共享 `16 MiB` 预算，超限 `Busy`（可重试）

##### 2.3 index-returning writes

```rust
pub async fn put_with_index(&self, key: &[u8], value: &[u8]) -> Result<Index, ArachneError>;
pub async fn multi_put_with_index(&self, entries: &[(&[u8], &[u8])]) -> Result<Index, ArachneError>;
pub async fn delete_with_index(&self, key: &[u8]) -> Result<Index, ArachneError>;
```

语义：
- 返回**使写入实际生效**的 log entry index；
- `multi_put_with_index` 整批共享一个 index（一次原子 apply）；
- `delete_with_index` 即使 key 不存在也返回该 delete entry 的 index；
- 同 `(client_id, seq_no)` 重放返回**原始 index**；该保证仅在 session TTL + grace 内，
  GC 后的重试是新 session，返回新 index；
- follower 转发写入返回 **leader** 的 index；
- 旧 peer 不支持 → `Unsupported`（terminal，操作未执行）。

##### 2.4 `wait_applied`

```rust
pub async fn wait_applied(
    &self,
    index: Index,
    timeout: std::time::Duration,
) -> Result<Index, ArachneError>;
```

语义：
- 已追平（`applied >= index`）立即返回当前 applied（可能 ≥ index）；
- 否则在 apply 推进时事件唤醒（无忙轮询）；
- 纯本地：不 redirect、不要求 quorum，**少数派 follower 可用**；
- 超时 `Timeout`；`index == 0` no-op；`timeout == 0` 立即 `Timeout`；
- 取消安全（drop future 不泄漏 waiter）；
- **不是 quorum barrier**，不能替代线性读。

##### 2.5 新增错误

```rust
ArachneError::Unsupported(String)
```

- 对端不支持请求能力（旧 peer、wire `unknown_kind`、缺字段）；
- **terminal**：操作未执行，对同一 peer 重试无益；
- `ArachneError` 现为 **17 个变体**；穷尽 `match` 的下游需要新增分支。

---

#### 3. 推荐集成配方

##### 3.1 发布（Hydra 形状）

```rust
// 1) 先写 entities / TOC shards（content-addressed，幂等）
let entity_index = handle
    .multi_put_with_index(&entity_chunk)
    .await?;

// 2) 最后写 manifest + retain + head（同一原子批次）
let final_index = handle
    .multi_put_with_index(&[
        (manifest_key, manifest_bytes),
        (retain_key, retain_bytes),
        (head_key, head_token),
    ])
    .await?;

// final_index = 整棵树 generation；记录下来
```

关键：**entity/shard 必须先于 head 写入**，因此 entity index < `head_index`。

##### 3.2 物化（关闭 head 可见 / entity 缺失竞态）

```rust
use arachne_kv::ReadConsistency;

// 应用层：None = head 尚不可见 / 空树；按你的语义处理。
// 不要用 ArachneError::Unsupported 表示“缺键”（它专指旧 peer 能力不足）。
let (head_token, head_index) = handle
    .get_stale_with_index(head_key)
    .await?
    .expect("application-level: handle head absence before materializing");

// 等本地状态机真正 apply 到 head index
handle
    .wait_applied(head_index, MATERIALIZE_WAIT)
    .await?;

// head 已 apply ⇒ 更早写入的 entity/shard 也已 apply
let manifest = handle.multi_get(&[manifest_key], ReadConsistency::Stale).await?;
let shards = handle.multi_get(&shard_keys, ReadConsistency::Stale).await?;
let entities = handle.multi_get(&entity_keys, ReadConsistency::Stale).await?;

// 校验 digest、合并、写入本地 SQLite
```

分批建议：`multi_get` 每批 **256–512** 个 entity；24k entities ≈ 48–96 条命令。

##### 3.3 分块 helper（可直接复制）

```rust
use arachne_kv::{ArachneError, Handle, ReadConsistency};

async fn multi_get_chunked(
    handle: &Handle,
    keys: &[Vec<u8>],
    batch: usize,
) -> Result<Vec<Option<Vec<u8>>>, ArachneError> {
    assert!(batch > 0 && batch <= 4096);
    let mut out = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(batch) {
        let refs: Vec<&[u8]> = chunk.iter().map(|k| k.as_slice()).collect();
        let r = handle.multi_get(&refs, ReadConsistency::Stale).await?;
        out.extend(r.values);
    }
    Ok(out)
}
```

##### 3.4 重试策略

```rust
use arachne_kv::ArachneError;
use std::time::Duration;

fn is_retryable(e: &ArachneError) -> bool {
    matches!(
        e,
        ArachneError::Timeout
            | ArachneError::Busy
            | ArachneError::QuorumUnavailable
            | ArachneError::NotLeader { .. }
    )
}

// Timeout = 结果未知；写重试应复用同一 (client_id, seq_no) session（幂等）。
// Unsupported = terminal，不要重试。
```

---

#### 4. 升级 / 兼容性

##### 4.1 API

- 既有 `put` / `get` / `get_stale` / `get_stale_with_index` / `multi_put` / `delete` /
  `cas` / `watch` / `get_stale_prefix` / `get_stale_range` 签名与语义**不变**；
- 新 API additive；crate `0.4.0 → 0.5.0`。

##### 4.2 数据目录（破坏性，必须停机）

- KV 快照 `2 → 3`：session 记录新增 effective index；
- storage `FORMAT_VERSION 3 → 4`；
- **无新增命令 opcode**；破坏性来自 session 快照格式 + 升级策略；
- 旧 v3 目录在 0.5.0 启动时 fail-stop（格式不匹配）；
- 不允许原地滚动；不允许新 leader 向旧节点发 v3 快照；
- 推荐：整集群停机 → 备份旧目录 → 重播种/迁移 → 启动 0.5.0；
- 回滚 = 恢复备份 + 旧二进制。

完整步骤见 `dev-docs/runbook-format-v4-upgrade.md`。

##### 4.3 线协议

- `PROTOCOL_MINOR` 保持 0；
- `Forward` kind：`3 = multi_get`，`4 = ProposeWithIndex`；
- 旧 peer：未知 kind 在执行前返回 `unknown_kind`；index 写省略 `commit_index`；
- 新客户端统一映射为 `Unsupported`，不静默回退；
- `multi_get` 的 `Some(vec![])`（present empty）在 wire 上可区分于 `None`。

---

#### 5. 下游验收清单

在真实 3 节点集群上逐项验证：

- [ ] 24k entities 分批 `multi_get(Stale)`：批大小 256–512，读命令数 ≤ ~100；
- [ ] `get_stale_with_index(head)` → `wait_applied(head_index)` → `multi_get`：
      无 missing entity、无 torn batch；
- [ ] `put_with_index` / `multi_put_with_index`：返回 index 单调可用；
      `multi_put_with_index` 整批同 index；
- [ ] 同 session 重放返回原始 index（TTL+grace 内）；
- [ ] follower 写 → leader index；另一 follower `wait_applied` 追平；
- [ ] 杀 leader、重选后 index 仍可比，已确认写入不丢；
- [ ] minority follower：`multi_get(Stale)` 与 `wait_applied` 可用；
      `wait_applied` 超时而非挂起；
- [ ] 既有 `watch` / `get_stale_range` 行为不变；
- [ ] 旧 peer 的 `multi_get` / index 写返回 `Unsupported`，无静默降级。

可参考 Arachne 仓内压测（`#[ignore]`）：

```bash
cargo test -p arachne-kv --test batch_read_load -- --ignored --nocapture
cargo test -p arachne-kv-transport-tonic --test stress_24k_tonic -- --ignored --nocapture
cargo test -p arachne-kv --test stress_24k_partition -- --ignored --nocapture
cargo test -p arachne-kv-transport-tonic --test stress_10min_mixed -- --ignored --nocapture
### 快速 smoke：ARACHNE_STRESS_SECS=30 ...
```

---

#### 6. 反模式（不要这样做）

1. **不要** 24k 次 `get_stale`；用 `multi_get(Stale)` 分批。
2. **不要**用 `get_stale` 的单次 `None` 断言 key 不存在（非单调）。
3. **不要**用 entity index 与 head index 混比成「整树版本」；head index 才是世代。
4. **不要**轮询 `wait_applied`；它是事件驱动的本地等待。
5. **不要**把 `Unsupported` 当可重试；它表示操作未执行、对端能力不足。
6. **不要**在混合版本上触发/传输快照；数据目录升级必须整集群停机。
7. **不要**超过 `multi_get` 限制后自行截断；超限会 `InvalidArgument`，请拆批。
8. **不要**把 `Timeout` 当失败；结果未知，写重试应复用同一 session。

---

#### 7. 性能与已知限制

10 分钟真实 tonic 3 节点、混合读写、无 pause、40%/70% 杀 follower/leader 的本机
loopback 压测（**非容量承诺**）：

- 1,393.8 ops/s；706.7 write ops/s；423,928 writes / 836,263 ops；
- 24k/24k acknowledged keys + final entities 校验；
- 0 fatal / 0 `Unrecoverable` / 0 unexpected `ShuttingDown`；
- survivor applied index 53,124；
- p50/p99：write 15.5/66.1 ms；get 10.3/57.2 ms；get_stale 53 µs/7.3 ms；
  multi_get 315 µs/7.5 ms；wait_applied 39 µs/4.8 ms。

已知限制：
- 本发布未包含多小时 soak、`docker/bench` 1w/4w/8w A/B、bounded fuzz 实跑；
- Hydra 下游 4000 users + 4000 roles + 16000 API keys 的端到端验收由下游负责；
- 10 分钟 soak 中 RSS 14 MiB → 267 MiB（数据/WAL/快照/测试缓冲；未证明泄漏）；
- 内部测试加固 follow-up：3 节点快照恢复的 session index 传播、forwarded `multi_get`
  actor 侧限额复核、budget/waiter 饱和测试、`Many` 读 INV14 l2、ConfChange-index wait、
  写负载下 deadline 精度、wait + fail-stop。

---

#### 8. 参考

- 发布说明：`CHANGELOG.md`
- 设计：`dev-docs/arachne-kv-batch-read-applied-index-design.md`
- 开发计划：`dev-docs/plan-batch-read-applied-index.md`
- 升级 runbook：`dev-docs/runbook-format-v4-upgrade.md`
- API reference：`docs/api-reference.html`
- 教程：`docs/tutorial.html`


---

## 3. 发布说明（0.5.1 + 0.5.0）

<!-- source: CHANGELOG.md -->

### Changelog / 变更说明

面向 `arachne-kv` 使用方的发布说明。内部设计/取舍见
`dev-docs/arachne-kv-batch-read-applied-index-design.md`，升级步骤见
`dev-docs/runbook-format-v4-upgrade.md`。

---

#### 0.5.1 — 2026-11-06

主题：**P0 修复——写入 index 回退 / 静默丢写（跨主机 `client_id` 冲突）**。

##### P0 修复

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

##### 残余风险（P1）

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

#### 0.5.0 — 2026-11-06

主题：**批量精确读 + 写入/应用索引可见性 + 本地 applied-index 等待**。
对应需求：上游 Hydra 24k 实体配置树的「head 可见但 entity 尚未 apply」竞态。

##### 一句话总结

- 24k 个 entity 的物化从 **24,000 次单 key 读** 降到 **~47 次 `multi_get`**；
- 写入可以拿到**实际生效的 log index**，重放返回原始 index；
- follower 可以**本地等待**状态机追平它刚观察到的 head index，不再靠重试/轮询。

---

#### 1. 新增 API

##### 1.1 `multi_get` —— 精确 key 集合的批量读

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

##### 1.2 写入返回 index

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

##### 1.3 `wait_applied` —— 本地 applied-index 等待

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

##### 1.4 新增错误变体

```rust
ArachneError::Unsupported(String)
```

- 对端不支持所请求的能力（旧 peer 的 `multi_get` / indexed write、wire `unknown_kind` 或缺字段）；
- **terminal**：操作未执行；对同一 peer 重试同一调用无益；
- `ArachneError` 现为 **17 个变体**；对 `ArachneError` 做穷尽 `match` 的下游代码需要新增一个分支。

---

#### 2. 推荐用法（Hydra 形状）

##### 2.1 发布

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

##### 2.2 物化（关闭「head 可见、entity 缺失」竞态）

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

#### 3. 兼容性

##### 3.1 API

- 既有 `put` / `get` / `get_stale` / `get_stale_with_index` / `multi_put` / `delete` /
  `cas` / `watch` / `get_stale_prefix` / `get_stale_range` 的签名与语义**不变**；
- 新 API 全部为 additive；
- crate 版本 `0.4.0 → 0.5.0`。

##### 3.2 数据目录（破坏性）

- KV 快照版本 `2 → 3`：每条 session 记录新增 8 字节 effective index；
- storage `FORMAT_VERSION 3 → 4`；
- **没有新增命令 opcode**；WAL 命令流对 0.3.x 二进制仍可解析，破坏性来自 **session 快照格式 + 升级策略**；
- 旧 v3 数据目录在 0.5.0 启动打开时**直接 fail-stop**（格式不匹配）；
- 不允许「v3 目录原地升 v4」；不允许新 leader 向旧节点发送 v3 快照；
- 推荐：**整集群停机 → 备份旧目录 → 重播种/迁移 → 启动 0.5.0**；
- 回滚 = 恢复升级前整目录备份 + 旧二进制。

完整步骤：`dev-docs/runbook-format-v4-upgrade.md`。

##### 3.3 线协议

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

#### 4. 错误与重试

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

#### 5. 性能与验证摘要

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
### 快速 smoke：ARACHNE_STRESS_SECS=30 ...
```

---

#### 6. 已知限制 / 后续

- 本发布**未**包含：多小时 soak、`docker/bench` 1w/4w/8w 吞吐 A/B、bounded fuzz 实跑、
  Hydra 下游 4000 users + 4000 roles + 16000 API keys 的端到端验收（下游负责）。
- 10 分钟 soak 中 RSS 从 14 MiB 增至 267 MiB；主要是 24k 数据、持续写入的 WAL/快照、
  session 表与测试缓冲，**未证明是泄漏**；更长 soak 的内存曲线建议作为后续观察项。
- 内部测试加固 follow-up：3 节点快照恢复的 session index 传播、forwarded `multi_get`
  actor 侧限额复核、budget/waiter 饱和测试、`Many` 读的 INV14 l2 场景、ConfChange-index
  wait、写负载下 deadline 精度、wait + fail-stop。

---

#### 7. 参考

- 设计：`dev-docs/arachne-kv-batch-read-applied-index-design.md`
- 开发计划：`dev-docs/plan-batch-read-applied-index.md`
- 升级 runbook：`dev-docs/runbook-format-v4-upgrade.md`
- API reference：`docs/api-reference.html`
- 教程：`docs/tutorial.html`


---

## 4. 存储格式 v4 升级 runbook

<!-- source: dev-docs/runbook-format-v4-upgrade.md -->

### Runbook：存储格式 v3 → v4 破坏性升级（`FORMAT_VERSION` 3→4）

> 适用版本：升级到 **arachne-kv 0.5.x（Phase 2：写入返回 index）**（含 `KV_SNAPSHOT_VERSION` 3）。
> 关联：`dev-docs/arachne-kv-batch-read-applied-index-design.md`（§7/§11 Gate-1 Q1/Q2）；`dev-docs/plan-batch-read-applied-index.md`（Phase 2）；`dev-docs/runbook-format-v3-upgrade.md`（上一版，重播种路径同款）。
> 一句话：**本次没有新 opcode；破坏性来自 KV session 快照格式 + 升级策略。旧数据目录在新二进制启动打开时即 fail-stop，推荐整集群停机 + 重播种/迁移；任何情况下新 leader 都不得把 v3 快照发给旧节点。**

---

#### 1. 这是什么升级

| 项 | 说明 |
|---|---|
| 变更 | 存储 `FORMAT_VERSION` 3 → 4（`arachne/src/storage/meta.rs`）；KV 快照 `KV_SNAPSHOT_VERSION` 2 → 3（每条 session 记录在原有 outcome 编码后追加 8 字节 LE **effective index**，`arachne/src/state_machine/kv.rs`） |
| 为什么破坏 | `WalStorage::open` 校验 `META.format_version`，不一致即 **fail-stop**（`arachne/src/storage/wal.rs`）；session 快照缺少 index 时无法安全恢复（绝不能把缺失 index 迁移成 0） |
| **不**含什么 | **没有新增命令 opcode**。WAL 中的 `Put`/`Delete`/`MultiPut`/`Cas`/`SessionGc` 命令流对 v3 二进制仍可解析；v4 新写的 session 快照才需要新二进制 |
| 何时生效 | 新 v4 二进制打开 v3 数据目录 → 启动即拒绝；旧 v3 节点收到 v4（`KV_SNAPSHOT_VERSION=3`）快照 → 恢复失败 / fail-stop |

##### 1.1 与 wire 的关系

`ProposeWithIndex` 使用独立的转发 kind（不复用普通 `Propose`），旧 leader 在执行前返回 `unknown_kind`，新客户端映射为 `Unsupported`（“未执行”）。这是 wire 兼容门控，不是存储格式的一部分；但滚动期间仍应把「新 leader 不得给旧节点发 v3 快照」作为硬约束（见 §2）。

---

#### 2. 硬约束（必须先理解，再操作）

1. **旧数据目录 = 打开即 fail-stop**：不存在「v3 目录原地升 v4」。新二进制在 `WalStorage::open` 读到 `format_version=3` 会直接报格式不匹配。
2. **session index 不可伪造**：v2（`KV_SNAPSHOT_VERSION=2`）快照没有 per-session effective index。恢复时拒绝（`MalformedSnapshot`），绝不迁移成 `index 0`——否则 `0` 会作为合法值流入 `put_with_index`/`delete_with_index` 的公开返回并被新快照永久传播。
3. **禁止新 leader 向旧节点发送 v3 快照**：新旧快照格式无能力协商。滚动窗口中一次跨版本快照传输就会让旧节点 fail-stop。**默认按 §3 整集群停机。**
4. **回滚 = 整目录回退**：一旦 v4 打开并写过数据目录，不能把该目录交回 v3 二进制（`FORMAT_VERSION` 拒绝）。回滚必须恢复升级前的整目录备份 + 旧二进制。

---

#### 3. 推荐路径（整集群停机重播种/迁移）

> 当前 0.5.x 发布推荐此路径；适用于可接受一次性迁移成本的部署。

1. **停机**：停止全部 `arachne-kv` 进程（写入/读取全部停止）。
2. **保留旧数据**（回滚/审计用）：原 `data_dir` 整体备份（含 `META`、`segments`、`snapshot-*.snap`）。**不要删除**直到新集群验证通过。
3. **重播种或迁移**：
   a. 为新 v4 节点准备全新 `data_dir`（空目录）。
   b. 如需保留业务数据：在旧集群/环境中**导出**数据（经 `get`/`get_stale`/`get_stale_with_index` 逐 key 读出），再在 v4 集群**重新写入**（`put`/`multi_put`/`put_with_index`）。**不要**直接复制 v3 目录或快照文件。
   c. 数据写入后，新格式快照会在 `snapshot_threshold` 触发时自然生成；建议重播种后手动验证一次快照生成（见 §5）。
4. **校验**（新集群）：见 §5。
5. **回滚预案**：若 v4 校验不通过，恢复第 2 步备份的旧目录 + 旧二进制（v3）即可回到上一版本（旧目录从未被 v4 打开过则完整无损）。

---

#### 4. 滚动替代路径（不支持；仅单节点/全控场景）

v4 与前版**无快照能力协商**（session 快照格式变更）。滚动升级窗口内任何一次新 leader 生成/传输 v3 快照，都会让旧节点恢复失败。**不推荐**；仅当单节点、且能保证旧二进制永远不会读取 v4 产物时才考虑，且必须视为一次性迁移而非长驻混合拓扑。

若必须短暂共存：

1. 所有节点先运行旧版本并**全部追平**；
2. 升级期间禁止触发/传输快照（无法真正保证，风险自负）；
3. 新 leader 只在其能确认所有 follower 都是 v4 后才允许写新格式快照；
4. 一旦出现跨版本快照传输，立即按 §3 停机。

---

#### 5. 升级后校验（用新原语验证语义）

在新 v4 集群上：

1. **基础读写**：`put` 后 `get` / `get_stale` 正常；`get_stale_with_index` 返回 `Ok(Some((v, i)))` 且 `i >= 1`。
2. **index-returning 写**：
   - `put_with_index(k, v)` 返回的 index == 随后 `get_stale_with_index(k)` 的 origin index；
   - `multi_put_with_index` 整批共享同一 index，且每个 key 的 origin 都等于它；
   - `delete_with_index` 返回 delete entry 的 index；之后的 put 返回更大的 index。
3. **session dedup**：同一 `(client_id, seq_no)` 重放返回**原始** index（TTL+grace 内），且不重复应用新 payload。
4. **快照**：确认新快照以 `format_version == 4` 产出，`KV_SNAPSHOT_VERSION == 3`；从快照恢复后第 2、3 项仍成立（session 的原始 index 跨重启保留）。
5. **旧快照拒绝**：构造/保留的 v2 快照恢复应 fail-stop（`MalformedSnapshot`），公开 API 永不出现 index 0。
6. **multi-put 原子**：一次 `multi_put` 后各 key 的 `get_stale_with_index` 报同一 index；3 节点追平后各节点 index 一致。

---

#### 6. 运维要点

- **不要**把 v4 数据目录交给旧二进制打开（`FORMAT_VERSION` 拒绝）——回滚必须整目录回退（§3.5），不能「新目录反降」。
- **不要**尝试只回退 `KV_SNAPSHOT_VERSION` 或手改 `META` 版本：session index 缺失无法重建。
- 快照体积：每条 session 记录增加 8 字节（effective index），`snapshot_threshold` 触发点基本不变——属预期内，告警阈值无需调整。
- 会话 TTL：session 记录同时承载 outcome 与 index；GC 语义不变（GC 后的重试是新 session，返回新 index，属标准 exactly-once 边界）。
- wire：`ProposeWithIndex` 的 `unknown_kind → Unsupported` 只表示「旧 peer 未执行」；不要把它当作「写入已成功」或伪造 index。

---

#### 7. 相关文档

- 设计：`dev-docs/arachne-kv-batch-read-applied-index-design.md`（§4.2/§5.3/§7/§11）；`dev-docs/arachne-kv-commit-index-design.md`（§9.1：禁止 post-apply 回查）
- 计划：`dev-docs/plan-batch-read-applied-index.md`（Phase 2）
- 上一版：`dev-docs/runbook-format-v3-upgrade.md`（整集群停机重播种路径同款）
- 格式账：`dev-docs/plan-vs-etcd.md`（§2.2 格式打包策略）
- 测试：`arachne/tests/write_index.rs`、`arachne/tests/sessions.rs`、`arachne/tests/write_outcome.rs`
