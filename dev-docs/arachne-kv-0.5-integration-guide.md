# Arachne-KV 0.5.x 使用方集成规范（Standalone）

> 本文档是**独立**的：下游 Agent 只读本文件即可完成 `arachne-kv` 0.5.x 的接入、
> 升级与新能力使用，不需要读 Arachne 内部设计文档。
>
> 版本：`arachne-kv 0.5.0`（2026-11-06）
> 适用：Rust 下游（Hydra 等），单节点 facade 或 `Runtime` + `Handle` 多节点嵌入。
> 关联发布说明：`CHANGELOG.md`；升级 runbook：`dev-docs/runbook-format-v4-upgrade.md`。

---

## 0. Agent 执行摘要（先读这一节）

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

## 1. 依赖与启动

### 1.1 Cargo

```toml
# 默认：带 tonic transport
arachne-kv = "0.5"

# 或 lean core（零外部依赖、无 transport）
# arachne-kv = { version = "0.5", default-features = false }
```

要求：Rust `1.93+`，edition 2024。

### 1.2 单节点 facade（最简单）

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

### 1.3 多节点 `Runtime` + `Handle`

多节点用 `ClusterConfig::member(...)`（见 `README.md` / `arachne-node/src/node.rs`），
或直接用 `Runtime::new(...)` 得到 `Handle`。所有新方法都在 `Handle` 上：

```rust
let handle: arachne_kv::client::Handle = /* from Runtime or Arachne::handle() */;
```

`Handle` 是廉价 `Clone`，可跨 task 共享。

---

## 2. API 契约

### 2.1 公共类型

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

### 2.2 `multi_get`

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

### 2.3 index-returning writes

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

### 2.4 `wait_applied`

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

### 2.5 新增错误

```rust
ArachneError::Unsupported(String)
```

- 对端不支持请求能力（旧 peer、wire `unknown_kind`、缺字段）；
- **terminal**：操作未执行，对同一 peer 重试无益；
- `ArachneError` 现为 **17 个变体**；穷尽 `match` 的下游需要新增分支。

---

## 3. 推荐集成配方

### 3.1 发布（Hydra 形状）

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

### 3.2 物化（关闭 head 可见 / entity 缺失竞态）

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

### 3.3 分块 helper（可直接复制）

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

### 3.4 重试策略

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

## 4. 升级 / 兼容性

### 4.1 API

- 既有 `put` / `get` / `get_stale` / `get_stale_with_index` / `multi_put` / `delete` /
  `cas` / `watch` / `get_stale_prefix` / `get_stale_range` 签名与语义**不变**；
- 新 API additive；crate `0.4.0 → 0.5.0`。

### 4.2 数据目录（破坏性，必须停机）

- KV 快照 `2 → 3`：session 记录新增 effective index；
- storage `FORMAT_VERSION 3 → 4`；
- **无新增命令 opcode**；破坏性来自 session 快照格式 + 升级策略；
- 旧 v3 目录在 0.5.0 启动时 fail-stop（格式不匹配）；
- 不允许原地滚动；不允许新 leader 向旧节点发 v3 快照；
- 推荐：整集群停机 → 备份旧目录 → 重播种/迁移 → 启动 0.5.0；
- 回滚 = 恢复备份 + 旧二进制。

完整步骤见 `dev-docs/runbook-format-v4-upgrade.md`。

### 4.3 线协议

- `PROTOCOL_MINOR` 保持 0；
- `Forward` kind：`3 = multi_get`，`4 = ProposeWithIndex`；
- 旧 peer：未知 kind 在执行前返回 `unknown_kind`；index 写省略 `commit_index`；
- 新客户端统一映射为 `Unsupported`，不静默回退；
- `multi_get` 的 `Some(vec![])`（present empty）在 wire 上可区分于 `None`。

---

## 5. 下游验收清单

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
# 快速 smoke：ARACHNE_STRESS_SECS=30 ...
```

---

## 6. 反模式（不要这样做）

1. **不要** 24k 次 `get_stale`；用 `multi_get(Stale)` 分批。
2. **不要**用 `get_stale` 的单次 `None` 断言 key 不存在（非单调）。
3. **不要**用 entity index 与 head index 混比成「整树版本」；head index 才是世代。
4. **不要**轮询 `wait_applied`；它是事件驱动的本地等待。
5. **不要**把 `Unsupported` 当可重试；它表示操作未执行、对端能力不足。
6. **不要**在混合版本上触发/传输快照；数据目录升级必须整集群停机。
7. **不要**超过 `multi_get` 限制后自行截断；超限会 `InvalidArgument`，请拆批。
8. **不要**把 `Timeout` 当失败；结果未知，写重试应复用同一 session。

---

## 7. 性能与已知限制

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

## 8. 参考

- 发布说明：`CHANGELOG.md`
- 设计：`dev-docs/arachne-kv-batch-read-applied-index-design.md`
- 开发计划：`dev-docs/plan-batch-read-applied-index.md`
- 升级 runbook：`dev-docs/runbook-format-v4-upgrade.md`
- API reference：`docs/api-reference.html`
- 教程：`docs/tutorial.html`
