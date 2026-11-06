# Arachne-KV 0.5.1 —— 使用方 Agent 开发交接简报

> 受众：下游（Hydra 等）负责继续开发的 Agent。
> 目标：在**不读 Arachne 内部实现**的前提下，完成 0.5.1 接入、修复后的验收、
> 以及下一阶段开发。API 细节见 `dev-docs/arachne-kv-0.5-integration-guide.md`；
> 变更记录见 `CHANGELOG.md`。

- 版本：`arachne-kv 0.5.1`
- 提交：`eef45ba`（P0 fix）+ `054285e`（release bump），已 push 到 `origin/main`
- 0.5.0：`98a7ee5`（功能）；文档：`7a9ab43`、`f0d00dc`
- 数据格式：`FORMAT_VERSION = 4`，KV snapshot v3；**0.5.0 → 0.5.1 不再 bump 格式**

---

## 1. 一句话状态

0.5.0 的 `multi_get` / index writes / `wait_applied` 已交付；0.5.1 修复了
**跨主机 `client_id` 冲突导致 `multi_put_with_index` 返回旧 index 并静默丢写**的 P0。
公开 API 不变；下游应升级到 `0.5.1`，保留 read-back guard，并按第 3 节补验收。

---

## 2. 这次到底改了什么

### 0.5.0（已可用）

- `multi_get(keys, Stale | Linearizable)`：整批共享一个 `applied_index`；24k entities
  可分 256–512/批，约 48–96 条命令（不再是 24,000 次 `get_stale`）。
- `put_with_index` / `multi_put_with_index` / `delete_with_index`：返回写入实际生效的
  log index；同 session 重放返回原始 index。
- `wait_applied(index, timeout)`：本地等待状态机追平；无 quorum，不是 barrier。
- 数据目录破坏性升级：v3 → v4；整集群停机 + 备份 + 重播种/迁移。

### 0.5.1（P0 fix，必须升级）

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

## 3. 下游 Agent 现在要做的（按顺序）

### 3.1 升级与保护

- [ ] `Cargo.toml` 改成 `arachne-kv = "0.5.1"`（或精确 `=0.5.1`）。
- [ ] **保留** read-back guard（在 payload-hash/`SessionConflict` 完整性机制交付前
      不要移除）：

```text
final_index = multi_put_with_index(...)
(head, head_index) = get_stale_with_index(ctl_head)

if head != expected_head || head_index < final_index:
    return 503 config_not_published
```

### 3.2 发布 / 物化接入

- [ ] 发布：先写 entities/TOC shards（`multi_put_with_index`），最后在同一原子批次写
      manifest + retain + head；记录 `final_index` 作为树 generation。
- [ ] 物化：
  1. `get_stale_with_index(head)` 拿 `head_index`；
  2. `wait_applied(head_index, MATERIALIZE_WAIT)`；
  3. `multi_get(shard_keys/entity_keys, Stale)`，256–512/批；
  4. 校验 digest、合并、写本地 SQLite。
- [ ] 前提：entity/shard 必须先于 head 写入（entity index < head index）。
      Arachne 只保证按 log 顺序 apply，不解释树/manifest/SQLite。

### 3.3 必须补的验收（3 节点真实集群）

- [ ] 24k entities 物化：读命令数 ≤ ~100；`wait_applied` 后无 missing entity。
- [ ] 写 head 到一个已知 index；kill leader，重选；从旧 leader/旧 follower 发
      `multi_put_with_index`；断言返回 index **大于**旧 head、所有节点最终同一新 head。
- [ ] quorum loss：`put_with_index` / `multi_put_with_index` / `delete_with_index`
      必须失败（`Timeout`/`NotLeader`/`QuorumUnavailable`），**不得** `Ok(index)`。
- [ ] 重启 v4 数据目录：index 不回退；同 session 重放返回原 index。
- [ ] follower 转发：真实 tonic，`multi_put_with_index` 返回 leader 的 index。

### 3.4 下一阶段开发（建议）

- [ ] 把 24k 物化从「每次全量」改为增量：利用 `watch`（0.5.0 已交付）维护 cache；
      或按树 generation 做差分。
- [ ] 在 Hydra 侧记录 `final_index`，发布后异步等待所有 follower `wait_applied`
      再宣告 generation 生效。
- [ ] 为 read-back guard 增加指标/告警（`config_not_published` 计数），观察是否仍有
      `final_index < head_index`；若出现，收集 Arachne 侧 `client_id`/session 日志。

---

## 4. API 速查

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

## 5. 错误/重试

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

## 6. 残余风险（P1，已记录在 CHANGELOG）

- **payload hash / `SessionConflict` 完整性机制**尚未交付；在它之前，read-back guard
  是运行期兜底。
- **复制窗口内崩溃**：写入在本地持久化前崩溃，结果仍可能 unknown（既有 `Timeout` 语义）。
- **单节点磁盘丢失 / 旧目录 re-seed 到活跃集群**：不要这样操作；按 runbook 做破坏性
  升级/替换。
- 10 分钟 soak 中 RSS 14 → 267 MiB（数据/WAL/快照/缓冲；未证明泄漏）；更长 soak 作为
  后续观察项。

---

## 7. 上游验证摘要（loopback，非容量承诺）

- 24k entities：**47 条读命令**（512/批）。
- 真实 tonic 3 节点 24k：每轮 47 条命令，15–38 ms；杀 leader 重选 273–379 ms。
- 10 分钟混合读写 + 故障（无 pause）：**1,393.8 ops/s**、706.7 write ops/s；
  423,928 writes / 836,263 ops；24k/24k 校验；**0 fatal / 0 `Unrecoverable`**。
- 0.5.1 回归：`write_index_client_id` 2/2（snapshot+tail）、`write_index` 8/8、
  `quorum_loss` 1/1、`three_node_client` 4/4、`cargo test --workspace` EXIT 0。

---

## 8. 参考

- 集成规范（完整 API/示例）：`dev-docs/arachne-kv-0.5-integration-guide.md`
- 发布说明：`CHANGELOG.md`
- 升级 runbook：`dev-docs/runbook-format-v4-upgrade.md`
- API reference：`docs/api-reference.html`
- 教程：`docs/tutorial.html`
