# Runbook：存储格式 v4 → v5 破坏性升级（`FORMAT_VERSION` 4→5）

> 适用版本：升级到 **arachne-kv 0.6.0（显式 `SessionConflict`）**（含 `KV_SNAPSHOT_VERSION` 4）。
> 关联：`dev-docs/runbook-format-v4-upgrade.md`（上一版，重播种路径同款）；会话冲突语义见 propsol §2.3 G1（v0.6.0 细化）。
> 一句话：**本次没有新 opcode；破坏性来自 KV session 快照格式（每条 session 记录追加 8 字节 LE 载荷摘要 digest）。旧数据目录在新二进制打开时即 fail-stop；推荐整集群停机 + 重播种/迁移；任何情况下新 leader 都不得把 v4 快照发给旧节点。**

---

## 1. 这是什么升级

| 项 | 说明 |
|---|---|
| 变更 | 存储 `FORMAT_VERSION` 4 → 5（`arachne/src/storage/meta.rs`）；KV 快照 `KV_SNAPSHOT_VERSION` 3 → 4（每条 session 记录在 effective index 之后追加 8 字节 LE **digest**，`arachne/src/state_machine/kv.rs`） |
| 为什么破坏 | `WalStorage::open` 校验 `META.format_version`，不一致即 **fail-stop**（`arachne/src/storage/wal.rs`）；session 快照缺少 digest 会让「快照恢复的节点」与「日志重放的节点」对后续 `SessionConflict` 判定**分歧** |
| 为什么 digest 必须入快照 | 冲突判定 = `f(入站命令, 已存 digest)` 的纯函数。digest 不进快照，重建节点就无从判定同一 session 的载荷是否相同 |
| **不**含什么 | **没有新增命令 opcode**。WAL 命令流仍是 `Put`/`Delete`/`MultiPut`/`Cas`/`SessionGc`；v5 新写的 session 快照才需要新二进制 |
| 何时生效 | 新 v5 二进制打开 v4 数据目录 → 启动即拒绝；旧 v4 节点收到 v5（`KV_SNAPSHOT_VERSION=4`）快照 → 恢复失败 / fail-stop |

### 1.1 与 wire 的关系

`SessionConflict` 在**同进程**内保留结构化错误；跨进程（Forward RPC）只传稳定错误码 `"session_conflict"`（有损透传，`(client_id, seq_no)` 诊断字段不跨线，落地为 `0/0`）。旧客户端收到该码 → 映射为 `Unrecoverable(...)`（terminal，可接受）。滚动期间仍应把「新 leader 不得给旧节点发 v4 快照」作为硬约束（见 §2）。

---

## 2. 硬约束（先理解再操作）

1. **旧数据目录 = 打开即 fail-stop**：v5 二进制对 v4 META 直接报错；不存在原地无损升级。
2. **禁止新 leader 在滚动期间把 v5 快照发给旧节点**：旧节点无法解析，且新旧格式无能力协商。
3. **wire 不受影响于存储格式**：`SessionConflict` 只是新增一个稳定错误码，不改 `Hello` 握手。

## 3. 推荐路径（整集群停机重播种）

1. 停机全部进程；整体备份旧 `data_dir`（含 `META`/`segments`/`snapshot-*.snap`）。
2. 为新 v0.6.0 节点准备全新空 `data_dir`；如需保留数据，在旧环境导出（`get`/`get_stale`）后在新集群重新写入（`put` / `multi_put`）。**不要**直接复制 v4 目录或快照。
3. 新格式快照在 `snapshot_threshold` 触发时自然生成；建议重播种后手动验证一次快照生成。
4. 回滚预案：恢复备份的旧目录 + 旧二进制（旧目录从未被 v0.6.0 打开过则完整无损）。

## 4. 升级后校验

1. 基础读写 `put`/`get`/`get_stale`/`get_stale_with_index` 正常，`present ⇒ index ≥ 1`。
2. **会话冲突**：同一 `(client_id, seq_no)` 用**不同载荷**重放 → `SessionConflict`（不改变状态）；同载荷重放 → 去重返回原结果。
3. 快照生成后重启/新节点加入：上述第 2 条在**从快照恢复**的节点上仍成立（证明 digest 随快照持久化）。
4. 确认新快照 `format_version == 5`。

## 5. 运维要点

- 不要把 v0.6.0 数据目录交给旧二进制（`FORMAT_VERSION` 拒绝）；回滚必须整目录回退。
- `SessionConflict` 是 **known-not-applied**（`put`/`delete`/`multi_put`/`cas` 未生效），调用方可安全地重新读取当前状态后重试；它**不是** `SessionExpired` 那种“结果未知”。
