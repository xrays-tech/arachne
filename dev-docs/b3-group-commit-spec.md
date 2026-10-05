# B3（U3）：WAL 组提交 —— flusher 批排合并 + 生产接线（E-rev，实现）

> 实现批次文档。执行依据：`upgrade-http-wal.md` §4「B3（U3）」与 `propsol-v0.2.md` rev U（§U3）。
> 批次编号 B3 = 升级计划第 3 批；`U3` = 设计文档 rev U 的组提交子项。
> 上一批 B2（U2，WAL sync 平台化）已落地并 commit（`a7f33f6`），本批在其 `sync_durable` 抽象上续接。

## 1. 目标与问题

etcd / RocksDB / LevelDB 均为 **组提交**：一次 `fdatasync` 落盘一个并发写入批次，而非每 Ready 逐条刷设备。
本项目 M1 基线（`handoff-m1.md` §1.35）单连接 put 40 ops/s（p50 25.1ms），主因之一即每次提交
做整设备 `sync_all`。B2 已把 sync 落到平台分派（Linux `fdatasync`），但**仍未组提交**——
rev P 的 flusher 线程虽单批一次 sync，却仍是**每 Ready 一个 FlushJob、每 Job 一次 sync**。
B3 的核心：**一次 sync 落一批并发写**，并修正 `offloaded_fsyncs` 计数口径。

## 2. 设计决定

### 2.1 批边界：flusher 排空合并，非固定时间窗

**不**引入"写缓冲满或短窗口到点"的计时窗口。批边界 = **flusher 线程一次唤醒时的
排空合并（drain coalescing）**：

1. `rx.recv()` 取到第一个 Job（这是本批的锚，也阻塞直到有 Job）。
2. `try_recv()` 连续排空所有已到达、与本 Job 同一批次的 Job —— 没有 sleep、没有定时器。
3. 按 `segment_first_index` 分组（首现顺序，同段取最后 Job 的 fd）；**每段一次 `sync_durable`**。

这给了 rev P 的 `FlushToken` 流水线一个**自然批窗口**：并发提案在到达 flusher 时自动并入
同一批，无需在 actor 侧再维护"窗口到点"状态机。窗口大小由真实并发决定，天然适配
写洪峰（合并到几次 fsync）与写稀（退化到每 Ready 一次，不恶化 p99）。

### 2.2 每段一次 sync + 失败即停

- 一个批内**多个 Job 落同一段** ⇒ 只刷一次该段的 inode（`sync_durable` 刷 inode 即
  覆盖所有先行 append，因同一文件单写者、fd 是同一 inode 的 dup）。
- 一失败即整批失败（fail-stop）：任一段 `sync_durable` 报错，则该批所有 Job 报
  `Err`。保守选择——节点据此 fail-stop，绝不误认未 durable 为 durable。
- 每批的 `flusher_syncs` 增量为**实际尝试的段数**（失败批也计入其尝试）。

### 2.3 `offloaded_fsyncs` 计数口径修正（本批关键 bug 修复）

**旧（错误）**：`poll_flush` 每次返回 `Ok` 就 `offloaded_fsyncs += 1` —— 把
**token 数**当成了 **设备 fsync 数**。一个批内 N 个 Job 落同一段，实际只刷了 1 次，
却被记为 N 次。

**新（正确）**：`stats().offloaded_fsyncs` = **设备真实 sync 次数**，由两部分相加：
- 同步路径：`note_flushed`（sync 路径每次真正 fsync 后 +1，本批不动）；
- flusher 路径：`flusher_syncs`（新增，每批每段一次，失败计入）。
`stats()` 求和；offloaded 模式下同步路径闲置，flusher 计数主导；sync 模式下反之。

配套：flusher 的 `notify_fsynced`（每真实 fsync 后通知 `fsync_observer`）在 sync 路径是
**逐 Job** 的；本批通过 **owner 标记** 将其坍缩为**每段一次** —— 批内每个段
最后到达的 Job 被标记 `is_segment_owner = true`，只有它触发该段的单次 `notify_fsynced`。
这样 `FsyncLedger`（外部可观察）也如实记录"一段一次 fsync 事件"。

### 2.4 红线（本批不改）

| 项 | 决定 | 理由 |
|---|---|---|
| `FsyncPolicy` 变体/默认值 | 不动 | B2 已落地；本批只改 flusher 批处理与计数 |
| Seam 类型（`FlushHandle`/`PersistSubmit`/`FlushToken`） | 不动 | 跨 crate 契约冻结；`FlushJobResult` 为内部私有类型可增字段 |
| 提交窗口计时 | 不做 | 用 drain 合并替代；窗口大小由真实并发定 |
| HardState 批处理（I1） | 不动 | `set_hard_state_buffered` 仍逐次 sync；BatchMs 仅作用于日志条目 |
| 文件范围 | `wal.rs` + `node.rs` + docs | 不动 `arachne-seam/`、`node.rs` 之外的 crate |

## 3. 实现落点（`arachne/src/storage/wal.rs`）

| 符号 | 改动 |
|---|---|
| `Offloaded` struct | 增 `flusher_syncs: Arc<AtomicU64>`（flusher 真实 fsync 计数） |
| `FlushJobResult` struct | 增 `is_segment_owner: bool`（段 owner 标记，坍缩 per-segment notify） |
| flusher 循环（`enable_offloaded_durability` 内 `move` closure） | 重写：`recv` → 排空 drain → 按段分组 → 每段 `sync_durable`（fail-stop）→ `flusher_syncs += 段数` → 回填每个 Job（含 owner 标记） |
| `stats()` | `offloaded_fsyncs` = sync 路径 + `flusher_syncs` 之和 |
| `poll_flush` | 删 `offloaded_fsyncs += 1`；仅 `is_segment_owner && 段==当前段` 时触发单次 `notify_fsynced` + `pending_entry_fsync=false` |
| `submit_flush` | 不变（仍建 Job：`dup` 当前段 fd、记 `segment_first_index`） |

**所有权细节**：`std::fs::File` 是 `!Clone`，Job 的文件须 **move 出** 批（不能 clone）。
`flusher_syncs` / `thread_delay` 因 `move` closure 会 move 掉，故各做一份独立 `Arc::clone`
——一份交线程，一份留 storage 结构体供 `set_flush_delay_ms` / `stats()` 使用（两 Arc 别名同一计数器）。

## 4. 生产接线（`arachne-node/src/node.rs`）

`Runtime::new` 之前，在 WAL 打开/配置完成后显式开启：

```rust
wal.set_trailing_keep_bytes(profile.wal_trailing_keep_bytes);
wal.enable_offloaded_durability()?;   // ← B3: 生产节点默认走 offloaded 流水线
```

raft 节点（`consensus/node.rs`）**已内置** offloaded 处理：`persist_ready_records`
返回 `PersistSubmit::Offloaded(token)` 时，`step` 存 token 于 `PendingPersist`，
`finish_persisted` 轮询该 token 并**失败即 stop**（`NodeError::Storage("offloaded flush failed")`）。
故 node 侧仅需"开启"，轮询/失败停机制无需新增代码。

**为何默认开**：库 `WalStorage` 默认仍为同步路径（不动，`enable_offloaded_durability`
显式开启），保证库级测试与逐字节兼容；**生产二进制**（`arachne-kv-node`）则默认走流水线
以拿到组提交收益。二者一致：库安全默认 + 生产优化默认。

## 5. 行为契约（可测试断言）

设 `FsyncLedger` 为外部 fsync 事件观测器（每真实 fsync 后 `on_segment_fsynced`）：

| 场景 | 批内 Job / 段 | 实际设备 fsync | `stats().offloaded_fsyncs` | `ledger.events()` 数 | 各 token |
|---|---|---|---|---|---|
| 单段 | 5 / 1 | 1 | 1 | 1 | 全 `Ok` |
| 跨段（rollover） | 6 / 2（`wal-…01.log`、`wal-…04.log`） | 2 | 2 | 2 | 全 `Ok` |
| 失败段（段= `/dev/null`） | 3 / 1 | 1 | 1 | 0（失败不 notify） | 全 `Err` |

- **失败段**：活跃段被 symlink 到不可 sync 的 `/dev/null`（macOS `F_FULLFSYNC` 报
  ENOTSUP）；整批 fail-stop，所有 token 报 `Err`，节点 fail-stop，不承认 durable。
- **`stats().offloaded_fsyncs` 是"设备真实 fsync 数"，不是 token 数**（本批修复的核心）。

## 6. 验证

### 6.1 TDD（`#[cfg(feature = "fault-injection")]`）

新增三处测试（`wal.rs` test module，先红后绿）：

1. `coalesced_flush_counted_once_per_segment`：5 次提交 → 全 `Offloaded` → 全 poll `Ok`；
   断言 `stats().offloaded_fsyncs == 1`、`ledger.events().len() == 1`。
2. `coalesced_flush_syncs_each_segment_across_rollover`：6 次（4 条数据 + 2 条心跳，
   `segment_bytes=80` 触发 rollover 成 2 段）；断言 `== 2`，且 `wal-…01.log` /
   `wal-…04.log` 两段皆建。
3. `coalesced_flush_failure_fails_every_covered_token`：3 次提交后把活跃段 symlink 到
   `/dev/null`；3 个 poll 全 `Err("…flusher batch sync failed…")`；`stats() == 1`；
   段文件仍在（失败发生在 fsync 非 append）。

`poll_token(store, token, deadline)` 辅助（带超时轮询）用于防止 flaky。
TDD 基线（改前）：测试 1/2 各差多倍（5 vs 1、6 vs 2）、测试 3 差 1（0 vs 1）——
确认旧代码不满足。改后全绿。

### 6.2 回归（§2 回归门）

- `cargo build --workspace` ✅
- `cargo test -p arachne-kv`（含全集成/不变式）✅；`--features fault-injection` ✅
- `cargo test -p arachne-kv-node`（生产节点现在默认走 offloaded 流水线）✅
- 门禁：`check-entropy.sh`、`check-deps.sh`、`check-profile-knobs.sh`、`check-release-features.sh`（见 §7）

**注**：本批未新增 profile 旋钮（`wal_offloaded` 字段），故 `check-profile-knobs.sh`
不受影响；生产默认开是 `node.rs` 显式接线，非 profile 驱动的旋钮。

## 7. 后续 / 开放项

- **效率实测**（T4）：B3 组提交后 put p50 应在 `docker/bench` 上进一步下降
  （B1+B2 后 4.1ms；预期逼近 etcd 量级）。由 orchestrator 复测对拍，本批次只要求
  build/测试/门禁绿。
- **读延迟观察项**（§1.29 升）：线性读 4 并发较 M1 基线下滑（286–337 vs 767 ops/s）
  待 B3 调和后复测；若仍下滑则单独归因（driver 并发形态 / hyper 复用 / 核心 ReadIndex）。
- **`BatchMs` 默认值**（rev U §U3 开放项 2）：本批以 drain 合并替代固定窗口，故
  `FsyncPolicy::BatchMs` 的"窗口"语义未被本批使用；默认参数权衡随实测数据定。

## 8. 关键文件

| 文件 | 改动 |
|---|---|
| `arachne/src/storage/wal.rs` | flusher 批排合并 + owner 标记 + `flusher_syncs` + `stats()`/`poll_flush` + 三处 TDD 测试 |
| `arachne/src/storage/sync.rs` | 不动（B2 的 `sync_durable`，本批调用它） |
| `arachne/src/consensus/node.rs` | 不动（已有 offloaded 轮询 + fail-stop） |
| `arachne-node/src/node.rs` | 加 `wal.enable_offloaded_durability()?`（生产默认开） |
| `dev-docs/b3-group-commit-spec.md` | 本文（新增） |
