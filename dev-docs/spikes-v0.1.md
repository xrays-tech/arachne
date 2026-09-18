# Arachne M0 spike 关闭记录（v0.1）

> 上游：`test-plan-v0.1.md` §13（S1–S6 待核验清单，各项带默认决策）。本文件关闭 M0 阶段的 spike：钉死 API 事实，或在核验不利时采用已记录的默认决策。S1/S6 的"是否"已由 D-S1/D-T2 锁定，此处**只核验机制**。

依据版本：`raft` 0.7.0（本机 `~/.cargo/registry/src/…/raft-0.7.0/`）。

## S1 — `raft` 0.7.0 选举超时 RNG 可注入性 —— **已核验：不可注入（需补丁）**

- **事实**：`reset_randomized_election_timeout`（`raft.rs:2807`）内部直接调用全局 RNG：
  - `raft.rs:2810`：`rand::thread_rng().gen_range(self.min_election_timeout..self.max_election_timeout)`
  - 模块导入：`raft.rs:27`：`use rand::{self, Rng};`
- **仅有部分钩子**：`Raft::set_randomized_election_timeout`（`raft.rs:474`）与 getter（`raft.rs:495`）存在，但每次选举/重置仍走 `thread_rng`，无法仅靠公开 API 得到确定性选举时序。
- **结论**：**RNG 不可注入/播种** → 触发 **D-S1 默认决策**：采用 workspace `[patch.crates-io]` 携带**一行 RNG 注入改动**，以上游化为目标，纳入 vendor/审计清单跟踪；补丁落地后**双跑复现门禁在选举时序层面全量生效**（当前 L1 双跑门禁只比较**提交结果**，见 `m0_determinism.rs` 头部说明，选举 RNG 只影响"谁/何时"当选，不影响结果）。
- **补丁定位**：`raft-0.7.0/src/raft.rs` 的 `reset_randomized_election_timeout`（2810 行附近）——将 `thread_rng()` 改为可注入的 RNG（例如从 `Config`/`Raft` 读取一个确定性种子源）。

## S2 — `tokio::select!` 无 `biased;` 的分支轮询熵 —— **策略已强制**

- M0 尚无 tokio 使用点；`scripts/check-entropy.sh` **Gate B** 已强制"每个 `tokio::select!` 必须伴随 `biased;`（10 行窗口内）"，否则 CI 失败。
- **默认决策已在位**（一律 `biased;` 或禁用宏）。该方法为启发式，限制已脚本内自述。

## S3 — tokio `Builder::rng_seed` 覆盖范围与最低版本 —— **推迟到 L2（M1）**

- M0 sim 路径未引入 tokio 运行时，无核验需求。
- **默认决策**记录在案：覆盖不足的熵源（如 `watch` 唤醒）→ 对应原语在 sim 路径替换为确定性封装。L2 骨架（`l2/`，turmoil）落地时核验。

## S4 — turmoil `unstable-fs` FsCorruption 能力面 —— **未核验；默认设计已在位**

- **默认决策已在位**（本方案本就如此设计）：磁盘故障**全部由 `FaultyStorage` 台账实现**，不依赖 sim FS 的撕裂/记账；turmoil FS 仅作补充。故该能力面不足不阻塞。

## S5 — turmoil 消息重复/变更钩子内建与否 —— **未核验；默认决策合理**

- **默认决策**：无内建 → `Transport` 测试适配器在收发路径自行注入（重复/丢弃/延迟）。

## S6 — stateright `semantics/linearizability.rs` 可否独立复用 —— **部分核验（接入完成，复用方式待定）**

- **已完成**：`model-check/`（独立非工作区项目）已接入 `stateright = "0.31"`，并用真实 `arachne` 跑通一个有界 3 节点复制日志模型（`cargo run`：3259 unique states，always 属性无反例，找到 liveness 例）。
- **待定**：`semantics/linearizability.rs` 能否**独立**（脱离其 `Tester`）作历史检查器复用尚未评估。
- **默认决策**：不可 → 将其算法移植进自建 Wing–Gong 检查器（主案不变），**交叉验证门禁不取消**。M1/M2 落地自建检查器时核验。

## 汇总

| Spike | 状态 | 处置 |
|---|---|---|
| S1 | **已核验** | RNG 不可注入 → 执行 D-S1 一行补丁（定位 `raft.rs:2810`），补丁后双跑门禁全量生效 |
| S2 | 已强制 | `check-entropy.sh` Gate B 强制 `biased;` |
| S3 | 推迟 M1 | 默认：sim 路径替换熵源 |
| S4 | 默认在位 | 磁盘故障走 `FaultyStorage`，不依赖 sim FS |
| S5 | 默认在位 | `Transport` 测试适配器自行注入 |
| S6 | 部分 | stateright 已接入并跑通模型；独立复用方式待 M1/M2 核验，默认移植算法入自建检查器 |
