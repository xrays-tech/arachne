---
status: in-progress
phase: 1
updated: 2026-10-07
---

# Implementation Plan: 多客户端并发读性能提升（对齐 etcd 并发读模型）

## Goal
在不牺牲线性一致性（保留 ReadIndex Safe 模式、禁止 lease 读）的前提下，通过"单飞合并式 ReadIndex 屏障 + 状态机读并行化 + 线程解耦"，把 4 连接并发线性读从落后 etcd ~14%（≈2043 vs ≈2365 ops/s / p50 0.43 vs 0.40ms）提升到持平或反超，并将 4w 门槛固化进性能门禁。

## Context & Decisions
| Decision | Rationale | Source |
|----------|-----------|--------|
| 优先级 A 优先：单飞合并式 ReadIndex 屏障（in-flight-attach + broadcast notifier） | 与 etcd "一轮 quorum 往返放行无界等待读者"（`linearizableReadLoop`）的差距是 4w 落后的最大来源；C4 时间窗口合并已被实测为 0 收益，本方案是架构上不同的"在途往返附加"，未试过 | `ref:exp-1`, `ref:res-1`, `research/report-read-concurrency.md` §6-A |
| B 次之：状态机读并行化（快照 / `Arc<RwLock<BTreeMap>>` + 直接读） | 目前 BTreeMap 无锁但被唯一 apply 任务串行独占读取，且每线性读多一次 mpsc hop；对齐 etcd bbolt 只读事务快照模型 | `ref:exp-1`, `ref:res-1`, `research/report-read-concurrency.md` §6-B |
| 快照版本不变式：仅当快照版本 ≥ 已确认 read_index 才可服务；快照发布与应用顺序强一致 | 保证线性读正确性不被并行读破坏；等价于 etcd `ApplyWait(confirmedIndex)` 共享等待 | `ref:exp-1`, `ref:res-1`, `findings-etcd.md` §4.5 |
| C/D/E 为结构性优先级：线程解耦、出站不内联 await、读簿记瘦身 | apply+actor 同处一个 OS 线程并发度=1；`step()` 内联 await 慢 peer 停摆全局；线性 token 扫描/Vec 重分配皆为并发基数放大成本 | `ref:exp-1`, `research/report-read-concurrency.md` §3.2 S1/S2/S6/S10 |
| 验证以 1w 为稳定信号，4w 取多轮中位数 + 同集群 A/B | 4w 是 ~2× 高方差区间，方向可翻转；GIL 实验已证瓶颈在 server 端 ReadIndex 串行段 | `ref:exp-1`, `propsol-v0.2.md:530-532,577-600` |
| 每阶段执行 TDD（先写测试后实现）+ 编译验证 + 确定性与模型测试回归 | 项目规约强制；任何读/apply 并发改动必须过 l2/model-check/fuzz 确定性校验 | AGENTS.md |
| 排外：不实现 lease/clock read | v1 设计硬性约束 `ReadOnlyOption=Safe`；提升只能来自合并/重叠/缩短往返与并行读服务 | `ref:exp-1`, `handoff-m1.md:94` |

---

## Phase 1: 基线与可观测性 [IN PROGRESS]
- [ ] **1.1 重跑 docker/bench 基线（1w/4w linear、stale、put，多轮取中位数，同集群 A/B，n=400）** ← CURRENT
- [ ] 1.2 确认 `arachne_read_index_rounds_total` / `arachne_read_index_pending` 在 4w 与写负载下可观测，记录 rounds/sec 与 4w p50 的相关性基线
- [ ] 1.3 录入对照表：本环节的 1w/4w linear + stale + put 数字（与 propsol-v0.2.md:561 及报告 §2.2 对齐）

## Phase 2: A 单飞合并式 ReadIndex 屏障 + E 读簿记瘦身 [PENDING]
- [ ] 2.1 (TDD) 设计 `PendingRead` 重构：token→`HashMap` 索引；新增"在途 round 状态"，新到达读挂到在途 round 的广播 notifier 而非新开轮 → 目标 `runtime/mod.rs:985-996,290-309,1800-1860`
- [ ] 2.2 (TDD) 实现：quorum 确认后一次放行所有挂起读者（读下界=该轮确认的 read_index），含超时重试与 leader 变化语义保持不回归
- [ ] 2.3 (TDD) E：token 匹配改 HashMap、去除 `pending_reads` 每轮 drain/重分配 → `runtime/mod.rs:1027-1032,1804-1859`
- [ ] 2.4 回归：`l2`、`model-check`、`fuzz` 确定性重跑 + `cargo build --release -p arachne-kv-node`
- [ ] 2.5 bench A/B（同集群、多轮中位数）：断言 rounds/sec 显著下降、4w p50/吞吐提升、1w 不回退

## Phase 3: B 状态机读并行化（快照发布 + 直接读） [PENDING]
- [ ] 3.1 (TDD) 设计快照发布：`KvStateMachine` 改为 `Arc<RwLock<BTreeMap>>`/不可变快照，apply 每批提交后发布新版本；确立"快照版本 ≥ read_index 才可服务"不变式 → `state_machine/kv.rs:66-71,305-307`
- [ ] 3.2 (TDD) 实现：`resolve_reads` 直接内存读（去掉 `reads` mpsc hop）→ `runtime/mod.rs:1824-1833`；`get_stale` 可在 apply 外部并发服务 → `runtime/mod.rs:1637-1651`
- [ ] 3.3 并发正确性测试：写风暴下 stale/线性读语义不破坏、快照版本与写应用顺序强一致（含 fsync ledger 断言）
- [ ] 3.4 回归 + bench A/B：4w/1w 提升且无单机读吞吐回退

## Phase 4: C apply/读服务线程解耦 + D 出站不内联 await [PENDING]
- [ ] 4.1 (TDD) apply 任务（或仅读服务）移出 actor 的 current-thread runtime → `runtime/mod.rs:885-886,2213-2227`
- [ ] 4.2 (TDD) peer 出站 `Message` 改有界 per-peer 队列 + 独立发送任务，`step()` 不再内联 await → `consensus/node.rs:1131-1137,1149-1171,1199-1202`
- [ ] 4.3 回归 + bench：尾延迟、并发写风暴下线性读 p99、慢 peer 场景

## Phase 5: F 可选旋钮 + 门槛固化 + 文档 [PENDING]
- [ ] 5.1（可选）受控"收集窗口"旋钮（p50 换吞吐），以实测数据决定是否保留 → `propsol-v0.2.md:556`
- [ ] 5.2 依据 Phase 1 新基线给 `scripts/check-perf-baseline.sh` 增加 4w 线性读门槛
- [ ] 5.3 更新 `README.md` 测量章节与 `dev-docs/propsol-v0.2.md` 记录；完整门禁 `scripts/check-*.sh` + 合入 main（PR）

## Notes
- 2026-10-07: 计划源自已合入 main 的调研报告（PR #1, `research/report-read-concurrency.md`）`ref:exp-1`, `ref:res-1`
- 2026-10-07: 4w 高方差警告 —— 每个实现阶段都必须"同集群 A/B + 多轮中位数"，不得以单次 4w 判胜
- 2026-10-07: A（§2）在途附加与已回滚 C4 时间窗口方案不同，属未试路径；若 A/B 证伪（合并无法提升），记录并直接转向 Phase 3 的并行读并行化
