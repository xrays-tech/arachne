# Arachne 集成测试方案（test-plan v0.1）

> 上游文档：`propsol-v0.2.7.md`（§9 为其策略大纲，本文是其**可执行展开**；两处冲突时以本文为准并回改上游）。
> 工具调研基线：2026-09，所引版本已核验（turmoil 0.7.2、loom 0.7.2、shuttle 0.9.3、cargo-fuzz 0.13.2、libfuzzer-sys 0.4.13、arbitrary 1.4.2、proptest 1.11.0、stateright 0.31.0、`raft` crate 0.7.0、madsim 0.2.34）。
> 设计前提沿用上游：共识内核复用 `raft` crate（tikv/raft-rs）、tonic 传输、WAL+快照存储、全内存状态机+会话表、§2 一致性语义、§4.2 不变量 I1–I4、§5.5.3 恢复算法。
> 修订记录：v0.1.1 新增决策记录表（D-T1/D-T2/D-L4/D-S1 评审锁定）与 **§3.2 工件/工作区布局、§3.3 进程级 harness（D-ART）**——补齐 L3/L4 所预设的可执行载体；v0.1.2 锁定 D-ART 四项子决策（crate 名、默认 feature、TOML 配置、test-observability 特性门禁）；**v0.1.3 D-ART-rev1：抽出无依赖叶 crate `arachne-seam`（接缝 trait + 核心类型）**——`arachne-transport-tonic` 改依赖 `arachne-seam`（不再 `→ arachne`）以消除 `transport-tonic → arachne` 与 D-ART-feature `arachne → transport-tonic` 构成的 **cargo 包循环**；`arachne` 重导出接缝保持嵌入者公共 API 不变；并澄清 sim/testsupport 精简构建（对 `arachne` 一律 `default-features = false`，testsupport 永不含 tonic，sim 的 tonic 仅 L2 落地时显式启用）。

## 决策记录（评审已锁定）

沿用上游 §11 的决策记录格式约定：推翻任何一条须追加 `-rev` 条目，不静默更改。

| # | 决策 | 结论 | 理由 | 落点 |
|---|---|---|---|---|
| D-T1 | 确定性模拟器 | **turmoil 0.7.2 锁定为 L2 主选**；madsim 为记录在案的否决项（磁盘故障 TODO 桩 + `[patch.crates-io]` 版本追高） | 跑真实 tokio/tonic、crash/bounce 语义与进程边界要求对齐 | §4 T1、§6.1；上游 §9.1 |
| D-T2 | 线性化检查器 | **stateright 0.31.0 锁定为 `dev-dependency`**（仅测试依赖，不进生产依赖树）：作 T3 有界模型检查引擎 + 自建 Wing–Gong 检查器的交叉验证参照。**"去掉参照检查器"不是选项**——检查器自身的正确性门禁依赖它（§11 缺口 8） | 自建检查器是新代码，必须有独立参照兜底；stateright 另承担设计层模型检查 | §4 T2/T3、§11；上游 §9.1 |
| D-L4 | L4 真机持久性门禁 | **专用真实磁盘 CI runner 现在即立项建设**（M0 起配置、M1 起可用、M4 出全量门禁）；"发版前手工执行"与"延后到 M2"两个备选已评审**否决**——fsync 语义不能等到 M4 才第一次被真实验证 | INV2/INV6 的真盘复验不可替代（模拟 fsync 是"撒谎的"）；自动化 runner 是唯一可重复的证明方式 | §2 L4 行、§10、§12 M0/M4；上游 §9.4 |
| D-S1 | raft-rs 选举 RNG 不可注入时的响应 | **锁定方案：workspace `[patch.crates-io]` 携带一行 RNG 注入改动，以上游化为目标**；补丁进 cargo-deny/vendor 审计清单跟踪。S1 spike 只核验"是否需要补丁"，不再决定"要不要补丁" | 双跑复现门禁是整个确定性体系的根基（§5），不能留缺口；一行 patch 成本远低于失去可复现性 | §5 E3、§13 S1；上游 §9.5、§13 R3 |
| D-ART-names | 工件与 crate 命名 | **锁定**：`arachne`（lib）、`arachne-transport-tonic`、`arachne-node`（bin）、`arachne-testsupport`、`arachne-sim`，外加 `examples/`、`fuzz/`、`model-check/` | 名称即职责：lib 产品主体 / 生产传输隔离 / 运维 bin / 测试支撑 dev-only / 测试 bin 不发布 | §3.2；上游 §4 工件边界 |
| D-ART-feature | `arachne` 默认 feature 拉入 tonic | **锁定：默认 feature `transport-tonic` 经 `arachne-transport-tonic` 拉入**，嵌入者单依赖即得完整栈；**代价**：默认依赖树更重（tonic/rustls）——精简核心可用 `default-features = false` 自选传输；sim 测试构建一律 `default-features = false` | 嵌入者工效学优先；代价由 embedder 显式选择规避，且 tonic 类型仍被隔离在单一 crate | §3.2；上游 §4 |
| D-ART-config | 节点配置文件格式 | **锁定 TOML**：`arachne-node --config node.toml`，字段 = 上游 §7 Profile + 覆盖项 + §5.7 `initial_cluster`；CLI 单项覆盖为辅；测试 harness 复用同一 TOML 配置面，不引入第二格式 | TOML 与 §7 配置预设表同构、可注释、diff 友好；单一格式避免"文档一处、文件一处"漂移 | §3.3；上游 §7 |
| D-ART-testobs | 生产 bin 携带 `test-observability` feature | **锁定为"无测试代码入生产"规则的最小例外**：`arachne-node` 可带 `test-observability` feature（仅追加结构化 marker 日志，零行为改变）；**CI 门禁强制 release/发布构建排除该 feature**（feature 解析检查 + 发布产物 hash 断言） | L4 的阶段精确 kill 需要进程内标记（§3.3）；feature flag 而非独立分支 = 无分叉维护成本；门禁保证出货产物不含该特性 | §3.3、§10；上游 §9.4 |
| D-ART | 测试工件与工作区布局 | **五工件布局锁定**（§3.2）：`arachne`（lib 产品主体）+ `arachne-transport-tonic` + **`arachne-node`（薄运维 bin，生产交付物）** + `arachne-testsupport`（仅 dev-dependency）+ `arachne-sim`/`examples/`/`fuzz`/`model-check`。L3/L4 的被测进程就是 `arachne-node` | force-recovery（上游 §6）与成员运维（上游 §3.1）本就是生产接口 → bin 是一等交付物而非测试装置；bin 与嵌入者共用同一 lib 代码路径，L3/L4 测的即出货代码；测试依赖不得泄漏进生产树 | §3.2/§3.3、§9、§10、§12；上游 §1/§3.1/§4/§6.1/§7 |
| D-ART-rev1 | 抽出叶 crate `arachne-seam` 消除包循环 | **锁定**：新增**无依赖叶 crate** `arachne-seam`（接缝 trait `Transport`/`TransportRx`/`TransportFactory`/`Clock`/`Rng`/`StateMachine` + 核心类型 `NodeId`/`LogIndex`/`Term`/`Timestamp`）；`arachne-transport-tonic` 依赖 `arachne-seam`（**不再依赖 `arachne`**）；`arachne` 依赖 `arachne-seam` 并重导出接缝（嵌入者公共 API 不变）；工件布局由五件扩为**六件**；sim/testsupport 对 `arachne` 一律 `default-features = false` | 原 `transport-tonic → arachne`（为 `impl arachne::seam::Transport`）与 D-ART-feature 的 `arachne --(default)--> transport-tonic` 构成 **cargo 包循环**（实测硬拒绝）；叶 crate 使"接缝归核心、实现归传输 crate"可落地，并顺带澄清 F2 精简构建 | §3.2；上游 §4 工件边界 |

---

## 1. 范围与目标 / 非目标

**"集成测试"在本方案中的定义**：≥3 节点的多实例系统测试，走**真实传输代码路径**（tonic，承载于确定性模拟网络之上），注入**崩溃 / 分区 / 磁盘故障 / 消息异常**，以**不变量断言 + 客户端预言机**自动判定对错——不是"跑通一个 happy path"。

**目标（证明什么）**：

1. §2.2 的对外语义：线性一致读/写（G2）、会话去重恰好一次（G1）、`get_stale` 弱读边界（N1）、"结果未知"窗口（N2/N3）。
2. §4.2 fsync 顺序不变量 I1–I4 与 §5.5.3 恢复算法的每一条分支。
3. §5 机制：选举/复制/成员变更/ReadIndex/快照/引导/版本握手。
4. §6 force-recovery 与启动校验。
5. §3.3 错误矩阵的每一格都有触发它的场景。

**非目标**：

- ❌ 重测 raft 内核本身（选举/日志匹配算法正确性由 tikv 上游保证；我们只测**我们的集成**——Storage 适配、Ready 处理顺序、配置开关组合）。
- ❌ 单元级覆盖（WAL 编解码、会话表数据结构、错误分类的纯函数测试）——属于常规 cargo test，不在本文展开，仅在分层表中占位。
- ❌ 性能/p99 SLO 基准体系（另立 benchmark 方案；本文仅在 M2/M4 保留延迟冒烟门槛）。
- ❌ 拜占庭行为、真实多数据中心网络物理故障、安全渗透测试。

## 2. 测试分层总览

| 层 | 载体 | 能证明 | 不能证明 | CI 门禁 |
|---|---|---|---|---|
| **L0 单元** | cargo test | 编解码、会话表、错误分类、单条不变量的纯函数部分 | 并发交叠、时序、跨节点行为 | 每 PR |
| **L1 单进程内核集成**（raft-rs `harness/` 模式） | 内存 `Network` + `Interface` 风格驱动 3 个 `RawNode` + 内存 Storage 适配 | Ready 处理顺序（I1–I4）、ConfChange 单步门控、read_index 状态机、Storage trait 契约——**确定性、微秒级、可单步** | 真实 tokio/tonic 栈、磁盘故障、崩溃边界 | 每 PR |
| **L2 单进程多节点确定性模拟**（turmoil 0.7.2） | 真实 tokio + 真实 tonic 代码路径，跑在 turmoil 模拟网络/时钟上；FaultyStorage 注入磁盘故障；`crash`/`bounce` 模拟进程边界 | §1 全部目标：分区/崩溃/磁盘故障下的不变量、线性化、**同种子可复现** | 真实磁盘 fsync 语义、真实网卡/内核栈（半开连接、缓冲回压） | 每 PR（固定种子）+ nightly（随机种子） |
| **L3 多进程真实网络** | **`arachne-node` 真实进程 ×3–5**（真实 TCP/TLS；进程 spawn/配置注入/就绪等待见 §3.3） | 真实传输栈、握手、升级矩阵冒烟、CLI 运维流程 | 时序确定性（不可复现） | nightly 冒烟 + Release |
| **L4 真机 kill -9 持久性门禁** | 真实磁盘 + `kill -9` 循环（**专用 runner，D-L4**；fault-hook 二进制 + 外部注入器，§3.3）+ WAL 校验脚本 | I1/I2 在**真实 fsync 语义**下成立（模拟环境的 fsync 是"撒谎的"，此项不可替代） | — | Release |

**关键分层判据**：一个故障场景优先放在能证明它的**最低层**；L2 是主力；L4 只保留模拟无法证明的持久性断言（INV2/INV6 的真盘子集）。

## 3. 测试接缝与测试工件（必须先行——M0 交付物，见 §12）

### 3.1 接缝总则与接缝表

**总则：一切熵源与外部效应必须经过可替换接缝；生产与测试共用业务代码，只换接缝实现。**

| 接缝 | 生产实现 | 测试实现 | 针对的风险 |
|---|---|---|---|
| `Transport` | `arachne-transport-tonic`（tonic + rustls mTLS） | turmoil 承载**真实 tonic**（`turmoil::net::TcpListener` + `serve_with_incoming` + 自定义 connector/`Connected` 适配）；另有内存交换机实现供 L1 | madsim 式"换运行时"路线迫使 `[patch.crates-io]` 追版本（madsim-tonic 钉 tonic 0.14）——**采纳 turmoil 后此风险消除，但 tonic/turmoil 版本演进仍需隔离在单一 adapter crate** |
| `WalStorage` + `StateMachine`（raft-rs `Storage` 之下的自有缝） | 真实 WAL/内存状态机 | `FaultyStorage`（§6.2） | 磁盘故障注入点；raft-rs 要求自实现 Storage，此缝天然存在 |
| `Clock` | `std::time` 包装 | turmoil 模拟时钟 / 手动拨钟 | 会话 TTL、read_index 超时、GC 调度不可控；**规则：模拟相关路径禁直接调用 `std::time::Instant/SystemTime`**（CI grep 门禁）。GC 时间戳本就走日志传播（§2.3），天然确定 |
| `Rng` | `rand` 默认 | 由 sim seed 派生的 `DeterministicRng`（每 host 一个子流） | 选举抖动、重试抖动、UUID 生成引入不可复现性 |
| `ProcessBoundary`（测试支撑，非生产 trait） | — | crash/bounce 后**必须走完整真实启动路径**（META→快照→WAL 回放→raft init），禁止任何内存态残留 | 崩溃测试空转（最隐蔽的假测试） |

### 3.2 工件与工作区布局（决策 D-ART，v0.1.1 新增）

**缺口闭合**：本方案 L3/L4、升级矩阵（S12/S20）、RunManifest 的 `binary_hash`、force-recovery 的 CLI 集成测试都预设一个**可运行的节点二进制**——上游 §3 只定义了库 API。D-ART 锁定五工件布局（**D-ART-rev1 扩为六工件：新增无依赖叶 crate `arachne-seam` 消除包循环**）；crate 命名、默认 feature、配置格式、test-observability 特性四项子决策已锁定（D-ART-names / D-ART-feature / D-ART-config / D-ART-testobs），包循环消除与精简构建澄清见 D-ART-rev1（见顶部决策记录表）：

| 工件 | 类型 | 职责 | 依赖方向 |
|---|---|---|---|
| `arachne-seam` | **lib（叶 crate，无依赖；D-ART-rev1 新增）** | 接缝 trait（`Transport`/`TransportRx`/`TransportFactory`/`Clock`/`Rng`/`StateMachine`）+ 核心类型（`NodeId`/`LogIndex`/`Term`/`Timestamp`）。**无依赖、不含任何传输/存储实现**——接缝归核心、实现归传输 crate | 无（叶 crate） |
| `arachne` | **lib（产品主体）** | 上游 §2–§6 全部核心：API/共识封装/状态机/存储。**接缝 trait 与核心类型现居 `arachne-seam`，`arachne` 依赖并重导出之（嵌入者公共 API 不变）**。不含 tonic/rustls 类型（传输实现经默认 feature `transport-tonic` 引入；sim 测试构建用 `default-features = false`）。**命名锁定（D-ART-names）；D-ART-rev1 增 `arachne-seam`** | → `arachne-seam`；可选（默认 feature `transport-tonic`）→ `arachne-transport-tonic` |
| `arachne-transport-tonic` | lib | 生产传输实现（tonic + rustls mTLS）；**唯一允许出现 tonic/rustls 类型的地方**；`impl arachne-seam::Transport` | → `arachne-seam`（**不依赖 `arachne`**——断环，D-ART-rev1） |
| `arachne-node` | **bin（生产交付物，非测试装置）** | 薄装配层：CLI/TOML 配置加载（上游 §7 配置面）、信号与优雅关闭、`/readyz` + `/metrics` 端点（上游 §8）、`force-recovery` 与成员运维子命令（上游 §6/§3.1）。**禁止包含共识/存储/状态机逻辑**——保证 L3/L4 被测进程与嵌入者运行同一份 lib 代码 | → `arachne`（接缝经其重导出；M1 接线真实传输时按上文对齐依赖） |
| `arachne-testsupport` | lib（**仅 dev-dependency，`publish = false`**） | FaultyStorage、fsync 台账、ClientOracle、不变量检查器、RunManifest、**进程 harness（§3.3）**、turmoil 包装（SimNetwork） | → `arachne-seam`（仅叶 crate——**结构上不可能拉入 tonic**；D-ART-rev1）（+ turmoil/shuttle 等 dev 生态） |
| `arachne-sim` | 测试 bin（`publish = false`） | L2 场景注册表 + repro CLI（§9） | → `arachne`（`default-features = false`）+ `arachne-seam` + `arachne-testsupport`；tonic 仅在 L2 harness 落地时**显式**启用（D-ART-rev1） |
| `examples/`（arachne 的 examples） | 示例（**禁止演示版**） | 完整可运行的公共 API 用例，无 TODO/桩/`unwrap` 演示味；每个示例都是 **API 工效学门禁**（示例难写 = API 难用，PR review 可见） | → `arachne`（默认 feature `transport-tonic` 含传输） |
| `fuzz/`、`model-check/` | 测试 bin | T5 fuzz targets / T3 stateright 模型 | → `arachne`（+ `arachne-seam`）/ 纯模型（不依赖 arachne） |

**依赖方向规则**：`arachne-seam` 为无依赖叶 crate，接缝 trait 与核心类型全部归其所有；`arachne` 依赖 `arachne-seam` 并**重导出**之（**嵌入者公共 API 不变**）。`arachne-transport-tonic` 仅依赖 `arachne-seam`（**不依赖 `arachne`**）——由此 D-ART-feature 的"默认 feature `arachne → arachne-transport-tonic`"与"传输 crate 实现 `Transport`"不再构成 **cargo 包循环**（D-ART-rev1）。常规依赖图无环（`arachne` 的集成测试经 `dev-dependencies` 引 `arachne-testsupport`——cargo 允许 dev-cycle，但**生产依赖图必须无环**）；CI 以 `cargo-tree` 门禁检查"测试依赖不进生产 crate 的 `[dependencies]`"。嵌入者视角：`arachne` + 默认 feature 即得完整栈（D-ART-feature）。**精简构建（F2）**：`arachne-testsupport` 仅依赖叶 crate `arachne-seam`（结构上不可能拉入 tonic）；`arachne-sim` 对 `arachne` 用 `default-features = false`，其 tonic 仅在 L2 harness 落地时**显式**启用（D-ART-rev1）。**注意 feature 统一**：workspace 级 `cargo build --workspace` 会做 feature unification——sim 二进制会链接到含 tonic 的 `arachne` rlib；"sim 不编译 tonic"严格成立于 `-p arachne-sim` 构建，L2 启用 tonic 时据此决策。

**否决项（记录在案）**：① test-only 专用 main——测试路径与生产路径分叉，L3/L4 将不再测出货代码；② 用 example 充当进程 harness——示例职责是 API 工效学与编译门禁，进程编排是 `arachne-testsupport::proc` 的职责（§3.3）。

### 3.3 进程级 harness（L3/L4 被测进程与注入机制）

- **被测进程** = `arachne-node`（生产 bin）。两种构建：**生产二进制**（无测试 feature；用于 S12 升级矩阵与 L3 冒烟）与 **fault-hook 二进制**（`--features test-observability`：仅追加结构化标记日志——fsync 批次完成、ready 循环计数、快照起止——**不改变行为逻辑**）。L4 的阶段精确 kill 用 fault-hook 二进制，升级矩阵用生产二进制。
- **配置注入（D-ART-config 锁定）**：**配置文件格式 = TOML**（`arachne-node --config node.toml`，字段 = 上游 §7 Profile + 覆盖项 + §5.7 `initial_cluster`），CLI 单项覆盖（`--data-dir`/`--advertise`/`--node-id`）为辅；harness 为每节点生成独立 TOML 配置 + 临时 `data_dir` + OS 分配的真实端口写入配置。**测试不引入第二配置格式**——测试用的就是用户用的配置面。
- **就绪等待**：`/readyz`（metrics 端点，上游 §8）返回 200 = raft 初始化完成（已当选或已追平）；辅以 stdout JSON ready 行（`{"event":"ready","node_id":…,"raft_addr":…}`）双保险；harness 等待超时上限 = 5×election_timeout。
- **故障注入（L4）**：`kill -9` 由 `arachne-testsupport::proc` 注入器从**外部**发送（进程组管理，杜绝孤儿进程）；触发时机 correlate 到 fault-hook 标记（如"第 N 次 ready 循环"/"第 M 次 fsync 批"），使 INV2 的"任意 ready 阶段 kill"**精确可复现**而非随机碰运气；注入序列写入 RunManifest。
- **`test-observability` 特性门禁（D-ART-testobs）**：该 feature 仅追加结构化 marker 日志、零行为改变，是"无测试代码入生产"规则的**唯一记录在案例外**；CI 强制 release/发布产物不含它（feature 解析检查 + 发布产物 hash 断言，入 §10 PR 门禁）。
- **磁盘故障（L4）**：真盘 kill -9 + **离线变异**（停机 → WAL 位翻转/截断 → 重启，对应 S04/S05/S06）；运行中在线腐蚀是 L2 FaultyStorage 的职责（§11 缺口 1 如实声明 L4 不覆盖在线腐蚀）。
- **日志/指标收集**：每节点 stdout/stderr → `artifacts/<run_id>/node-<id>.log`；结束时抓取 `/metrics` 快照；RunManifest 记录 marker 序列用于与 kill 时刻对账。
- **`binary_hash` 产出与用途（§9）**：harness 在构建后对 bin 产物取 sha256 + `git_rev` + feature 标志（`fault-hooks` 有/无）写入 RunManifest——复现时据此重建同一二进制；S12 升级矩阵记录 v(n−1)/v(n) 两个 hash，并断言混版拓扑按上游 §5.6 握手规则运行。
- **清理**：进程组 kill、失败时 tempdir 保留（§9 产物保留策略）、端口释放检查。
- **bin 自身的集成测试**（独立于库测试，PR 门禁）：CLI 参数解析与校验（非法配置 → fail-start 且错误码正确）、启动校验清单（上游 §6.2：flock 冲突 → `DataDirLocked`、META 不一致 → fail-start）、`force-recovery` 全流程（§6.1 前置检查 + 算法 + 双确认 flag）、优雅关闭（有界 10s、可选 leader 转让）、`/readyz` + `/metrics` 端点行为。单节点/双进程即可跑，无需集群。

### 3.4 M0 spike 核验清单

接缝可行性在写场景前用 spike 钉死（条目与默认决策见 §13）：turmoil 承载 tonic 的 connector 适配、`unstable-fs` 的 FsCorruption 确切能力面、tokio `Builder::rng_seed` 覆盖范围、raft-rs 选举 RNG 可注入性（S1 已锁定响应方案 D-S1）、stateright 检查器复用方式（S6 已锁定引入 D-T2）。

## 4. 测试基建选型（决策记录 T1–T7）

> 本节修订上游 §9.1 的 madsim 选型（v0.2.2 变更日志 E 条）。调研基线 2026-09，版本已核验。

### T1 模拟器：**turmoil 0.7.2（已锁定，决策 D-T1）**

| 维度 | turmoil 0.7.2 ✅ | madsim 0.2.34 ❌ | 自建 deterministic runtime ❌ |
|---|---|---|---|
| 与生产栈关系 | **跑真实 tokio + 真实 tonic**（单线程 + 种子化调度），测的就是上线路径 | 要求换 `madsim-tokio`/`madsim-tonic` + `[patch.crates-io]`（钉 tonic 0.14），版本追高成本高，测的不再是上线路径 | 全部自担 |
| 磁盘故障 | 有 `unstable-fs`/FsCorruption 钩子（确切能力面待 M0 spike 核验）；**且我们的磁盘故障模型主要落在自有 FaultyStorage 缝上，不依赖模拟 FS** | 磁盘故障是 TODO 桩（`power_fail()` 未实现、无撕裂写/损坏）——INV2/INV6 无法成立 | 自建成本最高 |
| 崩溃语义 | `Sim::crash`（丢未 sync 写）/`bounce`（以已 sync 数据重启）——**与"进程边界真实"的要求天然对齐**（§6.3） | 有，但与磁盘桩同样不可信 | 自建 |
| 网络原语 | partition / partition_oneway / repair / hold / release / set_fail_rate / 每链路延迟抖动 | 相似 | 全自建 |
| 维护活跃度 | 2026-04 仍活跃 | 可用但 tonic 绑定滞后 | 永久维护负担 |
| 采用用户 | — | RisingWave / Xline（成熟度佐证，非采用理由） | — |

**保留 madsim 为记录在案的否决项**；`Transport` trait 缝仍保留（单元测试、L1 内存交换机、未来换 gRPC 实现都需要它）。**规则：同一测试域只用一个模拟器**（避免双模拟器的语义漂移）。

### T2 线性化检查器：**自建 Wing–Gong 检查器 + openraft 式客户端预言机；stateright 锁定为 dev-dependency 参照（决策 D-T2）**

调研结论：**没有成熟的 Rust Porcupine 移植**（`porcupine` 0.2.4 是 Win32 API 包装——同名异物；`porcupine-rs` 0.3.0 单作者、零反向依赖，**不进生产 CI 依赖**）。

- **主判定器 = 客户端预言机**（§6.4，O(n) 检查，跑在每次 L2 运行内）：逐 seq 跟踪、幻值检测、每 seq 至多一个 log-id、read-your-writes、单调读、前驱链、缺失下界、崩溃后持久性扫描。
- **完备线性化检查**（Wing–Gong，NP 随历史长度爆炸）：自建小检查器，**仅在缩减历史上运行**（≤8 客户端 × ≤2000 ops，nightly）。
- **stateright 0.31.0 以 `dev-dependency` 锁定引入（D-T2）**，双重角色：① 交叉验证参照——小历史上与自建检查器比对判定一致性；② T3 有界模型检查的引擎。S6 spike 只核验其 `semantics/linearizability.rs` API 能否独立复用（影响**复用方式**：直接调用 vs 移植算法），不影响"是否引入"——后者已锁定。
- **元测试防检查器自身 bug**：向检查器喂已知非一致历史（构造的坏历史用例），必须报错；且小历史上自建检查器与 stateright 参照实现交叉验证一致。

### T3 有界模型检查：stateright 0.31.0（安全性质 only，小范围）

对**设计的抽象模型**（非真实代码）做 3 节点、有界日志的有界模型检查：选举安全、日志匹配、状态机安全。产出是"设计层反例或无可达反例"的独立证据。**明确的维护代价**：模型与设计文档的漂移由设计变更 checklist 挂钩（改 §5 必须过 model-check）；只查安全，不查活性（有界模型查活性会产生大量假反例）。

### T4 并发测试：loom 0.7.2（sync）+ shuttle 0.9.3（async）

- `loom` 仅用于无锁 `std::sync`/原子（WAL 段管理、会话表 LRU、apply 通道内的小无锁结构）；loom 无异步运行时、README 自述存在健全性缺口——**不做网络/崩溃建模**。
- `shuttle` 用于异步任务竞争的随机化探索（apply 任务 vs tick 任务 vs 读等待队列），绑定种子复现。与 turmoil 互斥使用（一个测试域一个调度器）。

### T5 模糊测试：cargo-fuzz 0.13.2 + arbitrary 1.4.2

Target：① WAL 记录解码（逐字节截断/位翻转变异 → 断言 INV6 的"合法截断或 fail-start"）；② RPC 消息解码；③ 快照 meta 解码；④ 握手消息解码。**libFuzzer 入口是同步的**——fuzz target 内自建 `tokio::runtime::Runtime` + `block_on`，禁 `#[tokio::main]`。

### T6 存储一致性 Suite：移植 openraft `testing::log::Suite` 模式

把 raft-rs `Storage` 契约（entries/term/first_index/last_index/snapshot/Compacted 语义）做成**可对任意实现运行的测试电池** `arachne-testsupport::store::Suite`：真实 WAL 实现、FaultyStorage 各故障档、以及未来若 D1 决议翻转引入 redb 时的第三实现——同一套 Suite 全绿才准接。raft-rs 仓库自带 `harness/`（内存 Network、Interface、failpoints_cases、datadriven）**作为模式复用，不作为依赖**（它是 repo-only，且测的是内核不是我们的集成）。

### T7 状态性属性测试：proptest 1.11 + proptest-state-machine

会话表（注册/刷新/GC/grace 淘汰）与 WAL 段状态机用 `ReferenceStateMachine`/`StateMachineTest` 建模，生成操作序列跑 L0。proptest-lockstep 0.1.1 备选（step 并发模型），暂不引入。

## 5. 确定性保障清单（一等公民）

**原则：L2 的同种子双跑必须逐字节复现（故障调度 + 各节点 apply 序列哈希 + 判定结果）。任何复现失败 = 确定性缺陷 = P0 bug，不允许白名单。**

| # | 熵源 | 出现位置 | 封堵方法 | 验证 |
|---|---|---|---|---|
| E1 | tokio 内部 RNG（任务唤醒抖动等） | 运行时 | sim 构建用 `tokio_unstable` + `Builder::rng_seed(seed)`（覆盖范围与最低版本为 M0 spike 项 §13-S3） | 双跑门禁 |
| E2 | `futures_util::select!` 的进程级 thread-local RNG（openraft 曾为此 fork futures-util 加 `reseed()`） | 任何多路等待 | **代码库禁用 `futures::select!`**（CI grep/clippy 门禁）；改用 `futures::future::select`/`poll_fn` 手写选择；tokio 的 `select!` 若使用须加 `biased;`（其分支轮询默认也走 thread-local RNG——spike 核验，§13-S2） | grep 门禁 + 双跑 |
| E3 | raft-rs 选举超时抖动的 RNG | `raft` crate 内部 | M0 spike 核验是否可注入/播种（§13-S1）；**若不可注入：workspace `[patch.crates-io]` 携带最小一行改动注入 RNG（可上游化）**。这是本清单最大的未知项，M0 第一周必须落地 | 双跑门禁（补丁后全量复现） |
| E4 | HashMap/HashSet 迭代序 | 复制状态机、会话表、对等节点遍历 | 复制路径与消息发射顺序一律 `BTreeMap`/有序遍历；map 迭代序进 INV3 哈希即暴露 | INV3 双跑哈希 |
| E5 | `Instant`/`SystemTime` 直接调用 | TTL、超时、GC | `Clock` trait；GC 时间戳走日志（§2.3 已定）；CI grep 禁令（测试支撑模块除外） | grep 门禁 |
| E6 | `getrandom`/UUID（client_id 生成） | handle 创建 | sim 中 client_id 由测试注入固定值；生产路径不进 sim | 场景固定客户端名册 |
| E7 | 端口/地址分配 | 传输层 | turmoil host 名解析，不绑真实端口 | — |
| E8 | 任务调度（多线程 runtime） | 全部 | L2 使用 current-thread runtime + turmoil 调度；多线程仅 L3/L4 | 双跑门禁 |
| E9 | 故障调度本身 | 场景 | 场景 = 纯函数 `fn(scenario_ctx, seed) → 故障脚本`；调度由 turmoil 按种子派生并记录进 RunManifest | `--reproduce` 重放 |

**双跑复现门禁**：每个 L2 场景 CI 中以同一种子连跑两次，比对 ①故障调度事件序列、②各节点 apply 序列哈希、③预言机判定。差异即确定性泄漏，定位到 E1–E9 之一。

## 6. 测试替身契约

### 6.1 SimNetwork（turmoil 包装，`arachne-sim`）

| 原语 | 语义 | 主要服务的场景 |
|---|---|---|
| `partition(a, b)` / `partition_oneway(a→b)` | 双向/单向断链（非对称分区用于 S16：leader 在多数侧但对端视角失联） | S02/S16 |
| `repair(a, b)` | 恢复链路 | 两阶段模糊器的 liveness 相 |
| `hold(m)` / `release(m)` | 在途消息滞留/放行（跨 term 旧消息送达） | S15 |
| `set_fail_rate(p)` | 按概率丢包（种子化） | 全场景背景噪声 |
| 链路 latency/jitter | 每链路独立配置 | Lan/Wan profile 对照 |
| `crash(host)` | 进程崩溃：任务全灭 + **未 sync 写丢失** | S01/S08/S11/S19 |
| `bounce(host)` | 以**已 sync 数据**重启 | 所有重启类场景 |
| 消息重复/变更 | turmoil 若无内建钩子（§13-S5 核验），由 `Transport` 测试适配器在收发路径自行注入重复/乱序/变异 | S07、fuzz 联动 |

**契约**：故障只允许通过本层注入；场景是 `(seed, scenario_id) → 故障脚本` 的纯函数；脚本内每个事件带模拟时刻，可序列化进 RunManifest。

### 6.2 FaultyStorage（`WalStorage`+`StateMachine` 缝的包装器）

**故障操作**：`fail_next_fsync(err)`、`fail_fsync_every(n, err)`、`torn_write(seg, keep_bytes)`（撕裂写：最后一条记录只落部分字节）、`flip_bits(seg, range)`（位翻转/损坏）、`truncate_wal(index)`、`corrupt_meta()`、`corrupt_snapshot()`、`slow_fsync(ms)`。

**fsync 台账（INV1 的强制机制）**：每次 fsync 成功递增 `fsync_seq` 并推进 per-segment `synced_until` 水位；SimNetwork 记录每条消息发出时刻的载荷覆盖区间与当时 `fsync_seq`。**事后对账**：对每条发出的消息 M，断言 M 载荷条目区间 ⊆ 发送时刻已 fsync 区间（I2）；对每次 term/vote 变更消息，断言 HardState fsync 先于发送（I1）；快照回执先于 meta fsync（I3）。`crash` 语义 = 丢弃台账水位之后的一切（与 turmoil `crash` 对齐；若 unstable-fs 记账不可用，则由 FaultyStorage 自身台账供能——spike §13-S4 定）。

**Torn write 语义注记**：WAL 是自有格式，记录级撕裂写必须由 FaultyStorage 在**我们的格式层**表达（sim FS 最多给你文件级截断），这也是磁盘故障不依赖 turmoil FS 的原因。

### 6.3 ProcessBoundary（崩溃 = 真实重启路径）

`bounce(host)` 后的节点必须执行与生产完全一致的启动序列：flock → META 校验 → 快照加载 → WAL 回放（§5.5.3 全分支）→ raft init。**禁止**测试特化路径跳过恢复。反空转断言：启动后内存态（日志缓存、会话表、read_index 等待队列、提案队列）哈希 == 从盘重放期望哈希；若某测试在 bounce 后直接复用旧内存句柄，框架直接判失败（`ProcessBoundary` wrapper 在 harness 层持有唯一构造入口，天然强制）。

### 6.4 ClientOracle（预言机）

- **历史记录**：单 sequencer 线程记录 invoke/complete（实时序全序），JSONL 格式（§9）；同一 `(client_id, seq_no)` 的重试在喂给检查器前合并为单次调用（对应 §2.2 G1）。
- **检查项**：① 幻值（读到从未写的值）；② 每 seq 至多一个 log-id（一个写只进一次日志）；③ 会话内 read-your-writes；④ 线性一致 `get` 的单调读（每 client）；⑤ 前驱链/缺失下界（版本化值考古，detect丢写）；⑥ 崩溃恢复后的持久性扫描（所有已 ack 提交必须在场——直接对应 N4 之外的 G1/G2）。
- **注入能力**：重试风暴（同 seq 重复 propose）、超时后重试、会话过期后新会话重发（触发 N2 语义验证）。
- **规模控制**：PR 级 4 客户端 × 500 ops（仅预言机）；nightly 线性化完备检查 8 × 2000（自建 Wing–Gong，缩减历史）。

## 7. 不变量与预言机

INV1–INV6 承接上游 §9.2 并细化，INV7–15 借鉴 openraft 不变量清单（各条注明 Raft 论文 Ongaro § 出处）：

| # | 不变量（精确陈述） | 检测方法 | 强制层 |
|---|---|---|---|
| INV1 | 任何消息发出前其载荷已 fsync（I1/I2）；快照回执前 meta 已 fsync（I3）；apply 不超前持久化（I4） | FaultyStorage fsync 台账 × SimNetwork 发送日志事后对账 | L2 每次运行 |
| INV2 | 任意 ready 阶段注入崩溃，重启后 `HardState.commit` 及之前的条目全部在场且 apply 结果一致 | crash 边界扫描（ready 各阶段逐点注入）+ 持久性扫描 | L2 + L4 |
| INV3 | 状态机确定性：同一日志前缀在任意节点产生相同状态哈希（KV + 会话表 + grace 区） | 双跑比对 + 跨节点周期性哈希互查 | L1/L2 |
| INV4 | 任意分区/崩溃历史下 `put`/`get` 线性一致（重试按 G1 合并） | ClientOracle + 自建 Wing–Gong 检查器（缩减历史） | L2 |
| INV5 | 会话去重：同 `(client_id, seq_no)` 恰好一次生效；`SessionExpired` 后的重复执行被限制在 ttl+grace 窗口（N2） | 重试注入 + oracle one-log-id-per-seq | L2 + L0 属性测试 |
| INV6 | WAL 恢复永不静默丢弃 ≤ `HardState.commit` 的数据：任意变异落在"合法前缀截断"或"fail-start"两象限 | WAL 逐字节截断/位翻转变异 fuzz + 恢复算法判定 | L0 fuzz + L4 |
| INV7 | 选举安全（Ongaro §5.2/§8）：每 term 至多一个 leader | 事件记录 (term, leader) 无重复 | L1/L2 |
| INV8 | 日志匹配（§5.3）：任意两节点在重叠 index 上 (term, 条目) 相同 | 场景后全节点日志前缀比对 | L1/L2 |
| INV9 | Leader completeness（§5.4.1）：新 leader 含全部已提交条目 | 每次换主后按 oracle 已提交集核对 | L2 |
| INV10 | 状态机安全（§5.4.2）：同 index 同 apply 结果，**含会话表去重判定一致**（INV15 并入此域） | 跨节点状态哈希互查 | L2 |
| INV11 | Commit-on-majority：已 ack 提交在 ≥ 多数派在场（synced） | oracle 提交集 vs 台账水位扫描 | L2 |
| INV12 | 提交不可回退：commit index 跨重启不减小 | 崩溃前后 HardState 断言 | L2/L4 |
| INV13 | 单调性：每节点 term/vote/commit/applied 随时间与跨重启单调 | 状态转移日志断言 | L1/L2/L4 |
| INV14 | 失权 leader 不服务线性读：ReadIndex quorum 轮失败即拒（`QuorumUnavailable`/`NotLeader`） | 非对称分区场景 + oracle | L2 |
| INV15 | 会话表跨节点一致（快照含会话表，§5.5.4） | 并入 INV10 哈希域 | L2 |

**fail-stop 纪律断言**：任何不变量违反时进程必须 abort（§3.2），测试同时断言"违规 → abort"路径本身可达（错误处理不为空转）。

## 8. 场景矩阵

承接上游 §9.3 清单并展开。两阶段模糊器模型（借鉴 openraft `tests-turmoil` 范式）：**safe phase**（客户端负载 + 成员变更 + 网络/进程/磁盘混沌，N tick）→ **liveness phase**（修复一切故障 → 有界时间内必须：选出 leader → 日志/状态收敛 → 线性读写恢复 → 持久性扫描通过）。liveness 失败即缺陷，无论 safe phase 发生了什么。

| ID | 场景 | 注入 | 期望行为 | 断言不变量 | 层 |
|---|---|---|---|---|---|
| S01 | 选举中旧 leader 复活 | crash(bounce) leader + PreVote 窗口 | 复活者不抬 term 抢回；至多一 leader | INV7/INV4 | L2 |
| S02 | 双分区（2+1 / 3+2） | partition | 少数侧写 `QuorumUnavailable`、线性读拒、`get_stale` 可用且弱于 N1 | INV4/INV14 | L2 |
| S03 | follower 追赶中 leader 切换 | 慢链路 + crash leader | 快照路径追赶完成，无日志分叉残留 | INV8/INV9 | L2 |
| S04 | WAL 尾部损坏（commit 之后） | torn_write 尾部 | 自动截断到合法前缀 + 从 leader 追赶 | INV6/INV13 | L2+L4 |
| S05 | WAL 损坏侵入已提交区间 | flip_bits ≤ commit | **fail-start（Unrecoverable）**，绝不静默丢 | INV6 | L0+L2+L4 |
| S06 | HardState 记录损坏 | corrupt hardstate 记录 | fail-start（term/vote 不可重建） | I1/INV13 | L0/L2 |
| S07 | 客户端重试风暴 | oracle 注入同 seq 重复 + 超时重试 | 恰好一次生效 | INV5/G1 | L2 |
| S08 | Learner 追平瞬间断电 | crash learner during catch-up/snapshot install | 恢复后继续追赶，集群多数派不受影响 | INV8/INV10 | L2 |
| S09 | 快照传输中断 | install 中 crash / 链路切断 | tmp 未污染正式快照（I3）；重传或重建 | I3/INV10 | L2 |
| S10 | 会话 GC 与重试竞态 | 模拟时钟推进过 TTL 后重试 | `SessionExpired` 结果未知语义，窗口 ≤ ttl+grace（N2） | INV5 | L2 |
| S11 | 磁盘满 / fsync 失败 | fail_fsync_every | 失败节点 fail-stop；多数派存活则集群继续 | I1/I2 + fail-stop | L2 |
| S12 | 滚动升级混版 | protocol_version 混合矩阵 | 握手拒绝/兼容规则（§5.6） | — | L3 |
| S13 | force-recovery 后旧多数派复活 | bounce 旧成员 + 旧 cluster_id | 握手互拒，无同 ID 双集群（§6.1） | — | L2 |
| S14 | 时钟跳变 × TTL | 模拟时钟前跳 | lease 过期为弱保证（N6），一致性不变 | INV5 | L2 |
| S15 | 跨 term 旧消息滞留 | hold → 换主 → release | 旧 term 消息被拒，无状态污染 | INV8/INV13 | L2 |
| S16 | 非对称分区 | partition_oneway | CheckQuorum step-down + ReadIndex 拒读正确 | INV7/INV14 | L2 |
| S17 | 首启初始化竞态（§5.7） | 3 节点并发首启 / initial_cluster 不一致 | 语义符合 §5.7（fail-start/WARN） | — | L2 |
| S18 | 快照安装期间本地读 | install 中 `get` | 短暂 `Busy`，无部分状态可见 | INV10 | L2 |
| S19 | kill -9 任意 ready 阶段边界（真实盘） | L4 注入器逐阶段 kill | INV2 在真实 fsync 语义下成立 | INV2/INV6/INV13 | L4 |
| S20 | 升级矩阵 v(n-1)↔v(n) 滚动 | L3 混版运行 | 全程可写，握手规则成立（§5.6） | — | L3/Release |

固定种子集：PR 门禁跑 S01–S12（各 3 种子）；nightly 全矩阵 × 随机种子采样。

## 9. 测试数据结构与结果

- **RunManifest（每次 L2/L3/L4 运行产出）**：`{scenario_id, seed, git_rev, profile, fault_schedule[], host_list, binary_hash, chaos_phase_ticks}`——种子 + 场景即可完全重导出调度。`binary_hash` = sha256(bin 产物) + `git_rev` + feature 标志（`fault-hooks` 有/无），复现时据此重建同一二进制；L3/L4 由 `arachne-testsupport::proc` 生成（§3.3）；S12 升级矩阵记录 v(n−1)/v(n) 两个 hash 并断言混版拓扑按上游 §5.6 握手规则运行。
- **历史**：oracle 产出 JSONL `{client_id, seq, op, key, val, invoke_ts, complete_ts, result, log_id?}`；重试合并规则在记录层保留原始多次调用（供调试），喂检查器时合并。
- **复现**：`cargo run -p arachne-sim -- repro --manifest run.json`（同种子重放，双跑门禁同路径）。
- **失败产物保留**：WAL/快照目录副本、fsync 台账、sim 事件日志、历史 JSONL、不变量违规报告（指明 INV 编号 + 最小反例历史摘录），保留 30 天，路径 `artifacts/<run_id>/`。
- **不变量违规报告格式**：`[INV#] 场景 种子 节点 最小反例片段`——保证可一键转回归用例（固定该种子入场景注册表）。

## 10. CI 门禁与运行预算

| 门禁 | 内容 | 预算 | 频率 |
|---|---|---|---|
| PR | L0 单元 + T6 存储 Suite + loom 快集 + shuttle 快集 + L2 固定场景 S01–S12（各 3 种子，<90s 墙钟）+ **双跑复现金丝雀（3 种子）** + grep 门禁（禁 `futures::select!`、禁 sim 外 `std::time`/`tokio::net`） | ≤ 10 min | 每 PR |
| Nightly（分片 ≤4h） | L2 全矩阵 × 100 随机种子；线性化完备检查（缩减历史 8×2000）；shuttle 随机 30min；fuzz 4 target × 10min；stateright 有界模型检查 15min；L3 多进程冒烟 15min | ~4h，8 分片并行 | 每夜 |
| Release | 混沌矩阵全绿 ×3 种子；**L4 真机 kill -9 循环 30min + WAL 校验脚本**；升级矩阵（S12）；延迟冒烟（M2 预算） | ~1 天 | 发版前 |

**L4 专用 runner（决策 D-L4，已锁定）**：带真实磁盘的 CI runner **现在即立项建设**——M0 完成脚手架（runner 配置 + WAL 校验脚本 + kill -9 注入器），M1 起跑冒烟，M4 出全量门禁。"发版前手工执行"与"延后到 M2"两个备选已在评审中**否决**：fsync 真实语义的复验不可替代（§2 L4 行），且手工执行不可重复、不可归因。

**Flake 政策**：L0–L2 任何 flake = 确定性缺陷（P0），禁止"重跑到绿"；L3/L4 涉及真实环境，允许一次重跑并强制记录原因。PR 门禁红即禁合并——"混沌清单全绿"是 M4 验收，不是 nightly 的可选项。

**`test-observability` 与依赖隔离门禁（D-ART-testobs / D-ART）**：CI 强制 release/发布构建**排除** `test-observability` feature（feature 解析检查 + 发布产物 hash 断言）——该 feature 仅追加结构化 marker 日志、零行为改变，是"无测试代码入生产"规则的唯一记录在案例外（§3.3）；PR 门禁另跑 `cargo-tree` 检查，确保测试工件（`arachne-testsupport`/`arachne-sim` 及 turmoil/shuttle/stateright 等）不进入任何生产 crate 的 `[dependencies]`。

## 11. 覆盖矩阵与缺口

**设计保证 → 测试追溯**（§ 编号指 propsol-v0.2.7，节号与 v0.2.2 相同）：

| 设计保证 | 覆盖测试 |
|---|---|
| §2.2 G1 恰好一次 | INV5 + S07/S10 |
| §2.2 G2 线性一致 | INV4 + S01/S02/S16 |
| §2.2 G3 会话表全量复制 | INV10/INV15 + S09 |
| §2.2 N1–N3/N6 弱语义边界 | S02（stale 可用）、S10（过期窗口）、oracle 负面断言 |
| §3.3 错误矩阵 | 各场景断言错误码；PR 单元表驱动逐格覆盖 |
| §4.2 I1–I4 | INV1（台账对账）+ INV2 |
| §5.1 选举 | INV7 + S01/S16 |
| §5.2 复制/流控 | INV8/INV9 + S03 |
| §5.3 成员变更 | M3 场景 + INV7–10 + ConfChangePending 单测 |
| §5.4 ReadIndex | INV14 + S02/S16 |
| §5.5 存储/恢复 | INV2/INV6/INV12/INV13 + S04–S06/S09 + T6 Suite |
| §5.6 版本握手 | S12 |
| §5.7 引导 | S17 |
| §6 force-recovery 与启动校验 | S13 + `arachne-node` bin 集成测试（§3.3：启动校验/flock/META 校验/优雅关闭/force-recovery 前置检查与双确认） |

**诚实缺口清单（无法/不打算在此体系内证明）**：

1. **真实磁盘坏扇区/静默腐蚀**超出 FaultyStorage 变异模型——L4 只覆盖 kill -9 与常见撕裂；企业级 E2E 校验（scrub）不在此范围。
2. **真实内核栈行为**（半开连接、缓冲回压、TLS 重协商）仅 L3 小集冒烟，无确定性。
3. **性能/p99 SLO**：仅 M2/M4 冒烟预算门槛，完整基准另立方案。
4. **5 节点拓扑**在 L2 成本高，仅抽样场景覆盖；默认矩阵以 3 节点为主。
5. **升级矩阵只测相邻版本**，任意版本对组合爆炸不测（§5.6 已声明）。
6. **fuzz 深度**有限（每 target 10min nightly），不声明"无解码漏洞"。
7. **时钟漂移对 TTL**仅模拟跳变，不覆盖真实 NTP 异常。
8. **自建线性化检查器自身的正确性**——以 stateright 参照交叉验证 + 坏历史元测试缓解（T2；stateright 引入已锁定为 D-T2），但这是新代码，存在残余 bug 风险。

## 12. 里程碑映射（对齐上游 §10）

| 里程碑 | 本方案交付 | 支撑的验收项（上游 §10） |
|---|---|---|
| **M0** | 全部接缝 trait（Transport/Clock/Rng/存储）+ FaultyStorage + fsync 台账 + L1 harness（raft-rs harness 模式）+ T6 Suite + fuzz target ①（WAL 变异）+ **双跑复现门禁上线** + §13 spike 清单关闭 + **L4 专用 runner 脚手架（D-L4：runner 配置 + WAL 校验脚本 + kill -9 注入器）** + stateright dev-dependency 接入（D-T2）+ **工作区六工件骨架（D-ART，D-ART-rev1 增 `arachne-seam` 叶 crate）与 `arachne-node` 单节点可运行（/readyz、--config）+ examples 编译门禁** | 上游 M0 验收 ①–④ 全部依赖此处基建 |
| **M1** | turmoil L2 骨架 + SimNetwork 全原语 + ClientOracle v1（G1/G2 判定）+ INV3/4/7/8/9 + S01/S02/S16 + **L3 冒烟（`arachne-node` ×3 真实进程）+ bin CLI 集成测试（§3.3）** | M1 验收 ④（线性化判定） |
| **M2** | 快照/追赶场景 S03/S09/S18 + INV10–INV14 + stateright 有界模型检查 + 延迟冒烟基准 | M2 验收 ①–⑤ |
| **M3** | 成员变更/会话场景 + INV5/INV15 + shuttle 全集 + proptest-state-machine | M3 验收 ①–⑥ |
| **M4** | L3 多进程 + L4 kill -9 门禁全量（专用 runner，D-L4）+ 升级矩阵 + 混沌矩阵全绿 ×3 种子 + force-recovery 场景 S13 | M4 验收 ①–⑤ |

**硬规则**：测试基建是 M0 交付物本身（上游文档自有建议），M0 验收未含基建即视为 M0 未完成。

## 13. M0 spike 待核验清单（带默认决策）

S2–S5 为待核验 API 事实：M0 第一周以最小 spike 钉死，每项附默认决策（核验失败即采用默认，不阻塞）。**S1 与 S6 的"是否"已被评审锁定（D-S1/D-T2），spike 只核验实现机制，不重开决策**：

| # | 待核验项 | 默认决策（若核验不利） |
|---|---|---|
| S1 | `raft` 0.7.0 选举超时 RNG 是否可注入/播种 | **已锁定（D-S1）**：若核验确认不可注入，即采用 workspace `[patch.crates-io]` 携带一行 RNG 注入改动，**以上游化为目标**，补丁进 cargo-deny/vendor 审计清单跟踪；双跑门禁在补丁后全量生效。spike 只核验"是否需要补丁"，不重开"要不要补丁" |
| S2 | `tokio::select!` 无 `biased;` 时分支轮询 RNG 是否可被 `rng_seed` 覆盖 | 一律 `biased;` 或禁用宏，改手写选择 |
| S3 | tokio `Builder::rng_seed` 覆盖范围与最低版本 | 覆盖不足的熵源（如 watch 唤醒）→ 对应原语在 sim 路径替换为确定性封装 |
| S4 | turmoil `unstable-fs` FsCorruption 能力面（撕裂写/位翻转/fsync 失败/记账） | 能力不足 → 磁盘故障全部由 FaultyStorage 台账实现（本方案默认如此设计，不依赖 sim FS 的记账，turmoil FS 仅作补充） |
| S5 | turmoil 消息重复/变更钩子有无内建 | 无 → `Transport` 测试适配器在收发路径自行注入 |
| S6 | stateright `semantics/linearizability.rs` 可否独立作历史检查器复用 | stateright 引入本身**已锁定（D-T2，dev-dependency）**；spike 仅核验复用**方式**：API 可独立调用 → 直接用作交叉验证参照；不可 → 移植其算法进自建检查器（Wing–Gong 主案不变，交叉验证门禁不取消） |

---

*test-plan v0.1 完（v0.1.1 新增决策记录表 D-T1/T2/L4/S1 锁定 + §3.2 工件布局、§3.3 进程级 harness（D-ART）；v0.1.2 锁定 D-ART 四项子决策：crate 命名、默认 feature 拉 tonic、TOML 配置、test-observability 门禁；**v0.1.3 D-ART-rev1：抽出无依赖叶 crate `arachne-seam` 消除包循环**——`arachne-transport-tonic → arachne-seam` 而非 `→ arachne`、`arachne` 重导出接缝保持公共 API 不变、sim/testsupport 对 `arachne` 一律 `default-features = false`）。上游 `propsol-v0.2.7.md` §9 已改为本方案的摘要并指向本文；工具选型修订（madsim→turmoil、porcupine→自建检查器+stateright 参照）记录于上游变更日志 E 条，评审锁定的决策（D-T1/D-T2/D-L4/D-S1/D-ART 及其四项子决策）记录于上游变更日志 F/G/H 条与本文顶部决策记录表。*
