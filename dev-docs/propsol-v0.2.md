# Arachne 设计文档（v0.2.10 RFC）

> 上游文档：`propsol.md`（v0.1）。本文件在其基础上做需求/设计精化，**不包含实现代码**。
> 设计基线不变：复用成熟共识内核（`tikv/raft-rs`，备选 `openraft`）、CP 语义、不自研共识、失去多数派不自动接管、不基于 ACK 超时自动剔除节点。

---

## 变更日志（v0.1 → v0.2.10）

### A. 三项待决策项已决议（见 §11）

| # | 决策 | 结论 | 一句话理由 |
|---|---|---|---|
| D1 | 状态机存储 | **v1 全内存 + WAL + 快照** | 注册中心数据量小；`redb` 会引入第二套事务边界（双日志 + fsync 叠加），复杂度不成比例。以**硬性容量上限 + 超限拒绝写入**对冲 OOM 风险 |
| D2 | 传输 | **tonic（gRPC over HTTP/2 + rustls mTLS）**，藏在 `Transport` trait 之后 | 手写帧协议意味着自己维护 TLS、背压、流式快照、版本协商——高风险零收益；trait 边界使生产实现（tonic）与测试实现（turmoil 模拟网络）可互换 |
| D3 | Watch/TTL | **TTL（lease）提前至 v1.5，Watch 留 v2** | TTL 可完全复用 v1 的会话表 + 日志驱动的过期扫描（过期即 propose 删除），无新一致性问题；Watch 需要 revision/事件历史/compaction/断点续传，是独立子系统 |

### B. 修正 v0.1 中不安全或含糊的表述

1. **fsync 策略修正（安全性）**：v0.1 的 `FsyncPolicy::BatchMs` 若作用于 HardState 是**不安全的**——崩溃后丢失 `(term, voted_for)` 可能导致同一 term 内双投票，破坏 Raft 安全性。v0.2 规定：**HardState 永远逐次 fsync；BatchMs 只允许作用于日志条目批量**（§5.5 不变量 I1）。
2. **WAL 尾部截断的安全边界修正**：v0.1 "损坏则截断到最近合法条目" 无条件成立是错的。仅当**损坏点位于最后一次持久化 commit index 之后**才允许自动截断；损坏覆盖已提交区间必须 fail-start，交由运维决策（§5.5 恢复算法）。
3. **"追赶不上转 Learner" 表述修正**：降级/移除本身是 ConfChange，需要多数派投票，**系统不自动执行**（与 §1 非目标一致）；系统只做告警，由运维显式操作（§5.2）。
4. **快照必须包含会话表**：v0.1 未说明。若快照缺失会话表，follower 安装快照后去重信息丢失，客户端重试可能重复生效（§5.5）。
5. **会话 GC 必须走日志且禁止读本地墙钟**：状态机 apply 必须确定性，时间戳由 leader 写入日志条目携带（§2.3）。
6. **读路径补全**：`get`（ReadIndex）在 follower 上也必须转发；quorum 心跳轮失败时读返回 `QuorumUnavailable`（v0.1 只写了写的失败行为）（§5.4）。
7. **成员变更补全硬约束**：同一时刻至多一个未提交 ConfChange；移除 leader 必须先转让；跨 cluster_id 节点拒绝加入（§5.3）。

### C. v0.1 完全缺失、v0.2 新增的契约

- **集群引导/初始化规范**（首启 `initial_cluster`、cluster_id 生成与校验、META 文件）——§5.7
- **每操作 × 条件的完整错误矩阵** 与 **"结果未知"边界清单** ——§3.3、§2.4
- **幂等会话完整生命周期**（创建/TTL 刷新/GC/内存上限/过期中重试语义）——§2.3
- **背压与资源上限总表**（提案队列、inflight、会话表、WAL 增长、快照限速）——§4.1
- **协议版本握手与滚动升级规则**（major/minor 兼容矩阵、WAL 格式冻结、回滚限制）——§5.6
- **force-recovery 的精确算法、前置条件与防脑裂措施**（默认更换 cluster_id）——§6
- **可观测性缺口补全 + 告警信号表** ——§8
- **测试策略落地**（harness 选型、可执行不变量清单、CI 门禁、明确列出难测项）——§9
- **里程碑可证伪验收标准 + 依赖关系** ——§10
- **假设（§12）、风险登记（§13）、决策记录（§14）**

### D. v0.2.1：原 §14 七项开放问题经评审**全部决议锁定**

决议已传播至各自生效章节（无遗留开放设计问题）：

| # | 决议 | 落点 |
|---|---|---|
| Q1 | `session_ttl` = 60s；`client_id` 不跨进程持久化（进程重启 = 新会话，即 N5） | §2.3、§7 |
| Q2 | ReadIndex 超时 = `2×election_timeout`，重试 1 次 | §5.4、§7 |
| Q3 | `transfer_leader` 作为公开 API 暴露 | §3.1、§5.3 |
| Q4 | 快照期间阻塞 apply + `snapshot_last_duration > 1s` 告警；容量逼近内存上限时 v1.1 重审双缓冲 | §5.5.4、§8.2 |
| Q5 | v1 不提供 per-handle 单调读变体；仅文档声明 `get_stale` 非单调 | §2.1、§5.4 |
| Q6 | `wal_trailing_keep` 与 `snapshot_threshold` 绑定同值（可显式覆盖解耦） | §5.5.4、§7 |
| Q7 | 提案队列按字节计容，64MB | §4.1、§7 |

### E. v0.2.2：测试基建选型修订 + 集成测试方案独立成文

经工具调研核验（2026-09），**修订 §9.1 的确定性模拟选型：madsim → turmoil 0.7.2 为主选**，并补全线性化检查器选型。完整可执行方案独立成文：**`test-plan-v0.1.md`**（分层、接缝、确定性保障清单、替身契约、INV1–15、场景矩阵、CI 门禁、里程碑映射）。

| 修订点 | v0.2.1 表述 | v0.2.2 决议 | 理由 |
|---|---|---|---|
| 确定性模拟器 | madsim | **turmoil 0.7.2** | madsim 磁盘故障模拟是 TODO 桩（无撕裂写/损坏/记账），且要求 `madsim-tokio`/`madsim-tonic` + `[patch.crates-io]`（钉 tonic 0.14）——测的不再是上线代码路径；turmoil 跑**真实 tokio/tonic**、种子化单进程调度，`crash`（丢未 sync 写）/`bounce`（按已 sync 数据重启）与"进程边界必须真实"的测试要求天然对齐，并具备 `unstable-fs` 故障钩子。磁盘故障模型主要落在自有 `FaultyStorage` 缝上，不依赖模拟 FS。同一测试域只用一个模拟器 |
| 线性化检查器 | `porcupine`（Rust 移植） | **自建 Wing–Gong 检查器 + openraft 式客户端预言机**；stateright `semantics/linearizability.rs` 为参照实现并交叉验证 | 无成熟 Rust Porcupine 移植（`porcupine` 0.2.4 是 Win32 API 包装——同名异物；`porcupine-rs` 0.3.0 单作者、零反向依赖，不进 CI 依赖） |
| 并发测试补充 | 仅 loom | loom（sync 无锁结构）+ **shuttle 0.9.3**（async 随机调度） | loom 无异步运行时、不能建模网络/崩溃；异步任务竞争需要 shuttle |
| raft-rs harness | 未提及 | 明确为**模式复用，不作依赖**（repo-only） | 它测单节点内核，不测我们的集成 |

决策记录格式约定（§11 末）同样适用于本条：推翻须追加 E-rev 条目。

### F. v0.2.3：测试方案四项决策经评审锁定（详录于 `test-plan-v0.1.md` 顶部决策记录表）

| # | 决策 | 结论 | 落点 |
|---|---|---|---|
| D-T1 | 确定性模拟器 | **turmoil 0.7.2 确认锁定**（本文件 v0.2.2 的 E 条修订经评审批准）；madsim 保持为记录在案的否决项 | test-plan §4 T1；本文 §9.1 |
| D-T2 | 线性化检查器 | **stateright 0.31.0 以 `dev-dependency` 锁定引入**（T3 模型检查引擎 + 自建 Wing–Gong 检查器的交叉验证参照）；"去掉参照检查器"不是选项——检查器自身正确性门禁依赖它 | test-plan §4 T2/T3、§11；本文 §9.1 |
| D-L4 | L4 真机持久性门禁 | **专用真实磁盘 CI runner 现在即立项建设**（M0 脚手架、M1 冒烟、M4 全量）；"发版前手工执行"与"延后到 M2"两个备选已否决——fsync 真实语义复验不可替代且必须可重复 | test-plan §2/§10/§12；本文 §9.4、§9.5 |
| D-S1 | raft-rs 选举 RNG 不可注入时的响应 | **锁定：workspace `[patch.crates-io]` 一行 RNG 注入补丁，以上游化为目标**，补丁进 cargo-deny/vendor 审计清单；S1 spike 只核验"是否需要补丁"，不重开"要不要补丁" | test-plan §5 E3、§13 S1；本文 §9.5、§13 R3 |

### G. v0.2.4：工件与工作区决策（D-ART）——补齐 L3/L4 的可执行载体

**缺口**：test-plan 的 L3/L4 层、升级矩阵（S12/S20）、RunManifest 的 `binary_hash`、force-recovery 的 CLI 集成测试，都预设一个**可运行的节点二进制**，而本文 §3 只定义了库 API——测试方案在 L3/L4 层实际不可执行。

| # | 决策 | 结论 | 理由 |
|---|---|---|---|
| D-ART | 工件与工作区 | 五工件布局：`arachne`（lib 产品主体，transport-agnostic 核心）+ `arachne-transport-tonic`（生产传输，唯一 tonic/rustls 所在 crate，默认 feature 引入）+ **`arachne-node`（薄运维 bin，生产交付物）** + `arachne-node` 之外的测试工件（`arachne-testsupport` 仅 dev-dependency、`arachne-sim` 测试 bin）+ `examples/`（完整可运行，CI 编译门禁） | ① force-recovery（§6.1）与成员运维（§3.1）本就是生产接口 → bin 是一等交付物，不是测试装置；② L3/L4 需要可 spawn 进程，且必须与嵌入者运行**同一份 lib 代码**（bin 仅装配，禁止第二套逻辑）；③ 测试依赖不泄漏进生产树（cargo-tree 门禁）；④ sim 构建不编译 tonic（更快且杜绝 `std::net` 泄漏） |

**否决项（记录在案）**：test-only 专用 main（测试路径与生产路径分叉）；用 example 充当进程 harness（示例的职责是 API 工效学，不是进程 harness）。布局细节与进程 harness 机制见 `test-plan-v0.1.md` §3.2/§3.3；落点：本文 §1/§3.1/§4/§6.1/§7/§9.4/§10，test-plan §2/§3.2/§3.3/§9/§10/§11/§12。

### H. v0.2.5：D-ART 四项子决策经评审锁定（crate 命名 / 默认 feature / 配置格式 / test-observability）

| # | 决策 | 结论 | 落点 |
|---|---|---|---|
| D-ART-names | 工件与 crate 命名 | **锁定**：`arachne`（lib）、`arachne-transport-tonic`、`arachne-node`（bin）、`arachne-testsupport`、`arachne-sim`，外加 `examples/`、`fuzz/`、`model-check/` | 本文 §4 工件边界；test-plan §3.2 |
| D-ART-feature | `arachne` 默认 feature 拉入 tonic | **锁定**：默认 feature `transport-tonic` 经 `arachne-transport-tonic` 拉入——嵌入者单依赖即得完整栈；代价是默认依赖树更重（tonic/rustls），精简核心可用 `default-features = false`，sim 测试构建一律 `default-features = false` | 本文 §4；test-plan §3.2 |
| D-ART-config | 节点配置文件格式 | **锁定 TOML**：`arachne-node --config node.toml`（字段 = §7 Profile + 覆盖项 + §5.7 `initial_cluster`）；CLI 单项覆盖为辅；测试 harness 复用同一配置面，不引入第二格式 | 本文 §7；test-plan §3.3 |
| D-ART-testobs | 生产 bin 的 `test-observability` feature | **锁定为"无测试代码入生产"规则的最小例外**：`arachne-node` 可带该 feature（仅追加结构化 marker 日志、零行为改变）；**CI 强制 release/发布构建排除该 feature**（feature 解析 + 发布产物 hash 断言） | 本文 §4/§9.4；test-plan §3.3/§10 |

评审结论：四项均按推荐案一致通过。test-plan §13 的 S2–S5 仍为待核验 **API 事实**（其对应决策已锁定，非开放设计问题）。

### I. v0.2.6：D-ART-rev1 —— 抽出 `arachne-seam` 叶 crate，消除包循环

| 修订点 | v0.2.4/v0.2.5 表述 | v0.2.6 修订 (D-ART-rev1) | 理由 |
|---|---|---|---|
| 工件布局 | 五工件 | **六工件**：新增叶 crate `arachne-seam`（接缝 trait + 核心类型，无依赖） | 原 §3.2 的 `transport-tonic → arachne` 与 D-ART-feature 的 `arachne → transport-tonic` 构成 cargo **包循环**（实测硬拒绝）；叶 crate 使"接缝归核心、实现归传输 crate"的原意可落地 |
| 传输 crate 依赖 | `arachne-transport-tonic → arachne` | `arachne-transport-tonic → arachne-seam` | 断环；公共 API 经 `arachne` 重导出保持不变 |
| sim/testsupport 构建 | 默认 feature 隐含拉 tonic | `default-features = false`；testsupport 永不含 tonic，sim 的 tonic 由 L2 harness 显式启用 | 落实 D-ART ④"sim 构建不编译 tonic"，消除文档内部矛盾 |

### J. v0.2.7：§5.5.3 恢复算法安全收紧（E-rev）

P2 崩溃安全门禁评审发现 §5.5.3 的恢复算法**按原文实现不安全**：它靠嗅探记录类型字节分类坏记录，但该字节在 CRC 保护体内、损坏时不可信——类型字节翻成 `0x01` 的损坏 HardState 会被误读为 Entry 而自动截断，丢 `term`/`vote`（同 term 双投票破 INV7、commit 回退破 INV12）；结构性撕裂分支绕过 commit-window 检查；新段未目录 fsync（I2 漏洞）；重开每次重启新建命名错乱的段。

| 修订点 | v0.2.6 原文 | v0.2.7 修订 | 理由 |
|---|---|---|---|
| 坏记录分类 | 嗅探类型字节判定 HardState/Entry | **不再嗅探**：CRC/解码失败且记录在磁盘上完整存在 → **无条件 fail-start** | 类型字节在 CRC 覆盖范围内，损坏时不可信；误判可丢 term/vote |
| 可截断范围 | 首个坏记录且 i>commit+1 即自动截断 | **仅最后段的结构性撕裂**可自动截断（头不足 8 字节 / `record_end>file_len` / `len` 非法）；且撕裂仍过 commit-window 检查（`estimated_index ≤ commit+1` → fail-start） | 撕裂只可能是未 fsync 的尾部；其余损坏是真损坏 |
| 非最后段坏记录 | 未明确 | **一律 fail-start** | 撕裂只可能出现在最后写入的段 |
| 恢复后校验 | 无 | 增加 **`hard_state.commit ≤ last_index` 断言**，否则 fail-start | 关闭"损坏 len 伪装撕裂"导致已提交数据静默丢失 |
| 新段持久化 | 未提 | 新建段文件后 **fsync 数据目录** | 目录项不持久化会使已 fsync 的条目在掉电后消失（I2） |
| 段命名/滚动 | 段名=段首 log index；未明确重开行为 | 滚动在超 `segment_bytes` 时新建段；**重开续写最高编号既有段**，仅无段时建 `wal-1.log` | 避免每次重启新建空段、破坏"段名=首 index"不变式 |

### K. v0.2.8：§5.1/§7 约束注解自相矛盾修正（E-rev）

M1-1 实现配置管线（`ProfileConfig::validate`）时发现：§7 的**约束注解与其自身预设表冲突**——注解要求 `rpc_timeout < heartbeat_interval`（§5.1 同述）与 `election_timeout ≥ 10× heartbeat`，但预设 Lan 为 `rpc 500ms > hb 100ms`；Wan 为 `el 2500ms < 10×500ms` 且 `rpc 2000ms > hb 500ms`。若按注解校验，两个预设都非法、节点无法启动。

| 修订点 | v0.2.7 原文 | v0.2.8 修正 | 理由 |
|---|---|---|---|
| RPC 超时约束 | `rpc_timeout < heartbeat_interval`（§5.1、§7） | **`rpc_timeout < election_timeout`** | 单次 RPC 须在选举超时前结束才构成可用性约束；`< heartbeat` 无物理依据且与预设冲突 |
| 选举超时约束 | `election_timeout ≥ 10× heartbeat` | **`election_timeout ≥ 5× heartbeat`**（Lan 预设仍为 10×） | Wan 预设为 5×；5× 是预设实际采用的最小比 |
| 预设数值 | 不变 | 不变 | 预设是冻结决策；本次只修**注解**使其与预设一致 |

实现侧已按修正后的约束强制（`el ≥ 5·hb`、`rpc < el`）；本 E-rev 使**文档 / 实现 / 预设**三者自洽。

### L. v0.2.9：INV2 的测试专用崩溃注入点（E-rev，test-only hook）

M2 落地 INV2（"任意 ready 阶段注入崩溃，重启后 `HardState.commit` 及之前的条目全部在场且 apply 结果一致"，§7）时确认：**轮次级** harness 崩溃点（`crash`/`bounce`）只能表达"某一轮结束后崩溃"，无法在 `RaftNode::step` 的 **persist 与 deliver 之间**注入崩溃——而那正是"条目已落盘、尚未传播"这一关键窗口。字节级变异电池（`wal_mutation` / `m2_wal_faults`）与 `FaultyStorage` 分别覆盖磁盘损坏与 fsync 失败，均**不能**表达"进程在此刻死亡"。故需一个测试专用注入点。

| 决策点 | 选择 | 否决的备选 | 理由 |
|---|---|---|---|
| 注入位置 | `RaftNode::step` 内两个阶段边界：`AfterPersist`（entries + HardState 持久化之后、任何消息发出之前）与 `AfterDeliver`（消息发出之后、apply 之前） | ①仅 harness 轮级崩溃；②在 `Storage` 接缝注入 | ①无法表达 persist/deliver 之间；②接缝是逻辑层，只能表达存储错误而非进程死亡 |
| 开关 | crate **feature `fault-injection`，默认关闭**；关闭时调用点被 `#[cfg]` 完全编译掉（零行为改变、零运行时开销） | 运行时 flag / trait 对象 | 运行时开关会在生产热路径留下分支与状态 |
| 状态 | **thread-local、一次性**（armed 后触发一次即清除） | 全局可变状态 | 测试并行运行时互不干扰 |
| "崩溃"语义 | 触发点以固定 payload **panic**，由 harness `catch_unwind` 捕获后丢弃该节点（保留 WAL 目录） | 让 `step` 返回"崩溃"错误（须给 `NodeError` 加变体，改公共 API） | 不改动公共错误类型；"死亡即丢弃内存态"正是要建模的语义 |
| 发布产物隔离 | 同 D-ART-testobs：**不是默认 feature**；`scripts/check-release-features.sh` 断言发布构建不含该 feature（哨兵字符串不出现） | 仅靠"记得别开" | 与既有 `test-observability` 门禁同一纪律 |
| 不建模什么 | 撕裂写（字节级电池负责）、fsync 失败（`FaultyStorage` 负责）、真实掉电时序（L4） | —— | 明确边界，避免把三类故障混为一谈 |

落点：§7 INV2 的"crash 边界扫描"；调用点在 `arachne/src/consensus/node.rs`，feature 模块 `arachne/src/fault_injection.rs`，测试在 `arachne/tests/m2_durability.rs`。

### M. v0.2.10：快照落盘契约、META 快照指针与压缩边界（E-rev）

M2 落地 §5.5.4 的快照与压缩时，需把三处此前只有文字规定、没有落盘契约的地方钉死。

| 决策点 | 选择 | 否决的备选 | 理由 |
|---|---|---|---|
| 快照落盘形态 | **独立文件 `snapshot-<index>-<term>.snap`**：`[u32 magic][u32 format_version][u64 index][u64 term][u32 voters_len][voters u64…][u32 learners_len][learners u64…][u64 data_len][data][u32 crc32c]`，CRC 覆盖其前全部字节（含 header 与成员表）；原子写（tmp → fsync → rename → 目录 fsync） | ①塞进 WAL 的 `Meta(0x03)` 记录；②放进 META 文件 | ①记录上限 64 MiB，快照会超；大记录进段内也拖慢回放。②META 是身份文件，与段无关 |
| "最新快照"判定 | **扫描目录，取 index 最大且 CRC 合法者**；非法则回退更早快照；无更早快照且 WAL 不能从更早完整回放 → fail-start | 仅以 META 指针为唯一判据 | 直接对应 §5.5.3 第 3 步"加载最新合法快照 / 回退上一份"；扫描天然给出"上一份" |
| META 快照指针 | **payload 尾部追加 `snapshot_index: u64, snapshot_term: u64`（无快照 = 0,0）**，仍保持 `format_version = 1` | ①升 `FORMAT_VERSION` 到 2；②不加指针 | §5.6 允许"major 内只扩 payload"；`decode_meta` 只读到 `created_at` 为止、**不校验尾部长度**，故旧文件可读、新文件对旧读者只是多出被忽略的尾部——零版本代价。指针给运维/排障一个"当前生效快照"的单点事实，扫描是兜底 |
| 压缩边界 | `compact(to)` 只删除**完全**位于 `to` 之前的段；`first_index = to + 1`；`entries`/`term` 对 `< first_index` 返回 `Compacted` | 允许段内前缀截断 | v1 的段粒度压缩已满足 §5.5.4 保留策略；段内截断复杂度高而收益低 |
| **快照保留份数** | 磁盘上保留**最新两份**快照文件，其余在每次 `save_snapshot` 后删除（目录 fsync） | 只留最新一份 | 最新文件 CRC 损坏时需要回退到上一份（§5.5.3 第 3 步）；仅一份则只能 fail-start |
| trailing 保留 | 压缩时保留 `to` 之前的段直到其字节量 ≤ `wal_trailing_keep`（与 `snapshot_threshold` 同值，Q6） | 压缩后立即全删 | 慢 follower 需要 trailing 追赶；超出则走快照传输 |
| **（v0.2.10 落地范围）** | 本版 `compact(to)` 实现为**段粒度全删**（`to` 之前完全覆盖的段）；**字节级 trailing 窗口尚未接线**——`wal_trailing_keep` 只在 `ProfileConfig`，未进 `WalConfig`，慢 follower 一律走快照传输 | 本版即接字节保留 | 保留窗口是空间/带宽的优化而非正确性条件：删段后落后 follower 由 `Compacted` 路径拉快照，语义不变。接线需把 `wal_trailing_keep` 下沉到 `WalConfig`，随 M2 追赶测试一并做 |
| 快照触发口径 | **逻辑增长**：自上次快照以来**已 apply 的日志字节数** ≥ `snapshot_threshold` 即触发（快照后归零）；`wal_bytes` 仍作为物理占用指标上报 | ①以物理段字节数为触发输入；②按条数触发 | ①段粒度压缩下物理字节**不会**因压缩而下降（含已压缩前缀的段要等下一次 rollover 才能回收），用物理字节会在跨过阈值的每个条目上重复触发快照；②按条数偏离 §7 的字节口径。物理占用仍上报，运维据此判断回收进度 |
| follower 安装快照的日志语义 | **整体替换**：`install_snapshot` 持久化后令 `compacted_to = snapshot.index`、**清空本地全部条目**，并把物理日志轮换为从 `snapshot.index + 1` 起的空段（删除其余所有段） | ①只删 ≤ index 的条目；②原地保留 > index 的条目 | 安装快照时本地 > index 的条目必属另一分支且未提交（raft 断言 `snapshot.index ≥ committed`）；保留它们会让 `last_index()` 报出 raft 视图之外的尾巴，恢复时还会看到"陈旧条目 + leader 重发条目"之间的**空洞**而 fail-start（实测）。对齐 etcd `MemoryStorage.ApplySnapshot` 的整体替换语义，缺的条目由 leader 重发 |
| 恢复时的成员配置 | **`initial_state()` 返回快照携带的 `ConfState`**（无快照时回落到 `initial_state` 引导集） | 始终回落到引导集 | §5.5.3 第 6 步"ConfState 来自快照/日志中的 ConfChange"；安装过快照的节点重启后必须知道自己属于哪个集群 |

落点：§5.5.3 第 3/5/6 步、§5.5.4；实现 `arachne/src/storage/snapshot.rs`（新）、`meta.rs`、`wal.rs`、`consensus/raft_storage.rs`、`consensus/node.rs`、`runtime/mod.rs`。

---

## 1. 目标与非目标

**目标**

- 嵌入式 Rust 分布式 KV 库，**CP 语义**：任何时刻至多一个有效 leader，提交需多数派；失去多数派时**线性一致操作全部失败**，不降级、不脑裂。
- **产品形态（v0.2.4 决议 D-ART，v0.2.6 D-ART-rev1 增 `arachne-seam` 叶 crate）**：`arachne` 库是产品主体；随库交付**薄运维 bin `arachne-node`**（force-recovery、成员运维、metrics 端点的承载者，亦为 L3/L4 集成测试的被测进程）——见 §4 工件边界。
- 共识内核复用成熟实现（首选 `tikv/raft-rs`，备选 `openraft`），自研部分限定为：传输层、存储层（WAL/快照）、状态机、客户端 API。
- 面向注册中心类应用的演进路径（TTL/lease 列入 v1.5，Watch 列入 v2，见 §11 D3）。

**非目标（v1 明确不做）**

- ❌ 失去多数派后自动单节点接管（改为人工 `force-recovery` 命令，见 §6）
- ❌ 基于 ACK 超时自动剔除节点；同理，**不自动降级 Learner**（成员变更只走 Quorum 投票的 ConfChange）
- ❌ 分片 / 多 Raft Group
- ❌ 自研共识算法
- ❌ 跨 key 事务 / 多 op 原子批量（v1 单 key 原子；批量 API 仅是逐条提交的便利封装，文档明示无原子性）
- ❌ Lease Read（依赖时钟偏移上限配置，v2 再评估；v1 读一致性只靠 ReadIndex，不靠时钟）

**语义界定**："CP" 指的是**线性一致 API 子集**（`put`/`delete`/`get`）。`get_stale` 是显式 opt-out 的弱读，不削弱整体 CP 声明，但其语义边界必须在 crate 文档首页写清（见 §2.1/§2.2）。

## 2. 一致性语义（对外承诺）

### 2.1 操作语义表

| 操作 | 语义 | 实现 |
|---|---|---|
| `put` / `delete` | 线性一致写 | Raft 日志，多数派持久化后提交，apply 后应答 |
| `get` | 线性一致读（默认） | ReadIndex（§5.4）；非 leader 节点转发/重定向 |
| `get_stale` | 允许任意旧读 | 本地状态机直接读；**不保证跨调用单调**（文档显著声明） |
| 失去多数派 | 写与线性读返回 `QuorumUnavailable`；`get_stale` 仍可用 | CheckQuorum + leader 自动 step down |

### 2.2 线性一致的边界（必须写入 crate 文档首页）

**保证**：

- G1：对每个唯一 `(client_id, seq_no)` 的写，在**会话存活窗口内**重试，效果恰好一次（去重使重试在历史中等价于单次调用；线性化检查器以 `seq_no` 合并同一 invocation 的重试）。
- G2：`get` 与 `put`/`delete` 构成线性一致历史（ReadIndex 保证，不依赖时钟）。
- G3：会话表在快照/日志中全量复制，leader 切换不丢失去重状态。

**不保证（结果未知或弱化的显式清单）**：

- N1：`get_stale` 可以读到任意旧的已提交值，且跨调用、跨 handle 不保证单调。
- N2：**会话过期后的重试**：返回 `SessionExpired` 时原操作**可能已生效也可能未生效**，客户端重发即可能重复执行；重复风险窗口 ≤ `session_ttl + grace_period`（§2.3）。
- N3：`Timeout`（propose 后有界等待超时）同样**结果未知**，客户端应带同一 `(client_id, seq_no)` 重试。
- N4：`Unrecoverable` 与 `force-recovery` 之后的一切行为不在一致性承诺范围内。
- N5：进程崩溃后原 `client_id` 不恢复（新进程 = 新会话）；跨进程崩溃的重试不做去重。
- N6：TTL 过期事件的时间基准是 **leader 墙钟（随日志传播）**，受 leader 时钟质量影响，精度为弱保证（lease 语义，同 etcd）。

### 2.3 幂等会话规范

**数据结构**（复制状态机内，随日志/快照全量复制）：

```
SessionTable: Map<ClientId, Session>
Session {
    expiry_ts: u64,               // 毫秒，leader 墙钟
    last_seq: u64,
    results: BoundedMap<seq_no, Result, CAP_RESULT=128>,  // LRU；被逐出的 result 记为 Unknown
}
grace 区: Map<ClientId, GraceEntry{ expired_at }>  // 过期后再保留 grace_period
```

**生命周期**：

| 阶段 | 规则 |
|---|---|
| 创建 | **惰性**：首次携带未见过的 `client_id` 的写提议在 apply 时注册会话，`expiry = entry.ts + ttl`。不引入单独的会话建立 RPC |
| `client_id` | 每次 `Handle` 创建时生成 UUIDv7，**不持久化**；同一进程多个 handle 各自独立会话 |
| `seq_no` | 客户端每个会话内单调递增 u64；重试必须复用原 `seq_no` |
| TTL 刷新 | 仅**成功 apply 的写操作**刷新 expiry（读不创建也不刷新会话） |
| GC | 每 `ttl/2`，leader propose 一条 `SessionGc{ts}` 日志；**所有节点在 apply 时用条目内 `ts` 判定过期**（禁止读本地时钟，保证状态机确定性）。过期会话先移入 grace 区，grace 满后删除 |
| 内存上限 | `max_sessions`（默认 10_000）+ 每 client 结果缓存 128 条。表满时**拒绝新会话**（`SessionTableFull`），已有会话不受影响；grace 区同样有界（`max_sessions/2`），满则按 expired_at 最旧淘汰 |

**过期中重试的精确语义**（对应 N2）：

1. 重试命中 **活跃会话** 且 `seq_no` 有缓存结果 → 返回缓存结果（恰好一次）。
2. 命中活跃会话但结果已被 LRU 逐出（该 client 写过多于 128 个不同 key 且重试极旧）→ `SessionExpired`（结果未知）。
3. 命中 **grace 区**（已过期但未删除）→ `SessionExpired`（结果未知；结果缓存在转入 grace 时一并丢弃）。
4. 完全无记录 → 视为新会话首写。

> 客户端契约：收到 `SessionExpired` 后必须重建会话（新 seq 从 0 开始语义上等同新 client 亦可）并**接受可能重复执行**。TTL 必须显著大于客户端最大重试窗口：`ttl ≥ retry_attempts × rpc_timeout + 安全余量`；默认 `session_ttl = 60s`（v0.2.1 决议 Q1，见 §7）。`client_id` 不持久化已锁定（进程重启 = 新会话，对应 N5）。

### 2.4 "结果未知"情形总清单

| 情形 | 返回 | 客户端正确动作 | 重复风险 |
|---|---|---|---|
| propose 后等待提交超时 | `Timeout` | 同 seq 重试 | 无（会话去重） |
| 会话过期后重试 | `SessionExpired` | 新会话重发 | **可能重复**（窗口 ≤ ttl+grace） |
| leader 提交前崩溃且条目未达多数派 | `Timeout`/`NotLeader` | 同 seq 重试 | 无 |
| `force-recovery` 后 | — | 以恢复点为准，人工核对 | 不在承诺内 |

## 3. API

### 3.1 构造与引导参数

```rust
let node = Arachne::builder()
    .node_id("node1")                        // 持久化于 META；与持久值不一致 → fail-start
    .listen("0.0.0.0:7000")                  // 监听地址
    .advertise("node1:7000")                 // 对外通告地址；listen 为非回环地址时必填
    .seeds(["node1:7000", "node2:7000", "node3:7000"])   // 仅首启引导用，见 §5.7
    .data_dir("/var/lib/arachne")            // flock 独占锁，双进程 → fail-start
    .profile(Profile::Lan)                   // Lan | Wan，字段可逐项覆盖
    .build().await?;

let kv = node.handle();                      // cheap Clone，多任务共享
kv.put(b"k", b"v").await?;                   // follower → NotLeader 重定向（§3.3）
kv.get(b"k").await?;                         // ReadIndex 读
kv.get_stale(b"k").await?;                   // 旧读，任意节点本地执行
kv.delete(b"k").await?;
node.shutdown().await?;                      // 优雅关闭：停提案 → (可选)转让 leader → flush → 释放锁；有界 10s
```

运维 API（`node` 上，供 CLI/嵌入方调用）：`add_learner`、`promote_learner`、`remove_member`（若目标是 leader 自动先 transfer）、`transfer_leader`、`leader_id()`、`metrics()`。`transfer_leader` 为**公开 API**（v0.2.1 决议 Q3）：shutdown 的可选转让与 `remove_member(leader)` 的先转让流程均内部依赖它，藏不住不如明示并纳入 M3 API review。
运维入口（v0.2.4 决议 D-ART）：上述运维操作同时经 **`arachne-node` CLI 子命令**暴露（`force-recovery`、成员操作、`leader-id`、metrics 导出）——嵌入方走 lib API，运维走 CLI，**二者调用同一 lib 路径**，不允许 bin 内出现第二套逻辑（test-plan §3.3）。

### 3.2 错误模型（完整枚举）

```rust
enum ArachneError {
    NotLeader { leader_hint: Option<(NodeId, SocketAddr)> },  // 重定向；hint 可能过期
    QuorumUnavailable,            // leader 已 step down 或 ReadIndex quorum 轮失败
    Timeout,                      // 有界等待超时；结果未知（见 §2.4 N3）
    Busy,                         // 背压：提案队列满 / 读等待队列满
    InvalidArgument(String),      // key/value 超限等；propose 前校验，不入日志
    SessionExpired,               // 会话过期/结果被逐出；结果未知
    SessionTableFull,             // 新会话被拒，已有会话不受影响
    ConfChangePending,            // 已有未提交的成员变更
    LeaderRemovalRequiresTransfer,// remove(leader) 且未授权自动转让
    ClusterIdMismatch,            // 握手期发现异集群节点
    ShuttingDown,
    DataDirLocked,
    Unrecoverable(String),        // 存储损坏/状态机不变量破坏等，进程应 fail-stop
}
```

**fail-stop 纪律**：任何 apply 期不变量违反（状态机 bug）→ 进程 abort，绝不带偏差继续服务；此后所有调用返回 `Unrecoverable`。**状态机 apply 必须无失败**：一切用户输入校验（key/value 上限等）都在 propose 之前完成，`InvalidArgument` 不进入日志。

### 3.3 每操作 × 条件错误矩阵

| 条件 | put/delete | get（ReadIndex） | get_stale |
|---|---|---|---|
| 参数非法 | `InvalidArgument`（propose 前拒绝） | `InvalidArgument` | `InvalidArgument` |
| 本节点非 leader | `NotLeader{hint}` | `NotLeader{hint}` | **正常执行**（本地读） |
| 失去多数派 | `QuorumUnavailable` | `QuorumUnavailable`（quorum 心跳轮失败） | 正常执行（可能旧） |
| 提案/读队列满 | `Busy` | `Busy` | `Busy`（仅读队列） |
| 有界等待超时 | `Timeout`（结果未知） | `Timeout` | 正常执行 |
| 会话表满 / 过期 | `SessionTableFull` / `SessionExpired` | 不适用（读不用会话） | 不适用 |
| 关闭中 | `ShuttingDown` | `ShuttingDown` | `ShuttingDown` |
| 存储致命错误 | `Unrecoverable` | `Unrecoverable` | `Unrecoverable` |

**重定向策略**：`NotLeader{hint}` 时客户端直连 hint；hint 失效则按 seeds 轮询。内置重试：默认 3 次重定向 + 总超时；总超时后返回 `Timeout`。转发在客户端侧完成（本库嵌入式场景客户端与节点同进程，重定向比服务端 proxy 少一跳且保持节点无状态）。

### 3.4 引导语义要点（详见 §5.7）

- 首启（data_dir 为空）必须提供含自身的 `initial_cluster`；`initial_cluster` 只有自己 → 直接成为单节点集群 leader（quorum=1，合法）。
- 非首启：以持久化 ConfState 为准，`initial_cluster` 与其不一致仅告警。
- `node_id`、显式配置的 `cluster_id` 与 META 不一致 → fail-start。

## 4. 架构分层

```
┌──────────────────────────────────────────────┐
│  API 层（handle, 重定向, 重试策略, 会话管理）      │
├──────────────────────────────────────────────┤
│  共识层  raft-rs RawNode 封装（不自研算法）        │
├───────────────┬──────────────────────────────┤
│  Transport trait│  存储层 Storage trait          │
│  tonic 实现     │  WAL + Snapshot               │
│  (mTLS+版本号)  │  + 状态机(会话表+KV)            │
└───────────────┴──────────────────────────────┘
```

- **两个 trait 边界是刻意设计**：`Transport`（生产用 tonic 实现，测试用 turmoil 承载真实 tonic / L1 用内存交换机，见 §9 与 `test-plan-v0.1.md` §3）与 `Storage`（raft-rs 要求自实现，同时是故障注入点，见 §5.5/§9）。
- **工件边界（v0.2.4 决议 D-ART，v0.2.6 D-ART-rev1 增 `arachne-seam` 叶 crate，布局详见 test-plan §3.2）**：`arachne-seam`（**无依赖叶 crate**，持有接缝 trait `Transport`/`TransportRx`/`TransportFactory`/`Clock`/`Rng`/`StateMachine` 与核心类型 `NodeId`/`LogIndex`/`Term`/`Timestamp`）+ `arachne`（lib，产品主体，transport-agnostic 核心；依赖并重导出 `arachne-seam`，**公共 API 对嵌入者不变**）+ `arachne-transport-tonic`（生产传输实现，唯一允许出现 tonic/rustls 类型的 crate，**依赖 `arachne-seam` 而非 `arachne`**，经默认 feature 引入）+ **`arachne-node`（薄运维 bin，生产交付物）** + 测试工件（`arachne-testsupport` 仅 dev-dependency、`arachne-sim` 测试 bin、`examples/`，均对 `arachne` 用 `default-features = false`）。`arachne-node` 不含共识/存储逻辑，仅装配——保证 L3/L4 测试的进程与嵌入者运行的是同一份 lib 代码；测试依赖不得进入任何生产 crate 的 `[dependencies]`（CI 以 cargo-tree 门禁检查）。

### 4.1 任务模型与背压

- tokio async 任务 + `CancellationToken`；WAL fsync 在专用阻塞线程池执行，不占 runtime worker。
- **主循环**：单 tick loop 驱动 raft-rs（`tick()` → `ready()` → 持久化 → 发送 → `advance()`），严格按 raft-rs 的 Ready 处理顺序（§4.2）。
- **apply 独立任务**：tick loop 与 apply 分离，apply 落后时以有界通道反压提案，避免读延迟被写洪峰拖垮（验收见 §10 M2）。
- **背压与资源上限总表**：

| 资源 | 上限（默认） | 触顶行为 |
|---|---|---|
| 提案队列 | 64 MB（按字节计） | 新提案返回 `Busy`，不丢弃已入队提案 |
| 每 follower inflight | `max_inflight_bytes`（4MB/1MB）+ raft 内建 `max_inflight_msgs`(256) 计数 | 暂停向该 follower 发送；需要时改发快照 |
| 快照发送 | 限速 32 MB/s（可配）+ 1 MB 分片 | 慢速平稳传输 |
| 会话表 | `max_sessions` 10_000 | 拒绝新会话 `SessionTableFull` |
| ReadIndex 等待队列 | 4096 | `Busy` |
| WAL 增长 | `snapshot_threshold` 触发压缩（§5.5） | 软上限告警，不硬拒写 |
| 单值/键大小 | value ≤ 1 MiB，key ≤ 4 KiB | `InvalidArgument` |

### 4.2 raft-rs 集成契约

- 自实现 `Storage` trait：`initial_state / entries / term / first_index / last_index / snapshot`；日志压缩后对已 compact 区间返回 `Compacted`，raft 层据此对落后者转快照传输。
- **Ready 处理顺序（不变量，测试强制，见 §9 INV1）**：取出 ready → fsync 持久化 `entries + hard_state + snapshot` → 发送 `ready.messages` → `advance()` → 发送 `persisted_messages`。**任何消息不得在其载荷持久化之前发出**（以所用 raft-rs 版本 API 文档为准）。
- 启用特性：PreVote、CheckQuorum、Learner、`transfer_leader`、`read_index`。
- ConfChange 纪律：`propose_conf_change` 一次一个；存在未提交 ConfChange 时新提案返回 `ConfChangePending`。

## 5. 核心机制

### 5.1 选举

- 随机化选举超时（`[1x, 2x)` 均匀分布），心跳 = 选举超时的 1/10；约束：`rpc_timeout < election_timeout`（§7；v0.2.8 修正，原写作 `< heartbeat_interval` 与 §7 预设冲突）。
- **PreVote**：重新上线/分区内恢复的节点先预投票，不抬 term，防止抖动 leader。
- **CheckQuorum**：leader 超过选举超时未获多数派心跳则 step down；step down 时清空未决 ReadIndex 等待者（返回 `NotLeader{hint: None}`）。
- 与 ReadIndex 的关系：即使 CheckQuorum 尚未触发 step-down，ReadIndex 的 quorum 心跳轮也独立防止分区内旧 leader 服务线性读（§5.4），二者互为防线。

### 5.2 复制与流控

- **多数派 ACK 即提交**，不等全量；落后者异步追赶。
- 流控：raft-rs 内建 `max_inflight_msgs` 计数流控 + 自定义按字节 `max_inflight_bytes`；触顶暂停向该 follower 发送。
- **（v0.2 修正）** 落后**不触发任何自动成员变更**。落后超过 trailing 保留量 → leader 对其改发快照（由 `Compacted` 错误路径自然触发）；同时发出 `follower_lag_bytes` 告警，**由运维决定** demote 为 Learner 或移除——demote/remove 本身是 ConfChange，需要多数派投票，多数派不可用时该操作本身不可行，这正是非目标的体现。
- 提交位推进由 leader 统一计算（多数派 match index），不依赖 follower 主动上报的 commit 值做提交决策。

### 5.3 成员变更

- **单步 ConfChange**（每次只增/删/转一个节点），规避 joint consensus 复杂度；raft-rs 原生支持。
- **硬约束**：
  1. 同一时刻至多一个未提交 ConfChange；存在时新成员变更提案返回 `ConfChangePending`（普通写不受影响）。
  2. 新节点一律先以 **Learner** 加入（不参与 quorum），追平（`lag_bytes < promote_lag_threshold` 且在线）后由运维显式 `promote_learner` 转 Voter。
  3. **移除 leader**：`remove_member(current_leader)` 默认内部先 `transfer_leader` 成功后再 propose 移除；转让失败（无多数派）则移除失败并返回原错误。不提供"直接移除 leader"的旁路。
  4. 加入节点在握手期校验 `cluster_id` + mTLS 证书身份（CN ↔ node_id 映射）；异集群节点拒绝并返回 `ClusterIdMismatch`，防数据混写。
- **rejoin**：依赖持久化 `node_id` + PreVote，不重走"加入"流程。
- 集群可缩容至 1 节点（Raft 安全性由多数派保证），文档提示 2 节点集群容错为 0。

### 5.4 读路径

**ReadIndex 精确算法**（`get`）：

1. 请求到达 leader：在 ReadIndex 等待队列注册 `(token, oneshot)`，调用 `raw_node.read_index(ctx)`；raft 层以当前 term 记录 pending read index = commit index。
2. raft 层向多数派发一轮携带 ctx 的心跳；收到**多数派**心跳响应后，`ready().read_states` 弹出 `(index, ctx)`。
3. 主循环收到 read_state：等待 `applied ≥ index`（等待超时 `read_index_timeout`，**锁定为 `2 × election_timeout`、重试 1 次**，v0.2.1 决议 Q2），仍失败返回 `Timeout`。
4. 等待期间 term 变化 / step down → 清空等待者，返回 `NotLeader{hint}`。
5. **follower 收到 `get`**：与写一致地返回 `NotLeader{hint}` 走客户端重定向（不做 follower 本地 ReadIndex 转发代理，v1 不引入服务端 proxy 一跳）。
6. 应用侧约束：`applied` 长时间落后（如 WAL fsync 卡顿）时读等待队列有界，触顶 `Busy`。

`get_stale`：任意节点直接读本地 applied 状态机；多数派丢失时仍可用；**不保证单调**（N1）。**v1 不提供单调读变体**（v0.2.1 决议 Q5）：仅文档声明非单调；若出现强需求，v1.1 再评估 per-handle 已见 applied 水位的低成本追随变体。

Lease Read 留 v2，显式记录其前提：配置化的时钟偏移上限 + 安全性论证，v1 不做。

### 5.5 存储与恢复

#### 5.5.1 WAL 格式

- 分段文件：`wal-%020d.log`（**段名 = 该段首条记录的 log index**），默认 `segment_bytes`（128 MB）超限时滚动新建段；**重开时续写最高编号的既有段**（仅当无任何段时才建 `wal-1.log`），避免每次重启新建空段、破坏"段名=首 index"不变式。
- 记录布局：`[u32 len][u32 crc32c][u8 type][payload]`；`type ∈ {Entry, HardState, Meta}`。
- **新建段文件后须 fsync 数据目录**（持久化目录项），否则已 fsync 的条目在掉电后可能随目录项一并丢失（I2 漏洞）。
- `META` 文件（data_dir 下，独立于段）：`cluster_id, node_id, format_version, created_at`；写入 = 写临时文件 → fsync → rename → **fsync 目录**。
- 记录类型集合在 **protocol major 版本内冻结**（§5.6），保证同 major 内可回滚。

#### 5.5.2 fsync 顺序不变量（崩溃安全的根基）

| # | 不变量 |
|---|---|
| I1 | **HardState 每次变化（term/vote/commit）都逐次 fsync**，不参与 BatchMs 批量。在同 term 发出投票/请求投票前，`(term, voted_for)` 必须已持久化——否则崩溃后可能同 term 双投票，破坏安全性 |
| I2 | 任何携带日志条目的消息（leader 的 AppendEntries、follower 的成功响应）发出前，对应条目必须已 fsync |
| I3 | 快照 data+meta fsync 且目录 fsync 后，才可报告安装完成 / 才可删除被覆盖的 WAL 段 |
| I4 | apply 的条目必须已 fsync（由 Ready 顺序天然保证，测试断言） |

- `FsyncPolicy::{Always, BatchMs(d)}` **仅作用于 Entry 批量**（group commit：上一次 fsync 在途期间到达的条目合并为一批）。BatchMs 下提交确认至多延迟 d ms；窗口内崩溃只会导致条目丢失而无客户端收到 ack，安全。默认 Lan=Always；Wan=Always（BatchMs 为显式 opt-in）。

#### 5.5.3 恢复算法（启动时）

```
1. flock(data_dir)；失败 → DataDirLocked fail-start
2. 读 META：cluster_id/node_id/format_version 与配置比对，不一致 → fail-start
3. 加载最新合法快照 S（CRC 校验）：
   - S 损坏 → 回退上一份快照；无更早快照 → 若 WAL 可从更早完整回放则继续，否则 fail-start
4. 顺序回放 WAL 段：逐记录校验 CRC/解码，取最后一条合法 HardState 得 H = {term, vote, commit}
    **（v0.2.7 收紧）坏记录一律不得靠类型字节分类**——类型字节在 CRC 保护体内，损坏时不可信：
    - 记录在磁盘上**完整存在**但 CRC/解码失败（真损坏，含损坏的 HardState）→ **无条件 fail-start(Unrecoverable)**，
        运维可用 --repair=truncate 显式截断（安全前提：多数派在别处存活，本机以 Learner 重加追赶），或走 force-recovery（§6）
    - 段缺失/序号断档 → fail-start
    - **仅最后一段的结构性撕裂**（头不足 8 字节 / `record_end > file_len` / `len` 非法，即未 fsync 的尾部截断）可自动截断，
        且撕裂仍过 commit-window 检查：`estimated_index ≤ H.commit + 1` → fail-start（可能触及已提交区间）；
        否则截断到最后一条合法记录，计 metric wal_truncated_records_total，WARN（只丢弃未 fsync 的未提交尾部，Raft 会从 leader 重取，安全）
    - **非最后一段**出现坏记录 → 一律 fail-start（撕裂只可能出现在最后写入的段）
    **恢复后校验**：断言 **`H.commit ≤ last_index`**（last_index = 回放得到的最后合法条目 index），否则 fail-start——关闭"损坏 len 伪装成撕裂"导致已提交数据静默丢失
5. 从 S.index+1 起重建内存日志缓存，校验连续性
6. 以 {H, ConfState(来自快照/日志中的 ConfChange)} 初始化 raft-rs
7. 重放条目至状态机（apply 幂等：跳过 ≤ applied 的条目）
```

> **（v0.2 修正）** v0.1 的"损坏则截断"无条件表述在此收紧：**任何可能触及已提交区间的损坏一律 fail-start**，宁可拒绝服务也不静默丢已提交数据——这是 CP 承诺的一部分。

> **（v0.2.7 收紧，E-rev）** 本次收紧仍落在 INV6 的两个象限内——"合法前缀截断 或 fail-start"：唯一允许自动截断的是最后段的结构性撕裂（未 fsync 的尾部），其余一律 fail-start；`--repair=truncate` 仍是运维显式截断的逃生口。

#### 5.5.4 快照与保留

- **快照内容**（v0.2 明确）：`{last_applied_index, last_applied_term, ConfState, KV 全量, 会话表（含 grace 区）}`；格式带 `format_version` + 全文件 CRC。**会话表缺失将导致安装快照的 follower 丢失去重状态、重试重复生效——必须包含**。
- **创建**（v1 全内存状态机）：持 apply 锁、阻塞 apply，序列化 → tmp → fsync → rename → 目录 fsync → 更新 meta → 方可压缩 WAL。阻塞时长与数据量线性；**v0.2.1 决议 Q4：接受阻塞方案，`snapshot_last_duration > 1s` 触发告警（§8.2）；数据量逼近内存上限时 v1.1 重审双缓冲/持久结构**。
- **安装**（follower）：流式分片 → tmp 聚合 → CRC → fsync → rename → apply 锁内原子替换内存状态机 → 更新 applied index/term/ConfState → 回执。安装期间本地读短暂返回 `Busy`。
  **（v0.2.10 落地）** 存储侧 `install_snapshot` 先持久化快照，再整体替换日志（清空条目 + 轮换到 `index+1` 新段）；状态机侧由 runtime 在 `StepOutcome.snapshot` 上执行 `restore`，且**先于**本周期任何 `committed` 条目 apply（安装点覆盖这些条目，`step` 已按快照 index 过滤）。v1 的快照传输走单次 `Ready`（未分片），流式分片随传输层快照 RPC 落地。
- **保留策略**：快照成功后删除 `first_index` 之前的 WAL 段，保留 trailing 供慢 follower 追赶；**v0.2.1 决议 Q6：`wal_trailing_keep` 与 `snapshot_threshold` 绑定同值（Lan 64 MB / Wan 16 MB，可显式覆盖解耦）**；落后超出 trailing → 快照传输（`Compacted` 路径）。
- **（v0.2.10 落地范围）** 快照文件本身保留最新两份（防最新一份 CRC 损坏导致 fail-start）；WAL 侧实现段粒度全删，**字节级 trailing 窗口留待接线**（见 §M 表末行）。
- **顺序**：`save_snapshot` 先落快照文件（fsync + 目录 fsync），再更新 META 指针（原子写），最后删旧快照文件；调用方只有在 `save_snapshot` 返回后才可 `compact`。**压缩绝不删除未被快照覆盖的条目**——`compact(to)` 在快照缺失或 `snapshot.index < to` 时返回 `Unrecoverable`（fail-start）而非降级删除。
- **恢复时的水位**：载入快照即令 `first_index = snapshot.index + 1`（快照对其及更早的条目是权威视图），WAL 中仍物理存在的更早段在下一次 `compact` 时回收。
- **与 lagging follower 的交互**：压缩不等待任何 follower；追赶不上的 follower 一律走快照路径，leader 始终保留最新一份快照。

### 5.6 传输与安全

- **tonic（gRPC over HTTP/2）+ rustls mTLS**，藏在 `Transport` trait 之后（D2，§11）；快照传输用 server-streaming 分片。
- **握手**：建连时双向交换 `{protocol_version(major, minor), cluster_id, node_id, feature_flags}`。校验：`cluster_id` 相同、`major` 相同、对端 `minor ≤` 本端（只向后兼容）。不满足 → 拒绝连接 + `protocol_mismatch_total` 指标 + 限速日志。
- **滚动升级规则**：逐个升级 follower → 健康检查 → `transfer_leader` → 升级旧 leader；升级窗口内**禁止 ConfChange**（runbook 约定）；混合版本仅允许存在于升级窗口内。
- **回滚限制**：WAL/快照记录类型在 major 内冻结、只扩 payload；跨 minor 回滚需等待 WAL 已按旧格式压缩（通常一个快照周期）。文档明示。
- 证书轮换 v1 采用重启生效；`CN ↔ node_id` 映射在配置中显式声明，未映射证书拒绝。
- 消息均带 `protocol_version`；解析层由 `cargo-fuzz` 覆盖（§9）。

### 5.7 引导与集群初始化（v0.1 缺失，新增）

**首启**（data_dir 无 META）：

1. 校验 `initial_cluster` 非空且包含自身 `node_id`，否则 fail-start。
2. 生成 `cluster_id`（随机 UUID），写 META（fsync + 目录 fsync）。
3. 初始 ConfState = `initial_cluster` 全体 Voter；单成员集群直接当选。
4. 各节点独立首启、凭相同 `initial_cluster` 互相发现并选举——无需"先启动谁"的顺序约定。

**非首启**（META 存在）：

1. `node_id` 与配置不一致 → fail-start。
2. 显式配置的 `cluster_id` 与 META 不一致 → fail-start。
3. `initial_cluster` 与持久化 ConfState 不一致 → 仅 WARN（**持久化 ConfState 是唯一事实**，成员变更后配置文件不需要跟着改）。
4. 已有日志/快照 → 走 §5.5.3 恢复；PreVote 保证 rejoin 不抖动集群。

## 6. 运维与故障恢复

### 6.1 force-recovery（危险操作）

**入口**：`arachne-node force-recovery --data-dir ...`（`arachne-node` 为随库交付的薄运维 bin，见 §4 工件边界与 §3.1 运维入口）。

**语义**：将本节点重置为单节点集群，恢复点 = 本节点 applied index（**可能落后于旧集群已提交位**，即可能丢已提交数据）。

**前置条件（不满足即拒绝执行）**：

1. 成功获取 data_dir 文件锁（旧进程不在运行）。
2. **可达性预检**：尽力探测旧配置中其余成员（3s 超时）；**任一旧成员可达 → 拒绝**，除非再显式给出 `--i-know-data-loss`（双确认）。
3. 必须显式 `--i-know-data-loss` 才能执行（文档显著声明可能丢数据）。

**算法**：

```
停写 → 从最新快照+WAL 重建内存状态（保留 KV + 会话表）
→ 默认生成【新 cluster_id】（--keep-cluster-id 可覆盖）
→ HardState: term += 1, voted_for = self, commit = applied
→ 截断 WAL 至 applied → ConfState 重置为 [self]（Voter）
→ 正常启动为单节点 leader
```

**防脑裂关键设计**：默认更换 `cluster_id` 后，旧多数派若复活，与本节点握手因 cluster_id 不匹配互拒——**不会形成同 ID 双集群互连**。这比 etcd `--force-new-cluster`（保留 cluster ID）更安全，代价是旧成员复活后需人工清理重建，属于可接受的灾难流程。

### 6.2 启动校验清单

文件锁 → META 一致性（node_id / cluster_id / format_version）→ WAL 恢复算法（§5.5.3）→ initial_cluster 与持久 ConfState 比对（WARN 级）→ 加入网络前握手校验（cluster_id + 证书身份）。

## 7. 配置预设

| 参数 | Lan | Wan | 说明 / 约束 |
|---|---|---|---|
| heartbeat_interval | 100ms | 500ms | election_timeout ≥ 5× heartbeat（Lan 预设 10×、Wan 预设 5×；v0.2.8 修正，原写 10× 与 Wan 预设冲突） |
| election_timeout | 1s | 2.5s | 实际随机化 [1x, 2x) |
| rpc_timeout | 500ms | 2s | 必须 < election_timeout（v0.2.8 修正，原写 `< heartbeat_interval` 与预设冲突） |
| max_inflight_bytes | 4MB | 1MB | 每 follower |
| max_inflight_msgs | 256 | 256 | raft 内建计数流控 |
| snapshot_threshold | 64MB 日志 | 16MB 日志 | 触发快照+压缩 |
| wal_trailing_keep | 64MB | 16MB | 慢 follower 追赶窗口；**默认与 snapshot_threshold 绑定同值**（Q6，可显式覆盖解耦） |
| wal_segment_size | 128MB | 128MB | 段滚动 |
| fsync_policy | Always | Always（BatchMs 可选） | **HardState 恒为 Always**（I1） |
| read_index_timeout | 2×election_timeout | 同左 | **锁定**：超时重试 1 次（Q2，§5.4） |
| proposal_queue_bytes | 64MB | 64MB | **按字节计容**（Q7）；触顶 `Busy` |
| session_ttl | 60s | 60s | **锁定**（Q1）；需 ≥ 客户端最大重试窗口 |
| session_grace_period | 60s | 60s | 过期后结果未知窗口 |
| max_sessions | 10_000 | 10_000 | 触顶拒新会话 |
| max_value_bytes / max_key_bytes | 1MiB / 4KiB | 同左 | propose 前校验 |
| snapshot_transfer_rate | 32MB/s | 8MB/s | 快照发送限速 |

所有字段可在 `Profile` 基础上逐项覆盖。

**节点进程的配置注入（v0.2.5 决议 D-ART-config：格式锁定 TOML）**：`arachne-node --config node.toml`（字段即上表 + §5.7 `initial_cluster`）为主，CLI 单项覆盖（`--data-dir` / `--advertise` / `--node-id`）为辅；测试 harness 复用**同一配置面**为每节点生成独立配置文件，不引入第二配置格式（test-plan §3.3）。

## 8. 可观测性

### 8.1 指标清单（Prometheus）

v0.1 已有：`term`、`leader_info`、`commit_index`、`applied_index`、`per-follower lag`、`wal_fsync_p99`、`session_count`、`proposal_dropped`。

**v0.2 补全（运营实际必需）**：

| 类别 | 指标 |
|---|---|
| 领导权 | `raft_role`、`leader_changes_total`、`no_leader_seconds`、`elections_total` |
| 复制 | `apply_lag`（commit−applied）、`follower_lag_bytes{peer}`、`follower_active{peer}` |
| 提案 | `proposal_queue_bytes`、`proposal_wait_p99`、`proposal_fail_total{reason}`、`conf_change_pending_age` |
| 读路径 | `read_index_round_latency`、`read_index_timeout_total`、`redirects_total` |
| 存储 | `wal_bytes`、`wal_segment_count`、`wal_fsync_errors_total`、`wal_truncated_records_total`、`snapshot_in_progress`、`snapshot_last_duration`、`snapshot_last_size`、`snapshot_sent/received_total` |
| 会话 | `session_table_util`、`session_expired_total`、`session_grace_evicted_total` |
| 传输/安全 | `peer_conn_state{peer}`、`tls_handshake_failures_total`、`protocol_mismatch_total` |
| 灾难 | `unrecoverable_total`、`force_recovery_used_total` |
| 资源 | `kv_bytes`、`session_bytes`、`state_machine_apply_duration` |

tracing span 覆盖一次写请求全链路（client → redirect → propose → commit → apply → reply），以 `(client_id, seq_no)` 关联重试。

### 8.2 告警信号

| 信号 | 条件（默认） | 级别 |
|---|---|---|
| 无主 | `no_leader_seconds > 60s` | page |
| 选主抖动 | `leader_changes_total` 增速 > 3/10min | page |
| apply 落后 | `apply_lag > 5s` | warn |
| fsync 慢 | `wal_fsync_p99 > 50ms`（Lan） | warn |
| fsync 错误 | `wal_fsync_errors_total > 0` | page（磁盘异常前兆） |
| follower 追不上 | `follower_lag_bytes > wal_trailing_keep` | warn（即将转快照路径） |
| 快照阻塞 apply 过久 | `snapshot_last_duration > 1s` | warn（Q4 预算告警；持续超限是 v1.1 重审双缓冲的信号） |
| 会话表水位 | `session_table_util > 80%` | warn |
| ConfChange 卡死 | `conf_change_pending_age > 10m` | warn |
| 协议不匹配 | `protocol_mismatch_total > 0` | warn（升级窗口外出现即异常） |
| 不可恢复 | `unrecoverable_total > 0` | page |
| force-recovery 使用 | `force_recovery_used_total > 0` | page（人工确认是否预期） |

## 9. 测试策略

> **v0.2.2 起本节为摘要**；完整可执行方案（分层定义、接缝契约、确定性保障清单、替身契约、INV1–15、20 场景矩阵、CI 预算、M0 spike 清单）见 **`test-plan-v0.1.md`**。

### 9.1 测试基建（M0 起就位，越晚加越难改）

- **确定性模拟（L2 主力）**：**turmoil 0.7.2（评审锁定，D-T1）**——跑**真实 tokio + 真实 tonic** 代码路径（种子化单进程调度；partition/hold/release/crash/bounce 原语）。v0.2.1 的 madsim 选型废弃：其磁盘故障为 TODO 桩、且需 `[patch.crates-io]` 换运行时/tonic，测的不再是上线路径（详见变更日志 E 与 test-plan §4 T1）。
- **故障注入存储**：`FaultyStorage` 包装自有 `WalStorage`/`StateMachine` 缝——注入 fsync 失败、撕裂写、按字节截断、位翻转、慢盘，并**维护 fsync 台账以事后对账断言 I1–I4 顺序不变量**。磁盘故障不依赖 turmoil 模拟 FS（WAL 是自有格式，撕裂写须在格式层表达）。
- **崩溃边界真实性**：turmoil `bounce` 后节点必须走完整生产启动路径（META→快照→WAL 回放→raft init），禁止复用内存态——防"崩溃测试空转"。
- **线性化检查**：自建 Wing–Gong 检查器 + 客户端预言机（逐 seq 跟踪、幻值、one-log-id-per-seq、read-your-writes、持久性扫描）；**stateright 0.31.0 以 `dev-dependency` 锁定引入作交叉验证参照与 T3 模型检查引擎（D-T2）**（无成熟 Rust Porcupine 移植，见变更日志 E）。历史由**单一线程 sequencer** 记录，重试按 `(client_id, seq_no)` 合并（§2.2 G1）；完备检查仅跑缩减历史（≤8 客户端 × ≤2000 ops）。
- **并发与模糊**：loom 0.7.2（sync 无锁结构）+ shuttle 0.9.3（async 任务调度竞争）；cargo-fuzz（WAL/RPC/快照/握手解码）。
- **确定性保障**：熵源封堵清单是 M0 交付物（tokio `rng_seed`、禁 `futures::select!`、`Clock`/`Rng` trait、选举抖动 RNG 注入核验）+ **同种子双跑复现门禁**——见 test-plan §5。
- **确定性时钟注入**：会话 GC / TTL 测试用模拟时钟（状态机时间戳随日志传播，天然可测，见 §2.3）。

### 9.2 可执行不变量（每个都是 CI 中的断言，不只是场景）

| # | 不变量 | 验证手段 |
|---|---|---|
| INV1 | 任何消息发出前其载荷已 fsync（I1/I2） | FaultyStorage fsync 计数 + 消息序对账 |
| INV2 | 崩溃恢复后，HardState.commit 及之前的条目全部存在且 apply 结果一致 | 任意 ready 阶段注入 kill，重启比对 |
| INV3 | 状态机确定性：同一日志前缀在任意节点产生相同状态（含会话表） | 双节点同日志回放比对哈希 |
| INV4 | 任意分区/崩溃历史下 put/get 线性一致 | 自建线性化检查器 + 客户端预言机（test-plan §4 T2/§6.4），CI 必过 |
| INV5 | 会话去重：重复 propose 恰好一次生效 | 混沌注入重复请求 + 结果断言 |
| INV6 | WAL 恢复永不静默丢弃 ≤ commit 的数据 | 逐字节截断/位翻转变异 WAL，断言"合法前缀截断 或 fail-start" |

INV1–6 为核心六条；**INV7–15（Raft 安全性全家桶：选举安全、日志匹配、leader completeness、状态机安全含会话表、commit-on-majority、commit 不可回退、单调性、失权 leader 拒读）在 `test-plan-v0.1.md` §7 展开**，各条注明精确陈述、检测方法与强制层级。

### 9.3 混沌场景清单（每个 = 一条种子固定的回归用例）

选举中旧 leader 复活；双分区（多数/少数两侧各验证）；follower 追赶中 leader 切换；WAL 尾部损坏（自动截断路径）；WAL 损坏侵入已提交区间（fail-start 路径）；客户端重试风暴；Learner 追平瞬间断电；快照传输中断后恢复；会话 GC 与重试竞态（模拟时钟）；磁盘满（fsync 失败 → fail-stop）；滚动升级混合版本窗口；force-recovery 后旧多数派复活（cluster_id 隔离验证）；时钟跳变对 TTL 的影响。**完整展开为 20 场景矩阵（场景 × 注入 × 期望 × 不变量 × 层级）与两阶段模糊器模型（safe phase / liveness phase），见 `test-plan-v0.1.md` §8。**

### 9.4 CI 门禁

| 门禁 | 内容 | 频率 |
|---|---|---|
| PR | 单元测试 + `loom`（内部并发：会话表、等待队列、apply 通道）+ shuttle 快集 + 固定种子混沌 S01–S12（各 3 种子，<90s）+ 线性化缩减历史 + **同种子双跑复现金丝雀** + 熵源 grep 门禁（禁 `futures::select!`、禁 sim 路径 `std::time`/`tokio::net`）+ **examples 编译门禁（`cargo build --examples` + clippy `-D warnings`，D-ART）** + `arachne-node` 单进程 CLI 测试（启动校验/优雅关闭，test-plan §3.3） | 每 PR |
| Nightly | 全矩阵 100 随机种子混沌 + 线性化完备检查（缩减历史）+ `cargo-fuzz`（WAL/RPC/快照/握手解码）每 target 10min + stateright 有界模型检查 + L3 多进程冒烟（`arachne-node` ×3 真实进程，15min） | 每夜，分片 ≤4h |
| Release | 混沌清单全绿 ×3 种子 + **真实进程 kill -9 崩溃循环 30min + WAL 校验脚本**（于**专用真实磁盘 CI runner** 执行——v0.2.3 决议 D-L4：runner 随 M0 立项建设，"手工执行/延后"已否决）+ 升级矩阵 v(n-1)↔v(n)（双版本 `binary_hash`，test-plan §3.3）+ **发布产物排除 `test-observability` feature**（feature 解析 + 产物 hash 断言，v0.2.5 决议 D-ART-testobs） | 发版前 |

### 9.5 难测项（如实声明）

- **fsync 真实语义**：模拟环境的 fsync 是"撒谎的"；INV2 必须在真机 kill -9 下复验（Release 门禁，**专用 CI runner 已锁定建设——D-L4**），这是模拟测试无法替代的一项。
- **线性化完备检查的规模**：Wing–Gong 耗时随历史长度指数级，完备检查仅跑缩减历史（≤8 客户端 × ≤2000 ops），日常判定靠 O(n) 预言机（test-plan §6.4）。
- **确定性泄漏是最大隐藏成本**：tokio/futures/raft-rs 选举抖动等熵源未逐一封堵时，双跑复现失效——封堵清单与 spike 核验项见 test-plan §5/§13（raft-rs RNG 若确认不可注入，按 **D-S1 锁定方案**打最小 `[patch.crates-io]` 补丁并上游化）。
- **升级矩阵组合爆炸**：只测相邻版本滚动，不做任意版本对。
- **模拟覆盖面**：确定性模拟覆盖正确性，不覆盖真实网卡/内核栈行为（半开连接、缓冲回压、TLS 边缘）——由 L3/L4 真机门禁补位（test-plan §2 分层表）。

## 10. 里程碑

| 阶段 | 交付 | 可证伪验收标准 | 依赖 |
|---|---|---|---|
| M0 | 单进程 raft-rs 集成（3 个 RawNode 同进程）+ 内存状态机 + WAL；**测试基建：全部接缝 trait（Transport/Clock/Rng/存储）+ FaultyStorage + fsync 台账 + L1 harness（raft-rs harness 模式）+ 存储 Suite + WAL fuzz target + turmoil L2 骨架 + 双跑复现门禁 + spike 清单关闭 + L4 专用 runner 脚手架（D-L4）**（test-plan §12）；**工作区五工件骨架（D-ART）+ `arachne-node` 单节点可运行（/readyz、--config）+ examples 编译门禁** | ① INV2：任意 ready 阶段 kill 注入后重启，commit 前条目零丢失且 apply 结果一致；② INV6：WAL 逐字节变异 fuzz 全部落在"合法截断或 fail-start"；③ INV1 fsync 对账断言通过；④ HardState fsync 策略（I1）由计数断言强制 | — |
| M1 | 3 节点网络、选主、复制、切主、NotLeader 重定向 | ① kill leader 后 ≤ 2×election_timeout 内新 leader 产生；② 期间写返回 `QuorumUnavailable`/`NotLeader` 而非挂死；③ hint 失效时客户端经 seeds 轮询恢复；④ 线性化检查器判定含切主窗口的 put/get 历史线性一致（test-plan §4 T2）；⑤ crate 文档首页包含 §2 语义表与错误矩阵 | M0 |
| M2 | 快照/压缩/追赶 + ReadIndex 读 | ① 写入超过 `snapshot_threshold` 后 WAL 被压缩，新 follower 经快照追平；② 追赶中 kill leader 不中断；③ 任意分区拓扑（含 leader 失联未 step-down 窗口）下 `get` 线性一致（INV14 场景 S02/S16）；④ kill 多数派：写与线性读返回 `QuorumUnavailable`、`get_stale` 仍可用且符合 N1 声明；⑤ apply 独立任务下写洪峰时 `get` p99 有界（延迟预算基准入 CI） | M1 |
| M3 | ConfChange + Learner + 会话幂等 | ① 节点替换全流程（add_learner→追平→promote→remove）期间 quorum 存续、服务不中断；② `remove_member(leader)` 自动先 transfer 成功；③ 已有未提交 ConfChange 时新变更返回 `ConfChangePending`；④ 混沌注入重复请求，同 `(client_id, seq_no)` 恰好一次生效（INV5）；⑤ 会话过期重试返回 `SessionExpired` 且重复窗口 ≤ ttl+grace（模拟时钟测试）；⑥ 快照安装后会话去重状态完好（§5.5.4） | M1, M2 |
| M4 | mTLS、metrics、force-recovery、真实崩溃测试 | ① §9.3 混沌清单全绿（nightly ×3 种子）+ Release 门禁（kill -9 循环 + 升级矩阵）；② 未映射证书/异 cluster_id 握手拒绝；③ §8.2 告警逐条人工演练触发；④ force-recovery 后旧多数派复活被 cluster_id 隔离（场景 12）；⑤ WAL/快照 format_version 回滚限制由测试固化 | M3 |

---

## 11. 关键决策（v0.2 已决议）

### D1 状态机存储：**全内存 + WAL + 快照**（v1）

| 维度 | 全内存 + WAL + 快照 ✅ | 嵌入式 redb |
|---|---|---|
| 事务边界 | 单一日志边界；apply 在进程内原子，快照是唯一持久化形态 | **双日志**（我们的 WAL + redb 自身 WAL），必须把 applied_index 放进 redb 同一事务、回放按 applied_index 幂等跳过——正确性可解但每步 apply 都叠加一次 redb fsync |
| 延迟/吞吐 | apply 无盘操作，最优 | 每次 apply 批多一层 fsync；注册中心读多写少下主要为额外复杂度 |
| 数据上限 | 受内存约束 → 以 `max_value_bytes`/`max_key_bytes`/容量硬上限 + 启动校验对冲（嵌入式库 OOM 会伤及宿主进程，上限必须硬拒绝） | 上限大得多 |
| 快照 | 直接序列化内存结构，简单 | 需处理引擎在线导出一致性 |
| 结论 | 注册中心规模（§12 假设 ≤1 GiB）下复杂度收益比完胜 | 若未来超限，以 `StateMachine` trait 抽象换引擎，v1 不预建 |

### D2 传输：**tonic**

| 维度 | tonic ✅ | 手写帧协议 |
|---|---|---|
| TLS/mTLS | rustls 集成成熟 | 自行维护 TLS 栈与证书逻辑 |
| 快照传输 | server-streaming + HTTP/2 流控天然适配 | 自写分片、校验、背压、断点语义 |
| 版本协商 | 基于 gRPC metadata 实现简单 | 全套自制 |
| 依赖树 | 重（~100+ crates） | 轻 |
| 测试 | **turmoil 承载真实 tonic**（L2 主力，见变更日志 E）；`Transport` trait 隔离生产/测试实现 | 需自写模拟传输 |
| 结论 | 高风险项（TLS/流控/版本化）全部外包给成熟组件；重依赖是可接受的代价，`cargo-audit` 入 CI | 风险自担，收益仅体积 |

### D3 Watch/TTL 排期：**TTL（lease）提前至 v1.5，Watch 留 v2**

| 维度 | TTL @ v1.5 ✅ | Watch @ v2 |
|---|---|---|
| 复用度 | 复用 v1 会话表 + "日志携带时间戳的过期扫描"（过期 = leader propose 删除，线性一致事件，无新一致性问题） | 需要 state machine 增加 revision、事件环形历史 + compaction、resume token 语义——独立子系统与新故障面 |
| 一致性风险 | 低（过期事件走日志，确定性 apply） | 中（事件顺序 vs 快照安装、慢消费者背压） |
| 工作量估计 | 小（~1–2 周） | 大（~4–6 周） |
| 结论 | 注册中心首用户强依赖 lease，提前；Watch 语义独立，不阻塞 v1 | — |

### 决策记录格式约定

每个 D 编号决策如被推翻，须在本节追加 D#-rev 记录触发条件与证据，不静默更改。

## 12. 假设

1. **数据规模**：单集群 KV 总量 ≤ 1 GiB（默认，可配下调；上调需重审 D1），key 数 1e5–1e6 量级，读:写 ≥ 10:1。
2. **集群规模**：3 或 5 节点；节点间 RTT Lan ≤ 5ms / Wan ≤ 300ms；网络可任意丢包/重复/乱序/延迟/分区，无拜占庭行为。
3. **时钟**：正确性不依赖时钟（ReadIndex 不依赖；Lease Read 留 v2 才引入时钟偏移假设）。TTL/会话过期使用 leader 墙钟（随日志传播），精度为弱保证（N6）。NTP 偏移仅用于诊断告警。
4. **运行时**：宿主进程提供 tokio 1.x runtime；库不自建线程池（WAL fsync 专用阻塞线程除外）。
5. **独占性**：单 data_dir 单进程，flock 强制。
6. **v1 无 API 级 ACL/多租户**：安全边界 = mTLS 网络层 + 节点身份。
7. **运维在场**：多数派永久丢失等灾难场景假定有人工介入能力（force-recovery / repair）。

## 13. 风险登记

| # | 风险 | 严重度 | 可能性 | 影响 | 缓解 |
|---|---|---|---|---|---|
| R1 | raft-rs 维护停滞，bugfix 被阻塞 | 高 | 中 | 安全修复无着落 | 共识调用收敛在单一封装模块；M1 末做 openraft fallback 可行性评估点；本地补丁保持最小可上游化 |
| R2 | 内存上限误配 → OOM 伤及宿主进程（嵌入式库） | 高 | 中 | 宿主应用崩溃 | 启动时按上限估算内存硬校验；§8.2 内存告警；硬拒绝超限写入 |
| R3 | 确定性模拟的熵源泄漏与工具演进：turmoil `unstable-fs` 未稳定、tonic/turmoil 版本联动、raft-rs 选举 RNG 疑似不可注入 | 中 | 高 | 复现门禁失效 / 基建受阻 | `Transport` 适配器隔离在单一 crate + 版本 pin；熵源封堵清单 + 同种子双跑复现门禁（test-plan §5）；raft-rs RNG 最小 patch 方案**已锁定（D-S1）**（test-plan §13-S1） |
| R4 | 真实磁盘 fsync 语义与模拟不符，持久性 bug 漏网 | 高 | 低 | 已 ack 数据丢失 | INV1/INV2 真机 kill -9 门禁（M4）+ WAL 校验工具 |
| R5 | force-recovery 误用 → 数据丢失 / 双集群 | 高 | 中 | 灾难放大 | 可达性预检 + 双确认 flag + 默认更换 cluster_id 隔离（§6.1）+ 使用告警 |
| R6 | 单 tick loop 饱和拖垮读延迟 | 中 | 中 | 尾延迟劣化 | apply 独立任务 + 有界通道反压；M2 延迟预算基准 |
| R7 | 海量客户端撑爆会话表 | 中 | 中 | 新会话被拒 | `max_sessions` 硬上限 + 水位告警 + 客户端退避文档 |
| R8 | tonic 依赖树供应链/审计负担 | 低 | 中 | 安全维护成本 | `cargo-audit` 入 CI；依赖锁定 |
| R9 | 混合版本滚动升级窗口内行为漂移 | 中 | 低 | 集群异常 | major 内格式冻结 + 握手校验 + 窗口内禁 ConfChange（§5.6） |
| R10 | 确定性模拟通过但真实栈故障（半开连接等） | 中 | 中 | 上线期故障 | Release 真机混沌门禁；传输层超时全显式配置 |

## 14. 决策记录（v0.2.1 锁定）

v0.2 曾列为开放问题的 7 项，经评审**全部决议**并已传播至生效章节（见变更日志 D 表的落点索引）。此处保留决议原文与**未来重审触发条件**——重审触发条件是监视信号，不是未决问题。

| # | 议题 | 决议（锁定） | 落点 | 未来重审触发条件 |
|---|---|---|---|---|
| Q1 | `session_ttl` 默认值；`client_id` 是否持久化以支持跨进程重启去重 | TTL 60s；不持久化（进程重启 = 新会话，N5） | §2.3、§7 | 首个注册中心用户接入时按其重试窗口校准 |
| Q2 | ReadIndex 超时/重试参数 | `2×election_timeout`，重试 1 次 | §5.4、§7 | M2 基准数据出来后 |
| Q3 | `transfer_leader` 是否作为公开 API 暴露 | 暴露（shutdown 与 remove-leader 流程已内部依赖，藏不住不如明示） | §3.1、§5.3 | M3 API review |
| Q4 | 快照期间阻塞 apply 的预算；超预算何时升级为双缓冲/持久结构 | 阻塞 + `snapshot_last_duration` 告警阈值 1s；数据量逼近内存上限时 v1.1 重审 | §5.5.4、§8.2 | 容量告警首次触发 |
| Q5 | `get_stale` 是否提供 per-handle 单调读变体（记录已见 applied 水位） | v1 不提供，仅文档声明非单调；若用户强需求，v1.1 加低成本水位追随变体 | §2.1、§5.4 | 首个用户反馈 |
| Q6 | `wal_trailing_keep` 是否与 `snapshot_threshold` 解耦 | 绑定同值简化运维，出现慢 follower 快照风暴证据再解耦 | §5.5.4、§7 | M2 追赶测试 |
| Q7 | 提案队列按字节还是按条数计 | 按字节（64MB），对大值更稳 | §4.1、§7 | M1 压测 |

**截至 v0.2.10 无未决设计问题。** §12 假设与 §13 风险为需在实现与运维期持续监视的事项，不属于开放设计决策；推翻任何锁定决议须按 §11 的决策记录格式约定追加 rev 条目（上游 D/E/F/G/H/I/J/L/M 系列，test-plan 的 D-T/D-L/D-ART/D-ART-rev1/S 系列）。

---

*v0.2.10 完（v0.1 → v0.2 设计精化；v0.2.1 锁定参数级决议；v0.2.2 修订测试基建选型并产出 `test-plan-v0.1.md`；v0.2.3 锁定测试方案四项决策 D-T1/D-T2/D-L4/D-S1；v0.2.4 锁定工件与工作区决策 D-ART——`arachne-node` 运维 bin 为生产交付物；v0.2.5 锁定 D-ART 四项子决策：crate 命名、默认 feature 拉 tonic、TOML 配置、`test-observability` 门禁；v0.2.6 D-ART-rev1：抽出 `arachne-seam` 叶 crate，消除 `transport-tonic → arachne` 包循环；**v0.2.7 §5.5.3 恢复算法安全收紧（E-rev）：坏记录不再嗅探类型字节、仅末段结构性撕裂可截断、`commit ≤ last_index` 断言 + 新段目录 fsync**；**v0.2.8 §5.1/§7 约束注解与预设自相矛盾修正（E-rev）：`rpc_timeout < election_timeout`、`election_timeout ≥ 5× heartbeat`**；**v0.2.9 INV2 的测试专用崩溃注入点（E-rev）：feature `fault-injection` 默认关、发布构建排除，仅在 `RaftNode::step` 的 persist/deliver 边界提供一次性崩溃 hook**；**v0.2.10 快照落盘契约、META 快照指针与压缩边界（E-rev）：独立 `snapshot-<index>-<term>.snap` 文件 + 全负载 CRC、目录扫描取最新合法快照（保留最新两份）、META payload 尾部追加快照指针（版本不变）、段粒度压缩（字节级 trailing 窗口留待接线）、逻辑增长触发快照、follower 安装快照的整体替换语义**）。下一步：按 test-plan §12 的 M0 交付测试基建骨架、六工件工作区（D-ART-rev1 增 `arachne-seam` 叶 crate）与接缝 spike 清单（§13），再进入 raft-rs 集成原型（属实现工作，另立任务）。*
