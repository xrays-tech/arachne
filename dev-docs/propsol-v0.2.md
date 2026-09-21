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
| **（v0.2.10 落地范围）** | 本版 `compact(to)` 实现为**段粒度全删**（`to` 之前完全覆盖的段）；字节级 trailing 窗口当时未接线 | 当时即接字节保留 | 保留窗口是空间/带宽的优化而非正确性条件：删段后落后 follower 由 `Compacted` 路径拉快照，语义不变 |
| 快照触发口径 | **逻辑增长**：自上次快照以来**已 apply 的日志字节数** ≥ `snapshot_threshold` 即触发（快照后归零）；`wal_bytes` 仍作为物理占用指标上报 | ①以物理段字节数为触发输入；②按条数触发 | ①段粒度压缩下物理字节**不会**因压缩而下降（含已压缩前缀的段要等下一次 rollover 才能回收），用物理字节会在跨过阈值的每个条目上重复触发快照；②按条数偏离 §7 的字节口径。物理占用仍上报，运维据此判断回收进度 |
| follower 安装快照的日志语义 | **整体替换**：`install_snapshot` 持久化后令 `compacted_to = snapshot.index`、**清空本地全部条目**，并把物理日志轮换为从 `snapshot.index + 1` 起的空段（删除其余所有段） | ①只删 ≤ index 的条目；②原地保留 > index 的条目 | 安装快照时本地 > index 的条目必属另一分支且未提交（raft 断言 `snapshot.index ≥ committed`）；保留它们会让 `last_index()` 报出 raft 视图之外的尾巴，恢复时还会看到"陈旧条目 + leader 重发条目"之间的**空洞**而 fail-start（实测）。对齐 etcd `MemoryStorage.ApplySnapshot` 的整体替换语义，缺的条目由 leader 重发 |
| 恢复时的成员配置 | **`initial_state()` 返回快照携带的 `ConfState`**（无快照时回落到 `initial_state` 引导集） | 始终回落到引导集 | §5.5.3 第 6 步"ConfState 来自快照/日志中的 ConfChange"；安装过快照的节点重启后必须知道自己属于哪个集群 |

落点：§5.5.3 第 3/5/6 步、§5.5.4；实现 `arachne/src/storage/snapshot.rs`（新）、`meta.rs`、`wal.rs`、`consensus/raft_storage.rs`、`consensus/node.rs`、`runtime/mod.rs`。

### N. v0.2.11：apply 独立任务的通道契约与反压口径（E-rev）

M2 验收 ⑤ 要把 apply 从 actor loop 里挪出去（§347 / §701 R6）。挪动本身是结构问题，但一旦 apply 异步化，「什么时候可以回复客户端」「读什么时候算可见」「提案满了怎么办」就从「同一条控制流上的顺序」变成**跨任务的契约**，必须钉死。

| 决策点 | 选择 | 否决的备选 | 理由 |
|---|---|---|---|
| apply 通道形态 | actor → apply **两**条通道：`ApplyRequest`（快照安装 + 已提交条目批量、快照序列化请求）有界；`ReadRequest`（弱读）独立有界 | ①单通道（弱读排在写积压后面）；②共享 `Arc<RwLock<SM>>` | ①`get_stale` 的契约是「读本地已应用状态」，让它排在写积压后面等于把弱读绑上写延迟。②seam 明确 `StateMachine: Send` 而非 `Sync`（单任务独占），加锁会把 apply 的临界区带进 actor。 |
| apply → actor 进度 | **`watch` 单值**：`{applied_index, applied_bytes_total, failed}` | 有界/无界 mpsc 逐条进度 | actor 只需要「最新应用到哪」；逐条进度会在 actor 慢时反过来阻塞 apply。`watch` 只保最新值且不反压，`applied_bytes_total` 是累计量，与 actor 侧 `sent_bytes_total` 相减即得积压，不必逐条对账。`failed` 为**粘性**字段，合并更新也不会丢。 |
| apply 滞后时的提案反压 | **字节口径**：`sent_bytes_total − applied_bytes_total ≥ proposal_queue_bytes`（Q7 的 64MB）→ 新提案回 `Busy`，计入 `proposal_busy_total` | ①按条数；②无界吸收 | Q7 已锁定「提案队列按字节计容、触顶 Busy」，但此前**从未接线**。把它对准「已提交但尚未 apply 的字节」才是这条承诺的真实含义：队列不是 actor 内存里的一个 Vec，而是「已经落盘、客户端还看不见」的那段日志。字节口径对大 value 更稳（Q7 原话）。 |
| 通道满了怎么办 | actor 侧**持有一个待发批次**（`try_send` 失败就留着下轮再试），新到的已提交条目**合并**进去；合并时若新 Ready 带快照则**丢弃被覆盖的旧条目** | ①`send().await` 阻塞 actor；②丢弃条目 | ①阻塞 actor 等于把 apply 的背压传导成读延迟——正是 ⑤ 要消除的。②条目不能丢。合并安全：待发条目必 ≤ commit ≤ 新快照 index，被新快照完全覆盖。 |
| 快照请求 | 走 `ApplyRequest` 通道（与 apply 同序），apply 任务返回 `(applied_index, bytes)`，actor 再 `term_at` + `create_snapshot` + `compact` | actor 直接读 SM | 同序才能保证快照 index 与内容是同一时刻；Q4 的「创建阻塞 apply」自然成立（apply 任务在序列化期间不 apply）。 |
| 回复提案的判据 | actor 侧维护 `(client_id, seq_no) → index` 归属，`applied_index ≥ index` 即回复 | apply 任务回传「本批应用的 session 列表」 | actor 本来就知道自己发给 apply 的每条条目（含 index 与命令字节），归属在 actor 侧算只依赖累计进度，apply 任务保持「只管 apply」。 |
| raft 的 `applied` 由谁推进 | **不碰**：raft-rs 的 `RawNode::advance` 在条目**交付**时（`commit_since_index`）就把 `applied` 推到交付点，`raft.applied` 合法地领先真实 SM 一个「apply 任务积压」。需要「SM 真正应用到哪」的地方（快照 index、读可见性、提案回复）一律用 apply 任务回报的 `applied_index` | 收到进度确认后 `advance_apply_to(applied)` | 实测该做法**直接 fatal**：`applied(1) is out of range [prev_applied(2), …]`——raft-rs 的 `applied_to` 禁止回退。异步 apply 下「交付」与「应用」本就分离，raft 领先是这套 API 的既定模型（所有 raft-rs 应用都这么做）；把两个口径混用才会撞上这条断言。 |
| actor 跑在哪个线程 | **专用 OS 线程**（`Runtime::spawn_dedicated`：`std::thread` + current-thread tokio runtime + `block_on`）；`run()` 仍保留给测试/嵌入方自行放置 | 在调用方的 tokio 多线程 runtime 上 `tokio::spawn` | actor 每次 `sync_entries`/`set_hard_state`/META 写都是**阻塞** `fsync`（全量落盘本机约 10ms），放在共享 worker 上会把传输与客户端任务一起饿死（实测：洪峰中**所有**客户端操作的 p99 齐平在 ~100ms）。阻塞 I/O 归专用线程，async runtime 留给网络/客户端。**边界**：这只消除 worker 饥饿，不改变 actor 自身「一次 flush 串行」——读仍要等当前 flush 结束；真正压低该上界是 `FsyncPolicy::BatchMs` 的 group commit（另一增量）。也**不能**用 `block_in_place` 替代：它在 current-thread runtime（turmoil/L2 与专用线程）上会 panic。 |
| 延迟预算口径 | 同一轮内**三个参考量互相约束**：写 p99、弱读 p99、线性一致读 p99；断言「弱读 ≤ 4×写」「线性一致读 ≤ 3×弱读」+ 宽松绝对上限。时钟用 `tokio::time::Instant`（entropy Gate C 只禁 `std::time`） | ①绝对 p99 预算；②只比空载基线 | **实测教训**：`FsyncPolicy::Always` 下写路径每次全量落盘在本机约 10ms（macOS `sync_all`），洪峰中写/弱读/线性读 p99 **齐平在 ~100ms**——瓶颈是写路径的落盘序列化点，不是 apply。绝对阈值只会测出磁盘快慢（慢盘必挂、快盘无意义）；而「空载 vs 洪峰」比值同样由落盘决定。三量互相约束才是在测「apply 任务没有额外排队」：弱读若被 apply 积压拖住会先炸，读与弱读的差则正好是 ReadIndex 轮次 + 等待 read index 被 apply。真实数字：idle 0.5ms / storm 写 106ms、弱读 99ms、线性读 100ms。 |


---

### O. v0.2.12：不批 fsync，改批「持久化周期」（E-rev）

M2 ⑤ 落地后实测到：`FsyncPolicy::Always` 下一次全量落盘（macOS `sync_all` 是整设备刷新）本机约 10ms，写洪峰中**写/弱读/线性读的 p99 齐平在 ~100ms**——瓶颈是写路径的落盘序列化。于是"给条目路径做 group commit（实现 `BatchMs`）"看似是下一步，但**在本架构里不成立**，本轮改为批「周期」，并把结论钉在这里。

| 决策点 | 选择 | 否决的备选 | 理由 |
|---|---|---|---|
| 条目路径批 fsync | **不做**。维持 I2/I4 的"`sync_entries` 返回即持久"，`BatchMs` 仍是**未实现**的占位（不再含糊其辞） | ①延后 `ms` 再刷；②合并同周期两次 flush | ①单写者 WAL（`sync_entries(&mut self)`，actor 独占）**没有可合并的并发等待者**，"等 ms 攒一批"只会把 ack 提前到落盘之前，直接违反 I1/I2/I4，`m2_durability.rs` 的台账对账会（正确地）判失败。②一个周期内的两次 flush 是**结构性**的：entries 必须在 `advance` 前持久（否则 I2/I4），而 commit 索引要 `advance` 之后才知道 → `set_hard_state` 必须再刷一次。两者之间必然有一次写入，无法合并。 |
| 降低落盘成本的杠杆 | **批「持久化周期」**：actor 每轮先把**已排队**的命令与入站消息成批处理（上限 `CYCLE_BURST=64`），再走一次 `drive_cycle` | 提高 raft 的 `max_msg_size`/`max_inflight_msgs` | 洪峰里每次写几乎独占一个周期（各写者等 ack，每轮只有 1–4 个条目）→ 每写 2 次 flush。批周期让 raft 组更大的 `Ready`、多个 commit 前进共享一次 flush，且**不触碰任何持久化语义**。调 raft 参数只影响单条消息大小，不改变"每条消息一个 step"的节奏。 |
| seam 支持 | `TransportRx::try_recv()`：**默认 `None`**（= "现在没排队/无法非阻塞"，安全，因为被拒绝交接的消息仍由 `recv` 送达），tonic 与内存传输都已实现 | 只给内存传输加 | 生产（tonic）才是 fsync 瓶颈所在，只优化测试传输没有意义。 |
| 实测收益（同机、`read_latency`） | 洪峰写 p99 **106–157ms → 38–53ms**；线性读 **100–144ms → 38–52ms**；弱读 **85–99ms → 0.17ms**；测试耗时 6.1s → 4.1s | —— | 弱读的巨幅改善来自 actor 不再"每条消息一次 flush 对"地卡住，act→apply 的跳转能被及时服务。 |
| 仍未做 | 阻塞 fsync 仍在 actor 线程上同步等待（actor 已按 rev N 移出 async worker）；要消除"读等当前 flush"，需要**异步持久化流水线**（写缓冲 + 专用 flusher + 消息/advance 延后到 flush 完成），那是独立 rev 与独立故障注入验证 | —— | 它把 I2/I4 的时序从"同一控制流顺序"改成显式状态机，风险与工作量都不是本增量级别。 |

### P. v0.2.13：异步持久化流水线（E-rev，分阶段）

rev O 把洪峰中的写/读 p99 从 ~100ms 降到 38–53ms（批持久化周期），但 fsync 仍是 actor 线程上的**同步等待**：读仍要等 actor 正卡在的那次 flush。要再往下压就必须把"写"与"刷"分开，并且允许 actor 在 flush 在途时继续推进读。这是本设计要钉的东西——它改的是 I2/I4 的**实现方式**，不是它们的内容。

| 决策点 | 选择 | 否决的备选 | 理由 |
|---|---|---|---|
| 异步骨架 | **用 raft-rs 自带协议**：`Ready::number()` + `advance_append_async(rd)`（= 只 `commit_ready`，显式**不**推进 `persisted`）+ 落盘完成后 `on_persist_ready(number)` | 自建 ready→persisted 映射 | raft-rs 的文档把边界写得很清楚："it's still required that the updates can be read by raft from the `Storage` trait before calling `advance_append_async`"，且 `on_persist_ready` 按 number 顺序推进 `persisted`/`on_persist_snap`。自建同类机制只会更容易错。（已在本仓库 vendored 源码核对：`raw_node.rs:112/617/697`。） |
| 写与刷分离 | actor 侧只**写**（`append` 进页缓存、顺序写 HardState 记录）；worker 侧 `FlushHandle::flush()`——dup 的 fd 上 `fsync` 刷的是 **inode**，覆盖此前经原 fd 写入的全部字节 | 把整个 `WalStorage` 搬到 worker 线程 | storage 必须能被 raft **同步读**（`initial_state`/`entries`/`term`/`first_index`/`last_index`/`snapshot`），搬走就做不到。dup fd 让"写"留在 actor、"刷"离开 actor，读路径一行不改。 |
| rollover 期间的覆盖 | 沿用既有 N2 修复：`maybe_rollover` 切段前 fsync 出站段（`pending_entry_fsync`），所以 rollover 之前的条目已持久；worker 只需刷当前段 | 引入跨段 flush 协调 | 已有机制已经保证"切段前的字节必持久"，新增协调是重复造轮子。 |
| 发送与 advance 的时机 | 一律推迟到 flush 完成**之后**：`immediate`/`persisted` 消息、`advance` 的 LightReady 副作用、commit 落盘、apply 全部在 `on_persist_ready` 之后发生 | 部分消息提前发 | I2/I4「消息不得携带未持久负载」原样成立——`m2_durability.rs` 在 `Transport::send` 处的 fsync 台账对账因此仍然有效，且正是它要守的东西。 |
| 崩溃窗口 | 提交点仍是"记录已落盘"：`on_persist_ready` 之后才 advance/发送/apply；HardState 的落盘与条目**共享同一次 flush**（同段），不给"已 ack 未持久"留窗口 | 把 HardState 拖到下一次 flush 覆盖 | 那等于重新打开 I1 明确关闭的窗口（改的是实现方式，不是承诺）。 |
| 读在 flush 期间能否推进 | 依赖 raft-rs 允许**多个 ready 在途**（records 队列 + `on_persist_ready(number)` 顺序推进）：actor 在 flush 在途时仍可 `step()` 并发出 ReadIndex 心跳，只是其消息要等各自 ready 落盘后才发 | 只允许一个 ready 在途 | 单在途时读仍被当前 flush 阻塞，等于没解决问题（rev O 已把 flush 成本压到每次 ~2 次/批，读 p99 仍 38–53ms）。 |
| 分阶段（每阶段独立可验证、各自保持全绿） | **P1（本次已完成）** `Segment::try_clone_file()` + `WalStorage::flush_handle()`（dup fd + 段号），单测证明"取 handle 之后写入的字节也被这次 flush 覆盖"。**P2** `WalStorage` 增"只写不刷"路径（inherent，seam 的 `sync_entries` 语义一行不改）。**P3** `RaftNode::step` 拆 submit/complete 两相（`pending: Option<PendingReady{number,…}>`），消息与 `committed`/`read_states` 在完成相产出。**P4** runtime 增"持久化完成"事件分支，读/提案在 flush 在途时仍可推进。**P5** 更新**所有直接驱动 `step()` 的 harness**（`l2_scenarios` 3 处、`m2_durability` 3 处、`force_recovery` 等）、fault-injection 两个阶段（`AfterPersist`/`AfterDeliver`）的语义，并新增"flush 在途时崩溃"的 INV2 场景 | 一次性重构 | `step()` 的 outcome 形状会被每一个直接驱动它的 harness 观察到；必须分阶段、每阶段跑全绿，才能保证 INV1/INV2 的回归被当场抓住。 |

**实测结论（本设计最重要的一条，已推翻上面那句预期）**：P1–P4 全部落地后实测，pipeline **没有降低 p99**，反而略差：

| 场景（`read_latency`，p99 / 120 次读） | 同步（默认） | 离线程 pipeline（in-flight=8 / 2 / 1） |
|---|---|---|
| 洪峰线性读 | 38–53ms | 47–55 / 50 / 52ms |
| 洪峰写 | 38–53ms | 49–64 / 54 / 57ms |
| 弱读 | 0.17ms | 0.14–8.5ms |

原因：`FsyncPolicy::Always` 下每个周期都要一次真实设备 flush（本机 ~10ms），**设备就是瓶颈**。把 flush 挪到别的线程只改变"谁在等"，不改变"要等多久"；而 in-flight 窗口一大，客户端操作还要排在更多次 flush 之后（FIFO），所以更差。**能降低 p99 的只有"减少 flush 次数"**（rev O 的批周期已做，rev P 的这条做不到）。

因此 pipeline 的定位改为**可用性而非延迟**：它保证磁盘慢时 actor 仍能 tick/心跳/服务（`slow_fsync` 这类场景下，actor 不会因为一次 500ms 的 fsync 而停摆到丢选票）。据此：
- pipeline **默认关闭**（`WalStorage::enable_offloaded_durability()` 显式开启），默认路径与引入前逐字节一致；
- **不**在 node binary 里默认打开，也不把 `read_latency` 门槛切到 pipeline 上（门槛测的是默认路径）；
- 端到端正确性由 `lagging_follower_catches_up_with_offloaded_durability` 覆盖（同一套"写洪峰 → 快照 → follower 安装 → 重启 → 换主"场景跑在 pipeline 上）。

**修好测试传输后的重测（重要）**：上面那组"pipeline 更差"的数字是在一个**有缺陷的内存传输**上测的——它的 `RecvFuture` 从未向通道注册 waker，导致每条入站 raft 消息最多等一个 heartbeat（详见 handoff §1.12）。修好后（idle 读 p99 从 10.8ms 回到 0.23–0.66ms）重测：同步 storm 33–70ms、pipeline 50–56ms，**结论不变**（`Always` 下设备是瓶颈）。另外新加的 idle 门槛当天就抓到 pipeline 的一个真问题：`persist_ready_records` 原先在"本次没写任何记录"时也会提交 flush，使一次空周期（例如一个 ReadIndex 轮次）搭上更早周期的 flush（idle 6.7ms）。现在只有**本次真的写了记录**才提交 flush——空记录周期直接 `Durable`，因为调用方按提交顺序完成周期，更早周期的负载在它被报告前必然已持久。

**实现中发现的第二个要点**：不要像同步路径那样在 `land_records` 里显式写"commit 前进"的 HardState。raft 的 `prev_hs` 在 async 路径下不会被 `gen_light_ready` 更新，所以下一次 `ready()` 会自己带上这个 hs；若我们再显式写一次，磁盘上就会出现**重复的 HardState 记录**，并直接打破"末条撕裂=合法截断"的判定（实测：`structural tear at estimated index 3 (within committed window commit=2)`，`m2_wal_faults::torn_tail_recovers_and_still_serves_the_acked_write` 抓到）。删掉显式写入后，每个周期恰好一次 flush，状态机也简化成单阶段。

### Q. v0.2.14：`wal_trailing_keep` 的语义与落点（E-rev）

rev M 留了"字节级 trailing 窗口"，但当时没定义它到底保留什么。本轮接线时先钉死语义，因为字面读法（"保留 `to` 之前的段直到其字节量 ≤ keep"）会保留一批**逻辑上已经不存在**的字节。

| 决策点 | 选择 | 否决的备选 | 理由 |
|---|---|---|---|
| 窗口保留的是什么 | **把水位本身压住**：`compact(to)` 只从最旧段开始删，且一旦"删掉这段后仍可达的日志字节 < `wal_trailing_keep`"就停。`first_index` 因此停在最旧保留段的首索引上 | 按字面保留 `to` 之下的段（水位照常推进） | 水位以下的条目对 `entries()` 已经是 `Compacted`，**raft 拿不到**；把它们留在盘上对追赶毫无帮助（只对"最新快照损坏时回退上一份"有意义，而那份由 `SNAPSHOT_RETENTION=2` 覆盖）。慢 follower 真正需要的是水位别推那么远：这样 `next_idx ≥ first_index` 时可直接用日志补，超出窗口才走快照（§5.5.4/Q6 的原意）。 |
| 落点 | `WalStorage::set_trailing_keep_bytes`（inherent setter，默认 0）；node binary 从 `profile.wal_trailing_keep_bytes` 设置（Lan/Wan 64MB/16MB） | 加进 `WalConfig`/`WalOptions` | 加字段要改 19 处字面量构造，而窗口是**存储实例的策略**、默认 0 即完全保持旧行为；setter 让默认路径与既有测试逐字节不变。 |
| 成本边界 | WAL 稳态上界 ≈ `wal_trailing_keep + snapshot_threshold`（Q6 两者同值，故约 2×）；快照照常创建 | 让窗口无限大 | 窗口是空间换带宽的取舍，必须有界。 |
| 验证 | 单测：窗口 > 0 时水位被压住且保留字节 ≥ 窗口、窗口 = 0 时行为与旧版一致；端到端 `m2_trailing_keep`：先在**全员在册**时写够（leader 真的删掉了最旧段），再让一个 follower 掉线并只写**少于窗口**的量，重启后它必须由日志补上——`snapshots_installed_total == 0` | 只写单测 | 该性质是"leader 的裁剪 + follower 的追赶路径"合起来的结果；**反向对照**：把窗口设成 0 时同一场景必须失败（实测确实失败：`installed == 1`），证明测试非空洞。 |

### R. v0.2.15：会话幂等收尾——TTL / grace / `max_sessions` / GC（E-rev，待实现）

M3 缺的最后一块（M3 验收 ④⑤、INV5 S07/S10/S14）。先记录一个**已存在的缺陷**：`KvStateMachine` 的会话表是 `BTreeMap<SessionKey, ApplyOutcome>`，**没有任何淘汰**——每写一次就多一条，长期运行是无界增长；`session_ttl_ms`/`session_grace_period_ms`/`max_sessions` 三个旋钮至今无人读（`scripts/check-profile-knobs.sh` 白名单里的三项）。去重本身是对的（快照里也带会话表 ✓ M3 ⑥），缺的是"会话何时失效、失效时怎么回答、表满了怎么办"。

| 决策点 | 选择 | 否决的备选 | 理由 |
|---|---|---|---|
| 过期决策在哪做 | **leader 侧**（本机时钟）+ **复制的显式 GC 条目** | ①把时间戳塞进每条命令、由 SM 判定；②纯本地淘汰不复制 | ①`encode_put/delete` 是公开 API、所有 harness 都在用，改格式代价大；且客户端时钟不可信。②淘汰必须所有副本一致地删同一集合，否则去重状态与快照分叉 → 必须走日志。 |
| 会话状态拆两层 | SM 里 `(client_id, seq_no) -> outcome`（复制、已有）+ leader 本地 `(client_id, seq_no) -> last_used`（不复制、不进快照） | 把 `last_used` 放进 SM | 放进 SM 就必须把时间戳写进日志（同上）。本地表语义 = "本 leader 认为会话还活着多久"；换主后重新计时，只会让会话**活得更久**（更保守，不会双重生效）。 |
| 三个区间的语义 | ≤ TTL：正常提议（SM 去重）；TTL..TTL+grace：**回 `SessionExpired` 且不提议**；> TTL+grace：GC 后可当新会话 | 中段也照常提议 | 中段若照常提议，SM 里通常仍有该会话 → 返回旧结果是安全的；但**一旦 GC 已删**，同 `seq_no` 会被当成新命令**重复生效**。所以中段必须显式回答"结果未知"（`ArachneError::SessionExpired` 的文档正是 "Result unknown"），且 GC 只能在 `ttl+grace` 之后删——这给出 INV5 要的"重复窗口 ≤ ttl+grace"。 |
| `max_sessions` | leader 检查"本地已知会话数 ≥ `max_sessions` 且该会话未知" → `SessionTableFull`（不提议）；GC 释放后恢复 | 在 SM 里拒绝 | 已提交条目必须被所有副本一致 apply，SM 不能"拒绝"；容量检查只能发生在提议之前。表大小由 apply 任务在 `watch` 进度里发布（`sessions.len()`），leader 用本地表做"是否已知"的判断。 |
| GC 形态 | leader 每 `ttl` 一次，把 `last_used + ttl + grace < now` 的会话按**显式列表**（每条上限 1000）提议成 GC 命令；SM 按列表删除（幂等） | 提议"删除 ≤ cutoff" | cutoff 依赖时钟、副本无法重放；显式列表确定性、且天然幂等。 |
| 时钟来源 | 会话 TTL 读 **`Clock` seam**（`arachne_seam::Clock`），测试注入 `ManualClock`；I/O 超时继续用 `tokio::time::Instant` | TTL 也用 `Instant` | 否则 TTL 无法用模拟时钟做确定性测试（S10/S14 要求"模拟时钟推进过 TTL"）；`RuntimeConfig` 需要新增 clock 字段，约 10 处字面量构造要跟着改（机械改动，已知代价）。 |
| 快照 | 会话 outcome 表照旧进快照（已有 ✓ M3 ⑥）；`last_used` 不进 | 把 `last_used` 写进快照 | 快照必须可重放，时钟值不可重放。 |
| 验证（M3 ④⑤、INV5 S07/S10/S14） | `ManualClock` 驱动：①TTL 内重试 → oracle 判"同 seq 恰好一次"（已有）；②越过 TTL 未过 grace → `SessionExpired` **且 replicated index 未增长**（证明没有提议）；③越过 `ttl+grace` → GC 之后同 seq 可被当新会话执行，断言**重复生效窗口 ≤ ttl+grace**；④填满 `max_sessions` → 新会话得 `SessionTableFull`，GC 后恢复；⑤快照安装后去重完好（已有 + 补一条跨安装的重试断言） | 只做单元测试 | 这些都要求"日志里到底有没有条目"和"时钟推进"两个观测点，只有端到端 + 模拟时钟能同时给到。 |

**落地顺序（R1 已落地）**：**R1 ✓** 会话 TTL/grace 三区间 + leader 本地表 + `SessionExpired`——时钟经 `Runtime::with_session_clock`（builder，**不加** `RuntimeConfig` 字段以免动 ~10 处构造；不设置即"永不过期"= 旧行为），`session_ttl_ms`/`session_grace_period_ms` 由此被真正读取（两项已从 `check-profile-knobs.sh` 白名单移除）；本地表按 `ttl+grace` 窗口定期清扫，因此有界。端到端 `arachne/tests/sessions.rs`（feature 门控，配 `ManualClock` + `Handle::propose_raw` 才能复现"同 session 重试"）：TTL 内重试用**不同值**提议 → 断言原值存活（恰好一次，而非最后写入胜）；越 TTL 未过 grace → `SessionExpired` **且 `applied_index`/`commit_index` 不动**（证明没进日志）；越过 grace → 重新接受，而 SM 仍去重故效果仍为一次。**反向对照**：把 `session_ttl_ms` 设 0 → 该断言失败（测试非空洞）。**R2 ✓** GC 命令 + `max_sessions` 上限（同批落地，否则填满后新会话被永久拒绝）：
- SM 新增 `OP_SESSION_GC` 命令：`[op:1][count:u32][(client_id,seq_no)×count]`，**显式列表**（副本必须删同一集合，时间戳无法重放）；`apply` 只删列出的会话、不动 KV；`command_session` 对它返回 `None`（因此它既不归属提案也不延长会话）；新增 `session_count()` 与 `encode_session_gc`。
- runtime：`ApplyProgress` 增 `sessions`（由 apply 任务发布 `sm.session_count()`），actor 存 `live_sessions`；leader 每 `ttl` 检查一次，把 `now - last_used > ttl+grace` 的会话按上限 1000/条提议为 GC；**只在有新东西过期时才提议**（空闲集群保持静默），提议成功后从本地表删除以免重复提议。
- `max_sessions`：**只有 Fresh（新）会话**会在提议前被拒（`SessionTableFull`），且用**复制的**表大小判断；已有会话的重试永不被容量拒绝。换主后本地表为空 → 在极端情况下（表满 + 换主）可能误拒一个合法的重试，可重试恢复，已记录。
- 指标新增 `arachne_session_count`（propsol §8 本来就要求 `session_count`）。
- 验证：单测（GC 只删列出的、KV 不变、畸形 GC 被拒且不部分应用）；端到端 `session_gc_prunes_and_relieves_the_session_cap`：4 会话填满 → 第 5 个新会话得 `SessionTableFull` → 时钟越过 `ttl+grace` → GC 后 `session_count` 归 0 → 新会话重新被接受 → 且 KV 数据未被 GC 触碰。**反向对照**：`session_ttl_ms = 0` 时两个会话测试都失败（非空洞）。
- `check-profile-knobs.sh` 白名单**只剩 `snapshot_transfer_rate_bps`**（三项会话旋钮全部真被读）。
- **R3 ✓**：INV5 三个场景全部落地（`arachne/tests/sessions.rs`，`ManualClock` + `Handle::propose_raw`）：**S07** 24 并发同 session 重试（不同值）→ 全部接受、落定值此后不再移动（断言与竞态顺序无关）；**S10** 窗口内重试 `SessionExpired` 且 `applied_index` 不变 → 越 `ttl+grace` 后 GC 清空、同 seq 才作为新命令生效（把"重复生效窗口 ≤ ttl+grace"实测出来）；**S14** 时钟前跳一小时 → 会话全过期、数据完好、新会话可用。会话一侧（M3 ④⑤ + INV5）到此完结；M3 只剩 ConfChange（①②③⑥）。

### S. v0.2.16：ConfChange 落地设计——成员变更、Learner 追赶、持久化 ConfState（E-rev，待实现）

M3 的最后一块（验收 ①②③⑥）。先记录**两个已存在的缺陷**——它们正是"加 API 之前必须先补设计"的原因，而不是收尾细节：

1. **ConfChange 条目今天会被当作 KV 命令喂给状态机**。`RaftNode` 的注释（`node.rs:374`）写着 "is dropped for now"，但实际的提交条目**统一进 apply 通道**：ConfChange 条目的 `data` 是 ConfChange protobuf、不是 KV 命令编码 → `KvStateMachine` 解码失败 → 走 fail-stop。今天不可达（没有任何 API 能提议 ConfChange；`raft_storage.rs` 只做了 `EntryConfChange(V2)` 的类型映射），但一旦按 M3 加 API，**第一个成员变更提案就会打死整个集群**。所以"按类型路由 ConfChange 条目"是 S1 的前置条件。
2. **持久化 ConfState 没有落点**。§5.5.3 / §5.6 已承诺"非首启以持久化 ConfState 为准"，快照格式（§5.5.4）也已带 voters/learners 成员表——但**段内没有任何成员记录**，`RaftStorage::initial_state` 只能退回 `bootstrap_conf_state`（`raft_storage.rs:231`）。后果：**成员变更在重启后一律丢失**，集群退回 `initial_cluster`（§5.6 的"配置文件不必跟着改"因此不成立）。"靠快照顺带记成员"也不成立：快照只在越过 `snapshot_threshold` 时产生，小集群可能长期没有快照。

| 决策点 | 选择 | 否决的备选 | 理由 |
|---|---|---|---|
| 持久化 ConfState 落点 | **独立文件 `membership`**：`[u32 magic][u32 format_version][u64 conf_change_index][成员表][u32 crc32c]`，写入走 `membership.tmp` → fsync → rename → fsync(dir)（与 META 同纪律） | ①**WAL 新增记录类型 `ConfState(0x04)`**（本 rev 初稿的选择，**实现时被否决**）；②写进 META 文件；③只靠快照成员表 | ①**否决原因是一个实现时才暴露的硬冲突**：恢复对**末段结构性撕裂**只在"撕裂记录的估算条目 index > `commit + 1`"时才自动截断，而撕裂记录的**类型字节不可信**（在 CRC 保护体内，§5.5.3 的 P2 教训），所以恢复必须假设尾部可能是**更新的 HardState**（丢掉 term/vote 会破 INV7、允许同 term 双投票）。稳态下 `commit == last_index`，于是"成员变更记录写到一半就崩溃"会被判成 fail-start、需要人工 force-recovery——把例行崩溃变成运维事件，不可接受。独立文件用原子替换：崩溃后只可能是**旧的或新的**成员配置，永不是撕裂的，且不给 WAL 尾部增加新的写入位置。②META 是身份文件（§5.5.1），而成员配置是**可从日志重导出的缓存**，塞进身份文件语义不符。③见缺陷 2：快照可能长期不存在。 |
| 文件 payload | `[u64 conf_change_index][u32 voters_len][voters u64…][u32 learners_len][learners u64…]`——成员表**与快照成员表逐字节同构**（复用编解码）；`conf_change_index` = 产生它的那条 ConfChange 条目的 log index | 不记 index | 见下一行：`conf_change_index` 让"回放时是否已应用过"成为一个**可判定**的问题，幂等性由构造保证而非依赖上游。 |
| 重复 apply（重启回放） | 回放时**跳过 `index ≤ conf_state.conf_change_index` 的 ConfChange 条目** | 假设 raft-rs `apply_conf_change` 天然幂等 | 本系统的状态机本来就是"KV 快照 + 回放快照之后的全部条目"重建的，**回放 ConfChange 条目是正常路径**而非边缘情况。不假设上游幂等（`AddNode` 重复调用可能重置 progress）；用 index 判定，可测（S1 含"同一 ConfChange 回放两次，ConfState 与 progress 不变"）。 |
| ConfChange 条目路由 | 按 `entry_type` 分流：`EntryNormal` → KV SM（现有路径不变）；`EntryConfChange/EntryConfChangeV2` → **完全不进 KV SM**，直接 `raw_node.apply_conf_change()` → 落 `ConfState(0x04)` → 应答等待者 → 推进 applied | 把 ConfChange 编码成一条 KV 命令、由 SM 持有 raft 句柄 | SM 必须保持纯函数式可重放、不能持有 raft；且一旦走 SM，畸形命令会 fail-stop 整个集群（缺陷 1）。 |
| joint consensus | 只做**单步** ConfChange；空 `ConfChangeV2`（离开 joint 态）仍按类型路由给 `apply_conf_change` | 支持 joint | §5.3 已锁定单步；从不进入 joint 态，但仍须处理该条目类型以免落入"未知类型"分支。 |
| 单飞门（`ConfChangePending`） | runtime 侧 `conf_change_pending: Option<ConfChangeOp>` 为权威 API 契约：未清空时新成员变更提案返回 `ConfChangePending`（**普通写不受影响**） | 只依赖 raft-rs 的 `pending_conf_index` | §5.3 硬约束 1 是**对外错误码**，必须由我们决定何时算"未决"。raft-rs 的行为已**实测**（S2，`third_party/raft/src/raft.rs:2115`）：`Raft::step` 对 `MsgPropose` 里的 ConfChange 条目会检查 `has_pending_conf()`（`pending_conf_index > applied`），命中则**把条目改写成空的 `EntryNormal` 并只打一条 info 日志**——`RawNode::propose_conf_change` 返回 `Ok`，提案者拿到成功、集群却什么都没变。也就是说上游不是"报错拒绝"而是"静默吞掉"，所以**我们的门是唯一诚实的对外契约**（上游只是最后一道防线，且其文档自认 `has_pending_conf` 可能有假阳性）。清空时机 = 该条目 **applied**（不是 committed）。 |
| Learner 追平门 | 用**条目滞后**判定：`last_index - matched ≤ promote_lag_entries`（默认 128）**且** progress `matched > 0`（"答过至少一次"= 在线），不满足返回 `LearnerNotCaughtUp` | 按 §5.3 原文的 `lag_bytes` 字节口径 | WAL 没有 index→offset 映射，精确字节滞后需要新增索引结构；`matched` 是 raft 自己暴露的量，形状等价。**这是对 §5.3 用词的有意偏离**（已同步改注该节），byte 口径留待运维需要时再加映射。**"在线"不能用 `recent_active`**（S3 实测）：`ProgressTracker::apply_conf` 在**新增节点时故意把 `recent_active` 置 true**（防止 `CheckQuorum` 在节点还没通信前就把 leader 拉下台，`third_party/raft/src/tracker.rs:379`），因此它对"这个 learner 到底答过没有"毫无信息量——基于它的门会让一个**根本不存在的节点**通过。用 `matched > 0` 才是诚实的"答过至少一次"。 |
| `remove_member(leader)` | 先 `transfer_leader`，**转让成功才** propose 移除；转让失败则移除失败并返回原错误，不提供旁路 | 直接提案移除 leader | §5.3 硬约束 3 原文；转让失败（无多数派）时移除本就不可行，这是非目标（不自动接管）的体现。 |
| `transfer_leader` 的就绪判定 | 成功 = **领导权真的转移**（本节点不再 leader / 目标成为 leader）；超时 = `election_timeout` 量级；目标未知 → 错误 | 发出转让消息即算成功 | raft-rs 的 `transfer_leader` 只是发消息；`remove_member(leader)` 若在此处误判成功就会在没有多数派的情况下继续提案。 |
| 快照携带 live ConfState | `create_snapshot` 写入**raft 当前 ConfState**（voters/learners），`install_snapshot` 路径同样把成员表回灌进本地持久状态 | 继续写 bootstrap 静态集合 | 快照格式**已有**成员表（§5.5.4，v0.2.10 起），所以"快照能承载成员"是既定事实，缺的只是填真值。填真值后，压缩可以顺带丢弃水位之下的 `ConfState(0x04)`（由快照接棒）。 |
| 混合版本 / 回滚 | **完全不新增记录类型**（`0x04` 随上表一并撤销） | 扩记录类型集合 | 老二进制根本不读 `membership` 文件，照旧从快照成员表恢复成员 ✓，所以**不需要新的回滚窗口要求**——这正是独立文件的附带好处。（若走 `0x04` 路线则相反：老二进制遇到未知类型 fail-start，混合版本窗口必须禁 ConfChange 且回滚要等一个快照周期。）§5.6 既有的跨 minor 回滚规则不变。 |

**新增不变量**

- **I5**：同一时刻**至多一个未 apply 的 ConfChange**（§5.3 硬约束 1 的运行时形态）。
- **I6**：`membership` 文件的持久化时刻**不早于**其 ConfChange 条目的持久化（apply ⇐ durable）。因此任何时刻"恢复出的成员配置"都是真实已应用配置历史的**前缀**。方向性论证：成员**少**于真实值 → quorum 更难形成，不会造成双提交（只是可用性下降）；成员**多**于真实值 → 需要更多选票，不会丢掉已提交的条目。两个方向都只损失可用性、不破坏安全性。
- **I7**：恢复取 `membership` 文件中 `index > 快照 index` 的成员配置（快照成员表对它覆盖的区间有权威）；该文件不存在或更旧则取快照成员表；两者皆无则退回 `initial_cluster`（首启语义不变）。`conf_change_index` 取同一来源的 index（无文件时 = 快照 index），作为**回放起点**：≤ 它的 ConfChange 条目一律跳过。
- **I8**：`membership` 是**缓存而非权威**——日志与快照成员表才是权威。因此文件缺失/更旧只影响"回放要不要重放"，不影响正确性；但**存在却损坏**是磁盘损伤，fail-start（不静默退回旧配置）。

**落地顺序（S1–S5，每步独立可验证）**

- **S1 ✓（缺陷 1+2 一起修，S1a 存储侧 + S1b 路由侧均已落地）**：seam `Storage` 增 `save_conf_state`/`conf_change_index`，`WalStorage` 落 `membership` 文件（原子替换、CRC、I7 取用规则）+ 磁盘损坏 fail-start；`StepOutcome::committed` 改为携带 **`SeamEntryType`**（此前类型被丢弃），actor 据此**按类型分流**，ConfChange 条目改走 `apply_conf_change` 并按 `conf_change_index` 跳过已应用条目。
  **S1b 实现中钉死的两条细则**（初稿没写到，实现才暴露）：
  1. **KV 状态机要求 apply 索引连续**（`applied + 1`，缺口即 fail-stop）。ConfChange 不进 SM，于是必须在同一通道上、在它**之后**的条目之前，给 SM 递一个 **空 payload 的 no-op**（这正是 SM 已有的"leader term 空条目"路径）：索引推进、状态不变。少了这一步，紧随 ConfChange 之后的第一个命令会以 `index ordering violation` 打死节点。
  2. **顺序屏障只需挡住"成员变更本身"**，不需要挡住它之后的条目：同一 mpsc 通道保证 no-op 先于后续条目到达 apply 任务，所以日志顺序天然保持；屏障只保证"前序条目都已 apply"才调用 `apply_conf_change`（成员视图与 KV 视图的顺序一致）。
  3. 施工面：`StepOutcome`/`PendingPersist` 的元组多一列，9 处测试/harness 的解构点跟着改（`for (idx, _kind, data) in ...`）——机械改动，`cargo test --workspace` 一次性抓全。验证：手工构造一条 ConfChange 条目（`fault-injection`/测试专用入口）→ 提交 → **重启** → 成员保持（反向对照：不写 `0x04` 时退回 bootstrap，断言失败）；同一 ConfChange **回放两次** → ConfState 与 progress 不变、不报错。
- **S2 ✓**：公开 API `Handle::add_learner` / `promote_learner` / `remove_member` / `transfer_leader`（§5.6 已定名；Q3 已决议 `transfer_leader` 公开）+ 单飞门 → `ConfChangePending`。四处实现要点：
  1. **写路径与成员变更共用一条重定向循环**（新 `Request` 枚举：`Propose`/`ConfChange`/`TransferLeader`），因为两者契约相同：leader 受理、follower 回 `NotLeader{hint}`、同一个 deadline 覆盖整个追随过程。
  2. **`remove_member(leader)` 的复合动作在客户端做**（S1 期间才想清楚）：转让成功之后原 leader 已无法再提案，所以只能在 Handle 里按"先收到 `LeaderRemovalRequiresTransfer` 信号 → 转让 → 重发"的顺序完成；转让失败即移除失败、**什么都不提案**（§5.3 硬约束 3）。actor 只负责回那个信号，保持无状态。
  3. **`transfer_leader` 的成功判定 = 领导权真的转移**：actor 把转让放进 `pending_transfer` 等 `leader_id() == target` 才应答；落在第三个节点上是失败，短暂 `None`（已 step down 但还没学到新 leader）不算判定、继续等 deadline。`target: None` 表示"任意其他 voter"（自动选最小 id），供 leader 移除内部使用。
  4. **单飞门在 actor 命令入口**：`!pending_conf.is_empty()` → `ConfChangePending`，普通写走的是另一条命令路径、不受影响。验证：单节点 + 测试专用慢盘（`set_flush_delay_ms(120)`）把"已提案未 apply"的窗口撑开 → 并发两条 `add_learner` 恰好一条 Ok、一条 `ConfChangePending`，同批次的 put 正常成功，闸门释放后第三条又被接受（端到端 `membership_change.rs`）。
  5. 3 节点端到端：显式 `transfer_leader(指定 voter)` → 断言目标真的成为 leader 且旧 leader 认得它 → `remove_member(当前 leader)` → 断言两票仍可写、被移除节点不再收到 commit、停机后重开该存活节点的 WAL 断言 voters=2 且不含被移除者。
- **S3 ✓**：Learner 追平门 + `promote_lag_entries` 旋钮（默认 128，production 真读 → 过 `check-profile-knobs.sh`）；`RaftNode::learner_progress` 给出 `(条目滞后, 是否答过)`，`is_learner` 判定其是否在已应用配置里；纯函数 `learner_caught_up(behind, has_acked, threshold)` 承载判定以便边界可单测。门在 actor 命令入口、**提议之前**（提议后检查会被"条目已提交"竞态绕过）：
  - 是 learner 但没追平 → `LearnerNotCaughtUp { behind, threshold }`（新错误变体，带诊断字段）；
  - 不是 learner（已有 voter 或未知）→ `InvalidArgument`，从而"新节点一律先以 Learner 加入"不可被绕过；
  - **实测修正**："在线"用 `matched > 0` 而非 `recent_active`——raft-rs 新增节点时刻意把 `recent_active` 置 true（CheckQuorum 安全），基于它会让不存在的节点通过门（见上表）。
  验证：单测覆盖边界（`0/128/129` 条目 + 未答过 + 阈值 0）；端到端（`membership_change.rs`）对不存在的 learner `promote` → `LearnerNotCaughtUp` 且 `behind > 0`、对未知节点 `promote` → `InvalidArgument`、之后写仍正常、停机后 durable 成员仍是 voters=[1]/learners=[2]（**反向对照**：什么都没被提升）。**正向路径**（真实第 4 个节点追平后 promote 成功）随 S5 的 learner 加入一起做——那需要"新节点以 learner 身份启动"的配置面。
- **S4 ✓**：快照写 **live** ConfState + 安装/保存路径把新于 `membership` 文件的成员表**回灌**到内存视图。三处改动：
  1. **`create_snapshot` 的调用方**（runtime）此前传的是"启动时的静态 voter 列表 + `learners: Vec::new()`"——即快照会**丢掉所有 learner 与所有成员变更**；改为 `RaftNode::applied_conf_state()`（读已应用成员配置）。静态列表连同它的推导一起删除（否则就是第二份会分叉的真相）。
  2. `WalStorage::save_snapshot`/`install_snapshot`：当 `snapshot.index > conf_change_index` 时用快照成员表接管内存视图。**这是运行时正确性问题而不只是重启问题**：`is_learner`/`voter_ids` 都读它，安装快照后若内存里还是旧配置，追赶中的 learner 会被追平门误判。
  3. 快照格式**不变**（成员表 v0.2.10 起就有），所以"压缩丢弃水位之下的成员记录、由快照接棒"无需新格式，回滚也不受影响。
  验证：wal 单测（安装 index 9 的快照 → `initial_state` 报快照成员、`conf_change_index=9`；**反向对照**：更旧的快照不得把成员拖回去）；端到端 `a_local_snapshot_carries_the_live_membership`（小阈值触发本地快照 → 停机后重开 WAL 断言 `snapshot().meta.conf_state.learners == [2]`，**修复前该断言必失败**）。
- **S5（进行中：加入/追平/promote/remove 端到端 ✓ + INV-4 断电场景 ✓，node 运维面待做）**：端到端 M3 ①②：3 voter + 新节点以 **Learner** 加入（新节点启动声明 = `initial_cluster` voter 集 + `learners=[self]`，**仅作无持久状态时的兜底**，与既有 `voters.is_empty() && learners.is_empty()` 判定一致）→ 施压写入 → promote → remove 一个成员，全程**quorum 存续、写不中断**；`remove_member(leader)` 自动先转让成功（M3 ②）；`fault-injection` 崩在 promote 前后（INV-4 "Learner 追平瞬间断电"）→ 重启后成员状态与日志前缀自洽 **✓ 已落地**：第 4 节点追平后**断电**（abort 其 runtime）→ 在它缺席时 promote 仍成功（门用的是它最近一次应答的进度）→ 三票仍可写 → 用同一 data_dir 重启 → 重新追平，且**自己的持久成员配置收敛为 4 voter（不再是 learner）**——即成员是"日志+快照"的函数，而不是崩溃节点记住了什么（I6/I8）。运维面（`arachne-node` 子命令与 metrics）在 S5 末接线 **✓ 已落地**：节点操作端点 `GET /members` + `POST /members/{add-learner,promote,remove,transfer-leader}/<raft-id>`（`200` 表示**已 apply/已持久**，所以命令返回后立刻读 `/members` 就能看到新配置），以及同名子命令 `arachne-node add-learner|promote|remove|transfer-leader|members --config <toml> [--id] [--http]`。子命令是**独立进程**，因此走 HTTP 而不是 in-process `Handle`；**不跟随 hint**（hint 给的是 raft 地址、不是操作端点地址），遇到 `409` 时打印 leader 提示并退出非零，由运维把命令指向该节点；唯一的例外是移除当前 leader：节点回 `409 requires a transfer`，CLI 自己先 `transfer-leader/0`（0 = 任意其他 voter，节点挑）再重试——与 in-process `Handle::remove_member` 的复合动作同构，只是跨进程。HTTP 调度器随之放行 `POST`（原先只放行 GET/PUT/DELETE）；错误映射新增 `409`（`ConfChangePending`、`LeaderRemovalRequiresTransfer`）与 `412`（`LearnerNotCaughtUp`，格式良好但前置条件不满足）。

**实现修正（E-rev，S1 期间）**：初稿把持久化 ConfState 定为"WAL 新增记录类型 `ConfState(0x04)`"，理由是复用段内的 CRC/撕裂/恢复规则。真正动手时才发现这条路线与 §5.5.3 的**提交窗口撕裂规则**冲突：末段撕裂只在 `estimated > commit + 1` 时自动截断，而撕裂记录的类型不可信，于是**稳态（`commit == last_index`）下一次写入中途崩溃 = fail-start**。任何"在已提交条目之后追加的非 Entry 记录"都继承这个性质，因此记录类型方案被否决，改为独立的原子替换文件 `membership`（同时避免了扩记录类型集合，回滚更简单）。教训与 P2 同源：**尾部撕裂只能靠"不产生新的尾部写入位置"来回避，不能靠类型嗅探**。

**S5 实现补充**：`RaftNodeConfig` 增 `bootstrap_voters: Option<Vec<RaftId>>` 与 `join_as_learner: bool`，把"**传输可达的节点**"与"**启动时的投票配置**"分开。二者此前混在一起（bootstrap voter 集由 `peers` 推导）：而一个加入者必须在**成为 voter 之前**就能被现有节点发消息（否则永远追不上），于是现有节点的 `peers` 必须包含它——若不拆开，现有节点就会把加入者当成 voter 来 bootstrap，得到一个全集群从未同意过的配置（`add_learner` 之后它还留在 voter 集里，`promote` 又会以"不是 learner"失败）。拆分后：加入者 `bootstrap_voters = 既有 voter 集` + `join_as_learner = true`（本地声明，仅在没有持久配置时生效），既有节点 `bootstrap_voters = 既有 voter 集`（`peers` 里带加入者只为可达性）。`RaftNodeConfig` 因此不再 `Copy`（含 `Vec`）。

**本 rev 未决**：byte 口径的 `promote_lag_threshold`（等 index→offset 映射）；joint consensus（非目标）；自动 demote（§5.2 明示由运维决定）。

### T. v0.2.17：快照流式传输（E-rev，设计；最后一个已知缺口）

**先记一个实测出来的硬缺口**：传输层默认把 gRPC 消息上限设为 **8 MiB**（`DEFAULT_MAX_MESSAGE_SIZE`，同时作用于 client/server 的 encode/decode，`arachne-transport-tonic/src/factory.rs:70`），而快照触发阈值是 **Lan 64 MiB / Wan 16 MiB**（§7），且 raft 的 `max_size_per_msg` **只限制 AppendEntries 的条目批量、不限制快照消息**（`raft.rs:870` 只用于 `entries()`）。于是：**任何大于 8 MiB 的快照在真实传输上根本发不出去**，即"落后超过 `wal_trailing_keep` 的 follower 永远追不上"——这是**可用性缺口**（不是安全性），而且**被现有测试完全掩盖**：所有快照追赶测试都跑在 in-memory 传输上，它直接传 `Vec<u8>`、不经过 gRPC ✓。§5.5.4 早已把 v1 记为"单次 `Ready`（未分片），流式分片随传输层快照 RPC 落地"，本 rev 就是那个 RPC。

| 决策点 | 选择 | 否决的备选 | 理由 |
|---|---|---|---|
| 谁发起传输 | **follower 主动拉**：收到"只有元数据"的快照消息后，向该 leader 发 `FetchSnapshot` 请求 | leader 推送分片 | raft 已经把"要快照"这件事表达清楚了：`RawNode::request_snapshot()` → `pending_request_snapshot` → leader 的 `prepare_send_snapshot`，还有现成的 `report_snapshot(id, status)` 回执通道（`MsgSnapshotStatus`）✓。让 follower 拉，leader 不需要为每个 follower 维护推送状态机，重试也天然由 follower 发起。 |
| 快照消息带什么 | **只有元数据**（index/term/ConfState，data 为空） | 仍然带全量 data（现状） | 单消息路线在 8 MiB 之上直接失败（见上）。元数据消息仍然走普通 raft 消息通道 ✓，所以"哪个 follower 需要哪份快照"依旧由 raft 决定。**实现要点**：`RaftStorage::snapshot()` 要能返回"无 data 的元数据快照"，否则 raft 依旧会塞满整个 data ✗。 |
| RPC 形态 | **server-streaming** `FetchSnapshot(SnapshotRequest{index,term}) returns (stream SnapshotChunk{offset,data})` | 一元 RPC + 客户端循环 | D2 选 tonic 的理由之一就是 server-streaming + HTTP/2 流控（§11/§14）；分块大小由服务端定、客户端不必知道总量，流控与取消由 HTTP/2 承担。 |
| 限速 | 服务端**令牌桶**，速率 = `snapshot_transfer_rate_bps`（0 = 不限），按块发放；桶在流开始时建立 | 客户端自行 sleep | 限速的语义是"别让一次快照打满链路、影响正常复制"，只有发送方知道何时发下一块；客户端限速挡不住服务端的突发。 |
| follower 侧安装 | **分块写 tmp 文件 → CRC 校验 → fsync → rename → 复用既有 `install_snapshot`**（META 指针 + 段替换 + 水位）+ 状态机 `restore` + 回 `report_snapshot(Success)` | 全量驻留内存再安装 | `install_snapshot` 已经实现了 v0.2.10 钉死的原子替换语义，聚合到文件后再走它，内存占用与快照大小无关（64 MiB 快照不该要 64 MiB 堆）。CRC 在快照文件格式里本来就有（§5.5.4 全负载 CRC）✓。 |
| 失败与重试 | 流中断/CRC 失败 → 丢弃 tmp、`report_snapshot(Failure)`（raft 会把该 follower 转回 probe 并重试）→ 下一轮重新拉 | 断点续传 | 断点续传要维护 offset 与校验点，v1 不值得；重来一次的代价是带宽，而它本来就被限速了。**但** §9 的"快照传输中断后恢复"场景仍是验收项，用"中断 → 重传 → 追上"验证 ✓。 |
| in-memory / turmoil 传输 | `Transport` trait 增 `fetch_snapshot`（**默认实现返回"不支持"**），进程内传输直接返回本地快照 | 强制所有传输实现流式 | 进程内传输本来就能整份交递 ✓；默认实现保证 `arachne-sim`/L2/turmoil 与所有既有测试零改动（与 `save_conf_state`/`Runtime::with_session_clock` 同样的"不改接口就不动调用方"取舍）。 |
| 可观测性 | 复用 `snapshot_*` 指标，新增 `snapshot_stream_chunks_total` / `snapshot_stream_bytes_total` / `snapshot_stream_failures_total` | 无 | 限速与分块是"看不见的"，没有计数器就无法判断"是慢还是卡住"。 |

**新增不变量**：**I9** — 安装快照前必须完成 CRC 校验（沿用快照文件的 CRC，不新增信任面）；**I10** — 流式路径与单消息路径**安装结果必须逐字节一致**（同一 `install_snapshot` 入口），因此两条路径可以用同一个断言检查。

**落地顺序**：
- **T1 ✓（传输层，本轮落地）**：proto 增 `FetchSnapshot(SnapshotRequest) returns (stream SnapshotChunk)`；`SnapshotProvider` seam（`open(index, term) -> Option<SnapshotReader{len, reader}>`——**len 与 reader 一次取出**，避免"问大小"与"取字节"之间被压缩掉而产生的短读；reader 让服务端一次只持有一个分块）；服务端实现为**先验握手再取数据**（快照是集群数据，未认证调用者一个字节也拿不到，拒绝计入 `handshake_rejections`），然后用一个后台任务从 provider 读块、经**令牌桶**（`RateLimiter`，0 = 不限）节流后推入容量 2 的通道（这就是背压：客户端不读则任务原地等待，不会把快照堆进内存）；`TonicTransportFactory` 增 `snapshot_provider(...)` 与 `snapshot_rate_bps(...)` 两个 builder。
  **验证**（`arachne-transport-tonic/tests/snapshot_stream.rs`，3 项，连跑两次）：①**逐字节相同**（2×256 KiB + 1234 字节跨块拼接，快照自身 CRC 是最终裁判 ⇒ I10）；②**未认证调用者拿不到字节**（缺握手 / 异 cluster / 协议 major 不符 → `PermissionDenied`，且三次拒绝都被计数）；③**限速可观测**：768 KiB @ 256 KiB/s ⇒ 500ms 内**不可能**完成、但最终完成，rate=0 的同一份数据 500ms 内有富余 ⇒ 反向对照成立。另加 `RateLimiter` 单测（rate 0 立即返回；限速后三块耗时落在可观测区间）。
  **`check-profile-knobs.sh` 的一个假阳性顺带修掉**：新代码的**文档注释**里提到 `snapshot_transfer_rate_bps` 就被判成"已被读取" ✗（门用纯文本 grep）——门现在忽略**整行注释**（`//`/`*`/`/*` 开头），真实读取永远不会在注释行上。白名单条目保留（接线在 T2），理由文案更新为"T1 已落地、缺 T2 接线"。
- **T1b ✓（follower 侧客户端）**：seam `Transport` 增 `supports_snapshot_streaming()`（默认 false）与 `fetch_snapshot(from, index, term, dest) -> Result<Option<u64>, _>`（默认 `Ok(None)` = "本传输不支持流式"，**绝不表示空快照**——用 `Option` 而不是构造 `Self::Error` 的默认实现，是因为默认实现无法凭空造出关联错误类型）。`TonicTransport` 两者都实现：写入 `dest` 文件而不是内存缓冲（快照可达几十 MB，且校验靠文件自带 CRC ⇒ I9）。
  **一个实现中发现的坑**：`send` 用的缓存 channel 带**按请求超时**（默认 5s），而限速下的几十 MB 快照合法地远超它 ⇒ 会掐断健康传输。因此快照改用**自己的连接**（保留 connect timeout 与 keep-alive，不设按请求超时），也不污染发送路径的 channel 缓存——快照拉取本就稀少且长命。
  验证：传输层 `the_transport_streams_a_snapshot_into_a_file`（经 `Transport` trait 拉取 2×256 KiB+77 字节 → 文件逐字节相同、unknow snapshot 报错含 "no such snapshot"）；反向对照 `the_in_memory_transport_does_not_stream_snapshots`（进程内传输必须答"不支持"且**不创建文件**——若它谎报 true，follower 会只拿到元数据而没有数据）。
- **T2（运行时）**：`RaftStorage::snapshot()` 在"对端支持流式"时只返回元数据；runtime 收到无 data 的快照消息 → `RaftNode::request_snapshot` 已有的是**发起方**能力，follower 侧要新增"收到元数据快照 → 拉取 → 聚合 → `install_snapshot` → `report_snapshot`"的编排；快照请求与 apply 的排队沿用 §5.5.4 的"同序入队"（快照请求不得抢在已提交条目之前 apply）。验证：**真实 tonic 三节点**、把 gRPC 上限调到 64 KiB、让状态机数据超过它 → 单消息路线必失败、流式路线追上（这正是把 8 MiB 缺口钉成回归测试）。
- **T3（中断恢复）**：流中途断开 → 丢弃 tmp、report failure、重试后追上；并把 §9 的"快照传输中断后恢复"场景补进 L2/L3。

**本 rev 状态**：**T1 ✓ / T1b ✓ / T2a ✓ / T2b ✓**（传输层 RPC + 令牌桶 + follower 侧拉取；**leader 侧元数据快照与 node 侧接口**）；**T2b 待做**——把 follower 编排接上并**打开开关**：
- `Runtime` 持一份 transport 克隆（因此需要 `T: Clone`；`TonicTransport` 已是 Clone，`InMemoryTx` 需补 derive），按 `transport.supports_snapshot_streaming()` 设 `RaftNodeConfig.streamed_snapshots`（**当前没有人打开它**，所以 T2a 是休眠能力、线上行为与之前逐字节相同 ✓ 分期落地的既有先例见 rev P 的 P1/P2/P3）。
- actor 收到"元数据快照 + `snapshot_from`"→ **spawn 拉取任务**（不能 await：几十 MB 的传输会卡住 tick/读/心跳），结果经一条新通道回到 actor；期间该 cycle 携带的 committed 条目与后续 cycle 的条目一律**暂存**（快照必须先进 SM 才能 apply 其后的条目）。
- 拉取完成 → 读文件 `decode_snapshot`（CRC 即 I9）→ `install_local_snapshot` → 复用既有 `enqueue_apply(Some(完整快照), 暂存条目)`（安装记账、覆盖语义、SM restore 全在既有路径里）→ `report_snapshot(from, true)`；失败则 `report_snapshot(from, false)`（raft 把该 follower 转回 probe 并重试）并丢弃 tmp。
- 测试：进程内三节点 + 给 `InMemoryTransportFactory` 注册"快照字节来源"（按 `(from, index, term)` 读 leader 数据目录里的快照文件）⇒ 打开开关后走**与 tonic 相同的运行时路径**；T1/T1b 的传输层测试已覆盖真实 gRPC 流。**真实 tonic 三节点回归**（把 gRPC 上限压到 64 KiB 让单消息路线必失败）仍列为 T2b 的验收项。
- **T3 ✓（已解决，第 16 轮）**：根因是 raft 的**本地消息**分类——`RawNode::step` 对 `is_local_msg` （含 `MsgSnapStatus`）直接 `Err(StepLocalMsg)`，因此 follower 无法把传输结果告诉 leader，而 `snapshot_failure()` 只有 leader 本地一个调用点。修法：**follower 侧本地重试**（8 次、退避），成功安装后的普通 `MsgAppendResponse` 经 `handle_append_response` 的 `ProgressState::Snapshot` 分支把 leader 的 progress 转回 Probe ✓。测试 `a_failed_snapshot_transfer_is_retried_and_recovers` 通过 ✓。
- **真实 tonic 三节点回归（未完成，第 17 轮尝试）**：骨架（每节点一 factory + `start_with_bind` + 各自 provider + 64 KiB 上限 + `wal_trailing_keep_bytes = 0` + kill/重启）已跑通到「重启成功」，但 victim 停在 `applied=2` 而 **provider 调用次数 = 0** ⇒ leader 从未把快照送达；已排除「传输能力未开」（断言通过 ✓）。下一步怀疑 leader 侧到 victim 的**缓存 gRPC channel** 在对方重启后失连。做法与诊断数据见 handoff，重建很快。
- **（历史记录，第 13–14 轮）**
  - **已修的真 bug（第 14 轮才真正落地并验证）**：T2b 用 `RawNode::report_snapshot(to, status)` 向 leader 汇报传输结果——**这个 API 是 leader 侧的**：它把状态 step 进**本节点**的 raft（供 leader 替 follower 代报），而不是发给 leader。follower 调用它等于什么都没做 ⇒ leader 永远停在 `Snapshot` 状态、**失败的传输永不重试** ✗（成功路径因为 raft 的 AppendResponse 也能清状态而侥幸通过，所以只有失败路径暴露它）。已改为构造 `MsgSnapStatus{from=self, to=leader, term=self.term, reject}` 并经 transport **直接发给 leader**（term 必须带上：term 低于对端的消息会被丢弃）。
  - **仍未打通（第 15 轮已定位到 raft 内部）**：新增 `RaftNode::peer_progress` 后取得硬证据——状态已送达、`msg_term == self_term`、leader 处于 Leader、目标 progress 正是 `Snapshot` 且 `recent_active=true`，**但 `handle_snapshot_status` 没有执行**（否则 `pending_snapshot` 会归 0、state 会变 Probe；实测仍是 `Snapshot/149`）。该函数内无 index 校验、两个早退也都不成立 ⇒ **消息在 `Raft::step` 更早的分派阶段被丢掉**，与线缆/时序/进度状态无关。下一步：读 `step_leader` 的完整路由（或临时在 vendored raft 加探针、用完还原），并试"失败后同时发一条 reject 的 MsgAppendResponse"的旁路。
  - **第 14 轮已排除**"状态没送达"、"leader 收不到该 follower 流量"、"集群太安静没有 append 触发"三种假设，均以探针证据排除；下一步读 leader 侧 `Progress` 的真实状态）（source 首次返回 `None`）后，follower 仍追不上；探针显示 status 已能发出（改动后未再复现"报错路径未执行"的中间态），但**没有观察到第二份快照被发送**（`[probe] held` 只出现一次）。已把该诊断过程与探针结论记在 handoff，T3 的测试暂从套件撤下（不留红/不留 ignored），恢复路径作为**已知开放项**继续跟进。影响面：**可用性**（那个 follower 追不上，集群在多数派下继续服务 ✓）而非安全性（没有任何东西进入 raft/存储 ✓ 元数据从未被 restore ✓）。
  - 复现脚本：`catch_up_scenario_inner(offloaded, streaming=true, fail_first=1)`（参数化能力已保留在 m2_snapshot.rs 里 ✓ 只是当前没有测试调用 `fail_first>0`）。
  - **T2b 的形状由一处实测安全分析定下来（本轮最重要的设计产出）**：**元数据快照不能先交给 raft**。`Raft::restore`（`third_party/raft/src/raft.rs:2617`）只要 `snapshot.index >= committed` 就会把 raft 的日志截断、`committed`/`applied` 推到快照 index 并改配置，而**没有任何代码会自动回 `MsgSnapshotStatus`**（全仓 grep 无 `send_snapshot_status`）——回执必须由 app 调 `report_snapshot`。于是"先 step 进 raft、再拉取"的写法有个致命窗口：**拉取失败时 raft 的视图已跳到快照 index，而本地存储还停在旧日志** ⇒ 两者不一致（此后到达的条目会以 `index ordering violation` 打死节点）。
    因此 T2b 的正确形状是**拦截**：runtime 在把入站消息交给 raft **之前**识别出"流式元数据快照"并**不 step**，先 spawn 拉取；成功 → 解码（CRC = I9）→ `install_local_snapshot` → **再**把该消息 step 进 raft（此时 raft 拿到真数据，走的是与既有 in-`Ready` 路径**完全相同**的 restore + SM restore 路径 ✓ 一套代码两条路）→ `report_snapshot(from, true)`；失败 → 什么都没进 raft，`report_snapshot(from, false)` 让 raft 转 probe 并重试 ✓（raft 视图与存储始终一致）。识别不需要额外解析：`on_message` 本来就要 parse，在其中判断类型并记下 `(from, raw_bytes)` 即可（node 侧留"待拉取的入站快照"槽位 + `step_held_snapshot()`）。
    该形状还消掉了原计划里"暂存 committed 条目"的复杂度：快照消息根本没进 raft，同 cycle 就不会有"快照之后的条目"要等它 ✓。
  - **T2b ✓ 已落地（本轮）**：拦截 + spawn 拉取 + 解码（CRC）+ **先 step 进 raft、再由普通 `Ready` 路径安装** + `report_snapshot`。端到端 `lagging_follower_catches_up_through_a_streamed_snapshot`（3 节点、in-memory 传输开启流式、按 peer 注册快照字节来源并**断言来源真被调用**）：leader 只发元数据 → follower 拦截拉取 → 追上（4.2s，未接通时该测试会一直追不上）。
    实现中纠正了两处**顺序**错误（都由测试直接暴露，值得记下）：
    1. **不能先安装到存储再交给 raft**：`Raft::restore` 会拿快照去**存储**核验（`match_term`），而先把存储日志轮转到 `index+1` 会让 restore 静默失败，于是 raft 的日志与存储不一致 ⇒ 下一条 AppendEntries 直接在 `unstable.slice` 越界 panic（`log_unstable.rs:201`）。正确顺序 = **解码校验 → step（raft restore）→ 由 in-`Ready` 路径安装**（一条代码路径覆盖两种来源 ✓）。原先为"先装后交"设计的 `install_local_snapshot` 因此**删除**（是我发明的方法，实现证明不需要）。
    2. `submit_ready` 里"跳过安装"的判据必须看**有没有字节**，而不是"是否开了流式"：拦截流程 step 进去的消息**带真实数据**，其 `Ready` 快照必须照常安装 ✓（只有"空 data 的元数据快照"才无事可做）。
    另有一个测试侧教训：磁盘上的快照文件名是**零填充**的（`snapshot-<020>-<020>.snap`），而请求里带的是裸数字 ⇒ 任何 provider 都必须做这个映射（真实实现用 `snapshot_file_name`）。
  - **生产接线（T2b 收尾）**：`arachne-node` 在工厂上装 `DataDirSnapshots`（按 `snapshot_file_name` 从数据目录取快照，len 与 reader 一次取出）+ `snapshot_rate_bps(profile.snapshot_transfer_rate_bps)` ⇒ **`check-profile-knobs.sh` 白名单清空**（"known gaps: none"，最后一个旋钮真正被读）。同时把能力判定收紧为**安全默认**：`TonicTransport::supports_snapshot_streaming()` = 该节点**注册了 provider**（能拉取但不会服务是半残状态：leader 会发出对端永远补不齐的元数据快照），所以没接 provider 的嵌入方/测试仍走旧路径 ✓ 而真实部署装上 provider 后自动启用流式 ✓。
  - **T2b 脚手架（`arachne-testsupport`，独立验证）**：`with_snapshot_streaming()` + `set_snapshot_source(peer, closure)`（按 `(index, term)` 供字节；`None` = 该 peer 没有这份快照 = 模拟传输失败）、`InMemoryTx` 补 `Clone`（拉取任务需要）+ 实现 `supports_snapshot_streaming`/`fetch_snapshot`（写 `dest`）。测试：注册源则逐字节相同；未注册源/不存在的快照都 `Ok(None)` 且不留下文件。

T2a 已落地的部分：`RaftStorage::streamed_snapshots(bool)` + 元数据快照（`snapshot()` 清空 data，保留 index/term/ConfState——raft 仍决定"要哪份快照"）；`RaftNodeConfig.streamed_snapshots`；`RaftNode` 记录 `MsgSnapshot` 的来源（raft 从 `Ready` 交出的快照不带 sender）并经 `StepOutcome.snapshot_from` 透出；`report_snapshot` / `install_local_snapshot` / `fetch_snapshot`（raft id → transport NodeId 的映射在 node 内）；`submit_ready` 在流式模式下**不安装**元数据快照（否则会用空状态机替换日志）。测试：`a_streamed_snapshot_hands_raft_metadata_only`（开/关两态都断言：关 = 消息里带全量字节，开 = data 为空但 index/term/成员表完好）。

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
  2. 新节点一律先以 **Learner** 加入（不参与 quorum），追平（**rev S 修订：`last_index - matched ≤ promote_lag_entries`**，即条目滞后而非原 `lag_bytes` 字节口径，理由见 rev S；且 progress 在线）后由运维显式 `promote_learner` 转 Voter。
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
- **成员配置**独立落盘为数据目录下的 `membership` 文件（rev S）：原子替换（tmp → fsync → rename → 目录 fsync），不占用记录类型——末段撕裂的类型不可信，任何追加在已提交条目之后的非 Entry 记录都会把例行崩溃变成 fail-start（详见 rev S 实现修正）。
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

*v0.2.17 规划中（v0.1 → v0.2 设计精化；v0.2.1 锁定参数级决议；v0.2.2 修订测试基建选型并产出 `test-plan-v0.1.md`；v0.2.3 锁定测试方案四项决策 D-T1/D-T2/D-L4/D-S1；v0.2.4 锁定工件与工作区决策 D-ART——`arachne-node` 运维 bin 为生产交付物；v0.2.5 锁定 D-ART 四项子决策：crate 命名、默认 feature 拉 tonic、TOML 配置、`test-observability` 门禁；v0.2.6 D-ART-rev1：抽出 `arachne-seam` 叶 crate，消除 `transport-tonic → arachne` 包循环；**v0.2.7 §5.5.3 恢复算法安全收紧（E-rev）：坏记录不再嗅探类型字节、仅末段结构性撕裂可截断、`commit ≤ last_index` 断言 + 新段目录 fsync**；**v0.2.8 §5.1/§7 约束注解与预设自相矛盾修正（E-rev）：`rpc_timeout < election_timeout`、`election_timeout ≥ 5× heartbeat`**；**v0.2.9 INV2 的测试专用崩溃注入点（E-rev）：feature `fault-injection` 默认关、发布构建排除，仅在 `RaftNode::step` 的 persist/deliver 边界提供一次性崩溃 hook**；**v0.2.10 快照落盘契约、META 快照指针与压缩边界（E-rev）：独立 `snapshot-<index>-<term>.snap` 文件 + 全负载 CRC、目录扫描取最新合法快照（保留最新两份）、META payload 尾部追加快照指针（版本不变）、段粒度压缩（字节级 trailing 窗口留待接线）、逻辑增长触发快照、follower 安装快照的整体替换语义**；**v0.2.11 apply 独立任务、反压口径与 actor 专用线程（E-rev）：actor↔apply 两通道 + `watch` 进度、Q7 的 `proposal_queue_bytes` 真正接线为「已提交未 apply」的字节背压（触顶回 `Busy`）、快照请求同序入队、进度确认后才 `advance_apply`、延迟预算用空载/洪峰比值口径**；**v0.2.12 不批 fsync 而批持久化周期（E-rev）：明确 `BatchMs` 为何不可实现（单写者无并发等待者 + 周期内两次 flush 结构性）、改为命令/入站成批处理后每次 `drive_cycle`（`CYCLE_BURST`）、seam 增 `TransportRx::try_recv`，实测洪峰写 p99 106–157ms→38–53ms、弱读 85–99ms→0.17ms**；**v0.2.13 异步持久化流水线（E-rev，分阶段进行）：用 raft-rs 自带 `Ready::number`/`advance_append_async`/`on_persist_ready` 骨架，写留在 actor、刷移到 worker（dup fd，`fsync` 刷 inode），消息/advance/commit 全部推迟到 flush 完成之后；P1 已落地（`flush_handle`）**；**v0.2.14 `wal_trailing_keep` 接线（E-rev）：窗口压住压缩水位（而非保留水位之下不可达的字节），默认 0 保持旧行为，node binary 按 profile 设置，端到端含反向对照**；**v0.2.14 收尾：Q4 快照预算告警落地（`> 1s` warn + `snapshot_slow_total`），CI 增加单飞项目构建门禁（fuzz/model-check，`--locked`）**；**v0.2.14 收尾之二：rev P 的「可用性而非延迟」论断已证实——feature `fault-injection` 下新增测试专用 flush 延迟（`set_flush_delay_ms`，同时作用于同步与流水线两条 flush 路径），A/B 实测慢盘（400ms）下写落盘期间弱读最坏值：同步 360ms vs pipeline 127µs；CI 同 feature 步骤运行该测试，Gate C 增加第二哨兵**；**v0.2.14 收尾之三：新增门禁 `scripts/check-profile-knobs.sh`——`ProfileConfig` 每个旋钮必须被生产代码读取，已知缺口（M3 会话三项 + 快照限速）带理由白名单，白名单项一旦被使用即失败**；**v0.2.15 会话幂等收尾设计（E-rev）：记录会话表无界增长缺陷，钉死 leader 侧 TTL/grace 判定 + 复制的显式 GC 条目 + `max_sessions` 提议前检查 + TTL 走 `Clock` seam 以便模拟时钟验证，分 R1/R2/R3 落地**；**v0.2.16 ConfChange 落地设计（E-rev）：记录两个**既有缺陷**——ConfChange 条目今天会被当作 KV 命令喂给状态机（解码失败 → fail-stop，一旦加 API 第一个成员变更就打死集群）、段内没有持久化 ConfState（成员变更重启即丢，退回 `initial_cluster`）；钉死成员配置落盘为独立 `membership` 文件（原子替换 + CRC；**初稿的 WAL `ConfState(0x04)` 记录方案在实现时被否决**——末段撕裂的类型不可信，追加在已提交条目之后的非 Entry 记录会把"成员变更写一半就崩"变成 fail-start）、`conf_change_index` 作回放起点以保证幂等、apply 通道**按条目类型分流**、单飞门 `ConfChangePending` 为对外契约、Learner 追平门改用**条目滞后**（对 §5.3 用词的有意修订）、`remove_member(leader)` 先转让成功再提案、快照写 live 成员表以便压缩接棒（格式不变，故回滚只需一个快照周期），分 S1–S5 落地**；**v0.2.17 快照流式传输设计（E-rev）：实测 gRPC 8 MiB 消息上限 vs Lan 64 MiB 快照阈值 + raft 不限制快照消息 ⇒ 大于 8 MiB 的快照在真实传输上发不出去（被 in-memory 传输掩盖），选定 follower 主动拉 + server-streaming + 令牌桶限速（`snapshot_transfer_rate_bps` 接线）+ 分块落盘后复用 `install_snapshot`，分 T1–T3 落地**；**v0.2.16 收尾之一：S1 落地（存储侧 `membership` 文件 + 路由侧 `entry_type` 透传/分流）**——实现中另钉死两条：KV 状态机的 apply 索引必须连续，故 ConfChange 索引要以**空 payload no-op**递给它（复用既有的 term 空条目路径），否则其后第一个命令会以 `index ordering violation` fail-stop；顺序屏障只需保证"前序条目已 apply"才改成员，后续条目的顺序由同一通道天然保证。9 处 harness 解构点随之机械更新**）。下一步：按 test-plan §12 的 M0 交付测试基建骨架、六工件工作区（D-ART-rev1 增 `arachne-seam` 叶 crate）与接缝 spike 清单（§13），再进入 raft-rs 集成原型（属实现工作，另立任务）。*
