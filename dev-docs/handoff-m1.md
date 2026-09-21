# Arachne 交接：M0 完成，M1 进行中

> 面向**新会话**。先读本文，再读 `dev-docs/propsol-v0.2.8`（设计权威）与 `dev-docs/test-plan-v0.1.3`（测试方案）。会话态进度见 `.slim/deepwork/arachne-m1.md`（git-local，但 OpenCode 可读）。
>
> 仓库：`/Users/alex/Projects/workspace/Arachne`，git 已 init，工作树干净，**294 tests 0 failed**、0 告警、`scripts/check-deps.sh` 与 `scripts/check-entropy.sh` 全绿。

## 1. 现状总览

- **M0 已完成**（含终检门禁，COMPLETE）。六工件工作区 + 崩溃安全 WAL + raft 集成 + KV/会话状态机 + 全套测试基建 + `arachne-node` 单节点可运行 + examples + L4 runner 脚手架 + stateright/turmoil 骨架 + spike 关闭。
- **M1 进行中**。已完成 M1-1、M1-2（含整改）、M1-3a、**(A) arachne-node 多进程 tonic 接线（commit 5081417）**、**(B) M1-3b ReadIndex 线性一致读（commit 6616827）**、**(C) stage 1 ClientOracle + 线性化检查器（a915f6b；自证 395fc9a；门禁 GO）**、**(C) stage 2 D-S1 raft 可播种选举 RNG + 双跑金丝雀（commit f132e44；门禁 GO）** 与 **(C) stage 3a/3c**（3a transport I/O 接缝 `edd913d`；3c inc.1–10 见 §3(C)，L2 场景 18 项全绿）；**`(D)/M1-5` 已收尾（commit `adb2bd7`，含 `6c79886` 的持久化 commit 修复，见 §1.5）**；**(C) stage 3b（real tonic over turmoil）已解决**（harness 显式链路延迟，见 §1.6）。
- **✅ 已恢复**：oracle provider 故障（模型 id 无法解析 + 空结果）已解决；(C) stage 1 确认门禁已补跑并 **GO**，stage 2 门禁亦 **GO**。

### 1.5 本轮收尾（**已提交**：`6c79886` 修复 + `adb2bd7` 收尾）

面向 `(D)/M1-5` 与 L4 义务的一轮收尾：

1. **跨进程 `409 + leader hint`（M1 验收③）**
   - `Handle::without_redirect()`（`arachne/src/client/handle.rs`）：`max_redirects` 改为 **handle 级**字段（移出共享 `Arc<HandleInner>`），单发克隆把 `NotLeader{hint}` 原样返回。
   - `arachne-node` HTTP 改用 `node.handle().without_redirect()`；`NodeHttp::map_error` 新增 `Timeout`/`Busy` → **503**（原先 `Timeout` 落 500，与验收②"不挂死 + 服务不可用类状态"不一致）。
   - 测试：`arachne/tests/client_redirect.rs`（**新**：redirect 路径 + 单发 hint 断言）、`arachne-node/tests/multi_node.rs`（非 leader 必须回 `409` 且 hint 指向**真实 leader**）。
   - **顺带发现（重要）**：`arachne/src/runtime/tests.rs` 从未被声明（`runtime/mod.rs` 无 `mod tests;`，且其中的 `Node::new(...)` 类型早已不存在）——**从未编译、从未运行**的死代码（自 `2b8b046` 起）。已**删除**；其覆盖由 `client_runtime.rs`/`read_index.rs`/新的 `client_redirect.rs` 承担。
2. **杀 leader（M1 验收①②）**
   - `multi_node.rs` 新增 `killing_the_leader_elects_a_new_one_and_writes_never_hang`：SIGKILL leader 进程 → 幸存者接管；窗口内写只能是 `409`/`503`，**绝不挂死**（HTTP 读超时返回 `None` 即判失败）；新 leader 仍服务杀前已提交值。
   - `l2_scenarios.rs` 新增 `new_leader_within_two_election_windows_after_leader_loss`：**确定性 tick 计数**断言换主 ≤ `2×election_tick`（固定种子实测 **16 轮 / 预算 20**）。Gate C 禁止 `tests/` 读真实时钟，故 L3 只能做有界轮询；严格的时序上界由 L2 的 tick 版承担。
3. **bin CLI 集成测试（test-plan §3.3）**：新增 `arachne-node/tests/cli.rs`（**11 项**）+ 共享 `arachne-node/tests/common/mod.rs`：`--help`/参数错误退出码、配置不可读/校验失败 fail-start、**flock 冲突**、**META cluster_id 不一致**、**SIGTERM 优雅关闭并释放锁**、`/readyz` + `/metrics` + 404/405，以及 4 项 `force-recovery`（见下）。
4. **D-ART-testobs**：`arachne-node` 新增 `test-observability` feature（**默认关**；启用时仅追加 stdout marker `{"event":"ready"|"shutdown",…}`，零行为改变，已实测）。新增 `scripts/check-release-features.sh`（Gate A：无默认 feature；Gate B：release 产物**不含** marker，而 feature 构建含之且二者 hash 不同），已接入 CI。
5. **L4 已 ack 写入**：`l4/kill_loop.sh` 每轮发起真实 `PUT /kv/...`（200 = 已提交 + 已 apply），`kill -9` 后重启用线性一致 `GET` 读回复验；循环结束再额外重启一次复验**最后一次**写入。**顺带修复**：`l4/node.toml` / `kill_loop.sh` / `verify_wal.sh` 生成的配置缺 `rpc_timeout_ms`，在 M1-1 引入 profile 校验后**必然 fail-start**（实测复现）——三者已补齐；`l4/README.md` 的"M0 空命题"声明已更新。
6. **`force-recovery` 子命令（propsol §6.1）——现已实现**
   - **存储层**：`WalStorage::force_recovery(dir, node_id, new_cluster_id, config, ts)` + `ForceRecoveryReport`（`arachne/src/storage/wal.rs`，已从 `storage::mod` 导出）。语义：读 META → 以**旧** cluster_id 打开（取得数据目录锁，拒绝并发进程）→ **丢弃未提交尾部**（index > commit；跨 segment 正确截断，丢弃 0 条时为 no-op）→ 写新 HardState（`term+1`、`vote=self`、`commit` 不变）→ 重写 META（默认换 cluster_id）。单测 5 项（丢弃尾部 + 轮换 cluster_id / 保留 cluster_id / 零 commit 丢弃全部 / 锁占用拒绝 / node_id 不一致拒绝）。
   - **M1 membership 边界（如实声明）**：membership 到 **M3** 才持久化 `ConfState`（`raft_storage.rs` 自述静态），故本方法**不**持久化 `ConfState=[self]`；由 CLI 生成/打印**单票 config**（`initial_cluster=[self]`）使该后置条件成立。M3 落地后可去掉这层 config 依赖。
   - **CLI**：`arachne-node force-recovery --config <path> --i-know-data-loss [--keep-cluster-id] [--out-config <path>]`（`arachne-node/src/force_recovery.rs`）。前置条件：必须显式 `--i-know-data-loss`（否则 exit 2）；旧成员可达性预检（3s TCP，可达则**告警**）；数据目录锁（占用 → exit 1）。`--out-config` 写出可直接启动的单票 config。退出码 0/1/2。单测 5 项 + CLI 集成 4 项（含端到端：acked 写入跨 force-recovery 存活 + cluster_id 轮换 + 单票 config 复起）。
7. **发现并修复：持久化的 `commit` 从未被写入（严重）**
   - **现象**：`force-recovery` 报 `commit=0`，把已 ack 的写入全部当作"未提交尾部"丢弃；端到端表现为恢复后 `GET` 返回 not found。
   - **根因**：raft-rs 把 commit 前进放在 **`LightReady::commit_index`** 上，**不是** `Ready::hs`（`hs` 只在 term/vote 变化时携带），且 `LightReady` 路径会同时推进 raft 内部的 `prev_hs.commit`。`RaftNode::step()` 只持久化了 `ready.hs()`，于是 **commit 一旦前进就再也不会被写盘**，`initial_state()` 报出的 commit 永久滞后。
   - **为何此前没暴露**：单节点/多数派重启后会把日志**重新 commit**，指标与数据看起来正常；L4 的"commit/applied 不回退"也因重新 commit 而通过。只有"按 commit 截断"的 force-recovery 会暴露它。**这也说明 L4 原来的 commit 不回退断言对 commit 持久性其实不敏感。**
   - **修复**：`step()` 在 `advance` 之后，若 `light.commit_index()` 为 `Some(c)`，以当前 in-memory term/vote + `c` 调 `set_hard_state`（I1：逐次 fsync），与 raft-rs 自带示例 `examples/single_mem_node` 的用法一致。**代价**：每次 commit 前进多一次 HardState fsync（正确性优先）。
   - **回归测试**：`arachne/tests/force_recovery.rs::runtime_persists_the_committed_index_for_force_recovery`（真实 `Runtime` actor：put 后停机，断言 `force_recovery` 报出 `commit >= 1` 且 `discarded == 0`）。
   - **建议**：值得在 propsol 补一条 rev 说明"durable HardState 必须包含最新 commit（I1 的一部分）"，但**未擅自改动** v0.2.8 设计文档。

**验证（`6c79886` + `adb2bd7`）**：`cargo test --workspace` = **321 passed / 0 failed**（294 → 321）；`check-deps.sh`、`check-entropy.sh`、`check-release-features.sh` 全绿；`cargo build --workspace --all-targets`、`--examples` 无告警；`l4/kill_loop.sh`（3 轮 acked 写入）本地 PASS；`force-recovery` 端到端手工复验通过（恢复后 acked 值仍在）。

### 提交线（新 → 旧）
```
adb2bd7 M1-5 (D): 跨进程 409+hint + 杀 leader 断言 + force-recovery CLI + bin CLI 测试 + test-observability  ← HEAD
6c79886 fix(consensus): 持久化 LightReady 的 commit 前进（durable HardState 的 commit）
635da14 docs: M1 handoff — (C) stage 3c inc.10 gate GO + replay/resync sharpening
cbf1a85 M1-4 (C) stage 3c inc.10: gate P3 sharpen（隔离 WAL replay 与 resync）
e4bedbe docs: M1 handoff — (C) stage 3c inc.10（真实 crash + WAL 重启）
6226ec2 M1-4 (C) stage 3c inc.10: 真实 crash + WAL 重启
bdfea02 docs: (C) stage 3c inc.9（换主后同 seq 重试）
2159fd9 M1-4 (C) stage 3c inc.9: 换主后同 seq 重试（at-most-once）
fba2aca docs: (C) stage 3c inc.8 门禁 GO
6815bef M1-4 (C) stage 3c inc.1: 内存传输确定性 L2 场景（S01/S02+INV3/4/7/8/9）
76df587 M1-4 (C) stage 3b spike: 真实 tonic over turmoil（部分；commit 路径卡住）
377f3ce fix(runtime): actor 截止时间改用模拟 tokio::time::Instant（确定性）
edd913d M1-4 (C) stage 3a: 可注入 transport I/O 接缝（生产默认不变）
f132e44 M1-4 (C) stage 2: D-S1 可播种 raft 选举 RNG [patch.crates-io] + 双跑金丝雀
9bf857f docs: (C) stage 1 确认门禁 GO
395fc9a M1-4 (C) stage 1 自证补强（1000 种子差分/witness 回放/optional 单调性）
a915f6b M1-4 (C) stage 1: ClientOracle v1 + 自建线性化检查器
6616827 M1-3b 真实 ReadIndex 线性一致读（Safe 读 + wait applied + 1 重试）
5081417 M1-5 (A) arachne-node 真实 tonic 3 进程集群 + start_with_bind + config addresses
8cbc765 M1-3a(3/3) follower 写重定向到 leader（真实 tonic 3 节点）
ea08dab M1-3a(2/3) arachne-node 接线 lib runtime + /kv HTTP 端点
2b8b046 M1-3a(1/3) lib 客户端 Handle + runtime actor
67358fa ci: 安装 pinned protoc 3.x
148e3a7 fix(build): find-protoc.sh 兜底 PyTorch 3.x protoc
ffef8ef M1-2 整改（超时/有界入站/消息尺寸/start 泄漏/shutdown）
60d61e2 M1-2 tonic 真实传输 + 3 节点网络证明
b3043b3 docs: propsol v0.2.8（E-rev K：修正 §7 约束与预设矛盾）
151066a M1-1 Profile/配置 + tick 接线 + crate 文档语义表
69d3038 M0 终检整改（关闭）
… (M0 的 P1–P5 提交见 git log)
```

## 2. 工件与关键路径

- `arachne-seam`（零依赖叶：接缝 trait + 核心类型）、`arachne`（产品核心：WAL/raft 集成/KV 状态机/客户端库）、`arachne-transport-tonic`（唯一含 tonic/prost 的 crate）、`arachne-node`（运维 bin）、`arachne-testsupport`（dev-only）、`arachne-sim`（L2 bin）。
- 非工作区独立项目：`fuzz/`、`model-check/`（stateright）、`l2/`（turmoil）、`l4/`（kill -9 脚本）。
- 客户端 API：`arachne::client::{ArachneError, Handle}`；运行时 actor：`arachne::runtime::{Runtime, RuntimeConfig, Command}`；指标：`arachne::Metrics`。
- `arachne-node` 的 HTTP：`/readyz`、`/metrics`、`PUT /kv/<key>/<value>`、`GET /kv/<key>[?stale=1]`、`DELETE /kv/<key>`（非法参数→400、quorum→503）。**多进程注意**：非 leader 的写当前返回 **503**——`Handle` 的 redirect 只认识**进程内** peer（`register_peer`），跨进程没有 client redirect，因此文档语义中的 `NotLeader→409+leader hint` 在多进程 HTTP 面**不可达**（进程内嵌入仍有 409）。跨进程 409+hint 留给 (D)。

## 3. M1 剩余工作（按推荐顺序）

### (A) arachne-node → tonic 多进程（M1-5/L3 前置）— **已完成（commit 5081417）**
实现与最初建议的差异（供 (D)/(B) 接手）：
1. `TonicTransportFactory::start_with_bind(me, bind)` 已加（`start()` 会绑定地址表**所有**条目——多进程会 EADDRINUSE）；公共 serve 逻辑抽到私有 `start_targets`，`start()` 行为不变。
2. 配置：`initial_cluster` 条目现支持 `"<id>"` 或 `"<id>=<ip:port>"`（**向后兼容**，l4/ 脚本的 `["n1"]` 不变）；`Config.addresses: HashMap<NodeId, SocketAddr>`，self 恒为 `listen`（若 self 内联地址与 `listen` 不一致 → fail-start），**非 self 成员必须给出内联地址**（缺失 → fail-start）。
3. `node.rs` 用 tonic 传输 + `RuntimeConfig{ peers, addresses }`（peers 由 `initial_cluster` 的 1-based 下标导出）；`shutdown` 先 `factory.shutdown().await` 再 abort actor。M0 占位 `transport.rs` 已删除。
4. 多进程 e2e：`arachne-node/tests/multi_node.rs`（3 个真实进程，`/readyz`→PUT→GET→`?stale=1` 复制，Gate C 合规）+ 传输级 `arachne-transport-tonic/tests/multi_node_bind.rs`（per-node bind + 跨 factory 投递 + 0 握手拒绝）。
顺带修复：`http.rs::dispatch_line` 原先只放行 GET，导致文档化的 `PUT`/`DELETE /kv/...` 永远 405——现放行 `GET|PUT|DELETE`，其余 405。

### (B) M1-3b：ReadIndex 线性一致读 — **已完成（commit 6616827）**
- `ReadOnlyOption` 由 `LeaseBased` 改为 **`Safe`**（propsol §149 明确 v1 禁 Lease Read，读一致性只靠 ReadIndex，不靠时钟）。
- `RaftNode`：新增 `read_index(ctx)`；`step()` 返回 `StepOutcome { committed, read_states }`（`read_states` 在 `advance` 之前从 `Ready` 捕获——只出现在 `Ready`，不在 `LightReady`）。
- actor：`Command::Get` → `Command::Read`；`PendingRead` 等待队列（上限 4096 → `Busy`）；读状态 ctx→token 关联后等待 `applied ≥ read_index`，超时 `read_index_timeout`(=2×election) 重试 1 次，term 变化/step-down → `NotLeader{hint}`，仍失败 → `Timeout`。follower 直接 `NotLeader`（§5.4 step 5，不做服务端转发）。
- 指标：新增 `arachne_read_index_timeout_total`（counter）、`arachne_read_index_pending`（gauge）。
- 测试：`arachne/tests/read_index.rs`（进程内 3 节点：leader ReadIndex 读、follower `get` 重定向、缺失键 `None`）+ node 单测（read_state index 语义）。
- **顺带修复**：`client/mod.rs` 首页语义表与 `arachne-node/main.rs` 的 GET 说明原为"临时 leader-local 读"，已更正为 ReadIndex；`ArachneError::Busy` 文案覆盖读等待队列。
- **已知缺口/延后**：`read_index_round_latency` 直方图留 M2（无直方图设施）；`redirects_total` 属客户端侧（`Metrics` 无法从 `Handle` 触达）；**"已被废黜但尚未察觉的旧 leader 不得服务陈旧读"这一决定性性质需要分区注入 → 属 (C)/L2（INV14、S02/S16）**。现有 `read_index.rs` 健康集群下无法区分 ReadIndex 与旧 leader-local 读（已记录）。

### (C) M1-4：L2 turmoil + 不变量 + 线性化（M1 验收 ④）— **分阶段执行中（用户选定：每阶段一个 oracle 门禁）**
**stage 1（已完成，commit a915f6b）：ClientOracle v1 + 自建线性化检查器**
- `arachne-testsupport::oracle`：无时钟纯逻辑历史（`ValueId`/`ClientId`/`SeqNo`/`CallId` + 逻辑 ts）、`merge_retries`（记录层保留原多次调用）、检查①幻值（区间语义：Put 可解释读当且仅当 `put.invoke < get.complete`）、②one-log-id-per-seq（含 Put|Delete）、③RYW（latest-mutation-wins）、④单调读，可选⑤丢写/⑥持久性；`History::validate` 对畸形历史（complete 无 invoke／CallId 复用／同 `(client,seq)` op 不一致）**报错而非静默丢弃**；§9 JSONL。
- `arachne-testsupport::linearizability`：实时偏序上的完备回溯搜索（Wing–Gong 等价）；**required**点（成功 op）+ **optional**点（结果未知/在途写，故 timeout-committed 竞态不再是假违规）；超 `MAX_CHECK_POINTS=12` → `Inconclusive`；witness 贪心最小化；确定性（BTree/Vec、无 HashMap 迭代）。
- 测试：testsupport 79 项（含每检查一条坏历史元测试、贪心对抗回溯正例、**32 种子与 stateright 差分一致**——stateright 仅 dev-dependency，结构化隔离经 Gate C 验证）。
- **门禁状态：GO（已通过）**。初次 oracle 门禁 NO-GO（B1 并发 put/读假阳性、B2 自删 RYW 假阳性、B3 timeout 写被当作未生效）已按门禁处方修复并加回归；期间 oracle provider 出过故障（模型 id `zhipuai-coding-plan/GLM-5.3-Flash` 无法解析 + 空结果），先补强本地自证（commit `395fc9a`：1000 种子多键差分对 stateright、witness 回放 500 种子模糊、optional 单调性、500 顺序历史 oracle+checker 双通过）。provider 恢复后**确认门禁经 ora-2 判 **GO****：三个 blocker 均被证明真实修复且回归测试在旧代码上会失败；S1–S5 解决；新增自证被判为实质性。**遗留 nit（L2 阶段跟进）**：(a) 差分生成器均为顺序历史，未对并发/排序搜索做 stateright 交叉验证（现靠手写并发/回溯用例覆盖）；(b) `MAX_CHECK_POINTS=12` 在对抗性全失败历史上仍有 ~12! 最坏情况——L2 接真实交错历史时需降 cap 或加 memoization。stateright 交叉验证暂限"全成功"历史（未知结果写不喂 stateright，避免 pending-invocation 建模偏差），门禁判定为**可接受的已记录缺口**。

**stage 2（已完成，commit f132e44）：D-S1 可播种 raft 选举 RNG**：`arithmetic raft` 0.7.0 唯一 RNG 点（`reset_randomized_election_timeout`）原用不可播种的 `thread_rng`。已 vendor `third_party/raft/`（与 registry 仅差一个最小可上游化 hook：thread-local 可播种选举 RNG + 每节点子流 `base ^ node_id`；未设种子时行为与上游逐字节一致），root `Cargo.toml` 加 `[patch.crates-io]`，`ARACHNE-PATCH.md` 记录 diff 与「需单线程」约束；`arachne/tests/determinism_canary.rs` = 同种子双跑轨迹逐字节一致 + 不同种子→不同选举超时。**门禁 GO**；已知 nit：Cargo.lock 附带把 windows-sys 0.61.2→0.52.0（semver 合法、Windows-only）；hook 需单线程（E8），已在 PATCH.md 记录。
**stage 3a（已完成，commit edd913d）：transport I/O 接缝**：`arachne-transport-tonic` 新增 `TransportIo` 接缝 + `TokioIoProvider`（真实 tokio）+ `TcpConnector`；`TonicTransportFactory`/`TonicTransport` 对 `Io = TokioIoProvider` 泛型，server 经 `io.bind`/`io.incoming`、client 经 `Endpoint::connect_with_connector`。既有 API 不变（默认类型参数保住 `arachne-node` 用法）；`hyper/hyper-util/tower(util)/http` 均经 tonic 已传递，无新包。**门禁 NO-GO→修 `TcpConnector` 丢 `TCP_NODELAY`（tonic 默认连接器会设、自定义连接器绕过）→GO**。
**stage 3b（部分完成 → 记为 spike，commit 76df587）：真实 tonic over turmoil + SimNetwork**。已落地并可编译：`l2/` 的 `TurmoilIo`（`turmoil::net` listener/stream + `Accepted` newtype 实现 tonic `Connected` + `TokioIo` connector）、`SimNetwork`（partition/partition_oneway/repair/hold/release/crash/bounce/set_fail_rate/set_link_latency）、3 节点 in-sim 真实 tonic 集群；两个 `in_sim` 测试标 `#[ignore]`（原因见下）以保 `l2` suite 绿。
- **已证明可用**：3 节点全部启动（WAL+bind+Runtime actor）；在 turmoil 承载的真实 tonic 上**成功选出 leader**；follower 学到 leader；消息双向健康（`MsgHeartbeat`/`MsgHeartbeatResponse` 双向、term 1）。
- **卡点（已解，见 §1.6）**：客户端 `put` 永不提交。**旧记录的两处结论均已被推翻**：(a) 不是"actor 收不到 `Command::Propose`"——命令会被收到并被答复；(b) 不是 actor 逻辑死锁。根因是 harness 未设显式链路延迟、用了 turmoil 的大抖动默认延迟，超过 election_timeout 导致 CheckQuorum 反复降级。
- **附带修复**：`Runtime` 的 propose/ReadIndex 截止时间原用 `std::time::Instant`（真实墙钟）→ 在模拟器下是确定性泄漏，已改 `tokio::time::Instant`（commit 377f3ce，root 仍 277 绿）。
- **现状**：3b **已解决**——`l2/tests/in_sim.rs` 两个测试已去 `#[ignore]` 并通过，新增 `l2/tests/transport_echo.rs` 传输门禁，CI 增 `l2` job；L2 不变量/场景另有 **20 项**跑在内存传输（`arachne/tests/l2_scenarios.rs`），两条轨道互补。

### 1.6 stage 3b 复诊 → **已解决**（`l2` · real tonic over turmoil）

**结论：不是产品缺陷，是 L2 harness 的配置问题。** harness 之前没有设置显式链路延迟，于是使用了 **turmoil 的默认链路延迟**——它大且抖动（单次 RPC 往返几十毫秒模拟时间、尾部过百毫秒），超过节点的 `election_timeout`，使 leader 被 CheckQuorum 反复降级 → 选举抖动（term 1→2→3…）→ 客户端 `put` 先 `Timeout` 后 `QuorumUnavailable`。**harness 现显式固定 1ms 链路延迟**，`l2` 全部测试通过且**不再 `#[ignore]`**（3 节点真实 tonic 集群、双跑确定性、传输 echo），并新增 CI `l2` job。

**定位过程（探针均已回退）**
1. **事件序列探针**：actor 停在 `drive_cycle` 内，而这里唯一的 `await` 是 `deliver → transport.send`；同一时刻其他 actor 继续 tick。
2. **传输探针**：`n2->n3 connected` 之后没有 `replied`（该方向此前成功 7 次）；对端 actor 仍 tick，accept 未报错。
3. **命令探针**：`begin cmd` 紧跟 `Propose rejected as non-leader` —— 命令**被收到并被答复**（推翻旧记录的"actor 收不到 `Command::Propose`"）。
4. **最小可复现对照** `l2/tests/transport_echo.rs`（纯传输、无 raft/WAL，带 `ECHO_*` 旋钮与延迟分布）：
   - 默认链路延迟：`p50≈40–50ms`、`p90≈73–97ms`、`p99≈100ms`，100ms 界内偶发超时；
   - **显式 1ms：`p50=p90=p99=2ms`（一个 RTT）、0 超时**；显式 10ms：`p50=p99=20ms`、0 超时；
   - 关 keep-alive、预热、改发送间隔、单向/双向**都不改变结论** → 排除 keep-alive / 连接建立 / 排队 / 方向，确定为**默认网络模型的延迟量级**。
5. 把显式 1ms 加进 `l2/src/harness.rs` 后：3 节点集群 `put_ok=true`，双跑确定性通过。

**留下的门禁**：`l2/tests/in_sim.rs`（2 项，已去 `#[ignore]`）+ `l2/tests/transport_echo.rs`（1 项：默认显式 1ms，`ECHO_LINK_LATENCY_US=0` 可复现默认延迟导致的停机）；CI 新增 `l2` job 跑 `cargo test`。**教训**：模拟器下必须显式设定网络模型参数——其默认值可能远超测试 profile 的超时。

**stage 3c（增量 1 已完成，commit 6815bef + 46a8328；门禁 GO）：内存传输上的确定性 L2 场景**。新增 `arachne/tests/l2_scenarios.rs`：进程内 3 节点 harness（沿用 `m0_determinism` 的同步 `RaftNode` 模式：手工 `tick`+`block_on(step)`+`on_message`，harness 自控消息投递），用 harness 级 `Faults{isolated}` 丢弃隔离节点往返消息来注入分区/崩溃；选举 RNG 用 stage-2 的 `raft::set_election_rng_seed` 保证 leader 身份可复现。
- 场景/不变量：**S02**（2+1 分区，并断言隔离节点确实未收到多数侧写、多数两侧都提交）+ **INV7**（全轨迹每 term ≤1 leader）；**S01**（隔离=崩溃旧 leader → 幸存者选新主 → 复活收敛）+ **INV9**（新主含已提交条目）+ **INV8**（重叠 index 日志一致）+ **INV3**（收敛后状态快照逐字节一致）；**INV4**（M1 验收④：分区下客户端 put/get 历史经 stage-1 ClientOracle **与** 自建检查器判定为线性一致）；**双跑确定性**（同种子 → leader 轨迹与日志逐字节一致）。
- 门禁 NO-GO 风险已修：分区此前只"配置"未被断言，现已断言（P2）；四测 4/4、workspace 281 绿。
- **增量 2（已完成，commit d895d1c）**：`Faults` 增单向丢包；**S16**（丢所有 follower→leader 消息 → CheckQuorum 将 leader 降级；INV7 仍成立）+ **换主 INV4**（历史中隔离 leader、幸存者选新主、客户端改投新主，整段 put/get 历史经 oracle+checker 判定线性一致；断言恰有一次换主）。`l2_scenarios` 6/6。
- **增量 3（已完成，commit 2b3e3b0；门禁 GO，ora-9；P4 空白已修 777fd6f）**：**并发 INV4**（两客户端 put/get 实时区间重叠 → 真正驱动检查器的并发搜索路径；oracle+checker 双通过）+ **ReadIndex 局部性**（leader 的 `read_index` 经全量投递产生读状态；follower 在不投递时不自服务读，只转发）。harness 记录全量读状态 `(node,ctx,index)`；`l2_scenarios` 8/8。
- **增量 4（已完成，commit dfc92be；门禁 GO，ora-9；P4 修正 5c4d6e4）：多键 INV4 + 分区下并发**：多键（"a"/"b"）写读交替 + 读一个从未写过的键断言为 `None`——使 oracle 首次跑在真正**多键**历史上（跨键混淆会被 harness 直断言 + checker 逐键模型 + oracle 正向幻值检查共同捕获；**注意**：absent-key 的 `None` 读走 oracle 的跳过路径，其"负路径"由 testsupport 单测 `phantom_read_is_a_violation_with_witness` 覆盖）；两客户端实时重叠区间在隔离一个 follower 的 2+1 分区下由多数侧服务、历史仍线性一致（断言被隔离者确实饥饿）。`l2_scenarios` 10/10。
- **增量 5（已完成，commit 2f35da1；门禁 GO，ora-9；P4/保留加强 97f785a）：INV4 跨越换主**：某次 put/get 在换主前被调用、换主后在**新 leader** 上完成（其实时区间横跨 leadership change）；oracle+checker 仍判定线性一致，并断言轨迹出现非初始 leader；加强：对该换主轨迹断言 INV7，且**新 leader 必须保留换主前已提交的条目**（独立键 `"old"`）。`l2_scenarios` 11/11。
- **增量 6（已完成，commit 32b8b7e；门禁 GO，ora-9；P4 打磨 f010304）：oracle 幻值负路径（注入）**：两条注入式违规测试断言 oracle 对幻值读**判定失败**——读一个从未写过的值、以及跨键读（值只写给另一个键）；打磨后两条测试**同时**断言自建检查器亦判 `Violation`。在 L2 层闭合幻值检查的负路径（此前仅 testsupport 单测覆盖）。`l2_scenarios` 13/13。
- **增量 7（已完成，commit 272967a；门禁 GO，ora-9；P3/P4 打磨 f4f5b72）：ReadIndex 路径的读**：客户端读改走 raft ReadIndex（leader `read_index` → 等 quorum 确认的读状态 → 等 `applied ≥ read_index` → 再读 SM），而非直接读 leader 状态机；所得 put/get 历史经 oracle+checker 判定线性一致，并以"每次读都产生读状态"作非空断言；打磨后加"applied 屏障确已到达"断言。闭合"读绕过 ReadIndex"缺口——**M1 ④ 的核心主张（客户端历史在真实共识读机制上线性一致）至此已演示**。`l2_scenarios` 14/14。
- **增量 8（已完成，commit e1b7f31）：故障下的 ReadIndex 读**：一个读在 leader0 上发起后 leader0 被隔离——断言其**不能完成**（无该 ctx 的读状态，绝不返回陈旧值），客户端记录为失败（`OpResult::Err(Timeout)`，非线性化点）；换主后由新 leader 服务一次新的 ReadIndex 读。整段历史（含失败读）仍线性一致。`l2_scenarios` 15/15。
- **增量 9（已完成，commit 2159fd9）：换主后同 seq 重试（at-most-once）**：客户端对 `(client 0, seq 1)` 的 `Put` 先在 leader0 上发起、随即隔离（已 propose 但**未提交**，断言旧 leader 已提交日志无该命令）记为失败；换主后**用同一 `(client, seq)` 在新 leader 重试**成功，再以 ReadIndex 读观察该值。断言 oracle+checker 通过且 `merge_retries().ops.len()==2`（两次尝试合并为一个逻辑 op）。另给 inc.8 的换主轨迹补 INV7 断言。`l2_scenarios` 16/16。
- **增量 10（已完成，commit 6226ec2；门禁 GO，ora-9；P3 打磨 cbf1a85）：真实 crash + WAL 重启**：harness 现支持 `crash(i)`（丢弃节点=任务死亡，保留 WAL 目录）与 `bounce(i)`（从同一目录重开 `WalStorage`、重建 `RaftNode(applied=0)`、**重置内存状态机并清空已记录日志**＝volatile 丢失）。场景：崩溃一个 follower → 多数侧提交 v2 → 重启该 follower → 断言其状态机恢复出 v2，且全节点日志一致（INV8）、状态快照一致（INV3）、INV7 成立；打磨：重启后**仅驱动该节点（不投递）**断言其已由 WAL 重放出 v1（区分 replay 与后续 resync）。**M1 ④ 的最后一项实质内容已闭合。** `l2_scenarios` 17/17。
- **M1 ④（L2）覆盖小结**：S01/S02/S16；INV3/4/7/8/9；双跑确定性；INV4 共 9 种形态（顺序、分区下、换主、并发、多键、分区下并发、ReadIndex 读、故障下 ReadIndex 读+恢复、换主后同 seq 重试 at-most-once）；oracle 负路径（幻值/跨键）；真实 crash+WAL 重启。
- **剩余（非阻塞）**：更大规模/随机种子历史与更多故障组合。~~follower 服务读的完整来回~~ 已由 `follower_read_index_round_trip_completes` 覆盖（运行时契约差异已记入 `client/mod.rs`）；~~oracle ② 空转~~ 已由 `oracle_check_two_uses_real_log_ids` 激活；~~3b（real-tonic-on-turmoil）~~ **已解决**（§1.6）。

### 1.7 CI 暴露并修复的问题（`d788878` + `d13d129` + `a26d21d`）

仓库首次 push 到 GitHub 后 CI 多次变红（`l2` job 一直通过），暴露出三个问题（其中第 3 个是真正的读路径 liveness bug）：

1. **测试竞态（`d788878`）**：`three_node_client.rs` 只等"存在一个 leader"，随后对 follower 的 handle 只 `put` 一次。重定向需要 leader hint，而新选出的 leader 未必已到达每个 follower；缺 hint 返回 `QuorumUnavailable`（handle 不重试）。本地 macOS 侥幸通过，2 核 CI runner 上失败。改为：先等该 follower 报告 leader hint，再对窗口期错误做**有界重试**（全部失败仍判失败，重定向契约仍被断言）。
2. **`WalStorage::append` 缺覆盖语义（严重，本次修复）**：raft 在选举后会把**冲突后缀**重新交给存储（follower 必须覆盖上任短命 leader 的条目）。`append` 原本假设纯尾部追加、只用 `debug_assert` 守连续性 → debug 下 panic（`append continuity violation: expected 2, got 1`，actor 任务死亡）→ 测试在后段 `get_stale` 收到 `ShuttingDown`；**release 下 `debug_assert` 被编译掉，会写入重复 index，静默损坏日志**。现在 `append` 在追加前先截断到首个入参 index（复用 force-recovery 的物理截断，改名 `truncate_log_to`）；覆盖**已提交**条目属 raft 安全违规，故 fail-stop。新增 3 个单测（冲突后缀重写 + 重开后的持久性、从 index 1 覆盖、拒绝覆盖已提交）。

### 1.8 M2 第一块（已完成）：FaultyStorage + fsync 台账 → INV1 两半在 L2 落地

M1 的验收项与已记录缺口已全部闭合，现按 `l2/README` 的 M2 路线推进。

- **`DurabilityLedger`（新，`arachne-testsupport/src/durability.rs`）**：每节点一份"已持久化"台账 = **条目 fsync**（内嵌 `FsyncLedger`，经 WAL 的 `FsyncObserver`）+ **HardState 持久化记录**（term/vote/commit）。seam 没有 HardState-fsync 回调，但 `FaultyStorage` 位于 raft 与 WAL 之间、能看到每次 `set_hard_state`，而 `WalStorage::set_hard_state` 返回即已 fsync（I1）——因此"记录到的 HardState 就是持久的"。提供 `entries_cover` / `max_persisted_term` / `persisted_commit`。**注意**：HardState fsync 也会让同 segment 内**已写入但未 fsync** 的条目字节变持久（fsync 刷整文件），所以 `entries_cover` 而非"事件非空"才是正确的判据。
- **`FaultyStorage` 扩展**：新增注入 `fail_append_at`、`fail_sync_entries_every`（原有 `fail_sync_entries_at` / `fail_set_hard_state_at`），并新增 `with_ledger(...)`——**成功**写入的 HardState 才记入台账（失败的 fsync 不记）。单测 +3。
- **新 L2 场景 `arachne/tests/m2_durability.rs`**：3 节点、真实 `WalStorage` 外包 `FaultyStorage`、每节点一份 `DurabilityLedger`；**每条出站 raft 消息在 `Transport::send` 时**与台账对账，闭合 INV1 两半：
  - **I2/I4**：消息携带的每个条目都已被台账覆盖；
  - **I1**：消息的 `term` 不超过该节点已持久化的最高 term——**pre-vote 消息豁免**（`pre_vote: true`，pre-vote 故意探测未来 term 且不持久化，这是它存在的意义）。
  - 场景：① follower 崩溃 + 重启后 INV1 仍成立、已提交条目保留（INV2 的"已提交"半边）；② 全节点注入 `sync_entries` 失败 → 节点 **fail-stop**，全程**没有任何条目上过线**、也没有任何条目变为持久；③ 两个探测器各自的负向对照 + 一个无假警报对照。5 项测试。
- **与既有覆盖的关系**：`m0_inv1_ordering.rs`（M0 验收③）已证 I2/I4 半边（2 节点、健康、send 时对账）；本块补齐 **I1** 半边，并把存储换成故障注入包装、加入崩溃重启。
- **字节级故障（本轮补齐，`arachne/tests/m2_wal_faults.rs`，3 项）**：在**真实单节点 runtime 已 ack 写入**的数据目录上做字节级注入，并绑定 INV2：
  - `META` 变异（空/截断/坏 magic/坏 CRC/payload 逐字节位翻转）→ **必须 fail-start**，绝不静默重建并接管该目录；
  - **活 WAL 变异扫描**：对每个截断偏移 + 有界位翻转做两象限分类（合法前缀 / fail-start），断言永不 panic、恢复前缀连续且不增长、且"仍在盘上的字节"不会被静默改坏；
  - **撕裂尾**（最后 1 字节缺失）→ 合法截断，且**在该目录上重启 runtime 后仍能读回 acked 值**。
  - **更正**：我先前的记录称 INV6 "只由 `fuzz/` + L4 承担"是**错的**——`arachne/tests/wal_mutation.rs`（M0 ②）早已有 PR 级确定性变异电池（合成 WAL：全偏移截断 + 位翻转 + len 破坏 + 非末段 fail-start + 已提交区不丢）。本文件补的是**活 WAL**、**META 变异**与**恢复后仍可服务**。
  - **边界（如实声明）**：META 不记录"期望的最后 index"，且 N3 明确规定"有 META 无 segment = 合法空日志"（`meta_only_dir_reopens_as_empty_log`）。因此**移除已提交前缀字节的变异会得到更短的前缀（甚至空日志）**，恢复无法与"本来就是空"区分——INV6 的两象限允许这一结果。故强断言（acked 前缀必须在场）只施加于"不可能移除已提交字节"的变异（无变异基线、尾 1 字节撕裂）。
- **INV2 崩溃扫描（本轮补齐，`m2_durability.rs` 第 6 项）**：在 **harness 可表达**的崩溃点（`step` 之前 / `step` 之后未 apply / apply 之后）扫描「崩溃目标（leader 或 follower）× 崩溃时刻」，重启后**仅驱动该节点、不投递消息**以走纯 WAL 回放：
  - 重启后立即读到的 commit（从盘上 HardState 恢复）≥ 崩溃前**已持久化**的 commit；
  - 崩溃前**已持久化**的已提交前缀在回放后逐字节重现（顺序与内容一致）→ 不丢已提交条目；
  - 之后重新入群，全节点状态快照一致（INV3/INV8）。
  - **顺带澄清一个语义细节（重要）**：`RaftNode::hard_state().commit` 是 raft 的**内存** commit，可能**领先于持久化 commit**（要到下一个 `step()` 才落盘）。因此 **INV2 的基准必须是"已持久化 commit"**（`DurabilityLedger::persisted_commit`），而不是内存值——我第一版扫描拿内存值当基准，被这个差异判成**假违规**（实测该 follower 的持久 HardState 只有 `(term1,commit0)`、`(term1,commit1)`，而内存 commit 已是 2）。**ack 路径不受影响**：runtime 只在 `step()` 落盘 commit 之后才 apply + ack。
  - **精确崩溃注入（本轮补齐，propsol v0.2.9 L）**：新增**默认关闭**的 crate feature `fault-injection`（`arachne/src/fault_injection.rs`），在 `RaftNode::step` 的 `AfterPersist`（entries+HardState 已落盘、尚未发出任何消息）与 `AfterDeliver`（消息已发、尚未 apply）两个边界提供 **thread-local 一次性**崩溃 hook；关闭时调用点被 `#[cfg]` 完全编译掉（零行为改变、零运行时开销）。`m2_durability.rs` 新增 `inv2_precise_ready_stage_crash_replays_the_durable_prefix`（feature 开启时运行；leader/follower × 两阶段共 4 例）：崩溃后从 WAL **纯回放**，断言恢复出的 commit ≥ 崩溃前**已持久化** commit、applied ≥ 该 commit、**leader 已 ack 的写入不丢**、重新入群后全节点状态一致。CI 增加 `cargo test -p arachne --features fault-injection --test m2_durability` 一步；`scripts/check-release-features.sh` 新增 **Gate C**（默认 release rlib **不含** hook 哨兵、feature 构建含之——用 `--message-format=json` 取产物，避免 mtime 误判）。
- **仍未覆盖（M2 剩余）**：`slow_fsync`（逻辑 `Storage` seam 无法用模拟时间表达；属 L4/文件层时序）；INV5 属 M3（会话去重）。**I3 与快照路径已在 §1.9 落地。**

3. **客户端读截止时间短于 actor 的 ReadIndex 预算（`a26d21d`，本次 CI 再次暴露）**：`Handle` 原本把**所有**操作都限制在一个 `election_timeout` 内——对写是对的（runtime 的 propose 超时相同），对读太短：runtime 的 ReadIndex 路径每次等 `read_index_timeout_ms`（= 2×election）并可重试一次，actor 合法地可能用约 `2×read_index_timeout`（≈4×election）才给出结果。于是客户端在 actor 尚在解析读时就放弃，把一次慢的 ReadIndex 轮次变成客户端可见的 `Timeout`/503。现在读有独立截止：两次 ReadIndex 等待 + 一个 election 的调度余量。CI 上的表现正是 L3 杀 leader 测试偶发失败（换主后新 leader "读不到"杀前值）——实际是读超时而非数据丢失；修完后 CI 绿。

### 1.9 M2 第二块：快照 / 压缩 / 追赶（propsol v0.2.10 M）

设计权威见 propsol **rev M**（本块新增 5 行决议：落盘形态、最新快照判定、META 指针、压缩边界、**快照触发口径**与**follower 安装语义**、恢复时成员配置）。四个层次一次打通：

- **存储层**（`arachne/src/storage/snapshot.rs` 新 + `meta.rs` + `wal.rs`）：
  - 独立快照文件 `snapshot-<index:020>-<term:020>.snap`，布局 `[magic][format_version][index][term][voters][learners][data_len][data][crc32c]`，CRC 覆盖其前全部字节；原子写（tmp → fsync → rename → 目录 fsync）。
  - `Meta` payload 尾部追加 `snapshot_index/snapshot_term`（16 字节，**版本仍为 1**；旧文件 decode 为 (0,0)）。
  - 水位：`first_index = compacted_to + 1`，`term(snapshot.index)` 在条目被压缩后**仍可回答**（raft 追赶时就要问这个）。
  - `save_snapshot` → 落盘 → 更新 META 指针 → 只保留最新两份（最新一份 CRC 坏时可回退上一份，`open` 会扫描目录取最新**合法**者）。
  - `compact(to)` 只删**完全**位于 `to` 之前的段，且要求已有覆盖 `to` 的快照；否则 `Unrecoverable`（fail-start），**绝不删没有快照覆盖的条目**。
- **seam**（`arachne-seam/src/storage.rs`）：新增 `save_snapshot` / `install_snapshot`（默认实现**响亮失败**，只有能落快照的存储覆盖它）。
- **raft 适配 + 节点**（`consensus/raft_storage.rs`、`consensus/node.rs`）：`from_raft_snapshot` / `save_snapshot` / `install_snapshot` / `compact` / `term` / `log_bytes` 透传；`persist_ready` **先安装快照再 append 条目**（一个 `Ready` 里两者可能同时出现，条目从 `snapshot.index+1` 起），并把安装的快照经 `StepOutcome.snapshot` 交给上层；`StepOutcome.committed` 已按快照 index 过滤（安装点覆盖本周期已提交条目，直接 apply 会触发状态机的 `IndexViolation`）。`initial_state()` 现在返回**快照携带的 `ConfState`**。
- **runtime**（`runtime/mod.rs`）：启动时先读持久快照并 `sm.restore`，再建节点（raft 的 applied 从 `first_index-1` 起算，正好等于快照 index，两者天然对齐）；`drive_cycle` 收到安装快照时先 restore 再 apply；新增 `maybe_snapshot` 触发"落快照 + 压缩"。指标新增 `wal_bytes`、`snapshot_last_duration_ms`、`snapshot_last_size_bytes`、`snapshots_created_total`、`snapshots_installed_total`。

**本块发现并修掉的两个真问题（都不是新代码引入的，但被新代码逼出来）**：
1. **`append` 用 `entries.last()` 当"日志尾"**：`entries` 被压缩清空后 `last_index()` 应为 `compacted_to`，而旧写法回落到 0 → 连续性与覆盖点判断双双错位（表现为 `append continuity violation: expected 1, got 9`）。改成一律走 `self.last_index()`，并且把原来的 `debug_assert!`（release 下**不存在**）升级为硬错误：带空洞的 append 会静默造出"有洞的持久日志"。新增"append 落在压缩水位及以下 → fail-start"的守卫。
2. **物理字节数不能当触发输入**：v1 是段粒度压缩，含已压缩前缀的段要等下一次 rollover 才能回收字节，用物理 `wal_bytes` 判阈值会在跨过阈值的**每个条目**上重复触发快照。改为**逻辑增长**（自上次快照以来已 apply 的日志字节数达到 `snapshot_threshold` 即触发，快照后归零），`wal_bytes` 仍作为物理占用指标上报（rev M 新行）。

**外加一个实测出来的恢复语义（重要）**：follower 安装快照时若只删 ≤ index 的条目、保留 > index 的本地条目，重启恢复会看到"陈旧条目 + leader 重发条目"之间的**空洞**而 fail-start（第一版就是这么挂的：`log index gap: expected 4, got 9`）。因此 `install_snapshot` 采用对齐 etcd `MemoryStorage.ApplySnapshot` 的**整体替换**：清空全部条目 + 把物理日志轮换为从 `snapshot.index+1` 起的空段（删除其余所有段）。安装时本地 > index 的条目必属另一分支且未提交（raft 断言 `snapshot.index ≥ committed`），丢弃安全，缺的由 leader 重发。

**测试**：存储单测 +13（快照编解码 8、META 指针 1、WAL 水位/回退/剪枝/越界拒绝/段粒度删除 + 安装 3）；端到端 2 项（新 `arachne/tests/m2_snapshot.rs`，真实 `Runtime` actor + 3 节点内存传输）：
- `lagging_follower_catches_up_through_a_snapshot`（**M2 验收 ①**）：杀掉一个 follower → leader 写入远超阈值（自动落 2 次快照并压缩）→ follower 带旧 WAL 重启 → 断言它**装上了快照**（`snapshots_installed_total > 0` + 盘上有快照文件）、追平最新值、且**只可能来自快照**的那个 key（写于 follower 宕机之前、已被 leader 压缩）仍在；最后线性一致读与全节点 applied index 一致。
- `a_restart_rebuilds_the_state_machine_from_the_snapshot`：单节点写入跨阈值 → 重启后**启动即从快照恢复**（`applied_index >= newest snapshot index`，空状态机这里必然是 0）、压缩掉的早期 key 与日志尾部 key 都能读回。
- `killing_the_leader_does_not_interrupt_a_follower_catching_up`（**M2 验收 ②**）：lagging follower 重启与旧 leader 被杀的**同时**发生，两个存活节点各自都已压缩过日志 → follower 只能从**新 leader** 拿快照；断言新 leader 产生、写入恢复、follower 装上快照并追平两个时代的 key。
- **确定性 L2 场景 S03**（`arachne/tests/l2_scenarios.rs`，同步内存传输、逐轮驱动、可字节比对）：`s03_lagging_node_catches_up_through_a_snapshot` + `double_run_snapshot_scenario_is_deterministic`。与上面两个真实 async 测试互补——它们证"能跑通"，S03 证"**确定性地**跑通"。要点：
  - 用**杀掉再重启**（`crash`/`bounce`）而非分区来制造落后：被分区节点会不断竞选抬高 term，可能换主，而新主日志没压缩过、直接用日志就能把 follower 补上——测试会"绕过"快照路径假绿。重启节点任期陈旧、选举计时器重置，无法干扰现任 leader。
  - leader 调 `snapshot_and_compact`（runtime `maybe_snapshot` 的直接形态）后断言：快照文件落盘、**水位以下的条目确实读不到了**（`term_at(snap_index-1)` 为 Err）、`term_at(snap_index)` 仍可答；随后再写两条，follower 重启后必须"快照恢复 + 尾部回放"两半都走。
  - 断言 follower 的**条目日志跳过了快照覆盖的区间**（`!contains(victim_applied_before+1)`）却包含 `snap_index+1`，且最终 `applied_index`、状态机快照、重叠 index 内容与全组一致（INV3/INV8/INV7）。
  - **两个变体**：`s03_restarted_follower_catches_up_through_a_snapshot`（跑过一段、被杀、带短 WAL 重启）与 `s04_new_follower_catches_up_through_a_snapshot`（**从未参与过**的新成员：空数据目录 → leader 首次 append 被拒 → `next_idx` 回退到 1 → 快照）；`double_run_snapshot_scenario_is_deterministic` 对**两种模式**各跑两遍比对 trace 与各节点日志。
  - **顺带发现一条运维边界（重要，值得进 runbook）**：把**已经 ack 过条目**的节点磁盘清空后再放回，raft 会**直接 fatal**——heartbeat 携带 `min(matched, committed)`，而 leader 记着该节点 `matched=6`、对方 `last_index=0`，`commit_to(6)` 越界即 panic（实测 `to_commit 6 is out of range [last_index 0]`；栈在 `handle_heartbeat`）。即"从旧备份/空盘恢复一个成员"**不是**普通复制能处理的场景，必须先把成员移除再加回（M3 ConfChange）或走 `force-recovery`。真正的新成员（从未 ack 过，`matched=0`）没有这个问题——这也是 S04 必须在 `elect` **之前**就把该节点拿掉的原因。已记入 propsol/本文件，M3 成员变更时要一并处理。
  - 为此给确定性 harness 补上了 runtime 的**安装半边**：`round()`/`step_local()`/`step_local_apply()` 现在遇 `StepOutcome.snapshot` 会先 `sm.restore` 再 apply（此前只 apply `committed`，装了快照的状态机会因 `applied` 不连续而 `IndexViolation`）。
- `arachne/tests/quorum_loss.rs`（**M2 验收 ④**）：3 节点杀掉 leader + 一个 follower → 唯一存活节点**永远无法组成多数派**；断言 `put` 与线性一致 `get` 都在有界时间内返回 **`QuorumUnavailable`**（既不成功也不挂死），`get_stale` 仍从本地状态机正常返回值（N1 弱读），并且两个 peer 恢复后集群重新选主、继续写入、断电前的写入仍在。

**该 ④ 测试暴露并修掉一个客户端错误映射缺陷（`client/handle.rs`）**：重定向链里若某个 peer 已经消失（其命令通道关闭），原实现把该 peer 的 **`ShuttingDown`**（"本节点正在关闭"）原样抛给调用方——语义完全错位，且与 §2.1 承诺的 `QuorumUnavailable` 不符。现在：目标不是自己且报 `ShuttingDown` → 视为"该 peer 不可达"，继续尝试下一个候选；所有 peer 都不可达 → `QuorumUnavailable`。目标是**自己**时的 `ShuttingDown` 仍原样上抛（那才是真的本节点在关闭）。`put`/`get` 两条重定向路径同改。

**M2 验收进度**：①②③④ 已证；⑤ 已落地（apply 独立任务 + Q7 反压 + 延迟冒烟门槛入 CI，见 §1.10）。

**如实声明（M2 仍未做）**：
- 快照传输仍是**单条 `Ready` 承载**，未分片流式（§5.5.4 的 server-streaming 分片随传输层快照 RPC 落地）；安装期间本地读返回 `Busy` 也未实现（v1 安装是同步阻塞的）。
- 字节级 `wal_trailing_keep` 窗口未接线（旋钮只在 `ProfileConfig`，未下沉 `WalConfig`），慢 follower 一律走快照路径（rev M 已记录：优化而非正确性）。
- `snapshot_last_duration_ms` 已上报，但 `>1s` 的**告警发射**在 `arachne-node` 侧，尚未接线。

### 1.10 M2 第三块：apply 独立任务 + 反压 + 延迟冒烟门槛（M2 验收 ⑤，propsol v0.2.11 N）

设计权威：propsol **rev N**（8 行决议：通道形态、进度用 `watch`、Q7 字节反压、通道满时合并待发批次、快照请求同序入队、回复归属在 actor 侧算、raft `applied` 不碰、延迟预算口径）。实现：

- **apply 任务**（`runtime/mod.rs` 新 `ApplyTask`）：独占 `KvStateMachine`，从 actor 收 `ApplyRequest`（批量 = 可选快照 + 紧随条目，或快照序列化请求），向 actor 发 `watch` 进度 `{applied_index, applied_bytes_total, failed}`。actor 不再持有 SM。
- **弱读独立通道**：`get_stale` 与线性读取值走 `ReadRequest` 专用通道，apply 任务**优先**以有界突发（`READ_BURST=64`）服务，再处理一个写单元 → 弱读不排写积压后面，也不会饿死 apply。
- **提案反压（Q7 首次真正接线）**：actor 记 `sent_bytes_total`，apply 任务回累计 `applied_bytes_total`，`backlog ≥ proposal_queue_bytes`（64MB）即回 `Busy` 并计 `proposal_busy_total`。此前该旋钮只在 `ProfileConfig` 里躺着，从未生效。
- **通道满不阻塞 actor**：`try_reserve` 失败就把批次留在 `deferred` 下轮再试，新到条目**合并**进去（同序；若新 Ready 带快照则丢弃被覆盖的待发条目）——`send().await` 会把 apply 的背压变成读延迟，正是 ⑤ 要消除的。
- **快照**：actor 发 `ApplyRequest::Snapshot`（与 apply 同序，index 精确），任务返回 `(applied_index, bytes)` 后 actor 才 `term_at` + `create_snapshot` + `compact`；Q4 的「创建阻塞 apply」自然成立。
- **回复提案**：actor 在把条目交给 apply 时按命令字节提取 `(client_id, seq_no) → index`（新增 `KvStateMachine::command_session`），`applied_index ≥ index` 即回复。指标新增 `apply_lag`、`apply_backlog_bytes`、`proposal_busy_total`。

**踩到并修正的两个真问题**：
1. **不能按「进度确认」推进 raft 的 applied**：第一版在 `absorb_progress` 里调 `advance_apply_to(applied_index)`，直接 fatal——`applied(1) is out of range [prev_applied(2), …]`。raft-rs 的 `RawNode::advance` 在条目**交付**时就把 `applied` 推到交付点（`commit_since_index`），异步 apply 下「交付」与「应用」本就分离；`applied_to` 又禁止回退。改成**完全不碰**，凡需要「SM 真正应用到哪」的地方（快照 index、读可见性、提案回复）一律用 apply 任务回报的 `applied_index`。rev N 该行已按实测改写。
2. **`client_redirect` 的即时可见断言过强**：`follower_handle_redirects_to_leader` 原本在 leader ack 后立刻断言**每个节点**的 `get_stale` 都能看到值。这在同步 apply 时代也是靠时序侥幸（follower 要靠 leader 的下一条消息才知道 commit），apply 异步化后窗口变大；改为有界轮询（弱读本可陈旧，N1）。**注意**：这不是把 bug 测过去——该测试的语义是「写最终落到每个节点」，而「弱读立即可见」从来不是承诺。

**延迟冒烟门槛（`arachne/tests/read_latency.rs`，随 `cargo test --workspace` 进 CI）**：3 节点真实 runtime，120 次 `get` 采样；同一轮内测三个量——写 p99、弱读 p99、线性一致读 p99，断言「弱读 ≤ 4×写」「线性读 ≤ 3×弱读」+ 2s 绝对上限。**为什么不是绝对 p99 预算**（这是本轮最有价值的实测结论）：`FsyncPolicy::Always` 下写路径每次全量落盘在本机约 **10ms**（macOS `sync_all` 是整设备刷新），洪峰中 actor 的 `drive_cycle` 就是 11–12ms；实测 idle 0.5ms、洪峰**写 106ms / 弱读 99ms / 线性读 100ms 三者齐平**——瓶颈是写路径的落盘序列化点，apply 任务没有额外排队（这正是 ⑤ 要证的东西）。绝对阈值只会测出磁盘快慢；「空载 vs 洪峰」比值同样由落盘决定。三量互相约束才在测「apply 没加队列」。稳定性：连跑 3 次比值 0.5–0.9× 与 1.2–1.6×，余量充足。

**如实声明（接线到此为止的部分）**：阻塞式存储 I/O（fsync）仍在 async worker 线程上同步执行，是洪峰中一切客户端延迟的共同上界；把 WAL 落盘挪到专用阻塞线程/`spawn_blocking`（或异步 I/O 后端）是**下一步**的独立增量，本轮未做也未宣称。另：本轮未改 `BatchMs` 语义（其注释仍写 "advisory until P4"）。

### 1.11 异步持久化流水线（propsol v0.2.13 P）——做完了，但**没有降低延迟**（重要结论）

动机：rev O 之后洪峰读 p99 仍 38–53ms，因为 fsync 是 actor 线程上的同步等待。于是把"写"留在 actor、"刷"交给 worker（dup fd，`fsync` 刷 inode），用 raft-rs 自带的异步 Ready 协议（`Ready::number` + `advance_append_async` + `on_persist_ready`），消息/apply 全部推迟到 flush 完成之后（I2/I4 语义不变）。

**落地**：`Segment::try_clone_file` + `WalStorage::flush_handle/append_buffered/set_hard_state_buffered/note_flushed`（P1/P2，各带单测）→ `RaftNode::step` 拆 submit/finish 两相 + `MAX_IN_FLIGHT_READIES` 窗口 + 每节点一个 flusher 线程与 `Arc<Notify>` 唤醒（P3）→ runtime 增 `Outcome::Durability` 分支（P4）→ 端到端场景跑在 pipeline 上（P5）。**默认关闭**（`enable_offloaded_durability()` 显式开启），默认路径逐字节不变：这是能一次跑绿全部既有测试（含 INV1 台账对账、INV2 崩溃扫描、`l2_scenarios` 双跑确定性、`force_recovery`）的原因——`StepOutcome` 形状一行没动。

**实测（`read_latency`，p99）：同步 38–53ms vs pipeline 47–55ms（in-flight 8）/ 50（2）/ 52（1）；写 38–53 vs 49–64；弱读 0.17ms vs 0.14–8.5ms。** 结论：`Always` 下每周期一次真实设备 flush，**设备就是瓶颈**，换线程只改变"谁在等"；窗口越大客户端操作排得越靠后，反而更差。要降 p99 只能减少 flush 次数（rev O 的批周期已做）。所以把 pipeline 的定位改成**可用性**：磁盘慢时 actor 仍能 tick/心跳/服务（对应 `slow_fsync` 这类 L4 场景），并据此保持默认关闭、不切 `read_latency` 门槛、不在 node binary 里默认打开。

**顺带修掉一个实现陷阱**：不要在 async 路径里显式写"commit 前进"的 HardState——raft 的 `prev_hs` 不会被 async advance 更新，下一次 `ready()` 自带该 hs；显式再写一次会在盘上留下**重复 HardState 记录**，并打破"末条撕裂=合法截断"的判定（`m2_wal_faults::torn_tail_recovers_and_still_serves_the_acked_write` 抓到，报 `structural tear at estimated index 3 (within committed window commit=2)`）。删掉后每周期恰好一次 flush，状态机也简化成单阶段。

**新增覆盖**：`m2_snapshot.rs` 的追赶场景参数化后跑两遍——`lagging_follower_catches_up_through_a_snapshot`（默认同步）与 `lagging_follower_catches_up_with_offloaded_durability`（pipeline：写洪峰 → 快照 → follower 安装 → 重启 → 换主，全绿）。

### 1.12 已结案：空闲 `get` = 一个 heartbeat interval —— 是**测试传输的 waker bug**，不是产品退化

**现象**：空闲 3 节点内存集群上线性一致 `get` 几乎每次都等于一个 heartbeat interval（interval=10ms → 9.2–10.7ms；改成 50ms → 44–50ms，严格线性缩放），而弱读 ~0.2ms、写 1–2ms。加时间戳后确认 10ms **全部在 ReadIndex 的 quorum 阶段**（`quorum=9.27ms`、`apply=166ns`、`read_index == applied`）。

**二分定位**（用"p99 是 0.4ms 还是 10ms"当判据，每次 checkout + 重建）：

| 提交 | idle p99 |
|---|---|
| `cb4a704`（apply 独立任务 + 门槛） | **42µs** |
| `88ab876`（修忙循环） | **10.8ms** ← 退化出现在这里 |
| `09aa51d` / `2ed0fbc` / HEAD | ~11ms（继承） |

**根因**：`arachne-testsupport` 的内存传输用 **`std::sync::mpsc`**，它没有 waker 注册能力；手写的 `RecvFuture` 只把 `cx.waker().clone()` 存进自己的字段、**从未向通道订阅**。于是 actor 停在该 future 上时只能被**自己的 tick** 唤醒 → 每条入站 raft 消息最多等一个 heartbeat。`88ab876` 之前，"每轮发空批次 + 每次都 publish 进度"的忙循环一直在替它兜底（actor 始终醒着），所以读数很快；忙循环一修，问题立刻显形——这也解释了早先观察到的"每次写 ~22ms"（leader→follower、follower→leader 两次入站各等一个 tick）。

**修复**（`arachne-testsupport`）：内存 switch 换成 `tokio::sync::mpsc::unbounded`（`send` 仍是同步的，`UnboundedReceiver` 正常注册 waker），删掉手写 future。testsupport 只新增 tokio 的 `sync` feature，`check-deps.sh` Gate C 仍全过（未引入 tonic）。

**效果**：空闲读 p99 **10.8ms → 230–660µs**（比最早的 372µs 还好）；`read_latency` 整体 **4.2s → 2.2s**；workspace **374 passed** 全绿。

**新增守卫**：`read_latency` 增加 **idle 门槛**（idle p99 ≤ 5ms）。这条守卫当天就抓到了第二个真问题：pipeline 下"空记录周期"会搭上更早周期的 flush（idle 6.7ms）——已在 `WalStorage::persist_ready_records` 修掉（只有本次真的写了记录才提交 flush；FIFO 已保证更早周期的负载在它被报告前已持久）。

**顺带把延迟门槛改成生产放置**：`read_latency` 原先在 tokio runtime 上 `tokio::spawn(runtime.run())`，于是 4 个 worker 被 3 个 actor 的同步 `fsync`（本机每次 ~10ms）占住——**弱读 p99 因此在洪峰下是 9–14ms，测的其实是 worker 饥饿**。改用 `Runtime::spawn_dedicated()`（生产放置）并让清理只 drop 句柄 + 短睡（不 join，避免某个 `Handle` 克隆让 join 挂死）后：弱读 **225–438µs**、洪峰读 **35–36ms**（更稳）、写 37–47ms、idle 222–322µs，测试耗时不变。

**对既有结论的反向影响**：此前所有基于内存传输的延迟/吞吐数字都被这个 tick 放大了，rev O / rev P 的数字应在修好的 harness 上重读。rev O 的批周期结论不变；rev P 的"设备是瓶颈、pipeline 不降 p99"在重测后也不变（修好 harness 后：同步 storm 33–70ms vs pipeline 50–56ms），但 pipeline 下"空闲读可能等一次在途 flush"这条已被记录并缓解。

### 1.13 `wal_trailing_keep` 接线（propsol v0.2.14 Q）——慢 follower 由日志追赶

rev M 留下的"字节级 trailing 窗口"本轮接上，语义按 **Q 节**钉死：窗口**压住压缩水位**，而不是保留水位之下那些 raft 已经拿不到的字节。

- `WalStorage::set_trailing_keep_bytes(bytes)`（inherent setter，默认 0 = 旧行为）+ `trailing_keep_bytes()`；`compact(to)` 只从最旧段删，且一旦"删后仍可达的日志 < 窗口"就停，`first_index` 因此停住。窗口 = 0 时走原来的 `delete_segments_below` 路径，既有测试逐字节不变。
- node binary 从 `profile.wal_trailing_keep_bytes`（Lan 64MB / Wan 16MB）设置。
- **顺带给 `RuntimeThread` 补了真正的停止**：原先 drop 只是 detach，actor 仍持有 WAL 与其目录锁（端到端测试里"停一个节点再重启"因此报 `data directory is locked`）。新增 `shutdown()`/`stop()`（`Notify` + `notify_one`，注意 `notify_waiters` 会丢掉"先发后等"的信号——第一版就踩了这个，shutdown 直接挂住）。
- 单测 2 项（窗口压住水位且保留 ≥ 窗口；窗口 0 与旧行为一致）+ 端到端 `m2_trailing_keep`：在**全员在册**时写够让 leader 真的删掉最旧段（断言最旧段文件名 > 1），再让一个 follower 掉线并只写**少于窗口**的量，重启后必须由日志补上（`snapshots_installed_total == 0`）；**反向对照**：窗口设 0 时同场景失败（`installed == 1`），证明测试非空洞。
- 这轮全量：workspace **377 passed / 0 failed**，l2 全绿，三个门禁 PASS。

### 1.14 收尾：快照预算告警、CI 单飞项目腐化、fuzz 任务

- **`slow_fsync` 的"可用性"论断被证实**（这是 rev P 里我明确标注"未验证"的那条）：给 WAL 加了一个 **feature `fault-injection` 下的测试专用 flush 延迟**（`set_flush_delay_ms`，同时作用于 actor 的同步 `fsync` 与 flusher 线程的 flush），新增 `arachne/tests/slow_fsync.rs` 做 A/B：单节点、400ms 模拟慢盘、写正在落盘时连打弱读——**同步路径最坏 360ms（读要等 actor 的 fsync），pipeline 127µs**，约 2800× 差距。这正是 pipeline 存在的理由（它不降 p99，但保证磁盘慢时 actor 不停摆）。CI 的 fault-injection 步骤已同时跑 `m2_durability` 与 `slow_fsync`；`check-release-features.sh` Gate C 增加第二个哨兵（`set_flush_delay_ms`）以免 release 混入该注入（第一版把检查插在了 feature 构建之后、grep 的是 feature rlib，等于白查——已修）。
- **新增第四道门禁 `scripts/check-profile-knobs.sh`**：把"profile 里的旋钮必须被生产代码读取"变成 CI 断言——`wal_trailing_keep` 与 `proposal_queue_bytes` 都曾在 profile 里躺了整个里程碑而无人读。当前 4 个已知缺口带理由列入白名单（`session_ttl_ms`/`session_grace_period_ms`/`max_sessions` → M3 会话；`snapshot_transfer_rate_bps` → 快照流式传输），且**一旦某个白名单项开始被使用就会失败**，强制删掉过期豁免。双向反向对照都验过（去掉豁免 → 报未接线；给已用的旋钮加豁免 → 报过期豁免）。
- **Q4 快照预算告警落地**（propsol §8.2）：`Runtime::poll_snapshot` 在测完时长后，`> 1s` 即 `slog::warn!`（带 duration/index/size/budget）+ `snapshot_slow_total` 计数器（新增指标，已在 `/metrics` 渲染）。此前只有 `snapshot_last_duration_ms` 这个 gauge，没有任何告警路径。测试：指标渲染/计数单测 + `m2_snapshot` 里"正常快照**不**触发告警"的负向断言（避免误报）。正路径要造 >1s 的快照得靠 L4 慢盘，未做，如实记录。
- **发现并修掉一类系统性腐化：单飞（非 workspace）项目没人建**。追查失败的定时 CI 时发现：
  1. **fuzz 任务从未装 protoc 3.x**（另两个 job 都装了）→ `raft-proto` 的 build script 直接 panic（`Option::unwrap() on None`），**在构建阶段就死**，压根没跑到 fuzzer；所以定时任务一直红在 job 设置上。
  2. **fuzz target 本身编译不过**：它用结构体字面量造 `Meta`，而 v0.2.10 M 给 `Meta` 加了 `snapshot_index`/`snapshot_term` → 漏改。已补两个 0 字段。
  3. **`model-check/Cargo.lock` 缺 5 个包**，`cargo build --locked` 直接拒绝。
- **防复发**：PR 门禁新增一步 **Build standalone projects (fuzz, model-check)**，两者都用 `--locked` 构建——这类漂移以后在 PR 上就红，而不是等到夜间定时任务。验证：手动 `gh workflow run ci.yml` 触发全量，三个 job（Build & test / **Fuzz (wal_recovery)** / L2）**全绿**。
- 本轮全量：workspace **377 passed / 0 failed**，三个门禁 PASS。

### 1.15 下一块：M3 会话幂等收尾（propsol v0.2.15 R，设计已钉，待实现）

本轮把设计做完了，代码留给下一轮（避免把里程碑级改动做成半成品）。**先记一个已存在的缺陷**：`KvStateMachine` 的会话表没有淘汰——每写一次多一条，长期运行无界增长；`session_ttl_ms`/`session_grace_period_ms`/`max_sessions` 从未被读（新门禁白名单里的三项）。去重与"快照带会话表"都是对的（M3 ⑥ ✓），缺的是失效/回答/满表三件事。

设计要点（详见 rev R）：过期判定放 **leader 侧**（本机时钟）而不是把时间戳写进每条命令；淘汰必须走**复制的显式 GC 条目**（否则副本去重状态分叉）；三区间语义 ≤TTL 正常去重 / TTL..TTL+grace 回 `SessionExpired` 且**不提议** / 之后可由 GC 删除，由此给出 INV5 要的"重复窗口 ≤ ttl+grace"；`max_sessions` 只能在**提议前**检查（已提交条目不能拒绝）；**TTL 必须走 `Clock` seam**（测试注入 `ManualClock`），代价是 `RuntimeConfig` 新增 clock 字段、约 10 处字面量构造跟着改。落地分 R1（时钟+本地表+两错误）/R2（GC 命令与提议循环）/R3（INV5 S07/S10/S14 + 删白名单三项）。

### 1.16 M3 会话 R1 落地：TTL/grace 三区间 + `SessionExpired`

- **时钟不改配置结构**：`Runtime::with_session_clock(clock)`（builder）。加 `RuntimeConfig` 字段要跟着改约 10 处字面量构造，而 builder 让"不设置 = 永不过期 = 旧行为"，零破坏。
- **leader 本地会话表**（`(client_id, seq_no) -> last_used`，不复制、不进快照）：换主后窗口从零重算 → 会话只会**活得更久**（保守方向，不会双重生效）。表按 `ttl+grace` 窗口定期清扫，所以有界（不像 SM 的表那样无界增长）。
- **三区间**：≤TTL → 正常提议（SM 去重）；TTL..TTL+grace → **`SessionExpired` 且不提议**（这正是 INV5"重复窗口 ≤ ttl+grace"的界）；之后 → 当新会话，仍安全因为 SM 还留着 outcome（GC 在 R2 才删）。
- `last_used` 只在条目**交给 apply 任务时**更新——提议失败（丢主）不能延长会话，否则会给"根本没发生"的操作回 `SessionExpired`。
- 测试需要"同 session 重试"，而 `put/delete` 每次自增 seq，因此新增 **feature 门控的 `Handle::propose_raw`**（与既有 test-only 注入同一模式）。端到端验证四条：TTL 内重试用**不同值** → 原值存活（恰好一次）；越 TTL 未过 grace → `SessionExpired` **且 `applied_index`/`commit_index` 不变**（没进日志）；越过 grace → 重新接受且效果仍一次。**反向对照**：`session_ttl_ms = 0` 时该断言失败。
- 门禁：`session_ttl_ms`/`session_grace_period_ms` 已从 `check-profile-knobs.sh` 白名单移除（现在真被读），白名单只剩 `max_sessions`（R2，必须与 GC 同批）与 `snapshot_transfer_rate_bps`（流式传输）。CI 的 fault-injection 步骤加了 `--test sessions`。
- 本轮全量：workspace **377 passed**、fault-injection **214 passed**、l2 全绿、四道门禁 PASS。

### 1.17 M3 会话 R2 落地：GC 命令 + `max_sessions`（必须同批）

- **SM 侧**：新增 `OP_SESSION_GC`（`[op:1][count:u32][(client_id,seq_no)×count]`）——**显式列表**而非 cutoff 时间戳，因为副本必须删掉完全相同的集合，而时间戳不可重放。`apply` 只删列出的会话、不碰 KV；`command_session` 对 GC 返回 `None`（不归属提案、不延长会话）；畸形 GC 整条拒绝、不部分应用。新增 `session_count()`/`encode_session_gc`。
- **runtime 侧**：`ApplyProgress` 增 `sessions`（apply 任务发布 `sm.session_count()`）→ actor 存 `live_sessions`；leader 每 `ttl` 检查、把超过 `ttl+grace` 未活动的会话按 **1000/条**上限提议为 GC，且**只在确有过期项时**才提议（空闲不产生日志）；提议成功后从本地表删除，避免重复提议。
- **`max_sessions` 与 GC 同批**（这是刻意的顺序）：只有 **Fresh（新）会话**会在提议前被拒 `SessionTableFull`，且依据**复制的**表大小；已有会话的重试永不被容量拒绝。若先上 cap 而没有 GC，填满后新会话会被**永久**拒绝 ✗。换主后本地表为空，极端情况（表满+换主）可能误拒一次合法重试，可重试恢复——已记录。
- 指标新增 `arachne_session_count`（propsol §8 一直要求这个 metric）。
- 测试：GC 单测（只删列出的、KV 不变、畸形整条拒绝）+ 端到端 `session_gc_prunes_and_relieves_the_session_cap`（4 会话填满 → 第 5 个 `SessionTableFull` → 时钟越 `ttl+grace` → GC 后计数归 0 → 新会话被接受 → KV 未被触碰）。**反向对照**：`session_ttl_ms = 0` → 两个会话测试全失败。
- 门禁联动：`check-profile-knobs.sh` 白名单**只剩 `snapshot_transfer_rate_bps`**（TTL/grace/max_sessions 三项都已真读）。
- **如实记录一次观察到的 flake**：某次 `cargo test --workspace` 输出被工具截断，可见尾部出现 2 个失败；随后**连续 3 次全量 378 passed / 0 failed**，未能定位那 2 个（可信度受限：输出被截断）。R2 对非 feature 路径零开销（无 clock 时 `maybe_collect_sessions` 立即返回），因此与被测路径无关；下次若再现，应先关掉并行构建负载再跑，以排除机器争用。

### (D) M1-5：L3 冒烟 + bin CLI 集成测试（M1 验收 ①②③）— **已收尾（commit `adb2bd7`，见 §1.5）**
- ✅ 3 进程成形/写读/复制冒烟 + **杀 leader 换主 + 窗口内写不挂死**（`arachne-node/tests/multi_node.rs`，2 项）。
- ✅ **跨进程 `409`+hint**：`Handle::without_redirect()` + node HTTP 单发 handle。跨进程**自动跟随** hint 仍需 HTTP-port 映射（config 暂无），M1 只做「409+hint body」，与 propsol §3.3 一致。
- ✅ 失联旧 leader 的 propose 超时 `ArachneError::Timeout` 现映射 **503**（原 500）。
- ✅ bin CLI 集成测试（§3.3）：`arachne-node/tests/cli.rs`（11 项）+ `tests/common/mod.rs`。
- ✅ **`force-recovery` 子命令已实现**（存储层原语 + CLI + 端到端测试，见 §1.5.6）。**唯一 M1 边界**：membership 到 M3 才持久化 `ConfState`，故单票 membership 由命令生成的 `initial_cluster=[self]` config 提供（M3 后去掉该依赖）。

### M1 验收对照（propsol §10）
① 杀 leader 后 ≤2×election_timeout 出新主 —— **已证**：L2 确定性 tick 计数（≤2×election_tick，实测 16/20）+ L3 进程 SIGKILL 换主。
② 期间写返回 `NotLeader`/`QuorumUnavailable` 不挂死 —— **已证**：L3 窗口内写只接受 409/503，挂死即判失败；客户端面 `8cbc765`；`Timeout` 现映射 503。
③ hint 失效经 seeds 轮询恢复 —— **契约已打通**：跨进程 `409`+hint 可达（`without_redirect`）；`Handle` 的 seeds 兜底已实现（`client_runtime`/`read_index` 覆盖）。
④ 含切主窗口的 put/get 线性一致 —— **已在 L2（内存传输）证明**：stage 3c inc.4–10（分区/换主/并发/多键/ReadIndex 读/故障下 ReadIndex 读/换主后同 seq 重试 at-most-once 共 9 种形态）；**real-tonic-on-turmoil（stage 3b）亦已打通**（3 节点真实 tonic 集群选主/提交/复制 + 双跑确定性 + 传输延迟门禁，见 §1.6）。
⑤ crate 文档首页语义表/错误矩阵 —— **已完成**（`151066a`，并在 (B) 校正 `get` 行为）。

## 4. 环境注意（踩过的坑）

- **protoc 3.x**：`raft-proto` 的 `protobuf-build` 只接受 `protoc` 3.x；本机系统 protoc 是 25.3。`.cargo/config.toml` 设 `PROTOC = { value = "scripts/find-protoc.sh", relative = true }`；该脚本搜 `PATH`/常见目录/`PROTOC_FALLBACK`/PyTorch 内置 3.x。CI 已加 pinned protoc 3.20.3 安装步骤。**若构建报 `raft-proto` panic `Option::unwrap() on None` + `find-protoc:` 提示，就是没找到 3.x protoc**。
- **子代理稳定性**：本会话 subagent provider 约半数失败（`all candidate providers failed`），重试可恢复；失败时可能留下**未接线的部分文件**，接手前先 `git status`。
- **`NodeError<TonicTransport>` 未实现 `Debug`**（`TonicTransport` 未派生）→ 用 `.expect()` 会编译失败，改用 `match`/Display。可考虑给 `TonicTransport` 派生 Debug。
- **`TonicTransportFactory::start()` 绑定地址表全部条目**（单进程测试可用）；多进程用 `start_with_bind(me, bind)`（已实现，只绑定自身）。
- **Gate 误报**：`check-entropy.sh` 用纯文本 grep，**注释里字面写** `tokio::select!`/`std::time` 会被误判——注释请改述（如"biased select 事件循环"）。测试内用 `core::time::Duration`。
- **默认 profile**：`lan`（heartbeat 100ms / election 1s）。测试里可逐字段覆盖以加速。

## 5. 验证命令
```
cargo build --workspace
cargo test --workspace
cargo build --examples
cargo build -p arachne --no-default-features
bash scripts/check-deps.sh
bash scripts/check-entropy.sh
bash scripts/check-release-features.sh          # D-ART-testobs（release 产物排除 test-observability）
# 3 节点真实网络（库级）：
cargo test -p arachne-transport-tonic --test three_node -- --nocapture
cargo test -p arachne-transport-tonic --test three_node_client -- --nocapture
# 3 进程真实集群（bin 级，L3 冒烟 + 杀 leader）：
cargo test -p arachne-node --test multi_node -- --nocapture
cargo test -p arachne-node --test cli -- --nocapture
cargo test -p arachne-transport-tonic --test multi_node_bind -- --nocapture
# ReadIndex 读路径（进程内 3 节点）：
cargo test -p arachne --test read_index -- --nocapture
# 客户端重定向契约（普通 handle + 单发 handle）：
cargo test -p arachne --test client_redirect -- --nocapture
# 持久化 commit 回归（真实 Runtime actor + force-recovery）：
cargo test -p arachne --test force_recovery -- --nocapture
# L4 真机 kill -9（每轮一次 acked 写入 + 崩溃后复验；需真实磁盘）：
ARACHNE_L4_ITERATIONS=3 bash l4/kill_loop.sh
# L2 独立工程（turmoil；真实 tonic 集群 + 传输延迟门禁，CI 有独立 job）：
cd l2 && cargo test
# 复现「默认链路延迟导致停机」的对照（预期 FAIL）：
cd l2 && ECHO_LINK_LATENCY_US=0 cargo test --test transport_echo -- --nocapture
```
**本地注意**：若 `CARGO_TARGET_DIR` 在仓库外（本机默认 `~/.cargo/global-target`），沙箱可能拒绝写入；测试时用仓库内目录覆盖，如 `CARGO_TARGET_DIR=.dsh-target cargo test --workspace`。

## 6. 协作约定（沿用）
- 生产代码禁 `unwrap`/`expect`/`panic!`/`unsafe`；TDD；改后必编译+测试。
- 设计变更须按 propsol 决策记录格式追加 rev 条目（当前最新 v0.2.8）。
- 测试基建/工件边界见 test-plan §3.2/§3.3；改动边界须过 `check-deps.sh`/`check-entropy.sh`。
- 每阶段一个 @oracle 门禁（除非用户另有指示）。
