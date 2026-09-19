# Arachne 交接：M0 完成，M1 进行中

> 面向**新会话**。先读本文，再读 `dev-docs/propsol-v0.2.8`（设计权威）与 `dev-docs/test-plan-v0.1.3`（测试方案）。会话态进度见 `.slim/deepwork/arachne-m1.md`（git-local，但 OpenCode 可读）。
>
> 仓库：`/Users/alex/Projects/workspace/Arachne`，git 已 init，工作树干净，**224 tests 0 failed**、0 告警、`scripts/check-deps.sh` 与 `scripts/check-entropy.sh` 全绿。

## 1. 现状总览

- **M0 已完成**（含终检门禁，COMPLETE）。六工件工作区 + 崩溃安全 WAL + raft 集成 + KV/会话状态机 + 全套测试基建 + `arachne-node` 单节点可运行 + examples + L4 runner 脚手架 + stateright/turmoil 骨架 + spike 关闭。
- **M1 进行中**。已完成 M1-1、M1-2（含整改）与 M1-3a；**未完成 M1-3b / M1-4 / M1-5**。

### 提交线（新 → 旧）
```
8cbc765 M1-3a(3/3) follower 写重定向到 leader（真实 tonic 3 节点）   ← HEAD
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
- `arachne-node` 的 HTTP：`/readyz`、`/metrics`、`PUT /kv/<key>/<value>`、`GET /kv/<key>[?stale=1]`、`DELETE /kv/<key>`（NotLeader→409+leader hint、quorum→503、非法参数→400）。

## 3. M1 剩余工作（按推荐顺序）

### (A) arachne-node → tonic 多进程（M1-5/L3 前置）
当前 `arachne-node` 仍用**占位无 peer 传输**（单节点）。要做真实 3 进程集群：
1. 给 `TonicTransportFactory` 增 **只绑定自身**的启动路径：`start_with_bind(me: NodeId, bind: SocketAddr)`（现有 `start()` 会绑定地址表中**所有**条目——多进程会 EADDRINUSE）。参考 `three_node.rs` 与 `three_node_client.rs` 的用法。
2. `arachne-node` 配置加入各节点地址（建议 `initial_cluster = ["n1=127.0.0.1:7001", ...]`，self 用 `listen`）；`Config` 增 `addresses: HashMap<NodeId, SocketAddr>`。
3. `node.rs` 改用 tonic 传输 + `RuntimeConfig{ peers, addresses }` 组集群；`main.rs` 在关闭时调 `factory.shutdown()`。
4. 多进程 e2e：3 个 `arachne-node` 进程，各自配置；经 HTTP 写/读验证（L3 冒烟）。

### (B) M1-3b：ReadIndex 线性一致读
`Handle::get` 目前是**临时 leader-local 读**。需在 `RaftNode` 实现 `read_index` 轮询 + `ready().read_states` 处理（等待 `applied ≥ index`，超时 `2×election_timeout`、重试 1 次），actor 提供 `Command::Read`。`read_index_timeout_ms` 已在 `ProfileConfig`。

### (C) M1-4：L2 turmoil + 不变量 + 线性化（M1 验收 ④）
- `l2/` 铺满 SimNetwork 原语（partition/hold/release/crash/bounce）。
- `ClientOracle` v1（逐 seq、幻值、one-log-id-per-seq、read-your-writes、单调读、持久性扫描）+ 自建 Wing–Gong 检查器（缩减历史）；stateright 作交叉验证（`model-check/`）。
- INV3/4/7/8/9 + 场景 S01/S02/S16。
- **D-S1**：raft 0.7 选举 RNG 不可注入（`raft-0.7.0/src/raft.rs` 的 `reset_randomized_election_timeout` 用 `thread_rng`）→ 视需要打 workspace `[patch.crates-io]` 一行补丁，以上游化为目标。

### (D) M1-5：L3 冒烟 + bin CLI 集成测试（M1 验收 ①）
- `arachne-node` ×3 真实进程（依赖 (A)）；杀 leader ≤2×election_timeout 出新主；期间写返回 `NotLeader`/`QuorumUnavailable` 而非挂死（②）。
- bin CLI 集成测试（§3.3）：配置校验/flock/META 不一致 fail-start/优雅关闭/`/readyz`/`/metrics`。

### M1 验收对照（propsol §10）
① 杀 leader ≤2×election_timeout 出新主（→ M1-5）② 期间写返回 `NotLeader`/`QuorumUnavailable` 不挂死（**客户端面已在 `8cbc765` 证明**；多进程面待 (A)/(D)）③ hint 失效经 seeds 轮询恢复（Handle 已实现 seeds 兜底）④ 含切主窗口的 put/get 线性一致（→ M1-3b + M1-4）⑤ crate 文档首页语义表/错误矩阵（**已完成** `151066a`）。

## 4. 环境注意（踩过的坑）

- **protoc 3.x**：`raft-proto` 的 `protobuf-build` 只接受 `protoc` 3.x；本机系统 protoc 是 25.3。`.cargo/config.toml` 设 `PROTOC = { value = "scripts/find-protoc.sh", relative = true }`；该脚本搜 `PATH`/常见目录/`PROTOC_FALLBACK`/PyTorch 内置 3.x。CI 已加 pinned protoc 3.20.3 安装步骤。**若构建报 `raft-proto` panic `Option::unwrap() on None` + `find-protoc:` 提示，就是没找到 3.x protoc**。
- **子代理稳定性**：本会话 subagent provider 约半数失败（`all candidate providers failed`），重试可恢复；失败时可能留下**未接线的部分文件**，接手前先 `git status`。
- **`NodeError<TonicTransport>` 未实现 `Debug`**（`TonicTransport` 未派生）→ 用 `.expect()` 会编译失败，改用 `match`/Display。可考虑给 `TonicTransport` 派生 Debug。
- **`TonicTransportFactory::start()` 绑定地址表全部条目**（单进程测试可用；多进程需 (A)(1) 的 `start_with_bind`）。
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
# 3 节点真实网络（库级）：
cargo test -p arachne-transport-tonic --test three_node -- --nocapture
cargo test -p arachne-transport-tonic --test three_node_client -- --nocapture
```

## 6. 协作约定（沿用）
- 生产代码禁 `unwrap`/`expect`/`panic!`/`unsafe`；TDD；改后必编译+测试。
- 设计变更须按 propsol 决策记录格式追加 rev 条目（当前最新 v0.2.8）。
- 测试基建/工件边界见 test-plan §3.2/§3.3；改动边界须过 `check-deps.sh`/`check-entropy.sh`。
- 每阶段一个 @oracle 门禁（除非用户另有指示）。
