# Arachne L4 — 真机 `kill -9` 持久性门禁（`l4/`）

L4 是测试分层的**最底层、也是模拟无法替代的一层**（`test-plan-v0.1.md` §2 L4 行、§10、§12；上游 `propsol-v0.2.7.md` §9.4/§9.5、D-L4）。它在一台**带真实磁盘的机器**上，对一个**真实 `arachne-node` 进程**反复 `kill -9`，验证 **WAL 在真实 fsync 语义下的持久性**——因为模拟环境里的 fsync 是"撒谎的"（`test-plan` §11 缺口 1：这一项不可替代）。

## L4 证明什么

1. **进程级崩溃恢复**：节点被 `SIGKILL`（无优雅关闭、无 fsync 排空）后，在**同一 data dir** 上重启，必须能起来并重新就绪。
2. **WAL 打开干净（fail-start 即信号）**：重启时 `WalStorage::open` 回放 WAL——若 `kill -9` 造成落在**已提交区间**内的撕裂/静默损坏，节点会 `Unrecoverable` **fail-start**（在 `/readyz` 之前非零退出）。`kill_loop.sh` 把"重启后始终就绪"当作 WAL 完整性的证据；一旦节点起不来，脚本判 **FAIL**。
3. **持久进度不回退（INV12/INV13 真盘复验）**：每次重启后 `/metrics` 的 `arachne_commit_index` / `arachne_applied_index` 不得**低于**上次观测值。

> 模拟（L2）能证明崩溃/分区/磁盘故障下的**逻辑**不变量，但**无法**证明真实 fsync 语义下的持久性——那是 L4 的专属职责。

## 文件

| 文件 | 作用 |
|---|---|
| `node.toml` | `arachne-node` 的**模板**配置：temp-ish data dir、ephemeral 端口、短 heartbeat（快速自选举）。既可独立运行（`arachne-node --config l4/node.toml`），也是两个脚本读取稳定设置（cluster_id / node_id / initial_cluster / heartbeat / election）的来源 |
| `kill_loop.sh` | 主 harness：`set -euo pipefail`。起节点 → 等 `/readyz` → 循环 N 次「`kill -9` → 同 data dir 重启 → 校验 (a) 进程起来 (b) `/readyz` 就绪 (c) WAL 打开干净 + commit/applied 不回退」→ 打印 PASS/FAIL，失败即非零退出。清理 temp data dir |
| `verify_wal.sh` | 独立探针：给定一个 data dir，起节点重开它。WAL 完好 → 就绪（PASS）；WAL 损坏 → 节点 fail-start（非零退出）→ **FAIL**。用于离线变异（S04/S05/S06）后复验，或作为可复用工具 |

## 运行

```sh
# 本地（M0 脚手架，5 轮）：
bash l4/kill_loop.sh

# 覆盖节点二进制路径与迭代次数：
ARACHNE_NODE=./target/debug/arachne-node ARACHNE_L4_ITERATIONS=10 bash l4/kill_loop.sh

# 独立复验某个 data dir（健康 vs 损坏）：
bash l4/verify_wal.sh /path/to/data-dir            # 自动构建/查找 arachne-node
bash l4/verify_wal.sh /path/to/data-dir ./node-bin
```

`kill_loop.sh` 的环境变量：

| 变量 | 默认 | 说明 |
|---|---|---|
| `ARACHNE_NODE` | 自动（`cargo metadata` 定位 target dir，缺则 `cargo build -p arachne-node`） | 节点二进制路径 |
| `ARACHNE_L4_ITERATIONS` | `5` | `kill -9` / 重启循环次数 |
| `ARACHNE_L4_READY_TIMEOUT` | `30` | 每次重启等 `/readyz` 的秒数上限 |
| `ARACHNE_L4_CONFIG` | `l4/node.toml` | 模板配置路径 |
| `ARACHNE_L4_KEEP_ON_FAIL` | `1` | 失败时保留运行目录（含 data dir + 各次日志）以便排查 |

每次重启用**全新的空闲 loopback 端口**（`kill -9` 后重绑同一端口在 Linux 上可能撞 TIME_WAIT）；**data dir 全程不变**（这才是被测的持久性载体）。成功时清理 temp 运行目录；失败时默认保留并打印路径。

## CI 要求（决策 D-L4，已锁定）

- **专用真实磁盘 CI runner 现在即立项建设**（D-L4）："发版前手工执行"与"延后到 M2"两个备选已评审**否决**——fsync 真实语义的复验不可替代且必须可重复（`test-plan` §10、§12 M0/M4）。
- **M0**：本脚手架就位（runner 配置 + `verify_wal.sh` + `kill_loop.sh` 注入器），**本地可跑**（即本 README 的运行方式）。
- **M1**：专用 runner 起跑冒烟。
- **M4 / Release 门禁**：真机 `kill -9` 循环跑满预算（`test-plan` §10 Release 行：`kill -9` 循环 30min + WAL 校验脚本），作为发版前的硬门禁。

> 为什么必须是真盘：模拟 fsync 不落地（`turmoil` 的 `unstable-fs` 是记账而非真 fsync），INV2/INV6 的"已 ack 数据不丢"只有在**真实磁盘 + 真实 `kill -9`** 下才第一次被真实验证。自动化 runner 是唯一可重复、可归因的证明方式。

## 边界（如实声明）

- L4 **不覆盖运行中的在线腐蚀**（那是 L2 `FaultyStorage` 的职责，`test-plan` §3.3）；L4 的磁盘故障 = 真盘 `kill -9` + **离线变异**（停机 → WAL 位翻转/截断 → 重启，对应 S04/S05/S06，可用 `verify_wal.sh` 复验）。
- 真实磁盘坏扇区/静默腐蚀（企业级 scrub）超出本体系范围（`test-plan` §11 缺口 1）。
- 本目录（`l4/`）**不是**根 workspace 成员，`scripts/check-deps.sh` / `check-entropy.sh` 不扫描它；它只依赖已构建的 `arachne-node` 二进制 + `curl` + `python3`，**不新增任何 crate**。
