# Arachne L2 — deterministic-simulator harness（`turmoil`）

本目录是 **L2 单进程多节点确定性模拟** 的 harness 载体（决策 **D-T1**：`turmoil 0.7.2` 锁定为 L2 主选；`test-plan-v0.1.md` §4 T1 / §3.2、上游 `propsol-v0.2.7.md` D-T1）。

L2 的目标（`test-plan` §1/§2）：在 **真实 tokio（以及 M1 起的真实 tonic）** 代码路径上，跑在 `turmoil` 的模拟网络/时钟之上，注入 **崩溃 / 分区 / 磁盘故障 / 消息异常**，以 **不变量断言 + 客户端预言机** 自动判定对错，并且 **同种子可复现**。

## 为什么是独立项目（不在根 workspace 里）

`l2/` 是一个 **独立的、非 workspace 成员** 的 cargo 项目（`Cargo.toml` 末尾的 `[workspace]` 表使其自成 workspace 根，与 `fuzz/`、`model-check/` 同一模式）。原因（决策 **D-ART** / `test-plan` §3.2）：

- `turmoil` 及其整棵依赖树是 **dev-only 工具**。把它们放在生产 workspace 之外，保证模拟器 **绝不** 泄漏进任何生产 crate 的正常依赖树（`scripts/check-deps.sh` 只扫描 workspace 成员，天然看不到 `l2/`）。
- 产品核心经 path 依赖引入，且一律 **`default-features = false`**（精简核心，不拉 tonic）——与生产 sim 构建规则一致（D-ART-rev1 / `test-plan` §3.2）。

> 依赖：`turmoil = "0.7"`、`tokio = "1"`、`arachne`（path，`default-features = false`）、`arachne-seam`（path）。

## 现在能做什么（M0 骨架）

`cargo run`（或 `cargo build && cargo run`）运行一个 **最小但真实** 的 `turmoil::Sim` 场景，验证 harness 的三种原语形状：

1. **hosts** —— 一个 TCP echo 服务（host `a`）与一个客户端（host `b*`），走 `turmoil::net` 的模拟 TCP。
2. **partitions** —— 断链（`turmoil::partition`）后连接必须失败，修复（`turmoil::repair`）后连接成功。
3. **crash / restart** —— `Sim::crash` 杀掉服务节点，`Sim::bounce` 重启（服务重新 bind/accept 并恢复服务）。

场景在 **固定种子** 下运行，并 **断言确定性**：同种子连跑两次，可观察结果（三次 echo 载荷 + 模拟耗时）必须逐字节一致。这是 L2 双跑复现门禁（`test-plan` §5）的 **金丝雀**——任何差异即确定性泄漏（P0，禁止白名单）。

```sh
cd l2
cargo build
cargo run
# => arachne-l2 skeleton OK: seed 0x005eed42 -> 3 echoes, elapsed 188ms (deterministic across two same-seed runs)
#    linked product core: arachne 0.1.0
```

不同种子产生不同模拟耗时（0x5EED_43→384ms、0xDEAD_BEEF→127ms…），证明耗时信号确由种子化调度驱动，而非常量。

**注意**：`main` 是普通 `fn main`，**不是** `#[tokio::main]`——`turmoil` 自带单线程模拟运行时，在内部驱动所有 host/client future（与 `fuzz/` 禁 `#[tokio::main]` 同理）。

## 路线图（骨架 → 完整 L2）

| 里程碑 | 交付 | 说明 |
|---|---|---|
| **M1** | 真实 tonic 接线 + SimNetwork 全原语 + ClientOracle v1 | 让 `turmoil` 承载 **真实 tonic**（`turmoil::net::TcpListener` + `serve_with_incoming` + 自定义 connector/`Connected` 适配），把真实 `arachne` 节点（多实例）跑在模拟网络上；`SimNetwork` 补齐 `partition_oneway`/`hold`/`release`/`set_fail_rate`/链路延迟抖动（`test-plan` §6.1）；ClientOracle 做 G1/G2 判定（§6.4）。**此时才把 `arachne` 的 `transport-tonic` 显式启用**（D-ART-rev1：sim 的 tonic 仅在 L2 落地时显式开） |
| **M2** | FaultyStorage + INV1–INV6 | `FaultyStorage` 包装自有 `WalStorage`/`StateMachine` 缝，注入 fsync 失败/撕裂写/截断/位翻转/慢盘，并维护 **fsync 台账** 事后对账断言 I1–I4（§6.2）；INV1–INV6 逐条可执行断言（§7） |
| **M2+** | 双跑复现门禁 + 线性化检查 | 每个场景同种子连跑两次，比对 ①故障调度序列 ②各节点 apply 序列哈希 ③预言机判定（§5）；自建 Wing–Gong 检查器 + stateright 交叉验证（T2，D-T2） |
| **M3/M4** | 全场景矩阵 + 两阶段模糊器 | S01–S20 场景矩阵（§8）、safe/liveness 两阶段模型、`--reproduce` 重放（§9） |

**L2 不覆盖**（如实声明，`test-plan` §11 缺口）：真实磁盘 fsync 语义（那是 L4 的不可替代项，§2 L4 行）、真实网卡/内核栈行为（L3/L4 补位）。

## 平台注记（macOS aarch64）

`fuzz/` 的 `--sanitizer none` 注记是针对 **cargo-fuzz + ASan** 在 macOS aarch64 上 dyld 初始化挂起的问题（见 `fuzz/README.md`）。**本 L2 harness 经 `cargo run` 直接运行，不挂 ASan/UBSan，不受该问题影响**——本地 `cargo build && cargo run` 即可。

仅当 L2 未来把某个 target 接到 sanitizer / 调试器下（例如把 L2 场景喂给 fuzzer 做随机种子探索）时，才可能触及同一类 macOS dyld 交互；届时参照 `fuzz/README.md` 的 `--sanitizer none` 处理。CI 的 L2 门禁跑在 Linux（`ubuntu-latest`），无此限制。

## 运行

```sh
cd l2
cargo build     # 构建（独立 workspace，有自己的 Cargo.lock）
cargo run       # 运行确定性金丝雀（固定种子 0x5EED_42）
```

`l2/` 有独立的 `Cargo.lock`，与根 workspace 互不影响；它 **不是** 根 workspace 成员，故 `cargo build --workspace`（根）不会构建它，`scripts/check-deps.sh` / `check-entropy.sh` 也不会扫描它。
