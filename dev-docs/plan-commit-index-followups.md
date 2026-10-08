# 开发计划：commit-index 增量（v0.3.0 发布之后）

> 状态：**定稿**（经 @oracle 回归评审，2026-10-08）。
> 范围：v0.3.0 发布后遗留的欠账/延后项，按优先级排期。
> 关联：设计文档 `dev-docs/arachne-kv-commit-index-design.md`（§7/§8/§9 的承诺即本清单来源）；需求 `dev-docs/arachne-kv-commit-index-request.md`。
> 现状基线：main 与 `v0.3.0` tag 已推；lean 全套件 + 发布门禁 PASS。

---

## 0. 优先级裁决（@oracle 回归评审，已核对 ci.yml / 独立 lockfile / 测试落点）

| # | 项 | 优先级 | 理由 | 依赖 | 估量 |
|---|---|---|---|---|---|
| 1 | fuzz / model-check / l2 独立 lockfile resync | **P0** | `ci.yml` "Build standalone projects" 对 fuzz、model-check 跑 `--locked`；lock 记 `arachne-kv 0.1.2` 而路径 crate 已 0.3.0 → `--locked` exit 101，**main 的 PR 门禁当前必红** | 无 | S |
| 2 | propsol 决议条目（Q5 延展） | P1 | 兑现设计 §7 的 traceability；零风险 | 无 | S |
| 3 | test-plan-v0.1.md 对齐（INV/S02/S16 补 index 口径） | P1 | I1–I6/I2 测试已存在，缺口是文档断言口径 | 承 2（可并行） | S–M |
| 4 | 运维文档 + 破坏性升级 runbook | P1 | 设计 §5.4 明写「此条写入运维文档」但缺失；FORMAT_VERSION 1→2 是面向交付物的破坏性变更，对外后果最重 | 无 | S–M |
| 5 | node HTTP 暴露/冒烟 `get_stale_with_index` | P2 | §8 承诺，改动 S；B 纯本地读、无实际消费方 → 完整性而非风险 | — | S |
| 6 | fuzz：kv snapshot `restore`/`decode_store` target | P2 | 新格式（payload 版本字节 + 8B index）确为 panic 面，现 `fuzz_targets/` 仅 `wal_recovery` | **1**；改 `ci.yml` nightly fuzz job 目标名 | M |
| 7 | l2 确定性 lane（`l2/tests/in_sim.rs`，turmoil）+ CI `--locked` | P2 | 实质确定性套件在此（≥core lean 已测的 l2_scenarios）；补 `--locked` 堵 drift | **1** | S–M |
| 8 | model-check 跑通 | P3 | `model-check/src/main.rs` 系 M0 scaffold，跑通**不覆盖新编码**，勿当格式验证 | — | S |
| 9 | `put_with_index`（Option A） | P3 | 可被「put + get_stale_with_index」覆盖；动写 ack 管线 + proto + 滚动门控，收益/成本不划算 | 下游触发 | L |
| 10 | delete 带序号 / tombstone | P3 | 语义未定（缺键 None 冲突），需先开 API 语义设计 | 下游触发 | L+ |
| 11 | 快照尺寸/压缩点观察 | P3 | 每 key +8B index +1B 头 → `snapshot_threshold` 触发点前移；`bench-baseline` 已有 | — | S |
| 12 | 公开 API 站点/embedding 文档同步 | P3 | `docs/api-reference.html` 与 facade/embedding 一致性 | — | S |

> **评审差异说明**：原草案将 #1 标 P2、#5 标 P1、`arachne-sim` 单列——@oracle 实测改为 #1 P0（CI 已红）、#5 P2、`arachne-sim` 为 13 行 scaffold 无判定（剔除，并入 #7）。

## 1. 执行序（依赖驱动）

```
Phase 0  #1 lockfile resync + l2/ci.yml `--locked` 收敛   ← 立刻（CI 红）
Phase 1  #2 propsol 决议  ‖  #3 test-plan 对齐（可并行）  ‖  #4 运维 runbook
Phase 2  #7 l2 in_sim 确定性（依赖1） ‖  #6 fuzz target（依赖1） ‖  #5 node HTTP
Phase 3  #8/#11/#12 观察与文档项；#9/#10 下游触发
```

顺序理由（@oracle）：#1 是无条件前置且现在就是红的；#2/#3 零依赖零风险；#4 对外后果最重应尽早；#6/#7 都要 #1 先绿；#9/#10 以需求为准、不要提前做。

## 2. 每项规格（内容 / 验收 / Owner）

### P0
**#1 lockfile resync**（S，立即）
- 内容：`fuzz/`、`model-check/`、`l2/` 三个独立 Cargo.lock 重新解析至 arachne-kv 0.3.0（路径依赖，`cargo update -p arachne-kv` / 重生成锁），并与 main 一起提交。l2 的 CI job 补 `--locked`（与 fuzz/model-check 口径统一，堵静默 drift）。
- 验收：三个目录 `cargo metadata --locked` 通过；本地跑一遍各自测试构建不因版本冲突失败；`ci.yml` 口径三处一致。
- 注：该 stale 系 0.2.0 发布时遗留（非本 release 引入），本次一并修复。

### P1
**#2 propsol 决议条目**（S）
- 内容：`dev-docs/propsol-v0.2.md` 追加决议——「首个用户反馈触发 Q5 重审：采用 value 溯源 index 原语（`get_stale_with_index`），**替代** per-handle 单调水位备选」；N1 表述补「可附 index 的 stale 读」；引用 `dev-docs/arachne-kv-commit-index-design.md`。
- 验收：propsol 决策记录格式一致，Q5 行标注「已重审 / 触发条件已兑现」。

**#3 test-plan 对齐**（S–M）
- 内容：`dev-docs/test-plan-v0.1.md` 的 S02/S16 弱读场景补「附 index」断言；新增 INV（I1–I8 对应，编号延续现有序列）。
- 验收：测试计划 §9 分层与已落地测试（`kv.rs` 单测、`tests/stale_read_with_index.rs`）一一对应。

**#4 运维文档 + 破坏性升级 runbook**（S–M）
- 内容：成文：FORMAT_VERSION 1→2 = 数据目录破坏性升级（旧目录 open 即 fail-stop）；**禁止新 leader 向旧节点发 v2 快照**；推荐整集群停机 → 重播种/迁移 → 起新版本；滚动升级需快照能力门控 + 先追平再做快照；S20 升级矩阵 v0.2↔v0.3 路径。
- 验收：`docs/` 或 dev-docs 有可执行 runbook；设计 §5.4 承诺闭环。

### P2
**#5 node HTTP 暴露/冒烟**（S）
- 内容：`arachne-node` HTTP 表层冒烟 `Arachne::get_stale_with_index`（B 纯本地读，经 facade 即可）。
- 验收：HTTP 冒烟测试绿。

**#6 fuzz target：kv snapshot restore**（M）
- 内容：新增 `fuzz_targets/kv_snapshot_restore.rs`（喂 `decode_store`/`restore`，断言仅 `MalformedSnapshot`、无 panic）；`ci.yml` nightly fuzz job 现硬编码 `wal_recovery`，**必须加/改目标名**，否则永不执行；该步补 `--locked`。
- 验收：nightly CI 实际跑新 target 一轮 smoke；本地 `cargo +nightly fuzz run kv_snapshot_restore` 有界运行无 panic。

**#7 l2 确定性 lane**（S–M）
- 内容：`l2/tests/in_sim.rs`（turmoil）跑通并进门禁；CI `--locked`。
- 验收：in_sim 套件绿；与 fuzz/model-check 同口径。

### P3（观察/触发式，不做则挂账）
- **#8 model-check 跑通**——注意：scaffold 不验证新编码，仅作观测。
- **#9 `put_with_index`**——设计 §9.1 已含并发修正；下游 `put` 后免回读需求出现才做。
- **#10 delete tombstone**——先开 API 语义设计（缺键+序号 vs 缺键=None）；hydra 出现「删有序键」需求才做。
- **#11 快照尺寸/压缩点观察**——记 `bench-baseline` 一条。
- **#12 API 站点/embedding 文档同步**——`docs/api-reference.html` 补 `get_stale_with_index`。

## 3. 外部挂账（不计入本仓待办，不得以内绿关闭）
- **hydra 三方抽验**：以「head index ≥ 已应用 index 才物化」接入并按下游验收流程回执（design §8）。这是 v0.3.0 的 acceptance，由下游侧驱动，本仓侧保持开放挂账。

## 4. 风险与注意
1. **勿把 model-check scaffold 当新格式覆盖**（最隐蔽）：会形成虚假信心。
2. **ci.yml fuzz job 硬编码 target 名**：加 #6 时必改，否则静默不跑。
3. **FORMAT_VERSION v2 升级矩阵**：runbook（#4）须覆盖「旧目录 fail-stop / 重播种」路径，防止 operator 在混合版本下丢状态。
4. **快照体积**：格式每 key +8B，阈值触发前移是预期内尺寸-性能权衡，记录而非修复。

## 5. 里程碑映射
- **M-A（今天可开）**：#1 → #2/#3/#4 并行 → #6/#7 → #5：全部 P0–P2，预计 1–2 个工作日。
- **M-B（触发式）**：#9/#10 以下游需求为准；#8/#11/#12 随发布节奏补齐。
