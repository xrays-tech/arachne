# Release 过程：arachne-kv v0.4.0 发布踩坑与固定流程

> 目的：把 v0.4.0（P0–P2 功能批次）的完整发布流程与全部踩坑固化成文档，配合
> `scripts/release.sh` 自动化，避免下次重踩。

---

## 1. 完整流程（对应 release.sh 各阶段）

```
preflight → bump → 独立锁 resync → 发布门禁 → commit+tag+push → crates.io publish → 验证
```

1. **preflight**：工作区必须 clean（功能开发提交完成后）；读当前版本；校验新版本号格式 `X.Y.Z`；确认 crates.io 凭证可用。
2. **bump**（沿 v0.3.0→0.3.1 惯例）：
   - `Cargo.toml`：`[workspace.package] version` **和** `[workspace.dependencies]` 下 6 条 path dep 的 `version = "旧版"`（它们随 workspace 一起 bump——发布时 path 会被剥掉，留下 semver 要求）。
   - `arachne/Cargo.toml`：唯一独立写 `version = "..."` 的成员（其余成员 `version.workspace = true` 自动跟随）。
   - 根 `Cargo.lock`：`cargo check --workspace` 重生成。
3. **独立锁 resync**：`fuzz/`、`l2/`、`model-check/` 三个**非 workspace 成员**目录各跑 `cargo generate-lockfile`（CI 对它们 `--locked`，不同步会让门禁直接红）。
4. **发布门禁**：`scripts/check-release-features.sh` + lean 串行全量套件 + tonic/node 构建。
5. **commit+tag+push**：`chore(release): bump workspace + arachne-kv to vX.Y.Z`；`git tag -a vX.Y.Z -m "<一行摘要>"`；`git push origin main --tags`。
6. **crates.io publish（依赖序）**：`arachne-kv-seam` → `arachne-kv-transport-tonic` → `arachne-kv` → `arachne-kv-node` → `arachne-kv-testsupport`。
7. **验证**：crates.io API 逐 crate 确认新版本 available。

## 2. 关键坑（每一步都踩过，已固化进脚本）

### 坑 1：crates.io 发布有一个「发布级依赖环」
- `arachne-kv` 的默认特性 `transport-tonic` 依赖 `arachne-kv-transport-tonic`（optional，但默认开启 → 发布验证会解析它）。
- `arachne-kv-transport-tonic` 的 `[dev-dependencies]` 依赖 `arachne-kv`。
- `cargo publish` 的 verify 会**解析 dev-dependencies**，于是两头互相要求「对方先在 crates.io 存在」：

```
arachne-kv (默认特性) ──依赖──▶ arachne-kv-transport-tonic
      ▲                                    │
      └──── (dev-dependency) ──────────────┘      ← 环
```

- **破环惯例（经 v0.3.1 / v0.4.0 两次验证）**：发布某个 crate 前，**临时裁剪它 `[dev-dependencies]` 里指向「本次尚未发布的 workspace 内部 crate」的那几条**，用 `--allow-dirty` 发布（因为裁剪后 git dirty），发布后 `git checkout -- <文件>` 恢复。
- 规则自动化：对每个待发布 crate，挑出它 dev-deps 中「在发布顺序里位于自己之后」的 `arachne-kv-*` 依赖，注释掉 → 发布 → 恢复。当前实例：tonic 裁 `arachne-kv`；kv 裁 `arachne-kv-testsupport`；其余无需裁。

### 坑 2：`cargo publish --allow-dirty` 会连带改写根 `Cargo.lock`
- 发布时 cargo 按被裁剪后的 manifest 重算依赖图，把「被裁掉的 dev-dep 边」从根 `Cargo.lock` 删除。
- **发布完成后必须 `git checkout -- Cargo.lock` 恢复**（提交树里应保留完整 dev-dep 边），否则工作区脏、锁文件与源码不一致。

### 坑 3：包查找用 crate 名，不用目录名
- `cargo publish -p arachne` ❌（目录是 `arachne/`，crate 名是 `arachne-kv`）。
- 目录→crate：`arachne-seam/`=arachne-kv-seam、`arachne/`=arachne-kv、`arachne-transport-tonic/`=arachne-kv-transport-tonic、`arachne-node/`=arachne-kv-node、`arachne-testsupport/`=arachne-kv-testsupport。

### 坑 4：认证
- crates.io 凭证：`~/.cargo/credentials.toml`（`cargo login` 生成），或 `~/.cargo-token.txt`（本项目定制的 token 文件），或 `CARGO_REGISTRY_TOKEN` 环境变量。
- 脚本按 `CARGO_REGISTRY_TOKEN` → `~/.cargo-token.txt` → `~/.cargo/credentials.toml` 顺序取用；token 绝不打印。

### 坑 5：门禁顺序
- 发布门禁必须先于 commit/tag/push；lean 套件必须**串行**跑（`--test-threads=1`）以规避 `facade_no_runtime` 的既有进程级单例竞态（并行下偶发 `AlreadyInitialized`，与本次改动无关）。

## 3. 使用方式

```sh
scripts/release.sh 0.4.1                  # 完整发布（bump→门禁→tag→push→publish→验证）
scripts/release.sh 0.4.1 --skip-publish   # 只做到 push + tag（crates.io 稍后单独发）
scripts/release.sh 0.4.1 --dry-run        # 只打印将执行的命令，不落地
```

> 注意：publish 阶段对外部副作用不可逆（crates.io），务必先 `--skip-publish` 或 `--dry-run` 核对，
> 再全量执行。
