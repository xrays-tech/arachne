#!/usr/bin/env bash
#
# release.sh — Arachne 发布脚本（v0.4.0 发布流程固化，见 dev-docs/release-process.md）
#
# 一键执行完整发布：preflight → bump → 独立锁 resync → 发布门禁 → commit+tag+push
# → crates.io 依赖序 publish → crates.io 验证。固化 v0.4.0 踩过的所有坑：
#
#   * 发布级依赖环（kv 默认特性 -> tonic；tonic dev-dep -> kv）：发布时临时裁剪
#     指向「本次发布顺序中位于自己之后」的 workspace 内部 dev-dep，`--allow-dirty`
#     发布后恢复（v0.3.1 / v0.4.0 两次验证的惯例）。
#   * `cargo publish --allow-dirty` 会改写根 Cargo.lock（删掉被裁 dev-dep 的边）：
#     发布后恢复 Cargo.lock。
#   * 包查找用 crate 名（目录 `arachne/` → crate `arachne-kv`）。
#   * lean 门禁需串行（规避 facade_no_runtime 进程级单例竞态）。
#
# 用法：
#   scripts/release.sh <new_version>             # 完整发布（含 crates.io publish）
#   scripts/release.sh <new_version> --skip-publish   # 只到 push+tag
#   scripts/release.sh <new_version> --dry-run    # 只打印命令，不落地
#
# 凭证：CARGO_REGISTRY_TOKEN > ~/.cargo-token.txt > ~/.cargo/credentials.toml。
# 安全性：token 绝不打印；publish 对外部副作用不可逆，先 --dry-run / --skip-publish 核对。

set -euo pipefail

# ---- 入口参数 ---------------------------------------------------------------

NEW_VERSION="${1:-}"
MODE="${2:-full}"   # full | skip-publish | dry-run
case "${MODE}" in
  full)          PUBLISH=1; DRY_RUN=0 ;;
  skip-publish)  PUBLISH=0; DRY_RUN=0 ;;
  dry-run)       PUBLISH=1; DRY_RUN=1 ;;
  *) echo "usage: $0 <x.y.z> [full|skip-publish|dry-run]" >&2; exit 2 ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

PACKAGES=(arachne-kv-seam arachne-kv-transport-tonic arachne-kv arachne-kv-node arachne-kv-testsupport)
STANDALONE_locks=(fuzz l2 model-check)   # 非 workspace 成员，各有独立 Cargo.lock

say()  { printf '\n== %s\n' "$*"; }
run() {
  # 在 dry-run 下打印，否则执行
  if [ "${DRY_RUN}" -eq 1 ]; then printf '  DRY: %s\n' "$*"; else eval "$@"; fi
}

# ---- 0. 预检 -----------------------------------------------------------------

[ -n "${NEW_VERSION}" ] || { echo "usage: $0 <x.y.z> [full|skip-publish|dry-run]" >&2; exit 2; }
echo "${NEW_VERSION}" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$' || {
  echo "版本号格式应为 X.Y.Z（如 0.4.1）：${NEW_VERSION}" >&2; exit 2; }

# 当前版本：只读 [workspace.package].version（唯一事实源）
OLD_VERSION="$(sed -n '/^\[workspace.package\]/,/^\[/p' Cargo.toml | grep -m1 '^version = ' | sed 's/.*"\([^"]*\)".*/\1/')"
[ -n "${OLD_VERSION}" ] || { echo "无法从 Cargo.toml 读取当前版本" >&2; exit 2; }
echo "当前版本: ${OLD_VERSION}  ->  新版本: ${NEW_VERSION}"

# 工作区必须 clean（功能/文档已提交后再发版）
if [ "$(git status --porcelain | wc -l)" -ne 0 ] && [ "${DRY_RUN}" -eq 0 ]; then
  echo "工作区有未提交改动，先提交或 stash 再发布：" >&2; git status --porcelain >&2; exit 2
fi

# crates.io 凭证探测（读之前不打印内容）
TOKEN_FILE=""
if [ -n "${CARGO_REGISTRY_TOKEN:-}" ]; then TOKEN_FILE="__env__";
elif [ -f "${HOME}/.cargo-token.txt" ]; then TOKEN_FILE="${HOME}/.cargo-token.txt";
elif [ -f "${HOME}/.cargo/credentials.toml" ]; then TOKEN_FILE="${HOME}/.cargo/credentials.toml";
fi
[ -n "${TOKEN_FILE}" ] || { echo "未找到 crates.io 凭证（CARGO_REGISTRY_TOKEN / ~/.cargo-token.txt / ~/.cargo/credentials.toml）" >&2; exit 2; }
echo "凭证来源: ${TOKEN_FILE/__env__/CARGO_REGISTRY_TOKEN 环境变量}"

# ---- 1. bump 版本（workspace.package + workspace.dependencies + arachne/Cargo.toml） --

say "1/7 bump 版本 ${OLD_VERSION} -> ${NEW_VERSION}"
for f in Cargo.toml arachne/Cargo.toml; do
  run "sed -i 's/version = \"${OLD_VERSION}\"/version = \"${NEW_VERSION}\"/g' ${f}"
done
# 根 Cargo.lock 重新生成（随 workspace 版本）
run "cargo check --workspace -q"

# ---- 2. 独立锁 resync（fuzz / l2 / model-check 在 CI 以 --locked 构建） ------------

say "2/7 resync 独立锁文件"
for d in "${STANDALONE_locks[@]}"; do
  run "(cd ${d} && cargo generate-lockfile -q)"
done

# ---- 3. 发布门禁 ---------------------------------------------------------------

say "3/7 发布门禁"
run "bash scripts/check-release-features.sh"
run "cargo test -p arachne-kv --no-default-features -- --test-threads=1"
run "cargo build -p arachne-kv-transport-tonic -p arachne-kv-node"

# ---- 4. commit + tag + 附注 + push --------------------------------------------

say "4/7 commit + tag + push（v${NEW_VERSION}）"
COMMIT_NSG="chore(release): bump workspace + arachne-kv to v${NEW_VERSION}"
# 附注：与 v0.3.0/v0.3.1 惯例一致的一行摘要（调用方可按需自定义；破折号后是示例）
TAG_NSG="v${NEW_VERSION}: release — summary = <短描述> (edit me, or set TAG_MSG=...)"
if [ -n "${TAG_MSG:-}" ]; then TAG_NSG="${TAG_MSG}"; fi
if [ "${DRY_RUN}" -eq 0 ]; then
  git add Cargo.toml arachne/Cargo.toml Cargo.lock fuzz/Cargo.lock l2/Cargo.lock model-check/Cargo.lock
  git commit -m "${COMMIT_NSG}"
  git tag -a "v${NEW_VERSION}" -m "${TAG_NSG}"
  git push origin main --tags
else
  echo "  DRY: git add Cargo.toml arachne/Cargo.toml Cargo.lock fuzz/Cargo.lock l2/Cargo.lock model-check/Cargo.lock"
  echo "  DRY: git commit -m \"${COMMIT_NSG}\""
  echo "  DRY: git tag -a v${NEW_VERSION} -m \"${TAG_NSG}\""
  echo "  DRY: git push origin main --tags"
fi

# ---- 5. crates.io 依赖序发布（含裁剪破环 + 锁恢复） --------------------------------

[ "${PUBLISH}" -eq 1 ] || { say "skip-publish：已到 push+tag，跳过 crates.io 发布"; exit 0; }
[ "${DRY_RUN}" -eq 1 ] && { say "dry-run：以下为 crates.io 发布序列（不会执行）"; }

say "5/7 crates.io 发布（依赖序：${PACKAGES[*]}）"
# 折叠函数：临时注释某个 crate 的 [dev-dependencies] 中指向「发布顺序里位于自己之后」的内部 dev-dep
# （破发布环，v0.4.0 惯例）。参数：当前发布中 crate 名 + 该 crate 的 manifest 路径。
trim_internal_dev_deps() {
  local pkg="$1" manifest="$2" crate
  local idx=-1 n=0 k
  # 找到发布序中位于本 crate 之后的包名（这些包尚未发布，dev-dep 指向它们会构成发布环）
  for crate in "${PACKAGES[@]}"; do [ "${crate}" = "${pkg}" ] && { idx=$n; break; }; n=$((n+1)); done
  local waitfor=""
  for ((k=idx+1; k<${#PACKAGES[@]}; k++)); do waitfor="${waitfor} ${PACKAGES[k]}"; done
  # 在 [dev-dependencies] 段内注释掉属于 waitfor 的 `crate = { ... }` 行（可能带尾注释）
  # 用 python 按段切分，安全性比 sed 高。
  run "python3 - <<'PY'
import re, sys
p='${manifest}'
s=open(p).read()
sec=re.search(r'^\[dev-dependencies\](.*?)(?=^\[|\Z)', s, re.M|re.S)
if not sec: sys.exit(0)
body=sec.group(1)
def repl(m):
    # 正则分组：\1=前导空白，\2=crate 名（取 \2 才是名字）
    name=m.group(2).strip()
    if name in '''${waitfor}'''.split():
        # 注释整行（保留原文以便恢复）
        return '#' + m.group(0)
    return m.group(0)
newbody=re.sub(r'^([ \t]*)([a-zA-Z0-9_-]+)[ \t]*=[ \t]*\{', repl, body, flags=re.M)
open(p,'w').write(s[:sec.start(1)] + newbody + s[sec.end(1):])
PY"
}

CRATE_DIRS=(arachne-seam arachne-transport-tonic arachne arachne-node arachne-testsupport)
for i in "${!PACKAGES[@]}"; do
  pkg="${PACKAGES[$i]}"; dir="${CRATE_DIRS[$i]}"
  say "  → ${pkg} v${NEW_VERSION} (${dir}/Cargo.toml)"
  # 裁剪指向「发布序靠后」的内部 dev-dep（破环）
  trim_internal_dev_deps "${pkg}" "${dir}/Cargo.toml"
  # 发布（裁剪后 git dirty，需 --allow-dirty）
  if [ "${DRY_RUN}" -eq 0 ]; then
    CARGO_REGISTRY_TOKEN="${CARGO_REGISTRY_TOKEN:-$(cat "${HOME}/.cargo-token.txt" 2>/dev/null || true)}" \
      cargo publish -p "${pkg}" --allow-dirty
  else
    echo "  DRY: cargo publish -p ${pkg} --allow-dirty"
  fi
  # 发布后恢复被裁剪的 manifest；根 Cargo.lock 也可能被 cargo 改写，一并无条件恢复
  if [ "${DRY_RUN}" -eq 0 ]; then
    git checkout -- "${dir}/Cargo.toml" Cargo.lock
  else
    echo "  DRY: git checkout -- ${dir}/Cargo.toml Cargo.lock"
  fi
done

# 收尾：确认没有把裁剪/锁改写带进提交；工作区应回到 clean（若 preflight 强制了 clean）
if [ "${DRY_RUN}" -eq 0 ]; then
  say "6/7 校验工作区 clean（不应有裁剪残留）"
  git status --porcelain || true
fi

# ---- 7. crates.io 验证 ----------------------------------------------------------

say "7/7 crates.io 验证 v${NEW_VERSION} available"
if [ "${DRY_RUN}" -eq 0 ]; then
  UA="arachne-release-check (contact: noreply@example.invalid)"
  for pkg in "${PACKAGES[@]}"; do
    printf '  %-32s ' "${pkg}"
    if curl -sfL --compressed -A "${UA}" "https://crates.io/api/v1/crates/${pkg}/${NEW_VERSION}" \
        | grep -q '"version"'; then
      echo "OK  v${NEW_VERSION}"
    else
      echo "MISSING  v${NEW_VERSION}"; exit 1
    fi
  done
else
  for pkg in "${PACKAGES[@]}"; do echo "  DRY: check crates.io/${pkg} == v${NEW_VERSION}"; done
fi

say "发布完成：v${OLD_VERSION} -> v${NEW_VERSION}"
