#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
TARGET_DIR="${TARGET_DIR:-"${REPO_ROOT}/target"}"
CLANG_WORKDIR="${CLANG_WORKDIR:-"${TARGET_DIR}/sinomo-build-tools-verify-clang-wasix-work"}"
CLANG_OUTDIR="${CLANG_OUTDIR:-"${TARGET_DIR}/sinomo-build-tools-verify-clang-wasix-out"}"
if [ -z "${SINOMO_VERIFY_DATA_DIR+x}" ]; then
    SINOMO_VERIFY_DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/sinomo-clang-wasix-verify.XXXXXX")"
    SINOMO_VERIFY_DATA_DIR_AUTO=1
else
    SINOMO_VERIFY_DATA_DIR_AUTO=0
fi
SANDBOX_ID="${SANDBOX_ID:-01k4z8q5f00000000000000001}"
TOOLCHAIN_ID="${TOOLCHAIN_ID:-01k4z8q5f00000000000000000}"
KEEP_SANDBOX="${KEEP_SANDBOX:-0}"
RUN_ENV=(SINOMO_DATA_DIR="${SINOMO_VERIFY_DATA_DIR}")

if [ -n "${SINOMO_BIN:-}" ]; then
    SINOMO_CMD=("${SINOMO_BIN}")
else
    SINOMO_CARGO_FEATURES="${SINOMO_CARGO_FEATURES:-sinomo-sandbox/wasix-runtime}"
    SINOMO_CMD=(cargo run --quiet --manifest-path "${REPO_ROOT}/Cargo.toml")
    if [ -n "${SINOMO_CARGO_FEATURES}" ]; then
        SINOMO_CMD+=(--features "${SINOMO_CARGO_FEATURES}")
    fi
    SINOMO_CMD+=(--bin sinomo --)
fi

cleanup() {
    if [ "${KEEP_SANDBOX}" != "1" ]; then
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox destroy "${SANDBOX_ID}" >/dev/null 2>&1 || true
    fi
    if [ "${SINOMO_VERIFY_DATA_DIR_AUTO}" = "1" ] && [ "${KEEP_SANDBOX}" != "1" ]; then
        chmod -R u+rwX "${SINOMO_VERIFY_DATA_DIR}" >/dev/null 2>&1 || true
        rm -rf "${SINOMO_VERIFY_DATA_DIR}"
    fi
}
trap cleanup EXIT

if [ "${SKIP_BUILD:-0}" != "1" ]; then
    echo "building Clang/LLVM WASIX package"
    WORKDIR="${CLANG_WORKDIR}" OUTDIR="${CLANG_OUTDIR}" \
        "${ROOT}/build-clang-wasix.sh"
fi

if [ ! -f "${CLANG_OUTDIR}/package.json" ]; then
    echo "Clang package manifest missing: ${CLANG_OUTDIR}/package.json; rerun without SKIP_BUILD=1" >&2
    exit 1
fi

HELLO_C="${CLANG_OUTDIR}/hello.c"
cat > "${HELLO_C}" <<'EOF'
#include <stdio.h>
int main(void) {
    puts("hello from clang");
    return 0;
}
EOF

echo "verifying Clang package manifest"
"${SINOMO_CMD[@]}" sandbox toolchain verify-package "${CLANG_OUTDIR}/package.json"

echo "using disposable SINOMO_DATA_DIR=${SINOMO_VERIFY_DATA_DIR}"
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox policy set \
    --allow-unsigned-toolchains \
    --max-memory-mb 2048 \
    --max-wall-time-ms 300000 \
    --max-workspace-bytes 1073741824 \
    --max-written-bytes-per-command 1073741824 \
    --allow-capability child-process-exec \
    --allow-capability wasix.unpinned-child-exec \
    --allow-toolchain "${TOOLCHAIN_ID}" >/dev/null 2>&1 || true
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox toolchain import-package "${CLANG_OUTDIR}/package.json"
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox create \
    --id "${SANDBOX_ID}" \
    --toolchain "${TOOLCHAIN_ID}" \
    --memory-mb 2048 \
    --wall-time-ms 30000 \
    --max-workspace-bytes 1073741824
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox write "${SANDBOX_ID}" /work/hello.c --file "${HELLO_C}"
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox run "${SANDBOX_ID}" \
    --timeout-ms 30000 \
    --allow-capability child-process-exec \
    --allow-capability wasix.unpinned-child-exec \
    clang -- --version
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox run "${SANDBOX_ID}" \
    --timeout-ms 30000 \
    --allow-capability child-process-exec \
    --allow-capability wasix.unpinned-child-exec \
    clang -- /work/hello.c -o /work/hello.wasm
WORK_LIST="$(env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox ls "${SANDBOX_ID}" /work)"
printf '%s\n' "${WORK_LIST}"
grep -q '/work/hello.wasm' <<<"${WORK_LIST}"
