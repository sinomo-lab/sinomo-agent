#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../.." && pwd)"
TARGET_DIR="${TARGET_DIR:-"${REPO_ROOT}/target"}"
BUSYBOX_WORKDIR="${BUSYBOX_WORKDIR:-"${TARGET_DIR}/sinomo-bash-verify-busybox-wasix-work"}"
BUSYBOX_OUTDIR="${BUSYBOX_OUTDIR:-"${TARGET_DIR}/sinomo-bash-verify-busybox-wasix-out"}"
BASH_WORKDIR="${BASH_WORKDIR:-"${TARGET_DIR}/sinomo-bash-verify-bash-wasix-work"}"
BASH_OUTDIR="${BASH_OUTDIR:-"${TARGET_DIR}/sinomo-bash-verify-bash-wasix-out"}"
if [ -z "${SINOMO_VERIFY_DATA_DIR+x}" ]; then
    SINOMO_VERIFY_DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/sinomo-bash-wasix-verify.XXXXXX")"
    SINOMO_VERIFY_DATA_DIR_AUTO=1
else
    SINOMO_VERIFY_DATA_DIR_AUTO=0
fi
SANDBOX_ID="${SANDBOX_ID:-01k4z8q5b00000000000000001}"
TOOLCHAIN_ID="${TOOLCHAIN_ID:-086wdctfpphb26n9k2cg751qqs}"
VERIFY_BACKEND="${VERIFY_BACKEND:-local}"
UPDATE_CLUSTER_POLICY="${UPDATE_CLUSTER_POLICY:-0}"
KEEP_SANDBOX="${KEEP_SANDBOX:-0}"
DAEMON_PORT="${DAEMON_PORT:-21454}"
DAEMON_STARTED=0
RUN_ENV=()

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

case "${VERIFY_BACKEND}" in
    local|daemon|daemon-local) ;;
    *)
        echo "VERIFY_BACKEND must be local, daemon, or daemon-local, got: ${VERIFY_BACKEND}" >&2
        exit 1
        ;;
esac

cleanup() {
    if [[ "${VERIFY_BACKEND}" == daemon* ]] && [ "${KEEP_SANDBOX}" != "1" ]; then
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox destroy "${SANDBOX_ID}" --daemon >/dev/null 2>&1 || true
    fi
    if [ "${DAEMON_STARTED}" = "1" ]; then
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" daemon stop >/dev/null 2>&1 || true
    fi
    if [ "${SINOMO_VERIFY_DATA_DIR_AUTO}" = "1" ] && [ "${KEEP_SANDBOX}" != "1" ]; then
        rm -rf "${SINOMO_VERIFY_DATA_DIR}"
    fi
}
trap cleanup EXIT

if [ "${SKIP_BUILD:-0}" != "1" ]; then
    echo "building BusyBox WASIX toolchain package"
    BUSYBOX_TARGET_ABI=wasix WORKDIR="${BUSYBOX_WORKDIR}" OUTDIR="${BUSYBOX_OUTDIR}" \
        "${ROOT}/busybox/build-busybox-wasi.sh"

    echo "building Bash WASIX package"
    WORKDIR="${BASH_WORKDIR}" OUTDIR="${BASH_OUTDIR}" \
        "${ROOT}/bash/build-bash-wasix.sh"
fi

if [ ! -f "${BUSYBOX_OUTDIR}/package.json" ] || [ ! -f "${BASH_OUTDIR}/package.json" ]; then
    echo "Bash verifier package manifests are missing; rerun without SKIP_BUILD=1" >&2
    exit 1
fi

echo "verifying package manifests"
"${SINOMO_CMD[@]}" sandbox toolchain verify-package "${BUSYBOX_OUTDIR}/package.json"
"${SINOMO_CMD[@]}" sandbox toolchain verify-package "${BASH_OUTDIR}/package.json"

if [ "${VERIFY_BACKEND}" = "daemon-local" ]; then
    echo "using disposable daemon SINOMO_DATA_DIR=${SINOMO_VERIFY_DATA_DIR} port=${DAEMON_PORT}"
    RUN_ENV=(SINOMO_DATA_DIR="${SINOMO_VERIFY_DATA_DIR}")
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" cluster init \
        --name sandbox-bash-verify \
        --cluster-id 01k4z8q5b00000000000000003 \
        --node-id 01k4z8q5b00000000000000004 >/dev/null
    UPDATE_CLUSTER_POLICY=1
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" daemon start --port "${DAEMON_PORT}" >/dev/null
    DAEMON_STARTED=1
    for _ in $(seq 1 50); do
        if env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" daemon status >/dev/null 2>&1; then
            break
        fi
        sleep 0.1
    done
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" daemon status >/dev/null
elif [ "${VERIFY_BACKEND}" = "daemon" ]; then
    RUN_ENV=()
else
    echo "using disposable SINOMO_DATA_DIR=${SINOMO_VERIFY_DATA_DIR}"
    RUN_ENV=(SINOMO_DATA_DIR="${SINOMO_VERIFY_DATA_DIR}")
fi

if [ "${UPDATE_CLUSTER_POLICY}" = "1" ]; then
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox policy set \
        --allow-unsigned-toolchains \
        --allow-capability child-process-exec \
        --allow-capability wasix.unpinned-child-exec \
        --allow-toolchain "${TOOLCHAIN_ID}" \
        --allow-toolchain-provenance-prefix "Sinomo BusyBox WASIX recipe:" \
        --allow-toolchain-provenance-prefix "Sinomo Bash WASIX recipe:" \
        --allow-package-cache-prefix /services/sandbox/toolchains >/dev/null
fi

IMPORT_ARGS=(sandbox toolchain import-package)
CREATE_ARGS=(sandbox create --id "${SANDBOX_ID}" --toolchain "${TOOLCHAIN_ID}")
VERIFY_ARGS=(sandbox toolchain verify-bash "${SANDBOX_ID}")
if [[ "${VERIFY_BACKEND}" == daemon* ]]; then
    IMPORT_ARGS+=(--daemon)
    CREATE_ARGS+=(--daemon)
    VERIFY_ARGS+=(--daemon)
else
    VERIFY_ARGS+=(--allow-unsigned)
fi

env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${IMPORT_ARGS[@]}" \
    "${BUSYBOX_OUTDIR}/package.json"

env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${IMPORT_ARGS[@]}" \
    --into "${TOOLCHAIN_ID}" \
    "${BASH_OUTDIR}/package.json"

env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${CREATE_ARGS[@]}"

env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${VERIFY_ARGS[@]}"

WORK_EXEC_WASM="${BUSYBOX_OUTDIR}/busybox-1.36.1-wasix.wasm"
if [ ! -f "${WORK_EXEC_WASM}" ]; then
    echo "sandbox-local exec fixture is missing: ${WORK_EXEC_WASM}" >&2
    exit 1
fi

WRITE_ARGS=(sandbox write "${SANDBOX_ID}" /work/printf --file "${WORK_EXEC_WASM}")
RUN_ARGS=(
    sandbox run "${SANDBOX_ID}"
    --timeout-ms 30000
    bash -- -c "chmod +x /work/printf && /work/printf 'sandbox local exec\\n'"
)
if [[ "${VERIFY_BACKEND}" == daemon* ]]; then
    WRITE_ARGS+=(--daemon)
    RUN_ARGS+=(--daemon)
else
    RUN_ARGS=(
        sandbox run "${SANDBOX_ID}"
        --allow-capability child-process-exec
        --allow-capability wasix.unpinned-child-exec
        --timeout-ms 30000
        bash -- -c "chmod +x /work/printf && /work/printf 'sandbox local exec\\n'"
    )
fi

env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${WRITE_ARGS[@]}" >/dev/null
LOCAL_EXEC_OUTPUT="$(env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}")"
printf '%s\n' "${LOCAL_EXEC_OUTPUT}"
if ! printf '%s\n' "${LOCAL_EXEC_OUTPUT}" | grep -q '^sandbox local exec$'; then
    echo "bash-exec-work-wasm: expected sandbox-local WASM exec output" >&2
    exit 1
fi
