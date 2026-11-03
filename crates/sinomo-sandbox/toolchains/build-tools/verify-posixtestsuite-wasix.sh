#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
TARGET_DIR="${TARGET_DIR:-"${REPO_ROOT}/target"}"
CLANG_OUTDIR="${CLANG_OUTDIR:-"${TARGET_DIR}/sinomo-build-tools-verify-clang-wasix-out"}"
BUSYBOX_OUTDIR="${BUSYBOX_OUTDIR:-"${TARGET_DIR}/sinomo-build-tools-verify-busybox-wasix-out"}"
BASH_OUTDIR="${BASH_OUTDIR:-"${TARGET_DIR}/sinomo-build-tools-verify-bash-wasix-out"}"
POSIXTESTSUITE_REV="${POSIXTESTSUITE_REV:-85555325079ea362fa680bd2209c843cfe47e670}"
POSIXTESTSUITE_REPO="${POSIXTESTSUITE_REPO:-https://github.com/emscripten-core/posixtestsuite}"
POSIXTESTSUITE_WORKDIR="${POSIXTESTSUITE_WORKDIR:-"${TARGET_DIR}/sinomo-posixtestsuite-${POSIXTESTSUITE_REV}"}"
SINOMO_VERIFY_TMPDIR="${SINOMO_VERIFY_TMPDIR:-"${TARGET_DIR}/sinomo-posixtestsuite-wasix-tmp"}"
RESULTS_DIR="${RESULTS_DIR:-"${TARGET_DIR}/sinomo-posixtestsuite-results"}"
POSIXTESTSUITE_SCOPE="${POSIXTESTSUITE_SCOPE:-all}"
POSIXTESTSUITE_OFFSET="${POSIXTESTSUITE_OFFSET:-0}"
POSIXTESTSUITE_LIMIT="${POSIXTESTSUITE_LIMIT:-0}"
POSIXTESTSUITE_RUN_SHELL="${POSIXTESTSUITE_RUN_SHELL:-1}"
POSIXTESTSUITE_COMPILE_MODE="${POSIXTESTSUITE_COMPILE_MODE:-host}"
POSIXTESTSUITE_HOST_SYSROOT="${POSIXTESTSUITE_HOST_SYSROOT:-"${CLANG_OUTDIR}/artifacts/sysroot"}"
POSIXTESTSUITE_HOST_CC_BIN="${POSIXTESTSUITE_HOST_CC_BIN:-}"
COMPILE_TIMEOUT_MS="${COMPILE_TIMEOUT_MS:-30000}"
LINK_TIMEOUT_MS="${LINK_TIMEOUT_MS:-30000}"
RUN_TIMEOUT_MS="${RUN_TIMEOUT_MS:-30000}"
SH_TIMEOUT_MS="${SH_TIMEOUT_MS:-30000}"
read -r -a POSIXTESTSUITE_EXTRA_CFLAGS <<<"${POSIXTESTSUITE_CFLAGS:-}"
read -r -a POSIXTESTSUITE_SANDBOX_ENV <<<"${POSIXTESTSUITE_SANDBOX_ENV:-}"
SANDBOX_MEMORY_MB="${SANDBOX_MEMORY_MB:-2048}"
SANDBOX_MAX_WALL_TIME_MS="${SANDBOX_MAX_WALL_TIME_MS:-300000}"
SANDBOX_MAX_INPUT_BYTES="${SANDBOX_MAX_INPUT_BYTES:-67108864}"
SANDBOX_MAX_OUTPUT_BYTES="${SANDBOX_MAX_OUTPUT_BYTES:-67108864}"
SANDBOX_MAX_WORKSPACE_BYTES="${SANDBOX_MAX_WORKSPACE_BYTES:-1073741824}"
SANDBOX_MAX_WRITTEN_BYTES_PER_COMMAND="${SANDBOX_MAX_WRITTEN_BYTES_PER_COMMAND:-268435456}"
SANDBOX_MAX_FILE_COUNT="${SANDBOX_MAX_FILE_COUNT:-100000}"
SINOMO_HOST_MEMORY_MAX="${SINOMO_HOST_MEMORY_MAX:-12G}"
SINOMO_HOST_TASKS_MAX="${SINOMO_HOST_TASKS_MAX:-2048}"
SINOMO_HOST_AS_LIMIT_BYTES="${SINOMO_HOST_AS_LIMIT_BYTES:-unlimited}"
RESULT_SCOPE_NAME="${POSIXTESTSUITE_SCOPE//[^A-Za-z0-9_.-]/_}"
RESULT_BASENAME="posixtestsuite-wasix-${RESULT_SCOPE_NAME}-offset-${POSIXTESTSUITE_OFFSET}-limit-${POSIXTESTSUITE_LIMIT}"
RESULTS_FILE="${RESULTS_FILE:-"${RESULTS_DIR}/${RESULT_BASENAME}.tsv"}"
SUMMARY_FILE="${SUMMARY_FILE:-"${RESULTS_DIR}/${RESULT_BASENAME}.summary.txt"}"
LATEST_RESULTS_FILE="${RESULTS_DIR}/posixtestsuite-wasix-latest.tsv"
LATEST_SUMMARY_FILE="${RESULTS_DIR}/posixtestsuite-wasix-latest.summary.txt"

if [ -z "${SINOMO_VERIFY_DATA_DIR+x}" ]; then
    mkdir -p "${SINOMO_VERIFY_TMPDIR}"
    SINOMO_VERIFY_DATA_DIR="$(mktemp -d "${SINOMO_VERIFY_TMPDIR}/sinomo-posixtestsuite-wasix.XXXXXX")"
    SINOMO_VERIFY_DATA_DIR_AUTO=1
else
    SINOMO_VERIFY_DATA_DIR_AUTO=0
fi

SANDBOX_ID="${SANDBOX_ID:-01k4z8q5f00000000000000011}"
TOOLCHAIN_ID="${TOOLCHAIN_ID:-01k4z8q5f00000000000000000}"
KEEP_SANDBOX="${KEEP_SANDBOX:-0}"
RUN_ENV=(SINOMO_DATA_DIR="${SINOMO_VERIFY_DATA_DIR}" TMPDIR="${SINOMO_VERIFY_TMPDIR}")

SINOMO_CARGO_FEATURES="${SINOMO_CARGO_FEATURES:-sinomo-sandbox/wasix-runtime}"
if [ -z "${SINOMO_BIN:-}" ]; then
    SINOMO_BIN="${REPO_ROOT}/target/debug/sinomo"
    if [ ! -x "${SINOMO_BIN}" ]; then
        CARGO_BUILD=(cargo build --quiet --manifest-path "${REPO_ROOT}/Cargo.toml")
        if [ -n "${SINOMO_CARGO_FEATURES}" ]; then
            CARGO_BUILD+=(--features "${SINOMO_CARGO_FEATURES}")
        fi
        CARGO_BUILD+=(--bin sinomo)
        "${CARGO_BUILD[@]}"
    fi
fi
export SINOMO_BIN
SINOMO_CMD=("${SINOMO_BIN}")
if [ "${SINOMO_HOST_MEMORY_MAX}" != "unlimited" ] && [ "${SINOMO_HOST_MEMORY_MAX}" != "0" ]; then
    if command -v systemd-run >/dev/null 2>&1; then
        SINOMO_CMD=(systemd-run --user --scope --quiet -p "MemoryMax=${SINOMO_HOST_MEMORY_MAX}" -p "TasksMax=${SINOMO_HOST_TASKS_MAX}" -- "${SINOMO_BIN}")
    else
        echo "warning: systemd-run unavailable; host cgroup memory limit is not enforced" >&2
    fi
fi
if [ "${SINOMO_HOST_AS_LIMIT_BYTES}" != "unlimited" ] && [ "${SINOMO_HOST_AS_LIMIT_BYTES}" != "0" ]; then
    if command -v prlimit >/dev/null 2>&1; then
        SINOMO_CMD=(prlimit "--as=${SINOMO_HOST_AS_LIMIT_BYTES}" -- "${SINOMO_BIN}")
    else
        echo "warning: prlimit unavailable; host address-space limit is not enforced" >&2
    fi
fi

cleanup() {
    if [ "${KEEP_SANDBOX}" != "1" ]; then
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox destroy "${SANDBOX_ID}" >/dev/null 2>&1 || true
    fi
    if [ "${SINOMO_VERIFY_DATA_DIR_AUTO}" = "1" ] && [ "${KEEP_SANDBOX}" != "1" ]; then
        rm -rf "${SINOMO_VERIFY_DATA_DIR}"
    fi
}
trap cleanup EXIT

if [ "${POSIXTESTSUITE_COMPILE_MODE}" != "host" ] && [ "${POSIXTESTSUITE_COMPILE_MODE}" != "sandbox" ]; then
    echo "POSIXTESTSUITE_COMPILE_MODE must be 'host' or 'sandbox'" >&2
    exit 1
fi

if [ ! -f "${CLANG_OUTDIR}/package.json" ]; then
    echo "Clang package manifest missing: ${CLANG_OUTDIR}/package.json" >&2
    echo "Run crates/sinomo-sandbox/toolchains/build-tools/verify-clang-wasix.sh first, or set CLANG_OUTDIR." >&2
    exit 1
fi

if [ "${POSIXTESTSUITE_COMPILE_MODE}" = "host" ]; then
    HOST_ENV_CANDIDATE="${POSIXTESTSUITE_HOST_ENV:-"${TARGET_DIR}/sinomo-build-tools-verify-wasix-sysroot-work/package/host-env.sh"}"
    if [ -f "${HOST_ENV_CANDIDATE}" ]; then
        # shellcheck disable=SC1090
        source "${HOST_ENV_CANDIDATE}"
        POSIXTESTSUITE_HOST_CC_BIN="${POSIXTESTSUITE_HOST_CC_BIN:-"${SINOMO_WASI_SDK}/bin/clang"}"
        POSIXTESTSUITE_HOST_SYSROOT="${POSIXTESTSUITE_HOST_SYSROOT:-"${SINOMO_WASIX_SYSROOT}"}"
    fi
    if [ -z "${POSIXTESTSUITE_HOST_CC_BIN}" ] && [ -n "${WASI_SDK:-}" ]; then
        POSIXTESTSUITE_HOST_CC_BIN="${WASI_SDK}/bin/clang"
    fi
    if [ -z "${POSIXTESTSUITE_HOST_CC_BIN}" ]; then
        POSIXTESTSUITE_HOST_CC_BIN="$(find "${TARGET_DIR}" -maxdepth 4 \( -type f -o -type l \) -path '*/wasi-sdk-*/bin/clang' -executable 2>/dev/null | sort | head -n 1 || true)"
    fi
    if [ -z "${POSIXTESTSUITE_HOST_CC_BIN}" ] || [ ! -x "${POSIXTESTSUITE_HOST_CC_BIN}" ]; then
        echo "host compile mode requires POSIXTESTSUITE_HOST_CC_BIN or WASI_SDK/bin/clang" >&2
        exit 1
    fi
    if [ ! -f "${POSIXTESTSUITE_HOST_SYSROOT}/include/unistd.h" ] ||
        [ ! -f "${POSIXTESTSUITE_HOST_SYSROOT}/lib/wasm32-wasi/libc.a" ]; then
        echo "host compile mode requires a prepared WASIX sysroot: ${POSIXTESTSUITE_HOST_SYSROOT}" >&2
        exit 1
    fi
fi

fetch_posixtestsuite() {
    if [ ! -d "${POSIXTESTSUITE_WORKDIR}/.git" ] ||
        [ "$(git -C "${POSIXTESTSUITE_WORKDIR}" rev-parse HEAD 2>/dev/null || true)" != "${POSIXTESTSUITE_REV}" ]; then
        rm -rf "${POSIXTESTSUITE_WORKDIR}"
        git clone --filter=blob:none --no-checkout "${POSIXTESTSUITE_REPO}" "${POSIXTESTSUITE_WORKDIR}"
        git -C "${POSIXTESTSUITE_WORKDIR}" checkout "${POSIXTESTSUITE_REV}"
    fi
    git -C "${POSIXTESTSUITE_WORKDIR}" sparse-checkout disable >/dev/null 2>&1 || true
    git -C "${POSIXTESTSUITE_WORKDIR}" checkout -f "${POSIXTESTSUITE_REV}" >/dev/null
}

write_result() {
    local path="$1"
    local phase="$2"
    local result="$3"
    local detail="${4:-}"
    printf '%s\t%s\t%s\t%s\n' "${path}" "${phase}" "${result}" "${detail//$'\t'/ }" >>"${RESULTS_FILE}"
}

run_sandbox_capture() {
    local log="$1"
    shift
    set +e
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "$@" >"${log}.stdout" 2>"${log}.stderr"
    local status=$?
    set -e
    local stderr_log
    stderr_log="$(sed -n 's/^stderr: //p; s/^.*stderr log: //p' "${log}.stdout" "${log}.stderr" 2>/dev/null | tail -n 1)"
    if [ -n "${stderr_log}" ]; then
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox cat "${SANDBOX_ID}" "${stderr_log}" >"${log}.guest.stderr" 2>/dev/null || true
    fi
    local stdout_log
    stdout_log="$(sed -n 's/^stdout: //p' "${log}.stdout" "${log}.stderr" 2>/dev/null | tail -n 1)"
    if [ -n "${stdout_log}" ]; then
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox cat "${SANDBOX_ID}" "${stdout_log}" >"${log}.guest.stdout" 2>/dev/null || true
    fi
    return "${status}"
}

last_detail() {
    local file
    for file in "$@"; do
        if [ -s "${file}" ]; then
            awk '
                NF == 0 { next }
                /^sinomo:/ { sinomo = $0; next }
                { detail = $0 }
                END {
                    if (detail != "") {
                        print detail
                    } else if (sinomo != "") {
                        print sinomo
                    }
                }
            ' "${file}"
            return
        fi
    done
}

captured_exit_status() {
    local stdout_path="$1"
    sed -n 's/^exit status: //p' "${stdout_path}" | tail -n 1
}

posix_result_name() {
    case "$1" in
        0) printf 'PASS' ;;
        1) printf 'FAIL' ;;
        2) printf 'UNRESOLVED' ;;
        4) printf 'UNSUPPORTED' ;;
        5) printf 'UNTESTED' ;;
        *) printf 'INTERRUPTED' ;;
    esac
}

sanitize_test_name() {
    local name="${1%.c}"
    name="${name%.sh}"
    name="${name//\//_}"
    printf '%s' "${name}"
}

stage_c_test_companions() {
    local test_path="$1"
    local test_dir="${test_path%/*}"
    local file
    while IFS= read -r file; do
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox write "${SANDBOX_ID}" \
            "/work/posixtestsuite/${file}" \
            --file "${POSIXTESTSUITE_WORKDIR}/${file}" >/dev/null
    done < <(
        git -C "${POSIXTESTSUITE_WORKDIR}" ls-tree -r --name-only HEAD "${test_dir}" |
            rg "\.(c|h)$"
    )
}

fetch_posixtestsuite
mkdir -p "${RESULTS_DIR}" "${SINOMO_VERIFY_TMPDIR}/posixtestsuite-logs"
printf 'path\tphase\tresult\tdetail\n' >"${RESULTS_FILE}"

mapfile -t C_TESTS < <(
    git -C "${POSIXTESTSUITE_WORKDIR}" ls-tree -r --name-only HEAD |
        rg '^(conformance|functional|stress)/.*\.c$' |
        rg -v '/(testfrmw|threads_scenarii)\.c$' |
        sort
)
mapfile -t SH_TESTS < <(
    git -C "${POSIXTESTSUITE_WORKDIR}" ls-tree -r --name-only HEAD |
        rg '^(conformance|functional|stress)/.*\.sh$' |
        sort
)

if [ "${POSIXTESTSUITE_SCOPE}" = "smoke" ]; then
    C_TESTS=(
        conformance/interfaces/clock_gettime/1-1.c
        conformance/interfaces/clock_gettime/7-1.c
        conformance/interfaces/clock_gettime/8-1.c
        conformance/interfaces/clock_gettime/8-2.c
        conformance/interfaces/clock_getres/1-1.c
        conformance/interfaces/clock_getres/5-1.c
        conformance/interfaces/clock_getres/6-1.c
        conformance/interfaces/clock_getres/6-2.c
        conformance/interfaces/nanosleep/5-1.c
        conformance/interfaces/nanosleep/6-1.c
    )
    SH_TESTS=()
elif [[ "${POSIXTESTSUITE_SCOPE}" == path:* ]]; then
    prefix="${POSIXTESTSUITE_SCOPE#path:}"
    mapfile -t C_TESTS < <(printf '%s\n' "${C_TESTS[@]}" | rg "^${prefix}")
    mapfile -t SH_TESTS < <(printf '%s\n' "${SH_TESTS[@]}" | rg "^${prefix}")
fi

if [ "${POSIXTESTSUITE_OFFSET}" != "0" ]; then
    if [ "${#C_TESTS[@]}" -gt 0 ]; then
        mapfile -t C_TESTS < <(printf '%s\n' "${C_TESTS[@]}" | tail -n +"$((POSIXTESTSUITE_OFFSET + 1))")
    fi
    if [ "${#SH_TESTS[@]}" -gt 0 ]; then
        mapfile -t SH_TESTS < <(printf '%s\n' "${SH_TESTS[@]}" | tail -n +"$((POSIXTESTSUITE_OFFSET + 1))")
    fi
fi

if [ "${POSIXTESTSUITE_LIMIT}" != "0" ]; then
    if [ "${#C_TESTS[@]}" -gt 0 ]; then
        mapfile -t C_TESTS < <(printf '%s\n' "${C_TESTS[@]}" | head -n "${POSIXTESTSUITE_LIMIT}")
    fi
    if [ "${#SH_TESTS[@]}" -gt 0 ]; then
        mapfile -t SH_TESTS < <(printf '%s\n' "${SH_TESTS[@]}" | head -n "${POSIXTESTSUITE_LIMIT}")
    fi
fi

echo "verifying Clang package manifest"
if [ "${POSIXTESTSUITE_COMPILE_MODE}" = "sandbox" ]; then
    "${SINOMO_CMD[@]}" sandbox toolchain verify-package "${CLANG_OUTDIR}/package.json" >/dev/null
else
    echo "using host WASIX compiler: ${POSIXTESTSUITE_HOST_CC_BIN}"
    echo "using host WASIX sysroot: ${POSIXTESTSUITE_HOST_SYSROOT}"
fi
if [ -f "${BUSYBOX_OUTDIR}/package.json" ]; then
    "${SINOMO_CMD[@]}" sandbox toolchain verify-package "${BUSYBOX_OUTDIR}/package.json" >/dev/null
    HAVE_BASE_TOOLCHAIN=1
else
    HAVE_BASE_TOOLCHAIN=0
fi
if [ "${POSIXTESTSUITE_RUN_SHELL}" = "1" ] &&
    [ "${#SH_TESTS[@]}" -gt 0 ] &&
    [ "${HAVE_BASE_TOOLCHAIN}" = "1" ] &&
    [ -f "${BASH_OUTDIR}/package.json" ]; then
    "${SINOMO_CMD[@]}" sandbox toolchain verify-package "${BASH_OUTDIR}/package.json" >/dev/null
    HAVE_SHELL_TOOLCHAIN=1
else
    HAVE_SHELL_TOOLCHAIN=0
fi

echo "using disposable SINOMO_DATA_DIR=${SINOMO_VERIFY_DATA_DIR}"
echo "sandbox limits: memory=${SANDBOX_MEMORY_MB}MiB wall=${SANDBOX_MAX_WALL_TIME_MS}ms input=${SANDBOX_MAX_INPUT_BYTES}B output=${SANDBOX_MAX_OUTPUT_BYTES}B workspace=${SANDBOX_MAX_WORKSPACE_BYTES}B written-per-command=${SANDBOX_MAX_WRITTEN_BYTES_PER_COMMAND}B files=${SANDBOX_MAX_FILE_COUNT}"
echo "host limits: cgroup-memory=${SINOMO_HOST_MEMORY_MAX} cgroup-tasks=${SINOMO_HOST_TASKS_MAX} address-space=${SINOMO_HOST_AS_LIMIT_BYTES}"
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox policy set \
    --allow-unsigned-toolchains \
    --max-memory-mb "${SANDBOX_MEMORY_MB}" \
    --max-wall-time-ms "${SANDBOX_MAX_WALL_TIME_MS}" \
    --max-input-bytes "${SANDBOX_MAX_INPUT_BYTES}" \
    --max-output-bytes "${SANDBOX_MAX_OUTPUT_BYTES}" \
    --max-workspace-bytes "${SANDBOX_MAX_WORKSPACE_BYTES}" \
    --max-written-bytes-per-command "${SANDBOX_MAX_WRITTEN_BYTES_PER_COMMAND}" \
    --max-file-count "${SANDBOX_MAX_FILE_COUNT}" \
    --allow-capability child-process-exec \
    --allow-capability wasix.unpinned-child-exec \
    --allow-toolchain "${TOOLCHAIN_ID}" >/dev/null 2>&1 || true

if [ "${POSIXTESTSUITE_COMPILE_MODE}" = "sandbox" ]; then
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox toolchain import-package --into "${TOOLCHAIN_ID}" "${CLANG_OUTDIR}/package.json" >/dev/null
fi
if [ "${HAVE_BASE_TOOLCHAIN}" = "1" ]; then
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox toolchain import-package --into "${TOOLCHAIN_ID}" "${BUSYBOX_OUTDIR}/package.json" >/dev/null
fi
if [ "${HAVE_SHELL_TOOLCHAIN}" = "1" ]; then
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox toolchain import-package --into "${TOOLCHAIN_ID}" "${BASH_OUTDIR}/package.json" >/dev/null
fi
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox create \
    --id "${SANDBOX_ID}" \
    --toolchain "${TOOLCHAIN_ID}" \
    --memory-mb "${SANDBOX_MEMORY_MB}" \
    --wall-time-ms "${SANDBOX_MAX_WALL_TIME_MS}" \
    --max-input-bytes "${SANDBOX_MAX_INPUT_BYTES}" \
    --max-output-bytes "${SANDBOX_MAX_OUTPUT_BYTES}" \
    --max-workspace-bytes "${SANDBOX_MAX_WORKSPACE_BYTES}" \
    --max-written-bytes-per-command "${SANDBOX_MAX_WRITTEN_BYTES_PER_COMMAND}" \
    --max-file-count "${SANDBOX_MAX_FILE_COUNT}" >/dev/null

for sandbox_env in "${POSIXTESTSUITE_SANDBOX_ENV[@]}"; do
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox config "${SANDBOX_ID}" --env "${sandbox_env}" >/dev/null
done

env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox write "${SANDBOX_ID}" /work/posixtestsuite/include/posixtest.h \
    --file "${POSIXTESTSUITE_WORKDIR}/include/posixtest.h" >/dev/null
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox write "${SANDBOX_ID}" /work/posixtestsuite/out/.keep "" >/dev/null

echo "running POSIX C tests: ${#C_TESTS[@]}"
test_index=0
for test_path in "${C_TESTS[@]}"; do
    test_index=$((test_index + 1))
    echo "C ${test_index}/${#C_TESTS[@]} ${test_path}"
    source_path="${POSIXTESTSUITE_WORKDIR}/${test_path}"
    test_name="$(sanitize_test_name "${test_path}")"
    guest_source="/work/posixtestsuite/${test_path}"
    guest_wasm="/work/posixtestsuite/out/${test_name}.wasm"
    guest_wrapper="/work/posixtestsuite/out/${test_name}.compile-wrapper.c"
    host_wasm="${SINOMO_VERIFY_TMPDIR}/${test_name}.wasm"
    host_wrapper="${SINOMO_VERIFY_TMPDIR}/${test_name}.compile-wrapper.c"
    log_prefix="${SINOMO_VERIFY_TMPDIR}/posixtestsuite-logs/${test_name}"

    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox write "${SANDBOX_ID}" "${guest_source}" --file "${source_path}" >/dev/null
    stage_c_test_companions "${test_path}"
    if rg -q '\bmain[[:space:]]*\(|^[[:space:]]*#[[:space:]]+define[[:space:]]+STD_MAIN\b|#include[[:space:]]+"threads_scenarii\.c"' "${source_path}"; then
        if [ "${POSIXTESTSUITE_COMPILE_MODE}" = "host" ]; then
            if "${POSIXTESTSUITE_HOST_CC_BIN}" \
                --target=wasm32-wasi \
                --sysroot="${POSIXTESTSUITE_HOST_SYSROOT}" \
                -matomics \
                -mbulk-memory \
                -mmutable-globals \
                -pthread \
                -mthread-model posix \
                -D_WASI_EMULATED_MMAN \
                "${source_path}" \
                "${POSIXTESTSUITE_EXTRA_CFLAGS[@]}" \
                -I"${POSIXTESTSUITE_WORKDIR}/include" \
                -I"${POSIXTESTSUITE_WORKDIR}/${test_path%/*}" \
                -lwasi-emulated-mman \
                -Wl,--export-all \
                -Wl,--export-dynamic \
                -Wl,--shared-memory \
                -Wl,--import-memory \
                -Wl,--extra-features=atomics \
                -Wl,--extra-features=bulk-memory \
                -Wl,--extra-features=mutable-globals \
                -Wl,--export=__wasm_call_ctors \
                -Wl,--export=__wasm_init_tls \
                -Wl,--export=__wasm_signal \
                -Wl,--export-if-defined=__indirect_function_table \
                -Wl,--export-if-defined=__stack_pointer \
                -Wl,--export-if-defined=__heap_base \
                -Wl,--export-if-defined=__data_end \
                -Wl,-z,stack-size=1048576 \
                -Wl,--initial-memory=67108864 \
                -Wl,--max-memory=268435456 \
                -o "${host_wasm}" \
                >"${log_prefix}.build.stdout" \
                2>"${log_prefix}.build.stderr"; then
                write_result "${test_path}" "compile" "PASS"
                write_result "${test_path}" "link" "PASS"
            else
                detail="$(last_detail "${log_prefix}.build.stderr")"
                write_result "${test_path}" "compile" "FAIL" "${detail}"
                write_result "${test_path}" "link" "FAIL" "${detail}"
                continue
            fi
        elif run_sandbox_capture "${log_prefix}.build" sandbox run "${SANDBOX_ID}" \
            --timeout-ms "${LINK_TIMEOUT_MS}" \
            --allow-capability child-process-exec \
            --allow-capability wasix.unpinned-child-exec \
            clang -- "${guest_source}" "${POSIXTESTSUITE_EXTRA_CFLAGS[@]}" -I/work/posixtestsuite/include -I"/work/posixtestsuite/${test_path%/*}" -o "${guest_wasm}"; then
            write_result "${test_path}" "compile" "PASS"
            write_result "${test_path}" "link" "PASS"
        else
            detail="$(last_detail "${log_prefix}.build.guest.stderr" "${log_prefix}.build.stderr")"
            write_result "${test_path}" "compile" "FAIL" "${detail}"
            write_result "${test_path}" "link" "FAIL" "${detail}"
            continue
        fi

        if [ "${POSIXTESTSUITE_COMPILE_MODE}" = "sandbox" ]; then
            env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox cat "${SANDBOX_ID}" "${guest_wasm}" >"${host_wasm}"
        fi
        if run_sandbox_capture "${log_prefix}.run" sandbox run "${SANDBOX_ID}" \
            --timeout-ms "${RUN_TIMEOUT_MS}" \
            --wasm "${host_wasm}"; then
            write_result "${test_path}" "run" "PASS"
        else
            guest_status="$(captured_exit_status "${log_prefix}.run.stdout")"
            if [ -n "${guest_status}" ]; then
                result="$(posix_result_name "${guest_status}")"
                write_result "${test_path}" "run" "${result}" "exit status ${guest_status}"
            else
                detail="$(last_detail "${log_prefix}.run.guest.stderr" "${log_prefix}.run.stderr")"
                write_result "${test_path}" "run" "FAIL" "${detail}"
            fi
        fi
    else
        cp "${source_path}" "${host_wrapper}"
        printf '\nint main(void) { return 0; }\n' >>"${host_wrapper}"
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox write "${SANDBOX_ID}" "${guest_wrapper}" --file "${host_wrapper}" >/dev/null
        if [ "${POSIXTESTSUITE_COMPILE_MODE}" = "host" ]; then
            if "${POSIXTESTSUITE_HOST_CC_BIN}" \
                --target=wasm32-wasi \
                --sysroot="${POSIXTESTSUITE_HOST_SYSROOT}" \
                -matomics \
                -mbulk-memory \
                -mmutable-globals \
                -pthread \
                -mthread-model posix \
                -D_WASI_EMULATED_MMAN \
                "${host_wrapper}" \
                "${POSIXTESTSUITE_EXTRA_CFLAGS[@]}" \
                -I"${POSIXTESTSUITE_WORKDIR}/include" \
                -I"${POSIXTESTSUITE_WORKDIR}/${test_path%/*}" \
                -lwasi-emulated-mman \
                -Wl,--export-all \
                -Wl,--export-dynamic \
                -Wl,--shared-memory \
                -Wl,--import-memory \
                -Wl,--extra-features=atomics \
                -Wl,--extra-features=bulk-memory \
                -Wl,--extra-features=mutable-globals \
                -Wl,--export=__wasm_call_ctors \
                -Wl,--export=__wasm_init_tls \
                -Wl,--export=__wasm_signal \
                -Wl,--export-if-defined=__indirect_function_table \
                -Wl,--export-if-defined=__stack_pointer \
                -Wl,--export-if-defined=__heap_base \
                -Wl,--export-if-defined=__data_end \
                -Wl,-z,stack-size=1048576 \
                -Wl,--initial-memory=67108864 \
                -Wl,--max-memory=268435456 \
                -o "${host_wasm}" \
                >"${log_prefix}.build.stdout" \
                2>"${log_prefix}.build.stderr"; then
                write_result "${test_path}" "compile" "PASS"
                write_result "${test_path}" "link" "PASS"
            else
                detail="$(last_detail "${log_prefix}.build.stderr")"
                write_result "${test_path}" "compile" "FAIL" "${detail}"
                write_result "${test_path}" "link" "FAIL" "${detail}"
                continue
            fi
        elif run_sandbox_capture "${log_prefix}.build" sandbox run "${SANDBOX_ID}" \
            --timeout-ms "${LINK_TIMEOUT_MS}" \
            --allow-capability child-process-exec \
            --allow-capability wasix.unpinned-child-exec \
            clang -- "${guest_wrapper}" "${POSIXTESTSUITE_EXTRA_CFLAGS[@]}" -I/work/posixtestsuite/include -I"/work/posixtestsuite/${test_path%/*}" -o "${guest_wasm}"; then
            write_result "${test_path}" "compile" "PASS"
            write_result "${test_path}" "link" "PASS"
        else
            detail="$(last_detail "${log_prefix}.build.guest.stderr" "${log_prefix}.build.stderr")"
            write_result "${test_path}" "compile" "FAIL" "${detail}"
            write_result "${test_path}" "link" "FAIL" "${detail}"
            continue
        fi
        write_result "${test_path}" "run" "SKIP" "compile-only definitions test"
    fi
done

echo "running POSIX shell tests: ${#SH_TESTS[@]}"
test_index=0
for test_path in "${SH_TESTS[@]}"; do
    test_index=$((test_index + 1))
    echo "sh ${test_index}/${#SH_TESTS[@]} ${test_path}"
    source_path="${POSIXTESTSUITE_WORKDIR}/${test_path}"
    test_name="$(sanitize_test_name "${test_path}")"
    guest_source="/work/posixtestsuite/${test_path}"
    log_prefix="${SINOMO_VERIFY_TMPDIR}/posixtestsuite-logs/${test_name}"

    if [ "${HAVE_SHELL_TOOLCHAIN}" != "1" ]; then
        write_result "${test_path}" "run" "UNSUPPORTED" "shell toolchain package not available"
        continue
    fi
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox write "${SANDBOX_ID}" "${guest_source}" --file "${source_path}" >/dev/null
    if run_sandbox_capture "${log_prefix}.sh" sandbox run "${SANDBOX_ID}" \
        --timeout-ms "${SH_TIMEOUT_MS}" \
        --allow-capability child-process-exec \
        --allow-capability wasix.unpinned-child-exec \
        sh -- "${guest_source}"; then
        write_result "${test_path}" "run" "PASS"
    else
        guest_status="$(captured_exit_status "${log_prefix}.sh.stdout")"
        if [ -n "${guest_status}" ]; then
            result="$(posix_result_name "${guest_status}")"
            write_result "${test_path}" "run" "${result}" "exit status ${guest_status}"
        else
            detail="$(last_detail "${log_prefix}.sh.guest.stderr" "${log_prefix}.sh.stderr")"
            write_result "${test_path}" "run" "FAIL" "${detail}"
        fi
    fi
done

awk -F '\t' '
NR == 1 { next }
{
    key = $2 "\t" $3
    counts[key]++
    total[$2]++
}
END {
    print "results: " FILENAME
    for (phase in total) {
        printf "%s total %d\n", phase, total[phase]
    }
    for (key in counts) {
        split(key, parts, "\t")
        printf "%s %s %d\n", parts[1], parts[2], counts[key]
    }
}' "${RESULTS_FILE}" | sort >"${SUMMARY_FILE}"

cat "${SUMMARY_FILE}"
cp "${RESULTS_FILE}" "${LATEST_RESULTS_FILE}"
cp "${SUMMARY_FILE}" "${LATEST_SUMMARY_FILE}"
echo "POSIX test suite WASIX run complete (${POSIXTESTSUITE_SCOPE}, ${POSIXTESTSUITE_REV})"
echo "results: ${RESULTS_FILE}"
