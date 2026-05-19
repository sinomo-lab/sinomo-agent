#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
TARGET_DIR="${TARGET_DIR:-"${REPO_ROOT}/target"}"
TMPDIR="${TMPDIR:-"${TARGET_DIR}/sinomo-sandbox-host-tmp"}"
OUTDIR="${OUTDIR:-"${ROOT}/out-wasix"}"
DATA_DIR="${DATA_DIR:-"${TARGET_DIR}/sinomo-python-wasix-native-verify-data"}"
WHEELHOUSE="${WHEELHOUSE:-"${TARGET_DIR}/wasix-wheelhouse"}"
SHELL_BUSYBOX_PACKAGE="${SHELL_BUSYBOX_PACKAGE:-"${TARGET_DIR}/sinomo-bash-verify-busybox-wasix-out/package.json"}"
SHELL_BASH_PACKAGE="${SHELL_BASH_PACKAGE:-"${TARGET_DIR}/sinomo-bash-verify-bash-wasix-out/package.json"}"
TOOLCHAIN_ID="${TOOLCHAIN_ID:-01krqz00000000000000000000}"
SANDBOX_ID="${SANDBOX_ID:-01krqz00000000000000000001}"
VERIFY_BACKEND="${VERIFY_BACKEND:-local}"
UPDATE_CLUSTER_POLICY="${UPDATE_CLUSTER_POLICY:-0}"
TIMEOUT_MS="${TIMEOUT_MS:-900000}"
MEMORY_MB="${MEMORY_MB:-4096}"
KEEP_SANDBOX="${KEEP_SANDBOX:-0}"
DAEMON_PORT="${DAEMON_PORT:-21455}"
DAEMON_STARTED=0

if [ -n "${SINOMO_BIN:-}" ]; then
    SINOMO_CMD=("${SINOMO_BIN}")
else
    SINOMO_CMD=(cargo run --quiet --manifest-path "${REPO_ROOT}/Cargo.toml" --features wasix-runtime --bin sinomo --)
fi

PACKAGE="${OUTDIR}/package.json"
RUN_ENV=(TMPDIR="${TMPDIR}" SINOMO_DATA_DIR="${DATA_DIR}")
RUN_CAPS=(--allow-capability child-process-exec --allow-capability wasix.unpinned-child-exec)
RUN_DAEMON_ARGS=()
WRITE_DAEMON_ARGS=()
CAT_DAEMON_ARGS=()
CHECKPOINT_DAEMON_ARGS=()
RESUME_DAEMON_ARGS=()

case "${VERIFY_BACKEND}" in
    local|daemon|daemon-local) ;;
    *)
        echo "VERIFY_BACKEND must be local, daemon, or daemon-local, got: ${VERIFY_BACKEND}" >&2
        exit 1
        ;;
esac

cleanup() {
    if [ "${KEEP_SANDBOX}" != "1" ]; then
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox destroy "${SANDBOX_ID}" "${RUN_DAEMON_ARGS[@]}" >/dev/null 2>&1 || true
    fi
    if [ "${DAEMON_STARTED}" = "1" ]; then
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" daemon stop >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

require_file() {
    local path="$1"
    if [ ! -f "${path}" ]; then
        echo "required file not found: ${path}" >&2
        exit 1
    fi
}

require_wheel() {
    local pattern="$1"
    if ! find "${WHEELHOUSE}" -maxdepth 1 -type f -name "${pattern}" | grep -q .; then
        echo "required WASIX wheel missing from ${WHEELHOUSE}: ${pattern}" >&2
        exit 1
    fi
}

run_python() {
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
        sandbox run "${SANDBOX_ID}" \
        "${RUN_DAEMON_ARGS[@]}" \
        "${RUN_CAPS[@]}" \
        python -- "$@"
}

run_python_with_timeout() {
    local timeout_ms="$1"
    shift
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
        sandbox run "${SANDBOX_ID}" \
        "${RUN_DAEMON_ARGS[@]}" \
        --timeout-ms "${timeout_ms}" \
        "${RUN_CAPS[@]}" \
        python -- "$@"
}

run_pip() {
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
        sandbox run "${SANDBOX_ID}" \
        "${RUN_DAEMON_ARGS[@]}" \
        "${RUN_CAPS[@]}" \
        pip -- "$@"
}

write_sandbox_file() {
    local sandbox_path="$1"
    local host_path="$2"
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
        sandbox write "${SANDBOX_ID}" "${sandbox_path}" "${WRITE_DAEMON_ARGS[@]}" --file "${host_path}" >/dev/null
}

cat_sandbox_file() {
    local sandbox_path="$1"
    local output_path="$2"
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
        sandbox cat "${SANDBOX_ID}" "${sandbox_path}" "${CAT_DAEMON_ARGS[@]}" >"${output_path}"
}

mkdir -p "${TMPDIR}"

if [ "${FORCE_BUILD:-0}" = "1" ] || [ ! -f "${PACKAGE}" ]; then
    echo "building CPython WASIX package"
    WORKDIR="${WORKDIR:-"${TARGET_DIR}/sinomo-python-wasix-build-work"}" \
        OUTDIR="${OUTDIR}" \
        TMPDIR="${TMPDIR}" \
        "${ROOT}/build-cpython-wasix.sh"
fi

require_file "${PACKAGE}"
require_file "${SHELL_BUSYBOX_PACKAGE}"
require_file "${SHELL_BASH_PACKAGE}"
if [ ! -d "${WHEELHOUSE}" ]; then
    echo "WASIX wheelhouse not found: ${WHEELHOUSE}" >&2
    exit 1
fi
require_wheel 'numpy-*-cp313-cp313-wasix_wasm32.whl'
require_wheel 'matplotlib-*-cp313-cp313-wasix_wasm32.whl'

if [ -d "${DATA_DIR}" ]; then
    chmod -R u+rwX "${DATA_DIR}" 2>/dev/null || true
fi
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"

echo "verifying CPython WASIX package manifest"
env TMPDIR="${TMPDIR}" "${SINOMO_CMD[@]}" sandbox toolchain verify-package "${PACKAGE}" >/dev/null
env TMPDIR="${TMPDIR}" "${SINOMO_CMD[@]}" sandbox toolchain verify-package "${SHELL_BUSYBOX_PACKAGE}" >/dev/null
env TMPDIR="${TMPDIR}" "${SINOMO_CMD[@]}" sandbox toolchain verify-package "${SHELL_BASH_PACKAGE}" >/dev/null

if [ "${VERIFY_BACKEND}" = "local" ]; then
    echo "using disposable local SINOMO_DATA_DIR=${DATA_DIR}"
    echo "importing WASIX shell support packages"
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
        sandbox toolchain import-package --into "${TOOLCHAIN_ID}" "${SHELL_BUSYBOX_PACKAGE}" >/dev/null
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
        sandbox toolchain import-package --into "${TOOLCHAIN_ID}" "${SHELL_BASH_PACKAGE}" >/dev/null
    echo "importing CPython WASIX package"
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
        sandbox toolchain import-package --into "${TOOLCHAIN_ID}" "${PACKAGE}" >/dev/null

    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
        sandbox create \
        --id "${SANDBOX_ID}" \
        --toolchain "${TOOLCHAIN_ID}" \
        --memory-mb "${MEMORY_MB}" \
        --wall-time-ms 900000 \
        --max-workspace-bytes 1073741824 \
        --max-written-bytes-per-command 1073741824 >/dev/null
else
    echo "using daemon/SIFS backend"
    RUN_DAEMON_ARGS=(--daemon)
    WRITE_DAEMON_ARGS=(--daemon)
    CAT_DAEMON_ARGS=(--daemon)
    CHECKPOINT_DAEMON_ARGS=(--daemon)
    RESUME_DAEMON_ARGS=(--daemon)
    RUN_CAPS=()
    if [ "${VERIFY_BACKEND}" = "daemon-local" ]; then
        echo "using disposable daemon SINOMO_DATA_DIR=${DATA_DIR} port=${DAEMON_PORT}"
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" cluster init \
            --name sandbox-python-wasix-native-verify \
            --cluster-id 01krqz00000000000000000003 \
            --node-id 01krqz00000000000000000004 >/dev/null
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
    fi
    if [ "${UPDATE_CLUSTER_POLICY}" = "1" ]; then
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox policy set \
            --allow-unsigned-toolchains \
            --allow-capability child-process-exec \
            --allow-capability wasix.unpinned-child-exec \
            --allow-toolchain "${TOOLCHAIN_ID}" \
            --allow-toolchain-provenance-prefix "Sinomo BusyBox WASIX recipe:" \
            --allow-toolchain-provenance-prefix "Sinomo Bash WASIX recipe:" \
            --allow-toolchain-provenance-prefix "Sinomo CPython WASIX recipe:" \
            --allow-package-cache-prefix /services/sandbox/toolchains \
            --max-memory-mb "${MEMORY_MB}" \
            --max-wall-time-ms 900000 \
            --max-workspace-bytes 1073741824 \
            --max-written-bytes-per-command 1073741824 >/dev/null
    fi
    echo "importing WASIX shell support packages through daemon/SIFS"
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
        sandbox toolchain import-package --daemon --into "${TOOLCHAIN_ID}" "${SHELL_BUSYBOX_PACKAGE}" >/dev/null
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
        sandbox toolchain import-package --daemon --into "${TOOLCHAIN_ID}" "${SHELL_BASH_PACKAGE}" >/dev/null
    echo "importing CPython WASIX package through daemon/SIFS"
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
        sandbox toolchain import-package --daemon --into "${TOOLCHAIN_ID}" "${PACKAGE}" >/dev/null

    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
        sandbox create --daemon \
        --id "${SANDBOX_ID}" \
        --toolchain "${TOOLCHAIN_ID}" \
        --memory-mb "${MEMORY_MB}" \
        --wall-time-ms 900000 \
        --max-workspace-bytes 1073741824 \
        --max-written-bytes-per-command 1073741824 >/dev/null
fi

echo "verifying real python command"
PYTHON_OUTPUT="$(run_python -c "print('sinomo python wasix')")"
if ! grep -qx "sinomo python wasix" <<<"${PYTHON_OUTPUT}"; then
    echo "python command did not produce expected output" >&2
    echo "${PYTHON_OUTPUT}" >&2
    exit 1
fi

echo "verifying python subprocess sandbox-local WASM exec"
PYTHON_WASM="${OUTDIR}/python-3.13.13-wasix.wasm"
require_file "${PYTHON_WASM}"
write_sandbox_file /work/python-child "${PYTHON_WASM}"
PYTHON_CHILD_OUTPUT="$(
    run_python -c "import os, subprocess; os.chmod('/work/python-child', 0o755); result = subprocess.run(['/work/python-child', '-c', 'print(\"child-ok\")'], check=True, text=True, capture_output=True); print(result.stdout.strip())"
)"
if ! grep -qx "child-ok" <<<"${PYTHON_CHILD_OUTPUT}"; then
    echo "python subprocess did not execute sandbox-local WASM child" >&2
    echo "${PYTHON_CHILD_OUTPUT}" >&2
    exit 1
fi

echo "verifying python subprocess POSIX process semantics"
PYTHON_PROCESS_OUTPUT="$(
    run_python -c "import os, subprocess, sys; os.makedirs('/work/subprocess-cwd', exist_ok=True); os.chmod('/work/python-child', 0o755); stdin_result = subprocess.run(['/work/python-child', '-c', 'import sys; print(sys.stdin.read().upper())'], input='child-stdin', text=True, capture_output=True, check=True); cwd_result = subprocess.run(['/work/python-child', '-c', 'import os; print(os.getcwd())'], cwd='/work/subprocess-cwd', text=True, capture_output=True, check=True); env_result = subprocess.run(['/work/python-child', '-c', 'import os; print(os.environ[\"SINOMO_CHILD_ENV\"])'], env={**os.environ, 'SINOMO_CHILD_ENV': 'child-env-ok'}, text=True, capture_output=True, check=True); stderr_result = subprocess.run(['/work/python-child', '-c', 'import sys; print(\"child-err\", file=sys.stderr)'], text=True, capture_output=True, check=True); status_result = subprocess.run(['/work/python-child', '-c', 'import sys; sys.exit(7)'], text=True, capture_output=True); print(stdin_result.stdout.strip()); print(cwd_result.stdout.strip()); print(env_result.stdout.strip()); print(stderr_result.stderr.strip()); print(status_result.returncode)"
)"
for expected in "CHILD-STDIN" "/work/subprocess-cwd" "child-env-ok" "child-err" "7"; do
    if ! grep -qx -- "${expected}" <<<"${PYTHON_PROCESS_OUTPUT}"; then
        echo "python subprocess process semantics missing expected output: ${expected}" >&2
        echo "${PYTHON_PROCESS_OUTPUT}" >&2
        exit 1
    fi
done

echo "verifying python Popen communicate file descriptor behavior"
PYTHON_POPEN_OUTPUT="$(
    run_python -c "import os, subprocess; os.chmod('/work/python-child', 0o755); p = subprocess.Popen(['/work/python-child', '-c', 'import sys; data=sys.stdin.read(); print(data.upper()); print(\"child-err\", file=sys.stderr)'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True); out, err = p.communicate('abc'); print(p.returncode); print(out.strip()); print(err.strip())"
)"
for expected in "0" "ABC" "child-err"; do
    if ! grep -qx -- "${expected}" <<<"${PYTHON_POPEN_OUTPUT}"; then
        echo "python Popen communicate missing expected output: ${expected}" >&2
        echo "${PYTHON_POPEN_OUTPUT}" >&2
        exit 1
    fi
done

echo "verifying python waitpid signal status encoding"
PYTHON_WAITPID_SIGNAL_OUTPUT="$(
    run_python -c "import os, signal, subprocess; os.chmod('/work/python-child', 0o755); p = subprocess.Popen(['/work/python-child', '-c', 'while True: pass']); p.kill(); pid, status = os.waitpid(p.pid, 0); print(pid == p.pid); print(os.WIFSIGNALED(status)); print(os.WTERMSIG(status)); print(os.waitstatus_to_exitcode(status))"
)"
for expected in "True" "True" "9" "-9"; do
    if ! grep -qx -- "${expected}" <<<"${PYTHON_WAITPID_SIGNAL_OUTPUT}"; then
        echo "python waitpid signal status missing expected output: ${expected}" >&2
        echo "${PYTHON_WAITPID_SIGNAL_OUTPUT}" >&2
        exit 1
    fi
done

echo "verifying python Popen wait timeout semantics"
PYTHON_POPEN_WAIT_OUTPUT="$(
    run_python -c "import os, subprocess; os.chmod('/work/python-child', 0o755); p = subprocess.Popen(['/work/python-child', '-c', 'while True: pass']);
try:
    p.wait(timeout=1)
except subprocess.TimeoutExpired:
    print('popen-wait-timeout-ok')
finally:
    p.kill()
    try:
        p.wait(timeout=5)
    except subprocess.TimeoutExpired:
        print('popen-kill-timeout')"
)"
if ! grep -qx "popen-wait-timeout-ok" <<<"${PYTHON_POPEN_WAIT_OUTPUT}"; then
    echo "python Popen.wait(timeout=...) did not time out for a running child" >&2
    echo "${PYTHON_POPEN_WAIT_OUTPUT}" >&2
    exit 1
fi
if grep -qx "popen-kill-timeout" <<<"${PYTHON_POPEN_WAIT_OUTPUT}"; then
    echo "python Popen.kill() did not terminate the running child" >&2
    echo "${PYTHON_POPEN_WAIT_OUTPUT}" >&2
    exit 1
fi

echo "verifying python subprocess timeout semantics"
PYTHON_SUBPROCESS_TIMEOUT_OUTPUT="$(
    run_python -c "import os, subprocess; os.chmod('/work/python-child', 0o755); cases = ({}, {'capture_output': True});
for kwargs in cases:
    try:
        subprocess.run(['/work/python-child', '-c', 'while True: pass'], timeout=1, text=True, **kwargs)
    except subprocess.TimeoutExpired:
        print('timeout-ok', bool(kwargs))
    else:
        print('timeout-missing', bool(kwargs))"
)"
for expected in "timeout-ok False" "timeout-ok True"; do
    if ! grep -qx -- "${expected}" <<<"${PYTHON_SUBPROCESS_TIMEOUT_OUTPUT}"; then
        echo "python subprocess timeout missing expected output: ${expected}" >&2
        echo "${PYTHON_SUBPROCESS_TIMEOUT_OUTPUT}" >&2
        exit 1
    fi
done

echo "verifying python subprocess shell=True through pinned /bin/sh"
PYTHON_SHELL_OUTPUT="$(
    run_python -c "import subprocess; result = subprocess.run('printf shell-ok', shell=True, text=True, capture_output=True, check=True); print(result.stdout.strip())"
)"
if ! grep -qx "shell-ok" <<<"${PYTHON_SHELL_OUTPUT}"; then
    echo "python subprocess shell=True did not execute through pinned /bin/sh" >&2
    echo "${PYTHON_SHELL_OUTPUT}" >&2
    exit 1
fi

echo "verifying python runaway compute is bounded"
set +e
PYTHON_TIMEOUT_OUTPUT="$(run_python_with_timeout 1000 -c "while True: pass" 2>&1)"
PYTHON_TIMEOUT_STATUS=$?
set -e
if [ "${PYTHON_TIMEOUT_STATUS}" -eq 0 ]; then
    echo "python runaway loop unexpectedly completed" >&2
    echo "${PYTHON_TIMEOUT_OUTPUT}" >&2
    exit 1
fi
if ! grep -qi "timed out\\|timeout" <<<"${PYTHON_TIMEOUT_OUTPUT}"; then
    echo "python runaway loop failed without a timeout diagnostic" >&2
    echo "${PYTHON_TIMEOUT_OUTPUT}" >&2
    exit 1
fi

echo "verifying real pip command"
PIP_OUTPUT="$(run_pip --version)"
if ! grep -q "pip " <<<"${PIP_OUTPUT}"; then
    echo "pip command did not report a version" >&2
    echo "${PIP_OUTPUT}" >&2
    exit 1
fi

run_python -c "from pathlib import Path; Path('/work/wheelhouse').mkdir(parents=True, exist_ok=True); Path('/work/native-site').mkdir(parents=True, exist_ok=True)" >/dev/null

echo "seeding WASIX wheelhouse into sandbox"
while IFS= read -r wheel; do
    name="$(basename "${wheel}")"
    write_sandbox_file "/work/wheelhouse/${name}" "${wheel}"
done < <(find "${WHEELHOUSE}" -maxdepth 1 -type f -name '*.whl' | sort)

echo "installing numpy and matplotlib with real pip"
run_pip install \
    --no-index \
    --find-links /work/wheelhouse \
    --target /work/native-site \
    --force-reinstall \
    numpy matplotlib >/dev/null

echo "verifying native package tree survives checkpoint and resume"
CHECKPOINT_ID="$(
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
        sandbox checkpoint "${SANDBOX_ID}" "${CHECKPOINT_DAEMON_ARGS[@]}" |
    awk '/^checkpoint:/ { print $2; exit }'
)"
if [ -z "${CHECKPOINT_ID}" ]; then
    echo "failed to create CPython WASIX native verification checkpoint" >&2
    exit 1
fi
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" \
    sandbox resume "${SANDBOX_ID}" "${CHECKPOINT_ID}" "${RESUME_DAEMON_ARGS[@]}" >/dev/null

echo "verifying numpy native extension import and execution"
NUMPY_OUTPUT="$(
    run_python -c "import sys; sys.path.insert(0, '/work/native-site'); import numpy as np; print(np.__version__); print((np.arange(4) ** 2).tolist())"
)"
if ! grep -Fq "[0, 1, 4, 9]" <<<"${NUMPY_OUTPUT}"; then
    echo "numpy did not execute the expected array operation" >&2
    echo "${NUMPY_OUTPUT}" >&2
    exit 1
fi

echo "verifying matplotlib native dependency import and Agg render"
MPL_OUTPUT="$(
    run_python -c "import sys; sys.path.insert(0, '/work/native-site'); import matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt; print(matplotlib.__version__); plt.plot([1, 2, 3], [1, 4, 9]); plt.savefig('/work/matplotlib-wasix-native-gate.png'); print('saved')"
)"
if ! grep -Fq "saved" <<<"${MPL_OUTPUT}"; then
    echo "matplotlib did not report a saved render" >&2
    echo "${MPL_OUTPUT}" >&2
    exit 1
fi

RENDERED="${DATA_DIR}/sandbox/runtime/instances/${SANDBOX_ID}/work/matplotlib-wasix-native-gate.png"
if [[ "${VERIFY_BACKEND}" == daemon* ]]; then
    RENDERED="${TMPDIR}/matplotlib-wasix-native-gate-${SANDBOX_ID}.png"
    cat_sandbox_file /work/matplotlib-wasix-native-gate.png "${RENDERED}"
else
    require_file "${RENDERED}"
fi
RENDERED_SIZE="$(wc -c <"${RENDERED}")"
if [ "${RENDERED_SIZE}" -le 1000 ]; then
    echo "matplotlib render is unexpectedly small: ${RENDERED_SIZE} bytes" >&2
    exit 1
fi

echo "CPython WASIX native package gate passed"
echo "rendered: ${RENDERED} (${RENDERED_SIZE} bytes)"
