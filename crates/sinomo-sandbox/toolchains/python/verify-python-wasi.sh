#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
TARGET_DIR="${TARGET_DIR:-"${REPO_ROOT}/target"}"
WORKDIR="${WORKDIR:-"${TARGET_DIR}/sinomo-python-verify-work"}"
OUTDIR="${OUTDIR:-"${TARGET_DIR}/sinomo-python-verify-out"}"
DATA_DIR="${DATA_DIR:-"${TARGET_DIR}/sinomo-python-verify-data"}"
VERIFY_BACKEND="${VERIFY_BACKEND:-local}"
SANDBOX_ID="${SANDBOX_ID:-}"
TOOLCHAIN_ID="${TOOLCHAIN_ID:-01k4z8q5c00000000000000000}"
UPDATE_CLUSTER_POLICY="${UPDATE_CLUSTER_POLICY:-0}"
KEEP_SANDBOX="${KEEP_SANDBOX:-0}"
DAEMON_PORT="${DAEMON_PORT:-21453}"
PYTHON_INDEX_PORT="${PYTHON_INDEX_PORT:-21454}"
FETCH_TIMEOUT_MS="${FETCH_TIMEOUT_MS:-120000}"
DAEMON_STARTED=0
HTTP_SERVER_PID=""
RUN_ENV=()

if [ -n "${SINOMO_BIN:-}" ]; then
    SINOMO_CMD=("${SINOMO_BIN}")
else
    SINOMO_CMD=(cargo run --quiet --manifest-path "${REPO_ROOT}/Cargo.toml" --bin sinomo --)
fi

case "${VERIFY_BACKEND}" in
    local|daemon|daemon-local) ;;
    *)
        echo "VERIFY_BACKEND must be local, daemon, or daemon-local, got: ${VERIFY_BACKEND}" >&2
        exit 1
        ;;
esac

cleanup() {
    if [ -n "${HTTP_SERVER_PID}" ]; then
        kill "${HTTP_SERVER_PID}" >/dev/null 2>&1 || true
        wait "${HTTP_SERVER_PID}" >/dev/null 2>&1 || true
    fi
    if [[ "${VERIFY_BACKEND}" == daemon* ]] && [ "${KEEP_SANDBOX}" != "1" ] && [ -n "${SANDBOX_ID}" ]; then
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox destroy "${SANDBOX_ID}" --daemon >/dev/null 2>&1 || true
    fi
    if [ "${DAEMON_STARTED}" = "1" ]; then
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" daemon stop >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

VERIFY_MEDIATED_PIP_INDEX="${VERIFY_MEDIATED_PIP_INDEX:-}"
if [ -z "${VERIFY_MEDIATED_PIP_INDEX}" ]; then
    if [ "${VERIFY_BACKEND}" = "daemon-local" ]; then
        VERIFY_MEDIATED_PIP_INDEX=1
    else
        VERIFY_MEDIATED_PIP_INDEX=0
    fi
fi

if [ "${SKIP_BUILD:-0}" != "1" ]; then
    echo "building CPython WASIp1 package"
    WORKDIR="${WORKDIR}" OUTDIR="${OUTDIR}" "${ROOT}/build-cpython-wasi.sh"
fi

PACKAGE="${OUTDIR}/package.json"
if [ ! -f "${PACKAGE}" ]; then
    echo "python package manifest not found: ${PACKAGE}" >&2
    exit 1
fi

rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"

echo "verifying package manifest"
"${SINOMO_CMD[@]}" sandbox toolchain verify-package "${PACKAGE}" >/dev/null

if [ "${VERIFY_BACKEND}" = "local" ]; then
    if [ -z "${SANDBOX_ID}" ]; then
        SANDBOX_ID="01k4z8q5p00000000000000001"
    fi
    echo "using disposable SINOMO_DATA_DIR=${DATA_DIR}"
    SINOMO_DATA_DIR="${DATA_DIR}" "${SINOMO_CMD[@]}" \
        sandbox toolchain import-package --into "${TOOLCHAIN_ID}" "${PACKAGE}" >/dev/null

    CREATE_OUTPUT="$(
        SINOMO_DATA_DIR="${DATA_DIR}" "${SINOMO_CMD[@]}" \
            sandbox create \
            --id "${SANDBOX_ID}" \
            --toolchain "${TOOLCHAIN_ID}"
    )"
    VERIFY_ARGS=(sandbox toolchain verify-python "${SANDBOX_ID}" --allow-unsigned)
    CHECKPOINT_ARGS=(sandbox checkpoint "${SANDBOX_ID}")
    RESUME_ARGS=(sandbox resume "${SANDBOX_ID}")
    RESUME_EXTRA_ARGS=()
    RUN_ARGS=(sandbox run "${SANDBOX_ID}" --timeout-ms 30000 --allow-capability preview1.pinned-subprocess python --)
    RUN_ENV=(SINOMO_DATA_DIR="${DATA_DIR}")
else
    echo "using daemon/SIFS backend"
    if [ "${VERIFY_BACKEND}" = "daemon-local" ]; then
        echo "using disposable daemon SINOMO_DATA_DIR=${DATA_DIR} port=${DAEMON_PORT}"
        RUN_ENV=(SINOMO_DATA_DIR="${DATA_DIR}")
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" cluster init \
            --name sandbox-python-verify \
            --cluster-id 01k4z8q5c00000000000000003 \
            --node-id 01k4z8q5c00000000000000004 >/dev/null
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
        POLICY_ARGS=(
            sandbox policy set
            --allow-unsigned-toolchains
            --allow-capability preview1.pinned-subprocess
            --allow-toolchain "${TOOLCHAIN_ID}"
            --allow-toolchain-provenance-prefix "Sinomo CPython WASI recipe:"
            --allow-package-cache-prefix /services/sandbox/toolchains
        )
        if [ "${VERIFY_MEDIATED_PIP_INDEX}" = "1" ]; then
            POLICY_ARGS+=(--allow-capability network.http --allow-http-host 127.0.0.1 --max-http-timeout-ms "${FETCH_TIMEOUT_MS}")
        fi
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${POLICY_ARGS[@]}" >/dev/null
    fi
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox toolchain import-package --daemon --into "${TOOLCHAIN_ID}" "${PACKAGE}" >/dev/null
    CREATE_CMD=(sandbox create --daemon --toolchain "${TOOLCHAIN_ID}")
    if [ -n "${SANDBOX_ID}" ]; then
        CREATE_CMD+=(--id "${SANDBOX_ID}")
    fi
    CREATE_OUTPUT="$(env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${CREATE_CMD[@]}")"
    if [ -z "${SANDBOX_ID}" ]; then
        SANDBOX_ID="$(awk '/^sandbox:/ { print $2; exit } /^sandbox created:/ { print $3; exit }' <<<"${CREATE_OUTPUT}")"
    fi
    if [ -z "${SANDBOX_ID}" ]; then
        echo "failed to read daemon-created sandbox id" >&2
        echo "${CREATE_OUTPUT}" >&2
        exit 1
    fi
    VERIFY_ARGS=(sandbox toolchain verify-python "${SANDBOX_ID}" --daemon)
    CHECKPOINT_ARGS=(sandbox checkpoint "${SANDBOX_ID}" --daemon)
    RESUME_ARGS=(sandbox resume "${SANDBOX_ID}")
    RESUME_EXTRA_ARGS=(--daemon)
    RUN_ARGS=(sandbox run "${SANDBOX_ID}" --daemon --timeout-ms 120000 python --)
fi

env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${VERIFY_ARGS[@]}"

CHECKPOINT_ID="$(
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${CHECKPOINT_ARGS[@]}" |
    awk '/^checkpoint:/ { print $2; exit }'
)"
if [ -z "${CHECKPOINT_ID}" ]; then
    echo "failed to create Python verification checkpoint" >&2
    exit 1
fi

env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RESUME_ARGS[@]}" "${CHECKPOINT_ID}" "${RESUME_EXTRA_ARGS[@]}" >/dev/null

RESUME_IMPORT_OUTPUT="$(
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
        -c "import sys; sys.path.insert(0, '/work/site-packages'); import sinomo_pkg; print(sinomo_pkg.value())"
)"
if ! grep -qx "installed package" <<<"${RESUME_IMPORT_OUTPUT}"; then
    echo "installed package import after resume failed" >&2
    echo "${RESUME_IMPORT_OUTPUT}" >&2
    exit 1
fi

RESUME_CACHE_OUTPUT="$(
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
        -c "from pathlib import Path; cache = Path('/work/pip-cache'); files = [path for path in cache.rglob('*.whl') if path.is_file()]; print('pip wheel cache files' if files else 'missing pip wheel cache files')"
)"
if ! grep -qx "pip wheel cache files" <<<"${RESUME_CACHE_OUTPUT}"; then
    echo "pip cache state after resume failed" >&2
    echo "${RESUME_CACHE_OUTPUT}" >&2
    exit 1
fi

if [ "${VERIFY_MEDIATED_PIP_INDEX}" = "1" ]; then
    if [[ "${VERIFY_BACKEND}" != daemon* ]]; then
        echo "VERIFY_MEDIATED_PIP_INDEX=1 requires daemon or daemon-local backend" >&2
        exit 1
    fi
    INDEX_ROOT="${WORKDIR}/mediated-pip-index"
    INDEX_WHEEL="sinomo_index_fixture-0.0.0-py3-none-any.whl"
    DEP_OLD_WHEEL="sinomo_dependency_fixture-0.0.0-py3-none-any.whl"
    DEP_NEW_WHEEL="sinomo_dependency_fixture-0.1.0-py3-none-any.whl"
    DEP_INCOMPATIBLE_WHEEL="sinomo_dependency_fixture-0.2.0-py3-none-any.whl"
    DEP_YANKED_WHEEL="sinomo_dependency_fixture-0.3.0-py3-none-any.whl"
    TRANSITIVE_WHEEL="sinomo_transitive_fixture-0.0.0-py3-none-any.whl"
    EXTRA_WHEEL="sinomo_extra_fixture-0.0.0-py3-none-any.whl"
    NORMALIZED_WHEEL="sinomo_normalized_fixture-0.0.0-py3-none-any.whl"
    rm -rf "${INDEX_ROOT}"
    mkdir -p "${INDEX_ROOT}"
    python3 - "${INDEX_ROOT}" "${INDEX_WHEEL}" "${DEP_OLD_WHEEL}" "${DEP_NEW_WHEEL}" "${DEP_INCOMPATIBLE_WHEEL}" "${DEP_YANKED_WHEEL}" "${TRANSITIVE_WHEEL}" "${EXTRA_WHEEL}" "${NORMALIZED_WHEEL}" <<'PY'
import csv
import io
import sys
import zipfile
from pathlib import Path

root = Path(sys.argv[1])
index_wheel = sys.argv[2]
dep_old_wheel = sys.argv[3]
dep_new_wheel = sys.argv[4]
dep_incompatible_wheel = sys.argv[5]
dep_yanked_wheel = sys.argv[6]
transitive_wheel = sys.argv[7]
extra_wheel = sys.argv[8]
normalized_wheel = sys.argv[9]
packages = root / "packages"
simple = root / "simple"
index_project = simple / "sinomo-index-fixture"
dep_project = simple / "sinomo-dependency-fixture"
transitive_project = simple / "sinomo-transitive-fixture"
extra_project = simple / "sinomo-extra-fixture"
normalized_project = simple / "sinomo-normalized-fixture"
packages.mkdir(parents=True)
index_project.mkdir(parents=True)
dep_project.mkdir(parents=True)
transitive_project.mkdir(parents=True)
extra_project.mkdir(parents=True)
normalized_project.mkdir(parents=True)

def write_wheel(wheel_name, dist, records):
    record_io = io.StringIO()
    writer = csv.writer(record_io)
    for name, content in records:
        writer.writerow([name, "", str(len(content.encode()) if isinstance(content, str) else len(content))])
    writer.writerow([f"{dist}/RECORD", "", ""])
    with zipfile.ZipFile(packages / wheel_name, "w", zipfile.ZIP_DEFLATED) as wheel:
        for name, content in records:
            wheel.writestr(name, content)
        wheel.writestr(f"{dist}/RECORD", record_io.getvalue())

write_wheel(
    dep_old_wheel,
    "sinomo_dependency_fixture-0.0.0.dist-info",
    [
        ("sinomo_dependency_fixture.py", "def value():\n    return 'dependency 0.0.0'\n"),
        ("sinomo_dependency_fixture-0.0.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: sinomo-dependency-fixture\nVersion: 0.0.0\n"),
        ("sinomo_dependency_fixture-0.0.0.dist-info/WHEEL", "Wheel-Version: 1.0\nGenerator: sinomo verifier\nRoot-Is-Purelib: true\nTag: py3-none-any\n"),
    ],
)
write_wheel(
    dep_new_wheel,
    "sinomo_dependency_fixture-0.1.0.dist-info",
    [
        ("sinomo_dependency_fixture.py", "import sinomo_transitive_fixture\n\ndef value():\n    return 'dependency 0.1.0 ' + sinomo_transitive_fixture.value()\n"),
        ("sinomo_dependency_fixture-0.1.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: sinomo-dependency-fixture\nVersion: 0.1.0\nRequires-Dist: sinomo-transitive-fixture==0.0.0\n"),
        ("sinomo_dependency_fixture-0.1.0.dist-info/WHEEL", "Wheel-Version: 1.0\nGenerator: sinomo verifier\nRoot-Is-Purelib: true\nTag: py3-none-any\n"),
    ],
)
write_wheel(
    dep_incompatible_wheel,
    "sinomo_dependency_fixture-0.2.0.dist-info",
    [
        ("sinomo_dependency_fixture.py", "def value():\n    return 'dependency 0.2.0 incompatible'\n"),
        ("sinomo_dependency_fixture-0.2.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: sinomo-dependency-fixture\nVersion: 0.2.0\nRequires-Python: >999\n"),
        ("sinomo_dependency_fixture-0.2.0.dist-info/WHEEL", "Wheel-Version: 1.0\nGenerator: sinomo verifier\nRoot-Is-Purelib: true\nTag: py3-none-any\n"),
    ],
)
write_wheel(
    dep_yanked_wheel,
    "sinomo_dependency_fixture-0.3.0.dist-info",
    [
        ("sinomo_dependency_fixture.py", "def value():\n    return 'dependency 0.3.0 yanked'\n"),
        ("sinomo_dependency_fixture-0.3.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: sinomo-dependency-fixture\nVersion: 0.3.0\n"),
        ("sinomo_dependency_fixture-0.3.0.dist-info/WHEEL", "Wheel-Version: 1.0\nGenerator: sinomo verifier\nRoot-Is-Purelib: true\nTag: py3-none-any\n"),
    ],
)
write_wheel(
    transitive_wheel,
    "sinomo_transitive_fixture-0.0.0.dist-info",
    [
        ("sinomo_transitive_fixture.py", "def value():\n    return 'transitive'\n"),
        ("sinomo_transitive_fixture-0.0.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: sinomo-transitive-fixture\nVersion: 0.0.0\n"),
        ("sinomo_transitive_fixture-0.0.0.dist-info/WHEEL", "Wheel-Version: 1.0\nGenerator: sinomo verifier\nRoot-Is-Purelib: true\nTag: py3-none-any\n"),
    ],
)
write_wheel(
    extra_wheel,
    "sinomo_extra_fixture-0.0.0.dist-info",
    [
        ("sinomo_extra_fixture.py", "def value():\n    return 'extra'\n"),
        ("sinomo_extra_fixture-0.0.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: sinomo-extra-fixture\nVersion: 0.0.0\n"),
        ("sinomo_extra_fixture-0.0.0.dist-info/WHEEL", "Wheel-Version: 1.0\nGenerator: sinomo verifier\nRoot-Is-Purelib: true\nTag: py3-none-any\n"),
    ],
)
write_wheel(
    normalized_wheel,
    "sinomo_normalized_fixture-0.0.0.dist-info",
    [
        ("sinomo_normalized_fixture.py", "def value():\n    return 'normalized'\n"),
        ("sinomo_normalized_fixture-0.0.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: Sinomo.Normalized_Fixture\nVersion: 0.0.0\n"),
        ("sinomo_normalized_fixture-0.0.0.dist-info/WHEEL", "Wheel-Version: 1.0\nGenerator: sinomo verifier\nRoot-Is-Purelib: true\nTag: py3-none-any\n"),
    ],
)
write_wheel(
    index_wheel,
    "sinomo_index_fixture-0.0.0.dist-info",
    [
        ("sinomo_index_fixture.py", "import sinomo_dependency_fixture\nimport sinomo_normalized_fixture\n\ndef value():\n    return 'mediated package index ' + sinomo_dependency_fixture.value() + ' ' + sinomo_normalized_fixture.value()\n"),
        ("sinomo_index_fixture-0.0.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: sinomo-index-fixture\nVersion: 0.0.0\nRequires-Dist: sinomo-dependency-fixture>=0.0.0,<1\nRequires-Dist: Sinomo.Normalized_Fixture==0.0.0\nRequires-Dist: sinomo-marker-fixture==0.0.0; python_version > \"999\"\nProvides-Extra: cli\nRequires-Dist: sinomo-extra-fixture==0.0.0; extra == \"cli\"\n"),
        ("sinomo_index_fixture-0.0.0.dist-info/WHEEL", "Wheel-Version: 1.0\nGenerator: sinomo verifier\nRoot-Is-Purelib: true\nTag: py3-none-any\n"),
    ],
)
(dep_project / "index.html").write_text(
    "<!doctype html>"
    f"<a href=\"../../packages/{dep_old_wheel}\">{dep_old_wheel}</a>\n"
    f"<a href=\"../../packages/{dep_new_wheel}\">{dep_new_wheel}</a>\n"
    f"<a data-requires-python=\">999\" href=\"../../packages/{dep_incompatible_wheel}\">{dep_incompatible_wheel}</a>\n"
    f"<a data-yanked=\"bad fixture\" href=\"../../packages/{dep_yanked_wheel}\">{dep_yanked_wheel}</a>\n"
)
(index_project / "index.html").write_text(
    f"<!doctype html><a href=\"../../packages/{index_wheel}\">{index_wheel}</a>\n"
)
(transitive_project / "index.html").write_text(
    f"<!doctype html><a href=\"../../packages/{transitive_wheel}\">{transitive_wheel}</a>\n"
)
(extra_project / "index.html").write_text(
    f"<!doctype html><a href=\"../../packages/{extra_wheel}\">{extra_wheel}</a>\n"
)
(normalized_project / "index.html").write_text(
    f"<!doctype html><a href=\"../../packages/{normalized_wheel}\">{normalized_wheel}</a>\n"
)
PY
    python3 -m http.server "${PYTHON_INDEX_PORT}" --bind 127.0.0.1 --directory "${INDEX_ROOT}" >/dev/null 2>&1 &
    HTTP_SERVER_PID=$!
    for path in \
        "simple/sinomo-index-fixture/index.html" \
        "simple/sinomo-dependency-fixture/index.html" \
        "simple/sinomo-transitive-fixture/index.html" \
        "simple/sinomo-extra-fixture/index.html" \
        "simple/sinomo-normalized-fixture/index.html" \
        "packages/${INDEX_WHEEL}" \
        "packages/${DEP_OLD_WHEEL}" \
        "packages/${DEP_NEW_WHEEL}" \
        "packages/${DEP_INCOMPATIBLE_WHEEL}" \
        "packages/${DEP_YANKED_WHEEL}" \
        "packages/${TRANSITIVE_WHEEL}" \
        "packages/${EXTRA_WHEEL}" \
        "packages/${NORMALIZED_WHEEL}"
    do
        for _ in $(seq 1 50); do
            if python3 - "${PYTHON_INDEX_PORT}" "${path}" <<'PY' >/dev/null 2>&1
import sys
from urllib.request import urlopen
urlopen(f"http://127.0.0.1:{sys.argv[1]}/{sys.argv[2]}", timeout=1).read(1)
PY
            then
                break
            fi
            sleep 0.1
        done
    done
    if env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
        -m pip install --progress-bar off --retries 0 --timeout 1 --index-url "http://127.0.0.1:${PYTHON_INDEX_PORT}/simple" --target /work/ambient-network-site-packages sinomo-index-fixture==0.0.0 >/dev/null 2>&1
    then
        echo "sandboxed pip unexpectedly accessed ambient HTTP package index" >&2
        exit 1
    fi
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
        -c "from pathlib import Path; Path('/work/simple/sinomo-index-fixture').mkdir(parents=True, exist_ok=True); Path('/work/simple/sinomo-dependency-fixture').mkdir(parents=True, exist_ok=True); Path('/work/simple/sinomo-transitive-fixture').mkdir(parents=True, exist_ok=True); Path('/work/simple/sinomo-extra-fixture').mkdir(parents=True, exist_ok=True); Path('/work/simple/sinomo-normalized-fixture').mkdir(parents=True, exist_ok=True); Path('/work/packages').mkdir(parents=True, exist_ok=True)"
    for path in \
        "simple/sinomo-index-fixture/index.html" \
        "simple/sinomo-dependency-fixture/index.html" \
        "simple/sinomo-transitive-fixture/index.html" \
        "simple/sinomo-extra-fixture/index.html" \
        "simple/sinomo-normalized-fixture/index.html" \
        "packages/${INDEX_WHEEL}" \
        "packages/${DEP_OLD_WHEEL}" \
        "packages/${DEP_NEW_WHEEL}" \
        "packages/${DEP_INCOMPATIBLE_WHEEL}" \
        "packages/${DEP_YANKED_WHEEL}" \
        "packages/${TRANSITIVE_WHEEL}" \
        "packages/${EXTRA_WHEEL}" \
        "packages/${NORMALIZED_WHEEL}"
    do
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox fetch "${SANDBOX_ID}" \
            "http://127.0.0.1:${PYTHON_INDEX_PORT}/${path}" \
            "/work/${path}" \
            --daemon \
            --timeout-ms "${FETCH_TIMEOUT_MS}" \
            --max-response-bytes 1048576 >/dev/null
    done
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
        -m pip install --progress-bar off --index-url file:///work/simple --target /work/mediated-site-packages sinomo-index-fixture==0.0.0 >/dev/null
    INDEX_IMPORT_OUTPUT="$(
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
            -c "import sys; sys.path.insert(0, '/work/mediated-site-packages'); import sinomo_index_fixture; print(sinomo_index_fixture.value())"
    )"
    if ! grep -qx "mediated package index dependency 0.1.0 transitive normalized" <<<"${INDEX_IMPORT_OUTPUT}"; then
        echo "mediated package index install verification failed" >&2
        echo "${INDEX_IMPORT_OUTPUT}" >&2
        exit 1
    fi
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
        -c "import importlib.util, sys; sys.path.insert(0, '/work/mediated-site-packages'); sys.exit(0 if importlib.util.find_spec('sinomo_marker_fixture') is None else 1)"
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
        -c "from pathlib import Path; Path('/work/pip-constraints.txt').write_text('sinomo-dependency-fixture==0.0.0\n')"
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
        -m pip install --progress-bar off --index-url file:///work/simple --constraint /work/pip-constraints.txt --target /work/constrained-site-packages sinomo-index-fixture==0.0.0 >/dev/null
    CONSTRAINT_IMPORT_OUTPUT="$(
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
            -c "import sys; sys.path.insert(0, '/work/constrained-site-packages'); import sinomo_index_fixture; print(sinomo_index_fixture.value())"
    )"
    if ! grep -qx "mediated package index dependency 0.0.0 normalized" <<<"${CONSTRAINT_IMPORT_OUTPUT}"; then
        echo "mediated package index constraint verification failed" >&2
        echo "${CONSTRAINT_IMPORT_OUTPUT}" >&2
        exit 1
    fi
    INDEX_HASH="$(python3 - "${INDEX_ROOT}/packages/${INDEX_WHEEL}" <<'PY'
import hashlib
import sys
from pathlib import Path
print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)"
    DEP_NEW_HASH="$(python3 - "${INDEX_ROOT}/packages/${DEP_NEW_WHEEL}" <<'PY'
import hashlib
import sys
from pathlib import Path
print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)"
    TRANSITIVE_HASH="$(python3 - "${INDEX_ROOT}/packages/${TRANSITIVE_WHEEL}" <<'PY'
import hashlib
import sys
from pathlib import Path
print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)"
    NORMALIZED_HASH="$(python3 - "${INDEX_ROOT}/packages/${NORMALIZED_WHEEL}" <<'PY'
import hashlib
import sys
from pathlib import Path
print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)"
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
        -c "from pathlib import Path; Path('/work/pip-hashes.txt').write_text('sinomo-index-fixture==0.0.0 --hash=sha256:${INDEX_HASH}\nsinomo-dependency-fixture==0.1.0 --hash=sha256:${DEP_NEW_HASH}\nsinomo-transitive-fixture==0.0.0 --hash=sha256:${TRANSITIVE_HASH}\nSinomo.Normalized_Fixture==0.0.0 --hash=sha256:${NORMALIZED_HASH}\n')"
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
        -m pip install --progress-bar off --require-hashes --index-url file:///work/simple --target /work/hashed-site-packages -r /work/pip-hashes.txt >/dev/null
    HASHED_IMPORT_OUTPUT="$(
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
            -c "import sys; sys.path.insert(0, '/work/hashed-site-packages'); import sinomo_index_fixture; print(sinomo_index_fixture.value())"
    )"
    if ! grep -qx "mediated package index dependency 0.1.0 transitive normalized" <<<"${HASHED_IMPORT_OUTPUT}"; then
        echo "mediated package index hash verification failed" >&2
        echo "${HASHED_IMPORT_OUTPUT}" >&2
        exit 1
    fi
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
        -c "from pathlib import Path; Path('/work/pip-bad-hashes.txt').write_text('sinomo-index-fixture==0.0.0 --hash=sha256:0000000000000000000000000000000000000000000000000000000000000000\n')"
    if env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
        -m pip install --progress-bar off --require-hashes --index-url file:///work/simple --target /work/bad-hash-site-packages -r /work/pip-bad-hashes.txt >/dev/null 2>&1
    then
        echo "mediated package index hash mismatch unexpectedly succeeded" >&2
        exit 1
    fi
    env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
        -m pip install --progress-bar off --index-url file:///work/simple --target /work/extra-site-packages 'sinomo-index-fixture[cli]==0.0.0' >/dev/null
    EXTRA_IMPORT_OUTPUT="$(
        env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" "${RUN_ARGS[@]}" \
            -c "import sys; sys.path.insert(0, '/work/extra-site-packages'); import sinomo_index_fixture, sinomo_extra_fixture; print(sinomo_index_fixture.value() + ' ' + sinomo_extra_fixture.value())"
    )"
    if ! grep -qx "mediated package index dependency 0.1.0 transitive normalized extra" <<<"${EXTRA_IMPORT_OUTPUT}"; then
        echo "mediated package index extra verification failed" >&2
        echo "${EXTRA_IMPORT_OUTPUT}" >&2
        exit 1
    fi
fi

echo "python WASIp1 ${VERIFY_BACKEND} verifier passed"
