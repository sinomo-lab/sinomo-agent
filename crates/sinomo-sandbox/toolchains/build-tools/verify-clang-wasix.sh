#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
LLVM_VERSION="${LLVM_VERSION:-21.1.6}"
TARGET_DIR="${TARGET_DIR:-"${REPO_ROOT}/target"}"
CLANG_WORKDIR="${CLANG_WORKDIR:-"${TARGET_DIR}/sinomo-build-tools-verify-clang-wasix-work"}"
CLANG_OUTDIR="${CLANG_OUTDIR:-"${TARGET_DIR}/sinomo-build-tools-verify-clang-wasix-out"}"
WASIX_SYSROOT_WORKDIR="${WASIX_SYSROOT_WORKDIR:-"${TARGET_DIR}/sinomo-build-tools-verify-wasix-sysroot-work"}"
WASIX_BUILD_SYSROOT="${WASIX_BUILD_SYSROOT:-"${WASIX_SYSROOT_WORKDIR}/build/sysroot"}"
WASIX_PACKAGE_SYSROOT="${WASIX_PACKAGE_SYSROOT:-"${WASIX_SYSROOT_WORKDIR}/package/sysroot"}"
SINOMO_VERIFY_TMPDIR="${SINOMO_VERIFY_TMPDIR:-"${TARGET_DIR}/sinomo-build-tools-verify-clang-wasix-tmp"}"
if [ -z "${SINOMO_VERIFY_DATA_DIR+x}" ]; then
    mkdir -p "${SINOMO_VERIFY_TMPDIR}"
    SINOMO_VERIFY_DATA_DIR="$(mktemp -d "${SINOMO_VERIFY_TMPDIR}/sinomo-clang-wasix-verify.XXXXXX")"
    SINOMO_VERIFY_DATA_DIR_AUTO=1
else
    SINOMO_VERIFY_DATA_DIR_AUTO=0
fi
SANDBOX_ID="${SANDBOX_ID:-01k4z8q5f00000000000000001}"
TOOLCHAIN_ID="${TOOLCHAIN_ID:-01k4z8q5f00000000000000000}"
KEEP_SANDBOX="${KEEP_SANDBOX:-0}"
mkdir -p "${SINOMO_VERIFY_TMPDIR}"
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
    echo "preparing Clang build sysroot"
    WORKDIR="${WASIX_SYSROOT_WORKDIR}/build" \
        SYSROOT_OUTDIR="${WASIX_BUILD_SYSROOT}" \
        "${ROOT}/prepare-wasix-sysroot.sh"

    echo "building Clang/LLVM WASIX modules"
    WORKDIR="${CLANG_WORKDIR}" OUTDIR="${CLANG_OUTDIR}" WASIX_SYSROOT="${WASIX_BUILD_SYSROOT}" \
        "${ROOT}/build-clang-wasix.sh"

    echo "preparing Clang package sysroot"
    WORKDIR="${WASIX_SYSROOT_WORKDIR}/package" \
        SYSROOT_OUTDIR="${WASIX_PACKAGE_SYSROOT}" \
        HOST_ENV_OUT="${WASIX_SYSROOT_WORKDIR}/package/host-env.sh" \
        "${ROOT}/prepare-wasix-sysroot.sh"

    OUTDIR="${CLANG_OUTDIR}" WASIX_SYSROOT="${WASIX_PACKAGE_SYSROOT}" LLVM_VERSION="${LLVM_VERSION}" \
        "${ROOT}/package-clang-wasix.sh"
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
OPEN_CREATE_C="${CLANG_OUTDIR}/open-create.c"
cat > "${OPEN_CREATE_C}" <<'EOF'
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

int main(void) {
    const char *path = "/work/open-create-check";
    unlink(path);
    int fd = open(path, O_CREAT | O_EXCL, 0644);
    if (fd < 0) {
        perror("open");
        return 2;
    }
    close(fd);

    struct stat st;
    if (stat(path, &st) != 0) {
        perror("stat");
        return 3;
    }
    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "open did not create a regular file\n");
        return 4;
    }
    if ((st.st_mode & 0777) != 0644) {
        fprintf(stderr, "open created mode %o, expected 644\n", st.st_mode & 0777);
        return 5;
    }
    unlink(path);
    return 0;
}
EOF
PTHREAD_C="${CLANG_OUTDIR}/pthread.c"
cat > "${PTHREAD_C}" <<'EOF'
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
static int ready = 0;
static int value = 0;

static void *worker(void *arg) {
    (void)arg;
    pthread_mutex_lock(&lock);
    value = 42;
    ready = 1;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&lock);
    return (void *)(intptr_t)7;
}

int main(void) {
    pthread_t thread;
    void *result = 0;

    if (pthread_create(&thread, 0, worker, 0) != 0) {
        puts("pthread_create failed");
        return 2;
    }

    pthread_mutex_lock(&lock);
    while (!ready) {
        pthread_cond_wait(&cond, &lock);
    }
    pthread_mutex_unlock(&lock);

    if (pthread_join(thread, &result) != 0) {
        puts("pthread_join failed");
        return 3;
    }
    if (value != 42 || (intptr_t)result != 7) {
        printf("unexpected result value=%d result=%ld\n", value, (long)(intptr_t)result);
        return 4;
    }
    puts("pthread ok");
    return 0;
}
EOF

echo "verifying Clang package manifest"
"${SINOMO_CMD[@]}" sandbox toolchain verify-package "${CLANG_OUTDIR}/package.json"

echo "using disposable SINOMO_DATA_DIR=${SINOMO_VERIFY_DATA_DIR}"
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox policy set \
    --allow-unsigned-toolchains \
    --max-memory-mb 2048 \
    --max-wall-time-ms unlimited \
    --max-workspace-bytes unlimited \
    --max-written-bytes-per-command unlimited \
    --allow-capability child-process-exec \
    --allow-capability wasix.unpinned-child-exec \
    --allow-toolchain "${TOOLCHAIN_ID}" >/dev/null 2>&1 || true
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox toolchain import-package "${CLANG_OUTDIR}/package.json"
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox create \
    --id "${SANDBOX_ID}" \
    --toolchain "${TOOLCHAIN_ID}" \
    --memory-mb 2048 \
    --max-workspace-bytes unlimited \
    --max-written-bytes-per-command unlimited \
    --max-file-count unlimited
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
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox cat "${SANDBOX_ID}" /work/hello.wasm >"${CLANG_OUTDIR}/hello.wasm"
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox run "${SANDBOX_ID}" \
    --timeout-ms 30000 \
    --wasm "${CLANG_OUTDIR}/hello.wasm"

env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox write "${SANDBOX_ID}" /work/open-create.c --file "${OPEN_CREATE_C}"
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox run "${SANDBOX_ID}" \
    --timeout-ms 30000 \
    --allow-capability child-process-exec \
    --allow-capability wasix.unpinned-child-exec \
    clang -- /work/open-create.c -o /work/open-create.wasm
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox cat "${SANDBOX_ID}" /work/open-create.wasm >"${CLANG_OUTDIR}/open-create.wasm"
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox run "${SANDBOX_ID}" \
    --timeout-ms 30000 \
    --wasm "${CLANG_OUTDIR}/open-create.wasm"

env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox write "${SANDBOX_ID}" /work/pthread.c --file "${PTHREAD_C}"
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox run "${SANDBOX_ID}" \
    --timeout-ms 30000 \
    --allow-capability child-process-exec \
    --allow-capability wasix.unpinned-child-exec \
    clang -- /work/pthread.c -o /work/pthread.wasm
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox cat "${SANDBOX_ID}" /work/pthread.wasm >"${CLANG_OUTDIR}/pthread.wasm"
python3 - "${CLANG_OUTDIR}/pthread.wasm" <<'PY'
import sys

data = memoryview(open(sys.argv[1], "rb").read())
if data[:4] != b"\0asm":
    raise SystemExit("pthread output is not a WebAssembly module")

offset = 8

def read_u32():
    global offset
    shift = 0
    result = 0
    while True:
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if byte < 0x80:
            return result
        shift += 7

def read_bytes():
    length = read_u32()
    global offset
    value = bytes(data[offset:offset + length])
    offset += length
    return value

has_shared_imported_memory = False
while offset < len(data):
    section_id = data[offset]
    offset += 1
    section_len = read_u32()
    section_end = offset + section_len
    if section_id == 2:
        count = read_u32()
        for _ in range(count):
            module = read_bytes()
            name = read_bytes()
            kind = data[offset]
            offset += 1
            if kind == 0:
                read_u32()
            elif kind == 1:
                offset += 1
                read_u32()
            elif kind == 2:
                flags = read_u32()
                read_u32()
                if flags & 1:
                    read_u32()
                if module == b"env" and name == b"memory" and flags & 2:
                    has_shared_imported_memory = True
            elif kind == 3:
                offset += 2
            else:
                raise SystemExit(f"unsupported import kind {kind}")
    offset = section_end

if not has_shared_imported_memory:
    raise SystemExit("pthread output did not import shared env memory")
PY
env "${RUN_ENV[@]}" "${SINOMO_CMD[@]}" sandbox run "${SANDBOX_ID}" \
    --timeout-ms 30000 \
    --wasm "${CLANG_OUTDIR}/pthread.wasm"
