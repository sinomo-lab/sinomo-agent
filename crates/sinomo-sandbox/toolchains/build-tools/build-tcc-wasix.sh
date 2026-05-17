#!/usr/bin/env bash
set -euo pipefail

TCC_VERSION="${TCC_VERSION:-0.9.27}"
TCC_SHA256="${TCC_SHA256:-de23af78fca90ce32dff2dd45b3432b2334740bb9bb7b05bf60fdbfc396ceb9c}"
TCC_TOOLCHAIN_ID="${TCC_TOOLCHAIN_ID:-01k4z8q5e00000000000000000}"
WASI_SDK_VERSION="${WASI_SDK_VERSION:-24}"
WASI_SDK_VERSION_FULL="${WASI_SDK_VERSION_FULL:-24.0}"
WASI_SDK_ARCH="${WASI_SDK_ARCH:-x86_64}"
WASI_SDK_OS="${WASI_SDK_OS:-linux}"
WASI_SDK_SHA256="${WASI_SDK_SHA256:-c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5}"
BINARYEN_VERSION="${BINARYEN_VERSION:-124}"
BINARYEN_ARCH="${BINARYEN_ARCH:-x86_64}"
BINARYEN_OS="${BINARYEN_OS:-linux}"
BINARYEN_SHA256="${BINARYEN_SHA256:-0290c3779fedf592b8da0ded3032ff55c41a2b7bfa2d6bf7b7bac6f0e6e28963}"
WASIX_LIBC_VERSION="${WASIX_LIBC_VERSION:-v2026-05-12.1}"
WASIX_LIBC_TARBALL="${WASIX_LIBC_TARBALL:-sysroot.tar.gz}"
WASIX_LIBC_SHA256="${WASIX_LIBC_SHA256:-e9f7435c0fdc050c1f813f83d3b22388ba3b16d1303ced510915eb028a38abb4}"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1566950400}"
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
WORKDIR="${WORKDIR:-"${ROOT}/work"}"
OUTDIR="${OUTDIR:-"${ROOT}/out"}"

TCC_TARBALL="tcc-${TCC_VERSION}.tar.bz2"
TCC_URL="https://download-mirror.savannah.gnu.org/releases/tinycc/${TCC_TARBALL}"
WASI_SDK_TARBALL="wasi-sdk-${WASI_SDK_VERSION_FULL}-${WASI_SDK_ARCH}-${WASI_SDK_OS}.tar.gz"
WASI_SDK_URL="https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_SDK_VERSION}/${WASI_SDK_TARBALL}"
BINARYEN_TARBALL="binaryen-version_${BINARYEN_VERSION}-${BINARYEN_ARCH}-${BINARYEN_OS}.tar.gz"
BINARYEN_URL="https://github.com/WebAssembly/binaryen/releases/download/version_${BINARYEN_VERSION}/${BINARYEN_TARBALL}"
WASIX_LIBC_URL="${WASIX_LIBC_URL:-https://github.com/wasix-org/wasix-libc/releases/download/${WASIX_LIBC_VERSION}/${WASIX_LIBC_TARBALL}}"

download() {
    local url="$1"
    local output="$2"
    if [ ! -f "${output}" ]; then
        curl -L --fail --retry 3 --output "${output}" "${url}"
    fi
}

verify_sha256() {
    local expected="$1"
    local file="$2"
    local actual
    actual="$(sha256sum "${file}" | awk '{print $1}')"
    if [ "${actual}" != "${expected}" ]; then
        echo "sha256 mismatch for ${file}: expected ${expected}, actual ${actual}" >&2
        exit 1
    fi
}

sinomo_hash() {
    local file="$1"
    if [ -n "${SINOMO_BIN:-}" ]; then
        env -u CC -u CFLAGS -u LDFLAGS -u AR -u RANLIB \
            "${SINOMO_BIN}" sandbox toolchain hash "${file}"
    else
        env -u CC -u CFLAGS -u LDFLAGS -u AR -u RANLIB \
            cargo run --quiet --manifest-path "${REPO_ROOT}/Cargo.toml" --bin sinomo -- sandbox toolchain hash "${file}"
    fi
}

mkdir -p "${WORKDIR}" "${OUTDIR}" "${WORKDIR}/downloads"

download "${TCC_URL}" "${WORKDIR}/downloads/${TCC_TARBALL}"
download "${WASI_SDK_URL}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
download "${BINARYEN_URL}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
download "${WASIX_LIBC_URL}" "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"
verify_sha256 "${TCC_SHA256}" "${WORKDIR}/downloads/${TCC_TARBALL}"
verify_sha256 "${WASI_SDK_SHA256}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
verify_sha256 "${BINARYEN_SHA256}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
verify_sha256 "${WASIX_LIBC_SHA256}" "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"

rm -rf \
    "${WORKDIR}/tcc-${TCC_VERSION}" \
    "${WORKDIR}"/wasi-sdk-${WASI_SDK_VERSION_FULL}* \
    "${WORKDIR}"/binaryen-version_${BINARYEN_VERSION} \
    "${WORKDIR}/wasix-sysroot"
tar -C "${WORKDIR}" -xjf "${WORKDIR}/downloads/${TCC_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
mkdir -p "${WORKDIR}/wasix-sysroot"
tar -C "${WORKDIR}/wasix-sysroot" -xzf "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"

TCC_SRC="${WORKDIR}/tcc-${TCC_VERSION}"
WASI_SDK="$(find "${WORKDIR}" -maxdepth 1 -type d -name "wasi-sdk-${WASI_SDK_VERSION_FULL}*" | sort | head -n 1)"
if [ -z "${WASI_SDK}" ] || [ ! -x "${WASI_SDK}/bin/clang" ]; then
    echo "wasi-sdk clang not found after extracting ${WASI_SDK_TARBALL}" >&2
    exit 1
fi
BINARYEN="${WORKDIR}/binaryen-version_${BINARYEN_VERSION}"
if [ ! -x "${BINARYEN}/bin/wasm-opt" ]; then
    echo "Binaryen wasm-opt not found after extracting ${BINARYEN_TARBALL}" >&2
    exit 1
fi
WASIX_SYSROOT="$(find "${WORKDIR}/wasix-sysroot" -type d -path "*/sysroot" | sort | head -n 1)"
if [ -z "${WASIX_SYSROOT}" ] || [ ! -f "${WASIX_SYSROOT}/include/unistd.h" ] || [ ! -f "${WASIX_SYSROOT}/lib/wasm32-wasi/libc.a" ]; then
    echo "wasix-libc sysroot not found after extracting ${WASIX_LIBC_TARBALL}" >&2
    exit 1
fi

COMPAT_HEADER="${TCC_SRC}/sinomo-tcc-wasix-compat.h"
COMPAT_SOURCE="${TCC_SRC}/sinomo-tcc-wasix-compat.c"
cat > "${COMPAT_HEADER}" <<'COMPAT'
#include <errno.h>
int execvp(const char *file, char *const argv[]);
COMPAT
cat > "${COMPAT_SOURCE}" <<'COMPAT'
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <wasi/api_wasix.h>

static const char sinomo_exec_magic[] = "__SINOMO_WASIX_ESCAPED_STRING_LIST_V1__\n";
extern char **environ;

static char *sinomo_exec_combine_strings(char *const strings[]) {
    size_t combined_len = sizeof(sinomo_exec_magic) - 1;
    for (char **ptr = (char **)strings; ptr != NULL && *ptr != NULL; ptr++) {
        for (const unsigned char *p = (const unsigned char *)*ptr; *p != 0; p++) {
            combined_len += (*p == '\n' || *p == '\r' || *p == '\\') ? 2 : 1;
        }
        combined_len += 1;
    }

    char *combined = malloc(combined_len + 1);
    if (combined == NULL) {
        errno = ENOMEM;
        return NULL;
    }

    char *out = combined;
    memcpy(out, sinomo_exec_magic, sizeof(sinomo_exec_magic) - 1);
    out += sizeof(sinomo_exec_magic) - 1;
    for (char **ptr = (char **)strings; ptr != NULL && *ptr != NULL; ptr++) {
        for (const unsigned char *p = (const unsigned char *)*ptr; *p != 0; p++) {
            if (*p == '\n') {
                *out++ = '\\';
                *out++ = 'n';
            } else if (*p == '\r') {
                *out++ = '\\';
                *out++ = 'r';
            } else if (*p == '\\') {
                *out++ = '\\';
                *out++ = '\\';
            } else {
                *out++ = (char)*p;
            }
        }
        *out++ = '\n';
    }
    *out = 0;
    return combined;
}

int execvp(const char *file, char *const argv[]) {
    if (file == NULL || argv == NULL) {
        errno = EFAULT;
        return -1;
    }
    char *combined_argv = sinomo_exec_combine_strings(argv);
    char *combined_env = sinomo_exec_combine_strings(environ);
    if (combined_argv == NULL || combined_env == NULL) {
        free(combined_argv);
        free(combined_env);
        return -1;
    }
    const char *search_path = getenv("PATH");
    if (search_path == NULL) {
        search_path = "";
    }

    __wasi_errno_t error = __wasi_proc_exec3(file, combined_argv, combined_env, 0, search_path);
    free(combined_argv);
    free(combined_env);
    errno = (int)error;
    return -1;
}
COMPAT

export SOURCE_DATE_EPOCH
cd "${TCC_SRC}"
./configure \
    --cc="${WASI_SDK}/bin/clang" \
    --ar="${WASI_SDK}/bin/llvm-ar" \
    --prefix=/ \
    --cpu=x86_64 \
    --config-musl \
    --extra-cflags="--target=wasm32-wasi --sysroot=${WASIX_SYSROOT} -Os -fno-ident -matomics -mbulk-memory -mmutable-globals -pthread -mthread-model posix -fno-exceptions -ftls-model=local-exec -ffile-prefix-map=${WORKDIR}=sinomo-tcc-build -ffile-prefix-map=${REPO_ROOT}=sinomo -D_WASI_EMULATED_MMAN -D_WASI_EMULATED_PROCESS_CLOCKS" \
    --extra-ldflags="--target=wasm32-wasi --sysroot=${WASIX_SYSROOT} -Wl,--export-all -Wl,--export-dynamic -Wl,--shared-memory -Wl,--import-memory -Wl,--extra-features=atomics -Wl,--extra-features=bulk-memory -Wl,--extra-features=mutable-globals -Wl,--export=__wasm_call_ctors -Wl,--export=__wasm_init_tls -Wl,--export=__wasm_signal -Wl,--export-if-defined=__indirect_function_table -Wl,--export-if-defined=__stack_pointer -Wl,--export-if-defined=__heap_base -Wl,--export-if-defined=__data_end -Wl,-z,stack-size=1048576 -Wl,--initial-memory=67108864 -Wl,--max-memory=268435456"
printf '%s\n' 'CFLAGS += -include sinomo-tcc-wasix-compat.h' >> config.mak
make -j"${JOBS}" tcc.o libtcc.a

COMPAT_OBJECT="${WORKDIR}/sinomo-tcc-wasix-compat.o"
"${WASI_SDK}/bin/clang" \
    --target=wasm32-wasi \
    --sysroot="${WASIX_SYSROOT}" \
    -Os \
    -fno-ident \
    -matomics \
    -mbulk-memory \
    -mmutable-globals \
    -pthread \
    -mthread-model posix \
    -fno-exceptions \
    -ftls-model=local-exec \
    -c "${COMPAT_SOURCE}" \
    -o "${COMPAT_OBJECT}"

TCC_WASM_UNOPTIMIZED="${OUTDIR}/tcc-${TCC_VERSION}-wasix.unoptimized.wasm"
TCC_WASM="${OUTDIR}/tcc-${TCC_VERSION}-wasix.wasm"
"${WASI_SDK}/bin/clang" \
    --target=wasm32-wasi \
    --sysroot="${WASIX_SYSROOT}" \
    -Os \
    -fno-ident \
    -matomics \
    -mbulk-memory \
    -mmutable-globals \
    -pthread \
    -mthread-model posix \
    -fno-exceptions \
    -ftls-model=local-exec \
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
    -o "${TCC_WASM_UNOPTIMIZED}" \
    tcc.o \
    libtcc.a \
    "${COMPAT_OBJECT}" \
    -lm \
    -ldl \
    -lwasi-emulated-mman \
    -lwasi-emulated-process-clocks \
    -lrt \
    -lpthread

"${BINARYEN}/bin/wasm-opt" \
    --asyncify \
    --fpcast-emu \
    -Os \
    --no-validation \
    --enable-threads \
    --enable-mutable-globals \
    --enable-bulk-memory \
    --enable-bulk-memory-opt \
    --enable-exception-handling \
    --enable-simd \
    --enable-relaxed-simd \
    --enable-extended-const \
    -o "${TCC_WASM}" \
    "${TCC_WASM_UNOPTIMIZED}"

printf '%s\n' "${TCC_VERSION}" > "${OUTDIR}/TCC_VERSION"
cp "${TCC_SRC}/COPYING" "${OUTDIR}/COPYING"
TCC_HASH="$(sinomo_hash "${TCC_WASM}")"
VERSION_HASH="$(sinomo_hash "${OUTDIR}/TCC_VERSION")"
COPYING_HASH="$(sinomo_hash "${OUTDIR}/COPYING")"

cat > "${OUTDIR}/package.json" <<PACKAGE
{
  "toolchain_id": "${TCC_TOOLCHAIN_ID}",
  "version": "tcc-${TCC_VERSION}-wasix",
  "provenance": "Sinomo TinyCC WASIX recipe: TinyCC ${TCC_VERSION}, wasi-sdk ${WASI_SDK_VERSION_FULL}",
  "target_abi": "wasix",
  "commands": [
    {
      "name": "tcc",
      "module": "tcc-${TCC_VERSION}-wasix.wasm",
      "content_hash": "${TCC_HASH}",
      "required_capabilities": [],
      "default_limits": {
        "memory_mb": 256,
        "wall_time_ms": 10000,
        "max_input_bytes": 1048576,
        "max_output_bytes": 1048576,
        "max_workspace_bytes": 1073741824,
        "max_written_bytes_per_command": 67108864,
        "max_file_count": 100000
      }
    }
  ],
  "artifacts": [
    {
      "relative_path": "share/tcc/VERSION",
      "file": "TCC_VERSION",
      "content_hash": "${VERSION_HASH}"
    },
    {
      "relative_path": "share/tcc/COPYING",
      "file": "COPYING",
      "content_hash": "${COPYING_HASH}"
    }
  ]
}
PACKAGE

cat > "${OUTDIR}/tcc-${TCC_VERSION}-build.manifest" <<MANIFEST
tcc_version=${TCC_VERSION}
tcc_source=${TCC_URL}
tcc_sha256=${TCC_SHA256}
toolchain_id=${TCC_TOOLCHAIN_ID}
wasi_sdk_version=${WASI_SDK_VERSION_FULL}
wasi_sdk_source=${WASI_SDK_URL}
wasi_sdk_sha256=${WASI_SDK_SHA256}
binaryen_version=${BINARYEN_VERSION}
binaryen_source=${BINARYEN_URL}
binaryen_sha256=${BINARYEN_SHA256}
wasm_asyncify=true
wasm_fpcast_emu=true
wasix_libc_version=${WASIX_LIBC_VERSION}
wasix_libc_source=${WASIX_LIBC_URL}
wasix_libc_sha256=${WASIX_LIBC_SHA256}
source_date_epoch=${SOURCE_DATE_EPOCH}
wasm=${TCC_WASM}
wasm_hash=${TCC_HASH}
package=${OUTDIR}/package.json
import_command=sinomo sandbox toolchain import-package --daemon --into toolchain_busybox ${OUTDIR}/package.json
MANIFEST

file "${TCC_WASM}"
echo "wrote ${TCC_WASM}"
echo "hash ${TCC_HASH}"
echo "package ${OUTDIR}/package.json"
