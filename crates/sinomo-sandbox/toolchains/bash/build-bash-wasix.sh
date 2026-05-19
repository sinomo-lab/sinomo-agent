#!/usr/bin/env bash
set -euo pipefail

GNU_BASH_VERSION="${GNU_BASH_VERSION:-5.2.37}"
BASH_SHA256="${BASH_SHA256:-9599b22ecd1d5787ad7d3b7bf0c59f312b3396d1e281175dd1f8a4014da621ff}"
BASH_TOOLCHAIN_ID="${BASH_TOOLCHAIN_ID:-01k4z8q5b00000000000000000}"
WASI_SDK_VERSION="${WASI_SDK_VERSION:-24}"
WASI_SDK_VERSION_FULL="${WASI_SDK_VERSION_FULL:-24.0}"
WASI_SDK_ARCH="${WASI_SDK_ARCH:-x86_64}"
WASI_SDK_OS="${WASI_SDK_OS:-linux}"
WASI_SDK_SHA256="${WASI_SDK_SHA256:-c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5}"
BINARYEN_VERSION="${BINARYEN_VERSION:-124}"
BINARYEN_ARCH="${BINARYEN_ARCH:-x86_64}"
BINARYEN_OS="${BINARYEN_OS:-linux}"
BINARYEN_SHA256="${BINARYEN_SHA256:-0290c3779fedf592b8da0ded3032ff55c41a2b7bfa2d6bf7b7bac6f0e6e28963}"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1751328000}"
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
WORKDIR="${WORKDIR:-"${ROOT}/work"}"
OUTDIR="${OUTDIR:-"${ROOT}/out"}"

BASH_TARBALL="bash-${GNU_BASH_VERSION}.tar.gz"
BASH_URL="https://ftp.gnu.org/gnu/bash/${BASH_TARBALL}"
WASI_SDK_TARBALL="wasi-sdk-${WASI_SDK_VERSION_FULL}-${WASI_SDK_ARCH}-${WASI_SDK_OS}.tar.gz"
WASI_SDK_URL="https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_SDK_VERSION}/${WASI_SDK_TARBALL}"
BINARYEN_TARBALL="binaryen-version_${BINARYEN_VERSION}-${BINARYEN_ARCH}-${BINARYEN_OS}.tar.gz"
BINARYEN_URL="https://github.com/WebAssembly/binaryen/releases/download/version_${BINARYEN_VERSION}/${BINARYEN_TARBALL}"
WASIX_LIBC_VERSION="${WASIX_LIBC_VERSION:-v2026-05-12.1}"
WASIX_LIBC_TARBALL="${WASIX_LIBC_TARBALL:-sysroot.tar.gz}"
WASIX_LIBC_SHA256="${WASIX_LIBC_SHA256:-e9f7435c0fdc050c1f813f83d3b22388ba3b16d1303ced510915eb028a38abb4}"
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

download "${BASH_URL}" "${WORKDIR}/downloads/${BASH_TARBALL}"
download "${WASI_SDK_URL}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
download "${BINARYEN_URL}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
download "${WASIX_LIBC_URL}" "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"
verify_sha256 "${BASH_SHA256}" "${WORKDIR}/downloads/${BASH_TARBALL}"
verify_sha256 "${WASI_SDK_SHA256}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
verify_sha256 "${BINARYEN_SHA256}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
verify_sha256 "${WASIX_LIBC_SHA256}" "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"

rm -rf "${WORKDIR}/bash-${GNU_BASH_VERSION}" "${WORKDIR}"/wasi-sdk-${WASI_SDK_VERSION_FULL}* "${WORKDIR}"/binaryen-version_${BINARYEN_VERSION} "${WORKDIR}/wasix-sysroot"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${BASH_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"

BASH_SRC="${WORKDIR}/bash-${GNU_BASH_VERSION}"
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
WASIX_SYSROOT="${WORKDIR}/wasix-sysroot/sysroot"
if [ ! -f "${WASIX_SYSROOT}/include/unistd.h" ] || [ ! -f "${WASIX_SYSROOT}/lib/wasm32-wasi/libc.a" ]; then
    echo "wasix-libc sysroot not found after extracting ${WASIX_LIBC_TARBALL}" >&2
    exit 1
fi

export SOURCE_DATE_EPOCH
export CC="${WASI_SDK}/bin/clang --target=wasm32-wasi --sysroot=${WASIX_SYSROOT}"
export AR="${WASI_SDK}/bin/llvm-ar"
export RANLIB="${WASI_SDK}/bin/llvm-ranlib"
BASH_COMPAT_INCLUDE="${SINOMO_BASH_WASIX_DIR}/bash-compat.h"
BASH_ENV_COMPAT_SOURCE="${SINOMO_BASH_WASIX_DIR}/env-compat.c"
BASH_ENV_COMPAT_OBJECT="${WORKDIR}/sinomo-bash-wasix-env-compat.o"

BASH_MAIN_COMPAT_SOURCE="${SINOMO_BASH_WASIX_DIR}/main-compat.c"
BASH_MAIN_COMPAT_OBJECT="${WORKDIR}/sinomo-bash-wasix-main-compat.o"
"${WASI_SDK}/bin/clang" \
    --target=wasm32-wasi \
    --sysroot="${WASIX_SYSROOT}" \
    -Os \
    -matomics \
    -mbulk-memory \
    -mmutable-globals \
    -pthread \
    -mthread-model posix \
    -fno-exceptions \
    -ftls-model=local-exec \
    -c "${BASH_ENV_COMPAT_SOURCE}" \
    -o "${BASH_ENV_COMPAT_OBJECT}"
"${WASI_SDK}/bin/clang" \
    --target=wasm32-wasi \
    --sysroot="${WASIX_SYSROOT}" \
    -Os \
    -matomics \
    -mbulk-memory \
    -mmutable-globals \
    -pthread \
    -mthread-model posix \
    -fno-exceptions \
    -ftls-model=local-exec \
    -c "${BASH_MAIN_COMPAT_SOURCE}" \
    -o "${BASH_MAIN_COMPAT_OBJECT}"

# Bash intentionally overrides libc getenv/putenv/setenv/unsetenv so library
# calls observe the shell's exported variable table. Static Wasm archives cannot
# tolerate duplicate strong putenv/setenv definitions, so this recipe derives a
# Bash-local libc archive by removing only the colliding libc objects and adding
# tiny compatibility objects needed by wasix-libc startup/clearenv. This keeps
# upstream Bash source and shell behavior unchanged.
"${WASI_SDK}/bin/llvm-ar" d "${WASIX_SYSROOT}/lib/wasm32-wasi/libc.a" setenv.o putenv.o
"${WASI_SDK}/bin/llvm-ar" r \
    "${WASIX_SYSROOT}/lib/wasm32-wasi/libc.a" \
    "${BASH_ENV_COMPAT_OBJECT}" \
    "${BASH_MAIN_COMPAT_OBJECT}"

export CFLAGS="-Os -fwrapv -fno-ident -matomics -mbulk-memory -mmutable-globals -pthread -mthread-model posix -fno-trapping-math -fno-exceptions -ftls-model=local-exec -msimd128 -mrelaxed-simd -mextended-const -include ${BASH_COMPAT_INCLUDE} -ffile-prefix-map=${WORKDIR}=sinomo-bash-build -ffile-prefix-map=${REPO_ROOT}=sinomo -D_WASI_EMULATED_MMAN -D_WASI_EMULATED_SIGNAL -D_WASI_EMULATED_PROCESS_CLOCKS"
export LDFLAGS="-Wl,--export-all -Wl,--export-dynamic -Wl,--shared-memory -Wl,--import-memory -Wl,--extra-features=atomics -Wl,--extra-features=bulk-memory -Wl,--extra-features=mutable-globals -Wl,--export=__wasm_call_ctors -Wl,--export=__wasm_init_tls -Wl,--export=__wasm_signal -Wl,--export=__tls_size -Wl,--export=__tls_align -Wl,--export=__tls_base -Wl,--export-if-defined=__indirect_function_table -Wl,--export-if-defined=__stack_pointer -Wl,--export-if-defined=__heap_base -Wl,--export-if-defined=__data_end -Wl,-z,stack-size=1048576 -Wl,--initial-memory=67108864 -Wl,--max-memory=268435456 -lwasi-emulated-getpid -lwasi-emulated-mman -lwasi-emulated-process-clocks -lresolv -lrt -lm -lpthread -lutil"
BUILD_CFLAGS="-g -DCROSS_COMPILING -std=gnu89"

cd "${BASH_SRC}"

# Keep the first recipe close to upstream Bash. The build may still fail until
# Sinomo's WASIX/POSIX substrate and the required compatibility patches are
# complete; failures are intentional blockers, not a reason to expose a shim.
#
# These are cross-compile answers for libc surfaces that the pinned wasix-libc
# sysroot provides but upstream Bash cannot execute-probe while targeting wasm.
# Keep them limited to facts verified against the pinned sysroot.
bash_cv_getcwd_malloc=yes \
bash_cv_getenv_redef=yes \
ac_cv_func_siginterrupt=yes \
CONFIG_SITE=/dev/null ./configure \
    --host=wasm32-wasi \
    --build="$(sh ./support/config.guess)" \
    --without-bash-malloc \
    --disable-nls \
    --disable-readline \
    --disable-history \
    --enable-minimal-config

make -j"${JOBS}" CFLAGS_FOR_BUILD="${BUILD_CFLAGS}" BASE_CFLAGS_FOR_BUILD="${BUILD_CFLAGS}"

BASH_WASM_CANDIDATE="$(find "${BASH_SRC}" -maxdepth 2 -type f \( -name 'bash.wasm' -o -name 'bash' \) | sort | head -n 1)"
if [ -z "${BASH_WASM_CANDIDATE}" ]; then
    echo "Bash build completed but no wasm output was found" >&2
    exit 1
fi

BASH_WASM_UNOPTIMIZED="${OUTDIR}/bash-${GNU_BASH_VERSION}-wasix.unoptimized.wasm"
BASH_WASM="${OUTDIR}/bash-${GNU_BASH_VERSION}-wasix.wasm"
cp "${BASH_WASM_CANDIDATE}" "${BASH_WASM_UNOPTIMIZED}"
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
    -o "${BASH_WASM}" \
    "${BASH_WASM_UNOPTIMIZED}"
cp "${BASH_SRC}/COPYING" "${OUTDIR}/COPYING"
printf '%s\n' "${GNU_BASH_VERSION}" > "${OUTDIR}/VERSION"

BASH_HASH="$(sinomo_hash "${BASH_WASM}")"
COPYING_HASH="$(sinomo_hash "${OUTDIR}/COPYING")"
VERSION_HASH="$(sinomo_hash "${OUTDIR}/VERSION")"

cat > "${OUTDIR}/package.json" <<PACKAGE
{
  "toolchain_id": "${BASH_TOOLCHAIN_ID}",
  "version": "bash-${GNU_BASH_VERSION}-wasix",
  "provenance": "Sinomo Bash WASIX recipe: GNU Bash ${GNU_BASH_VERSION}, wasi-sdk ${WASI_SDK_VERSION_FULL}",
  "target_abi": "wasix",
  "commands": [
    {
      "name": "bash",
      "module": "bash-${GNU_BASH_VERSION}-wasix.wasm",
      "content_hash": "${BASH_HASH}",
      "required_capabilities": ["child-process-exec", "wasix.unpinned-child-exec"],
      "default_limits": {
        "memory_mb": 256,
        "wall_time_ms": 10000,
        "max_input_bytes": 1048576,
        "max_output_bytes": 1048576,
        "max_workspace_bytes": 1073741824,
        "max_written_bytes_per_command": 67108864,
        "max_file_count": 100000
      }
    },
    {
      "name": "sh",
      "module": "bash-${GNU_BASH_VERSION}-wasix.wasm",
      "content_hash": "${BASH_HASH}",
      "exec_name": "sh",
      "required_capabilities": ["child-process-exec", "wasix.unpinned-child-exec"],
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
      "relative_path": "share/bash/COPYING",
      "file": "COPYING",
      "content_hash": "${COPYING_HASH}"
    },
    {
      "relative_path": "share/bash/VERSION",
      "file": "VERSION",
      "content_hash": "${VERSION_HASH}"
    }
  ]
}
PACKAGE

cat > "${OUTDIR}/bash-${GNU_BASH_VERSION}-build.manifest" <<MANIFEST
bash_version=${GNU_BASH_VERSION}
bash_source=${BASH_URL}
bash_sha256=${BASH_SHA256}
toolchain_id=${BASH_TOOLCHAIN_ID}
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
wasm=${BASH_WASM}
wasm_hash=${BASH_HASH}
package=${OUTDIR}/package.json
import_command=sinomo sandbox toolchain import-package --daemon --into toolchain_busybox ${OUTDIR}/package.json
MANIFEST

file "${BASH_WASM}"
echo "wrote ${BASH_WASM}"
echo "hash ${BASH_HASH}"
echo "package ${OUTDIR}/package.json"
