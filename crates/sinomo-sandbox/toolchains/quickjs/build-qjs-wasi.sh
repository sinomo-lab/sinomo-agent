#!/usr/bin/env bash
set -euo pipefail

QUICKJS_VERSION="${QUICKJS_VERSION:-2025-09-13}"
QUICKJS_RELEASE="${QUICKJS_RELEASE:-2025-09-13-2}"
QUICKJS_SHA256="${QUICKJS_SHA256:-996c6b5018fc955ad4d06426d0e9cb713685a00c825aa5c0418bd53f7df8b0b4}"
QUICKJS_TOOLCHAIN_ID="${QUICKJS_TOOLCHAIN_ID:-01k4z8q5c00000000000000000}"
WASI_SDK_VERSION="${WASI_SDK_VERSION:-24}"
WASI_SDK_VERSION_FULL="${WASI_SDK_VERSION_FULL:-24.0}"
WASI_SDK_ARCH="${WASI_SDK_ARCH:-x86_64}"
WASI_SDK_OS="${WASI_SDK_OS:-linux}"
WASI_SDK_SHA256="${WASI_SDK_SHA256:-c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5}"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1757721600}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
WORKDIR="${WORKDIR:-"${ROOT}/work"}"
OUTDIR="${OUTDIR:-"${ROOT}/out"}"

QUICKJS_TARBALL="quickjs-${QUICKJS_RELEASE}.tar.xz"
QUICKJS_URL="https://bellard.org/quickjs/${QUICKJS_TARBALL}"
WASI_SDK_TARBALL="wasi-sdk-${WASI_SDK_VERSION_FULL}-${WASI_SDK_ARCH}-${WASI_SDK_OS}.tar.gz"
WASI_SDK_URL="https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_SDK_VERSION}/${WASI_SDK_TARBALL}"

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
        "${SINOMO_BIN}" sandbox toolchain hash "${file}"
    else
        cargo run --quiet --manifest-path "${REPO_ROOT}/Cargo.toml" --bin sinomo -- sandbox toolchain hash "${file}"
    fi
}

mkdir -p "${WORKDIR}" "${OUTDIR}" "${WORKDIR}/downloads"

download "${QUICKJS_URL}" "${WORKDIR}/downloads/${QUICKJS_TARBALL}"
download "${WASI_SDK_URL}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
verify_sha256 "${QUICKJS_SHA256}" "${WORKDIR}/downloads/${QUICKJS_TARBALL}"
verify_sha256 "${WASI_SDK_SHA256}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"

rm -rf "${WORKDIR}/quickjs-${QUICKJS_VERSION}" "${WORKDIR}"/wasi-sdk-${WASI_SDK_VERSION_FULL}*
tar -C "${WORKDIR}" -xJf "${WORKDIR}/downloads/${QUICKJS_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"

QUICKJS_SRC="${WORKDIR}/quickjs-${QUICKJS_VERSION}"
WASI_SDK="$(find "${WORKDIR}" -maxdepth 1 -type d -name "wasi-sdk-${WASI_SDK_VERSION_FULL}*" | sort | head -n 1)"
if [ -z "${WASI_SDK}" ] || [ ! -x "${WASI_SDK}/bin/clang" ]; then
    echo "wasi-sdk clang not found after extracting ${WASI_SDK_TARBALL}" >&2
    exit 1
fi

QJS_REPL_STUB="${ROOT}/wasi/repl-stub.c"
QJS_WASI_COMPAT_HEADER="${ROOT}/wasi/wasi-compat.h"

QJS_WASM="${OUTDIR}/qjs-${QUICKJS_RELEASE}-wasip1.wasm"
WASI_CC="${WASI_SDK}/bin/clang"
COMMON_FLAGS=(
    --target=wasm32-wasip1
    "--sysroot=${WASI_SDK}/share/wasi-sysroot"
    -Os
    -fwrapv
    -fno-ident
    "-ffile-prefix-map=${WORKDIR}=sinomo-quickjs-build"
    "-ffile-prefix-map=${REPO_ROOT}=sinomo"
    -mllvm
    -wasm-enable-sjlj
    "-fdebug-prefix-map=${QUICKJS_SRC}=quickjs-${QUICKJS_VERSION}"
    "-fdebug-prefix-map=${WORKDIR}=sinomo-quickjs-build"
    "-fdebug-prefix-map=${REPO_ROOT}=sinomo"
    -D_WASI_EMULATED_SIGNAL
    -D_WASI_EMULATED_GETPID
    -D_WASI_EMULATED_PROCESS_CLOCKS
    -D_GNU_SOURCE
    "-DCONFIG_VERSION=\"${QUICKJS_VERSION}\""
    -DCONFIG_BIGNUM
    "-Dmalloc_usable_size(x)=0"
    -include
    "${QJS_WASI_COMPAT_HEADER}"
    -I"${QUICKJS_SRC}"
)
LINK_FLAGS=(
    -Wl,--export-all
    -Wl,--no-entry
    -Wl,--allow-undefined
    -Wl,-z,stack-size=1048576
)

export SOURCE_DATE_EPOCH
"${WASI_CC}" \
    "${COMMON_FLAGS[@]}" \
    "${LINK_FLAGS[@]}" \
    -o "${QJS_WASM}" \
    "${QUICKJS_SRC}/qjs.c" \
    "${QUICKJS_SRC}/quickjs.c" \
    "${QUICKJS_SRC}/quickjs-libc.c" \
    "${QUICKJS_SRC}/libregexp.c" \
    "${QUICKJS_SRC}/libunicode.c" \
    "${QUICKJS_SRC}/cutils.c" \
    "${QUICKJS_SRC}/dtoa.c" \
    "${QJS_REPL_STUB}" \
    -lm \
    -lwasi-emulated-signal \
    -lwasi-emulated-getpid \
    -lwasi-emulated-process-clocks

cp "${QUICKJS_SRC}/LICENSE" "${OUTDIR}/LICENSE"
printf '%s\n' "${QUICKJS_VERSION}" > "${OUTDIR}/VERSION"

QJS_HASH="$(sinomo_hash "${QJS_WASM}")"
LICENSE_HASH="$(sinomo_hash "${OUTDIR}/LICENSE")"
VERSION_HASH="$(sinomo_hash "${OUTDIR}/VERSION")"

cat > "${OUTDIR}/package.json" <<PACKAGE
{
  "toolchain_id": "${QUICKJS_TOOLCHAIN_ID}",
  "version": "quickjs-${QUICKJS_RELEASE}-wasip1",
  "provenance": "Sinomo QuickJS WASIp1 recipe: QuickJS ${QUICKJS_RELEASE}, wasi-sdk ${WASI_SDK_VERSION_FULL}",
  "commands": [
    {
      "name": "qjs",
      "module": "qjs-${QUICKJS_RELEASE}-wasip1.wasm",
      "content_hash": "${QJS_HASH}",
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
      "relative_path": "share/quickjs/LICENSE",
      "file": "LICENSE",
      "content_hash": "${LICENSE_HASH}"
    },
    {
      "relative_path": "share/quickjs/VERSION",
      "file": "VERSION",
      "content_hash": "${VERSION_HASH}"
    }
  ]
}
PACKAGE

cat > "${OUTDIR}/quickjs-${QUICKJS_RELEASE}-build.manifest" <<MANIFEST
quickjs_release=${QUICKJS_RELEASE}
quickjs_source=${QUICKJS_URL}
quickjs_sha256=${QUICKJS_SHA256}
toolchain_id=${QUICKJS_TOOLCHAIN_ID}
wasi_sdk_version=${WASI_SDK_VERSION_FULL}
wasi_sdk_source=${WASI_SDK_URL}
wasi_sdk_sha256=${WASI_SDK_SHA256}
source_date_epoch=${SOURCE_DATE_EPOCH}
wasm=${QJS_WASM}
wasm_hash=${QJS_HASH}
package=${OUTDIR}/package.json
import_command=sinomo sandbox toolchain import-package --daemon ${OUTDIR}/package.json
MANIFEST

file "${QJS_WASM}"
echo "wrote ${QJS_WASM}"
echo "hash ${QJS_HASH}"
echo "package ${OUTDIR}/package.json"
