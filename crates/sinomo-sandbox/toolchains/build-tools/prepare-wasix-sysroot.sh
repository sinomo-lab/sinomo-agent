#!/usr/bin/env bash
set -euo pipefail

WASI_SDK_VERSION="${WASI_SDK_VERSION:-24}"
WASI_SDK_VERSION_FULL="${WASI_SDK_VERSION_FULL:-24.0}"
WASI_SDK_ARCH="${WASI_SDK_ARCH:-x86_64}"
WASI_SDK_OS="${WASI_SDK_OS:-linux}"
WASI_SDK_SHA256="${WASI_SDK_SHA256:-c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5}"
WASIX_LIBC_VERSION="${WASIX_LIBC_VERSION:-v2026-05-12.1}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
TARGET_DIR="${TARGET_DIR:-"${REPO_ROOT}/target"}"
WORKDIR="${WORKDIR:-"${TARGET_DIR}/sinomo-build-tools-wasix-sysroot-work"}"
SYSROOT_OUTDIR="${SYSROOT_OUTDIR:-"${TARGET_DIR}/sinomo-build-tools-wasix-sysroot/sysroot"}"
HOST_ENV_OUT="${HOST_ENV_OUT:-}"
SINOMO_WASIX_LIBC_SOURCE="${SINOMO_WASIX_LIBC_SOURCE:-"${REPO_ROOT}/modules/sandbox/wasix/vendor/wasix-libc"}"

WASI_SDK_TARBALL="wasi-sdk-${WASI_SDK_VERSION_FULL}-${WASI_SDK_ARCH}-${WASI_SDK_OS}.tar.gz"
WASI_SDK_URL="https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_SDK_VERSION}/${WASI_SDK_TARBALL}"

mkdir -p "${WORKDIR}" "$(dirname "${SYSROOT_OUTDIR}")"
WORKDIR="$(cd "${WORKDIR}" && pwd -P)"
SYSROOT_OUTDIR_PARENT="$(cd "$(dirname "${SYSROOT_OUTDIR}")" && pwd -P)"
SYSROOT_OUTDIR="${SYSROOT_OUTDIR_PARENT}/$(basename "${SYSROOT_OUTDIR}")"
if [ -n "${HOST_ENV_OUT}" ]; then
    mkdir -p "$(dirname "${HOST_ENV_OUT}")"
    HOST_ENV_OUT_PARENT="$(cd "$(dirname "${HOST_ENV_OUT}")" && pwd -P)"
    HOST_ENV_OUT="${HOST_ENV_OUT_PARENT}/$(basename "${HOST_ENV_OUT}")"
fi

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
    if [ -z "${expected}" ]; then
        echo "sha256 not pinned for ${file}" >&2
        exit 1
    fi
    local actual
    actual="$(sha256sum "${file}" | awk '{print $1}')"
    if [ "${actual}" != "${expected}" ]; then
        echo "sha256 mismatch for ${file}: expected ${expected}, actual ${actual}" >&2
        exit 1
    fi
}

if [ ! -f "${SINOMO_WASIX_LIBC_SOURCE}/Makefile" ] || [ ! -d "${SINOMO_WASIX_LIBC_SOURCE}/sinomo/libc" ]; then
    echo "Sinomo WASIX libc source tree not found: ${SINOMO_WASIX_LIBC_SOURCE}" >&2
    exit 1
fi

mkdir -p "${WORKDIR}/downloads"
download "${WASI_SDK_URL}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
verify_sha256 "${WASI_SDK_SHA256}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"

rm -rf \
    "${WORKDIR}"/wasi-sdk-${WASI_SDK_VERSION_FULL}* \
    "${WORKDIR}/wasix-libc-build" \
    "${SYSROOT_OUTDIR}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"

WASI_SDK="$(find "${WORKDIR}" -maxdepth 1 -type d -name "wasi-sdk-${WASI_SDK_VERSION_FULL}*" | sort | head -n 1)"
if [ -z "${WASI_SDK}" ] || [ ! -x "${WASI_SDK}/bin/clang" ]; then
    echo "wasi-sdk clang not found after extracting ${WASI_SDK_TARBALL}" >&2
    exit 1
fi

make -C "${SINOMO_WASIX_LIBC_SOURCE}" \
    TARGET_ARCH=wasm32 \
    THREAD_MODEL=posix \
    CHECK_SYMBOLS=no \
    CC="${WASI_SDK}/bin/clang" \
    AR="${WASI_SDK}/bin/llvm-ar" \
    NM="${WASI_SDK}/bin/llvm-nm" \
    SYSROOT="${SYSROOT_OUTDIR}" \
    OBJDIR="${WORKDIR}/wasix-libc-build/wasm32-wasi" \
    default

cat >"${SYSROOT_OUTDIR}/sinomo-wasix-sysroot.manifest" <<EOF
sinomo_sysroot_format=2
wasi_sdk_version=${WASI_SDK_VERSION_FULL}
wasi_sdk_source=${WASI_SDK_URL}
wasi_sdk_sha256=${WASI_SDK_SHA256}
wasix_libc_source=vendor/wasix-libc
wasix_libc_base=${WASIX_LIBC_VERSION}
EOF

if [ -n "${HOST_ENV_OUT}" ]; then
    mkdir -p "$(dirname "${HOST_ENV_OUT}")"
    cat >"${HOST_ENV_OUT}" <<EOF
# Source this file when building Sinomo WASIX command modules outside the sandbox
# with the same sysroot input set used by the sandbox toolchain packages.
export SINOMO_WASI_SDK="${WASI_SDK}"
export SINOMO_WASIX_SYSROOT="${SYSROOT_OUTDIR}"
export CC="\${SINOMO_WASI_SDK}/bin/clang --target=wasm32-wasi --sysroot=\${SINOMO_WASIX_SYSROOT}"
export CXX="\${SINOMO_WASI_SDK}/bin/clang++ --target=wasm32-wasi --sysroot=\${SINOMO_WASIX_SYSROOT}"
export AR="\${SINOMO_WASI_SDK}/bin/llvm-ar"
export RANLIB="\${SINOMO_WASI_SDK}/bin/llvm-ranlib"
EOF
fi

echo "prepared WASIX sysroot: ${SYSROOT_OUTDIR}"
