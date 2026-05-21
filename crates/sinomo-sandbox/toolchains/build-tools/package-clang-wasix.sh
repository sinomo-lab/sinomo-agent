#!/usr/bin/env bash
set -euo pipefail

LLVM_VERSION="${LLVM_VERSION:-21.1.6}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
TARGET_DIR="${TARGET_DIR:-"${REPO_ROOT}/target"}"
OUTDIR="${OUTDIR:-"${TARGET_DIR}/sinomo-build-tools-clang-wasix-out"}"
PACKAGE_SYSROOT="${OUTDIR}/artifacts/sysroot"

if [ -z "${WASIX_SYSROOT:-}" ]; then
    echo "WASIX_SYSROOT must point to a prepared WASIX sysroot." >&2
    echo "Prepare it explicitly before packaging Clang." >&2
    echo "Example: SYSROOT_OUTDIR=target/wasix-sysroot ${ROOT}/prepare-wasix-sysroot.sh" >&2
    echo "         WASIX_SYSROOT=target/wasix-sysroot OUTDIR=${OUTDIR} ${ROOT}/package-clang-wasix.sh" >&2
    exit 1
fi
if [ ! -f "${WASIX_SYSROOT}/include/unistd.h" ] || [ ! -f "${WASIX_SYSROOT}/lib/wasm32-wasi/libc.a" ]; then
    echo "prepared WASIX sysroot missing or incomplete: ${WASIX_SYSROOT}" >&2
    exit 1
fi
SYSROOT_SOURCE_REAL="$(cd "${WASIX_SYSROOT}" && pwd -P)"
PACKAGE_SYSROOT_PARENT="$(dirname "${PACKAGE_SYSROOT}")"
mkdir -p "${PACKAGE_SYSROOT_PARENT}"
PACKAGE_SYSROOT_REAL_PARENT="$(cd "${PACKAGE_SYSROOT_PARENT}" && pwd -P)"
PACKAGE_SYSROOT_REAL="${PACKAGE_SYSROOT_REAL_PARENT}/$(basename "${PACKAGE_SYSROOT}")"
if [ "${SYSROOT_SOURCE_REAL}" != "${PACKAGE_SYSROOT_REAL}" ]; then
    echo "copying prepared WASIX sysroot into Clang package"
    rm -rf "${PACKAGE_SYSROOT}"
    mkdir -p "${PACKAGE_SYSROOT}"
    cp -a "${WASIX_SYSROOT}/." "${PACKAGE_SYSROOT}/"
else
    echo "using prepared WASIX sysroot already in Clang package layout"
fi

echo "writing Clang/LLVM WASIX package manifest"
OUTDIR="${OUTDIR}" LLVM_VERSION="${LLVM_VERSION}" \
    "${ROOT}/write-clang-wasix-package-manifest.sh"
