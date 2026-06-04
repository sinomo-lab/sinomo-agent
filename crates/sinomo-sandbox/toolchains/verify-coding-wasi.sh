#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../.." && pwd)"
TARGET_DIR="${TARGET_DIR:-"${REPO_ROOT}/target"}"
BUSYBOX_WORKDIR="${BUSYBOX_WORKDIR:-"${TARGET_DIR}/sinomo-coding-verify-busybox-work"}"
BUSYBOX_OUTDIR="${BUSYBOX_OUTDIR:-"${TARGET_DIR}/sinomo-coding-verify-busybox-out"}"
BASH_WORKDIR="${BASH_WORKDIR:-"${TARGET_DIR}/sinomo-coding-verify-bash-wasix-work"}"
BASH_OUTDIR="${BASH_OUTDIR:-"${TARGET_DIR}/sinomo-coding-verify-bash-wasix-out"}"
PYTHON_WORKDIR="${PYTHON_WORKDIR:-"${TARGET_DIR}/sinomo-coding-verify-python-wasix-work"}"
PYTHON_OUTDIR="${PYTHON_OUTDIR:-"${TARGET_DIR}/sinomo-coding-verify-python-wasix-out"}"
MAKE_WORKDIR="${MAKE_WORKDIR:-"${TARGET_DIR}/sinomo-coding-verify-make-wasix-work"}"
MAKE_OUTDIR="${MAKE_OUTDIR:-"${TARGET_DIR}/sinomo-coding-verify-make-wasix-out"}"
TCC_WORKDIR="${TCC_WORKDIR:-"${TARGET_DIR}/sinomo-coding-verify-tcc-wasix-work"}"
TCC_OUTDIR="${TCC_OUTDIR:-"${TARGET_DIR}/sinomo-coding-verify-tcc-wasix-out"}"
VERIFY_FULL="${VERIFY_FULL:-0}"
if [ -z "${SINOMO_VERIFY_DATA_DIR+x}" ]; then
    if [ "${VERIFY_FULL}" = "1" ]; then
        mkdir -p "${TARGET_DIR}"
        SINOMO_VERIFY_DATA_DIR="$(mktemp -d "${TARGET_DIR}/sinomo-coding-verify.XXXXXX")"
    else
        SINOMO_VERIFY_DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/sinomo-coding-verify.XXXXXX")"
    fi
    SINOMO_VERIFY_DATA_DIR_AUTO=1
else
    SINOMO_VERIFY_DATA_DIR_AUTO=0
fi
if [ "${VERIFY_FULL}" = "1" ]; then
    if [ -z "${SINOMO_SHELL_VERIFY_DATA_DIR+x}" ]; then
        mkdir -p "${TARGET_DIR}"
        SINOMO_SHELL_VERIFY_DATA_DIR="$(mktemp -d "${TARGET_DIR}/sinomo-coding-shell-verify.XXXXXX")"
        SINOMO_SHELL_VERIFY_DATA_DIR_AUTO=1
    else
        SINOMO_SHELL_VERIFY_DATA_DIR_AUTO=0
    fi
fi
SANDBOX_ID="${SANDBOX_ID:-01k4z8q5c00000000000000001}"
SHELL_SANDBOX_ID="${SHELL_SANDBOX_ID:-01k4z8q5d00000000000000001}"
BUSYBOX_VERSION="${BUSYBOX_VERSION:-busybox-1.36.1-wasix-coding-verify}"
SHELL_TOOLCHAIN_ID="${SHELL_TOOLCHAIN_ID:-086wdctfpphb26n9k2cg751qqs}"
KEEP_SANDBOX="${KEEP_SANDBOX:-0}"

cleanup() {
    if [ "${SINOMO_VERIFY_DATA_DIR_AUTO}" = "1" ] && [ "${KEEP_SANDBOX}" != "1" ]; then
        rm -rf "${SINOMO_VERIFY_DATA_DIR}"
    fi
    if [ "${VERIFY_FULL}" = "1" ] && [ "${SINOMO_SHELL_VERIFY_DATA_DIR_AUTO}" = "1" ] && [ "${KEEP_SANDBOX}" != "1" ]; then
        rm -rf "${SINOMO_SHELL_VERIFY_DATA_DIR}"
    fi
}
trap cleanup EXIT

if [ -n "${SINOMO_BIN:-}" ]; then
    SINOMO_CMD=("${SINOMO_BIN}")
else
    SINOMO_CMD=(cargo run --quiet --manifest-path "${REPO_ROOT}/Cargo.toml")
    SINOMO_CMD+=(--bin sinomo --)
fi

if [ "${SKIP_BUILD:-0}" != "1" ]; then
    echo "building BusyBox WASIX package"
    WORKDIR="${BUSYBOX_WORKDIR}" OUTDIR="${BUSYBOX_OUTDIR}" \
        "${ROOT}/busybox/build-busybox-wasi.sh"

    if [ "${VERIFY_FULL}" = "1" ]; then
        echo "building Bash WASIX package"
        WORKDIR="${BASH_WORKDIR}" OUTDIR="${BASH_OUTDIR}" \
            "${ROOT}/bash/build-bash-wasix.sh"

        echo "building CPython WASIX package"
        WORKDIR="${PYTHON_WORKDIR}" OUTDIR="${PYTHON_OUTDIR}" \
            "${ROOT}/python/build-cpython-wasix.sh"

        echo "building GNU Make WASIX package"
        WORKDIR="${MAKE_WORKDIR}" OUTDIR="${MAKE_OUTDIR}" \
            "${ROOT}/build-tools/build-make-wasix.sh"

        echo "building TinyCC WASIX package"
        WORKDIR="${TCC_WORKDIR}" OUTDIR="${TCC_OUTDIR}" \
            "${ROOT}/build-tools/build-tcc-wasix.sh"
    fi
fi

BUSYBOX_PACKAGE="${BUSYBOX_OUTDIR}/package.json"
if [ ! -f "${BUSYBOX_PACKAGE}" ]; then
    echo "coding verifier BusyBox package manifest missing: ${BUSYBOX_PACKAGE}; rerun without SKIP_BUILD=1" >&2
    exit 1
fi
PYTHON_PACKAGE="${PYTHON_OUTDIR}/package.json"
SHELL_PACKAGES=()
if [ "${VERIFY_FULL}" = "1" ]; then
    SHELL_PACKAGES=(
        "${BASH_OUTDIR}/package.json"
        "${MAKE_OUTDIR}/package.json"
        "${TCC_OUTDIR}/package.json"
    )
    if [ ! -f "${PYTHON_PACKAGE}" ]; then
        echo "coding verifier Python package manifest missing: ${PYTHON_PACKAGE}; rerun without SKIP_BUILD=1" >&2
        exit 1
    fi
    for package in "${SHELL_PACKAGES[@]}"; do
        if [ ! -f "${package}" ]; then
            echo "coding verifier full package manifest missing: ${package}; rerun without SKIP_BUILD=1" >&2
            exit 1
        fi
    done
fi

if [ "${VERIFY_FULL}" = "1" ]; then
    echo "verifying full coding package manifests"
    "${SINOMO_CMD[@]}" sandbox toolchain verify-package "${PYTHON_PACKAGE}" >/dev/null
    for package in "${SHELL_PACKAGES[@]}"; do
        "${SINOMO_CMD[@]}" sandbox toolchain verify-package "${package}" >/dev/null
    done
fi

echo "verifying BusyBox package manifest"
"${SINOMO_CMD[@]}" sandbox toolchain verify-package "${BUSYBOX_PACKAGE}" >/dev/null

echo "using disposable SINOMO_DATA_DIR=${SINOMO_VERIFY_DATA_DIR}"
SINOMO_DATA_DIR="${SINOMO_VERIFY_DATA_DIR}" "${SINOMO_CMD[@]}" \
    sandbox toolchain import-package \
    "${BUSYBOX_PACKAGE}"

if [ "${VERIFY_FULL}" = "1" ]; then
    SINOMO_DATA_DIR="${SINOMO_VERIFY_DATA_DIR}" "${SINOMO_CMD[@]}" \
        sandbox toolchain import-package \
        --into toolchain_busybox \
        "${PYTHON_PACKAGE}"
fi

SINOMO_DATA_DIR="${SINOMO_VERIFY_DATA_DIR}" "${SINOMO_CMD[@]}" \
    sandbox create \
    --id "${SANDBOX_ID}" \
    --toolchain toolchain_busybox

VERIFY_ARGS=(sandbox toolchain verify-coding "${SANDBOX_ID}" --allow-unsigned)
if [ "${VERIFY_FULL}" = "1" ]; then
    PYTHON_VERIFY_ARGS=(sandbox toolchain verify-python "${SANDBOX_ID}" --allow-unsigned)
fi

SINOMO_DATA_DIR="${SINOMO_VERIFY_DATA_DIR}" "${SINOMO_CMD[@]}" "${VERIFY_ARGS[@]}"
if [ "${VERIFY_FULL}" = "1" ]; then
    SINOMO_DATA_DIR="${SINOMO_VERIFY_DATA_DIR}" "${SINOMO_CMD[@]}" "${PYTHON_VERIFY_ARGS[@]}"

    echo "using disposable shell SINOMO_DATA_DIR=${SINOMO_SHELL_VERIFY_DATA_DIR}"
    SINOMO_DATA_DIR="${SINOMO_SHELL_VERIFY_DATA_DIR}" "${SINOMO_CMD[@]}" \
        sandbox toolchain import-package \
        "${BUSYBOX_PACKAGE}"
    for package in "${SHELL_PACKAGES[@]}"; do
        SINOMO_DATA_DIR="${SINOMO_SHELL_VERIFY_DATA_DIR}" "${SINOMO_CMD[@]}" \
            sandbox toolchain import-package \
            --into "${SHELL_TOOLCHAIN_ID}" \
            "${package}"
    done
    SINOMO_DATA_DIR="${SINOMO_SHELL_VERIFY_DATA_DIR}" "${SINOMO_CMD[@]}" \
        sandbox create \
        --id "${SHELL_SANDBOX_ID}" \
        --toolchain "${SHELL_TOOLCHAIN_ID}"
    SINOMO_DATA_DIR="${SINOMO_SHELL_VERIFY_DATA_DIR}" "${SINOMO_CMD[@]}" \
        sandbox toolchain verify-bash "${SHELL_SANDBOX_ID}" --allow-unsigned
    SINOMO_DATA_DIR="${SINOMO_SHELL_VERIFY_DATA_DIR}" "${SINOMO_CMD[@]}" \
        sandbox toolchain verify-make "${SHELL_SANDBOX_ID}" --allow-unsigned
    SINOMO_DATA_DIR="${SINOMO_SHELL_VERIFY_DATA_DIR}" "${SINOMO_CMD[@]}" \
        sandbox toolchain verify-tcc "${SHELL_SANDBOX_ID}" --allow-unsigned
fi
