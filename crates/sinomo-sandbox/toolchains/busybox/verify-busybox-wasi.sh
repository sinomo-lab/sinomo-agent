#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
WORKDIR="${WORKDIR:-${ROOT}/target/sinomo-busybox-verify-work}"
OUTDIR="${OUTDIR:-${ROOT}/target/sinomo-busybox-verify-out}"
DATA_DIR="${DATA_DIR:-${ROOT}/target/sinomo-busybox-verify-data}"
VERSION_SUFFIX="${VERSION_SUFFIX:-verify}"
PROVENANCE="${PROVENANCE:-Sinomo BusyBox WASIX recipe verification}"
TOOLCHAIN_ID="${TOOLCHAIN_ID:-086wdctfpphb26n9k2cg751qqs}"

cd "${ROOT}"

chmod -R u+w "${WORKDIR}" "${OUTDIR}" "${DATA_DIR}" 2>/dev/null || true
rm -rf "${WORKDIR}" "${OUTDIR}" "${DATA_DIR}"
mkdir -p "${WORKDIR}" "${OUTDIR}" "${DATA_DIR}"

WORKDIR="${WORKDIR}" OUTDIR="${OUTDIR}" "${ROOT}/crates/sinomo-sandbox/toolchains/busybox/build-busybox-wasi.sh"

PACKAGE="${OUTDIR}/package.json"
SINOMO_DATA_DIR="${DATA_DIR}" cargo run --quiet --bin sinomo -- \
    sandbox toolchain verify-package \
    "${PACKAGE}"
SINOMO_DATA_DIR="${DATA_DIR}" cargo run --quiet --bin sinomo -- \
    sandbox toolchain import-package \
    "${PACKAGE}"

SANDBOX_ID="$(
    SINOMO_DATA_DIR="${DATA_DIR}" cargo run --quiet --bin sinomo -- \
        sandbox create --toolchain "${TOOLCHAIN_ID}" |
    awk '/^sandbox created:/ { print $3 }'
)"

if [[ -z "${SANDBOX_ID}" ]]; then
    echo "failed to create verification sandbox" >&2
    exit 1
fi

SINOMO_DATA_DIR="${DATA_DIR}" cargo run --quiet --bin sinomo -- \
    sandbox toolchain verify-busybox "${SANDBOX_ID}" --allow-unsigned
