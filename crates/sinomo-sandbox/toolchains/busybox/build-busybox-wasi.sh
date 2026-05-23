#!/usr/bin/env bash
set -euo pipefail

BUSYBOX_VERSION="${BUSYBOX_VERSION:-1.36.1}"
BUSYBOX_SHA256="${BUSYBOX_SHA256:-b8cc24c9574d809e7279c3be349795c5d5ceb6fdf19ca709f80cde50e47de314}"
BUSYBOX_TARGET_ABI="${BUSYBOX_TARGET_ABI:-wasix}"
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
WASIX_LIBC_SOURCE="${WASIX_LIBC_SOURCE:-vendor/wasix-libc}"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1719878400}"
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
WORKDIR="${WORKDIR:-"${ROOT}/work"}"
OUTDIR="${OUTDIR:-"${ROOT}/out"}"
mkdir -p "${WORKDIR}" "${OUTDIR}"
WORKDIR="$(cd "${WORKDIR}" && pwd -P)"
OUTDIR="$(cd "${OUTDIR}" && pwd -P)"
FRAGMENT="${FRAGMENT:-"${ROOT}/busybox-wasi.fragment"}"
SINOMO_PREPARE_WASIX_SYSROOT="${REPO_ROOT}/crates/sinomo-sandbox/toolchains/build-tools/prepare-wasix-sysroot.sh"

BUSYBOX_TARBALL="busybox-${BUSYBOX_VERSION}.tar.bz2"
BUSYBOX_URL="https://busybox.net/downloads/${BUSYBOX_TARBALL}"
BINARYEN_TARBALL="binaryen-version_${BINARYEN_VERSION}-${BINARYEN_ARCH}-${BINARYEN_OS}.tar.gz"
BINARYEN_URL="https://github.com/WebAssembly/binaryen/releases/download/version_${BINARYEN_VERSION}/${BINARYEN_TARBALL}"
WASI_SDK_URL="https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_SDK_VERSION}/wasi-sdk-${WASI_SDK_VERSION_FULL}-${WASI_SDK_ARCH}-${WASI_SDK_OS}.tar.gz"

case "${BUSYBOX_TARGET_ABI}" in
    wasix) ;;
    *)
        echo "unsupported BUSYBOX_TARGET_ABI=${BUSYBOX_TARGET_ABI}; BusyBox now builds only the WASIX package target" >&2
        exit 1
        ;;
esac

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

enable_config() {
    local key="$1"
    set_config "${key}" "y"
}

disable_config() {
    local key="$1"
    set_config "${key}" "n"
}

set_config() {
    local key="$1"
    local value="$2"
    local config="${BUSYBOX_SRC}/.config"
    local enabled="CONFIG_${key}=y"
    local disabled="# CONFIG_${key} is not set"

    if grep -q -E "^(# )?CONFIG_${key}(=| is not set)" "${config}"; then
        if [ "${value}" = "y" ]; then
            sed -i -E "s@^(# )?CONFIG_${key}(=.*| is not set)@${enabled}@" "${config}"
        else
            sed -i -E "s@^(# )?CONFIG_${key}(=.*| is not set)@${disabled}@" "${config}"
        fi
    elif [ "${value}" = "y" ]; then
        printf '%s\n' "${enabled}" >> "${config}"
    else
        printf '%s\n' "${disabled}" >> "${config}"
    fi
}

mkdir -p "${WORKDIR}" "${OUTDIR}" "${WORKDIR}/downloads"

download "${BUSYBOX_URL}" "${WORKDIR}/downloads/${BUSYBOX_TARBALL}"
download "${BINARYEN_URL}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
verify_sha256 "${BUSYBOX_SHA256}" "${WORKDIR}/downloads/${BUSYBOX_TARBALL}"
verify_sha256 "${BINARYEN_SHA256}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
mkdir -p "${WORKDIR}/downloads"

rm -rf "${WORKDIR}/busybox-${BUSYBOX_VERSION}" "${WORKDIR}"/binaryen-version_${BINARYEN_VERSION} "${WORKDIR}/sinomo-wasix-sysroot" "${WORKDIR}/sinomo-wasix-sysroot-work"
tar -C "${WORKDIR}" -xjf "${WORKDIR}/downloads/${BUSYBOX_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${BINARYEN_TARBALL}"

WASIX_SYSROOT="${WORKDIR}/sinomo-wasix-sysroot/sysroot"
SINOMO_WASIX_HOST_ENV="${WORKDIR}/sinomo-wasix-sysroot/host-env.sh"
WORKDIR="${WORKDIR}/sinomo-wasix-sysroot-work" \
SYSROOT_OUTDIR="${WASIX_SYSROOT}" \
HOST_ENV_OUT="${SINOMO_WASIX_HOST_ENV}" \
WASI_SDK_VERSION="${WASI_SDK_VERSION}" \
WASI_SDK_VERSION_FULL="${WASI_SDK_VERSION_FULL}" \
WASI_SDK_ARCH="${WASI_SDK_ARCH}" \
WASI_SDK_OS="${WASI_SDK_OS}" \
WASI_SDK_SHA256="${WASI_SDK_SHA256}" \
WASIX_LIBC_VERSION="${WASIX_LIBC_VERSION}" \
"${SINOMO_PREPARE_WASIX_SYSROOT}"
# shellcheck source=/dev/null
. "${SINOMO_WASIX_HOST_ENV}"

BUSYBOX_SRC="${WORKDIR}/busybox-${BUSYBOX_VERSION}"
WASI_SDK="${SINOMO_WASI_SDK}"
if [ -z "${WASI_SDK}" ] || [ ! -x "${WASI_SDK}/bin/clang" ]; then
    echo "wasi-sdk clang not found after preparing the Sinomo WASIX sysroot" >&2
    exit 1
fi
BINARYEN="${WORKDIR}/binaryen-version_${BINARYEN_VERSION}"
if [ ! -x "${BINARYEN}/bin/wasm-opt" ]; then
    echo "Binaryen wasm-opt not found after extracting ${BINARYEN_TARBALL}" >&2
    exit 1
fi
if [ ! -f "${WASIX_SYSROOT}/include/unistd.h" ] || [ ! -f "${WASIX_SYSROOT}/lib/wasm32-wasi/libc.a" ]; then
    echo "Sinomo WASIX sysroot not found after preparation" >&2
    exit 1
fi
mkdir -p "${WORKDIR}/bin"
cat > "${WORKDIR}/bin/sinomo-wasix-cc" <<EOF
#!/usr/bin/env bash
set -euo pipefail
args=()
for arg in "\$@"; do
    case "\${arg}" in
        -Wl,--start-group|-Wl,--end-group|-Wl,--sort-common|-Wl,--sort-section,alignment|-Wl,--warn-common)
            ;;
        *)
            args+=("\${arg}")
            ;;
    esac
done
exec "${WASI_SDK}/bin/clang" "\${args[@]}"
EOF
chmod +x "${WORKDIR}/bin/sinomo-wasix-cc"
export SOURCE_DATE_EPOCH
export KCONFIG_NOTIMESTAMP=1

make -C "${BUSYBOX_SRC}" allnoconfig

while IFS= read -r line; do
    case "${line}" in
        "") continue ;;
        "# CONFIG_"*" is not set")
            key="${line#"# CONFIG_"}"
            disable_config "${key%" is not set"}"
            ;;
        \#*) continue ;;
        CONFIG_*=y)
            key="${line#CONFIG_}"
            enable_config "${key%=y}"
            ;;
        CONFIG_*=n)
            key="${line#CONFIG_}"
            disable_config "${key%=n}"
            ;;
        *)
            echo "unsupported config fragment line: ${line}" >&2
            exit 1
            ;;
    esac
done < "${FRAGMENT}"

set +o pipefail
yes "" | make -C "${BUSYBOX_SRC}" oldconfig
oldconfig_status="${PIPESTATUS[1]}"
set -o pipefail
if [ "${oldconfig_status}" -ne 0 ]; then
    exit "${oldconfig_status}"
fi

BUSYBOX_TARGET_TRIPLE="wasm32-wasi"
BUSYBOX_TARGET_SUFFIX="wasix"
BUSYBOX_SYSROOT="${WASIX_SYSROOT}"
WASI_CC="${WORKDIR}/bin/sinomo-wasix-cc --target=${BUSYBOX_TARGET_TRIPLE} --sysroot=${BUSYBOX_SYSROOT}"
WASI_CC_FLAGS="-Os -D_WASI_EMULATED_SIGNAL -D_WASI_EMULATED_MMAN -D_WASI_EMULATED_PROCESS_CLOCKS -D_WASI_EMULATED_GETPID -fno-ident -fdebug-prefix-map=${BUSYBOX_SRC}=busybox-${BUSYBOX_VERSION} -matomics -mbulk-memory -mmutable-globals -pthread -mthread-model posix -ftls-model=local-exec -msimd128 -mrelaxed-simd -mextended-const"
WASI_LD_FLAGS="-Wl,--export-all -Wl,--export-dynamic -Wl,--shared-memory -Wl,--import-memory -Wl,--extra-features=atomics -Wl,--extra-features=bulk-memory -Wl,--extra-features=mutable-globals -Wl,--export=__wasm_call_ctors -Wl,--export=__wasm_init_tls -Wl,--export=__wasm_signal -Wl,--export=__tls_size -Wl,--export=__tls_align -Wl,--export=__tls_base -Wl,--export-if-defined=__indirect_function_table -Wl,--export-if-defined=__stack_pointer -Wl,--export-if-defined=__heap_base -Wl,--export-if-defined=__data_end -Wl,-z,stack-size=1048576 -Wl,--initial-memory=67108864 -Wl,--max-memory=268435456"
WASI_LD_LIBS="-lwasi-emulated-getpid -lwasi-emulated-mman -lwasi-emulated-process-clocks -lresolv -lrt -lm -lpthread -lutil"

make -C "${BUSYBOX_SRC}" \
    -j"${JOBS}" \
    HOSTCC=gcc \
    CC="${WASI_CC}" \
    LD="${WASI_CC}" \
    STRIP="${WASI_SDK}/bin/llvm-strip" \
    CFLAGS="${WASI_CC_FLAGS}" \
    LDFLAGS="${WASI_LD_FLAGS}" \
    LDLIBS="${WASI_LD_LIBS}" \
    busybox

mkdir -p "${OUTDIR}"
BUSYBOX_UNOPTIMIZED_WASM="${OUTDIR}/busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX}.unoptimized.wasm"
BUSYBOX_WASM="${OUTDIR}/busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX}.wasm"
cp "${BUSYBOX_SRC}/busybox" "${BUSYBOX_UNOPTIMIZED_WASM}"
"${BINARYEN}/bin/wasm-opt" \
    --asyncify \
    --fpcast-emu \
    -Os \
    --no-validation \
    --enable-threads \
    --enable-threads \
    --enable-mutable-globals \
    --enable-bulk-memory \
    --enable-bulk-memory-opt \
    --enable-exception-handling \
    --enable-simd \
    --enable-relaxed-simd \
    --enable-extended-const \
    -o "${BUSYBOX_WASM}" \
    "${BUSYBOX_UNOPTIMIZED_WASM}"
cp "${BUSYBOX_SRC}/.config" "${OUTDIR}/busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX}.config"

BUSYBOX_WASM_SHA256="$(sha256sum "${BUSYBOX_WASM}" | awk '{print $1}')"
BUSYBOX_WASM_HASH="$(sinomo_hash "${BUSYBOX_WASM}")"

cat > "${OUTDIR}/busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX}.manifest" <<MANIFEST
busybox_version=${BUSYBOX_VERSION}
busybox_target_abi=${BUSYBOX_TARGET_ABI}
busybox_source=${BUSYBOX_URL}
busybox_sha256=${BUSYBOX_SHA256}
wasi_sdk_version=${WASI_SDK_VERSION_FULL}
wasi_sdk_source=${WASI_SDK_URL}
wasi_sdk_sha256=${WASI_SDK_SHA256}
MANIFEST

cat >> "${OUTDIR}/busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX}.manifest" <<MANIFEST
binaryen_version=${BINARYEN_VERSION}
binaryen_source=${BINARYEN_URL}
binaryen_sha256=${BINARYEN_SHA256}
wasix_libc_version=${WASIX_LIBC_VERSION}
wasix_libc_source=${WASIX_LIBC_SOURCE}
MANIFEST

cat >> "${OUTDIR}/busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX}.manifest" <<MANIFEST
source_date_epoch=${SOURCE_DATE_EPOCH}
wasm=${BUSYBOX_WASM}
wasm_sha256=${BUSYBOX_WASM_SHA256}
wasm_hash=${BUSYBOX_WASM_HASH}
import_command=sinomo sandbox toolchain import-package /package.json
MANIFEST

cat > "${OUTDIR}/package.json" <<PACKAGE
{
  "toolchain_id": "086wdctfpphb26n9k2cg751qqs",
  "version": "busybox-${BUSYBOX_VERSION}-wasix",
  "provenance": "Sinomo BusyBox WASIX recipe: BusyBox ${BUSYBOX_VERSION}, wasi-sdk ${WASI_SDK_VERSION_FULL}",
  "target_abi": "wasix",
  "commands": [
    "awk", "cat", "chmod", "cp", "cmp", "date", "diff", "dirname", "echo", "env", "false", "find", "grep", "gzip", "head", "ln", "ls", "mkdir", "mv", "patch", "printf", "pwd", "rm", "rmdir", "sed", "sha256sum", "sh", "sort", "tail", "tar", "test", "touch", "true", "tr", "uniq", "uname", "wc"
  ],
  "artifacts": []
}
PACKAGE
python3 - "${OUTDIR}/package.json" "${BUSYBOX_WASM}" "${BUSYBOX_WASM_HASH}" <<'PY'
import json
import sys
from pathlib import Path

package_path = Path(sys.argv[1])
module_path = Path(sys.argv[2])
module_hash = sys.argv[3]
package = json.loads(package_path.read_text())
package["commands"] = [
    {
        "name": name,
        "module": module_path.name,
        "content_hash": module_hash,
        "default_limits": {
            "memory_mb": 256,
            "wall_time_ms": 10000,
            "max_input_bytes": 1048576,
            "max_output_bytes": 1048576,
            "max_workspace_bytes": 1073741824,
            "max_written_bytes_per_command": 67108864,
            "max_file_count": 100000,
        },
    }
    for name in package["commands"]
]
package_path.write_text(json.dumps(package, indent=2) + "\n")
PY

file "${BUSYBOX_WASM}"
echo "wrote ${BUSYBOX_WASM}"
echo "sha256 ${BUSYBOX_WASM_SHA256}"
echo "hash ${BUSYBOX_WASM_HASH}"
echo "manifest ${OUTDIR}/busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX}.manifest"
echo "package ${OUTDIR}/package.json"
