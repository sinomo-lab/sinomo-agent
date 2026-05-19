#!/usr/bin/env bash
set -euo pipefail

GNU_MAKE_VERSION="${GNU_MAKE_VERSION:-4.4.1}"
MAKE_SHA256="${MAKE_SHA256:-dd16fb1d67bfab79a72f5e8390735c49e3e8e70b4945a15ab1f81ddb78658fb3}"
MAKE_TOOLCHAIN_ID="${MAKE_TOOLCHAIN_ID:-01k4z8q5d00000000000000000}"
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
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1711843200}"
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
WORKDIR="${WORKDIR:-"${ROOT}/work"}"
OUTDIR="${OUTDIR:-"${ROOT}/out"}"

MAKE_TARBALL="make-${GNU_MAKE_VERSION}.tar.gz"
MAKE_URL="https://ftp.gnu.org/gnu/make/${MAKE_TARBALL}"
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

download "${MAKE_URL}" "${WORKDIR}/downloads/${MAKE_TARBALL}"
download "${WASI_SDK_URL}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
download "${BINARYEN_URL}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
download "${WASIX_LIBC_URL}" "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"
verify_sha256 "${MAKE_SHA256}" "${WORKDIR}/downloads/${MAKE_TARBALL}"
verify_sha256 "${WASI_SDK_SHA256}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
verify_sha256 "${BINARYEN_SHA256}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
verify_sha256 "${WASIX_LIBC_SHA256}" "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"

rm -rf \
    "${WORKDIR}/make-${GNU_MAKE_VERSION}" \
    "${WORKDIR}"/wasi-sdk-${WASI_SDK_VERSION_FULL}* \
    "${WORKDIR}"/binaryen-version_${BINARYEN_VERSION} \
    "${WORKDIR}/wasix-sysroot"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${MAKE_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
mkdir -p "${WORKDIR}/wasix-sysroot"
tar -C "${WORKDIR}/wasix-sysroot" -xzf "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"

MAKE_SRC="${WORKDIR}/make-${GNU_MAKE_VERSION}"
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

# GNU Make's mocked glob dirent allocation assumes d_name has a complete array
# type. wasix-libc exposes it as a flexible array, so use the standard offset.
perl -0pi -e 's/size_t sz = sizeof \(\*d\) - sizeof \(d->d_name\) \+ len;/size_t sz = offsetof (struct dirent, d_name) + len;/' \
    "${MAKE_SRC}/src/dir.c"

MAKE_COMPAT_SOURCE="${ROOT}/make/wasix/make-compat.c"
MAKE_COMPAT_OBJECT="${WORKDIR}/sinomo-make-wasix-compat.o"

export SOURCE_DATE_EPOCH
export CC="${WASI_SDK}/bin/clang --target=wasm32-wasi --sysroot=${WASIX_SYSROOT}"
export AR="${WASI_SDK}/bin/llvm-ar"
export RANLIB="${WASI_SDK}/bin/llvm-ranlib"
export CFLAGS="-Os -fno-ident -matomics -mbulk-memory -mmutable-globals -pthread -mthread-model posix -fno-exceptions -ftls-model=local-exec -ffile-prefix-map=${WORKDIR}=sinomo-make-build -ffile-prefix-map=${REPO_ROOT}=sinomo -DNO_OUTPUT_SYNC -D_WASI_EMULATED_SIGNAL -D_WASI_EMULATED_PROCESS_CLOCKS"
export LDFLAGS="-Wl,--export-all -Wl,--export-dynamic -Wl,--shared-memory -Wl,--import-memory -Wl,--extra-features=atomics -Wl,--extra-features=bulk-memory -Wl,--extra-features=mutable-globals -Wl,--export=__wasm_call_ctors -Wl,--export=__wasm_init_tls -Wl,--export=__wasm_signal -Wl,--export-if-defined=__indirect_function_table -Wl,--export-if-defined=__stack_pointer -Wl,--export-if-defined=__heap_base -Wl,--export-if-defined=__data_end -Wl,-z,stack-size=1048576 -Wl,--initial-memory=67108864 -Wl,--max-memory=268435456"

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
    -c "${MAKE_COMPAT_SOURCE}" \
    -o "${MAKE_COMPAT_OBJECT}"

cd "${MAKE_SRC}"
CONFIG_SITE=/dev/null ./configure \
    --host=wasm32-wasi \
    --build="$(sh ./build-aux/config.guess)" \
    --disable-nls \
    --without-guile \
    --disable-load
MAKE_OBJECTS=(
    src/ar.o
    src/arscan.o
    src/commands.o
    src/default.o
    src/dir.o
    src/expand.o
    src/file.o
    src/function.o
    src/getopt.o
    src/getopt1.o
    src/guile.o
    src/hash.o
    src/implicit.o
    src/job.o
    src/load.o
    src/loadapi.o
    src/main.o
    src/misc.o
    src/output.o
    src/read.o
    src/remake.o
    src/rule.o
    src/shuffle.o
    src/signame.o
    src/strcache.o
    src/variable.o
    src/version.o
    src/vpath.o
    src/posixos.o
    src/remote-stub.o
)
make -C lib alloca.h fnmatch.h glob.h
make -C lib -j"${JOBS}" all-recursive
make -j"${JOBS}" "${MAKE_OBJECTS[@]}"

MAKE_WASM_UNOPTIMIZED="${OUTDIR}/make-${GNU_MAKE_VERSION}-wasix.unoptimized.wasm"
MAKE_WASM="${OUTDIR}/make-${GNU_MAKE_VERSION}-wasix.wasm"
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
    "-ffile-prefix-map=${WORKDIR}=sinomo-make-build" \
    "-ffile-prefix-map=${REPO_ROOT}=sinomo" \
    -DNO_OUTPUT_SYNC \
    -D_WASI_EMULATED_SIGNAL \
    -D_WASI_EMULATED_PROCESS_CLOCKS \
    ${LDFLAGS} \
    -o "${MAKE_WASM_UNOPTIMIZED}" \
    "${MAKE_OBJECTS[@]}" \
    "${MAKE_COMPAT_OBJECT}" \
    lib/libgnu.a \
    -lwasi-emulated-process-clocks \
    -lrt \
    -lm \
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
    -o "${MAKE_WASM}" \
    "${MAKE_WASM_UNOPTIMIZED}"

cp "${MAKE_SRC}/COPYING" "${OUTDIR}/COPYING"
printf '%s\n' "${GNU_MAKE_VERSION}" > "${OUTDIR}/VERSION"

MAKE_HASH="$(sinomo_hash "${MAKE_WASM}")"
COPYING_HASH="$(sinomo_hash "${OUTDIR}/COPYING")"
VERSION_HASH="$(sinomo_hash "${OUTDIR}/VERSION")"

cat > "${OUTDIR}/package.json" <<PACKAGE
{
  "toolchain_id": "${MAKE_TOOLCHAIN_ID}",
  "version": "make-${GNU_MAKE_VERSION}-wasix",
  "provenance": "Sinomo GNU Make WASIX recipe: GNU Make ${GNU_MAKE_VERSION}, wasi-sdk ${WASI_SDK_VERSION_FULL}",
  "target_abi": "wasix",
  "commands": [
    {
      "name": "make",
      "module": "make-${GNU_MAKE_VERSION}-wasix.wasm",
      "content_hash": "${MAKE_HASH}",
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
      "relative_path": "share/make/COPYING",
      "file": "COPYING",
      "content_hash": "${COPYING_HASH}"
    },
    {
      "relative_path": "share/make/VERSION",
      "file": "VERSION",
      "content_hash": "${VERSION_HASH}"
    }
  ]
}
PACKAGE

cat > "${OUTDIR}/make-${GNU_MAKE_VERSION}-build.manifest" <<MANIFEST
make_version=${GNU_MAKE_VERSION}
make_source=${MAKE_URL}
make_sha256=${MAKE_SHA256}
toolchain_id=${MAKE_TOOLCHAIN_ID}
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
wasm=${MAKE_WASM}
wasm_hash=${MAKE_HASH}
package=${OUTDIR}/package.json
import_command=sinomo sandbox toolchain import-package --daemon --into toolchain_busybox ${OUTDIR}/package.json
MANIFEST

file "${MAKE_WASM}"
echo "wrote ${MAKE_WASM}"
echo "hash ${MAKE_HASH}"
echo "package ${OUTDIR}/package.json"
