#!/usr/bin/env bash
set -euo pipefail

LLVM_VERSION="${LLVM_VERSION:-21.1.6}"
LLVM_PROJECT_SHA256="${LLVM_PROJECT_SHA256:-ae67086eb04bed7ca11ab880349b5f1ab6f50e1b88cda376eaf8a845b935762b}"
WASI_SDK_VERSION="${WASI_SDK_VERSION:-24}"
WASI_SDK_VERSION_FULL="${WASI_SDK_VERSION_FULL:-24.0}"
WASI_SDK_ARCH="${WASI_SDK_ARCH:-x86_64}"
WASI_SDK_OS="${WASI_SDK_OS:-linux}"
WASI_SDK_SHA256="${WASI_SDK_SHA256:-c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5}"
BINARYEN_VERSION="${BINARYEN_VERSION:-124}"
BINARYEN_ARCH="${BINARYEN_ARCH:-x86_64}"
BINARYEN_OS="${BINARYEN_OS:-linux}"
BINARYEN_SHA256="${BINARYEN_SHA256:-0290c3779fedf592b8da0ded3032ff55c41a2b7b7bac6f0e6e28963}"
CLANG_OPTIMIZE_WASM="${CLANG_OPTIMIZE_WASM:-0}"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1719878400}"
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
TARGET_DIR="${TARGET_DIR:-"${REPO_ROOT}/target"}"
WORKDIR="${WORKDIR:-"${TARGET_DIR}/sinomo-build-tools-clang-wasix-work"}"
OUTDIR="${OUTDIR:-"${TARGET_DIR}/sinomo-build-tools-clang-wasix-out"}"
CLANG_BUILD_TMPDIR="${CLANG_BUILD_TMPDIR:-"${WORKDIR}/tmp"}"
mkdir -p "${CLANG_BUILD_TMPDIR}"
export TMPDIR="${TMPDIR:-"${CLANG_BUILD_TMPDIR}"}"

if [ -z "${WASIX_SYSROOT:-}" ]; then
    echo "WASIX_SYSROOT must point to a prepared WASIX sysroot." >&2
    echo "Prepare one with prepare-wasix-sysroot.sh before running build-clang-wasix.sh." >&2
    exit 1
fi

LLVM_TARBALL="llvm-project-${LLVM_VERSION}.src.tar.xz"
LLVM_URL="${LLVM_URL:-https://github.com/llvm/llvm-project/releases/download/llvmorg-${LLVM_VERSION}/${LLVM_TARBALL}}"
WASI_SDK_TARBALL="wasi-sdk-${WASI_SDK_VERSION_FULL}-${WASI_SDK_ARCH}-${WASI_SDK_OS}.tar.gz"
WASI_SDK_URL="https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_SDK_VERSION}/${WASI_SDK_TARBALL}"
BINARYEN_TARBALL="binaryen-version_${BINARYEN_VERSION}-${BINARYEN_ARCH}-${BINARYEN_OS}.tar.gz"
BINARYEN_URL="https://github.com/WebAssembly/binaryen/releases/download/version_${BINARYEN_VERSION}/${BINARYEN_TARBALL}"

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

verify_optional_sha256() {
    local expected="$1"
    local file="$2"
    if [ -z "${expected}" ]; then
        return
    fi
    verify_sha256 "${expected}" "${file}"
}

copy_wasm_tool() {
    local source="$1"
    local output="$2"
    if [ ! -f "${source}" ]; then
        echo "expected LLVM tool output missing: ${source}" >&2
        exit 1
    fi
    if ! head -c 4 "${source}" | cmp -s - <(printf '\0asm'); then
        echo "LLVM tool output is not a WebAssembly module: ${source}" >&2
        exit 1
    fi
    cp "${source}" "${output}"
}

mkdir -p "${WORKDIR}" "${OUTDIR}" "${WORKDIR}/downloads"

download "${LLVM_URL}" "${WORKDIR}/downloads/${LLVM_TARBALL}"
download "${WASI_SDK_URL}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
download "${BINARYEN_URL}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
verify_optional_sha256 "${LLVM_PROJECT_SHA256}" "${WORKDIR}/downloads/${LLVM_TARBALL}"
verify_sha256 "${WASI_SDK_SHA256}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
verify_sha256 "${BINARYEN_SHA256}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"

rm -rf \
    "${WORKDIR}/llvm-project-${LLVM_VERSION}.src" \
    "${WORKDIR}"/wasi-sdk-${WASI_SDK_VERSION_FULL}* \
    "${WORKDIR}"/binaryen-version_${BINARYEN_VERSION} \
    "${WORKDIR}/llvm-native-build" \
    "${WORKDIR}/llvm-wasix-build" \
    "${WORKDIR}/llvm-wasix-install"
tar -C "${WORKDIR}" -xJf "${WORKDIR}/downloads/${LLVM_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${BINARYEN_TARBALL}"

LLVM_SRC="${WORKDIR}/llvm-project-${LLVM_VERSION}.src/llvm"
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
if [ ! -f "${WASIX_SYSROOT}/include/unistd.h" ] || [ ! -f "${WASIX_SYSROOT}/lib/wasm32-wasi/libc.a" ]; then
    echo "prepared WASIX sysroot missing: ${WASIX_SYSROOT}" >&2
    echo "Run prepare-wasix-sysroot.sh first, then pass that path as WASIX_SYSROOT." >&2
    exit 1
fi

export SOURCE_DATE_EPOCH

cmake -S "${LLVM_SRC}" -B "${WORKDIR}/llvm-native-build" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DLLVM_ENABLE_PROJECTS="clang;lld" \
    -DLLVM_TARGETS_TO_BUILD=WebAssembly \
    -DLLVM_ENABLE_TERMINFO=OFF \
    -DLLVM_ENABLE_ZLIB=OFF \
    -DLLVM_ENABLE_ZSTD=OFF \
    -DLLVM_ENABLE_LIBXML2=OFF \
    -DLLVM_INCLUDE_BENCHMARKS=OFF \
    -DLLVM_INCLUDE_EXAMPLES=OFF \
    -DLLVM_INCLUDE_TESTS=OFF \
    -DCLANG_INCLUDE_TESTS=OFF \
    -DLLD_INCLUDE_TESTS=OFF
ninja -C "${WORKDIR}/llvm-native-build" -j"${JOBS}" llvm-tblgen clang-tblgen

"${WASI_SDK}/bin/clang" \
    --target=wasm32-wasi \
    --sysroot="${WASIX_SYSROOT}" \
    -Os \
    -c "${REPO_ROOT}/crates/sinomo-sandbox/vendor/wasix-libc/sinomo/libc/llvm-wasix-compat.c" \
    -o "${WORKDIR}/llvm-wasix-compat.o"

COMMON_C_FLAGS="-O2 -fno-ident -matomics -mbulk-memory -mmutable-globals -pthread -mthread-model posix -ftls-model=local-exec -DCLANG_BUILD_STATIC -D_GNU_SOURCE -DBYTE_ORDER=1234 -DLITTLE_ENDIAN=1234 -DBIG_ENDIAN=4321 -D_WASI_EMULATED_PROCESS_CLOCKS -D_WASI_EMULATED_MMAN -Df_flags=f_flag -DMNT_LOCAL=0 -DF_GETLK=5 -DF_SETLK=6 -DF_SETLKW=7 -DF_RDLCK=0 -DF_WRLCK=1 -DF_UNLCK=2 -DSS_ONSTACK=1 -ffile-prefix-map=${WORKDIR}=sinomo-clang-build -ffile-prefix-map=${REPO_ROOT}=sinomo"
COMMON_CXX_FLAGS="${COMMON_C_FLAGS} -fno-exceptions -fno-rtti"
COMMON_LD_FLAGS="-Wl,--shared-memory -Wl,--import-memory -Wl,--extra-features=atomics -Wl,--extra-features=bulk-memory -Wl,--extra-features=mutable-globals -Wl,--export=_start -Wl,--export=__wasm_init_tls -Wl,--export=__wasm_signal -Wl,--export-if-defined=__indirect_function_table -Wl,--export-if-defined=__stack_pointer -Wl,--export-if-defined=__heap_base -Wl,--export-if-defined=__data_end -Wl,-z,stack-size=8388608 -Wl,--initial-memory=268435456 -Wl,--max-memory=2147483648 ${WORKDIR}/llvm-wasix-compat.o -lwasi-emulated-mman -lwasi-emulated-process-clocks -lwasi-emulated-getpid"

cmake -S "${LLVM_SRC}" -B "${WORKDIR}/llvm-wasix-build" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${WORKDIR}/llvm-wasix-install" \
    -DCMAKE_SYSTEM_NAME=WASI \
    -DCMAKE_SYSTEM_PROCESSOR=wasm32 \
    -DUNIX=1 \
    -DCMAKE_C_COMPILER="${WASI_SDK}/bin/clang" \
    -DCMAKE_CXX_COMPILER="${WASI_SDK}/bin/clang++" \
    -DCMAKE_AR="${WASI_SDK}/bin/llvm-ar" \
    -DCMAKE_RANLIB="${WASI_SDK}/bin/llvm-ranlib" \
    -DCMAKE_C_COMPILER_TARGET=wasm32-wasi \
    -DCMAKE_CXX_COMPILER_TARGET=wasm32-wasi \
    -DCMAKE_SYSROOT="${WASIX_SYSROOT}" \
    -DCMAKE_C_FLAGS="${COMMON_C_FLAGS}" \
    -DCMAKE_CXX_FLAGS="${COMMON_CXX_FLAGS}" \
    -DCMAKE_EXE_LINKER_FLAGS="${COMMON_LD_FLAGS}" \
    -DLLVM_TABLEGEN="${WORKDIR}/llvm-native-build/bin/llvm-tblgen" \
    -DCLANG_TABLEGEN="${WORKDIR}/llvm-native-build/bin/clang-tblgen" \
    -DLLVM_ENABLE_PROJECTS="clang;lld" \
    -DLLVM_TARGETS_TO_BUILD=WebAssembly \
    -DLLVM_DEFAULT_TARGET_TRIPLE=wasm32-wasi \
    -DLLVM_HOST_TRIPLE=wasm32-wasi \
    -DLLVM_ENABLE_THREADS=OFF \
    -DLLVM_ENABLE_TERMINFO=OFF \
    -DLLVM_ENABLE_ZLIB=OFF \
    -DLLVM_ENABLE_ZSTD=OFF \
    -DLLVM_ENABLE_LIBXML2=OFF \
    -DLLVM_ENABLE_LIBEDIT=OFF \
    -DLLVM_ENABLE_PIC=OFF \
    -DLLVM_INCLUDE_BENCHMARKS=OFF \
    -DLLVM_INCLUDE_EXAMPLES=OFF \
    -DLLVM_INCLUDE_TESTS=OFF \
    -DCLANG_INCLUDE_TESTS=OFF \
    -DLLD_INCLUDE_TESTS=OFF
ninja -C "${WORKDIR}/llvm-wasix-build" -j"${JOBS}" clang lld llvm-ar llvm-ranlib llvm-nm

rm -rf "${OUTDIR}/artifacts/lib"
mkdir -p "${OUTDIR}/artifacts/lib"

copy_wasm_tool "${WORKDIR}/llvm-wasix-build/bin/clang" "${OUTDIR}/clang-${LLVM_VERSION}-wasix.wasm"
copy_wasm_tool "${WORKDIR}/llvm-wasix-build/bin/wasm-ld" "${OUTDIR}/wasm-ld-${LLVM_VERSION}-wasix.wasm"
copy_wasm_tool "${WORKDIR}/llvm-wasix-build/bin/llvm-ar" "${OUTDIR}/llvm-ar-${LLVM_VERSION}-wasix.wasm"
copy_wasm_tool "${WORKDIR}/llvm-wasix-build/bin/llvm-ranlib" "${OUTDIR}/llvm-ranlib-${LLVM_VERSION}-wasix.wasm"
copy_wasm_tool "${WORKDIR}/llvm-wasix-build/bin/llvm-nm" "${OUTDIR}/llvm-nm-${LLVM_VERSION}-wasix.wasm"
if [ "${CLANG_OPTIMIZE_WASM}" = "1" ]; then
    "${BINARYEN}/bin/wasm-opt" -Oz --enable-bulk-memory --enable-mutable-globals --enable-threads \
        "${OUTDIR}/clang-${LLVM_VERSION}-wasix.wasm" -o "${OUTDIR}/clang-${LLVM_VERSION}-wasix.wasm"
    "${BINARYEN}/bin/wasm-opt" -Oz --enable-bulk-memory --enable-mutable-globals --enable-threads \
        "${OUTDIR}/wasm-ld-${LLVM_VERSION}-wasix.wasm" -o "${OUTDIR}/wasm-ld-${LLVM_VERSION}-wasix.wasm"
fi

cp -a "${WORKDIR}/llvm-wasix-build/lib/clang" "${OUTDIR}/artifacts/lib/"
find "${OUTDIR}/artifacts/lib/clang" -type f \
    \( -name '*.a' -o -name '*.so' -o -name '*.dylib' \) -delete

echo "built Clang/LLVM WASIX modules: ${OUTDIR}"
echo "package them by preparing a sysroot separately, then running WASIX_SYSROOT=<prepared-sysroot> OUTDIR=${OUTDIR} ${ROOT}/package-clang-wasix.sh"
