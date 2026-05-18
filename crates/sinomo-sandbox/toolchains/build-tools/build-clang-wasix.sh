#!/usr/bin/env bash
set -euo pipefail

LLVM_VERSION="${LLVM_VERSION:-21.1.6}"
LLVM_TOOLCHAIN_ID="${LLVM_TOOLCHAIN_ID:-01k4z8q5f00000000000000000}"
LLVM_PROJECT_SHA256="${LLVM_PROJECT_SHA256:-ae67086eb04bed7ca11ab880349b5f1ab6f50e1b88cda376eaf8a845b935762b}"
WASI_SDK_VERSION="${WASI_SDK_VERSION:-24}"
WASI_SDK_VERSION_FULL="${WASI_SDK_VERSION_FULL:-24.0}"
WASI_SDK_ARCH="${WASI_SDK_ARCH:-x86_64}"
WASI_SDK_OS="${WASI_SDK_OS:-linux}"
WASI_SDK_SHA256="${WASI_SDK_SHA256:-c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5}"
BINARYEN_VERSION="${BINARYEN_VERSION:-124}"
BINARYEN_ARCH="${BINARYEN_ARCH:-x86_64}"
BINARYEN_OS="${BINARYEN_OS:-linux}"
BINARYEN_SHA256="${BINARYEN_SHA256:-0290c3779fedf592b8da0ded3032ff55c41a2b7bfa2d6bf7b7bac6f0e6e28963}"
CLANG_OPTIMIZE_WASM="${CLANG_OPTIMIZE_WASM:-0}"
WASIX_LIBC_VERSION="${WASIX_LIBC_VERSION:-v2026-05-12.1}"
WASIX_LIBC_TARBALL="${WASIX_LIBC_TARBALL:-sysroot.tar.gz}"
WASIX_LIBC_SHA256="${WASIX_LIBC_SHA256:-e9f7435c0fdc050c1f813f83d3b22388ba3b16d1303ced510915eb028a38abb4}"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1719878400}"
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
WORKDIR="${WORKDIR:-"${ROOT}/work-clang"}"
OUTDIR="${OUTDIR:-"${ROOT}/out-clang"}"

LLVM_TARBALL="llvm-project-${LLVM_VERSION}.src.tar.xz"
LLVM_URL="${LLVM_URL:-https://github.com/llvm/llvm-project/releases/download/llvmorg-${LLVM_VERSION}/${LLVM_TARBALL}}"
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
    if [ -z "${expected}" ]; then
        echo "sha256 not pinned for ${file}; set LLVM_PROJECT_SHA256 after recording the upstream source hash" >&2
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

sinomo_hash() {
    local file="$1"
    if [ -n "${SINOMO_BIN:-}" ]; then
        env -u CC -u CXX -u CFLAGS -u CXXFLAGS -u LDFLAGS -u AR -u RANLIB \
            "${SINOMO_BIN}" sandbox toolchain hash "${file}"
    else
        env -u CC -u CXX -u CFLAGS -u CXXFLAGS -u LDFLAGS -u AR -u RANLIB \
            cargo run --quiet --manifest-path "${REPO_ROOT}/Cargo.toml" --bin sinomo -- sandbox toolchain hash "${file}"
    fi
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
download "${WASIX_LIBC_URL}" "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"
verify_optional_sha256 "${LLVM_PROJECT_SHA256}" "${WORKDIR}/downloads/${LLVM_TARBALL}"
verify_sha256 "${WASI_SDK_SHA256}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
verify_sha256 "${BINARYEN_SHA256}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
verify_sha256 "${WASIX_LIBC_SHA256}" "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"

rm -rf \
    "${WORKDIR}/llvm-project-${LLVM_VERSION}.src" \
    "${WORKDIR}"/wasi-sdk-${WASI_SDK_VERSION_FULL}* \
    "${WORKDIR}"/binaryen-version_${BINARYEN_VERSION} \
    "${WORKDIR}/wasix-sysroot" \
    "${WORKDIR}/llvm-native-build" \
    "${WORKDIR}/llvm-wasix-build" \
    "${WORKDIR}/llvm-wasix-install" \
    "${WORKDIR}/package-stage"
tar -C "${WORKDIR}" -xJf "${WORKDIR}/downloads/${LLVM_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
mkdir -p "${WORKDIR}/wasix-sysroot"
tar -C "${WORKDIR}/wasix-sysroot" -xzf "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"

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
WASIX_SYSROOT="$(find "${WORKDIR}/wasix-sysroot" -type d -path "*/sysroot" | sort | head -n 1)"
if [ -z "${WASIX_SYSROOT}" ] || [ ! -f "${WASIX_SYSROOT}/include/unistd.h" ] || [ ! -f "${WASIX_SYSROOT}/lib/wasm32-wasi/libc.a" ]; then
    echo "wasix-libc sysroot not found after extracting ${WASIX_LIBC_TARBALL}" >&2
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

cat > "${WORKDIR}/llvm-wasix-compat.c" <<'COMPAT_EOF'
#include <stddef.h>

int dladdr(const void *addr, void *info) {
    (void)addr;
    (void)info;
    return 0;
}

int posix_madvise(void *addr, size_t len, int advice) {
    (void)addr;
    (void)len;
    (void)advice;
    return 0;
}

int mprotect(void *addr, size_t len, int prot) {
    (void)addr;
    (void)len;
    (void)prot;
    return 0;
}
COMPAT_EOF
"${WASI_SDK}/bin/clang" \
    --target=wasm32-wasi \
    --sysroot="${WASIX_SYSROOT}/sysroot" \
    -Os \
    -c "${WORKDIR}/llvm-wasix-compat.c" \
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

rm -rf "${WORKDIR}/package-stage" "${OUTDIR}/artifacts"
mkdir -p "${OUTDIR}/artifacts/sysroot"

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

mkdir -p "${OUTDIR}/artifacts/lib"
cp -a "${WORKDIR}/llvm-wasix-build/lib/clang" "${OUTDIR}/artifacts/lib/"
find "${OUTDIR}/artifacts" -type f \
	    \( -name '*.a' -o -name '*.so' -o -name '*.dylib' \) -delete
rsync -a --delete "${WASIX_SYSROOT}/" "${OUTDIR}/artifacts/sysroot/"
mkdir -p "${OUTDIR}/artifacts/lib/clang/${LLVM_VERSION%%.*}/lib/wasm32-unknown-wasi"
cp "${WASIX_SYSROOT}/lib/wasm32-wasi/libclang_rt.builtins-wasm32.a" \
    "${OUTDIR}/artifacts/lib/clang/${LLVM_VERSION%%.*}/lib/wasm32-unknown-wasi/libclang_rt.builtins.a"

cat > "${OUTDIR}/host-env.sh" <<EOF
# Source this file when building Sinomo WASIX command modules outside the sandbox
# with the same compiler input set used by the sandbox Clang package.
export SINOMO_CLANG_LLVM_VERSION="${LLVM_VERSION}"
export SINOMO_CLANG_WASI_SDK="${WASI_SDK}"
export SINOMO_CLANG_WASIX_SYSROOT="${WASIX_SYSROOT}"
export CC="\${SINOMO_CLANG_WASI_SDK}/bin/clang --target=wasm32-wasi --sysroot=\${SINOMO_CLANG_WASIX_SYSROOT}"
export CXX="\${SINOMO_CLANG_WASI_SDK}/bin/clang++ --target=wasm32-wasi --sysroot=\${SINOMO_CLANG_WASIX_SYSROOT}"
export AR="\${SINOMO_CLANG_WASI_SDK}/bin/llvm-ar"
export RANLIB="\${SINOMO_CLANG_WASI_SDK}/bin/llvm-ranlib"
EOF

python3 - "$OUTDIR" "$OUTDIR/artifacts" "$LLVM_VERSION" "$LLVM_TOOLCHAIN_ID" "$REPO_ROOT" <<'PY'
import json
import os
import subprocess
import sys

outdir, artifact_root, version, toolchain_id, repo = sys.argv[1:6]
root = os.path.abspath(artifact_root)
resource_version = version.split(".", 1)[0]

def hash_file(path):
    if os.environ.get("SINOMO_BIN"):
        cmd = [os.environ["SINOMO_BIN"], "sandbox", "toolchain", "hash", path]
    else:
        cmd = ["cargo", "run", "--quiet", "--manifest-path", os.path.join(repo, "Cargo.toml"), "--bin", "sinomo", "--", "sandbox", "toolchain", "hash", path]
    return subprocess.check_output(cmd, text=True).strip()

def host_memory_mb():
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                fields = line.split()
                if len(fields) >= 2 and fields[0] == "MemTotal:":
                    return (int(fields[1]) + 1023) // 1024
    except OSError:
        pass
    return (2**64 - 1) // (1024 * 1024)

limits = {
    "memory_mb": int(os.environ.get("CLANG_SANDBOX_MAX_MEMORY_MB", str(host_memory_mb()))),
    "wall_time_ms": int(os.environ.get("CLANG_SANDBOX_MAX_WALL_TIME_MS", str(2**64 - 1))),
    "max_input_bytes": int(os.environ.get("CLANG_SANDBOX_MAX_INPUT_BYTES", "10485760")),
    "max_output_bytes": int(os.environ.get("CLANG_SANDBOX_MAX_OUTPUT_BYTES", "10485760")),
    "max_workspace_bytes": int(os.environ.get("CLANG_SANDBOX_MAX_WORKSPACE_BYTES", "1073741824")),
    "max_written_bytes_per_command": int(os.environ.get("CLANG_SANDBOX_MAX_WRITTEN_BYTES_PER_COMMAND", "1073741824")),
    "max_file_count": int(os.environ.get("CLANG_SANDBOX_MAX_FILE_COUNT", "200000")),
}
caps = ["child-process-exec", "wasix.unpinned-child-exec"]

clang_argv_prefix = [
    "--target=wasm32-wasi",
    "--sysroot=/sysroot",
    f"-resource-dir=/lib/clang/{resource_version}",
    "-fintegrated-cc1",
    "-B/bin",
    "-matomics",
    "-mbulk-memory",
    "-mmutable-globals",
    "-pthread",
    "-mthread-model",
    "posix",
]

def command(name, module, exec_name=None, default_env=None, argv_prefix=None):
    item = {
        "name": name,
        "module": module,
        "content_hash": hash_file(os.path.join(outdir, module)),
        "target_abi": "wasix",
        "required_capabilities": caps,
        "default_limits": limits,
    }
    if exec_name:
        item["exec_name"] = exec_name
    if default_env:
        item["default_env"] = default_env
    if argv_prefix:
        item["argv_prefix"] = argv_prefix
    return item

clang_env = {
    "PATH": "/bin",
    "CLANG_RESOURCE_DIR": "/lib/clang/" + resource_version,
}
commands = [
    command("clang", f"clang-{version}-wasix.wasm", exec_name="/bin/clang", default_env=clang_env, argv_prefix=clang_argv_prefix),
    command("clang++", f"clang-{version}-wasix.wasm", exec_name="/bin/clang++", default_env=clang_env, argv_prefix=clang_argv_prefix),
    command("cc", f"clang-{version}-wasix.wasm", exec_name="/bin/cc", default_env=clang_env, argv_prefix=clang_argv_prefix),
    command("c++", f"clang-{version}-wasix.wasm", exec_name="/bin/c++", default_env=clang_env, argv_prefix=clang_argv_prefix),
    command("wasm-ld", f"wasm-ld-{version}-wasix.wasm"),
    command("ld.lld", f"wasm-ld-{version}-wasix.wasm", exec_name="ld.lld"),
    command("llvm-ar", f"llvm-ar-{version}-wasix.wasm"),
    command("llvm-ranlib", f"llvm-ranlib-{version}-wasix.wasm"),
    command("llvm-nm", f"llvm-nm-{version}-wasix.wasm"),
]

artifacts = []
for dirpath, dirnames, filenames in os.walk(root):
    dirnames.sort()
    for filename in sorted(filenames):
        path = os.path.join(dirpath, filename)
        relative = os.path.relpath(path, root)
        artifacts.append({
            "relative_path": relative,
            "file": os.path.relpath(path, outdir),
            "content_hash": hash_file(path),
        })

manifest = {
    "toolchain_id": toolchain_id,
    "version": f"clang-llvm-{version}-wasix",
    "target_abi": "wasix",
    "provenance": f"Sinomo Clang/LLVM WASIX recipe: LLVM {version}, wasi-sdk 24, wasix-libc v2026-05-12.1",
    "commands": commands,
    "artifacts": artifacts,
}
with open(os.path.join(outdir, "package.json"), "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY

echo "built Clang/LLVM WASIX package: ${OUTDIR}/package.json"
echo "host-side matching compiler environment: ${OUTDIR}/host-env.sh"
