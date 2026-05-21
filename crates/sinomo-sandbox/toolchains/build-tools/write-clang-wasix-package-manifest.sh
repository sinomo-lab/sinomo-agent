#!/usr/bin/env bash
set -euo pipefail

LLVM_VERSION="${LLVM_VERSION:-21.1.6}"
LLVM_TOOLCHAIN_ID="${LLVM_TOOLCHAIN_ID:-01k4z8q5f00000000000000000}"
WASI_SDK_VERSION="${WASI_SDK_VERSION:-24}"
WASIX_LIBC_VERSION="${WASIX_LIBC_VERSION:-v2026-05-12.1}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
TARGET_DIR="${TARGET_DIR:-"${REPO_ROOT}/target"}"
OUTDIR="${OUTDIR:-"${TARGET_DIR}/sinomo-build-tools-clang-wasix-out"}"

for tool in clang wasm-ld llvm-ar llvm-ranlib llvm-nm; do
    if [ ! -f "${OUTDIR}/${tool}-${LLVM_VERSION}-wasix.wasm" ]; then
        echo "missing WASIX compiler module: ${OUTDIR}/${tool}-${LLVM_VERSION}-wasix.wasm" >&2
        exit 1
    fi
done
if [ ! -d "${OUTDIR}/artifacts" ]; then
    echo "missing package artifacts directory: ${OUTDIR}/artifacts" >&2
    exit 1
fi
if [ ! -f "${OUTDIR}/artifacts/sysroot/include/unistd.h" ] || [ ! -f "${OUTDIR}/artifacts/sysroot/lib/wasm32-wasi/libc.a" ]; then
    echo "missing prepared package sysroot in ${OUTDIR}/artifacts/sysroot" >&2
    echo "Run prepare-wasix-sysroot.sh with SYSROOT_OUTDIR=${OUTDIR}/artifacts/sysroot before writing the manifest." >&2
    exit 1
fi
if [ ! -d "${OUTDIR}/artifacts/lib/clang/${LLVM_VERSION%%.*}" ]; then
    echo "missing Clang resource directory: ${OUTDIR}/artifacts/lib/clang/${LLVM_VERSION%%.*}" >&2
    exit 1
fi
CLANG_BUILTINS_SOURCE="${OUTDIR}/artifacts/sysroot/lib/wasm32-wasi/libclang_rt.builtins-wasm32.a"
CLANG_BUILTINS_DEST_DIR="${OUTDIR}/artifacts/lib/clang/${LLVM_VERSION%%.*}/lib/wasm32-unknown-wasi"
if [ ! -f "${CLANG_BUILTINS_SOURCE}" ]; then
    echo "missing WASIX compiler-rt builtins in prepared package sysroot: ${CLANG_BUILTINS_SOURCE}" >&2
    exit 1
fi
mkdir -p "${CLANG_BUILTINS_DEST_DIR}"
cp "${CLANG_BUILTINS_SOURCE}" "${CLANG_BUILTINS_DEST_DIR}/libclang_rt.builtins.a"
if [ -z "${SINOMO_BIN:-}" ]; then
    CARGO_TARGET_DIR_RESOLVED="${CARGO_TARGET_DIR:-"${REPO_ROOT}/target"}"
    case "${CARGO_TARGET_DIR_RESOLVED}" in
        /*) ;;
        *) CARGO_TARGET_DIR_RESOLVED="${REPO_ROOT}/${CARGO_TARGET_DIR_RESOLVED}" ;;
    esac
    SINOMO_BIN="${CARGO_TARGET_DIR_RESOLVED}/debug/sinomo"
    if [ ! -x "${SINOMO_BIN}" ]; then
        cargo build --quiet --manifest-path "${REPO_ROOT}/Cargo.toml" --bin sinomo
    fi
    export SINOMO_BIN
fi

python3 - "$OUTDIR" "$OUTDIR/artifacts" "$LLVM_VERSION" "$LLVM_TOOLCHAIN_ID" "$REPO_ROOT" "$WASI_SDK_VERSION" "$WASIX_LIBC_VERSION" <<'PY'
import json
import os
import subprocess
import sys

outdir, artifact_root, version, toolchain_id, repo, wasi_sdk_version, wasix_libc_version = sys.argv[1:8]
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
    "max_input_bytes": int(os.environ.get("CLANG_SANDBOX_MAX_INPUT_BYTES", str(2**64 - 1))),
    "max_output_bytes": int(os.environ.get("CLANG_SANDBOX_MAX_OUTPUT_BYTES", str(2**64 - 1))),
    "max_workspace_bytes": int(os.environ.get("CLANG_SANDBOX_MAX_WORKSPACE_BYTES", str(2**64 - 1))),
    "max_written_bytes_per_command": int(os.environ.get("CLANG_SANDBOX_MAX_WRITTEN_BYTES_PER_COMMAND", str(2**64 - 1))),
    "max_file_count": int(os.environ.get("CLANG_SANDBOX_MAX_FILE_COUNT", str(2**64 - 1))),
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
    "-D_WASI_EMULATED_PROCESS_CLOCKS",
    "-D_WASI_EMULATED_MMAN",
    "-Wl,--shared-memory",
    "-Wl,--import-memory",
    "-Wl,--extra-features=atomics",
    "-Wl,--extra-features=bulk-memory",
    "-Wl,--extra-features=mutable-globals",
    "-Wl,--export=__stack_pointer",
    "-Wl,-z,stack-size=1048576",
    "-Wl,--initial-memory=16777216",
    "-Wl,--max-memory=2147483648",
    "-lwasi-emulated-process-clocks",
    "-lwasi-emulated-mman",
    "-lwasi-emulated-getpid",
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
    "provenance": f"Sinomo Clang/LLVM WASIX recipe: LLVM {version}, wasi-sdk {wasi_sdk_version}, wasix-libc {wasix_libc_version}",
    "commands": commands,
    "artifacts": artifacts,
}
with open(os.path.join(outdir, "package.json"), "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY

echo "wrote Clang/LLVM WASIX package manifest: ${OUTDIR}/package.json"
