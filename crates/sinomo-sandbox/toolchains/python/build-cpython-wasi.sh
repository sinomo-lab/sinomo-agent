#!/usr/bin/env bash
set -euo pipefail

CPYTHON_VERSION="${CPYTHON_VERSION:-3.13.13}"
CPYTHON_SHA256="${CPYTHON_SHA256:-2ab91ff401783ccca64f75d10c882e957bdfd60e2bf5a72f8421793729b78a71}"
PYTHON_TOOLCHAIN_ID="${PYTHON_TOOLCHAIN_ID:-01k4z8q5c00000000000000000}"
ZLIB_VERSION="${ZLIB_VERSION:-1.3.1}"
ZLIB_SHA256="${ZLIB_SHA256:-9a93b2b7dfdac77ceba5a558a580e74667dd6fede4585b91eefb60f03b72df23}"
SETUPTOOLS_VERSION="${SETUPTOOLS_VERSION:-80.9.0}"
SETUPTOOLS_SHA256="${SETUPTOOLS_SHA256:-062d34222ad13e0cc312a4c02d73f059e86a4acbfbdea8f8f76b28c99f306922}"
WASI_SDK_VERSION="${WASI_SDK_VERSION:-24}"
WASI_SDK_VERSION_FULL="${WASI_SDK_VERSION_FULL:-24.0}"
WASI_SDK_ARCH="${WASI_SDK_ARCH:-x86_64}"
WASI_SDK_OS="${WASI_SDK_OS:-linux}"
WASI_SDK_SHA256="${WASI_SDK_SHA256:-c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5}"
WASMTIME_VERSION="${WASMTIME_VERSION:-44.0.1}"
WASMTIME_SHA256="${WASMTIME_SHA256:-afd58715f105e3a7f454169daed22168c5736ec5f225fb04c4ac62c54c9508a3}"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1778597230}"
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
WORKDIR="${WORKDIR:-"${ROOT}/work"}"
OUTDIR="${OUTDIR:-"${ROOT}/out"}"
mkdir -p "${WORKDIR}" "${OUTDIR}"
WORKDIR="$(cd "${WORKDIR}" && pwd)"
OUTDIR="$(cd "${OUTDIR}" && pwd)"
PACKAGE_ROOT="${OUTDIR}/package-files"
SINOMO_PYTHON_WASI_DIR="${ROOT}/wasi"

CPYTHON_TARBALL="Python-${CPYTHON_VERSION}.tar.xz"
CPYTHON_URL="https://www.python.org/ftp/python/${CPYTHON_VERSION}/${CPYTHON_TARBALL}"
ZLIB_TARBALL="zlib-${ZLIB_VERSION}.tar.gz"
ZLIB_URL="https://zlib.net/fossils/${ZLIB_TARBALL}"
SETUPTOOLS_WHEEL="setuptools-${SETUPTOOLS_VERSION}-py3-none-any.whl"
SETUPTOOLS_URL="https://files.pythonhosted.org/packages/a3/dc/17031897dae0efacfea57dfd3a82fdd2a2aeb58e0ff71b77b87e44edc772/${SETUPTOOLS_WHEEL}"
WASI_SDK_TARBALL="wasi-sdk-${WASI_SDK_VERSION_FULL}-${WASI_SDK_ARCH}-${WASI_SDK_OS}.tar.gz"
WASI_SDK_URL="https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_SDK_VERSION}/${WASI_SDK_TARBALL}"
WASMTIME_TARBALL="wasmtime-v${WASMTIME_VERSION}-x86_64-linux.tar.xz"
WASMTIME_URL="https://github.com/bytecodealliance/wasmtime/releases/download/v${WASMTIME_VERSION}/${WASMTIME_TARBALL}"

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

mkdir -p "${WORKDIR}/downloads" "${OUTDIR}"

download "${CPYTHON_URL}" "${WORKDIR}/downloads/${CPYTHON_TARBALL}"
download "${ZLIB_URL}" "${WORKDIR}/downloads/${ZLIB_TARBALL}"
download "${SETUPTOOLS_URL}" "${WORKDIR}/downloads/${SETUPTOOLS_WHEEL}"
download "${WASI_SDK_URL}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
download "${WASMTIME_URL}" "${WORKDIR}/downloads/${WASMTIME_TARBALL}"
verify_sha256 "${CPYTHON_SHA256}" "${WORKDIR}/downloads/${CPYTHON_TARBALL}"
verify_sha256 "${ZLIB_SHA256}" "${WORKDIR}/downloads/${ZLIB_TARBALL}"
verify_sha256 "${SETUPTOOLS_SHA256}" "${WORKDIR}/downloads/${SETUPTOOLS_WHEEL}"
verify_sha256 "${WASI_SDK_SHA256}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
verify_sha256 "${WASMTIME_SHA256}" "${WORKDIR}/downloads/${WASMTIME_TARBALL}"

rm -rf \
    "${WORKDIR}/Python-${CPYTHON_VERSION}" \
    "${WORKDIR}/zlib-${ZLIB_VERSION}" \
    "${WORKDIR}/zlib-wasi" \
    "${WORKDIR}"/wasi-sdk-${WASI_SDK_VERSION_FULL}* \
    "${WORKDIR}/wasmtime-v${WASMTIME_VERSION}-x86_64-linux" \
    "${PACKAGE_ROOT}"
tar -C "${WORKDIR}" -xJf "${WORKDIR}/downloads/${CPYTHON_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${ZLIB_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
tar -C "${WORKDIR}" -xJf "${WORKDIR}/downloads/${WASMTIME_TARBALL}"

CPYTHON_SRC="${WORKDIR}/Python-${CPYTHON_VERSION}"
ZLIB_SRC="${WORKDIR}/zlib-${ZLIB_VERSION}"
ZLIB_OUT="${WORKDIR}/zlib-wasi"
WASI_SDK="$(find "${WORKDIR}" -maxdepth 1 -type d -name "wasi-sdk-${WASI_SDK_VERSION_FULL}*" | sort | head -n 1)"
WASMTIME="${WORKDIR}/wasmtime-v${WASMTIME_VERSION}-x86_64-linux/wasmtime"
if [ -z "${WASI_SDK}" ] || [ ! -x "${WASI_SDK}/bin/clang" ]; then
    echo "wasi-sdk clang not found after extracting ${WASI_SDK_TARBALL}" >&2
    exit 1
fi
if [ ! -x "${WASMTIME}" ]; then
    echo "wasmtime not found after extracting ${WASMTIME_TARBALL}" >&2
    exit 1
fi

mkdir -p "${ZLIB_OUT}/lib" "${ZLIB_OUT}/include"
(
    cd "${ZLIB_SRC}"
    for source in \
        adler32.c compress.c crc32.c deflate.c gzclose.c gzlib.c gzread.c \
        gzwrite.c infback.c inffast.c inflate.c inftrees.c trees.c uncompr.c zutil.c
    do
        "${WASI_SDK}/bin/clang" \
            --target=wasm32-wasi \
            --sysroot="${WASI_SDK}/share/wasi-sysroot" \
            -O2 \
            -DHAVE_UNISTD_H \
            -c "${source}" \
            -o "${source%.c}.o"
    done
    "${WASI_SDK}/bin/llvm-ar" rcs "${ZLIB_OUT}/lib/libz.a" *.o
    cp zlib.h zconf.h "${ZLIB_OUT}/include/"
)

export SOURCE_DATE_EPOCH
PATH="${WORKDIR}/wasmtime-v${WASMTIME_VERSION}-x86_64-linux:${PATH}" \
    python3 "${CPYTHON_SRC}/Tools/wasm/wasi.py" configure-build-python --quiet
PATH="${WORKDIR}/wasmtime-v${WASMTIME_VERSION}-x86_64-linux:${PATH}" \
    python3 "${CPYTHON_SRC}/Tools/wasm/wasi.py" make-build-python --quiet
PATH="${WORKDIR}/wasmtime-v${WASMTIME_VERSION}-x86_64-linux:${PATH}" \
    python3 "${CPYTHON_SRC}/Tools/wasm/wasi.py" configure-host --quiet --wasi-sdk "${WASI_SDK}"
cp "${SINOMO_PYTHON_WASI_DIR}/sinomo-python-wasi-mman-compat.c" "${CPYTHON_SRC}/Modules/sinomo-python-wasi-mman-compat.c"
cp "${SINOMO_PYTHON_WASI_DIR}/_sinomo_subprocessmodule.c" "${CPYTHON_SRC}/Modules/_sinomo_subprocessmodule.c"
sed "s|@ZLIB_OUT@|${ZLIB_OUT}|g" "${SINOMO_PYTHON_WASI_DIR}/Setup.local.in" > "${CPYTHON_SRC}/cross-build/wasm32-wasip1/Modules/Setup.local"
make -C "${CPYTHON_SRC}/cross-build/wasm32-wasip1" --jobs "${JOBS}" all

mkdir -p "${PACKAGE_ROOT}"
cp "${CPYTHON_SRC}/cross-build/wasm32-wasip1/python.wasm" \
    "${OUTDIR}/python-${CPYTHON_VERSION}-wasip1.wasm"
cp -a "${CPYTHON_SRC}/Lib" "${PACKAGE_ROOT}/Lib"
mkdir -p "${PACKAGE_ROOT}/Lib/site-packages"
mkdir -p "${PACKAGE_ROOT}/Lib/sinomo-wheels"
cp "${WORKDIR}/downloads/${SETUPTOOLS_WHEEL}" "${PACKAGE_ROOT}/Lib/sinomo-wheels/"
python3 - "${PACKAGE_ROOT}/Lib/ensurepip/_bundled/pip-"*.whl "${PACKAGE_ROOT}/Lib/site-packages" <<'PY'
import pathlib
import sys
import zipfile

target = pathlib.Path(sys.argv[-1])
target.mkdir(parents=True, exist_ok=True)
for wheel_arg in sys.argv[1:-1]:
    wheel = pathlib.Path(wheel_arg)
    with zipfile.ZipFile(wheel) as archive:
        archive.extractall(target)
PY
python3 - "${WORKDIR}/downloads/${SETUPTOOLS_WHEEL}" "${PACKAGE_ROOT}/Lib/site-packages" <<'PY'
import pathlib
import sys
import zipfile

wheel = pathlib.Path(sys.argv[1])
target = pathlib.Path(sys.argv[2])
target.mkdir(parents=True, exist_ok=True)
with zipfile.ZipFile(wheel) as archive:
    archive.extractall(target)
PY
cp "${SINOMO_PYTHON_WASI_DIR}/sitecustomize.py" "${PACKAGE_ROOT}/Lib/sitecustomize.py"
mkdir -p "${PACKAGE_ROOT}/cross-build/wasm32-wasip1/build"
cp -a \
    "${CPYTHON_SRC}/cross-build/wasm32-wasip1/build/lib.wasi-wasm32-3.13" \
    "${PACKAGE_ROOT}/cross-build/wasm32-wasip1/build/"
mkdir -p "${PACKAGE_ROOT}/include/python3.13"
cp -a "${CPYTHON_SRC}/Include/." "${PACKAGE_ROOT}/include/python3.13/"
cp "${CPYTHON_SRC}/cross-build/wasm32-wasip1/pyconfig.h" \
    "${PACKAGE_ROOT}/include/python3.13/pyconfig.h"
python3 - "${PACKAGE_ROOT}/cross-build/wasm32-wasip1/build/lib.wasi-wasm32-3.13/_sysconfigdata__wasi_wasm32-wasi.py" "${WORKDIR}" <<'PY'
import ast
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
workdir = sys.argv[2]
namespace = {}
exec(path.read_text(encoding="utf-8"), namespace)
vars = dict(namespace["build_time_vars"])

compiler = "/bin/clang --sysroot=/wasi-sysroot"
compiler_cxx = "/bin/clang++ --sysroot=/wasi-sysroot"
vars.update(
    {
        "AR": "/bin/llvm-ar",
        "BLDSHARED": f"{compiler} -shared",
        "CC": compiler,
        "CPP": "/bin/clang-cpp --sysroot=/wasi-sysroot",
        "CXX": compiler_cxx,
        "LDCXXSHARED": f"{compiler_cxx} -shared",
        "LDSHARED": f"{compiler} -shared",
        "LINKCC": compiler,
        "CONFINCLUDEPY": "/include/python3.13",
        "INCLUDEPY": "/include/python3.13",
        "LIBPL": "/lib/python3.13/config-3.13-wasm32-wasi",
    }
)
for key, value in list(vars.items()):
    if isinstance(value, str):
        vars[key] = value.replace(workdir, "/__sinomo_build_host__")

lines = [
    "# system configuration generated and sanitized by Sinomo CPython WASI recipe\n",
    "build_time_vars = ",
    repr(vars),
    "\n",
]
path.write_text("".join(lines), encoding="utf-8")
PY

if [ -z "${SINOMO_BIN:-}" ]; then
    cargo build --quiet --manifest-path "${REPO_ROOT}/Cargo.toml" --bin sinomo
    SINOMO_BIN="${REPO_ROOT}/target/debug/sinomo"
fi

python3 - "${OUTDIR}" "${PACKAGE_ROOT}" "${SINOMO_BIN}" "${PYTHON_TOOLCHAIN_ID}" "${CPYTHON_VERSION}" <<'PY'
import json
import pathlib
import subprocess
import sys

outdir = pathlib.Path(sys.argv[1])
package_root = pathlib.Path(sys.argv[2])
sinomo = pathlib.Path(sys.argv[3])
toolchain_id = sys.argv[4]
version = sys.argv[5]
python_wasm = outdir / f"python-{version}-wasip1.wasm"

def sinomo_hash(path: pathlib.Path) -> str:
    return subprocess.check_output(
        [str(sinomo), "sandbox", "toolchain", "hash", str(path)],
        text=True,
    ).strip()

artifacts = []
for path in sorted(package_root.rglob("*")):
    if not path.is_file():
        continue
    relative = path.relative_to(package_root).as_posix()
    artifacts.append({
        "relative_path": relative,
        "file": f"package-files/{relative}",
        "content_hash": sinomo_hash(path),
    })

limits = {
    "memory_mb": 256,
    "wall_time_ms": 10000,
    "max_input_bytes": 1048576,
    "max_output_bytes": 1048576,
    "max_workspace_bytes": 1073741824,
    "max_written_bytes_per_command": 67108864,
    "max_file_count": 100000,
}
default_env = {
    "HOME": "/home",
    "PIP_DISABLE_PIP_VERSION_CHECK": "1",
    "PIP_FIND_LINKS": "/Lib/sinomo-wheels",
    "PYTHONHOME": "/",
    "PYTHONPATH": "/Lib:/cross-build/wasm32-wasip1/build/lib.wasi-wasm32-3.13:/Lib/site-packages",
}
package = {
    "toolchain_id": toolchain_id,
    "version": f"cpython-{version}-wasip1",
    "provenance": f"Sinomo CPython WASI recipe: CPython {version}, zlib, wasi-sdk",
    "target_abi": "preview1",
    "commands": [
        {
            "name": "python",
            "module": python_wasm.name,
            "content_hash": sinomo_hash(python_wasm),
            "required_capabilities": ["preview1.pinned-subprocess"],
            "default_env": default_env,
            "default_limits": limits,
        },
        {
            "name": "pip",
            "module": python_wasm.name,
            "content_hash": sinomo_hash(python_wasm),
            "argv_prefix": ["-m", "pip"],
            "required_capabilities": ["preview1.pinned-subprocess"],
            "default_env": default_env,
            "default_limits": limits,
        },
    ],
    "artifacts": artifacts,
}
(outdir / "package.json").write_text(json.dumps(package, indent=2) + "\n", encoding="utf-8")
PY

cat > "${OUTDIR}/python-${CPYTHON_VERSION}-build.manifest" <<MANIFEST
cpython_version=${CPYTHON_VERSION}
cpython_source=${CPYTHON_URL}
cpython_sha256=${CPYTHON_SHA256}
zlib_version=${ZLIB_VERSION}
zlib_source=${ZLIB_URL}
zlib_sha256=${ZLIB_SHA256}
setuptools_version=${SETUPTOOLS_VERSION}
setuptools_source=${SETUPTOOLS_URL}
setuptools_sha256=${SETUPTOOLS_SHA256}
wasi_sdk_version=${WASI_SDK_VERSION_FULL}
wasi_sdk_source=${WASI_SDK_URL}
wasi_sdk_sha256=${WASI_SDK_SHA256}
wasmtime_version=${WASMTIME_VERSION}
wasmtime_source=${WASMTIME_URL}
wasmtime_sha256=${WASMTIME_SHA256}
source_date_epoch=${SOURCE_DATE_EPOCH}
package=${OUTDIR}/package.json
MANIFEST

echo "python package: ${OUTDIR}/package.json"
