#!/usr/bin/env bash
set -euo pipefail

CPYTHON_VERSION="${CPYTHON_VERSION:-3.13.13}"
CPYTHON_SHA256="${CPYTHON_SHA256:-2ab91ff401783ccca64f75d10c882e957bdfd60e2bf5a72f8421793729b78a71}"
PYTHON_TOOLCHAIN_ID="${PYTHON_TOOLCHAIN_ID:-01k4z8q5c10000000000000000}"
ZLIB_VERSION="${ZLIB_VERSION:-1.3.1}"
ZLIB_SHA256="${ZLIB_SHA256:-9a93b2b7dfdac77ceba5a558a580e74667dd6fede4585b91eefb60f03b72df23}"
OPENSSL_VERSION="${OPENSSL_VERSION:-3.5.0}"
OPENSSL_SHA256="${OPENSSL_SHA256:-344d0a79f1a9b08029b0744e2cc401a43f9c90acd1044d09a530b4885a8e9fc0}"
SETUPTOOLS_VERSION="${SETUPTOOLS_VERSION:-80.9.0}"
SETUPTOOLS_SHA256="${SETUPTOOLS_SHA256:-062d34222ad13e0cc312a4c02d73f059e86a4acbfbdea8f8f76b28c99f306922}"
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
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1778597230}"
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
WORKDIR="${WORKDIR:-"${ROOT}/work-wasix"}"
OUTDIR="${OUTDIR:-"${ROOT}/out-wasix"}"
SINOMO_PYTHON_WASIX_DIR="${ROOT}/wasix"
SINOMO_PREPARE_WASIX_SYSROOT="${REPO_ROOT}/crates/sinomo-sandbox/toolchains/build-tools/prepare-wasix-sysroot.sh"
mkdir -p "${WORKDIR}" "${OUTDIR}"
WORKDIR="$(cd "${WORKDIR}" && pwd)"
OUTDIR="$(cd "${OUTDIR}" && pwd)"
PACKAGE_ROOT="${OUTDIR}/package-files"

CPYTHON_TARBALL="Python-${CPYTHON_VERSION}.tar.xz"
CPYTHON_URL="https://www.python.org/ftp/python/${CPYTHON_VERSION}/${CPYTHON_TARBALL}"
ZLIB_TARBALL="zlib-${ZLIB_VERSION}.tar.gz"
ZLIB_URL="https://zlib.net/fossils/${ZLIB_TARBALL}"
OPENSSL_TARBALL="openssl-${OPENSSL_VERSION}.tar.gz"
OPENSSL_URL="https://www.openssl.org/source/${OPENSSL_TARBALL}"
SETUPTOOLS_WHEEL="setuptools-${SETUPTOOLS_VERSION}-py3-none-any.whl"
SETUPTOOLS_URL="https://files.pythonhosted.org/packages/a3/dc/17031897dae0efacfea57dfd3a82fdd2a2aeb58e0ff71b77b87e44edc772/${SETUPTOOLS_WHEEL}"
BINARYEN_TARBALL="binaryen-version_${BINARYEN_VERSION}-${BINARYEN_ARCH}-${BINARYEN_OS}.tar.gz"
BINARYEN_URL="https://github.com/WebAssembly/binaryen/releases/download/version_${BINARYEN_VERSION}/${BINARYEN_TARBALL}"
WASI_SDK_URL="https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_SDK_VERSION}/wasi-sdk-${WASI_SDK_VERSION_FULL}-${WASI_SDK_ARCH}-${WASI_SDK_OS}.tar.gz"

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

mkdir -p "${WORKDIR}/downloads" "${OUTDIR}"

download "${CPYTHON_URL}" "${WORKDIR}/downloads/${CPYTHON_TARBALL}"
download "${ZLIB_URL}" "${WORKDIR}/downloads/${ZLIB_TARBALL}"
download "${OPENSSL_URL}" "${WORKDIR}/downloads/${OPENSSL_TARBALL}"
download "${SETUPTOOLS_URL}" "${WORKDIR}/downloads/${SETUPTOOLS_WHEEL}"
download "${BINARYEN_URL}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
verify_sha256 "${CPYTHON_SHA256}" "${WORKDIR}/downloads/${CPYTHON_TARBALL}"
verify_sha256 "${ZLIB_SHA256}" "${WORKDIR}/downloads/${ZLIB_TARBALL}"
verify_sha256 "${OPENSSL_SHA256}" "${WORKDIR}/downloads/${OPENSSL_TARBALL}"
verify_sha256 "${SETUPTOOLS_SHA256}" "${WORKDIR}/downloads/${SETUPTOOLS_WHEEL}"
verify_sha256 "${BINARYEN_SHA256}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"

rm -rf \
    "${WORKDIR}/Python-${CPYTHON_VERSION}" \
    "${WORKDIR}/zlib-${ZLIB_VERSION}" \
    "${WORKDIR}/zlib-wasix" \
    "${WORKDIR}/openssl-${OPENSSL_VERSION}" \
    "${WORKDIR}/openssl-wasix" \
    "${WORKDIR}"/binaryen-version_${BINARYEN_VERSION} \
    "${WORKDIR}/sinomo-wasix-sysroot" \
    "${WORKDIR}/sinomo-wasix-sysroot-work" \
    "${WORKDIR}/wasixcc-sysroot-prefix" \
    "${PACKAGE_ROOT}"
tar -C "${WORKDIR}" -xJf "${WORKDIR}/downloads/${CPYTHON_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${ZLIB_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${OPENSSL_TARBALL}"
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

CPYTHON_SRC="${WORKDIR}/Python-${CPYTHON_VERSION}"
ZLIB_SRC="${WORKDIR}/zlib-${ZLIB_VERSION}"
ZLIB_OUT="${WORKDIR}/zlib-wasix"
OPENSSL_SRC="${WORKDIR}/openssl-${OPENSSL_VERSION}"
OPENSSL_OUT="${WORKDIR}/openssl-wasix"
WASI_SDK="${SINOMO_WASI_SDK}"
BINARYEN="${WORKDIR}/binaryen-version_${BINARYEN_VERSION}"
if [ -z "${WASI_SDK}" ] || [ ! -x "${WASI_SDK}/bin/clang" ]; then
    echo "wasi-sdk clang not found after preparing the Sinomo WASIX sysroot" >&2
    exit 1
fi
if [ ! -x "${BINARYEN}/bin/wasm-opt" ]; then
    echo "Binaryen wasm-opt not found after extracting ${BINARYEN_TARBALL}" >&2
    exit 1
fi
if [ -z "${WASIX_SYSROOT}" ] || [ ! -f "${WASIX_SYSROOT}/include/unistd.h" ] || [ ! -f "${WASIX_SYSROOT}/lib/wasm32-wasi/libc.a" ]; then
    echo "Sinomo WASIX sysroot not found after preparation" >&2
    exit 1
fi
WASIXCC_BIN="${WASIXCC_BIN:-$(command -v wasixcc || true)}"
WASIXCXX_BIN="${WASIXCXX_BIN:-$(command -v wasix++ || command -v wasixcc++ || true)}"
WASIXAR_BIN="${WASIXAR_BIN:-$(command -v wasixar || true)}"
WASIXRANLIB_BIN="${WASIXRANLIB_BIN:-$(command -v wasixranlib || true)}"
if [ -z "${WASIXCC_BIN}" ] || [ -z "${WASIXCXX_BIN}" ] || [ -z "${WASIXAR_BIN}" ] || [ -z "${WASIXRANLIB_BIN}" ]; then
    echo "wasixcc toolchain not found; install it with: cargo install wasixcc --locked && wasixccenv download-all && wasixccenv install-executables <bin-dir>" >&2
    exit 1
fi

WASIXCC_ENV_BIN="${WASIXCCENV_BIN:-$(command -v wasixccenv || true)}"
if [ -z "${WASIXCC_ENV_BIN}" ]; then
    echo "wasixccenv not found; install it with: cargo install wasixcc --locked" >&2
    exit 1
fi
WASIXCC_DEFAULT_SYSROOT="$("${WASIXCC_ENV_BIN}" print-sysroot)"
if [ -z "${WASIXCC_DEFAULT_SYSROOT}" ] || [ ! -d "${WASIXCC_DEFAULT_SYSROOT}" ]; then
    echo "wasixcc default sysroot not found" >&2
    exit 1
fi
WASIXCC_DEFAULT_SYSROOT_PREFIX="$(dirname "${WASIXCC_DEFAULT_SYSROOT}")"
WASIXCC_SYSROOT_PREFIX="${WORKDIR}/wasixcc-sysroot-prefix"
cp -a "${WASIXCC_DEFAULT_SYSROOT_PREFIX}" "${WASIXCC_SYSROOT_PREFIX}"
while IFS= read -r sysroot; do
    rm -rf "${sysroot}"
    mkdir -p "$(dirname "${sysroot}")"
    rsync -a "${WASIX_SYSROOT}/" "${sysroot}/"
done < <(find "${WASIXCC_SYSROOT_PREFIX}" -mindepth 1 -maxdepth 1 -type d | sort)

WASIXCC_SETTINGS=(
    "-sSYSROOT_PREFIX=${WASIXCC_SYSROOT_PREFIX}"
    "-sMODULE_KIND=dynamic-main"
    "-sPIC=yes"
    "-sEMULATE_FUNCTION_POINTER_CASTS=1"
    "-sINITIAL_MEMORY=268435456"
    "-sRUN_WASM_OPT=no"
)
WASIX_CFLAGS="-Os -fPIC -D_WASI_EMULATED_MMAN -fno-ident -ffile-prefix-map=${WORKDIR}=sinomo-python-wasix-build -ffile-prefix-map=${REPO_ROOT}=sinomo"
WASIX_LDFLAGS="-L${ZLIB_OUT}/lib -L${OPENSSL_OUT}/lib -lwasi-emulated-signal -lwasi-emulated-mman -lcrypt -Wl,--initial-memory=268435456 -Wl,-z,stack-size=67108864"
mkdir -p "${ZLIB_OUT}/lib" "${ZLIB_OUT}/include"
(
    cd "${ZLIB_SRC}"
    for source in \
        adler32.c compress.c crc32.c deflate.c gzclose.c gzlib.c gzread.c \
        gzwrite.c infback.c inffast.c inflate.c inftrees.c trees.c uncompr.c zutil.c
    do
        "${WASIXCC_BIN}" \
            "-sSYSROOT_PREFIX=${WASIXCC_SYSROOT_PREFIX}" \
            -sPIC=yes \
            -sRUN_WASM_OPT=no \
            -O2 \
            -fPIC \
            -DHAVE_UNISTD_H \
            -c "${source}" \
            -o "${source%.c}.o"
    done
    "${WASIXAR_BIN}" rcs "${ZLIB_OUT}/lib/libz.a" *.o
    cp zlib.h zconf.h "${ZLIB_OUT}/include/"
)

mkdir -p "${OPENSSL_OUT}"
(
    cd "${OPENSSL_SRC}"
    CC="${WASIXCC_BIN} -sSYSROOT_PREFIX=${WASIXCC_SYSROOT_PREFIX} -sPIC=yes -sRUN_WASM_OPT=no" \
    AR="${WASIXAR_BIN}" \
    RANLIB="${WASIXRANLIB_BIN}" \
    ./Configure \
        linux-generic32 \
        no-shared \
        no-asm \
        no-tests \
        no-engine \
        no-dso \
        no-secure-memory \
        no-ui-console \
        no-apps \
        no-docs \
        no-dgram \
        no-dtls \
        --prefix="${OPENSSL_OUT}" \
        --openssldir=/ssl \
        -fPIC \
        -Os
    make -j"${JOBS}" build_libs
    make install_sw
)

export SOURCE_DATE_EPOCH
PATH="${WASI_SDK}/bin:${PATH}" python3 "${CPYTHON_SRC}/Tools/wasm/wasi.py" configure-build-python --quiet
PATH="${WASI_SDK}/bin:${PATH}" python3 "${CPYTHON_SRC}/Tools/wasm/wasi.py" make-build-python --quiet

CONFIG_SITE_FILE="${WORKDIR}/config.site-wasm32-wasix"
cp "${SINOMO_PYTHON_WASIX_DIR}/config.site" "${CONFIG_SITE_FILE}"

HOST_BUILD_PYTHON="${CPYTHON_SRC}/cross-build/build/python"
WASIX_BUILD_DIR="${CPYTHON_SRC}/cross-build/wasm32-wasix"
mkdir -p "${WASIX_BUILD_DIR}"
(
    cd "${WASIX_BUILD_DIR}"
    CONFIG_SITE="${CONFIG_SITE_FILE}" \
    CC="${WASIXCC_BIN} ${WASIXCC_SETTINGS[*]}" \
    CXX="${WASIXCXX_BIN} ${WASIXCC_SETTINGS[*]}" \
    AR="${WASIXAR_BIN}" \
    RANLIB="${WASIXRANLIB_BIN}" \
    CPPFLAGS="-I${ZLIB_OUT}/include" \
    CFLAGS="${WASIX_CFLAGS}" \
    LDFLAGS="${WASIX_LDFLAGS}" \
    OPENSSL_INCLUDES="-I${OPENSSL_OUT}/include" \
    OPENSSL_LIBS="-L${OPENSSL_OUT}/lib -lssl -lcrypto" \
    ZLIB_CFLAGS="-I${ZLIB_OUT}/include" \
    ZLIB_LIBS="-L${ZLIB_OUT}/lib -lz" \
    ac_sys_system=WASI \
    MACHDEP=wasix \
	    ../../configure \
	        --host=wasm32-wasi \
	        --build="$(../../config.guess)" \
	        --with-build-python="${HOST_BUILD_PYTHON}" \
	        --with-openssl="${OPENSSL_OUT}" \
	        --with-openssl-rpath=no \
	        --without-readline \
	        --disable-test-modules
    cat > Modules/Setup.local <<'SETUP_LOCAL'
_posixsubprocess _posixsubprocess.c
_multiprocessing _multiprocessing/multiprocessing.c _multiprocessing/semaphore.c
SETUP_LOCAL
	    make -j"${JOBS}" python.wasm
	    make platform
	)

PYTHON_WASM_UNOPTIMIZED="${OUTDIR}/python-${CPYTHON_VERSION}-wasix.unoptimized.wasm"
PYTHON_WASM="${OUTDIR}/python-${CPYTHON_VERSION}-wasix.wasm"
cp "${WASIX_BUILD_DIR}/python.wasm" "${PYTHON_WASM_UNOPTIMIZED}"
if ! "${BINARYEN}/bin/wasm-opt" \
    --set-globals \
    --pass-arg=set-globals@__memory_base=64,__table_base=1 \
    --pass-arg=max-func-params@64 \
    --precompute \
    --simplify-globals-optimizing \
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
    -o "${PYTHON_WASM}" \
    "${PYTHON_WASM_UNOPTIMIZED}"; then
    echo "wasm-opt asyncify pass failed; retrying without asyncify" >&2
    "${BINARYEN}/bin/wasm-opt" \
        --set-globals \
        --pass-arg=set-globals@__memory_base=64,__table_base=1 \
        --pass-arg=max-func-params@64 \
        --precompute \
        --simplify-globals-optimizing \
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
        -o "${PYTHON_WASM}" \
        "${PYTHON_WASM_UNOPTIMIZED}"
fi

mkdir -p "${PACKAGE_ROOT}/Lib/site-packages" "${PACKAGE_ROOT}/Lib/sinomo-wheels"
cp -a "${CPYTHON_SRC}/Lib/." "${PACKAGE_ROOT}/Lib/"
cp "${WORKDIR}/downloads/${SETUPTOOLS_WHEEL}" "${PACKAGE_ROOT}/Lib/sinomo-wheels/"
python3 - "${PACKAGE_ROOT}/Lib/ensurepip/_bundled/pip-"*.whl "${PACKAGE_ROOT}/Lib/site-packages" <<'PY'
import pathlib
import sys
import zipfile

target = pathlib.Path(sys.argv[-1])
target.mkdir(parents=True, exist_ok=True)
for wheel_arg in sys.argv[1:-1]:
    with zipfile.ZipFile(wheel_arg) as archive:
        archive.extractall(target)
PY
python3 - "${WORKDIR}/downloads/${SETUPTOOLS_WHEEL}" "${PACKAGE_ROOT}/Lib/site-packages" <<'PY'
import pathlib
import sys
import zipfile

target = pathlib.Path(sys.argv[2])
target.mkdir(parents=True, exist_ok=True)
with zipfile.ZipFile(sys.argv[1]) as archive:
    archive.extractall(target)
PY

mkdir -p \
    "${PACKAGE_ROOT}/include/python3.13" \
    "${PACKAGE_ROOT}/cross-build/wasm32-wasix/build"
cp -a "${CPYTHON_SRC}/Include/." "${PACKAGE_ROOT}/include/python3.13/"
cp "${WASIX_BUILD_DIR}/pyconfig.h" "${PACKAGE_ROOT}/include/python3.13/pyconfig.h"
mkdir -p "${PACKAGE_ROOT}/etc/ssl/certs"
if [ -f /etc/ssl/certs/ca-certificates.crt ]; then
    cp /etc/ssl/certs/ca-certificates.crt "${PACKAGE_ROOT}/etc/ssl/certs/ca-certificates.crt"
fi
SYSCONFIG_BUILD_DIR="$(find "${WASIX_BUILD_DIR}/build" -mindepth 1 -maxdepth 1 -type d -name 'lib.*-wasm32-3.13' | sort | head -n 1)"
if [ -n "${SYSCONFIG_BUILD_DIR}" ]; then
    cp -a "${SYSCONFIG_BUILD_DIR}" "${PACKAGE_ROOT}/cross-build/wasm32-wasix/build/"
fi

SYSCONFIG_DATA_DIR="${PACKAGE_ROOT}/cross-build/wasm32-wasix/build/$(basename "${SYSCONFIG_BUILD_DIR:-lib.wasix-wasm32-3.13}")"
SYSCONFIG_DATA_FILES=("${SYSCONFIG_DATA_DIR}"/_sysconfigdata_*.py)
if [ -e "${SYSCONFIG_DATA_FILES[0]}" ]; then
    python3 - "${WORKDIR}" "${SYSCONFIG_DATA_FILES[@]}" <<'PY'
import pathlib
import sys

workdir = sys.argv[1]
replacements = {
    "AR": "/bin/llvm-ar",
    "BLDSHARED": "/bin/clang --sysroot=/wasi-sysroot -shared",
    "CC": "/bin/clang --sysroot=/wasi-sysroot",
    "CPP": "/bin/clang-cpp --sysroot=/wasi-sysroot",
    "CXX": "/bin/clang++ --sysroot=/wasi-sysroot",
    "LDCXXSHARED": "/bin/clang++ --sysroot=/wasi-sysroot -shared",
    "LDSHARED": "/bin/clang --sysroot=/wasi-sysroot -shared",
    "LINKCC": "/bin/clang --sysroot=/wasi-sysroot",
    "RANLIB": "/bin/llvm-ranlib",
    "CONFINCLUDEPY": "/include/python3.13",
    "INCLUDEPY": "/include/python3.13",
    "LIBPL": "/lib/python3.13/config-3.13-wasm32-wasi",
}
for raw_path in sys.argv[2:]:
    path = pathlib.Path(raw_path)
    namespace = {}
    exec(path.read_text(), namespace)
    build_time_vars = namespace["build_time_vars"]
    build_time_vars.update(replacements)
    for key, value in list(build_time_vars.items()):
        if isinstance(value, str):
            value = value.replace(workdir, "/__sinomo_build_host__")
            value = value.replace(str(pathlib.Path(workdir).parents[1]), "/__sinomo_repo_host__")
            value = value.replace(str(pathlib.Path.home() / ".cargo" / "bin"), "/bin")
            build_time_vars[key] = value
    path.write_text(
        "# system configuration generated and sanitized by Sinomo\n"
        + "build_time_vars = "
        + repr(build_time_vars)
        + "\n"
    )
PY
fi

mkdir -p "${PACKAGE_ROOT}/share/python"
cp "${CPYTHON_SRC}/LICENSE" "${PACKAGE_ROOT}/share/python/LICENSE"
printf '%s\n' "${CPYTHON_VERSION}" > "${PACKAGE_ROOT}/share/python/VERSION"
PYTHON_HASH="$(sinomo_hash "${PYTHON_WASM}")"

python3 - \
    "${OUTDIR}/package.json" \
    "${PACKAGE_ROOT}" \
    "${PYTHON_TOOLCHAIN_ID}" \
    "cpython-${CPYTHON_VERSION}-wasix" \
    "Sinomo CPython WASIX recipe: CPython ${CPYTHON_VERSION}, wasi-sdk ${WASI_SDK_VERSION_FULL}" \
    "python-${CPYTHON_VERSION}-wasix.wasm" \
    "${PYTHON_HASH}" \
    "${SINOMO_BIN:-}" \
    "${REPO_ROOT}" <<'PY'
import json
import os
import pathlib
import subprocess
import sys

(
    package_json,
    package_root,
    toolchain_id,
    version,
    provenance,
    module,
    module_hash,
    sinomo_bin,
    repo_root,
) = sys.argv[1:]
package_json = pathlib.Path(package_json)
package_root = pathlib.Path(package_root)
repo_root = pathlib.Path(repo_root)

def hash_file(path: pathlib.Path) -> str:
    env = os.environ.copy()
    for name in ("CC", "CFLAGS", "LDFLAGS", "AR", "RANLIB"):
        env.pop(name, None)
    if sinomo_bin:
        command = [sinomo_bin, "sandbox", "toolchain", "hash", str(path)]
    else:
        local_sinomo = repo_root / "target" / "debug" / "sinomo"
        if local_sinomo.exists():
            command = [str(local_sinomo), "sandbox", "toolchain", "hash", str(path)]
        else:
            command = [
                "cargo",
                "run",
                "--quiet",
                "--manifest-path",
                str(repo_root / "Cargo.toml"),
                "--bin",
                "sinomo",
                "--",
                "sandbox",
                "toolchain",
                "hash",
                str(path),
            ]
    return subprocess.check_output(command, env=env, text=True).strip()

artifacts = []
for path in sorted(package_root.rglob("*")):
    if not path.is_file():
        continue
    relative_path = path.relative_to(package_root).as_posix()
    artifacts.append(
        {
            "relative_path": relative_path,
            "file": f"package-files/{relative_path}",
            "content_hash": hash_file(path),
        }
    )

python_limits = {
    "memory_mb": 4096,
    "wall_time_ms": 900000,
    "max_input_bytes": 10485760,
    "max_output_bytes": 10485760,
    "max_workspace_bytes": 1073741824,
    "max_written_bytes_per_command": 134217728,
    "max_file_count": 100000,
}
pip_limits = dict(python_limits)
pip_limits["max_written_bytes_per_command"] = 1073741824
default_env = {
    "HOME": "/home",
    "MPLBACKEND": "Agg",
    "PIP_DISABLE_PIP_VERSION_CHECK": "1",
    "PIP_FIND_LINKS": "/Lib/sinomo-wheels",
    "PYTHONDONTWRITEBYTECODE": "1",
    "PYTHONHOME": "/",
    "PYTHONPATH": "/work/.python-packages:/Lib:/Lib/site-packages",
    "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
    "TMPDIR": "/work/.tmp",
}
spec = {
    "toolchain_id": toolchain_id,
    "version": version,
    "provenance": provenance,
    "target_abi": "wasix",
    "commands": [
        {
            "name": "python",
            "module": module,
            "content_hash": module_hash,
            "default_env": default_env,
            "required_capabilities": ["child-process-exec", "wasix.unpinned-child-exec"],
            "default_limits": python_limits,
        },
        {
            "name": "pip",
            "module": module,
            "content_hash": module_hash,
            "argv_prefix": ["-m", "pip"],
            "default_env": default_env,
            "required_capabilities": ["child-process-exec", "wasix.unpinned-child-exec"],
            "default_limits": pip_limits,
        },
    ],
    "artifacts": artifacts,
}
package_json.write_text(json.dumps(spec, indent=2, sort_keys=False) + "\n")
PY

cat > "${OUTDIR}/cpython-${CPYTHON_VERSION}-wasix-build.manifest" <<MANIFEST
cpython_version=${CPYTHON_VERSION}
cpython_source=${CPYTHON_URL}
cpython_sha256=${CPYTHON_SHA256}
openssl_version=${OPENSSL_VERSION}
openssl_source=${OPENSSL_URL}
openssl_sha256=${OPENSSL_SHA256}
toolchain_id=${PYTHON_TOOLCHAIN_ID}
wasi_sdk_version=${WASI_SDK_VERSION_FULL}
wasi_sdk_source=${WASI_SDK_URL}
wasi_sdk_sha256=${WASI_SDK_SHA256}
binaryen_version=${BINARYEN_VERSION}
binaryen_source=${BINARYEN_URL}
binaryen_sha256=${BINARYEN_SHA256}
wasix_libc_version=${WASIX_LIBC_VERSION}
wasix_libc_source=${WASIX_LIBC_SOURCE}
source_date_epoch=${SOURCE_DATE_EPOCH}
wasm=${PYTHON_WASM}
wasm_hash=${PYTHON_HASH}
package=${OUTDIR}/package.json
MANIFEST

file "${PYTHON_WASM}"
echo "wrote ${PYTHON_WASM}"
echo "hash ${PYTHON_HASH}"
echo "package ${OUTDIR}/package.json"
