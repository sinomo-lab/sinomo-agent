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
WASIX_LIBC_TARBALL="${WASIX_LIBC_TARBALL:-sysroot.tar.gz}"
WASIX_LIBC_SHA256="${WASIX_LIBC_SHA256:-e9f7435c0fdc050c1f813f83d3b22388ba3b16d1303ced510915eb028a38abb4}"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1778597230}"
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
WORKDIR="${WORKDIR:-"${ROOT}/work-wasix"}"
OUTDIR="${OUTDIR:-"${ROOT}/out-wasix"}"
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

mkdir -p "${WORKDIR}/downloads" "${OUTDIR}"

download "${CPYTHON_URL}" "${WORKDIR}/downloads/${CPYTHON_TARBALL}"
download "${ZLIB_URL}" "${WORKDIR}/downloads/${ZLIB_TARBALL}"
download "${OPENSSL_URL}" "${WORKDIR}/downloads/${OPENSSL_TARBALL}"
download "${SETUPTOOLS_URL}" "${WORKDIR}/downloads/${SETUPTOOLS_WHEEL}"
download "${WASI_SDK_URL}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
download "${BINARYEN_URL}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
download "${WASIX_LIBC_URL}" "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"
verify_sha256 "${CPYTHON_SHA256}" "${WORKDIR}/downloads/${CPYTHON_TARBALL}"
verify_sha256 "${ZLIB_SHA256}" "${WORKDIR}/downloads/${ZLIB_TARBALL}"
verify_sha256 "${OPENSSL_SHA256}" "${WORKDIR}/downloads/${OPENSSL_TARBALL}"
verify_sha256 "${SETUPTOOLS_SHA256}" "${WORKDIR}/downloads/${SETUPTOOLS_WHEEL}"
verify_sha256 "${WASI_SDK_SHA256}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
verify_sha256 "${BINARYEN_SHA256}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
verify_sha256 "${WASIX_LIBC_SHA256}" "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"

rm -rf \
    "${WORKDIR}/Python-${CPYTHON_VERSION}" \
    "${WORKDIR}/zlib-${ZLIB_VERSION}" \
    "${WORKDIR}/zlib-wasix" \
    "${WORKDIR}/openssl-${OPENSSL_VERSION}" \
    "${WORKDIR}/openssl-wasix" \
    "${WORKDIR}"/wasi-sdk-${WASI_SDK_VERSION_FULL}* \
    "${WORKDIR}"/binaryen-version_${BINARYEN_VERSION} \
    "${WORKDIR}/wasix-sysroot" \
    "${PACKAGE_ROOT}"
tar -C "${WORKDIR}" -xJf "${WORKDIR}/downloads/${CPYTHON_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${ZLIB_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${OPENSSL_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
mkdir -p "${WORKDIR}/wasix-sysroot"
tar -C "${WORKDIR}/wasix-sysroot" -xzf "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"

CPYTHON_SRC="${WORKDIR}/Python-${CPYTHON_VERSION}"
ZLIB_SRC="${WORKDIR}/zlib-${ZLIB_VERSION}"
ZLIB_OUT="${WORKDIR}/zlib-wasix"
OPENSSL_SRC="${WORKDIR}/openssl-${OPENSSL_VERSION}"
OPENSSL_OUT="${WORKDIR}/openssl-wasix"
WASI_SDK="$(find "${WORKDIR}" -maxdepth 1 -type d -name "wasi-sdk-${WASI_SDK_VERSION_FULL}*" | sort | head -n 1)"
BINARYEN="${WORKDIR}/binaryen-version_${BINARYEN_VERSION}"
WASIX_SYSROOT="$(find "${WORKDIR}/wasix-sysroot" -type d -path "*/sysroot" | sort | head -n 1)"
if [ -z "${WASI_SDK}" ] || [ ! -x "${WASI_SDK}/bin/clang" ]; then
    echo "wasi-sdk clang not found after extracting ${WASI_SDK_TARBALL}" >&2
    exit 1
fi
if [ ! -x "${BINARYEN}/bin/wasm-opt" ]; then
    echo "Binaryen wasm-opt not found after extracting ${BINARYEN_TARBALL}" >&2
    exit 1
fi
if [ -z "${WASIX_SYSROOT}" ] || [ ! -f "${WASIX_SYSROOT}/include/unistd.h" ] || [ ! -f "${WASIX_SYSROOT}/lib/wasm32-wasi/libc.a" ]; then
    echo "wasix-libc sysroot not found after extracting ${WASIX_LIBC_TARBALL}" >&2
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

perl -0pi -e 's/defined\(HAVE_NETPACKET_PACKET_H\)/defined(HAVE_NETPACKET_PACKET_H) \&\& defined(AF_PACKET)/g; s/#ifdef HAVE_NETPACKET_PACKET_H/#if defined(HAVE_NETPACKET_PACKET_H) \&\& defined(AF_PACKET)/g' \
    "${CPYTHON_SRC}/Modules/socketmodule.c"
perl -0pi -e 's/\n\s*\[mmap\],(\n\s*\[termios\],)/$1/' \
    "${CPYTHON_SRC}/configure" \
    "${CPYTHON_SRC}/configure.ac"
perl -0pi -e 's/\n\s*py_cv_module_mmap=n\/a(\n\s*py_cv_module_termios=n\/a)/$1/' \
    "${CPYTHON_SRC}/configure"
perl -0pi -e 's/\n\s*\[fcntl\],(\n\s*\[termios\],)/$1/' \
    "${CPYTHON_SRC}/configure.ac"
perl -0pi -e 's/\n\s*py_cv_module_fcntl=n\/a(\n\s*py_cv_module_termios=n\/a)/$1/' \
    "${CPYTHON_SRC}/configure"
perl -0pi -e 's/#define my_getpagesize getpagesize/#define my_getpagesize getpagesize\n#define my_getallocationgranularity my_getpagesize/' \
    "${CPYTHON_SRC}/Modules/mmapmodule.c"
python3 - "${CPYTHON_SRC}/Modules/fcntlmodule.c" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
needle = '''#include "clinic/fcntlmodule.c.h"\n'''
replacement = '''#include "clinic/fcntlmodule.c.h"

#ifndef F_RDLCK
#define F_RDLCK 0
#endif
#ifndef F_WRLCK
#define F_WRLCK 1
#endif
#ifndef F_UNLCK
#define F_UNLCK 2
#endif
#ifndef F_SETLK
#define F_SETLK 7
#endif
#ifndef F_SETLKW
#define F_SETLKW 8
#endif
'''
if needle not in text:
    raise SystemExit("fcntl WASIX lock constant patch target not found")
path.write_text(text.replace(needle, replacement, 1))
PY
perl -0pi -e 's/ -lwasi-emulated-signal//g' \
    "${CPYTHON_SRC}/configure" \
    "${CPYTHON_SRC}/configure.ac"
python3 - "${CPYTHON_SRC}/Lib/subprocess.py" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
replacements = [
    (
        '_can_fork_exec = sys.platform not in {"emscripten", "wasi", "ios", "tvos", "watchos"}',
        '_can_fork_exec = sys.platform not in {"emscripten", "ios", "tvos", "watchos"} and (sys.platform != "wasi" or hasattr(os, "posix_spawn"))',
    ),
    (
        '''else:
    if _can_fork_exec:
        from _posixsubprocess import fork_exec as _fork_exec
        # used in methods that are called by __del__
        class _del_safe:''',
        '''else:
    if _can_fork_exec and sys.platform != "wasi":
        from _posixsubprocess import fork_exec as _fork_exec
    if _can_fork_exec:
        # used in methods that are called by __del__
        class _del_safe:''',
    ),
    (
        '''    # By default, assume that posix_spawn() does not properly report errors.
    return False''',
        '''    if sys.platform == "wasi":
        return True

    # By default, assume that posix_spawn() does not properly report errors.
    return False''',
    ),
]
for needle, replacement in replacements:
    if needle not in text:
        raise SystemExit(f"subprocess WASIX patch target not found: {needle!r}")
    text = text.replace(needle, replacement, 1)
path.write_text(text)
PY
perl -0pi -e 's/and \(not close_fds or _HAVE_POSIX_SPAWN_CLOSEFROM\)/and (not close_fds or _HAVE_POSIX_SPAWN_CLOSEFROM or sys.platform == "wasi")/' \
    "${CPYTHON_SRC}/Lib/subprocess.py"
perl -0pi -e 's/and os\.path\.dirname\(executable\)/and (os.path.dirname(executable) or sys.platform == "wasi")/' \
    "${CPYTHON_SRC}/Lib/subprocess.py"
perl -0pi -e 's/            if close_fds:\n                file_actions\.append\(\(os\.POSIX_SPAWN_CLOSEFROM, 3\)\)/            if close_fds and _HAVE_POSIX_SPAWN_CLOSEFROM:\n                file_actions.append((os.POSIX_SPAWN_CLOSEFROM, 3))/' \
    "${CPYTHON_SRC}/Lib/subprocess.py"
python3 - "${CPYTHON_SRC}/Lib/subprocess.py" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
needle = '''            self.pid = os.posix_spawn(executable, args, env, **kwargs)
            self._child_created = True'''
replacement = '''            spawn_func = os.posix_spawn
            if sys.platform == "wasi" and not os.path.dirname(executable) and hasattr(os, "posix_spawnp"):
                spawn_func = os.posix_spawnp
            try:
                self.pid = spawn_func(executable, args, env, **kwargs)
            except OSError as error:
                if sys.platform != "wasi" or error.errno != errno.ENOEXEC:
                    raise
                shell_args = ["/bin/sh", executable, *args[1:]]
                self.pid = os.posix_spawn("/bin/sh", shell_args, env, **kwargs)
            self._child_created = True'''
if needle not in text:
    raise SystemExit("subprocess WASIX ENOEXEC patch target not found")
path.write_text(text.replace(needle, replacement))
PY
python3 - "${CPYTHON_SRC}/Modules/posixmodule.c" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
replacements = [
    (
        '''enum posix_spawn_file_actions_identifier {
    POSIX_SPAWN_OPEN,
    POSIX_SPAWN_CLOSE,
    POSIX_SPAWN_DUP2
#ifdef HAVE_POSIX_SPAWN_FILE_ACTIONS_ADDCLOSEFROM_NP
    ,POSIX_SPAWN_CLOSEFROM
#endif
};''',
        '''enum posix_spawn_file_actions_identifier {
    POSIX_SPAWN_OPEN,
    POSIX_SPAWN_CLOSE,
    POSIX_SPAWN_DUP2,
    POSIX_SPAWN_CHDIR
#ifdef HAVE_POSIX_SPAWN_FILE_ACTIONS_ADDCLOSEFROM_NP
    ,POSIX_SPAWN_CLOSEFROM
#endif
};''',
    ),
    (
        '''#ifdef HAVE_POSIX_SPAWN_FILE_ACTIONS_ADDCLOSEFROM_NP
            case POSIX_SPAWN_CLOSEFROM: {
                int fd;
                if (!PyArg_ParseTuple(file_action, "Oi"
                        ";A closefrom file_action tuple must have 2 elements",
                        &tag_obj, &fd))
                {
                    goto fail;
                }
                errno = posix_spawn_file_actions_addclosefrom_np(file_actionsp,
                                                                 fd);
                if (errno) {
                    posix_error();
                    goto fail;
                }
                break;
            }
#endif
            default: {''',
        '''            case POSIX_SPAWN_CHDIR: {
                PyObject *path;
                if (!PyArg_ParseTuple(file_action, "OO&"
                        ";A chdir file_action tuple must have 2 elements",
                        &tag_obj, PyUnicode_FSConverter, &path))
                {
                    goto fail;
                }
                if (PyList_Append(temp_buffer, path)) {
                    Py_DECREF(path);
                    goto fail;
                }
                errno = posix_spawn_file_actions_addchdir_np(file_actionsp,
                                                             PyBytes_AS_STRING(path));
                if (errno) {
                    posix_error();
                    Py_DECREF(path);
                    goto fail;
                }
                Py_DECREF(path);
                break;
            }
#ifdef HAVE_POSIX_SPAWN_FILE_ACTIONS_ADDCLOSEFROM_NP
            case POSIX_SPAWN_CLOSEFROM: {
                int fd;
                if (!PyArg_ParseTuple(file_action, "Oi"
                        ";A closefrom file_action tuple must have 2 elements",
                        &tag_obj, &fd))
                {
                    goto fail;
                }
                errno = posix_spawn_file_actions_addclosefrom_np(file_actionsp,
                                                                 fd);
                if (errno) {
                    posix_error();
                    goto fail;
                }
                break;
            }
#endif
            default: {''',
    ),
    (
        '''    if (PyModule_AddIntConstant(m, "POSIX_SPAWN_DUP2", POSIX_SPAWN_DUP2)) return -1;
#ifdef HAVE_POSIX_SPAWN_FILE_ACTIONS_ADDCLOSEFROM_NP''',
        '''    if (PyModule_AddIntConstant(m, "POSIX_SPAWN_DUP2", POSIX_SPAWN_DUP2)) return -1;
    if (PyModule_AddIntConstant(m, "POSIX_SPAWN_CHDIR", POSIX_SPAWN_CHDIR)) return -1;
#ifdef HAVE_POSIX_SPAWN_FILE_ACTIONS_ADDCLOSEFROM_NP''',
    ),
]
for needle, replacement in replacements:
    if needle not in text:
        raise SystemExit(f"posix_spawn chdir patch target not found: {needle!r}")
    text = text.replace(needle, replacement, 1)
path.write_text(text)
PY
python3 - "${CPYTHON_SRC}/Objects/obmalloc.c" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
replacements = [
    (
        '''    return _PyMem_Raw.malloc(_PyMem_Raw.ctx, size);''',
        '''#ifdef __wasi__
    return _PyMem_RawMalloc(NULL, size);
#else
    return _PyMem_Raw.malloc(_PyMem_Raw.ctx, size);
#endif''',
    ),
    (
        '''    return _PyMem_Raw.calloc(_PyMem_Raw.ctx, nelem, elsize);''',
        '''#ifdef __wasi__
    return _PyMem_RawCalloc(NULL, nelem, elsize);
#else
    return _PyMem_Raw.calloc(_PyMem_Raw.ctx, nelem, elsize);
#endif''',
    ),
    (
        '''    return _PyMem_Raw.realloc(_PyMem_Raw.ctx, ptr, new_size);''',
        '''#ifdef __wasi__
    return _PyMem_RawRealloc(NULL, ptr, new_size);
#else
    return _PyMem_Raw.realloc(_PyMem_Raw.ctx, ptr, new_size);
#endif''',
    ),
    (
        '''    _PyMem_Raw.free(_PyMem_Raw.ctx, ptr);''',
        '''#ifdef __wasi__
    _PyMem_RawFree(NULL, ptr);
#else
    _PyMem_Raw.free(_PyMem_Raw.ctx, ptr);
#endif''',
    ),
]
for needle, replacement in replacements:
    if needle not in text:
        raise SystemExit(f"obmalloc WASIX raw allocator patch target not found: {needle!r}")
    text = text.replace(needle, replacement, 1)
path.write_text(text)
PY
python3 - "${CPYTHON_SRC}/Lib/subprocess.py" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
needle = '''            if executable is None:
                executable = args[0]

            sys.audit("subprocess.Popen", executable, args, cwd, env)'''
replacement = '''            if executable is None:
                executable = args[0]
            if sys.platform == "wasi" and not executable:
                executable = "/bin/python"
                if args and not args[0]:
                    args[0] = executable

            sys.audit("subprocess.Popen", executable, args, cwd, env)'''
if needle not in text:
    raise SystemExit("subprocess WASIX executable patch target not found")
path.write_text(text.replace(needle, replacement))
PY
python3 - "${CPYTHON_SRC}/Lib/subprocess.py" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
replacements = [
    (
        '''_HAVE_POSIX_SPAWN_CLOSEFROM = hasattr(os, 'POSIX_SPAWN_CLOSEFROM')''',
        '''_HAVE_POSIX_SPAWN_CLOSEFROM = hasattr(os, 'POSIX_SPAWN_CLOSEFROM')
_HAVE_POSIX_SPAWN_CHDIR = hasattr(os, 'POSIX_SPAWN_CHDIR')''',
    ),
    (
        '''        def _posix_spawn(self, args, executable, env, restore_signals, close_fds,
                         p2cread, p2cwrite,
                         c2pread, c2pwrite,
                         errread, errwrite):''',
        '''        def _posix_spawn(self, args, executable, env, restore_signals, close_fds, cwd,
                         p2cread, p2cwrite,
                         c2pread, c2pwrite,
                         errread, errwrite):''',
    ),
    (
        '''            if close_fds and _HAVE_POSIX_SPAWN_CLOSEFROM:
                file_actions.append((os.POSIX_SPAWN_CLOSEFROM, 3))

            if file_actions:''',
        '''            if cwd is not None:
                file_actions.append((os.POSIX_SPAWN_CHDIR, cwd))
            elif sys.platform == "wasi" and _HAVE_POSIX_SPAWN_CHDIR:
                file_actions.append((os.POSIX_SPAWN_CHDIR, os.getcwd()))

            if close_fds and _HAVE_POSIX_SPAWN_CLOSEFROM:
                file_actions.append((os.POSIX_SPAWN_CLOSEFROM, 3))

            if file_actions:''',
    ),
    (
        '''                    and cwd is None
                    and (p2cread == -1 or p2cread > 2)''',
        '''                    and (cwd is None or _HAVE_POSIX_SPAWN_CHDIR)
                    and (p2cread == -1 or p2cread > 2)''',
    ),
    (
        '''                self._posix_spawn(args, executable, env, restore_signals, close_fds,
                                  p2cread, p2cwrite,''',
        '''                self._posix_spawn(args, executable, env, restore_signals, close_fds, cwd,
                                  p2cread, p2cwrite,''',
    ),
]
for needle, replacement in replacements:
    if needle not in text:
        raise SystemExit(f"subprocess WASIX posix_spawn cwd patch target not found: {needle!r}")
    text = text.replace(needle, replacement, 1)
path.write_text(text)
PY

WASIXCC_SETTINGS=(
    "-sMODULE_KIND=dynamic-main"
    "-sPIC=yes"
    "-sEMULATE_FUNCTION_POINTER_CASTS=1"
    "-sRUN_WASM_OPT=no"
)
WASIX_CFLAGS="-Os -fPIC -fno-ident -ffile-prefix-map=${WORKDIR}=sinomo-python-wasix-build -ffile-prefix-map=${REPO_ROOT}=sinomo"
WASIX_LDFLAGS="-L${ZLIB_OUT}/lib -L${OPENSSL_OUT}/lib"
SINOMO_WASIX_ARGV_COMPAT_SOURCE="${WORKDIR}/sinomo-wasix-argv-compat.c"
SINOMO_WASIX_ARGV_COMPAT_OBJECT="${WORKDIR}/sinomo-wasix-argv-compat.o"
cat > "${SINOMO_WASIX_ARGV_COMPAT_SOURCE}" <<'COMPAT'
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <wasi/api_wasix.h>

static const char magic[] = "__SINOMO_WASIX_ESCAPED_STRING_LIST_V1__\n";
extern char **environ;
extern int __real_fcntl(int fd, int cmd, ...);

#ifndef F_RDLCK
#define F_RDLCK 0
#endif
#ifndef F_WRLCK
#define F_WRLCK 1
#endif
#ifndef F_UNLCK
#define F_UNLCK 2
#endif
#ifndef F_GETLK
#define F_GETLK 9
#endif
#ifndef F_SETLK
#define F_SETLK 7
#endif
#ifndef F_SETLKW
#define F_SETLKW 8
#endif

static int sinomo_mkdir_if_missing(const char *path) {
    if (mkdir(path, 0700) == 0 || errno == EEXIST) {
        return 0;
    }
    return -1;
}

static int sinomo_lock_path(int fd, char *buffer, size_t len) {
    struct stat st;
    if (fstat(fd, &st) != 0) {
        return -1;
    }

    const char *tmp = getenv("TMPDIR");
    if (tmp == NULL || tmp[0] == 0) {
        tmp = "/tmp";
    }

    char dir[256];
    int dir_len = snprintf(dir, sizeof(dir), "%s/.sinomo-fcntl-locks", tmp);
    if (dir_len <= 0 || (size_t)dir_len >= sizeof(dir)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    if (sinomo_mkdir_if_missing(tmp) != 0 || sinomo_mkdir_if_missing(dir) != 0) {
        return -1;
    }

    int path_len = snprintf(
        buffer,
        len,
        "%s/%llu-%llu.lock",
        dir,
        (unsigned long long)st.st_dev,
        (unsigned long long)st.st_ino
    );
    if (path_len <= 0 || (size_t)path_len >= len) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

int __wrap_fcntl(int fd, int cmd, ...) {
    va_list ap;
    va_start(ap, cmd);

    if (cmd == F_GETLK || cmd == F_SETLK || cmd == F_SETLKW) {
        struct flock *lock = va_arg(ap, struct flock *);
        va_end(ap);

        if (lock == NULL) {
            errno = EINVAL;
            return -1;
        }

        char path[320];
        if (sinomo_lock_path(fd, path, sizeof(path)) != 0) {
            return -1;
        }

        if (cmd == F_GETLK) {
            if (access(path, F_OK) == 0) {
                lock->l_type = F_WRLCK;
                lock->l_pid = 0;
            } else {
                lock->l_type = F_UNLCK;
            }
            return 0;
        }

        if (lock->l_type == F_UNLCK) {
            if (unlink(path) == 0 || errno == ENOENT) {
                return 0;
            }
            return -1;
        }

        for (;;) {
            int lock_fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
            if (lock_fd >= 0) {
                char owner[32];
                int owner_len = snprintf(owner, sizeof(owner), "%ld\n", (long)getpid());
                if (owner_len > 0) {
                    write(lock_fd, owner, (size_t)owner_len);
                }
                close(lock_fd);
                return 0;
            }
            if (errno != EEXIST || cmd == F_SETLK) {
                if (errno == EEXIST) {
                    errno = EACCES;
                }
                return -1;
            }
            struct timespec delay = {0, 10000000};
            nanosleep(&delay, NULL);
        }
    }

    int int_arg = va_arg(ap, int);
    va_end(ap);
    return __real_fcntl(fd, cmd, int_arg);
}

char *__wrap___wasilibc_exec_combine_strings(char *const strings[]) {
    size_t combined_len = sizeof(magic) - 1;
    for (char **ptr = (char **)strings; *ptr != NULL; ptr++) {
        for (const unsigned char *p = (const unsigned char *)*ptr; *p != 0; p++) {
            combined_len += (*p == '\n' || *p == '\r' || *p == '\\') ? 2 : 1;
        }
        combined_len += 1;
    }

    char *combined = malloc(combined_len + 1);
    char *out = combined;
    memcpy(out, magic, sizeof(magic) - 1);
    out += sizeof(magic) - 1;
    for (char **ptr = (char **)strings; *ptr != NULL; ptr++) {
        for (const unsigned char *p = (const unsigned char *)*ptr; *p != 0; p++) {
            if (*p == '\n') {
                *out++ = '\\';
                *out++ = 'n';
            } else if (*p == '\r') {
                *out++ = '\\';
                *out++ = 'r';
            } else if (*p == '\\') {
                *out++ = '\\';
                *out++ = '\\';
            } else {
                *out++ = (char)*p;
            }
        }
        *out++ = '\n';
    }
    *out = 0;
    return combined;
}

int __wrap_execve(const char *path, char *const argv[], char *const envp[]) {
    char *combined_argv = __wrap___wasilibc_exec_combine_strings(argv);
    char *combined_env = __wrap___wasilibc_exec_combine_strings(envp ? envp : environ);
    const char *search_path = getenv("PATH");
    if (search_path == NULL) {
        search_path = "";
    }

    __wasi_errno_t error = __wasi_proc_exec3(path, combined_argv, combined_env, 0, search_path);
    free(combined_argv);
    free(combined_env);
    errno = (int)error;
    return -1;
}

COMPAT
"${WASIXCC_BIN}" \
    "${WASIXCC_SETTINGS[@]}" \
    ${WASIX_CFLAGS} \
    -c "${SINOMO_WASIX_ARGV_COMPAT_SOURCE}" \
    -o "${SINOMO_WASIX_ARGV_COMPAT_OBJECT}"

mkdir -p "${ZLIB_OUT}/lib" "${ZLIB_OUT}/include"
(
    cd "${ZLIB_SRC}"
    for source in \
        adler32.c compress.c crc32.c deflate.c gzclose.c gzlib.c gzread.c \
        gzwrite.c infback.c inffast.c inflate.c inftrees.c trees.c uncompr.c zutil.c
    do
        "${WASIXCC_BIN}" \
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
    CC="${WASIXCC_BIN} -sPIC=yes -sRUN_WASM_OPT=no" \
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
cat > "${CONFIG_SITE_FILE}" <<'CONFIG_SITE'
ac_cv_pthread_is_default=yes
ac_cv_kpthread=no
ac_cv_kthread=no
ac_cv_pthread=no
ac_cv_cxx_thread=yes
ac_cv_func_acosh=yes
ac_cv_func_asinh=yes
ac_cv_func_atanh=yes
ac_cv_func_erf=yes
ac_cv_func_erfc=yes
ac_cv_func_expm1=yes
ac_cv_func_log1p=yes
ac_cv_func_log2=yes
ac_cv_func_clock_gettime=yes
ac_cv_func_clock_getres=yes
ac_cv_func_clock=yes
ac_cv_func_dup2=yes
ac_cv_func_fstatat=yes
ac_cv_func_getegid=yes
ac_cv_func_geteuid=yes
ac_cv_func_getgid=yes
ac_cv_func_gethostname=yes
ac_cv_func_getpid=yes
ac_cv_func_getppid=yes
ac_cv_func_getuid=yes
ac_cv_func_lstat=yes
ac_cv_func_mmap=yes
ac_cv_func_nanosleep=yes
ac_cv_func_timegm=yes
ac_cv_func_umask=yes
ac_cv_func_uname=yes
ac_cv_func_memfd_create=no
ac_cv_func_dlopen=yes
ac_cv_lib_dl_dlopen=no
ac_cv_file__dev_ptmx=no
ac_cv_file__dev_ptc=no
py_cv_module__bz2=n/a
py_cv_module__lzma=n/a
py_cv_module_readline=n/a
py_cv_module__uuid=n/a
py_cv_module__ctypes=n/a
CONFIG_SITE

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
    ../../configure \
        --host=wasm32-wasi \
        --build="$(../../config.guess)" \
        --with-build-python="${HOST_BUILD_PYTHON}" \
        --with-openssl="${OPENSSL_OUT}" \
        --with-openssl-rpath=no \
        --without-readline \
        --disable-test-modules
    python3 - Makefile "${SINOMO_WASIX_ARGV_COMPAT_OBJECT}" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
object_path = sys.argv[2]
text = path.read_text()
text = text.replace(
    "LIBS=\t\t",
    f"LIBS=\t\t{object_path} -lc++ -lc++abi -lunwind ",
    1,
)
text = text.replace(
    "LDFLAGS=\t",
    "LDFLAGS=\t-Wl,--wrap=__wasilibc_exec_combine_strings -Wl,--wrap=execve -Wl,--wrap=fcntl "
    + " ".join(
        f"-Wl,--undefined={symbol} -Wl,--export-if-defined={symbol}"
        for symbol in [
            "__wasm_lpad_context",
            "__gxx_personality_wasm0",
            "__cxa_allocate_exception",
            "__cxa_free_exception",
            "__cxa_throw",
            "__cxa_begin_catch",
            "__cxa_end_catch",
            "__cxa_rethrow",
            "__cxa_demangle",
            "__cxa_guard_acquire",
            "__cxa_guard_abort",
            "__cxa_guard_release",
            "__cxa_atexit",
            "_ZSt9terminatev",
            "_ZSt17current_exceptionv",
            "_ZSt17rethrow_exceptionSt13exception_ptr",
            "_ZNSt13exception_ptrC1ERKS_",
            "_ZNSt13exception_ptrD1Ev",
            "_ZNSt13exception_ptraSERKS_",
            "_ZNSt11logic_errorC2EPKc",
            "_ZNSt11logic_errorC2ERKNSt3__212basic_stringIcNS0_11char_traitsIcEENS0_9allocatorIcEEEE",
            "_ZNSt9exceptionD1Ev",
            "_ZNSt9exceptionD2Ev",
            "_ZNKSt9exception4whatEv",
            "_ZTVSt9exception",
            "_ZTISt9exception",
            "_ZTSSt9exception",
            "_ZNSt9bad_allocC1Ev",
            "_ZNSt9bad_allocD1Ev",
            "_ZNSt9bad_allocD2Ev",
            "_ZNKSt9bad_alloc4whatEv",
            "_ZTVSt9bad_alloc",
            "_ZTISt9bad_alloc",
            "_ZTSSt9bad_alloc",
            "_ZNSt20bad_array_new_lengthC1Ev",
            "_ZNSt20bad_array_new_lengthD1Ev",
            "_ZNSt20bad_array_new_lengthD2Ev",
            "_ZTVSt20bad_array_new_length",
            "_ZTISt20bad_array_new_length",
            "_ZTSSt20bad_array_new_length",
            "_ZNSt13runtime_errorC1EPKc",
            "_ZNSt13runtime_errorC1ERKNSt3__212basic_stringIcNS0_11char_traitsIcEENS0_9allocatorIcEEEE",
            "_ZNSt13runtime_errorC2EPKc",
            "_ZNSt13runtime_errorC2ERKNSt3__212basic_stringIcNS0_11char_traitsIcEENS0_9allocatorIcEEEE",
            "_ZNSt13runtime_errorD1Ev",
            "_ZNSt13runtime_errorD2Ev",
            "_ZNKSt13runtime_error4whatEv",
            "_ZTVSt13runtime_error",
            "_ZTISt13runtime_error",
            "_ZTSSt13runtime_error",
            "_ZTVSt12domain_error",
            "_ZTISt12domain_error",
            "_ZTSSt12domain_error",
            "_ZNSt12length_errorD1Ev",
            "_ZNSt12length_errorD2Ev",
            "_ZTVSt12length_error",
            "_ZTISt12length_error",
            "_ZTSSt12length_error",
            "_ZTVSt12out_of_range",
            "_ZTISt12out_of_range",
            "_ZTSSt12out_of_range",
            "_ZTVSt16invalid_argument",
            "_ZTISt16invalid_argument",
            "_ZTSSt16invalid_argument",
            "_ZTVSt11range_error",
            "_ZTISt11range_error",
            "_ZTSSt11range_error",
            "_ZTVSt14overflow_error",
            "_ZTISt14overflow_error",
            "_ZTSSt14overflow_error",
            "_ZTISt16nested_exception",
            "_ZTSSt16nested_exception",
            "_ZTVN10__cxxabiv117__class_type_infoE",
            "_ZTVN10__cxxabiv119__pointer_type_infoE",
            "_ZTVN10__cxxabiv120__function_type_infoE",
            "_ZTVN10__cxxabiv120__si_class_type_infoE",
            "_ZTVN10__cxxabiv121__vmi_class_type_infoE",
            "_ZNKSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE4findEcm",
            "_ZNSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE17__assign_externalEPKc",
            "_ZNSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE17__assign_externalEPKcm",
            "_ZNSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE17__assign_no_aliasILb0EEERS5_PKcm",
            "_ZNSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE17__assign_no_aliasILb1EEERS5_PKcm",
            "_ZNSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE25__init_copy_ctor_externalEPKcm",
            "_ZNSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE26__erase_external_with_moveEmm",
            "_ZNSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE6appendEPKc",
            "_ZNSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE6appendEPKcm",
            "_ZNSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE6appendERKS5_mm",
            "_ZNSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE6insertEmPKc",
            "_ZNSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE6insertEmPKcm",
            "_ZNSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE6resizeEmc",
            "_ZNSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE7reserveEm",
            "_ZNSt3__212basic_stringIcNS_11char_traitsIcEENS_9allocatorIcEEE9push_backEc",
            "_ZNKSt3__215basic_stringbufIcNS_11char_traitsIcEENS_9allocatorIcEEE3strEv",
            "_ZNSt3__215basic_stringbufIcNS_11char_traitsIcEENS_9allocatorIcEEE3strERKNS_12basic_stringIcS2_S4_EE",
            "_ZNSt3__218basic_stringstreamIcNS_11char_traitsIcEENS_9allocatorIcEEED1Ev",
            "_ZNSt3__213basic_ostreamIcNS_11char_traitsIcEEE3putEc",
            "_ZNSt3__213basic_ostreamIcNS_11char_traitsIcEEE5flushEv",
            "_ZNSt3__213basic_ostreamIcNS_11char_traitsIcEEE6sentryC1ERS3_",
            "_ZNSt3__213basic_ostreamIcNS_11char_traitsIcEEE6sentryD1Ev",
            "_ZNSt3__213basic_ostreamIcNS_11char_traitsIcEEED2Ev",
            "_ZNSt3__213basic_ostreamIcNS_11char_traitsIcEEElsEd",
            "_ZNSt3__213basic_ostreamIcNS_11char_traitsIcEEElsEi",
            "_ZNSt3__213basic_ostreamIcNS_11char_traitsIcEEElsEj",
            "_ZNSt3__213basic_ostreamIcNS_11char_traitsIcEEElsEl",
            "_ZNSt3__213basic_ostreamIcNS_11char_traitsIcEEElsEy",
            "_ZNSt3__214basic_iostreamIcNS_11char_traitsIcEEED2Ev",
            "_ZNKSt3__26locale9use_facetERNS0_2idE",
            "_ZNSt3__26localeC1Ev",
            "_ZNSt3__26localeD1Ev",
            "_ZNSt3__25ctypeIcE2idE",
            "_ZNKSt3__28ios_base6getlocEv",
            "_ZNSt3__28ios_base33__set_badbit_and_consider_rethrowEv",
            "_ZNSt3__28ios_base4initEPv",
            "_ZNSt3__28ios_base5clearEj",
            "_ZNSt3__29basic_iosIcNS_11char_traitsIcEEED2Ev",
            "_ZNSt3__24coutE",
            "_ZNSt3__25mutex4lockEv",
            "_ZNSt3__25mutex6unlockEv",
            "_ZNSt3__25mutexD1Ev",
            "_ZNSt3__215recursive_mutexC1Ev",
            "_ZNSt3__215recursive_mutexD1Ev",
            "_ZNSt3__218condition_variable10notify_allEv",
            "_ZNSt3__218condition_variable4waitERNS_11unique_lockINS_5mutexEEE",
            "_ZNSt3__218condition_variableD1Ev",
            "_ZNSt3__215__thread_structC1Ev",
            "_ZNSt3__215__thread_structD1Ev",
            "_ZNSt3__219__thread_local_dataEv",
            "_ZNSt3__26thread20hardware_concurrencyEv",
            "_ZNSt3__26thread4joinEv",
            "_ZNSt3__26threadD1Ev",
            "_ZNSt3__211__call_onceERVmPvPFvS2_E",
            "_ZNSt3__220__throw_system_errorEiPKc",
            "_ZNSt3__222__libcpp_verbose_abortEPKcz",
            "_ZNSt3__29to_stringEi",
            "_ZNSt3__29to_stringEj",
            "_ZNSt3__29to_stringEl",
            "_ZNSt3__29to_stringEm",
            "_ZNSt3__212__next_primeEm",
            "_ZNSt3__219piecewise_constructE",
            "_ZTVNSt3__217bad_function_callE",
            "_ZTINSt3__217bad_function_callE",
            "_ZTSNSt3__217bad_function_callE",
            "_ZNSt3__217bad_function_callD1Ev",
            "_ZTINSt3__219__shared_weak_countE",
            "_ZNKSt3__219__shared_weak_count13__get_deleterERKSt9type_info",
            "_ZNSt3__219__shared_weak_count14__release_weakEv",
            "_ZNSt3__219__shared_weak_countD2Ev",
            "_ZTH5errno",
            "_Znwm",
            "_ZnwmSt11align_val_t",
            "_ZdlPv",
            "_ZdlPvm",
            "_ZdlPvSt11align_val_t",
            "_ZdlPvmSt11align_val_t",
        ]
    )
    + " ",
    1,
)
text = text.replace(
    "Python/$(DYNLOADFILE) \\\n",
    "Python/$(DYNLOADFILE) \\\n\t\tPython/emscripten_trampoline.o \\\n",
    1,
)
path.write_text(text)
PY
    perl -0pi -e 's/ -lwasi-emulated-signal//g; s/ -latomic//g' Makefile
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
python3 - "${PACKAGE_ROOT}/Lib/site-packages/pip/_internal/build_env.py" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
needle = '''            # The prefix specified two lines above, thus
            # target from config file or env var should be ignored
            "--target",
            "",
'''
replacement = '''            # Sinomo/WASIX does not set PIP_TARGET in the default environment;
            # an empty --target conflicts with --prefix in this runtime.
'''
if needle not in text:
    raise SystemExit("pip build environment target patch target not found")
path.write_text(text.replace(needle, replacement))
PY
python3 - "${PACKAGE_ROOT}/Lib/site-packages/pip/_vendor/packaging/tags.py" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
needle = '''def _generic_platforms() -> Iterator[str]:
    yield _normalize_string(sysconfig.get_platform())
'''
replacement = '''def _generic_platforms() -> Iterator[str]:
    platform = _normalize_string(sysconfig.get_platform())
    yield platform
    if platform == "wasi_0_0_0_wasm32":
        yield "wasix_wasm32"
'''
if needle not in text:
    raise SystemExit("pip WASIX platform tag patch target not found")
path.write_text(text.replace(needle, replacement))
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
if [ -d "${WASIX_BUILD_DIR}/build/lib.wasi-wasm32-3.13" ]; then
    cp -a "${WASIX_BUILD_DIR}/build/lib.wasi-wasm32-3.13" \
        "${PACKAGE_ROOT}/cross-build/wasm32-wasix/build/"
fi

cat > "${PACKAGE_ROOT}/Lib/sitecustomize.py" <<'PY'
import importlib._bootstrap_external
import importlib.machinery
import os
import sys

if not sys.executable:
    sys.executable = "/bin/python"

for _path in (
    "/work/.python-packages",
    "/cross-build/wasm32-wasix/build/lib.wasi-wasm32-3.13",
    "/Lib/site-packages",
):
    if _path not in sys.path:
        sys.path.insert(0, _path)

os.environ.setdefault("HOME", "/home")
os.environ.setdefault("MPLBACKEND", "Agg")
os.environ.setdefault("PIP_DISABLE_PIP_VERSION_CHECK", "1")
os.environ.setdefault("PIP_FIND_LINKS", "/Lib/sinomo-wheels")
os.environ.setdefault("PYTHONHOME", "/")
os.environ.setdefault("SSL_CERT_FILE", "/etc/ssl/certs/ca-certificates.crt")
os.environ.setdefault("TMPDIR", "/work/.tmp")
try:
    os.makedirs(os.environ["TMPDIR"], exist_ok=True)
except OSError:
    pass

for _suffix in (".cpython-313-wasm32-wasi.so",):
    if _suffix not in importlib._bootstrap_external.EXTENSION_SUFFIXES:
        importlib._bootstrap_external.EXTENSION_SUFFIXES.append(_suffix)
    if _suffix not in importlib.machinery.EXTENSION_SUFFIXES:
        importlib.machinery.EXTENSION_SUFFIXES.append(_suffix)

sys.path_hooks[:] = [
    _hook for _hook in sys.path_hooks if "FileFinder" not in repr(_hook)
]
sys.path_hooks.append(
    importlib.machinery.FileFinder.path_hook(
        (
            importlib.machinery.ExtensionFileLoader,
            importlib.machinery.EXTENSION_SUFFIXES,
        ),
        (importlib.machinery.SourceFileLoader, importlib.machinery.SOURCE_SUFFIXES),
        (
            importlib.machinery.SourcelessFileLoader,
            importlib.machinery.BYTECODE_SUFFIXES,
        ),
    )
)
sys.path_importer_cache.clear()
PY

SYSCONFIG_DATA_DIR="${PACKAGE_ROOT}/cross-build/wasm32-wasix/build/lib.wasi-wasm32-3.13"
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
wasix_libc_source=${WASIX_LIBC_URL}
wasix_libc_sha256=${WASIX_LIBC_SHA256}
source_date_epoch=${SOURCE_DATE_EPOCH}
wasm=${PYTHON_WASM}
wasm_hash=${PYTHON_HASH}
package=${OUTDIR}/package.json
MANIFEST

file "${PYTHON_WASM}"
echo "wrote ${PYTHON_WASM}"
echo "hash ${PYTHON_HASH}"
echo "package ${OUTDIR}/package.json"
