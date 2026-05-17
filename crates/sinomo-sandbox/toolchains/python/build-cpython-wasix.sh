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
#ifndef F_DUPFD_CLOEXEC
#define F_DUPFD_CLOEXEC 1030
#endif
#ifndef F_SETPIPE_SZ
#define F_SETPIPE_SZ 1031
#endif
#ifndef F_GETPIPE_SZ
#define F_GETPIPE_SZ 1032
#endif
#ifndef F_ADD_SEALS
#define F_ADD_SEALS 1033
#endif
#ifndef F_GET_SEALS
#define F_GET_SEALS 1034
#endif
#ifndef F_OFD_GETLK
#define F_OFD_GETLK 36
#endif
#ifndef F_OFD_SETLK
#define F_OFD_SETLK 37
#endif
#ifndef F_OFD_SETLKW
#define F_OFD_SETLKW 38
#endif
#ifndef F_SEAL_SEAL
#define F_SEAL_SEAL 0x0001
#endif
#ifndef F_SEAL_SHRINK
#define F_SEAL_SHRINK 0x0002
#endif
#ifndef F_SEAL_GROW
#define F_SEAL_GROW 0x0004
#endif
#ifndef F_SEAL_WRITE
#define F_SEAL_WRITE 0x0008
#endif
#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
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
_HAVE_POSIX_SPAWN_CHDIR = hasattr(os, 'POSIX_SPAWN_CHDIR')

def _sinomo_spawn_close_actions(first_fd):
    try:
        fds = sorted(
            int(name) for name in os.listdir("/proc/self/fd")
            if name.isdecimal() and int(name) >= first_fd
        )
        return [(os.POSIX_SPAWN_CLOSE, fd) for fd in fds]
    except (FileNotFoundError, NotADirectoryError, PermissionError, OSError):
        pass
    try:
        import resource
        limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        if limit == resource.RLIM_INFINITY:
            raise ValueError
    except (ImportError, OSError, ValueError):
        try:
            limit = os.sysconf("SC_OPEN_MAX")
        except (AttributeError, OSError, ValueError):
            limit = 1024
    return [(os.POSIX_SPAWN_CLOSE, fd) for fd in range(first_fd, int(limit))]''',
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
            elif close_fds and sys.platform == "wasi":
                file_actions.extend(_sinomo_spawn_close_actions(3))

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
WASIX_CFLAGS="-Os -fPIC -D_WASI_EMULATED_MMAN -fno-ident -ffile-prefix-map=${WORKDIR}=sinomo-python-wasix-build -ffile-prefix-map=${REPO_ROOT}=sinomo"
WASIX_LDFLAGS="-L${ZLIB_OUT}/lib -L${OPENSSL_OUT}/lib -lwasi-emulated-mman -lcrypt"
SINOMO_WASIX_ARGV_COMPAT_SOURCE="${WORKDIR}/sinomo-wasix-argv-compat.c"
SINOMO_WASIX_ARGV_COMPAT_OBJECT="${WORKDIR}/sinomo-wasix-argv-compat.o"
cat > "${SINOMO_WASIX_ARGV_COMPAT_SOURCE}" <<'COMPAT'
#include <errno.h>
#include <err.h>
#include <dirent.h>
#include <fenv.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <glob.h>
#include <grp.h>
#include <langinfo.h>
#include <libgen.h>
#include <limits.h>
#include <locale.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <pwd.h>
#include <sched.h>
#include <search.h>
#include <shadow.h>
#include <signal.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdio_ext.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <sys/random.h>
#include <sys/sendfile.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/statvfs.h>
#include <sys/times.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/sysinfo.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/un.h>
#include <syslog.h>
#include <netinet/tcp.h>
#include <time.h>
#include <termios.h>
#include <unistd.h>
#include <utime.h>
#include <wchar.h>
#include <wctype.h>
#include <wasi/api_wasi.h>
#include <wasi/api_wasix.h>

static const char magic[] = "__SINOMO_WASIX_ESCAPED_STRING_LIST_V1__\n";
extern char **environ;
extern ssize_t __real_read(int fd, void *buf, size_t count);
extern ssize_t __real_write(int fd, const void *buf, size_t count);
extern int __real_close(int fd);
extern int __real_fcntl(int fd, int cmd, ...);
extern int __real_munmap(void *addr, size_t len);
extern int __real_utimensat(int dirfd, const char *path, const struct timespec times[2], int flags);
int fchdir(int fd);
ssize_t __wrap_read(int fd, void *buf, size_t count);
ssize_t __wrap_write(int fd, const void *buf, size_t count);
int __wrap_close(int fd);
int __wrap_fcntl(int fd, int cmd, ...);
int __wrap_openat(int dirfd, const char *path, int flags, ...);
int __wrap_fsync(int fd);
int __wrap_fdatasync(int fd);
void __wrap_sync(void);
int __wrap_syncfs(int fd);
ssize_t __wrap_pread(int fd, void *buf, size_t count, off_t offset);
ssize_t __wrap_pwrite(int fd, const void *buf, size_t count, off_t offset);
ssize_t __wrap_readv(int fd, const struct iovec *iov, int iovcnt);
ssize_t __wrap_writev(int fd, const struct iovec *iov, int iovcnt);
ssize_t __wrap_preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset);
ssize_t __wrap_pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset);
ssize_t __wrap_preadv2(int fd, const struct iovec *iov, int iovcnt, off_t offset, int flags);
ssize_t __wrap_pwritev2(int fd, const struct iovec *iov, int iovcnt, off_t offset, int flags);
int __wrap_ftruncate(int fd, off_t length);
int __wrap_truncate(const char *path, off_t length);
int __wrap_getrlimit(int resource, struct rlimit *limit);
int __wrap_setrlimit(int resource, const struct rlimit *limit);
int __wrap_prlimit(pid_t pid, int resource, const struct rlimit *new_limit, struct rlimit *old_limit);
int __wrap_getrusage(int who, struct rusage *usage);
clock_t __wrap_times(struct tms *buf);
int __wrap_getpriority(int which, id_t who);
int __wrap_setpriority(int which, id_t who, int priority);
int __wrap_sched_getscheduler(pid_t pid);
int __wrap_sched_getparam(pid_t pid, struct sched_param *param);
int __wrap_sched_setparam(pid_t pid, const struct sched_param *param);
int __wrap_sched_setscheduler(pid_t pid, int policy, const struct sched_param *param);
int __wrap_sched_getaffinity(pid_t pid, size_t cpusetsize, void *mask);
int __wrap_sched_setaffinity(pid_t pid, size_t cpusetsize, const void *mask);
int __wrap_sched_yield(void);
int __wrap_sched_rr_get_interval(pid_t pid, struct timespec *interval);
pid_t __wrap_tcgetpgrp(int fd);
int __wrap_tcsetpgrp(int fd, pid_t pgid);
int __wrap_posix_openpt(int flags);
int __wrap_grantpt(int fd);
int __wrap_unlockpt(int fd);
char *__wrap_ptsname(int fd);
int __wrap_ptsname_r(int fd, char *buf, size_t buflen);
int __wrap_openpty(int *amaster, int *aslave, char *name, const struct termios *termp, const struct winsize *winp);
int __wrap_login_tty(int fd);
pid_t __wrap_forkpty(int *amaster, char *name, const struct termios *termp, const struct winsize *winp);
int __wrap_clock_nanosleep(clockid_t clock_id, int flags, const struct timespec *request, struct timespec *remain);
int __wrap_timer_create(clockid_t clock_id, struct sigevent *event, timer_t *timer_id);
int __wrap_timer_settime(timer_t timer_id, int flags, const struct itimerspec *new_value, struct itimerspec *old_value);
int __wrap_timer_gettime(timer_t timer_id, struct itimerspec *value);
int __wrap_timer_getoverrun(timer_t timer_id);
int __wrap_timer_delete(timer_t timer_id);
int __wrap_timerfd_create(clockid_t clock_id, int flags);
int __wrap_timerfd_settime(int fd, int flags, const struct itimerspec *new_value, struct itimerspec *old_value);
int __wrap_timerfd_gettime(int fd, struct itimerspec *value);
int __wrap_signalfd(int fd, const sigset_t *mask, int flags);
int __wrap_inotify_init(void);
int __wrap_inotify_init1(int flags);
int __wrap_inotify_add_watch(int fd, const char *path, uint32_t mask);
int __wrap_inotify_rm_watch(int fd, int wd);
int __wrap_fanotify_init(unsigned int flags, unsigned int event_f_flags);
int __wrap_fanotify_mark(int fd, unsigned int flags, uint64_t mask, int dirfd, const char *path);
int __wrap_name_to_handle_at(int dirfd, const char *path, void *handle, int *mount_id, int flags);
int __wrap_open_by_handle_at(int mount_fd, void *handle, int flags);
int __wrap_utimensat(int dirfd, const char *path, const struct timespec times[2], int flags);
pid_t __wrap_getpid(void);
pid_t gettid(void);
uid_t __wrap_getuid(void);
pid_t __wrap_getpgrp(void);
pid_t __wrap_setsid(void);
pid_t __wrap_fork(void);
pid_t __wrap_vfork(void);
int __wrap_prctl(int option, ...);
int __wrap_kill(pid_t pid, int signum);
int __wrap_sigaltstack(const stack_t *stack, stack_t *old_stack);
int __wrap_chdir(const char *path);
int __wrap_mknod(const char *path, mode_t mode, dev_t dev);
int __wrap_mknodat(int dirfd, const char *path, mode_t mode, dev_t dev);
int __wrap_flock(int fd, int operation);
ssize_t __wrap_copy_file_range(int fd_in, off_t *off_in, int fd_out, off_t *off_out, size_t len, unsigned int flags);
ssize_t __wrap_sendfile(int out_fd, int in_fd, off_t *offset, size_t count);
ssize_t __wrap_splice(int fd_in, off_t *off_in, int fd_out, off_t *off_out, size_t len, unsigned int flags);
ssize_t __wrap_vmsplice(int fd, const struct iovec *iov, unsigned long nr_segs, unsigned int flags);
int __wrap_memfd_create(const char *name, unsigned int flags);
int __wrap_memfd_secret(unsigned int flags);
int __wrap_eventfd(unsigned int initval, int flags);
int __wrap_dup(int oldfd);
ssize_t __wrap_getdents64(int fd, void *buf, size_t count);
int __wrap_statfs(const char *path, void *buf);
int __wrap_fstatfs(int fd, void *buf);
int __wrap_statx(int dirfd, const char *path, int flags, unsigned int mask, void *buf);
int __wrap_cachestat(int fd, const void *range, void *cstat, unsigned int flags);
int __wrap_mlock(const void *addr, size_t len);
int __wrap_munlock(const void *addr, size_t len);
int __wrap_mlockall(int flags);
int __wrap_munlockall(void);
int __wrap_mlock2(const void *addr, size_t len, unsigned int flags);
void *__wrap_map_shadow_stack(void *addr, size_t size, unsigned int flags);
int __wrap_mincore(void *addr, size_t len, unsigned char *vec);
int __wrap_mprotect(void *addr, size_t len, int prot);
int __wrap_munmap(void *addr, size_t len);
void *__wrap_mremap(void *old_address, size_t old_size, size_t new_size, int flags, ...);
int __wrap_mseal(void *addr, size_t len, unsigned long flags);
int __wrap_renameat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath);
int __wrap_openat2(int dirfd, const char *path, const void *how, size_t size);
int __wrap_renameat2(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, unsigned int flags);
int __wrap_sendmmsg(int fd, void *messages, unsigned int vlen, unsigned int flags);
int __wrap_recvmmsg(int fd, void *messages, unsigned int vlen, unsigned int flags, struct timespec *timeout);
int __wrap_socket(int domain, int type, int protocol);
int __wrap_bind(int fd, const struct sockaddr *addr, socklen_t addrlen);
int __wrap_connect(int fd, const struct sockaddr *addr, socklen_t addrlen);
int __wrap_listen(int fd, int backlog);
int __wrap_accept4(int fd, struct sockaddr *addr, socklen_t *addrlen, int flags);
int __wrap_accept(int fd, struct sockaddr *addr, socklen_t *addrlen);
int __wrap_getsockname(int fd, struct sockaddr *addr, socklen_t *addrlen);
int __wrap_getpeername(int fd, struct sockaddr *addr, socklen_t *addrlen);
ssize_t __wrap_send(int fd, const void *buf, size_t length, int flags);
ssize_t __wrap_recv(int fd, void *buf, size_t length, int flags);
ssize_t __wrap_sendto(int fd, const void *buf, size_t length, int flags, const struct sockaddr *addr, socklen_t addrlen);
ssize_t __wrap_recvfrom(int fd, void *buf, size_t length, int flags, struct sockaddr *addr, socklen_t *addrlen);
ssize_t __wrap_sendmsg(int fd, const struct msghdr *msg, int flags);
ssize_t __wrap_recvmsg(int fd, struct msghdr *msg, int flags);
int __wrap_shutdown(int fd, int how);
int __wrap_setsockopt(int fd, int level, int option, const void *value, socklen_t value_len);
int __wrap_getsockopt(int fd, int level, int option, void *value, socklen_t *value_len);
int __wrap_socketpair(int domain, int type, int protocol, int sv[2]);
pid_t __wrap_wait4(pid_t pid, int *status_ptr, int options, struct rusage *usage);
int __wrap_waitid(idtype_t idtype, id_t id, siginfo_t *infop, int options);
int __wrap_execve(const char *path, char *const argv[], char *const envp[]);
int __wrap_execveat(int dirfd, const char *path, char *const argv[], char *const envp[], int flags);
int fexecve(int fd, char *const argv[], char *const envp[]);
static int sinomo_apply_fd_flags(int fd, int flags);
static int sinomo_signal_valid(int signum);
static void sinomo_inotify_emit_path(const char *path, uint32_t mask);
static void sinomo_fanotify_emit_path(const char *path, uint32_t inotify_mask);
static ssize_t sinomo_iov_total(const struct iovec *iov, unsigned long iovcnt);
static int sinomo_special_fd_known(int fd);
static int sinomo_special_fd_readable(int fd);
static int sinomo_special_fd_next_timeout_ms(int fd);
static void sinomo_epoll_forget(int epfd);
static void sinomo_fanotify_forget(int fd);
static int sinomo_resolve_at_path(int dirfd, const char *path, int flags, char *resolved, size_t resolved_len);
static int sinomo_pidfd_index(int fd);
static int sinomo_pidfd_readable(int fd);
static void sinomo_pidfd_forget(int fd);
static void sinomo_socket_forget(int fd);
static void sinomo_socket_duplicate_metadata(int oldfd, int newfd);
static int sinomo_memfd_get_seals(int fd);
static void sinomo_memfd_forget(int fd);
static void sinomo_pipe_forget(int fd);
static void sinomo_landlock_forget(int fd);

__attribute__((import_module("wasix_32v1"), import_name("proc_getgroups")))
extern int sinomo_proc_getgroups(unsigned int *groups, unsigned int max_groups, unsigned int *count);
__attribute__((import_module("wasix_32v1"), import_name("proc_umask")))
extern int sinomo_proc_umask(unsigned int mask, unsigned int *previous);
__attribute__((import_module("wasix_32v1"), import_name("path_chmod")))
extern int sinomo_path_chmod(const char *path, unsigned int path_len, unsigned int mode);
__attribute__((import_module("wasix_32v1"), import_name("fd_chmod")))
extern int sinomo_fd_chmod(int fd, unsigned int mode);
__attribute__((import_module("wasix_32v1"), import_name("path_chown")))
extern int sinomo_path_chown(const char *path, unsigned int path_len, unsigned int uid, unsigned int gid);
__attribute__((import_module("wasix_32v1"), import_name("fd_chown")))
extern int sinomo_fd_chown(int fd, unsigned int uid, unsigned int gid);
__attribute__((import_module("wasix_32v1"), import_name("proc_getuid")))
extern int sinomo_proc_getuid(unsigned int *uid);
__attribute__((import_module("wasix_32v1"), import_name("proc_getresuid")))
extern int sinomo_proc_getresuid(unsigned int *ruid, unsigned int *euid, unsigned int *suid);
__attribute__((import_module("wasix_32v1"), import_name("proc_getgid")))
extern int sinomo_proc_getgid(unsigned int *gid);
__attribute__((import_module("wasix_32v1"), import_name("proc_getresgid")))
extern int sinomo_proc_getresgid(unsigned int *rgid, unsigned int *egid, unsigned int *sgid);
__attribute__((import_module("wasix_32v1"), import_name("proc_setresuid")))
extern int sinomo_proc_setresuid(unsigned int ruid, unsigned int euid, unsigned int suid);
__attribute__((import_module("wasix_32v1"), import_name("proc_setresgid")))
extern int sinomo_proc_setresgid(unsigned int rgid, unsigned int egid, unsigned int sgid);
__attribute__((import_module("wasix_32v1"), import_name("proc_setgroups")))
extern int sinomo_proc_setgroups(const unsigned int *groups, unsigned int count);
__attribute__((import_module("wasix_32v1"), import_name("proc_id")))
extern int sinomo_proc_id(unsigned int *pid);
__attribute__((import_module("wasix_32v1"), import_name("proc_parent")))
extern int sinomo_proc_parent(unsigned int pid, unsigned int *ppid);
__attribute__((import_module("wasix_32v1"), import_name("proc_inherit_parent")))
extern int sinomo_proc_inherit_parent(void);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_parent_death_signal")))
extern int sinomo_proc_set_parent_death_signal(int signal);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_parent_death_signal")))
extern int sinomo_proc_get_parent_death_signal(unsigned int *signal);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_child_subreaper")))
extern int sinomo_proc_set_child_subreaper(unsigned int enabled);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_child_subreaper")))
extern int sinomo_proc_get_child_subreaper(unsigned int *enabled);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_name")))
extern int sinomo_proc_set_name(const char *name, unsigned int name_len);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_name")))
extern int sinomo_proc_get_name(char *name, unsigned int name_len);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_uts_name")))
extern int sinomo_proc_set_uts_name(unsigned int kind, const char *name, unsigned int name_len);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_uts_name")))
extern int sinomo_proc_get_uts_name(unsigned int kind, char *name, unsigned int name_len);
__attribute__((import_module("wasix_32v1"), import_name("futex_wait_mask")))
extern __wasi_errno_t sinomo_futex_wait_mask(uint32_t *futex, uint32_t expected, uint32_t mask, const __wasi_option_timestamp_t *timeout, __wasi_bool_t *woken);
__attribute__((import_module("wasix_32v1"), import_name("futex_wait_any")))
extern __wasi_errno_t sinomo_futex_wait_any(const void *waiters, uint32_t waiters_len, const __wasi_option_timestamp_t *timeout, uint32_t *woken_index);
__attribute__((import_module("wasix_32v1"), import_name("futex_wake_mask")))
extern __wasi_errno_t sinomo_futex_wake_mask(uint32_t *futex, uint32_t mask, __wasi_bool_t *woken);
__attribute__((import_module("wasix_32v1"), import_name("futex_requeue")))
extern __wasi_errno_t sinomo_futex_requeue(uint32_t *from_futex, uint32_t *to_futex, uint32_t wake_count, uint32_t requeue_count, uint32_t mask, uint32_t *affected);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_dumpable")))
extern int sinomo_proc_set_dumpable(unsigned int dumpable);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_dumpable")))
extern int sinomo_proc_get_dumpable(unsigned int *dumpable);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_no_new_privs")))
extern int sinomo_proc_set_no_new_privs(unsigned int enabled);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_no_new_privs")))
extern int sinomo_proc_get_no_new_privs(unsigned int *enabled);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_seccomp_mode")))
extern int sinomo_proc_set_seccomp_mode(unsigned int mode);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_seccomp_mode")))
extern int sinomo_proc_get_seccomp_mode(unsigned int *mode);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_timer_slack")))
extern int sinomo_proc_set_timer_slack(unsigned long long nanos);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_timer_slack")))
extern int sinomo_proc_get_timer_slack(unsigned long long *nanos);
__attribute__((import_module("wasix_32v1"), import_name("proc_personality")))
extern int sinomo_proc_personality(unsigned long long persona, unsigned long long *previous);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_clear_child_tid")))
extern int sinomo_proc_set_clear_child_tid(unsigned long long address);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_clear_child_tid")))
extern int sinomo_proc_get_clear_child_tid(unsigned long long *address);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_robust_list")))
extern int sinomo_proc_set_robust_list(unsigned long long head, unsigned long long len);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_robust_list")))
extern int sinomo_proc_get_robust_list(int pid, unsigned long long *head, unsigned long long *len);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_capabilities")))
extern int sinomo_proc_get_capabilities(unsigned long long *effective, unsigned long long *permitted, unsigned long long *inheritable, unsigned long long *bounding);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_capabilities")))
extern int sinomo_proc_set_capabilities(unsigned long long effective, unsigned long long permitted, unsigned long long inheritable);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_capability_bounding")))
extern int sinomo_proc_get_capability_bounding(unsigned int capability, unsigned int *enabled);
__attribute__((import_module("wasix_32v1"), import_name("proc_drop_capability_bounding")))
extern int sinomo_proc_drop_capability_bounding(unsigned int capability);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_process_control")))
extern int sinomo_proc_set_process_control(unsigned int key, unsigned long long value);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_process_control")))
extern int sinomo_proc_get_process_control(unsigned int key, unsigned long long *value);
__attribute__((import_module("wasix_32v1"), import_name("proc_unshare_namespaces")))
extern int sinomo_proc_unshare_namespaces(unsigned long long flags);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_arch_base")))
extern int sinomo_proc_set_arch_base(unsigned int kind, unsigned long long address);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_arch_base")))
extern int sinomo_proc_get_arch_base(unsigned int kind, unsigned long long *address);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_rseq")))
extern int sinomo_proc_set_rseq(unsigned long long address, unsigned int len, unsigned int signature);
__attribute__((import_module("wasix_32v1"), import_name("proc_clear_rseq")))
extern int sinomo_proc_clear_rseq(unsigned long long address);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_mempolicy")))
extern int sinomo_proc_set_mempolicy(int mode, unsigned long long nodes);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_mempolicy")))
extern int sinomo_proc_get_mempolicy(int *mode, unsigned long long *nodes);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_mempolicy_home_node")))
extern int sinomo_proc_set_mempolicy_home_node(unsigned long long home_node);
__attribute__((import_module("wasix_32v1"), import_name("proc_set_ioprio")))
extern int sinomo_proc_set_ioprio(int ioprio);
__attribute__((import_module("wasix_32v1"), import_name("proc_get_ioprio")))
extern int sinomo_proc_get_ioprio(int *ioprio);
__attribute__((import_module("wasix_32v1"), import_name("thread_id")))
extern int sinomo_thread_id(unsigned int *tid);
__attribute__((import_module("wasix_32v1"), import_name("proc_getpgid")))
extern int sinomo_proc_getpgid(int pid, int *pgid);
__attribute__((import_module("wasix_32v1"), import_name("proc_getpgrp")))
extern int sinomo_proc_getpgrp(int *pgid);
__attribute__((import_module("wasix_32v1"), import_name("proc_getsid")))
extern int sinomo_proc_getsid(int pid, int *sid);
__attribute__((import_module("wasix_32v1"), import_name("proc_setpgid")))
extern int sinomo_proc_setpgid(int pid, int pgid);
__attribute__((import_module("wasix_32v1"), import_name("proc_setsid")))
extern int sinomo_proc_setsid(int *sid);
__attribute__((import_module("wasix_32v1"), import_name("proc_tcgetpgrp")))
extern int sinomo_proc_tcgetpgrp(int fd, int *pgid);
__attribute__((import_module("wasix_32v1"), import_name("proc_tcgetsid")))
extern int sinomo_proc_tcgetsid(int fd, int *sid);
__attribute__((import_module("wasix_32v1"), import_name("proc_tcsetpgrp")))
extern int sinomo_proc_tcsetpgrp(int fd, int pgid);
__attribute__((import_module("wasix_32v1"), import_name("proc_tty_get_tostop")))
extern int sinomo_proc_tty_get_tostop(int *tostop);
__attribute__((import_module("wasix_32v1"), import_name("proc_tty_set_tostop")))
extern int sinomo_proc_tty_set_tostop(int tostop);
__attribute__((import_module("wasix_32v1"), import_name("proc_wait_status")))
extern int sinomo_proc_wait_status(__wasi_option_pid_t *pid, __wasi_join_flags_t flags, __wasi_join_status_t *status);
__attribute__((import_module("wasix_32v1"), import_name("proc_wait_status_select")))
extern int sinomo_proc_wait_status_select(__wasi_option_pid_t *pid, __wasi_join_flags_t flags, uint32_t status_kinds, __wasi_join_status_t *status);
__attribute__((import_module("wasix_32v1"), import_name("proc_child_continued_status")))
extern int sinomo_proc_child_continued_status(int pid, int consume, int *continued_pid);
__attribute__((import_module("wasix_32v1"), import_name("proc_child_continued_status_wait")))
extern int sinomo_proc_child_continued_status_wait(int pid, int consume, int nonblocking, int *continued_pid);
__attribute__((import_module("wasix_32v1"), import_name("proc_getpriority")))
extern int sinomo_proc_getpriority(int which, int who, int *priority);
__attribute__((import_module("wasix_32v1"), import_name("proc_setpriority")))
extern int sinomo_proc_setpriority(int which, int who, int priority);
__attribute__((import_module("wasix_32v1"), import_name("proc_getrlimit")))
extern int sinomo_proc_getrlimit(int pid, int resource, unsigned long long *current, unsigned long long *maximum);
__attribute__((import_module("wasix_32v1"), import_name("proc_setrlimit")))
extern int sinomo_proc_setrlimit(int pid, int resource, unsigned long long current, unsigned long long maximum);
__attribute__((import_module("wasix_32v1"), import_name("proc_getrusage")))
extern int sinomo_proc_getrusage(int who, unsigned long long *user_usec, unsigned long long *system_usec, unsigned long long *maxrss_kib);
__attribute__((import_module("wasix_32v1"), import_name("proc_times")))
extern int sinomo_proc_times(unsigned long long *elapsed_ticks, unsigned long long *user_ticks, unsigned long long *system_ticks, unsigned long long *child_user_ticks, unsigned long long *child_system_ticks);
__attribute__((import_module("wasix_32v1"), import_name("proc_getscheduler")))
extern int sinomo_proc_getscheduler(int pid, int *policy, int *priority);
__attribute__((import_module("wasix_32v1"), import_name("proc_setscheduler")))
extern int sinomo_proc_setscheduler(int pid, int policy, int priority);
__attribute__((import_module("wasix_32v1"), import_name("proc_getaffinity")))
extern int sinomo_proc_getaffinity(int pid, unsigned long long *mask_low, unsigned long long *mask_high);
__attribute__((import_module("wasix_32v1"), import_name("proc_setaffinity")))
extern int sinomo_proc_setaffinity(int pid, unsigned long long mask_low, unsigned long long mask_high);
__attribute__((import_module("wasix_32v1"), import_name("proc_getcpu")))
extern int sinomo_proc_getcpu(unsigned int *cpu);
__attribute__((import_module("wasix_32v1"), import_name("fchdir")))
extern int sinomo_fchdir(int fd);

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
#ifndef F_DUPFD_CLOEXEC
#define F_DUPFD_CLOEXEC 1030
#endif
#ifndef F_SETPIPE_SZ
#define F_SETPIPE_SZ 1031
#endif
#ifndef F_GETPIPE_SZ
#define F_GETPIPE_SZ 1032
#endif
#ifndef F_ADD_SEALS
#define F_ADD_SEALS 1033
#endif
#ifndef F_GET_SEALS
#define F_GET_SEALS 1034
#endif
#ifndef F_OFD_GETLK
#define F_OFD_GETLK 36
#endif
#ifndef F_OFD_SETLK
#define F_OFD_SETLK 37
#endif
#ifndef F_OFD_SETLKW
#define F_OFD_SETLKW 38
#endif
#ifndef F_SEAL_SEAL
#define F_SEAL_SEAL 0x0001
#endif
#ifndef F_SEAL_SHRINK
#define F_SEAL_SHRINK 0x0002
#endif
#ifndef F_SEAL_GROW
#define F_SEAL_GROW 0x0004
#endif
#ifndef F_SEAL_WRITE
#define F_SEAL_WRITE 0x0008
#endif
#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
#endif
#ifndef SS_ONSTACK
#define SS_ONSTACK 1
#endif
#ifndef SS_DISABLE
#define SS_DISABLE 2
#endif
#ifndef MINSIGSTKSZ
#define MINSIGSTKSZ 2048
#endif
#ifndef TCGETS
#define TCGETS 0x5401
#endif
#ifndef TCSETS
#define TCSETS 0x5402
#endif
#ifndef TCSETSW
#define TCSETSW 0x5403
#endif
#ifndef TCSETSF
#define TCSETSF 0x5404
#endif
#ifndef TIOCGPGRP
#define TIOCGPGRP 0x540f
#endif
#ifndef TIOCSPGRP
#define TIOCSPGRP 0x5410
#endif
#ifndef TIOCGPTN
#define TIOCGPTN 0x80045430
#endif
#ifndef TIOCSPTLCK
#define TIOCSPTLCK 0x40045431
#endif
#ifndef TIOCGPTPEER
#define TIOCGPTPEER 0x5441
#endif
#ifndef F_ULOCK
#define F_ULOCK 0
#endif
#ifndef F_LOCK
#define F_LOCK 1
#endif
#ifndef F_TLOCK
#define F_TLOCK 2
#endif
#ifndef F_TEST
#define F_TEST 3
#endif
#ifndef LOCK_SH
#define LOCK_SH 1
#endif
#ifndef LOCK_EX
#define LOCK_EX 2
#endif
#ifndef LOCK_NB
#define LOCK_NB 4
#endif
#ifndef LOCK_UN
#define LOCK_UN 8
#endif
#ifndef PRIO_MIN
#define PRIO_MIN (-20)
#endif
#ifndef PRIO_MAX
#define PRIO_MAX 20
#endif
#ifndef PRIO_PROCESS
#define PRIO_PROCESS 0
#endif
#ifndef PRIO_PGRP
#define PRIO_PGRP 1
#endif
#ifndef PRIO_USER
#define PRIO_USER 2
#endif
#ifndef RUSAGE_SELF
#define RUSAGE_SELF 0
#endif
#ifndef RUSAGE_CHILDREN
#define RUSAGE_CHILDREN (-1)
#endif
#ifndef RUSAGE_THREAD
#define RUSAGE_THREAD 1
#endif
#ifndef SSIZE_MAX
#define SSIZE_MAX ((ssize_t)(SIZE_MAX / 2))
#endif
#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif
#ifndef MFD_ALLOW_SEALING
#define MFD_ALLOW_SEALING 0x0002U
#endif
#ifndef CLOSE_RANGE_UNSHARE
#define CLOSE_RANGE_UNSHARE 0x2U
#endif
#ifndef CLOSE_RANGE_CLOEXEC
#define CLOSE_RANGE_CLOEXEC 0x4U
#endif
#ifndef AT_EMPTY_PATH
#define AT_EMPTY_PATH 0x1000
#endif
#ifndef AT_SYMLINK_FOLLOW
#define AT_SYMLINK_FOLLOW 0x400
#endif
#ifndef RESOLVE_NO_XDEV
#define RESOLVE_NO_XDEV 0x01
#endif
#ifndef RESOLVE_NO_MAGICLINKS
#define RESOLVE_NO_MAGICLINKS 0x02
#endif
#ifndef RESOLVE_NO_SYMLINKS
#define RESOLVE_NO_SYMLINKS 0x04
#endif
#ifndef RESOLVE_BENEATH
#define RESOLVE_BENEATH 0x08
#endif
#ifndef RESOLVE_IN_ROOT
#define RESOLVE_IN_ROOT 0x10
#endif
#ifndef RESOLVE_CACHED
#define RESOLVE_CACHED 0x20
#endif
#ifndef ESTALE
#define ESTALE 116
#endif
#ifndef L_tmpnam
#define L_tmpnam 20
#endif
#ifndef POSIX_MADV_NORMAL
#define POSIX_MADV_NORMAL 0
#endif
#ifndef POSIX_MADV_RANDOM
#define POSIX_MADV_RANDOM 1
#endif
#ifndef POSIX_MADV_SEQUENTIAL
#define POSIX_MADV_SEQUENTIAL 2
#endif
#ifndef POSIX_MADV_WILLNEED
#define POSIX_MADV_WILLNEED 3
#endif
#ifndef POSIX_MADV_DONTNEED
#define POSIX_MADV_DONTNEED 4
#endif
#ifndef MADV_NORMAL
#define MADV_NORMAL POSIX_MADV_NORMAL
#endif
#ifndef MADV_RANDOM
#define MADV_RANDOM POSIX_MADV_RANDOM
#endif
#ifndef MADV_SEQUENTIAL
#define MADV_SEQUENTIAL POSIX_MADV_SEQUENTIAL
#endif
#ifndef MADV_WILLNEED
#define MADV_WILLNEED POSIX_MADV_WILLNEED
#endif
#ifndef MADV_DONTNEED
#define MADV_DONTNEED POSIX_MADV_DONTNEED
#endif
#ifndef MADV_FREE
#define MADV_FREE 8
#endif
#ifndef MADV_REMOVE
#define MADV_REMOVE 9
#endif
#ifndef MADV_DONTFORK
#define MADV_DONTFORK 10
#endif
#ifndef MADV_DOFORK
#define MADV_DOFORK 11
#endif
#ifndef MADV_MERGEABLE
#define MADV_MERGEABLE 12
#endif
#ifndef MADV_UNMERGEABLE
#define MADV_UNMERGEABLE 13
#endif
#ifndef MADV_HUGEPAGE
#define MADV_HUGEPAGE 14
#endif
#ifndef MADV_NOHUGEPAGE
#define MADV_NOHUGEPAGE 15
#endif
#ifndef MADV_DONTDUMP
#define MADV_DONTDUMP 16
#endif
#ifndef MADV_DODUMP
#define MADV_DODUMP 17
#endif
#ifndef MADV_WIPEONFORK
#define MADV_WIPEONFORK 18
#endif
#ifndef MADV_KEEPONFORK
#define MADV_KEEPONFORK 19
#endif
#ifndef MADV_COLD
#define MADV_COLD 20
#endif
#ifndef MADV_PAGEOUT
#define MADV_PAGEOUT 21
#endif
#ifndef MADV_POPULATE_READ
#define MADV_POPULATE_READ 22
#endif
#ifndef MADV_POPULATE_WRITE
#define MADV_POPULATE_WRITE 23
#endif
#ifndef MADV_COLLAPSE
#define MADV_COLLAPSE 25
#endif
#ifndef FUTEX_WAIT
#define FUTEX_WAIT 0
#endif
#ifndef FUTEX_WAKE
#define FUTEX_WAKE 1
#endif
#ifndef FUTEX_FD
#define FUTEX_FD 2
#endif
#ifndef FUTEX_REQUEUE
#define FUTEX_REQUEUE 3
#endif
#ifndef FUTEX_CMP_REQUEUE
#define FUTEX_CMP_REQUEUE 4
#endif
#ifndef FUTEX_WAKE_OP
#define FUTEX_WAKE_OP 5
#endif
#ifndef FUTEX_LOCK_PI
#define FUTEX_LOCK_PI 6
#endif
#ifndef FUTEX_UNLOCK_PI
#define FUTEX_UNLOCK_PI 7
#endif
#ifndef FUTEX_TRYLOCK_PI
#define FUTEX_TRYLOCK_PI 8
#endif
#ifndef FUTEX_WAIT_BITSET
#define FUTEX_WAIT_BITSET 9
#endif
#ifndef FUTEX_WAKE_BITSET
#define FUTEX_WAKE_BITSET 10
#endif
#ifndef FUTEX_WAIT_REQUEUE_PI
#define FUTEX_WAIT_REQUEUE_PI 11
#endif
#ifndef FUTEX_CMP_REQUEUE_PI
#define FUTEX_CMP_REQUEUE_PI 12
#endif
#ifndef FUTEX_LOCK_PI2
#define FUTEX_LOCK_PI2 13
#endif
#ifndef FUTEX_PRIVATE_FLAG
#define FUTEX_PRIVATE_FLAG 128
#endif
#ifndef FUTEX_CLOCK_REALTIME
#define FUTEX_CLOCK_REALTIME 256
#endif
#ifndef FUTEX_BITSET_MATCH_ANY
#define FUTEX_BITSET_MATCH_ANY 0xffffffffU
#endif
#ifndef FUTEX2_SIZE_U32
#define FUTEX2_SIZE_U32 0x02U
#endif
#ifndef FUTEX2_PRIVATE
#define FUTEX2_PRIVATE FUTEX_PRIVATE_FLAG
#endif
#ifndef FUTEX2_SIZE_MASK
#define FUTEX2_SIZE_MASK 0x03U
#endif
#ifndef FUTEX_OP_SET
#define FUTEX_OP_SET 0
#endif
#ifndef FUTEX_OP_ADD
#define FUTEX_OP_ADD 1
#endif
#ifndef FUTEX_OP_OR
#define FUTEX_OP_OR 2
#endif
#ifndef FUTEX_OP_ANDN
#define FUTEX_OP_ANDN 3
#endif
#ifndef FUTEX_OP_XOR
#define FUTEX_OP_XOR 4
#endif
#ifndef FUTEX_OP_OPARG_SHIFT
#define FUTEX_OP_OPARG_SHIFT 8
#endif
#ifndef FUTEX_OP_CMP_EQ
#define FUTEX_OP_CMP_EQ 0
#endif
#ifndef FUTEX_OP_CMP_NE
#define FUTEX_OP_CMP_NE 1
#endif
#ifndef FUTEX_OP_CMP_LT
#define FUTEX_OP_CMP_LT 2
#endif
#ifndef FUTEX_OP_CMP_LE
#define FUTEX_OP_CMP_LE 3
#endif
#ifndef FUTEX_OP_CMP_GT
#define FUTEX_OP_CMP_GT 4
#endif
#ifndef FUTEX_OP_CMP_GE
#define FUTEX_OP_CMP_GE 5
#endif
#ifndef FUTEX_CMD_MASK
#define FUTEX_CMD_MASK ~(FUTEX_PRIVATE_FLAG | FUTEX_CLOCK_REALTIME)
#endif
#ifndef FUTEX_WAITERS
#define FUTEX_WAITERS 0x80000000U
#endif
#ifndef FUTEX_OWNER_DIED
#define FUTEX_OWNER_DIED 0x40000000U
#endif
#ifndef FUTEX_TID_MASK
#define FUTEX_TID_MASK 0x3fffffffU
#endif
#ifndef FUTEX_32
#define FUTEX_32 2U
#endif
#ifndef LSM_ID_LANDLOCK
#define LSM_ID_LANDLOCK 110
#endif
#ifndef LSM_ATTR_CURRENT
#define LSM_ATTR_CURRENT 100
#endif
#ifndef LSM_ATTR_EXEC
#define LSM_ATTR_EXEC 101
#endif
#ifndef LSM_ATTR_FSCREATE
#define LSM_ATTR_FSCREATE 102
#endif
#ifndef LSM_ATTR_KEYCREATE
#define LSM_ATTR_KEYCREATE 103
#endif
#ifndef LSM_ATTR_PREV
#define LSM_ATTR_PREV 104
#endif
#ifndef LSM_ATTR_SOCKCREATE
#define LSM_ATTR_SOCKCREATE 105
#endif
#ifndef LSM_FLAG_SINGLE
#define LSM_FLAG_SINGLE 0x0001U
#endif
#ifndef MOUNT_ATTR_RDONLY
#define MOUNT_ATTR_RDONLY 0x00000001ULL
#endif
#ifndef MOUNT_ATTR_NOSUID
#define MOUNT_ATTR_NOSUID 0x00000002ULL
#endif
#ifndef MOUNT_ATTR_NODEV
#define MOUNT_ATTR_NODEV 0x00000004ULL
#endif
#ifndef MOUNT_ATTR_NOEXEC
#define MOUNT_ATTR_NOEXEC 0x00000008ULL
#endif
#ifndef MS_PRIVATE
#define MS_PRIVATE (1 << 18)
#endif
#ifndef STATMOUNT_SB_BASIC
#define STATMOUNT_SB_BASIC 0x00000001ULL
#endif
#ifndef STATMOUNT_MNT_BASIC
#define STATMOUNT_MNT_BASIC 0x00000002ULL
#endif
#ifndef STATMOUNT_PROPAGATE_FROM
#define STATMOUNT_PROPAGATE_FROM 0x00000004ULL
#endif
#ifndef STATMOUNT_MNT_ROOT
#define STATMOUNT_MNT_ROOT 0x00000008ULL
#endif
#ifndef STATMOUNT_MNT_POINT
#define STATMOUNT_MNT_POINT 0x00000010ULL
#endif
#ifndef STATMOUNT_FS_TYPE
#define STATMOUNT_FS_TYPE 0x00000020ULL
#endif
#ifndef STATMOUNT_MNT_NS_ID
#define STATMOUNT_MNT_NS_ID 0x00000040ULL
#endif
#ifndef STATMOUNT_MNT_OPTS
#define STATMOUNT_MNT_OPTS 0x00000080ULL
#endif
#ifndef STATMOUNT_FS_SUBTYPE
#define STATMOUNT_FS_SUBTYPE 0x00000100ULL
#endif
#ifndef STATMOUNT_SB_SOURCE
#define STATMOUNT_SB_SOURCE 0x00000200ULL
#endif
#ifndef STATMOUNT_OPT_ARRAY
#define STATMOUNT_OPT_ARRAY 0x00000400ULL
#endif
#ifndef STATMOUNT_OPT_SEC_ARRAY
#define STATMOUNT_OPT_SEC_ARRAY 0x00000800ULL
#endif
#ifndef STATMOUNT_SUPPORTED_MASK
#define STATMOUNT_SUPPORTED_MASK 0x00001000ULL
#endif
#ifndef STATMOUNT_MNT_UIDMAP
#define STATMOUNT_MNT_UIDMAP 0x00002000ULL
#endif
#ifndef STATMOUNT_MNT_GIDMAP
#define STATMOUNT_MNT_GIDMAP 0x00004000ULL
#endif
#ifndef LSMT_ROOT
#define LSMT_ROOT 0xffffffffffffffffULL
#endif
#ifndef LISTMOUNT_REVERSE
#define LISTMOUNT_REVERSE (1U << 0)
#endif
#ifndef MNT_ID_REQ_SIZE_VER0
#define MNT_ID_REQ_SIZE_VER0 24U
#endif
#ifndef MNT_ID_REQ_SIZE_VER1
#define MNT_ID_REQ_SIZE_VER1 32U
#endif
#ifndef TFD_CLOEXEC
#define TFD_CLOEXEC O_CLOEXEC
#endif
#ifndef TFD_NONBLOCK
#define TFD_NONBLOCK O_NONBLOCK
#endif
#ifndef TFD_TIMER_ABSTIME
#define TFD_TIMER_ABSTIME 1
#endif
#ifndef TFD_TIMER_CANCEL_ON_SET
#define TFD_TIMER_CANCEL_ON_SET 2
#endif
#ifndef SFD_CLOEXEC
#define SFD_CLOEXEC O_CLOEXEC
#endif
#ifndef SFD_NONBLOCK
#define SFD_NONBLOCK O_NONBLOCK
#endif
#ifndef IN_CLOEXEC
#define IN_CLOEXEC O_CLOEXEC
#endif
#ifndef IN_NONBLOCK
#define IN_NONBLOCK O_NONBLOCK
#endif
#ifndef IN_ACCESS
#define IN_ACCESS 0x00000001U
#endif
#ifndef IN_MODIFY
#define IN_MODIFY 0x00000002U
#endif
#ifndef IN_ATTRIB
#define IN_ATTRIB 0x00000004U
#endif
#ifndef IN_CLOSE_WRITE
#define IN_CLOSE_WRITE 0x00000008U
#endif
#ifndef IN_CLOSE_NOWRITE
#define IN_CLOSE_NOWRITE 0x00000010U
#endif
#ifndef IN_OPEN
#define IN_OPEN 0x00000020U
#endif
#ifndef IN_MOVED_FROM
#define IN_MOVED_FROM 0x00000040U
#endif
#ifndef IN_MOVED_TO
#define IN_MOVED_TO 0x00000080U
#endif
#ifndef IN_CREATE
#define IN_CREATE 0x00000100U
#endif
#ifndef IN_DELETE
#define IN_DELETE 0x00000200U
#endif
#ifndef IN_DELETE_SELF
#define IN_DELETE_SELF 0x00000400U
#endif
#ifndef IN_MOVE_SELF
#define IN_MOVE_SELF 0x00000800U
#endif
#ifndef IN_UNMOUNT
#define IN_UNMOUNT 0x00002000U
#endif
#ifndef IN_Q_OVERFLOW
#define IN_Q_OVERFLOW 0x00004000U
#endif
#ifndef IN_IGNORED
#define IN_IGNORED 0x00008000U
#endif
#ifndef FAN_ACCESS
#define FAN_ACCESS 0x00000001ULL
#endif
#ifndef FAN_MODIFY
#define FAN_MODIFY 0x00000002ULL
#endif
#ifndef FAN_CLOSE_WRITE
#define FAN_CLOSE_WRITE 0x00000008ULL
#endif
#ifndef FAN_CLOSE_NOWRITE
#define FAN_CLOSE_NOWRITE 0x00000010ULL
#endif
#ifndef FAN_OPEN
#define FAN_OPEN 0x00000020ULL
#endif
#ifndef FAN_MOVED_FROM
#define FAN_MOVED_FROM 0x00000040ULL
#endif
#ifndef FAN_MOVED_TO
#define FAN_MOVED_TO 0x00000080ULL
#endif
#ifndef FAN_CREATE
#define FAN_CREATE 0x00000100ULL
#endif
#ifndef FAN_DELETE
#define FAN_DELETE 0x00000200ULL
#endif
#ifndef FAN_DELETE_SELF
#define FAN_DELETE_SELF 0x00000400ULL
#endif
#ifndef FAN_MOVE_SELF
#define FAN_MOVE_SELF 0x00000800ULL
#endif
#ifndef FAN_OPEN_EXEC
#define FAN_OPEN_EXEC 0x00001000ULL
#endif
#ifndef FAN_Q_OVERFLOW
#define FAN_Q_OVERFLOW 0x00004000ULL
#endif
#ifndef FAN_EVENT_ON_CHILD
#define FAN_EVENT_ON_CHILD 0x08000000ULL
#endif
#ifndef FAN_ONDIR
#define FAN_ONDIR 0x40000000ULL
#endif
#ifndef FAN_CLOEXEC
#define FAN_CLOEXEC 0x00000001U
#endif
#ifndef FAN_NONBLOCK
#define FAN_NONBLOCK 0x00000002U
#endif
#ifndef FAN_CLASS_CONTENT
#define FAN_CLASS_CONTENT 0x00000004U
#endif
#ifndef FAN_CLASS_PRE_CONTENT
#define FAN_CLASS_PRE_CONTENT 0x00000008U
#endif
#ifndef FAN_UNLIMITED_QUEUE
#define FAN_UNLIMITED_QUEUE 0x00000010U
#endif
#ifndef FAN_UNLIMITED_MARKS
#define FAN_UNLIMITED_MARKS 0x00000020U
#endif
#ifndef FAN_MARK_ADD
#define FAN_MARK_ADD 0x00000001U
#endif
#ifndef FAN_MARK_REMOVE
#define FAN_MARK_REMOVE 0x00000002U
#endif
#ifndef FAN_MARK_DONT_FOLLOW
#define FAN_MARK_DONT_FOLLOW 0x00000004U
#endif
#ifndef FAN_MARK_ONLYDIR
#define FAN_MARK_ONLYDIR 0x00000008U
#endif
#ifndef FAN_MARK_MOUNT
#define FAN_MARK_MOUNT 0x00000010U
#endif
#ifndef FAN_MARK_IGNORED_MASK
#define FAN_MARK_IGNORED_MASK 0x00000020U
#endif
#ifndef FAN_MARK_IGNORED_SURV_MODIFY
#define FAN_MARK_IGNORED_SURV_MODIFY 0x00000040U
#endif
#ifndef FAN_MARK_FLUSH
#define FAN_MARK_FLUSH 0x00000080U
#endif
#ifndef FAN_MARK_FILESYSTEM
#define FAN_MARK_FILESYSTEM 0x00000100U
#endif
#ifndef FAN_NOFD
#define FAN_NOFD -1
#endif
#ifndef FANOTIFY_METADATA_VERSION
#define FANOTIFY_METADATA_VERSION 3
#endif
#ifndef IN_ONLYDIR
#define IN_ONLYDIR 0x01000000U
#endif
#ifndef IN_DONT_FOLLOW
#define IN_DONT_FOLLOW 0x02000000U
#endif
#ifndef IN_EXCL_UNLINK
#define IN_EXCL_UNLINK 0x04000000U
#endif
#ifndef IN_MASK_ADD
#define IN_MASK_ADD 0x20000000U
#endif
#ifndef IN_ISDIR
#define IN_ISDIR 0x40000000U
#endif
#ifndef IN_ONESHOT
#define IN_ONESHOT 0x80000000U
#endif
#ifndef STATX_TYPE
#define STATX_TYPE 0x00000001U
#endif
#ifndef STATX_MODE
#define STATX_MODE 0x00000002U
#endif
#ifndef STATX_NLINK
#define STATX_NLINK 0x00000004U
#endif
#ifndef STATX_UID
#define STATX_UID 0x00000008U
#endif
#ifndef STATX_GID
#define STATX_GID 0x00000010U
#endif
#ifndef STATX_ATIME
#define STATX_ATIME 0x00000020U
#endif
#ifndef STATX_MTIME
#define STATX_MTIME 0x00000040U
#endif
#ifndef STATX_CTIME
#define STATX_CTIME 0x00000080U
#endif
#ifndef STATX_INO
#define STATX_INO 0x00000100U
#endif
#ifndef STATX_SIZE
#define STATX_SIZE 0x00000200U
#endif
#ifndef STATX_BLOCKS
#define STATX_BLOCKS 0x00000400U
#endif
#ifndef STATX_BASIC_STATS
#define STATX_BASIC_STATS 0x000007ffU
#endif
#ifndef STATX_ATTR_COMPRESSED
#define STATX_ATTR_COMPRESSED 0x00000004ULL
#endif
#ifndef STATX_ATTR_IMMUTABLE
#define STATX_ATTR_IMMUTABLE 0x00000010ULL
#endif
#ifndef STATX_ATTR_APPEND
#define STATX_ATTR_APPEND 0x00000020ULL
#endif
#ifndef STATX_ATTR_NODUMP
#define STATX_ATTR_NODUMP 0x00000040ULL
#endif
#ifndef STATX_ATTR_ENCRYPTED
#define STATX_ATTR_ENCRYPTED 0x00000800ULL
#endif
#ifndef STATX_ATTR_AUTOMOUNT
#define STATX_ATTR_AUTOMOUNT 0x00001000ULL
#endif
#ifndef STATX_ATTR_MOUNT_ROOT
#define STATX_ATTR_MOUNT_ROOT 0x00002000ULL
#endif
#ifndef STATX_ATTR_VERITY
#define STATX_ATTR_VERITY 0x00100000ULL
#endif
#ifndef STATX_ATTR_DAX
#define STATX_ATTR_DAX 0x00200000ULL
#endif
#ifndef STATX_ATTR_WRITE_ATOMIC
#define STATX_ATTR_WRITE_ATOMIC 0x00400000ULL
#endif
#ifndef RENAME_NOREPLACE
#define RENAME_NOREPLACE (1U << 0)
#endif
#ifndef RENAME_EXCHANGE
#define RENAME_EXCHANGE (1U << 1)
#endif
#ifndef RENAME_WHITEOUT
#define RENAME_WHITEOUT (1U << 2)
#endif

static int sinomo_mkdir_if_missing(const char *path) {
    if (mkdir(path, 0700) == 0 || errno == EEXIST) {
        return 0;
    }
    return -1;
}

static unsigned int sinomo_uid(uid_t value) {
    return value == (uid_t)-1 ? UINT32_MAX : (unsigned int)value;
}

static unsigned int sinomo_gid(gid_t value) {
    return value == (gid_t)-1 ? UINT32_MAX : (unsigned int)value;
}

static unsigned long long sinomo_env_u64(const char *name, unsigned long long fallback) {
    const char *value = getenv(name);
    if (value == NULL || value[0] == 0) {
        return fallback;
    }
    char *end = NULL;
    errno = 0;
    unsigned long long parsed = strtoull(value, &end, 10);
    if (errno != 0 || end == value || *end != 0) {
        errno = 0;
        return fallback;
    }
    return parsed;
}

static void sinomo_copy_uts_field(char *dest, size_t dest_len, const char *src) {
    if (dest_len == 0) {
        return;
    }
    snprintf(dest, dest_len, "%s", src);
}

#define SINOMO_UTS_HOSTNAME 1U
#define SINOMO_UTS_DOMAINNAME 2U

static int sinomo_get_uts_string(unsigned int kind, char *dest, size_t dest_len) {
    if (dest == NULL || dest_len == 0) {
        errno = EFAULT;
        return -1;
    }
    int rc = sinomo_proc_get_uts_name(kind, dest, (unsigned int)dest_len);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    dest[dest_len - 1] = 0;
    return 0;
}

static int sinomo_set_uts_string(unsigned int kind, const char *src, size_t len) {
    if (src == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (len >= 65) {
        errno = EINVAL;
        return -1;
    }
    int rc = sinomo_proc_set_uts_name(kind, src, (unsigned int)len);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    return 0;
}

int __wrap_uname(struct utsname *name) {
    if (name == NULL) {
        errno = EFAULT;
        return -1;
    }
    char hostname[65];
    char domainname[65];
    if (sinomo_get_uts_string(SINOMO_UTS_HOSTNAME, hostname, sizeof(hostname)) != 0 ||
        sinomo_get_uts_string(SINOMO_UTS_DOMAINNAME, domainname, sizeof(domainname)) != 0) {
        return -1;
    }
    memset(name, 0, sizeof(*name));
    sinomo_copy_uts_field(name->sysname, sizeof(name->sysname), "Linux");
    sinomo_copy_uts_field(name->nodename, sizeof(name->nodename), hostname);
    sinomo_copy_uts_field(name->release, sizeof(name->release), "wasix");
    sinomo_copy_uts_field(name->version, sizeof(name->version), "Sinomo WASIX sandbox");
    sinomo_copy_uts_field(name->machine, sizeof(name->machine), "wasm32");
#ifdef _GNU_SOURCE
    sinomo_copy_uts_field(name->domainname, sizeof(name->domainname), domainname);
#else
    sinomo_copy_uts_field(name->__domainname, sizeof(name->__domainname), domainname);
#endif
    return 0;
}

int __wrap_gethostname(char *name, size_t len) {
    if (name == NULL) {
        errno = EFAULT;
        return -1;
    }
    char hostname[65];
    if (sinomo_get_uts_string(SINOMO_UTS_HOSTNAME, hostname, sizeof(hostname)) != 0) {
        return -1;
    }
    size_t hostname_len = strlen(hostname);
    if (len == 0 || len <= hostname_len) {
        errno = ENAMETOOLONG;
        return -1;
    }
    memcpy(name, hostname, hostname_len + 1);
    return 0;
}

int __wrap_sethostname(const char *name, size_t len) {
    return sinomo_set_uts_string(SINOMO_UTS_HOSTNAME, name, len);
}

int __wrap_getdomainname(char *name, size_t len) {
    if (name == NULL) {
        errno = EFAULT;
        return -1;
    }
    char domainname[65];
    if (sinomo_get_uts_string(SINOMO_UTS_DOMAINNAME, domainname, sizeof(domainname)) != 0) {
        return -1;
    }
    size_t domain_len = strlen(domainname);
    if (len == 0 || len <= domain_len) {
        errno = ENAMETOOLONG;
        return -1;
    }
    memcpy(name, domainname, domain_len + 1);
    return 0;
}

int __wrap_setdomainname(const char *name, size_t len) {
    return sinomo_set_uts_string(SINOMO_UTS_DOMAINNAME, name, len);
}

#define SINOMO_PTY_SLOTS 64
#define SINOMO_PTY_FDS 256
struct sinomo_pty_slot {
    int active;
    int id;
    int unlocked;
    struct termios term;
    struct winsize winsize;
};

struct sinomo_pty_fd {
    int fd;
    int id;
    int master;
};

static struct sinomo_pty_fd *sinomo_pty_fd_entry(int fd);
static struct sinomo_pty_slot *sinomo_pty_slot_by_id(int id);
static struct sinomo_pty_slot *sinomo_pty_slot_for_fd(int fd);

int __wrap_isatty(int fd) {
    switch (fd) {
        case STDIN_FILENO:
        case STDOUT_FILENO:
        case STDERR_FILENO:
            return 1;
        default:
            if (sinomo_pty_fd_entry(fd) != NULL) {
                return 1;
            }
            errno = ENOTTY;
            return 0;
    }
}

int __wrap_ttyname_r(int fd, char *buf, size_t buflen) {
    if (buf == NULL) {
        return EINVAL;
    }
    struct sinomo_pty_fd *pty = sinomo_pty_fd_entry(fd);
    char tty[64];
    if (pty != NULL) {
        if (pty->master) {
            snprintf(tty, sizeof(tty), "/dev/ptmx");
        } else {
            snprintf(tty, sizeof(tty), "/dev/pts/%d", pty->id);
        }
    } else if (__wrap_isatty(fd)) {
        snprintf(tty, sizeof(tty), "/dev/tty");
    } else {
        return ENOTTY;
    }
    size_t tty_len = strlen(tty) + 1U;
    if (buflen < tty_len) {
        return ERANGE;
    }
    memcpy(buf, tty, tty_len);
    return 0;
}

char *__wrap_ttyname(int fd) {
    static char tty[64];
    int rc = __wrap_ttyname_r(fd, tty, sizeof(tty));
    if (rc != 0) {
        errno = rc;
        return NULL;
    }
    return tty;
}

char *__wrap_ctermid(char *s) {
    static char tty[] = "/dev/tty";
    if (s == NULL) {
        return tty;
    }
    memcpy(s, tty, sizeof(tty));
    return s;
}

char *__wrap_ctermid_r(char *s) {
    return __wrap_ctermid(s);
}

static void sinomo_default_termios(struct termios *term) {
    memset(term, 0, sizeof(*term));
    term->c_iflag = ICRNL | IXON | IUTF8;
    term->c_oflag = OPOST | ONLCR;
    term->c_cflag = B38400 | CS8 | CREAD;
    term->c_lflag = ISIG | ICANON | ECHO | ECHOE | ECHOK | IEXTEN;
    term->c_cc[VINTR] = 3;
    term->c_cc[VQUIT] = 28;
    term->c_cc[VERASE] = 127;
    term->c_cc[VKILL] = 21;
    term->c_cc[VEOF] = 4;
    term->c_cc[VMIN] = 1;
    term->c_cc[VTIME] = 0;
    term->c_cc[VSTART] = 17;
    term->c_cc[VSTOP] = 19;
    term->c_cc[VSUSP] = 26;
    term->__c_ispeed = B38400;
    term->__c_ospeed = B38400;
}

static struct termios sinomo_tty_termios;
static int sinomo_tty_termios_initialized = 0;
static struct winsize sinomo_tty_winsize = {24, 80, 0, 0};

static struct sinomo_pty_slot sinomo_pty_slots[SINOMO_PTY_SLOTS];
static struct sinomo_pty_fd sinomo_pty_fds[SINOMO_PTY_FDS];
static int sinomo_next_pty_id = 0;

static struct termios *sinomo_current_termios(void) {
    if (!sinomo_tty_termios_initialized) {
        sinomo_default_termios(&sinomo_tty_termios);
        sinomo_tty_termios_initialized = 1;
    }
    return &sinomo_tty_termios;
}

static struct sinomo_pty_fd *sinomo_pty_fd_entry(int fd) {
    for (size_t index = 0; index < SINOMO_PTY_FDS; index++) {
        if (sinomo_pty_fds[index].fd == fd) {
            return &sinomo_pty_fds[index];
        }
    }
    return NULL;
}

static struct sinomo_pty_slot *sinomo_pty_slot_by_id(int id) {
    for (size_t index = 0; index < SINOMO_PTY_SLOTS; index++) {
        if (sinomo_pty_slots[index].active && sinomo_pty_slots[index].id == id) {
            return &sinomo_pty_slots[index];
        }
    }
    return NULL;
}

static struct sinomo_pty_slot *sinomo_pty_slot_for_fd(int fd) {
    struct sinomo_pty_fd *entry = sinomo_pty_fd_entry(fd);
    return entry == NULL ? NULL : sinomo_pty_slot_by_id(entry->id);
}

static int sinomo_pty_remember_fd(int fd, int id, int master) {
    for (size_t index = 0; index < SINOMO_PTY_FDS; index++) {
        if (sinomo_pty_fds[index].fd == fd || sinomo_pty_fds[index].fd == 0) {
            sinomo_pty_fds[index].fd = fd;
            sinomo_pty_fds[index].id = id;
            sinomo_pty_fds[index].master = master;
            return 0;
        }
    }
    errno = EMFILE;
    return -1;
}

static void sinomo_pty_forget_fd(int fd) {
    int id = -1;
    for (size_t index = 0; index < SINOMO_PTY_FDS; index++) {
        if (sinomo_pty_fds[index].fd == fd) {
            id = sinomo_pty_fds[index].id;
            memset(&sinomo_pty_fds[index], 0, sizeof(sinomo_pty_fds[index]));
        }
    }
    if (id < 0) {
        return;
    }
    for (size_t index = 0; index < SINOMO_PTY_FDS; index++) {
        if (sinomo_pty_fds[index].fd != 0 && sinomo_pty_fds[index].id == id) {
            return;
        }
    }
    struct sinomo_pty_slot *slot = sinomo_pty_slot_by_id(id);
    if (slot != NULL) {
        memset(slot, 0, sizeof(*slot));
    }
}

static struct sinomo_pty_slot *sinomo_pty_allocate_slot(void) {
    for (size_t index = 0; index < SINOMO_PTY_SLOTS; index++) {
        if (!sinomo_pty_slots[index].active) {
            struct sinomo_pty_slot *slot = &sinomo_pty_slots[index];
            memset(slot, 0, sizeof(*slot));
            slot->active = 1;
            slot->id = ++sinomo_next_pty_id;
            sinomo_default_termios(&slot->term);
            slot->winsize = sinomo_tty_winsize;
            return slot;
        }
    }
    errno = ENOSPC;
    return NULL;
}

static int sinomo_pty_path_id(const char *path) {
    const char prefix[] = "/dev/pts/";
    size_t prefix_len = sizeof(prefix) - 1U;
    if (path == NULL || strncmp(path, prefix, prefix_len) != 0 || path[prefix_len] == 0) {
        return -1;
    }
    char *end = NULL;
    long id = strtol(path + prefix_len, &end, 10);
    if (id <= 0 || id > INT_MAX || end == NULL || *end != 0) {
        return -1;
    }
    return (int)id;
}

static int sinomo_pty_open_slave_id(int id, int flags) {
    struct sinomo_pty_slot *slot = sinomo_pty_slot_by_id(id);
    if (slot == NULL) {
        errno = ENOENT;
        return -1;
    }
    if (!slot->unlocked) {
        errno = EIO;
        return -1;
    }
    for (size_t index = 0; index < SINOMO_PTY_FDS; index++) {
        if (sinomo_pty_fds[index].fd != 0 && sinomo_pty_fds[index].id == id && !sinomo_pty_fds[index].master) {
            int fd = __wrap_dup(sinomo_pty_fds[index].fd);
            if (fd < 0) {
                return -1;
            }
            int fd_flags = 0;
#ifdef O_CLOEXEC
            if ((flags & O_CLOEXEC) != 0) {
                fd_flags |= O_CLOEXEC;
            }
#endif
#ifdef O_NONBLOCK
            if ((flags & O_NONBLOCK) != 0) {
                fd_flags |= O_NONBLOCK;
            }
#endif
            if (sinomo_apply_fd_flags(fd, fd_flags) != 0 || sinomo_pty_remember_fd(fd, id, 0) != 0) {
                int saved_errno = errno;
                __real_close(fd);
                errno = saved_errno;
                return -1;
            }
            return fd;
        }
    }
    errno = ENXIO;
    return -1;
}

speed_t __wrap_cfgetospeed(const struct termios *term) {
    return term == NULL ? (speed_t)0 : term->__c_ospeed;
}

speed_t __wrap_cfgetispeed(const struct termios *term) {
    return term == NULL ? (speed_t)0 : term->__c_ispeed;
}

int __wrap_cfsetospeed(struct termios *term, speed_t speed) {
    if (term == NULL) {
        errno = EFAULT;
        return -1;
    }
    term->__c_ospeed = speed;
    return 0;
}

int __wrap_cfsetispeed(struct termios *term, speed_t speed) {
    if (term == NULL) {
        errno = EFAULT;
        return -1;
    }
    term->__c_ispeed = speed;
    return 0;
}

int __wrap_cfsetspeed(struct termios *term, speed_t speed) {
    if (__wrap_cfsetispeed(term, speed) != 0) {
        return -1;
    }
    return __wrap_cfsetospeed(term, speed);
}

void __wrap_cfmakeraw(struct termios *term) {
    if (term == NULL) {
        return;
    }
    term->c_iflag &= (tcflag_t)~(IGNBRK | BRKINT | PARMRK | ISTRIP | INLCR | IGNCR | ICRNL | IXON);
    term->c_oflag &= (tcflag_t)~OPOST;
    term->c_lflag &= (tcflag_t)~(ECHO | ECHONL | ICANON | ISIG | IEXTEN);
    term->c_cflag &= (tcflag_t)~(CSIZE | PARENB);
    term->c_cflag |= CS8;
    term->c_cc[VMIN] = 1;
    term->c_cc[VTIME] = 0;
}

int __wrap_tcgetattr(int fd, struct termios *term) {
    if (term == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (!__wrap_isatty(fd)) {
        return -1;
    }
    struct sinomo_pty_slot *pty = sinomo_pty_slot_for_fd(fd);
    *term = pty == NULL ? *sinomo_current_termios() : pty->term;
#ifdef TOSTOP
    if (pty == NULL) {
        int tostop = 0;
        if (sinomo_proc_tty_get_tostop(&tostop) == 0) {
            if (tostop) {
                term->c_lflag |= TOSTOP;
            } else {
                term->c_lflag &= (tcflag_t)~TOSTOP;
            }
        }
    }
#endif
    return 0;
}

int __wrap_tcsetattr(int fd, int optional_actions, const struct termios *term) {
    if (term == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (optional_actions != TCSANOW && optional_actions != TCSADRAIN && optional_actions != TCSAFLUSH) {
        errno = EINVAL;
        return -1;
    }
    if (!__wrap_isatty(fd)) {
        return -1;
    }
    struct sinomo_pty_slot *pty = sinomo_pty_slot_for_fd(fd);
    if (pty == NULL) {
        *sinomo_current_termios() = *term;
#ifdef TOSTOP
        (void)sinomo_proc_tty_set_tostop((term->c_lflag & TOSTOP) != 0);
#endif
    } else {
        pty->term = *term;
    }
    return 0;
}

int __wrap_tcgetwinsize(int fd, struct winsize *winsize) {
    if (winsize == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (!__wrap_isatty(fd)) {
        return -1;
    }
    struct sinomo_pty_slot *pty = sinomo_pty_slot_for_fd(fd);
    *winsize = pty == NULL ? sinomo_tty_winsize : pty->winsize;
    return 0;
}

int __wrap_tcsetwinsize(int fd, const struct winsize *winsize) {
    if (winsize == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (!__wrap_isatty(fd)) {
        return -1;
    }
    struct sinomo_pty_slot *pty = sinomo_pty_slot_for_fd(fd);
    if (pty == NULL) {
        sinomo_tty_winsize = *winsize;
    } else {
        pty->winsize = *winsize;
    }
    return 0;
}

int __wrap_tcsendbreak(int fd, int duration) {
    (void)duration;
    return __wrap_isatty(fd) ? 0 : -1;
}

int __wrap_tcdrain(int fd) {
    if (!__wrap_isatty(fd)) {
        return -1;
    }
    return fsync(fd) == 0 || errno == EINVAL ? 0 : -1;
}

int __wrap_tcflush(int fd, int queue_selector) {
    if (queue_selector != TCIFLUSH && queue_selector != TCOFLUSH && queue_selector != TCIOFLUSH) {
        errno = EINVAL;
        return -1;
    }
    return __wrap_isatty(fd) ? 0 : -1;
}

int __wrap_tcflow(int fd, int action) {
    if (action != TCOOFF && action != TCOON && action != TCIOFF && action != TCION) {
        errno = EINVAL;
        return -1;
    }
    return __wrap_isatty(fd) ? 0 : -1;
}

pid_t __wrap_tcgetsid(int fd) {
    int sid = 0;
    if (!__wrap_isatty(fd)) {
        return (pid_t)-1;
    }
    int rc = sinomo_proc_tcgetsid(fd, &sid);
    if (rc != 0) {
        errno = rc;
        return (pid_t)-1;
    }
    return (pid_t)sid;
}

int __wrap_ioctl(int fd, int request, ...) {
    va_list ap;
    va_start(ap, request);
    void *arg = va_arg(ap, void *);
    va_end(ap);

    switch (request) {
        case TCGETS:
            return __wrap_tcgetattr(fd, (struct termios *)arg);
        case TCSETS:
            return __wrap_tcsetattr(fd, TCSANOW, (const struct termios *)arg);
        case TCSETSW:
            return __wrap_tcsetattr(fd, TCSADRAIN, (const struct termios *)arg);
        case TCSETSF:
            return __wrap_tcsetattr(fd, TCSAFLUSH, (const struct termios *)arg);
        case TIOCGPGRP:
            if (arg == NULL) {
                errno = EFAULT;
                return -1;
            }
            *(pid_t *)arg = __wrap_tcgetpgrp(fd);
            return *(pid_t *)arg == (pid_t)-1 ? -1 : 0;
        case TIOCSPGRP:
            if (arg == NULL) {
                errno = EFAULT;
                return -1;
            }
            return __wrap_tcsetpgrp(fd, *(const pid_t *)arg);
        case TIOCGPTN: {
            if (arg == NULL) {
                errno = EFAULT;
                return -1;
            }
            struct sinomo_pty_fd *pty = sinomo_pty_fd_entry(fd);
            if (pty == NULL || !pty->master) {
                errno = ENOTTY;
                return -1;
            }
            *(int *)arg = pty->id;
            return 0;
        }
        case TIOCSPTLCK: {
            if (arg == NULL) {
                errno = EFAULT;
                return -1;
            }
            struct sinomo_pty_fd *pty = sinomo_pty_fd_entry(fd);
            struct sinomo_pty_slot *slot = pty == NULL || !pty->master ? NULL : sinomo_pty_slot_by_id(pty->id);
            if (slot == NULL) {
                errno = ENOTTY;
                return -1;
            }
            slot->unlocked = *(const int *)arg == 0;
            return 0;
        }
        case TIOCGPTPEER: {
            struct sinomo_pty_fd *pty = sinomo_pty_fd_entry(fd);
            if (pty == NULL || !pty->master) {
                errno = ENOTTY;
                return -1;
            }
            return sinomo_pty_open_slave_id(pty->id, (int)(intptr_t)arg);
        }
#ifdef TIOCGWINSZ
        case TIOCGWINSZ:
            return __wrap_tcgetwinsize(fd, (struct winsize *)arg);
#endif
#ifdef TIOCSWINSZ
        case TIOCSWINSZ:
            return __wrap_tcsetwinsize(fd, (const struct winsize *)arg);
#endif
#ifdef FIONBIO
        case FIONBIO:
            if (arg == NULL) {
                errno = EFAULT;
                return -1;
            }
            if (*(int *)arg) {
                return sinomo_apply_fd_flags(fd, O_NONBLOCK);
            } else {
                int current = __real_fcntl(fd, F_GETFL, 0);
                if (current < 0) {
                    return -1;
                }
                return __real_fcntl(fd, F_SETFL, current & ~O_NONBLOCK);
            }
#endif
#ifdef FIONREAD
        case FIONREAD:
            if (arg == NULL) {
                errno = EFAULT;
                return -1;
            }
            *(int *)arg = 0;
            return 0;
#endif
        default:
            errno = ENOTTY;
            return -1;
    }
}

int __wrap_getlogin_r(char *name, size_t namesize) {
    const char login[] = "sinomo";
    size_t login_len = sizeof(login);
    if (name == NULL) {
        return EINVAL;
    }
    if (namesize < login_len) {
        return ERANGE;
    }
    memcpy(name, login, login_len);
    return 0;
}

char *__wrap_getlogin(void) {
    static char login[] = "sinomo";
    return login;
}

char *__wrap_getpass(const char *prompt) {
    static char password[256];
    FILE *input = fopen("/dev/tty", "r");
    int close_input = input != NULL;
    if (input == NULL) {
        input = stdin;
    }
    if (prompt != NULL) {
        fputs(prompt, stderr);
        fflush(stderr);
    }
    struct termios old_term;
    int term_fd = fileno(input);
    int restore_term = 0;
    if (term_fd >= 0 && __wrap_tcgetattr(term_fd, &old_term) == 0) {
        struct termios new_term = old_term;
        new_term.c_lflag &= (tcflag_t)~ECHO;
        if (__wrap_tcsetattr(term_fd, TCSAFLUSH, &new_term) == 0) {
            restore_term = 1;
        }
    }
    if (fgets(password, sizeof(password), input) == NULL) {
        password[0] = 0;
    }
    password[strcspn(password, "\r\n")] = 0;
    if (restore_term) {
        (void)__wrap_tcsetattr(term_fd, TCSAFLUSH, &old_term);
        fputc('\n', stderr);
    }
    if (close_input) {
        fclose(input);
    }
    return password;
}

int __wrap_getrlimit(int resource, struct rlimit *limit) {
    if (limit == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (resource < 0 || resource >= RLIM_NLIMITS) {
        errno = EINVAL;
        return -1;
    }
    unsigned long long current = 0;
    unsigned long long maximum = 0;
    int rc = sinomo_proc_getrlimit(0, resource, &current, &maximum);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    limit->rlim_cur = (rlim_t)current;
    limit->rlim_max = (rlim_t)maximum;
    return 0;
}

int __wrap_setrlimit(int resource, const struct rlimit *limit) {
    if (limit == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (resource < 0 || resource >= RLIM_NLIMITS) {
        errno = EINVAL;
        return -1;
    }
    int rc = sinomo_proc_setrlimit(0, resource, (unsigned long long)limit->rlim_cur, (unsigned long long)limit->rlim_max);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    return 0;
}

int __wrap_prlimit(pid_t pid, int resource, const struct rlimit *new_limit, struct rlimit *old_limit) {
    if (pid < 0) {
        errno = EINVAL;
        return -1;
    }
    if (resource < 0 || resource >= RLIM_NLIMITS) {
        errno = EINVAL;
        return -1;
    }
    if (old_limit != NULL) {
        unsigned long long current = 0;
        unsigned long long maximum = 0;
        int rc = sinomo_proc_getrlimit((int)pid, resource, &current, &maximum);
        if (rc != 0) {
            errno = rc;
            return -1;
        }
        old_limit->rlim_cur = (rlim_t)current;
        old_limit->rlim_max = (rlim_t)maximum;
    }
    if (new_limit != NULL) {
        int rc = sinomo_proc_setrlimit((int)pid, resource, (unsigned long long)new_limit->rlim_cur, (unsigned long long)new_limit->rlim_max);
        if (rc != 0) {
            errno = rc;
            return -1;
        }
    }
    return 0;
}

int __wrap_getpagesize(void) {
    return 4096;
}

static unsigned long long sinomo_memory_limit_bytes(void) {
    unsigned long long current = 0;
    unsigned long long maximum = 0;
    int rc = sinomo_proc_getrlimit(0, RLIMIT_AS, &current, &maximum);
    if (rc == 0 && current != 0 && current != ULLONG_MAX) {
        return current;
    }
    return sinomo_env_u64("SINOMO_SANDBOX_MEMORY_LIMIT_BYTES", 512ULL * 1024ULL * 1024ULL);
}

long __wrap_get_phys_pages(void) {
    return (long)(sinomo_memory_limit_bytes() / (unsigned long long)__wrap_getpagesize());
}

long __wrap_get_avphys_pages(void) {
    return __wrap_get_phys_pages();
}

int __wrap_get_nprocs_conf(void) {
    return 1;
}

int __wrap_get_nprocs(void) {
    return 1;
}

int __wrap_getloadavg(double loadavg[], int nelem) {
    if (nelem < 0) {
        errno = EINVAL;
        return -1;
    }
    if (loadavg == NULL && nelem > 0) {
        errno = EFAULT;
        return -1;
    }
    int count = nelem > 3 ? 3 : nelem;
    for (int index = 0; index < count; index++) {
        loadavg[index] = 0.0;
    }
    return count;
}

int __wrap_sysinfo(struct sysinfo *info) {
    if (info == NULL) {
        errno = EFAULT;
        return -1;
    }
    memset(info, 0, sizeof(*info));
    unsigned long long memory_limit = sinomo_memory_limit_bytes();
    info->uptime = (unsigned long)time(NULL);
    info->totalram = (unsigned long)(memory_limit / 4096ULL);
    info->freeram = info->totalram;
    info->mem_unit = 4096U;
    info->procs = 1;
    return 0;
}

static fenv_t sinomo_fenv = {0};

int __wrap_feclearexcept(int excepts) {
    if ((excepts & ~FE_ALL_EXCEPT) != 0) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

int __wrap_fegetexceptflag(fexcept_t *flagp, int excepts) {
    if (flagp == NULL) {
        errno = EFAULT;
        return -1;
    }
    if ((excepts & ~FE_ALL_EXCEPT) != 0) {
        errno = EINVAL;
        return -1;
    }
    *flagp = 0;
    return 0;
}

int __wrap_fesetexceptflag(const fexcept_t *flagp, int excepts) {
    if (flagp == NULL) {
        errno = EFAULT;
        return -1;
    }
    if ((excepts & ~FE_ALL_EXCEPT) != 0) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

int __wrap_feraiseexcept(int excepts) {
    if ((excepts & ~FE_ALL_EXCEPT) != 0) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

int __wrap_fetestexcept(int excepts) {
    if ((excepts & ~FE_ALL_EXCEPT) != 0) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

int __wrap_fegetround(void) {
    return FE_TONEAREST;
}

int __wrap_fesetround(int round) {
    if (round != FE_TONEAREST) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

int __wrap_fegetenv(fenv_t *envp) {
    if (envp == NULL) {
        errno = EFAULT;
        return -1;
    }
    *envp = sinomo_fenv;
    return 0;
}

int __wrap_feholdexcept(fenv_t *envp) {
    return __wrap_fegetenv(envp);
}

int __wrap_fesetenv(const fenv_t *envp) {
    if (envp == FE_DFL_ENV) {
        sinomo_fenv.__cw = 0;
        return 0;
    }
    if (envp == NULL) {
        errno = EFAULT;
        return -1;
    }
    sinomo_fenv = *envp;
    return 0;
}

int __wrap_feupdateenv(const fenv_t *envp) {
    return __wrap_fesetenv(envp);
}

static uint64_t sinomo_random_state = 1;
static char *sinomo_random_state_buffer = NULL;

static uint64_t sinomo_random_next_u64(void) {
    sinomo_random_state = sinomo_random_state * 6364136223846793005ULL + 1442695040888963407ULL;
    return sinomo_random_state;
}

long __wrap_random(void) {
    return (long)((sinomo_random_next_u64() >> 33) & 0x7fffffffL);
}

void __wrap_srandom(unsigned int seed) {
    sinomo_random_state = seed == 0 ? 1 : (uint64_t)seed;
}

char *__wrap_initstate(unsigned int seed, char *state, size_t n) {
    if (state == NULL || n < sizeof(uint64_t)) {
        errno = EINVAL;
        return NULL;
    }
    char *previous = sinomo_random_state_buffer;
    sinomo_random_state_buffer = state;
    uint64_t stored = seed == 0 ? 1 : (uint64_t)seed;
    memcpy(state, &stored, sizeof(stored));
    sinomo_random_state = stored;
    return previous;
}

char *__wrap_setstate(char *state) {
    if (state == NULL) {
        errno = EINVAL;
        return NULL;
    }
    char *previous = sinomo_random_state_buffer;
    sinomo_random_state_buffer = state;
    memcpy(&sinomo_random_state, state, sizeof(sinomo_random_state));
    if (sinomo_random_state == 0) {
        sinomo_random_state = 1;
    }
    return previous;
}

static unsigned long long sinomo_drand48_state = 0x330eULL;

static unsigned long long sinomo_drand48_pack(const unsigned short xsubi[3]) {
    return ((unsigned long long)xsubi[2] << 32) | ((unsigned long long)xsubi[1] << 16) | (unsigned long long)xsubi[0];
}

static void sinomo_drand48_unpack(unsigned long long state, unsigned short xsubi[3]) {
    xsubi[0] = (unsigned short)(state & 0xffffU);
    xsubi[1] = (unsigned short)((state >> 16) & 0xffffU);
    xsubi[2] = (unsigned short)((state >> 32) & 0xffffU);
}

static unsigned long long sinomo_drand48_next(unsigned long long state) {
    return (state * 0x5deece66dULL + 0xbULL) & ((1ULL << 48) - 1);
}

double __wrap_erand48(unsigned short xsubi[3]) {
    if (xsubi == NULL) {
        errno = EFAULT;
        return 0.0;
    }
    unsigned long long state = sinomo_drand48_next(sinomo_drand48_pack(xsubi));
    sinomo_drand48_unpack(state, xsubi);
    return (double)state / (double)(1ULL << 48);
}

double __wrap_drand48(void) {
    unsigned short xsubi[3];
    sinomo_drand48_unpack(sinomo_drand48_state, xsubi);
    double value = __wrap_erand48(xsubi);
    sinomo_drand48_state = sinomo_drand48_pack(xsubi);
    return value;
}

long __wrap_nrand48(unsigned short xsubi[3]) {
    (void)__wrap_erand48(xsubi);
    return xsubi == NULL ? 0 : (long)(sinomo_drand48_pack(xsubi) >> 17);
}

long __wrap_lrand48(void) {
    unsigned short xsubi[3];
    sinomo_drand48_unpack(sinomo_drand48_state, xsubi);
    long value = __wrap_nrand48(xsubi);
    sinomo_drand48_state = sinomo_drand48_pack(xsubi);
    return value;
}

long __wrap_jrand48(unsigned short xsubi[3]) {
    (void)__wrap_erand48(xsubi);
    if (xsubi == NULL) {
        return 0;
    }
    return (long)((int32_t)(sinomo_drand48_pack(xsubi) >> 16));
}

long __wrap_mrand48(void) {
    unsigned short xsubi[3];
    sinomo_drand48_unpack(sinomo_drand48_state, xsubi);
    long value = __wrap_jrand48(xsubi);
    sinomo_drand48_state = sinomo_drand48_pack(xsubi);
    return value;
}

void __wrap_srand48(long seedval) {
    sinomo_drand48_state = (((unsigned long long)(uint32_t)seedval) << 16) | 0x330eULL;
}

unsigned short *__wrap_seed48(unsigned short seed16v[3]) {
    static unsigned short previous[3];
    if (seed16v == NULL) {
        errno = EFAULT;
        return NULL;
    }
    sinomo_drand48_unpack(sinomo_drand48_state, previous);
    sinomo_drand48_state = sinomo_drand48_pack(seed16v);
    return previous;
}

void __wrap_lcong48(unsigned short param[7]) {
    if (param == NULL) {
        return;
    }
    sinomo_drand48_state = sinomo_drand48_pack(param);
}

void *__wrap_reallocarray(void *ptr, size_t nmemb, size_t size) {
    if (size != 0 && nmemb > SIZE_MAX / size) {
        errno = ENOMEM;
        return NULL;
    }
    return realloc(ptr, nmemb * size);
}

int __wrap_clearenv(void) {
    static char *empty_env[] = {NULL};
    environ = empty_env;
    return 0;
}

char *__wrap_secure_getenv(const char *name) {
    return getenv(name);
}

float __wrap_strtof_l(const char *nptr, char **endptr, locale_t locale) {
    (void)locale;
    return strtof(nptr, endptr);
}

double __wrap_strtod_l(const char *nptr, char **endptr, locale_t locale) {
    (void)locale;
    return strtod(nptr, endptr);
}

long double __wrap_strtold_l(const char *nptr, char **endptr, locale_t locale) {
    (void)locale;
    return strtold(nptr, endptr);
}

char *__wrap_l64a(long value) {
    static char buffer[8];
    static const char digits[] = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    unsigned long v = (unsigned long)value;
    size_t index = 0;
    while (v != 0 && index < sizeof(buffer) - 1) {
        buffer[index++] = digits[v & 0x3fU];
        v >>= 6;
    }
    buffer[index] = 0;
    return buffer;
}

long __wrap_a64l(const char *s) {
    if (s == NULL) {
        errno = EFAULT;
        return 0;
    }
    unsigned long value = 0;
    unsigned int shift = 0;
    for (size_t index = 0; index < 6 && s[index] != 0; index++) {
        unsigned char c = (unsigned char)s[index];
        unsigned long digit;
        if (c == '.') {
            digit = 0;
        } else if (c == '/') {
            digit = 1;
        } else if (c >= '0' && c <= '9') {
            digit = (unsigned long)(c - '0') + 2;
        } else if (c >= 'A' && c <= 'Z') {
            digit = (unsigned long)(c - 'A') + 12;
        } else if (c >= 'a' && c <= 'z') {
            digit = (unsigned long)(c - 'a') + 38;
        } else {
            break;
        }
        value |= digit << shift;
        shift += 6;
    }
    return (long)value;
}

static int sinomo_is_leap_year(long long year) {
    return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

static long long sinomo_days_from_civil(long long year, unsigned int month, unsigned int day) {
    year -= month <= 2;
    long long era = (year >= 0 ? year : year - 399) / 400;
    unsigned int yoe = (unsigned int)(year - era * 400);
    unsigned int doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    unsigned int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (long long)doe - 719468;
}

static void sinomo_civil_from_days(long long days, int *year, unsigned int *month, unsigned int *day) {
    days += 719468;
    long long era = (days >= 0 ? days : days - 146096) / 146097;
    unsigned int doe = (unsigned int)(days - era * 146097);
    unsigned int yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    long long y = (long long)yoe + era * 400;
    unsigned int doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned int mp = (5 * doy + 2) / 153;
    unsigned int d = doy - (153 * mp + 2) / 5 + 1;
    unsigned int m = mp + (mp < 10 ? 3 : -9);
    y += m <= 2;
    *year = (int)y;
    *month = m;
    *day = d;
}

static int sinomo_normalize_tm(struct tm *tm) {
    long long year = (long long)tm->tm_year + 1900;
    long long month_index = tm->tm_mon;
    year += month_index / 12;
    month_index %= 12;
    if (month_index < 0) {
        month_index += 12;
        year--;
    }

    long long days = sinomo_days_from_civil(year, (unsigned int)month_index + 1, 1);
    days += (long long)tm->tm_mday - 1;
    long long seconds = days * 86400LL + (long long)tm->tm_hour * 3600LL + (long long)tm->tm_min * 60LL + (long long)tm->tm_sec;
    long long normalized_days = seconds / 86400LL;
    long long day_seconds = seconds % 86400LL;
    if (day_seconds < 0) {
        day_seconds += 86400LL;
        normalized_days--;
    }

    int normalized_year = 1970;
    unsigned int normalized_month = 1;
    unsigned int normalized_day = 1;
    sinomo_civil_from_days(normalized_days, &normalized_year, &normalized_month, &normalized_day);
    tm->tm_year = normalized_year - 1900;
    tm->tm_mon = (int)normalized_month - 1;
    tm->tm_mday = (int)normalized_day;
    tm->tm_hour = (int)(day_seconds / 3600);
    tm->tm_min = (int)((day_seconds % 3600) / 60);
    tm->tm_sec = (int)(day_seconds % 60);
    tm->tm_wday = (int)((normalized_days + 4) % 7);
    if (tm->tm_wday < 0) {
        tm->tm_wday += 7;
    }
    tm->tm_yday = (int)(normalized_days - sinomo_days_from_civil(normalized_year, 1, 1));
    tm->tm_isdst = 0;
    return 0;
}

struct tm *__wrap_gmtime_r(const time_t *timep, struct tm *result) {
    if (timep == NULL || result == NULL) {
        errno = EFAULT;
        return NULL;
    }
    long long seconds = (long long)*timep;
    long long days = seconds / 86400LL;
    long long day_seconds = seconds % 86400LL;
    if (day_seconds < 0) {
        day_seconds += 86400LL;
        days--;
    }

    int year = 1970;
    unsigned int month = 1;
    unsigned int day = 1;
    sinomo_civil_from_days(days, &year, &month, &day);
    memset(result, 0, sizeof(*result));
    result->tm_year = year - 1900;
    result->tm_mon = (int)month - 1;
    result->tm_mday = (int)day;
    result->tm_hour = (int)(day_seconds / 3600);
    result->tm_min = (int)((day_seconds % 3600) / 60);
    result->tm_sec = (int)(day_seconds % 60);
    result->tm_wday = (int)((days + 4) % 7);
    if (result->tm_wday < 0) {
        result->tm_wday += 7;
    }
    result->tm_yday = (int)(days - sinomo_days_from_civil(year, 1, 1));
    result->tm_isdst = 0;
    return result;
}

struct tm *__wrap_localtime_r(const time_t *timep, struct tm *result) {
    return __wrap_gmtime_r(timep, result);
}

time_t __wrap_timegm(struct tm *tm) {
    if (tm == NULL) {
        errno = EFAULT;
        return (time_t)-1;
    }
    struct tm normalized = *tm;
    sinomo_normalize_tm(&normalized);
    long long days = sinomo_days_from_civil(
        (long long)normalized.tm_year + 1900,
        (unsigned int)normalized.tm_mon + 1,
        (unsigned int)normalized.tm_mday
    );
    long long seconds = days * 86400LL + (long long)normalized.tm_hour * 3600LL + (long long)normalized.tm_min * 60LL + (long long)normalized.tm_sec;
    *tm = normalized;
    return (time_t)seconds;
}

time_t __wrap_mktime(struct tm *tm) {
    return __wrap_timegm(tm);
}

void __wrap_tzset(void) {
}

size_t __wrap_strftime_l(char *s, size_t max, const char *format, const struct tm *tm, locale_t locale) {
    (void)locale;
    return strftime(s, max, format, tm);
}

char *__wrap_asctime_r(const struct tm *tm, char *buf) {
    if (tm == NULL || buf == NULL) {
        errno = EFAULT;
        return NULL;
    }
    if (strftime(buf, 26, "%a %b %e %H:%M:%S %Y\n", tm) == 0) {
        errno = EOVERFLOW;
        return NULL;
    }
    return buf;
}

char *__wrap_ctime_r(const time_t *timep, char *buf) {
    struct tm tm;
    if (__wrap_localtime_r(timep, &tm) == NULL) {
        return NULL;
    }
    return __wrap_asctime_r(&tm, buf);
}

ssize_t __wrap_getrandom(void *buf, size_t buflen, unsigned int flags) {
    if (buf == NULL && buflen > 0) {
        errno = EFAULT;
        return -1;
    }
    if ((flags & ~(GRND_NONBLOCK | GRND_RANDOM | GRND_INSECURE)) != 0) {
        errno = EINVAL;
        return -1;
    }
    if (buflen == 0) {
        return 0;
    }
    if (buflen > (size_t)SSIZE_MAX) {
        buflen = (size_t)SSIZE_MAX;
    }
    __wasi_errno_t rc = __wasi_random_get((uint8_t *)buf, buflen);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    return (ssize_t)buflen;
}

int __wrap_getentropy(void *buf, size_t buflen) {
    if (buf == NULL && buflen > 0) {
        errno = EFAULT;
        return -1;
    }
    if (buflen > 256) {
        errno = EIO;
        return -1;
    }
    return __wrap_getrandom(buf, buflen, 0) < 0 ? -1 : 0;
}

int __wrap_posix_madvise(void *addr, size_t len, int advice) {
    (void)addr;
    (void)len;
    switch (advice) {
        case POSIX_MADV_NORMAL:
        case POSIX_MADV_RANDOM:
        case POSIX_MADV_SEQUENTIAL:
        case POSIX_MADV_WILLNEED:
        case POSIX_MADV_DONTNEED:
            return 0;
        default:
            return EINVAL;
    }
}

int __wrap_madvise(void *addr, size_t len, int advice) {
    (void)addr;
    (void)len;
    switch (advice) {
        case MADV_NORMAL:
        case MADV_RANDOM:
        case MADV_SEQUENTIAL:
        case MADV_WILLNEED:
        case MADV_DONTNEED:
        case MADV_FREE:
        case MADV_REMOVE:
        case MADV_DONTFORK:
        case MADV_DOFORK:
        case MADV_MERGEABLE:
        case MADV_UNMERGEABLE:
        case MADV_HUGEPAGE:
        case MADV_NOHUGEPAGE:
        case MADV_DONTDUMP:
        case MADV_DODUMP:
        case MADV_WIPEONFORK:
        case MADV_KEEPONFORK:
        case MADV_COLD:
        case MADV_PAGEOUT:
        case MADV_POPULATE_READ:
        case MADV_POPULATE_WRITE:
        case MADV_COLLAPSE:
            return 0;
        default:
            errno = EINVAL;
            return -1;
    }
}

int __wrap_mlock(const void *addr, size_t len) {
    if (addr == NULL && len > 0) {
        errno = EFAULT;
        return -1;
    }
    return 0;
}

int __wrap_munlock(const void *addr, size_t len) {
    if (addr == NULL && len > 0) {
        errno = EFAULT;
        return -1;
    }
    return 0;
}

int __wrap_mlockall(int flags) {
    int supported = MCL_CURRENT | MCL_FUTURE;
#ifdef MCL_ONFAULT
    supported |= MCL_ONFAULT;
#endif
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

int __wrap_munlockall(void) {
    return 0;
}

int __wrap_mlock2(const void *addr, size_t len, unsigned int flags) {
    unsigned int supported = 0;
#ifdef MLOCK_ONFAULT
    supported |= MLOCK_ONFAULT;
#endif
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    return __wrap_mlock(addr, len);
}

#ifndef MREMAP_MAYMOVE
#define MREMAP_MAYMOVE 1
#endif
#ifndef MREMAP_FIXED
#define MREMAP_FIXED 2
#endif
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif
#ifndef SHADOW_STACK_SET_TOKEN
#define SHADOW_STACK_SET_TOKEN 0x1U
#endif

#define SINOMO_VMA_PROTECTION_SLOTS 128
struct sinomo_vma_protection {
    void *addr;
    size_t len;
    int prot;
};
static struct sinomo_vma_protection sinomo_vma_protections[SINOMO_VMA_PROTECTION_SLOTS];

#define SINOMO_VMA_SEAL_SLOTS 128
struct sinomo_vma_seal {
    void *addr;
    size_t len;
};
static struct sinomo_vma_seal sinomo_vma_seals[SINOMO_VMA_SEAL_SLOTS];

static int sinomo_validate_prot(int prot) {
    int supported = PROT_READ | PROT_WRITE | PROT_EXEC | PROT_NONE;
#ifdef PROT_GROWSDOWN
    supported |= PROT_GROWSDOWN;
#endif
#ifdef PROT_GROWSUP
    supported |= PROT_GROWSUP;
#endif
    if ((prot & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

static int sinomo_remember_vma_protection(void *addr, size_t len, int prot) {
    size_t slot = SINOMO_VMA_PROTECTION_SLOTS;
    for (size_t index = 0; index < SINOMO_VMA_PROTECTION_SLOTS; index++) {
        if (sinomo_vma_protections[index].addr == addr && sinomo_vma_protections[index].len == len) {
            slot = index;
            break;
        }
        if (slot == SINOMO_VMA_PROTECTION_SLOTS && sinomo_vma_protections[index].addr == NULL) {
            slot = index;
        }
    }
    if (slot == SINOMO_VMA_PROTECTION_SLOTS) {
        errno = ENOMEM;
        return -1;
    }
    sinomo_vma_protections[slot].addr = addr;
    sinomo_vma_protections[slot].len = len;
    sinomo_vma_protections[slot].prot = prot;
    return 0;
}

static int sinomo_vma_ranges_overlap(void *left_addr, size_t left_len, void *right_addr, size_t right_len) {
    uintptr_t left_start = (uintptr_t)left_addr;
    uintptr_t right_start = (uintptr_t)right_addr;
    uintptr_t left_end = left_len > UINTPTR_MAX - left_start ? UINTPTR_MAX : left_start + left_len;
    uintptr_t right_end = right_len > UINTPTR_MAX - right_start ? UINTPTR_MAX : right_start + right_len;
    return left_start < right_end && right_start < left_end;
}

static int sinomo_vma_range_sealed(void *addr, size_t len) {
    if (addr == NULL || len == 0) {
        return 0;
    }
    for (size_t index = 0; index < SINOMO_VMA_SEAL_SLOTS; index++) {
        if (sinomo_vma_seals[index].addr != NULL &&
            sinomo_vma_ranges_overlap(addr, len, sinomo_vma_seals[index].addr, sinomo_vma_seals[index].len)) {
            return 1;
        }
    }
    return 0;
}

int __wrap_mseal(void *addr, size_t len, unsigned long flags) {
    if (addr == NULL || len == 0 || flags != 0) {
        errno = EINVAL;
        return -1;
    }
    long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
        page_size = 4096;
    }
    size_t page = (size_t)page_size;
    uintptr_t start = (uintptr_t)addr;
    if ((start & (page - 1)) != 0 || (len & (page - 1)) != 0) {
        errno = EINVAL;
        return -1;
    }
    for (size_t index = 0; index < SINOMO_VMA_SEAL_SLOTS; index++) {
        if (sinomo_vma_seals[index].addr == addr && sinomo_vma_seals[index].len == len) {
            return 0;
        }
    }
    for (size_t index = 0; index < SINOMO_VMA_SEAL_SLOTS; index++) {
        if (sinomo_vma_seals[index].addr == NULL) {
            sinomo_vma_seals[index].addr = addr;
            sinomo_vma_seals[index].len = len;
            return 0;
        }
    }
    errno = ENOMEM;
    return -1;
}

void *__wrap_map_shadow_stack(void *addr, size_t size, unsigned int flags) {
    if ((flags & ~SHADOW_STACK_SET_TOKEN) != 0 || size == 0) {
        errno = EINVAL;
        return MAP_FAILED;
    }
    long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
        page_size = 4096;
    }
    size_t page = (size_t)page_size;
    if (size > SIZE_MAX - (page - 1)) {
        errno = ENOMEM;
        return MAP_FAILED;
    }
    size_t rounded = (size + page - 1) & ~(page - 1);
    int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS;
#ifdef MAP_FIXED_NOREPLACE
    if (addr != NULL) {
        mmap_flags |= MAP_FIXED_NOREPLACE;
    }
#endif
    void *mapped = mmap(addr, rounded, PROT_READ | PROT_WRITE, mmap_flags, -1, 0);
    if (mapped == MAP_FAILED) {
        return MAP_FAILED;
    }
    if ((flags & SHADOW_STACK_SET_TOKEN) != 0 && rounded >= sizeof(uintptr_t)) {
        uintptr_t token = ((uintptr_t)mapped + rounded) | 1U;
        memcpy(mapped, &token, sizeof(token));
    }
    (void)sinomo_remember_vma_protection(mapped, rounded, PROT_READ | PROT_WRITE);
    return mapped;
}

int __wrap_mprotect(void *addr, size_t len, int prot) {
    if (addr == NULL && len > 0) {
        errno = EFAULT;
        return -1;
    }
    if (sinomo_validate_prot(prot) != 0) {
        return -1;
    }
    if (len == 0) {
        return 0;
    }
    if (sinomo_vma_range_sealed(addr, len)) {
        errno = EPERM;
        return -1;
    }
    return sinomo_remember_vma_protection(addr, len, prot);
}

int __wrap_munmap(void *addr, size_t len) {
    if (sinomo_vma_range_sealed(addr, len)) {
        errno = EPERM;
        return -1;
    }
    return __real_munmap(addr, len);
}

void *__wrap_mremap(void *old_address, size_t old_size, size_t new_size, int flags, ...) {
    void *new_address = NULL;
    if ((flags & MREMAP_FIXED) != 0) {
        va_list ap;
        va_start(ap, flags);
        new_address = va_arg(ap, void *);
        va_end(ap);
    }
    if (old_address == NULL || old_size == 0 || new_size == 0) {
        errno = EINVAL;
        return MAP_FAILED;
    }
    if ((flags & ~(MREMAP_MAYMOVE | MREMAP_FIXED)) != 0) {
        errno = EINVAL;
        return MAP_FAILED;
    }
    if ((flags & MREMAP_FIXED) != 0 && new_address == NULL) {
        errno = EINVAL;
        return MAP_FAILED;
    }
    if (sinomo_vma_range_sealed(old_address, old_size)) {
        errno = EPERM;
        return MAP_FAILED;
    }
    if (new_size <= old_size && (flags & MREMAP_FIXED) == 0) {
        return old_address;
    }
    if ((flags & MREMAP_MAYMOVE) == 0) {
        errno = ENOMEM;
        return MAP_FAILED;
    }
    int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS;
    if ((flags & MREMAP_FIXED) != 0) {
        mmap_flags |= MAP_FIXED;
    }
    void *mapped = mmap(new_address, new_size, PROT_READ | PROT_WRITE, mmap_flags, -1, 0);
    if (mapped == MAP_FAILED) {
        return MAP_FAILED;
    }
    memcpy(mapped, old_address, old_size < new_size ? old_size : new_size);
    if (__wrap_munmap(old_address, old_size) != 0) {
        int saved_errno = errno;
        __real_munmap(mapped, new_size);
        errno = saved_errno;
        return MAP_FAILED;
    }
    return mapped;
}

int __wrap_mincore(void *addr, size_t len, unsigned char *vec) {
    if (addr == NULL && len > 0) {
        errno = EFAULT;
        return -1;
    }
    if (vec == NULL && len > 0) {
        errno = EFAULT;
        return -1;
    }
    size_t page_size = (size_t)__wrap_getpagesize();
    size_t pages = (len + page_size - 1) / page_size;
    for (size_t index = 0; index < pages; index++) {
        vec[index] = 1;
    }
    return 0;
}

void *__wrap_explicit_memset(void *dest, int value, size_t len) {
    volatile unsigned char *ptr = (volatile unsigned char *)dest;
    for (size_t index = 0; index < len; index++) {
        ptr[index] = (unsigned char)value;
    }
    return dest;
}

void __wrap_explicit_bzero(void *dest, size_t len) {
    (void)__wrap_explicit_memset(dest, 0, len);
}

size_t __wrap_strnlen(const char *s, size_t maxlen);

size_t __wrap_strlcpy(char *dst, const char *src, size_t dstsize) {
    size_t src_len = strlen(src);
    if (dstsize != 0) {
        size_t copy_len = src_len >= dstsize ? dstsize - 1 : src_len;
        memcpy(dst, src, copy_len);
        dst[copy_len] = 0;
    }
    return src_len;
}

size_t __wrap_strlcat(char *dst, const char *src, size_t dstsize) {
    size_t dst_len = __wrap_strnlen(dst, dstsize);
    size_t src_len = strlen(src);
    if (dst_len == dstsize) {
        return dstsize + src_len;
    }
    size_t space = dstsize - dst_len;
    size_t copy_len = src_len >= space ? space - 1 : src_len;
    memcpy(dst + dst_len, src, copy_len);
    dst[dst_len + copy_len] = 0;
    return dst_len + src_len;
}

void *__wrap_memrchr(const void *s, int c, size_t n) {
    const unsigned char *ptr = (const unsigned char *)s;
    unsigned char needle = (unsigned char)c;
    while (n > 0) {
        n--;
        if (ptr[n] == needle) {
            return (void *)(ptr + n);
        }
    }
    return NULL;
}

void *__wrap_mempcpy(void *dest, const void *src, size_t n) {
    memcpy(dest, src, n);
    return (char *)dest + n;
}

void *__wrap_memccpy(void *dest, const void *src, int c, size_t n) {
    unsigned char *out = (unsigned char *)dest;
    const unsigned char *in = (const unsigned char *)src;
    unsigned char needle = (unsigned char)c;
    for (size_t index = 0; index < n; index++) {
        out[index] = in[index];
        if (in[index] == needle) {
            return out + index + 1;
        }
    }
    return NULL;
}

size_t __wrap_strnlen(const char *s, size_t maxlen) {
    size_t len = 0;
    while (len < maxlen && s[len] != 0) {
        len++;
    }
    return len;
}

char *__wrap_stpcpy(char *dest, const char *src) {
    size_t len = strlen(src);
    memcpy(dest, src, len + 1);
    return dest + len;
}

char *__wrap_stpncpy(char *dest, const char *src, size_t n) {
    size_t len = __wrap_strnlen(src, n);
    memcpy(dest, src, len);
    if (len < n) {
        memset(dest + len, 0, n - len);
    }
    return dest + len;
}

char *__wrap_strdup(const char *s) {
    size_t len = strlen(s);
    char *copy = malloc(len + 1);
    if (copy == NULL) {
        errno = ENOMEM;
        return NULL;
    }
    memcpy(copy, s, len + 1);
    return copy;
}

char *__wrap_strndup(const char *s, size_t n) {
    size_t len = __wrap_strnlen(s, n);
    char *copy = malloc(len + 1);
    if (copy == NULL) {
        errno = ENOMEM;
        return NULL;
    }
    memcpy(copy, s, len);
    copy[len] = 0;
    return copy;
}

static int sinomo_ascii_tolower(int c) {
    if (c >= 'A' && c <= 'Z') {
        return c + ('a' - 'A');
    }
    return c;
}

int __wrap_strcasecmp(const char *left, const char *right) {
    for (;;) {
        int lc = sinomo_ascii_tolower((unsigned char)*left);
        int rc = sinomo_ascii_tolower((unsigned char)*right);
        if (lc != rc || *left == 0 || *right == 0) {
            return lc - rc;
        }
        left++;
        right++;
    }
}

int __wrap_strncasecmp(const char *left, const char *right, size_t n) {
    for (size_t index = 0; index < n; index++) {
        int lc = sinomo_ascii_tolower((unsigned char)left[index]);
        int rc = sinomo_ascii_tolower((unsigned char)right[index]);
        if (lc != rc || left[index] == 0 || right[index] == 0) {
            return lc - rc;
        }
    }
    return 0;
}

int __wrap_strcasecmp_l(const char *left, const char *right, locale_t locale) {
    (void)locale;
    return __wrap_strcasecmp(left, right);
}

int __wrap_strncasecmp_l(const char *left, const char *right, size_t n, locale_t locale) {
    (void)locale;
    return __wrap_strncasecmp(left, right, n);
}

char *__wrap_strcasestr(const char *haystack, const char *needle) {
    size_t needle_len = strlen(needle);
    if (needle_len == 0) {
        return (char *)haystack;
    }
    for (const char *cursor = haystack; *cursor != 0; cursor++) {
        if (__wrap_strncasecmp(cursor, needle, needle_len) == 0) {
            return (char *)cursor;
        }
    }
    return NULL;
}

char *__wrap_strsep(char **stringp, const char *delim) {
    if (stringp == NULL || *stringp == NULL) {
        return NULL;
    }
    char *start = *stringp;
    char *cursor = start;
    while (*cursor != 0) {
        if (strchr(delim, *cursor) != NULL) {
            *cursor = 0;
            *stringp = cursor + 1;
            return start;
        }
        cursor++;
    }
    *stringp = NULL;
    return start;
}

int __wrap_strcoll_l(const char *left, const char *right, locale_t locale) {
    (void)locale;
    return strcmp(left, right);
}

size_t __wrap_strxfrm_l(char *dest, const char *src, size_t n, locale_t locale) {
    (void)locale;
    size_t len = strlen(src);
    if (n != 0 && dest != NULL) {
        size_t copy_len = len >= n ? n - 1 : len;
        memcpy(dest, src, copy_len);
        dest[copy_len] = 0;
    }
    return len;
}

char *__wrap_strerror_l(int errnum, locale_t locale) {
    (void)locale;
    return strerror(errnum);
}

int __wrap_strverscmp(const char *left, const char *right) {
    return strcmp(left, right);
}

char *__wrap_basename(char *path) {
    static char dot[] = ".";
    static char slash[] = "/";
    if (path == NULL || path[0] == 0) {
        return dot;
    }
    size_t len = strlen(path);
    while (len > 1 && path[len - 1] == '/') {
        path[--len] = 0;
    }
    if (len == 1 && path[0] == '/') {
        return slash;
    }
    char *last = strrchr(path, '/');
    return last == NULL ? path : last + 1;
}

char *__wrap_dirname(char *path) {
    static char dot[] = ".";
    static char slash[] = "/";
    if (path == NULL || path[0] == 0) {
        return dot;
    }
    size_t len = strlen(path);
    while (len > 1 && path[len - 1] == '/') {
        path[--len] = 0;
    }
    char *last = strrchr(path, '/');
    if (last == NULL) {
        return dot;
    }
    while (last > path && *last == '/') {
        last--;
    }
    if (last == path && path[0] == '/') {
        path[1] = 0;
        return slash;
    }
    last[1] = 0;
    return path;
}

static int sinomo_fnmatch_char_equal(char pattern, char value, int flags) {
    if ((flags & FNM_CASEFOLD) != 0) {
        return sinomo_ascii_tolower((unsigned char)pattern) == sinomo_ascii_tolower((unsigned char)value);
    }
    return pattern == value;
}

static int sinomo_fnmatch_range(const char **pattern, char value, int flags) {
    const char *cursor = *pattern;
    int negate = 0;
    int matched = 0;
    if (*cursor == '!' || *cursor == '^') {
        negate = 1;
        cursor++;
    }
    if (*cursor == ']') {
        if (sinomo_fnmatch_char_equal(']', value, flags)) {
            matched = 1;
        }
        cursor++;
    }
    while (*cursor != 0 && *cursor != ']') {
        char start = *cursor++;
        if (start == '\\' && (flags & FNM_NOESCAPE) == 0 && *cursor != 0) {
            start = *cursor++;
        }
        if (*cursor == '-' && cursor[1] != 0 && cursor[1] != ']') {
            cursor++;
            char end = *cursor++;
            if (end == '\\' && (flags & FNM_NOESCAPE) == 0 && *cursor != 0) {
                end = *cursor++;
            }
            int cmp_value = (flags & FNM_CASEFOLD) != 0 ? sinomo_ascii_tolower((unsigned char)value) : (unsigned char)value;
            int cmp_start = (flags & FNM_CASEFOLD) != 0 ? sinomo_ascii_tolower((unsigned char)start) : (unsigned char)start;
            int cmp_end = (flags & FNM_CASEFOLD) != 0 ? sinomo_ascii_tolower((unsigned char)end) : (unsigned char)end;
            if (cmp_start <= cmp_value && cmp_value <= cmp_end) {
                matched = 1;
            }
        } else if (sinomo_fnmatch_char_equal(start, value, flags)) {
            matched = 1;
        }
    }
    if (*cursor != ']') {
        return 0;
    }
    *pattern = cursor + 1;
    return negate ? !matched : matched;
}

static int sinomo_fnmatch_impl(const char *pattern, const char *string, int flags, int component_start) {
    for (;;) {
        char pc = *pattern++;
        switch (pc) {
            case 0:
                if (*string == 0) {
                    return 1;
                }
                return ((flags & FNM_LEADING_DIR) != 0 && *string == '/') ? 1 : 0;
            case '?':
                if (*string == 0 || ((flags & FNM_PATHNAME) != 0 && *string == '/')) {
                    return 0;
                }
                if ((flags & FNM_PERIOD) != 0 && component_start && *string == '.') {
                    return 0;
                }
                component_start = *string == '/';
                string++;
                break;
            case '*':
                while (*pattern == '*') {
                    pattern++;
                }
                if ((flags & FNM_PERIOD) != 0 && component_start && *string == '.') {
                    return 0;
                }
                if (*pattern == 0) {
                    if ((flags & FNM_PATHNAME) == 0 || strchr(string, '/') == NULL || (flags & FNM_LEADING_DIR) != 0) {
                        return 1;
                    }
                    return 0;
                }
                for (const char *cursor = string;; cursor++) {
                    if (sinomo_fnmatch_impl(pattern, cursor, flags, component_start)) {
                        return 1;
                    }
                    if (*cursor == 0 || ((flags & FNM_PATHNAME) != 0 && *cursor == '/')) {
                        return 0;
                    }
                    component_start = cursor[0] == '/';
                }
            case '[':
                if (*string == 0 || ((flags & FNM_PATHNAME) != 0 && *string == '/')) {
                    return 0;
                }
                if ((flags & FNM_PERIOD) != 0 && component_start && *string == '.') {
                    return 0;
                }
                pattern--;
                pattern++;
                if (!sinomo_fnmatch_range(&pattern, *string, flags)) {
                    return 0;
                }
                component_start = *string == '/';
                string++;
                break;
            case '\\':
                if ((flags & FNM_NOESCAPE) == 0 && *pattern != 0) {
                    pc = *pattern++;
                }
                /* fall through */
            default:
                if (!sinomo_fnmatch_char_equal(pc, *string, flags)) {
                    return 0;
                }
                component_start = *string == '/';
                string++;
                break;
        }
    }
}

int __wrap_fnmatch(const char *pattern, const char *string, int flags) {
    if (pattern == NULL || string == NULL) {
        errno = EFAULT;
        return FNM_NOMATCH;
    }
    int supported = FNM_PATHNAME | FNM_NOESCAPE | FNM_PERIOD | FNM_LEADING_DIR | FNM_CASEFOLD;
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return FNM_NOMATCH;
    }
    return sinomo_fnmatch_impl(pattern, string, flags, 1) ? 0 : FNM_NOMATCH;
}

static int sinomo_glob_has_magic(const char *text, size_t len, int flags) {
    for (size_t index = 0; index < len; index++) {
        char ch = text[index];
        if (ch == '*' || ch == '?' || ch == '[') {
            return 1;
        }
        if (ch == '\\' && (flags & GLOB_NOESCAPE) == 0 && index + 1 < len) {
            index++;
        }
    }
    return 0;
}

static int sinomo_glob_join(char *out, size_t out_len, const char *dir, const char *name) {
    if (dir == NULL || dir[0] == 0) {
        return snprintf(out, out_len, "%s", name) < (int)out_len ? 0 : -1;
    }
    if (strcmp(dir, "/") == 0) {
        return snprintf(out, out_len, "/%s", name) < (int)out_len ? 0 : -1;
    }
    return snprintf(out, out_len, "%s/%s", dir, name) < (int)out_len ? 0 : -1;
}

static int sinomo_glob_add(glob_t *pglob, const char *path, int flags) {
    char marked[4096];
    const char *stored = path;
    if ((flags & GLOB_MARK) != 0) {
        struct stat st;
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
            size_t len = strlen(path);
            if (len == 0 || path[len - 1] != '/') {
                if (snprintf(marked, sizeof(marked), "%s/", path) >= (int)sizeof(marked)) {
                    return GLOB_NOSPACE;
                }
                stored = marked;
            }
        }
    }

    size_t offset = (flags & GLOB_DOOFFS) != 0 ? pglob->gl_offs : 0;
    size_t slots = offset + pglob->gl_pathc + 2;
    char **pathv = realloc(pglob->gl_pathv, slots * sizeof(char *));
    if (pathv == NULL) {
        return GLOB_NOSPACE;
    }
    pglob->gl_pathv = pathv;
    if (pglob->gl_pathc == 0) {
        for (size_t index = 0; index < offset; index++) {
            pglob->gl_pathv[index] = NULL;
        }
    }
    char *copy = strdup(stored);
    if (copy == NULL) {
        return GLOB_NOSPACE;
    }
    pglob->gl_pathv[offset + pglob->gl_pathc] = copy;
    pglob->gl_pathc++;
    pglob->gl_pathv[offset + pglob->gl_pathc] = NULL;
    return 0;
}

static int sinomo_glob_error(const char *path, int err, int flags, int (*errfunc)(const char *, int)) {
    if (errfunc != NULL && errfunc(path, err) != 0) {
        return GLOB_ABORTED;
    }
    return (flags & GLOB_ERR) != 0 ? GLOB_ABORTED : 0;
}

static int sinomo_glob_walk(const char *prefix, const char *pattern, int flags, int (*errfunc)(const char *, int), glob_t *pglob) {
    while (*pattern == '/') {
        pattern++;
    }
    if (*pattern == 0) {
        const char *path = prefix[0] == 0 ? "." : prefix;
        struct stat st;
        if (stat(path, &st) != 0) {
            return 0;
        }
        return sinomo_glob_add(pglob, path, flags);
    }

    const char *slash = strchr(pattern, '/');
    size_t component_len = slash == NULL ? strlen(pattern) : (size_t)(slash - pattern);
    const char *rest = slash == NULL ? "" : slash + 1;
    char component[1024];
    if (component_len == 0 || component_len >= sizeof(component)) {
        return GLOB_NOSPACE;
    }
    memcpy(component, pattern, component_len);
    component[component_len] = 0;

    if (!sinomo_glob_has_magic(component, component_len, flags)) {
        char next[4096];
        if (sinomo_glob_join(next, sizeof(next), prefix, component) != 0) {
            return GLOB_NOSPACE;
        }
        struct stat st;
        if (stat(next, &st) != 0) {
            return 0;
        }
        if (*rest != 0) {
            if (!S_ISDIR(st.st_mode)) {
                return 0;
            }
            return sinomo_glob_walk(next, rest, flags, errfunc, pglob);
        }
        return sinomo_glob_add(pglob, next, flags);
    }

    const char *dir_path = prefix[0] == 0 ? "." : prefix;
    DIR *dir = opendir(dir_path);
    if (dir == NULL) {
        return sinomo_glob_error(dir_path, errno, flags, errfunc);
    }

    int fnm_flags = 0;
    if ((flags & GLOB_NOESCAPE) != 0) {
        fnm_flags |= FNM_NOESCAPE;
    }
    if ((flags & GLOB_PERIOD) == 0) {
        fnm_flags |= FNM_PERIOD;
    }

    int result = 0;
    for (;;) {
        errno = 0;
        struct dirent *entry = readdir(dir);
        if (entry == NULL) {
            if (errno != 0) {
                result = sinomo_glob_error(dir_path, errno, flags, errfunc);
            }
            break;
        }
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        if (__wrap_fnmatch(component, entry->d_name, fnm_flags) != 0) {
            continue;
        }
        char next[4096];
        if (sinomo_glob_join(next, sizeof(next), prefix, entry->d_name) != 0) {
            result = GLOB_NOSPACE;
            break;
        }
        if (*rest != 0) {
            struct stat st;
            if (stat(next, &st) == 0 && S_ISDIR(st.st_mode)) {
                result = sinomo_glob_walk(next, rest, flags, errfunc, pglob);
            }
        } else {
            result = sinomo_glob_add(pglob, next, flags);
        }
        if (result != 0) {
            break;
        }
    }
    closedir(dir);
    return result;
}

static int sinomo_glob_compare(const void *left, const void *right) {
    const char *const *left_path = (const char *const *)left;
    const char *const *right_path = (const char *const *)right;
    return strcmp(*left_path, *right_path);
}

int __wrap_glob(const char *pattern, int flags, int (*errfunc)(const char *, int), glob_t *pglob) {
    if (pattern == NULL || pglob == NULL) {
        errno = EFAULT;
        return GLOB_ABORTED;
    }
    int supported = GLOB_ERR | GLOB_MARK | GLOB_NOSORT | GLOB_DOOFFS | GLOB_NOCHECK | GLOB_APPEND | GLOB_NOESCAPE | GLOB_PERIOD;
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return GLOB_ABORTED;
    }
    if ((flags & GLOB_APPEND) == 0) {
        pglob->gl_pathc = 0;
        pglob->gl_pathv = NULL;
        if ((flags & GLOB_DOOFFS) == 0) {
            pglob->gl_offs = 0;
        }
    }

    size_t start_count = pglob->gl_pathc;
    char prefix[4096] = "";
    const char *rest = pattern;
    if (pattern[0] == '/') {
        snprintf(prefix, sizeof(prefix), "/");
        while (*rest == '/') {
            rest++;
        }
    }

    int result = sinomo_glob_walk(prefix, rest, flags, errfunc, pglob);
    if (result != 0) {
        return result;
    }
    if (pglob->gl_pathc == start_count) {
        if ((flags & GLOB_NOCHECK) == 0) {
            return GLOB_NOMATCH;
        }
        result = sinomo_glob_add(pglob, pattern, flags);
        if (result != 0) {
            return result;
        }
    }
    if ((flags & GLOB_NOSORT) == 0 && pglob->gl_pathc > 1) {
        size_t offset = (flags & GLOB_DOOFFS) != 0 ? pglob->gl_offs : 0;
        qsort(pglob->gl_pathv + offset, pglob->gl_pathc, sizeof(char *), sinomo_glob_compare);
    }
    return 0;
}

void __wrap_globfree(glob_t *pglob) {
    if (pglob == NULL || pglob->gl_pathv == NULL) {
        return;
    }
    size_t offset = pglob->gl_offs;
    for (size_t index = 0; index < pglob->gl_pathc; index++) {
        free(pglob->gl_pathv[offset + index]);
    }
    free(pglob->gl_pathv);
    pglob->gl_pathv = NULL;
    pglob->gl_pathc = 0;
}

static void sinomo_verr_print(int include_errno, const char *fmt, va_list ap) {
    int saved_errno = errno;
    fputs("sinomo-sandbox", stderr);
    if (fmt != NULL && fmt[0] != 0) {
        fputs(": ", stderr);
        vfprintf(stderr, fmt, ap);
    }
    if (include_errno) {
        fputs(": ", stderr);
        fputs(strerror(saved_errno), stderr);
    }
    fputc('\n', stderr);
}

void __wrap_vwarn(const char *fmt, va_list ap) {
    sinomo_verr_print(1, fmt, ap);
}

void __wrap_warn(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    __wrap_vwarn(fmt, ap);
    va_end(ap);
}

void __wrap_vwarnx(const char *fmt, va_list ap) {
    sinomo_verr_print(0, fmt, ap);
}

void __wrap_warnx(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    __wrap_vwarnx(fmt, ap);
    va_end(ap);
}

_Noreturn void __wrap_verr(int eval, const char *fmt, va_list ap) {
    sinomo_verr_print(1, fmt, ap);
    exit(eval);
}

_Noreturn void __wrap_err(int eval, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    __wrap_verr(eval, fmt, ap);
}

_Noreturn void __wrap_verrx(int eval, const char *fmt, va_list ap) {
    sinomo_verr_print(0, fmt, ap);
    exit(eval);
}

_Noreturn void __wrap_errx(int eval, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    __wrap_verrx(eval, fmt, ap);
}

struct __tab {
    size_t capacity;
    size_t len;
    ENTRY *entries;
    unsigned char *occupied;
};

#ifndef _GNU_SOURCE
struct hsearch_data {
    struct __tab *__tab;
    unsigned int __unused1;
    unsigned int __unused2;
};

struct qelem {
    struct qelem *q_forw;
    struct qelem *q_back;
    char q_data[1];
};
#endif

static struct hsearch_data sinomo_global_hsearch;
void __wrap_hdestroy_r(struct hsearch_data *htab);

static unsigned long long sinomo_hash_string(const char *key) {
    unsigned long long hash = 1469598103934665603ULL;
    while (*key != 0) {
        hash ^= (unsigned char)*key++;
        hash *= 1099511628211ULL;
    }
    return hash;
}

int __wrap_hcreate_r(size_t nel, struct hsearch_data *htab) {
    if (htab == NULL) {
        errno = EFAULT;
        return 0;
    }
    if (nel == 0) {
        nel = 1;
    }
    if (nel > SIZE_MAX / 2 || nel * 2 > SIZE_MAX / sizeof(ENTRY)) {
        errno = ENOMEM;
        return 0;
    }
    struct __tab *tab = calloc(1, sizeof(*tab));
    if (tab == NULL) {
        errno = ENOMEM;
        return 0;
    }
    tab->capacity = nel * 2 + 1;
    tab->entries = calloc(tab->capacity, sizeof(ENTRY));
    tab->occupied = calloc(tab->capacity, 1);
    if (tab->entries == NULL || tab->occupied == NULL) {
        free(tab->entries);
        free(tab->occupied);
        free(tab);
        errno = ENOMEM;
        return 0;
    }
    if (htab->__tab != NULL) {
        __wrap_hdestroy_r(htab);
    }
    htab->__tab = tab;
    return 1;
}

void __wrap_hdestroy_r(struct hsearch_data *htab) {
    if (htab == NULL || htab->__tab == NULL) {
        return;
    }
    free(htab->__tab->entries);
    free(htab->__tab->occupied);
    free(htab->__tab);
    htab->__tab = NULL;
}

int __wrap_hsearch_r(ENTRY item, ACTION action, ENTRY **retval, struct hsearch_data *htab) {
    if (retval == NULL || htab == NULL || htab->__tab == NULL || item.key == NULL) {
        errno = EINVAL;
        return 0;
    }
    struct __tab *tab = htab->__tab;
    unsigned long long hash = sinomo_hash_string(item.key);
    size_t first_empty = SIZE_MAX;
    for (size_t probe = 0; probe < tab->capacity; probe++) {
        size_t index = (hash + probe) % tab->capacity;
        if (!tab->occupied[index]) {
            first_empty = index;
            break;
        }
        if (strcmp(tab->entries[index].key, item.key) == 0) {
            *retval = &tab->entries[index];
            return 1;
        }
    }
    if (action == FIND || first_empty == SIZE_MAX || tab->len >= tab->capacity) {
        errno = ESRCH;
        *retval = NULL;
        return 0;
    }
    tab->occupied[first_empty] = 1;
    tab->entries[first_empty] = item;
    tab->len++;
    *retval = &tab->entries[first_empty];
    return 1;
}

int __wrap_hcreate(size_t nel) {
    return __wrap_hcreate_r(nel, &sinomo_global_hsearch);
}

void __wrap_hdestroy(void) {
    __wrap_hdestroy_r(&sinomo_global_hsearch);
}

ENTRY *__wrap_hsearch(ENTRY item, ACTION action) {
    ENTRY *entry = NULL;
    if (__wrap_hsearch_r(item, action, &entry, &sinomo_global_hsearch) == 0) {
        return NULL;
    }
    return entry;
}

void *__wrap_lfind(const void *key, const void *base, size_t *nmemb, size_t size, int (*compar)(const void *, const void *)) {
    if (key == NULL || base == NULL || nmemb == NULL || compar == NULL || size == 0) {
        errno = EINVAL;
        return NULL;
    }
    const unsigned char *cursor = (const unsigned char *)base;
    for (size_t index = 0; index < *nmemb; index++) {
        void *entry = (void *)(cursor + index * size);
        if (compar(key, entry) == 0) {
            return entry;
        }
    }
    return NULL;
}

void *__wrap_lsearch(const void *key, void *base, size_t *nmemb, size_t size, int (*compar)(const void *, const void *)) {
    void *found = __wrap_lfind(key, base, nmemb, size, compar);
    if (found != NULL) {
        return found;
    }
    if (key == NULL || base == NULL || nmemb == NULL || size == 0) {
        errno = EINVAL;
        return NULL;
    }
    unsigned char *slot = (unsigned char *)base + (*nmemb * size);
    memcpy(slot, key, size);
    (*nmemb)++;
    return slot;
}

void __wrap_insque(void *elem, void *prev) {
    struct qelem *entry = (struct qelem *)elem;
    struct qelem *previous = (struct qelem *)prev;
    if (entry == NULL) {
        return;
    }
    if (previous == NULL) {
        entry->q_forw = NULL;
        entry->q_back = NULL;
        return;
    }
    entry->q_forw = previous->q_forw;
    entry->q_back = previous;
    if (previous->q_forw != NULL) {
        previous->q_forw->q_back = entry;
    }
    previous->q_forw = entry;
}

void __wrap_remque(void *elem) {
    struct qelem *entry = (struct qelem *)elem;
    if (entry == NULL) {
        return;
    }
    if (entry->q_back != NULL) {
        entry->q_back->q_forw = entry->q_forw;
    }
    if (entry->q_forw != NULL) {
        entry->q_forw->q_back = entry->q_back;
    }
    entry->q_forw = NULL;
    entry->q_back = NULL;
}

struct sinomo_tnode {
    const void *key;
    struct sinomo_tnode *left;
    struct sinomo_tnode *right;
};

void *__wrap_tfind(const void *key, void *const *rootp, int (*compar)(const void *, const void *)) {
    if (key == NULL || rootp == NULL || compar == NULL) {
        errno = EINVAL;
        return NULL;
    }
    struct sinomo_tnode *node = *(struct sinomo_tnode *const *)rootp;
    while (node != NULL) {
        int cmp = compar(key, node->key);
        if (cmp == 0) {
            return &node->key;
        }
        node = cmp < 0 ? node->left : node->right;
    }
    return NULL;
}

void *__wrap_tsearch(const void *key, void **rootp, int (*compar)(const void *, const void *)) {
    if (key == NULL || rootp == NULL || compar == NULL) {
        errno = EINVAL;
        return NULL;
    }
    struct sinomo_tnode **slot = (struct sinomo_tnode **)rootp;
    while (*slot != NULL) {
        int cmp = compar(key, (*slot)->key);
        if (cmp == 0) {
            return &(*slot)->key;
        }
        slot = cmp < 0 ? &(*slot)->left : &(*slot)->right;
    }
    struct sinomo_tnode *node = calloc(1, sizeof(*node));
    if (node == NULL) {
        errno = ENOMEM;
        return NULL;
    }
    node->key = key;
    *slot = node;
    return &node->key;
}

static struct sinomo_tnode *sinomo_tdelete_min(struct sinomo_tnode **rootp) {
    struct sinomo_tnode **slot = rootp;
    while ((*slot)->left != NULL) {
        slot = &(*slot)->left;
    }
    struct sinomo_tnode *node = *slot;
    *slot = node->right;
    return node;
}

void *__wrap_tdelete(const void *key, void **rootp, int (*compar)(const void *, const void *)) {
    if (key == NULL || rootp == NULL || compar == NULL || *rootp == NULL) {
        errno = EINVAL;
        return NULL;
    }
    struct sinomo_tnode **slot = (struct sinomo_tnode **)rootp;
    struct sinomo_tnode *parent = NULL;
    while (*slot != NULL) {
        int cmp = compar(key, (*slot)->key);
        if (cmp == 0) {
            struct sinomo_tnode *node = *slot;
            if (node->left == NULL) {
                *slot = node->right;
            } else if (node->right == NULL) {
                *slot = node->left;
            } else {
                struct sinomo_tnode *replacement = sinomo_tdelete_min(&node->right);
                replacement->left = node->left;
                replacement->right = node->right;
                *slot = replacement;
            }
            free(node);
            return parent == NULL ? (void *)rootp : (void *)&parent->key;
        }
        parent = *slot;
        slot = cmp < 0 ? &(*slot)->left : &(*slot)->right;
    }
    return NULL;
}

static void sinomo_twalk_node(const struct sinomo_tnode *node, void (*action)(const void *, VISIT, int), int level) {
    if (node == NULL) {
        return;
    }
    if (node->left == NULL && node->right == NULL) {
        action(&node->key, leaf, level);
        return;
    }
    action(&node->key, preorder, level);
    sinomo_twalk_node(node->left, action, level + 1);
    action(&node->key, postorder, level);
    sinomo_twalk_node(node->right, action, level + 1);
    action(&node->key, endorder, level);
}

void __wrap_twalk(const void *root, void (*action)(const void *, VISIT, int)) {
    if (root == NULL || action == NULL) {
        return;
    }
    sinomo_twalk_node((const struct sinomo_tnode *)root, action, 0);
}

static void sinomo_tdestroy_node(struct sinomo_tnode *node, void (*free_node)(void *)) {
    if (node == NULL) {
        return;
    }
    sinomo_tdestroy_node(node->left, free_node);
    sinomo_tdestroy_node(node->right, free_node);
    if (free_node != NULL) {
        free_node((void *)node->key);
    }
    free(node);
}

void __wrap_tdestroy(void *root, void (*free_node)(void *)) {
    sinomo_tdestroy_node((struct sinomo_tnode *)root, free_node);
}

void __wrap__flushlbf(void) {
    fflush(NULL);
}

int __wrap___fsetlocking(FILE *stream, int type) {
    if (stream == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (type != FSETLOCKING_QUERY && type != FSETLOCKING_INTERNAL && type != FSETLOCKING_BYCALLER) {
        errno = EINVAL;
        return -1;
    }
    return FSETLOCKING_INTERNAL;
}

int __wrap___fwriting(FILE *stream) {
    if (stream == NULL) {
        errno = EFAULT;
        return 0;
    }
    return stream == stdout || stream == stderr;
}

int __wrap___freading(FILE *stream) {
    if (stream == NULL) {
        errno = EFAULT;
        return 0;
    }
    return stream == stdin;
}

int __wrap___freadable(FILE *stream) {
    if (stream == NULL) {
        errno = EFAULT;
        return 0;
    }
    return 1;
}

int __wrap___fwritable(FILE *stream) {
    if (stream == NULL) {
        errno = EFAULT;
        return 0;
    }
    return stream != stdin;
}

int __wrap___flbf(FILE *stream) {
    if (stream == NULL) {
        errno = EFAULT;
        return 0;
    }
    return __wrap_isatty(fileno(stream));
}

size_t __wrap___fbufsize(FILE *stream) {
    if (stream == NULL) {
        errno = EFAULT;
        return 0;
    }
    return BUFSIZ;
}

size_t __wrap___fpending(FILE *stream) {
    if (stream == NULL) {
        errno = EFAULT;
        return 0;
    }
    return 0;
}

int __wrap___fpurge(FILE *stream) {
    if (stream == NULL) {
        errno = EFAULT;
        return -1;
    }
    clearerr(stream);
    return 0;
}

size_t __wrap___freadahead(FILE *stream) {
    if (stream == NULL) {
        errno = EFAULT;
        return 0;
    }
    return 0;
}

const char *__wrap___freadptr(FILE *stream, size_t *sizep) {
    if (sizep != NULL) {
        *sizep = 0;
    }
    if (stream == NULL) {
        errno = EFAULT;
    }
    return NULL;
}

void __wrap___freadptrinc(FILE *stream, size_t increment) {
    (void)stream;
    (void)increment;
}

void __wrap___fseterr(FILE *stream) {
    if (stream != NULL) {
        errno = EIO;
    }
}

static char sinomo_syslog_ident[64] = "sinomo-sandbox";
static int sinomo_syslog_option = 0;
static int sinomo_syslog_facility = LOG_USER;
static int sinomo_syslog_mask = 0xff;

void __wrap_openlog(const char *ident, int option, int facility) {
    if (ident != NULL && ident[0] != 0) {
        snprintf(sinomo_syslog_ident, sizeof(sinomo_syslog_ident), "%s", ident);
    }
    sinomo_syslog_option = option;
    if ((facility & LOG_FACMASK) != 0) {
        sinomo_syslog_facility = facility & LOG_FACMASK;
    }
}

void __wrap_closelog(void) {
    snprintf(sinomo_syslog_ident, sizeof(sinomo_syslog_ident), "%s", "sinomo-sandbox");
    sinomo_syslog_option = 0;
    sinomo_syslog_facility = LOG_USER;
}

int __wrap_setlogmask(int maskpri) {
    int previous = sinomo_syslog_mask;
    if (maskpri != 0) {
        sinomo_syslog_mask = maskpri;
    }
    return previous;
}

void __wrap_vsyslog(int priority, const char *format, va_list ap) {
    int pri = LOG_PRI(priority);
    if ((LOG_MASK(pri) & sinomo_syslog_mask) == 0) {
        return;
    }
    int facility = priority & LOG_FACMASK;
    if (facility == 0) {
        facility = sinomo_syslog_facility;
    }
    fprintf(stderr, "%s[%ld]: <%d.%d> ", sinomo_syslog_ident, (long)__wrap_getpid(), LOG_FAC(facility), pri);
    if (format != NULL) {
        vfprintf(stderr, format, ap);
    }
    fputc('\n', stderr);
    if ((sinomo_syslog_option & LOG_PERROR) != 0 && stderr != stdout) {
        fflush(stderr);
    }
}

void __wrap_syslog(int priority, const char *format, ...) {
    va_list ap;
    va_start(ap, format);
    __wrap_vsyslog(priority, format, ap);
    va_end(ap);
}

char *__wrap_strchrnul(const char *s, int c) {
    unsigned char needle = (unsigned char)c;
    while (*s != 0 && (unsigned char)*s != needle) {
        s++;
    }
    return (char *)s;
}

void *__wrap_memmem(const void *haystack, size_t haystack_len, const void *needle, size_t needle_len) {
    if (needle_len == 0) {
        return (void *)haystack;
    }
    if (haystack_len < needle_len) {
        return NULL;
    }
    const unsigned char *hay = (const unsigned char *)haystack;
    const unsigned char *need = (const unsigned char *)needle;
    for (size_t index = 0; index <= haystack_len - needle_len; index++) {
        if (hay[index] == need[0] && memcmp(hay + index, need, needle_len) == 0) {
            return (void *)(hay + index);
        }
    }
    return NULL;
}

char *__wrap_setlocale(int category, const char *locale) {
    static char current[] = "C.UTF-8";
    if (category < LC_CTYPE || category > LC_ALL) {
        errno = EINVAL;
        return NULL;
    }
    if (locale == NULL || locale[0] == 0) {
        return current;
    }
    if (
        strcmp(locale, "C") == 0
        || strcmp(locale, "POSIX") == 0
        || strcmp(locale, "C.UTF-8") == 0
        || strcmp(locale, "C.utf8") == 0
        || strcmp(locale, "en_US.UTF-8") == 0
    ) {
        strcpy(current, strcmp(locale, "C") == 0 || strcmp(locale, "POSIX") == 0 ? "C" : "C.UTF-8");
        return current;
    }
    errno = EINVAL;
    return NULL;
}

struct lconv *__wrap_localeconv(void) {
    static struct lconv conv = {
        .decimal_point = ".",
        .thousands_sep = "",
        .grouping = "",
        .int_curr_symbol = "",
        .currency_symbol = "",
        .mon_decimal_point = "",
        .mon_thousands_sep = "",
        .mon_grouping = "",
        .positive_sign = "",
        .negative_sign = "",
        .int_frac_digits = CHAR_MAX,
        .frac_digits = CHAR_MAX,
        .p_cs_precedes = CHAR_MAX,
        .p_sep_by_space = CHAR_MAX,
        .n_cs_precedes = CHAR_MAX,
        .n_sep_by_space = CHAR_MAX,
        .p_sign_posn = CHAR_MAX,
        .n_sign_posn = CHAR_MAX,
        .int_p_cs_precedes = CHAR_MAX,
        .int_p_sep_by_space = CHAR_MAX,
        .int_n_cs_precedes = CHAR_MAX,
        .int_n_sep_by_space = CHAR_MAX,
        .int_p_sign_posn = CHAR_MAX,
        .int_n_sign_posn = CHAR_MAX,
    };
    return &conv;
}

static int sinomo_locale_name_supported(const char *locale) {
    return locale == NULL
        || locale[0] == 0
        || strcmp(locale, "C") == 0
        || strcmp(locale, "POSIX") == 0
        || strcmp(locale, "C.UTF-8") == 0
        || strcmp(locale, "C.utf8") == 0
        || strcmp(locale, "en_US.UTF-8") == 0;
}

locale_t __wrap_newlocale(int mask, const char *locale, locale_t base) {
    (void)base;
    if (mask < 0 || !sinomo_locale_name_supported(locale)) {
        errno = EINVAL;
        return (locale_t)0;
    }
    return LC_GLOBAL_LOCALE;
}

locale_t __wrap_duplocale(locale_t locale) {
    if (locale == (locale_t)0) {
        errno = EINVAL;
        return (locale_t)0;
    }
    return LC_GLOBAL_LOCALE;
}

void __wrap_freelocale(locale_t locale) {
    (void)locale;
}

locale_t __wrap_uselocale(locale_t locale) {
    (void)locale;
    return LC_GLOBAL_LOCALE;
}

char *__wrap_nl_langinfo(nl_item item) {
    switch (item) {
        case CODESET: return "UTF-8";
        case RADIXCHAR: return ".";
        case THOUSEP: return "";
        case YESEXPR: return "^[yY]";
        case NOEXPR: return "^[nN]";
#ifdef YESSTR
        case YESSTR: return "yes";
#endif
#ifdef NOSTR
        case NOSTR: return "no";
#endif
        case CRNCYSTR: return "";
        case AM_STR: return "AM";
        case PM_STR: return "PM";
        case D_T_FMT: return "%a %b %e %H:%M:%S %Y";
        case D_FMT: return "%m/%d/%y";
        case T_FMT: return "%H:%M:%S";
        case T_FMT_AMPM: return "%I:%M:%S %p";
        case ABDAY_1: return "Sun";
        case ABDAY_2: return "Mon";
        case ABDAY_3: return "Tue";
        case ABDAY_4: return "Wed";
        case ABDAY_5: return "Thu";
        case ABDAY_6: return "Fri";
        case ABDAY_7: return "Sat";
        case DAY_1: return "Sunday";
        case DAY_2: return "Monday";
        case DAY_3: return "Tuesday";
        case DAY_4: return "Wednesday";
        case DAY_5: return "Thursday";
        case DAY_6: return "Friday";
        case DAY_7: return "Saturday";
        case ABMON_1: return "Jan";
        case ABMON_2: return "Feb";
        case ABMON_3: return "Mar";
        case ABMON_4: return "Apr";
        case ABMON_5: return "May";
        case ABMON_6: return "Jun";
        case ABMON_7: return "Jul";
        case ABMON_8: return "Aug";
        case ABMON_9: return "Sep";
        case ABMON_10: return "Oct";
        case ABMON_11: return "Nov";
        case ABMON_12: return "Dec";
        case MON_1: return "January";
        case MON_2: return "February";
        case MON_3: return "March";
        case MON_4: return "April";
        case MON_5: return "May";
        case MON_6: return "June";
        case MON_7: return "July";
        case MON_8: return "August";
        case MON_9: return "September";
        case MON_10: return "October";
        case MON_11: return "November";
        case MON_12: return "December";
        case ERA:
        case ERA_D_FMT:
        case ERA_D_T_FMT:
        case ERA_T_FMT:
        case ALT_DIGITS:
            return "";
        default:
#ifdef NL_LOCALE_NAME
            if (item == NL_LOCALE_NAME(LC_CTYPE)
                || item == NL_LOCALE_NAME(LC_NUMERIC)
                || item == NL_LOCALE_NAME(LC_TIME)
                || item == NL_LOCALE_NAME(LC_COLLATE)
                || item == NL_LOCALE_NAME(LC_MONETARY)
                || item == NL_LOCALE_NAME(LC_MESSAGES)
                || item == NL_LOCALE_NAME(LC_ALL)) {
                return "C.UTF-8";
            }
#endif
            return "";
    }
}

char *__wrap_nl_langinfo_l(nl_item item, locale_t locale) {
    (void)locale;
    return __wrap_nl_langinfo(item);
}

int __wrap_wcscoll(const wchar_t *left, const wchar_t *right) {
    return wcscmp(left, right);
}

size_t __wrap_wcsxfrm(wchar_t *dest, const wchar_t *src, size_t n) {
    size_t len = wcslen(src);
    if (n != 0 && dest != NULL) {
        size_t copy_len = len >= n ? n - 1 : len;
        wmemcpy(dest, src, copy_len);
        dest[copy_len] = 0;
    }
    return len;
}

int __wrap_wcscoll_l(const wchar_t *left, const wchar_t *right, locale_t locale) {
    (void)locale;
    return __wrap_wcscoll(left, right);
}

size_t __wrap_wcsxfrm_l(wchar_t *dest, const wchar_t *src, size_t n, locale_t locale) {
    (void)locale;
    return __wrap_wcsxfrm(dest, src, n);
}

wchar_t *__wrap_wcsdup(const wchar_t *src) {
    if (src == NULL) {
        errno = EFAULT;
        return NULL;
    }
    size_t len = wcslen(src);
    if (len > (SIZE_MAX / sizeof(wchar_t)) - 1) {
        errno = EOVERFLOW;
        return NULL;
    }
    wchar_t *copy = malloc((len + 1) * sizeof(wchar_t));
    if (copy == NULL) {
        errno = ENOMEM;
        return NULL;
    }
    wmemcpy(copy, src, len + 1);
    return copy;
}

size_t __wrap_wcsnlen(const wchar_t *src, size_t maxlen) {
    if (src == NULL) {
        return 0;
    }
    size_t len = 0;
    while (len < maxlen && src[len] != 0) {
        len++;
    }
    return len;
}

wchar_t *__wrap_wcpcpy(wchar_t *dest, const wchar_t *src) {
    while ((*dest = *src) != 0) {
        dest++;
        src++;
    }
    return dest;
}

wchar_t *__wrap_wcpncpy(wchar_t *dest, const wchar_t *src, size_t n) {
    wchar_t *out = dest;
    while (n > 0 && *src != 0) {
        *out++ = *src++;
        n--;
    }
    while (n > 0) {
        *out++ = 0;
        n--;
    }
    return out;
}

static int sinomo_wide_casecmp(wchar_t left, wchar_t right) {
    wint_t lower_left = towlower((wint_t)left);
    wint_t lower_right = towlower((wint_t)right);
    if (lower_left < lower_right) {
        return -1;
    }
    if (lower_left > lower_right) {
        return 1;
    }
    return 0;
}

int __wrap_wcscasecmp(const wchar_t *left, const wchar_t *right) {
    for (;;) {
        int cmp = sinomo_wide_casecmp(*left, *right);
        if (cmp != 0 || *left == 0 || *right == 0) {
            return cmp;
        }
        left++;
        right++;
    }
}

int __wrap_wcsncasecmp(const wchar_t *left, const wchar_t *right, size_t n) {
    for (size_t index = 0; index < n; index++) {
        int cmp = sinomo_wide_casecmp(left[index], right[index]);
        if (cmp != 0 || left[index] == 0 || right[index] == 0) {
            return cmp;
        }
    }
    return 0;
}

int __wrap_wcscasecmp_l(const wchar_t *left, const wchar_t *right, locale_t locale) {
    (void)locale;
    return __wrap_wcscasecmp(left, right);
}

int __wrap_wcsncasecmp_l(const wchar_t *left, const wchar_t *right, size_t n, locale_t locale) {
    (void)locale;
    return __wrap_wcsncasecmp(left, right, n);
}

static int sinomo_wide_range(uint32_t value) {
    return (value >= 0x1100U && (
        value <= 0x115fU
        || value == 0x2329U
        || value == 0x232aU
        || (value >= 0x2e80U && value <= 0xa4cfU && value != 0x303fU)
        || (value >= 0xac00U && value <= 0xd7a3U)
        || (value >= 0xf900U && value <= 0xfaffU)
        || (value >= 0xfe10U && value <= 0xfe19U)
        || (value >= 0xfe30U && value <= 0xfe6fU)
        || (value >= 0xff00U && value <= 0xff60U)
        || (value >= 0xffe0U && value <= 0xffe6U)
        || (value >= 0x1f300U && value <= 0x1f64fU)
        || (value >= 0x1f900U && value <= 0x1f9ffU)
        || (value >= 0x20000U && value <= 0x3fffdU)
    ));
}

static int sinomo_combining_range(uint32_t value) {
    return (value >= 0x0300U && (
        value <= 0x036fU
        || (value >= 0x0483U && value <= 0x0489U)
        || (value >= 0x0591U && value <= 0x05bdU)
        || value == 0x05bfU
        || (value >= 0x05c1U && value <= 0x05c2U)
        || (value >= 0x05c4U && value <= 0x05c5U)
        || value == 0x05c7U
        || (value >= 0x0610U && value <= 0x061aU)
        || (value >= 0x064bU && value <= 0x065fU)
        || value == 0x0670U
        || (value >= 0x06d6U && value <= 0x06dcU)
        || (value >= 0x06dfU && value <= 0x06e4U)
        || (value >= 0x06e7U && value <= 0x06e8U)
        || (value >= 0x06eaU && value <= 0x06edU)
        || (value >= 0x0711U && value <= 0x074aU)
        || (value >= 0x07a6U && value <= 0x07b0U)
        || (value >= 0x07ebU && value <= 0x07f3U)
        || (value >= 0x0816U && value <= 0x082dU)
        || (value >= 0x0859U && value <= 0x085bU)
        || (value >= 0x08d3U && value <= 0x08ffU)
        || (value >= 0x0900U && value <= 0x0dcaU)
        || (value >= 0x0e31U && value <= 0x0e4eU)
        || (value >= 0x0eb1U && value <= 0x0ecdU)
        || (value >= 0x0f18U && value <= 0x0f35U)
        || (value >= 0x0f37U && value <= 0x0f39U)
        || (value >= 0x0f3eU && value <= 0x0f87U)
        || (value >= 0x0f8dU && value <= 0x0fbcU)
        || value == 0x0fc6U
        || (value >= 0x102bU && value <= 0x103eU)
        || (value >= 0x1056U && value <= 0x1059U)
        || (value >= 0x105eU && value <= 0x1060U)
        || (value >= 0x1062U && value <= 0x1064U)
        || (value >= 0x1067U && value <= 0x106dU)
        || (value >= 0x1071U && value <= 0x108dU)
        || value == 0x108fU
        || (value >= 0x109aU && value <= 0x109dU)
        || (value >= 0x135dU && value <= 0x135fU)
        || (value >= 0x1712U && value <= 0x1734U)
        || (value >= 0x1752U && value <= 0x1753U)
        || (value >= 0x1772U && value <= 0x1773U)
        || (value >= 0x17b4U && value <= 0x17d3U)
        || value == 0x17ddU
        || (value >= 0x180bU && value <= 0x180fU)
        || (value >= 0x1885U && value <= 0x1886U)
        || value == 0x18a9U
        || (value >= 0x1920U && value <= 0x192bU)
        || (value >= 0x1930U && value <= 0x193bU)
        || (value >= 0x1a17U && value <= 0x1a1bU)
        || (value >= 0x1a55U && value <= 0x1a7fU)
        || (value >= 0x1ab0U && value <= 0x1aceU)
        || (value >= 0x1b00U && value <= 0x1b73U)
        || (value >= 0x1b80U && value <= 0x1bf3U)
        || (value >= 0x1c24U && value <= 0x1c37U)
        || (value >= 0x1cd0U && value <= 0x1cd2U)
        || (value >= 0x1cd4U && value <= 0x1ce8U)
        || value == 0x1cedU
        || (value >= 0x1cf2U && value <= 0x1cf4U)
        || (value >= 0x1cf7U && value <= 0x1cf9U)
        || (value >= 0x1dc0U && value <= 0x1dffU)
        || (value >= 0x20d0U && value <= 0x20ffU)
        || (value >= 0x302aU && value <= 0x302fU)
        || (value >= 0x3099U && value <= 0x309aU)
        || (value >= 0xa66fU && value <= 0xa672U)
        || (value >= 0xa674U && value <= 0xa67dU)
        || (value >= 0xa69eU && value <= 0xa69fU)
        || (value >= 0xa6f0U && value <= 0xa6f1U)
        || (value >= 0xa802U && value <= 0xa827U)
        || (value >= 0xa880U && value <= 0xa8c5U)
        || (value >= 0xa8e0U && value <= 0xa8f1U)
        || (value >= 0xa8ffU && value <= 0xa92dU)
        || (value >= 0xa947U && value <= 0xa953U)
        || (value >= 0xa980U && value <= 0xa9c0U)
        || (value >= 0xa9b3U && value <= 0xa9c0U)
        || (value >= 0xaa29U && value <= 0xaa36U)
        || (value >= 0xaa43U && value <= 0xaa4dU)
        || (value >= 0xaa7bU && value <= 0xaa7dU)
        || (value >= 0xaab0U && value <= 0xaabfU)
        || (value >= 0xaac1U && value <= 0xaac2U)
        || (value >= 0xaaf5U && value <= 0xaaf6U)
        || (value >= 0xabe3U && value <= 0xabedU)
        || value == 0xfb1eU
        || (value >= 0xfe00U && value <= 0xfe0fU)
        || (value >= 0xfe20U && value <= 0xfe2fU)
        || (value >= 0x1d165U && value <= 0x1d169U)
        || (value >= 0x1d16dU && value <= 0x1d172U)
        || (value >= 0x1d17bU && value <= 0x1d182U)
        || (value >= 0x1d185U && value <= 0x1d18bU)
        || (value >= 0x1d1aaU && value <= 0x1d1adU)
        || (value >= 0xe0100U && value <= 0xe01efU)
    ));
}

int __wrap_wcwidth(wchar_t wc) {
    uint32_t value = (uint32_t)wc;
    if (value == 0) {
        return 0;
    }
    if (value < 32U || (value >= 0x7fU && value < 0xa0U)) {
        return -1;
    }
    if (value > 0x10ffffU || (value >= 0xd800U && value <= 0xdfffU)) {
        return -1;
    }
    if (sinomo_combining_range(value)) {
        return 0;
    }
    return sinomo_wide_range(value) ? 2 : 1;
}

int __wrap_wcswidth(const wchar_t *s, size_t n) {
    if (s == NULL) {
        errno = EFAULT;
        return -1;
    }
    int width = 0;
    for (size_t index = 0; index < n && s[index] != 0; index++) {
        int char_width = __wrap_wcwidth(s[index]);
        if (char_width < 0) {
            return -1;
        }
        if (width > INT_MAX - char_width) {
            errno = EOVERFLOW;
            return -1;
        }
        width += char_width;
    }
    return width;
}

static int sinomo_utf8_cont(unsigned char byte) {
    return (byte & 0xc0U) == 0x80U;
}

size_t __wrap_mbrtowc(wchar_t *pwc, const char *s, size_t n, mbstate_t *ps) {
    (void)ps;
    if (s == NULL) {
        return 0;
    }
    if (n == 0) {
        return (size_t)-2;
    }

    const unsigned char *bytes = (const unsigned char *)s;
    wchar_t value = 0;
    size_t needed = 0;

    if (bytes[0] == 0) {
        if (pwc != NULL) {
            *pwc = 0;
        }
        return 0;
    } else if (bytes[0] < 0x80U) {
        value = (wchar_t)bytes[0];
        needed = 1;
    } else if ((bytes[0] & 0xe0U) == 0xc0U) {
        if (bytes[0] < 0xc2U) {
            errno = EILSEQ;
            return (size_t)-1;
        }
        needed = 2;
        if (n < needed) {
            return (size_t)-2;
        }
        if (!sinomo_utf8_cont(bytes[1])) {
            errno = EILSEQ;
            return (size_t)-1;
        }
        value = (wchar_t)(((bytes[0] & 0x1fU) << 6) | (bytes[1] & 0x3fU));
    } else if ((bytes[0] & 0xf0U) == 0xe0U) {
        needed = 3;
        if (n < needed) {
            return (size_t)-2;
        }
        if (!sinomo_utf8_cont(bytes[1]) || !sinomo_utf8_cont(bytes[2])) {
            errno = EILSEQ;
            return (size_t)-1;
        }
        if (bytes[0] == 0xe0U && bytes[1] < 0xa0U) {
            errno = EILSEQ;
            return (size_t)-1;
        }
        if (bytes[0] == 0xedU && bytes[1] >= 0xa0U) {
            errno = EILSEQ;
            return (size_t)-1;
        }
        value = (wchar_t)(((bytes[0] & 0x0fU) << 12) | ((bytes[1] & 0x3fU) << 6) | (bytes[2] & 0x3fU));
    } else if ((bytes[0] & 0xf8U) == 0xf0U) {
        needed = 4;
        if (n < needed) {
            return (size_t)-2;
        }
        if (!sinomo_utf8_cont(bytes[1]) || !sinomo_utf8_cont(bytes[2]) || !sinomo_utf8_cont(bytes[3])) {
            errno = EILSEQ;
            return (size_t)-1;
        }
        if (bytes[0] == 0xf0U && bytes[1] < 0x90U) {
            errno = EILSEQ;
            return (size_t)-1;
        }
        if (bytes[0] > 0xf4U || (bytes[0] == 0xf4U && bytes[1] > 0x8fU)) {
            errno = EILSEQ;
            return (size_t)-1;
        }
        value = (wchar_t)(((bytes[0] & 0x07U) << 18) | ((bytes[1] & 0x3fU) << 12) | ((bytes[2] & 0x3fU) << 6) | (bytes[3] & 0x3fU));
    } else {
        errno = EILSEQ;
        return (size_t)-1;
    }

    if (pwc != NULL) {
        *pwc = value;
    }
    return needed;
}

size_t __wrap_wcrtomb(char *s, wchar_t wc, mbstate_t *ps) {
    (void)ps;
    if (s == NULL) {
        return 1;
    }
    uint32_t value = (uint32_t)wc;
    if (value < 0x80U) {
        s[0] = (char)value;
        return 1;
    }
    if (value < 0x800U) {
        s[0] = (char)(0xc0U | (value >> 6));
        s[1] = (char)(0x80U | (value & 0x3fU));
        return 2;
    }
    if (value >= 0xd800U && value <= 0xdfffU) {
        errno = EILSEQ;
        return (size_t)-1;
    }
    if (value < 0x10000U) {
        s[0] = (char)(0xe0U | (value >> 12));
        s[1] = (char)(0x80U | ((value >> 6) & 0x3fU));
        s[2] = (char)(0x80U | (value & 0x3fU));
        return 3;
    }
    if (value <= 0x10ffffU) {
        s[0] = (char)(0xf0U | (value >> 18));
        s[1] = (char)(0x80U | ((value >> 12) & 0x3fU));
        s[2] = (char)(0x80U | ((value >> 6) & 0x3fU));
        s[3] = (char)(0x80U | (value & 0x3fU));
        return 4;
    }
    errno = EILSEQ;
    return (size_t)-1;
}

wint_t __wrap_btowc(int c) {
    if (c == EOF) {
        return WEOF;
    }
    unsigned char byte = (unsigned char)c;
    if (byte < 0x80U) {
        return (wint_t)byte;
    }
    return WEOF;
}

int __wrap_wctob(wint_t c) {
    if (c >= 0 && c < 0x80) {
        return (int)c;
    }
    return EOF;
}

int __wrap_mbtowc(wchar_t *pwc, const char *s, size_t n) {
    if (s == NULL) {
        return 0;
    }
    size_t rc = __wrap_mbrtowc(pwc, s, n, NULL);
    if (rc == (size_t)-1 || rc == (size_t)-2) {
        return -1;
    }
    return (int)rc;
}

int __wrap_wctomb(char *s, wchar_t wc) {
    if (s == NULL) {
        return 0;
    }
    size_t rc = __wrap_wcrtomb(s, wc, NULL);
    if (rc == (size_t)-1) {
        return -1;
    }
    return (int)rc;
}

int __wrap_mbsinit(const mbstate_t *ps) {
    (void)ps;
    return 1;
}

size_t __wrap_mbrlen(const char *s, size_t n, mbstate_t *ps) {
    return __wrap_mbrtowc(NULL, s, n, ps);
}

size_t __wrap_mbsnrtowcs(wchar_t *dst, const char **src, size_t nms, size_t len, mbstate_t *ps) {
    if (src == NULL || *src == NULL) {
        errno = EFAULT;
        return (size_t)-1;
    }
    const char *cursor = *src;
    size_t remaining = nms;
    size_t written = 0;
    while (remaining > 0) {
        wchar_t wc = 0;
        size_t rc = __wrap_mbrtowc(&wc, cursor, remaining, ps);
        if (rc == (size_t)-1) {
            return (size_t)-1;
        }
        if (rc == (size_t)-2) {
            errno = EILSEQ;
            return (size_t)-1;
        }
        if (dst != NULL && written >= len) {
            *src = cursor;
            return written;
        }
        if (dst != NULL) {
            dst[written] = wc;
        }
        if (rc == 0) {
            *src = NULL;
            return written;
        }
        cursor += rc;
        remaining -= rc;
        written++;
    }
    *src = cursor;
    return written;
}

size_t __wrap_mbsrtowcs(wchar_t *dst, const char **src, size_t len, mbstate_t *ps) {
    if (src == NULL || *src == NULL) {
        errno = EFAULT;
        return (size_t)-1;
    }
    const char *cursor = *src;
    size_t written = 0;
    for (;;) {
        wchar_t wc = 0;
        size_t rc = __wrap_mbrtowc(&wc, cursor, MB_LEN_MAX, ps);
        if (rc == (size_t)-1 || rc == (size_t)-2) {
            if (rc == (size_t)-2) {
                errno = EILSEQ;
            }
            return (size_t)-1;
        }
        if (dst != NULL && written >= len) {
            *src = cursor;
            return written;
        }
        if (dst != NULL) {
            dst[written] = wc;
        }
        if (rc == 0) {
            *src = NULL;
            return written;
        }
        cursor += rc;
        written++;
    }
}

size_t __wrap_mbstowcs(wchar_t *dst, const char *src, size_t len) {
    if (src == NULL) {
        errno = EFAULT;
        return (size_t)-1;
    }
    const char *cursor = src;
    return __wrap_mbsrtowcs(dst, &cursor, len, NULL);
}

size_t __wrap_wcsnrtombs(char *dst, const wchar_t **src, size_t nwc, size_t len, mbstate_t *ps) {
    if (src == NULL || *src == NULL) {
        errno = EFAULT;
        return (size_t)-1;
    }
    const wchar_t *cursor = *src;
    size_t remaining = nwc;
    size_t written = 0;
    while (remaining > 0) {
        char encoded[MB_LEN_MAX];
        size_t rc = __wrap_wcrtomb(encoded, *cursor, ps);
        if (rc == (size_t)-1) {
            return (size_t)-1;
        }
        if (dst != NULL && written + rc > len) {
            *src = cursor;
            return written;
        }
        if (dst != NULL) {
            memcpy(dst + written, encoded, rc);
        }
        if (*cursor == 0) {
            *src = NULL;
            return written;
        }
        cursor++;
        remaining--;
        written += rc;
    }
    *src = cursor;
    return written;
}

size_t __wrap_wcsrtombs(char *dst, const wchar_t **src, size_t len, mbstate_t *ps) {
    if (src == NULL || *src == NULL) {
        errno = EFAULT;
        return (size_t)-1;
    }
    const wchar_t *cursor = *src;
    size_t written = 0;
    for (;;) {
        char encoded[MB_LEN_MAX];
        size_t rc = __wrap_wcrtomb(encoded, *cursor, ps);
        if (rc == (size_t)-1) {
            return (size_t)-1;
        }
        if (dst != NULL && written + rc > len) {
            *src = cursor;
            return written;
        }
        if (dst != NULL) {
            memcpy(dst + written, encoded, rc);
        }
        if (*cursor == 0) {
            *src = NULL;
            return written;
        }
        cursor++;
        written += rc;
    }
}

size_t __wrap_wcstombs(char *dst, const wchar_t *src, size_t len) {
    if (src == NULL) {
        errno = EFAULT;
        return (size_t)-1;
    }
    const wchar_t *cursor = src;
    return __wrap_wcsrtombs(dst, &cursor, len, NULL);
}

static char *sinomo_wide_ascii_dup(const wchar_t *input, wchar_t **endptr, char **narrow_end) {
    if (input == NULL) {
        errno = EFAULT;
        return NULL;
    }
    size_t len = wcslen(input);
    char *buffer = malloc(len + 1);
    if (buffer == NULL) {
        errno = ENOMEM;
        return NULL;
    }
    for (size_t index = 0; index < len; index++) {
        uint32_t value = (uint32_t)input[index];
        buffer[index] = value <= 0x7fU ? (char)value : '?';
    }
    buffer[len] = 0;
    if (endptr != NULL) {
        *endptr = (wchar_t *)input;
    }
    if (narrow_end != NULL) {
        *narrow_end = buffer;
    }
    return buffer;
}

static void sinomo_update_wide_end(const wchar_t *input, wchar_t **endptr, char *buffer, char *narrow_end) {
    if (endptr != NULL) {
        *endptr = (wchar_t *)input + (narrow_end - buffer);
    }
}

float __wrap_wcstof(const wchar_t *nptr, wchar_t **endptr) {
    char *narrow_end = NULL;
    char *buffer = sinomo_wide_ascii_dup(nptr, endptr, &narrow_end);
    if (buffer == NULL) {
        return 0.0f;
    }
    float value = strtof(buffer, &narrow_end);
    sinomo_update_wide_end(nptr, endptr, buffer, narrow_end);
    free(buffer);
    return value;
}

double __wrap_wcstod(const wchar_t *nptr, wchar_t **endptr) {
    char *narrow_end = NULL;
    char *buffer = sinomo_wide_ascii_dup(nptr, endptr, &narrow_end);
    if (buffer == NULL) {
        return 0.0;
    }
    double value = strtod(buffer, &narrow_end);
    sinomo_update_wide_end(nptr, endptr, buffer, narrow_end);
    free(buffer);
    return value;
}

long double __wrap_wcstold(const wchar_t *nptr, wchar_t **endptr) {
    char *narrow_end = NULL;
    char *buffer = sinomo_wide_ascii_dup(nptr, endptr, &narrow_end);
    if (buffer == NULL) {
        return 0.0L;
    }
    long double value = strtold(buffer, &narrow_end);
    sinomo_update_wide_end(nptr, endptr, buffer, narrow_end);
    free(buffer);
    return value;
}

long __wrap_wcstol(const wchar_t *nptr, wchar_t **endptr, int base) {
    char *narrow_end = NULL;
    char *buffer = sinomo_wide_ascii_dup(nptr, endptr, &narrow_end);
    if (buffer == NULL) {
        return 0;
    }
    long value = strtol(buffer, &narrow_end, base);
    sinomo_update_wide_end(nptr, endptr, buffer, narrow_end);
    free(buffer);
    return value;
}

unsigned long __wrap_wcstoul(const wchar_t *nptr, wchar_t **endptr, int base) {
    char *narrow_end = NULL;
    char *buffer = sinomo_wide_ascii_dup(nptr, endptr, &narrow_end);
    if (buffer == NULL) {
        return 0;
    }
    unsigned long value = strtoul(buffer, &narrow_end, base);
    sinomo_update_wide_end(nptr, endptr, buffer, narrow_end);
    free(buffer);
    return value;
}

long long __wrap_wcstoll(const wchar_t *nptr, wchar_t **endptr, int base) {
    char *narrow_end = NULL;
    char *buffer = sinomo_wide_ascii_dup(nptr, endptr, &narrow_end);
    if (buffer == NULL) {
        return 0;
    }
    long long value = strtoll(buffer, &narrow_end, base);
    sinomo_update_wide_end(nptr, endptr, buffer, narrow_end);
    free(buffer);
    return value;
}

unsigned long long __wrap_wcstoull(const wchar_t *nptr, wchar_t **endptr, int base) {
    char *narrow_end = NULL;
    char *buffer = sinomo_wide_ascii_dup(nptr, endptr, &narrow_end);
    if (buffer == NULL) {
        return 0;
    }
    unsigned long long value = strtoull(buffer, &narrow_end, base);
    sinomo_update_wide_end(nptr, endptr, buffer, narrow_end);
    free(buffer);
    return value;
}

size_t __wrap_wcsftime(wchar_t *s, size_t maxsize, const wchar_t *format, const struct tm *tm) {
    if (s == NULL && maxsize > 0) {
        errno = EFAULT;
        return 0;
    }
    if (format == NULL || tm == NULL) {
        errno = EFAULT;
        return 0;
    }

    const wchar_t *format_cursor = format;
    size_t format_bytes_len = __wrap_wcsrtombs(NULL, &format_cursor, 0, NULL);
    if (format_bytes_len == (size_t)-1) {
        return 0;
    }
    char *format_bytes = malloc(format_bytes_len + 1);
    if (format_bytes == NULL) {
        errno = ENOMEM;
        return 0;
    }
    format_cursor = format;
    if (__wrap_wcsrtombs(format_bytes, &format_cursor, format_bytes_len + 1, NULL) == (size_t)-1) {
        free(format_bytes);
        return 0;
    }

    if (maxsize > (SIZE_MAX - 1) / MB_LEN_MAX) {
        free(format_bytes);
        errno = EOVERFLOW;
        return 0;
    }
    size_t output_bytes_len = maxsize == 0 ? 1 : (maxsize * MB_LEN_MAX) + 1;
    char *output_bytes = malloc(output_bytes_len);
    if (output_bytes == NULL) {
        free(format_bytes);
        errno = ENOMEM;
        return 0;
    }
    size_t formatted = strftime(output_bytes, output_bytes_len, format_bytes, tm);
    free(format_bytes);
    if (formatted == 0) {
        free(output_bytes);
        return 0;
    }

    const char *output_cursor = output_bytes;
    size_t wide_len = __wrap_mbsrtowcs(s, &output_cursor, maxsize, NULL);
    free(output_bytes);
    if (wide_len == (size_t)-1) {
        return 0;
    }
    return wide_len;
}

size_t __wrap_wcsftime_l(wchar_t *s, size_t maxsize, const wchar_t *format, const struct tm *tm, locale_t locale) {
    (void)locale;
    return __wrap_wcsftime(s, maxsize, format, tm);
}

wint_t __wrap_fgetwc(FILE *stream) {
    if (stream == NULL) {
        errno = EFAULT;
        return WEOF;
    }
    char bytes[MB_LEN_MAX];
    size_t len = 0;
    while (len < sizeof(bytes)) {
        int c = fgetc(stream);
        if (c == EOF) {
            if (len == 0) {
                return WEOF;
            }
            errno = EILSEQ;
            return WEOF;
        }
        bytes[len++] = (char)c;
        wchar_t wc = 0;
        size_t rc = __wrap_mbrtowc(&wc, bytes, len, NULL);
        if (rc == (size_t)-1) {
            return WEOF;
        }
        if (rc != (size_t)-2) {
            return (wint_t)wc;
        }
    }
    errno = EILSEQ;
    return WEOF;
}

wint_t __wrap_getwc(FILE *stream) {
    return __wrap_fgetwc(stream);
}

wint_t __wrap_getwchar(void) {
    return __wrap_fgetwc(stdin);
}

wint_t __wrap_fputwc(wchar_t wc, FILE *stream) {
    if (stream == NULL) {
        errno = EFAULT;
        return WEOF;
    }
    char bytes[MB_LEN_MAX];
    size_t len = __wrap_wcrtomb(bytes, wc, NULL);
    if (len == (size_t)-1) {
        return WEOF;
    }
    if (fwrite(bytes, 1, len, stream) != len) {
        return WEOF;
    }
    return (wint_t)wc;
}

wint_t __wrap_putwc(wchar_t wc, FILE *stream) {
    return __wrap_fputwc(wc, stream);
}

wint_t __wrap_putwchar(wchar_t wc) {
    return __wrap_fputwc(wc, stdout);
}

wchar_t *__wrap_fgetws(wchar_t *ws, int n, FILE *stream) {
    if (ws == NULL || stream == NULL) {
        errno = EFAULT;
        return NULL;
    }
    if (n <= 0) {
        errno = EINVAL;
        return NULL;
    }
    int count = 0;
    while (count < n - 1) {
        wint_t wc = __wrap_fgetwc(stream);
        if (wc == WEOF) {
            break;
        }
        ws[count++] = (wchar_t)wc;
        if (wc == L'\n') {
            break;
        }
    }
    if (count == 0) {
        return NULL;
    }
    ws[count] = 0;
    return ws;
}

int __wrap_fputws(const wchar_t *ws, FILE *stream) {
    if (ws == NULL || stream == NULL) {
        errno = EFAULT;
        return -1;
    }
    for (const wchar_t *cursor = ws; *cursor != 0; cursor++) {
        if (__wrap_fputwc(*cursor, stream) == WEOF) {
            return -1;
        }
    }
    return 0;
}

wint_t __wrap_ungetwc(wint_t wc, FILE *stream) {
    if (stream == NULL) {
        errno = EFAULT;
        return WEOF;
    }
    if (wc == WEOF) {
        return WEOF;
    }
    char bytes[MB_LEN_MAX];
    size_t len = __wrap_wcrtomb(bytes, (wchar_t)wc, NULL);
    if (len == (size_t)-1) {
        return WEOF;
    }
    while (len > 0) {
        len--;
        if (ungetc((unsigned char)bytes[len], stream) == EOF) {
            return WEOF;
        }
    }
    return wc;
}

wint_t __wrap_fgetwc_unlocked(FILE *stream) {
    return __wrap_fgetwc(stream);
}

wint_t __wrap_getwc_unlocked(FILE *stream) {
    return __wrap_fgetwc(stream);
}

wint_t __wrap_getwchar_unlocked(void) {
    return __wrap_fgetwc(stdin);
}

wint_t __wrap_fputwc_unlocked(wchar_t wc, FILE *stream) {
    return __wrap_fputwc(wc, stream);
}

wint_t __wrap_putwc_unlocked(wchar_t wc, FILE *stream) {
    return __wrap_fputwc(wc, stream);
}

wint_t __wrap_putwchar_unlocked(wchar_t wc) {
    return __wrap_fputwc(wc, stdout);
}

wchar_t *__wrap_fgetws_unlocked(wchar_t *ws, int n, FILE *stream) {
    return __wrap_fgetws(ws, n, stream);
}

int __wrap_fputws_unlocked(const wchar_t *ws, FILE *stream) {
    return __wrap_fputws(ws, stream);
}

char *__wrap_strsignal(int signum) {
    switch (signum) {
#ifdef SIGHUP
        case SIGHUP: return "Hangup";
#endif
#ifdef SIGINT
        case SIGINT: return "Interrupt";
#endif
#ifdef SIGQUIT
        case SIGQUIT: return "Quit";
#endif
#ifdef SIGILL
        case SIGILL: return "Illegal instruction";
#endif
#ifdef SIGTRAP
        case SIGTRAP: return "Trace/breakpoint trap";
#endif
#ifdef SIGABRT
        case SIGABRT: return "Aborted";
#endif
#ifdef SIGBUS
        case SIGBUS: return "Bus error";
#endif
#ifdef SIGFPE
        case SIGFPE: return "Floating point exception";
#endif
#ifdef SIGKILL
        case SIGKILL: return "Killed";
#endif
#ifdef SIGUSR1
        case SIGUSR1: return "User defined signal 1";
#endif
#ifdef SIGSEGV
        case SIGSEGV: return "Segmentation fault";
#endif
#ifdef SIGUSR2
        case SIGUSR2: return "User defined signal 2";
#endif
#ifdef SIGPIPE
        case SIGPIPE: return "Broken pipe";
#endif
#ifdef SIGALRM
        case SIGALRM: return "Alarm clock";
#endif
#ifdef SIGTERM
        case SIGTERM: return "Terminated";
#endif
#ifdef SIGCHLD
        case SIGCHLD: return "Child exited";
#endif
#ifdef SIGCONT
        case SIGCONT: return "Continued";
#endif
#ifdef SIGSTOP
        case SIGSTOP: return "Stopped";
#endif
#ifdef SIGTSTP
        case SIGTSTP: return "Stopped";
#endif
#ifdef SIGTTIN
        case SIGTTIN: return "Stopped (tty input)";
#endif
#ifdef SIGTTOU
        case SIGTTOU: return "Stopped (tty output)";
#endif
#ifdef SIGURG
        case SIGURG: return "Urgent I/O condition";
#endif
#ifdef SIGXCPU
        case SIGXCPU: return "CPU time limit exceeded";
#endif
#ifdef SIGXFSZ
        case SIGXFSZ: return "File size limit exceeded";
#endif
#ifdef SIGVTALRM
        case SIGVTALRM: return "Virtual timer expired";
#endif
#ifdef SIGPROF
        case SIGPROF: return "Profiling timer expired";
#endif
#ifdef SIGWINCH
        case SIGWINCH: return "Window changed";
#endif
#ifdef SIGIO
        case SIGIO: return "I/O possible";
#endif
#ifdef SIGPWR
        case SIGPWR: return "Power failure";
#endif
#ifdef SIGSYS
        case SIGSYS: return "Bad system call";
#endif
        default: break;
    }
    static char unknown[32];
    snprintf(unknown, sizeof(unknown), "Unknown signal %d", signum);
    return unknown;
}

int __wrap_getpriority(int which, id_t who) {
    int priority = 0;
    int rc = sinomo_proc_getpriority(which, (int)who, &priority);
    if (rc == 0) {
        errno = 0;
        return priority;
    }
    errno = rc;
    return -1;
}

int __wrap_setpriority(int which, id_t who, int priority) {
    int rc = sinomo_proc_setpriority(which, (int)who, priority);
    if (rc == 0) {
        return 0;
    }
    errno = rc;
    return -1;
}

int __wrap_nice(int increment) {
    errno = 0;
    int current = __wrap_getpriority(PRIO_PROCESS, 0);
    if (current == -1 && errno != 0) {
        return -1;
    }
    int next = current + increment;
    if (next < PRIO_MIN) {
        next = PRIO_MIN;
    }
    if (next > PRIO_MAX) {
        next = PRIO_MAX;
    }
    if (__wrap_setpriority(PRIO_PROCESS, 0, next) != 0) {
        return -1;
    }
    return next;
}

clock_t __wrap_times(struct tms *buf) {
    unsigned long long elapsed = 0, user = 0, system = 0, child_user = 0, child_system = 0;
    int rc = sinomo_proc_times(&elapsed, &user, &system, &child_user, &child_system);
    if (rc != 0) {
        errno = rc;
        return (clock_t)-1;
    }
    if (buf != NULL) {
        buf->tms_utime = (clock_t)user;
        buf->tms_stime = (clock_t)system;
        buf->tms_cutime = (clock_t)child_user;
        buf->tms_cstime = (clock_t)child_system;
    }
    return (clock_t)elapsed;
}

int __wrap_getrusage(int who, struct rusage *usage) {
    if (usage == NULL) {
        errno = EFAULT;
        return -1;
    }
    unsigned long long user_usec = 0;
    unsigned long long system_usec = 0;
    unsigned long long maxrss_kib = 0;
    int rc = sinomo_proc_getrusage(who, &user_usec, &system_usec, &maxrss_kib);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    memset(usage, 0, sizeof(*usage));
    usage->ru_utime.tv_sec = (time_t)(user_usec / 1000000ULL);
    usage->ru_utime.tv_usec = (suseconds_t)(user_usec % 1000000ULL);
    usage->ru_stime.tv_sec = (time_t)(system_usec / 1000000ULL);
    usage->ru_stime.tv_usec = (suseconds_t)(system_usec % 1000000ULL);
    usage->ru_maxrss = (long)maxrss_kib;
    return 0;
}

int __wrap_ftruncate(int fd, off_t length) {
    if (length < 0) {
        errno = EINVAL;
        return -1;
    }
    int seals = sinomo_memfd_get_seals(fd);
    if (seals >= 0) {
        struct stat st;
        if (fstat(fd, &st) != 0) {
            return -1;
        }
        if (length < st.st_size && (seals & F_SEAL_SHRINK) != 0) {
            errno = EPERM;
            return -1;
        }
        if (length > st.st_size && (seals & F_SEAL_GROW) != 0) {
            errno = EPERM;
            return -1;
        }
    } else if (errno == ENODEV) {
        errno = 0;
    } else {
        return -1;
    }
    __wasi_errno_t rc = __wasi_fd_filestat_set_size((__wasi_fd_t)fd, (__wasi_filesize_t)length);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    return 0;
}

static int sinomo_memfd_check_write(int fd, off_t offset, size_t count) {
    int seals = sinomo_memfd_get_seals(fd);
    if (seals < 0) {
        if (errno == ENODEV) {
            errno = 0;
            return 0;
        }
        return -1;
    }
    if ((seals & (F_SEAL_WRITE | F_SEAL_FUTURE_WRITE)) != 0) {
        errno = EPERM;
        return -1;
    }
    if ((seals & F_SEAL_GROW) != 0 && count > 0) {
        struct stat st;
        if (fstat(fd, &st) != 0) {
            return -1;
        }
        if (offset < 0) {
            offset = lseek(fd, 0, SEEK_CUR);
            if (offset < 0) {
                return -1;
            }
        }
        if ((uint64_t)offset + (uint64_t)count > (uint64_t)st.st_size) {
            errno = EPERM;
            return -1;
        }
    }
    return 0;
}

int __wrap_truncate(const char *path, off_t length) {
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (length < 0) {
        errno = EINVAL;
        return -1;
    }
    int fd = open(path, O_WRONLY);
    if (fd < 0) {
        return -1;
    }
    int rc = __wrap_ftruncate(fd, length);
    int saved_errno = errno;
    close(fd);
    errno = saved_errno;
    if (rc == 0) {
        sinomo_inotify_emit_path(path, IN_MODIFY | IN_ATTRIB);
    }
    return rc;
}

int __wrap_posix_fallocate(int fd, off_t offset, off_t len) {
    if (offset < 0 || len < 0) {
        return EINVAL;
    }
    __wasi_errno_t rc = __wasi_fd_allocate((__wasi_fd_t)fd, (__wasi_filesize_t)offset, (__wasi_filesize_t)len);
    return (int)rc;
}

int __wrap_posix_fadvise(int fd, off_t offset, off_t len, int advice) {
    if (offset < 0 || len < 0) {
        return EINVAL;
    }
    switch (advice) {
        case POSIX_FADV_NORMAL:
        case POSIX_FADV_SEQUENTIAL:
        case POSIX_FADV_RANDOM:
        case POSIX_FADV_WILLNEED:
        case POSIX_FADV_DONTNEED:
        case POSIX_FADV_NOREUSE:
            break;
        default:
            return EINVAL;
    }
    __wasi_errno_t rc = __wasi_fd_advise(
        (__wasi_fd_t)fd,
        (__wasi_filesize_t)offset,
        (__wasi_filesize_t)len,
        (__wasi_advice_t)advice
    );
    return (int)rc;
}

int __wrap_fsync(int fd) {
    __wasi_errno_t rc = __wasi_fd_sync((__wasi_fd_t)fd);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    return 0;
}

int __wrap_fdatasync(int fd) {
    __wasi_errno_t rc = __wasi_fd_datasync((__wasi_fd_t)fd);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    return 0;
}

void __wrap_sync(void) {
}

int __wrap_syncfs(int fd) {
    return __wrap_fsync(fd);
}

ssize_t __wrap_readahead(int fd, off_t offset, size_t count) {
    int rc = __wrap_posix_fadvise(fd, offset, (off_t)count, POSIX_FADV_WILLNEED);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    return 0;
}

#ifndef SYNC_FILE_RANGE_WAIT_BEFORE
#define SYNC_FILE_RANGE_WAIT_BEFORE 1
#endif
#ifndef SYNC_FILE_RANGE_WRITE
#define SYNC_FILE_RANGE_WRITE 2
#endif
#ifndef SYNC_FILE_RANGE_WAIT_AFTER
#define SYNC_FILE_RANGE_WAIT_AFTER 4
#endif

int __wrap_sync_file_range(int fd, off_t offset, off_t nbytes, unsigned int flags) {
    unsigned int supported = SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER;
    if (offset < 0 || nbytes < 0 || (flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    if ((flags & SYNC_FILE_RANGE_WRITE) == 0 && (flags & (SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WAIT_AFTER)) == 0) {
        return 0;
    }
    return __wrap_fdatasync(fd);
}

ssize_t __wrap_pread(int fd, void *buf, size_t count, off_t offset) {
    if (buf == NULL && count > 0) {
        errno = EFAULT;
        return -1;
    }
    if (offset < 0) {
        errno = EINVAL;
        return -1;
    }
    __wasi_iovec_t iov = {
        .buf = (uint8_t *)buf,
        .buf_len = count,
    };
    __wasi_size_t nread = 0;
    __wasi_errno_t rc = __wasi_fd_pread((__wasi_fd_t)fd, &iov, 1, (__wasi_filesize_t)offset, &nread);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    if (nread > (size_t)SSIZE_MAX) {
        errno = EOVERFLOW;
        return -1;
    }
    return (ssize_t)nread;
}

ssize_t __wrap_pwrite(int fd, const void *buf, size_t count, off_t offset) {
    if (buf == NULL && count > 0) {
        errno = EFAULT;
        return -1;
    }
    if (offset < 0) {
        errno = EINVAL;
        return -1;
    }
    if (sinomo_memfd_check_write(fd, offset, count) != 0) {
        return -1;
    }
    __wasi_ciovec_t iov = {
        .buf = (const uint8_t *)buf,
        .buf_len = count,
    };
    __wasi_size_t nwritten = 0;
    __wasi_errno_t rc = __wasi_fd_pwrite((__wasi_fd_t)fd, &iov, 1, (__wasi_filesize_t)offset, &nwritten);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    if (nwritten > (size_t)SSIZE_MAX) {
        errno = EOVERFLOW;
        return -1;
    }
    return (ssize_t)nwritten;
}

ssize_t __wrap_preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    if (iov == NULL && iovcnt > 0) {
        errno = EFAULT;
        return -1;
    }
    if (iovcnt < 0 || offset < 0) {
        errno = EINVAL;
        return -1;
    }
    __wasi_size_t nread = 0;
    __wasi_errno_t rc = __wasi_fd_pread(
        (__wasi_fd_t)fd,
        (const __wasi_iovec_t *)iov,
        (size_t)iovcnt,
        (__wasi_filesize_t)offset,
        &nread
    );
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    if (nread > (size_t)SSIZE_MAX) {
        errno = EOVERFLOW;
        return -1;
    }
    return (ssize_t)nread;
}

ssize_t __wrap_pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    if (iov == NULL && iovcnt > 0) {
        errno = EFAULT;
        return -1;
    }
    if (iovcnt < 0 || offset < 0) {
        errno = EINVAL;
        return -1;
    }
    ssize_t total = sinomo_iov_total(iov, (unsigned long)iovcnt);
    if (total < 0 || sinomo_memfd_check_write(fd, offset, (size_t)total) != 0) {
        return -1;
    }
    __wasi_size_t nwritten = 0;
    __wasi_errno_t rc = __wasi_fd_pwrite(
        (__wasi_fd_t)fd,
        (const __wasi_ciovec_t *)iov,
        (size_t)iovcnt,
        (__wasi_filesize_t)offset,
        &nwritten
    );
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    if (nwritten > (size_t)SSIZE_MAX) {
        errno = EOVERFLOW;
        return -1;
    }
    return (ssize_t)nwritten;
}

ssize_t __wrap_readv(int fd, const struct iovec *iov, int iovcnt) {
    if (iov == NULL && iovcnt > 0) {
        errno = EFAULT;
        return -1;
    }
    if (iovcnt < 0) {
        errno = EINVAL;
        return -1;
    }
    __wasi_size_t nread = 0;
    __wasi_errno_t rc = __wasi_fd_read((__wasi_fd_t)fd, (const __wasi_iovec_t *)iov, (size_t)iovcnt, &nread);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    if (nread > (size_t)SSIZE_MAX) {
        errno = EOVERFLOW;
        return -1;
    }
    return (ssize_t)nread;
}

ssize_t __wrap_writev(int fd, const struct iovec *iov, int iovcnt) {
    if (iov == NULL && iovcnt > 0) {
        errno = EFAULT;
        return -1;
    }
    if (iovcnt < 0) {
        errno = EINVAL;
        return -1;
    }
    ssize_t total = sinomo_iov_total(iov, (unsigned long)iovcnt);
    if (total < 0 || sinomo_memfd_check_write(fd, -1, (size_t)total) != 0) {
        return -1;
    }
    __wasi_size_t nwritten = 0;
    __wasi_errno_t rc = __wasi_fd_write((__wasi_fd_t)fd, (const __wasi_ciovec_t *)iov, (size_t)iovcnt, &nwritten);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    if (nwritten > (size_t)SSIZE_MAX) {
        errno = EOVERFLOW;
        return -1;
    }
    return (ssize_t)nwritten;
}

#ifndef RWF_HIPRI
#define RWF_HIPRI 0x00000001
#endif
#ifndef RWF_DSYNC
#define RWF_DSYNC 0x00000002
#endif
#ifndef RWF_SYNC
#define RWF_SYNC 0x00000004
#endif
#ifndef RWF_NOWAIT
#define RWF_NOWAIT 0x00000008
#endif
#ifndef RWF_APPEND
#define RWF_APPEND 0x00000010
#endif

static int sinomo_validate_rwf_flags(int flags) {
    int supported = RWF_HIPRI | RWF_DSYNC | RWF_SYNC | RWF_NOWAIT | RWF_APPEND;
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

ssize_t __wrap_preadv2(int fd, const struct iovec *iov, int iovcnt, off_t offset, int flags) {
    if (sinomo_validate_rwf_flags(flags) != 0) {
        return -1;
    }
    if (offset == (off_t)-1) {
        return __wrap_readv(fd, iov, iovcnt);
    }
    return __wrap_preadv(fd, iov, iovcnt, offset);
}

ssize_t __wrap_pwritev2(int fd, const struct iovec *iov, int iovcnt, off_t offset, int flags) {
    if (sinomo_validate_rwf_flags(flags) != 0) {
        return -1;
    }
    if (offset == (off_t)-1) {
        return __wrap_writev(fd, iov, iovcnt);
    }
    return __wrap_pwritev(fd, iov, iovcnt, offset);
}

static ssize_t sinomo_iov_total(const struct iovec *iov, unsigned long iovcnt) {
    size_t total = 0;
    if (iov == NULL && iovcnt > 0) {
        errno = EFAULT;
        return -1;
    }
    for (unsigned long index = 0; index < iovcnt; index++) {
        if (iov[index].iov_base == NULL && iov[index].iov_len > 0) {
            errno = EFAULT;
            return -1;
        }
        if (iov[index].iov_len > (size_t)SSIZE_MAX || total > (size_t)SSIZE_MAX - iov[index].iov_len) {
            errno = EINVAL;
            return -1;
        }
        total += iov[index].iov_len;
    }
    return (ssize_t)total;
}

static ssize_t sinomo_copy_iovecs(
    const struct iovec *source,
    unsigned long source_count,
    const struct iovec *dest,
    unsigned long dest_count
) {
    ssize_t source_total = sinomo_iov_total(source, source_count);
    if (source_total < 0) {
        return -1;
    }
    ssize_t dest_total = sinomo_iov_total(dest, dest_count);
    if (dest_total < 0) {
        return -1;
    }
    size_t remaining = (size_t)(source_total < dest_total ? source_total : dest_total);
    size_t copied = 0;
    unsigned long source_index = 0;
    unsigned long dest_index = 0;
    size_t source_offset = 0;
    size_t dest_offset = 0;
    while (remaining > 0) {
        while (source_index < source_count && source_offset >= source[source_index].iov_len) {
            source_index++;
            source_offset = 0;
        }
        while (dest_index < dest_count && dest_offset >= dest[dest_index].iov_len) {
            dest_index++;
            dest_offset = 0;
        }
        if (source_index >= source_count || dest_index >= dest_count) {
            break;
        }
        size_t source_available = source[source_index].iov_len - source_offset;
        size_t dest_available = dest[dest_index].iov_len - dest_offset;
        size_t chunk = source_available < dest_available ? source_available : dest_available;
        if (chunk > remaining) {
            chunk = remaining;
        }
        memcpy(
            (char *)dest[dest_index].iov_base + dest_offset,
            (const char *)source[source_index].iov_base + source_offset,
            chunk
        );
        copied += chunk;
        remaining -= chunk;
        source_offset += chunk;
        dest_offset += chunk;
    }
    return (ssize_t)copied;
}

static ssize_t sinomo_process_vm_readv(
    pid_t pid,
    const struct iovec *local_iov,
    unsigned long local_iovcnt,
    const struct iovec *remote_iov,
    unsigned long remote_iovcnt,
    unsigned long flags
) {
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    if (pid != __wrap_getpid()) {
        errno = ESRCH;
        return -1;
    }
    return sinomo_copy_iovecs(remote_iov, remote_iovcnt, local_iov, local_iovcnt);
}

static ssize_t sinomo_process_vm_writev(
    pid_t pid,
    const struct iovec *local_iov,
    unsigned long local_iovcnt,
    const struct iovec *remote_iov,
    unsigned long remote_iovcnt,
    unsigned long flags
) {
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    if (pid != __wrap_getpid()) {
        errno = ESRCH;
        return -1;
    }
    return sinomo_copy_iovecs(local_iov, local_iovcnt, remote_iov, remote_iovcnt);
}

static ssize_t sinomo_process_madvise(
    pid_t pid,
    const struct iovec *iov,
    unsigned long iovcnt,
    int advice,
    unsigned int flags
) {
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    if (pid != __wrap_getpid()) {
        errno = ESRCH;
        return -1;
    }
    ssize_t total = sinomo_iov_total(iov, iovcnt);
    if (total < 0) {
        return -1;
    }
    for (unsigned long index = 0; index < iovcnt; index++) {
        if (__wrap_madvise(iov[index].iov_base, iov[index].iov_len, advice) != 0) {
            return -1;
        }
    }
    return total;
}

#ifndef KCMP_FILE
#define KCMP_FILE 0
#endif
#ifndef KCMP_VM
#define KCMP_VM 1
#endif
#ifndef KCMP_FILES
#define KCMP_FILES 2
#endif
#ifndef KCMP_FS
#define KCMP_FS 3
#endif
#ifndef KCMP_SIGHAND
#define KCMP_SIGHAND 4
#endif
#ifndef KCMP_IO
#define KCMP_IO 5
#endif
#ifndef KCMP_SYSVSEM
#define KCMP_SYSVSEM 6
#endif
#ifndef KCMP_EPOLL_TFD
#define KCMP_EPOLL_TFD 7
#endif

static int sinomo_kcmp(pid_t pid1, pid_t pid2, int type, unsigned long idx1, unsigned long idx2) {
    pid_t current = __wrap_getpid();
    if ((pid1 != 0 && pid1 != current) || (pid2 != 0 && pid2 != current)) {
        errno = ESRCH;
        return -1;
    }
    switch (type) {
        case KCMP_FILE: {
            struct stat left;
            struct stat right;
            if (fstat((int)idx1, &left) != 0 || fstat((int)idx2, &right) != 0) {
                return -1;
            }
            if (left.st_dev == right.st_dev && left.st_ino == right.st_ino) {
                return 0;
            }
            return idx1 < idx2 ? -1 : 1;
        }
        case KCMP_VM:
        case KCMP_FILES:
        case KCMP_FS:
        case KCMP_SIGHAND:
        case KCMP_IO:
        case KCMP_SYSVSEM:
            return 0;
        case KCMP_EPOLL_TFD:
            errno = EINVAL;
            return -1;
        default:
            errno = EINVAL;
            return -1;
    }
}

static int sinomo_xattr_root(void) {
    return sinomo_mkdir_if_missing("/tmp") == 0 && sinomo_mkdir_if_missing("/tmp/.sinomo-xattr") == 0 ? 0 : -1;
}

static int sinomo_xattr_object_dir(const struct stat *st, char *dir, size_t dir_len, int create) {
    if (st == NULL || dir == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (sinomo_xattr_root() != 0) {
        return -1;
    }
    int written = snprintf(
        dir,
        dir_len,
        "/tmp/.sinomo-xattr/%llu-%llu",
        (unsigned long long)st->st_dev,
        (unsigned long long)st->st_ino
    );
    if (written < 0 || (size_t)written >= dir_len) {
        errno = ENAMETOOLONG;
        return -1;
    }
    if (create && sinomo_mkdir_if_missing(dir) != 0) {
        return -1;
    }
    return 0;
}

static int sinomo_xattr_name_path(const char *dir, const char *name, char *path, size_t path_len) {
    if (name == NULL || name[0] == 0) {
        errno = ERANGE;
        return -1;
    }
    size_t dir_len = strlen(dir);
    if (dir_len + 1 + (strlen(name) * 2) >= path_len) {
        errno = ENAMETOOLONG;
        return -1;
    }
    memcpy(path, dir, dir_len);
    path[dir_len] = '/';
    char *out = path + dir_len + 1;
    static const char hex[] = "0123456789abcdef";
    for (const unsigned char *ptr = (const unsigned char *)name; *ptr != 0; ptr++) {
        *out++ = hex[*ptr >> 4];
        *out++ = hex[*ptr & 0x0f];
    }
    *out = 0;
    return 0;
}

static int sinomo_xattr_stat_path(const char *path, int nofollow, struct stat *st) {
    if (path == NULL || st == NULL) {
        errno = EFAULT;
        return -1;
    }
    return nofollow ? lstat(path, st) : stat(path, st);
}

static ssize_t sinomo_xattr_get_by_stat(const struct stat *st, const char *name, void *value, size_t size) {
    char dir[256];
    char attr_path[512];
    if (sinomo_xattr_object_dir(st, dir, sizeof(dir), 0) != 0 ||
        sinomo_xattr_name_path(dir, name, attr_path, sizeof(attr_path)) != 0) {
        return -1;
    }
    FILE *file = fopen(attr_path, "rb");
    if (file == NULL) {
        if (errno == ENOENT) {
            errno = ENODATA;
        }
        return -1;
    }
    if (fseek(file, 0, SEEK_END) != 0) {
        int saved = errno;
        fclose(file);
        errno = saved;
        return -1;
    }
    long length = ftell(file);
    if (length < 0) {
        int saved = errno;
        fclose(file);
        errno = saved;
        return -1;
    }
    rewind(file);
    if (value == NULL) {
        fclose(file);
        return (ssize_t)length;
    }
    if ((size_t)length > size) {
        fclose(file);
        errno = ERANGE;
        return -1;
    }
    size_t read_len = fread(value, 1, (size_t)length, file);
    int failed = ferror(file);
    fclose(file);
    if (failed || read_len != (size_t)length) {
        errno = EIO;
        return -1;
    }
    return (ssize_t)length;
}

static int sinomo_xattr_set_by_stat(const struct stat *st, const char *name, const void *value, size_t size, int flags) {
    if (value == NULL && size > 0) {
        errno = EFAULT;
        return -1;
    }
    if ((flags & ~(1 | 2)) != 0 || (flags & 1) != 0 && (flags & 2) != 0) {
        errno = EINVAL;
        return -1;
    }
    char dir[256];
    char attr_path[512];
    if (sinomo_xattr_object_dir(st, dir, sizeof(dir), 1) != 0 ||
        sinomo_xattr_name_path(dir, name, attr_path, sizeof(attr_path)) != 0) {
        return -1;
    }
    int exists = access(attr_path, F_OK) == 0;
    if ((flags & 1) != 0 && exists) {
        errno = EEXIST;
        return -1;
    }
    if ((flags & 2) != 0 && !exists) {
        errno = ENODATA;
        return -1;
    }
    FILE *file = fopen(attr_path, "wb");
    if (file == NULL) {
        return -1;
    }
    size_t written = size == 0 ? 0 : fwrite(value, 1, size, file);
    int failed = ferror(file);
    if (fclose(file) != 0 || failed || written != size) {
        errno = EIO;
        return -1;
    }
    return 0;
}

static ssize_t sinomo_xattr_list_by_stat(const struct stat *st, char *list, size_t size) {
    char dir[256];
    if (sinomo_xattr_object_dir(st, dir, sizeof(dir), 0) != 0) {
        return -1;
    }
    DIR *directory = opendir(dir);
    if (directory == NULL) {
        if (errno == ENOENT) {
            return 0;
        }
        return -1;
    }
    size_t total = 0;
    struct dirent *entry;
    while ((entry = readdir(directory)) != NULL) {
        if (entry->d_name[0] == '.') {
            continue;
        }
        size_t encoded_len = strlen(entry->d_name);
        if ((encoded_len % 2) != 0) {
            continue;
        }
        size_t name_len = encoded_len / 2;
        if (list != NULL) {
            if (total + name_len + 1 > size) {
                closedir(directory);
                errno = ERANGE;
                return -1;
            }
            for (size_t index = 0; index < name_len; index++) {
                char hi = entry->d_name[index * 2];
                char lo = entry->d_name[index * 2 + 1];
                int hv = hi >= '0' && hi <= '9' ? hi - '0' : hi >= 'a' && hi <= 'f' ? hi - 'a' + 10 : -1;
                int lv = lo >= '0' && lo <= '9' ? lo - '0' : lo >= 'a' && lo <= 'f' ? lo - 'a' + 10 : -1;
                if (hv < 0 || lv < 0) {
                    closedir(directory);
                    errno = EINVAL;
                    return -1;
                }
                list[total + index] = (char)((hv << 4) | lv);
            }
            list[total + name_len] = 0;
        }
        total += name_len + 1;
    }
    closedir(directory);
    return (ssize_t)total;
}

static int sinomo_xattr_remove_by_stat(const struct stat *st, const char *name) {
    char dir[256];
    char attr_path[512];
    if (sinomo_xattr_object_dir(st, dir, sizeof(dir), 0) != 0 ||
        sinomo_xattr_name_path(dir, name, attr_path, sizeof(attr_path)) != 0) {
        return -1;
    }
    if (unlink(attr_path) != 0) {
        if (errno == ENOENT) {
            errno = ENODATA;
        }
        return -1;
    }
    return 0;
}

int __wrap_setxattr(const char *path, const char *name, const void *value, size_t size, int flags) {
    struct stat st;
    return sinomo_xattr_stat_path(path, 0, &st) == 0 ? sinomo_xattr_set_by_stat(&st, name, value, size, flags) : -1;
}

int __wrap_lsetxattr(const char *path, const char *name, const void *value, size_t size, int flags) {
    struct stat st;
    return sinomo_xattr_stat_path(path, 1, &st) == 0 ? sinomo_xattr_set_by_stat(&st, name, value, size, flags) : -1;
}

int __wrap_fsetxattr(int fd, const char *name, const void *value, size_t size, int flags) {
    struct stat st;
    return fstat(fd, &st) == 0 ? sinomo_xattr_set_by_stat(&st, name, value, size, flags) : -1;
}

ssize_t __wrap_getxattr(const char *path, const char *name, void *value, size_t size) {
    struct stat st;
    return sinomo_xattr_stat_path(path, 0, &st) == 0 ? sinomo_xattr_get_by_stat(&st, name, value, size) : -1;
}

ssize_t __wrap_lgetxattr(const char *path, const char *name, void *value, size_t size) {
    struct stat st;
    return sinomo_xattr_stat_path(path, 1, &st) == 0 ? sinomo_xattr_get_by_stat(&st, name, value, size) : -1;
}

ssize_t __wrap_fgetxattr(int fd, const char *name, void *value, size_t size) {
    struct stat st;
    return fstat(fd, &st) == 0 ? sinomo_xattr_get_by_stat(&st, name, value, size) : -1;
}

ssize_t __wrap_listxattr(const char *path, char *list, size_t size) {
    struct stat st;
    return sinomo_xattr_stat_path(path, 0, &st) == 0 ? sinomo_xattr_list_by_stat(&st, list, size) : -1;
}

ssize_t __wrap_llistxattr(const char *path, char *list, size_t size) {
    struct stat st;
    return sinomo_xattr_stat_path(path, 1, &st) == 0 ? sinomo_xattr_list_by_stat(&st, list, size) : -1;
}

ssize_t __wrap_flistxattr(int fd, char *list, size_t size) {
    struct stat st;
    return fstat(fd, &st) == 0 ? sinomo_xattr_list_by_stat(&st, list, size) : -1;
}

int __wrap_removexattr(const char *path, const char *name) {
    struct stat st;
    return sinomo_xattr_stat_path(path, 0, &st) == 0 ? sinomo_xattr_remove_by_stat(&st, name) : -1;
}

int __wrap_lremovexattr(const char *path, const char *name) {
    struct stat st;
    return sinomo_xattr_stat_path(path, 1, &st) == 0 ? sinomo_xattr_remove_by_stat(&st, name) : -1;
}

int __wrap_fremovexattr(int fd, const char *name) {
    struct stat st;
    return fstat(fd, &st) == 0 ? sinomo_xattr_remove_by_stat(&st, name) : -1;
}

ssize_t __wrap_copy_file_range(int fd_in, off_t *off_in, int fd_out, off_t *off_out, size_t len, unsigned int flags) {
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    unsigned char buffer[65536];
    size_t remaining = len;
    ssize_t total = 0;
    while (remaining > 0) {
        size_t chunk = remaining < sizeof(buffer) ? remaining : sizeof(buffer);
        ssize_t nread = off_in != NULL
            ? __wrap_pread(fd_in, buffer, chunk, *off_in)
            : read(fd_in, buffer, chunk);
        if (nread < 0) {
            return total > 0 ? total : -1;
        }
        if (nread == 0) {
            break;
        }
        size_t written = 0;
        while (written < (size_t)nread) {
            ssize_t nwritten = off_out != NULL
                ? __wrap_pwrite(fd_out, buffer + written, (size_t)nread - written, *off_out)
                : write(fd_out, buffer + written, (size_t)nread - written);
            if (nwritten < 0) {
                return total > 0 ? total : -1;
            }
            if (nwritten == 0) {
                errno = EIO;
                return total > 0 ? total : -1;
            }
            if (off_out != NULL) {
                *off_out += nwritten;
            }
            written += (size_t)nwritten;
            total += nwritten;
        }
        if (off_in != NULL) {
            *off_in += nread;
        }
        remaining -= (size_t)nread;
        if ((size_t)nread < chunk) {
            break;
        }
    }
    return total;
}

ssize_t __wrap_sendfile(int out_fd, int in_fd, off_t *offset, size_t count) {
    return __wrap_copy_file_range(in_fd, offset, out_fd, NULL, count, 0);
}

#ifndef SPLICE_F_MOVE
#define SPLICE_F_MOVE 1
#endif
#ifndef SPLICE_F_NONBLOCK
#define SPLICE_F_NONBLOCK 2
#endif
#ifndef SPLICE_F_MORE
#define SPLICE_F_MORE 4
#endif
#ifndef SPLICE_F_GIFT
#define SPLICE_F_GIFT 8
#endif

ssize_t __wrap_splice(int fd_in, off_t *off_in, int fd_out, off_t *off_out, size_t len, unsigned int flags) {
    unsigned int supported = SPLICE_F_MOVE | SPLICE_F_NONBLOCK | SPLICE_F_MORE | SPLICE_F_GIFT;
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    return __wrap_copy_file_range(fd_in, off_in, fd_out, off_out, len, 0);
}

ssize_t __wrap_vmsplice(int fd, const struct iovec *iov, unsigned long nr_segs, unsigned int flags) {
    unsigned int supported = SPLICE_F_MOVE | SPLICE_F_NONBLOCK | SPLICE_F_MORE | SPLICE_F_GIFT;
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    if (nr_segs > (unsigned long)INT_MAX) {
        errno = EINVAL;
        return -1;
    }
    return __wrap_writev(fd, iov, (int)nr_segs);
}

static int sinomo_apply_fd_flags(int fd, int flags) {
    if ((flags & O_CLOEXEC) != 0 && __real_fcntl(fd, F_SETFD, FD_CLOEXEC) != 0) {
        return -1;
    }
    if ((flags & O_NONBLOCK) != 0) {
        int current = __real_fcntl(fd, F_GETFL, 0);
        if (current < 0 || __real_fcntl(fd, F_SETFL, current | O_NONBLOCK) != 0) {
            return -1;
        }
    }
    return 0;
}

#define SINOMO_SOCKET_METADATA_SLOTS 512
struct sinomo_socket_metadata {
    int fd;
    int domain;
    int type;
    int protocol;
};
static struct sinomo_socket_metadata sinomo_socket_metadata[SINOMO_SOCKET_METADATA_SLOTS];

static struct sinomo_socket_metadata *sinomo_socket_metadata_entry(int fd) {
    for (size_t index = 0; index < SINOMO_SOCKET_METADATA_SLOTS; index++) {
        if (sinomo_socket_metadata[index].fd == fd) {
            return &sinomo_socket_metadata[index];
        }
    }
    return NULL;
}

static void sinomo_socket_remember(int fd, int domain, int type, int protocol) {
    if (fd < 0) {
        return;
    }
    struct sinomo_socket_metadata *entry = sinomo_socket_metadata_entry(fd);
    if (entry == NULL) {
        for (size_t index = 0; index < SINOMO_SOCKET_METADATA_SLOTS; index++) {
            if (sinomo_socket_metadata[index].fd == 0) {
                entry = &sinomo_socket_metadata[index];
                break;
            }
        }
    }
    if (entry == NULL) {
        return;
    }
    entry->fd = fd;
    entry->domain = domain;
    entry->type = type;
    entry->protocol = protocol;
}

static void sinomo_socket_forget(int fd) {
    struct sinomo_socket_metadata *entry = sinomo_socket_metadata_entry(fd);
    if (entry != NULL) {
        memset(entry, 0, sizeof(*entry));
    }
}

static void sinomo_socket_duplicate_metadata(int oldfd, int newfd) {
    struct sinomo_socket_metadata *old_entry = sinomo_socket_metadata_entry(oldfd);
    if (old_entry == NULL) {
        sinomo_socket_forget(newfd);
        return;
    }
    sinomo_socket_remember(newfd, old_entry->domain, old_entry->type, old_entry->protocol);
}

#define SINOMO_PIPE_SIZE_SLOTS 256
struct sinomo_pipe_size_entry {
    int fd;
    int size;
};
static struct sinomo_pipe_size_entry sinomo_pipe_sizes[SINOMO_PIPE_SIZE_SLOTS];

static void sinomo_pipe_remember(int fd, int size) {
    for (size_t index = 0; index < SINOMO_PIPE_SIZE_SLOTS; index++) {
        if (sinomo_pipe_sizes[index].fd == fd || sinomo_pipe_sizes[index].fd == 0) {
            sinomo_pipe_sizes[index].fd = fd;
            sinomo_pipe_sizes[index].size = size;
            return;
        }
    }
}

static int sinomo_pipe_get_size(int fd) {
    for (size_t index = 0; index < SINOMO_PIPE_SIZE_SLOTS; index++) {
        if (sinomo_pipe_sizes[index].fd == fd) {
            return sinomo_pipe_sizes[index].size;
        }
    }
    return 65536;
}

static int sinomo_pipe_set_size(int fd, int size) {
    if (size <= 0) {
        errno = EINVAL;
        return -1;
    }
    int normalized = size < 4096 ? 4096 : size;
    sinomo_pipe_remember(fd, normalized);
    return normalized;
}

static void sinomo_pipe_forget(int fd) {
    for (size_t index = 0; index < SINOMO_PIPE_SIZE_SLOTS; index++) {
        if (sinomo_pipe_sizes[index].fd == fd) {
            sinomo_pipe_sizes[index].fd = 0;
            sinomo_pipe_sizes[index].size = 0;
        }
    }
}

int __wrap_pipe2(int pipefd[2], int flags) {
    int supported = 0;
#ifdef O_CLOEXEC
    supported |= O_CLOEXEC;
#endif
#ifdef O_NONBLOCK
    supported |= O_NONBLOCK;
#endif
    if (pipefd == NULL) {
        errno = EFAULT;
        return -1;
    }
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    __wasi_fd_t read_fd = 0;
    __wasi_fd_t write_fd = 0;
    __wasi_errno_t rc = __wasi_fd_pipe(&read_fd, &write_fd);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    pipefd[0] = (int)read_fd;
    pipefd[1] = (int)write_fd;
    if (sinomo_apply_fd_flags(pipefd[0], flags) != 0 || sinomo_apply_fd_flags(pipefd[1], flags) != 0) {
        int saved_errno = errno;
        close(pipefd[0]);
        close(pipefd[1]);
        errno = saved_errno;
        return -1;
    }
    sinomo_pipe_remember(pipefd[0], 65536);
    sinomo_pipe_remember(pipefd[1], 65536);
    return 0;
}

int __wrap_pipe(int pipefd[2]) {
    return __wrap_pipe2(pipefd, 0);
}

int __wrap_eventfd(unsigned int initval, int flags) {
    int supported = 0;
#ifdef EFD_CLOEXEC
    supported |= EFD_CLOEXEC;
#endif
#ifdef EFD_NONBLOCK
    supported |= EFD_NONBLOCK;
#endif
#ifdef EFD_SEMAPHORE
    supported |= EFD_SEMAPHORE;
#endif
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }

    __wasi_fd_t fd = 0;
    __wasi_errno_t rc = __wasi_fd_event((uint64_t)initval, (__wasi_eventfdflags_t)flags, &fd);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    if (sinomo_apply_fd_flags((int)fd, flags) != 0) {
        int saved_errno = errno;
        close((int)fd);
        errno = saved_errno;
        return -1;
    }
    return (int)fd;
}

int __wrap_eventfd_read(int fd, eventfd_t *value) {
    if (value == NULL) {
        errno = EFAULT;
        return -1;
    }
    uint64_t raw = 0;
    ssize_t nread = read(fd, &raw, sizeof(raw));
    if (nread < 0) {
        return -1;
    }
    if ((size_t)nread != sizeof(raw)) {
        errno = EINVAL;
        return -1;
    }
    *value = (eventfd_t)raw;
    return 0;
}

int __wrap_eventfd_write(int fd, eventfd_t value) {
    uint64_t raw = (uint64_t)value;
    ssize_t nwritten = write(fd, &raw, sizeof(raw));
    if (nwritten < 0) {
        return -1;
    }
    if ((size_t)nwritten != sizeof(raw)) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

#define SINOMO_INOTIFY_INSTANCES 32
#define SINOMO_INOTIFY_WATCHES 256
#define SINOMO_INOTIFY_EVENTS 512
#define SINOMO_INOTIFY_NAME_BYTES 256

struct sinomo_inotify_event_record {
    int wd;
    uint32_t mask;
    uint32_t cookie;
    uint32_t len;
    char name[SINOMO_INOTIFY_NAME_BYTES];
};

struct sinomo_inotify_watch {
    int active;
    int wd;
    uint32_t mask;
    char path[PATH_MAX];
};

struct sinomo_inotify_instance {
    int active;
    int fd;
    int next_wd;
    struct sinomo_inotify_watch watches[SINOMO_INOTIFY_WATCHES];
    struct sinomo_inotify_event_record events[SINOMO_INOTIFY_EVENTS];
    size_t event_start;
    size_t event_count;
};

static struct sinomo_inotify_instance sinomo_inotify_instances[SINOMO_INOTIFY_INSTANCES];

struct sinomo_fanotify_event_record {
    uint64_t mask;
    pid_t pid;
};

struct sinomo_fanotify_mark {
    int active;
    char path[PATH_MAX];
    uint64_t mask;
};

#define SINOMO_FANOTIFY_INSTANCES 32
#define SINOMO_FANOTIFY_MARKS 128
#define SINOMO_FANOTIFY_EVENTS 256

struct sinomo_fanotify_instance {
    int active;
    int fd;
    unsigned int event_f_flags;
    struct sinomo_fanotify_mark marks[SINOMO_FANOTIFY_MARKS];
    struct sinomo_fanotify_event_record events[SINOMO_FANOTIFY_EVENTS];
    size_t event_start;
    size_t event_count;
};

struct sinomo_fanotify_event_metadata {
    uint32_t event_len;
    uint8_t vers;
    uint8_t reserved;
    uint16_t metadata_len;
    uint64_t mask;
    int32_t fd;
    int32_t pid;
};

static struct sinomo_fanotify_instance sinomo_fanotify_instances[SINOMO_FANOTIFY_INSTANCES];

struct sinomo_inotify_event_wire {
    int wd;
    uint32_t mask;
    uint32_t cookie;
    uint32_t len;
};

static int sinomo_inotify_index(int fd) {
    for (int index = 0; index < SINOMO_INOTIFY_INSTANCES; index++) {
        if (sinomo_inotify_instances[index].active && sinomo_inotify_instances[index].fd == fd) {
            return index;
        }
    }
    return -1;
}

static void sinomo_inotify_forget(int fd) {
    int index = sinomo_inotify_index(fd);
    if (index >= 0) {
        memset(&sinomo_inotify_instances[index], 0, sizeof(sinomo_inotify_instances[index]));
    }
}

static int sinomo_fanotify_index(int fd) {
    for (int index = 0; index < SINOMO_FANOTIFY_INSTANCES; index++) {
        if (sinomo_fanotify_instances[index].active && sinomo_fanotify_instances[index].fd == fd) {
            return index;
        }
    }
    return -1;
}

static void sinomo_fanotify_forget(int fd) {
    int index = sinomo_fanotify_index(fd);
    if (index >= 0) {
        memset(&sinomo_fanotify_instances[index], 0, sizeof(sinomo_fanotify_instances[index]));
    }
}

static int sinomo_fanotify_push_event(struct sinomo_fanotify_instance *instance, uint64_t mask) {
    if (instance->event_count >= SINOMO_FANOTIFY_EVENTS) {
        struct sinomo_fanotify_event_record *overflow = &instance->events[instance->event_start];
        overflow->mask = FAN_Q_OVERFLOW;
        overflow->pid = __wrap_getpid();
        return 0;
    }
    size_t index = (instance->event_start + instance->event_count) % SINOMO_FANOTIFY_EVENTS;
    instance->events[index].mask = mask;
    instance->events[index].pid = __wrap_getpid();
    instance->event_count++;
    (void)__wrap_eventfd_write(instance->fd, 1);
    return 0;
}

static uint64_t sinomo_fanotify_mask_from_inotify(uint32_t mask) {
    uint64_t fan_mask = 0;
    if ((mask & IN_ACCESS) != 0) fan_mask |= FAN_ACCESS;
    if ((mask & (IN_MODIFY | IN_ATTRIB)) != 0) fan_mask |= FAN_MODIFY;
    if ((mask & IN_CLOSE_WRITE) != 0) fan_mask |= FAN_CLOSE_WRITE;
    if ((mask & IN_CLOSE_NOWRITE) != 0) fan_mask |= FAN_CLOSE_NOWRITE;
    if ((mask & IN_OPEN) != 0) fan_mask |= FAN_OPEN;
    if ((mask & IN_MOVED_FROM) != 0) fan_mask |= FAN_MOVED_FROM;
    if ((mask & IN_MOVED_TO) != 0) fan_mask |= FAN_MOVED_TO;
    if ((mask & IN_CREATE) != 0) fan_mask |= FAN_CREATE;
    if ((mask & IN_DELETE) != 0) fan_mask |= FAN_DELETE;
    if ((mask & IN_DELETE_SELF) != 0) fan_mask |= FAN_DELETE_SELF;
    if ((mask & IN_MOVE_SELF) != 0) fan_mask |= FAN_MOVE_SELF;
    if ((mask & IN_Q_OVERFLOW) != 0) fan_mask |= FAN_Q_OVERFLOW;
    if ((mask & IN_ISDIR) != 0) fan_mask |= FAN_ONDIR;
    return fan_mask;
}

static int sinomo_path_is_same_or_child(const char *parent, const char *path) {
    if (parent == NULL || path == NULL) {
        return 0;
    }
    if (strcmp(parent, "/") == 0 || strcmp(parent, path) == 0) {
        return 1;
    }
    size_t parent_len = strlen(parent);
    return strncmp(path, parent, parent_len) == 0 && path[parent_len] == '/';
}

static void sinomo_fanotify_emit_path(const char *path, uint32_t inotify_mask) {
    uint64_t mask = sinomo_fanotify_mask_from_inotify(inotify_mask);
    if (mask == 0) {
        return;
    }
    for (int instance_index = 0; instance_index < SINOMO_FANOTIFY_INSTANCES; instance_index++) {
        struct sinomo_fanotify_instance *instance = &sinomo_fanotify_instances[instance_index];
        if (!instance->active) {
            continue;
        }
        for (int mark_index = 0; mark_index < SINOMO_FANOTIFY_MARKS; mark_index++) {
            struct sinomo_fanotify_mark *mark = &instance->marks[mark_index];
            if (!mark->active || (mark->mask & mask) == 0) {
                continue;
            }
            if (sinomo_path_is_same_or_child(mark->path, path)) {
                (void)sinomo_fanotify_push_event(instance, mask & mark->mask);
                break;
            }
        }
    }
}

static int sinomo_inotify_push_event(struct sinomo_inotify_instance *instance, int wd, uint32_t mask, const char *name) {
    if (instance->event_count >= SINOMO_INOTIFY_EVENTS) {
        if (instance->event_count == 0) {
            return 0;
        }
        struct sinomo_inotify_event_record *overflow =
            &instance->events[(instance->event_start + instance->event_count - 1) % SINOMO_INOTIFY_EVENTS];
        memset(overflow, 0, sizeof(*overflow));
        overflow->wd = -1;
        overflow->mask = IN_Q_OVERFLOW;
        return 0;
    }
    size_t index = (instance->event_start + instance->event_count) % SINOMO_INOTIFY_EVENTS;
    struct sinomo_inotify_event_record *event = &instance->events[index];
    memset(event, 0, sizeof(*event));
    event->wd = wd;
    event->mask = mask;
    if (name != NULL && name[0] != 0) {
        size_t name_len = strnlen(name, SINOMO_INOTIFY_NAME_BYTES - 1);
        memcpy(event->name, name, name_len);
        event->name[name_len] = 0;
        event->len = (uint32_t)(name_len + 1);
    }
    instance->event_count++;
    (void)__wrap_eventfd_write(instance->fd, 1);
    return 0;
}

static void sinomo_inotify_split_parent(const char *path, char *parent, size_t parent_len, const char **name) {
    const char *slash = path == NULL ? NULL : strrchr(path, '/');
    if (path == NULL || path[0] == 0) {
        snprintf(parent, parent_len, ".");
        *name = "";
    } else if (slash == NULL) {
        snprintf(parent, parent_len, ".");
        *name = path;
    } else if (slash == path) {
        snprintf(parent, parent_len, "/");
        *name = slash + 1;
    } else {
        size_t len = (size_t)(slash - path);
        if (len >= parent_len) {
            len = parent_len - 1;
        }
        memcpy(parent, path, len);
        parent[len] = 0;
        *name = slash + 1;
    }
}

static void sinomo_inotify_emit_path(const char *path, uint32_t mask) {
    if (path == NULL || path[0] == 0) {
        return;
    }
    sinomo_fanotify_emit_path(path, mask);
    char parent[PATH_MAX];
    const char *name = NULL;
    sinomo_inotify_split_parent(path, parent, sizeof(parent), &name);
    for (int instance_index = 0; instance_index < SINOMO_INOTIFY_INSTANCES; instance_index++) {
        struct sinomo_inotify_instance *instance = &sinomo_inotify_instances[instance_index];
        if (!instance->active) {
            continue;
        }
        for (int watch_index = 0; watch_index < SINOMO_INOTIFY_WATCHES; watch_index++) {
            struct sinomo_inotify_watch *watch = &instance->watches[watch_index];
            if (!watch->active) {
                continue;
            }
            uint32_t watched_mask = watch->mask;
            if ((watched_mask & mask) == 0 && (mask & IN_ISDIR) == 0) {
                continue;
            }
            if (strcmp(watch->path, path) == 0) {
                (void)sinomo_inotify_push_event(instance, watch->wd, mask & ~IN_ISDIR, NULL);
                if ((watched_mask & IN_ONESHOT) != 0) {
                    (void)__wrap_inotify_rm_watch(instance->fd, watch->wd);
                }
                continue;
            }
            if (strcmp(watch->path, parent) == 0 && name != NULL && name[0] != 0) {
                (void)sinomo_inotify_push_event(instance, watch->wd, mask, name);
                if ((watched_mask & IN_ONESHOT) != 0) {
                    (void)__wrap_inotify_rm_watch(instance->fd, watch->wd);
                }
            }
        }
    }
}

int __wrap_inotify_init1(int flags) {
    int supported = IN_CLOEXEC | IN_NONBLOCK;
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    int slot = -1;
    for (int index = 0; index < SINOMO_INOTIFY_INSTANCES; index++) {
        if (!sinomo_inotify_instances[index].active) {
            slot = index;
            break;
        }
    }
    if (slot < 0) {
        errno = EMFILE;
        return -1;
    }
    int event_flags = 0;
#ifdef EFD_CLOEXEC
    if ((flags & IN_CLOEXEC) != 0) {
        event_flags |= EFD_CLOEXEC;
    }
#endif
#ifdef EFD_NONBLOCK
    if ((flags & IN_NONBLOCK) != 0) {
        event_flags |= EFD_NONBLOCK;
    }
#endif
    int fd = __wrap_eventfd(0, event_flags);
    if (fd < 0) {
        return -1;
    }
    memset(&sinomo_inotify_instances[slot], 0, sizeof(sinomo_inotify_instances[slot]));
    sinomo_inotify_instances[slot].active = 1;
    sinomo_inotify_instances[slot].fd = fd;
    sinomo_inotify_instances[slot].next_wd = 1;
    return fd;
}

int __wrap_inotify_init(void) {
    return __wrap_inotify_init1(0);
}

int __wrap_inotify_add_watch(int fd, const char *path, uint32_t mask) {
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    int index = sinomo_inotify_index(fd);
    if (index < 0) {
        errno = EINVAL;
        return -1;
    }
    struct stat st;
    if (stat(path, &st) != 0) {
        return -1;
    }
    if ((mask & IN_ONLYDIR) != 0 && !S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        return -1;
    }
    struct sinomo_inotify_instance *instance = &sinomo_inotify_instances[index];
    for (int watch_index = 0; watch_index < SINOMO_INOTIFY_WATCHES; watch_index++) {
        struct sinomo_inotify_watch *watch = &instance->watches[watch_index];
        if (watch->active && strcmp(watch->path, path) == 0) {
            watch->mask = (mask & IN_MASK_ADD) != 0 ? (watch->mask | mask) : mask;
            return watch->wd;
        }
    }
    for (int watch_index = 0; watch_index < SINOMO_INOTIFY_WATCHES; watch_index++) {
        struct sinomo_inotify_watch *watch = &instance->watches[watch_index];
        if (!watch->active) {
            memset(watch, 0, sizeof(*watch));
            watch->active = 1;
            watch->wd = instance->next_wd++;
            watch->mask = mask;
            snprintf(watch->path, sizeof(watch->path), "%s", path);
            return watch->wd;
        }
    }
    errno = ENOSPC;
    return -1;
}

int __wrap_inotify_rm_watch(int fd, int wd) {
    int index = sinomo_inotify_index(fd);
    if (index < 0) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_inotify_instance *instance = &sinomo_inotify_instances[index];
    for (int watch_index = 0; watch_index < SINOMO_INOTIFY_WATCHES; watch_index++) {
        struct sinomo_inotify_watch *watch = &instance->watches[watch_index];
        if (watch->active && watch->wd == wd) {
            memset(watch, 0, sizeof(*watch));
            return sinomo_inotify_push_event(instance, wd, IN_IGNORED, NULL);
        }
    }
    errno = EINVAL;
    return -1;
}

int __wrap_fanotify_init(unsigned int flags, unsigned int event_f_flags) {
    unsigned int supported =
        FAN_CLOEXEC | FAN_NONBLOCK | FAN_CLASS_CONTENT | FAN_CLASS_PRE_CONTENT |
        FAN_UNLIMITED_QUEUE | FAN_UNLIMITED_MARKS;
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    int slot = -1;
    for (int index = 0; index < SINOMO_FANOTIFY_INSTANCES; index++) {
        if (!sinomo_fanotify_instances[index].active) {
            slot = index;
            break;
        }
    }
    if (slot < 0) {
        errno = EMFILE;
        return -1;
    }
    int event_flags = 0;
#ifdef EFD_CLOEXEC
    if ((flags & FAN_CLOEXEC) != 0) {
        event_flags |= EFD_CLOEXEC;
    }
#endif
#ifdef EFD_NONBLOCK
    if ((flags & FAN_NONBLOCK) != 0) {
        event_flags |= EFD_NONBLOCK;
    }
#endif
    int fd = __wrap_eventfd(0, event_flags);
    if (fd < 0) {
        return -1;
    }
    memset(&sinomo_fanotify_instances[slot], 0, sizeof(sinomo_fanotify_instances[slot]));
    sinomo_fanotify_instances[slot].active = 1;
    sinomo_fanotify_instances[slot].fd = fd;
    sinomo_fanotify_instances[slot].event_f_flags = event_f_flags;
    return fd;
}

int __wrap_fanotify_mark(int fd, unsigned int flags, uint64_t mask, int dirfd, const char *path) {
    unsigned int supported =
        FAN_MARK_ADD | FAN_MARK_REMOVE | FAN_MARK_DONT_FOLLOW | FAN_MARK_ONLYDIR |
        FAN_MARK_MOUNT | FAN_MARK_IGNORED_MASK | FAN_MARK_IGNORED_SURV_MODIFY |
        FAN_MARK_FLUSH | FAN_MARK_FILESYSTEM;
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    int index = sinomo_fanotify_index(fd);
    if (index < 0) {
        errno = EBADF;
        return -1;
    }
    struct sinomo_fanotify_instance *instance = &sinomo_fanotify_instances[index];
    if ((flags & FAN_MARK_FLUSH) != 0) {
        memset(instance->marks, 0, sizeof(instance->marks));
        return 0;
    }
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    struct stat st;
    if (fstatat(dirfd, path, &st, (flags & FAN_MARK_DONT_FOLLOW) != 0 ? AT_SYMLINK_NOFOLLOW : 0) != 0) {
        return -1;
    }
    if ((flags & FAN_MARK_ONLYDIR) != 0 && !S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        return -1;
    }
    char resolved[PATH_MAX];
    if (path[0] == '/') {
        snprintf(resolved, sizeof(resolved), "%s", path);
    } else if (dirfd == AT_FDCWD) {
        snprintf(resolved, sizeof(resolved), "%s", path);
    } else {
        snprintf(resolved, sizeof(resolved), "/proc/self/fd/%d/%s", dirfd, path);
    }
    for (int mark_index = 0; mark_index < SINOMO_FANOTIFY_MARKS; mark_index++) {
        struct sinomo_fanotify_mark *mark = &instance->marks[mark_index];
        if (mark->active && strcmp(mark->path, resolved) == 0) {
            if ((flags & FAN_MARK_REMOVE) != 0) {
                memset(mark, 0, sizeof(*mark));
                return 0;
            }
            mark->mask = (flags & FAN_MARK_ADD) != 0 ? (mark->mask | mask) : mask;
            return 0;
        }
    }
    if ((flags & FAN_MARK_REMOVE) != 0) {
        errno = ENOENT;
        return -1;
    }
    for (int mark_index = 0; mark_index < SINOMO_FANOTIFY_MARKS; mark_index++) {
        struct sinomo_fanotify_mark *mark = &instance->marks[mark_index];
        if (!mark->active) {
            mark->active = 1;
            snprintf(mark->path, sizeof(mark->path), "%s", resolved);
            mark->mask = mask;
            return 0;
        }
    }
    errno = ENOSPC;
    return -1;
}

static ssize_t sinomo_fanotify_read(int fd, void *buf, size_t count) {
    int index = sinomo_fanotify_index(fd);
    if (index < 0) {
        return -2;
    }
    if (buf == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (count < sizeof(struct sinomo_fanotify_event_metadata)) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_fanotify_instance *instance = &sinomo_fanotify_instances[index];
    for (;;) {
        if (instance->event_count > 0) {
            size_t written = 0;
            while (instance->event_count > 0 && count - written >= sizeof(struct sinomo_fanotify_event_metadata)) {
                struct sinomo_fanotify_event_record *event = &instance->events[instance->event_start];
                struct sinomo_fanotify_event_metadata metadata = {
                    .event_len = (uint32_t)sizeof(struct sinomo_fanotify_event_metadata),
                    .vers = FANOTIFY_METADATA_VERSION,
                    .reserved = 0,
                    .metadata_len = (uint16_t)sizeof(struct sinomo_fanotify_event_metadata),
                    .mask = event->mask,
                    .fd = FAN_NOFD,
                    .pid = (int32_t)event->pid,
                };
                memcpy((char *)buf + written, &metadata, sizeof(metadata));
                written += sizeof(metadata);
                memset(event, 0, sizeof(*event));
                instance->event_start = (instance->event_start + 1) % SINOMO_FANOTIFY_EVENTS;
                instance->event_count--;
            }
            return (ssize_t)written;
        }
        int fd_flags = __real_fcntl(fd, F_GETFL, 0);
        if (fd_flags >= 0 && (fd_flags & O_NONBLOCK) != 0) {
            errno = EAGAIN;
            return -1;
        }
        struct timespec delay = {0, 10000000L};
        if (nanosleep(&delay, NULL) != 0 && errno != EINTR) {
            return -1;
        }
    }
}

static ssize_t sinomo_inotify_read(int fd, void *buf, size_t count) {
    int index = sinomo_inotify_index(fd);
    if (index < 0) {
        return -2;
    }
    if (buf == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (count < sizeof(struct sinomo_inotify_event_wire)) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_inotify_instance *instance = &sinomo_inotify_instances[index];
    for (;;) {
        if (instance->event_count > 0) {
            char *out = (char *)buf;
            size_t written = 0;
            while (instance->event_count > 0) {
                struct sinomo_inotify_event_record *event = &instance->events[instance->event_start];
                size_t name_len = event->len;
                size_t record_len = sizeof(struct sinomo_inotify_event_wire) + name_len;
                if (written > 0 && written + record_len > count) {
                    break;
                }
                if (record_len > count - written) {
                    errno = EINVAL;
                    return -1;
                }
                struct sinomo_inotify_event_wire wire = {
                    .wd = event->wd,
                    .mask = event->mask,
                    .cookie = event->cookie,
                    .len = event->len,
                };
                memcpy(out + written, &wire, sizeof(wire));
                if (name_len > 0) {
                    memcpy(out + written + sizeof(wire), event->name, name_len);
                }
                written += record_len;
                memset(event, 0, sizeof(*event));
                instance->event_start = (instance->event_start + 1) % SINOMO_INOTIFY_EVENTS;
                instance->event_count--;
            }
            uint64_t drained = 0;
            (void)__wrap_eventfd_read(fd, &drained);
            if (instance->event_count > 0) {
                (void)__wrap_eventfd_write(fd, 1);
            }
            return (ssize_t)written;
        }
        int fd_flags = __real_fcntl(fd, F_GETFL, 0);
        if (fd_flags >= 0 && (fd_flags & O_NONBLOCK) != 0) {
            errno = EAGAIN;
            return -1;
        }
        struct timespec delay = {0, 10000000L};
        if (nanosleep(&delay, NULL) != 0 && errno != EINTR) {
            return -1;
        }
    }
}

int __wrap_dup(int oldfd) {
    __wasi_fd_t fd = 0;
    __wasi_errno_t rc = __wasi_fd_dup((__wasi_fd_t)oldfd, &fd);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    struct sinomo_pty_fd *pty = sinomo_pty_fd_entry(oldfd);
    if (pty != NULL && sinomo_pty_remember_fd((int)fd, pty->id, pty->master) != 0) {
        int saved_errno = errno;
        __real_close((int)fd);
        errno = saved_errno;
        return -1;
    }
    sinomo_socket_duplicate_metadata(oldfd, (int)fd);
    return (int)fd;
}

int __wrap_dup2(int oldfd, int newfd) {
    struct sinomo_pty_fd *old_pty = sinomo_pty_fd_entry(oldfd);
    int pty_id = old_pty == NULL ? -1 : old_pty->id;
    int pty_master = old_pty == NULL ? 0 : old_pty->master;
    __wasi_fd_t fd = 0;
    __wasi_errno_t rc = __wasi_fd_dup2((__wasi_fd_t)oldfd, (__wasi_fd_t)newfd, 0, &fd);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    if ((int)fd != oldfd) {
        sinomo_pty_forget_fd((int)fd);
        sinomo_socket_forget((int)fd);
    }
    if (pty_id >= 0 && sinomo_pty_remember_fd((int)fd, pty_id, pty_master) != 0) {
        int saved_errno = errno;
        __real_close((int)fd);
        errno = saved_errno;
        return -1;
    }
    sinomo_socket_duplicate_metadata(oldfd, (int)fd);
    return (int)fd;
}

int __wrap_dup3(int oldfd, int newfd, int flags) {
    int supported = 0;
#ifdef O_CLOEXEC
    supported |= O_CLOEXEC;
#endif
    if ((flags & ~supported) != 0 || oldfd == newfd) {
        errno = EINVAL;
        return -1;
    }
    int fd = __wrap_dup2(oldfd, newfd);
    if (fd < 0) {
        return -1;
    }
    if (sinomo_apply_fd_flags(fd, flags) != 0) {
        int saved_errno = errno;
        close(fd);
        errno = saved_errno;
        return -1;
    }
    return fd;
}

int __wrap_close_range(unsigned int first, unsigned int last, int flags) {
    if ((flags & ~(CLOSE_RANGE_UNSHARE | CLOSE_RANGE_CLOEXEC)) != 0) {
        errno = EINVAL;
        return -1;
    }
    if (first > last) {
        errno = EINVAL;
        return -1;
    }
    if (last == UINT_MAX) {
        struct rlimit limit;
        if (__wrap_getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur != RLIM_INFINITY && limit.rlim_cur > 0) {
            last = limit.rlim_cur > (rlim_t)UINT_MAX ? UINT_MAX : (unsigned int)(limit.rlim_cur - 1);
        }
    }
    for (unsigned int fd = first; fd <= last; fd++) {
        if ((flags & CLOSE_RANGE_CLOEXEC) != 0) {
            if (__real_fcntl((int)fd, F_SETFD, FD_CLOEXEC) != 0 && errno != EBADF) {
                return -1;
            }
        } else if (close((int)fd) != 0 && errno != EBADF) {
            return -1;
        }
        if (fd == UINT_MAX) {
            break;
        }
    }
    return 0;
}

void __wrap_closefrom(int lowfd) {
    if (lowfd < 0) {
        lowfd = 0;
    }
    (void)__wrap_close_range((unsigned int)lowfd, UINT_MAX, 0);
}

static __wasi_address_family_t sinomo_wasi_address_family(int domain) {
    switch (domain) {
#ifdef AF_UNIX
        case AF_UNIX:
            return __WASI_ADDRESS_FAMILY_UNIX;
#endif
#ifdef AF_INET
        case AF_INET:
            return __WASI_ADDRESS_FAMILY_INET4;
#endif
#ifdef AF_INET6
        case AF_INET6:
            return __WASI_ADDRESS_FAMILY_INET6;
#endif
        case AF_UNSPEC:
            return __WASI_ADDRESS_FAMILY_UNSPEC;
        default:
            return UINT8_MAX;
    }
}

static __wasi_sock_type_t sinomo_wasi_socket_type(int type) {
    switch (type) {
        case SOCK_STREAM:
            return __WASI_SOCK_TYPE_SOCKET_STREAM;
        case SOCK_DGRAM:
            return __WASI_SOCK_TYPE_SOCKET_DGRAM;
#ifdef SOCK_RAW
        case SOCK_RAW:
            return __WASI_SOCK_TYPE_SOCKET_RAW;
#endif
#ifdef SOCK_SEQPACKET
        case SOCK_SEQPACKET:
            return __WASI_SOCK_TYPE_SOCKET_SEQPACKET;
#endif
        default:
            return __WASI_SOCK_TYPE_SOCKET_UNUSED;
    }
}

static __wasi_sock_proto_t sinomo_wasi_socket_proto(int type, int protocol) {
    if (protocol != 0) {
        return (__wasi_sock_proto_t)protocol;
    }
    switch (type) {
        case SOCK_STREAM:
            return __WASI_SOCK_PROTO_TCP;
        case SOCK_DGRAM:
            return __WASI_SOCK_PROTO_UDP;
        default:
            return __WASI_SOCK_PROTO_IP;
    }
}

static int sinomo_socket_supported_type(int type, int supported_flags) {
    return (type & ~supported_flags & ~SOCK_STREAM & ~SOCK_DGRAM
#ifdef SOCK_RAW
        & ~SOCK_RAW
#endif
#ifdef SOCK_SEQPACKET
        & ~SOCK_SEQPACKET
#endif
    ) == 0;
}

int __wrap_socket(int domain, int type, int protocol) {
    int supported_flags = 0;
#ifdef SOCK_CLOEXEC
    supported_flags |= SOCK_CLOEXEC;
#endif
#ifdef SOCK_NONBLOCK
    supported_flags |= SOCK_NONBLOCK;
#endif
    int socket_flags = type & supported_flags;
    int socket_type = type & ~supported_flags;
    __wasi_address_family_t wasi_domain = sinomo_wasi_address_family(domain);
    __wasi_sock_type_t wasi_type = sinomo_wasi_socket_type(socket_type);
    if (!sinomo_socket_supported_type(type, supported_flags) || wasi_domain == UINT8_MAX || wasi_type == __WASI_SOCK_TYPE_SOCKET_UNUSED) {
        errno = EAFNOSUPPORT;
        return -1;
    }
    __wasi_fd_t fd = 0;
    __wasi_errno_t rc = __wasi_sock_open(wasi_domain, wasi_type, sinomo_wasi_socket_proto(socket_type, protocol), &fd);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    int fd_flags = 0;
#ifdef SOCK_CLOEXEC
    if ((socket_flags & SOCK_CLOEXEC) != 0) {
        fd_flags |= O_CLOEXEC;
    }
#endif
#ifdef SOCK_NONBLOCK
    if ((socket_flags & SOCK_NONBLOCK) != 0) {
        fd_flags |= O_NONBLOCK;
    }
#endif
    if (sinomo_apply_fd_flags((int)fd, fd_flags) != 0) {
        int saved_errno = errno;
        __real_close((int)fd);
        errno = saved_errno;
        return -1;
    }
    sinomo_socket_remember((int)fd, domain, socket_type, (int)sinomo_wasi_socket_proto(socket_type, protocol));
    return (int)fd;
}

static int sinomo_sockaddr_to_wasi(const struct sockaddr *addr, socklen_t addrlen, __wasi_addr_port_t *target) {
    if (addr == NULL || target == NULL) {
        errno = EFAULT;
        return -1;
    }
    memset(target, 0, sizeof(*target));
    switch (addr->sa_family) {
        case AF_INET: {
            if (addrlen < (socklen_t)sizeof(struct sockaddr_in)) {
                errno = EINVAL;
                return -1;
            }
            const struct sockaddr_in *in = (const struct sockaddr_in *)addr;
            target->tag = __WASI_ADDRESS_FAMILY_INET4;
            target->u.inet4.port = in->sin_port;
            memcpy(&target->u.inet4.addr, &in->sin_addr, 4);
            return 0;
        }
        case AF_INET6: {
            if (addrlen < (socklen_t)sizeof(struct sockaddr_in6)) {
                errno = EINVAL;
                return -1;
            }
            const struct sockaddr_in6 *in6 = (const struct sockaddr_in6 *)addr;
            target->tag = __WASI_ADDRESS_FAMILY_INET6;
            target->u.inet6.port = in6->sin6_port;
            memcpy(&target->u.inet6.addr, &in6->sin6_addr, 16);
            return 0;
        }
#ifdef AF_UNIX
        case AF_UNIX: {
            if (addrlen < (socklen_t)offsetof(struct sockaddr_un, sun_path)) {
                errno = EINVAL;
                return -1;
            }
            const struct sockaddr_un *un = (const struct sockaddr_un *)addr;
            size_t path_space = addrlen > (socklen_t)offsetof(struct sockaddr_un, sun_path)
                ? (size_t)(addrlen - (socklen_t)offsetof(struct sockaddr_un, sun_path))
                : 0;
            if (path_space > sizeof(target->u.unix)) {
                path_space = sizeof(target->u.unix);
            }
            target->tag = __WASI_ADDRESS_FAMILY_UNIX;
            memcpy(&target->u.unix, un->sun_path, path_space);
            return 0;
        }
#endif
        default:
            errno = EAFNOSUPPORT;
            return -1;
    }
}

static socklen_t sinomo_sockaddr_from_wasi(const __wasi_addr_port_t *source, struct sockaddr *addr, socklen_t addr_capacity) {
    if (source == NULL || addr == NULL || addr_capacity == 0) {
        return 0;
    }
    if (source->tag == __WASI_ADDRESS_FAMILY_INET4) {
        struct sockaddr_in out;
        memset(&out, 0, sizeof(out));
        out.sin_family = AF_INET;
        out.sin_port = source->u.inet4.port;
        unsigned char *octets = (unsigned char *)&source->u.inet4.addr;
        memcpy(&out.sin_addr, octets, 4);
        socklen_t needed = (socklen_t)sizeof(out);
        memcpy(addr, &out, addr_capacity < needed ? addr_capacity : needed);
        return needed;
    }
    if (source->tag == __WASI_ADDRESS_FAMILY_INET6) {
        struct sockaddr_in6 out;
        memset(&out, 0, sizeof(out));
        out.sin6_family = AF_INET6;
        out.sin6_port = source->u.inet6.port;
        unsigned char *octets = (unsigned char *)&source->u.inet6.addr;
        memcpy(&out.sin6_addr, octets, 16);
        socklen_t needed = (socklen_t)sizeof(out);
        memcpy(addr, &out, addr_capacity < needed ? addr_capacity : needed);
        return needed;
    }
    if (source->tag == __WASI_ADDRESS_FAMILY_UNIX) {
        struct sockaddr_un out;
        memset(&out, 0, sizeof(out));
        out.sun_family = AF_UNIX;
        memcpy(out.sun_path, &source->u.unix, sizeof(out.sun_path) < sizeof(source->u.unix) ? sizeof(out.sun_path) : sizeof(source->u.unix));
        socklen_t needed = (socklen_t)sizeof(out);
        memcpy(addr, &out, addr_capacity < needed ? addr_capacity : needed);
        return needed;
    }
    return 0;
}

int __wrap_bind(int fd, const struct sockaddr *addr, socklen_t addrlen) {
    __wasi_addr_port_t wasi_addr;
    if (sinomo_sockaddr_to_wasi(addr, addrlen, &wasi_addr) != 0) {
        return -1;
    }
    __wasi_errno_t rc = __wasi_sock_bind((__wasi_fd_t)fd, &wasi_addr);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    return 0;
}

int __wrap_connect(int fd, const struct sockaddr *addr, socklen_t addrlen) {
    __wasi_addr_port_t wasi_addr;
    if (sinomo_sockaddr_to_wasi(addr, addrlen, &wasi_addr) != 0) {
        return -1;
    }
    __wasi_errno_t rc = __wasi_sock_connect((__wasi_fd_t)fd, &wasi_addr);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    return 0;
}

int __wrap_listen(int fd, int backlog) {
    if (backlog < 0) {
        errno = EINVAL;
        return -1;
    }
    __wasi_errno_t rc = __wasi_sock_listen((__wasi_fd_t)fd, (size_t)backlog);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    return 0;
}

static int sinomo_sockaddr_query_common(int fd, struct sockaddr *addr, socklen_t *addrlen, int peer) {
    if (addrlen == NULL) {
        errno = EFAULT;
        return -1;
    }
    __wasi_addr_port_t wasi_addr;
    memset(&wasi_addr, 0, sizeof(wasi_addr));
    __wasi_errno_t rc = peer
        ? __wasi_sock_addr_peer((__wasi_fd_t)fd, &wasi_addr)
        : __wasi_sock_addr_local((__wasi_fd_t)fd, &wasi_addr);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    socklen_t capacity = addr == NULL ? 0 : *addrlen;
    socklen_t needed = sinomo_sockaddr_from_wasi(&wasi_addr, addr, capacity);
    *addrlen = needed;
    return 0;
}

int __wrap_getsockname(int fd, struct sockaddr *addr, socklen_t *addrlen) {
    return sinomo_sockaddr_query_common(fd, addr, addrlen, 0);
}

int __wrap_getpeername(int fd, struct sockaddr *addr, socklen_t *addrlen) {
    return sinomo_sockaddr_query_common(fd, addr, addrlen, 1);
}

int __wrap_accept4(int fd, struct sockaddr *addr, socklen_t *addrlen, int flags) {
    int supported = 0;
#ifdef SOCK_CLOEXEC
    supported |= SOCK_CLOEXEC;
#endif
#ifdef SOCK_NONBLOCK
    supported |= SOCK_NONBLOCK;
#endif
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    __wasi_fdflags_t wasi_flags = 0;
#ifdef SOCK_NONBLOCK
    if ((flags & SOCK_NONBLOCK) != 0) {
        wasi_flags |= __WASI_FDFLAGS_NONBLOCK;
    }
#endif
    __wasi_fd_t accepted = 0;
    __wasi_addr_port_t peer;
    memset(&peer, 0, sizeof(peer));
    __wasi_errno_t rc = __wasi_sock_accept_v2((__wasi_fd_t)fd, wasi_flags, &accepted, &peer);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
#ifdef SOCK_CLOEXEC
    if ((flags & SOCK_CLOEXEC) != 0 && sinomo_apply_fd_flags((int)accepted, O_CLOEXEC) != 0) {
        int saved_errno = errno;
        __real_close((int)accepted);
        errno = saved_errno;
        return -1;
    }
#endif
    if (addrlen != NULL) {
        socklen_t capacity = *addrlen;
        socklen_t needed = sinomo_sockaddr_from_wasi(&peer, addr, capacity);
        *addrlen = needed;
    }
    struct sinomo_socket_metadata *listener = sinomo_socket_metadata_entry(fd);
    if (listener != NULL) {
        sinomo_socket_remember((int)accepted, listener->domain, listener->type, listener->protocol);
    }
    return (int)accepted;
}

int __wrap_accept(int fd, struct sockaddr *addr, socklen_t *addrlen) {
    return __wrap_accept4(fd, addr, addrlen, 0);
}

static __wasi_riflags_t sinomo_wasi_recv_flags(int flags) {
    __wasi_riflags_t wasi_flags = 0;
#ifdef MSG_PEEK
    if ((flags & MSG_PEEK) != 0) {
        wasi_flags |= __WASI_RIFLAGS_RECV_PEEK;
    }
#endif
#ifdef MSG_WAITALL
    if ((flags & MSG_WAITALL) != 0) {
        wasi_flags |= __WASI_RIFLAGS_RECV_WAITALL;
    }
#endif
    return wasi_flags;
}

static void sinomo_posix_recv_flags(__wasi_roflags_t wasi_flags, int *flags) {
    if (flags == NULL) {
        return;
    }
#ifdef MSG_TRUNC
    if ((wasi_flags & __WASI_ROFLAGS_RECV_DATA_TRUNCATED) != 0) {
        *flags |= MSG_TRUNC;
    }
#else
    (void)wasi_flags;
#endif
}

static int sinomo_wasi_iov_from_iov(const struct iovec *iov, size_t iovcnt, __wasi_iovec_t *out) {
    if (iovcnt == 0) {
        return 0;
    }
    if (iov == NULL || out == NULL) {
        errno = EFAULT;
        return -1;
    }
    for (size_t i = 0; i < iovcnt; i++) {
        out[i].buf = (uint8_t *)iov[i].iov_base;
        out[i].buf_len = (__wasi_size_t)iov[i].iov_len;
    }
    return 0;
}

static int sinomo_wasi_ciov_from_iov(const struct iovec *iov, size_t iovcnt, __wasi_ciovec_t *out) {
    if (iovcnt == 0) {
        return 0;
    }
    if (iov == NULL || out == NULL) {
        errno = EFAULT;
        return -1;
    }
    for (size_t i = 0; i < iovcnt; i++) {
        out[i].buf = (const uint8_t *)iov[i].iov_base;
        out[i].buf_len = (__wasi_size_t)iov[i].iov_len;
    }
    return 0;
}

static int sinomo_socket_begin_dontwait(int fd, int flags, int *old_flags) {
    if (old_flags == NULL) {
        errno = EFAULT;
        return -1;
    }
    *old_flags = -1;
#ifdef MSG_DONTWAIT
    if ((flags & MSG_DONTWAIT) == 0) {
        return 0;
    }
    int current = __real_fcntl(fd, F_GETFL, 0);
    if (current < 0) {
        return -1;
    }
    if ((current & O_NONBLOCK) != 0) {
        return 0;
    }
    if (__real_fcntl(fd, F_SETFL, current | O_NONBLOCK) != 0) {
        return -1;
    }
    *old_flags = current;
#else
    (void)fd;
    (void)flags;
#endif
    return 0;
}

static void sinomo_socket_end_dontwait(int fd, int old_flags, int *saved_errno) {
    if (old_flags < 0) {
        return;
    }
    int restore_errno = errno;
    if (__real_fcntl(fd, F_SETFL, old_flags) != 0 && saved_errno != NULL && *saved_errno == 0) {
        *saved_errno = errno;
    }
    errno = restore_errno;
}

ssize_t __wrap_sendto(int fd, const void *buf, size_t length, int flags, const struct sockaddr *addr, socklen_t addrlen) {
    __wasi_ciovec_t iov;
    iov.buf = (const uint8_t *)buf;
    iov.buf_len = (__wasi_size_t)length;
    __wasi_size_t written = 0;
    __wasi_errno_t rc;
    int old_flags = -1;
    if (addr != NULL) {
        __wasi_addr_port_t wasi_addr;
        if (sinomo_sockaddr_to_wasi(addr, addrlen, &wasi_addr) != 0) {
            return -1;
        }
        if (sinomo_socket_begin_dontwait(fd, flags, &old_flags) != 0) {
            return -1;
        }
        rc = __wasi_sock_send_to((__wasi_fd_t)fd, &iov, 1, (__wasi_siflags_t)0, &wasi_addr, &written);
    } else {
        if (sinomo_socket_begin_dontwait(fd, flags, &old_flags) != 0) {
            return -1;
        }
        rc = __wasi_sock_send((__wasi_fd_t)fd, &iov, 1, (__wasi_siflags_t)0, &written);
    }
    (void)flags;
    int saved_errno = rc == 0 ? 0 : (int)rc;
    sinomo_socket_end_dontwait(fd, old_flags, &saved_errno);
    if (rc != 0) {
        errno = saved_errno;
        return -1;
    }
    if (saved_errno != 0) {
        errno = saved_errno;
        return -1;
    }
    return (ssize_t)written;
}

ssize_t __wrap_send(int fd, const void *buf, size_t length, int flags) {
    return __wrap_sendto(fd, buf, length, flags, NULL, 0);
}

ssize_t __wrap_recvfrom(int fd, void *buf, size_t length, int flags, struct sockaddr *addr, socklen_t *addrlen) {
    __wasi_iovec_t iov;
    iov.buf = (uint8_t *)buf;
    iov.buf_len = (__wasi_size_t)length;
    __wasi_size_t received = 0;
    __wasi_roflags_t out_flags = 0;
    __wasi_errno_t rc;
    int old_flags = -1;
    if (addr != NULL || addrlen != NULL) {
        __wasi_addr_port_t peer;
        memset(&peer, 0, sizeof(peer));
        if (sinomo_socket_begin_dontwait(fd, flags, &old_flags) != 0) {
            return -1;
        }
        rc = __wasi_sock_recv_from((__wasi_fd_t)fd, &iov, 1, sinomo_wasi_recv_flags(flags), &received, &out_flags, &peer);
        if (rc == 0 && addrlen != NULL) {
            socklen_t capacity = addr == NULL ? 0 : *addrlen;
            *addrlen = sinomo_sockaddr_from_wasi(&peer, addr, capacity);
        }
    } else {
        if (sinomo_socket_begin_dontwait(fd, flags, &old_flags) != 0) {
            return -1;
        }
        rc = __wasi_sock_recv((__wasi_fd_t)fd, &iov, 1, sinomo_wasi_recv_flags(flags), &received, &out_flags);
    }
    int saved_errno = rc == 0 ? 0 : (int)rc;
    sinomo_socket_end_dontwait(fd, old_flags, &saved_errno);
    if (rc != 0) {
        errno = saved_errno;
        return -1;
    }
    if (saved_errno != 0) {
        errno = saved_errno;
        return -1;
    }
    return (ssize_t)received;
}

ssize_t __wrap_recv(int fd, void *buf, size_t length, int flags) {
    return __wrap_recvfrom(fd, buf, length, flags, NULL, NULL);
}

ssize_t __wrap_sendmsg(int fd, const struct msghdr *msg, int flags) {
    if (msg == NULL) {
        errno = EFAULT;
        return -1;
    }
    __wasi_ciovec_t stack_iov[16];
    __wasi_ciovec_t *iov = stack_iov;
    if (msg->msg_iovlen > (int)(sizeof(stack_iov) / sizeof(stack_iov[0]))) {
        iov = calloc((size_t)msg->msg_iovlen, sizeof(*iov));
        if (iov == NULL) {
            errno = ENOMEM;
            return -1;
        }
    }
    if (sinomo_wasi_ciov_from_iov(msg->msg_iov, (size_t)msg->msg_iovlen, iov) != 0) {
        if (iov != stack_iov) {
            free(iov);
        }
        return -1;
    }
    __wasi_size_t written = 0;
    __wasi_errno_t rc;
    int old_flags = -1;
    if (msg->msg_name != NULL) {
        __wasi_addr_port_t wasi_addr;
        if (sinomo_sockaddr_to_wasi((const struct sockaddr *)msg->msg_name, (socklen_t)msg->msg_namelen, &wasi_addr) != 0) {
            if (iov != stack_iov) {
                free(iov);
            }
            return -1;
        }
        if (sinomo_socket_begin_dontwait(fd, flags, &old_flags) != 0) {
            if (iov != stack_iov) {
                free(iov);
            }
            return -1;
        }
        rc = __wasi_sock_send_to((__wasi_fd_t)fd, iov, (size_t)msg->msg_iovlen, (__wasi_siflags_t)0, &wasi_addr, &written);
    } else {
        if (sinomo_socket_begin_dontwait(fd, flags, &old_flags) != 0) {
            if (iov != stack_iov) {
                free(iov);
            }
            return -1;
        }
        rc = __wasi_sock_send((__wasi_fd_t)fd, iov, (size_t)msg->msg_iovlen, (__wasi_siflags_t)0, &written);
    }
    (void)flags;
    int saved_errno = rc == 0 ? 0 : (int)rc;
    sinomo_socket_end_dontwait(fd, old_flags, &saved_errno);
    if (iov != stack_iov) {
        free(iov);
    }
    if (rc != 0) {
        errno = saved_errno;
        return -1;
    }
    if (saved_errno != 0) {
        errno = saved_errno;
        return -1;
    }
    return (ssize_t)written;
}

ssize_t __wrap_recvmsg(int fd, struct msghdr *msg, int flags) {
    if (msg == NULL) {
        errno = EFAULT;
        return -1;
    }
    __wasi_iovec_t stack_iov[16];
    __wasi_iovec_t *iov = stack_iov;
    if (msg->msg_iovlen > (int)(sizeof(stack_iov) / sizeof(stack_iov[0]))) {
        iov = calloc((size_t)msg->msg_iovlen, sizeof(*iov));
        if (iov == NULL) {
            errno = ENOMEM;
            return -1;
        }
    }
    if (sinomo_wasi_iov_from_iov(msg->msg_iov, (size_t)msg->msg_iovlen, iov) != 0) {
        if (iov != stack_iov) {
            free(iov);
        }
        return -1;
    }
    __wasi_size_t received = 0;
    __wasi_roflags_t out_flags = 0;
    __wasi_errno_t rc;
    int old_flags = -1;
    if (msg->msg_name != NULL) {
        __wasi_addr_port_t peer;
        memset(&peer, 0, sizeof(peer));
        if (sinomo_socket_begin_dontwait(fd, flags, &old_flags) != 0) {
            if (iov != stack_iov) {
                free(iov);
            }
            return -1;
        }
        rc = __wasi_sock_recv_from((__wasi_fd_t)fd, iov, (size_t)msg->msg_iovlen, sinomo_wasi_recv_flags(flags), &received, &out_flags, &peer);
        if (rc == 0) {
            msg->msg_namelen = sinomo_sockaddr_from_wasi(&peer, (struct sockaddr *)msg->msg_name, (socklen_t)msg->msg_namelen);
        }
    } else {
        if (sinomo_socket_begin_dontwait(fd, flags, &old_flags) != 0) {
            if (iov != stack_iov) {
                free(iov);
            }
            return -1;
        }
        rc = __wasi_sock_recv((__wasi_fd_t)fd, iov, (size_t)msg->msg_iovlen, sinomo_wasi_recv_flags(flags), &received, &out_flags);
    }
    int saved_errno = rc == 0 ? 0 : (int)rc;
    sinomo_socket_end_dontwait(fd, old_flags, &saved_errno);
    if (iov != stack_iov) {
        free(iov);
    }
    if (rc != 0) {
        errno = saved_errno;
        return -1;
    }
    if (saved_errno != 0) {
        errno = saved_errno;
        return -1;
    }
    msg->msg_flags = 0;
    sinomo_posix_recv_flags(out_flags, &msg->msg_flags);
    return (ssize_t)received;
}

int __wrap_shutdown(int fd, int how) {
    __wasi_sdflags_t wasi_how = 0;
    switch (how) {
        case SHUT_RD:
            wasi_how = __WASI_SDFLAGS_RD;
            break;
        case SHUT_WR:
            wasi_how = __WASI_SDFLAGS_WR;
            break;
        case SHUT_RDWR:
            wasi_how = __WASI_SDFLAGS_RD | __WASI_SDFLAGS_WR;
            break;
        default:
            errno = EINVAL;
            return -1;
    }
    __wasi_errno_t rc = __wasi_sock_shutdown((__wasi_fd_t)fd, wasi_how);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    return 0;
}

static int sinomo_sockopt_flag(int level, int option, __wasi_sock_option_t *out) {
    if (out == NULL) {
        errno = EFAULT;
        return 0;
    }
#ifdef SOL_SOCKET
    if (level == SOL_SOCKET) {
        switch (option) {
#ifdef SO_REUSEADDR
            case SO_REUSEADDR:
                *out = __WASI_SOCK_OPTION_REUSE_ADDR;
                return 1;
#endif
#ifdef SO_REUSEPORT
            case SO_REUSEPORT:
                *out = __WASI_SOCK_OPTION_REUSE_PORT;
                return 1;
#endif
#ifdef SO_KEEPALIVE
            case SO_KEEPALIVE:
                *out = __WASI_SOCK_OPTION_KEEP_ALIVE;
                return 1;
#endif
#ifdef SO_BROADCAST
            case SO_BROADCAST:
                *out = __WASI_SOCK_OPTION_BROADCAST;
                return 1;
#endif
#ifdef SO_DONTROUTE
            case SO_DONTROUTE:
                *out = __WASI_SOCK_OPTION_DONT_ROUTE;
                return 1;
#endif
#ifdef SO_OOBINLINE
            case SO_OOBINLINE:
                *out = __WASI_SOCK_OPTION_OOB_INLINE;
                return 1;
#endif
#ifdef SO_ACCEPTCONN
            case SO_ACCEPTCONN:
                *out = __WASI_SOCK_OPTION_LISTENING;
                return 1;
#endif
            default:
                break;
        }
    }
#endif
#ifdef IPPROTO_TCP
    if (level == IPPROTO_TCP) {
        switch (option) {
#ifdef TCP_NODELAY
            case TCP_NODELAY:
                *out = __WASI_SOCK_OPTION_NO_DELAY;
                return 1;
#endif
            default:
                break;
        }
    }
#endif
#ifdef IPPROTO_IPV6
    if (level == IPPROTO_IPV6) {
        switch (option) {
#ifdef IPV6_V6ONLY
            case IPV6_V6ONLY:
                *out = __WASI_SOCK_OPTION_ONLY_V6;
                return 1;
#endif
            default:
                break;
        }
    }
#endif
    return 0;
}

static int sinomo_sockopt_size(int level, int option, __wasi_sock_option_t *out) {
    if (out == NULL) {
        errno = EFAULT;
        return 0;
    }
#ifdef SOL_SOCKET
    if (level == SOL_SOCKET) {
        switch (option) {
#ifdef SO_RCVBUF
            case SO_RCVBUF:
                *out = __WASI_SOCK_OPTION_RECV_BUF_SIZE;
                return 1;
#endif
#ifdef SO_SNDBUF
            case SO_SNDBUF:
                *out = __WASI_SOCK_OPTION_SEND_BUF_SIZE;
                return 1;
#endif
#ifdef SO_RCVLOWAT
            case SO_RCVLOWAT:
                *out = __WASI_SOCK_OPTION_RECV_LOWAT;
                return 1;
#endif
#ifdef SO_SNDLOWAT
            case SO_SNDLOWAT:
                *out = __WASI_SOCK_OPTION_SEND_LOWAT;
                return 1;
#endif
#ifdef SO_TYPE
            case SO_TYPE:
                *out = __WASI_SOCK_OPTION_TYPE;
                return 1;
#endif
#ifdef SO_PROTOCOL
            case SO_PROTOCOL:
                *out = __WASI_SOCK_OPTION_PROTO;
                return 1;
#endif
#ifdef SO_ERROR
            case SO_ERROR:
                *out = __WASI_SOCK_OPTION_LAST_ERROR;
                return 1;
#endif
            default:
                break;
        }
    }
#endif
#ifdef IPPROTO_IP
    if (level == IPPROTO_IP) {
        switch (option) {
#ifdef IP_TTL
            case IP_TTL:
                *out = __WASI_SOCK_OPTION_TTL;
                return 1;
#endif
#ifdef IP_MULTICAST_TTL
            case IP_MULTICAST_TTL:
                *out = __WASI_SOCK_OPTION_MULTICAST_TTL_V4;
                return 1;
#endif
            default:
                break;
        }
    }
#endif
    return 0;
}

static int sinomo_sockopt_time(int level, int option, __wasi_sock_option_t *out) {
    if (out == NULL) {
        errno = EFAULT;
        return 0;
    }
#ifdef SOL_SOCKET
    if (level == SOL_SOCKET) {
        switch (option) {
#ifdef SO_RCVTIMEO
            case SO_RCVTIMEO:
                *out = __WASI_SOCK_OPTION_RECV_TIMEOUT;
                return 1;
#endif
#ifdef SO_SNDTIMEO
            case SO_SNDTIMEO:
                *out = __WASI_SOCK_OPTION_SEND_TIMEOUT;
                return 1;
#endif
#ifdef SO_LINGER
            case SO_LINGER:
                *out = __WASI_SOCK_OPTION_LINGER;
                return 1;
#endif
            default:
                break;
        }
    }
#endif
    return 0;
}

static int sinomo_copy_sockopt_int(const void *value, socklen_t value_len, int *out) {
    if (value == NULL || out == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (value_len < (socklen_t)sizeof(int)) {
        errno = EINVAL;
        return -1;
    }
    memcpy(out, value, sizeof(int));
    return 0;
}

static int sinomo_write_sockopt_int(void *value, socklen_t *value_len, int output) {
    if (value == NULL || value_len == NULL) {
        errno = EFAULT;
        return -1;
    }
    socklen_t capacity = *value_len;
    *value_len = (socklen_t)sizeof(int);
    if (capacity < (socklen_t)sizeof(int)) {
        errno = EINVAL;
        return -1;
    }
    memcpy(value, &output, sizeof(output));
    return 0;
}

static __wasi_timestamp_t sinomo_timeval_to_wasi_ns(const struct timeval *tv) {
    if (tv == NULL) {
        return 0;
    }
    return (__wasi_timestamp_t)tv->tv_sec * 1000000000ULL + (__wasi_timestamp_t)tv->tv_usec * 1000ULL;
}

static void sinomo_wasi_ns_to_timeval(__wasi_timestamp_t ns, struct timeval *tv) {
    tv->tv_sec = (time_t)(ns / 1000000000ULL);
    tv->tv_usec = (suseconds_t)((ns % 1000000000ULL) / 1000ULL);
}

int __wrap_setsockopt(int fd, int level, int option, const void *value, socklen_t value_len) {
    __wasi_sock_option_t wasi_opt = __WASI_SOCK_OPTION_NOOP;
    if (sinomo_sockopt_flag(level, option, &wasi_opt)) {
        int enabled = 0;
        if (sinomo_copy_sockopt_int(value, value_len, &enabled) != 0) {
            return -1;
        }
        __wasi_errno_t rc = __wasi_sock_set_opt_flag((__wasi_fd_t)fd, wasi_opt, enabled ? __WASI_BOOL_TRUE : __WASI_BOOL_FALSE);
        if (rc != 0) {
            errno = (int)rc;
            return -1;
        }
        return 0;
    }
    if (sinomo_sockopt_size(level, option, &wasi_opt)) {
        int size = 0;
        if (sinomo_copy_sockopt_int(value, value_len, &size) != 0) {
            return -1;
        }
        __wasi_errno_t rc = __wasi_sock_set_opt_size((__wasi_fd_t)fd, wasi_opt, (__wasi_filesize_t)size);
        if (rc != 0) {
            errno = (int)rc;
            return -1;
        }
        return 0;
    }
    if (sinomo_sockopt_time(level, option, &wasi_opt)) {
        __wasi_option_timestamp_t ts;
        memset(&ts, 0, sizeof(ts));
        ts.tag = __WASI_OPTION_SOME;
#ifdef SO_LINGER
        if (option == SO_LINGER) {
            if (value == NULL || value_len < (socklen_t)sizeof(struct linger)) {
                errno = value == NULL ? EFAULT : EINVAL;
                return -1;
            }
            const struct linger *linger = (const struct linger *)value;
            if (!linger->l_onoff) {
                ts.tag = __WASI_OPTION_NONE;
            } else {
                ts.u.some = (__wasi_timestamp_t)linger->l_linger * 1000000000ULL;
            }
        } else
#endif
        {
            if (value == NULL || value_len < (socklen_t)sizeof(struct timeval)) {
                errno = value == NULL ? EFAULT : EINVAL;
                return -1;
            }
            ts.u.some = sinomo_timeval_to_wasi_ns((const struct timeval *)value);
        }
        __wasi_errno_t rc = __wasi_sock_set_opt_time((__wasi_fd_t)fd, wasi_opt, &ts);
        if (rc != 0) {
            errno = (int)rc;
            return -1;
        }
        return 0;
    }
    errno = ENOPROTOOPT;
    return -1;
}

int __wrap_getsockopt(int fd, int level, int option, void *value, socklen_t *value_len) {
    __wasi_sock_option_t wasi_opt = __WASI_SOCK_OPTION_NOOP;
#ifdef SOL_SOCKET
    if (level == SOL_SOCKET) {
#ifdef SO_PEERCRED
        if (option == SO_PEERCRED) {
            if (value == NULL || value_len == NULL) {
                errno = EFAULT;
                return -1;
            }
            socklen_t capacity = *value_len;
            *value_len = (socklen_t)sizeof(struct ucred);
            if (capacity < (socklen_t)sizeof(struct ucred)) {
                errno = EINVAL;
                return -1;
            }
            struct ucred credentials;
            memset(&credentials, 0, sizeof(credentials));
            credentials.pid = __wrap_getpid();
            credentials.uid = __wrap_getuid();
            credentials.gid = __wrap_getgid();
            memcpy(value, &credentials, sizeof(credentials));
            return 0;
        }
#endif
        struct sinomo_socket_metadata *metadata = sinomo_socket_metadata_entry(fd);
        if (metadata != NULL) {
#ifdef SO_DOMAIN
            if (option == SO_DOMAIN) {
                return sinomo_write_sockopt_int(value, value_len, metadata->domain);
            }
#endif
#ifdef SO_TYPE
            if (option == SO_TYPE) {
                return sinomo_write_sockopt_int(value, value_len, metadata->type);
            }
#endif
#ifdef SO_PROTOCOL
            if (option == SO_PROTOCOL) {
                return sinomo_write_sockopt_int(value, value_len, metadata->protocol);
            }
#endif
        }
    }
#endif
    if (sinomo_sockopt_flag(level, option, &wasi_opt)) {
        __wasi_bool_t enabled = __WASI_BOOL_FALSE;
        __wasi_errno_t rc = __wasi_sock_get_opt_flag((__wasi_fd_t)fd, wasi_opt, &enabled);
        if (rc != 0) {
            errno = (int)rc;
            return -1;
        }
        return sinomo_write_sockopt_int(value, value_len, enabled == __WASI_BOOL_TRUE ? 1 : 0);
    }
    if (sinomo_sockopt_size(level, option, &wasi_opt)) {
        __wasi_filesize_t size = 0;
        __wasi_errno_t rc = __wasi_sock_get_opt_size((__wasi_fd_t)fd, wasi_opt, &size);
        if (rc != 0) {
            errno = (int)rc;
            return -1;
        }
        return sinomo_write_sockopt_int(value, value_len, (int)size);
    }
    if (sinomo_sockopt_time(level, option, &wasi_opt)) {
        if (value == NULL || value_len == NULL) {
            errno = EFAULT;
            return -1;
        }
        __wasi_option_timestamp_t ts;
        memset(&ts, 0, sizeof(ts));
        __wasi_errno_t rc = __wasi_sock_get_opt_time((__wasi_fd_t)fd, wasi_opt, &ts);
        if (rc != 0) {
            errno = (int)rc;
            return -1;
        }
#ifdef SO_LINGER
        if (option == SO_LINGER) {
            socklen_t capacity = *value_len;
            *value_len = (socklen_t)sizeof(struct linger);
            if (capacity < (socklen_t)sizeof(struct linger)) {
                errno = EINVAL;
                return -1;
            }
            struct linger out;
            memset(&out, 0, sizeof(out));
            if (ts.tag == __WASI_OPTION_SOME) {
                out.l_onoff = 1;
                out.l_linger = (int)(ts.u.some / 1000000000ULL);
            }
            memcpy(value, &out, sizeof(out));
            return 0;
        }
#endif
        socklen_t capacity = *value_len;
        *value_len = (socklen_t)sizeof(struct timeval);
        if (capacity < (socklen_t)sizeof(struct timeval)) {
            errno = EINVAL;
            return -1;
        }
        struct timeval out;
        memset(&out, 0, sizeof(out));
        if (ts.tag == __WASI_OPTION_SOME) {
            sinomo_wasi_ns_to_timeval(ts.u.some, &out);
        }
        memcpy(value, &out, sizeof(out));
        return 0;
    }
    errno = ENOPROTOOPT;
    return -1;
}

int __wrap_socketpair(int domain, int type, int protocol, int sv[2]) {
    int supported_flags = 0;
#ifdef SOCK_CLOEXEC
    supported_flags |= SOCK_CLOEXEC;
#endif
#ifdef SOCK_NONBLOCK
    supported_flags |= SOCK_NONBLOCK;
#endif
    int socket_flags = type & supported_flags;
    int socket_type = type & ~supported_flags;
    __wasi_address_family_t wasi_domain = sinomo_wasi_address_family(domain);
    __wasi_sock_type_t wasi_type = sinomo_wasi_socket_type(socket_type);
    if (sv == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (!sinomo_socket_supported_type(type, supported_flags) || wasi_domain == UINT8_MAX || wasi_type == __WASI_SOCK_TYPE_SOCKET_UNUSED) {
        errno = EAFNOSUPPORT;
        return -1;
    }
    __wasi_fd_t fd0 = 0;
    __wasi_fd_t fd1 = 0;
    __wasi_errno_t rc = __wasi_sock_pair(wasi_domain, wasi_type, sinomo_wasi_socket_proto(socket_type, protocol), &fd0, &fd1);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    sv[0] = (int)fd0;
    sv[1] = (int)fd1;
    int fd_flags = 0;
#ifdef SOCK_CLOEXEC
    if ((socket_flags & SOCK_CLOEXEC) != 0) {
        fd_flags |= O_CLOEXEC;
    }
#endif
#ifdef SOCK_NONBLOCK
    if ((socket_flags & SOCK_NONBLOCK) != 0) {
        fd_flags |= O_NONBLOCK;
    }
#endif
    if (sinomo_apply_fd_flags(sv[0], fd_flags) != 0 || sinomo_apply_fd_flags(sv[1], fd_flags) != 0) {
        int saved_errno = errno;
        close(sv[0]);
        close(sv[1]);
        errno = saved_errno;
        return -1;
    }
    int metadata_proto = (int)sinomo_wasi_socket_proto(socket_type, protocol);
    sinomo_socket_remember(sv[0], domain, socket_type, metadata_proto);
    sinomo_socket_remember(sv[1], domain, socket_type, metadata_proto);
    return 0;
}

int __wrap_posix_openpt(int flags) {
    int supported = O_RDWR | O_NOCTTY;
#ifdef O_CLOEXEC
    supported |= O_CLOEXEC;
#endif
#ifdef O_NONBLOCK
    supported |= O_NONBLOCK;
#endif
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_pty_slot *slot = sinomo_pty_allocate_slot();
    if (slot == NULL) {
        return -1;
    }
    int socket_type = SOCK_STREAM;
#ifdef SOCK_CLOEXEC
    if ((flags & O_CLOEXEC) != 0) {
        socket_type |= SOCK_CLOEXEC;
    }
#endif
#ifdef SOCK_NONBLOCK
    if ((flags & O_NONBLOCK) != 0) {
        socket_type |= SOCK_NONBLOCK;
    }
#endif
    int sv[2] = {-1, -1};
    if (__wrap_socketpair(AF_UNIX, socket_type, 0, sv) != 0) {
        memset(slot, 0, sizeof(*slot));
        return -1;
    }
    if (sinomo_pty_remember_fd(sv[0], slot->id, 1) != 0 || sinomo_pty_remember_fd(sv[1], slot->id, 0) != 0) {
        int saved_errno = errno;
        __real_close(sv[0]);
        __real_close(sv[1]);
        memset(slot, 0, sizeof(*slot));
        errno = saved_errno;
        return -1;
    }
    return sv[0];
}

int __wrap_grantpt(int fd) {
    struct sinomo_pty_fd *pty = sinomo_pty_fd_entry(fd);
    if (pty == NULL || !pty->master) {
        errno = ENOTTY;
        return -1;
    }
    return 0;
}

int __wrap_unlockpt(int fd) {
    struct sinomo_pty_fd *pty = sinomo_pty_fd_entry(fd);
    struct sinomo_pty_slot *slot = pty == NULL || !pty->master ? NULL : sinomo_pty_slot_by_id(pty->id);
    if (slot == NULL) {
        errno = ENOTTY;
        return -1;
    }
    slot->unlocked = 1;
    return 0;
}

int __wrap_ptsname_r(int fd, char *buf, size_t buflen) {
    if (buf == NULL) {
        return EINVAL;
    }
    struct sinomo_pty_fd *pty = sinomo_pty_fd_entry(fd);
    if (pty == NULL || !pty->master) {
        return ENOTTY;
    }
    char path[64];
    snprintf(path, sizeof(path), "/dev/pts/%d", pty->id);
    size_t path_len = strlen(path) + 1U;
    if (buflen < path_len) {
        return ERANGE;
    }
    memcpy(buf, path, path_len);
    return 0;
}

char *__wrap_ptsname(int fd) {
    static char path[64];
    int rc = __wrap_ptsname_r(fd, path, sizeof(path));
    if (rc != 0) {
        errno = rc;
        return NULL;
    }
    return path;
}

int __wrap_openpty(int *amaster, int *aslave, char *name, const struct termios *termp, const struct winsize *winp) {
    if (amaster == NULL || aslave == NULL) {
        errno = EFAULT;
        return -1;
    }
    int master = __wrap_posix_openpt(O_RDWR | O_NOCTTY);
    if (master < 0) {
        return -1;
    }
    if (__wrap_grantpt(master) != 0 || __wrap_unlockpt(master) != 0) {
        int saved_errno = errno;
        __real_close(master);
        errno = saved_errno;
        return -1;
    }
    struct sinomo_pty_fd *pty = sinomo_pty_fd_entry(master);
    struct sinomo_pty_slot *slot = pty == NULL ? NULL : sinomo_pty_slot_by_id(pty->id);
    if (slot == NULL) {
        __real_close(master);
        errno = ENOTTY;
        return -1;
    }
    if (termp != NULL) {
        slot->term = *termp;
    }
    if (winp != NULL) {
        slot->winsize = *winp;
    }
    int slave = sinomo_pty_open_slave_id(slot->id, O_RDWR | O_NOCTTY);
    if (slave < 0) {
        int saved_errno = errno;
        __real_close(master);
        errno = saved_errno;
        return -1;
    }
    if (name != NULL) {
        (void)__wrap_ptsname_r(master, name, 64);
    }
    *amaster = master;
    *aslave = slave;
    return 0;
}

int __wrap_login_tty(int fd) {
    if (!__wrap_isatty(fd)) {
        return -1;
    }
    if (__wrap_setsid() < 0 && errno != EPERM) {
        return -1;
    }
    if (__wrap_dup2(fd, STDIN_FILENO) < 0 || __wrap_dup2(fd, STDOUT_FILENO) < 0 || __wrap_dup2(fd, STDERR_FILENO) < 0) {
        return -1;
    }
    if (fd > STDERR_FILENO) {
        (void)__wrap_close(fd);
    }
    return 0;
}

pid_t __wrap_forkpty(int *amaster, char *name, const struct termios *termp, const struct winsize *winp) {
    int master = -1;
    int slave = -1;
    if (__wrap_openpty(&master, &slave, name, termp, winp) != 0) {
        return (pid_t)-1;
    }
    pid_t pid = __wrap_fork();
    if (pid < 0) {
        int saved_errno = errno;
        __wrap_close(master);
        __wrap_close(slave);
        errno = saved_errno;
        return (pid_t)-1;
    }
    if (pid == 0) {
        (void)__wrap_close(master);
        if (__wrap_login_tty(slave) != 0) {
            _exit(127);
        }
        return 0;
    }
    (void)__wrap_close(slave);
    if (amaster != NULL) {
        *amaster = master;
    } else {
        (void)__wrap_close(master);
    }
    return pid;
}

static void sinomo_to_wasi_epoll_event(const struct epoll_event *event, __wasi_epoll_event_t *wasi_event) {
    memset(wasi_event, 0, sizeof(*wasi_event));
    if (event == NULL) {
        return;
    }
    wasi_event->events = event->events;
    wasi_event->data.fd = event->data.fd;
    wasi_event->data.data2 = event->data.u64;
}

static void sinomo_from_wasi_epoll_event(const __wasi_epoll_event_t *wasi_event, struct epoll_event *event) {
    memset(event, 0, sizeof(*event));
    event->events = wasi_event->events;
    event->data.u64 = wasi_event->data.data2;
    if (event->data.u64 == 0) {
        event->data.fd = wasi_event->data.fd;
    }
}

#define SINOMO_EPOLL_INSTANCES 64
#define SINOMO_EPOLL_SPECIAL_ENTRIES 256

struct sinomo_epoll_special_entry {
    int active;
    int fd;
    struct epoll_event event;
};

struct sinomo_epoll_shadow {
    int active;
    int epfd;
    struct sinomo_epoll_special_entry entries[SINOMO_EPOLL_SPECIAL_ENTRIES];
};

static struct sinomo_epoll_shadow sinomo_epoll_shadows[SINOMO_EPOLL_INSTANCES];

static int sinomo_epoll_shadow_index(int epfd) {
    for (int index = 0; index < SINOMO_EPOLL_INSTANCES; index++) {
        if (sinomo_epoll_shadows[index].active && sinomo_epoll_shadows[index].epfd == epfd) {
            return index;
        }
    }
    return -1;
}

static void sinomo_epoll_remember(int epfd) {
    if (sinomo_epoll_shadow_index(epfd) >= 0) {
        return;
    }
    for (int index = 0; index < SINOMO_EPOLL_INSTANCES; index++) {
        if (!sinomo_epoll_shadows[index].active) {
            memset(&sinomo_epoll_shadows[index], 0, sizeof(sinomo_epoll_shadows[index]));
            sinomo_epoll_shadows[index].active = 1;
            sinomo_epoll_shadows[index].epfd = epfd;
            return;
        }
    }
}

static void sinomo_epoll_forget(int epfd) {
    int index = sinomo_epoll_shadow_index(epfd);
    if (index >= 0) {
        memset(&sinomo_epoll_shadows[index], 0, sizeof(sinomo_epoll_shadows[index]));
    }
}

static void sinomo_epoll_shadow_ctl(int epfd, int op, int fd, const struct epoll_event *event) {
    int shadow_index = sinomo_epoll_shadow_index(epfd);
    if (shadow_index < 0) {
        return;
    }
    struct sinomo_epoll_shadow *shadow = &sinomo_epoll_shadows[shadow_index];
    for (int index = 0; index < SINOMO_EPOLL_SPECIAL_ENTRIES; index++) {
        struct sinomo_epoll_special_entry *entry = &shadow->entries[index];
        if (entry->active && entry->fd == fd) {
            if (op == EPOLL_CTL_DEL || !sinomo_special_fd_known(fd)) {
                memset(entry, 0, sizeof(*entry));
            } else if (event != NULL) {
                entry->event = *event;
            }
            return;
        }
    }
    if (op == EPOLL_CTL_DEL || event == NULL || !sinomo_special_fd_known(fd)) {
        return;
    }
    for (int index = 0; index < SINOMO_EPOLL_SPECIAL_ENTRIES; index++) {
        struct sinomo_epoll_special_entry *entry = &shadow->entries[index];
        if (!entry->active) {
            memset(entry, 0, sizeof(*entry));
            entry->active = 1;
            entry->fd = fd;
            entry->event = *event;
            return;
        }
    }
}

static int sinomo_epoll_collect_special(int epfd, struct epoll_event *events, int maxevents) {
    int shadow_index = sinomo_epoll_shadow_index(epfd);
    if (shadow_index < 0) {
        return 0;
    }
    struct sinomo_epoll_shadow *shadow = &sinomo_epoll_shadows[shadow_index];
    int ready = 0;
    for (int index = 0; index < SINOMO_EPOLL_SPECIAL_ENTRIES && ready < maxevents; index++) {
        struct sinomo_epoll_special_entry *entry = &shadow->entries[index];
        if (
            entry->active
            && (entry->event.events & (EPOLLIN | EPOLLPRI)) != 0
            && sinomo_special_fd_readable(entry->fd)
        ) {
            events[ready] = entry->event;
            events[ready].events |= EPOLLIN;
            ready++;
        }
    }
    return ready;
}

static int sinomo_epoll_special_timeout_ms(int epfd) {
    int shadow_index = sinomo_epoll_shadow_index(epfd);
    if (shadow_index < 0) {
        return -1;
    }
    struct sinomo_epoll_shadow *shadow = &sinomo_epoll_shadows[shadow_index];
    int timeout = -1;
    for (int index = 0; index < SINOMO_EPOLL_SPECIAL_ENTRIES; index++) {
        struct sinomo_epoll_special_entry *entry = &shadow->entries[index];
        if (!entry->active || (entry->event.events & (EPOLLIN | EPOLLPRI)) == 0) {
            continue;
        }
        int fd_timeout = sinomo_special_fd_next_timeout_ms(entry->fd);
        if (fd_timeout >= 0 && (timeout < 0 || fd_timeout < timeout)) {
            timeout = fd_timeout;
        }
    }
    return timeout;
}

int __wrap_epoll_create(int size) {
    if (size <= 0) {
        errno = EINVAL;
        return -1;
    }
    __wasi_fd_t fd = 0;
    __wasi_errno_t rc = __wasi_epoll_create(&fd);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    sinomo_epoll_remember((int)fd);
    return (int)fd;
}

int __wrap_epoll_create1(int flags) {
    int supported = 0;
#ifdef EPOLL_CLOEXEC
    supported |= EPOLL_CLOEXEC;
#endif
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    int fd = __wrap_epoll_create(1);
    if (fd < 0) {
        return -1;
    }
    if (sinomo_apply_fd_flags(fd, flags & EPOLL_CLOEXEC ? O_CLOEXEC : 0) != 0) {
        int saved_errno = errno;
        close(fd);
        errno = saved_errno;
        return -1;
    }
    return fd;
}

int __wrap_epoll_ctl(int epfd, int op, int fd, struct epoll_event *event) {
    if (event == NULL && op != EPOLL_CTL_DEL) {
        errno = EFAULT;
        return -1;
    }
    __wasi_epoll_event_t wasi_event;
    sinomo_to_wasi_epoll_event(event, &wasi_event);
    __wasi_errno_t rc = __wasi_epoll_ctl((__wasi_fd_t)epfd, (__wasi_epoll_ctl_t)op, (__wasi_fd_t)fd, &wasi_event);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    sinomo_epoll_shadow_ctl(epfd, op, fd, event);
    return 0;
}

int __wrap_epoll_wait(int epfd, struct epoll_event *events, int maxevents, int timeout) {
    if (events == NULL || maxevents <= 0) {
        errno = EINVAL;
        return -1;
    }
    int special_ready = sinomo_epoll_collect_special(epfd, events, maxevents);
    if (special_ready > 0 || timeout == 0) {
        return special_ready;
    }
    int adjusted_timeout = timeout;
    int special_timeout = sinomo_epoll_special_timeout_ms(epfd);
    if (special_timeout >= 0 && (adjusted_timeout < 0 || special_timeout < adjusted_timeout)) {
        adjusted_timeout = special_timeout;
    }
    __wasi_epoll_event_t *wasi_events = calloc((size_t)maxevents, sizeof(__wasi_epoll_event_t));
    if (wasi_events == NULL) {
        errno = ENOMEM;
        return -1;
    }
    __wasi_size_t event_count = 0;
    __wasi_timestamp_t timeout_ns = adjusted_timeout < 0 ? UINT64_MAX : (uint64_t)adjusted_timeout * 1000000ULL;
    __wasi_errno_t rc = __wasi_epoll_wait((__wasi_fd_t)epfd, wasi_events, (size_t)maxevents, timeout_ns, &event_count);
    if (rc != 0) {
        free(wasi_events);
        errno = (int)rc;
        return -1;
    }
    for (size_t index = 0; index < event_count && index < (size_t)maxevents; index++) {
        sinomo_from_wasi_epoll_event(&wasi_events[index], &events[index]);
    }
    int count = (int)(event_count > (size_t)maxevents ? (size_t)maxevents : event_count);
    if (count < maxevents) {
        count += sinomo_epoll_collect_special(epfd, events + count, maxevents - count);
    }
    free(wasi_events);
    return count;
}

int __wrap_epoll_pwait(int epfd, struct epoll_event *events, int maxevents, int timeout, const sigset_t *sigmask) {
    (void)sigmask;
    return __wrap_epoll_wait(epfd, events, maxevents, timeout);
}

int __wrap_epoll_pwait2(int epfd, struct epoll_event *events, int maxevents, const struct timespec *timeout, const sigset_t *sigmask) {
    if (timeout == NULL) {
        return __wrap_epoll_pwait(epfd, events, maxevents, -1, sigmask);
    }
    if (timeout->tv_sec < 0 || timeout->tv_nsec < 0 || timeout->tv_nsec >= 1000000000L) {
        errno = EINVAL;
        return -1;
    }
    unsigned long long millis = (unsigned long long)timeout->tv_sec * 1000ULL;
    millis += ((unsigned long long)timeout->tv_nsec + 999999ULL) / 1000000ULL;
    int timeout_ms = millis > (unsigned long long)INT_MAX ? INT_MAX : (int)millis;
    return __wrap_epoll_pwait(epfd, events, maxevents, timeout_ms, sigmask);
}

int __wrap_poll(struct pollfd *fds, nfds_t nfds, int timeout) {
    if (fds == NULL && nfds > 0) {
        errno = EFAULT;
        return -1;
    }
    int ready = 0;
    int adjusted_timeout = timeout;
    for (nfds_t index = 0; index < nfds; index++) {
        fds[index].revents = 0;
        if (fds[index].fd < 0) {
            continue;
        }
        if ((fds[index].events & (POLLIN | POLLPRI)) != 0 && sinomo_special_fd_readable(fds[index].fd)) {
            fds[index].revents |= POLLIN;
            ready++;
            continue;
        }
        if ((fds[index].events & (POLLIN | POLLPRI)) != 0) {
            int fd_timeout = sinomo_special_fd_next_timeout_ms(fds[index].fd);
            if (fd_timeout >= 0 && (adjusted_timeout < 0 || fd_timeout < adjusted_timeout)) {
                adjusted_timeout = fd_timeout;
            }
        }
    }
    if (ready > 0 || timeout == 0) {
        return ready;
    }
    size_t max_subscriptions = (size_t)nfds * 2 + (adjusted_timeout >= 0 ? 1 : 0);
    if (max_subscriptions == 0) {
        return 0;
    }
    __wasi_subscription_t *subscriptions = calloc(max_subscriptions, sizeof(__wasi_subscription_t));
    __wasi_event_t *events = calloc(max_subscriptions, sizeof(__wasi_event_t));
    if (subscriptions == NULL || events == NULL) {
        free(subscriptions);
        free(events);
        errno = ENOMEM;
        return -1;
    }
    size_t subscription_count = 0;
    for (nfds_t index = 0; index < nfds; index++) {
        if (fds[index].fd < 0) {
            continue;
        }
        if ((fds[index].events & (POLLIN | POLLPRI)) != 0) {
            subscriptions[subscription_count].userdata = ((uint64_t)index << 8) | 1;
            subscriptions[subscription_count].u.tag = __WASI_EVENTTYPE_FD_READ;
            subscriptions[subscription_count].u.u.fd_read.file_descriptor = (__wasi_fd_t)fds[index].fd;
            subscription_count++;
        }
        if ((fds[index].events & POLLOUT) != 0) {
            subscriptions[subscription_count].userdata = ((uint64_t)index << 8) | 2;
            subscriptions[subscription_count].u.tag = __WASI_EVENTTYPE_FD_WRITE;
            subscriptions[subscription_count].u.u.fd_write.file_descriptor = (__wasi_fd_t)fds[index].fd;
            subscription_count++;
        }
    }
    if (adjusted_timeout >= 0) {
        subscriptions[subscription_count].userdata = UINT64_MAX;
        subscriptions[subscription_count].u.tag = __WASI_EVENTTYPE_CLOCK;
        subscriptions[subscription_count].u.u.clock.id = __WASI_CLOCKID_MONOTONIC;
        subscriptions[subscription_count].u.u.clock.timeout = (uint64_t)adjusted_timeout * 1000000ULL;
        subscriptions[subscription_count].u.u.clock.precision = 1000000ULL;
        subscriptions[subscription_count].u.u.clock.flags = 0;
        subscription_count++;
    }
    if (subscription_count == 0) {
        free(subscriptions);
        free(events);
        return 0;
    }
    __wasi_size_t event_count = 0;
    __wasi_errno_t rc = __wasi_poll_oneoff(subscriptions, events, subscription_count, &event_count);
    if (rc != 0) {
        free(subscriptions);
        free(events);
        errno = (int)rc;
        return -1;
    }
    for (size_t index = 0; index < event_count; index++) {
        if (events[index].userdata == UINT64_MAX) {
            continue;
        }
        size_t fd_index = (size_t)(events[index].userdata >> 8);
        int kind = (int)(events[index].userdata & 0xff);
        if (fd_index >= (size_t)nfds) {
            continue;
        }
        short revents = 0;
        if (events[index].error != 0) {
            revents |= events[index].error == __WASI_ERRNO_BADF ? POLLNVAL : POLLERR;
        } else if (kind == 1) {
            revents |= POLLIN;
        } else if (kind == 2) {
            revents |= POLLOUT;
        }
        if ((events[index].fd_readwrite.flags & __WASI_EVENTRWFLAGS_FD_READWRITE_HANGUP) != 0) {
            revents |= POLLHUP;
        }
        if (revents != 0 && fds[fd_index].revents == 0) {
            ready++;
        }
        fds[fd_index].revents |= revents;
    }
    for (nfds_t index = 0; index < nfds; index++) {
        if (
            fds[index].fd >= 0
            && fds[index].revents == 0
            && (fds[index].events & (POLLIN | POLLPRI)) != 0
            && sinomo_special_fd_readable(fds[index].fd)
        ) {
            fds[index].revents |= POLLIN;
            ready++;
        }
    }
    free(subscriptions);
    free(events);
    return ready;
}

int __wrap_ppoll(struct pollfd *fds, nfds_t nfds, const struct timespec *timeout_ts, const sigset_t *sigmask) {
    (void)sigmask;
    int timeout_ms = -1;
    if (timeout_ts != NULL) {
        if (timeout_ts->tv_sec < 0 || timeout_ts->tv_nsec < 0 || timeout_ts->tv_nsec >= 1000000000L) {
            errno = EINVAL;
            return -1;
        }
        uint64_t ms = (uint64_t)timeout_ts->tv_sec * 1000ULL + (uint64_t)((timeout_ts->tv_nsec + 999999L) / 1000000L);
        timeout_ms = ms > (uint64_t)INT_MAX ? INT_MAX : (int)ms;
    }
    return __wrap_poll(fds, nfds, timeout_ms);
}

static int sinomo_select_timeout_ms(const struct timeval *timeout) {
    if (timeout == NULL) {
        return -1;
    }
    if (timeout->tv_sec < 0 || timeout->tv_usec < 0 || timeout->tv_usec >= 1000000L) {
        errno = EINVAL;
        return -2;
    }
    uint64_t ms = (uint64_t)timeout->tv_sec * 1000ULL + (uint64_t)((timeout->tv_usec + 999L) / 1000L);
    return ms > (uint64_t)INT_MAX ? INT_MAX : (int)ms;
}

static int sinomo_pselect_timeout_ms(const struct timespec *timeout) {
    if (timeout == NULL) {
        return -1;
    }
    if (timeout->tv_sec < 0 || timeout->tv_nsec < 0 || timeout->tv_nsec >= 1000000000L) {
        errno = EINVAL;
        return -2;
    }
    uint64_t ms = (uint64_t)timeout->tv_sec * 1000ULL + (uint64_t)((timeout->tv_nsec + 999999L) / 1000000L);
    return ms > (uint64_t)INT_MAX ? INT_MAX : (int)ms;
}

static int sinomo_select_from_poll(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, int timeout_ms) {
    if (nfds < 0 || nfds > FD_SETSIZE) {
        errno = EINVAL;
        return -1;
    }
    struct pollfd *pollfds = calloc((size_t)nfds, sizeof(struct pollfd));
    if (pollfds == NULL && nfds > 0) {
        errno = ENOMEM;
        return -1;
    }
    nfds_t poll_count = 0;
    for (int fd = 0; fd < nfds; fd++) {
        short events = 0;
        if (readfds != NULL && FD_ISSET(fd, readfds)) {
            events |= POLLIN;
        }
        if (writefds != NULL && FD_ISSET(fd, writefds)) {
            events |= POLLOUT;
        }
        if (exceptfds != NULL && FD_ISSET(fd, exceptfds)) {
            events |= POLLPRI;
        }
        if (events != 0) {
            pollfds[poll_count].fd = fd;
            pollfds[poll_count].events = events;
            poll_count++;
        }
    }
    if (readfds != NULL) {
        FD_ZERO(readfds);
    }
    if (writefds != NULL) {
        FD_ZERO(writefds);
    }
    if (exceptfds != NULL) {
        FD_ZERO(exceptfds);
    }
    int rc = __wrap_poll(pollfds, poll_count, timeout_ms);
    if (rc < 0) {
        free(pollfds);
        return -1;
    }
    int ready = 0;
    for (nfds_t index = 0; index < poll_count; index++) {
        int fd = pollfds[index].fd;
        if (readfds != NULL && (pollfds[index].revents & (POLLIN | POLLHUP | POLLERR)) != 0) {
            FD_SET(fd, readfds);
            ready++;
        }
        if (writefds != NULL && (pollfds[index].revents & (POLLOUT | POLLERR)) != 0) {
            FD_SET(fd, writefds);
            ready++;
        }
        if (exceptfds != NULL && (pollfds[index].revents & POLLPRI) != 0) {
            FD_SET(fd, exceptfds);
            ready++;
        }
    }
    free(pollfds);
    return ready;
}

int __wrap_select(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, struct timeval *timeout) {
    int timeout_ms = sinomo_select_timeout_ms(timeout);
    if (timeout_ms == -2) {
        return -1;
    }
    return sinomo_select_from_poll(nfds, readfds, writefds, exceptfds, timeout_ms);
}

int __wrap_pselect(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timespec *timeout, const sigset_t *sigmask) {
    (void)sigmask;
    int timeout_ms = sinomo_pselect_timeout_ms(timeout);
    if (timeout_ms == -2) {
        return -1;
    }
    return sinomo_select_from_poll(nfds, readfds, writefds, exceptfds, timeout_ms);
}

int __wrap_sched_yield(void) {
    __wasi_errno_t rc = __wasi_sched_yield();
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    return 0;
}

int __wrap_sched_get_priority_max(int policy) {
    switch (policy) {
        case SCHED_OTHER:
        case SCHED_FIFO:
        case SCHED_RR:
#ifdef SCHED_BATCH
        case SCHED_BATCH:
#endif
#ifdef SCHED_IDLE
        case SCHED_IDLE:
#endif
            return 0;
        default:
            errno = EINVAL;
            return -1;
    }
}

int __wrap_sched_get_priority_min(int policy) {
    return __wrap_sched_get_priority_max(policy);
}

int __wrap_sched_getscheduler(pid_t pid) {
    int policy = 0;
    int priority = 0;
    int rc = sinomo_proc_getscheduler((int)pid, &policy, &priority);
    (void)priority;
    if (rc == 0) {
        return policy;
    }
    errno = rc;
    return -1;
}

int __wrap_sched_getparam(pid_t pid, struct sched_param *param) {
    if (param == NULL) {
        errno = EFAULT;
        return -1;
    }
    int policy = 0;
    int priority = 0;
    int rc = sinomo_proc_getscheduler((int)pid, &policy, &priority);
    (void)policy;
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    memset(param, 0, sizeof(*param));
    param->sched_priority = priority;
    return 0;
}

int __wrap_sched_setparam(pid_t pid, const struct sched_param *param) {
    if (param == NULL) {
        errno = EFAULT;
        return -1;
    }
    int policy = __wrap_sched_getscheduler(pid);
    if (policy < 0) {
        return -1;
    }
    int rc = sinomo_proc_setscheduler((int)pid, policy, param->sched_priority);
    if (rc == 0) {
        return 0;
    }
    errno = rc;
    return -1;
}

int __wrap_sched_setscheduler(pid_t pid, int policy, const struct sched_param *param) {
    if (param == NULL) {
        errno = EFAULT;
        return -1;
    }
    int rc = sinomo_proc_setscheduler((int)pid, policy, param->sched_priority);
    if (rc == 0) {
        return 0;
    }
    errno = rc;
    return -1;
}

int __wrap_sched_getaffinity(pid_t pid, size_t cpusetsize, void *mask) {
    if (mask == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (cpusetsize == 0) {
        errno = EINVAL;
        return -1;
    }
    unsigned long long words[2] = {0, 0};
    int rc = sinomo_proc_getaffinity((int)pid, &words[0], &words[1]);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    memset(mask, 0, cpusetsize);
    size_t copy_size = cpusetsize < sizeof(words) ? cpusetsize : sizeof(words);
    memcpy(mask, words, copy_size);
    return 0;
}

int __wrap_sched_setaffinity(pid_t pid, size_t cpusetsize, const void *mask) {
    if (mask == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (cpusetsize == 0) {
        errno = EINVAL;
        return -1;
    }
    unsigned long long words[2] = {0, 0};
    size_t copy_size = cpusetsize < sizeof(words) ? cpusetsize : sizeof(words);
    memcpy(words, mask, copy_size);
    int rc = sinomo_proc_setaffinity((int)pid, words[0], words[1]);
    if (rc == 0) {
        return 0;
    }
    errno = rc;
    return -1;
}

int __wrap_sched_getcpu(void) {
    unsigned int cpu = 0;
    int rc = sinomo_proc_getcpu(&cpu);
    if (rc == 0) {
        return (int)cpu;
    }
    errno = rc;
    return -1;
}

int __wrap_sched_rr_get_interval(pid_t pid, struct timespec *interval) {
    if (interval == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (__wrap_sched_getscheduler(pid) < 0) {
        return -1;
    }
    interval->tv_sec = 0;
    interval->tv_nsec = 10000000L;
    return 0;
}

static struct sigaction sinomo_signal_actions[_NSIG];
static sigset_t sinomo_signal_mask;
static sigset_t sinomo_pending_signals;
static siginfo_t sinomo_pending_signal_info[_NSIG];
static stack_t sinomo_signal_stack = {
    .ss_sp = NULL,
    .ss_flags = SS_DISABLE,
    .ss_size = 0,
};

static int sinomo_signal_valid(int signum) {
    return signum > 0 && signum < _NSIG;
}

static int sinomo_signal_noncatchable(int signum) {
    return signum == SIGKILL || signum == SIGSTOP;
}

static int sinomo_signal_default_ignored(int signum) {
    switch (signum) {
#ifdef SIGCHLD
        case SIGCHLD:
            return 1;
#endif
#ifdef SIGURG
        case SIGURG:
            return 1;
#endif
#ifdef SIGWINCH
        case SIGWINCH:
            return 1;
#endif
        default:
            return 0;
    }
}

static int sinomo_signal_currently_ignored(int signum) {
    if (!sinomo_signal_valid(signum)) {
        return 0;
    }
    struct sigaction *action = &sinomo_signal_actions[signum];
    return action->sa_handler == SIG_IGN
        || ((action->sa_handler == SIG_DFL || action->sa_handler == NULL) && sinomo_signal_default_ignored(signum));
}

static void sinomo_signal_reset_handlers_for_clone(void) {
    for (int signum = 1; signum < _NSIG; signum++) {
        if (sinomo_signal_noncatchable(signum)) {
            continue;
        }
        struct sigaction *action = &sinomo_signal_actions[signum];
        if (action->sa_handler != SIG_IGN) {
            memset(action, 0, sizeof(*action));
            action->sa_handler = SIG_DFL;
        }
    }
}

static void sinomo_sigset_word_bit(int signum, size_t *word, unsigned long *bit) {
    unsigned int index = (unsigned int)signum - 1;
    *word = index / (8U * sizeof(unsigned long));
    *bit = 1UL << (index % (8U * sizeof(unsigned long)));
}

static int sinomo_sigset_contains(const sigset_t *set, int signum) {
    if (set == NULL || !sinomo_signal_valid(signum)) {
        return 0;
    }
    size_t word = 0;
    unsigned long bit = 0;
    sinomo_sigset_word_bit(signum, &word, &bit);
    return (set->__bits[word] & bit) != 0;
}

static void sinomo_sigset_add(sigset_t *set, int signum) {
    if (set == NULL || !sinomo_signal_valid(signum)) {
        return;
    }
    size_t word = 0;
    unsigned long bit = 0;
    sinomo_sigset_word_bit(signum, &word, &bit);
    set->__bits[word] |= bit;
}

static void sinomo_sigset_del(sigset_t *set, int signum) {
    if (set == NULL || !sinomo_signal_valid(signum)) {
        return;
    }
    size_t word = 0;
    unsigned long bit = 0;
    sinomo_sigset_word_bit(signum, &word, &bit);
    set->__bits[word] &= ~bit;
}

static void sinomo_sigset_remove_unblockable(sigset_t *set) {
    if (set == NULL) {
        return;
    }
    sinomo_sigset_del(set, SIGKILL);
    sinomo_sigset_del(set, SIGSTOP);
}

static void sinomo_queue_pending_signal(int signum, const siginfo_t *info) {
    if (!sinomo_signal_valid(signum)) {
        return;
    }
    sinomo_sigset_add(&sinomo_pending_signals, signum);
    memset(&sinomo_pending_signal_info[signum], 0, sizeof(sinomo_pending_signal_info[signum]));
    if (info != NULL) {
        sinomo_pending_signal_info[signum] = *info;
        sinomo_pending_signal_info[signum].si_signo = signum;
        if (sinomo_pending_signal_info[signum].si_pid == 0) {
            sinomo_pending_signal_info[signum].si_pid = __wrap_getpid();
        }
        if (sinomo_pending_signal_info[signum].si_uid == 0) {
            sinomo_pending_signal_info[signum].si_uid = __wrap_getuid();
        }
    } else {
        sinomo_pending_signal_info[signum].si_signo = signum;
        sinomo_pending_signal_info[signum].si_pid = __wrap_getpid();
        sinomo_pending_signal_info[signum].si_uid = __wrap_getuid();
    }
}

int __wrap_sigemptyset(sigset_t *set) {
    if (set == NULL) {
        errno = EFAULT;
        return -1;
    }
    memset(set, 0, sizeof(*set));
    return 0;
}

int __wrap_sigfillset(sigset_t *set) {
    if (set == NULL) {
        errno = EFAULT;
        return -1;
    }
    memset(set, 0xff, sizeof(*set));
    return 0;
}

int __wrap_sigaddset(sigset_t *set, int signum) {
    if (set == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (!sinomo_signal_valid(signum)) {
        errno = EINVAL;
        return -1;
    }
    sinomo_sigset_add(set, signum);
    return 0;
}

int __wrap_sigdelset(sigset_t *set, int signum) {
    if (set == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (!sinomo_signal_valid(signum)) {
        errno = EINVAL;
        return -1;
    }
    sinomo_sigset_del(set, signum);
    return 0;
}

int __wrap_sigismember(const sigset_t *set, int signum) {
    if (set == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (!sinomo_signal_valid(signum)) {
        errno = EINVAL;
        return -1;
    }
    return sinomo_sigset_contains(set, signum);
}

int __wrap_sigprocmask(int how, const sigset_t *set, sigset_t *oldset) {
    if (oldset != NULL) {
        *oldset = sinomo_signal_mask;
    }
    if (set == NULL) {
        return 0;
    }
    switch (how) {
        case SIG_BLOCK:
            for (int signum = 1; signum < _NSIG; signum++) {
                if (sinomo_sigset_contains(set, signum) && !sinomo_signal_noncatchable(signum)) {
                    sinomo_sigset_add(&sinomo_signal_mask, signum);
                }
            }
            return 0;
        case SIG_UNBLOCK:
            for (int signum = 1; signum < _NSIG; signum++) {
                if (sinomo_sigset_contains(set, signum)) {
                    sinomo_sigset_del(&sinomo_signal_mask, signum);
                }
            }
            return 0;
        case SIG_SETMASK:
            sinomo_signal_mask = *set;
            sinomo_sigset_remove_unblockable(&sinomo_signal_mask);
            return 0;
        default:
            errno = EINVAL;
            return -1;
    }
}

int __wrap_pthread_sigmask(int how, const sigset_t *set, sigset_t *oldset) {
    return __wrap_sigprocmask(how, set, oldset);
}

int __wrap_sigpending(sigset_t *set) {
    if (set == NULL) {
        errno = EFAULT;
        return -1;
    }
    *set = sinomo_pending_signals;
    return 0;
}

int __wrap_sigaction(int signum, const struct sigaction *act, struct sigaction *oldact) {
    if (!sinomo_signal_valid(signum)) {
        errno = EINVAL;
        return -1;
    }
    if (act != NULL && sinomo_signal_noncatchable(signum)) {
        errno = EINVAL;
        return -1;
    }
    if (oldact != NULL) {
        *oldact = sinomo_signal_actions[signum];
    }
    if (act != NULL) {
        sinomo_signal_actions[signum] = *act;
    }
    return 0;
}

int __wrap_sigaltstack(const stack_t *stack, stack_t *old_stack) {
    if (old_stack != NULL) {
        *old_stack = sinomo_signal_stack;
    }
    if (stack == NULL) {
        return 0;
    }
    int allowed_flags = SS_DISABLE;
#ifdef SS_AUTODISARM
    allowed_flags |= SS_AUTODISARM;
#endif
    if ((stack->ss_flags & ~allowed_flags) != 0) {
        errno = EINVAL;
        return -1;
    }
    if ((stack->ss_flags & SS_DISABLE) == 0 && stack->ss_size < MINSIGSTKSZ) {
        errno = ENOMEM;
        return -1;
    }
    sinomo_signal_stack = *stack;
    if ((sinomo_signal_stack.ss_flags & SS_DISABLE) != 0) {
        sinomo_signal_stack.ss_sp = NULL;
        sinomo_signal_stack.ss_size = 0;
    }
    return 0;
}

void (*__wrap_signal(int signum, void (*handler)(int)))(int) {
    struct sigaction action;
    struct sigaction old_action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = handler;
    if (__wrap_sigaction(signum, &action, &old_action) != 0) {
        return SIG_ERR;
    }
    return old_action.sa_handler;
}

static int sinomo_deliver_signal_now(int signum, const siginfo_t *queued_info) {
    struct sigaction *action = &sinomo_signal_actions[signum];
    if (action->sa_handler == SIG_IGN) {
        return 0;
    }
    if ((action->sa_handler == SIG_DFL || action->sa_handler == NULL) && sinomo_signal_default_ignored(signum)) {
        return 0;
    }
    if (action->sa_handler != SIG_DFL && action->sa_handler != NULL) {
        if (sinomo_signal_noncatchable(signum)) {
            __wasi_errno_t rc = __wasi_proc_raise((__wasi_signal_t)signum);
            if (rc != 0) {
                errno = (int)rc;
                return -1;
            }
            return 0;
        }
        struct sigaction delivered_action = *action;
        sigset_t old_mask = sinomo_signal_mask;
        for (int masked = 1; masked < _NSIG; masked++) {
            if (sinomo_sigset_contains(&delivered_action.sa_mask, masked) && !sinomo_signal_noncatchable(masked)) {
                sinomo_sigset_add(&sinomo_signal_mask, masked);
            }
        }
        if ((delivered_action.sa_flags & SA_NODEFER) == 0) {
            sinomo_sigset_add(&sinomo_signal_mask, signum);
        }
        if ((delivered_action.sa_flags & SA_RESETHAND) != 0) {
            memset(action, 0, sizeof(*action));
            action->sa_handler = SIG_DFL;
        }
        if ((delivered_action.sa_flags & SA_SIGINFO) != 0 && delivered_action.sa_sigaction != NULL) {
            siginfo_t info;
            if (queued_info != NULL) {
                info = *queued_info;
                info.si_signo = signum;
                if (info.si_pid == 0) {
                    info.si_pid = __wrap_getpid();
                }
                if (info.si_uid == 0) {
                    info.si_uid = __wrap_getuid();
                }
            } else {
                memset(&info, 0, sizeof(info));
                info.si_signo = signum;
                info.si_pid = __wrap_getpid();
                info.si_uid = __wrap_getuid();
            }
            delivered_action.sa_sigaction(signum, &info, NULL);
        } else {
            delivered_action.sa_handler(signum);
        }
        sinomo_signal_mask = old_mask;
        return 0;
    }
    __wasi_errno_t rc = __wasi_proc_raise((__wasi_signal_t)signum);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    return 0;
}

int __wrap_raise(int signum) {
    if (!sinomo_signal_valid(signum)) {
        errno = EINVAL;
        return -1;
    }
    if (sinomo_sigset_contains(&sinomo_signal_mask, signum)) {
        siginfo_t info;
        memset(&info, 0, sizeof(info));
        info.si_signo = signum;
        info.si_pid = __wrap_getpid();
        info.si_uid = __wrap_getuid();
        sinomo_queue_pending_signal(signum, &info);
        return 0;
    }
    return sinomo_deliver_signal_now(signum, NULL);
}

static int sinomo_take_pending_signal(const sigset_t *set, siginfo_t *info) {
    if (set == NULL) {
        return 0;
    }
    for (int signum = 1; signum < _NSIG; signum++) {
        if (
            sinomo_sigset_contains(set, signum)
            && sinomo_sigset_contains(&sinomo_pending_signals, signum)
        ) {
            sinomo_sigset_del(&sinomo_pending_signals, signum);
            if (info != NULL) {
                *info = sinomo_pending_signal_info[signum];
                if (info->si_signo == 0) {
                    memset(info, 0, sizeof(*info));
                    info->si_signo = signum;
                    info->si_pid = __wrap_getpid();
                    info->si_uid = __wrap_getuid();
                }
            }
            memset(&sinomo_pending_signal_info[signum], 0, sizeof(sinomo_pending_signal_info[signum]));
            return signum;
        }
    }
    return 0;
}

static void sinomo_fill_siginfo(siginfo_t *info, int signum) {
    if (info == NULL) {
        return;
    }
    memset(info, 0, sizeof(*info));
    info->si_signo = signum;
    info->si_pid = __wrap_getpid();
    info->si_uid = __wrap_getuid();
}

int __wrap_sigtimedwait(const sigset_t *set, siginfo_t *info, const struct timespec *timeout) {
    if (set == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (timeout != NULL && (timeout->tv_sec < 0 || timeout->tv_nsec < 0 || timeout->tv_nsec >= 1000000000L)) {
        errno = EINVAL;
        return -1;
    }
    int pending = sinomo_take_pending_signal(set, info);
    if (pending != 0) {
        return pending;
    }
    if (timeout != NULL) {
        if (timeout->tv_sec != 0 || timeout->tv_nsec != 0) {
            nanosleep(timeout, NULL);
        }
        pending = sinomo_take_pending_signal(set, info);
        if (pending != 0) {
            return pending;
        }
        errno = EAGAIN;
        return -1;
    }
    for (;;) {
        struct timespec delay = {0, 10000000L};
        nanosleep(&delay, NULL);
        pending = sinomo_take_pending_signal(set, info);
        if (pending != 0) {
            return pending;
        }
    }
}

int __wrap_sigwaitinfo(const sigset_t *set, siginfo_t *info) {
    return __wrap_sigtimedwait(set, info, NULL);
}

int __wrap_sigwait(const sigset_t *set, int *signum) {
    if (signum == NULL) {
        errno = EFAULT;
        return EFAULT;
    }
    siginfo_t info;
    int rc = __wrap_sigwaitinfo(set, &info);
    if (rc < 0) {
        return errno;
    }
    *signum = rc;
    return 0;
}

int __wrap_sigsuspend(const sigset_t *mask) {
    if (mask == NULL) {
        errno = EFAULT;
        return -1;
    }
    sigset_t old_mask = sinomo_signal_mask;
    sinomo_signal_mask = *mask;
    sinomo_sigset_remove_unblockable(&sinomo_signal_mask);
    for (;;) {
        for (int signum = 1; signum < _NSIG; signum++) {
            if (
                sinomo_sigset_contains(&sinomo_pending_signals, signum)
                && !sinomo_sigset_contains(&sinomo_signal_mask, signum)
            ) {
                sigset_t selected;
                siginfo_t info;
                __wrap_sigemptyset(&selected);
                sinomo_sigset_add(&selected, signum);
                (void)sinomo_take_pending_signal(&selected, &info);
                if (sinomo_signal_currently_ignored(signum)) {
                    continue;
                }
                int deliver_rc = sinomo_deliver_signal_now(signum, &info);
                sinomo_signal_mask = old_mask;
                if (deliver_rc != 0) {
                    return -1;
                }
                errno = EINTR;
                return -1;
            }
        }
        struct timespec delay = {0, 10000000L};
        nanosleep(&delay, NULL);
    }
}

int __wrap_pause(void) {
    sigset_t empty;
    __wrap_sigemptyset(&empty);
    return __wrap_sigsuspend(&empty);
}

int __wrap_sighold(int signum) {
    sigset_t set;
    if (__wrap_sigemptyset(&set) != 0 || __wrap_sigaddset(&set, signum) != 0) {
        return -1;
    }
    return __wrap_sigprocmask(SIG_BLOCK, &set, NULL);
}

int __wrap_sigrelse(int signum) {
    sigset_t set;
    if (__wrap_sigemptyset(&set) != 0 || __wrap_sigaddset(&set, signum) != 0) {
        return -1;
    }
    return __wrap_sigprocmask(SIG_UNBLOCK, &set, NULL);
}

int __wrap_sigignore(int signum) {
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = SIG_IGN;
    return __wrap_sigaction(signum, &action, NULL);
}

int __wrap_siginterrupt(int signum, int flag) {
    struct sigaction action;
    if (__wrap_sigaction(signum, NULL, &action) != 0) {
        return -1;
    }
    if (flag) {
        action.sa_flags &= ~SA_RESTART;
    } else {
        action.sa_flags |= SA_RESTART;
    }
    return __wrap_sigaction(signum, &action, NULL);
}

int __wrap_sigpause(int signum) {
    sigset_t mask = sinomo_signal_mask;
    if (!sinomo_signal_valid(signum)) {
        errno = EINVAL;
        return -1;
    }
    sinomo_sigset_del(&mask, signum);
    return __wrap_sigsuspend(&mask);
}

int __wrap_sigqueue(pid_t pid, int signum, union sigval value) {
    if (!sinomo_signal_valid(signum)) {
        errno = EINVAL;
        return -1;
    }
    if (pid == __wrap_getpid() && sinomo_sigset_contains(&sinomo_signal_mask, signum)) {
        siginfo_t info;
        memset(&info, 0, sizeof(info));
        info.si_signo = signum;
        info.si_pid = __wrap_getpid();
        info.si_uid = __wrap_getuid();
        info.si_value = value;
        sinomo_queue_pending_signal(signum, &info);
        return 0;
    }
    return __wrap_kill(pid, signum);
}

int __wrap_pthread_kill(pthread_t thread, int signum) {
    if (signum == 0) {
        return 0;
    }
    if (!sinomo_signal_valid(signum)) {
        return EINVAL;
    }
    if (thread != pthread_self()) {
        return ESRCH;
    }
    return __wrap_raise(signum) == 0 ? 0 : errno;
}

#define SINOMO_SIGNALFD_SLOTS 64
#define SINOMO_SIGNALFD_RECORD_SIZE 128

struct sinomo_signalfd {
    int active;
    int fd;
    sigset_t mask;
};

struct sinomo_signalfd_siginfo {
    uint32_t ssi_signo;
    int32_t ssi_errno;
    int32_t ssi_code;
    uint32_t ssi_pid;
    uint32_t ssi_uid;
    int32_t ssi_fd;
    uint32_t ssi_tid;
    uint32_t ssi_band;
    uint32_t ssi_overrun;
    uint32_t ssi_trapno;
    int32_t ssi_status;
    int32_t ssi_int;
    uint64_t ssi_ptr;
    uint64_t ssi_utime;
    uint64_t ssi_stime;
    uint64_t ssi_addr;
    uint16_t ssi_addr_lsb;
    uint8_t pad[SINOMO_SIGNALFD_RECORD_SIZE - 82];
};

static struct sinomo_signalfd sinomo_signalfds[SINOMO_SIGNALFD_SLOTS];

static int sinomo_signalfd_index(int fd) {
    for (int index = 0; index < SINOMO_SIGNALFD_SLOTS; index++) {
        if (sinomo_signalfds[index].active && sinomo_signalfds[index].fd == fd) {
            return index;
        }
    }
    return -1;
}

static void sinomo_signalfd_forget(int fd) {
    int index = sinomo_signalfd_index(fd);
    if (index >= 0) {
        memset(&sinomo_signalfds[index], 0, sizeof(sinomo_signalfds[index]));
    }
}

int __wrap_signalfd(int fd, const sigset_t *mask, int flags) {
    int supported = SFD_CLOEXEC | SFD_NONBLOCK;
    if (mask == NULL) {
        errno = EFAULT;
        return -1;
    }
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    int index = -1;
    if (fd >= 0) {
        index = sinomo_signalfd_index(fd);
        if (index < 0) {
            errno = EINVAL;
            return -1;
        }
        sinomo_signalfds[index].mask = *mask;
        return fd;
    }
    for (int candidate = 0; candidate < SINOMO_SIGNALFD_SLOTS; candidate++) {
        if (!sinomo_signalfds[candidate].active) {
            index = candidate;
            break;
        }
    }
    if (index < 0) {
        errno = EMFILE;
        return -1;
    }
    int event_flags = 0;
#ifdef EFD_CLOEXEC
    if ((flags & SFD_CLOEXEC) != 0) {
        event_flags |= EFD_CLOEXEC;
    }
#endif
#ifdef EFD_NONBLOCK
    if ((flags & SFD_NONBLOCK) != 0) {
        event_flags |= EFD_NONBLOCK;
    }
#endif
    int new_fd = __wrap_eventfd(0, event_flags);
    if (new_fd < 0) {
        return -1;
    }
    memset(&sinomo_signalfds[index], 0, sizeof(sinomo_signalfds[index]));
    sinomo_signalfds[index].active = 1;
    sinomo_signalfds[index].fd = new_fd;
    sinomo_signalfds[index].mask = *mask;
    return new_fd;
}

static ssize_t sinomo_signalfd_read(int fd, void *buf, size_t count) {
    int index = sinomo_signalfd_index(fd);
    if (index < 0) {
        return -2;
    }
    if (buf == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (count < SINOMO_SIGNALFD_RECORD_SIZE) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_signalfd *descriptor = &sinomo_signalfds[index];
    for (;;) {
        siginfo_t pending_info;
        int signum = sinomo_take_pending_signal(&descriptor->mask, &pending_info);
        if (signum != 0) {
            struct sinomo_signalfd_siginfo info;
            memset(&info, 0, sizeof(info));
            info.ssi_signo = (uint32_t)signum;
            info.ssi_errno = pending_info.si_errno;
            info.ssi_code = pending_info.si_code;
            info.ssi_pid = (uint32_t)pending_info.si_pid;
            info.ssi_uid = (uint32_t)pending_info.si_uid;
            info.ssi_int = pending_info.si_value.sival_int;
            info.ssi_ptr = (uint64_t)(uintptr_t)pending_info.si_value.sival_ptr;
            memcpy(buf, &info, sizeof(info));
            return (ssize_t)sizeof(info);
        }
        int fd_flags = __real_fcntl(fd, F_GETFL, 0);
        if (fd_flags >= 0 && (fd_flags & O_NONBLOCK) != 0) {
            errno = EAGAIN;
            return -1;
        }
        struct timespec delay = {0, 10000000L};
        if (nanosleep(&delay, NULL) != 0 && errno != EINTR) {
            return -1;
        }
    }
}

static struct itimerval sinomo_itimers[3];

static int sinomo_itimer_signal(int which) {
    switch (which) {
        case ITIMER_REAL:
            return SIGALRM;
        case ITIMER_VIRTUAL:
            return SIGVTALRM;
        case ITIMER_PROF:
            return SIGPROF;
        default:
            return 0;
    }
}

static int sinomo_timeval_valid(const struct timeval *value) {
    return value != NULL
        && value->tv_sec >= 0
        && value->tv_usec >= 0
        && value->tv_usec < 1000000L;
}

static __wasi_timestamp_t sinomo_timeval_ns(const struct timeval *value) {
    return (__wasi_timestamp_t)value->tv_sec * 1000000000ULL
        + (__wasi_timestamp_t)value->tv_usec * 1000ULL;
}

int __wrap_getitimer(int which, struct itimerval *curr_value) {
    if (curr_value == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (which < 0 || which >= 3 || sinomo_itimer_signal(which) == 0) {
        errno = EINVAL;
        return -1;
    }
    *curr_value = sinomo_itimers[which];
    return 0;
}

int __wrap_setitimer(int which, const struct itimerval *new_value, struct itimerval *old_value) {
    if (new_value == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (which < 0 || which >= 3 || sinomo_itimer_signal(which) == 0) {
        errno = EINVAL;
        return -1;
    }
    if (!sinomo_timeval_valid(&new_value->it_value) || !sinomo_timeval_valid(&new_value->it_interval)) {
        errno = EINVAL;
        return -1;
    }
    if (old_value != NULL) {
        *old_value = sinomo_itimers[which];
    }
    sinomo_itimers[which] = *new_value;

    __wasi_timestamp_t first_ns = sinomo_timeval_ns(&new_value->it_value);
    __wasi_timestamp_t interval_ns = sinomo_timeval_ns(&new_value->it_interval);
    __wasi_timestamp_t scheduled_ns = first_ns != 0 ? first_ns : interval_ns;
    __wasi_errno_t rc = __wasi_proc_raise_interval(
        (__wasi_signal_t)sinomo_itimer_signal(which),
        scheduled_ns,
        interval_ns != 0
    );
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    return 0;
}

unsigned __wrap_alarm(unsigned seconds) {
    struct itimerval old_value;
    struct itimerval new_value;
    memset(&new_value, 0, sizeof(new_value));
    new_value.it_value.tv_sec = (time_t)seconds;
    if (__wrap_setitimer(ITIMER_REAL, &new_value, &old_value) != 0) {
        return 0;
    }
    return old_value.it_value.tv_sec > (time_t)UINT_MAX
        ? UINT_MAX
        : (unsigned)old_value.it_value.tv_sec;
}

unsigned __wrap_ualarm(unsigned useconds, unsigned interval) {
    struct itimerval old_value;
    struct itimerval new_value;
    memset(&new_value, 0, sizeof(new_value));
    new_value.it_value.tv_sec = useconds / 1000000U;
    new_value.it_value.tv_usec = useconds % 1000000U;
    new_value.it_interval.tv_sec = interval / 1000000U;
    new_value.it_interval.tv_usec = interval % 1000000U;
    if (__wrap_setitimer(ITIMER_REAL, &new_value, &old_value) != 0) {
        return 0;
    }
    uint64_t old_us = (uint64_t)old_value.it_value.tv_sec * 1000000ULL + (uint64_t)old_value.it_value.tv_usec;
    return old_us > UINT_MAX ? UINT_MAX : (unsigned)old_us;
}

int __wrap_clock_nanosleep(clockid_t clock_id, int flags, const struct timespec *request, struct timespec *remain) {
    if (request == NULL) {
        return EFAULT;
    }
    if (request->tv_sec < 0 || request->tv_nsec < 0 || request->tv_nsec >= 1000000000L) {
        return EINVAL;
    }
    if (flags != 0 && flags != TIMER_ABSTIME) {
        return EINVAL;
    }
    if (flags == TIMER_ABSTIME) {
        struct timespec now;
        if (clock_gettime(clock_id, &now) != 0) {
            return errno;
        }
        time_t sec = request->tv_sec - now.tv_sec;
        long nsec = request->tv_nsec - now.tv_nsec;
        if (nsec < 0) {
            sec--;
            nsec += 1000000000L;
        }
        if (sec < 0) {
            return 0;
        }
        struct timespec relative = {
            .tv_sec = sec,
            .tv_nsec = nsec,
        };
        return nanosleep(&relative, remain) == 0 ? 0 : errno;
    }
    return nanosleep(request, remain) == 0 ? 0 : errno;
}

#ifndef TIMER_ABSTIME
#define TIMER_ABSTIME 1
#endif

struct sinomo_posix_timer {
    int active;
    clockid_t clock_id;
    struct itimerspec spec;
    struct timespec armed_at;
};

static struct sinomo_posix_timer sinomo_posix_timers[64];

static int sinomo_timespec_valid(const struct timespec *value) {
    return value != NULL && value->tv_sec >= 0 && value->tv_nsec >= 0 && value->tv_nsec < 1000000000L;
}

static int sinomo_itimerspec_valid(const struct itimerspec *value) {
    return value != NULL && sinomo_timespec_valid(&value->it_value) && sinomo_timespec_valid(&value->it_interval);
}

static long long sinomo_timespec_to_ns(struct timespec value) {
    if (value.tv_sec > LLONG_MAX / 1000000000LL) {
        return LLONG_MAX;
    }
    long long sec = (long long)value.tv_sec;
    long long nsec = (long long)value.tv_nsec;
    if (sec > (LLONG_MAX - nsec) / 1000000000LL) {
        return LLONG_MAX;
    }
    return sec * 1000000000LL + nsec;
}

static struct timespec sinomo_ns_to_timespec(long long ns) {
    struct timespec value;
    if (ns <= 0) {
        value.tv_sec = 0;
        value.tv_nsec = 0;
        return value;
    }
    value.tv_sec = (time_t)(ns / 1000000000LL);
    value.tv_nsec = (long)(ns % 1000000000LL);
    return value;
}

static int sinomo_timer_index(timer_t timer_id) {
    uintptr_t raw = (uintptr_t)timer_id;
    if (raw == 0 || raw > sizeof(sinomo_posix_timers) / sizeof(sinomo_posix_timers[0])) {
        errno = EINVAL;
        return -1;
    }
    int index = (int)(raw - 1);
    if (!sinomo_posix_timers[index].active) {
        errno = EINVAL;
        return -1;
    }
    return index;
}

int __wrap_timer_create(clockid_t clock_id, struct sigevent *event, timer_t *timer_id) {
    (void)event;
    if (timer_id == NULL) {
        errno = EFAULT;
        return -1;
    }
    struct timespec now;
    if (clock_gettime(clock_id, &now) != 0) {
        return -1;
    }
    for (size_t index = 0; index < sizeof(sinomo_posix_timers) / sizeof(sinomo_posix_timers[0]); index++) {
        if (!sinomo_posix_timers[index].active) {
            memset(&sinomo_posix_timers[index], 0, sizeof(sinomo_posix_timers[index]));
            sinomo_posix_timers[index].active = 1;
            sinomo_posix_timers[index].clock_id = clock_id;
            sinomo_posix_timers[index].armed_at = now;
            *timer_id = (timer_t)(uintptr_t)(index + 1);
            return 0;
        }
    }
    errno = EAGAIN;
    return -1;
}

int __wrap_timer_gettime(timer_t timer_id, struct itimerspec *value) {
    if (value == NULL) {
        errno = EFAULT;
        return -1;
    }
    int index = sinomo_timer_index(timer_id);
    if (index < 0) {
        return -1;
    }
    struct sinomo_posix_timer *timer = &sinomo_posix_timers[index];
    *value = timer->spec;
    long long initial_ns = sinomo_timespec_to_ns(timer->spec.it_value);
    if (initial_ns == 0) {
        return 0;
    }
    struct timespec now;
    if (clock_gettime(timer->clock_id, &now) != 0) {
        return -1;
    }
    long long elapsed_ns = sinomo_timespec_to_ns(now) - sinomo_timespec_to_ns(timer->armed_at);
    if (elapsed_ns < 0) {
        elapsed_ns = 0;
    }
    long long interval_ns = sinomo_timespec_to_ns(timer->spec.it_interval);
    if (elapsed_ns >= initial_ns) {
        if (interval_ns == 0) {
            value->it_value = sinomo_ns_to_timespec(0);
        } else {
            long long into_period = (elapsed_ns - initial_ns) % interval_ns;
            value->it_value = sinomo_ns_to_timespec(interval_ns - into_period);
        }
    } else {
        value->it_value = sinomo_ns_to_timespec(initial_ns - elapsed_ns);
    }
    return 0;
}

int __wrap_timer_settime(timer_t timer_id, int flags, const struct itimerspec *new_value, struct itimerspec *old_value) {
    if ((flags & ~TIMER_ABSTIME) != 0 || !sinomo_itimerspec_valid(new_value)) {
        errno = EINVAL;
        return -1;
    }
    int index = sinomo_timer_index(timer_id);
    if (index < 0) {
        return -1;
    }
    if (old_value != NULL && __wrap_timer_gettime(timer_id, old_value) != 0) {
        return -1;
    }
    struct sinomo_posix_timer *timer = &sinomo_posix_timers[index];
    struct timespec now;
    if (clock_gettime(timer->clock_id, &now) != 0) {
        return -1;
    }
    timer->spec = *new_value;
    if ((flags & TIMER_ABSTIME) != 0) {
        long long target_ns = sinomo_timespec_to_ns(new_value->it_value);
        long long now_ns = sinomo_timespec_to_ns(now);
        timer->spec.it_value = sinomo_ns_to_timespec(target_ns > now_ns ? target_ns - now_ns : 0);
    }
    timer->armed_at = now;
    return 0;
}

int __wrap_timer_getoverrun(timer_t timer_id) {
    int index = sinomo_timer_index(timer_id);
    if (index < 0) {
        return -1;
    }
    struct sinomo_posix_timer *timer = &sinomo_posix_timers[index];
    long long initial_ns = sinomo_timespec_to_ns(timer->spec.it_value);
    long long interval_ns = sinomo_timespec_to_ns(timer->spec.it_interval);
    if (initial_ns == 0 || interval_ns == 0) {
        return 0;
    }
    struct timespec now;
    if (clock_gettime(timer->clock_id, &now) != 0) {
        return -1;
    }
    long long elapsed_ns = sinomo_timespec_to_ns(now) - sinomo_timespec_to_ns(timer->armed_at);
    if (elapsed_ns <= initial_ns) {
        return 0;
    }
    long long overrun = (elapsed_ns - initial_ns) / interval_ns;
    return overrun > INT_MAX ? INT_MAX : (int)overrun;
}

int __wrap_timer_delete(timer_t timer_id) {
    int index = sinomo_timer_index(timer_id);
    if (index < 0) {
        return -1;
    }
    memset(&sinomo_posix_timers[index], 0, sizeof(sinomo_posix_timers[index]));
    return 0;
}

#define SINOMO_TIMERFD_SLOTS 64
struct sinomo_timerfd {
    int active;
    int fd;
    clockid_t clock_id;
    struct itimerspec spec;
    struct timespec armed_at;
    unsigned long long consumed;
};

static struct sinomo_timerfd sinomo_timerfds[SINOMO_TIMERFD_SLOTS];

static int sinomo_timerfd_index(int fd) {
    for (int index = 0; index < SINOMO_TIMERFD_SLOTS; index++) {
        if (sinomo_timerfds[index].active && sinomo_timerfds[index].fd == fd) {
            return index;
        }
    }
    return -1;
}

static void sinomo_timerfd_forget(int fd) {
    int index = sinomo_timerfd_index(fd);
    if (index >= 0) {
        memset(&sinomo_timerfds[index], 0, sizeof(sinomo_timerfds[index]));
    }
}

static unsigned long long sinomo_timerfd_expirations(const struct sinomo_timerfd *timer, struct timespec now) {
    long long initial_ns = sinomo_timespec_to_ns(timer->spec.it_value);
    if (initial_ns == 0) {
        return 0;
    }
    long long elapsed_ns = sinomo_timespec_to_ns(now) - sinomo_timespec_to_ns(timer->armed_at);
    if (elapsed_ns < initial_ns) {
        return 0;
    }
    long long interval_ns = sinomo_timespec_to_ns(timer->spec.it_interval);
    if (interval_ns == 0) {
        return 1;
    }
    unsigned long long extra = (unsigned long long)((elapsed_ns - initial_ns) / interval_ns);
    return extra == ULLONG_MAX ? ULLONG_MAX : extra + 1ULL;
}

static struct timespec sinomo_timerfd_remaining(const struct sinomo_timerfd *timer, struct timespec now) {
    long long initial_ns = sinomo_timespec_to_ns(timer->spec.it_value);
    if (initial_ns == 0) {
        return sinomo_ns_to_timespec(0);
    }
    long long elapsed_ns = sinomo_timespec_to_ns(now) - sinomo_timespec_to_ns(timer->armed_at);
    if (elapsed_ns < 0) {
        elapsed_ns = 0;
    }
    long long interval_ns = sinomo_timespec_to_ns(timer->spec.it_interval);
    if (elapsed_ns < initial_ns) {
        return sinomo_ns_to_timespec(initial_ns - elapsed_ns);
    }
    if (interval_ns == 0) {
        return sinomo_ns_to_timespec(0);
    }
    long long into_period = (elapsed_ns - initial_ns) % interval_ns;
    return sinomo_ns_to_timespec(interval_ns - into_period);
}

int __wrap_timerfd_create(clockid_t clock_id, int flags) {
    int supported = TFD_CLOEXEC | TFD_NONBLOCK;
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    struct timespec now;
    if (clock_gettime(clock_id, &now) != 0) {
        return -1;
    }
    int slot = -1;
    for (int index = 0; index < SINOMO_TIMERFD_SLOTS; index++) {
        if (!sinomo_timerfds[index].active) {
            slot = index;
            break;
        }
    }
    if (slot < 0) {
        errno = EMFILE;
        return -1;
    }
    int event_flags = 0;
#ifdef EFD_CLOEXEC
    if ((flags & TFD_CLOEXEC) != 0) {
        event_flags |= EFD_CLOEXEC;
    }
#endif
#ifdef EFD_NONBLOCK
    if ((flags & TFD_NONBLOCK) != 0) {
        event_flags |= EFD_NONBLOCK;
    }
#endif
    int fd = __wrap_eventfd(0, event_flags);
    if (fd < 0) {
        return -1;
    }
    memset(&sinomo_timerfds[slot], 0, sizeof(sinomo_timerfds[slot]));
    sinomo_timerfds[slot].active = 1;
    sinomo_timerfds[slot].fd = fd;
    sinomo_timerfds[slot].clock_id = clock_id;
    sinomo_timerfds[slot].armed_at = now;
    return fd;
}

int __wrap_timerfd_settime(int fd, int flags, const struct itimerspec *new_value, struct itimerspec *old_value) {
    if ((flags & ~(TFD_TIMER_ABSTIME | TFD_TIMER_CANCEL_ON_SET)) != 0 || !sinomo_itimerspec_valid(new_value)) {
        errno = EINVAL;
        return -1;
    }
    int index = sinomo_timerfd_index(fd);
    if (index < 0) {
        errno = EBADF;
        return -1;
    }
    struct sinomo_timerfd *timer = &sinomo_timerfds[index];
    if (old_value != NULL && __wrap_timerfd_gettime(fd, old_value) != 0) {
        return -1;
    }
    struct timespec now;
    if (clock_gettime(timer->clock_id, &now) != 0) {
        return -1;
    }
    timer->spec = *new_value;
    if ((flags & TFD_TIMER_ABSTIME) != 0) {
        long long target_ns = sinomo_timespec_to_ns(new_value->it_value);
        long long now_ns = sinomo_timespec_to_ns(now);
        timer->spec.it_value = sinomo_ns_to_timespec(target_ns > now_ns ? target_ns - now_ns : 0);
    }
    timer->armed_at = now;
    timer->consumed = 0;
    return 0;
}

int __wrap_timerfd_gettime(int fd, struct itimerspec *value) {
    if (value == NULL) {
        errno = EFAULT;
        return -1;
    }
    int index = sinomo_timerfd_index(fd);
    if (index < 0) {
        errno = EBADF;
        return -1;
    }
    struct sinomo_timerfd *timer = &sinomo_timerfds[index];
    *value = timer->spec;
    struct timespec now;
    if (clock_gettime(timer->clock_id, &now) != 0) {
        return -1;
    }
    value->it_value = sinomo_timerfd_remaining(timer, now);
    return 0;
}

static int sinomo_special_fd_known(int fd) {
    return sinomo_signalfd_index(fd) >= 0 || sinomo_inotify_index(fd) >= 0 || sinomo_fanotify_index(fd) >= 0 || sinomo_timerfd_index(fd) >= 0 || sinomo_pidfd_index(fd) >= 0;
}

static int sinomo_special_fd_readable(int fd) {
    if (sinomo_pidfd_index(fd) >= 0) {
        return sinomo_pidfd_readable(fd);
    }
    int signal_index = sinomo_signalfd_index(fd);
    if (signal_index >= 0) {
        struct sinomo_signalfd *descriptor = &sinomo_signalfds[signal_index];
        for (int signum = 1; signum < _NSIG; signum++) {
            if (
                sinomo_sigset_contains(&descriptor->mask, signum)
                && sinomo_sigset_contains(&sinomo_pending_signals, signum)
            ) {
                return 1;
            }
        }
        return 0;
    }
    int inotify_index = sinomo_inotify_index(fd);
    if (inotify_index >= 0) {
        return sinomo_inotify_instances[inotify_index].event_count > 0;
    }
    int fanotify_index = sinomo_fanotify_index(fd);
    if (fanotify_index >= 0) {
        return sinomo_fanotify_instances[fanotify_index].event_count > 0;
    }
    int timer_index = sinomo_timerfd_index(fd);
    if (timer_index >= 0) {
        struct timespec now;
        if (clock_gettime(sinomo_timerfds[timer_index].clock_id, &now) != 0) {
            return 0;
        }
        return sinomo_timerfd_expirations(&sinomo_timerfds[timer_index], now) > sinomo_timerfds[timer_index].consumed;
    }
    return 0;
}

static int sinomo_special_fd_next_timeout_ms(int fd) {
    int timer_index = sinomo_timerfd_index(fd);
    if (timer_index < 0) {
        return -1;
    }
    struct sinomo_timerfd *timer = &sinomo_timerfds[timer_index];
    struct timespec now;
    if (clock_gettime(timer->clock_id, &now) != 0) {
        return -1;
    }
    struct timespec remaining = sinomo_timerfd_remaining(timer, now);
    if (remaining.tv_sec == 0 && remaining.tv_nsec == 0) {
        return 0;
    }
    unsigned long long ms = (unsigned long long)remaining.tv_sec * 1000ULL;
    ms += ((unsigned long long)remaining.tv_nsec + 999999ULL) / 1000000ULL;
    return ms > (unsigned long long)INT_MAX ? INT_MAX : (int)ms;
}

ssize_t __wrap_read(int fd, void *buf, size_t count) {
    ssize_t signal_read = sinomo_signalfd_read(fd, buf, count);
    if (signal_read != -2) {
        return signal_read;
    }
    ssize_t inotify_read = sinomo_inotify_read(fd, buf, count);
    if (inotify_read != -2) {
        return inotify_read;
    }
    ssize_t fanotify_read = sinomo_fanotify_read(fd, buf, count);
    if (fanotify_read != -2) {
        return fanotify_read;
    }
    int index = sinomo_timerfd_index(fd);
    if (index < 0) {
        return __real_read(fd, buf, count);
    }
    if (buf == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (count < sizeof(uint64_t)) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_timerfd *timer = &sinomo_timerfds[index];
    for (;;) {
        struct timespec now;
        if (clock_gettime(timer->clock_id, &now) != 0) {
            return -1;
        }
        unsigned long long total = sinomo_timerfd_expirations(timer, now);
        if (total > timer->consumed) {
            uint64_t ready = (uint64_t)(total - timer->consumed);
            timer->consumed = total;
            memcpy(buf, &ready, sizeof(ready));
            return (ssize_t)sizeof(ready);
        }
        int fd_flags = __real_fcntl(fd, F_GETFL, 0);
        if (fd_flags >= 0 && (fd_flags & O_NONBLOCK) != 0) {
            errno = EAGAIN;
            return -1;
        }
        struct timespec delay = sinomo_timerfd_remaining(timer, now);
        if (delay.tv_sec == 0 && delay.tv_nsec == 0) {
            delay.tv_nsec = 1000000L;
        }
        if (nanosleep(&delay, NULL) != 0 && errno != EINTR) {
            return -1;
        }
    }
}

ssize_t __wrap_write(int fd, const void *buf, size_t count) {
    if (buf == NULL && count > 0) {
        errno = EFAULT;
        return -1;
    }
    if (sinomo_memfd_check_write(fd, -1, count) != 0) {
        return -1;
    }
    return __real_write(fd, buf, count);
}

int __wrap_close(int fd) {
    sinomo_epoll_forget(fd);
    sinomo_signalfd_forget(fd);
    sinomo_inotify_forget(fd);
    sinomo_fanotify_forget(fd);
    sinomo_timerfd_forget(fd);
    sinomo_pidfd_forget(fd);
    sinomo_socket_forget(fd);
    sinomo_memfd_forget(fd);
    sinomo_pipe_forget(fd);
    sinomo_landlock_forget(fd);
    sinomo_pty_forget_fd(fd);
    return __real_close(fd);
}

unsigned __wrap_sleep(unsigned seconds) {
    struct timespec request = {
        .tv_sec = (time_t)seconds,
        .tv_nsec = 0,
    };
    struct timespec remain;
    if (nanosleep(&request, &remain) == 0) {
        return 0;
    }
    return remain.tv_sec > (time_t)UINT_MAX ? UINT_MAX : (unsigned)remain.tv_sec;
}

int __wrap_usleep(unsigned useconds) {
    struct timespec request = {
        .tv_sec = (time_t)(useconds / 1000000U),
        .tv_nsec = (long)(useconds % 1000000U) * 1000L,
    };
    return nanosleep(&request, NULL);
}

static void sinomo_timeval_pair_to_timespec_pair(const struct timeval tv[2], struct timespec ts[2]) {
    memset(ts, 0, sizeof(struct timespec) * 2);
    if (tv == NULL) {
        ts[0].tv_nsec = UTIME_NOW;
        ts[1].tv_nsec = UTIME_NOW;
        return;
    }
    ts[0].tv_sec = tv[0].tv_sec;
    ts[0].tv_nsec = tv[0].tv_usec * 1000L;
    ts[1].tv_sec = tv[1].tv_sec;
    ts[1].tv_nsec = tv[1].tv_usec * 1000L;
}

int __wrap_futimesat(int dirfd, const char *path, const struct timeval times[2]) {
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    struct timespec ts[2];
    sinomo_timeval_pair_to_timespec_pair(times, ts);
    return __wrap_utimensat(dirfd, path, ts, 0);
}

int __wrap_lutimes(const char *path, const struct timeval times[2]) {
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    struct timespec ts[2];
    sinomo_timeval_pair_to_timespec_pair(times, ts);
    return __wrap_utimensat(AT_FDCWD, path, ts, AT_SYMLINK_NOFOLLOW);
}

int __wrap_utime(const char *path, const struct utimbuf *times) {
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    struct timespec ts[2];
    if (times == NULL) {
        ts[0].tv_sec = 0;
        ts[0].tv_nsec = UTIME_NOW;
        ts[1].tv_sec = 0;
        ts[1].tv_nsec = UTIME_NOW;
    } else {
        ts[0].tv_sec = times->actime;
        ts[0].tv_nsec = 0;
        ts[1].tv_sec = times->modtime;
        ts[1].tv_nsec = 0;
    }
    return __wrap_utimensat(AT_FDCWD, path, ts, 0);
}

int __wrap_utimensat(int dirfd, const char *path, const struct timespec times[2], int flags) {
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (path[0] == 0 && (flags & AT_EMPTY_PATH) != 0) {
        if ((flags & ~(AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW)) != 0) {
            errno = EINVAL;
            return -1;
        }
        return futimens(dirfd, times);
    }
    if ((flags & AT_EMPTY_PATH) != 0) {
        errno = ENOENT;
        return -1;
    }
    int rc = __real_utimensat(dirfd, path, times, flags);
    if (rc == 0) {
        sinomo_inotify_emit_path(path, IN_ATTRIB);
    }
    return rc;
}

static long sinomo_pathconf_value(int name) {
    switch (name) {
#ifdef _PC_LINK_MAX
        case _PC_LINK_MAX:
            return 127;
#endif
#ifdef _PC_MAX_CANON
        case _PC_MAX_CANON:
            return 255;
#endif
#ifdef _PC_MAX_INPUT
        case _PC_MAX_INPUT:
            return 255;
#endif
#ifdef _PC_NAME_MAX
        case _PC_NAME_MAX:
            return 255;
#endif
#ifdef _PC_PATH_MAX
        case _PC_PATH_MAX:
            return 4096;
#endif
#ifdef _PC_PIPE_BUF
        case _PC_PIPE_BUF:
            return 4096;
#endif
#ifdef _PC_CHOWN_RESTRICTED
        case _PC_CHOWN_RESTRICTED:
            return 1;
#endif
#ifdef _PC_NO_TRUNC
        case _PC_NO_TRUNC:
            return 1;
#endif
#ifdef _PC_VDISABLE
        case _PC_VDISABLE:
            return 0;
#endif
#ifdef _PC_FILESIZEBITS
        case _PC_FILESIZEBITS:
            return 64;
#endif
#ifdef _PC_ALLOC_SIZE_MIN
        case _PC_ALLOC_SIZE_MIN:
            return 4096;
#endif
#ifdef _PC_REC_INCR_XFER_SIZE
        case _PC_REC_INCR_XFER_SIZE:
            return 4096;
#endif
#ifdef _PC_REC_MAX_XFER_SIZE
        case _PC_REC_MAX_XFER_SIZE:
            return (long)sinomo_env_u64("SINOMO_SANDBOX_WORKSPACE_LIMIT_BYTES", 1024ULL * 1024ULL * 1024ULL);
#endif
#ifdef _PC_REC_MIN_XFER_SIZE
        case _PC_REC_MIN_XFER_SIZE:
            return 1;
#endif
#ifdef _PC_REC_XFER_ALIGN
        case _PC_REC_XFER_ALIGN:
            return 4096;
#endif
#ifdef _PC_SYMLINK_MAX
        case _PC_SYMLINK_MAX:
            return 4096;
#endif
        default:
            errno = EINVAL;
            return -1;
    }
}

long __wrap_fpathconf(int fd, int name) {
    struct stat st;
    if (fstat(fd, &st) != 0) {
        return -1;
    }
    return sinomo_pathconf_value(name);
}

long __wrap_pathconf(const char *path, int name) {
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    struct stat st;
    if (stat(path, &st) != 0) {
        return -1;
    }
    return sinomo_pathconf_value(name);
}

long __wrap_sysconf(int name) {
    switch (name) {
#ifdef _SC_ARG_MAX
        case _SC_ARG_MAX:
            return 131072;
#endif
#ifdef _SC_CHILD_MAX
        case _SC_CHILD_MAX:
            return 1024;
#endif
#ifdef _SC_CLK_TCK
        case _SC_CLK_TCK:
            return 100;
#endif
#ifdef _SC_NGROUPS_MAX
        case _SC_NGROUPS_MAX:
            return 32;
#endif
#ifdef _SC_OPEN_MAX
        case _SC_OPEN_MAX:
            return 1024;
#endif
#ifdef _SC_STREAM_MAX
        case _SC_STREAM_MAX:
            return 1024;
#endif
#ifdef _SC_TZNAME_MAX
        case _SC_TZNAME_MAX:
            return 255;
#endif
#ifdef _SC_JOB_CONTROL
        case _SC_JOB_CONTROL:
            return 1;
#endif
#ifdef _SC_SAVED_IDS
        case _SC_SAVED_IDS:
            return 1;
#endif
#ifdef _SC_VERSION
        case _SC_VERSION:
            return 200809L;
#endif
#ifdef _SC_PAGE_SIZE
        case _SC_PAGE_SIZE:
            return 4096;
#endif
#ifdef _SC_PAGESIZE
#if !defined(_SC_PAGE_SIZE) || _SC_PAGESIZE != _SC_PAGE_SIZE
        case _SC_PAGESIZE:
            return 4096;
#endif
#endif
#ifdef _SC_IOV_MAX
        case _SC_IOV_MAX:
            return 1024;
#endif
#ifdef _SC_UIO_MAXIOV
#if !defined(_SC_IOV_MAX) || _SC_UIO_MAXIOV != _SC_IOV_MAX
        case _SC_UIO_MAXIOV:
            return 1024;
#endif
#endif
#ifdef _SC_THREADS
        case _SC_THREADS:
            return 200809L;
#endif
#ifdef _SC_THREAD_SAFE_FUNCTIONS
        case _SC_THREAD_SAFE_FUNCTIONS:
            return 200809L;
#endif
#ifdef _SC_THREAD_STACK_MIN
        case _SC_THREAD_STACK_MIN:
            return 16384;
#endif
#ifdef _SC_NPROCESSORS_CONF
        case _SC_NPROCESSORS_CONF:
            return 1;
#endif
#ifdef _SC_NPROCESSORS_ONLN
        case _SC_NPROCESSORS_ONLN:
            return 1;
#endif
#ifdef _SC_PHYS_PAGES
        case _SC_PHYS_PAGES:
            return __wrap_get_phys_pages();
#endif
#ifdef _SC_AVPHYS_PAGES
        case _SC_AVPHYS_PAGES:
            return __wrap_get_avphys_pages();
#endif
#ifdef _SC_HOST_NAME_MAX
        case _SC_HOST_NAME_MAX:
            return 64;
#endif
#ifdef _SC_TTY_NAME_MAX
        case _SC_TTY_NAME_MAX:
            return 32;
#endif
        default:
            errno = EINVAL;
            return -1;
    }
}

static const char *sinomo_confstr_value(int name) {
    switch (name) {
#ifdef _CS_PATH
        case _CS_PATH:
            return "/bin:/usr/bin";
#endif
#ifdef _CS_GNU_LIBC_VERSION
        case _CS_GNU_LIBC_VERSION:
            return "wasix-libc";
#endif
#ifdef _CS_GNU_LIBPTHREAD_VERSION
        case _CS_GNU_LIBPTHREAD_VERSION:
            return "wasix-threads";
#endif
        default:
            errno = EINVAL;
            return NULL;
    }
}

size_t __wrap_confstr(int name, char *buf, size_t len) {
    const char *value = sinomo_confstr_value(name);
    if (value == NULL) {
        return 0;
    }
    size_t required = strlen(value) + 1;
    if (buf != NULL && len > 0) {
        size_t copy_len = required <= len ? required : len;
        memcpy(buf, value, copy_len);
        if (required > len) {
            buf[len - 1] = 0;
        }
    }
    return required;
}

static void sinomo_fill_statvfs(struct statvfs *buf) {
    unsigned long block_size = 4096;
    unsigned long long workspace_bytes = sinomo_env_u64(
        "SINOMO_SANDBOX_WORKSPACE_LIMIT_BYTES",
        1024ULL * 1024ULL * 1024ULL
    );
    unsigned long long file_count = sinomo_env_u64("SINOMO_SANDBOX_FILE_COUNT_LIMIT", 100000ULL);
    fsblkcnt_t blocks = (fsblkcnt_t)((workspace_bytes + block_size - 1) / block_size);
    memset(buf, 0, sizeof(*buf));
    buf->f_bsize = block_size;
    buf->f_frsize = block_size;
    buf->f_blocks = blocks;
    buf->f_bfree = blocks;
    buf->f_bavail = blocks;
    buf->f_files = (fsfilcnt_t)file_count;
    buf->f_ffree = (fsfilcnt_t)file_count;
    buf->f_favail = (fsfilcnt_t)file_count;
    buf->f_namemax = 255;
}

int __wrap_statvfs(const char *path, struct statvfs *buf) {
    if (path == NULL || buf == NULL) {
        errno = EFAULT;
        return -1;
    }
    struct stat st;
    if (stat(path, &st) != 0) {
        return -1;
    }
    sinomo_fill_statvfs(buf);
    return 0;
}

int __wrap_fstatvfs(int fd, struct statvfs *buf) {
    if (buf == NULL) {
        errno = EFAULT;
        return -1;
    }
    struct stat st;
    if (fstat(fd, &st) != 0) {
        return -1;
    }
    sinomo_fill_statvfs(buf);
    return 0;
}

struct sinomo_linux_statfs {
    long f_type;
    long f_bsize;
    uint64_t f_blocks;
    uint64_t f_bfree;
    uint64_t f_bavail;
    uint64_t f_files;
    uint64_t f_ffree;
    long f_fsid[2];
    long f_namelen;
    long f_frsize;
    long f_flags;
    long f_spare[4];
};

static void sinomo_statvfs_to_linux_statfs(const struct statvfs *source, struct sinomo_linux_statfs *target) {
    memset(target, 0, sizeof(*target));
    target->f_type = 0x53494653L;
    target->f_bsize = (long)source->f_bsize;
    target->f_blocks = (uint64_t)source->f_blocks;
    target->f_bfree = (uint64_t)source->f_bfree;
    target->f_bavail = (uint64_t)source->f_bavail;
    target->f_files = (uint64_t)source->f_files;
    target->f_ffree = (uint64_t)source->f_ffree;
    target->f_namelen = (long)source->f_namemax;
    target->f_frsize = (long)source->f_frsize;
    target->f_flags = (long)source->f_flag;
}

int __wrap_statfs(const char *path, void *buf) {
    if (buf == NULL) {
        errno = EFAULT;
        return -1;
    }
    struct statvfs vfs;
    if (__wrap_statvfs(path, &vfs) != 0) {
        return -1;
    }
    sinomo_statvfs_to_linux_statfs(&vfs, (struct sinomo_linux_statfs *)buf);
    return 0;
}

int __wrap_fstatfs(int fd, void *buf) {
    if (buf == NULL) {
        errno = EFAULT;
        return -1;
    }
    struct statvfs vfs;
    if (__wrap_fstatvfs(fd, &vfs) != 0) {
        return -1;
    }
    sinomo_statvfs_to_linux_statfs(&vfs, (struct sinomo_linux_statfs *)buf);
    return 0;
}

struct sinomo_linux_statx_timestamp {
    int64_t tv_sec;
    uint32_t tv_nsec;
    int32_t __reserved;
};

struct sinomo_linux_statx {
    uint32_t stx_mask;
    uint32_t stx_blksize;
    uint64_t stx_attributes;
    uint32_t stx_nlink;
    uint32_t stx_uid;
    uint32_t stx_gid;
    uint16_t stx_mode;
    uint16_t __spare0[1];
    uint64_t stx_ino;
    uint64_t stx_size;
    uint64_t stx_blocks;
    uint64_t stx_attributes_mask;
    struct sinomo_linux_statx_timestamp stx_atime;
    struct sinomo_linux_statx_timestamp stx_btime;
    struct sinomo_linux_statx_timestamp stx_ctime;
    struct sinomo_linux_statx_timestamp stx_mtime;
    uint32_t stx_rdev_major;
    uint32_t stx_rdev_minor;
    uint32_t stx_dev_major;
    uint32_t stx_dev_minor;
    uint64_t __spare2[14];
};

static void sinomo_stat_to_statx_timestamp(time_t sec, long nsec, struct sinomo_linux_statx_timestamp *target) {
    target->tv_sec = (int64_t)sec;
    target->tv_nsec = nsec < 0 ? 0U : (uint32_t)nsec;
}

static uint64_t sinomo_statx_attribute_mask(void) {
    return STATX_ATTR_COMPRESSED |
        STATX_ATTR_IMMUTABLE |
        STATX_ATTR_APPEND |
        STATX_ATTR_NODUMP |
        STATX_ATTR_ENCRYPTED |
        STATX_ATTR_AUTOMOUNT |
        STATX_ATTR_MOUNT_ROOT |
        STATX_ATTR_VERITY |
        STATX_ATTR_DAX |
        STATX_ATTR_WRITE_ATOMIC;
}

int __wrap_statx(int dirfd, const char *path, int flags, unsigned int mask, void *buf) {
    if (buf == NULL) {
        errno = EFAULT;
        return -1;
    }
    struct stat st;
    int stat_result = path != NULL && path[0] == 0 && (flags & AT_EMPTY_PATH) != 0
        ? fstat(dirfd, &st)
        : fstatat(dirfd, path, &st, flags);
    if (stat_result != 0) {
        return -1;
    }
    struct sinomo_linux_statx *statx = (struct sinomo_linux_statx *)buf;
    memset(statx, 0, sizeof(*statx));
    statx->stx_mask = mask == 0 ? STATX_BASIC_STATS : (mask & STATX_BASIC_STATS);
    statx->stx_blksize = 4096;
    statx->stx_attributes = 0;
    statx->stx_attributes_mask = sinomo_statx_attribute_mask();
    statx->stx_nlink = (uint32_t)st.st_nlink;
    statx->stx_uid = (uint32_t)st.st_uid;
    statx->stx_gid = (uint32_t)st.st_gid;
    statx->stx_mode = (uint16_t)st.st_mode;
    statx->stx_ino = (uint64_t)st.st_ino;
    statx->stx_size = (uint64_t)st.st_size;
    statx->stx_blocks = (uint64_t)st.st_blocks;
    sinomo_stat_to_statx_timestamp(st.st_atim.tv_sec, st.st_atim.tv_nsec, &statx->stx_atime);
    sinomo_stat_to_statx_timestamp(st.st_ctim.tv_sec, st.st_ctim.tv_nsec, &statx->stx_ctime);
    sinomo_stat_to_statx_timestamp(st.st_mtim.tv_sec, st.st_mtim.tv_nsec, &statx->stx_mtime);
    statx->stx_rdev_major = (uint32_t)((uint64_t)st.st_rdev >> 8);
    statx->stx_rdev_minor = (uint32_t)((uint64_t)st.st_rdev & 0xffU);
    statx->stx_dev_major = (uint32_t)((uint64_t)st.st_dev >> 8);
    statx->stx_dev_minor = (uint32_t)((uint64_t)st.st_dev & 0xffU);
    return 0;
}

struct sinomo_cachestat_range {
    uint64_t off;
    uint64_t len;
};

struct sinomo_cachestat {
    uint64_t nr_cache;
    uint64_t nr_dirty;
    uint64_t nr_writeback;
    uint64_t nr_evicted;
    uint64_t nr_recently_evicted;
};

int __wrap_cachestat(int fd, const void *range_ptr, void *cstat_ptr, unsigned int flags) {
    if (range_ptr == NULL || cstat_ptr == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    struct stat st;
    if (fstat(fd, &st) != 0) {
        return -1;
    }
    const struct sinomo_cachestat_range *range = (const struct sinomo_cachestat_range *)range_ptr;
    struct sinomo_cachestat *cstat = (struct sinomo_cachestat *)cstat_ptr;
    memset(cstat, 0, sizeof(*cstat));
    uint64_t file_size = st.st_size < 0 ? 0 : (uint64_t)st.st_size;
    uint64_t start = range->off;
    uint64_t end = range->len == 0 || range->len > UINT64_MAX - start
        ? UINT64_MAX
        : start + range->len;
    if (start >= file_size || end <= start) {
        return 0;
    }
    if (end > file_size) {
        end = file_size;
    }
    uint64_t page_size = 4096;
    cstat->nr_cache = (end - start + page_size - 1) / page_size;
    return 0;
}

#define SINOMO_ROOT_MNT_ID 1ULL
#define SINOMO_MNT_NS_ID 1ULL
#define SINOMO_STATMOUNT_SUPPORTED_MASK (STATMOUNT_SB_BASIC | STATMOUNT_MNT_BASIC | STATMOUNT_PROPAGATE_FROM | STATMOUNT_MNT_ROOT | STATMOUNT_MNT_POINT | STATMOUNT_FS_TYPE | STATMOUNT_MNT_NS_ID | STATMOUNT_MNT_OPTS | STATMOUNT_FS_SUBTYPE | STATMOUNT_SB_SOURCE | STATMOUNT_OPT_ARRAY | STATMOUNT_OPT_SEC_ARRAY | STATMOUNT_SUPPORTED_MASK | STATMOUNT_MNT_UIDMAP | STATMOUNT_MNT_GIDMAP)

struct sinomo_mnt_id_req {
    uint32_t size;
    uint32_t mnt_ns_fd;
    uint64_t mnt_id;
    uint64_t param;
    uint64_t mnt_ns_id;
};

struct sinomo_statmount {
    uint32_t size;
    uint32_t mnt_opts;
    uint64_t mask;
    uint32_t sb_dev_major;
    uint32_t sb_dev_minor;
    uint64_t sb_magic;
    uint32_t sb_flags;
    uint32_t fs_type;
    uint64_t mnt_id;
    uint64_t mnt_parent_id;
    uint32_t mnt_id_old;
    uint32_t mnt_parent_id_old;
    uint64_t mnt_attr;
    uint64_t mnt_propagation;
    uint64_t mnt_peer_group;
    uint64_t mnt_master;
    uint64_t propagate_from;
    uint32_t mnt_root;
    uint32_t mnt_point;
    uint64_t mnt_ns_id;
    uint32_t fs_subtype;
    uint32_t sb_source;
    uint32_t opt_num;
    uint32_t opt_array;
    uint32_t opt_sec_num;
    uint32_t opt_sec_array;
    uint64_t supported_mask;
    uint32_t mnt_uidmap_num;
    uint32_t mnt_uidmap;
    uint32_t mnt_gidmap_num;
    uint32_t mnt_gidmap;
    uint64_t __spare2[43];
    char str[];
};

static int sinomo_mnt_id_req_valid(const struct sinomo_mnt_id_req *req) {
    return req != NULL && (req->size == MNT_ID_REQ_SIZE_VER0 || req->size >= MNT_ID_REQ_SIZE_VER1);
}

static int sinomo_statmount_append_string(
    struct sinomo_statmount *mount,
    size_t buffer_size,
    uint32_t *offset_field,
    size_t *used,
    const char *value
) {
    size_t length = strlen(value) + 1U;
    if (*used > UINT32_MAX || length > UINT32_MAX - *used) {
        errno = EOVERFLOW;
        return -1;
    }
    if (buffer_size < *used + length) {
        mount->size = (uint32_t)(*used + length);
        errno = EOVERFLOW;
        return -1;
    }
    *offset_field = (uint32_t)(*used - offsetof(struct sinomo_statmount, str));
    memcpy(((char *)mount) + *used, value, length);
    *used += length;
    return 0;
}

static long sinomo_statmount(const struct sinomo_mnt_id_req *req, struct sinomo_statmount *mount, size_t buffer_size, unsigned int flags) {
    if (!sinomo_mnt_id_req_valid(req) || mount == NULL) {
        errno = req == NULL || mount == NULL ? EFAULT : EINVAL;
        return -1;
    }
    if (flags != 0 || (req->mnt_id != SINOMO_ROOT_MNT_ID && req->mnt_id != LSMT_ROOT)) {
        errno = EINVAL;
        return -1;
    }
    size_t fixed_size = offsetof(struct sinomo_statmount, str);
    if (buffer_size < fixed_size) {
        if (buffer_size >= sizeof(uint32_t)) {
            mount->size = (uint32_t)fixed_size;
        }
        errno = EOVERFLOW;
        return -1;
    }

    uint64_t requested = req->param & SINOMO_STATMOUNT_SUPPORTED_MASK;
    memset(mount, 0, fixed_size);
    mount->size = (uint32_t)fixed_size;
    mount->mask = requested;
    size_t used = fixed_size;

    if ((requested & STATMOUNT_SB_BASIC) != 0) {
        mount->sb_magic = 0x53494653ULL;
    }
    if ((requested & STATMOUNT_MNT_BASIC) != 0) {
        mount->mnt_id = SINOMO_ROOT_MNT_ID;
        mount->mnt_parent_id = SINOMO_ROOT_MNT_ID;
        mount->mnt_id_old = (uint32_t)SINOMO_ROOT_MNT_ID;
        mount->mnt_parent_id_old = (uint32_t)SINOMO_ROOT_MNT_ID;
        mount->mnt_attr = 0;
        mount->mnt_propagation = MS_PRIVATE;
    }
    if ((requested & STATMOUNT_PROPAGATE_FROM) != 0) {
        mount->propagate_from = SINOMO_ROOT_MNT_ID;
    }
    if ((requested & STATMOUNT_MNT_NS_ID) != 0) {
        mount->mnt_ns_id = SINOMO_MNT_NS_ID;
    }
    if ((requested & STATMOUNT_SUPPORTED_MASK) != 0) {
        mount->supported_mask = SINOMO_STATMOUNT_SUPPORTED_MASK;
    }
    if ((requested & STATMOUNT_MNT_ROOT) != 0 && sinomo_statmount_append_string(mount, buffer_size, &mount->mnt_root, &used, "/") != 0) {
        return -1;
    }
    if ((requested & STATMOUNT_MNT_POINT) != 0 && sinomo_statmount_append_string(mount, buffer_size, &mount->mnt_point, &used, "/") != 0) {
        return -1;
    }
    if ((requested & STATMOUNT_FS_TYPE) != 0 && sinomo_statmount_append_string(mount, buffer_size, &mount->fs_type, &used, "sifs") != 0) {
        return -1;
    }
    if ((requested & STATMOUNT_MNT_OPTS) != 0 && sinomo_statmount_append_string(mount, buffer_size, &mount->mnt_opts, &used, "rw") != 0) {
        return -1;
    }
    if ((requested & STATMOUNT_FS_SUBTYPE) != 0 && sinomo_statmount_append_string(mount, buffer_size, &mount->fs_subtype, &used, "") != 0) {
        return -1;
    }
    if ((requested & STATMOUNT_SB_SOURCE) != 0 && sinomo_statmount_append_string(mount, buffer_size, &mount->sb_source, &used, "sifs") != 0) {
        return -1;
    }
    if ((requested & STATMOUNT_OPT_ARRAY) != 0 && sinomo_statmount_append_string(mount, buffer_size, &mount->opt_array, &used, "rw") != 0) {
        return -1;
    }
    if ((requested & STATMOUNT_OPT_ARRAY) != 0) {
        mount->opt_num = 1;
    }
    if ((requested & STATMOUNT_OPT_SEC_ARRAY) != 0 && sinomo_statmount_append_string(mount, buffer_size, &mount->opt_sec_array, &used, "") != 0) {
        return -1;
    }
    if ((requested & STATMOUNT_MNT_UIDMAP) != 0 && sinomo_statmount_append_string(mount, buffer_size, &mount->mnt_uidmap, &used, "0 0 4294967295") != 0) {
        return -1;
    }
    if ((requested & STATMOUNT_MNT_UIDMAP) != 0) {
        mount->mnt_uidmap_num = 1;
    }
    if ((requested & STATMOUNT_MNT_GIDMAP) != 0 && sinomo_statmount_append_string(mount, buffer_size, &mount->mnt_gidmap, &used, "0 0 4294967295") != 0) {
        return -1;
    }
    if ((requested & STATMOUNT_MNT_GIDMAP) != 0) {
        mount->mnt_gidmap_num = 1;
    }

    mount->size = (uint32_t)used;
    return 0;
}

static long sinomo_listmount(const struct sinomo_mnt_id_req *req, uint64_t *ids, size_t count, unsigned int flags) {
    if (!sinomo_mnt_id_req_valid(req) || ids == NULL) {
        errno = req == NULL || ids == NULL ? EFAULT : EINVAL;
        return -1;
    }
    if ((flags & ~LISTMOUNT_REVERSE) != 0) {
        errno = EINVAL;
        return -1;
    }
    if (count == 0) {
        return 0;
    }
    if (req->mnt_id == LSMT_ROOT && req->param < SINOMO_ROOT_MNT_ID) {
        ids[0] = SINOMO_ROOT_MNT_ID;
        return 1;
    }
    if (req->mnt_id == SINOMO_ROOT_MNT_ID) {
        return 0;
    }
    errno = EINVAL;
    return -1;
}

struct sinomo_linux_file_handle {
    unsigned int handle_bytes;
    int handle_type;
    unsigned char f_handle[];
};

struct sinomo_file_handle_payload {
    uint64_t cookie;
    uint64_t dev;
    uint64_t ino;
};

struct sinomo_file_handle_entry {
    int active;
    uint64_t cookie;
    dev_t dev;
    ino_t ino;
    char path[PATH_MAX];
};

#define SINOMO_FILE_HANDLE_TYPE 0x53494e4f
#define SINOMO_FILE_HANDLE_ENTRIES 256

static struct sinomo_file_handle_entry sinomo_file_handles[SINOMO_FILE_HANDLE_ENTRIES];
static uint64_t sinomo_next_file_handle_cookie = 1;

static struct sinomo_file_handle_entry *sinomo_file_handle_find(uint64_t cookie) {
    for (int index = 0; index < SINOMO_FILE_HANDLE_ENTRIES; index++) {
        if (sinomo_file_handles[index].active && sinomo_file_handles[index].cookie == cookie) {
            return &sinomo_file_handles[index];
        }
    }
    return NULL;
}

static struct sinomo_file_handle_entry *sinomo_file_handle_upsert(dev_t dev, ino_t ino, const char *path) {
    struct sinomo_file_handle_entry *free_slot = NULL;
    for (int index = 0; index < SINOMO_FILE_HANDLE_ENTRIES; index++) {
        struct sinomo_file_handle_entry *entry = &sinomo_file_handles[index];
        if (entry->active && entry->dev == dev && entry->ino == ino) {
            snprintf(entry->path, sizeof(entry->path), "%s", path);
            return entry;
        }
        if (!entry->active && free_slot == NULL) {
            free_slot = entry;
        }
    }
    if (free_slot == NULL) {
        free_slot = &sinomo_file_handles[0];
    }
    memset(free_slot, 0, sizeof(*free_slot));
    free_slot->active = 1;
    free_slot->cookie = sinomo_next_file_handle_cookie++;
    if (sinomo_next_file_handle_cookie == 0) {
        sinomo_next_file_handle_cookie = 1;
    }
    free_slot->dev = dev;
    free_slot->ino = ino;
    snprintf(free_slot->path, sizeof(free_slot->path), "%s", path);
    return free_slot;
}

int __wrap_name_to_handle_at(int dirfd, const char *path, void *handle_ptr, int *mount_id, int flags) {
    if (path == NULL || handle_ptr == NULL || mount_id == NULL) {
        errno = EFAULT;
        return -1;
    }
    if ((flags & ~(AT_EMPTY_PATH | AT_SYMLINK_FOLLOW)) != 0) {
        errno = EINVAL;
        return -1;
    }
    struct stat st;
    int stat_flags = (flags & AT_SYMLINK_FOLLOW) != 0 ? 0 : AT_SYMLINK_NOFOLLOW;
    int stat_result = path[0] == 0 && (flags & AT_EMPTY_PATH) != 0
        ? fstat(dirfd, &st)
        : fstatat(dirfd, path, &st, stat_flags);
    if (stat_result != 0) {
        return -1;
    }

    struct sinomo_linux_file_handle *handle = (struct sinomo_linux_file_handle *)handle_ptr;
    unsigned int required = (unsigned int)sizeof(struct sinomo_file_handle_payload);
    if (handle->handle_bytes < required) {
        handle->handle_bytes = required;
        errno = EOVERFLOW;
        return -1;
    }

    char resolved[PATH_MAX];
    if (sinomo_resolve_at_path(dirfd, path, flags, resolved, sizeof(resolved)) != 0) {
        return -1;
    }
    struct sinomo_file_handle_entry *entry = sinomo_file_handle_upsert(st.st_dev, st.st_ino, resolved);
    struct sinomo_file_handle_payload payload = {
        .cookie = entry->cookie,
        .dev = (uint64_t)st.st_dev,
        .ino = (uint64_t)st.st_ino,
    };
    handle->handle_bytes = required;
    handle->handle_type = SINOMO_FILE_HANDLE_TYPE;
    memcpy(handle->f_handle, &payload, sizeof(payload));
    *mount_id = (int)((uint64_t)st.st_dev & 0x7fffffffU);
    return 0;
}

int __wrap_open_by_handle_at(int mount_fd, void *handle_ptr, int flags) {
    if (handle_ptr == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (mount_fd != AT_FDCWD) {
        struct stat mount_stat;
        if (fstat(mount_fd, &mount_stat) != 0) {
            return -1;
        }
    }
    struct sinomo_linux_file_handle *handle = (struct sinomo_linux_file_handle *)handle_ptr;
    if (handle->handle_type != SINOMO_FILE_HANDLE_TYPE ||
        handle->handle_bytes < sizeof(struct sinomo_file_handle_payload)) {
        errno = ESTALE;
        return -1;
    }
    struct sinomo_file_handle_payload payload;
    memcpy(&payload, handle->f_handle, sizeof(payload));
    struct sinomo_file_handle_entry *entry = sinomo_file_handle_find(payload.cookie);
    if (entry == NULL || (uint64_t)entry->dev != payload.dev || (uint64_t)entry->ino != payload.ino) {
        errno = ESTALE;
        return -1;
    }

    struct stat st;
    if (stat(entry->path, &st) != 0) {
        return -1;
    }
    if ((uint64_t)st.st_dev != payload.dev || (uint64_t)st.st_ino != payload.ino) {
        errno = ESTALE;
        return -1;
    }
    return __wrap_openat(AT_FDCWD, entry->path, flags & ~(O_CREAT | O_EXCL | O_TRUNC), 0);
}

struct sinomo_linux_open_how {
    uint64_t flags;
    uint64_t mode;
    uint64_t resolve;
};

static int sinomo_path_has_parent_escape(const char *path) {
    const char *cursor = path;
    while (cursor != NULL && *cursor != 0) {
        while (*cursor == '/') {
            cursor++;
        }
        const char *end = strchr(cursor, '/');
        size_t len = end == NULL ? strlen(cursor) : (size_t)(end - cursor);
        if (len == 2 && cursor[0] == '.' && cursor[1] == '.') {
            return 1;
        }
        cursor = end;
    }
    return 0;
}

static int sinomo_path_has_magic_link_prefix(const char *path) {
    if (path == NULL) {
        return 0;
    }
    return strncmp(path, "/proc/self/fd/", 14) == 0 ||
        strncmp(path, "/proc/thread-self/fd/", 21) == 0 ||
        strncmp(path, "/dev/fd/", 8) == 0 ||
        strcmp(path, "/proc/self/exe") == 0 ||
        strcmp(path, "/proc/self/cwd") == 0 ||
        strcmp(path, "/proc/self/root") == 0;
}

int __wrap_openat2(int dirfd, const char *path, const void *how_ptr, size_t size) {
    if (path == NULL || how_ptr == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (size < sizeof(struct sinomo_linux_open_how)) {
        errno = EINVAL;
        return -1;
    }
    const struct sinomo_linux_open_how *how = (const struct sinomo_linux_open_how *)how_ptr;
    uint64_t supported_resolve =
        RESOLVE_NO_XDEV | RESOLVE_NO_MAGICLINKS | RESOLVE_NO_SYMLINKS |
        RESOLVE_BENEATH | RESOLVE_IN_ROOT | RESOLVE_CACHED;
    if ((how->resolve & ~supported_resolve) != 0 || how->flags > (uint64_t)INT_MAX || how->mode > (uint64_t)UINT_MAX) {
        errno = EINVAL;
        return -1;
    }
    if ((how->resolve & (RESOLVE_BENEATH | RESOLVE_IN_ROOT)) != 0 &&
        (path[0] == '/' || sinomo_path_has_parent_escape(path))) {
        errno = EXDEV;
        return -1;
    }
    if ((how->resolve & (RESOLVE_NO_MAGICLINKS | RESOLVE_NO_SYMLINKS)) != 0 &&
        sinomo_path_has_magic_link_prefix(path)) {
        errno = ELOOP;
        return -1;
    }
    int flags = (int)how->flags;
#ifdef O_NOFOLLOW
    if ((how->resolve & RESOLVE_NO_SYMLINKS) != 0) {
        flags |= O_NOFOLLOW;
    }
#endif
    return __wrap_openat(dirfd, path, flags, (mode_t)how->mode);
}

int __wrap_renameat2(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, unsigned int flags) {
    if (flags == 0) {
        return __wrap_renameat(olddirfd, oldpath, newdirfd, newpath);
    }
    if (flags == RENAME_NOREPLACE) {
        struct stat st;
        if (fstatat(newdirfd, newpath, &st, AT_SYMLINK_NOFOLLOW) == 0) {
            errno = EEXIST;
            return -1;
        }
        if (errno != ENOENT) {
            return -1;
        }
        return __wrap_renameat(olddirfd, oldpath, newdirfd, newpath);
    }
    errno = EINVAL;
    return -1;
}

struct sinomo_mmsghdr {
    struct msghdr msg_hdr;
    unsigned int msg_len;
};

int __wrap_sendmmsg(int fd, void *messages_ptr, unsigned int vlen, unsigned int flags) {
    struct sinomo_mmsghdr *messages = (struct sinomo_mmsghdr *)messages_ptr;
    if (messages == NULL && vlen > 0) {
        errno = EFAULT;
        return -1;
    }
    unsigned int sent = 0;
    for (; sent < vlen; sent++) {
        ssize_t rc = __wrap_sendmsg(fd, &messages[sent].msg_hdr, flags);
        if (rc < 0) {
            return sent > 0 ? (int)sent : -1;
        }
        messages[sent].msg_len = (unsigned int)rc;
    }
    return (int)sent;
}

int __wrap_recvmmsg(int fd, void *messages_ptr, unsigned int vlen, unsigned int flags, struct timespec *timeout) {
    (void)timeout;
    struct sinomo_mmsghdr *messages = (struct sinomo_mmsghdr *)messages_ptr;
    if (messages == NULL && vlen > 0) {
        errno = EFAULT;
        return -1;
    }
    unsigned int received = 0;
    for (; received < vlen; received++) {
        ssize_t rc = __wrap_recvmsg(fd, &messages[received].msg_hdr, flags);
        if (rc < 0) {
            return received > 0 ? (int)received : -1;
        }
        messages[received].msg_len = (unsigned int)rc;
        if (rc == 0) {
            received++;
            break;
        }
    }
    return (int)received;
}

mode_t __wrap_umask(mode_t mask) {
    unsigned int previous = 0;
    int rc = sinomo_proc_umask((unsigned int)mask, &previous);
    if (rc != 0) {
        errno = rc;
        return (mode_t)-1;
    }
    return (mode_t)previous;
}

int __wrap_chmod(const char *path, mode_t mode) {
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    int rc = sinomo_path_chmod(path, (unsigned int)strlen(path), (unsigned int)mode);
    if (rc == 0) {
        sinomo_inotify_emit_path(path, IN_ATTRIB);
        return 0;
    }
    errno = rc;
    return -1;
}

int __wrap_fchmod(int fd, mode_t mode) {
    int rc = sinomo_fd_chmod(fd, (unsigned int)mode);
    if (rc == 0) {
        return 0;
    }
    errno = rc;
    return -1;
}

int __wrap_chown(const char *path, uid_t uid, gid_t gid) {
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    int rc = sinomo_path_chown(
        path,
        (unsigned int)strlen(path),
        sinomo_uid(uid),
        sinomo_gid(gid)
    );
    if (rc == 0) {
        sinomo_inotify_emit_path(path, IN_ATTRIB);
        return 0;
    }
    errno = rc;
    return -1;
}

int __wrap_lchown(const char *path, uid_t uid, gid_t gid) {
    return __wrap_chown(path, uid, gid);
}

int __wrap_fchown(int fd, uid_t uid, gid_t gid) {
    int rc = sinomo_fd_chown(fd, sinomo_uid(uid), sinomo_gid(gid));
    if (rc == 0) {
        return 0;
    }
    errno = rc;
    return -1;
}

static int sinomo_with_dirfd_cwd(int dirfd, int (*operation)(void *), void *data) {
    if (dirfd == AT_FDCWD) {
        return operation(data);
    }

    int saved_cwd = open(".", O_RDONLY | O_DIRECTORY);
    if (saved_cwd < 0) {
        return -1;
    }
    if (fchdir(dirfd) != 0) {
        int saved_errno = errno;
        close(saved_cwd);
        errno = saved_errno;
        return -1;
    }

    int result = operation(data);
    int operation_errno = errno;
    if (fchdir(saved_cwd) != 0 && result == 0) {
        result = -1;
        operation_errno = errno;
    }
    close(saved_cwd);
    errno = operation_errno;
    return result;
}

static int sinomo_resolve_at_path(int dirfd, const char *path, int flags, char *resolved, size_t resolved_len) {
    if (path == NULL || resolved == NULL || resolved_len == 0) {
        errno = EFAULT;
        return -1;
    }
    if (path[0] == 0) {
        if ((flags & AT_EMPTY_PATH) == 0) {
            errno = ENOENT;
            return -1;
        }
        snprintf(resolved, resolved_len, "/proc/self/fd/%d", dirfd);
        return 0;
    }
    if (path[0] == '/') {
        snprintf(resolved, resolved_len, "%s", path);
        return 0;
    }

    int saved_cwd = -1;
    if (dirfd != AT_FDCWD) {
        saved_cwd = open(".", O_RDONLY | O_DIRECTORY);
        if (saved_cwd < 0) {
            return -1;
        }
        if (fchdir(dirfd) != 0) {
            int saved_errno = errno;
            close(saved_cwd);
            errno = saved_errno;
            return -1;
        }
    }

    char cwd[PATH_MAX];
    int result = getcwd(cwd, sizeof(cwd)) == NULL ? -1 : 0;
    int operation_errno = errno;
    if (result == 0) {
        snprintf(resolved, resolved_len, strcmp(cwd, "/") == 0 ? "/%s" : "%s/%s", cwd, path);
        operation_errno = 0;
    }
    if (saved_cwd >= 0) {
        if (fchdir(saved_cwd) != 0 && result == 0) {
            result = -1;
            operation_errno = errno;
        }
        close(saved_cwd);
    }
    errno = operation_errno;
    return result;
}

struct sinomo_fchmodat_data {
    const char *path;
    mode_t mode;
};

static int sinomo_fchmodat_operation(void *raw) {
    struct sinomo_fchmodat_data *data = raw;
    return __wrap_chmod(data->path, data->mode);
}

int __wrap_fchmodat(int dirfd, const char *path, mode_t mode, int flags) {
    if (path == NULL) {
        errno = EINVAL;
        return -1;
    }
    if (path[0] == 0 && (flags & AT_EMPTY_PATH) != 0) {
        if ((flags & ~(AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW)) != 0) {
            errno = EINVAL;
            return -1;
        }
        return __wrap_fchmod(dirfd, mode);
    }
    if (path[0] == '/') {
        return __wrap_chmod(path, mode);
    }
    if ((flags & ~(AT_SYMLINK_NOFOLLOW)) != 0) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_fchmodat_data data = {
        .path = path,
        .mode = mode,
    };
    return sinomo_with_dirfd_cwd(dirfd, sinomo_fchmodat_operation, &data);
}

struct sinomo_fchownat_data {
    const char *path;
    uid_t uid;
    gid_t gid;
};

static int sinomo_fchownat_operation(void *raw) {
    struct sinomo_fchownat_data *data = raw;
    return __wrap_chown(data->path, data->uid, data->gid);
}

int __wrap_fchownat(int dirfd, const char *path, uid_t uid, gid_t gid, int flags) {
    if (path == NULL) {
        errno = EINVAL;
        return -1;
    }
    if (path[0] == 0 && (flags & AT_EMPTY_PATH) != 0) {
        if ((flags & ~(AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW)) != 0) {
            errno = EINVAL;
            return -1;
        }
        return __wrap_fchown(dirfd, uid, gid);
    }
    if (path[0] == '/') {
        return __wrap_chown(path, uid, gid);
    }
    if ((flags & ~(AT_SYMLINK_NOFOLLOW)) != 0) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_fchownat_data data = {
        .path = path,
        .uid = uid,
        .gid = gid,
    };
    return sinomo_with_dirfd_cwd(dirfd, sinomo_fchownat_operation, &data);
}

static int sinomo_same_or_at_fdcwd(int left, int right) {
    return left == right || left == AT_FDCWD || right == AT_FDCWD;
}

struct sinomo_openat_data {
    const char *path;
    int flags;
    mode_t mode;
};

static int sinomo_openat_operation(void *raw) {
    struct sinomo_openat_data *data = raw;
    return open(data->path, data->flags, data->mode);
}

int __wrap_openat(int dirfd, const char *path, int flags, ...) {
    if (path == NULL) {
        errno = EINVAL;
        return -1;
    }
    va_list ap;
    va_start(ap, flags);
    mode_t mode = 0;
    if ((flags & O_CREAT) != 0) {
        mode = (mode_t)va_arg(ap, int);
    }
    va_end(ap);
    if (path[0] == '/') {
        if (strcmp(path, "/dev/ptmx") == 0) {
            return __wrap_posix_openpt(flags);
        }
        int pty_id = sinomo_pty_path_id(path);
        if (pty_id > 0) {
            return sinomo_pty_open_slave_id(pty_id, flags);
        }
        int fd = open(path, flags, mode);
        if (fd >= 0) {
            if ((flags & O_CREAT) != 0) {
                sinomo_inotify_emit_path(path, IN_CREATE);
            }
            sinomo_inotify_emit_path(path, IN_OPEN);
        }
        return fd;
    }
    struct sinomo_openat_data data = {
        .path = path,
        .flags = flags,
        .mode = mode,
    };
    if (dirfd == AT_FDCWD) {
        char cwd_path[PATH_MAX];
        if (getcwd(cwd_path, sizeof(cwd_path)) != NULL) {
            char combined[PATH_MAX];
            if (snprintf(combined, sizeof(combined), "%s/%s", cwd_path, path) < (int)sizeof(combined)) {
                int pty_id = sinomo_pty_path_id(combined);
                if (pty_id > 0) {
                    return sinomo_pty_open_slave_id(pty_id, flags);
                }
            }
        }
    }
    int fd = sinomo_with_dirfd_cwd(dirfd, sinomo_openat_operation, &data);
    if (fd >= 0) {
        if ((flags & O_CREAT) != 0) {
            sinomo_inotify_emit_path(path, IN_CREATE);
        }
        sinomo_inotify_emit_path(path, IN_OPEN);
    }
    return fd;
}

int __wrap_faccessat(int dirfd, const char *path, int mode, int flags) {
    if (path == NULL) {
        errno = EINVAL;
        return -1;
    }
    if ((mode & ~(R_OK | W_OK | X_OK | F_OK)) != 0) {
        errno = EINVAL;
        return -1;
    }
    if ((flags & ~(AT_EACCESS | AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH)) != 0) {
        errno = EINVAL;
        return -1;
    }
    if (path[0] == 0 && (flags & AT_EMPTY_PATH) != 0) {
        struct stat st;
        if (fstat(dirfd, &st) != 0) {
            return -1;
        }
        if (mode == F_OK) {
            return 0;
        }
        mode_t permitted = 0;
        if ((st.st_mode & S_IRUSR) != 0) {
            permitted |= R_OK;
        }
        if ((st.st_mode & S_IWUSR) != 0) {
            permitted |= W_OK;
        }
        if ((st.st_mode & S_IXUSR) != 0) {
            permitted |= X_OK;
        }
        if ((mode & permitted) == mode) {
            return 0;
        }
        errno = EACCES;
        return -1;
    }
    if ((flags & AT_EMPTY_PATH) != 0) {
        errno = ENOENT;
        return -1;
    }

    struct stat st;
    int result;
    if (path[0] == '/') {
        result = (flags & AT_SYMLINK_NOFOLLOW) != 0 ? lstat(path, &st) : stat(path, &st);
    } else {
        result = fstatat(
            dirfd,
            path,
            &st,
            (flags & AT_SYMLINK_NOFOLLOW) != 0 ? AT_SYMLINK_NOFOLLOW : 0
        );
    }
    if (result != 0) {
        return -1;
    }
    if (mode == F_OK) {
        return 0;
    }

    mode_t permitted = 0;
    if ((st.st_mode & S_IRUSR) != 0) {
        permitted |= R_OK;
    }
    if ((st.st_mode & S_IWUSR) != 0) {
        permitted |= W_OK;
    }
    if ((st.st_mode & S_IXUSR) != 0) {
        permitted |= X_OK;
    }
    if ((mode & permitted) == mode) {
        return 0;
    }
    errno = EACCES;
    return -1;
}

int __wrap_access(const char *path, int mode) {
    return __wrap_faccessat(AT_FDCWD, path, mode, 0);
}

struct sinomo_mkdirat_data {
    const char *path;
    mode_t mode;
};

static int sinomo_mkdirat_operation(void *raw) {
    struct sinomo_mkdirat_data *data = raw;
    return mkdir(data->path, data->mode);
}

int __wrap_mkdirat(int dirfd, const char *path, mode_t mode) {
    if (path == NULL) {
        errno = EINVAL;
        return -1;
    }
    if (path[0] == '/') {
        int rc = mkdir(path, mode);
        if (rc == 0) {
            sinomo_inotify_emit_path(path, IN_CREATE | IN_ISDIR);
        }
        return rc;
    }
    struct sinomo_mkdirat_data data = {
        .path = path,
        .mode = mode,
    };
    int rc = sinomo_with_dirfd_cwd(dirfd, sinomo_mkdirat_operation, &data);
    if (rc == 0) {
        sinomo_inotify_emit_path(path, IN_CREATE | IN_ISDIR);
    }
    return rc;
}

int __wrap_mknodat(int dirfd, const char *path, mode_t mode, dev_t dev) {
    (void)dev;
    if (path == NULL) {
        errno = EINVAL;
        return -1;
    }
    if ((mode & S_IFMT) != 0 && (mode & S_IFMT) != S_IFREG) {
        errno = EPERM;
        return -1;
    }

    int fd = __wrap_openat(dirfd, path, O_WRONLY | O_CREAT | O_EXCL, mode & 07777);
    if (fd < 0) {
        return -1;
    }
    if (__wrap_close(fd) != 0) {
        return -1;
    }
    sinomo_inotify_emit_path(path, IN_CREATE);
    return 0;
}

int __wrap_mknod(const char *path, mode_t mode, dev_t dev) {
    return __wrap_mknodat(AT_FDCWD, path, mode, dev);
}

struct sinomo_unlinkat_data {
    const char *path;
    int flags;
};

static int sinomo_unlinkat_operation(void *raw) {
    struct sinomo_unlinkat_data *data = raw;
    if ((data->flags & AT_REMOVEDIR) != 0) {
        return rmdir(data->path);
    }
    return unlink(data->path);
}

int __wrap_unlinkat(int dirfd, const char *path, int flags) {
    if (path == NULL) {
        errno = EINVAL;
        return -1;
    }
    if ((flags & ~AT_REMOVEDIR) != 0) {
        errno = EINVAL;
        return -1;
    }
    if (path[0] == '/') {
        if ((flags & AT_REMOVEDIR) != 0) {
            int rc = rmdir(path);
            if (rc == 0) {
                sinomo_inotify_emit_path(path, IN_DELETE | IN_DELETE_SELF | IN_ISDIR);
            }
            return rc;
        }
        int rc = unlink(path);
        if (rc == 0) {
            sinomo_inotify_emit_path(path, IN_DELETE | IN_DELETE_SELF);
        }
        return rc;
    }
    struct sinomo_unlinkat_data data = {
        .path = path,
        .flags = flags,
    };
    int rc = sinomo_with_dirfd_cwd(dirfd, sinomo_unlinkat_operation, &data);
    if (rc == 0) {
        sinomo_inotify_emit_path(path, (flags & AT_REMOVEDIR) != 0 ? (IN_DELETE | IN_DELETE_SELF | IN_ISDIR) : (IN_DELETE | IN_DELETE_SELF));
    }
    return rc;
}

struct sinomo_readlinkat_data {
    const char *path;
    char *buf;
    size_t bufsiz;
};

static int sinomo_readlinkat_operation(void *raw) {
    struct sinomo_readlinkat_data *data = raw;
    return (int)readlink(data->path, data->buf, data->bufsiz);
}

ssize_t __wrap_readlinkat(int dirfd, const char *path, char *buf, size_t bufsiz) {
    if (path == NULL || buf == NULL) {
        errno = EINVAL;
        return -1;
    }
    if (path[0] == '/') {
        return readlink(path, buf, bufsiz);
    }
    struct sinomo_readlinkat_data data = {
        .path = path,
        .buf = buf,
        .bufsiz = bufsiz,
    };
    return (ssize_t)sinomo_with_dirfd_cwd(dirfd, sinomo_readlinkat_operation, &data);
}

struct sinomo_symlinkat_data {
    const char *target;
    const char *linkpath;
};

static int sinomo_symlinkat_operation(void *raw) {
    struct sinomo_symlinkat_data *data = raw;
    return symlink(data->target, data->linkpath);
}

int __wrap_symlinkat(const char *target, int newdirfd, const char *linkpath) {
    if (target == NULL || linkpath == NULL) {
        errno = EINVAL;
        return -1;
    }
    if (linkpath[0] == '/') {
        int rc = symlink(target, linkpath);
        if (rc == 0) {
            sinomo_inotify_emit_path(linkpath, IN_CREATE);
        }
        return rc;
    }
    struct sinomo_symlinkat_data data = {
        .target = target,
        .linkpath = linkpath,
    };
    int rc = sinomo_with_dirfd_cwd(newdirfd, sinomo_symlinkat_operation, &data);
    if (rc == 0) {
        sinomo_inotify_emit_path(linkpath, IN_CREATE);
    }
    return rc;
}

struct sinomo_linkat_data {
    const char *oldpath;
    const char *newpath;
    int flags;
};

static int sinomo_linkat_operation(void *raw) {
    struct sinomo_linkat_data *data = raw;
    char resolved_oldpath[PATH_MAX];
    if ((data->flags & AT_SYMLINK_FOLLOW) != 0) {
        if (realpath(data->oldpath, resolved_oldpath) == NULL) {
            return -1;
        }
        return link(resolved_oldpath, data->newpath);
    }
    return link(data->oldpath, data->newpath);
}

int __wrap_linkat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags) {
    if (oldpath == NULL || newpath == NULL) {
        errno = EINVAL;
        return -1;
    }
    if ((flags & ~AT_SYMLINK_FOLLOW) != 0) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_linkat_data data = {
        .oldpath = oldpath,
        .newpath = newpath,
        .flags = flags,
    };
    if (oldpath[0] == '/' && newpath[0] == '/') {
        int rc = sinomo_linkat_operation(&data);
        if (rc == 0) {
            sinomo_inotify_emit_path(newpath, IN_CREATE);
        }
        return rc;
    }
    if (oldpath[0] == '/') {
        int rc = sinomo_with_dirfd_cwd(newdirfd, sinomo_linkat_operation, &data);
        if (rc == 0) {
            sinomo_inotify_emit_path(newpath, IN_CREATE);
        }
        return rc;
    }
    if (newpath[0] == '/') {
        int rc = sinomo_with_dirfd_cwd(olddirfd, sinomo_linkat_operation, &data);
        if (rc == 0) {
            sinomo_inotify_emit_path(newpath, IN_CREATE);
        }
        return rc;
    }
    if (!sinomo_same_or_at_fdcwd(olddirfd, newdirfd)) {
        errno = EXDEV;
        return -1;
    }
    int dirfd = olddirfd == AT_FDCWD ? newdirfd : olddirfd;
    int rc = sinomo_with_dirfd_cwd(dirfd, sinomo_linkat_operation, &data);
    if (rc == 0) {
        sinomo_inotify_emit_path(newpath, IN_CREATE);
    }
    return rc;
}

struct sinomo_renameat_data {
    const char *oldpath;
    const char *newpath;
};

static int sinomo_renameat_operation(void *raw) {
    struct sinomo_renameat_data *data = raw;
    return rename(data->oldpath, data->newpath);
}

int __wrap_renameat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath) {
    if (oldpath == NULL || newpath == NULL) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_renameat_data data = {
        .oldpath = oldpath,
        .newpath = newpath,
    };
    if (oldpath[0] == '/' && newpath[0] == '/') {
        int rc = sinomo_renameat_operation(&data);
        if (rc == 0) {
            sinomo_inotify_emit_path(oldpath, IN_MOVED_FROM | IN_MOVE_SELF);
            sinomo_inotify_emit_path(newpath, IN_MOVED_TO);
        }
        return rc;
    }
    if (oldpath[0] == '/') {
        int rc = sinomo_with_dirfd_cwd(newdirfd, sinomo_renameat_operation, &data);
        if (rc == 0) {
            sinomo_inotify_emit_path(oldpath, IN_MOVED_FROM | IN_MOVE_SELF);
            sinomo_inotify_emit_path(newpath, IN_MOVED_TO);
        }
        return rc;
    }
    if (newpath[0] == '/') {
        int rc = sinomo_with_dirfd_cwd(olddirfd, sinomo_renameat_operation, &data);
        if (rc == 0) {
            sinomo_inotify_emit_path(oldpath, IN_MOVED_FROM | IN_MOVE_SELF);
            sinomo_inotify_emit_path(newpath, IN_MOVED_TO);
        }
        return rc;
    }
    if (!sinomo_same_or_at_fdcwd(olddirfd, newdirfd)) {
        errno = EXDEV;
        return -1;
    }
    int dirfd = olddirfd == AT_FDCWD ? newdirfd : olddirfd;
    int rc = sinomo_with_dirfd_cwd(dirfd, sinomo_renameat_operation, &data);
    if (rc == 0) {
        sinomo_inotify_emit_path(oldpath, IN_MOVED_FROM | IN_MOVE_SELF);
        sinomo_inotify_emit_path(newpath, IN_MOVED_TO);
    }
    return rc;
}

int __wrap_mkdir(const char *path, mode_t mode) {
    return __wrap_mkdirat(AT_FDCWD, path, mode);
}

int __wrap_rmdir(const char *path) {
    return __wrap_unlinkat(AT_FDCWD, path, AT_REMOVEDIR);
}

int __wrap_unlink(const char *path) {
    return __wrap_unlinkat(AT_FDCWD, path, 0);
}

ssize_t __wrap_readlink(const char *path, char *buf, size_t bufsiz) {
    return __wrap_readlinkat(AT_FDCWD, path, buf, bufsiz);
}

int __wrap_symlink(const char *target, const char *linkpath) {
    return __wrap_symlinkat(target, AT_FDCWD, linkpath);
}

int __wrap_link(const char *oldpath, const char *newpath) {
    return __wrap_linkat(AT_FDCWD, oldpath, AT_FDCWD, newpath, 0);
}

int __wrap_rename(const char *oldpath, const char *newpath) {
    return __wrap_renameat(AT_FDCWD, oldpath, AT_FDCWD, newpath);
}

char *__wrap_getcwd(char *buf, size_t size) {
    size_t needed = size;
    int allocated = 0;
    if (buf == NULL) {
        needed = 4096;
        buf = malloc(needed);
        if (buf == NULL) {
            errno = ENOMEM;
            return NULL;
        }
        allocated = 1;
    }
    __wasi_pointersize_t path_len = (__wasi_pointersize_t)needed;
    __wasi_errno_t rc = __wasi_getcwd((uint8_t *)buf, &path_len);
    if (rc != 0) {
        if (allocated) {
            free(buf);
        }
        errno = (int)rc;
        return NULL;
    }
    return buf;
}

char *__wrap_get_current_dir_name(void) {
    return __wrap_getcwd(NULL, 0);
}

char *__wrap_getwd(char *buf) {
    if (buf == NULL) {
        errno = EFAULT;
        return NULL;
    }
    return __wrap_getcwd(buf, 4096);
}

static int sinomo_dirname_copy(const char *path, char *dir, size_t dir_len) {
    const char *slash = strrchr(path, '/');
    if (slash == NULL) {
        return snprintf(dir, dir_len, ".") > 0 ? 0 : -1;
    }
    if (slash == path) {
        return snprintf(dir, dir_len, "/") > 0 ? 0 : -1;
    }
    size_t len = (size_t)(slash - path);
    if (len >= dir_len) {
        errno = ENAMETOOLONG;
        return -1;
    }
    memcpy(dir, path, len);
    dir[len] = 0;
    return 0;
}

static const char *sinomo_basename_ptr(const char *path) {
    const char *slash = strrchr(path, '/');
    return slash == NULL ? path : slash + 1;
}

static char *sinomo_realpath_alloc(const char *path) {
    if (path == NULL) {
        errno = EINVAL;
        return NULL;
    }
    if (path[0] == 0) {
        errno = ENOENT;
        return NULL;
    }

    char current[4096];
    if (path[0] == '/') {
        if (snprintf(current, sizeof(current), "%s", path) >= (int)sizeof(current)) {
            errno = ENAMETOOLONG;
            return NULL;
        }
    } else {
        char *cwd = __wrap_getcwd(NULL, 0);
        if (cwd == NULL) {
            return NULL;
        }
        int written = snprintf(current, sizeof(current), "%s/%s", cwd, path);
        free(cwd);
        if (written < 0 || (size_t)written >= sizeof(current)) {
            errno = ENAMETOOLONG;
            return NULL;
        }
    }

    for (int depth = 0; depth < 40; depth++) {
        struct stat st;
        if (lstat(current, &st) != 0) {
            return NULL;
        }
        if (S_ISLNK(st.st_mode)) {
            char target[4096];
            ssize_t target_len = readlink(current, target, sizeof(target) - 1);
            if (target_len < 0) {
                return NULL;
            }
            target[target_len] = 0;
            if (target[0] == '/') {
                if (snprintf(current, sizeof(current), "%s", target) >= (int)sizeof(current)) {
                    errno = ENAMETOOLONG;
                    return NULL;
                }
            } else {
                char dir[4096];
                if (sinomo_dirname_copy(current, dir, sizeof(dir)) != 0) {
                    return NULL;
                }
                int written = snprintf(current, sizeof(current), "%s/%s", dir, target);
                if (written < 0 || (size_t)written >= sizeof(current)) {
                    errno = ENAMETOOLONG;
                    return NULL;
                }
            }
            continue;
        }
        if (S_ISDIR(st.st_mode)) {
            int saved_cwd = open(".", O_RDONLY | O_DIRECTORY);
            if (saved_cwd < 0) {
                return NULL;
            }
            if (__wrap_chdir(current) != 0) {
                int saved_errno = errno;
                close(saved_cwd);
                errno = saved_errno;
                return NULL;
            }
            char *resolved = __wrap_getcwd(NULL, 0);
            int saved_errno = errno;
            fchdir(saved_cwd);
            close(saved_cwd);
            errno = saved_errno;
            return resolved;
        }

        char dir[4096];
        if (sinomo_dirname_copy(current, dir, sizeof(dir)) != 0) {
            return NULL;
        }
        int saved_cwd = open(".", O_RDONLY | O_DIRECTORY);
        if (saved_cwd < 0) {
            return NULL;
        }
        if (__wrap_chdir(dir) != 0) {
            int saved_errno = errno;
            close(saved_cwd);
            errno = saved_errno;
            return NULL;
        }
        char *resolved_dir = __wrap_getcwd(NULL, 0);
        int saved_errno = errno;
        fchdir(saved_cwd);
        close(saved_cwd);
        errno = saved_errno;
        if (resolved_dir == NULL) {
            return NULL;
        }
        const char *base = sinomo_basename_ptr(current);
        size_t len = strlen(resolved_dir) + 1 + strlen(base) + 1;
        char *resolved = malloc(len);
        if (resolved == NULL) {
            free(resolved_dir);
            errno = ENOMEM;
            return NULL;
        }
        if (strcmp(resolved_dir, "/") == 0) {
            snprintf(resolved, len, "/%s", base);
        } else {
            snprintf(resolved, len, "%s/%s", resolved_dir, base);
        }
        free(resolved_dir);
        return resolved;
    }
    errno = ELOOP;
    return NULL;
}

char *__wrap_realpath(const char *path, char *resolved_path) {
    char *resolved = sinomo_realpath_alloc(path);
    if (resolved == NULL) {
        return NULL;
    }
    if (resolved_path == NULL) {
        return resolved;
    }
    snprintf(resolved_path, 4096, "%s", resolved);
    free(resolved);
    return resolved_path;
}

int __wrap_chdir(const char *path) {
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    __wasi_errno_t rc = __wasi_chdir(path);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    return 0;
}

int __wrap_chroot(const char *path) {
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    struct stat st;
    if (stat(path, &st) != 0) {
        return -1;
    }
    if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        return -1;
    }
    return 0;
}

static int sinomo_identity_initialized = 0;
static uid_t sinomo_ruid = 0;
static uid_t sinomo_euid = 0;
static uid_t sinomo_suid = 0;
static uid_t sinomo_fsuid = 0;
static gid_t sinomo_rgid = 0;
static gid_t sinomo_egid = 0;
static gid_t sinomo_sgid = 0;
static gid_t sinomo_fsgid = 0;
static gid_t sinomo_groups[128];
static size_t sinomo_group_count = 1;
static char sinomo_lsm_current_attr[128] = "unconfined";

#define SINOMO_VMA_NAME_MAX 80
#define SINOMO_VMA_NAME_SLOTS 64
struct sinomo_vma_name {
    void *addr;
    size_t len;
    char name[SINOMO_VMA_NAME_MAX];
};
static struct sinomo_vma_name sinomo_vma_names[SINOMO_VMA_NAME_SLOTS];

static int sinomo_init_identity(void) {
    if (sinomo_identity_initialized) {
        return 0;
    }
    unsigned int ruid = 0;
    unsigned int euid = 0;
    unsigned int suid = 0;
    int rc = sinomo_proc_getresuid(&ruid, &euid, &suid);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    unsigned int rgid = 0;
    unsigned int egid = 0;
    unsigned int sgid = 0;
    rc = sinomo_proc_getresgid(&rgid, &egid, &sgid);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    sinomo_ruid = (uid_t)ruid;
    sinomo_euid = (uid_t)euid;
    sinomo_suid = (uid_t)suid;
    sinomo_fsuid = (uid_t)euid;
    sinomo_rgid = (gid_t)rgid;
    sinomo_egid = (gid_t)egid;
    sinomo_sgid = (gid_t)sgid;
    sinomo_fsgid = (gid_t)egid;

    unsigned int group_count = 0;
    rc = sinomo_proc_getgroups(NULL, 0, &group_count);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    if (group_count > sizeof(sinomo_groups) / sizeof(sinomo_groups[0])) {
        errno = EINVAL;
        return -1;
    }
    if (group_count > 0) {
        unsigned int runtime_groups[sizeof(sinomo_groups) / sizeof(sinomo_groups[0])];
        unsigned int written = 0;
        rc = sinomo_proc_getgroups(runtime_groups, group_count, &written);
        if (rc != 0) {
            errno = rc;
            return -1;
        }
        if (written != group_count) {
            errno = EINVAL;
            return -1;
        }
        for (unsigned int index = 0; index < group_count; index++) {
            sinomo_groups[index] = (gid_t)runtime_groups[index];
        }
        sinomo_group_count = group_count;
    } else {
        sinomo_group_count = 0;
    }
    sinomo_identity_initialized = 1;
    return 0;
}

static int sinomo_commit_identity(void) {
    unsigned int groups[sizeof(sinomo_groups) / sizeof(sinomo_groups[0])];
    for (size_t index = 0; index < sinomo_group_count; index++) {
        groups[index] = sinomo_gid(sinomo_groups[index]);
    }
    int rc = sinomo_proc_setresuid(
        sinomo_uid(sinomo_ruid),
        sinomo_uid(sinomo_euid),
        sinomo_uid(sinomo_suid));
    if (rc == 0) {
        rc = sinomo_proc_setresgid(
            sinomo_gid(sinomo_rgid),
            sinomo_gid(sinomo_egid),
            sinomo_gid(sinomo_sgid));
    }
    if (rc == 0) {
        rc = sinomo_proc_setgroups(groups, (unsigned int)sinomo_group_count);
    }
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    return 0;
}

int getgroups(int size, gid_t list[]) {
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    size_t count = sinomo_group_count;
    if (size == 0) {
        return (int)count;
    }
    if (size < 0 || (size_t)size < count) {
        errno = EINVAL;
        return -1;
    }
    if (list == NULL) {
        errno = EFAULT;
        return -1;
    }
    for (size_t index = 0; index < count; index++) {
        list[index] = sinomo_groups[index];
    }
    return (int)count;
}

uid_t __wrap_getuid(void) {
    if (sinomo_init_identity() == 0) {
        return sinomo_ruid;
    }
    return (uid_t)-1;
}

uid_t __wrap_geteuid(void) {
    if (sinomo_init_identity() == 0) {
        return sinomo_euid;
    }
    return (uid_t)-1;
}

gid_t __wrap_getgid(void) {
    if (sinomo_init_identity() == 0) {
        return sinomo_rgid;
    }
    return (gid_t)-1;
}

gid_t __wrap_getegid(void) {
    if (sinomo_init_identity() == 0) {
        return sinomo_egid;
    }
    return (gid_t)-1;
}

static struct passwd sinomo_passwd = {
    .pw_name = "sinomo",
    .pw_passwd = "x",
    .pw_uid = 0,
    .pw_gid = 0,
    .pw_gecos = "Sinomo Sandbox",
    .pw_dir = "/home/sinomo",
    .pw_shell = "/bin/sh",
};
static int sinomo_passwd_iterated = 0;

static void sinomo_refresh_passwd(void) {
    uid_t uid = __wrap_getuid();
    gid_t gid = __wrap_getgid();
    if (uid != (uid_t)-1) {
        sinomo_passwd.pw_uid = uid;
    }
    if (gid != (gid_t)-1) {
        sinomo_passwd.pw_gid = gid;
    }
}

static int sinomo_copy_passwd(struct passwd *pwd, char *buf, size_t buflen, struct passwd **result) {
    const char *fields[] = {
        "sinomo",
        "x",
        "Sinomo Sandbox",
        "/home/sinomo",
        "/bin/sh",
    };
    size_t needed = 0;
    for (size_t index = 0; index < sizeof(fields) / sizeof(fields[0]); index++) {
        needed += strlen(fields[index]) + 1;
    }
    if (buflen < needed) {
        *result = NULL;
        return ERANGE;
    }
    char *cursor = buf;
    char *copies[5];
    for (size_t index = 0; index < sizeof(fields) / sizeof(fields[0]); index++) {
        size_t len = strlen(fields[index]) + 1;
        memcpy(cursor, fields[index], len);
        copies[index] = cursor;
        cursor += len;
    }
    sinomo_refresh_passwd();
    pwd->pw_name = copies[0];
    pwd->pw_passwd = copies[1];
    pwd->pw_uid = sinomo_passwd.pw_uid;
    pwd->pw_gid = sinomo_passwd.pw_gid;
    pwd->pw_gecos = copies[2];
    pwd->pw_dir = copies[3];
    pwd->pw_shell = copies[4];
    *result = pwd;
    return 0;
}

struct passwd *__wrap_getpwuid(uid_t uid) {
    sinomo_refresh_passwd();
    if (uid == sinomo_passwd.pw_uid) {
        return &sinomo_passwd;
    }
    errno = ENOENT;
    return NULL;
}

struct passwd *__wrap_getpwnam(const char *name) {
    if (name != NULL && strcmp(name, "sinomo") == 0) {
        sinomo_refresh_passwd();
        return &sinomo_passwd;
    }
    errno = ENOENT;
    return NULL;
}

int __wrap_getpwuid_r(uid_t uid, struct passwd *pwd, char *buf, size_t buflen, struct passwd **result) {
    if (pwd == NULL || buf == NULL || result == NULL) {
        return EINVAL;
    }
    sinomo_refresh_passwd();
    if (uid != sinomo_passwd.pw_uid) {
        *result = NULL;
        return 0;
    }
    return sinomo_copy_passwd(pwd, buf, buflen, result);
}

int __wrap_getpwnam_r(const char *name, struct passwd *pwd, char *buf, size_t buflen, struct passwd **result) {
    if (name == NULL || pwd == NULL || buf == NULL || result == NULL) {
        return EINVAL;
    }
    if (strcmp(name, "sinomo") != 0) {
        *result = NULL;
        return 0;
    }
    return sinomo_copy_passwd(pwd, buf, buflen, result);
}

void __wrap_setpwent(void) {
    sinomo_passwd_iterated = 0;
}

void __wrap_endpwent(void) {
    sinomo_passwd_iterated = 1;
}

struct passwd *__wrap_getpwent(void) {
    if (sinomo_passwd_iterated) {
        return NULL;
    }
    sinomo_passwd_iterated = 1;
    sinomo_refresh_passwd();
    return &sinomo_passwd;
}

static char *sinomo_group_members[] = {"sinomo", NULL};
static struct group sinomo_group = {
    .gr_name = "sinomo",
    .gr_passwd = "x",
    .gr_gid = 0,
    .gr_mem = sinomo_group_members,
};
static int sinomo_group_iterated = 0;

static void sinomo_refresh_group(void) {
    gid_t gid = __wrap_getgid();
    if (gid != (gid_t)-1) {
        sinomo_group.gr_gid = gid;
    }
}

static int sinomo_copy_group(struct group *grp, char *buf, size_t buflen, struct group **result) {
    const char *name = "sinomo";
    const char *passwd = "x";
    size_t member_slots = 2 * sizeof(char *);
    size_t aligned_slots = (member_slots + sizeof(void *) - 1) & ~(sizeof(void *) - 1);
    size_t needed = aligned_slots + strlen(name) + 1 + strlen(passwd) + 1 + strlen(name) + 1;
    if (buflen < needed) {
        *result = NULL;
        return ERANGE;
    }
    char **members = (char **)buf;
    char *cursor = buf + aligned_slots;
    grp->gr_name = cursor;
    strcpy(cursor, name);
    cursor += strlen(name) + 1;
    grp->gr_passwd = cursor;
    strcpy(cursor, passwd);
    cursor += strlen(passwd) + 1;
    members[0] = cursor;
    strcpy(cursor, name);
    cursor += strlen(name) + 1;
    (void)cursor;
    members[1] = NULL;
    sinomo_refresh_group();
    grp->gr_gid = sinomo_group.gr_gid;
    grp->gr_mem = members;
    *result = grp;
    return 0;
}

struct group *__wrap_getgrgid(gid_t gid) {
    sinomo_refresh_group();
    if (gid == sinomo_group.gr_gid) {
        return &sinomo_group;
    }
    errno = ENOENT;
    return NULL;
}

struct group *__wrap_getgrnam(const char *name) {
    if (name != NULL && strcmp(name, "sinomo") == 0) {
        sinomo_refresh_group();
        return &sinomo_group;
    }
    errno = ENOENT;
    return NULL;
}

int __wrap_getgrgid_r(gid_t gid, struct group *grp, char *buf, size_t buflen, struct group **result) {
    if (grp == NULL || buf == NULL || result == NULL) {
        return EINVAL;
    }
    sinomo_refresh_group();
    if (gid != sinomo_group.gr_gid) {
        *result = NULL;
        return 0;
    }
    return sinomo_copy_group(grp, buf, buflen, result);
}

int __wrap_getgrnam_r(const char *name, struct group *grp, char *buf, size_t buflen, struct group **result) {
    if (name == NULL || grp == NULL || buf == NULL || result == NULL) {
        return EINVAL;
    }
    if (strcmp(name, "sinomo") != 0) {
        *result = NULL;
        return 0;
    }
    return sinomo_copy_group(grp, buf, buflen, result);
}

void __wrap_setgrent(void) {
    sinomo_group_iterated = 0;
}

void __wrap_endgrent(void) {
    sinomo_group_iterated = 1;
}

struct group *__wrap_getgrent(void) {
    if (sinomo_group_iterated) {
        return NULL;
    }
    sinomo_group_iterated = 1;
    sinomo_refresh_group();
    return &sinomo_group;
}

int __wrap_getgrouplist(const char *user, gid_t basegid, gid_t *groups, int *ngroups) {
    if (user == NULL || groups == NULL || ngroups == NULL) {
        errno = EINVAL;
        return -1;
    }
    if (strcmp(user, "sinomo") != 0) {
        *ngroups = 0;
        return 0;
    }
    gid_t gid = __wrap_getgid();
    if (gid == (gid_t)-1) {
        gid = basegid;
    }
    if (*ngroups < 1) {
        *ngroups = 1;
        return -1;
    }
    groups[0] = gid;
    *ngroups = 1;
    return 0;
}

static struct spwd sinomo_spwd = {
    .sp_namp = "sinomo",
    .sp_pwdp = "!",
    .sp_lstchg = 0,
    .sp_min = 0,
    .sp_max = 99999,
    .sp_warn = 7,
    .sp_inact = -1,
    .sp_expire = -1,
    .sp_flag = 0,
};
static int sinomo_shadow_iterated = 0;

void __wrap_setspent(void) {
    sinomo_shadow_iterated = 0;
}

void __wrap_endspent(void) {
    sinomo_shadow_iterated = 1;
}

struct spwd *__wrap_getspent(void) {
    if (sinomo_shadow_iterated) {
        return NULL;
    }
    sinomo_shadow_iterated = 1;
    return &sinomo_spwd;
}

struct spwd *__wrap_getspnam(const char *name) {
    if (name != NULL && strcmp(name, "sinomo") == 0) {
        return &sinomo_spwd;
    }
    errno = ENOENT;
    return NULL;
}

static int sinomo_copy_shadow(struct spwd *sp, char *buf, size_t buflen, struct spwd **result) {
    const char *name = "sinomo";
    const char *passwd = "!";
    size_t needed = strlen(name) + 1 + strlen(passwd) + 1;
    if (buflen < needed) {
        *result = NULL;
        return ERANGE;
    }
    char *cursor = buf;
    sp->sp_namp = cursor;
    strcpy(cursor, name);
    cursor += strlen(name) + 1;
    sp->sp_pwdp = cursor;
    strcpy(cursor, passwd);
    sp->sp_lstchg = sinomo_spwd.sp_lstchg;
    sp->sp_min = sinomo_spwd.sp_min;
    sp->sp_max = sinomo_spwd.sp_max;
    sp->sp_warn = sinomo_spwd.sp_warn;
    sp->sp_inact = sinomo_spwd.sp_inact;
    sp->sp_expire = sinomo_spwd.sp_expire;
    sp->sp_flag = sinomo_spwd.sp_flag;
    *result = sp;
    return 0;
}

int __wrap_getspnam_r(const char *name, struct spwd *spbuf, char *buf, size_t buflen, struct spwd **spbufp) {
    if (name == NULL || spbuf == NULL || buf == NULL || spbufp == NULL) {
        return EINVAL;
    }
    if (strcmp(name, "sinomo") != 0) {
        *spbufp = NULL;
        return 0;
    }
    return sinomo_copy_shadow(spbuf, buf, buflen, spbufp);
}

static long sinomo_shadow_parse_long(const char *field, long fallback) {
    if (field == NULL || field[0] == 0) {
        return fallback;
    }
    char *end = NULL;
    errno = 0;
    long value = strtol(field, &end, 10);
    if (errno != 0 || end == field || *end != 0) {
        errno = 0;
        return fallback;
    }
    return value;
}

struct spwd *__wrap_sgetspent(const char *line) {
    static struct spwd parsed;
    static char storage[512];
    if (line == NULL) {
        errno = EFAULT;
        return NULL;
    }
    snprintf(storage, sizeof(storage), "%s", line);
    char *fields[9] = {0};
    char *cursor = storage;
    for (size_t index = 0; index < 9; index++) {
        fields[index] = cursor;
        char *sep = strchr(cursor, ':');
        if (sep == NULL) {
            if (index < 8) {
                errno = EINVAL;
                return NULL;
            }
            break;
        }
        *sep = 0;
        cursor = sep + 1;
    }
    parsed.sp_namp = fields[0];
    parsed.sp_pwdp = fields[1] == NULL ? "" : fields[1];
    parsed.sp_lstchg = sinomo_shadow_parse_long(fields[2], -1);
    parsed.sp_min = sinomo_shadow_parse_long(fields[3], -1);
    parsed.sp_max = sinomo_shadow_parse_long(fields[4], -1);
    parsed.sp_warn = sinomo_shadow_parse_long(fields[5], -1);
    parsed.sp_inact = sinomo_shadow_parse_long(fields[6], -1);
    parsed.sp_expire = sinomo_shadow_parse_long(fields[7], -1);
    parsed.sp_flag = (unsigned long)sinomo_shadow_parse_long(fields[8], 0);
    return &parsed;
}

struct spwd *__wrap_fgetspent(FILE *stream) {
    static char line[512];
    if (stream == NULL) {
        errno = EFAULT;
        return NULL;
    }
    if (fgets(line, sizeof(line), stream) == NULL) {
        return NULL;
    }
    line[strcspn(line, "\r\n")] = 0;
    return __wrap_sgetspent(line);
}

int __wrap_putspent(const struct spwd *sp, FILE *stream) {
    if (sp == NULL || stream == NULL) {
        errno = EFAULT;
        return -1;
    }
    return fprintf(
        stream,
        "%s:%s:%ld:%ld:%ld:%ld:%ld:%ld:%lu\n",
        sp->sp_namp ? sp->sp_namp : "",
        sp->sp_pwdp ? sp->sp_pwdp : "",
        sp->sp_lstchg,
        sp->sp_min,
        sp->sp_max,
        sp->sp_warn,
        sp->sp_inact,
        sp->sp_expire,
        sp->sp_flag
    ) < 0 ? -1 : 0;
}

int __wrap_lckpwdf(void) {
    return 0;
}

int __wrap_ulckpwdf(void) {
    return 0;
}

static unsigned int sinomo_temp_counter = 0;

static int sinomo_temp_path(char *buf, size_t buflen, const char *dir, const char *prefix) {
    const char *safe_dir = (dir != NULL && dir[0] != 0) ? dir : "/tmp";
    const char *safe_prefix = (prefix != NULL && prefix[0] != 0) ? prefix : "st";
    if (sinomo_mkdir_if_missing(safe_dir) != 0) {
        return -1;
    }
    for (unsigned int attempt = 0; attempt < 128; attempt++) {
        unsigned int value = sinomo_temp_counter++ ^ (unsigned int)__wrap_getpid() ^ attempt;
        int written = snprintf(buf, buflen, "%s/%.2s%08x", safe_dir, safe_prefix, value);
        if (written < 0 || (size_t)written >= buflen) {
            errno = ENAMETOOLONG;
            return -1;
        }
        if (access(buf, F_OK) != 0) {
            if (errno == ENOENT) {
                errno = 0;
                return 0;
            }
            return -1;
        }
    }
    errno = EEXIST;
    return -1;
}

char *__wrap_tmpnam(char *s) {
    static char static_path[L_tmpnam];
    char *out = s == NULL ? static_path : s;
    if (sinomo_temp_path(out, L_tmpnam, "/tmp", "tn") != 0) {
        return NULL;
    }
    return out;
}

char *__wrap_tmpnam_r(char *s) {
    if (s == NULL) {
        errno = EFAULT;
        return NULL;
    }
    return __wrap_tmpnam(s);
}

char *__wrap_tempnam(const char *dir, const char *prefix) {
    char path[4096];
    if (sinomo_temp_path(path, sizeof(path), dir, prefix) != 0) {
        return NULL;
    }
    char *out = malloc(strlen(path) + 1);
    if (out == NULL) {
        errno = ENOMEM;
        return NULL;
    }
    strcpy(out, path);
    return out;
}

FILE *__wrap_tmpfile(void) {
    char path[4096];
    for (unsigned int attempt = 0; attempt < 128; attempt++) {
        if (sinomo_temp_path(path, sizeof(path), "/tmp", "tf") != 0) {
            return NULL;
        }
        int fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0600);
        if (fd < 0) {
            if (errno == EEXIST) {
                continue;
            }
            return NULL;
        }
        if (unlink(path) != 0) {
            int saved_errno = errno;
            close(fd);
            errno = saved_errno;
            return NULL;
        }
        FILE *file = fdopen(fd, "w+");
        if (file == NULL) {
            int saved_errno = errno;
            close(fd);
            errno = saved_errno;
            return NULL;
        }
        return file;
    }
    errno = EEXIST;
    return NULL;
}

static int sinomo_template_suffix(char *template, int suffixlen, char **slots) {
    if (template == NULL || slots == NULL || suffixlen < 0) {
        errno = EINVAL;
        return -1;
    }
    size_t len = strlen(template);
    if (len < (size_t)suffixlen + 6) {
        errno = EINVAL;
        return -1;
    }
    char *candidate = template + len - (size_t)suffixlen - 6;
    if (memcmp(candidate, "XXXXXX", 6) != 0) {
        errno = EINVAL;
        return -1;
    }
    *slots = candidate;
    return 0;
}

static void sinomo_fill_template_slots(char *slots, unsigned int attempt) {
    static const char alphabet[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    unsigned int value = sinomo_temp_counter++ ^ (unsigned int)__wrap_getpid() ^ (attempt * 1103515245U);
    for (size_t index = 0; index < 6; index++) {
        slots[index] = alphabet[value % (sizeof(alphabet) - 1)];
        value = value / (sizeof(alphabet) - 1) + 17U;
    }
}

int __wrap_mkostemps(char *template, int suffixlen, int flags) {
    char *slots = NULL;
    if (sinomo_template_suffix(template, suffixlen, &slots) != 0) {
        return -1;
    }
    int supported = 0;
#ifdef O_CLOEXEC
    supported |= O_CLOEXEC;
#endif
#ifdef O_APPEND
    supported |= O_APPEND;
#endif
#ifdef O_SYNC
    supported |= O_SYNC;
#endif
#ifdef O_DSYNC
    supported |= O_DSYNC;
#endif
#ifdef O_RSYNC
    supported |= O_RSYNC;
#endif
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    for (unsigned int attempt = 0; attempt < 256; attempt++) {
        sinomo_fill_template_slots(slots, attempt);
        int fd = open(template, O_RDWR | O_CREAT | O_EXCL | flags, 0600);
        if (fd >= 0) {
            return fd;
        }
        if (errno != EEXIST) {
            return -1;
        }
    }
    errno = EEXIST;
    return -1;
}

int __wrap_mkostemp(char *template, int flags) {
    return __wrap_mkostemps(template, 0, flags);
}

int __wrap_mkstemps(char *template, int suffixlen) {
    return __wrap_mkostemps(template, suffixlen, 0);
}

int __wrap_mkstemp(char *template) {
    return __wrap_mkostemps(template, 0, 0);
}

char *__wrap_mkdtemp(char *template) {
    char *slots = NULL;
    if (sinomo_template_suffix(template, 0, &slots) != 0) {
        return NULL;
    }
    for (unsigned int attempt = 0; attempt < 256; attempt++) {
        sinomo_fill_template_slots(slots, attempt);
        if (mkdir(template, 0700) == 0) {
            return template;
        }
        if (errno != EEXIST) {
            return NULL;
        }
    }
    errno = EEXIST;
    return NULL;
}

char *__wrap_mktemp(char *template) {
    char *slots = NULL;
    if (sinomo_template_suffix(template, 0, &slots) != 0) {
        return NULL;
    }
    for (unsigned int attempt = 0; attempt < 256; attempt++) {
        sinomo_fill_template_slots(slots, attempt);
        if (access(template, F_OK) != 0) {
            if (errno == ENOENT) {
                errno = 0;
                return template;
            }
            return NULL;
        }
    }
    errno = EEXIST;
    return NULL;
}

int __wrap_setuid(uid_t uid) {
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    sinomo_ruid = uid;
    sinomo_euid = uid;
    sinomo_suid = uid;
    sinomo_fsuid = uid;
    return sinomo_commit_identity();
}

int __wrap_seteuid(uid_t uid) {
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    sinomo_euid = uid;
    sinomo_fsuid = uid;
    return sinomo_commit_identity();
}

int __wrap_setgid(gid_t gid) {
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    sinomo_rgid = gid;
    sinomo_egid = gid;
    sinomo_sgid = gid;
    sinomo_fsgid = gid;
    if (sinomo_group_count == 0) {
        sinomo_group_count = 1;
    }
    sinomo_groups[0] = gid;
    return sinomo_commit_identity();
}

int __wrap_setegid(gid_t gid) {
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    sinomo_egid = gid;
    sinomo_fsgid = gid;
    return sinomo_commit_identity();
}

int getresuid(uid_t *ruid, uid_t *euid, uid_t *suid) {
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    if (ruid != NULL) {
        *ruid = sinomo_ruid;
    }
    if (euid != NULL) {
        *euid = sinomo_euid;
    }
    if (suid != NULL) {
        *suid = sinomo_suid;
    }
    return 0;
}

int getresgid(gid_t *rgid, gid_t *egid, gid_t *sgid) {
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    if (rgid != NULL) {
        *rgid = sinomo_rgid;
    }
    if (egid != NULL) {
        *egid = sinomo_egid;
    }
    if (sgid != NULL) {
        *sgid = sinomo_sgid;
    }
    return 0;
}

int setreuid(uid_t ruid, uid_t euid) {
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    if (ruid != (uid_t)-1) {
        sinomo_ruid = ruid;
    }
    if (euid != (uid_t)-1) {
        sinomo_euid = euid;
        sinomo_suid = euid;
        sinomo_fsuid = euid;
    }
    return sinomo_commit_identity();
}

int setregid(gid_t rgid, gid_t egid) {
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    if (rgid != (gid_t)-1) {
        sinomo_rgid = rgid;
    }
    if (egid != (gid_t)-1) {
        sinomo_egid = egid;
        sinomo_sgid = egid;
        sinomo_fsgid = egid;
    }
    return sinomo_commit_identity();
}

int setresuid(uid_t ruid, uid_t euid, uid_t suid) {
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    if (ruid != (uid_t)-1) {
        sinomo_ruid = ruid;
    }
    if (euid != (uid_t)-1) {
        sinomo_euid = euid;
        sinomo_fsuid = euid;
    }
    if (suid != (uid_t)-1) {
        sinomo_suid = suid;
    }
    return sinomo_commit_identity();
}

int setresgid(gid_t rgid, gid_t egid, gid_t sgid) {
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    if (rgid != (gid_t)-1) {
        sinomo_rgid = rgid;
    }
    if (egid != (gid_t)-1) {
        sinomo_egid = egid;
        sinomo_fsgid = egid;
    }
    if (sgid != (gid_t)-1) {
        sinomo_sgid = sgid;
    }
    return sinomo_commit_identity();
}

int __wrap_setfsuid(uid_t uid) {
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    uid_t previous = sinomo_fsuid;
    if (uid != (uid_t)-1) {
        sinomo_fsuid = uid;
    }
    return (int)previous;
}

int setfsuid(uid_t uid) {
    return __wrap_setfsuid(uid);
}

int __wrap_setfsgid(gid_t gid) {
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    gid_t previous = sinomo_fsgid;
    if (gid != (gid_t)-1) {
        sinomo_fsgid = gid;
    }
    return (int)previous;
}

int setfsgid(gid_t gid) {
    return __wrap_setfsgid(gid);
}

int setgroups(size_t size, const gid_t *list) {
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    if (size > sizeof(sinomo_groups) / sizeof(sinomo_groups[0])) {
        errno = EINVAL;
        return -1;
    }
    if (size > 0) {
        if (list == NULL) {
            errno = EFAULT;
            return -1;
        }
        memcpy(sinomo_groups, list, size * sizeof(gid_t));
    }
    sinomo_group_count = size;
    return sinomo_commit_identity();
}

int initgroups(const char *user, gid_t group) {
    (void)user;
    if (sinomo_init_identity() != 0) {
        return -1;
    }
    sinomo_groups[0] = group;
    sinomo_group_count = 1;
    return sinomo_commit_identity();
}

#ifndef SYS_getpid
#define SYS_getpid 39
#endif
#ifndef SYS_close
#define SYS_close 3
#endif
#ifndef SYS_read
#define SYS_read 0
#endif
#ifndef SYS_write
#define SYS_write 1
#endif
#ifndef SYS_open
#define SYS_open 2
#endif
#ifndef SYS_fsync
#define SYS_fsync 74
#endif
#ifndef SYS_fdatasync
#define SYS_fdatasync 75
#endif
#ifndef SYS_io_setup
#define SYS_io_setup 206
#endif
#ifndef SYS_io_destroy
#define SYS_io_destroy 207
#endif
#ifndef SYS_io_getevents
#define SYS_io_getevents 208
#endif
#ifndef SYS_io_submit
#define SYS_io_submit 209
#endif
#ifndef SYS_io_cancel
#define SYS_io_cancel 210
#endif
#ifndef SYS_truncate
#define SYS_truncate 76
#endif
#ifndef SYS_ftruncate
#define SYS_ftruncate 77
#endif
#ifndef SYS_fcntl
#define SYS_fcntl 72
#endif
#ifndef SYS_stat
#define SYS_stat 4
#endif
#ifndef SYS_fstat
#define SYS_fstat 5
#endif
#ifndef SYS_lstat
#define SYS_lstat 6
#endif
#ifndef SYS_getdents
#define SYS_getdents 78
#endif
#ifndef SYS_access
#define SYS_access 21
#endif
#ifndef SYS_rename
#define SYS_rename 82
#endif
#ifndef SYS_mkdir
#define SYS_mkdir 83
#endif
#ifndef SYS_mknod
#define SYS_mknod 133
#endif
#ifndef SYS_rmdir
#define SYS_rmdir 84
#endif
#ifndef SYS_creat
#define SYS_creat 85
#endif
#ifndef SYS_link
#define SYS_link 86
#endif
#ifndef SYS_unlink
#define SYS_unlink 87
#endif
#ifndef SYS_symlink
#define SYS_symlink 88
#endif
#ifndef SYS_readlink
#define SYS_readlink 89
#endif
#ifndef SYS_chmod
#define SYS_chmod 90
#endif
#ifndef SYS_fchmod
#define SYS_fchmod 91
#endif
#ifndef SYS_chown
#define SYS_chown 92
#endif
#ifndef SYS_fchown
#define SYS_fchown 93
#endif
#ifndef SYS_lchown
#define SYS_lchown 94
#endif
#ifndef SYS_rt_sigaction
#define SYS_rt_sigaction 13
#endif
#ifndef SYS_rt_sigprocmask
#define SYS_rt_sigprocmask 14
#endif
#ifndef SYS_rt_sigreturn
#define SYS_rt_sigreturn 15
#endif
#ifndef SYS_sigaltstack
#define SYS_sigaltstack 131
#endif
#ifndef SYS_mmap
#define SYS_mmap 9
#endif
#ifndef SYS_mprotect
#define SYS_mprotect 10
#endif
#ifndef SYS_poll
#define SYS_poll 7
#endif
#ifndef SYS_lseek
#define SYS_lseek 8
#endif
#ifndef SYS_munmap
#define SYS_munmap 11
#endif
#ifndef SYS_brk
#define SYS_brk 12
#endif
#ifndef SYS_ioctl
#define SYS_ioctl 16
#endif
#ifndef SYS_msync
#define SYS_msync 26
#endif
#ifndef SYS_mremap
#define SYS_mremap 25
#endif
#ifndef SYS_mincore
#define SYS_mincore 27
#endif
#ifndef SYS_madvise
#define SYS_madvise 28
#endif
#ifndef SYS_sendfile
#define SYS_sendfile 40
#endif
#ifndef SYS_socket
#define SYS_socket 41
#endif
#ifndef SYS_connect
#define SYS_connect 42
#endif
#ifndef SYS_accept
#define SYS_accept 43
#endif
#ifndef SYS_sendto
#define SYS_sendto 44
#endif
#ifndef SYS_recvfrom
#define SYS_recvfrom 45
#endif
#ifndef SYS_sendmsg
#define SYS_sendmsg 46
#endif
#ifndef SYS_recvmsg
#define SYS_recvmsg 47
#endif
#ifndef SYS_shutdown
#define SYS_shutdown 48
#endif
#ifndef SYS_bind
#define SYS_bind 49
#endif
#ifndef SYS_listen
#define SYS_listen 50
#endif
#ifndef SYS_getsockname
#define SYS_getsockname 51
#endif
#ifndef SYS_getpeername
#define SYS_getpeername 52
#endif
#ifndef SYS_socketpair
#define SYS_socketpair 53
#endif
#ifndef SYS_setsockopt
#define SYS_setsockopt 54
#endif
#ifndef SYS_getsockopt
#define SYS_getsockopt 55
#endif
#ifndef SYS_execve
#define SYS_execve 59
#endif
#ifndef SYS_execveat
#define SYS_execveat 322
#endif
#ifndef SYS_exit
#define SYS_exit 60
#endif
#ifndef SYS_wait4
#define SYS_wait4 61
#endif
#ifndef SYS_clone
#define SYS_clone 56
#endif
#ifndef SYS_clone3
#define SYS_clone3 435
#endif
#ifndef SYS_pidfd_send_signal
#define SYS_pidfd_send_signal 424
#endif
#ifndef SYS_pidfd_open
#define SYS_pidfd_open 434
#endif
#ifndef SYS_pidfd_getfd
#define SYS_pidfd_getfd 438
#endif
#ifndef SYS_rt_sigpending
#define SYS_rt_sigpending 127
#endif
#ifndef SYS_rt_sigtimedwait
#define SYS_rt_sigtimedwait 128
#endif
#ifndef SYS_rt_sigqueueinfo
#define SYS_rt_sigqueueinfo 129
#endif
#ifndef SYS_rt_tgsigqueueinfo
#define SYS_rt_tgsigqueueinfo 297
#endif
#ifndef SYS_rt_sigsuspend
#define SYS_rt_sigsuspend 130
#endif
#ifndef SYS_sched_yield
#define SYS_sched_yield 24
#endif
#ifndef SYS_nanosleep
#define SYS_nanosleep 35
#endif
#ifndef SYS_getitimer
#define SYS_getitimer 36
#endif
#ifndef SYS_alarm
#define SYS_alarm 37
#endif
#ifndef SYS_setitimer
#define SYS_setitimer 38
#endif
#ifndef SYS_getcwd
#define SYS_getcwd 79
#endif
#ifndef SYS_chdir
#define SYS_chdir 80
#endif
#ifndef SYS_fchdir
#define SYS_fchdir 81
#endif
#ifndef SYS_chroot
#define SYS_chroot 161
#endif
#ifndef SYS_gettimeofday
#define SYS_gettimeofday 96
#endif
#ifndef SYS_umask
#define SYS_umask 95
#endif
#ifndef SYS_getrlimit
#define SYS_getrlimit 97
#endif
#ifndef SYS_getrusage
#define SYS_getrusage 98
#endif
#ifndef SYS_sysinfo
#define SYS_sysinfo 99
#endif
#ifndef SYS_times
#define SYS_times 100
#endif
#ifndef SYS_time
#define SYS_time 201
#endif
#ifndef SYS_getpriority
#define SYS_getpriority 140
#endif
#ifndef SYS_setpriority
#define SYS_setpriority 141
#endif
#ifndef SYS_sched_setparam
#define SYS_sched_setparam 142
#endif
#ifndef SYS_sched_getparam
#define SYS_sched_getparam 143
#endif
#ifndef SYS_sched_setscheduler
#define SYS_sched_setscheduler 144
#endif
#ifndef SYS_sched_getscheduler
#define SYS_sched_getscheduler 145
#endif
#ifndef SYS_sched_rr_get_interval
#define SYS_sched_rr_get_interval 148
#endif
#ifndef SYS_mlock
#define SYS_mlock 149
#endif
#ifndef SYS_munlock
#define SYS_munlock 150
#endif
#ifndef SYS_mlockall
#define SYS_mlockall 151
#endif
#ifndef SYS_munlockall
#define SYS_munlockall 152
#endif
#ifndef SYS_prctl
#define SYS_prctl 157
#endif
#ifndef SYS_arch_prctl
#define SYS_arch_prctl 158
#endif
#ifndef SYS_setrlimit
#define SYS_setrlimit 160
#endif
#ifndef SYS_sethostname
#define SYS_sethostname 170
#endif
#ifndef SYS_setdomainname
#define SYS_setdomainname 171
#endif
#ifndef SYS_personality
#define SYS_personality 135
#endif
#ifndef SYS_pause
#define SYS_pause 34
#endif
#ifndef SYS_pipe
#define SYS_pipe 22
#endif
#ifndef SYS_select
#define SYS_select 23
#endif
#ifndef SYS_dup
#define SYS_dup 32
#endif
#ifndef SYS_dup2
#define SYS_dup2 33
#endif
#ifndef SYS_kill
#define SYS_kill 62
#endif
#ifndef SYS_uname
#define SYS_uname 63
#endif
#ifndef SYS_flock
#define SYS_flock 73
#endif
#ifndef SYS_getuid
#define SYS_getuid 102
#endif
#ifndef SYS_capget
#define SYS_capget 125
#endif
#ifndef SYS_capset
#define SYS_capset 126
#endif
#ifndef SYS_getgid
#define SYS_getgid 104
#endif
#ifndef SYS_setuid
#define SYS_setuid 105
#endif
#ifndef SYS_setgid
#define SYS_setgid 106
#endif
#ifndef SYS_geteuid
#define SYS_geteuid 107
#endif
#ifndef SYS_getegid
#define SYS_getegid 108
#endif
#ifndef SYS_setpgid
#define SYS_setpgid 109
#endif
#ifndef SYS_getppid
#define SYS_getppid 110
#endif
#ifndef SYS_getpgrp
#define SYS_getpgrp 111
#endif
#ifndef SYS_setsid
#define SYS_setsid 112
#endif
#ifndef SYS_setreuid
#define SYS_setreuid 113
#endif
#ifndef SYS_setregid
#define SYS_setregid 114
#endif
#ifndef SYS_getgroups
#define SYS_getgroups 115
#endif
#ifndef SYS_setgroups
#define SYS_setgroups 116
#endif
#ifndef SYS_utime
#define SYS_utime 132
#endif
#ifndef SYS_statfs
#define SYS_statfs 137
#endif
#ifndef SYS_fstatfs
#define SYS_fstatfs 138
#endif
#ifndef SYS_setresuid
#define SYS_setresuid 117
#endif
#ifndef SYS_getresuid
#define SYS_getresuid 118
#endif
#ifndef SYS_setresgid
#define SYS_setresgid 119
#endif
#ifndef SYS_getresgid
#define SYS_getresgid 120
#endif
#ifndef SYS_getpgid
#define SYS_getpgid 121
#endif
#ifndef SYS_setfsuid
#define SYS_setfsuid 122
#endif
#ifndef SYS_setfsgid
#define SYS_setfsgid 123
#endif
#ifndef SYS_getsid
#define SYS_getsid 124
#endif
#ifndef SYS_tkill
#define SYS_tkill 200
#endif
#ifndef SYS_futex
#define SYS_futex 202
#endif
#ifndef SYS_set_tid_address
#define SYS_set_tid_address 218
#endif
#ifndef SYS_set_robust_list
#define SYS_set_robust_list 273
#endif
#ifndef SYS_get_robust_list
#define SYS_get_robust_list 274
#endif
#ifndef SYS_fadvise64
#define SYS_fadvise64 221
#endif
#ifndef SYS_timer_create
#define SYS_timer_create 222
#endif
#ifndef SYS_timer_settime
#define SYS_timer_settime 223
#endif
#ifndef SYS_timer_gettime
#define SYS_timer_gettime 224
#endif
#ifndef SYS_timer_getoverrun
#define SYS_timer_getoverrun 225
#endif
#ifndef SYS_timer_delete
#define SYS_timer_delete 226
#endif
#ifndef SYS_splice
#define SYS_splice 275
#endif
#ifndef SYS_vmsplice
#define SYS_vmsplice 278
#endif
#ifndef SYS_gettid
#define SYS_gettid 186
#endif
#ifndef SYS_setxattr
#define SYS_setxattr 188
#endif
#ifndef SYS_lsetxattr
#define SYS_lsetxattr 189
#endif
#ifndef SYS_fsetxattr
#define SYS_fsetxattr 190
#endif
#ifndef SYS_getxattr
#define SYS_getxattr 191
#endif
#ifndef SYS_lgetxattr
#define SYS_lgetxattr 192
#endif
#ifndef SYS_fgetxattr
#define SYS_fgetxattr 193
#endif
#ifndef SYS_listxattr
#define SYS_listxattr 194
#endif
#ifndef SYS_llistxattr
#define SYS_llistxattr 195
#endif
#ifndef SYS_flistxattr
#define SYS_flistxattr 196
#endif
#ifndef SYS_removexattr
#define SYS_removexattr 197
#endif
#ifndef SYS_lremovexattr
#define SYS_lremovexattr 198
#endif
#ifndef SYS_fremovexattr
#define SYS_fremovexattr 199
#endif
#ifndef SYS_tgkill
#define SYS_tgkill 234
#endif
#ifndef SYS_mbind
#define SYS_mbind 237
#endif
#ifndef SYS_set_mempolicy
#define SYS_set_mempolicy 238
#endif
#ifndef SYS_get_mempolicy
#define SYS_get_mempolicy 239
#endif
#ifndef SYS_migrate_pages
#define SYS_migrate_pages 256
#endif
#ifndef SYS_move_pages
#define SYS_move_pages 279
#endif
#ifndef SYS_utimes
#define SYS_utimes 235
#endif
#ifndef SYS_epoll_create
#define SYS_epoll_create 213
#endif
#ifndef SYS_epoll_wait
#define SYS_epoll_wait 232
#endif
#ifndef SYS_epoll_ctl
#define SYS_epoll_ctl 233
#endif
#ifndef SYS_pselect6
#define SYS_pselect6 270
#endif
#ifndef SYS_ppoll
#define SYS_ppoll 271
#endif
#ifndef SYS_unshare
#define SYS_unshare 272
#endif
#ifndef SYS_setns
#define SYS_setns 308
#endif
#ifndef SYS_utimensat
#define SYS_utimensat 280
#endif
#ifndef SYS_epoll_pwait
#define SYS_epoll_pwait 281
#endif
#ifndef SYS_fork
#define SYS_fork 57
#endif
#ifndef SYS_vfork
#define SYS_vfork 58
#endif
#ifndef SYS_eventfd
#define SYS_eventfd 284
#endif
#ifndef SYS_signalfd
#define SYS_signalfd 282
#endif
#ifndef SYS_timerfd_create
#define SYS_timerfd_create 283
#endif
#ifndef SYS_timerfd_settime
#define SYS_timerfd_settime 286
#endif
#ifndef SYS_timerfd_gettime
#define SYS_timerfd_gettime 287
#endif
#ifndef SYS_fallocate
#define SYS_fallocate 285
#endif
#ifndef SYS_accept4
#define SYS_accept4 288
#endif
#ifndef SYS_signalfd4
#define SYS_signalfd4 289
#endif
#ifndef SYS_epoll_create1
#define SYS_epoll_create1 291
#endif
#ifndef SYS_inotify_init1
#define SYS_inotify_init1 294
#endif
#ifndef SYS_eventfd2
#define SYS_eventfd2 290
#endif
#ifndef SYS_renameat2
#define SYS_renameat2 316
#endif
#ifndef SYS_sched_setattr
#define SYS_sched_setattr 314
#endif
#ifndef SYS_sched_getattr
#define SYS_sched_getattr 315
#endif
#ifndef SYS_sched_setaffinity
#define SYS_sched_setaffinity 203
#endif
#ifndef SYS_sched_getaffinity
#define SYS_sched_getaffinity 204
#endif
#ifndef SYS_clock_gettime
#define SYS_clock_gettime 228
#endif
#ifndef SYS_clock_getres
#define SYS_clock_getres 229
#endif
#ifndef SYS_clock_nanosleep
#define SYS_clock_nanosleep 230
#endif
#ifndef SYS_exit_group
#define SYS_exit_group 231
#endif
#ifndef SYS_waitid
#define SYS_waitid 247
#endif
#ifndef SYS_ioprio_set
#define SYS_ioprio_set 251
#endif
#ifndef SYS_ioprio_get
#define SYS_ioprio_get 252
#endif
#ifndef SYS_inotify_init
#define SYS_inotify_init 253
#endif
#ifndef SYS_inotify_add_watch
#define SYS_inotify_add_watch 254
#endif
#ifndef SYS_inotify_rm_watch
#define SYS_inotify_rm_watch 255
#endif
#ifndef SYS_fanotify_init
#define SYS_fanotify_init 300
#endif
#ifndef SYS_fanotify_mark
#define SYS_fanotify_mark 301
#endif
#ifndef SYS_sync
#define SYS_sync 162
#endif
#ifndef SYS_readahead
#define SYS_readahead 187
#endif
#ifndef SYS_sync_file_range
#define SYS_sync_file_range 277
#endif
#ifndef SYS_openat
#define SYS_openat 257
#endif
#ifndef SYS_mkdirat
#define SYS_mkdirat 258
#endif
#ifndef SYS_mknodat
#define SYS_mknodat 259
#endif
#ifndef SYS_fchownat
#define SYS_fchownat 260
#endif
#ifndef SYS_futimesat
#define SYS_futimesat 261
#endif
#ifndef SYS_newfstatat
#define SYS_newfstatat 262
#endif
#ifndef SYS_unlinkat
#define SYS_unlinkat 263
#endif
#ifndef SYS_renameat
#define SYS_renameat 264
#endif
#ifndef SYS_linkat
#define SYS_linkat 265
#endif
#ifndef SYS_symlinkat
#define SYS_symlinkat 266
#endif
#ifndef SYS_readlinkat
#define SYS_readlinkat 267
#endif
#ifndef SYS_fchmodat
#define SYS_fchmodat 268
#endif
#ifndef SYS_faccessat
#define SYS_faccessat 269
#endif
#ifndef SYS_dup3
#define SYS_dup3 292
#endif
#ifndef SYS_pipe2
#define SYS_pipe2 293
#endif
#ifndef SYS_pread64
#define SYS_pread64 17
#endif
#ifndef SYS_pwrite64
#define SYS_pwrite64 18
#endif
#ifndef SYS_readv
#define SYS_readv 19
#endif
#ifndef SYS_writev
#define SYS_writev 20
#endif
#ifndef SYS_preadv
#define SYS_preadv 295
#endif
#ifndef SYS_pwritev
#define SYS_pwritev 296
#endif
#ifndef SYS_preadv2
#define SYS_preadv2 327
#endif
#ifndef SYS_pwritev2
#define SYS_pwritev2 328
#endif
#ifndef SYS_recvmmsg
#define SYS_recvmmsg 299
#endif
#ifndef SYS_syncfs
#define SYS_syncfs 306
#endif
#ifndef SYS_sendmmsg
#define SYS_sendmmsg 307
#endif
#ifndef SYS_prlimit64
#define SYS_prlimit64 302
#endif
#ifndef SYS_name_to_handle_at
#define SYS_name_to_handle_at 303
#endif
#ifndef SYS_open_by_handle_at
#define SYS_open_by_handle_at 304
#endif
#ifndef SYS_getcpu
#define SYS_getcpu 309
#endif
#ifndef SYS_process_vm_readv
#define SYS_process_vm_readv 310
#endif
#ifndef SYS_process_vm_writev
#define SYS_process_vm_writev 311
#endif
#ifndef SYS_kcmp
#define SYS_kcmp 312
#endif
#ifndef SYS_seccomp
#define SYS_seccomp 317
#endif
#ifndef SYS_getrandom
#define SYS_getrandom 318
#endif
#ifndef SYS_memfd_create
#define SYS_memfd_create 319
#endif
#ifndef SYS_mlock2
#define SYS_mlock2 325
#endif
#ifndef SYS_membarrier
#define SYS_membarrier 324
#endif
#ifndef SYS_getdents64
#define SYS_getdents64 217
#endif
#ifndef SYS_copy_file_range
#define SYS_copy_file_range 326
#endif
#ifndef SYS_statx
#define SYS_statx 332
#endif
#ifndef SYS_rseq
#define SYS_rseq 334
#endif
#ifndef SYS_openat2
#define SYS_openat2 437
#endif
#ifndef SYS_close_range
#define SYS_close_range 436
#endif
#ifndef SYS_faccessat2
#define SYS_faccessat2 439
#endif
#ifndef SYS_futex_waitv
#define SYS_futex_waitv 449
#endif
#ifndef SYS_futex_wake
#define SYS_futex_wake 454
#endif
#ifndef SYS_futex_wait
#define SYS_futex_wait 455
#endif
#ifndef SYS_epoll_pwait2
#define SYS_epoll_pwait2 441
#endif
#ifndef SYS_landlock_create_ruleset
#define SYS_landlock_create_ruleset 444
#endif
#ifndef SYS_landlock_add_rule
#define SYS_landlock_add_rule 445
#endif
#ifndef SYS_landlock_restrict_self
#define SYS_landlock_restrict_self 446
#endif
#ifndef SYS_memfd_secret
#define SYS_memfd_secret 447
#endif
#ifndef SYS_fchmodat2
#define SYS_fchmodat2 452
#endif
#ifndef SYS_map_shadow_stack
#define SYS_map_shadow_stack 453
#endif
#ifndef SYS_statmount
#define SYS_statmount 457
#endif
#ifndef SYS_listmount
#define SYS_listmount 458
#endif
#ifndef SYS_mseal
#define SYS_mseal 462
#endif
#ifndef SYS_process_madvise
#define SYS_process_madvise 440
#endif
#ifndef SYS_process_mrelease
#define SYS_process_mrelease 448
#endif
#ifndef SYS_set_mempolicy_home_node
#define SYS_set_mempolicy_home_node 450
#endif
#ifndef SYS_cachestat
#define SYS_cachestat 451
#endif
#ifndef SYS_lsm_get_self_attr
#define SYS_lsm_get_self_attr 459
#endif
#ifndef SYS_lsm_set_self_attr
#define SYS_lsm_set_self_attr 460
#endif
#ifndef SYS_lsm_list_modules
#define SYS_lsm_list_modules 461
#endif

pid_t __wrap_getpid(void) {
    unsigned int pid = 0;
    int rc = sinomo_proc_id(&pid);
    if (rc == 0) {
        return (pid_t)pid;
    }
    errno = rc;
    return -1;
}

pid_t __wrap_getppid(void) {
    unsigned int pid = 0;
    unsigned int ppid = 0;
    int rc = sinomo_proc_id(&pid);
    if (rc == 0) {
        rc = sinomo_proc_parent(pid, &ppid);
    }
    if (rc == 0) {
        return (pid_t)ppid;
    }
    errno = rc;
    return -1;
}

pid_t gettid(void) {
    unsigned int tid = 0;
    int rc = sinomo_thread_id(&tid);
    if (rc == 0) {
        return (pid_t)tid;
    }
    errno = rc;
    return -1;
}

int fchdir(int fd) {
    int rc = sinomo_fchdir(fd);
    if (rc == 0) {
        return 0;
    }
    errno = rc;
    return -1;
}

pid_t __wrap_getpgid(pid_t pid) {
    int pgid = 0;
    int rc = sinomo_proc_getpgid((int)pid, &pgid);
    if (rc == 0) {
        return (pid_t)pgid;
    }
    errno = rc;
    return -1;
}

pid_t __wrap_getpgrp(void) {
    int pgid = 0;
    int rc = sinomo_proc_getpgrp(&pgid);
    if (rc == 0) {
        return (pid_t)pgid;
    }
    errno = rc;
    return -1;
}

pid_t __wrap_getsid(pid_t pid) {
    int sid = 0;
    int rc = sinomo_proc_getsid((int)pid, &sid);
    if (rc == 0) {
        return (pid_t)sid;
    }
    errno = rc;
    return -1;
}

int __wrap_setpgid(pid_t pid, pid_t pgid) {
    int rc = sinomo_proc_setpgid((int)pid, (int)pgid);
    if (rc == 0) {
        return 0;
    }
    errno = rc;
    return -1;
}

int __wrap_setpgrp(void) {
    return __wrap_setpgid(0, 0);
}

pid_t __wrap_setsid(void) {
    int sid = 0;
    int rc = sinomo_proc_setsid(&sid);
    if (rc == 0) {
        return (pid_t)sid;
    }
    errno = rc;
    return -1;
}

pid_t __wrap_tcgetpgrp(int fd) {
    int pgid = 0;
    int rc = sinomo_proc_tcgetpgrp(fd, &pgid);
    if (rc == 0) {
        return (pid_t)pgid;
    }
    errno = rc;
    return -1;
}

pid_t tcgetsid(int fd) {
    int sid = 0;
    int rc = sinomo_proc_tcgetsid(fd, &sid);
    if (rc == 0) {
        return (pid_t)sid;
    }
    errno = rc;
    return -1;
}

int __wrap_tcsetpgrp(int fd, pid_t pgid) {
    int rc = sinomo_proc_tcsetpgrp(fd, (int)pgid);
    if (rc == 0) {
        return 0;
    }
    errno = rc;
    return -1;
}

static int sinomo_signal_valid(int signum);
static int sinomo_sigset_contains(const sigset_t *set, int signum);
static void sinomo_queue_pending_signal(int signum, const siginfo_t *info);

int __wrap_kill(pid_t pid, int signal) {
    if (signal < 0 || signal > __WASI_SIGNAL_SYS) {
        errno = EINVAL;
        return -1;
    }
    __wasi_errno_t rc = __wasi_proc_signal((__wasi_pid_t)pid, (__wasi_signal_t)signal);
    if (rc == 0) {
        return 0;
    }
    errno = (int)rc;
    return -1;
}

static int sinomo_tkill(pid_t tid, int signal) {
    if (tid <= 0) {
        errno = EINVAL;
        return -1;
    }
    if (tid != gettid()) {
        errno = ESRCH;
        return -1;
    }
    return __wrap_kill(__wrap_getpid(), signal);
}

static int sinomo_tgkill(pid_t tgid, pid_t tid, int signal) {
    if (tgid <= 0 || tid <= 0) {
        errno = EINVAL;
        return -1;
    }
    if (tgid != __wrap_getpid() || tid != gettid()) {
        errno = ESRCH;
        return -1;
    }
    return __wrap_kill(tgid, signal);
}

static int sinomo_rt_tgsigqueueinfo(pid_t tgid, pid_t tid, int signal, siginfo_t *info) {
    if (tgid == __wrap_getpid() && tid == gettid() && sinomo_signal_valid(signal) && sinomo_sigset_contains(&sinomo_signal_mask, signal)) {
        siginfo_t queued;
        if (info != NULL) {
            queued = *info;
        } else {
            memset(&queued, 0, sizeof(queued));
        }
        queued.si_signo = signal;
        if (queued.si_pid == 0) {
            queued.si_pid = __wrap_getpid();
        }
        if (queued.si_uid == 0) {
            queued.si_uid = __wrap_getuid();
        }
        sinomo_queue_pending_signal(signal, &queued);
        return 0;
    }
    return sinomo_tgkill(tgid, tid, signal);
}

int __wrap_killpg(pid_t pgrp, int signal) {
    if (pgrp <= 0) {
        errno = EINVAL;
        return -1;
    }
    return __wrap_kill(-pgrp, signal);
}

static int sinomo_pidfd_open(pid_t pid, unsigned int flags);
static void sinomo_signal_reset_handlers_for_clone(void);

static pid_t sinomo_proc_fork_with_mode(__wasi_bool_t copy_memory) {
    __wasi_pid_t pid = 0;
    __wasi_errno_t rc = __wasi_proc_fork(copy_memory, &pid);
    if (rc == 0) {
        return (pid_t)pid;
    }
    errno = (int)rc;
    return -1;
}

pid_t __wrap_fork(void) {
    return sinomo_proc_fork_with_mode(1);
}

pid_t __wrap_vfork(void) {
    return sinomo_proc_fork_with_mode(0);
}

#ifndef CLONE_VM
#define CLONE_VM 0x00000100
#endif
#ifndef CLONE_FS
#define CLONE_FS 0x00000200
#endif
#ifndef CLONE_FILES
#define CLONE_FILES 0x00000400
#endif
#ifndef CLONE_SIGHAND
#define CLONE_SIGHAND 0x00000800
#endif
#ifndef CLONE_PIDFD
#define CLONE_PIDFD 0x00001000
#endif
#ifndef CLONE_VFORK
#define CLONE_VFORK 0x00004000
#endif
#ifndef CLONE_PARENT
#define CLONE_PARENT 0x00008000
#endif
#ifndef CLONE_THREAD
#define CLONE_THREAD 0x00010000
#endif
#ifndef SINOMO_PROC_ARCH_FS_BASE
#define SINOMO_PROC_ARCH_FS_BASE 1U
#endif
#ifndef SINOMO_PROC_ARCH_GS_BASE
#define SINOMO_PROC_ARCH_GS_BASE 2U
#endif
#ifndef CLONE_NEWNS
#define CLONE_NEWNS 0x00020000
#endif
#ifndef CLONE_SYSVSEM
#define CLONE_SYSVSEM 0x00040000
#endif
#ifndef CLONE_SETTLS
#define CLONE_SETTLS 0x00080000
#endif
#ifndef CLONE_PARENT_SETTID
#define CLONE_PARENT_SETTID 0x00100000
#endif
#ifndef CLONE_CHILD_CLEARTID
#define CLONE_CHILD_CLEARTID 0x00200000
#endif
#ifndef CLONE_DETACHED
#define CLONE_DETACHED 0x00400000
#endif
#ifndef CLONE_UNTRACED
#define CLONE_UNTRACED 0x00800000
#endif
#ifndef CLONE_CHILD_SETTID
#define CLONE_CHILD_SETTID 0x01000000
#endif
#ifndef CLONE_NEWCGROUP
#define CLONE_NEWCGROUP 0x02000000
#endif
#ifndef CLONE_NEWUTS
#define CLONE_NEWUTS 0x04000000
#endif
#ifndef CLONE_NEWIPC
#define CLONE_NEWIPC 0x08000000
#endif
#ifndef CLONE_NEWUSER
#define CLONE_NEWUSER 0x10000000
#endif
#ifndef CLONE_NEWPID
#define CLONE_NEWPID 0x20000000
#endif
#ifndef CLONE_NEWNET
#define CLONE_NEWNET 0x40000000
#endif
#ifndef CLONE_IO
#define CLONE_IO 0x80000000
#endif
#ifndef CLONE_CLEAR_SIGHAND
#define CLONE_CLEAR_SIGHAND 0x100000000ULL
#endif
#ifndef CLONE_INTO_CGROUP
#define CLONE_INTO_CGROUP 0x200000000ULL
#endif
#ifndef SINOMO_WAIT_STATUS_EXITED
#define SINOMO_WAIT_STATUS_EXITED (1U << 0)
#endif
#ifndef SINOMO_WAIT_STATUS_STOPPED
#define SINOMO_WAIT_STATUS_STOPPED (1U << 1)
#endif
#ifndef SINOMO_WAIT_STATUS_CONTINUED
#define SINOMO_WAIT_STATUS_CONTINUED (1U << 2)
#endif
#ifndef CSIGNAL
#define CSIGNAL 0x000000ff
#endif

static pid_t sinomo_syscall_clone(uint64_t flags, void *child_stack, int *parent_tid, int *child_tid, unsigned long tls) {
    (void)child_stack;
    uint64_t supported_flags =
        (uint64_t)CSIGNAL | CLONE_VM | CLONE_FS | CLONE_FILES | CLONE_SIGHAND |
        CLONE_PIDFD | CLONE_VFORK | CLONE_PARENT | CLONE_THREAD | CLONE_NEWNS |
        CLONE_SYSVSEM | CLONE_SETTLS | CLONE_PARENT_SETTID | CLONE_CHILD_CLEARTID |
        CLONE_DETACHED | CLONE_UNTRACED | CLONE_CHILD_SETTID | CLONE_NEWCGROUP |
        CLONE_NEWUTS | CLONE_NEWIPC | CLONE_NEWUSER | CLONE_NEWPID | CLONE_NEWNET |
        CLONE_IO | CLONE_CLEAR_SIGHAND;
    if ((flags & ~supported_flags) != 0) {
        errno = ENOSYS;
        return -1;
    }
    uint64_t unsupported_sharing = CLONE_FS | CLONE_FILES | CLONE_SIGHAND | CLONE_THREAD;
    if ((flags & CLONE_VFORK) == 0) {
        unsupported_sharing |= CLONE_VM;
    }
    if ((flags & unsupported_sharing) != 0 || ((flags & CLONE_SETTLS) == 0 && tls != 0)) {
        errno = ENOSYS;
        return -1;
    }
    if ((flags & CLONE_PIDFD) != 0) {
        if (parent_tid == NULL) {
            errno = EFAULT;
            return -1;
        }
        if ((flags & CLONE_PARENT_SETTID) != 0) {
            errno = EINVAL;
            return -1;
        }
    }
    uint64_t namespace_flags =
        flags & (CLONE_NEWNS | CLONE_NEWCGROUP | CLONE_NEWUTS | CLONE_NEWIPC |
                 CLONE_NEWUSER | CLONE_NEWPID | CLONE_NEWNET);
    pid_t pid = (flags & CLONE_VFORK) != 0 ? __wrap_vfork() : __wrap_fork();
    if (pid < 0) {
        return -1;
    }
    if (namespace_flags != 0) {
        int rc = sinomo_proc_unshare_namespaces((unsigned long long)namespace_flags);
        if (rc != 0) {
            errno = rc;
            return -1;
        }
    }
    if (pid == 0 && (flags & CLONE_PARENT) != 0) {
        int rc = sinomo_proc_inherit_parent();
        if (rc != 0) {
            errno = rc;
            return -1;
        }
    }
    if (pid == 0 && (flags & CLONE_CLEAR_SIGHAND) != 0) {
        sinomo_signal_reset_handlers_for_clone();
    }
    if (pid == 0 && (flags & CLONE_CHILD_CLEARTID) != 0) {
        int rc = sinomo_proc_set_clear_child_tid((unsigned long long)(uintptr_t)child_tid);
        if (rc != 0) {
            errno = rc;
            return -1;
        }
    }
    if (pid > 0 && (flags & CLONE_PIDFD) != 0) {
        int pidfd = sinomo_pidfd_open(pid, 0);
        if (pidfd < 0) {
            return -1;
        }
        *parent_tid = pidfd;
    }
    if (pid > 0 && (flags & CLONE_PARENT_SETTID) != 0 && parent_tid != NULL) {
        *parent_tid = (int)pid;
    }
    if (pid == 0 && (flags & CLONE_CHILD_SETTID) != 0 && child_tid != NULL) {
        *child_tid = (int)__wrap_getpid();
    }
    if (pid == 0 && (flags & CLONE_SETTLS) != 0) {
        int rc = sinomo_proc_set_arch_base(SINOMO_PROC_ARCH_FS_BASE, (unsigned long long)tls);
        if (rc != 0) {
            errno = rc;
            return -1;
        }
    }
    return pid;
}

struct sinomo_clone_args {
    uint64_t flags;
    uint64_t pidfd;
    uint64_t child_tid;
    uint64_t parent_tid;
    uint64_t exit_signal;
    uint64_t stack;
    uint64_t stack_size;
    uint64_t tls;
    uint64_t set_tid;
    uint64_t set_tid_size;
    uint64_t cgroup;
};

#define SINOMO_PIDFD_SLOTS 128
struct sinomo_pidfd_entry {
    int fd;
    pid_t pid;
};
static struct sinomo_pidfd_entry sinomo_pidfds[SINOMO_PIDFD_SLOTS];

static void sinomo_pidfd_remember(int fd, pid_t pid) {
    if (fd < 0 || pid <= 0) {
        return;
    }
    int empty = -1;
    for (int index = 0; index < SINOMO_PIDFD_SLOTS; index++) {
        if (sinomo_pidfds[index].fd == fd) {
            sinomo_pidfds[index].pid = pid;
            return;
        }
        if (empty < 0 && sinomo_pidfds[index].fd == 0) {
            empty = index;
        }
    }
    if (empty >= 0) {
        sinomo_pidfds[empty].fd = fd;
        sinomo_pidfds[empty].pid = pid;
    }
}

static pid_t sinomo_pidfd_lookup(int fd) {
    for (int index = 0; index < SINOMO_PIDFD_SLOTS; index++) {
        if (sinomo_pidfds[index].fd == fd) {
            return sinomo_pidfds[index].pid;
        }
    }
    errno = EBADF;
    return -1;
}

static int sinomo_pidfd_index(int fd) {
    for (int index = 0; index < SINOMO_PIDFD_SLOTS; index++) {
        if (sinomo_pidfds[index].fd == fd) {
            return index;
        }
    }
    return -1;
}

static int sinomo_pidfd_readable(int fd) {
    int index = sinomo_pidfd_index(fd);
    if (index < 0) {
        return 0;
    }
    pid_t pid = sinomo_pidfds[index].pid;
    __wasi_option_pid_t option_pid = {
        .tag = __WASI_OPTION_SOME,
        .u = {.some = (__wasi_pid_t)pid},
    };
    __wasi_join_status_t status = {0};
    int rc = sinomo_proc_wait_status(&option_pid, __WASI_JOIN_FLAGS_NON_BLOCKING, &status);
    if (rc == 0) {
        return status.tag != __WASI_JOIN_STATUS_TYPE_NOTHING;
    }
    int saved_errno = errno;
    int missing = __wrap_kill(pid, 0) != 0;
    errno = saved_errno;
    return missing;
}

static void sinomo_pidfd_forget(int fd) {
    for (int index = 0; index < SINOMO_PIDFD_SLOTS; index++) {
        if (sinomo_pidfds[index].fd == fd) {
            memset(&sinomo_pidfds[index], 0, sizeof(sinomo_pidfds[index]));
            return;
        }
    }
}

static int sinomo_pidfd_open(pid_t pid, unsigned int flags) {
    if (pid <= 0) {
        errno = EINVAL;
        return -1;
    }
    unsigned int supported = 0;
#ifdef O_NONBLOCK
    supported |= O_NONBLOCK;
#endif
#ifdef O_CLOEXEC
    supported |= O_CLOEXEC;
#endif
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    if (pid != __wrap_getpid() && __wrap_kill(pid, 0) != 0) {
        return -1;
    }
    int event_flags = 0;
#ifdef EFD_NONBLOCK
    if ((flags & O_NONBLOCK) != 0) {
        event_flags |= EFD_NONBLOCK;
    }
#endif
#ifdef EFD_CLOEXEC
    if ((flags & O_CLOEXEC) != 0) {
        event_flags |= EFD_CLOEXEC;
    }
#endif
    int fd = __wrap_eventfd(0, event_flags);
    if (fd < 0) {
        return -1;
    }
    sinomo_pidfd_remember(fd, pid);
    return fd;
}

static int sinomo_pidfd_send_signal(int pidfd, int signum, const siginfo_t *info, unsigned int flags) {
    (void)info;
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    pid_t pid = sinomo_pidfd_lookup(pidfd);
    if (pid < 0) {
        return -1;
    }
    return __wrap_kill(pid, signum);
}

static int sinomo_pidfd_getfd(int pidfd, int targetfd, unsigned int flags) {
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    pid_t pid = sinomo_pidfd_lookup(pidfd);
    if (pid < 0) {
        return -1;
    }
    if (pid != __wrap_getpid()) {
        errno = EPERM;
        return -1;
    }
    return __wrap_dup(targetfd);
}

static int sinomo_process_mrelease(int pidfd, unsigned int flags) {
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    pid_t pid = sinomo_pidfd_lookup(pidfd);
    if (pid < 0) {
        return -1;
    }
    if (pid != __wrap_getpid() && __wrap_kill(pid, 0) != 0) {
        return -1;
    }
    return 0;
}

static pid_t sinomo_syscall_clone3(const struct sinomo_clone_args *args, size_t size) {
    if (args == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (size < offsetof(struct sinomo_clone_args, tls) + sizeof(args->tls)) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_clone_args copy;
    memset(&copy, 0, sizeof(copy));
    memcpy(&copy, args, size < sizeof(copy) ? size : sizeof(copy));
    if (copy.set_tid == 0 && copy.set_tid_size != 0) {
        errno = EINVAL;
        return -1;
    }
    if (copy.exit_signal > 0xffU) {
        errno = EINVAL;
        return -1;
    }
    uint64_t flags = copy.flags | (uint64_t)copy.exit_signal;
    if ((flags & CLONE_PIDFD) != 0 && copy.pidfd == 0) {
        errno = EFAULT;
        return -1;
    }
    int *parent_result = (int *)(uintptr_t)copy.parent_tid;
    if ((flags & CLONE_PIDFD) != 0) {
        parent_result = (int *)(uintptr_t)copy.pidfd;
    }
    pid_t pid = sinomo_syscall_clone(
        flags,
        (void *)(uintptr_t)copy.stack,
        parent_result,
        (int *)(uintptr_t)copy.child_tid,
        (unsigned long)copy.tls
    );
    return pid;
}

static long sinomo_unshare(unsigned long flags) {
    unsigned long supported =
        CLONE_FS | CLONE_FILES | CLONE_NEWNS | CLONE_SYSVSEM |
        CLONE_NEWCGROUP | CLONE_NEWUTS | CLONE_NEWIPC | CLONE_NEWUSER |
        CLONE_NEWPID | CLONE_NEWNET;
    if ((flags & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    int rc = sinomo_proc_unshare_namespaces((unsigned long long)flags);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    return 0;
}

static long sinomo_setns(int fd, int nstype) {
    struct stat st;
    if (fstat(fd, &st) != 0) {
        return -1;
    }
    unsigned long supported =
        CLONE_NEWNS | CLONE_NEWCGROUP | CLONE_NEWUTS | CLONE_NEWIPC |
        CLONE_NEWUSER | CLONE_NEWPID | CLONE_NEWNET;
    if (nstype != 0 && ((unsigned long)nstype & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    int rc = sinomo_proc_unshare_namespaces((unsigned long long)nstype);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    return 0;
}

#ifndef PR_SET_PDEATHSIG
#define PR_SET_PDEATHSIG 1
#endif
#ifndef PR_GET_PDEATHSIG
#define PR_GET_PDEATHSIG 2
#endif
#ifndef PR_GET_DUMPABLE
#define PR_GET_DUMPABLE 3
#endif
#ifndef PR_SET_DUMPABLE
#define PR_SET_DUMPABLE 4
#endif
#ifndef PR_GET_KEEPCAPS
#define PR_GET_KEEPCAPS 7
#endif
#ifndef PR_SET_KEEPCAPS
#define PR_SET_KEEPCAPS 8
#endif
#ifndef PR_SET_NAME
#define PR_SET_NAME 15
#endif
#ifndef PR_GET_NAME
#define PR_GET_NAME 16
#endif
#ifndef PR_GET_SECCOMP
#define PR_GET_SECCOMP 21
#endif
#ifndef PR_SET_SECCOMP
#define PR_SET_SECCOMP 22
#endif
#ifndef PR_CAPBSET_READ
#define PR_CAPBSET_READ 23
#endif
#ifndef PR_CAPBSET_DROP
#define PR_CAPBSET_DROP 24
#endif
#ifndef PR_GET_SECUREBITS
#define PR_GET_SECUREBITS 27
#endif
#ifndef PR_SET_SECUREBITS
#define PR_SET_SECUREBITS 28
#endif
#ifndef PR_GET_TIMING
#define PR_GET_TIMING 13
#endif
#ifndef PR_SET_TIMING
#define PR_SET_TIMING 14
#endif
#ifndef PR_TIMING_STATISTICAL
#define PR_TIMING_STATISTICAL 0
#endif
#ifndef PR_TIMING_TIMESTAMP
#define PR_TIMING_TIMESTAMP 1
#endif
#ifndef PR_SET_TIMERSLACK
#define PR_SET_TIMERSLACK 29
#endif
#ifndef PR_GET_TIMERSLACK
#define PR_GET_TIMERSLACK 30
#endif
#ifndef PR_SET_CHILD_SUBREAPER
#define PR_SET_CHILD_SUBREAPER 36
#endif
#ifndef PR_GET_CHILD_SUBREAPER
#define PR_GET_CHILD_SUBREAPER 37
#endif
#ifndef PR_SET_NO_NEW_PRIVS
#define PR_SET_NO_NEW_PRIVS 38
#endif
#ifndef PR_GET_NO_NEW_PRIVS
#define PR_GET_NO_NEW_PRIVS 39
#endif
#ifndef PR_GET_TID_ADDRESS
#define PR_GET_TID_ADDRESS 40
#endif
#ifndef PR_SET_THP_DISABLE
#define PR_SET_THP_DISABLE 41
#endif
#ifndef PR_GET_THP_DISABLE
#define PR_GET_THP_DISABLE 42
#endif
#ifndef PR_SET_PTRACER
#define PR_SET_PTRACER 0x59616d61
#endif
#ifndef PR_SET_VMA
#define PR_SET_VMA 0x53564d41
#endif
#ifndef PR_SET_VMA_ANON_NAME
#define PR_SET_VMA_ANON_NAME 0
#endif
#ifndef PR_SET_IO_FLUSHER
#define PR_SET_IO_FLUSHER 57
#endif
#ifndef PR_GET_IO_FLUSHER
#define PR_GET_IO_FLUSHER 58
#endif

#define SINOMO_PROC_CONTROL_KEEPCAPS 1U
#define SINOMO_PROC_CONTROL_SECUREBITS 2U
#define SINOMO_PROC_CONTROL_TIMING_MODE 3U
#define SINOMO_PROC_CONTROL_THP_DISABLE 4U
#define SINOMO_PROC_CONTROL_IO_FLUSHER 5U
#define SINOMO_PROC_CONTROL_PTRACER 6U
#ifndef SECCOMP_MODE_DISABLED
#define SECCOMP_MODE_DISABLED 0
#endif
#ifndef SECCOMP_MODE_STRICT
#define SECCOMP_MODE_STRICT 1
#endif
#ifndef SECCOMP_MODE_FILTER
#define SECCOMP_MODE_FILTER 2
#endif
#ifndef SECCOMP_SET_MODE_STRICT
#define SECCOMP_SET_MODE_STRICT 0
#endif
#ifndef SECCOMP_SET_MODE_FILTER
#define SECCOMP_SET_MODE_FILTER 1
#endif
#ifndef SECCOMP_GET_ACTION_AVAIL
#define SECCOMP_GET_ACTION_AVAIL 2
#endif
#ifndef SECCOMP_GET_NOTIF_SIZES
#define SECCOMP_GET_NOTIF_SIZES 3
#endif
#ifndef SECCOMP_FILTER_FLAG_TSYNC
#define SECCOMP_FILTER_FLAG_TSYNC 1U
#endif
#ifndef SECCOMP_FILTER_FLAG_LOG
#define SECCOMP_FILTER_FLAG_LOG 2U
#endif
#ifndef SECCOMP_FILTER_FLAG_SPEC_ALLOW
#define SECCOMP_FILTER_FLAG_SPEC_ALLOW 4U
#endif
#ifndef SECCOMP_FILTER_FLAG_NEW_LISTENER
#define SECCOMP_FILTER_FLAG_NEW_LISTENER 8U
#endif
#ifndef SECCOMP_FILTER_FLAG_TSYNC_ESRCH
#define SECCOMP_FILTER_FLAG_TSYNC_ESRCH 16U
#endif
#ifndef SECCOMP_FILTER_FLAG_WAIT_KILLABLE_RECV
#define SECCOMP_FILTER_FLAG_WAIT_KILLABLE_RECV 32U
#endif
#ifndef SECCOMP_RET_KILL_PROCESS
#define SECCOMP_RET_KILL_PROCESS 0x80000000U
#endif
#ifndef SECCOMP_RET_KILL_THREAD
#define SECCOMP_RET_KILL_THREAD 0x00000000U
#endif
#ifndef SECCOMP_RET_TRAP
#define SECCOMP_RET_TRAP 0x00030000U
#endif
#ifndef SECCOMP_RET_ERRNO
#define SECCOMP_RET_ERRNO 0x00050000U
#endif
#ifndef SECCOMP_RET_USER_NOTIF
#define SECCOMP_RET_USER_NOTIF 0x7fc00000U
#endif
#ifndef SECCOMP_RET_TRACE
#define SECCOMP_RET_TRACE 0x7ff00000U
#endif
#ifndef SECCOMP_RET_LOG
#define SECCOMP_RET_LOG 0x7ffc0000U
#endif
#ifndef SECCOMP_RET_ALLOW
#define SECCOMP_RET_ALLOW 0x7fff0000U
#endif
#ifndef SECCOMP_RET_ACTION_FULL
#define SECCOMP_RET_ACTION_FULL 0xffff0000U
#endif
#ifndef LINUX_CAPABILITY_VERSION_1
#define LINUX_CAPABILITY_VERSION_1 0x19980330
#endif
#ifndef LINUX_CAPABILITY_VERSION_2
#define LINUX_CAPABILITY_VERSION_2 0x20071026
#endif
#ifndef LINUX_CAPABILITY_VERSION_3
#define LINUX_CAPABILITY_VERSION_3 0x20080522
#endif

struct sinomo_cap_user_header {
    uint32_t version;
    int pid;
};

struct sinomo_cap_user_data {
    uint32_t effective;
    uint32_t permitted;
    uint32_t inheritable;
};

static int sinomo_cap_word_count(uint32_t version) {
    switch (version) {
        case LINUX_CAPABILITY_VERSION_1:
            return 1;
        case LINUX_CAPABILITY_VERSION_2:
        case LINUX_CAPABILITY_VERSION_3:
            return 2;
        default:
            return 0;
    }
}

static int sinomo_cap_index_valid(unsigned long cap) {
    if (cap >= 64UL) {
        errno = EINVAL;
        return 0;
    }
    return 1;
}

static int sinomo_capget(struct sinomo_cap_user_header *header, struct sinomo_cap_user_data *data) {
    if (header == NULL || data == NULL) {
        errno = EFAULT;
        return -1;
    }
    int words = sinomo_cap_word_count(header->version);
    if (words == 0) {
        header->version = LINUX_CAPABILITY_VERSION_3;
        errno = EINVAL;
        return -1;
    }
    pid_t current = __wrap_getpid();
    if (header->pid != 0 && header->pid != current) {
        errno = ESRCH;
        return -1;
    }
    unsigned long long effective = 0;
    unsigned long long permitted = 0;
    unsigned long long inheritable = 0;
    unsigned long long bounding = 0;
    int rc = sinomo_proc_get_capabilities(&effective, &permitted, &inheritable, &bounding);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    for (int index = 0; index < words; index++) {
        data[index].effective = (uint32_t)(effective >> (32 * index));
        data[index].permitted = (uint32_t)(permitted >> (32 * index));
        data[index].inheritable = (uint32_t)(inheritable >> (32 * index));
    }
    return 0;
}

static int sinomo_capset(const struct sinomo_cap_user_header *header, const struct sinomo_cap_user_data *data) {
    if (header == NULL || data == NULL) {
        errno = EFAULT;
        return -1;
    }
    int words = sinomo_cap_word_count(header->version);
    if (words == 0) {
        errno = EINVAL;
        return -1;
    }
    pid_t current = __wrap_getpid();
    if (header->pid != 0 && header->pid != current) {
        errno = ESRCH;
        return -1;
    }
    unsigned long long effective = 0;
    unsigned long long permitted = 0;
    unsigned long long inheritable = 0;
    for (int index = 0; index < words; index++) {
        effective |= ((unsigned long long)data[index].effective) << (32 * index);
        permitted |= ((unsigned long long)data[index].permitted) << (32 * index);
        inheritable |= ((unsigned long long)data[index].inheritable) << (32 * index);
    }
    int rc = sinomo_proc_set_capabilities(effective, permitted, inheritable);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    return 0;
}

static long sinomo_personality_call(unsigned long persona) {
    unsigned long long previous = 0;
    unsigned long long requested = persona == 0xffffffffUL ? ~0ULL : (unsigned long long)persona;
    int rc = sinomo_proc_personality(requested, &previous);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    if (previous > (unsigned long long)LONG_MAX) {
        return LONG_MAX;
    }
    return (long)previous;
}

static long sinomo_set_tid_address(int *tidptr) {
    int rc = sinomo_proc_set_clear_child_tid((unsigned long long)(uintptr_t)tidptr);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    return (long)gettid();
}

static int sinomo_set_robust_list(void *head, size_t len) {
    int rc = sinomo_proc_set_robust_list((unsigned long long)(uintptr_t)head, (unsigned long long)len);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    return 0;
}

static int sinomo_get_robust_list(pid_t pid, void **head, size_t *len) {
    if (head == NULL || len == NULL) {
        errno = EFAULT;
        return -1;
    }
    pid_t current = __wrap_getpid();
    if (pid != 0 && pid != current) {
        errno = ESRCH;
        return -1;
    }
    unsigned long long head_address = 0;
    unsigned long long length = 0;
    int rc = sinomo_proc_get_robust_list((int)pid, &head_address, &length);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    *head = (void *)(uintptr_t)head_address;
    *len = (size_t)length;
    return 0;
}

#ifndef SCHED_NORMAL
#define SCHED_NORMAL 0
#endif
#ifndef SCHED_FIFO
#define SCHED_FIFO 1
#endif
#ifndef SCHED_RR
#define SCHED_RR 2
#endif
#ifndef SCHED_BATCH
#define SCHED_BATCH 3
#endif
#ifndef SCHED_IDLE
#define SCHED_IDLE 5
#endif
#ifndef SCHED_DEADLINE
#define SCHED_DEADLINE 6
#endif

struct sinomo_sched_attr {
    uint32_t size;
    uint32_t sched_policy;
    uint64_t sched_flags;
    int32_t sched_nice;
    uint32_t sched_priority;
    uint64_t sched_runtime;
    uint64_t sched_deadline;
    uint64_t sched_period;
    uint32_t sched_util_min;
    uint32_t sched_util_max;
};

static int sinomo_sched_getattr(pid_t pid, struct sinomo_sched_attr *attr, unsigned int size, unsigned int flags) {
    if (attr == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (size < 48 || flags != 0) {
        errno = EINVAL;
        return -1;
    }
    struct sched_param param;
    memset(&param, 0, sizeof(param));
    int policy = __wrap_sched_getscheduler(pid);
    if (policy < 0) {
        return -1;
    }
    if (__wrap_sched_getparam(pid, &param) != 0) {
        return -1;
    }
    struct sinomo_sched_attr value;
    memset(&value, 0, sizeof(value));
    value.size = size < sizeof(value) ? size : (uint32_t)sizeof(value);
    value.sched_policy = (uint32_t)policy;
    value.sched_priority = (uint32_t)param.sched_priority;
    value.sched_nice = __wrap_getpriority(PRIO_PROCESS, pid);
    if (value.sched_nice == -1 && errno != 0) {
        value.sched_nice = 0;
        errno = 0;
    }
    value.sched_runtime = 10000000ULL;
    value.sched_deadline = 10000000ULL;
    value.sched_period = 10000000ULL;
    value.sched_util_max = 1024;
    memcpy(attr, &value, size < sizeof(value) ? size : sizeof(value));
    return 0;
}

static int sinomo_sched_setattr(pid_t pid, const struct sinomo_sched_attr *attr, unsigned int flags) {
    if (attr == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (attr->size < 48 || flags != 0) {
        errno = EINVAL;
        return -1;
    }
    if (attr->sched_policy == SCHED_DEADLINE) {
        errno = EINVAL;
        return -1;
    }
    struct sched_param param;
    memset(&param, 0, sizeof(param));
    param.sched_priority = (int)attr->sched_priority;
    return __wrap_sched_setscheduler(pid, (int)attr->sched_policy, &param);
}

#ifndef MEMBARRIER_CMD_QUERY
#define MEMBARRIER_CMD_QUERY 0
#endif
#ifndef MEMBARRIER_CMD_GLOBAL
#define MEMBARRIER_CMD_GLOBAL 1
#endif
#ifndef MEMBARRIER_CMD_GLOBAL_EXPEDITED
#define MEMBARRIER_CMD_GLOBAL_EXPEDITED 2
#endif
#ifndef MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED
#define MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED 4
#endif
#ifndef MEMBARRIER_CMD_PRIVATE_EXPEDITED
#define MEMBARRIER_CMD_PRIVATE_EXPEDITED 8
#endif
#ifndef MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED
#define MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED 16
#endif
#ifndef MEMBARRIER_CMD_PRIVATE_EXPEDITED_SYNC_CORE
#define MEMBARRIER_CMD_PRIVATE_EXPEDITED_SYNC_CORE 32
#endif
#ifndef MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED_SYNC_CORE
#define MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED_SYNC_CORE 64
#endif
#ifndef MEMBARRIER_CMD_PRIVATE_EXPEDITED_RSEQ
#define MEMBARRIER_CMD_PRIVATE_EXPEDITED_RSEQ 128
#endif
#ifndef MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED_RSEQ
#define MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED_RSEQ 256
#endif

static int sinomo_membarrier(int command, unsigned int flags, int cpu_id) {
    (void)cpu_id;
    int supported =
        MEMBARRIER_CMD_GLOBAL |
        MEMBARRIER_CMD_GLOBAL_EXPEDITED |
        MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED |
        MEMBARRIER_CMD_PRIVATE_EXPEDITED |
        MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED |
        MEMBARRIER_CMD_PRIVATE_EXPEDITED_SYNC_CORE |
        MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED_SYNC_CORE |
        MEMBARRIER_CMD_PRIVATE_EXPEDITED_RSEQ |
        MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED_RSEQ;
    if (command == MEMBARRIER_CMD_QUERY) {
        return supported;
    }
    if (flags != 0 || (command & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    __sync_synchronize();
    return 0;
}

#ifndef RSEQ_FLAG_UNREGISTER
#define RSEQ_FLAG_UNREGISTER 1
#endif

static int sinomo_rseq(void *rseq, uint32_t rseq_len, int flags, uint32_t signature) {
    if ((flags & ~RSEQ_FLAG_UNREGISTER) != 0) {
        errno = EINVAL;
        return -1;
    }
    if ((flags & RSEQ_FLAG_UNREGISTER) != 0) {
        int rc = sinomo_proc_clear_rseq((unsigned long long)(uintptr_t)rseq);
        if (rc != 0) {
            errno = EINVAL;
            return -1;
        }
        return 0;
    }
    if (rseq == NULL || rseq_len == 0) {
        errno = EINVAL;
        return -1;
    }
    int rc = sinomo_proc_set_rseq((unsigned long long)(uintptr_t)rseq, rseq_len, signature);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    return 0;
}

#ifndef ARCH_SET_GS
#define ARCH_SET_GS 0x1001
#endif
#ifndef ARCH_SET_FS
#define ARCH_SET_FS 0x1002
#endif
#ifndef ARCH_GET_FS
#define ARCH_GET_FS 0x1003
#endif
#ifndef ARCH_GET_GS
#define ARCH_GET_GS 0x1004
#endif

static int sinomo_arch_prctl(int code, unsigned long addr) {
    switch (code) {
        case ARCH_SET_FS: {
            int rc = sinomo_proc_set_arch_base(SINOMO_PROC_ARCH_FS_BASE, (unsigned long long)addr);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case ARCH_SET_GS: {
            int rc = sinomo_proc_set_arch_base(SINOMO_PROC_ARCH_GS_BASE, (unsigned long long)addr);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case ARCH_GET_FS: {
            if ((unsigned long *)addr == NULL) {
                errno = EFAULT;
                return -1;
            }
            unsigned long long base = 0;
            int rc = sinomo_proc_get_arch_base(SINOMO_PROC_ARCH_FS_BASE, &base);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            *(unsigned long *)addr = (unsigned long)base;
            return 0;
        }
        case ARCH_GET_GS: {
            if ((unsigned long *)addr == NULL) {
                errno = EFAULT;
                return -1;
            }
            unsigned long long base = 0;
            int rc = sinomo_proc_get_arch_base(SINOMO_PROC_ARCH_GS_BASE, &base);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            *(unsigned long *)addr = (unsigned long)base;
            return 0;
        }
        default:
            errno = EINVAL;
            return -1;
    }
}

#ifndef MPOL_DEFAULT
#define MPOL_DEFAULT 0
#endif
#ifndef MPOL_PREFERRED
#define MPOL_PREFERRED 1
#endif
#ifndef MPOL_BIND
#define MPOL_BIND 2
#endif
#ifndef MPOL_INTERLEAVE
#define MPOL_INTERLEAVE 3
#endif
#ifndef MPOL_LOCAL
#define MPOL_LOCAL 4
#endif
#ifndef MPOL_F_NODE
#define MPOL_F_NODE (1 << 0)
#endif
#ifndef MPOL_F_ADDR
#define MPOL_F_ADDR (1 << 1)
#endif
#ifndef MPOL_MF_STRICT
#define MPOL_MF_STRICT (1 << 0)
#endif
#ifndef MPOL_MF_MOVE
#define MPOL_MF_MOVE (1 << 1)
#endif
#ifndef MPOL_MF_MOVE_ALL
#define MPOL_MF_MOVE_ALL (1 << 2)
#endif

static int sinomo_mempolicy_valid_mode(int mode) {
    int base_mode = mode & 0xffff;
    return base_mode == MPOL_DEFAULT ||
        base_mode == MPOL_PREFERRED ||
        base_mode == MPOL_BIND ||
        base_mode == MPOL_INTERLEAVE ||
        base_mode == MPOL_LOCAL;
}

static int sinomo_mempolicy_nodes_valid(const unsigned long *nodemask, unsigned long maxnode) {
    if (nodemask == NULL || maxnode == 0) {
        return 1;
    }
    unsigned long words = (maxnode + (sizeof(unsigned long) * 8UL) - 1UL) / (sizeof(unsigned long) * 8UL);
    for (unsigned long index = 0; index < words; index++) {
        unsigned long allowed = index == 0 ? 1UL : 0UL;
        if ((nodemask[index] & ~allowed) != 0) {
            errno = EINVAL;
            return 0;
        }
    }
    return 1;
}

static long sinomo_set_mempolicy(int mode, const unsigned long *nodemask, unsigned long maxnode) {
    if (!sinomo_mempolicy_valid_mode(mode) || !sinomo_mempolicy_nodes_valid(nodemask, maxnode)) {
        if (errno == 0) {
            errno = EINVAL;
        }
        return -1;
    }
    unsigned long nodes = nodemask != NULL && maxnode > 0 ? (nodemask[0] & 1UL) : 1UL;
    if (nodes == 0) {
        nodes = 1UL;
    }
    int rc = sinomo_proc_set_mempolicy(mode, (unsigned long long)nodes);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    return 0;
}

static long sinomo_get_mempolicy(int *mode, unsigned long *nodemask, unsigned long maxnode, void *addr, unsigned long flags) {
    (void)addr;
    if ((flags & ~(MPOL_F_NODE | MPOL_F_ADDR)) != 0) {
        errno = EINVAL;
        return -1;
    }
    int stored_mode = 0;
    unsigned long long stored_nodes = 1;
    int rc = sinomo_proc_get_mempolicy(&stored_mode, &stored_nodes);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    if (mode != NULL) {
        *mode = (flags & MPOL_F_NODE) != 0 ? 0 : stored_mode;
    }
    if (nodemask != NULL && maxnode > 0) {
        unsigned long words = (maxnode + (sizeof(unsigned long) * 8UL) - 1UL) / (sizeof(unsigned long) * 8UL);
        for (unsigned long index = 0; index < words; index++) {
            nodemask[index] = index == 0 ? (unsigned long)stored_nodes : 0UL;
        }
    }
    return 0;
}

static long sinomo_mbind(void *addr, unsigned long len, int mode, const unsigned long *nodemask, unsigned long maxnode, unsigned int flags) {
    (void)addr;
    (void)len;
    if ((flags & ~(MPOL_MF_STRICT | MPOL_MF_MOVE | MPOL_MF_MOVE_ALL)) != 0) {
        errno = EINVAL;
        return -1;
    }
    return sinomo_set_mempolicy(mode, nodemask, maxnode);
}

static long sinomo_migrate_pages(pid_t pid, unsigned long maxnode, const unsigned long *old_nodes, const unsigned long *new_nodes) {
    if (pid != 0 && pid != __wrap_getpid()) {
        errno = ESRCH;
        return -1;
    }
    if (!sinomo_mempolicy_nodes_valid(old_nodes, maxnode) || !sinomo_mempolicy_nodes_valid(new_nodes, maxnode)) {
        if (errno == 0) {
            errno = EINVAL;
        }
        return -1;
    }
    return 0;
}

static long sinomo_move_pages(pid_t pid, unsigned long count, void **pages, const int *nodes, int *status, int flags) {
    (void)pages;
    if (pid != 0 && pid != __wrap_getpid()) {
        errno = ESRCH;
        return -1;
    }
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    for (unsigned long index = 0; index < count; index++) {
        if (nodes != NULL && nodes[index] > 0) {
            errno = EINVAL;
            return -1;
        }
        if (status != NULL) {
            status[index] = 0;
        }
    }
    return 0;
}

static long sinomo_set_mempolicy_home_node(unsigned long start, unsigned long len, unsigned long home_node, unsigned long flags) {
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    if (len > 0 && start + len < start) {
        errno = EINVAL;
        return -1;
    }
    if (home_node != 0) {
        errno = EINVAL;
        return -1;
    }
    int rc = sinomo_proc_set_mempolicy_home_node((unsigned long long)home_node);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    return 0;
}

#ifndef IOPRIO_WHO_PROCESS
#define IOPRIO_WHO_PROCESS 1
#endif
#ifndef IOPRIO_WHO_PGRP
#define IOPRIO_WHO_PGRP 2
#endif
#ifndef IOPRIO_WHO_USER
#define IOPRIO_WHO_USER 3
#endif
#ifndef IOPRIO_CLASS_NONE
#define IOPRIO_CLASS_NONE 0
#endif
#ifndef IOPRIO_CLASS_RT
#define IOPRIO_CLASS_RT 1
#endif
#ifndef IOPRIO_CLASS_BE
#define IOPRIO_CLASS_BE 2
#endif
#ifndef IOPRIO_CLASS_IDLE
#define IOPRIO_CLASS_IDLE 3
#endif

static int sinomo_ioprio_valid_target(int which, int who) {
    switch (which) {
        case IOPRIO_WHO_PROCESS:
            return who == 0 || who == __wrap_getpid();
        case IOPRIO_WHO_PGRP:
            return who == 0 || who == __wrap_getpgrp();
        case IOPRIO_WHO_USER:
            return who == 0 || who == (int)__wrap_getuid();
        default:
            errno = EINVAL;
            return 0;
    }
}

static int sinomo_ioprio_valid_value(int value) {
    int class_id = (value >> 13) & 0x7;
    int data = value & 0x1fff;
    if (class_id < IOPRIO_CLASS_NONE || class_id > IOPRIO_CLASS_IDLE) {
        errno = EINVAL;
        return 0;
    }
    if (class_id == IOPRIO_CLASS_NONE) {
        return data == 0;
    }
    if (class_id == IOPRIO_CLASS_IDLE) {
        return data == 0;
    }
    return data >= 0 && data <= 7;
}

static long sinomo_ioprio_set(int which, int who, int value) {
    if (!sinomo_ioprio_valid_target(which, who)) {
        if (errno == 0) {
            errno = ESRCH;
        }
        return -1;
    }
    if (!sinomo_ioprio_valid_value(value)) {
        if (errno == 0) {
            errno = EINVAL;
        }
        return -1;
    }
    int rc = sinomo_proc_set_ioprio(value);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    return 0;
}

static long sinomo_ioprio_get(int which, int who) {
    if (!sinomo_ioprio_valid_target(which, who)) {
        if (errno == 0) {
            errno = ESRCH;
        }
        return -1;
    }
    int value = 0;
    int rc = sinomo_proc_get_ioprio(&value);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    return value;
}

static int sinomo_set_vma_name(void *addr, size_t len, const char *name) {
    if (addr == NULL || len == 0) {
        errno = EINVAL;
        return -1;
    }
    if (name == NULL || name[0] == 0) {
        for (size_t index = 0; index < SINOMO_VMA_NAME_SLOTS; index++) {
            if (sinomo_vma_names[index].addr == addr && sinomo_vma_names[index].len == len) {
                memset(&sinomo_vma_names[index], 0, sizeof(sinomo_vma_names[index]));
                return 0;
            }
        }
        return 0;
    }
    size_t name_len = strlen(name);
    if (name_len >= SINOMO_VMA_NAME_MAX) {
        errno = ERANGE;
        return -1;
    }
    size_t slot = SINOMO_VMA_NAME_SLOTS;
    for (size_t index = 0; index < SINOMO_VMA_NAME_SLOTS; index++) {
        if (sinomo_vma_names[index].addr == addr && sinomo_vma_names[index].len == len) {
            slot = index;
            break;
        }
        if (slot == SINOMO_VMA_NAME_SLOTS && sinomo_vma_names[index].addr == NULL) {
            slot = index;
        }
    }
    if (slot == SINOMO_VMA_NAME_SLOTS) {
        errno = ENOMEM;
        return -1;
    }
    sinomo_vma_names[slot].addr = addr;
    sinomo_vma_names[slot].len = len;
    memset(sinomo_vma_names[slot].name, 0, sizeof(sinomo_vma_names[slot].name));
    memcpy(sinomo_vma_names[slot].name, name, name_len);
    return 0;
}

struct sinomo_seccomp_notif_sizes {
    uint16_t seccomp_notif;
    uint16_t seccomp_notif_resp;
    uint16_t seccomp_data;
};

static int sinomo_seccomp_action_supported(uint32_t action) {
    switch (action & SECCOMP_RET_ACTION_FULL) {
        case SECCOMP_RET_KILL_PROCESS:
        case SECCOMP_RET_KILL_THREAD:
        case SECCOMP_RET_TRAP:
        case SECCOMP_RET_ERRNO:
        case SECCOMP_RET_USER_NOTIF:
        case SECCOMP_RET_TRACE:
        case SECCOMP_RET_LOG:
        case SECCOMP_RET_ALLOW:
            return 1;
        default:
            return 0;
    }
}

static long sinomo_seccomp(unsigned int operation, unsigned int flags, void *args) {
    switch (operation) {
        case SECCOMP_SET_MODE_STRICT:
            if (flags != 0 || args != NULL) {
                errno = EINVAL;
                return -1;
            }
            int rc = sinomo_proc_set_seccomp_mode(SECCOMP_MODE_STRICT);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        case SECCOMP_SET_MODE_FILTER: {
            const unsigned int supported_flags =
                SECCOMP_FILTER_FLAG_TSYNC
                | SECCOMP_FILTER_FLAG_LOG
                | SECCOMP_FILTER_FLAG_SPEC_ALLOW
                | SECCOMP_FILTER_FLAG_NEW_LISTENER
                | SECCOMP_FILTER_FLAG_TSYNC_ESRCH
                | SECCOMP_FILTER_FLAG_WAIT_KILLABLE_RECV;
            if ((flags & ~supported_flags) != 0) {
                errno = EINVAL;
                return -1;
            }
            int rc = sinomo_proc_set_seccomp_mode(SECCOMP_MODE_FILTER);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            if ((flags & SECCOMP_FILTER_FLAG_NEW_LISTENER) != 0) {
                return (long)__wrap_eventfd(0, EFD_CLOEXEC);
            }
            return 0;
        }
        case SECCOMP_GET_ACTION_AVAIL: {
            if (flags != 0 || args == NULL) {
                errno = EINVAL;
                return -1;
            }
            uint32_t action = *(uint32_t *)args;
            if (!sinomo_seccomp_action_supported(action)) {
                errno = EOPNOTSUPP;
                return -1;
            }
            return 0;
        }
        case SECCOMP_GET_NOTIF_SIZES: {
            if (flags != 0 || args == NULL) {
                errno = EINVAL;
                return -1;
            }
            struct sinomo_seccomp_notif_sizes *sizes = (struct sinomo_seccomp_notif_sizes *)args;
            sizes->seccomp_notif = 80;
            sizes->seccomp_notif_resp = 24;
            sizes->seccomp_data = 64;
            return 0;
        }
        default:
            errno = EINVAL;
            return -1;
    }
}

struct sinomo_lsm_ctx {
    uint64_t id;
    uint64_t flags;
    uint64_t len;
    uint64_t ctx_len;
    uint8_t ctx[];
};

static int sinomo_lsm_attr_supported(uint32_t attr) {
    return attr == LSM_ATTR_CURRENT ||
        attr == LSM_ATTR_EXEC ||
        attr == LSM_ATTR_FSCREATE ||
        attr == LSM_ATTR_KEYCREATE ||
        attr == LSM_ATTR_PREV ||
        attr == LSM_ATTR_SOCKCREATE;
}

static const char *sinomo_lsm_attr_value(uint32_t attr) {
    return attr == LSM_ATTR_CURRENT ? sinomo_lsm_current_attr : "";
}

static long sinomo_lsm_get_self_attr(uint32_t attr, struct sinomo_lsm_ctx *ctx, uint32_t *size, uint32_t flags) {
    if (size == NULL || !sinomo_lsm_attr_supported(attr) || (flags & ~LSM_FLAG_SINGLE) != 0) {
        errno = size == NULL ? EFAULT : EINVAL;
        return -1;
    }
    const char *value = sinomo_lsm_attr_value(attr);
    uint64_t ctx_len = (uint64_t)strlen(value) + 1U;
    uint64_t required = sizeof(struct sinomo_lsm_ctx) + ctx_len;
    if (ctx == NULL || *size < required) {
        *size = (uint32_t)required;
        errno = E2BIG;
        return -1;
    }
    memset(ctx, 0, (size_t)required);
    ctx->id = LSM_ID_LANDLOCK;
    ctx->len = required;
    ctx->ctx_len = ctx_len;
    memcpy(ctx->ctx, value, (size_t)ctx_len);
    *size = (uint32_t)required;
    return 1;
}

static long sinomo_lsm_set_self_attr(uint32_t attr, const struct sinomo_lsm_ctx *ctx, uint32_t size, uint32_t flags) {
    if (ctx == NULL || !sinomo_lsm_attr_supported(attr) || (flags & ~LSM_FLAG_SINGLE) != 0) {
        errno = ctx == NULL ? EFAULT : EINVAL;
        return -1;
    }
    if (attr != LSM_ATTR_CURRENT) {
        errno = EINVAL;
        return -1;
    }
    if (size < sizeof(*ctx) || ctx->len < sizeof(*ctx) || ctx->len > size || ctx->ctx_len == 0 || ctx->ctx_len > ctx->len - sizeof(*ctx)) {
        errno = EINVAL;
        return -1;
    }
    size_t copy_len = (size_t)ctx->ctx_len;
    if (copy_len >= sizeof(sinomo_lsm_current_attr)) {
        errno = ERANGE;
        return -1;
    }
    memset(sinomo_lsm_current_attr, 0, sizeof(sinomo_lsm_current_attr));
    memcpy(sinomo_lsm_current_attr, ctx->ctx, copy_len);
    sinomo_lsm_current_attr[sizeof(sinomo_lsm_current_attr) - 1] = 0;
    return 0;
}

static long sinomo_lsm_list_modules(uint64_t *ids, uint32_t *size, uint32_t flags) {
    if (size == NULL || flags != 0) {
        errno = size == NULL ? EFAULT : EINVAL;
        return -1;
    }
    uint32_t required = (uint32_t)sizeof(uint64_t);
    if (ids == NULL || *size < required) {
        *size = required;
        errno = E2BIG;
        return -1;
    }
    ids[0] = LSM_ID_LANDLOCK;
    *size = required;
    return 1;
}

int __wrap_prctl(int option, ...) {
    va_list ap;
    va_start(ap, option);

    switch (option) {
        case PR_SET_NAME: {
            const char *name = va_arg(ap, const char *);
            va_end(ap);
            if (name == NULL) {
                errno = EFAULT;
                return -1;
            }
            int rc = sinomo_proc_set_name(name, (unsigned int)strnlen(name, 15));
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case PR_GET_NAME: {
            char *name = va_arg(ap, char *);
            va_end(ap);
            if (name == NULL) {
                errno = EFAULT;
                return -1;
            }
            int rc = sinomo_proc_get_name(name, 16);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case PR_SET_SECCOMP: {
            unsigned long arg2 = va_arg(ap, unsigned long);
            unsigned long arg3 = va_arg(ap, unsigned long);
            unsigned long arg4 = va_arg(ap, unsigned long);
            unsigned long arg5 = va_arg(ap, unsigned long);
            va_end(ap);
            if (arg4 != 0 || arg5 != 0) {
                errno = EINVAL;
                return -1;
            }
            if (arg2 == SECCOMP_MODE_STRICT) {
                if (arg3 != 0) {
                    errno = EINVAL;
                    return -1;
                }
                int rc = sinomo_proc_set_seccomp_mode(SECCOMP_MODE_STRICT);
                if (rc != 0) {
                    errno = rc;
                    return -1;
                }
                return 0;
            }
            if (arg2 == SECCOMP_MODE_FILTER) {
                int rc = sinomo_proc_set_seccomp_mode(SECCOMP_MODE_FILTER);
                if (rc != 0) {
                    errno = rc;
                    return -1;
                }
                return 0;
            }
            errno = EINVAL;
            return -1;
        }
        case PR_GET_SECCOMP: {
            va_end(ap);
            unsigned int mode = 0;
            int rc = sinomo_proc_get_seccomp_mode(&mode);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return (int)mode;
        }
        case PR_SET_TIMING: {
            unsigned long arg2 = va_arg(ap, unsigned long);
            va_end(ap);
            if (arg2 != PR_TIMING_STATISTICAL && arg2 != PR_TIMING_TIMESTAMP) {
                errno = EINVAL;
                return -1;
            }
            int rc = sinomo_proc_set_process_control(SINOMO_PROC_CONTROL_TIMING_MODE, (unsigned long long)arg2);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case PR_GET_TIMING: {
            va_end(ap);
            unsigned long long value = 0;
            int rc = sinomo_proc_get_process_control(SINOMO_PROC_CONTROL_TIMING_MODE, &value);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return (int)value;
        }
        case PR_SET_DUMPABLE: {
            unsigned long arg2 = va_arg(ap, unsigned long);
            va_end(ap);
            if (arg2 > 1) {
                errno = EINVAL;
                return -1;
            }
            int rc = sinomo_proc_set_dumpable(arg2 != 0 ? 1U : 0U);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case PR_GET_DUMPABLE: {
            va_end(ap);
            unsigned int dumpable = 0;
            int rc = sinomo_proc_get_dumpable(&dumpable);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return dumpable != 0 ? 1 : 0;
        }
        case PR_SET_KEEPCAPS: {
            unsigned long arg2 = va_arg(ap, unsigned long);
            unsigned long arg3 = va_arg(ap, unsigned long);
            unsigned long arg4 = va_arg(ap, unsigned long);
            unsigned long arg5 = va_arg(ap, unsigned long);
            va_end(ap);
            if (arg2 > 1 || arg3 != 0 || arg4 != 0 || arg5 != 0) {
                errno = EINVAL;
                return -1;
            }
            int rc = sinomo_proc_set_process_control(SINOMO_PROC_CONTROL_KEEPCAPS, arg2 != 0 ? 1ULL : 0ULL);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case PR_GET_KEEPCAPS: {
            va_end(ap);
            unsigned long long value = 0;
            int rc = sinomo_proc_get_process_control(SINOMO_PROC_CONTROL_KEEPCAPS, &value);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return value != 0 ? 1 : 0;
        }
        case PR_CAPBSET_READ: {
            unsigned long cap = va_arg(ap, unsigned long);
            va_end(ap);
            if (!sinomo_cap_index_valid(cap)) {
                return -1;
            }
            unsigned int enabled = 0;
            int rc = sinomo_proc_get_capability_bounding((unsigned int)cap, &enabled);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return enabled != 0 ? 1 : 0;
        }
        case PR_CAPBSET_DROP: {
            unsigned long cap = va_arg(ap, unsigned long);
            unsigned long arg3 = va_arg(ap, unsigned long);
            unsigned long arg4 = va_arg(ap, unsigned long);
            unsigned long arg5 = va_arg(ap, unsigned long);
            va_end(ap);
            if (arg3 != 0 || arg4 != 0 || arg5 != 0 || !sinomo_cap_index_valid(cap)) {
                if (errno == 0) {
                    errno = EINVAL;
                }
                return -1;
            }
            int rc = sinomo_proc_drop_capability_bounding((unsigned int)cap);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case PR_SET_SECUREBITS: {
            unsigned long arg2 = va_arg(ap, unsigned long);
            va_end(ap);
            int rc = sinomo_proc_set_process_control(SINOMO_PROC_CONTROL_SECUREBITS, (unsigned long long)arg2);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case PR_GET_SECUREBITS: {
            va_end(ap);
            unsigned long long value = 0;
            int rc = sinomo_proc_get_process_control(SINOMO_PROC_CONTROL_SECUREBITS, &value);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return (int)value;
        }
        case PR_SET_NO_NEW_PRIVS: {
            unsigned long arg2 = va_arg(ap, unsigned long);
            unsigned long arg3 = va_arg(ap, unsigned long);
            unsigned long arg4 = va_arg(ap, unsigned long);
            unsigned long arg5 = va_arg(ap, unsigned long);
            va_end(ap);
            if (arg2 != 1 || arg3 != 0 || arg4 != 0 || arg5 != 0) {
                errno = EINVAL;
                return -1;
            }
            int rc = sinomo_proc_set_no_new_privs(1);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case PR_GET_NO_NEW_PRIVS: {
            va_end(ap);
            unsigned int enabled = 0;
            int rc = sinomo_proc_get_no_new_privs(&enabled);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return enabled != 0 ? 1 : 0;
        }
        case PR_GET_TID_ADDRESS: {
            int **tidptr = va_arg(ap, int **);
            va_end(ap);
            if (tidptr == NULL) {
                errno = EFAULT;
                return -1;
            }
            unsigned long long address = 0;
            int rc = sinomo_proc_get_clear_child_tid(&address);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            *tidptr = (int *)(uintptr_t)address;
            return 0;
        }
        case PR_SET_THP_DISABLE: {
            unsigned long arg2 = va_arg(ap, unsigned long);
            va_end(ap);
            if ((arg2 & ~3UL) != 0) {
                errno = EINVAL;
                return -1;
            }
            int rc = sinomo_proc_set_process_control(SINOMO_PROC_CONTROL_THP_DISABLE, (unsigned long long)arg2);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case PR_GET_THP_DISABLE: {
            va_end(ap);
            unsigned long long value = 0;
            int rc = sinomo_proc_get_process_control(SINOMO_PROC_CONTROL_THP_DISABLE, &value);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return (int)value;
        }
        case PR_SET_IO_FLUSHER: {
            unsigned long arg2 = va_arg(ap, unsigned long);
            va_end(ap);
            if (arg2 > 1) {
                errno = EINVAL;
                return -1;
            }
            int rc = sinomo_proc_set_process_control(SINOMO_PROC_CONTROL_IO_FLUSHER, arg2 != 0 ? 1ULL : 0ULL);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case PR_GET_IO_FLUSHER: {
            va_end(ap);
            unsigned long long value = 0;
            int rc = sinomo_proc_get_process_control(SINOMO_PROC_CONTROL_IO_FLUSHER, &value);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return value != 0 ? 1 : 0;
        }
        case PR_SET_PTRACER: {
            unsigned long arg2 = va_arg(ap, unsigned long);
            va_end(ap);
            if (arg2 != 0 && (pid_t)arg2 != __wrap_getpid() && (pid_t)arg2 != __wrap_getppid() && (long)arg2 != -1L) {
                errno = ESRCH;
                return -1;
            }
            int rc = sinomo_proc_set_process_control(SINOMO_PROC_CONTROL_PTRACER, (unsigned long long)arg2);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case PR_SET_VMA: {
            unsigned long arg2 = va_arg(ap, unsigned long);
            void *addr = va_arg(ap, void *);
            unsigned long len = va_arg(ap, unsigned long);
            const char *name = va_arg(ap, const char *);
            va_end(ap);
            if (arg2 != PR_SET_VMA_ANON_NAME) {
                errno = EINVAL;
                return -1;
            }
            return sinomo_set_vma_name(addr, (size_t)len, name);
        }
        case PR_SET_PDEATHSIG: {
            unsigned long arg2 = va_arg(ap, unsigned long);
            va_end(ap);
            if (!sinomo_signal_valid((int)arg2) && arg2 != 0) {
                errno = EINVAL;
                return -1;
            }
            int rc = sinomo_proc_set_parent_death_signal((int)arg2);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case PR_GET_PDEATHSIG: {
            int *signal_ptr = va_arg(ap, int *);
            va_end(ap);
            if (signal_ptr == NULL) {
                errno = EFAULT;
                return -1;
            }
            unsigned int signal = 0;
            int rc = sinomo_proc_get_parent_death_signal(&signal);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            *signal_ptr = (int)signal;
            return 0;
        }
        case PR_SET_CHILD_SUBREAPER: {
            unsigned long arg2 = va_arg(ap, unsigned long);
            va_end(ap);
            int rc = sinomo_proc_set_child_subreaper(arg2 != 0 ? 1U : 0U);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case PR_GET_CHILD_SUBREAPER: {
            int *value = va_arg(ap, int *);
            va_end(ap);
            if (value == NULL) {
                errno = EFAULT;
                return -1;
            }
            unsigned int enabled = 0;
            int rc = sinomo_proc_get_child_subreaper(&enabled);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            *value = enabled != 0 ? 1 : 0;
            return 0;
        }
        case PR_SET_TIMERSLACK: {
            unsigned long arg2 = va_arg(ap, unsigned long);
            va_end(ap);
            int rc = sinomo_proc_set_timer_slack(arg2 == 0 ? 50000ULL : (unsigned long long)arg2);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            return 0;
        }
        case PR_GET_TIMERSLACK: {
            va_end(ap);
            unsigned long long nanos = 0;
            int rc = sinomo_proc_get_timer_slack(&nanos);
            if (rc != 0) {
                errno = rc;
                return -1;
            }
            if (nanos > (unsigned long long)INT_MAX) {
                return INT_MAX;
            }
            return (int)nanos;
        }
        default:
            va_end(ap);
            errno = EINVAL;
            return -1;
    }
}

static int sinomo_futex_timeout_option(
    int absolute,
    int realtime,
    const struct timespec *timeout,
    __wasi_option_timestamp_t *timeout_option,
    const __wasi_option_timestamp_t **timeout_ptr
) {
    *timeout_ptr = NULL;
    memset(timeout_option, 0, sizeof(*timeout_option));
    if (timeout == NULL) {
        return 0;
    }
    if (timeout->tv_sec < 0 || timeout->tv_nsec < 0 || timeout->tv_nsec >= 1000000000L) {
        errno = EINVAL;
        return -1;
    }
    unsigned long long sec = (unsigned long long)timeout->tv_sec;
    unsigned long long nsec = (unsigned long long)timeout->tv_nsec;
    if (absolute) {
        struct timespec now;
        if (clock_gettime(realtime ? CLOCK_REALTIME : CLOCK_MONOTONIC, &now) != 0) {
            return -1;
        }
        if (
            timeout->tv_sec < now.tv_sec
            || (timeout->tv_sec == now.tv_sec && timeout->tv_nsec <= now.tv_nsec)
        ) {
            timeout_option->tag = 1;
            timeout_option->u.some = 0;
            *timeout_ptr = timeout_option;
            return 0;
        }
        sec = (unsigned long long)(timeout->tv_sec - now.tv_sec);
        if (timeout->tv_nsec >= now.tv_nsec) {
            nsec = (unsigned long long)(timeout->tv_nsec - now.tv_nsec);
        } else {
            sec--;
            nsec = 1000000000ULL + (unsigned long long)timeout->tv_nsec - (unsigned long long)now.tv_nsec;
        }
    }
    unsigned long long max_sec = ULLONG_MAX / 1000000000ULL;
    timeout_option->tag = 1;
    timeout_option->u.some = sec > max_sec
        ? ULLONG_MAX
        : (sec * 1000000000ULL) + nsec;
    *timeout_ptr = timeout_option;
    return 0;
}

static int sinomo_futex_wait_masked(uint32_t *uaddr, uint32_t value, uint32_t mask, const struct timespec *timeout, int absolute, int realtime) {
    if (mask == 0) {
        errno = EINVAL;
        return -1;
    }
    __wasi_option_timestamp_t timeout_option = {0};
    const __wasi_option_timestamp_t *timeout_ptr = NULL;
    if (sinomo_futex_timeout_option(absolute, realtime, timeout, &timeout_option, &timeout_ptr) != 0) {
        return -1;
    }
    __wasi_bool_t woken = 0;
    __wasi_errno_t rc = mask == FUTEX_BITSET_MATCH_ANY
        ? __wasi_futex_wait(uaddr, value, timeout_ptr, &woken)
        : sinomo_futex_wait_mask(uaddr, value, mask, timeout_ptr, &woken);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    if (!woken) {
        errno = ETIMEDOUT;
        return -1;
    }
    return 0;
}

static int sinomo_futex_wait(uint32_t *uaddr, uint32_t value, const struct timespec *timeout, int absolute, int realtime) {
    return sinomo_futex_wait_masked(uaddr, value, FUTEX_BITSET_MATCH_ANY, timeout, absolute, realtime);
}

static int sinomo_futex_wake_masked(uint32_t *uaddr, int requested, uint32_t mask) {
    if (mask == 0) {
        errno = EINVAL;
        return -1;
    }
    if (requested <= 0) {
        return 0;
    }
    int woken_count = 0;
    for (int index = 0; index < requested; index++) {
        __wasi_bool_t woken = 0;
        __wasi_errno_t rc = mask == FUTEX_BITSET_MATCH_ANY
            ? __wasi_futex_wake(uaddr, &woken)
            : sinomo_futex_wake_mask(uaddr, mask, &woken);
        if (rc != 0) {
            errno = (int)rc;
            return -1;
        }
        if (!woken) {
            break;
        }
        woken_count++;
    }
    return woken_count;
}

static int sinomo_futex_wake(uint32_t *uaddr, int requested) {
    return sinomo_futex_wake_masked(uaddr, requested, FUTEX_BITSET_MATCH_ANY);
}

static int sinomo_futex_requeue_waiters(uint32_t *uaddr, int wake_count, uint32_t *uaddr2, int requeue_count, uint32_t mask) {
    if (uaddr == NULL || uaddr2 == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (uaddr == uaddr2 || wake_count < 0 || requeue_count < 0 || mask == 0) {
        errno = EINVAL;
        return -1;
    }
    uint32_t affected = 0;
    __wasi_errno_t rc = sinomo_futex_requeue(
        uaddr,
        uaddr2,
        (uint32_t)wake_count,
        (uint32_t)requeue_count,
        mask,
        &affected
    );
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    return affected > (uint32_t)INT_MAX ? INT_MAX : (int)affected;
}

static int sinomo_futex_arg12(uint32_t value) {
    int arg = (int)(value & 0xfffU);
    return (arg & 0x800) != 0 ? arg | ~0xfff : arg;
}

static int sinomo_futex_compare(int old_value, int cmp, int cmp_arg) {
    switch (cmp) {
        case FUTEX_OP_CMP_EQ:
            return old_value == cmp_arg;
        case FUTEX_OP_CMP_NE:
            return old_value != cmp_arg;
        case FUTEX_OP_CMP_LT:
            return old_value < cmp_arg;
        case FUTEX_OP_CMP_LE:
            return old_value <= cmp_arg;
        case FUTEX_OP_CMP_GT:
            return old_value > cmp_arg;
        case FUTEX_OP_CMP_GE:
            return old_value >= cmp_arg;
        default:
            errno = EINVAL;
            return -1;
    }
}

static int sinomo_futex_wake_op(uint32_t *uaddr, int wake_count, uint32_t *uaddr2, int wake_count2, uint32_t encoded_op) {
    if (uaddr2 == NULL) {
        errno = EFAULT;
        return -1;
    }
    int op = (int)((encoded_op >> 28) & 0xfU);
    int cmp = (int)((encoded_op >> 24) & 0xfU);
    int op_arg = sinomo_futex_arg12(encoded_op >> 12);
    int cmp_arg = sinomo_futex_arg12(encoded_op);
    if ((op & FUTEX_OP_OPARG_SHIFT) != 0) {
        int shift = op_arg < 0 ? 0 : op_arg;
        op_arg = shift >= 31 ? INT_MIN : (1 << shift);
        op &= ~FUTEX_OP_OPARG_SHIFT;
    }
    uint32_t old_raw = *uaddr2;
    int old_value = (int)old_raw;
    switch (op) {
        case FUTEX_OP_SET:
            *uaddr2 = (uint32_t)op_arg;
            break;
        case FUTEX_OP_ADD:
            *uaddr2 = (uint32_t)(old_value + op_arg);
            break;
        case FUTEX_OP_OR:
            *uaddr2 = old_raw | (uint32_t)op_arg;
            break;
        case FUTEX_OP_ANDN:
            *uaddr2 = old_raw & ~(uint32_t)op_arg;
            break;
        case FUTEX_OP_XOR:
            *uaddr2 = old_raw ^ (uint32_t)op_arg;
            break;
        default:
            errno = EINVAL;
            return -1;
    }
    int woken = sinomo_futex_wake(uaddr, wake_count);
    if (woken < 0) {
        return -1;
    }
    int cmp_result = sinomo_futex_compare(old_value, cmp, cmp_arg);
    if (cmp_result < 0) {
        return -1;
    }
    if (cmp_result) {
        int woken2 = sinomo_futex_wake(uaddr2, wake_count2);
        if (woken2 < 0) {
            return -1;
        }
        woken += woken2;
    }
    return woken;
}

static int sinomo_futex_trylock_pi(uint32_t *uaddr) {
    if (uaddr == NULL) {
        errno = EFAULT;
        return -1;
    }
    uint32_t tid = (uint32_t)gettid() & FUTEX_TID_MASK;
    uint32_t current = *uaddr;
    if ((current & FUTEX_TID_MASK) == tid) {
        errno = EDEADLK;
        return -1;
    }
    if ((current & FUTEX_TID_MASK) != 0) {
        errno = EAGAIN;
        return -1;
    }
    if (!__sync_bool_compare_and_swap(uaddr, current, tid | (current & FUTEX_OWNER_DIED))) {
        errno = EAGAIN;
        return -1;
    }
    return 0;
}

static int sinomo_futex_lock_pi(uint32_t *uaddr, const struct timespec *timeout, int absolute, int realtime) {
    if (uaddr == NULL) {
        errno = EFAULT;
        return -1;
    }
    uint32_t tid = (uint32_t)gettid() & FUTEX_TID_MASK;
    for (;;) {
        uint32_t current = *uaddr;
        if ((current & FUTEX_TID_MASK) == tid) {
            errno = EDEADLK;
            return -1;
        }
        if ((current & FUTEX_TID_MASK) == 0) {
            if (__sync_bool_compare_and_swap(uaddr, current, tid | (current & FUTEX_OWNER_DIED))) {
                return 0;
            }
            continue;
        }
        uint32_t waiting = current | FUTEX_WAITERS;
        if (waiting != current) {
            __sync_bool_compare_and_swap(uaddr, current, waiting);
            current = waiting;
        }
        if (sinomo_futex_wait(uaddr, current, timeout, absolute, realtime) != 0) {
            return -1;
        }
    }
}

static int sinomo_futex_unlock_pi(uint32_t *uaddr) {
    if (uaddr == NULL) {
        errno = EFAULT;
        return -1;
    }
    uint32_t tid = (uint32_t)gettid() & FUTEX_TID_MASK;
    uint32_t current = *uaddr;
    if ((current & FUTEX_TID_MASK) != tid) {
        errno = EPERM;
        return -1;
    }
    *uaddr = 0;
    return sinomo_futex_wake(uaddr, 1) < 0 ? -1 : 0;
}

static int sinomo_futex_wait_requeue_pi(uint32_t *uaddr, uint32_t value, const struct timespec *timeout, uint32_t *uaddr2, int realtime) {
    if (uaddr == NULL || uaddr2 == NULL) {
        errno = EFAULT;
        return -1;
    }
    int wait_result = sinomo_futex_wait(uaddr, value, timeout, 0, realtime);
    if (wait_result != 0) {
        return -1;
    }
    return sinomo_futex_lock_pi(uaddr2, NULL, 0, 0);
}

static int sinomo_futex_cmp_requeue_pi(uint32_t *uaddr, int wake_count, uint32_t *uaddr2, uint32_t expected) {
    if (uaddr == NULL || uaddr2 == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (*uaddr != expected) {
        errno = EAGAIN;
        return -1;
    }
    int woken = sinomo_futex_wake(uaddr, wake_count);
    if (woken < 0) {
        return -1;
    }
    int pi_woken = sinomo_futex_wake(uaddr2, INT_MAX);
    if (pi_woken < 0) {
        return -1;
    }
    return woken + pi_woken;
}

struct sinomo_futex_waitv_entry {
    uint64_t val;
    uint64_t uaddr;
    uint32_t flags;
    uint32_t reserved;
};

static int sinomo_syscall_futex_waitv(
    const struct sinomo_futex_waitv_entry *waiters,
    unsigned int nr_futexes,
    unsigned int flags,
    const struct timespec *timeout,
    clockid_t clock_id
) {
    if (waiters == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (nr_futexes == 0 || nr_futexes > 128 || flags != 0) {
        errno = EINVAL;
        return -1;
    }
    int realtime = clock_id == CLOCK_REALTIME;
    if (!realtime && clock_id != CLOCK_MONOTONIC) {
        errno = EINVAL;
        return -1;
    }
    for (unsigned int index = 0; index < nr_futexes; index++) {
        if (waiters[index].reserved != 0 || (waiters[index].flags & ~(FUTEX_32 | FUTEX_PRIVATE_FLAG)) != 0 || (waiters[index].flags & FUTEX_32) == 0) {
            errno = EINVAL;
            return -1;
        }
        uint32_t *uaddr = (uint32_t *)(uintptr_t)waiters[index].uaddr;
        if (uaddr == NULL) {
            errno = EFAULT;
            return -1;
        }
        if (*uaddr != (uint32_t)waiters[index].val) {
            errno = EAGAIN;
            return -1;
        }
    }

    __wasi_option_timestamp_t timeout_option = {0};
    const __wasi_option_timestamp_t *timeout_ptr = NULL;
    if (sinomo_futex_timeout_option(1, realtime, timeout, &timeout_option, &timeout_ptr) != 0) {
        return -1;
    }
    uint32_t woken_index = UINT32_MAX;
    __wasi_errno_t rc = sinomo_futex_wait_any(waiters, nr_futexes, timeout_ptr, &woken_index);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }
    if (woken_index == UINT32_MAX) {
        errno = ETIMEDOUT;
        return -1;
    }
    if (woken_index >= nr_futexes) {
        errno = EINVAL;
        return -1;
    }
    return (int)woken_index;
}

static int sinomo_futex2_validate_flags(unsigned int flags) {
    unsigned int supported = FUTEX2_SIZE_U32 | FUTEX2_PRIVATE;
    if ((flags & ~supported) != 0 || (flags & FUTEX2_SIZE_MASK) != FUTEX2_SIZE_U32) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

static int sinomo_syscall_futex_wake(uint32_t *uaddr, unsigned long mask, int nr, unsigned int flags) {
    if (uaddr == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (mask == 0 || nr < 0 || sinomo_futex2_validate_flags(flags) != 0) {
        if (errno == 0) {
            errno = EINVAL;
        }
        return -1;
    }
    return sinomo_futex_wake_masked(uaddr, nr, (uint32_t)mask);
}

static int sinomo_syscall_futex_wait(uint32_t *uaddr, unsigned long val, unsigned long mask, unsigned int flags, const struct timespec *timeout, clockid_t clock_id) {
    if (uaddr == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (mask == 0 || val > UINT32_MAX || sinomo_futex2_validate_flags(flags) != 0) {
        if (errno == 0) {
            errno = EINVAL;
        }
        return -1;
    }
    int realtime = clock_id == CLOCK_REALTIME;
    if (!realtime && clock_id != CLOCK_MONOTONIC) {
        errno = EINVAL;
        return -1;
    }
    if (*uaddr != (uint32_t)val) {
        errno = EAGAIN;
        return -1;
    }
    return sinomo_futex_wait_masked(uaddr, (uint32_t)val, (uint32_t)mask, timeout, 0, realtime);
}

#ifndef IOCB_CMD_PREAD
#define IOCB_CMD_PREAD 0
#endif
#ifndef IOCB_CMD_PWRITE
#define IOCB_CMD_PWRITE 1
#endif
#ifndef IOCB_CMD_FSYNC
#define IOCB_CMD_FSYNC 2
#endif
#ifndef IOCB_CMD_FDSYNC
#define IOCB_CMD_FDSYNC 3
#endif
#ifndef IOCB_CMD_POLL
#define IOCB_CMD_POLL 5
#endif
#ifndef IOCB_CMD_NOOP
#define IOCB_CMD_NOOP 6
#endif
#ifndef IOCB_CMD_PREADV
#define IOCB_CMD_PREADV 7
#endif
#ifndef IOCB_CMD_PWRITEV
#define IOCB_CMD_PWRITEV 8
#endif
#ifndef IOCB_FLAG_RESFD
#define IOCB_FLAG_RESFD 1
#endif

struct sinomo_linux_iocb {
    uint64_t aio_data;
    uint32_t aio_key;
    uint32_t aio_rw_flags;
    uint16_t aio_lio_opcode;
    int16_t aio_reqprio;
    uint32_t aio_fildes;
    uint64_t aio_buf;
    uint64_t aio_nbytes;
    int64_t aio_offset;
    uint64_t aio_reserved2;
    uint32_t aio_flags;
    uint32_t aio_resfd;
};

struct sinomo_linux_io_event {
    uint64_t data;
    uint64_t obj;
    int64_t res;
    int64_t res2;
};

#define SINOMO_AIO_CONTEXTS 64
#define SINOMO_AIO_EVENTS 256
struct sinomo_aio_context {
    int active;
    unsigned long handle;
    unsigned int max_events;
    struct sinomo_linux_io_event events[SINOMO_AIO_EVENTS];
    unsigned int event_count;
};
static struct sinomo_aio_context sinomo_aio_contexts[SINOMO_AIO_CONTEXTS];
static unsigned long sinomo_aio_next_handle = 1;

static struct sinomo_aio_context *sinomo_aio_lookup(unsigned long handle) {
    for (size_t index = 0; index < SINOMO_AIO_CONTEXTS; index++) {
        if (sinomo_aio_contexts[index].active && sinomo_aio_contexts[index].handle == handle) {
            return &sinomo_aio_contexts[index];
        }
    }
    errno = EINVAL;
    return NULL;
}

static long sinomo_io_setup(unsigned int nr_events, unsigned long *ctxp) {
    if (ctxp == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (nr_events == 0) {
        errno = EINVAL;
        return -1;
    }
    for (size_t index = 0; index < SINOMO_AIO_CONTEXTS; index++) {
        if (!sinomo_aio_contexts[index].active) {
            memset(&sinomo_aio_contexts[index], 0, sizeof(sinomo_aio_contexts[index]));
            sinomo_aio_contexts[index].active = 1;
            sinomo_aio_contexts[index].handle = sinomo_aio_next_handle++;
            if (sinomo_aio_next_handle == 0) {
                sinomo_aio_next_handle = 1;
            }
            sinomo_aio_contexts[index].max_events = nr_events;
            *ctxp = sinomo_aio_contexts[index].handle;
            return 0;
        }
    }
    errno = EAGAIN;
    return -1;
}

static long sinomo_io_destroy(unsigned long handle) {
    struct sinomo_aio_context *context = sinomo_aio_lookup(handle);
    if (context == NULL) {
        return -1;
    }
    memset(context, 0, sizeof(*context));
    return 0;
}

static int sinomo_aio_push_event(struct sinomo_aio_context *context, const struct sinomo_linux_iocb *iocb, int64_t result, int64_t result2) {
    if (context->event_count >= SINOMO_AIO_EVENTS) {
        errno = EAGAIN;
        return -1;
    }
    struct sinomo_linux_io_event *event = &context->events[context->event_count++];
    event->data = iocb->aio_data;
    event->obj = (uint64_t)(uintptr_t)iocb;
    event->res = result;
    event->res2 = result2;
    if ((iocb->aio_flags & IOCB_FLAG_RESFD) != 0) {
        (void)__wrap_eventfd_write((int)iocb->aio_resfd, 1);
    }
    return 0;
}

static int64_t sinomo_aio_execute(const struct sinomo_linux_iocb *iocb) {
    errno = 0;
    switch (iocb->aio_lio_opcode) {
        case IOCB_CMD_PREAD:
            return (int64_t)__wrap_pread((int)iocb->aio_fildes, (void *)(uintptr_t)iocb->aio_buf, (size_t)iocb->aio_nbytes, (off_t)iocb->aio_offset);
        case IOCB_CMD_PWRITE:
            return (int64_t)__wrap_pwrite((int)iocb->aio_fildes, (const void *)(uintptr_t)iocb->aio_buf, (size_t)iocb->aio_nbytes, (off_t)iocb->aio_offset);
        case IOCB_CMD_PREADV:
            return (int64_t)__wrap_preadv((int)iocb->aio_fildes, (const struct iovec *)(uintptr_t)iocb->aio_buf, (int)iocb->aio_nbytes, (off_t)iocb->aio_offset);
        case IOCB_CMD_PWRITEV:
            return (int64_t)__wrap_pwritev((int)iocb->aio_fildes, (const struct iovec *)(uintptr_t)iocb->aio_buf, (int)iocb->aio_nbytes, (off_t)iocb->aio_offset);
        case IOCB_CMD_FSYNC:
            return (int64_t)__wrap_fsync((int)iocb->aio_fildes);
        case IOCB_CMD_FDSYNC:
            return (int64_t)__wrap_fdatasync((int)iocb->aio_fildes);
        case IOCB_CMD_POLL: {
            struct pollfd pfd = {
                .fd = (int)iocb->aio_fildes,
                .events = (short)iocb->aio_buf,
                .revents = 0,
            };
            int ready = __wrap_poll(&pfd, 1, 0);
            return ready < 0 ? -1 : (int64_t)pfd.revents;
        }
        case IOCB_CMD_NOOP:
            return 0;
        default:
            errno = EINVAL;
            return -1;
    }
}

static long sinomo_io_submit(unsigned long handle, long nr, struct sinomo_linux_iocb **iocbpp) {
    if (nr < 0 || (nr > 0 && iocbpp == NULL)) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_aio_context *context = sinomo_aio_lookup(handle);
    if (context == NULL) {
        return -1;
    }
    long submitted = 0;
    for (long index = 0; index < nr; index++) {
        struct sinomo_linux_iocb *iocb = iocbpp[index];
        if (iocb == NULL) {
            errno = EFAULT;
            return submitted > 0 ? submitted : -1;
        }
        int64_t result = sinomo_aio_execute(iocb);
        if (result < 0 && errno != 0) {
            result = -(int64_t)errno;
        }
        if (sinomo_aio_push_event(context, iocb, result, 0) != 0) {
            return submitted > 0 ? submitted : -1;
        }
        submitted++;
    }
    return submitted;
}

static long sinomo_io_getevents(unsigned long handle, long min_nr, long nr, struct sinomo_linux_io_event *events, const struct timespec *timeout) {
    if (min_nr < 0 || nr < 0 || min_nr > nr || (nr > 0 && events == NULL)) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_aio_context *context = sinomo_aio_lookup(handle);
    if (context == NULL) {
        return -1;
    }
    struct timespec start = {0, 0};
    if (timeout != NULL && clock_gettime(CLOCK_MONOTONIC, &start) != 0) {
        return -1;
    }
    for (;;) {
        long available = (long)context->event_count;
        if (available >= min_nr || (timeout != NULL && min_nr > 0)) {
            long copied = available < nr ? available : nr;
            if (copied > 0) {
                memcpy(events, context->events, (size_t)copied * sizeof(*events));
                memmove(context->events, context->events + copied, (size_t)(context->event_count - (unsigned int)copied) * sizeof(context->events[0]));
                context->event_count -= (unsigned int)copied;
            }
            if (copied >= min_nr || timeout == NULL || min_nr == 0) {
                return copied;
            }
        }
        if (timeout == NULL) {
            struct timespec delay = {0, 1000000L};
            if (nanosleep(&delay, NULL) != 0 && errno != EINTR) {
                return -1;
            }
            continue;
        }
        struct timespec now;
        if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
            return -1;
        }
        long long elapsed_ns = ((long long)(now.tv_sec - start.tv_sec) * 1000000000LL) + (long long)(now.tv_nsec - start.tv_nsec);
        long long timeout_ns = ((long long)timeout->tv_sec * 1000000000LL) + (long long)timeout->tv_nsec;
        if (timeout->tv_sec < 0 || timeout->tv_nsec < 0 || timeout->tv_nsec >= 1000000000L || elapsed_ns >= timeout_ns) {
            return 0;
        }
        struct timespec delay = {0, 1000000L};
        if (nanosleep(&delay, NULL) != 0 && errno != EINTR) {
            return -1;
        }
    }
}

static long sinomo_io_cancel(unsigned long handle, struct sinomo_linux_iocb *iocb, struct sinomo_linux_io_event *event) {
    if (iocb == NULL) {
        errno = EFAULT;
        return -1;
    }
    struct sinomo_aio_context *context = sinomo_aio_lookup(handle);
    if (context == NULL) {
        return -1;
    }
    uint64_t obj = (uint64_t)(uintptr_t)iocb;
    for (unsigned int index = 0; index < context->event_count; index++) {
        if (context->events[index].obj == obj) {
            if (event != NULL) {
                *event = context->events[index];
            }
            memmove(context->events + index, context->events + index + 1, (size_t)(context->event_count - index - 1) * sizeof(context->events[0]));
            context->event_count--;
            return 0;
        }
    }
    errno = EAGAIN;
    return -1;
}

#ifndef LANDLOCK_CREATE_RULESET_VERSION
#define LANDLOCK_CREATE_RULESET_VERSION 1U
#endif
#ifndef LANDLOCK_RULE_PATH_BENEATH
#define LANDLOCK_RULE_PATH_BENEATH 1
#endif

#define SINOMO_LANDLOCK_ABI_VERSION 4
#define SINOMO_LANDLOCK_RULESETS 64
#define SINOMO_LANDLOCK_RULES 128

struct sinomo_landlock_ruleset_attr {
    uint64_t handled_access_fs;
};

struct sinomo_landlock_path_beneath_attr {
    uint64_t allowed_access;
    int parent_fd;
};

struct sinomo_landlock_rule {
    int rule_type;
    uint64_t allowed_access;
    int parent_fd;
};

struct sinomo_landlock_ruleset {
    int active;
    int fd;
    uint64_t handled_access_fs;
    struct sinomo_landlock_rule rules[SINOMO_LANDLOCK_RULES];
    unsigned int rule_count;
};

static struct sinomo_landlock_ruleset sinomo_landlock_rulesets[SINOMO_LANDLOCK_RULESETS];
static uint64_t sinomo_landlock_restricted_access_fs = 0;

static struct sinomo_landlock_ruleset *sinomo_landlock_lookup(int fd) {
    for (size_t index = 0; index < SINOMO_LANDLOCK_RULESETS; index++) {
        if (sinomo_landlock_rulesets[index].active && sinomo_landlock_rulesets[index].fd == fd) {
            return &sinomo_landlock_rulesets[index];
        }
    }
    errno = EBADF;
    return NULL;
}

static long sinomo_landlock_create_ruleset(const struct sinomo_landlock_ruleset_attr *attr, size_t size, uint32_t flags) {
    if ((flags & LANDLOCK_CREATE_RULESET_VERSION) != 0) {
        if ((flags & ~LANDLOCK_CREATE_RULESET_VERSION) != 0 || attr != NULL || size != 0) {
            errno = EINVAL;
            return -1;
        }
        return SINOMO_LANDLOCK_ABI_VERSION;
    }
    if (flags != 0 || attr == NULL || size < sizeof(*attr)) {
        errno = EINVAL;
        return -1;
    }
    int slot = -1;
    for (size_t index = 0; index < SINOMO_LANDLOCK_RULESETS; index++) {
        if (!sinomo_landlock_rulesets[index].active) {
            slot = (int)index;
            break;
        }
    }
    if (slot < 0) {
        errno = EMFILE;
        return -1;
    }
    int fd = __wrap_eventfd(0, EFD_CLOEXEC);
    if (fd < 0) {
        return -1;
    }
    memset(&sinomo_landlock_rulesets[slot], 0, sizeof(sinomo_landlock_rulesets[slot]));
    sinomo_landlock_rulesets[slot].active = 1;
    sinomo_landlock_rulesets[slot].fd = fd;
    sinomo_landlock_rulesets[slot].handled_access_fs = attr->handled_access_fs;
    return fd;
}

static long sinomo_landlock_add_rule(int ruleset_fd, int rule_type, const void *rule_attr, uint32_t flags) {
    if (flags != 0 || rule_attr == NULL || rule_type != LANDLOCK_RULE_PATH_BENEATH) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_landlock_ruleset *ruleset = sinomo_landlock_lookup(ruleset_fd);
    if (ruleset == NULL) {
        return -1;
    }
    if (ruleset->rule_count >= SINOMO_LANDLOCK_RULES) {
        errno = ENOSPC;
        return -1;
    }
    const struct sinomo_landlock_path_beneath_attr *path_rule = (const struct sinomo_landlock_path_beneath_attr *)rule_attr;
    struct stat st;
    if (fstat(path_rule->parent_fd, &st) != 0) {
        return -1;
    }
    struct sinomo_landlock_rule *rule = &ruleset->rules[ruleset->rule_count++];
    rule->rule_type = rule_type;
    rule->allowed_access = path_rule->allowed_access;
    rule->parent_fd = path_rule->parent_fd;
    return 0;
}

static long sinomo_landlock_restrict_self(int ruleset_fd, uint32_t flags) {
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_landlock_ruleset *ruleset = sinomo_landlock_lookup(ruleset_fd);
    if (ruleset == NULL) {
        return -1;
    }
    sinomo_landlock_restricted_access_fs |= ruleset->handled_access_fs;
    return 0;
}

static void sinomo_landlock_forget(int fd) {
    for (size_t index = 0; index < SINOMO_LANDLOCK_RULESETS; index++) {
        if (sinomo_landlock_rulesets[index].active && sinomo_landlock_rulesets[index].fd == fd) {
            memset(&sinomo_landlock_rulesets[index], 0, sizeof(sinomo_landlock_rulesets[index]));
        }
    }
}

static int sinomo_syscall_futex(uint32_t *uaddr, int op, uint32_t value, const struct timespec *timeout, uint32_t *uaddr2, uint32_t value3) {
    int command = op & FUTEX_CMD_MASK;
    int realtime = (op & FUTEX_CLOCK_REALTIME) != 0;
    if (command == FUTEX_WAIT) {
        return sinomo_futex_wait(uaddr, value, timeout, realtime, realtime);
    }
    if (command == FUTEX_WAIT_BITSET) {
        if (value3 == 0) {
            errno = EINVAL;
            return -1;
        }
        return sinomo_futex_wait_masked(uaddr, value, value3, timeout, 1, realtime);
    }
    if (command == FUTEX_WAKE) {
        return sinomo_futex_wake(uaddr, (int)value);
    }
    if (command == FUTEX_WAKE_BITSET) {
        return sinomo_futex_wake_masked(uaddr, (int)value, value3);
    }
    if (command == FUTEX_FD) {
        errno = ENOSYS;
        return -1;
    }
    if (command == FUTEX_REQUEUE) {
        return sinomo_futex_requeue_waiters(uaddr, (int)value, uaddr2, (int)(uintptr_t)timeout, FUTEX_BITSET_MATCH_ANY);
    }
    if (command == FUTEX_CMP_REQUEUE) {
        if (uaddr2 == NULL) {
            errno = EFAULT;
            return -1;
        }
        if (*uaddr != value3) {
            errno = EAGAIN;
            return -1;
        }
        return sinomo_futex_requeue_waiters(uaddr, (int)value, uaddr2, (int)(uintptr_t)timeout, FUTEX_BITSET_MATCH_ANY);
    }
    if (command == FUTEX_WAKE_OP) {
        return sinomo_futex_wake_op(uaddr, (int)value, uaddr2, (int)(uintptr_t)timeout, value3);
    }
    if (command == FUTEX_TRYLOCK_PI) {
        return sinomo_futex_trylock_pi(uaddr);
    }
    if (command == FUTEX_LOCK_PI || command == FUTEX_LOCK_PI2) {
        return sinomo_futex_lock_pi(uaddr, timeout, command == FUTEX_LOCK_PI2, realtime);
    }
    if (command == FUTEX_UNLOCK_PI) {
        return sinomo_futex_unlock_pi(uaddr);
    }
    if (command == FUTEX_WAIT_REQUEUE_PI) {
        return sinomo_futex_wait_requeue_pi(uaddr, value, timeout, uaddr2, realtime);
    }
    if (command == FUTEX_CMP_REQUEUE_PI) {
        return sinomo_futex_cmp_requeue_pi(uaddr, (int)value, uaddr2, value3);
    }
    errno = ENOSYS;
    return -1;
}

long syscall(long number, ...) {
    va_list ap;
    va_start(ap, number);
    long result = -1;

    switch (number) {
        case SYS_getpid:
            result = (long)__wrap_getpid();
            break;
        case SYS_uname: {
            struct utsname *name = va_arg(ap, struct utsname *);
            result = (long)__wrap_uname(name);
            break;
        }
        case SYS_sethostname: {
            const char *name = va_arg(ap, const char *);
            size_t len = va_arg(ap, size_t);
            result = (long)__wrap_sethostname(name, len);
            break;
        }
        case SYS_setdomainname: {
            const char *name = va_arg(ap, const char *);
            size_t len = va_arg(ap, size_t);
            result = (long)__wrap_setdomainname(name, len);
            break;
        }
        case SYS_close: {
            int fd = va_arg(ap, int);
            result = (long)__wrap_close(fd);
            break;
        }
        case SYS_read: {
            int fd = va_arg(ap, int);
            void *buf = va_arg(ap, void *);
            size_t count = va_arg(ap, size_t);
            result = (long)__wrap_read(fd, buf, count);
            break;
        }
        case SYS_write: {
            int fd = va_arg(ap, int);
            const void *buf = va_arg(ap, const void *);
            size_t count = va_arg(ap, size_t);
            result = (long)__wrap_write(fd, buf, count);
            break;
        }
        case SYS_lseek: {
            int fd = va_arg(ap, int);
            off_t offset = (off_t)va_arg(ap, long);
            int whence = va_arg(ap, int);
            result = (long)lseek(fd, offset, whence);
            break;
        }
        case SYS_sendfile: {
            int out_fd = va_arg(ap, int);
            int in_fd = va_arg(ap, int);
            off_t *offset = va_arg(ap, off_t *);
            size_t count = va_arg(ap, size_t);
            result = (long)__wrap_sendfile(out_fd, in_fd, offset, count);
            break;
        }
        case SYS_copy_file_range: {
            int fd_in = va_arg(ap, int);
            off_t *off_in = va_arg(ap, off_t *);
            int fd_out = va_arg(ap, int);
            off_t *off_out = va_arg(ap, off_t *);
            size_t length = va_arg(ap, size_t);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)__wrap_copy_file_range(fd_in, off_in, fd_out, off_out, length, flags);
            break;
        }
        case SYS_execve: {
            const char *path = va_arg(ap, const char *);
            char *const *argv = va_arg(ap, char *const *);
            char *const *envp = va_arg(ap, char *const *);
            result = (long)__wrap_execve(path, (char *const *)argv, (char *const *)envp);
            break;
        }
        case SYS_execveat: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            char *const *argv = va_arg(ap, char *const *);
            char *const *envp = va_arg(ap, char *const *);
            int flags = va_arg(ap, int);
            result = (long)__wrap_execveat(dirfd, path, (char *const *)argv, (char *const *)envp, flags);
            break;
        }
        case SYS_exit: {
            int status = va_arg(ap, int);
            _exit(status);
        }
        case SYS_exit_group: {
            int status = va_arg(ap, int);
            _exit(status);
        }
        case SYS_wait4: {
            pid_t pid = (pid_t)va_arg(ap, long);
            int *status = va_arg(ap, int *);
            int options = va_arg(ap, int);
            struct rusage *usage = va_arg(ap, struct rusage *);
            result = (long)__wrap_wait4(pid, status, options, usage);
            break;
        }
        case SYS_waitid: {
            idtype_t idtype = (idtype_t)va_arg(ap, int);
            id_t id = (id_t)va_arg(ap, long);
            siginfo_t *info = va_arg(ap, siginfo_t *);
            int options = va_arg(ap, int);
            (void)va_arg(ap, struct rusage *);
            result = (long)__wrap_waitid(idtype, id, info, options);
            break;
        }
        case SYS_open: {
            const char *path = va_arg(ap, const char *);
            int flags = va_arg(ap, int);
            mode_t mode = (mode_t)va_arg(ap, int);
            result = (long)__wrap_openat(AT_FDCWD, path, flags, mode);
            break;
        }
        case SYS_creat: {
            const char *path = va_arg(ap, const char *);
            mode_t mode = (mode_t)va_arg(ap, int);
            result = (long)__wrap_openat(AT_FDCWD, path, O_CREAT | O_WRONLY | O_TRUNC, mode);
            break;
        }
        case SYS_openat: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            int flags = va_arg(ap, int);
            mode_t mode = (mode_t)va_arg(ap, int);
            result = (long)__wrap_openat(dirfd, path, flags, mode);
            break;
        }
        case SYS_openat2: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            const void *how = va_arg(ap, const void *);
            size_t size = va_arg(ap, size_t);
            result = (long)__wrap_openat2(dirfd, path, how, size);
            break;
        }
        case SYS_fcntl: {
            int fd = va_arg(ap, int);
            int cmd = va_arg(ap, int);
            if (cmd == F_GETLK || cmd == F_SETLK || cmd == F_SETLKW || cmd == F_OFD_GETLK || cmd == F_OFD_SETLK || cmd == F_OFD_SETLKW) {
                struct flock *lock = va_arg(ap, struct flock *);
                result = (long)__wrap_fcntl(fd, cmd, lock);
            } else if (cmd == F_GET_SEALS || cmd == F_GETPIPE_SZ) {
                result = (long)__wrap_fcntl(fd, cmd);
            } else {
                int int_arg = va_arg(ap, int);
                result = (long)__wrap_fcntl(fd, cmd, int_arg);
            }
            break;
        }
        case SYS_fsync: {
            int fd = va_arg(ap, int);
            result = (long)__wrap_fsync(fd);
            break;
        }
        case SYS_fdatasync: {
            int fd = va_arg(ap, int);
            result = (long)__wrap_fdatasync(fd);
            break;
        }
        case SYS_sync:
            __wrap_sync();
            result = 0;
            break;
        case SYS_syncfs: {
            int fd = va_arg(ap, int);
            result = (long)__wrap_syncfs(fd);
            break;
        }
        case SYS_readahead: {
            int fd = va_arg(ap, int);
            off_t offset = (off_t)va_arg(ap, long);
            size_t count = va_arg(ap, size_t);
            result = (long)__wrap_readahead(fd, offset, count);
            break;
        }
        case SYS_sync_file_range: {
            int fd = va_arg(ap, int);
            off_t offset = (off_t)va_arg(ap, long);
            off_t nbytes = (off_t)va_arg(ap, long);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)__wrap_sync_file_range(fd, offset, nbytes, flags);
            break;
        }
        case SYS_ioctl: {
            int fd = va_arg(ap, int);
            unsigned long request = va_arg(ap, unsigned long);
            void *arg = va_arg(ap, void *);
            result = (long)__wrap_ioctl(fd, (int)request, arg);
            break;
        }
        case SYS_flock: {
            int fd = va_arg(ap, int);
            int operation = va_arg(ap, int);
            result = (long)__wrap_flock(fd, operation);
            break;
        }
        case SYS_getrandom: {
            void *buf = va_arg(ap, void *);
            size_t length = va_arg(ap, size_t);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)__wrap_getrandom(buf, length, flags);
            break;
        }
        case SYS_memfd_create: {
            const char *name = va_arg(ap, const char *);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)__wrap_memfd_create(name, flags);
            break;
        }
        case SYS_memfd_secret: {
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)__wrap_memfd_secret(flags);
            break;
        }
        case SYS_io_setup: {
            unsigned int nr_events = va_arg(ap, unsigned int);
            unsigned long *ctxp = va_arg(ap, unsigned long *);
            result = sinomo_io_setup(nr_events, ctxp);
            break;
        }
        case SYS_io_destroy: {
            unsigned long ctx = va_arg(ap, unsigned long);
            result = sinomo_io_destroy(ctx);
            break;
        }
        case SYS_io_submit: {
            unsigned long ctx = va_arg(ap, unsigned long);
            long nr = va_arg(ap, long);
            struct sinomo_linux_iocb **iocbpp = va_arg(ap, struct sinomo_linux_iocb **);
            result = sinomo_io_submit(ctx, nr, iocbpp);
            break;
        }
        case SYS_io_getevents: {
            unsigned long ctx = va_arg(ap, unsigned long);
            long min_nr = va_arg(ap, long);
            long nr = va_arg(ap, long);
            struct sinomo_linux_io_event *events = va_arg(ap, struct sinomo_linux_io_event *);
            const struct timespec *timeout = va_arg(ap, const struct timespec *);
            result = sinomo_io_getevents(ctx, min_nr, nr, events, timeout);
            break;
        }
        case SYS_io_cancel: {
            unsigned long ctx = va_arg(ap, unsigned long);
            struct sinomo_linux_iocb *iocb = va_arg(ap, struct sinomo_linux_iocb *);
            struct sinomo_linux_io_event *event = va_arg(ap, struct sinomo_linux_io_event *);
            result = sinomo_io_cancel(ctx, iocb, event);
            break;
        }
        case SYS_landlock_create_ruleset: {
            const struct sinomo_landlock_ruleset_attr *attr = va_arg(ap, const struct sinomo_landlock_ruleset_attr *);
            size_t size = va_arg(ap, size_t);
            uint32_t flags = va_arg(ap, uint32_t);
            result = sinomo_landlock_create_ruleset(attr, size, flags);
            break;
        }
        case SYS_landlock_add_rule: {
            int ruleset_fd = va_arg(ap, int);
            int rule_type = va_arg(ap, int);
            const void *rule_attr = va_arg(ap, const void *);
            uint32_t flags = va_arg(ap, uint32_t);
            result = sinomo_landlock_add_rule(ruleset_fd, rule_type, rule_attr, flags);
            break;
        }
        case SYS_landlock_restrict_self: {
            int ruleset_fd = va_arg(ap, int);
            uint32_t flags = va_arg(ap, uint32_t);
            result = sinomo_landlock_restrict_self(ruleset_fd, flags);
            break;
        }
        case SYS_futex: {
            uint32_t *uaddr = va_arg(ap, uint32_t *);
            int op = va_arg(ap, int);
            uint32_t value = va_arg(ap, uint32_t);
            const struct timespec *timeout = va_arg(ap, const struct timespec *);
            int command = op & FUTEX_CMD_MASK;
            uint32_t *uaddr2 = NULL;
            uint32_t value3 = FUTEX_BITSET_MATCH_ANY;
            if (command == FUTEX_REQUEUE || command == FUTEX_CMP_REQUEUE || command == FUTEX_WAKE_OP || command == FUTEX_WAIT_BITSET || command == FUTEX_WAKE_BITSET || command == FUTEX_WAIT_REQUEUE_PI || command == FUTEX_CMP_REQUEUE_PI) {
                uaddr2 = va_arg(ap, uint32_t *);
            }
            if (command == FUTEX_CMP_REQUEUE || command == FUTEX_WAKE_OP || command == FUTEX_WAIT_BITSET || command == FUTEX_WAKE_BITSET || command == FUTEX_CMP_REQUEUE_PI) {
                value3 = va_arg(ap, uint32_t);
            }
            result = (long)sinomo_syscall_futex(uaddr, op, value, timeout, uaddr2, value3);
            break;
        }
        case SYS_futex_waitv: {
            const struct sinomo_futex_waitv_entry *waiters = va_arg(ap, const struct sinomo_futex_waitv_entry *);
            unsigned int nr_futexes = va_arg(ap, unsigned int);
            unsigned int flags = va_arg(ap, unsigned int);
            const struct timespec *timeout = va_arg(ap, const struct timespec *);
            clockid_t clock_id = va_arg(ap, clockid_t);
            result = (long)sinomo_syscall_futex_waitv(waiters, nr_futexes, flags, timeout, clock_id);
            break;
        }
        case SYS_futex_wake: {
            uint32_t *uaddr = va_arg(ap, uint32_t *);
            unsigned long mask = va_arg(ap, unsigned long);
            int nr = va_arg(ap, int);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)sinomo_syscall_futex_wake(uaddr, mask, nr, flags);
            break;
        }
        case SYS_futex_wait: {
            uint32_t *uaddr = va_arg(ap, uint32_t *);
            unsigned long val = va_arg(ap, unsigned long);
            unsigned long mask = va_arg(ap, unsigned long);
            unsigned int flags = va_arg(ap, unsigned int);
            const struct timespec *timeout = va_arg(ap, const struct timespec *);
            clockid_t clock_id = va_arg(ap, clockid_t);
            result = (long)sinomo_syscall_futex_wait(uaddr, val, mask, flags, timeout, clock_id);
            break;
        }
        case SYS_fallocate: {
            int fd = va_arg(ap, int);
            int mode = va_arg(ap, int);
            off_t offset = (off_t)va_arg(ap, long);
            off_t length = (off_t)va_arg(ap, long);
            if (mode != 0) {
                errno = EINVAL;
                result = -1;
            } else {
                int rc = __wrap_posix_fallocate(fd, offset, length);
                if (rc == 0) {
                    result = 0;
                } else {
                    errno = rc;
                    result = -1;
                }
            }
            break;
        }
        case SYS_fadvise64: {
            int fd = va_arg(ap, int);
            off_t offset = (off_t)va_arg(ap, long);
            off_t length = (off_t)va_arg(ap, long);
            int advice = va_arg(ap, int);
            int rc = __wrap_posix_fadvise(fd, offset, length, advice);
            if (rc == 0) {
                result = 0;
            } else {
                errno = rc;
                result = -1;
            }
            break;
        }
        case SYS_mmap: {
            void *addr = va_arg(ap, void *);
            size_t length = va_arg(ap, size_t);
            int prot = va_arg(ap, int);
            int flags = va_arg(ap, int);
            int fd = va_arg(ap, int);
            off_t offset = (off_t)va_arg(ap, long);
            result = (long)mmap(addr, length, prot, flags, fd, offset);
            break;
        }
        case SYS_map_shadow_stack: {
            void *addr = (void *)va_arg(ap, unsigned long);
            size_t size = va_arg(ap, size_t);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)__wrap_map_shadow_stack(addr, size, flags);
            break;
        }
        case SYS_munmap: {
            void *addr = va_arg(ap, void *);
            size_t length = va_arg(ap, size_t);
            result = (long)__wrap_munmap(addr, length);
            break;
        }
        case SYS_mprotect: {
            void *addr = va_arg(ap, void *);
            size_t length = va_arg(ap, size_t);
            int prot = va_arg(ap, int);
            result = (long)__wrap_mprotect(addr, length, prot);
            break;
        }
        case SYS_mseal: {
            void *addr = (void *)va_arg(ap, unsigned long);
            size_t length = va_arg(ap, size_t);
            unsigned long flags = va_arg(ap, unsigned long);
            result = (long)__wrap_mseal(addr, length, flags);
            break;
        }
        case SYS_mremap: {
            void *old_address = va_arg(ap, void *);
            size_t old_size = va_arg(ap, size_t);
            size_t new_size = va_arg(ap, size_t);
            int flags = va_arg(ap, int);
            if ((flags & MREMAP_FIXED) != 0) {
                void *new_address = va_arg(ap, void *);
                result = (long)__wrap_mremap(old_address, old_size, new_size, flags, new_address);
            } else {
                result = (long)__wrap_mremap(old_address, old_size, new_size, flags);
            }
            break;
        }
        case SYS_brk: {
            void *addr = va_arg(ap, void *);
            void *current = sbrk(0);
            if (addr != NULL && current != (void *)-1) {
                intptr_t delta = (char *)addr - (char *)current;
                (void)sbrk(delta);
                current = sbrk(0);
            }
            result = (long)current;
            break;
        }
        case SYS_msync: {
            void *addr = va_arg(ap, void *);
            size_t length = va_arg(ap, size_t);
            int flags = va_arg(ap, int);
            result = (long)msync(addr, length, flags);
            break;
        }
        case SYS_mincore: {
            void *addr = va_arg(ap, void *);
            size_t length = va_arg(ap, size_t);
            unsigned char *vec = va_arg(ap, unsigned char *);
            result = (long)__wrap_mincore(addr, length, vec);
            break;
        }
        case SYS_madvise: {
            void *addr = va_arg(ap, void *);
            size_t length = va_arg(ap, size_t);
            int advice = va_arg(ap, int);
            result = (long)__wrap_madvise(addr, length, advice);
            break;
        }
        case SYS_mlock: {
            const void *addr = va_arg(ap, const void *);
            size_t length = va_arg(ap, size_t);
            result = (long)__wrap_mlock(addr, length);
            break;
        }
        case SYS_munlock: {
            const void *addr = va_arg(ap, const void *);
            size_t length = va_arg(ap, size_t);
            result = (long)__wrap_munlock(addr, length);
            break;
        }
        case SYS_mlockall: {
            int flags = va_arg(ap, int);
            result = (long)__wrap_mlockall(flags);
            break;
        }
        case SYS_munlockall:
            result = (long)__wrap_munlockall();
            break;
        case SYS_mlock2: {
            const void *addr = va_arg(ap, const void *);
            size_t length = va_arg(ap, size_t);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)__wrap_mlock2(addr, length, flags);
            break;
        }
        case SYS_poll: {
            struct pollfd *fds = va_arg(ap, struct pollfd *);
            nfds_t nfds = (nfds_t)va_arg(ap, unsigned long);
            int timeout = va_arg(ap, int);
            result = (long)__wrap_poll(fds, nfds, timeout);
            break;
        }
        case SYS_ppoll: {
            struct pollfd *fds = va_arg(ap, struct pollfd *);
            nfds_t nfds = (nfds_t)va_arg(ap, unsigned long);
            const struct timespec *timeout = va_arg(ap, const struct timespec *);
            const sigset_t *sigmask = va_arg(ap, const sigset_t *);
            (void)va_arg(ap, size_t);
            result = (long)__wrap_ppoll(fds, nfds, timeout, sigmask);
            break;
        }
        case SYS_select: {
            int nfds = va_arg(ap, int);
            fd_set *readfds = va_arg(ap, fd_set *);
            fd_set *writefds = va_arg(ap, fd_set *);
            fd_set *exceptfds = va_arg(ap, fd_set *);
            struct timeval *timeout = va_arg(ap, struct timeval *);
            result = (long)__wrap_select(nfds, readfds, writefds, exceptfds, timeout);
            break;
        }
        case SYS_pselect6: {
            struct sinomo_pselect_sigmask {
                const sigset_t *mask;
                size_t size;
            };
            int nfds = va_arg(ap, int);
            fd_set *readfds = va_arg(ap, fd_set *);
            fd_set *writefds = va_arg(ap, fd_set *);
            fd_set *exceptfds = va_arg(ap, fd_set *);
            const struct timespec *timeout = va_arg(ap, const struct timespec *);
            const struct sinomo_pselect_sigmask *sigmask = va_arg(ap, const struct sinomo_pselect_sigmask *);
            const sigset_t *mask = sigmask == NULL ? NULL : sigmask->mask;
            result = (long)__wrap_pselect(nfds, readfds, writefds, exceptfds, timeout, mask);
            break;
        }
        case SYS_epoll_create: {
            int size = va_arg(ap, int);
            result = (long)__wrap_epoll_create(size);
            break;
        }
        case SYS_epoll_create1: {
            int flags = va_arg(ap, int);
            result = (long)__wrap_epoll_create1(flags);
            break;
        }
        case SYS_epoll_ctl: {
            int epfd = va_arg(ap, int);
            int op = va_arg(ap, int);
            int fd = va_arg(ap, int);
            struct epoll_event *event = va_arg(ap, struct epoll_event *);
            result = (long)__wrap_epoll_ctl(epfd, op, fd, event);
            break;
        }
        case SYS_epoll_wait: {
            int epfd = va_arg(ap, int);
            struct epoll_event *events = va_arg(ap, struct epoll_event *);
            int maxevents = va_arg(ap, int);
            int timeout = va_arg(ap, int);
            result = (long)__wrap_epoll_wait(epfd, events, maxevents, timeout);
            break;
        }
        case SYS_epoll_pwait: {
            int epfd = va_arg(ap, int);
            struct epoll_event *events = va_arg(ap, struct epoll_event *);
            int maxevents = va_arg(ap, int);
            int timeout = va_arg(ap, int);
            const sigset_t *sigmask = va_arg(ap, const sigset_t *);
            (void)va_arg(ap, size_t);
            result = (long)__wrap_epoll_pwait(epfd, events, maxevents, timeout, sigmask);
            break;
        }
        case SYS_epoll_pwait2: {
            int epfd = va_arg(ap, int);
            struct epoll_event *events = va_arg(ap, struct epoll_event *);
            int maxevents = va_arg(ap, int);
            const struct timespec *timeout = va_arg(ap, const struct timespec *);
            const sigset_t *sigmask = va_arg(ap, const sigset_t *);
            (void)va_arg(ap, size_t);
            result = (long)__wrap_epoll_pwait2(epfd, events, maxevents, timeout, sigmask);
            break;
        }
        case SYS_socket: {
            int domain = va_arg(ap, int);
            int type = va_arg(ap, int);
            int protocol = va_arg(ap, int);
            result = (long)__wrap_socket(domain, type, protocol);
            break;
        }
        case SYS_connect: {
            int fd = va_arg(ap, int);
            const struct sockaddr *addr = va_arg(ap, const struct sockaddr *);
            socklen_t addrlen = (socklen_t)va_arg(ap, int);
            result = (long)__wrap_connect(fd, addr, addrlen);
            break;
        }
        case SYS_accept: {
            int fd = va_arg(ap, int);
            struct sockaddr *addr = va_arg(ap, struct sockaddr *);
            socklen_t *addrlen = va_arg(ap, socklen_t *);
            result = (long)__wrap_accept(fd, addr, addrlen);
            break;
        }
        case SYS_accept4: {
            int fd = va_arg(ap, int);
            struct sockaddr *addr = va_arg(ap, struct sockaddr *);
            socklen_t *addrlen = va_arg(ap, socklen_t *);
            int flags = va_arg(ap, int);
            result = (long)__wrap_accept4(fd, addr, addrlen, flags);
            break;
        }
        case SYS_sendto: {
            int fd = va_arg(ap, int);
            const void *buf = va_arg(ap, const void *);
            size_t length = va_arg(ap, size_t);
            int flags = va_arg(ap, int);
            const struct sockaddr *addr = va_arg(ap, const struct sockaddr *);
            socklen_t addrlen = (socklen_t)va_arg(ap, int);
            result = (long)__wrap_sendto(fd, buf, length, flags, addr, addrlen);
            break;
        }
        case SYS_recvfrom: {
            int fd = va_arg(ap, int);
            void *buf = va_arg(ap, void *);
            size_t length = va_arg(ap, size_t);
            int flags = va_arg(ap, int);
            struct sockaddr *addr = va_arg(ap, struct sockaddr *);
            socklen_t *addrlen = va_arg(ap, socklen_t *);
            result = (long)__wrap_recvfrom(fd, buf, length, flags, addr, addrlen);
            break;
        }
        case SYS_sendmsg: {
            int fd = va_arg(ap, int);
            const struct msghdr *msg = va_arg(ap, const struct msghdr *);
            int flags = va_arg(ap, int);
            result = (long)__wrap_sendmsg(fd, msg, flags);
            break;
        }
        case SYS_recvmsg: {
            int fd = va_arg(ap, int);
            struct msghdr *msg = va_arg(ap, struct msghdr *);
            int flags = va_arg(ap, int);
            result = (long)__wrap_recvmsg(fd, msg, flags);
            break;
        }
        case SYS_sendmmsg: {
            int fd = va_arg(ap, int);
            void *messages = va_arg(ap, void *);
            unsigned int vlen = va_arg(ap, unsigned int);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)__wrap_sendmmsg(fd, messages, vlen, flags);
            break;
        }
        case SYS_recvmmsg: {
            int fd = va_arg(ap, int);
            void *messages = va_arg(ap, void *);
            unsigned int vlen = va_arg(ap, unsigned int);
            unsigned int flags = va_arg(ap, unsigned int);
            struct timespec *timeout = va_arg(ap, struct timespec *);
            result = (long)__wrap_recvmmsg(fd, messages, vlen, flags, timeout);
            break;
        }
        case SYS_shutdown: {
            int fd = va_arg(ap, int);
            int how = va_arg(ap, int);
            result = (long)__wrap_shutdown(fd, how);
            break;
        }
        case SYS_bind: {
            int fd = va_arg(ap, int);
            const struct sockaddr *addr = va_arg(ap, const struct sockaddr *);
            socklen_t addrlen = (socklen_t)va_arg(ap, int);
            result = (long)__wrap_bind(fd, addr, addrlen);
            break;
        }
        case SYS_listen: {
            int fd = va_arg(ap, int);
            int backlog = va_arg(ap, int);
            result = (long)__wrap_listen(fd, backlog);
            break;
        }
        case SYS_getsockname: {
            int fd = va_arg(ap, int);
            struct sockaddr *addr = va_arg(ap, struct sockaddr *);
            socklen_t *addrlen = va_arg(ap, socklen_t *);
            result = (long)__wrap_getsockname(fd, addr, addrlen);
            break;
        }
        case SYS_getpeername: {
            int fd = va_arg(ap, int);
            struct sockaddr *addr = va_arg(ap, struct sockaddr *);
            socklen_t *addrlen = va_arg(ap, socklen_t *);
            result = (long)__wrap_getpeername(fd, addr, addrlen);
            break;
        }
        case SYS_socketpair: {
            int domain = va_arg(ap, int);
            int type = va_arg(ap, int);
            int protocol = va_arg(ap, int);
            int *sv = va_arg(ap, int *);
            result = (long)__wrap_socketpair(domain, type, protocol, sv);
            break;
        }
        case SYS_setsockopt: {
            int fd = va_arg(ap, int);
            int level = va_arg(ap, int);
            int option = va_arg(ap, int);
            const void *value = va_arg(ap, const void *);
            socklen_t value_len = (socklen_t)va_arg(ap, int);
            result = (long)__wrap_setsockopt(fd, level, option, value, value_len);
            break;
        }
        case SYS_getsockopt: {
            int fd = va_arg(ap, int);
            int level = va_arg(ap, int);
            int option = va_arg(ap, int);
            void *value = va_arg(ap, void *);
            socklen_t *value_len = va_arg(ap, socklen_t *);
            result = (long)__wrap_getsockopt(fd, level, option, value, value_len);
            break;
        }
        case SYS_truncate: {
            const char *path = va_arg(ap, const char *);
            off_t length = (off_t)va_arg(ap, long);
            result = (long)__wrap_truncate(path, length);
            break;
        }
        case SYS_ftruncate: {
            int fd = va_arg(ap, int);
            off_t length = (off_t)va_arg(ap, long);
            result = (long)__wrap_ftruncate(fd, length);
            break;
        }
        case SYS_stat: {
            const char *path = va_arg(ap, const char *);
            struct stat *st = va_arg(ap, struct stat *);
            result = (long)stat(path, st);
            break;
        }
        case SYS_lstat: {
            const char *path = va_arg(ap, const char *);
            struct stat *st = va_arg(ap, struct stat *);
            result = (long)lstat(path, st);
            break;
        }
        case SYS_fstat: {
            int fd = va_arg(ap, int);
            struct stat *st = va_arg(ap, struct stat *);
            result = (long)fstat(fd, st);
            break;
        }
        case SYS_getdents:
        case SYS_getdents64: {
            int fd = va_arg(ap, int);
            void *buf = va_arg(ap, void *);
            size_t count = va_arg(ap, size_t);
            result = (long)__wrap_getdents64(fd, buf, count);
            break;
        }
        case SYS_newfstatat: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            struct stat *st = va_arg(ap, struct stat *);
            int flags = va_arg(ap, int);
            result = path != NULL && path[0] == 0 && (flags & AT_EMPTY_PATH) != 0
                ? (long)fstat(dirfd, st)
                : (long)fstatat(dirfd, path, st, flags);
            break;
        }
        case SYS_statfs: {
            const char *path = va_arg(ap, const char *);
            void *buf = va_arg(ap, void *);
            result = (long)__wrap_statfs(path, buf);
            break;
        }
        case SYS_fstatfs: {
            int fd = va_arg(ap, int);
            void *buf = va_arg(ap, void *);
            result = (long)__wrap_fstatfs(fd, buf);
            break;
        }
        case SYS_statx: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            int flags = va_arg(ap, int);
            unsigned int mask = va_arg(ap, unsigned int);
            void *buf = va_arg(ap, void *);
            result = (long)__wrap_statx(dirfd, path, flags, mask, buf);
            break;
        }
        case SYS_cachestat: {
            int fd = va_arg(ap, int);
            const void *range = va_arg(ap, const void *);
            void *cstat = va_arg(ap, void *);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)__wrap_cachestat(fd, range, cstat, flags);
            break;
        }
        case SYS_statmount: {
            const struct sinomo_mnt_id_req *req = va_arg(ap, const struct sinomo_mnt_id_req *);
            struct sinomo_statmount *mount = va_arg(ap, struct sinomo_statmount *);
            size_t size = va_arg(ap, size_t);
            unsigned int flags = va_arg(ap, unsigned int);
            result = sinomo_statmount(req, mount, size, flags);
            break;
        }
        case SYS_listmount: {
            const struct sinomo_mnt_id_req *req = va_arg(ap, const struct sinomo_mnt_id_req *);
            uint64_t *ids = va_arg(ap, uint64_t *);
            size_t count = va_arg(ap, size_t);
            unsigned int flags = va_arg(ap, unsigned int);
            result = sinomo_listmount(req, ids, count, flags);
            break;
        }
        case SYS_name_to_handle_at: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            void *handle = va_arg(ap, void *);
            int *mount_id = va_arg(ap, int *);
            int flags = va_arg(ap, int);
            result = (long)__wrap_name_to_handle_at(dirfd, path, handle, mount_id, flags);
            break;
        }
        case SYS_open_by_handle_at: {
            int mount_fd = va_arg(ap, int);
            void *handle = va_arg(ap, void *);
            int flags = va_arg(ap, int);
            result = (long)__wrap_open_by_handle_at(mount_fd, handle, flags);
            break;
        }
        case SYS_setxattr: {
            const char *path = va_arg(ap, const char *);
            const char *name = va_arg(ap, const char *);
            const void *value = va_arg(ap, const void *);
            size_t size = va_arg(ap, size_t);
            int flags = va_arg(ap, int);
            result = (long)__wrap_setxattr(path, name, value, size, flags);
            break;
        }
        case SYS_lsetxattr: {
            const char *path = va_arg(ap, const char *);
            const char *name = va_arg(ap, const char *);
            const void *value = va_arg(ap, const void *);
            size_t size = va_arg(ap, size_t);
            int flags = va_arg(ap, int);
            result = (long)__wrap_lsetxattr(path, name, value, size, flags);
            break;
        }
        case SYS_fsetxattr: {
            int fd = va_arg(ap, int);
            const char *name = va_arg(ap, const char *);
            const void *value = va_arg(ap, const void *);
            size_t size = va_arg(ap, size_t);
            int flags = va_arg(ap, int);
            result = (long)__wrap_fsetxattr(fd, name, value, size, flags);
            break;
        }
        case SYS_getxattr: {
            const char *path = va_arg(ap, const char *);
            const char *name = va_arg(ap, const char *);
            void *value = va_arg(ap, void *);
            size_t size = va_arg(ap, size_t);
            result = (long)__wrap_getxattr(path, name, value, size);
            break;
        }
        case SYS_lgetxattr: {
            const char *path = va_arg(ap, const char *);
            const char *name = va_arg(ap, const char *);
            void *value = va_arg(ap, void *);
            size_t size = va_arg(ap, size_t);
            result = (long)__wrap_lgetxattr(path, name, value, size);
            break;
        }
        case SYS_fgetxattr: {
            int fd = va_arg(ap, int);
            const char *name = va_arg(ap, const char *);
            void *value = va_arg(ap, void *);
            size_t size = va_arg(ap, size_t);
            result = (long)__wrap_fgetxattr(fd, name, value, size);
            break;
        }
        case SYS_listxattr: {
            const char *path = va_arg(ap, const char *);
            char *list = va_arg(ap, char *);
            size_t size = va_arg(ap, size_t);
            result = (long)__wrap_listxattr(path, list, size);
            break;
        }
        case SYS_llistxattr: {
            const char *path = va_arg(ap, const char *);
            char *list = va_arg(ap, char *);
            size_t size = va_arg(ap, size_t);
            result = (long)__wrap_llistxattr(path, list, size);
            break;
        }
        case SYS_flistxattr: {
            int fd = va_arg(ap, int);
            char *list = va_arg(ap, char *);
            size_t size = va_arg(ap, size_t);
            result = (long)__wrap_flistxattr(fd, list, size);
            break;
        }
        case SYS_removexattr: {
            const char *path = va_arg(ap, const char *);
            const char *name = va_arg(ap, const char *);
            result = (long)__wrap_removexattr(path, name);
            break;
        }
        case SYS_lremovexattr: {
            const char *path = va_arg(ap, const char *);
            const char *name = va_arg(ap, const char *);
            result = (long)__wrap_lremovexattr(path, name);
            break;
        }
        case SYS_fremovexattr: {
            int fd = va_arg(ap, int);
            const char *name = va_arg(ap, const char *);
            result = (long)__wrap_fremovexattr(fd, name);
            break;
        }
        case SYS_access: {
            const char *path = va_arg(ap, const char *);
            int mode = va_arg(ap, int);
            result = (long)__wrap_access(path, mode);
            break;
        }
        case SYS_faccessat: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            int mode = va_arg(ap, int);
            result = (long)__wrap_faccessat(dirfd, path, mode, 0);
            break;
        }
        case SYS_faccessat2: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            int mode = va_arg(ap, int);
            int flags = va_arg(ap, int);
            result = (long)__wrap_faccessat(dirfd, path, mode, flags);
            break;
        }
        case SYS_mkdir: {
            const char *path = va_arg(ap, const char *);
            mode_t mode = (mode_t)va_arg(ap, int);
            result = (long)__wrap_mkdir(path, mode);
            break;
        }
        case SYS_mkdirat: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            mode_t mode = (mode_t)va_arg(ap, int);
            result = (long)__wrap_mkdirat(dirfd, path, mode);
            break;
        }
        case SYS_mknod: {
            const char *path = va_arg(ap, const char *);
            mode_t mode = (mode_t)va_arg(ap, int);
            dev_t dev = (dev_t)va_arg(ap, unsigned long);
            result = (long)__wrap_mknod(path, mode, dev);
            break;
        }
        case SYS_mknodat: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            mode_t mode = (mode_t)va_arg(ap, int);
            dev_t dev = (dev_t)va_arg(ap, unsigned long);
            result = (long)__wrap_mknodat(dirfd, path, mode, dev);
            break;
        }
        case SYS_rmdir: {
            const char *path = va_arg(ap, const char *);
            result = (long)__wrap_rmdir(path);
            break;
        }
        case SYS_unlink: {
            const char *path = va_arg(ap, const char *);
            result = (long)__wrap_unlink(path);
            break;
        }
        case SYS_unlinkat: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            int flags = va_arg(ap, int);
            result = (long)__wrap_unlinkat(dirfd, path, flags);
            break;
        }
        case SYS_rename: {
            const char *oldpath = va_arg(ap, const char *);
            const char *newpath = va_arg(ap, const char *);
            result = (long)__wrap_rename(oldpath, newpath);
            break;
        }
        case SYS_renameat: {
            int olddirfd = va_arg(ap, int);
            const char *oldpath = va_arg(ap, const char *);
            int newdirfd = va_arg(ap, int);
            const char *newpath = va_arg(ap, const char *);
            result = (long)__wrap_renameat(olddirfd, oldpath, newdirfd, newpath);
            break;
        }
        case SYS_renameat2: {
            int olddirfd = va_arg(ap, int);
            const char *oldpath = va_arg(ap, const char *);
            int newdirfd = va_arg(ap, int);
            const char *newpath = va_arg(ap, const char *);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)__wrap_renameat2(olddirfd, oldpath, newdirfd, newpath, flags);
            break;
        }
        case SYS_link: {
            const char *oldpath = va_arg(ap, const char *);
            const char *newpath = va_arg(ap, const char *);
            result = (long)__wrap_link(oldpath, newpath);
            break;
        }
        case SYS_linkat: {
            int olddirfd = va_arg(ap, int);
            const char *oldpath = va_arg(ap, const char *);
            int newdirfd = va_arg(ap, int);
            const char *newpath = va_arg(ap, const char *);
            int flags = va_arg(ap, int);
            result = (long)__wrap_linkat(olddirfd, oldpath, newdirfd, newpath, flags);
            break;
        }
        case SYS_symlink: {
            const char *target = va_arg(ap, const char *);
            const char *linkpath = va_arg(ap, const char *);
            result = (long)__wrap_symlink(target, linkpath);
            break;
        }
        case SYS_symlinkat: {
            const char *target = va_arg(ap, const char *);
            int newdirfd = va_arg(ap, int);
            const char *linkpath = va_arg(ap, const char *);
            result = (long)__wrap_symlinkat(target, newdirfd, linkpath);
            break;
        }
        case SYS_readlink: {
            const char *path = va_arg(ap, const char *);
            char *buf = va_arg(ap, char *);
            size_t bufsiz = va_arg(ap, size_t);
            result = (long)__wrap_readlink(path, buf, bufsiz);
            break;
        }
        case SYS_readlinkat: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            char *buf = va_arg(ap, char *);
            size_t bufsiz = va_arg(ap, size_t);
            result = (long)__wrap_readlinkat(dirfd, path, buf, bufsiz);
            break;
        }
        case SYS_chmod: {
            const char *path = va_arg(ap, const char *);
            mode_t mode = (mode_t)va_arg(ap, int);
            result = (long)__wrap_chmod(path, mode);
            break;
        }
        case SYS_fchmod: {
            int fd = va_arg(ap, int);
            mode_t mode = (mode_t)va_arg(ap, int);
            result = (long)__wrap_fchmod(fd, mode);
            break;
        }
        case SYS_fchmodat: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            mode_t mode = (mode_t)va_arg(ap, int);
            result = (long)__wrap_fchmodat(dirfd, path, mode, 0);
            break;
        }
        case SYS_fchmodat2: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            mode_t mode = (mode_t)va_arg(ap, int);
            int flags = va_arg(ap, int);
            result = (long)__wrap_fchmodat(dirfd, path, mode, flags);
            break;
        }
        case SYS_chown: {
            const char *path = va_arg(ap, const char *);
            uid_t uid = (uid_t)va_arg(ap, long);
            gid_t gid = (gid_t)va_arg(ap, long);
            result = (long)__wrap_chown(path, uid, gid);
            break;
        }
        case SYS_lchown: {
            const char *path = va_arg(ap, const char *);
            uid_t uid = (uid_t)va_arg(ap, long);
            gid_t gid = (gid_t)va_arg(ap, long);
            result = (long)__wrap_lchown(path, uid, gid);
            break;
        }
        case SYS_fchown: {
            int fd = va_arg(ap, int);
            uid_t uid = (uid_t)va_arg(ap, long);
            gid_t gid = (gid_t)va_arg(ap, long);
            result = (long)__wrap_fchown(fd, uid, gid);
            break;
        }
        case SYS_fchownat: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            uid_t uid = (uid_t)va_arg(ap, long);
            gid_t gid = (gid_t)va_arg(ap, long);
            int flags = va_arg(ap, int);
            result = (long)__wrap_fchownat(dirfd, path, uid, gid, flags);
            break;
        }
        case SYS_utime: {
            const char *path = va_arg(ap, const char *);
            const struct utimbuf *times = va_arg(ap, const struct utimbuf *);
            result = (long)__wrap_utime(path, times);
            break;
        }
        case SYS_utimes: {
            const char *path = va_arg(ap, const char *);
            const struct timeval *times = va_arg(ap, const struct timeval *);
            result = (long)utimes(path, times);
            break;
        }
        case SYS_futimesat: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            const struct timeval *times = va_arg(ap, const struct timeval *);
            result = (long)__wrap_futimesat(dirfd, path, times);
            break;
        }
        case SYS_utimensat: {
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            const struct timespec *times = va_arg(ap, const struct timespec *);
            int flags = va_arg(ap, int);
            result = (long)__wrap_utimensat(dirfd, path, times, flags);
            break;
        }
        case SYS_getcwd: {
            char *buf = va_arg(ap, char *);
            size_t size = va_arg(ap, size_t);
            result = __wrap_getcwd(buf, size) == NULL ? -1 : (long)buf;
            break;
        }
        case SYS_chdir: {
            const char *path = va_arg(ap, const char *);
            result = (long)__wrap_chdir(path);
            break;
        }
        case SYS_fchdir: {
            int fd = va_arg(ap, int);
            result = (long)fchdir(fd);
            break;
        }
        case SYS_chroot: {
            const char *path = va_arg(ap, const char *);
            result = (long)__wrap_chroot(path);
            break;
        }
        case SYS_umask: {
            mode_t mask = (mode_t)va_arg(ap, int);
            result = (long)__wrap_umask(mask);
            break;
        }
        case SYS_getrlimit: {
            int resource = va_arg(ap, int);
            struct rlimit *limit = va_arg(ap, struct rlimit *);
            result = (long)__wrap_getrlimit(resource, limit);
            break;
        }
        case SYS_setrlimit: {
            int resource = va_arg(ap, int);
            const struct rlimit *limit = va_arg(ap, const struct rlimit *);
            result = (long)__wrap_setrlimit(resource, limit);
            break;
        }
        case SYS_prlimit64: {
            pid_t pid = (pid_t)va_arg(ap, long);
            int resource = va_arg(ap, int);
            const struct rlimit *new_limit = va_arg(ap, const struct rlimit *);
            struct rlimit *old_limit = va_arg(ap, struct rlimit *);
            result = (long)__wrap_prlimit(pid, resource, new_limit, old_limit);
            break;
        }
        case SYS_getrusage: {
            int who = va_arg(ap, int);
            struct rusage *usage = va_arg(ap, struct rusage *);
            result = (long)__wrap_getrusage(who, usage);
            break;
        }
        case SYS_sysinfo: {
            struct sysinfo *info = va_arg(ap, struct sysinfo *);
            result = (long)__wrap_sysinfo(info);
            break;
        }
        case SYS_times: {
            struct tms *buf = va_arg(ap, struct tms *);
            result = (long)__wrap_times(buf);
            break;
        }
        case SYS_time: {
            time_t *out = va_arg(ap, time_t *);
            struct timespec ts;
            if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
                result = -1;
            } else {
                if (out != NULL) {
                    *out = ts.tv_sec;
                }
                result = (long)ts.tv_sec;
            }
            break;
        }
        case SYS_getpriority: {
            int which = va_arg(ap, int);
            id_t who = (id_t)va_arg(ap, long);
            result = (long)__wrap_getpriority(which, who);
            break;
        }
        case SYS_setpriority: {
            int which = va_arg(ap, int);
            id_t who = (id_t)va_arg(ap, long);
            int priority = va_arg(ap, int);
            result = (long)__wrap_setpriority(which, who, priority);
            break;
        }
        case SYS_ioprio_get: {
            int which = va_arg(ap, int);
            int who = va_arg(ap, int);
            result = sinomo_ioprio_get(which, who);
            break;
        }
        case SYS_ioprio_set: {
            int which = va_arg(ap, int);
            int who = va_arg(ap, int);
            int value = va_arg(ap, int);
            result = sinomo_ioprio_set(which, who, value);
            break;
        }
        case SYS_inotify_init:
            result = (long)__wrap_inotify_init();
            break;
        case SYS_inotify_init1: {
            int flags = va_arg(ap, int);
            result = (long)__wrap_inotify_init1(flags);
            break;
        }
        case SYS_inotify_add_watch: {
            int fd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            uint32_t mask = va_arg(ap, uint32_t);
            result = (long)__wrap_inotify_add_watch(fd, path, mask);
            break;
        }
        case SYS_inotify_rm_watch: {
            int fd = va_arg(ap, int);
            int wd = va_arg(ap, int);
            result = (long)__wrap_inotify_rm_watch(fd, wd);
            break;
        }
        case SYS_fanotify_init: {
            unsigned int flags = va_arg(ap, unsigned int);
            unsigned int event_f_flags = va_arg(ap, unsigned int);
            result = (long)__wrap_fanotify_init(flags, event_f_flags);
            break;
        }
        case SYS_fanotify_mark: {
            int fd = va_arg(ap, int);
            unsigned int flags = va_arg(ap, unsigned int);
            uint64_t mask = va_arg(ap, uint64_t);
            int dirfd = va_arg(ap, int);
            const char *path = va_arg(ap, const char *);
            result = (long)__wrap_fanotify_mark(fd, flags, mask, dirfd, path);
            break;
        }
        case SYS_sched_yield:
            result = (long)__wrap_sched_yield();
            break;
        case SYS_sched_getscheduler: {
            pid_t pid = (pid_t)va_arg(ap, long);
            result = (long)__wrap_sched_getscheduler(pid);
            break;
        }
        case SYS_sched_getparam: {
            pid_t pid = (pid_t)va_arg(ap, long);
            struct sched_param *param = va_arg(ap, struct sched_param *);
            result = (long)__wrap_sched_getparam(pid, param);
            break;
        }
        case SYS_sched_setparam: {
            pid_t pid = (pid_t)va_arg(ap, long);
            const struct sched_param *param = va_arg(ap, const struct sched_param *);
            result = (long)__wrap_sched_setparam(pid, param);
            break;
        }
        case SYS_sched_setscheduler: {
            pid_t pid = (pid_t)va_arg(ap, long);
            int policy = va_arg(ap, int);
            const struct sched_param *param = va_arg(ap, const struct sched_param *);
            result = (long)__wrap_sched_setscheduler(pid, policy, param);
            break;
        }
        case SYS_sched_getaffinity: {
            pid_t pid = (pid_t)va_arg(ap, long);
            size_t cpusetsize = va_arg(ap, size_t);
            void *mask = va_arg(ap, void *);
            result = (long)__wrap_sched_getaffinity(pid, cpusetsize, mask);
            break;
        }
        case SYS_sched_setaffinity: {
            pid_t pid = (pid_t)va_arg(ap, long);
            size_t cpusetsize = va_arg(ap, size_t);
            const void *mask = va_arg(ap, const void *);
            result = (long)__wrap_sched_setaffinity(pid, cpusetsize, mask);
            break;
        }
        case SYS_sched_rr_get_interval: {
            pid_t pid = (pid_t)va_arg(ap, long);
            struct timespec *interval = va_arg(ap, struct timespec *);
            result = (long)__wrap_sched_rr_get_interval(pid, interval);
            break;
        }
        case SYS_sched_getattr: {
            pid_t pid = (pid_t)va_arg(ap, long);
            struct sinomo_sched_attr *attr = va_arg(ap, struct sinomo_sched_attr *);
            unsigned int size = va_arg(ap, unsigned int);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)sinomo_sched_getattr(pid, attr, size, flags);
            break;
        }
        case SYS_sched_setattr: {
            pid_t pid = (pid_t)va_arg(ap, long);
            const struct sinomo_sched_attr *attr = va_arg(ap, const struct sinomo_sched_attr *);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)sinomo_sched_setattr(pid, attr, flags);
            break;
        }
        case SYS_set_mempolicy: {
            int mode = va_arg(ap, int);
            const unsigned long *nodemask = va_arg(ap, const unsigned long *);
            unsigned long maxnode = va_arg(ap, unsigned long);
            result = sinomo_set_mempolicy(mode, nodemask, maxnode);
            break;
        }
        case SYS_get_mempolicy: {
            int *mode = va_arg(ap, int *);
            unsigned long *nodemask = va_arg(ap, unsigned long *);
            unsigned long maxnode = va_arg(ap, unsigned long);
            void *addr = va_arg(ap, void *);
            unsigned long flags = va_arg(ap, unsigned long);
            result = sinomo_get_mempolicy(mode, nodemask, maxnode, addr, flags);
            break;
        }
        case SYS_mbind: {
            void *addr = va_arg(ap, void *);
            unsigned long len = va_arg(ap, unsigned long);
            int mode = va_arg(ap, int);
            const unsigned long *nodemask = va_arg(ap, const unsigned long *);
            unsigned long maxnode = va_arg(ap, unsigned long);
            unsigned int flags = va_arg(ap, unsigned int);
            result = sinomo_mbind(addr, len, mode, nodemask, maxnode, flags);
            break;
        }
        case SYS_migrate_pages: {
            pid_t pid = (pid_t)va_arg(ap, long);
            unsigned long maxnode = va_arg(ap, unsigned long);
            const unsigned long *old_nodes = va_arg(ap, const unsigned long *);
            const unsigned long *new_nodes = va_arg(ap, const unsigned long *);
            result = sinomo_migrate_pages(pid, maxnode, old_nodes, new_nodes);
            break;
        }
        case SYS_move_pages: {
            pid_t pid = (pid_t)va_arg(ap, long);
            unsigned long count = va_arg(ap, unsigned long);
            void **pages = va_arg(ap, void **);
            const int *nodes = va_arg(ap, const int *);
            int *status = va_arg(ap, int *);
            int flags = va_arg(ap, int);
            result = sinomo_move_pages(pid, count, pages, nodes, status, flags);
            break;
        }
        case SYS_set_mempolicy_home_node: {
            unsigned long start = va_arg(ap, unsigned long);
            unsigned long len = va_arg(ap, unsigned long);
            unsigned long home_node = va_arg(ap, unsigned long);
            unsigned long flags = va_arg(ap, unsigned long);
            result = sinomo_set_mempolicy_home_node(start, len, home_node, flags);
            break;
        }
        case SYS_prctl: {
            int option = va_arg(ap, int);
            unsigned long arg2 = va_arg(ap, unsigned long);
            unsigned long arg3 = va_arg(ap, unsigned long);
            unsigned long arg4 = va_arg(ap, unsigned long);
            unsigned long arg5 = va_arg(ap, unsigned long);
            result = (long)__wrap_prctl(option, arg2, arg3, arg4, arg5);
            break;
        }
        case SYS_seccomp: {
            unsigned int operation = va_arg(ap, unsigned int);
            unsigned int flags = va_arg(ap, unsigned int);
            void *args = va_arg(ap, void *);
            result = sinomo_seccomp(operation, flags, args);
            break;
        }
        case SYS_lsm_get_self_attr: {
            uint32_t attr = va_arg(ap, uint32_t);
            struct sinomo_lsm_ctx *ctx = va_arg(ap, struct sinomo_lsm_ctx *);
            uint32_t *size = va_arg(ap, uint32_t *);
            uint32_t flags = va_arg(ap, uint32_t);
            result = sinomo_lsm_get_self_attr(attr, ctx, size, flags);
            break;
        }
        case SYS_lsm_set_self_attr: {
            uint32_t attr = va_arg(ap, uint32_t);
            const struct sinomo_lsm_ctx *ctx = va_arg(ap, const struct sinomo_lsm_ctx *);
            uint32_t size = va_arg(ap, uint32_t);
            uint32_t flags = va_arg(ap, uint32_t);
            result = sinomo_lsm_set_self_attr(attr, ctx, size, flags);
            break;
        }
        case SYS_lsm_list_modules: {
            uint64_t *ids = va_arg(ap, uint64_t *);
            uint32_t *size = va_arg(ap, uint32_t *);
            uint32_t flags = va_arg(ap, uint32_t);
            result = sinomo_lsm_list_modules(ids, size, flags);
            break;
        }
        case SYS_arch_prctl: {
            int code = va_arg(ap, int);
            unsigned long addr = va_arg(ap, unsigned long);
            result = (long)sinomo_arch_prctl(code, addr);
            break;
        }
        case SYS_membarrier: {
            int command = va_arg(ap, int);
            unsigned int flags = va_arg(ap, unsigned int);
            int cpu_id = va_arg(ap, int);
            result = (long)sinomo_membarrier(command, flags, cpu_id);
            break;
        }
        case SYS_rseq: {
            void *rseq = va_arg(ap, void *);
            uint32_t rseq_len = va_arg(ap, uint32_t);
            int flags = va_arg(ap, int);
            uint32_t signature = va_arg(ap, uint32_t);
            result = (long)sinomo_rseq(rseq, rseq_len, flags, signature);
            break;
        }
        case SYS_getcpu: {
            unsigned int *cpu = va_arg(ap, unsigned int *);
            unsigned int *node = va_arg(ap, unsigned int *);
            (void)va_arg(ap, void *);
            unsigned int current_cpu = 0;
            int rc = sinomo_proc_getcpu(&current_cpu);
            if (rc == 0) {
                if (cpu != NULL) {
                    *cpu = current_cpu;
                }
                if (node != NULL) {
                    *node = 0;
                }
                result = 0;
            } else {
                errno = rc;
                result = -1;
            }
            break;
        }
        case SYS_nanosleep: {
            const struct timespec *request = va_arg(ap, const struct timespec *);
            struct timespec *remain = va_arg(ap, struct timespec *);
            result = (long)nanosleep(request, remain);
            break;
        }
        case SYS_getitimer: {
            int which = va_arg(ap, int);
            struct itimerval *value = va_arg(ap, struct itimerval *);
            result = (long)__wrap_getitimer(which, value);
            break;
        }
        case SYS_setitimer: {
            int which = va_arg(ap, int);
            const struct itimerval *new_value = va_arg(ap, const struct itimerval *);
            struct itimerval *old_value = va_arg(ap, struct itimerval *);
            result = (long)__wrap_setitimer(which, new_value, old_value);
            break;
        }
        case SYS_alarm: {
            unsigned int seconds = va_arg(ap, unsigned int);
            result = (long)__wrap_alarm(seconds);
            break;
        }
        case SYS_timer_create: {
            clockid_t clock_id = (clockid_t)va_arg(ap, int);
            struct sigevent *event = va_arg(ap, struct sigevent *);
            timer_t *timer_id = va_arg(ap, timer_t *);
            result = (long)__wrap_timer_create(clock_id, event, timer_id);
            break;
        }
        case SYS_timer_settime: {
            timer_t timer_id = va_arg(ap, timer_t);
            int flags = va_arg(ap, int);
            const struct itimerspec *new_value = va_arg(ap, const struct itimerspec *);
            struct itimerspec *old_value = va_arg(ap, struct itimerspec *);
            result = (long)__wrap_timer_settime(timer_id, flags, new_value, old_value);
            break;
        }
        case SYS_timer_gettime: {
            timer_t timer_id = va_arg(ap, timer_t);
            struct itimerspec *value = va_arg(ap, struct itimerspec *);
            result = (long)__wrap_timer_gettime(timer_id, value);
            break;
        }
        case SYS_timer_getoverrun: {
            timer_t timer_id = va_arg(ap, timer_t);
            result = (long)__wrap_timer_getoverrun(timer_id);
            break;
        }
        case SYS_timer_delete: {
            timer_t timer_id = va_arg(ap, timer_t);
            result = (long)__wrap_timer_delete(timer_id);
            break;
        }
        case SYS_timerfd_create: {
            clockid_t clock_id = (clockid_t)va_arg(ap, int);
            int flags = va_arg(ap, int);
            result = (long)__wrap_timerfd_create(clock_id, flags);
            break;
        }
        case SYS_timerfd_settime: {
            int fd = va_arg(ap, int);
            int flags = va_arg(ap, int);
            const struct itimerspec *new_value = va_arg(ap, const struct itimerspec *);
            struct itimerspec *old_value = va_arg(ap, struct itimerspec *);
            result = (long)__wrap_timerfd_settime(fd, flags, new_value, old_value);
            break;
        }
        case SYS_timerfd_gettime: {
            int fd = va_arg(ap, int);
            struct itimerspec *value = va_arg(ap, struct itimerspec *);
            result = (long)__wrap_timerfd_gettime(fd, value);
            break;
        }
        case SYS_clock_nanosleep: {
            clockid_t clock_id = (clockid_t)va_arg(ap, int);
            int flags = va_arg(ap, int);
            const struct timespec *request = va_arg(ap, const struct timespec *);
            struct timespec *remain = va_arg(ap, struct timespec *);
            int rc = __wrap_clock_nanosleep(clock_id, flags, request, remain);
            if (rc == 0) {
                result = 0;
            } else {
                errno = rc;
                result = -1;
            }
            break;
        }
        case SYS_gettimeofday: {
            struct timeval *tv = va_arg(ap, struct timeval *);
            void *tz = va_arg(ap, void *);
            result = (long)gettimeofday(tv, tz);
            break;
        }
        case SYS_clock_gettime: {
            clockid_t clock_id = (clockid_t)va_arg(ap, int);
            struct timespec *ts = va_arg(ap, struct timespec *);
            result = (long)clock_gettime(clock_id, ts);
            break;
        }
        case SYS_clock_getres: {
            clockid_t clock_id = (clockid_t)va_arg(ap, int);
            struct timespec *ts = va_arg(ap, struct timespec *);
            result = (long)clock_getres(clock_id, ts);
            break;
        }
        case SYS_rt_sigaction: {
            int signum = va_arg(ap, int);
            const struct sigaction *act = va_arg(ap, const struct sigaction *);
            struct sigaction *oldact = va_arg(ap, struct sigaction *);
            (void)va_arg(ap, size_t);
            result = (long)__wrap_sigaction(signum, act, oldact);
            break;
        }
        case SYS_rt_sigprocmask: {
            int how = va_arg(ap, int);
            const sigset_t *set = va_arg(ap, const sigset_t *);
            sigset_t *oldset = va_arg(ap, sigset_t *);
            (void)va_arg(ap, size_t);
            result = (long)__wrap_sigprocmask(how, set, oldset);
            break;
        }
        case SYS_rt_sigpending: {
            sigset_t *set = va_arg(ap, sigset_t *);
            (void)va_arg(ap, size_t);
            result = (long)__wrap_sigpending(set);
            break;
        }
        case SYS_rt_sigtimedwait: {
            const sigset_t *set = va_arg(ap, const sigset_t *);
            siginfo_t *info = va_arg(ap, siginfo_t *);
            const struct timespec *timeout = va_arg(ap, const struct timespec *);
            (void)va_arg(ap, size_t);
            result = (long)__wrap_sigtimedwait(set, info, timeout);
            break;
        }
        case SYS_rt_sigqueueinfo: {
            pid_t pid = (pid_t)va_arg(ap, long);
            int signum = va_arg(ap, int);
            siginfo_t *info = va_arg(ap, siginfo_t *);
            if (!sinomo_signal_valid(signum)) {
                errno = EINVAL;
                result = -1;
                break;
            }
            if (pid == __wrap_getpid() && sinomo_sigset_contains(&sinomo_signal_mask, signum)) {
                siginfo_t queued;
                if (info != NULL) {
                    queued = *info;
                } else {
                    memset(&queued, 0, sizeof(queued));
                }
                queued.si_signo = signum;
                if (queued.si_pid == 0) {
                    queued.si_pid = __wrap_getpid();
                }
                if (queued.si_uid == 0) {
                    queued.si_uid = __wrap_getuid();
                }
                sinomo_queue_pending_signal(signum, &queued);
                result = 0;
                break;
            }
            result = (long)__wrap_kill(pid, signum);
            break;
        }
        case SYS_rt_tgsigqueueinfo: {
            pid_t tgid = (pid_t)va_arg(ap, long);
            pid_t tid = (pid_t)va_arg(ap, long);
            int signum = va_arg(ap, int);
            siginfo_t *info = va_arg(ap, siginfo_t *);
            result = (long)sinomo_rt_tgsigqueueinfo(tgid, tid, signum, info);
            break;
        }
        case SYS_signalfd: {
            int fd = va_arg(ap, int);
            const sigset_t *mask = va_arg(ap, const sigset_t *);
            (void)va_arg(ap, size_t);
            result = (long)__wrap_signalfd(fd, mask, 0);
            break;
        }
        case SYS_signalfd4: {
            int fd = va_arg(ap, int);
            const sigset_t *mask = va_arg(ap, const sigset_t *);
            (void)va_arg(ap, size_t);
            int flags = va_arg(ap, int);
            result = (long)__wrap_signalfd(fd, mask, flags);
            break;
        }
        case SYS_rt_sigsuspend: {
            const sigset_t *mask = va_arg(ap, const sigset_t *);
            (void)va_arg(ap, size_t);
            result = (long)__wrap_sigsuspend(mask);
            break;
        }
        case SYS_sigaltstack: {
            const stack_t *stack = va_arg(ap, const stack_t *);
            stack_t *old_stack = va_arg(ap, stack_t *);
            result = (long)__wrap_sigaltstack(stack, old_stack);
            break;
        }
        case SYS_rt_sigreturn:
            errno = ENOSYS;
            result = -1;
            break;
        case SYS_pause:
            result = (long)__wrap_pause();
            break;
        case SYS_pread64: {
            int fd = va_arg(ap, int);
            void *buf = va_arg(ap, void *);
            size_t count = va_arg(ap, size_t);
            off_t offset = (off_t)va_arg(ap, long);
            result = (long)__wrap_pread(fd, buf, count, offset);
            break;
        }
        case SYS_pwrite64: {
            int fd = va_arg(ap, int);
            const void *buf = va_arg(ap, const void *);
            size_t count = va_arg(ap, size_t);
            off_t offset = (off_t)va_arg(ap, long);
            result = (long)__wrap_pwrite(fd, buf, count, offset);
            break;
        }
        case SYS_readv: {
            int fd = va_arg(ap, int);
            const struct iovec *iov = va_arg(ap, const struct iovec *);
            int iovcnt = va_arg(ap, int);
            result = (long)__wrap_readv(fd, iov, iovcnt);
            break;
        }
        case SYS_writev: {
            int fd = va_arg(ap, int);
            const struct iovec *iov = va_arg(ap, const struct iovec *);
            int iovcnt = va_arg(ap, int);
            result = (long)__wrap_writev(fd, iov, iovcnt);
            break;
        }
        case SYS_preadv: {
            int fd = va_arg(ap, int);
            const struct iovec *iov = va_arg(ap, const struct iovec *);
            int iovcnt = va_arg(ap, int);
            off_t offset = (off_t)va_arg(ap, long);
            result = (long)__wrap_preadv(fd, iov, iovcnt, offset);
            break;
        }
        case SYS_pwritev: {
            int fd = va_arg(ap, int);
            const struct iovec *iov = va_arg(ap, const struct iovec *);
            int iovcnt = va_arg(ap, int);
            off_t offset = (off_t)va_arg(ap, long);
            result = (long)__wrap_pwritev(fd, iov, iovcnt, offset);
            break;
        }
        case SYS_preadv2: {
            int fd = va_arg(ap, int);
            const struct iovec *iov = va_arg(ap, const struct iovec *);
            int iovcnt = va_arg(ap, int);
            off_t offset = (off_t)va_arg(ap, long);
            int flags = va_arg(ap, int);
            result = (long)__wrap_preadv2(fd, iov, iovcnt, offset, flags);
            break;
        }
        case SYS_pwritev2: {
            int fd = va_arg(ap, int);
            const struct iovec *iov = va_arg(ap, const struct iovec *);
            int iovcnt = va_arg(ap, int);
            off_t offset = (off_t)va_arg(ap, long);
            int flags = va_arg(ap, int);
            result = (long)__wrap_pwritev2(fd, iov, iovcnt, offset, flags);
            break;
        }
        case SYS_process_vm_readv: {
            pid_t pid = (pid_t)va_arg(ap, long);
            const struct iovec *local_iov = va_arg(ap, const struct iovec *);
            unsigned long local_iovcnt = va_arg(ap, unsigned long);
            const struct iovec *remote_iov = va_arg(ap, const struct iovec *);
            unsigned long remote_iovcnt = va_arg(ap, unsigned long);
            unsigned long flags = va_arg(ap, unsigned long);
            result = (long)sinomo_process_vm_readv(pid, local_iov, local_iovcnt, remote_iov, remote_iovcnt, flags);
            break;
        }
        case SYS_process_vm_writev: {
            pid_t pid = (pid_t)va_arg(ap, long);
            const struct iovec *local_iov = va_arg(ap, const struct iovec *);
            unsigned long local_iovcnt = va_arg(ap, unsigned long);
            const struct iovec *remote_iov = va_arg(ap, const struct iovec *);
            unsigned long remote_iovcnt = va_arg(ap, unsigned long);
            unsigned long flags = va_arg(ap, unsigned long);
            result = (long)sinomo_process_vm_writev(pid, local_iov, local_iovcnt, remote_iov, remote_iovcnt, flags);
            break;
        }
        case SYS_process_madvise: {
            pid_t pid = (pid_t)va_arg(ap, long);
            const struct iovec *iov = va_arg(ap, const struct iovec *);
            unsigned long iovcnt = va_arg(ap, unsigned long);
            int advice = va_arg(ap, int);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)sinomo_process_madvise(pid, iov, iovcnt, advice, flags);
            break;
        }
        case SYS_process_mrelease: {
            int pidfd = va_arg(ap, int);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)sinomo_process_mrelease(pidfd, flags);
            break;
        }
        case SYS_kcmp: {
            pid_t pid1 = (pid_t)va_arg(ap, long);
            pid_t pid2 = (pid_t)va_arg(ap, long);
            int type = va_arg(ap, int);
            unsigned long idx1 = va_arg(ap, unsigned long);
            unsigned long idx2 = va_arg(ap, unsigned long);
            result = (long)sinomo_kcmp(pid1, pid2, type, idx1, idx2);
            break;
        }
        case SYS_splice: {
            int fd_in = va_arg(ap, int);
            off_t *off_in = va_arg(ap, off_t *);
            int fd_out = va_arg(ap, int);
            off_t *off_out = va_arg(ap, off_t *);
            size_t len = va_arg(ap, size_t);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)__wrap_splice(fd_in, off_in, fd_out, off_out, len, flags);
            break;
        }
        case SYS_vmsplice: {
            int fd = va_arg(ap, int);
            const struct iovec *iov = va_arg(ap, const struct iovec *);
            unsigned long nr_segs = va_arg(ap, unsigned long);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)__wrap_vmsplice(fd, iov, nr_segs, flags);
            break;
        }
        case SYS_close_range: {
            unsigned int first = va_arg(ap, unsigned int);
            unsigned int last = va_arg(ap, unsigned int);
            int flags = va_arg(ap, int);
            result = (long)__wrap_close_range(first, last, flags);
            break;
        }
        case SYS_pipe: {
            int *pipefd = va_arg(ap, int *);
            result = (long)__wrap_pipe(pipefd);
            break;
        }
        case SYS_pipe2: {
            int *pipefd = va_arg(ap, int *);
            int flags = va_arg(ap, int);
            result = (long)__wrap_pipe2(pipefd, flags);
            break;
        }
        case SYS_dup: {
            int oldfd = va_arg(ap, int);
            result = (long)__wrap_dup(oldfd);
            break;
        }
        case SYS_dup2: {
            int oldfd = va_arg(ap, int);
            int newfd = va_arg(ap, int);
            result = (long)__wrap_dup2(oldfd, newfd);
            break;
        }
        case SYS_dup3: {
            int oldfd = va_arg(ap, int);
            int newfd = va_arg(ap, int);
            int flags = va_arg(ap, int);
            result = (long)__wrap_dup3(oldfd, newfd, flags);
            break;
        }
        case SYS_getppid:
            result = (long)__wrap_getppid();
            break;
        case SYS_getpgrp:
            result = (long)__wrap_getpgrp();
            break;
        case SYS_gettid:
            result = (long)gettid();
            break;
        case SYS_set_tid_address: {
            int *tidptr = va_arg(ap, int *);
            result = sinomo_set_tid_address(tidptr);
            break;
        }
        case SYS_set_robust_list: {
            void *head = va_arg(ap, void *);
            size_t len = va_arg(ap, size_t);
            result = (long)sinomo_set_robust_list(head, len);
            break;
        }
        case SYS_get_robust_list: {
            pid_t pid = (pid_t)va_arg(ap, long);
            void **head = va_arg(ap, void **);
            size_t *len = va_arg(ap, size_t *);
            result = (long)sinomo_get_robust_list(pid, head, len);
            break;
        }
        case SYS_getuid:
            result = (long)__wrap_getuid();
            break;
        case SYS_geteuid:
            result = (long)__wrap_geteuid();
            break;
        case SYS_getgid:
            result = (long)__wrap_getgid();
            break;
        case SYS_getegid:
            result = (long)__wrap_getegid();
            break;
        case SYS_capget: {
            struct sinomo_cap_user_header *header = va_arg(ap, struct sinomo_cap_user_header *);
            struct sinomo_cap_user_data *data = va_arg(ap, struct sinomo_cap_user_data *);
            result = (long)sinomo_capget(header, data);
            break;
        }
        case SYS_capset: {
            const struct sinomo_cap_user_header *header = va_arg(ap, const struct sinomo_cap_user_header *);
            const struct sinomo_cap_user_data *data = va_arg(ap, const struct sinomo_cap_user_data *);
            result = (long)sinomo_capset(header, data);
            break;
        }
        case SYS_getpgid: {
            pid_t pid = (pid_t)va_arg(ap, long);
            result = (long)__wrap_getpgid(pid);
            break;
        }
        case SYS_getsid: {
            pid_t pid = (pid_t)va_arg(ap, long);
            result = (long)__wrap_getsid(pid);
            break;
        }
        case SYS_setpgid: {
            pid_t pid = (pid_t)va_arg(ap, long);
            pid_t pgid = (pid_t)va_arg(ap, long);
            result = (long)__wrap_setpgid(pid, pgid);
            break;
        }
        case SYS_setsid:
            result = (long)__wrap_setsid();
            break;
        case SYS_setuid: {
            uid_t uid = (uid_t)va_arg(ap, long);
            result = (long)__wrap_setuid(uid);
            break;
        }
        case SYS_setgid: {
            gid_t gid = (gid_t)va_arg(ap, long);
            result = (long)__wrap_setgid(gid);
            break;
        }
        case SYS_setreuid: {
            uid_t ruid = (uid_t)va_arg(ap, long);
            uid_t euid = (uid_t)va_arg(ap, long);
            result = (long)setreuid(ruid, euid);
            break;
        }
        case SYS_setregid: {
            gid_t rgid = (gid_t)va_arg(ap, long);
            gid_t egid = (gid_t)va_arg(ap, long);
            result = (long)setregid(rgid, egid);
            break;
        }
        case SYS_getgroups: {
            int size = va_arg(ap, int);
            gid_t *list = va_arg(ap, gid_t *);
            result = (long)getgroups(size, list);
            break;
        }
        case SYS_setgroups: {
            size_t size = (size_t)va_arg(ap, unsigned long);
            const gid_t *list = va_arg(ap, const gid_t *);
            result = (long)setgroups(size, list);
            break;
        }
        case SYS_getresuid: {
            uid_t *ruid = va_arg(ap, uid_t *);
            uid_t *euid = va_arg(ap, uid_t *);
            uid_t *suid = va_arg(ap, uid_t *);
            result = (long)getresuid(ruid, euid, suid);
            break;
        }
        case SYS_getresgid: {
            gid_t *rgid = va_arg(ap, gid_t *);
            gid_t *egid = va_arg(ap, gid_t *);
            gid_t *sgid = va_arg(ap, gid_t *);
            result = (long)getresgid(rgid, egid, sgid);
            break;
        }
        case SYS_setresuid: {
            uid_t ruid = (uid_t)va_arg(ap, long);
            uid_t euid = (uid_t)va_arg(ap, long);
            uid_t suid = (uid_t)va_arg(ap, long);
            result = (long)setresuid(ruid, euid, suid);
            break;
        }
        case SYS_setresgid: {
            gid_t rgid = (gid_t)va_arg(ap, long);
            gid_t egid = (gid_t)va_arg(ap, long);
            gid_t sgid = (gid_t)va_arg(ap, long);
            result = (long)setresgid(rgid, egid, sgid);
            break;
        }
        case SYS_setfsuid: {
            uid_t uid = (uid_t)va_arg(ap, long);
            result = (long)__wrap_setfsuid(uid);
            break;
        }
        case SYS_setfsgid: {
            gid_t gid = (gid_t)va_arg(ap, long);
            result = (long)__wrap_setfsgid(gid);
            break;
        }
        case SYS_personality: {
            unsigned long persona = va_arg(ap, unsigned long);
            result = sinomo_personality_call(persona);
            break;
        }
        case SYS_unshare: {
            unsigned long flags = va_arg(ap, unsigned long);
            result = sinomo_unshare(flags);
            break;
        }
        case SYS_setns: {
            int fd = va_arg(ap, int);
            int nstype = va_arg(ap, int);
            result = sinomo_setns(fd, nstype);
            break;
        }
        case SYS_kill: {
            pid_t pid = (pid_t)va_arg(ap, long);
            int signum = va_arg(ap, int);
            result = (long)__wrap_kill(pid, signum);
            break;
        }
        case SYS_tkill: {
            pid_t tid = (pid_t)va_arg(ap, long);
            int signum = va_arg(ap, int);
            result = (long)sinomo_tkill(tid, signum);
            break;
        }
        case SYS_tgkill: {
            pid_t pid = (pid_t)va_arg(ap, long);
            pid_t tid = (pid_t)va_arg(ap, long);
            int signum = va_arg(ap, int);
            result = (long)sinomo_tgkill(pid, tid, signum);
            break;
        }
        case SYS_fork:
            result = (long)__wrap_fork();
            break;
        case SYS_vfork:
            result = (long)__wrap_vfork();
            break;
        case SYS_clone: {
            unsigned long flags = va_arg(ap, unsigned long);
            void *child_stack = va_arg(ap, void *);
            int *parent_tid = va_arg(ap, int *);
            int *child_tid = va_arg(ap, int *);
            unsigned long tls = va_arg(ap, unsigned long);
            result = (long)sinomo_syscall_clone(flags, child_stack, parent_tid, child_tid, tls);
            break;
        }
        case SYS_clone3: {
            const struct sinomo_clone_args *args = va_arg(ap, const struct sinomo_clone_args *);
            size_t size = va_arg(ap, size_t);
            result = (long)sinomo_syscall_clone3(args, size);
            break;
        }
        case SYS_pidfd_open: {
            pid_t pid = (pid_t)va_arg(ap, long);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)sinomo_pidfd_open(pid, flags);
            break;
        }
        case SYS_pidfd_send_signal: {
            int pidfd = va_arg(ap, int);
            int signum = va_arg(ap, int);
            const siginfo_t *info = va_arg(ap, const siginfo_t *);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)sinomo_pidfd_send_signal(pidfd, signum, info, flags);
            break;
        }
        case SYS_pidfd_getfd: {
            int pidfd = va_arg(ap, int);
            int targetfd = va_arg(ap, int);
            unsigned int flags = va_arg(ap, unsigned int);
            result = (long)sinomo_pidfd_getfd(pidfd, targetfd, flags);
            break;
        }
        case SYS_eventfd: {
            unsigned int initval = (unsigned int)va_arg(ap, long);
            result = (long)__wrap_eventfd(initval, 0);
            break;
        }
        case SYS_eventfd2: {
            unsigned int initval = (unsigned int)va_arg(ap, long);
            int flags = va_arg(ap, int);
            result = (long)__wrap_eventfd(initval, flags);
            break;
        }
        default:
            errno = ENOSYS;
            result = -1;
            break;
    }

    va_end(ap);
    return result;
}

static int sinomo_wait_status(__wasi_join_status_t *status) {
    switch (status->tag) {
    case __WASI_JOIN_STATUS_TYPE_NOTHING:
        return 0;
    case __WASI_JOIN_STATUS_TYPE_EXIT_NORMAL:
        return ((int)status->u.exit_normal & 0xff) << 8;
    case __WASI_JOIN_STATUS_TYPE_EXIT_SIGNAL:
        return (int)status->u.exit_signal.signal & 0x7f;
    case __WASI_JOIN_STATUS_TYPE_STOPPED:
        return (((int)status->u.stopped & 0xff) << 8) | 0x7f;
    default:
        errno = EINVAL;
        return -1;
    }
}

static pid_t sinomo_take_continued_child(pid_t pid) {
#ifdef WCONTINUED
    int continued_pid = 0;
    int rc = sinomo_proc_child_continued_status((int)pid, 1, &continued_pid);
    if (rc != 0) {
        errno = rc;
        return (pid_t)-1;
    }
    return (pid_t)continued_pid;
#else
    (void)pid;
    return (pid_t)0;
#endif
}

static pid_t sinomo_peek_continued_child(pid_t pid) {
#ifdef WCONTINUED
    int continued_pid = 0;
    int rc = sinomo_proc_child_continued_status((int)pid, 0, &continued_pid);
    if (rc != 0) {
        errno = rc;
        return (pid_t)-1;
    }
    return (pid_t)continued_pid;
#else
    (void)pid;
    return (pid_t)0;
#endif
}

static pid_t sinomo_wait_continued_child(pid_t pid, int consume, int nonblocking) {
#ifdef WCONTINUED
    int continued_pid = 0;
    int rc = sinomo_proc_child_continued_status_wait((int)pid, consume, nonblocking, &continued_pid);
    if (rc != 0) {
        errno = rc;
        return (pid_t)-1;
    }
    return (pid_t)continued_pid;
#else
    (void)pid;
    (void)consume;
    (void)nonblocking;
    return (pid_t)0;
#endif
}

pid_t __wrap_waitpid(pid_t pid, int *status_ptr, int options) {
#ifdef WCONTINUED
    if ((options & WCONTINUED) != 0) {
        pid_t continued_pid = sinomo_take_continued_child(pid);
        if (continued_pid < 0) {
            return -1;
        }
        if (continued_pid > 0) {
            if (status_ptr != NULL) {
                *status_ptr = 0xffff;
            }
            return continued_pid;
        }
    }
#endif

    __wasi_option_pid_t option_pid = {
        .tag = 1,
        .u = {.some = (__wasi_pid_t)pid},
    };
    __wasi_join_flags_t flags = 0;
    if ((options & WNOHANG) != 0) {
        flags |= __WASI_JOIN_FLAGS_NON_BLOCKING;
    }
    if ((options & WUNTRACED) != 0) {
        flags |= __WASI_JOIN_FLAGS_WAKE_STOPPED;
    }

    __wasi_join_status_t status = {0};
    __wasi_errno_t rc = __wasi_proc_join(&option_pid, flags, &status);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }

    int wait_status = sinomo_wait_status(&status);
    if (wait_status < 0) {
        return -1;
    }
    if (status_ptr != NULL) {
        *status_ptr = wait_status;
    }
    return (pid_t)option_pid.u.some;
}

pid_t __wrap_wait(int *status_ptr) {
    return __wrap_waitpid(-1, status_ptr, 0);
}

static long long sinomo_timeval_usec(const struct timeval *value) {
    return ((long long)value->tv_sec * 1000000LL) + (long long)value->tv_usec;
}

static void sinomo_usec_timeval(long long value, struct timeval *out) {
    if (value < 0) {
        value = 0;
    }
    out->tv_sec = (time_t)(value / 1000000LL);
    out->tv_usec = (suseconds_t)(value % 1000000LL);
}

static void sinomo_child_rusage_delta(
    struct rusage *usage,
    const struct rusage *before,
    const struct rusage *after
) {
    memset(usage, 0, sizeof(*usage));
    sinomo_usec_timeval(
        sinomo_timeval_usec(&after->ru_utime) - sinomo_timeval_usec(&before->ru_utime),
        &usage->ru_utime
    );
    sinomo_usec_timeval(
        sinomo_timeval_usec(&after->ru_stime) - sinomo_timeval_usec(&before->ru_stime),
        &usage->ru_stime
    );
    usage->ru_maxrss = after->ru_maxrss;
}

pid_t __wrap_wait3(int *status_ptr, int options, struct rusage *usage) {
    struct rusage before;
    int have_before = usage != NULL && __wrap_getrusage(RUSAGE_CHILDREN, &before) == 0;
    pid_t pid = __wrap_waitpid(-1, status_ptr, options);
    if (pid > 0 && usage != NULL) {
        struct rusage after;
        if (have_before && __wrap_getrusage(RUSAGE_CHILDREN, &after) == 0) {
            sinomo_child_rusage_delta(usage, &before, &after);
        } else {
            memset(usage, 0, sizeof(*usage));
        }
    }
    return pid;
}

pid_t __wrap_wait4(pid_t pid, int *status_ptr, int options, struct rusage *usage) {
    struct rusage before;
    int have_before = usage != NULL && __wrap_getrusage(RUSAGE_CHILDREN, &before) == 0;
    pid_t waited = __wrap_waitpid(pid, status_ptr, options);
    if (waited > 0 && usage != NULL) {
        struct rusage after;
        if (have_before && __wrap_getrusage(RUSAGE_CHILDREN, &after) == 0) {
            sinomo_child_rusage_delta(usage, &before, &after);
        } else {
            memset(usage, 0, sizeof(*usage));
        }
    }
    return waited;
}

int __wrap_waitid(idtype_t idtype, id_t id, siginfo_t *infop, int options) {
    if (infop == NULL) {
        errno = EFAULT;
        return -1;
    }
    if ((options & (WEXITED | WSTOPPED | WCONTINUED)) == 0) {
        errno = EINVAL;
        return -1;
    }
    __wasi_option_pid_t option_pid = {
        .tag = 1,
        .u = {.some = -1},
    };
    switch (idtype) {
    case P_ALL:
        option_pid.u.some = -1;
        break;
    case P_PID:
        option_pid.u.some = (__wasi_pid_t)id;
        break;
    case P_PGID:
        option_pid.u.some = id == 0 ? 0 : -((__wasi_pid_t)id);
        break;
    case P_PIDFD: {
        pid_t pid = sinomo_pidfd_lookup((int)id);
        if (pid < 0) {
            return -1;
        }
        option_pid.u.some = (__wasi_pid_t)pid;
        break;
    }
    default:
        errno = EINVAL;
        return -1;
    }

#ifdef WCONTINUED
    if ((options & WCONTINUED) != 0) {
        int continued_only = (options & (WEXITED | WSTOPPED)) == 0;
        int continued_nonblocking = (options & WNOHANG) != 0 || !continued_only;
        pid_t continued_pid = continued_only
            ? sinomo_wait_continued_child(
                (pid_t)option_pid.u.some,
                (options & WNOWAIT) == 0,
                continued_nonblocking
            )
            : ((options & WNOWAIT) != 0
                ? sinomo_peek_continued_child((pid_t)option_pid.u.some)
                : sinomo_take_continued_child((pid_t)option_pid.u.some));
        if (continued_pid < 0) {
            return -1;
        }
        if (continued_pid > 0) {
            memset(infop, 0, sizeof(*infop));
            infop->si_signo = SIGCHLD;
            infop->si_pid = continued_pid;
            infop->si_uid = __wrap_getuid();
            infop->si_code = CLD_CONTINUED;
            infop->si_status = SIGCONT;
            return 0;
        }
        if (continued_only) {
            memset(infop, 0, sizeof(*infop));
            return 0;
        }
    }
#endif

    __wasi_join_flags_t flags = 0;
    if ((options & WNOHANG) != 0) {
        flags |= __WASI_JOIN_FLAGS_NON_BLOCKING;
    }
    if ((options & WSTOPPED) != 0) {
        flags |= __WASI_JOIN_FLAGS_WAKE_STOPPED;
    }

    __wasi_join_status_t status = {0};
    uint32_t status_kinds = 0;
    if ((options & WEXITED) != 0) {
        status_kinds |= SINOMO_WAIT_STATUS_EXITED;
    }
    if ((options & WSTOPPED) != 0) {
        status_kinds |= SINOMO_WAIT_STATUS_STOPPED;
    }
    if ((options & WCONTINUED) != 0) {
        status_kinds |= SINOMO_WAIT_STATUS_CONTINUED;
    }
    __wasi_errno_t rc = ((options & WNOWAIT) != 0 || (options & WEXITED) == 0 || (options & WCONTINUED) != 0)
        ? sinomo_proc_wait_status_select(&option_pid, flags, status_kinds, &status)
        : __wasi_proc_join(&option_pid, flags, &status);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }

    memset(infop, 0, sizeof(*infop));
    if (status.tag == __WASI_JOIN_STATUS_TYPE_NOTHING) {
        if ((options & WCONTINUED) != 0 && option_pid.u.some > 0) {
            if ((options & WNOWAIT) == 0) {
                pid_t consumed = sinomo_take_continued_child((pid_t)option_pid.u.some);
                if (consumed < 0) {
                    return -1;
                }
            }
            infop->si_signo = SIGCHLD;
            infop->si_pid = (pid_t)option_pid.u.some;
            infop->si_uid = __wrap_getuid();
            infop->si_code = CLD_CONTINUED;
            infop->si_status = SIGCONT;
        }
        return 0;
    }
    if ((options & WCONTINUED) != 0 && (options & WNOWAIT) == 0 && status.tag != __WASI_JOIN_STATUS_TYPE_STOPPED) {
        __wasi_option_pid_t reap_pid = {.tag = 1, .u = {.some = option_pid.u.some}};
        rc = __wasi_proc_join(&reap_pid, flags, &status);
        if (rc != 0) {
            errno = (int)rc;
            return -1;
        }
        option_pid.u.some = reap_pid.u.some;
    }

    infop->si_signo = SIGCHLD;
    infop->si_pid = (pid_t)option_pid.u.some;
    infop->si_uid = __wrap_getuid();
    infop->si_utime = 0;
    infop->si_stime = 0;

    switch (status.tag) {
    case __WASI_JOIN_STATUS_TYPE_EXIT_NORMAL:
        if ((options & WEXITED) == 0) {
            memset(infop, 0, sizeof(*infop));
            return 0;
        }
        infop->si_code = CLD_EXITED;
        infop->si_status = (int)status.u.exit_normal;
        return 0;
    case __WASI_JOIN_STATUS_TYPE_EXIT_SIGNAL:
        if ((options & WEXITED) == 0) {
            memset(infop, 0, sizeof(*infop));
            return 0;
        }
        infop->si_code = CLD_KILLED;
        infop->si_status = (int)status.u.exit_signal.signal;
        return 0;
    case __WASI_JOIN_STATUS_TYPE_STOPPED:
        if ((options & WSTOPPED) == 0) {
            memset(infop, 0, sizeof(*infop));
            return 0;
        }
        infop->si_code = CLD_STOPPED;
        infop->si_status = (int)status.u.stopped;
        return 0;
    default:
        errno = EINVAL;
        return -1;
    }
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

static unsigned int sinomo_memfd_counter = 0;

#define SINOMO_MEMFD_SEAL_SLOTS 256
struct sinomo_memfd_seal_entry {
    dev_t dev;
    ino_t ino;
    int seals;
};
static struct sinomo_memfd_seal_entry sinomo_memfd_seals[SINOMO_MEMFD_SEAL_SLOTS];

static struct sinomo_memfd_seal_entry *sinomo_memfd_seal_entry_for_fd(int fd, int create, int initial_seals) {
    struct stat st;
    if (fstat(fd, &st) != 0) {
        return NULL;
    }
    size_t empty = SINOMO_MEMFD_SEAL_SLOTS;
    for (size_t index = 0; index < SINOMO_MEMFD_SEAL_SLOTS; index++) {
        if (sinomo_memfd_seals[index].ino == st.st_ino && sinomo_memfd_seals[index].dev == st.st_dev) {
            return &sinomo_memfd_seals[index];
        }
        if (empty == SINOMO_MEMFD_SEAL_SLOTS && sinomo_memfd_seals[index].ino == 0) {
            empty = index;
        }
    }
    if (!create || empty == SINOMO_MEMFD_SEAL_SLOTS) {
        errno = ENODEV;
        return NULL;
    }
    sinomo_memfd_seals[empty].dev = st.st_dev;
    sinomo_memfd_seals[empty].ino = st.st_ino;
    sinomo_memfd_seals[empty].seals = initial_seals;
    return &sinomo_memfd_seals[empty];
}

static int sinomo_memfd_get_seals(int fd) {
    struct sinomo_memfd_seal_entry *entry = sinomo_memfd_seal_entry_for_fd(fd, 0, 0);
    if (entry == NULL) {
        return -1;
    }
    return entry->seals;
}

static int sinomo_memfd_add_seals(int fd, int seals) {
    int supported = F_SEAL_SEAL | F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE | F_SEAL_FUTURE_WRITE;
    if ((seals & ~supported) != 0) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_memfd_seal_entry *entry = sinomo_memfd_seal_entry_for_fd(fd, 0, 0);
    if (entry == NULL) {
        return -1;
    }
    if ((entry->seals & F_SEAL_SEAL) != 0) {
        errno = EPERM;
        return -1;
    }
    entry->seals |= seals;
    return 0;
}

static void sinomo_memfd_forget(int fd) {
    (void)fd;
}

int __wrap_memfd_create(const char *name, unsigned int flags) {
    if (name == NULL) {
        errno = EFAULT;
        return -1;
    }
    if ((flags & ~(MFD_CLOEXEC | MFD_ALLOW_SEALING)) != 0) {
        errno = EINVAL;
        return -1;
    }
    if (strlen(name) > 249) {
        errno = EINVAL;
        return -1;
    }
    if (sinomo_mkdir_if_missing("/tmp") != 0) {
        return -1;
    }

    struct timespec now = {0, 0};
    if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
        return -1;
    }
    for (unsigned int attempt = 0; attempt < 128; attempt++) {
        unsigned int counter = sinomo_memfd_counter++;
        char path[128];
        int path_len = snprintf(
            path,
            sizeof(path),
            "/tmp/.sinomo-memfd-%ld-%lld-%u-%u",
            (long)__wrap_getpid(),
            (long long)now.tv_nsec,
            counter,
            attempt
        );
        if (path_len <= 0 || (size_t)path_len >= sizeof(path)) {
            errno = ENAMETOOLONG;
            return -1;
        }
        int open_flags = O_RDWR | O_CREAT | O_EXCL;
#ifdef O_CLOEXEC
        if ((flags & MFD_CLOEXEC) != 0) {
            open_flags |= O_CLOEXEC;
        }
#endif
        int fd = open(path, open_flags, 0600);
        if (fd < 0) {
            if (errno == EEXIST) {
                continue;
            }
            return -1;
        }
        if (unlink(path) != 0) {
            int saved_errno = errno;
            close(fd);
            errno = saved_errno;
            return -1;
        }
#ifndef O_CLOEXEC
        if ((flags & MFD_CLOEXEC) != 0 && sinomo_apply_fd_flags(fd, O_CLOEXEC) != 0) {
            int saved_errno = errno;
            close(fd);
            errno = saved_errno;
            return -1;
        }
#endif
        int initial_seals = (flags & MFD_ALLOW_SEALING) != 0 ? 0 : F_SEAL_SEAL;
        if (sinomo_memfd_seal_entry_for_fd(fd, 1, initial_seals) == NULL) {
            int saved_errno = errno;
            close(fd);
            errno = saved_errno;
            return -1;
        }
        return fd;
    }
    errno = EEXIST;
    return -1;
}

int __wrap_memfd_secret(unsigned int flags) {
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    int fd = __wrap_memfd_create("secretmem", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (fd < 0) {
        return -1;
    }
    (void)sinomo_memfd_add_seals(fd, F_SEAL_SHRINK | F_SEAL_SEAL);
    return fd;
}

static int sinomo_shm_path(const char *name, char *path, size_t path_len) {
    if (name == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (name[0] != '/' || name[1] == 0 || strchr(name + 1, '/') != NULL) {
        errno = EINVAL;
        return -1;
    }
    if (sinomo_mkdir_if_missing("/dev") != 0 || sinomo_mkdir_if_missing("/dev/shm") != 0) {
        return -1;
    }
    int written = snprintf(path, path_len, "/dev/shm/%s", name + 1);
    if (written < 0 || (size_t)written >= path_len) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

int __wrap_shm_open(const char *name, int oflag, mode_t mode) {
    char path[4096];
    if (sinomo_shm_path(name, path, sizeof(path)) != 0) {
        return -1;
    }
    int flags = oflag;
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    int fd = open(path, flags, mode);
#ifndef O_CLOEXEC
    if (fd >= 0 && sinomo_apply_fd_flags(fd, O_CLOEXEC) != 0) {
        int saved_errno = errno;
        close(fd);
        errno = saved_errno;
        return -1;
    }
#endif
    return fd;
}

int __wrap_shm_unlink(const char *name) {
    char path[4096];
    if (sinomo_shm_path(name, path, sizeof(path)) != 0) {
        return -1;
    }
    return unlink(path);
}

struct sinomo_linux_dirent64 {
    uint64_t d_ino;
    int64_t d_off;
    unsigned short d_reclen;
    unsigned char d_type;
    char d_name[];
};

ssize_t __wrap_getdents64(int fd, void *buf, size_t count) {
    if (buf == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (count > (size_t)SSIZE_MAX) {
        count = (size_t)SSIZE_MAX;
    }
    int dup_fd = dup(fd);
    if (dup_fd < 0) {
        return -1;
    }
    DIR *dir = fdopendir(dup_fd);
    if (dir == NULL) {
        int saved_errno = errno;
        close(dup_fd);
        errno = saved_errno;
        return -1;
    }

    unsigned char *out = (unsigned char *)buf;
    size_t written = 0;
    errno = 0;
    for (;;) {
        struct dirent *entry = readdir(dir);
        if (entry == NULL) {
            int saved_errno = errno;
            closedir(dir);
            if (saved_errno != 0 && written == 0) {
                errno = saved_errno;
                return -1;
            }
            return (ssize_t)written;
        }

        size_t name_len = strlen(entry->d_name) + 1;
        size_t reclen = sizeof(struct sinomo_linux_dirent64) + name_len;
        reclen = (reclen + 7U) & ~((size_t)7U);
        if (reclen > count - written) {
            if (written == 0) {
                closedir(dir);
                errno = EINVAL;
                return -1;
            }
            closedir(dir);
            return (ssize_t)written;
        }

        struct sinomo_linux_dirent64 *linux_entry = (struct sinomo_linux_dirent64 *)(out + written);
        memset(linux_entry, 0, reclen);
        linux_entry->d_ino = (uint64_t)entry->d_ino;
        linux_entry->d_off = (int64_t)telldir(dir);
        linux_entry->d_reclen = (unsigned short)reclen;
        linux_entry->d_type = entry->d_type;
        memcpy(linux_entry->d_name, entry->d_name, name_len);
        written += reclen;
    }
}

int __wrap_fcntl(int fd, int cmd, ...) {
    va_list ap;
    va_start(ap, cmd);

    if (cmd == F_GET_SEALS) {
        va_end(ap);
        return sinomo_memfd_get_seals(fd);
    }
    if (cmd == F_GETPIPE_SZ) {
        va_end(ap);
        return sinomo_pipe_get_size(fd);
    }

    if (cmd == F_GETLK || cmd == F_SETLK || cmd == F_SETLKW || cmd == F_OFD_GETLK || cmd == F_OFD_SETLK || cmd == F_OFD_SETLKW) {
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

        if (cmd == F_GETLK || cmd == F_OFD_GETLK) {
            if (access(path, F_OK) == 0) {
                lock->l_type = F_WRLCK;
                lock->l_pid = cmd == F_OFD_GETLK ? -1 : 0;
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
            if (errno != EEXIST || cmd == F_SETLK || cmd == F_OFD_SETLK) {
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
    if (cmd == F_DUPFD_CLOEXEC) {
        int duplicated = __real_fcntl(fd, F_DUPFD, int_arg);
        if (duplicated < 0) {
            return -1;
        }
        if (__real_fcntl(duplicated, F_SETFD, FD_CLOEXEC) != 0) {
            int saved_errno = errno;
            close(duplicated);
            errno = saved_errno;
            return -1;
        }
        return duplicated;
    }
    if (cmd == F_ADD_SEALS) {
        return sinomo_memfd_add_seals(fd, int_arg);
    }
    if (cmd == F_SETPIPE_SZ) {
        return sinomo_pipe_set_size(fd, int_arg);
    }
    return __real_fcntl(fd, cmd, int_arg);
}

int __wrap_lockf(int fd, int cmd, off_t len) {
    struct flock lock = {
        .l_type = F_WRLCK,
        .l_whence = SEEK_CUR,
        .l_start = 0,
        .l_len = len,
        .l_pid = 0,
    };

    switch (cmd) {
        case F_ULOCK:
            lock.l_type = F_UNLCK;
            return __wrap_fcntl(fd, F_SETLK, &lock);
        case F_LOCK:
            return __wrap_fcntl(fd, F_SETLKW, &lock);
        case F_TLOCK:
            return __wrap_fcntl(fd, F_SETLK, &lock);
        case F_TEST:
            return __wrap_fcntl(fd, F_GETLK, &lock);
        default:
            errno = EINVAL;
            return -1;
    }
}

int __wrap_flock(int fd, int operation) {
    struct flock lock = {
        .l_type = F_WRLCK,
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0,
        .l_pid = 0,
    };

    int command = (operation & LOCK_NB) != 0 ? F_SETLK : F_SETLKW;
    int lock_kind = operation & ~LOCK_NB;
    switch (lock_kind) {
        case LOCK_SH:
            lock.l_type = F_RDLCK;
            return __wrap_fcntl(fd, command, &lock);
        case LOCK_EX:
            lock.l_type = F_WRLCK;
            return __wrap_fcntl(fd, command, &lock);
        case LOCK_UN:
            lock.l_type = F_UNLCK;
            return __wrap_fcntl(fd, F_SETLK, &lock);
        default:
            errno = EINVAL;
            return -1;
    }
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

struct sinomo_execveat_data {
    const char *path;
    char *const *argv;
    char *const *envp;
};

static int sinomo_execveat_operation(void *raw) {
    struct sinomo_execveat_data *data = raw;
    return __wrap_execve(data->path, (char *const *)data->argv, (char *const *)data->envp);
}

int __wrap_execveat(int dirfd, const char *path, char *const argv[], char *const envp[], int flags) {
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    if ((flags & ~(AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW)) != 0) {
        errno = EINVAL;
        return -1;
    }
    if ((flags & AT_EMPTY_PATH) != 0 && path[0] == 0) {
        char fd_path[64];
        snprintf(fd_path, sizeof(fd_path), "/proc/self/fd/%d", dirfd);
        return __wrap_execve(fd_path, argv, envp);
    }
    if (path[0] == '/' || dirfd == AT_FDCWD) {
        return __wrap_execve(path, argv, envp);
    }

    struct sinomo_execveat_data data = {
        .path = path,
        .argv = argv,
        .envp = envp,
    };
    return sinomo_with_dirfd_cwd(dirfd, sinomo_execveat_operation, &data);
}

int fexecve(int fd, char *const argv[], char *const envp[]) {
    return __wrap_execveat(fd, "", argv, envp, AT_EMPTY_PATH);
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
ac_cv_func_alarm=yes
ac_cv_func_a64l=yes
ac_cv_func_asctime_r=yes
ac_cv_func_asinh=yes
ac_cv_func_atanh=yes
ac_cv_func_basename=yes
ac_cv_func_erf=yes
ac_cv_func_erfc=yes
ac_cv_func_execveat=yes
ac_cv_func_expm1=yes
ac_cv_func_log1p=yes
ac_cv_func_log2=yes
ac_cv_func_clock_gettime=yes
ac_cv_func_clock_getres=yes
ac_cv_func_clearenv=yes
ac_cv_func_ctime_r=yes
ac_cv_func_clock_nanosleep=yes
ac_cv_func_clock=yes
ac_cv_func_chdir=yes
ac_cv_func_chroot=yes
ac_cv_func_chmod=yes
ac_cv_func_chown=yes
ac_cv_func_cfgetispeed=yes
ac_cv_func_cfgetospeed=yes
ac_cv_func_cfmakeraw=yes
ac_cv_func_cfsetispeed=yes
ac_cv_func_cfsetospeed=yes
ac_cv_func_cfsetspeed=yes
ac_cv_func_closelog=yes
ac_cv_func_closefrom=yes
ac_cv_func_close_range=yes
ac_cv_func_confstr=yes
ac_cv_func_copy_file_range=yes
ac_cv_func_crypt=yes
ac_cv_func_crypt_r=yes
ac_cv_func_ctermid=yes
ac_cv_func_ctermid_r=yes
ac_cv_func__flushlbf=yes
ac_cv_func___fbufsize=yes
ac_cv_func___flbf=yes
ac_cv_func___fpending=yes
ac_cv_func___fpurge=yes
ac_cv_func___freadahead=yes
ac_cv_func___freadable=yes
ac_cv_func___freading=yes
ac_cv_func___freadptr=yes
ac_cv_func___freadptrinc=yes
ac_cv_func___fseterr=yes
ac_cv_func___fsetlocking=yes
ac_cv_func___fwritable=yes
ac_cv_func___fwriting=yes
ac_cv_func_dup=yes
ac_cv_func_dup2=yes
ac_cv_func_dup3=yes
ac_cv_func_duplocale=yes
ac_cv_func_dirname=yes
ac_cv_func_drand48=yes
ac_cv_func_endspent=yes
ac_cv_func_err=yes
ac_cv_func_errx=yes
ac_cv_func_erand48=yes
ac_cv_func_eventfd=yes
ac_cv_func_eventfd_read=yes
ac_cv_func_eventfd_write=yes
ac_cv_func_inotify_add_watch=yes
ac_cv_func_inotify_init=yes
ac_cv_func_inotify_init1=yes
ac_cv_func_inotify_rm_watch=yes
ac_cv_func_epoll_create=yes
ac_cv_func_epoll_create1=yes
ac_cv_func_explicit_bzero=yes
ac_cv_func_explicit_memset=yes
ac_cv_func_faccessat=yes
ac_cv_func_feclearexcept=yes
ac_cv_func_fchdir=yes
ac_cv_func_fchmod=yes
ac_cv_func_fchmodat=yes
ac_cv_func_fchown=yes
ac_cv_func_fchownat=yes
ac_cv_func_fdatasync=yes
ac_cv_func_fegetenv=yes
ac_cv_func_fegetexceptflag=yes
ac_cv_func_fegetround=yes
ac_cv_func_feholdexcept=yes
ac_cv_func_feraiseexcept=yes
ac_cv_func_fesetenv=yes
ac_cv_func_fesetexceptflag=yes
ac_cv_func_fesetround=yes
ac_cv_func_fetestexcept=yes
ac_cv_func_feupdateenv=yes
ac_cv_func_fgetwc=yes
ac_cv_func_fgetwc_unlocked=yes
ac_cv_func_fgetspent=yes
ac_cv_func_fgetxattr=yes
ac_cv_func_fgetws=yes
ac_cv_func_fgetws_unlocked=yes
ac_cv_func_flock=yes
ac_cv_func_flistxattr=yes
ac_cv_func_fnmatch=yes
ac_cv_func_fork=yes
ac_cv_func_fpathconf=yes
ac_cv_func_freelocale=yes
ac_cv_func_fputwc=yes
ac_cv_func_fputwc_unlocked=yes
ac_cv_func_fputws=yes
ac_cv_func_fputws_unlocked=yes
ac_cv_func_fsync=yes
ac_cv_func_fstatat=yes
ac_cv_func_fstatvfs=yes
ac_cv_func_ftruncate=yes
ac_cv_func_fremovexattr=yes
ac_cv_func_fsetxattr=yes
ac_cv_func_futimens=yes
ac_cv_func_futimes=yes
ac_cv_func_futimesat=yes
ac_cv_func_fexecve=yes
ac_cv_func_getegid=yes
ac_cv_func_geteuid=yes
ac_cv_func_getgid=yes
ac_cv_func_get_avphys_pages=yes
ac_cv_func_getopt=yes
ac_cv_func_getopt_long=yes
ac_cv_func_gmtime_r=yes
ac_cv_func_getgrent=yes
ac_cv_func_getgrgid=yes
ac_cv_func_getgrgid_r=yes
ac_cv_func_getgrouplist=yes
ac_cv_func_getgrnam=yes
ac_cv_func_getgrnam_r=yes
ac_cv_func_getgroups=yes
ac_cv_func_glob=yes
ac_cv_func_getentropy=yes
ac_cv_func_getwc=yes
ac_cv_func_getwc_unlocked=yes
ac_cv_func_getwchar=yes
ac_cv_func_getwchar_unlocked=yes
ac_cv_func_gethostname=yes
ac_cv_func_getloadavg=yes
ac_cv_func_getlogin=yes
ac_cv_func_getlogin_r=yes
ac_cv_func_get_nprocs=yes
ac_cv_func_get_nprocs_conf=yes
ac_cv_func_get_phys_pages=yes
ac_cv_func_getcwd=yes
ac_cv_func_getwd=yes
ac_cv_func_hcreate=yes
ac_cv_func_hcreate_r=yes
ac_cv_func_hdestroy=yes
ac_cv_func_hdestroy_r=yes
ac_cv_func_hsearch=yes
ac_cv_func_hsearch_r=yes
ac_cv_func_getpgid=yes
ac_cv_func_getpgrp=yes
ac_cv_func_getpid=yes
ac_cv_func_getppid=yes
ac_cv_func_getpwent=yes
ac_cv_func_getpwnam=yes
ac_cv_func_getpwnam_r=yes
ac_cv_func_getpwuid=yes
ac_cv_func_getpwuid_r=yes
ac_cv_func_getspent=yes
ac_cv_func_getspnam=yes
ac_cv_func_getspnam_r=yes
ac_cv_func_getrlimit=yes
ac_cv_func_getresgid=yes
ac_cv_func_getresuid=yes
ac_cv_func_getrusage=yes
ac_cv_func_getsid=yes
ac_cv_func_gettid=yes
ac_cv_func_getxattr=yes
ac_cv_func_getpriority=yes
ac_cv_func_getrandom=yes
ac_cv_func_getitimer=yes
ac_cv_func_getpagesize=yes
ac_cv_func_getuid=yes
ac_cv_func_getpass=yes
ac_cv_func_gettext=yes
ac_cv_func_iconv=yes
ac_cv_func_initgroups=yes
ac_cv_func_ioctl=yes
ac_cv_func_isatty=yes
ac_cv_func_initstate=yes
ac_cv_func_insque=yes
ac_cv_func_jrand48=yes
ac_cv_func_l64a=yes
ac_cv_func_linkat=yes
ac_cv_func_link=yes
ac_cv_func_listxattr=yes
ac_cv_func_lfind=yes
ac_cv_func_lstat=yes
ac_cv_func_lgetxattr=yes
ac_cv_func_lsearch=yes
ac_cv_func_lockf=yes
ac_cv_func_lchown=yes
ac_cv_func_llistxattr=yes
ac_cv_func_lremovexattr=yes
ac_cv_func_lsetxattr=yes
ac_cv_func_lutimes=yes
ac_cv_func_localtime_r=yes
ac_cv_func_localeconv=yes
ac_cv_func_lcong48=yes
ac_cv_func_lrand48=yes
ac_cv_func_madvise=yes
ac_cv_func_mprotect=yes
ac_cv_func_mktime=yes
ac_cv_func_mbrlen=yes
ac_cv_func_mbrtowc=yes
ac_cv_func_mbstowcs=yes
ac_cv_func_mbsinit=yes
ac_cv_func_mbsnrtowcs=yes
ac_cv_func_mbsrtowcs=yes
ac_cv_func_memccpy=yes
ac_cv_func_memmem=yes
ac_cv_func_mempcpy=yes
ac_cv_func_memrchr=yes
ac_cv_func_mkdir=yes
ac_cv_func_mkdirat=yes
ac_cv_func_mmap=yes
ac_cv_func_mremap=yes
ac_cv_func_mrand48=yes
ac_cv_func_msync=yes
ac_cv_func_munmap=yes
ac_cv_func_nanosleep=yes
ac_cv_func_newlocale=yes
ac_cv_func_nice=yes
ac_cv_func_nrand48=yes
ac_cv_func_nftw=yes
ac_cv_func_nl_langinfo=yes
ac_cv_func_nl_langinfo_l=yes
ac_cv_func_openlog=yes
ac_cv_func_openat=yes
ac_cv_func_pause=yes
ac_cv_func_pathconf=yes
ac_cv_func_pipe=yes
ac_cv_func_pipe2=yes
ac_cv_func_poll=yes
ac_cv_func_ppoll=yes
ac_cv_func_posix_fadvise=yes
ac_cv_func_posix_fallocate=yes
ac_cv_func_posix_openpt=yes
ac_cv_func_posix_spawn=yes
ac_cv_func_posix_spawn_file_actions_addchdir_np=yes
ac_cv_func_posix_spawn_file_actions_addclose=yes
ac_cv_func_posix_spawn_file_actions_adddup2=yes
ac_cv_func_posix_spawn_file_actions_addfchdir_np=yes
ac_cv_func_posix_spawn_file_actions_addopen=yes
ac_cv_func_posix_spawn_file_actions_destroy=yes
ac_cv_func_posix_spawn_file_actions_init=yes
ac_cv_func_posix_spawnattr_destroy=yes
ac_cv_func_posix_spawnattr_getflags=yes
ac_cv_func_posix_spawnattr_getpgroup=yes
ac_cv_func_posix_spawnattr_getschedparam=yes
ac_cv_func_posix_spawnattr_getschedpolicy=yes
ac_cv_func_posix_spawnattr_getsigdefault=yes
ac_cv_func_posix_spawnattr_getsigmask=yes
ac_cv_func_posix_spawnattr_init=yes
ac_cv_func_posix_spawnattr_setflags=yes
ac_cv_func_posix_spawnattr_setpgroup=yes
ac_cv_func_posix_spawnattr_setschedparam=yes
ac_cv_func_posix_spawnattr_setschedpolicy=yes
ac_cv_func_posix_spawnattr_setsigdefault=yes
ac_cv_func_posix_spawnattr_setsigmask=yes
ac_cv_func_posix_spawnp=yes
ac_cv_func_prlimit=yes
ac_cv_func_putspent=yes
ac_cv_func_putwc=yes
ac_cv_func_putwc_unlocked=yes
ac_cv_func_putwchar=yes
ac_cv_func_putwchar_unlocked=yes
ac_cv_func_pread=yes
ac_cv_func_preadv=yes
ac_cv_func_pwrite=yes
ac_cv_func_pwritev=yes
ac_cv_func_readlinkat=yes
ac_cv_func_readlink=yes
ac_cv_func_readv=yes
ac_cv_func_readahead=yes
ac_cv_func_random=yes
ac_cv_func_realpath=yes
ac_cv_func_reallocarray=yes
ac_cv_func_regcomp=yes
ac_cv_func_regerror=yes
ac_cv_func_regexec=yes
ac_cv_func_regfree=yes
ac_cv_func_remque=yes
ac_cv_func_removexattr=yes
ac_cv_func_renameat=yes
ac_cv_func_rename=yes
ac_cv_func_rmdir=yes
ac_cv_func_sched_get_priority_max=yes
ac_cv_func_sched_get_priority_min=yes
ac_cv_func_sched_getaffinity=yes
ac_cv_func_sched_getcpu=yes
ac_cv_func_sched_getparam=yes
ac_cv_func_sched_getscheduler=yes
ac_cv_func_sched_rr_get_interval=yes
ac_cv_func_sched_setaffinity=yes
ac_cv_func_sched_setparam=yes
ac_cv_func_sched_setscheduler=yes
ac_cv_func_sched_yield=yes
ac_cv_func_sgetspent=yes
ac_cv_func_timegm=yes
ac_cv_func_timer_create=yes
ac_cv_func_timer_delete=yes
ac_cv_func_timer_getoverrun=yes
ac_cv_func_timer_gettime=yes
ac_cv_func_timer_settime=yes
ac_cv_func_timerfd_create=yes
ac_cv_func_timerfd_gettime=yes
ac_cv_func_timerfd_settime=yes
ac_cv_func_umask=yes
ac_cv_func_uname=yes
ac_cv_func_ungetwc=yes
ac_cv_func_utimensat=yes
ac_cv_func_utimes=yes
ac_cv_func_setregid=yes
ac_cv_func_setrlimit=yes
ac_cv_func_setresgid=yes
ac_cv_func_setresuid=yes
ac_cv_func_setreuid=yes
ac_cv_func_setegid=yes
ac_cv_func_seteuid=yes
ac_cv_func_setfsgid=yes
ac_cv_func_setfsuid=yes
ac_cv_func_setgid=yes
ac_cv_func_setgroups=yes
ac_cv_func_setpgid=yes
ac_cv_func_setpgrp=yes
ac_cv_func_setsid=yes
ac_cv_func_setlocale=yes
ac_cv_func_setlogmask=yes
ac_cv_func_setxattr=yes
ac_cv_func_setspent=yes
ac_cv_func_seed48=yes
ac_cv_func_setpriority=yes
ac_cv_func_setstate=yes
ac_cv_func_secure_getenv=yes
ac_cv_func_setuid=yes
ac_cv_func_shm_open=yes
ac_cv_func_shm_unlink=yes
ac_cv_func_select=yes
ac_cv_func_pselect=yes
ac_cv_func_pthread_kill=yes
ac_cv_func_pthread_sigmask=yes
ac_cv_func_raise=yes
ac_cv_func_sendfile=yes
ac_cv_func_sigaction=yes
ac_cv_func_sigaddset=yes
ac_cv_func_sigaltstack=yes
ac_cv_func_sigdelset=yes
ac_cv_func_sigemptyset=yes
ac_cv_func_sigfillset=yes
ac_cv_func_sigismember=yes
ac_cv_func_sigignore=yes
ac_cv_func_siginterrupt=yes
ac_cv_func_signalfd=yes
ac_cv_func_signal=yes
ac_cv_func_sigpause=yes
ac_cv_func_sigpending=yes
ac_cv_func_sigprocmask=yes
ac_cv_func_sigqueue=yes
ac_cv_func_sighold=yes
ac_cv_func_sigrelse=yes
ac_cv_func_sigsuspend=yes
ac_cv_func_sigtimedwait=yes
ac_cv_func_sigwait=yes
ac_cv_func_sigwaitinfo=yes
ac_cv_func_socketpair=yes
ac_cv_func_statvfs=yes
ac_cv_func_strfmon=yes
ac_cv_func_strfmon_l=yes
ac_cv_func_strftime_l=yes
ac_cv_func_sysinfo=yes
ac_cv_func_stpcpy=yes
ac_cv_func_stpncpy=yes
ac_cv_func_strcasecmp=yes
ac_cv_func_strcasecmp_l=yes
ac_cv_func_strcasestr=yes
ac_cv_func_strchrnul=yes
ac_cv_func_strcoll_l=yes
ac_cv_func_strdup=yes
ac_cv_func_strerror_l=yes
ac_cv_func_strlcat=yes
ac_cv_func_strlcpy=yes
ac_cv_func_strncasecmp=yes
ac_cv_func_strncasecmp_l=yes
ac_cv_func_strndup=yes
ac_cv_func_strnlen=yes
ac_cv_func_strsep=yes
ac_cv_func_strsignal=yes
ac_cv_func_strtod_l=yes
ac_cv_func_strtof_l=yes
ac_cv_func_strtold_l=yes
ac_cv_func_strverscmp=yes
ac_cv_func_strxfrm_l=yes
ac_cv_func_srandom=yes
ac_cv_func_srand48=yes
ac_cv_func_sync=yes
ac_cv_func_sync_file_range=yes
ac_cv_func_symlink=yes
ac_cv_func_symlinkat=yes
ac_cv_func_syscall=yes
ac_cv_func_sysconf=yes
ac_cv_func_syslog=yes
ac_cv_func_tcgetpgrp=yes
ac_cv_func_tcgetsid=yes
ac_cv_func_tcgetattr=yes
ac_cv_func_tcgetwinsize=yes
ac_cv_func_tcsetpgrp=yes
ac_cv_func_tcsetattr=yes
ac_cv_func_tcsetwinsize=yes
ac_cv_func_tcsendbreak=yes
ac_cv_func_tcdrain=yes
ac_cv_func_tcflush=yes
ac_cv_func_tcflow=yes
ac_cv_func_tempnam=yes
ac_cv_func_tdelete=yes
ac_cv_func_tdestroy=yes
ac_cv_func_ttyname_r=yes
ac_cv_func_grantpt=yes
ac_cv_func_unlockpt=yes
ac_cv_func_ptsname=yes
ac_cv_func_ptsname_r=yes
ac_cv_func_openpty=yes
ac_cv_func_login_tty=yes
ac_cv_func_forkpty=yes
ac_cv_func_times=yes
ac_cv_func_tmpfile=yes
ac_cv_func_tmpnam=yes
ac_cv_func_tmpnam_r=yes
ac_cv_func_tfind=yes
ac_cv_func_tsearch=yes
ac_cv_func_twalk=yes
ac_cv_func_truncate=yes
ac_cv_func_tzset=yes
ac_cv_func_uselocale=yes
ac_cv_func_utime=yes
ac_cv_func_wcpcpy=yes
ac_cv_func_wcpncpy=yes
ac_cv_func_wcrtomb=yes
ac_cv_func_wcscasecmp=yes
ac_cv_func_wcscasecmp_l=yes
ac_cv_func_wcscoll=yes
ac_cv_func_wcscoll_l=yes
ac_cv_func_wcsftime=yes
ac_cv_func_wcsftime_l=yes
ac_cv_func_wcsnrtombs=yes
ac_cv_func_wcsdup=yes
ac_cv_func_wcsncasecmp=yes
ac_cv_func_wcsncasecmp_l=yes
ac_cv_func_wcsnlen=yes
ac_cv_func_wcsrtombs=yes
ac_cv_func_wcstod=yes
ac_cv_func_wcstof=yes
ac_cv_func_wcstold=yes
ac_cv_func_wcstol=yes
ac_cv_func_wcstoll=yes
ac_cv_func_wcstombs=yes
ac_cv_func_wcstoul=yes
ac_cv_func_wcstoull=yes
ac_cv_func_wcsxfrm=yes
ac_cv_func_wcsxfrm_l=yes
ac_cv_func_wcswidth=yes
ac_cv_func_wctomb=yes
ac_cv_func_wcwidth=yes
ac_cv_func_setitimer=yes
ac_cv_func_sleep=yes
ac_cv_func_ualarm=yes
ac_cv_func_unlinkat=yes
ac_cv_func_unlink=yes
ac_cv_func_usleep=yes
ac_cv_func_verr=yes
ac_cv_func_verrx=yes
ac_cv_func_vwarn=yes
ac_cv_func_vwarnx=yes
ac_cv_func_vfork=yes
ac_cv_func_vsyslog=yes
ac_cv_func_waitid=yes
ac_cv_func_warn=yes
ac_cv_func_warnx=yes
ac_cv_func_writev=yes
ac_cv_func_lckpwdf=yes
ac_cv_func_memfd_create=yes
ac_cv_func_ulckpwdf=yes
ac_cv_func_dlopen=yes
ac_cv_lib_dl_dlopen=no
ac_cv_header_sched_h=yes
ac_cv_header_crypt_h=yes
ac_cv_header_fenv_h=yes
ac_cv_header_err_h=yes
ac_cv_header_fnmatch_h=yes
ac_cv_header_ftw_h=yes
ac_cv_header_glob_h=yes
ac_cv_header_getopt_h=yes
ac_cv_header_iconv_h=yes
ac_cv_header_libintl_h=yes
ac_cv_header_libgen_h=yes
ac_cv_header_monetary_h=yes
ac_cv_header_paths_h=yes
ac_cv_header_regex_h=yes
ac_cv_header_search_h=yes
ac_cv_header_shadow_h=yes
ac_cv_header_spawn_h=yes
ac_cv_header_stdio_ext_h=yes
ac_cv_header_sysexits_h=yes
ac_cv_header_sys_file_h=yes
ac_cv_header_sys_eventfd_h=yes
ac_cv_header_sys_inotify_h=yes
ac_cv_header_sys_signalfd_h=yes
ac_cv_header_sys_timerfd_h=yes
ac_cv_header_syslog_h=yes
ac_cv_header_sys_epoll_h=yes
ac_cv_header_sys_poll_h=yes
ac_cv_header_sys_select_h=yes
ac_cv_header_sys_resource_h=yes
ac_cv_header_sys_sendfile_h=yes
ac_cv_header_sys_statvfs_h=yes
ac_cv_header_sys_sysinfo_h=yes
ac_cv_header_sys_utsname_h=yes
ac_cv_header_sys_ioctl_h=yes
ac_cv_header_sys_mman_h=yes
ac_cv_header_termios_h=yes
ac_cv_header_utime_h=yes
ac_cv_lib_sendfile_sendfile=no
ac_cv_file__dev_ptmx=yes
ac_cv_file__dev_ptc=no
py_cv_module_resource=yes
py_cv_module_grp=yes
py_cv_module_pwd=yes
py_cv_module_select=yes
py_cv_module_termios=yes
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
    "-Wl,--wrap=uname -Wl,--wrap=gethostname -Wl,--wrap=sethostname -Wl,--wrap=getdomainname -Wl,--wrap=setdomainname -Wl,--wrap=getrlimit -Wl,--wrap=setrlimit "
    "-Wl,--wrap=prlimit -Wl,--wrap=getpagesize -Wl,--wrap=get_phys_pages -Wl,--wrap=get_avphys_pages "
    "-Wl,--wrap=get_nprocs_conf -Wl,--wrap=get_nprocs -Wl,--wrap=getloadavg -Wl,--wrap=sysinfo "
    "-Wl,--wrap=tcgetattr -Wl,--wrap=tcsetattr -Wl,--wrap=tcgetwinsize -Wl,--wrap=tcsetwinsize "
    "-Wl,--wrap=cfgetospeed -Wl,--wrap=cfgetispeed -Wl,--wrap=cfsetospeed -Wl,--wrap=cfsetispeed "
    "-Wl,--wrap=cfsetspeed -Wl,--wrap=cfmakeraw -Wl,--wrap=tcsendbreak -Wl,--wrap=tcdrain "
    "-Wl,--wrap=tcflush -Wl,--wrap=tcflow -Wl,--wrap=tcgetsid -Wl,--wrap=ioctl "
    "-Wl,--wrap=gmtime_r -Wl,--wrap=localtime_r -Wl,--wrap=timegm -Wl,--wrap=mktime "
    "-Wl,--wrap=tzset -Wl,--wrap=strftime_l -Wl,--wrap=asctime_r -Wl,--wrap=ctime_r "
    "-Wl,--wrap=getrandom -Wl,--wrap=getentropy "
    "-Wl,--wrap=random -Wl,--wrap=srandom -Wl,--wrap=initstate -Wl,--wrap=setstate "
    "-Wl,--wrap=drand48 -Wl,--wrap=erand48 -Wl,--wrap=lrand48 -Wl,--wrap=nrand48 "
    "-Wl,--wrap=mrand48 -Wl,--wrap=jrand48 -Wl,--wrap=srand48 -Wl,--wrap=seed48 -Wl,--wrap=lcong48 "
    "-Wl,--wrap=reallocarray -Wl,--wrap=clearenv -Wl,--wrap=secure_getenv "
    "-Wl,--wrap=strtof_l -Wl,--wrap=strtod_l -Wl,--wrap=strtold_l -Wl,--wrap=l64a -Wl,--wrap=a64l "
    "-Wl,--wrap=feclearexcept -Wl,--wrap=fegetexceptflag -Wl,--wrap=fesetexceptflag "
    "-Wl,--wrap=feraiseexcept -Wl,--wrap=fetestexcept -Wl,--wrap=fegetround -Wl,--wrap=fesetround "
    "-Wl,--wrap=fegetenv -Wl,--wrap=feholdexcept -Wl,--wrap=fesetenv -Wl,--wrap=feupdateenv "
    "-Wl,--wrap=madvise -Wl,--wrap=posix_madvise -Wl,--wrap=mprotect -Wl,--wrap=munmap -Wl,--wrap=mremap "
    "-Wl,--wrap=mlock -Wl,--wrap=munlock "
    "-Wl,--wrap=mlockall -Wl,--wrap=munlockall -Wl,--wrap=mlock2 -Wl,--wrap=mincore "
    "-Wl,--wrap=explicit_bzero -Wl,--wrap=explicit_memset "
    "-Wl,--wrap=strlcpy -Wl,--wrap=strlcat -Wl,--wrap=memrchr -Wl,--wrap=mempcpy -Wl,--wrap=memccpy "
    "-Wl,--wrap=strnlen -Wl,--wrap=stpcpy -Wl,--wrap=stpncpy -Wl,--wrap=strdup "
    "-Wl,--wrap=strndup -Wl,--wrap=strcasecmp -Wl,--wrap=strncasecmp "
    "-Wl,--wrap=strcasecmp_l -Wl,--wrap=strncasecmp_l -Wl,--wrap=strcasestr -Wl,--wrap=strsep "
    "-Wl,--wrap=strcoll_l -Wl,--wrap=strxfrm_l -Wl,--wrap=strerror_l -Wl,--wrap=strverscmp "
    "-Wl,--wrap=basename -Wl,--wrap=dirname -Wl,--wrap=fnmatch -Wl,--wrap=glob -Wl,--wrap=globfree "
    "-Wl,--wrap=warn -Wl,--wrap=vwarn -Wl,--wrap=warnx -Wl,--wrap=vwarnx "
    "-Wl,--wrap=err -Wl,--wrap=verr -Wl,--wrap=errx -Wl,--wrap=verrx "
    "-Wl,--wrap=hcreate -Wl,--wrap=hdestroy -Wl,--wrap=hsearch "
    "-Wl,--wrap=hcreate_r -Wl,--wrap=hdestroy_r -Wl,--wrap=hsearch_r "
    "-Wl,--wrap=lfind -Wl,--wrap=lsearch -Wl,--wrap=insque -Wl,--wrap=remque "
    "-Wl,--wrap=tsearch -Wl,--wrap=tfind -Wl,--wrap=tdelete -Wl,--wrap=twalk -Wl,--wrap=tdestroy "
    "-Wl,--wrap=_flushlbf -Wl,--wrap=__fsetlocking -Wl,--wrap=__fwriting -Wl,--wrap=__freading "
    "-Wl,--wrap=__freadable -Wl,--wrap=__fwritable -Wl,--wrap=__flbf -Wl,--wrap=__fbufsize "
    "-Wl,--wrap=__fpending -Wl,--wrap=__fpurge -Wl,--wrap=__freadahead -Wl,--wrap=__freadptr "
    "-Wl,--wrap=__freadptrinc -Wl,--wrap=__fseterr "
    "-Wl,--wrap=openlog -Wl,--wrap=closelog -Wl,--wrap=setlogmask -Wl,--wrap=syslog -Wl,--wrap=vsyslog "
    "-Wl,--wrap=strchrnul -Wl,--wrap=memmem -Wl,--wrap=setlocale "
    "-Wl,--wrap=localeconv -Wl,--wrap=newlocale -Wl,--wrap=duplocale "
    "-Wl,--wrap=freelocale -Wl,--wrap=uselocale -Wl,--wrap=nl_langinfo -Wl,--wrap=nl_langinfo_l "
    "-Wl,--wrap=wcscoll -Wl,--wrap=wcscoll_l -Wl,--wrap=wcsxfrm -Wl,--wrap=wcsxfrm_l "
    "-Wl,--wrap=wcsdup -Wl,--wrap=wcsnlen -Wl,--wrap=wcpcpy -Wl,--wrap=wcpncpy "
    "-Wl,--wrap=wcscasecmp -Wl,--wrap=wcscasecmp_l -Wl,--wrap=wcsncasecmp -Wl,--wrap=wcsncasecmp_l "
    "-Wl,--wrap=mbrtowc -Wl,--wrap=wcrtomb "
    "-Wl,--wrap=btowc -Wl,--wrap=wctob -Wl,--wrap=mbtowc -Wl,--wrap=wctomb -Wl,--wrap=mbsinit "
    "-Wl,--wrap=mbrlen -Wl,--wrap=mbstowcs -Wl,--wrap=wcstombs -Wl,--wrap=mbsrtowcs -Wl,--wrap=wcsrtombs "
    "-Wl,--wrap=mbsnrtowcs -Wl,--wrap=wcsnrtombs -Wl,--wrap=wcsftime -Wl,--wrap=wcsftime_l "
    "-Wl,--wrap=wcstof -Wl,--wrap=wcstod -Wl,--wrap=wcstold -Wl,--wrap=wcstol "
    "-Wl,--wrap=wcstoul -Wl,--wrap=wcstoll -Wl,--wrap=wcstoull "
    "-Wl,--wrap=fgetwc -Wl,--wrap=getwc -Wl,--wrap=getwchar -Wl,--wrap=fputwc -Wl,--wrap=putwc "
    "-Wl,--wrap=putwchar -Wl,--wrap=fgetws -Wl,--wrap=fputws -Wl,--wrap=ungetwc "
    "-Wl,--wrap=fgetwc_unlocked -Wl,--wrap=getwc_unlocked -Wl,--wrap=getwchar_unlocked "
    "-Wl,--wrap=fputwc_unlocked -Wl,--wrap=putwc_unlocked -Wl,--wrap=putwchar_unlocked "
    "-Wl,--wrap=fgetws_unlocked -Wl,--wrap=fputws_unlocked "
    "-Wl,--wrap=wcwidth -Wl,--wrap=wcswidth "
    "-Wl,--wrap=strsignal "
    "-Wl,--wrap=getpriority -Wl,--wrap=setpriority -Wl,--wrap=nice -Wl,--wrap=times -Wl,--wrap=getrusage "
    "-Wl,--wrap=read -Wl,--wrap=write -Wl,--wrap=close "
    "-Wl,--wrap=ftruncate -Wl,--wrap=truncate -Wl,--wrap=posix_fallocate -Wl,--wrap=posix_fadvise "
    "-Wl,--wrap=fsync -Wl,--wrap=fdatasync -Wl,--wrap=sync -Wl,--wrap=syncfs "
    "-Wl,--wrap=readahead -Wl,--wrap=sync_file_range "
    "-Wl,--wrap=pread -Wl,--wrap=pwrite -Wl,--wrap=preadv -Wl,--wrap=pwritev "
    "-Wl,--wrap=readv -Wl,--wrap=writev "
    "-Wl,--wrap=copy_file_range -Wl,--wrap=sendfile "
    "-Wl,--wrap=memfd_create -Wl,--wrap=shm_open -Wl,--wrap=shm_unlink "
    "-Wl,--wrap=eventfd -Wl,--wrap=eventfd_read -Wl,--wrap=eventfd_write "
    "-Wl,--wrap=inotify_init -Wl,--wrap=inotify_init1 -Wl,--wrap=inotify_add_watch -Wl,--wrap=inotify_rm_watch "
    "-Wl,--wrap=pipe -Wl,--wrap=pipe2 -Wl,--wrap=dup -Wl,--wrap=dup2 -Wl,--wrap=dup3 "
    "-Wl,--wrap=close_range -Wl,--wrap=closefrom -Wl,--wrap=socketpair "
    "-Wl,--wrap=socket -Wl,--wrap=bind -Wl,--wrap=connect -Wl,--wrap=listen "
    "-Wl,--wrap=accept -Wl,--wrap=accept4 -Wl,--wrap=getsockname -Wl,--wrap=getpeername "
    "-Wl,--wrap=send -Wl,--wrap=recv -Wl,--wrap=sendto -Wl,--wrap=recvfrom "
    "-Wl,--wrap=sendmsg -Wl,--wrap=recvmsg "
    "-Wl,--wrap=shutdown -Wl,--wrap=setsockopt -Wl,--wrap=getsockopt "
    "-Wl,--wrap=posix_openpt -Wl,--wrap=grantpt -Wl,--wrap=unlockpt -Wl,--wrap=ptsname -Wl,--wrap=ptsname_r "
    "-Wl,--wrap=openpty -Wl,--wrap=login_tty -Wl,--wrap=forkpty "
    "-Wl,--wrap=epoll_create -Wl,--wrap=epoll_create1 -Wl,--wrap=epoll_ctl -Wl,--wrap=epoll_wait -Wl,--wrap=epoll_pwait "
    "-Wl,--wrap=poll -Wl,--wrap=ppoll -Wl,--wrap=select -Wl,--wrap=pselect "
    "-Wl,--wrap=sched_yield -Wl,--wrap=sched_get_priority_max -Wl,--wrap=sched_get_priority_min "
    "-Wl,--wrap=sched_getscheduler -Wl,--wrap=sched_getparam -Wl,--wrap=sched_setparam "
    "-Wl,--wrap=sched_setscheduler -Wl,--wrap=sched_getaffinity -Wl,--wrap=sched_setaffinity "
    "-Wl,--wrap=sched_getcpu -Wl,--wrap=sched_rr_get_interval -Wl,--wrap=prctl "
    "-Wl,--wrap=sigemptyset -Wl,--wrap=sigfillset -Wl,--wrap=sigaddset -Wl,--wrap=sigdelset "
    "-Wl,--wrap=sigismember -Wl,--wrap=sigprocmask -Wl,--wrap=pthread_sigmask "
    "-Wl,--wrap=sigpending -Wl,--wrap=sigaction -Wl,--wrap=signal -Wl,--wrap=raise "
    "-Wl,--wrap=signalfd "
    "-Wl,--wrap=sigaltstack -Wl,--wrap=sigwait -Wl,--wrap=sigwaitinfo -Wl,--wrap=sigtimedwait -Wl,--wrap=sigsuspend -Wl,--wrap=pause "
    "-Wl,--wrap=sighold -Wl,--wrap=sigrelse -Wl,--wrap=sigignore -Wl,--wrap=siginterrupt "
    "-Wl,--wrap=sigpause -Wl,--wrap=sigqueue -Wl,--wrap=pthread_kill "
    "-Wl,--wrap=getitimer -Wl,--wrap=setitimer -Wl,--wrap=alarm -Wl,--wrap=ualarm "
    "-Wl,--wrap=timer_create -Wl,--wrap=timer_settime -Wl,--wrap=timer_gettime "
    "-Wl,--wrap=timer_getoverrun -Wl,--wrap=timer_delete "
    "-Wl,--wrap=timerfd_create -Wl,--wrap=timerfd_settime -Wl,--wrap=timerfd_gettime "
    "-Wl,--wrap=clock_nanosleep -Wl,--wrap=sleep -Wl,--wrap=usleep "
    "-Wl,--wrap=futimesat -Wl,--wrap=lutimes -Wl,--wrap=utime -Wl,--wrap=utimensat "
    "-Wl,--wrap=isatty -Wl,--wrap=ttyname -Wl,--wrap=ttyname_r -Wl,--wrap=ctermid -Wl,--wrap=ctermid_r "
    "-Wl,--wrap=getlogin -Wl,--wrap=getlogin_r -Wl,--wrap=getpass "
    "-Wl,--wrap=tmpfile -Wl,--wrap=tmpnam -Wl,--wrap=tmpnam_r -Wl,--wrap=tempnam "
    "-Wl,--wrap=mkstemp -Wl,--wrap=mkostemp -Wl,--wrap=mkstemps -Wl,--wrap=mkostemps "
    "-Wl,--wrap=mkdtemp -Wl,--wrap=mktemp "
    "-Wl,--wrap=statvfs -Wl,--wrap=fstatvfs "
    "-Wl,--wrap=pathconf -Wl,--wrap=fpathconf -Wl,--wrap=sysconf -Wl,--wrap=confstr "
    "-Wl,--wrap=getcwd -Wl,--wrap=get_current_dir_name -Wl,--wrap=getwd -Wl,--wrap=realpath -Wl,--wrap=chdir -Wl,--wrap=chroot "
    "-Wl,--wrap=access -Wl,--wrap=faccessat -Wl,--wrap=openat -Wl,--wrap=mkdirat -Wl,--wrap=mknod -Wl,--wrap=mknodat "
    "-Wl,--wrap=unlinkat -Wl,--wrap=readlinkat -Wl,--wrap=symlinkat "
    "-Wl,--wrap=linkat -Wl,--wrap=renameat "
    "-Wl,--wrap=mkdir -Wl,--wrap=rmdir -Wl,--wrap=unlink -Wl,--wrap=readlink "
    "-Wl,--wrap=symlink -Wl,--wrap=link -Wl,--wrap=rename "
    "-Wl,--wrap=setxattr -Wl,--wrap=lsetxattr -Wl,--wrap=fsetxattr "
    "-Wl,--wrap=getxattr -Wl,--wrap=lgetxattr -Wl,--wrap=fgetxattr "
    "-Wl,--wrap=listxattr -Wl,--wrap=llistxattr -Wl,--wrap=flistxattr "
    "-Wl,--wrap=removexattr -Wl,--wrap=lremovexattr -Wl,--wrap=fremovexattr "
    "-Wl,--wrap=getuid -Wl,--wrap=geteuid -Wl,--wrap=getgid -Wl,--wrap=getegid "
    "-Wl,--wrap=getpwuid -Wl,--wrap=getpwuid_r -Wl,--wrap=getpwnam -Wl,--wrap=getpwnam_r "
    "-Wl,--wrap=getpwent -Wl,--wrap=setpwent -Wl,--wrap=endpwent "
    "-Wl,--wrap=getgrgid -Wl,--wrap=getgrgid_r -Wl,--wrap=getgrnam -Wl,--wrap=getgrnam_r "
    "-Wl,--wrap=getgrent -Wl,--wrap=setgrent -Wl,--wrap=endgrent -Wl,--wrap=getgrouplist "
    "-Wl,--wrap=getspent -Wl,--wrap=setspent -Wl,--wrap=endspent -Wl,--wrap=fgetspent "
    "-Wl,--wrap=sgetspent -Wl,--wrap=getspnam -Wl,--wrap=getspnam_r -Wl,--wrap=putspent "
    "-Wl,--wrap=lckpwdf -Wl,--wrap=ulckpwdf "
    "-Wl,--wrap=setuid -Wl,--wrap=seteuid -Wl,--wrap=setfsuid "
    "-Wl,--wrap=setgid -Wl,--wrap=setegid -Wl,--wrap=setfsgid "
    "-Wl,--wrap=getpid -Wl,--wrap=getppid "
    "-Wl,--wrap=umask -Wl,--wrap=chmod -Wl,--wrap=fchmod "
    "-Wl,--wrap=fchmodat "
    "-Wl,--wrap=lockf -Wl,--wrap=flock "
    "-Wl,--wrap=chown -Wl,--wrap=lchown -Wl,--wrap=fchown -Wl,--wrap=fchownat "
    "-Wl,--wrap=getpgid -Wl,--wrap=getpgrp -Wl,--wrap=getsid "
    "-Wl,--wrap=setpgid -Wl,--wrap=setpgrp -Wl,--wrap=setsid "
    "-Wl,--wrap=tcgetpgrp -Wl,--wrap=tcsetpgrp "
    "-Wl,--wrap=kill -Wl,--wrap=killpg "
    "-Wl,--wrap=fork -Wl,--wrap=vfork "
    "-Wl,--wrap=execve -Wl,--wrap=execveat "
    "-Wl,--wrap=wait -Wl,--wrap=waitpid -Wl,--wrap=wait3 -Wl,--wrap=wait4 -Wl,--wrap=waitid "
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
