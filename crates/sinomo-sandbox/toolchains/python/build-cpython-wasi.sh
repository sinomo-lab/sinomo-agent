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
cat > "${CPYTHON_SRC}/Modules/sinomo-python-wasi-mman-compat.c" <<'MMAN_COMPAT'
#include <errno.h>
#include <stddef.h>

/*
 * wasi-sdk's emulated mman implementation maps files into malloc-backed memory
 * and exposes mmap/munmap/mprotect, but the header also declares msync without
 * providing a symbol. There is no honest way to flush the malloc-backed copy
 * back to the original file from here, so report the unsupported operation if
 * Python code calls mmap.flush(). Read-only mmap users still get the real
 * wasi-sdk mmap behavior.
 */
int msync(void *addr, size_t length, int flags) {
    (void)addr;
    (void)length;
    (void)flags;
    errno = ENOTSUP;
    return -1;
}
MMAN_COMPAT
cat > "${CPYTHON_SRC}/Modules/_sinomo_subprocessmodule.c" <<'SINOMO_SUBPROCESS'
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <limits.h>
#include <string.h>

__attribute__((import_module("sinomo_sandbox_preview1"), import_name("proc_spawn_wait_at")))
extern int sinomo_proc_spawn_wait_at(char **argv, char **envp, const char *cwd, int cwd_len);

__attribute__((import_module("sinomo_sandbox_preview1"), import_name("proc_spawn_wait_at_io")))
extern int sinomo_proc_spawn_wait_at_io(
    char **argv,
    char **envp,
    const char *cwd,
    int cwd_len,
    const char *stdin_path,
    int stdin_path_len,
    const char *stdout_path,
    int stdout_path_len,
    int stdout_append,
    const char *stderr_path,
    int stderr_path_len,
    int stderr_append,
    int stderr_to_stdout);

typedef struct {
    char **values;
    PyObject **objects;
    Py_ssize_t len;
} SinomoCStringArray;

static void
sinomo_cstring_array_clear(SinomoCStringArray *array)
{
    if (array->objects != NULL) {
        for (Py_ssize_t index = 0; index < array->len; index++) {
            Py_XDECREF(array->objects[index]);
        }
    }
    PyMem_Free(array->objects);
    PyMem_Free(array->values);
    array->objects = NULL;
    array->values = NULL;
    array->len = 0;
}

static int
sinomo_sequence_to_cstring_array(PyObject *sequence, SinomoCStringArray *array)
{
    PyObject *fast = PySequence_Fast(sequence, "expected a sequence of strings");
    if (fast == NULL) {
        return -1;
    }

    Py_ssize_t len = PySequence_Fast_GET_SIZE(fast);
    array->values = PyMem_Calloc((size_t)len + 1, sizeof(char *));
    array->objects = PyMem_Calloc((size_t)len, sizeof(PyObject *));
    array->len = len;
    if (array->values == NULL || array->objects == NULL) {
        Py_DECREF(fast);
        PyErr_NoMemory();
        sinomo_cstring_array_clear(array);
        return -1;
    }

    PyObject **items = PySequence_Fast_ITEMS(fast);
    for (Py_ssize_t index = 0; index < len; index++) {
        PyObject *bytes = NULL;
        if (!PyUnicode_FSConverter(items[index], &bytes)) {
            Py_DECREF(fast);
            sinomo_cstring_array_clear(array);
            return -1;
        }
        array->objects[index] = bytes;
        array->values[index] = PyBytes_AS_STRING(bytes);
    }
    array->values[len] = NULL;
    Py_DECREF(fast);
    return 0;
}

static PyObject *
sinomo_spawn_wait(PyObject *self, PyObject *args)
{
    (void)self;
    PyObject *argv_obj = NULL;
    PyObject *env_obj = Py_None;
    const char *cwd = "/";
    Py_ssize_t cwd_len = 1;
    if (!PyArg_ParseTuple(args, "O|Os#:spawn_wait", &argv_obj, &env_obj, &cwd, &cwd_len)) {
        return NULL;
    }
    if (cwd_len < 0 || cwd_len > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "cwd length is outside the supported range");
        return NULL;
    }

    SinomoCStringArray argv = {0};
    SinomoCStringArray env = {0};
    if (sinomo_sequence_to_cstring_array(argv_obj, &argv) < 0) {
        return NULL;
    }
    if (argv.len == 0) {
        sinomo_cstring_array_clear(&argv);
        PyErr_SetString(PyExc_ValueError, "argv must not be empty");
        return NULL;
    }
    char **envp = NULL;
    if (env_obj != Py_None) {
        if (sinomo_sequence_to_cstring_array(env_obj, &env) < 0) {
            sinomo_cstring_array_clear(&argv);
            return NULL;
        }
        envp = env.values;
    }

    int status = sinomo_proc_spawn_wait_at(argv.values, envp, cwd, (int)cwd_len);
    sinomo_cstring_array_clear(&env);
    sinomo_cstring_array_clear(&argv);
    if (status < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return PyLong_FromLong((status >> 8) & 0xff);
}

static int
sinomo_check_len(Py_ssize_t len, const char *name)
{
    if (len < 0 || len > INT_MAX) {
        PyErr_Format(PyExc_ValueError, "%s length is outside the supported range", name);
        return -1;
    }
    return 0;
}

static PyObject *
sinomo_spawn_wait_io(PyObject *self, PyObject *args)
{
    (void)self;
    PyObject *argv_obj = NULL;
    PyObject *env_obj = Py_None;
    const char *cwd = "/";
    Py_ssize_t cwd_len = 1;
    const char *stdin_path = NULL;
    Py_ssize_t stdin_path_len = 0;
    const char *stdout_path = NULL;
    Py_ssize_t stdout_path_len = 0;
    int stdout_append = 0;
    const char *stderr_path = NULL;
    Py_ssize_t stderr_path_len = 0;
    int stderr_append = 0;
    int stderr_to_stdout = 0;
    if (!PyArg_ParseTuple(
            args,
            "OOs#z#z#iz#ii:spawn_wait_io",
            &argv_obj,
            &env_obj,
            &cwd,
            &cwd_len,
            &stdin_path,
            &stdin_path_len,
            &stdout_path,
            &stdout_path_len,
            &stdout_append,
            &stderr_path,
            &stderr_path_len,
            &stderr_append,
            &stderr_to_stdout)) {
        return NULL;
    }
    if (sinomo_check_len(cwd_len, "cwd") < 0 ||
        sinomo_check_len(stdin_path_len, "stdin path") < 0 ||
        sinomo_check_len(stdout_path_len, "stdout path") < 0 ||
        sinomo_check_len(stderr_path_len, "stderr path") < 0) {
        return NULL;
    }

    SinomoCStringArray argv = {0};
    SinomoCStringArray env = {0};
    if (sinomo_sequence_to_cstring_array(argv_obj, &argv) < 0) {
        return NULL;
    }
    if (argv.len == 0) {
        sinomo_cstring_array_clear(&argv);
        PyErr_SetString(PyExc_ValueError, "argv must not be empty");
        return NULL;
    }
    char **envp = NULL;
    if (env_obj != Py_None) {
        if (sinomo_sequence_to_cstring_array(env_obj, &env) < 0) {
            sinomo_cstring_array_clear(&argv);
            return NULL;
        }
        envp = env.values;
    }

    int status = sinomo_proc_spawn_wait_at_io(
        argv.values,
        envp,
        cwd,
        (int)cwd_len,
        stdin_path,
        (int)stdin_path_len,
        stdout_path,
        (int)stdout_path_len,
        stdout_append,
        stderr_path,
        (int)stderr_path_len,
        stderr_append,
        stderr_to_stdout);
    sinomo_cstring_array_clear(&env);
    sinomo_cstring_array_clear(&argv);
    if (status < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return PyLong_FromLong((status >> 8) & 0xff);
}

static PyMethodDef sinomo_subprocess_methods[] = {
    {"spawn_wait", sinomo_spawn_wait, METH_VARARGS, "Spawn a child Python process in the Sinomo sandbox and wait for it."},
    {"spawn_wait_io", sinomo_spawn_wait_io, METH_VARARGS, "Spawn a child Python process in the Sinomo sandbox with file-backed stdio and wait for it."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef sinomo_subprocess_module = {
    PyModuleDef_HEAD_INIT,
    "_sinomo_subprocess",
    "Sinomo sandbox subprocess bridge for CPython WASI.",
    -1,
    sinomo_subprocess_methods,
};

PyMODINIT_FUNC
PyInit__sinomo_subprocess(void)
{
    return PyModule_Create(&sinomo_subprocess_module);
}
SINOMO_SUBPROCESS
cat > "${CPYTHON_SRC}/cross-build/wasm32-wasip1/Modules/Setup.local" <<SETUP_LOCAL
# Generated by Sinomo CPython WASI recipe.
zlib zlibmodule.c -I${ZLIB_OUT}/include -L${ZLIB_OUT}/lib -lz
mmap mmapmodule.c sinomo-python-wasi-mman-compat.c -D_WASI_EMULATED_MMAN -lwasi-emulated-mman
_sinomo_subprocess _sinomo_subprocessmodule.c
SETUP_LOCAL
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
cat > "${PACKAGE_ROOT}/Lib/sitecustomize.py" <<'PY'
import os
import sys

for _path in (
    "/cross-build/wasm32-wasip1/build/lib.wasi-wasm32-3.13",
    "/Lib/site-packages",
):
    if _path not in sys.path:
        sys.path.insert(0, _path)

for _name in ("getuid", "geteuid", "getgid", "getegid"):
    if not hasattr(os, _name):
        setattr(os, _name, lambda _value=0: _value)
if not hasattr(os, "getgroups"):
    os.getgroups = lambda: [0]
if not hasattr(os, "umask"):
    _sinomo_umask_value = 0o022

    def _sinomo_umask(mask):
        global _sinomo_umask_value
        previous = _sinomo_umask_value
        _sinomo_umask_value = int(mask) & 0o777
        return previous

    os.umask = _sinomo_umask

if sys.platform == "wasi":
    try:
        import _sinomo_subprocess
        import subprocess as _subprocess

        _original_subprocess_run = _subprocess.run
        _original_subprocess_popen = _subprocess.Popen
        _sinomo_popen_counter = 0

        def _sinomo_env_list(env):
            if env is None:
                merged = dict(os.environ)
            else:
                merged = dict(env)
            merged.setdefault("HOME", "/home")
            merged.setdefault("PIP_DISABLE_PIP_VERSION_CHECK", "1")
            merged.setdefault("PIP_FIND_LINKS", "/Lib/sinomo-wheels")
            if not merged.get("PYTHONHOME"):
                merged["PYTHONHOME"] = "/"
            _required_python_paths = [
                "/Lib",
                "/cross-build/wasm32-wasip1/build/lib.wasi-wasm32-3.13",
                "/Lib/site-packages",
            ]
            _python_path = [part for part in merged.get("PYTHONPATH", "").split(":") if part]
            for _path in reversed(_required_python_paths):
                if _path not in _python_path:
                    _python_path.insert(0, _path)
            merged["PYTHONPATH"] = ":".join(_python_path)
            return [f"{key}={value}" for key, value in merged.items()]

        class _SinomoPipeWriter:
            def __init__(self, path):
                self.name = path
                self.closed = False

            def close(self):
                self.closed = True

            def write(self, data):
                raise OSError("Sinomo WASI subprocess stdin pipes are file-backed and must be written before spawn")

            def flush(self):
                pass

        class _SinomoPipeReader:
            def __init__(self, path, *, encoding=None, errors=None, text=False):
                mode = "r" if text else "rb"
                self._file = open(path, mode, encoding=encoding if text else None, errors=errors if text else None)
                self.name = path

            def close(self):
                return self._file.close()

            def read(self, *args):
                return self._file.read(*args)

            def readline(self, *args):
                return self._file.readline(*args)

            def readlines(self, *args):
                return self._file.readlines(*args)

            def __iter__(self):
                return iter(self._file)

            @property
            def closed(self):
                return self._file.closed

        def _sinomo_next_pipe_path(kind):
            global _sinomo_popen_counter
            _sinomo_popen_counter += 1
            return f"/tmp/sinomo-subprocess-{_sinomo_popen_counter}-{kind}"

        def _sinomo_normalize_argv(argv):
            if isinstance(argv, (str, bytes, os.PathLike)):
                raise TypeError("string commands require shell=True, which is unsupported in the Sinomo WASI subprocess bridge")
            normalized = [os.fsdecode(value) for value in argv]
            if normalized:
                executable_name = os.path.basename(normalized[0])
                if normalized[0] == sys.executable or executable_name in ("python", "python.wasm"):
                    normalized[0] = "python"
            return normalized

        class _SinomoPopen:
            def __init__(
                self,
                args,
                bufsize=-1,
                executable=None,
                stdin=None,
                stdout=None,
                stderr=None,
                preexec_fn=None,
                close_fds=True,
                shell=False,
                cwd=None,
                env=None,
                universal_newlines=None,
                startupinfo=None,
                creationflags=0,
                restore_signals=True,
                start_new_session=False,
                pass_fds=(),
                *,
                user=None,
                group=None,
                extra_groups=None,
                encoding=None,
                errors=None,
                text=None,
                umask=-1,
                pipesize=-1,
                process_group=None,
            ):
                unsupported = (
                    preexec_fn is not None
                    or not close_fds
                    or shell
                    or startupinfo is not None
                    or creationflags
                    or start_new_session
                    or pass_fds
                    or user is not None
                    or group is not None
                    or extra_groups is not None
                    or umask != -1
                    or pipesize != -1
                    or process_group is not None
                )
                if unsupported:
                    self._fallback = _original_subprocess_popen(
                        args,
                        bufsize=bufsize,
                        executable=executable,
                        stdin=stdin,
                        stdout=stdout,
                        stderr=stderr,
                        preexec_fn=preexec_fn,
                        close_fds=close_fds,
                        shell=shell,
                        cwd=cwd,
                        env=env,
                        universal_newlines=universal_newlines,
                        startupinfo=startupinfo,
                        creationflags=creationflags,
                        restore_signals=restore_signals,
                        start_new_session=start_new_session,
                        pass_fds=pass_fds,
                        user=user,
                        group=group,
                        extra_groups=extra_groups,
                        encoding=encoding,
                        errors=errors,
                        text=text,
                        umask=umask,
                        pipesize=pipesize,
                        process_group=process_group,
                    )
                    self.__dict__.update(self._fallback.__dict__)
                    return

                self.args = _sinomo_normalize_argv(args)
                if executable is not None and self.args:
                    self.args[0] = os.fsdecode(executable)
                    self.args = _sinomo_normalize_argv(self.args)
                self.stdin = None
                self.stdout = None
                self.stderr = None
                self.returncode = None
                self._text = bool(text or universal_newlines or encoding or errors)
                self._encoding = encoding or "utf-8"
                self._errors = errors or "strict"
                cwd = os.fsdecode(cwd if cwd is not None else os.environ.get("PWD") or os.getcwd())
                env_list = _sinomo_env_list(env)

                stdin_path = None
                if stdin == _subprocess.PIPE:
                    stdin_path = _sinomo_next_pipe_path("stdin")
                    open(stdin_path, "wb").close()
                    self.stdin = _SinomoPipeWriter(stdin_path)
                elif stdin is not None:
                    raise OSError("unsupported stdin configuration for Sinomo WASI subprocess")

                stdout_path = None
                if stdout == _subprocess.PIPE:
                    stdout_path = _sinomo_next_pipe_path("stdout")
                elif stdout is not None:
                    raise OSError("unsupported stdout configuration for Sinomo WASI subprocess")

                stderr_path = None
                stderr_to_stdout = stderr == _subprocess.STDOUT
                if stderr == _subprocess.PIPE:
                    stderr_path = _sinomo_next_pipe_path("stderr")
                elif stderr not in (None, _subprocess.STDOUT):
                    raise OSError("unsupported stderr configuration for Sinomo WASI subprocess")

                self.returncode = _sinomo_subprocess.spawn_wait_io(
                    self.args,
                    env_list,
                    cwd,
                    stdin_path,
                    stdout_path,
                    False,
                    stderr_path,
                    False,
                    stderr_to_stdout,
                )
                if stdout_path is not None:
                    if not os.path.exists(stdout_path):
                        open(stdout_path, "wb").close()
                    self.stdout = _SinomoPipeReader(
                        stdout_path,
                        encoding=self._encoding,
                        errors=self._errors,
                        text=self._text,
                    )
                if stderr_path is not None:
                    if not os.path.exists(stderr_path):
                        open(stderr_path, "wb").close()
                    self.stderr = _SinomoPipeReader(
                        stderr_path,
                        encoding=self._encoding,
                        errors=self._errors,
                        text=self._text,
                    )

            def __enter__(self):
                return self

            def __exit__(self, exc_type, value, traceback):
                for stream in (self.stdin, self.stdout, self.stderr):
                    if stream is not None:
                        stream.close()
                self.wait()
                return False

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                if timeout is not None:
                    raise OSError("Sinomo WASI subprocesses are waited synchronously at spawn time")
                return self.returncode

            def communicate(self, input=None, timeout=None):
                if input is not None or timeout is not None:
                    raise OSError("unsupported communicate configuration for Sinomo WASI subprocess")
                stdout = self.stdout.read() if self.stdout is not None else None
                stderr = self.stderr.read() if self.stderr is not None else None
                return stdout, stderr

            def kill(self):
                return None

            terminate = kill

        def _sinomo_subprocess_run(*popenargs, input=None, capture_output=False, timeout=None, check=False, **kwargs):
            if input is not None or timeout is not None:
                return _original_subprocess_run(
                    *popenargs,
                    input=input,
                    capture_output=capture_output,
                    timeout=timeout,
                    check=check,
                    **kwargs,
                )
            if capture_output:
                kwargs["stdout"] = _subprocess.PIPE
                kwargs["stderr"] = _subprocess.PIPE
            with _SinomoPopen(*popenargs, **kwargs) as process:
                stdout, stderr = process.communicate()
                returncode = process.poll()
            completed = _subprocess.CompletedProcess(popenargs[0], returncode, stdout, stderr)
            if check and returncode:
                raise _subprocess.CalledProcessError(returncode, popenargs[0], output=stdout, stderr=stderr)
            return completed

        def _sinomo_subprocess_call(*popenargs, timeout=None, **kwargs):
            return _sinomo_subprocess_run(*popenargs, timeout=timeout, **kwargs).returncode

        def _sinomo_subprocess_check_call(*popenargs, **kwargs):
            returncode = _sinomo_subprocess_call(*popenargs, **kwargs)
            if returncode:
                raise _subprocess.CalledProcessError(returncode, popenargs[0])
            return 0

        _subprocess.run = _sinomo_subprocess_run
        _subprocess.call = _sinomo_subprocess_call
        _subprocess.check_call = _sinomo_subprocess_check_call
        _subprocess.Popen = _SinomoPopen
    except Exception:
        pass
PY
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
