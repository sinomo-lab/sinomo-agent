#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <errno.h>
#include <limits.h>
#include <stdint.h>
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

__attribute__((import_module("wasix_32v1"), import_name("proc_getgid")))
extern int sinomo_proc_getgid(unsigned int *gid);

__attribute__((import_module("wasix_32v1"), import_name("proc_getgroups")))
extern int sinomo_proc_getgroups(unsigned int *groups, unsigned int max_groups, unsigned int *count);

__attribute__((import_module("wasix_32v1"), import_name("proc_id")))
extern int sinomo_proc_id(unsigned int *pid);

__attribute__((import_module("wasix_32v1"), import_name("proc_parent")))
extern int sinomo_proc_parent(unsigned int pid, unsigned int *ppid);

__attribute__((import_module("wasix_32v1"), import_name("proc_signal")))
extern int sinomo_proc_signal(int pid, int signal);

typedef struct {
    uint8_t tag;
    uint8_t padding[3];
    uint32_t pid;
} SinomoOptionPid;

typedef struct {
    uint16_t exit_code;
    uint8_t signal;
    uint8_t padding;
} SinomoErrnoSignal;

typedef union {
    uint8_t nothing;
    uint16_t exit_normal;
    SinomoErrnoSignal exit_signal;
    uint8_t stopped;
} SinomoJoinStatusUnion;

typedef struct {
    uint8_t tag;
    uint8_t padding;
    SinomoJoinStatusUnion u;
} SinomoJoinStatus;

__attribute__((import_module("wasix_32v1"), import_name("proc_join")))
extern int sinomo_proc_join(SinomoOptionPid *pid, unsigned int flags, SinomoJoinStatus *status);

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

static int
sinomo_chown_id(int value, unsigned int *out)
{
    if (value == -1) {
        *out = UINT_MAX;
        return 0;
    }
    if (value < 0) {
        PyErr_SetString(PyExc_ValueError, "ownership id must be non-negative or -1");
        return -1;
    }
    *out = (unsigned int)value;
    return 0;
}

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

static PyObject *
sinomo_umask(PyObject *self, PyObject *args)
{
    (void)self;
    unsigned int mask = 0;
    if (!PyArg_ParseTuple(args, "I:umask", &mask)) {
        return NULL;
    }
    unsigned int previous = 0;
    int err = sinomo_proc_umask(mask, &previous);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return PyLong_FromUnsignedLong(previous);
}

static PyObject *
sinomo_chmod(PyObject *self, PyObject *args)
{
    (void)self;
    PyObject *path_obj = NULL;
    unsigned int mode = 0;
    if (!PyArg_ParseTuple(args, "OI:chmod", &path_obj, &mode)) {
        return NULL;
    }
    PyObject *bytes = NULL;
    if (!PyUnicode_FSConverter(path_obj, &bytes)) {
        return NULL;
    }
    Py_ssize_t path_len = PyBytes_GET_SIZE(bytes);
    if (path_len < 0 || path_len > INT_MAX) {
        Py_DECREF(bytes);
        PyErr_SetString(PyExc_ValueError, "path length is outside the supported range");
        return NULL;
    }
    int err = sinomo_path_chmod(PyBytes_AS_STRING(bytes), (unsigned int)path_len, mode);
    Py_DECREF(bytes);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
sinomo_fchmod(PyObject *self, PyObject *args)
{
    (void)self;
    int fd = -1;
    unsigned int mode = 0;
    if (!PyArg_ParseTuple(args, "iI:fchmod", &fd, &mode)) {
        return NULL;
    }
    int err = sinomo_fd_chmod(fd, mode);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
sinomo_chown(PyObject *self, PyObject *args)
{
    (void)self;
    PyObject *path_obj = NULL;
    int uid_arg = 0;
    int gid_arg = 0;
    if (!PyArg_ParseTuple(args, "Oii:chown", &path_obj, &uid_arg, &gid_arg)) {
        return NULL;
    }
    unsigned int uid = 0;
    unsigned int gid = 0;
    if (sinomo_chown_id(uid_arg, &uid) < 0 || sinomo_chown_id(gid_arg, &gid) < 0) {
        return NULL;
    }
    PyObject *bytes = NULL;
    if (!PyUnicode_FSConverter(path_obj, &bytes)) {
        return NULL;
    }
    Py_ssize_t path_len = PyBytes_GET_SIZE(bytes);
    if (path_len < 0 || path_len > INT_MAX) {
        Py_DECREF(bytes);
        PyErr_SetString(PyExc_ValueError, "path length is outside the supported range");
        return NULL;
    }
    int err = sinomo_path_chown(PyBytes_AS_STRING(bytes), (unsigned int)path_len, uid, gid);
    Py_DECREF(bytes);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
sinomo_fchown(PyObject *self, PyObject *args)
{
    (void)self;
    int fd = -1;
    int uid_arg = 0;
    int gid_arg = 0;
    if (!PyArg_ParseTuple(args, "iii:fchown", &fd, &uid_arg, &gid_arg)) {
        return NULL;
    }
    unsigned int uid = 0;
    unsigned int gid = 0;
    if (sinomo_chown_id(uid_arg, &uid) < 0 || sinomo_chown_id(gid_arg, &gid) < 0) {
        return NULL;
    }
    int err = sinomo_fd_chown(fd, uid, gid);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
sinomo_getuid(PyObject *self, PyObject *args)
{
    (void)self;
    (void)args;
    unsigned int uid = 0;
    int err = sinomo_proc_getuid(&uid);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return PyLong_FromUnsignedLong(uid);
}

static PyObject *
sinomo_getgid(PyObject *self, PyObject *args)
{
    (void)self;
    (void)args;
    unsigned int gid = 0;
    int err = sinomo_proc_getgid(&gid);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return PyLong_FromUnsignedLong(gid);
}

static PyObject *
sinomo_getgroups(PyObject *self, PyObject *args)
{
    (void)self;
    (void)args;
    unsigned int count = 0;
    int err = sinomo_proc_getgroups(NULL, 0, &count);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    PyObject *list = PyList_New((Py_ssize_t)count);
    if (list == NULL) {
        return NULL;
    }
    if (count == 0) {
        return list;
    }
    unsigned int *groups = PyMem_Calloc(count, sizeof(unsigned int));
    if (groups == NULL) {
        Py_DECREF(list);
        return PyErr_NoMemory();
    }
    unsigned int written = 0;
    err = sinomo_proc_getgroups(groups, count, &written);
    if (err != 0) {
        PyMem_Free(groups);
        Py_DECREF(list);
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    if (written > count) {
        PyMem_Free(groups);
        Py_DECREF(list);
        PyErr_SetString(PyExc_RuntimeError, "runtime returned more groups than requested");
        return NULL;
    }
    for (unsigned int index = 0; index < written; index++) {
        PyObject *value = PyLong_FromUnsignedLong(groups[index]);
        if (value == NULL) {
            PyMem_Free(groups);
            Py_DECREF(list);
            return NULL;
        }
        PyList_SET_ITEM(list, (Py_ssize_t)index, value);
    }
    PyMem_Free(groups);
    if (written < count) {
        if (PyList_SetSlice(list, (Py_ssize_t)written, (Py_ssize_t)count, NULL) < 0) {
            Py_DECREF(list);
            return NULL;
        }
    }
    return list;
}

static PyObject *
sinomo_getpid(PyObject *self, PyObject *args)
{
    (void)self;
    (void)args;
    unsigned int pid = 0;
    int err = sinomo_proc_id(&pid);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return PyLong_FromUnsignedLong(pid);
}

static PyObject *
sinomo_getppid(PyObject *self, PyObject *args)
{
    (void)self;
    (void)args;
    unsigned int pid = 0;
    unsigned int ppid = 0;
    int err = sinomo_proc_id(&pid);
    if (err == 0) {
        err = sinomo_proc_parent(pid, &ppid);
    }
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return PyLong_FromUnsignedLong(ppid);
}

static PyObject *
sinomo_kill(PyObject *self, PyObject *args)
{
    (void)self;
    int pid = 0;
    int signal = 0;
    if (!PyArg_ParseTuple(args, "ii:kill", &pid, &signal)) {
        return NULL;
    }
    if (signal < 0 || signal > 31) {
        PyErr_SetString(PyExc_ValueError, "signal number out of WASIX range");
        return NULL;
    }
    int err = sinomo_proc_signal(pid, signal);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    Py_RETURN_NONE;
}

static int
sinomo_wait_status(SinomoJoinStatus *status)
{
    switch (status->tag) {
    case 0:
        return 0;
    case 1:
        return ((int)status->u.exit_normal & 0xff) << 8;
    case 2:
        return (int)status->u.exit_signal.signal & 0x7f;
    case 3:
        return (((int)status->u.stopped & 0xff) << 8) | 0x7f;
    default:
        errno = EINVAL;
        return -1;
    }
}

static PyObject *
sinomo_waitpid(PyObject *self, PyObject *args)
{
    (void)self;
    int pid_arg = -1;
    int options = 0;
    if (!PyArg_ParseTuple(args, "ii:waitpid", &pid_arg, &options)) {
        return NULL;
    }
    SinomoOptionPid pid = {
        .tag = 1,
        .padding = {0, 0, 0},
        .pid = (uint32_t)pid_arg,
    };
    unsigned int flags = 0;
    if ((options & 1) != 0) {
        flags |= 1;
    }
    if ((options & 2) != 0) {
        flags |= 2;
    }
    SinomoJoinStatus status = {0};
    int err = sinomo_proc_join(&pid, flags, &status);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    int wait_status = sinomo_wait_status(&status);
    if (wait_status < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return Py_BuildValue("(ii)", (int)pid.pid, wait_status);
}

static PyObject *
sinomo_wait(PyObject *self, PyObject *args)
{
    (void)self;
    (void)args;
    SinomoOptionPid pid = {
        .tag = 0,
        .padding = {0, 0, 0},
        .pid = 0,
    };
    SinomoJoinStatus status = {0};
    int err = sinomo_proc_join(&pid, 0, &status);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    int wait_status = sinomo_wait_status(&status);
    if (wait_status < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return Py_BuildValue("(ii)", (int)pid.pid, wait_status);
}

static PyObject *
sinomo_getpgid(PyObject *self, PyObject *args)
{
    (void)self;
    int pid = 0;
    if (!PyArg_ParseTuple(args, "i:getpgid", &pid)) {
        return NULL;
    }
    int pgid = 0;
    int err = sinomo_proc_getpgid(pid, &pgid);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return PyLong_FromLong(pgid);
}

static PyObject *
sinomo_getpgrp(PyObject *self, PyObject *args)
{
    (void)self;
    (void)args;
    int pgid = 0;
    int err = sinomo_proc_getpgrp(&pgid);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return PyLong_FromLong(pgid);
}

static PyObject *
sinomo_getsid(PyObject *self, PyObject *args)
{
    (void)self;
    int pid = 0;
    if (!PyArg_ParseTuple(args, "i:getsid", &pid)) {
        return NULL;
    }
    int sid = 0;
    int err = sinomo_proc_getsid(pid, &sid);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return PyLong_FromLong(sid);
}

static PyObject *
sinomo_setpgid(PyObject *self, PyObject *args)
{
    (void)self;
    int pid = 0;
    int pgid = 0;
    if (!PyArg_ParseTuple(args, "ii:setpgid", &pid, &pgid)) {
        return NULL;
    }
    int err = sinomo_proc_setpgid(pid, pgid);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
sinomo_setpgrp(PyObject *self, PyObject *args)
{
    (void)self;
    (void)args;
    int err = sinomo_proc_setpgid(0, 0);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
sinomo_setsid(PyObject *self, PyObject *args)
{
    (void)self;
    (void)args;
    int sid = 0;
    int err = sinomo_proc_setsid(&sid);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return PyLong_FromLong(sid);
}

static PyObject *
sinomo_tcgetpgrp(PyObject *self, PyObject *args)
{
    (void)self;
    int fd = -1;
    if (!PyArg_ParseTuple(args, "i:tcgetpgrp", &fd)) {
        return NULL;
    }
    int pgid = 0;
    int err = sinomo_proc_tcgetpgrp(fd, &pgid);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return PyLong_FromLong(pgid);
}

static PyObject *
sinomo_tcgetsid(PyObject *self, PyObject *args)
{
    (void)self;
    int fd = -1;
    if (!PyArg_ParseTuple(args, "i:tcgetsid", &fd)) {
        return NULL;
    }
    int sid = 0;
    int err = sinomo_proc_tcgetsid(fd, &sid);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    return PyLong_FromLong(sid);
}

static PyObject *
sinomo_tcsetpgrp(PyObject *self, PyObject *args)
{
    (void)self;
    int fd = -1;
    int pgid = 0;
    if (!PyArg_ParseTuple(args, "ii:tcsetpgrp", &fd, &pgid)) {
        return NULL;
    }
    int err = sinomo_proc_tcsetpgrp(fd, pgid);
    if (err != 0) {
        errno = err;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyMethodDef sinomo_subprocess_methods[] = {
    {"spawn_wait", sinomo_spawn_wait, METH_VARARGS, "Spawn a child Python process in the Sinomo sandbox and wait for it."},
    {"spawn_wait_io", sinomo_spawn_wait_io, METH_VARARGS, "Spawn a child Python process in the Sinomo sandbox with file-backed stdio and wait for it."},
    {"umask", sinomo_umask, METH_VARARGS, "Set the Sinomo WASIX process umask and return the previous mask."},
    {"chmod", sinomo_chmod, METH_VARARGS, "Set permissions on a Sinomo WASIX sandbox path."},
    {"fchmod", sinomo_fchmod, METH_VARARGS, "Set permissions on a Sinomo WASIX sandbox file descriptor."},
    {"chown", sinomo_chown, METH_VARARGS, "Set ownership on a Sinomo WASIX sandbox path."},
    {"fchown", sinomo_fchown, METH_VARARGS, "Set ownership on a Sinomo WASIX sandbox file descriptor."},
    {"getuid", sinomo_getuid, METH_NOARGS, "Return the Sinomo WASIX sandbox uid."},
    {"getgid", sinomo_getgid, METH_NOARGS, "Return the Sinomo WASIX sandbox gid."},
    {"getgroups", sinomo_getgroups, METH_NOARGS, "Return the Sinomo WASIX sandbox supplementary groups."},
    {"getpid", sinomo_getpid, METH_NOARGS, "Return the current Sinomo WASIX process id."},
    {"getppid", sinomo_getppid, METH_NOARGS, "Return the current Sinomo WASIX parent process id."},
    {"kill", sinomo_kill, METH_VARARGS, "Send a WASIX signal to a sandbox process or process group."},
    {"waitpid", sinomo_waitpid, METH_VARARGS, "Wait for a WASIX child process."},
    {"wait", sinomo_wait, METH_NOARGS, "Wait for any WASIX child process."},
    {"getpgid", sinomo_getpgid, METH_VARARGS, "Return a Sinomo WASIX process group id."},
    {"getpgrp", sinomo_getpgrp, METH_NOARGS, "Return the current Sinomo WASIX process group id."},
    {"getsid", sinomo_getsid, METH_VARARGS, "Return a Sinomo WASIX session id."},
    {"setpgid", sinomo_setpgid, METH_VARARGS, "Set a Sinomo WASIX process group id."},
    {"setpgrp", sinomo_setpgrp, METH_NOARGS, "Put the current Sinomo WASIX process in its own process group."},
    {"setsid", sinomo_setsid, METH_NOARGS, "Create a Sinomo WASIX session."},
    {"tcgetpgrp", sinomo_tcgetpgrp, METH_VARARGS, "Return a terminal foreground process group."},
    {"tcgetsid", sinomo_tcgetsid, METH_VARARGS, "Return a terminal session id."},
    {"tcsetpgrp", sinomo_tcsetpgrp, METH_VARARGS, "Set a terminal foreground process group."},
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
