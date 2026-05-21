//! POSIX-like functions supporting absolute path arguments, implemented in
//! terms of `__wasilibc_find_relpath` and `*at`-style functions.

#include <stdarg.h>
#include <stdint.h>
#include <errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <grp.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <utime.h>
#include <wasi/api.h>
#include <wasi/libc.h>
#include <wasi/libc-find-relpath.h>
#include <wasi/libc-nocwd.h>

#ifndef SINOMO_PTHREAD_DEBUG
#define SINOMO_PTHREAD_DEBUG 0
#endif
#define SINOMO_PTHREAD_LOG(...) do { if (SINOMO_PTHREAD_DEBUG) fprintf(stderr, __VA_ARGS__); } while (0)
#define SINOMO_PTHREAD_ENV_LOG(...) do { if (getenv("SINOMO_PTHREAD_LOG") != NULL) fprintf(stderr, __VA_ARGS__); } while (0)

#ifndef SIGCANCEL
#define SIGCANCEL 32
#endif

#define SINOMO_WASIX_DEFAULT_THREAD_STACK_SIZE (128u * 1024u)

int32_t __imported_wasix_fd_chmod(int32_t fd, uint32_t mode) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("fd_chmod")
));

int32_t __imported_wasix_path_chmod_at(int32_t fd, const char *path, int32_t path_len, uint32_t mode) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_chmod_at")
));

int32_t __imported_wasix_fd_chown(int32_t fd, uint32_t uid, uint32_t gid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("fd_chown")
));

int32_t __imported_wasix_path_chown_at(int32_t fd, const char *path, int32_t path_len, uint32_t uid, uint32_t gid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_chown_at")
));

int32_t __imported_wasix_path_touch_at(int32_t fd, const char *path, int32_t path_len, uint32_t touch_self, uint32_t touch_parent) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_touch_at")
));

int32_t __imported_wasix_fd_touch(int32_t fd) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("fd_touch")
));

int32_t __imported_wasix_path_symlink_at(int32_t fd, const char *path, int32_t path_len, const char *target, int32_t target_len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_symlink_at")
));

int32_t __imported_wasix_path_read_symlink_at(int32_t fd, const char *path, int32_t path_len, char *buf, int32_t buf_len, uint32_t *used) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_read_symlink_at")
));

int32_t __imported_wasix_path_unlink_symlink_at(int32_t fd, const char *path, int32_t path_len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_unlink_symlink_at")
));

int32_t __imported_wasix_proc_getresuid(uint32_t *ruid, uint32_t *euid, uint32_t *suid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("proc_getresuid")
));

int32_t __imported_wasix_proc_getresgid(uint32_t *rgid, uint32_t *egid, uint32_t *sgid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("proc_getresgid")
));

int32_t __imported_wasix_proc_setresuid(uint32_t ruid, uint32_t euid, uint32_t suid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("proc_setresuid")
));

int32_t __imported_wasix_proc_setresgid(uint32_t rgid, uint32_t egid, uint32_t sgid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("proc_setresgid")
));

int32_t __imported_wasix_proc_getgroups(uint32_t *groups, uint32_t max_groups, uint32_t *count) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("proc_getgroups")
));

int32_t __imported_wasix_proc_setgroups(const uint32_t *groups, uint32_t count) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("proc_setgroups")
));

int32_t __imported_wasix_chdir(const char *path, int32_t path_len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("chdir")
));

int32_t __imported_wasix_fchdir(int32_t fd) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("fchdir")
));

int32_t __imported_wasix_fork(void) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("fork")
));

int32_t __imported_wasix_sched_get_priority_max(int32_t policy) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("sched_get_priority_max")
));

int32_t __imported_wasix_sched_get_priority_min(int32_t policy) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("sched_get_priority_min")
));

int32_t __imported_wasix_sched_getscheduler(int32_t pid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("sched_getscheduler")
));

int32_t __imported_wasix_sched_getparam(int32_t pid, int32_t *priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("sched_getparam")
));

int32_t __imported_wasix_sched_setscheduler(int32_t pid, int32_t policy, int32_t priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("sched_setscheduler")
));

int32_t __imported_wasix_signal_mask_set(uint64_t low, uint64_t high) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("signal_mask_set")
));

int32_t __imported_wasix_signal_mask_get(uint64_t *low, uint64_t *high) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("signal_mask_get")
));

int32_t __imported_wasix_thread_signal(uint32_t tid, uint32_t sig) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("thread_signal")
));

int32_t __imported_wasix_thread_sleep(uint64_t duration) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("thread_sleep")
));

int32_t __imported_wasix_thread_join(uint32_t tid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("thread_join")
));

int32_t __sinomo_wasix_shared_memory_sync_from(void) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_memory_sync_from")
));
int32_t __sinomo_wasix_shared_memory_sync_from_preserving_futexes(void) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_memory_sync_from_preserving_futexes")
));
int32_t __sinomo_wasix_shared_memory_sync_to(void) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_memory_sync_to")
));
int32_t __sinomo_wasix_shared_futex_hint(uintptr_t offset, uintptr_t len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_futex_hint")
));
int32_t __sinomo_wasix_shared_futex_unhint(uintptr_t offset, uintptr_t len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_futex_unhint")
));
uint32_t __sinomo_wasix_shared_memory_is_cross_process(void) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_memory_is_cross_process")
));
uint32_t __sinomo_wasix_shared_barrier_generation(uintptr_t offset) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_barrier_generation")
));
int32_t __sinomo_wasix_shared_barrier_arrive(uintptr_t offset, uint32_t limit) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_barrier_arrive")
));
int32_t __sinomo_wasix_shared_barrier_release(uintptr_t offset) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_barrier_release")
));
int32_t __sinomo_wasix_shared_rwlock_try_rdlock(uintptr_t offset, int32_t policy, int32_t priority, uint32_t registered_waiter) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_try_rdlock")
));
int32_t __sinomo_wasix_shared_rwlock_try_wrlock(uintptr_t offset, uint32_t registered_waiter, int32_t policy, int32_t priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_try_wrlock")
));
int32_t __sinomo_wasix_shared_rwlock_register_writer(uintptr_t offset, int32_t policy, int32_t priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_register_writer")
));
int32_t __sinomo_wasix_shared_rwlock_unregister_writer(uintptr_t offset, int32_t policy, int32_t priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_unregister_writer")
));
int32_t __sinomo_wasix_shared_rwlock_register_reader(uintptr_t offset, int32_t policy, int32_t priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_register_reader")
));
int32_t __sinomo_wasix_shared_rwlock_unregister_reader(uintptr_t offset, int32_t policy, int32_t priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_unregister_reader")
));
int32_t __sinomo_wasix_shared_rwlock_unlock(uintptr_t offset) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_unlock")
));
int32_t __sinomo_wasix_shared_rwlock_wait(uintptr_t offset, int64_t timeout_ns) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_wait")
));
int32_t __sinomo_wasix_shared_rwlock_destroy(uintptr_t offset) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_destroy")
));

int __wasilibc_futex_wait_wasix(volatile void *addr, int op, int expected, int64_t max_wait_ns);
int __wasilibc_futex_wake_wasix(int *futex, int cnt);
int munmap(void *addr, size_t len);

static mode_t sinomo_wasix_umask = 0;
static uid_t sinomo_wasix_ruid = 0;
static uid_t sinomo_wasix_euid = 0;
static uid_t sinomo_wasix_suid = 0;
static gid_t sinomo_wasix_rgid = 0;
static gid_t sinomo_wasix_egid = 0;
static gid_t sinomo_wasix_sgid = 0;
static __thread uint64_t sinomo_wasix_pending_signal_low = 0;
static __thread uint64_t sinomo_wasix_pending_signal_high = 0;
static __thread int sinomo_wasix_tls_sched_valid = 0;
static __thread int sinomo_wasix_tls_sched_policy = SCHED_OTHER;
static __thread int sinomo_wasix_tls_sched_priority = 0;

static int normalize_absolute_path(const char *path, char **normalized_out) {
    size_t len = strlen(path);
    char *normalized = malloc(len + 2);
    if (normalized == NULL) {
        errno = ENOMEM;
        return -1;
    }

    size_t out_len = 0;
    normalized[out_len++] = '/';
    const char *cursor = path;
    while (*cursor != '\0') {
        while (*cursor == '/')
            cursor++;
        const char *component = cursor;
        while (*cursor != '\0' && *cursor != '/')
            cursor++;
        size_t component_len = (size_t)(cursor - component);
        if (component_len == 0 ||
            (component_len == 1 && component[0] == '.')) {
            continue;
        }
        if (component_len == 2 && component[0] == '.' && component[1] == '.') {
            if (out_len > 1) {
                out_len--;
                while (out_len > 1 && normalized[out_len - 1] != '/')
                    out_len--;
            }
            continue;
        }
        if (out_len > 1)
            normalized[out_len++] = '/';
        memcpy(normalized + out_len, component, component_len);
        out_len += component_len;
    }
    normalized[out_len] = '\0';
    *normalized_out = normalized;
    return 0;
}

static int absolute_path_from_cwd(const char *path, char **absolute_out) {
    if (path[0] == '/')
        return normalize_absolute_path(path, absolute_out);

    size_t cwd_len = 128;
    char *cwd = NULL;
    for (;;) {
        char *grown = realloc(cwd, cwd_len);
        if (grown == NULL) {
            free(cwd);
            errno = ENOMEM;
            return -1;
        }
        cwd = grown;
        if (getcwd(cwd, cwd_len) != NULL)
            break;
        if (errno != ERANGE) {
            free(cwd);
            return -1;
        }
        cwd_len *= 2;
    }

    size_t path_len = strlen(path);
    size_t joined_len = strlen(cwd) + 1 + path_len + 1;
    char *joined = malloc(joined_len);
    if (joined == NULL) {
        free(cwd);
        errno = ENOMEM;
        return -1;
    }
    snprintf(joined, joined_len, "%s/%s", cwd, path);
    free(cwd);

    int result = normalize_absolute_path(joined, absolute_out);
    free(joined);
    return result;
}

int __wasilibc_find_relpath_alloc(
    const char *path,
    const char **abs,
    char **relative,
    size_t *relative_len,
    int can_realloc
) {
    const char *local_abs = NULL;
    const char *relative_const = NULL;
    char *absolute = NULL;
    char *candidate = *relative;
    size_t candidate_len = *relative_len;

    if (abs == NULL)
        abs = &local_abs;
    if (absolute_path_from_cwd(path, &absolute) != 0)
        return -1;

    int fd = __wasilibc_find_abspath(absolute, abs, &relative_const);
    if (fd < 0) {
        free(absolute);
        return -1;
    }

    size_t needed = strlen(relative_const) + 1;
    if (candidate_len < needed) {
        if (!can_realloc) {
            free(absolute);
            errno = ERANGE;
            return -1;
        }
        char *grown = realloc(candidate, needed);
        if (grown == NULL) {
            free(absolute);
            errno = ENOMEM;
            return -1;
        }
        candidate = grown;
        candidate_len = needed;
    }
    memcpy(candidate, relative_const, needed);
    free(absolute);
    *relative = candidate;
    *relative_len = candidate_len;
    return fd;
}

mode_t __sinomo_wasix_get_umask(void) {
    return sinomo_wasix_umask;
}

uid_t __sinomo_wasix_geteuid(void);
gid_t __sinomo_wasix_getegid(void);

static int find_relpath2(const char *path, char **relative, size_t *relative_len) {
    const char *abs;
    return __wasilibc_find_relpath_alloc(path, &abs, relative, relative_len, 1);
}

static int find_relpath(const char *path, char **relative) {
    static __thread char *relative_buf = NULL;
    static __thread size_t relative_buf_len = 0;
    int fd = find_relpath2(path, &relative_buf, &relative_buf_len);
    *relative = relative_buf;
    return fd;
}

static int find_relpath_alt(const char *path, char **relative) {
    static __thread char *relative_buf = NULL;
    static __thread size_t relative_buf_len = 0;
    int fd = find_relpath2(path, &relative_buf, &relative_buf_len);
    *relative = relative_buf;
    return fd;
}

static int chmodat_guest(int fd, const char *path, mode_t mode) {
    int32_t error = __imported_wasix_path_chmod_at(fd, path, strlen(path), mode & 07777);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

static int chownat_guest(int fd, const char *path, uid_t uid, gid_t gid) {
    int32_t error = __imported_wasix_path_chown_at(fd, path, strlen(path), uid, gid);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

static void touchat_guest(int fd, const char *path, int touch_self, int touch_parent) {
    (void)__imported_wasix_path_touch_at(fd, path, strlen(path), touch_self ? 1 : 0, touch_parent ? 1 : 0);
}

void __sinomo_wasix_touchat(int fd, const char *path, int touch_self, int touch_parent) {
    touchat_guest(fd, path, touch_self, touch_parent);
}

int __sinomo_wasix_symlinkat(const char *target, int fd, const char *path) {
    int32_t error = __imported_wasix_path_symlink_at(
        fd, path, strlen(path), target, strlen(target));
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

ssize_t __sinomo_wasix_readlinkat(int fd, const char *path, char *buf, size_t bufsize) {
    uint32_t used = 0;
    int32_t error = __imported_wasix_path_read_symlink_at(
        fd, path, strlen(path), buf, bufsize, &used);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return (ssize_t)used;
}

int __sinomo_wasix_unlink_symlinkat(int fd, const char *path) {
    int32_t error = __imported_wasix_path_unlink_symlink_at(fd, path, strlen(path));
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

uid_t __sinomo_wasix_geteuid(void) {
    return sinomo_wasix_euid;
}

gid_t __sinomo_wasix_getegid(void) {
    return sinomo_wasix_egid;
}

int chdir(const char *path) {
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    int32_t error = __imported_wasix_chdir(path, (int32_t)strlen(path));
    if (error < 0) {
        errno = -error;
        return -1;
    }
    if (error > 0) {
        errno = error;
        return -1;
    }
    return 0;
}

int fchdir(int fd) {
    int32_t error = __imported_wasix_fchdir(fd);
    if (error < 0) {
        errno = -error;
        return -1;
    }
    if (error > 0) {
        errno = error;
        return -1;
    }
    return 0;
}

pid_t fork(void) {
    int32_t result = __imported_wasix_fork();
    if (result < 0) {
        errno = -result;
        return -1;
    }
    return (pid_t)result;
}

int sched_get_priority_max(int policy) {
    int32_t result = __imported_wasix_sched_get_priority_max(policy);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    return result;
}

int sched_get_priority_min(int policy) {
    int32_t result = __imported_wasix_sched_get_priority_min(policy);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    return result;
}

int pthread_attr_getschedpolicy(const pthread_attr_t *restrict attr, int *restrict policy) {
    if (attr == NULL || policy == NULL)
        return EINVAL;
    *policy = attr->__u.__i[5];
    return 0;
}

int pthread_attr_getschedparam(const pthread_attr_t *restrict attr, struct sched_param *restrict param) {
    if (attr == NULL || param == NULL)
        return EINVAL;
    param->sched_priority = attr->__u.__i[6];
    return 0;
}

int pthread_attr_getdetachstate(const pthread_attr_t *attr, int *state) {
    if (attr == NULL || state == NULL)
        return EINVAL;
    *state = attr->__u.__i[3];
    return 0;
}

int pthread_attr_getguardsize(const pthread_attr_t *restrict attr, size_t *restrict size) {
    if (attr == NULL || size == NULL)
        return EINVAL;
    *size = attr->__u.__s[1];
    return 0;
}

int pthread_attr_getinheritsched(const pthread_attr_t *restrict attr, int *restrict inherit) {
    if (attr == NULL || inherit == NULL)
        return EINVAL;
    *inherit = attr->__u.__i[4];
    return 0;
}

int pthread_attr_getscope(const pthread_attr_t *restrict attr, int *restrict scope) {
    if (attr == NULL || scope == NULL)
        return EINVAL;
    *scope = PTHREAD_SCOPE_SYSTEM;
    return 0;
}

int pthread_attr_getstack(const pthread_attr_t *restrict attr, void **restrict addr, size_t *restrict size) {
    if (attr == NULL || addr == NULL || size == NULL)
        return EINVAL;
    *size = attr->__u.__s[0];
    *addr = attr->__u.__s[2] == 0 ? NULL : (void *)(attr->__u.__s[2] - *size);
    return 0;
}

int pthread_attr_getstacksize(const pthread_attr_t *restrict attr, size_t *restrict size) {
    if (attr == NULL || size == NULL)
        return EINVAL;
    *size = attr->__u.__s[0];
    return 0;
}

int pthread_attr_setinheritsched(pthread_attr_t *attr, int inherit) {
    if (attr == NULL)
        return EINVAL;
    if (inherit != PTHREAD_INHERIT_SCHED && inherit != PTHREAD_EXPLICIT_SCHED)
        return EINVAL;
    attr->__u.__i[4] = inherit;
    return 0;
}

int pthread_attr_setschedparam(pthread_attr_t *restrict attr, const struct sched_param *restrict param) {
    if (attr == NULL || param == NULL)
        return EINVAL;
    int policy = attr->__u.__i[5];
    int min = sched_get_priority_min(policy);
    int max = sched_get_priority_max(policy);
    if (min == -1 || max == -1)
        return EINVAL;
    if (param->sched_priority < min || param->sched_priority > max)
        return EINVAL;
    attr->__u.__i[6] = param->sched_priority;
    return 0;
}

int pthread_attr_setschedpolicy(pthread_attr_t *attr, int policy) {
    if (attr == NULL)
        return EINVAL;
    switch (policy) {
    case SCHED_OTHER:
    case SCHED_FIFO:
    case SCHED_RR:
    case SCHED_BATCH:
    case SCHED_IDLE:
    case SCHED_DEADLINE:
        attr->__u.__i[5] = policy;
        return 0;
    default:
        if (policy < 0)
            return ENOTSUP;
        return EINVAL;
    }
}

#define SINOMO_BARRIER_MAGIC 0x53424152
#define SINOMO_BARRIER_LIMIT_WORD 0
#define SINOMO_BARRIER_COUNT_WORD 1
#define SINOMO_BARRIER_GEN_WORD 2
#define SINOMO_BARRIER_RELEASE_WORD 3
#define SINOMO_BARRIER_MAGIC_WORD 4

int pthread_barrier_init(pthread_barrier_t *restrict barrier, const pthread_barrierattr_t *restrict attr, unsigned count) {
    if (barrier == NULL || count == 0)
        return EINVAL;
    volatile int *words = barrier->__u.__vi;
    words[SINOMO_BARRIER_LIMIT_WORD] = (int)count;
    words[SINOMO_BARRIER_COUNT_WORD] = 0;
    words[SINOMO_BARRIER_GEN_WORD] = 0;
    words[SINOMO_BARRIER_RELEASE_WORD] = 0;
    words[SINOMO_BARRIER_MAGIC_WORD] = SINOMO_BARRIER_MAGIC;
    if (attr != NULL && attr->__attr == PTHREAD_PROCESS_SHARED) {
        int32_t hint = __sinomo_wasix_shared_futex_hint((uintptr_t)barrier, (uintptr_t)sizeof(*barrier));
        if (hint != 0)
            return (int)hint;
    }
    return 0;
}

int pthread_barrier_destroy(pthread_barrier_t *barrier) {
    if (barrier == NULL)
        return EINVAL;
    volatile int *words = barrier->__u.__vi;
    if (words[SINOMO_BARRIER_MAGIC_WORD] != SINOMO_BARRIER_MAGIC)
        return EINVAL;
    if (__atomic_load_n(&words[SINOMO_BARRIER_COUNT_WORD], __ATOMIC_SEQ_CST) != 0)
        return EBUSY;
    if (words[SINOMO_BARRIER_MAGIC_WORD] == SINOMO_BARRIER_MAGIC)
        (void)__sinomo_wasix_shared_futex_unhint((uintptr_t)barrier, (uintptr_t)sizeof(*barrier));
    memset(barrier, 0, sizeof(*barrier));
    return 0;
}

int pthread_barrier_wait(pthread_barrier_t *barrier) {
    if (barrier == NULL)
        return EINVAL;
    volatile int *words = barrier->__u.__vi;
    if (words[SINOMO_BARRIER_MAGIC_WORD] != SINOMO_BARRIER_MAGIC)
        return EINVAL;

    int limit = __atomic_load_n(&words[SINOMO_BARRIER_LIMIT_WORD], __ATOMIC_SEQ_CST);
    if (limit <= 0)
        return EINVAL;

    if (__sinomo_wasix_shared_memory_is_cross_process() != 0) {
        int generation = (int)__sinomo_wasix_shared_barrier_generation((uintptr_t)barrier);
        words[SINOMO_BARRIER_GEN_WORD] = generation;
        int32_t arrive = __sinomo_wasix_shared_barrier_arrive((uintptr_t)barrier, (uint32_t)limit);
        if (arrive == PTHREAD_BARRIER_SERIAL_THREAD) {
            words[SINOMO_BARRIER_GEN_WORD] = generation + 1;
            (void)__wasilibc_futex_wake_wasix((int *)&words[SINOMO_BARRIER_GEN_WORD], 1);
            return PTHREAD_BARRIER_SERIAL_THREAD;
        }
        if (arrive != 0)
            return (int)arrive;

        struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000L};
        for (;;) {
            int current = (int)__sinomo_wasix_shared_barrier_generation((uintptr_t)barrier);
            if (current != generation) {
                words[SINOMO_BARRIER_GEN_WORD] = current;
                int32_t remaining = __sinomo_wasix_shared_barrier_release((uintptr_t)barrier);
                if (remaining > 0)
                    (void)__wasilibc_futex_wake_wasix((int *)&words[SINOMO_BARRIER_GEN_WORD], 1);
                return 0;
            }
            while (nanosleep(&pause, &pause) != 0 && errno == EINTR) {
            }
            pause.tv_sec = 0;
            pause.tv_nsec = 1000000L;
        }
    }

    int generation = __atomic_load_n(&words[SINOMO_BARRIER_GEN_WORD], __ATOMIC_SEQ_CST);
    int arrived = __atomic_add_fetch(&words[SINOMO_BARRIER_COUNT_WORD], 1, __ATOMIC_SEQ_CST);
    if (arrived == limit) {
        __atomic_store_n(&words[SINOMO_BARRIER_COUNT_WORD], 0, __ATOMIC_SEQ_CST);
        __atomic_store_n(&words[SINOMO_BARRIER_RELEASE_WORD], limit - 1, __ATOMIC_SEQ_CST);
        __atomic_add_fetch(&words[SINOMO_BARRIER_GEN_WORD], 1, __ATOMIC_SEQ_CST);
        (void)__wasilibc_futex_wake_wasix((int *)&words[SINOMO_BARRIER_GEN_WORD], 1);
        return PTHREAD_BARRIER_SERIAL_THREAD;
    }
    if (arrived > limit) {
        (void)__atomic_sub_fetch(&words[SINOMO_BARRIER_COUNT_WORD], 1, __ATOMIC_SEQ_CST);
        return EINVAL;
    }

    for (;;) {
        int current = __atomic_load_n(&words[SINOMO_BARRIER_GEN_WORD], __ATOMIC_SEQ_CST);
        if (current != generation) {
            int remaining = __atomic_sub_fetch(&words[SINOMO_BARRIER_RELEASE_WORD], 1, __ATOMIC_SEQ_CST);
            if (remaining > 0)
                (void)__wasilibc_futex_wake_wasix((int *)&words[SINOMO_BARRIER_GEN_WORD], 1);
            return 0;
        }
        int wait_ret = __wasilibc_futex_wait_wasix(&words[SINOMO_BARRIER_GEN_WORD], 0, generation, -1);
        if (wait_ret == 0 || wait_ret == -EAGAIN || wait_ret == -EINTR || wait_ret == -ECANCELED)
            continue;
        if (wait_ret < 0)
            return -wait_ret;
        return wait_ret;
    }
}

int pthread_barrierattr_init(pthread_barrierattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr = PTHREAD_PROCESS_PRIVATE;
    return 0;
}

int pthread_barrierattr_destroy(pthread_barrierattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr = 0;
    return 0;
}

int pthread_barrierattr_setpshared(pthread_barrierattr_t *attr, int pshared) {
    if (attr == NULL)
        return EINVAL;
    switch (pshared) {
    case PTHREAD_PROCESS_PRIVATE:
    case PTHREAD_PROCESS_SHARED:
        attr->__attr = (unsigned)pshared;
        return 0;
    default:
        return EINVAL;
    }
}

int pthread_barrierattr_getpshared(const pthread_barrierattr_t *restrict attr, int *restrict pshared) {
    if (attr == NULL || pshared == NULL)
        return EINVAL;
    *pshared = (int)attr->__attr;
    return 0;
}

int pthread_condattr_init(pthread_condattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr = CLOCK_REALTIME;
    return 0;
}

int pthread_condattr_destroy(pthread_condattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr = 0;
    return 0;
}

int pthread_condattr_setpshared(pthread_condattr_t *attr, int pshared) {
    if (attr == NULL)
        return EINVAL;
    switch (pshared) {
    case PTHREAD_PROCESS_PRIVATE:
        attr->__attr &= 0x7fffffffu;
        return 0;
    case PTHREAD_PROCESS_SHARED:
        attr->__attr |= 0x80000000u;
        return 0;
    default:
        return EINVAL;
    }
}

int pthread_condattr_getpshared(const pthread_condattr_t *restrict attr, int *restrict pshared) {
    if (attr == NULL || pshared == NULL)
        return EINVAL;
    *pshared = attr->__attr >> 31;
    return 0;
}

int pthread_condattr_setclock(pthread_condattr_t *attr, clockid_t clock_id) {
    if (attr == NULL)
        return EINVAL;
    switch (clock_id) {
    case CLOCK_REALTIME:
    case CLOCK_MONOTONIC:
        attr->__attr = (attr->__attr & 0x80000000u) | ((unsigned)clock_id & 0x7fffffffu);
        return 0;
    default:
        return EINVAL;
    }
}

int pthread_condattr_getclock(const pthread_condattr_t *restrict attr, clockid_t *restrict clock_id) {
    if (attr == NULL || clock_id == NULL)
        return EINVAL;
    *clock_id = attr->__attr & 0x7fffffff;
    return 0;
}

int pthread_mutexattr_getprotocol(const pthread_mutexattr_t *restrict attr, int *restrict protocol) {
    if (attr == NULL || protocol == NULL)
        return EINVAL;
    *protocol = attr->__attr / 8U % 2;
    return 0;
}

int pthread_mutexattr_getpshared(const pthread_mutexattr_t *restrict attr, int *restrict pshared) {
    if (attr == NULL || pshared == NULL)
        return EINVAL;
    *pshared = attr->__attr / 128U % 2;
    return 0;
}

int pthread_mutexattr_getrobust(const pthread_mutexattr_t *restrict attr, int *restrict robust) {
    if (attr == NULL || robust == NULL)
        return EINVAL;
    *robust = attr->__attr / 4U % 2;
    return 0;
}

int pthread_mutexattr_gettype(const pthread_mutexattr_t *restrict attr, int *restrict type) {
    if (attr == NULL || type == NULL)
        return EINVAL;
    *type = attr->__attr & 3;
    return 0;
}

int pthread_mutexattr_init(pthread_mutexattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr = PTHREAD_MUTEX_DEFAULT;
    return 0;
}

int pthread_mutexattr_destroy(pthread_mutexattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr = 0;
    return 0;
}

int pthread_mutexattr_settype(pthread_mutexattr_t *attr, int type) {
    if (attr == NULL)
        return EINVAL;
    switch (type) {
    case PTHREAD_MUTEX_NORMAL:
    case PTHREAD_MUTEX_RECURSIVE:
    case PTHREAD_MUTEX_ERRORCHECK:
        attr->__attr = (attr->__attr & ~3U) | (unsigned)type;
        return 0;
    default:
        return EINVAL;
    }
}

int pthread_mutexattr_setpshared(pthread_mutexattr_t *attr, int pshared) {
    if (attr == NULL)
        return EINVAL;
    switch (pshared) {
    case PTHREAD_PROCESS_PRIVATE:
        attr->__attr &= ~128U;
        return 0;
    case PTHREAD_PROCESS_SHARED:
        attr->__attr |= 128U;
        return 0;
    default:
        return EINVAL;
    }
}

int pthread_rwlockattr_getpshared(const pthread_rwlockattr_t *restrict attr, int *restrict pshared) {
    if (attr == NULL || pshared == NULL)
        return EINVAL;
    *pshared = attr->__attr[0];
    return 0;
}

#define SINOMO_RWLOCK_MAGIC 0x5352574c
#define SINOMO_RWLOCK_STATE_WORD 0
#define SINOMO_RWLOCK_WRITER_WAITERS_WORD 1
#define SINOMO_RWLOCK_PSHARED_WORD 2
#define SINOMO_RWLOCK_MAGIC_WORD 3
#define SINOMO_RWLOCK_WRITER_PRIORITY_WORD 4
#define SINOMO_RWLOCK_WRITER_POLICY_WORD 5

static void sinomo_wasix_get_current_sched(int *policy, int *priority);

static int rwlock_is_shared(volatile int *words) {
    return words[SINOMO_RWLOCK_PSHARED_WORD] == PTHREAD_PROCESS_SHARED;
}

static int rwlock_sync_to_if_shared(volatile int *words) {
    if (!rwlock_is_shared(words) || __sinomo_wasix_shared_memory_is_cross_process() == 0)
        return 0;
    int32_t publish = __sinomo_wasix_shared_memory_sync_from_preserving_futexes();
    if (publish != 0)
        return (int)publish;
    int32_t err = __sinomo_wasix_shared_memory_sync_to();
    return err == 0 ? 0 : (int)err;
}

static int rwlock_sync_from_if_shared(volatile int *words) {
    if (!rwlock_is_shared(words) || __sinomo_wasix_shared_memory_is_cross_process() == 0)
        return 0;
    int32_t err = __sinomo_wasix_shared_memory_sync_from();
    return err == 0 ? 0 : (int)err;
}

static void rwlock_current_sched(int *policy, int *priority) {
    if (sinomo_wasix_tls_sched_valid) {
        *policy = sinomo_wasix_tls_sched_policy;
        *priority = sinomo_wasix_tls_sched_priority;
        return;
    }
    pthread_t self = pthread_self();
    struct sched_param param;
    int current_policy = SCHED_OTHER;
    param.sched_priority = 0;
    if (pthread_getschedparam(self, &current_policy, &param) != 0)
        sinomo_wasix_get_current_sched(&current_policy, &param.sched_priority);
    *policy = current_policy;
    *priority = param.sched_priority;
}

static int rwlock_policy_orders_readers(int policy) {
    return policy == SCHED_FIFO || policy == SCHED_RR;
}

static int rwlock_reader_must_wait_for_writer(volatile int *words) {
    int waiters = __atomic_load_n(&words[SINOMO_RWLOCK_WRITER_WAITERS_WORD], __ATOMIC_SEQ_CST);
    if (waiters <= 0)
        return 0;
    int writer_policy = __atomic_load_n(&words[SINOMO_RWLOCK_WRITER_POLICY_WORD], __ATOMIC_SEQ_CST);
    if (!rwlock_policy_orders_readers(writer_policy))
        return 0;
    int policy = SCHED_OTHER;
    int priority = 0;
    rwlock_current_sched(&policy, &priority);
    if (!rwlock_policy_orders_readers(policy))
        return 0;
    int writer_priority = __atomic_load_n(&words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD], __ATOMIC_SEQ_CST);
    return priority <= writer_priority;
}

static void rwlock_register_writer_waiter(volatile int *words) {
    int policy = SCHED_OTHER;
    int priority = 0;
    rwlock_current_sched(&policy, &priority);
    __atomic_fetch_add(&words[SINOMO_RWLOCK_WRITER_WAITERS_WORD], 1, __ATOMIC_SEQ_CST);
    int current_priority = __atomic_load_n(&words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD], __ATOMIC_SEQ_CST);
    while (priority > current_priority &&
           !__atomic_compare_exchange_n(&words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD], &current_priority, priority, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
    }
    if (rwlock_policy_orders_readers(policy))
        __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_POLICY_WORD], policy, __ATOMIC_SEQ_CST);
}

static void rwlock_unregister_writer_waiter(volatile int *words) {
    int remaining = __atomic_sub_fetch(&words[SINOMO_RWLOCK_WRITER_WAITERS_WORD], 1, __ATOMIC_SEQ_CST);
    if (remaining <= 0) {
        __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_WAITERS_WORD], 0, __ATOMIC_SEQ_CST);
        __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD], 0, __ATOMIC_SEQ_CST);
        __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_POLICY_WORD], SCHED_OTHER, __ATOMIC_SEQ_CST);
    }
}

static int rwlock_wait(pthread_rwlock_t *rwlock, volatile int *state_word, int expected, int shared, int64_t wait_ns) {
    if (shared && __sinomo_wasix_shared_memory_is_cross_process() != 0)
        return (int)__sinomo_wasix_shared_rwlock_wait((uintptr_t)rwlock, wait_ns);
    int64_t futex_wait_ns = wait_ns < 0 || wait_ns > 10000000LL ? 10000000LL : wait_ns;
    int wait_ret = __wasilibc_futex_wait_wasix(state_word, 0, expected, futex_wait_ns);
    if (wait_ret == 0 || wait_ret == -EAGAIN || wait_ret == -EINTR || wait_ret == -ETIMEDOUT || wait_ret == -ECANCELED)
        return 0;
    return wait_ret < 0 ? -wait_ret : wait_ret;
}

static int64_t rwlock_abstime_to_wait_ns(const struct timespec *abstime) {
    if (abstime == NULL)
        return -1;
    if (abstime->tv_nsec < 0 || abstime->tv_nsec >= 1000000000L)
        return INT64_MIN;
    struct timeval tv;
    if (gettimeofday(&tv, NULL) != 0)
        return INT64_MIN + 1;
    int64_t sec = (int64_t)abstime->tv_sec - (int64_t)tv.tv_sec;
    int64_t nsec = (int64_t)abstime->tv_nsec - ((int64_t)tv.tv_usec * 1000LL);
    int64_t total = sec * 1000000000LL + nsec;
    return total <= 0 ? 0 : total;
}

int pthread_rwlock_init(pthread_rwlock_t *restrict rwlock, const pthread_rwlockattr_t *restrict attr) {
    if (rwlock == NULL)
        return EINVAL;
    memset(rwlock, 0, sizeof(*rwlock));
    volatile int *words = rwlock->__u.__vi;
    words[SINOMO_RWLOCK_STATE_WORD] = 0;
    words[SINOMO_RWLOCK_WRITER_WAITERS_WORD] = 0;
    words[SINOMO_RWLOCK_PSHARED_WORD] = attr == NULL ? PTHREAD_PROCESS_PRIVATE : (int)attr->__attr[0];
    words[SINOMO_RWLOCK_MAGIC_WORD] = SINOMO_RWLOCK_MAGIC;
    words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD] = 0;
    words[SINOMO_RWLOCK_WRITER_POLICY_WORD] = SCHED_OTHER;
    if (rwlock_is_shared(words)) {
        int32_t hint = __sinomo_wasix_shared_futex_hint((uintptr_t)rwlock, (uintptr_t)sizeof(*rwlock));
        if (hint != 0)
            return (int)hint;
    }
    return 0;
}

int pthread_rwlock_destroy(pthread_rwlock_t *rwlock) {
    if (rwlock == NULL)
        return EINVAL;
    volatile int *words = rwlock->__u.__vi;
    if (words[SINOMO_RWLOCK_MAGIC_WORD] != SINOMO_RWLOCK_MAGIC)
        return EINVAL;
    int32_t destroy = __sinomo_wasix_shared_rwlock_destroy((uintptr_t)rwlock);
    if (destroy != 0)
        return (int)destroy;
    if (rwlock_is_shared(words))
        (void)__sinomo_wasix_shared_futex_unhint((uintptr_t)rwlock, (uintptr_t)sizeof(*rwlock));
    memset(rwlock, 0, sizeof(*rwlock));
    return 0;
}

int pthread_rwlock_tryrdlock(pthread_rwlock_t *rwlock) {
    if (rwlock == NULL)
        return EINVAL;
    volatile int *words = rwlock->__u.__vi;
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug rwlock_tryrd enter rwlock=%p words=[%d,%d,%d,%d]\n", (void *)rwlock, words[0], words[1], words[2], words[3]);
    if (words[SINOMO_RWLOCK_MAGIC_WORD] != SINOMO_RWLOCK_MAGIC)
        return EINVAL;
    int policy = SCHED_OTHER;
    int priority = 0;
    rwlock_current_sched(&policy, &priority);
    int32_t ret = __sinomo_wasix_shared_rwlock_try_rdlock((uintptr_t)rwlock, policy, priority, 0);
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug rwlock_tryrd result rwlock=%p ret=%d words=[%d,%d,%d,%d]\n", (void *)rwlock, ret, words[0], words[1], words[2], words[3]);
    return ret == 0 ? 0 : (int)ret;
}

int pthread_rwlock_trywrlock(pthread_rwlock_t *rwlock) {
    if (rwlock == NULL)
        return EINVAL;
    volatile int *words = rwlock->__u.__vi;
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug rwlock_trywr enter rwlock=%p words=[%d,%d,%d,%d]\n", (void *)rwlock, words[0], words[1], words[2], words[3]);
    if (words[SINOMO_RWLOCK_MAGIC_WORD] != SINOMO_RWLOCK_MAGIC)
        return EINVAL;
    int policy = SCHED_OTHER;
    int priority = 0;
    rwlock_current_sched(&policy, &priority);
    int32_t ret = __sinomo_wasix_shared_rwlock_try_wrlock((uintptr_t)rwlock, 0, policy, priority);
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug rwlock_trywr acquired rwlock=%p ret=%d words=[%d,%d,%d,%d]\n", (void *)rwlock, ret, words[0], words[1], words[2], words[3]);
    return ret == 0 ? 0 : (int)ret;
}

int pthread_rwlock_timedrdlock(pthread_rwlock_t *restrict rwlock, const struct timespec *restrict abstime) {
    if (rwlock == NULL)
        return EINVAL;
    volatile int *words = rwlock->__u.__vi;
    int policy = SCHED_OTHER;
    int priority = 0;
    rwlock_current_sched(&policy, &priority);
    int registered_waiter = 0;
    for (;;) {
        int ret = __sinomo_wasix_shared_rwlock_try_rdlock((uintptr_t)rwlock, policy, priority, registered_waiter ? 1U : 0U);
        if (ret == 0)
            return 0;
        if (ret != EBUSY) {
            if (registered_waiter)
                (void)__sinomo_wasix_shared_rwlock_unregister_reader((uintptr_t)rwlock, policy, priority);
            return ret;
        }
        int64_t wait_ns = rwlock_abstime_to_wait_ns(abstime);
        if (wait_ns == INT64_MIN)
            return EINVAL;
        if (wait_ns == INT64_MIN + 1)
            return errno == 0 ? EINVAL : errno;
        if (wait_ns == 0) {
            if (registered_waiter)
                (void)__sinomo_wasix_shared_rwlock_unregister_reader((uintptr_t)rwlock, policy, priority);
            return ETIMEDOUT;
        }
        if (!registered_waiter) {
            ret = __sinomo_wasix_shared_rwlock_register_reader((uintptr_t)rwlock, policy, priority);
            if (ret != 0)
                return ret;
            registered_waiter = 1;
        }
        int observed = __atomic_load_n(&words[SINOMO_RWLOCK_STATE_WORD], __ATOMIC_SEQ_CST);
        ret = rwlock_wait(rwlock, &words[SINOMO_RWLOCK_STATE_WORD], observed, rwlock_is_shared(words), wait_ns);
        if (ret != 0) {
            if (registered_waiter)
                (void)__sinomo_wasix_shared_rwlock_unregister_reader((uintptr_t)rwlock, policy, priority);
            return ret;
        }
    }
}

int pthread_rwlock_rdlock(pthread_rwlock_t *rwlock) {
    return pthread_rwlock_timedrdlock(rwlock, NULL);
}

int pthread_rwlock_timedwrlock(pthread_rwlock_t *restrict rwlock, const struct timespec *restrict abstime) {
    if (rwlock == NULL)
        return EINVAL;
    volatile int *words = rwlock->__u.__vi;
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug rwlock_wrlock enter rwlock=%p words=[%d,%d,%d,%d]\n", (void *)rwlock, words[0], words[1], words[2], words[3]);
    int registered_waiter = 0;
    for (;;) {
        int policy = SCHED_OTHER;
        int priority = 0;
        rwlock_current_sched(&policy, &priority);
        int ret = __sinomo_wasix_shared_rwlock_try_wrlock((uintptr_t)rwlock, registered_waiter ? 1U : 0U, policy, priority);
        if (ret == 0) {
            return 0;
        }
        if (ret != EBUSY) {
            if (registered_waiter)
                (void)__sinomo_wasix_shared_rwlock_unregister_writer((uintptr_t)rwlock, policy, priority);
            return ret;
        }
        int64_t wait_ns = rwlock_abstime_to_wait_ns(abstime);
        if (wait_ns == INT64_MIN)
            return EINVAL;
        if (wait_ns == INT64_MIN + 1)
            return errno == 0 ? EINVAL : errno;
        if (wait_ns == 0) {
            if (registered_waiter)
                (void)__sinomo_wasix_shared_rwlock_unregister_writer((uintptr_t)rwlock, policy, priority);
            return ETIMEDOUT;
        }
        if (!registered_waiter) {
            ret = __sinomo_wasix_shared_rwlock_register_writer((uintptr_t)rwlock, policy, priority);
            if (ret != 0)
                return ret;
            registered_waiter = 1;
        }
        int observed = __atomic_load_n(&words[SINOMO_RWLOCK_STATE_WORD], __ATOMIC_SEQ_CST);
        ret = rwlock_wait(rwlock, &words[SINOMO_RWLOCK_STATE_WORD], observed, rwlock_is_shared(words), wait_ns);
        if (ret != 0) {
            if (registered_waiter)
                (void)__sinomo_wasix_shared_rwlock_unregister_writer((uintptr_t)rwlock, policy, priority);
            return ret;
        }
    }
}

int pthread_rwlock_wrlock(pthread_rwlock_t *rwlock) {
    return pthread_rwlock_timedwrlock(rwlock, NULL);
}

int pthread_rwlock_unlock(pthread_rwlock_t *rwlock) {
    if (rwlock == NULL)
        return EINVAL;
    volatile int *words = rwlock->__u.__vi;
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug rwlock_unlock enter rwlock=%p words=[%d,%d,%d,%d]\n", (void *)rwlock, words[0], words[1], words[2], words[3]);
    if (words[SINOMO_RWLOCK_MAGIC_WORD] != SINOMO_RWLOCK_MAGIC)
        return EINVAL;
    int32_t ret = __sinomo_wasix_shared_rwlock_unlock((uintptr_t)rwlock);
    if (ret == 0)
        (void)__wasilibc_futex_wake_wasix((int *)&words[SINOMO_RWLOCK_STATE_WORD], INT_MAX);
    return ret == 0 ? 0 : (int)ret;
}

int __pthread_rwlock_rdlock(pthread_rwlock_t *rwlock) {
    return pthread_rwlock_rdlock(rwlock);
}

int __pthread_rwlock_wrlock(pthread_rwlock_t *rwlock) {
    return pthread_rwlock_wrlock(rwlock);
}

int __pthread_rwlock_unlock(pthread_rwlock_t *rwlock) {
    return pthread_rwlock_unlock(rwlock);
}

int __pthread_rwlock_tryrdlock(pthread_rwlock_t *rwlock) {
    return pthread_rwlock_tryrdlock(rwlock);
}

int __pthread_rwlock_trywrlock(pthread_rwlock_t *rwlock) {
    return pthread_rwlock_trywrlock(rwlock);
}

int pthread_rwlockattr_init(pthread_rwlockattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr[0] = PTHREAD_PROCESS_PRIVATE;
    attr->__attr[1] = 0;
    return 0;
}

int pthread_rwlockattr_destroy(pthread_rwlockattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr[0] = PTHREAD_PROCESS_PRIVATE;
    attr->__attr[1] = 0;
    return 0;
}

int pthread_rwlockattr_setpshared(pthread_rwlockattr_t *attr, int pshared) {
    if (attr == NULL)
        return EINVAL;
    switch (pshared) {
    case PTHREAD_PROCESS_PRIVATE:
    case PTHREAD_PROCESS_SHARED:
        attr->__attr[0] = (unsigned)pshared;
        return 0;
    default:
        return EINVAL;
    }
}

int pthread_getcpuclockid(pthread_t thread, clockid_t *clock_id) {
    (void)thread;
    if (clock_id == NULL)
        return EINVAL;
    *clock_id = CLOCK_THREAD_CPUTIME_ID;
    return 0;
}

int __pthread_create(
    pthread_t *restrict thread,
    const pthread_attr_t *restrict attr,
    void *(*start_routine)(void *),
    void *restrict arg
);

#define SINOMO_WASIX_PTHREAD_DETACHED_SLOTS 1024
static uintptr_t sinomo_wasix_detached_threads[SINOMO_WASIX_PTHREAD_DETACHED_SLOTS];

struct sinomo_wasix_thread_sched_record {
    uintptr_t thread;
    int tid;
    int policy;
    int priority;
};

static struct sinomo_wasix_thread_sched_record sinomo_wasix_thread_sched[SINOMO_WASIX_PTHREAD_DETACHED_SLOTS];
static int sinomo_wasix_pending_create_sched_valid = 0;
static int sinomo_wasix_pending_create_sched_policy = SCHED_OTHER;
static int sinomo_wasix_pending_create_sched_priority = 0;
static volatile int sinomo_wasix_created_thread_count = 0;
static volatile int sinomo_wasix_joined_thread_count = 0;

struct sinomo_wasix_pthread_tid_prefix {
    uintptr_t self;
    uintptr_t prev;
    uintptr_t next;
    uintptr_t sysinfo;
    uintptr_t canary;
    int32_t tid;
};

static size_t sinomo_wasix_detached_slot(pthread_t thread) {
    uintptr_t value = (uintptr_t)thread >> 4;
    value ^= value >> 12;
    value ^= value >> 24;
    return value % SINOMO_WASIX_PTHREAD_DETACHED_SLOTS;
}

static void sinomo_wasix_mark_detached_thread(pthread_t thread) {
    if (thread == NULL)
        return;
    sinomo_wasix_detached_threads[sinomo_wasix_detached_slot(thread)] = (uintptr_t)thread;
}

static void sinomo_wasix_clear_detached_thread(pthread_t thread) {
    if (thread == NULL)
        return;
    size_t slot = sinomo_wasix_detached_slot(thread);
    if (sinomo_wasix_detached_threads[slot] == (uintptr_t)thread)
        sinomo_wasix_detached_threads[slot] = 0;
}

static int sinomo_wasix_is_detached_thread(pthread_t thread) {
    if (thread == NULL)
        return 0;
    return sinomo_wasix_detached_threads[sinomo_wasix_detached_slot(thread)] == (uintptr_t)thread;
}

static int sinomo_wasix_valid_sched_policy(int policy) {
    switch (policy) {
    case SCHED_OTHER:
    case SCHED_FIFO:
    case SCHED_RR:
    case SCHED_BATCH:
    case SCHED_IDLE:
    case SCHED_DEADLINE:
        return 1;
    default:
        return 0;
    }
}

static void sinomo_wasix_get_current_sched(int *policy, int *priority) {
    if (sinomo_wasix_tls_sched_valid) {
        *policy = sinomo_wasix_tls_sched_policy;
        *priority = sinomo_wasix_tls_sched_priority;
        return;
    }
    int32_t sched_policy = __imported_wasix_sched_getscheduler(0);
    int32_t sched_priority = 0;
    if (sched_policy < 0)
        sched_policy = SCHED_OTHER;
    if (__imported_wasix_sched_getparam(0, &sched_priority) < 0)
        sched_priority = 0;
    *policy = sched_policy;
    *priority = sched_priority;
}

static int sinomo_wasix_get_thread_sched(pthread_t thread, int *policy, int *priority) {
    if (thread == NULL)
        return 0;
    size_t slot = sinomo_wasix_detached_slot(thread);
    if (sinomo_wasix_thread_sched[slot].thread != (uintptr_t)thread)
        return 0;
    *policy = sinomo_wasix_thread_sched[slot].policy;
    *priority = sinomo_wasix_thread_sched[slot].priority;
    return 1;
}

static int sinomo_wasix_get_thread_tid(pthread_t thread) {
    if (thread == NULL)
        return 0;
    size_t slot = sinomo_wasix_detached_slot(thread);
    if (sinomo_wasix_thread_sched[slot].thread != (uintptr_t)thread)
        return 0;
    return sinomo_wasix_thread_sched[slot].tid;
}

static void sinomo_wasix_set_thread_sched(pthread_t thread, int tid, int policy, int priority) {
    if (thread == NULL)
        return;
    size_t slot = sinomo_wasix_detached_slot(thread);
    sinomo_wasix_thread_sched[slot].thread = (uintptr_t)thread;
    sinomo_wasix_thread_sched[slot].tid = tid;
    sinomo_wasix_thread_sched[slot].policy = policy;
    sinomo_wasix_thread_sched[slot].priority = priority;
}

static void sinomo_wasix_clear_thread_sched(pthread_t thread) {
    if (thread == NULL)
        return;
    size_t slot = sinomo_wasix_detached_slot(thread);
    if (sinomo_wasix_thread_sched[slot].thread == (uintptr_t)thread)
        memset(&sinomo_wasix_thread_sched[slot], 0, sizeof(sinomo_wasix_thread_sched[slot]));
}

int pthread_getschedparam(pthread_t thread, int *restrict policy, struct sched_param *restrict param) {
    if (policy == NULL || param == NULL)
        return EINVAL;
    int priority = 0;
    if (!sinomo_wasix_get_thread_sched(thread, policy, &priority)) {
        for (int i = 0; i < 10000 && thread != NULL; i++) {
            if (sinomo_wasix_get_thread_sched(thread, policy, &priority))
                break;
            __asm__ __volatile__("" ::: "memory");
        }
    }
    if (!sinomo_wasix_get_thread_sched(thread, policy, &priority) && sinomo_wasix_pending_create_sched_valid) {
        *policy = sinomo_wasix_pending_create_sched_policy;
        priority = sinomo_wasix_pending_create_sched_priority;
    } else if (!sinomo_wasix_get_thread_sched(thread, policy, &priority)) {
        sinomo_wasix_get_current_sched(policy, &priority);
    }
    param->sched_priority = priority;
    return 0;
}

int pthread_setschedparam(pthread_t thread, int policy, const struct sched_param *param) {
    if (param == NULL)
        return EINVAL;
    if (!sinomo_wasix_valid_sched_policy(policy))
        return EINVAL;
    int min = sched_get_priority_min(policy);
    int max = sched_get_priority_max(policy);
    if (min == -1 || max == -1)
        return EINVAL;
    if (param->sched_priority < min || param->sched_priority > max)
        return EINVAL;
    sinomo_wasix_set_thread_sched(thread, sinomo_wasix_get_thread_tid(thread), policy, param->sched_priority);
    if (pthread_equal(thread, pthread_self())) {
        sinomo_wasix_tls_sched_valid = 1;
        sinomo_wasix_tls_sched_policy = policy;
        sinomo_wasix_tls_sched_priority = param->sched_priority;
    }
    return 0;
}

int pthread_setschedprio(pthread_t thread, int priority) {
    int policy = SCHED_OTHER;
    int current_priority = 0;
    if (!sinomo_wasix_get_thread_sched(thread, &policy, &current_priority))
        sinomo_wasix_get_current_sched(&policy, &current_priority);
    int min = sched_get_priority_min(policy);
    int max = sched_get_priority_max(policy);
    if (min == -1 || max == -1)
        return EINVAL;
    if (priority < min || priority > max)
        return EINVAL;
    sinomo_wasix_set_thread_sched(thread, sinomo_wasix_get_thread_tid(thread), policy, priority);
    if (pthread_equal(thread, pthread_self())) {
        sinomo_wasix_tls_sched_valid = 1;
        sinomo_wasix_tls_sched_policy = policy;
        sinomo_wasix_tls_sched_priority = priority;
    }
    return 0;
}

int pthread_create(
    pthread_t *restrict thread,
    const pthread_attr_t *restrict attr,
    void *(*start_routine)(void *),
    void *restrict arg
) {
    if (attr != NULL && attr != (const pthread_attr_t *)(uintptr_t)-1 && attr->__u.__s[0] == 0) {
        (void)raise(SIGSEGV);
        return EINVAL;
    }
    pthread_attr_t default_attr;
    const pthread_attr_t *create_attr = attr;
    if (attr == NULL) {
        int attr_result = pthread_attr_init(&default_attr);
        if (attr_result != 0)
            return attr_result;
        attr_result = pthread_attr_setstacksize(&default_attr, SINOMO_WASIX_DEFAULT_THREAD_STACK_SIZE);
        if (attr_result != 0)
            return attr_result;
        create_attr = &default_attr;
    }
    int create_policy = SCHED_OTHER;
    int create_priority = 0;
    int create_has_sched = 0;
    if (attr != NULL && attr != (const pthread_attr_t *)(uintptr_t)-1 && attr->__u.__i[4] == PTHREAD_EXPLICIT_SCHED) {
        create_policy = attr->__u.__i[5];
        create_priority = attr->__u.__i[6];
        create_has_sched = 1;
    } else {
        pthread_t self = pthread_self();
        if (!sinomo_wasix_get_thread_sched(self, &create_policy, &create_priority))
            sinomo_wasix_get_current_sched(&create_policy, &create_priority);
        create_has_sched = 1;
    }
    if (create_has_sched) {
        sinomo_wasix_pending_create_sched_valid = 1;
        sinomo_wasix_pending_create_sched_policy = create_policy;
        sinomo_wasix_pending_create_sched_priority = create_priority;
    }
    if (__sinomo_wasix_shared_memory_is_cross_process() != 0) {
        int32_t sync_result = __sinomo_wasix_shared_memory_sync_from();
        if (sync_result != 0) {
            sinomo_wasix_pending_create_sched_valid = 0;
            return (int)sync_result;
        }
    }
    int before_created = __atomic_load_n(&sinomo_wasix_created_thread_count, __ATOMIC_SEQ_CST);
    if (before_created >= 190) {
        SINOMO_PTHREAD_LOG("sinomo-debug pthread_create enter created=%d attr=%p\n",
            before_created, (void *)attr);
    }
    int result = __pthread_create(thread, create_attr, start_routine, arg);
    sinomo_wasix_pending_create_sched_valid = 0;
    if (result == 0 && thread != NULL) {
        int created = __atomic_add_fetch(&sinomo_wasix_created_thread_count, 1, __ATOMIC_SEQ_CST);
        if ((created % 100) == 0 || created >= 190) {
            SINOMO_PTHREAD_LOG("sinomo-debug pthread_create created=%d thread=%p\n", created, (void *)*thread);
        }
        if (create_attr != NULL && create_attr != (const pthread_attr_t *)(uintptr_t)-1 && create_attr->__u.__i[3] != 0)
            sinomo_wasix_mark_detached_thread(*thread);
        else
            sinomo_wasix_clear_detached_thread(*thread);
        if (create_has_sched) {
            struct sinomo_wasix_pthread_tid_prefix *created_thread = (struct sinomo_wasix_pthread_tid_prefix *)*thread;
            sinomo_wasix_set_thread_sched(*thread, created_thread != NULL ? created_thread->tid : 0, create_policy, create_priority);
        }
    } else if (result != 0) {
        int created = __atomic_load_n(&sinomo_wasix_created_thread_count, __ATOMIC_SEQ_CST);
        SINOMO_PTHREAD_LOG("sinomo-debug pthread_create failed result=%d created=%d attr=%p\n",
            result, created, (void *)attr);
    }
    return result;
}

enum {
    SINOMO_WASIX_DT_EXITED = 0,
    SINOMO_WASIX_DT_EXITING = 1,
    SINOMO_WASIX_DT_JOINABLE = 2,
    SINOMO_WASIX_DT_DETACHED = 3,
};

struct sinomo_wasix_pthread_state {
    uintptr_t self;
    uintptr_t prev;
    uintptr_t next;
    uintptr_t sysinfo;
    uintptr_t canary;
    int32_t tid;
    int32_t errno_val;
    volatile int32_t detach_state;
    volatile int32_t cancel;
    uint8_t canceldisable;
    uint8_t cancelasync;
    uint8_t tsd_used;
    uint8_t dlerror_flag;
    uintptr_t map_base;
    uintptr_t map_size;
    uintptr_t stack;
    uintptr_t stack_size;
    uintptr_t guard_size;
    uintptr_t result;
};

extern _Thread_local struct sinomo_wasix_pthread_state *__wasilibc_pthread_self;

static int sinomo_wasix_pthread_join_impl(pthread_t thread, void **retval) {
    if (thread == NULL)
        return ESRCH;
    struct sinomo_wasix_pthread_state *target = (struct sinomo_wasix_pthread_state *)thread;
    if (target == __wasilibc_pthread_self)
        return EDEADLK;

    int state = __atomic_load_n(&target->detach_state, __ATOMIC_SEQ_CST);
    if (state == SINOMO_WASIX_DT_DETACHED)
        return EINVAL;

    unsigned debug_count = 0;
    while (state != SINOMO_WASIX_DT_EXITED) {
        if (state != SINOMO_WASIX_DT_JOINABLE && state != SINOMO_WASIX_DT_EXITING)
            return ESRCH;
        debug_count++;
        if (debug_count == 1 || (debug_count % 10000u) == 0u) {
            SINOMO_PTHREAD_LOG("sinomo-debug pthread_join waiting thread=%p tid=%d state=%d count=%u\n",
                (void *)thread, target->tid, state, debug_count);
        }
        int wait_ret = __wasilibc_futex_wait_wasix(&target->detach_state, 0, state, 10000000LL);
        if (wait_ret != 0 && wait_ret != -EAGAIN && wait_ret != -EINTR && wait_ret != -ETIMEDOUT && wait_ret != -ECANCELED)
            return -wait_ret;
        pthread_testcancel();
        state = __atomic_load_n(&target->detach_state, __ATOMIC_SEQ_CST);
    }

    if (retval != NULL)
        *retval = (void *)target->result;
    int saved_tid = sinomo_wasix_get_thread_tid(thread);
    int32_t join_ret = saved_tid > 0 ? __imported_wasix_thread_join((uint32_t)saved_tid) : 0;
    if (join_ret != 0)
        return (int)join_ret;
    __atomic_store_n(&target->detach_state, SINOMO_WASIX_DT_DETACHED, __ATOMIC_SEQ_CST);
    sinomo_wasix_clear_detached_thread(thread);
    sinomo_wasix_clear_thread_sched(thread);
    int joined = __atomic_add_fetch(&sinomo_wasix_joined_thread_count, 1, __ATOMIC_SEQ_CST);
    if ((joined % 100) == 0 || joined >= 190) {
        SINOMO_PTHREAD_LOG("sinomo-debug pthread_join joined=%d thread=%p tid=%d map=%p size=%zu\n",
            joined, (void *)thread, target->tid, (void *)target->map_base, (size_t)target->map_size);
    }
    uintptr_t map_base = target->map_base;
    uintptr_t map_size = target->map_size;
    if (map_base != 0 && map_size != 0)
        (void)munmap((void *)map_base, (size_t)map_size);
    return 0;
}

int pthread_join(pthread_t thread, void **retval) {
    return sinomo_wasix_pthread_join_impl(thread, retval);
}

int __pthread_join(pthread_t thread, void **retval) {
    return sinomo_wasix_pthread_join_impl(thread, retval);
}

long __cancel(void) {
    struct sinomo_wasix_pthread_state *self = __wasilibc_pthread_self;
    if (self != NULL && self->canceldisable == PTHREAD_CANCEL_ENABLE)
        pthread_exit(PTHREAD_CANCELED);
    if (self != NULL)
        self->canceldisable = PTHREAD_CANCEL_DISABLE;
    return -ECANCELED;
}

void __testcancel(void) {
    struct sinomo_wasix_pthread_state *self = __wasilibc_pthread_self;
    if (self != NULL &&
        __atomic_load_n(&self->cancel, __ATOMIC_SEQ_CST) &&
        self->canceldisable == PTHREAD_CANCEL_ENABLE) {
        SINOMO_PTHREAD_LOG("sinomo-debug pthread_testcancel exiting thread=%p tid=%d\n",
            (void *)self, self->tid);
        pthread_exit(PTHREAD_CANCELED);
    }
}

void __pthread_testcancel(void) {
    __testcancel();
}

void pthread_testcancel(void) {
    __testcancel();
}

static int64_t sinomo_wasix_timespec_to_ns(const struct timespec *ts) {
    if (ts == NULL || ts->tv_sec < 0 || ts->tv_nsec < 0 || ts->tv_nsec >= 1000000000L)
        return -1;
    if (ts->tv_sec > (INT64_MAX - ts->tv_nsec) / 1000000000LL)
        return INT64_MAX;
    return (int64_t)ts->tv_sec * 1000000000LL + ts->tv_nsec;
}

static int sinomo_wasix_clock_now_ns(clockid_t clock_id, int64_t *now_out) {
    __wasi_clockid_t wasi_clock;
    switch (clock_id) {
    case CLOCK_REALTIME:
        wasi_clock = __WASI_CLOCKID_REALTIME;
        break;
    case CLOCK_MONOTONIC:
        wasi_clock = __WASI_CLOCKID_MONOTONIC;
        break;
    default:
        return EINVAL;
    }
    __wasi_timestamp_t now = 0;
    __wasi_errno_t error = __wasi_clock_time_get(wasi_clock, 1, &now);
    if (error != 0)
        return (int)error;
    *now_out = now > (uint64_t)INT64_MAX ? INT64_MAX : (int64_t)now;
    return 0;
}

static int sinomo_wasix_sleep_relative_ns(int64_t duration_ns, struct timespec *rem) {
    const int64_t chunk_ns = 100000000LL;
    int64_t remaining = duration_ns;
    if (remaining < 0)
        return EINVAL;
    while (remaining > 0) {
        pthread_testcancel();
        uint64_t chunk = remaining > chunk_ns ? (uint64_t)chunk_ns : (uint64_t)remaining;
        int32_t error = __imported_wasix_thread_sleep(chunk);
        pthread_testcancel();
        if (error != 0) {
            if (rem != NULL) {
                rem->tv_sec = remaining / 1000000000LL;
                rem->tv_nsec = remaining % 1000000000LL;
            }
            return error < 0 ? -error : error;
        }
        remaining -= (int64_t)chunk;
    }
    if (rem != NULL) {
        rem->tv_sec = 0;
        rem->tv_nsec = 0;
    }
    return 0;
}

int nanosleep(const struct timespec *req, struct timespec *rem) {
    int64_t duration_ns = sinomo_wasix_timespec_to_ns(req);
    if (duration_ns < 0) {
        errno = EINVAL;
        return -1;
    }
    int error = sinomo_wasix_sleep_relative_ns(duration_ns, rem);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

int clock_nanosleep(clockid_t clock_id, int flags, const struct timespec *req, struct timespec *rem) {
    if ((flags & ~TIMER_ABSTIME) != 0)
        return EINVAL;
    int64_t target_ns = sinomo_wasix_timespec_to_ns(req);
    if (target_ns < 0)
        return EINVAL;
    int64_t duration_ns = target_ns;
    if ((flags & TIMER_ABSTIME) != 0) {
        int64_t now_ns = 0;
        int error = sinomo_wasix_clock_now_ns(clock_id, &now_ns);
        if (error != 0)
            return error;
        duration_ns = target_ns > now_ns ? target_ns - now_ns : 0;
    } else if (clock_id != CLOCK_REALTIME && clock_id != CLOCK_MONOTONIC) {
        return EINVAL;
    }
    return sinomo_wasix_sleep_relative_ns(duration_ns, rem);
}

unsigned sleep(unsigned seconds) {
    struct timespec req = {
        .tv_sec = (time_t)seconds,
        .tv_nsec = 0,
    };
    struct timespec rem = {0};
    if (nanosleep(&req, &rem) != 0)
        return rem.tv_sec > 0 ? (unsigned)rem.tv_sec : 1;
    return 0;
}

int usleep(useconds_t useconds) {
    struct timespec req = {
        .tv_sec = (time_t)(useconds / 1000000u),
        .tv_nsec = (long)(useconds % 1000000u) * 1000L,
    };
    return nanosleep(&req, NULL);
}

int pthread_cancel(pthread_t thread) {
    if (thread == NULL)
        return ESRCH;

    struct sinomo_wasix_pthread_state *target = (struct sinomo_wasix_pthread_state *)thread;
    if (target->tid <= 0)
        return ESRCH;

    __atomic_store_n(&target->cancel, 1, __ATOMIC_SEQ_CST);
    SINOMO_PTHREAD_LOG("sinomo-debug pthread_cancel target=%p tid=%d disable=%u async=%u\n",
        (void *)thread, target->tid, (unsigned)target->canceldisable, (unsigned)target->cancelasync);

    if (target == __wasilibc_pthread_self &&
        target->canceldisable == PTHREAD_CANCEL_ENABLE &&
        target->cancelasync) {
        pthread_exit(PTHREAD_CANCELED);
    }

    int32_t error = __imported_wasix_thread_signal((uint32_t)target->tid, (uint32_t)SIGCANCEL);
    return error < 0 ? -error : error;
}

static uint64_t signal_bit(int sig, uint64_t *high) {
    if (high != NULL)
        *high = 0;
    if (sig <= 0 || sig > 128)
        return 0;
    if (sig <= 64)
        return UINT64_C(1) << (sig - 1);
    if (high != NULL)
        *high = UINT64_C(1) << (sig - 65);
    return 0;
}

static void sigset_to_words(const sigset_t *set, uint64_t *low, uint64_t *high) {
    *low = 0;
    *high = 0;
    if (set == NULL)
        return;
    memcpy(low, set, sizeof(*low) <= sizeof(*set) ? sizeof(*low) : sizeof(*set));
    if (sizeof(*set) > sizeof(*low)) {
        size_t high_bytes = sizeof(*set) - sizeof(*low);
        if (high_bytes > sizeof(*high))
            high_bytes = sizeof(*high);
        memcpy(high, ((const unsigned char *)set) + sizeof(*low), high_bytes);
    }
}

static void clear_signal_from_words(uint64_t *low, uint64_t *high, int sig) {
    uint64_t bit_high = 0;
    uint64_t bit_low = signal_bit(sig, &bit_high);
    *low &= ~bit_low;
    *high &= ~bit_high;
}

static void clear_unmaskable_signals(uint64_t *low, uint64_t *high) {
    clear_signal_from_words(low, high, SIGKILL);
    clear_signal_from_words(low, high, SIGSTOP);
}

static int deliver_pending_unblocked(uint64_t new_low, uint64_t new_high) {
    uint64_t deliver_low = sinomo_wasix_pending_signal_low & ~new_low;
    uint64_t deliver_high = sinomo_wasix_pending_signal_high & ~new_high;
    if (deliver_low == 0 && deliver_high == 0)
        return 0;

    sinomo_wasix_pending_signal_low &= ~deliver_low;
    sinomo_wasix_pending_signal_high &= ~deliver_high;

    __wasi_tid_t tid = 0;
    int32_t error = __wasi_thread_id(&tid);
    if (error != 0)
        return error < 0 ? -error : error;

    while (deliver_low != 0) {
        int sig = __builtin_ctzll(deliver_low) + 1;
        deliver_low &= deliver_low - 1;
        error = __imported_wasix_thread_signal((uint32_t)tid, (uint32_t)sig);
        if (error != 0)
            return error < 0 ? -error : error;
    }
    while (deliver_high != 0) {
        int sig = __builtin_ctzll(deliver_high) + 65;
        deliver_high &= deliver_high - 1;
        error = __imported_wasix_thread_signal((uint32_t)tid, (uint32_t)sig);
        if (error != 0)
            return error < 0 ? -error : error;
    }
    return 0;
}

static void words_to_sigset(sigset_t *set, uint64_t low, uint64_t high) {
    if (set == NULL)
        return;
    memset(set, 0, sizeof(*set));
    memcpy(set, &low, sizeof(low) <= sizeof(*set) ? sizeof(low) : sizeof(*set));
    if (sizeof(*set) > sizeof(low)) {
        size_t high_bytes = sizeof(*set) - sizeof(low);
        if (high_bytes > sizeof(high))
            high_bytes = sizeof(high);
        memcpy(((unsigned char *)set) + sizeof(low), &high, high_bytes);
    }
}

int pthread_sigmask(int how, const sigset_t *restrict set, sigset_t *restrict oldset) {
    uint64_t old_low = 0;
    uint64_t old_high = 0;
    int32_t error = __imported_wasix_signal_mask_get(&old_low, &old_high);
    if (error != 0)
        return error < 0 ? -error : error;
    words_to_sigset(oldset, old_low, old_high);
    if (set == NULL)
        return 0;

    uint64_t set_low = 0;
    uint64_t set_high = 0;
    sigset_to_words(set, &set_low, &set_high);
    clear_unmaskable_signals(&set_low, &set_high);
    uint64_t new_low = old_low;
    uint64_t new_high = old_high;
    switch (how) {
    case SIG_BLOCK:
        new_low |= set_low;
        new_high |= set_high;
        break;
    case SIG_UNBLOCK:
        new_low &= ~set_low;
        new_high &= ~set_high;
        break;
    case SIG_SETMASK:
        new_low = set_low;
        new_high = set_high;
        break;
    default:
        return EINVAL;
    }
    clear_unmaskable_signals(&new_low, &new_high);
    error = __imported_wasix_signal_mask_set(new_low, new_high);
    if (error != 0)
        return error < 0 ? -error : error;
    return deliver_pending_unblocked(new_low, new_high);
}

int sigprocmask(int how, const sigset_t *restrict set, sigset_t *restrict oldset) {
    return pthread_sigmask(how, set, oldset);
}

int sigpending(sigset_t *set) {
    if (set == NULL) {
        errno = EFAULT;
        return -1;
    }
    words_to_sigset(set, sinomo_wasix_pending_signal_low, sinomo_wasix_pending_signal_high);
    return 0;
}

int raise(int sig) {
    uint64_t high_bit = 0;
    uint64_t low_bit = signal_bit(sig, &high_bit);
    if (low_bit == 0 && high_bit == 0) {
        errno = EINVAL;
        return -1;
    }

    uint64_t mask_low = 0;
    uint64_t mask_high = 0;
    int32_t error = __imported_wasix_signal_mask_get(&mask_low, &mask_high);
    if (error != 0) {
        errno = error < 0 ? -error : error;
        return -1;
    }
    if ((mask_low & low_bit) != 0 || (mask_high & high_bit) != 0) {
        sinomo_wasix_pending_signal_low |= low_bit;
        sinomo_wasix_pending_signal_high |= high_bit;
        return 0;
    }

    __wasi_tid_t tid = 0;
    error = __wasi_thread_id(&tid);
    if (error != 0) {
        errno = error < 0 ? -error : error;
        return -1;
    }
    error = __imported_wasix_thread_signal((uint32_t)tid, (uint32_t)sig);
    if (error != 0) {
        errno = error < 0 ? -error : error;
        return -1;
    }
    return 0;
}

int chroot(const char *path) {
    (void)path;
    errno = ENOSYS;
    return -1;
}

int open(const char *path, int oflag, ...) {
    mode_t mode = 0666;
    if ((oflag & O_CREAT) != 0) {
        va_list ap;
        va_start(ap, oflag);
        mode = va_arg(ap, mode_t);
        va_end(ap);
    }

    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }

    struct stat before;
    int created = ((oflag & O_CREAT) != 0) &&
        (__wasilibc_nocwd_fstatat(dirfd, relative_path, &before, 0) != 0 && errno == ENOENT);

    int fd = __wasilibc_nocwd_openat_nomode(dirfd, relative_path, oflag);
    if (fd < 0)
        return fd;
    if ((oflag & O_CREAT) != 0) {
        if (fchmod(fd, mode & ~sinomo_wasix_umask) != 0 ||
            fchown(fd, __sinomo_wasix_geteuid(), __sinomo_wasix_getegid()) != 0) {
            close(fd);
            return -1;
        }
    }
    if (created)
        touchat_guest(dirfd, relative_path, 1, 1);
    else if ((oflag & O_TRUNC) != 0)
        touchat_guest(dirfd, relative_path, 1, 0);
    return fd;
}

int __wasilibc_open_nomode(const char *path, int oflag) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_openat_nomode(dirfd, relative_path, oflag);
}

int access(const char *path, int amode) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_faccessat(dirfd, relative_path, amode, 0);
}

ssize_t readlink(const char *restrict path, char *restrict buf, size_t bufsize) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __sinomo_wasix_readlinkat(dirfd, relative_path, buf, bufsize);
}

int stat(const char *restrict path, struct stat *restrict buf) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_fstatat(dirfd, relative_path, buf, 0);
}

int lstat(const char *restrict path, struct stat *restrict buf) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_fstatat(dirfd, relative_path, buf, AT_SYMLINK_NOFOLLOW);
}

int utime(const char *path, const struct utimbuf *times) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_utimensat(dirfd, relative_path,
                                      times ? ((struct timespec[2]){{.tv_sec = times->actime},
                                                                    {.tv_sec = times->modtime}})
                                            : NULL,
                                      0);
}

int utimes(const char *path, const struct timeval times[2]) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_utimensat(
        dirfd, relative_path,
        times ? ((struct timespec[2]){{.tv_sec = times[0].tv_sec, .tv_nsec = times[0].tv_usec * 1000},
                                      {.tv_sec = times[1].tv_sec, .tv_nsec = times[1].tv_usec * 1000}})
              : NULL,
        0);
}

int settimeofday(const struct timeval *tv, const struct timezone *tz) {
    (void)tv;
    (void)tz;
    errno = EPERM;
    return -1;
}

int unlink(const char *path) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    if (__sinomo_wasix_unlink_symlinkat(dirfd, relative_path) == 0)
        return 0;
    if (errno != ENOENT)
        return -1;
    return __wasilibc_nocwd___wasilibc_unlinkat(dirfd, relative_path);
}

int rmdir(const char *path) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd___wasilibc_rmdirat(dirfd, relative_path);
}

int remove(const char *path) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    int r = __wasilibc_nocwd___wasilibc_unlinkat(dirfd, relative_path);
    if (r != 0 && (errno == EISDIR || errno == ENOENT)) {
        r = __wasilibc_nocwd___wasilibc_rmdirat(dirfd, relative_path);
        if (errno == ENOTDIR)
            errno = ENOENT;
    }
    return r;
}

int mkdir(const char *path, mode_t mode) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    int ret = __wasilibc_nocwd_mkdirat_nomode(dirfd, relative_path);
    if (ret != 0)
        return ret;
    if (chmodat_guest(dirfd, relative_path, mode & ~sinomo_wasix_umask) != 0)
        return -1;
    if (chownat_guest(dirfd, relative_path, __sinomo_wasix_geteuid(), __sinomo_wasix_getegid()) != 0)
        return -1;
    touchat_guest(dirfd, relative_path, 1, 1);
    return 0;
}

int mknod(const char *path, mode_t mode, dev_t dev) {
    (void)dev;
    if (S_ISREG(mode) || (mode & S_IFMT) == 0) {
        int fd = open(path, O_CREAT | O_EXCL | O_WRONLY, mode & 07777);
        if (fd < 0)
            return -1;
        return close(fd);
    }
    errno = ENOSYS;
    return -1;
}

mode_t umask(mode_t mode) {
    mode_t previous = sinomo_wasix_umask;
    sinomo_wasix_umask = mode & 0777;
    return previous;
}

int chmod(const char *path, mode_t mode) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    int ret = chmodat_guest(dirfd, relative_path, mode);
    if (ret == 0)
        touchat_guest(dirfd, relative_path, 1, 0);
    return ret;
}

int fchmod(int fd, mode_t mode) {
    int32_t error = __imported_wasix_fd_chmod(fd, mode & 07777);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

int fchmodat(int fd, const char *path, mode_t mode, int flag) {
    (void)flag;
    int ret = chmodat_guest(fd, path, mode);
    if (ret == 0)
        touchat_guest(fd, path, 1, 0);
    return ret;
}

int chown(const char *path, uid_t uid, gid_t gid) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    int ret = chownat_guest(dirfd, relative_path, uid, gid);
    if (ret == 0)
        touchat_guest(dirfd, relative_path, 1, 0);
    return ret;
}

int lchown(const char *path, uid_t uid, gid_t gid) {
    return chown(path, uid, gid);
}

int fchown(int fd, uid_t uid, gid_t gid) {
    int32_t error = __imported_wasix_fd_chown(fd, uid, gid);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

int fchownat(int fd, const char *path, uid_t uid, gid_t gid, int flag) {
    (void)flag;
    int ret = chownat_guest(fd, path, uid, gid);
    if (ret == 0)
        touchat_guest(fd, path, 1, 0);
    return ret;
}

int setgroups(size_t size, const gid_t list[]) {
    if (size > UINT32_MAX) {
        errno = EINVAL;
        return -1;
    }
    int32_t error = __imported_wasix_proc_setgroups((const uint32_t *)list, (uint32_t)size);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

int getgroups(int size, gid_t list[]) {
    if (size < 0) {
        errno = EINVAL;
        return -1;
    }
    uint32_t count = 0;
    int32_t error = __imported_wasix_proc_getgroups((uint32_t *)list, (uint32_t)size, &count);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return (int)count;
}

uid_t getuid(void) {
    return sinomo_wasix_ruid;
}

uid_t geteuid(void) {
    return sinomo_wasix_euid;
}

gid_t getgid(void) {
    return sinomo_wasix_rgid;
}

gid_t getegid(void) {
    return sinomo_wasix_egid;
}

int setuid(uid_t uid) {
    int32_t error = __imported_wasix_proc_setresuid(uid, uid, uid);
    if (error != 0) {
        errno = error;
        return -1;
    }
    sinomo_wasix_ruid = uid;
    sinomo_wasix_euid = uid;
    sinomo_wasix_suid = uid;
    return 0;
}

int seteuid(uid_t uid) {
    int32_t error = __imported_wasix_proc_setresuid(UINT32_MAX, uid, UINT32_MAX);
    if (error != 0) {
        errno = error;
        return -1;
    }
    sinomo_wasix_euid = uid;
    return 0;
}

int setgid(gid_t gid) {
    int32_t error = __imported_wasix_proc_setresgid(gid, gid, gid);
    if (error != 0) {
        errno = error;
        return -1;
    }
    sinomo_wasix_rgid = gid;
    sinomo_wasix_egid = gid;
    sinomo_wasix_sgid = gid;
    return 0;
}

int setegid(gid_t gid) {
    int32_t error = __imported_wasix_proc_setresgid(UINT32_MAX, gid, UINT32_MAX);
    if (error != 0) {
        errno = error;
        return -1;
    }
    sinomo_wasix_egid = gid;
    return 0;
}

int ftruncate(int fildes, off_t length) {
    if (length < 0) {
        errno = EINVAL;
        return -1;
    }
    __wasi_filesize_t st_size = length;
    __wasi_errno_t error = __wasi_fd_filestat_set_size(fildes, st_size);
    if (error != 0) {
        errno = (error == EACCES || error == __WASI_ERRNO_ACCES) ? EINVAL : error;
        return -1;
    }
    (void)__imported_wasix_fd_touch(fildes);
    return 0;
}

int truncate(const char *path, off_t length) {
    if (length < 0) {
        errno = EINVAL;
        return -1;
    }
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    int fd = __wasilibc_nocwd_openat_nomode(dirfd, relative_path, O_WRONLY);
    if (fd < 0)
        return -1;
    int ret = ftruncate(fd, length);
    int saved_errno = errno;
    close(fd);
    errno = saved_errno;
    return ret;
}

DIR *opendir(const char *dirname) {
    char *relative_path;
    int dirfd = find_relpath(dirname, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return NULL;
    }
    return __wasilibc_nocwd_opendirat(dirfd, relative_path);
}

int scandir(const char *restrict dir, struct dirent ***restrict namelist,
            int (*filter)(const struct dirent *),
            int (*compar)(const struct dirent **, const struct dirent **)) {
    char *relative_path;
    int dirfd = find_relpath(dir, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_scandirat(dirfd, relative_path, namelist, filter, compar);
}

int symlink(const char *target, const char *linkpath) {
    char *relative_path;
    int dirfd = find_relpath(linkpath, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __sinomo_wasix_symlinkat(target, dirfd, relative_path);
}

int link(const char *old, const char *new) {
    char *old_relative_path;
    int old_dirfd = find_relpath_alt(old, &old_relative_path);
    if (old_dirfd != -1) {
        char *new_relative_path;
        int new_dirfd = find_relpath(new, &new_relative_path);
        if (new_dirfd != -1)
            return __wasilibc_nocwd_linkat(old_dirfd, old_relative_path, new_dirfd, new_relative_path, 0);
    }
    errno = ENOENT;
    return -1;
}

int rename(const char *old, const char *new) {
    char *old_relative_path;
    int old_dirfd = find_relpath_alt(old, &old_relative_path);
    if (old_dirfd != -1) {
        char *new_relative_path;
        int new_dirfd = find_relpath(new, &new_relative_path);
        if (new_dirfd != -1)
            return __wasilibc_nocwd_renameat(old_dirfd, old_relative_path, new_dirfd, new_relative_path);
    }
    errno = ENOENT;
    return -1;
}

int __wasilibc_access(const char *path, int mode, int flags) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_faccessat(dirfd, relative_path, mode, flags);
}

int __wasilibc_utimens(const char *path, const struct timespec times[2], int flags) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_utimensat(dirfd, relative_path, times, flags);
}

int __wasilibc_stat(const char *restrict path, struct stat *restrict st, int flags) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_fstatat(dirfd, relative_path, st, flags);
}

int __wasilibc_link(const char *oldpath, const char *newpath, int flags) {
    char *old_relative_path;
    char *new_relative_path;
    int old_dirfd = find_relpath(oldpath, &old_relative_path);
    int new_dirfd = find_relpath(newpath, &new_relative_path);
    if (old_dirfd == -1 || new_dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_linkat(old_dirfd, old_relative_path, new_dirfd, new_relative_path, flags);
}

int __wasilibc_link_oldat(int olddirfd, const char *oldpath, const char *newpath, int flags) {
    char *new_relative_path;
    int new_dirfd = find_relpath(newpath, &new_relative_path);
    if (new_dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_linkat(olddirfd, oldpath, new_dirfd, new_relative_path, flags);
}

int __wasilibc_link_newat(const char *oldpath, int newdirfd, const char *newpath, int flags) {
    char *old_relative_path;
    int old_dirfd = find_relpath(oldpath, &old_relative_path);
    if (old_dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_linkat(old_dirfd, old_relative_path, newdirfd, newpath, flags);
}

int __wasilibc_rename_oldat(int fromdirfd, const char *from, const char *to) {
    char *to_relative_path;
    int to_dirfd = find_relpath(to, &to_relative_path);
    if (to_dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_renameat(fromdirfd, from, to_dirfd, to_relative_path);
}

int __wasilibc_rename_newat(const char *from, int todirfd, const char *to) {
    char *from_relative_path;
    int from_dirfd = find_relpath(from, &from_relative_path);
    if (from_dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_renameat(from_dirfd, from_relative_path, todirfd, to);
}
