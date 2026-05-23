#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <wasi/api.h>

#define SINOMO_SEM_VALUE_WORD 0
#define SINOMO_SEM_PSHARED_WORD 1
#define SINOMO_SEM_PSHARED 1
#define SINOMO_SEM_NAMED 2
#define SINOMO_NAMED_SEM_MAX 256
#define SINOMO_NAMED_SEM_NAME_MAX 255

static volatile int sinomo_sem_open_lock[1];
volatile int *const __sem_open_lockptr = sinomo_sem_open_lock;

int32_t __sinomo_wasix_shared_memory_range_sync_from(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_range_sync_from")));
int32_t __sinomo_wasix_shared_memory_range_sync_to(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_range_sync_to")));
int32_t __sinomo_wasix_shared_futex_hint(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_futex_hint")));
uint32_t __sinomo_wasix_shared_memory_is_cross_process(void)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_is_cross_process")));
uint32_t __sinomo_wasix_shared_memory_has_direct_mapping(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_has_direct_mapping")));
int32_t __sinomo_wasix_shared_semaphore_getvalue(uintptr_t offset)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_semaphore_getvalue")));
int32_t __sinomo_wasix_shared_semaphore_post(uintptr_t offset, uint32_t max_value)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_semaphore_post")));
int32_t __sinomo_wasix_shared_semaphore_try_wait(uintptr_t offset)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_semaphore_try_wait")));
int32_t __sinomo_wasix_shared_semaphore_wait(uintptr_t offset, int64_t timeout_ns)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_semaphore_wait")));
int32_t __sinomo_wasix_shared_semaphore_destroy(uintptr_t offset)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_semaphore_destroy")));

int __wasilibc_futex_wait_wasix(volatile void *addr, int op, int expected, int64_t max_wait_ns);
int __wasilibc_futex_wake_wasix(int *futex, int cnt);
void pthread_testcancel(void);

static int64_t realtime_abstime_to_wait_ns(const struct timespec *abstime) {
    if (abstime == 0)
        return -1;
    if (abstime->tv_nsec < 0 || abstime->tv_nsec >= 1000000000L)
        return INT64_MIN;

    struct timespec now;
    if (clock_gettime(CLOCK_REALTIME, &now) != 0)
        return INT64_MIN + 1;

    int64_t sec = (int64_t)abstime->tv_sec - (int64_t)now.tv_sec;
    int64_t nsec = (int64_t)abstime->tv_nsec - (int64_t)now.tv_nsec;
    int64_t total = sec * 1000000000LL + nsec;
    return total <= 0 ? 0 : total;
}

struct sinomo_named_sem_slot {
    int used;
    int unlinked;
    unsigned refs;
    mode_t mode;
    uid_t owner;
    char name[SINOMO_NAMED_SEM_NAME_MAX + 1];
    sem_t sem;
};

static struct sinomo_named_sem_slot sinomo_named_sems[SINOMO_NAMED_SEM_MAX];

static int sem_is_pshared(const sem_t *sem) {
    return (sem->__val[SINOMO_SEM_PSHARED_WORD] & SINOMO_SEM_PSHARED) != 0;
}

static int sem_is_named(const sem_t *sem) {
    return (sem->__val[SINOMO_SEM_PSHARED_WORD] & SINOMO_SEM_NAMED) != 0;
}

static int sem_has_direct_mapping(const sem_t *sem) {
    return __sinomo_wasix_shared_memory_has_direct_mapping((uintptr_t)sem, (uintptr_t)sizeof(*sem)) != 0;
}

static int sem_uses_runtime_shared(const sem_t *sem) {
    return sem_is_named(sem) || (sem_is_pshared(sem) && !sem_has_direct_mapping(sem));
}

static int validate_named_sem_name(const char *name, size_t *len_out) {
    if (name == 0 || name[0] != '/' || name[1] == '\0') {
        errno = EINVAL;
        return -1;
    }
    size_t len = strlen(name);
    if (len > SINOMO_NAMED_SEM_NAME_MAX) {
        errno = ENAMETOOLONG;
        return -1;
    }
    for (size_t index = 1; index < len; index++) {
        if (name[index] == '/') {
            errno = EINVAL;
            return -1;
        }
    }
    if (len_out != 0)
        *len_out = len;
    return 0;
}

static struct sinomo_named_sem_slot *find_named_sem(const char *name) {
    for (size_t index = 0; index < SINOMO_NAMED_SEM_MAX; index++) {
        struct sinomo_named_sem_slot *slot = &sinomo_named_sems[index];
        if (slot->used && !slot->unlinked && strcmp(slot->name, name) == 0)
            return slot;
    }
    return 0;
}

static struct sinomo_named_sem_slot *find_named_sem_by_ptr(sem_t *sem) {
    for (size_t index = 0; index < SINOMO_NAMED_SEM_MAX; index++) {
        struct sinomo_named_sem_slot *slot = &sinomo_named_sems[index];
        if (slot->used && &slot->sem == sem)
            return slot;
    }
    return 0;
}

static struct sinomo_named_sem_slot *alloc_named_sem_slot(void) {
    for (size_t index = 0; index < SINOMO_NAMED_SEM_MAX; index++) {
        struct sinomo_named_sem_slot *slot = &sinomo_named_sems[index];
        if (!slot->used)
            return slot;
    }
    errno = ENFILE;
    return 0;
}

static int sync_from_shared_if_needed(sem_t *sem) {
    if (!sem_is_pshared(sem) || __sinomo_wasix_shared_memory_is_cross_process() == 0)
        return 0;
    if (sem_has_direct_mapping(sem))
        return 0;
    int32_t err = __sinomo_wasix_shared_memory_range_sync_to((uintptr_t)sem, (uintptr_t)sizeof(*sem));
    return err == 0 ? 0 : (int)err;
}

static int sync_to_shared_if_needed(sem_t *sem) {
    if (!sem_is_pshared(sem) || __sinomo_wasix_shared_memory_is_cross_process() == 0)
        return 0;
    if (sem_has_direct_mapping(sem))
        return 0;
    int32_t err = __sinomo_wasix_shared_memory_range_sync_from((uintptr_t)sem, (uintptr_t)sizeof(*sem));
    return err == 0 ? 0 : (int)err;
}

int sem_init(sem_t *sem, int pshared, unsigned value) {
    if (sem == 0) {
        errno = EINVAL;
        return -1;
    }
    if (value > SEM_VALUE_MAX) {
        errno = EINVAL;
        return -1;
    }
    sem->__val[SINOMO_SEM_VALUE_WORD] = (int)value;
    sem->__val[SINOMO_SEM_PSHARED_WORD] = pshared ? SINOMO_SEM_PSHARED : 0;
    sem->__val[2] = 0;
    sem->__val[3] = 0;
    if (pshared) {
        int32_t hint = __sinomo_wasix_shared_futex_hint((uintptr_t)sem, (uintptr_t)sizeof(*sem));
        if (hint != 0) {
            errno = (int)hint;
            return -1;
        }
        int ret = sync_to_shared_if_needed(sem);
        if (ret != 0) {
            errno = ret;
            return -1;
        }
    }
    return 0;
}

int sem_destroy(sem_t *sem) {
    if (sem == 0) {
        errno = EINVAL;
        return -1;
    }
    if (sem_uses_runtime_shared(sem)) {
        int32_t ret = __sinomo_wasix_shared_semaphore_destroy((uintptr_t)sem);
        if (ret != 0) {
            errno = ret;
            return -1;
        }
    }
    return 0;
}

sem_t *sem_open(const char *name, int oflag, ...) {
    size_t len = 0;
    if (validate_named_sem_name(name, &len) != 0)
        return SEM_FAILED;

    mode_t mode = 0600;
    unsigned value = 0;
    if ((oflag & O_CREAT) != 0) {
        va_list args;
        va_start(args, oflag);
        mode = (mode_t)va_arg(args, int);
        value = va_arg(args, unsigned);
        va_end(args);
        if (value > SEM_VALUE_MAX) {
            errno = EINVAL;
            return SEM_FAILED;
        }
    }

    struct sinomo_named_sem_slot *slot = find_named_sem(name);
    if (slot != 0) {
        if ((oflag & O_CREAT) != 0 && (oflag & O_EXCL) != 0) {
            errno = EEXIST;
            return SEM_FAILED;
        }
        if ((oflag & O_CREAT) != 0) {
            if ((mode & 0222) != 0 && (slot->mode & 0222) == 0) {
                errno = EACCES;
                return SEM_FAILED;
            }
            if ((mode & 0444) != 0 && (slot->mode & 0444) == 0) {
                errno = EACCES;
                return SEM_FAILED;
            }
        }
        slot->refs++;
        return &slot->sem;
    }

    if ((oflag & O_CREAT) == 0) {
        errno = ENOENT;
        return SEM_FAILED;
    }

    slot = alloc_named_sem_slot();
    if (slot == 0)
        return SEM_FAILED;
    memset(slot, 0, sizeof(*slot));
    if (len >= sizeof(slot->name)) {
        errno = ENAMETOOLONG;
        return SEM_FAILED;
    }
    memcpy(slot->name, name, len + 1);
    slot->used = 1;
    slot->refs = 1;
    slot->mode = mode;
    slot->owner = geteuid();
    if (sem_init(&slot->sem, 1, value) != 0) {
        memset(slot, 0, sizeof(*slot));
        return SEM_FAILED;
    }
    slot->sem.__val[SINOMO_SEM_PSHARED_WORD] |= SINOMO_SEM_NAMED;
    return &slot->sem;
}

int sem_close(sem_t *sem) {
    if (sem == 0) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_named_sem_slot *slot = find_named_sem_by_ptr(sem);
    if (slot == 0) {
        errno = EINVAL;
        return -1;
    }
    if (slot->refs > 0)
        slot->refs--;
    if (slot->refs == 0 && slot->unlinked) {
        /*
         * POSIX keeps an unlinked named semaphore alive until every process
         * holding it has closed, exited, or execed. The sysroot-local refcount
         * only describes this process image; after fork, other processes may
         * still be blocked on the runtime semaphore at the same guest address.
         * Do not remove the runtime object here or those waiters can be
         * stranded. The slot remains reserved in this process image until the
         * sandbox process exits.
         */
    }
    return 0;
}

int sem_unlink(const char *name) {
    if (validate_named_sem_name(name, 0) != 0) {
        if (errno == EINVAL)
            errno = ENOENT;
        return -1;
    }
    struct sinomo_named_sem_slot *slot = find_named_sem(name);
    if (slot == 0) {
        errno = ENOENT;
        return -1;
    }
    uid_t euid = geteuid();
    if (euid != 0 && euid != slot->owner && (slot->mode & 0022) == 0) {
        errno = EACCES;
        return -1;
    }
    slot->unlinked = 1;
    if (slot->refs == 0) {
        (void)sem_destroy(&slot->sem);
        memset(slot, 0, sizeof(*slot));
    }
    return 0;
}

int sem_getvalue(sem_t *restrict sem, int *restrict value) {
    if (sem == 0 || value == 0) {
        errno = EINVAL;
        return -1;
    }
    if (sem_uses_runtime_shared(sem)) {
        int32_t ret = __sinomo_wasix_shared_semaphore_getvalue((uintptr_t)sem);
        if (ret < 0) {
            errno = -ret;
            return -1;
        }
        *value = ret;
        return 0;
    }
    int ret = sync_from_shared_if_needed(sem);
    if (ret != 0) {
        errno = ret;
        return -1;
    }
    *value = __atomic_load_n(&sem->__val[SINOMO_SEM_VALUE_WORD], __ATOMIC_SEQ_CST);
    return 0;
}

int sem_post(sem_t *sem) {
    if (sem == 0) {
        errno = EINVAL;
        return -1;
    }
    if (sem_uses_runtime_shared(sem)) {
        int32_t ret = __sinomo_wasix_shared_semaphore_post((uintptr_t)sem, (uint32_t)SEM_VALUE_MAX);
        if (ret != 0) {
            errno = ret;
            return -1;
        }
        (void)__wasilibc_futex_wake_wasix((int *)&sem->__val[SINOMO_SEM_VALUE_WORD], INT_MAX);
        return 0;
    }
    int ret = sync_from_shared_if_needed(sem);
    if (ret != 0) {
        errno = ret;
        return -1;
    }
    int old = __atomic_load_n(&sem->__val[SINOMO_SEM_VALUE_WORD], __ATOMIC_SEQ_CST);
    for (;;) {
        if (old == SEM_VALUE_MAX) {
            errno = EOVERFLOW;
            return -1;
        }
        if (__atomic_compare_exchange_n(&sem->__val[SINOMO_SEM_VALUE_WORD], &old, old + 1, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
            break;
    }
    ret = sync_to_shared_if_needed(sem);
    if (ret != 0) {
        errno = ret;
        return -1;
    }
    (void)__wasilibc_futex_wake_wasix((int *)&sem->__val[SINOMO_SEM_VALUE_WORD], INT_MAX);
    return 0;
}

int sem_trywait(sem_t *sem) {
    if (sem == 0) {
        errno = EINVAL;
        return -1;
    }
    if (sem_uses_runtime_shared(sem)) {
        int32_t ret = __sinomo_wasix_shared_semaphore_try_wait((uintptr_t)sem);
        if (ret != 0) {
            errno = ret;
            return -1;
        }
        return 0;
    }
    int ret = sync_from_shared_if_needed(sem);
    if (ret != 0) {
        errno = ret;
        return -1;
    }
    int old = __atomic_load_n(&sem->__val[SINOMO_SEM_VALUE_WORD], __ATOMIC_SEQ_CST);
    while (old > 0) {
        if (__atomic_compare_exchange_n(&sem->__val[SINOMO_SEM_VALUE_WORD], &old, old - 1, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
            ret = sync_to_shared_if_needed(sem);
            if (ret != 0) {
                errno = ret;
                return -1;
            }
            return 0;
        }
    }
    errno = EAGAIN;
    return -1;
}

int sem_wait(sem_t *sem) {
    if (sem == 0) {
        errno = EINVAL;
        return -1;
    }
    if (sem_uses_runtime_shared(sem)) {
        for (;;) {
            int32_t try_ret = __sinomo_wasix_shared_semaphore_try_wait((uintptr_t)sem);
            if (try_ret == 0)
                return 0;
            if (try_ret != EAGAIN) {
                errno = try_ret;
                return -1;
            }
            int32_t wait_ret = __sinomo_wasix_shared_semaphore_wait((uintptr_t)sem, 10000000LL);
            if (wait_ret != 0) {
                errno = wait_ret;
                return -1;
            }
            pthread_testcancel();
        }
    }
    for (;;) {
        if (sem_trywait(sem) == 0)
            return 0;
        if (errno != EAGAIN)
            return -1;

        int observed = __atomic_load_n(&sem->__val[SINOMO_SEM_VALUE_WORD], __ATOMIC_SEQ_CST);
        int wait_ret = __wasilibc_futex_wait_wasix(&sem->__val[SINOMO_SEM_VALUE_WORD], 0, observed, 10000000LL);
        if (wait_ret == -EINTR) {
            errno = EINTR;
            return -1;
        }
        if (wait_ret != 0 && wait_ret != -EAGAIN && wait_ret != -EINTR && wait_ret != -ETIMEDOUT && wait_ret != -ECANCELED) {
            errno = -wait_ret;
            return -1;
        }
        pthread_testcancel();
    }
}

int sem_timedwait(sem_t *restrict sem, const struct timespec *restrict abstime) {
    if (sem == 0) {
        errno = EINVAL;
        return -1;
    }

    for (;;) {
        if (sem_trywait(sem) == 0)
            return 0;
        if (errno != EAGAIN)
            return -1;

        int64_t wait_ns = realtime_abstime_to_wait_ns(abstime);
        if (wait_ns == INT64_MIN) {
            errno = EINVAL;
            return -1;
        }
        if (wait_ns == INT64_MIN + 1)
            return -1;
        if (wait_ns == 0) {
            errno = ETIMEDOUT;
            return -1;
        }

        if (sem_uses_runtime_shared(sem)) {
            int32_t wait_ret = __sinomo_wasix_shared_semaphore_wait((uintptr_t)sem, wait_ns);
            if (wait_ret == ETIMEDOUT) {
                errno = ETIMEDOUT;
                return -1;
            }
            if (wait_ret == EINTR) {
                errno = EINTR;
                return -1;
            }
            if (wait_ret != 0 && wait_ret != EINTR && wait_ret != ECANCELED) {
                errno = wait_ret;
                return -1;
            }
        } else {
            int observed = __atomic_load_n(&sem->__val[SINOMO_SEM_VALUE_WORD], __ATOMIC_SEQ_CST);
            int wait_ret = __wasilibc_futex_wait_wasix(&sem->__val[SINOMO_SEM_VALUE_WORD], 0, observed, wait_ns);
            if (wait_ret == -ETIMEDOUT) {
                errno = ETIMEDOUT;
                return -1;
            }
            if (wait_ret == -EINTR) {
                errno = EINTR;
                return -1;
            }
            if (wait_ret != 0 && wait_ret != -EAGAIN && wait_ret != -EINTR && wait_ret != -ECANCELED) {
                errno = -wait_ret;
                return -1;
            }
        }
        pthread_testcancel();
    }
}
