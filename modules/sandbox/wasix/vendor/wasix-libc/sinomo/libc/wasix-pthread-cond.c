#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <wasi/api.h>

#define SINOMO_COND_PSHARED_ATTR 0x80000000u
#define SINOMO_COND_CLOCK_MASK 0x7fffffffu
#define SINOMO_COND_SHARED (-1)
#define SINOMO_COND_SHARED_WORD 0
#define SINOMO_COND_SEQ_WORD 2
#define SINOMO_COND_WAITERS_WORD 3
#define SINOMO_COND_CLOCK_WORD 4
#define SINOMO_COND_RUNTIME_WORDS 5
#define SINOMO_COND_RUNTIME_BYTES (SINOMO_COND_RUNTIME_WORDS * sizeof(int))
#ifndef SINOMO_PTHREAD_DEBUG
#define SINOMO_PTHREAD_DEBUG 0
#endif
#define SINOMO_PTHREAD_LOG(...) do { if (SINOMO_PTHREAD_DEBUG || getenv("SINOMO_PTHREAD_LOG") != 0) fprintf(stderr, __VA_ARGS__); } while (0)

int32_t __sinomo_wasix_shared_memory_sync_from(void)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_sync_from")));
int32_t __sinomo_wasix_shared_memory_sync_to(void)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_sync_to")));
int32_t __sinomo_wasix_shared_memory_sync_from_preserving_futexes(void)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_sync_from_preserving_futexes")));
int32_t __sinomo_wasix_shared_memory_range_sync_from(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_range_sync_from")));
int32_t __sinomo_wasix_shared_memory_range_sync_to(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_range_sync_to")));
int32_t __sinomo_wasix_shared_futex_hint(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_futex_hint")));
int32_t __sinomo_wasix_shared_futex_unhint(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_futex_unhint")));
uint32_t __sinomo_wasix_shared_cond_sequence(uintptr_t offset)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_cond_sequence")));
uint32_t __sinomo_wasix_shared_cond_broadcast(uintptr_t offset)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_cond_broadcast")));
int32_t __sinomo_wasix_shared_cond_wait(uintptr_t offset, uint32_t expected_seq, int64_t timeout_ns)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_cond_wait")));
uint32_t __sinomo_wasix_shared_memory_is_cross_process(void)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_is_cross_process")));
uint32_t __sinomo_wasix_shared_memory_was_cross_process(void)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_was_cross_process")));
uint32_t __sinomo_wasix_shared_memory_has_direct_mapping(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_has_direct_mapping")));

int __wasilibc_futex_wait_wasix(volatile void *addr, int op, int expected, int64_t max_wait_ns);
int __wasilibc_futex_wake_wasix(int *futex, int cnt);
void pthread_testcancel(void);

struct sinomo_wasix_pthread_cond_cancel_state {
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
};

extern _Thread_local struct sinomo_wasix_pthread_cond_cancel_state *__wasilibc_pthread_self;

static int pthread_cancel_defer_for_cond_relock(void) {
    struct sinomo_wasix_pthread_cond_cancel_state *self = __wasilibc_pthread_self;
    if (self == 0)
        return PTHREAD_CANCEL_ENABLE;
    int old = self->canceldisable;
    self->canceldisable = PTHREAD_CANCEL_DISABLE;
    return old;
}

static void pthread_cancel_restore_after_cond_relock(int old_state) {
    struct sinomo_wasix_pthread_cond_cancel_state *self = __wasilibc_pthread_self;
    if (self != 0)
        self->canceldisable = (uint8_t)old_state;
}

static int cond_is_shared(const pthread_cond_t *cond) {
    return cond->__u.__i[SINOMO_COND_SHARED_WORD] == SINOMO_COND_SHARED;
}

static int cond_has_direct_mapping(const pthread_cond_t *cond) {
    return __sinomo_wasix_shared_memory_has_direct_mapping((uintptr_t)cond, (uintptr_t)SINOMO_COND_RUNTIME_BYTES) != 0;
}

static int needs_runtime_shared_cond(const pthread_cond_t *cond) {
    return cond_is_shared(cond) &&
           __sinomo_wasix_shared_memory_was_cross_process() != 0 &&
           !cond_has_direct_mapping(cond);
}

static int needs_shared_memory_sync(void) {
    return __sinomo_wasix_shared_memory_is_cross_process() != 0 ||
           __sinomo_wasix_shared_memory_was_cross_process() != 0;
}

static int sync_to_if_shared(const pthread_cond_t *cond) {
    if (!cond_is_shared(cond))
        return 0;
    if (!needs_shared_memory_sync())
        return 0;
    if (cond_has_direct_mapping(cond))
        return 0;
    int32_t err = __sinomo_wasix_shared_memory_range_sync_to((uintptr_t)cond, (uintptr_t)sizeof(*cond));
    return err == 0 ? 0 : (int)err;
}

static int sync_from_if_shared(const pthread_cond_t *cond) {
    if (!cond_is_shared(cond))
        return 0;
    if (!needs_shared_memory_sync())
        return 0;
    if (cond_has_direct_mapping(cond))
        return 0;
    int32_t err = __sinomo_wasix_shared_memory_range_sync_from((uintptr_t)cond, (uintptr_t)sizeof(*cond));
    return err == 0 ? 0 : (int)err;
}

static int sync_waiters_from_if_shared(const pthread_cond_t *cond) {
    if (!cond_is_shared(cond))
        return 0;
    if (!needs_shared_memory_sync())
        return 0;
    if (cond_has_direct_mapping(cond))
        return 0;
    int32_t err = __sinomo_wasix_shared_memory_range_sync_from(
        (uintptr_t)&cond->__u.__vi[SINOMO_COND_WAITERS_WORD],
        (uintptr_t)sizeof(cond->__u.__vi[SINOMO_COND_WAITERS_WORD]));
    return err == 0 ? 0 : (int)err;
}

static int sync_non_futex_state_from_guest_if_shared(const pthread_cond_t *cond) {
    if (!cond_is_shared(cond))
        return 0;
    if (!needs_shared_memory_sync())
        return 0;
    if (cond_has_direct_mapping(cond))
        return 0;
    int32_t err = __sinomo_wasix_shared_memory_sync_from_preserving_futexes();
    return err == 0 ? 0 : (int)err;
}

static int64_t abstime_to_wait_ns(clockid_t clock_id, const struct timespec *abstime) {
    if (abstime == 0)
        return -1;
    if (abstime->tv_nsec < 0 || abstime->tv_nsec >= 1000000000L)
        return INT64_MIN;

    int64_t now_sec;
    int64_t now_nsec;
    if (clock_id == CLOCK_REALTIME) {
        struct timeval tv;
        if (gettimeofday(&tv, NULL) != 0)
            return INT64_MIN + 1;
        now_sec = (int64_t)tv.tv_sec;
        now_nsec = (int64_t)tv.tv_usec * 1000LL;
    } else if (clock_id == CLOCK_MONOTONIC) {
        struct timespec now;
        if (clock_gettime(clock_id, &now) != 0)
            return INT64_MIN + 1;
        now_sec = (int64_t)now.tv_sec;
        now_nsec = (int64_t)now.tv_nsec;
    } else {
        return INT64_MIN + 1;
    }

    int64_t sec = (int64_t)abstime->tv_sec - now_sec;
    int64_t nsec = (int64_t)abstime->tv_nsec - now_nsec;
    int64_t total = sec * 1000000000LL + nsec;
    return total <= 0 ? 0 : total;
}

int pthread_cond_init(pthread_cond_t *restrict cond, const pthread_condattr_t *restrict attr) {
    if (cond == 0)
        return EINVAL;

    unsigned flags = attr == 0 ? 0 : attr->__attr;
    (void)__sinomo_wasix_shared_futex_unhint((uintptr_t)cond, (uintptr_t)SINOMO_COND_RUNTIME_BYTES);
    memset(cond, 0, sizeof(*cond));
    cond->__u.__i[SINOMO_COND_SHARED_WORD] = (flags & SINOMO_COND_PSHARED_ATTR) != 0 ? SINOMO_COND_SHARED : 0;
    cond->__u.__i[SINOMO_COND_CLOCK_WORD] = (int)(flags & SINOMO_COND_CLOCK_MASK);
    if ((flags & SINOMO_COND_PSHARED_ATTR) != 0) {
        int32_t hint = __sinomo_wasix_shared_futex_hint((uintptr_t)cond, (uintptr_t)SINOMO_COND_RUNTIME_BYTES);
        if (hint != 0)
            return (int)hint;
        if (__sinomo_wasix_shared_memory_is_cross_process() != 0) {
            int32_t err = __sinomo_wasix_shared_memory_sync_from();
            if (err != 0)
                return (int)err;
        }
    }
    return 0;
}

int pthread_cond_destroy(pthread_cond_t *cond) {
    if (cond == 0)
        return EINVAL;

    int shared = cond_is_shared(cond);
    int cross_process = __sinomo_wasix_shared_memory_is_cross_process() != 0;
    if (shared && cross_process) {
        int32_t err = __sinomo_wasix_shared_memory_sync_to();
        if (err != 0)
            return (int)err;
    }
    return 0;
}

int pthread_cond_signal(pthread_cond_t *cond) {
    if (cond == 0)
        return EINVAL;

    int ret = sync_non_futex_state_from_guest_if_shared(cond);
    if (ret != 0)
        return ret;
    ret = sync_to_if_shared(cond);
    if (ret != 0)
        return ret;
    volatile int *words = cond->__u.__vi;
    int new_seq;
    if (needs_runtime_shared_cond(cond)) {
        new_seq = (int)__sinomo_wasix_shared_cond_broadcast((uintptr_t)cond);
        words[SINOMO_COND_SEQ_WORD] = new_seq;
    } else {
        new_seq = __atomic_add_fetch(&words[SINOMO_COND_SEQ_WORD], 1, __ATOMIC_SEQ_CST);
    }
    if (cond_is_shared(cond)) {
        SINOMO_PTHREAD_LOG("sinomo-debug cond_broadcast pid=%d cond=%p seq=%d waiters=%d\n",
            (int)getpid(), (void *)cond, new_seq, words[SINOMO_COND_WAITERS_WORD]);
    }
    ret = sync_from_if_shared(cond);
    if (ret != 0)
        return ret;
    (void)__wasilibc_futex_wake_wasix((int *)&words[SINOMO_COND_SEQ_WORD], 1);
    return 0;
}

int pthread_cond_broadcast(pthread_cond_t *cond) {
    if (cond == 0)
        return EINVAL;

    SINOMO_PTHREAD_LOG("sinomo-debug cond_broadcast enter pid=%d cond=%p shared=%d word0=%d seq=%d waiters=%d\n",
        (int)getpid(), (void *)cond, cond_is_shared(cond), cond->__u.__i[SINOMO_COND_SHARED_WORD],
        cond->__u.__vi[SINOMO_COND_SEQ_WORD], cond->__u.__vi[SINOMO_COND_WAITERS_WORD]);
    int ret = sync_non_futex_state_from_guest_if_shared(cond);
    if (ret != 0)
        return ret;
    ret = sync_to_if_shared(cond);
    if (ret != 0)
        return ret;
    if (cond_is_shared(cond)) {
        SINOMO_PTHREAD_LOG("sinomo-debug cond_broadcast after_sync_to pid=%d cond=%p seq=%d waiters=%d\n",
            (int)getpid(), (void *)cond, cond->__u.__vi[SINOMO_COND_SEQ_WORD],
            cond->__u.__vi[SINOMO_COND_WAITERS_WORD]);
    }
    volatile int *words = cond->__u.__vi;
    if (needs_runtime_shared_cond(cond)) {
        uint32_t new_seq = __sinomo_wasix_shared_cond_broadcast((uintptr_t)cond);
        words[SINOMO_COND_SEQ_WORD] = (int)new_seq;
    } else {
        __atomic_add_fetch(&words[SINOMO_COND_SEQ_WORD], 1, __ATOMIC_SEQ_CST);
    }
    ret = sync_from_if_shared(cond);
    if (ret != 0)
        return ret;
    (void)__wasilibc_futex_wake_wasix((int *)&words[SINOMO_COND_SEQ_WORD], INT_MAX);
    return 0;
}

int __private_cond_signal(pthread_cond_t *cond, int broadcast) {
    return broadcast < 0 ? pthread_cond_broadcast(cond) : pthread_cond_signal(cond);
}

int pthread_cond_timedwait(pthread_cond_t *restrict cond, pthread_mutex_t *restrict mutex, const struct timespec *restrict abstime) {
    if (cond == 0 || mutex == 0)
        return EINVAL;

    volatile int *words = cond->__u.__vi;
    int ret = 0;
    int seq = __atomic_load_n(&words[SINOMO_COND_SEQ_WORD], __ATOMIC_SEQ_CST);
    clockid_t clock_id = (clockid_t)words[SINOMO_COND_CLOCK_WORD];
    int runtime_shared = needs_runtime_shared_cond(cond);
    int64_t wait_ns = abstime_to_wait_ns(clock_id, abstime);
    if (wait_ns == INT64_MIN) {
        if (cond_is_shared(cond)) {
            SINOMO_PTHREAD_LOG("sinomo-debug cond_timedwait invalid-abstime pid=%d cond=%p mutex=%p clock=%d\n",
                (int)getpid(), (void *)cond, (void *)mutex, (int)clock_id);
        }
        return EINVAL;
    }
    if (wait_ns == INT64_MIN + 1)
        return errno == 0 ? EINVAL : errno;

    SINOMO_PTHREAD_LOG("sinomo-debug cond_wait enter pid=%d cond=%p mutex=%p clock=%d seq=%d wait_ns=%lld waiters=%d\n",
        (int)getpid(), (void *)cond, (void *)mutex, (int)clock_id, seq,
        (long long)wait_ns, words[SINOMO_COND_WAITERS_WORD]);

    words[SINOMO_COND_SEQ_WORD] = seq;
    __atomic_add_fetch(&words[SINOMO_COND_WAITERS_WORD], 1, __ATOMIC_SEQ_CST);

    ret = sync_waiters_from_if_shared(cond);
    if (ret != 0) {
        __atomic_sub_fetch(&words[SINOMO_COND_WAITERS_WORD], 1, __ATOMIC_SEQ_CST);
        (void)sync_waiters_from_if_shared(cond);
        return ret;
    }

    ret = pthread_mutex_unlock(mutex);
    if (ret != 0) {
        SINOMO_PTHREAD_LOG("sinomo-debug cond_wait unlock_failed pid=%d cond=%p mutex=%p ret=%d\n",
            (int)getpid(), (void *)cond, (void *)mutex, ret);
        __atomic_sub_fetch(&words[SINOMO_COND_WAITERS_WORD], 1, __ATOMIC_SEQ_CST);
        (void)sync_waiters_from_if_shared(cond);
        return ret;
    }
    SINOMO_PTHREAD_LOG("sinomo-debug cond_wait unlocked pid=%d cond=%p mutex=%p seq=%d wait_ns=%lld waiters=%d\n",
        (int)getpid(), (void *)cond, (void *)mutex, seq, (long long)wait_ns,
        words[SINOMO_COND_WAITERS_WORD]);

    if (runtime_shared) {
        unsigned spin_count = 0;
        for (;;) {
            int current_seq = (int)__sinomo_wasix_shared_cond_sequence((uintptr_t)cond);
            words[SINOMO_COND_SEQ_WORD] = current_seq;
            if (current_seq != seq) {
                SINOMO_PTHREAD_LOG("sinomo-debug cond_wait observed pid=%d cond=%p old_seq=%d new_seq=%d\n",
                    (int)getpid(), (void *)cond, seq, current_seq);
                ret = 0;
                break;
            }
            spin_count++;
            if ((spin_count % 10000u) == 0u) {
                SINOMO_PTHREAD_LOG("sinomo-debug cond_wait still_waiting pid=%d cond=%p seq=%d current_seq=%d waiters=%d wait_ns=%lld\n",
                    (int)getpid(), (void *)cond, seq, current_seq,
                    words[SINOMO_COND_WAITERS_WORD], (long long)wait_ns);
            }
            wait_ns = abstime_to_wait_ns(clock_id, abstime);
            if (wait_ns == INT64_MIN) {
                ret = EINVAL;
                break;
            }
            if (wait_ns == INT64_MIN + 1) {
                ret = errno == 0 ? EINVAL : errno;
                break;
            }
            if (wait_ns == 0) {
                SINOMO_PTHREAD_LOG("sinomo-debug cond_wait timeout pid=%d cond=%p seq=%d current_seq=%d waiters=%d\n",
                    (int)getpid(), (void *)cond, seq,
                    (int)__sinomo_wasix_shared_cond_sequence((uintptr_t)cond),
                    words[SINOMO_COND_WAITERS_WORD]);
                ret = ETIMEDOUT;
                break;
            }
            int wait_ret = __sinomo_wasix_shared_cond_wait((uintptr_t)cond, (uint32_t)seq, wait_ns);
            if (wait_ret == ETIMEDOUT) {
                ret = ETIMEDOUT;
                break;
            }
            if (wait_ret != 0 && wait_ret != EINTR && wait_ret != ECANCELED) {
                ret = wait_ret;
                break;
            }
        }
    }
    else {
        int wait_ret = __wasilibc_futex_wait_wasix(&words[SINOMO_COND_SEQ_WORD], 0, seq, wait_ns);
        SINOMO_PTHREAD_LOG("sinomo-debug cond_wait futex_return pid=%d cond=%p mutex=%p seq=%d wait_ret=%d current_seq=%d waiters=%d\n",
            (int)getpid(), (void *)cond, (void *)mutex, seq, wait_ret,
            words[SINOMO_COND_SEQ_WORD], words[SINOMO_COND_WAITERS_WORD]);
        if (wait_ret == -ETIMEDOUT)
            ret = ETIMEDOUT;
        else if (wait_ret == -EAGAIN || wait_ret == -EINTR || wait_ret == -ECANCELED || wait_ret == 0)
            ret = 0;
        else if (wait_ret < 0)
            ret = -wait_ret;
        else
            ret = wait_ret;
    }

    /*
     * Do not touch the condvar after the wait has been released. POSIX allows
     * another thread to broadcast, destroy the condvar, and reuse its storage
     * once no thread is blocked on it; at this point this waiter has already
     * been unblocked and only needs to reacquire the mutex before returning.
     */
    SINOMO_PTHREAD_LOG("sinomo-debug cond_wait relock_enter pid=%d cond=%p mutex=%p ret=%d\n",
        (int)getpid(), (void *)cond, (void *)mutex, ret);
    int old_cancel_state = pthread_cancel_defer_for_cond_relock();
    int lock_ret = pthread_mutex_lock(mutex);
    pthread_cancel_restore_after_cond_relock(old_cancel_state);
    SINOMO_PTHREAD_LOG("sinomo-debug cond_wait relock_done pid=%d cond=%p mutex=%p ret=%d lock_ret=%d old_cancel_state=%d\n",
        (int)getpid(), (void *)cond, (void *)mutex, ret, lock_ret, old_cancel_state);
    if (lock_ret == 0) {
        SINOMO_PTHREAD_LOG("sinomo-debug cond_wait testcancel pid=%d cond=%p mutex=%p\n",
            (int)getpid(), (void *)cond, (void *)mutex);
        pthread_testcancel();
    }
    if (ret == 0 && lock_ret != 0)
        ret = lock_ret;
    return ret;
}

int __pthread_cond_timedwait(pthread_cond_t *restrict cond, pthread_mutex_t *restrict mutex, const struct timespec *restrict abstime) {
    return pthread_cond_timedwait(cond, mutex, abstime);
}

int pthread_cond_wait(pthread_cond_t *restrict cond, pthread_mutex_t *restrict mutex) {
    return pthread_cond_timedwait(cond, mutex, 0);
}
