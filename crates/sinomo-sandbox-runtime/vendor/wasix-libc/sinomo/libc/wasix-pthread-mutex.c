#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <wasi/api.h>

#define SINOMO_MUTEX_TYPE_MASK 3
#define SINOMO_MUTEX_ROBUST 4
#define SINOMO_MUTEX_PROTOCOL_SHIFT 3
#define SINOMO_MUTEX_PROTOCOL_MASK (3u << SINOMO_MUTEX_PROTOCOL_SHIFT)
#define SINOMO_MUTEX_PSHARED 128
#define SINOMO_MUTEX_PRIOCEILING_SHIFT 16
#define SINOMO_MUTEX_PRIOCEILING_MASK (0x7fffu << SINOMO_MUTEX_PRIOCEILING_SHIFT)
#define SINOMO_MUTEX_ATTR_MAGIC 0x80000000u
#define SINOMO_MUTEX_PRIOCEILING_WORD 4
#define SINOMO_MUTEX_RUNTIME_WORDS 5
#define SINOMO_MUTEX_RUNTIME_BYTES (SINOMO_MUTEX_RUNTIME_WORDS * sizeof(int))
#define SINOMO_MUTEX_PSHARED_WAKE_BATCH 8
#ifndef SINOMO_PTHREAD_DEBUG
#define SINOMO_PTHREAD_DEBUG 0
#endif
#define SINOMO_PTHREAD_LOG(...) do { if (SINOMO_PTHREAD_DEBUG || getenv("SINOMO_PTHREAD_LOG") != 0) fprintf(stderr, __VA_ARGS__); } while (0)

int32_t __sinomo_wasix_shared_memory_sync_from(void)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_sync_from")));
int32_t __sinomo_wasix_shared_memory_sync_to(void)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_sync_to")));
int32_t __sinomo_wasix_shared_memory_range_sync_from(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_range_sync_from")));
int32_t __sinomo_wasix_shared_memory_range_sync_to(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_range_sync_to")));
int32_t __sinomo_wasix_shared_mutex_try_lock(uintptr_t offset, uint32_t mutex_type, uint32_t owner_tid)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_mutex_try_lock")));
int32_t __sinomo_wasix_shared_mutex_unlock(uintptr_t offset, uint32_t mutex_type, uint32_t owner_tid)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_mutex_unlock")));
int32_t __sinomo_wasix_shared_mutex_wait(uintptr_t offset, uint32_t observed_owner, int64_t timeout_ns)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_mutex_wait")));
int32_t __sinomo_wasix_shared_futex_hint(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_futex_hint")));
int32_t __sinomo_wasix_shared_futex_unhint(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_futex_unhint")));
uint32_t __sinomo_wasix_shared_memory_is_cross_process(void)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_is_cross_process")));
uint32_t __sinomo_wasix_shared_memory_was_cross_process(void)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_was_cross_process")));
uint32_t __sinomo_wasix_shared_memory_has_direct_mapping(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("shared_memory_has_direct_mapping")));

int __wasilibc_futex_wait_wasix(volatile void *addr, int op, int expected, int64_t max_wait_ns);
int __wasilibc_futex_wake_wasix(int *futex, int cnt);
void pthread_testcancel(void);

struct sinomo_wasix_pthread_prefix {
    struct sinomo_wasix_pthread_prefix *self;
    struct sinomo_wasix_pthread_prefix *prev;
    struct sinomo_wasix_pthread_prefix *next;
    uintptr_t sysinfo;
    uintptr_t canary;
    int tid;
};

extern _Thread_local struct sinomo_wasix_pthread_prefix *__wasilibc_pthread_self;

static int current_tid(void) {
    int thread_id = 1;
    __wasi_tid_t wasi_tid = 0;
    if (__wasi_thread_id(&wasi_tid) == 0 && wasi_tid > 0 && wasi_tid < 0x3fffffff) {
        thread_id = (int)wasi_tid;
    } else if (__wasilibc_pthread_self != 0 && __wasilibc_pthread_self->tid > 0) {
        thread_id = __wasilibc_pthread_self->tid & 0x7fffffff;
    }
    int process_id = (int)(getpid() & 0x7fffffff);
    int token = (int)(((uint32_t)process_id * 65537u) ^ (uint32_t)thread_id);
    token &= 0x7fffffff;
    return token == 0 ? 1 : token;
}

static int sync_to_if_shared(volatile int *words) {
    if ((words[0] & SINOMO_MUTEX_PSHARED) == 0)
        return 0;
    if (__sinomo_wasix_shared_memory_is_cross_process() == 0 &&
        __sinomo_wasix_shared_memory_was_cross_process() == 0)
        return 0;
    if (__sinomo_wasix_shared_memory_has_direct_mapping((uintptr_t)words, (uintptr_t)SINOMO_MUTEX_RUNTIME_BYTES) != 0)
        return 0;
    int32_t err = __sinomo_wasix_shared_memory_sync_to();
    return err == 0 ? 0 : (int)err;
}

static int sync_from_if_shared(volatile int *words) {
    if ((words[0] & SINOMO_MUTEX_PSHARED) == 0)
        return 0;
    if (__sinomo_wasix_shared_memory_is_cross_process() == 0 &&
        __sinomo_wasix_shared_memory_was_cross_process() == 0)
        return 0;
    if (__sinomo_wasix_shared_memory_has_direct_mapping((uintptr_t)words, (uintptr_t)SINOMO_MUTEX_RUNTIME_BYTES) != 0)
        return 0;
    int32_t err = __sinomo_wasix_shared_memory_sync_from();
    return err == 0 ? 0 : (int)err;
}

static int sync_mutex_range_to_if_was_cross(volatile int *words) {
    (void)words;
    if ((words[0] & SINOMO_MUTEX_PSHARED) == 0)
        return 0;
    if (__sinomo_wasix_shared_memory_was_cross_process() == 0)
        return 0;
    if (__sinomo_wasix_shared_memory_has_direct_mapping((uintptr_t)words, (uintptr_t)SINOMO_MUTEX_RUNTIME_BYTES) != 0)
        return 0;
    int32_t err = __sinomo_wasix_shared_memory_sync_to();
    return err == 0 ? 0 : (int)err;
}

static int sync_mutex_range_from_if_was_cross(volatile int *words) {
    (void)words;
    if ((words[0] & SINOMO_MUTEX_PSHARED) == 0)
        return 0;
    if (__sinomo_wasix_shared_memory_was_cross_process() == 0)
        return 0;
    if (__sinomo_wasix_shared_memory_has_direct_mapping((uintptr_t)words, (uintptr_t)SINOMO_MUTEX_RUNTIME_BYTES) != 0)
        return 0;
    int32_t err = __sinomo_wasix_shared_memory_sync_from();
    return err == 0 ? 0 : (int)err;
}

static int needs_runtime_shared_mutex(volatile int *words) {
    return (words[0] & SINOMO_MUTEX_PSHARED) != 0 &&
           __sinomo_wasix_shared_memory_was_cross_process() != 0 &&
           __sinomo_wasix_shared_memory_has_direct_mapping((uintptr_t)words, (uintptr_t)SINOMO_MUTEX_RUNTIME_BYTES) == 0;
}

static int mutex_default_prioceiling(void) {
    int min = sched_get_priority_min(SCHED_FIFO);
    int max = sched_get_priority_max(SCHED_FIFO);
    if (min >= 0 && max >= min)
        return min;
    return 0;
}

static int mutex_valid_prioceiling(int prioceiling) {
    int min = sched_get_priority_min(SCHED_FIFO);
    int max = sched_get_priority_max(SCHED_FIFO);
    if (min < 0 || max < min)
        return prioceiling >= 0;
    return prioceiling >= min && prioceiling <= max;
}

static unsigned mutex_attr_prioceiling_bits(int prioceiling) {
    return ((unsigned)prioceiling << SINOMO_MUTEX_PRIOCEILING_SHIFT) & SINOMO_MUTEX_PRIOCEILING_MASK;
}

static int mutex_attr_prioceiling(unsigned attr) {
    unsigned encoded = (attr & SINOMO_MUTEX_PRIOCEILING_MASK) >> SINOMO_MUTEX_PRIOCEILING_SHIFT;
    return encoded == 0 ? mutex_default_prioceiling() : (int)encoded;
}

static int64_t realtime_abstime_to_wait_ns(const struct timespec *abstime) {
    if (abstime == 0)
        return -1;
    if (abstime->tv_nsec < 0 || abstime->tv_nsec >= 1000000000L)
        return INT64_MIN;

    struct timeval tv;
    if (gettimeofday(&tv, NULL) != 0)
        return INT64_MIN + 1;

    int64_t sec = (int64_t)abstime->tv_sec - (int64_t)tv.tv_sec;
    int64_t nsec = (int64_t)abstime->tv_nsec - ((int64_t)tv.tv_usec * 1000LL);
    if (sec > (INT64_MAX / 1000000000LL) - 1)
        return INT64_MAX;
    if (sec < (INT64_MIN / 1000000000LL) + 1)
        return 0;
    int64_t total = sec * 1000000000LL + nsec;
    return total <= 0 ? 0 : total;
}

static int local_mutex_trylock_words(volatile int *words) {
    int tid = current_tid();
    int owner = __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff;
    int type = words[0] & SINOMO_MUTEX_TYPE_MASK;
    if (owner == tid) {
        if (type == PTHREAD_MUTEX_RECURSIVE) {
            words[3]++;
            return 0;
        }
        return EBUSY;
    }

    int expected = 0;
    if (!__atomic_compare_exchange_n(&words[1], &expected, tid, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
        return EBUSY;

    words[3] = 0;
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    SINOMO_PTHREAD_LOG("sinomo-debug local_mutex_trylock_success pid=%d tid=%d mutex_words=%p type=%d\n",
        (int)getpid(), tid, (void *)words, words[0] & SINOMO_MUTEX_TYPE_MASK);
    return 0;
}

static int local_mutex_lock_words(volatile int *words) {
    int tid = current_tid();
    unsigned wait_debug_count = 0;
    for (;;) {
        int owner = __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff;
        int type = words[0] & SINOMO_MUTEX_TYPE_MASK;
        if (owner == tid) {
            if (type == PTHREAD_MUTEX_RECURSIVE) {
                words[3]++;
                return 0;
            }
            if (type == PTHREAD_MUTEX_ERRORCHECK)
                return EDEADLK;
            for (;;) {
                __wasi_errno_t sleep_ret = __wasi_thread_sleep(10000000ULL);
                if (sleep_ret != 0 && sleep_ret != __WASI_ERRNO_INTR)
                    return (int)sleep_ret;
            }
        }

        int expected = 0;
        if (__atomic_compare_exchange_n(&words[1], &expected, tid, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
            words[3] = 0;
            __atomic_thread_fence(__ATOMIC_SEQ_CST);
            SINOMO_PTHREAD_LOG("sinomo-debug local_mutex_lock_success pid=%d tid=%d mutex_words=%p type=%d\n",
                (int)getpid(), tid, (void *)words, type);
            return 0;
        }

        words[2] = 1;
        wait_debug_count++;
        if ((type == PTHREAD_MUTEX_RECURSIVE || (words[0] & SINOMO_MUTEX_PSHARED) != 0) &&
            (wait_debug_count == 1 || (wait_debug_count % 10000u) == 0u)) {
            SINOMO_PTHREAD_LOG("sinomo-debug local_mutex_wait pid=%d tid=%d type=%d flags=%d owner=%d depth=%d waiters=%d count=%u\n",
                (int)getpid(), tid, type, words[0], owner, words[3], words[2], wait_debug_count);
        }
        int ret = __wasilibc_futex_wait_wasix(&words[1], 0, expected, -1);
        if (ret != 0 && ret != -EAGAIN && ret != -EINTR && ret != -ECANCELED)
            return -ret;
    }
}

static int local_mutex_unlock_words(volatile int *words) {
    int tid = current_tid();
    int owner = __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff;
    int type = words[0] & SINOMO_MUTEX_TYPE_MASK;
    if (owner != tid && type != PTHREAD_MUTEX_NORMAL)
        return EPERM;

    if (type == PTHREAD_MUTEX_RECURSIVE && words[3] > 0) {
        SINOMO_PTHREAD_LOG("sinomo-debug local_recursive_mutex_unlock_decrement pid=%d tid=%d owner=%d depth=%d waiters=%d\n",
            (int)getpid(), tid, owner, words[3], words[2]);
        words[3]--;
        return 0;
    }

    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    __atomic_store_n(&words[1], 0, __ATOMIC_SEQ_CST);
    SINOMO_PTHREAD_LOG("sinomo-debug local_mutex_unlock_success pid=%d tid=%d mutex_words=%p type=%d\n",
        (int)getpid(), tid, (void *)words, type);
    if (words[2] != 0) {
        if (type == PTHREAD_MUTEX_RECURSIVE || (words[0] & SINOMO_MUTEX_PSHARED) != 0) {
            SINOMO_PTHREAD_LOG("sinomo-debug local_mutex_unlock_wake pid=%d tid=%d type=%d flags=%d owner=%d depth=%d waiters=%d\n",
                (int)getpid(), tid, type, words[0], owner, words[3], words[2]);
        }
        (void)__wasilibc_futex_wake_wasix((int *)&words[1], 1);
    }
    return 0;
}

int pthread_mutex_trylock(pthread_mutex_t *mutex) {
    if (mutex == 0)
        return EINVAL;

    volatile int *words = mutex->__u.__vi;
    if (needs_runtime_shared_mutex(words)) {
        int tid = current_tid();
        int type = words[0] & SINOMO_MUTEX_TYPE_MASK;
        int owner = __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff;
        int depth = __atomic_load_n(&words[3], __ATOMIC_SEQ_CST);
        if (owner == tid && type == PTHREAD_MUTEX_ERRORCHECK)
            return EBUSY;
        int32_t ret = __sinomo_wasix_shared_mutex_try_lock((uintptr_t)mutex, (uint32_t)type, (uint32_t)tid);
        if (ret != 0 && ret != EBUSY) {
            SINOMO_PTHREAD_LOG("sinomo-debug pshared_mutex_trylock pid=%d mutex=%p type=%d tid=%d owner_before=%d depth_before=%d ret=%d owner_after=%d depth_after=%d\n",
                (int)getpid(), (void *)mutex, type, tid, owner, depth, (int)ret,
                __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff,
                __atomic_load_n(&words[3], __ATOMIC_SEQ_CST));
        }
        if (ret == EINVAL)
            return local_mutex_trylock_words(words);
        return ret == 0 ? 0 : (int)ret;
    }

    int ret = sync_to_if_shared(words);
    if (ret != 0)
        return ret;
    ret = sync_mutex_range_to_if_was_cross(words);
    if (ret != 0)
        return ret;

    int tid = current_tid();
    int owner = __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff;
    int type = words[0] & SINOMO_MUTEX_TYPE_MASK;
    if (owner == tid) {
        if (type == PTHREAD_MUTEX_RECURSIVE) {
            words[3]++;
            return sync_from_if_shared(words);
        }
        return EBUSY;
    }

    int expected = 0;
    if (!__atomic_compare_exchange_n(&words[1], &expected, tid, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
        return EBUSY;

    words[3] = 0;
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    SINOMO_PTHREAD_LOG("sinomo-debug pthread_mutex_trylock_success pid=%d tid=%d mutex_words=%p type=%d\n",
        (int)getpid(), tid, (void *)words, type);
    ret = sync_mutex_range_from_if_was_cross(words);
    if (ret != 0)
        return ret;
    return sync_from_if_shared(words);
}

int pthread_mutex_lock(pthread_mutex_t *mutex) {
    if (mutex == 0)
        return EINVAL;

    volatile int *words = mutex->__u.__vi;
    if (needs_runtime_shared_mutex(words)) {
        unsigned runtime_wait_debug_count = 0;
        for (;;) {
            int tid = current_tid();
            int owner = __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff;
            int type = words[0] & SINOMO_MUTEX_TYPE_MASK;
            if (owner == tid && type == PTHREAD_MUTEX_ERRORCHECK)
                return EDEADLK;
            int32_t ret = __sinomo_wasix_shared_mutex_try_lock((uintptr_t)mutex, (uint32_t)type, (uint32_t)tid);
            if (ret != 0 && ret != EBUSY) {
                SINOMO_PTHREAD_LOG("sinomo-debug pshared_mutex_lock pid=%d mutex=%p type=%d tid=%d owner_before=%d ret=%d owner_after=%d depth_after=%d\n",
                    (int)getpid(), (void *)mutex, type, tid, owner, (int)ret,
                    __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff,
                    __atomic_load_n(&words[3], __ATOMIC_SEQ_CST));
            }
            if (ret == 0) {
                __atomic_store_n(&words[1], tid, __ATOMIC_SEQ_CST);
                if (type != PTHREAD_MUTEX_RECURSIVE)
                    __atomic_store_n(&words[3], 0, __ATOMIC_SEQ_CST);
                __atomic_thread_fence(__ATOMIC_SEQ_CST);
                return 0;
            }
            if (ret == EINVAL) {
                SINOMO_PTHREAD_LOG("sinomo-debug pshared_mutex_lock_fallback_local pid=%d mutex=%p type=%d tid=%d owner_before=%d owner_after=%d depth_after=%d\n",
                    (int)getpid(), (void *)mutex, type, tid, owner,
                    __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff,
                    __atomic_load_n(&words[3], __ATOMIC_SEQ_CST));
                return local_mutex_lock_words(words);
            }
            if (ret != EBUSY)
                return (int)ret;
            runtime_wait_debug_count++;
            if (runtime_wait_debug_count == 1 || (runtime_wait_debug_count % 10000u) == 0u) {
                SINOMO_PTHREAD_LOG("sinomo-debug pshared_mutex_lock_busy pid=%d mutex=%p type=%d tid=%d owner_before=%d owner_after=%d depth_after=%d count=%u\n",
                    (int)getpid(), (void *)mutex, type, tid, owner,
                    __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff,
                    __atomic_load_n(&words[3], __ATOMIC_SEQ_CST),
                    runtime_wait_debug_count);
            }
            uint32_t observed_owner = (uint32_t)__atomic_load_n(&words[1], __ATOMIC_SEQ_CST);
            if ((observed_owner & 0x7fffffff) == 0) {
                (void)__sinomo_wasix_shared_mutex_wait((uintptr_t)mutex, observed_owner, 10000000);
                continue;
            }
            int wait_ret = __sinomo_wasix_shared_mutex_wait((uintptr_t)mutex, observed_owner, 100000000);
            if (wait_ret != 0 && wait_ret != ETIMEDOUT && wait_ret != EINTR && wait_ret != ECANCELED)
                return wait_ret;
        }
    }

    if ((words[0] & SINOMO_MUTEX_PSHARED) != 0) {
        SINOMO_PTHREAD_LOG("sinomo-debug pshared_local_mutex_lock pid=%d tid=%d flags=%d owner=%d depth=%d cross=%u\n",
            (int)getpid(), current_tid(), words[0], __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff,
            __atomic_load_n(&words[3], __ATOMIC_SEQ_CST), __sinomo_wasix_shared_memory_is_cross_process());
    }

    int tid = current_tid();
    unsigned wait_debug_count = 0;
    for (;;) {
        int ret = sync_to_if_shared(words);
        if (ret != 0)
            return ret;
        ret = sync_mutex_range_to_if_was_cross(words);
        if (ret != 0)
            return ret;

        int owner = __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff;
        int type = words[0] & SINOMO_MUTEX_TYPE_MASK;
        if (owner == tid) {
            if (type == PTHREAD_MUTEX_RECURSIVE) {
                words[3]++;
                return sync_from_if_shared(words);
            }
            if (type == PTHREAD_MUTEX_ERRORCHECK)
                return EDEADLK;
            for (;;) {
                __wasi_errno_t sleep_ret = __wasi_thread_sleep(10000000ULL);
                if (sleep_ret != 0 && sleep_ret != __WASI_ERRNO_INTR)
                    return (int)sleep_ret;
            }
        }

        int expected = 0;
        if (__atomic_compare_exchange_n(&words[1], &expected, tid, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
            words[3] = 0;
            __atomic_thread_fence(__ATOMIC_SEQ_CST);
            SINOMO_PTHREAD_LOG("sinomo-debug pthread_mutex_lock_success pid=%d tid=%d mutex_words=%p type=%d\n",
                (int)getpid(), tid, (void *)words, type);
            ret = sync_mutex_range_from_if_was_cross(words);
            if (ret != 0)
                return ret;
            return sync_from_if_shared(words);
        }

        words[2] = 1;
        ret = sync_mutex_range_from_if_was_cross(words);
        if (ret != 0)
            return ret;
        wait_debug_count++;
        if ((type == PTHREAD_MUTEX_RECURSIVE || (words[0] & SINOMO_MUTEX_PSHARED) != 0) &&
            (wait_debug_count == 1 || (wait_debug_count % 10000u) == 0u)) {
            SINOMO_PTHREAD_LOG("sinomo-debug pthread_mutex_wait pid=%d tid=%d type=%d flags=%d owner=%d depth=%d waiters=%d expected=%d count=%u cross=%u\n",
                (int)getpid(), tid, type, words[0], owner, words[3], words[2], expected, wait_debug_count,
                __sinomo_wasix_shared_memory_is_cross_process());
        }
        ret = sync_from_if_shared(words);
        if (ret != 0)
            return ret;
        ret = __wasilibc_futex_wait_wasix(&words[1], 0, expected, -1);
        if (ret != 0 && ret != -EAGAIN && ret != -EINTR && ret != -ECANCELED)
            return -ret;
    }
}

int pthread_mutex_timedlock(pthread_mutex_t *mutex, const struct timespec *timeout) {
    if (mutex == 0)
        return EINVAL;
    if (timeout == 0)
        return pthread_mutex_lock(mutex);

    int ret = pthread_mutex_trylock(mutex);
    if (ret != EBUSY)
        return ret;

    volatile int *words = mutex->__u.__vi;
    int tid = current_tid();
    for (;;) {
        int64_t wait_ns = realtime_abstime_to_wait_ns(timeout);
        if (wait_ns == INT64_MIN)
            return EINVAL;
        if (wait_ns == INT64_MIN + 1)
            return errno == 0 ? EINVAL : errno;
        if (wait_ns == 0)
            return ETIMEDOUT;

        if (needs_runtime_shared_mutex(words)) {
            int type = words[0] & SINOMO_MUTEX_TYPE_MASK;
            int owner = __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff;
            if (owner == tid && type == PTHREAD_MUTEX_ERRORCHECK)
                return EDEADLK;

            int32_t lock_ret = __sinomo_wasix_shared_mutex_try_lock((uintptr_t)mutex, (uint32_t)type, (uint32_t)tid);
            if (lock_ret == 0) {
                __atomic_store_n(&words[1], tid, __ATOMIC_SEQ_CST);
                if (type != PTHREAD_MUTEX_RECURSIVE)
                    __atomic_store_n(&words[3], 0, __ATOMIC_SEQ_CST);
                __atomic_thread_fence(__ATOMIC_SEQ_CST);
                return 0;
            }
            if (lock_ret == EINVAL) {
                ret = local_mutex_trylock_words(words);
                if (ret != EBUSY)
                    return ret;
            } else if (lock_ret != EBUSY) {
                return (int)lock_ret;
            }

            uint32_t observed_owner = (uint32_t)__atomic_load_n(&words[1], __ATOMIC_SEQ_CST);
            if ((observed_owner & 0x7fffffff) == 0) {
                (void)__sinomo_wasix_shared_mutex_wait((uintptr_t)mutex, observed_owner, wait_ns < 10000000 ? wait_ns : 10000000);
                continue;
            }
            int wait_ret = __sinomo_wasix_shared_mutex_wait((uintptr_t)mutex, observed_owner, wait_ns);
            if (wait_ret == ETIMEDOUT)
                return ETIMEDOUT;
            if (wait_ret != 0 && wait_ret != EINTR && wait_ret != ECANCELED)
                return wait_ret;
            continue;
        }

        ret = sync_to_if_shared(words);
        if (ret != 0)
            return ret;

        int owner = __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff;
        int type = words[0] & SINOMO_MUTEX_TYPE_MASK;
        if (owner == tid) {
            if (type == PTHREAD_MUTEX_RECURSIVE) {
                words[3]++;
                return sync_from_if_shared(words);
            }
            if (type == PTHREAD_MUTEX_ERRORCHECK)
                return EDEADLK;
        }

        int expected = 0;
        if (__atomic_compare_exchange_n(&words[1], &expected, tid, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
            words[3] = 0;
            __atomic_thread_fence(__ATOMIC_SEQ_CST);
            return sync_from_if_shared(words);
        }

        words[2] = 1;
        ret = sync_from_if_shared(words);
        if (ret != 0)
            return ret;
        int wait_ret = __wasilibc_futex_wait_wasix(&words[1], 0, expected, wait_ns);
        if (wait_ret == -ETIMEDOUT)
            return ETIMEDOUT;
        if (wait_ret != 0 && wait_ret != -EAGAIN && wait_ret != -EINTR && wait_ret != -ECANCELED)
            return -wait_ret;
    }
}

int __pthread_mutex_timedlock(pthread_mutex_t *mutex, const struct timespec *timeout) {
    return pthread_mutex_timedlock(mutex, timeout);
}

int __pthread_mutex_lock(pthread_mutex_t *mutex) {
    return pthread_mutex_lock(mutex);
}

int __pthread_mutex_trylock(pthread_mutex_t *mutex) {
    return pthread_mutex_trylock(mutex);
}

int __pthread_mutex_trylock_owner(pthread_mutex_t *mutex) {
    return pthread_mutex_trylock(mutex);
}

int pthread_mutex_init(pthread_mutex_t *restrict mutex, const pthread_mutexattr_t *restrict attr) {
    if (mutex == 0)
        return EINVAL;

    unsigned attr_flags = attr == 0 ? 0 : attr->__attr;
    if (attr != 0 && (attr_flags & SINOMO_MUTEX_ATTR_MAGIC) == 0)
        return EINVAL;
    unsigned flags = attr_flags & ~(SINOMO_MUTEX_ATTR_MAGIC | SINOMO_MUTEX_PRIOCEILING_MASK);
    (void)__sinomo_wasix_shared_futex_unhint((uintptr_t)mutex, (uintptr_t)SINOMO_MUTEX_RUNTIME_BYTES);
    memset(mutex, 0, sizeof(*mutex));
    mutex->__u.__vi[0] = (int)flags;
    mutex->__u.__vi[SINOMO_MUTEX_PRIOCEILING_WORD] = mutex_attr_prioceiling(attr_flags);
    if (__sinomo_wasix_shared_memory_was_cross_process() != 0) {
        int32_t err = __sinomo_wasix_shared_memory_range_sync_from(
            (uintptr_t)mutex, (uintptr_t)SINOMO_MUTEX_RUNTIME_BYTES);
        if (err != 0)
            return (int)err;
    }
    if ((flags & SINOMO_MUTEX_PSHARED) != 0) {
        int32_t hint = __sinomo_wasix_shared_futex_hint((uintptr_t)mutex, (uintptr_t)SINOMO_MUTEX_RUNTIME_BYTES);
        if (hint != 0)
            return (int)hint;
        int32_t err = __sinomo_wasix_shared_memory_sync_from();
        if (err != 0)
            return (int)err;
    }
    return 0;
}

int pthread_mutex_unlock(pthread_mutex_t *mutex) {
    if (mutex == 0)
        return EINVAL;

    volatile int *words = mutex->__u.__vi;
    if (needs_runtime_shared_mutex(words)) {
        int type = words[0] & SINOMO_MUTEX_TYPE_MASK;
        int tid = current_tid();
        int owner = __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff;
        int depth = __atomic_load_n(&words[3], __ATOMIC_SEQ_CST);
        __atomic_thread_fence(__ATOMIC_SEQ_CST);
        int32_t ret = __sinomo_wasix_shared_mutex_unlock((uintptr_t)mutex, (uint32_t)type, (uint32_t)tid);
        if (ret != 0) {
            SINOMO_PTHREAD_LOG("sinomo-debug pshared_mutex_unlock pid=%d mutex=%p type=%d tid=%d owner_before=%d depth_before=%d ret=%d owner_after=%d depth_after=%d\n",
                (int)getpid(), (void *)mutex, type, tid, owner, depth, (int)ret,
                __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff,
                __atomic_load_n(&words[3], __ATOMIC_SEQ_CST));
        } else if (type == PTHREAD_MUTEX_NORMAL) {
            SINOMO_PTHREAD_LOG("sinomo-debug pshared_mutex_unlock_success pid=%d mutex=%p type=%d tid=%d owner_before=%d depth_before=%d owner_after=%d depth_after=%d\n",
                (int)getpid(), (void *)mutex, type, tid, owner, depth,
                __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff,
                __atomic_load_n(&words[3], __ATOMIC_SEQ_CST));
        }
        if (ret == 0 && !(type == PTHREAD_MUTEX_RECURSIVE && depth > 0)) {
            __atomic_thread_fence(__ATOMIC_SEQ_CST);
            __atomic_store_n(&words[1], 0, __ATOMIC_SEQ_CST);
            __atomic_store_n(&words[3], 0, __ATOMIC_SEQ_CST);
        }
        if (ret == 0)
            (void)__wasilibc_futex_wake_wasix((int *)&words[1], SINOMO_MUTEX_PSHARED_WAKE_BATCH);
        if (ret == EINVAL)
            return local_mutex_unlock_words(words);
        return ret == 0 ? 0 : (int)ret;
    }

    int ret = sync_to_if_shared(words);
    if (ret != 0)
        return ret;

    int tid = current_tid();
    int owner = __atomic_load_n(&words[1], __ATOMIC_SEQ_CST) & 0x7fffffff;
    int type = words[0] & SINOMO_MUTEX_TYPE_MASK;
    if (owner != tid && type != PTHREAD_MUTEX_NORMAL)
        return EPERM;

    if (type == PTHREAD_MUTEX_RECURSIVE && words[3] > 0) {
        words[3]--;
        return sync_from_if_shared(words);
    }

    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    __atomic_store_n(&words[1], 0, __ATOMIC_SEQ_CST);
    SINOMO_PTHREAD_LOG("sinomo-debug pthread_mutex_unlock_success pid=%d tid=%d mutex_words=%p type=%d\n",
        (int)getpid(), tid, (void *)words, type);
    ret = sync_from_if_shared(words);
    if (ret != 0)
        return ret;
    if (words[2] != 0 || (words[0] & SINOMO_MUTEX_PSHARED) != 0 ||
        __sinomo_wasix_shared_memory_was_cross_process() != 0) {
        int wake_count = (words[0] & SINOMO_MUTEX_PSHARED) != 0 ? INT32_MAX : 1;
        if (type == PTHREAD_MUTEX_RECURSIVE || (words[0] & SINOMO_MUTEX_PSHARED) != 0) {
            SINOMO_PTHREAD_LOG("sinomo-debug pthread_mutex_unlock_wake pid=%d tid=%d type=%d flags=%d owner=%d depth=%d waiters=%d wake_count=%d cross=%u\n",
                (int)getpid(), tid, type, words[0], owner, words[3], words[2], wake_count,
                __sinomo_wasix_shared_memory_is_cross_process());
        }
        (void)__wasilibc_futex_wake_wasix((int *)&words[1], wake_count);
    }
    return 0;
}

int __pthread_mutex_unlock(pthread_mutex_t *mutex) {
    return pthread_mutex_unlock(mutex);
}

int pthread_mutex_getprioceiling(const pthread_mutex_t *restrict mutex, int *restrict ceiling) {
    if (mutex == 0 || ceiling == 0)
        return EINVAL;
    const volatile int *words = mutex->__u.__vi;
    int stored = __atomic_load_n(&words[SINOMO_MUTEX_PRIOCEILING_WORD], __ATOMIC_SEQ_CST);
    *ceiling = stored == 0 ? mutex_default_prioceiling() : stored;
    return 0;
}

int pthread_mutex_setprioceiling(pthread_mutex_t *restrict mutex, int prioceiling, int *restrict old_ceiling) {
    if (mutex == 0)
        return EINVAL;
    if (!mutex_valid_prioceiling(prioceiling))
        return EINVAL;
    volatile int *words = mutex->__u.__vi;
    int old = __atomic_exchange_n(&words[SINOMO_MUTEX_PRIOCEILING_WORD], prioceiling, __ATOMIC_SEQ_CST);
    if (old_ceiling != 0)
        *old_ceiling = old == 0 ? mutex_default_prioceiling() : old;
    return 0;
}

int pthread_mutex_destroy(pthread_mutex_t *mutex) {
    if (mutex == 0)
        return EINVAL;

    volatile int *words = mutex->__u.__vi;
    int shared = (words[0] & SINOMO_MUTEX_PSHARED) != 0;
    int cross_process = __sinomo_wasix_shared_memory_is_cross_process() != 0;
    if (shared && cross_process) {
        int32_t err = __sinomo_wasix_shared_memory_sync_to();
        if (err != 0)
            return (int)err;
    }
    int owner = words[1] & 0x7fffffff;
    if (shared && !cross_process && owner != 0 && __sinomo_wasix_shared_memory_was_cross_process() != 0) {
        int32_t sync_ret = __sinomo_wasix_shared_memory_sync_to();
        if (sync_ret != 0)
            return (int)sync_ret;
        owner = words[1] & 0x7fffffff;
        if (owner != 0) {
            words[1] = 0;
            words[3] = 0;
            sync_ret = __sinomo_wasix_shared_memory_sync_from();
            if (sync_ret != 0)
                return (int)sync_ret;
            owner = 0;
        }
    }
    if (shared && cross_process && owner != 0) {
        int tid = current_tid();
        int type = words[0] & SINOMO_MUTEX_TYPE_MASK;
        int32_t lock_ret = __sinomo_wasix_shared_mutex_try_lock((uintptr_t)mutex, (uint32_t)type, (uint32_t)tid);
        if (lock_ret == 0) {
            int32_t unlock_ret = __sinomo_wasix_shared_mutex_unlock((uintptr_t)mutex, (uint32_t)type, (uint32_t)tid);
            if (unlock_ret != 0)
                return (int)unlock_ret;
            words[1] = 0;
            words[3] = 0;
            int32_t sync_ret = __sinomo_wasix_shared_memory_sync_to();
            if (sync_ret != 0)
                return (int)sync_ret;
            owner = words[1] & 0x7fffffff;
        } else if (lock_ret == EBUSY) {
            int32_t unlock_ret = __sinomo_wasix_shared_mutex_unlock((uintptr_t)mutex, (uint32_t)type, (uint32_t)tid);
            if (unlock_ret == 0) {
                words[1] = 0;
                words[3] = 0;
                int32_t sync_ret = __sinomo_wasix_shared_memory_sync_to();
                if (sync_ret != 0)
                    return (int)sync_ret;
                owner = words[1] & 0x7fffffff;
            }
        }
    }
    if (owner != 0)
        return EBUSY;

    (void)__sinomo_wasix_shared_futex_unhint((uintptr_t)mutex, (uintptr_t)SINOMO_MUTEX_RUNTIME_BYTES);
    memset(mutex, 0, sizeof(*mutex));
    if (shared && cross_process) {
        int32_t err = __sinomo_wasix_shared_memory_sync_from();
        if (err != 0)
            return (int)err;
    }
    return 0;
}
