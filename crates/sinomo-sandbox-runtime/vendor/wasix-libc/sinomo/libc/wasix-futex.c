#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <time.h>
#include <wasi/api.h>

struct sinomo_wasix_pthread_cancel_prefix {
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

extern _Thread_local struct sinomo_wasix_pthread_cancel_prefix *__wasilibc_pthread_self;

static int current_thread_cancel_pending(void) {
    struct sinomo_wasix_pthread_cancel_prefix *self = __wasilibc_pthread_self;
    if (self == 0)
        return 0;
    if (!__atomic_load_n(&self->cancel, __ATOMIC_SEQ_CST))
        return 0;
    return self->canceldisable != PTHREAD_CANCEL_DISABLE;
}

static int64_t monotonic_now_ns(void) {
    __wasi_timestamp_t now = 0;
    if (__wasi_clock_time_get(__WASI_CLOCKID_MONOTONIC, 1, &now) != 0)
        return 0;
    if (now > (uint64_t)INT64_MAX)
        return INT64_MAX;
    return (int64_t)now;
}

static int wait_unaligned_futex(volatile int *addr, int expected, int64_t max_wait_ns) {
    const __wasi_timestamp_t sleep_ns = 1000000;
    int64_t deadline = 0;
    if (max_wait_ns > 0)
        deadline = monotonic_now_ns() + max_wait_ns;

    while (*addr == expected) {
        if (current_thread_cancel_pending())
            return -ECANCELED;
        if (max_wait_ns == 0)
            return -ETIMEDOUT;
        if (max_wait_ns > 0 && monotonic_now_ns() >= deadline)
            return -ETIMEDOUT;
        (void)__wasi_thread_sleep(sleep_ns);
    }
    if (current_thread_cancel_pending())
        return -ECANCELED;
    return 0;
}

int __wasilibc_futex_wait_wasix(volatile void *addr, int op, int expected, int64_t max_wait_ns) {
    (void)op;
    if (addr == 0)
        return -EINVAL;

    if (current_thread_cancel_pending())
        return -ECANCELED;

    if ((((uintptr_t)addr) & 3) != 0)
        return wait_unaligned_futex((volatile int *)addr, expected, max_wait_ns);

    __wasi_option_timestamp_t timeout;
    __wasi_option_timestamp_t *timeout_ptr = 0;
    if (max_wait_ns == 0)
        return -ETIMEDOUT;
    if (max_wait_ns > 0) {
        timeout.tag = __WASI_OPTION_SOME;
        timeout.u.some = (uint64_t)max_wait_ns;
        timeout_ptr = &timeout;
    }

    if (*(volatile int *)addr != expected)
        return -EAGAIN;
    if (current_thread_cancel_pending())
        return -ECANCELED;

    __wasi_bool_t woken = 0;
    __wasi_errno_t error = __wasi_futex_wait((uint32_t *)addr, (uint32_t)expected, timeout_ptr, &woken);
    if (error == __WASI_ERRNO_INTR && current_thread_cancel_pending())
        return -ECANCELED;
    if (error != 0)
        return -(int)error;
    if (current_thread_cancel_pending())
        return -ECANCELED;
    if (woken)
        return 0;
    if (*(volatile int *)addr != expected)
        return 0;
    if (max_wait_ns > 0)
        return -ETIMEDOUT;
    return 0;
}

int __wasilibc_futex_wake_wasix(int *futex, int cnt) {
    if (futex == 0)
        return -EINVAL;
    if ((((uintptr_t)futex) & 3) != 0)
        return 0;
    if (cnt < 1)
        return -EINVAL;

    __wasi_bool_t woken = 0;
    if (cnt == INT32_MAX) {
        __wasi_errno_t error = __wasi_futex_wake_all((uint32_t *)futex, &woken);
        return error == 0 ? 0 : -(int)error;
    }

    for (int i = 0; i < cnt; i++) {
        __wasi_errno_t error = __wasi_futex_wake((uint32_t *)futex, &woken);
        if (error != 0)
            return -(int)error;
        if (!woken)
            break;
    }
    return 0;
}
