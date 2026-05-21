#include <pthread.h>
#include <stdint.h>
#include <threads.h>
#include <wasi/api.h>

struct sinomo_wasix_pthread_prefix {
    struct sinomo_wasix_pthread_prefix *self;
    struct sinomo_wasix_pthread_prefix *prev;
    struct sinomo_wasix_pthread_prefix *next;
    uintptr_t sysinfo;
    uintptr_t canary;
    int tid;
};

_Thread_local struct sinomo_wasix_pthread_prefix *__wasilibc_pthread_self;

static pthread_t sinomo_wasix_pthread_self_internal(void) {
    return (pthread_t)__wasilibc_pthread_self;
}

static void __attribute__((constructor)) sinomo_wasix_init_main_thread_tid(void) {
    if (__wasilibc_pthread_self == 0)
        return;
    if (__wasilibc_pthread_self->tid != 0x3fffffff)
        return;

    __wasi_tid_t tid = 0;
    if (__wasi_thread_id(&tid) == 0 && tid > 0 && tid < 0x3fffffff)
        __wasilibc_pthread_self->tid = tid;
}

__attribute__((weak, alias("sinomo_wasix_pthread_self_internal")))
pthread_t pthread_self(void);

__attribute__((weak, alias("sinomo_wasix_pthread_self_internal")))
thrd_t thrd_current(void);
