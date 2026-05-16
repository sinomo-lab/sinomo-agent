#ifndef SINOMO_BUSYBOX_WASI_COMPAT_SETJMP_H
#define SINOMO_BUSYBOX_WASI_COMPAT_SETJMP_H

#include <stdlib.h>

typedef int jmp_buf[1];
typedef int sigjmp_buf[1];

#define setjmp(env) ((void)(env), 0)
#define sigsetjmp(env, savesigs) ((void)(env), (void)(savesigs), 0)

static inline void longjmp(jmp_buf env, int value) {
    (void)env;
    (void)value;
    abort();
}

static inline void siglongjmp(sigjmp_buf env, int value) {
    (void)env;
    (void)value;
    abort();
}

#endif
