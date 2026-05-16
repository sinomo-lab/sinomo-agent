#ifndef SINOMO_BUSYBOX_WASI_COMPAT_TIME_H
#define SINOMO_BUSYBOX_WASI_COMPAT_TIME_H

#include_next <time.h>
#include <errno.h>

static inline int clock_settime(clockid_t clock_id, const struct timespec *tp) {
    (void)clock_id;
    (void)tp;
    errno = ENOSYS;
    return -1;
}

#endif
