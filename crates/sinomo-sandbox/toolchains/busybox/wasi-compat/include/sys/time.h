#ifndef SINOMO_WASI_COMPAT_SYS_TIME_H
#define SINOMO_WASI_COMPAT_SYS_TIME_H

#include_next <sys/time.h>

#include <errno.h>

static inline int settimeofday(const struct timeval *tv, const struct timezone *tz)
{
    (void)tv;
    (void)tz;
    errno = ENOTSUP;
    return -1;
}

#endif
