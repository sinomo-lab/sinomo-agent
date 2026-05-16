#ifndef SINOMO_WASI_COMPAT_SYS_IOCTL_H
#define SINOMO_WASI_COMPAT_SYS_IOCTL_H

#include <errno.h>

static inline int ioctl(int fd, unsigned long request, void *arg)
{
    (void)fd;
    (void)request;
    (void)arg;
    errno = ENOTTY;
    return -1;
}

#endif
