#ifndef SINOMO_WASI_COMPAT_SYS_STAT_H
#define SINOMO_WASI_COMPAT_SYS_STAT_H

#include_next <sys/stat.h>

#include <errno.h>
#include <sys/types.h>

static inline int mknod(const char *path, mode_t mode, dev_t dev)
{
    (void)path;
    (void)mode;
    (void)dev;
    errno = ENOTSUP;
    return -1;
}

#endif
