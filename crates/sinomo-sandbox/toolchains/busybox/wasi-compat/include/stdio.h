#ifndef SINOMO_WASI_COMPAT_STDIO_H
#define SINOMO_WASI_COMPAT_STDIO_H

#include_next <stdio.h>

#ifdef SINOMO_BUSYBOX_WASI_COMPAT
#include <errno.h>

static inline FILE *popen(const char *command, const char *mode)
{
    (void)command;
    (void)mode;
    errno = ENOTSUP;
    return NULL;
}

static inline int pclose(FILE *stream)
{
    (void)stream;
    errno = ENOTSUP;
    return -1;
}
#endif

#endif
