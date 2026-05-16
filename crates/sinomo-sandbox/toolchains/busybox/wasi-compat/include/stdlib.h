#ifndef SINOMO_WASI_COMPAT_STDLIB_H
#define SINOMO_WASI_COMPAT_STDLIB_H

#include_next <stdlib.h>

#include <errno.h>

#define system(command) sinomo_wasi_system(command)

static inline int mkstemp(char *template)
{
    (void)template;
    errno = ENOTSUP;
    return -1;
}

static inline int sinomo_wasi_system(const char *command)
{
    (void)command;
    errno = ENOTSUP;
    return -1;
}

#endif
