#ifndef SINOMO_WASI_COMPAT_GRP_H
#define SINOMO_WASI_COMPAT_GRP_H

#include <errno.h>
#include <stddef.h>
#include <sys/types.h>

struct group {
    char *gr_name;
    char *gr_passwd;
    gid_t gr_gid;
    char **gr_mem;
};

static inline struct group *getgrnam(const char *name)
{
    (void)name;
    errno = ENOENT;
    return NULL;
}

static inline struct group *getgrgid(gid_t gid)
{
    (void)gid;
    errno = ENOENT;
    return NULL;
}

static inline int getgrnam_r(
    const char *name,
    struct group *grp,
    char *buf,
    size_t buflen,
    struct group **result)
{
    (void)name;
    (void)grp;
    (void)buf;
    (void)buflen;
    if (result) {
        *result = NULL;
    }
    return ENOENT;
}

static inline int getgrgid_r(
    gid_t gid,
    struct group *grp,
    char *buf,
    size_t buflen,
    struct group **result)
{
    (void)gid;
    (void)grp;
    (void)buf;
    (void)buflen;
    if (result) {
        *result = NULL;
    }
    return ENOENT;
}

static inline void setgrent(void) {}
static inline void endgrent(void) {}

static inline struct group *getgrent(void)
{
    errno = ENOENT;
    return NULL;
}

#endif
