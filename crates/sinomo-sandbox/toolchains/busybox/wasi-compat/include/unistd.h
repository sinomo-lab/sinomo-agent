#ifndef SINOMO_WASI_COMPAT_UNISTD_H
#define SINOMO_WASI_COMPAT_UNISTD_H

#include_next <unistd.h>

#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <sys/types.h>
#if defined(__wasi__)
#include <wasi/libc.h>
#endif

static inline uid_t getuid(void)
{
    return 0;
}

static inline uid_t geteuid(void)
{
    return 0;
}

static inline gid_t getgid(void)
{
    return 0;
}

static inline gid_t getegid(void)
{
    return 0;
}

#define getppid() sinomo_wasi_getppid()
static inline pid_t sinomo_wasi_getppid(void)
{
    return 1;
}

static inline int chown(const char *path, uid_t owner, gid_t group)
{
    (void)path;
    (void)owner;
    (void)group;
    errno = ENOTSUP;
    return -1;
}

static inline int fchown(int fd, uid_t owner, gid_t group)
{
    (void)fd;
    (void)owner;
    (void)group;
    errno = ENOTSUP;
    return -1;
}

static inline int lchown(const char *path, uid_t owner, gid_t group)
{
    (void)path;
    (void)owner;
    (void)group;
    errno = ENOTSUP;
    return -1;
}

static inline int setuid(uid_t uid)
{
    (void)uid;
    errno = ENOTSUP;
    return -1;
}

static inline int seteuid(uid_t uid)
{
    (void)uid;
    errno = ENOTSUP;
    return -1;
}

static inline int setgid(gid_t gid)
{
    (void)gid;
    errno = ENOTSUP;
    return -1;
}

static inline int setegid(gid_t gid)
{
    (void)gid;
    errno = ENOTSUP;
    return -1;
}

static inline int getgroups(int size, gid_t list[])
{
    if (size > 0 && list) {
        list[0] = getgid();
    }
    return size == 0 ? 1 : 1;
}

static inline int initgroups(const char *user, gid_t group)
{
    (void)user;
    (void)group;
    return 0;
}

static inline mode_t umask(mode_t mask)
{
    (void)mask;
    return 0;
}

static inline int dup(int oldfd)
{
    (void)oldfd;
    errno = ENOTSUP;
    return -1;
}

static inline int dup2(int oldfd, int newfd)
{
#if defined(__wasi__)
    if (oldfd == newfd) {
        return newfd;
    }
    if (__wasilibc_fd_renumber(oldfd, newfd) == 0) {
        return newfd;
    }
#else
    (void)oldfd;
    (void)newfd;
#endif
    errno = ENOTSUP;
    return -1;
}

static inline int pipe(int filedes[2])
{
    (void)filedes;
    errno = ENOTSUP;
    return -1;
}

static inline int fchdir(int fd)
{
    (void)fd;
    errno = ENOTSUP;
    return -1;
}

static inline int chroot(const char *path)
{
    (void)path;
    errno = ENOTSUP;
    return -1;
}

static inline int ttyname_r(int fd, char *buf, size_t buflen)
{
    (void)fd;
    (void)buf;
    (void)buflen;
    errno = ENOTTY;
    return ENOTTY;
}

static inline pid_t fork(void)
{
    errno = ENOTSUP;
    return -1;
}

static inline pid_t vfork(void)
{
    errno = ENOTSUP;
    return -1;
}

static inline pid_t setsid(void)
{
    errno = ENOTSUP;
    return -1;
}

static inline int execve(const char *pathname, char *const argv[], char *const envp[])
{
    (void)pathname;
    (void)argv;
    (void)envp;
    errno = ENOTSUP;
    return -1;
}

static inline int execv(const char *pathname, char *const argv[])
{
    (void)pathname;
    (void)argv;
    errno = ENOTSUP;
    return -1;
}

static inline int execvp(const char *file, char *const argv[])
{
    (void)file;
    (void)argv;
    errno = ENOTSUP;
    return -1;
}

static inline unsigned int alarm(unsigned int seconds)
{
    (void)seconds;
    return 0;
}

#endif
