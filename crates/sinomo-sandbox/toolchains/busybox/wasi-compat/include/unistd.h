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
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("getppid")))
    extern int __sinomo_wasix_getppid(void);
    int result = __sinomo_wasix_getppid();
    if (result >= 0) {
        return (pid_t)result;
    }
    errno = -result;
    return -1;
#else
    return 1;
#endif
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
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("dup")))
    extern int __sinomo_wasix_dup(int oldfd);
    int result = __sinomo_wasix_dup(oldfd);
    if (result >= 0) {
        return result;
    }
    errno = -result;
    return -1;
#else
    (void)oldfd;
    errno = ENOTSUP;
    return -1;
#endif
}

static inline int dup2(int oldfd, int newfd)
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("dup2")))
    extern int __sinomo_wasix_dup2(int oldfd, int newfd);
    int result = __sinomo_wasix_dup2(oldfd, newfd);
    if (result >= 0) {
        return result;
    }
    errno = -result;
    return -1;
#elif defined(__wasi__)
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
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("pipe")))
    extern int __sinomo_wasix_pipe(int filedes[2]);
    int result = __sinomo_wasix_pipe(filedes);
    if (result == 0) {
        return 0;
    }
    errno = -result;
    return -1;
#else
    (void)filedes;
    errno = ENOTSUP;
    return -1;
#endif
}

static inline int fchdir(int fd)
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("fchdir")))
    extern int __sinomo_wasix_fchdir(int fd);
    int result = __sinomo_wasix_fchdir(fd);
    if (result == 0) {
        return 0;
    }
    errno = -result;
    return -1;
#else
    (void)fd;
    errno = ENOTSUP;
    return -1;
#endif
}

static inline int chdir(const char *path)
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("chdir")))
    extern int __sinomo_wasix_chdir(const char *path);
    int result = __sinomo_wasix_chdir(path);
    if (result == 0) {
        return 0;
    }
    errno = -result;
    return -1;
#else
    (void)path;
    errno = ENOTSUP;
    return -1;
#endif
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
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("fork")))
    extern int __sinomo_wasix_fork(void);
    int result = __sinomo_wasix_fork();
    if (result >= 0) {
        return (pid_t)result;
    }
    errno = -result;
    return (pid_t)-1;
#else
    errno = ENOTSUP;
    return (pid_t)-1;
#endif
}

static inline pid_t vfork(void)
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("vfork")))
    extern int __sinomo_wasix_vfork(void);
    int result = __sinomo_wasix_vfork();
    if (result >= 0) {
        return (pid_t)result;
    }
    errno = -result;
    return (pid_t)-1;
#else
    errno = ENOTSUP;
    return (pid_t)-1;
#endif
}

static inline pid_t setsid(void)
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("setsid")))
    extern int __sinomo_wasix_setsid(void);
    int result = __sinomo_wasix_setsid();
    if (result >= 0) {
        return (pid_t)result;
    }
    errno = -result;
    return (pid_t)-1;
#else
    errno = ENOTSUP;
    return (pid_t)-1;
#endif
}

static inline pid_t getpgrp(void)
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("getpgrp")))
    extern int __sinomo_wasix_getpgrp(void);
    int result = __sinomo_wasix_getpgrp();
    if (result >= 0) {
        return (pid_t)result;
    }
    errno = -result;
    return (pid_t)-1;
#else
    return getpid();
#endif
}

static inline pid_t getpgid(pid_t pid)
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("getpgid")))
    extern int __sinomo_wasix_getpgid(int pid);
    int result = __sinomo_wasix_getpgid((int)pid);
    if (result >= 0) {
        return (pid_t)result;
    }
    errno = -result;
    return (pid_t)-1;
#else
    return pid == 0 ? getpid() : pid;
#endif
}

static inline pid_t getsid(pid_t pid)
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("getsid")))
    extern int __sinomo_wasix_getsid(int pid);
    int result = __sinomo_wasix_getsid((int)pid);
    if (result >= 0) {
        return (pid_t)result;
    }
    errno = -result;
    return (pid_t)-1;
#else
    return pid == 0 ? getpid() : pid;
#endif
}

static inline int setpgid(pid_t pid, pid_t pgid)
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("setpgid")))
    extern int __sinomo_wasix_setpgid(int pid, int pgid);
    int result = __sinomo_wasix_setpgid((int)pid, (int)pgid);
    if (result == 0) {
        return 0;
    }
    errno = -result;
    return -1;
#else
    (void)pid;
    (void)pgid;
    errno = ENOTSUP;
    return -1;
#endif
}

static inline pid_t tcgetpgrp(int fd)
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("tcgetpgrp")))
    extern int __sinomo_wasix_tcgetpgrp(int fd);
    int result = __sinomo_wasix_tcgetpgrp(fd);
    if (result >= 0) {
        return (pid_t)result;
    }
    errno = -result;
    return (pid_t)-1;
#else
    (void)fd;
    errno = ENOTTY;
    return (pid_t)-1;
#endif
}

static inline pid_t tcgetsid(int fd)
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("tcgetsid")))
    extern int __sinomo_wasix_tcgetsid(int fd);
    int result = __sinomo_wasix_tcgetsid(fd);
    if (result >= 0) {
        return (pid_t)result;
    }
    errno = -result;
    return (pid_t)-1;
#else
    (void)fd;
    errno = ENOTTY;
    return (pid_t)-1;
#endif
}

static inline int tcsetpgrp(int fd, pid_t pgid)
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("tcsetpgrp")))
    extern int __sinomo_wasix_tcsetpgrp(int fd, int pgid);
    int result = __sinomo_wasix_tcsetpgrp(fd, (int)pgid);
    if (result == 0) {
        return 0;
    }
    errno = -result;
    return -1;
#else
    (void)fd;
    (void)pgid;
    errno = ENOTTY;
    return -1;
#endif
}

static inline int execve(const char *pathname, char *const argv[], char *const envp[])
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("execve")))
    extern int __sinomo_wasix_execve(const char *pathname, char *const argv[], char *const envp[]);
    int result = __sinomo_wasix_execve(pathname, argv, envp);
    if (result == 0) {
        return 0;
    }
    errno = -result;
    return -1;
#else
    (void)pathname;
    (void)argv;
    (void)envp;
    errno = ENOTSUP;
    return -1;
#endif
}

static inline int execv(const char *pathname, char *const argv[])
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("execv")))
    extern int __sinomo_wasix_execv(const char *pathname, char *const argv[]);
    int result = __sinomo_wasix_execv(pathname, argv);
    if (result == 0) {
        return 0;
    }
    errno = -result;
    return -1;
#else
    (void)pathname;
    (void)argv;
    errno = ENOTSUP;
    return -1;
#endif
}

static inline int execvp(const char *file, char *const argv[])
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("execvp")))
    extern int __sinomo_wasix_execvp(const char *file, char *const argv[]);
    int result = __sinomo_wasix_execvp(file, argv);
    if (result == 0) {
        return 0;
    }
    errno = -result;
    return -1;
#else
    (void)file;
    (void)argv;
    errno = ENOTSUP;
    return -1;
#endif
}

static inline unsigned int alarm(unsigned int seconds)
{
    (void)seconds;
    return 0;
}

#endif
