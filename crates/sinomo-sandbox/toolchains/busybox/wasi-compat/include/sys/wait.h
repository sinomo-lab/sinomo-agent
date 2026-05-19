#ifndef SINOMO_BUSYBOX_WASI_COMPAT_SYS_WAIT_H
#define SINOMO_BUSYBOX_WASI_COMPAT_SYS_WAIT_H

#include <errno.h>

typedef int pid_t;

#define WNOHANG 1
#define WUNTRACED 2
#define WCONTINUED 8
#define WEXITSTATUS(status) (((status) & 0xff00) >> 8)
#define WTERMSIG(status) ((status) & 0x7f)
#define WSTOPSIG(status) WEXITSTATUS(status)
#define WIFEXITED(status) (WTERMSIG(status) == 0)
#define WIFSIGNALED(status) (WTERMSIG(status) != 0 && WTERMSIG(status) != 0x7f)
#define WIFSTOPPED(status) (((status) & 0xff) == 0x7f)
#define WIFCONTINUED(status) ((status) == 0xffff)
#define WCOREDUMP(status) (0)

static inline pid_t waitpid(pid_t pid, int *status, int options) {
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("waitpid")))
    extern int __sinomo_wasix_waitpid(int pid, int *status, int options);
    int result = __sinomo_wasix_waitpid((int)pid, status, options);
    if (result >= 0) {
        return (pid_t)result;
    }
    errno = -result;
    return (pid_t)-1;
#else
    (void)pid;
    (void)options;
    if (status) {
        *status = 127 << 8;
    }
    return -1;
#endif
}

static inline pid_t wait(int *status) {
    return waitpid(-1, status, 0);
}

#endif
