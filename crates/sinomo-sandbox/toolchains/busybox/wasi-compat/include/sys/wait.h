#ifndef SINOMO_BUSYBOX_WASI_COMPAT_SYS_WAIT_H
#define SINOMO_BUSYBOX_WASI_COMPAT_SYS_WAIT_H

typedef int pid_t;

#define WNOHANG 1
#define WUNTRACED 2
#define WEXITSTATUS(status) (((status) >> 8) & 0xff)
#define WIFEXITED(status) (1)
#define WIFSIGNALED(status) (0)
#define WIFSTOPPED(status) (0)
#define WCOREDUMP(status) (0)
#define WTERMSIG(status) (0)
#define WSTOPSIG(status) (0)

static inline pid_t waitpid(pid_t pid, int *status, int options) {
    (void)pid;
    (void)options;
    if (status) {
        *status = 127 << 8;
    }
    return -1;
}

static inline pid_t wait(int *status) {
    return waitpid(-1, status, 0);
}

#endif
