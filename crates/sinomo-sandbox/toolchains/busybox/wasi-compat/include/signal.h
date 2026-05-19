#ifndef SINOMO_WASI_COMPAT_SIGNAL_H
#define SINOMO_WASI_COMPAT_SIGNAL_H

#include_next <signal.h>

#include <errno.h>
#include <sys/types.h>

#ifndef SIG_BLOCK
#define SIG_BLOCK 0
#endif
#ifndef SIG_UNBLOCK
#define SIG_UNBLOCK 1
#endif
#ifndef SIG_SETMASK
#define SIG_SETMASK 2
#endif
#ifndef SA_RESTART
#define SA_RESTART 0
#endif

struct sigaction {
    void (*sa_handler)(int);
    sigset_t sa_mask;
    int sa_flags;
};

static inline int sigaction(int signum, const struct sigaction *act, struct sigaction *oldact)
{
    (void)signum;
    (void)act;
    (void)oldact;
    errno = ENOTSUP;
    return -1;
}

static inline int sigfillset(sigset_t *set)
{
    if (set) {
        *set = (sigset_t)-1;
    }
    return 0;
}

static inline int sigemptyset(sigset_t *set)
{
    if (set) {
        *set = (sigset_t)0;
    }
    return 0;
}

static inline int sigaddset(sigset_t *set, int signum)
{
    (void)signum;
    if (set) {
        *set = (sigset_t)-1;
    }
    return 0;
}

static inline int sigprocmask(int how, const sigset_t *set, sigset_t *oldset)
{
    (void)how;
    (void)set;
    if (oldset) {
        *oldset = (sigset_t)0;
    }
    return 0;
}

static inline int sigsuspend(const sigset_t *mask)
{
    (void)mask;
    errno = ENOTSUP;
    return -1;
}

static inline int kill(pid_t pid, int sig)
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("kill")))
    extern int __sinomo_wasix_kill(int pid, int sig);
    int result = __sinomo_wasix_kill((int)pid, sig);
    if (result == 0) {
        return 0;
    }
    errno = -result;
    return -1;
#else
    (void)pid;
    (void)sig;
    errno = ENOTSUP;
    return -1;
#endif
}

static inline int raise(int sig)
{
#if defined(SINOMO_BUSYBOX_WASIX)
    __attribute__((import_module("wasix_32v1"), import_name("raise")))
    extern int __sinomo_wasix_raise(int sig);
    int result = __sinomo_wasix_raise(sig);
    if (result == 0) {
        return 0;
    }
    errno = -result;
    return -1;
#else
    (void)sig;
    errno = ENOTSUP;
    return -1;
#endif
}

#endif
