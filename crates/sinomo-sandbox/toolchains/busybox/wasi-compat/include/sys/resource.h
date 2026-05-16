#ifndef SINOMO_BUSYBOX_WASI_COMPAT_SYS_RESOURCE_H
#define SINOMO_BUSYBOX_WASI_COMPAT_SYS_RESOURCE_H

#include_next <sys/resource.h>

#ifndef RLIM_INFINITY
typedef unsigned long long rlim_t;

struct rlimit {
    rlim_t rlim_cur;
    rlim_t rlim_max;
};

#define RLIM_INFINITY (~0ULL)
#define RLIMIT_CPU 0
#define RLIMIT_FSIZE 1
#define RLIMIT_DATA 2
#define RLIMIT_STACK 3
#define RLIMIT_CORE 4
#define RLIMIT_RSS 5
#define RLIMIT_NPROC 6
#define RLIMIT_NOFILE 7
#define RLIMIT_MEMLOCK 8
#define RLIMIT_AS 9
#define RLIMIT_LOCKS 10
#define RLIMIT_SIGPENDING 11
#define RLIMIT_MSGQUEUE 12
#define RLIMIT_NICE 13
#define RLIMIT_RTPRIO 14
#define RLIMIT_RTTIME 15
#define RLIMIT_NLIMITS 16
#endif

static inline int sinomo_wasi_getrlimit(int resource, struct rlimit *limit)
{
    (void)resource;
    if (limit) {
        limit->rlim_cur = RLIM_INFINITY;
        limit->rlim_max = RLIM_INFINITY;
    }
    return 0;
}

static inline int sinomo_wasi_setrlimit(int resource, const struct rlimit *limit)
{
    (void)resource;
    (void)limit;
    return 0;
}

#define getrlimit(resource, limit) sinomo_wasi_getrlimit((resource), (limit))
#define setrlimit(resource, limit) sinomo_wasi_setrlimit((resource), (limit))

#endif
