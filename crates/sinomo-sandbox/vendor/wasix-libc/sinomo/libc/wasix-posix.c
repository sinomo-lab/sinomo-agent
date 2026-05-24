//! POSIX-like functions supporting absolute path arguments, implemented in
//! terms of `__wasilibc_find_relpath` and `*at`-style functions.

#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

#include <stdarg.h>
#include <stdint.h>
#include <errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <grp.h>
#include <limits.h>
#include <mqueue.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/times.h>
#include <time.h>
#include <unistd.h>
#include <utime.h>
#include <wasi/api.h>
#include <wasi/api_wasix.h>
#include <wasi/libc.h>
#include <wasi/libc-find-relpath.h>
#include <wasi/libc-nocwd.h>

#define SINOMO_STDIO_MAYBE_WAITERS 0x40000000

struct sinomo_wasix_file_lock_view {
    unsigned flags;
    unsigned char *rpos;
    unsigned char *rend;
    int (*close)(FILE *);
    unsigned char *wend;
    unsigned char *wpos;
    unsigned char *wbase;
    size_t (*read)(FILE *, unsigned char *, size_t);
    size_t (*write)(FILE *, const unsigned char *, size_t);
    off_t (*seek)(FILE *, off_t, int);
    unsigned char *buf;
    size_t buf_size;
    FILE *prev;
    FILE *next;
    int fd;
    int pipe_pid;
    long lockcount;
    int mode;
    volatile int lock;
    int lbf;
    void *cookie;
    off_t off;
    char *getln_buf;
    unsigned char *shend;
    off_t shlim;
    off_t shcnt;
    FILE *prev_locked;
    FILE *next_locked;
};

#ifndef SINOMO_PTHREAD_DEBUG
#define SINOMO_PTHREAD_DEBUG 0
#endif
#define SINOMO_PTHREAD_LOG(...) do { if (SINOMO_PTHREAD_DEBUG) fprintf(stderr, __VA_ARGS__); } while (0)
#define SINOMO_PTHREAD_ENV_LOG(...) do { if (getenv("SINOMO_PTHREAD_LOG") != NULL) fprintf(stderr, __VA_ARGS__); } while (0)

#ifndef SIGCANCEL
#define SIGCANCEL 32
#endif
#ifndef WNOHANG
#define WNOHANG 1
#endif
#ifndef WUNTRACED
#define WUNTRACED 2
#endif

#define SINOMO_WASIX_DEFAULT_THREAD_STACK_SIZE (128u * 1024u)

struct sinomo_wasix_atfork_handler {
    void (*prepare)(void);
    void (*parent)(void);
    void (*child)(void);
};

static struct sinomo_wasix_atfork_handler *sinomo_wasix_atfork_handlers;
static size_t sinomo_wasix_atfork_count;
static size_t sinomo_wasix_atfork_capacity;
static volatile int sinomo_wasix_atfork_lock;

static void sinomo_wasix_atfork_lock_acquire(void) {
    while (__atomic_exchange_n(&sinomo_wasix_atfork_lock, 1, __ATOMIC_ACQUIRE) != 0)
        sched_yield();
}

static void sinomo_wasix_atfork_lock_release(void) {
    __atomic_store_n(&sinomo_wasix_atfork_lock, 0, __ATOMIC_RELEASE);
}

int pthread_atfork(void (*prepare)(void), void (*parent)(void), void (*child)(void)) {
    sched_yield();
    sinomo_wasix_atfork_lock_acquire();
    if (sinomo_wasix_atfork_count == sinomo_wasix_atfork_capacity) {
        size_t new_capacity = sinomo_wasix_atfork_capacity == 0 ? 16 : sinomo_wasix_atfork_capacity * 2;
        if (new_capacity <= sinomo_wasix_atfork_capacity) {
            sinomo_wasix_atfork_lock_release();
            return ENOMEM;
        }
        struct sinomo_wasix_atfork_handler *new_handlers =
            realloc(sinomo_wasix_atfork_handlers, new_capacity * sizeof(*new_handlers));
        if (new_handlers == NULL) {
            sinomo_wasix_atfork_lock_release();
            return ENOMEM;
        }
        sinomo_wasix_atfork_handlers = new_handlers;
        sinomo_wasix_atfork_capacity = new_capacity;
    }
    sinomo_wasix_atfork_handlers[sinomo_wasix_atfork_count++] = (struct sinomo_wasix_atfork_handler){
        .prepare = prepare,
        .parent = parent,
        .child = child,
    };
    sinomo_wasix_atfork_lock_release();
    return 0;
}

void __fork_handler(int who) {
    sinomo_wasix_atfork_lock_acquire();
    size_t count = sinomo_wasix_atfork_count;
    if (who < 0) {
        for (size_t i = count; i > 0; i--) {
            void (*handler)(void) = sinomo_wasix_atfork_handlers[i - 1].prepare;
            if (handler != NULL)
                handler();
        }
    } else {
        for (size_t i = 0; i < count; i++) {
            void (*handler)(void) = who ? sinomo_wasix_atfork_handlers[i].child : sinomo_wasix_atfork_handlers[i].parent;
            if (handler != NULL)
                handler();
        }
    }
    sinomo_wasix_atfork_lock_release();
}

int32_t __imported_wasix_fd_chmod(int32_t fd, uint32_t mode) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("fd_chmod")
));

int32_t __imported_wasix_path_chmod_at(int32_t fd, const char *path, int32_t path_len, uint32_t mode) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_chmod_at")
));

int32_t __imported_wasix_fd_chown(int32_t fd, uint32_t uid, uint32_t gid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("fd_chown")
));

int32_t __imported_wasix_path_chown_at(int32_t fd, const char *path, int32_t path_len, uint32_t uid, uint32_t gid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_chown_at")
));

int32_t __imported_wasix_path_touch_at(int32_t fd, const char *path, int32_t path_len, uint32_t touch_self, uint32_t touch_parent) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_touch_at")
));

int32_t __imported_wasix_fd_touch(int32_t fd) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("fd_touch")
));

int32_t __imported_wasix_path_symlink_at(int32_t fd, const char *path, int32_t path_len, const char *target, int32_t target_len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_symlink_at")
));

int32_t __imported_wasix_path_read_symlink_at(int32_t fd, const char *path, int32_t path_len, char *buf, int32_t buf_len, uint32_t *used) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_read_symlink_at")
));

int32_t __imported_wasix_path_unlink_symlink_at(int32_t fd, const char *path, int32_t path_len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_unlink_symlink_at")
));

int32_t __imported_wasix_proc_getresuid(uint32_t *ruid, uint32_t *euid, uint32_t *suid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("proc_getresuid")
));

int32_t __imported_wasix_proc_getresgid(uint32_t *rgid, uint32_t *egid, uint32_t *sgid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("proc_getresgid")
));

int32_t __imported_wasix_proc_setresuid(uint32_t ruid, uint32_t euid, uint32_t suid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("proc_setresuid")
));

int32_t __imported_wasix_proc_setresgid(uint32_t rgid, uint32_t egid, uint32_t sgid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("proc_setresgid")
));

int32_t __imported_wasix_proc_getgroups(uint32_t *groups, uint32_t max_groups, uint32_t *count) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("proc_getgroups")
));

int32_t __imported_wasix_proc_setgroups(const uint32_t *groups, uint32_t count) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("proc_setgroups")
));

int32_t __imported_wasix_chdir(const char *path, int32_t path_len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("chdir")
));

int32_t __imported_wasix_fchdir(int32_t fd) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("fchdir")
));

int32_t __imported_wasix_sched_get_priority_max(int32_t policy) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("sched_get_priority_max")
));

int32_t __imported_wasix_sched_get_priority_min(int32_t policy) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("sched_get_priority_min")
));

int32_t __imported_wasix_sched_getscheduler(int32_t pid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("sched_getscheduler")
));

int32_t __imported_wasix_sched_getparam(int32_t pid, int32_t *priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("sched_getparam")
));

int32_t __imported_wasix_sched_setscheduler(int32_t pid, int32_t policy, int32_t priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("sched_setscheduler")
));

int32_t __imported_wasix_sched_getcpu(void) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("sched_getcpu")
));

int32_t __imported_wasix_sched_getaffinity(int32_t pid, uint64_t *mask_low, uint64_t *mask_high) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("sched_getaffinity")
));

int32_t __imported_wasix_sched_setaffinity(int32_t pid, uint64_t mask_low, uint64_t mask_high) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("sched_setaffinity")
));

int32_t __imported_wasix_sched_rr_get_interval(int32_t pid, struct timespec *interval) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("sched_rr_get_interval")
));

int32_t __imported_wasix_getpriority(int32_t which, int32_t who) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("getpriority")
));

int32_t __imported_wasix_setpriority(int32_t which, int32_t who, int32_t priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("setpriority")
));

int32_t __imported_wasix_nice(int32_t increment) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("nice")
));

int32_t __imported_wasix_getrlimit(int32_t resource, uint64_t *current, uint64_t *maximum) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("getrlimit")
));

int32_t __imported_wasix_setrlimit(int32_t resource, uint64_t current, uint64_t maximum) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("setrlimit")
));

int32_t __imported_wasix_proc_times(
    uint64_t *elapsed_ticks,
    uint64_t *user_ticks,
    uint64_t *system_ticks,
    uint64_t *child_user_ticks,
    uint64_t *child_system_ticks
) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("proc_times")
));

int32_t __imported_wasix_signal_mask_set(uint64_t low, uint64_t high) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("signal_mask_set")
));

int32_t __imported_wasix_signal_mask_get(uint64_t *low, uint64_t *high) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("signal_mask_get")
));

int32_t __imported_wasix_signal_pending_get(uint64_t *low, uint64_t *high) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("signal_pending_get")
));

int32_t __imported_wasix_signal_pending_take(uint64_t low, uint64_t high) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("signal_pending_take")
));

int32_t __imported_wasix_thread_signal(uint32_t tid, uint32_t sig) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("thread_signal")
));

int32_t __imported_wasix_thread_sleep(uint64_t duration) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("thread_sleep")
));

int32_t __imported_wasix_thread_join(uint32_t tid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("thread_join")
));

int32_t __imported_wasix_itimer_settime(
    int32_t which,
    int64_t value_sec,
    int64_t value_usec,
    int64_t interval_sec,
    int64_t interval_usec,
    int64_t *old_value_sec,
    int64_t *old_value_usec,
    int64_t *old_interval_sec,
    int64_t *old_interval_usec
) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("itimer_settime")
));

int32_t __imported_wasix_itimer_gettime(
    int32_t which,
    int64_t *value_sec,
    int64_t *value_usec,
    int64_t *interval_sec,
    int64_t *interval_usec
) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("itimer_gettime")
));

int32_t __imported_wasix_posix_timer_create(int32_t clock_id) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("posix_timer_create")
));

int32_t __imported_wasix_posix_timer_settime(
    uint32_t timer_id,
    int32_t flags,
    int64_t value_sec,
    int64_t value_nsec,
    int64_t interval_sec,
    int64_t interval_nsec,
    int64_t *old_value_sec,
    int64_t *old_value_nsec,
    int64_t *old_interval_sec,
    int64_t *old_interval_nsec
) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("posix_timer_settime")
));

int32_t __imported_wasix_posix_timer_gettime(
    uint32_t timer_id,
    int64_t *value_sec,
    int64_t *value_nsec,
    int64_t *interval_sec,
    int64_t *interval_nsec
) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("posix_timer_gettime")
));

int32_t __imported_wasix_posix_timer_getoverrun(uint32_t timer_id) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("posix_timer_getoverrun")
));

int32_t __imported_wasix_posix_timer_delete(uint32_t timer_id) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("posix_timer_delete")
));

int32_t __imported_wasix_mqueue_open(
    const char *name,
    uintptr_t name_len,
    int32_t flags,
    int32_t create,
    int32_t exclusive,
    int32_t has_attr,
    uint64_t max_messages,
    uint64_t message_size
) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("mqueue_open")
));

int32_t __imported_wasix_mqueue_send(int32_t descriptor, const char *message, uintptr_t message_len, uint32_t priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("mqueue_send")
));

int32_t __imported_wasix_mqueue_timedsend(int32_t descriptor, const char *message, uintptr_t message_len, uint32_t priority, int64_t timeout_sec, int64_t timeout_nsec) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("mqueue_timedsend")
));

int32_t __imported_wasix_mqueue_receive(int32_t descriptor, char *buffer, uintptr_t buffer_len, uint32_t *priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("mqueue_receive")
));

int32_t __imported_wasix_mqueue_timedreceive(int32_t descriptor, char *buffer, uintptr_t buffer_len, uint32_t *priority, int64_t timeout_sec, int64_t timeout_nsec) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("mqueue_timedreceive")
));

int32_t __imported_wasix_mqueue_getattr(
    int32_t descriptor,
    int64_t *flags,
    int64_t *max_messages,
    int64_t *message_size,
    int64_t *current_messages
) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("mqueue_getattr")
));

int32_t __imported_wasix_mqueue_setattr(int32_t descriptor, int64_t flags) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("mqueue_setattr")
));

int32_t __imported_wasix_mqueue_close(int32_t descriptor) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("mqueue_close")
));

int32_t __imported_wasix_mqueue_notify(int32_t descriptor, int32_t has_notification, uint32_t signal) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("mqueue_notify")
));

int32_t __imported_wasix_mqueue_unlink(const char *name, uintptr_t name_len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("mqueue_unlink")
));

int32_t __sinomo_wasix_shared_memory_sync_from(void) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_memory_sync_from")
));
int32_t __sinomo_wasix_shared_memory_sync_from_preserving_futexes(void) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_memory_sync_from_preserving_futexes")
));
int32_t __sinomo_wasix_shared_memory_sync_to(void) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_memory_sync_to")
));
int32_t __sinomo_wasix_shared_memory_range_sync_from(uintptr_t offset, uintptr_t len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_memory_range_sync_from")
));
int32_t __sinomo_wasix_shared_memory_range_sync_to(uintptr_t offset, uintptr_t len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_memory_range_sync_to")
));
int32_t __sinomo_wasix_shared_futex_hint(uintptr_t offset, uintptr_t len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_futex_hint")
));
int32_t __sinomo_wasix_shared_futex_unhint(uintptr_t offset, uintptr_t len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_futex_unhint")
));
uint32_t __sinomo_wasix_shared_memory_is_cross_process(void) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_memory_is_cross_process")
));
uint32_t __sinomo_wasix_shared_memory_was_cross_process(void) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_memory_was_cross_process")
));
uint32_t __sinomo_wasix_shared_memory_has_direct_mapping(uintptr_t offset, uintptr_t len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_memory_has_direct_mapping")
));

static void sinomo_wasix_sync_from_guest_if_shared(void) {
    if (__sinomo_wasix_shared_memory_is_cross_process() != 0)
        (void)__sinomo_wasix_shared_memory_sync_from();
}

static void sinomo_wasix_sync_to_guest_if_shared(void) {
    if (__sinomo_wasix_shared_memory_is_cross_process() != 0 ||
        __sinomo_wasix_shared_memory_was_cross_process() != 0)
        (void)__sinomo_wasix_shared_memory_sync_to();
}

static void sinomo_wasix_sync_range_from_guest_if_shared(const void *ptr, size_t len) {
    if (__sinomo_wasix_shared_memory_has_direct_mapping((uintptr_t)ptr, (uintptr_t)len) != 0)
        return;
    if (__sinomo_wasix_shared_memory_is_cross_process() != 0)
        (void)__sinomo_wasix_shared_memory_range_sync_from((uintptr_t)ptr, (uintptr_t)len);
}

static void sinomo_wasix_sync_range_to_guest_if_shared(const void *ptr, size_t len) {
    if (__sinomo_wasix_shared_memory_has_direct_mapping((uintptr_t)ptr, (uintptr_t)len) != 0)
        return;
    if (__sinomo_wasix_shared_memory_is_cross_process() != 0)
        (void)__sinomo_wasix_shared_memory_range_sync_to((uintptr_t)ptr, (uintptr_t)len);
}
uint32_t __sinomo_wasix_shared_barrier_generation(uintptr_t offset) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_barrier_generation")
));
int32_t __sinomo_wasix_shared_barrier_arrive(uintptr_t offset, uint32_t limit) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_barrier_arrive")
));
int32_t __sinomo_wasix_shared_barrier_release(uintptr_t offset) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_barrier_release")
));
int32_t __sinomo_wasix_shared_rwlock_try_rdlock(uintptr_t offset, int32_t policy, int32_t priority, uint32_t registered_waiter) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_try_rdlock")
));
int32_t __sinomo_wasix_shared_rwlock_try_wrlock(uintptr_t offset, uint32_t registered_waiter, int32_t policy, int32_t priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_try_wrlock")
));
int32_t __sinomo_wasix_shared_rwlock_register_writer(uintptr_t offset, int32_t policy, int32_t priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_register_writer")
));
int32_t __sinomo_wasix_shared_rwlock_unregister_writer(uintptr_t offset, int32_t policy, int32_t priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_unregister_writer")
));
int32_t __sinomo_wasix_shared_rwlock_register_reader(uintptr_t offset, int32_t policy, int32_t priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_register_reader")
));
int32_t __sinomo_wasix_shared_rwlock_unregister_reader(uintptr_t offset, int32_t policy, int32_t priority) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_unregister_reader")
));
int32_t __sinomo_wasix_shared_rwlock_unlock(uintptr_t offset) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_unlock")
));
int32_t __sinomo_wasix_shared_rwlock_wait(uintptr_t offset, int64_t timeout_ns) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_wait")
));
int32_t __sinomo_wasix_shared_rwlock_destroy(uintptr_t offset) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("shared_rwlock_destroy")
));

int __wasilibc_futex_wait_wasix(volatile void *addr, int op, int expected, int64_t max_wait_ns);
int __wasilibc_futex_wake_wasix(int *futex, int cnt);
int munmap(void *addr, size_t len);

static mode_t sinomo_wasix_umask = 0;
static uid_t sinomo_wasix_ruid = 0;
static uid_t sinomo_wasix_euid = 0;
static uid_t sinomo_wasix_suid = 0;
static gid_t sinomo_wasix_rgid = 0;
static gid_t sinomo_wasix_egid = 0;
static gid_t sinomo_wasix_sgid = 0;
static __thread uint64_t sinomo_wasix_pending_signal_low = 0;
static __thread uint64_t sinomo_wasix_pending_signal_high = 0;
static __thread int sinomo_wasix_tls_sched_valid = 0;
static __thread int sinomo_wasix_tls_sched_policy = SCHED_OTHER;
static __thread int sinomo_wasix_tls_sched_priority = 0;
static __thread int sinomo_wasix_publish_before_next_sleep = 0;

static int normalize_absolute_path(const char *path, char **normalized_out) {
    size_t len = strlen(path);
    char *normalized = malloc(len + 2);
    if (normalized == NULL) {
        errno = ENOMEM;
        return -1;
    }

    size_t out_len = 0;
    normalized[out_len++] = '/';
    const char *cursor = path;
    while (*cursor != '\0') {
        while (*cursor == '/')
            cursor++;
        const char *component = cursor;
        while (*cursor != '\0' && *cursor != '/')
            cursor++;
        size_t component_len = (size_t)(cursor - component);
        if (component_len == 0 ||
            (component_len == 1 && component[0] == '.')) {
            continue;
        }
        if (component_len == 2 && component[0] == '.' && component[1] == '.') {
            if (out_len > 1) {
                out_len--;
                while (out_len > 1 && normalized[out_len - 1] != '/')
                    out_len--;
            }
            continue;
        }
        if (out_len > 1)
            normalized[out_len++] = '/';
        memcpy(normalized + out_len, component, component_len);
        out_len += component_len;
    }
    normalized[out_len] = '\0';
    *normalized_out = normalized;
    return 0;
}

static int absolute_path_from_cwd(const char *path, char **absolute_out) {
    if (path[0] == '/')
        return normalize_absolute_path(path, absolute_out);

    size_t cwd_len = 128;
    char *cwd = NULL;
    for (;;) {
        char *grown = realloc(cwd, cwd_len);
        if (grown == NULL) {
            free(cwd);
            errno = ENOMEM;
            return -1;
        }
        cwd = grown;
        if (getcwd(cwd, cwd_len) != NULL)
            break;
        if (errno != ERANGE) {
            free(cwd);
            return -1;
        }
        cwd_len *= 2;
    }

    size_t path_len = strlen(path);
    size_t joined_len = strlen(cwd) + 1 + path_len + 1;
    char *joined = malloc(joined_len);
    if (joined == NULL) {
        free(cwd);
        errno = ENOMEM;
        return -1;
    }
    snprintf(joined, joined_len, "%s/%s", cwd, path);
    free(cwd);

    int result = normalize_absolute_path(joined, absolute_out);
    free(joined);
    return result;
}

int __wasilibc_find_relpath_alloc(
    const char *path,
    const char **abs,
    char **relative,
    size_t *relative_len,
    int can_realloc
) {
    const char *local_abs = NULL;
    const char *relative_const = NULL;
    char *absolute = NULL;
    char *candidate = *relative;
    size_t candidate_len = *relative_len;

    if (abs == NULL)
        abs = &local_abs;
    if (absolute_path_from_cwd(path, &absolute) != 0)
        return -1;

    int fd = __wasilibc_find_abspath(absolute, abs, &relative_const);
    if (fd < 0) {
        free(absolute);
        return -1;
    }

    size_t needed = strlen(relative_const) + 1;
    if (candidate_len < needed) {
        if (!can_realloc) {
            free(absolute);
            errno = ERANGE;
            return -1;
        }
        char *grown = realloc(candidate, needed);
        if (grown == NULL) {
            free(absolute);
            errno = ENOMEM;
            return -1;
        }
        candidate = grown;
        candidate_len = needed;
    }
    memcpy(candidate, relative_const, needed);
    free(absolute);
    *relative = candidate;
    *relative_len = candidate_len;
    return fd;
}

mode_t __sinomo_wasix_get_umask(void) {
    return sinomo_wasix_umask;
}

uid_t __sinomo_wasix_geteuid(void);
gid_t __sinomo_wasix_getegid(void);

static int find_relpath2(const char *path, char **relative, size_t *relative_len) {
    const char *abs;
    return __wasilibc_find_relpath_alloc(path, &abs, relative, relative_len, 1);
}

static int find_relpath(const char *path, char **relative) {
    static __thread char *relative_buf = NULL;
    static __thread size_t relative_buf_len = 0;
    int fd = find_relpath2(path, &relative_buf, &relative_buf_len);
    *relative = relative_buf;
    return fd;
}

static int find_relpath_alt(const char *path, char **relative) {
    static __thread char *relative_buf = NULL;
    static __thread size_t relative_buf_len = 0;
    int fd = find_relpath2(path, &relative_buf, &relative_buf_len);
    *relative = relative_buf;
    return fd;
}

static int chmodat_guest(int fd, const char *path, mode_t mode) {
    int32_t error = __imported_wasix_path_chmod_at(fd, path, strlen(path), mode & 07777);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

static int chownat_guest(int fd, const char *path, uid_t uid, gid_t gid) {
    int32_t error = __imported_wasix_path_chown_at(fd, path, strlen(path), uid, gid);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

static void touchat_guest(int fd, const char *path, int touch_self, int touch_parent) {
    (void)__imported_wasix_path_touch_at(fd, path, strlen(path), touch_self ? 1 : 0, touch_parent ? 1 : 0);
}

void __sinomo_wasix_touchat(int fd, const char *path, int touch_self, int touch_parent) {
    touchat_guest(fd, path, touch_self, touch_parent);
}

int __sinomo_wasix_symlinkat(const char *target, int fd, const char *path) {
    int32_t error = __imported_wasix_path_symlink_at(
        fd, path, strlen(path), target, strlen(target));
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

ssize_t __sinomo_wasix_readlinkat(int fd, const char *path, char *buf, size_t bufsize) {
    uint32_t used = 0;
    int32_t error = __imported_wasix_path_read_symlink_at(
        fd, path, strlen(path), buf, bufsize, &used);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return (ssize_t)used;
}

int __sinomo_wasix_unlink_symlinkat(int fd, const char *path) {
    int32_t error = __imported_wasix_path_unlink_symlink_at(fd, path, strlen(path));
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

uid_t __sinomo_wasix_geteuid(void) {
    return sinomo_wasix_euid;
}

gid_t __sinomo_wasix_getegid(void) {
    return sinomo_wasix_egid;
}

int chdir(const char *path) {
    if (path == NULL) {
        errno = EFAULT;
        return -1;
    }
    int32_t error = __imported_wasix_chdir(path, (int32_t)strlen(path));
    if (error < 0) {
        errno = -error;
        return -1;
    }
    if (error > 0) {
        errno = error;
        return -1;
    }
    return 0;
}

int fchdir(int fd) {
    int32_t error = __imported_wasix_fchdir(fd);
    if (error < 0) {
        errno = -error;
        return -1;
    }
    if (error > 0) {
        errno = error;
        return -1;
    }
    return 0;
}

extern void __do_orphaned_stdio_locks(void);

static void sinomo_wasix_after_fork_child(void) {
    sinomo_wasix_pending_signal_low = 0;
    sinomo_wasix_pending_signal_high = 0;
    __do_orphaned_stdio_locks();
    funlockfile(stdin);
    funlockfile(stdout);
    funlockfile(stderr);
}

__attribute__((constructor))
static void sinomo_wasix_register_fork_handlers(void) {
    (void)pthread_atfork(NULL, NULL, sinomo_wasix_after_fork_child);
}

static void sinomo_wasix_unlock_orphaned_stream(FILE *stream) {
    if (stream == NULL)
        return;
    struct sinomo_wasix_file_lock_view *view = (struct sinomo_wasix_file_lock_view *)stream;
    if (view->lock == SINOMO_STDIO_MAYBE_WAITERS) {
        view->lock = 0;
        view->lockcount = 0;
        view->prev_locked = NULL;
        view->next_locked = NULL;
    }
}

static void sinomo_wasix_unlock_orphaned_standard_streams(void) {
    sinomo_wasix_unlock_orphaned_stream(stdin);
    sinomo_wasix_unlock_orphaned_stream(stdout);
    sinomo_wasix_unlock_orphaned_stream(stderr);
}

int fork(void) {
    __wasi_pid_t pid = 0;
    __wasi_errno_t err = __wasi_proc_fork(1, &pid);
    if (err != 0) {
        errno = err;
        return -1;
    }
    return pid;
}

int vfork(void) {
    __wasi_pid_t pid = 0;
    __wasi_errno_t err = __wasi_proc_fork(0, &pid);
    if (err != 0) {
        errno = err;
        return -1;
    }
    return pid;
}

int sched_get_priority_max(int policy) {
    int32_t result = __imported_wasix_sched_get_priority_max(policy);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    return result;
}

int sched_get_priority_min(int policy) {
    int32_t result = __imported_wasix_sched_get_priority_min(policy);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    return result;
}

int sched_getscheduler(pid_t pid) {
    int32_t result = __imported_wasix_sched_getscheduler((int32_t)pid);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    return result;
}

int sched_getparam(pid_t pid, struct sched_param *param) {
    if (param == NULL) {
        errno = EINVAL;
        return -1;
    }
    int32_t priority = 0;
    int32_t result = __imported_wasix_sched_getparam((int32_t)pid, &priority);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    param->sched_priority = priority;
    return 0;
}

int sched_setscheduler(pid_t pid, int policy, const struct sched_param *param) {
    if (param == NULL) {
        errno = EINVAL;
        return -1;
    }
    int32_t result = __imported_wasix_sched_setscheduler((int32_t)pid, policy, param->sched_priority);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    return result;
}

int sched_setparam(pid_t pid, const struct sched_param *param) {
    if (param == NULL) {
        errno = EINVAL;
        return -1;
    }
    int policy = sched_getscheduler(pid);
    if (policy < 0)
        return -1;
    return sched_setscheduler(pid, policy, param);
}

int sched_getcpu(void) {
    int32_t result = __imported_wasix_sched_getcpu();
    if (result < 0) {
        errno = -result;
        return -1;
    }
    return result;
}

int sched_getaffinity(pid_t pid, size_t cpusetsize, cpu_set_t *mask) {
    if (mask == NULL || cpusetsize < sizeof(cpu_set_t)) {
        errno = EINVAL;
        return -1;
    }
    uint64_t mask_low = 0;
    uint64_t mask_high = 0;
    int32_t result = __imported_wasix_sched_getaffinity((int32_t)pid, &mask_low, &mask_high);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    memset(mask, 0, cpusetsize);
    unsigned char *bytes = (unsigned char *)mask;
    for (size_t i = 0; i < 8 && i < cpusetsize; i++)
        bytes[i] = (unsigned char)((mask_low >> (i * 8)) & 0xffu);
    for (size_t i = 0; i < 8 && i + 8 < cpusetsize; i++)
        bytes[i + 8] = (unsigned char)((mask_high >> (i * 8)) & 0xffu);
    return 0;
}

int sched_setaffinity(pid_t pid, size_t cpusetsize, const cpu_set_t *mask) {
    if (mask == NULL || cpusetsize == 0) {
        errno = EINVAL;
        return -1;
    }
    uint64_t mask_low = 0;
    uint64_t mask_high = 0;
    const unsigned char *bytes = (const unsigned char *)mask;
    for (size_t i = 0; i < 8 && i < cpusetsize; i++)
        mask_low |= ((uint64_t)bytes[i]) << (i * 8);
    for (size_t i = 0; i < 8 && i + 8 < cpusetsize; i++)
        mask_high |= ((uint64_t)bytes[i + 8]) << (i * 8);
    int32_t result = __imported_wasix_sched_setaffinity((int32_t)pid, mask_low, mask_high);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    return 0;
}

int sched_rr_get_interval(pid_t pid, struct timespec *interval) {
    if (interval == NULL) {
        errno = EFAULT;
        return -1;
    }
    int32_t result = __imported_wasix_sched_rr_get_interval((int32_t)pid, interval);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    return result;
}

int __sched_cpucount(size_t cpusetsize, const cpu_set_t *mask) {
    if (mask == NULL)
        return 0;
    const unsigned char *bytes = (const unsigned char *)mask;
    int count = 0;
    for (size_t i = 0; i < cpusetsize; i++) {
        unsigned char value = bytes[i];
        while (value != 0) {
            count += value & 1u;
            value >>= 1u;
        }
    }
    return count;
}

int getpriority(int which, id_t who) {
    int32_t result = __imported_wasix_getpriority((int32_t)which, (int32_t)who);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    return result;
}

int setpriority(int which, id_t who, int priority) {
    int32_t result = __imported_wasix_setpriority((int32_t)which, (int32_t)who, (int32_t)priority);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    return result;
}

int nice(int inc) {
    int32_t result = __imported_wasix_nice((int32_t)inc);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    return result;
}

int close_range(unsigned int first, unsigned int last, int flags) {
    if (first > last) {
        errno = EINVAL;
        return -1;
    }
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    for (unsigned int fd = first; fd <= last && fd < 65536u; fd++) {
        if (close((int)fd) != 0 && errno != EBADF)
            return -1;
        if (fd == UINT32_MAX)
            break;
    }
    return 0;
}

void closefrom(int lowfd) {
    if (lowfd < 0)
        lowfd = 0;
    (void)close_range((unsigned int)lowfd, 65535u, 0);
}

ssize_t copy_file_range(int fd_in, off_t *off_in, int fd_out, off_t *off_out, size_t len, unsigned int flags) {
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    if (len == 0)
        return 0;
    size_t chunk_size = len < 65536 ? len : 65536;
    char *buffer = malloc(chunk_size);
    if (buffer == NULL) {
        errno = ENOMEM;
        return -1;
    }
    ssize_t total = 0;
    while ((size_t)total < len) {
        size_t chunk = len - (size_t)total;
        if (chunk > chunk_size)
            chunk = chunk_size;
        ssize_t read_count;
        if (off_in != NULL) {
            read_count = pread(fd_in, buffer, chunk, *off_in);
        } else {
            read_count = read(fd_in, buffer, chunk);
        }
        if (read_count < 0) {
            int saved = errno;
            free(buffer);
            errno = saved;
            return total > 0 ? total : -1;
        }
        if (read_count == 0)
            break;
        ssize_t written_total = 0;
        while (written_total < read_count) {
            ssize_t written;
            if (off_out != NULL) {
                written = pwrite(fd_out, buffer + written_total, (size_t)(read_count - written_total), *off_out);
            } else {
                written = write(fd_out, buffer + written_total, (size_t)(read_count - written_total));
            }
            if (written < 0) {
                int saved = errno;
                free(buffer);
                errno = saved;
                return total > 0 ? total : -1;
            }
            if (written == 0)
                break;
            if (off_out != NULL)
                *off_out += written;
            written_total += written;
        }
        if (off_in != NULL)
            *off_in += read_count;
        total += written_total;
        if (written_total < read_count)
            break;
    }
    free(buffer);
    return total;
}

char *ctermid_r(char *buf) {
    if (buf == NULL) {
        errno = EINVAL;
        return NULL;
    }
    strcpy(buf, "/dev/tty");
    return buf;
}

char *ctermid(char *buf) {
    static char terminal[] = "/dev/tty";
    if (buf == NULL)
        return terminal;
    strcpy(buf, terminal);
    return buf;
}

char *getlogin(void) {
    return "sandbox";
}

int getlogin_r(char *name, size_t namesize) {
    if (name == NULL || namesize == 0)
        return EINVAL;
    const char *login = getlogin();
    if (login == NULL)
        login = "sandbox";
    size_t len = strlen(login);
    if (len + 1 > namesize)
        return ERANGE;
    memcpy(name, login, len + 1);
    return 0;
}

int getloadavg(double loadavg[], int nelem) {
    if (nelem < 0) {
        errno = EINVAL;
        return -1;
    }
    int count = nelem > 3 ? 3 : nelem;
    for (int i = 0; i < count; i++)
        loadavg[i] = 0.0;
    return count;
}

int getresuid(uid_t *ruid, uid_t *euid, uid_t *suid) {
    uint32_t raw_ruid = 0;
    uint32_t raw_euid = 0;
    uint32_t raw_suid = 0;
    int32_t error = __imported_wasix_proc_getresuid(&raw_ruid, &raw_euid, &raw_suid);
    if (error != 0) {
        errno = error;
        return -1;
    }
    if (ruid != NULL)
        *ruid = raw_ruid;
    if (euid != NULL)
        *euid = raw_euid;
    if (suid != NULL)
        *suid = raw_suid;
    return 0;
}

int getresgid(gid_t *rgid, gid_t *egid, gid_t *sgid) {
    uint32_t raw_rgid = 0;
    uint32_t raw_egid = 0;
    uint32_t raw_sgid = 0;
    int32_t error = __imported_wasix_proc_getresgid(&raw_rgid, &raw_egid, &raw_sgid);
    if (error != 0) {
        errno = error;
        return -1;
    }
    if (rgid != NULL)
        *rgid = raw_rgid;
    if (egid != NULL)
        *egid = raw_egid;
    if (sgid != NULL)
        *sgid = raw_sgid;
    return 0;
}

int lockf(int fd, int cmd, off_t len) {
    (void)fd;
    (void)cmd;
    (void)len;
    errno = ENOSYS;
    return -1;
}

void sync(void) {
}

int sync_file_range(int fd, off_t offset, off_t nbytes, unsigned int flags) {
    (void)offset;
    (void)nbytes;
    (void)flags;
    return fsync(fd);
}

int pause(void) {
    for (;;) {
        int32_t error = __imported_wasix_thread_sleep(UINT64_MAX);
        if (error != 0) {
            errno = error < 0 ? -error : error;
            return -1;
        }
    }
}

int getrlimit(int resource, struct rlimit *limit) {
    if (limit == NULL) {
        errno = EFAULT;
        return -1;
    }
    uint64_t current = 0;
    uint64_t maximum = 0;
    int32_t result = __imported_wasix_getrlimit(resource, &current, &maximum);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    limit->rlim_cur = (rlim_t)current;
    limit->rlim_max = (rlim_t)maximum;
    return 0;
}

int setrlimit(int resource, const struct rlimit *limit) {
    if (limit == NULL) {
        errno = EFAULT;
        return -1;
    }
    int32_t result = __imported_wasix_setrlimit(resource, (uint64_t)limit->rlim_cur, (uint64_t)limit->rlim_max);
    if (result < 0) {
        errno = -result;
        return -1;
    }
    return 0;
}

clock_t times(struct tms *buffer) {
    if (buffer == NULL) {
        errno = EFAULT;
        return (clock_t)-1;
    }
    uint64_t elapsed_ticks = 0;
    uint64_t user_ticks = 0;
    uint64_t system_ticks = 0;
    uint64_t child_user_ticks = 0;
    uint64_t child_system_ticks = 0;
    int32_t result = __imported_wasix_proc_times(
        &elapsed_ticks,
        &user_ticks,
        &system_ticks,
        &child_user_ticks,
        &child_system_ticks);
    if (result < 0) {
        errno = -result;
        return (clock_t)-1;
    }
    buffer->tms_utime = (clock_t)user_ticks;
    buffer->tms_stime = (clock_t)system_ticks;
    buffer->tms_cutime = (clock_t)child_user_ticks;
    buffer->tms_cstime = (clock_t)child_system_ticks;
    return (clock_t)elapsed_ticks;
}

int pthread_attr_getschedpolicy(const pthread_attr_t *restrict attr, int *restrict policy) {
    if (attr == NULL || policy == NULL)
        return EINVAL;
    *policy = attr->__u.__i[5];
    return 0;
}

int pthread_attr_getschedparam(const pthread_attr_t *restrict attr, struct sched_param *restrict param) {
    if (attr == NULL || param == NULL)
        return EINVAL;
    param->sched_priority = attr->__u.__i[6];
    return 0;
}

int pthread_attr_getdetachstate(const pthread_attr_t *attr, int *state) {
    if (attr == NULL || state == NULL)
        return EINVAL;
    *state = attr->__u.__i[3];
    return 0;
}

int pthread_attr_getguardsize(const pthread_attr_t *restrict attr, size_t *restrict size) {
    if (attr == NULL || size == NULL)
        return EINVAL;
    *size = attr->__u.__s[1];
    return 0;
}

int pthread_attr_getinheritsched(const pthread_attr_t *restrict attr, int *restrict inherit) {
    if (attr == NULL || inherit == NULL)
        return EINVAL;
    *inherit = attr->__u.__i[4];
    return 0;
}

int pthread_attr_getscope(const pthread_attr_t *restrict attr, int *restrict scope) {
    if (attr == NULL || scope == NULL)
        return EINVAL;
    *scope = PTHREAD_SCOPE_SYSTEM;
    return 0;
}

int pthread_attr_getstack(const pthread_attr_t *restrict attr, void **restrict addr, size_t *restrict size) {
    if (attr == NULL || addr == NULL || size == NULL)
        return EINVAL;
    *size = attr->__u.__s[0];
    *addr = attr->__u.__s[2] == 0 ? NULL : (void *)(attr->__u.__s[2] - *size);
    return 0;
}

int pthread_attr_getstacksize(const pthread_attr_t *restrict attr, size_t *restrict size) {
    if (attr == NULL || size == NULL)
        return EINVAL;
    *size = attr->__u.__s[0];
    return 0;
}

int pthread_attr_setstack(pthread_attr_t *attr, void *addr, size_t size) {
    if (attr == NULL || addr == NULL)
        return EINVAL;
    if (size < PTHREAD_STACK_MIN)
        return EINVAL;
    attr->__u.__s[0] = size;
    attr->__u.__s[2] = (uintptr_t)addr + size;
    return 0;
}

int pthread_attr_setstacksize(pthread_attr_t *attr, size_t size) {
    if (attr == NULL)
        return EINVAL;
    if (size < PTHREAD_STACK_MIN)
        return EINVAL;
    attr->__u.__s[0] = size;
    return 0;
}

int pthread_attr_setinheritsched(pthread_attr_t *attr, int inherit) {
    if (attr == NULL)
        return EINVAL;
    if (inherit != PTHREAD_INHERIT_SCHED && inherit != PTHREAD_EXPLICIT_SCHED)
        return EINVAL;
    attr->__u.__i[4] = inherit;
    return 0;
}

int pthread_attr_setschedparam(pthread_attr_t *restrict attr, const struct sched_param *restrict param) {
    if (attr == NULL || param == NULL)
        return EINVAL;
    int policy = attr->__u.__i[5];
    int min = sched_get_priority_min(policy);
    int max = sched_get_priority_max(policy);
    if (min == -1 || max == -1)
        return EINVAL;
    if (param->sched_priority < min || param->sched_priority > max)
        return EINVAL;
    attr->__u.__i[6] = param->sched_priority;
    return 0;
}

int pthread_attr_setschedpolicy(pthread_attr_t *attr, int policy) {
    if (attr == NULL)
        return EINVAL;
    switch (policy) {
    case SCHED_OTHER:
    case SCHED_FIFO:
    case SCHED_RR:
    case SCHED_BATCH:
    case SCHED_IDLE:
    case SCHED_DEADLINE:
        attr->__u.__i[5] = policy;
        return 0;
    default:
        if (policy < 0)
            return ENOTSUP;
        return EINVAL;
    }
}

#define SINOMO_BARRIER_MAGIC 0x53424152
#define SINOMO_BARRIER_LIMIT_WORD 0
#define SINOMO_BARRIER_COUNT_WORD 1
#define SINOMO_BARRIER_GEN_WORD 2
#define SINOMO_BARRIER_RELEASE_WORD 3
#define SINOMO_BARRIER_MAGIC_WORD 4

int pthread_barrier_init(pthread_barrier_t *restrict barrier, const pthread_barrierattr_t *restrict attr, unsigned count) {
    if (barrier == NULL || count == 0)
        return EINVAL;
    volatile int *words = barrier->__u.__vi;
    words[SINOMO_BARRIER_LIMIT_WORD] = (int)count;
    words[SINOMO_BARRIER_COUNT_WORD] = 0;
    words[SINOMO_BARRIER_GEN_WORD] = 0;
    words[SINOMO_BARRIER_RELEASE_WORD] = 0;
    words[SINOMO_BARRIER_MAGIC_WORD] = SINOMO_BARRIER_MAGIC;
    if (attr != NULL && attr->__attr == PTHREAD_PROCESS_SHARED) {
        int32_t hint = __sinomo_wasix_shared_futex_hint((uintptr_t)barrier, (uintptr_t)sizeof(*barrier));
        if (hint != 0)
            return (int)hint;
    }
    return 0;
}

int pthread_barrier_destroy(pthread_barrier_t *barrier) {
    if (barrier == NULL)
        return EINVAL;
    volatile int *words = barrier->__u.__vi;
    if (words[SINOMO_BARRIER_MAGIC_WORD] != SINOMO_BARRIER_MAGIC)
        return EINVAL;
    if (__atomic_load_n(&words[SINOMO_BARRIER_COUNT_WORD], __ATOMIC_SEQ_CST) != 0)
        return EBUSY;
    if (words[SINOMO_BARRIER_MAGIC_WORD] == SINOMO_BARRIER_MAGIC)
        (void)__sinomo_wasix_shared_futex_unhint((uintptr_t)barrier, (uintptr_t)sizeof(*barrier));
    memset(barrier, 0, sizeof(*barrier));
    return 0;
}

int pthread_barrier_wait(pthread_barrier_t *barrier) {
    if (barrier == NULL)
        return EINVAL;
    volatile int *words = barrier->__u.__vi;
    if (words[SINOMO_BARRIER_MAGIC_WORD] != SINOMO_BARRIER_MAGIC)
        return EINVAL;

    int limit = __atomic_load_n(&words[SINOMO_BARRIER_LIMIT_WORD], __ATOMIC_SEQ_CST);
    if (limit <= 0)
        return EINVAL;

    if (__sinomo_wasix_shared_memory_is_cross_process() != 0 &&
        __sinomo_wasix_shared_memory_has_direct_mapping((uintptr_t)barrier, (uintptr_t)sizeof(*barrier)) == 0) {
        int generation = (int)__sinomo_wasix_shared_barrier_generation((uintptr_t)barrier);
        words[SINOMO_BARRIER_GEN_WORD] = generation;
        int32_t arrive = __sinomo_wasix_shared_barrier_arrive((uintptr_t)barrier, (uint32_t)limit);
        if (arrive == PTHREAD_BARRIER_SERIAL_THREAD) {
            words[SINOMO_BARRIER_GEN_WORD] = generation + 1;
            (void)__wasilibc_futex_wake_wasix((int *)&words[SINOMO_BARRIER_GEN_WORD], 1);
            return PTHREAD_BARRIER_SERIAL_THREAD;
        }
        if (arrive != 0)
            return (int)arrive;

        struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000L};
        for (;;) {
            int current = (int)__sinomo_wasix_shared_barrier_generation((uintptr_t)barrier);
            if (current != generation) {
                words[SINOMO_BARRIER_GEN_WORD] = current;
                int32_t remaining = __sinomo_wasix_shared_barrier_release((uintptr_t)barrier);
                if (remaining > 0)
                    (void)__wasilibc_futex_wake_wasix((int *)&words[SINOMO_BARRIER_GEN_WORD], 1);
                return 0;
            }
            while (nanosleep(&pause, &pause) != 0 && errno == EINTR) {
            }
            pause.tv_sec = 0;
            pause.tv_nsec = 1000000L;
        }
    }

    int generation = __atomic_load_n(&words[SINOMO_BARRIER_GEN_WORD], __ATOMIC_SEQ_CST);
    int arrived = __atomic_add_fetch(&words[SINOMO_BARRIER_COUNT_WORD], 1, __ATOMIC_SEQ_CST);
    if (arrived == limit) {
        __atomic_store_n(&words[SINOMO_BARRIER_COUNT_WORD], 0, __ATOMIC_SEQ_CST);
        __atomic_store_n(&words[SINOMO_BARRIER_RELEASE_WORD], limit - 1, __ATOMIC_SEQ_CST);
        __atomic_add_fetch(&words[SINOMO_BARRIER_GEN_WORD], 1, __ATOMIC_SEQ_CST);
        (void)__wasilibc_futex_wake_wasix((int *)&words[SINOMO_BARRIER_GEN_WORD], 1);
        return PTHREAD_BARRIER_SERIAL_THREAD;
    }
    if (arrived > limit) {
        (void)__atomic_sub_fetch(&words[SINOMO_BARRIER_COUNT_WORD], 1, __ATOMIC_SEQ_CST);
        return EINVAL;
    }

    for (;;) {
        int current = __atomic_load_n(&words[SINOMO_BARRIER_GEN_WORD], __ATOMIC_SEQ_CST);
        if (current != generation) {
            int remaining = __atomic_sub_fetch(&words[SINOMO_BARRIER_RELEASE_WORD], 1, __ATOMIC_SEQ_CST);
            if (remaining > 0)
                (void)__wasilibc_futex_wake_wasix((int *)&words[SINOMO_BARRIER_GEN_WORD], 1);
            return 0;
        }
        int wait_ret = __wasilibc_futex_wait_wasix(&words[SINOMO_BARRIER_GEN_WORD], 0, generation, -1);
        if (wait_ret == 0 || wait_ret == -EAGAIN || wait_ret == -EINTR || wait_ret == -ECANCELED)
            continue;
        if (wait_ret < 0)
            return -wait_ret;
        return wait_ret;
    }
}

int pthread_barrierattr_init(pthread_barrierattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr = PTHREAD_PROCESS_PRIVATE;
    return 0;
}

int pthread_barrierattr_destroy(pthread_barrierattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr = 0;
    return 0;
}

int pthread_barrierattr_setpshared(pthread_barrierattr_t *attr, int pshared) {
    if (attr == NULL)
        return EINVAL;
    switch (pshared) {
    case PTHREAD_PROCESS_PRIVATE:
    case PTHREAD_PROCESS_SHARED:
        attr->__attr = (unsigned)pshared;
        return 0;
    default:
        return EINVAL;
    }
}

int pthread_barrierattr_getpshared(const pthread_barrierattr_t *restrict attr, int *restrict pshared) {
    if (attr == NULL || pshared == NULL)
        return EINVAL;
    *pshared = (int)attr->__attr;
    return 0;
}

int pthread_condattr_init(pthread_condattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr = CLOCK_REALTIME;
    return 0;
}

int pthread_condattr_destroy(pthread_condattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr = 0;
    return 0;
}

int pthread_condattr_setpshared(pthread_condattr_t *attr, int pshared) {
    if (attr == NULL)
        return EINVAL;
    switch (pshared) {
    case PTHREAD_PROCESS_PRIVATE:
        attr->__attr &= 0x7fffffffu;
        return 0;
    case PTHREAD_PROCESS_SHARED:
        attr->__attr |= 0x80000000u;
        return 0;
    default:
        return EINVAL;
    }
}

int pthread_condattr_getpshared(const pthread_condattr_t *restrict attr, int *restrict pshared) {
    if (attr == NULL || pshared == NULL)
        return EINVAL;
    *pshared = attr->__attr >> 31;
    return 0;
}

int pthread_condattr_setclock(pthread_condattr_t *attr, clockid_t clock_id) {
    if (attr == NULL)
        return EINVAL;
    switch (clock_id) {
    case CLOCK_REALTIME:
    case CLOCK_MONOTONIC:
        attr->__attr = (attr->__attr & 0x80000000u) | ((unsigned)clock_id & 0x7fffffffu);
        return 0;
    default:
        return EINVAL;
    }
}

int pthread_condattr_getclock(const pthread_condattr_t *restrict attr, clockid_t *restrict clock_id) {
    if (attr == NULL || clock_id == NULL)
        return EINVAL;
    *clock_id = attr->__attr & 0x7fffffff;
    return 0;
}

#define SINOMO_MUTEX_ATTR_MAGIC 0x80000000u
#define SINOMO_MUTEX_ATTR_PRIOCEILING_MASK (0x7fffU << 16)

static int sinomo_mutexattr_valid(const pthread_mutexattr_t *attr) {
    return attr != NULL && (attr->__attr & SINOMO_MUTEX_ATTR_MAGIC) != 0;
}

int pthread_mutexattr_getprotocol(const pthread_mutexattr_t *restrict attr, int *restrict protocol) {
    if (attr == NULL || protocol == NULL)
        return EINVAL;
    if (!sinomo_mutexattr_valid(attr))
        return EINVAL;
    *protocol = (attr->__attr >> 3) & 3U;
    if (*protocol > PTHREAD_PRIO_PROTECT)
        return EINVAL;
    return 0;
}

int pthread_mutexattr_setprotocol(pthread_mutexattr_t *attr, int protocol) {
    if (attr == NULL)
        return EINVAL;
    if (!sinomo_mutexattr_valid(attr))
        return EINVAL;
    switch (protocol) {
    case PTHREAD_PRIO_NONE:
    case PTHREAD_PRIO_INHERIT:
    case PTHREAD_PRIO_PROTECT:
        attr->__attr = (attr->__attr & ~(3U << 3)) | ((unsigned)protocol << 3);
        return 0;
    default:
        return EINVAL;
    }
}

static int sinomo_mutex_default_prioceiling(void) {
    int min = sched_get_priority_min(SCHED_FIFO);
    int max = sched_get_priority_max(SCHED_FIFO);
    if (min >= 0 && max >= min)
        return min;
    return 0;
}

static int sinomo_mutex_valid_prioceiling(int prioceiling) {
    int min = sched_get_priority_min(SCHED_FIFO);
    int max = sched_get_priority_max(SCHED_FIFO);
    if (min < 0 || max < min)
        return prioceiling >= 0;
    return prioceiling >= min && prioceiling <= max;
}

int pthread_mutexattr_getprioceiling(const pthread_mutexattr_t *restrict attr, int *restrict prioceiling) {
    if (attr == NULL || prioceiling == NULL)
        return EINVAL;
    if (!sinomo_mutexattr_valid(attr))
        return EINVAL;
    unsigned encoded = (attr->__attr >> 16) & 0x7fffU;
    *prioceiling = encoded == 0 ? sinomo_mutex_default_prioceiling() : (int)encoded;
    return 0;
}

int pthread_mutexattr_setprioceiling(pthread_mutexattr_t *attr, int prioceiling) {
    if (attr == NULL)
        return EINVAL;
    if (!sinomo_mutexattr_valid(attr))
        return EINVAL;
    if (!sinomo_mutex_valid_prioceiling(prioceiling))
        return EINVAL;
    attr->__attr = (attr->__attr & ~SINOMO_MUTEX_ATTR_PRIOCEILING_MASK) |
        (((unsigned)prioceiling << 16) & SINOMO_MUTEX_ATTR_PRIOCEILING_MASK);
    return 0;
}

int pthread_mutexattr_getpshared(const pthread_mutexattr_t *restrict attr, int *restrict pshared) {
    if (attr == NULL || pshared == NULL)
        return EINVAL;
    if (!sinomo_mutexattr_valid(attr))
        return EINVAL;
    *pshared = attr->__attr / 128U % 2;
    return 0;
}

int pthread_mutexattr_getrobust(const pthread_mutexattr_t *restrict attr, int *restrict robust) {
    if (attr == NULL || robust == NULL)
        return EINVAL;
    if (!sinomo_mutexattr_valid(attr))
        return EINVAL;
    *robust = attr->__attr / 4U % 2;
    return 0;
}

int pthread_mutexattr_setrobust(pthread_mutexattr_t *attr, int robust) {
    if (attr == NULL)
        return EINVAL;
    if (!sinomo_mutexattr_valid(attr))
        return EINVAL;
    switch (robust) {
    case PTHREAD_MUTEX_STALLED:
        attr->__attr &= ~4U;
        return 0;
    case PTHREAD_MUTEX_ROBUST:
        attr->__attr |= 4U;
        return 0;
    default:
        return EINVAL;
    }
}

int pthread_mutexattr_gettype(const pthread_mutexattr_t *restrict attr, int *restrict type) {
    if (attr == NULL || type == NULL)
        return EINVAL;
    if (!sinomo_mutexattr_valid(attr))
        return EINVAL;
    *type = attr->__attr & 3;
    return 0;
}

int pthread_mutexattr_init(pthread_mutexattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr = SINOMO_MUTEX_ATTR_MAGIC | PTHREAD_MUTEX_DEFAULT;
    return 0;
}

int pthread_mutexattr_destroy(pthread_mutexattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr = 0;
    return 0;
}

int pthread_mutexattr_settype(pthread_mutexattr_t *attr, int type) {
    if (attr == NULL)
        return EINVAL;
    if (!sinomo_mutexattr_valid(attr))
        return EINVAL;
    switch (type) {
    case PTHREAD_MUTEX_NORMAL:
    case PTHREAD_MUTEX_RECURSIVE:
    case PTHREAD_MUTEX_ERRORCHECK:
        attr->__attr = (attr->__attr & ~3U) | (unsigned)type;
        return 0;
    default:
        return EINVAL;
    }
}

int pthread_mutexattr_setpshared(pthread_mutexattr_t *attr, int pshared) {
    if (attr == NULL)
        return EINVAL;
    if (!sinomo_mutexattr_valid(attr))
        return EINVAL;
    switch (pshared) {
    case PTHREAD_PROCESS_PRIVATE:
        attr->__attr &= ~128U;
        return 0;
    case PTHREAD_PROCESS_SHARED:
        attr->__attr |= 128U;
        return 0;
    default:
        return EINVAL;
    }
}

int pthread_rwlockattr_getpshared(const pthread_rwlockattr_t *restrict attr, int *restrict pshared) {
    if (attr == NULL || pshared == NULL)
        return EINVAL;
    *pshared = attr->__attr[0];
    return 0;
}

#define SINOMO_RWLOCK_MAGIC 0x5352574c
#define SINOMO_RWLOCK_STATE_WORD 0
#define SINOMO_RWLOCK_WRITER_WAITERS_WORD 1
#define SINOMO_RWLOCK_PSHARED_WORD 2
#define SINOMO_RWLOCK_MAGIC_WORD 3
#define SINOMO_RWLOCK_WRITER_PRIORITY_WORD 4
#define SINOMO_RWLOCK_WRITER_POLICY_WORD 5
#define SINOMO_RWLOCK_WRITER_OWNER_WORD 6
#define SINOMO_RWLOCK_READER_PRIORITY_WORD 7

static void sinomo_wasix_get_current_sched(int *policy, int *priority);

static int rwlock_is_shared(volatile int *words) {
    return words[SINOMO_RWLOCK_PSHARED_WORD] == PTHREAD_PROCESS_SHARED;
}

static int rwlock_has_direct_mapping(pthread_rwlock_t *rwlock) {
    return __sinomo_wasix_shared_memory_has_direct_mapping((uintptr_t)rwlock, (uintptr_t)sizeof(*rwlock)) != 0;
}

static int rwlock_needs_runtime_shared(pthread_rwlock_t *rwlock, volatile int *words) {
    return rwlock_is_shared(words) &&
           __sinomo_wasix_shared_memory_was_cross_process() != 0 &&
           !rwlock_has_direct_mapping(rwlock);
}

static int rwlock_ensure_initialized(pthread_rwlock_t *rwlock) {
    if (rwlock == NULL)
        return EINVAL;
    volatile int *words = rwlock->__u.__vi;
    int magic = __atomic_load_n(&words[SINOMO_RWLOCK_MAGIC_WORD], __ATOMIC_SEQ_CST);
    if (magic == SINOMO_RWLOCK_MAGIC)
        return 0;
    if (magic != 0)
        return EINVAL;
    __atomic_store_n(&words[SINOMO_RWLOCK_STATE_WORD], 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_WAITERS_WORD], 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&words[SINOMO_RWLOCK_PSHARED_WORD], PTHREAD_PROCESS_PRIVATE, __ATOMIC_SEQ_CST);
    __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD], 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_POLICY_WORD], SCHED_OTHER, __ATOMIC_SEQ_CST);
    __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_OWNER_WORD], 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&words[SINOMO_RWLOCK_READER_PRIORITY_WORD], 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&words[SINOMO_RWLOCK_MAGIC_WORD], SINOMO_RWLOCK_MAGIC, __ATOMIC_SEQ_CST);
    return 0;
}

static int rwlock_is_initialized(pthread_rwlock_t *rwlock) {
    if (rwlock == NULL)
        return 0;
    volatile int *words = rwlock->__u.__vi;
    return __atomic_load_n(&words[SINOMO_RWLOCK_MAGIC_WORD], __ATOMIC_SEQ_CST) == SINOMO_RWLOCK_MAGIC;
}

static void rwlock_current_sched(int *policy, int *priority) {
    if (sinomo_wasix_tls_sched_valid) {
        *policy = sinomo_wasix_tls_sched_policy;
        *priority = sinomo_wasix_tls_sched_priority;
        return;
    }
    pthread_t self = pthread_self();
    struct sched_param param;
    int current_policy = SCHED_OTHER;
    param.sched_priority = 0;
    if (pthread_getschedparam(self, &current_policy, &param) != 0)
        sinomo_wasix_get_current_sched(&current_policy, &param.sched_priority);
    *policy = current_policy;
    *priority = param.sched_priority;
}

static int rwlock_policy_orders_readers(int policy) {
    return policy == SCHED_FIFO || policy == SCHED_RR;
}

static int rwlock_reader_must_wait_for_writer(volatile int *words) {
    int waiters = __atomic_load_n(&words[SINOMO_RWLOCK_WRITER_WAITERS_WORD], __ATOMIC_SEQ_CST);
    if (waiters <= 0)
        return 0;
    int writer_policy = __atomic_load_n(&words[SINOMO_RWLOCK_WRITER_POLICY_WORD], __ATOMIC_SEQ_CST);
    if (!rwlock_policy_orders_readers(writer_policy))
        return 0;
    int policy = SCHED_OTHER;
    int priority = 0;
    rwlock_current_sched(&policy, &priority);
    if (!rwlock_policy_orders_readers(policy))
        return 0;
    int writer_priority = __atomic_load_n(&words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD], __ATOMIC_SEQ_CST);
    return priority <= writer_priority;
}

static void rwlock_register_writer_waiter(volatile int *words) {
    int policy = SCHED_OTHER;
    int priority = 0;
    rwlock_current_sched(&policy, &priority);
    __atomic_fetch_add(&words[SINOMO_RWLOCK_WRITER_WAITERS_WORD], 1, __ATOMIC_SEQ_CST);
    int current_priority = __atomic_load_n(&words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD], __ATOMIC_SEQ_CST);
    while (priority > current_priority &&
           !__atomic_compare_exchange_n(&words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD], &current_priority, priority, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
    }
    if (rwlock_policy_orders_readers(policy))
        __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_POLICY_WORD], policy, __ATOMIC_SEQ_CST);
}

static void rwlock_unregister_writer_waiter(volatile int *words) {
    int remaining = __atomic_sub_fetch(&words[SINOMO_RWLOCK_WRITER_WAITERS_WORD], 1, __ATOMIC_SEQ_CST);
    if (remaining <= 0) {
        __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_WAITERS_WORD], 0, __ATOMIC_SEQ_CST);
        __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD], 0, __ATOMIC_SEQ_CST);
        __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_POLICY_WORD], SCHED_OTHER, __ATOMIC_SEQ_CST);
    } else {
        int policy = SCHED_OTHER;
        int priority = 0;
        rwlock_current_sched(&policy, &priority);
        int current_priority = __atomic_load_n(&words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD], __ATOMIC_SEQ_CST);
        if (priority >= current_priority)
            __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD], 0, __ATOMIC_SEQ_CST);
    }
}

static void rwlock_register_reader_waiter(volatile int *words) {
    int policy = SCHED_OTHER;
    int priority = 0;
    rwlock_current_sched(&policy, &priority);
    if (!rwlock_policy_orders_readers(policy))
        return;
    int current_priority = __atomic_load_n(&words[SINOMO_RWLOCK_READER_PRIORITY_WORD], __ATOMIC_SEQ_CST);
    while (priority > current_priority &&
           !__atomic_compare_exchange_n(&words[SINOMO_RWLOCK_READER_PRIORITY_WORD], &current_priority, priority, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
    }
}

static void rwlock_unregister_reader_waiter(volatile int *words) {
    int policy = SCHED_OTHER;
    int priority = 0;
    rwlock_current_sched(&policy, &priority);
    if (!rwlock_policy_orders_readers(policy))
        return;
    int current_priority = __atomic_load_n(&words[SINOMO_RWLOCK_READER_PRIORITY_WORD], __ATOMIC_SEQ_CST);
    if (priority >= current_priority)
        __atomic_store_n(&words[SINOMO_RWLOCK_READER_PRIORITY_WORD], 0, __ATOMIC_SEQ_CST);
}

static int rwlock_wait(pthread_rwlock_t *rwlock, volatile int *state_word, int expected, int runtime_shared, int64_t wait_ns) {
    if (runtime_shared)
        return (int)__sinomo_wasix_shared_rwlock_wait((uintptr_t)rwlock, wait_ns);
    int64_t futex_wait_ns = wait_ns < 0 || wait_ns > 10000000LL ? 10000000LL : wait_ns;
    int wait_ret = __wasilibc_futex_wait_wasix(state_word, 0, expected, futex_wait_ns);
    if (wait_ret == 0 || wait_ret == -EAGAIN || wait_ret == -EINTR || wait_ret == -ETIMEDOUT || wait_ret == -ECANCELED)
        return 0;
    return wait_ret < 0 ? -wait_ret : wait_ret;
}

static int64_t rwlock_abstime_to_wait_ns(const struct timespec *abstime) {
    if (abstime == NULL)
        return -1;
    if (abstime->tv_nsec < 0 || abstime->tv_nsec >= 1000000000L)
        return INT64_MIN;
    struct timeval tv;
    if (gettimeofday(&tv, NULL) != 0)
        return INT64_MIN + 1;
    int64_t sec = (int64_t)abstime->tv_sec - (int64_t)tv.tv_sec;
    int64_t nsec = (int64_t)abstime->tv_nsec - ((int64_t)tv.tv_usec * 1000LL);
    int64_t total = sec * 1000000000LL + nsec;
    return total <= 0 ? 0 : total;
}

static int rwlock_try_rdlock_local(volatile int *words) {
    for (;;) {
        int state = __atomic_load_n(&words[SINOMO_RWLOCK_STATE_WORD], __ATOMIC_SEQ_CST);
        if (state < 0 || rwlock_reader_must_wait_for_writer(words))
            return EBUSY;
        if (__atomic_compare_exchange_n(&words[SINOMO_RWLOCK_STATE_WORD], &state, state + 1, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
            return 0;
    }
}

static int rwlock_try_wrlock_local(volatile int *words, int registered_waiter, int policy, int priority) {
    int waiters = __atomic_load_n(&words[SINOMO_RWLOCK_WRITER_WAITERS_WORD], __ATOMIC_SEQ_CST);
    int max_priority = __atomic_load_n(&words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD], __ATOMIC_SEQ_CST);
    int max_reader_priority = __atomic_load_n(&words[SINOMO_RWLOCK_READER_PRIORITY_WORD], __ATOMIC_SEQ_CST);
    int max_policy = __atomic_load_n(&words[SINOMO_RWLOCK_WRITER_POLICY_WORD], __ATOMIC_SEQ_CST);
    if (registered_waiter && waiters > 0 && rwlock_policy_orders_readers(max_policy) && rwlock_policy_orders_readers(policy) && priority < max_priority)
        return EBUSY;
    if (registered_waiter && rwlock_policy_orders_readers(policy) && priority < max_reader_priority)
        return EBUSY;
    int expected = 0;
    if (__atomic_compare_exchange_n(&words[SINOMO_RWLOCK_STATE_WORD], &expected, -1, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_OWNER_WORD], (int)(uintptr_t)pthread_self(), __ATOMIC_SEQ_CST);
        return 0;
    }
    return EBUSY;
}

static int rwlock_unlock_local(volatile int *words) {
    for (;;) {
        int state = __atomic_load_n(&words[SINOMO_RWLOCK_STATE_WORD], __ATOMIC_SEQ_CST);
        if (state == -1) {
            int expected = -1;
            if (__atomic_compare_exchange_n(&words[SINOMO_RWLOCK_STATE_WORD], &expected, 0, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
                __atomic_store_n(&words[SINOMO_RWLOCK_WRITER_OWNER_WORD], 0, __ATOMIC_SEQ_CST);
                return 0;
            }
            continue;
        }
        if (state > 0) {
            if (__atomic_compare_exchange_n(&words[SINOMO_RWLOCK_STATE_WORD], &state, state - 1, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
                return 0;
            continue;
        }
        return EPERM;
    }
}

int pthread_rwlock_init(pthread_rwlock_t *restrict rwlock, const pthread_rwlockattr_t *restrict attr) {
    if (rwlock == NULL)
        return EINVAL;
    memset(rwlock, 0, sizeof(*rwlock));
    volatile int *words = rwlock->__u.__vi;
    words[SINOMO_RWLOCK_STATE_WORD] = 0;
    words[SINOMO_RWLOCK_WRITER_WAITERS_WORD] = 0;
    words[SINOMO_RWLOCK_PSHARED_WORD] = attr == NULL ? PTHREAD_PROCESS_PRIVATE : (int)attr->__attr[0];
    words[SINOMO_RWLOCK_MAGIC_WORD] = SINOMO_RWLOCK_MAGIC;
    words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD] = 0;
    words[SINOMO_RWLOCK_WRITER_POLICY_WORD] = SCHED_OTHER;
    words[SINOMO_RWLOCK_WRITER_OWNER_WORD] = 0;
    words[SINOMO_RWLOCK_READER_PRIORITY_WORD] = 0;
    if (rwlock_is_shared(words)) {
        int32_t hint = __sinomo_wasix_shared_futex_hint((uintptr_t)rwlock, (uintptr_t)sizeof(*rwlock));
        if (hint != 0)
            return (int)hint;
    }
    return 0;
}

int pthread_rwlock_destroy(pthread_rwlock_t *rwlock) {
    int init_ret = rwlock_ensure_initialized(rwlock);
    if (init_ret != 0)
        return init_ret;
    volatile int *words = rwlock->__u.__vi;
    if (rwlock_needs_runtime_shared(rwlock, words)) {
        int32_t destroy = __sinomo_wasix_shared_rwlock_destroy((uintptr_t)rwlock);
        if (destroy != 0)
            return (int)destroy;
    }
    if (rwlock_is_shared(words))
        (void)__sinomo_wasix_shared_futex_unhint((uintptr_t)rwlock, (uintptr_t)sizeof(*rwlock));
    memset(rwlock, 0, sizeof(*rwlock));
    return 0;
}

int pthread_rwlock_tryrdlock(pthread_rwlock_t *rwlock) {
    int init_ret = rwlock_ensure_initialized(rwlock);
    if (init_ret != 0)
        return init_ret;
    volatile int *words = rwlock->__u.__vi;
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug rwlock_tryrd enter rwlock=%p words=[%d,%d,%d,%d]\n", (void *)rwlock, words[0], words[1], words[2], words[3]);
    int policy = SCHED_OTHER;
    int priority = 0;
    rwlock_current_sched(&policy, &priority);
    int32_t ret = rwlock_needs_runtime_shared(rwlock, words)
        ? __sinomo_wasix_shared_rwlock_try_rdlock((uintptr_t)rwlock, policy, priority, 0)
        : rwlock_try_rdlock_local(words);
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug rwlock_tryrd result rwlock=%p ret=%d words=[%d,%d,%d,%d]\n", (void *)rwlock, ret, words[0], words[1], words[2], words[3]);
    return ret == 0 ? 0 : (int)ret;
}

int pthread_rwlock_trywrlock(pthread_rwlock_t *rwlock) {
    int init_ret = rwlock_ensure_initialized(rwlock);
    if (init_ret != 0)
        return init_ret;
    volatile int *words = rwlock->__u.__vi;
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug rwlock_trywr enter rwlock=%p words=[%d,%d,%d,%d]\n", (void *)rwlock, words[0], words[1], words[2], words[3]);
    int policy = SCHED_OTHER;
    int priority = 0;
    rwlock_current_sched(&policy, &priority);
    int32_t ret = rwlock_needs_runtime_shared(rwlock, words)
        ? __sinomo_wasix_shared_rwlock_try_wrlock((uintptr_t)rwlock, 0, policy, priority)
        : rwlock_try_wrlock_local(words, 0, policy, priority);
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug rwlock_trywr acquired rwlock=%p ret=%d words=[%d,%d,%d,%d]\n", (void *)rwlock, ret, words[0], words[1], words[2], words[3]);
    return ret == 0 ? 0 : (int)ret;
}

int pthread_rwlock_timedrdlock(pthread_rwlock_t *restrict rwlock, const struct timespec *restrict abstime) {
    int init_ret = rwlock_ensure_initialized(rwlock);
    if (init_ret != 0)
        return init_ret;
    volatile int *words = rwlock->__u.__vi;
    int policy = SCHED_OTHER;
    int priority = 0;
    rwlock_current_sched(&policy, &priority);
    int runtime_shared = rwlock_needs_runtime_shared(rwlock, words);
    int registered_waiter = 0;
    for (;;) {
        int ret = runtime_shared
            ? __sinomo_wasix_shared_rwlock_try_rdlock((uintptr_t)rwlock, policy, priority, registered_waiter ? 1U : 0U)
            : rwlock_try_rdlock_local(words);
        if (ret == 0) {
            if (!runtime_shared && registered_waiter)
                rwlock_unregister_reader_waiter(words);
            return 0;
        }
        if (ret != EBUSY) {
            if (runtime_shared && registered_waiter)
                (void)__sinomo_wasix_shared_rwlock_unregister_reader((uintptr_t)rwlock, policy, priority);
            else if (registered_waiter)
                rwlock_unregister_reader_waiter(words);
            return ret;
        }
        int64_t wait_ns = rwlock_abstime_to_wait_ns(abstime);
        if (wait_ns == INT64_MIN)
            return EINVAL;
        if (wait_ns == INT64_MIN + 1)
            return errno == 0 ? EINVAL : errno;
        if (wait_ns == 0) {
            if (runtime_shared && registered_waiter)
                (void)__sinomo_wasix_shared_rwlock_unregister_reader((uintptr_t)rwlock, policy, priority);
            else if (registered_waiter)
                rwlock_unregister_reader_waiter(words);
            return ETIMEDOUT;
        }
        if (runtime_shared && !registered_waiter) {
            ret = __sinomo_wasix_shared_rwlock_register_reader((uintptr_t)rwlock, policy, priority);
            if (ret != 0)
                return ret;
            registered_waiter = 1;
        } else if (!runtime_shared && !registered_waiter) {
            rwlock_register_reader_waiter(words);
            registered_waiter = 1;
        }
        int observed = __atomic_load_n(&words[SINOMO_RWLOCK_STATE_WORD], __ATOMIC_SEQ_CST);
        ret = rwlock_wait(rwlock, &words[SINOMO_RWLOCK_STATE_WORD], observed, runtime_shared, wait_ns);
        if (ret != 0) {
            if (runtime_shared && registered_waiter)
                (void)__sinomo_wasix_shared_rwlock_unregister_reader((uintptr_t)rwlock, policy, priority);
            else if (registered_waiter)
                rwlock_unregister_reader_waiter(words);
            return ret;
        }
    }
}

int pthread_rwlock_rdlock(pthread_rwlock_t *rwlock) {
    return pthread_rwlock_timedrdlock(rwlock, NULL);
}

int pthread_rwlock_timedwrlock(pthread_rwlock_t *restrict rwlock, const struct timespec *restrict abstime) {
    int init_ret = rwlock_ensure_initialized(rwlock);
    if (init_ret != 0)
        return init_ret;
    volatile int *words = rwlock->__u.__vi;
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug rwlock_wrlock enter rwlock=%p words=[%d,%d,%d,%d]\n", (void *)rwlock, words[0], words[1], words[2], words[3]);
    int runtime_shared = rwlock_needs_runtime_shared(rwlock, words);
    int registered_waiter = 0;
    for (;;) {
        if (!runtime_shared &&
            __atomic_load_n(&words[SINOMO_RWLOCK_STATE_WORD], __ATOMIC_SEQ_CST) == -1 &&
            __atomic_load_n(&words[SINOMO_RWLOCK_WRITER_OWNER_WORD], __ATOMIC_SEQ_CST) == (int)(uintptr_t)pthread_self()) {
            return EDEADLK;
        }
        int policy = SCHED_OTHER;
        int priority = 0;
        rwlock_current_sched(&policy, &priority);
        int ret = runtime_shared
            ? __sinomo_wasix_shared_rwlock_try_wrlock((uintptr_t)rwlock, registered_waiter ? 1U : 0U, policy, priority)
            : rwlock_try_wrlock_local(words, registered_waiter, policy, priority);
        if (ret == 0) {
            if (!runtime_shared && registered_waiter)
                rwlock_unregister_writer_waiter(words);
            return 0;
        }
        if (ret != EBUSY) {
            if (runtime_shared && registered_waiter)
                (void)__sinomo_wasix_shared_rwlock_unregister_writer((uintptr_t)rwlock, policy, priority);
            else if (registered_waiter)
                rwlock_unregister_writer_waiter(words);
            return ret;
        }
        int64_t wait_ns = rwlock_abstime_to_wait_ns(abstime);
        if (wait_ns == INT64_MIN)
            return EINVAL;
        if (wait_ns == INT64_MIN + 1)
            return errno == 0 ? EINVAL : errno;
        if (wait_ns == 0) {
            if (runtime_shared && registered_waiter)
                (void)__sinomo_wasix_shared_rwlock_unregister_writer((uintptr_t)rwlock, policy, priority);
            else if (registered_waiter)
                rwlock_unregister_writer_waiter(words);
            return ETIMEDOUT;
        }
        if (!registered_waiter) {
            if (runtime_shared) {
                ret = __sinomo_wasix_shared_rwlock_register_writer((uintptr_t)rwlock, policy, priority);
                if (ret != 0)
                    return ret;
            } else {
                rwlock_register_writer_waiter(words);
            }
            registered_waiter = 1;
        }
        int observed = __atomic_load_n(&words[SINOMO_RWLOCK_STATE_WORD], __ATOMIC_SEQ_CST);
        ret = rwlock_wait(rwlock, &words[SINOMO_RWLOCK_STATE_WORD], observed, runtime_shared, wait_ns);
        if (ret != 0) {
            if (runtime_shared && registered_waiter)
                (void)__sinomo_wasix_shared_rwlock_unregister_writer((uintptr_t)rwlock, policy, priority);
            else if (registered_waiter)
                rwlock_unregister_writer_waiter(words);
            return ret;
        }
    }
}

int pthread_rwlock_wrlock(pthread_rwlock_t *rwlock) {
    return pthread_rwlock_timedwrlock(rwlock, NULL);
}

int pthread_rwlock_unlock(pthread_rwlock_t *rwlock) {
    if (!rwlock_is_initialized(rwlock))
        return EINVAL;
    int init_ret = rwlock_ensure_initialized(rwlock);
    if (init_ret != 0)
        return init_ret;
    volatile int *words = rwlock->__u.__vi;
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug rwlock_unlock enter rwlock=%p words=[%d,%d,%d,%d]\n", (void *)rwlock, words[0], words[1], words[2], words[3]);
    int32_t ret = rwlock_needs_runtime_shared(rwlock, words)
        ? __sinomo_wasix_shared_rwlock_unlock((uintptr_t)rwlock)
        : rwlock_unlock_local(words);
    if (ret == 0)
        (void)__wasilibc_futex_wake_wasix((int *)&words[SINOMO_RWLOCK_STATE_WORD], INT_MAX);
    return ret == 0 ? 0 : (int)ret;
}

int __pthread_rwlock_rdlock(pthread_rwlock_t *rwlock) {
    return pthread_rwlock_rdlock(rwlock);
}

int __pthread_rwlock_wrlock(pthread_rwlock_t *rwlock) {
    return pthread_rwlock_wrlock(rwlock);
}

int __pthread_rwlock_unlock(pthread_rwlock_t *rwlock) {
    return pthread_rwlock_unlock(rwlock);
}

int __pthread_rwlock_tryrdlock(pthread_rwlock_t *rwlock) {
    return pthread_rwlock_tryrdlock(rwlock);
}

int __pthread_rwlock_trywrlock(pthread_rwlock_t *rwlock) {
    return pthread_rwlock_trywrlock(rwlock);
}

int pthread_rwlockattr_init(pthread_rwlockattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr[0] = PTHREAD_PROCESS_PRIVATE;
    attr->__attr[1] = 0;
    return 0;
}

int pthread_rwlockattr_destroy(pthread_rwlockattr_t *attr) {
    if (attr == NULL)
        return EINVAL;
    attr->__attr[0] = PTHREAD_PROCESS_PRIVATE;
    attr->__attr[1] = 0;
    return 0;
}

int pthread_rwlockattr_setpshared(pthread_rwlockattr_t *attr, int pshared) {
    if (attr == NULL)
        return EINVAL;
    switch (pshared) {
    case PTHREAD_PROCESS_PRIVATE:
    case PTHREAD_PROCESS_SHARED:
        attr->__attr[0] = (unsigned)pshared;
        return 0;
    default:
        return EINVAL;
    }
}

int pthread_getcpuclockid(pthread_t thread, clockid_t *clock_id) {
    (void)thread;
    if (clock_id == NULL)
        return EINVAL;
    *clock_id = CLOCK_THREAD_CPUTIME_ID;
    return 0;
}

int clock_getcpuclockid(pid_t pid, clockid_t *clock_id) {
    if (clock_id == NULL)
        return EINVAL;
    if (pid < 0)
        return EINVAL;
    *clock_id = CLOCK_PROCESS_CPUTIME_ID;
    return 0;
}

int __pthread_create(
    pthread_t *restrict thread,
    const pthread_attr_t *restrict attr,
    void *(*start_routine)(void *),
    void *restrict arg
);

#define SINOMO_WASIX_PTHREAD_DETACHED_SLOTS 1024
static uintptr_t sinomo_wasix_detached_threads[SINOMO_WASIX_PTHREAD_DETACHED_SLOTS];
static uintptr_t sinomo_wasix_joined_threads[SINOMO_WASIX_PTHREAD_DETACHED_SLOTS];

struct sinomo_wasix_thread_sched_record {
    uintptr_t thread;
    int tid;
    int policy;
    int priority;
    uintptr_t stack_addr;
    uintptr_t stack_size;
};

static struct sinomo_wasix_thread_sched_record sinomo_wasix_thread_sched[SINOMO_WASIX_PTHREAD_DETACHED_SLOTS];
static int sinomo_wasix_pending_create_sched_valid = 0;
static int sinomo_wasix_pending_create_sched_policy = SCHED_OTHER;
static int sinomo_wasix_pending_create_sched_priority = 0;
static volatile int sinomo_wasix_created_thread_count = 0;
static volatile int sinomo_wasix_joined_thread_count = 0;

struct sinomo_wasix_pthread_tid_prefix {
    uintptr_t self;
    uintptr_t prev;
    uintptr_t next;
    uintptr_t sysinfo;
    uintptr_t canary;
    int32_t tid;
};

static size_t sinomo_wasix_detached_slot(pthread_t thread) {
    uintptr_t value = (uintptr_t)thread >> 4;
    value ^= value >> 12;
    value ^= value >> 24;
    return value % SINOMO_WASIX_PTHREAD_DETACHED_SLOTS;
}

static void sinomo_wasix_mark_detached_thread(pthread_t thread) {
    if (thread == NULL)
        return;
    sinomo_wasix_detached_threads[sinomo_wasix_detached_slot(thread)] = (uintptr_t)thread;
}

static void sinomo_wasix_clear_detached_thread(pthread_t thread) {
    if (thread == NULL)
        return;
    size_t slot = sinomo_wasix_detached_slot(thread);
    if (sinomo_wasix_detached_threads[slot] == (uintptr_t)thread)
        sinomo_wasix_detached_threads[slot] = 0;
}

static int sinomo_wasix_is_detached_thread(pthread_t thread) {
    if (thread == NULL)
        return 0;
    return sinomo_wasix_detached_threads[sinomo_wasix_detached_slot(thread)] == (uintptr_t)thread;
}

static void sinomo_wasix_mark_joined_thread(pthread_t thread) {
    if (thread == NULL)
        return;
    sinomo_wasix_joined_threads[sinomo_wasix_detached_slot(thread)] = (uintptr_t)thread;
}

static void sinomo_wasix_clear_joined_thread(pthread_t thread) {
    if (thread == NULL)
        return;
    size_t slot = sinomo_wasix_detached_slot(thread);
    if (sinomo_wasix_joined_threads[slot] == (uintptr_t)thread)
        sinomo_wasix_joined_threads[slot] = 0;
}

static int sinomo_wasix_is_joined_thread(pthread_t thread) {
    if (thread == NULL)
        return 0;
    return sinomo_wasix_joined_threads[sinomo_wasix_detached_slot(thread)] == (uintptr_t)thread;
}

static int sinomo_wasix_valid_sched_policy(int policy) {
    switch (policy) {
    case SCHED_OTHER:
    case SCHED_FIFO:
    case SCHED_RR:
    case SCHED_BATCH:
    case SCHED_IDLE:
    case SCHED_DEADLINE:
        return 1;
    default:
        return 0;
    }
}

static void sinomo_wasix_get_current_sched(int *policy, int *priority) {
    if (sinomo_wasix_tls_sched_valid) {
        *policy = sinomo_wasix_tls_sched_policy;
        *priority = sinomo_wasix_tls_sched_priority;
        return;
    }
    int32_t sched_policy = __imported_wasix_sched_getscheduler(0);
    int32_t sched_priority = 0;
    if (sched_policy < 0)
        sched_policy = SCHED_OTHER;
    if (__imported_wasix_sched_getparam(0, &sched_priority) < 0)
        sched_priority = 0;
    *policy = sched_policy;
    *priority = sched_priority;
}

static int sinomo_wasix_get_thread_sched(pthread_t thread, int *policy, int *priority) {
    if (thread == NULL)
        return 0;
    size_t slot = sinomo_wasix_detached_slot(thread);
    if (sinomo_wasix_thread_sched[slot].thread != (uintptr_t)thread)
        return 0;
    *policy = sinomo_wasix_thread_sched[slot].policy;
    *priority = sinomo_wasix_thread_sched[slot].priority;
    return 1;
}

static int sinomo_wasix_get_thread_tid(pthread_t thread) {
    if (thread == NULL)
        return 0;
    size_t slot = sinomo_wasix_detached_slot(thread);
    if (sinomo_wasix_thread_sched[slot].thread != (uintptr_t)thread)
        return 0;
    return sinomo_wasix_thread_sched[slot].tid;
}

static void sinomo_wasix_set_thread_sched(pthread_t thread, int tid, int policy, int priority) {
    if (thread == NULL)
        return;
    size_t slot = sinomo_wasix_detached_slot(thread);
    sinomo_wasix_thread_sched[slot].thread = (uintptr_t)thread;
    sinomo_wasix_thread_sched[slot].tid = tid;
    sinomo_wasix_thread_sched[slot].policy = policy;
    sinomo_wasix_thread_sched[slot].priority = priority;
}

static void sinomo_wasix_set_thread_stack(pthread_t thread, void *stack_addr, size_t stack_size) {
    if (thread == NULL)
        return;
    size_t slot = sinomo_wasix_detached_slot(thread);
    if (sinomo_wasix_thread_sched[slot].thread != (uintptr_t)thread)
        sinomo_wasix_thread_sched[slot].thread = (uintptr_t)thread;
    sinomo_wasix_thread_sched[slot].stack_addr = (uintptr_t)stack_addr;
    sinomo_wasix_thread_sched[slot].stack_size = (uintptr_t)stack_size;
}

static int sinomo_wasix_get_thread_stack(pthread_t thread, void **stack_addr, size_t *stack_size) {
    if (thread == NULL)
        return 0;
    size_t slot = sinomo_wasix_detached_slot(thread);
    if (sinomo_wasix_thread_sched[slot].thread != (uintptr_t)thread)
        return 0;
    if (stack_addr != NULL)
        *stack_addr = (void *)sinomo_wasix_thread_sched[slot].stack_addr;
    if (stack_size != NULL)
        *stack_size = (size_t)sinomo_wasix_thread_sched[slot].stack_size;
    return sinomo_wasix_thread_sched[slot].stack_size != 0;
}

static void sinomo_wasix_clear_thread_sched(pthread_t thread) {
    if (thread == NULL)
        return;
    size_t slot = sinomo_wasix_detached_slot(thread);
    if (sinomo_wasix_thread_sched[slot].thread == (uintptr_t)thread)
        memset(&sinomo_wasix_thread_sched[slot], 0, sizeof(sinomo_wasix_thread_sched[slot]));
}

int pthread_getschedparam(pthread_t thread, int *restrict policy, struct sched_param *restrict param) {
    if (policy == NULL || param == NULL)
        return EINVAL;
    int priority = 0;
    if (!sinomo_wasix_get_thread_sched(thread, policy, &priority)) {
        for (int i = 0; i < 10000 && thread != NULL; i++) {
            if (sinomo_wasix_get_thread_sched(thread, policy, &priority))
                break;
            __asm__ __volatile__("" ::: "memory");
        }
    }
    if (!sinomo_wasix_get_thread_sched(thread, policy, &priority) && sinomo_wasix_pending_create_sched_valid) {
        *policy = sinomo_wasix_pending_create_sched_policy;
        priority = sinomo_wasix_pending_create_sched_priority;
    } else if (!sinomo_wasix_get_thread_sched(thread, policy, &priority)) {
        sinomo_wasix_get_current_sched(policy, &priority);
    }
    param->sched_priority = priority;
    return 0;
}

int pthread_setschedparam(pthread_t thread, int policy, const struct sched_param *param) {
    if (param == NULL)
        return EINVAL;
    if (!sinomo_wasix_valid_sched_policy(policy))
        return EINVAL;
    int min = sched_get_priority_min(policy);
    int max = sched_get_priority_max(policy);
    if (min == -1 || max == -1)
        return EINVAL;
    if (param->sched_priority < min || param->sched_priority > max)
        return EINVAL;
    sinomo_wasix_set_thread_sched(thread, sinomo_wasix_get_thread_tid(thread), policy, param->sched_priority);
    if (pthread_equal(thread, pthread_self())) {
        sinomo_wasix_tls_sched_valid = 1;
        sinomo_wasix_tls_sched_policy = policy;
        sinomo_wasix_tls_sched_priority = param->sched_priority;
    }
    return 0;
}

int pthread_setschedprio(pthread_t thread, int priority) {
    int policy = SCHED_OTHER;
    int current_priority = 0;
    if (!sinomo_wasix_get_thread_sched(thread, &policy, &current_priority))
        sinomo_wasix_get_current_sched(&policy, &current_priority);
    int min = sched_get_priority_min(policy);
    int max = sched_get_priority_max(policy);
    if (min == -1 || max == -1)
        return EINVAL;
    if (priority < min || priority > max)
        return EINVAL;
    sinomo_wasix_set_thread_sched(thread, sinomo_wasix_get_thread_tid(thread), policy, priority);
    if (pthread_equal(thread, pthread_self())) {
        sinomo_wasix_tls_sched_valid = 1;
        sinomo_wasix_tls_sched_policy = policy;
        sinomo_wasix_tls_sched_priority = priority;
    }
    return 0;
}

int pthread_create(
    pthread_t *restrict thread,
    const pthread_attr_t *restrict attr,
    void *(*start_routine)(void *),
    void *restrict arg
) {
    if (attr != NULL && attr != (const pthread_attr_t *)(uintptr_t)-1 && attr->__u.__s[0] == 0) {
        (void)raise(SIGSEGV);
        return EINVAL;
    }
    pthread_attr_t default_attr;
    const pthread_attr_t *create_attr = attr;
    if (attr == NULL) {
        int attr_result = pthread_attr_init(&default_attr);
        if (attr_result != 0)
            return attr_result;
        attr_result = pthread_attr_setstacksize(&default_attr, SINOMO_WASIX_DEFAULT_THREAD_STACK_SIZE);
        if (attr_result != 0)
            return attr_result;
        create_attr = &default_attr;
    }
    int create_policy = SCHED_OTHER;
    int create_priority = 0;
    int create_has_sched = 0;
    if (attr != NULL && attr != (const pthread_attr_t *)(uintptr_t)-1 && attr->__u.__i[4] == PTHREAD_EXPLICIT_SCHED) {
        create_policy = attr->__u.__i[5];
        create_priority = attr->__u.__i[6];
        create_has_sched = 1;
    } else {
        pthread_t self = pthread_self();
        if (!sinomo_wasix_get_thread_sched(self, &create_policy, &create_priority))
            sinomo_wasix_get_current_sched(&create_policy, &create_priority);
        create_has_sched = 1;
    }
    if (create_has_sched) {
        sinomo_wasix_pending_create_sched_valid = 1;
        sinomo_wasix_pending_create_sched_policy = create_policy;
        sinomo_wasix_pending_create_sched_priority = create_priority;
    }
    if (__sinomo_wasix_shared_memory_is_cross_process() != 0) {
        int32_t sync_result = __sinomo_wasix_shared_memory_sync_from();
        if (sync_result != 0) {
            sinomo_wasix_pending_create_sched_valid = 0;
            return (int)sync_result;
        }
    }
    int before_created = __atomic_load_n(&sinomo_wasix_created_thread_count, __ATOMIC_SEQ_CST);
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug pthread_create enter created=%d attr=%p start=%p arg=%p\n",
        before_created, (void *)attr, (void *)start_routine, arg);
    if (before_created >= 190) {
        SINOMO_PTHREAD_LOG("sinomo-debug pthread_create enter created=%d attr=%p\n",
            before_created, (void *)attr);
    }
    int result = __pthread_create(thread, create_attr, start_routine, arg);
    sinomo_wasix_pending_create_sched_valid = 0;
    if (result == 0 && thread != NULL) {
        int created = __atomic_add_fetch(&sinomo_wasix_created_thread_count, 1, __ATOMIC_SEQ_CST);
        SINOMO_PTHREAD_ENV_LOG("sinomo-debug pthread_create created=%d thread=%p tid=%d\n",
            created, (void *)*thread,
            *thread != NULL ? ((struct sinomo_wasix_pthread_tid_prefix *)*thread)->tid : 0);
        if ((created % 100) == 0 || created >= 190) {
            SINOMO_PTHREAD_LOG("sinomo-debug pthread_create created=%d thread=%p\n", created, (void *)*thread);
        }
        if (create_attr != NULL && create_attr != (const pthread_attr_t *)(uintptr_t)-1 && create_attr->__u.__i[3] != 0)
            sinomo_wasix_mark_detached_thread(*thread);
        else
            sinomo_wasix_clear_detached_thread(*thread);
        sinomo_wasix_clear_joined_thread(*thread);
        if (create_has_sched) {
            struct sinomo_wasix_pthread_tid_prefix *created_thread = (struct sinomo_wasix_pthread_tid_prefix *)*thread;
            sinomo_wasix_set_thread_sched(*thread, created_thread != NULL ? created_thread->tid : 0, create_policy, create_priority);
        }
        if (create_attr != NULL && create_attr != (const pthread_attr_t *)(uintptr_t)-1) {
            void *stack_addr = NULL;
            size_t stack_size = 0;
            if (pthread_attr_getstack(create_attr, &stack_addr, &stack_size) == 0)
                sinomo_wasix_set_thread_stack(*thread, stack_addr, stack_size);
        }
    } else if (result != 0) {
        int created = __atomic_load_n(&sinomo_wasix_created_thread_count, __ATOMIC_SEQ_CST);
        SINOMO_PTHREAD_LOG("sinomo-debug pthread_create failed result=%d created=%d attr=%p\n",
            result, created, (void *)attr);
    }
    return result;
}

enum {
    SINOMO_WASIX_DT_EXITED = 0,
    SINOMO_WASIX_DT_EXITING = 1,
    SINOMO_WASIX_DT_JOINABLE = 2,
    SINOMO_WASIX_DT_DETACHED = 3,
};

struct sinomo_wasix_pthread_state {
    uintptr_t self;
    uintptr_t prev;
    uintptr_t next;
    uintptr_t sysinfo;
    uintptr_t canary;
    int32_t tid;
    int32_t errno_val;
    volatile int32_t detach_state;
    volatile int32_t cancel;
    uint8_t canceldisable;
    uint8_t cancelasync;
    uint8_t tsd_used;
    uint8_t dlerror_flag;
    uintptr_t map_base;
    uintptr_t map_size;
    uintptr_t stack;
    uintptr_t stack_size;
    uintptr_t guard_size;
    uintptr_t result;
    uintptr_t cancelbuf;
    void **tsd;
};

extern _Thread_local struct sinomo_wasix_pthread_state *__wasilibc_pthread_self;

static int sinomo_wasix_pthread_join_impl(pthread_t thread, void **retval) {
    if (thread == NULL)
        return ESRCH;
    if (sinomo_wasix_is_joined_thread(thread))
        return ESRCH;
    struct sinomo_wasix_pthread_state *target = (struct sinomo_wasix_pthread_state *)thread;
    if (target == __wasilibc_pthread_self)
        return EDEADLK;

    int state = __atomic_load_n(&target->detach_state, __ATOMIC_SEQ_CST);
    if (state == SINOMO_WASIX_DT_DETACHED)
        return EINVAL;

    unsigned debug_count = 0;
    while (state != SINOMO_WASIX_DT_EXITED) {
        if (state != SINOMO_WASIX_DT_JOINABLE && state != SINOMO_WASIX_DT_EXITING)
            return ESRCH;
        debug_count++;
        if (debug_count == 1 || (debug_count % 10000u) == 0u) {
            SINOMO_PTHREAD_ENV_LOG("sinomo-debug pthread_join waiting thread=%p tid=%d state=%d count=%u\n",
                (void *)thread, target->tid, state, debug_count);
            SINOMO_PTHREAD_LOG("sinomo-debug pthread_join waiting thread=%p tid=%d state=%d count=%u\n",
                (void *)thread, target->tid, state, debug_count);
        }
        int wait_ret = __wasilibc_futex_wait_wasix(&target->detach_state, 0, state, 10000000LL);
        if (wait_ret != 0 && wait_ret != -EAGAIN && wait_ret != -EINTR && wait_ret != -ETIMEDOUT && wait_ret != -ECANCELED)
            return -wait_ret;
        pthread_testcancel();
        state = __atomic_load_n(&target->detach_state, __ATOMIC_SEQ_CST);
    }

    if (retval != NULL)
        *retval = (void *)target->result;
    int saved_tid = sinomo_wasix_get_thread_tid(thread);
    if (saved_tid <= 0)
        saved_tid = target->tid;
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug pthread_join bridge thread=%p target_tid=%d saved_tid=%d state=%d\n",
        (void *)thread, target->tid, saved_tid, state);
    SINOMO_PTHREAD_LOG("sinomo-debug pthread_join bridge thread=%p target_tid=%d saved_tid=%d state=%d\n",
        (void *)thread, target->tid, saved_tid, state);
    int32_t join_ret = saved_tid > 0 ? __imported_wasix_thread_join((uint32_t)saved_tid) : 0;
    if (join_ret != 0)
        return (int)join_ret;
    sinomo_wasix_unlock_orphaned_standard_streams();
    __atomic_store_n(&target->detach_state, SINOMO_WASIX_DT_DETACHED, __ATOMIC_SEQ_CST);
    sinomo_wasix_clear_detached_thread(thread);
    sinomo_wasix_clear_thread_sched(thread);
    int joined = __atomic_add_fetch(&sinomo_wasix_joined_thread_count, 1, __ATOMIC_SEQ_CST);
    if ((joined % 100) == 0 || joined >= 190) {
        SINOMO_PTHREAD_LOG("sinomo-debug pthread_join joined=%d thread=%p tid=%d map=%p size=%zu\n",
            joined, (void *)thread, target->tid, (void *)target->map_base, (size_t)target->map_size);
    }
    uintptr_t map_base = target->map_base;
    uintptr_t map_size = target->map_size;
    sinomo_wasix_mark_joined_thread(thread);
    if (map_base != 0 && map_size != 0)
        (void)munmap((void *)map_base, (size_t)map_size);
    return 0;
}

int pthread_join(pthread_t thread, void **retval) {
    return sinomo_wasix_pthread_join_impl(thread, retval);
}

int __pthread_join(pthread_t thread, void **retval) {
    return sinomo_wasix_pthread_join_impl(thread, retval);
}

int pthread_detach(pthread_t thread) {
    if (thread == NULL)
        return ESRCH;
    if (sinomo_wasix_is_joined_thread(thread))
        return ESRCH;

    struct sinomo_wasix_pthread_state *target = (struct sinomo_wasix_pthread_state *)thread;
    int state = __atomic_load_n(&target->detach_state, __ATOMIC_SEQ_CST);
    for (;;) {
        if (state == SINOMO_WASIX_DT_DETACHED)
            return EINVAL;
        if (state != SINOMO_WASIX_DT_JOINABLE &&
            state != SINOMO_WASIX_DT_EXITING &&
            state != SINOMO_WASIX_DT_EXITED)
            return ESRCH;
        if (__atomic_compare_exchange_n(&target->detach_state, &state, SINOMO_WASIX_DT_DETACHED, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
            break;
    }

    sinomo_wasix_mark_detached_thread(thread);
    if (state == SINOMO_WASIX_DT_EXITED) {
        int saved_tid = sinomo_wasix_get_thread_tid(thread);
        if (saved_tid <= 0)
            saved_tid = target->tid;
        int32_t join_ret = saved_tid > 0 ? __imported_wasix_thread_join((uint32_t)saved_tid) : 0;
        if (join_ret != 0)
            return (int)join_ret;
        sinomo_wasix_clear_detached_thread(thread);
        sinomo_wasix_clear_thread_sched(thread);
        uintptr_t map_base = target->map_base;
        uintptr_t map_size = target->map_size;
        sinomo_wasix_mark_joined_thread(thread);
        if (map_base != 0 && map_size != 0)
            (void)munmap((void *)map_base, (size_t)map_size);
    }
    return 0;
}

int __pthread_detach(pthread_t thread) {
    return pthread_detach(thread);
}

int pthread_getattr_np(pthread_t thread, pthread_attr_t *attr) {
    if (thread == NULL || attr == NULL)
        return EINVAL;
    int ret = pthread_attr_init(attr);
    if (ret != 0)
        return ret;

    void *stack_addr = NULL;
    size_t stack_size = 0;
    if (sinomo_wasix_get_thread_stack(thread, &stack_addr, &stack_size)) {
        ret = stack_addr == NULL
            ? pthread_attr_setstacksize(attr, stack_size)
            : pthread_attr_setstack(attr, stack_addr, stack_size);
        if (ret != 0)
            return ret;
    } else {
        struct sinomo_wasix_pthread_state *target = (struct sinomo_wasix_pthread_state *)thread;
        if (target->stack_size != 0) {
            attr->__u.__s[0] = target->stack_size;
            attr->__u.__s[2] = target->stack == 0 ? 0 : target->stack + target->stack_size;
        }
    }
    if (sinomo_wasix_is_detached_thread(thread))
        attr->__u.__i[3] = PTHREAD_CREATE_DETACHED;
    else
        attr->__u.__i[3] = PTHREAD_CREATE_JOINABLE;
    return 0;
}

void *pthread_getspecific(pthread_key_t key) {
    struct sinomo_wasix_pthread_state *self = __wasilibc_pthread_self;
    if (self == NULL || key >= PTHREAD_KEYS_MAX)
        return NULL;
    void **tsd = self->tsd;
    if (tsd == NULL)
        return NULL;
    return tsd[key];
}

long __cancel(void) {
    struct sinomo_wasix_pthread_state *self = __wasilibc_pthread_self;
    if (self != NULL && self->canceldisable == PTHREAD_CANCEL_ENABLE)
        pthread_exit(PTHREAD_CANCELED);
    if (self != NULL)
        self->canceldisable = PTHREAD_CANCEL_DISABLE;
    return -ECANCELED;
}

void __testcancel(void) {
    struct sinomo_wasix_pthread_state *self = __wasilibc_pthread_self;
    if (self != NULL)
        sinomo_wasix_sync_range_to_guest_if_shared((const void *)&self->cancel, sizeof(self->cancel));
    if (self != NULL &&
        __atomic_load_n(&self->cancel, __ATOMIC_SEQ_CST) &&
        self->canceldisable == PTHREAD_CANCEL_ENABLE) {
        SINOMO_PTHREAD_LOG("sinomo-debug pthread_testcancel exiting thread=%p tid=%d\n",
            (void *)self, self->tid);
        pthread_exit(PTHREAD_CANCELED);
    }
}

void __pthread_testcancel(void) {
    __testcancel();
}

void pthread_testcancel(void) {
    __testcancel();
}

static int64_t sinomo_wasix_timespec_to_ns(const struct timespec *ts) {
    if (ts == NULL || ts->tv_sec < 0 || ts->tv_nsec < 0 || ts->tv_nsec >= 1000000000L)
        return -1;
    if (ts->tv_sec > (INT64_MAX - ts->tv_nsec) / 1000000000LL)
        return INT64_MAX;
    return (int64_t)ts->tv_sec * 1000000000LL + ts->tv_nsec;
}

static int sinomo_wasix_clock_now_ns(clockid_t clock_id, int64_t *now_out) {
    __wasi_clockid_t wasi_clock;
    switch (clock_id) {
    case CLOCK_REALTIME:
        wasi_clock = __WASI_CLOCKID_REALTIME;
        break;
    case CLOCK_MONOTONIC:
        wasi_clock = __WASI_CLOCKID_MONOTONIC;
        break;
    default:
        return EINVAL;
    }
    __wasi_timestamp_t now = 0;
    __wasi_errno_t error = __wasi_clock_time_get(wasi_clock, 1, &now);
    if (error != 0)
        return (int)error;
    *now_out = now > (uint64_t)INT64_MAX ? INT64_MAX : (int64_t)now;
    return 0;
}

static int sinomo_wasix_sleep_relative_ns(int64_t duration_ns, struct timespec *rem);
static uint64_t signal_bit(int sig, uint64_t *high);
static int take_signal_from_words(uint64_t *low, uint64_t *high, uint64_t mask_low, uint64_t mask_high);

struct sinomo_wasix_posix_timer_slot {
    int allocated;
    unsigned generation;
    clockid_t clock_id;
    int64_t armed_at_ns;
    int64_t value_ns;
    int64_t interval_ns;
    int notification_pending;
    int64_t last_overrun_count;
    int notify;
    int signo;
    union sigval value;
    void (*notify_function)(union sigval);
    pthread_attr_t notify_attributes;
    int has_notify_attributes;
};

#define SINOMO_WASIX_POSIX_TIMER_MAX 256

static pthread_mutex_t sinomo_wasix_posix_timer_lock = PTHREAD_MUTEX_INITIALIZER;
static struct sinomo_wasix_posix_timer_slot sinomo_wasix_posix_timers[SINOMO_WASIX_POSIX_TIMER_MAX];
static int sinomo_wasix_posix_timer_atexit_registered;

struct sinomo_wasix_timer_thread_arg {
    uint32_t timer_id;
    unsigned generation;
    int64_t value_sec;
    int64_t value_nsec;
    int64_t interval_sec;
    int64_t interval_nsec;
};

static uint32_t sinomo_wasix_timer_id_from_timer(timer_t timer_id) {
    return (uint32_t)(uintptr_t)timer_id;
}

static timer_t sinomo_wasix_timer_from_id(uint32_t timer_id) {
    return (timer_t)(uintptr_t)timer_id;
}

static int64_t sinomo_wasix_timer_duration_ns(int64_t sec, int64_t nsec) {
    if (sec < 0 || nsec < 0 || nsec >= 1000000000LL)
        return -1;
    if (sec > (INT64_MAX - nsec) / 1000000000LL)
        return INT64_MAX;
    return sec * 1000000000LL + nsec;
}

static int64_t sinomo_wasix_timer_overrun_count_at(
    const struct sinomo_wasix_posix_timer_slot *slot,
    int64_t now_ns
) {
    if (slot == NULL || slot->value_ns <= 0 || slot->interval_ns <= 0)
        return 0;
    int64_t elapsed_ns = now_ns - slot->armed_at_ns;
    if (elapsed_ns <= slot->value_ns)
        return 0;
    int64_t overruns = (elapsed_ns - slot->value_ns) / slot->interval_ns;
    return overruns < 0 ? 0 : overruns;
}

static int sinomo_wasix_timer_slot_snapshot(
    uint32_t timer_id,
    unsigned generation,
    struct sinomo_wasix_posix_timer_slot *snapshot
) {
    if (timer_id == 0 || timer_id > SINOMO_WASIX_POSIX_TIMER_MAX)
        return 0;
    pthread_mutex_lock(&sinomo_wasix_posix_timer_lock);
    struct sinomo_wasix_posix_timer_slot *slot = &sinomo_wasix_posix_timers[timer_id - 1];
    int active = slot->allocated && slot->generation == generation;
    if (active)
        *snapshot = *slot;
    pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);
    return active;
}

static void sinomo_wasix_clear_timer_notification_for_signal(int sig) {
    pthread_mutex_lock(&sinomo_wasix_posix_timer_lock);
    for (size_t i = 0; i < SINOMO_WASIX_POSIX_TIMER_MAX; i++) {
        struct sinomo_wasix_posix_timer_slot *slot = &sinomo_wasix_posix_timers[i];
        if (slot->allocated && slot->notify == SIGEV_SIGNAL && slot->signo == sig) {
            slot->notification_pending = 0;
            slot->last_overrun_count = -1;
        }
    }
    pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);
}

static void sinomo_wasix_disarm_all_posix_timers(void) {
    pthread_mutex_lock(&sinomo_wasix_posix_timer_lock);
    for (size_t i = 0; i < SINOMO_WASIX_POSIX_TIMER_MAX; i++) {
        struct sinomo_wasix_posix_timer_slot *slot = &sinomo_wasix_posix_timers[i];
        if (!slot->allocated)
            continue;
        slot->allocated = 0;
        slot->generation++;
        if (slot->generation == 0)
            slot->generation = 1;
        slot->notification_pending = 0;
        slot->last_overrun_count = -1;
    }
    pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);
}

static void sinomo_wasix_freeze_timer_notification_for_signal(int sig) {
    pthread_mutex_lock(&sinomo_wasix_posix_timer_lock);
    for (size_t i = 0; i < SINOMO_WASIX_POSIX_TIMER_MAX; i++) {
        struct sinomo_wasix_posix_timer_slot *slot = &sinomo_wasix_posix_timers[i];
        if (!slot->allocated ||
            slot->notify != SIGEV_SIGNAL ||
            slot->signo != sig ||
            !slot->notification_pending) {
            continue;
        }
        int64_t now_ns = 0;
        int error = sinomo_wasix_clock_now_ns(slot->clock_id, &now_ns);
        slot->last_overrun_count =
            error == 0 ? sinomo_wasix_timer_overrun_count_at(slot, now_ns) : 0;
        slot->notification_pending = 0;
    }
    pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);
}

static void sinomo_wasix_freeze_timer_notifications_for_words(uint64_t low, uint64_t high) {
    while (low != 0) {
        int sig = __builtin_ctzll(low) + 1;
        low &= low - 1;
        sinomo_wasix_freeze_timer_notification_for_signal(sig);
    }
    while (high != 0) {
        int sig = __builtin_ctzll(high) + 65;
        high &= high - 1;
        sinomo_wasix_freeze_timer_notification_for_signal(sig);
    }
}

static void *sinomo_wasix_timer_thread_main(void *arg_ptr) {
    struct sinomo_wasix_timer_thread_arg arg = *(struct sinomo_wasix_timer_thread_arg *)arg_ptr;
    free(arg_ptr);

    for (;;) {
        int64_t sleep_ns = sinomo_wasix_timer_duration_ns(arg.value_sec, arg.value_nsec);
        if (sleep_ns <= 0)
            return NULL;
        if (sleep_ns < 1000000LL)
            sleep_ns = 1000000LL;
        if (sinomo_wasix_sleep_relative_ns(sleep_ns, NULL) != 0)
            return NULL;

        struct sinomo_wasix_posix_timer_slot slot = {0};
        if (!sinomo_wasix_timer_slot_snapshot(arg.timer_id, arg.generation, &slot))
            return NULL;

        if (slot.notify == SIGEV_THREAD && slot.notify_function != NULL) {
            slot.notify_function(slot.value);
        } else if (slot.notify == SIGEV_SIGNAL && slot.signo > 0) {
            sigset_t timer_signal_set;
            if (sigemptyset(&timer_signal_set) == 0 &&
                sigaddset(&timer_signal_set, slot.signo) == 0) {
                (void)pthread_sigmask(SIG_BLOCK, &timer_signal_set, NULL);
            }
            int should_signal = 0;
            pthread_mutex_lock(&sinomo_wasix_posix_timer_lock);
            struct sinomo_wasix_posix_timer_slot *live_slot =
                &sinomo_wasix_posix_timers[arg.timer_id - 1];
            if (live_slot->allocated &&
                live_slot->generation == arg.generation &&
                !live_slot->notification_pending) {
                live_slot->notification_pending = 1;
                should_signal = 1;
            }
            pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);
            if (should_signal)
                (void)kill(getpid(), slot.signo);
        }

        if (arg.interval_sec == 0 && arg.interval_nsec == 0)
            return NULL;
        arg.value_sec = arg.interval_sec;
        arg.value_nsec = arg.interval_nsec;
    }
}

static void sinomo_wasix_start_timer_thread(
    uint32_t timer_id,
    unsigned generation,
    const struct itimerspec *new_value
) {
    if (new_value == NULL)
        return;
    if (new_value->it_value.tv_sec == 0 && new_value->it_value.tv_nsec == 0)
        return;

    struct sinomo_wasix_timer_thread_arg *arg = calloc(1, sizeof(*arg));
    if (arg == NULL)
        return;
    arg->timer_id = timer_id;
    arg->generation = generation;
    arg->value_sec = new_value->it_value.tv_sec;
    arg->value_nsec = new_value->it_value.tv_nsec;
    arg->interval_sec = new_value->it_interval.tv_sec;
    arg->interval_nsec = new_value->it_interval.tv_nsec;

    struct sinomo_wasix_posix_timer_slot slot = {0};
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (sinomo_wasix_timer_slot_snapshot(timer_id, generation, &slot) &&
        slot.has_notify_attributes) {
        attr = slot.notify_attributes;
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    }

    pthread_t thread;
    if (pthread_create(&thread, &attr, sinomo_wasix_timer_thread_main, arg) != 0)
        free(arg);
    if (!slot.has_notify_attributes)
        pthread_attr_destroy(&attr);
}

static int sinomo_wasix_sleep_relative_ns(int64_t duration_ns, struct timespec *rem) {
    const int64_t max_chunk_ns = 10000000LL;
    const int64_t fine_chunk_ns = 1000000LL;
    if (duration_ns < 0)
        return EINVAL;
    int64_t start_ns = 0;
    int error = sinomo_wasix_clock_now_ns(CLOCK_MONOTONIC, &start_ns);
    if (error != 0)
        return error;
    int64_t deadline_ns =
        INT64_MAX - start_ns < duration_ns ? INT64_MAX : start_ns + duration_ns;
    for (;;) {
        int64_t now_ns = 0;
        error = sinomo_wasix_clock_now_ns(CLOCK_MONOTONIC, &now_ns);
        if (error != 0)
            return error;
        int64_t remaining = deadline_ns > now_ns ? deadline_ns - now_ns : 0;
        if (remaining <= 0)
            break;
        pthread_testcancel();
        if (remaining <= 5000000LL)
            continue;
        int64_t chunk_ns = remaining <= 50000000LL ? fine_chunk_ns : max_chunk_ns;
        uint64_t chunk = remaining > chunk_ns ? (uint64_t)chunk_ns : (uint64_t)remaining;
        if (sinomo_wasix_publish_before_next_sleep) {
            sinomo_wasix_sync_from_guest_if_shared();
            sinomo_wasix_publish_before_next_sleep = 0;
        }
        sinomo_wasix_sync_to_guest_if_shared();
        int32_t sleep_error = __imported_wasix_thread_sleep(chunk);
        pthread_testcancel();
        sinomo_wasix_sync_to_guest_if_shared();
        if (sleep_error != 0) {
            if (rem != NULL) {
                int64_t interrupted_now_ns = 0;
                int time_error = sinomo_wasix_clock_now_ns(CLOCK_MONOTONIC, &interrupted_now_ns);
                if (time_error == 0)
                    remaining = deadline_ns > interrupted_now_ns ? deadline_ns - interrupted_now_ns : 0;
                rem->tv_sec = remaining / 1000000000LL;
                rem->tv_nsec = remaining % 1000000000LL;
            }
            return sleep_error < 0 ? -sleep_error : sleep_error;
        }
    }
    if (rem != NULL) {
        rem->tv_sec = 0;
        rem->tv_nsec = 0;
    }
    return 0;
}

int nanosleep(const struct timespec *req, struct timespec *rem) {
    int64_t duration_ns = sinomo_wasix_timespec_to_ns(req);
    if (duration_ns < 0) {
        errno = EINVAL;
        return -1;
    }
    int error = sinomo_wasix_sleep_relative_ns(duration_ns, rem);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

int clock_nanosleep(clockid_t clock_id, int flags, const struct timespec *req, struct timespec *rem) {
    if ((flags & ~TIMER_ABSTIME) != 0)
        return EINVAL;
    int64_t target_ns = sinomo_wasix_timespec_to_ns(req);
    if (target_ns < 0)
        return EINVAL;
    int64_t duration_ns = target_ns;
    if ((flags & TIMER_ABSTIME) != 0) {
        int64_t now_ns = 0;
        int error = sinomo_wasix_clock_now_ns(clock_id, &now_ns);
        if (error != 0)
            return error;
        duration_ns = target_ns > now_ns ? target_ns - now_ns : 0;
    } else if (clock_id != CLOCK_REALTIME && clock_id != CLOCK_MONOTONIC) {
        return EINVAL;
    }
    return sinomo_wasix_sleep_relative_ns(duration_ns, rem);
}

unsigned sleep(unsigned seconds) {
    struct timespec req = {
        .tv_sec = (time_t)seconds,
        .tv_nsec = 0,
    };
    struct timespec rem = {0};
    if (nanosleep(&req, &rem) != 0)
        return rem.tv_sec > 0 ? (unsigned)rem.tv_sec : 1;
    return 0;
}

static pthread_mutex_t sinomo_wasix_alarm_lock = PTHREAD_MUTEX_INITIALIZER;
static timer_t sinomo_wasix_alarm_timer;
static int sinomo_wasix_alarm_timer_created;

unsigned alarm(unsigned seconds) {
    pthread_mutex_lock(&sinomo_wasix_alarm_lock);
    if (!sinomo_wasix_alarm_timer_created) {
        struct sigevent event;
        memset(&event, 0, sizeof(event));
        event.sigev_notify = SIGEV_SIGNAL;
        event.sigev_signo = SIGALRM;
        if (timer_create(CLOCK_REALTIME, &event, &sinomo_wasix_alarm_timer) != 0) {
            pthread_mutex_unlock(&sinomo_wasix_alarm_lock);
            return 0;
        }
        sinomo_wasix_alarm_timer_created = 1;
    }

    struct itimerspec old_value;
    struct itimerspec new_value;
    memset(&old_value, 0, sizeof(old_value));
    memset(&new_value, 0, sizeof(new_value));
    new_value.it_value.tv_sec = (time_t)seconds;
    if (timer_settime(sinomo_wasix_alarm_timer, 0, &new_value, &old_value) != 0) {
        pthread_mutex_unlock(&sinomo_wasix_alarm_lock);
        return 0;
    }
    pthread_mutex_unlock(&sinomo_wasix_alarm_lock);
    return old_value.it_value.tv_sec + (old_value.it_value.tv_nsec != 0);
}

int usleep(useconds_t useconds) {
    struct timespec req = {
        .tv_sec = (time_t)(useconds / 1000000u),
        .tv_nsec = (long)(useconds % 1000000u) * 1000L,
    };
    return nanosleep(&req, NULL);
}

int setitimer(int which, const struct itimerval *restrict new_value, struct itimerval *restrict old_value) {
    if (new_value == NULL) {
        errno = EFAULT;
        return -1;
    }
    int64_t old_value_sec = 0;
    int64_t old_value_usec = 0;
    int64_t old_interval_sec = 0;
    int64_t old_interval_usec = 0;
    int32_t ret = __imported_wasix_itimer_settime(
        which,
        new_value->it_value.tv_sec,
        new_value->it_value.tv_usec,
        new_value->it_interval.tv_sec,
        new_value->it_interval.tv_usec,
        &old_value_sec,
        &old_value_usec,
        &old_interval_sec,
        &old_interval_usec);
    if (ret != 0) {
        errno = ret < 0 ? -ret : ret;
        return -1;
    }
    if (old_value != NULL) {
        old_value->it_value.tv_sec = old_value_sec;
        old_value->it_value.tv_usec = old_value_usec;
        old_value->it_interval.tv_sec = old_interval_sec;
        old_value->it_interval.tv_usec = old_interval_usec;
    }
    return 0;
}

int getitimer(int which, struct itimerval *value) {
    if (value == NULL) {
        errno = EFAULT;
        return -1;
    }
    int64_t value_sec = 0;
    int64_t value_usec = 0;
    int64_t interval_sec = 0;
    int64_t interval_usec = 0;
    int32_t ret = __imported_wasix_itimer_gettime(
        which,
        &value_sec,
        &value_usec,
        &interval_sec,
        &interval_usec);
    if (ret != 0) {
        errno = ret < 0 ? -ret : ret;
        return -1;
    }
    value->it_value.tv_sec = value_sec;
    value->it_value.tv_usec = value_usec;
    value->it_interval.tv_sec = interval_sec;
    value->it_interval.tv_usec = interval_usec;
    return 0;
}

int timer_create(clockid_t clock_id, struct sigevent *restrict event, timer_t *restrict timer_id) {
    if (timer_id == NULL) {
        errno = EFAULT;
        return -1;
    }
    int32_t ret = __imported_wasix_posix_timer_create((int32_t)clock_id);
    if (ret < 0) {
        errno = -ret;
        return -1;
    }
    if (ret == 0 || ret > SINOMO_WASIX_POSIX_TIMER_MAX) {
        (void)__imported_wasix_posix_timer_delete((uint32_t)ret);
        errno = EAGAIN;
        return -1;
    }

    pthread_mutex_lock(&sinomo_wasix_posix_timer_lock);
    if (!sinomo_wasix_posix_timer_atexit_registered) {
        if (atexit(sinomo_wasix_disarm_all_posix_timers) == 0)
            sinomo_wasix_posix_timer_atexit_registered = 1;
    }
    struct sinomo_wasix_posix_timer_slot *slot = &sinomo_wasix_posix_timers[ret - 1];
    memset(slot, 0, sizeof(*slot));
    slot->allocated = 1;
    slot->generation++;
    if (slot->generation == 0)
        slot->generation = 1;
    slot->clock_id = clock_id;
    slot->last_overrun_count = -1;
    if (event != NULL) {
        slot->notify = event->sigev_notify;
        slot->signo = event->sigev_signo;
        slot->value = event->sigev_value;
        slot->notify_function = event->sigev_notify_function;
        if (event->sigev_notify == SIGEV_THREAD && event->sigev_notify_attributes != NULL) {
            slot->notify_attributes = *event->sigev_notify_attributes;
            slot->has_notify_attributes = 1;
        }
    } else {
        slot->notify = SIGEV_SIGNAL;
        slot->signo = SIGALRM;
        slot->value.sival_int = ret;
    }
    pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);

    *timer_id = sinomo_wasix_timer_from_id((uint32_t)ret);
    return 0;
}

int timer_settime(timer_t timer_id, int flags, const struct itimerspec *restrict new_value, struct itimerspec *restrict old_value) {
    if (new_value == NULL) {
        errno = EFAULT;
        return -1;
    }
    uint32_t id = sinomo_wasix_timer_id_from_timer(timer_id);
    if (id == 0 || id > SINOMO_WASIX_POSIX_TIMER_MAX) {
        errno = EINVAL;
        return -1;
    }

    clockid_t slot_clock_id = CLOCK_REALTIME;
    pthread_mutex_lock(&sinomo_wasix_posix_timer_lock);
    struct sinomo_wasix_posix_timer_slot *slot = &sinomo_wasix_posix_timers[id - 1];
    if (!slot->allocated) {
        pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);
        errno = EINVAL;
        return -1;
    }
    slot_clock_id = slot->clock_id;
    pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);

    int64_t set_at_ns = 0;
    int time_error = sinomo_wasix_clock_now_ns(slot_clock_id, &set_at_ns);
    if (time_error != 0) {
        errno = time_error;
        return -1;
    }

    int64_t old_value_sec = 0;
    int64_t old_value_nsec = 0;
    int64_t old_interval_sec = 0;
    int64_t old_interval_nsec = 0;
    int32_t ret = __imported_wasix_posix_timer_settime(
        id,
        flags,
        new_value->it_value.tv_sec,
        new_value->it_value.tv_nsec,
        new_value->it_interval.tv_sec,
        new_value->it_interval.tv_nsec,
        old_value != NULL ? &old_value_sec : NULL,
        old_value != NULL ? &old_value_nsec : NULL,
        old_value != NULL ? &old_interval_sec : NULL,
        old_value != NULL ? &old_interval_nsec : NULL);
    if (ret != 0) {
        errno = ret < 0 ? -ret : ret;
        return -1;
    }
    unsigned generation = 0;
    int notify = 0;
    int signo = 0;
    pthread_mutex_lock(&sinomo_wasix_posix_timer_lock);
    slot = &sinomo_wasix_posix_timers[id - 1];
    if (!slot->allocated) {
        pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);
        errno = EINVAL;
        return -1;
    }
    slot->generation++;
    if (slot->generation == 0)
        slot->generation = 1;
    generation = slot->generation;
    notify = slot->notify;
    signo = slot->signo;
    slot_clock_id = slot->clock_id;
    pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);

    if (old_value != NULL) {
        old_value->it_value.tv_sec = old_value_sec;
        old_value->it_value.tv_nsec = old_value_nsec;
        old_value->it_interval.tv_sec = old_interval_sec;
        old_value->it_interval.tv_nsec = old_interval_nsec;
    }
    if (new_value->it_value.tv_sec == 0 && new_value->it_value.tv_nsec == 0) {
        if (notify == SIGEV_SIGNAL && signo > 0) {
            uint64_t high = 0;
            uint64_t low = signal_bit(signo, &high);
            (void)take_signal_from_words(
                &sinomo_wasix_pending_signal_low,
                &sinomo_wasix_pending_signal_high,
                low,
                high);
            (void)__imported_wasix_signal_pending_take(low, high);
        }
        pthread_mutex_lock(&sinomo_wasix_posix_timer_lock);
        slot = &sinomo_wasix_posix_timers[id - 1];
        if (slot->allocated && slot->generation == generation) {
            slot->notification_pending = 0;
            slot->last_overrun_count = -1;
            slot->value_ns = 0;
            slot->interval_ns = 0;
            slot->armed_at_ns = 0;
        }
        pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);
        return 0;
    }

    struct itimerspec armed_value = *new_value;
    if ((flags & TIMER_ABSTIME) != 0) {
        int64_t target_ns = sinomo_wasix_timer_duration_ns(
            new_value->it_value.tv_sec,
            new_value->it_value.tv_nsec);
        if (target_ns < 0) {
            errno = EINVAL;
            return -1;
        }
        int64_t relative_ns = target_ns > set_at_ns ? target_ns - set_at_ns : 1;
        armed_value.it_value.tv_sec = relative_ns / 1000000000LL;
        armed_value.it_value.tv_nsec = relative_ns % 1000000000LL;
    }
    if (armed_value.it_value.tv_sec == 0 && armed_value.it_value.tv_nsec == 0)
        armed_value.it_value.tv_nsec = 1;
    int64_t armed_value_ns = sinomo_wasix_timer_duration_ns(
        armed_value.it_value.tv_sec,
        armed_value.it_value.tv_nsec);
    int64_t armed_interval_ns = sinomo_wasix_timer_duration_ns(
        armed_value.it_interval.tv_sec,
        armed_value.it_interval.tv_nsec);
    int64_t armed_at_ns = 0;
    time_error = sinomo_wasix_clock_now_ns(slot_clock_id, &armed_at_ns);
    if (time_error != 0) {
        errno = time_error;
        return -1;
    }
    pthread_mutex_lock(&sinomo_wasix_posix_timer_lock);
    slot = &sinomo_wasix_posix_timers[id - 1];
    if (slot->allocated && slot->generation == generation) {
        slot->armed_at_ns = armed_at_ns;
        slot->value_ns = armed_value_ns;
        slot->interval_ns = armed_interval_ns;
        slot->notification_pending = 0;
        slot->last_overrun_count = -1;
    }
    pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);
    sinomo_wasix_start_timer_thread(id, generation, &armed_value);
    return 0;
}

int timer_gettime(timer_t timer_id, struct itimerspec *value) {
    if (value == NULL) {
        errno = EFAULT;
        return -1;
    }
    uint32_t id = sinomo_wasix_timer_id_from_timer(timer_id);
    if (id == 0 || id > SINOMO_WASIX_POSIX_TIMER_MAX) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_wasix_posix_timer_slot slot = {0};
    pthread_mutex_lock(&sinomo_wasix_posix_timer_lock);
    struct sinomo_wasix_posix_timer_slot *live_slot = &sinomo_wasix_posix_timers[id - 1];
    if (live_slot->allocated)
        slot = *live_slot;
    pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);
    if (!slot.allocated) {
        errno = EINVAL;
        return -1;
    }
    if (slot.value_ns > 0) {
        int64_t now_ns = 0;
        int error = sinomo_wasix_clock_now_ns(slot.clock_id, &now_ns);
        if (error != 0) {
            errno = error;
            return -1;
        }
        int64_t remaining_ns = 0;
        int64_t elapsed_ns = now_ns - slot.armed_at_ns;
        if (elapsed_ns < slot.value_ns) {
            remaining_ns = slot.value_ns - elapsed_ns;
        } else if (slot.interval_ns > 0) {
            int64_t since_interval_start = (elapsed_ns - slot.value_ns) % slot.interval_ns;
            remaining_ns = since_interval_start == 0
                ? slot.interval_ns
                : slot.interval_ns - since_interval_start;
        }
        value->it_value.tv_sec = remaining_ns / 1000000000LL;
        value->it_value.tv_nsec = remaining_ns % 1000000000LL;
        value->it_interval.tv_sec = slot.interval_ns / 1000000000LL;
        value->it_interval.tv_nsec = slot.interval_ns % 1000000000LL;
        return 0;
    }
    int64_t value_sec = 0;
    int64_t value_nsec = 0;
    int64_t interval_sec = 0;
    int64_t interval_nsec = 0;
    int32_t ret = __imported_wasix_posix_timer_gettime(
        id,
        &value_sec,
        &value_nsec,
        &interval_sec,
        &interval_nsec);
    if (ret != 0) {
        errno = ret < 0 ? -ret : ret;
        return -1;
    }
    value->it_value.tv_sec = value_sec;
    value->it_value.tv_nsec = value_nsec;
    value->it_interval.tv_sec = interval_sec;
    value->it_interval.tv_nsec = interval_nsec;
    return 0;
}

int timer_getoverrun(timer_t timer_id) {
    uint32_t id = sinomo_wasix_timer_id_from_timer(timer_id);
    if (id == 0 || id > SINOMO_WASIX_POSIX_TIMER_MAX) {
        errno = EINVAL;
        return -1;
    }
    struct sinomo_wasix_posix_timer_slot slot = {0};
    pthread_mutex_lock(&sinomo_wasix_posix_timer_lock);
    struct sinomo_wasix_posix_timer_slot *live_slot = &sinomo_wasix_posix_timers[id - 1];
    if (live_slot->allocated)
        slot = *live_slot;
    pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);
    if (!slot.allocated) {
        errno = EINVAL;
        return -1;
    }
    pthread_mutex_lock(&sinomo_wasix_posix_timer_lock);
    live_slot = &sinomo_wasix_posix_timers[id - 1];
    if (live_slot->allocated) {
        if (live_slot->last_overrun_count >= 0) {
            int64_t overruns = live_slot->last_overrun_count;
            live_slot->last_overrun_count = -1;
            pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);
            if (overruns > INT_MAX)
                return INT_MAX;
            return (int)overruns;
        }
        live_slot->notification_pending = 0;
    }
    pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);
    if (slot.value_ns > 0 && slot.interval_ns > 0) {
        int64_t now_ns = 0;
        int error = sinomo_wasix_clock_now_ns(slot.clock_id, &now_ns);
        if (error != 0) {
            errno = error;
            return -1;
        }
        int64_t overruns = sinomo_wasix_timer_overrun_count_at(&slot, now_ns);
        if (overruns > INT_MAX)
            return INT_MAX;
        return (int)overruns;
    }
    int32_t ret = __imported_wasix_posix_timer_getoverrun(id);
    if (ret < 0) {
        errno = -ret;
        return -1;
    }
    return ret;
}

int timer_delete(timer_t timer_id) {
    uint32_t id = sinomo_wasix_timer_id_from_timer(timer_id);
    int32_t ret = __imported_wasix_posix_timer_delete(id);
    if (ret != 0) {
        errno = ret < 0 ? -ret : ret;
        return -1;
    }
    if (id > 0 && id <= SINOMO_WASIX_POSIX_TIMER_MAX) {
        pthread_mutex_lock(&sinomo_wasix_posix_timer_lock);
        struct sinomo_wasix_posix_timer_slot *slot = &sinomo_wasix_posix_timers[id - 1];
        slot->allocated = 0;
        slot->generation++;
        pthread_mutex_unlock(&sinomo_wasix_posix_timer_lock);
    }
    return 0;
}

mqd_t mq_open(const char *name, int flags, ...) {
    if (name == NULL) {
        errno = EFAULT;
        return (mqd_t)-1;
    }

    mode_t mode = 0;
    const struct mq_attr *attr = NULL;
    if ((flags & O_CREAT) != 0) {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        attr = va_arg(ap, const struct mq_attr *);
        va_end(ap);
    }

    uint64_t max_messages = attr != NULL ? (uint64_t)attr->mq_maxmsg : 0;
    uint64_t message_size = attr != NULL ? (uint64_t)attr->mq_msgsize : 0;
    int32_t ret = __imported_wasix_mqueue_open(
        name,
        (uintptr_t)strlen(name),
        flags,
        (flags & O_CREAT) != 0,
        (flags & O_EXCL) != 0,
        attr != NULL,
        max_messages,
        message_size);
    (void)mode;
    if (ret < 0) {
        errno = -ret;
        return (mqd_t)-1;
    }
    return (mqd_t)ret;
}

int mq_send(mqd_t mqdes, const char *msg_ptr, size_t msg_len, unsigned msg_prio) {
    if (msg_ptr == NULL && msg_len != 0) {
        errno = EFAULT;
        return -1;
    }
    int32_t ret = __imported_wasix_mqueue_send((int32_t)mqdes, msg_ptr, (uintptr_t)msg_len, msg_prio);
    if (ret != 0) {
        errno = ret < 0 ? -ret : ret;
        return -1;
    }
    return 0;
}

ssize_t mq_receive(mqd_t mqdes, char *msg_ptr, size_t msg_len, unsigned *msg_prio) {
    if (msg_ptr == NULL) {
        errno = EFAULT;
        return -1;
    }
    int32_t ret = __imported_wasix_mqueue_receive((int32_t)mqdes, msg_ptr, (uintptr_t)msg_len, msg_prio);
    if (ret < 0) {
        errno = -ret;
        return -1;
    }
    return (ssize_t)ret;
}

int mq_getattr(mqd_t mqdes, struct mq_attr *attr) {
    if (attr == NULL) {
        errno = EFAULT;
        return -1;
    }
    int64_t flags = 0;
    int64_t max_messages = 0;
    int64_t message_size = 0;
    int64_t current_messages = 0;
    int32_t ret = __imported_wasix_mqueue_getattr(
        (int32_t)mqdes,
        &flags,
        &max_messages,
        &message_size,
        &current_messages);
    if (ret != 0) {
        errno = ret < 0 ? -ret : ret;
        return -1;
    }
    memset(attr, 0, sizeof(*attr));
    attr->mq_flags = (long)flags;
    attr->mq_maxmsg = (long)max_messages;
    attr->mq_msgsize = (long)message_size;
    attr->mq_curmsgs = (long)current_messages;
    return 0;
}

int mq_close(mqd_t mqdes) {
    int32_t ret = __imported_wasix_mqueue_close((int32_t)mqdes);
    if (ret != 0) {
        errno = ret < 0 ? -ret : ret;
        return -1;
    }
    return 0;
}

int mq_unlink(const char *name) {
    if (name == NULL) {
        errno = EFAULT;
        return -1;
    }
    int32_t ret = __imported_wasix_mqueue_unlink(name, (uintptr_t)strlen(name));
    if (ret != 0) {
        errno = ret < 0 ? -ret : ret;
        return -1;
    }
    return 0;
}

int mq_setattr(mqd_t mqdes, const struct mq_attr *restrict newattr, struct mq_attr *restrict oldattr) {
    if (oldattr != NULL && mq_getattr(mqdes, oldattr) != 0)
        return -1;
    if (newattr == NULL)
        return 0;
    int32_t ret = __imported_wasix_mqueue_setattr((int32_t)mqdes, (int64_t)newattr->mq_flags);
    if (ret != 0) {
        errno = ret < 0 ? -ret : ret;
        return -1;
    }
    return 0;
}

int mq_notify(mqd_t mqdes, const struct sigevent *event) {
    uint32_t signal = 0;
    int32_t has_notification = event != NULL;
    if (event != NULL) {
        if (event->sigev_notify != SIGEV_SIGNAL) {
            errno = ENOTSUP;
            return -1;
        }
        signal = (uint32_t)event->sigev_signo;
    }
    int32_t ret = __imported_wasix_mqueue_notify((int32_t)mqdes, has_notification, signal);
    if (ret != 0) {
        errno = ret < 0 ? -ret : ret;
        return -1;
    }
    return 0;
}

ssize_t mq_timedreceive(mqd_t mqdes, char *restrict msg_ptr, size_t msg_len, unsigned *restrict msg_prio, const struct timespec *restrict timeout) {
    if (msg_ptr == NULL || timeout == NULL) {
        errno = EFAULT;
        return -1;
    }
    int32_t ret = __imported_wasix_mqueue_timedreceive(
        (int32_t)mqdes,
        msg_ptr,
        (uintptr_t)msg_len,
        msg_prio,
        (int64_t)timeout->tv_sec,
        (int64_t)timeout->tv_nsec);
    if (ret < 0) {
        errno = -ret;
        return -1;
    }
    return (ssize_t)ret;
}

int mq_timedsend(mqd_t mqdes, const char *msg_ptr, size_t msg_len, unsigned msg_prio, const struct timespec *timeout) {
    if ((msg_ptr == NULL && msg_len != 0) || timeout == NULL) {
        errno = EFAULT;
        return -1;
    }
    int32_t ret = __imported_wasix_mqueue_timedsend(
        (int32_t)mqdes,
        msg_ptr,
        (uintptr_t)msg_len,
        msg_prio,
        (int64_t)timeout->tv_sec,
        (int64_t)timeout->tv_nsec);
    if (ret != 0) {
        errno = ret < 0 ? -ret : ret;
        return -1;
    }
    return 0;
}

int waitpid(pid_t pid, int *wstatus, int options) {
    __wasi_option_pid_t option_pid = {0};
    __wasi_join_flags_t flags = 0;
    __wasi_join_status_t status = {0};

    if (pid == -1) {
        option_pid.tag = __WASI_OPTION_NONE;
    } else if (pid > 0) {
        option_pid.tag = __WASI_OPTION_SOME;
        option_pid.u.some = (__wasi_pid_t)pid;
    } else {
        errno = EINVAL;
        return -1;
    }

    if ((options & ~(WNOHANG | WUNTRACED)) != 0) {
        errno = EINVAL;
        return -1;
    }
    if ((options & WNOHANG) != 0)
        flags |= __WASI_JOIN_FLAGS_NON_BLOCKING;
    if ((options & WUNTRACED) != 0)
        flags |= __WASI_JOIN_FLAGS_WAKE_STOPPED;

    SINOMO_PTHREAD_ENV_LOG("sinomo-debug waitpid enter pid=%d options=%d cross=%u\n",
        (int)pid, options, __sinomo_wasix_shared_memory_is_cross_process());
    sinomo_wasix_sync_from_guest_if_shared();
    __wasi_errno_t error = __wasi_proc_join(&option_pid, flags, &status);
    sinomo_wasix_sync_to_guest_if_shared();
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug waitpid result error=%u tag=%u pid_tag=%u pid=%u\n",
        (unsigned)error, (unsigned)status.tag, (unsigned)option_pid.tag, (unsigned)option_pid.u.some);
    if (error != 0) {
        errno = error;
        return -1;
    }

    if (status.tag == __WASI_JOIN_STATUS_TYPE_NOTHING)
        return 0;
    if (option_pid.tag != __WASI_OPTION_SOME) {
        errno = ECHILD;
        return -1;
    }

    if (wstatus != NULL) {
        switch (status.tag) {
        case __WASI_JOIN_STATUS_TYPE_EXIT_NORMAL:
            *wstatus = ((int)status.u.exit_normal & 0xff) << 8;
            break;
        case __WASI_JOIN_STATUS_TYPE_EXIT_SIGNAL:
            *wstatus = (int)status.u.exit_signal.signal & 0x7f;
            break;
        case __WASI_JOIN_STATUS_TYPE_STOPPED:
            *wstatus = (((int)status.u.stopped & 0xff) << 8) | 0x7f;
            break;
        default:
            *wstatus = 0;
            break;
        }
    }
    return (pid_t)option_pid.u.some;
}

pid_t wait(int *wstatus) {
    return waitpid(-1, wstatus, 0);
}

int pthread_cancel(pthread_t thread) {
    if (thread == NULL)
        return ESRCH;

    struct sinomo_wasix_pthread_state *target = (struct sinomo_wasix_pthread_state *)thread;
    if (target->tid <= 0)
        return ESRCH;

    __atomic_store_n(&target->cancel, 1, __ATOMIC_SEQ_CST);
    sinomo_wasix_sync_range_from_guest_if_shared((const void *)&target->cancel, sizeof(target->cancel));
    SINOMO_PTHREAD_LOG("sinomo-debug pthread_cancel target=%p tid=%d disable=%u async=%u\n",
        (void *)thread, target->tid, (unsigned)target->canceldisable, (unsigned)target->cancelasync);

    if (target == __wasilibc_pthread_self &&
        target->canceldisable == PTHREAD_CANCEL_ENABLE &&
        target->cancelasync) {
        pthread_exit(PTHREAD_CANCELED);
    }

    int32_t error = __imported_wasix_thread_signal((uint32_t)target->tid, (uint32_t)SIGCANCEL);
    return error < 0 ? -error : error;
}

int pthread_kill(pthread_t thread, int sig) {
    if (thread == NULL)
        return ESRCH;
    if (sig < 0 || sig >= NSIG)
        return EINVAL;

    struct sinomo_wasix_pthread_state *target = (struct sinomo_wasix_pthread_state *)thread;
    if (target->tid <= 0)
        return ESRCH;
    if (sig == 0)
        return 0;

    int32_t error = __imported_wasix_thread_signal((uint32_t)target->tid, (uint32_t)sig);
    return error < 0 ? -error : error;
}

#define SINOMO_SPIN_UNLOCKED 0
#define SINOMO_SPIN_LOCKED EBUSY
#define SINOMO_SPIN_INVALID 0x7ffffffe

int pthread_spin_init(pthread_spinlock_t *lock, int pshared) {
    if (lock == NULL)
        return EINVAL;
    if (pshared != PTHREAD_PROCESS_PRIVATE && pshared != PTHREAD_PROCESS_SHARED)
        return EINVAL;
    __atomic_store_n((int *)lock, SINOMO_SPIN_UNLOCKED, __ATOMIC_SEQ_CST);
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug spin_init lock=%p pshared=%d state=%d cross=%u\n",
        (void *)lock, pshared, __atomic_load_n((int *)lock, __ATOMIC_SEQ_CST), __sinomo_wasix_shared_memory_is_cross_process());
    if (pshared == PTHREAD_PROCESS_SHARED)
        (void)__sinomo_wasix_shared_futex_hint((uintptr_t)lock, (uintptr_t)sizeof(*lock));
    sinomo_wasix_sync_range_from_guest_if_shared(lock, sizeof(*lock));
    sinomo_wasix_publish_before_next_sleep = 1;
    return 0;
}

int pthread_spin_destroy(pthread_spinlock_t *lock) {
    if (lock == NULL)
        return EINVAL;
    __atomic_store_n((int *)lock, SINOMO_SPIN_INVALID, __ATOMIC_SEQ_CST);
    (void)__sinomo_wasix_shared_futex_unhint((uintptr_t)lock, (uintptr_t)sizeof(*lock));
    sinomo_wasix_sync_range_from_guest_if_shared(lock, sizeof(*lock));
    return 0;
}

int pthread_spin_trylock(pthread_spinlock_t *lock) {
    if (lock == NULL)
        return EINVAL;
    sinomo_wasix_sync_range_to_guest_if_shared(lock, sizeof(*lock));
    int expected = SINOMO_SPIN_UNLOCKED;
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug spin_trylock before lock=%p state=%d cross=%u\n",
        (void *)lock, __atomic_load_n((int *)lock, __ATOMIC_SEQ_CST), __sinomo_wasix_shared_memory_is_cross_process());
    if (__atomic_compare_exchange_n((int *)lock, &expected, SINOMO_SPIN_LOCKED, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        sinomo_wasix_sync_range_from_guest_if_shared(lock, sizeof(*lock));
        sinomo_wasix_publish_before_next_sleep = 1;
        SINOMO_PTHREAD_ENV_LOG("sinomo-debug spin_trylock acquired lock=%p state=%d\n",
            (void *)lock, __atomic_load_n((int *)lock, __ATOMIC_SEQ_CST));
        return 0;
    }
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug spin_trylock busy lock=%p expected=%d state=%d\n",
        (void *)lock, expected, __atomic_load_n((int *)lock, __ATOMIC_SEQ_CST));
    if (expected == SINOMO_SPIN_INVALID)
        return EINVAL;
    sinomo_wasix_publish_before_next_sleep = 1;
    return EBUSY;
}

int pthread_spin_lock(pthread_spinlock_t *lock) {
    if (lock == NULL)
        return EINVAL;
    for (;;) {
        int ret = pthread_spin_trylock(lock);
        if (ret == 0 || ret == EINVAL)
            return ret;
        int32_t sleep_ret = __imported_wasix_thread_sleep(1000000ULL);
        sinomo_wasix_sync_to_guest_if_shared();
        if (sleep_ret != 0 && sleep_ret != EINTR && sleep_ret != ECANCELED)
            return sleep_ret < 0 ? -sleep_ret : sleep_ret;
        pthread_testcancel();
    }
}

int pthread_spin_unlock(pthread_spinlock_t *lock) {
    if (lock == NULL)
        return EINVAL;
    int state = __atomic_load_n((int *)lock, __ATOMIC_SEQ_CST);
    if (state == SINOMO_SPIN_INVALID)
        return EINVAL;
    __atomic_store_n((int *)lock, SINOMO_SPIN_UNLOCKED, __ATOMIC_SEQ_CST);
    SINOMO_PTHREAD_ENV_LOG("sinomo-debug spin_unlock lock=%p old=%d state=%d cross=%u\n",
        (void *)lock, state, __atomic_load_n((int *)lock, __ATOMIC_SEQ_CST), __sinomo_wasix_shared_memory_is_cross_process());
    sinomo_wasix_sync_range_from_guest_if_shared(lock, sizeof(*lock));
    sinomo_wasix_publish_before_next_sleep = 1;
    (void)__wasilibc_futex_wake_wasix((int *)lock, INT_MAX);
    return 0;
}

static uint64_t signal_bit(int sig, uint64_t *high) {
    if (high != NULL)
        *high = 0;
    if (sig <= 0 || sig > 128)
        return 0;
    if (sig <= 64)
        return UINT64_C(1) << (sig - 1);
    if (high != NULL)
        *high = UINT64_C(1) << (sig - 65);
    return 0;
}

static void sigset_to_words(const sigset_t *set, uint64_t *low, uint64_t *high) {
    *low = 0;
    *high = 0;
    if (set == NULL)
        return;
    memcpy(low, set, sizeof(*low) <= sizeof(*set) ? sizeof(*low) : sizeof(*set));
    if (sizeof(*set) > sizeof(*low)) {
        size_t high_bytes = sizeof(*set) - sizeof(*low);
        if (high_bytes > sizeof(*high))
            high_bytes = sizeof(*high);
        memcpy(high, ((const unsigned char *)set) + sizeof(*low), high_bytes);
    }
}

static void clear_signal_from_words(uint64_t *low, uint64_t *high, int sig) {
    uint64_t bit_high = 0;
    uint64_t bit_low = signal_bit(sig, &bit_high);
    *low &= ~bit_low;
    *high &= ~bit_high;
}

static int take_signal_from_words(uint64_t *low, uint64_t *high, uint64_t mask_low, uint64_t mask_high) {
    uint64_t matched_low = *low & mask_low;
    if (matched_low != 0) {
        int sig = __builtin_ctzll(matched_low) + 1;
        *low &= ~(UINT64_C(1) << (sig - 1));
        return sig;
    }
    uint64_t matched_high = *high & mask_high;
    if (matched_high != 0) {
        int sig = __builtin_ctzll(matched_high) + 65;
        *high &= ~(UINT64_C(1) << (sig - 65));
        return sig;
    }
    return 0;
}

static void clear_unmaskable_signals(uint64_t *low, uint64_t *high) {
    clear_signal_from_words(low, high, SIGKILL);
    clear_signal_from_words(low, high, SIGSTOP);
}

static int deliver_pending_unblocked(uint64_t old_low, uint64_t old_high, uint64_t new_low, uint64_t new_high) {
    uint64_t unblocked_low = old_low & ~new_low;
    uint64_t unblocked_high = old_high & ~new_high;
    uint64_t deliver_low = sinomo_wasix_pending_signal_low & unblocked_low;
    uint64_t deliver_high = sinomo_wasix_pending_signal_high & unblocked_high;

    sinomo_wasix_pending_signal_low &= ~deliver_low;
    sinomo_wasix_pending_signal_high &= ~deliver_high;

    __wasi_tid_t tid = 0;
    int32_t error = __wasi_thread_id(&tid);
    if (error != 0)
        return error < 0 ? -error : error;

    while (deliver_low != 0) {
        int sig = __builtin_ctzll(deliver_low) + 1;
        deliver_low &= deliver_low - 1;
        sinomo_wasix_freeze_timer_notification_for_signal(sig);
        error = __imported_wasix_thread_signal((uint32_t)tid, (uint32_t)sig);
        if (error != 0)
            return error < 0 ? -error : error;
    }
    while (deliver_high != 0) {
        int sig = __builtin_ctzll(deliver_high) + 65;
        deliver_high &= deliver_high - 1;
        sinomo_wasix_freeze_timer_notification_for_signal(sig);
        error = __imported_wasix_thread_signal((uint32_t)tid, (uint32_t)sig);
        if (error != 0)
            return error < 0 ? -error : error;
    }
    return 0;
}

static void words_to_sigset(sigset_t *set, uint64_t low, uint64_t high) {
    if (set == NULL)
        return;
    memset(set, 0, sizeof(*set));
    memcpy(set, &low, sizeof(low) <= sizeof(*set) ? sizeof(low) : sizeof(*set));
    if (sizeof(*set) > sizeof(low)) {
        size_t high_bytes = sizeof(*set) - sizeof(low);
        if (high_bytes > sizeof(high))
            high_bytes = sizeof(high);
        memcpy(((unsigned char *)set) + sizeof(low), &high, high_bytes);
    }
}

int pthread_sigmask(int how, const sigset_t *restrict set, sigset_t *restrict oldset) {
    uint64_t old_low = 0;
    uint64_t old_high = 0;
    int32_t error = __imported_wasix_signal_mask_get(&old_low, &old_high);
    if (error != 0)
        return error < 0 ? -error : error;
    words_to_sigset(oldset, old_low, old_high);
    if (set == NULL)
        return 0;

    uint64_t set_low = 0;
    uint64_t set_high = 0;
    sigset_to_words(set, &set_low, &set_high);
    clear_unmaskable_signals(&set_low, &set_high);
    uint64_t new_low = old_low;
    uint64_t new_high = old_high;
    switch (how) {
    case SIG_BLOCK:
        new_low |= set_low;
        new_high |= set_high;
        break;
    case SIG_UNBLOCK:
        new_low &= ~set_low;
        new_high &= ~set_high;
        break;
    case SIG_SETMASK:
        new_low = set_low;
        new_high = set_high;
        break;
    default:
        return EINVAL;
    }
    clear_unmaskable_signals(&new_low, &new_high);
    uint64_t newly_unblocked_low = old_low & ~new_low;
    uint64_t newly_unblocked_high = old_high & ~new_high;
    clear_unmaskable_signals(&newly_unblocked_low, &newly_unblocked_high);
    sinomo_wasix_freeze_timer_notifications_for_words(
        newly_unblocked_low,
        newly_unblocked_high);
    error = __imported_wasix_signal_mask_set(new_low, new_high);
    if (error != 0)
        return error < 0 ? -error : error;
    return deliver_pending_unblocked(old_low, old_high, new_low, new_high);
}

int sigprocmask(int how, const sigset_t *restrict set, sigset_t *restrict oldset) {
    int error = pthread_sigmask(how, set, oldset);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

int sigpending(sigset_t *set) {
    if (set == NULL) {
        errno = EFAULT;
        return -1;
    }
    uint64_t runtime_low = 0;
    uint64_t runtime_high = 0;
    int32_t error = __imported_wasix_signal_pending_get(&runtime_low, &runtime_high);
    if (error != 0) {
        errno = error < 0 ? -error : error;
        return -1;
    }
    words_to_sigset(
        set,
        sinomo_wasix_pending_signal_low | runtime_low,
        sinomo_wasix_pending_signal_high | runtime_high);
    return 0;
}

int sigtimedwait(const sigset_t *restrict mask, siginfo_t *restrict si, const struct timespec *restrict timeout) {
    if (mask == NULL) {
        errno = EFAULT;
        return -1;
    }
    if (timeout != NULL && (timeout->tv_nsec < 0 || timeout->tv_nsec >= 1000000000L)) {
        errno = EINVAL;
        return -1;
    }

    uint64_t mask_low = 0;
    uint64_t mask_high = 0;
    sigset_to_words(mask, &mask_low, &mask_high);
    if (mask_low == 0 && mask_high == 0) {
        errno = EAGAIN;
        return -1;
    }

    uint64_t old_mask_low = 0;
    uint64_t old_mask_high = 0;
    int32_t mask_error = __imported_wasix_signal_mask_get(&old_mask_low, &old_mask_high);
    int restore_mask = 0;
    if (mask_error != 0) {
        errno = mask_error < 0 ? -mask_error : mask_error;
        return -1;
    }
    uint64_t wait_mask_low = old_mask_low | mask_low;
    uint64_t wait_mask_high = old_mask_high | mask_high;
    clear_unmaskable_signals(&wait_mask_low, &wait_mask_high);
    if (wait_mask_low != old_mask_low || wait_mask_high != old_mask_high) {
        mask_error = __imported_wasix_signal_mask_set(wait_mask_low, wait_mask_high);
        if (mask_error != 0) {
            errno = mask_error < 0 ? -mask_error : mask_error;
            return -1;
        }
        restore_mask = 1;
    }

    int64_t timeout_ns = -1;
    int64_t deadline_ns = 0;
    if (timeout != NULL) {
        timeout_ns = sinomo_wasix_timespec_to_ns(timeout);
        if (timeout_ns < 0) {
            if (restore_mask)
                (void)__imported_wasix_signal_mask_set(old_mask_low, old_mask_high);
            errno = EINVAL;
            return -1;
        }
        int error = sinomo_wasix_clock_now_ns(CLOCK_MONOTONIC, &deadline_ns);
        if (error != 0) {
            if (restore_mask)
                (void)__imported_wasix_signal_mask_set(old_mask_low, old_mask_high);
            errno = error;
            return -1;
        }
        if (INT64_MAX - deadline_ns < timeout_ns)
            deadline_ns = INT64_MAX;
        else
            deadline_ns += timeout_ns;
    }

    for (;;) {
        int sig = take_signal_from_words(
            &sinomo_wasix_pending_signal_low,
            &sinomo_wasix_pending_signal_high,
            mask_low,
            mask_high);
        if (sig == 0) {
            int32_t taken = __imported_wasix_signal_pending_take(mask_low, mask_high);
            if (taken < 0) {
                errno = -taken;
                return -1;
            }
            sig = taken;
        }
        if (sig > 0) {
            if (si != NULL) {
                memset(si, 0, sizeof(*si));
                si->si_signo = sig;
            }
            sinomo_wasix_freeze_timer_notification_for_signal(sig);
            if (restore_mask)
                (void)__imported_wasix_signal_mask_set(old_mask_low, old_mask_high);
            return sig;
        }

        int64_t sleep_ns = 10000000LL;
        if (timeout != NULL) {
            int64_t now_ns = 0;
            int error = sinomo_wasix_clock_now_ns(CLOCK_MONOTONIC, &now_ns);
            if (error != 0) {
                if (restore_mask)
                    (void)__imported_wasix_signal_mask_set(old_mask_low, old_mask_high);
                errno = error;
                return -1;
            }
            if (now_ns >= deadline_ns) {
                if (restore_mask)
                    (void)__imported_wasix_signal_mask_set(old_mask_low, old_mask_high);
                errno = EAGAIN;
                return -1;
            }
            int64_t remaining_ns = deadline_ns - now_ns;
            if (sleep_ns > remaining_ns)
                sleep_ns = remaining_ns;
        }

        int error = sinomo_wasix_sleep_relative_ns(sleep_ns, NULL);
        if (error != 0 && error != EINTR && error != ECANCELED) {
            if (restore_mask)
                (void)__imported_wasix_signal_mask_set(old_mask_low, old_mask_high);
            errno = error;
            return -1;
        }
    }
}

int raise(int sig) {
    uint64_t high_bit = 0;
    uint64_t low_bit = signal_bit(sig, &high_bit);
    if (low_bit == 0 && high_bit == 0) {
        errno = EINVAL;
        return -1;
    }

    uint64_t mask_low = 0;
    uint64_t mask_high = 0;
    int32_t error = __imported_wasix_signal_mask_get(&mask_low, &mask_high);
    if (error != 0) {
        errno = error < 0 ? -error : error;
        return -1;
    }
    if ((mask_low & low_bit) != 0 || (mask_high & high_bit) != 0) {
        sinomo_wasix_pending_signal_low |= low_bit;
        sinomo_wasix_pending_signal_high |= high_bit;
        return 0;
    }

    __wasi_tid_t tid = 0;
    error = __wasi_thread_id(&tid);
    if (error != 0) {
        errno = error < 0 ? -error : error;
        return -1;
    }
    error = __imported_wasix_thread_signal((uint32_t)tid, (uint32_t)sig);
    if (error != 0) {
        errno = error < 0 ? -error : error;
        return -1;
    }
    return 0;
}

int kill(pid_t pid, int sig) {
    if (sig < 0 || sig >= NSIG) {
        errno = EINVAL;
        return -1;
    }
    __wasi_errno_t error = __wasi_proc_signal((__wasi_pid_t)pid, (__wasi_signal_t)sig);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

int chroot(const char *path) {
    (void)path;
    errno = ENOSYS;
    return -1;
}

int open(const char *path, int oflag, ...) {
    mode_t mode = 0666;
    if ((oflag & O_CREAT) != 0) {
        va_list ap;
        va_start(ap, oflag);
        mode = va_arg(ap, mode_t);
        va_end(ap);
    }

    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }

    struct stat before;
    int created = ((oflag & O_CREAT) != 0) &&
        (__wasilibc_nocwd_fstatat(dirfd, relative_path, &before, 0) != 0 && errno == ENOENT);

    int fd = __wasilibc_nocwd_openat_nomode(dirfd, relative_path, oflag);
    if (fd < 0)
        return fd;
    if ((oflag & O_CREAT) != 0) {
        if (fchmod(fd, mode & ~sinomo_wasix_umask) != 0 ||
            fchown(fd, __sinomo_wasix_geteuid(), __sinomo_wasix_getegid()) != 0) {
            close(fd);
            return -1;
        }
    }
    if (created)
        touchat_guest(dirfd, relative_path, 1, 1);
    else if ((oflag & O_TRUNC) != 0)
        touchat_guest(dirfd, relative_path, 1, 0);
    return fd;
}

int __wasilibc_open_nomode(const char *path, int oflag) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_openat_nomode(dirfd, relative_path, oflag);
}

int access(const char *path, int amode) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_faccessat(dirfd, relative_path, amode, 0);
}

ssize_t readlink(const char *restrict path, char *restrict buf, size_t bufsize) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __sinomo_wasix_readlinkat(dirfd, relative_path, buf, bufsize);
}

int stat(const char *restrict path, struct stat *restrict buf) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_fstatat(dirfd, relative_path, buf, 0);
}

int lstat(const char *restrict path, struct stat *restrict buf) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_fstatat(dirfd, relative_path, buf, AT_SYMLINK_NOFOLLOW);
}

int utime(const char *path, const struct utimbuf *times) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_utimensat(dirfd, relative_path,
                                      times ? ((struct timespec[2]){{.tv_sec = times->actime},
                                                                    {.tv_sec = times->modtime}})
                                            : NULL,
                                      0);
}

int utimes(const char *path, const struct timeval times[2]) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_utimensat(
        dirfd, relative_path,
        times ? ((struct timespec[2]){{.tv_sec = times[0].tv_sec, .tv_nsec = times[0].tv_usec * 1000},
                                      {.tv_sec = times[1].tv_sec, .tv_nsec = times[1].tv_usec * 1000}})
              : NULL,
        0);
}

int settimeofday(const struct timeval *tv, const struct timezone *tz) {
    (void)tv;
    (void)tz;
    errno = EPERM;
    return -1;
}

int unlink(const char *path) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    if (__sinomo_wasix_unlink_symlinkat(dirfd, relative_path) == 0)
        return 0;
    if (errno != ENOENT)
        return -1;
    return __wasilibc_nocwd___wasilibc_unlinkat(dirfd, relative_path);
}

int rmdir(const char *path) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd___wasilibc_rmdirat(dirfd, relative_path);
}

int remove(const char *path) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    int r = __wasilibc_nocwd___wasilibc_unlinkat(dirfd, relative_path);
    if (r != 0 && (errno == EISDIR || errno == ENOENT)) {
        r = __wasilibc_nocwd___wasilibc_rmdirat(dirfd, relative_path);
        if (errno == ENOTDIR)
            errno = ENOENT;
    }
    return r;
}

int mkdir(const char *path, mode_t mode) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    int ret = __wasilibc_nocwd_mkdirat_nomode(dirfd, relative_path);
    if (ret != 0)
        return ret;
    if (chmodat_guest(dirfd, relative_path, mode & ~sinomo_wasix_umask) != 0)
        return -1;
    if (chownat_guest(dirfd, relative_path, __sinomo_wasix_geteuid(), __sinomo_wasix_getegid()) != 0)
        return -1;
    touchat_guest(dirfd, relative_path, 1, 1);
    return 0;
}

int mknod(const char *path, mode_t mode, dev_t dev) {
    (void)dev;
    if (S_ISREG(mode) || (mode & S_IFMT) == 0) {
        int fd = open(path, O_CREAT | O_EXCL | O_WRONLY, mode & 07777);
        if (fd < 0)
            return -1;
        return close(fd);
    }
    errno = ENOSYS;
    return -1;
}

mode_t umask(mode_t mode) {
    mode_t previous = sinomo_wasix_umask;
    sinomo_wasix_umask = mode & 0777;
    return previous;
}

int chmod(const char *path, mode_t mode) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    int ret = chmodat_guest(dirfd, relative_path, mode);
    if (ret == 0)
        touchat_guest(dirfd, relative_path, 1, 0);
    return ret;
}

int fchmod(int fd, mode_t mode) {
    int32_t error = __imported_wasix_fd_chmod(fd, mode & 07777);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

int fchmodat(int fd, const char *path, mode_t mode, int flag) {
    (void)flag;
    int ret = chmodat_guest(fd, path, mode);
    if (ret == 0)
        touchat_guest(fd, path, 1, 0);
    return ret;
}

int chown(const char *path, uid_t uid, gid_t gid) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    int ret = chownat_guest(dirfd, relative_path, uid, gid);
    if (ret == 0)
        touchat_guest(dirfd, relative_path, 1, 0);
    return ret;
}

int lchown(const char *path, uid_t uid, gid_t gid) {
    return chown(path, uid, gid);
}

int fchown(int fd, uid_t uid, gid_t gid) {
    int32_t error = __imported_wasix_fd_chown(fd, uid, gid);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

int fchownat(int fd, const char *path, uid_t uid, gid_t gid, int flag) {
    (void)flag;
    int ret = chownat_guest(fd, path, uid, gid);
    if (ret == 0)
        touchat_guest(fd, path, 1, 0);
    return ret;
}

int setgroups(size_t size, const gid_t list[]) {
    if (size > UINT32_MAX) {
        errno = EINVAL;
        return -1;
    }
    int32_t error = __imported_wasix_proc_setgroups((const uint32_t *)list, (uint32_t)size);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return 0;
}

int getgroups(int size, gid_t list[]) {
    if (size < 0) {
        errno = EINVAL;
        return -1;
    }
    uint32_t count = 0;
    int32_t error = __imported_wasix_proc_getgroups((uint32_t *)list, (uint32_t)size, &count);
    if (error != 0) {
        errno = error;
        return -1;
    }
    return (int)count;
}

uid_t getuid(void) {
    return sinomo_wasix_ruid;
}

uid_t geteuid(void) {
    return sinomo_wasix_euid;
}

gid_t getgid(void) {
    return sinomo_wasix_rgid;
}

gid_t getegid(void) {
    return sinomo_wasix_egid;
}

int setuid(uid_t uid) {
    int32_t error = __imported_wasix_proc_setresuid(uid, uid, uid);
    if (error != 0) {
        errno = error;
        return -1;
    }
    sinomo_wasix_ruid = uid;
    sinomo_wasix_euid = uid;
    sinomo_wasix_suid = uid;
    return 0;
}

int seteuid(uid_t uid) {
    int32_t error = __imported_wasix_proc_setresuid(UINT32_MAX, uid, UINT32_MAX);
    if (error != 0) {
        errno = error;
        return -1;
    }
    sinomo_wasix_euid = uid;
    return 0;
}

int setreuid(uid_t ruid, uid_t euid) {
    int32_t raw_ruid = ruid == (uid_t)-1 ? UINT32_MAX : (uint32_t)ruid;
    int32_t raw_euid = euid == (uid_t)-1 ? UINT32_MAX : (uint32_t)euid;
    int32_t error = __imported_wasix_proc_setresuid((uint32_t)raw_ruid, (uint32_t)raw_euid, UINT32_MAX);
    if (error != 0) {
        errno = error;
        return -1;
    }
    if (ruid != (uid_t)-1)
        sinomo_wasix_ruid = ruid;
    if (euid != (uid_t)-1)
        sinomo_wasix_euid = euid;
    return 0;
}

int setresuid(uid_t ruid, uid_t euid, uid_t suid) {
    uint32_t raw_ruid = ruid == (uid_t)-1 ? UINT32_MAX : (uint32_t)ruid;
    uint32_t raw_euid = euid == (uid_t)-1 ? UINT32_MAX : (uint32_t)euid;
    uint32_t raw_suid = suid == (uid_t)-1 ? UINT32_MAX : (uint32_t)suid;
    int32_t error = __imported_wasix_proc_setresuid(raw_ruid, raw_euid, raw_suid);
    if (error != 0) {
        errno = error;
        return -1;
    }
    if (ruid != (uid_t)-1)
        sinomo_wasix_ruid = ruid;
    if (euid != (uid_t)-1)
        sinomo_wasix_euid = euid;
    if (suid != (uid_t)-1)
        sinomo_wasix_suid = suid;
    return 0;
}

int setgid(gid_t gid) {
    int32_t error = __imported_wasix_proc_setresgid(gid, gid, gid);
    if (error != 0) {
        errno = error;
        return -1;
    }
    sinomo_wasix_rgid = gid;
    sinomo_wasix_egid = gid;
    sinomo_wasix_sgid = gid;
    return 0;
}

int setegid(gid_t gid) {
    int32_t error = __imported_wasix_proc_setresgid(UINT32_MAX, gid, UINT32_MAX);
    if (error != 0) {
        errno = error;
        return -1;
    }
    sinomo_wasix_egid = gid;
    return 0;
}

int setregid(gid_t rgid, gid_t egid) {
    uint32_t raw_rgid = rgid == (gid_t)-1 ? UINT32_MAX : (uint32_t)rgid;
    uint32_t raw_egid = egid == (gid_t)-1 ? UINT32_MAX : (uint32_t)egid;
    int32_t error = __imported_wasix_proc_setresgid(raw_rgid, raw_egid, UINT32_MAX);
    if (error != 0) {
        errno = error;
        return -1;
    }
    if (rgid != (gid_t)-1)
        sinomo_wasix_rgid = rgid;
    if (egid != (gid_t)-1)
        sinomo_wasix_egid = egid;
    return 0;
}

int setresgid(gid_t rgid, gid_t egid, gid_t sgid) {
    uint32_t raw_rgid = rgid == (gid_t)-1 ? UINT32_MAX : (uint32_t)rgid;
    uint32_t raw_egid = egid == (gid_t)-1 ? UINT32_MAX : (uint32_t)egid;
    uint32_t raw_sgid = sgid == (gid_t)-1 ? UINT32_MAX : (uint32_t)sgid;
    int32_t error = __imported_wasix_proc_setresgid(raw_rgid, raw_egid, raw_sgid);
    if (error != 0) {
        errno = error;
        return -1;
    }
    if (rgid != (gid_t)-1)
        sinomo_wasix_rgid = rgid;
    if (egid != (gid_t)-1)
        sinomo_wasix_egid = egid;
    if (sgid != (gid_t)-1)
        sinomo_wasix_sgid = sgid;
    return 0;
}

int ftruncate(int fildes, off_t length) {
    if (length < 0) {
        errno = EINVAL;
        return -1;
    }
    __wasi_filesize_t st_size = length;
    __wasi_errno_t error = __wasi_fd_filestat_set_size(fildes, st_size);
    if (error != 0) {
        errno = (error == EACCES || error == __WASI_ERRNO_ACCES) ? EINVAL : error;
        return -1;
    }
    (void)__imported_wasix_fd_touch(fildes);
    return 0;
}

int truncate(const char *path, off_t length) {
    if (length < 0) {
        errno = EINVAL;
        return -1;
    }
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    int fd = __wasilibc_nocwd_openat_nomode(dirfd, relative_path, O_WRONLY);
    if (fd < 0)
        return -1;
    int ret = ftruncate(fd, length);
    int saved_errno = errno;
    close(fd);
    errno = saved_errno;
    return ret;
}

DIR *opendir(const char *dirname) {
    char *relative_path;
    int dirfd = find_relpath(dirname, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return NULL;
    }
    return __wasilibc_nocwd_opendirat(dirfd, relative_path);
}

int scandir(const char *restrict dir, struct dirent ***restrict namelist,
            int (*filter)(const struct dirent *),
            int (*compar)(const struct dirent **, const struct dirent **)) {
    char *relative_path;
    int dirfd = find_relpath(dir, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_scandirat(dirfd, relative_path, namelist, filter, compar);
}

int symlink(const char *target, const char *linkpath) {
    char *relative_path;
    int dirfd = find_relpath(linkpath, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __sinomo_wasix_symlinkat(target, dirfd, relative_path);
}

int link(const char *old, const char *new) {
    char *old_relative_path;
    int old_dirfd = find_relpath_alt(old, &old_relative_path);
    if (old_dirfd != -1) {
        char *new_relative_path;
        int new_dirfd = find_relpath(new, &new_relative_path);
        if (new_dirfd != -1)
            return __wasilibc_nocwd_linkat(old_dirfd, old_relative_path, new_dirfd, new_relative_path, 0);
    }
    errno = ENOENT;
    return -1;
}

int rename(const char *old, const char *new) {
    char *old_relative_path;
    int old_dirfd = find_relpath_alt(old, &old_relative_path);
    if (old_dirfd != -1) {
        char *new_relative_path;
        int new_dirfd = find_relpath(new, &new_relative_path);
        if (new_dirfd != -1)
            return __wasilibc_nocwd_renameat(old_dirfd, old_relative_path, new_dirfd, new_relative_path);
    }
    errno = ENOENT;
    return -1;
}

int __wasilibc_access(const char *path, int mode, int flags) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_faccessat(dirfd, relative_path, mode, flags);
}

int __wasilibc_utimens(const char *path, const struct timespec times[2], int flags) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_utimensat(dirfd, relative_path, times, flags);
}

int __wasilibc_stat(const char *restrict path, struct stat *restrict st, int flags) {
    char *relative_path;
    int dirfd = find_relpath(path, &relative_path);
    if (dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_fstatat(dirfd, relative_path, st, flags);
}

int __wasilibc_link(const char *oldpath, const char *newpath, int flags) {
    char *old_relative_path;
    char *new_relative_path;
    int old_dirfd = find_relpath(oldpath, &old_relative_path);
    int new_dirfd = find_relpath(newpath, &new_relative_path);
    if (old_dirfd == -1 || new_dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_linkat(old_dirfd, old_relative_path, new_dirfd, new_relative_path, flags);
}

int __wasilibc_link_oldat(int olddirfd, const char *oldpath, const char *newpath, int flags) {
    char *new_relative_path;
    int new_dirfd = find_relpath(newpath, &new_relative_path);
    if (new_dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_linkat(olddirfd, oldpath, new_dirfd, new_relative_path, flags);
}

int __wasilibc_link_newat(const char *oldpath, int newdirfd, const char *newpath, int flags) {
    char *old_relative_path;
    int old_dirfd = find_relpath(oldpath, &old_relative_path);
    if (old_dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_linkat(old_dirfd, old_relative_path, newdirfd, newpath, flags);
}

int __wasilibc_rename_oldat(int fromdirfd, const char *from, const char *to) {
    char *to_relative_path;
    int to_dirfd = find_relpath(to, &to_relative_path);
    if (to_dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_renameat(fromdirfd, from, to_dirfd, to_relative_path);
}

int __wasilibc_rename_newat(const char *from, int todirfd, const char *to) {
    char *from_relative_path;
    int from_dirfd = find_relpath(from, &from_relative_path);
    if (from_dirfd == -1) {
        errno = ENOENT;
        return -1;
    }
    return __wasilibc_nocwd_renameat(from_dirfd, from_relative_path, todirfd, to);
}
