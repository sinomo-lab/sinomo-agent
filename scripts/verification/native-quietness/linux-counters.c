/* Read-only native-host census. No setns, capabilities, subprocesses or daemon control.
 * Build: cc -O2 -std=c11 -Wall -Wextra -Werror linux-counters.c -lcrypto -o COUNTER
 * Protocol: SAMPLE sequence nonce\n, then QUIT. Full evidence stays in --raw;
 * stdout carries bounded summaries, avoiding an SSH transfer of every task row.
 * This polling adapter deliberately cannot certify continuous host coverage.
 */
#define _GNU_SOURCE
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <linux/netlink.h>
#include <linux/rtnetlink.h>
#include <net/if.h>
#include <openssl/evp.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

typedef uint64_t U;
typedef struct { U start, user, system, flags; char state; } Task;
typedef struct { dev_t dev; ino_t ino; U ns; U begin, end; char *interfaces; } Group;
static FILE *raw;
static unsigned errors, vanished, denied_ns, skipped, task_count, process_count;
static Group *groups;
static size_t group_count;
static int links = -1;
static U link_events, link_losses;
static char boot[80], initial_boot[80], self_ns[80];
static U self_ns_inode;
typedef struct { int tid, fd; U generation; bool seen; } Incarnation;
static Incarnation *incarnations;
static size_t incarnation_count;
static U next_generation = 1;

static U ns(clockid_t clock) {
    struct timespec t;
    if (clock_gettime(clock, &t)) { perror("clock_gettime"); exit(2); }
    return (U)t.tv_sec * 1000000000 + (U)t.tv_nsec;
}

static void quote(FILE *f, const char *s, size_t n) {
    fputc('"', f);
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '"' || c == '\\') fprintf(f, "\\%c", c);
        else if (c >= 32 && c < 127) fputc(c, f);
        else fprintf(f, "\\u%04x", c);
    }
    fputc('"', f);
}

static char *read_file(const char *path, size_t *length) {
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return NULL;
    char *s = NULL; size_t n = 0, cap = 4096;
    s = malloc(cap);
    if (!s) { close(fd); errno = ENOMEM; return NULL; }
    for (;;) {
        if (n + 1 == cap) {
            if (cap >= 16777216) { free(s); close(fd); errno = EOVERFLOW; return NULL; }
            cap *= 2; char *v = realloc(s, cap);
            if (!v) { free(s); close(fd); errno = ENOMEM; return NULL; } s = v;
        }
        ssize_t got = read(fd, s + n, cap - n - 1);
        if (got < 0 && errno == EINTR) continue;
        if (got < 0) { int e = errno; free(s); close(fd); errno = e; return NULL; }
        if (!got) break;
        n += (size_t)got;
    }
    close(fd); s[n] = 0; if (length) *length = n; return s;
}

static void problem(const char *kind, const char *path, int e) {
    if (e == ENOENT || e == ESRCH) vanished++; else errors++;
    fprintf(raw, "{\"type\":\"reader_error\",\"kind\":"); quote(raw, kind, strlen(kind));
    fprintf(raw, ",\"path\":"); quote(raw, path, strlen(path));
    fprintf(raw, ",\"errno\":%d}\n", e);
}

static int task_stat(const char *path, Task *t) {
    char *s = read_file(path, NULL); if (!s) return -1;
    char *p = strrchr(s, ')');
    if (!p || p[1] != ' ') { free(s); errno = EPROTO; return -1; }
    p += 2; t->state = *p;
    char *save = NULL; char *v = strtok_r(p, " ", &save); unsigned field = 3;
    memset(&t->start, 0, sizeof(U) * 4);
    for (; v; v = strtok_r(NULL, " ", &save), field++) {
        if (field == 9 || field == 14 || field == 15 || field == 22) {
            errno = 0; char *end = NULL; U value = strtoull(v, &end, 10);
            if (errno || end == v || (*end && *end != '\n')) { free(s); errno = EPROTO; return -1; }
            if (field == 9) t->flags = value;
            if (field == 14) t->user = value;
            if (field == 15) t->system = value;
            if (field == 22) t->start = value;
        }
    }
    free(s); if (field <= 22) { errno = EPROTO; return -1; } return 0;
}

static bool number(const char *s) {
    if (!*s) return false;
    for (; *s; s++) if (!isdigit((unsigned char)*s)) return false;
    return true;
}

/* PIDFD_THREAD is O_EXCL in Linux's public pidfd UAPI (Linux >= 6.9).
 * No signal, ptrace, namespace-get or other privileged pidfd operation is used.
 * Retaining a pidfd does not retain a network namespace. No PID/start-tick
 * collision, even within one scheduler tick, can bridge a dead pidfd epoch. */
static U incarnation(int tid) {
    size_t i = 0;
    for (; i < incarnation_count; i++) if (incarnations[i].tid == tid) break;
    if (i < incarnation_count) {
        struct pollfd p = {.fd=incarnations[i].fd, .events=POLLIN};
        int result = poll(&p, 1, 0);
        if (result == 0 && p.fd >= 0) { incarnations[i].seen = true; return incarnations[i].generation; }
        if (incarnations[i].fd >= 0) close(incarnations[i].fd);
        incarnations[i].fd = -1;
        if (result < 0) problem("pidfd_poll", "/proc", errno);
    } else {
        Incarnation *v = realloc(incarnations, (incarnation_count + 1) * sizeof(*v));
        if (!v) exit(2);
        incarnations = v; incarnation_count++;
    }
    int fd = (int)syscall(SYS_pidfd_open, tid, O_EXCL);
    incarnations[i] = (Incarnation){.tid=tid, .fd=fd, .generation=next_generation++, .seen=true};
    if (fd < 0) { problem("pidfd_thread_unavailable", "/proc", errno); return 0; }
    return incarnations[i].generation;
}

static bool incarnation_alive(int tid, U generation) {
    for (size_t i = 0; i < incarnation_count; i++) if (incarnations[i].tid == tid) {
        struct pollfd p = {.fd=incarnations[i].fd, .events=POLLIN};
        return p.fd >= 0 && incarnations[i].generation == generation && poll(&p, 1, 0) == 0;
    }
    return false;
}

static int digest(const char *path, char result[65]) {
    int fd = open(path, O_RDONLY | O_CLOEXEC); if (fd < 0) return -1;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new(); unsigned char data[65536], value[EVP_MAX_MD_SIZE];
    unsigned len = 0; int ok = ctx && EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    while (ok) {
        ssize_t n = read(fd, data, sizeof(data));
        if (n < 0 && errno == EINTR) continue;
        if (n < 0) { ok = 0; break; }
        if (!n) break;
        ok = EVP_DigestUpdate(ctx, data, (size_t)n);
    }
    if (ok) ok = EVP_DigestFinal_ex(ctx, value, &len);
    EVP_MD_CTX_free(ctx); close(fd);
    if (!ok || len != 32) { errno = EIO; return -1; }
    for (unsigned i = 0; i < len; i++) snprintf(result + 2 * i, 3, "%02x", value[i]);
    return 0;
}

/* Cache only while executable inode/size/mtime/ctime are identical. Every process
 * row includes these fields, its start time, argv, and a before/after stat guard. */
typedef struct Hash { struct stat st; char hash[65]; struct Hash *next; } Hash;
static Hash *hashes;
static bool same_file(const struct stat *a, const struct stat *b) {
    return a->st_dev == b->st_dev && a->st_ino == b->st_ino && a->st_size == b->st_size &&
        a->st_mtim.tv_sec == b->st_mtim.tv_sec && a->st_mtim.tv_nsec == b->st_mtim.tv_nsec &&
        a->st_ctim.tv_sec == b->st_ctim.tv_sec && a->st_ctim.tv_nsec == b->st_ctim.tv_nsec;
}

static void process_identity(const char *pid, const char *kind) {
    char path[PATH_MAX], exe[PATH_MAX], comm_path[PATH_MAX]; Task before, after;
    struct stat process_dir = {0};
    snprintf(path, sizeof(path), "/proc/%s", pid);
    if (stat(path, &process_dir)) problem("process_directory", path, errno);
    snprintf(path, sizeof(path), "/proc/%s/stat", pid);
    if (task_stat(path, &before)) { problem("process_stat_before", path, errno); return; }
    if (before.flags & 0x200000 || before.state == 'Z' || before.state == 'X') return;
    U epoch = incarnation(atoi(pid));
    if (task_stat(path, &before)) { problem("process_stat_pidfd_guard", path, errno); return; }
    snprintf(path, sizeof(path), "/proc/%s/exe", pid);
    ssize_t n = readlink(path, exe, sizeof(exe) - 1); int exe_errno = n < 0 ? errno : 0;
    if (n < 0) n = 0;
    exe[n] = 0;
    struct stat st = {0}, after_st = {0}; bool have_st = stat(path, &st) == 0;
    snprintf(comm_path, sizeof(comm_path), "/proc/%s/comm", pid);
    char *comm = read_file(comm_path, NULL);
    snprintf(comm_path, sizeof(comm_path), "/proc/%s/cmdline", pid);
    size_t argv_len = 0; char *argv = read_file(comm_path, &argv_len); int argv_errno = argv ? 0 : errno;
    /* Unknown executables remain explicitly unknown. A substring is only a
     * candidate for identity capture; Python's exact argv parser assigns roles. */
    const char *base = strrchr(exe, '/');
    bool candidate = !strncmp(base ? base + 1 : exe, "sinomo", 6) || (comm && !strncmp(comm, "sinomo", 6));
    const char *hash = NULL;
    if (candidate && have_st) {
        Hash *h = hashes;
        for (; h && !same_file(&st, &h->st); h = h->next) {}
        if (!h) {
            h = calloc(1, sizeof(*h)); if (!h) exit(2); h->st = st;
            if (digest(path, h->hash) || stat(path, &after_st) || !same_file(&st, &after_st)) {
                problem("runtime_digest_or_executable_changed", path, errno ? errno : ESTALE); free(h); h = NULL;
            } else { h->next = hashes; hashes = h; }
        }
        if (h) hash = h->hash;
    }
    fprintf(raw, "{\"type\":\"%s\",\"tgid\":%s,\"uid\":%ju,\"process_epoch\":%" PRIu64 ",\"start_ticks\":%" PRIu64 ",\"exe\":", kind, pid, (uintmax_t)process_dir.st_uid, epoch, before.start);
    quote(raw, exe, (size_t)n);
    fprintf(raw, ",\"exe_errno\":%d,\"argv_errno\":%d,\"argv\":[", exe_errno, argv_errno);
    bool comma = false;
    for (size_t i = 0; argv && i < argv_len;) {
        size_t length = strnlen(argv + i, argv_len - i);
        if (comma) fputc(',', raw);
        comma = true; quote(raw, argv + i, length); i += length + 1;
    }
    fprintf(raw, "],\"comm\":"); quote(raw, comm ? comm : "", comm ? strlen(comm) : 0);
    fprintf(raw, ",\"candidate\":%s,\"exe_identity\":[%ju,%ju,%jd,%jd,%ld,%jd,%ld],\"sha256\":",
        candidate ? "true" : "false", (uintmax_t)st.st_dev, (uintmax_t)st.st_ino, (intmax_t)st.st_size,
        (intmax_t)st.st_mtim.tv_sec, st.st_mtim.tv_nsec, (intmax_t)st.st_ctim.tv_sec, st.st_ctim.tv_nsec);
    if (hash) quote(raw, hash, 64); else fputs("null", raw);
    fputs("}\n", raw); free(argv); free(comm); if (!strcmp(kind, "process")) process_count++;
    snprintf(path, sizeof(path), "/proc/%s/stat", pid);
    if (task_stat(path, &after) || before.start != after.start) problem("process_identity_discontinuity", path, ESTALE);
    if (!incarnation_alive(atoi(pid), epoch)) problem("process_pidfd_discontinuity", path, ESTALE);
    if (have_st) {
        snprintf(path, sizeof(path), "/proc/%s/exe", pid);
        if (stat(path, &after_st) || !same_file(&st, &after_st)) problem("process_executable_discontinuity", path, ESTALE);
    }
}

static char *interfaces(int fd) {
    FILE *f = fdopen(dup(fd), "r"); if (!f) return NULL;
    char *line = NULL; size_t capacity = 0; char *json = NULL; size_t length = 0;
    FILE *out = open_memstream(&json, &length); if (!out) exit(2);
    bool comma = false, lo = false; unsigned lines = 0;
    fputc('{', out);
    while (getline(&line, &capacity, f) >= 0) {
        if (++lines <= 2) continue;
        char *colon = strchr(line, ':'); U v[16]; int used = 0;
        if (!colon) { errors++; continue; }
        *colon = 0; char *name = line; while (isspace((unsigned char)*name)) name++;
        char *end = colon; while (end > name && isspace((unsigned char)end[-1])) *--end = 0;
        int count = sscanf(colon + 1,
            "%" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64
            " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %n",
            &v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9], &v[10], &v[11], &v[12], &v[13], &v[14], &v[15], &used);
        if (!*name || count != 16 || colon[1 + used]) { errors++; continue; }
        if (comma) fputc(',', out);
        comma = true; quote(out, name, strlen(name));
        fprintf(out, ":[%" PRIu64 ",%" PRIu64 "]", v[0], v[8]);
        if (!strcmp(name, "lo")) lo = true;
    }
    if (ferror(f) || lines < 3 || !lo) errors++;
    fputc('}', out); fclose(out); fclose(f); free(line); return json;
}

static void thread(const char *pid, const char *tid) {
    char path[PATH_MAX], netpath[PATH_MAX], nspath[PATH_MAX], namespace[80]; Task a, b;
    snprintf(path, sizeof(path), "/proc/%s/task/%s/stat", pid, tid);
    U begin = ns(CLOCK_MONOTONIC);
    if (task_stat(path, &a)) { problem("task_stat_before", path, errno); return; }
    if (a.flags & 0x200000 || a.state == 'Z' || a.state == 'X') { skipped++; return; }
    U epoch = incarnation(atoi(tid));
    if (task_stat(path, &a)) { problem("task_stat_pidfd_guard", path, errno); return; }
    snprintf(netpath, sizeof(netpath), "/proc/%s/task/%s/net/dev", pid, tid);
    snprintf(nspath, sizeof(nspath), "/proc/%s/task/%s/ns/net", pid, tid);
    ssize_t n = readlink(nspath, namespace, sizeof(namespace) - 1); U known_ns = 0;
    if (n >= 0) { namespace[n] = 0; if (sscanf(namespace, "net:[%" SCNu64 "]", &known_ns) != 1) errors++; }
    else if (errno == EACCES || errno == EPERM) denied_ns++; else problem("namespace_link", nspath, errno);
    U read_begin = ns(CLOCK_MONOTONIC);
    int fd = open(netpath, O_RDONLY | O_CLOEXEC); struct stat identity = {0}, identity2 = {0};
    if (fd < 0 || fstat(fd, &identity)) {
        problem("counter_open", netpath, errno); if (fd >= 0) close(fd); return;
    }
    size_t i = 0; for (; i < group_count; i++) if (groups[i].dev == identity.st_dev && groups[i].ino == identity.st_ino) break;
    if (i == group_count) {
        Group *g = realloc(groups, (group_count + 1) * sizeof(*groups)); if (!g) exit(2); groups = g;
        groups[i] = (Group){.dev=identity.st_dev, .ino=identity.st_ino, .ns=known_ns, .begin=read_begin};
        groups[i].interfaces = interfaces(fd); groups[i].end = ns(CLOCK_MONOTONIC); group_count++;
    } else if (known_ns) {
        if (groups[i].ns && groups[i].ns != known_ns) problem("counter_identity_namespace_collision", netpath, ESTALE);
        groups[i].ns = known_ns;
    }
    if (fstat(fd, &identity2) || identity.st_dev != identity2.st_dev || identity.st_ino != identity2.st_ino)
        problem("counter_fd_changed", netpath, ESTALE);
    close(fd);
    /* Re-open, rather than just fstat the old FD: a live thread can change netns. */
    fd = open(netpath, O_RDONLY | O_CLOEXEC);
    if (fd < 0 || fstat(fd, &identity2) || identity.st_dev != identity2.st_dev || identity.st_ino != identity2.st_ino)
        problem("counter_binding_changed", netpath, ESTALE);
    if (fd >= 0) close(fd);
    if (task_stat(path, &b) || a.start != b.start || b.user < a.user || b.system < a.system)
        problem("task_incarnation_or_counter_changed", path, ESTALE);
    n = readlink(nspath, namespace, sizeof(namespace) - 1);
    if (n >= 0) {
        U after_ns = 0; namespace[n] = 0;
        if (sscanf(namespace, "net:[%" SCNu64 "]", &after_ns) != 1 || after_ns != known_ns)
            problem("namespace_link_changed_during_read", nspath, ESTALE);
    } else if (known_ns || (errno != EACCES && errno != EPERM)) problem("namespace_link_after", nspath, errno);
    if (!incarnation_alive(atoi(tid), epoch)) problem("thread_pidfd_discontinuity", path, ESTALE);
    U end = ns(CLOCK_MONOTONIC); task_count++;
    fprintf(raw, "{\"type\":\"thread\",\"tgid\":%s,\"tid\":%s,\"thread_epoch\":%" PRIu64 ",\"start_ticks\":%" PRIu64
        ",\"cpu_ticks\":[%" PRIu64 ",%" PRIu64 "],\"read_ns\":[%" PRIu64 ",%" PRIu64
        "],\"counter_file\":[%ju,%ju],\"netns\":%" PRIu64 "}\n",
        pid, tid, epoch, a.start, a.user, a.system, begin, end, (uintmax_t)identity.st_dev, (uintmax_t)identity.st_ino, known_ns);
}

static void census(void) {
    DIR *p = opendir("/proc"); if (!p) { problem("proc_inventory", "/proc", errno); return; }
    struct dirent *e;
    errno = 0;
    while ((e = readdir(p))) {
        if (!number(e->d_name)) continue;
        process_identity(e->d_name, "process");
        char path[PATH_MAX]; snprintf(path, sizeof(path), "/proc/%s/task", e->d_name);
        DIR *t = opendir(path); if (!t) { problem("task_inventory", path, errno); continue; }
        struct dirent *v;
        while ((v = readdir(t))) if (number(v->d_name)) thread(e->d_name, v->d_name);
        closedir(t);
        process_identity(e->d_name, "process_end");
    }
    closedir(p);
}

static void link_drain(void) {
    if (links < 0) return;
    union { struct nlmsghdr alignment; char data[65536]; } buffer;
    for (;;) {
        struct sockaddr_nl address = {0}; struct iovec io = {buffer.data, sizeof(buffer.data)};
        struct msghdr m = {.msg_name=&address, .msg_namelen=sizeof(address), .msg_iov=&io, .msg_iovlen=1};
        ssize_t n = recvmsg(links, &m, MSG_DONTWAIT);
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
        if (n <= 0) { link_losses++; break; }
        if ((m.msg_flags & MSG_TRUNC) || address.nl_pid != 0) link_losses++;
        int left = (int)n;
        for (struct nlmsghdr *h = (void *)buffer.data; NLMSG_OK(h, left); h = NLMSG_NEXT(h, left)) {
            if (h->nlmsg_type == RTM_NEWLINK || h->nlmsg_type == RTM_DELLINK) link_events++;
            else if (h->nlmsg_type == NLMSG_OVERRUN || h->nlmsg_type == NLMSG_ERROR) link_losses++;
        }
        if (left) link_losses++;
    }
}

static void mounts(void) {
    const char *paths[] = {"/run/netns", "/run/docker/netns", NULL};
    for (int i = 0; paths[i]; i++) {
        errno = 0; DIR *d = opendir(paths[i]); int e = d ? 0 : errno;
        fprintf(raw, "{\"type\":\"persistent_namespace_directory\",\"path\":"); quote(raw, paths[i], strlen(paths[i]));
        fprintf(raw, ",\"errno\":%d,\"entries\":[", e);
        if (d) {
            bool comma = false; struct dirent *v;
            while ((v = readdir(d))) if (strcmp(v->d_name, ".") && strcmp(v->d_name, "..")) {
                if (comma) fputc(',', raw);
                comma = true; quote(raw, v->d_name, strlen(v->d_name));
            }
            closedir(d);
        }
        fputs("]}\n", raw);
    }
    char *m = read_file("/proc/self/mountinfo", NULL);
    fprintf(raw, "{\"type\":\"mountinfo\",\"value\":");
    if (m) { quote(raw, m, strlen(m)); free(m); } else { fputs("null", raw); errors++; }
    fputs("}\n", raw);
}

/* Keep raw block layers separate. Polling cannot certify device lifetimes,
 * physical topology, fsync calls or completion of deferred writeback. */
static void disk_snapshot(FILE *out) {
    U begin = ns(CLOCK_MONOTONIC);
    char *stats = read_file("/proc/diskstats", NULL);
    int stats_error = stats ? 0 : errno;
    char *vm = read_file("/proc/vmstat", NULL);
    int vm_error = vm ? 0 : errno;
    long page = sysconf(_SC_PAGESIZE);
    U end = ns(CLOCK_MONOTONIC);
    fprintf(out, ",\"disk\":{\"read_ns\":[%" PRIu64 ",%" PRIu64 "],\"page_size\":%ld,\"diskstats\":", begin, end, page);
    if (stats) quote(out, stats, strlen(stats)); else fputs("null", out);
    fputs(",\"vmstat\":", out);
    if (vm) quote(out, vm, strlen(vm)); else fputs("null", out);
    fputs(",\"read_errors\":[", out);
    if (stats_error) fprintf(out, "{\"source\":\"/proc/diskstats\",\"errno\":%d}", stats_error);
    if (vm_error) fprintf(out, "%s{\"source\":\"/proc/vmstat\",\"errno\":%d}", stats_error ? "," : "", vm_error);
    fputs("],\"device_lifecycle_coverage\":false,\"sync_syscall_coverage\":false}", out);
    free(stats); free(vm);
}

static void sample(U sequence, const char *nonce) {
    U begin = ns(CLOCK_MONOTONIC), cpu_begin = ns(CLOCK_PROCESS_CPUTIME_ID);
    errors = vanished = denied_ns = skipped = task_count = process_count = 0;
    for (size_t i = 0; i < incarnation_count; i++) incarnations[i].seen = false;
    for (size_t i = 0; i < group_count; i++) free(groups[i].interfaces);
    free(groups); groups = NULL; group_count = 0;
    char *b = read_file("/proc/sys/kernel/random/boot_id", NULL);
    if (!b) { problem("boot_id", "/proc/sys/kernel/random/boot_id", errno); boot[0] = 0; }
    else { b[strcspn(b, "\n")] = 0; snprintf(boot, sizeof(boot), "%s", b); free(b); }
    if (strcmp(boot, initial_boot)) errors++;
    fprintf(raw, "{\"type\":\"begin\",\"sequence\":%" PRIu64 ",\"nonce\":", sequence); quote(raw, nonce, strlen(nonce));
    fprintf(raw, ",\"boot_id\":"); quote(raw, boot, strlen(boot)); fprintf(raw, ",\"begin_ns\":%" PRIu64 "}\n", begin);
    link_drain(); census(); mounts(); link_drain();
    for (size_t i = 0; i < incarnation_count;) {
        Incarnation *v = &incarnations[i];
        if (v->seen) { i++; continue; }
        fprintf(raw, "{\"type\":\"unobserved_incarnation\",\"tid\":%d,\"epoch\":%" PRIu64 "}\n", v->tid, v->generation);
        if (v->fd >= 0) close(v->fd);
        incarnations[i] = incarnations[--incarnation_count];
    }
    U cpu_read_begin = ns(CLOCK_MONOTONIC), ticks[10] = {0};
    char *s = read_file("/proc/stat", NULL);
    if (!s || sscanf(s, "cpu %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64,
        &ticks[0], &ticks[1], &ticks[2], &ticks[3], &ticks[4], &ticks[5], &ticks[6], &ticks[7], &ticks[8], &ticks[9]) != 10)
        problem("host_cpu", "/proc/stat", EPROTO);
    free(s); U cpu_read_end = ns(CLOCK_MONOTONIC);
    char *summary = NULL; size_t size = 0; FILE *out = open_memstream(&summary, &size); if (!out) exit(2);
    fprintf(out, "{\"type\":\"sample\",\"sequence\":%" PRIu64 ",\"nonce\":", sequence); quote(out, nonce, strlen(nonce));
    fprintf(out, ",\"boot_id\":"); quote(out, boot, strlen(boot));
    fprintf(out, ",\"begin_ns\":%" PRIu64 ",\"cpu_read_ns\":[%" PRIu64 ",%" PRIu64 "],\"cpu_fields\":[", begin, cpu_read_begin, cpu_read_end);
    for (int i = 0; i < 10; i++) fprintf(out, "%s%" PRIu64, i ? "," : "", ticks[i]);
    fputc(']', out);
    disk_snapshot(out);
    fprintf(out, ",\"groups\":{");
    for (size_t i = 0; i < group_count; i++) {
        Group *g = &groups[i];
        fprintf(out, "%s\"%ju:%ju\":{\"netns\":%" PRIu64 ",\"read_ns\":[%" PRIu64 ",%" PRIu64 "],\"interfaces\":%s,\"interface_indices\":",
            i ? "," : "", (uintmax_t)g->dev, (uintmax_t)g->ino, g->ns, g->begin, g->end, g->interfaces ? g->interfaces : "null");
        if (g->ns && g->ns == self_ns_inode) {
            /* Public name/index inventory for OUR namespace only. A foreign
             * namespace's /proc counters must never borrow our sysfs indices. */
            struct if_nameindex *inventory = if_nameindex();
            if (!inventory) { errors++; fputs("null", out); }
            else {
                fputc('{', out);
                for (size_t j = 0; inventory[j].if_index; j++) {
                    if (j) fputc(',', out);
                    quote(out, inventory[j].if_name, strlen(inventory[j].if_name));
                    fprintf(out, ":%u", inventory[j].if_index);
                }
                fputc('}', out); if_freenameindex(inventory);
            }
        } else fputs("null", out);
        fputc('}', out);
    }
    fprintf(out, "},\"tasks\":%u,\"processes\":%u,\"errors\":%u,\"vanished\":%u,\"namespace_link_denials\":%u,\"skipped_kernel_or_zombie\":%u,\"self_link_events\":%" PRIu64 ",\"self_link_losses\":%" PRIu64 ",\"self_link_reader\":%s,\"coverage\":{\"continuous_host_namespaces\":false,\"all_interface_lifecycles\":false,\"persistent_namespaces\":false,\"process_lifetimes\":false},\"observer_cpu_ns\":%" PRIu64 ",\"end_ns\":%" PRIu64 "}\n",
        task_count, process_count, errors, vanished, denied_ns, skipped, link_events, link_losses, links >= 0 ? "true" : "false", ns(CLOCK_PROCESS_CPUTIME_ID) - cpu_begin, ns(CLOCK_MONOTONIC));
    fclose(out); fputs(summary, raw);
    if (fflush(raw) || ferror(raw)) { perror("raw evidence write"); free(summary); exit(2); }
    fputs(summary, stdout); free(summary); if (fflush(stdout) || ferror(stdout)) exit(2);
}

int main(int argc, char **argv) {
    if (argc != 3 || strcmp(argv[1], "--raw")) { fprintf(stderr, "usage: %s --raw NEW_LOCAL_NDJSON\n", argv[0]); return 2; }
    int fd = open(argv[2], O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0 || !(raw = fdopen(fd, "w"))) { perror("new raw file"); return 2; }
    char *b = read_file("/proc/sys/kernel/random/boot_id", NULL); if (!b) return 2;
    b[strcspn(b, "\n")] = 0; snprintf(initial_boot, sizeof(initial_boot), "%s", b); free(b);
    ssize_t n = readlink("/proc/self/ns/net", self_ns, sizeof(self_ns) - 1); if (n < 0) return 2; self_ns[n] = 0;
    if (sscanf(self_ns, "net:[%" SCNu64 "]", &self_ns_inode) != 1) return 2;
    links = socket(AF_NETLINK, SOCK_RAW | SOCK_NONBLOCK | SOCK_CLOEXEC, NETLINK_ROUTE);
    struct sockaddr_nl address = {.nl_family=AF_NETLINK, .nl_groups=RTMGRP_LINK};
    if (links >= 0 && bind(links, (void *)&address, sizeof(address))) { close(links); links = -1; }
    char sha[65]; if (digest("/proc/self/exe", sha)) return 2;
    Task self; if (task_stat("/proc/self/stat", &self)) return 2;
    char host[256]; if (gethostname(host, sizeof(host))) return 2; host[sizeof(host)-1] = 0;
    char *ready = NULL; size_t length = 0; FILE *f = open_memstream(&ready, &length); if (!f) return 2;
    fprintf(f, "{\"type\":\"ready\",\"schema\":\"sinomo.native-host-counters.v1\",\"pid\":%d,\"start_ticks\":%" PRIu64 ",\"hertz\":%ld,\"boot_id\":", getpid(), self.start, sysconf(_SC_CLK_TCK)); quote(f, initial_boot, strlen(initial_boot));
    fputs(",\"host\":", f); quote(f, host, strlen(host));
    fprintf(f, ",\"collector_sha256\":\"%s\",\"self_netns\":", sha); quote(f, self_ns, strlen(self_ns));
    fputs(",\"whole_host_scope_verified\":false,\"qualification\":\"Public live-thread polling cannot certify host namespace/interface lifecycle or persistent/orphan namespace completeness. No acceptance PASS from this adapter.\"}\n", f);
    fclose(f); fputs(ready, raw); fputs(ready, stdout); free(ready); fflush(raw); fflush(stdout);
    char *line = NULL; size_t capacity = 0; U expected = 0;
    while (getline(&line, &capacity, stdin) >= 0) {
        if (!strcmp(line, "QUIT\n")) { free(line); if (fclose(raw)) return 2; if (links >= 0) close(links); return 0; }
        U sequence; char nonce[81], extra;
        if (sscanf(line, "SAMPLE %" SCNu64 " %80[a-zA-Z0-9-] %c", &sequence, nonce, &extra) != 2 || sequence != expected++) {
            fprintf(stderr, "invalid or discontinuous controller command\n"); free(line); fclose(raw); return 2;
        }
        sample(sequence, nonce);
    }
    free(line); fclose(raw); fprintf(stderr, "controller missing QUIT; evidence incomplete\n"); return 2;
}
