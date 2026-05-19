//! POSIX-like functions supporting absolute path arguments, implemented in
//! terms of `__wasilibc_find_relpath` and `*at`-style functions.

#include <stdarg.h>
#include <stdint.h>
#include <errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <grp.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <utime.h>
#include <wasi/api.h>
#include <wasi/libc.h>
#include <wasi/libc-find-relpath.h>
#include <wasi/libc-nocwd.h>

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

static mode_t sinomo_wasix_umask = 0;
static uid_t sinomo_wasix_ruid = 0;
static uid_t sinomo_wasix_euid = 0;
static uid_t sinomo_wasix_suid = 0;
static gid_t sinomo_wasix_rgid = 0;
static gid_t sinomo_wasix_egid = 0;
static gid_t sinomo_wasix_sgid = 0;

mode_t __sinomo_wasix_get_umask(void) {
    return sinomo_wasix_umask;
}

uid_t __sinomo_wasix_geteuid(void);
gid_t __sinomo_wasix_getegid(void);

int __wasilibc_find_relpath_alloc(
    const char *path,
    const char **abs,
    char **relative,
    size_t *relative_len,
    int can_realloc
) {
    const char *local_abs = NULL;
    char *candidate = *relative;
    size_t candidate_len = *relative_len;
    int fd;

    if (abs == NULL)
        abs = &local_abs;

    if (candidate_len == 0) {
        candidate_len = strlen(path) + 1;
        candidate = malloc(candidate_len);
        if (candidate == NULL) {
            errno = ENOMEM;
            return -1;
        }
    }

    for (;;) {
        fd = __wasilibc_find_relpath(path, abs, &candidate, candidate_len);
        if (fd >= 0) {
            *relative = candidate;
            *relative_len = candidate_len;
            return fd;
        }
        if (errno != ERANGE) {
            if (*relative == NULL && candidate != NULL)
                free(candidate);
            return -1;
        }
        if (!can_realloc)
            return -1;
        candidate_len *= 2;
        char *grown = realloc(candidate, candidate_len);
        if (grown == NULL) {
            if (*relative == NULL && candidate != NULL)
                free(candidate);
            errno = ENOMEM;
            return -1;
        }
        candidate = grown;
    }
}

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
