// Handle AT_FDCWD and absolute paths for the *at functions.

#include <stdarg.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <wasi/libc.h>
#include <wasi/libc-nocwd.h>

#ifndef O_TMPFILE
#define O_TMPFILE 0
#endif

mode_t __sinomo_wasix_get_umask(void);
uid_t __sinomo_wasix_geteuid(void);
gid_t __sinomo_wasix_getegid(void);
void __sinomo_wasix_touchat(int fd, const char *path, int touch_self, int touch_parent);
int __sinomo_wasix_symlinkat(const char *target, int fd, const char *path);
ssize_t __sinomo_wasix_readlinkat(int fd, const char *path, char *buf, size_t bufsize);
int __sinomo_wasix_unlink_symlinkat(int fd, const char *path);

int openat(int dirfd, const char *pathname, int flags, ...) {
    mode_t mode = 0666;
    if ((flags & O_CREAT) != 0) {
        va_list ap;
        va_start(ap, flags);
        mode = va_arg(ap, mode_t);
        va_end(ap);
    }
    if (dirfd == AT_FDCWD || pathname[0] == '/') {
        return open(pathname, flags, mode);
    }

    struct stat before;
    int created = ((flags & O_CREAT) != 0) &&
        (__wasilibc_nocwd_fstatat(dirfd, pathname, &before, 0) != 0 && errno == ENOENT);

    int fd = __wasilibc_nocwd_openat_nomode(dirfd, pathname, flags);
    if (fd < 0)
        return fd;
    if ((flags & O_CREAT) != 0) {
        if (fchmod(fd, mode & ~__sinomo_wasix_get_umask()) != 0 ||
            fchown(fd, __sinomo_wasix_geteuid(), __sinomo_wasix_getegid()) != 0) {
            close(fd);
            return -1;
        }
    }
    if (created)
        __sinomo_wasix_touchat(dirfd, pathname, 1, 1);
    else if ((flags & O_TRUNC) != 0)
        __sinomo_wasix_touchat(dirfd, pathname, 1, 0);
    return fd;
}

int symlinkat(const char *target, int dirfd, const char *linkpath) {
    if (dirfd == AT_FDCWD || linkpath[0] == '/') {
        return symlink(target, linkpath);
    }
    return __sinomo_wasix_symlinkat(target, dirfd, linkpath);
}

ssize_t readlinkat(int dirfd, const char *restrict pathname, char *restrict buf, size_t bufsiz) {
    if (dirfd == AT_FDCWD || pathname[0] == '/') {
        return readlink(pathname, buf, bufsiz);
    }
    return __sinomo_wasix_readlinkat(dirfd, pathname, buf, bufsiz);
}

int mkdirat(int dirfd, const char *pathname, mode_t mode) {
    if (dirfd == AT_FDCWD || pathname[0] == '/') {
        return mkdir(pathname, mode);
    }
    int ret = __wasilibc_nocwd_mkdirat_nomode(dirfd, pathname);
    if (ret != 0)
        return ret;
    if (fchmodat(dirfd, pathname, mode & ~__sinomo_wasix_get_umask(), 0) != 0)
        return -1;
    if (fchownat(dirfd, pathname, __sinomo_wasix_geteuid(), __sinomo_wasix_getegid(), 0) != 0)
        return -1;
    __sinomo_wasix_touchat(dirfd, pathname, 1, 1);
    return 0;
}

int mknodat(int dirfd, const char *pathname, mode_t mode, dev_t dev) {
    if (dirfd == AT_FDCWD || pathname[0] == '/') {
        return mknod(pathname, mode, dev);
    }
    (void)dev;
    if (S_ISREG(mode) || (mode & S_IFMT) == 0) {
        int fd = openat(dirfd, pathname, O_CREAT | O_EXCL | O_WRONLY, mode & 07777);
        if (fd < 0)
            return -1;
        return close(fd);
    }
    errno = ENOSYS;
    return -1;
}

DIR *opendirat(int dirfd, const char *path) {
    if (dirfd == AT_FDCWD || path[0] == '/') {
        return opendir(path);
    }
    return __wasilibc_nocwd_opendirat(dirfd, path);
}

int scandirat(int dirfd, const char *dirp, struct dirent ***namelist,
              int (*filter)(const struct dirent *),
              int (*compar)(const struct dirent **, const struct dirent **)) {
    if (dirfd == AT_FDCWD || dirp[0] == '/') {
        return scandir(dirp, namelist, filter, compar);
    }
    return __wasilibc_nocwd_scandirat(dirfd, dirp, namelist, filter, compar);
}

int faccessat(int dirfd, const char *pathname, int mode, int flags) {
    if (dirfd == AT_FDCWD || pathname[0] == '/') {
        return __wasilibc_access(pathname, mode, flags);
    }
    return __wasilibc_nocwd_faccessat(dirfd, pathname, mode, flags);
}

int fstatat(int dirfd, const char *restrict pathname, struct stat *restrict statbuf, int flags) {
    if (dirfd == AT_FDCWD || pathname[0] == '/') {
        return __wasilibc_stat(pathname, statbuf, flags);
    }
    return __wasilibc_nocwd_fstatat(dirfd, pathname, statbuf, flags);
}

int utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags) {
    if (dirfd == AT_FDCWD || pathname[0] == '/') {
        return __wasilibc_utimens(pathname, times, flags);
    }
    return __wasilibc_nocwd_utimensat(dirfd, pathname, times, flags);
}

int linkat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags) {
    if ((olddirfd == AT_FDCWD || oldpath[0] == '/') &&
        (newdirfd == AT_FDCWD || newpath[0] == '/')) {
        return __wasilibc_link(oldpath, newpath, flags);
    }
    if (olddirfd == AT_FDCWD || oldpath[0] == '/') {
        return __wasilibc_link_newat(oldpath, newdirfd, newpath, flags);
    }
    if (newdirfd == AT_FDCWD || newpath[0] == '/') {
        return __wasilibc_link_oldat(olddirfd, oldpath, newpath, flags);
    }
    return __wasilibc_nocwd_linkat(olddirfd, oldpath, newdirfd, newpath, flags);
}

int renameat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath) {
    if ((olddirfd == AT_FDCWD || oldpath[0] == '/') &&
        (newdirfd == AT_FDCWD || newpath[0] == '/')) {
        return rename(oldpath, newpath);
    }
    if (olddirfd == AT_FDCWD || oldpath[0] == '/') {
        return __wasilibc_rename_newat(oldpath, newdirfd, newpath);
    }
    if (newdirfd == AT_FDCWD || newpath[0] == '/') {
        return __wasilibc_rename_oldat(olddirfd, oldpath, newpath);
    }
    return __wasilibc_nocwd_renameat(olddirfd, oldpath, newdirfd, newpath);
}

int __wasilibc_unlinkat(int dirfd, const char *path) {
    if (dirfd == AT_FDCWD || path[0] == '/') {
        return unlink(path);
    }
    if (__sinomo_wasix_unlink_symlinkat(dirfd, path) == 0)
        return 0;
    if (errno != ENOENT)
        return -1;
    return __wasilibc_nocwd___wasilibc_unlinkat(dirfd, path);
}

int __wasilibc_rmdirat(int dirfd, const char *path) {
    if (dirfd == AT_FDCWD || path[0] == '/') {
        return rmdir(path);
    }
    return __wasilibc_nocwd___wasilibc_rmdirat(dirfd, path);
}
