#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <wasi/api.h>
#include <wasi/libc.h>

static_assert(O_APPEND == __WASI_FDFLAGS_APPEND, "Value mismatch");
static_assert(O_DSYNC == __WASI_FDFLAGS_DSYNC, "Value mismatch");
static_assert(O_NONBLOCK == __WASI_FDFLAGS_NONBLOCK, "Value mismatch");
static_assert(O_RSYNC == __WASI_FDFLAGS_RSYNC, "Value mismatch");
static_assert(O_SYNC == __WASI_FDFLAGS_SYNC, "Value mismatch");

static_assert(O_CREAT >> 12 == __WASI_OFLAGS_CREAT, "Value mismatch");
static_assert(O_DIRECTORY >> 12 == __WASI_OFLAGS_DIRECTORY, "Value mismatch");
static_assert(O_EXCL >> 12 == __WASI_OFLAGS_EXCL, "Value mismatch");
static_assert(O_TRUNC >> 12 == __WASI_OFLAGS_TRUNC, "Value mismatch");

static_assert(O_CLOEXEC >> 30 == __WASI_FDFLAGSEXT_CLOEXEC, "Value mismatch");

int __wasilibc_nocwd_openat_nomode(int fd, const char *path, int oflag) {
  int access_mode = oflag & O_ACCMODE;
  if (access_mode == 0)
    access_mode = O_RDONLY;

  __wasi_rights_t max =
      __WASI_RIGHTS_FD_ADVISE |
      __WASI_RIGHTS_FD_FDSTAT_SET_FLAGS |
      __WASI_RIGHTS_FD_FILESTAT_GET |
      __WASI_RIGHTS_FD_SEEK |
      __WASI_RIGHTS_FD_TELL |
      __WASI_RIGHTS_POLL_FD_READWRITE;
  switch (access_mode) {
    case O_RDONLY:
      max |= __WASI_RIGHTS_FD_READ | __WASI_RIGHTS_FD_READDIR;
      break;
    case O_WRONLY:
      max |= __WASI_RIGHTS_FD_DATASYNC | __WASI_RIGHTS_FD_WRITE |
             __WASI_RIGHTS_FD_ALLOCATE |
             __WASI_RIGHTS_FD_FILESTAT_SET_SIZE;
      break;
    case O_RDWR:
      max |= __WASI_RIGHTS_FD_READ | __WASI_RIGHTS_FD_READDIR |
             __WASI_RIGHTS_FD_DATASYNC | __WASI_RIGHTS_FD_WRITE |
             __WASI_RIGHTS_FD_ALLOCATE |
             __WASI_RIGHTS_FD_FILESTAT_SET_SIZE;
      break;
    case O_EXEC:
      break;
    case O_SEARCH:
      break;
    default:
      errno = EINVAL;
      return -1;
  }

  __wasi_fdstat_t fsb_cur;
  __wasi_errno_t error = __wasi_fd_fdstat_get(fd, &fsb_cur);
  if (error != 0) {
    errno = error;
    return -1;
  }

  __wasi_lookupflags_t lookup_flags = 0;
  if ((oflag & O_NOFOLLOW) == 0)
    lookup_flags |= __WASI_LOOKUPFLAGS_SYMLINK_FOLLOW;

  __wasi_fdflags_t fs_flags = oflag & 0xfff;
  __wasi_fdflagsext_t fd_flags = (oflag >> 30) & 0x03;
  __wasi_rights_t fs_rights_base = max & fsb_cur.fs_rights_inheriting;
  __wasi_rights_t fs_rights_inheriting = fsb_cur.fs_rights_inheriting;
  __wasi_fd_t newfd;
  error = __wasi_path_open(fd, lookup_flags, path,
                           (oflag >> 12) & 0xfff,
                           fs_rights_base, fs_rights_inheriting, fs_flags,
                           &newfd);
  if (error != 0) {
    errno = error;
    return -1;
  }
  if (fd_flags != 0) {
    (void)__wasi_fd_fdflags_set(newfd, fd_flags);
  }
  return newfd;
}
