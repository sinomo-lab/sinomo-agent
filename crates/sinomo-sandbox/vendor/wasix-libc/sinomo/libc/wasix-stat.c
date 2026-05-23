#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#include <wasi/api.h>

int32_t __imported_wasix_fd_mode(int32_t fd, uint32_t *mode) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("fd_mode")
));

int32_t __imported_wasix_path_mode_at(int32_t fd, const char *path, int32_t path_len, uint32_t *mode) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_mode_at")
));

int32_t __imported_wasix_fd_owner(int32_t fd, uint32_t *uid, uint32_t *gid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("fd_owner")
));

int32_t __imported_wasix_path_owner_at(int32_t fd, const char *path, int32_t path_len, uint32_t *uid, uint32_t *gid) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_owner_at")
));

int32_t __imported_wasix_path_times_at(int32_t fd, const char *path, int32_t path_len, uint64_t *atime, uint64_t *mtime, uint64_t *ctime) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_times_at")
));

int32_t __imported_wasix_path_is_symlink_at(int32_t fd, const char *path, int32_t path_len) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_is_symlink_at")
));

int32_t __imported_wasix_path_read_symlink_at(int32_t fd, const char *path, int32_t path_len, char *buf, int32_t buf_len, uint32_t *used) __attribute__((
    __import_module__("wasix_32v1"),
    __import_name__("path_read_symlink_at")
));

static mode_t sinomo_wasix_type_mode(__wasi_filetype_t filetype) {
  switch (filetype) {
    case __WASI_FILETYPE_BLOCK_DEVICE:
      return S_IFBLK;
    case __WASI_FILETYPE_CHARACTER_DEVICE:
      return S_IFCHR;
    case __WASI_FILETYPE_DIRECTORY:
      return S_IFDIR;
    case __WASI_FILETYPE_REGULAR_FILE:
      return S_IFREG;
    case __WASI_FILETYPE_SOCKET_DGRAM:
    case __WASI_FILETYPE_SOCKET_STREAM:
    case __WASI_FILETYPE_SOCKET_SEQPACKET:
    case __WASI_FILETYPE_SOCKET_RAW:
      return S_IFSOCK;
    case __WASI_FILETYPE_SYMBOLIC_LINK:
      return S_IFLNK;
    default:
      return 0;
  }
}

static mode_t sinomo_wasix_default_permissions(__wasi_filetype_t filetype) {
  switch (filetype) {
    case __WASI_FILETYPE_DIRECTORY:
      return 0777;
    case __WASI_FILETYPE_SYMBOLIC_LINK:
      return 0777;
    default:
      return 0666;
  }
}

static void sinomo_wasix_to_public_stat(const __wasi_filestat_t *in,
                                        struct stat *out,
                                        uint32_t permissions,
                                        uint32_t uid,
                                        uint32_t gid) {
  *out = (struct stat){
      .st_dev = in->dev,
      .st_ino = in->ino,
      .st_nlink = in->nlink,
      .st_size = in->size,
      .st_uid = uid,
      .st_gid = gid,
      .st_atim = (struct timespec){
          .tv_sec = in->atim / 1000000000,
          .tv_nsec = in->atim % 1000000000,
      },
      .st_mtim = (struct timespec){
          .tv_sec = in->mtim / 1000000000,
          .tv_nsec = in->mtim % 1000000000,
      },
      .st_ctim = (struct timespec){
          .tv_sec = in->ctim / 1000000000,
          .tv_nsec = in->ctim % 1000000000,
      },
  };
  if ((permissions & 07777) == 0)
    permissions = sinomo_wasix_default_permissions(in->filetype);
  out->st_mode = sinomo_wasix_type_mode(in->filetype) | (permissions & 07777);
}

static int sinomo_wasix_read_symlink_target(int fd, const char *path, char *buf, size_t bufsize) {
  uint32_t used = 0;
  int32_t error = __imported_wasix_path_read_symlink_at(
      fd, path, strlen(path), buf, bufsize - 1, &used);
  if (error != 0)
    return -1;
  buf[used] = '\0';
  return (int)used;
}

static int sinomo_wasix_resolve_symlink_path(const char *link_path,
                                             const char *target,
                                             char *out,
                                             size_t out_size) {
  if (target[0] == '/') {
    if (strlen(target) + 1 > out_size)
      return -1;
    strcpy(out, target);
    return 0;
  }

  const char *slash = strrchr(link_path, '/');
  if (slash == NULL) {
    if (strlen(target) + 1 > out_size)
      return -1;
    strcpy(out, target);
    return 0;
  }

  size_t parent_len = (size_t)(slash - link_path);
  size_t target_len = strlen(target);
  if (parent_len + 1 + target_len + 1 > out_size)
    return -1;
  memcpy(out, link_path, parent_len);
  out[parent_len] = '/';
  memcpy(out + parent_len + 1, target, target_len + 1);
  return 0;
}

int fstat(int fildes, struct stat *buf) {
  __wasi_filestat_t internal_stat;
  __wasi_errno_t error = __wasi_fd_filestat_get(fildes, &internal_stat);
  if (error != 0) {
    errno = error;
    return -1;
  }

  uint32_t mode = 0;
  uint32_t uid = 0;
  uint32_t gid = 0;
  (void)__imported_wasix_fd_mode(fildes, &mode);
  (void)__imported_wasix_fd_owner(fildes, &uid, &gid);
  sinomo_wasix_to_public_stat(&internal_stat, buf, mode, uid, gid);
  return 0;
}

int __wasilibc_nocwd_fstatat(int fd, const char *restrict path, struct stat *restrict buf,
                             int flag) {
  char target[4096];
  char resolved_path[8192];
  const char *stat_path = path;

  if (__imported_wasix_path_is_symlink_at(fd, path, strlen(path)) == 0) {
    int target_len = sinomo_wasix_read_symlink_target(fd, path, target, sizeof(target));
    if (target_len < 0) {
      errno = EINVAL;
      return -1;
    }
    if ((flag & AT_SYMLINK_NOFOLLOW) != 0) {
      __wasi_filestat_t symlink_stat = {
          .dev = 0,
          .ino = 0,
          .filetype = __WASI_FILETYPE_SYMBOLIC_LINK,
          .nlink = 1,
          .size = (uint64_t)target_len,
      };
      uint64_t atime = 0;
      uint64_t mtime = 0;
      uint64_t ctime = 0;
      if (__imported_wasix_path_times_at(fd, path, strlen(path), &atime, &mtime, &ctime) == 0) {
        symlink_stat.atim = atime;
        symlink_stat.mtim = mtime;
        symlink_stat.ctim = ctime;
      }
      sinomo_wasix_to_public_stat(&symlink_stat, buf, 0777, 0, 0);
      return 0;
    }
    if (sinomo_wasix_resolve_symlink_path(path, target, resolved_path, sizeof(resolved_path)) != 0) {
      errno = ENAMETOOLONG;
      return -1;
    }
    stat_path = resolved_path;
  }

  __wasi_lookupflags_t lookup_flags = 0;
  if ((flag & AT_SYMLINK_NOFOLLOW) == 0)
    lookup_flags |= __WASI_LOOKUPFLAGS_SYMLINK_FOLLOW;

  __wasi_filestat_t internal_stat;
  __wasi_errno_t error =
      __wasi_path_filestat_get(fd, lookup_flags, stat_path, &internal_stat);
  if (error != 0) {
    errno = error;
    return -1;
  }

  uint32_t mode = 0;
  uint32_t uid = 0;
  uint32_t gid = 0;
  uint64_t atime = 0;
  uint64_t mtime = 0;
  uint64_t ctime = 0;
  (void)__imported_wasix_path_mode_at(fd, stat_path, strlen(stat_path), &mode);
  (void)__imported_wasix_path_owner_at(fd, stat_path, strlen(stat_path), &uid, &gid);
  if (__imported_wasix_path_times_at(fd, stat_path, strlen(stat_path), &atime, &mtime, &ctime) == 0) {
    internal_stat.atim = atime;
    internal_stat.mtim = mtime;
    internal_stat.ctim = ctime;
  }
  sinomo_wasix_to_public_stat(&internal_stat, buf, mode, uid, gid);
  return 0;
}
