#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#if !defined(MAP_ANONYMOUS) && defined(MAP_ANON)
#define MAP_ANONYMOUS MAP_ANON
#endif

int32_t __sinomo_wasix_mmap_shared_hint(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("mmap_shared_hint")));
int32_t __sinomo_wasix_mmap_shared_file_hint(int32_t fd, uint64_t file_offset, uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("mmap_shared_file_hint")));

static int read_mapping_contents(int fd, void *addr, size_t length, off_t offset) {
    char *cursor = (char *)addr;
    size_t remaining = length;

    while (remaining > 0) {
        ssize_t bytes = pread(fd, cursor, remaining, offset);
        if (bytes < 0) {
            return -1;
        }
        if (bytes == 0) {
            memset(cursor, 0, remaining);
            return 0;
        }
        cursor += bytes;
        offset += bytes;
        remaining -= (size_t)bytes;
    }

    return 0;
}

void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset) {
    (void)prot;

    if (addr != NULL) {
        errno = ENOTSUP;
        return MAP_FAILED;
    }
    if (length == 0 || offset < 0) {
        errno = EINVAL;
        return MAP_FAILED;
    }
    if ((flags & MAP_ANONYMOUS) == 0 && fd < 0) {
        errno = EBADF;
        return MAP_FAILED;
    }

    void *mapping = malloc(length);
    if (mapping == NULL) {
        errno = ENOMEM;
        return MAP_FAILED;
    }

    if ((flags & MAP_ANONYMOUS) != 0) {
        memset(mapping, 0, length);
    } else if (read_mapping_contents(fd, mapping, length, offset) != 0) {
        int saved_errno = errno;
        free(mapping);
        errno = saved_errno;
        return MAP_FAILED;
    }

    if ((flags & MAP_SHARED) != 0) {
        int32_t hint_errno = (flags & MAP_ANONYMOUS) != 0
            ? __sinomo_wasix_mmap_shared_hint((uintptr_t)mapping, (uintptr_t)length)
            : __sinomo_wasix_mmap_shared_file_hint(fd, (uint64_t)offset, (uintptr_t)mapping, (uintptr_t)length);
        if (hint_errno != 0) {
            free(mapping);
            errno = hint_errno;
            return MAP_FAILED;
        }
    }

    return mapping;
}

int munmap(void *addr, size_t length) {
    (void)length;
    free(addr);
    return 0;
}

int msync(void *addr, size_t length, int flags) {
    (void)addr;
    (void)length;
    (void)flags;
    return 0;
}
