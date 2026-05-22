#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define SINOMO_WASM_PAGE_SIZE 65536
#define SINOMO_MMAP_REGISTRY_CAP 256
#define SINOMO_SHM_DIR "/tmp/.sinomo-shm"

#if !defined(MAP_ANONYMOUS) && defined(MAP_ANON)
#define MAP_ANONYMOUS MAP_ANON
#endif

int32_t __sinomo_wasix_mmap_shared_hint(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("mmap_shared_hint")));
int32_t __sinomo_wasix_mmap_shared_file_hint(int32_t fd, uint64_t file_offset, uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("mmap_shared_file_hint")));
int32_t __sinomo_wasix_mmap_protect_none(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("mmap_protect_none")));
int32_t __sinomo_wasix_mmap_protect_read(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("mmap_protect_read")));
int32_t __sinomo_wasix_mmap_protect_read_write(uintptr_t offset, uintptr_t len)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("mmap_protect_read_write")));
int32_t __sinomo_wasix_fd_touch(int32_t fd)
    __attribute__((__import_module__("wasix_32v1"), __import_name__("fd_touch")));

struct mapping_record {
    void *addr;
    size_t len;
    int fd;
    off_t offset;
    int flags;
    int prot;
    int allocated;
    int active;
};

static struct mapping_record mapping_records[SINOMO_MMAP_REGISTRY_CAP];
static struct mapping_record protected_records[SINOMO_MMAP_REGISTRY_CAP];
static int mmap_atexit_registered = 0;
static int mlockall_state = 0;
static unsigned long shm_unlink_generation = 0;

static void clear_protected_record(int index);

static void restore_protected_mappings(void) {
    for (int index = 0; index < SINOMO_MMAP_REGISTRY_CAP; index++) {
        if (!protected_records[index].active)
            continue;
        (void)__sinomo_wasix_mmap_protect_read_write(
            (uintptr_t)protected_records[index].addr,
            (uintptr_t)protected_records[index].len);
        clear_protected_record(index);
    }
}

static void ensure_mmap_atexit_registered(void) {
    if (mmap_atexit_registered)
        return;
    mmap_atexit_registered = 1;
    atexit(restore_protected_mappings);
}

static void clear_mapping_record(int index) {
    if (index < 0 || index >= SINOMO_MMAP_REGISTRY_CAP)
        return;
    if (mapping_records[index].active && mapping_records[index].fd >= 0)
        close(mapping_records[index].fd);
    memset(&mapping_records[index], 0, sizeof(mapping_records[index]));
    mapping_records[index].fd = -1;
}

static int find_mapping_record(void *addr) {
    for (int index = 0; index < SINOMO_MMAP_REGISTRY_CAP; index++) {
        if (mapping_records[index].active && mapping_records[index].addr == addr)
            return index;
    }
    return -1;
}

static int find_mapping_record_containing(void *addr, size_t length) {
    uintptr_t start = (uintptr_t)addr;
    uintptr_t end = start + length;
    if (end < start)
        return -1;
    for (int index = 0; index < SINOMO_MMAP_REGISTRY_CAP; index++) {
        if (!mapping_records[index].active)
            continue;
        uintptr_t record_start = (uintptr_t)mapping_records[index].addr;
        uintptr_t record_end = record_start + mapping_records[index].len;
        if (record_end < record_start)
            continue;
        if (start >= record_start && end <= record_end)
            return index;
    }
    return -1;
}

static int reserve_mapping_record(void *addr) {
    int existing = find_mapping_record(addr);
    if (existing >= 0)
        return existing;
    for (int index = 0; index < SINOMO_MMAP_REGISTRY_CAP; index++) {
        if (!mapping_records[index].active)
            return index;
    }
    errno = ENOMEM;
    return -1;
}

static int reserve_protected_record(void *addr) {
    for (int index = 0; index < SINOMO_MMAP_REGISTRY_CAP; index++) {
        if (protected_records[index].active && protected_records[index].addr == addr)
            return index;
    }
    for (int index = 0; index < SINOMO_MMAP_REGISTRY_CAP; index++) {
        if (!protected_records[index].active)
            return index;
    }
    errno = ENOMEM;
    return -1;
}

static void clear_protected_record(int index) {
    if (index < 0 || index >= SINOMO_MMAP_REGISTRY_CAP)
        return;
    memset(&protected_records[index], 0, sizeof(protected_records[index]));
}

static void *take_reusable_protected_mapping(size_t length, int *allocated) {
    for (int index = 0; index < SINOMO_MMAP_REGISTRY_CAP; index++) {
        if (!protected_records[index].active || protected_records[index].len < length)
            continue;
        void *addr = protected_records[index].addr;
        int was_allocated = protected_records[index].allocated;
        int32_t protect_errno = __sinomo_wasix_mmap_protect_read_write(
            (uintptr_t)addr,
            (uintptr_t)protected_records[index].len);
        if (protect_errno != 0) {
            errno = protect_errno;
            return MAP_FAILED;
        }
        clear_protected_record(index);
        if (allocated != NULL)
            *allocated = was_allocated;
        return addr;
    }
    return NULL;
}

static int sync_mapping_record(int index, void *addr, size_t length) {
    if (index < 0 || index >= SINOMO_MMAP_REGISTRY_CAP) {
        errno = EINVAL;
        return -1;
    }
    struct mapping_record *record = &mapping_records[index];
    if (!record->active)
        return 0;
    if ((record->flags & MAP_SHARED) == 0 || (record->flags & MAP_ANONYMOUS) != 0)
        return 0;
    if ((record->prot & PROT_WRITE) == 0)
        return 0;

    uintptr_t sync_start = (uintptr_t)addr;
    uintptr_t record_start = (uintptr_t)record->addr;
    size_t offset_in_mapping = sync_start > record_start ? (size_t)(sync_start - record_start) : 0;
    if (offset_in_mapping > record->len)
        offset_in_mapping = record->len;
    size_t remaining = record->len - offset_in_mapping;
    if (length != 0 && length < remaining)
        remaining = length;

    const char *cursor = (const char *)record->addr + offset_in_mapping;
    off_t file_offset = record->offset + (off_t)offset_in_mapping;
    while (remaining > 0) {
        ssize_t written = pwrite(record->fd, cursor, remaining, file_offset);
        if (written < 0)
            return -1;
        if (written == 0) {
            errno = EIO;
            return -1;
        }
        cursor += written;
        file_offset += written;
        remaining -= (size_t)written;
    }
    (void)__sinomo_wasix_fd_touch(record->fd);
    return 0;
}

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

static size_t mapping_page_size(void) {
    long page_size = sysconf(_SC_PAGE_SIZE);
    if (page_size <= 0)
        return SINOMO_WASM_PAGE_SIZE;
    return (size_t)page_size;
}

static int round_up_size(size_t value, size_t alignment, size_t *rounded) {
    if (alignment == 0)
        return -1;
    size_t remainder = value % alignment;
    if (remainder == 0) {
        *rounded = value;
        return 0;
    }
    size_t increment = alignment - remainder;
    if (value > SIZE_MAX - increment)
        return -1;
    *rounded = value + increment;
    return 0;
}

static int shm_object_path(const char *name, char *buffer, size_t buffer_len) {
    if (name == NULL || name[0] == '\0') {
        errno = EINVAL;
        return -1;
    }
    size_t path_max = PATH_MAX;
    size_t len = strnlen(name, path_max + 1);
    if (len >= path_max) {
        errno = ENAMETOOLONG;
        return -1;
    }

    const char *object = name[0] == '/' ? name + 1 : name;
    if (object[0] == '\0') {
        errno = EINVAL;
        return -1;
    }
    if (strcmp(object, ".") == 0 || strcmp(object, "..") == 0) {
        errno = EINVAL;
        return -1;
    }
    size_t name_max = NAME_MAX;
    if (strlen(object) > name_max) {
        errno = ENAMETOOLONG;
        return -1;
    }
    if (strchr(object, '/') != NULL) {
        errno = EINVAL;
        return -1;
    }

    int needed = snprintf(buffer, buffer_len, "%s/%s", SINOMO_SHM_DIR, object);
    if (needed < 0 || (size_t)needed >= buffer_len) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

int shm_open(const char *name, int oflag, mode_t mode) {
    char path[PATH_MAX];
    if (shm_object_path(name, path, sizeof(path)) != 0)
        return -1;
    if (mkdir(SINOMO_SHM_DIR, 01777) != 0 && errno != EEXIST)
        return -1;
    return open(path, oflag | O_CLOEXEC, mode);
}

int shm_unlink(const char *name) {
    char path[PATH_MAX];
    if (shm_object_path(name, path, sizeof(path)) != 0)
        return -1;
    struct stat st;
    if (stat(path, &st) != 0)
        return -1;
    uid_t euid = geteuid();
    if (euid != 0 && euid != st.st_uid) {
        errno = EACCES;
        return -1;
    }
    char tombstone[PATH_MAX];
    int needed = snprintf(
        tombstone,
        sizeof(tombstone),
        "%s/.unlinked.%ld.%lu",
        SINOMO_SHM_DIR,
        (long)getpid(),
        shm_unlink_generation++);
    if (needed < 0 || (size_t)needed >= sizeof(tombstone)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    if (rename(path, tombstone) != 0)
        return -1;
    return unlink(tombstone);
}

static int apply_mapping_protection(void *addr, size_t len, int prot) {
    int32_t protect_errno;
    if ((prot & PROT_NONE) == PROT_NONE && (prot & (PROT_READ | PROT_WRITE | PROT_EXEC)) == 0) {
        protect_errno = __sinomo_wasix_mmap_protect_none((uintptr_t)addr, (uintptr_t)len);
    } else if ((prot & PROT_WRITE) == 0) {
        protect_errno = __sinomo_wasix_mmap_protect_read((uintptr_t)addr, (uintptr_t)len);
    } else {
        protect_errno = __sinomo_wasix_mmap_protect_read_write((uintptr_t)addr, (uintptr_t)len);
    }
    if (protect_errno != 0) {
        errno = protect_errno;
        return -1;
    }
    return 0;
}

void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset) {
    ensure_mmap_atexit_registered();
    if (addr != NULL && (flags & MAP_FIXED) == 0) {
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
    if (((flags & MAP_SHARED) == 0 && (flags & MAP_PRIVATE) == 0) ||
        ((flags & MAP_SHARED) != 0 && (flags & MAP_PRIVATE) != 0)) {
        errno = EINVAL;
        return MAP_FAILED;
    }

    size_t page_size = mapping_page_size();
    if (offset % (off_t)page_size != 0 || (addr != NULL && ((uintptr_t)addr % page_size) != 0)) {
        errno = EINVAL;
        return MAP_FAILED;
    }

    size_t page_aligned_length = 0;
    if (round_up_size(length, page_size, &page_aligned_length) != 0) {
        errno = ENOMEM;
        return MAP_FAILED;
    }
    size_t allocation_length = 0;
    if (round_up_size(page_aligned_length, SINOMO_WASM_PAGE_SIZE, &allocation_length) != 0) {
        errno = ENOMEM;
        return MAP_FAILED;
    }
    if ((flags & MAP_FIXED) != 0 && addr != NULL) {
        struct rlimit address_limit;
        if (getrlimit(RLIMIT_AS, &address_limit) == 0 && address_limit.rlim_cur != RLIM_INFINITY) {
            uintptr_t start = (uintptr_t)addr;
            if (start > (uintptr_t)address_limit.rlim_cur ||
                page_aligned_length > (size_t)((uintptr_t)address_limit.rlim_cur - start)) {
                errno = ENOMEM;
                return MAP_FAILED;
            }
        }
    }
    if ((uint64_t)page_aligned_length > UINT32_MAX ||
        (uint64_t)offset > UINT32_MAX - (uint64_t)page_aligned_length) {
        errno = EOVERFLOW;
        return MAP_FAILED;
    }
    if ((mlockall_state & MCL_FUTURE) != 0) {
        struct rlimit memlock_limit;
        if (getrlimit(RLIMIT_MEMLOCK, &memlock_limit) == 0 &&
            memlock_limit.rlim_cur != RLIM_INFINITY &&
            page_aligned_length > (size_t)memlock_limit.rlim_cur) {
            errno = EAGAIN;
            return MAP_FAILED;
        }
    }

    int mapping_fd = -1;
    if ((flags & MAP_ANONYMOUS) == 0) {
        struct stat st;
        if (fstat(fd, &st) != 0)
            return MAP_FAILED;
        if (!S_ISREG(st.st_mode)) {
            errno = ENODEV;
            return MAP_FAILED;
        }
        if ((flags & MAP_SHARED) != 0 && (uint64_t)st.st_size >= (uint64_t)page_size) {
            uint64_t mapped_end = (uint64_t)offset <= UINT64_MAX - (uint64_t)page_aligned_length
                ? (uint64_t)offset + (uint64_t)page_aligned_length
                : UINT64_MAX;
            uint64_t conservative_object_end = (uint64_t)st.st_size > UINT64_MAX / 2
                ? UINT64_MAX
                : (uint64_t)st.st_size * 2;
            if (mapped_end > conservative_object_end) {
                errno = ENXIO;
                return MAP_FAILED;
            }
        }
        if ((flags & MAP_SHARED) != 0 && (prot & PROT_WRITE) != 0) {
            int fd_flags = fcntl(fd, F_GETFL);
            if (fd_flags < 0)
                return MAP_FAILED;
            int access_mode = fd_flags & O_ACCMODE;
            if (access_mode == O_RDONLY) {
                errno = EACCES;
                return MAP_FAILED;
            }
        }
        mapping_fd = fcntl(fd, F_DUPFD_CLOEXEC, 0);
        if (mapping_fd < 0)
            return MAP_FAILED;
    }

    int existing_index = addr == NULL ? -1 : find_mapping_record(addr);
    int allocated = 0;
    void *mapping = addr;
    if (mapping == NULL) {
        mapping = take_reusable_protected_mapping(allocation_length, &allocated);
        if (mapping == MAP_FAILED) {
            if (mapping_fd >= 0)
                close(mapping_fd);
            return MAP_FAILED;
        }
    }
    if (mapping != NULL) {
        int32_t protect_errno = __sinomo_wasix_mmap_protect_read_write(
            (uintptr_t)mapping,
            (uintptr_t)allocation_length);
        if (protect_errno != 0) {
            if (mapping_fd >= 0)
                close(mapping_fd);
            errno = protect_errno;
            return MAP_FAILED;
        }
        int protected_index = reserve_protected_record(mapping);
        if (protected_index >= 0)
            clear_protected_record(protected_index);
    }
    if (mapping == NULL && posix_memalign(&mapping, SINOMO_WASM_PAGE_SIZE, allocation_length) != 0) {
        if (mapping_fd >= 0)
            close(mapping_fd);
        errno = ENOMEM;
        return MAP_FAILED;
    }
    if (addr == NULL)
        allocated = 1;
    else if (existing_index >= 0)
        allocated = mapping_records[existing_index].allocated;

    if ((flags & MAP_ANONYMOUS) != 0) {
        memset(mapping, 0, allocation_length);
    } else if (read_mapping_contents(fd, mapping, page_aligned_length, offset) != 0) {
        int saved_errno = errno;
        if (addr == NULL)
            free(mapping);
        if (mapping_fd >= 0)
            close(mapping_fd);
        errno = saved_errno;
        return MAP_FAILED;
    } else {
        (void)__sinomo_wasix_fd_touch(mapping_fd >= 0 ? mapping_fd : fd);
    }

    if ((flags & MAP_SHARED) != 0) {
        int32_t hint_errno = (flags & MAP_ANONYMOUS) != 0
            ? __sinomo_wasix_mmap_shared_hint((uintptr_t)mapping, (uintptr_t)page_aligned_length)
            : __sinomo_wasix_mmap_shared_file_hint(fd, (uint64_t)offset, (uintptr_t)mapping, (uintptr_t)page_aligned_length);
        if (hint_errno != 0) {
            if (addr == NULL)
                free(mapping);
            if (mapping_fd >= 0)
                close(mapping_fd);
            errno = hint_errno;
            return MAP_FAILED;
        }
    }

    int record_index = reserve_mapping_record(mapping);
    if (record_index < 0) {
        if (addr == NULL)
            free(mapping);
        if (mapping_fd >= 0)
            close(mapping_fd);
        return MAP_FAILED;
    }
    clear_mapping_record(record_index);
    mapping_records[record_index] = (struct mapping_record){
        .addr = mapping,
        .len = page_aligned_length,
        .fd = mapping_fd,
        .offset = offset,
        .flags = flags,
        .prot = prot,
        .allocated = allocated,
        .active = 1,
    };
    if (apply_mapping_protection(mapping, allocation_length, prot) != 0) {
        int saved_errno = errno;
        clear_mapping_record(record_index);
        if (addr == NULL)
            free(mapping);
        errno = saved_errno;
        return MAP_FAILED;
    }
    return mapping;
}

int mprotect(void *addr, size_t len, int prot) {
    if (addr == NULL || len == 0 || ((uintptr_t)addr % mapping_page_size()) != 0) {
        errno = EINVAL;
        return -1;
    }
    size_t page_aligned_length = 0;
    if (round_up_size(len, mapping_page_size(), &page_aligned_length) != 0) {
        errno = ENOMEM;
        return -1;
    }
    size_t protected_length = 0;
    if (round_up_size(page_aligned_length, SINOMO_WASM_PAGE_SIZE, &protected_length) != 0) {
        errno = ENOMEM;
        return -1;
    }
    int index = find_mapping_record_containing(addr, page_aligned_length);
    if (index < 0) {
        errno = ENOMEM;
        return -1;
    }
    if (apply_mapping_protection(addr, protected_length, prot) != 0)
        return -1;
    mapping_records[index].prot = prot;
    return 0;
}

int munmap(void *addr, size_t length) {
    if (addr == NULL || length == 0) {
        errno = EINVAL;
        return -1;
    }
    if (((uintptr_t)addr % mapping_page_size()) != 0) {
        errno = EINVAL;
        return -1;
    }
    int index = find_mapping_record(addr);
    if (index < 0)
        return 0;
    int sync_result = sync_mapping_record(index, addr, length);
    int saved_errno = errno;
    int allocated = mapping_records[index].allocated;
    void *allocation = mapping_records[index].addr;
    size_t mapped_len = mapping_records[index].len;
    int32_t protect_errno = __sinomo_wasix_mmap_protect_none((uintptr_t)allocation, (uintptr_t)mapped_len);
    int protected_index = reserve_protected_record(allocation);
    if (protected_index >= 0) {
        protected_records[protected_index] = (struct mapping_record){
            .addr = allocation,
            .len = mapped_len,
            .fd = -1,
            .allocated = allocated,
            .active = 1,
        };
    }
    clear_mapping_record(index);
    if (allocated && protect_errno != 0)
        free(allocation);
    if (protect_errno != 0) {
        errno = protect_errno;
        return -1;
    }
    if (sync_result != 0) {
        errno = saved_errno;
        return -1;
    }
    return 0;
}

int msync(void *addr, size_t length, int flags) {
    (void)flags;
    if (addr == NULL || length == 0) {
        errno = EINVAL;
        return -1;
    }
    int index = find_mapping_record_containing(addr, length);
    if (index < 0) {
        errno = ENOMEM;
        return -1;
    }
    return sync_mapping_record(index, addr, length);
}

int mlock(const void *addr, size_t len) {
    if (addr == NULL && len != 0) {
        errno = ENOMEM;
        return -1;
    }
    struct rlimit memlock_limit;
    if (getrlimit(RLIMIT_MEMLOCK, &memlock_limit) == 0 &&
        memlock_limit.rlim_cur != RLIM_INFINITY &&
        len > (size_t)memlock_limit.rlim_cur) {
        errno = EAGAIN;
        return -1;
    }
    return 0;
}

int munlock(const void *addr, size_t len) {
    if (addr == NULL && len != 0) {
        errno = ENOMEM;
        return -1;
    }
    return 0;
}

int mlockall(int flags) {
    if ((flags & ~(MCL_CURRENT | MCL_FUTURE)) != 0 || flags == 0) {
        errno = EINVAL;
        return -1;
    }
    mlockall_state = flags;
    return 0;
}

int munlockall(void) {
    mlockall_state = 0;
    return 0;
}

int mlock2(const void *addr, size_t len, unsigned flags) {
    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }
    return mlock(addr, len);
}
