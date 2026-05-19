#include <errno.h>
#include <stddef.h>

/*
 * wasi-sdk's emulated mman implementation maps files into malloc-backed memory
 * and exposes mmap/munmap/mprotect, but the header also declares msync without
 * providing a symbol. There is no honest way to flush the malloc-backed copy
 * back to the original file from here, so report the unsupported operation if
 * Python code calls mmap.flush(). Read-only mmap users still get the real
 * wasi-sdk mmap behavior.
 */
int msync(void *addr, size_t length, int flags) {
    (void)addr;
    (void)length;
    (void)flags;
    errno = ENOTSUP;
    return -1;
}
