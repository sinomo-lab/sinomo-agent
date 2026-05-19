#include <stddef.h>

int dladdr(const void *addr, void *info) {
    (void)addr;
    (void)info;
    return 0;
}

int posix_madvise(void *addr, size_t len, int advice) {
    (void)addr;
    (void)len;
    (void)advice;
    return 0;
}

int mprotect(void *addr, size_t len, int prot) {
    (void)addr;
    (void)len;
    (void)prot;
    return 0;
}
