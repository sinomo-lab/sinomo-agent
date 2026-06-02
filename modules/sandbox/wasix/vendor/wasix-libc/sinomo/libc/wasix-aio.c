#include <aio.h>
#include <errno.h>

static int sinomo_aio_unsupported(void) {
    errno = ENOSYS;
    return -1;
}

int aio_cancel(int fd, struct aiocb *aiocbp) {
    (void)fd;
    (void)aiocbp;
    return sinomo_aio_unsupported();
}

int aio_error(const struct aiocb *aiocbp) {
    (void)aiocbp;
    return sinomo_aio_unsupported();
}

int aio_fsync(int op, struct aiocb *aiocbp) {
    (void)op;
    (void)aiocbp;
    return sinomo_aio_unsupported();
}

int aio_read(struct aiocb *aiocbp) {
    (void)aiocbp;
    return sinomo_aio_unsupported();
}

ssize_t aio_return(struct aiocb *aiocbp) {
    (void)aiocbp;
    errno = ENOSYS;
    return -1;
}

int aio_suspend(const struct aiocb *const list[], int nent, const struct timespec *timeout) {
    (void)list;
    (void)nent;
    (void)timeout;
    return sinomo_aio_unsupported();
}

int aio_write(struct aiocb *aiocbp) {
    (void)aiocbp;
    return sinomo_aio_unsupported();
}

int lio_listio(int mode, struct aiocb *restrict const list[restrict], int nent, struct sigevent *restrict sig) {
    (void)mode;
    (void)list;
    (void)nent;
    (void)sig;
    return sinomo_aio_unsupported();
}
