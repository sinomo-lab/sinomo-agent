#ifndef SINOMO_WASI_COMPAT_SYS_SOCKET_H
#define SINOMO_WASI_COMPAT_SYS_SOCKET_H

#include_next <sys/socket.h>

#include <errno.h>

#ifndef SOCK_RDM
#define SOCK_RDM 4
#endif

#ifndef SOCK_SEQPACKET
#define SOCK_SEQPACKET 5
#endif

#ifndef SOCK_RAW
#define SOCK_RAW 3
#endif
#ifndef SO_REUSEADDR
#define SO_REUSEADDR 2
#endif
#ifndef SO_BROADCAST
#define SO_BROADCAST 6
#endif
#ifndef SO_KEEPALIVE
#define SO_KEEPALIVE 9
#endif

static inline int getsockname(int socket, struct sockaddr *address, socklen_t *address_len)
{
    (void)socket;
    (void)address;
    (void)address_len;
    errno = ENOTSUP;
    return -1;
}

static inline int getpeername(int socket, struct sockaddr *address, socklen_t *address_len)
{
    (void)socket;
    (void)address;
    (void)address_len;
    errno = ENOTSUP;
    return -1;
}

static inline int setsockopt(
    int socket,
    int level,
    int option_name,
    const void *option_value,
    socklen_t option_len
)
{
    (void)socket;
    (void)level;
    (void)option_name;
    (void)option_value;
    (void)option_len;
    errno = ENOTSUP;
    return -1;
}

static inline int socket(int domain, int type, int protocol)
{
    (void)domain;
    (void)type;
    (void)protocol;
    errno = ENOTSUP;
    return -1;
}

static inline int connect(int socket, const struct sockaddr *address, socklen_t address_len)
{
    (void)socket;
    (void)address;
    (void)address_len;
    errno = ENOTSUP;
    return -1;
}

static inline int bind(int socket, const struct sockaddr *address, socklen_t address_len)
{
    (void)socket;
    (void)address;
    (void)address_len;
    errno = ENOTSUP;
    return -1;
}

static inline int listen(int socket, int backlog)
{
    (void)socket;
    (void)backlog;
    errno = ENOTSUP;
    return -1;
}

static inline ssize_t sendto(
    int socket,
    const void *message,
    size_t length,
    int flags,
    const struct sockaddr *dest_addr,
    socklen_t dest_len
)
{
    (void)socket;
    (void)message;
    (void)length;
    (void)flags;
    (void)dest_addr;
    (void)dest_len;
    errno = ENOTSUP;
    return -1;
}

#endif
