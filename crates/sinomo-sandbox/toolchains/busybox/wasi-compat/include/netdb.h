#ifndef SINOMO_BUSYBOX_WASI_COMPAT_NETDB_H
#define SINOMO_BUSYBOX_WASI_COMPAT_NETDB_H

#include <stdint.h>

struct hostent {
    char *h_name;
    char **h_aliases;
    int h_addrtype;
    int h_length;
    char **h_addr_list;
};

struct servent {
    char *s_name;
    char **s_aliases;
    int s_port;
    char *s_proto;
};

struct protoent {
    char *p_name;
    char **p_aliases;
    int p_proto;
};

struct addrinfo {
    int ai_flags;
    int ai_family;
    int ai_socktype;
    int ai_protocol;
    uint32_t ai_addrlen;
    void *ai_addr;
    char *ai_canonname;
    struct addrinfo *ai_next;
};

#define EAI_NONAME (-2)
#define EAI_SERVICE (-8)
#define EAI_FAIL (-4)
#define AI_CANONNAME 0x0002
#define AI_NUMERICHOST 0x0004
#define NI_MAXHOST 1025
#define NI_MAXSERV 32
#define NI_NUMERICHOST 1
#define NI_NUMERICSERV 2
#define NI_NAMEREQD 4
#define NI_NUMERICSCOPE 8
#define HOST_NOT_FOUND 1
#define NO_DATA 4

static int h_errno = HOST_NOT_FOUND;

static inline struct hostent *gethostbyname(const char *name) {
    (void)name;
    return 0;
}

static inline struct servent *getservbyname(const char *name, const char *proto) {
    (void)name;
    (void)proto;
    return 0;
}

static inline struct protoent *getprotobyname(const char *name) {
    (void)name;
    return 0;
}

static inline int getaddrinfo(
    const char *node,
    const char *service,
    const struct addrinfo *hints,
    struct addrinfo **res
) {
    (void)node;
    (void)service;
    (void)hints;
    if (res) {
        *res = 0;
    }
    return EAI_NONAME;
}

static inline void freeaddrinfo(struct addrinfo *res) {
    (void)res;
}

static inline int getnameinfo(
    const void *sa,
    uint32_t salen,
    char *host,
    uint32_t hostlen,
    char *serv,
    uint32_t servlen,
    int flags
) {
    (void)sa;
    (void)salen;
    (void)host;
    (void)hostlen;
    (void)serv;
    (void)servlen;
    (void)flags;
    return EAI_FAIL;
}

static inline const char *gai_strerror(int errcode) {
    (void)errcode;
    return "network name resolution is unavailable in the Sinomo WASI BusyBox build";
}

static inline const char *hstrerror(int errcode) {
    (void)errcode;
    return "network name resolution is unavailable in the Sinomo WASI BusyBox build";
}

#endif
