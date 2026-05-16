#ifndef SINOMO_WASI_COMPAT_ARPA_INET_H
#define SINOMO_WASI_COMPAT_ARPA_INET_H

#include_next <arpa/inet.h>

#include <netinet/in.h>

static inline char *inet_ntoa(struct in_addr in)
{
    (void)in;
    return (char *)"0.0.0.0";
}

#endif
