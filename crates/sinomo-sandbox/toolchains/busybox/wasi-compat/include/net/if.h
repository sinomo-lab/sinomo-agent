#ifndef SINOMO_WASI_COMPAT_NET_IF_H
#define SINOMO_WASI_COMPAT_NET_IF_H

#define IFNAMSIZ 16

struct if_nameindex {
    unsigned int if_index;
    char *if_name;
};

#endif
