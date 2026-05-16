#ifndef SINOMO_BUSYBOX_WASI_COMPAT_FCNTL_H
#define SINOMO_BUSYBOX_WASI_COMPAT_FCNTL_H

#include_next <fcntl.h>

#ifndef F_DUPFD
#define F_DUPFD 0
#endif

#ifndef F_DUPFD_CLOEXEC
#define F_DUPFD_CLOEXEC 1030
#endif

#endif
