#ifndef SINOMO_BUSYBOX_WASI_COMPAT_SYS_SYSMACROS_H
#define SINOMO_BUSYBOX_WASI_COMPAT_SYS_SYSMACROS_H

#define major(dev) ((int)(((dev) >> 8) & 0xfff))
#define minor(dev) ((int)((dev) & 0xff))
#define makedev(maj, min) ((((maj) & 0xfff) << 8) | ((min) & 0xff))

#endif
