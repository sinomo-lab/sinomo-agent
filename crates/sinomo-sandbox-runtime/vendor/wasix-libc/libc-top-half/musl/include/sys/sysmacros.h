#ifndef _SYS_SYSMACROS_H
#define _SYS_SYSMACROS_H

#include <sys/types.h>

#ifndef major
#define major(dev) ((unsigned int)(((dev) >> 8) & 0xfff))
#endif

#ifndef minor
#define minor(dev) ((unsigned int)(((dev) & 0xff) | (((dev) >> 12) & 0xfffff00)))
#endif

#ifndef makedev
#define makedev(major_id, minor_id) \
    ((dev_t)((((dev_t)(major_id) & 0xfff) << 8) | ((dev_t)(minor_id) & 0xff) | ((((dev_t)(minor_id) & 0xfffff00) << 12))))
#endif

#endif
