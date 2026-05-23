#ifndef _SYS_TIMES_H
#define _SYS_TIMES_H

#ifdef __cplusplus
extern "C" {
#endif

#define __NEED_clock_t
#include <bits/alltypes.h>
#include <__struct_tms.h>

clock_t times(struct tms *);

#ifdef __cplusplus
}
#endif

#endif
