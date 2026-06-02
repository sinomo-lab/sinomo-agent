#ifndef SINOMO_WASIX_PTY_H
#define SINOMO_WASIX_PTY_H 1

#include <sys/types.h>
#include <termios.h>

struct winsize;

int forkpty(int *amaster, char *name, const struct termios *termp, const struct winsize *winp);
int openpty(int *amaster, int *aslave, char *name, const struct termios *termp, const struct winsize *winp);

#endif
