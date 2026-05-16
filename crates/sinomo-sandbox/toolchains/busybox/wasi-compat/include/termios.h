#ifndef SINOMO_WASI_COMPAT_TERMIOS_H
#define SINOMO_WASI_COMPAT_TERMIOS_H

#include <errno.h>

typedef unsigned int tcflag_t;
typedef unsigned char cc_t;
typedef unsigned int speed_t;

#define NCCS 32

struct termios {
    tcflag_t c_iflag;
    tcflag_t c_oflag;
    tcflag_t c_cflag;
    tcflag_t c_lflag;
    cc_t c_cc[NCCS];
    speed_t c_ispeed;
    speed_t c_ospeed;
};

#define VINTR 0
#define VQUIT 1
#define VERASE 2
#define VKILL 3
#define VEOF 4
#define VTIME 5
#define VMIN 6
#define VSWTC 7
#define VSTART 8
#define VSTOP 9
#define VSUSP 10
#define VEOL 11
#define VREPRINT 12
#define VDISCARD 13
#define VWERASE 14
#define VLNEXT 15
#define VEOL2 16

#define IGNBRK 0000001
#define BRKINT 0000002
#define IGNPAR 0000004
#define PARMRK 0000010
#define INPCK 0000020
#define ISTRIP 0000040
#define INLCR 0000100
#define IGNCR 0000200
#define ICRNL 0000400
#define IXON 0002000
#define IXOFF 0010000
#define IMAXBEL 0020000
#define IUTF8 0040000

#define OPOST 0000001
#define ONLCR 0000004
#define OCRNL 0000010
#define ONOCR 0000020
#define ONLRET 0000040

#define CSIZE 0000060
#define CS5 0000000
#define CS6 0000020
#define CS7 0000040
#define CS8 0000060
#define CSTOPB 0000100
#define CREAD 0000200
#define PARENB 0000400
#define PARODD 0001000
#define HUPCL 0002000
#define CLOCAL 0004000

#define ISIG 0000001
#define ICANON 0000002
#define ECHO 0000010
#define ECHOE 0000020
#define ECHOK 0000040
#define ECHONL 0000100
#define NOFLSH 0000200
#define TOSTOP 0000400
#define IEXTEN 0100000

#define TCSANOW 0
#define TCSADRAIN 1
#define TCSAFLUSH 2
#define TCOOFF 0
#define TCOON 1
#define TCIOFF 2
#define TCION 3
#define TCIFLUSH 0
#define TCOFLUSH 1
#define TCIOFLUSH 2

#define B0 0
#define B50 50
#define B75 75
#define B110 110
#define B134 134
#define B150 150
#define B200 200
#define B300 300
#define B600 600
#define B1200 1200
#define B1800 1800
#define B2400 2400
#define B4800 4800
#define B9600 9600
#define B19200 19200
#define B38400 38400
#define B57600 57600
#define B115200 115200
#define B230400 230400
#define TIOCGWINSZ 0x5413

struct winsize {
    unsigned short ws_row;
    unsigned short ws_col;
    unsigned short ws_xpixel;
    unsigned short ws_ypixel;
};

static inline int tcgetattr(int fd, struct termios *termios_p)
{
    (void)fd;
    (void)termios_p;
    errno = ENOTTY;
    return -1;
}

static inline int tcsetattr(int fd, int optional_actions, const struct termios *termios_p)
{
    (void)fd;
    (void)optional_actions;
    (void)termios_p;
    errno = ENOTTY;
    return -1;
}

static inline int tcdrain(int fd)
{
    (void)fd;
    errno = ENOTTY;
    return -1;
}

static inline int tcflow(int fd, int action)
{
    (void)fd;
    (void)action;
    errno = ENOTTY;
    return -1;
}

static inline int tcflush(int fd, int queue_selector)
{
    (void)fd;
    (void)queue_selector;
    errno = ENOTTY;
    return -1;
}

static inline speed_t cfgetispeed(const struct termios *termios_p)
{
    return termios_p ? termios_p->c_ispeed : 0;
}

static inline speed_t cfgetospeed(const struct termios *termios_p)
{
    return termios_p ? termios_p->c_ospeed : 0;
}

static inline int cfsetispeed(struct termios *termios_p, speed_t speed)
{
    if (termios_p) {
        termios_p->c_ispeed = speed;
    }
    return 0;
}

static inline int cfsetospeed(struct termios *termios_p, speed_t speed)
{
    if (termios_p) {
        termios_p->c_ospeed = speed;
    }
    return 0;
}

#endif
