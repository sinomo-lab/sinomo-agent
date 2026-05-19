#include <signal.h>
#include <setjmp.h>

/* wasix-libc provides siginterrupt but the current sysroot header does not
 * declare it consistently for upstream Bash's nojobs path. */
int siginterrupt(int, int);

/* The non-EH WASIX setjmp header defines sigjmp_buf but does not declare the
 * POSIX sigsetjmp/siglongjmp entrypoints. Declare the real libc symbols
 * without substituting behavior. */
int sigsetjmp(sigjmp_buf, int);
_Noreturn void siglongjmp(sigjmp_buf, int);
