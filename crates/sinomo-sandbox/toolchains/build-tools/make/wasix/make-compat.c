#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <wasi/api.h>

#define SINOMO_MAKE_MAX_ARGC 256
#define SINOMO_MAKE_MAX_ARGV_BYTES 32768

extern char **environ;
extern int main(int argc, char **argv, char **env);

char *getlogin(void) {
    char *name = getenv("LOGNAME");
    if (name != NULL && *name != 0) {
        return name;
    }
    name = getenv("USER");
    if (name != NULL && *name != 0) {
        return name;
    }
    return "sinomo";
}

int getlogin_r(char *buf, size_t bufsize) {
    const char *name = getlogin();
    size_t len = strlen(name);
    if (buf == NULL) {
        return EFAULT;
    }
    if (bufsize <= len) {
        return ERANGE;
    }
    memcpy(buf, name, len + 1);
    return 0;
}

int __main_argc_argv(int argc, char **argv) {
    return main(argc, argv, environ);
}

int __main_void(void) {
    __wasi_size_t argc = 0;
    __wasi_size_t argv_buf_size = 0;
    char *argv[SINOMO_MAKE_MAX_ARGC + 1];
    char argv_buf[SINOMO_MAKE_MAX_ARGV_BYTES];

    if (__wasi_args_sizes_get(&argc, &argv_buf_size) != 0) {
        _Exit(71);
    }
    if (argc > SINOMO_MAKE_MAX_ARGC || argv_buf_size > SINOMO_MAKE_MAX_ARGV_BYTES) {
        _Exit(71);
    }
    if (__wasi_args_get((uint8_t **)argv, (uint8_t *)argv_buf) != 0) {
        _Exit(71);
    }
    argv[argc] = 0;
    return __main_argc_argv((int)argc, argv);
}
