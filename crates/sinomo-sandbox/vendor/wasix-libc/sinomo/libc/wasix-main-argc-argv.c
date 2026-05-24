#include <stdint.h>
#include <stdlib.h>

#define SINOMO_WASIX_MAX_ARGC 128
#define SINOMO_WASIX_MAX_ARGV_BYTES 16384

extern int __main_argc_argv(int argc, char **argv);
extern int __wasi_args_sizes_get(uint32_t *argc, uint32_t *argv_buf_size);
extern int __wasi_args_get(char **argv, char *argv_buf);

int __main_void(void) {
    uint32_t argc = 0;
    uint32_t argv_buf_size = 0;
    char *argv[SINOMO_WASIX_MAX_ARGC + 1];
    char argv_buf[SINOMO_WASIX_MAX_ARGV_BYTES];

    if (__wasi_args_sizes_get(&argc, &argv_buf_size) != 0) {
        _Exit(71);
    }
    if (argc > SINOMO_WASIX_MAX_ARGC || argv_buf_size > SINOMO_WASIX_MAX_ARGV_BYTES) {
        _Exit(71);
    }
    if (__wasi_args_get(argv, argv_buf) != 0) {
        _Exit(71);
    }
    argv[argc] = 0;
    return __main_argc_argv((int)argc, argv);
}
