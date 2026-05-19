#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <wasi/api_wasix.h>

static const char sinomo_exec_magic[] = "__SINOMO_WASIX_ESCAPED_STRING_LIST_V1__\n";
extern char **environ;

static char *sinomo_exec_combine_strings(char *const strings[]) {
    size_t combined_len = sizeof(sinomo_exec_magic) - 1;
    for (char **ptr = (char **)strings; ptr != NULL && *ptr != NULL; ptr++) {
        for (const unsigned char *p = (const unsigned char *)*ptr; *p != 0; p++) {
            combined_len += (*p == '\n' || *p == '\r' || *p == '\\') ? 2 : 1;
        }
        combined_len += 1;
    }

    char *combined = malloc(combined_len + 1);
    if (combined == NULL) {
        errno = ENOMEM;
        return NULL;
    }

    char *out = combined;
    memcpy(out, sinomo_exec_magic, sizeof(sinomo_exec_magic) - 1);
    out += sizeof(sinomo_exec_magic) - 1;
    for (char **ptr = (char **)strings; ptr != NULL && *ptr != NULL; ptr++) {
        for (const unsigned char *p = (const unsigned char *)*ptr; *p != 0; p++) {
            if (*p == '\n') {
                *out++ = '\\';
                *out++ = 'n';
            } else if (*p == '\r') {
                *out++ = '\\';
                *out++ = 'r';
            } else if (*p == '\\') {
                *out++ = '\\';
                *out++ = '\\';
            } else {
                *out++ = (char)*p;
            }
        }
        *out++ = '\n';
    }
    *out = 0;
    return combined;
}

int execvp(const char *file, char *const argv[]) {
    if (file == NULL || argv == NULL) {
        errno = EFAULT;
        return -1;
    }
    char *combined_argv = sinomo_exec_combine_strings(argv);
    char *combined_env = sinomo_exec_combine_strings(environ);
    if (combined_argv == NULL || combined_env == NULL) {
        free(combined_argv);
        free(combined_env);
        return -1;
    }
    const char *search_path = getenv("PATH");
    if (search_path == NULL) {
        search_path = "";
    }

    __wasi_errno_t error = __wasi_proc_exec3(file, combined_argv, combined_env, 0, search_path);
    free(combined_argv);
    free(combined_env);
    errno = (int)error;
    return -1;
}
