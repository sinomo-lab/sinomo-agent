#include <stdlib.h>

void __env_rm_add(char *old, char *new) {
    static char **env_alloced;
    static size_t env_alloced_n;

    for (size_t i = 0; i < env_alloced_n; i++) {
        if (env_alloced[i] == old) {
            env_alloced[i] = new;
            free(old);
            return;
        }
        if (!env_alloced[i] && new) {
            env_alloced[i] = new;
            new = 0;
        }
    }
    if (!new) {
        return;
    }
    char **entries = realloc(env_alloced, sizeof *entries * (env_alloced_n + 1));
    if (!entries) {
        return;
    }
    env_alloced = entries;
    env_alloced[env_alloced_n++] = new;
}
