#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <time.h>

const uint8_t qjsc_repl[] = { 0 };
const uint32_t qjsc_repl_size = 0;

FILE *popen(const char *command, const char *type) {
    (void)command;
    (void)type;
    errno = ENOSYS;
    return NULL;
}

int pclose(FILE *stream) {
    (void)stream;
    errno = ENOSYS;
    return -1;
}

FILE *tmpfile(void) {
    errno = ENOSYS;
    return NULL;
}

int kill(pid_t pid, int sig) {
    (void)pid;
    (void)sig;
    errno = ENOSYS;
    return -1;
}

void *dlopen(const char *filename, int flags) {
    (void)filename;
    (void)flags;
    errno = ENOSYS;
    return NULL;
}

void *dlsym(void *handle, const char *symbol) {
    (void)handle;
    (void)symbol;
    errno = ENOSYS;
    return NULL;
}

int dlclose(void *handle) {
    (void)handle;
    errno = ENOSYS;
    return -1;
}

char *dlerror(void) {
    return "dynamic loading is unsupported in the Sinomo WASIp1 QuickJS package";
}

int pthread_mutex_init(pthread_mutex_t *mutex, const pthread_mutexattr_t *attr) {
    (void)mutex;
    (void)attr;
    return 0;
}

int pthread_mutex_destroy(pthread_mutex_t *mutex) {
    (void)mutex;
    return 0;
}

int pthread_mutex_lock(pthread_mutex_t *mutex) {
    (void)mutex;
    return 0;
}

int pthread_mutex_unlock(pthread_mutex_t *mutex) {
    (void)mutex;
    return 0;
}

int pthread_cond_init(pthread_cond_t *cond, const pthread_condattr_t *attr) {
    (void)cond;
    (void)attr;
    return 0;
}

int pthread_cond_destroy(pthread_cond_t *cond) {
    (void)cond;
    return 0;
}

int pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex) {
    (void)cond;
    (void)mutex;
    return ENOSYS;
}

int pthread_cond_timedwait(pthread_cond_t *cond, pthread_mutex_t *mutex, const struct timespec *abstime) {
    (void)cond;
    (void)mutex;
    (void)abstime;
    return ENOSYS;
}

int pthread_cond_signal(pthread_cond_t *cond) {
    (void)cond;
    return 0;
}

int pthread_attr_init(pthread_attr_t *attr) {
    (void)attr;
    return 0;
}

int pthread_attr_destroy(pthread_attr_t *attr) {
    (void)attr;
    return 0;
}

int pthread_attr_setdetachstate(pthread_attr_t *attr, int detachstate) {
    (void)attr;
    (void)detachstate;
    return 0;
}

int pthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine)(void *), void *arg) {
    (void)thread;
    (void)attr;
    (void)start_routine;
    (void)arg;
    return ENOSYS;
}
