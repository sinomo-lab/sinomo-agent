#!/usr/bin/env bash
set -euo pipefail

WASI_SDK_VERSION="${WASI_SDK_VERSION:-24}"
WASI_SDK_VERSION_FULL="${WASI_SDK_VERSION_FULL:-24.0}"
WASI_SDK_ARCH="${WASI_SDK_ARCH:-x86_64}"
WASI_SDK_OS="${WASI_SDK_OS:-linux}"
WASI_SDK_SHA256="${WASI_SDK_SHA256:-c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5}"
WASIX_LIBC_VERSION="${WASIX_LIBC_VERSION:-v2026-05-12.1}"
WASIX_LIBC_TARBALL="${WASIX_LIBC_TARBALL:-sysroot.tar.gz}"
WASIX_LIBC_SHA256="${WASIX_LIBC_SHA256:-e9f7435c0fdc050c1f813f83d3b22388ba3b16d1303ced510915eb028a38abb4}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
TARGET_DIR="${TARGET_DIR:-"${REPO_ROOT}/target"}"
WORKDIR="${WORKDIR:-"${TARGET_DIR}/sinomo-build-tools-wasix-sysroot-work"}"
SYSROOT_OUTDIR="${SYSROOT_OUTDIR:-"${TARGET_DIR}/sinomo-build-tools-wasix-sysroot/sysroot"}"
HOST_ENV_OUT="${HOST_ENV_OUT:-}"
SYSROOT_PATCH_DIR="${ROOT}/sysroot"
SYSROOT_LIBC_DIR="${SYSROOT_PATCH_DIR}/libc"
SYSROOT_INCLUDE_DIR="${SYSROOT_PATCH_DIR}/include"
SYSROOT_INCLUDE_PATCH_DIR="${SYSROOT_PATCH_DIR}/include-patches"

WASI_SDK_TARBALL="wasi-sdk-${WASI_SDK_VERSION_FULL}-${WASI_SDK_ARCH}-${WASI_SDK_OS}.tar.gz"
WASI_SDK_URL="https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_SDK_VERSION}/${WASI_SDK_TARBALL}"
WASIX_LIBC_URL="${WASIX_LIBC_URL:-https://github.com/wasix-org/wasix-libc/releases/download/${WASIX_LIBC_VERSION}/${WASIX_LIBC_TARBALL}}"

download() {
    local url="$1"
    local output="$2"
    if [ ! -f "${output}" ]; then
        curl -L --fail --retry 3 --output "${output}" "${url}"
    fi
}

verify_sha256() {
    local expected="$1"
    local file="$2"
    if [ -z "${expected}" ]; then
        echo "sha256 not pinned for ${file}" >&2
        exit 1
    fi
    local actual
    actual="$(sha256sum "${file}" | awk '{print $1}')"
    if [ "${actual}" != "${expected}" ]; then
        echo "sha256 mismatch for ${file}: expected ${expected}, actual ${actual}" >&2
        exit 1
    fi
}

apply_sysroot_header_patches() {
    local sysroot="$1"
    while IFS= read -r patch_file; do
        local relative="${patch_file#"${SYSROOT_INCLUDE_PATCH_DIR}/"}"
        case "${relative}" in
            *.append)
                local target="${sysroot}/include/${relative%.append}"
                if [ ! -f "${target}" ]; then
                    echo "sysroot header missing for patch: ${target}" >&2
                    exit 1
                fi
                if ! grep -q "SINOMO_WASIX_" "${target}"; then
                    cat "${patch_file}" >>"${target}"
                fi
                ;;
            *)
                mkdir -p "$(dirname "${sysroot}/include/${relative}")"
                cp "${patch_file}" "${sysroot}/include/${relative}"
                ;;
        esac
    done < <(find "${SYSROOT_INCLUDE_PATCH_DIR}" -type f | sort)
}

compile_sysroot_object() {
    local source="$1"
    local output="$2"
    shift 2
    "${WASI_SDK}/bin/clang" \
        --target=wasm32-wasi \
        --sysroot="${SYSROOT_OUTDIR}" \
        -matomics \
        -mbulk-memory \
        -mmutable-globals \
        -pthread \
        -mthread-model posix \
        -Os \
        "$@" \
        -c "${source}" \
        -o "${output}"
}

mkdir -p "${WORKDIR}/downloads"
download "${WASI_SDK_URL}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
download "${WASIX_LIBC_URL}" "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"
verify_sha256 "${WASI_SDK_SHA256}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
verify_sha256 "${WASIX_LIBC_SHA256}" "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"

rm -rf \
    "${WORKDIR}"/wasi-sdk-${WASI_SDK_VERSION_FULL}* \
    "${WORKDIR}/wasix-sysroot" \
    "${SYSROOT_OUTDIR}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
mkdir -p "${WORKDIR}/wasix-sysroot" "$(dirname "${SYSROOT_OUTDIR}")"
tar -C "${WORKDIR}/wasix-sysroot" -xzf "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"

WASI_SDK="$(find "${WORKDIR}" -maxdepth 1 -type d -name "wasi-sdk-${WASI_SDK_VERSION_FULL}*" | sort | head -n 1)"
if [ -z "${WASI_SDK}" ] || [ ! -x "${WASI_SDK}/bin/clang" ]; then
    echo "wasi-sdk clang not found after extracting ${WASI_SDK_TARBALL}" >&2
    exit 1
fi
UPSTREAM_SYSROOT="$(find "${WORKDIR}/wasix-sysroot" -type d -path "*/sysroot" | sort | head -n 1)"
if [ -z "${UPSTREAM_SYSROOT}" ] || [ ! -f "${UPSTREAM_SYSROOT}/include/unistd.h" ] || [ ! -f "${UPSTREAM_SYSROOT}/lib/wasm32-wasi/libc.a" ]; then
    echo "wasix-libc sysroot not found after extracting ${WASIX_LIBC_TARBALL}" >&2
    exit 1
fi

rsync -a --delete "${UPSTREAM_SYSROOT}/" "${SYSROOT_OUTDIR}/"
rsync -a "${SYSROOT_INCLUDE_DIR}/" "${SYSROOT_OUTDIR}/include/"
apply_sysroot_header_patches "${SYSROOT_OUTDIR}"

compile_sysroot_object "${SYSROOT_LIBC_DIR}/wasix-openat.c" "${WORKDIR}/sinomo-wasix-openat.o" -fPIC
"${WASI_SDK}/bin/llvm-ar" d "${SYSROOT_OUTDIR}/lib/wasm32-wasi/libc.a" openat.o
"${WASI_SDK}/bin/llvm-ar" r "${SYSROOT_OUTDIR}/lib/wasm32-wasi/libc.a" "${WORKDIR}/sinomo-wasix-openat.o"
"${WASI_SDK}/bin/llvm-ranlib" "${SYSROOT_OUTDIR}/lib/wasm32-wasi/libc.a"

compile_sysroot_object "${SYSROOT_LIBC_DIR}/wasix-stat.c" "${WORKDIR}/sinomo-wasix-stat.o" -fPIC
"${WASI_SDK}/bin/llvm-ar" d "${SYSROOT_OUTDIR}/lib/wasm32-wasi/libc.a" fstat.o fstatat.o
"${WASI_SDK}/bin/llvm-ar" r "${SYSROOT_OUTDIR}/lib/wasm32-wasi/libc.a" "${WORKDIR}/sinomo-wasix-stat.o"
"${WASI_SDK}/bin/llvm-ranlib" "${SYSROOT_OUTDIR}/lib/wasm32-wasi/libc.a"

compile_sysroot_object "${SYSROOT_LIBC_DIR}/wasix-posix.c" "${WORKDIR}/sinomo-wasix-posix.o"
compile_sysroot_object "${SYSROOT_LIBC_DIR}/wasix-at-fdcwd.c" "${WORKDIR}/sinomo-wasix-at-fdcwd.o"
compile_sysroot_object "${SYSROOT_LIBC_DIR}/wasix-futex.c" "${WORKDIR}/sinomo-wasix-futex.o"
compile_sysroot_object "${SYSROOT_LIBC_DIR}/wasix-semaphore.c" "${WORKDIR}/sinomo-wasix-semaphore.o"
compile_sysroot_object "${SYSROOT_LIBC_DIR}/wasix-pthread-mutex.c" "${WORKDIR}/sinomo-wasix-pthread-mutex.o"
compile_sysroot_object "${SYSROOT_LIBC_DIR}/wasix-pthread-self.c" "${WORKDIR}/sinomo-wasix-pthread-self.o"
compile_sysroot_object "${SYSROOT_LIBC_DIR}/wasix-pthread-cond.c" "${WORKDIR}/sinomo-wasix-pthread-cond.o"
"${WASI_SDK}/bin/llvm-ar" d "${SYSROOT_OUTDIR}/lib/wasm32-wasi/libc.a" \
    posix.o at_fdcwd.o chdir.o fchdir.o chown.o fchown.o fchownat.o lchown.o \
    ftruncate.o truncate.o getegid.o geteuid.o getgid.o getuid.o setegid.o seteuid.o setgid.o setgroups.o setuid.o \
    pthread_self.o \
    pthread_mutex_destroy.o pthread_mutex_getprioceiling.o pthread_mutex_init.o pthread_mutex_lock.o pthread_mutex_setprioceiling.o \
    pthread_mutex_timedlock.o pthread_mutex_trylock.o pthread_mutex_unlock.o \
    pthread_mutexattr_destroy.o pthread_mutexattr_getprioceiling.o pthread_mutexattr_getprotocol.o pthread_mutexattr_getpshared.o \
    pthread_mutexattr_getrobust.o pthread_mutexattr_gettype.o pthread_mutexattr_init.o pthread_mutexattr_setprioceiling.o \
    pthread_mutexattr_setprotocol.o pthread_mutexattr_setpshared.o pthread_mutexattr_setrobust.o pthread_mutexattr_settype.o \
    pthread_condattr_destroy.o pthread_condattr_init.o pthread_condattr_setclock.o pthread_condattr_setpshared.o \
    pthread_cond_broadcast.o pthread_cond_destroy.o pthread_cond_init.o pthread_cond_signal.o pthread_cond_timedwait.o pthread_cond_wait.o \
    pthread_barrier_destroy.o pthread_barrier_init.o pthread_barrier_wait.o \
    pthread_barrierattr_destroy.o pthread_barrierattr_getpshared.o pthread_barrierattr_init.o pthread_barrierattr_setpshared.o \
    pthread_rwlock_destroy.o pthread_rwlock_init.o pthread_rwlock_rdlock.o pthread_rwlock_timedrdlock.o pthread_rwlock_timedwrlock.o \
    pthread_rwlock_tryrdlock.o pthread_rwlock_trywrlock.o pthread_rwlock_unlock.o pthread_rwlock_wrlock.o \
    pthread_rwlockattr_destroy.o pthread_rwlockattr_getpshared.o pthread_rwlockattr_init.o pthread_rwlockattr_setpshared.o \
    pthread_cancel.o pthread_testcancel.o \
    sem_close.o sem_destroy.o sem_getvalue.o sem_init.o sem_open.o sem_post.o sem_timedwait.o sem_trywait.o sem_unlink.o sem_wait.o \
    pthread_join.o \
    pthread_spin_init.o pthread_spin_destroy.o pthread_spin_lock.o pthread_spin_trylock.o pthread_spin_unlock.o \
    pthread_attr_get.o \
    pthread_attr_setstack.o pthread_attr_setstacksize.o pthread_getattr_np.o pthread_getspecific.o \
    pthread_attr_setinheritsched.o pthread_attr_setschedparam.o pthread_attr_setschedpolicy.o \
    pthread_getcpuclockid.o pthread_getschedparam.o pthread_kill.o pthread_setschedparam.o pthread_setschedprio.o pthread_sigmask.o raise.o sigpending.o sigprocmask.o kill.o \
    pthread_atfork.o \
    clock_nanosleep.o nanosleep.o sleep.o usleep.o wait.o waitpid.o getitimer.o setitimer.o getrlimit.o setrlimit.o times.o \
    __wasilibc_futex.o
"${WASI_SDK}/bin/llvm-ar" r "${SYSROOT_OUTDIR}/lib/wasm32-wasi/libc.a" \
    "${WORKDIR}/sinomo-wasix-posix.o" \
    "${WORKDIR}/sinomo-wasix-at-fdcwd.o" \
    "${WORKDIR}/sinomo-wasix-futex.o" \
    "${WORKDIR}/sinomo-wasix-semaphore.o" \
    "${WORKDIR}/sinomo-wasix-pthread-mutex.o" \
    "${WORKDIR}/sinomo-wasix-pthread-self.o" \
    "${WORKDIR}/sinomo-wasix-pthread-cond.o"
"${WASI_SDK}/bin/llvm-ranlib" "${SYSROOT_OUTDIR}/lib/wasm32-wasi/libc.a"

compile_sysroot_object "${SYSROOT_LIBC_DIR}/wasix-aio.c" "${WORKDIR}/sinomo-wasix-aio.o"
"${WASI_SDK}/bin/llvm-ar" r "${SYSROOT_OUTDIR}/lib/wasm32-wasi/libc.a" "${WORKDIR}/sinomo-wasix-aio.o"
"${WASI_SDK}/bin/llvm-ranlib" "${SYSROOT_OUTDIR}/lib/wasm32-wasi/libc.a"

compile_sysroot_object "${SYSROOT_LIBC_DIR}/wasix-mman.c" "${WORKDIR}/sinomo-wasix-mman.o" -D_WASI_EMULATED_MMAN
"${WASI_SDK}/bin/llvm-ar" d "${SYSROOT_OUTDIR}/lib/wasm32-wasi/libwasi-emulated-mman.a" mman.o
"${WASI_SDK}/bin/llvm-ar" r "${SYSROOT_OUTDIR}/lib/wasm32-wasi/libwasi-emulated-mman.a" "${WORKDIR}/sinomo-wasix-mman.o"
"${WASI_SDK}/bin/llvm-ranlib" "${SYSROOT_OUTDIR}/lib/wasm32-wasi/libwasi-emulated-mman.a"

if [ -n "${HOST_ENV_OUT}" ]; then
    mkdir -p "$(dirname "${HOST_ENV_OUT}")"
    cat >"${HOST_ENV_OUT}" <<EOF
# Source this file when building Sinomo WASIX command modules outside the sandbox
# with the same sysroot input set used by the sandbox toolchain packages.
export SINOMO_WASI_SDK="${WASI_SDK}"
export SINOMO_WASIX_SYSROOT="${SYSROOT_OUTDIR}"
export CC="\${SINOMO_WASI_SDK}/bin/clang --target=wasm32-wasi --sysroot=\${SINOMO_WASIX_SYSROOT}"
export CXX="\${SINOMO_WASI_SDK}/bin/clang++ --target=wasm32-wasi --sysroot=\${SINOMO_WASIX_SYSROOT}"
export AR="\${SINOMO_WASI_SDK}/bin/llvm-ar"
export RANLIB="\${SINOMO_WASI_SDK}/bin/llvm-ranlib"
EOF
fi

echo "prepared WASIX sysroot: ${SYSROOT_OUTDIR}"
