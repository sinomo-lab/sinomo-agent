#!/usr/bin/env bash
set -euo pipefail

BUSYBOX_VERSION="${BUSYBOX_VERSION:-1.36.1}"
BUSYBOX_SHA256="${BUSYBOX_SHA256:-b8cc24c9574d809e7279c3be349795c5d5ceb6fdf19ca709f80cde50e47de314}"
BUSYBOX_TARGET_ABI="${BUSYBOX_TARGET_ABI:-wasip1}"
WASI_SDK_VERSION="${WASI_SDK_VERSION:-24}"
WASI_SDK_VERSION_FULL="${WASI_SDK_VERSION_FULL:-24.0}"
WASI_SDK_ARCH="${WASI_SDK_ARCH:-x86_64}"
WASI_SDK_OS="${WASI_SDK_OS:-linux}"
WASI_SDK_SHA256="${WASI_SDK_SHA256:-c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5}"
BINARYEN_VERSION="${BINARYEN_VERSION:-124}"
BINARYEN_ARCH="${BINARYEN_ARCH:-x86_64}"
BINARYEN_OS="${BINARYEN_OS:-linux}"
BINARYEN_SHA256="${BINARYEN_SHA256:-0290c3779fedf592b8da0ded3032ff55c41a2b7bfa2d6bf7b7bac6f0e6e28963}"
WASIX_LIBC_VERSION="${WASIX_LIBC_VERSION:-v2026-05-12.1}"
WASIX_LIBC_TARBALL="${WASIX_LIBC_TARBALL:-sysroot.tar.gz}"
WASIX_LIBC_SHA256="${WASIX_LIBC_SHA256:-e9f7435c0fdc050c1f813f83d3b22388ba3b16d1303ced510915eb028a38abb4}"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1719878400}"
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
WORKDIR="${WORKDIR:-"${ROOT}/work"}"
OUTDIR="${OUTDIR:-"${ROOT}/out"}"
FRAGMENT="${FRAGMENT:-"${ROOT}/busybox-wasi.fragment"}"

BUSYBOX_TARBALL="busybox-${BUSYBOX_VERSION}.tar.bz2"
BUSYBOX_URL="https://busybox.net/downloads/${BUSYBOX_TARBALL}"
WASI_SDK_TARBALL="wasi-sdk-${WASI_SDK_VERSION_FULL}-${WASI_SDK_ARCH}-${WASI_SDK_OS}.tar.gz"
WASI_SDK_URL="https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_SDK_VERSION}/${WASI_SDK_TARBALL}"
BINARYEN_TARBALL="binaryen-version_${BINARYEN_VERSION}-${BINARYEN_ARCH}-${BINARYEN_OS}.tar.gz"
BINARYEN_URL="https://github.com/WebAssembly/binaryen/releases/download/version_${BINARYEN_VERSION}/${BINARYEN_TARBALL}"
WASIX_LIBC_URL="${WASIX_LIBC_URL:-https://github.com/wasix-org/wasix-libc/releases/download/${WASIX_LIBC_VERSION}/${WASIX_LIBC_TARBALL}}"

case "${BUSYBOX_TARGET_ABI}" in
    wasip1 | wasix) ;;
    *)
        echo "unsupported BUSYBOX_TARGET_ABI=${BUSYBOX_TARGET_ABI}; expected wasip1 or wasix" >&2
        exit 1
        ;;
esac

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
    local actual
    actual="$(sha256sum "${file}" | awk '{print $1}')"
    if [ "${actual}" != "${expected}" ]; then
        echo "sha256 mismatch for ${file}: expected ${expected}, actual ${actual}" >&2
        exit 1
    fi
}

sinomo_hash() {
    local file="$1"
    if [ -n "${SINOMO_BIN:-}" ]; then
        env -u CC -u CFLAGS -u LDFLAGS -u AR -u RANLIB \
            "${SINOMO_BIN}" sandbox toolchain hash "${file}"
    else
        env -u CC -u CFLAGS -u LDFLAGS -u AR -u RANLIB \
            cargo run --quiet --manifest-path "${REPO_ROOT}/Cargo.toml" --bin sinomo -- sandbox toolchain hash "${file}"
    fi
}

enable_config() {
    local key="$1"
    set_config "${key}" "y"
}

disable_config() {
    local key="$1"
    set_config "${key}" "n"
}

set_config() {
    local key="$1"
    local value="$2"
    local config="${BUSYBOX_SRC}/.config"
    local enabled="CONFIG_${key}=y"
    local disabled="# CONFIG_${key} is not set"

    if grep -q -E "^(# )?CONFIG_${key}(=| is not set)" "${config}"; then
        if [ "${value}" = "y" ]; then
            sed -i -E "s@^(# )?CONFIG_${key}(=.*| is not set)@${enabled}@" "${config}"
        else
            sed -i -E "s@^(# )?CONFIG_${key}(=.*| is not set)@${disabled}@" "${config}"
        fi
    elif [ "${value}" = "y" ]; then
        printf '%s\n' "${enabled}" >> "${config}"
    else
        printf '%s\n' "${disabled}" >> "${config}"
    fi
}

mkdir -p "${WORKDIR}" "${OUTDIR}" "${WORKDIR}/downloads"

download "${BUSYBOX_URL}" "${WORKDIR}/downloads/${BUSYBOX_TARBALL}"
download "${WASI_SDK_URL}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
if [ "${BUSYBOX_TARGET_ABI}" = "wasix" ]; then
    download "${BINARYEN_URL}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
    download "${WASIX_LIBC_URL}" "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"
fi
verify_sha256 "${BUSYBOX_SHA256}" "${WORKDIR}/downloads/${BUSYBOX_TARBALL}"
verify_sha256 "${WASI_SDK_SHA256}" "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
if [ "${BUSYBOX_TARGET_ABI}" = "wasix" ]; then
    verify_sha256 "${BINARYEN_SHA256}" "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
    verify_sha256 "${WASIX_LIBC_SHA256}" "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"
fi

rm -rf "${WORKDIR}/busybox-${BUSYBOX_VERSION}" "${WORKDIR}"/wasi-sdk-${WASI_SDK_VERSION_FULL}* "${WORKDIR}"/binaryen-version_${BINARYEN_VERSION} "${WORKDIR}/wasix-sysroot"
tar -C "${WORKDIR}" -xjf "${WORKDIR}/downloads/${BUSYBOX_TARBALL}"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${WASI_SDK_TARBALL}"
if [ "${BUSYBOX_TARGET_ABI}" = "wasix" ]; then
    tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${BINARYEN_TARBALL}"
    tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/wasix-libc-${WASIX_LIBC_VERSION}-${WASIX_LIBC_TARBALL}"
fi

BUSYBOX_SRC="${WORKDIR}/busybox-${BUSYBOX_VERSION}"
WASI_SDK="$(find "${WORKDIR}" -maxdepth 1 -type d -name "wasi-sdk-${WASI_SDK_VERSION_FULL}*" | sort | head -n 1)"
if [ -z "${WASI_SDK}" ] || [ ! -x "${WASI_SDK}/bin/clang" ]; then
    echo "wasi-sdk clang not found after extracting ${WASI_SDK_TARBALL}" >&2
    exit 1
fi
if [ "${BUSYBOX_TARGET_ABI}" = "wasix" ]; then
    BINARYEN="${WORKDIR}/binaryen-version_${BINARYEN_VERSION}"
    if [ ! -x "${BINARYEN}/bin/wasm-opt" ]; then
        echo "Binaryen wasm-opt not found after extracting ${BINARYEN_TARBALL}" >&2
        exit 1
    fi
    WASIX_SYSROOT="${WORKDIR}/wasix-sysroot/sysroot"
    if [ ! -f "${WASIX_SYSROOT}/include/unistd.h" ] || [ ! -f "${WASIX_SYSROOT}/lib/wasm32-wasi/libc.a" ]; then
        echo "wasix-libc sysroot not found after extracting ${WASIX_LIBC_TARBALL}" >&2
        exit 1
    fi
fi
export SOURCE_DATE_EPOCH
export KCONFIG_NOTIMESTAMP=1

make -C "${BUSYBOX_SRC}" allnoconfig

while IFS= read -r line; do
    case "${line}" in
        "") continue ;;
        "# CONFIG_"*" is not set")
            key="${line#"# CONFIG_"}"
            disable_config "${key%" is not set"}"
            ;;
        \#*) continue ;;
        CONFIG_*=y)
            key="${line#CONFIG_}"
            enable_config "${key%=y}"
            ;;
        CONFIG_*=n)
            key="${line#CONFIG_}"
            disable_config "${key%=n}"
            ;;
        *)
            echo "unsupported config fragment line: ${line}" >&2
            exit 1
            ;;
    esac
done < "${FRAGMENT}"

set +o pipefail
yes "" | make -C "${BUSYBOX_SRC}" oldconfig
oldconfig_status="${PIPESTATUS[1]}"
set -o pipefail
if [ "${oldconfig_status}" -ne 0 ]; then
    exit "${oldconfig_status}"
fi

cat >> "${BUSYBOX_SRC}/include/platform.h" <<'PLATFORM_WASI'

/* Sinomo WASIp1 build target adjustments. BusyBox 1.36 does not recognize
 * __wasi__, so disable host libc probes that are known absent from wasi-libc.
 */
#if defined(__wasi__)
# undef HAVE_CLEARENV
# undef HAVE_DPRINTF
# undef HAVE_FDATASYNC
# undef HAVE_MNTENT_H
# undef HAVE_NET_ETHERNET_H
# undef HAVE_PTSNAME_R
# undef HAVE_SYS_STATFS_H
# undef HAVE_SYS_SYSMACROS_H
# undef HAVE_SIGHANDLER_T
# undef HAVE_STRVERSCMP
# undef HAVE_UNLOCKED_LINE_OPS
# undef HAVE_UNLOCKED_STDIO
# undef HAVE_WAIT3
# undef HAVE_XTABS
#endif
PLATFORM_WASI

# BusyBox's normal static build keeps main() in libbb/appletlib.c. That works
# for ELF links, but the WASI CRT did not pull main out of the archive and
# produced an _start path that traps through an unresolved main stub. Reuse the
# existing applets-level entry wrapper for the static WASI module.
sed -i 's/^#if ENABLE_BUILD_LIBBUSYBOX$/#if 1/' "${BUSYBOX_SRC}/applets/applets.c"
sed -i '/#include "busybox.h"/a int lbb_main(char **argv);' "${BUSYBOX_SRC}/applets/applets.c"
sed -i '0,/^#if ENABLE_BUILD_LIBBUSYBOX$/s//\#if 1/' "${BUSYBOX_SRC}/libbb/appletlib.c"

# WASI preview1 has no process-level chdir. Wasmtime represents the command cwd
# as a preopen named "."; wasi-libc getcwd therefore reports "/" even when the
# Sinomo virtual cwd is "/work". Sinomo injects PWD from the sandbox manifest,
# so let BusyBox's existing logical PWD validation path run first on WASI.
sed -i '/^[[:space:]]*char \*buf;$/a\
\
#if defined(__wasi__)\
	if (logical_getcwd())\
		return fflush_all();\
#endif' "${BUSYBOX_SRC}/coreutils/pwd.c"

sed -i '/^[[:space:]]*int fd = mkstemp(template);$/,/^[[:space:]]*return fd;$/c\
#if defined(__wasi__)\
	char *slot = strstr(template, "XXXXXX");\
	if (!slot) {\
		errno = EINVAL;\
		bb_perror_msg_and_die("can'\''t create temp file '\''%s'\''", template);\
	}\
	for (unsigned i = 0; i < 1000000; i++) {\
		snprintf(slot, 7, "%06u", i);\
		int fd = open(template, O_RDWR | O_CREAT | O_EXCL, 0600);\
		if (fd >= 0)\
			return fd;\
		if (errno != EEXIST)\
			break;\
	}\
	bb_perror_msg_and_die("can'\''t create temp file '\''%s'\''", template);\
#else\
	int fd = mkstemp(template);\
	if (fd < 0)\
		bb_perror_msg_and_die("can'\''t create temp file '\''%s'\''", template);\
	return fd;\
#endif' "${BUSYBOX_SRC}/libbb/xfuncs_printf.c"

sed -i '/^#include "libbb.h"$/a\
#if defined(__wasi__)\
# include <wasi/libc.h>\
#endif' "${BUSYBOX_SRC}/libbb/xfuncs_printf.c"

sed -i '/^void FAST_FUNC xmove_fd(int from, int to)$/,/^}$/c\
void FAST_FUNC xmove_fd(int from, int to)\
{\
	if (from == to)\
		return;\
#if defined(__wasi__)\
	if (__wasilibc_fd_renumber(from, to) != 0)\
		bb_simple_perror_msg_and_die("can'\''t renumber file descriptor");\
#else\
	xdup2(from, to);\
	close(from);\
#endif\
}' "${BUSYBOX_SRC}/libbb/xfuncs_printf.c"

sed -i '/^#include <grp.h>$/a\
#if defined(__wasi__)\
# if defined(SINOMO_BUSYBOX_PREVIEW1_EXTENSIONS)\
__attribute__((import_module("sinomo_sandbox_preview1"), import_name("path_mode")))\
int sinomo_sandbox_path_mode(const char *path, unsigned path_len, unsigned *mode);\
# else\
__attribute__((import_module("wasix_32v1"), import_name("path_mode")))\
int sinomo_sandbox_path_mode(const char *path, unsigned path_len, unsigned *mode);\
# endif\
static inline int sinomo_wasi_stat(const char *path, struct stat *st)\
{\
	int rc = stat(path, st);\
	unsigned mode;\
	if (rc == 0 && sinomo_sandbox_path_mode(path, strlen(path), &mode) == 0)\
		st->st_mode = (st->st_mode & ~07777) | (mode & 07777);\
	return rc;\
}\
# define stat(path, st) sinomo_wasi_stat((path), (st))\
# define lstat(path, st) sinomo_wasi_stat((path), (st))\
#endif' "${BUSYBOX_SRC}/include/libbb.h"

sed -i '/^#if !defined(major) || defined(__GLIBC__)$/,/^#endif$/c\
#if !defined(__wasi__) && (!defined(major) || defined(__GLIBC__))\
# include <sys/sysmacros.h>\
#endif' "${BUSYBOX_SRC}/include/libbb.h"

sed -i '/^#include <grp.h>$/a\
#if defined(__wasi__)\
# include <fcntl.h>\
# include <sys/resource.h>\
# include <sched.h>\
# include <sys/times.h>\
# ifndef major\
#  define major(dev) ((unsigned)(((dev) >> 8) & 0xfff))\
# endif\
# ifndef minor\
#  define minor(dev) ((unsigned)(((dev) & 0xff) | (((dev) >> 12) & 0xfffff00)))\
# endif\
# ifndef SOCK_RDM\
#  define SOCK_RDM 0\
# endif\
# ifndef SOCK_SEQPACKET\
#  define SOCK_SEQPACKET 0\
# endif\
static inline int sinomo_wasi_mknod(const char *path, mode_t mode, dev_t dev)\
{\
	(void) dev;\
	if ((mode & S_IFMT) == S_IFREG || (mode & S_IFMT) == 0) {\
		int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, mode & 07777);\
		if (fd < 0)\
			return -1;\
		close(fd);\
		return 0;\
	}\
	errno = EPERM;\
	return -1;\
}\
# define mknod(path, mode, dev) sinomo_wasi_mknod((path), (mode), (dev))\
static inline int sinomo_wasi_getgroups(int size, gid_t list[])\
{\
#if defined(SINOMO_BUSYBOX_PREVIEW1_EXTENSIONS)\
	if (size == 0)\
		return 1;\
	if (size < 1) {\
		errno = EINVAL;\
		return -1;\
	}\
	list[0] = getgid();\
	return 1;\
#else\
	__attribute__((import_module("wasix_32v1"), import_name("proc_getgroups")))\
	int sinomo_wasix_proc_getgroups(unsigned *groups, unsigned max_groups, unsigned *count);\
	unsigned count = 0;\
	int rc = sinomo_wasix_proc_getgroups(NULL, 0, &count);\
	if (rc != 0) {\
		errno = rc;\
		return -1;\
	}\
	if (size == 0)\
		return (int)count;\
	if (size < 0 || (unsigned)size < count) {\
		errno = EINVAL;\
		return -1;\
	}\
	unsigned groups[32];\
	if (count > sizeof(groups) / sizeof(groups[0])) {\
		errno = EINVAL;\
		return -1;\
	}\
	unsigned written = 0;\
	rc = sinomo_wasix_proc_getgroups(groups, count, &written);\
	if (rc != 0) {\
		errno = rc;\
		return -1;\
	}\
	if (written > count) {\
		errno = EINVAL;\
		return -1;\
	}\
	for (unsigned i = 0; i < written; i++)\
		list[i] = (gid_t)groups[i];\
	return (int)written;\
#endif\
}\
# define getgroups(size, list) sinomo_wasi_getgroups((size), (list))\
static inline unsigned sinomo_wasi_id_or_keep(long value)\
{\
	return value < 0 ? (unsigned)-1 : (unsigned)value;\
}\
# if !defined(SINOMO_BUSYBOX_PREVIEW1_EXTENSIONS)\
__attribute__((import_module("wasix_32v1"), import_name("proc_getresuid")))\
int sinomo_wasix_proc_getresuid(unsigned *ruid, unsigned *euid, unsigned *suid);\
__attribute__((import_module("wasix_32v1"), import_name("proc_getresgid")))\
int sinomo_wasix_proc_getresgid(unsigned *rgid, unsigned *egid, unsigned *sgid);\
__attribute__((import_module("wasix_32v1"), import_name("proc_setresuid")))\
int sinomo_wasix_proc_setresuid(unsigned ruid, unsigned euid, unsigned suid);\
__attribute__((import_module("wasix_32v1"), import_name("proc_setresgid")))\
int sinomo_wasix_proc_setresgid(unsigned rgid, unsigned egid, unsigned sgid);\
__attribute__((import_module("wasix_32v1"), import_name("proc_setgroups")))\
int sinomo_wasix_proc_setgroups(const unsigned *groups, unsigned count);\
__attribute__((import_module("wasix_32v1"), import_name("proc_getrlimit")))\
int sinomo_wasix_proc_getrlimit(int pid, int resource, unsigned long long *current, unsigned long long *maximum);\
__attribute__((import_module("wasix_32v1"), import_name("proc_setrlimit")))\
int sinomo_wasix_proc_setrlimit(int pid, int resource, unsigned long long current, unsigned long long maximum);\
__attribute__((import_module("wasix_32v1"), import_name("proc_getrusage")))\
int sinomo_wasix_proc_getrusage(int who, unsigned long long *user_usec, unsigned long long *system_usec, unsigned long long *maxrss_kib);\
__attribute__((import_module("wasix_32v1"), import_name("proc_times")))\
int sinomo_wasix_proc_times(unsigned long long *elapsed_ticks, unsigned long long *user_ticks, unsigned long long *system_ticks, unsigned long long *child_user_ticks, unsigned long long *child_system_ticks);\
__attribute__((import_module("wasix_32v1"), import_name("proc_getscheduler")))\
int sinomo_wasix_proc_getscheduler(int pid, int *policy, int *priority);\
__attribute__((import_module("wasix_32v1"), import_name("proc_setscheduler")))\
int sinomo_wasix_proc_setscheduler(int pid, int policy, int priority);\
__attribute__((import_module("wasix_32v1"), import_name("proc_getaffinity")))\
int sinomo_wasix_proc_getaffinity(int pid, unsigned long long *mask_low, unsigned long long *mask_high);\
__attribute__((import_module("wasix_32v1"), import_name("proc_setaffinity")))\
int sinomo_wasix_proc_setaffinity(int pid, unsigned long long mask_low, unsigned long long mask_high);\
__attribute__((import_module("wasix_32v1"), import_name("proc_getcpu")))\
int sinomo_wasix_proc_getcpu(unsigned int *cpu);\
static inline int sinomo_wasi_getresuid(uid_t *ruid, uid_t *euid, uid_t *suid)\
{\
	unsigned rr = 0, ee = 0, ss = 0;\
	int rc = sinomo_wasix_proc_getresuid(&rr, &ee, &ss);\
	if (rc != 0) {\
		errno = rc;\
		return -1;\
	}\
	if (ruid)\
		*ruid = (uid_t)rr;\
	if (euid)\
		*euid = (uid_t)ee;\
	if (suid)\
		*suid = (uid_t)ss;\
	return 0;\
}\
static inline int sinomo_wasi_getresgid(gid_t *rgid, gid_t *egid, gid_t *sgid)\
{\
	unsigned rr = 0, ee = 0, ss = 0;\
	int rc = sinomo_wasix_proc_getresgid(&rr, &ee, &ss);\
	if (rc != 0) {\
		errno = rc;\
		return -1;\
	}\
	if (rgid)\
		*rgid = (gid_t)rr;\
	if (egid)\
		*egid = (gid_t)ee;\
	if (sgid)\
		*sgid = (gid_t)ss;\
	return 0;\
}\
static inline uid_t sinomo_wasi_getuid(void)\
{\
	uid_t id = 0;\
	return sinomo_wasi_getresuid(&id, NULL, NULL) == 0 ? id : (uid_t)-1;\
}\
static inline uid_t sinomo_wasi_geteuid(void)\
{\
	uid_t id = 0;\
	return sinomo_wasi_getresuid(NULL, &id, NULL) == 0 ? id : (uid_t)-1;\
}\
static inline gid_t sinomo_wasi_getgid(void)\
{\
	gid_t id = 0;\
	return sinomo_wasi_getresgid(&id, NULL, NULL) == 0 ? id : (gid_t)-1;\
}\
static inline gid_t sinomo_wasi_getegid(void)\
{\
	gid_t id = 0;\
	return sinomo_wasi_getresgid(NULL, &id, NULL) == 0 ? id : (gid_t)-1;\
}\
static inline int sinomo_wasi_setuid(uid_t uid)\
{\
	int rc = sinomo_wasix_proc_setresuid((unsigned)uid, (unsigned)uid, (unsigned)uid);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_seteuid(uid_t uid)\
{\
	int rc = sinomo_wasix_proc_setresuid((unsigned)-1, (unsigned)uid, (unsigned)-1);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_setgid(gid_t gid)\
{\
	unsigned group = (unsigned)gid;\
	int rc = sinomo_wasix_proc_setresgid((unsigned)gid, (unsigned)gid, (unsigned)gid);\
	if (rc == 0)\
		rc = sinomo_wasix_proc_setgroups(&group, 1);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_setegid(gid_t gid)\
{\
	int rc = sinomo_wasix_proc_setresgid((unsigned)-1, (unsigned)gid, (unsigned)-1);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_setreuid(uid_t ruid, uid_t euid)\
{\
	unsigned saved = euid == (uid_t)-1 ? (unsigned)-1 : (unsigned)euid;\
	int rc = sinomo_wasix_proc_setresuid(sinomo_wasi_id_or_keep(ruid), sinomo_wasi_id_or_keep(euid), saved);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_setregid(gid_t rgid, gid_t egid)\
{\
	unsigned saved = egid == (gid_t)-1 ? (unsigned)-1 : (unsigned)egid;\
	int rc = sinomo_wasix_proc_setresgid(sinomo_wasi_id_or_keep(rgid), sinomo_wasi_id_or_keep(egid), saved);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_setresuid(uid_t ruid, uid_t euid, uid_t suid)\
{\
	int rc = sinomo_wasix_proc_setresuid(sinomo_wasi_id_or_keep(ruid), sinomo_wasi_id_or_keep(euid), sinomo_wasi_id_or_keep(suid));\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_setresgid(gid_t rgid, gid_t egid, gid_t sgid)\
{\
	int rc = sinomo_wasix_proc_setresgid(sinomo_wasi_id_or_keep(rgid), sinomo_wasi_id_or_keep(egid), sinomo_wasi_id_or_keep(sgid));\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_setgroups(size_t size, const gid_t *list)\
{\
	if (size > 0 && list == NULL) {\
		errno = EFAULT;\
		return -1;\
	}\
	unsigned groups[64];\
	if (size > sizeof(groups) / sizeof(groups[0])) {\
		errno = EINVAL;\
		return -1;\
	}\
	for (size_t i = 0; i < size; i++)\
		groups[i] = (unsigned)list[i];\
	int rc = sinomo_wasix_proc_setgroups(groups, (unsigned)size);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_getrlimit(int resource, struct rlimit *limit)\
{\
	if (limit == NULL) {\
		errno = EFAULT;\
		return -1;\
	}\
	unsigned long long current = 0, maximum = 0;\
	int rc = sinomo_wasix_proc_getrlimit(0, resource, &current, &maximum);\
	if (rc != 0) {\
		errno = rc;\
		return -1;\
	}\
	limit->rlim_cur = (rlim_t)current;\
	limit->rlim_max = (rlim_t)maximum;\
	return 0;\
}\
static inline int sinomo_wasi_setrlimit(int resource, const struct rlimit *limit)\
{\
	if (limit == NULL) {\
		errno = EFAULT;\
		return -1;\
	}\
	int rc = sinomo_wasix_proc_setrlimit(0, resource, (unsigned long long)limit->rlim_cur, (unsigned long long)limit->rlim_max);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_getrusage(int who, struct rusage *usage)\
{\
	if (usage == NULL) {\
		errno = EFAULT;\
		return -1;\
	}\
	unsigned long long user_usec = 0, system_usec = 0, maxrss_kib = 0;\
	int rc = sinomo_wasix_proc_getrusage(who, &user_usec, &system_usec, &maxrss_kib);\
	if (rc != 0) {\
		errno = rc;\
		return -1;\
	}\
	memset(usage, 0, sizeof(*usage));\
	usage->ru_utime.tv_sec = (time_t)(user_usec / 1000000ULL);\
	usage->ru_utime.tv_usec = (suseconds_t)(user_usec % 1000000ULL);\
	usage->ru_stime.tv_sec = (time_t)(system_usec / 1000000ULL);\
	usage->ru_stime.tv_usec = (suseconds_t)(system_usec % 1000000ULL);\
	usage->ru_maxrss = (long)maxrss_kib;\
	return 0;\
}\
static inline clock_t sinomo_wasi_times(struct tms *buf)\
{\
	unsigned long long elapsed = 0, user = 0, system = 0, child_user = 0, child_system = 0;\
	int rc = sinomo_wasix_proc_times(&elapsed, &user, &system, &child_user, &child_system);\
	if (rc != 0) {\
		errno = rc;\
		return (clock_t)-1;\
	}\
	if (buf != NULL) {\
		buf->tms_utime = (clock_t)user;\
		buf->tms_stime = (clock_t)system;\
		buf->tms_cutime = (clock_t)child_user;\
		buf->tms_cstime = (clock_t)child_system;\
	}\
	return (clock_t)elapsed;\
}\
static inline int sinomo_wasi_sched_getscheduler(pid_t pid)\
{\
	int policy = 0, priority = 0;\
	int rc = sinomo_wasix_proc_getscheduler((int)pid, &policy, &priority);\
	if (rc == 0)\
		return policy;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_sched_getparam(pid_t pid, struct sched_param *param)\
{\
	if (param == NULL) {\
		errno = EFAULT;\
		return -1;\
	}\
	int policy = 0, priority = 0;\
	int rc = sinomo_wasix_proc_getscheduler((int)pid, &policy, &priority);\
	if (rc != 0) {\
		errno = rc;\
		return -1;\
	}\
	memset(param, 0, sizeof(*param));\
	param->sched_priority = priority;\
	return 0;\
}\
static inline int sinomo_wasi_sched_setparam(pid_t pid, const struct sched_param *param)\
{\
	if (param == NULL) {\
		errno = EFAULT;\
		return -1;\
	}\
	int policy = sinomo_wasi_sched_getscheduler(pid);\
	if (policy < 0)\
		return -1;\
	int rc = sinomo_wasix_proc_setscheduler((int)pid, policy, param->sched_priority);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_sched_setscheduler(pid_t pid, int policy, const struct sched_param *param)\
{\
	if (param == NULL) {\
		errno = EFAULT;\
		return -1;\
	}\
	int rc = sinomo_wasix_proc_setscheduler((int)pid, policy, param->sched_priority);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_sched_getaffinity(pid_t pid, size_t cpusetsize, void *mask)\
{\
	if (mask == NULL) {\
		errno = EFAULT;\
		return -1;\
	}\
	if (cpusetsize == 0) {\
		errno = EINVAL;\
		return -1;\
	}\
	unsigned long long words[2] = {0, 0};\
	int rc = sinomo_wasix_proc_getaffinity((int)pid, &words[0], &words[1]);\
	if (rc != 0) {\
		errno = rc;\
		return -1;\
	}\
	memset(mask, 0, cpusetsize);\
	size_t copy_size = cpusetsize < sizeof(words) ? cpusetsize : sizeof(words);\
	memcpy(mask, words, copy_size);\
	return 0;\
}\
static inline int sinomo_wasi_sched_setaffinity(pid_t pid, size_t cpusetsize, const void *mask)\
{\
	if (mask == NULL) {\
		errno = EFAULT;\
		return -1;\
	}\
	if (cpusetsize == 0) {\
		errno = EINVAL;\
		return -1;\
	}\
	unsigned long long words[2] = {0, 0};\
	size_t copy_size = cpusetsize < sizeof(words) ? cpusetsize : sizeof(words);\
	memcpy(words, mask, copy_size);\
	int rc = sinomo_wasix_proc_setaffinity((int)pid, words[0], words[1]);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_sched_getcpu(void)\
{\
	unsigned int cpu = 0;\
	int rc = sinomo_wasix_proc_getcpu(&cpu);\
	if (rc == 0)\
		return (int)cpu;\
	errno = rc;\
	return -1;\
}\
#  define setuid(uid) sinomo_wasi_setuid((uid))\
#  define seteuid(uid) sinomo_wasi_seteuid((uid))\
#  define setgid(gid) sinomo_wasi_setgid((gid))\
#  define setegid(gid) sinomo_wasi_setegid((gid))\
#  define setreuid(ruid, euid) sinomo_wasi_setreuid((ruid), (euid))\
#  define setregid(rgid, egid) sinomo_wasi_setregid((rgid), (egid))\
#  define setresuid(ruid, euid, suid) sinomo_wasi_setresuid((ruid), (euid), (suid))\
#  define setresgid(rgid, egid, sgid) sinomo_wasi_setresgid((rgid), (egid), (sgid))\
#  define setgroups(size, list) sinomo_wasi_setgroups((size), (list))\
#  define getuid() sinomo_wasi_getuid()\
#  define geteuid() sinomo_wasi_geteuid()\
#  define getgid() sinomo_wasi_getgid()\
#  define getegid() sinomo_wasi_getegid()\
#  define getresuid(ruid, euid, suid) sinomo_wasi_getresuid((ruid), (euid), (suid))\
#  define getresgid(rgid, egid, sgid) sinomo_wasi_getresgid((rgid), (egid), (sgid))\
#  define getrlimit(resource, limit) sinomo_wasi_getrlimit((resource), (limit))\
#  define setrlimit(resource, limit) sinomo_wasi_setrlimit((resource), (limit))\
#  define getrusage(who, usage) sinomo_wasi_getrusage((who), (usage))\
#  define times(buf) sinomo_wasi_times((buf))\
#  define sched_getscheduler(pid) sinomo_wasi_sched_getscheduler((pid))\
#  define sched_getparam(pid, param) sinomo_wasi_sched_getparam((pid), (param))\
#  define sched_setparam(pid, param) sinomo_wasi_sched_setparam((pid), (param))\
#  define sched_setscheduler(pid, policy, param) sinomo_wasi_sched_setscheduler((pid), (policy), (param))\
#  define sched_getaffinity(pid, cpusetsize, mask) sinomo_wasi_sched_getaffinity((pid), (cpusetsize), (mask))\
#  define sched_setaffinity(pid, cpusetsize, mask) sinomo_wasi_sched_setaffinity((pid), (cpusetsize), (mask))\
#  define sched_getcpu() sinomo_wasi_sched_getcpu()\
# endif\
static inline unsigned sinomo_wasi_ownership_id(long value)\
{\
	return value < 0 ? (unsigned)-1 : (unsigned)value;\
}\
# if !defined(SINOMO_BUSYBOX_PREVIEW1_EXTENSIONS)\
__attribute__((import_module("wasix_32v1"), import_name("path_chown")))\
int sinomo_wasix_path_chown(const char *path, unsigned path_len, unsigned uid, unsigned gid);\
__attribute__((import_module("wasix_32v1"), import_name("fd_chown")))\
int sinomo_wasix_fd_chown(int fd, unsigned uid, unsigned gid);\
__attribute__((import_module("wasix_32v1"), import_name("proc_getpgid")))\
int sinomo_wasix_proc_getpgid(int pid, int *pgid);\
__attribute__((import_module("wasix_32v1"), import_name("proc_getpgrp")))\
int sinomo_wasix_proc_getpgrp(int *pgid);\
__attribute__((import_module("wasix_32v1"), import_name("proc_getsid")))\
int sinomo_wasix_proc_getsid(int pid, int *sid);\
__attribute__((import_module("wasix_32v1"), import_name("proc_setpgid")))\
int sinomo_wasix_proc_setpgid(int pid, int pgid);\
__attribute__((import_module("wasix_32v1"), import_name("proc_setsid")))\
int sinomo_wasix_proc_setsid(int *sid);\
__attribute__((import_module("wasix_32v1"), import_name("proc_tcgetpgrp")))\
int sinomo_wasix_proc_tcgetpgrp(int fd, int *pgid);\
__attribute__((import_module("wasix_32v1"), import_name("proc_tcgetsid")))\
int sinomo_wasix_proc_tcgetsid(int fd, int *sid);\
__attribute__((import_module("wasix_32v1"), import_name("proc_tcsetpgrp")))\
int sinomo_wasix_proc_tcsetpgrp(int fd, int pgid);\
__attribute__((import_module("wasix_32v1"), import_name("proc_getpriority")))\
int sinomo_wasix_proc_getpriority(int which, int who, int *priority);\
__attribute__((import_module("wasix_32v1"), import_name("proc_setpriority")))\
int sinomo_wasix_proc_setpriority(int which, int who, int priority);\
static inline int sinomo_wasi_chown(const char *path, uid_t uid, gid_t gid)\
{\
	int rc = sinomo_wasix_path_chown(path, strlen(path), sinomo_wasi_ownership_id(uid), sinomo_wasi_ownership_id(gid));\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_fchown(int fd, uid_t uid, gid_t gid)\
{\
	int rc = sinomo_wasix_fd_chown(fd, sinomo_wasi_ownership_id(uid), sinomo_wasi_ownership_id(gid));\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
#  define chown(path, uid, gid) sinomo_wasi_chown((path), (uid), (gid))\
#  define lchown(path, uid, gid) sinomo_wasi_chown((path), (uid), (gid))\
#  define fchown(fd, uid, gid) sinomo_wasi_fchown((fd), (uid), (gid))\
static inline pid_t sinomo_wasi_getpgid(pid_t pid)\
{\
	int pgid = 0;\
	int rc = sinomo_wasix_proc_getpgid((int)pid, &pgid);\
	if (rc == 0)\
		return (pid_t)pgid;\
	errno = rc;\
	return -1;\
}\
static inline pid_t sinomo_wasi_getpgrp(void)\
{\
	int pgid = 0;\
	int rc = sinomo_wasix_proc_getpgrp(&pgid);\
	if (rc == 0)\
		return (pid_t)pgid;\
	errno = rc;\
	return -1;\
}\
static inline pid_t sinomo_wasi_getsid(pid_t pid)\
{\
	int sid = 0;\
	int rc = sinomo_wasix_proc_getsid((int)pid, &sid);\
	if (rc == 0)\
		return (pid_t)sid;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_setpgid(pid_t pid, pid_t pgid)\
{\
	int rc = sinomo_wasix_proc_setpgid((int)pid, (int)pgid);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline pid_t sinomo_wasi_setsid(void)\
{\
	int sid = 0;\
	int rc = sinomo_wasix_proc_setsid(&sid);\
	if (rc == 0)\
		return (pid_t)sid;\
	errno = rc;\
	return -1;\
}\
static inline pid_t sinomo_wasi_tcgetpgrp(int fd)\
{\
	int pgid = 0;\
	int rc = sinomo_wasix_proc_tcgetpgrp(fd, &pgid);\
	if (rc == 0)\
		return (pid_t)pgid;\
	errno = rc;\
	return -1;\
}\
static inline pid_t sinomo_wasi_tcgetsid(int fd)\
{\
	int sid = 0;\
	int rc = sinomo_wasix_proc_tcgetsid(fd, &sid);\
	if (rc == 0)\
		return (pid_t)sid;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_tcsetpgrp(int fd, pid_t pgid)\
{\
	int rc = sinomo_wasix_proc_tcsetpgrp(fd, (int)pgid);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_getpriority(int which, id_t who)\
{\
	int priority = 0;\
	int rc = sinomo_wasix_proc_getpriority(which, (int)who, &priority);\
	if (rc == 0) {\
		errno = 0;\
		return priority;\
	}\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_setpriority(int which, id_t who, int priority)\
{\
	int rc = sinomo_wasix_proc_setpriority(which, (int)who, priority);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
}\
static inline int sinomo_wasi_nice(int increment)\
{\
	errno = 0;\
	int current = sinomo_wasi_getpriority(0, 0);\
	if (current == -1 && errno != 0)\
		return -1;\
	int next = current + increment;\
	if (next < -20)\
		next = -20;\
	if (next > 20)\
		next = 20;\
	if (sinomo_wasi_setpriority(0, 0, next) != 0)\
		return -1;\
	return next;\
}\
#  define getpgid(pid) sinomo_wasi_getpgid((pid))\
#  define getpgrp() sinomo_wasi_getpgrp()\
#  define getsid(pid) sinomo_wasi_getsid((pid))\
#  define setpgid(pid, pgid) sinomo_wasi_setpgid((pid), (pgid))\
#  define setsid() sinomo_wasi_setsid()\
#  define tcgetpgrp(fd) sinomo_wasi_tcgetpgrp((fd))\
#  define tcgetsid(fd) sinomo_wasi_tcgetsid((fd))\
#  define tcsetpgrp(fd, pgid) sinomo_wasi_tcsetpgrp((fd), (pgid))\
#  define getpriority(which, who) sinomo_wasi_getpriority((which), (who))\
#  define setpriority(which, who, priority) sinomo_wasi_setpriority((which), (who), (priority))\
#  define nice(increment) sinomo_wasi_nice((increment))\
# endif\
static inline int sinomo_wasi_fchdir(int fd)\
{\
#if defined(SINOMO_BUSYBOX_PREVIEW1_EXTENSIONS)\
	(void) fd;\
	errno = ENOTSUP;\
	return -1;\
#else\
	__attribute__((import_module("wasix_32v1"), import_name("fchdir")))\
	int sinomo_wasix_fchdir_import(int fd);\
	int rc = sinomo_wasix_fchdir_import(fd);\
	if (rc == 0)\
		return 0;\
	errno = rc;\
	return -1;\
#endif\
}\
# define fchdir(fd) sinomo_wasi_fchdir((fd))\
static inline int sinomo_wasi_chroot(const char *path)\
{\
	(void) path;\
	errno = EPERM;\
	return -1;\
}\
# define chroot(path) sinomo_wasi_chroot((path))\
static inline int sinomo_wasi_settimeofday(const struct timeval *tv, const void *tz)\
{\
	(void) tv;\
	(void) tz;\
	errno = EPERM;\
	return -1;\
}\
# define settimeofday(tv, tz) sinomo_wasi_settimeofday((tv), (tz))\
#endif' "${BUSYBOX_SRC}/include/libbb.h"

sed -i 's/^#ifdef __GLIBC__$/#if defined(__GLIBC__) || defined(__wasi__)/' "${BUSYBOX_SRC}/include/libbb.h"

sed -i 's/^# if (S_IRWXU | S_IRWXG | S_IRWXO | S_ISUID | S_ISGID | S_ISVTX) == 07777$/# if !defined(__wasi__) \&\& (S_IRWXU | S_IRWXG | S_IRWXO | S_ISUID | S_ISGID | S_ISVTX) == 07777/' "${BUSYBOX_SRC}/libbb/mode_string.c"

sed -i '/^#if defined(__FreeBSD__)/,/^#endif$/c\
#if defined(__wasi__)\
# include <sys/types.h>\
# ifndef makedev\
#  define makedev(ma, mi) ((((ma) & 0xfff) << 8) | ((mi) & 0xff) | (((mi) & 0xfffff00) << 12))\
# endif\
#elif defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__) \\\
 || defined(__APPLE__)\
# include <sys/types.h>\
#else\
# include <features.h>\
# include <sys/sysmacros.h>\
#endif' "${BUSYBOX_SRC}/libbb/makedev.c"

sed -i 's/^#ifdef __GLIBC__$/#if defined(__GLIBC__) || defined(__wasi__)/' "${BUSYBOX_SRC}/libbb/makedev.c"

sed -i '/^#include "libbb.h"$/a\
#if defined(__wasi__)\
# if defined(SINOMO_BUSYBOX_PREVIEW1_EXTENSIONS)\
__attribute__((import_module("sinomo_sandbox_preview1"), import_name("path_chmod")))\
int sinomo_sandbox_path_chmod(const char *path, unsigned path_len, unsigned mode);\
# else\
__attribute__((import_module("wasix_32v1"), import_name("path_chmod")))\
int sinomo_sandbox_path_chmod(const char *path, unsigned path_len, unsigned mode);\
# endif\
static int sinomo_wasi_chmod(const char *path, mode_t mode)\
{\
	int err = sinomo_sandbox_path_chmod(path, strlen(path), mode);\
	if (err == 0)\
		return 0;\
	errno = err;\
	return -1;\
}\
#else\
# define sinomo_wasi_chmod(path, mode) chmod((path), (mode))\
#endif' "${BUSYBOX_SRC}/coreutils/chmod.c"

sed -i 's/chmod(fileName, newmode)/sinomo_wasi_chmod(fileName, newmode)/' "${BUSYBOX_SRC}/coreutils/chmod.c"

sed -i '/R_d = (ENABLE_FEATURE_ALLOW_EXEC && L.s && \*L.s)/,+1c\
#if defined(__wasi__) && defined(SINOMO_BUSYBOX_PREVIEW1_EXTENSIONS)\
				errno = ENOTSUP;\
				setvar_i(intvar[ERRNO], errno);\
				R_d = -1;\
#else\
				R_d = (ENABLE_FEATURE_ALLOW_EXEC && L.s && *L.s)\
						? (system(L.s) >> 8) : 0;\
#endif' "${BUSYBOX_SRC}/editors/awk.c"

sed -i \
    -e 's/-Wl,--start-group//g' \
    -e 's/-Wl,--end-group//g' \
    -e 's/-Wl,--warn-common//g' \
    "${BUSYBOX_SRC}/scripts/trylink"

if [ "${BUSYBOX_TARGET_ABI}" = "wasix" ]; then
    BUSYBOX_TARGET_TRIPLE="wasm32-wasi"
    BUSYBOX_TARGET_SUFFIX="wasix"
    BUSYBOX_SYSROOT="${WASIX_SYSROOT}"
    WASI_CC="${WASI_SDK}/bin/clang --target=${BUSYBOX_TARGET_TRIPLE} --sysroot=${BUSYBOX_SYSROOT}"
    WASI_CC_FLAGS="-Os -DSINOMO_BUSYBOX_WASI_COMPAT -D_WASI_EMULATED_SIGNAL -D_WASI_EMULATED_MMAN -D_WASI_EMULATED_PROCESS_CLOCKS -D_WASI_EMULATED_GETPID -fno-ident -fdebug-prefix-map=${BUSYBOX_SRC}=busybox-${BUSYBOX_VERSION} -matomics -mbulk-memory -mmutable-globals -pthread -mthread-model posix -ftls-model=local-exec -msimd128 -mrelaxed-simd -mextended-const"
    WASI_LD_FLAGS="-Wl,--export-all -Wl,--export-dynamic -Wl,--shared-memory -Wl,--import-memory -Wl,--extra-features=atomics -Wl,--extra-features=bulk-memory -Wl,--extra-features=mutable-globals -Wl,--export=__wasm_call_ctors -Wl,--export=__wasm_init_tls -Wl,--export=__wasm_signal -Wl,--export=__tls_size -Wl,--export=__tls_align -Wl,--export=__tls_base -Wl,--export-if-defined=__indirect_function_table -Wl,--export-if-defined=__stack_pointer -Wl,--export-if-defined=__heap_base -Wl,--export-if-defined=__data_end -Wl,-z,stack-size=1048576 -Wl,--initial-memory=67108864 -Wl,--max-memory=268435456"
    WASI_LD_LIBS="-lwasi-emulated-getpid -lwasi-emulated-mman -lwasi-emulated-process-clocks -lresolv -lrt -lm -lpthread -lutil"
    SINOMO_WASIX_ARGV_COMPAT_SOURCE="${WORKDIR}/sinomo-wasix-argv-compat.c"
    SINOMO_WASIX_ARGV_COMPAT_OBJECT="${WORKDIR}/sinomo-wasix-argv-compat.o"
    cat > "${SINOMO_WASIX_ARGV_COMPAT_SOURCE}" <<'COMPAT'
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <termios.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <wasi/api_wasix.h>

static const char magic[] = "__SINOMO_WASIX_ESCAPED_STRING_LIST_V1__\n";
extern char **environ;
extern int __real_tcgetattr(int fd, struct termios *termios_p);
extern int __real_tcsetattr(int fd, int optional_actions, const struct termios *termios_p);
extern pid_t __real_waitpid(pid_t pid, int *status, int options);
extern int __real_waitid(idtype_t idtype, id_t id, siginfo_t *infop, int options);

__attribute__((import_module("wasix_32v1"), import_name("proc_tty_get_tostop")))
extern int sinomo_proc_tty_get_tostop(int *tostop);
__attribute__((import_module("wasix_32v1"), import_name("proc_tty_set_tostop")))
extern int sinomo_proc_tty_set_tostop(int tostop);
__attribute__((import_module("wasix_32v1"), import_name("proc_child_continued_status")))
extern int sinomo_proc_child_continued_status(int pid, int consume, int *continued_pid);
__attribute__((import_module("wasix_32v1"), import_name("proc_child_continued_status_wait")))
extern int sinomo_proc_child_continued_status_wait(int pid, int consume, int nonblocking, int *continued_pid);
__attribute__((import_module("wasix_32v1"), import_name("proc_wait_status")))
extern int sinomo_proc_wait_status(__wasi_option_pid_t *pid, __wasi_join_flags_t flags, __wasi_join_status_t *status);
__attribute__((import_module("wasix_32v1"), import_name("proc_wait_status_select")))
extern int sinomo_proc_wait_status_select(__wasi_option_pid_t *pid, __wasi_join_flags_t flags, uint32_t status_kinds, __wasi_join_status_t *status);
__attribute__((import_module("wasix_32v1"), import_name("proc_getrusage")))
extern int sinomo_proc_getrusage(int who, unsigned long long *user_usec, unsigned long long *system_usec, unsigned long long *maxrss_kib);

#define SINOMO_WAIT_STATUS_EXITED (1U << 0)
#define SINOMO_WAIT_STATUS_STOPPED (1U << 1)
#define SINOMO_WAIT_STATUS_CONTINUED (1U << 2)

static int sinomo_wait_status(__wasi_join_status_t *status) {
    switch (status->tag) {
    case __WASI_JOIN_STATUS_TYPE_NOTHING:
        return 0;
    case __WASI_JOIN_STATUS_TYPE_EXIT_NORMAL:
        return ((int)status->u.exit_normal & 0xff) << 8;
    case __WASI_JOIN_STATUS_TYPE_EXIT_SIGNAL:
        return (int)status->u.exit_signal.signal & 0x7f;
    case __WASI_JOIN_STATUS_TYPE_STOPPED:
        return (((int)status->u.stopped & 0xff) << 8) | 0x7f;
    default:
        errno = EINVAL;
        return -1;
    }
}

static pid_t sinomo_take_continued_child(pid_t pid) {
#ifdef WCONTINUED
    int continued_pid = 0;
    int rc = sinomo_proc_child_continued_status((int)pid, 1, &continued_pid);
    if (rc != 0) {
        errno = rc;
        return (pid_t)-1;
    }
    return (pid_t)continued_pid;
#else
    (void)pid;
    return (pid_t)0;
#endif
}

static pid_t sinomo_peek_continued_child(pid_t pid) {
#ifdef WCONTINUED
    int continued_pid = 0;
    int rc = sinomo_proc_child_continued_status((int)pid, 0, &continued_pid);
    if (rc != 0) {
        errno = rc;
        return (pid_t)-1;
    }
    return (pid_t)continued_pid;
#else
    (void)pid;
    return (pid_t)0;
#endif
}

static pid_t sinomo_wait_continued_child(pid_t pid, int consume, int nonblocking) {
#ifdef WCONTINUED
    int continued_pid = 0;
    int rc = sinomo_proc_child_continued_status_wait((int)pid, consume, nonblocking, &continued_pid);
    if (rc != 0) {
        errno = rc;
        return (pid_t)-1;
    }
    return (pid_t)continued_pid;
#else
    (void)pid;
    (void)consume;
    (void)nonblocking;
    return (pid_t)0;
#endif
}

char *__wrap___wasilibc_exec_combine_strings(char *const strings[]) {
    size_t combined_len = sizeof(magic) - 1;
    for (char **ptr = (char **)strings; *ptr != NULL; ptr++) {
        for (const unsigned char *p = (const unsigned char *)*ptr; *p != 0; p++) {
            combined_len += (*p == '\n' || *p == '\r' || *p == '\\') ? 2 : 1;
        }
        combined_len += 1;
    }

    char *combined = malloc(combined_len + 1);
    char *out = combined;
    memcpy(out, magic, sizeof(magic) - 1);
    out += sizeof(magic) - 1;
    for (char **ptr = (char **)strings; *ptr != NULL; ptr++) {
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

int __wrap_execve(const char *path, char *const argv[], char *const envp[]) {
    char *combined_argv = __wrap___wasilibc_exec_combine_strings(argv);
    char *combined_env = __wrap___wasilibc_exec_combine_strings(envp ? envp : environ);
    const char *search_path = getenv("PATH");
    if (search_path == NULL) {
        search_path = "";
    }

    __wasi_errno_t error = __wasi_proc_exec3(path, combined_argv, combined_env, 0, search_path);
    free(combined_argv);
    free(combined_env);
    errno = (int)error;
    return -1;
}

int __wrap_tcgetattr(int fd, struct termios *termios_p) {
    int rc = __real_tcgetattr(fd, termios_p);
#ifdef TOSTOP
    if (rc == 0 && termios_p != 0) {
        int tostop = 0;
        if (sinomo_proc_tty_get_tostop(&tostop) == 0) {
            if (tostop) {
                termios_p->c_lflag |= TOSTOP;
            } else {
                termios_p->c_lflag &= (tcflag_t)~TOSTOP;
            }
        }
    }
#endif
    return rc;
}

int __wrap_tcsetattr(int fd, int optional_actions, const struct termios *termios_p) {
    int rc = __real_tcsetattr(fd, optional_actions, termios_p);
#ifdef TOSTOP
    if (rc == 0 && termios_p != 0) {
        (void)sinomo_proc_tty_set_tostop((termios_p->c_lflag & TOSTOP) != 0);
    }
#endif
    return rc;
}

pid_t __wrap_waitpid(pid_t pid, int *status, int options) {
#ifdef WCONTINUED
    if ((options & WCONTINUED) != 0) {
        pid_t continued_pid = sinomo_take_continued_child(pid);
        if (continued_pid < 0) {
            return -1;
        }
        if (continued_pid > 0) {
            if (status != 0) {
                *status = 0xffff;
            }
            return continued_pid;
        }
    }
#endif

    __wasi_option_pid_t option_pid = {
        .tag = 1,
        .u = {.some = (__wasi_pid_t)pid},
    };
    __wasi_join_flags_t flags = 0;
    if ((options & WNOHANG) != 0) {
        flags |= __WASI_JOIN_FLAGS_NON_BLOCKING;
    }
    if ((options & WUNTRACED) != 0) {
        flags |= __WASI_JOIN_FLAGS_WAKE_STOPPED;
    }

    __wasi_join_status_t join_status = {0};
    __wasi_errno_t rc = __wasi_proc_join(&option_pid, flags, &join_status);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }

    int wait_status = sinomo_wait_status(&join_status);
    if (wait_status < 0) {
        return -1;
    }
    if (status != 0) {
        *status = wait_status;
    }
    return (pid_t)option_pid.u.some;
}

pid_t __wrap_wait(int *status) {
    return __wrap_waitpid(-1, status, 0);
}

static void sinomo_usec_timeval(unsigned long long value, struct timeval *out) {
    out->tv_sec = (time_t)(value / 1000000ULL);
    out->tv_usec = (suseconds_t)(value % 1000000ULL);
}

static unsigned long long sinomo_timeval_usec(const struct timeval *value) {
    return ((unsigned long long)value->tv_sec * 1000000ULL) + (unsigned long long)value->tv_usec;
}

static int sinomo_get_child_rusage(struct rusage *usage) {
    unsigned long long user_usec = 0;
    unsigned long long system_usec = 0;
    unsigned long long maxrss_kib = 0;
    int rc = sinomo_proc_getrusage(RUSAGE_CHILDREN, &user_usec, &system_usec, &maxrss_kib);
    if (rc != 0) {
        errno = rc;
        return -1;
    }
    memset(usage, 0, sizeof(*usage));
    sinomo_usec_timeval(user_usec, &usage->ru_utime);
    sinomo_usec_timeval(system_usec, &usage->ru_stime);
    usage->ru_maxrss = (long)maxrss_kib;
    return 0;
}

static void sinomo_child_rusage_delta(
    struct rusage *usage,
    const struct rusage *before,
    const struct rusage *after
) {
    unsigned long long before_user = sinomo_timeval_usec(&before->ru_utime);
    unsigned long long after_user = sinomo_timeval_usec(&after->ru_utime);
    unsigned long long before_system = sinomo_timeval_usec(&before->ru_stime);
    unsigned long long after_system = sinomo_timeval_usec(&after->ru_stime);

    memset(usage, 0, sizeof(*usage));
    sinomo_usec_timeval(after_user > before_user ? after_user - before_user : 0, &usage->ru_utime);
    sinomo_usec_timeval(after_system > before_system ? after_system - before_system : 0, &usage->ru_stime);
    usage->ru_maxrss = after->ru_maxrss;
}

pid_t __wrap_wait3(int *status, int options, struct rusage *usage) {
    struct rusage before;
    int have_before = usage != 0 && sinomo_get_child_rusage(&before) == 0;
    pid_t pid = __wrap_waitpid(-1, status, options);
    if (pid > 0 && usage != 0) {
        struct rusage after;
        if (have_before && sinomo_get_child_rusage(&after) == 0) {
            sinomo_child_rusage_delta(usage, &before, &after);
        } else {
            memset(usage, 0, sizeof(*usage));
        }
    }
    return pid;
}

pid_t __wrap_wait4(pid_t pid, int *status, int options, struct rusage *usage) {
    struct rusage before;
    int have_before = usage != 0 && sinomo_get_child_rusage(&before) == 0;
    pid_t waited = __wrap_waitpid(pid, status, options);
    if (waited > 0 && usage != 0) {
        struct rusage after;
        if (have_before && sinomo_get_child_rusage(&after) == 0) {
            sinomo_child_rusage_delta(usage, &before, &after);
        } else {
            memset(usage, 0, sizeof(*usage));
        }
    }
    return waited;
}

int __wrap_waitid(idtype_t idtype, id_t id, siginfo_t *infop, int options) {
    if (infop == 0) {
        errno = EFAULT;
        return -1;
    }
#ifdef WCONTINUED
    if ((options & WCONTINUED) != 0) {
        pid_t pid = -1;
        if (idtype == P_ALL) {
            pid = -1;
        } else if (idtype == P_PID) {
            pid = (pid_t)id;
        } else if (idtype == P_PGID) {
            pid = id == 0 ? 0 : -((pid_t)id);
        }
        int continued_only = (options & (WEXITED | WSTOPPED)) == 0;
        int continued_nonblocking = (options & WNOHANG) != 0 || !continued_only;
        pid_t continued_pid = continued_only
            ? sinomo_wait_continued_child(pid, (options & WNOWAIT) == 0, continued_nonblocking)
            : ((options & WNOWAIT) != 0
                ? sinomo_peek_continued_child(pid)
                : sinomo_take_continued_child(pid));
        if (continued_pid < 0) {
            return -1;
        }
        if (continued_pid > 0) {
            memset(infop, 0, sizeof(*infop));
            infop->si_signo = SIGCHLD;
            infop->si_pid = continued_pid;
            infop->si_uid = getuid();
            infop->si_code = CLD_CONTINUED;
            infop->si_status = SIGCONT;
            return 0;
        }
        if (continued_only) {
            memset(infop, 0, sizeof(*infop));
            return 0;
        }
    }
#endif

    __wasi_option_pid_t option_pid = {
        .tag = 1,
        .u = {.some = -1},
    };
    if (idtype == P_ALL) {
        option_pid.u.some = -1;
    } else if (idtype == P_PID) {
        option_pid.u.some = (__wasi_pid_t)id;
    } else if (idtype == P_PGID) {
        option_pid.u.some = id == 0 ? 0 : -((__wasi_pid_t)id);
    } else {
        return __real_waitid(idtype, id, infop, options);
    }

    __wasi_join_flags_t flags = 0;
    if ((options & WNOHANG) != 0) {
        flags |= __WASI_JOIN_FLAGS_NON_BLOCKING;
    }
    if ((options & WSTOPPED) != 0) {
        flags |= __WASI_JOIN_FLAGS_WAKE_STOPPED;
    }

    __wasi_join_status_t join_status = {0};
    uint32_t status_kinds = 0;
    if ((options & WEXITED) != 0) {
        status_kinds |= SINOMO_WAIT_STATUS_EXITED;
    }
    if ((options & WSTOPPED) != 0) {
        status_kinds |= SINOMO_WAIT_STATUS_STOPPED;
    }
    if ((options & WCONTINUED) != 0) {
        status_kinds |= SINOMO_WAIT_STATUS_CONTINUED;
    }
    __wasi_errno_t rc = ((options & WNOWAIT) != 0 || (options & WEXITED) == 0 || (options & WCONTINUED) != 0)
        ? sinomo_proc_wait_status_select(&option_pid, flags, status_kinds, &join_status)
        : __wasi_proc_join(&option_pid, flags, &join_status);
    if (rc != 0) {
        errno = (int)rc;
        return -1;
    }

    memset(infop, 0, sizeof(*infop));
    if (join_status.tag == __WASI_JOIN_STATUS_TYPE_NOTHING) {
        if ((options & WCONTINUED) != 0 && option_pid.u.some > 0) {
            if ((options & WNOWAIT) == 0) {
                pid_t consumed = sinomo_take_continued_child((pid_t)option_pid.u.some);
                if (consumed < 0) {
                    return -1;
                }
            }
            infop->si_signo = SIGCHLD;
            infop->si_pid = (pid_t)option_pid.u.some;
            infop->si_uid = getuid();
            infop->si_code = CLD_CONTINUED;
            infop->si_status = SIGCONT;
        }
        return 0;
    }
    if ((options & WCONTINUED) != 0 && (options & WNOWAIT) == 0 && join_status.tag != __WASI_JOIN_STATUS_TYPE_STOPPED) {
        __wasi_option_pid_t reap_pid = {.tag = 1, .u = {.some = option_pid.u.some}};
        rc = __wasi_proc_join(&reap_pid, flags, &join_status);
        if (rc != 0) {
            errno = (int)rc;
            return -1;
        }
        option_pid.u.some = reap_pid.u.some;
    }
    infop->si_signo = SIGCHLD;
    infop->si_pid = (pid_t)option_pid.u.some;
    infop->si_uid = getuid();
    infop->si_utime = 0;
    infop->si_stime = 0;

    switch (join_status.tag) {
    case __WASI_JOIN_STATUS_TYPE_EXIT_NORMAL:
        if ((options & WEXITED) == 0) {
            memset(infop, 0, sizeof(*infop));
            return 0;
        }
        infop->si_code = CLD_EXITED;
        infop->si_status = (int)join_status.u.exit_normal;
        return 0;
    case __WASI_JOIN_STATUS_TYPE_EXIT_SIGNAL:
        if ((options & WEXITED) == 0) {
            memset(infop, 0, sizeof(*infop));
            return 0;
        }
        infop->si_code = CLD_KILLED;
        infop->si_status = (int)join_status.u.exit_signal.signal;
        return 0;
    case __WASI_JOIN_STATUS_TYPE_STOPPED:
        if ((options & WSTOPPED) == 0) {
            memset(infop, 0, sizeof(*infop));
            return 0;
        }
        infop->si_code = CLD_STOPPED;
        infop->si_status = (int)join_status.u.stopped;
        return 0;
    default:
        errno = EINVAL;
        return -1;
    }
}
COMPAT
    "${WASI_SDK}/bin/clang" --target="${BUSYBOX_TARGET_TRIPLE}" --sysroot="${BUSYBOX_SYSROOT}" \
        ${WASI_CC_FLAGS} -c "${SINOMO_WASIX_ARGV_COMPAT_SOURCE}" -o "${SINOMO_WASIX_ARGV_COMPAT_OBJECT}"
    python3 - "${BUSYBOX_SRC}/scripts/trylink" "${SINOMO_WASIX_ARGV_COMPAT_OBJECT}" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
object_path = sys.argv[2]
text = path.read_text()
needle = "$START_GROUP $O_FILES $A_FILES $END_GROUP"
if needle not in text:
    raise SystemExit("busybox trylink object group target not found")
path.write_text(text.replace(needle, f"$START_GROUP $O_FILES $A_FILES {object_path} $END_GROUP"))
PY
    WASI_LD_FLAGS="-Wl,--wrap=__wasilibc_exec_combine_strings -Wl,--wrap=execve -Wl,--wrap=tcgetattr -Wl,--wrap=tcsetattr -Wl,--wrap=wait -Wl,--wrap=waitpid -Wl,--wrap=wait3 -Wl,--wrap=wait4 -Wl,--wrap=waitid ${WASI_LD_FLAGS}"
else
    BUSYBOX_TARGET_TRIPLE="wasm32-wasip1"
    BUSYBOX_TARGET_SUFFIX="wasip1"
    BUSYBOX_SYSROOT="${WASI_SDK}/share/wasi-sysroot"
    WASI_CC="${WASI_SDK}/bin/clang --target=${BUSYBOX_TARGET_TRIPLE} --sysroot=${BUSYBOX_SYSROOT}"
    WASI_CC_FLAGS="-Os -DSINOMO_BUSYBOX_WASI_COMPAT -DSINOMO_BUSYBOX_PREVIEW1_EXTENSIONS -D_WASI_EMULATED_SIGNAL -D_WASI_EMULATED_MMAN -D_WASI_EMULATED_PROCESS_CLOCKS -D_WASI_EMULATED_GETPID -I${ROOT}/wasi-compat/include -fno-ident -fdebug-prefix-map=${BUSYBOX_SRC}=busybox-${BUSYBOX_VERSION}"
    WASI_LD_FLAGS="-Wl,--export-all"
    WASI_LD_LIBS="-lwasi-emulated-signal -lwasi-emulated-mman -lwasi-emulated-process-clocks -lwasi-emulated-getpid"
fi

make -C "${BUSYBOX_SRC}" \
    -j"${JOBS}" \
    HOSTCC=gcc \
    CC="${WASI_CC}" \
    LD="${WASI_CC}" \
    STRIP="${WASI_SDK}/bin/llvm-strip" \
    CFLAGS="${WASI_CC_FLAGS}" \
    LDFLAGS="${WASI_LD_FLAGS}" \
    LDLIBS="${WASI_LD_LIBS}" \
    busybox

mkdir -p "${OUTDIR}"
BUSYBOX_UNOPTIMIZED_WASM="${OUTDIR}/busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX}.unoptimized.wasm"
BUSYBOX_WASM="${OUTDIR}/busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX}.wasm"
cp "${BUSYBOX_SRC}/busybox" "${BUSYBOX_UNOPTIMIZED_WASM}"
if [ "${BUSYBOX_TARGET_ABI}" = "wasix" ]; then
    "${BINARYEN}/bin/wasm-opt" \
        --asyncify \
        --fpcast-emu \
        -Os \
        --no-validation \
        --enable-threads \
        --enable-mutable-globals \
        --enable-bulk-memory \
        --enable-bulk-memory-opt \
        --enable-exception-handling \
        --enable-simd \
        --enable-relaxed-simd \
        --enable-extended-const \
        -o "${BUSYBOX_WASM}" \
        "${BUSYBOX_UNOPTIMIZED_WASM}"
else
    cp "${BUSYBOX_UNOPTIMIZED_WASM}" "${BUSYBOX_WASM}"
fi
cp "${BUSYBOX_SRC}/.config" "${OUTDIR}/busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX}.config"

BUSYBOX_WASM_SHA256="$(sha256sum "${BUSYBOX_WASM}" | awk '{print $1}')"
BUSYBOX_WASM_HASH="$(sinomo_hash "${BUSYBOX_WASM}")"

cat > "${OUTDIR}/busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX}.manifest" <<MANIFEST
busybox_version=${BUSYBOX_VERSION}
busybox_target_abi=${BUSYBOX_TARGET_ABI}
busybox_source=${BUSYBOX_URL}
busybox_sha256=${BUSYBOX_SHA256}
wasi_sdk_version=${WASI_SDK_VERSION_FULL}
wasi_sdk_source=${WASI_SDK_URL}
wasi_sdk_sha256=${WASI_SDK_SHA256}
MANIFEST

if [ "${BUSYBOX_TARGET_ABI}" = "wasix" ]; then
    cat >> "${OUTDIR}/busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX}.manifest" <<MANIFEST
binaryen_version=${BINARYEN_VERSION}
binaryen_source=${BINARYEN_URL}
binaryen_sha256=${BINARYEN_SHA256}
wasix_libc_version=${WASIX_LIBC_VERSION}
wasix_libc_source=${WASIX_LIBC_URL}
wasix_libc_sha256=${WASIX_LIBC_SHA256}
MANIFEST
fi

cat >> "${OUTDIR}/busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX}.manifest" <<MANIFEST
source_date_epoch=${SOURCE_DATE_EPOCH}
wasm=${BUSYBOX_WASM}
wasm_sha256=${BUSYBOX_WASM_SHA256}
wasm_hash=${BUSYBOX_WASM_HASH}
import_command=sinomo sandbox toolchain import-busybox --version busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX} --provenance 'Sinomo BusyBox ${BUSYBOX_TARGET_SUFFIX} recipe: BusyBox ${BUSYBOX_VERSION}, wasi-sdk ${WASI_SDK_VERSION_FULL}, sha256 ${BUSYBOX_WASM_SHA256}' ${BUSYBOX_WASM}
MANIFEST

if [ "${BUSYBOX_TARGET_ABI}" = "wasix" ]; then
    cat > "${OUTDIR}/package.json" <<PACKAGE
{
  "toolchain_id": "086wdctfpphb26n9k2cg751qqs",
  "version": "busybox-${BUSYBOX_VERSION}-wasix",
  "provenance": "Sinomo BusyBox WASIX recipe: BusyBox ${BUSYBOX_VERSION}, wasi-sdk ${WASI_SDK_VERSION_FULL}",
  "target_abi": "wasix",
  "commands": [
    "awk", "cat", "chmod", "cp", "cmp", "date", "diff", "dirname", "echo", "env", "false", "find", "grep", "gzip", "head", "ln", "ls", "mkdir", "mv", "patch", "printf", "pwd", "rm", "rmdir", "sed", "sha256sum", "sh", "sort", "tail", "tar", "test", "touch", "true", "tr", "uniq", "uname", "wc"
  ],
  "artifacts": []
}
PACKAGE
    python3 - "${OUTDIR}/package.json" "${BUSYBOX_WASM}" "${BUSYBOX_WASM_HASH}" <<'PY'
import json
import sys
from pathlib import Path

package_path = Path(sys.argv[1])
module_path = Path(sys.argv[2])
module_hash = sys.argv[3]
package = json.loads(package_path.read_text())
package["commands"] = [
    {
        "name": name,
        "module": module_path.name,
        "content_hash": module_hash,
        "default_limits": {
            "memory_mb": 256,
            "wall_time_ms": 10000,
            "max_input_bytes": 1048576,
            "max_output_bytes": 1048576,
            "max_workspace_bytes": 1073741824,
            "max_written_bytes_per_command": 67108864,
            "max_file_count": 100000,
        },
    }
    for name in package["commands"]
]
package_path.write_text(json.dumps(package, indent=2) + "\n")
PY
fi

file "${BUSYBOX_WASM}"
echo "wrote ${BUSYBOX_WASM}"
echo "sha256 ${BUSYBOX_WASM_SHA256}"
echo "hash ${BUSYBOX_WASM_HASH}"
echo "manifest ${OUTDIR}/busybox-${BUSYBOX_VERSION}-${BUSYBOX_TARGET_SUFFIX}.manifest"
if [ "${BUSYBOX_TARGET_ABI}" = "wasix" ]; then
    echo "package ${OUTDIR}/package.json"
fi
