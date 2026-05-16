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
#if defined(SINOMO_BUSYBOX_PREVIEW1_EXTENSIONS)\
__attribute__((import_module("sinomo_sandbox_preview1"), import_name("path_mode")))\
int sinomo_sandbox_path_mode(const char *path, unsigned path_len, unsigned *mode);\
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
	(void) path;\
	(void) mode;\
	(void) dev;\
	errno = ENOTSUP;\
	return -1;\
}\
# define mknod(path, mode, dev) sinomo_wasi_mknod((path), (mode), (dev))\
static inline int sinomo_wasi_getgroups(int size, gid_t list[])\
{\
	if (size == 0)\
		return 1;\
	if (size < 1) {\
		errno = EINVAL;\
		return -1;\
	}\
	list[0] = getgid();\
	return 1;\
}\
# define getgroups(size, list) sinomo_wasi_getgroups((size), (list))\
static inline int sinomo_wasi_fchdir(int fd)\
{\
	(void) fd;\
	errno = ENOTSUP;\
	return -1;\
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
#if defined(SINOMO_BUSYBOX_PREVIEW1_EXTENSIONS)\
__attribute__((import_module("sinomo_sandbox_preview1"), import_name("path_chmod")))\
int sinomo_sandbox_path_chmod(const char *path, unsigned path_len, unsigned mode);\
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
#if defined(__wasi__)\
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
#include <wasi/api_wasix.h>

static const char magic[] = "__SINOMO_WASIX_ESCAPED_STRING_LIST_V1__\n";
extern char **environ;

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
    WASI_LD_FLAGS="-Wl,--wrap=__wasilibc_exec_combine_strings -Wl,--wrap=execve ${WASI_LD_FLAGS}"
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
