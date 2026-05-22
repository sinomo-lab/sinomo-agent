#!/usr/bin/env bash
set -euo pipefail

PACMAN_VERSION="${PACMAN_VERSION:-7.1.0}"
PACMAN_SHA256="${PACMAN_SHA256:-1090f680958a73dc1f443a1eb919bd2ad77b3caff38e92e9737dec100c647bc9}"
MAKEPKG_TOOLCHAIN_ID="${MAKEPKG_TOOLCHAIN_ID:-01k4z8q5d00000000000000000}"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1748736000}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOLCHAINS_ROOT="$(cd "${ROOT}/.." && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
WORKDIR="${WORKDIR:-"${ROOT}/work-makepkg"}"
OUTDIR="${OUTDIR:-"${ROOT}/out-makepkg"}"
BASH_OUTDIR="${BASH_OUTDIR:-"${TOOLCHAINS_ROOT}/bash/out"}"

PACMAN_TARBALL="pacman-v${PACMAN_VERSION}.tar.gz"
PACMAN_URL="https://gitlab.archlinux.org/pacman/pacman/-/archive/v${PACMAN_VERSION}/${PACMAN_TARBALL}"

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

render_template() {
    local input="$1"
    local output="$2"
    mkdir -p "$(dirname "${output}")"
    sed \
        -e "s|@PACKAGE_VERSION@|${PACMAN_VERSION}|g" \
        -e "s|@localedir@|/usr/share/locale|g" \
        -e "s|@sysconfdir@|/etc|g" \
        -e "s|@localstatedir@|/var|g" \
        -e "s|@libmakepkgdir@|/usr/share/makepkg|g" \
        -e "s|@BUILDSCRIPT@|PKGBUILD|g" \
        -e "s|@BSDTAR_NO_READ_SPARSE@||g" \
        -e "s|@DEBUGSUFFIX@|debug|g" \
        -e "s|@FILECMD@|file|g" \
        -e "s|@INODECMD@|stat -c %i|g" \
        -e "s|@TEMPLATE_DIR@|/usr/share/makepkg-template|g" \
        -e "s|@keyringdir@|/usr/share/pacman/keyrings|g" \
        -e "s|@ROOTDIR@|/|g" \
        -e "s|@CARCH@|wasm32-wasix|g" \
        -e "s|@CHOST@|wasm32-wasix|g" \
        -e "s|@STRIP_BINARIES@||g" \
        -e "s|@STRIP_SHARED@||g" \
        -e "s|@STRIP_STATIC@||g" \
        -e "s|@PKGEXT@|.pkg.tar.gz|g" \
        -e "s|@SRCEXT@|.src.tar.gz|g" \
        "${input}" > "${output}"
}

mkdir -p "${WORKDIR}" "${OUTDIR}" "${WORKDIR}/downloads"
download "${PACMAN_URL}" "${WORKDIR}/downloads/${PACMAN_TARBALL}"
verify_sha256 "${PACMAN_SHA256}" "${WORKDIR}/downloads/${PACMAN_TARBALL}"

BASH_PACKAGE="${BASH_PACKAGE:-"${BASH_OUTDIR}/package.json"}"
if [ ! -f "${BASH_PACKAGE}" ]; then
    echo "Bash package manifest not found at ${BASH_PACKAGE}; build Bash first or set BASH_PACKAGE" >&2
    exit 1
fi
BASH_MODULE="$(awk '
    /"name"[[:space:]]*:[[:space:]]*"bash"/ { in_bash = 1 }
    in_bash && /"module"/ {
        gsub(/[",]/, "", $2)
        print $2
        exit
    }
' "${BASH_PACKAGE}")"
if [ -z "${BASH_MODULE}" ] || [ ! -f "$(dirname "${BASH_PACKAGE}")/${BASH_MODULE}" ]; then
    echo "Bash module referenced by ${BASH_PACKAGE} was not found" >&2
    exit 1
fi

rm -rf "${WORKDIR}/pacman-v${PACMAN_VERSION}" "${OUTDIR}/package-root"
tar -C "${WORKDIR}" -xzf "${WORKDIR}/downloads/${PACMAN_TARBALL}"
PACMAN_SRC="${WORKDIR}/pacman-v${PACMAN_VERSION}"
PACKAGE_ROOT="${OUTDIR}/package-root"

cp "$(dirname "${BASH_PACKAGE}")/${BASH_MODULE}" "${OUTDIR}/makepkg-bash.wasm"
cp "${PACMAN_SRC}/COPYING" "${OUTDIR}/COPYING"
printf '%s\n' "${PACMAN_VERSION}" > "${OUTDIR}/VERSION"

render_template "${PACMAN_SRC}/scripts/makepkg.sh.in" "${PACKAGE_ROOT}/usr/bin/makepkg"
chmod 0755 "${PACKAGE_ROOT}/usr/bin/makepkg"
render_template "${PACMAN_SRC}/etc/makepkg.conf.in" "${PACKAGE_ROOT}/etc/makepkg.conf"
render_template "${PACMAN_SRC}/etc/pacman.conf.in" "${PACKAGE_ROOT}/etc/pacman.conf"
mkdir -p "${PACKAGE_ROOT}/etc/makepkg.conf.d" "${PACKAGE_ROOT}/etc/pacman.d" "${PACKAGE_ROOT}/var/lib/pacman" "${PACKAGE_ROOT}/var/cache/pacman/pkg"
for conf in "${PACMAN_SRC}"/etc/makepkg.conf.d/*.conf.in; do
    render_template "${conf}" "${PACKAGE_ROOT}/etc/makepkg.conf.d/$(basename "${conf}" .in)"
done

find "${PACMAN_SRC}/scripts/libmakepkg" -type f -name '*.sh.in' | sort | while read -r source; do
    relative="${source#"${PACMAN_SRC}/scripts/libmakepkg/"}"
    render_template "${source}" "${PACKAGE_ROOT}/usr/share/makepkg/${relative%.in}"
done

cat > "${PACKAGE_ROOT}/etc/makepkg.conf.d/sinomo-wasix.conf" <<'CONF'
CARCH="wasm32-wasix"
CHOST="wasm32-wasix"
MAKEFLAGS="-j1"
BUILDENV=(!distcc !color !ccache !check !sign)
OPTIONS=(!strip docs libtool staticlibs emptydirs !zipman !purge !debug !lto !autodeps)
INTEGRITY_CHECK=(sha256)
PKGEXT='.pkg.tar.gz'
SRCEXT='.src.tar.gz'
PACMAN_AUTH=()
CONF

find "${PACKAGE_ROOT}" -type f | sort | while read -r file; do
    relative="${file#"${PACKAGE_ROOT}/"}"
    cp "${file}" "${OUTDIR}/artifact-${relative//\//__}"
done

MAKEPKG_HASH="$(sinomo_hash "${OUTDIR}/makepkg-bash.wasm")"
COPYING_HASH="$(sinomo_hash "${OUTDIR}/COPYING")"
VERSION_HASH="$(sinomo_hash "${OUTDIR}/VERSION")"

{
    cat <<PACKAGE
{
  "toolchain_id": "${MAKEPKG_TOOLCHAIN_ID}",
  "version": "makepkg-${PACMAN_VERSION}-wasix",
  "provenance": "Sinomo makepkg WASIX recipe: pacman ${PACMAN_VERSION}, Bash WASIX package",
  "target_abi": "wasix",
  "commands": [
    {
      "name": "makepkg",
      "module": "makepkg-bash.wasm",
      "content_hash": "${MAKEPKG_HASH}",
      "exec_name": "bash",
      "argv_prefix": ["/usr/bin/makepkg"],
      "default_env": {
        "MAKEPKG_LIBRARY": "/usr/share/makepkg",
        "MAKEPKG_CONF": "/etc/makepkg.conf",
        "PATH": "/bin:/usr/bin"
      },
      "required_capabilities": ["child-process-exec", "wasix.unpinned-child-exec"],
      "default_limits": {
        "memory_mb": 256,
        "wall_time_ms": 30000,
        "max_input_bytes": 1048576,
        "max_output_bytes": 2097152,
        "max_workspace_bytes": 1073741824,
        "max_written_bytes_per_command": 67108864,
        "max_file_count": 100000
      }
    }
  ],
  "artifacts": [
    {
      "relative_path": "share/makepkg/COPYING",
      "file": "COPYING",
      "content_hash": "${COPYING_HASH}"
    },
    {
      "relative_path": "share/makepkg/VERSION",
      "file": "VERSION",
      "content_hash": "${VERSION_HASH}"
    }
PACKAGE
    find "${PACKAGE_ROOT}" -type f | sort | while read -r file; do
        relative="${file#"${PACKAGE_ROOT}/"}"
        artifact_file="artifact-${relative//\//__}"
        artifact_hash="$(sinomo_hash "${OUTDIR}/${artifact_file}")"
        cat <<ARTIFACT
    ,
    {
      "relative_path": "${relative}",
      "file": "${artifact_file}",
      "content_hash": "${artifact_hash}"
    }
ARTIFACT
    done
    cat <<PACKAGE
  ]
}
PACKAGE
} > "${OUTDIR}/package.json"

cat > "${OUTDIR}/makepkg-${PACMAN_VERSION}-build.manifest" <<MANIFEST
pacman_version=${PACMAN_VERSION}
pacman_source=${PACMAN_URL}
pacman_sha256=${PACMAN_SHA256}
toolchain_id=${MAKEPKG_TOOLCHAIN_ID}
bash_package=${BASH_PACKAGE}
source_date_epoch=${SOURCE_DATE_EPOCH}
MANIFEST

file "${OUTDIR}/makepkg-bash.wasm"
echo "wrote ${OUTDIR}/package.json"
