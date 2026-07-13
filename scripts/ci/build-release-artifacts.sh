#!/usr/bin/env bash
set -euo pipefail

workspace_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${workspace_root}"

export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER="${CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER:-aarch64-linux-gnu-gcc}"
export CC_aarch64_unknown_linux_gnu="${CC_aarch64_unknown_linux_gnu:-aarch64-linux-gnu-gcc}"
export AR_aarch64_unknown_linux_gnu="${AR_aarch64_unknown_linux_gnu:-aarch64-linux-gnu-ar}"
export CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER="${CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER:-x86_64-w64-mingw32-gcc-posix}"
export CC_x86_64_pc_windows_gnu="${CC_x86_64_pc_windows_gnu:-x86_64-w64-mingw32-gcc-posix}"
export AR_x86_64_pc_windows_gnu="${AR_x86_64_pc_windows_gnu:-x86_64-w64-mingw32-ar}"

target_dir="${CARGO_TARGET_DIR:-${workspace_root}/target}"
dist_dir="${SINOMO_DIST_DIR:-${workspace_root}/dist}"
build_id="${SINOMO_BUILD_ID:-${CI_COMMIT_TAG:-${CI_COMMIT_SHORT_SHA:-dev}}}"
build_id="${build_id//\//-}"

read -r -a targets <<< "${SINOMO_RELEASE_TARGETS:-x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu x86_64-pc-windows-gnu}"
mkdir -p "${dist_dir}"

archives=()
for target in "${targets[@]}"; do
  case "${target}" in
    x86_64-unknown-linux-gnu)
      platform="linux-x86_64"
      binary_name="sinomo"
      archive_name="sinomo-${build_id}-${platform}.tar.gz"
      ;;
    aarch64-unknown-linux-gnu)
      platform="linux-aarch64"
      binary_name="sinomo"
      archive_name="sinomo-${build_id}-${platform}.tar.gz"
      ;;
    x86_64-pc-windows-gnu)
      platform="windows-x86_64"
      binary_name="sinomo.exe"
      archive_name="sinomo-${build_id}-${platform}.zip"
      ;;
    *)
      echo "Unsupported release target: ${target}" >&2
      exit 1
      ;;
  esac

  echo "Building sinomo for ${target}"
  cargo build --locked --release -p sinomo --target "${target}"

  binary_path="${target_dir}/${target}/release/${binary_name}"
  if [[ ! -f "${binary_path}" ]]; then
    echo "Expected binary was not produced: ${binary_path}" >&2
    exit 1
  fi
  file "${binary_path}"

  if [[ "${archive_name}" == *.zip ]]; then
    zip -j -q "${dist_dir}/${archive_name}" "${binary_path}"
  else
    tar -C "$(dirname "${binary_path}")" -czf "${dist_dir}/${archive_name}" "${binary_name}"
  fi
  archives+=("${archive_name}")
done

(
  cd "${dist_dir}"
  sha256sum "${archives[@]}" > SHA256SUMS
)

echo "Produced release artifacts:"
for archive in "${archives[@]}"; do
  echo "  ${dist_dir}/${archive}"
done
echo "  ${dist_dir}/SHA256SUMS"
