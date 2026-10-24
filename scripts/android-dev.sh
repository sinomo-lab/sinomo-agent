#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

ndk_version="27.2.12479018"
package_name="net.sinomo.app"
profile_dir="debug"

if [[ -n "${SINOMO_ANDROID_HOME:-}" ]]; then
  resolved_android_home="${SINOMO_ANDROID_HOME}"
elif [[ -d "${HOME}/.local/share/android-sdk/platforms/android-35" ]]; then
  resolved_android_home="${HOME}/.local/share/android-sdk"
elif [[ -n "${ANDROID_HOME:-}" ]]; then
  resolved_android_home="${ANDROID_HOME}"
else
  resolved_android_home="/opt/android-sdk"
fi

export ANDROID_HOME="${resolved_android_home}"
unset ANDROID_SDK_ROOT
export ANDROID_NDK_HOME="${ANDROID_NDK_HOME:-${ANDROID_HOME}/ndk/${ndk_version}}"
export ANDROID_NDK_ROOT="${ANDROID_NDK_ROOT:-${ANDROID_NDK_HOME}}"
export PATH="${ANDROID_HOME}/platform-tools:${ANDROID_HOME}/cmdline-tools/latest/bin:${HOME}/.cargo/bin:${PATH}"
export CARGO_TARGET_DIR="${SINOMO_ANDROID_TARGET_DIR:-${repo_root}/target/android-host}"
export CARGO_PROFILE_DEV_DEBUG="${CARGO_PROFILE_DEV_DEBUG:-1}"

apk_path="${CARGO_TARGET_DIR}/${profile_dir}/apk/sinomo-app.apk"
debug_keystore="${CARGO_TARGET_DIR}/android-debug.keystore"
debug_keystore_password="android"
export CARGO_APK_DEV_KEYSTORE="${debug_keystore}"
export CARGO_APK_DEV_KEYSTORE_PASSWORD="${debug_keystore_password}"

usage() {
  cat <<'EOF'
Usage: scripts/android-dev.sh <command>

Commands:
  doctor       Check SDK, NDK, Rust, cargo-apk, and ADB device access
  build        Build an APK with line-table debug symbols
  install      Install the existing debug APK on the connected device
  run          Build, install, and launch the app
  logs         Follow logcat for the running app process
  debug        Build, install, launch, and attach NDK LLDB
  uninstall    Remove the app from the connected device

Set ANDROID_SERIAL when more than one device is connected.
EOF
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

ensure_debug_keystore() {
  require_command keytool
  if [[ -f "${debug_keystore}" ]]; then
    return
  fi
  mkdir -p "$(dirname "${debug_keystore}")"
  keytool -genkeypair \
    -keystore "${debug_keystore}" \
    -storepass "${debug_keystore_password}" \
    -keypass "${debug_keystore_password}" \
    -alias androiddebugkey \
    -keyalg RSA \
    -keysize 2048 \
    -validity 10000 \
    -dname "CN=Android Debug,O=Android,C=US" \
    -noprompt
}

device_serial() {
  if [[ -n "${ANDROID_SERIAL:-}" ]]; then
    printf '%s\n' "${ANDROID_SERIAL}"
    return
  fi

  mapfile -t devices < <(adb devices | awk 'NR > 1 && $2 == "device" { print $1 }')
  if [[ "${#devices[@]}" -eq 0 ]]; then
    echo "No authorized Android device is available:" >&2
    adb devices -l >&2
    exit 1
  fi
  if [[ "${#devices[@]}" -gt 1 ]]; then
    echo "Multiple Android devices are connected; set ANDROID_SERIAL:" >&2
    adb devices -l >&2
    exit 1
  fi
  printf '%s\n' "${devices[0]}"
}

build_apk() {
  require_command cargo
  ensure_debug_keystore
  cargo apk build -p sinomo-app-android --target-dir "${CARGO_TARGET_DIR}"
  python3 modules/desktop/app-use/android/package.py "${apk_path}" \
    --keystore "${debug_keystore}" --password-env CARGO_APK_DEV_KEYSTORE_PASSWORD
}

install_apk() {
  if [[ ! -f "${apk_path}" ]]; then
    echo "Debug APK is missing; run '$0 build' first." >&2
    exit 1
  fi
  local serial
  serial="$(device_serial)"
  adb -s "${serial}" install -r -t "${apk_path}"
}

launch_app() {
  local serial
  serial="$(device_serial)"
  adb -s "${serial}" shell am start -S -n "${package_name}/android.app.NativeActivity"
}

doctor() {
  local failed=0
  for command in adb cargo cargo-apk keytool rustup; do
    if command -v "${command}" >/dev/null 2>&1; then
      printf 'ok  %-12s %s\n' "${command}" "$(command -v "${command}")"
    else
      printf 'ERR %-12s missing\n' "${command}" >&2
      failed=1
    fi
  done

  for path in \
    "${ANDROID_HOME}/platforms/android-35" \
    "${ANDROID_HOME}/build-tools/35.0.0" \
    "${ANDROID_NDK_HOME}"; do
    if [[ -d "${path}" ]]; then
      printf 'ok  %-12s %s\n' "sdk" "${path}"
    else
      printf 'ERR %-12s missing: %s\n' "sdk" "${path}" >&2
      failed=1
    fi
  done

  if rustup target list --installed | grep -qx 'aarch64-linux-android'; then
    printf 'ok  %-12s aarch64-linux-android\n' "rust-target"
  else
    printf 'ERR %-12s missing aarch64-linux-android\n' "rust-target" >&2
    failed=1
  fi

  adb devices -l
  if ! device_serial >/dev/null; then
    failed=1
  fi
  return "${failed}"
}

command="${1:-}"
case "${command}" in
  doctor)
    doctor
    ;;
  build)
    build_apk
    ;;
  install)
    require_command adb
    install_apk
    ;;
  run)
    require_command adb
    build_apk
    install_apk
    launch_app
    ;;
  logs)
    require_command adb
    serial="$(device_serial)"
    pid="$(adb -s "${serial}" shell pidof "${package_name}" | tr -d '\r')"
    if [[ -z "${pid}" ]]; then
      echo "${package_name} is not running; run '$0 run' first." >&2
      exit 1
    fi
    exec adb -s "${serial}" logcat -v color --pid="${pid}"
    ;;
  debug)
    require_command adb
    require_command cargo-apk
    ensure_debug_keystore
    serial="$(device_serial)"
    exec cargo apk gdb -p sinomo-app-android \
      --target-dir "${CARGO_TARGET_DIR}" --device "${serial}"
    ;;
  uninstall)
    require_command adb
    serial="$(device_serial)"
    adb -s "${serial}" uninstall "${package_name}"
    ;;
  *)
    usage
    [[ -n "${command}" ]] && exit 1
    ;;
esac
