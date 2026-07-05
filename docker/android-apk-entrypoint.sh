#!/usr/bin/env bash
set -euo pipefail

args=("$@")
has_profile=0
profile_name=""

for ((i = 0; i < ${#args[@]}; i++)); do
  case "${args[$i]}" in
    --release|-r|--profile|--profile=*)
      has_profile=1
      ;;
  esac

  case "${args[$i]}" in
    --profile)
      profile_name="${args[$((i + 1))]:-}"
      ;;
    --profile=*)
      profile_name="${args[$i]#--profile=}"
      ;;
  esac
done

if [[ "${has_profile}" -eq 0 ]]; then
  profile_name="android-debug-small"
  args=(--profile android-debug-small "${args[@]}")
fi

if [[ "${profile_name}" == "android-debug-small" ]]; then
  debug_keystore="${CARGO_APK_ANDROID_DEBUG_SMALL_KEYSTORE:-/workspace/target/android-debug.keystore}"
  debug_keystore_password="${CARGO_APK_ANDROID_DEBUG_SMALL_KEYSTORE_PASSWORD:-android}"

  if [[ ! -f "${debug_keystore}" ]]; then
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
  fi

  export CARGO_APK_ANDROID_DEBUG_SMALL_KEYSTORE="${debug_keystore}"
  export CARGO_APK_ANDROID_DEBUG_SMALL_KEYSTORE_PASSWORD="${debug_keystore_password}"
fi

status=0
cargo apk build -p sinomo-app-android "${args[@]}" || status=$?

if [[ -n "${HOST_UID:-}" && -n "${HOST_GID:-}" ]]; then
  chown -R "${HOST_UID}:${HOST_GID}" \
    /workspace/target/android-debug.keystore \
    /workspace/target/android-debug-small/apk \
    /workspace/target/debug/apk \
    /workspace/target/release/apk \
    /workspace/target/aarch64-linux-android \
    /workspace/target/x86_64-linux-android \
    2>/dev/null || true
fi

exit "${status}"
