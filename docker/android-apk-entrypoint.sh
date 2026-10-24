#!/usr/bin/env bash
set -euo pipefail

args=("$@")
has_profile=0
profile_name=""
target_dir="${CARGO_TARGET_DIR:-/workspace/target}"

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
    --release|-r)
      profile_name="release"
      ;;
    --target-dir)
      target_dir="${args[$((i + 1))]:-}"
      ;;
    --target-dir=*)
      target_dir="${args[$i]#--target-dir=}"
      ;;
  esac
done

if [[ "${has_profile}" -eq 0 ]]; then
  profile_name="android-debug-small"
  args=(--profile android-debug-small "${args[@]}")
fi

if [[ "${profile_name}" == "android-debug-small" ]]; then
  debug_keystore="${CARGO_APK_ANDROID_DEBUG_SMALL_KEYSTORE:-${target_dir}/android-debug.keystore}"
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

if [[ "${status}" -eq 0 ]]; then
  profile_name="${profile_name:-debug}"
  [[ "${profile_name}" != "dev" ]] || profile_name="debug"
  signing_profile="${profile_name^^}"
  signing_profile="${signing_profile//-/_}"
  [[ "${signing_profile}" != "DEBUG" ]] || signing_profile="DEV"
  keystore_var="CARGO_APK_${signing_profile}_KEYSTORE"
  password_var="CARGO_APK_${signing_profile}_KEYSTORE_PASSWORD"
  if [[ "${signing_profile}" == "DEV" && -z "${!password_var:-}" ]]; then
    export CARGO_APK_DEV_KEYSTORE_PASSWORD="android"
  fi
  # Use the same key as cargo-apk; never silently change the APK signer.
  python3 modules/desktop/app-use/android/package.py \
    "${target_dir}/${profile_name}/apk/sinomo-app.apk" \
    --keystore "${!keystore_var:-${HOME}/.android/debug.keystore}" \
    --password-env "${password_var}" || status=$?
fi

if [[ -n "${HOST_UID:-}" && -n "${HOST_GID:-}" ]]; then
  chown -R "${HOST_UID}:${HOST_GID}" \
    "${target_dir}/android-debug.keystore" \
    "${target_dir}/android-debug-small/apk" \
    "${target_dir}/debug/apk" \
    "${target_dir}/release/apk" \
    "${target_dir}/aarch64-linux-android" \
    "${target_dir}/x86_64-linux-android" \
    "${target_dir}/cargo-apk-temp-extra-link-libraries" \
    2>/dev/null || true
fi

exit "${status}"
