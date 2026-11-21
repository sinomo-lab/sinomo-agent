#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

args=("$@")
has_profile=0
profile_name=""
target_dir="${CARGO_TARGET_DIR:-$repo_root/target}"

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

if [[ "${profile_name}" == "android-debug-small" || "${profile_name}" == "android-debug" ]]; then
  signing_profile="${profile_name^^}"
  signing_profile="${signing_profile//-/_}"
  keystore_var="CARGO_APK_${signing_profile}_KEYSTORE"
  password_var="CARGO_APK_${signing_profile}_KEYSTORE_PASSWORD"
  debug_keystore="${!keystore_var:-${target_dir}/android-debug.keystore}"
  debug_keystore_password="${!password_var:-android}"

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

  export "${keystore_var}=${debug_keystore}"
  export "${password_var}=${debug_keystore_password}"
fi

cargo apk build -p sinomo-app-android "${args[@]}"

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
python3 scripts/android-runtime-jars.py --output "$target_dir/android-java"
package_args=(--app-res crates/sinomo-app-android/res
              --java-archive "$target_dir/android-java/rustls-platform-verifier.jar"
              --java-archive "$target_dir/android-java/kotlin-stdlib.jar")
if [[ "${profile_name}" == "android-debug" || "${profile_name}" == "android-debug-small" ]]; then
  package_args+=(--debuggable)
fi
# Use the same key as cargo-apk; never silently change the APK signer.
python3 modules/desktop/app-use/android/package.py \
  "${target_dir}/${profile_name}/apk/sinomo-app.apk" \
  --keystore "${!keystore_var:-${HOME}/.android/debug.keystore}" \
  --password-env "${password_var}" "${package_args[@]}"
