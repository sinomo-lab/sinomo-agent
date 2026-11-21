#!/usr/bin/env bash
# Provision the Linux builder used by Compose and the GitLab Docker executor.
set -euo pipefail

: "${ANDROID_HOME:?set ANDROID_HOME to the SDK installation directory}"
api="${ANDROID_API:-35}"
build_tools="${ANDROID_BUILD_TOOLS:-35.0.0}"
ndk="${ANDROID_NDK_VERSION:-27.2.12479018}"
tools_url="${ANDROID_COMMANDLINE_TOOLS_URL:-https://dl.google.com/android/repository/commandlinetools-linux-11076708_latest.zip}"

apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates curl git openjdk-17-jdk-headless python3 unzip cmake clang libclang-dev

tmp=$(mktemp -d)
trap 'rm -r -- "$tmp"' EXIT
mkdir -p "$ANDROID_HOME/cmdline-tools"
curl --fail --location --show-error --silent --retry 3 "$tools_url" -o "$tmp/tools.zip"
unzip -q "$tmp/tools.zip" -d "$tmp"
mv "$tmp/cmdline-tools" "$ANDROID_HOME/cmdline-tools/latest"
sdkmanager="$ANDROID_HOME/cmdline-tools/latest/bin/sdkmanager"
# yes may receive SIGPIPE once all prompts have been answered.
(set +o pipefail; yes | "$sdkmanager" --sdk_root="$ANDROID_HOME" --licenses >/dev/null)
"$sdkmanager" --sdk_root="$ANDROID_HOME" "platform-tools" "platforms;android-$api" \
    "build-tools;$build_tools" "ndk;$ndk"
rustup target add aarch64-linux-android x86_64-linux-android
cargo install cargo-apk --version 0.10.0 --locked --root /usr/local
