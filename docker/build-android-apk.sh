#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${repo_root}/docker/docker-compose.android.yml"
image="${SINOMO_ANDROID_IMAGE:-sinomo-android-dev:latest}"

export HOST_UID="${HOST_UID:-$(id -u)}"
export HOST_GID="${HOST_GID:-$(id -g)}"
export SINOMO_ANDROID_IMAGE="${image}"
export SINOMO_REPO_ROOT="${SINOMO_REPO_ROOT:-${repo_root}}"

docker compose -f "${compose_file}" run --build --rm android-apk "$@"
