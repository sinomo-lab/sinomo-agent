#!/usr/bin/env bash
set -euo pipefail

status=0
cargo apk build -p sinomo-app-android "$@" || status=$?

if [[ -n "${HOST_UID:-}" && -n "${HOST_GID:-}" ]]; then
  chown -R "${HOST_UID}:${HOST_GID}" \
    /workspace/target/debug/apk \
    /workspace/target/aarch64-linux-android \
    /workspace/target/x86_64-linux-android \
    2>/dev/null || true
fi

exit "${status}"
