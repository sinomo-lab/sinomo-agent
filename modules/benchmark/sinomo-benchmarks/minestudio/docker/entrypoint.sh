#!/usr/bin/env bash
set -euo pipefail

XVFB_DISPLAY="${DISPLAY:-:99}"
XVFB_SCREEN="${MINESTUDIO_XVFB_SCREEN:-0}"
XVFB_GEOMETRY="${MINESTUDIO_XVFB_GEOMETRY:-1280x720x24}"

if [ "$(id -u)" = "0" ]; then
  HOST_UID="${SINOMO_HOST_UID:-1000}"
  HOST_GID="${SINOMO_HOST_GID:-1000}"
  if ! getent group "${HOST_GID}" >/dev/null 2>&1; then
    groupadd --gid "${HOST_GID}" sinomo-host
  fi
  if ! getent passwd "${HOST_UID}" >/dev/null 2>&1; then
    useradd --uid "${HOST_UID}" --gid "${HOST_GID}" --create-home --shell /bin/bash sinomo-host
  fi
  export HOME="$(getent passwd "${HOST_UID}" | cut -d: -f6)"
  exec gosu "${HOST_UID}:${HOST_GID}" bash "$0" "$@"
fi

mkdir -p /workspace/benchmark-results /tmp/MineStudio /cache/huggingface

if ! pgrep -f "Xvfb ${XVFB_DISPLAY}" >/dev/null 2>&1; then
  Xvfb "${XVFB_DISPLAY}" -screen "${XVFB_SCREEN}" "${XVFB_GEOMETRY}" -ac +extension GLX +render -noreset &
fi

for _ in $(seq 1 50); do
  if xdpyinfo -display "${XVFB_DISPLAY}" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

case "${1:-server}" in
  server)
    exec python /opt/sinomo/minestudio/minestudio_http_server.py
    ;;
  smoke)
    exec python /opt/sinomo/minestudio/minestudio_http_server.py --smoke
    ;;
  shell)
    exec /bin/bash
    ;;
  *)
    exec "$@"
    ;;
esac
