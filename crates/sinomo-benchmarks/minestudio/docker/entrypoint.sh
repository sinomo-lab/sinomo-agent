#!/usr/bin/env bash
set -euo pipefail

XVFB_DISPLAY="${DISPLAY:-:99}"
XVFB_SCREEN="${MINESTUDIO_XVFB_SCREEN:-0}"
XVFB_GEOMETRY="${MINESTUDIO_XVFB_GEOMETRY:-1280x720x24}"

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
