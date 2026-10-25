#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$HOME" "$XDG_RUNTIME_DIR"
chmod 700 "$XDG_RUNTIME_DIR"
exec dbus-run-session -- bash /opt/wayland-test/session.sh
