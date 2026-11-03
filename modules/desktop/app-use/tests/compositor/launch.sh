#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$HOME" "$XDG_RUNTIME_DIR"
exec dbus-run-session -- bash /opt/wayland-test/session.sh
