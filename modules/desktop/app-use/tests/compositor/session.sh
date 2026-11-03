#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$XDG_RUNTIME_DIR" "$HOME/.config/xdg-desktop-portal" /out
cat > "$HOME/.config/xdg-desktop-portal/portals.conf" <<'EOF'
[preferred]
default=kde
org.freedesktop.impl.portal.ScreenCast=kde
org.freedesktop.impl.portal.RemoteDesktop=kde
EOF
export WAYLAND_DISPLAY=wayland-test
dbus-update-activation-environment WAYLAND_DISPLAY XDG_RUNTIME_DIR XDG_SESSION_TYPE XDG_CURRENT_DESKTOP QT_LINUX_ACCESSIBILITY_ALWAYS_ON
pipewire > /out/pipewire.log 2>&1 &
wireplumber > /out/wireplumber.log 2>&1 &
kwin_wayland --virtual --width 1280 --height 800 --scale "${SINOMO_TEST_SCALE:-1}" --socket "$WAYLAND_DISPLAY" --no-lockscreen --no-global-shortcuts > /out/kwin.log 2>&1 &
kwin_pid=$!
for attempt in $(seq 1 100); do
  [[ ! -S "$XDG_RUNTIME_DIR/$WAYLAND_DISPLAY" ]] || break
  kill -0 "$kwin_pid"
  sleep 0.1
done
[[ -S "$XDG_RUNTIME_DIR/$WAYLAND_DISPLAY" ]]
export QT_QPA_PLATFORM=wayland
dbus-update-activation-environment QT_QPA_PLATFORM LIBGL_ALWAYS_SOFTWARE GALLIUM_DRIVER QT_QUICK_BACKEND
declare -px DBUS_SESSION_BUS_ADDRESS WAYLAND_DISPLAY XDG_RUNTIME_DIR XDG_SESSION_TYPE XDG_CURRENT_DESKTOP QT_QPA_PLATFORM > /out/session.env
/usr/lib/xdg-desktop-portal-kde > /out/portal-kde.log 2>&1 &
# Activate the frontend through D-Bus once; concurrent manual/automatic startup
# can race ownership of its background monitor and well-known service name.
gdbus call --session --dest org.freedesktop.DBus --object-path /org/freedesktop/DBus \
  --method org.freedesktop.DBus.StartServiceByName org.freedesktop.portal.Desktop 0 \
  > /out/portal-start.log 2>&1
GDK_BACKEND=wayland GSK_RENDERER=cairo python3 /opt/wayland-test/fixture.py > /out/fixture.log 2>&1 &
printf 'ready\n' > /out/session-ready
wait "$kwin_pid"
