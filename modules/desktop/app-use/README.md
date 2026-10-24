# App Use OS provider

`sinomo-app-use-module` is an independently built native dylib under the generic
Sinomo module ABI. It provides `module.app-use.os` / `sinomo.app-use.os.v1`, with
the typed `sinomo.app-use.os` service interface version `1.0.0`.

The module owns native target enumeration, identity checks, capture and input.
Windows uses WGC/D3D11, background window messages, SendInput and conditional
gamepad injection. Linux uses local X11 with XRes, Composite, Damage, XFixes and
XTest. Android uses a same-UID local OS adapter with MediaProjection and a
user-enabled AccessibilityService. Other platforms report unsupported capabilities.
It does not own an encoder, network stack, agent runtime or alternate toolset.

The linked App Use service in `sinomo-actions` supplies the single skill/tool,
actor-bound sessions, generic events and the existing media transport. It calls
this provider through `ActiveNativeComponent`; `sinomo-device` contains only
shared contracts and validation. No host crate depends on this package.

Build explicitly (native modules are not default workspace members):

```powershell
cargo build -p sinomo-app-use-module
sinomo module native-dylib activate --module module.app-use.os --interface sinomo.app-use.os.v1 --cache-key module/desktop/app-use target/debug/sinomo_app_use_module.dll
```

Installation does not grant input or capture consent. The provider requires a
host-bound permission-store identity and rechecks node-local grants for each OS
effect. Background mode is default; raw mode must be explicitly selected on a
monitor with its own exposure grants. No implicit escalation or input replay.

Native capture runs continuously in a bounded latest-frame mailbox. Raw samples
cross the stable ABI output callback as a single bounded SOBJ byte blob and enter
the host's shared media encoder. The generic invocation response carries only a
small acknowledgement, avoiding nested byte-sequence serialization. Invocation
callbacks are serialized and never retained by OS workers. Shutdown releases
held input, closes capture and joins workers before releasing code mappings.

Run artifact-backed verification after building:

```powershell
cargo test -p sinomo-actions --test app_use_native_provider -- --ignored
cargo test -p sinomo-actions --test app_use_native_windows -- --ignored --test-threads=1
cargo test -p sinomo-actions --test desktop_media_windows -- --ignored
```

The latter two require an idle interactive desktop. They create owned background
windows and verify actual pixels/messages, DPI handling, resize, revocation and
cleanup without foreground/pointer changes. `SINOMO_APP_USE_MODULE` can select a
specific built DLL; test installation is confined to temporary data directories.
See the [desktop agent harness](../../../docs/developer/testing.md#live-desktop-agent-verification-windows)
for recorded real-agent tests of both modes through the same tools.

## Linux

Run the daemon in the user's local X11 session with its `DISPLAY` and
`XAUTHORITY`. The target monitor is the X screen's complete virtual desktop.
Windows use XRes connection-owner PIDs and `/proc` process start times; a
client-supplied `_NET_WM_PID` is never authority. Background frames contain the
client area, including when another window occludes it. Background input uses
synthetic X events; applications may ignore them. Raw input uses XTest and may
affect the user's focus and pointer. Text must exist in the current keyboard
map; the provider never rewrites that map. Raw Set-1 keys assume Xorg's evdev
keycode mapping. Gamepads, remote X servers, native Wayland and Xwayland sessions
are explicitly unsupported.

```sh
cargo build -p sinomo-app-use-module
sinomo module native-dylib activate --module module.app-use.os --interface sinomo.app-use.os.v1 --cache-key module/desktop/app-use target/debug/libsinomo_app_use_module.so
cargo test -p sinomo-actions --test app_use_native_provider -- --ignored
xvfb-run cargo test -p sinomo-app-use-module x11_background_pixels_input_revocation_and_cleanup -- --ignored
```

Use an isolated X server for the native test. It creates and destroys its own
window, checks pixels and input delivery, and checks revocation and cleanup.

## Android

Build the app with `scripts/android-dev.sh build` or the Docker Android build
wrapper. Both run [the APK packager](android/package.py) after cargo-apk: it
merges the [OS service declarations](android/manifest.xml), compiles the Java
adapter, aligns the APK, and verifies that re-signing preserves the original
certificate. Direct `cargo apk build` does not package these services. The
packager needs the existing signing keystore and password environment variable;
it never generates a replacement signing identity.

Build `sinomo-app-use-module` for `aarch64-linux-android` with the Android NDK and
activate its `.so` through the ordinary native-module lifecycle on the Android
node. The Android embedded profile exposes the same App Use tools as desktop;
there is no linked provider fallback.

On the device, open Sinomo once and enable Remote Management in local Settings.
Open the **Sinomo App Use** launcher entry, enable its accessibility service,
request screen sharing, then grant capture and optionally touch/text input to
that share. The Android OS dialog must authorize the whole default display.
The service is non-sticky and does not restore grants or capture after process
death. An unattached projection expires after 30 seconds; closing the last
input/capture session ends sharing. Stop sharing or disable accessibility to revoke
access. A new share or rotation requires re-enumeration and a fresh exact-display
grant. Protected content remains subject to Android's capture restrictions.

Android supports **explicit raw mode only**. `move` sets a virtual touch point;
left-button down/move/up submits a finite touch gesture on release. Closing or
releasing the lease discards an unfinished gesture. Wheel events submit bounded
swipes. Text inserts at the selection of a focused accessible non-password
editor. Supported raw keys are Escape/Back (1), Backspace (14) and Enter (28);
other keys, background input and gamepads fail explicitly. Android does not add
a synthetic pointer to captured frames; `include_cursor` has no effect there.
MediaProjection pushes BGRA samples into a bounded latest-frame slot; encoding
and routed video remain in the existing host path.

For device validation, check local consent denial, capture of a moving fixture,
touch/text effects, screen-share stop, accessibility revocation, rotation,
socket disconnect, and provider removal. Compile/package checks alone do not
establish on-device capture or input behavior.
Use the [device fixture and native ABI probe](android/fixture/README.md) to check
observable effects without suppressing the accessibility service under test.
