# App Use OS provider

`sinomo-app-use-module` is an independently built native dylib under the generic
Sinomo module ABI. It provides `module.app-use.os` / `sinomo.app-use.os.v3`, with
the typed `sinomo.app-use.os` service interface version `2.0.0`.

The module owns native target enumeration, identity checks, capture and input.
Windows uses WGC/D3D11, background window messages, SendInput and conditional
gamepad injection. Linux uses local X11 with XRes, Composite, Damage, XFixes and
XTest, or Wayland portals with PipeWire capture and approved raw input. Android
uses a same-UID local OS adapter with MediaProjection and a
user-enabled AccessibilityService. Other platforms report unsupported capabilities.
It does not own an encoder, network stack, agent runtime or alternate toolset.

The linked App Use service in `sinomo-actions` supplies the single skill/tool,
actor-bound sessions, generic events and the existing media transport. It calls
this provider through `ActiveNativeComponent`; `sinomo-device` contains only
shared contracts and validation. No host crate depends on this package.

Build explicitly (native modules are not default workspace members):

```powershell
cargo build -p sinomo-app-use-module
sinomo module native-dylib activate --module module.app-use.os --interface sinomo.app-use.os.v3 --cache-key module/desktop/app-use target/debug/sinomo_app_use_module.dll
```

Installation does not grant input or capture consent. The provider requires a
host-bound permission-store identity and rechecks node-local grants for each OS
effect. Background mode is default; raw mode must be explicitly selected on a
monitor with its own exposure grants. No implicit escalation or input replay.

Pixel capture uses a bounded latest-frame mailbox. Capture requests accept an
optional `resolution: {width, height}` bounding box and `pixel_format`. Outputs
preserve aspect ratio, never upscale, and report actual and native dimensions
separately. Native dimensions remain the coordinate space for input. An omitted
pixel format selects the backend's native format; capabilities enumerate supported
formats. Version 2 requires explicit format and native geometry so an older host
cannot interpret RGBA bytes as BGRA. Update the host and provider together.

Samples cross the ABI output callback as a single bounded SOBJ byte blob. Shared
storage is retained through provider and host frame wrappers. Pixel samples enter
the host's media encoder; native encoded samples enter the ordered video transport. The generic invocation response carries only a
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

## Capture output selection

The `/app-use/capture` request accepts `resolution: {width, height}` and
`capture_path: "auto" | "pixels" | "surface"`. Without explicit bounds,
`encoding.max_dimension` supplies square bounds. Explicit bounds take precedence.
H264 output uses even dimensions and the transport's 1920-pixel edge limit.
`auto` uses native encoded capture when supported for H264; `pixels` keeps the
raw capture and media-encoder path; `surface` requires native encoded capture.
Application overview composition requires pixels. Frame metadata includes
`capture_format`, actual `width`/`height`, and native `source_width`/`source_height`.

Android captures into one GPU texture and renders per-consumer output surfaces,
so concurrent consumers can request different resolutions. Raw RGBA is scaled
before CPU readback. H264 renders directly into a Surface supplied by the app's
media encoder adapter; the OS bridge owns capture and scaling, while the media
adapter owns codec configuration and draining. Encoded output is demand-driven
and ordered: a latest-frame replacement must never discard a reference frame.
The same local consent, target validation, lease and revocation checks apply to
both paths. A display geometry change ends the share and requires fresh consent.

## Windows application and shell surfaces

Application sessions are rooted in a verified PID/process-creation identity.
Out-of-context WinEvent hooks invalidate observed window references and mark
window inventories dirty. The hooks run on owned message-loop threads and are
removed before those threads or the DLL are released. Child controls retain
parent relationships; cross-process owner links are discovery evidence and do
not inherit process-wide authority.

Application and per-surface grants are checked at every input/capture boundary.
Legacy exact-window grants retain their original API scope. Native handles stay
bound to one surface reference; closing an application closes its native handles
and observers. Application discovery, window capture and semantic operations
share the existing host/provider protocol without adding a second toolset.

Visible tray and app-menu discovery uses a bounded UIA worker with automatic
focus disabled. Tray controls are not attributed to an app by label. Semantic
invocation requires both explicit desktop-interaction opt-in and local grants.
Overflow is never opened implicitly. Unknown/unsupported context-menu behavior
is reported, and no raw input is synthesized as a fallback. Timed-out requests
are not replayed; failed worker shutdown retains the DLL mapping through the
normal native-module quarantine mechanism.

The shared host media path builds bounded application overviews and selected
window views. Per-frame identities and tile metadata keep observation selection
separate from input targeting. Provider-specific UIA objects and WinEvent hooks
never escape the native module.

## Linux X11

The Linux provider builds with bundled PipeWire/SPA ABI headers and ordinary C
and libclang build tools. Loading or installing the provider does not require
GUI client libraries. X11 uses its existing Rust protocol client. Wayland capture
loads the optional `libpipewire-0.3.so.0` API only when needed; missing or
incompatible symbols make capture unavailable without preventing headless use.
Portal-based input remains independently available when its portal supports it.

Run the daemon in the user's local X11 session with its `DISPLAY` and
`XAUTHORITY`. The target monitor is the X screen's complete virtual desktop.
Windows use XRes connection-owner PIDs and `/proc` process start times; a
client-supplied `_NET_WM_PID` is never authority. Background frames contain the
client area, including when another window occludes it. Background input uses
synthetic X events; applications may ignore them. Raw input uses XTest and may
affect the user's focus and pointer. Text must exist in the current keyboard
map; the provider never rewrites that map. Raw Set-1 keys assume Xorg's evdev
keycode mapping. Gamepads and remote X servers are unsupported. Wayland sessions,
including those with an Xwayland `DISPLAY`, select the portal backend below.

```sh
cargo build -p sinomo-app-use-module
sinomo module native-dylib activate --module module.app-use.os --interface sinomo.app-use.os.v3 --cache-key module/desktop/app-use target/debug/libsinomo_app_use_module.so
cargo test -p sinomo-actions --test app_use_native_provider -- --ignored
xvfb-run cargo test -p sinomo-app-use-module x11_background_pixels_input_revocation_and_cleanup -- --ignored
```

Use an isolated X server for the native test. It creates and destroys its own
window, checks pixels and input delivery, and checks revocation and cleanup.

## Linux Wayland

Run the provider in the desktop user's session with its `WAYLAND_DISPLAY` (or
`XDG_SESSION_TYPE=wayland`), session D-Bus and `XDG_RUNTIME_DIR`. The desktop must
provide its ScreenCast/RemoteDesktop portal backend for these optional functions.
Sinomo installation does not install or configure a GUI stack on a server.
Capture uses native PipeWire buffers from the [ScreenCast portal's restricted
descriptor](https://flatpak.github.io/xdg-desktop-portal/docs/doc-org.freedesktop.portal.ScreenCast.html);
input uses the [RemoteDesktop portal's keyboard/pointer notification
methods](https://flatpak.github.io/xdg-desktop-portal/docs/doc-org.freedesktop.portal.RemoteDesktop.html).
There is no Xwayland, direct compositor-protocol or privileged-device fallback.
Backends providing only ScreenCast can capture but cannot inject input.

Wayland supports **explicit raw mode**. After node-local Remote Management
consent, `targets` with `mode:raw` starts the local portal chooser asynchronously
and initially returns an empty list. The user approves a single monitor and
available input devices. Repeat target discovery after approval, then expose
that exact returned resource for `capture` and/or `input` using the normal local
permission controls. Portal approval never writes those grants. Background
requests fail explicitly and never open a raw session automatically.

The chooser and unclaimed approved target expire after 120 seconds. Existing
input/capture leases share one portal session; closing the final lease, revoking
host consent, stopping sharing, losing the portal owner, resizing a captured
stream or unloading the provider invalidates the target. Re-enumerate and grant
the new resource after approval. The provider does not persist portal restore
tokens. ScreenCast v6 sessions require a stable PipeWire serial; older sessions
use their restricted session node ID and never reconnect a removed stream.

Target bounds are **portal-advertised stream coordinates**, with origin `(0,0)`.
Scale raw video coordinates by `target_width / frame_width` and
`target_height / frame_height` before sending `move`; this handles compositor
scaling without assuming desktop logical or physical pixel units. Do not blindly
divide coordinates by the display scale: use the returned bounds. Relative motion
uses the portal's relative units. Raw Set-1 keys are translated to evdev codes; text uses Unicode keysyms.
Discrete wheel deltas must be multiples of 120. Background window messages,
`move_in_frame`, gamepads and cursor overlays are unsupported; capture uses a
hidden cursor for the complete portal session. A failed/uncertain input is never
replayed; a failed D-Bus input acknowledgement retires the session.

Run isolated protocol and native-buffer checks with PipeWire tools/plugins
installed:

```sh
python3 modules/desktop/app-use/tests/run_wayland.py
```

The runner creates a private D-Bus portal fixture and real PipeWire video source,
without contacting the user's desktop or discovering hardware. It checks fast
portal replies, approval/denial, device grants, input mapping, held-input
cleanup, owner loss, pending-request cancellation, real native pixels and
capture revocation. Live compositor validation additionally needs the actual
desktop chooser, a disposable application, scaled monitors, OS stop/resize, and
observed input effects; the fixture does not establish compositor compatibility.
The [Docker compositor lane](tests/compositor/README.md) exercises a real KWin
session, KDE portals, PipeWire, and a native Wayland app without a host GUI stack.

## Android

Build the app with `scripts/android-dev.sh build` or the Docker Android build
wrapper. Both run [the APK packager](android/package.py) after cargo-apk: it
merges the [OS service declarations](android/manifest.xml), compiles the Java
adapter, aligns the APK, and verifies that re-signing preserves the original
certificate. Direct `cargo apk build` does not package these services. The
packager needs the existing signing keystore and password environment variable;
it never generates a replacement signing identity.

Build `sinomo-app-use-module` for `aarch64-linux-android` with the Android NDK
and `--profile android-debug` (or `android-debug-small`) so its pixel conversion
uses the Android media optimization settings. Activate its `.so` through the ordinary native-module lifecycle on the Android
node. The Android embedded profile exposes the same App Use tools as desktop;
there is no linked provider fallback.

On the device, open Sinomo and enable Remote Management in local Settings.
The **Sinomo App Use** launcher entry separates persistent Accessibility access
from screen sharing. Enable Accessibility in Android Settings, then allow
observation and optional control for its persistent display resource. App switches
and agent disconnects do not revoke that grant. Disable Accessibility or revoke
the local resource grant to remove access.

Accessibility supplies bounded semantic observations, observation-bound actions,
and screenshots (Android 11+). Password content is redacted; disabled/protected
nodes and stale observations fail explicitly. Screenshots obey Android's rate
limit and are not a high-frame-rate video source. Raw touch/text input also uses
the Accessibility target, independently of projection. `move` positions a virtual
touch point; left-button down/move/up submits a finite gesture on release.
Closing or releasing an input lease discards an unfinished gesture. Wheel events
submit bounded swipes; text edits a focused non-password editor. Supported raw
keys are Escape/Back (1), Backspace (14) and Enter (28). Background input,
gamepads and unsupported keys fail explicitly. A coordinate lease is invalidated
by display geometry changes; it does not stop screen sharing.

MediaProjection belongs to screen video. The local sharing activity requests the
whole default display and grants viewing/stop access to that share. The generic
`/media/video/devices`, `/media/video/capture` and `/media/video/control` routes
expose it alongside cameras. OS permission and Sinomo resource exposure remain
separate. The same-UID OS bridge remains available when either Accessibility or
projection is active; disabling Accessibility does not terminate projection.

An approved projection remains active without subscribers, across consumer
reconnections and while other apps are foreground. It ends only on an explicit
user/authorized-agent stop, Android interruption, or fatal error. It is non-sticky
and cannot resume an invalidated OS token after process death. Geometry changes
resize the existing virtual display and notify consumers. Existing encoder
surfaces retain their dimensions and letterbox the resized source; frame metadata
reports native dimensions and the content rectangle. Protected content remains
subject to Android capture restrictions.

The private Android socket capture protocol is version 3; upgrade the APK and
native provider together. It keeps the existing bounded native frame delivery
contract, and never opens a separate network path.

For device validation, check local consent denial, capture of a moving fixture,
touch/text effects, screen-share stop, accessibility revocation, rotation,
socket disconnect, and provider removal. Compile/package checks alone do not
establish on-device capture or input behavior.
Use the [device fixture and native ABI probe](android/fixture/README.md) to check
observable effects without suppressing the accessibility service under test.
