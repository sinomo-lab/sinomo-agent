# App Use

App Use provides one skill and one control tool. Prefer semantic Accessibility
observation and actions when the platform advertises them. The agent explicitly
chooses observations and can combine them with screenshots or video. Coordinate
input retains two explicit modes.
Background OS window messages are preferred: they do not move the user's pointer,
take keyboard focus, or require the app to be foreground. The agent can select
raw desktop input when background control is unsuitable or unavailable on the
platform. There is no automatic
fallback or replay of a failed batch.

| Mode | Target and capture | Input |
|---|---|---|
| `background` (default) | Exact window; continuous compositor capture even while occluded | Posted virtual-key, character and mouse messages |
| `raw` (explicit) | Exact monitor; continuous compositor capture | Scan-code keyboard, Unicode text, absolute/relative mouse, buttons, wheels and conditional gamepad injection |

Native capture and input are implemented in the independently built
[App Use OS provider](../../modules/desktop/app-use/README.md).

| Platform | Available behavior |
|---|---|
| Windows | Background and raw modes using Windows Graphics Capture/D3D11, PostMessageW, SendInput and conditional InputInjector |
| Linux X11 | Background client-window capture and synthetic events; raw virtual-desktop capture and XTest input; no gamepads |
| Linux Wayland | Explicit raw mode with locally approved portal capture/input; ScreenCast-only backends provide capture without input |
| Android | Persistent Accessibility observations, screenshots and semantic actions; explicit raw touch/text input; independent MediaProjection screen video |
| Other systems | Explicitly unsupported |

Common actions, events, media encoding and transmission remain in the host
runtime. Main and embedded/mobile profiles expose App Use; shadow profiles omit it.

## Installation and local consent

Build and activate the OS provider with the ordinary native-module lifecycle:

```powershell
cargo build -p sinomo-app-use-module
sinomo module native-dylib activate --module module.app-use.os --interface sinomo.app-use.os.v3 --cache-key module/desktop/app-use target/debug/sinomo_app_use_module.dll
```

Upgrade the app/daemon and provider together; their capture interface versions
must match.

On Linux the artifact is `target/debug/libsinomo_app_use_module.so`. The daemon
needs access to the user's desktop session. X11 uses `DISPLAY` and `XAUTHORITY`;
Wayland uses the session bus and desktop portals. The Linux provider requires
the PipeWire runtime library. See [Wayland setup](../../modules/desktop/app-use/README.md#linux-wayland)
for build dependencies and portal prerequisites.
See the provider's [Android setup](../../modules/desktop/app-use/README.md#android)
for APK packaging and native artifact activation. On Android, open Sinomo and
enable Remote Management locally, then use the **Sinomo App Use** launcher entry
to enable Accessibility and allow persistent observation with optional control.
These grants survive agent sessions and app switches. Disabling Accessibility in
Android Settings or revoking App Use access locally removes access; neither
changes a separate screen-sharing session. Android OS consent does not enable
Remote Management automatically. App Use input and semantic actions cannot
operate Sinomo's own consent controls or approve its screen-sharing dialog.

For continuous screen video, start screen sharing separately and grant viewing
and stop access to that share. The OS dialog authorizes the whole default display.
Sharing continues with no viewers, after agent disconnects, and while other apps
are foreground. Only an explicit user/authorized-agent stop, Android interruption,
or fatal capture error ends it. Android requires a fresh OS grant after a
projection ends; Sinomo never caches or replays that token.

The linked `module.app-use` host service exposes the shared tools and media
routes. Its existing Settings/CLI enablement controls apply:

```sh
sinomo module native-rust disable module.app-use
sinomo module native-rust enable module.app-use
```

The former `module.computer-use` registration, separate skill and routes are
retired. Both modes now use App Use. A missing or removed native provider fails
closed; the host has no linked OS backend fallback. Provider replacement or
removal ends existing sessions; open new sessions after activation completes.

Activation grants no host access. Operator-owned local consent must permit
remote management and each exact window/display operation:

```sh
sinomo node permissions allow remote-management
sinomo node permissions expose "RESOURCE_FROM_TARGETS" capture
sinomo node permissions expose "RESOURCE_FROM_TARGETS" input
```

Window exposure never grants monitor/raw input. The native provider binds the
permission store through a live host callback and rechecks consent at the OS
boundary. Revocation stops video and releases held inputs. Neither tool changes
permissions. Windows identities contain handle, PID, thread ID and process
creation time; re-enumerate after recreation.
Linux identities use the X11 window ID, the server-reported local owner PID and
process start time. Android Accessibility uses a persistent display resource;
projection identities belong to one explicitly approved share. Rotation updates
projection geometry without changing its identity or ending sharing. Coordinate
input leases reject stale geometry: observe again and open a new input lease.
Wayland identities belong to one approved portal session. Request raw targets,
approve the local chooser, then repeat discovery to obtain the exact resource
to expose. Discovery initially returns an empty list while approval is pending.
Closing the final lease or stopping sharing invalidates the target.

## Android observation and screen video

When `capabilities.accessibility` is true, enumerate raw targets and prefer
`android:accessibility:display:0`. Obtain bounded UI nodes with:

```json
{"action":"observe","target":{"kind":"monitor","device_name":"android:accessibility:display:0"},"max_nodes":64}
```

Use the returned `snapshot_id` and node `id` in `act`, with `operation.kind`
`click`, `long_click`, `focus`, `scroll_forward`, `scroll_backward`, or `set_text`
(the latter includes `text`). Snapshots expire, are consumed by an action, and
cannot retarget a changed node. Observe after an action to verify its result;
never replay an uncertain effect. Password text is redacted and protected nodes
cannot be edited. Screenshot capture on this target requires Android 11 or newer
and respects Android's screenshot rate limit. It needs no MediaProjection grant.

The generic media API lists cameras and active screen shares through
`/media/video/devices`; the agent tool's `video_sources` action uses the same
discovery. Each entry carries a typed `source`. Pass it unchanged to
`/media/video/capture`, for example:

```json
{"source":{"kind":"screen","target":{"kind":"monitor","device_name":"TARGET_FROM_VIDEO_DEVICES"}},"lease_ms":60000,"frames_per_second":10,"resolution":{"width":640,"height":640},"transport":"auto","encoding":{"frame_format":"jpeg","max_frame_rate_hz":10,"encoding_quality":85}}
```

An omitted source retains default-camera capture; `{"kind":"camera"}` selects
it explicitly. Camera requests retain `duration_ms`; screen requests use
`lease_ms` so older camera-only servers reject them before accessing a device.
Both sources share transport negotiation, framing, decoding and stream
cancellation. Closing a reader ends only its finite capture lease.
`/media/video/control` accepts `{"action":"stop","source":SOURCE}` for a screen
share with a separate local `stop` grant. The agent tool's `stop_video` action
accepts the same `source` and uses that media-owned operation. Neither path
starts an OS consent dialog.
The agent can keep semantic observations, screenshots and video open together;
video is an optional visual source, not a prerequisite for Android control.

Screen and App Use streams advertise version 3. Consumers must understand
geometry notifications and content bounds; older consumers must reject this
version. Camera image/H.264 streams retain versions 1/2.

Projection streams preserve their negotiated output dimensions through source
resizing. Each frame reports native source dimensions and `content_bounds`
(`[left,top,right,bottom]`) for its letterboxed content. Map positions within that
rectangle to native coordinates. `notifications` carries
`media.video.geometry.changed` on the initial frame and subsequent geometry
changes. Latest-frame consumers coalesce pending changes while retaining the
original source sequence and timestamp. Do not use a cached image or old input
lease after a geometry change.

## Unified agent tools

The embedded `app-use` skill documents both modes. The usual module projection
exposes `/app-use/control` as one JSON tool, with requests under `input`:

```json
{"action":"capabilities"}
{"action":"targets"}
{"action":"targets","mode":"raw"}
{"action":"input","session_id":"RETURNED_ID","events":[{"kind":"text","text":"hello"}]}
{"action":"close","session_id":"RETURNED_ID"}
```

Open with the exact discovered target, `input:true` and/or `capture:true`, a
bounded `lease_ms`, and `mode:background` or `mode:raw`. Omitting mode means
background, including when a monitor was supplied: it will be rejected rather
than silently granting raw control. Close the background input lease before
switching to raw. A raw session can interrupt the user's keyboard/mouse and
change foreground.

Background `key` codes are Windows virtual keys; raw codes are Set-1 scan codes.
Linux translates supported codes to the current X11 keyboard mapping; text is
limited to mapped characters. Android supports raw Escape/Back (1), Backspace
(14) and Enter (28); text edits the focused accessible non-password field.
On Android, left-button down/move/up becomes a finite touch gesture on release,
and wheel events become swipes. Closing or releasing the lease discards an
unfinished gesture. There is no held hardware keyboard or gamepad endpoint.
Background `move` uses physical client pixels; raw uses physical virtual-desktop
pixels on Windows/X11. Wayland raw input instead uses the portal target's
advertised stream-local bounds; scale source video pixels to those bounds before
input. Prefer background `move_in_frame` for image grounding: provide normalized
fractions of the full image plus its current source dimensions. The provider
handles chrome and DPI; stale sizes after a resize are rejected. Resize in one
batch and observe a fresh frame before subsequent input. Optional coordinate
rulers can be enabled through the shared encoder.

`children` enumerates classic child windows for separate input leases. Do not
apply top-level capture coordinates to child input. Raw-input/DirectInput apps,
hardware polling, foreground checks, custom routing and UIPI may prevent useful
background behavior. Minimized/protected/suspended windows may stop producing
frames. Capture never restores or activates them. OS acceptance alone does not
prove a successful click or application state change.

Gamepad injection is available only in raw mode and requires native OS support.
The provider verifies that initialization exposes a controller endpoint; an API
success without an endpoint is reported as unsupported. It installs no virtual
controller driver and never substitutes keyboard input.

## Applications with multiple windows

On Windows, the `capabilities` result includes a `surfaces` object describing
application sessions, window events and semantic tray/menu support. Existing
single-window and raw operations retain their original scope on every platform.

`applications` returns running process instances with an executable label,
process identity and application resource ID. Use the returned identity with
`open_application`. An application session follows that process lifetime, so
closing its main window does not close a tray-only application session.

Grant `observe` on the application resource to follow its windows. Optional
application `input` and `capture` grants cover windows created by that same
process instance. These are explicit additional grants; an existing window
exposure never becomes an application-wide grant. Related windows in other
processes can be discovered through owner relationships, but require their own
surface grants. Matching titles, executable names and owner links are not control
authority. A process restart requires fresh identity and consent.

Use `surfaces` with the application session ID. Results distinguish top-level
windows and child controls and include owner/parent relationships, visibility,
enabled state and whether an owned popup blocks its owner. Observations are
coalesced; the current surface list is authoritative for selecting a target.
The local event bus publishes `app.window.added`, `app.window.changed`,
`app.window.removed` and `app.session.closed` observations with session/actor
provenance. Do not treat them as a complete historical record of transient UI.

Use `open_window` with the application session ID and the current opaque
`surface_id`. Every input on that window session includes the same surface ID;
references are not reused after an observed destruction/recreation. Per-surface
resource IDs are scoped to that observation lifetime. Existing window grants
continue to work through the original `open` API and are not reinterpreted as
surface-lifetime grants. Input to a disabled application window is rejected so
the agent can select its dialog explicitly.

`open_overview` opens a bounded application media view. `select_window` changes
that view between thumbnails (`surface_id:null`) and a selected full-resolution
window. It never changes any input session's target. Frames carry capture
identity; overview tiles carry their surface IDs, dimensions and timestamps.
Omitted windows are explicit. An `unavailable` identity describes a generated
view-status frame, not pixels captured from a window. Refresh the surface list
and explicitly select another window when the selected one disappears.

## Tray icons and menus

`tray_items` lists exposed visible Windows UI Automation controls. It does not
open the overflow flyout. Tray controls are shell-owned, so their application
ownership is reported as unknown rather than inferred from a tooltip. Their
resource IDs and references are tied to the observed shell element; rediscover
after disappearance, expiry or shell recreation.

`invoke_tray` accepts an item ID and `operation:invoke` or `context_menu`.
`menu_items` lists menu items for a specific application window; use
`invoke_menu_item` with that same application/window identity. A nullable
context-menu capability means provider support is unverified; an attempted
operation can report unsupported. UI Automation automatic focus is disabled,
but the shell or application may activate UI in response to an action.

These semantic actions require explicit `allow_desktop_interaction:true`.
Tray resources additionally require local `invoke` and `desktop-interaction`
grants. Menu invocation requires `input` and `desktop-interaction` grants for
the application or exact surface. Application grants never authorize Explorer
or the entire tray. For example, use the resource returned by discovery:

```sh
sinomo node permissions expose "TRAY_RESOURCE_FROM_DISCOVERY" invoke
sinomo node permissions expose "TRAY_RESOURCE_FROM_DISCOVERY" desktop-interaction
```

Hidden/overflow icons may not exist in the UIA tree until the shell UI is shown.
No background operation silently opens it or injects real input. There is no
Explorer-memory or private callback-message fallback. A timed-out semantic
invocation may have an unknown outcome; observe its result before deciding what
to do next and never replay it automatically. An unresponsive UIA worker cannot
be unloaded while it might still execute provider code.

## Continuous video and cleanup

An open response supplies a stream descriptor when capture is requested.
Attach in category `media` using:

```json
{"session_id":"RETURNED_ID","frames_per_second":30,"include_cursor":false}
```

`/app-use/capture` produces one continuous object-RPC body. The OS DLL keeps a
bounded latest-frame slot and supplies timestamped raw BGRA samples through the
native ABI output callback as bounded SOBJ byte blobs. Sinomo's shared media path
performs resizing/encoding and network transmission. Use
`sinomo_video::stream::VideoStreamReader`, `RemoteVideoSource` or
`forward_to_realtime`; consumers cancel abandoned bodies with `cancel_on_drop()`.
The legacy voice `--computer-stream` option remains a capture-session alias on
this same route; it does not create another skill, tool or OS provider.

Input batches are bounded to 256 events; text to 4096 UTF-16 units. Idle input
leases expire after 30 seconds and all sessions after at most 300 seconds.
Always pair held inputs and close sessions after success or failure. Do not
replay partially applied batches. Host teardown drains media workers, closes
native handles and joins OS workers before unloading the DLL.
