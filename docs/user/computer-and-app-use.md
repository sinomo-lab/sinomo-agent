# Computer Use and App Use

Sinomo provides two independently enabled native Rust modules. The Windows
backend captures continuous compositor video and injects input without installing
or loading a driver. Other operating systems return explicit unsupported
capabilities. Shadow daemons and mobile embedded profiles omit these modules.

| Module | Capture | Input |
|---|---|---|
| Computer Use (`module.computer-use`) | One monitor via Windows Graphics Capture/D3D11 | Scan-code keyboard, Unicode text, absolute/relative mouse, buttons, wheels, conditional native gamepad injection |
| App Use (`module.app-use`) | One HWND via Windows Graphics Capture/D3D11, including occluded windows | Posted keyboard, character and mouse messages, without changing focus or moving the user's cursor |

Windows desktop input uses `SendInput`; app input uses `PostMessageW`. UIPI and
secure-desktop restrictions still apply. Gamepad injection uses `InputInjector`,
whose `inputInjectionBrokered` restricted capability must be available to the
host. Ordinary unpackaged hosts may receive an unsupported error. Sinomo does
not install virtual-controller drivers or replace gamepads with key mappings.
The first gamepad input also checks that initialization exposes a new native
controller. Some hosts accept the initialization API without creating a usable
endpoint; Sinomo reports that condition as unsupported instead of claiming that
input was applied. Target-application reception still requires verification.

App input is best effort. Raw Input, DirectInput, hardware polling, foreground
checks, custom control routing and global modifier polling can prevent useful
behavior. Use `children` to discover classic child controls and open an input
lease for the exact control. Capture the top-level window. Minimized, protected,
suspended or closed windows may stop producing video; capture never restores or
activates them. Queued messages do not prove that an application handled them.

## Enablement and local consent

Modules are enabled by default on standard main/embedded hosts. Existing
Settings → Modules controls and CLI toggles apply:

```sh
sinomo module native-rust disable module.computer-use
sinomo module native-rust enable module.app-use
```

Enablement grants no host access. First allow `remote-management` locally, then
use the module's `targets` action to obtain exact resource identifiers. Grant
only the required operations on the receiving node:

```sh
sinomo node permissions allow remote-management
sinomo node permissions expose "RESOURCE_FROM_TARGETS" capture
sinomo node permissions expose "RESOURCE_FROM_TARGETS" input
```

These are operator actions, not remote module operations. Neither module edits
permissions. Input and video reads recheck local consent; revocation stops video
and releases this session's held input. Window targets include PID, thread ID
and process creation time. Re-enumerate after window recreation.

## Agent tools and video consumers

The `computer-use` and `app-use` skills are embedded module resources, available
independently of an agent package. The normal module-tool projection exposes
`/computer-use/control` and `/app-use/control` as JSON tools. Requests go under
the tool's `input` property:

```json
{"action":"capabilities"}
{"action":"targets"}
{"action":"open","target":{"kind":"monitor","device_name":"\\\\.\\DISPLAY1"},"input":true,"capture":true,"lease_ms":60000}
{"action":"input","session_id":"RETURNED_ID","events":[{"kind":"move","x":100,"y":100},{"kind":"button","button":"left","down":true},{"kind":"button","button":"left","down":false}]}
{"action":"close","session_id":"RETURNED_ID"}
```

Use the exact enumerated target rather than copying the example display name.
For App Use, `{"action":"children","target":WINDOW_TARGET}` returns child HWNDs.
The skills document all event shapes. Computer Use keys are Set-1 scan codes;
App Use keys are Windows virtual-key codes. Mouse coordinates are physical
virtual-desktop pixels or window client pixels respectively. Input batches are
bounded to 256 events and text to 4096 UTF-16 units. Do not retry an ambiguous
input batch automatically; a failure may follow partially applied input.

Targets expose physical `bounds` and window `client_bounds`. Video records expose
both source and encoded dimensions. Undo encoder scaling before input; for app
input also subtract the non-client offset (`client_bounds` origin minus `bounds`
origin). Refresh target geometry after moving or resizing a window.

An open response includes a media stream descriptor when capture is requested.
It reserves a bounded session; native capture starts when a consumer attaches.
Call the descriptor's route in category `media`, with:

```json
{"session_id":"RETURNED_ID","frames_per_second":30,"include_cursor":false}
```

`/computer-use/capture` and `/app-use/capture` return one long-lived streamed
object-RPC body. They are media operations rather than JSON tools: the generic
tool adapter must not materialize video into JSON. Routing, identity, framing
and transfer use Sinomo's existing object-RPC media path. Consumers must call
`cancel_on_drop()` on an abandoned body.

`sinomo_video::stream::read_frame` decodes bounded timestamped records. Send
these directly to `RealtimeSession::send_encoded_video_frame`, or use
`forward_to_realtime`/`RemoteVideoSource`. Encoding uses the same
`sinomo_lm::encode_realtime_video_frame` path as cameras. No separate encoder or
provider protocol lives in either control module. The current model path uses
JPEG/PNG video frames at a configured cadence, rather than an H.264/RTP track.
Native capture remains continuous at its own cadence. Raw and encoded queues
are bounded; slow consumers skip frames instead of accumulating latency.

The realtime CLI can attach to a lease opened by the same authenticated node:

```sh
sinomo voice call --computer-stream SESSION_ID --no-audio --call-ms 30000
sinomo voice call --app-stream SESSION_ID --video-node NODE_ID --native-mic --call-ms 30000
```

Use existing provider/model arguments for a provider supporting video. The
caller owns provider authentication and transmission. Streams cannot attach to
another authenticated caller's lease. Desktop stream flags and native camera
selection are mutually exclusive.

## Lifetime and verification

Sessions expire after their requested lease (1–300 seconds), allow one video
consumer, and are limited to eight per module. Input leases additionally expire
after 30 seconds idle. Only one computer input owner is admitted, or one app
input owner per exact window. Close releases owned keys/buttons and neutralizes
owned gamepad state. Disconnect stops capture. Unload withdraws routes, cancels
sessions, joins workers and releases devices. Input is not durably queued or
replayed.

The opt-in Windows test creates an owned no-activate window, verifies changing
video pixels and message delivery, checks foreground/cursor preservation, and
revokes consent while a key is held:

```sh
cargo test -p sinomo-device --test desktop_windows -- --ignored --nocapture
cargo test -p sinomo-actions --test desktop_media_windows -- --ignored --nocapture
```

The second test exercises component dispatch, live capture, shared encoding,
RPC media framing, resize and teardown together. Neither test injects raw input
into the user's foreground application. Gamepad
availability and compatibility with any third-party application remain dependent
on the OS capability and the app's input design.

For real model-driven acceptance, the opt-in `desktop-e2e` build adds owned-window
tasks with a separate video and tool trace for each case. See
[live desktop agent verification](../developer/testing.md#live-desktop-agent-verification-windows)
for the launcher, credential stop behavior and interactive-desktop requirement.
It starts a minimal agent/component host without requiring the real cluster.

Platform references: [Windows Graphics Capture](https://learn.microsoft.com/en-us/uwp/api/windows.graphics.capture.direct3d11captureframepool.createfreethreaded),
[SendInput](https://learn.microsoft.com/en-us/windows/win32/api/winuser/nf-winuser-sendinput),
[PostMessageW](https://learn.microsoft.com/en-us/windows/win32/api/winuser/nf-winuser-postmessagew),
[InputInjector](https://learn.microsoft.com/en-us/uwp/api/windows.ui.input.preview.injection.inputinjector).
