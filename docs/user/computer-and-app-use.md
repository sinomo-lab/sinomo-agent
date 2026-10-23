# App Use

App Use provides one skill and one control tool with two explicit modes.
Background OS window messages are preferred: they do not move the user's pointer,
take keyboard focus, or require the app to be foreground. The agent can select
raw desktop input when background control is unsuitable. There is no automatic
fallback or replay of a failed batch.

| Mode | Target and capture | Input |
|---|---|---|
| `background` (default) | Exact window; continuous compositor capture even while occluded | Posted virtual-key, character and mouse messages |
| `raw` (explicit) | Exact monitor; continuous compositor capture | Scan-code keyboard, Unicode text, absolute/relative mouse, buttons, wheels and conditional gamepad injection |

Windows capture and input are implemented in the independently built
[App Use OS provider](../../modules/desktop/app-use/README.md). It uses Windows
Graphics Capture/D3D11, PostMessageW, SendInput and conditional InputInjector
without installing drivers. Other platforms explicitly report unsupported
capabilities. Common actions, events, media encoding and transmission remain in
the host runtime. Shadow/mobile profiles omit the linked App Use host service.

## Installation and local consent

Build and activate the OS provider with the ordinary native-module lifecycle:

```powershell
cargo build -p sinomo-app-use-module
sinomo module native-dylib activate --module module.app-use.os --interface sinomo.app-use.os.v1 --cache-key module/desktop/app-use target/debug/sinomo_app_use_module.dll
```

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
Background `move` uses physical client pixels; raw uses physical virtual-desktop
pixels. Prefer background `move_in_frame` for image grounding: provide normalized
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
`sinomo_video::stream::read_frame`, `RemoteVideoSource` or
`forward_to_realtime`; consumers cancel abandoned bodies with `cancel_on_drop()`.
The legacy voice `--computer-stream` option remains a capture-session alias on
this same route; it does not create another skill, tool or OS provider.

Input batches are bounded to 256 events; text to 4096 UTF-16 units. Idle input
leases expire after 30 seconds and all sessions after at most 300 seconds.
Always pair held inputs and close sessions after success or failure. Do not
replay partially applied batches. Host teardown drains media workers, closes
native handles and joins OS workers before unloading the DLL.
