---
name: app-use
description: Observe and control apps through available accessibility semantics, background window messages, and explicitly selected raw input or video.
---

# App Use

Use the single discovered `app-use` control tool and its media capture route.
Query `{"action":"capabilities"}` first. The native App Use OS provider must be
installed. Unsupported platforms and unavailable capabilities are explicit;
installation and discovery never grant local capture/input consent.

Linux X11 supports background and raw control. Wayland uses explicit raw mode
through desktop portals, including when Xwayland is present. The first raw
target request starts a local chooser and returns an empty list; repeat discovery
after user approval, then request local grants for that exact target. A
ScreenCast-only portal offers capture without input. Query raw targets directly
when capabilities report no background input. Never infer a silent mode change
from a failed input batch.

On Android, prefer persistent Accessibility when `capabilities.accessibility`
is true. Enumerate raw targets and select `android:accessibility:display:0`.
`observe` takes that `target` and optional `max_nodes` (1..128), returning a bounded
snapshot with opaque node IDs. `act` takes `target`, `snapshot_id`, `node_id`, and
`operation`: `{"kind":"click"}`, `long_click`, `focus`, `scroll_forward`,
`scroll_backward`, or `{"kind":"set_text","text":"..."}`. An action consumes
its snapshot; expired or changed nodes require a fresh observation. Passwords
are redacted. Verify effects by observing again; never replay an uncertain action.

The Accessibility target also offers rate-limited screenshots through the capture
route without screen-sharing consent. Select observations explicitly: semantic
state is preferred, but screenshots and generic video can supplement it. You may
combine these sources. `{"action":"video_sources"}` discovers camera and
screen sources through the same media-owned checks as `/media/video/devices`.
Each device includes a typed `source` descriptor; `/media/video/capture` takes a selected source, cadence, encoding
and transport. Use `lease_ms` for a screen source and `duration_ms` for a camera;
this prevents older camera-only peers from misinterpreting a screen request. An omitted source retains default-camera behavior.
Screen video requires a locally approved MediaProjection share. It is independent
of Accessibility and persists without consumers. Closing a stream does not stop
sharing. An authorized explicit `/media/video/control` stop request ends that
screen share; the agent tool's `{"action":"stop_video","source":SOURCE}`
uses the same media-owned operation and local stop grant. Neither path can
disable Accessibility or approve a new projection.

For a Windows app with multiple windows, check `capabilities.surfaces` and use
an application session. `applications` returns process-instance identities and
resource IDs. `open_application` takes that exact `application` and a bounded
`lease_ms`. Its `session_id` remains valid while the process runs, even if its
main window closes. Local `observe` consent for the application resource is
required; discovery never grants it.

Call `surfaces` with the application `session_id` to see current top-level
windows and child controls, their opaque `surface_id`, owner/parent, visibility,
enabled state and `blocks_owner`. A disabled window is not an input target;
select its enabled dialog explicitly. Destroyed/recreated windows get new
references. Process-wide `input`/`capture` grants cover windows in that exact
process instance. Cross-process owned windows require their own lifetime-scoped
surface grants. Legacy window grants remain usable only through the legacy
window API and are never promoted into application consent.

Use `open_window` with `application_session_id`, `surface_id`, `input`, `capture`
and `lease_ms`. Every `input` on that session must repeat its `surface_id`.
Capture records carry the matching identity. Never substitute a different
window's coordinates, even when dimensions happen to match.

`open_overview` creates a media view for an application session (up to four
windows). Attach its stream through the normal capture route. `select_window`
with `overview_session_id` and a `surface_id` switches that view to the selected
window's full-resolution video; `surface_id:null` returns to thumbnails.
Selection changes observation only, never an input session's target. Use a full
window view before coordinate input. Overview tiles retain source dimensions
and capture timestamps; omitted surfaces are listed explicitly. An
`identity.kind:unavailable` frame is a derived status, not live window pixels.
Use `surfaces` to choose a new window instead of acting on a stale frame.

`tray_items` discovers exposed, visible Windows shell controls without opening
overflow. These controls have no verified application ownership. Use their exact
`item_id` for `invoke_tray`, choosing `operation:invoke` or `context_menu`.
UIA context-menu support may be unknown (`context_menu:null`) and can fail
explicitly. Never assume an accepted semantic call completed the app task.

Tray/menu invocation can open or activate app/shell UI. It requires
`allow_desktop_interaction:true` plus local consent: tray resources need `invoke`
and `desktop-interaction`; app menu actions need `input` and
`desktop-interaction` for the app or exact surface. App/window consent does not
authorize the tray. Hidden/overflow icons are unavailable until the relevant
shell UI is explicitly opened. Do not open it or switch to raw input silently.
No pointer injection, Explorer memory scraping or guessed callback messages are
used for semantic actions.

After an app exposes a menu window, `menu_items` takes
`application_session_id` and its `surface_id`. `invoke_menu_item` takes those
same fields, the returned `item_id`, and explicit desktop-interaction opt-in.
Menu references expire when their owning window/session is gone. Closing an
application session with the existing `close` action closes its input/view
leases and releases native observers. Close any separately opened legacy/raw
sessions as well. The application-session and semantic APIs report unsupported
on platforms where they are unavailable; existing per-window/raw tools remain
usable according to their capabilities.

Prefer **background** control. `{"action":"targets"}` defaults to
`"mode":"background"` and lists windows. Preserve the complete target identity
(handle, process ID, thread ID, process creation time). Open with
`{"action":"open","target":TARGET,"mode":"background","input":true,
"capture":true,"lease_ms":60000}`. Background input never activates the app,
changes keyboard focus, moves the user's pointer, or injects global input.
Occluded windows can be captured. Minimized/protected/suspended apps may stop
producing frames; capture does not restore or focus them.

For classic controls, `{"action":"children","target":TARGET}` provides exact
child identities for input leases. Keep capture on the top-level window when
the compositor does not expose child capture. Do not use a top-level frame's
coordinates with a child input lease.

Open the returned capture descriptor through Sinomo's shared media path using
`{"session_id":ID,"frames_per_second":30,"include_cursor":false}`. Capture is
continuous, independent of tool execution. Preserve frame sequence/timestamps;
a stale cached image does not establish success. If capture is already supplied,
open only input (`input:true,capture:false`).

Send `{"action":"input","session_id":ID,"events":[EVENTS]}`:

- `text`: UTF-16 text, e.g. `{"kind":"text","text":"hello"}`.
- `key`: numeric Windows virtual-key code in background mode (Enter is 13).
  Pair `down:true` and `down:false`; `extended` defaults false.
- `move`: physical client pixels in background mode.
- `button`: `button` is `left`, `middle`, `right`, `x1` or `x2`; pair down/up.
- `wheel`: signed `delta`, 120 units per tick; positive scrolls up. Optional
  `horizontal:true` selects horizontal scrolling. Set position before button
  or wheel events. `release_all` releases this lease's held input.

For background grounding, prefer `move_in_frame`: `x` and `y` are fractions
[0,1] of the entire image including chrome. Zero is the left/top edge, one the
right/bottom edge. Choose the control center. Include `source_width` and
`source_height` copied from the current video metadata, before encoding. Do not
subtract chrome, screen origins or DPI offsets. The provider maps coordinates
and rejects stale sizes and points outside the client. Resize in a separate
batch, then observe the new dimensions before acting. Optional
`encoding.coordinate_grid:true` (with `encoding.frame_format`) draws generic
rulers: top labels give x fractions and left labels y fractions. Interpolate
between the lines. Recording and model input receive the same guides.

Queued messages are best effort. Raw-input/DirectInput apps, hardware polling,
foreground checks, custom routing and higher-integrity apps may ignore them.
Inspect the actual visual result after each meaningful action. Correct grounding
or child targeting if possible; OS acceptance alone never proves success.

When background control is unsuitable, explicitly choose **raw** control with
the same tool. Close the background input lease, query
`{"action":"targets","mode":"raw"}` for monitors, and open a new input lease
with `"mode":"raw"` and the exact monitor target. Raw mode can interrupt the
user's keyboard/mouse and change foreground. It requires separate display
exposure grants; window consent does not authorize raw input. There is no
silent fallback or automatic replay of previously queued input.

In raw mode, `move` uses physical virtual-desktop pixels, including negative
origins. Scale encoded pixels by source/encoded dimensions and add the monitor
origin. `key` uses Set-1 scan codes (Enter is 28). `relative_move` accepts `dx,dy`.
`gamepad` supplies a complete Windows GamepadButtons mask plus axes
`left_x,left_y,right_x,right_y` in [-1,1] and `left_trigger,right_trigger` in [0,1].
Driver-free gamepad support is conditional; report Unsupported honestly. Send a
separate neutral state to release held controller inputs. Window-only
`move_in_frame` is unavailable in raw mode.

On Android, `move` sets a virtual touch point. Left-button down/move/up submits
a finite tap/drag on release; `release_all` or close discards an unfinished
gesture. Wheel events submit swipes. Text requires a focused accessible
non-password editor. Only raw key codes 1 (Escape/Back), 14 (Backspace) and 28
(Enter) are supported; gamepads and other keys are unavailable. Input uses the
persistent Accessibility target even when grounded in screen video. Rotation
invalidates old coordinate input leases; observe and reopen input. It does not
end projection. Video frames carry native dimensions, `content_bounds` for
letterboxing, and `media.video.geometry.changed` notifications. Map coordinates
through the current content rectangle, never through a stale frame.
On X11, text is limited to the current keyboard map and wheel deltas must
be multiples of 120. Wayland text uses Unicode keysyms, and its
wheel deltas must also be multiples of 120. Wayland `move` coordinates are
portal-advertised stream coordinates: scale source-frame pixels to the target's
bounds, without adding a global desktop origin. Its portal session uses a hidden
cursor and ends when its final input/capture lease closes. Observe the visual
result before continuing.

Always close using `{"action":"close","session_id":ID}`, including after failure.
Input leases expire after 30 seconds idle; all sessions have bounded lifetimes
(maximum 300000 ms). Release held inputs before switching modes. Never replay a
partially applied batch. Re-enumerate after window recreation.
