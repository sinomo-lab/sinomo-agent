---
name: app-use
description: Observe and control apps through background window messages, with explicit raw desktop input when background control is unsuitable.
---

# App Use

Use the single discovered `app-use` control tool and its media capture route.
Query `{"action":"capabilities"}` first. The native App Use OS provider must be
installed. Unsupported platforms and unavailable capabilities are explicit;
installation and discovery never grant local capture/input consent.

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

Always close using `{"action":"close","session_id":ID}`, including after failure.
Input leases expire after 30 seconds idle; all sessions have bounded lifetimes
(maximum 300000 ms). Release held inputs before switching modes. Never replay a
partially applied batch. Re-enumerate after window recreation.
