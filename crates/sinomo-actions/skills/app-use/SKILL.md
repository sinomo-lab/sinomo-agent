---
name: app-use
description: Capture and send keyboard/mouse messages to a Windows app without taking the user's foreground focus or mouse.
---

# App Use

Use the discovered `app-use` module control tool. Query
`{"action":"capabilities"}` and `{"action":"targets"}` and preserve the full
returned window target (handle, PID, thread ID and process creation time).
Open with `{"action":"open","target":TARGET,"input":true,"capture":true,
"lease_ms":60000}`. Do not substitute a bare handle or infer identity from a
title. Open the returned media stream descriptor through the shared Sinomo video
path using `{"session_id":ID,"frames_per_second":30,"include_cursor":false}`.
For classic controls, enumerate `{"action":"children","target":TARGET}` and
open an input lease on the exact child. Keep capture on the top-level window;
the compositor may not expose a child control as an independent capture item.
Capture begins when a media consumer attaches and continues while occluded;
minimized/protected/suspended apps may stop producing new frames. Preserve frame
sequence and timestamps and never present an old frame as a fresh observation.

Send `{"action":"input","session_id":ID,"events":[EVENTS]}`. `key` uses a
Windows virtual-key code (unlike Computer Use's scan codes):
`{"kind":"key","code":65,"down":true,"extended":false}`. `text` sends UTF-16
WM_CHAR messages: `{"kind":"text","text":"hello"}`. `move` uses physical
window client coordinates: `{"kind":"move","x":30,"y":20}`. Set position
before `button` or `wheel`; button, wheel and release_all shapes match Computer
Use. Coordinates must fit signed 16 bits. Right/middle/X buttons and horizontal
wheel messages are supported. Send key/button up events after down events.

App Use never calls SetForegroundWindow, SetFocus, AttachThreadInput, SetCursorPos,
or SendInput. A successful result means messages were queued. Raw-input and
DirectInput programs, foreground checks, modifier-state polling, custom widget
routing, minimized windows and higher-integrity apps may ignore them. Verify
behavior through video. Do not silently switch to desktop injection. Relative
hardware mouse motion and gamepad injection are not window-message capabilities.

For visual grounding, prefer `move_in_frame` over pixel `move`. Its `x` and `y`
are fractions from 0 to 1 of the **entire image**, including window chrome:
0 is the left/top edge and 1 the right/bottom edge. Choose the control center
by its relative position; do not subtract chrome or screen origins. Include
`source_width` and `source_height` copied from the current video metadata
(dimensions before encoding). The device maps these fractions into client
coordinates and handles the target window's DPI. Fractions survive video
downscaling; no model-side pixel scaling is needed. Chrome/outside-client points
and source dimensions that disagree with the current window are rejected.
For models that struggle to estimate positions, request
`"encoding":{"coordinate_grid":true}` (alongside the encoding's `frame_format`)
on the media stream. The shared encoder draws a generic grid at every 0.1;
top labels give x fractions and left labels give y fractions. Interpolate the
control center between those lines. This opt-in overlay preserves the viewport
and is included in both recorded and model-visible frames.

Perform a resize in its own input batch. Observe a fresh frame and use its new
source dimensions for subsequent input. Likewise inspect the result after each
meaningful action before continuing: queued messages alone do not prove a hit.
For legacy pixel `move`, scale image pixels by source/encoded dimensions, add
the current window `bounds` origin and subtract `client_bounds` origin. Those
rectangles and legacy input pixels are physical; the backend converts them to
the target's message coordinates. Do not use a top-level frame's coordinates
with an input lease on a child window.

Close with `{"action":"close","session_id":ID}` even after a failure. Input
leases release held events after 30 seconds idle; session leases expire at their
requested duration (at most 300000 ms). Do not replay failed batches, since some
messages may already have been queued. Re-enumerate after window recreation.
Node-local remote-management consent and exact window resource exposures for
capture/input are required; module enablement and discovery do not grant them.
