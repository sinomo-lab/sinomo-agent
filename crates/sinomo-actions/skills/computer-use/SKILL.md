---
name: computer-use
description: Observe a Windows monitor as continuous video and control the desktop keyboard, mouse, or supported driver-free gamepad.
---

# Computer Use

Use the discovered `computer-use` module control tool. Its `input` is a tagged
request: `{"action":"capabilities"}` or `{"action":"targets"}`. Always inspect
capabilities and select an exact returned monitor target. This mode controls
the user's real desktop and pointer. Use App Use for background window messages.

Open a lease with `{"action":"open","target":TARGET,"input":true,
"capture":true,"lease_ms":60000}`. The result contains `session_id` and a media
stream descriptor. Open that descriptor through Sinomo's media client with
`{"session_id":ID,"frames_per_second":30,"include_cursor":true}`. Capture is a
continuous Windows compositor stream, not a series of screenshot tool calls.
Use the existing video stream reader/realtime session adapter for model input;
do not put video bytes in JSON tool results. Capture starts when the media
consumer attaches. Closing that body stops the capture. The lease is bounded
to 300000 ms; open another lease for longer work.

Send `{"action":"input","session_id":ID,"events":[EVENTS]}`. Events:

- `{"kind":"key","code":30,"down":true,"extended":false}` uses a Set-1 scan code.
- `{"kind":"text","text":"hello"}` sends Unicode text.
- `{"kind":"move","x":100,"y":100}` uses physical virtual-desktop pixels.
- `{"kind":"relative_move","dx":10,"dy":0}` sends relative motion.
- `{"kind":"button","button":"left","down":true}`; buttons also include right, middle, x1, x2.
- `{"kind":"wheel","delta":120,"horizontal":false}`.
- `{"kind":"gamepad","buttons":4,"left_x":0,"left_y":0,"right_x":0,"right_y":0,"left_trigger":0,"right_trigger":0}` sends a full Windows GamepadButtons state. Axes are [-1,1], triggers [0,1]. Windows may deny the restricted inputInjectionBrokered capability; report this and never install a driver as a fallback.
- `{"kind":"release_all"}` releases this lease's held input.

Video encoding may resize frames. Convert image coordinates to native pixels
using `source_width / width` and `source_height / height` from the frame metadata,
then add the monitor's `bounds` left/top origin. Do not send scaled image
coordinates directly. Re-enumerate geometry after display changes.

Release keys/buttons explicitly and close with `{"action":"close","session_id":ID}`.
Idle input expires after 30 seconds. Input batches are not retryable: on partial
failure inspect the receipt and observe current state before deciding the next
action. Confirm outcomes visually; injection acceptance does not prove the app
performed the intended action. Re-enumerate after monitor or target changes.

Installation grants no authority. The affected node must already grant local
`remote-management` and exact resource exposures `capture` and/or `input`.
Permission revocation stops capture and releases owned input. Never change
permissions through this tool, bypass secure desktops, or assume other OSes
support a capability that their capability response marks unavailable.
