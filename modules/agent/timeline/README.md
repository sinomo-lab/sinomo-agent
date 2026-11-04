# Agent timeline

`module.agent-timeline` adds an **Agent timeline** panel to chats and channels.
It is an independent native dylib; it does not link into the app or require the
subagents module. Native channel participants and their recorded tool calls and
messages appear in separate lanes. Related sessions appear automatically when
present, including sessions created by the subagents module.

After normal verified module activation, open a conversation and select
**Agent timeline**. Select a bar for its status and argument preview. Use the
wheel to zoom, horizontal scrolling or Left/Right to pan, Up/Down to scroll lanes,
A to show or hide archived sessions, and R to reset the view. Overlapping calls
occupy separate tracks. The view refreshes while open and displays failures
instead of blocking the UI.

The timeline reads canonical session journals and channel message references.
Intervals measure recorded call-to-result time; they are not CPU time. Detached
completions correlate by session and tool-call identity even across operations.
A result whose start lies outside the loaded window reports an unknown duration.
The view reads the latest 128 activity records per session, up to 64 sessions,
and the latest 256 channel messages; limited windows are labelled partial.
Long record payloads are bounded previews and report truncation.

The module owns projection, layout, input handling and view state. The app owns
the panel slot and native frame renderer. Data access uses the generic scoped
session host service; it grants no new tool or filesystem authority. The panel
cannot issue session mutations or UI host-effect requests.

Build and verify:

```sh
cargo build -p sinomo-agent-timeline
cargo test -p sinomo-agent-timeline
cargo test -p sinomo-agent --test agent_timeline
```

Activation follows the [module extension guide](../../../docs/developer/extension-points.md).
Building does not modify the signed default distribution profile or activate a
module in an installed daemon.
