---
name: subagents
description: Delegate focused work to reusable temporary agents and coordinate their sessions.
---

Use `subagents` to spawn a child with a self-contained task and an optional
specialized prompt. Children inherit your configuration, tools, skills,
permissions, workspace and resources. Coordinate edits to shared files.

Use `message` with an agent id (or `parent`) to send instructions or findings.
A message queues into a running session; an idle child starts another turn in
its existing session. Completion notifies the parent automatically. Use
`inspect` with `session: true` for the current transcript and queued inputs.

`list` returns retained active children, including idle children. Specify
`archived: true` to search archived children, or inspect an archived id directly.
`discard` cancels execution and archives the session. An idle child is archived
after 24 hours. Archived children cannot be resumed; spawn a new child instead.
Use `cancel` to stop a turn while keeping the child available for later work.
