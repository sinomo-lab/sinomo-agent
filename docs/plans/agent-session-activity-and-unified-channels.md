# Agent Session Activity and Unified Channels

Status: Active implementation plan (phases 1–5 implemented; follow-ups open)
Last reviewed: 2026-10-06

This plan makes the agent session the owner of everything an agent does
(reasoning, tool calls, async work) and makes a chat a channel with one agent.
A channel then differs from a chat only in its participant set and in the
policy that decides who acts on a message.

## Implementation progress

- **Phase 1 (activity identity): done.** Session item records carry `seq`
  (assigned by the store, per items log), `op_id`, `tool_call_id`, and
  `caused_by`. `AgentItem::ToolCall` carries `tool_call_id`; the executor uses
  it as the result's `call_id`, the progress scope, and the artifact identity;
  `AsyncTaskUpdate` can name the call that started its work. Recovery pairs
  interrupted calls by `tool_call_id`.
- **Phase 2 (activity read path): done.** Messages carry
  `produced_by { participant_id, session_id, op_id }`; the stored trace copy is
  gone. `/chat/session/activity` projects each requested operation's journaled
  items into a `ChatWorkTrace`; the app fetches it for loaded history and
  keeps a streamed trace when the durable reply replaces the live row.
  Decisions and gaps:
  - `produced_by` names the operation, not a sequence range: an operation's
    records are found by `op_id`, and a message closes exactly one operation.
  - Activity is read from the local session store of the chat's host.
  - Tool output files are not yet part of the projection (they are copied into
    channel attachments only on the live path).
- **Phase 3 (chat on the channel driver): done.** Chat sends run through
  `AgentChannelRuntime` with `AgentChannelSingleAgentPolicy`; the separate chat
  send path is gone. The policy interface (`AgentChannelPolicy`) decides who
  acts and how: `Answer` delivers the message as the turn's input (held back
  from history sync, then reconciled by its stable item id); `Observe` syncs it
  and lets the participant stay silent (`AgentChannelRoundRobinPolicy`, the
  default). Messages carry `audience` and `reply_to`; a chat's turn-handling
  notices (errors, no response) are users-only, so they stay out of the
  agent's context. A failed or silent answer's notice keeps `produced_by`.
- **Phase 4 (one runtime per participant): done, sequentially.** A single
  `ParticipantExecutor` runs a chat's agent on the chat's live runtime and
  each channel agent on its own runtime, kept across turns and invalidated
  when the chat changes channel; no session is swapped through another's
  runtime. Channel agents follow the chat's model selection, as they did when
  they shared its runtime. Every participant's full activity streams to
  clients, and `SpeakerStarted { speaker }` attributes what follows to that
  participant; channel replies get `produced_by` too. Gaps:
  - Participants still act one after another: the round-robin policy needs
    later speakers to see earlier replies. Concurrent participants wait for a
    policy that allows it.
  - Live control (steer, interrupt) targets the chat's own session, as before;
    routing it to running channel participants is open.
  - Client stream events carry speaker attribution through `SpeakerStarted`
    rather than a `source` on every event, and the dotted `agent.*` event
    publication is not implemented.
- **Phase 5 (detached tool calls): done for tool calls.** When the host can
  continue a session (the session runtime's progress sink carries a detached
  cancellation token), an `async` call that the surface allows runs on a
  background thread. The turn records an immediate result
  (`{"detached": true, "task_id", "tool_call_id"}`) and moves on. The
  completion becomes an `AsyncTaskUpdate` with the call's `tool_call_id`,
  queued on the session runtime; detached work and queued completions count
  as running work, so the host is not released. An idle chat host polls for
  completions (every 250 ms) and runs `AgentChat::continue_async_updates`:
  the participant takes a turn with the update as input
  (`AgentChannelRuntime::continue_with_async_update`, gated by
  `AgentChannelPolicy::may_publish_unprompted`), the turn journals the update,
  and the reply is published unprompted, framed by `TurnStarted`/`TurnDone`.
  The projection shows the starting operation's call as `detached` until the
  completion, then as finished; the app refetches such replies after a turn.
  Gaps:
  - Subagent spawns still run as polled handles, not detached calls.
  - Detached work and queued completions live in memory: a restart or host
    release loses them, and the call stays `detached`. Persisting them as
    session tasks (paused on restart) is open.
  - A completion that arrives during a turn waits for that turn to end.
  - In multi-agent channels, the host's idle check covers the chat's own
    session, not channel participants' detached work.
  - Detached calls show no live process output; their result arrives with
    the completion.

## Goals

- One driver for chats and channels. A chat is a channel with one
  participant and the single-agent policy; message storage, delivery,
  participant sessions, events, and history are shared code.
- Session-scoped activity. Reasoning, intermediate output, and tool calls are
  recorded once, in the participant's session, with a lifecycle that may span
  turns. Async tool calls and subagents can finish in a later turn and still
  update the call that started them.
- Visible messages refer to the activity that produced them instead of
  copying it.
- Channel messages carry enough addressing to support private messages,
  addressed messages, and speaker selection later without a storage change.
- Every participant runs in its own session runtime, so participants can work
  concurrently.

## Non-goals for this plan

- Implementing private messages, mentions, or a "who should talk" selector.
  The data model and policy interface must admit them; this plan does not
  build them.
- Changing compaction, context assembly, memory, or model providers.
- Cross-user access control for shared channels beyond the audience field
  described below.

## Current state

The engine is already shared; the drivers are not.

- Both paths run turns through `AgentSessionRuntime`
  (`crates/sinomo-agent/src/agent/control.rs`). It owns turn operations,
  steering and follow-up queues, tasks, runtime events, and the control plane.
- Chat (`AgentChat::send_with_attachments`, `crates/sinomo-cli/src/app_api.rs`)
  drives the primary participant's runtime directly and forwards model and tool
  events to clients.
- Channels (`send_channel_with_attachments`) build `AgentChannelRuntime`
  (`crates/sinomo-agent/src/channel.rs`) with `AppAgentChannelExecutor`. The
  executor reuses the chat's single `InteractionRuntime`, switching each
  participant's agent definition and session into it in turn and restoring the
  primary agent afterwards. Participants therefore never run concurrently, and
  the executor forwards only reply tokens and tool artifacts; reasoning and
  tool-call events are dropped.
- The session item log already journals `AgentItem::ToolCall`, `ToolResult`,
  `AssistantReasoningSummary`, and `AsyncTaskUpdate` when they are produced
  (`AgentSessionStore::append_items`). Records carry `session_id`, `item_id`,
  and `recorded_at`, but no operation id and no per-session sequence.
- Tool calls have no stable identity across their lifecycle. `ToolCall` items
  carry the model's `call` (with an optional provider `call_id`) and
  step/call indexes; `ToolResult.call_id` is a fresh id generated by the
  executor and is not recorded on the call. Pairing works only by indexes
  inside one turn.
- Async work is partial. `AsyncTaskUpdate` (keyed by task id) can enter a later
  turn through `AgentLoopInput.async_update`, and tasks live on the session
  control plane, but nothing executes `async` tool calls in the background, and
  subagent results are only polled from inside a turn.
- The `chat-per-turn-work-trace` branch stores a `ChatWorkTrace` copy on the
  channel message that closed each turn. That copy cannot be updated when an
  async call finishes later, and it duplicates the session item log. This plan
  replaces it with a reference to session activity (see
  [Disposition of the per-turn trace](#disposition-of-the-per-turn-trace)).

## Concepts

| Term | Meaning |
|---|---|
| Channel | The user-facing conversation: membership, durable visible messages, and workspaces. A chat is a channel with one agent participant. |
| Participant | A member of a channel. Agent participants are bound to exactly one agent session for that channel. |
| Agent session | The execution unit behind one agent participant: model context, queues, tasks, tool state, and its activity log. Hosted on one session server. |
| Operation (op) | One unit of work submitted to a session, identified by `op_id`. A turn is an operation; so is a continuation started by an async result. |
| Activity | The session's ordered record of what the agent did: items (reasoning, text, tool calls, tool results, async updates, errors) plus live progress. |
| Tool call | A request by the model to run a tool, with a stable `tool_call_id` and a lifecycle that may span operations. |
| Message | An immutable visible record in the channel, authored by a user, an agent participant, or the system. |
| Audience | Which participants a message is delivered to. |
| Channel policy | The orchestration rule that decides which participants act on a message. |

## Layers

```
 clients (app, CLI, peers)
        |  commands, typed events, history and activity reads
 +------v-------------------------------------------------------+
 | Channel host                                                 |
 |   Channel store      membership, messages, audience, turns   |
 |   Channel policy     who acts on a message; may a session    |
 |                      publish on its own initiative           |
 |   Delivery           message -> participant session, by      |
 |                      audience (sync_participant_session)     |
 +------+--------------------------+----------------------------+
        | ops, delivered messages  | ops, delivered messages
 +------v---------------+   +------v---------------+
 | Participant runtime  |   | Participant runtime  |   one per agent
 |  AgentSessionRuntime |   |  AgentSessionRuntime |   participant, on its
 |  activity log        |   |  activity log        |   session server
 +----------------------+   +----------------------+
```

1. **Channel store.** Unchanged in role: the source of truth for visible
   messages. Gains the message fields below.
2. **Channel policy.** The only layer where chat and multi-agent channels
   differ.
3. **Delivery.** The existing `sync_participant_session` projection, extended
   to filter by audience.
4. **Participant runtime.** One long-lived `AgentSessionRuntime` per agent
   participant. The executor no longer swaps sessions into a shared runtime.
5. **Session activity.** The session item log, extended into an addressable,
   ordered activity log, published as typed events.

## Session activity

### Activity records

Each durable activity record extends today's `AgentSessionItemRecord`:

| Field | Purpose |
|---|---|
| `session_id` | Owning session. |
| `seq` | Per-session monotonic sequence, assigned when journaled. Readers page and resume by `seq`. |
| `item_id` | Existing item identity; the reconciliation key for live progress. |
| `op_id` | Operation that produced the item. Absent only for items the session records outside an operation. |
| `tool_call_id` | Present on tool-call, tool-result, process-progress, and async-update records that belong to a call. |
| `recorded_at` | Existing timestamp. |
| `caused_by` | Event or item ids that caused this record, such as the delivered message id that started the op, or the task completion behind an async update. |
| `item` | The `AgentItem`. |

The log stays append-only. A record is never rewritten; a later state of the
same tool call is a new record with the same `tool_call_id`.

### Tool-call identity and lifecycle

A `tool_call_id` is assigned when the model's call is first recorded and is
carried by every later record about that call: the `ToolCall` item, process
progress, the `ToolResult`, artifact outputs, and any `AsyncTaskUpdate` for
work the call started. The provider's `call_id` remains a separate
`native_call_id`; the executor's per-execution id becomes the `tool_call_id`
instead of an unrecorded value.

Lifecycle, as observed by readers:

```
requested -> running -> completed | failed | cancelled | blocked
                  \
                   -> detached -> (later op) completed | failed | cancelled
```

- `detached` means the call returned control to the model and continues as
  background work owned by the session (a task with `tool_call_id` and
  `task_id`). Its completion is recorded in whichever operation is current
  when it arrives, and may start a continuation operation through
  `AsyncTaskUpdate`.
- Subagents follow the same rule: `spawn` is a detached call whose task
  completion produces a record carrying the spawning call's `tool_call_id`.
- Process progress carries the `tool_call_id` instead of the step/call scope
  used on the `chat-per-turn-work-trace` branch.

### Live progress versus durable records

Model streaming produces progress that is not journaled item by item
(reasoning deltas, assistant text deltas, tool-input deltas, process output
chunks). These are published as live activity events tagged with the
`item_id` or `tool_call_id` they will finalize into. The durable record
supersedes them: a reader that finds the durable record ignores earlier live
progress for the same id. Clients that attach late read durable records and
then follow live events from the current position.

Process output is stored once, as evidence artifacts with bounded previews,
following the existing storage rule. Activity records refer to those
artifacts; they do not inline complete streams.

### Visibility

A session's activity is private to its participant: other agent participants
see only messages published to the channel. Users with access to the channel
can read every participant's activity. Activity is never delivered into
another participant's model context.

## Messages and activity references

A channel message gains these fields; all are optional and default to today's
behavior:

| Field | Purpose |
|---|---|
| `produced_by` | For agent messages: `{ participant_id, session_id, op_id }`, the operation whose activity produced the message. |
| `audience` | `everyone` (default) or `participants: [participant_id]`. Delivery skips participants outside the audience. |
| `addressed_to` | Participants the message is directed at. Advisory; consumed by policy. |
| `reply_to` | Message id this message answers. |

The "work above this reply" view is the activity of `produced_by.op_id`,
read from the producing session. A tool call that started in that range and
finished later is shown with its latest state, found by `tool_call_id`, so
the view stays correct after async completion without rewriting the message.
System messages that close a failed operation ("No agent responded", errors)
carry `produced_by` too, so failed turns keep their work visible.

Durable messages stay immutable. Nothing about a message changes when its
activity changes.

## Channel policy

```rust
trait ChannelPolicy {
    /// Participants that should act on a newly appended message, in order.
    fn on_message(&self, channel: &ChannelView, message: &AgentChannelMessage)
        -> Vec<ActRequest>;

    /// Whether a session may publish without a triggering message, for example
    /// after an async result continues its work.
    fn may_publish_unprompted(&self, channel: &ChannelView, participant: &ParticipantId)
        -> bool;
}

struct ActRequest {
    participant_id: ParticipantId,
    trigger: MessageId,
    /// Whether the participant may stay silent instead of publishing.
    silence_allowed: bool,
}
```

- **Single agent.** Every user message produces one `ActRequest` for the agent,
  with silence not allowed; unprompted publishing is allowed. This is today's
  chat behavior.
- **Round robin (current channels).** Every enabled participant except the
  author acts in order and may stay silent; agent replies re-enter the policy
  as new messages, bounded by `max_agent_messages_per_ingress`.
- **Later policies** plug in here without touching storage or runtimes: a
  selector that picks one speaker, mention routing via `addressed_to`, and
  private side conversations via `audience`.

The channel host owns policy evaluation and message sequencing. Participant
runtimes never append to the channel directly; they publish through the host,
which assigns the sequence, applies `audience`, and records `produced_by`.

## Participant runtimes and concurrency

- Each agent participant has its own `AgentSessionRuntime` on its session
  server, following the existing placement rules. Sessions of one channel may
  live on different servers; the channel host reaches them through the same
  session routing used for live control.
- A session runs at most one operation at a time, as today. Different
  participants may run concurrently.
- The channel host serializes message appends. Concurrent publications receive
  distinct sequences in arrival order; `reply_to` preserves which message each
  answered.
- Tool runtime and workspace projection are configured once per participant
  runtime and refreshed when channel workspaces change, instead of on every
  speak call.
- The per-ingress response bound stays with the policy, so concurrent
  participants cannot amplify loops.

## Events and client API

Event names follow the dotted event contract. Every event carries
`channel_id`, `participant_id`, `session_id`, `op_id` (when applicable), the
producing node, and `caused_by`.

| Event | Emitted when |
|---|---|
| `agent.op.started` / `agent.op.completed` / `agent.op.failed` | A session operation changes state. |
| `agent.activity.appended` | A durable activity record is journaled; carries `seq`. |
| `agent.activity.progress` | Live progress for an `item_id` or `tool_call_id` (deltas, process chunks). Not retained. |
| `tool.call.updated` | A tool call changes lifecycle state, including detached completion in a later op. |
| `channel.message.published` | The host appends a message; carries `produced_by` and `audience`. |
| `channel.policy.decided` | The policy produced act requests for a message (for audit and UI "who is responding"). |

Client changes:

- Chat stream events gain a `source { participant_id, session_id, op_id }`.
  `AgentToken`'s speaker becomes this general source, so single-agent and
  multi-agent clients consume one stream shape.
- History pages return messages with `produced_by`; they do not embed activity.
- A new read route, `/chat/session/activity`, returns activity records for a
  session by `seq` range or by `op_id`, and the latest state of given
  `tool_call_id`s. It is served by the session's server and routed there like
  other session control, with a durable-store fallback when the session is
  dormant.

## UI model

- The transcript renders messages from the channel and, for each agent message
  with `produced_by`, an activity block above it, loaded on demand when the
  message scrolls into view.
- While an operation runs, the live block is built from `agent.activity.*`
  events for that `op_id` and shown above the participant's in-progress reply.
- A tool call found in an older block that changes state later updates in
  place, by `tool_call_id`.
- Channels show one live block per active participant. A chat is the same view
  with one participant.

## Disposition of the per-turn trace

The `chat-per-turn-work-trace` branch remains a useful intermediate step. When
this plan lands:

- **Kept:** process progress scoped to its tool call (re-keyed to
  `tool_call_id`); appending process output deltas instead of replacing them;
  sandbox completion events that do not repeat output; per-message rendering
  and per-block fold state in the transcript.
- **Reworked:** `ChatWorkTrace` becomes a client-side projection of activity
  records and progress events for one op, not a stored artifact.
- **Removed:** `AgentChannelMessage.work_trace` and
  `ChatHistoryStoredMessage.work_trace`, replaced by `produced_by`.

## Implementation phases

Each phase is independently mergeable and leaves chats working.

1. **Activity identity.** Add `seq`, `op_id`, `tool_call_id`, and `caused_by`
   to session item records; assign `tool_call_id` at call recording and carry
   it through results, process progress, artifacts, and async updates.
   Tests: pairing across a continuation op; restart recovery keeps ids; the
   failed result synthesized for an unfinished call keeps its `tool_call_id`.
2. **Activity read path and events.** Add `/chat/session/activity`, the
   `agent.activity.*`, `agent.op.*`, and `tool.call.updated` events, and
   `source` on client events. Replace stored traces with `produced_by`; the UI
   reads activity per message.
   Tests: history reload shows each turn's work; a late attach mid-op shows
   durable records plus live progress without duplicates.
3. **Chat on the channel driver.** Route chat sends through the channel host
   with the single-agent policy. Remove the separate chat send path.
   Tests: existing chat behavior (steering, follow-up, interrupt, error and
   no-response messages) through the channel driver.
4. **One runtime per participant.** Replace session swapping in
   `AppAgentChannelExecutor` with per-participant runtimes; configure tool
   runtime per participant; forward full activity for every participant.
   Tests: two participants run concurrently; each participant's activity is
   attributed correctly; per-ingress bound still holds.
5. **Detached tool calls.** Execute `async` tool calls and subagent spawns as
   detached session tasks whose completion appends records with the original
   `tool_call_id` and may start a continuation op subject to
   `may_publish_unprompted`.
   Tests: a call started in op N completes in op N+1 and updates the block of
   op N; restart while detached reports the call as interrupted.

When the phases complete, move the durable decisions into
[agent system architecture](../agent-system-architecture.md) and the chat
boundary paragraph of the [architecture overview](../architecture.md).

## Open questions

- **Dormant and remote reads.** Should activity for a dormant session be read
  from the session server's local store (requires the server to be reachable),
  or should finished activity be checkpointed to SIFS with the session
  checkpoint so any node can serve history?
- **Reasoning retention.** Providers differ in whether raw reasoning or only
  summaries may be stored. The record should store what the provider permits
  and mark which it is.
- **Ordering across participants.** Channel `seq` orders messages; session
  `seq` orders activity. A combined timeline view of concurrent participants
  needs a merge rule (by `recorded_at` with `seq` tie-breaks is the default
  proposal).
- **Unprompted publishing.** When a detached call completes and the agent
  wants to speak, which message is `reply_to`, and can the policy decline
  without losing the result?
- **Progress volume.** Live progress for many concurrent participants may need
  coalescing per `item_id` before it reaches clients.
