# Sinomo Agent System Architecture

## Purpose

This document defines the first agent-system shape for Sinomo: the user-facing
main agent, the maintenance-oriented maid agent, the local core model that
drives real-time interaction, and the async deliberation, tool, cluster, and
self-evolution paths around them.

It complements the distributed runtime in [architecture.md](architecture.md),
the cluster control plane in
[clustering-architecture.md](clustering-architecture.md), the cluster root
filesystem in [sifs.md](sifs.md), and controlled mutation in
[self-evolution-architecture.md](self-evolution-architecture.md). Current
implementation entry points are tracked in
[agent-system-implementation.md](agent-system-implementation.md).

## Design Position

Sinomo should feel like one continuous entity to users, but internally it is a
real-time multimodal agent running on top of a cluster-aware action substrate.
The main agent must never block live interaction just because slow work is in
progress. Tool calls, research, coding, background cleanup, and delegated agent
work run asynchronously and report results back into the session timeline.

The first implementation should use
[OpenBMB MiniCPM-o](https://github.com/OpenBMB/MiniCPM-o) as the local core
model family. As of 2026-05-10, the repository describes MiniCPM-o 4.5 as a 9B
end-to-end multimodal model for image, video, text, and audio input with text
and speech output, including full-duplex multimodal live streaming where
input streams and output streams do not block each other. The repository also
documents local deployment paths including PyTorch inference, llama.cpp-omni,
Ollama, vLLM, SGLang, LLaMA-Factory, and SWIFT.

Local inference is preferred over API inference for the main user-facing loop.
API and CLI models remain important, but they are helpers for slower or more
specialized work rather than the identity of Sinomo.

## Agent Roles

### Main Agent: Sinomo

The main agent is the user-facing agent named Sinomo.

Responsibilities:

- Maintain continuous full-duplex multimodal interaction with one or more
  users.
- Track session context, active user surfaces, active action targets, current
  task state, and pending async work.
- Decide when to speak, listen, interrupt, stay silent, ask for clarification,
  or continue a background task.
- Use local model inference for the real-time loop whenever hardware and
  policy allow.
- Use advanced API or CLI models for slow deliberation, planning, research,
  coding, maintenance planning, and debugging.
- Start, monitor, summarize, and cancel async work without blocking live
  conversation.
- Handoff cluster-operation and system-maintenance work to the maid agent by
  default.
- Start or request self-evolution when explicitly asked by the user, or when an
  architectural limitation prevents a bounded fix.

The main agent should not need to reason about low-level cluster mechanics for
ordinary user tasks. It sees SIFS as its filesystem, receives useful cluster
status summaries, and invokes tools with session-aware defaults. When placement
matters, it can target a specific node, device, or SIFS subtree explicitly.

### Maid Agent

The maid agent is the maintenance and background-operations agent.

Responsibilities:

- Keep the cluster healthy.
- Handle background tasks that are not directly part of the immediate user
  conversation.
- Aggregate operational events and decide whether action is needed.
- Clean up old files and artifacts according to storage rules and policy.
- Investigate logged errors, degraded nodes, failed tasks, stale workspaces,
  rollout issues, and unexpected daemon behavior.
- Support main-agent operations by preparing environments, checking node
  health, running diagnostics, or scheduling safe maintenance.
- Apply minimal bug fixes through the normal self-evolution or rollout path
  when the scope is clearly operational and bounded.
- Escalate large functionality changes to self-evolution instead of making
  broad ad hoc changes.

The maid agent should stay silent most of the time. Its default report target
is the main agent, not the user. It may address the user directly only when
there is a critical, time-sensitive, dangerous, or destructive issue, or when
the main agent is malfunctioning and cannot mediate.

The maid agent defaults to a powerful API-hosted or CLI-accessible coding LLM.
It is event-woken rather than continuously active. A cluster-wide concurrency
limit controls sessions for this agent; the default limit is one active maid
session across the whole cluster. Multiple events should coalesce into one wake
cycle so routine maintenance does not consume tokens continuously.

## Core Model

The core model is the model that drives Sinomo's real-time identity and user
interaction loop.

Requirements:

- Full-duplex multimodal operation.
- Streaming audio input and speech output.
- Streaming visual input from cameras, screens, or encoded video events.
- Text input and text output.
- Barge-in and overlap handling, where listening and speaking can happen at
  the same time.
- Low-latency local deployment path.
- Trainable or adaptable local model path for Sinomo-specific behavior.
- Stable event-level integration with the runtime so the model can observe
  async task progress without waiting on tool calls.

Initial model choice:

- First version: MiniCPM-o 4.5 or the best compatible MiniCPM-o release
  available at implementation time.
- First local serving path: PyTorch. This should be the initial reference
  runtime for model loading, streaming behavior, interrupt behavior, and
  training or fine-tuning experiments.
- Later serving paths: llama.cpp-omni, Ollama, vLLM, SGLang, or other
  deployment backends can be added after the PyTorch path defines the runtime
  contract.
- API fallback: allowed when local inference is unavailable, overloaded, or
  below the required quality threshold, but this should be visible in runtime
  status and policy.

The main agent should treat the core model as the live interaction controller,
not as the only reasoning engine. It can ask stronger or specialized models for
deliberation while continuing to talk with users.

## Runtime Topology

```text
Users / Devices / Sessions
  <-> Sinomo main agent
      <-> Core full-duplex multimodal model
      <-> Session timeline and world-state
      <-> Async task registry
      <-> Deliberation model gateway
      <-> Cluster-aware tool/action gateway
      <-> SIFS filesystem view
      <-> Maid handoff queue

Cluster events / health / storage / logs
  -> Maid wake aggregator
  -> Maid agent session
  -> Maintenance actions, reports, or self-evolution jobs
```

The main agent and maid agent are logical roles. They may run on the same node
in a single-machine deployment, but their responsibilities, wake policies,
permissions, and reporting paths should remain separate.

## Real-Time Interaction Model

Traditional LLM tool loops block on a request-response cycle. Sinomo should
instead use an event-driven, real-time loop:

1. Perception, user input, tool progress, cluster health, and async worker
   updates enter the session timeline as events.
2. The main agent continuously updates an interaction state: speaking,
   listening, waiting, delegating, recovering, or idle.
3. Slow work creates async task records and returns a handle immediately.
4. Partial progress, completion, failure, and cancellation arrive as new
   events.
5. The main agent decides whether each event should change the live
   conversation, update memory silently, wake the maid, or trigger
   self-evolution.

Async task status can be injected into the live model stream as steering. The
default immediate response after starting a background task is a short hint that
the task is running asynchronously and will report back later. The runtime
injects this hint right after the ongoing tool call or output stream boundary
instead of waiting for the original turn to complete. If MiniCPM-o's native
interrupt mechanism works reliably for tool-call progress, the runtime may use
that path for lower-latency background-task updates.

The main agent must be trainable because most language models are optimized for
turn-based tool calls, not concurrent full-duplex interaction with long-running
background work. Runtime traces should explicitly capture interruption,
overlap, task progress, delayed results, and user corrections so future local
training can teach the model this interaction style.

## Async Work and Deliberation

Advanced models from APIs and CLIs are used for deliberate work that does not
belong in the core real-time loop.

Examples:

- Planning complex multi-step tasks.
- Internet research and synthesis.
- Coding tasks.
- Maintenance and cleanup planning.
- Debugging Sinomo itself after crashes, unexpected behavior, regressions, or
  confusing user-visible failures.
- Reviewing proposed self-evolution candidates.
- Generating tests, diagnostics, and reproduction steps.

Async work should use the standard task-call schema, with durable task records
behind each call:

- `task_id`
- owner agent role
- originating session
- user-visible summary
- current state
- priority
- node placement
- permissions and resource limits
- cancellation policy
- input evidence handles
- output artifact handles
- progress events

The immediate return value follows normal task-call shape but marks the task as
backgrounded and includes the stream-injection hint to present to the main
agent. Completion and progress messages are delivered later through task events.
No new external call shape is required for the language model: from the model's
perspective these remain normal tool calls. The runtime guarantee is that a
later injected response will arrive when progress, completion, failure, or
cancellation needs attention.

Recommended injected response shape:

```json
{
  "kind": "async_task_update",
  "task_id": "task_...",
  "status": "running|progress|completed|failed|cancelled",
  "summary": "Short natural-language update for the live agent.",
  "needs_user_attention": false,
  "result_ref": "sifs:/services/tasks/task_.../result.json",
  "next_action_hint": "continue_silently|tell_user|ask_user|handoff_to_maid|inspect_result"
}
```

The `summary` should be safe to inject into a live stream. Larger outputs,
logs, files, or artifacts should be referenced through `result_ref` instead of
being inlined. `next_action_hint` is advisory steering, not authority; the main
agent still decides how to respond in context.

The main agent should be able to continue speaking with the user while these
tasks run. When a result arrives, it is just another event in the session
timeline and can be summarized, ignored, acted on, or handed to the maid.

## Cluster-Aware Tooling

All environment interaction runs on top of the Sinomo cluster infrastructure.
This does not require distributed model inference. It means tool execution,
filesystem access, device control, process execution, diagnostics, rollout, and
storage work are cluster-aware by default.

Main-agent view:

- The default filesystem root is SIFS.
- Session context supplies default node, user, workspace, and device targets.
- Tools choose sensible defaults when the target is obvious.
- The ordinary model context includes only basic cluster overview information:
  cluster status, available services, and endpoint information.
- Cluster inspection tools are available when the main agent needs deeper
  details.
- The agent can explicitly target a node, device, or SIFS path when needed.

Tool gateway responsibilities:

- Resolve session defaults into concrete node/device/workspace targets.
- Check cluster membership and orphan guards before distributed work.
- Enforce node-local policy and sandbox limits.
- Route actions to the node closest to the relevant device or data.
- Return async handles for slow calls.
- Attach provenance, node identity, and evidence handles to every result.
- Prefer structured machine-readable output over terminal-only text.

Cluster-operation tasks should be handed to the maid agent unless they are a
direct, immediate part of the user's task and safe for the main agent to drive.
This keeps cluster details mostly transparent to Sinomo's real-time persona
while still preserving explicit control when placement matters.

## Maid Wake and Event Aggregation

The maid agent is woken by events, not by polling an expensive model loop.

Wake sources:

- Cluster error logs.
- Repeated task failures.
- Storage rule violations.
- Low disk space or runaway artifact growth.
- Daemon crash or restart events.
- Rollout failures.
- DFS repair or replication failures.
- Security, policy, or sandbox incidents.
- Main-agent handoff requests.
- Scheduled maintenance windows.

Wake aggregation rules:

- Events are queued durably and grouped by family, affected node, affected
  service, workspace, and time window.
- Normal events wake the maid only once per configurable interval. The default
  interval is once per day.
- Emergency events, such as logged errors or dangerous policy incidents, wake
  the maid immediately.
- A wake cycle receives a queued incident bundle, not one isolated event.
- Cluster-wide maid concurrency defaults to one active session.
- New non-emergency events remain queued while the current maid session is
  running and are considered in the next wake cycle.
- Emergency events that arrive during an active maid session are queued after
  the current emergency bundle and do not start parallel sessions unless policy
  explicitly permits it.
- The event bundle is written to a temporary file instead of being injected
  directly into the model context. The maid agent receives the file path and
  uses file tools to inspect, filter, summarize, and act on the queued events.
- The temporary file format is LM-friendly JSON: a top-level list of event
  objects. Sinomo Object remains appropriate for internal storage and transport,
  but JSON is the handoff format for model-facing event review.
- Token budgets and wall-clock budgets are explicit per wake cycle.
- The maid writes a concise report to the main agent unless escalation rules
  require direct user notification.

Example wake condition:

```text
storage.rule_violation:
  path: /services/evolution/workspaces/old-candidates
  rule: delete_or_archive_after_days
  count: 42
  oldest_age_days: 31
  suggested_action: archive_or_delete
```

## Data Collection and Local Training

When local models are used, Sinomo should be able to train or adapt them from
operation data. Data collection is therefore part of the architecture, not an
afterthought.

Collected data classes:

- User interaction streams: text, audio transcripts, speech timing, barge-in
  points, selected visual summaries, and explicit user corrections.
- Agent decisions: when Sinomo spoke, stayed silent, interrupted, delegated,
  asked a question, used a tool, or handed off to the maid.
- Async task traces: task creation, progress, delayed results, cancellations,
  and user satisfaction signals.
- Tool traces: structured inputs, outputs, errors, node placement, duration,
  and policy decisions.
- Cluster traces: node health, rollout state, DFS issues, daemon errors, and
  recovery actions.
- Self-evolution traces: issue detection, proposal, implementation, evaluation,
  rollout, rollback, and postmortem outcomes.

Training data requirements:

- Collection defaults to on for every data class, including audio and video,
  because local traces are critical for training the real-time multimodal
  agent. Users, devices, sessions, and data classes must still support explicit
  opt-out.
- Consent, privacy, retention policy, and opt-out state must be explicit per
  user, session, device, and data class.
- Raw sensitive media should stay local by default. Redacted summaries,
  embeddings, or clips can be derived for training according to policy.
- Training examples should preserve concurrency and timing information, not
  only final text turns.
- Bad outcomes and corrected behavior should be kept as high-value training and
  evaluation material.
- Data used for training should have provenance, policy labels, and deletion
  hooks.
- Local fine-tuning, adapter training, preference tuning, and replay-based
  evaluation should be separate pipeline stages.

The first trainable path can start with adapter-style tuning or supervised
fine-tuning on curated traces. Later versions can add preference optimization,
replay-driven evaluation, and targeted training for real-time task-state
management.

## Self-Evolution

Both agents can start and monitor self-evolution tasks, but they should do so
for different reasons.

Main-agent triggers:

- The user explicitly asks Sinomo to evolve.
- A user-facing task repeatedly fails for architectural reasons.
- The main agent lacks a required tool, skill, or runtime behavior.
- The interaction model cannot represent the user's workflow cleanly.

Maid-agent triggers:

- A system issue cannot be fixed by bounded maintenance.
- A recurring cluster, DFS, rollout, daemon, or policy failure points to a
  design flaw.
- A minimal operational bug fix is insufficient or would be brittle.
- A maintenance task reveals missing observability, missing tooling, or unsafe
  manual steps.

Self-evolution handoff rules:

- Small operational repairs from the maid go directly to approval. If approved,
  they go directly to rollout. This path is intended for quick, narrow system
  fixes and must remain bounded.
- Until Sinomo has an autonomous approval system, approval for quick
  maid-driven operational fixes is delegated to a human. This should use the
  same approval and rollout surfaces as self-evolution.
- Large functionality changes use the self-evolution architecture.
- Evolution jobs must have bounded scope, write-set ownership, evaluation
  gates, rollback policy, and provenance.
- The main agent can explain and negotiate user-facing evolution goals.
- The maid can provide diagnostics, incident bundles, reproduction steps, and
  operational constraints.

## Permissions and Reporting

The main agent and maid agent need different defaults.

| Capability | Main agent | Maid agent |
|---|---|---|
| Direct user interaction | Default yes | Default no |
| Real-time speech/video loop | Default yes | No |
| Cluster health inspection | Summary by default | Full operational view |
| Cluster operation execution | Handoff by default | Default owner |
| User task execution | Default owner | Support role |
| Background cleanup | Handoff by default | Default owner |
| Minimal operational bug fix | Request or monitor | May drive through gates |
| Large functionality change | Self-evolution | Self-evolution |
| Direct user alert | Normal conversation | Critical escalation only |

Every action should carry the responsible agent role, initiating event, session
or maintenance bundle, target node, and policy decision. This is needed for
debugging, training, user explanation, and self-evolution evidence.

## Remaining Refinements

- Define the exact JSON schemas for async task updates and maid event-bundle
  files once the event and task crates stabilize.
- Define the user-facing controls for per-user, per-device, per-session, and
  per-data-class collection opt-out.
- Define the human approval UI or CLI flow shared by quick maid fixes and
  self-evolution rollout approval.
