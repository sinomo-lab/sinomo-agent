# Sinomo Agent System Architecture

## Purpose

This document defines the first agent-system shape for Sinomo: the user-facing main agent, the maintenance-oriented background agent, the local core model that drives real-time interaction, and the async deliberation, tool, cluster, and self-evolution paths around them.

It complements the distributed runtime in [architecture.md](architecture.md), the cluster control plane in [clustering-architecture.md](clustering-architecture.md), the cluster root filesystem in [sifs.md](sifs.md), and controlled mutation in [self-evolution-architecture.md](self-evolution-architecture.md). Current implementation entry points are tracked in [agent-system-implementation.md](agent-system-implementation.md).

## Design Position

Sinomo should feel like one continuous entity to users, but internally it is a real-time multimodal agent running on top of a cluster-aware action substrate. The main agent must never block live interaction just because slow work is in progress. Tool calls, research, coding, background cleanup, and delegated agent work run asynchronously and report results back into the session timeline.

The implementation should follow the coding-harness pattern used by mature agent runtimes: a small core loop around a streaming model, a typed transcript/evidence store, runtime-owned tool policy, explicit context assembly, and first-class compaction. Provider-specific behavior belongs at the provider adapter boundary. Prompt text can guide behavior, but it must not be the security, storage, rollout, or tool-execution authority.

The first implementation should use [OpenBMB MiniCPM-o](https://github.com/OpenBMB/MiniCPM-o) as the local core model family. As of 2026-05-10, the repository describes MiniCPM-o 4.5 as a 9B end-to-end multimodal model for image, video, text, and audio input with text and speech output, including full-duplex multimodal live streaming where input streams and output streams do not block each other. The repository also documents local deployment paths including PyTorch inference, llama.cpp-omni, Ollama, vLLM, SGLang, LLaMA-Factory, and SWIFT.

Local inference is preferred over API inference for the main user-facing loop. API and CLI models remain important, but they are helpers for slower or more specialized work rather than the identity of Sinomo.

## Agent Roles

### Main Agent: Sinomo

The main agent is the user-facing agent named Sinomo.

Responsibilities:

- Maintain continuous full-duplex multimodal interaction with one or more users.
- Track session context, active user surfaces, active action targets, current task state, and pending async work.
- Decide when to speak, listen, interrupt, stay silent, ask for clarification, or continue a background task.
- Use local model inference for the real-time loop whenever hardware and policy allow.
- Use advanced API or CLI models for slow deliberation, planning, research, coding, maintenance planning, and debugging.
- Start, monitor, summarize, and cancel async work without blocking live conversation.
- Handoff cluster-operation and system-maintenance work to the background agent by default.
- Start or request self-evolution when explicitly asked by the user, or when an architectural limitation prevents a bounded fix.

The main agent should not need to reason about low-level cluster mechanics for ordinary user tasks. It sees SIFS as its filesystem, receives useful cluster status summaries, and invokes tools with session-aware defaults. When placement matters, it can target a specific node, device, or SIFS subtree explicitly.

### Background Agent

The background agent is the maintenance and background-operations agent.

Responsibilities:

- Keep the cluster healthy.
- Handle background tasks that are not directly part of the immediate user conversation.
- Aggregate operational events and decide whether action is needed.
- Clean up old files and artifacts according to storage rules and policy.
- Investigate logged errors, degraded nodes, failed tasks, stale workspaces, rollout issues, and unexpected daemon behavior.
- Support main-agent operations by preparing environments, checking node health, running diagnostics, or scheduling safe maintenance.
- Apply minimal bug fixes through the normal self-evolution or rollout path when the scope is clearly operational and bounded.
- Escalate large functionality changes to self-evolution instead of making broad ad hoc changes.

The background agent should stay silent most of the time. Its default report target is the main agent, not the user. It may address the user directly only when there is a critical, time-sensitive, dangerous, or destructive issue, or when the main agent is malfunctioning and cannot mediate.

The background agent defaults to a powerful API-hosted or CLI-accessible coding LLM. It is event-woken rather than continuously active. A cluster-wide concurrency limit controls sessions for this agent; the default limit is one active background-agent session across the whole cluster. Multiple events should coalesce into one wake cycle so routine maintenance does not consume tokens continuously.

## Core Model

The core model is the model that drives Sinomo's real-time identity and user interaction loop.

Requirements:

- Full-duplex multimodal operation.
- Streaming audio input and speech output.
- Streaming visual input from cameras, screens, or encoded video events.
- Text input and text output.
- Barge-in and overlap handling, where listening and speaking can happen at the same time.
- Low-latency local deployment path.
- Trainable or adaptable local model path for Sinomo-specific behavior.
- Stable event-level integration with the runtime so the model can observe async task progress without waiting on tool calls.

Initial model choice:

- First version: MiniCPM-o 4.5 or the best compatible MiniCPM-o release available at implementation time.
- First local serving path: PyTorch. This should be the initial reference runtime for model loading, streaming behavior, interrupt behavior, and training or fine-tuning experiments.
- Later serving paths: llama.cpp-omni, Ollama, vLLM, SGLang, or other deployment backends can be added after the PyTorch path defines the runtime contract.
- API fallback: allowed when local inference is unavailable, overloaded, or below the required quality threshold, but this should be visible in runtime status and policy.

The main agent should treat the core model as the live interaction controller, not as the only reasoning engine. It can ask stronger or specialized models for deliberation while continuing to talk with users.

## Runtime Topology

```text
Users / Devices / Sessions
  <-> Sinomo main agent
      <-> Core full-duplex multimodal model
      <-> Session timeline and world-state
      <-> Async task registry
      <-> Typed transcript and evidence store
      <-> Deliberation model gateway
      <-> Deliberation harness
          <-> Context assembler and compactor
          <-> Tool registry and executor
          <-> Provider adapters through sinomo-lm
      <-> Cluster-aware tool/action gateway
      <-> SIFS filesystem view
      <-> Background handoff queue

Cluster events / health / storage / logs
  -> Background wake aggregator
  -> Background agent session
  -> Maintenance actions, reports, or self-evolution jobs
```

The main agent and background agent are logical roles. They may run on the same node in a single-machine deployment, but their responsibilities, wake policies, permissions, and reporting paths should remain separate.

## Real-Time Interaction Model

Traditional LLM tool loops block on a request-response cycle. Sinomo should instead use an event-driven, real-time loop:

1. Perception, user input, tool progress, cluster health, and async worker updates enter the session timeline as events.
2. The main agent continuously updates an interaction state: speaking, listening, waiting, delegating, recovering, or idle.
3. Slow work creates async task records and returns a handle immediately.
4. Partial progress, completion, failure, and cancellation arrive as new events.
5. The main agent decides whether each event should change the live conversation, update memory silently, wake the background agent, or trigger self-evolution.

Async task status can be injected into the live model stream as steering. The default immediate response after starting a background task is a short hint that the task is running asynchronously and will report back later. The runtime injects this hint right after the ongoing tool call or output stream boundary instead of waiting for the original turn to complete. If MiniCPM-o's native interrupt mechanism works reliably for tool-call progress, the runtime may use that path for lower-latency background-task updates.

The main agent must be trainable because most language models are optimized for turn-based tool calls, not concurrent full-duplex interaction with long-running background work. Runtime traces should explicitly capture interruption, overlap, task progress, delayed results, and user corrections so future local training can teach the model this interaction style.

## Async Work and Deliberation

Advanced models from APIs and CLIs are used for deliberate work that does not belong in the core real-time loop.

Examples:

- Planning complex multi-step tasks.
- Internet research and synthesis.
- Coding tasks.
- Maintenance and cleanup planning.
- Debugging Sinomo itself after crashes, unexpected behavior, regressions, or confusing user-visible failures.
- Reviewing proposed self-evolution candidates.
- Generating tests, diagnostics, and reproduction steps.

### Deliberation Harness

Async model work should run through a reusable deliberation harness rather than bespoke prompt calls. The first implementation can live in `sinomo-agent` for velocity, but the stable contracts should stay compatible with `sinomo-lm`, `sinomo-events`, `sinomo-deliberation`, and the self-evolution crates.

The harness loop is intentionally small:

```text
assemble turn context
  -> render provider request
  -> stream model events
  -> persist assistant items
  -> execute requested tools through runtime policy
  -> persist tool results as evidence
  -> compact, retry, or continue if needed
  -> finish with a final answer and evidence summary
```

Core harness responsibilities:

- Maintain a typed transcript and evidence view, not only a model-visible text transcript.
- Normalize provider output into provider-neutral stream events for assistant text, reasoning summaries, tool-call deltas, usage, finish reasons, errors, and aborts.
- Keep model-visible tool schemas separate from runtime tool handlers.
- Run every tool through policy, sandbox, scope, and output-size checks before execution.
- Return tool results to the model as continuation context when another step is needed.
- Preserve enough evidence for replay, compaction, user explanation, training, and self-evolution review.
- Stop on final answer, budget exhaustion, explicit cancellation, or a harness-level policy decision.

OpenAI Responses should be the first direct API renderer for the deliberation harness because it maps well to typed text, reasoning, tool-call, and tool-result items. The durable Sinomo transcript must remain provider-neutral so Chat Completions-compatible local servers, Anthropic, Google, process-backed coding agents, and smaller interactive models can be added later without changing the core session contract.

Async work should use the standard task-call schema, with durable task records behind each call:

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

The immediate return value follows normal task-call shape but marks the task as backgrounded and includes the stream-injection hint to present to the main agent. Completion and progress messages are delivered later through task events. No new external call shape is required for the language model: from the model's perspective these remain normal tool calls. The runtime guarantee is that a later injected response will arrive when progress, completion, failure, or cancellation needs attention.

Recommended injected response shape:

```json
{
  "kind": "async_task_update",
  "task_id": "task_...",
  "status": "running|progress|completed|failed|cancelled",
  "summary": "Short natural-language update for the live agent.",
  "needs_user_attention": false,
  "result_ref": "sifs:/services/tasks/task_.../result.json",
  "next_action_hint": "continue_silently|tell_user|ask_user|handoff_to_background|inspect_result"
}
```

The `summary` should be safe to inject into a live stream. Larger outputs, logs, files, or artifacts should be referenced through `result_ref` instead of being inlined. `next_action_hint` is advisory steering, not authority; the main agent still decides how to respond in context.

The main agent should be able to continue speaking with the user while these tasks run. When a result arrives, it is just another event in the session timeline and can be summarized, ignored, acted on, or handed to the background agent.

## Transcript, Context, and Compaction

The agent system should define the transcript before it defines provider payloads. Provider requests are rendered from Sinomo transcript state, not the other way around.

Initial transcript item families:

- user messages;
- assistant text and optional reasoning summaries;
- tool calls and tool results;
- async task creation, progress, completion, failure, and cancellation;
- context-injection records;
- model usage and provider diagnostics;
- patch, diff, snapshot, and verification records;
- errors, retries, aborts, and policy denials;
- compaction summaries and retained-tail markers.

Every deliberation turn should be assembled from durable state into a `TurnContext`:

```text
TurnContext
  session_id
  active agent role
  provider and model
  stable system/base instructions
  transcript view
  workspace and SIFS facts
  loaded instruction files
  evolution job facts when relevant
  available tool specs
  budget estimates
  omitted-context report
```

The context assembler should emit a context assembly report for every turn. The report should list included files, summaries, memory fragments, tool schemas, token or size estimates, omitted items, and omission reasons. This report is evidence; it is also the first place to debug context-packing failures.

Compaction must be explicit typed state. A compaction event should include:

- reason and trigger threshold;
- summary text;
- first retained item or event;
- token or size estimate before compaction;
- retained-tail size;
- files read and modified;
- current objective and latest user request;
- active plan, completed work, blockers, and next actions;
- verification state and open risks;
- policy constraints that must survive the summary.

Compaction summaries are reference material for future model turns, not authority. Runtime policy, tool state, verification state, and rollout decisions remain structured state outside the summary.

## Cluster-Aware Tooling

All environment interaction runs on top of the Sinomo cluster infrastructure. This does not require distributed model inference. It means tool execution, filesystem access, device control, process execution, diagnostics, rollout, and storage work are cluster-aware by default.

Main-agent view:

- The default filesystem root is SIFS.
- Session context supplies default node, user, workspace, and device targets.
- The main agent can start, inspect, and cancel background work without blocking live interaction.
- The main agent can hand off cluster-health, SIFS file inspection, cleanup, rollout, and maintenance work to the background agent.
- Direct `cluster_status`, `read_sifs`, `write_sifs`, and `list_sifs` tools are not part of the main-agent default tool set.
- The background-agent default tool set includes direct cluster status and SIFS read/write/list tools.
- The ordinary model context includes only basic cluster overview information such as available services and endpoint information.

Tool gateway responsibilities:

- Resolve session defaults into concrete node/device/workspace targets.
- Check cluster membership and orphan guards before distributed work.
- Select model-visible tool schemas from the runtime registry according to agent role, trust ring, provider capability, workspace scope, and policy.
- Validate model tool arguments before execution.
- Enforce node-local policy and sandbox limits.
- Route actions to the node closest to the relevant device or data.
- Return async handles for slow calls.
- Attach provenance, node identity, policy decision, and evidence handles to every result.
- Truncate large model-visible output while preserving full-output handles for audit and later inspection.
- Prefer structured machine-readable output over terminal-only text.

Cluster-operation tasks should be handed to the background agent unless they are a direct, immediate part of the user's task and safe for the main agent to drive. This keeps cluster details mostly transparent to Sinomo's real-time persona while still preserving explicit control when placement matters.

The initial deliberation tool set should stay small and auditable:

- `read_file`
- `list_files`
- `search`
- `apply_patch`
- `run_command`
- `git_status`
- `git_diff`
- `format`
- `test`
- `request_user_input`

Deferred tool discovery can be added later when the cluster has enough tools that exposing every schema wastes context. Until then, direct visibility plus strict runtime policy is simpler and easier to verify.

## Background Wake and Event Aggregation

The background agent is woken by events, not by polling an expensive model loop.

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

- Events are queued durably and grouped by family, affected node, affected service, workspace, and time window.
- Normal events wake the background agent only once per configurable interval. The default interval is once per day.
- Emergency events, such as logged errors or dangerous policy incidents, wake the background agent immediately.
- A wake cycle receives a queued incident bundle, not one isolated event.
- Cluster-wide background concurrency defaults to one active session.
- New non-emergency events remain queued while the current background-agent session is running and are considered in the next wake cycle.
- Emergency events that arrive during an active background-agent session are queued after the current emergency bundle and do not start parallel sessions unless policy explicitly permits it.
- The event bundle is written to a temporary file instead of being injected directly into the model context. The background agent receives the file path and uses file tools to inspect, filter, summarize, and act on the queued events.
- The temporary file format is LM-friendly JSON: a top-level list of event objects. Sinomo Object remains appropriate for internal storage and transport, but JSON is the handoff format for model-facing event review.
- Token budgets and wall-clock budgets are explicit per wake cycle.
- The background agent writes a concise report to the main agent unless escalation rules require direct user notification.

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

When local models are used, Sinomo should be able to train or adapt them from operation data. Data collection is therefore part of the architecture, not an afterthought.

Collected data classes:

- User interaction streams: text, audio transcripts, speech timing, barge-in points, selected visual summaries, and explicit user corrections.
- Agent decisions: when Sinomo spoke, stayed silent, interrupted, delegated, asked a question, used a tool, or handed off to the background.
- Async task traces: task creation, progress, delayed results, cancellations, and user satisfaction signals.
- Tool traces: structured inputs, outputs, errors, node placement, duration, and policy decisions.
- Deliberation harness traces: context assembly reports, provider request metadata, normalized stream events, tool-call continuations, compaction events, retries, and final answer evidence.
- Cluster traces: node health, rollout state, DFS issues, daemon errors, and recovery actions.
- Self-evolution traces: issue detection, proposal, implementation, evaluation, rollout, rollback, and postmortem outcomes.

Training data requirements:

- Collection defaults to on for every data class, including audio and video, because local traces are critical for training the real-time multimodal agent. Users, devices, sessions, and data classes must still support explicit opt-out.
- Consent, privacy, retention policy, and opt-out state must be explicit per user, session, device, and data class.
- Raw sensitive media should stay local by default. Redacted summaries, embeddings, or clips can be derived for training according to policy.
- Training examples should preserve concurrency and timing information, not only final text turns.
- Bad outcomes and corrected behavior should be kept as high-value training and evaluation material.
- Data used for training should have provenance, policy labels, and deletion hooks.
- Local fine-tuning, adapter training, preference tuning, and replay-based evaluation should be separate pipeline stages.

The first trainable path can start with adapter-style tuning or supervised fine-tuning on curated traces. Later versions can add preference optimization, replay-driven evaluation, and targeted training for real-time task-state management.

## Self-Evolution

Both agents can start and monitor self-evolution tasks, but they should do so for different reasons.

Main-agent triggers:

- The user explicitly asks Sinomo to evolve.
- A user-facing task repeatedly fails for architectural reasons.
- The main agent lacks a required tool, skill, or runtime behavior.
- The interaction model cannot represent the user's workflow cleanly.

Background-agent triggers:

- A system issue cannot be fixed by bounded maintenance.
- A recurring cluster, DFS, rollout, daemon, or policy failure points to a design flaw.
- A minimal operational bug fix is insufficient or would be brittle.
- A maintenance task reveals missing observability, missing tooling, or unsafe manual steps.

Self-evolution handoff rules:

- Small operational repairs from the background agent go directly to approval. If approved, they go directly to rollout. This path is intended for quick, narrow system fixes and must remain bounded.
- Until Sinomo has an autonomous approval system, approval for quick background-driven operational fixes is delegated to a human. This should use the same approval and rollout surfaces as self-evolution.
- Large functionality changes use the self-evolution architecture.
- Evolution jobs must have bounded scope, write-set ownership, evaluation gates, rollback policy, and provenance.
- Mutation packets and implementation runs should be ordinary deliberation-harness sessions with extra job, scope, verification, and rollback context.
- Verification commands, patch snapshots, changed-file summaries, tool results, and final model claims should be stored as typed candidate evidence.
- A candidate cannot be treated as successful only because the model says it is done. Verification evidence and policy decisions remain authoritative.
- The main agent can explain and negotiate user-facing evolution goals.
- The background agent can provide diagnostics, incident bundles, reproduction steps, and operational constraints.

## Permissions and Reporting

The main agent and background agent need different defaults.

| Capability | Main agent | Background agent |
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

Every action should carry the responsible agent role, initiating event, session or maintenance bundle, target node, and policy decision. This is needed for debugging, training, user explanation, and self-evolution evidence.

Tool and rollout permissions are runtime decisions. System prompts should tell models what to expect, but actual permission checks happen before tool schema exposure, file reads, file writes, command execution, network access, memory writes, candidate approval, and rollout activation.

## Remaining Refinements

- Define the exact JSON schemas for async task updates and background event-bundle files once the event and task crates stabilize.
- Define the stable typed transcript and evidence item schema shared by `sinomo-agent`, `sinomo-deliberation`, `sinomo-lm`, and self-evolution jobs.
- Define the normalized provider stream event enum and the first OpenAI Responses renderer/parser.
- Define the context assembly report schema and compaction event schema.
- Define the policy metadata required by tool descriptors before broadening the tool registry.
- Define the user-facing controls for per-user, per-device, per-session, and per-data-class collection opt-out.
- Define the human approval UI or CLI flow shared by quick background fixes and self-evolution rollout approval.
