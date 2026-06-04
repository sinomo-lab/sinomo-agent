# Coding Agent Improvement Plan

## Purpose

Improve Sinomo's coding-agent path until it is reliable enough to be the default mutation worker for self-evolution and competitive with purpose-built coding agents on real repository work.

The goal is not to fork a second agent system. The generic `sinomo-agent::Agent` loop remains the provider-neutral runtime for sessions, tool execution, transcript state, compaction, policy, traces, and evidence. Coding-specific behavior should live in a controller layer above that loop and feed typed state back into the same agent runtime.

The coding agent is the first serious implementation target for Sinomo's async deliberation task substrate. The implementation is deliberately scoped to repository mutation because it is a well-studied and measurable area, but the shared contracts should fit the larger Sinomo architecture: real-time interaction stays unblocked, work can be represented as async tasks, events are distributed-runtime friendly, workspaces can be SIFS-compatible, and policy/tool execution remain runtime authority rather than prompt convention.

This does not mean `sinomo-agent` should become tightly coupled to the full Sinomo daemon, cluster, DFS, rollout, or self-evolution stack. From a software engineering perspective, `sinomo-agent` should remain a standalone, modular agent runtime that can run locally with file-backed stores and local tools. Sinomo-specific integration should happen through narrow traits, adapters, storage backends, event sinks, task handles, workspace resolvers, and tool gateway layers supplied by the host runtime.

Research against the Codex Rust implementation under `tmp/harness-research/codex/codex-rs` points to a two-part target: first make the generic agent runtime a real session/task/tool event system, then add a Sinomo-specific coding controller that improves stop policy, validation classification, and self-evolution evidence.

Public SWE-bench results are useful as a regression signal, not the final authority. The durable target is repeatable coding behavior inside Sinomo's own evolution loop: small diffs, clear validation evidence, bounded extra turns after a correct patch, and enough trace structure to turn failed runs into future regression cases.

## Baseline

Current strengths:

- `sinomo-agent` already has a provider-neutral `Agent` loop, dynamic tool registry, session store, trace store, context assembly, compaction, typed tool evidence, and local workspace tools.
- The local workspace tools cover the first coding loop: `read_file`, `search`, `apply_patch`, `run_command`, command sessions, and user-input requests.
- Tool results carry model-visible summaries plus durable evidence, including patch snapshots, changed files, diff summaries, command output diagnostics, and artifact refs for truncated output.
- `sinomo-benchmarks` can run traced SWE-bench experiments and compare Sinomo behavior against external coding-agent baselines.
- Self-evolution already routes normal mutation through `sinomo-agent` and records agent run evidence in candidate bundles.

Current gaps:

- The loop still relies too much on model self-control for when to stop. A correct patch can be followed by unnecessary validation and reread turns.
- Validation state is present in tool output, but it is not yet reduced into a typed coding state machine that can distinguish patch-relevant failures from missing local dependencies or blocked environment setup.
- Provider output discipline is uneven. A strict generic JSON decision contract is useful as a fallback, but some models need provider-specific rendering, native tool calls, or deterministic repair before consuming repeated full model turns.
- The session runtime is still weaker than purpose-built coding agents. Codex uses an operation/event session handle, one active interruptible task, structured turn lifecycle events, and provider-native tool calls instead of a prompt-only controller.
- Tool execution policy is not centralized enough. Coding reliability needs one place for tool visibility, approval, sandbox selection, cancellation, retry, and model-visible output shaping.
- Repository intelligence is too thin. The model must shell out or search manually for common operations such as file-tree discovery, changed-file inspection, diff review, symbol lookup, and project check selection.
- Editing ergonomics still favor full patches. Narrow replace-by-range or replace-exact operations would reduce patch parser friction for small changes.
- Context growth is still high for repeated command, diff, and file-read output. The model needs concise coding-state updates while the full evidence remains durable outside the prompt.
- Benchmark runs can be invalidated by stale local runner binaries or missing host dependencies. The harness should report those as harness or environment failures, not model failures.

## Codex Reference Lessons

Codex's current Rust design is useful as a runtime reference, not as a complete Sinomo architecture.

Important pieces to borrow:

- A live session boundary accepts structured operations and emits structured events. UI, RPC, approval responses, user steering, shell stdin, compaction, review, and shutdown use the same path.
- Each session has at most one active task. Starting a new task aborts or replaces the previous task through an explicit cancellation and lifecycle path.
- A regular coding turn is provider-native: send transcript plus tool specs to the model, stream response items, execute tool calls, append tool outputs, and sample again until no follow-up is needed.
- Tools are assembled per turn from registry, policy, model capability, dynamic tools, and environment state. The model-visible tool list is a filtered view of the executable registry.
- Tool execution goes through a shared orchestrator for approval, sandbox selection, permission requests, denial handling, retry, cancellation, and post-tool hooks.
- Command output is captured as structured evidence with stdout, stderr, aggregated output, exit code, duration, timeout state, and truncation metadata. stderr must be part of the model-visible diagnostics.
- Patch application is a first-class edit tool with a documented grammar, verification, sandbox checks, streaming progress, and patch evidence. Shell-form patch commands can be intercepted and routed through the same implementation.
- Transcript history is normalized before prompting. Tool call/output pairs must stay balanced, unsupported media should be stripped, and large tool outputs should be truncated into durable artifact refs.
- Thread/session persistence is a separate store abstraction. Resume should not be baked into the agent loop.

Important pieces not to copy blindly:

- Codex is strongly optimized for OpenAI Responses semantics. Sinomo still needs provider-neutral rendering and local-provider support.
- Codex does not appear to have a first-class coding-state reducer that classifies validation status, patch relevance, or "extra turns after correct patch." Sinomo should add that controller because benchmark traces show this is a real failure mode.
- Codex exposes repository work mostly through shell, patch, and search-style tools. Sinomo should add higher-level repository tools where they reduce repeated context and fragile shell planning.

## OpenClaw And Hermes Reference Lessons

OpenClaw and Hermes are useful references for the generic agent system around the coding loop. They should influence Sinomo's session control plane, tool ecosystem, context plumbing, and provider adapters without replacing the Codex-style provider-native coding turn loop.

OpenClaw pieces to borrow:

- ACP-style runtime/control-plane separation: a backend-neutral runtime handle, explicit session creation, event-stream plus terminal-result turn execution, cancellation, close, status, backend selection, and backend failover.
- Per-session actor queues and active-turn tracking. A session should have ordered operations, bounded pending work, one active turn, explicit timeout cleanup, and observable cancellation state.
- Plugin contract discipline: static catalog contracts declare exported tools, providers, middleware, and surfaces; runtime registration must match those contracts. This maps well to Sinomo module registration where rollout, policy, telemetry, and inspection need pure data, while native code supplies executable hooks.
- A typed context engine boundary with lifecycle methods such as bootstrap, ingest, assemble, compact, after-turn update, prompt-cache hints, and disposal.

OpenClaw pieces to avoid:

- The embedded-agent-runner shape is too monolithic for Sinomo. It mixes auth fallback, UI channels, workspace setup, runtime plugin setup, context, retries, compaction, failover, and product policy in one runner. Sinomo should borrow the lifecycle concerns, not that structure.

Hermes pieces to borrow:

- Provider-specific normalization and repair should live in adapters: transcript role repair, tool-call/tool-result balancing, invalid tool-name repair, malformed JSON argument recovery, provider-incompatible field stripping, empty-response nudges, and fallback classification.
- Tool registration should have scoped toolsets, availability checks, cached status, dynamic schema overrides, a generation counter, and explicit shadowing rules.
- Progressive tool disclosure should be first-class. Core coding tools stay visible; large plugin, module, MCP, cluster, or rarely used toolsets can be exposed through `tool_search`, `tool_describe`, and `tool_call`-style bridge tools that still route through the same orchestrator and policy.
- Tool result storage should have three budgets: per-tool visible output, per-result persisted evidence, and per-turn aggregate output. Large command logs, diffs, and file contents should spill to SIFS-backed evidence refs with concise model-visible previews.
- Provider metadata belongs in `sinomo-lm`: context window resolution, output-token caps, reasoning-effort options, vendor-specific options, base-URL scoped model caches, local endpoint probes, and normalized errors that distinguish prompt overflow from max-output-too-large.

Hermes pieces to avoid:

- The main conversation loop is intentionally defensive but too global and monolithic for Sinomo. UI printing, tool globals, memory updates, compression, provider quirks, and recovery paths should remain separate Rust subsystems with explicit interfaces.

## Architectural Fit Constraints

This plan scopes implementation to coding, but the shared runtime pieces must remain usable by Sinomo's real-time, distributed, always-on agent system.

Required constraints:

- A coding run is an async deliberation task, not a blocking live-core-model turn. The main agent should be able to start it, keep interacting with the user, receive progress events, summarize or ignore updates, steer the task, and cancel it.
- TUI, CLI, RPC, main-agent handoff, background-agent wake cycles, benchmarks, and self-evolution should observe the same task and event stream. They should not need separate stdout scraping, ad hoc polling, or special coding-only state files.
- Shared events should be envelope-ready for Sinomo's distributed event model. The first local implementation can fill local defaults, but event records should have room for session, task, agent role, origin node, target node, workspace, policy, privacy, salience, and evidence references.
- These distributed concepts should enter `sinomo-agent` as host-neutral metadata and extension points, not as direct dependencies on daemon, DFS, cluster, rollout, or self-evolution crates.
- The stable controller concept is a workspace reference, not a bare local path. The first implementation can resolve a `WorkspaceRef` to a `PathBuf`; SIFS roots, local mounts, owner nodes, replication/write policy, and candidate workspaces should be host-provided interpretations of that reference.
- Tool execution belongs behind a host-provided tool gateway. Local execution is the first placement strategy, but tool calls should carry enough metadata for later node selection by workspace owner, action target, OS/architecture, sandbox class, dependency availability, and data locality.
- Provider adapter improvements apply to the async deliberation harness. They must not force the real-time full-duplex core model into a coding-style JSON or tool-turn shape.
- The tool registry should be able to consume module catalog surfaces and loader state, but it must not become a second module system. Module catalog data describes what exists, loader state says what is active, and the model-visible tool projection is assembled from those plus session policy by an integration layer.
- The coding controller produces evidence and recommendations. It does not approve candidates, activate modules, roll out changes, or decide live runtime authority. Self-evolution promotion remains controlled by verification, policy, provenance, and rollout gates.
- Standalone use must remain first-class: the same core agent runtime should run in a local CLI benchmark, a TUI chat session, or a unit test with in-memory stores without requiring a cluster, SIFS, module loader, daemon, or self-evolution service.

Implementation guardrails:

- `sinomo-agent` owns portable primitives: session operations and events, transcript state, trace and evidence contracts, context assembly boundaries, tool registry contracts, provider adapter boundaries, and local default stores or tools.
- Higher-level Sinomo runtime crates own integration: distributed task placement, SIFS workspace resolution, rollout and self-evolution policy, module catalog projection, daemon/RPC publication, and cluster persistence.
- Any coding-agent API added to `sinomo-agent` should be usable from a standalone binary or unit test with in-memory or file-backed implementations. If the API needs Sinomo-specific behavior, the agent side should define a narrow trait or serializable data contract and the host runtime should provide the adapter.
- New `sinomo-agent` dependencies should be treated as architecture decisions. The default answer should be data contracts and injected services, not direct imports from daemon, DFS, rollout, module-loader, or self-evolution crates.

## Architecture

Use the coding work as the first concrete implementation of the async deliberation path. In the full Sinomo runtime the path is embedded like this:

```text
session timeline / world events
  -> async task registry
  -> generic deliberation session runtime
  -> session control plane
  -> provider-native turn loop
  -> tool registry/router/orchestrator
  -> structured events, transcript, evidence, and persistence
  -> coding run controller
  -> self-evolution candidate bundle
```

In standalone embedding, the same `generic deliberation session runtime -> session control plane -> provider-native turn loop -> tool registry/router/orchestrator -> structured events, transcript, evidence, and persistence -> coding run controller` path should work with local task handles, local workspace refs, file-backed artifact storage, and a local tool gateway.

The generic runtime should expose a session handle shaped around operations and events:

```rust
pub enum AgentOp {
    UserInput { text: String },
    ToolApproval { request_id: String, decision: ToolApprovalDecision },
    ToolInput { tool_call_id: String, input: String },
    Compact,
    Interrupt,
    Abort,
    Shutdown,
}

pub struct AgentEventEnvelope {
    pub event_id: String,
    pub session_id: String,
    pub task_id: Option<String>,
    pub agent_role: AgentRole,
    pub origin_node: Option<String>,
    pub target_node: Option<String>,
    pub workspace: Option<WorkspaceRef>,
    pub privacy_class: Option<String>,
    pub salience: Option<String>,
    pub evidence_refs: Vec<ArtifactRef>,
    pub policy_decision: Option<AgentPolicyDecision>,
    pub payload: AgentEventPayload,
}

pub enum AgentEventPayload {
    TurnStarted { turn_id: String },
    ModelDelta { text: String },
    ToolCallStarted { call_id: String, tool_name: String },
    ToolCallOutput { call_id: String, output: AgentToolOutput },
    PatchUpdated { call_id: String, summary: String },
    TurnCompleted { outcome: AgentTurnOutcome },
    TurnAborted { reason: String },
}
```

The exact enum names can change during implementation. The invariant should not: TUI, CLI, RPC, self-evolution, and benchmark harnesses should observe the same structured event stream instead of scraping stdout, stderr, or ad hoc text.

The session control plane should be shaped around durable operations and observable turn state:

- a durable task record for async coding work, including owner role, status, progress, cancellation policy, placement, and result refs;
- one active turn per session;
- a per-session operation queue;
- separate event streaming and terminal result handling;
- explicit interrupt, abort, timeout, and cleanup paths;
- backend runtime selection and failover that is visible in events;
- status snapshots that can be consumed by TUI, RPC, tests, and self-evolution.

This control plane should remain below coding policy. It manages session lifecycle and provider/tool execution; it does not decide whether a Rust test failure is patch-relevant or whether a self-evolution candidate is acceptable.

Add a coding controller above that generic session runtime.

Suggested shape:

```rust
pub struct CodingRunController {
    pub session: AgentSessionHandle,
    pub task: AgentTaskRef,
    pub workspace: WorkspaceRef,
    pub placement: AgentPlacementPolicy,
    pub policy: CodingRunPolicy,
}

pub struct WorkspaceRef {
    pub id: String,
    pub sifs_root: Option<String>,
    pub local_mount: Option<PathBuf>,
    pub owner_node: Option<String>,
    pub write_policy: WorkspaceWritePolicy,
}

pub struct CodingRunState {
    pub objective: String,
    pub allowed_paths: Vec<PathBuf>,
    pub changed_files: Vec<PathBuf>,
    pub current_diff_hash: Option<String>,
    pub current_diff_summary: Option<String>,
    pub patch_status: CodingPatchStatus,
    pub validation_attempts: Vec<CodingValidationAttempt>,
    pub validation_status: CodingValidationStatus,
    pub blocked_reason: Option<CodingBlockedReason>,
    pub outstanding_evidence: Vec<CodingEvidenceRequirement>,
    pub stop_recommendation: CodingStopDecision,
    pub open_risks: Vec<String>,
}
```

The controller should consume agent events, tool outcomes, command diagnostics, and `AgentItem::PatchSnapshot` records. It should then update `CodingRunState` and inject a concise typed state summary into the next agent turn.

The lower `Agent` should stay generic:

- it does not know about Python, Rust, SWE-bench, Git, or self-evolution semantics;
- it records provider-neutral transcript and evidence;
- it emits progress and completion events for the async task that owns the run;
- it executes tools through a registry/router/orchestrator boundary;
- it captures stdout and stderr as structured tool evidence, never as direct terminal writes;
- it supports provider-native tool calls when available and the JSON decision contract as a fallback;
- it remains usable for TUI chat, background agents, and non-coding workflows;
- it depends on host-neutral traits and data contracts for task state, event sinks, artifact storage, workspace resolution, and tool execution rather than importing higher-level Sinomo services directly.

The tool/module surface should have two projections:

- an executable registry containing handlers, runtime policy, availability checks, and optional host-provided module catalog/loader inputs;
- a model-visible projection assembled per turn from policy, toolsets, provider capability, environment state, and progressive disclosure.

The coding controller owns coding-specific decisions:

- whether a validation failure is patch-relevant, environment-blocked, inconclusive, or a command setup issue;
- whether another model turn is useful;
- whether to finish with a patch, finish blocked, request user input, or abort;
- which repository tools and validation plans should be offered for the current task.

## Control State

Introduce explicit state enums instead of encoding all control flow in prompt text.

Suggested validation status:

```rust
pub enum CodingValidationStatus {
    NotRun,
    Passed,
    FailedPatchRelevant,
    FailedEnvironment,
    FailedCommandSetup,
    TimedOut,
    Inconclusive,
}
```

Suggested stop decision:

```rust
pub enum CodingStopDecision {
    Continue,
    FinishWithPatch,
    FinishBlocked,
    RequestUserInput,
    Abort,
}
```

The first useful stop policy should be conservative:

- continue when no patch exists and no blocker is known;
- continue when validation fails in a way that points to the edited code;
- finish with patch when the diff is narrow, no patch-relevant validation failure exists, and the remaining validation failure is classified as missing dependencies or environment setup;
- finish blocked when the requested operation cannot be done without unavailable credentials, network, tools, or user input;
- request user input only when the controller cannot safely infer the next step from repository state and policy.

## Tooling Roadmap

Keep `run_command` language-neutral. Command classification belongs in the coding controller, not in the shell tool.

The command tool contract should be promoted to a durable structured output:

```rust
pub struct AgentCommandOutput {
    pub exit_code: Option<i32>,
    pub stdout_ref: Option<ArtifactRef>,
    pub stderr_ref: Option<ArtifactRef>,
    pub aggregated_output: String,
    pub duration_ms: u64,
    pub timed_out: bool,
    pub truncated: bool,
}
```

The model-visible summary should include exit status and a bounded aggregate of stdout plus stderr. Full output belongs in evidence artifacts.

Artifact refs should be storage-backend neutral. The first local implementation can write to the standard data-dir evidence layout; the contract should also support SIFS-backed refs so command logs, diffs, and candidate artifacts can move across nodes without changing the agent transcript.

Near-term tool additions:

- `changed_files`: return the workspace files changed by the current run and their status.
- `git_diff`: return a bounded diff summary and artifact ref for the full diff.
- `list_files`: list files by directory, glob, or repository-root filter with bounded output.
- `replace_exact`: replace an exact snippet in one file, with before/after evidence.
- `replace_range`: replace a line range in one file, with before/after evidence.

Later tool additions:

- symbol and reference search backed by language-specific indexes when available;
- project check discovery that proposes likely format, lint, unit, and focused test commands;
- structured test result parsing for common runners;
- dependency and environment probes that report missing tools without mutating the host.

`apply_patch` should remain available for multi-file edits. Its prompt and descriptor should describe the actual Sinomo patch grammar and examples, not call it a Codex patch. Narrow edit tools should reduce friction for one-line and small localized changes.

Tool runtime work:

- split tool descriptor, executable handler, model-visible schema, and runtime policy;
- add scoped toolsets such as core, coding, TUI, local-only, cluster, self-evolution, module, plugin, and MCP;
- maintain a generation counter for registry changes so sessions can refresh projections deterministically;
- add cached availability checks and dynamic schema overrides before a tool is exposed to a model;
- carry execution placement metadata: target node, workspace owner, OS/architecture, sandbox class, dependency requirements, data locality, and local-only or cluster-capable status;
- add per-tool parallelism, cancellation, and timeout metadata;
- route all tools through one orchestrator for approval, sandbox, retry, and post-tool evidence shaping;
- let TUI-only, cluster-only, self-evolution-only, and coding-only tools be extra registry layers over the shared runtime;
- support progressive disclosure by keeping core coding tools visible and deferring large plugin/module/MCP toolsets behind search/describe/call bridge tools that still use the same orchestrator and policy.

## Provider Adapters

Keep the existing JSON decision contract as the portable fallback, but add provider-specific adapters where they materially improve reliability.

Priority adapters:

- OpenAI/Codex Responses native tool-call rendering and streaming parse as a first-class path, not a wrapper around the JSON decision contract.
- DeepSeek-compatible output rendering or deterministic repair before the loop spends multiple turns on malformed structured output.
- Anthropic native tool calls once the provider path is needed for coding runs.

Provider adapter metrics should be part of every coding run:

- parse failures;
- repair attempts;
- native tool-call usage;
- fallback JSON usage;
- model-visible tokens spent on repair;
- final structured decision source.

The adapter boundary should preserve one provider-neutral transcript and event model. Native provider items can be stored as raw evidence, but session replay, compaction, TUI rendering, and self-evolution bundles should consume Sinomo events and `AgentItem` records.

This adapter work is for async deliberation sessions. The real-time full-duplex core model can consume task progress and world-state events without being forced into the same coding-oriented request/response and tool-call cadence.

Provider metadata should be centralized in `sinomo-lm`, not duplicated in the agent loop:

- context window and max-output resolution;
- reasoning effort and vendor-specific option schemas;
- base-URL scoped model metadata caches;
- local endpoint capability probes;
- normalized prompt-too-large, output-too-large, rate-limit, authentication, and malformed-response errors.

Adapters may repair provider-specific transcript and tool-call issues before spending another full model turn, but every repair attempt should be recorded as structured run metadata.

## Context And Evidence

The model-visible context should stay small while the evidence remains complete.

Implementation direction:

- after every tool batch, reduce the tool outputs into generic session state and `CodingRunState`;
- inject a concise "current coding state" message into the next turn;
- keep large diffs, command logs, and file contents as durable artifact refs;
- preserve exact patch snapshots and validation records in trace storage and candidate bundles;
- compact repeated read/diff/command results into durable facts instead of replaying them verbatim.

Add a reusable context engine boundary below the coding controller:

- `assemble` creates the model-facing context projection for the next turn;
- `after_turn` ingests model output, tool output, usage, and evidence refs;
- `should_compact` and `compact` decide when to reduce prompt history;
- `update_model` reacts to provider/model switches and context-window metadata;
- optional retrieval tools can query durable session facts without replaying all previous output.

Context assembly should treat async task state and distributed workspace facts as first-class inputs: task status, latest progress, owner role, current placement, workspace refs, active write policy, SIFS roots, local mount hints, and omitted evidence refs.

Transcript invariants:

- every model tool call recorded in history must have a corresponding tool output, even if the turn was aborted;
- every tool output sent to the model must correspond to a recorded tool call;
- command output must preserve stderr and stdout separately in evidence while giving the model a bounded aggregate;
- compaction may change the model-facing projection, but raw transcript and evidence must remain append-only.

This should reduce token use and reduce repeated validation turns, especially for models that otherwise reread the same file or diff after a correct patch.

## Benchmark And Regression Plan

Fix harness freshness first. A benchmark command must rebuild the Sinomo coding-agent runner when source changes, or require an explicit binary path and record that binary's provenance.

Minimum regression suite:

- one simple localized SWE-bench instance that exercises patching and stop policy;
- one missing-dependency validation case that must be classified as environment-blocked;
- one multi-file edit case;
- one malformed model-output case that exercises provider repair;
- one structured-command-output case where diagnostics are on stderr and must still be visible to the model;
- one session interruption/resume case that verifies no orphan tool calls or direct terminal output leaks;
- one active-turn cancellation and timeout case that verifies cleanup and terminal events;
- one async task case where the main session can continue while a coding task emits progress and completion events;
- one steering/cancellation case that targets the correct task when multiple async tasks exist in the same session timeline;
- one workspace-ref case that starts with a local path but records SIFS-compatible evidence and workspace metadata;
- one placement/provenance case that records node, workspace, sandbox, and policy metadata for a local command;
- one progressive tool disclosure case that verifies scoped bridge tools cannot bypass policy;
- one module-tool projection case that verifies model-visible tools derive from active catalog/loader state rather than ambient discovery;
- one large-output case that verifies command logs or diffs spill to evidence refs with bounded model-visible previews;
- one provider-context case that verifies prompt-overflow and output-overflow are classified differently;
- one self-evolution candidate mutation case using the same controller.

Every report should include:

- resolved status;
- model turns;
- tool calls and tool results;
- first successful patch turn;
- extra turns after first successful patch;
- validation status;
- stop decision;
- parse repair attempts;
- token usage;
- stale binary or harness freshness failures;
- session event counts and orphan tool call/output counts.

Acceptance target for the first simple localized case: once a correct patch is present and validation is only environment-blocked, DeepSeek-like runs should stop within a small bounded number of turns instead of spending most of the budget on rereads and repeated validation.

## Self-Evolution Integration

The coding controller should become the canonical mutation worker for self-evolution.

Candidate bundles should include:

- `agent-run.json`;
- `agent-summary.json`;
- `agent-events.jsonl`;
- `coding-run-state.json`;
- patch snapshots and changed-file records;
- validation records with typed statuses;
- benchmark deltas when a benchmark gate runs.

The coding controller may recommend `ready`, `blocked`, or `needs-more-evidence`, but those are evidence summaries, not promotion authority. Candidate success and rollout eligibility remain decisions made by evaluation, policy, provenance, registry, and rollout gates.

When a run fails, the durable evidence should be good enough to create a follow-up evolution task that targets the actual failure class: provider parsing, tool ergonomics, validation classification, context bloat, stale runner, or model capability.

## First Implementation Slice

1. Fix benchmark runner freshness so local SWE-bench retries cannot silently reuse a stale `sinomo-swebench-agent` binary.
2. Add the first `AgentOp` / `AgentEvent` session surface over the existing `AgentSession` methods without breaking the current one-shot `Agent::run` helper.
3. Add local-first `AgentTaskRef`, `WorkspaceRef`, placement metadata, and event-envelope fields so the first coding implementation already records task, role, node, workspace, policy, and evidence provenance through host-neutral data contracts.
4. Add a session control plane with per-session ordering, one active turn, durable task status, progress events, status snapshots, cancellation, timeout cleanup, and separate event-stream/result handling.
5. Promote command outcomes to structured stdout, stderr, aggregate output, exit status, timeout, truncation, and storage-backend-neutral artifact refs.
6. Add a shared tool orchestrator boundary for visibility, policy, sandbox, placement, cancellation, timeout, and post-tool evidence shaping.
7. Split the tool registry into executable handlers and per-turn model-visible projections with scoped toolsets, availability checks, generation tracking, dynamic schema overrides, and active module catalog/loader input.
8. Add progressive tool disclosure for large module/plugin/MCP toolsets through search/describe/call bridge tools routed through the same orchestrator.
9. Move provider metadata resolution and normalized provider errors into `sinomo-lm`, including context window, max-output, reasoning effort, vendor-specific options, and local endpoint probes.
10. Add OpenAI/Codex Responses native tool-call rendering and streaming parse for coding runs, while keeping JSON decisions as fallback.
11. Add `CodingRunState`, validation status, blocked reason, and stop decision types.
12. Add a state reducer that consumes patch snapshots, command diagnostics, task progress, workspace metadata, and tool outcomes from the session event stream.
13. Add `changed_files` and `git_diff` tools to the local workspace registry.
14. Add the first stop policy: finish when a narrow patch exists, no patch-relevant validation failure exists, and remaining validation is environment-blocked.
15. Add fake-model regression tests for structured command output, simple localized patch flow, stop decision, session interruption, async progress, workspace refs, progressive disclosure, and provider error classification.
16. Rerun the current DeepSeek and Codex benchmark cases and record before/after turn, tool, event, and token deltas.

This batch keeps the core architecture stable: the generic agent session runtime becomes reusable across TUI, RPC, benchmarks, and self-evolution, while coding-specific control becomes explicit, testable, and traceable.
