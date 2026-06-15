# Coding Agent Improvement Plan

## Purpose

Improve Sinomo's coding-agent path until it is reliable enough to be the default mutation worker for self-evolution and competitive with purpose-built coding agents on real repository work.

The goal is not to fork a second agent system. The generic `sinomo-agent::Agent` loop remains the provider-neutral runtime for sessions, tool execution, transcript state, compaction, tool-surface availability, traces, and evidence. Coding-specific behavior should live in a controller layer above that loop and feed typed state back into the same agent runtime.

The coding agent is the first serious implementation target for Sinomo's async deliberation task substrate. The implementation is deliberately scoped to repository mutation because it is a well-studied and measurable area, but the shared contracts should fit the larger Sinomo architecture: real-time interaction stays unblocked, work can be represented as async tasks, events are distributed-runtime friendly, workspaces can be SIFS-compatible, and tool visibility/execution remain runtime authority rather than prompt convention.

This does not mean `sinomo-agent` should become tightly coupled to the full Sinomo daemon, cluster, DFS, rollout, or self-evolution stack. From a software engineering perspective, `sinomo-agent` should remain a standalone, modular agent runtime that can run locally with file-backed stores and local tools. Sinomo-specific integration should happen through narrow traits, adapters, storage backends, event sinks, task handles, workspace resolvers, and tool gateway layers supplied by the host runtime.

Research against the Codex Rust implementation under `tmp/harness-research/codex/codex-rs` points to a two-part target: first make the generic agent runtime a real session/task/tool event system, then add a Sinomo-specific coding controller that improves stop policy, validation classification, and self-evolution evidence.

Public SWE-bench results are useful as a regression signal, not the final authority. The durable target is repeatable coding behavior inside Sinomo's own evolution loop: small diffs, clear validation evidence, bounded extra turns after a correct patch, and enough trace structure to turn failed runs into future regression cases.

## Baseline

Current strengths:

- `sinomo-agent` already has a provider-neutral `Agent` loop, dynamic tool registry, session store, trace store, context assembly, compaction, typed tool evidence, and local workspace tools.
- `AgentDefinition` is now the package artifact: it carries package version, provenance, prompt, tool surface, skill refs, context strategy, compaction strategy, provider preferences, runtime limits, and output format, with `sinomo agent inspect`, `validate`, `export`, and `import` exposing CLI package inspection and exchange paths.
- Hermes-style progressive skill disclosure now has a first agent-side path: `skill_search`, `skill_describe`, and `skill_activate` expose package skill refs and module declarative `Skill` resources without loading all skill text into the prompt. `skill_activate` currently returns the explicit blocked host-runtime activation contract rather than becoming a prompt-level permission switch.
- The local workspace tools cover the first coding loop: `read_file`, `search`, `apply_patch`, `run_command`, command sessions, and user-input requests.
- Tool results carry model-visible summaries plus durable evidence, including patch snapshots, changed files, diff summaries, command output diagnostics, typed command failure kinds, and artifact refs for truncated output.
- `sinomo-benchmarks` can run traced SWE-bench experiments and compare Sinomo behavior against external coding-agent baselines.
- Self-evolution already routes normal mutation through `sinomo-agent` and records agent run evidence in candidate bundles.

Current gaps:

- The loop still relies too much on model self-control for when to stop. A correct patch can be followed by unnecessary validation and reread turns.
- Validation state now has a first typed reducer and attempt-level command failure kinds for timeouts, setup errors, missing commands or dependencies, network failures, permission failures, environment blockers, and patch-relevant failures. The remaining work is calibration against real benchmark traces and live cluster runs.
- Provider output discipline is uneven. A strict generic JSON decision contract is useful as a fallback, and native-tool protocol hints now come from `sinomo-lm`, but broader provider-specific rendering, native tool calls, and deterministic repair still need to stay in provider adapters instead of leaking provider/model checks into `sinomo-agent`.
- The session runtime now has a first local operation/event control plane and interruptible worker handle, but host integrations still need to adopt it. TUI, RPC, benchmarks, and self-evolution should use the same async runtime rather than calling blocking turns directly.
- Tool execution orchestration is not centralized enough. Coding reliability needs one place for visible tool projection, sandbox selection, cancellation, retry, and model-visible output shaping.
- Skill activation semantics are now explicit in search/describe output and in the `skill_activate` bridge: disclosed skills report `activation.mode = "host_runtime"` and `available = false`, while activation requests return a typed blocked result until a host-provided executor is attached.
- Repository interaction should stay tool-native: the agent should use shell commands or installed repo CLI/tool modules, while generic command evidence remains responsible for stdout/stderr, exit status, duration, timeout state, artifact refs, failure classification, and concise summaries.
- Editing ergonomics now have a first structured small-edit path through `apply_patch` `changes`: whole-file replacements still work, while `replace_exact` and one-based `replace_range` reduce patch parser friction for small changes without adding separate repo-editing tools.
- Context growth is still high for repeated command, diff, and file-read output. The model needs concise coding-state updates while the full evidence remains durable outside the prompt.
- Benchmark freshness, host dependency evidence, and agent-subprocess failures now have first harness handling: the default SWE-bench runner rebuilds the local agent binary, records explicit-binary provenance, keeps the main pass uncapped unless `--max-steps` is supplied, writes a plan/summary `host_dependencies` probe for command availability, Python imports, Docker reachability, agent-binary status, and blocking host issues, stops real run modes before launching subprocesses when selected host dependencies are blocked, and writes per-instance `harness-blocked` reports when the agent subprocess fails before producing a prediction. Remaining work is calibration against live benchmark traces and richer trace-derived regression cases.

## Codex Reference Lessons

Codex's current Rust design is useful as a runtime reference, not as a complete Sinomo architecture.

Important pieces to borrow:

- A live session boundary accepts structured operations and emits structured events. UI, RPC, user steering, shell stdin, compaction, review, and shutdown use the same path.
- Each session has at most one active task. Starting a new task aborts or replaces the previous task through an explicit cancellation and lifecycle path.
- A regular coding turn is provider-native: send transcript plus tool specs to the model, stream response items, execute tool calls, append tool outputs, and sample again until no follow-up is needed.
- Tools are assembled per turn from the registry, visible tool surface, model capability, dynamic tools, and environment state. The model-visible tool list is a filtered view of the executable registry.
- Tool execution goes through a shared orchestrator for visibility projection, sandbox selection, hidden-tool guarding, retry, cancellation, and post-tool hooks.
- Command output is captured as structured evidence with stdout, stderr, aggregated output, exit code, duration, timeout state, and truncation metadata. stderr must be part of the model-visible diagnostics.
- Patch application is a first-class edit tool with a documented grammar, verification, sandbox checks, streaming progress, and patch evidence. Shell-form patch commands can be intercepted and routed through the same implementation.
- Transcript history is normalized before prompting. Tool call/output pairs must stay balanced, unsupported media should be stripped, and large tool outputs should be truncated into durable artifact refs.
- Thread/session persistence is a separate store abstraction. Resume should not be baked into the agent loop.

Important pieces not to copy blindly:

- Codex is strongly optimized for OpenAI Responses semantics. Sinomo still needs provider-neutral rendering and local-provider support.
- Codex does not appear to have a first-class coding-state reducer that classifies validation status, patch relevance, or "extra turns after correct patch." Sinomo should add that controller because benchmark traces show this is a real failure mode.
- Codex exposes repository work mostly through shell, patch, and search-style tools. Sinomo should keep repository interaction tool-native: use shell commands or installed repo CLI/tool modules, and improve the evidence layer around those calls.

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
- Progressive tool disclosure should be first-class. Core coding tools stay visible; large plugin, module, MCP, cluster, or rarely used toolsets can be exposed through `tool_search`, `tool_describe`, and `tool_call`-style bridge tools that still route through the same orchestrator.
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
- The tool registry should be able to consume module catalog surfaces and loader state, but it must not become a second module system. Module catalog data describes what exists, loader state says what is active, and the model-visible tool projection is assembled from those plus the session tool surface by an integration layer.
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
    ToolInput { tool_call_id: String, input: String },
    ToolCredentialAttached { tool_name: String, credential_ref: ArtifactRef },
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
    pub tool_surface_id: Option<String>,
    pub tool_availability: Option<AgentToolAvailabilityDecision>,
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

This control plane should remain below coding judgment. It manages session lifecycle and provider/tool execution; it does not decide whether a Rust test failure is patch-relevant or whether a self-evolution candidate is acceptable. It should not introduce an interactive tool-approval operation: visible tools are already available to the agent, hidden or stale tools are blocked by the tool surface and executor registry, and forbidden resources belong in the sandbox or host isolation layer.

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

`CodingValidationAttempt` also preserves the concrete command, exit code, timeout flag, and an optional `CodingCommandFailureKind` so downstream reports can distinguish missing dependencies from patch-relevant failures without parsing stderr again.

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

- an executable registry containing handlers, tool-surface availability checks, and optional host-provided module catalog/loader inputs;
- a model-visible projection assembled per turn from visible toolsets, provider capability, environment state, and progressive disclosure.

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
- request user input only when the controller lacks required task facts, credentials, or operator intent that cannot be inferred from repository state and evidence.

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
- `apply_patch`: remain the single source-editing tool, with a provider-friendly descriptor and clear patch grammar examples. Narrow replace tools were intentionally dropped so editing semantics stay centralized.

Later tool additions:

- symbol and reference search backed by language-specific indexes when available;
- project check discovery that proposes likely format, lint, unit, and focused test commands;
- structured test result parsing for common runners;
- dependency and environment probes that report missing tools without mutating the host.

`apply_patch` should remain available for one-line, localized, and multi-file edits. Its prompt and descriptor should describe the actual Sinomo patch grammar and examples, not call it a Codex patch. Editing evidence, path checks, and changed-file tracking should stay centralized there.

Tool runtime work:

- split tool descriptor, executable handler, model-visible schema, and tool-surface availability;
- add scoped toolsets such as core, coding, TUI, local-only, cluster, self-evolution, module, plugin, and MCP;
- maintain a generation counter for registry changes so sessions can refresh projections deterministically;
- add cached availability checks and dynamic schema overrides before a tool is exposed to a model;
- carry execution placement metadata: target node, workspace owner, OS/architecture, sandbox class, dependency requirements, data locality, and local-only or cluster-capable status;
- add per-tool parallelism, cancellation, and timeout metadata;
- route all tools through one orchestrator for sandbox placement, retry, cancellation, and post-tool evidence shaping;
- let TUI-only, cluster-only, self-evolution-only, and coding-only tools be extra registry layers over the shared runtime;
- support progressive disclosure by keeping core coding tools visible and deferring large plugin/module/MCP toolsets behind search/describe/call bridge tools that still use the same orchestrator and tool-surface availability checks.

## Provider Adapters

Keep the existing JSON decision contract as the portable fallback, but add provider-specific adapters where they materially improve reliability.

Priority adapters:

- OpenAI/Codex Responses native tool-call rendering and streaming parse as a first-class path, not a wrapper around the JSON decision contract.
- DeepSeek-compatible output rendering or deterministic repair before the loop spends multiple turns on malformed structured output.
- Anthropic native tool calls once the provider path is needed for coding runs.

Provider adapter metrics should be part of every coding run. The generic agent run now records the first local subset in `AgentRunModelMetrics`: output format, parsed model-turn count, total sampled model-response count, model-response parse failures, strict-JSON decision turns, text-fallback turns, provider-native tool-call turns and call count, vendor-fallback tool-call turns and call count, repair attempts, model-visible repair-request bytes, provider-reported token usage, provider-reported repair-response token usage, and final decision source. Provider adapters should extend that evidence with adapter-local normalization and additional token-cost details:

- parse failures;
- repair attempts;
- native tool-call usage;
- fallback JSON usage;
- model-visible repair-request bytes, plus provider-reported tokens spent on repair responses when available;
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

Fix harness freshness first. A benchmark command must rebuild the Sinomo coding-agent runner when source changes, or require an explicit binary path and record that binary's provenance. The current SWE-bench runner does this for the default binary and records explicit-binary provenance when an operator supplies `--sinomo-agent-bin`.

Minimum regression suite:

- one simple localized SWE-bench instance that exercises patching and stop policy;
- one missing-dependency validation case that must be classified as environment-blocked with a missing-dependency failure kind;
- one multi-file edit case;
- one malformed model-output case that exercises provider repair;
- one structured-command-output case where diagnostics are on stderr and must still be visible to the model;
- one session interruption/resume case that verifies no orphan tool calls or direct terminal output leaks;
- one active-turn cancellation and timeout case that verifies cleanup and terminal events;
- one async task case where the main session can continue while a coding task emits progress and completion events;
- one steering/cancellation case that targets the correct task when multiple async tasks exist in the same session timeline;
- one workspace-ref case that starts with a local path but records SIFS-compatible evidence and workspace metadata;
- one placement/provenance case that records node, workspace, sandbox, and policy metadata for a local command;
- one progressive tool disclosure case that verifies scoped bridge tools cannot bypass hidden-tool guards or sandbox placement;
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
- decision-source path counts for strict JSON, text fallback, provider-native tool calls, and vendor-fallback tool calls;
- token usage;
- stale binary, harness freshness, and per-instance harness-blocked failures;
- session event counts and orphan tool call/output counts.

Acceptance target for the first simple localized case: once a correct patch is present and validation is only environment-blocked, DeepSeek-like runs should stop within a small bounded number of turns instead of spending most of the budget on rereads and repeated validation.

## Self-Evolution Integration

The coding controller should become the canonical mutation worker for self-evolution.

Candidate bundles should include:

- `agent-run.json`;
- `agent-summary.json` with compact `AgentRunModelMetrics` for provider path, parse failures, repair attempts, tool-call mode, token usage, and final decision source, plus tool call/result pairing integrity counts; approval `candidate-summary.md` renders the same compact agent evidence when present;
- `agent-events.jsonl`;
- `coding-run-state.json`;
- `patch-snapshots.json` and changed-file records;
- `validation-records.json` with typed statuses;
- benchmark deltas when a benchmark gate runs.

The coding controller may recommend `ready`, `blocked`, or `needs-more-evidence`, but those are evidence summaries, not promotion authority. Candidate success and rollout eligibility remain decisions made by evaluation, policy, provenance, registry, and rollout gates.

When a run fails, the durable evidence should be good enough to create a follow-up evolution task that targets the actual failure class: provider parsing, tool ergonomics, validation classification, context bloat, stale runner, or model capability.

## First Implementation Slice

1. Fix benchmark runner freshness so local SWE-bench retries cannot silently reuse a stale `sinomo-swebench-agent` binary.
2. Add the first `AgentOp` / `AgentEvent` session surface over the existing `AgentSession` methods without breaking the current one-shot `Agent::run` helper.
3. Add local-first `AgentTaskRef`, `WorkspaceRef`, placement metadata, and event-envelope fields so the first coding implementation already records task, role, node, workspace, tool-surface availability, and evidence provenance through host-neutral data contracts.
4. Add a session control plane with per-session ordering, one active turn, durable task status, progress events, status snapshots, cancellation, timeout cleanup, and separate event-stream/result handling.
5. Promote command outcomes to structured stdout, stderr, aggregate output, exit status, timeout, truncation, and storage-backend-neutral artifact refs.
6. Add a shared tool orchestrator boundary for visibility, tool-surface availability, sandbox placement, cancellation, timeout, and post-tool evidence shaping.
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

## Current Implementation Status

Implemented in `sinomo-agent`:

- `AgentOp` / `AgentEvent` session operations over the existing session methods.
- `AgentSessionControlPlane` for ordered operations, one active turn, task status snapshots, progress batches, cancellation requests, timeout cleanup, terminal results, and store persistence.
- `AgentSessionRuntime` as the first Codex-style orchestration handle: turn operations are queued quickly, executed on an interruptible worker, and observed through the same control-plane events and task snapshots.
- `AgentRuntimeEvent` as a runtime-level event stream for worker lifecycle markers, live model progress, live tool progress, and wrapped control events.
- `AgentCancellationToken` threaded through the model-call boundary, tool registry, and orchestrator, including progressive `tool_call` dispatch.
- Model progress sinks threaded through the provider streaming path so runtime-backed surfaces can render assistant deltas, reasoning summaries, tool-call deltas, usage, provider errors, and aborted model calls before the turn completes.
- Tool progress sinks threaded through the registry/orchestrator path, including progressive `tool_call`, so runtime-backed surfaces can observe live process progress without scraping stdout.
- Local `run_command` now runs on `AgentProcessManager`, emits process start/output/completion progress, preserves stdout/stderr evidence, and reports explicit cancelled tool results separate from timeouts.
- Registry-level cancellation now prevents any registered executor from starting after a turn interruption, covering local tools plus future host-provided module/plugin/MCP gateways. Local non-command tools also check cancellation cooperatively during read, recursive list/search traversal, and `apply_patch` planning plus pre-mutation phases. `changed_files` and `git_diff` now route their git subprocesses through `AgentProcessManager`, so version-control helper subprocesses can be cancelled after start and emit the same process progress/evidence shape as `run_command`. The sandbox container runner now exposes a sandbox-owned cancellation token and requests running-container removal on cancellation without adding an R2 dependency to `sinomo-sandbox`; the sandbox service registers run operations by `sinomo-operation-id` and serves `/sandbox/run/cancel` for matching `ObjectRpcCancelRequest` frames. Clustered daemon sandbox runs propagate that token through `DaemonSandboxBackend`, the WASIX SIFS runner, and the native-module `is_cancelled` callback. Daemon-hosted native-dylib module object-RPC routes now register active operation ids, expose `/module/native-dylib/cancel`, and pass the cancellation token into the module ABI's existing `is_cancelled` callback. The optional `module.benchmarks` native-dylib exports `/benchmarks/cancel-probe` as the first cancel-aware module fixture. Host-provided tool gateways can register `AgentToolGateway` start/wait/cancel tasks so the agent adapter can invoke cancellation for already-running gateway work. Remote object-RPC tools can use `AgentRemoteObjectRpcTool`, which stamps run requests with `sinomo-operation-id` and sends `ObjectRpcCancelRequest` frames with `sinomo-cancels-operation-id` to a configured cancel route.
- `AgentToolPlan::readiness_issues` now reports visible planned tools without runtime executors; context assembly filters those tools out of the model-visible schema list and records `tool_readiness` omissions, giving package/module/tool-surface validation a reusable preflight gate before model sampling. Tool orchestration evidence records the registry generation and visible toolsets from the exact plan used for each call, including blocked missing-executor and stale-tool outcomes.
- The coding-state reducer now records attempt-level command failure kinds for timeout, command setup, missing command, missing dependency, network unavailable, permission denied, environment blocked, and patch-relevant validation failures; model-visible command diagnostics include the same classification plus duration, timeout, bounded output excerpts, and artifact-ref availability.
- The first coding stop policy is wired into the loop: narrow patches can finish only after passed validation or classified dependency/setup/environment-blocked validation, while patch-relevant failures continue and network or permission blockers finish as blocked evidence instead of being reported as ready.
- Final coding stop messages now surface the reducer's typed validation failure kind, so ready or blocked outcomes say whether validation hit a missing command, missing dependency, network unavailable, permission denied, or another classified cause.
- Model-visible file and command output now caps read-file content plus raw stdout, stderr, and aggregate command fields, applies a per-turn aggregate budget to `tool_results` and provider-native tool-output projections, bounds replayed tool-call inputs across prior tool calls, assistant JSON decisions, provider-native tool-call items, and `tool_results.call.input`, and strips local and SIFS artifact URIs while preserving durable evidence refs in traces/session records, so full file contents, stdout/stderr, and original tool inputs remain recoverable without leaking storage paths or oversized logs into the prompt.
- Durable `CodingRunState` records remain complete, but the model-visible `coding_run_state` transcript projection is bounded to recent validation attempts, recent risks, and omitted counts so repeated command and validation history does not grow the prompt unchecked.
- Provider-native tool protocol preference and the system prompt hint for that preference now live in `sinomo-lm`, so `sinomo-agent` requests provider protocol instructions instead of composing provider/model-specific text locally.
- `AgentRunModelMetrics` gives every completed run a compact provider-path summary covering output format, strict-JSON decisions, text fallback, provider-native tool calls, vendor-fallback tool calls, parse failures, repair attempts, parsed model-turn count, total sampled model-response count, model-visible repair-request bytes, provider-reported token usage, provider-reported repair-response token usage, and final decision source. Per-step `agent.loop.model.completed` events also carry `decision_source`, so live traces expose the same path before the run summary is available. Self-evolution candidate bundles now write `agent-run.json`, `agent-events.jsonl`, `coding-run-state.json`, `patch-snapshots.json`, `validation-records.json` when present, and `agent-summary.json`; the compact summary copies those metrics and `AgentToolPairIntegrity`, so candidate bundle consumers can inspect provider behavior and orphan tool-call evidence without loading the full `agent-run.json`. SWE-bench per-instance reports now include combined `agent_model_metrics`, compact coding-state labels, first successful patch turn, session event/item counts, tool call/result item counts, and orphan tool call/result counts; suite summaries aggregate parse failures, model repair attempts, native/vendor/text/strict decision paths, validation status counts, stop-recommendation counts, validation failure-kind counts, repair prompt bytes, repair token usage, first-patch-turn min/max/sum, command-observation turns, command tool results, cancellations, extra turns after patch, host-dependency blocker counts, orphan tool-pair counts, and final decision-source counts for calibration.
- Prompt assembly now separates package prompts from runtime facts: `AgentDefinition.system_prompt` stays role-oriented, output contracts and provider-native tool hints are appended by `sinomo-lm`, runtime provider/model/output facts are rendered in `turn_context`, plain-process provider prompts render request tool schemas from `LanguageModelRequest.tools` rather than copying tool behavior into the package prompt, and workspace instruction files, prompt-reference expansion, context attachment payloads, and async task updates are bounded by default before model request rendering. Context attachment payload JSON and async-update JSON now fit within their configured model-visible byte budgets after metadata and JSON escaping.
- Built-in coding/evolution package prompts now describe runtime-owned tool execution, tool-surface availability, sandbox enforcement, event emission, and session state rather than a prompt-level permission layer; local tool evidence reasons use availability wording.
- `apply_patch` remains the only source-editing tool but now accepts structured `changes` entries for `replace_exact` and one-based inclusive `replace_range`, plus the existing add/delete/move/full-file replacement operations. Exact replacements must match once, and all structured changes still preflight before any file is mutated.
- `AgentToolSurface`, `ToolAvailability`, and `AgentToolAvailabilityDecision` are now the primary runtime availability types; `AgentPolicyBundle`, `AgentToolPolicy`, and `AgentToolPolicyDecision` remain deprecated compatibility aliases. Context, orchestration, trace, session-store, and prompt-facing evidence metadata now report the surface as `agent_tool_surface` / `surface_id` and tool availability as `availability`; old `policy_id` / `policy` fields are accepted only as persisted-input aliases.
- `sinomo agent validate [agent-id|--file <path>] [--json]`, `sinomo agent export [agent-id] [--output <path>]`, and `sinomo agent import <path> [--json]` now expose direct package artifact validation and exchange over the same `AgentDefinition`/`FileAgentDefinitionStore` contract; imported JSON records `imported` provenance and its source path.
- Agent package validation now also checks persisted tool names, duplicate tool declarations, tool-surface entries, skill refs, context provider refs, blank model preferences, invalid temperatures, conflicting legacy/model-policy limits, and zero main-loop step/tool-call limits before packages enter the catalog or file store. `sinomo agent inspect` reports both raw `model_policy` and effective provider preferences so legacy top-level package fields do not hide runtime behavior, and text inspection renders absent runtime caps as `unbounded` to make the continuous-agent default explicit.
- `run_local_agent_readiness_gate` and `sinomo agent readiness [--json]` now provide a deterministic local gate for built-in package validation, package JSON round-trip, file-backed package artifact save/load/catalog merge with import provenance, malformed-package rejection across metadata, provenance, tool, skill, model, context, compaction, and runtime-limit fields, continuous-agent unbounded runtime defaults with explicit caps remaining opt-in, runtime-cap source persistence across JSON, prompt-decomposition vocabulary, coding-agent plan terminology for the no-tool-approval session-control model, provider-boundary delegation to `sinomo-lm`, tool-surface compatibility aliases, visible local tool executors, tool/skill disclosure bridges, hidden-tool discovery plus direct-call guarding, typed missing-executor and stale unplanned tool calls producing model-visible failures plus registry-generation/visible-toolset orchestration evidence, task/workspace/placement run-context provenance across model requests, emitted events, and tool orchestration evidence, default-bounded prompt-reference expansion, default-bounded workspace instruction loading, default-bounded context attachment projection, default-bounded async-update projection, empty optional context-budget accounting, hidden `git_diff` disclosure with bounded preview plus durable full-diff evidence, registry-level non-command tool cancellation, version-control active subprocess cancellation, sandbox container active cancellation, host-provided gateway active cancellation, remote object-RPC cancellation propagation by operation id, package and module skill search/describe disclosure plus blocked `skill_activate` host-runtime activation contracts including module embedded skill text and explicit host-runtime activation metadata, JSON fallback, malformed model-output repair, prompt/output provider error classification, native tool-call normalization, run-level model metrics and model-completed decision-source events across strict JSON, text fallback, provider-native tool calls, vendor-fallback tool calls, parse failures, repair attempts, model-visible repair bytes, provider-reported repair token usage, shared `AgentToolPairIntegrity` pairing for balanced and orphaned runtime call slots, compaction projection invariants, compaction eval harness contract drift, session snapshot resume, read-only runtime object-RPC empty and persisted inspect/progress/terminal-result readback including progress cursor offsets, last-event id continuity, and task filters, runtime-control object-RPC dispatch for live steer/follow-up queueing, runtime-control live active-turn reattach reporting, runtime-control live interrupt with cancelled terminal task status, runtime-control route fallback to typed persisted-only and missing reattach reporting, coding-state bounded projection, read-file model-visible content bounding, replayed tool-call input bounding, per-turn aggregate tool-result bounding, missing-command and missing-dependency environment-blocked validation stop evidence with final-text failure labels, terminal validation blocker evidence with final-text failure labels, command evidence preservation across stdout, stderr, exit status, duration, timeout state, artifact refs, availability, and execution evidence on success, failure, and timeout paths, SIFS-backed command evidence mirroring for large outputs, and model-visible artifact URI redaction plus raw command-output bounding while preserving durable refs.
- `sinomo test run cluster.agent.runtime-routes --require-cluster` is the first live-cluster agent readiness probe. It calls the daemon-hosted read-only agent session inspect, progress, and terminal-results routes with a synthetic session id, proving the route is registered and reachable on the current cluster node without creating a model session or mutating cluster state. `sinomo test run cluster.agent.runtime-persisted-session-routes --require-cluster` now seeds a completed file-backed agent session in the configured Sinomo data dir and reads it back through the same daemon-hosted routes, including cursor offset and last-event-id continuation plus terminal-result task filters, proving the live route package is wired to durable session metadata rather than only returning empty synthetic responses.
- `sinomo test run cluster.agent.module-native-dylib-cancel-route --require-cluster` probes the daemon-hosted native-dylib module cancel route with a synthetic operation id and expects the typed inactive-operation response, proving the route is registered and reachable before an active cancel-aware module fixture is run.
- `sinomo test run cluster.agent.module-native-dylib-active-cancel --require-cluster` uses the optional `module.benchmarks` `/benchmarks/cancel-probe` route as the cancel-aware native-dylib fixture, cancels it through `/module/native-dylib/cancel`, and requires the module response to report host cancellation. The case skips when that optional fixture is not active on the live node.
- `sinomo test run cluster.agent.sandbox-cancel-route --require-cluster` probes the daemon-hosted sandbox cancel route with a synthetic operation id and expects the typed inactive-operation response, proving the route is registered and reachable without starting sandbox work.
- `sinomo test run cluster.agent.sandbox-active-cancel --require-cluster` creates a temporary daemon sandbox with the standard BusyBox toolchain id, starts `sh -c "sleep 30"` with `sinomo-operation-id`, cancels it through `/sandbox/run/cancel`, and requires the run to finish as cancelled. The case skips only when the live node lacks daemon sandbox execution prerequisites such as the BusyBox toolchain or WASIX runtime.
- Native modules can now register `sinomo test` cases through the CLI runtime configuration. The shipped binary registers `sinomo-evolution`'s `evolution.self-dry-run` case, which exercises the real candidate dry-run orchestrator path and verifies bundle, mutation packet, scorecard, resource usage, diff, changed-file, and command-log artifacts without model invocation.
- `sinomo test run --tag <tag>` now runs every registered case with a matching tag through the same test harness and prints a suite summary. `--tag local-readiness` is the deterministic local readiness path, while `--tag agent-readiness --require-cluster` is the grouped live-cluster agent readiness path. Skipped selected cases fail the suite command so missing live prerequisites cannot be mistaken for a passed gate.
- `AgentRuntimeService` now provides read-only object-RPC routes for persisted session inspect, progress, and terminal results. `sinomo agent session inspect|progress|terminal-results` exposes those routes to operators with compact text or exact JSON output. `AgentRuntimeControlService` adds host-injected steer, follow-up, interrupt, and resume routes through `AgentRuntimeControlHost`; `sinomo agent session steer|follow-up|interrupt|resume` calls those routes when a long-running host has registered live runtime control. `AgentRuntimeControlRegistry` type-erases live `AgentSessionRuntime` handles behind `AgentRuntimeControlSession`, so hosts can inject real active sessions without exposing model generics. Resume responses carry `runtime_attached` plus a typed `reattach` report: live hosts return `live_runtime_attached`, while an empty or restarted host returns `persisted_metadata_only` with active-turn/task/event counts when persisted metadata exists or `missing` otherwise, instead of turning the resume route into a hard RPC error. The runtime-backed TUI/app chat handler now owns this registry and registers live session handles as runtimes are created, resumed, or swapped. `AgentChat` exposes first-class GUI-facing `steer`, `follow_up`, `interrupt`, and `control_state` methods with plain status/state return structs including reattach state, the graphical app chat worker carries matching control commands/events to the running app process, and the composed app chat UI can queue follow-ups while streaming or interrupt the active turn. The default daemon hook remains read-only until an active runtime host is supplied. `sinomo-daemon` accepts generic service-registration hooks, and the application CLI supplies `sinomo_agent::register_services`, so the shipped daemon can host the persisted inspect surface without R1 depending on R2.
- The optional `sinomo-benchmarks` module now catalogs `agent-compaction-quality`; `modules/benchmark/sinomo-benchmarks/agent/run_compaction_eval.py` keeps live/model compaction-quality work out of ordinary Rust unit tests while covering long-session resume, constraint retention, coding-after-compaction, and cluster-ops-after-compaction scenarios. Fixture mode validates scenario definitions and deterministic marker scoring; run mode feeds generated compaction summaries into continuation probes for live/model eval evidence. Local readiness now checks that this eval harness, benchmark catalog entry, and documentation boundary remain present without turning the live/model score into a Rust unit assertion.
- `AgentProcessManager` as the reusable process/session substrate for spawned commands, stdin writes, output snapshots, timeout state, and cancellation.
- The old command-session compatibility tools were retired; command lifecycle now goes through `run_command` plus runtime/tool progress events and the reusable `AgentProcessManager` substrate.
- `FileAgentSessionStore::subscribe_records` for clone-shared live delivery of newly appended session store records while retaining JSONL persistence as the durable source of truth.
- Completed runtime outcomes are addressable by operation id so hosts can submit a turn asynchronously, join or poll it, and retrieve the terminal `AgentRun` without falling back to direct blocking turn calls.
- The full ratatui chat path and the line-oriented TUI handler construction now use `AgentSessionRuntime` for normal model turns. Model deltas and process output are rendered as live stream text, usage updates are folded into the status bar, and Ctrl+C interrupts an active runtime task before falling back to quit.
- The explicit `sinomo agent run` main-agent prompt path now submits through `AgentSessionRuntime`; the legacy main-agent decision helper was removed.
- The old TUI `MainAgentInteractionHandler` implementation was removed from `sinomo-cli`; runtime-backed orchestration is now the only main-agent TUI handler.
- The SWE-bench coding-agent runner submits initial and repair turns through `AgentSessionRuntime`, then records the completed `AgentRun` through runtime outcome lookup.
- The SWE-bench verified runner preserves the continuous-agent default by omitting `--max-steps` unless the operator explicitly supplies it; dry-run plans report `max_steps_policy` as either `unlimited-default` or `explicit-cap`.
- The SWE-bench verified runner now writes a `host_dependencies` object into plan and summary JSON, plus a `host-dependency-probe` progress event. The probe records `git`, `cargo`, `mini-extra`, Python, `swebench` importability, Docker CLI/daemon reachability, agent-binary status, and blocking issues for the selected mode/evaluation path. Plan mode remains observational, while `sinomo`, `mini`, and `both` modes stop before launching subprocesses when blockers are present, write `harness_status: "host-dependency-blocked"`, and emit `host-dependency-blocked`.
- The SWE-bench verified runner writes per-instance `status: "harness-blocked"` reports when the agent subprocess fails before writing a prediction, continues to the next instance, blocks official evaluation when predictions are incomplete, and reports completed, blocked, and missing report counts separately in the suite summary.
- The self-evolution mutation worker submits evolution turns through `AgentSessionRuntime`, preserving the same orchestration, cancellation, event, and terminal-result path used by TUI and benchmarks.
- SWE-bench report and suite-summary counters for command-observation turns, command tool calls, cancellations, cancelled runs, extra model turns after the first successful patch, session event/item counts, tool call/result item counts, and orphan tool call/result counts. Tool-pair integrity now uses `sinomo-agent`'s shared `AgentToolPairIntegrity` helper keyed by the runtime step/call slot shared by `AgentItem::ToolCall` and `AgentItem::ToolResult`; self-evolution scorecards extract those counters plus host-dependency, harness-blocked, and missing-report counts. Benchmark summaries with host dependency blockers or incomplete harness evidence produce explicit inconclusive verdicts instead of generic missing-results or error-instance verdicts, and summaries with orphan tool call/result counts or mismatched tool call/result item counts now fail the benchmark gate even if SWE-bench reports the patch resolved.
- Fake-model regression coverage for async runtime interruption of an active shell command.
- Direct process-manager regression coverage for stdin, stdout/stderr observation, and cancellation.
- File session-store regression coverage for matching-session live record delivery.
- Runtime regression coverage for cancelling while a model call is active, producing a cancelled turn instead of a worker failure.
- TUI regression coverage for runtime-backed chat submission, terminal decision rendering, live command/model progress, and interrupt versus quit behavior.

Next-stage hardening after this Codex-style orchestration slice:

- The GUI app now shares one `AgentRuntimeControlRegistry` between its chat worker and app-started in-process daemon. That daemon registers `sinomo_agent::register_services_with_runtime_control`, so app-owned active sessions have read and control RPC routes backed by the same registry. The standalone CLI daemon remains read-only because it does not host chat sessions.
- Persist active async task handles and resume/inspect metadata across process restarts. The local runtime now has the control-plane, route, TUI/app registry, live session-control shape, local readiness coverage for live steer/follow-up queueing, active-turn reattach, and interrupt through runtime-control object-RPC routes backed by `AgentRuntimeControlRegistry`, and a typed persisted-only reattach report when a live runtime cannot reattach, but true in-flight task reattachment across process restart still needs a host-owned mechanism for restoring running handles.
- Run the module active-cancel live gate on a daemon node with `module.benchmarks` installed and the sandbox active-cancel live gate on a node with BusyBox/WASIX prerequisites installed as part of cluster readiness.
