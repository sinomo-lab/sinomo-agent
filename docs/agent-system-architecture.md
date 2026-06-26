# Sinomo Agent System Architecture

## Purpose

This document is the single source of truth for the current Sinomo agent system.
It describes the architecture that is implemented now: agent packages,
provider-neutral model requests, runtime-owned tool execution, durable internal
session state, readiness gates, live session control, command evidence,
cancellation, and compaction evaluation boundaries.

It complements the distributed runtime in [architecture.md](architecture.md),
the cluster control plane in [clustering-architecture.md](clustering-architecture.md),
long-horizon memory in [memory-architecture.md](memory-architecture.md), the
cluster root filesystem in [sifs.md](sifs.md), and controlled mutation in
[self-evolution-architecture.md](self-evolution-architecture.md). The agent loop
is generic R2 infrastructure; self-evolution is one caller of it, not a separate
agent runtime layer.

## Design Position

Sinomo should feel like one continuous agent to users, but the implementation is
a cluster-aware runtime around model turns, durable internal session state, tool
orchestration, and async task control. The main agent must not block live
interaction just because slow work is in progress. Tool calls, command
execution, research, coding, maintenance, and background work are represented as
typed internal session activity with evidence.

The runtime owns security and execution. Prompts describe behavior, but they are
not the authority for tools, filesystem access, sandboxing, cancellation,
storage, rollout, or cluster placement. If a tool is visible to the model, it is
available in that model-session tool surface; hidden tools can be searched and
described through progressive disclosure, but guessed direct calls are rejected.

Provider-specific behavior belongs in `sinomo-lm`. `sinomo-agent` builds a
provider-neutral `LanguageModelRequest`; `sinomo-lm` renders provider protocol
hints, output contracts, tool schemas, plain-prompt fallbacks, native tool-call
normalization, malformed-output repair classification, context-window
inference, and provider error categories.

## Crate Boundaries

The main implementation lives in `crates/sinomo-agent`.

Important exported surfaces:

- `Agent`, `AgentSession`, `AgentRun`, `AgentItem`, and `AgentTranscript`;
- `AgentDefinition`, `AgentPackageDefinition`, `AgentPackageProvenance`, and
  `AgentCatalog`;
- `AgentConfig`, `AgentLoopInput`, `AgentOutputFormat`, and `TurnContext`;
- `ContextAssembler`, `ContextBlock`, `ContextFrame`, `ContextDelta`,
  `ContextAssemblyReport`, and `ContextBudgetEstimate`;
- `AgentCompactor` and `AgentCompactionSummary`;
- `AgentToolRegistry`, `AgentToolOrchestrator`, `AgentToolSurface`,
  `ToolAvailability`, `AgentToolEvidence`, and `AgentToolPairIntegrity`;
- `AgentSessionStore`, `FileAgentSessionStore`, `AgentTraceStore`, and
  `FileAgentTraceStore`;
- `AgentSessionControlPlane`, `AgentSessionRuntime`,
  `AgentRuntimeService`, `AgentRuntimeControlService`,
  `AgentRuntimeControlHost`, and `AgentRuntimeControlRegistry`;
- `run_local_agent_readiness_gate` and the CLI-facing readiness report types.

Supporting crates:

- `sinomo-lm` owns provider adapters, protocol rendering, provider-neutral
  stream events, tool-call collection, context-window inference, and provider
  error classification.
- `sinomo-cli` exposes `sinomo agent ...` commands, `sinomo test run ...`,
  daemon registration hooks, and local/readiness test cases.
- `sinomo-daemon` hosts service routes without depending on R2 agent code
  directly; application registration supplies the agent service hooks.
- `sinomo-network`, `sinomo-sandbox`, and `sinomo-sandbox-wasix-host` provide
  object-RPC routing and cancellable sandbox execution used by agent tools.
- `modules/benchmark/sinomo-benchmarks` owns optional benchmark and eval
  harnesses, including compaction quality scenarios.

## Agent Packages

`AgentDefinition` is the agent package artifact. There is no extra runtime profile layer.
A package carries:

- schema version and package version;
- provenance (`built-in`, `local`, `module`, or `imported`);
- role, description, and base prompt;
- declared tools and tool-surface projection hints;
- skill references;
- context strategy and compaction strategy;
- provider preferences and raw model policy;
- optional runtime caps;
- output format.

Built-in packages use built-in provenance. File-backed custom packages are stored
under `$SINOMO_DATA_DIR/agents/*.json`. Imported package JSON receives imported
provenance with the source path. Module-projected packages receive module
provenance from the module registration id and version.

Package validation rejects malformed ids, package versions, duplicate tools,
invalid tool names, invalid tool-surface entries, invalid skills, invalid
context providers, blank model preferences, invalid temperatures, conflicting
legacy/model-policy fields, zero runtime limits, and unknown context or
compaction strategies before the package enters the catalog.

Runtime limits are unbounded by default. Explicit caps remain opt-in and can be
set by callers that own a concrete session or model constraint. Package-derived
caps do not become sticky: switching from a capped package back to an unbounded
package clears package-derived caps, while caller-provided explicit caps survive
package swaps and JSON persistence.

CLI package commands:

```sh
sinomo agent inspect [agent-id] [--json]
sinomo agent validate [agent-id|--file <path>] [--json]
sinomo agent export [agent-id] [--output <path>]
sinomo agent import <path> [--json]
```

## Prompts and Provider Rendering

Package prompts are role-oriented. They do not hardcode runtime model facts,
provider protocol rules, tool schemas, or output contracts.

The current rendering pipeline is:

1. `sinomo-agent` selects the package prompt and assembles typed turn context.
2. `sinomo-agent` attaches request tools as `LanguageModelRequest.tools`.
3. `sinomo-agent` passes the package output format to `sinomo-lm`.
4. `sinomo-lm` renders provider-native protocol hints and output contracts.
5. Providers return text, native tool calls, vendor fallback tool calls, usage,
   finish events, or provider errors as provider-neutral values.

`Agent` treats malformed model output as recoverable. The failed response is
preserved as typed evidence, model metrics record a repair attempt, and the
runtime sends a focused repair request. `max_model_repair_attempts` is optional;
when absent, repair attempts are left to session cancellation, steering, or
runtime policy.

## Session and Turn Model

The generic agent loop has a session/turn split. A session is an internal
model-turn execution unit, not the user-facing conversation or participant
model. Chats and channels own visible timelines and participants; sessions drive
model context, compaction, tool state, traces, steering, and follow-up input.

- `Agent::run` remains the compatibility helper for one-shot calls.
- `Agent::start_turn`, `continue_turn`, `steer`, `follow_up`, `compact`, and
  `abort` operate on `AgentSession`.
- `AgentSession` keeps both a model-facing projection transcript and an
  append-only raw transcript so compaction never destroys source history.
- Queued steering and follow-up messages are consumed into the next turn as
  explicit queued context.

Every model step assembles a `TurnContext` containing runtime facts, workspace
facts, current session projection, working state, async updates, workspace
instruction files such as `AGENTS.md`, prompt-reference expansions, generic
context attachments, available tools, and budget estimates.

Within one session, everything the model sees is owned by the context
assembler. Memory may contribute retrieved blocks, but memory does not manage
same-session persistence, maintain the active projection, or decide which
same-session information remains visible. Memory retrieval decides visibility
only at session boundaries, such as starting a new model session, resuming older
work, or bringing durable chat, channel, task, workspace, user, or cluster
context into a new model turn.

The context assembler writes a `ContextAssemblyReport` for each turn. The report
records included sources, omitted sources, byte estimates, and budget lanes for
fixed prompt, current work, recent tail, retrieved memories, old summaries,
prompt references, context attachments, async updates, and tool-surface
availability.

The context assembler also builds a content-addressed `ContextFrame` from typed
`ContextBlock`s. Blocks carry stable keys, content hashes, source references,
priority, byte estimates, and a cache policy (`stable_prefix`, `append_only`,
`volatile_tail`, or `trace_only`). Each sampled frame records a `ContextDelta`
against the previous sampled frame in the run, including kept, added, updated,
removed, compressed, and externalized block refs plus reusable prefix bytes.
This makes prompt contents inspectable and lets the UI explain which context
changed without requiring the model-visible prompt to carry trace bookkeeping.

Default model-visible bounds:

- workspace instruction files: 64 KiB;
- prompt-reference files: 64 KiB;
- prompt-reference directories: 200 entries;
- context attachment payloads: bounded to configured model-visible byte budget;
- async updates: bounded to configured model-visible byte budget;
- replayed large tool-call inputs and tool results: bounded before the next
  model request.

Durable inputs and tool outcomes remain complete even when model-visible
projections are bounded.

## Storage and Evidence

`AgentSessionStore` is the reloadable session-state boundary.
`FileAgentSessionStore` writes:

- `events/agent/sessions/<session>/snapshot.json`;
- `events/agent/sessions/<session>/events.jsonl`;
- `events/agent/sessions/<session>/items.jsonl`;
- `events/agent/sessions/<session>/projections.jsonl`.

The snapshot is the canonical resume projection. Item and projection streams are
append-only audit trails.

`AgentTraceStore` is the append-only observability boundary.
`FileAgentTraceStore` writes run boundaries, context reports, model events,
typed context frames and deltas, transcript items, tool outcomes, compactions,
and emitted runtime events under `events/agent/traces/<session>.jsonl`.

Full command and tool evidence can be written under
`events/agent/evidence/<session>/...`. `LocalWorkspaceAgentConfig` wires local
evidence roots and optional SIFS-backed evidence mounts into workspace tools.

`sinomo-agent::register_storage_paths` registers `events/agent` as the R2-owned
agent storage subtree.

## Tool Surface and Progressive Disclosure

The primary availability surface is `AgentToolSurface`. Compatibility aliases
for older `AgentPolicyBundle` and policy naming remain loadable for persisted
data, but new code should use tool-surface and availability terminology.

The tool planner combines:

- registry descriptors;
- module-projected tools;
- package-declared tools;
- visible toolsets;
- explicit allow and deny lists;
- declared tool capabilities;
- registry generation.

Visible local tools must have executors. Planned visible tools without executors
produce typed model-visible failures. Stale or hallucinated tool calls that are
not planned for the current generation also produce typed model-visible
failures with registry-generation and visible-toolset evidence.

The coding package's visible bridge tools are:

- `tool_search`;
- `tool_describe`;
- `tool_call`;
- `skill_search`;
- `skill_describe`;
- `skill_activate`.

Hidden tools such as `git_diff` can be discovered and invoked through these
bridges. Direct guessed calls to hidden tools are blocked.

`skill_search` and `skill_describe` disclose package-declared skills and module
embedded skills. `skill_activate` currently returns an explicit host-runtime
activation contract unless an embedding runtime attaches an activation executor.

## Workspace Tools

The local workspace registry includes file, search, patch, version-control,
command, and user-input tools.

Important behavior:

- `read_file`, `list_files`, and `search` bound model-visible output.
- `edit_file` is the provider-neutral targeted source-editing tool. It accepts
  structured exact text replacement inputs, requires one match by default, and
  can explicitly replace all matches when requested.
- `write_file` is the provider-neutral whole-file creation/replacement tool.
  Existing files require `overwrite=true`; missing parent directories require
  `create_parents=true`.
- `apply_patch` remains available as an advanced/provider-specific source
  editing tool and supports raw patch text plus structured small-edit forms such
  as exact replacement and one-based range replacement. It is not the
  provider-neutral default model-visible editing surface. OpenAI deliberation
  and evolution agents expose it by default because the OpenAI tool adapter has
  a custom grammar path for this tool.
- `changed_files` and `git_diff` are version-control helpers. `git_diff` is
  normally hidden behind progressive disclosure and preserves durable full-diff
  artifacts while returning a bounded preview.
- `run_command` preserves stdout, stderr, exit status, duration, timeout state,
  availability, execution evidence, and artifact refs across success, failure,
  and timeout paths.
- Large command output can be mirrored to SIFS-backed artifact refs while the
  model-visible turn sees only bounded output and redacted artifact availability.
- Model-visible tool results omit local and SIFS artifact URI values while
  durable evidence keeps the exact refs.

The coding-state reducer classifies validation outcomes, including missing
commands, missing dependencies, environment blockers, terminal validation
blockers, and patch-relevant failures. A narrow patch can finish as
environment-blocked when remaining validation is blocked by missing setup rather
than by patch evidence.

## Runtime Services and Live Control

`AgentSessionControlPlane` serializes active-turn state, task snapshots, task
progress summaries, result refs, terminal statuses, and buffered events through
`AgentSessionControlSnapshot`.

Read-only object-RPC routes:

- `/agent/session/inspect`;
- `/agent/session/progress`;
- `/agent/session/terminal-results`.

Runtime-control object-RPC routes:

- `/agent/session/steer`;
- `/agent/session/follow-up`;
- `/agent/session/interrupt`;
- `/agent/session/resume`.

The default daemon registration exposes the read-only routes. A process that
owns live runtimes can register `AgentRuntimeControlService` with an
`AgentRuntimeControlHost`. `AgentRuntimeControlRegistry` is the reusable host for
live `AgentSessionRuntime` handles.

Resume responses include `runtime_attached` and a typed reattach report:

- `live_runtime_attached` when the host still owns the live runtime;
- `persisted_metadata_only` when session/control metadata exists but live
  handles cannot be restored;
- `missing` when neither live runtime nor persisted metadata exists.

True in-flight task reattachment across process restart remains a host-owned
mechanism. The implemented route reports that limitation explicitly instead of
pretending persisted metadata can restore process handles.

The app chat worker and app-started in-process daemon share an
`AgentRuntimeControlRegistry`, so GUI calls can steer, queue follow-ups,
interrupt, and inspect active model sessions backing chats owned by the app
process.

## Cancellation

Cancellation is explicit across local and remote work.

Implemented cancellation paths:

- registry-level cancellation before non-command tool execution;
- active version-control helper subprocess cancellation;
- active sandbox/container cancellation;
- host-provided gateway task cancellation;
- remote object-RPC cancellation by operation id;
- daemon sandbox cancellation routes;
- native-dylib module cancellation routes for cancel-aware modules.

Remote object-RPC tools stamp a `sinomo-operation-id` on run requests and send
`ObjectRpcCancelRequest` to a configured cancel route with
`sinomo-cancels-operation-id`. If a remote run returns an error after accepted
cancellation, the agent classifies the outcome as cancelled rather than as an
ordinary failure.

Live-cluster test cases cover sandbox cancel route availability, active sandbox
cancellation, native-dylib cancel route availability, and active cancel-aware
module cancellation when the required fixtures are installed.

## Readiness Gates

`sinomo agent readiness [--json]` runs the deterministic local readiness gate.
It validates:

- built-in package metadata and package JSON round trips;
- file-backed package save/load/import/catalog merge behavior;
- malformed package rejection;
- unbounded continuous-agent runtime defaults and explicit cap persistence;
- prompt decomposition and provider-boundary delegation to `sinomo-lm`;
- tool-surface compatibility aliases;
- visible local tools having executors;
- tool and skill progressive disclosure;
- hidden-tool guarding and stale tool-call rejection;
- task/workspace/placement run-context provenance;
- prompt-reference, instruction-file, context-attachment, async-update, read-file,
  tool-call input, and aggregate tool-result bounding;
- `git_diff` hidden disclosure and durable full-diff evidence;
- local, sandbox, gateway, and remote cancellation;
- JSON decision parsing, text fallback, malformed-output repair, native tool
  normalization, provider error classification, model metrics, and
  `AgentToolPairIntegrity`;
- deterministic compaction projection invariants;
- session snapshot resume;
- read-only runtime routes and runtime-control routes;
- live steer/follow-up queueing, live interrupt, live resume, persisted-only
  resume fallback, and missing-metadata resume fallback;
- coding-state validation classification;
- command evidence preservation, SIFS-backed command evidence, and
  model-visible evidence redaction.

Live-cluster agent tests are registered through the normal `sinomo test` harness:

```sh
sinomo test run cluster.agent.runtime-routes --require-cluster
sinomo test run cluster.agent.runtime-persisted-session-routes --require-cluster
sinomo test run cluster.agent.sandbox-cancel-route --require-cluster
sinomo test run cluster.agent.sandbox-active-cancel --require-cluster
sinomo test run cluster.agent.module-native-dylib-cancel-route --require-cluster
sinomo test run cluster.agent.module-native-dylib-active-cancel --require-cluster
sinomo test run --tag agent-readiness --require-cluster
```

Skipped selected cases are reported as `skipped` in text and JSON output. The
suite command can still exit successfully when a live prerequisite is absent, so
readiness consumers must treat `skipped` as incomplete gate evidence rather than
as a pass.

## Compaction

Compaction has two separate concerns.

Deterministic runtime invariants live in `sinomo-agent` and are covered by local
readiness:

- compaction produces typed projection state;
- source item counts and retained item ids are preserved;
- tool-surface availability constraints survive the projection;
- session snapshots can resume after compaction.

Model quality is eval work, not ordinary Rust unit testing. The optional
benchmark module owns the live/model harness:

```sh
python3 modules/benchmark/sinomo-benchmarks/agent/run_compaction_eval.py --mode plan
python3 modules/benchmark/sinomo-benchmarks/agent/run_compaction_eval.py --mode fixture --summary-out benchmark-results/agent-compaction/fixture-summary.json
python3 modules/benchmark/sinomo-benchmarks/agent/run_compaction_eval.py --mode run --provider codex --model gpt-5.2-codex
```

The checked-in scenarios cover long-session resume, explicit constraint
retention, coding continuation after compaction, and live-cluster operations
after compaction. Fixture mode validates harness structure without invoking a
model. Live/model results are research and eval evidence; local readiness remains responsible only for deterministic projection and persistence invariants.

## Model and Local Runtime

The local PyTorch bridge lives in `crates/sinomo-pytorch`. It owns the stable
request/response schema, process invocation, status reporting, and a
`LanguageModel` provider adapter. Python and Transformers integration stays
isolated in `crates/sinomo-pytorch/python/pytorch_bridge.py`.

The bridge supports text requests, vision requests with image path or base64
payload, audio and duplex request modes in the stable schema, dependency probes,
and local mock mode for smoke tests. MiniCPM-o remains the first configured
backend, but the Rust boundary is model-family neutral: any local
PyTorch/Transformers model that satisfies the schema can be hosted behind the
bridge or managed server.

Product runtime configuration is stored through model runtime settings rather
than prompt text. Cluster-scoped settings live in cluster KV at
`/config/lm/runtime`; local-only test overrides use internal degraded-mode test
helpers rather than a public process flag.

## Background Agent and Cluster Operations

The main and background agents are logical roles. The current generic runtime
supports both through packages and tool surfaces.

Main-agent defaults:

- user-facing interaction;
- session/task state tracking;
- local live inference when available;
- handoff of cluster maintenance by default;
- bounded direct workspace work when visible tools allow it.

Background-agent defaults:

- maintenance and background operations;
- event-woken execution rather than continuous model polling;
- cluster and SIFS diagnostics through operational tools;
- concise reports back to the main agent unless escalation requires user
  notification.

Cluster operation tools should carry session, workspace, task, placement, node,
and evidence metadata. The ordinary model-visible context should get summaries
and handles, not unbounded cluster logs.

## Self-Evolution Boundary

Self-evolution is outside the agent runtime core. It is an R2 caller that uses
the generic `Agent` loop with context attachments for evolution jobs, mutation
packets, candidate bundles, and prior evidence.

The agent runtime provides generic evidence: model events, tool calls, tool
results, patch snapshots, validation records, model metrics, and tool-pair
integrity. `sinomo-evolution` decides how to package that evidence into
candidate bundles and approval summaries. The agent runtime should not grow
self-evolution-specific control flow.

## Training and Data Collection

Local training and adaptation depend on durable traces.

Useful data classes:

- user interaction streams and explicit corrections;
- agent decisions to speak, stay silent, interrupt, delegate, ask, or use tools;
- async task lifecycle and delayed results;
- tool inputs, outputs, errors, placement, duration, and availability decisions;
- context assembly reports, model events, compactions, repair turns, and final
  evidence;
- cluster health and recovery traces.

Training data must carry provenance, privacy labels, retention policy, and
deletion hooks. Raw sensitive media should stay local by default; derived
summaries, embeddings, or clips can be used only according to policy.

The first training path can be adapter-style tuning or supervised fine-tuning on
curated traces. Later paths can add preference optimization and replay-driven
evaluation.
