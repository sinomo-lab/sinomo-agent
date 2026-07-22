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

| Owner | Responsibility |
|---|---|
| `sinomo-agent` | Packages, sessions, context assembly, tool orchestration, compaction, cancellation, and live control. |
| `sinomo-lm` | Provider adapters, protocol rendering, stream normalization, context-window inference, and provider error classification. |
| `sinomo-daemon` | Hosting agent service routes through application-supplied hooks without depending on R2 behavior. |
| Network and sandbox crates | Routed execution and cancellable isolation used by tools. |
| `sinomo-cli` and `sinomo-app` | User interaction and runtime-control clients. |
| Benchmark modules | Optional model-quality and compaction evaluation. |

Public Rust types are documented in source and Rustdoc. This document focuses
on ownership and behavior that must remain stable across those types.

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

Built-in packages use built-in provenance. Custom and imported package
definitions are cluster-wide agent configuration and are stored in SIFS under
`/services/sinomo/agents/*.json`; the importing node only contributes the source
path recorded in provenance. Module-projected packages receive module provenance
from the module registration id and version. The four built-in ids are reserved
principals: stored, imported, and module-projected packages cannot replace them.
This is an identity boundary because the definition id also selects durable
session-history and memory namespaces.

Package validation rejects malformed ids, package versions, duplicate tools,
invalid tool names, invalid tool-surface entries, invalid skills, invalid
context providers, blank model preferences, invalid temperatures, conflicting
legacy/model-policy fields, zero runtime limits, and unknown context or
compaction strategies before the package enters the catalog.

Package policy is enforced at runtime, not treated as advisory metadata:

- toolset visibility feeds planning and package allow/deny lists feed the
  pre-execution `AgentToolSurface`;
- provider/model preferences select the runtime model unless an explicit CLI
  override owns that selection;
- temperature, output limits, runtime caps, compaction tail, and output format
  feed the corresponding runtime controls;
- home-server and sandbox-node preferences are accepted only when the selected
  execution path can honor them; the current local chat/CLI path rejects remote
  placement until remote agent routing exists;
- unknown context/compaction strategies are rejected, and non-empty context
  provider or vendor-option extensions are rejected until an executor exists.

Every `AgentConfig` also receives a model-derived per-turn context-byte bound.
Known models use `sinomo-lm` context-window metadata; unknown models use a
conservative 32K-token fallback. The model output allowance is reserved before
the 80-percent compaction threshold is calculated.

## Skill Catalog

The managed skill catalog is cluster state stored in SIFS under
`/services/sinomo/skills`. Skill bodies are human-readable `SKILL.md` files.
Catalog metadata, active pointers, and conflict records are SOBJ files. Sinomo
built-ins and module-declared skills are provider-managed sources and can be
reconciled automatically into SIFS. User, agent, and evolution skills are
override sources and must not be overwritten silently by provider updates.

Module skills use existing `ModuleDeclarativeResourceKind::Skill` declarations.
Embedded module skill text can be materialized directly; path-backed module
resources resolve their bytes through the module payload before publishing the
same catalog write. `AgentSkillPlan` merges package-declared skills,
module-projected skills, and active catalog entries. Catalog descriptors carry
SIFS path, version, content hash, base hash, status, and manifest path so
`skill_search` and `skill_describe` can disclose provenance without loading
every skill body into prompt context.

Upstream updates for Sinomo or module skills auto-replace only clean
provider-managed versions from the same source. When a user, agent, or
evolution override exists, the catalog records either a three-way merge
candidate or an open conflict. Resolution modes preserve the Hermes-style
operator choices: keep the existing override, overwrite with backup, rename the
override, or accept a merged version.

Runtime limits are unbounded by default. Explicit caps remain opt-in and can be
set by callers that own a concrete session or model constraint. Package-derived
caps do not become sticky: switching from a capped package back to an unbounded
package clears package-derived caps, while caller-provided explicit caps survive
package swaps and JSON persistence.

The CLI can inspect, validate, import, and export agent packages, and can list,
inspect, import, export, and activate skills. Use `sinomo agent help` and
`sinomo skill help` for the current command surface.

Community skill import treats the Markdown `SKILL.md` as the ground-truth body.
Common frontmatter keys such as `name`, `description`, and
`metadata.short-description` are used to seed Sinomo descriptors when present,
but Sinomo-specific SOBJ metadata remains optional. Export preserves existing
portable frontmatter or synthesizes `name` and `description` for plain
Markdown bodies.

Agent runtimes expose skill lookup through `skill_search` and `skill_describe`.
When the host registry is configured with a local data directory, `skill_update`
persists a prompt-based improvement as a new `evolution` source version, records
the previous active content hash as `base_hash`, and activates the new version by
default. This is the lightweight Hermes-style skill evolution path; broader
mutation campaigns remain owned by `sinomo-evolution`.

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
An agent is the runtime entity that owns its model runtime and session state;
chats and channels route visible messages to agents rather than treating the
model runtime as a chat object.

Agent identity also owns the agent home. The canonical home is
`/sinomo/agents/<agent-id>/home`, is shared by every session of that agent, and
is made available to the agent runtime by default. It is neither a session
directory nor a coding-agent feature. Session-scoped traces, evidence, queues,
and checkpoints remain keyed by session id.

- `Agent::run` remains the compatibility helper for one-shot calls.
- `Agent::start_turn`, `continue_turn`, `steer`, `follow_up`, `compact`, and
  `abort` operate on `AgentSession`.
- `AgentSession` keeps both a model-facing projection transcript and an
  append-only raw transcript so compaction never destroys source history.
- Queued steering and follow-up messages are consumed into the next turn as
  explicit queued context.

Chats use the standard session-based loop: one selected agent owns the active
model session, user input becomes a turn, and the resulting answer is appended
back to the chat timeline.

Channels use an event loop over the shared visible timeline. A user or agent
message is appended once, then every other enabled agent can observe it through
that agent's own session projection and decide whether to publish a reply or
stay silent. Published replies become new channel messages visible to the other
agents, so agents can continue a discussion. The runtime bounds each ingress
with a per-message response limit to prevent unbounded self-sustaining loops.

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

Session storage has three distinct products:

- a canonical resume snapshot;
- append-only session items and context projections for audit;
- traces and full tool evidence for diagnosis and replay.

Model-visible output may be bounded or redacted, while durable evidence retains
the complete command outcome and artifact references allowed by policy. Local
storage and optional SIFS-backed evidence use the same ownership contract, and
the agent subsystem registers its entire durable subtree through the shared
storage registry.

Agent homes are outside the session evidence subtree and are keyed by agent id.
They use SIFS so a newly placed or explicitly migrated session sees the same
home instead of receiving a new node-local directory. A runtime that cannot
project the live SIFS filesystem must report the home as unavailable; it must
not silently substitute a session-scoped or host-scoped directory.

## Historical Recall and Markdown Notes

Every normal agent runtime, including one created from a custom agent
definition, exposes four recall tools by default:
`session_history_search`, `memory_search`, `memory_read`, and `memory_write`.
Their authority is supplied by the host from the active agent identity; an
agent cannot select another agent id or storage root in tool input.

Historical search is deliberately an agent-activity search, not a model-session
reader. Its stable input is the agent's chat timelines, currently authorized
shared-channel timelines, and typed background-activity projections for work
the agent engaged in. Results use the
normalized activity kinds `chat`, `channel`, and `background_activity`. Provider
session records, compaction records, raw realtime media state, and other private
model-session data are not part of this contract. Provider-neutral output text
that the realtime UI actually receives is aggregated at stable turn boundaries
and projected as ordinary agent-authored chat messages. Speech-start/stop events
do not invent user transcripts: spoken input enters history only when a future
provider-neutral surface supplies an actual input transcript. This keeps recall
stable across text, full-duplex, realtime, image, and future model runtimes whose
internal session formats may differ substantially.

Shared-channel recall is authorized from current durable participant
membership, not from message authorship or an old session association.
Participant enable/disable transitions carry their own causal revision, so a
later unrelated save from a stale replica cannot resurrect revoked history
access. Authorization metadata and message bodies are read through no-link
SIFS walks to prevent one channel from aliasing another channel's records.
The pager covers both current immutable channel-message records and older
stable channel JSONL chunks through bounded byte continuations; it never falls
back to a materialized transcript snapshot or a provider session.

Chat, channel, and background-activity read models live in SIFS so recall
follows an agent when placement changes. A background activity is sanitized
before projection: its stable id, owner, kind, summary, lifecycle status,
status-derived progress label, and update time are retained, while raw runtime
progress, model-session handles,
result artifacts, active operation ids, and raw event ids remain in the private
execution store. A durable node-local typed projection is written first, then
each activity update records a local pending marker before attempting its
best-effort SIFS mirror. A later activity write for the same agent retries a
bounded page of pending mirrors and clears each marker only after the roaming
write succeeds. Mirror failure never makes ordinary session persistence depend
on cluster storage availability. Same-node search reports a partial result
while any pending marker remains, instead of reading private model-session
files. A pending marker stranded on another unavailable node cannot be detected
globally, which is why this backend advertises best-effort rather than complete
consistency.

Markdown notes live below the owning agent's SIFS home at
`/sinomo/agents/<agent-id>/home/memory/`; the sandbox projection is
`/home/agent/memory/`. Note paths are safe relative lowercase `.md` paths, and
every directory listing, read, parent creation, and write walks the scoped SIFS
tree without following symbolic or hard-link records. `memory_search` streams
bounded, version-bound byte ranges so it can reach matches anywhere in a large
note, and an empty query lists notes for discovery. `memory_read` similarly
returns `next_offset` plus an opaque note version, allowing callers to page an
arbitrarily large existing note without mixing versions. Each `memory_write`
currently accepts at most 1 MiB of complete Markdown as a tool-managed
create/replace operation cap; that cap is not a SIFS storage or read limit.
Writes either create a new path exclusively or
compare-and-replace an existing note using the opaque version returned by a
prior read or write, so concurrent agents do not silently overwrite each
other's notes. If SIFS is unavailable, these tools fail closed rather than
falling back to node-local storage.

This is the immediately useful recall layer, not the long-term indexed-memory
architecture. Search and storage sit behind backend contracts so the indexed
memory system can later supply catalogs, ranking, and richer retrieval without
changing the agent-visible note or activity abstractions.

## Tool Surface and Progressive Disclosure

The primary availability surface is `AgentToolSurface`. It projects the tools
that a package, runtime, and current registry generation make available to a
model turn; it is distinct from the executor registry and from capability
authorization.

The tool planner combines registry and module descriptors, package declarations,
visibility, allow and deny policy, capabilities, and the current registry
generation. Only the planned surface is callable in a model turn.

Large tool catalogs are progressively disclosed through search, description,
and invocation bridges. Hidden tools cannot be called by guessing a name, and a
stale call from an older registry generation fails with typed evidence. Every
visible local tool must have an executor; advertising a tool never grants the
capabilities needed to run it.

Skill discovery follows the same rule. Package and module skills can be
described without loading every skill body into prompt context, and activation
remains a host-runtime operation rather than a prompt-side authority.

## Workspace Tools

Workspaces are conversation grants. A chat or channel owns an ordered list of
`WorkspaceRef`s, and all agents participating in that conversation receive the
same grants subject to their individual policy. The first workspace is the
default cwd in the sandbox; later workspaces are additional accessible roots.
An `AgentSession` may retain a legacy workspace binding only as a compatibility
snapshot, never as the authority for what the user attached to a conversation.

Workspace projection depends on the sandbox runtime:

- an isolated-filesystem runtime receives explicit mounts into its persistent
  filesystem;
- a host-filesystem runtime receives bounded host access through its permission
  mechanism;
- a runtime that supports neither reports the workspace unavailable.

The grant does not imply portability. SIFS-backed workspaces can be resolved on
another server. Host-local files stay on their owning server and are not copied
as part of session migration.

The workspace surface covers bounded file discovery and reads, targeted and
whole-file editing, structured patching, version-control inspection, command
execution, and user input. Its architecture is defined by behavior rather than
individual tool names:

- Reads and previews are bounded before entering model context.
- Destructive or ambiguous writes require explicit intent.
- Version-control and command tools preserve full durable evidence even when
  the model sees only a compact preview.
- Timeouts, cancellation, unavailable commands, and nonzero exits remain typed
  outcomes rather than being flattened into text.
- Local and SIFS artifact locations may be redacted from model-visible output
  while remaining available to authorized runtime components.
- Validation distinguishes patch failures from missing dependencies or other
  environment blockers.

## Runtime Services and Live Control

An active agent session has one authoritative session server. New sessions use
the agent's optional home-server preference; without one, the daemon processing
the creation request becomes the session server. Message ingress from another
device is routed to the active server and never moves the session implicitly.
Consequently, agents in one channel may have sessions hosted on different
servers.

Session migration is an explicit control operation. It moves session ownership
and resumable session state, not arbitrary local resources. Host-local files
that are not in SIFS become unavailable after migration, and Sinomo does not
attempt local-resource transfer. The placement generation changes atomically so
routers can reject stale ownership information.

The sandbox is a persistent VM-like resource with its own filesystem, process
runtime, placement, lifecycle, and portability. It is architecturally
independent of the agent system and is created, configured, inspected,
checkpointed, rebooted, migrated where supported, and destroyed only through
the node-local or cluster-wide sandbox management interface.

An agent session holds one sandbox attachment, not sandbox ownership. Different
sessions may attach the same sandbox. Normal chat and channel orchestration
automatically creates and attaches one sandbox for each new agent session, and
that attachment persists with the session. An explicit resource-attachment flow
may instead attach an existing sandbox, including one shared with other agents.

Tool calls require the existing attachment and execute inside that sandbox.
They never create, replace, reboot, migrate, or destroy a sandbox. Conversation
workspaces are mounted into the attached sandbox before tools are exposed, and
the first mount becomes cwd.

Migrating a session does not implicitly migrate, recreate, or destroy its
sandbox. A session may invoke a sandbox hosted on another server; in that
arrangement every tool operation executes on the sandbox server. A non-portable
sandbox remains bound to its server until explicitly replaced or reattached
through sandbox-specific control.

The control plane exposes read-only inspection separately from live steering,
follow-up, interrupt, and resume operations. A daemon may always expose durable
session metadata; only the process that owns a live runtime can attach the
control operations that mutate it.

Resume reports distinguish a live attachment, persisted metadata without live
handles, and a missing session. Persisted state does not pretend to restore
subprocess or model-stream handles after a process restart. A host may implement
deeper task reattachment, but that capability remains explicit.

CLI, GUI, and cluster callers use the same typed service boundary. Chat UI state
never becomes the owner of the underlying model session.

## Cancellation

Cancellation is explicit and follows one operation identity across registries,
subprocesses, sandboxes, gateways, object RPC, and cancel-aware modules. A
caller can cancel before execution or while work is active. Once cancellation
is accepted, a later transport or worker error is classified as cancellation
rather than an unrelated failure.

Components that cannot stop active work must report that limitation. They must
not acknowledge cancellation and continue producing authoritative side effects.

## Readiness Gates

`sinomo agent readiness [--json]` runs the deterministic local readiness gate.
It covers package and catalog validation, provider-boundary behavior, context
budgets, progressive disclosure, tool evidence, cancellation, compaction,
session resume, and live-control semantics.

Cluster-dependent checks run through the normal `sinomo test` harness under the
`agent-readiness` tag. Exact cases belong in the test registry rather than this
architecture document.

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
benchmark module covers long-session resume, constraint retention, coding
continuation, and cluster work after compaction. Live/model results are eval
evidence; local readiness remains responsible for deterministic projection and
persistence invariants.

Run the compaction harness from the repository root in progressively stronger
modes:

```bash
python3 modules/benchmark/sinomo-benchmarks/agent/run_compaction_eval.py --mode plan
python3 modules/benchmark/sinomo-benchmarks/agent/run_compaction_eval.py --mode fixture
python3 modules/benchmark/sinomo-benchmarks/agent/run_compaction_eval.py --mode run
```

Plan and fixture modes provide deterministic harness validation. Run mode may
call configured models or a live cluster.

Live/model results are research and eval evidence. Local and CI gates interpret
them separately: local readiness remains responsible only for deterministic projection and persistence invariants.

## Skills

Deterministic coverage verifies progressive disclosure and skill writeback.
The optional benchmark module evaluates whether a model can discover required
skill text and use a newly persisted skill in a later turn. Benchmark fixtures
use an isolated data directory and do not modify the active catalog.

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

Agent roles determine their product exposure. `main` and `deliberation` packages
are available to interactive chat selectors. `background` and `evolution`
packages are intentionally hidden and rejected by interactive switching because
their authority and lifecycle belong to dedicated background/service and
self-evolution paths. Package inspection and validation surfaces still show all
four roles.

Main-agent defaults:

- user-facing interaction;
- session/task state tracking;
- local live inference when available;
- typed background-handoff requests when a dispatcher is available;
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
