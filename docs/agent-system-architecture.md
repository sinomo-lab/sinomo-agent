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
- tool-runtime placement and MCP integration references;
- output format.

Built-in packages use built-in provenance. Custom and imported package
definitions are cluster-wide agent configuration and are stored in SIFS under
`/services/sinomo/agents/*.json`; the importing node only contributes the source
path recorded in provenance. Module-projected packages receive module provenance
from the module registration id and version. The three core built-in ids
(`main-agent`, `background-agent`, and `coding-agent`) are reserved principals:
stored, imported, and module-projected packages cannot replace them. Active
module agent IDs are also collision-checked against stored packages and every
other module, so activation order never chooses an identity. This is an identity
boundary because the definition id also selects durable session-history and
memory namespaces.

Modules may export complete agent packages through their serializable module
surfaces. The native-dylib ABI carries the agent prompt, role, tool descriptors,
skill references, and context/tool-surface hints; the host validates and projects
that data into the catalog with immutable module provenance. The default-enabled
`module.evolution` owns `evolution-agent`, its coding tool manifest, and its
embedded planning and review skills. Removing or disabling the module removes
that package instead of leaving a host-owned fallback implementation.
The package lists those skills and the evolution module toolset as defaults,
but skills and callable module routes remain independent catalog surfaces. A
user or another module may configure another agent to use the same skills or
toolset, and non-agent callers may invoke the routes through the normal typed
service boundary.

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
`/services/sinomo/skills`. Its portable package boundary is the open
[Agent Skills specification](https://agentskills.io/specification): every skill
is a directory whose required `SKILL.md` starts with YAML frontmatter containing
`name` and `description`. Optional `license`, `compatibility`, `metadata`, and
experimental `allowed-tools` fields are preserved. `scripts/`, `references/`,
`assets/`, and other regular files remain relative resources in the same
package. Catalog metadata, active pointers, and conflict records are SOBJ files.
Sinomo built-ins and module-declared skills are provider-managed sources and can
be reconciled automatically into SIFS. User, agent, and evolution skills are
override sources and must not be overwritten silently by provider updates.

Imports validate the standard naming, length, frontmatter, and directory-name
contracts before publishing anything. Symlinks are rejected, resource paths
must remain inside the package, and bounded file-count and byte limits prevent
uncontrolled traversal. The catalog content hash covers `SKILL.md` plus every
bundled resource, while each resource also has an independently verified hash.
Exports recreate a standard skill directory rather than flattening the package
to one Markdown file.

Module skills use existing `ModuleDeclarativeResourceKind::Skill` declarations.
Embedded module skill text can be materialized directly; path-backed module
resources resolve their bytes through the module payload before publishing the
same catalog write. `AgentSkillPlan` merges package-declared skills,
module-projected skills, and active catalog entries. Catalog descriptors carry
SIFS path, version, content hash, base hash, status, and manifest path so
`skill_search` and `skill_describe` can disclose metadata and provenance without
loading skill instructions into prompt context.

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

Agent runtimes follow Agent Skills progressive disclosure. The stable turn
context contains only the name and description of each valid, readable active
skill. When a task matches, `skill_activate` returns the Markdown instruction
body and the package's relative resource list. A later `skill_activate` call
with one listed resource reads that file with a bounded model-visible payload;
resources are never eagerly injected. Skill activation output remains in the
ordinary traced tool transcript.

Direct and terminal chat also support `/skill <name> [request]`. This is an
explicit, turn-scoped invocation: the host validates that the skill is
available to the selected agent, loads the complete instruction body before
the model call, records `interaction.skill.activated`, and leaves listed
resources on demand through `skill_activate`. The trailing request remains
user-level input through a typed `explicit_skill_request` context attachment;
it is never promoted into system instructions. Existing administrative chat
commands such as `/skill list` and `/skill activate ...` retain their CLI
meaning.

The experimental `allowed-tools` field is descriptive metadata. It does not
grant a capability, change the current tool surface, or bypass Sinomo policy,
workspace, sandbox, device, network, or host authority checks.

When the host registry is configured with a local data directory, `skill_update`
persists a prompt-based improvement as a new `evolution` source version, records
the previous active content hash as `base_hash`, and activates it immediately
for every agent: skills are memory, and versions exist for tracking and review
rather than as a separate activation step. The host supplies the source identity
and scope. A write never replaces an existing `skill@version` entry from any
source, and it is rejected when the active version changed after the tool read
it, so concurrent updates are not discarded silently. Re-activating an older
version is a catalog operation, not part of the agent tool surface. Updates must
provide a complete standard `SKILL.md`. Broader mutation campaigns remain owned
by `sinomo-evolution`.

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

Chats and channels run on one channel runtime; a chat is a channel with one
agent. A channel policy decides which participants act on a new message and
how. Each agent participant runs on its own runtime; none is swapped through
another's.

A chat uses the single-agent policy: its agent answers every user message,
which becomes the turn's input, and the answer is appended back to the chat
timeline. The chat's notices about turn handling (errors, no response) are
addressed to the user only and stay out of the agent's context.

Multi-agent channels use the round-robin policy, an event loop over the shared
visible timeline. A user or agent message is appended once, then every other
enabled agent can observe it through that agent's own session projection and
decide whether to publish a reply or stay silent. Published replies become new
channel messages visible to the other agents, so agents can continue a
discussion. The runtime bounds each ingress with a per-message response limit
to prevent unbounded self-sustaining loops. Messages carry an audience, so a
message can be delivered to only some participants.

An agent's reasoning and tool calls stay in its session's activity log; the
message an operation publishes refers to that operation (`produced_by`).

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

Native process output is captured in private files under the agent evidence
root. Polls read only the requested byte window. Normal command results retain
bounded previews and refer to complete stdout/stderr artifacts; they do not
collect the full streams again to construct outcome metadata. Temporary captures
are removed when their process/evidence owner is released.

The resume snapshot is replaced atomically: it is written to a synced sibling
file and renamed into place, so a crash leaves the previous complete snapshot
or the new one, never a truncated file. Append-only session logs ignore a
final record torn by a crash, and the next append starts on a new line.

Every item a session turn produces is journaled to the session item log when it
is produced, before the snapshot that will include it. On load, items logged
after the last item the snapshot knows are folded back in. Such items come
from a turn that stopped before its result was saved. A tool call that has no
recorded result gets a failed result, so calls stay paired, and an error item
tells the model the turn did not finish. Loading writes nothing; once the
recovered session is saved, the items are known.

A completed turn's merged resume snapshot is saved before successful runtime
completion is published. A snapshot failure is reported through both the failed
runtime event and the worker join result, while the merged state stays available
in memory for a later save attempt.

An execution error preserves the turn's completed tool results and other recorded
items in the resumable session, together with typed failure evidence. The worker
merges that partial turn with inputs received during execution before saving it;
only inputs already represented in the turn are consumed. Provider failures reach
both the runtime failure event and the worker join result, without publishing a
successful final decision. Continuation therefore sees completed operations even
when the following model request failed.

Control edits are prepared on a staged session and control state. Persistence
errors leave the live session, input queues, revision, and published control events
unchanged. In the session runtime the snapshot save is the edit's commit point
and runs last, after the control result is persisted, so an edit reported as
failed is also not resumed after a restart. Worker cancellation is signalled only after its control result has been
persisted. Executed turn effects follow the partial-progress rule above rather
than being rolled back as control edits.

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
and invocation bridges. A name outside the turn's plan is blocked as
`tool_not_planned` even if an executor was registered after planning, and a call
whose plan was built from an older registry generation is blocked as
`stale_tool_plan` before any executor runs, directly or through the invocation
bridge. Every visible local tool must have an executor; advertising a tool never
grants the capabilities needed to run it.

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

The desktop workspace selector exposes the ordered grants as a folder list.
Adding a folder preserves the existing default; making a folder the default
moves it to the front. Removing a folder leaves the others attached, while
removing all folders revokes the conversation's workspace access. The app sends
explicit mutations to the daemon and displays its returned projection rather
than treating picker results or restored UI snapshots as authorization. Edits
are disabled during an active turn or an outstanding workspace change.

Workspace projection depends on the sandbox runtime:

- an isolated-filesystem runtime receives explicit mounts into its persistent
  filesystem;
- a host-filesystem runtime receives bounded host access through its permission
  mechanism;
- a runtime that supports neither reports the workspace unavailable.

The grant does not imply portability. SIFS-backed workspaces can be resolved on
another server. Host-local files stay on their owning server and are not copied
as part of session migration.

File tools, context assembly (workspace instruction files and prompt
references), and hosted-model reference images never touch the host filesystem
directly. They go through `tools::workspace_files`, which opens each grant once
as a confined VFS handle:

- a host-local grant uses `ConfinedLocalVfsBackend`, an open directory
  capability on which links are followed only while they stay inside the grant;
- a grant under the live SIFS mount uses the SIFS VFS scoped to the grant's SIFS
  path, so SIFS-backed files are reached directly, not through the FUSE
  projection. Its walks reject every link, and mutations commit only against
  the generation the walk opened.

Tool inputs keep naming files by the host path commands see, or relative to the
primary grant. Those names are mapped to a grant and VFS path lexically. Writes
target the primary grant: added files are created exclusively, modified files
are replaced atomically with their permissions preserved, and moves rename the
source. Owned runtime paths such as evidence, generated artifacts, and sandbox
instance directories use the local VFS.

The crate's `clippy.toml` denies direct `std::fs` and `Path` filesystem calls in
these modules, and `tests/workspace_io_boundary.rs` enforces the same rule
without Clippy. Host probes (installed programs, enclosing Git repository) live
in `local_workspace/host_probe.rs`, the only declared exception.

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

## MCP Tools

Agent packages may declare bounded Streamable HTTP MCP servers under
`tool_runtime.mcp_servers`. Sinomo connects to each enabled server, discovers
its tools, and registers namespaced tools such as `mcp__docs__search` in the
ordinary `AgentToolRegistry`. The same executor is consequently available to
turn-based chat and realtime voice; MCP is not a second tool implementation
path and does not bypass Sinomo's tool-surface or execution checks.

An agent package can contain a runtime fragment like:

```json
{
  "tool_runtime": {
    "mcp_servers": [
      {
        "name": "docs",
        "url": "https://mcp.example.net/v1",
        "protocol_era": "auto",
        "required": false,
        "allowed_tools": ["search", "fetch"],
        "bearer_token_env": "SINOMO_MCP_DOCS_TOKEN"
      }
    ]
  }
}
```

### Protocol eras

MCP revision `2026-07-28` removed the `initialize` handshake and protocol-level
sessions. Sinomo's client speaks both generations and `protocol_era` selects
which:

- `auto` (the default) opens with `server/discover`, the RPC every stateless
  server must implement. A reply, or one of the error codes the specification
  reserves for itself (`-32020` header mismatch, `-32021` missing client
  capability, `-32022` unsupported version), settles the endpoint as stateless.
  Anything else identifies a handshake-era server and the client falls back to
  `initialize`. The era is decided once per connection.
- `modern` pins the stateless protocol and reports an error rather than falling
  back, for endpoints that must not be probed twice.
- `legacy` skips the probe entirely and negotiates `2025-06-18`, with
  `2025-03-26` and `2024-11-05` accepted if the server selects them.

Stateless requests carry `io.modelcontextprotocol/protocolVersion`,
`clientInfo`, and `clientCapabilities` in every `params._meta`, and mirror
`Mcp-Method`, `Mcp-Name`, and any `x-mcp-header`-annotated tool arguments into
`Mcp-Param-*` request headers so intermediaries can route without parsing
bodies. Header values that are not plain visible ASCII travel Base64-wrapped in
the specification's `=?base64?…?=` sentinel. Sinomo declares no client
capabilities: it provides no roots, sampling, or elicitation, so a server that
answers with an `input_required` result — the multi round-trip request pattern —
is reported as an error rather than driven. A tool whose `x-mcp-header`
annotations are malformed is dropped on its own and reported in the readiness
check; the rest of the server's catalog still registers.

Definitions contain only environment-variable names; values are resolved on
the executing node and credential references must use the dedicated
`SINOMO_MCP_*` namespace. Discovery and every final tool call fail closed unless
the local operator grants `external-integrations` on that node:

```bash
sinomo node permissions allow external-integrations
```

Revoking the permission blocks already-registered MCP tools at the final
registry boundary. Optional unavailable servers are omitted with a warning;
an unavailable server marked `required` prevents that agent registry from
being reconfigured. Server responses, tool counts, pagination, startup, and
calls are bounded. This implementation supports Streamable HTTP JSON/SSE;
non-loopback endpoints must use HTTPS and redirects are rejected. Stdio process
spawning, OAuth negotiation, resources, prompts, and `subscriptions/listen`
change streams are not part of this host path. After granting a previously
denied integration, reopen or otherwise reconfigure the agent session so tool
discovery can run.

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

Orchestration manages sandboxes through one lifecycle interface for every
node (`SandboxLifecycleClient`): create, status, update, and destroy go to the
local daemon's sandbox service, which serves them or forwards them to the node
hosting the sandbox, and these four routes accept calls from cluster members.
Only a host running without a daemon serves them in process. Automatic agent
sandboxes are created non-portable, in the hosting node's local store, because
tool execution still runs against that store directly; execution through the
sandbox service, and with it tool execution on a sandbox hosted by another
node, is not available yet.

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

### Session lifecycle

A live runtime is a cache in front of the persisted session, not the session
itself. A session moves between these states:

| State | Live runtime | Entered by | Left by |
|---|---|---|---|
| Active | yes | a message | the turn ends, interrupt, cancel |
| Idle | yes | the turn ends | a message; release after idling |
| Paused | while work runs | interrupt, or a restart mid-turn | any new message |
| Dormant | no | release, or a process restart | a message restores it from disk |
| Deleted | no | deleting its chat | purge after the retention window |

- **Restart.** Each active turn records the process instance hosting it.
  When a host restores a session whose persisted control state still shows
  an active turn from another instance, or unfinished tasks, the runtime
  reconciles it (`recover_interrupted_turn`): the tasks become `Paused`, a
  `TurnInterrupted` event is recorded, and the session is paused, so a restart
  is an involuntary interrupt and the next message resumes it. Tool calls
  without results were already closed when the session loaded. A turn hosted
  by the current process is never reconciled, because it may still be live.
- **Release.** The daemon releases a chat host that no window shows after 30
  idle minutes, and the longest-idle unviewed host when it reaches its
  resident-host limit. A host is never released while it is processing a
  command or while its session has work in flight: a turn, tools a paused turn
  is finishing, or subagents. Releasing the host gives up its ownership of the
  chat and removes its runtime's live-control route; a runtime removes only
  its own route, never one a newer host registered. Reopening the chat
  restores it from durable state.
- **Delete.** Deleting a chat runs on the node that hosts it, where its live
  work and session records are; a chat no node hosts is deleted where the
  request arrives. It first cancels each of its sessions' live work, then
  tombstones the chat, then releases each session's sandbox when the session
  owns it: an automatic attachment to the sandbox derived from that session
  that no other session attaches. The release goes through the sandbox
  lifecycle interface to whichever node hosts the sandbox. Explicit and shared
  sandboxes are left alone.
- **Purge.** Thirty days after deletion the daemon purges each session's
  local data (snapshot, transcript journal, control events, traces, evidence)
  and the chat's shared content (messages, attachments, session checkpoints).
  The chat's metadata records remain so the tombstone keeps winning when
  replicas merge.

### Chats across windows and nodes

A chat has one live runtime in the cluster, on one node: its host. Every
window showing the chat, on that node or another, uses that host, so they
share one conversation, one turn, and one control route.

- **Views.** Each window tab is a view with its own event stream, command
  journal, and current chat. Views are independent: closing or switching one
  never affects another view of the same chat. A view that attaches to a chat
  mid-turn receives the chat's current runtime, tool, and control state at
  once. A message one window sends is shown in the others as it starts its
  turn.
- **Ownership.** The hosting node holds an ownership record for the chat in
  cluster KV (`/agent/chat-hosts/<chat-id>`), written through the daemon's KV
  API with compare-and-set and renewed every 10 seconds. A node that opens
  the chat while a live owner renews it follows that owner instead of starting
  a runtime. A restarted daemon takes its own chats back at once.
- **Routing.** A node following another node's chat forwards its windows'
  commands to the owner over the cluster chat routes (`/chat/peer/command`,
  `/chat/peer/events`, `/chat/peer/delete`, open to cluster members) and
  follows the owner's events into its windows. Retried forwards run once. App
  routes remain local commands.
- **Failover.** An owner that stops renewing for 45 seconds is presumed gone,
  and the next node that needs the chat takes it over and restores it from
  durable state; followers move their windows to the new host. A host that
  cannot renew within half that window, or finds the chat owned elsewhere,
  cancels its turn and stops, so two nodes never run one chat. A turn in
  progress on a lost owner is lost; the new host resumes from the last saved
  session.

CLI, GUI, and cluster callers use the same typed service boundary. Chat UI state
never becomes the owner of the underlying model session.

## Detached Tool Calls

A tool call the model marks `async` runs detached when the session's host can
continue the session afterwards; otherwise it runs inline. A detached call
returns at once with a result naming its background task, and the turn goes
on. When the work finishes, its outcome becomes an async update that names
the call's stable tool call id. The host continues the session with it: the
agent takes a turn with the update as input, the turn records the update in
the session's activity, and the agent posts what it found without a new user
message. A channel policy can decline such unprompted turns.

Detached work and completions waiting to be continued count as work in
flight, so the host keeps the session. Each call's start, completion, and
delivery are recorded in the session that started it. When a session is
attached, a completion it has not continued with is queued again, and a call
whose process stopped before it finished completes as interrupted ("it may or
may not have taken effect"), so the agent can tell the user. Delivery is
recorded only once the continuation's session is saved. A paused session
holds its completions until it resumes.

## Subagents

`AgentSessionRuntime` registers the `subagents` tool for temporary parallel or
focused child tasks. `spawn` returns a handle immediately; `list`, `inspect`,
and bounded `wait` calls expose each child's status, result, and failure.
`cancel` requests cancellation and `dispose` cancels and removes the handle.
For example:

```json
{"action":"spawn","name":"review","task":"Review the parser changes and report defects."}
```

Subagents inherit the parent's model, package identity, tool surface, runtime
limits, skills, memory access, workspace, sandbox attachment, placement, and
resource bindings. Launch captures the tool executors and resource leases before
returning, so parent registry reconfiguration cannot release a running child's
resources. Node-local authorization is checked at each effect, including after
permission revocation. The tool accepts no permission or resource overrides.

Each child has a fresh session and transcript. Its task remains user input;
surface-specific participation instructions are not carried into the child.
Session and task identities, the parent task when present, and event causality
attribute child work. Children can launch their own children through the same
tool. They share inherited workspaces and sandboxes, so callers must coordinate
concurrent writes to shared resources.

Child handles belong to exactly one parent session. Cancellation and deadlines
flow to descendants, while child cancellation leaves parents and siblings
running. Aborting or interrupting a parent cancels its children. Releasing the last parent
runtime handle cancels and removes its children; child completion also releases
that child's descendants. Cancellation is cooperative: a running handle stays
`running` until its worker exits, and `cancel_requested` distinguishes the
request from confirmed termination.

Children do not create agent packages, background activities, durable session
checkpoints, or new sandboxes. Inherited evidence and trace stores may retain
their attributable records. Terminal handles remain inspectable until disposal
or parent release; result previews are bounded to 64 KiB and report truncation.
They cannot resume after process restart. The synchronous `Agent::run` helper
has no parent lifecycle and does not install the subagents tool; callers that
need child execution use `AgentSessionRuntime`.

## Cancellation

Cancellation is explicit and follows one operation identity across registries,
subprocesses, sandboxes, gateways, object RPC, and cancel-aware modules. A
caller can cancel before execution or while work is active. Turn-operation
timeouts set a monotonic deadline on the same cancellation token observed by
model, tool, and process execution. Deadline expiry requests cooperative
cancellation; it does not pretend that a non-cooperative call has already stopped.
Both synchronous and worker-driven turns clear their active state after an
execution failure. Once cancellation is accepted, a later transport or worker error is classified as cancellation
rather than an unrelated failure. The turn's published cancelled state is
final: the worker's eventual outcome adds no failed or completed status over
it. A turn reports a timeout only when its deadline actually cancelled it, not
when it merely finished after the deadline.

Components that cannot stop active work must report that limitation. They must
not acknowledge cancellation and continue producing authoritative side effects.

### Interrupt (pause) versus cancel

Interrupting a session pauses it: it is the same operation as `Pause`, used by
the `interrupt` route (`/agent/session/interrupt`), `sinomo agent session
interrupt`, the TUI interrupt, and the app's Esc / Interrupt button.

- No further model request starts. Running tool calls, processes, and
  subagents keep going, and their results are kept.
- A running turn stops at its next model-request boundary with
  `AgentStopReason::Paused`, and its task becomes `Paused`. Repair requests
  count as model requests.
- The pause is recorded on the session, so a paused session starts no new turn
  on its own.
- Any new message resumes the session: a sent message, steering, or a
  follow-up unpauses it before it is delivered. `Unpause`
  (`/agent/session/unpause`) resumes without a message. "Unpause" is distinct
  from `resume`, which reattaches to a live runtime.
- Pause reaches running subagents, which also stop before their next model
  request.

Cancelling (`/agent/session/cancel`, `sinomo agent session cancel`, the app's
Cancel button) stops all work right away and may lose progress. Aborting the
session also cancels its running turn.

- The model stream stops at its next chunk. Every provider that streams through
  the shared SSE reader drops the HTTP response.
- No further tool call starts.
- Running tool calls are cancelled. Each call runs on its own thread, so the
  turn never waits on it for more than a two-second grace period. A tool that
  stops within that window reports what it actually did; for example, sandboxed
  commands kill and reap their process tree. A tool that does not, such as a
  blocking network request, is abandoned and reported as cancelled with
  `tool_cancellation_unconfirmed`, because it may still complete in the
  background.
- Local processes are marked cancelled only once they have exited. The kill is
  repeated at most every 500 ms until they do.
- Subagents are cancelled.

Host code that must stop a turn outright, such as switching models mid-turn,
uses cancel rather than interrupt.

## Readiness Gates

`sinomo agent readiness [--json]` runs the deterministic local readiness gate.
It covers package and catalog validation, provider-boundary behavior, context
budgets, progressive disclosure, tool evidence, cancellation, compaction,
session resume, and live-control semantics.

Cluster-dependent required checks run through the normal `sinomo test` harness
under the `agent-readiness` tag. That baseline contains only capabilities every
supported agent node must provide and is expected to finish without skips.
Fixture-backed checks, such as active native-dylib cancellation, use the
`agent-readiness-extended` tag and declare their runtime capabilities explicitly.
Exact cases belong in the test registry rather than this architecture document.

Skipped selected cases are reported as `skipped` in text and JSON output. The
suite command can still exit successfully when a live prerequisite is absent, so
readiness consumers must treat `skipped` as incomplete gate evidence rather than
as a pass. The single-node debug image activates the extended readiness fixtures
before daemon startup so both readiness tags can complete without skips.

## Compaction

Compaction has two separate concerns.

Automatic compaction summarizes the current session projection together with
items produced in the active turn. Earlier turns are supplied to the summarizer
before their full text is removed from model context. The resulting projection
and retained item identities are persisted for subsequent turns and resume;
the raw transcript remains available as evidence.

Retained tails never separate a tool result from the call that produced it.
Stored tails extend back to include the call. The model-visible tail starts at
the user or assistant message that began the step. The stored tail is sized
from the same model-visible items the model was shown.

Manual compaction summarizes the current projection (latest summary plus the
items after it), not the full raw history. Without a configured retained tail
it keeps the 12 most recent items rather than keeping everything.

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
four roles and identify the providing module for module-contributed packages.

Main-agent defaults:

- user-facing interaction;
- session/task state tracking;
- local live inference when available;
- typed background-handoff requests when a dispatcher is available;
- the same workspace read, edit, command, clarification, tool-discovery, and
  skill capabilities as the built-in coding agent when the conversation has
  an explicit workspace grant;
- local configuration and cluster-health tools, plus discoverable module,
  integration, and remote-node operations that retain their normal runtime and
  node-local authority checks.

Host-effect authority is a typed `AgentPrincipalAuthority` computed by
`sinomo-agent` from reserved built-in identity, never from package JSON, role,
or serialized provenance. The built-in main and coding identities are the only
ordinary interactive packages granted workspace-mutation authority, and
`config_set` writes are granted only to the built-in main and background
identities; config tools are otherwise read-only, and values the config schema
marks secret are redacted. A custom package does not gain any of this authority
by declaring the `main` or `deliberation` role or copying the same tool names.
`remote_file_get` and `remote_file_put` read or write arbitrary host paths on the
initiating node, so they also require that node's local `remote-management`
grant in addition to the receiving node's. Background defaults remain workspace-restricted and do not
inherit the main agent's workspace mutation or generic command tools.

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

Self-evolution is outside the agent runtime core. The separately built
`module.evolution` native dylib is an R2 caller that uses the generic `Agent`
loop with context attachments for evolution jobs, mutation packets, candidate
bundles, and prior evidence. Agent and daemon host crates do not link the
evolution implementation.

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
