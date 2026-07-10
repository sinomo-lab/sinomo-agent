# Sinomo Memory Architecture

## Purpose

This document defines the long-horizon memory architecture for Sinomo. It is
the stable design reference for continuous operation, collaborative multi-agent
work, retrieval into model context, memory storage, memory consolidation, and
memory auditability.

The design assumes Sinomo is a distributed runtime, not a single chat process.
Memory must survive across internal model sessions, chats, channels, and tasks;
move across nodes according to policy; serve multiple agents without leaking
private state; and remain inspectable when it influences model context or
runtime actions.

## Design Goals

- Support long-running agents across model-session boundaries without relying
  on one large context window.
- Keep memory structured, traceable, and policy-governed.
- Treat memory as a context provider, not hidden mutable model state.
- Preserve provenance from events, internal sessions, chats/channels, tools,
  traces, and derived records.
- Keep writes append-oriented and cache friendly.
- Support collaborative multi-agent use without one ungoverned global memory
  pool.
- Avoid required external runtime dependencies in the base architecture. External
  databases, vector stores, or graph stores may be optional index providers
  behind `sinomo-memory` traits, but they are not architectural requirements.

## Literature Drivers

The design draws on hierarchical context management, episodic and semantic
memory, reflection, procedural skill libraries, evidence-backed retrieval, and
long-horizon memory benchmarks. Sinomo adopts the durable patterns while
avoiding opaque mutable summaries without provenance.

The supporting literature and benchmark analysis lives in the research notes:

- [Indexed file memory](research_notes/indexed_file_memory_literature.md)
- [Long-horizon streaming agents](research_notes/long_horizon_streaming_agent_lit.md)
- [Channel and multi-agent benchmarks](research_notes/channel_multi_agent_benchmark_literature.md)

Research findings become architectural requirements only when stated in this
document's principles and invariants.

## Core Principles

### Memory Is Explicit Context

Memory is included in model context only through a `MemoryContextProvider`.
Context assembly should receive bounded, structured, traceable memory blocks.
The model should not depend on implicit hidden state.

### Source Records Are Immutable

`MemoryRecord` is the durable base unit. Once written, it is not edited in
place. Corrections, contradictions, consolidation, embeddings, and graph links
are separate derived records.

This gives Sinomo replayability and makes memory changes auditable: the system
can explain what was kept, what was derived, what was superseded, and what was
retrieved.

### Derivations Are Rebuildable

Token indexes, embeddings, graph links, summaries, and retrieval caches are
derived from immutable records. They may be compacted, rebuilt, replaced, or
backfilled without changing the source record log.

### Writes Are Selective

The event log is the complete replayable timeline. Memory is a selected working
set derived from that timeline. Events should be promoted when they are salient,
repeated, unresolved, user-important, linked to task success or failure, needed
for future planning, or required for safety and auditability.

### Scopes Are Security Boundaries

Agent-private, task, workspace, user, team, and cluster memory have different
visibility and propagation rules. Sharing memory across scopes is an explicit
policy decision, not a side effect of retrieval.

### Retrieval Is Auditable

Every retrieval should be explainable: which records were considered, included,
omitted, superseded, or rejected by policy; why they were ranked; and which
context blocks they produced.

## Memory Planes

| Plane | Purpose | Base storage | Context behavior |
|---|---|---|---|
| Hot memory | Immediate live state: recent observations, current turn, active tool calls, open jobs | In-process bounded buffers plus selected durable events | Usually volatile tail |
| Episodic memory | Durable records of important events, corrections, outcomes, incidents, decisions, and task transitions | Append-only `MemoryRecord` log under `sinomo-memory` | Retrieved as bounded structured blocks |
| Semantic memory | Derived facts, summaries, entities, preferences, unresolved questions, and cross-session or cross-channel knowledge | Derived records plus rebuildable indexes | Stable or append-only context depending on scope |
| Procedural memory | Reusable workflows, skills, policies, recovery recipes, and tool-use playbooks | Versioned files, modules, or managed skill store with memory references | Retrieved as stable instructions or tool affordances |

The planes are related but not interchangeable. Hot memory is fast and local.
Episodic memory is evidence. Semantic memory is interpretation. Procedural
memory is reusable behavior.

## Integration Boundaries

Memory integrates with the rest of Sinomo through explicit, narrow boundaries:

- `MemoryRecord` is the immutable base record with derived ID, scope, tier,
  summary, source references, visibility, and creation time.
- Context assembly retrieves memory through a bounded context-provider API.
- Record writes and event promotion go through memory-store APIs instead of
  direct log mutation.
- Durable storage, indexes, compaction, and replication planning stay behind
  `sinomo-memory` abstractions.
- Agents consume retrieved memory blocks and write candidate summaries without
  depending on storage internals.

Storage engines and indexes must fit behind these boundaries instead of leaking
implementation-specific behavior into `sinomo-agent`.

## Record Model

### Base Record

`MemoryRecord` stays intentionally small:

- `memory_id`
- `scope`
- `tier`
- `summary`
- `source_refs`
- `visibility`
- `created_at`

Do not add fields just because an additional index might need them. Add derived
records instead.

### Derived Records

Richer memory behavior should be represented with append-only derived records:

| Record | Purpose |
|---|---|
| `MemoryLinkRecord` | Connect memories by relation such as supports, contradicts, elaborates, caused, supersedes, same-task, or same-entity |
| `MemorySupersessionRecord` | Mark one record or derived fact as replaced by another without deleting history |
| `MemoryConsolidationRecord` | Store a summary derived from a bounded set of source memory IDs |
| `MemoryEmbeddingRecord` | Store embedding metadata and vector payload or external vector reference |
| `MemoryAccessRecord` | Audit writes, retrievals, propagation, and context inclusion |
| `ProceduralMemoryRecord` | Link a durable lesson to a versioned skill, workflow, policy, or module |

Base records remain minimal. Extended behavior belongs in derived records so
source memory stays replayable, auditable, and safe to compact.

## Sessions, Chats, Channels, and Sharing

An agent session is an internal model-turn execution unit. It drives context
assembly, compaction, tool turn state, steering, follow-up input, traces, and
model-facing projection state. It has no participants. Participants belong to
chats and channels.

Within one model session, everything the model sees is a context-assembly
concern. The memory system may contribute retrieved blocks, but it does not
manage same-session persistence, maintain the active projection, or decide
which same-session information remains visible. In-session history, compaction,
context-frame deltas, and the current transcript projection are owned by the
context assembler and session runtime.

Memory retrieval becomes the visibility boundary only when crossing session
boundaries: starting a new model session, resuming older work, moving between
chats or channels, or bringing durable task, workspace, user, or cluster memory
into the current context.

Memory should be scoped before it is ranked. Recommended scopes:

| Scope | Meaning | Sharing default |
|---|---|---|
| Agent | One agent's private operating memory | Private to that agent |
| Chat | One user-visible conversation timeline | Visible to chat participants |
| Channel | A shared topic, workspace, or coordination stream | Visible to channel participants |
| Task | A durable task, project, issue, investigation, or rollout | Visible to assigned agents |
| Workspace | Project or filesystem context | Visible under workspace policy |
| User | Stable user preferences, corrections, and long-term facts | Strict user visibility policy |
| Team | Shared multi-agent coordination memory | Explicit team policy |
| Cluster | Operational memory for cluster health, rollout, incidents, and recovery | Trusted cluster services |

Supported memory scopes are `Agent`, `Chat`, `Channel`, `Task`, `Workspace`,
`User`, `Team`, and `Cluster`. Retrieval gates every non-cluster scope through
explicit query selectors; records from one chat, channel, task, workspace,
user, team, or agent are filtered unless the query carries the matching scope
ID. Session IDs remain source references for provenance and replay, but they do
not become memory sharing scopes, conversations, or participant containers.

Cross-scope propagation is a separate operation:

1. An agent writes private or task-scoped memory.
2. A policy engine decides whether the memory can be shared more broadly.
3. A derived shared memory record references the source memory ID.
4. Retrieval reports preserve both source and propagated records.

## Write Path

Memory writes should follow a policy pipeline:

1. A stored event, agent run, tool result, user correction, or consolidation job
   proposes a memory candidate.
2. Promotion policy evaluates salience, novelty, task relevance, safety,
   unresolved state, user importance, and privacy.
3. The writer creates a canonical immutable `MemoryRecord`.
4. The durable store appends the record to the segmented SOBJ log.
5. Lightweight indexes are updated or scheduled.
6. Background jobs derive semantic summaries, links, embeddings, and procedural
   candidates.

The hot interaction path should not block on embeddings, graph construction, or
large consolidation jobs.

## Retrieval Path

Retrieval is a staged process:

1. **Scope gate**: select only memory visible to the agent, chat/channel, task,
   workspace, user, and cluster context.
2. **Candidate generation**: use recency, token index, source references, task
   IDs, chat/channel IDs, prior session IDs as provenance, workspace IDs,
   embeddings, or graph links.
3. **Policy filtering**: remove records outside visibility, retention, or
   sharing policy.
4. **Scoring**: combine relevance, recency, salience, source reliability,
   unresolved status, and procedural usefulness.
5. **Conflict handling**: prefer non-superseded semantic records while retaining
   access to source evidence.
6. **Packing**: produce deterministic, bounded context blocks with stable cache
   policies.
7. **Trace**: record included, omitted, and superseded records plus reasons.

The model should receive compact memory blocks, not arbitrary transcript dumps.
A model session's transcript, projection, and compaction state are not
reconstructed from memory retrieval; they are owned by context assembly and
session state.

## Context Assembly

Memory context blocks should preserve cache friendliness:

- Cluster and user stable memory should appear in stable-prefix lanes when safe.
- Agent and task memory should be append-only when possible.
- Retrieved memory for the current turn should appear before the transcript item
  it informs.
- Volatile retrieval metadata and budget estimates should stay in the volatile
  tail.
- Repeated memory blocks should keep stable IDs based on `memory_id`.

Context reports should include memory source counts, byte counts, retrieval
criteria, and omitted-memory reasons.

## Evaluation

Memory evaluation is part of the architecture because memory failures are often
model-facing rather than pure storage bugs. Sinomo evaluates memory on four
axes:

| Axis | What it checks |
|---|---|
| Retrieval quality | Relevant memory is selected with low noise, bounded latency, stable ordering, and clear omission reasons |
| Downstream model usefulness | Retrieved memory improves answers, citations, abstention, temporal/update correctness, and action choices |
| Governance and scope correctness | Private or stale memory does not leak, superseded records are handled, and every model-facing block carries source references |
| Operational and cache efficiency | Memory blocks remain stable and append-friendly, selected bytes stay bounded, and compaction preserves provenance |

The test layers are deliberately separated:

- `sinomo-memory` Rust tests validate deterministic store, replay, index,
  scope, byte-limit, supersession, consolidation, and replication invariants.
- Cheap cluster sanity checks should verify that configured memory scopes can be
  written, replayed, queried, and included in context without expensive model
  calls.
- Expensive live/model checks belong in `sinomo-benchmarks`, where they can run
  against real providers and emit scorecard artifacts without becoming normal
  unit-test gates.

The optional `agent-memory-quality` benchmark plan covers information
extraction, multi-session reasoning, superseded knowledge, temporal reasoning,
scope-boundary abstention, and procedural recall. Deterministic fixture mode
validates the harness; live mode scores retrieval, citations, leakage,
source-reference coverage, and cache churn against a configured model.

These scorecards are evaluation evidence. They should inform regressions,
release gates, and architecture work, but they should not replace deterministic
store tests or context-assembly trace inspection.

## Consolidation

Consolidation is a background activity. It should never mutate source memory.

Useful consolidation jobs:

- Session-boundary recap: summarize a closed internal model session into chat,
  channel, task, or user memory when that information must survive across
  future sessions.
- Task outcome: summarize what worked, failed, and remains unresolved.
- Reflection: extract higher-level lessons from related episodic memories.
- Preference consolidation: derive stable user preferences from repeated
  corrections.
- Incident consolidation: turn rollout, cluster, DFS, or sandbox incidents into
  operational memory and possible procedural candidates.
- Skill promotion: convert repeated successful workflows into versioned
  procedural memory.

Every consolidation record must list its source memory IDs and model/tool
provenance.

## Multi-Agent Collaboration

Collaborative agents should not share one mutable scratchpad. Use explicit
shared memory channels:

- Agent-private memory for local reasoning continuity.
- Chat and channel memory for visible conversational continuity and participant
  coordination.
- Task memory for shared goals, constraints, decisions, and unresolved work.
- Workspace memory for project-specific conventions and artifacts.
- Cluster memory for operational facts and recovery procedures.
- User memory for preferences and corrections with strict visibility.

When agents disagree, write separate memories and link them with contradiction
or supersession records. Do not silently overwrite a prior agent's memory.

For multi-agent tasks, retrieval should include:

- task state and decisions,
- relevant source memories,
- recent unresolved contradictions,
- active handoffs,
- procedural memory for known workflows,
- policy limits for sharing or acting on retrieved memory.

## Storage Architecture

The base memory store is local and log-structured:

- append segmented length-prefixed SOBJ records,
- replay records at startup,
- maintain compact SOBJ snapshots,
- maintain rebuildable materialized indexes,
- compact logs without losing provenance needed by policy,
- store under the Sinomo data directory and SIFS-managed paths,
- replicate selected records or snapshots by scope and policy.

The store should use Sinomo's durable-file helpers and storage contract
registration. It should not require PostgreSQL, a vector database, or a graph
database in the base runtime.

SIFS replication is scope-partitioned: a replication plan names the selected
memory scope, export artifact (`snapshot.sobj`, `records.sobjlog`, or
`index.sobj`), logical memory-export source ref, target SIFS path under
`/services/sinomo/memory/...`, consistency target, replica count, and optional
target nodes. DFS/SIFS workers materialize those exports and confirm
replication according to cluster policy.

Optional index providers include:

- embedding index,
- graph/link index,
- full-text index,
- remote cluster index,
- cold archive index.

Those providers must be rebuildable from source records and must sit behind
`sinomo-memory` APIs.

## Governance and Audit

Memory governance must answer:

- Who wrote this memory?
- Which event, internal session, chat/channel, tool result, trace, or memory produced it?
- Which policy allowed promotion?
- Which scope owns it?
- Which agents can retrieve it?
- Was it propagated from a narrower scope?
- Was it superseded or contradicted?
- Was it included in a model context?
- Did it influence an action, rollout, repair, or user-visible answer?

The memory system should emit audit events for writes, retrievals, propagation,
supersession, consolidation, compaction, and policy denials.

## Failure Modes

The architecture should explicitly test and defend against:

- context pollution from low-value memories,
- stale semantic summaries overriding newer evidence,
- private memory leaking into shared contexts,
- multi-agent contradiction collapse,
- embeddings or indexes drifting from source records,
- compaction losing provenance,
- unbounded memory growth,
- retrieval instability that invalidates prompt caches too often,
- model over-trusting retrieved memory without evidence.

## Non-Goals

- Do not store entire raw streams in memory records.
- Do not make memory retrieval an unbounded transcript search.
- Do not require a server database in the base runtime.
- Do not mutate memory records in place.
- Do not let one agent silently rewrite another agent's memory.
- Do not expose memory to model context without retrieval and policy traces.
