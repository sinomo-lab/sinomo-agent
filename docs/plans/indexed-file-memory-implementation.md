# Indexed-File Memory Implementation Plan

Status: in progress. Last reviewed 2026-07-18.

The foundational Phase 0 contracts needed by the Recommended First Slice and
the slice itself are implemented: local Markdown/text roots can be registered,
scanned through capability-rooted no-follow traversal with bounded cursors,
read through verified current-only snapshots, and published through a
recoverable journal into immutable per-file generations and a persistent paged
lexical router. Queries use authenticated snapshot-pinned continuations,
recheck current policy epochs, return byte-bounded exact evidence and durable
traces, and inject that evidence into agent context. Overlay append/replay is
immutable, recoverable, authenticated, and snapshot-pinned. The cross-ring
extension ABI is available. Legacy record compatibility is deliberately
excluded: the indexed-file system neither reads nor dual-writes the old record
store. Phases 2 through 6 below remain the ordered follow-up for automatic
maintenance, richer formats and relations, optional providers, federation,
and file-first writes. Extension invocation and host-side resource enforcement
are not active in this slice; declarations whose route cannot already be
proven are rejected at registration.

The target design is defined by the
[memory architecture](../memory-architecture.md). This plan sequences delivery
of file-first, automatically maintained, extensible multimodal memory as a new
storage and serving boundary.

## Goal and Completion Criteria

The implementation is complete when:

- ordinary local and SIFS files can be registered, discovered, versioned,
  incrementally indexed, and retrieved as exact evidence anchors;
- restart and ordinary queries do not replay the complete corpus or rewrite a
  global index;
- source changes schedule bounded idempotent maintenance automatically;
- modules can add workspace detectors and format/index capabilities through a
  versioned, ring-safe interface;
- retrieval returns evidence bundles across lexical, structural, temporal,
  relational, and optional vector providers;
- agents can create managed memory files and append scoped overlay assertions
  without mutating automatic facts;
- agent context assembly uses bounded evidence bundles with full retrieval
  traces;
- placement-aware local and federated retrieval preserve source policy;
- new memory writes are file-first and do not depend on the old record store.

## Current Baseline

The existing record-first prototype is a useful correctness and provenance
reference:

- base `MemoryScope`, `MemoryTier`, `MemorySourceRef`, and `MemoryRecord` are in
  `crates/sinomo-base/src/domain.rs`;
- almost all memory behavior is in `crates/sinomo-memory/src/lib.rs`;
- `DurableMemoryStore` appends length-prefixed SOBJ records and maintains a
  snapshot plus one global `memory/index.sobj`;
- `MemoryContextProvider` and `MemoryRecordStore` connect the store to
  `sinomo-agent`;
- the agent writes a warm summary after a completed turn and retrieves at most
  eight records/16 KiB using a text task hint;
- link, supersession, consolidation, embedding-snapshot, graph-snapshot,
  replication-plan, evaluation, and audit types already establish useful
  append-only and provenance patterns.

It is not yet a filesystem index:

- `DurableMemoryStore::retrieve_records` replays the complete store, scans and
  sorts all visible records, then writes a retrieval access record;
- each write replays state and rewrites the global token/index snapshot;
- retrieval does not use the stored token index;
- graph and embedding providers rebuild whole in-memory snapshots and are not
  on the normal serving path;
- no root, source file, immutable source version, anchor, workspace detector,
  change cursor, tombstone, partition, maintenance scheduler, relation path,
  evidence bundle, or overlay exists;
- agent queries do not currently provide chat, channel, task, workspace, user,
  or team focus;
- scope matching does not constitute a full authorization check, and promoted
  events currently lose important routing/privacy detail.

The indexed-file implementation is a clean serving boundary. It does not reuse
the isolated record-first subsystem, and that subsystem is not a compatibility
or acceptance gate for the new memory system.

## Fixed Design Decisions

1. New file/index domain types live in R2 `sinomo-memory` unless they are
   required on an extension wire boundary. The R0 `MemoryRecord` contract does
   not grow into, or get projected through, the new schema.
2. SIFS source identity is filesystem id + object id + immutable version;
   paths are mutable aliases. `DfsObjectMetadata`, `DfsObjectVersionRecord`,
   and metadata-subscription cursors are reused through `sinomo-runtime`.
3. Automatic output is normalized and staged into immutable generations. Only
   trusted memory index backends publish serving manifests.
4. Extensions never own watchers, jobs, source policy, or index files.
5. Model/user/agent changes are append-only overlay operations stored apart
   from automatic generations.
6. Retrieval audit is append-oriented and must not force index mutation or a
   corpus replay.
7. The base runtime has no required server database or vector engine.

## Cross-Ring Extension Contract

The module registry needs one generic typed-interface export rather than a
memory-specific callback in the R1 daemon:

```text
ModuleInterfaceExport {
  export_id,
  interface_id,
  interface_version,
  operations: [
    {
      logical_operation,
      target,                 # native invoke, object-RPC route/op, sandbox entry
      request_schema_id,
      response_schema_id,
      max_request_bytes,
      max_response_bytes,
      max_facts_per_batch,
      supports_cancellation,
      continuation_schema_id?,
    }
  ],
  descriptor_sobj,            # compact and hard-bounded
}
```

Add a defaulted `interface_exports` field to `ModuleSurfaces` and
`NativeModuleSurfaces`, with compatibility tests for older manifests and native
descriptors. Memory interface ids include:

- `sinomo.memory.source-connector/v1`;
- `sinomo.memory.workspace-detector/v1`;
- `sinomo.memory.format-detector/v1`;
- `sinomo.memory.content-extractor/v1`;
- `sinomo.memory.relation-extractor/v1`;
- `sinomo.memory.embedding-provider/v1`;
- `sinomo.memory.anchor-lineage/v1`;
- `sinomo.memory.query-adapter/v1`;
- `sinomo.memory.reranker/v1`;
- `sinomo.memory.preview-renderer/v1`.

Create a data/ABI-only R1 crate, tentatively
`sinomo-memory-extension-api`, for the versioned descriptor,
`IndexedContentRef`, extraction requests, continuations, normalized anchors,
facts, relations, vector/artifact references, diagnostics, and
`DerivedFactBatch`. Re-export it through
`sinomo_runtime::memory_extension_api`; R2 memory code must use the facade.
The crate owns no scheduler, storage, retrieval, or policy behavior.

The active identity includes module/export/interface version, artifact id and
content hash, descriptor hash, and invocation lane; all enter the pipeline
fingerprint. Registration validates every operation target against exported
native/object-RPC/sandbox routes and enforces hard payload, batch,
continuation, deadline, and cancellation contracts.

Adding the R1 ABI crate also requires updating the workspace membership/default
member declarations and `workspace_ring_boundaries.rs`, including
`ABI_OBJECT_EXEMPT` only if the narrow object-codec exception is actually
needed. Prefer crossing R0 through `sinomo-core` when the ABI can do so without
compromising its boundary.

Native dylibs reuse `NativeModuleRequest::Invoke` and SOBJ payloads. Static R2
implementations use memory-owned trait adapters. Object-RPC and sandbox/Wasm
implementations use the same logical protocol. Third-party executable
extractors require a read-only source-lease/preopen adapter in
`sinomo-sandbox`; the current ambient command runner is not an acceptable
containment boundary.

`ModuleDeclarativeResourceKind::Config` plus an activation tag can carry an
interim descriptor during development, but it is migration scaffolding rather
than the permanent discovery contract.

## Phase 0: Contracts, Fixtures, and Clean Boundary

Introduce the logical types before replacing storage:

- `IndexRoot`, `MemoryFileRef`, `MemoryFileVersionRef`, source descriptor, and
  coverage/freshness state, including `native-versioned`, `snapshot-capable`,
  and `current-only` retention capabilities;
- typed `MemoryAnchor` with whole-file, byte/text, structured, code, document,
  image, media-time, 3D-scene, and event/session selector envelopes;
- `IndexFact`, extraction provenance, pipeline fingerprint, generation
  manifest, and policy derivation;
- open `MemoryNodeRef` and `MemoryRelation` with evidence, confidence, valid and
  observed time, disputes, and supersession;
- overlay operation and validation-state vocabulary;
- structured `MemoryQuery`, retrieval plan/feature trace, and
  `MemoryEvidenceBundle`, including pinned query-snapshot and
  complete/approximate/budget-truncated coverage contracts;
- paged/cursor-based source, index, overlay, and trace interfaces.

Add deterministic fixtures for:

- rename with and without stable object identity;
- content and extractor-version invalidation;
- exact anchor validation and cross-version lineage;
- competing relations and overlay replay;
- scope/policy intersection and relation leakage;
- freshness and source deletion;
- deterministic evidence packing.

Expected outcome: new APIs compile and can be tested independently of the old
record store.

## Phase 1: First File-to-Evidence Vertical Slice

Build the smallest useful serving path before graph or model work:

1. Register explicit local/VFS index roots.
2. Catalog stable source identity, path, version/hash, media type, scope, and
   policy in paged source partitions.
3. Implement bounded verified local snapshot leases. Treat them as current-only
   across mutation/restart unless policy explicitly pins bytes into versioned
   content storage.
4. Add deterministic plain-text/Markdown and JSON/YAML/TOML detectors and
   extractors.
5. Publish metadata and lexical facts to immutable partition segments through
   an atomic manifest.
6. Build a versioned term/range-to-partition router and query metadata and
   lexical postings without replaying source facts.
7. Return exact text/structured anchors in a `MemoryEvidenceBundle`.
8. Use the indexed `MemoryRetriever` boundary and adapt the context assembler
   to consume evidence bundles directly.
9. Append retrieval traces independently from serving indexes.

Split the monolithic `sinomo-memory/src/lib.rs` by domain boundary as these
components land; avoid a mechanical split that obscures ownership.

Verification gates:

- clean restart opens bounded manifests rather than every fact segment;
- querying does not write the catalog or serving index;
- one file update rewrites no global corpus snapshot;
- all returned text is pinned to a verified source version and bounded anchor;
- a current-only source cannot serve old bytes after change unless an
  authorized snapshot was retained;
- an unconstrained exact lexical query either searches every routed partition
  in its pinned snapshot or returns an honest continuation/truncation state;
- compaction/publication during a query cannot mix partition generations;
- a large synthetic root is processed with bounded memory and cursor-based
  enumeration;
- evidence-only context assembly remains deterministic and byte-bounded.

Expected outcome: a user can index a large text/structured root, modify one
file, publish a replacement generation manually, and retrieve source-backed
evidence efficiently.

## Phase 2: Automatic Maintenance and Workspaces

Add a durable maintenance service owned by `sinomo-memory`:

- checkpointed source changes and reconciliation cursors;
- coalesced idempotent jobs keyed by source version and pipeline fingerprint;
- priorities, per-root and per-extension budgets, retry/backoff, cancellation,
  leases, and crash recovery;
- local watcher hints plus periodic bounded reconciliation;
- an R1 read-only `DfsCatalog`/`DfsChangeFeed` facade, re-exported through
  `sinomo_runtime::dfs`, with paged logical objects, opaque per-shard/generation
  checkpoints, stable scan tokens, bounded logical changes, and exact revision
  range reads;
- SIFS indexing through that logical facade, including visible-version,
  namespace, tombstone, policy, and placement changes;
- atomic policy/deletion fences independent of background cleanup;
- content-chunk reuse and separate metadata/content/enrichment stages;
- index status and repair inspection.

The DFS facade must use paged enumeration and bounded journal batches across
every metadata shard, resolve cross-shard moves through authoritative object
placement, and force mark-and-sweep reconciliation after a cursor gap or
snapshot generation change. Do not expose the current first-shard/unbounded
backend subscription as the memory correctness contract. Host-exposed
local-owned files still need a scoped native/VFS watcher plus reconciliation.
Checkpoint advancement must be durably coupled to catalog application or
idempotent job enqueue so a crash cannot acknowledge and lose a change.

`begin_scan` must return one catalog generation, a stable paged-scan token, and
per-shard feed watermarks. Changes after those watermarks remain replayable
until the completed scan and feed checkpoints commit together. A connector
without a stable snapshot starts/buffers its watcher before scanning and repeats
reconciliation until convergence; a single racy walk cannot mark coverage
current.

Generated memory partitions, staging output, traces, and exported snapshots
must be excluded from source roots so maintenance cannot recursively index its
own derived artifacts.

Add host-evaluated marker rules and the first generic workspace detector for
Git/Cargo repositories. It should emit nested boundaries, ignore rules,
workspace/package membership, and dependency hints. Move Git-presence
assumptions out of private agent-tool logic; the agent consumes the resulting
workspace profile.

Update agent retrieval focus to pass authorized agent, chat/channel, task,
workspace, user/team, active-file, and participant selectors where known.
Index failures and omitted coverage become visible in context reports rather
than being discarded.

Verification gates:

- watcher event loss is repaired by reconciliation;
- cross-shard moves and journal/snapshot gaps cannot create false deletions;
- duplicate and reordered change notifications are idempotent;
- a file changing during extraction cannot publish as the newer version;
- rename reuses content work only when identity and inputs prove it safe;
- policy restriction fences discovery immediately;
- job recovery survives process restart;
- indexing lag, queue age, coverage, and failure metrics are emitted.

Expected outcome: text/structured repositories stay searchable transparently
after local or SIFS changes.

## Phase 3: Structural Graph, Overlays, and Curator

Add normalized entity/relation storage and serving adjacency:

- namespaced open relation types and versioned relation descriptors;
- directional and reverse adjacency, relation paths, temporal constraints, and
  evidence filters;
- deterministic code anchors and an initial language-agnostic code index
  protocol, with language-specific extractors supplied as extensions;
- repository dependency and reverse-dependency relations;
- explicit contradiction, dispute, supersession, and entity-resolution facts.

Add event/session and evolution-lesson source adapters so episodic task history
and the existing file-oriented evolution lesson memory participate through the
same source/anchor contracts rather than remaining separate retrieval silos.

Add authoritative immutable one-operation objects or independently fenced
per-writer overlay streams, deterministic operation ids, idempotent merge, and
materialized overlay partitions. Do not require multi-writer append to one SIFS
file. A proposed dispute/supersession may rank or explain a conflict but cannot
suppress an automatic fact.
Expose bounded `memory_search`, `memory_explain`, `memory_read`,
`memory_annotate`, `memory_link`, `memory_dispute`, and `memory_reindex` tools.
Model writes default to proposed and cannot hide base facts globally.

Introduce a specialized curator package/job loop only after deterministic
overlay tests exist. Initial triggers should be task/session completion,
unresolved contradiction, important anchor drift, and repeated retrieval miss.
The curator writes strict proposals and cannot approve itself.

Verification gates:

- “who calls this function?” uses directional source-backed edges;
- overlay replay produces the same composite view after restart;
- a source revision marks an exact-version overlay as needing revalidation;
- one agent cannot overwrite or broaden another scope;
- graph expansion is bounded by hops, branches, bytes, and policy;
- relation-only results cannot leak hidden source facts.

Expected outcome: code/workspace memory and agent-maintained high-level
relations work without making the graph or model an authority.

## Phase 4: Optional Vector and Multimodal Providers

Separate embedding generation from vector serving. A named vector-space
descriptor records modality, model, dimensions, normalization, metric, version,
and policy. Lexical and graph retrieval continue to work with no vector
provider. A coarse vector-to-partition router reports approximate coverage;
only a completed exhaustive paged search reports complete vector coverage.

Add extensions incrementally:

- PDF/document layout, pages, tables, figures, and regions;
- image metadata, OCR, regions, captions, and text-image embeddings;
- audio ASR/events and time anchors;
- video scene/shot segmentation, key frames, audio-video events, and clip
  handles;
- 3D scene hierarchy, objects, spatial bounds, materials, references, and
  animation intervals.

Each extension must support bounded input ranges or continuations for large
content and must emit normalized evidence rather than mutate an index.

Verification gates include the architecture's reference-image, gun-fire-scene,
temporal-discussion, and album-cover relation queries. Fixtures must verify
anchor accuracy, transfer policy, stale media versions, provider absence, and
bounded preview/context behavior.

Expected outcome: the same query/evidence API supports multimodal sources and
cross-modal retrieval without flattening every source to text.

## Phase 5: Placement and Federation

Advertise compact memory-service capabilities and partition summaries through
service discovery/cluster control. Keep per-file and per-index state out of KV.

Implement:

- near-content job placement using DFS locations, capability, resource, and
  privacy inputs;
- placement projections updated independently from content extraction;
- bounded local-first federated query and stable result fusion;
- pinned federated query snapshots/manifest leases, coverage states, and
  continuations across remote partitions;
- remote reauthorization and evidence handles;
- policy epochs plus expiring offline authorization leases for shared cached
  indexes, with fail-closed behavior after expiry;
- raw-content-local result modes;
- content-addressed partition snapshot export/import with schema and pipeline
  compatibility checks;
- optional derived-index replication by scope/usefulness, separate from source
  and overlay replication.

Verification gates cover duplicate indexers, offline nodes, stale placement,
policy changes during query, expired/offline authorization, unavailable remote
evidence, deterministic merge, honest incomplete coverage, and no dependence
on cluster KV for high-cardinality state.

Expected outcome: a single node remains the base case, while a cluster can
index and retrieve near data without centralizing raw files.

## Phase 6: File-First Writes and Old-Store Removal

Change completed-turn and explicit memory writes to create scope-appropriate
managed memory documents or overlay operations. Preserve full session/tool
evidence in its owning files; do not reduce every turn to a mandatory warm
summary.

Do not dual-write or dual-read the old record store. Existing
`memory/records/...` and `memory/index.sobj` data is outside the indexed-file
serving path. No reader, importer, migration shim, or preservation guarantee is
provided for it; discard it and rebuild the indexed-file state from its source
files.

Complete the file-first write transition after:

- file/overlay rebuild passes without record storage;
- scope, temporal, supersession, procedural, and context-cache evaluations
  match or improve on the legacy baseline;
- operator tools can explain managed documents and overlay operations;
- the storage-version transition is explicit;
- crash matrices cover every file/overlay commit and indexing boundary without
  duplicate context blocks or divergent authority.

Expected outcome: all new memory is file-first, with no live dependency on the
old record subsystem.

## Recommended First Slice

Implement Phase 0 plus the Phase 1 vertical slice for one local root and
Markdown/text only:

```text
register root
  -> catalog current file version and retention capability
  -> acquire verified snapshot lease
  -> extract text anchors
  -> publish lexical partition generation
  -> query evidence bundle
  -> inject bounded context block
  -> modify file
  -> publish replacement generation
  -> restart and query without corpus replay
```

This slice validates the hardest cross-cutting choices—identity, anchors,
generation publication, bounded serving, and agent context—
before code graphs, models, vectors, media, or federation add more variables.

## Benchmark and Readiness Work

Create versioned benchmark presets rather than embedding hardware-specific
numbers in architecture. At minimum measure:

- cold/warm startup work versus file/fact count;
- catalog throughput and resident memory on deep and wide trees;
- single-file update write amplification;
- lexical query latency and bytes read by partition count;
- change-to-searchable lag under burst and continuous load;
- compaction debt and query behavior during compaction;
- workspace detection accuracy and bounded probing;
- graph path recall/cost, vector recall/cost, and multimodal anchor accuracy;
- source-reference coverage, policy leakage, stale-hit rate, and context bytes;
- agent task quality, citations, abstention, and cache churn.

Extend `modules/benchmark/sinomo-benchmarks/agent/run_memory_eval.py` so some
cases exercise real file candidate generation rather than only preselected
memory blocks. Keep fixture and live-model results separate from deterministic
Rust gates.

## Implementation Choices Still Requiring Spikes

These choices do not alter the architectural contracts and should be decided
with Phase 1 benchmarks:

- custom SOBJ term/posting segments versus an embedded lexical engine;
- local watcher implementation and platform-specific identity quality;
- fixed, content-defined, or extractor-owned incremental chunk reuse;
- graph adjacency encoding and compaction thresholds;
- initial vector backend and quantization;
- derived snapshot export granularity;
- whether the first sandboxed extension uses Wasm or a contained process.

Document the selected implementation and measurements in source/Rustdoc or a
focused follow-up plan. Do not make a provider-specific storage shape part of
the public memory API.
