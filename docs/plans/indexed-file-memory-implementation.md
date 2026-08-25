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
store. The scale-hardening requirements below define the storage and serving
shape used by Phases 2 through 5. Phases 2 through 6 remain the ordered
follow-up for automatic maintenance, richer formats and relations, optional
providers, federation, and file-first writes. Extension invocation and
host-side resource enforcement are not active in this slice; declarations
whose route cannot already be proven are rejected at registration.

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

Publish the versioned descriptor and schemas with external memory components.
Do not create or re-export a provider API crate. `IndexedContentRef`, extraction
requests, continuations, normalized anchors, facts, relations,
vector/artifact references, diagnostics, and `DerivedFactBatch` are identified
by standardized schema IDs and transported through the generic module
invocation contract. R2 memory owns validation and interpretation; the generic
module runtime owns no scheduler, storage, retrieval, or policy behavior.

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

Expected outcome: a user can index a representative bounded text/structured
root, modify one file, publish a replacement generation manually, and retrieve
source-backed evidence through the final contracts. This phase proves the
correctness path, not final large-root efficiency.

## Scale-Efficient Storage and Serving Shape

Phase 1 is a correctness baseline, not the intended storage shape for roots
with very large files or millions of files. Its bounded APIs, immutable
generations, policy checks, authenticated continuations, and evidence contracts
remain useful. The per-source catalog files, whole-file snapshots, per-file
partitions, and per-file router publication do not.

This section constrains work across later phases rather than adding a separate
phase. Ordered catalog, streaming source, segment, and maintenance foundations
belong in [Phase 2](#phase-2-automatic-maintenance-and-workspaces); the
distributed pieces constrain [Phase 5](#phase-5-placement-and-federation).
The owning architecture sections remain
[Partitions](../memory-architecture.md#partitions),
[Automatic Maintenance](../memory-architecture.md#automatic-maintenance),
[Physical Storage](../memory-architecture.md#physical-storage), and
[Retrieval Architecture](../memory-architecture.md#retrieval-architecture).

Scale hardening keeps the architecture's authority model unchanged:

- files and overlay operations remain authoritative;
- `MemoryFileVersion` and `MemoryAnchor` remain the evidence identities;
- chunks, catalogs, jobs, segments, routers, and provider databases are
  rebuildable node-local derived state;
- policy gates candidate generation and is rechecked before materialization;
- deletion and policy restriction fence serving immediately rather than
  waiting for compaction;
- queries pin coherent generations and report complete, approximate, or
  budget-truncated coverage honestly.

The target flow is:

```text
source files / DFS versions
  -> ordered catalog and durable change journal
  -> verified bounded readers and content-addressed chunk derivations
  -> immutable multi-file index segments
  -> compact term/range-to-partition routing
  -> ranked anchor candidates
  -> range-materialized evidence bundle
```

### Current First-Slice Scaling Gaps

The following implementation choices must be replaced before claiming
million-file scale or processing files larger than memory under explicit
resource budgets:

| Area | First-slice behavior | Required scale shape |
|---|---|---|
| Catalog pagination | `bounded_ids` walks a flat directory and sorts candidates again for every page | Ordered packed catalog with manifest-pinned keyset cursors |
| Reconciliation | Metadata-unchanged files are still opened, read, and hashed | Change-driven scheduling plus metadata reconciliation and periodic integrity scrubs |
| Source acquisition | One `VerifiedSourceSnapshot` owns the complete file bytes | Verified streaming/range reader with bounded buffers |
| Extraction generation | One generation serializes every anchor and term for one file | Content-addressed chunk outputs and bounded immutable segment files |
| Partitioning | One source file is one serving partition | Stable policy-domain shards containing many source/chunk documents |
| Routing publication | Every file change rewrites term-to-file postings and publishes a router manifest | Term-to-coarse-partition routing, batched publication, and partition-local postings |
| Retrieval | Candidate partitions load all anchors and reread whole source files | Postings-first top-k selection followed by selected-range materialization |
| Evidence packing | The growing bundle is cloned and serialized for every candidate | Incremental accounting and one final serialization |
| Concurrency | A store-wide writer lock covers scans, hashing, extraction, and publication | Work outside locks plus short per-root/partition compare-and-publish commits |
| Lifecycle | Immutable generations and router pages have no bounded reclamation lease | Expiring snapshot leases, compaction, and reachable-manifest garbage collection |

For example, with one million catalog objects and a 64-object page, rescanning
the complete unordered directory for every page can require roughly 15.6
billion directory-entry visits for one sweep.

Page-size and source-size constants do not repair these asymptotic paths.
Increasing a whole-file limit can instead increase peak memory and query I/O.

### Ordered Packed Catalog

The source catalog needs an ordered embedded representation rather than one
filesystem object per source. A catalog page pins a catalog manifest and uses
a cursor equivalent to `(shard, last_key)`. Page work must be proportional to
the requested page plus index height, not to the total number of sources.

The logical source row contains at least:

- root and stable source identity;
- current locator aliases and connector version hints;
- byte length, media type, timestamps, workspace facets, and placement;
- policy and scope derivation epochs;
- desired and currently published pipeline generations;
- catalog status plus searchable, enriched, pending, failed, and stale status
  per index family and pipeline, and source tombstone status.

The physical implementation may initially use an embedded ordered database,
such as SQLite in WAL mode with key-oriented tables, or a custom paged
SOBJ/LSM catalog. That choice is private to `sinomo-memory`: it is replaceable
derived state and cannot become a public API or authority.

Reconciliation writes an ordered observed-identity run and merge-diffs it
against the previously committed catalog generation. A deletion becomes
visible only when the complete pass commits. This avoids both false deletion
after an interrupted walk and rewriting every unchanged row merely to update a
scan marker.

For connectors without atomic scans:

1. start or buffer watcher/change hints;
2. perform the bounded catalog walk with a durable directory checkpoint;
3. replay changes after the starting watermark;
4. repeat until the catalog and feed converge;
5. atomically publish the completed catalog generation and feed checkpoint.

OS directory handles are process-local implementation details, not durable
continuations. A restart resumes from completed directory/key checkpoints and
may safely replay the last incomplete directory.

Normal reconciliation compares connector identity and version metadata; it
does not parse or hash every unchanged file. Exact content publication still
requires a verified version. Periodic low-priority scrubs detect same-size or
same-time mutations that weak local metadata could miss.

### Bounded Source Readers and Chunk Manifests

Whole-file ownership should be replaced by a private connector interface with
the semantics of:

```text
VerifiedSourceReader {
  source_version
  retention_capability
  verify_identity_before_read()
  read_range(offset, max_bytes)
  stream_chunks(chunk_policy, byte_budget)
  verify_identity_after_read()
  establish_content_hash_before_publish()
}
```

The reader pins an immutable SIFS version or a verified current-only local
lease and never requires one allocation proportional to file size. Extractors
consume bounded buffers and may spill sorted output runs to staging storage.

Chunk boundaries are extractor-owned when the format has useful structure:

- Markdown headings, paragraphs, and bounded line groups;
- source-code declarations or syntax regions;
- JSON/JSONL records and bounded structured subtrees;
- PDF pages;
- image tiles or regions;
- audio/video scenes and time windows;
- content-defined chunks as the fallback for unstructured streams.

Every chunk carries its source range, content hash, extractor-owned boundary
identity, and `pipeline_fingerprint`. A version manifest references immutable
derived chunk outputs; indexing does not automatically retain a copy of all
source bytes. Reusable content-only extraction is keyed by chunk content hash
and pipeline fingerprint. Any reuse across source versions or aliases is
rebound to new exact-version anchors and facts and reauthorized for that
`MemoryFileVersion`. Path, scope, workspace, policy, and placement projections
remain per file even when two files contain identical bytes.

Chunks are derived processing units, not source identities. Exact evidence
still cites the `MemoryFileVersion` and bounded source anchor. Long records or
lines must be split into bounded anchors so one pathological record cannot
make a continuation permanently unable to deliver progress.

An edit affecting a small portion of a file should cause approximately that
portion to be re-extracted and republished when chunk identity proves reuse.
It does not always imply equally small source I/O: a current-only local
connector without authoritative block hashes or changed-range information may
still need to stream the complete file to verify its new content. SIFS range
reads avoid that full read only when paired with authoritative chunk manifests,
change extents, or a format index that proves the changed ranges; range access
alone does not discover new chunk boundaries.

### Multi-File Immutable Serving Segments

An `IndexPartition` remains derived from root, policy domain, and a stable
shard key. Workspace may influence routing and placement, but mutable
workspace membership must not destabilize content identity. A partition owns
many source or chunk documents rather than exactly one file.

Updates append bounded immutable delta segments containing query-oriented
state such as:

- a compact term dictionary;
- compressed term-to-document/anchor postings and optional positions;
- source, chunk, and anchor ordinal tables;
- metadata/time columns and range handles;
- tombstones, generation metadata, and score/block statistics.

The partition manifest atomically selects a complete set of segments. Small
updates publish delta segments; background LSM-style compaction merges them
according to measured segment count, tombstone ratio, read amplification, and
bytes. Segment sizes and compaction thresholds belong in versioned benchmark
presets, not architecture constants.

A physical segment may pack facts from many logical per-source and
per-pipeline `IndexGeneration`s. Publication and query selection still expose
exactly one complete successful generation for every active pipeline; segment
packing and compaction cannot weaken generation atomicity.

GC roots include current and retained serving manifests, bounded query/export
or remote-serving leases, active job/staging leases, and policy-retained audit
or snapshot references. A mark-and-sweep pass deletes objects outside those
roots; abandoned staging becomes collectible only after its attempt is
terminal or its lease expires. Compaction is never the security boundary: a
current policy/deletion fence excludes stale postings immediately.

SOBJ remains the portable contract and manifest/segment envelope. A provider
may store specialized internal dictionaries and postings, but it must preserve
atomic publication, provenance, bounded cursors, rebuild, and export/import
compatibility.

### Coarse Routing and Batched Publication

The global lexical router maps a term to policy-domain partition ids, not to
every file generation. Routing data does not grant access: authorization
selects which policy domains and router pages a query may inspect, and routing
summaries remain access-controlled so term or partition existence cannot leak.
Exact document and anchor postings live inside the partition. The router does
not duplicate a current generation id or full root policy in every posting;
the pinned partition-generation table supplies that state.

A normal source change therefore appends a partition delta and swaps the
partition manifest. The global router changes only when a term first appears
or finally disappears from that partition. A maintenance commit batches these
term-boundary changes and publishes one router generation rather than one
generation per source.

Catalog, router, and partition publication are paired by one atomically
published top-level serving snapshot. A new partition generation that changes
term boundaries cannot coexist with an older router still claiming complete
coverage. If publication is staged across multiple durable steps, the router
is marked incomplete and exhaustive paged fallback remains required until the
matching serving snapshot commits. Crash recovery never exposes a complete
router with unmatched partition manifests.

Scope, policy domain, time, and explicit source or workspace filters route
before lexical candidate work. Active-workspace focus without an explicit
filter is a relevance prior and cannot silently prune other eligible
partitions. Policy-domain separation prevents denied postings from consuming
query budgets or exposing hidden-corpus volume. Unconstrained authorized
enumeration uses the ordered catalog rather than a synthetic all-files term
posting.

If a router is incomplete, exhaustive search pages eligible partition
manifests and returns a continuation. It never reports complete coverage after
searching only a convenient fixed subset.

### Postings-First Retrieval and Late Materialization

Retrieval selects anchor candidates before reading source evidence:

1. pin catalog, router, policy, and partition generations;
2. authorize and select eligible partitions;
3. choose selective terms and intersect or advance posting lists;
4. use a bounded top-k algorithm such as DAAT/WAND or Block-Max WAND;
5. apply stable score features, contextual priors, deduplication, and
   diversification;
6. reauthorize the selected source versions;
7. batch and range-read only selected anchors;
8. pack and serialize the evidence bundle once.

Posting encodings may use delta/bit-packed ordinals or bitmaps according to
density. Ranking may use BM25-style statistics, but the public contract carries
stable feature/reason data rather than provider-specific scores alone.

Continuation state pins the query snapshot and contains bounded per-provider
or per-partition cursors. Work allocation is query-plan-aware: dividing one
global scan budget equally across every term is not sufficient for selective
intersection or a very common term.

Evidence packing maintains a source-id-to-bundle-slot map and exact incremental
encoded-byte accounting, or a conservative upper bound followed by
deterministic hard-limit enforcement. It does not clone and serialize every
prefix of the bundle, cannot emit an oversized result, and serializes the final
bundle once. Final materialization and tracing remain bounded independently.

### Scale-Specific Maintenance Requirements

[Phase 2](#phase-2-automatic-maintenance-and-workspaces) owns checkpoint,
watcher, job-recovery, policy-fence, and workspace semantics. The scale-specific
requirements are coalesced desired state, short publication locks, byte-based
backpressure, and packed lifecycle storage.

Maintenance keeps a compact desired-state record keyed by source and pipeline
plus immutable execution attempts. The idempotency key is equivalent to:

```text
(root_id, source_id, source_version_id, pipeline_fingerprint, stage)
```

A newer version supersedes queued work for an older version. Deletion and
policy restrictions outrank all extraction work and install serving fences
immediately. At-least-once execution is safe because generation identity and
compare-and-publish validation are deterministic.

Job leases carry owner, lease epoch, expiry, heartbeat, and cancellation.
Publication succeeds only if the desired source version, pipeline, lease, and
current policy epoch still match. Scanning, source verification, extraction,
and segment construction occur outside serving locks; publication uses short
per-root or per-partition manifest commits.

Backpressure is byte- and resource-based, not only job-count-based:

- source-read, output, staging, memory, accelerator, and temporary-byte
  budgets;
- open-handle and per-extractor concurrency limits;
- weighted fairness between roots and active workspaces;
- separate immediate-fence, interactive lexical, reconciliation, enrichment,
  and compaction lanes;
- high-water behavior that pauses enrichment first and coalesces changes to
  the newest desired version;
- a durable `reconciliation_required` state when a feed overruns rather than
  silently dropping correctness-relevant work.

Derived traces use rotated append segments, bounded pages, and group commit
where durability permits. Authoritative overlay operations retain the
architecture's immutable one-operation objects or independently fenced
per-writer streams. Group commit may batch durable acknowledgement, but an
overlay operation is never acknowledged before its local or DFS durability
contract is satisfied, and it never relies on concurrent append to one shared
file.

### Distributed Summaries and Candidate-First Federation

[Phase 5](#phase-5-placement-and-federation) owns placement, offline leases,
snapshot transfer, and federation semantics. The scale-specific serving shape
is hierarchical routing followed by candidate-first remote retrieval.

Cluster KV contains only compact service advertisements, capabilities, policy
epochs, coarse root/partition groups, resource state, and a digest/pointer to a
content-addressed routing summary. It never contains source catalog rows,
terms, postings, jobs, or per-file placement state.

Federated routing is hierarchical:

```text
service discovery
  -> authorized nodes by scope, capability, locality, and policy
  -> pinned node routing-summary generation
  -> node-local exact router
  -> partition segments
```

Queries search local partitions first and then bounded remote waves. Each node
reauthorizes before returning candidate ids, scores, counts, omissions, or
other metadata. Authorized responses carry compact candidate ids, feature
components, freshness, coverage, and authenticated continuations. The
coordinator performs deterministic rank fusion and deduplication before asking
the owning nodes to reauthorize and materialize the final evidence. Sensitive
or large content may remain behind an opaque handle and the second
authorization check.

Bloom-like summaries may eliminate obvious remote misses only when their
pinned generation declares the relevant coverage complete. A stale,
unavailable, or fanout-truncated node reports incomplete coverage rather than
an empty result.

### Provider Spike Direction

Within the replaceable-provider boundary defined by
[Physical Storage](../memory-architecture.md#physical-storage), the initial
scale benchmark should compare a split such as:

- an embedded ordered store, with SQLite as one candidate, for catalog, jobs,
  checkpoints, desired state, and leases;
- a Tantivy-like embedded provider or custom immutable segments for lexical
  dictionaries, postings, and top-k retrieval;
- SOBJ for public contracts, durable manifests, portable metadata, and
  snapshot/export envelopes;
- optional graph and vector providers added behind the same generation and
  policy contracts.

No provider is a source of truth, mandatory external service, or exposed
storage API. Provider loss triggers rebuild or compatible snapshot import from
authorized sources and overlays. Final selections remain tracked in
[Implementation Choices Still Requiring Spikes](#implementation-choices-still-requiring-spikes).

### Delivery Order and Scale Gates

Implement scale hardening in this order:

1. ordered packed catalog, durable catalog cursors, and complete-scan deletion
   semantics;
2. durable change/checkpoint journal and coalesced desired-state scheduler;
3. verified streaming/range readers and content-addressed format-chunk
   derivations/manifests;
4. multi-file immutable delta segments and batched partition/router
   publication;
5. postings-first top-k retrieval and selected-range evidence materialization;
6. compaction, snapshot leases, garbage collection, and byte-based
   backpressure;
7. read-only federated candidates and explicit coverage reporting;
8. derived segment placement, snapshot replication, and advanced providers.

Scale acceptance uses operation counters in addition to wall-clock numbers:

- a catalog page examines work proportional to the page plus index height;
- a complete unchanged reconciliation reads approximately zero source content
  bytes during its normal path;
- peak indexing resident memory is independent of the largest permitted source
  through streaming and spill;
- one source update rewrites no global corpus and causes write amplification
  proportional to changed chunks and changed partition terms plus index
  height, not total corpus size;
- for retained range-addressable sources, query source bytes are proportional
  to selected anchor evidence rather than candidate-file sizes; current-only
  local verification bytes are reported separately and remain streamed;
- index inode count grows with packed catalogs and segments, not with
  files multiplied by terms or revisions;
- interrupted scans cannot tombstone unseen live files;
- policy restriction and deletion fence serving before cleanup;
- query and compaction can run concurrently without mixing generations;
- unreachable generations are reclaimed after bounded continuation/export
  leases expire.

The workload matrix and measured counters live in
[Benchmark and Readiness Work](#benchmark-and-readiness-work).

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
- catalog throughput, entries examined per page, and resident memory on deep
  and wide trees;
- unchanged-reconciliation source bytes read and hashed;
- localized large-file update extraction, segment, and router write
  amplification;
- lexical query latency, segment fanout, source bytes read, and evidence bytes
  returned by partition count;
- change-to-searchable lag under burst and continuous load;
- index inode growth, publication lock time, router page rewrites, compaction
  debt, and query behavior during compaction;
- workspace detection accuracy and bounded probing;
- graph path recall/cost, vector recall/cost, and multimodal anchor accuracy;
- source-reference coverage, policy leakage, stale-hit rate, and context bytes;
- agent task quality, citations, abstention, and cache churn.

Presets must cover at least millions of tiny files, deep and wide trees, a very
large source with a localized edit, burst and continuous changes, an extremely
common term, cold restart, crash during reconciliation, query during
compaction, and policy restriction during remote retrieval. Record the
coverage state for every query workload rather than treating missing or timed
out providers as empty results.

Extend `modules/benchmark/sinomo-benchmarks/agent/run_memory_eval.py` so some
cases exercise real file candidate generation rather than only preselected
memory blocks. Keep fixture and live-model results separate from deterministic
Rust gates.

The first external baseline is implemented by
`modules/benchmark/sinomo-benchmarks/agent/run_longmemeval_s.py`. It streams the
pinned cleaned LongMemEval-S dataset, builds and queries the real indexed-file
store, and can optionally run the Sinomo reader. Every question receives a
fresh disposable data directory, corpus, workspace, home, and environment; no
cluster, DFS, permission, activity, session, or operational memory state is
copied. Retrieval-only results and optional reader hypotheses remain separate,
and the latter use the upstream evaluator rather than becoming a Rust gate.

## Implementation Choices Still Requiring Spikes

These choices do not alter the architectural contracts and should be decided
with the scale-hardening benchmark presets before a backend becomes the
default:

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
