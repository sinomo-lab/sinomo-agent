# Sinomo Memory Architecture

## Purpose

This document defines Sinomo's long-horizon memory architecture. The core
model is **indexed files**: durable files and file-like objects are memory;
catalogs, chunks, embeddings, summaries, entities, relations, and serving
indexes are projections over versioned file content.

The design covers very large namespaces, automatic incremental maintenance,
extensible multimodal extraction, temporal and relational knowledge,
model-authored index overlays, distributed placement, retrieval into bounded
agent context, and collaborative maintenance by conventional software and a
specialized memory agent.

This replaces the record-first target described by earlier versions of this
document. The current Rust implementation includes the first local
file-to-evidence slice; the old record subsystem remains isolated and is not a
serving or write-compatibility layer. Delivery order and readiness gates live
in the
[indexed-file memory implementation plan](plans/indexed-file-memory-implementation.md).
The research basis lives in the
[indexed-file memory literature note](research_notes/indexed_file_memory_literature.md).

## Design Position

Memory is a policy-governed view over versioned files, not a second filesystem
and not invisible mutable model state.

An ordinary source file, repository file, image, scene, transcript, event-log
segment, or agent-authored memory document can all participate in memory. The
memory service does not need to copy their bytes into a separate record store.
It records stable source identity, indexes exact versions, and returns evidence
anchored back to those versions.

The architecture separates five kinds of state:

| State | Source of truth | Mutability |
|---|---|---|
| File content and file metadata | The owning local filesystem, VFS, or DFS metadata/content authority | Source-native versions or verified current-only leases; namespace changes through its owner |
| Agent-managed memory documents | Versioned files under the policy-appropriate local or SIFS scope | Normal file versioning |
| Model-authored annotations and relations | Append-only overlay operation files | Corrections and supersession are new operations |
| Automatic and serving indexes | Rebuildable partition generations | Replaced or compacted atomically |
| Retrieval and context-use trace | Append-oriented audit files | Append only, subject to retention policy |

An index hit is never authority to read a file, perform an action, infer an
access grant, or claim that a relation is true. Source policy is rechecked when
candidates are generated and when evidence is materialized.

## Goals

- Scale to namespaces whose source and index state cannot fit in memory.
- Make file identity, file version, and exact evidence anchors first-class.
- Maintain useful indexes automatically without blocking foreground writes.
- Support text, structured data, code, images, audio, video, 3D scenes, and
  future formats through extensions rather than one universal parser.
- Support lexical, structural, temporal, relational, spatial, and optional
  vector retrieval in one bounded query pipeline.
- Use active workspace, task, dependency, participant, and locality context as
  relevance priors without turning those priors into access grants.
- Let models enrich, dispute, and organize memory through an auditable overlay
  without mutating automatically derived facts.
- Preserve provenance, freshness, confidence, and policy through every
  derivation and retrieval result.
- Work on one node without required server infrastructure and distribute
  indexing and retrieval through normal Sinomo service, network, DFS, and
  policy paths.
- Expose working, episodic, semantic, long-term, procedural, and archival
  memory as bounded views over the same file substrate.

## Core Vocabulary

| Term | Meaning |
|---|---|
| `IndexRoot` | An explicitly configured local, VFS, SIFS, or file-like namespace eligible for discovery and indexing. |
| `MemoryFile` | A stable source object inside an index root. A path is a locator, not its identity. |
| `MemoryFileVersion` | An immutable snapshot of a memory file, identified by source-native version and content identity. |
| `MemoryAnchor` | A typed, bounded selector into one exact file version. |
| `IndexFact` | A provenance-carrying derived statement about a file, anchor, entity, time range, or relation. |
| `IndexGeneration` | An atomically published set of automatic facts produced for a source version and pipeline fingerprint. |
| `MemoryRelation` | An evidence-backed, typed edge between files, anchors, entities, runtime objects, or other addressable nodes. |
| `MemoryOverlay` | A scoped append-only set of model, user, or agent assertions composed over automatic generations. |
| `IndexPartition` | The bounded physical and query-serving unit for a root, policy domain, and shard. |
| `Workspace` | A detected project or corpus that contributes membership, dependency, ignore, and retrieval-prior information. |
| `MemoryEvidenceBundle` | A bounded retrieval result containing source handles, anchors, relation paths, projections, scores, freshness, and provenance. |

## Source Identity

### File and Version References

A memory reference must separate stable object identity from a mutable path:

```text
MemoryFileRef {
  connector_id,
  root_id,
  object_id,
  path_hint,
}

MemoryFileVersionRef {
  file_ref,
  version_id,
  content_hash?,
  byte_length,
}
```

`connector_id + root_id + object_id` is the logical identity. `path_hint` is a
human-facing locator and may change after a rename. A result that cites content
always carries a `MemoryFileVersionRef`, never only a path.

The connector-issued `version_id` is required. `content_hash` may be absent
while a local file is catalog-only so an initial namespace crawl does not need
to read every byte. It becomes mandatory for content-derived facts and evidence
materialization. A verified extraction snapshot computes it while streaming and
checks that source identity/version metadata did not change. SIFS normally
provides the authoritative hash before extraction.

For SIFS, `root_id`, `object_id`, and `version_id` map to the DFS filesystem,
object, and visible immutable version. The DFS content hash and object metadata
are reused rather than recomputed when authoritative metadata already supplies
them. Moves preserve object identity.

A local-filesystem connector persists its own root namespace and binds catalog
objects to a platform file identity when one is reliable. It verifies identity
and metadata before and after extraction and computes content identity while
streaming. If a platform cannot prove that a rename preserved identity, the
connector must tombstone the old object and create a new one instead of
inventing continuity.

Every connector declares a version-retention capability:

- `native-versioned`: the source can reopen an exact historical version;
- `snapshot-capable`: Sinomo may selectively pin authorized bytes into a
  content-addressed or managed versioned file;
- `current-only`: a verified handle is stable only for the current extraction
  lease and old bytes may disappear after mutation or deletion.

A normal local file handle is not described as durable across restart. For a
current-only source, extraction results can remain historical provenance after
change, but retrieval cannot materialize the old content unless it was pinned.
Scopes that require durable episodic or historical evidence must use a
native-versioned source or explicitly retain a snapshot. Retention is selective
and policy-governed; cataloging a huge root does not copy all of its bytes.

Hardlinks retain separate namespace aliases but share extraction of the exact
target content identity; alias path, scope, and policy projections remain
separate. Symlinks are indexed as links and are followed only when the root and
source policy explicitly allow the target, never merely because it is
reachable.

File-like connectors may expose durable objects that are not ordinary mounted
paths, including event-log segments, session journals, mailbox exports, or
content-addressed artifacts. They must still implement stable identity,
version, policy, bounded snapshot-lease/range-read behavior, change cursors,
and an explicit retention capability.

### Source Descriptor

The source catalog stores a compact descriptor rather than file bytes:

- file and current version references;
- current path and alternate locators;
- object kind, detected media types, length, and source timestamps;
- declared memory scopes and workspace memberships;
- owner and policy references;
- source connector change cursor;
- content-index coverage and freshness;
- a freshness-stamped DFS placement projection when applicable.

Workspace membership is a relevance relationship, not authorization. The
underlying file policy and requested memory scopes are both enforced.

## Anchors

Every snippet, feature, entity, summary, and relation must resolve to one or
more typed anchors. An anchor has this logical shape:

```text
MemoryAnchor {
  anchor_id,
  file_version_ref,
  kind_id,
  selector,
  schema_ref,
  logical_key?,
}
```

`anchor_id` is derived from the exact file version, kind, schema, and canonical
selector. `logical_key` is an optional extractor-owned identity such as a code
symbol or transcript utterance that may survive revisions. It does not weaken
the exact-version evidence reference.

Built-in anchor families are deliberately small:

| Anchor kind | Selector examples |
|---|---|
| Whole file | Entire immutable version |
| Bytes or text | Half-open byte range, line/column range, heading path |
| Structured data | JSON Pointer, table row/column/cell range |
| Code | Symbol identity plus definition, reference, or call-site range |
| Document | Page plus bounding box, figure, table, or caption |
| Image | Pixel-normalized bounding box, OCR span, region mask reference |
| Audio/video | Half-open time interval, track, transcript utterance, frame range |
| 3D scene | Scene path or object id, component, spatial bounds, animation interval |
| Event/session | Event id, turn id, message id, or log-record range |

Extensions may add namespaced anchor kinds with a versioned selector schema.
All selectors must be bounded and validatable without trusting extractor text.

Anchor lineage is a separate derived relation between anchors in different
versions. A diff, language server, media tracker, or agent may propose that a
new anchor continues an older logical object. Retrieval preserves the original
exact evidence even when it follows that lineage.

## Index Roots, Workspaces, and Scale

### Explicit Roots

Sinomo does not recursively adopt every ambient host path. An `IndexRoot`
names a connector, namespace selector, scope assignments, inclusion and ignore
rules, resource budgets, indexing priority, and policy reference. Roots may be
created for a user library, a repository, an SIFS subtree, session archives, or
service-owned managed memory.

Local-only root definitions live in the owning node's configuration. Compact
shared desired-root definitions may live in cluster configuration, but their
per-file catalog, coverage, cursors, and index contents do not enter cluster
KV.

Symlinks, mounts, and references do not silently expand a root. Crossing a root
or policy boundary requires an explicit eligible root.

### Tiered Coverage

Large roots are indexed progressively:

1. **Cataloged**: identity, version, path, type, size, time, scope, policy, and
   workspace facts are known.
2. **Searchable**: cheap deterministic metadata, text, and structural indexes
   are current.
3. **Enriched**: embeddings, media analysis, entity resolution, summaries, and
   model-derived relations are available.

Active workspaces, recently changed files, referenced dependencies, explicit
pins, and query misses receive priority. Cold files may remain catalog-only
until background budget or a query promotes them. Coverage and omissions are
reported; absence from an expensive index is not reported as absence from the
filesystem.

### Workspace Detection

Workspace detectors consume bounded directory metadata and small marker-file
probes. They emit:

- workspace id, kind, root, and nested-workspace boundaries;
- marker evidence and detector version;
- file inclusion and ignore rules;
- package, repository, asset-library, or scene membership;
- dependency and reverse-dependency hints;
- preferred extractors and language or format configuration;
- confidence and invalidation markers.

Detectors are repeatable functions of a bounded snapshot. A module can add a
detector for a custom repository layout, game project, media library, CAD
project, scientific corpus, or other workspace type.

### Partitions

The serving unit is an `IndexPartition`, normally derived from root, policy
domain, and a stable shard key. A large root can have many partitions. Scope,
workspace, time, and explicit source selectors often choose a small set
directly, but an unconstrained corpus query requires a scalable routing index;
it cannot assume a fixed bounded subset without admitting lost recall.

Routing is itself derived, versioned, disk-backed index state. Lexical routing
maps terms to partition ids, metadata/time routing uses ordered ranges, graph
routing maps stable endpoint ids, and vector routing may use coarse centroids
or another explicitly approximate scheme. Routing state stays outside cluster
KV and can be sharded and paged. If no complete router exists, exhaustive
search walks eligible partitions in bounded pages and returns a continuation
rather than claiming complete results.

Partition rules are load-bearing:

- startup opens manifests and compact catalogs; it does not replay every fact;
- enumeration, reconciliation, compaction, and query are cursor- or
  page-based;
- no foreground operation materializes an entire root or partition;
- extraction input and output are streamed in bounded batches;
- immutable segments are published through an atomic manifest pointer;
- tombstones and replacement generations are compacted in the background;
- term dictionaries, postings, metadata columns, and graph adjacency are
  stored in query-oriented forms rather than reconstructed for every query;
- vector indexes are optional partition capabilities, not the catalog.
- exact, approximate, and budget-truncated query coverage are distinguished;
  bounded work is achieved through paging and continuations, not silent
  partition omission.

The concrete transition from the first per-file implementation slice to an
ordered catalog, chunked extraction, multi-file immutable segments, coarse
routing, and postings-first retrieval is sequenced in
[Scale-Efficient Storage and Serving Shape](plans/indexed-file-memory-implementation.md#scale-efficient-storage-and-serving-shape).

## Automatic Index Model

### Generations and Pipeline Identity

Automatic facts are published as immutable generations. A generation key
includes:

- source file version;
- extractor or pipeline id and version, plus module artifact id/content hash
  when supplied by a module;
- configuration hash;
- model, prompt, decoding, and embedding-space identity when model-derived;
- input dependency hashes;
- output schema versions.

Together these fields form a `pipeline_fingerprint`. Changing any field creates
a new generation and a resumable backfill; it does not mutate old facts in
place. A query view selects one complete successful generation for each active
pipeline. It never exposes half of a replacement generation.

Content-only extraction may be cached by content hash plus pipeline
fingerprint. File aliases, paths, workspaces, scopes, policy, and placement are
always projected separately. Identical bytes never cause one file's access
decision or contextual metadata to be reused for another.

### Common Fact Envelope

Every derived output shares a common envelope even when its serving index uses
a specialized physical representation:

```text
IndexFact {
  fact_id,
  kind_id,
  schema_ref,
  source_file_versions,
  evidence_anchors,
  payload,
  producer,
  extraction_run_id,
  pipeline_fingerprint,
  confidence?,
  quality?,
  valid_time?,
  observed_at,
  policy_derivation,
}
```

Facts without evidence anchors may describe only catalog or runtime objects
whose authoritative references are carried explicitly. A model-generated fact
must identify the model and prompt/configuration fingerprint. Reproducibility
does not require a hosted model to return identical tokens forever; it requires
Sinomo to know exactly which derivation produced the stored generation and to
replace it safely.

### Index Families

| Family | Examples |
|---|---|
| Catalog and metadata | Path terms, media type, size, timestamps, owner, scope, workspace membership |
| Lexical | Normalized tokens, n-grams, symbols, captions, OCR, transcripts |
| Structural | Headings, tables, AST nodes, code definitions/references/calls, scene hierarchy |
| Entity and relation | People, projects, functions, songs, assets, concepts, evidence-backed edges |
| Temporal | File time, event time, media intervals, fact validity, observed time |
| Spatial | Image regions, document layout, 3D bounds, containment and placement |
| Embedding | Optional named vector spaces for text, image, audio, video, code, or cross-modal retrieval |
| Summary and preview | Bounded summaries, thumbnails, waveforms, key frames, scene previews |
| Placement | Cached DFS locations, residency, availability, and policy references with metadata freshness |

Derived media artifacts remain derivations unless deliberately promoted to a
managed source file. A generated transcript or thumbnail must retain its source
version and pipeline fingerprint.

## Relation and Semantic Model

### Nodes

Relations use an open endpoint vocabulary:

- files and exact anchors;
- extractor-defined entities and concepts;
- workspaces, packages, repositories, tasks, sessions, chats, channels, agents,
  tools, events, and devices by durable runtime reference;
- relation assertions when a higher-order claim is needed.

Derived entity identity is namespaced by its resolver. Two entities are not
silently merged because their display labels match. Entity-resolution facts
connect them with confidence and evidence.

### Relations

A relation has this logical shape:

```text
MemoryRelation {
  relation_id,
  relation_type_id,
  relation_schema_ref?,
  subject_ref,
  object_ref,
  qualifiers,
  evidence_anchor_refs,
  producer,
  confidence?,
  valid_time?,
  observed_at,
  assertion_state,
  supersedes_relation_ids,
  disputes_relation_ids,
  policy_derivation,
}
```

Relation type ids are open and namespaced, for example `code.calls`,
`workspace.depends-on`, `media.depicts`, `music.cover-of`, or
`memory.contradicts`. Built-in and module-declared relation schemas may attach
validation and query semantics. An unregistered model-proposed type is durable
and queryable as opaque data, but it acquires no runtime, policy, or action
semantics merely by being named.

Relation type descriptors are versioned files. Built-ins are projected by the
memory service; modules may supply descriptors through their declared
extension; users may install policy-approved descriptors in the managed memory
namespace. A model can propose a new descriptor only into an overlay and it
remains `proposed` until validated.

Contradiction, correction, and supersession are explicit relations or overlay
operations. A new file version makes old current-version facts stale; it does
not by itself assert that the older content was false. Conflicting assertions
may coexist with separate evidence and confidence.

### Time

The system distinguishes:

- source or event time;
- media time within an audio, video, or animated scene;
- fact valid time, when the assertion is claimed to hold;
- observed time, when Sinomo learned or extracted it;
- index time, when a serving generation became visible.

Intervals are half-open where the domain permits it. Temporal queries state
which clock they constrain. This distinction is required for queries such as
“what did we discuss yesterday?” and for correcting knowledge that changed
after it was first observed.

### Placement

SIFS placement remains DFS metadata, not memory authority. The memory catalog
may cache a `PlacementProjection` containing current locations, owner, policy
reference, and the DFS metadata sequence or epoch from which it was read.
Placement-only changes update this projection without re-extracting content.

Retrieval uses placement for locality, availability, cost, and raw-data egress
planning. It revalidates the source version and policy at the node that serves
the evidence.

## Model and Agent Overlays

Automatic generations are read-only to models. A model changes the logical
index through a `MemoryOverlay`, whose authoritative operations are themselves
stored as immutable one-operation objects or per-writer operation streams with
deterministic operation ids. No correctness path assumes that multiple writers
can append safely to one shared DFS file.

Overlay operations include:

- assert an entity, facet, summary, or relation;
- annotate or label a file or anchor;
- dispute or supersede an earlier assertion;
- connect an exact anchor to a logical entity or newer anchor;
- pin evidence or add a bounded relevance hint within a scope;
- request re-extraction or revalidation.

An overlay cannot delete or rewrite an automatic fact. A proposed dispute or
supersession cannot suppress an automatic fact; it adds a conflicting
interpretation and ranking evidence. An authorized approved overlay may change
the default interpretation in its scope, but the automatic fact and evidence
remain inspectable. Overlay supersession normally replaces another overlay
assertion, not extractor output. Rank hints are bounded features, not arbitrary
final scores.

Each operation carries author identity, scope, evidence, model/tool provenance,
time, confidence, and one of `proposed`, `corroborated`, `approved`, `disputed`,
or `superseded`. Model writes default to `proposed`. Repeated extraction may
corroborate an assertion, but only an authorized policy or user workflow can
approve an assertion for a use that requires approval.

The composite view has no implicit global “last writer wins.” An operation
applies only when its scope is selected and authorized. Explicit dispute and
supersession edges determine active alternatives within that scope; otherwise
conflicting assertions coexist. Validation state and scope specificity are
ranking/explanation features, not permission for one author to erase another.

An operation targeting content cites an exact version and may also cite a
logical anchor. When the source changes, the exact assertion remains valid as
history, while its current-view projection becomes `needs-revalidation` until
an anchor-lineage provider or authorized agent reattaches it. Automatic
maintenance never silently moves model-authored evidence.

Overlay visibility can be no broader than both its declared memory scope and
the most restrictive evidence source. An overlay cannot grant access, alter
DFS placement authority, change policy, or turn a proposed relation into an
executable capability.

Append-only is a logical overlay invariant. Writers use immutable operation
objects or independently fenced per-writer streams; a materializer merges them
idempotently by operation id into a deterministic scope view. Storage uses
normal DFS namespace/version commits and does not depend on the reserved DFS
append-only object policy being implemented.

## Automatic Maintenance

### Change Sources

Maintenance consumes durable or checkpointed change streams where available:

- DFS visible-version, namespace, tombstone, policy, and placement changes;
- local filesystem watcher hints;
- connector-native change cursors;
- workspace detector and root-configuration changes;
- extractor, model, prompt, and relation-schema activation changes;
- explicit reindex, pin, repair, or query-time indexing requests.

For SIFS, memory consumes a read-only logical `DfsCatalog`/`DfsChangeFeed`
facade rather than treating a physical shard-replication subscription as a
file event stream. The facade provides paged current objects, opaque
generation-aware checkpoints, bounded logical changes, and exact revision
range reads. It resolves cross-shard moves and stale shard copies before it
emits an upsert or delete. A sequence gap, snapshot replacement, or unknown
checkpoint requires a bounded full reconciliation; the memory service does not
guess across the gap.

Initial enumeration has an atomic scan/feed handoff. `begin_scan` returns a
catalog generation, a stable paged-scan token, and per-shard change watermarks.
The feed retains or can replay logical changes after those watermarks until the
scan commits. The memory service publishes the completed scan and installs the
post-watermark feed checkpoints together. If a connector cannot provide a
stable scan, it must start/buffer its watcher or feed before enumeration and
repeat reconciliation until it proves convergence; it cannot mark the root
current after one racy walk.

Local filesystem watchers are hints because operating systems may coalesce or
drop events, and host-exposed local-owned DFS files may bypass metadata
journals. Every root therefore also has a bounded, cursor-based reconciliation
walk. Reconciliation compares catalog identity/version metadata; it does not
repeatedly parse every unchanged file. A deletion sweep becomes visible only
after a complete successful catalog pass so an interrupted scan cannot erase
live sources.

### Job Pipeline

Indexing follows this logical sequence:

1. Checkpoint and normalize a source change.
2. Coalesce bursts by file identity while preserving the newest required
   version and any deletion or policy fence.
3. Resolve the current source descriptor and affected workspace memberships.
4. Plan applicable format detectors, extractors, and pipeline fingerprints.
5. Create idempotent jobs keyed by source version and pipeline fingerprint.
6. Open an immutable SIFS version or a verified local snapshot lease; pin a
   retained version only when the root's history policy requires it.
7. Run cheap deterministic detectors and extractors first, reusing unchanged
   content chunks when the extractor supports it.
8. Schedule optional model, embedding, and media enrichment under separate
   resource and privacy budgets.
9. Validate anchors, schemas, output bounds, provenance, and policy derivation
   into a staging generation.
10. Atomically publish the generation manifest, update catalog freshness, and
    emit index events.

Advancing a change-feed checkpoint is coupled durably with applying the catalog
delta or enqueueing its idempotent jobs. After a crash, Sinomo either sees the
committed work or replays the logical change; it cannot acknowledge a source
change and silently lose its indexing work.

Jobs, attempts, leases, cursors, and final states are durable and resumable.
Duplicate execution is safe because publication is idempotent. A crashed or
superseded job cannot publish for a different source version.

Foreground file writes do not wait for extraction. The scheduler prioritizes
active-workspace changes and cheap catalog/text updates, applies debounce and
backpressure, and runs expensive enrichment in background or deliberative
lanes. Per-root and per-extension quotas bound CPU, memory, accelerator use,
temporary bytes, output facts, and concurrent reads.

Large files are range-read and chunked according to format boundaries. Media
pipelines may publish progressively useful complete generations, such as
metadata first and scene segments later, as distinct pipeline stages. A single
generation is never partially visible.

### Change Semantics

| Change | Required behavior |
|---|---|
| Content version | Publish new source-version generations; retain historical generations only when the source or an authorized pin retains their bytes |
| Rename with stable identity | Update locator/path facets and workspace membership; reuse content generations when inputs are unchanged |
| Delete or tombstone | Fence the current view immediately, cancel obsolete jobs, and retain only policy-allowed historical evidence |
| Policy or scope restriction | Fence candidates immediately; asynchronous index cleanup is not the security boundary |
| Placement only | Refresh placement projection without content extraction |
| Extractor/config/model change | Backfill a new pipeline fingerprint while the old complete generation remains available and marked pipeline-stale |
| Workspace boundary change | Re-evaluate membership, ignore rules, dependency priors, and applicable extractors |
| Overlay write | Append and materialize the overlay view; do not schedule source re-extraction unless explicitly requested |

### Freshness

Every candidate and evidence bundle reports one of these source/index states:

- `current`: source version and active pipeline match;
- `source-newer`: a newer source version exists;
- `pipeline-stale`: source matches but the active pipeline changed;
- `catalog-only`: no requested content index exists;
- `pending` or `failed`: an applicable job is outstanding or failed;
- `deleted`: the source is not in the current namespace view;
- `source-unretained`: an exact older version is known but its source bytes
  were not retained;
- `unavailable` or `policy-blocked`: bytes cannot currently be served.

A query selects an explicit freshness policy. The normal policy may use stale
facts as labeled hints while materializing only authorized current bytes. A
strict-current query may wait to a deadline, request bounded on-demand
indexing, fall back to direct source inspection, or fail explicitly. It never
pretends stale evidence is current.

Historical queries may select deliberately retained older file versions.
Unretained versions may leave non-sensitive audit tombstones and provenance
references, but not serving facts, source quotations, regions, or media
handles. Current-view queries exclude tombstoned versions even if an old
partition segment still contains their postings.

## Extension Architecture

### Extension Roles

One extension may implement one or more roles:

| Role | Responsibility |
|---|---|
| Source connector | Enumerate versioned file-like objects, changes, metadata, policy, and bounded reads |
| Workspace detector | Recognize a workspace and emit boundaries, membership, dependencies, and ignore rules |
| Format detector | Identify media/schema using bounded content probes rather than extension alone |
| Extractor | Emit anchors, text projections, facets, entities, relations, temporal/spatial features, or derived artifacts |
| Embedder | Produce vectors in a named, versioned modality or cross-modal space |
| Anchor-lineage provider | Relate logical content across source versions |
| Query adapter | Recognize a domain query and produce bounded structural, temporal, spatial, or graph operators |
| Reranker | Reorder a bounded authorized candidate set with an explainable model/version identity |
| Preview renderer | Produce bounded model- or user-facing projections such as thumbnails, clips, waveforms, or scene snapshots |

An extension descriptor declares its id and version, roles, accepted media
types, probe limits, output schemas, relation and anchor schemas, incremental
capabilities, determinism, required models and hardware, capability grants,
resource ceilings, invocation route, and compatibility range.

Each logical operation has a validated binding, not only a string name:

```text
MemoryExtensionOperationBinding {
  logical_operation,
  target,                 # native invoke, object-RPC route/op, or sandbox entry
  request_schema_id,
  response_schema_id,
  max_request_bytes,
  max_response_bytes,
  max_facts_per_batch,
  supports_cancellation,
  continuation_schema_id?,
}
```

The active extension identity includes module id, interface export/version,
artifact id and content hash, descriptor hash, and invocation lane. Those
values enter the pipeline fingerprint. Registry validation rejects an operation
whose target is not exported/routable or whose schemas and hard bounds are not
compatible with the host.

### Module Integration

Trusted built-ins can register Rust implementations directly with
`sinomo-memory`. Dynamic, sandboxed, Wasm, declarative, and future backends use
the normal module lifecycle and a versioned SOBJ request/response protocol.

The R1 module system carries generic `ModuleInterfaceExport` metadata with
typed operation targets and an opaque SOBJ descriptor; it does not learn memory
semantics and does not depend on the R2 memory crate. Memory roles use
versioned `sinomo.memory.<role>/v1` interface ids through `sinomo-runtime`. This
preserves the trust-ring direction while allowing modules to add formats and
workspace detectors.

The versioned wire vocabulary may live in a data/ABI-only R1 package such as
`sinomo-memory-extension-api`, re-exported by `sinomo-runtime`. That package
contains descriptors and SOBJ request/result values only; all scheduling,
validation, policy, storage, and retrieval behavior remains in R2
`sinomo-memory`.

An out-of-process extractor receives an authorized VFS/SIFS snapshot handle or
bounded range stream, never an ambient host path. The host enforces read scope,
network class, device/model access, secrets, deadlines, cancellation, output
bounds, continuation budgets, and temporary storage. Results are untrusted
until the memory service validates the envelope, source lease/version, anchors,
schemas, batch counts, and policy derivation.

Module activation creates a new pipeline fingerprint and a controlled backfill.
Deactivation stops new jobs and leaves already published generations readable
only while their schemas remain supported. Rollback reselects the prior
descriptor and generation set; it does not rewrite source files.

### Model-Based Extractors

Model extractors are optional enrichment stages. Their output is automatic
derived data, distinct from interactive agent overlays. They must record model,
provider, prompt, tool schema, decoding settings, input anchors, and output
validation. They may propose new relation labels, but those labels remain
opaque and untrusted unless a declared schema or authorized promotion gives
them additional semantics.

## Physical Storage

The base implementation is embedded and log/segment structured. It does not
require PostgreSQL, a message broker, a distributed cache, a graph server, a
vector database, or external object storage.

The local data directory has this logical ownership shape:

```text
memory/
  catalog/                 # root, source, workspace, and partition catalogs
  routing/                 # versioned term/range/endpoint routing indexes
  jobs/                    # durable change cursors and indexing job state
  partitions/<id>/
    manifest.sobj          # atomic serving view
    generations/<id>/      # immutable fact/index segments
    staging/<job_id>/      # unpublished bounded job output
  traces/                  # append-oriented retrieval and maintenance audit
  records/...              # old subsystem data; ignored by indexed serving
  index.sobj               # old subsystem index; ignored by indexed serving
```

Exact paths and schemas are registered by `sinomo-memory` through the storage
registry. Startup reads bounded manifests and catalog pages. Immutable
segments, compact term dictionaries/postings, metadata columns, relation
adjacency, tombstones, and periodic compaction provide the baseline serving
shape. SOBJ is the portable metadata and segment envelope; an implementation
may use a specialized embedded lexical, graph, or vector provider behind the
same contracts.

Generated partitions, staging files, traces, previews, and snapshot exports
are excluded from source roots by identity and storage ownership. A memory
index must not recursively index its own derived artifacts unless an operator
deliberately promotes one into a separate managed source root.

Provider choice does not change authority. A SQLite, Tantivy-like, HNSW, GPU,
or remote provider is replaceable derived state. It must support versioned
publication, policy-aware lookup, bounded cursors, rebuild, and provenance.

Agent-managed memory documents, overlay operation files, and shared relation
type descriptors live in policy-selected local or SIFS paths under
`/services/sinomo/memory/scopes/<scope>/<id>/documents`,
`.../overlays`, and a policy-owned relation-type namespace. Managed documents
are normal source roots. Overlay operations and relation descriptors are
consumed by their dedicated stores and are not recursively treated as generic
content. They use the scope's DFS policy rather than inheriting one global
replication mode. Derived partition snapshots may be exported as
content-addressed artifacts for warm start or remote serving, but the source
files and overlay operation files remain authoritative.

## Distributed Indexing and Retrieval

Each node may index readable source versions for which it has the required
extension and resources. Indexing normally runs near an existing content copy.
Duplicate derived work is safe; deterministic generation identity and source
version allow results to be deduplicated. Work leases optimize placement but
are not source authority.

Cluster KV stores only compact memory-service advertisements, extension
capabilities, root/partition summaries, and desired placement hints. It never
stores per-file catalog entries, postings, vectors, relations, overlays, or
jobs. Those remain in node-local memory stores, DFS-owned source namespaces, or
scope-owned SIFS files.

A federated query proceeds through normal service discovery and typed object
RPC:

1. Pin eligible routing/catalog generations and resolve partitions using scope,
   workspace, modality, freshness, capability, locality, policy, and the
   requested completeness mode.
2. Query a bounded local-first page of memory services, continuing or reporting
   truncation when more eligible partitions remain.
3. Reauthorize at each serving node before it returns candidate metadata.
4. Merge stable candidate ids and score features at the coordinator.
5. Materialize bounded evidence from eligible source locations.

Raw bytes remain near their owning data when policy requires it. A remote node
may return a derived caption, embedding match, relation path, or opaque evidence
handle instead of transferring raw media. The node that serves content makes
the final policy decision.

Index loss does not lose memory. A node can rebuild from authorized source and
overlay files, import a compatible snapshot, or forward queries to another
eligible service. A disconnected node may search a local source governed by
local authority. Shared or remotely governed data requires a still-valid
offline authorization lease carrying a policy epoch and expiry; after expiry,
or when the cached policy never allowed offline use, search fails closed. An
offline node cannot promise instantaneous knowledge of a revocation, so policy
defines the maximum lease lifetime and reconnect invalidates older epochs.

## Retrieval Architecture

### Query Contract

A `MemoryQuery` is structured even when it starts as natural language. It may
carry:

- text and optional image, audio, video, object, or file query references;
- agent, user, chat, channel, task, team, and workspace scope selectors;
- active workspace, open file/anchor, repository, dependency, participant,
  session, and device focus;
- desired modalities and result projections;
- temporal, spatial, entity, and relation-pattern constraints;
- freshness, privacy, locality, and remote-fanout policy;
- latency, candidate, evidence-byte, graph-hop, and context-token budgets;
- whether direct source inspection or on-demand indexing is allowed.

The query planner uses registered partition and extension capabilities. It does
not invoke every provider for every query.

### Query Snapshot and Completeness

Planning creates a query snapshot that pins the authorized catalog/router
generation, partition manifests, overlay watermarks, and policy epochs used by
the query. Local compaction retains referenced segments until the bounded query
lease ends. Federated services return equivalent manifest/lease tokens. A
source change observed during evidence materialization is reported as stale or
causes a bounded replan; providers from different generations are not silently
mixed into one result.

Every result reports coverage as `complete`, `approximate`, or
`budget-truncated`, with searched/omitted partition counts and a continuation
when more eligible partitions remain. Lexical or metadata search may be exact
when its routing generation is complete. Coarse vector routing is approximate
unless an exhaustive paged search completes. A timeout or fanout budget cannot
be represented as “no matching memory.”

### Retrieval Stages

1. **Authorize and select partitions.** Scope and policy gates run before
   candidate generation so counts and scores do not leak hidden corpora.
2. **Plan retrieval.** Choose lexical, metadata, vector, structural, temporal,
   spatial, graph, or extension-specific operators under the latency budget.
3. **Generate candidates.** Providers return stable source/anchor ids, feature
   scores, freshness, and reason codes.
4. **Fuse.** Use calibrated features or rank fusion; do not compare unrelated
   raw vector and lexical scores as though they shared a scale.
5. **Apply contextual priors.** Boost the active workspace, referenced files,
   dependency neighborhood, current task, participants, recency, and local
   availability. A prior never bypasses authorization or hard query filters.
6. **Expand relations.** Traverse only authorized, evidence-backed paths with
   bounded hops, branching, relation types, and cost.
7. **Rerank and diversify.** Optionally rerank a bounded set, remove redundant
   anchors, and preserve source and modality diversity.
8. **Materialize evidence.** Verify current source/version/policy and fetch only
   the snippets, regions, clips, projections, or handles required by the
   result.
9. **Pack and trace.** Produce deterministic evidence bundles within byte and
   token budgets and record included, omitted, stale, failed, and denied paths.

Candidate and audit traces are bounded too. They retain selected candidates,
the highest-ranked or decision-relevant omissions, aggregate counts/reason
histograms, provider cursors, and a reproducible plan id; they do not serialize
one trace row for every file or posting considered in a huge corpus.

### Evidence Bundles

The retrieval result is not an unstructured bag of chunks:

```text
MemoryEvidenceBundle {
  bundle_id,
  query_plan_id,
  query_snapshot_id,
  files,
  anchors,
  projections,
  relation_paths,
  summaries,
  score_features,
  freshness,
  coverage,
  retrieval_modes_used,
  provenance,
  omitted_due_to_budget,
  policy_omissions,
  continuation?,
}
```

`projections` may contain bounded text, OCR, captions, thumbnails, clips,
waveforms, key frames, or scene views. Large or sensitive content is represented
by an authorized handle. Relation paths include the evidence that justified
each edge and explain why a seemingly non-matching file was included.

The requested examples map naturally onto this contract:

| Query | Required extension/index behavior |
|---|---|
| “Reference image for a traditional Japanese building” | Cross-modal text-image candidates, visual labels, image-region anchors, and bounded previews |
| “Gun fire scenes” | Video/audio scene segmentation, time anchors, captions or AV embeddings, and clip handles into long videos |
| “Who called this function?” | Workspace-aware code symbol resolution and reverse traversal of evidence-backed `code.calls` edges |
| “What did we discuss yesterday?” | Participant/scope filtering plus event-time search over message or transcript anchors and temporal relations |
| “Album cover images for songs made by this singer” | Entity resolution and a bounded relation join from artist to song/release to cover-image anchors |

Optional embeddings improve recall but are not required for identity, scope,
lexical search, structured graph queries, or auditability.

### Automatic Context Injection

`sinomo-agent` asks the memory context provider for bounded evidence using the
current turn, active task, workspace, open files, participants, and budget. The
memory service retrieves; the agent context assembler decides what becomes a
model-visible `ContextBlock`.

Stable bundle ids and source hashes support prompt-cache reuse. Current-task
evidence normally appears before the transcript item it informs. Volatile
scores, freshness, and omission details remain in the trace or volatile tail.
The context assembly report records why each block was included and how many
bytes/tokens it consumed. Memory never injects hidden context outside this
path.

## Workload Memory Views

Short-term, long-term, episodic, semantic, and procedural memory are retrieval
and retention views, not incompatible storage silos:

| View | Typical sources and indexes | Default behavior |
|---|---|---|
| Working | Current session projection, active files, recent events, open tool artifacts, task scratch documents | Highest priority, small budget, high resolution; volatile state is checkpointed to a file only when it must survive |
| Episodic | Session/message/event files, transcripts, task outcomes, incidents, observations | Ordered by event and valid time with participant, task, and causal relations |
| Semantic long-term | User/workspace documents, facts, entities, summaries, relation graph, repeated patterns | Durable, cross-session, evidence-backed, conflict-aware |
| Procedural | Skills, workflows, code, policies, tool examples, recovery recipes | Retrieved as versioned instructions or affordances with applicability evidence |
| Archive | Cold project/media/history roots and retained old versions | Cataloged broadly; expensive content indexing and transfer are demand driven |

Hot in-process event buffers remain part of the reactive/agent runtime. They
are not durable memory until their owner persists a session, event segment,
task document, or other file-like source. Promotion means creating or retaining
an appropriate file and indexing it, not copying an opaque record between
memory tiers.

An agent that wants to remember new content has two explicit choices:

- create or update an authorized managed memory document, which enters the
  normal automatic indexing path; or
- append a scoped annotation/relation to an overlay when the source file should
  remain unchanged.

## Collaborative Maintenance

Conventional indexing handles the common path:

- change detection, hashing, type detection, parsing, chunking, OCR/ASR when
  configured, lexical indexing, deterministic code/scene relations, embedding,
  invalidation, compaction, and repair;
- reproducible model-enrichment pipelines for schemas that justify their cost.

A specialized memory curator handles work that needs high-level judgment:

- entity resolution across formats and workspaces;
- causal, thematic, task, dependency, and personal-meaning relations;
- contradiction review and anchor migration;
- session/task-boundary synthesis into managed memory documents;
- procedural lessons and relevance hints;
- analysis of repeated query misses or co-retrieval patterns.

Curator work is event- or job-driven, not continuous model polling. Useful
triggers include low-confidence retrieval, an unresolved contradiction, a
high-salience task outcome, repeated joint retrieval, a new workspace, or a
source revision that orphaned important annotations.

The curator reads evidence bundles and writes strict overlay proposals through
tools. It cannot write automatic generations, approve its own assertions,
broaden visibility, modify policy, or bypass normal file tools. Its jobs have
budgets, cancellation, provenance, and evaluation evidence.

Multiple agents do not share one mutable scratchpad. They write agent-, task-,
chat-, channel-, workspace-, team-, user-, or cluster-scoped documents and
overlays. Disagreement is represented as competing assertions and explicit
relations; one agent cannot silently overwrite another's evidence.

## Tool Surface

The agent-facing concepts should be small and progressive:

- `memory_search`: return bounded evidence bundles and trace handles;
- `memory_explain`: explain scores, paths, freshness, omissions, and policy
  decisions for a prior retrieval;
- `memory_read`: materialize an authorized evidence handle or larger bounded
  source region;
- `memory_annotate` and `memory_link`: append scoped overlay assertions;
- `memory_dispute` or `memory_supersede`: append corrections without deleting
  history;
- `memory_reindex`: request bounded re-extraction or revalidation;
- normal workspace/file tools: create or edit managed memory documents.

Tool availability does not grant the source, overlay, model, filesystem,
network, or policy capabilities required to execute it. Full tool inputs and
results remain durable evidence even when the model sees only a compact
projection.

## Governance and Security

Memory governance must answer:

- Which exact file version and anchors support this result or assertion?
- Who or which extractor produced it, with what code/model/configuration?
- Which scope and source policy permit discovery and materialization?
- Is it current, historical, stale, disputed, or awaiting revalidation?
- Which retrieval modes and relations selected it?
- Which model context and action, if any, used it?

Rules:

1. Scope and policy filter before ranking and graph traversal.
2. Materialization reauthorizes against the current source owner.
3. A derived fact, embedding, caption, entity label, relation, or score inherits
   at least the most restrictive policy of all evidence inputs. Relations can
   leak content even when their source snippet is hidden.
4. A service that observes a newer policy epoch or deletion fences serving
   synchronously; physical index cleanup may be asynchronous. Disconnected
   shared-data access is limited by the explicit offline authorization lease
   and fails closed when it expires.
5. Privacy-preserving deletion removes derived payloads and overlay evidence as
   required by source policy while retaining only a non-sensitive audit
   tombstone when allowed.
6. Query counts, score ranges, remote partition existence, and error details
   must not reveal unauthorized sources.
7. Model-derived and overlay assertions are evidence, not trusted policy facts.
8. Retrieval cannot authorize a tool, action, rollout, device, secret, or file
   write.
9. Retrieved file content is untrusted data, not an instruction-priority
   upgrade. Only separately authorized procedural memory may enter an
   instruction lane; a file, caption, relation, or overlay cannot grant tools
   or rewrite the agent's policy hierarchy.

## Events and Observability

The memory service emits typed events such as:

- `memory.source.observed` and `memory.source.fenced`;
- `memory.index.scheduled`, `memory.index.published`, and
  `memory.index.failed`;
- `memory.overlay.appended` and `memory.overlay.revalidation-needed`;
- `memory.retrieval.completed` and `memory.retrieval.denied`;
- `memory.compaction.completed` and `memory.repair.requested`.

Events carry operation, node, source/version, root/partition, workspace, scope,
pipeline, timing, policy, and evidence references as applicable. They do not
copy raw sensitive content by default.

Operators need bounded metrics for catalog coverage, change-cursor lag,
indexing queue age, bytes and files per coverage tier, extraction throughput,
reuse ratio, generation failures, compaction debt, stale-hit rate, query
latency/fanout, evidence bytes, policy denials, overlay revalidation backlog,
and provider/model cost.

## Logical APIs

The architecture requires narrow interfaces rather than one storage-shaped
API:

| Interface | Responsibility |
|---|---|
| `MemorySourceConnector` | Roots, source descriptors, change cursors, immutable snapshots, bounded reads |
| `MemoryExtensionRegistry` | Active workspace, format, extraction, embedding, query, and preview capabilities |
| `MemoryIndexScheduler` | Durable idempotent planning, prioritization, backpressure, cancellation, and recovery |
| `MemoryIndexStore` | Staging, atomic generation publication, partition queries, compaction, and rebuild |
| `MemoryOverlayStore` | Append, validate, scope, replay, and materialize authoritative overlay operations |
| `MemoryRetriever` | Plan, authorize, fuse, expand, materialize, pack, and trace evidence bundles |

Storage engines, parsers, vector providers, graph providers, and models remain
behind these boundaries. `sinomo-agent` consumes `MemoryRetriever` directly and
must not depend on their internals or on the old record-serving interface.

## Crate and Trust-Ring Ownership

| Owner | Responsibility |
|---|---|
| R0 files/events/storage contracts through `sinomo-runtime` | Base ids, event envelopes, VFS primitives, and local storage ownership vocabulary |
| `sinomo-dfs` (R1) | SIFS object/version authority, paged logical catalog/change-feed and exact revision-read facade, content reads, placement, replication, and policy enforcement |
| `sinomo-module` and daemon registry (R1) | Generic lifecycle, opaque extension descriptors, routes, capabilities, activation, and runtime binding; no memory semantics |
| Memory extension data/ABI package (R1) | Versioned descriptors, operation bindings, source leases, and normalized SOBJ request/result values; no memory behavior |
| `sinomo-policy` and sandbox crates (R1) | Authorization decisions and contained extension execution |
| `sinomo-memory` (R2) | Memory domain types, root/source catalogs, schedulers, partitions, overlays, retrieval, traces, and built-in extractors |
| `sinomo-agent` (R2) | Query focus, context assembly, memory tools, agent-written managed documents, and curator orchestration |
| Media, code, model, and extension modules | Domain extraction, embeddings, query adapters, reranking, and previews through declared contracts |
| Applications | Composition, configuration, inspection, and operator/user surfaces |

R2 code accesses DFS, modules, events, policy, VFS, and storage through
`sinomo-runtime`. No R0/R1 crate imports `sinomo-memory`. Domain extension
descriptors pass upward through generic R1 module surfaces rather than moving
memory behavior into a lower trust ring.

## Evaluation and Verification

Evaluation has four layers:

1. **Deterministic correctness**: identity, version pinning, anchor bounds,
   retention capability, scan/feed handoff, idempotency, atomic generation
   publication, query-snapshot pinning, tombstones, cursor recovery,
   scope/policy intersection, overlay merge/replay, and deterministic packing.
2. **Scale and operations**: catalog and query behavior on large synthetic
   trees, bounded memory, incremental update amplification, startup work,
   compaction, backpressure, crash recovery, and partition fanout.
3. **Retrieval quality**: lexical, temporal, code-graph, workspace-prior,
   cross-modal, relation-path, contradiction, and stale-knowledge fixtures.
4. **Downstream usefulness**: model answers, citations, abstention, task
   continuation, cache stability, and action choices with and without memory.

Benchmarks must report corpus/version, index coverage, provider versions,
latency percentiles, resource use, freshness, recall/precision or task score,
source-reference coverage, leakage failures, and omissions. Live-model scores
are evaluation evidence, not replacements for deterministic tests.

No acceptance benchmark may hide a full-root scan behind a query or startup.
Scale thresholds belong in versioned benchmark presets so they can evolve with
hardware while the bounded-work invariant remains architectural.

## Storage Transition

The indexed-file system is a clean break from the old `MemoryRecord` and
`DurableMemoryStore` subsystem. It does not project old records as virtual
files, dual-read results, or dual-write an outbox. Lower-ring record types are
not expanded into the new R2 index schema.

The indexed-file serving path ignores existing `memory/records/...` and
`memory/index.sobj` files. There is no reader, importer, migration shim, or
preservation guarantee for that data. Deployments discard it and rebuild the
new indexes from their source files.

## Failure Modes

The implementation must explicitly defend against:

- full-log replay or full-index rewrite on ordinary query/write paths;
- watcher loss, duplicate events, rename ambiguity, and indexing the wrong
  local snapshot;
- a racy initial scan missing changes before its feed checkpoint is installed;
- treating a current-only local handle as retained historical evidence;
- a partially published generation or stale job replacing a newer version;
- an incomplete router silently dropping partitions or one query mixing
  incompatible manifest generations;
- unbounded root scans, graph expansion, remote fanout, media decode, model
  enrichment, or context packing;
- stale policy caches leaking candidates or relation structure;
- disconnected shared-data search continuing after its authorization lease or
  policy epoch is no longer valid;
- embeddings, captions, relations, or summaries surviving source deletion
  beyond policy;
- high-level model assertions being mistaken for source facts or authority;
- overlay drift after file edits and silent anchor migration;
- workspace priors overwhelming stronger evidence from outside the active
  repository;
- relation/entity explosion and low-value agent-generated index pollution;
- raw media transfer when a derived local result would satisfy the query;
- provider loss making authoritative memory unreadable;
- accidental reads or writes to the old record store creating a second
  authority or duplicate context result;
- multi-agent contradiction collapse or one agent silently suppressing another.

## Architectural Invariants

1. Durable memory content resolves to a versioned file or file-like object.
2. A path is a locator; source identity and exact version are separate.
3. Every content-derived fact and relation cites bounded evidence anchors and a
   derivation fingerprint.
4. Automatic indexes are immutable, atomically published, rebuildable derived
   state.
5. Model and user index changes are append-only overlays; they never mutate
   automatic generations.
6. Scope and policy are enforced before candidate generation and rechecked
   before evidence is served.
7. Derived visibility is never broader than every source used to derive it.
8. Index freshness is explicit; stale data is never presented as current.
9. File writes do not wait for expensive indexing, embeddings, graph work, or
   model enrichment.
10. Startup, enumeration, query, graph traversal, extraction, federation, and
    compaction are bounded and cursor-based.
11. Cluster KV contains compact service/control facts, not per-file or index
    data.
12. DFS remains the authority for SIFS versions, paths, placement, and bytes.
13. Extensions cannot bypass VFS/DFS, sandbox, policy, capability, module, or
    trust-ring boundaries.
14. Retrieval returns evidence bundles with selection and omission traces; it
    does not inject invisible model state.
15. Working, episodic, semantic, procedural, and archival memory are views over
    the same substrate, not competing sources of truth.
16. Optional databases, vector stores, graph engines, and hosted models remain
    replaceable and cannot become undeclared authorities.
17. Historical content is retrievable only when its source connector retains
    that version or policy authorized a durable pin.
18. Retrieval pins a coherent query snapshot and reports incomplete or
    approximate corpus coverage explicitly.
19. The indexed-file system does not dual-read or dual-write the old record
    store; files and overlays are its only authorities.

## Non-Goals

- Do not make the memory index a second namespace or a write authority for
  ordinary files.
- Do not require every eligible file to receive expensive semantic or
  multimodal enrichment.
- Do not flatten all modalities into text chunks as the universal model.
- Do not require vectors for basic retrieval or graph relations for simple
  lookup.
- Do not define one closed ontology for code, media, people, workspaces, and 3D
  scenes.
- Do not let a model silently edit source files, automatic facts, policy, or
  another agent's assertions through a memory tool.
- Do not store high-cardinality catalogs, postings, vectors, or relations in
  cluster KV.
- Do not make memory responsible for the active session transcript or
  same-session context projection; those remain agent/context-assembly state.
