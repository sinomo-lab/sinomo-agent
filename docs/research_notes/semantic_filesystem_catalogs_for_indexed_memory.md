# Semantic Filesystem Catalogs for Indexed Memory


This is a supporting research note. The
[memory architecture](../memory-architecture.md),
[DFS architecture](../dfs-architecture.md), and
[indexed-file memory implementation plan](../plans/indexed-file-memory-implementation.md)
remain the normative design and delivery documents.

## Question

Sinomo memory treats files and immutable file versions as its durable source of
truth while catalogs, lexical indexes, embeddings, relation graphs, and routing
structures are rebuildable derived state. The initial local connector discovers
files itself and records a memory-owned catalog.

Because SIFS already owns stable objects, visible versions, namespace metadata,
placement, and versioned content, should `sinomo-dfs` expose a lower-level
catalog and change facility for memory? Could this remove expensive filesystem
walks without making DFS responsible for semantic extraction? How does this
compare with keeping the complete catalog in an embedded database owned by
`sinomo-memory`?

## Short Answer

Yes. SIFS should expose an authoritative, read-only, content-agnostic catalog,
logical change feed, and exact-revision read boundary. It should become easy to
index, but it should not become the owner of semantic indexes.

`sinomo-memory` should retain a node-local embedded database. Its role changes
from independently reconstructing SIFS truth to maintaining a disposable
materialized projection plus memory-specific workflow state. The recommended
shape is:

```text
sinomo-dfs authority
  stable object and version identity
  generation-pinned catalog snapshots
  logical namespace/version/policy/placement change feed
  exact-version bounded reads and retention leases
                         |
                         v
sinomo-memory node-local catalog and scheduler
  applied checkpoints and normalized connector state
  workspace, scope, policy, pipeline, job, and serving state
                         |
                         v
immutable derived lexical, graph, vector, and media index segments
```

This is not primarily a choice between a filesystem and a database. It is a
choice about authority. DFS owns facts that are already necessary for correct
filesystem behavior. Memory owns interpretations and query structures that can
be invalidated and rebuilt.

## Scope and Terminology

The phrase *semantic filesystem* covers several substantially different
architectures:

1. A filesystem may transactionally index typed attributes and answer exact
   metadata predicates.
2. A sidecar service may observe files, extract content metadata, and maintain a
   separate search database.
3. A virtual filesystem may present query results or semantic groupings as
   directories while another store owns the index.
4. A database may become the filesystem's primary object and relationship
   representation.

The proposal in this note is narrower than making SIFS semantic. It gives SIFS
an **indexability substrate**: stable identity, consistent enumeration, durable
logical changes, exact revision reads, and policy-aware access. Tokenization,
entities, relations, embeddings, ranking, and virtual semantic views remain
above that substrate.

The term *catalog generation* means a stable logical view of the relevant DFS
namespace. It does not require DFS to write a second complete catalog object.
An implementation may pin existing metadata tables, a snapshot manifest, or a
set of shard generations as long as the consumer-visible semantics are the
same.

## Precedent Map

| System | Architectural pattern | Useful lesson | Main warning |
| --- | --- | --- | --- |
| [Semantic File Systems](https://www.cs.cornell.edu/courses/cs615/2001fa/615/sfs.pdf) | File-type transducers extract attributes; query-named virtual directories expose associative access | Semantic extraction and a filesystem-compatible query view can remain derived from ordinary files | Query directories and extractor schemas can become coupled to namespace behavior |
| [Be File System](https://www.haiku-os.org/legacy-docs/practical-file-system-design.pdf) / [Haiku](https://www.haiku-os.org/docs/userguide/en/index.html) | Typed extended attributes and selected indexes are maintained with the filesystem; queries can update live | Integrating small, stable metadata fields eliminates synchronization ambiguity | Every maintained index adds mutation, storage, backfill, and schema cost |
| [Apple Spotlight](https://developer.apple.com/library/archive/documentation/Carbon/Conceptual/MetadataIntro/Concepts/HowDoesItWork.html) | Kernel file-change notification feeds type-specific importers and a separate metadata store | The filesystem can provide reliable change discovery while extraction and search remain isolated | Notification loss, importer failure, and rebuild behavior still need explicit handling |
| [GNOME LocalSearch](https://gnome.pages.gitlab.gnome.org/localsearch/overview.html) | A filesystem monitor synchronizes a sidecar RDF store; a restricted helper extracts file content | Semantic databases can be disposable, sandboxed projections rather than storage authority | Sidecars must reconcile when monitoring is incomplete or their database is lost |
| [Spyglass](https://www.usenix.org/conference/fast-09/spyglass-fast-scalable-metadata-search-large-scale-storage-systems) | Snapshot-based incremental collection, namespace-local partitions, signature routing, and versioned indexes | Storage snapshots and namespace locality make very large metadata indexes practical | Search authorization and index placement are separate hard problems |
| [DelveFS](https://www.salkhordeh.de/files/pdfs/delvefs-cluster2020.pdf) | Object-store events feed a local RocksDB projection that presents rule-defined semantic filesystem views | Object storage can own raw events while a derived database owns semantic views | Event lag and eventual consistency must be visible rather than reported as complete results |
| [Amazon S3 Metadata](https://docs.aws.amazon.com/AmazonS3/latest/userguide/metadata-tables-overview.html) | S3 maintains read-only journal and live-inventory tables for object versions; external engines query them | A storage service can expose current inventory and changes as a reusable low-level facility | Inventory refresh is eventual and the documented tables do not provide Sinomo's required atomic snapshot/feed handoff |
| [WinFS](https://learn.microsoft.com/en-us/archive/msdn-magazine/2004/january/winfs-lets-users-search-and-manage-files-based-on-content) | Typed items, schemas, properties, relationships, transactions, and SQL views share one storage model | Deep integration enables expressive cross-object queries and stable identities | Storage correctness becomes coupled to schema, database, and semantic evolution |

The closest pattern for Sinomo is the combination of Spotlight and Spyglass:
authoritative storage supplies exact identity, snapshots, and changes; a
separate service performs extraction and maintains query-oriented indexes.
S3 Metadata is a current object-storage example of the same authority split.

## Architectural Patterns

### Filesystem-Integrated Semantic Indexes

Putting typed attributes and their indexes in filesystem transactions provides
excellent consistency. A rename, delete, or attribute update cannot leave an
external catalog silently out of sync. Exact metadata queries can also execute
close to the metadata authority.

This is attractive for small, stable facts that DFS already owns: object id,
visible version, path, size, content identity, timestamps, tombstone state,
placement, and policy. It is much less attractive for model-derived facts.
Embedding dimensions, relation schemas, extractor versions, prompts, ranking
features, and media pipelines evolve independently of filesystem formats.
Putting them in DFS transactions would turn model upgrades into metadata
backfills and would add index work to ordinary file mutation latency.

### Sidecar Database Indexes

A sidecar indexer has a small failure blast radius and can evolve its schema,
compaction, and query engine independently. It naturally supports local files,
SIFS, imported archives, and future connectors through one normalized catalog.
The database can atomically coordinate jobs and serving publication without
changing filesystem code.

The weakness is discovery. Recursive walks are expensive for large roots,
native watchers can coalesce or lose events, path-based identity turns renames
into delete/create churn, and a watcher started after a scan can miss changes in
the handoff window. Every consumer must solve these problems again.

### Hybrid Storage Catalog and Derived Projection

The hybrid keeps the database benefits while moving only the universally useful
source facts and change-capture guarantees into DFS. For SIFS, the database is
rebuilt from an authoritative catalog snapshot and then advanced by logical
changes. For local and other sources, the same database is fed by connector
walks, watcher hints, and reconciliation.

This does duplicate a bounded amount of raw metadata on indexing nodes. The
duplication is intentional: it gives memory fast local joins, transactions, job
coalescing, and query routing without placing read-heavy semantic workloads on
DFS metadata primaries.

## Comparison for Sinomo

| Concern | Memory-owned database only | DFS owns semantic indexes | Recommended hybrid |
| --- | --- | --- | --- |
| Source authority | Reconstructed by every connector | DFS | DFS for raw SIFS facts; memory for derived state |
| Initial discovery | Full walk plus verification | Direct filesystem query | Stable, cursor-paged catalog snapshot |
| Incremental maintenance | Watcher hints plus reconciliation | Transactional with every mutation | Bounded logical feed plus reconciliation on gaps |
| Rename and movement | Often inferred as delete/create | Exact | Stable object identity and explicit namespace change |
| Revision correctness | Requires a verified snapshot protocol | Exact | Exact immutable DFS revision or verified local snapshot |
| Query evolution | Independent | Coupled to DFS schema and rollout | Independent memory schemas and immutable segments |
| Write amplification | Outside DFS | Potentially high on every file change | Only DFS-native metadata and journal work on the write path |
| Failure blast radius | Local indexer only | Filesystem authority | Feed bugs are foundational; extraction/index bugs remain isolated |
| Non-DFS sources | Natural | Poor fit | Normalized through memory-owned connector projections |
| Rebuild | Recrawl all source trees | Filesystem repair may involve semantic data | Replay retained changes or rescan metadata; rebuild semantic segments independently |
| Placement | Indexer must rediscover locality | Can be tightly coupled to storage | DFS exposes placement; memory independently places derived work |

The hybrid is more complex than a database-only first implementation because
the DFS contract must be correct across shards, topology changes, gaps, and
policy changes. That cost is justified when more than one service benefits from
authoritative enumeration or when roots are too large for repeated crawling.
It is not justification for pushing content interpretation into DFS.

## Recommended Sinomo Boundary

### DFS-Owned Facilities

`sinomo-dfs` should own generic contracts for:

- stable filesystem, object, and immutable visible-version identity;
- stable namespace-entry identity, target object/version identity, current
  locator aliases, and authoritative namespace/tombstone state;
- policy and placement facts already required by DFS;
- a generation-pinned, cursor-paged current-object catalog;
- an atomic catalog-scan/change-feed handoff;
- opaque per-shard and topology-aware feed checkpoints;
- bounded logical changes with explicit gap and reconciliation results;
- exact-version range reads with content verification;
- bounded historical-version retention leases for asynchronous consumers;
- optionally, stable changed-logical-range or content-layout information when
  DFS can prove it without exposing physical blob layout.

These contracts belong in the R1 `sinomo-dfs` crate and should be re-exported
through `sinomo-runtime`. R2 `sinomo-memory` must not import DFS implementation
internals or introduce memory types into the lower ring.

### Memory-Owned State

`sinomo-memory` should own:

- normalized catalog projections for SIFS, local, and future connectors;
- source scope and workspace membership projections;
- policy-decision epochs and immediate serving fences;
- pipeline fingerprints and desired, staged, and published generations;
- extraction, enrichment, compaction, and repair jobs;
- job attempts, leases, retries, cancellation, and failure diagnostics;
- lexical dictionaries and postings, embeddings, graph facts, relations,
  summaries, media descriptors, routers, and ranking statistics;
- query snapshots, coverage, continuations, traces, and serving heads.

Applying a logical DFS change, coalescing desired work, durably enqueueing any
required job, and advancing the feed checkpoint should occur in one embedded
database transaction. At-least-once delivery is then safe because jobs and
publications are content-addressed and idempotent.

### Responsibilities Explicitly Excluded from DFS

DFS should not own or synchronously maintain:

- tokenization, chunking policy, embeddings, summaries, or OCR;
- entities, model-defined relations, confidence, or prompt outputs;
- term postings, vector indexes, graph-serving indexes, or ranking models;
- memory job queues, extractor scheduling, or serving generations;
- automatic semantic extended attributes on authoritative file records;
- query-named virtual directories in the core namespace;
- per-file or per-term routing in cluster KV.

A future FUSE or UI view may present memory queries as virtual folders. That
view should be a client of the memory retrieval service, not a new DFS source of
truth.

## Current Sinomo Substrate

Sinomo already has most of the physical ingredients needed for the proposed
facade:

- [`DfsMetadataBackend`](../../crates/sinomo-dfs/src/metadata_api.rs) exposes
  cursor-bounded object pages and bounded per-shard journal batches.
- [`DfsObjectMetadata`](../../crates/sinomo-dfs/src/model.rs) carries stable
  metadata-record identity, path, visible version, link target, content identity
  and size, locations, timestamps, and DFS placement policy.
- [`DfsObjectVersionRecord`](../../crates/sinomo-dfs/src/metadata_records.rs)
  records immutable object-version identity and visible-version metadata.
- [`ContentManifest`](../../crates/sinomo-dfs/src/content_store.rs) supports
  extent-backed objects and exact-version bounded range reads.
- The memory implementation's current
  [`LocalSourceConnector`](../../crates/sinomo-memory/src/indexed/local.rs)
  already supplies the connector-side scan and verified-snapshot abstraction
  that an SIFS connector can generalize.

The missing piece is a public logical boundary. The current sharded backend in
[`metadata_sharded_backend.rs`](../../crates/sinomo-dfs/src/metadata_sharded_backend.rs)
merges physical journal entries whose sequence numbers are local to different
shards, while its generic subscription starts on one shard. Those operations
are useful for DFS replication and inspection, but they are not a complete,
generation-aware file change stream. Current page cursors also do not pin a
catalog generation across a multi-page scan.

The implementation should therefore adapt existing paged metadata, journal,
and content-range machinery behind a new logical facade. It should not expose
metadata table layouts, local blob paths, replication receipts, write intents,
or physical shard subscriptions to `sinomo-memory`.

## Proposed Logical Facilities

The following Rust-like interfaces illustrate behavior, not a committed ABI:

```rust
trait DfsCatalog {
    fn begin_scan(
        scope: CatalogScope,
        limits: CatalogLimits,
    ) -> Result<CatalogScanStart, DfsCatalogError>;

    fn scan_page(
        handle: &CatalogScanHandle,
        cursor: Option<&CatalogCursor>,
        limits: CatalogLimits,
    ) -> Result<CatalogPage, DfsCatalogError>;

    fn complete_scan(
        handle: CatalogScanHandle,
        installed_checkpoint: &DfsFeedCheckpoint,
    ) -> Result<(), DfsCatalogError>;
}

trait DfsChangeFeed {
    fn changes_after(
        checkpoint: &DfsFeedCheckpoint,
        limits: ChangeLimits,
    ) -> Result<LogicalChangePage, DfsChangeError>;
}

trait DfsRevisionReader {
    fn open_exact_range(
        object: &DfsObjectId,
        version: &DfsVersion,
        content_identity: &ContentIdentity,
        offset: u64,
        max_bytes: u64,
    ) -> Result<VerifiedRangeStream, DfsRevisionError>;

    fn lease_exact_version(
        object: &DfsObjectId,
        version: &DfsVersion,
        requested_ttl: Duration,
    ) -> Result<RevisionLease, DfsRevisionError>;
}
```

`begin_scan` returns an opaque catalog generation or snapshot token, topology
epoch, per-shard watermarks, an initial feed checkpoint, and an expiring
retention lease. `scan_page` returns records bounded by both count and encoded
bytes. The caller atomically installs the completed catalog projection and the
initial feed checkpoint, then calls `complete_scan` to release scan retention.
An expired handle forces reconciliation rather than creating an unprotected
scan/feed window.

The logical feed should expose consumer-relevant state transitions rather than
physical metadata mutations. A compact shape could include:

```text
DfsLogicalChange {
  stable namespace-entry and target object identities
  previous and current target version identities
  previous and current namespace locators
  flags: content | namespace | tombstone | policy | placement
  current policy and authority epochs
  provenance and logical change identity
}
```

DFS must resolve cross-shard moves, duplicate stale shard copies, prepares,
receipts, replay, repair, and other intermediate mutations before emitting this
record. A consumer should never reconstruct logical file state from the raw
replication journal.

Namespace and content identity are deliberately separate. Several namespace
entries may refer to one target object/version, and adding, removing, or moving
a hardlink can change workspace membership and policy projection without
changing content. Catalog pages and changes must preserve those namespace
transitions rather than deduplicating them by content identity.

## Why Object Versioning Alone Is Insufficient

Each SIFS object can have an immutable version while a multi-page catalog scan
still observes an inconsistent namespace. For example:

1. a scan reads page A;
2. an object moves from a later page into the already-read key range;
3. the scan reads the remaining pages;
4. the object is absent from the scan even though every returned object version
   was individually valid.

A catalog snapshot therefore needs a generation-pinned ordering and an atomic
feed handoff. Changes after the snapshot watermarks must remain replayable until
the scan completes or its lease expires. If DFS cannot preserve that history,
it must return an explicit `ReconcileRequired` result instead of silently
continuing from an invalid cursor.

## Failure, Consistency, and Policy Requirements

### Shards and Topology

One scalar journal sequence is not a cluster-wide ordering when metadata shards
advance independently. A checkpoint must be opaque and carry, directly or by
reference, the relevant shard generations, per-shard positions, topology epoch,
and scope. DFS, not memory, resolves a move crossing shard boundaries.

Catalog and feed results must distinguish at least:

- valid page or change batch;
- temporary incomplete coverage because an authoritative shard is unavailable;
- expired scan or revision lease;
- compacted journal gap;
- replaced metadata snapshot;
- changed shard map or authority epoch;
- authorization or policy denial;
- `ReconcileRequired` with a bounded restart strategy.

A timeout or unavailable shard is never equivalent to an empty result.

### Retention and Garbage Collection

The feed may identify version `V` and enqueue extraction asynchronously. If DFS
garbage-collects `V` before a worker opens it, the catalog was exact but unusable.
The consumer therefore needs either a documented minimum version-retention
window or, preferably, an explicit bounded revision lease. Leases must be
renewable only while policy still permits the read and must not grant namespace
or write authority.

### Changed Ranges and Physical Extents

DFS content manifests and hash trees may make incremental reads significantly
cheaper, but physical extents are storage implementation details. Memory
extractor chunks are format-aware semantic units such as headings, JSON records,
PDF pages, image regions, or time windows. DFS should expose only stable logical
range/digest semantics, if any, and memory should map those ranges onto its own
chunk boundaries.

Range reads alone do not prove which semantic chunks changed. A new revision may
still require a full streaming verification unless DFS can authoritatively
provide changed logical ranges or compatible content digests.

### Authorization and Information Leakage

A catalog leaks information even without file bytes. Paths, names, sizes,
timestamps, content hashes, placement, deletion history, and the existence of a
file can all be sensitive. Catalog, feed, layout, and revision reads therefore
need dedicated, policy-filtered capabilities; cluster membership alone is not
sufficient authorization.

Every page and range read is authorized against current policy. A policy
restriction in the logical feed installs an eager memory cache/index fence
before asynchronous cleanup, but feed delivery is not the serving security
boundary. Every candidate, score/count response, and evidence materialization
is independently reauthorized against current policy, or against a still-valid
offline authorization lease carrying a policy epoch and expiry. Feed lag or an
unavailable feed must never extend visibility. Cached semantic facts and
relation edges inherit at least the most restrictive current visibility of
their source anchors.

Host-exposed local files remain subject to node-local exposure permission. A
DFS catalog capability must not make an unexposed host path remotely enumerable.
Local-owned files that can change outside the DFS metadata path still require a
scoped native or VFS watcher plus reconciliation.

### Backpressure

Catalog scans, feed batches, and reads are bounded by encoded bytes as well as
item counts. Consumers use authenticated continuations and explicit budgets.
DFS metadata primaries should never perform content extraction or unbounded
semantic queries while serving filesystem operations.

These byte bounds are work and allocation targets, not an RPC or transport
message-size ceiling. An oversized metadata record must either be streamed or
returned alone under an explicit one-record progress rule; a continuation can
never become permanently stuck because the next record exceeds the preferred
batch target. Independently enforced schema/resource limits may reject an
invalid record, but they do not divide the network API into small- and
large-message variants.

## Optional Sharing of Derived Artifacts

Some extraction work, such as OCR or parsing a large immutable PDF, may be
expensive enough to share between services or nodes. A later facility could let
DFS store opaque immutable artifact manifests keyed by a shape such as:

```text
(object_id, version_id, producer_namespace, pipeline_fingerprint)
```

This can reuse DFS content addressing, placement, and transfer without giving
DFS ownership of the artifact schema. Such artifacts must remain:

- optional and rebuildable;
- excluded from ordinary file mutation transactions;
- separately authorized from the source object;
- garbage-collectable through explicit reachability or leases;
- ignored by DFS correctness and recovery;
- unusable as trusted policy facts merely because they are stored in DFS.

This optimization should follow the catalog and feed work, not precede it.

## Incremental Delivery

### Slice 1: Logical Catalog

- Define generic catalog entry, scope, limits, cursor, and coverage contracts.
- Serve generation-pinned, ordered pages across every metadata shard.
- Filter by current authorization and hide physical replication records.
- Add tests for concurrent rename, hardlink add/remove, deletion, and
  visible-version advancement during a paged scan.

### Slice 2: Atomic Scan and Feed Handoff

- Capture per-shard watermarks and topology identity at `begin_scan`.
- Retain post-watermark changes until scan completion or lease expiry.
- Emit explicit gap, replacement, topology-change, and reconciliation results.
- Prove that a concurrent mutation is visible either in the snapshot or in the
  post-snapshot feed.

### Slice 3: Logical Cross-Shard Changes

- Collapse physical journal mutations into stable logical transitions.
- Resolve cross-shard moves, hardlink/alias changes, and stale copies before
  emission.
- Keep batches bounded by entries and encoded bytes.
- Test replay, duplicate delivery, consumer restart, unavailable shards, and
  rescan recovery.

### Slice 4: Exact Revision Access

- Re-export exact-version bounded range streams through the R1 facade.
- Add bounded revision-retention leases and policy revalidation.
- Consider a versioned logical changed-range contract only after measuring
  large-file incremental extraction.

### Slice 5: Memory Projection

- Add an SIFS connector that normalizes catalog entries into memory source rows.
- Apply source changes, desired state, jobs, and checkpoints in one embedded
  database transaction.
- Preserve local watcher and reconciliation connectors for non-SIFS and
  host-exposed local-owned roots.
- Benchmark initial scan, steady-state feed cost, gap recovery, rename churn,
  policy fencing, and exact-version extraction.

### Slice 6: Optional Shared Derivations

- Measure duplicated extraction and transfer before adding shared artifacts.
- Keep artifact schemas and pipeline compatibility in their producing modules.
- Do not introduce semantic write amplification into DFS metadata transactions.

## Evaluation Questions

The implementation should answer these questions with measured results:

- How many metadata pages and bytes are required for one million objects?
- Does one source update cause work proportional to changed metadata rather than
  corpus size?
- Can a scan concurrent with arbitrary moves and deletes converge without a
  missed object or false tombstone?
- How long may a consumer lag before reconciliation becomes necessary?
- What is the write and storage amplification of retaining feed history?
- Does a policy restriction fence catalog and memory serving before cleanup?
- Can indexing run near an existing content replica without coupling index
  placement to source replica placement?
- How much large-file I/O is saved by exact ranges, changed ranges, or chunk
  digests?
- Can a node rebuild its projection while another metadata shard is offline,
  and does it report partial coverage honestly?
- How quickly can a corrupt or deleted memory database rebuild from a catalog
  snapshot and retained changes?

## Open Questions

- What storage mechanism should pin a catalog generation across sharded
  metadata backends without keeping unbounded snapshots alive?
- Should the catalog expose path aliases explicitly, or only a canonical path
  plus namespace change identities?
- Which policy fields are safe to project, and which must remain opaque
  authorization decisions?
- What feed-retention window is economical for intermittently connected nodes?
- Are revision leases necessary for every index job, or can a minimum history
  policy cover common pipelines?
- Should changed-range information expose fixed logical digest blocks, a
  content-defined map, or only a revision comparison operation?
- When does sharing an opaque derived artifact save enough work to justify its
  placement, retention, and authorization machinery?
- Should a semantic virtual-directory view exist at all, or are memory query
  APIs and UI projections the better interface?

## Sources Consulted

- [Semantic File Systems, SOSP 1991](https://www.cs.cornell.edu/courses/cs615/2001fa/615/sfs.pdf)
- [Practical File System Design with the Be File System](https://www.haiku-os.org/legacy-docs/practical-file-system-design.pdf)
- [Haiku attributes and indexes](https://www.haiku-os.org/docs/userguide/en/index.html)
- [Apple Spotlight architecture](https://developer.apple.com/library/archive/documentation/Carbon/Conceptual/MetadataIntro/Concepts/HowDoesItWork.html)
- [GNOME LocalSearch architecture](https://gnome.pages.gitlab.gnome.org/localsearch/overview.html)
- [Spyglass: Fast, Scalable Metadata Search for Large-Scale Storage Systems](https://www.usenix.org/conference/fast-09/spyglass-fast-scalable-metadata-search-large-scale-storage-systems)
- [DelveFS: An Event-Driven Semantic File System for Object Stores](https://www.salkhordeh.de/files/pdfs/delvefs-cluster2020.pdf)
- [Amazon S3 Metadata tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/metadata-tables-overview.html)
- [WinFS: Search and Management Based on Content](https://learn.microsoft.com/en-us/archive/msdn-magazine/2004/january/winfs-lets-users-search-and-manage-files-based-on-content)
- [Indexed File Memory Literature](indexed_file_memory_literature.md)
