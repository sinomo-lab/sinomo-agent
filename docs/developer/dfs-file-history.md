# DFS File History Interfaces

Read the [DFS architecture](../dfs-architecture.md) for authority and durability
rules and the [user guide](../user/file-history.md) for commands. This document
describes the implemented contracts and their verification boundaries.

## API Ownership

Shared request and response types live in `sinomo-api::dfs::file_history`.
`sinomo-client::sifs::SifsBrowser` exposes typed daemon calls. R2 callers import
these contracts through `sinomo_runtime`; feature services and CLI grammar live
in `sinomo-dfs-history` (`sinomo_runtime::dfs_history`). The core owns immutable
revision identity, admitted mutations, durability, persisted roots and GC.

`module.dfs.history` is enabled by default. Disabling it withdraws history
routes and workers when the daemon reconstructs its registry; it does not clear
persisted retention configuration, revoke pins or cancel durable copy jobs.
Core commits keep honoring those rules, including while the feature is disabled.
Only an explicit policy change, recorded expiry or reference release changes
that promise. Frontend clients keep using the data-only API facade.

| Operation | Request | Client method |
|---|---|---|
| Discover captures | `DfsFileHistoryListRequest` | `list_file_history` |
| Read a range | `DfsFileHistoryReadRequest` | `read_file_history` |
| Restore or recover | `DfsFileHistoryRestoreRequest` | `restore_file_history` |
| Capture, pin, unpin | `DfsFileHistoryPinRequest` | `file_history_pin` |
| Read retention configuration | `DfsFileHistoryPolicyRequest` | `file_history_policy` |
| Compare and replace configuration | `DfsFileHistoryPolicyUpdateRequest` | `update_file_history_policy` |

Routes are the versioned `/daemon/dfs/browser/file-history/*-v1` constants in
`sinomo-api::dfs::browser_api`. Use the typed clients or constants rather than
opening the daemon's metadata stores. Compact policy is cluster configuration;
versions, pins, intents, jobs, receipts, and bytes remain DFS-owned.

`DfsFileVersionIdentity` binds filesystem, canonical object, and immutable
version. List pages scan at most 128 version records; an empty page need not be
terminal. Continue while `next_cursor` is present. The cursor binds the filters
and authority topology, so callers restart when it expires.
An exact object filter reads that object's version index, so its pages scan
only the object's own internal generations and captures. Path and subtree
filters scan the shard's version records in object order; a small page limit
bounds each scan rather than guaranteeing a matching result per page.
`DfsFileHistoryEntry::deleted` reports that the object's current namespace entry
is a deletion; such content can only be recovered with exclusive creation.
Authorities that predate the field omit it and decode as `false`.

History reads accept an offset and `max_bytes` in 1..=65536. They return
`Found`, `NotRetained`, or `ContentUnavailable`. A collector validates identity,
offset, total length, and returned range length on every response. Bytes are not
rehashed on ordinary reads. Scrub verifies stored bytes; ingestion establishes
their content address.

The older SIFS `read_object_version` client call collects a complete result,
and its service resolves the current authority before reading. Only an exact
current or explicitly retained version is admitted. Cached metadata, leftover
manifests, and a supplied hash are not read authority. Internal content-store
helpers remain available to typed repair and transfer paths; they are not
public history interfaces.

Explicit version reads use the resolved immutable descriptor, for an exact
current version as well as an explicitly retained version. They share the
ordinary read path's local range reader and bounded remote transfer loop;
immutable reads disable stale fallback and retain identity, hash-metadata,
bounds, and length validation.

Operator clients (`DfsOperatorClient` in `sinomo-client` and in the CLI's
`app_api`) also expose `list_file_history`, `read_file_history`,
`file_history_pin`, `file_history_policy`, and `restore_file_history` on the
same browser history routes. CLI and app version selection use the durable
restore API; DFS has no separate conflict-record or version-selection path.

The app's Files history sheet runs on the Files worker lane. For a file it
inspects the current object, lists captures with an object filter, and reads
the effective retention rule. For a folder it lists by path so deleted files
remain discoverable. One refresh follows continuations until 64 entries, 24
pages, or four seconds; the remaining cursor backs **Search older versions**.
Capture, pin, and unpin requests carry fresh ordered operation IDs because they
are idempotent by pin name. Restore freezes the source, destination
expectation, and operation ID when staging confirmation; recovery freezes its
request when confirmed and reuses it while the destination is unchanged. Both
are retained after an error for an exact retry. Capture of a directory is
rejected by the authority with an explicit reason rather than a missing-hash
error.

## Capture and Retention

An ordinary object generation is not file history. The core version store has
namespaced `retention: BTreeMap<String, DfsRevisionRetentionState>` and versioned
opaque `module_metadata`. Filesystem configuration uses a corresponding
`retention_policies` map of generic path rules and count/age minimum grants.
The history module owns its namespace, capture/source payload and user-facing
projection; the core interprets only the generic immutable revision grants,
ordering, object snapshots and independent named pin roots.

`DfsRetainedRevisionDescriptor` is the core's bounded, owner-checked descriptor
for a revision, current object and live pins. History queries decode their
module payload after that core authorization. Unknown module metadata remains
preserved. `sinomo-dfs-migration` converts legacy typed history fields into
this generic representation; current DFS reads and writes only the generic
form. No separate content database is introduced.

The default policy is empty. An enabled subtree rule atomically captures
outgoing content during successful content replacement or deletion. A stored
capture records its rule, publication time, and object-local sequence. Count
protection uses authoritative capture order, including across GC gaps; age
protection starts at capture. Protection is the union of count, age, and named
pins. Later policy changes cannot shorten a capture's recorded minimum grant.

`Capture` requires the exact current identity and creates its first named pin.
`Pin` requires an existing capture. Independent names create independent roots.
Capture/pin changes use namespace transactions and the same publication proof
as other metadata mutations. Their operation IDs bind retries to the original
request.

Single-file file-manager writes report success only after W and canonical
publication. Explicitly deferred APIs can stage earlier, but their completion
drain propagates a primary rejection instead of treating any terminal response
as success. Failure before publication cannot create a rule capture. Metadata
and N-W maintenance failures after a known commit remain follow-up work.

History listing and exact historical descriptors evaluate live count/age grants
as well as pins. The existence of an old policy-capture descriptor is not a
live retention grant. Count checks use the object's durable ordering high-water
mark, with per-page reuse in listings; pins and live age grants avoid that
lookup. Expired unpinned history is absent immediately, independently of physical
GC timing. The retained-content repair path uses the same read-side predicate.

### Explicit Local-operation Preservation

Applications can explicitly preserve staged local-operation bytes with
`NetworkedDfsService::preserve_local_file_version`, a
`DfsFileHistoryPreserveRequest`, a streaming reader, and `DfsWriteOptions`.
The request records its local operation and base version, canonical identity,
lookup path, named pin, and stable preservation operation ID. Retained writes
use the ordinary immutable content, placement, receipt, and repair machinery.
They cannot advance the current head or take its pending-writer slot. A pending
outcome is not a durable capture acknowledgement; retry the original request.

No ordinary write error invokes this API. Stale input, lost replies, uncertain
commit outcomes, and replica divergence retain their normal reconciliation or
failure behavior. A caller must explicitly request preservation rather than
using history as a consistency fallback.

## Content Durability

New data writes record `content_durability` alongside the immutable version:
its data-write generation, coordinator provenance, desired replicas, and
complete replication policy. Local single-replica batches record
their validated receipt contract; these have no write-intent epoch and use zero.
This field does not retain content or create a history entry. Metadata-only
changes preserve it when content identity and its modification event are
unchanged. Cross-shard moves carry it with the moved generation under the same
authority and content-identity checks as the rest of the family.

The original data generation and retry records may later be collected. A new
explicit capture can still reconstruct its repair owner from the recorded plan.
GC keeps an unfinished data-write obligation while a current metadata successor
still uses its content. Restoring that intent preserves the successor's current
CAS head; it cannot rewind the namespace to the data-write generation.

### Content Without a Recorded Plan

Current DFS never infers a durability plan from surviving locations or older
records. `sinomo-dfs-migration` converts retained legacy durability evidence;
its import grants follow actual source membership and fresh durable copies. A
historical or already retained version without a complete plan is rejected
until storage conversion supplies one.

A current visible file head without a recorded plan can still be captured.
Explicit capture and rule capture establish a fresh grant under the current
filesystem placement policy: the authority verifies the exact source bytes
through bounded ingestion and obtains fresh durable acknowledgements from W
selected replicas before recording the grant. Before preparing a rule-based
overwrite, bulk replacement, or deletion, the author records that plan in a
version-only metadata effect that changes neither the current head nor its
retention descriptor; the successful content mutation then captures the
outgoing generation and its repair owner together. Every publication path,
including batched single-replica writes, refreshes the current retention
policy before preparing its mutation.

Failed copying cannot publish a capture or advance the namespace; only
successful replacement or deletion adds retention provenance. Partial
transfers before capture can be repeated without creating a generation or a
second filesystem head. After capture, the ordinary retained repair owner
preserves responsibility for N-W replicas.

## Repair Ownership and Garbage Collection

Pin publication reconciles executable repair work for the exact retained
version on its current metadata authority. Releasing the last applicable root
retires jobs and sender state; another pin or a live policy grant keeps them
active. Expired grants are checked before retry backoff. Re-pinning can
reactivate a full committed intent and emit its exact missing-replica delta to
the durable queue without changing the canonical generation. Historical repair
reconstruction uses the current primary, not the original writer's identity.
Compacted terminal retry receipts are excluded from executable repair recovery.

Captures with recorded durability create their maintenance owner in the same
metadata commit, unless a full active or committed write already owns that
exact version with a matching durability plan. Placement work and incompatible
W/N plans cannot substitute for that owner. This applies to explicit captures
and policy captures on overwrite or deletion. Materialization and replay make
the same decision;
snapshot/table recovery does not depend on a later client retry. After the new
namespace head is installed, the service activates only the affected captured
identities and emits their repair-queue delta. Bulk publication reuses its
already-read filesystem policy when deciding whether this work is needed.

For a capture of an intent's target generation, GC journals a separate committed
`RetainedRepair` owner before compacting the full intent or evicting its retry
receipt. Re-pinning can also create this owner from a still-available receipt.
The owner requires the version's recorded durability plan and never infers N
from current locations. Owner creation consumes the GC mutation budget; a budget
of one publishes the prerequisite and leaves compaction for a later pass.

The owner cannot enter a write-publication route or become a user retry receipt.
GC keeps its durability plan while retained version metadata exists without
making the owner itself an unconditional content-retention root. After the
version is collected, a typed `RemoveRetainedRepairOwner` journal entry removes
the maintenance record. Replay rejects using that operation to remove an
ordinary write intent, and rejects changing an owner's recorded durability plan.

A completed pin request can be retried after a crash following its metadata
commit. Its replay preserves the original metadata result; the repair work the
pin made actionable is already derived from the shard tables. It does not
recreate a subsequently removed pin. A new pin transaction uses its
durable request admission time, not the original content's capture time. Local
execution without request admission uses the timestamp in a generated ordered
operation ID. Expired identities never acquire a new wall-clock timestamp to
escape a GC fence; legacy opaque identities retain conservative expiry.
Each DFS component startup or activation requests authoritative reconciliation
of metadata-owned obligations with the node-local queue; no completion marker
suppresses this recovery. Reconciliation generations are not reused after
completion, and a queue-persistence failure
requests reconciliation while the process is still running. These are startup,
activation, and failure edges; ordinary reads and idle wakes do not scan history.

Pending-repair summary counts include unfinished current content after
metadata-only edits and retained versions protected by pins or captured rules.
Age-only obligations expire from the read-only summary without a metadata
mutation. An incremental deadline index bounds each filesystem's expiry query
to the timestamp width, independently of the number of captured versions.
Pin, version, receipt, and cross-shard pin-transfer mutations maintain the same
projection; snapshot recovery rebuilds it from durable metadata.

Detailed repair inspection uses the same current-content and retention
predicate. Each repair's receipts belong to its exact target generation.
Global inspection scans durable intents in bounded input pages and keeps only
the requested output window, so a deleted object's repairs remain visible
after its path is reused. Object inspection and paged inspection continue to
exclude compact retry receipts from executable work.

After a certified primary transfer, retained repair runs under the new metadata
primary while preserving the immutable original grant. It can fetch bytes from
the former owner and finish missing replicas without changing the current head.
Committed repair work cannot act as an uncommitted current write, regardless of
the ordering of its opaque version ID.

History mutation lanes opened while a node was a replica can be reused after
promotion. Rebinding checks the instance-local fence, refreshes and matches the
authority assignment, and validates the exact completion and prefix-extension
proof. A stale cached replica role cannot hide that fence. Read-only lookups
remain observational; they do not activate a mutation lane.

## Restore and Recovery

A restore request names a retained source and a SIFS destination.
`expected_destination: Some(identity)` means conditional replacement;
`None` means exclusive creation. Both publish through the ordinary writer and
its configured W/N and metadata authority rules. Historical permission fields
do not replace the destination's current policy.

Before reading source bytes, the destination authority persists a namespace
job binding the requester, complete request, write options, deterministic
destination identity, and stable write identity. The job selects and pins the
source, records the write result durably, then releases its internal pin.
Maintenance resumes incomplete jobs and reconciles lost pin/unpin replies.
An incomplete restore protects its destination write witness against GC.

Restore executors check cancellation at entry and between durable steps.
Cancellation of a caller or worker does not revoke an admitted job or release
its source pin; recovery resumes the recorded operation and cleans up after its
durable result. The source remains protected even if the user's original pin
is removed while execution is stopped.

Retries locate the durable job or compact result before re-reading source or
destination. A known result is returned even after later edits or source-pin
release. Reusing the operation ID with changed inputs is rejected. Expired
operation evidence fails closed rather than repeating an uncertain write.

## Compatibility and Verification

History mutations use the current V7 prepare/commit family. Metadata-shard
configuration admission requires the `dfs.metadata.prepared-operation.v7`
capability on every assigned peer, so incompatible daemons must be upgraded
before they participate. Retained publication has distinct wire variants that
cannot be interpreted as canonical writes, and publication-purpose checks apply
before durable authoring and replay.

Retained storage formats, including earlier typed history columns, durability
plans and snapshot containers, are converted by `sinomo-dfs-migration` through
the shared [durable migration framework](storage-migrations.md). The DFS runtime
opens only current storage. Conversion requires no filesystem reset or
file-content rewrite; it preserves captured versions, pins, restore receipts
and repair ownership, and never invents history or changes the canonical file
head. A stored capture alone is not authoring authority.

Focused regressions live in the history crate's
[`core_integration_tests.rs`](../../crates/sinomo-dfs-history/src/core_integration_tests.rs)
and [`tests.rs`](../../crates/sinomo-dfs-history/src/tests.rs), with core
retention and collection planning in
[`metadata_history_gc.rs`](../../crates/sinomo-dfs/src/metadata_history_gc.rs).
They cover capture ordering and expiry, W rejection and N-W repair, fresh
current-policy grants, restore cancellation and reopen, exact replay, GC, and
authority handoff. Retained-format conversion is tested in
`sinomo-dfs-migration`.

Run the affected crate suites and trust-boundary checks:

```sh
cargo test -p sinomo-dfs --lib
cargo test -p sinomo-dfs-history -p sinomo-dfs-attachments --lib
cargo test -p sinomo-api --lib
cargo test -p sinomo-cli --lib
cargo check -p sinomo-client --all-targets
cargo test -p sinomo-core --test workspace_ring_boundaries
```

The [file-history verifier](../../docker/README.md#file-history-acceptance)
exercises the public CLI across a disposable six-node cluster. Its restart
profile verifies durable captures and restore receipts through two full-cluster
SIGKILL/restart cycles. Its handoff profile opens the destination's history lane
as a replica, then exercises capture, retained reads, and recovery after fenced
promotion. Both retain the standard readiness/mutation/convergence gates.

Keep command output, executable hashes, source patches, and results in the
verifier's artifact directory. A reused cluster establishes functional, routing,
and recovery behavior; clean formation, performance, and fault injection at
every WAL boundary require separate verification. The handoff profile covers
completed content. Incomplete retained replication after transfer is covered by
the focused service regression, not by that live profile.
