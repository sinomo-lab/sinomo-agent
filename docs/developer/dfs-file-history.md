# DFS File History Interfaces

Read the [DFS architecture](../dfs-architecture.md) for authority and durability
rules and the [user guide](../user/file-history.md) for commands. Remaining
integration and acceptance work is tracked in the
[implementation plan](../plans/dfs-file-history.md).

## API Ownership

Shared request and response types live in `sinomo-api::dfs::file_history`.
`sinomo-client::sifs::SifsBrowser` exposes typed daemon calls. R2 callers import
these contracts through `sinomo_runtime`; service implementation remains in
`sinomo-dfs`.

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

History reads accept an offset and `max_bytes` in 1..=65536. They return
`Found`, `NotRetained`, or `ContentUnavailable`. A collector validates identity,
offset, total length, and returned range length on every response. Bytes are not
rehashed on ordinary reads. Scrub verifies stored bytes; ingestion establishes
their content address.

The legacy SIFS `read_object_version` client call collects a complete result,
but its service now resolves the current authority before reading. Only an
exact current or explicitly retained version is admitted. Cached metadata,
leftover manifests, and a supplied hash are not read authority. Internal
content-store helpers remain available to typed repair and transfer paths;
they are not public history interfaces.

Explicit version reads use the resolved immutable descriptor without asking
legacy conflict records to select its content. This applies to an exact current
version as well as an explicitly retained version. The ordinary namespace read
path retains its conflict handling. Both paths share the same local range reader
and bounded remote transfer loop; immutable reads disable stale fallback and
retain identity, hash-metadata, bounds, and length validation. Reading or restoring
retained bytes does not itself clear legacy conflict records. Existing conflict
maintenance can mark a record superseded after a new canonical restore commit.

Operator clients also expose `restore_file_history` and send the same restore
request to the browser history route. The former `UseConflictVersion`
inspection request is retained only to return a migration error: a metadata
backend cannot implement content durability, conditional publication, or exact
restore replay. CLI and app version selection use the durable restore API.
The app freezes the source, destination expectation, and operation ID when
staging confirmation and retains them after an error for an exact retry.
Diagnostic conflict-record cleanup remains a separate explicit operation.

## Capture and Retention

An ordinary object generation has no retained provenance and is not file
history. Explicit captures annotate the existing version store and use the
existing named version pins. There is no second content database.

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

Pin publication reconciles executable repair work for the exact retained
version on its current metadata authority. Releasing the last applicable root
retires jobs and sender state; another pin or a live policy grant keeps them
active. Expired grants are checked before retry backoff. Re-pinning can
reactivate a full committed intent and emit its exact missing-replica delta to
the durable queue without changing the canonical generation. Historical repair
reconstruction uses the current primary, not the original writer's identity.
Compacted terminal retry receipts are excluded from executable repair recovery.

If the original full intent has compacted but its retry receipt is still
available, re-pinning creates a separate committed `RetainedRepair` owner. It
copies the recorded desired nodes and W/N policy, never inferring N from current
locations. The owner cannot enter a write-publication route or become a user
retry receipt. GC keeps its durability plan while retained version metadata
exists without making the owner itself an unconditional content-retention root.

A completed pin request can be retried after a crash between metadata commit
and the local queue handoff. Its replay re-emits newly actionable repair records
and the maintenance wake while preserving the original metadata result. It
does not recreate a subsequently removed pin. Preserving policy before the
last original retry witness is evicted, resolving metadata-only generation
aliases, and unattended process-crash closure remain unfinished; see the
implementation plan for these limits and distributed acceptance work.

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

Retries locate the durable job or compact result before re-reading source or
destination. A known result is returned even after later edits or source-pin
release. Reusing the operation ID with changed inputs is rejected. Expired
operation evidence fails closed rather than repeating an uncertain write.

## Compatibility and Verification

History mutations require at least PackedV3 metadata prepares and the advertised
`dfs.metadata.file-history.v1` capability. Retained publication has distinct
wire variants that older peers cannot silently interpret as canonical writes.
Codec and publication-purpose checks apply before durable authoring and replay.
An authoritative current-object query may run on earlier shard codecs without
enabling historical capture there; retained queries require PackedV3 or newer.
Creating a retained repair owner on an assigned shard additionally requires
PackedV4 and `dfs.metadata.file-history-repair.v1`. Codec selection checks every
assigned member and never downgrades a persisted shard contract.

Focused regressions live in `src/tests/file_history.rs` and
`src/metadata_backend/tests/file_history.rs` under `crates/sinomo-dfs`. Exercise
the complete affected DFS suite, API/CLI/client checks, and workspace boundary
tests after changes. Process, disk, handoff, and transport acceptance must use
the corresponding distributed harness; in-process routing tests do not prove
those boundaries.
