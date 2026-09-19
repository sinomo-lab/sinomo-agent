# Explicit DFS file history

Implementation goal: file content history, restore, deleted-file recovery,
retention configuration, and named pins through DFS APIs and CLI.

## Required semantics

- The default policy captures no history. Only explicitly pinned versions are
  retained without an enabled retention rule.
- Normal DFS has one authoritative filesystem view. Rejected/stale writes,
  replica divergence, and ambiguous delivery never create alternate file
  versions or resolve consistency failures by keeping both outcomes.
- Internal object generations and temporary write/recovery evidence are not
  user-visible history. A retained version needs an explicit capture reason:
  a named checkpoint, an enabled retention rule, or an explicit request to
  preserve local-operation content. Local-operation variants use the same
  retained-version storage, reads, pins, and restore path as other captures.
- History follows canonical object identity across renames and hardlinks.
  Deleted and replaced objects remain discoverable by their captured path;
  reusing a path does not merge independent object identities.
- Restore is a new conditional write, with stable retry identity. It does not
  rewind the current generation, silently replace a concurrent write, restore
  historical permissions, or bypass content/metadata publication requirements.
- A retention rule captures the outgoing content on successful replacement or
  deletion. Attribute-only updates and shard source-eviction tombstones do not
  capture content. Count and age protection are a union, measured from capture
  on supersession/deletion. Independent named pins are absolute roots.
- Retained metadata and bytes participate in the existing shard journals,
  snapshot validation, handoffs, content-root catalog, and repair mechanisms.
  They do not live in a separate database or in cluster KV. Compact retention
  configuration belongs in cluster KV and its authoritative DFS projection.
- Current authorization applies to historical reads and restore. Historical
  reads resolve an authoritative retained record, use bounded ranges, and
  distinguish unavailable content from absent/expired history.
- Old peers must not silently ignore active retention semantics. Capability
  admission and persisted/wire validation must cover every affected peer path.

## Implementation and acceptance checklist

- [ ] Shared history/policy/capture/restore/pin request and response contracts.
- [ ] DFS-owned retained-version records, indexes, journal/snapshot/replay,
      validation, and shard migration.
- [ ] Atomic policy capture at content replacement/deletion boundaries; default
      no automatic captures; explicit checkpoint and local-content capture.
- [ ] Named pins and retention-aware metadata/content GC, including deleted
      objects and protection against GC/read/capture races.
- [ ] Historical content routing, repair, and existing W/N/placement behavior.
- [ ] Conditional, idempotent restore and recovery to an unoccupied path.
- [ ] Cluster retention configuration, service routes, typed client, and CLI.
- [ ] Shared local-operation conflict preservation integration without normal
      operation creating conflicts.
- [ ] Capability and persistence compatibility checks.
- [ ] Regression tests for default behavior, pins, count/age retention, delete,
      replacement, hardlinks, rename/handoff, restore races, retry, restart,
      replica repair, and GC safety.
- [ ] Full affected crate checks, architecture boundary checks, and relevant
      distributed process/disk verification.
- [ ] User/developer documentation and requirement-by-requirement completion
      audit against the current worktree and test results.

## Current evidence

The starting checkout already has immutable object versions, named durable
metadata version pins, metadata-first GC with a complete content-root catalog,
and complete retained-family transfer during cross-shard handoff. Ordinary
stale-write/reconnect paths already reject or reconcile without minting DFS
conflicts. Existing exact-version browser reads are local whole-buffer reads;
repair eligibility follows current content. These mechanisms must be extended,
not replaced with a second version/conflict store.

No implementation or acceptance item is complete merely because its API or
record type exists. This checklist tracks the full requested outcome.

## Implementation progress

The first implementation slice is present, but the overall feature is not
complete. `sinomo-api::dfs::file_history` owns the shared retention and capture
contracts. `DfsObjectVersionRecord.retained` is an optional boxed provenance
record in the existing version store; default commit generations allocate no
capture record. Pins remain independent existing metadata-version pins.

Policy capture is planned in the object-index transition, including namespace
publication and replay. Count/age roots feed metadata GC; default keep-latest
floors are now zero. `CaptureFileVersion` provides an atomic backend capture
and first pin. Namespace transactions additionally carry explicit pin changes
so service operations use the normal replicated prepare/publication path.

The current service and browser API implement capture/pin/unpin and retention
configuration. The client has typed methods. CLI grammar and execution cover
`dfs history capture`, `pin`, `unpin`, and `retention`. Cluster configuration
updates compare the prior policy and preserve unrelated placement settings.
Writers refresh the policy before publishing. Packed-v3 and the
`dfs.metadata.file-history.v1` capability fence history-aware metadata shards.

Completed checks during this slice: DFS history tests (10 passed), API policy
tests (3 passed), CLI/client checks including all targets, and diff checks.
Later hardening edits and new projection assertions still need their final
test run. The complete DFS library suite is also being run; its result must be
inspected rather than inferred from the focused checks.

Remaining implementation includes distributed authoritative listing and
historical reads, restore/recovery, historical content repair, actual staged
local-operation preservation and conflict integration, the corresponding CLI
and client methods, documentation, and distributed acceptance. The metadata
local-operation test does not prove content transfer or adapter integration.

Specific review obligations before completion:

- Count retention currently orders policy captures by timestamp. Replace that
  with authoritative ordering and test clock reversal, equal timestamps,
  mixed policy changes, collection gaps, and shard moves. Do not claim those
  retention cases are already covered.
- Test retry after later writes and restart through the public capture API,
  including standalone backend use; preserve command identity through recovery.
- Route hardlink capture to canonical identity and verify deletion/replacement
  discovery. Preserve retained provenance during every location/metadata update.
- Reconcile the legacy exact-version browser read and conflict-version selection
  surfaces with explicit capture and new-generation restore. Internal generations
  must not become an alternate user-visible history surface.
- Validate all new pin/capture fields at every prepare, snapshot, handoff, and
  replica boundary. Check mixed-version rejection and capability loss.
- Cover version-only namespace transactions in capability/path selection and
  ensure policy refresh and batching do not introduce per-object KV scans.

### Authoritative queries and first restore path

`file_history_query.rs` now routes shard-scoped history queries through the
existing authenticated namespace RPC envelope. It rejects stale authority
stamps, serves a bounded coherent backend projection, filters source-eviction
copies, and binds list cursors to the query and complete topology. The public
range read resolves an explicit capture first, then uses the existing exact
immutable range transfer without publishing a current head or rehashing stored
bytes. Absent retention and unavailable content have distinct typed responses.

The API, typed client, and CLI now also expose list/read/restore/recover.
Restoration uses the normal conditional reader-backed write path, preserves
current permissions, and creates a new generation. Recovery is an exclusive
create and obtains a new object identity. Remote-only restore content is staged
inside the DFS content store with bounded memory. An internal source pin protects
an in-progress restore; durable cleanup/reconciliation of interrupted restores
still needs completion.

Pins now publish version/pin-only transactions. Republishing the old object's
tombstone while unpinning after recovery could otherwise hide a newly recreated
path. Both the namespace prepare and local sharded backend preserve these
metadata-only pin transactions.

The completed focused run passed 13 history tests, including remote primary
query and old-version range transfer, stale restore rejection, new-generation
restore, deleted-path recovery, and query-bound cursors. The previous full DFS
library run passed 1,855 tests and failed two: the primary-transfer fixture
started at packed-v2 and consequently exercised an extra codec-upgrade epoch;
it now starts at packed-v3 for its intended transfer test. The resident cache
discovery test's transient Busy failure passed in isolation. Both tests passed
individually on the newer test binary. A complete final suite rerun remains
required.

Additional completion obligations found during this slice:

- Persist/reconcile restore outcomes independently of source availability and
  later destination writes. Internal source-pin cleanup failures must remain
  recoverable; a completed write must never be reported as rejected merely
  because cleanup failed. Current code returns the committed receipt and logs
  a cleanup failure, but has not yet implemented its durable cleanup worker.
- Verify replicated capture/pin transactions and restore with actual failure,
  lost-reply, restart, and shard-handoff scenarios. The remote query test is
  transport-backed in-process evidence, not process/disk acceptance.
- Historical replica repair and staged local-operation capture integration are
  still unimplemented. Do not infer those from the metadata source enum or its
  tests.

The CLI/client all-target check passed after the query/restore surface was
added. The full DFS library rerun is in progress, with output in
`/tmp/sinomo-file-history-full-v2.log`; resume the existing process before
starting another full run. The resident-cache test now retries only its
documented process-wide cache `try_lock` contention result, with a bounded
deadline; production status behavior is unchanged.

For subsequent implementation, prioritize the remaining shared content-write
and repair boundary, durable restore outcome/cleanup ownership, and retention
ordering. A local-operation candidate must use the same content, receipt, and
retained-version machinery while never advancing the canonical object head.
Also test age retention after delayed metadata publication; file timestamps
must not substitute for the authoritative supersession boundary.

### Retention ordering and publication time

The complete preceding DFS library run passed all 1,860 tests. The next focused
run passed 20 history tests. Count retention now uses an object-local ordering
high-water mark carried by current generation records and transferred with the
version family. Collecting intermediate versions cannot renew an older count
grant. The counter follows metadata-only changes and survives checkpoint,
restart, and migration. Data-free counters on deleted objects become collectible
after the retained family is gone. Ordinary writes use point lookups; family
scans are restricted to adoption, rather than every new generation.

Age grants are now stamped from the authoritative publication journal entry or
the committed-prepare receipt, rather than file timestamps or prepare creation.
The author, journal replay, and semantic table-marker replay use the same time.
Tests cover equal/reversed journal clocks, collection gaps with changed rules,
migration, counter exhaustion before publication, delayed prepare publication,
and final tombstone cleanup.

History capture through a hardlink now resolves the canonical object and its
authority path. Existing version pins can follow the canonical object's path
after link promotion. A service regression verifies capture through an alias,
unlink of the original path, and unpin without changing surviving file content.
Cross-shard adoption now requires a history-aware destination for retained
records or a nonzero ordering counter.

Current remaining major work is unchanged: staged local-operation preservation
and historical repair must share the existing content/receipt machinery;
restore outcomes and source-pin cleanup need durable ownership and restart
reconciliation; legacy read/conflict-selection surfaces need integration; and
the final full, distributed, compatibility, and architecture checks plus user
documentation remain outstanding. A metadata-only local-operation test is not
proof of uploaded-content durability or repair.

### Validation environment

During this work the sibling SUI checkout received unrelated dependency
upgrades. Its new `js-sys`/`wasm-bindgen` requirements conflict with this
checkout's existing lockfile; a resolution dry run would upgrade 20 UI
packages. Those source manifests and the original lockfile were left alone.

An isolated verification pair is available under the ignored directory
`target/file-history-verification/`: `agent` starts at Sinomo
`3da2ee0db7553e54ef4d17a99ccc9b9491de62e4`, and `sui` is pinned to
`2324413b09748f2652f3fa8b81d01529b6729f06`. Run
`python target/file-history-verification/sync.py` from the debug checkout to
copy current source changes into `agent`. The script fingerprints every copied
file in `source-state.json`; the verification lockfile is an independent copy
of the existing pre-upgrade lockfile and is not refreshed from the changing UI
workspace. Continue editing the original debug checkout, never the copy.

Strict `cargo clippy -p sinomo-dfs -p sinomo-api --all-targets --locked -- -D warnings`
passed in that synchronized checkout after two small style fixes. This is
validation against the recorded SUI baseline, not a claim that the concurrent
UI dependency upgrade has been integrated or verified.

Next content-path work should persist an explicit publication purpose through
intents and compact receipts, so retaining local-operation bytes can reuse the
existing staging/receipt/repair engine without ever advancing the canonical
head. Replica ingestion and restart recovery must preserve that distinction;
adding a metadata source label alone is insufficient. Historical repair also
needs to recognize explicit retained roots when an originally current write is
superseded, and must stop when those roots are released.

### Explicit retained-content publication and repair

The shared write intent now carries `DfsWritePublication`, preserved in compact
intent receipts. Retained intents use the ordinary W receipts and repair jobs,
but commit only a retained version plus its first pin through the namespace
publication lane. They neither occupy the canonical pending-writer slot nor
advance the current generation. Full and incremental hydration preserve this
distinction and never install a retained descriptor as transient current data.

`StageRetainedBlob` and `RepairRetainedBlob` are distinct wire variants. They
reuse authenticated blob streaming, receipt correlation, and immutable content
storage, without canonical replica metadata or active-current-stage effects.
The sender selects storage-only repair for retained records. An old current
intent remains repairable when its exact target has a live pin or policy grant;
GC preserves an incomplete retained repair owner until N or root release.

A regression exposed that replay materialization rejected a changed publication
purpose only after journal append. Sparse intent-transition validation now runs
before author batches and replay persistence. Deferred replica ingest catches
up an existing lag when an incoming intent transition needs that prior state;
namespace-only acknowledgements keep their deferred materialization path.
Publication purpose is immutable across retry, replay, snapshot, and compaction.

The focused unoptimized DFS test run passed 26 history tests
(`/tmp/sinomo-history-publication-tests-v5.log`), including rejection of an
incompatible purpose change within one received batch before any journal append.
Strict Clippy for DFS/API and all targets passed with the normal profile
(`/tmp/sinomo-history-publication-clippy-v2.log`). An earlier optimized test build
was stopped after about eleven minutes because its source snapshot predated
the repair work; no test result is claimed for that interrupted build.
For focused iteration, use the isolated verification checkout with
`CARGO_BUILD_JOBS=2 cargo test -p sinomo-dfs file_history --lib --locked
--config profile.dev.package.sinomo-dfs.opt-level=0`. This is a command-local
override; repository profiles are unchanged. Final checks still need the normal
repository settings. The complete DFS library suite passed all 1,873 tests with
the same command-local test override (`/tmp/sinomo-history-content-full.log`).
After that full run, a final retained-transfer filesystem guard was added: a
matching supplied descriptor cannot override the canonical object's filesystem.
The latest focused run and strict Clippy above cover this final guard and its
regression. The unrelated original Cargo.lock change remains only the existing
`lru` dependency under `sinomo-ui-runtime`.

Remaining content work: connect a public, explicitly invoked reader-backed
local-operation preservation API/route to the new intent path, capability-gate
before persisting the intent, and cover W<N, lost reply, cancellation, metadata
handoff, and remote content recovery end to end. Audit released-history repair
job retirement and re-pin scheduling: liveness is checked, but in-memory job
suppression and durable repair-attempt ownership must not cause repeated idle
work or prevent an explicit new pin from resuming repair. Validate preservation
intent/receipt proof at every namespace-prepare receiver boundary. Durable
restore outcomes and source-pin cleanup, legacy read/conflict-selection
integration, user documentation, and full/distributed acceptance remain open.

### Reader-backed explicit local preservation

`NetworkedDfsService::preserve_local_file_version` now accepts a reader and the
shared `DfsFileHistoryPreserveRequest`. It captures the input into DFS immutable
content and returns a retained identity or a pending operation. The request
separates the preservation operation from the originating local operation and
its base version. Capability admission happens before consuming the reader.
Metadata-less callers use a path hint to resolve and verify canonical identity;
an occupied path cannot silently substitute another object.

The explicit publication purpose travels through the existing stable manifest
write path. `RetainedWriteRequest` is a distinct wire variant; ordinary and
exclusive-create frames reject a retained purpose. Redirects preserve the
variant. Pending retained operations do not block ordinary writes in either
the coordinator indexes or the durable intent scan. Retries bind the original
bytes and provenance, and a completed retry never recreates a released pin.
Ordinary write reconciliation cannot report a retained receipt as a canonical
write. Local-owned content remains on its sole owner; replicas and confirmed
objects retain their existing placement and W/N constraints.

Routed testing exposed a namespace-prepare assumption that every receipt must
have a current-object row. Retained publication now carries a narrowly validated
version/pin transaction instead: the receiver requires the matching durable
retained intent, exact source and pin, valid replication plan, and W matching
receipts already represented in its metadata prefix. A prepared capture stays
hidden until the usual metadata publication barrier. Completed transaction
receipts support exact replay after intent compaction. Captured content
locations describe its own receipt set, rather than the current file's copies.

Validation: all 1,881 DFS library tests passed in the isolated verification
checkout using the command-local unoptimized DFS test setting
(`/tmp/sinomo-history-preserve-full.log`). Strict DFS/API Clippy across all
targets passed with repository settings
(`/tmp/sinomo-history-preserve-clippy.log`). Tests cover local and routed reader
preservation, restart and replay after unpin, mismatched input, old-peer wire
rejection, W-pending preservation alongside current writes, receipt completion,
capability rejection before reader consumption, local-owned placement, and
receiver rejection without durable W evidence. CLI/client all-target checks
passed (`/tmp/sinomo-history-preserve-client-check.log`), and all 16 architecture
boundary tests passed (`/tmp/sinomo-history-preserve-boundaries.log`).

The library/peer preservation entry point is now implemented; the earlier
content-work note should not be read as saying it is still absent. Remaining
work includes preservation failover/cancellation and W<N recovery coverage,
retired repair work and explicit re-pin wakeups, durable restore results and
source-pin cleanup, legacy read/conflict-selection integration, user/developer
documentation, and distributed process/disk acceptance. No normal write failure
calls the preservation API.

### Durable restore results and cleanup

Restore now admits a destination-owned namespace job before reserving source
content. The job binds the original requester, restore request, replication
options, ordinary write operation identity, and deterministic destination
identity. Read-only history queries find full jobs and compact result receipts
before looking up the source or current destination. A completed retry therefore
returns the original result after source unpinning, destination edits, restart,
and job compaction. Expired operation evidence fails closed instead of repeating
the restore.

The existing namespace-job maintenance lane owns source-pin cleanup. It records
source selection before acquiring the pin and records the destination outcome
before cleanup. Exact pin-presence queries reconcile a lost pin/unpin reply
without depending on the old namespace request's retry window. An unfinished
job protects its destination write witness through GC; job-state validation
prevents stale runners from replacing newer progress or changing the request.
These validations run before journal persistence and during replica/snapshot
admission. GC derives restore-intent roots once per pass.

The ordinary conditional/exclusive writer now accepts the job's durable
requester identity internally. Ingest receipts still name the node that actually
stored the input; restoring on a primary does not credit a metadata-less caller
with nonexistent bytes. Restored input is bound to the selected history hash and
length before publication. Remote input is quarantined once and passed directly
to the existing write path. Ordinary reads retain their existing no-rehash
behavior.

A routed regression exposed an existing history-pin prepare bug: the pin's
journal provenance ID overrode the request-owned prepare ID while still attaching
the completed request. History pins now use the standard request-owned
transaction path. Restore job execution also suspends the enclosing namespace
request while running independent source queries/pin requests, preserving both
requests' completion and authority scopes.

Validation passed in the synchronized isolated checkout: 38 focused history
tests (`/tmp/sinomo-history-restore-job-tests-v5.log`), all 1,885 DFS library tests
with the command-local unoptimized DFS test setting
(`/tmp/sinomo-history-restore-job-full.log`), strict all-target DFS/API Clippy
(`/tmp/sinomo-history-restore-job-clippy-v2.log`), CLI/client all-target checks
(`/tmp/sinomo-history-restore-job-client-check.log`), and all 16 architecture
boundary tests (`/tmp/sinomo-history-restore-job-boundaries.log`). Regressions
cover the commit-before-result crash boundary followed by a later edit and GC,
restart after a lost unpin reply, compacted-result replay after source unpinning,
stale job-state rejection, routed capture/restore with a distinct requester, and
wrong input rejected before destination/parent publication.

Still required: actual metadata-primary handoff and broader cancellation/W<N
failure coverage, historical repair retirement and re-pin scheduling, legacy
conflict-selection/exact-version interfaces, documentation, and distributed
process/disk acceptance. The legacy `use_dfs_conflict_version` still rewinds the
visible generation through a metadata-only helper; its callers are the offline
metadata-root CLI path and `inspect_cluster_aware_dfs_from_metadata`. Both browser
and CLI file-manager exact-version reads still call the unrestricted internal
`read_object_version_content` primitive. Those boundaries need explicit history
integration before completion. The durable restore job is implemented; earlier
progress notes describing only log-based source-pin cleanup are superseded.
