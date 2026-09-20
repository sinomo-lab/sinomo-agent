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

### Authoritative admission for public exact-version reads

After merging main, the legacy browser/file-manager/CLI exact-version reader
still opened content using a supplied hash without current-or-retained
admission. A regression reproduced successful reading of an unretained
superseded generation (`/tmp/sinomo-history-exact-read-repro.log`).

`read_object_version_content` now delegates to an authoritative SIFS version
reader. The generic `read_file_version_content` takes a full filesystem/object/
version identity. It resolves the current object through the owning metadata
authority, or resolves an explicitly retained descriptor, before accessing any
bytes. Supplied hash and size are expectations, not authority. Reads collect
bounded 64 KiB ranges under the resolved immutable descriptor, validating
identity, offset, length, and total size without rehashing stored bytes.

The existing shard-query envelope gained a `CurrentObject` selection. Its
admission checks filesystem, shard, epoch, primary, and in-flight namespace
operations. This selection works on earlier metadata codecs; retained queries
still require PackedV3. Sender and receiver use the same codec predicate.
No durable metadata format or internal repair content reader changed.

Both new regressions pass, together with all 40 focused history tests
(`/tmp/sinomo-history-exact-read-tests-v2.log`). They cover unretained and
unanchored content, stale local metadata and bytes, remote current and retained
reads, multiple ranges, empty content, mismatched hash/length, pin release, and
PackedV2 current-read compatibility. All 1,893 DFS library tests passed
(`/tmp/sinomo-history-exact-read-full.log`). These checks run directly in the
merged debug checkout with `--locked`; the earlier isolated pre-upgrade SUI
verification checkout is no longer needed. DFS unit tests retain the
command-local unoptimized DFS test setting.

Strict all-target DFS/API Clippy passed
(`/tmp/sinomo-history-exact-read-clippy.log`), as did CLI/client all-target
checks (`/tmp/sinomo-history-exact-read-client-check.log`) and all 16 architecture
boundary tests (`/tmp/sinomo-history-exact-read-boundaries.log`). Formatting,
diff whitespace, and new guide link checks passed. No deployment or distributed
process/disk acceptance is claimed by these checks.

User instructions now live in `docs/user/file-history.md`, with API and
durability details in `docs/developer/dfs-file-history.md`. The documentation
indexes and DFS architecture link these guides. The Files guide distinguishes
diagnostic generations from explicitly retained content.

Remaining: legacy conflict selection still calls the metadata-only
`use_dfs_conflict_version` helper through daemon inspection, CLI, and the
client/app operator interfaces. It must use the conditional restore path with
stable operation identity and destination expectations. Historical repair
retirement/re-pin scheduling, actual primary handoff, broader cancellation and
W<N recovery, and distributed process/disk acceptance also remain open. Public
exact-version read admission and the new guides supersede the corresponding
unfinished items in the previous progress note.

### Legacy selection uses durable restore

The old `use_dfs_conflict_version` path could set the current object back to a
stored generation and delete conflict records using metadata alone. A regression
reproduced that behavior before the change
(`/tmp/sinomo-history-legacy-repro-v2.log`). The retained wire request and helper
now return a migration error without changing the journal, current object, or
conflict records. They cannot authorize a content mutation.

CLI `dfs conflicts use-version` is now an alias for conditional history restore,
including a retained source identity, exact destination version, and stable
operation ID. Explicit `--object`, `--path`, `--version`, `--expected-version`,
and `--operation-id` avoid rediscovery on a retry. The alias no longer takes
conflict IDs or an offline metadata override; its JSON result is the ordinary
history restore response. Diagnostic record cleanup is a separate explicit
`conflicts resolve` operation.

Both operator clients send `DfsFileHistoryRestoreRequest` to the existing
history restore route. The app offers restore for retained content even without
an open conflict. It distinguishes internal generations from retained versions
and uses only exact object/version equality to label the current generation.
Staging confirmation freezes the source, destination observation, and operation
ID. An error preserves that complete request for retry; success clears it.
No code path silently captures an unretained internal generation to make a
restore succeed. User and developer guides describe the changed interfaces.

All 1,893 DFS library tests passed after removing the metadata rewrite
(`/tmp/sinomo-history-legacy-full.log`). All three affected app regressions
passed: retained selection, rejection of an internal generation, and exact
retry after a newer metadata observation. The wider `history_` filter passed 21
cases and failed the existing compact extra-large reconciliation layout case
(`/tmp/sinomo-history-legacy-app-tests.log`). That same test failed with identical
rectangle coordinates on unchanged Rust sources at `3e7b8eb2` and clean SUI
`29a205d` (`/tmp/sinomo-history-legacy-layout-baseline.log`). Every modified Rust
file was backed up and restored byte-for-byte after this comparison; the
unrelated layout assertion was not changed. The CLI parser regression also
passed (`/tmp/sinomo-history-legacy-cli-parser.log`).

Strict all-target Clippy passed for DFS, API, client, and CLI
(`/tmp/sinomo-history-legacy-scoped-clippy.log`). App all-target compilation
passed (`/tmp/sinomo-history-legacy-app-check.log`), as did all 16 architecture
checks (`/tmp/sinomo-history-legacy-boundaries.log`). The broader strict Clippy
invocation including the app reported ten lint errors in `backend/mod.rs`,
`state.rs`, and `ui/chat.rs`; those three files are byte-identical to HEAD
(`/tmp/sinomo-history-legacy-clippy.log`). They were not edited or suppressed.
Formatting and diff whitespace checks passed. These changes remain local and
uncommitted; no deployment or distributed acceptance was performed.

Next repair-lifecycle audit: both repair runners suppress old work when
`pending_repair_targets_current_content` becomes false, while pin publication
currently only applies its metadata transaction. Reproduce whether releasing
and restoring the last root leaves that work suppressed, including after full
intent compaction. Any repair reactivation must preserve existing authority and
W/N requirements rather than reviving a compact retry receipt as executable
work. Primary handoff, cancellation/W<N recovery, and distributed process/disk
acceptance remain outstanding.

Legacy coexistence also needs a focused case with an open old-style conflict
record and remote-only retained bytes: ordinary range reads still consult
`open_conflicts_for_object`, and conflict maintenance reconciles old records
against the current generation. Do not infer that interaction from the app's
request-shaping tests or from a restore whose source bytes are already local.
Also exercise exact current `Artifact` reads: retained capture supports file and
artifact content, while the new current-read selector currently names file and
symlink kinds. Resolve that coverage/behavior discrepancy before completion.

### Current artifacts and repair-root lifecycle

Two regressions reproduced before fixing this slice
(`/tmp/sinomo-history-repin-repro-v2.log`): the public exact reader rejected a
current unretained artifact, and last-pin release left executable coordinator
repair jobs behind after removing their pending sender state. Current artifacts
now use the same authoritative admission as current files without creating a
capture.

The coordinator now tracks inactive repair intents independently of their
durable records and canonical head. Pin transactions reconcile only the exact
object/version's full committed intents under current metadata authority.
They retire work when its roots disappear and reactivate missing-replica jobs
when an explicit root returns. New jobs feed the existing per-message durable
queue delta and maintenance wake. Restoring an older canonical intent restores
the actual current head in the coordinator's validation cache, so a re-pin
cannot rewind it. Retained sender reconstruction follows the current primary
for committed work while pending writes retain their existing coordinator
admission. Compact terminal receipts cannot revive executable repair work.

Both repair runners evaluate root/authority eligibility before retry backoff,
so an expired retention grant does not leave repair work queued until a distant
network retry. Independent pins and current-content obligations remain roots.
After the final missing replica becomes durable, ordinary cleanup releases
sender and retry state.

A further regression covers lost local repair-queue handoff after pin metadata
publication: a lazy restarted lane replays the completed request, reconstructs
the exact missing-copy queue entry and wake, and appends no metadata journal
entry. The shared routed/local namespace replay path owns that recovery; it
never recreates a pin that was subsequently released.

Lazy restoration also records an unrooted committed retained intent as inactive
in the coordinator. A later generic receipt restore therefore cannot rebuild
its obsolete repair jobs. Replaying the original capture commit after pin
release completes its durable write witness without restarting N-replica work.
The pin lifecycle regression now covers that cold coordinator/receipt sequence.

All 45 focused history tests pass (`/tmp/sinomo-history-repin-tests-v5.log`).
They cover current artifact reads without capture; two independent roots;
release/re-pin and successful repair to a returning replica; canonical writes
after re-pin and restart; expiry during retry backoff; and completed pin replay
after the queue handoff is lost. After the replay hook, all 1,898 DFS library
tests passed (`/tmp/sinomo-history-repin-full-v3.log`), together with strict
all-target DFS/API Clippy (`/tmp/sinomo-history-repin-clippy-v3.log`), all-target
CLI/client compilation (`/tmp/sinomo-history-repin-client-check-v3.log`), and all
16 architecture checks (`/tmp/sinomo-history-repin-boundaries-v3.log`). Formatting
and diff whitespace checks passed. Tests ran directly in the merged debug
checkout with `--locked`, retaining the command-local DFS unit-test opt-level
override. The changes remain uncommitted; no rollout or distributed process
acceptance occurred.

Still required: a new repair obligation when a version is re-pinned after its
full intent has already compacted, unattended process-crash queue recovery,
actual metadata-primary handoff and broader cancellation/W<N acceptance,
legacy open-conflict coexistence with remote-only retained bytes, and final
distributed process/disk verification. The artifact admission discrepancy and
full-intent pin/retry cases above supersede those earlier audit notes; they do
not prove these remaining boundaries.

### Retained reads with legacy conflict records

A regression reproduced the remote-source failure before changing the reader
(`/tmp/sinomo-history-legacy-read-repro.log`). The metadata authority had an
explicitly preserved local-operation version, a matching receipt from a data-only
peer, and an imported open conflict record, but no local content. History read
returned `ContentUnavailable` without attempting that peer because the shared
range path applied namespace conflict handling first.

The range reader now distinguishes namespace admission from an already resolved
immutable descriptor. Public exact-version reads still resolve current or
explicitly retained metadata before choosing the immutable path. Namespace reads
keep their existing conflict behavior. Both paths reuse the physical range
reader and the remote retry/response loop; immutable reads never enable stale
fallback. No stored-byte rehashing was added, and all identity/length/bounds
checks remain.

The expanded regression passes
(`/tmp/sinomo-history-legacy-read-restore-tests.log`). It exercises a primary with
no local bytes, a data-only peer, retained range reads, exact retained and current
reads, recovery at an absent path, and conditional restore over the original
file. Recovery leaves the source head unchanged; replacement creates a fresh
current generation. Neither read nor restore clears the old conflict record.
The existing conflict-maintenance pass then marks that record superseded, and
ordinary namespace reads return the newly restored content. The peer never
acquires an alternate namespace object. The focused history suite passed 46
cases, and the final expanded test passed separately. All 1,899 DFS library
tests then passed (`/tmp/sinomo-history-legacy-read-full.log`), together with
strict all-target DFS/API Clippy (`/tmp/sinomo-history-legacy-read-clippy.log`),
all-target CLI/client checks (`/tmp/sinomo-history-legacy-read-client-check.log`),
and all 16 architecture checks
(`/tmp/sinomo-history-legacy-read-boundaries.log`). Formatting and diff whitespace
checks passed. These checks used the current debug checkout with `--locked` and
the command-local DFS unit-test opt-level override; they are not distributed
process/disk acceptance.

The compaction gap remains distinct. GC deliberately roots an intent's target
through its terminal retry witness, so its full intent can compact while the
retained version row remains available to a later pin. Root reconciliation
correctly refuses to execute that compact receipt, but needs a fresh durable
repair owner. Existing placement reconciliation demonstrates how to create
committed repair work for existing immutable bytes without advancing a head.
Historical recovery must also preserve the intended W/N plan after retry
witness eviction; observed locations alone are not evidence of the desired
replica count. That ownership/policy lifetime and unattended process-crash
recovery still need implementation and distributed acceptance.

### Fresh repair ownership after intent compaction

The W=1/N=3 re-pin regression reproduced zero queued repairs after the original
full intent compacted (`/tmp/sinomo-history-compaction-repro.log`). Reconciliation
now creates a separate committed `RetainedRepair` owner from an available
original retry receipt, retaining the recorded W/N policy and desired nodes.
The regression verifies two missing-copy jobs under a fresh intent ID, their
queue handoff, and an unchanged canonical head and content.

This owner cannot be submitted as a new write, carry user operation identity,
or become a compact user retry receipt. GC keeps its durability plan while the
retained version metadata exists, without treating the closed owner as an
unconditional retention root. Assigned shards require the new PackedV4 contract
and `dfs.metadata.file-history-repair.v1` capability before creating it; current
and retained reads continue to support the earlier applicable codecs.

This is an implementation checkpoint, not completion of repair recovery.
Automatic owner materialization before the last original policy witness is
evicted is still missing. So are metadata-only generation lineage coverage,
unattended crash recovery of the queue handoff, and distributed primary-handoff,
cancellation, W<N, process, and disk acceptance. A PackedV3 shard can publish a
pin before discovering that creating a new repair owner requires PackedV4;
admission/upgrade handling for that case still needs closure. The targeted
compaction regression does not establish these remaining boundaries.

Pre-commit verification passed all 1,900 DFS library tests
(`/tmp/sinomo-history-commit-dfs-tests-v2.log`) and all 16 architecture checks
(`/tmp/sinomo-history-commit-boundaries.log`). The first full run exposed a
handoff fixture starting at PackedV3: the legitimate codec upgrade advanced its
epoch before the primary-transfer assertion. The fixture now starts at PackedV4
so the unchanged handoff assertions isolate primary transfer. The CLI shard
verification harness also now recognizes and labels PackedV3 and PackedV4.

Strict all-target Clippy passed for DFS, API, node, client, and CLI
(`/tmp/sinomo-history-commit-clippy-v2.log`). App all-target compilation passed
(`/tmp/sinomo-history-commit-app-check.log`), as did formatting, staged whitespace,
and new guide link checks. All Cargo checks used `--locked`; the full DFS test
command retained `--config profile.dev.package.sinomo-dfs.opt-level=0`.
No deployment or distributed acceptance was performed for this checkpoint.

The focused CLI codec and history/conflict-command parser tests also passed
(`/tmp/sinomo-history-commit-cli-codec-test.log` and
`/tmp/sinomo-history-commit-cli-parser-test.log`).

### Preserve replication policy before retry-witness eviction

The new regression reproduced zero missing-copy jobs after evicting the original
retry receipt (`/tmp/sinomo-history-policy-eviction-repro.log`). For a retained
capture of that write's target generation, GC now plans an explicit
`RetainedRepair` owner before removing its last policy witness. The owner copies
the original desired replicas and W/N policy. Its journal entry precedes
compaction and eviction, so incremental replication and restart reproduce the
same state without deriving new semantics from old GC records.

Owner creation shares the GC mutation budget. With a budget of one, a pass can
publish the owner and leave its prerequisite-dependent removal for the next
pass. Reports count created and collected owners separately from user-intent
compaction. An older assigned codec holds the original witness without
advertising an immediately runnable GC candidate; it cannot append a new owner.
New history mutations and retention configuration now require PackedV4 before
publication, while historical reads remain compatible with PackedV3.

Expanded lifecycle coverage exposed an invalid owner-removal record: using
`PutIntent` without a payload made replay fail. Collection now uses a typed
`RemoveRetainedRepairOwner` mutation, including table rollback, status indexes,
service repair retirement, and migrated-family replay. It cannot remove an
ordinary write intent. The owner remains while retained metadata exists and
can be collected after that metadata disappears, without producing a user
retry receipt. Its identity and W/N plan are immutable during local writes and
batch replay.

The same lifecycle test reproduced fresh unpin requests being rejected after
GC because their transaction timestamp reused the original capture time.
History transactions now use stable request-admission time in clustered
execution, or the generated operation ID's timestamp for standalone execution.
Expired operations are not restamped with the current time. Legacy opaque IDs
keep conservative expiry; the user guide now describes ordered operation IDs.

Still open: captures after metadata-only generation changes must resolve their
original content durability plan, including before that source witness is
collected. Current GC backfill covers the intent's exact target generation.
Unattended process-crash queue recovery, actual metadata-primary handoff,
cancellation and W<N acceptance, and distributed process/disk verification also
remain required. These changes do not complete the feature goal.

Validation passed all 1,903 DFS library tests
(`/tmp/sinomo-history-policy-owner-full.log`). The final expansion of the
returning-peer lifecycle regression then passed with all 50 focused history
tests (`/tmp/sinomo-history-policy-owner-peer-tests.log`). After the original
receipt is evicted and the primary restarts, both missing peers receive the
retained content under the new owner, N reaches three, and neither peer gains
an alternate namespace object. Current content stays unchanged, expired pin
retries remain rejected, a fresh unpin succeeds, and subsequent GC removes the
version and its maintenance owner without creating a retry receipt.

All 21 base-library tests and all 16 architecture checks passed
(`/tmp/sinomo-history-policy-owner-base-tests.log` and
`/tmp/sinomo-history-policy-owner-boundaries.log`). Final strict all-target Clippy
passed for DFS, base, CLI, and client
(`/tmp/sinomo-history-policy-owner-clippy-final.log`), along with formatting and
diff checks. Cargo used `--locked`, with the command-local DFS unit-test
opt-level override. These changes are uncommitted and have no deployment or
distributed process/disk acceptance.

### Data durability across metadata generations

A chmod/capture regression reproduced the loss of W=1/N=3 work after overwrite,
GC, and restart (`/tmp/sinomo-history-lineage-repro.log`). Version records now
carry an optional `DfsContentDurability` plan independently of capture metadata
and user retry witnesses. Data writes populate it from their validated intent;
local-eventual batches populate the single-replica receipt contract. Normal
writes still leave `retained` unset. Metadata-only successors copy the plan
without hashing or copying file bytes.

The recorded source generation can outlive its operation evidence. Explicit
capture/re-pin can materialize a fresh repair owner from this plan alone.
GC preserves a still-incomplete original write while current metadata refers
to its content. A restart regression also found the coordinator's CAS cache
rewinding to the original data generation; eager, lazy, and journal-driven
intent restoration now retain the proven metadata successor as current.
Repair-owner creation defers while the original content-preservation write is
still active, preventing duplicate ownership during capture publication.

The new persisted field has its own PackedV5 contract and
`dfs.metadata.content-durability.v1` capability. History mutation admission
requires V5; V3/V4 retained reads remain available, and ordinary writes on older
shards omit the new field. Native SOBJ testing exposed heterogeneous integer/
object tuples in sparse columns. Typed row objects now encode both retained
descriptors and durability plans; native round trips and legacy JSON row input
are covered, along with malformed plans and downgrade rejection.

Cross-shard adoption initially dropped the plan
(`/tmp/sinomo-history-lineage-cross-shard-repro-v3.log`). The moved generation
and destination publication now carry it, and both endpoints require the
appropriate capability before transferring a family that contains it. The
regression uses admitted V5 members, the real fence/replay/activation handoff
protocol with exact member-ring rotation, and a subsequent cross-primary rename
and history capture. It does not bypass packed-shard activation with a raw KV
assignment. Local rename/GC coverage first reaches N and then removes the
original generation and retry evidence before capture; the stored plan still
provides the original W/N after restart.

Remaining: migrate/backfill legacy metadata-only generations whose plan is
absent; reconcile repair demand for policy captures as well as explicit pin
operations; recover unattended loss between committed roots and the local
repair queue; and verify retained repair through primary handoff, cancellation,
W<N, and distributed process/disk scenarios. The pending-repair inventory and
status projections also need review for metadata-successor and historical roots.
The V5 plan makes the obligation reconstructable, but does not by itself prove
those scheduling and recovery boundaries.

Final validation passed all 1,908 DFS library tests
(`/tmp/sinomo-history-lineage-full-final.log`), including native codec checks and
the new cross-shard and local-batch cases. The focused history run passed 54
tests (`/tmp/sinomo-history-lineage-final-focused.log`). Strict all-target Clippy
passed for DFS, API, node, CLI, and client
(`/tmp/sinomo-history-lineage-clippy-final.log`); app all-target compilation and
all 16 architecture checks passed (`/tmp/sinomo-history-lineage-app-final.log`
and `/tmp/sinomo-history-lineage-boundaries.log`). Formatting and diff checks
passed. These checks used locked dependencies and the command-local DFS
unit-test optimization override. The changes remain uncommitted; no deployment
or distributed process/disk acceptance occurred.

### Atomic capture ownership and rule-driven repair scheduling

A new regression reproduced a policy capture of a chmod generation with no
repair jobs (`/tmp/sinomo-history-demand-repro.log`). Captures that carry a
durability plan now create their `RetainedRepair` owner in the same metadata
materialization as the retained version. A full active or committed write for
that exact generation remains its owner instead, so content-preservation
publication does not race a duplicate maintenance executor. The derived owner
is part of the same journal meaning and table rollback; it is present in every
replayed prefix that exposes the capture.

Publication collects only affected outgoing identities, then activates their
owners after installing the new canonical head. Canonical overwrites, namespace
deletions, bulk writes, and explicit pins use the same reconciliation path.
The bulk snapshot-budget regression caught an unnecessary filesystem lookup;
the namespace path now reuses its already-read policy instead of weakening
that test (`/tmp/sinomo-history-demand-bulk-budget.log`).

The queue's persistent one-time migration marker was not evidence that a later
process stopped after persisting every repair delta. DFS startup/activation now
requests the existing authoritative reconciliation, which rebuilds full owned
intents and merges their work into the durable queue. Separate per-root counters
prevent generation reuse after a completed request, so a late completion from
an old activation cannot consume a newer recovery request. Queue-persistence
errors also request reconciliation immediately. Reads and ordinary idle wakes
do not request inventory scans.

The new rule test discards process-local state and the unpersisted queue delta,
reopens a lazy service, runs the same hydration used by authoritative recovery,
and persists the reconstructed history work without a client retry. It also
replays the journal one entry at a time and checks that capture and owner appear
together. Delete and bulk-replace variants verify that the current head remains
the deletion or replacement result.

Still required: legacy-plan migration/backfill; inspection/inventory treatment
of metadata-successor and historical roots; and process-kill, cancellation,
retained-work primary handoff, and distributed transport/disk acceptance. The
startup scheduling and lazy-reopen tests strengthen recovery evidence but do
not replace those integration cases.

Validation passed all 1,910 DFS library tests
(`/tmp/sinomo-history-demand-full-final.log`) and 56 focused history tests
(`/tmp/sinomo-history-demand-focused-v3.log`). The startup-generation test and
unchanged bulk snapshot-budget regression passed separately. Strict all-target
DFS/API/CLI/client Clippy passed (`/tmp/sinomo-history-demand-clippy-v2.log`), as
did all 16 architecture checks and app all-target compilation
(`/tmp/sinomo-history-demand-boundaries.log` and
`/tmp/sinomo-history-demand-app-check.log`). Formatting and diff checks passed.
Cargo used locked dependencies and the command-local DFS unit-test optimization
override. Changes remain uncommitted; no deployment or process-kill/distributed
acceptance is claimed.

### Repair-summary accounting and local commit validation

The repair-summary regression reproduced a zero backlog after a metadata-only
successor replaced an unfinished data-write generation. Both the incremental
projection and its scan reference now count that obligation and explicitly
retained historical obligations. Pinned and count-retained work remains live;
age-only work uses an incremental expiry index, allowing read-only status to
advance without scanning history or scheduling maintenance.

Version, pin, receipt, and cross-shard pin-transfer materialization update the
projection. Source eviction removes pins through the same counter updates.
Regression coverage verifies metadata successors, pin release, exact expiry,
rollback, snapshot rebuild, and receipt completion. The expiry index also
checks weighted replacements against an ordered reference and preserves cloned
rollback state.

The local commit checks passed all 1,913 DFS library tests
(`/tmp/sinomo-history-commit-dfs.log`), strict all-target Clippy for DFS, API,
CLI, and client (`/tmp/sinomo-history-commit-clippy.log`), all 16 architecture
checks (`/tmp/sinomo-history-commit-boundaries.log`), and formatting/whitespace
checks. Legacy-plan backfill, detailed operator repair-list alignment, and the
process-kill/distributed acceptance cases remain open. This commit records the
implemented durability, recovery, and summary work; it does not claim completion
of the feature goal or a deployment.
