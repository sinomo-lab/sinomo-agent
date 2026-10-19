# Durable Storage Migrations

`sinomo-storage::migration` supplies a reusable migration state machine and a
durable local job store. R1 owners use `sinomo_core::storage::migration`; R2 and
application owners use `sinomo_runtime::storage::migration`. The framework
contains no DFS, chat, session, or settings implementation dependencies.

Each storage owner supplies a `MigrationDriver` and retains responsibility for
decoding, authorization, concurrency, and publication. Register job and staging
paths in that owner's storage contribution through the component catalog. Start
and cancel the worker through the owner's existing lifecycle. The framework
does not create a second daemon scheduler or bypass a domain's transaction path.

## Contract

A `MigrationIdentity` binds the owner, migration algorithm, storage scope,
source and target versions, and immutable source generation. Change the
migration ID when changing an incompatible transformation or cursor encoding.
An existing job with a different identity fails before executing data effects.

The durable phases are:

```text
Copying -> Verifying -> Publishing -> Complete
```

A replaced source terminates its old job as `Superseded` before the owner
discards unpublished staging output. It cannot activate stale data. Authority
loss is a retryable error instead, allowing the new authorized executor to
continue the same source generation.

`MigrationJobStore::advance` executes one batch or publication step. Copy and
verification have record and byte limits and a cooperative deadline. Drivers
check cancellation and time between records; an already-entered blocking I/O or
decoder call cannot be forcibly interrupted. A record larger than the configured
byte budget rejects the batch instead of creating an unbounded allocation.
After mandatory batch setup, drivers may call `MigrationStep::after_preparation`
once to start the record-work deadline. This preserves cancellation and the
record/byte bounds while preventing slow checkpoint-file opens from consuming
every retry's budget before any record can be processed.

Drivers must make their effects idempotent using the job's `batch_id()` or an
equivalent generation-bound identity. Data becomes durable **before** progress
is checkpointed. A crash in between repeats the same batch; it must not duplicate
effects or overwrite newer foreground data. Progress errors never authorize
skipping uncommitted output. Cancellation stops further work while preserving
the admitted job and its durable cursor.

The local job store uses atomic, directory-synchronized SOBJ checkpoints with a
checksum, bounded cursors, and an OS file lock shared across processes. Process
death releases the executor lock. A corrupt checkpoint fails closed. Status
reads are observational and do not admit or resume work.

Owners with transactional or replicated storage implement `MigrationCheckpoint`
and call `advance_migration` using their existing executor/authority fence.
`compare_and_store` must atomically compare the complete predecessor before
committing progress. Driver effects still use the domain's ordinary authority
and idempotency rules. A local file lock is never cluster authority.

## Reader and Writer Integration

The driver is only the background mechanism. Each owner must also implement:

1. Readers that recognize supported old representations and decode them into
   the current domain model without requiring a whole-store rewrite.
2. Writers that emit the current representation after the owner's format
   admission boundary permits it.
3. A stable migration source: for example, an immutable checkpoint plus an
   active journal, or per-record revision comparisons that prevent migration
   from replacing a concurrent foreground update.
4. Idempotent publication that rechecks source and authority, then atomically
   changes the active generation. A retry must recognize prior publication.

Chat/session stores can use an immutable log cut and continue appending current
records. Cluster settings can use revision-checked updates and a progress record
committed through their existing cluster-KV authority. These are integration
patterns; the framework does not automatically rewrite those modules' data.
The independent settings fixture in the storage crate exercises both the local
job store and a transaction-backed checkpoint adapter.

## DFS Integration

Current DFS storage uses the local job store directly for bounded compaction of
its inline content-binding logs. The published catalog stays unchanged through
copy and verification, each private copy segment is at most one batch, a
superseded source never publishes, and existing readers keep their pinned
generation. Current metadata checkpoints use the shared checksummed stream
codec for bounded SOBJ records.

Retained older DFS formats are converted by `sinomo-dfs-migration`, the R1
component that alone decodes older snapshot, journal, content, and local-state
representations. Main and Embedded hosts register it as the DFS
storage-preparation provider before DFS storage opens; Shadow hosts do not
convert or publish. Each cold open of a metadata shard, the metadata-routing
representation, the content store, or local state asks the provider to advance
at most `DFS_STORAGE_PREPARATION_RECORD_BUDGET` (256) records. Unfinished work
returns `StoragePreparationPending`; the component's initial startup recovery
resumes it under the same data owner instead of failing the mount.

A contended metadata destination returns `Pending { processed_records: 0 }`
without waiting on its active preparation session. Physical aliases share one
stable slot. The current owner retains its source lock, validated cursor and
checkpoint; another caller cannot replace or advance them. After that turn
releases the session, callers resume through the same bounded preparation entry
and must still obtain actual `Ready` before opening storage. Cancellation of a
waiting caller does not cancel the owner or release source custody. A poisoned
session is discarded under the same slot before the source can be reacquired;
reconstruction still validates the last durable checkpoint.

Source replay checkpoints one bounded lifecycle batch at a time. Each record
keeps its original bytes, source point and chained stream digest. The private
event stream is synchronized before the combined source/event cursor is
published. An error discards the partial warm projection. Restart validates the
saved source and event cursors, discards only the unacknowledged private stream
suffix, and reconstructs the acknowledged prefix under the same record limit.
A synchronized event tail without a published cursor is not acknowledged work;
an empty stream header is not a completed conversion or publication seal.

Finalization returns to the same lifecycle after recording the verified candidate
source cut and again after recording its import proposal. The same slot retains
the physical source lock and the typed checkpoint across these yields. Each
continuation checks the exact retained checkpoint bytes, source inventory and
current owner before advancing. A completed checkpoint counts as one durable
record of progress; contention and dependencies that have not advanced report
zero. Neither checkpoint reports `Ready` or contains a reusable certificate.

The migration component retains at most one decoded native plan between calls.
Every reuse reads and compares the complete current file bytes against the bytes
that passed full plan validation. A different file body is parsed and validated
again; missing or truncated files fail normally. This retains immutable DATA
only: source custody, current assignment, native receipts, participant proof and
installed-image checks still run at their original boundaries. A new component
owner starts with no retained plan, and no parsed plan is an authority token.

Within that component, pure plan validation can reuse this same single entry
when every typed field matches. Opaque JSON numbers and original path spelling
are compared exactly, including distinctions that ordinary numeric or path
equality normalizes. This avoids repeatedly serializing an unchanged plan during
one bounded publication turn. Changed data earns full validation again. Reads
of retained files still check their entire current contents, and every native
scope, source, receipt, floor and publication check remains independent of this
data reuse.

During preparation and installation, a current candidate's complete native row
decoder supplies a private inspection value. The component retains at most one
candidate inspection and one decoded ancestry challenge alongside its existing
single decoded plan entry. Challenge reuse reads and compares all current file
bytes; its source custody, floors and authority are checked afresh by the caller.
Each subsequent candidate check reads
the entire physical stream again, including the header, every framed record,
record count, stream chain and terminal EOF, then compares the raw file hash.
Only identical bytes reuse the decoded semantic result. This value grants no
writer permission, survives no process restart, and does not replace source or
target fences. The same checks apply after the candidate's root is renamed.

Retained raw journal copies are checkpointed within the existing bounded source
turn. New copies stay in uniquely named private files until every file has been
synced. No-clobber installation then publishes their canonical names and syncs
the directories before a projection reader or the saved source cursor can use
them. A failure may leave a prefix of individually synced canonical copies, but
never an acknowledged batch. Restart independently replays the source/events,
compares exact bytes, and syncs reused names before acknowledging progress.
Unpublished abandoned files are not source evidence. Conflicting canonical or
pending copies remain errors, and the original source writer fence stays held.

Hidden native candidate frames retain their ordinary journal durability barrier.
A commit still requires its exact durable prepare frame. The separately written
journal head uses the existing derived-hint API; lost, stale or corrupt hints
are recovered from the actual durable tail rather than authorizing a sequence.

Source-version lookup hints are derived from the verified source projection.
Their publication consumes the existing preparation record budget, retaining a
cursor in the same fenced session between calls. Each hint is atomically
replaced using the normal hint writer. The source event ledger, progress,
captured cuts, proposals and installation records retain their durability
barriers; hint files never supply those guarantees or native authority.

A fresh owner first validates source custody and replays the durable event
checkpoint under the same record budget, then reconstructs the hints. No saved
hint cursor or hint file can replace this replay. Missing hints remain
unavailable until reconstructed; malformed or foreign hints still fail source
validation. Every lookup checks current membership, original source bytes and
row provenance. Losing these derived files across cancellation or restart cannot
make incomplete preparation Ready or change the native publication checks.

Cancellation between calls preserves this pending state. A process restart
reconstructs the source projection under the original record budget, verifies
the staged candidate and reuses the retained native plan and request identities.
Certification and installation remain one publication turn: they obtain actual
native receipts and full participant proof, recheck the source/image, retain the
target writer fence, and use the existing durable installation intent before
exchange. Interrupted installation reobserves native authority and exact bytes;
saved JSON alone cannot establish readiness. The candidate/proposal restart
tests stop the real disposable native owner at each checkpoint and require
eventual native readiness, unchanged source data and exact checkpoint replay.

The converter retains original source bytes and digests, synchronizes output
before progress, and discards only an uncheckpointed staging suffix on restart.
It stages a hidden current candidate. Installation binds the captured source
cut, current configuration, physical storage ownership, and exact KV
comparisons through native authority; standalone stores use explicit local
ownership evidence. Old reservations, jobs, and proofs remain conversion data
until current authority establishes their replacements; they never grant
publication authority.

The current DFS reader accepts only the current representation, and there is no
mixed-version writer path: an incompatible daemon must be upgraded before it
joins. [DFS metadata authority](../dfs-architecture.md#metadata-authority) owns
the current format and authority rules.

`sinomo dfs status --json` still carries a `storage_migrations` list of
`MigrationJob` records, but current DFS status leaves it empty; conversion
progress is not reported there.

## Verification

```sh
cargo test -p sinomo-storage --lib
cargo test -p sinomo-dfs --lib inline_compaction
cargo test -p sinomo-dfs-migration
cargo test -p sinomo-core --test workspace_ring_boundaries
```

The storage tests kill a separate executor process during copy and during
publication, then resume from durable progress. They cover checksum
corruption, truncation, cancellation, executor exclusion, source changes, and
transaction checkpoint conflicts. The DFS compaction tests cover reopen, lost
plan and job checkpoints, superseded sources, and reader generations. The
`sinomo-dfs-migration` tests cover interrupted conversion and resume for
metadata snapshots, journals with torn tails, content publication, and local
state.
