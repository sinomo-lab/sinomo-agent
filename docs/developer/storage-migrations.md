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
