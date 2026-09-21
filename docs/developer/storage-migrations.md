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
patterns; this change does not automatically rewrite those modules' data.
The independent settings fixture in the storage crate exercises both the local
job store and a transaction-backed checkpoint adapter.

## DFS Integration

DFS uses a job per sealed metadata checkpoint to convert its local materialized
snapshot to physical format V3. It preserves the existing metadata journal,
sequence numbers, exact prefix proof, object identities, version pins, repair
records, and content hashes. File content blobs are not copied or rewritten.
Each replica migrates its own physical projection; migration progress does not
become per-file cluster KV state or grant metadata-primary authority.

The checkpoint worker seals the active table-log prefix under the existing
metadata writer lock. New writes append to a new active suffix immediately.
Copy and verification run outside that lock against the immutable checkpoint.
The copy cursor identifies a table family and its last key; B-tree range seeks
avoid rescanning preceding records on every batch. All persisted record families
are included, including retention floors, pins, restore jobs, and retry receipts.

V3 snapshots are checksummed streams of bounded SOBJ records. A durable cursor
records the output boundary and hash-chain state. Resume removes only the
uncheckpointed staging tail. Verification resumes independently and checks each
record against the immutable source. A complete footer is required.

Publication briefly reacquires the metadata writer lock, compares the source
snapshot generation, and atomically replaces the snapshot file. Readers decode
through one open file handle, so a format change cannot mix two file generations.
The newer active journal suffix is replayed after the installed checkpoint.
The sealed log and older journal segments are retired only after publication,
using the existing handoff-proof and journal-retention rules.

Writer/recovery opens resume pending work, including legacy stores with only a
table log and no snapshot. Current-format logs retain normal compaction timing.
Read-only inspection does not start migration. `sinomo dfs status --json` exposes
`storage_migrations`, with phase,
record/byte counts, source identity, and the last error. Counts are metadata
records, not a percentage of file contents. Superseded source generations fail
publication rather than overwrite newer state.

Physical snapshot V1/V2 readers remain available. New table batches and snapshots
use the current local representation. Namespace-prepare V3 remains a separate
distributed capability gate. Interim history V4/V5 journal bodies remain
decodable, but new binaries do not author those prepares; their shard contracts
advance to V3 through the normal fenced handoff after assigned peers support it.
The new binary's ability to read old data does not make old binaries able to
read newly written V3 storage; downgrades require a compatible recovery plan.

The existing metadata backend still materializes its shard tables on a cold
open. The migration adds bounded output/verification batches and durable resume;
it does not replace that resident-table architecture. Reloading an old immutable
source can therefore read its metadata again after restart, while already
checkpointed conversion output is reused. Initial legacy proof bootstrap also
retains its existing authoring fence and initialization costs, but its conversion
now uses durable migration progress. No bound on million-file cold-open
latency or full-filesystem performance is implied by the batch limit.

## Verification

```sh
cargo test -p sinomo-storage --lib
cargo test -p sinomo-dfs --lib
cargo test -p sinomo-core --test workspace_ring_boundaries
```

The storage tests kill a separate executor process after output durability and
after publication, then resume from durable progress. They cover checksum
corruption, truncation, cancellation, executor exclusion, source changes, and
transaction checkpoint conflicts. DFS tests cover V1/V2 reads, restart-driven
resume, retained metadata, concurrent foreground updates and unpinning, staged
corruption, and publication before its completion checkpoint.

The million-record workload is a standalone benchmark, excluded from development
tests. It uses a fresh output directory and retains a machine-readable result:

```sh
SINOMO_MIGRATION_SCALE_ROOT="$PWD/target/migration-scale/data" \
  cargo bench -p sinomo-dfs --features migration-bench --bench storage_migration
```

It defaults to one million object metadata records. Set
`SINOMO_MIGRATION_SCALE_RECORDS=1000` for a benchmark smoke run. Omitting
`SINOMO_MIGRATION_SCALE_ROOT` creates a unique directory under
`target/storage-migration-bench/`. It measures source reload,
conversion, verification, batch bounds, and current-format reload; it is not a
full mounted-filesystem or content-throughput benchmark. Keep measurements and
process/disk verification artifacts under `target/`, outside the documentation.
