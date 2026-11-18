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
representations. Its component-scoped owner runs independently of ordinary DFS
readiness. Main and Embedded hosts register that owner before DFS storage opens;
Shadow hosts do not convert or publish.

Background discovery visits every managed storage unit, including cold data.
An ordinary DFS access to an unavailable unit schedules that same owner's job
with demand priority and returns `StoragePreparationPending`. Content conversion
carries one absolute work deadline through enrollment, setup, inspection and
payload work; it does not restart that deadline with the framework's optional
`after_preparation` API. Qualified warm state avoids repeating unchanged setup
barriers, while restart performs the full cold qualification. The callback does
not perform conversion while a DFS caller holds its storage slot. Background
and demand work share the original converter checkpoint and source custody;
they do not maintain separate conversion cursors. Readiness notifications only
wake callers to recheck their exact unit and generation. A caller's cancellation
does not cancel the component owner or release its retained source.

The current DFS runtime decodes only current representations. A current content
root can expose imported immutable versions while its inventory remains pending.
Already available versions remain readable; unknown versions require the owner
to establish their presence or absence. Imports preserve newer current bindings
and tombstones. Garbage collection and reservation reconciliation require
completed inventory in addition to their existing native catalog and retention
proofs. Inventory completion alone never authorizes deletion.

Metadata generations bind their exact filesystem, shard, and native origin.
Each metadata shard has a storage record naming the generation it serves and
the evidence that activated it: its native genesis, or a conversion attempt
with that attempt's activation digest. A shard opens only when its local
generation header matches an Active record. A Converting record keeps it
closed, and a shard with no record is old storage that the migration owner
must convert before it opens. A private staged generation cannot serve
ordinary operations merely because a provider reports readiness. A shard born
current records its Active record together with its assignment; its own
committed genesis certifies that activation, so compacting the genesis prepare
body must retain the exact genesis evidence. A retired pre-conversion root
cannot qualify by adopting a newer header.

Each shard converts on its own, from its primary's own copy. The primary
captures that copy and records the attempt Converting before it reads what the
other shards contribute to its counts. It then builds and seals the image,
installs it under the shard's install rule (the write set of a switched shard,
otherwise the primary and a majority of the configured members) and activates
it with one transaction on the shard's own keys. Members then activate their
staged copies. A serving change supersedes an attempt, and the shard's next primary
starts a new one from its own copy. When a closed shard's primary is lost, a
member converts from its own copy in its place, and that activation also
makes it primary at the next epoch. Conversion never writes the filesystem
configuration or advances its catalog generation, which guards path
assignment only. Only the converting shard is closed: a node's closed copy
never blocks its other shards.

A contended destination remains pending without waiting on its active
preparation session. Physical aliases share one stable slot. The owner retains
its source lock, validated cursor and checkpoint; another caller cannot replace
them. Poisoned warm state is discarded under that same slot, and reconstruction
validates the durable checkpoint before resuming. Missing, corrupt, or uncertain
original work blocks its unit rather than being counted as completed coverage.

Source replay checkpoints one bounded lifecycle batch at a time. Each record
keeps its original bytes, source point and chained stream digest. The private
event stream is synchronized before the combined source/event cursor is
published. An error discards the partial warm projection. Restart validates the
saved source and event cursors, discards only the unacknowledged private stream
suffix, and reconstructs the acknowledged prefix under the same record limit.
A synchronized event tail without a published cursor is not acknowledged work;
an empty stream header is not a completed conversion or publication seal.

Once the replay covers the whole source and its journal tail, the capture
records the facts of the copy it captured: source identity, snapshot, journal
head, filesystem, assignments and any original handoff reservation. A later
turn or a restart must find exactly those facts, or the capture fails. The
captured unit then stays in the same slot, under the same physical source lock,
for the shard's conversion to build its image from. A completed capture does
not report `Ready` and grants no authority; the shard's storage record and its
own transactions decide activation.

Every check of a staged image reads the entire physical stream again, including
the header, every framed record, record count, stream chain and terminal EOF,
then compares the raw file hash. A member keeps the floors and captures it
recorded with an attempt's ancestry challenge, and a retry that finds them
changed fails. Nothing decoded from an image grants writer permission, survives
a process restart, or replaces source or target fences.

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

Cancellation between calls preserves this pending state. A process restart
reconstructs the source projection under the original record budget and resumes
the attempt from its files. Each transaction request is saved before it is
submitted and resubmitted byte for byte, so a lost outcome settles under the
same request identity. Installation obtains actual native receipts from the
members the shard's install rule names, rechecks the source and image, and
keeps the shard closed under its Converting record until activation. An
interrupted installation reobserves native authority and exact bytes; saved
JSON alone cannot establish readiness.

The converter retains original source bytes and digests, synchronizes output
before progress, and discards only an uncheckpointed staging suffix on restart.
Its hidden canonical candidate contains current rows and proven compact terminal
outcomes. No original preparation crosses into it: the converted journal starts
at the conversion's own base and holds no frame to recover one from. A V7
prepare whose outcome the copy does not record, a count-neutral metadata update
included, is the shard's own crash-left work. Like unsupported namespace,
parent, count, content and completion work, it stops that shard's conversion
with a typed reason for operator repair, and the shard stays closed.
Original bytes and historical journal frames remain in the source archive;
conversion never relabels an old protocol body or invents a terminal outcome.
Installation binds the captured source cut, the attempt's serving record,
physical storage ownership, and exact comparisons of the shard's own keys
through native authority. The migration owner must recheck the original source
identities before publication. Old reservations,
jobs, and proofs remain conversion data until current authority establishes their
replacements; they never grant publication authority.

The current DFS reader accepts only the current representation, and there is no
mixed-version writer path: an incompatible daemon must be upgraded before it
joins. [DFS metadata authority](../dfs-architecture.md#metadata-authority) owns
the current format and authority rules.

`sinomo dfs status --json` lists each metadata shard's storage conversion in
`storage_migrations`, from the shard's storage record: `old` when the shard has
no record, `converting` with its attempt, or `active`, with the attempt that
activated it when the shard was converted. A takeover attempt also reports its
round. On a node that holds a closed shard's copy, a conversion stopped for
operator repair reports `stopped` with its attempt and reason.

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
