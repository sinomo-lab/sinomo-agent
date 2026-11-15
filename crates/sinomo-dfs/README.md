# sinomo-dfs

`sinomo-dfs` is Sinomo's local-first distributed filesystem layer. It provides the object, metadata, content, replication, repair, and POSIX-like path APIs used to expose SIFS, the cluster root filesystem.

The crate is not a replacement for the host filesystem, Git, or the cluster KV. It is a policy-aware workspace layer: file bytes stay in node-local content stores, high-cardinality filesystem metadata stays in DFS metadata backends, and the cluster KV is used only for compact DFS control-plane configuration.

The canonical hash of zero-length content is intrinsic: readers reconstruct it without a physical blob file, and namespace creation does not publish a redundant content-store reservation for it. The namespace metadata commit remains durable before an empty file creation is acknowledged.

See also:

- `docs/dfs-architecture.md` for the full distributed design.
- `docs/sifs.md` for the SIFS cluster root layout.
- `docs/storage-architecture.md` for data-directory ownership.

## Architecture

The DFS has four planes.

### Namespace and Object Metadata

DFS metadata records the stable identity and visible state of filesystem objects:

- filesystem ID,
- object ID,
- path,
- object kind,
- owner node,
- current version,
- current content hash,
- current replica locations,
- policy,
- write intents and receipts,
- durable namespace-request admission and terminal results.

Object identity is stable across path moves. Path operations update namespace metadata, not content identity.

The default filesystem is SIFS. From this crate's perspective SIFS is ordinary DFS metadata with a predefined layout:

```text
/
/sinomo
/sinomo/nodes
/sinomo/nodes/<node_id>
/sinomo/nodes/<node_id>/files
/users
/services
```

`/sinomo/nodes/<node_id>/files` is local-owned. `/users` and `/services` are distributed DFS subtrees.

### Content Store

`LocalContentStore` stores bytes under the local data directory as immutable content-addressed blobs plus object-version manifests.

Object versions are protected by hash. Rewriting the same `(object_id, version)` with different bytes is rejected. Storage writes use unique temporary files, atomic rename, and fsync of the file and parent directory.

Object versions can be represented as manifests over extents. Full-object writes remain the compact single-blob fast path, while write-at patches publish extent manifests so random writes and remote patch replication do not rewrite the full base object. Local APIs expose bounded range reads and range streams for serving large blobs without materializing the whole object in memory.

### Metadata Backend

`DfsMetadataBackend` is the durable metadata interface. The current `LocalSobjMetadataBackend` stores materialized shard state as a structured table snapshot under `records/tables/snapshot.sobj` plus a packed tail log under `records/tables/materialized.slog`; the synchronized shard journal remains the replay and replication source of truth. Tail-log appends are rebuildable cache writes and are not synchronized per mutation: reopen truncates a torn tail, rewinds any materialization cursor ahead of the surviving table head, and replays the durable journal. Snapshot compaction is synchronized before replacing that cache generation. Local materialization cursors are checkpointed periodically instead of after every mutation, and namespace transactions can publish object records and their visible version records in one replayable table batch. This avoids creating one host file per object, path, dentry, receipt, or version while reducing per-file metadata write amplification for large namespace imports and first-visible-version commits.

- filesystem records,
- object and path records,
- version records,
- write-intent records,
- replica receipts,
- namespace transactions,
- namespace-request admission records,
- immutable prepared operations and exact digest commits.

Every journal mutation is either a complete `PrepareOperationV7` payload or its exact `CommitPreparedOperationV7` digest. Namespace, content visibility, receipt staging, durable jobs and GC effects use this one family. An opaque `DfsPublicationGuard` retains normal runtime ownership and authorization through commit. Reopen and replica replay use the same projection as publication.

Mutating namespace RPCs first persist a request record keyed by authenticated
requester and request id with the semantic operation, stable operation id, and
exact authority shard. A request-owned hidden prepare commits the namespace
transaction and canonical terminal result in one table batch. Startup and
reactive maintenance resume admissions interrupted before a prepare, while
prepared transactions and durable namespace jobs follow their existing
recovery paths. A completed record replays its result across restart, reuse
with a different semantic operation is rejected, and `Pending` is an internal
automatically recovering state rather than an operator action.

Metadata journal replication is bounded by both entry count and encoded batch size. Oversized single entries still advance by themselves so a large namespace operation cannot stall a follower cursor forever. Daemon maintenance drains multiple bounded batches per publish call, with a cap that keeps each maintenance pass finite while avoiding one-batch-per-interval convergence for large metadata backlogs.

The table tail log is compacted into a streamed snapshot using storage schema
9, bounding reopen cost by the latest checkpoint and recent writes. Its storage
version is independent of the V7 operation family. `sinomo-dfs` accepts only
current storage and wire formats.

`sinomo-dfs-migration` owns incremental, on-demand conversion of retained storage. The Main or Embedded component supplies its cold-open provider; original source digests, archived bytes, restart progress and native installation authority remain explicit. Incompatible peer versions must be upgraded.

Operator status includes per-shard journal positions, inbound replay cursors, and configured replica cursor lag so cluster checks can distinguish object-level repair work from metadata replication backlog.

Replication carries the original current journal frames in one bounded carrier. Their contiguous scope and prefix are validated before ingestion; sender-side re-encoding cannot substitute for the durable bytes.

### Scale Benchmarks

The crate includes a JSONL scale benchmark for DFS namespace, metadata replication, local content, and network content paths:

```sh
cargo bench -p sinomo-dfs --bench scale -- --scenario all --preset smoke
```

The default `smoke` preset keeps local validation cheap. Use the production preset for the baseline targets that matter before DFS scaling changes: 1M namespace objects, full and paginated directory listing, create throughput, reopen hydration, metadata replication catch-up, and multi-GB local/network blob streaming.

```sh
cargo bench -p sinomo-dfs --bench scale -- \
  --scenario all \
  --preset production \
  --root /path/to/fast/sinomo-dfs-scale-baseline \
  --keep
```

The benchmark prints one JSON object per measured step. Byte-sized flags such as `--blob-bytes`, `--blob-range-bytes`, and `--network-blob-bytes` accept suffixes like `MiB` and `GiB`, so expensive one-off runs can be narrowed without changing source code.
Namespace-load and metadata-replication setup use grouped metadata commits by default; tune `--metadata-batch-size` when comparing write-amplification changes.

To isolate the networked DFS algorithm from physical-disk and container-network overhead, run the `InProcessNetwork` blob scenario with every content and metadata store rooted on tmpfs. This example measures a 128 MiB write with desired replica count N=5 and write quorum W=1, then removes its generated data even if the benchmark fails:

```sh
(
  bench_root="$(mktemp -d /dev/shm/sinomo-dfs-network-blob.XXXXXX)"
  trap 'rm -rf "$bench_root"' EXIT
  CARGO_INCREMENTAL=0 cargo bench -p sinomo-dfs --bench scale -- \
    --scenario network-blob \
    --preset smoke \
    --root "$bench_root" \
    --network-blob-bytes 128MiB \
    --network-blob-desired-replicas 5 \
    --network-blob-write-quorum 1
)
```

In the JSONL output, `desired_replicas` is N and `write_quorum` is W. `network_blob_w_visible_commit.time_to_w_ms` starts immediately before the replica workers are released and the POSIX write is invoked, then stops when that call returns with the version visible and exactly W durable receipts. It is marked as an upper bound because the harness observes the return boundary rather than the internal instant at which W was crossed. `network_blob_all_desired_committed.time_to_all_replica_commits_ms` uses the same start and stops at the latest replica's commit timestamp. It excludes the later owner-side response drain, metrics collection, and worker finalization. Logical throughput counts the object bytes once, while effective quorum/replicated throughput counts the durable bytes represented by W or N copies.

`effective_cores` is process CPU time divided by elapsed wall time over the corresponding boundary. It includes the owner, in-process replica worker threads, and small harness overhead; it can exceed one and is not a percentage of all host CPUs. The reported owner/replica idle-loop counters include a 1 ms sleep used only by this benchmark driver when an `InProcessNetwork` queue is empty. That polling is outside the DFS service hot path and must not be interpreted as an internal DFS wait.

### Networked DFS Service

`NetworkedDfsService<T>` is the distributed object service over a `sinomo-network` transport. It combines:

- local metadata cache,
- local content store,
- optional control-plane KV client,
- optional durable metadata backend,
- distributed write coordinator,
- data-plane DFS network messages.

Distributed writes are coordinated by the responsible metadata-shard primary. A node that receives a write for a shard it does not currently primary routes the request to that primary instead of creating a local intent. The primary creates the persisted intent, stages content to desired replicas, records durable receipts, commits the visible version after the policy threshold, and keeps repair jobs until the desired replica set converges.

Ordinary writes do not store per-file state in cluster KV or perform an unconditional Raft round. Before visible publication, the primary journals a hidden, operation-bound prepare in DFS metadata: the pending intent and exact `W` receipt set for content writes, plus the namespace transaction for first publication or namespace-only mutations. Each required member must durably apply and verify that exact prepare before acknowledging it: every configured member of a shard on the all-member rule, or the write set and pending adds of a shard on the write-set protocol (below). Missing acknowledgements leave the prepare durable and the outcome unknown; there is no cluster-quorum fallback. Cluster KV therefore retains only DFS configuration; content durability remains governed independently by `W`.

The acknowledgement wait has no DFS deadline. It ends when every required member has acknowledged, at the caller's operation deadline if the caller set one, on cancellation, when every attempt to a required member has ended with a network delivery failure, on a definite rejection, or when the retained shard record changes epoch. Complete evidence is used even when the caller's deadline has passed. A required member that is behind by more than one bounded batch, or rejects the prepare, is caught up through the ordinary durability barrier: within the caller's deadline, or, without one, for as long as each catch-up slice advances the member's acknowledged cursor. A target reached only over the routed path gets its empty confirmation re-sent on a timer that backs off to the network's liveness interval; the timer re-sends and never ends the wait. The durability barrier re-sends the same way: a member whose attempt did not take is sent to again after an interval that starts at 250 ms and doubles to the network's liveness interval, and once the fallback grace has passed and the network reports every member the barrier still waits for definitely unreachable, the barrier ends rather than waiting out its deadline. A caller's deadline does not cross the wire, so a served request's wait is bounded only by these events.

Shard membership changes retain the current primary and do not immediately activate a replacement authority. The quorum side first records a bounded pending handoff; this is not itself a write fence because the isolated retiring primary may not have observed it. After reconnecting, the retiring primary takes its publication guard, installs a durable local backend freeze, and acknowledges its exact journal head and fingerprint. That exact prefix must then reach and be acknowledged by the union of the retiring and desired assignments before the desired assignment activates. The local marker then advances to the activated epoch instead of disappearing: only the backend instance explicitly rebound after observing that exact control-plane generation may resume authoritative mutations, while independently opened stale instances remain fenced. Pending markers reject authoritative metadata mutations, and both pending and activated markers still permit replay and handoff transfer. This carries writes completed after the pending decision without putting healthy DFS operations through Raft.

#### Unknown commit outcomes

Once a prepare is durable, the outcome of any failed commit attempt is decided from the durable record, not from the error: a `Committed` record is the success it already is, an `Aborted` or absent record keeps the original error, and a `Prepared` record is `DfsError::MetadataCommitOutcomeUnknown { file_system_id, shard_id, epoch, operation_id, cause, current_primary_node_id }`. Typed kernel rejections (precondition failure, existing path, non-empty directory, stale directory version) stay definite. `cause` is a `DfsCommitOutcomeUnknownCause`: `acknowledgements-missing { required, acknowledged, missing }`, `below-write-set-floor { write_set, floor }`, `replication-below-intent { accepted, intent_w }`, `record-changed { epoch }`, `deadline`, `cancelled` or `interrupted { detail }`.

An unknown outcome is neither success nor failure. The caller retries the same operation id; the primary answers that retry from the durable prepare and commits it at most once. Its forms on each surface:

| Surface | Form |
|---|---|
| Routed write reply | `DfsDistributedWriteResult::CommitOutcomeUnknown { intent_id, file_system_id, shard_id, epoch, operation_id, cause, current_primary_node_id }`; the requester keeps the compact pending write instead of abandoning it. A commit attempted when a completing content receipt arrives answers a pending served write the same way; the receipt stays in the durable prepare that carries it |
| Routed namespace reply | `Rejected` carrying `DfsNamespacePosixError::CommitOutcomeUnknown { file_system_id, shard_id, epoch, operation_id, cause, current_primary_node_id }`; the request record stays `Pending`, and admission of a request whose prepare awaits recovery answers the same way. An admission whose own request-row commit is left unknown answers the same way; the same-id retry answers from that row once it commits |
| Routed namespace timeout after dispatch | `NetworkDelivery { PossiblyApplied }`; the stable-id wrapper retries the same id, and the per-attempt-id wrapper returns instead of retrying under a new id |
| FUSE / NFS | `EAGAIN` / `NFS3ERR_JUKEBOX`, as for a pending write |
| Browser conditional replace | `DaemonDfsBrowserReplaceFileIfVersionOutcome::Unknown { detail }`, also used for a pending write and a possibly applied dispatch |
| POSIX write that committed but whose post-commit stat fails | `NetworkDelivery { PossiblyApplied }`, never `DefinitelyNotSent`: the change applied, so the caller retries the same operation id rather than writing under a new one |
| Same-shard namespace jobs and prepared-change recovery | the job stays `Running`; a recovery pass attempts every other body and reports itself incomplete so it runs again; an unknown caused by cancellation stops the pass, and `DfsError::is_cancellation` treats it as the cancellation it is |

#### Shard write sets

A shard on the write-set protocol keeps its configuration at
`metadata-shards/<shard>` and adds `metadata-write-sets/<shard>` under the same
filesystem prefix. The write-set record holds the write set, its generation,
the epoch start as an exact prefix proof, the epoch at which the shard switched,
the substitutes recruited for unavailable members, the raised N and the change
that produced it. It names the configuration's epoch and primary; a record that does not
match them, or cannot be decoded, grants nothing and the shard keeps the
all-member rule. The shard configuration's format is unchanged, the shard scan
decodes only `metadata-shards/` keys, and a write-set change is an authority
change that does not reopen metadata stores. A change of a switched shard that
keeps the shard's routing identity and a node's placement (the node holds a
copy before and after, or neither) rebinds that node's resident store to the
new configuration in place, as primary or follower; a node that gains or loses
a copy opens or deletes it, and an unswitched shard reopens on any change. A
freeze marker the record has moved past (activated below its epoch, or pending
for a handoff that can no longer complete) is removed on rebind and when the
store ingests from a later epoch.

Every switch, shrink, add, recruit, N raise, removal and takeover swap is one
guarded transaction built from one exact-key-set read. It compares the exact bytes of
both keys, the pending handoff, the filesystem configuration when its guard
uses W or N, and the member and capability-policy records of each member it
admits. Each change advances the generation, so a transaction built from an
earlier record can never apply. Transactions carry a stable request id: a
takeover candidate settles an unknown swap by resubmitting the identical
request, which returns the committed reply or commits once, and the resulting
record names that request. A cooperative handoff compares the write-set key;
on a switched shard it admits only a primary rotation, and its completion moves
the write set to the new primary with the frozen prefix as epoch start. Recruit
and removal change the replica list within an epoch, so a lane rebinds a
completed handoff's activated marker on a switched shard when the proof's
successor matches the current assignment in everything but its replica list.

The W floor and N of a shard are the highest content W and N among the rules
that can apply to its paths (`metadata_shard_replication_bounds`). A commit also
needs at least its own intent's W (`metadata_commit_write_set_floor`), because
object and per-write policies are not derivable from configuration. The intent
is the one the commit makes visible: its terminal intent, or else the intent it
prepares; metadata-only publications carry none. A switched shard's effective N
(`metadata_shard_effective_n`) is the largest of its rules' N, its W floor and
the record's `raised_n`; the record accepts content up to that W
(`accepted_w`). A commit whose intent W is above it collects acknowledgements,
reports `replication-below-intent` and asks the keeper to raise N
(`build_metadata_write_set_raise_n`), which only grows it.

A commit on a switched shard needs acknowledgements from the write set at the
retained generation plus every durable pending add the record has not moved
past. A pending add is dropped from that set only when the retained record is
at a later epoch or a later generation than the add's read, never merely when
it differs, because the retained view can lag that read. The prepare goes only
to those members; others receive the journal through ordinary replication. A
write set smaller than the commit floor still collects acknowledgements, so the
change is durable on the live write set, and then reports
`below-write-set-floor`. The authorization (`DfsCanonicalAuthorization::WriteSet`)
names the generation it was evaluated at. Under the shard writer lock the
publication fence evaluates the requirement again from the retained record and
the member-state file: another epoch leaves the outcome unknown, a moved
generation or replica list that the acknowledgements still cover commits under
the new generation, and anything else runs another round. The fence also drops
superseded pending adds. An acknowledgement carrying `member_view` counts only
when the view names the probe's epoch and exact prefix; the generation is not
compared, and the primary keeps each member's latest view, from any reply, as
add evidence. A catch-up probe can name a record generation
(`write_set_generation`): the member answers it only once its own published
view of the record reaches that generation, waiting on the publication rather
than a timer, and a routed reply that cannot wait rejects with
`write-set-view-behind`. The prepared body seals no member set: retries and
prepared-change recovery are evaluated against the record current when they
run.

On a switched shard a member answers a source that names another epoch than
its own record with a typed code and its record view, `member_record { epoch,
generation, primary_node_id }`: `source-epoch-superseded` for an earlier epoch,
`write-set-view-behind` for a later one, `source-epoch-fenced` when its
acceptance fence refuses the epoch, and `epoch-alignment-required` when it has
not aligned with the epoch start (below). Older nodes read unknown codes as
`other`. A commit that a required member refuses for its epoch ends unknown as
`record-changed` without a catch-up, naming that member's primary when it is
another node, and a fenced member is posted to the keeper, which may shrink it.
A member that must be aligned first counts as a missing acknowledgement and is
posted to the keeper, which aligns it. The publisher suppresses replication to
a member that superseded its epoch; a fenced member is not suppressed, because
a takeover that does not apply releases the fence at the same epoch.

On a switched shard, file and directory sync certify the write set and pending
adds at the retained generation instead of a configured majority: each member is
brought to the synced prefix within the caller's sync timeout. Below the W floor
nothing is certified, and file and directory sync both return a pending write
(`EAGAIN` / `NFS3ERR_JUKEBOX`), never a definite failure.

A member admits the write-set capability only when its own member record
advertises it and its capability policy, if present, allows it. Main daemons
do not advertise it until they implement the whole member-side contract.

#### Keeping the write set

On a Main daemon one keeper task (`run_dfs_write_set_keeping`) changes the
record; nothing else does. As a shard's primary it keeps the write set; as a
write-set member other than the primary it takes over a lost primary
([Takeover](#takeover)). It acts only on events: configuration publications
(the record, cluster members, service providers and rules), the network layer's
per-peer reachability verdicts, member views arriving with replies, demands
left by commits and requesters, corroboration requests and its own
completions. An idle shard whose record already matches its plan sends
nothing, and a verdict the network is still verifying starts nothing. The
keeper holds a network reachability subscription only while its node is
primary of a switched shard or a write-set member that may take one over. For
each shard it is primary of it plans one step
from the record and those facts (`plan_metadata_write_set_keeping`):

- switch a shard whose configured members all advertise the protocol, then
  reduce it to the N members it should keep, ranked reachable first, then
  original before substitute before new candidate, then in the write set, then
  by a per-shard hash. The switch evidence includes one pass over every version
  record the primary's own shard store holds (`version_records_on_shard_page`),
  current or retained, visible or not: the record's initial `raised_n` is the
  highest `content_durability.policy.min_commit_replicas` among them, and a
  version with content but no recorded policy keeps every configured member
  (`raised_n` at least their number; the switch event's cause says so). The
  pass and the switch submission hold the shard's mutation admission, so no
  version commits in between;
- shrink a member with a settled no-path verdict, one the cluster no longer
  authorizes, or one that refused the primary's epoch for a takeover candidate,
  never below the W floor;
- re-add a returning member: on an epoch that started at a takeover or handoff
  it is first aligned with the epoch start; it is then caught up through the
  ordinary durability barrier, confirms the head under the record's generation,
  and its pending add is recorded under the shard writer lock before the add is
  submitted, all while the keeper holds the shard's mutation admission, the
  admission a commit holds, so no change is appended between. Work on every
  other shard is admitted beside it, the member answering another primary's
  probe included, so two primaries re-adding each other's members do not wait
  on each other. The join yields to nothing: a join that gave way to commits
  would not converge under sustained writes. Because the keeper waits on the
  member while it holds that admission, its write-set probes preempt a
  maintenance pass on the member, as a handoff probe does: a member pass that
  is itself waiting on this primary yields instead of waiting on the keeper. A
  catch-up probe or join that fails is retried once an input it saw moves, and
  on the same re-send timer while none does, so a momentary failure in a quiet
  cluster does not leave the member out;
- recruit an eligible member, preferring non-mobile ones, when the write set is
  below the effective N, or one replacement beyond N when every configured member is in
  the write set (`DfsMetadataShardRecruit`);
- raise N for a commit demand;
- remove surplus members beyond the desired set, never below it, and members
  the cluster no longer authorizes that are outside the write set.

A commit that could not reach a required member asks the network to verify
that peer; once the verdict settles, either the member leaves or the prepared
change is recovered. A probe answered with a refusal of the epoch does not count
as an answer, so a fenced member is not joined. Each applied change publishes a typed
`dfs.metadata.write_set.*` event (`switched`, `shrunk`, `add_submitted`,
`added`, `recruited`, `retired`, `n_raised`) naming the change id, generation,
members and cause, and a shrink or N raise wakes prepared-change recovery. An
unknown transaction outcome is settled from the record before the keeper acts
again. Members are eligible when the cluster authorizes them, they provide
`dfs.metadata.replicate` and advertise the prepared-operation and write-set
capabilities; they need not be KV voters. Until a cluster publication carries the member and service scopes
the keeper judges no member.

Each member's durable state for a write-set shard is
`coordination/write-set-member-state.sobj` in the shard directory: the
acceptance fence, a takeover candidate's promise and a primary's pending adds.
Snapshot install never archives `coordination/`; leaving the shard's placement
deletes it with the copy. Under the shard writer lock, journal ingest, snapshot
install and both acknowledgement-floor writers refuse a source epoch below the
fence. A promise captures the materialized journal head as epoch start and
raises the fence to the next epoch in one writer-lock section. A pending add is
written in one section with the evidence check and stays required until the
record moves past the add's read.

On a write-set shard an acknowledgement floor records its authority epoch and
binds only at that epoch; a floor without one belongs to the switch epoch.
Floors merge by epoch, then sequence. An epoch-tagged authority floor may name
sequence zero, an epoch that started on an empty journal. A snapshot from a
later epoch replaces an earlier epoch's floors before it replaces the state
they describe, and writes its epoch's authority floor even at sequence zero. A
member's durable acknowledgement carries `member_view`: its epoch, write-set
generation and exact durable prefix. Other shards write floors and replies in
their existing encoding.

#### Takeover

A write-set member other than the primary starts an attempt when its own
verdict for the primary settles to no path, or the cluster no longer
authorizes the primary (which then has no verdict at all). A routed namespace
request that cannot be delivered to the primary asks the network to verify it
first. An attempt that ended on its own evidence (a veto, a newer record or a
failed promise) starts again only when the record, the publication or this
node's verdicts move; a failed confirm read is re-sent instead (below). The
decisions are pure functions
(`plan_metadata_takeover`, `decide_metadata_takeover_round`,
`decide_metadata_takeover_hand_off`); the candidate role in the keeper runs
them.

- **Corroborate.** The candidate asks every other write-set member it does not
  judge lost on `/dfs/metadata/takeover-corroboration`
  (`DfsMetadataTakeoverCorroborationRequest`, format 1, exact member identity),
  served without a shard lock, lane or admission. The member checks the request
  against its own record (`abstain`: `not-a-member`, `newer-record`,
  `malformed`), asks the network to verify the primary unless its verdict is
  already a settled no path, waits for that episode to end, and answers
  `reaches`, `lost` or `abstain { no-evidence }` with its record view and
  durable journal head. A call ends on the reply, a network delivery failure or
  this node's no-path verdict for that member; nothing re-sends it on a timer.
  Any `reaches` vetoes the attempt and a `newer-record` ends it. Otherwise the
  candidate and every `lost` member are ranked by journal head, then non-mobile
  first, then node id. A member ranked first above the candidate gets a
  `hand-off { ranking }` request and runs the attempt itself when it still
  ranks first with its own head; it declines with `prefer` otherwise, and a
  member already promising answers `followed`. A member that cannot run the
  attempt declines with `no-quorum` when its confirm read fails, or with
  `unavailable` when its keeper cannot evaluate the shard. On such a decline
  the candidate reads the record itself: if that read fails too it re-sends
  its read as a failed confirm read is re-sent (below), and once a read
  succeeds a quorum is serving, so it asks the same member once more.
  On any other decline, or a second availability decline, the candidate ranks
  again without that member. A write set of two asks nobody.
- **Confirm, promise, swap.** The candidate reads the record with one
  authoritative read of the shard keys and its configured members; a failed read
  promises nothing, so a node outside the KV quorum promises nothing. A failed
  read can be momentary while the KV voters elect, and no other change may
  arrive to prompt another attempt, so it is re-sent every 250 ms, doubling to
  the network timeout, while the primary is still judged lost (a settled no-path
  verdict, or its departure) and the record is unchanged. Any record or verdict
  change ends it, and the re-sent read's own outcome decides. The
  record must still name the judged primary at the attempt's epoch with this
  node in the write set. The promise captures the materialized journal head as
  epoch start and fences the read epoch under the writer lock
  (`promise_metadata_takeover`). The swap (`build_metadata_takeover_swap`) is
  submitted and settled by its own outcome
  (`settle_metadata_takeover_swap`): applied or superseded keeps the fence,
  rejected at an unchanged epoch releases it, and an unknown outcome is re-sent
  as the identical request every 250 ms, doubling to the network timeout. That
  timer only re-sends. A promise found on disk at start, or by a later attempt,
  is settled the same way; a promise is never replaced.
- **Activate.** Once this node's publication names it primary at a takeover
  epoch and its own settlement, if any, has finished, activation rebinds its
  store in place, ends a promise from the read epoch and retires its fence, and
  aligns every write-set member it does not judge lost under the shard's
  mutation admission. It then requests the authoritative reconciliation, which runs
  prepared-change recovery at the new epoch (`recover:<operation>:<epoch>`)
  and hydration. Activation runs once per epoch per process, and again after a
  restart.

Alignment uses a prefix probe that names the epoch start and
`epoch_start_alignment = <epoch>`, answered on the direct journal-batch route
from the member's own record. A member already holding a floor at the epoch
past the target confirms its head. One whose journal ends before the epoch
start reports its head; the primary checks it against its own journal
(`historical_metadata_prefix_proof`) and confirms it, or repairs the member by
snapshot. One whose journal matches at the epoch start writes both epoch floors
at it, then cuts its journal and tables back to it and replays from its table
checkpoint (`align_metadata_replica_to_prefix`). One whose journal differs
there, or whose checkpoint is past it, rejects as `replica-diverged` and is
repaired by snapshot. A crash between the floors and the cut leaves an openable
store that the next alignment cuts. The epoch's first ingest on a member that
has written no floor at the epoch makes the same decision under the same lock:
it cuts and ingests, or rejects with `epoch-alignment-required`. The primary
seeds its replication cursor for each member from the confirmed head.

Each step publishes a `dfs.metadata.takeover.*` event (schema
`sinomo.dfs.metadata-takeover` 1.0.0): `corroboration_started`,
`corroboration_vetoed`, `corroboration_handed_off`, `promised`, `swapped`,
`swap_superseded`, `promise_released`, `activated` and, from the new primary,
`aligned` per member it confirmed. Each names the attempt, the read epoch, the
swap request id and the epoch start; the swap request id links them to the
record's last change. The old primary is never made primary again
automatically; once its keeper's publication deposes it, the new primary's
keeper aligns, catches up and re-adds it like any returning member.

Normal quorum-backed DFS operation must not create conflict records. Base-version races and stale writers are serialized at the owner or metadata-shard primary and must retry, reject, remain pending, or fail fast; a conflict record in this path is a correctness bug. Conflict records are reserved for future split-cluster or disconnected-authority recovery and need an explicit reconciliation algorithm before that mode can be enabled.

Replica-side commands are authorized against object ownership, metadata-shard primary state when available, desired replica membership, persisted intent records, and content hashes. Rejected replica commands do not create replica metadata.

Repair is restart-aware. On service hydration, persisted intents and receipts rebuild coordinator state and missing-replica jobs. Repair retries have backoff-like scheduling, and a recovered coordinator that no longer has local blob bytes can fetch the blob from another durable replica before repairing a missing node.

Reader- and file-backed data-plane operations attach their semantic byte bodies
to the canonical `sinomo-network` object-RPC call at every size. DFS heads
retain object/version identity, declared size, and content hashes, while the
network layer owns framing, flow control, health timeouts, and replay-safe
retries. DFS does not expose transport chunk indexes, windows, or resume routes.

### Lifecycle Maintenance

`NetworkedDfsService::run_lifecycle_maintenance` combines the operational lifecycle paths:

- content scrub,
- local content anti-entropy,
- old-version collection with retention,
- orphan blob GC,
- metadata tombstone compaction,
- bounded metadata-history GC of terminal records and receipts,
- metadata-backed namespace-job maintenance through daemon ticks.

`LocalContentStore` can enforce blob, logical-object, manifest, and staging-byte quotas and reports content footprint through operational metrics.

## Policies

A policy never has `min_commit_replicas` above `desired_replicas`: constructing
or decoding one that states a higher W raises N to W
(`normalized_replica_counts`), for replication, placement and subtree policies
and rules alike. Such a policy is never refused or lowered for it.

The main object policies currently used by the crate are:

- `LocalOwned`: one owner node is the write authority. Other nodes route reads and writes to the owner.
- `ReplicatedEventual`: a write becomes visible once `min_commit_replicas` durable receipts exist, then repair converges toward `desired_replicas`.
- `ReplicatedConfirmed`: a stricter replicated policy where all desired replicas must confirm before commit.

The policy is attached to objects and inherited by higher-level path APIs. DFS does not hardcode behavior by path except for the SIFS bootstrap defaults.

## Storage Layout

`register_storage_paths` declares the crate's data-directory areas. The main
ones are:

```text
dfs/content/blobs
dfs/content/inline-bindings
dfs/content/objects
dfs/content/catalog
dfs/content/staging
dfs/metadata
dfs/metadata/records
dfs/metadata/journal
dfs/metadata/root
dfs/metadata/shard-index
dfs/metadata-replication-cursors
dfs/anti-entropy-cursors
dfs/maintenance
dfs/mounted-mutation-outbox
dfs/imports
dfs/operator
dfs/pins
files
```

The same function registers the content-store observation, counter and lock
files. Retained-format areas such as `dfs/content/blob-packs` are registered by
`sinomo-dfs-migration`, and the attachment component registers its own state.

The exact storage format is intentionally hidden behind crate APIs and metadata backend traits. Other crates should use the DFS interfaces instead of reading these files directly.

## Using the SIFS POSIX Facade

The Main or Embedded DFS component supplies the client, transport, data owner,
metadata backend and publication executor. Mutations use that mounted runtime;
constructing a content store or backend does not grant publication authority.

```rust
use sinomo_dfs::{DfsNodeTransport, DfsWriteOptions, NetworkedDfsService};

fn write_example(
    dfs: &mut NetworkedDfsService<DfsNodeTransport>,
    options: DfsWriteOptions,
) -> Result<(), sinomo_dfs::DfsPosixError> {
    dfs.posix_mkdir_all("/users/example")?;
    dfs.posix_write_file("/users/example/hello.txt", b"hello from SIFS", options)?;
    assert_eq!(dfs.posix_read_file("/users/example/hello.txt")?, b"hello from SIFS");
    Ok(())
}
```

Tests use the shared embedded runtime in `test_support::DfsTestRuntime` with
`test-support` enabled. Its normal clients use `DfsNodeTransport`, native
component startup, authenticated ingress and the same guarded publication path.

## Operational Notes

- Do not put large file bytes in `sinomo-kv`; DFS content stores carry bytes.
- Do not put per-file metadata, content manifests, intents or receipts in `sinomo-kv`; DFS metadata shards carry high-cardinality state.
- Treat metadata backend records as the source of restart recovery.
- Use SOBJ or packed binary formats for internal DFS storage; JSON/JSONL is reserved for CLI output, service display, benchmark artifacts, and legacy migration.
- Use SIFS path APIs for filesystem-like workflows and object/version APIs for replication, repair, audit, and operator tooling.
- Quorum-backed write races reject, retry or remain pending according to their exact durable outcome.
- Local-owned subtrees remain useful for sensitive or node-specific files even inside the cluster root filesystem.
