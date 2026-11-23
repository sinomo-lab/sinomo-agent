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
recovery paths. A request's prepares are its admission, whether they record
its row or complete it: a primary that holds them, the request's own or the
member that took over its shard, recovers them in journal order before it
admits a repeat, so the repeat is never admitted beside them. A caller whose
shard record comes to name its own node runs the call there under the same
admission. A completed record replays its result across restart and is
final: no later row replaces it, and recovery aborts a row prepare it has
superseded. Reuse with a different semantic operation is rejected, and
`Pending` is an internal automatically recovering state rather than an
operator action.

Metadata journal replication is bounded by both entry count and encoded batch size. Oversized single entries still advance by themselves so a large namespace operation cannot stall a follower cursor forever. Daemon maintenance drains multiple bounded batches per publish call, with a cap that keeps each maintenance pass finite while avoiding one-batch-per-interval convergence for large metadata backlogs.

The table tail log is compacted into a streamed snapshot using storage format
12, bounding reopen cost by the latest checkpoint and recent writes. Its storage
version is independent of the V7 operation family. `sinomo-dfs` accepts only
current storage and wire formats.

Each metadata shard has a storage record beside its configuration and write
set. It names the current storage generation the shard serves and the evidence
that activated it. A native birth records it together with the assignment, and
only the shard's own conversion changes it: the attempt is recorded Converting
before it reads its inputs, sealed with the converted image, installed on the
members the shard's install rule names, then activated, and each member
activates its staged copy under the Active record. When the converting primary
is lost, another configured member may take the attempt over once every member
that has not left the cluster advertises the storage generation capability;
its activation also makes it primary at the next epoch in the same
transaction. Other serving changes (takeover, rotation, recruit, removal) never
write or compare the record, and no conversion transaction writes or compares
the filesystem configuration or its catalog generation. A shard opens only
when its local generation header matches an Active record, and a Converting
record keeps the shard closed, as does a root that still awaits storage
preparation. A node's closed
copy does not stop operations or node-wide passes on its other shards: a
node-wide pass sees the node as one without that copy and deletes nothing of
it, while work that addresses the closed shard is refused, or parked until its
storage activates. An activation wakes only the nodes holding work parked on
that shard. While a job waits at such a step, its coordinator's
count-contribution pages report the shards it waits on. The coordinator
announces the park, and the job's end while a shard it touched converts, as
typed `dfs.namespace.job.parked` and `dfs.namespace.job.completed` events
naming the filesystem, coordinator shard, job and touched shards; a
conversion waiting on the job wakes on them. A shard without a record is old
storage that `sinomo-dfs-migration` owns: it stays closed until its conversion
activates a record.

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

A served request runs under the node's mutation admission: one shard's exact lane, several shards' lanes, or the global writer, which waits for every lane holder and excludes them while it holds. Work on different lanes runs side by side. A shard's exact lane serves a member's answer to a journal batch, write-set probe or snapshot install, a namespace request stamped with exactly one authority shard other than those below that take several lanes, and a routed write, exclusive create or retained write stamped with its shard. A routed write keeps its coordinator, pending-write and receipt state in its lane's write service until it finishes; the lane's namespace requests run on a separate service that reads published state only, and the maintenance pass runs each unfinished write's stage retries and owed reply on its write service. The stage, commit, repair and fetch replies to the write's transfers run on the same lane. A reply that returns inline runs inside the write; one that returns on its own carries no shard, so it is matched under the global writer to the live write it answers and then runs on that write's lane. A reply that completes the write commits it on that lane. A reply that no live write on a lane answers runs on the global service. A namespace request whose footprint can span shards takes several lanes at once: a request stamped with more than one shard, such as a hardlink or bounded directory move whose two paths lie in different shards; every rename or unlink request, conditional or not, because it can change a hardlink's inode on another shard; and every namespace-job resume, because the job's frames can own steps on other shards. Its exact shards are not known before it plans, so it takes the lane of every shard of its filesystem that this node is primary of, all at once in canonical (filesystem, shard id) order, waits for a busy lane only while it holds none, and never takes a shard the node only replicates, so a member's answer on such a shard passes it. It runs on a service leased exactly those shards and reads every other shard from that shard's primary, as a node outside that shard's replica set does. A row whose path now routes to a shard that the node, or such a lease, holds no copy of is decided by placement among the copies it holds, as for an uncorroborated index, so it can show the old copy until migration evicts it. Each such read, and each step on another primary such as adopting a moved record there, is sent once the request's lanes and admission have been released, and the request then runs again with the result, so the other primary never waits on a lane this request holds. While a move's adopt step is on the destination, the source holds a fence on the moved object's family, its namespace entry and the move's job row, not the shard's writer: a publication that would change a fenced row is refused as busy, and every other publication on the shard goes ahead. A membership handoff of the source shard can start during that step; the move then fails closed at its source assignment checks and resumes through recovery. A destination that another cross-shard move holds refuses the adoption with a typed conflict, so the adopting move is rejected rather than waiting on the other. A move in flight guards its path: a listing or mutation is refused as busy at, under or above the moving path, while a lookup or path stat is refused only at or under it, so a directory that merely contains a moving entry still resolves and another move into that directory does not answer busy for it. The global writer serves a namespace read without exactly one stamp, a namespace request on a node that is primary of none of its filesystem's shards, an unstamped write and the other replies. A member's answer passes a global writer that is only waiting and never runs while one holds, because that writer may itself be waiting on the operation the answer serves. A mutation on one or several lanes that finds the resident generation behind takes the global writer only to initialize or roll it, then returns to its lanes; one still not current afterwards, such as one with a stale stamp, runs under the global writer, whose service rejects it.

Ordinary writes do not store per-file state in cluster KV or perform an unconditional Raft round. Before visible publication, the primary journals a hidden, operation-bound prepare in DFS metadata: the pending intent and exact `W` receipt set for content writes, plus the namespace transaction for first publication or namespace-only mutations. Each required member must durably apply and verify that exact prepare before acknowledging it: every configured member of a shard on the all-member rule, or the write set and pending adds of a shard on the write-set protocol (below). Missing acknowledgements leave the prepare durable and the outcome unknown; there is no cluster-quorum fallback. Cluster KV therefore retains only DFS configuration; content durability remains governed independently by `W`.

The acknowledgement wait has no DFS deadline. It ends when every required member has acknowledged, at the caller's operation deadline if the caller set one, on cancellation, when every attempt to a required member has ended with a network delivery failure, or on a definite rejection. A shard record that moved to another epoch ends it in one of two ways: a required member refuses the prepare's epoch with its own record view, or, after a catch-up round, the primary's retained shard context names another assignment or a pending handoff. Complete evidence is used even when the caller's deadline has passed. A required member that is behind by more than one bounded batch, or rejects the prepare, is caught up through the ordinary durability barrier, which adds no deadline to a commit: the catch-up ends when the member holds the prefix, when the network reports the member unreachable, on cancellation, or at the caller's deadline. A slow member is waited for. A target reached only over the routed path gets its empty confirmation re-sent on a timer that backs off to the network's liveness interval; the timer re-sends and never ends the wait. The durability barrier re-sends the same way: a member whose attempt did not take is sent to again after an interval that starts at 250 ms and doubles to the network's liveness interval, and once the fallback grace has passed and the network reports every member the barrier still waits for definitely unreachable, the barrier ends.

A requester's budget and its departure reach the node that serves its request. DFS adds no deadline of its own on either side: a POSIX commit, a routed namespace request, a caller's path lookup, stat or content read, a file or directory sync, an advisory lock, a browser route and a reconciliation probe are each bounded in time only by a budget their caller sets. A surface that wants a bound passes its own.

- **Budget.** Every request a budgeted caller sends carries its remaining budget as a relative duration in the request's metadata (`sinomo.dfs.request.budget-millis`), stamped again on each send. The serving node measures it from the request's arrival and installs it as the served request's operation deadline, which the acknowledgement wait, the catch-up barrier, a sync and the request's own sends to other nodes all observe. A served request that reaches the budget while it waits for admission or for a publication ends without having run. One that reaches it during its commit answers with the unknown outcome `deadline`. An advisory-lock request also names its budget in `timeout_millis`, which is absent when the caller set none; the authority applies no default and no cap.
- **Departure.** A requester that stops waiting drops its call: at the end of its budget, when it is cancelled, when its process exits, or when its connection is lost. The serving node observes the dropped request, which cancels the served request at its waits; a commit in progress ends with the unknown outcome `cancelled`.
- **Effect.** Either event ends the served wait and releases the request's mutation admission. Nothing is rolled back: the change stays prepared or committed as it was, and a retry with the same operation id, or the shard's recovery, resolves it.
- **No budget.** A request whose caller set no budget waits for events only: its commit, a definite failure, the network's verdict that a required member cannot be reached, a record change, or its requester's departure.
- **Replay.** A response-retry-safe request carries its caller's whole remaining budget on its first attempt. It is sent again, unchanged, only when that attempt lost its response to a broken call before the budget ended. The broken call has ended the first attempt's served wait, so the two attempts never hold admission together.

Timers on these paths only re-send: a waiting write re-drives its own stages, and a blind overwrite that met another writer's intent is sent again, each on an interval that doubles to the network's liveness interval and never ends the wait.

A requester follows its shard's record. It sends each request to the primary its own configuration projection names, and three answers tell it that record is changing: an unknown outcome, a refusal for the request's authority, and a request that could not be delivered. None of them routes the request: a node an answer names is never sent to on that answer's word.

- **Wait.** The requester waits on its projection until the shard record is strictly newer than the one it sent with: a higher epoch, at least the epoch an unknown outcome names, or another primary. It then sends the identical request, under the same operation id and with the body it kept for the whole call, to the primary the newer record names. There is no limit on how many records one call follows; each step needs a newer one.
- **Re-send.** While the record has not changed, a timer that starts at 250 ms and doubles to the network's liveness interval sends the request to the current primary again. Each re-send uses the current projected replica list, which can change while the epoch and primary stay the same. It also covers a member that returns and a primary that had not yet read its own record. The timer never ends the wait.
- **End.** The call ends on a definite answer, at its caller's budget, or when its caller is cancelled. At the budget the caller gets what the call still holds: the unknown outcome, the refusal, or the delivery failure.
- **Same id.** Every mutation keeps one operation id for its whole call, its caller's or one minted once for the call: a namespace mutation, a content write, a file or directory sync, an advisory lock, a pending-write abort and a namespace-job step. Each attempt is sent under that id, so the primary answers a repeat with what the first attempt did: from the request row or write intent it recorded, from the lock state's record of the lock command's own request id, from the aborted intent, or, for a sync, by certifying the same frozen target again. A read changes nothing, so each of its attempts has an id of its own.
- **Definite refusal.** A typed refusal that is neither an unknown outcome nor a refusal for the request's authority is the call's answer and is not waited on. That includes `Busy` and the refusal of a shard whose storage is closed for preparation or conversion. The serving node sends that refusal typed, with its cause (`preparation` or `conversion`): as `DfsNamespacePosixError::ShardStorageClosed` on a namespace reply and `DfsDistributedWriteResult::ShardStorageClosed` on a routed-write reply, whether its shard refused while the request ran or before its lane opened. The caller receives `DfsError::ShardStorageClosed`, never an undelivered or possibly applied call, and a mount returns `EAGAIN` (`NFS3ERR_JUKEBOX`). A closed shard cannot read what an earlier attempt of the same call did, so when such an attempt may have applied, that attempt's evidence stays the call's answer.
- **Pending write.** A routed write its primary answered `Pending` is held by that primary, which answers again when the write commits; no timer sends it again. Its caller's wait also observes the shard's record: when the projection moves past the record the write was sent under, the same write goes, under its id and with its body, to the primary the projection names.
- **Late answer.** An answer to an earlier attempt that arrives while the call waits for the record ends that wait at once and is read before anything is sent again. A commit the dispatch primary reports is the write's result even when the requester's projection has since moved past the record the write was sent under; only that answer's journal is left unreplayed.
- **Verification.** A request that could not be delivered leaves a hint for its node's takeover candidate role, which asks the network to verify the primary. A requester that delegates to its node's Main (a FUSE mount, an NFS export, the CLI, a hosted client) sends the hint to its own node as the DFS message `primary-verification-request` (`dfs.metadata.primary.verification_requested`: filesystem, shard id, and the epoch and primary of the record it sent with) on the DFS read route. Main hears it at ingress, without mutation admission, so it neither waits behind a lane holder nor holds lane work back. Main accepts it only from its own node and only while it still projects that record, and then treats it as its own hint. The message carries no obligation and its sender does not wait for an answer: one attempt that cannot be delivered posts it once, so a call posts it for the first attempt against a record and for each attempt the re-send timer makes, and never from the wait itself. A read-only probe that cannot be delivered (a stable reconciliation probe, a metadata query) posts it too.
- **Who waits.** A requester that delegates to its node's Main: a mount, the CLI, a hosted client. Work Main does for itself (a request it is serving, work under a shard's mutation admission, maintenance, recovery and placement reconciliation) reads the projection once and goes on only if the record already moved, because a wait there would hold a lane or stall the pass. A read follows the record the same way, within its caller's budget when the caller set one. A CLI operation fetches its configuration once and subscribes to its daemon's configuration announcements when it first has to wait, as a mount does for its whole life. A requester whose configuration client has no publication events does the same single read. A stable reconciliation probe that cannot be delivered is sent again like a read; an answered probe is returned as it is. A metadata query names one exact record. A query an attribute read makes (a stat by path or by object, which an NFS export makes for every request) follows that record like any other read when it cannot be delivered or its primary refuses it for authority: it is sent again on the re-send timer while the record stands, and a newer record ends it so that the attribute read plans again at the new record. A question put to every shard (the current record of an object by id, an object's lifecycle, a hardlink's target or aliases) asks each shard once: an answer that decides it ends the question, and a shard whose primary was not reached is needed only when no other shard decided. Its query's error is then the answer, except in an attribute read, which follows that shard's record and asks every shard again at its re-send time. A query another operation planned is not sent again: that operation's plan stands, and the collection it belongs to plans again when the record changes. A query waits for its answer's arrival, bounded only by its caller's budget, unless it is made while its service serves a request or holds a shard's admission.
- **Background passes park reads.** A read with no budget ends an attempt on the network's verdict, then follows the record. A background pass that runs on a requester that follows records (the attachment cleanup runs on the browser's service) does not make that wait: the read ends with `RecordWaitParked`, and the pass defers that work and goes on with the rest. The attachment cleanup tries a parked read again as soon as that shard's record changes, which it learns from the node's `dfs.configuration.published` event; its ten-second retry is only a fallback. Mutations in such a pass follow the record as everywhere else.
- **Mounts wait unlocked.** A FUSE mount or NFS export serves every call through one service behind one lock. A path lookup, an attribute or link read, a read by path, a metadata query, a namespace mutation, a content write, an advisory-lock command or an undelivered probe there that would wait for a newer record hands the wait back instead: the call releases the service lock and its mutation admission, waits on its configuration watch until the record changes, the re-send timer is due or its budget ends, and runs again. A mutation keeps one outbox marker for the whole call: the marker is armed once, nothing is written to the outbox while the call waits, and the work of every run is handed to the daemon once, when the call ends. The next run answers each namespace call an earlier run settled with that call's answer, in the order the run makes them, and resumes the waiting call where it stopped: under its operation id, with its body, its re-send timer and its budget. A content write, a truncate, a file create, a symlink and an advisory-lock command keep one operation id for the whole syscall, so each run sends the same write or command and its primary answers a repeat with what the first attempt did. A write its primary answered `Pending` hands its wait for the commit back too, with no timer: the primary holds the write and answers when it commits, so the call runs again only when a message arrives for it, the record changes or its budget ends, and that run sends nothing for the write but takes or awaits the primary's answer. A run that has handed its wait back sends nothing more. Only a run whose changes all go under such an id, or are namespace calls, can be run again, so a run that has sent another change makes its later waits where it is.

Shard membership changes retain the current primary and do not immediately activate a replacement authority. The quorum side first records a bounded pending handoff; this is not itself a write fence because the isolated retiring primary may not have observed it. After reconnecting, the retiring primary takes its publication guard, installs a durable local backend freeze, and acknowledges its exact journal head and fingerprint. That exact prefix must then reach and be acknowledged by the union of the retiring and desired assignments before the desired assignment activates. The local marker then advances to the activated epoch instead of disappearing: only the backend instance explicitly rebound after observing that exact control-plane generation may resume authoritative mutations, while independently opened stale instances remain fenced. Pending markers reject authoritative metadata mutations, and both pending and activated markers still permit replay and handoff transfer. This carries writes completed after the pending decision without putting healthy DFS operations through Raft.

#### Unknown commit outcomes

Once a prepare is durable, the outcome of any failed commit attempt is decided from the durable record, not from the error: a `Committed` record is the success it already is, an `Aborted` or absent record keeps the original error, and a `Prepared` record is `DfsError::MetadataCommitOutcomeUnknown { file_system_id, shard_id, epoch, operation_id, cause, current_primary_node_id }`. Typed kernel rejections (precondition failure, existing path, non-empty directory, stale directory version) stay definite. `cause` is a `DfsCommitOutcomeUnknownCause`: `acknowledgements-missing { required, acknowledged, missing }`, `below-write-set-floor { write_set, floor }`, `replication-below-intent { accepted, intent_w }`, `record-changed { epoch }`, `deadline`, `cancelled` or `interrupted { detail }`.

An unknown outcome is neither success nor failure. The caller retries the same operation id; the primary answers that retry from the durable prepare and commits it at most once. Its forms on each surface:

| Surface | Form |
|---|---|
| Routed write reply | `DfsDistributedWriteResult::CommitOutcomeUnknown { intent_id, file_system_id, shard_id, epoch, operation_id, cause, current_primary_node_id }`; the requester holds the pending write with its body and follows the shard record, and keeps a compact entry for a late response once its caller has stopped waiting. A commit attempted when a completing content receipt arrives answers a pending served write the same way; the receipt stays in the durable prepare that carries it |
| Routed namespace reply | `Rejected` carrying `DfsNamespacePosixError::CommitOutcomeUnknown { file_system_id, shard_id, epoch, operation_id, cause, current_primary_node_id }`; the request record stays `Pending`, and admission of a request whose prepare awaits recovery answers the same way. An admission whose own request-row commit is left unknown answers the same way; the same-id retry answers from that row once it commits |
| Routed namespace request whose caller's budget ends after dispatch | `NetworkDelivery { PossiblyApplied }`; the request was sent again under its one id until the budget |
| FUSE / NFS | `EIO` / `NFS3ERR_IO`, returned only once the syscall has followed the shard record for as long as the mount's operation budget allows, or without limit when the mount has none. The mount keeps serving |
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
both keys, the pending handoff, and the member and capability-policy records of
each member it admits. A shrink, recruit or removal also compares the
filesystem configuration, whose placement rules give the W floor and N it
checks. A switch compares the shard's storage record and is refused unless it
is Active, so a converting shard's install rule cannot change under it.
No change, and no membership handoff, advances the filesystem's catalog
generation, which guards path assignment only, so changes of different shards
never reject each other.
Each change advances the generation, so a transaction built from an
earlier record can never apply. Transactions carry a stable request id: a
takeover candidate settles an unknown swap by resubmitting the identical
request, which returns the committed reply or commits once, and the resulting
record names that request. A cooperative handoff compares the write-set key;
on a switched shard it admits only a primary rotation, and its completion moves
the write set to the new primary with the frozen prefix as epoch start. Default
setup never hands a shard with a write-set record to another layout or primary,
whether the record is bound to the configuration or not: a record that no
longer matches means an older writer changed the configuration or the binding
broke, and only a re-switch rebinds it. Recruit
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
not aligned with the epoch start (below). A request that matched the member's
record on arrival but still waits for the shard's admission when that record
moves to another epoch or primary gets the same answer, without the admission.
Older nodes read unknown codes as `other`. A commit that a required member
refuses for its epoch ends unknown as `record-changed` without a catch-up,
naming that member's primary when it is another node, and a fenced member is
posted to the keeper, which may shrink it.
A member that must be aligned first counts as a missing acknowledgement and is
posted to the keeper, which aligns it. The publisher suppresses replication to
a member that superseded its epoch; a fenced member is not suppressed, because
a takeover that does not apply releases the fence at the same epoch.

On a switched shard, file and directory sync certify the write set and pending
adds at the retained generation instead of a configured majority: each member is
brought to the synced prefix within the caller's budget, if it set one. Below the W floor
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
- shrink a member with a settled no-path verdict that counts under the
  record's epoch (the rule under [Takeover](#takeover)), one the cluster no
  longer authorizes, or one that refused the primary's epoch for a takeover
  candidate, never below the W floor;
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
authorizes the primary (which then has no verdict at all). A no-path verdict
counts only as evidence gathered under the record's epoch. One that turns
no-path while this node already reads that primary at that epoch counts as it
is. One that already stood when this node first read the epoch counts once the
network has examined the primary since: the node asks for a verification
episode and waits until `PeerReachability::concluded_generation` moves past
the value it noted, which only an episode whose own probe reached or exhausted
the peer's direct edges does. An episode that settles any other way
(cancelled, nothing to dial, no observation, out of rounds, or answered from
recorded evidence) certifies nothing; the node asks again after 250 ms,
doubling to the network timeout, for as long as the verdict stands. A request
that cannot be delivered to the primary, a content write as much as a namespace
request, asks the network to verify it first. An attempt that ended on its own evidence (a veto, a newer record or a
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
  `malformed`) and answers with its record view and durable journal head. It
  answers `lost` only when its own no-path verdict counts under its record's
  epoch by the rule above, at once. Any other no-path verdict has the network
  examine the primary first, and the answer follows how that examination ends:
  `lost` on a concluded no path, `reaches` on a path, `abstain { no-evidence }`
  when it ends without a conclusion or nothing can run it. A verdict that is
  not no-path has the network verify the primary and answers `reaches` or
  `abstain { no-evidence }` once that episode ends. The network bounds every
  episode, so the member always answers. A call ends on the reply, a network delivery failure or
  this node's no-path verdict for that member; nothing re-sends it on a timer.
  Any `reaches` vetoes the attempt and a `newer-record` ends it. Otherwise the
  candidate and every `lost` member are ranked by journal head, then non-mobile
  first, then node id. A member ranked first above the candidate gets a
  `hand-off { ranking }` request and runs the attempt itself when it still
  ranks first with its own head and its own no-path verdict counts under the
  epoch by the rule above. While the examination it asked for is pending or
  running it holds the request; it declines with `primary-unexamined` when
  nothing is examining the primary. One that reaches the primary, by the
  verification it ran when asked or by that examination, declines with
  `primary-reached`, which vetoes the attempt as a `reaches` answer does; one
  left with no verdict declines with `primary-not-lost`. It declines with
  `prefer` when it does not rank first, and a member already promising answers
  `followed`. A member that cannot run the
  attempt declines with `no-quorum` when its confirm read fails, or with
  `unavailable` when its keeper cannot evaluate the shard. On such a decline
  the candidate reads the record itself: if that read fails too it re-sends
  its read as a failed confirm read is re-sent (below), and once a read
  succeeds a quorum is serving, so it asks the same member once more.
  On any other decline but `primary-reached`, or a second availability
  decline, the candidate ranks again without that member. A write set of two
  asks nobody.
- **Confirm, promise, swap.** The candidate reads the record with one
  authoritative read of the shard keys and its configured members; a failed read
  promises nothing, so a node outside the KV quorum promises nothing. A failed
  read can be momentary while the KV voters elect, and no other change may
  arrive to prompt another attempt, so it is re-sent every 250 ms, doubling to
  the network timeout, while the primary is still judged lost (a settled no-path
  verdict, or its departure) and the record is unchanged. Any record or verdict
  change ends it, and the re-sent read's own outcome decides. The
  record must still name the judged primary at the attempt's epoch with this
  node in the write set, and must admit this node's swap
  (`check_metadata_takeover_swap`): the swap is built from the same record only
  after the promise, so a record that cannot build it, such as one under
  which this node's member record no longer admits it, promises nothing.
  The promise captures the materialized journal head as
  epoch start and fences the read epoch under the writer lock
  (`promise_metadata_takeover`). The swap (`build_metadata_takeover_swap`) is
  submitted and settled by its own outcome
  (`settle_metadata_takeover_swap`): applied or superseded keeps the fence,
  rejected at an unchanged epoch releases it, and an unknown outcome is re-sent
  as the identical request every 250 ms, doubling to the network timeout. That
  timer only re-sends. A promise found on disk at start, or by a later attempt,
  is settled the same way; a promise is never replaced. A promise whose swap
  can no longer be built from its record is kept, since its identical request
  may already be in flight, and is not re-sent.
- **Activate.** Once this node's publication names it primary at a takeover
  epoch and its own settlement, if any, has finished, activation rebinds its
  store in place, ends a promise from the read epoch and retires its fence, and
  aligns every write-set member it does not judge lost under the shard's
  mutation admission. It then requests the authoritative reconciliation, which runs
  prepared-change recovery at the new epoch (`recover:<operation>:<epoch>`)
  and hydration. Activation runs once per epoch per process, and again after a
  restart.
- **Open writes.** The new primary owns the shard's open content writes. It
  adopts each at its own epoch, one row update, and builds the write's transfer
  from the row, whose content size the write recorded when it opened. The
  write's bytes come from a receipt holder or from the caller's same-id retry,
  whose content is checked against the row's hash, never from the lost
  primary; the retry then finishes the write once.

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
`sinomo.dfs.metadata-takeover` 1.0.0): `corroboration_started` when the
attempt asks write-set members, `uncorroborated_started` when it has none to ask,
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
