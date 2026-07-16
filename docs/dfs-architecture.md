# Sinomo DFS Architecture

This document defines the developer architecture for Sinomo's distributed
filesystem. It focuses on authority, consistency, placement, recovery, and the
boundary between control metadata and file content. The [SIFS guide](sifs.md)
describes the default user-visible namespace.

## Purpose

Sinomo DFS is a local-first object and metadata layer for workspaces, evidence,
artifacts, service state, and cross-device handoff. It is designed for a small
trusted cluster whose workstations, home servers, laptops, and mobile devices
may connect intermittently.

The design separates:

- a compact replicated control plane for filesystem and shard placement;
- sharded metadata authority for namespace and version decisions;
- node-local content stores for immutable bytes;
- policy-driven replication, caching, and repair;
- bounded network transfer for content and metadata catch-up.

## Non-Goals

- Replacing the host filesystem or Git.
- Storing file bytes or high-cardinality file metadata in cluster KV.
- Public federation between unrelated clusters.
- Strong multi-writer POSIX behavior across disconnected authorities.
- Requiring laptops or phones for cluster quorum.
- Inferring ownership transfer merely because an owner is unreachable.

## Supported Policies

The current durable policies are:

| Policy | Visible commit | Typical use |
|---|---|---|
| `local-owned` | The owner commits the version. Other nodes may cache reads when policy permits transfer. | Repository checkouts, private files, and device-local state. |
| `replicated-eventual` | A configured receipt threshold commits the version; repair continues toward the desired replica count. | Shared artifacts, task handoff, and generated output. |
| `replicated-confirmed` | The required replica set acknowledges before the version becomes visible. | Artifacts that must survive the configured failures at commit time. |

The object-policy vocabulary also reserves append-only and ephemeral-local
modes, but they are not complete end-to-end durable service policies today.
Their implementation is tracked in the
[DFS policy and authority plan](plans/dfs-policy-and-authority-hardening.md).

The current replication policy separates the receipt threshold required for
visible commit from the desired replica count maintained over time. Candidate
selection and other service-level policy checks remain separate inputs.

## Consistency Invariants

Normal cluster operation has one authoritative visible history per object.
All visible writes serialize through the responsible owner or metadata-shard
primary. Ordinary DFS metadata and content stay off cluster KV: a healthy
primary first journals a hidden operation prepare in DFS metadata, then proves
the current assignment with a fresh acknowledgement from every configured
metadata-shard replica that durably applied and verified that exact prepare.
The prepare carries the pending intent and `W` receipts and, for namespace
changes, the complete transaction without making it visible. If the complete
shard cannot acknowledge, only an operation-bound cluster-quorum certificate
for that assignment and epoch may replace the full-shard proof. This prevents a
partition with only the content receipt threshold `W` from creating a second
visible history while allowing the quorum side to change membership safely.

Reconfiguration commitment is not itself authority activation. A direct shard
CAS cannot activate a changed configuration. A proof-carrying handoff may
either change membership while retaining the primary, or rotate the primary to
an existing replica while preserving the exact member ring; mixed primary and
membership changes are rejected. Either form advances exactly one epoch and
compares the bounded quorum-operation floor. The resulting pending handoff is
not itself a retiring-primary fence because an isolated primary may not have
observed it and may still complete a valid write. Once the retiring primary
reconnects, it takes the same publication guard used by canonical commits,
installs a durable shard-local backend write freeze, and acknowledges the exact
journal head and fingerprint captured under that guard. A pending freeze
survives process restart and blocks every authoritative metadata-table mutation
while still permitting replay, snapshot inspection, and handoff transfer.

Activation transfers that immutable fenced prefix across the union of the
retiring and desired metadata assignments and requires a joint-consensus
certificate: a majority of each assignment plus explicit acknowledgements from
both the retiring and desired primaries. The completion proof must equal the
retiring primary's fence; a watermark taken only when the quorum side created
the pending record is insufficient. This permits a configured replica to be
offline without freezing a healthy shard while ensuring the successor starts
with a durable majority and neither side of a reconfiguration can activate
alone. For same-primary membership changes, the completing backend instance
advances its local marker and explicitly rebinds. For a primary rotation, the
retiring instance records completion without binding the successor epoch and
therefore remains fenced. Before its exact-prefix acknowledgement can count,
the promoted replica must durably install a marker bound to the handoff id,
retiring epoch, sequence, and fingerprint, and echo that identity. It binds
only after freshly observing the active successor configuration, no pending
record, the exact completion proof, and a local prefix that either equals the
marker or is a validated contiguous extension of it. This extension case is
required because legitimate writes after activation may advance the durable
prefix before another service lane opens. An equal-height or later divergent
prefix cannot bind. Independently opened instances remain fenced until they
perform the same proof. Thus the quorum side may record replacement replicas
while the old shard is isolated, but it cannot activate a replacement writer
until the retiring primary has handed off every write completed before its
local fence. Healthy full-shard writes remain off Raft; exceptional quorum-
authorized prepares advance only one bounded per-shard transfer floor in KV.

Membership-handoff synchronization is therefore a joint-quorum operation, not
an ordinary metadata quorum barrier. It must not take the ordinary POSIX
cluster-quorum fallback based only on a majority of the union: a desired-only
quorum is not valid evidence for a retiring-epoch prefix, and a retiring-only
quorum does not make the successor durable. Progress stays pending and
retry-enabled until both assignment majorities and both primaries acknowledge,
or the bounded attempt times out without activation. A timed-out attempt
returns to durable pending state, retains the retiring write fence, and is
eligible for prompt rediscovery and retry. Missing desired replicas catch up
through normal replica maintenance after activation; missing retiring-only
replicas remain fenced by the committed successor epoch when they return.

Daemon maintenance discovery includes a pending handoff only on its retiring
primary. This allows a handoff committed immediately after one maintenance pass
to request the prompt maintenance cadence instead of idling until the long
periodic tick. A pending handoff owned by another node does not schedule a local
coordinator. Triggered reconciliation retains the same write-fence and joint-
quorum activation rules; the trigger changes scheduling, not authority.

Every distributed SIFS bootstrap performs the completed-generation rebind
check before its first authoritative namespace mutation. This closes the
fresh-service-lane case where cluster activation and the durable marker survive
but process-local authority binding does not. The marker remains immutable
handoff evidence; later contiguous journal entries advance the current prefix
without invalidating it.

Author authority and replica maintenance use separate admission gates. A
retiring lane with an activated marker remains unable to originate metadata,
but it may consume a journal batch only after the receiver has matched the
transport source to the exact current shard primary, or install a snapshot only
after matching that source, filesystem, and authority epoch to the committed
assignment. Batch replay still requires a contiguous authenticated prefix and
snapshot install still requires the exact authoritative state proof. Neither
path changes the process-local authority epoch. Snapshot replacement preserves
the durable handoff marker outside the archived replica state, so repair cannot
accidentally turn a retired lane back into an author.

That exception is represented by a narrow replica-ingest permit bound to the
validated filesystem, shard, source node, and authority epoch. Before taking
the writer lock, journal replay checks every mutation's outer filesystem and
every embedded record against that permit. Snapshot replacement first decodes
the complete canonical resulting tables and applies the same check across every
record family. A foreign or mixed-filesystem payload therefore rejects without
partial mutation or backend refresh. Raw journal replay and raw snapshot
replacement still enter the ordinary author gate and therefore remain frozen.
Direct object-RPC admission binds the payload source to the authenticated
cluster signer; in-service admission binds it to the transport source. Journal
commands carry the filesystem and authority epoch so ordinary clustered replay
fails closed when either identity is absent or stale. A journal returned with a
routed operation is additionally checked against both the immutable dispatch-
time shard assignment and the current committed assignment, rejecting late
old-primary frames and earlier epochs of the same primary before a permit can
be minted.

Prefix attestation is purpose-separated. An ordinary exact-prefix handoff probe
verifies the canonical direct backend that ordinary replication updates; a
pre-opened routed service lane is not authoritative evidence for that purpose
because its process-local view may lag the same durable files. A promoted-
primary probe remains on the routed current lane because it must verify,
persist, and echo the special promotion marker as well as the prefix. The
ordinary direct path cannot substitute for that marker proof.

Immediately before either attestation, the selected backend refreshes its
prefix proof and marker state from the durable view. This is cold control-path
work for synchronization, handoff, and recovery, not a read or wait added to
the ordinary metadata mutation hot path.

A replica counts toward the healthy proof only after it has replayed a
contiguous journal prefix through the prepare, re-read the exact hidden
operation from its DFS metadata tables, and proved that its complete resulting
sequence and fingerprint equal the primary's prepared prefix. Only then may it
durably record a monotonic local authority-acknowledgement floor and echo the
request nonce. Finding the named operation at the expected sequence is not
sufficient when earlier history diverges. Such a receiver rejects without
persisting the authority floor, leaving a later exact authoritative snapshot
repair admissible. A batch that skips unseen history is rejected rather than
advancing the replay cursor. Snapshot repair must advance strictly beyond an
ordinary authority floor, so an older in-flight snapshot cannot erase an
acknowledged prepare. These floors are small replica-local DFS coordination
records; they are not file metadata in cluster KV and do not require Raft.

The commit path must preserve these properties:

1. The writer names the base and target version.
2. The authority validates its epoch, policy, and base version.
3. Content is durable on the nodes required by policy.
4. Matching receipts exist before the visible version advances.
5. The exact hidden intent/receipt/namespace prepare is durable on every
   configured metadata-shard replica, or the exceptional quorum fallback has
   authorized that same operation.
6. The metadata commit revalidates the typed authorization and compares the
   current version, path occupants, and write intent
   atomically.
7. Replicas and caches may lag, but they cannot become write authorities.

A stale-base writer retries or fails. A write that cannot reach authority stays
pending or fails according to policy; it does not create a second visible
history. Any normal quorum-backed path that creates a conflict record is a
correctness bug.

Conflict records are reserved for a separate split-cluster recovery mode. That
mode remains disabled until a reconciliation algorithm is specified and
verified.

## Layered Model

| Layer | Authority and responsibility |
|---|---|
| Cluster KV | Filesystem configuration, metadata-shard placement, primary epochs, compact policies, and service discovery. |
| Metadata shards | Paths, object identity, versions, write intents, receipts, namespace transactions, durable jobs, and journals. |
| Content stores | Immutable blobs, extent manifests, staging data, local quotas, and garbage collection. |
| Network data plane | Bounded metadata batches, manifests, ranges, chunks, receipts, and repair traffic. |
| Daemon maintenance | Journal catch-up, namespace-job resume, anti-entropy, scrub, and lifecycle work. |

The cluster KV is small enough to replicate to every control-plane participant.
Per-path and per-version state remains in metadata shards, and bytes remain in
content stores. This keeps large namespaces and objects out of Raft while using
Raft to fence the authorities that own them.

## Metadata Authority

Metadata is partitioned into shards. Cluster KV records the current primary,
replicas, and epoch for each shard. The primary serializes namespace and version
changes; replicas follow its ordered journal and may install a compact snapshot
when incremental catch-up is no longer valid.

Shard rules:

- Only the current primary may originate mutations for a replicated shard.
- Every mutation is durable before it is acknowledged.
- Journal batches are bounded by entry count and encoded size.
- A replica applies the primary's sequence exactly; it does not append local
  entries into that sequence.
- A sequence fork triggers authoritative snapshot recovery rather than merge.
- Scans and hydration are cursor-based, so restart and listing do not require
  one unbounded allocation.
- Large directories use fragments to keep lookup and pagination bounded.

The persisted journal next-sequence head is a recovery cache, not allocation
authority. On reopen, the allocator audits both the durable segment tail and
the materialized table proof and promotes its next sequence to at least one
beyond their maximum before admitting a mutation. A stale head at `N - 1`
cannot cause sequence `N` to be reused when either durable source has already
reached `N`.

Directory tombstone validation is defined against the terminal namespace after
the complete shard-local transaction is replayed in planner order: tombstones
first, then live objects, with matching compacted tombstones skipped. The
directory may be removed only when its terminal direct-child set is empty. A
formally verified target optimization preserves this rule with a sparse overlay
seeded from the candidate directory's current direct children, removing each
transaction object's prior placement and adding only its final live placement.
This avoids cloning or scanning unrelated metadata; it does not weaken the
terminal emptiness precondition.

The logical metadata model contains:

| Record | Purpose |
|---|---|
| Object | Stable identity, path, kind, owner, policy, and visible version. |
| Version | Content hash, length, manifest, parent/base, writer, and commit state. |
| Intent | Base version, target version, authority epoch, placement, and required receipts. |
| Receipt | Evidence that an eligible node durably holds the expected content. |
| Namespace transaction | Atomic changes within one metadata authority. |
| Namespace job | Bounded, resumable work such as recursive operations or cross-shard moves. |
| Journal and snapshot | Ordered replication and bounded recovery of shard state. |

Object identity remains stable across path moves. Versions are immutable once
committed. Tombstones preserve enough history for replicas and caches to reject
stale paths without retaining deleted content indefinitely.

## Content Stores

Every storage-capable node may host a local content store. Content identity uses
BLAKE3 and immutable manifests. Bytes enter a staging area, are verified, and
become visible to metadata only after the relevant durability policy is met.

Small contiguous objects may use a whole-blob fast path. Large or patched
objects use extent manifests, bounded streaming I/O, and range reads. Packed
segments can reduce file-count overhead for small blobs without changing
content identity.

Content-store durability does not grant namespace authority. A node can possess
the right bytes while its receipt is stale, its metadata view is behind, or its
policy no longer permits serving them.

## Placement

The current planner receives candidate nodes from its caller, prefers the
coordinator where required, and uses stable weighted hashing to select a
deterministic set for the object and version. The replication policy supplies
the minimum commit and desired replica counts.

Health, capacity, endpoint quality, session locality, battery, bandwidth,
privacy, and trust-domain inputs belong in upstream eligibility today; the
planner does not yet score all of them directly. Bringing those inputs into one
auditable placement policy is tracked in the
[DFS policy and authority plan](plans/dfs-policy-and-authority-hardening.md).

Read caches remain distinct from durable receipts and never become write
authorities merely because they possess current bytes. If the minimum durability
threshold cannot be met, the write remains pending or fails.

## Write Protocols

### Replicated Commit Path

Replicated-eventual and replicated-confirmed writes use this logical sequence:

1. Resolve the object, metadata authority, and expected base version.
2. Create or validate a write intent at that authority.
3. Stage bytes and verify their content hash.
4. Place content on eligible nodes according to policy.
5. Record durable receipts that match the intent and hash.
6. Journal the hidden intent/receipt prepare in DFS metadata, including the
   namespace transaction when this is first publication.
7. Replicate and verify that exact prepare on every configured metadata-shard
   replica; if any is unavailable or stale, obtain the operation-bound
   cluster-quorum fallback certificate instead.
8. Atomically compare the authority epoch, base version, intent state, and
   receipt threshold.
9. Advance the visible version and schedule any remaining repair.
10. Publish metadata progress to caches and interested sessions.

The durable write intent lets retry and recovery determine whether work is still
open, committed, pending repair, or aborted without inferring state from an
in-memory queue.

### Local-Owned

The owner is the only write authority. Requests from other nodes route to the
owner, which validates the base version and commits locally. Confirmed remote
caches may serve reads when policy permits but cannot accept writes.

An offline owner does not trigger automatic ownership transfer. A transfer must
fence the old epoch, prove that the new owner has current content, and update
metadata authority before the old owner can reconnect.

### Replicated-Eventual

The metadata primary commits after the configured minimum number of durable
receipts. Remaining desired replicas are repair work. The visible version is
serialized even though placement convergence is asynchronous.

The primary retains an effective repair mode after transport and deadline
handling; this may differ from the caller's outer wait policy. In background
mode it reserves only the exact missing `W` stage slots, publishes the response
as soon as `W` is durable, and starts `N-W` repair afterward. In foreground
mode it may stage toward every desired replica and visibly commit at `W`, but
its own response waits until all `N` replicas are durable. This differs from
replicated-confirmed policy, where the visible version itself waits for the
complete required replica set.

Full-body stage and repair transfers are work-conserving within those eligible
sets under a fixed two-transfer fan-out cap. Stage and repair reservations are
disjoint. Each target retains its own request identity and success or retry
outcome, so one unavailable replica does not cancel a healthy sibling. A zero-
or one-target operation stays direct. A bounded transport batch joins before
its messages return to the service, but background scheduling never places an
unneeded `N-W` sibling in the batch that establishes `W`. Inline blobs and
patches retain their existing sequential path.

A durable staged receipt may require a detached `CommitVersion` notification
to promote the replica's visible cache. This notification is not durability
evidence: it is queued after the write response is constructed and drained by
a level-triggered worker with a separate fan-out cap of two. The worker claims
the outbox after one blocking-pool lane acquisition, releases that lane, and
performs every RPC wait outside it, so a slow replica cannot serialize later
primary mutations and a busy lane does not create a one-millisecond poll/task
loop. The scheduler wakes the exact lane returned by request processing; cache
replacement cannot redirect that wake to a new empty lane. Primary, read, and
dedicated recovery services share one detached identity state for the daemon
data directory while retaining independent routing and response queues.

Only a matching `Committed` `CommitVersionResponse` settles an entry;
`Durable` proves staging only. Transport errors, durable-only, missing or
mismatched acknowledgements, and retryable explicit rejection retain the exact
identity for idempotent backoff retry without replaying successful siblings.
Two authenticated rejection classes are terminal for the exact notification.
A receiver whose durable intent excludes its local node cannot become
authorized by replaying the identical command. A current metadata-shard primary
that cannot open its exact local committed content first durably removes its
stale receipt and visible location; its typed repair-pending rejection transfers
convergence ownership to the ordinary committed-intent repair ledger. That
acknowledgement proves the receipt/location prune is durable; it does not claim
that content repair has completed. When no exact-version source is currently
reachable, the repair remains pending under the existing bounded backoff. A
non-authority cannot make the prune transition, so the same missing-content
failure remains retryable. The sender emits a structured warning and retires
either terminal identity without claiming a committed acknowledgement. A bounded
terminal frontier retains one settled, undesired, or repair-pending
intent/version/hash per target/filesystem/object key and suppresses exact
duplicates while allowing a new intent identity for that key. Terminal
classification must match the expected requester and intent or the exact repair
object, version, and hash; wrapped or mismatched reasons stay retryable. A drain
attempts every ready identity once under the cap before returning its first
retryable error, then rotates failed identities behind unattempted or
concurrently queued work, preventing permanent failures from head-of-line
blocking healthy promotions.

Durable committed intents and receipts reconstruct outstanding exact
promotions in a dedicated background maintenance hydration after restart, so
recovery does not wait for a namespace read or hold a cached service lane.
Shared pending and settled indexes prevent the read-lane pool or periodic
hydration from duplicating the same identity. Completed routed-write responses
for a caller other than the request currently attached to a served transport
use the same replay-safe detached boundary; the current caller's response stays
inline, and unrelated requester latency no longer holds the service lane. The
abstract formal
`responsePublished` step refines the response-construction/enqueue boundary,
not proof that client bytes reached the wire before the independent worker ran.
Once every desired replica is durable, the write has no repair jobs, transfer
reservations, retry or attempt records, or retained pending-write state. Queued
promotion remains independent post-response work.

The serial metadata portion of one hot shard is expected to occupy roughly one
core because its primary orders that shard's namespace and version decisions.
The default root, services, and users shards have three distinct primaries and
can therefore use about three aggregate metadata cores under independent lane
writes. More host parallelism comes from additional shards and concurrent
writes, plus content hashing, replica transfer, and transport work; one metadata
shard is not designed to consume every host CPU.

Daemon object-RPC ingress remains Tokio-concurrent, while the synchronous DFS
state machine runs on admitted blocking workers. Authority-scoped routed writes
and single-shard namespace commands carry a full filesystem/shard authority
stamp; direct journal and snapshot commands carry the exact backend identity.
The daemon validates those identities in the DFS service, queues same-shard
mutations before `spawn_blocking`, and gives disjoint current shards independent
service lanes. An exact request reserves its shard before awaiting the global
read fence. A same-shard backlog therefore holds no global read guards, and a
queued global writer waits only for currently active exact owners. Neither kind
of waiter consumes a blocking worker. The admission guard moves into the
non-cancellable blocking closure, so disconnecting a caller cannot release
shard ownership while its mutation is still running. Read-only requests bypass
mutation admission and select the least-loaded service lane, growing the pool
only when every existing lane is assigned. Replica-transfer authority stamps
that are not yet compared with the receiver's complete current shard config,
rolling-upgrade messages without a provable footprint, responses whose state
owner is local, and multi-path namespace requests retain the global
compatibility lane. Wire-derived gate state is bounded, and cached exact
services are created only for shards present in the committed topology.

The current Rust scheduler deliberately implements the exact single-shard and
global subset of the formal model. Atomic multi-shard admission and explicit
queued-request cancellation across topology generations remain target
hardening; multi-path work stays globally exclusive until those refinements are
implemented.

If a live metadata authority and at least one current source remain reachable,
and eligible nodes eventually reconnect, anti-entropy converges the desired
replica set to the current version.

### Replicated-Confirmed

The metadata primary advances the visible version only after the complete
required replica set has durable matching content. A timeout does not downgrade
the write silently; the intent remains pending or is rejected according to the
declared policy.

## Namespace Operations

Path mutations execute at the primary responsible for the affected namespace.
The primary validates every object and parent involved, commits the namespace
transaction, and returns bounded journal progress so the requester can read its
own change.

Operations contained within one authority are atomic. Recursive work is split
into bounded durable jobs that resume after restart.

Retained namespace-prepare history requires transaction identity to bind the
immutable mutation generation, not only the operation kind and stable
object/path. A directory recreated from a selected tombstone may deliberately
reuse the stable object id and path, but its newer object version changes the
prepare payload. Its mkdir transaction id therefore includes that version. An
exact retry of the same generation retains the same id and payload and replays
without another mutation; a later resurrection receives a distinct id and does
not collide with the original mkdir retained in history.

Hidden namespace prepares reserve their normalized object/path footprint in
filesystem-qualified active-owner indexes. Admission performs only direct
lookups for the candidate footprint; it does not scan retained prepare history.
An exact same-operation retry ignores its own ownership. Only `Prepared` rows
own keys: `Committed` and `Aborted` rows remain durable for recovery and audit
but release the footprint atomically. Structured-snapshot hydration and table-
log replay derive the indexes from the same prepare table, so restart preserves
active reservations without resurrecting terminal ones.

A move or hardlink that crosses metadata primaries uses a durable handoff:

1. The source authority validates and locks the affected namespace.
2. The destination authority validates its side and durably adopts the record.
3. The source authority tombstones the old path and completes the handoff.

Transport failure parks the job with its lock and progress. After destination
adoption, recovery proceeds forward; it does not roll back an already visible
destination into an ambiguous state. Authority changes are re-resolved on
resume, and both sides validate current epochs.

## POSIX Synchronization and Partition Fencing

The canonical DFS synchronization contract distinguishes advisory file locks,
internal namespace-job reservations, write visibility, and durability. These
are separate mechanisms:

- a POSIX advisory lock coordinates cooperating sessions on a stable object id;
- an internal reservation protects a bounded metadata transaction or durable
  namespace job;
- neither kind of lock grants metadata authority or counts as a content
  receipt; and
- `fsync` is a durability barrier for writes that already returned, not a lock
  acquisition or an authority transfer.

The first formally modeled advisory-lock surface is deliberately bounded to
whole-file shared and exclusive locks. Grants carry the metadata-authority
epoch, a unique grant token, and a finite validity interval. An exact token can
release its grant. A session that can reach the current authority renews the
same grant, so logical time alone cannot expire a healthy lock. A session-local
renewal outage starts the bounded expiry countdown. Close, process/session loss,
expiry, or a newer authority epoch makes the old grant ineffective. Expiry and
fencing produce explicit lost-lock outcomes, including terminal errors for
blocking requests. A stale token cannot release a lock granted in the new
epoch. The locks remain advisory: clients that bypass the lock protocol are not
made safe by the existence of a grant.

A partitioned lock holder may finish an already certified canonical operation
only while all of these remain true:

1. every metadata shard touched by the operation is present at the certified
   current journal prefix on the holder's component;
2. all required content is present and hash-validated; and
3. the operation's strong permit remains valid;
4. before publication, the combined model consumes an abstract proof token for
   the configured content-receipt threshold and metadata prepare quorum. The
   focused sync model verifies that their bounded recovery sets intersect; the
   atomic publication point then forms the failover-recoverable commit
   certificate. A direct refinement mapping between the two models remains
   future work.

Shard or content presence alone never grants authority. With renewable permits,
the quorum component waits through the old permit's fencing deadline before it
activates a higher epoch. If the holder lacks any required state, it suspends
immediately and cannot commit while waiting for that barrier. Once a quorum
activates the higher epoch, it wins regardless of how many content shards are
currently reachable on either side. The old lock and reservation are fenced.
If the winner lacks the latest committed content, it remains in an explicit
`pending-repair` state and cannot serve stale bytes or accept dependent writes
until repair validates the canonical object, version, and hash.

This rule permits a complete holder to finish a bounded operation before its
permit expires without allowing it to race a new quorum authority. Allowing a
minority holder to commit while a quorum can immediately activate another epoch
would create two canonical histories and is forbidden.

The synchronization barriers are:

| Operation | Successful-return guarantee |
|---|---|
| `write` | The write is visible through the current authority, but may still be volatile across authority loss. |
| `fdatasync(file)` | All writes returned before the call, plus data-retrieval metadata such as length/extents, meet the configured receipt policy and have a failover-recoverable metadata certificate. |
| `fsync(file)` | The same prefix plus file-integrity metadata is durable. It does not make the parent directory entry durable. |
| `fsync(directory)` | Namespace changes captured by the call, such as create, rename, and unlink entries, are durably certified. |

The formal target assigns every sync request a durable idempotency id and
immutable payload. The current Rust barrier is deliberately narrower: its
captured target and restart-unique metadata probe are immutable, but
caller-selected sync ids and outcome-unknown replay records remain future
work. A lost reply must therefore be retried by recapturing the current
stable-object target, not by assuming that a pathname or cached per-handle
version is still the same prefix.

Obsolete locks and abandoned internal reservations must not create protocol
deadlock. Multi-shard reservations use deterministic acquisition order,
destination-visible jobs recover forward, expired/fenced locks do not block the
current epoch, and terminal jobs release their effective reservations. Eventual
completion still assumes eventual logical-time advancement, a stable quorum, a
validated source for committed content, and fair recovery scheduling.

The Rust DFS now implements the first bounded refinement of the advisory-lock
and sync slices. Whole-file `F_GETLK`, `F_SETLK`, and `F_SETLKW` route to the
stable object's metadata primary; lock aggregates are journaled, replicated to
every committed shard member on the healthy path, epoch-fenced, leased,
renewed by mounted clients, and moved atomically with a cross-shard object
adoption. A partial metadata recovery quorum may complete only after publishing
an exact operation-bound head watermark through the cluster-quorum fallback.
Blocking waits run outside the FUSE dispatch lane, and exact close/cancel
transitions plus finite leases keep abandoned waiters from becoming protocol
deadlocks. Partial byte ranges remain unsupported, and a conflicting blocking
shared-to-exclusive conversion returns `EDEADLK` instead of creating a
conversion cycle.

Lock sessions and request ids are scoped to the authenticated requesting node;
one node cannot close or cancel another node's grant by replaying an observed
identifier. Clients consume an authority-reported relative lease budget using a
local monotonic clock. On authority change, the new primary also waits the old
deadline plus a configured five-minute maximum inter-node clock-skew bound.
Strong renewable locks therefore require clocks to stay within that bound.
The current FUSE refinement caps each mount at two distinct held or pending
locks and uses ten-second coordination calls, leaving renewal headroom behind a
foreground durability barrier; a third independent lock returns `ENOLCK`.
Mutating lock requests retain their durable idempotency outcome without a
bounded eviction window so an arbitrarily delayed exact retry cannot resurrect
an acquisition. Read-only `F_GETLK` queries do not grow that history. A
monotone per-session acknowledgement watermark is required before the retained
history can be compacted safely. The current `fuser` API drops
`FUSE_INTERRUPT`, so mount shutdown, close/cancel, and finite wait leases bound
blocking requests, but signal-driven cancellation of one `F_SETLKW` call is
not yet available.
When a locked object moves between metadata shards, the destination installs
the rebinding-marked lock aggregate with the object, syncs it to metadata
quorum, and returns an exact typed digest receipt before the source may evict.
Peers that predate lock-state adoption cannot return that receipt, so a rolling
upgrade fails the move safely; an exact retry can repair an object that the old
peer adopted before it understood the lock aggregate.

FUSE writes now publish through the canonical DFS write path before the write
reply, eliminating per-handle dirty-buffer visibility races during lock
handoff. `flush` and `release` therefore perform close/lock cleanup but do not
claim durability. `O_DSYNC` and `O_SYNC` writes invoke the file-data and
full-file barriers respectively. File `fsync`, file `fdatasync`, and directory
`fsync` capture immutable
targets and route distinct operations to the metadata authority. A successful
file result validates the exact version/hash on every desired content replica,
collects a fresh acknowledgement of the complete canonical materialized
metadata checkpoint at the exact journal head, and intersects that recovery set
with the content holders. When every committed shard member acknowledges, the
barrier returns without a KV transaction. If a member is unavailable but the
existing metadata-recovery quorum and content intersection are satisfied, the
exceptional path publishes one epoch- and fingerprint-bound metadata-head
watermark through cluster Raft before returning.
The checkpoint probe has a restart-unique identity and carries an empty journal
batch, so a delayed reply cannot satisfy another attempt and a rolling-upgrade
peer cannot skip its replay cursor. A persisted replication cursor alone is
never a sync acknowledgement. Each replica also persists a monotonic,
fingerprint-bound checkpoint floor before acknowledging the probe; delayed
snapshot repair is rejected below that floor or at an equal sequence with a
different state. The current `fdatasync` physical barrier is
deliberately as strong as `fsync` because essential and ancillary file metadata
share one journal transaction.

Direct checkpoint journal requests use a work-conserving fan-out cap of two and
one 250 ms operation-wide best-effort budget. On the production multithread
Tokio runtime, sibling peer RPC futures run concurrently, each successful
response is retained independently, and late siblings are aborted and dropped
at the budget. The blocking compatibility path has the same two-worker cap and
returns at the budget; a foreign blocking call already entered may outlive that
return, so a per-transport worker-slot bound prevents repeated calls from
accumulating detached threads. Direct fan-out is only an accelerator: quorum
fallback and later catch-up retain correctness when a peer is slow or rejected.

This is not yet a full refinement of the combined partition model. Rust can
continue when the complete committed metadata shard acknowledges even if Raft
is unavailable; when neither that full-shard proof nor the cluster-quorum
fallback is available, new grants, renewals, and sync completion fail or wait
rather than using the model's complete-holder permit. Explicit caller-selected durable sync ids, byte-range locks,
application-level wait-cycle detection, open-unlinked lifetime, and directory
open-handle identity remain target hardening. FUSE data I/O now targets the
stable object id and fails closed if the pathname has been replaced; it does
not yet retain an unlinked inode after the last name disappears. Each shard
persists an incremental hash-chain prefix proof, so healthy exact-head probes
compare sequence and digest in O(1) work. A legacy shard hashes its materialized
snapshot once to seed the chain; a genuine sequence/digest mismatch still uses
an authoritative full snapshot repair. Bounded journal catch-up before that
snapshot is future scale hardening.

## Opt-in Local-first Degraded Operation

The target local-first protocol is an extension beside the canonical DFS
protocol, not a second metadata authority. A service or subtree must opt in
through previously certified strong metadata. When a client cannot reach the
current authority, the target permits operations on a durable local branch
rooted at an exact certified metadata checkpoint. This remains true even when
a quorum exists elsewhere: the disconnected node cannot observe that quorum,
and its branch never becomes a second canonical history. The bounded model
represents certification as a supplied opt-in policy and certified base state;
it does not derive either from the canonical control plane.

The bounded file slice varies global quorum availability independently from the
local authority route. Canonical writes and reconciliation stop without quorum,
while local overlay work remains isolated. If the authority route returns to
an open branch, a locally synced dirty image seals atomically; an unsynced or
clean image enters a permanently frozen phase that cannot resume degraded
mutation after another route flap. The frozen branch may locally sync and seal,
or a clean branch may close without publishing anything.

The formal boundary is an assume/guarantee abstraction, represented by a
compact projection of authority epoch, journal head, object and namespace
revision counters, visible-authority/head counts, and canonical conflict count.
Local writes, local durability barriers, crashes, branch sealing,
reconciliation planning, and local conflict preservation stutter under that
projection. The standalone file and namespace slices still consume seven
compact publication facts: quorum, current authority, matching epoch and plan
head, content policy, metadata prepare, and atomic publication. The composed
file and namespace slices replace that assumption at the branch boundary:
planning and waiting remain nonterminal, and the only exposed terminal action
pairs the branch result with the gateway journal commit in the same transition.
Consequently `BranchTerminal` holds exactly when the matching gateway entry is
committed; result-table and conflict-sidecar materialization may still lag and
replay from that committed envelope. The file composition closes its
two-manifest domain by allowing one dirty local sync before sealing, and the
namespace composition reserves one bounded journal slot for its terminal entry
so environment commits cannot exhaust the finite model before reconciliation.

A separate one-shard reconciliation gateway now refines those facts into a
typed permit. Its quorum, authority, content-receipt, and metadata-prepare
certificates are all bound to the same payload digest, configuration revision,
authority epoch, predecessor head, and successor journal index. The permit also
contains one atomic publication envelope holding the canonical delta, terminal
result, and optional conflict-sidecar descriptor. Changing the head, epoch,
primary, configuration, or payload invalidates the permit instead of relying on
a caller-supplied Boolean. The gateway explicitly models volatile planning,
append, quorum commit, crash before and after commit, journal replay,
materialization, lost replies, exact retry, and changed-payload rejection.
The branch-to-gateway compositions close the abstract action-level
result-recording gap for the bounded terminal protocols. They admit two exact
one-sync file manifests and seven named one-operation namespace manifests, and
map applicable, converged, conflict, and superseded results to distinct atomic
envelopes. This is not yet composition with Raft, advisory locks, canonical
`fsync`, partition recovery, no-conflict enforcement, or the Rust journal.
Under the derived permit, a publication retains one visible authority and head
and creates no canonical conflict record.

The target protocol gives each branch a durable incarnation, monotonic
operation identities, an immutable base checkpoint, exact object-version and
namespace-entry stamps, and a sealed manifest. Offline-created objects use
globally unique stable ids, and file and directory operations target those ids
rather than relying on a pathname that may have moved. The bounded file slice
models one complete reconciliation payload and a local sync generation. A
companion namespace-durability slice models one returned namespace mutation,
local directory `fsync`, crash restoration from the exact locally durable
image, and sealing only after that image is durable. These local barriers do
not produce a canonical quorum sync certificate; distinct local `fdatasync`
and full-file `fsync` semantics remain a target protocol concern. A distributed
advisory-lock token is never converted into a local authority token after loss
or fencing, but direct lock-state composition is also future work.

The target reconciliation protocol includes applicable, already-converged,
conflicted, superseded, dependency-blocked, and rejected outcomes. The bounded
file slice exercises apply, converge, and conflict; the namespace slice adds
superseded. An applicable operation becomes one canonical metadata
transaction. In the focused branch models, a conflict leaves the quorum winner
at the canonical path and retains a symbolic file-content value or a set of
namespace object identities in a disjoint sidecar; superseded creates no
sidecar. Those slices do not prove storage retention receipts, retained subtree
bytes, or post-crash reachability of those objects. The gateway adds a bounded
receipt certificate from two distinct symbolic holders and proves that it
intersects the metadata-prepare quorum. It still does not validate concrete
bytes, hashes, holder persistence, subtree closure, or post-crash object
reachability. File reconciliation records are keyed by the full modeled payload, so
a lost reply can be replayed exactly while changed content, base head, or local
sync generation is rejected. In the gateway, the committed journal envelope is
the durable source of the terminal result and canonical effect; table and
sidecar materialization may lag and recover idempotently from that envelope. A
modeled process restart clears the transient reply but preserves the committed
envelope before replay. The byte-level WAL and Rust metadata-transaction
refinement remain open. The namespace composition binds each reachable finite
manifest shape to a distinct symbolic digest and couples its deterministic
terminal retry to the gateway result. It still fixes one request identity and
does not model arbitrary payload hashes, multiple requests, or a general
payload-keyed namespace replay table. A changed canonical journal head
invalidates either plan and requires reclassification. Explicit conflict
resolution is itself a target strong compare-and-swap transaction.

The first bounded Quint slices cover regular-file write, truncate, local sync,
crash recovery, sealing and reconciliation, plus one same-shard create, mkdir,
unlink, rmdir, or atomic no-replacement rename with stable parent/object
identity and exact entry stamps. Rename requires an absent destination; POSIX
replacement semantics are not modeled. The directory-durability companion is
separate from namespace reconciliation. The slices exclude hardlinks,
symlinks, cross-shard local changes, branch gossip, semantic content mergers,
concrete byte-validated retention receipts, byte-range locks, and full
open-unlinked lifetime. The Rust
DFS still takes the conservative strong-path behavior described above; the
local-first slices are target-hardening models, not a claim that degraded
mutation is implemented end to end. The new terminal compositions verify the
bounded branch/gateway boundary only; they do not turn the separate local
durability, lock, canonical sync, partition, or Raft models into one end-to-end
refinement.

## Reads and Caches

Reads prefer the closest valid copy:

1. local content for the requested current version;
2. an allowed local stale version;
3. a nearby confirmed replica;
4. the owner or durable home;
5. a routed or relayed source.

Every result exposes freshness: current, stale, pending repair, unavailable,
or policy-blocked. A local hash must match the selected immutable version before
bytes are returned. Metadata watches mark cached objects stale when the visible
version changes.

Automatic caches are useful for mobile inspection and collaborative review.
Evicting a cache never changes authoritative metadata or the durable receipt
set.

## Transfer and Repair

The network data plane uses typed object RPC and stream extensions owned by
`sinomo-network`. Transfers are bounded by range, chunk count, window size, and
deadline. Optional per-chunk hashes detect corruption before final manifest
validation.

Repair is manifest-based rather than an arbitrary directory scan:

1. Compare current metadata and receipts with locally available manifests.
2. Fetch missing current content from an eligible source.
3. Refresh a receipt only after the local hash matches.
4. Quarantine divergent blobs instead of treating them as versions.
5. Continue until the desired placement is satisfied or authority/policy
   suppresses the work explicitly.

Persisted intents and receipts reconstruct the repair backlog after restart.
Scrub, old-version collection, orphan cleanup, and tombstone compaction are
background lifecycle operations.

## Roaming and Failure Behavior

Roaming nodes are ordinary trusted members but should not normally be required
for control-plane quorum. They may hold caches or data replicas and repair after
reconnect.

| Condition | Required behavior |
|---|---|
| KV quorum unavailable | KV-backed placement and authority changes cannot commit. A shard primary may continue from its latest valid assignment; stronger quorum fencing is planned hardening. |
| Metadata primary unavailable | Writes wait, reroute after an authorized authority change, or fail explicitly. |
| Local-owned owner offline | Writes queue or fail; ownership does not move automatically. |
| Commit threshold unavailable | Intent remains pending or rejects without weakening policy. |
| Desired replica offline after eventual commit | Current version remains visible and repair resumes later. |
| Cache stale | Serve only when caller and policy allow stale reads. |
| Replica journal diverges | Install an authoritative shard snapshot. |
| Node reconnects | Compare manifests and receipts, then perform bounded catch-up and repair. |

Pins and prefetch hints can prepare likely-needed SIFS paths before a device
leaves a LAN. Sensitive raw content remains on its owner unless policy allows
transfer.

## SIFS Cluster Root

SIFS is the default DFS namespace exposed to users, agents, services, and
rollout workflows. Its baseline layout is:

```text
/
/sinomo/nodes/<node_id>/files
/users
/services
```

The node files subtree is local-owned. User and service subtrees are shared
according to their inherited DFS policies. Higher-level concepts such as agent
skills, workspaces, and evolution artifacts map onto SIFS paths without adding
new filesystem consistency rules.

See [SIFS](sifs.md) for the full layout and platform access behavior.

## Security and Policy

Every node enforces local authority for bytes it stores or serves. Current
checks cover:

- current cluster membership and request identity;
- metadata authority epoch and base version;
- object mode and replication receipt requirements;
- content hashes, chunk bounds, quotas, and sandbox scope.

Richer trust-domain, raw-content egress, encryption, and retention policy is
part of the [DFS hardening plan](plans/dfs-policy-and-authority-hardening.md).
Metadata stores hashes and policy references rather than cleartext secrets.
Policy denials are structured events containing requester, executor,
filesystem, object, and reason.

## Events and Observation

DFS emits events for filesystem and object lifecycle, reads, write intents,
staging, receipts, commits, rejection, repair, cache activity, and policy
violations. Events carry node, filesystem, object, version, policy, content hash,
and routing metadata when applicable.

Operators need metrics for repair backlog, metadata counts, content footprint,
namespace jobs, transfer progress, quota pressure, and shard catch-up. Metrics
must not require unbounded scans in the foreground write path.

## Verification Boundary

Benchmarks and integration tests are the source of truth for supported scale and
performance. Formal models cover authority, routing freshness, namespace
publication, receipt policy, quorum-aware replica fan-out and response
scheduling, repair authorization, content validation, and the rule that normal
operation does not create conflict histories. Network-session framing and body
attestation are implementation-tested rather than modeled by the DFS specs. See
the [formal verification guide](formal/cluster-verification.md).

This architecture records required behavior. Exact Rust types, storage table
names, protocol message variants, benchmark presets, and migration readers
belong in source, Rustdoc, crate documentation, and tests.
