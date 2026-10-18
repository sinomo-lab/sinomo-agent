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

## Runtime Ownership

`sinomo-dfs` owns the distributed semantic filesystem core, including the
resident authority cache, data-owner lock, services, bootstrap barrier,
maintenance scheduling, placement observers, events, and metrics. Its
component factory consumes only generic host capabilities from
`sinomo-module::ComponentContext`: cluster reads and commits, target-addressed
object RPC, runtime execution, node-resource observations, activity counters,
and component events. The host does not provide DFS-specific callbacks.

The shared product profile selects the DFS component for main and embedded
products. Shadow and disabled profiles omit the component and therefore expose
no DFS routes or background work. Component-scope teardown is the sole
process-lifetime boundary for resident DFS workers and the exclusive data-root
lease.

File-history and attachment behavior lives in separate default-enabled R1
components assembled by the same product catalog:

| Component | Owner |
|---|---|
| `module.dfs` / `sinomo-dfs` | Namespace, current and immutable revision identity, authority, conditional publication, retention/reference evidence, durability, repair and GC. |
| `module.dfs.history` / `sinomo-dfs-history` | Retention selection and configuration, capture/pin commands, history queries and restore/recover orchestration. |
| `module.dfs.attachments` / `sinomo-dfs-attachments` | Source catalogs, schemas, producer admission and quotas, streamed upload spools, and bounded reclamation. |

Core revision records hold namespaced `retention` entries and opaque,
versioned `module_metadata`. A `DfsRevisionRetentionState` carries the namespace's
ordering and optional `DfsRevisionRetentionGrant`: an immutable object snapshot,
a generic rule or explicit-operation origin, and an optional ordered minimum
count/age grant. Filesystem configuration holds namespaced `retention_policies`
with generic path rules and minimum grants. Named pins remain independent roots.
The core validates these records and preserves their durability and GC meaning
without decoding a feature's metadata payload.

The history module uses its namespace to translate source and capture provenance
into user-facing history. Other modules can retain immutable revisions under
their own namespaces without introducing a second content database or modifying
history's semantics. `sinomo-dfs-migration` alone interprets retained legacy
records and converts them into the current representation. The DFS runtime
accepts only its current storage and wire protocols; incompatible nodes must be
upgraded before participating.

Retained-revision path queries use generic derived indexes over captured grant
paths and current object families. Table operations, replay and rollback maintain
these indexes from the same durable revision records. Candidate pages retain
object/version cursor order and still validate live grants and pins; an index
entry is not retention authority. Path expansion is bounded, with paged candidate
fallbacks for deep or long paths. Historical path reuse, moves and unlinked
families remain visible under the owning module's query semantics.

Reference operation outcomes have a separate lifetime from the revisions they
protect. Full and compact transaction witnesses identify their immutable source,
and an object-local index supports bounded transfer with the source family.
Cross-shard adoption includes those outcomes and their conservative eviction
evidence in its checked acknowledgement. A move cannot make an old release act
on a later reference, or turn an expired operation into a fresh admission.
Exact replay recovers current repair obligations without recreating released
references; the requested locator stays part of the input while current object
authority selects the effect's destination.

Atomic module-metadata contributions retain their complete immutable request
for retries, including `created_at`, expected current heads and canonical paths,
and the original shard, primary and authority epoch. A generated operation ULID
fixes `created_at` to its exact UTC millisecond timestamp, so a retry cannot
refresh an expired identity's admission time. Fresh distributed execution
requires this supplied authority fence and checks the head observations through
the normal conditional publication path. An exact full or compact outcome is
checked first against the digest of the complete request. Without that proof,
a moved source, changed head or changed authority rejects the old request;
receipt-expiry admission uses the original timestamp. Contribution receipts
remain scoped to their original authoritative shard and are not transferred
with reference-operation outcomes. Local contributions use the same immutable
retry request and conditional metadata checks.

Feature components require `sinomo.dfs.semantic-filesystem/1.0.0`. Checked
component calls validate the active core owner before borrowing its one
resident service; all namespace and content effects retain the normal owner,
epoch, policy and durability checks. Feature algorithms cannot substitute
component-event handlers for atomic publication or content-reference safety.
Namespace and cluster-control events wake reconstructible attachment work;
durable namespace-feed cursors recover missed delivery and lag.
The authoritative feed also projects revision and reference changes as
`RevisionMetadataChanged` identity notifications with the same shard, epoch,
journal sequence and ordinal provenance. These notifications are shard-scoped;
each consumer resolves identity scope and final authority before reading data
or choosing cleanup. They grant no metadata, content or collection authority.
Cloud Files skips these known metadata-only events while durably advancing the
batch cursor; it continues to apply only the feed's namespace effects.

`sinomo module native-rust disable module.dfs.history` and the equivalent
`module.dfs.attachments` command apply when the daemon next constructs its
registry. Each toggle is independent. A disabled feature exposes no routes or
workers, while core create/read/overwrite/rename/unlink remains available.
Committed grants, pins, retention rules, payloads and catalogs survive
disablement. Stored retention rules remain core promises until an explicit,
authorized configuration change: ordinary writes still honor those rules, and
recorded age/count expiry and reference release still govern old grants.
Enabling again resumes feature operations over the surviving objects. Disabling
`module.dfs` leaves its dependent feature components pending. The core owns
final retention and GC decisions even while the optional policy provider is
absent.

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

A durable exact-frame replication batch can lag behind a receiver's durable
prefix or authority-acknowledgement floor. Such a stale batch remains rejected
with a typed prefix mismatch, allowing the existing authenticated snapshot-repair
path to advance catch-up. It never lowers the floor or treats an old batch as a
new authority acknowledgement. Snapshot admission retains its existing sequence,
prefix and source-authority checks.

Normal cluster operation has one authoritative visible history per object.
All visible writes serialize through the responsible owner or metadata-shard
primary. Ordinary DFS metadata and content stay off cluster KV: a healthy
primary first journals a hidden operation prepare in DFS metadata, then proves
the current assignment with a fresh acknowledgement from every configured
metadata-shard replica that durably persisted and verified that exact prepare.
The sole journal family is `PrepareOperationV7(payload)` followed by
`CommitPreparedOperationV7(digest)`. The complete payload seals namespace
changes, metadata effects, original read predicates, content receipts, intent
transitions and terminal request outcomes before authority collection. The
commit marker carries only the exact prepared digest and slot. Canonical
publication requires the complete assigned metadata participant proof, the
existing coherent configuration projection, and the actual held writer's
bound generation and durable handoff fence. An incomplete participant set
leaves the operation pending or unavailable. Content receipt threshold `W`
cannot substitute for metadata evidence, and cluster KV cannot mint a
metadata recovery permit.

KV access on DFS operation paths is explicit. Namespace routing and policy reads
reuse the existing retained configuration projections; peer routing and connection
health remain owned by the network layer. Current protocol support is checked
when a shard configuration is admitted or changed. File operations reuse that
admitted configuration instead of rereading every participant's member and
capability-policy records.

Ordinary daemon writes take one published configuration view for filesystem
policy, content advertisements and filesystem provider records. The existing
authority runtime owns that view, its committed subscriptions and its single
refresh owner. Cold bootstrap, gap recovery and migration inventories read each
DFS configuration prefix through the cluster's complete-prefix authoritative
read, which projects one native quorum-backed snapshot capture and rejects the
whole read beyond 65,536 entries or 64 MiB; see
[Control-Plane KV](clustering-architecture.md#control-plane-kv). Ordinary
prefix, value and per-delta limits are unchanged, and the retained view and its
delta admission use the complete-read bounds. Recovery retains that read's
authority certificate and replays contiguous committed deltas to one common
watermark before publishing. The complete advertised zone map remains part of
durable operation identity, including when the policy requires zero distinct
zones. Provider/content configuration changes advance authority fencing while
the physical metadata-store generation stays unchanged.

Warm write view access never fetches prefixes, hydrates KV or opens another
configuration watcher. Its narrow native floor check borrows the exact resident
committed Raft config with term/revision metadata under the existing pending
higher-term fence. Unknown ownership, a changed config identity, a higher term
or a projection gap rejects policy preparation before parent creation or file
mutation. Unrelated resident KV progress does not by itself invalidate a
configuration whose committed subscription remains contiguous. Genuine global
consistency, configuration administration and operation-bound recovery retain
their existing fresh KV barriers. Native single-node bootstrap with canonical
known absence of a committed Raft config retains its existing configuration
path only after a verified native single-local-voter certificate at pristine KV
revision zero anchors canonical resident absence, local leadership, term and
applied index. That anchor survives this runtime's subsequent initialization;
any observed committed config disables it. A restarted, nonpristine configless
runtime cannot infer bootstrap from absent data or discovery roles. A cold or
unknown resident floor never substitutes for authority. Configuration
invalidation while refresh is active latches one replacement request; the
existing refresh owner's release reserves that successor before notifying
waiters. Cancellation retains the owner reservation until its bootstrap token
has been released, so incomplete buffering cannot strand readiness. Foreground
waiters follow the active handoff within their original deadline; a terminal
refresh failure does not start another retry. A pending bootstrap completed by
a committed delta wakes the same waiters.

Browser writes join configuration readiness before acquiring execution or
backend admission. If configuration is withdrawn while the write is queued, a
typed preexecution result returns the unchanged request before parent creation
or intent publication. Bounded replanning releases the worker and admission
before joining the same refresh owner. A ready write still consumes its
published view without issuing KV reads. Native floor failures remain errors.
Committed-delta warnings describe an actual transition from a retained proof
to unavailable admission; subsequent deltas while that proof is missing do not
repeat the warning or claim another revision gap.

Retained authority observes both contiguous committed deltas and replacement
snapshot notifications. A newer snapshot whose DFS prefix differs withdraws the
old admission proof and schedules the existing bounded authoritative refresh;
observational snapshots never grant authority. Unrelated, identical and older
snapshot notifications do not reopen metadata or schedule recovery. This keeps
imported configuration changes from stranding replicas on an old shard epoch.

Routed request cursors describe the requester's metadata projection. Authorized
replica journal ingestion and snapshot replacement publish a process-local
progress hint shared by backend handles for the same durable root. Before a
write or namespace request advertises its cursor, it consumes new progress by
checking the current shard authority and pinning an exact durable prefix. The
cursor, namespace job guards and cached object invalidation are replaced together
from that pinned view. A notification or a higher sequence alone never authorizes
cursor advancement. Genuine divergent or unverifiable replay retains snapshot
recovery; normal request completion should not initiate full journal backfill.

Foreground replica ACKs also update shared, node-local delivery hints for the
background publisher. These hints select journal suffixes and can retire an
already-delivered batch; they never replace fresh authority nonces or prefix
probes. If a prefix-mismatch rejection belongs to a plan whose source head has
since advanced, the publisher retains its last checkpointed cursor and prepares
fresh work. It does not mistake the obsolete plan for proof of replica damage.
Rejections against an unchanged source retain the existing snapshot-repair path.

Genuine cluster-wide configuration decisions retain existing authoritative KV
reads and atomic transactions: placement policy changes, configuration CAS and
proof-carrying handoff activation. Ordinary metadata publication and sync use
retained projections and DFS-owned participant evidence. They do not request
per-operation KV authority, degraded permits, floors, or watermarks. The `dfs_implicit_kv` Dylint
rule rejects unapproved access through normal service/POSIX/browser entrypoints
and local helper calls. Narrow configuration and consistency boundary markers
record why each intentional use is required; see
[the lint contract](../lints/sinomo-architecture/README.md).

Every current-content publication compares the visible head against the write
intent's original base version under the metadata writer lock. Delayed receipt
completion uses the same prepared publication as immediate completion. A fresh
head lookup cannot rebase an already prepared write. Exact committed retries may
repair matching locations, but cannot republish their old object image.

Namespace planners retain the object versions and path occupants they observed
before constructing a mutation. These original predicates accompany the hidden
prepare and are checked again under its publication writer lock. Namespace
transactions use this same prepare/commit family. Separate saga phases and
reentrant requests collect separate observations. A later phase may deliberately
read a committed successor, while records already planned by its caller retain
their original predicates. Missing-replica pruning is a conditional update to
locations and matching receipts, never a whole-object replacement.

Durable hardlink alias frames retain their original physical shard, object
versions and path occupants before yielding to the canonical shard. Resuming or
reopening the job uses those same comparisons. An observed absence is distinct
from a missing observation; neither can be replaced with a fresh parent version.
The same saga applies when distinct physical shards share one primary.

A terminal write outcome cannot be reopened by a delayed prepare or contradicted
by repair cancellation. Current-primary recovery compares the exact observed
intent and object atomically: a published target is committed, and a pending
write whose original base has been replaced is aborted. Missing objects alone
retain the existing recovery grace period. Historical versions remain available
to explicit history operations; they never replace the current head implicitly.

Cluster KV retains its atomic transactional API and orders cluster
configuration and authority changes. DFS does not add caller bindings,
caller sessions, historical replay APIs, or substitute KV operation ledgers.
An ambiguous configuration transaction is reconciled from its existing native
outcome and exact guarded configuration state; DFS then resumes its own durable
shard protocol. Paths, intents, receipts, namespace transactions, journals,
snapshots, object metadata, bytes, repair queues, and metadata acknowledgement
floors remain exclusively in DFS. Historical global metadata floors and
watermarks may be interpreted only by migration to preserve old obligations;
they do not authorize a current ordinary write.

Hosted browser operations that share the namespace service wait in a FIFO async
queue before occupying a blocking worker. Existing per-route concurrency and
handler deadlines bound the queue. The executing blocking operation owns the
admission permit until it finishes, including when its caller is cancelled.
Immutable node identity and independent placement observations bypass this
queue. Health inspection runs on a blocking worker under the same admission as
other namespace reads. Direct backend callers retain their short lock bound.

Normal clustered FileManager clients delegate supported operations to the
checked main daemon's resident browser service. The direct engine is initialized
lazily for standalone and specialized operations whose contracts the browser
wire API cannot express. Owner identity, operation deadlines and ambiguous
mutation delivery remain checked; a selected owner's failure does not silently
redirect normal operations to a local writer.

Native clients that delegate namespace authority also send current, retained
and exclusive content publication to that resident Main when the local node is
the metadata primary. Byte, manifest and reader inputs keep their original
operation identity and source custody. Main owns admission and completed-handoff
rebind; replies return to the original caller's registered inbox.

An immutable blob fetch may use content-read admission only after a guarded
read of a currently certified, fully materialized shard verifies its original
intent, requester and local-copy permissions. The process-local result seals
the request and opened reader; the response retains content and data-owner
leases through delivery. Unproven, legacy and additional metadata-primary
permissions retain the globally guarded path. Transfer responses and other
metadata mutations keep their existing admission and authority checks.

Routine placement maintenance uses asynchronous peer dispatch and response
waiting. A foreground request can cancel that local wait; the complete
backend-bearing maintenance future is dropped before releasing global
admission. The placement session restores its deadline and response slot,
without undoing a remote effect or treating uncertain delivery as rejection.
A later pass revalidates the durable job and object; only the existing page CAS
advances progress. Local work remains bounded by the existing object budget,
and continuous preemption may defer placement completion.

Regular authoritative reconciliation checkpoints recovered repair obligations
before sharing routine maintenance's deferred replica transport. Payload
dispatch begins after global admission is released. Explicit content GC,
reservation and commit-promotion recovery retain their synchronous completion
contract; the existing queue checkpoint and outcome rules still govern retries.

Read admission remains bounded independently of an admitted content fetch.
Daemon-backed VFS readers treat browser-busy and route-saturation replies
as read-only backpressure: they may retry admission for up to three seconds,
with each attempt receiving only the remaining original RPC timeout. They do
not replay mutations or turn an unavailable, conflicted, or failed read into
namespace absence. The compatibility classifier for existing diagnostic replies
lives with the shared browser API contract.

Reconfiguration commitment is not itself authority activation. A direct shard
CAS cannot activate a changed configuration. A proof-carrying handoff may
either change membership while retaining the primary, or rotate the primary to
an existing replica while preserving the exact member ring; mixed primary and
membership changes are rejected. Either form advances exactly one epoch and
must preserve every accepted DFS-owned prepare, commit and acknowledgement
floor through the exact retiring participant prefix. The resulting pending handoff is not itself a retiring-primary
fence because an isolated primary may not have
observed it and may still complete a valid write. Once the retiring primary
reconnects, it takes the same publication guard used by canonical commits,
installs a durable shard-local backend write freeze, and acknowledges the exact
journal head and fingerprint captured under that guard. A pending freeze
survives process restart and blocks every authoritative metadata-table mutation
while still permitting replay, snapshot inspection, and handoff transfer.

Activation transfers that immutable fenced prefix across the union of the
retiring and desired metadata assignments and requires a joint-consensus
certificate: a majority of each assignment plus explicit acknowledgements from
both the retiring and desired primaries. For an ordinary membership handoff,
the completion proof must equal the retiring primary's fence and may not
precede any independently retained DFS participant acknowledgement obligation.
A completed assignment is persistent configuration. Default reconciliation
validates its scope and current authorized members and preserves its primary
and replica ring. Discovery supplies initial defaults; only the exact initial
singleton may expand after its native genesis commits. Later role changes and
member growth use the explicit guarded handoff protocol, so discovery cannot
silently undo an operator's completed handoff.
A storage import separately binds the original source fence and ACK clock,
the converted candidate's native target prefix, and its semantic image
fingerprint. Those three domains are not interchangeable. This permits a
configured replica to be offline during a fenced configuration handoff while
ensuring the successor starts with a durable majority and neither side of a
reconfiguration can activate alone. For
same-primary membership changes, the completing backend instance
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
local fence. Ordinary full-shard writes remain participant-scoped. An
incomplete configured participant proof does not advance visibility or sync
through a global KV substitute. A whole-root installation must carry the
existing real native generation fence into the candidate before exposing it;
retained handles and lagging projections cannot regain write authority.

Membership-handoff synchronization is therefore a joint-quorum operation, not
an ordinary publication barrier. A majority of the union alone is insufficient:
a desired-only quorum is not valid evidence for a retiring-epoch prefix, and a
retiring-only quorum does not make the successor durable. Progress stays pending and
retry-enabled until both assignment majorities and both primaries acknowledge,
or the bounded attempt times out without activation. A timed-out attempt
returns to durable pending state, retains the retiring write fence, and is
eligible for prompt rediscovery and retry. Missing desired replicas catch up
through normal replica maintenance after activation; missing retiring-only
replicas remain fenced by the committed successor epoch when they return.

DFS component maintenance discovery includes a pending handoff only on its
retiring primary. This allows a handoff committed immediately after one
maintenance pass to request the prompt maintenance cadence instead of idling
until the long periodic tick. A pending handoff owned by another node does not
schedule a local coordinator. Triggered reconciliation retains the same
write-fence and joint- quorum activation rules; the trigger changes scheduling,
not authority.

The resident SIFS bootstrap owner performs the completed-generation rebind check
before its first authoritative namespace mutation. Delegating clients seed only
their filesystem projection; Main owns bootstrap completion and retry. This
closes the case where cluster activation and the durable marker survive but a
fresh owner has no process-local authority binding. The marker remains immutable
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

Journal batches attached to routed reads and write responses use that same
replica observation path. After durable replay, the receiver updates namespace
guards and its replay cursor. It also refreshes affected visible objects from
the final backend view to retire a routed writer's optimistic create metadata.
Reconstructing a primary's intent coordinator on a replica must not turn a
historical intent conflict into a locally authored rejection. Such a write
would fork an unfenced replica's journal or fail against a retired primary's
write freeze, even though the authoritative read itself succeeded.

A routed requester waiting for snapshot repair must observe installations made
by another resident service over the same shard. A successful, authorized
installation can notify that requester, but the notification grants no authority.
The requester revalidates the assignment and reads the exact durable prefix and
namespace guards from one coherent shard view before advancing its cursor. It
also retires stale cached object projections without discarding unresolved write
ownership. Sending a repair request, an ordinary checkpoint, or a failed install
cannot advance the cursor; incoming retained journal entries still undergo exact
fork checks. This reconciliation is triggered by repair, not by a metadata scan
on every healthy request.

Replica materialization enters the existing shard admission queue as soon as
journal input is durable. Ingress registers its work before releasing producer
admission. Once the materializer owns that shard gate it claims the queued input
edges and replays their accepted journal prefix; later appends enqueue new work.
There is no quiet-period timer or traffic-based postponement. The worker resolves
the current generation before replay and retains its data owner and mutation
guards throughout blocking work, including cancellation and authority rollover.

Restart hydration leaves a non-terminal current-write intent out of the
in-memory coordinator when its base and target both differ from the pinned
visible object version. The superseded record remains durable for normal
authority-owned intent reconciliation; it cannot abort mount or rebind, gain
publication authority, or prevent unrelated current-base work from recovering.

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

Each logical floor has its own checksummed local append log and current format
pointer. Conversion of old floor sidecars belongs to `sinomo-dfs-migration`;
normal acknowledgement never interprets an old representation. Every update,
including an equal or older retry after a failed sync, synchronizes the current
log and its parent before replying. The log is checkpointed at 128 records;
replacement preserves the latest complete floor. Missing migrated logs and
complete invalid records fail closed. Incomplete EOF recovery uses the storage
layer's normal fsync crash contract: an acknowledged record survives the crash,
while an unfinished successor may be discarded. None of these local records is
appended to the canonical replicated journal. Each acknowledgement replays and
validates its bounded floor log through the storage layer's sequential record
reader. The reader samples the locked file's extent once and batches reads;
frame boundaries advance its local offset without per-record stat/seek calls. It
retains no evidence across acknowledgements, and every retry still performs the
required file and directory durability barriers.

Authored journal admission retains uncertainty at the canonical data-root owner,
across replacement of a resident backend handle. A failed append can leave
complete frames even when its sequence allocator did not advance. Recovery
validates complete frames needed beyond the exact checkpoint, truncates only
incomplete EOF framing, and may skip older segments wholly covered by the
self-contained checkpoint. It synchronizes the surviving journal and checkpoint
sources plus their parent directories, and restores the exact prefix before
authoring or acknowledging a retry. A writable reopen without a current owner
certificate performs this certification; decoding readable bytes alone is
insufficient. Certificates bind
the shard and physical file generation, so snapshot replacement, reset, aliases,
and rollover cannot reuse a certificate for another source. This complements the
existing writer flock and does not add independent multi-process allocation.
An owned table append or checkpoint updates the certificate under that same
writer lock; it preserves a stronger certified journal suffix when table
materialization is deferred. A head or cursor hint cannot extend the certified
history, and retained acknowledgement floors still fence source rollback.

Current publication has one durable prepare and one digest-only commit marker.
A create seals the parent and child records, visible version, exact content
receipts and terminal intent in the same immutable payload. Receipt staging,
repair metadata, request admission and GC transitions use metadata effects in
that same family. The materializer expands the committed body once; recovery
replays these same two record kinds.

`DfsPublicationGuard` is opaque and can be created only by the authorization
boundary. It retains the actual data owner, admitted effect-shard execution,
current authority evidence, handoff exclusion and physical writer session until
publication finishes. A digest, content receipt, routing hint or held backend
cannot manufacture this guard. Exact committed retries return retained evidence
without authoring another commit or replacing a newer visible head.

Storage conversion runs incrementally at cold open through a provider owned by
the normal Main or Embedded component. `sinomo-dfs-migration` decodes original
source formats and verifies their original digests before normalization, retains
source archives and restart progress, and stages a current image without exposing
it. Native source and target authority must certify installation and retirement.
The DFS crate supplies current staging and admission interfaces, with no old
reader, old journal family or mixed-version writer path.

The commit path must preserve these properties:

1. The writer names the base and target version.
2. The authority validates its epoch, policy, and base version.
3. Content is durable on the nodes required by policy.
4. Matching receipts exist before the visible version advances.
5. The exact complete operation prepare is durable on every configured
   metadata-shard participant; incomplete evidence remains pending or fails
   without a KV metadata permit or lowered count.
6. The metadata commit revalidates the typed authorization and compares the
   current version, path occupants, and write intent
   atomically.
7. Replicas and caches may lag, but they cannot become write authorities.

A stale-base writer retries or fails. A write that cannot reach authority stays
pending or fails according to policy; it does not create a second visible
history. The current canonical DFS has no conflict-record path. Conflict
classification belongs only to the unimplemented local-first extension
described later in this document.

## Layered Model

| Layer | Authority and responsibility |
|---|---|
| Cluster KV | Filesystem configuration, metadata-shard placement, primary epochs, compact policies, service discovery, and proof-carrying configuration handoff records. |
| Metadata shards | Paths, object identity, versions, write intents, receipts, namespace transactions, durable jobs, and journals. |
| Content stores | Immutable blobs, extent manifests, staging data, local quotas, and garbage collection. |
| Network data plane | Bounded metadata batches, manifests, ranges, chunks, receipts, and repair traffic. |
| Daemon maintenance | Journal catch-up, namespace-job resume, anti-entropy, scrub, and lifecycle work. |

The cluster KV is small enough to replicate to every control-plane participant.
Per-path and per-version state remains in metadata shards, and bytes remain in
content stores. This keeps large namespaces and objects out of Raft while using
the existing cluster consensus to order configuration and authority changes.
DFS participant journals, durable fences and acknowledgement floors preserve
the exact metadata prefixes across those transitions.

Local content reservations are durable roots until an authoritative catalog
positively covers their exact object/version, or a later complete root cut
proves absence after the rejection grace. Before requesting its first primary
page, the catalog collector advances a durable content-store scan ordinal
under the reservation flock. Every reservation publication or renewal records
the current ordinal and store incarnation in its existing durable record.
Negative reconciliation requires a matching incarnation and a reservation
ordinal strictly below the original catalog cut. Page continuations and delayed
catalog installation never advance that cut; timestamps measure freshness and
grace, not causality. Missing or conflicting legacy provenance retains roots.
Catalogs without a local cut can release positively covered reservations but
cannot authorize absence aging or destructive GC.

The current reservation sentinel (format 3, `inline-bindings-v2` layout) gates
this local persistence contract and its supported content-binding layout.
Existing handles reread it after taking the cross-process lock; older writers
and unknown future layouts fail closed. The migration provider preserves old
reservations and their original provenance while converting storage. Current DFS
opens only the selected current layout; missing source provenance cannot
authorize destructive collection.

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

Sender replication cursors and local materialization cursors are also recovery
hints. Sender checkpoints are atomically replaced without forcing disk durability;
missing or malformed hints restart catch-up conservatively and never supply an
acknowledgement. Foreground authority ACKs advance only the service's in-memory
cursor map, which survives lazy shell rebuilds. The independent journal publisher
checkpoints its own verified responses. Losing a shell or restarting can resend
from an older checkpoint; retained-history gaps still require exact-prefix or
snapshot recovery, and persisted status may lag until background progress.
Default-primary transfer selection registers its exact target and local-head
hint dependency with the retained reconciliation owner before reading the
checkpoint. A matching background checkpoint or already-published hint wakes
that selector once, including after its caller is cancelled; ordinary updates
without a waiting selector do not wake default reconciliation. Semantic placement
changes discard stale dependencies. This only retries selection: the retiring
freeze and exact prefix/handoff certificates still authorize activation.
Live materialization starts from its validated, durable table
prefix and checkpoints its hint in batches instead of rewriting it before every
replay. The exact journal/table proof remains the recovery authority. Prefix and
operation acknowledgement floors remain durable before their responses; these
floors are not interchangeable with progress hints.

Materialized-table checkpointing is not part of the foreground mutation cost.
Crossing the table-log threshold schedules one process-global-deduplicated
worker per shard root. Under the metadata writer lock, the worker rotates the
active table log through exact boundary `S` into an immutable checkpoint
generation and creates a new active generation for later writes. It releases
the lock while compacting and serializing the immutable generation, then
reacquires the lock to atomically publish the snapshot and retire the rotated
generation. Reopen ignores table-log records at or below the installed
snapshot sequence. A crash after snapshot publication but before retiring the
rotated generation therefore replays neither the superseded prefix nor a
partial suffix; a crash before publication leaves the old snapshot, rotated
generation, and active suffix authoritative.

Current checkpoints use snapshot storage schema 10 and checksummed streamed SOBJ
records. The normal DFS reader accepts this schema only. The operation journal
uses the independent V7 prepare/commit family.

`sinomo-dfs-migration` owns older snapshot, journal, content and local-state
decoders. Its cold-open provider advances bounded conversion and verification
cursors on demand. Original bytes and digests are retained before adapting
source records. Output is synchronized before progress; restart discards only
an uncheckpointed staging suffix. Installation compares the captured source
generation and publishes a current candidate through native authority. Old
records and proofs are conversion data and cannot grant publication authority.
Migration validates original native9 alias digests with their released encoding.
An old frame that omits required original comparisons remains unavailable until
source custody can establish them; a current table lookup cannot supply them.
An incompatible daemon must be upgraded before joining the current execution
path. See [storage migrations](developer/storage-migrations.md) for shared
storage ownership and durable progress contracts.

Only after the snapshot and newer table-log suffix are durable may checkpoint
maintenance prune journal segments strictly older than `S`. It always retains
the segment containing `S`. A replica behind the retained history consequently
observes a non-contiguous first entry and enters authoritative snapshot repair;
an empty retained suffix can never be mistaken for successful catch-up.

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

### File attachments

`sinomo-dfs-attachments` stores file-associated data as independently versioned
DFS objects with a bounded typed header and range-readable payload.
Identity-based, bucketed paths provide direct lookup and paged listing without
expanding core object metadata or putting per-file state in cluster KV. Object
annotations and version-bound derivations have distinct freshness semantics. See
the [attachment contract](developer/file-attachments.md) for storage, lifecycle,
bounds, module integration, tags/transcripts, and thumbnail persistence.

### Explicit File History

`sinomo-dfs-history` owns the content-history policy and user-visible history
operations. The default policy captures nothing automatically. Named pins
retain explicitly captured versions. Normal generation changes do not create
user-visible history, and rejected or uncertain writes never preserve alternate
versions as a substitute for consistency.
Configured subtree retention is an explicit rule: it captures outgoing content
atomically on successful replacement or deletion. Count and age grants combine
with independent named pins. Count uses authoritative per-object capture order;
age starts at capture. Policy changes cannot shorten a recorded minimum grant.
Expired, unpinned captures become unavailable to history queries before physical
GC collects them. History follows canonical object identity across renames and
hardlinks. Deleted objects remain discoverable by their captured paths, and
path reuse does not combine distinct files.

Historical content shares the ordinary immutable content store, metadata shard,
receipt, and repair mechanisms. Compact retention configuration belongs in
cluster KV; version records, pins, jobs, and content remain DFS-owned. Explicit
preservation of local-operation bytes uses that same retained-version model
without changing the canonical head or taking its pending-writer slot.

Each retained version preserves its data-write durability plan: source
generation, coordinator provenance, desired replicas, and W/N policy. Metadata
edits and retry-record collection do not discard this guarantee. When a
current visible head has no recorded plan, capture establishes a fresh grant
under the current filesystem policy, verifying source content and obtaining W
durable replica acknowledgements before publication. Surviving locations alone
never establish N. Recovering plans for older retained versions belongs to
`sinomo-dfs-migration`; the core rejects a historical version without a
complete plan until conversion supplies one.

Capture commits its repair owner atomically with retained provenance unless a
matching ordinary write already owns the obligation. The current metadata
primary owns unfinished N-W repair across restart and primary transfer, including
deleted objects. A retained repair owner is maintenance evidence, not an
independent retention root; GC removes it when its version is collected.

Restore is a new conditional write or exclusive create under current authority,
permissions, and placement policy. Its durable job owns source protection,
stable operation identity, result replay, and cleanup across interruption.
Cancellation stops an executor without revoking an admitted job or releasing
its source pin. Exact retries return the recorded result even after later edits.
Single-file write success requires both W and canonical publication; background
N-W repair cannot override a later committed head.

Public exact-version reads resolve current authority and require the current
identity or a live retention grant. A leftover blob alone does not grant access
to history. Historical reads use bounded ranges and distinguish expired or
absent retention from unavailable content. History mutation admission requires
`dfs.metadata.prepared-operation.v7` across all assigned metadata peers. The
migration crate converts older packed records before current DFS opens them.
Incompatible nodes must be upgraded.

See the [file history guide](user/file-history.md) and
[API reference](developer/dfs-file-history.md) for commands and contracts, and
the [distributed verifier](../docker/README.md#file-history-acceptance) for
restart and primary-handoff checks.

### Immutable Content Storage

Every storage-capable node may host a local content store. Content identity uses
BLAKE3 and immutable manifests. Bytes enter a staging area, are verified, and
become visible to metadata only after the relevant durability policy is met.

Small contiguous objects may use a whole-blob fast path. Large or patched
objects use extent manifests, bounded streaming I/O, and range reads. Retained
packed-blob segments from older layouts are read only by
`sinomo-dfs-migration` during content conversion.

Ordinary whole-content writes of at most 16 KiB use the local inline binding
layout after cold activation. One checksummed storage frame contains the exact
object/version manifest, payload, publication age, and causal reservation stamp.
The append's synchronization barrier precedes its durable receipt. Shared hashes
may reference a previously certified payload in the same selected generation;
small blob-only staging combines its payload and exact reservation in one frame
while continuing to omit public version manifests. Compaction preserves that
absence; only explicit publication adds the version entry. Already-materialized
values with an expected hash validate it before entering the same inline writer,
without first creating a private quarantine file. Streamed quarantine
still performs its existing private-stage synchronization before a bounded small
payload can be adopted into the binding log. Large and extent representations
retain their existing paths.

The mandatory current sentinel selects this layout. Cold open first runs the
component-owned migration provider, then opens only current storage before
publishing routes or starting workers. Fresh initialization holds the content
owner and persists an initialization intent bound to the physical root; restart
completes that same initialization before publishing the current sentinel.
Inline entries and explicit deletion/release records form the current projection.
There is no legacy read fallback inside the DFS content store.

The current catalog selects a generation of bounded checksummed segments. Sealed
segments carry durable lengths and digests; an incomplete active suffix can be
truncated during writer recovery, while complete corruption fails closed. A new
writer synchronizes surviving uncertain frames and recertifies the catalog's
namespace before granting a receipt. This uses the normal filesystem fsync crash
model, not a promise against arbitrary post-acknowledgement media rollback.

Stats, quota admission, scrubbing, reservation reconciliation, and GC consume
the same current projection. Logical deletion becomes durable before old files
are reclaimed. Compaction copies exact payloads, manifests, ages, reservation
states, and tombstones into a private generation in bounded batches, verifies
the copy, and publishes its catalog last. Foreground changes supersede an
unfinished copy. This initial compactor requires a quiet interval spanning its
bounded copy and verification steps; continuous writes can postpone reclamation,
although segment rotation remains bounded. It does not claim guaranteed
compaction progress under unending mutations. Readers retain exact locations and
generation reclamation pins; retiring a logical entry cannot invalidate a lazy
reader's unopened extents. Old generation files are reclaimed in bounded passes
only after those readers drain.

Content-store durability does not grant namespace authority. A node can possess
the right bytes while its receipt is stale, its metadata view is behind, or its
policy no longer permits serving them.

## Placement

The planner receives candidate nodes from its caller, prefers the coordinator
where required, and uses stable weighted hashing to select a deterministic set
for the object and version. The replication policy supplies the minimum commit
and desired replica counts. Fresh placement snapshots rank storage health, path
state, network metering, coarse node resource pressure, path performance, and
capacity. Current replicas rank ahead of resource pressure so a transient low
battery state does not create evacuation traffic; required nodes and manual pins
remain hard constraints. Exact battery and thermal readings never leave their
node.

For new-write placement, a failed directed endpoint probe leaves node-level
delivery unknown. Networking may still reuse an authenticated duplex carrier
or select a relay route. DFS must not turn that endpoint failure into a replica
exclusion before routing runs; storage admission, configured replica counts,
and durable content receipts continue to gate the write.

Session locality, privacy, and trust-domain inputs still belong in upstream
eligibility. Bringing every input into one auditable placement policy is tracked
in the [DFS policy and authority plan](plans/dfs-policy-and-authority-hardening.md).

Read caches remain distinct from durable receipts and never become write
authorities merely because they possess current bytes. If the minimum durability
threshold cannot be met, the write remains pending or fails.

Local-owned and ephemeral-local rules default omitted replica counts to one,
regardless of the candidate pool. Reconciliation preserves the object's existing
owner and cached or pinned copies; it cannot substitute a healthier node or use
generic replica repair to restore a missing owner. Converting an existing object
into or between local modes requires an explicit conversion protocol and is
rejected before effects by the current reconciler. Legacy managed repair intents
for a local policy are fenced without retracting an already committed outcome.

A placement page can skip an unchanged local object only after certifying its
current metadata-shard authority and rereading the exact object. Remote placement
calls retain their authoritative response without requester journal replay. Every
page still rechecks placement configuration and compares the durable job revision
before publishing progress.

## Write Protocols

Placement preview tokens bind the reviewed configuration, path/object/version,
selected replica and commit plan, typed selector eligibility, candidate identity
and hard constraints, reachability/compatibility diagnostics, and exact scoped
metadata/impact digest. The existing bounded impact pass uses the same snapshot
and per-object selector as reconciliation, binding each descendant's ordered
plan, eligibility, resolved constraints and pins in that digest. Raw capacity,
latency, throughput and freshness samples, snapshot counters/timestamps and
display explanations remain diagnostics. Apply still recomputes the entire plan
and impact before comparing the authorization fingerprint; a changed plan,
eligibility, configuration or scoped metadata rejects. The selector owns
eligibility, and old decision payloads default false rather than granting it.
Changed token material makes pre-upgrade previews stale safely.

Placement Apply compares configuration, job, queue, activity and mutation-permit
state atomically. A false compare can reflect unrelated background work rather
than a changed policy. The rejecting boundary marks a false compare or a known
pre-transaction live-permit conflict as definitely unpublished. A client owning
the proposal can refresh its preview and retry within its existing bounds.
Transport, quorum and post-commit errors retain ambiguous-outcome handling;
neither placement updates nor ordinary writes are blindly retried after them.

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
   participant. If a member is unavailable, leave publication pending or fail
   within the existing budget; preserve the participant count and DFS-owned
   evidence.
8. Atomically compare the authority epoch, base version, intent state, and
   receipt threshold.
9. Advance the visible version and schedule any remaining repair.
10. Publish metadata progress to caches and interested sessions.

The durable write intent lets retry and recovery determine whether work is still
open, committed, pending repair, or aborted without inferring state from an
in-memory queue.

When the primary has a placement snapshot, its planner owns the complete replica
set, including required nodes and content pins. The write path does not append
the historical object owner again or widen W/N after that decision. Legacy
explicit plans retain their compatibility normalization when no snapshot exists.

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

Private mounted/browser transports inside the same DFS runtime register exact
outstanding routed-write response identities with that runtime's bounded
message-delivery owner. Canonical request and stable intent aliases are installed
before dispatch. Authenticated ingress transfers matching responses to the
owning transport's independent inbox and wakes its waiter before taking resident
service admission; it never invokes the browser service under a resident lock.
The receiving service still validates the source, object, current authority and
journal evidence. Queue admission or transport acknowledgement is delivery
custody, not proof that a write completed. A full inbox rejects delivery so the
existing sender retains its response. Cancellation preserves only the existing
bounded late-response identities; pruning, abandonment and transport/runtime
shutdown release registrations without retaining write bodies or services.
After an active wait ends, a frame delivered to a retained private inbox is
processed on that service's next use; no new background consumer is created.
The primary already owns commit, repair and promotion, independently of this
requester's local manifest processing.
Standalone transports in another process retain their existing delivery path.

A delayed promotion can be acknowledged without changing the current head when
its exact committed intent, visible immutable version, content hash, local
receipt, and available bytes still agree. A newer head alone is not evidence
that an older notification completed; absent or mismatched durable evidence
retains the normal base-version fence.

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
Placement repair intents are excluded: `RepairReplica` already commits their
target binding and metadata before acknowledging success; they have no separate
staged-write promotion obligation. Ordinary staged writes retain promotion
acknowledgement and retry recovery.
Shared pending and settled indexes prevent the read-lane pool or periodic
hydration from duplicating the same identity. Completed routed-write responses
for a caller other than the request currently attached to a served transport
use the same replay-safe detached boundary; the current caller's response stays
inline, and unrelated requester latency does not hold the service lane. The
abstract formal `responsePublished` step refines the
response-construction/enqueue boundary,
not proof that client bytes reached the wire before the independent worker ran.
Once every desired replica is durable, the write has no repair jobs, transfer
reservations, retry or attempt records, or retained pending-write state. Queued
promotion remains independent post-response work.

`dfs repair` is read-only inspection. An administrator may explicitly request
`dfs repair retry [--target NODE_ID]` to reconstruct repair work from the
resident owner's authoritative metadata when historical intents lack durable
retry records. This requests one coalesced inventory reconciliation, after which
the normal bounded repair worker retains authority, exact-version and content
verification checks. The response confirms the reconciliation request, not
completed replicas; `dfs status --cluster` reports the remaining work. The
operation has no offline metadata-root override and is unavailable on a shadow
or a host without the resident DFS owner.
Committed repair sender state is reconstructed on the current metadata primary,
including after a shard handoff. The original write coordinator and request
identity remain durable history; the retired coordinator does not regain repair
authority merely because it originally accepted the write.
Compacted terminal write identities remain visible in object history for exact
retries. They are excluded from repair inference, just as they are excluded from
the recovery inventory; their historical replica list is not a live repair
obligation. Full committed intents continue to report missing matching receipts.

A placement change installs its new current repair contract before compacting
replaced committed intents into exact retry receipts. Compaction uses bounded
object/version index ranges and the existing journal batch mechanism. Explicitly
retained versions keep their independent original durability owner, and their
required locations remain protected from current-placement pruning. Metadata GC
keeps a managed repair contract while it owns the current content; a later
placement or a different data generation ends that ownership.

Legacy repair records can predate this durable ownership handoff. Repair
execution first checks the existing filesystem projection and active intent
index for a possible placement conflict. Ordinary current-policy repairs do not
read placement history. When recovery needs a superseding decision, it loads a
bounded recent placement window once for the pass and uses the existing planner.
Only a still-authoritative completed plan may finish that old obligation; a
running plan retains its transition. Every replacement rechecks the job and
configuration under the normal placement mutation permit, then records the
terminal receipt before retiring volatile repair state. Missing or stale
evidence does not authorize suppression of a repair.

The serial metadata portion of one hot shard is expected to occupy roughly one
core because its primary orders that shard's namespace and version decisions.
When initial placement distributes root, services, and users across three
primaries, they can use about three aggregate metadata cores under independent
lane writes. The committed assignments determine that distribution. More host
parallelism comes from additional shards and concurrent
writes, plus content hashing, replica transfer, and transport work; one metadata
shard is not designed to consume every host CPU.

Daemon object-RPC ingress remains Tokio-concurrent, while the synchronous DFS
state machine runs on admitted blocking workers. Direct journal and snapshot
commands carry an exact filesystem/shard authority identity. Single-shard
namespace mutations carry the receiver-validated complete authority assignment
for their path set. Once the cache has a proven-current topology generation,
these operations reserve only that shard and use an independent service lane.
An exact request reserves its shard before awaiting the global read fence. A
same-shard backlog therefore holds no global read guards, and a queued global
writer waits only for currently active exact owners. Neither kind of waiter
consumes a blocking worker. The admission guard moves into the non-cancellable
blocking closure, so disconnecting a caller cannot release ownership while its
mutation is still running.

A configuration transition during lane selection is a typed pre-execution
replan, including transitions after the first preflight fence or while an
admitted resident backend is selected. The original request and authority epoch
remain intact. Message, direct replica, publication and maintenance owners
release their admission and backend-bearing guards before consuming the current
retained projection and trying at most two replans. Ready successor projections
require no new ReadIndex. Actual backend, decoding and post-execution failures
remain terminal; this path never retries service effects. Expected generation,
current generation and current readiness accompany the stale-preflight
diagnostic.

The lanes borrow one resident metadata generation. That generation owns exactly
one live authority-bearing backend instance per shard. An exact lane checks out
only its shard for one request; global compatibility and maintenance work
temporarily reconstruct the sharded router from those same instances in
canonical shard order. Dropping either lease returns the instances without
reopen or recovery. Physical store lifecycle has its own generation, separate
from the complete configuration projection and exact authority fence. Committed
placement/history/display changes and handoff proof updates apply to
the retained projection immediately and continue to fence authority, while
retaining the same physical stores, service lanes and repair ownership. Their
control-plane clients consume the new projection on their next configuration
read. Filesystem creation/removal/identity or owner changes, shard configuration
changes, and invalid structural configuration retire the store generation.
Replacement requires global admission after all exact leases have drained; a
missing or stale exact stamp falls back to the global lane for structured service
validation. Revision gaps withdraw authority readiness until bounded ReadIndex
refresh; retaining a store never turns a stale projection into authority.

Routine stale-intent discovery spends its maintenance budget on visited physical
intent-order records, including terminal, missing, and ownership-rejected rows.
One call enumerates at most one shard and one bounded page, with one lookahead;
empty shards advance without opening the next shard in that call. Each shard's
sweep captures an upper key so continuing appends cannot indefinitely delay later
shards. The public inventory APIs retain their existing complete/filtering
semantics; a backend without the specialized bounded primitive fails explicitly.
This enumeration bound follows required backend open/snapshot-freshness work and
does not bound canonical recovery or ownership point reads.

The advisory continuation lives in the daemon cache generation and is shared by
replaceable maintenance service shells. A completed page advances it before
fallible candidate recovery; preemption, lazy reset, or destruction of a shell
therefore does not repeatedly restart the same terminal-history prefix. EOF wraps
the scan so failed/skipped candidates and records inserted before the cursor are
revisited. Cache/topology replacement safely rewinds it. `scan_has_more` requests
another pass after releasing global admission; the cursor neither grants recovery
authority nor proves a durable operation complete. Recovery still rechecks exact
current records, physical uniqueness, ownership, W receipts and publication CAS.

Fully addressed content reads do not initialize metadata and bypass mutation
admission. Metadata-assisted reads reuse the resident global lane instead of
opening a second inspection backend. A single-shard namespace lane owns its
durable request, prepare, retry, and replay state; packed local-eventual batches
for disjoint shard primaries therefore do not share the primary service
mutex. Coordinator-bearing distributed writes and their response/repair state
remain globally admitted until every related message carries a complete shard
identity. Running cross-shard namespace guards are shared across every shell in
the resident generation, while multi-shard namespace operations retain global
admission.

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

Every mutating routed request is first recorded on its exact authority shard
under the authenticated requester and request id. The record retains the
semantic operation, a stable operation id, and recovery diagnostics; response
journal options are not part of mutation identity. A request-owned hidden
prepare carries the canonical terminal result, so committing a transaction
materializes both the visible namespace change and `Completed` request record
in one table batch. If a crash occurs before any prepare is durable, startup or
reactive maintenance resumes the stored operation. Prepared transactions,
cross-shard handoffs, and bounded namespace jobs use their existing durable
recovery paths. Consequently `Pending` is an internal, automatically retried
state rather than an operator reconciliation boundary. Exact retries either
join that recovery or replay the durable terminal result without another
namespace effect.

Bounded metadata-history GC, a journaled authority operation, compacts
completed routed requests, terminal prepared operations and finished namespace
jobs into compact identity/result receipts, and later evicts those receipts
once they are older than the minimum history age (seven days for resident
maintenance). Full `Pending` requests and `Prepared` operations remain intact
because autonomous recovery still needs their semantic payload, and a receipt
that a surviving prepare still depends on is kept. Eviction persists monotonic
timestamp-and-identity floors. An exact receipt still inside the window replays
its canonical outcome and a changed-payload reuse still rejects. Once the
receipt has crossed its floor, a retry returns explicit expired/unknown instead
of being admitted as a new mutation. Generated request ids carry a sortable
ULID suffix for this fence; after evicting an opaque legacy id, another missing
opaque id is conservatively expired rather than risking duplicate application.

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
own keys: `Committed` and `Aborted` rows release the footprint atomically and
become bounded terminal receipts at checkpoint. Structured-snapshot hydration
and table-log replay derive the indexes from the same prepare table, so restart
preserves active reservations without resurrecting terminal ones.

A move or hardlink that crosses metadata primaries uses a durable handoff:

1. The source authority validates and locks the affected namespace.
2. The destination authority validates its side and durably adopts the record.
3. The source authority tombstones the old path and completes the handoff.

Transport failure parks the job with its lock and progress. After destination
adoption, recovery proceeds forward; it does not roll back an already visible
destination into an ambiguous state. Authority changes are re-resolved on
resume, and both sides validate current epochs.

A running rename coordinator reads its source through explicit metadata query
variants carrying the complete recorded job and selected source frame or page
cursor. The receiver checks the authenticated coordinator against current shard
configuration, the source shard and epoch, and the canonical source path within
the job's footprint. Only that job's read reservation is excluded; public reads,
other reservations and all mutation guards retain their normal checks. The
queries return canonical physical source rows without falling back to an older
ancestor-shard copy. These rows are input data, not source/job writer custody or
publication authority; capture and binding still verify the exact native bodies
under their actual writers. Peers that do not implement these query variants
reject them and must be upgraded before participating in this protocol.

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

A partitioned lock holder can complete a new canonical publication only through
the current primary with fresh exact-prefix evidence from every assigned
metadata participant, validated content receipts, the coherent configuration
projection and the actual writer's generation and durable handoff fence. A
cached sender cursor, advisory lock grant or content receipt cannot substitute
for a missing metadata acknowledgement. An incomplete participant set leaves
the operation pending or unavailable within the existing request budget; there
is no ordinary KV-backed quorum fallback.

Handoff activation requires the retiring primary's durable freeze, the exact
frozen prefix, both assignment majorities and both primary acknowledgements.
After activation, the former primary's writer, locks and reservations remain
fenced. If the successor lacks required committed content, it stays in an
explicit `pending-repair` state and cannot serve stale bytes or accept dependent
writes until repair validates the canonical object, version and hash. Local
shard or content presence alone never grants publication authority.

Earlier formal targets describe renewable permits and degraded publication.
Those models do not establish the current full-participant Rust contract or
authorize a minority component to publish. Evidence must identify the model
revision and the implemented transition it actually checks.

The synchronization barriers are:

| Operation | Successful-return guarantee |
|---|---|
| `write` | At the canonical DFS API boundary, the write is visible through the current authority but may still be volatile across authority loss. The current FUSE adapter may acknowledge an ordinary write into its process-local dirty buffer before reaching this boundary. |
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
completion still assumes eventual logical-time advancement, a stable assignment
with all required metadata participants available, a
validated source for committed content, and fair recovery scheduling.

The advisory-lock and sync contract covers a bounded surface. Whole-file
`F_GETLK`, `F_SETLK`, and `F_SETLKW` route to the
stable object's metadata primary; lock aggregates are journaled, replicated to
every committed shard member on the healthy path, epoch-fenced, leased,
renewed by mounted clients, and moved atomically with a cross-shard object
adoption. A partial metadata participant set cannot complete a new canonical
lock mutation or sync barrier through a cluster-KV watermark. The complete
assigned proof and actual DFS writer-generation fence remain required.
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

Ordinary FUSE writes use process-local, per-handle dirty ranges and may return
before canonical DFS publication. Each handle is currently bounded to 64 MiB
of dirty bytes. If a new range would exceed that bound, the existing buffer is
published before the write is retried; an individual write larger than the
bound publishes directly. Before buffering a write, the adapter publishes a
different dirty handle for the same stable object, preserving serialized FUSE
write order across descriptors. A publication failure restores the removed
dirty ranges so a later `flush` or `fsync` can retry them.

`flush` and `release` publish the affected handle's pending ranges and perform
their lock/close cleanup, but do not claim durability. `O_DSYNC` and `O_SYNC`
first publish every earlier buffered write for the object, publish the current
write, and then invoke the file-data or full-file barrier respectively. File
`fsync` and `fdatasync` first publish all dirty handles for the object, then
capture an immutable sync target. Directory `fsync` captures an immutable
namespace target. Each barrier routes its captured target as a distinct
operation to the metadata authority. A successful
file result validates the exact version/hash on every named replica in a complete
committed durability plan, retaining the strongest desired replica count recorded
for that prefix. A completed placement plan can therefore certify the same bytes
after the original write's replica set has retired. A lazy authority lane restores
only that prefix's pending repair owner before waiting for missing receipts. The
barrier collects a fresh acknowledgement of the complete canonical materialized
metadata checkpoint at the exact journal head and validates the required
content holders. Every assigned metadata participant must acknowledge before
an ordinary barrier completes. A missing member leaves sync pending or
unavailable within its existing budget. No KV head watermark, barrier or
operation receipt can replace that participant evidence. Joint membership
handoff retains its separate exact prefix and retiring/desired majority proof.
The checkpoint probe has a restart-unique identity and carries an empty journal
batch, so a delayed reply cannot satisfy another attempt and a rolling-upgrade
peer cannot skip its replay cursor. A persisted replication cursor alone is
never a sync acknowledgement. Each replica also persists a monotonic,
fingerprint-bound checkpoint floor before acknowledging the probe; delayed
snapshot repair is rejected below that floor or at an equal sequence with a
different state. The current `fdatasync` physical barrier is
deliberately as strong as `fsync` because essential and ancillary file metadata
share one journal transaction.

Because ordinary dirty ranges exist only in the FUSE process until publication,
a mount-process crash can lose a write whose syscall already returned. The
mounted-mutation handoff and durable DFS recovery protocol apply once canonical
publication is attempted; they do not make the earlier in-memory buffer
durable.

Direct checkpoint journal requests use a work-conserving fan-out cap of two and
one 250 ms operation-wide best-effort budget. On the production multithread
Tokio runtime, sibling peer RPC futures run concurrently, each successful
response is retained independently, and late siblings are aborted and dropped
at the budget. The blocking compatibility path has the same two-worker cap and
returns at the budget; a foreign blocking call already entered may outlive that
return, so a per-transport worker-slot bound prevents repeated calls from
accumulating detached threads. Direct fan-out is only an accelerator:
missing current participant evidence leaves the ordinary operation unfinished,
while later catch-up can earn the same required proof.

This is not yet a full refinement of the combined partition model. The selected
contract permits progress when the complete assigned metadata shard
acknowledges under its valid local generation even if KV is unavailable.
Without the complete participant proof, new grants, renewals, publication and
ordinary sync completion fail or wait. Older KV-assisted degraded results do
not establish this participant-only contract. Explicit caller-selected
durable sync ids, byte-range locks,
application-level wait-cycle detection, open-unlinked lifetime, and directory
open-handle identity remain target hardening. FUSE data I/O targets the
stable object id and fails closed if the pathname has been replaced; it does
not yet retain an unlinked inode after the last name disappears. Each shard
persists an incremental hash-chain prefix proof, so healthy exact-head probes
compare sequence and digest in O(1) work. Storage conversion verifies the
original source cut before certifying a current chain. A sequence/digest mismatch
uses authoritative current snapshot repair; ordinary catch-up uses bounded
original-frame journal pages.

## Opt-in Local-first Degraded Operation

Local-first degraded operation is a target protocol, not implemented behavior.
The Rust DFS takes the conservative strong path described above. The target is
an extension beside the canonical protocol, not a second metadata authority. A
service or subtree opts in through previously certified strong metadata. When a
client cannot reach the current authority, it may operate on a durable local
branch rooted at an exact certified metadata checkpoint, even if a quorum
exists elsewhere. The branch never becomes a second canonical history.

Each branch has a durable incarnation, monotonic operation identities, an
immutable base checkpoint, exact object-version and namespace-entry stamps, and
a sealed manifest. Offline-created objects use globally unique stable ids, and
operations target those ids rather than a pathname that may have moved. When
the authority route returns, a locally synced dirty branch seals atomically; an
unsynced or clean branch freezes permanently, so a later route flap cannot
resume degraded mutation. A frozen branch may still sync and seal locally, and
a clean branch may close without publishing. Local durability barriers never
produce a canonical quorum sync certificate, and a distributed advisory-lock
token is never converted into local authority after loss or fencing.

Reconciliation classifies each operation as applicable, already converged,
conflicted, superseded, dependency-blocked or rejected. An applicable operation
becomes one canonical metadata transaction. A conflict leaves the quorum winner
at the canonical path and retains the branch value in a disjoint sidecar;
superseded work creates no sidecar. A changed canonical head invalidates a plan
and requires reclassification, and explicit conflict resolution is itself a
strong compare-and-swap transaction.

Publication goes through a one-shard reconciliation gateway. Its typed permit
binds the quorum, authority, content-receipt and metadata-prepare certificates
to one payload digest, configuration revision, authority epoch, predecessor
head and successor journal index, and carries one atomic envelope holding the
canonical delta, terminal result and optional conflict-sidecar descriptor.
Changing any bound input invalidates the permit. A branch terminal result
exists exactly when its gateway journal entry commits; result and sidecar
materialization may lag and replay idempotently from that envelope. Lost
replies retry exactly, while changed content, base head or local sync
generation is rejected. A publication keeps one visible authority and head and
creates no canonical conflict record.

Bounded Quint slices model regular-file branches, same-shard create, mkdir,
unlink, rmdir and no-replacement rename, local directory durability, the
gateway, and the branch/gateway terminal compositions. They exclude hardlinks,
symlinks, cross-shard local changes, branch gossip, semantic merges,
byte-validated retention receipts, byte-range locks and open-unlinked lifetime,
and are not yet composed with Raft, advisory locks, canonical `fsync`,
partition recovery or the Rust journal. The
[formal verification guide](formal/cluster-verification.md#what-the-models-check)
records each slice's exact scope.

## Reads and Caches

Distributed POSIX stat derives current inode fields through bounded authenticated
metadata projections; see [the authoritative stat contract](dfs-authoritative-stat.md)
for generation, cross-shard completeness, compatibility and outage behavior.

Reads prefer the closest valid copy:

1. local content for the requested current version;
2. an allowed local stale version;
3. a nearby confirmed replica;
4. the owner or durable home;
5. a routed or relayed source.

Every result exposes freshness: current, stale, pending repair, unavailable,
or policy-blocked. Reads validate the selected immutable object identity, version,
content-hash metadata, range bounds, and declared length without rehashing stored
bytes. Scrub owns byte-level corruption detection. Hashing newly ingested content
still establishes its immutable content address. Metadata watches mark cached
objects stale when the visible version changes.

Automatic caches are useful for mobile inspection and collaborative review.
Evicting a cache never changes authoritative metadata or the durable receipt
set.

## Transfer and Repair

The network data plane uses typed object RPC and stream extensions owned by
`sinomo-network`. Transfers are bounded by range, chunk count, window size, and
deadline. Optional per-chunk hashes detect corruption before final manifest
validation.

Write replanning can exclude the original requester from the final replica set
while its staged bytes remain the only transfer source. When that requester has
the durable pending Current intent, it may supply the exact target version and
hash to the independently configured current metadata primary. Caller-supplied
shard hints cannot grant this source permission. It expires when the intent
terminalizes and never contributes a receipt to W or a replica to N; selected
targets must still establish their own durable canonical bindings before commit.

The receiving metadata primary reconstructs pending transfer ownership from that
same durable intent when a response crosses an exact/global service lane or a
process restart. With no in-flight request map, requester responses require the
authenticated original source and exact object, target version, hash and current
owner epoch. Transfer reconstruction does not credit requester bytes toward W.
Every blob-fetch rejection increments a counter with a bounded diagnostic kind.
Warnings include source, intent, object and reason, with the existing per-peer
and global rate limits and suppression counts; a successful RPC alone does not
establish that DFS accepted the transfer.

Metadata journal and snapshot rejections carry an optional typed recovery code
alongside their diagnostic text. New receivers preserve that code from backend
validation through RPC and durable replication-suppression records. Snapshot
repair and epoch-scoped suppression use the code; diagnostic wording cannot
override it. One compatibility adapter interprets text only when an older peer
or stored record omits the code. Unknown future codes fail closed. Source,
assignment, epoch, and exact-prefix checks still authorize every repair.

Repair is manifest-based rather than an arbitrary directory scan:

1. Compare current metadata and receipts with locally available manifests.
2. Fetch missing current content from an eligible source.
3. Refresh a receipt only after the local hash matches.
4. Quarantine divergent blobs instead of treating them as versions.
5. Continue until the desired placement is satisfied or authority/policy
   suppresses the work explicitly.

When repair reuses an existing content-addressed blob, it must durably bind
that blob to the exact object/version before restoring the local receipt.
Hydration follows the same rule. A matching blob alone cannot satisfy a later
`CommitVersion` replay; conflicting manifests and declared sizes remain repair
failures. This binding does not add stored-byte hashing to normal reads.

Committed mutations append their exact missing-copy identities to the durable
repair queue before releasing the service lane. Routine maintenance consumes
that queue; users never reconstruct or administer it directly. Startup,
component activation and queue-persistence failures request reconciliation
from authoritative intents and receipts, so a previous process's completion
marker cannot hide a lost queue handoff. A manual maintenance request forces
reconciliation immediately before bounded repair and placement work. Status
reads and idle wakes do not request metadata scans. An admitted recovery scan
is invalidated by a DFS authority-generation change or a missing complete
proof, not by unrelated cluster-KV revisions. Its shard routing reuses the
authority owner's decoded configuration index; gaps, malformed records and
newer private proofs take the refresh/error path, and routing reads never
substitute for publication fences.

Recovery checkpoints its inventory under the exact request and authority
generation and hands discovered repairs to the durable enqueue protocol in
bounded batches, preserving retry and backoff state. Network retries reuse
that checkpoint without rebuilding metadata or replaying every commit
promotion. Lost authority, a newer request or failed persistence cannot reuse
it, and deferred local reconciliation keeps its retry even if no other work is
runnable. Startup promotion recovery has one owner, the current metadata-shard
primary; the original write coordinator owns only standalone objects, so a
historical coordinator cannot replay promotions alongside the primary. Repair
execution restores pending protocol state as it admits targets under the pass
budget instead of preparing the whole coordinator queue first. Durable terminal
receipts and current authority and content checks still fence effects, and
expired retained work retires even during retry backoff.

Repair transport stores its exact attempts and transfer request IDs durably
before sending. Cancellation before persistence drops only unsent private
preparation; afterwards the checkpointed payload and original request ID
survive outside the destroyed service, and a persistence failure cannot publish
a send. The detached worker starts peer I/O only after global, lifecycle and
foreground-interest guards are released, and keeps offline parked payloads
across worker replacement. A reply must match the original peer, destination,
request, intent, filesystem, object and version before ordinary validation
under fresh nonpreempting global admission. Current authority, content and
receipt checks still decide the effect; queue acceptance never contributes to
W or N, and an unknown transport or validation outcome retains the durable
in-flight repair owner and its original transfer identity.

Background work must not preempt the work it waits for. Routine replica
repair, routed placement polls, detached `CommitVersion` notifications and the
asynchronous metadata publisher's frame batches run as background work under
the receiver's normal admission, shard gate and authority checks. Only routine
background calls carry the optional `background_repair` hint: a missing hint
means foreground, the hint grants no authority, and mixed-purpose commands
cannot opt out. User operations, intents with a pending foreground write
response, synchronous durability and handoff catch-up, and legacy senders keep
foreground preemption, which avoids cross-node admission deadlocks.
`CommitVersion` acknowledgements and retries keep the ordinary promotion
protocol. Managed placement uses `RepairReplica` and has no staged-write
promotion phase.

Reactive placement scheduling uses the executor's root-primary ownership. Other
nodes keep their local repair and recovery obligations but do not scan
historical intents merely because a remote placement job exists. Managed
placement RPCs retain their durable job, deterministic object/version repair
identity and live placement-authority permit, and do not author a namespace
receipt per poll, so a converged remote check cannot preempt its own
coordinator; legacy pending receipts terminalize through bounded recovery. An
apparently converged remote object is confirmed through the authoritative
`LookupPaths` read: only an exact object match skips the mutation RPC, rejected
lookups cannot certify a no-op, and the read neither applies objects nor
advances a replica journal cursor. Local no-ops still require the
certified-primary proof. The daemon's placement coordinator consumes the owning
primary's current object result without preparing or replaying a requester
replica journal; older replies without object results use the authoritative
lookup. Placement replies and single-path lookups pin only the relevant path's
metadata view, and progress polls do not emit publication edges.

Explicit and adaptive placement pages share a default 250 ms time slice and
save the backend's exact continuation after completed objects. One slow object
may exceed the slice, but no following object starts in that pass, and
cancellation cannot publish unfinished progress. Placement enumerates the
requested path range directly: a self-only job performs one indexed lookup and
a subtree job seeks its root and descendants on each shard with the shard-aware
cursor, finalizing the scope size when the scan completes. Continuations seek
the normalized path index without rescanning the visited prefix, and repairing
pages checkpoint their pending replica subtotal so a later empty or converged
page cannot hide earlier missing replicas. Finishing a scan or retiring a job
immediately advances ready verification or the next queued job; an empty work
set parks the worker.

An authority-checked placement mutation hands its exact missing targets to the
durable daemon queue and emits the repair wake. Replay repeats that handoff
without rewriting settled metadata, rebuilding unrelated historical intents or
resetting the queue's retry backoff. The bounded selector coalesces older
pending adaptive observations when a newer authoritative job covers the same
scope. Replacement selection uses creation time independently of queue
priority; explicit jobs and saved scan progress remain intact, and retirement
atomically compares the replacement and configuration while requiring the
placement mutation permit to be free. Legacy repair-plan conversion groups a
bounded number of local objects for one completed placement job under one
job-scoped permit; each object still validates its version, path, pins and
accepted plan, and the permit is released before blob transfer resumes.

Repair-state restoration is local. Fresh placement-consistency permits are
checked at the executor's and receipt handlers' effect boundaries, not for each
entry while rebuilding the in-memory backlog, and the executor checks local
ownership, target eligibility and retry backoff before acquiring a permit, so
parked targets create no consistency transactions. Hydration rebuilds queued
current intents and receipts once per batch, preserving inactive historical
intents, and finishes that derived rebuild even after a partial restore error.
Identical intent-and-receipt replay preserves existing receipt provenance and
appends no journal entry, while receipt validation and missing-location repair
still run. Content staging, commit notifications and repair results do not
schedule metadata publication or storage-capacity samples; actual intent,
receipt and visible-version writes keep their own publication edge, and the
preceding content write owns the storage-change edge.

Journal replay reads the admitted writer's working tables, including earlier
operations in the same batch; it must not call freshness-checking getters
between advancing the journal prefix and materializing tables, which could
reopen a full checkpoint and hide staged changes. Shard refresh reconciles
derived indexes from cached object IDs without exporting or hashing complete
snapshots; exact handoff completion and prefix checks remain the authority
boundary. Journal reads use indexed record byte ranges before selecting the
decoded batch.

Attachment cleanup is reactive. Startup and authority-generation changes take an
exact local-shard journal prefix and reconcile only the managed attachment
directories in bounded pages, then consume the journal tail. They do not replay
all historical namespace operations. Authoritative source removals and managed
attachment mutations retain a cleanup notification in the same committed-work
tracker as publication and repair. Browser mutation guards deliver that edge
even when response delivery is abandoned; ordinary source upserts, replica
ingest and receipt-only repair do not create it. Productive pages continue
immediately. Once caught up, the worker parks without feed polls, periodic
rescans, or authority polling. Only payload grace/lease expiry and actual errors
retain deadlines. A cancelled blocking pass keeps its pending checkpoint and
hints, and an idle worker releases its attachment backend generation.

Scrub, old-version collection, orphan cleanup, and tombstone compaction are
background lifecycle operations.

On a resource-restricted mobile node or a metered link, remote-only routine
repair, placement reconciliation, metadata catch-up, and deferred sends park
without a periodic probe. A constrained node on an unmetered link may make
progress with one repair, a small placement page, and one metadata batch per
pass instead of workstation-sized budgets. A change in either resource mode or
metering re-probes the durable queue. Local write, namespace and handoff
recovery continues, while an explicit operator disaster-recovery request
bypasses this advisory background policy.

The storage sampler is event-driven in every active DFS component, not only
mobile hosts. It samples physical capacity once at startup and after a short
coalescing delay for committed DFS mutations or completed lifecycle maintenance.
Idle time and advertisement lease age never sample storage or publish a
heartbeat. Exact resource transitions re-evaluate cached advisory fields, while
the capacity evidence keeps its original expiry and becomes conservatively stale
if no later storage mutation supplies a fresh sample. Metering and
resource-pressure fields bypass storage-health hysteresis because they come from
exact host callbacks; capacity and health transitions retain their ordinary
consecutive-sample protection.

## Roaming and Failure Behavior

Roaming nodes are ordinary trusted members but should not normally be required
for control-plane quorum. They may hold caches or data replicas and repair after
reconnect.

| Condition | Required behavior |
|---|---|
| KV quorum unavailable | Placement and authority changes cannot commit. A shard primary may continue from its latest valid assignment only when the complete configured DFS shard acknowledges; degraded partial-shard publication fails or waits. |
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

## Windows Cloud Files Adapter

Windows exposes a SIFS subtree as a supervised CFAPI sync root. The adapter is
a durable NTFS projection, not another DFS metadata implementation: DFS remains
authoritative, placeholder hydration is bound to an exact object version, and
the local queue and reconciliation manifest are replay evidence rather than a
second visible history.

Local NTFS changes are copied into immutable staging and appended to a durable
operation queue before canonical DFS publication. Ambiguous outcomes reconcile
by stable operation identity before retry. Committed remote namespace changes
persist intent before their local placeholder effect and advance their shard
cursor only after that effect and the manifest are durable. Dirty local paths
become conflicts instead of being overwritten.

The provider pushes queued, uploading, retrying, conflict, and failure state
into Windows-owned File Explorer item properties. Completed paths clear that
custom property and return to the standard CFAPI in-sync badge. This projection
is equality-cached and one-way; Explorer does not load Sinomo provider code or
query DFS while rendering a folder.

Registration, provider replacement, state-preserving disable, and cursor
recovery are jointly owned by the daemon supervisor and the native provider.
They use pinned module provenance, physical sync-root identity, deterministic
successor files, and fail-closed phase classifiers. The detailed data paths,
reactive-at-rest contract, recovery commit protocol, and Windows verification
boundary are defined in the
[Windows Cloud Files architecture](cloud-files-architecture.md).

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
publication, receipt policy, full-shard authorization, replica fan-out and
response scheduling, repair authorization, content validation, and the
single-visible-history invariant. Historical results for superseded
KV-assisted recovery remain limited to their exact models and source; they
are not proof of the current participant-only contract.
Network-session framing and body
attestation are implementation-tested rather than modeled by the DFS specs. See
the [formal verification guide](formal/cluster-verification.md).

This architecture records required behavior. Exact Rust types, storage table
names, protocol message variants, benchmark presets, and migration readers
belong in source, Rustdoc, crate documentation, and tests.
