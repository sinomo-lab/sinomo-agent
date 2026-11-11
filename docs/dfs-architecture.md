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

The history module uses its namespace to translate source and capture provenance
into user-facing history. Other modules can retain immutable revisions under
their own namespaces without introducing a second content database or modifying
history's semantics. `sinomo-dfs-migration` alone interprets retained legacy
records and converts them into the current representation. The DFS runtime
accepts only its current storage and wire protocols; incompatible nodes must be
upgraded before participating.

A disabled feature exposes no routes or
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

For replicated-eventual writes, W is the required durable receipt threshold and
N is the desired replica count. A safe surviving configuration must be able to
commit at W while repair continues toward N. The metadata protocol must preserve
this availability requirement through abrupt node loss; it cannot make every
participant mandatory or require the missing primary to cooperate.

## Consistency Invariants

Normal cluster operation has one authoritative visible history per object.
All visible writes serialize through the responsible owner or metadata-shard
primary.

KV access on DFS operation paths is explicit. Namespace routing and policy reads
use the one configuration snapshot the metadata authority publishes from native
proofs and committed deltas. An operation takes that snapshot once for its
shards, filesystem policy, members and providers, and its commit point is fenced
only by that snapshot's generation. Processes outside the daemon consume the
same publication: the local DFS component announces each change as a typed
`dfs.configuration.published` event and serves its current floor-checked
snapshot on request. A long-lived client such as mounted SIFS holds the snapshot
it fetched after the newest announcement and none while its subscription is
down; a one-shot command reads it once per operation. Neither keeps its own
configuration cache or refresh loop, and keys outside the published scopes are
read through ReadIndex. Peer routing and connection health remain owned by the
network layer.

Content eligibility joins authorized cluster membership, compatible filesystem
providers and explicit content acceptance from one coherent configuration.
Offline desired replicas remain repair targets without supplying current
receipts. Observation and discovery cannot substitute for an active owner,
current authority or durable content evidence.

Cluster KV retains its atomic transactional API and orders cluster
configuration and authority changes. DFS does not add caller bindings,
caller sessions, historical replay APIs, or substitute KV operation ledgers.
An ambiguous configuration transaction is reconciled from its existing native
outcome and exact guarded configuration state; DFS then resumes its own durable
shard protocol. Paths, intents, receipts, namespace transactions, journals,
snapshots, object metadata, bytes, the derived repair index, and metadata
acknowledgement floors remain exclusively in DFS. Historical global metadata
floors and watermarks may be interpreted only by migration to preserve old
obligations; they do not authorize a current ordinary write.

The commit path must preserve these properties:

1. The writer names the base and target version.
2. The authority validates its epoch, policy, and base version.
3. Content is durable on the nodes required by policy.
4. Matching receipts exist before the visible version advances.
5. Metadata authority remains safe across node loss and reconfiguration.
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
- Only the primary and configured replicas hold a shard's metadata, plus a
  replica a pending membership handoff is adding. Journal batches, snapshot
  installs and routed-response batches feed only those nodes. A node outside
  a shard's placement deletes its copy when it opens metadata for that
  placement; if it is added back, it receives the shard from the primary.
- A sequence fork triggers authoritative snapshot recovery rather than merge.
- Scans and hydration are cursor-based, so restart and listing do not require
  one unbounded allocation.
- Large directories use fragments to keep lookup and pagination bounded.

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

Authority transfer requires no repair state from the retiring owner, which is
usually offline when a transfer happens. Every replica derives the shard's
remaining repair work from its replicated tables, so the successor schedules
that work as soon as it is assigned. Each replica receipt slot carries a
generation that a repair reply must match to publish, which fences late replies
to attempts made before the transfer. Retry timing restarts at the successor.
Metadata handoff does not manufacture missing content or weaken the configured
durability requirement.

## Content Stores

### File attachments

`sinomo-dfs-attachments` stores file-associated data as independently versioned
DFS objects with a bounded typed header and range-readable payload. Object
annotations and version-bound derivations have distinct freshness semantics. See
the [attachment contract](developer/file-attachments.md) for storage, lifecycle,
bounds, module integration, tags/transcripts, and thumbnail persistence.

### Explicit File History

`sinomo-dfs-history` owns the content-history policy and user-visible history
operations. The default policy captures nothing automatically. Named pins
retain explicitly captured versions. Normal generation changes do not create
user-visible history, and rejected or uncertain writes never preserve alternate
versions as a substitute for consistency.

Historical content shares the ordinary immutable content store, metadata shard,
receipt, and repair mechanisms. Compact retention configuration belongs in
cluster KV; version records, pins, jobs, and content remain DFS-owned. Explicit
preservation of local-operation bytes uses that same retained-version model
without changing the canonical head or taking its pending-writer slot.

Restore is a new conditional write or exclusive create under current authority,
permissions, and placement policy. Single-file write success requires both W and canonical publication; background
N-W repair cannot override a later committed head.

Public exact-version reads resolve current authority and require the current
identity or a live retention grant. A leftover blob alone does not grant access
to history. Historical reads use bounded ranges and distinguish expired or
absent retention from unavailable content.

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

An accepted placement intent retains the meaning of its captured observations
at creation time. Fresh previews evaluate current observations, while every
effect still rechecks current authority, policy, pins and storage evidence.
A content-read scheduling hint cannot settle an uncertain storage mutation.

Session locality, privacy, and trust-domain inputs still belong in upstream
eligibility. Bringing every input into one auditable placement policy is tracked
in the [DFS policy and authority plan](plans/dfs-policy-and-authority-hardening.md).

Read caches remain distinct from durable receipts and never become write
authorities merely because they possess current bytes. If the minimum durability
threshold cannot be met, the write remains pending or fails.

## Write Protocols

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
The primary validates every object and parent involved and commits the namespace
transaction; the requester reads its own change from that primary.

Operations contained within one authority are atomic. Recursive work is split
into bounded durable jobs that resume after restart.

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

## Opt-in Local-first Degraded Operation

Local-first degraded operation is a target protocol, not implemented behavior.
The target is
an extension beside the canonical protocol, not a second metadata authority. A
service or subtree opts in through previously certified strong metadata. When a
client cannot reach the current authority, it may operate on a durable local
branch rooted at an exact certified metadata checkpoint, even if a quorum
exists elsewhere. The branch never becomes a second canonical history.

## Reads and Caches

Distributed POSIX stat derives current inode fields through bounded authenticated
metadata projections; see [the authoritative stat contract](dfs-authoritative-stat.md)
for generation, cross-shard completeness, compatibility and outage behavior.
Path and object metadata is answered by the shard's primary or one of its
replicas. A node outside the replica set reads paths, open handles, inode
identities and hardlink targets through the primary, plans its writes against
the primary's current object, and surfaces the primary's error rather than a
local copy. Callers that delegate to their node's Main (mounted SIFS, sandbox,
browser, CLI, Cloud Files) keep no metadata store of their own: they read
through the shard authorities and write through Main, which alone owns and
recovers the node's shards.

Reads prefer the closest valid copy:

1. local content for the requested current version;
2. an allowed local stale version;
3. a nearby confirmed replica;
4. the owner or durable home;
5. a routed or relayed source.

Every result exposes freshness: current, stale, pending repair, unavailable,
or policy-blocked.

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

Each shard's tables derive its repair needs from desired placement and replica
receipts, so every replica holds the same repair index and a new primary can
schedule the remaining work as soon as it is assigned. Only the shard primary's
repair executor dispatches repairs. It keeps attempt and retry state in memory;
there is no node-local repair queue and no custody of earlier attempts. The
executor is reactive: it runs on changes to the index, attempt outcomes,
reachability and authority changes, a gained primary assignment, a durability
wait on a missing replica, and its own re-send timer, and each pass does work
proportional to those events. It reads a shard's whole index only when it gains
that shard, including at process start. A durability wait can move its object
ahead of other ready repairs but never shortens a backoff that an earlier failure
earned.

Scrub, old-version collection, orphan cleanup, and tombstone compaction are
background lifecycle operations.

## Roaming and Failure Behavior

Roaming nodes are ordinary trusted members but should not normally be required
for control-plane quorum. They may hold caches or data replicas and repair after
reconnect.

| Condition | Required behavior |
|---|---|
| KV quorum unavailable | Placement and authority changes cannot commit. Existing shard operations require valid authority; loss of KV quorum does not itself grant or revoke that authority. |
| Metadata primary unavailable | A supported surviving configuration must establish safe authority without cooperation from the missing primary. Otherwise writes remain pending or fail explicitly. |
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

Registration, provider replacement, state-preserving disable, and cursor
recovery are jointly owned by the daemon supervisor and the native provider. The detailed data paths,
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

Live content availability observations come from the actual active content
owner, with authenticated node identity, current process ownership and original
evidence freshness. Reading an observation does not sample storage, renew
expired evidence, publish durable state or grant write authority. Missing or
stale owner evidence fails closed. Idle observation and maintenance must not
create physical writes merely to refresh timestamps or report unchanged work;
quietness includes attributable helper and writeback activity.

Operators need metrics for repair backlog, metadata counts, content footprint,
namespace jobs, transfer progress, quota pressure, and shard catch-up. Metrics
must not require unbounded scans in the foreground write path.

## Verification Boundary

Benchmarks and integration tests are the source of truth for supported scale and
performance. Formal models cover authority, routing freshness, namespace
publication, receipt policy, full-shard authorization, replica fan-out and
response scheduling, repair authorization, content validation, and the
single-visible-history invariant.
See the [formal verification guide](formal/cluster-verification.md).

This architecture records required behavior. Exact Rust types, storage table
names, protocol message variants, benchmark presets, and migration readers
belong in source, Rustdoc, crate documentation, and tests.
