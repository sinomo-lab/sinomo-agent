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
When KV quorum and the responsible owner or metadata-shard primary are
available, all visible writes serialize through that authority.

The commit path must preserve these properties:

1. The writer names the base and target version.
2. The authority validates its epoch, policy, and base version.
3. Content is durable on the nodes required by policy.
4. Matching receipts exist before the visible version advances.
5. The metadata commit compares the current version and write intent
   atomically.
6. Replicas and caches may lag, but they cannot become write authorities.

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
6. Atomically compare the authority epoch, base version, intent state, and
   receipt threshold.
7. Advance the visible version and schedule any remaining repair.
8. Publish metadata progress to caches and interested sessions.

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
The primary validates every object and parent involved, commits the namespace
transaction, and returns bounded journal progress so the requester can read its
own change.

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
metadata quorum, epoch-fenced, leased, renewed by mounted clients, and moved
atomically with a cross-shard object adoption. Blocking waits run outside the
FUSE dispatch lane, and exact close/cancel transitions plus finite leases keep
abandoned waiters from becoming protocol deadlocks. Partial byte ranges remain
unsupported, and a conflicting blocking shared-to-exclusive conversion returns
`EDEADLK` instead of creating a conversion cycle.

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
metadata checkpoint at the exact journal head from a metadata majority,
intersects that recovery set with the content holders, publishes an epoch-bound
durable metadata-head watermark through cluster Raft, and only then returns.
The checkpoint probe has a restart-unique identity and carries an empty journal
batch, so a delayed reply cannot satisfy another attempt and a rolling-upgrade
peer cannot skip its replay cursor. A persisted replication cursor alone is
never a sync acknowledgement. Each replica also persists a monotonic,
fingerprint-bound checkpoint floor before acknowledging the probe; delayed
snapshot repair is rejected below that floor or at an equal sequence with a
different state. The current `fdatasync` physical barrier is
deliberately as strong as `fsync` because essential and ancillary file metadata
share one journal transaction.

This is not yet a full refinement of the combined partition model. Rust takes
the conservative path when quorum is unavailable: new grants, renewals, and
sync completion fail or wait rather than using the model's complete-holder
permit. Explicit caller-selected durable sync ids, byte-range locks,
application-level wait-cycle detection, open-unlinked lifetime, and directory
open-handle identity remain target hardening. FUSE data I/O now targets the
stable object id and fails closed if the pathname has been replaced; it does
not yet retain an unlinked inode after the last name disappears. The initial exact
metadata-checkpoint proof also hashes the complete shard snapshot on each
participant; replacing this bounded correctness-first path with a cached
incremental or Merkle commitment is future scale hardening.

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
publication, receipt policy, repair authorization, chunk transfer, and the rule
that normal operation does not create conflict histories. See the
[formal verification guide](formal/cluster-verification.md).

This architecture records required behavior. Exact Rust types, storage table
names, protocol message variants, benchmark presets, and migration readers
belong in source, Rustdoc, crate documentation, and tests.
