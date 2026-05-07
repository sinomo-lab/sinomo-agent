# Sinomo DFS Architecture

## Purpose

Sinomo DFS is a local-first distributed object and metadata layer for agent
artifacts, evidence bundles, task staging, and cross-device handoff. It exposes
a default virtual filesystem named `sifs` (Sinomo File System). SIFS is the root
filesystem for the cluster and many cluster operations depend on it. From
`sinomo-dfs`'s perspective, SIFS is not a special storage engine; it is the
default filesystem with predefined subtree policies. See [SIFS](sifs.md) for the
cluster filesystem layout.

The design borrows the useful shape of Ceph:

- a small replicated control plane for cluster and placement truth,
- separate metadata and content planes,
- content-addressed objects stored by many storage participants,
- policy-driven placement instead of hardcoded replication,
- explicit handling of node loss, stale caches, and recovery.

It intentionally differs from Ceph in scale and priority. Sinomo optimizes for a
small trusted cluster of desktops, phones, home servers, and roaming devices.
Local access, privacy, battery, and intermittent connectivity matter more than
fleet-scale throughput.

## Non-Goals

- Replacing the host filesystem.
- Replacing Git for source history and merge review.
- Storing large file bytes in the cluster KV.
- Storing per-file, per-directory-entry, object-version, or replica-receipt
  metadata in the cluster KV.
- Public federation between unrelated users.
- Strong multi-writer POSIX semantics across unreliable mobile links.
- Requiring phones or laptops to stay online for quorum.

The DFS should make distributed SIFS objects reliable and inspectable without
pretending every device is always available.

## Existing Baseline

The current repository already has the first useful slice:

- `sinomo-dfs` defines the default `sifs` filesystem, subtree policies, object
  metadata, read/write routing, network messages, policy violation events, and
  a local-owned service.
- `sinomo-kv` provides the etcd-like control-plane core: revisions,
  compare-and-swap transactions, leases, watches, snapshots, durable WAL
  replay, hard-state persistence, and deterministic Raft replication tests.
- `sinomo-network` has local/in-process transport, peer registry, health,
  endpoint candidates, LAN/roaming metadata, and protocol-message framing.
- `sinomo-local` reserves `$SINOMO_DATA_DIR/dfs`.
- `sinomo-supervision` already indexes `dfs.*` events for audit views.

The next implementation should evolve the DFS crate from an in-memory
local-owned skeleton into a sharded DFS metadata service, a durable local
content store, and a small KV-backed control plane for configuration.

## First Version Scope

The first real DFS version should support two object classes:

1. `local-owned` objects with automatic read-cache replicas.
2. distributed eventual objects backed by two or more storage nodes.

`local-owned` remains the default for repository checkouts and sensitive local
files. The owner is the only write authority. Other nodes may cache confirmed
versions for fast local reads when policy permits raw content transfer.

Distributed eventual objects are for artifacts, task handoff bundles, generated
outputs, shared evidence, and work products that should survive a single node
loss without requiring every desired replica to acknowledge every write.

For distributed eventual objects, policy separates:

- `min_commit_replicas`: how many durable receipts are needed before the version
  becomes visible,
- `desired_replicas`: how many replicas the cluster should eventually maintain,
- `eligible_nodes`: nodes or trust domains that may hold raw content,
- `read_staleness`: whether stale confirmed versions may be returned,
- `repair_deadline`: how aggressively the cluster should converge after a node
  reconnects.

This mirrors the useful Ceph distinction between the replicas needed to accept a
write and the target replica count the cluster repairs toward later. Sinomo
should expose that distinction as policy from the beginning.

## System Shape

Ceph term mapping for Sinomo:

| Ceph-ish role | Sinomo role |
|---|---|
| Monitor | `sinomo-kv` replicated control plane |
| MDS | DFS metadata coordinator and local metadata cache |
| OSD | node-local DFS content store |
| CRUSH map | policy and placement planner |
| RADOS object | DFS object version/chunk |

Sinomo should not copy Ceph's placement group complexity at first. A small
cluster can compute object placement directly from object metadata, node
capabilities, node health, and inherited subtree policy.

## Recommended Distributed Algorithm

Use a hybrid algorithm:

1. Raft-backed KV only for compact control-plane configuration.
2. Sharded DFS metadata service for namespace, version, and receipt records.
3. Owner/coordinator protocols for object writes.
4. Deterministic policy placement for replicas and read caches.
5. Content-addressed immutable blobs with BLAKE3 hashes.
6. Journal-driven metadata catch-up and anti-entropy repair after reconnect.

This is intentionally not a fully symmetric peer-to-peer filesystem. Cluster
configuration lives in a small, understandable KV control plane. Bytes can move
peer-to-peer. High-cardinality file metadata lives in sharded metadata stores
that are replicated only to the nodes responsible for those shards.

### Control Plane

DFS uses the existing `sinomo-kv` Raft control-plane core for small, duplicated
configuration only. The DFS service talks to it through a small control-plane KV
client interface so production can bind the same code to the networked KV
service and tests can use the deterministic durable Raft harness.

Properties:

- filesystem config, metadata shard placement, policy defaults, service
  discovery, and owner/primary epochs are KV keys,
- KV data must remain small enough to duplicate across all control-plane voters,
- file paths, dentries, inodes, object versions, chunk manifests, write intents,
  replica receipts, and conflict records are not stored in KV,
- phones and roaming devices should normally be learners or non-voting data
  nodes, not quorum voters.

Recommendation: keep only cluster-wide routing and placement decisions in KV.
Namespace and version decisions belong to DFS metadata shards.

### Metadata Plane

The metadata plane is a CephFS-MDS-like service with Sinomo local-first
adjustments:

- metadata is partitioned into SIFS metadata shards,
- shard ownership and replica sets are configured by the KV control plane,
- each shard stores records in a local ordered store, not global KV,
- each shard has an append-only journal for mutation replication and replay,
- large directories split into directory fragments by filename hash,
- shard replicas subscribe to journal entries and compact snapshots,
- roaming devices can hold read caches or learner replicas without becoming
  quorum voters.

The baseline metadata key model should be:

```text
inode/{inode_id}
dirfrag/{dir_inode}/{frag_id}/dentry/{name_hash}/{name}
version/{object_id}/{version}
receipt/{object_id}/{version}/{replica_node_id}
conflict/{file_system_id}/{conflict_id}
journal/{shard_id}/{sequence}
```

This gives the system room for millions of files in one build tree without
putting millions of keys into a cluster-wide KV replicated to every node.

### Object Write Algorithm

Mutable objects use a single logical coordinator per write. For `local-owned`,
the coordinator is the owner node. For replicated policies, the coordinator is
usually the owner node at first and can later become an elected metadata-shard
coordinator if needed.

Algorithm:

1. Writer resolves metadata and expected base version.
2. Coordinator creates or validates a write intent.
3. Writer stages content locally and computes a BLAKE3 content hash.
4. Coordinator checks policy, object epoch, and expected base version.
5. Content is committed locally or replicated according to policy.
6. Required replica receipts are recorded in the metadata shard journal/store.
7. A shard-local compare-and-append advances the visible version.
8. Metadata subscribers notify caches and session participants.

This gives serialized visible-version changes within the responsible metadata
shard without forcing file metadata or bytes through the cluster KV.

### Distributed Eventual Object Algorithm

Distributed eventual objects use threshold commit plus anti-entropy repair.
The concrete first-version algorithm is:

```text
Threshold Primary-Copy Object Replication with Anti-Entropy Repair
```

The primary/coordinator serializes visible versions through the responsible
metadata shard. Replica convergence is asynchronous after the commit threshold
is met.

Definitions:

- `N`: number of desired replicas selected by placement policy.
- `W`: number of durable receipts required before a version is visible.
- `W <= N`.
- `W` is usually `1` or `2` for local-first clusters.
- `N` is usually `2` or `3` for the first version.

Commit protocol:

1. Coordinator computes the desired replica set using placement policy.
2. Coordinator creates a write intent with `N`, `W`, selected replicas, base
   version, target version, owner/coordinator epoch, and BLAKE3 hash.
3. Writer stages content locally.
4. Coordinator sends `StageBlob` to selected replicas until at least `W`
   eligible nodes have durable receipts.
5. Coordinator advances the visible version with one metadata-shard transaction
   comparing base version, intent state, epoch, and the required receipt set.
6. Replication jobs continue asynchronously until all `N` desired replicas have
   the committed version.
7. Reads prefer the nearest current replica, may fall back to the coordinator or
   owner, and may return stale content only if caller and policy allow it.

Convergence guarantee:

- If the metadata shard has a live primary or can elect/fence a new primary, at
  least one current committed replica remains reachable over time, and eligible
  replica nodes eventually reconnect, then anti-entropy repair eventually brings
  every desired replica to the current committed version.
- If concurrent disconnected writes happen, convergence is to explicit conflict
  records, not silent overwrite.

This is "eventual consistency" for content placement and cache convergence. The
visible object version is still serialized by the metadata shard. That boundary
keeps the system understandable: replicas may lag, but the authoritative current
version does not fork silently.

Intent states:

```text
open -> staging -> commit-ready -> committed
                  -> pending-repair
                  -> aborted
                  -> conflicted
```

Receipt states:

```text
staged -> durable -> committed -> repairing -> lost
```

Protocol messages:

```text
ProbeVersion { object_id, version, hash }
StageBlob { intent_id, object_id, version, hash, bytes|chunk }
StageBlobAck { intent_id, node_id, hash, durable }
CommitVersion { intent_id, object_id, version, hash }
CommitVersionAck { intent_id, node_id }
FetchManifest { object_id, version }
FetchBlob { object_id, version, hash, range }
RepairReplica { object_id, version, hash, target_node_id }
AbortIntent { intent_id, reason }
```

Commit predicate:

```text
object.visible_version == intent.base_version
object.owner_epoch == intent.owner_epoch
intent.state in { staging, commit-ready }
count(durable_receipts matching intent.hash from eligible nodes) >= min_commit_replicas
```

Commit operation:

```text
txn(
  compare: commit predicate,
  success:
    append journal entry for version/<object_id>/<target_version>
    update object/<object_id>.visible_version = target_version
    update intent/<object_id>/<intent_id>.state = committed
    append replication-job entries for missing desired replicas
  failure:
    leave intent pending or create conflict according to policy
)
```

Read predicate:

```text
current_read:
  local blob hash == version.content_hash
  and local receipt state in { durable, committed }
  and metadata visible_version == requested version

stale_read:
  caller_allows_stale
  and policy_allows_stale
  and local blob hash matches an older committed version
```

Anti-entropy predicate:

```text
for each desired replica:
  if no durable receipt for visible version:
    enqueue RepairReplica
  if local blob exists and hash matches:
    write or refresh receipt
  if local blob hash diverges:
    create conflict or quarantine local blob
```

### Placement Algorithm

Use deterministic weighted rendezvous selection rather than Ceph-style placement
groups for the first implementation.

For each candidate node, compute a score from:

- policy eligibility,
- node health,
- local/session proximity,
- endpoint quality,
- storage capability,
- battery and bandwidth hints,
- trust domain and privacy eligibility,
- stable hash of `(file_system_id, object_id, version, node_id)`.

Then select:

- owner as mandatory for `local-owned`,
- durable home node for confirmed replicas when eligible,
- session participants as read-cache targets,
- additional replicas by lowest score until `desired_replicas` is met.

The stable hash prevents all objects from clustering on one node while still
allowing policy and current health to dominate. If the cluster is tiny, the
algorithm degenerates cleanly to owner plus durable home plus active session
caches.

### Read Cache Algorithm

The default coding-subtree policy should be `local-owned` plus automatic
read-cache replicas.

Rules:

- writes still route to owner,
- successful owner reads can populate the requester cache when policy allows,
- active session participants receive cache hints for small and recently touched
  files,
- cached content is keyed by `(object_id, version, blake3_hash)`,
- metadata watches mark cached content stale when visible version changes,
- stale reads require explicit caller permission,
- cache eviction never mutates authoritative metadata.

This keeps repository ownership simple while making mobile inspection and
cross-device review fast.

### Anti-Entropy Repair

Repair is manifest-based, not directory-scan-based.

On reconnect or periodic background tick:

1. Node compares local manifests against KV version and receipt records.
2. Missing confirmed blobs are pulled from owner or eligible replicas.
3. Extra unreferenced blobs become garbage-collection candidates.
4. Pending receipts are retried if the local blob hash matches the version
   manifest.
5. Divergent local writes become conflict records unless policy allows discard.
6. Distributed eventual objects are re-replicated until their desired replica
   count is satisfied.

This is the main roaming recovery loop.

### Failure Semantics

- No KV quorum: no visible metadata writes for distributed objects.
- Owner offline for `local-owned`: writes queue, fail, or create conflict
  candidates according to policy; ownership does not move automatically.
- Too few eligible receipts for distributed eventual objects: commit waits,
  degrades to pending, or rejects based on timeout policy.
- Some desired replicas offline after threshold commit: visible version may
  commit, and repair later converges replicas toward policy.
- Cache stale: reads can continue only when caller and policy allow stale reads.
- Split brain: stale owner epochs are fenced by KV; reconnected old owners must
  reject writes from obsolete epochs.

### Why This Algorithm

This is the best fit for Sinomo's expected cluster shape:

- small node count,
- high value on inspectability,
- local-first access,
- intermittent mobile devices,
- policy-sensitive raw content,
- coding subtrees where explicit conflicts are better than hidden merges.

It also maps directly onto existing crates: `sinomo-kv` already provides a
small replicated control plane, `sinomo-network` already carries protocol
messages, and `sinomo-dfs` already has owner-routed local-owned behavior.

## Core Components

### Metadata Coordinator

The metadata coordinator owns path and object truth through DFS metadata shards.
It is a library/service surface in `sinomo-dfs`, backed by shard-local ordered
stores and append-only journals. `sinomo-kv` only tells nodes where shards live
and which epoch/primary is valid.

Responsibilities:

- create, update, and tombstone namespace records,
- map filesystem paths to stable object IDs,
- track object kind, owner, visible version, policy, and provenance,
- track version manifests, content hashes, replica receipts, and conflicts,
- perform shard-local compare-and-append write coordination,
- publish journal-backed metadata updates to local caches.

The coordinator does not store file content.

### Local Content Store

Every node that can store DFS bytes runs a local content store under:

```text
$SINOMO_DATA_DIR/dfs/content/
```

Recommended layout:

```text
content/
  blobs/<hash-prefix>/<hash>
  objects/<object_id>/<version>/manifest.sobj
  staging/<intent_id>/
  gc/
metadata-cache/
  snapshot.sobj
  watches/
```

The content store should be append-first: write staged bytes, fsync as needed,
verify hash, then publish a manifest or receipt. Visible metadata advances only
after the relevant policy allows it.

Content identity uses BLAKE3. The canonical content-hash string should be:

```text
blake3:<lowercase-hex-digest>
```

### Placement Planner

Placement is policy-driven. Inputs:

- object or inherited directory/subtree policy,
- owner node,
- preferred nodes and allowed trust domains,
- node health and endpoint quality,
- storage capability and free-space hints,
- privacy class and raw-content egress rules,
- latency, battery, bandwidth, and roaming cost,
- active session and pinned filesystem state.

Outputs:

- authoritative owner or coordinator,
- desired replica set,
- read-preferred locations,
- required receipts before commit,
- background replication tasks,
- cache and prefetch hints.

For the MVP, placement can be deterministic and simple:

1. Owner is always included unless policy is `ephemeral-local`.
2. Prefer a durable home node for confirmed replicas.
3. Prefer current session participants for automatic read-cache replicas.
4. Avoid offline, suspect, low-battery, or privacy-ineligible nodes.
5. Use weighted rendezvous scoring as the tie-breaker.

### Data Plane

The data plane moves bytes between nodes over `sinomo-network` protocol
messages. It should support:

- digest probe: "do you have object/version/hash?",
- manifest fetch,
- full blob fetch,
- range fetch,
- staged blob push,
- replica receipt,
- repair pull,
- abort staging.

The current `DfsNetworkMessage` is extended rather than replaced. The first
operating implementation transfers bounded whole blobs in protocol messages for
staging and repair. Large payloads should move to chunked/ranged streams after
the minimal service path is stable.

## SIFS Cluster Root

SIFS is the cluster root filesystem. It is the default DFS namespace exposed to
operators, agents, cluster services, and rollout/self-evolution workflows.
Higher-level concepts such as evolution workspaces are layered onto SIFS paths;
DFS itself records object identity, path metadata, versions, locations,
receipts, conflicts, and inherited subtree policy.

The initial SIFS layout is mixed-architecture:

```text
/
/sinomo
/sinomo/nodes
/sinomo/nodes/<node_id>
/sinomo/nodes/<node_id>/files
/users
/services
```

`/sinomo/nodes/<node_id>/files` is a local-owned subtree where a node exposes
its local `$SINOMO_DATA_DIR/files` directory. `/users` and `/services` are true
distributed DFS subtrees for cluster-shared user and service state.

## Metadata Model

SIFS is the default DFS namespace and cluster root filesystem. Agents and
higher-level services can map their own directory layouts onto SIFS, but DFS
metadata only records object identity, paths, versions, locations, receipts,
conflicts, and inherited subtree policy.

Object identity is stable across path moves:

- `file_system_id`
- `object_id`
- `path`
- `kind`
- `owner_node_id`
- `visible_version`
- `policy`
- `current_locations`
- `content_hash`
- provenance event IDs

Version metadata should be split from object metadata:

- `object_id`
- `version`
- parent version or base version,
- manifest hash,
- content hash,
- content length,
- chunk list when chunked,
- writer node,
- desired replica count,
- commit receipt threshold,
- committed event ID,
- replica receipts,
- commit state.

Conflict metadata is first-class:

- conflicted object ID,
- base version,
- local candidate version,
- competing visible version,
- producing node,
- conflict strategy,
- resolution status.

The public DFS API should expose both levels:

- SIFS path APIs for filesystem-like agent workflows,
- lower-level object/version APIs for replication, repair, audit, and advanced
  tools.

The lower-level API should make object identity, visible version, requested
version, content hash, replica receipts, and conflict state explicit.

## Control-Plane Keyspace

Use slash-delimited `sinomo-kv` keys and JSON values only for compact
configuration:

```text
/dfs/filesystems/<file_system_id>/config
/dfs/filesystems/<file_system_id>/metadata-shards/<shard_id>
/dfs/policies/<policy_id>
/dfs/services/metadata/<node_id>
/dfs/services/content/<node_id>
```

Important rule: this keyspace must not contain per-path, per-object,
per-version, receipt, or conflict records.

## Metadata-Shard Keyspace

Each metadata shard uses its own local ordered store and journal. The exact
storage engine can change, but the logical records should look like:

```text
inode/<inode_id>
dirfrag/<dir_inode>/<frag_id>/dentry/<name_hash>/<name>
object/<object_id>
version/<object_id>/<version>
intent/<object_id>/<intent_id>
receipt/<object_id>/<version>/<node_id>
replication-job/<object_id>/<version>/<node_id>
conflict/<object_id>/<conflict_id>
pin/<node_id>/<file_system_id>/<escaped_path>
cache-hint/<node_id>/<file_system_id>
gc/<node_id>/<object_id>/<version>
journal/<sequence>
```

Important rule: these records are replicated by shard policy, not duplicated to
every KV voter.

## Policy Model

The existing `DfsObjectPolicy` enum is useful for routing decisions, but the
implementation needs a structured policy record behind it.

Suggested shape:

```text
mode:
  local-owned | replicated-confirmed | replicated-eventual | append-only | ephemeral-local
replication:
  min_commit_replicas
  desired_replicas
  preferred_nodes
  forbidden_nodes
  allowed_trust_domains
  repair_deadline
locality:
  owner_required
  prefer_session_nodes
  automatic_read_cache
  pin_on_open
  prefetch_depth
privacy:
  raw_content_may_leave_owner
  encryption_required
  redaction_profile
conflict:
  reject | owner-serial | branch | last-writer-wins | resolver
retention:
  ttl
  keep_versions
  gc_class
limits:
  max_object_bytes
  max_chunk_bytes
```

The enum can remain the public quick mode while the structured policy becomes
the persisted and inherited subtree policy.

Default coding-subtree policy:

```text
mode: local-owned
locality:
  owner_required: true
  automatic_read_cache: true
  prefer_session_nodes: true
conflict: owner-serial
privacy:
  raw_content_may_leave_owner: true unless subtree policy narrows it
```

Default distributed-eventual policy:

```text
mode: replicated-eventual
replication:
  min_commit_replicas: 1
  desired_replicas: 2
  repair_deadline: background
locality:
  owner_required: false
  automatic_read_cache: true
  prefer_session_nodes: true
conflict: branch
privacy:
  raw_content_may_leave_owner: true unless subtree policy narrows it
```

For more durable artifacts, use:

```text
mode: replicated-eventual
replication:
  min_commit_replicas: 2
  desired_replicas: 3
```

## Write Flows

### Local-Owned

1. Requester reads metadata from the responsible metadata shard or local cache.
2. If requester is owner, stage and commit locally.
3. If requester is not owner, route the write request to owner.
4. Owner checks policy and expected base version.
5. Owner stages bytes, verifies hash, commits local content, updates metadata,
   and emits `dfs.write.committed`.
6. Requester receives receipt and updates its metadata cache.

This is the current implemented behavior and remains the lowest-risk default
for repository checkouts. Read-cache replicas may be created automatically, but
they do not accept writes.

### Replicated-Confirmed

1. Coordinator creates a write intent in the responsible metadata shard with the
   expected base version.
2. Writer stages content locally and pushes/pulls content to selected replicas.
3. Each replica verifies hash and returns a durable receipt.
4. Coordinator advances visible version only after required receipts exist.
5. If receipts do not arrive before timeout, intent becomes rejected or pending
   according to policy.

The visible version update must be one shard-local transaction comparing the
current version and required receipts.

### Replicated-Eventual

1. Coordinator creates a write intent with `min_commit_replicas` and
   `desired_replicas`.
2. Writer stages content locally and computes BLAKE3.
3. Coordinator obtains durable receipts from at least `min_commit_replicas`
   eligible nodes.
4. Coordinator advances visible version through the metadata shard.
5. Replication jobs are queued for remaining desired locations.
6. Reads can return the new version from any current replica.
7. Remote caches expose stale/pending state until replication catches up.

This is suitable for first-version distributed objects. It does not require all
desired replicas to confirm before the write becomes visible, but it does
require the policy's commit threshold.

### Append-Only

1. Writer creates a new immutable version, chunk, or record object.
2. Metadata links the new entry to the append stream.
3. No in-place overwrite is allowed.
4. Replication follows confirmed or eventual settings attached to the stream.

This is suitable for logs, audit evidence, and event-linked artifacts.

### Ephemeral-Local

1. Content is written only to the local content store or policy-scoped local path.
2. No cluster metadata is required unless promoted.
3. Promotion creates a normal DFS object and emits provenance.

This is suitable for build products, scratch outputs, and temporary model files.

## Read Flows

Read routing should prefer the closest valid content:

1. local confirmed content for requested version,
2. local stale content if caller allows stale reads,
3. session-near confirmed replica,
4. owner node,
5. durable home replica,
6. relayed/roaming fetch as fallback.

For `local-owned` coding subtrees, automatic read-cache replicas are allowed
when policy permits raw content to leave the owner. These caches improve local
inspection and review but never become write authorities.

Every read result should make freshness explicit:

- exact current version,
- stale version,
- current metadata but content still repairing,
- unavailable because all eligible nodes are offline,
- rejected by policy,
- conflict requires caller decision.

## Conflict Strategy

Silent overwrite should be avoided.

Default strategies:

- `local-owned`: owner serializes writes; reject mismatched base version.
- `replicated-confirmed`: CAS intent rejects concurrent visible-version change.
- `replicated-eventual`: create conflict versions unless policy explicitly says
  last-writer-wins.
- `append-only`: no conflict for appends, only ordering metadata.
- `ephemeral-local`: local process owns conflict handling.

For coding subtrees, conflict versions should appear as explicit files or
reviewable metadata so agents can resolve them using diff and tests.

## Roaming Behavior

Roaming devices are normal members but usually learners or non-voting data-plane
nodes. They should not be required for KV quorum.

Behavior rules:

- Phone and laptop sleep should not block cluster control-plane writes.
- Reads may use cached content if policy permits stale reads.
- Writes to an offline owner should queue, fail fast, or create a conflict
  candidate according to policy.
- Ownership transfer is explicit and fenced through the control plane or
  responsible metadata shard; it is not inferred from owner unreachability.
- Pinned SIFS paths can prefetch likely-needed files before a node leaves LAN.
- Reconnect should repair by comparing manifests and receipts, not by scanning
  arbitrary directories.

Ownership transfer needs a conservative rule:

1. old owner lease expired or operator/policy requested handoff,
2. candidate owner has the latest confirmed content,
3. shard/control-plane transaction fences the old owner epoch and updates the
   responsible owner or primary,
4. old owner must reject writes from stale epochs after reconnect.

## Local-First Optimizations

The system should make common local actions cheap:

- Metadata watch cache on every active node.
- Local content-addressed blob cache.
- Pinning for active SIFS subtrees and task paths.
- Prefetch recent manifests and small files for active sessions.
- Avoid copying large repository checkouts when a local Git checkout can serve
  as the owner path.
- Store derived summaries or evidence handles when raw content may not move.
- Run build/test actions on the node that owns the checkout unless policy says
  otherwise.

Cache states should be visible to agents:

- local-current,
- local-stale,
- remote-current,
- pending-replication,
- unavailable-offline,
- conflicted,
- policy-blocked.

## Security and Policy Enforcement

Every node enforces local authority for bytes it owns or receives.

Required checks:

- cluster membership and certificate validity,
- object policy and caller capability,
- raw-content egress permission,
- encryption requirement,
- object size and chunk size limits,
- write base-version match,
- local sandbox filesystem scope,
- retention and evidence policy.

Sensitive raw content should be encrypted per filesystem or object before it
leaves the owner. The metadata plane should store hashes, versions, and policy
references, not cleartext secrets.

Policy denials emit `dfs.policy.violation` with requesting node, executing node,
filesystem, object, and reason.

## Event Families

Initial event kinds:

```text
dfs.filesystem.created
dfs.filesystem.policy-scoped
dfs.object.created
dfs.object.moved
dfs.object.tombstoned
dfs.read.served
dfs.read.rejected
dfs.write.intent.created
dfs.write.staged
dfs.write.receipt.recorded
dfs.write.committed
dfs.write.rejected
dfs.write.conflict.created
dfs.replica.requested
dfs.replica.completed
dfs.replica.failed
dfs.cache.pinned
dfs.cache.evicted
dfs.policy.violation
```

These events should carry node, filesystem, object, version, policy, content hash,
and routing metadata where applicable.

## Implementation Plan

### Phase 1: Durable Local Content Store

- Add a `LocalContentStore` to `sinomo-dfs`.
- Replace the placeholder FNV hash for persisted content with BLAKE3.
- Persist staged blobs and manifests under `$SINOMO_DATA_DIR/dfs/content`.
- Keep local metadata only as a cache; authoritative DFS metadata should be
  written through the DFS metadata backend.

### Phase 2: DFS Metadata Plane

- Add serializers and key builders for inodes, dentries, dirfrags, object
  versions, intents, receipts, and conflicts. The first implementation includes
  filesystem, path, inode, dirfrag, object-version, write-intent, receipt, and
  conflict records.
- Add a `DfsMetadataBackend` facade over a local ordered store plus append-only
  journal. The first backend is `LocalJsonMetadataBackend`; it is a durable
  correctness backend, not the final high-throughput storage engine.
- Wire `NetworkedDfsService` so filesystem/object registration, durable receipts,
  and visible-version commits can persist through `DfsMetadataBackend` while
  the in-memory `DfsMetadataStore` remains a routing cache.
- Add directory fragmentation thresholds for large directories.
- Use shard-local compare-and-append for object version updates.
- Add metadata subscription support so nodes keep local caches fresh. The first
  implementation exposes a shard journal cursor and pollable journal batches.

### Phase 2a: KV Control Plane

- Add small `sinomo-kv` control-plane records for filesystem config and metadata
  shard placement.
- Keep this keyspace separate from file/object metadata.
- Use KV only to discover or fence the metadata shard primary and replica set.

### Phase 3: First Distributed Eventual Objects

- Add deterministic placement planning for `min_commit_replicas` and
  `desired_replicas`. The first implementation uses a candidate-node placement
  request and weighted rendezvous ordering with coordinator preference.
- Add replica push/pull messages and receipt records.
- Commit after policy threshold receipts, then queue background replication.
- Surface stale/pending cache state on reads through explicit freshness results.
- Add anti-entropy repair that retries offline replicas until desired replica
  count is met.

### Phase 4: Replicated-Confirmed

- Add strict confirmed policy where `min_commit_replicas == desired_replicas`.
  The first implementation exposes `DfsReplicationPolicy::confirmed(N)` and
  rejects replicated-confirmed writes that try to use an eventual threshold.
- Add stricter read behavior that can require reads from fully confirmed replica
  sets. Confirmed reads check the required replica list against receipt-backed
  metadata when a metadata backend is present, otherwise against local routing
  metadata.
- Implement timeout handling for pending confirmed writes:
  keep pending, abort the intent, or explicitly downgrade the pending intent to
  an eventual threshold while keeping the desired replica set for repair.
- Add tests for all-replica commit, replica offline pending behavior, duplicate
  receipts, stale base version, strict threshold failure, abort, downgrade, and
  reconnect repair.

### Phase 5: Conflict Objects and Handoff

- Make conflict metadata operational:
  `DfsMetadataBackend` can persist, fetch, list, and update conflict records;
  `NetworkedDfsService` keeps a local conflict cache and exposes open conflict
  read views before returning current or stale bytes.
- For `replicated-eventual`, stale base writes and duplicate concurrent target
  versions become explicit branch-style conflict records with local candidate
  hash, visible competing hash, base version, producing node, and status.
  `replicated-confirmed` keeps the stricter Phase 4 behavior: stale commits
  reject instead of silently branching.
- Reconnect reconciliation compares local manifests with visible object
  metadata:
  matching current manifests refresh durable receipts and current locations,
  while divergent non-visible manifests create conflict records for operator or
  agent resolution.
- Owner epoch fencing uses the control-plane metadata-shard epoch. Write intents
  capture the current shard epoch, commits re-check that epoch before advancing
  the visible version, and old owners reject new writes after a shard primary
  handoff.
- Conservative handoff is explicit:
  the candidate node must already have the latest visible content hash locally,
  the control-plane shard primary transfer must compare the expected epoch, the
  epoch increments on success, and local filesystem/object ownership metadata is
  updated only after the fenced transfer succeeds.
- Added tests for conflict persistence/update, stale-write conflict read views,
  reconnect receipt/conflict reconciliation, epoch-CAS shard transfer, and old
  owner fencing after handoff.

### Phase 6: Operator and Agent Surfaces

- Add reusable operator inspection views in `sinomo-dfs`:
  status summaries, SIFS summaries, object detail views, conflict lists,
  inferred repair gaps, and agent-facing object states.
- Add a local pin-record store for operator and agent workflows. Pins are local
  records under the DFS data directory; they express desired local metadata or
  content locality without changing object ownership or replication policy.
- Add `sinomo dfs` inspection commands:
  `status`, `filesystem`, `object`, `repair`, `conflicts`, and `pin`.
  All inspection commands support `--json`; metadata commands support
  `--metadata-root <path>` for explicit shard inspection.
- Add `sinomo dfs pin list|add|remove` with optional `--pin-root <path>` for
  tests, task-layout helpers, and future prefetch services.
- The first operator repair view is derived from committed intents, desired
  replica sets, and durable receipts. It reports missing replicas; it does not
  trigger repair by itself.
- Agent-facing SIFS summaries label objects as `current`,
  `pending-write`, `pending-repair`, `conflicted`, or `metadata-only`, giving
  evolution/coding workflows a compact decision surface before opening files.
- Supervision can now consume the same operator models for replication lag,
  policy denials, stale reads, and unresolved conflicts.
