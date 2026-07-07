# Sinomo DFS Architecture

## Purpose

Sinomo DFS is a local-first distributed object and metadata layer for agent artifacts, evidence bundles, task staging, and cross-device handoff. It exposes a default virtual filesystem named `sifs` (Sinomo File System). SIFS is the root filesystem for the cluster and many cluster operations depend on it. From `sinomo-dfs`'s perspective, SIFS is not a special storage engine; it is the default filesystem with predefined subtree policies. See [SIFS](sifs.md) for the cluster filesystem layout.

The design borrows the useful shape of Ceph:

- a small replicated control plane for cluster and placement truth,
- separate metadata and content planes,
- content-addressed objects stored by many storage participants,
- policy-driven placement instead of hardcoded replication,
- explicit handling of node loss, stale caches, and recovery.

It intentionally differs from Ceph in scale and priority. Sinomo optimizes for a small trusted cluster of desktops, phones, home servers, and roaming devices. Local access, privacy, battery, and intermittent connectivity matter more than fleet-scale throughput.

## Non-Goals

- Replacing the host filesystem.
- Replacing Git for source history and merge review.
- Storing large file bytes in the cluster KV.
- Storing per-file, per-directory-entry, object-version, or replica-receipt metadata in the cluster KV.
- Public federation between unrelated users.
- Strong multi-writer POSIX semantics across unreliable mobile links.
- Requiring phones or laptops to stay online for quorum.

The DFS should make distributed SIFS objects reliable and inspectable without pretending every device is always available.

## Current Implementation

The repository now has the first production-oriented DFS slice:

- `sinomo-dfs` defines the default `sifs` filesystem, subtree policies, object metadata, POSIX-like path APIs, read/write routing, network messages, policy violation events, and local-owned directory exposure.
- `LocalContentStore` stores immutable content-addressed blobs plus object-version manifests and supports chunked writes, extent manifests, range reads, range streams, write-at patches, scrub, version GC, blob GC, quotas, and content stats.
- `LocalSobjMetadataBackend` stores sharded durable metadata as compact SOBJ table snapshots plus packed table and journal logs. File/object metadata, versions, intents, receipts, namespace transactions, visible-version commits, namespace jobs, tombstone compaction state, and future split-cluster conflict records do not live in the cluster KV.
- `NetworkedDfsService` can run local-owned, replicated-eventual, and replicated-confirmed object workflows, route writes to metadata-shard primaries, apply metadata journal batches incrementally, and rebuild pending repair work from persisted intents and receipts after restart.
- `sinomo-kv` remains the compact control plane for filesystem config, metadata-shard placement, service discovery, and fenced primary epochs.
- `sinomo-network` carries chunked DFS data-plane messages with range offsets, transfer windows, and per-chunk checksums.
- `sinomo-local` reserves DFS content, metadata, operator, pin, and local-owned file exposure paths under the Sinomo data directory.
- `sinomo-daemon` runs DFS maintenance: namespace-job resume, lifecycle maintenance, anti-entropy cursor persistence, and bounded metadata journal publish/catch-up.

CLI inspection and benchmark artifacts may use JSON/JSONL because they are intended for humans and agents to read. Internal DFS storage uses SOBJ or packed binary logs, with legacy JSON readers only where migration is required.

## Implemented Scope

The current DFS version supports these object classes:

1. `local-owned` objects with owner-routed writes and cacheable remote reads.
2. `replicated-eventual` objects that commit at a policy threshold and repair toward the desired replica set.
3. `replicated-confirmed` objects that require the desired replica set before visible commit.

`local-owned` remains the default for repository checkouts and sensitive local files. The owner is the only write authority. Other nodes may cache confirmed versions for fast local reads when policy permits raw content transfer.

Distributed eventual objects are for artifacts, task handoff bundles, generated outputs, shared evidence, and work products that should survive a single node loss without requiring every desired replica to acknowledge every write.

For distributed eventual objects, policy separates:

- `min_commit_replicas`: how many durable receipts are needed before the version becomes visible,
- `desired_replicas`: how many replicas the cluster should eventually maintain,
- `eligible_nodes`: nodes or trust domains that may hold raw content,
- `read_staleness`: whether stale confirmed versions may be returned,
- `repair_deadline`: how aggressively the cluster should converge after a node reconnects.

This mirrors the useful Ceph distinction between the replicas needed to accept a write and the target replica count the cluster repairs toward later. Sinomo exposes that distinction through `DfsReplicationPolicy` and persisted write-intent/receipt records.

## Consistency Invariant

SIFS/DFS guarantees strong eventual consistency for normal cluster operation. When KV quorum is available and the responsible owner or metadata-shard primary can be reached through `sinomo-network`, DFS must serialize writes through that authority and must not create conflict records. Concurrent writes are resolved by compare-and-append metadata transactions: one write advances the visible version, stale-base writers retry or fail, and intents remain pending until their receipt policy is satisfied or they are rejected. Replicas and caches may lag, but the authoritative visible version does not fork. Any normal quorum-backed path that records a conflict is a correctness bug.

Conflict records are reserved for future split-cluster or disconnected-authority recovery, where a partitioned cluster might later discover two independently accepted histories. That mode is future work and must be backed by an explicit reconciliation algorithm before it can be enabled. In the current design, no-quorum or unreachable-authority writes queue, remain pending, or fail fast; they do not become normal conflicts.

## System Shape

Ceph term mapping for Sinomo:

| Ceph-ish role | Sinomo role |
|---|---|
| Monitor | `sinomo-kv` replicated control plane |
| MDS | DFS metadata coordinator and local metadata cache |
| OSD | node-local DFS content store |
| CRUSH map | policy and placement planner |
| RADOS object | DFS object version/chunk |

Sinomo does not copy Ceph's placement group complexity at this stage. A small cluster can compute object placement directly from object metadata, node capabilities, node health, and inherited subtree policy.

## Recommended Distributed Algorithm

Use a hybrid algorithm:

1. Raft-backed KV only for compact control-plane configuration.
2. Sharded DFS metadata service for namespace, version, and receipt records.
3. Owner/coordinator protocols for object writes.
4. Deterministic policy placement for replicas and read caches.
5. Content-addressed immutable blobs with BLAKE3 hashes.
6. Journal-driven metadata catch-up and anti-entropy repair after reconnect.

This is intentionally not a fully symmetric peer-to-peer filesystem. Cluster configuration lives in a small, understandable KV control plane. Bytes can move peer-to-peer. High-cardinality file metadata lives in sharded metadata stores that are replicated only to the nodes responsible for those shards.

### Control Plane

DFS uses the existing `sinomo-kv` Raft control-plane core for small, duplicated configuration only. The DFS service talks to it through a small control-plane KV client interface so production can bind the same code to the networked KV service and tests can use the deterministic durable Raft harness.

Properties:

- filesystem config, metadata shard placement, policy defaults, service discovery, and owner/primary epochs are KV keys,
- KV data must remain small enough to duplicate across all control-plane voters,
- file paths, dentries, inodes, object versions, chunk manifests, write intents, replica receipts, and future split-cluster conflict records are not stored in KV,
- phones and roaming devices should normally be learners or non-voting data nodes, not quorum voters.

Recommendation: keep only cluster-wide routing and placement decisions in KV. Namespace and version decisions belong to DFS metadata shards.

### Metadata Plane

The metadata plane is a CephFS-MDS-like service with Sinomo local-first adjustments:

- metadata is partitioned into SIFS metadata shards,
- shard ownership and replica sets are configured by the KV control plane,
- each shard stores records in a local ordered store, not global KV,
- the current local backend materializes that ordered store as compact table snapshots plus a bounded tail log, so reopen cost follows current metadata size and recent churn rather than all historical table updates,
- namespace transactions are the preferred batching primitive for large metadata changes; they can carry object and visible-version records together so first-publication commits do not require a separate version append,
- each shard has an append-only journal for mutation replication and replay,
- large directories split into directory fragments by filename hash,
- directory/object/intent scans have cursor APIs and service hydration walks pages instead of loading one bulk vector for each record class; conflict scans are reserved for future split-cluster reconciliation metadata,
- metadata journal batches are bounded by entry count and encoded size and apply to service state incrementally instead of forcing full `hydrate_from_metadata_backend()` after every received batch,
- shard replicas accept journal batches and compact snapshots only from the current configured shard primary,
- roaming devices can hold read caches or learner replicas without becoming quorum voters.

The logical metadata key model is:

```text
inode/{inode_id}
dirfrag/{dir_inode}/{frag_id}/dentry/{name_hash}/{name}
version/{object_id}/{version}
receipt/{object_id}/{version}/{replica_node_id}
conflict/{file_system_id}/{conflict_id}        # future split-cluster recovery only
journal/{shard_id}/{sequence}
```

This gives the system room for millions of files in one build tree without putting millions of keys into a cluster-wide KV replicated to every node.

### Object Write Algorithm

Mutable objects use a single logical coordinator per write. For `local-owned`, the coordinator is the owner node. For replicated policies, the coordinator is usually the owner node at first and can later become an elected metadata-shard coordinator if needed.

Algorithm:

1. Writer resolves metadata and expected base version.
2. Coordinator creates or validates a write intent.
3. Writer stages content locally and computes a BLAKE3 content hash.
4. Coordinator checks policy, object epoch, and expected base version.
5. Content is committed locally or replicated according to policy.
6. Required replica receipts are recorded in the metadata shard journal/store.
7. A shard-local compare-and-append advances the visible version.
8. Metadata subscribers notify caches and session participants.

This gives serialized visible-version changes within the responsible metadata shard without forcing file metadata or bytes through the cluster KV.

### Distributed Eventual Object Algorithm

Distributed eventual objects use threshold commit plus anti-entropy repair. The concrete first-version algorithm is:

```text
Threshold Primary-Copy Object Replication with Anti-Entropy Repair
```

The primary/coordinator serializes visible versions through the responsible metadata shard. Replica convergence is asynchronous after the commit threshold is met.

Definitions:

- `N`: number of desired replicas selected by placement policy.
- `W`: number of durable receipts required before a version is visible.
- `W <= N`.
- `W` is usually `1` or `2` for local-first clusters.
- `N` is usually `2` or `3` for the first version.

Commit protocol:

1. Coordinator computes the desired replica set using placement policy.
2. Coordinator creates a write intent with `N`, `W`, selected replicas, base version, target version, owner/coordinator epoch, and BLAKE3 hash.
3. Writer stages content locally.
4. Coordinator sends `StageBlob` to selected replicas until at least `W` eligible nodes have durable receipts.
5. Coordinator advances the visible version with one metadata-shard transaction comparing base version, intent state, epoch, and the required receipt set.
6. Replication jobs continue asynchronously until all `N` desired replicas have the committed version.
7. Reads prefer the nearest current replica, may fall back to the coordinator or owner, and may return stale content only if caller and policy allow it.

Convergence guarantee:

- If the metadata shard has a live primary or can elect/fence a new primary, at least one current committed replica remains reachable over time, and eligible replica nodes eventually reconnect, then anti-entropy repair eventually brings every desired replica to the current committed version.
- If quorum and the metadata authority are available, concurrent writes do not create conflicts: the metadata transaction serializes one visible version and rejects or retries stale writers. Split-cluster disconnected writes are future work; if that mode is enabled later, convergence must be to explicit conflict records, not silent overwrite.

This is "eventual consistency" for content placement and cache convergence. The visible object version is still serialized by the metadata shard. That boundary keeps the system understandable: replicas may lag, but the authoritative current version does not fork silently.

Intent states:

```text
open -> staging -> commit-ready -> committed
                  -> pending-repair
                  -> aborted
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
    leave intent pending or reject/retry according to policy
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
    quarantine local blob and repair from authoritative metadata
```

### Placement Algorithm

Use deterministic weighted rendezvous selection rather than Ceph-style placement groups for the first implementation.

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

The stable hash prevents all objects from clustering on one node while still allowing policy and current health to dominate. If the cluster is tiny, the algorithm degenerates cleanly to owner plus durable home plus active session caches.

### Read Cache Algorithm

The default coding-subtree policy is `local-owned` plus automatic read-cache replicas.

Rules:

- writes still route to owner,
- successful owner reads can populate the requester cache when policy allows,
- active session participants receive cache hints for small and recently touched files,
- cached content is keyed by `(object_id, version, blake3_hash)`,
- metadata watches mark cached content stale when visible version changes,
- stale reads require explicit caller permission,
- cache eviction never mutates authoritative metadata.

This keeps repository ownership simple while making mobile inspection and cross-device review fast.

### Anti-Entropy Repair

Repair is manifest-based, not directory-scan-based.

On reconnect or periodic background tick:

1. Node compares local manifests against metadata-shard version and receipt records.
2. Missing confirmed blobs are pulled from owner or eligible replicas.
3. Extra unreferenced blobs become garbage-collection candidates.
4. Pending receipts are retried if the local blob hash matches the version manifest.
5. Divergent local blobs are quarantined unless they match authoritative metadata. Divergent independently accepted writes are only possible in future split-cluster recovery mode.
6. Distributed eventual objects are re-replicated until their desired replica count is satisfied.

This is the main roaming recovery loop.

### Failure Semantics

- No KV quorum: no visible metadata writes for distributed objects.
- Owner offline for `local-owned`: writes queue or fail fast according to policy; ownership does not move automatically.
- Too few eligible receipts for distributed eventual objects: commit waits, degrades to pending, or rejects based on timeout policy.
- Some desired replicas offline after threshold commit: visible version may commit, and repair later converges replicas toward policy.
- Cache stale: reads can continue only when caller and policy allow stale reads.
- Split brain: stale owner epochs are fenced by KV; reconnected old owners must not coordinate writes from obsolete epochs. They may route client writes to the current metadata-shard primary, or reject if that route is unavailable.

### Why This Algorithm

This is the best fit for Sinomo's expected cluster shape:

- small node count,
- high value on inspectability,
- local-first access,
- intermittent mobile devices,
- policy-sensitive raw content,
- coding subtrees where future split-cluster conflicts must be explicit rather than hidden merges.

It also maps directly onto existing crates: `sinomo-kv` already provides a small replicated control plane, `sinomo-network` already carries protocol messages, and `sinomo-dfs` already has owner-routed local-owned behavior.

## Core Components

### Metadata Coordinator

The metadata coordinator owns path and object truth through DFS metadata shards. It is a library/service surface in `sinomo-dfs`, backed by shard-local ordered stores and append-only journals. `sinomo-kv` only tells nodes where shards live and which epoch/primary is valid.

Responsibilities:

- create, update, and tombstone namespace records,
- map filesystem paths to stable object IDs,
- track object kind, owner, visible version, policy, and provenance,
- track version manifests, content hashes, replica receipts, and future split-cluster conflicts,
- perform shard-local compare-and-append write coordination,
- publish journal-backed metadata updates to local caches.

The coordinator does not store file content.

### Local Content Store

Every node that can store DFS bytes runs a local content store under:

```text
$SINOMO_DATA_DIR/dfs/content/
```

Current layout:

```text
content/
  blobs/<hash>
  blob-packs/index.sobj
  blob-packs/segments/<segment>.pack
  objects/<object_id>/<version>/manifest.sobj
  stats.sobj
  staging/<intent_id>/
```

The content store is append-first: write staged bytes or chunked staging files, fsync as needed, verify hashes, then publish a manifest or receipt. Small blobs may be appended into packed segments and resolved through the packed index to avoid one loose file per tiny object. Visible metadata advances only after the relevant policy allows it.

Content identity uses BLAKE3. The canonical content-hash string is:

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

The data plane moves bytes between nodes over `sinomo-network` protocol messages. It supports:

- digest probe: "do you have object/version/hash?",
- manifest fetch,
- full blob fetch,
- range fetch,
- staged blob push,
- replica receipt,
- repair pull,
- abort staging.

`DfsNetworkMessage` carries whole-object fast paths for small objects and chunk messages for large stage, fetch, read, and repair flows. Chunk messages include object/version metadata, range offsets where relevant, total size, chunk count, and optional per-chunk BLAKE3 checksum. Senders advance bounded transfer windows from disk-backed range streams, so retries and repair do not need to load an entire blob into memory.

## SIFS Cluster Root

SIFS is the cluster root filesystem. It is the default DFS namespace exposed to operators, agents, cluster services, and rollout/self-evolution workflows. Higher-level concepts such as evolution workspaces are layered onto SIFS paths; DFS itself records object identity, path metadata, versions, locations, receipts, future split-cluster conflicts, and inherited subtree policy.

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

`/sinomo/nodes/<node_id>/files` is a local-owned subtree where a node exposes its local `$SINOMO_DATA_DIR/files` directory. `/users` and `/services` are true distributed DFS subtrees for cluster-shared user and service state.

## Metadata Model

SIFS is the default DFS namespace and cluster root filesystem. Agents and higher-level services can map their own directory layouts onto SIFS, but DFS metadata only records object identity, paths, versions, locations, receipts, future split-cluster conflicts, and inherited subtree policy.

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

Version metadata is split from object metadata:

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

Future split-cluster conflict metadata is first-class but dormant in normal operation:

- conflicted object ID,
- base version,
- local candidate version,
- competing visible version,
- producing node,
- split-recovery strategy,
- resolution status.

The public DFS API exposes both levels:

- SIFS path APIs for filesystem-like agent workflows,
- lower-level object/version APIs for replication, repair, audit, and advanced tools.

The lower-level API makes object identity, visible version, requested version, content hash, replica receipts, and future split-cluster conflict state explicit.

## Control-Plane Keyspace

Use slash-delimited `sinomo-kv` keys and typed compact values only for configuration:

```text
/dfs/filesystems/<file_system_id>/config
/dfs/filesystems/<file_system_id>/metadata-shards/<shard_id>
/dfs/policies/<policy_id>
/dfs/services/metadata/<node_id>
/dfs/services/content/<node_id>
```

Important rule: this keyspace must not contain per-path, per-object, per-version, receipt, or future split-cluster conflict records.

## Metadata-Shard Keyspace

Each metadata shard uses its own local ordered store and journal. The current backend persists compact SOBJ table snapshots plus packed table and journal logs, but the logical records are:

```text
inode/<inode_id>
dirfrag/<dir_inode>/<frag_id>/dentry/<name_hash>/<name>
object/<object_id>
version/<object_id>/<version>
intent/<object_id>/<intent_id>
receipt/<object_id>/<version>/<node_id>
conflict/<object_id>/<conflict_id>        # future split-cluster recovery only
namespace-transaction/<transaction_id>
namespace-job/<job_id>
pin/<node_id>/<file_system_id>/<escaped_path>
cache-hint/<node_id>/<file_system_id>
gc/<node_id>/<object_id>/<version>
journal/<sequence>
```

Important rule: these records are replicated by shard policy, not duplicated to every KV voter.

## Policy Model

The existing `DfsObjectPolicy` enum is useful for routing decisions, but the implementation needs a structured policy record behind it.

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
  normal: metadata-authority-serial
  split_recovery: reject | branch | resolver
retention:
  ttl
  keep_versions
  gc_class
limits:
  max_object_bytes
  max_chunk_bytes
```

The enum can remain the public quick mode while the structured policy becomes the persisted and inherited subtree policy.

Default coding-subtree policy:

```text
mode: local-owned
locality:
  owner_required: true
  automatic_read_cache: true
  prefer_session_nodes: true
conflict:
  normal: owner-serial
  split_recovery: reject
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
conflict:
  normal: metadata-authority-serial
  split_recovery: branch
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
5. Owner stages bytes, verifies hash, commits local content, updates metadata, and emits `dfs.write.committed`.
6. Requester receives receipt and updates its metadata cache.

This is the current implemented behavior and remains the lowest-risk default for repository checkouts. Read-cache replicas may be created automatically, but they do not accept writes.

### Replicated-Confirmed

1. Coordinator creates a write intent in the responsible metadata shard with the expected base version.
2. Writer stages content locally and pushes/pulls content to selected replicas.
3. Each replica verifies hash and returns a durable receipt.
4. Coordinator advances visible version only after required receipts exist.
5. If receipts do not arrive before timeout, intent becomes rejected or pending according to policy.

The visible version update must be one shard-local transaction comparing the current version and required receipts.

### Replicated-Eventual

1. Coordinator creates a write intent with `min_commit_replicas` and `desired_replicas`.
2. Writer stages content locally and computes BLAKE3.
3. Coordinator obtains durable receipts from at least `min_commit_replicas` eligible nodes.
4. Coordinator advances visible version through the metadata shard.
5. Replication jobs are queued for remaining desired locations.
6. Reads can return the new version from any current replica.
7. Remote caches expose stale/pending state until replication catches up.

This is suitable for first-version distributed objects. It does not require all desired replicas to confirm before the write becomes visible, but it does require the policy's commit threshold.

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

Read routing prefers the closest valid content:

1. local confirmed content for requested version,
2. local stale content if caller allows stale reads,
3. session-near confirmed replica,
4. owner node,
5. durable home replica,
6. relayed/roaming fetch as fallback.

For `local-owned` coding subtrees, automatic read-cache replicas are allowed when policy permits raw content to leave the owner. These caches improve local inspection and review but never become write authorities.

Every read result makes freshness explicit:

- exact current version,
- stale version,
- current metadata but content still repairing,
- unavailable because all eligible nodes are offline,
- rejected by policy,
- future split-cluster conflict requires caller decision.

## Write Serialization And Future Split Recovery

Silent overwrite is avoided. During normal operation, conflict creation is also avoided: quorum-backed metadata authority serializes visible writes and rejects or retries stale-base writers.

Normal strategies:

- `local-owned`: owner serializes writes; reject mismatched base version.
- `replicated-confirmed`: CAS intent rejects concurrent visible-version change.
- `replicated-eventual`: metadata-shard authority serializes visible-version changes after the commit threshold is met; stale-base writes retry or reject.
- `append-only`: no conflict for appends, only ordering metadata.
- `ephemeral-local`: local process owns local overwrite behavior until content is promoted.

Future split-cluster recovery may introduce conflict records when two isolated authorities later discover divergent committed histories. That mode is not enabled by normal DFS operation and needs a documented reconciliation algorithm before use. For coding subtrees, future conflict versions should appear as explicit files or reviewable metadata so agents can resolve them using diff and tests.

## Roaming Behavior

Roaming devices are normal members but usually learners or non-voting data-plane nodes. They should not be required for KV quorum.

Behavior rules:

- Phone and laptop sleep should not block cluster control-plane writes.
- Reads may use cached content if policy permits stale reads.
- Writes to an offline owner should queue or fail fast according to policy.
- Ownership transfer is explicit and fenced through the control plane or responsible metadata shard; it is not inferred from owner unreachability.
- Pinned SIFS paths can prefetch likely-needed files before a node leaves LAN.
- Reconnect should repair by comparing manifests and receipts, not by scanning arbitrary directories.

Ownership transfer needs a conservative rule:

1. old owner lease expired or operator/policy requested handoff,
2. candidate owner has the latest confirmed content,
3. shard/control-plane transaction fences the old owner epoch and updates the responsible owner or primary,
4. old owner must reject writes from stale epochs after reconnect.

## Local-First Optimizations

The system makes common local actions cheap:

- Metadata watch cache on every active node.
- Local content-addressed blob cache.
- Pinning for active SIFS subtrees and task paths.
- Prefetch recent manifests and small files for active sessions.
- Avoid copying large repository checkouts when a local Git checkout can serve as the owner path.
- Store derived summaries or evidence handles when raw content may not move.
- Run build/test actions on the node that owns the checkout unless policy says otherwise.

Cache states are visible to agents:

- local-current,
- local-stale,
- remote-current,
- pending-replication,
- unavailable-offline,
- split-conflicted (future split-cluster recovery only),
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

Sensitive raw content should be encrypted per filesystem or object before it leaves the owner. The metadata plane should store hashes, versions, and policy references, not cleartext secrets.

Policy denials emit `dfs.policy.violation` with requesting node, executing node, filesystem, object, and reason.

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
dfs.write.conflict.created        # future split-cluster recovery only
dfs.replica.requested
dfs.replica.completed
dfs.replica.failed
dfs.cache.pinned
dfs.cache.evicted
dfs.policy.violation
```

These events carry node, filesystem, object, version, policy, content hash, and routing metadata where applicable.

## Implementation Status

### Scale And Benchmarking

- `crates/sinomo-dfs/benches/scale.rs` covers namespace load, full and paginated listing, create rate, restart hydration, metadata replication catch-up, directory jobs, local blob streaming/range reads, network blob transfer/range reads, and network write-at patches.
- Presets include `smoke`, `scale`, and `production`; the production preset targets 1M namespace objects and multi-GB local/network blob paths.
- The benchmark emits JSONL because it is a human/agent review artifact, not an internal DFS storage format.

### Metadata Scaling

- Backend scans expose cursor APIs for object, intent, receipt, future split-cluster conflict, namespace-job, and directory-fragment records.
- Service restart hydration walks pages and reconstructs coordinator, repair, future split-cluster conflict, and tombstone state from backend records.
- Metadata journal replication uses bounded batches and applies each journal entry directly to service state. Sparse legacy entries are hydrated from keyed backend records when possible; unrecoverable namespace transaction entries fail explicitly instead of importing the whole filesystem.

### Write Amplification

- `put_objects`, grouped object/version commits, visible-version commits, and namespace transactions batch journal/table writes where the operation semantics allow it.
- Materialized metadata is stored as packed table batches and compact snapshots, not one file per object/path/version/receipt.
- Local materialization cursors checkpoint after a sequence interval and replay idempotently on open, avoiding an fsync-triggering cursor write for every metadata mutation.

### Large Files And Network Transfer

- Object versions are manifests over content extents. Whole-object blobs remain the fast path for small or naturally contiguous writes, while write-at patches create extent manifests rather than rewriting the base object.
- Local APIs provide streaming writes/reads, bounded range reads, and range streams.
- Network stage, repair, fetch, and remote-read paths use chunk windows, resumable chunk indexes, range offsets, and optional per-chunk hashes. Transfer state is bounded and stale in-flight state is pruned.

### Directory Operations

- Directory listing is page-first through directory fragments. Non-paged helpers are convenience wrappers over paged scans.
- Recursive clear and directory rename are durable bounded namespace jobs. Jobs persist in the metadata backend, resume after reopen, and daemon maintenance can resume pending jobs in bounded batches.

### Namespace Operation Routing

POSIX namespace mutations (`mkdir`, `mkdir_all`, `rmdir`, `unlink`, `rename`, `hardlink`, `set_mode`, `set_owner`, `set_times`, recursive clear) route to the metadata-shard primary when the local node is not the namespace authority for the path, mirroring routed content writes:

- the request carries the requester's per-shard journal cursors,
- the primary validates authority over every involved path (and its parent), executes the operation, and journals it,
- the response carries journal catch-up batches per affected shard plus a structured POSIX error kind, so errno semantics and read-your-write both survive routing.

The load-bearing invariant behind this design: a shard replica's backend journal must stay sequence-aligned with the primary's stream, so replicas never append local entries for replicated shards. Implicit atime updates on non-primary nodes are skipped (`noatime`-style) for the same reason, and parent-directory mtime updates for routed creates are journaled by the primary, not the requester.

### Cross-Shard-Primary Renames And Hardlinks

File and symlink renames (and hardlinks) whose source and destination live in metadata shards owned by *different* primaries run as a durable two-step saga coordinated by the source-side primary:

1. the coordinator validates the source, persists a `RenameObjectAcrossShardPrimaries` (or `HardlinkAcrossShardPrimaries`) namespace job, and locks the in-flight path against concurrent content writes and namespace mutations,
2. the destination primary atomically validates the destination and adopts the moved record together with its version, receipt, intent, and future split-cluster conflict history (`AdoptObjectAtPath` / `AdoptHardlinkAlias`, mirroring same-node shard migration), journaled in the destination shard,
3. the coordinator then atomically tombstones the old path, evicts the moved record from the source shard (same-object-id tombstone plus shard-index repoint, exactly the journal shape of a same-node cross-shard move), and touches the source parent.

Structured destination rejections abort the saga before anything mutated; transport failures park the job (lock held) for daemon maintenance to resume, re-resolving the destination primary from the control plane so shard transfers mid-saga are followed. After adoption commits, recovery is forward only — the move either fully applies everywhere or cleanly rejects. Unlinking a canonical record whose surviving hardlink alias lives under another primary promotes it through the same saga. Requesters regain read-your-write for the foreign half via a `SyncPath` round to the destination primary. Intent ids cover the target version so a new shard authority can never mint an id that collides with the previous coordinator's history.

Directory renames across shard primaries migrate the whole subtree through the same saga, scaled up: the root record moves first (the destination primary validates an empty or absent destination and adopts atomically; the tombstoned old root then hides the not-yet-moved children), and the subtree is walked in paged, resumable `CrossShardDirScan` frames that adopt every child at its target path and evict the source record — the per-shard journal shape is identical to a same-node cross-shard move, so replicas replay it with the existing machinery. The in-flight lock covers the entire source subtree (descendants and ancestors), freezing writes and namespace mutations against half-moved objects until the saga completes or aborts.

Cross-shard saga safety rails:

- Moves fence on in-flight distributed write state (open intents, pending writes, outstanding repairs) and reject with the retriable `Busy` error (`EAGAIN` at the POSIX boundary) so a settling write can never commit into a shard its node no longer owns; adopt frames re-read the object's current state on every attempt, so parked sagas resume against reality rather than a stale snapshot.
- Adoptions are authority-checked on both ends: the receiver must own the destination shard, and the sender must be the configured primary of the record's source path.
- Saga execution honors the bounded-step budget shared with the other namespace jobs, and adoption bundles cap shipped version history (older records remain in the source shard until lifecycle GC).
- Routed-operation responses carry a bounded run of journal batches toward the shard head; requesters that remain behind continue with bounded `SyncPath` rounds, so read-your-write survives deep catch-up.
- Replica-side object resolution prefers live records over cross-shard eviction tombstones, independent of shard naming or replay order.
- A replica whose journal forked from the primary's stream (for example a locally bootstrapped node joining as a follower) is repaired in-service: a sequence-conflict rejection triggers a per-shard snapshot install over the same transport, mirroring the standalone publisher's reconciliation.
- Stray routed-operation responses (late, duplicate, or retried elsewhere) are discarded as events and never fail the node's event loop.

### Lifecycle And Operations

- Repair jobs are reconstructed from persisted committed intents plus durable receipts, so missing-replica work survives service restart without a separate volatile queue.
- Lifecycle maintenance can scrub content, run local content anti-entropy, collect old object versions, reclaim orphan blobs, compact tombstones, and report skipped work when a metadata backend is absent.
- Content quotas cover blob, logical object, manifest, and staging bytes.
- Operational metrics report repair backlog, metadata counts, content footprint, transfer windows, and incoming transfer progress.
- DFS expose config defaults to `configs/dfs/expose.json` as a shared `sinomo-config` document; legacy `config/dfs-expose.sobj` and `config/dfs-expose.json` are read for migration, and explicit `.sobj` or `.json` paths remain supported for compatibility.
