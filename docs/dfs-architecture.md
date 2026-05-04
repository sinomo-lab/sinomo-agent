# Sinomo DFS Architecture

## Purpose

Sinomo DFS is a local-first distributed workspace layer for agent workspaces,
artifacts, evidence bundles, task staging, and cross-device handoff.

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
- Public federation between unrelated users.
- Strong multi-writer POSIX semantics across unreliable mobile links.
- Requiring phones or laptops to stay online for quorum.

The DFS should make distributed workspaces reliable and inspectable without
pretending every device is always available.

## Existing Baseline

The current repository already has the first useful slice:

- `sinomo-dfs` defines workspaces, mounts, object metadata, policies, read/write
  routing, network messages, policy violation events, and a local-owned service.
- `sinomo-kv` provides the in-process etcd-like control-plane surface:
  revisions, transactions, leases, watches, snapshots, and a Raft-style test
  harness.
- `sinomo-network` has local/in-process transport, peer registry, health,
  endpoint candidates, LAN/roaming metadata, and protocol-message framing.
- `sinomo-local` reserves `$SINOMO_DATA_DIR/dfs`.
- `sinomo-supervision` already indexes `dfs.*` events for audit views.

The next implementation should evolve the DFS crate from an in-memory
local-owned skeleton into a KV-backed metadata service plus a durable local
content store.

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
capabilities, node health, and workspace policy.

## Recommended Distributed Algorithm

Use a hybrid algorithm:

1. Raft-backed KV for metadata authority.
2. Owner/coordinator protocols for object writes.
3. Deterministic policy placement for replicas and read caches.
4. Content-addressed immutable blobs with BLAKE3 hashes.
5. Watch-driven cache invalidation for metadata.
6. Anti-entropy repair after reconnect.

This is intentionally not a fully symmetric peer-to-peer filesystem. Mutable
truth lives in a small, understandable control plane. Bytes can move
peer-to-peer, but metadata, ownership, visible versions, receipts, and conflicts
are resolved through KV transactions.

### Metadata Consensus

DFS metadata uses the existing `sinomo-kv` Raft-style control plane.

Properties:

- workspace, path, object, version, intent, receipt, and conflict records are KV
  keys,
- every visible metadata mutation has a monotonic KV revision,
- writers use compare-and-swap transactions against the expected object version,
- readers may use a local watch cache but can fall back to a linearizable KV read
  for decisions that need current truth,
- phones and roaming devices should normally be learners or non-voting data
  nodes, not quorum voters.

Recommendation: keep all mutable namespace and version decisions in KV, even
when object content transfer happens directly between nodes.

### Object Write Algorithm

Mutable objects use a single logical coordinator per write. For `local-owned`,
the coordinator is the owner node. For replicated policies, the coordinator is
usually the owner node at first and can later become an elected workspace
coordinator if needed.

Algorithm:

1. Writer resolves metadata and expected base version.
2. Coordinator creates or validates a write intent.
3. Writer stages content locally and computes a BLAKE3 content hash.
4. Coordinator checks policy, object epoch, and expected base version.
5. Content is committed locally or replicated according to policy.
6. Required replica receipts are recorded in KV.
7. A single KV compare-and-swap advances the visible version.
8. Metadata watchers notify caches and session participants.

This gives linearizable visible-version changes without forcing all bytes
through the consensus log.

### Distributed Eventual Object Algorithm

Distributed eventual objects use threshold commit plus anti-entropy repair.
The concrete first-version algorithm is:

```text
Threshold Primary-Copy Object Replication with Anti-Entropy Repair
```

The primary/coordinator serializes visible versions through KV. Replica
convergence is asynchronous after the commit threshold is met.

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
5. Coordinator advances the visible version with one KV transaction comparing
   base version, intent state, epoch, and the required receipt set.
6. Replication jobs continue asynchronously until all `N` desired replicas have
   the committed version.
7. Reads prefer the nearest current replica, may fall back to the coordinator or
   owner, and may return stale content only if caller and policy allow it.

Convergence guarantee:

- If the KV control plane has quorum, at least one current committed replica
  remains reachable over time, and eligible replica nodes eventually reconnect,
  then anti-entropy repair eventually brings every desired replica to the current
  committed version.
- If concurrent disconnected writes happen, convergence is to explicit conflict
  records, not silent overwrite.

This is "eventual consistency" for content placement and cache convergence. The
visible object version is still serialized by KV when the cluster has quorum.
That boundary keeps the system understandable: replicas may lag, but the
authoritative current version does not fork silently.

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
    put /dfs/versions/<object_id>/<target_version>
    put /dfs/objects/<workspace_id>/<object_id>.visible_version = target_version
    put /dfs/intents/...state = committed
    put /dfs/replication-jobs/... for missing desired replicas
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
- stable hash of `(workspace_id, object_id, version, node_id)`.

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

The default coding-workspace policy should be `local-owned` plus automatic
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
- coding workspaces where explicit conflicts are better than hidden merges.

It also maps directly onto existing crates: `sinomo-kv` already provides
transactions and watches, `sinomo-network` already carries protocol messages,
and `sinomo-dfs` already has owner-routed local-owned behavior.

## Core Components

### Metadata Coordinator

The metadata coordinator owns path and object truth through the KV. It is a
library/service surface in `sinomo-dfs`, backed by `sinomo-kv`.

Responsibilities:

- create, update, and tombstone workspaces,
- map workspace paths to stable object IDs,
- track object kind, owner, visible version, policy, and provenance,
- track version manifests, content hashes, replica receipts, and conflicts,
- perform compare-and-swap write coordination,
- publish watchable metadata updates to local caches.

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
  objects/<object_id>/<version>/manifest.json
  staging/<intent_id>/
  gc/
metadata-cache/
  snapshot.json
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

- object or inherited directory/workspace policy,
- owner node,
- preferred nodes and allowed trust domains,
- node health and endpoint quality,
- storage capability and free-space hints,
- privacy class and raw-content egress rules,
- latency, battery, bandwidth, and roaming cost,
- active session and pinned workspace state.

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

The current `DfsNetworkMessage` can be extended rather than replaced. Large
payloads should eventually move out of single JSON frames into chunked streams,
but the first implementation can keep tests in-process and bounded.

## Metadata Model

Workspaces are named views over mounted objects and local paths.

Object identity is stable across path moves:

- `workspace_id`
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

- workspace/path APIs for filesystem-like agent workflows,
- lower-level object/version APIs for replication, repair, audit, and advanced
  tools.

The lower-level API should make object identity, visible version, requested
version, content hash, replica receipts, and conflict state explicit.

## KV Keyspace

Use slash-delimited keys and JSON values:

```text
/dfs/workspaces/<workspace_id>
/dfs/mounts/<workspace_id>/<mount_id>
/dfs/paths/<workspace_id>/<escaped_path>
/dfs/objects/<workspace_id>/<object_id>
/dfs/versions/<object_id>/<version>
/dfs/placements/<object_id>/<version>
/dfs/intents/<workspace_id>/<object_id>/<intent_id>
/dfs/receipts/<object_id>/<version>/<node_id>
/dfs/replication-jobs/<object_id>/<version>/<node_id>
/dfs/conflicts/<workspace_id>/<object_id>/<conflict_id>
/dfs/pins/<node_id>/<workspace_id>/<escaped_path>
/dfs/cache-hints/<node_id>/<workspace_id>
/dfs/gc/<node_id>/<object_id>/<version>
```

Important rule: path keys are indexes. Object and version keys are the durable
identity records.

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
the persisted and inherited workspace policy.

Default coding-workspace policy:

```text
mode: local-owned
locality:
  owner_required: true
  automatic_read_cache: true
  prefer_session_nodes: true
conflict: owner-serial
privacy:
  raw_content_may_leave_owner: true unless workspace policy narrows it
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
  raw_content_may_leave_owner: true unless workspace policy narrows it
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

1. Requester reads metadata from KV or local watch cache.
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

1. Coordinator creates a write intent in KV with expected base version.
2. Writer stages content locally and pushes/pulls content to selected replicas.
3. Each replica verifies hash and writes a receipt key.
4. Coordinator advances visible version only after required receipts exist.
5. If receipts do not arrive before timeout, intent becomes rejected or pending
   according to policy.

The visible version update must be one KV transaction comparing the current
version and required receipt keys.

### Replicated-Eventual

1. Coordinator creates a write intent with `min_commit_replicas` and
   `desired_replicas`.
2. Writer stages content locally and computes BLAKE3.
3. Coordinator obtains durable receipts from at least `min_commit_replicas`
   eligible nodes.
4. Coordinator advances visible version through KV.
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

1. Content is written only to the local content store or mounted local path.
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

For `local-owned` coding workspaces, automatic read-cache replicas are allowed
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

For coding workspaces, conflict versions should appear as explicit files or
reviewable metadata so agents can resolve them using diff and tests.

## Roaming Behavior

Roaming devices are normal members but usually learners or non-voting data-plane
nodes. They should not be required for KV quorum.

Behavior rules:

- Phone and laptop sleep should not block cluster control-plane writes.
- Reads may use cached content if policy permits stale reads.
- Writes to an offline owner should queue, fail fast, or create a conflict
  candidate according to policy.
- Ownership transfer is explicit and fenced through KV; it is not inferred from
  owner unreachability.
- Pinned workspaces can prefetch likely-needed files before a node leaves LAN.
- Reconnect should repair by comparing manifests and receipts, not by scanning
  arbitrary directories.

Ownership transfer needs a conservative rule:

1. old owner lease expired or operator/policy requested handoff,
2. candidate owner has the latest confirmed content,
3. KV transaction fences the old owner epoch and updates `owner_node_id`,
4. old owner must reject writes from stale epochs after reconnect.

## Local-First Optimizations

The system should make common local actions cheap:

- Metadata watch cache on every active node.
- Local content-addressed blob cache.
- Pinning for active workspaces and task paths.
- Prefetch recent manifests and small files for active sessions.
- Avoid copying large repository checkouts when a local Git mount can serve as
  the owner path.
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

Sensitive raw content should be encrypted per workspace or object before it
leaves the owner. The metadata plane should store hashes, versions, and policy
references, not cleartext secrets.

Policy denials emit `dfs.policy.violation` with requesting node, executing node,
workspace, object, and reason.

## Event Families

Initial event kinds:

```text
dfs.workspace.created
dfs.workspace.mounted
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

These events should carry node, workspace, object, version, policy, content hash,
and routing metadata where applicable.

## Implementation Plan

### Phase 1: Durable Local Content Store

- Add a `LocalContentStore` to `sinomo-dfs`.
- Replace the placeholder FNV hash for persisted content with BLAKE3.
- Persist staged blobs and manifests under `$SINOMO_DATA_DIR/dfs/content`.
- Keep the current in-memory service for tests, but add filesystem-backed tests
  for local-owned reads and writes.

### Phase 2: KV-Backed Metadata

- Add serializers and key builders for DFS workspace, path, object, version,
  intent, receipt, and conflict records.
- Add a `KvDfsMetadataStore` facade over `sinomo-kv`.
- Use KV compare-and-swap for object version updates.
- Add metadata watch support so nodes keep local caches fresh.

### Phase 3: First Distributed Eventual Objects

- Add placement planning for `min_commit_replicas` and `desired_replicas`.
- Add replica push/pull messages and receipt records.
- Commit after policy threshold receipts, then queue background replication.
- Surface stale/pending cache state on reads.
- Add anti-entropy repair that re-replicates until desired replica count is met.

### Phase 4: Replicated-Confirmed

- Add strict confirmed policy where `min_commit_replicas == desired_replicas`.
- Add stricter read behavior that can require reads from fully confirmed replica
  sets.
- Implement timeout, abort, and policy downgrade behavior.
- Add tests for quorum available, replica offline, duplicate receipt, stale base
  version, strict threshold failure, and reconnect repair.

### Phase 5: Conflict Objects and Handoff

- Add explicit conflict records and conflict read views.
- Add owner epoch fencing.
- Add conservative ownership transfer for pinned or relocated workspaces.
- Add reconnect reconciliation from manifests and KV receipts.

### Phase 6: Operator and Agent Surfaces

- Add `sinomo dfs` inspection commands:
  `workspace`, `object`, `status`, `pin`, `repair`, `conflicts`.
- Add supervision audit views for replication lag, stale reads, policy denials,
  and unresolved conflicts.
- Add task-workspace helpers for evolution and coding workflows.

## First Coding Slice

The most useful next code change is Phase 1 plus the beginning of Phase 2:

1. Introduce durable content-store types in `sinomo-dfs`.
2. Introduce DFS KV key builders and JSON record helpers.
3. Add path-level and lower-level object/version API contracts.
4. Keep existing `LocalOwnedDfsService` behavior intact.
5. Add focused tests proving a local-owned object can be staged, committed,
   persisted, reopened, and routed through existing metadata.
6. Add tests proving read-cache replicas are never write authorities.

That preserves the current working skeleton while creating the foundation needed
for replicated policies.
