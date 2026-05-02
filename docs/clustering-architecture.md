# Sinomo Clustering Architecture

## Purpose

Sinomo clustering turns a single local runtime into a small to medium trusted node set. The cluster is the control plane for distributed sessions, service placement, replicated configuration, and the distributed filesystem metadata layer.

The first implementation should optimize for correctness, debuggability, and local-first operation rather than internet-scale membership. A normal cluster is expected to fit comfortably in memory on every node. Every node may store basic metadata for every other node, including identity, capabilities, advertised endpoints, health, and replication role.

## Non-Goals

- Public federation between unrelated users.
- Large fleet orchestration.
- Anonymous peer-to-peer membership.
- Replacing Git, object stores, or local filesystems.
- High-throughput data-plane replication for large file content in the cluster KV.

The cluster KV is for compact, strongly ordered control-plane state. Large artifacts and file bytes belong in DFS content stores, local filesystems, Git repositories, or future object stores.

## CLI Surface

The cluster lifecycle starts with `sinomo cluster`.

Commands:

```text
sinomo cluster
sinomo cluster init [--name <name>] [--cluster-id cluster_...] [--node-id node_...]
sinomo cluster join <invite> [--name <name>] [--cluster-id cluster_...] [--node-id node_...]
sinomo cluster status
sinomo cluster peers
sinomo cluster leave
```

An uninitialized node is an orphan. Orphan nodes must not run distributed operations, write distributed config, mount distributed DFS namespaces, accept remote action requests, or publish cluster-routed events. The only allowed cluster operations while orphaned are:

- `sinomo cluster status`
- `sinomo cluster init`
- `sinomo cluster join`
- local help and credential setup commands that do not mutate cluster state

`sinomo cluster init` creates a single-node cluster. `sinomo cluster join` records an intent to join an existing cluster and moves the node into pending learner state until the cluster control plane accepts it.

Initial local state lives at:

```text
$SINOMO_DATA_DIR/cluster/state.json
```

The state file uses the shared `ClusterBootstrapState` contract from `sinomo-core`. It is only a bootstrap artifact. Once the cluster service is implemented, durable membership and config state come from the replicated KV log. The file remains the local anchor that tells the daemon which cluster identity and local node identity to use at startup.

## Identity Model

Cluster identity:

- `cluster_id`: stable cluster identifier, prefixed with `cluster_`.
- `cluster_name`: operator-friendly display name.
- `cluster_epoch`: monotonic epoch incremented only by explicit destructive re-bootstrap or disaster recovery.
- `created_at`: bootstrap timestamp.

Node identity:

- `node_id`: stable Sinomo node ID, prefixed with `node_`.
- `node_public_key`: long-lived node identity key.
- `node_certificate`: short-lived cluster-issued certificate for transport authentication.
- `display_name`: operator-friendly name.
- `role`: workstation, mobile, server, sensor, or test.
- `capabilities`: advertised observable capabilities.
- `allowed_capabilities`: policy-filtered capabilities this node may use.
- `addresses`: current direct and relay candidates.
- `health`: available, degraded, suspect, offline, or removed.

Cluster membership is explicit. Seeing a node on the LAN is never enough to trust it. Discovery can find candidates, but only signed membership records authorize cluster participation.

## Bootstrap and Join

### Init

`sinomo cluster init`:

1. Generates or accepts a `cluster_id`.
2. Generates or accepts a local `node_id`.
3. Creates a cluster root key or uses a future operator-provided key.
4. Creates the first member as an active voter.
5. Creates an initial KV log entry containing cluster metadata, member metadata, and schema versions.
6. Writes local bootstrap state under `$SINOMO_DATA_DIR/cluster/state.json`.

The first node is both control-plane voter and data-plane participant. Additional nodes join as learners first.

### Join

`sinomo cluster join <invite>`:

1. Parses the invite payload.
2. Generates or accepts a local `node_id`.
3. Stores pending bootstrap state locally.
4. Contacts one or more introducers from the invite.
5. Presents a join request containing node identity, public key, capabilities, and requested role.
6. Waits for an existing voter to commit a pending member record.
7. Starts as a learner.
8. Promotes to voter only after snapshot catch-up, health checks, and policy approval.

Invite payload fields:

- `cluster_id`
- introducer endpoints
- join token ID
- token expiry
- allowed trust domain
- allowed node roles
- optional relay or signaling endpoints
- signature from an active voter or cluster authority

Join tokens are one-time or short-lived. A leaked token should not grant ongoing authority after the token expires or after a successful use.

## Orphan Guard

The orphan guard is a hard runtime boundary.

Allowed while orphaned:

- create or inspect local bootstrap state
- print help

Blocked while orphaned:

- starting the cluster-aware daemon
- distributed DFS mounts
- replicated config reads or writes
- event routing with `TrustedNetwork` or `DurableGlobal` scope
- remote action requests
- self-evolution rollout that targets more than local dry-run artifacts
- accepting inbound peer messages

Implementation rule: code paths that depend on cluster authority call a small `require_cluster_state` guard before doing work. The guard first checks local bootstrap state, then asks the cluster service for an active member view once the daemon exists.

## Membership Model

The cluster stores full basic membership metadata on every node.

Member states:

| State | Meaning |
|---|---|
| `pending` | Join request recorded, not yet trusted for normal work |
| `learner` | Receives log replication and can serve limited local reads |
| `voter` | Participates in consensus quorum |
| `non-voting` | Trusted data-plane node that does not affect quorum |
| `suspect` | Heartbeats or probes are failing; avoid new work |
| `offline` | Not currently reachable; preserve durable membership |
| `removed` | No longer authorized; reject old certificates and sessions |

Small clusters should keep an odd voter count, usually 1, 3, or 5. Mobile and intermittently connected nodes should usually be learners or non-voting members, not voters.

Membership changes are KV log entries. A node cannot unilaterally promote itself, remove peers, or change quorum rules.

## Transport Strategy

Connections are local-first and roam-capable.

Transport preference:

1. In-process transport for tests and single-process simulation.
2. Loopback for local daemon clients.
3. LAN direct QUIC for same-network nodes.
4. LAN TCP only for development and compatibility.
5. Iroh QUIC endpoints for roaming across NATs, using endpoint public keys, direct addresses, and relay URL fallback.
6. WebRTC data channels for browsers or environments where iroh is unavailable.
7. TURN relay or an HTTPS relay for difficult networks.

Discovery mechanisms:

- Static endpoints from local bootstrap state and KV membership records.
- mDNS/DNS-SD for LAN candidates.
- UDP hole-punch candidates where available.
- Iroh endpoint records published through the KV under `/iroh/endpoints/<cluster>/<node>`.
- WebRTC ICE candidates for cross-network paths.
- Optional user-hosted rendezvous/signaling service.

Discovery finds paths, not authority. Transport sessions are authorized only after mutual authentication with cluster-issued credentials.

Path selection is continuous. A node may move from LAN QUIC to iroh, WebRTC relay, and back without changing node identity or session membership. The peer registry stores multiple endpoint candidates per node and scores them by latency, reliability, privacy class, and cost.

Iroh is the preferred general roaming backend once LAN direct paths are unavailable. The cluster stores the peer endpoint public key, any known direct socket addresses, the selected relay URL or default relay mode, and a TTL. The runtime can use the optional `iroh-transport` Cargo feature to turn that record into an iroh dial configuration using the cluster ALPN `sinomo.iroh-roaming.v1`. WebRTC remains the fallback for browser surfaces and deployments that do not compile or enable iroh.

## Security Model

Baseline requirements:

- Every node has a stable identity key.
- Every transport session uses mutual authentication.
- Cluster-issued certificates are short-lived and bound to `cluster_id`, `node_id`, and capability scope.
- Membership records contain public keys or key references.
- Removed members are rejected even if they still have an old endpoint.
- Join tokens are short-lived and scoped.
- All control-plane messages include cluster ID, node ID, term, log index or revision where applicable, and protocol version.

Remote requests never bypass local authority. The node that owns a device, file owner role, secret, or local action target performs the final policy check.

## Control Plane KV

Sinomo should implement an etcd-like distributed KV for compact control-plane state. The KV provides linearizable writes, watchable revisions, leases, compare-and-swap transactions, snapshots, and log compaction.

The KV is not a general database. It stores:

- cluster metadata
- membership and endpoint records
- capability advertisements
- service registry records
- active session summaries
- DFS workspace and object metadata
- policy config
- rollout desired state
- small coordination leases

The KV does not store:

- raw audio/video
- large file contents
- build outputs
- model weights
- large memory embeddings
- unbounded event history

Those are referenced by hash, DFS object ID, event ID, local path, or future object-store handle.

### Consensus

Use Raft-style consensus for the KV.

Rationale:

- Small and medium clusters need understandable failure semantics.
- Strongly ordered config updates are more important than write throughput.
- Learners and snapshots make intermittent nodes manageable.
- The implementation maps cleanly to etcd-like revisions and watches.

Voters maintain:

- current term
- voted-for record
- replicated WAL
- committed index
- applied index
- current snapshot metadata

Learners receive the log and snapshots but do not vote. Non-voting data-plane nodes may cache KV snapshots and subscribe to watches through a nearby voter.

### Revisions and MVCC

Every committed KV mutation advances a global revision.

Each key stores:

- key bytes
- value bytes
- create revision
- modify revision
- version counter
- optional lease ID
- tombstone status

Reads:

- linearizable read for control decisions
- serializable read for cached status and UI
- prefix read for namespaces
- range read for ordered keys

Writes:

- `put`
- `delete`
- `txn(compare, success_ops, failure_ops)`
- `lease_grant`
- `lease_keepalive`
- `lease_revoke`

Watches:

- watch from a revision
- watch prefix
- compacted revision error with snapshot recovery
- progress notifications for long-lived clients

### Keyspace

Use slash-delimited namespaces:

```text
/cluster/meta
/cluster/members/<node_id>
/cluster/endpoints/<node_id>/<endpoint_id>
/cluster/capabilities/<node_id>/<capability>
/services/<service_name>/<node_id>
/sessions/<session_id>/summary
/sessions/<session_id>/participants/<node_id>
/dfs/workspaces/<workspace_id>
/dfs/objects/<workspace_id>/<object_id>
/dfs/paths/<workspace_id>/<escaped_path>
/dfs/locks/<workspace_id>/<object_id>
/policy/<domain>/<key>
/rollout/desired/<node_id>/<artifact_id>
```

Large or frequently changing values should store references to event log entries or DFS objects rather than embedding payloads directly.

### Leases

Leases are used for ephemeral liveness and coordination:

- service presence
- leader hints for non-consensus services
- session participant activity
- DFS write locks where a policy requires serialization
- temporary rollout reservations

Lease loss must be treated as a hint, not proof that durable state should be deleted. Durable membership and DFS metadata require explicit committed mutations.

## DFS Foundation

The DFS builds on the cluster KV as its metadata and coordination plane.

KV-owned DFS metadata:

- workspace records
- object metadata
- path-to-object indexes
- owner node
- current locations
- object policy
- version and content hash
- replica requirements
- conflict records
- write intent records

DFS content remains outside the KV. Content is stored locally on owner/replica nodes and addressed by object ID, version, and content hash.

Write behavior:

- `local-owned`: KV records route writes to the owner node.
- `replicated-confirmed`: KV write intent is committed, replicas store content, then visible version advances after required receipts.
- `replicated-eventual`: owner advances visible version and schedules replication.
- `append-only`: each append creates an immutable object version or chunk.
- `ephemeral-local`: no cluster KV metadata unless promoted.

The KV gives every node a consistent answer to "who owns this object, what version is current, and where can I fetch it?" The DFS data plane handles bytes separately.

## Event Routing Integration

Cluster membership feeds `sinomo-network` routing.

Routing rules:

- `Local` scope never requires cluster state.
- `Session` scope requires the session participant set if the session spans nodes.
- `TrustedNetwork` scope requires active cluster membership.
- `DurableGlobal` scope requires active cluster membership and durable replication policy.

Peer liveness changes should update both the peer registry and KV endpoint/lease state. Offline nodes remain members until explicitly removed.

## Failure Behavior

Network partition:

- Majority side continues linearizable KV writes.
- Minority voters step down and reject writes.
- Learners continue serving local cached reads only where policy allows.
- DFS writes requiring confirmed replicas may block or degrade according to object policy.

Leader loss:

- Voters elect a new leader.
- Clients retry against discovered voters.
- Watches resume from the last observed revision.

Roaming:

- Existing transport path may fail.
- Peer registry tries alternate LAN, iroh, WebRTC, or relay candidates.
- Node identity and session membership remain stable.

Node restart:

- Node reloads local bootstrap state.
- Reconnects to voters.
- Catches up WAL or snapshot.
- Reinstates leases and service presence after local health checks.

Disaster recovery:

- Restore from the latest KV snapshot plus WAL where available.
- Require explicit operator or future policy-authorized recovery to create a new `cluster_epoch`.
- Never silently form a new cluster with the same identity after quorum loss.

## Implementation Plan

Phase 0, local bootstrap:

- Add `sinomo cluster` CLI. Implemented in `crates/sinomo-cli`.
- Persist local cluster state. Implemented as `$SINOMO_DATA_DIR/cluster/state.json`.
- Add orphan guard hooks around distributed operations. Implemented for every non-help, non-`cluster` top-level command.

Phase 1, contracts:

- Add cluster identity, membership, endpoint, invite, and certificate structs. Implemented in `crates/sinomo-core/src/cluster.rs`.
- Extend `sinomo-network` transport kinds for QUIC, iroh, and WebRTC candidate metadata. Implemented through `TransportKind`, `NodeEndpoint::from_cluster_endpoint`, and `PeerEndpointCandidate`.
- Add cluster-aware peer registry records. Implemented through `PeerRegistry` cluster member and endpoint candidate indexes.

Phase 2, KV core:

- Implement in-process Raft/KV simulation for tests. Implemented in `crates/sinomo-kv`.
- Add WAL, snapshots, revisions, compare-and-swap transactions, and watches. Implemented as `KvLogEntry`, `KvSnapshot`, `KvRevision`, `KvTxn`, and prefix watches.
- Store membership and service registry records in the KV. Implemented for cluster membership records under `/cluster/members/<node_id>`; service registry records use the same generic keyspace and lease primitives.

Phase 3, real local network:

- Add QUIC transport for LAN direct connections. Implemented the LAN QUIC endpoint config and bounded frame codec in `sinomo-network`; the async socket runtime is deferred until the daemon transport loop exists.
- Add mDNS discovery. Implemented an mDNS-style discovery cache that records LAN QUIC endpoint advertisements, TTL expiry, and endpoint candidate conversion.
- Add mutual auth using node identity and cluster certificates. Implemented cluster-auth material, certificate/member verification, and authenticated session establishment against cluster membership records.

Phase 4, roaming:

- Add iroh as the preferred roaming backend after LAN direct paths. Implemented as the optional `iroh-transport` Cargo feature, `ClusterTransportKind::Iroh`, `IrohEndpointRecord`, and KV endpoint publication under `/iroh/endpoints/<cluster>/<node>`.
- Add WebRTC signaling abstraction. Implemented in `sinomo-network` as `WebRtcSignalingEnvelope`, `WebRtcSignalMessage`, and `IceCandidateRecord`.
- Add ICE candidate exchange through the KV or rendezvous service. Implemented `WebRtcKvExchange` helpers for `/webrtc/signals/...` and `/webrtc/candidates/...` keys using the Phase 2 KV operation surface.
- Add TURN/relay fallback. Implemented `RoamingConnectionPlanner`, which prefers LAN direct candidates, then iroh candidates, then WebRTC candidates, then TURN/HTTPS relay candidates when direct paths fail or disappear.

Phase 5, DFS integration:

- Move DFS metadata coordination into the KV.
- Implement write intents, replica receipts, object version advancement, and conflict records.
- Keep content transfer in DFS data-plane protocols.

Phase 6, operations:

- Add `sinomo cluster invite`, `promote`, `demote`, `remove`, `doctor`, and `kv` inspection commands.
- Add snapshot export/import.
- Add metrics and event traces for elections, watches, membership changes, and replication lag.
