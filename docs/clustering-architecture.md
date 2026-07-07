# Sinomo Clustering Architecture

## Purpose

Sinomo clustering turns a single local runtime into a small to medium trusted node set. The cluster is the control plane for distributed sessions, service placement, replicated configuration, and distributed filesystem routing.

The first implementation should optimize for correctness, debuggability, and local-first operation rather than internet-scale membership. A normal cluster is expected to fit comfortably in memory on every node. Every node may store basic metadata for every other node, including identity, capabilities, advertised endpoints, health, and replication role.

## Non-Goals

- Public federation between unrelated users.
- Large fleet orchestration.
- Anonymous peer-to-peer membership.
- Replacing Git, object stores, or local filesystems.
- Using the cluster KV as a high-throughput large-content replication path.

The cluster KV is for compact, strongly ordered control-plane state. Large artifacts and file bytes belong in DFS content stores, local filesystems, Git repositories, or future object stores.

## Crate Ownership

Three R0 crates split the clustering stack, and they are deliberately decoupled (`crates/sinomo-core/tests/workspace_ring_boundaries.rs` enforces the cut).

`sinomo-kv` owns KV storage semantics and the Raft consensus core: models, WAL, snapshots, revisions, transactions, leases, prefix watches, and the election/replication state machines. It is transport-free and depends on neither `sinomo-network` nor `sinomo-cluster`.

`sinomo-cluster` owns the cluster control plane: membership, invites, join registration, member mutation, stable member/state/network-info/status publishing, the KV/Raft replication and election drivers that move consensus traffic over network-selected routes, cluster health rows and doctor findings, cluster bootstrap-state I/O, and the daemon-mediated `/cluster/control/*` object-RPC routes.

`sinomo-network` owns routing and endpoint selection only. It has no dependency on `sinomo-kv` or `sinomo-cluster`; stable cluster facts such as members, advertised endpoints, node statuses, configured network information, and the authoritative connectivity graph arrive through the `ClusterDirectory` provider that `sinomo-cluster` implements on top of cluster KV. Each daemon records local probe results and publishes the current source-owned connectivity set through the cluster control plane when quorum is available. `sinomo-network` uses the local connectivity file only as a bootstrap cache while the KV-backed directory graph is unavailable or empty, plus the narrow cold-join case where the current node's own source-owned probe records may seed routing until that source appears in KV. Local records from other sources never extend or override a non-empty KV graph. `sinomo-service` owns service registration, discovery records, and routing target selection above this stack.

## CLI Surface

The cluster lifecycle starts with `sinomo cluster`.

Commands:

```text
sinomo cluster
sinomo cluster init [--name <name>] [--cluster-id cluster_...] [--node-id node_...]
sinomo cluster invite [--introducer <host:port>] [--ttl-seconds <seconds>] [--output <path>]
sinomo cluster join <invite> [--name <name>] [--cluster-id cluster_...] [--node-id node_...]
sinomo cluster migrate plan <invite> [--mode single-node|cluster-merge]
sinomo cluster status
sinomo cluster health
sinomo cluster peers
sinomo cluster leave
```

A node's normal lifecycle starts as a single-node cluster. A data directory with no cluster state is a pre-bootstrap or recovery condition, not an operating mode. A node with no cluster state must not run distributed operations, write distributed config, mount distributed DFS namespaces, accept remote action requests, or publish cluster-routed events. The only allowed operations before cluster state exists are:

- `sinomo cluster status`
- `sinomo cluster init`
- `sinomo cluster join`
- local daemon lifecycle and install commands, so a machine can be prepared before joining
- local help and credential setup commands that do not mutate cluster state

Normal cluster lifecycle operations are daemon-mediated: `cluster init`, `join`, `invite`, `rename`, `role`, `remove`, and `leave` reach the local daemon through the `/cluster/control/{init,join,invite,rename,role,remove,leave}` object-RPC routes registered by `sinomo-cluster`, so they require a running daemon. Documented debug/repair commands stay direct against local state: `cluster doctor`, `cluster connectivity`, `cluster repair-members`, `cluster cleanup-legacy-shadow-members`, `cluster state-authority-role`, and `cluster migrate` planning.

`sinomo cluster init` creates a single-node cluster. `sinomo cluster join` records an intent to join an existing cluster and moves a pre-bootstrap node into pending learner state until the cluster control plane accepts it. When a node already has initialized cluster state, joining another cluster is treated as migration rather than delete-and-join. `sinomo cluster leave` leaves the current cluster by quarantining old cluster state/KV/Raft/watch storage under `$SINOMO_DATA_DIR/cluster/migrations/<leave_id>/leave-source/`, then creates a fresh single-node cluster with the preserved local node identity. The left cluster's KV remains archived evidence and is not imported into the new single-node cluster.

`sinomo cluster invite` creates a portable `sinomo-join://v1/...` invite. The URI is intentionally small because invites are short-lived. It carries only the cluster's current public key, the invitation ID, the invitation secret, and communication channels the invited node can use to reach the cluster. Full invite metadata, expiry, allowed roles, issuer identity, and policy stay in the cluster KV under the invitation ID and are verified by the contacted introducer during join.

When the main daemon is running on a wildcard bind address such as `0.0.0.0`, invite generation tries to add the host's LAN address before local loopback endpoints. If the command warns that only local-only introducers were found, or if the machine has multiple interfaces, the initialized machine should include an address reachable from the remote host:

```text
sinomo cluster invite --introducer 192.168.1.10:21353 --output join.txt
```

The command prints the raw invite URI and a ready-to-run join command. When `--output` is supplied, the output file contains the ready-to-run command:

```text
sinomo cluster join <sinomo-join://v1/...>
```

The remote machine can run the command from the file directly, append `--name <remote-name>` if it wants a custom display name, or pass the file to `cluster join`; the join parser accepts both raw invite files and command files:

```text
sinomo cluster join join.txt --name <remote-name>
```

On join, a fresh remote node builds local pending cluster state and then contacts the invite introducers over the daemon control channel. A reachable introducer records the joining installation's main cluster member, with main and shadow daemon endpoints folded into that member's endpoint set, returns the updated cluster member view, and lets `sinomo cluster peers` show the new node on both sides. If the invite contains introducers but none are reachable, join fails with an actionable connectivity error instead of silently recording an isolated pending state. `sinomo cluster health` combines the KV member view, daemon-published node heartbeats, and live endpoint probes to report online/offline state, daemon version, platform, last-seen time, rollout state, and shadow reachability through the main member endpoints.

Nodes behind routers, cloud NAT, or port forwards may have externally reachable addresses that are not visible from the node's own network interfaces. A node can publish those direct TCP daemon endpoints from node-local config at:

```text
$SINOMO_DATA_DIR/configs/cluster/endpoints.json
```

The daemon reads this file during membership refresh and merges the configured endpoints into the main and shadow endpoint sets. A missing file keeps the normal automatic LAN/public-IP discovery behavior. A malformed file is treated as a configuration error so an operator can fix stale or invalid routing data instead of silently advertising the wrong endpoint.

```json
{
  "main": [
    {
      "address": "tcp://198.51.100.76:21353",
      "scope": "internet-direct"
    }
  ],
  "shadow": [
    {
      "address": "tcp://198.51.100.76:21354",
      "scope": "internet-direct"
    }
  ]
}
```

`address` may be a URL. `tcp://host[:port]` publishes a direct TCP daemon endpoint; if no scheme is supplied, the address is treated as TCP. A bare host/IP uses the daemon's configured port. Other network modules can define their own URL schemes, such as `iroh://...`, and the endpoint selector will use the corresponding transport when that module is available. `scope` is optional and defaults from the address: public TCP socket addresses become `internet-direct`; private TCP addresses become `lan`; Iroh relay URLs become `relayed`. `privacy` is accepted as a deprecated alias for `scope` in this config file. `priority` is optional and uses the endpoint record priority directly; lower priority is preferred.

`sinomo cluster migrate plan <invite>` validates the current initialized cluster as a migration source and validates the target invite without mutating local state. The implemented planner supports:

- `single-node`, the default mode for a device that initialized a local one-node cluster and later needs to join another cluster;
- `cluster-merge`, the planning mode for a source cluster that already has remote peers.

The durable contract is `ClusterMigrationRecord` under `/cluster/migrations/<migration_id>`. The planner reports the source cluster, target invite identity, local node mapping, member counts, and the required execution steps. `sinomo cluster join <invite>` on an already initialized settled solo node runs the first migration path inside the daemon (`/cluster/control/join` detects the settled source and serializes against the daemon's own maintenance commits): source cluster state, KV, Raft, watches, and DFS metadata are quarantined under the local migration/import roots, the target join is recorded with the preserved local node identity, and the migration record is finalized or rolled back locally. Target cluster KV is authoritative after the join. Source cluster KV is archived as evidence/import source only: cluster-level `/config/...` keys are skipped, and only explicitly node-local `/config/nodes/<source_node_id>/...` keys are eligible to be reapplied to target KV as normal KV writes. If the target join is still pending, those node-local keys are recorded as deferred in `kv-import.json` next to the quarantined source storage. Shared DFS data import remains future work and is not implemented by this first single-node migration path.

Initial local state lives at:

```text
$SINOMO_DATA_DIR/cluster/state.sobj
```

The state file uses the shared `ClusterBootstrapState` contract from `sinomo-base` (re-exported as `sinomo_core::cluster::ClusterBootstrapState`) for the local cluster identity, local node identity, current Raft term/revision, and current member records; `sinomo-cluster` owns the read/write helpers. It records the current cluster status through member records, keeping the documented node role separate from Raft membership state. Durable membership and config state come from the replicated KV log; the file remains the local anchor that tells the daemon which cluster identity and local node identity to use at startup.

Each local installation has one normal cluster member: the main daemon node, which is eligible to be the local voter when this installation owns a voting role. The shadow daemon is a supervision and routing process, not a `/cluster/members/<node_id>` participant. It advertises reachable endpoints through the main member so other nodes can route to the installation when main is down, but it is not counted in consensus quorum and should not own DFS objects, run normal scheduled workloads, or publish routine cluster services.

## Identity Model

Cluster identity:

- `cluster_id`: stable cluster identifier, prefixed with `cluster_`.
- `cluster_name`: operator-friendly display name.
- `cluster_epoch`: monotonic epoch incremented only by explicit destructive re-bootstrap or disaster recovery.
- `created_at`: cluster creation timestamp.

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

Daemon membership is installation-scoped. A workstation contributes one cluster member, while its shadow daemon remains a local process with communication/supervision reachability through the main member endpoint set.

## Bootstrap and Join

### Init

`sinomo cluster init`:

1. Generates or accepts a `cluster_id`.
2. Generates or accepts a local `node_id`.
3. Creates a cluster root key or uses a future operator-provided key.
4. Creates the first member as an active voter.
5. Creates an initial KV log entry containing cluster metadata, member metadata, and schema versions.
6. Writes local cluster state under `$SINOMO_DATA_DIR/cluster/state.sobj`.

The first node is both control-plane voter and data-plane participant. Additional nodes join as learners first.

### Join

`sinomo cluster join <invite>`:

1. Parses the invite payload.
2. Generates or accepts a local `node_id`.
3. Stores pending cluster state locally.
4. Contacts one or more introducers from the invite.
5. Presents a join request containing node identity, public key, capabilities, and requested role.
6. Waits for an existing voter to commit a pending member record.
7. Starts as a learner.
8. Promotes to voter only after snapshot catch-up, health checks, and policy approval.

Invite payload fields:

- `public_key`: the cluster's current public key for invite challenge verification.
- `invite_id`: the invitation ID the invited node uses when contacting an introducer.
- `secret`: the short-lived secret presented to prove possession of the invite.
- repeated `channel`: communication channels containing transport, address, and responder public key. The invite URI does not need to disclose peer node IDs. Default invites keep the fixed fallback order: LAN direct channels first when available, public direct channels next, and Iroh last as the cross-network fallback. Runtime route choice uses the active `sinomo-network` connectivity graph to avoid endpoints this node already knows are unavailable; it does not reorder invite channels to guess the remote topology.

The persisted invitation record is larger than the URI. It contains the cluster identity, expiry, allowed roles, issuer identity, signature, and any other policy needed by introducers to validate and register the join.

Join tokens are one-time or short-lived. A leaked token should not grant ongoing authority after the token expires or after a successful use.

## Cluster-State Guard

The cluster-state guard is a hard runtime boundary.

Allowed before local cluster state exists:

- create or inspect local cluster state
- print help

Blocked before local cluster state exists:

- starting the cluster-aware daemon
- distributed DFS mounts
- replicated config reads or writes
- event routing with `TrustedNetwork` or `DurableGlobal` scope
- remote action requests
- self-evolution rollout that targets more than local dry-run artifacts
- accepting inbound peer messages

Implementation rule: code paths that depend on cluster authority call a small `require_cluster_state` guard before doing work. The guard first checks local cluster state, then asks the cluster service for an active member view once the daemon exists.

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
4. LAN TCP for development nodes.
5. Iroh QUIC endpoints for roaming across NATs, using endpoint public keys, direct addresses, and relay URL.
6. WebRTC data channels for browsers or environments where iroh is unavailable.
7. TURN relay or an HTTPS relay for difficult networks.

Discovery mechanisms:

- Static endpoints from local cluster state and KV membership records.
- mDNS/DNS-SD for LAN candidates.
- UDP hole-punch candidates where available.
- Iroh endpoint records published through the KV under `/iroh/endpoints/<cluster>/<node>`.
- WebRTC ICE candidates for cross-network paths.
- Optional user-hosted rendezvous/signaling service.

Discovery finds paths, not authority. Transport sessions are authorized only after mutual authentication with cluster-issued credentials.

Path selection is continuous. A node may move from LAN QUIC to iroh, WebRTC relay, and back without changing node identity or session membership. The peer registry stores multiple endpoint candidates per node and scores them by latency, reliability, privacy class, and cost.

Iroh is the preferred general roaming backend once LAN direct paths are unavailable. The cluster stores the peer endpoint public key, any known direct socket addresses, the selected relay URL or default relay mode, and a TTL. The `modules/network/iroh` network module (a native dylib loaded by the daemon) turns that record into an iroh dial configuration using the cluster ALPN `sinomo.iroh-roaming.v1`; without the module installed and enabled the node neither listens nor dials iroh. WebRTC remains available for browser surfaces and deployments that do not install the module.

## Security Model

Baseline requirements:

- Every node has a stable identity key.
- Every transport session uses mutual authentication.
- Cluster-issued certificates are short-lived and bound to `cluster_id`, `node_id`, and capability scope.
- Membership records contain public keys or key references.
- The local daemon control port is authenticated even on loopback. Each daemon command is wrapped in a signed `sinomo.daemon-auth.v1` envelope containing the node id, public key, timestamp, nonce, and command bytes. The daemon verifies the Ed25519 signature and authorizes the signer against the local node identity or the cluster membership key set before executing sensitive commands.
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
- service instance records, published by the daemon from the `sinomo-service` registry
- active session summaries
- DFS filesystem config, shard placement, service routing, and compact policies
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
/cluster/config/identity
/cluster/raft/config
/cluster/members/<node_id>
/cluster/node-status/<node_id>
/cluster/endpoints/<node_id>/<endpoint_id>
/cluster/policy/capabilities/<node_id>
/cluster/certificates/<certificate_id>
/cluster/invitations/<invite_id>
/cluster/invitations/<invite_id>/state
/cluster/rollout/desired
/cluster/rollout/nodes/<node_id>
/cluster/services/<service_name>/<node_id>
/sessions/<session_id>/summary
/sessions/<session_id>/participants/<node_id>
/sinomo/imports/<source_cluster_id>/...
/dfs/filesystems/<file_system_id>/config
/dfs/filesystems/<file_system_id>/metadata-shards/<shard_id>
/dfs/services/metadata/<node_id>
/dfs/services/content/<node_id>
/dfs/policies/<policy_id>
/dfs/locks/<file_system_id>/<object_id>
/policy/<domain>/<key>
```

Large or frequently changing values should store references to event log entries or DFS objects rather than embedding payloads directly.

### Leases

Leases are used for ephemeral liveness and coordination:

- service presence
- leader hints for non-consensus services
- session participant activity
- DFS write locks where a policy requires serialization
- temporary rollout reservations

Lease loss must be treated as a hint, not proof that durable state should be deleted. Durable membership and DFS shard assignments require explicit committed mutations.

## DFS Foundation

The DFS uses the cluster KV for compact routing and placement configuration. High-cardinality namespace, object, version, receipt, intent, conflict, and directory records live in DFS metadata shards, not in the cluster KV.

KV-owned DFS control records:

- filesystem config
- metadata-shard primary and replica assignment
- metadata service advertisements
- content service advertisements
- compact policy records
- optional coarse write locks where a policy requires serialization

DFS metadata shards own the durable filesystem truth:

- object metadata and path indexes
- owner node and visible version
- content hashes and manifests
- replica requirements and receipts
- conflict records
- write intent records
- namespace transactions and bounded namespace jobs

DFS content also remains outside the KV. Content is stored locally on owner/replica nodes and addressed by object ID, version, and content hash.

Write behavior:

- `local-owned`: shard metadata routes writes to the owner node.
- `replicated-confirmed`: a DFS write intent is committed, replicas store content, then visible version advances after required receipts.
- `replicated-eventual`: owner advances visible version and schedules replication.
- `append-only`: each append creates an immutable object version or chunk.
- `ephemeral-local`: no cluster KV metadata unless promoted.

The KV gives every node a consistent answer to "which metadata shard is responsible and where is its primary?" The DFS metadata plane answers "who owns this object, what version is current, and where can I fetch it?" The DFS data plane handles bytes separately.

## Event Routing Integration

Cluster membership feeds `sinomo-network` routing through the `ClusterDirectory` provider that `sinomo-cluster` implements over cluster KV.

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

- Node reloads local cluster state.
- Reconnects to voters.
- Catches up WAL or snapshot.
- Reinstates leases and service presence after local health checks.

Disaster recovery:

- Restore from the latest KV snapshot plus WAL where available.
- Require explicit operator or future policy-authorized recovery to create a new `cluster_epoch`.
- Never silently form a new cluster with the same identity after quorum loss.

## Implementation Plan

Phase 0, local cluster state:

- Add `sinomo cluster` CLI. Implemented in `crates/sinomo-cli`.
- Persist local cluster state. Implemented as `$SINOMO_DATA_DIR/cluster/state.sobj`.
- Add cluster-state guard hooks around distributed operations. Implemented for every non-help, non-`cluster` top-level command.
- Require short-lived join material. Implemented by storing signed invitation records in cluster KV while the portable `sinomo-join://v1` URI carries only the public key, invite ID, secret, and communication channels needed to reach an introducer.

Phase 1, contracts:

- Add cluster identity, membership, endpoint, invite, and certificate structs. Implemented in `crates/sinomo-base/src/cluster.rs`, re-exported through the `sinomo-core` facade.
- Add cluster migration contract records and CLI planning. Implemented as `ClusterMigrationRecord`, `ClusterMigrationMode`, `ClusterMigrationPhase`, `/cluster/migrations/<migration_id>` keys, and `sinomo cluster migrate plan`.
- Extend `sinomo-network` transport kinds for QUIC, iroh, and WebRTC candidate metadata. Implemented through `TransportKind`, `NodeEndpoint::from_cluster_endpoint`, and `PeerEndpointCandidate`.
- Add cluster-aware peer registry records. Implemented through `PeerRegistry` cluster member and endpoint candidate indexes.
- Represent the local installation as one main cluster member while preserving shadow reachability. Implemented in `crates/sinomo-cluster`: main and shadow daemon endpoints are folded into the main member, and stale local shadow members are removed during daemon membership refresh.
- Use real cluster identity material. Implemented by generating the local Ed25519 identity before cluster state creation, storing explicit member public keys in every member record, signing invites with the issuer node identity, and verifying invite signatures before local join or introducer acceptance.

Phase 2, KV core:

- Implement a real Raft/KV core for tests, local development, and daemon integration. Implemented in `crates/sinomo-kv` with voter quorum commit, append-entries validation, vote requests, leader transfer, follower catch-up, committed-index tracking, hard-state records, and deterministic multi-node tests.
- Add WAL, snapshots, revisions, compare-and-swap transactions, and watches. Implemented as `DurableRaftKvNode`, `KvWalPaths`, `RaftHardState`, `KvLogEntry`, `RaftSnapshot`, `KvRevision`, `KvTxn`, and prefix watches; durable nodes replay JSONL WAL entries, persist hard state, save and restore snapshots, preserve watchable event history across snapshot restore, and compact WAL after snapshots.
- Store membership and service registry records in the KV. Implemented for cluster identity under `/cluster/config/identity`, Raft role config under `/cluster/raft/config`, membership records under `/cluster/members/<node_id>`, split endpoint records under `/cluster/endpoints/<node_id>/<endpoint_id>`, capability policy under `/cluster/policy/capabilities/<node_id>`, invitations and invite state under `/cluster/invitations/...`, rollout desired/node state under `/cluster/rollout/...`, and runtime health records under `/cluster/node-status/<node_id>`; service instance records are published by the daemon through `sinomo-service` under `/cluster/services/<service_name>/<node_id>`.
- Generate node identity key pairs and publish public keys in KV membership records. Implemented with node-local Ed25519 identity files under `$SINOMO_DATA_DIR/identity/`; the main daemon identity is the cluster member identity, and the shadow identity remains local to supervision/routing traffic.
- Expose the KV through the local daemon control plane for internal Sinomo use. Implemented as daemon-owned object RPC routes backed by a per-node durable Raft WAL under `$SINOMO_DATA_DIR/cluster/kv-raft/<node_id>/`; `$SINOMO_DATA_DIR/cluster/kv-snapshot.sobj` stores the current local snapshot. Cluster init, daemon registration, and join registration persist member records into the KV, daemon maintenance publishes version/health/rollout node-status records, and join responses carry the current snapshot to the joining node.
- Integrate the daemon's cluster KV transport with the durable Raft core. Implemented with authenticated `CLUSTER_RAFT_APPEND`, `CLUSTER_RAFT_VOTE`, and `CLUSTER_KV_WATCH_EVENTS` daemon messages. The periodic cluster path no longer exchanges and merges whole KV snapshots; the current durable Raft leader sends append-entries with optional Raft snapshot-install payloads to followers, and vote requests are handled against durable local hard state. Daemon maintenance checks leader reachability, starts a new election when the known leader is missing, persists the elected leader after quorum, steps down when it cannot observe quorum during replication, and fans out watch event history over the authenticated transport so local daemon-backed prefix watches can resume and drain committed changes. The replication, election, quorum, and committed-watch fanout drivers are owned by `crates/sinomo-cluster`; `sinomo-kv` stays transport-free.

Phase 3, real local network:

- Add direct LAN transport. Implemented daemon-to-daemon signed TCP control messages for local network synchronization, Raft KV replication, and rollout control. Daemons bound to `0.0.0.0` or another non-loopback host advertise LAN TCP endpoints alongside loopback endpoints, and the current main-daemon Raft leader continuously replicates committed KV log state to reachable peer daemon endpoints.
- Add LAN discovery. Implemented a real UDP multicast discovery socket in `sinomo-network` using the `sinomo.lan-discovery.v1` advertisement envelope. The daemon publishes its current LAN endpoints, drains peer advertisements, records them in the discovery cache, and promotes discovered endpoints into known member records only when the advertised node is already trusted by cluster membership.
- Add mutual auth using node identity and cluster certificates. Implemented cluster-auth material, certificate/member verification, authenticated session establishment against cluster membership records, and signed daemon control envelopes for LAN peer sync. Discovery finds paths only; signed cluster membership still authorizes use.

Phase 4, roaming:

- Add iroh as the preferred roaming backend after LAN direct paths. Implemented as `ClusterTransportKind::Iroh`, `IrohEndpointRecord`, KV endpoint publication under `/iroh/endpoints/<cluster>/<node>`, and the `modules/network/iroh` native-dylib network module carrying the iroh SDK, listener/proxy, and dial client. `sinomo-network` keeps only the transport-agnostic record contract and dispatches iroh dials to the registered module.
- Add WebRTC signaling abstraction. Implemented in `sinomo-network` as `WebRtcSignalingEnvelope`, `WebRtcSignalMessage`, and `IceCandidateRecord`.
- Add ICE candidate exchange through the KV or rendezvous service. Implemented `WebRtcKvExchange` helpers for `/webrtc/signals/...` and `/webrtc/candidates/...` keys using the Phase 2 KV operation surface.
- Add TURN/relay fallback. Implemented `RoamingConnectionPlanner`, which prefers LAN direct candidates, then iroh candidates, then WebRTC candidates, then TURN/HTTPS relay candidates when direct paths fail or disappear.

Phase 5, DFS integration:

- Integrate DFS with the KV for filesystem config, metadata-shard assignment, and service routing. Implemented as compact DFS control-plane records while high-cardinality metadata remains in DFS metadata backends.
- Implement write intents, replica receipts, object version advancement, and conflict records. Implemented in DFS metadata shards with replayable journals, compact materialized tables, and restart recovery.
- Keep content transfer in DFS data-plane protocols. Implemented through DFS network messages, including chunked large-object transfer with bounded windows and per-chunk checksums.

Phase 6, operations:

- Add `sinomo cluster invite`, `promote`, `demote`, `remove`, `doctor`, and `kv` inspection commands. `sinomo cluster invite` is implemented for local signed invites; `cluster role` updates the documented node role; `cluster raft-role` and `cluster promote` update Raft membership state; and `cluster status` / `cluster health` / `cluster peers` read the daemon-backed KV membership and node-status view when available. Cluster lifecycle commands are daemon-mediated through the `/cluster/control/*` routes; doctor/repair-style commands stay direct. Approval, revocation, demotion, removal, and explicit KV inspection remain future work.
- Add snapshot export/import.
- Add metrics and event traces for elections, watches, membership changes, and replication lag.
