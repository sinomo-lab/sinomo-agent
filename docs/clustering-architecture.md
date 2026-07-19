# Sinomo Clustering Architecture

This document defines the developer architecture for Sinomo cluster identity,
membership, control-plane state, and failure behavior. Operators should use the
[cluster operations guide](user/cluster-operations.md) for commands and
recovery workflows.

## Purpose

A Sinomo cluster is a trusted set of runtime nodes. It coordinates identity,
membership, service discovery, compact replicated configuration, placement,
and distributed filesystem routing. A standalone installation is represented as
a one-node cluster, so local and distributed deployments use the same model.

The design targets small clusters of workstations, mobile devices, and home
servers. It prioritizes understandable authority, intermittent connectivity,
and local-first operation over fleet-scale throughput.

## Non-Goals

- Public federation between unrelated users.
- Anonymous peer-to-peer membership.
- Large fleet orchestration.
- Storing files, media, model weights, or unbounded history in cluster KV.
- Treating reachability or discovery as proof of trust.
- Requiring every mobile or intermittently connected node for quorum.

## Ownership Boundaries

The cluster stack is split across independent R0 crates:

| Crate | Responsibility |
|---|---|
| `sinomo-kv` | KV semantics, Raft state, transactions, revisions, watches, leases, WAL, and snapshots. It is transport-free. |
| `sinomo-network` | Typed communication, endpoint selection, route planning, relays, retries, and transport modules. It does not read KV or apply cluster policy. |
| `sinomo-cluster` | Identity, membership, invitations, lifecycle, health projection, and the drivers that carry KV/Raft traffic over network routes. |
| `sinomo-service` | Service registration, discovery, and target selection above cluster membership. |

`sinomo-cluster` is the only production bridge between networking and KV. It
projects committed cluster facts through the network directory interface and
supplies the transport used by Raft. Workspace tests enforce that
`sinomo-network` and `sinomo-kv` have no production dependency on one another;
dev-only test-support dependencies are exempt.

The daemon hosts these components and supplies process identity,
authentication, persistence paths, and runtime scheduling. It does not absorb
their domain rules.

## Core Invariants

1. Discovery identifies possible routes, never authorized members.
2. Membership changes are committed control-plane operations.
3. A node cannot grant itself authority or promote itself into quorum.
4. Control-plane writes require the configured authority quorum.
5. Cached local state cannot authorize a decision that requires fresh cluster
   authority.
6. Large content and high-cardinality filesystem metadata remain outside KV.
7. A remote request is authorized again by the node that owns the affected
   resource or device.
8. Cluster identity is never recreated silently after quorum loss.

## Identity Model

The cluster has a stable identifier and display name. Trust is currently bound
to membership records and each member's identity key. Every node also carries
descriptive metadata such as its name, device role, capabilities, endpoints,
and health.

Node identity and node role are separate from control-plane authority. A mobile
node may be a trusted cluster member and data-plane participant without voting
on replicated state. Likewise, an authority replica can be temporarily offline
without losing its durable membership.

Each installation contributes one normal member. The main daemon acts for that
member. The shadow daemon retains a local process identity for signed
supervision and repair traffic, but it is represented as an alternate endpoint
of the installation rather than a second member. It never votes, owns DFS data,
or receives ordinary workload placement.

Membership records bind node identity keys to the cluster. Network discovery,
DNS, LAN advertisements, or possession of an old endpoint cannot replace that
binding.

## Lifecycle

### Bootstrap

Cluster initialization creates a one-node cluster, records the local node as the
first authority, and establishes the initial control-plane state. A data
directory without cluster state is a bootstrap or recovery condition, not a
normal operating mode.

Before cluster state exists, the installation may run local setup, credentials,
help, daemon lifecycle, and cluster initialization or join. It must not perform
distributed writes, publish trusted-network events, mount distributed
namespaces, or accept remote actions as an authorized member.

### Invite and Join

An invite is a short-lived capability for contacting an existing member. The
portable invite contains only the material needed to identify the target
cluster, prove possession of the invite, and reach an introducer. Full policy,
expiry, issuer, and allowed-role data remain in committed cluster state.

A joining installation presents its identity and capabilities to an
introducer. The existing cluster validates the invite and commits the new
member. Normal invitations use learner or non-voting roles. Authority promotion
is a separate committed membership change. The current lifecycle records learner
admission, requires proof that the learner reached the required committed log
index before promotion, and persists completion or failure evidence so restart
reconciliation can resume an interrupted change. If no introducer is reachable,
the join fails explicitly instead of leaving an apparently joined but isolated
member.

### Migration and Leave

Joining a different cluster from an initialized installation is a migration,
not a deletion followed by an unrelated join. Migration preserves the local
node identity, quarantines the old cluster's local state as evidence, and makes
the target cluster authoritative. Cluster-wide configuration and shared DFS
data are not silently merged.

Leaving performs the reverse transition: old cluster state is quarantined and
the installation creates a new one-node cluster with the same local node
identity. Archived state is recovery evidence, not active authority.

## Membership and Authority

Membership has three separate dimensions:

- **Trust:** whether the node is an authorized member.
- **State authority:** whether it participates in, replicates, or merely
  observes control-plane consensus.
- **Health:** whether it is currently reachable and suitable for work.

These dimensions must not be collapsed into one status. An offline member stays
trusted until explicitly removed. A reachable observer does not gain a vote. A
descriptive role such as workstation or mobile does not determine authority.

Small clusters should normally use an odd number of reliable authority nodes.
Mobile and intermittently connected nodes usually replicate or observe state
without being required for quorum. Authority changes are themselves
quorum-backed membership operations.

## Control-Plane KV

The cluster KV is a compact, strongly ordered authority for:

- cluster identity and authority configuration;
- membership, public keys, and advertised endpoints;
- node health summaries and capabilities;
- invitations and lifecycle state;
- service registrations;
- compact DFS configuration and shard placement;
- shared policy and rollout desired state;
- compact queued build state and artifact references;
- expiring build-worker capability and resource advertisements;
- short-lived coordination leases.

It does not contain raw media, file bytes, build artifacts, model weights,
unbounded event history, embeddings, or per-file DFS metadata. Those values are
stored in the owning subsystem and referenced from control state when needed.

Committed mutations advance an ordered revision. Transactions provide
compare-and-swap behavior, watches resume from revisions, and snapshots bound
recovery cost. The KV state machine owns these semantics; the cluster layer only
drives replication and exposes authorized service operations.

Public reads use one of two explicit consistency contracts:

- A **local** read returns the best materialized or applied view available on
  the node. It remains available without quorum and reports its revision and
  term, but its freshness may be unknown.
- An **authoritative** read first establishes a current-term quorum barrier and
  returns a view at or after that barrier together with its revision, term, and
  committed log index. It fails rather than degrading to a local read when the
  barrier cannot be established.

Leader preference is a routing optimization, not a third consistency level. A
reachable leader without current quorum evidence does not prove freshness.
Status and telemetry views normally use local reads. Membership changes,
invitations, migration, authority changes, cluster configuration, and rollout
decisions use authoritative reads or fail with structured quorum diagnostics.
An authoritative watch establishes its starting revision through the same
barrier; subsequent events remain ordered and expose the last applied revision.
Because watch registrations are node-local, the current watch-start RPC must be
sent to the Raft authority and executes its barrier and registration on that
same node. A non-authority receiver fails closed. It must not forward only the
barrier and then register locally, because that would attach the watch to a
potentially lagging follower; any future forwarding must forward the complete
watch-start operation as one unit.

Every mutation carries a stable request ID. Success means either that the exact
request is durably recorded as committed or that its command committed through
the current quorum. A locally detected semantic no-op is not success by itself;
it still requires a quorum barrier unless an exact committed-request record can
replay its response. After an uncertain timeout, callers query the request ID
instead of guessing whether a blind retry is safe. The request-outcome route
establishes a fresh current-term quorum barrier first and returns its authority
proof with the outcome. Committed history comes from that authoritative
snapshot; node-local rejected, unknown, or expired attempt state is considered
only after the snapshot rules out a commit. Without authority, the query fails
closed.

The config service exposes this identity at its outer write boundary:
`ConfigSetRequest.request_id` is preserved into the cluster KV transaction, and
typed remote clients can call `set_with_request_id` when they need to reconcile
an uncertain config write through the request-outcome route.

Authority configuration changes are explicit lifecycle operations. A node is
added as a learner, proves catch-up through a required committed revision or log
index, and is then promoted in a separate quorum-backed transition. Voter
demotion or removal likewise records its transition and completion or failure
evidence, while preserving an overlapping quorum.

## Networking and Discovery

A member may advertise several endpoints, including local, LAN, direct
internet, roaming, and relayed paths. Route selection considers purpose,
reachability, transport availability, identity feedback, latency, and cost.
Changing route does not change node identity or authority.

The committed cluster directory is the authoritative source of member and
endpoint facts. Each node records its own observations and may use local
connectivity data during bootstrap, but observations from one node cannot
silently extend or override a non-empty authoritative graph for another node.

LAN and roaming discovery only supply candidates. A transport session becomes
usable for cluster work after mutual identity verification and route-level
authorization. See [network architecture](network-architecture.md) for endpoint
selection, fallback, and retry rules.

## Security Boundary

Every node has a stable signing identity. Sensitive daemon and cluster requests
carry authenticated identity, freshness, and replay protection even on
loopback. The receiver verifies the signer against local identity or current
cluster membership before executing the request.

Baseline rules:

- Invitations are scoped and short-lived.
- Removed members and stale keys are rejected even if their endpoints remain
  reachable.
- Control messages identify the cluster, node, protocol version, and relevant
  term or revision.
- Capabilities distinguish what a node can observe from what policy permits it
  to do.
- Device, file, secret, and action authority remains local to the owning node.

## DFS and Service Integration

The cluster KV answers compact routing questions: which metadata shard is
responsible, where its current authority runs, which services are registered,
and which nodes are eligible for placement. DFS metadata shards answer
high-cardinality namespace and version questions. Content stores move and retain
bytes according to DFS policy.

Service discovery uses durable registration and current health to select a
target. Selection does not bypass network route validation or the target
service's own authorization.

Cluster build workers use the same discovery boundary with stricter placement
evidence. Durable registration identifies a possible worker; a fresh expiring
advertisement states its supported target identities and toolchains, available
slots, resource headroom, queue pressure, and cache locality. The build
scheduler hard-filters for compatibility, policy, liveness, route health, and
capacity, then ranks eligible workers by estimated time to a usable artifact.
A short-lived lease reserves the selected slot before dispatch.

Rollout targets are grouped by compatible artifact target and resolved from
verified caches before compilation is scheduled. Worker or route loss causes
re-ranking and retry, not relaxed compatibility. Service registration,
membership, or a descriptive device role alone is never liveness or build
eligibility evidence. See
[build service architecture](build-service-architecture.md).

The initial build-service delivery queries these capabilities live from each
reachable daemon and keeps queue/cache state node-local. Durable cluster-wide
advertisements and slot leases are the intended coordination layer, not a
claim about the initial wire implementation.

See [DFS architecture](dfs-architecture.md) and
[service responsibilities in the overview](architecture.md#distributed-foundation).

## Reactive Convergence

Cluster activity drives bounded maintenance; Sinomo does not require a
conventional always-on heartbeat and election loop. Daemon startup, app or CLI
traffic, status and health requests, rollout and migration work, peer Raft RPCs,
network changes, and a low-frequency timer while the daemon is already running
all coalesce into the same maintenance action.

One action refreshes observed leadership and term, performs an appropriate
append heartbeat or catch-up attempt, pulls a newer snapshot when necessary,
applies and compacts durable state within configured bounds, drains watch work,
and schedules status or endpoint publication. Each step is bounded and records
completed, deferred, skipped, or failed evidence. These wake signals schedule
existing Raft actions; they never establish authority or create a second
consensus path.

## Failure Behavior

| Failure | Required behavior |
|---|---|
| Network partition | The authority majority may continue. A minority cannot commit control-plane writes. |
| Leader loss | Eligible authority nodes elect a replacement; clients retry through current directory information. |
| Node restart | The node restores durable identity and consensus state, then catches up before claiming freshness. |
| Route loss | Routing tries bounded alternate endpoints without changing membership or trust. |
| Intermittent observer | Local work may continue within cached-state policy; authoritative writes wait or fail explicitly. |
| Build worker loss | Its lease expires and the queued job is retried on another compatible worker; partial output is never eligible for rollout. |
| No compatible builder | The job remains visibly queued or fails with an explicit capability blocker; rollout does not substitute an incompatible artifact. |
| Lost quorum | The cluster becomes read-only for authority-dependent work; supervision and repair endpoints remain available. |
| Disaster recovery | Recovery requires an explicit operator or policy-authorized transition; the runtime must not silently create a replacement cluster identity. |

Leases and health timeouts are evidence of liveness, not permission to delete
durable membership or data. Recovery must distinguish a temporarily unreachable
node from a removed node.

## Verification and Plans

The [formal verification guide](formal/cluster-verification.md) maps executable
models to these safety boundaries. Future cluster work is tracked in
[plans](plans/README.md), including the remaining election-recovery,
write-coalescing, resumable-snapshot, and diagnostic follow-ups retained in the
[reactive KV/Raft plan](plans/reactive-cluster-kv-raft-improvement-plan.md).
Completed phase-by-phase implementation history is not part of this architecture
document.
