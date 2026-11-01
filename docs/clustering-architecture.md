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
| `sinomo-network` | Direct links, connectivity evidence, endpoint selection, route planning, routed streams and relays, object RPC, and transport modules. It does not read KV or apply cluster policy. |
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

The main daemon keeps its decoded Raft node and indexed KV state resident while
it owns the node-local writer lock. Snapshot/WAL restore is therefore a startup
or recovery cost, not a point-read or per-command cost. A read-only process may
seed an identity-validated immutable view from one cold restore, but it never
retains the writer lock. Commits return their bounded event/final-key delta;
full Raft snapshots are created only at the log/event compaction threshold or
when a compatibility/export caller explicitly requests one. The shared
main/shadow materialized file is a derived observational checkpoint and may lag
ordinary commits in every keyspace; it never grants authority. Process-local
observers consume committed deltas, while correctness-sensitive readers use the
resident applied Raft index or a bounded daemon ReadIndex projection. Checkpoint
publication happens only after Raft, commit-sequence, and resident-state locks
have been released, so filesystem I/O cannot hold the authority path.
Ordinary follower appends publish their complete committed delta before an
optional checkpoint snapshot, matching local commits. Subscribers can therefore
advance through the committed revisions before observing the checkpoint. An
installed recovery snapshot still arrives first as replacement evidence; it
cannot claim continuity for history that the subscriber did not observe.

Startup and resident-initialization retries preserve the valid durable Raft
suffix, including entries above the locally recorded commit index. A predecessor
may have replicated that suffix before learning its quorum outcome. Erasing it
locally can reuse the same term/index for a different command. Only legitimate
Raft conflict reconciliation may replace pending history; normal reads,
proposal cleanup, and restart never truncate it. A durable commit position whose
required log entries are missing fails closed instead of moving commit backwards.

KV leases use logical ticks for expiry. The legacy `granted_at` wire
field is retained for decoding existing snapshots; new leases use `UNIX_EPOCH`
as an explicit absent-wall-clock value. Applying a grant or renewal never reads
the local clock. Historical timestamp differences require state reconciliation;
upgrading code alone does not rewrite existing snapshots. All participating
replicas must be upgraded before deterministic lease-state equality is assumed.

Concurrent ordinary proposals use an operation-owned persistence queue per local
Raft node. An idle-to-active transition collects work for at most 1 ms; a batch
contains at most 64 requests or 1 MiB of accounted payload, with a larger valid
request handled alone. The queue admits at most 256 waiting requests and roughly
8 MiB of payload. Every request retains its own identity, transaction semantics,
log entry, deadline, and result. Submission records share a ledger flush and log
entries share a WAL flush. The resident lease hides staged entries until that
flush succeeds; an ambiguous WAL failure invalidates resident reuse and requires
disk replay. Membership changes and election barriers retain individual
admission, and queued data proposals revalidate leadership and the committed
voter set before appending. Local durability still precedes network replication.
Distinct current-term voter acknowledgements may confirm several queued entries.
Finalization uses the quorum's common confirmed index, bounded by locally synced
WAL state, at most 64 entries from the requesting entry, 1 MiB of accounted
payload, and retained event capacity. It stops before configuration changes,
barriers, and operations whose event expansion is not statically bounded. One
commit-marker update applies that prefix; each caller still receives its own
transaction result and request authority. Observers publish the full applied
revision and delta, and the internal compact result carries a separate prefix
proof when needed. A cold-restored WAL enables this lookahead only after a fresh
successful WAL sync, so an ambiguous earlier write cannot become an extra local
quorum acknowledgement.

The cluster layer owns bounded send queues for individual followers. Concurrent
ordinary appends may share an active or queued send only when the transmitted
history covers their exact entries under the same membership, term, leader, and
commit fences. Advancing observational KV cursors and publication timestamps do
not split an otherwise identical membership fence. Each peer has one active send
and at most 256 waiting callers, with at most 8 MiB of pending payload.
Completed acknowledgements are never cached for new requests. ReadIndex probes,
snapshots, and oversized appends use dedicated paths. A caller finishing at
quorum does not cancel shared work; its send owner retains the remaining
deadline and publishes any observed higher term even if all callers have
stopped waiting. Empty proposal and peer queues exit without idle polling.
Proposal batching and shared follower sends do not change the WAL or RPC
formats.

Public reads use one of two explicit consistency contracts:

- A **local** read returns the best materialized or applied view available on
  the node. It remains available without quorum and reports its revision and
  term, but its freshness may be unknown.
- An **authoritative** read first establishes a current-term quorum barrier and
  returns a view at or after that barrier together with its revision, term, and
  committed log index. It fails rather than degrading to a local read when the
  barrier cannot be established.

Cold bootstrap and explicit configuration-gap recovery may request a complete
authoritative prefix through
`ClusterAccess::kv_range_prefix_complete_authoritative_async`. This operation
captures one native current-term quorum-backed snapshot and projects the
requested prefix from that exact owned generation. It does not combine pages,
reopen the observational checkpoint, or register idle work. The complete
projection is limited to 65,536 entries and the 64 MiB native snapshot payload
budget, and individual values to 4 MiB; an exceeded bound rejects the whole
read. It uses the distinct authenticated
`/cluster/kv/authoritative/complete-range-prefix` route, which fails on
receivers that predate it. Ordinary prefix reads keep their 4,096-entry/8 MiB
limits. Complete reads are recovery operations rather than the ordinary
point/prefix request path; forwarded recovery may transfer one native snapshot
before projecting the prefix.

For a leader with an entry committed in its current term, point and bounded
prefix reads use a ReadIndex-style empty AppendEntries quorum confirmation and
then project from the resident index. This does not append a log entry. The
first read after election commits one compact barrier, then performs its final
indexed read under the same term, leader, and exact configuration fence. A
non-leader forwards the bounded range operation itself; it does not fetch a
whole authoritative snapshot to answer a point or prefix request. APIs whose
declared result is a complete snapshot still materialize that snapshot.
Concurrent reads may share one confirmation only when their root, leader,
term, commit position, exact configuration entry, and voter set are identical.
Each caller retains its own deadline and independently revalidates that fence
against the resident applied state before returning data.

DFS clients retain only the exact keys and prefixes they have validated through
the daemon. Metadata-backend construction and repeated operations reuse those
scope certificates; they do not reopen or replay the daemon-owned Raft node, and
they never treat a stale or missing materialized checkpoint as an authoritative
absence.

Leader preference is a routing optimization, not a third consistency level. A
reachable leader without current quorum evidence does not prove freshness.
An entirely unobserved leader remains preferred until liveness evidence
arrives. Once at least one main-daemon endpoint has active unreachable evidence
and no observed main route is healthy or degraded, unobserved alternate
endpoints do not preserve stale leader preference; an eligible candidate may
campaign, but still becomes leader only with the committed voter quorum.
A routed call has no single direct endpoint to blame when every candidate or
relay path is exhausted. A target-bound RPC which consumes its complete
route/response budget therefore records one bounded, process-local liveness
failure for that exact cluster, caller, and target. That transient evidence is
the reactive election-timeout edge for a stale leader: it permits the same
authority operation to campaign, but cannot grant leadership without the
committed voter quorum. Any authenticated response clears the failure
immediately; otherwise it expires after a short bound. This evidence is never
persisted and creates no polling or idle filesystem writes.
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
proof with the outcome. The authority reads only its resident committed-request
and expiry indexes; followers forward that bounded lookup rather than fetching
the complete KV snapshot. Node-local rejected, unknown, or expired attempt
state is considered only after the authoritative lookup rules out a commit.
Without authority, the query fails closed.

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
must be bound to an authenticated identity even on loopback. Direct transports
authenticate the adjacent peer once per link, and routed RPC takes the caller
from that established transport context instead of signing each call; see
[network architecture](network-architecture.md#peer-identity-and-authorization).
Bootstrap handshakes that precede an authenticated link carry signed identity,
freshness, and replay protection. The receiver maps the authenticated identity
to local identity or current cluster membership before executing the request.

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

Cluster KV is fully reactive. It has no generic maintenance task, periodic
sweep, or process-wide wake that speculatively checks every convergence
responsibility. Work is owned by the concrete operation or state transition
that made it necessary:

- A successful commit applies and fans out its exact entries, delivers their
  watch events, and persists peer-progress hints from that fanout result.
- A mutation or authoritative read owns the leadership and quorum work its
  contract requires. A membership or Raft-configuration operation reconciles
  its own durable lifecycle transition.
- An observed append gap, explicit startup recovery, a restored route to a
  known-behind peer, or that peer's own authenticated contact initiates targeted
  log or snapshot recovery for that peer. A peer whose committed append failed
  or was rejected is known-behind until an append is accepted.
- Log, WAL, and snapshot mutations trigger compaction when the owning store's
  threshold is crossed.
- A changed local member, endpoint, or connectivity source publishes its typed
  value. An unchanged source does not send a publication RPC or create a KV
  mutation.
- A peer Raft RPC owns its validation, response, and any exact apply or catch-up
  work exposed by that request.

Local status, health, app, and CLI reads are observational. They do not schedule
KV writes, authority barriers, snapshot pulls, configuration reconciliation, or
other convergence work. An authoritative read performs only the barrier that
the caller explicitly requested. Retries and coalescing remain attached to the
originating operation or typed, keyed source-change job; they never become an
untyped maintenance notification.

This ownership keeps the passive cluster model without an always-on Raft loop
and makes idle behavior an architecture invariant: when inputs and membership
are stable and no operation is active, the KV control plane is quiet. Tracing
and metrics describe owned actions but never trigger them.

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
