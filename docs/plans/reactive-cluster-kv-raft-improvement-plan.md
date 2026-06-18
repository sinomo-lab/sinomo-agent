# Reactive Cluster KV/Raft Improvement Plan

## Context

Sinomo's intended cluster shape is reactive and passive. A cluster may be made of
home PCs, laptops, and mobile devices where no node is guaranteed to be online
continuously. The KV/Raft layer should therefore converge opportunistically when
nodes wake, operators run commands, apps connect, or peer traffic arrives. The
goal is not to turn `sinomo-kv` into a conventional always-on Raft service.

The current implementation already has useful safety machinery: persisted Raft
hard state, WAL-backed log entries, quorum-gated writes, append conflict checks,
vote freshness checks, snapshots, materialized local snapshots, durable watches,
and host-provided transport. The main gaps are around explicit consistency
contracts, passive-cluster liveness, idempotency, membership-change lifecycle,
and operational recovery.

## Design Principles

1. Preserve the passive cluster model.
   Raft work should be driven by user actions, daemon wakeups, app sessions,
   rollout activity, peer RPCs, and periodic opportunistic maintenance when a
   daemon is already alive.

2. Separate correctness from freshness.
   Writes should remain quorum-gated. Reads should be explicit about whether
   they are local-cache reads, leader-refreshed reads, or quorum-fresh reads.

3. Keep ownership boundaries.
   `sinomo-kv` owns KV semantics and Raft safety decisions. `sinomo-network`
   owns topology, routing, endpoint selection, and transport. The daemon binds
   them together with identity, authorization, runtime, and process ownership.

4. Prefer bounded convergence over background churn.
   A node that wakes should quickly discover current leadership, catch up its
   local KV view, publish status, and drain queued work without assuming an
   always-on server set.

## P0: Safety And Correctness

### 1. Define the KV consistency contract

Document every KV-facing operation as one of:

- `local-cache`: reads the best local materialized or applied view and may be
  stale.
- `leader-refreshed`: routes to or refreshes from the known leader when
  possible, and fails or degrades explicitly when freshness cannot be proven.
- `quorum-fresh`: requires a quorum-backed check or commit path before
  returning.

Apply this classification to snapshot, range-prefix, watch creation/drain,
cluster membership, migration, rollout desired state, node status, and network
connectivity reads.

Expected outcome: callers cannot accidentally use a stale local snapshot for a
decision that needs cluster authority.

### 2. Add critical-read freshness APIs

Keep cheap local reads for dashboards and status views, but add explicit helper
APIs for critical decisions:

- Read from known leader if reachable.
- Refresh local KV from leader or quorum before returning.
- Fail with structured diagnostics when freshness cannot be established.

Initial critical callers should include cluster join/leave/migration planning,
membership mutation, Raft config mutation, rollout desired state, invitations,
and cluster-level config.

Expected outcome: passive clusters keep cheap reads, while dangerous decisions
get a stronger freshness path.

### 3. Add request IDs and idempotency records

Add stable client request IDs for KV transactions. Persist enough metadata to
answer whether a submitted request committed, failed before commit, or is
unknown after timeout.

This should complement, not replace, semantic no-effect checks. Semantic
no-effect is useful for repeated state publication, but it is not a general
duplicate-request protocol.

Expected outcome: retries after disconnects or mobile sleep do not depend on
guessing whether a previous request committed.

### 4. Make Raft config changes first-class lifecycle operations

Keep the current transition validation, but represent membership/config changes
as explicit committed lifecycle steps:

- Add learner.
- Prove catch-up to a required revision/log index.
- Promote one learner to voter.
- Demote or remove voter with overlapping quorum validation.
- Record completion/failure evidence.

Expected outcome: membership changes are auditable, restart-safe, and safer
than ad hoc config writes.

## P1: Passive-Cluster Liveness

### 5. Add an opportunistic maintenance tick

Create a reusable maintenance action that can run whenever the daemon is already
active. It should not require an always-on Raft loop.

The tick should:

- Refresh known leader and observed term.
- Send or respond to append heartbeats when appropriate.
- Pull or apply missing KV snapshots.
- Catch up local WAL/snapshot state.
- Compact Raft state when thresholds are met.
- Drain and fan out watch events.
- Publish node status and endpoint observations.
- Record structured evidence for any skipped step.

Trigger it from daemon startup, app/CLI connection, cluster status/health,
rollout activity, migration operations, peer RPC handling, and low-frequency
daemon timers when the daemon is already running.

Expected outcome: clusters converge when touched without requiring dedicated
server nodes.

### 6. Improve leader recovery for intermittent nodes

Refine election behavior for passive clusters:

- Prefer the freshest reachable voter when leadership is unknown.
- Avoid repeated local campaigns when another leader was recently observed.
- Record election-failure reasons: no quorum, stale log, transport failure,
  voter rejection, higher observed term, or local node not voter.
- Surface recommended recovery action in CLI/app diagnostics.

Expected outcome: waking nodes converge toward the best leader candidate instead
of repeatedly thrashing elections.

### 7. Persist soft peer replication progress hints

Store non-authoritative per-peer hints such as last accepted index and last
observed match index. These hints should accelerate catch-up but must be safe to
discard.

Expected outcome: reconnecting nodes start near the right `next_index` and avoid
unnecessary backtracking or full snapshots.

### 8. Add uncertain-write recovery

Expose a query path for submitted request IDs:

- committed with response
- rejected before commit
- still unknown
- expired from idempotency retention

Expected outcome: clients can recover from timeout and reconnect without blind
retries.

## P2: Performance And Operability

### 9. Batch and coalesce noisy writes

Add short-window opportunistic batching for high-churn keys such as node status,
network connectivity, and network info. Preserve strict paths for membership,
config, migration, invitations, and rollout desired state.

Expected outcome: passive wakeups publish fewer redundant commits while critical
state remains strongly handled.

### 10. Split critical and telemetry KV behavior

Define keyspace classes:

- Critical: membership, Raft config, cluster config, migrations, invitations,
  rollout desired state.
- Operational: endpoint records, capability records, node-local config.
- Telemetry: node status, network connectivity, network info, health probes.

Use stricter freshness/durability defaults for critical keys and allow deferred
materialized snapshot writes or stale reads for telemetry keys.

Expected outcome: the system stops treating every KV key as if it had the same
consistency and durability requirements.

### 11. Make snapshot transfer resumable

Whole-state snapshots are acceptable for the current system, but large passive
clusters need a safer transfer path. Add chunked or resumable snapshot transfer
before KV state grows substantially.

Expected outcome: mobile and home-network nodes can catch up reliably over
interrupted links.

### 12. Improve quorum and freshness diagnostics

Every read-only, stale-read, or quorum failure should include:

- Local node id and role.
- Known leader and last observed term.
- Reachable voters versus required quorum.
- Last successful append or snapshot refresh.
- Whether local data is stale, fresh, or unknown.
- Suggested recovery action.

Expected outcome: operators can distinguish real data loss, temporary offline
quorum, stale local cache, and routing failure.

## P3: Full Raft Features To Defer

### 13. Full joint consensus

Do not start here unless membership churn proves unsafe. A disciplined
learner-catch-up-promotion lifecycle may be enough for Sinomo's near-term
cluster model.

### 14. ReadIndex everywhere

Do not make quorum reads the default. Add leader/quorum freshness only for
critical operations. Dashboards and local status views should remain cheap and
available during partial connectivity.

### 15. Autonomous Raft heartbeat/election loop

Avoid a conventional always-on loop as the default. It conflicts with the
reactive cluster model. If needed later, make it an optional server-mode
capability for deployments that intentionally run stable always-on nodes.

## Recommended First Implementation Slice

1. Add the consistency contract to docs and route/type names.
2. Implement critical-read freshness helpers.
3. Add request IDs and idempotency records for KV transactions.
4. Add the opportunistic maintenance tick and wire it to daemon/app/CLI wake
   points.
5. Convert Raft config changes into explicit lifecycle operations with evidence.

This slice preserves Sinomo's passive design while addressing the most important
correctness and recovery gaps first.
