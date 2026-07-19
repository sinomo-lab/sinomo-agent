# Reactive Cluster KV/Raft Improvement Plan

Status: recommended first implementation slice completed 2026-07-19; remaining
P1-P3 follow-up work is identified below. Last reviewed 2026-07-19.

## Context

Sinomo's intended cluster shape is reactive and passive. A cluster may be made of
home PCs, laptops, and mobile devices where no node is guaranteed to be online
continuously. The KV/Raft layer should therefore converge opportunistically when
nodes wake, operators run commands, apps connect, or peer traffic arrives. The
goal is not to turn `sinomo-kv` into a conventional always-on Raft service.

The implementation now combines the earlier persisted Raft hard state,
WAL-backed log entries, quorum-gated mutation paths, append conflict checks,
vote freshness checks, snapshots, durable watches, and host-provided transport
with the completed first slice: explicit local and authoritative consistency
contracts, current-term barriers, semantic-no-effect write fencing, stable
request identities and outcome recovery, reactive maintenance, peer progress
hints, and restart-safe membership/config-change lifecycle records.

## Delivery Status

The numbered sections below preserve the design and verification rationale that
guided the refactor. Their current status is:

| Item | Status |
|---|---|
| P0.1-P0.4 consistency, barriers, request identity, config lifecycle | Completed. |
| P1.5 opportunistic maintenance tick | Completed. |
| P1.6 intermittent-node leader recovery | Partial: bounded recovery and structured failure evidence exist; proactive freshest-voter preference and complete CLI/app recommendations remain follow-up work. |
| P1.7 peer replication progress hints | Completed; hints are persisted, consumed, term-validated, and safe to discard. |
| P1.8 uncertain-write recovery | Completed through the authoritative request-outcome route. |
| P2.9 noisy-write batching | Planned; semantic no-op suppression and maintenance wake coalescing do not constitute general short-window transaction batching. |
| P2.10 keyspace read/projection behavior | Completed for explicit keyspace classification, critical-read authority defaults, and deferred materialized projections; broader retention tuning remains operational follow-up. |
| P2.11 resumable snapshots | Planned. |
| P2.12 diagnostics | Partial: structured authority/quorum evidence exists; complete last-success timestamps and CLI/app recovery presentation remain follow-up work. |
| P3.13-P3.15 full joint consensus, ubiquitous ReadIndex, autonomous loop | Deliberately deferred. |

Future-tense requirements under completed items describe the delivered contract;
they are retained as design history rather than an assertion that the work is
still missing.

## Design Principles

1. Preserve the passive cluster model.
   Raft work should be driven by user actions, daemon wakeups, app sessions,
   rollout activity, peer RPCs, and periodic opportunistic maintenance when a
   daemon is already alive.

2. Separate local availability from cluster authority.
   Reads expose either a local view that may be stale or an authoritative view
   backed by a current-term quorum barrier. Known-leader preference and snapshot
   refresh are implementation mechanisms, not caller-visible consistency
   levels. A cluster KV write reports success only after quorum-backed commit.

3. Keep ownership boundaries.
   `sinomo-kv` owns KV semantics and Raft safety decisions. `sinomo-network`
   owns topology, routing, endpoint selection, and transport. `sinomo-cluster`
   owns the transport-coupled Raft and membership drivers and is the only R0
   bridge between them. The daemon hosts that stack and supplies process
   identity, authorization, runtime scheduling, and local persistence.

4. Prefer bounded convergence over background churn.
   A node that wakes should quickly discover current leadership, catch up its
   local KV view, publish status, and drain queued work without assuming an
   always-on server set.

## P0: Safety And Correctness

### 1. Define the two-level KV consistency contract

Document public KV reads as one of:

- `local`: reads the best local materialized or applied view and may be stale.
- `authoritative`: returns a view at or after a current-term quorum-confirmed
  barrier and fails when that authority cannot be established.

Do not expose `leader-refreshed` as a consistency level. Trying the known leader
first is useful routing, but a leader response without current quorum evidence
does not prove freshness. An authoritative read must never silently degrade to
a local read.

Apply this classification to snapshot, range-prefix, watch creation/drain,
cluster membership, migration, rollout desired state, node status, and network
connectivity reads. An authoritative watch establishes its starting revision
through a barrier; later delivery may lag but must remain ordered and report its
last applied revision.

Define the write contract at the same boundary: every successful cluster KV
mutation, including a locally detected semantic no-op, must be backed by either
a current-term quorum barrier or durable evidence that the exact request
already committed. State that does not justify quorum-backed commit belongs in
a local store, event/telemetry path, or another subsystem rather than cluster
KV.

Expected outcome: callers cannot accidentally use a stale local snapshot for a
decision that needs cluster authority, and write success always means committed
cluster state.

### 2. Add authoritative-read barrier APIs

Keep cheap local reads for dashboards and status views, but add explicit helper
APIs for critical decisions:

- Establish a current-term quorum barrier through a ReadIndex-style check,
  equivalent quorum confirmation, or initially a quorum-backed no-op.
- Return the authoritative snapshot/value plus its committed revision and term.
- Prefer the known leader internally, but do not treat reachability alone as
  authority.
- Fail with structured diagnostics when the barrier cannot be established; the
  caller must explicitly choose a separate local-read API if stale data is
  acceptable.

Initial critical callers should include cluster join/leave/migration planning,
membership mutation, Raft config mutation, rollout desired state, invitations,
and cluster-level config.

Expected outcome: passive clusters keep cheap local reads, while dangerous
decisions have one unambiguous authoritative path.

### 3. Add request IDs and idempotency records

Add stable client request IDs for KV transactions. Persist enough metadata to
answer whether a submitted request committed, failed before commit, or is
unknown after timeout.

This should complement, not replace, semantic no-effect checks. Semantic
no-effect is useful for repeated state publication, but it is not a general
duplicate-request protocol and cannot by itself prove that local state is
current. An exact retry may reuse a stored response without a new log entry only
when its idempotency record proves that the request previously committed.

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

The tick schedules existing Raft actions; it must not introduce a second
consensus path, weaken quorum rules, or make timer/wake evidence authoritative.

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
network connectivity, and network info. Every accepted batch still follows the
same quorum commit contract. Preserve immediate paths for membership, config,
migration, invitations, and rollout desired state.

Expected outcome: passive wakeups publish fewer redundant commits while critical
state remains strongly handled.

### 10. Split read defaults and projection behavior by keyspace

Define keyspace classes:

- Critical: membership, Raft config, cluster config, migrations, invitations,
  rollout desired state.
- Operational: endpoint records and capability records.
- Telemetry: node status, network connectivity, network info, health probes.

All cluster KV classes retain the same quorum-backed write semantics. Use
authoritative reads by default for critical decisions, and local reads by
default for operational and telemetry views. Operational and telemetry keys may
use coalescing, retention limits, and deferred derived/materialized projections.
If telemetry is not worth a quorum commit, keep it outside cluster KV. Node-local
configuration and fail-closed permission state remain outside cluster KV.

Expected outcome: the system varies availability and projection cost without
creating weaker meanings of cluster KV write success.

### 11. Make snapshot transfer resumable

Whole-state snapshots are acceptable for the current system, but large passive
clusters need a safer transfer path. Add chunked or resumable snapshot transfer
before KV state grows substantially.

Expected outcome: mobile and home-network nodes can catch up reliably over
interrupted links.

### 12. Improve quorum and freshness diagnostics

Every local-read warning or authoritative/quorum failure should include:

- Local node id and role.
- Known leader and last observed term.
- Reachable voters versus required quorum.
- Last successful append or snapshot refresh.
- Whether the returned data is local or authoritative, its revision/term, and
  whether its freshness is unknown.
- Suggested recovery action.

Expected outcome: operators can distinguish real data loss, temporary offline
quorum, stale local cache, and routing failure.

## P3: Full Raft Features To Defer

### 13. Full joint consensus

Do not start here unless membership churn proves unsafe. A disciplined
learner-catch-up-promotion lifecycle may be enough for Sinomo's near-term
cluster model.

### 14. ReadIndex everywhere

Do not make authoritative reads the default. Implement one bounded barrier path
for critical operations; it may use ReadIndex or an equivalent quorum check.
Dashboards and local status views should remain cheap and available during
partial connectivity.

### 15. Autonomous Raft heartbeat/election loop

Avoid a conventional always-on loop as the default. It conflicts with the
reactive cluster model. If needed later, make it an optional server-mode
capability for deployments that intentionally run stable always-on nodes.

## Verification Strategy

### Quint boundary

Do not model daemon wake sources, maintenance timers, backoff, routing, snapshot
RPCs, or app/CLI wiring in Quint. The reactive refactor changes when existing
Raft actions run, not the core consensus algorithm. Model those implementation
mechanics with deterministic Rust and cluster tests.

Keep `specs/quint/models/kv/raft_kv.qnt` as the small algorithm contract. Extend
it only enough to express:

- local-read success may observe an older committed prefix;
- authoritative-read success requires a current-term quorum barrier and returns
  the canonical committed prefix;
- write success requires quorum-backed commit; and
- a semantic no-op cannot report authoritative success without a barrier or an
  exact durable committed-request record.

Add a deliberately broken no-quorum/no-effect transition and require the model
to find its counterexample before accepting the refinement. Do not expand
`raft_protocol.qnt` for the reactive work. Model request-id retention and
uncertain outcomes separately only when their algorithm is concrete enough to
state a small independent invariant.

### Runtime and cluster evidence

The completed first slice is covered by:

- focused tests proving local reads remain available without quorum while
  authoritative reads fail;
- tests proving ordinary writes and semantic no-op writes cannot return success
  without quorum;
- tests proving quorum-backed writes, authoritative reads, and exact committed
  retries return the expected revision/result;
- deterministic virtual-time tests for wake coalescing, retry backoff, shutdown,
  and non-starvation of maintenance work; and
- a three-node intermittent-cluster scenario covering offline local reads,
  failed authoritative operations, quorum restoration, catch-up, and eventual
  convergence.

## Completed First Implementation Slice

Completed 2026-07-19:

1. Added the consistency contract to docs and route/type names.
2. Implemented the authoritative-read barrier and migrated the initial critical
   callers.
3. Closed the semantic-no-effect fast path so write success always has quorum or
   durable prior-commit evidence.
4. Added request IDs, bounded committed-request retention, and outcome records
   for KV transactions.
5. Added the opportunistic maintenance tick and wired it to daemon/app/CLI wake
   points.
6. Converted Raft config changes into explicit lifecycle operations with
   catch-up and terminal evidence plus restart reconciliation.

The small `raft_kv` contract refinement covers steps 2-3. Runtime and cluster
tests cover step 5 instead of expanding the Raft protocol model. The focused
formal run reached the deliberately broken no-quorum/no-effect witness in 5000
of 5000 traces, and the five-step Apalache check of the corrected relation found
no violation; exact commands and tool versions are recorded in the
[formal verification guide](../formal/cluster-verification.md).

This slice preserves Sinomo's passive design while addressing the most important
correctness and recovery gaps first.
