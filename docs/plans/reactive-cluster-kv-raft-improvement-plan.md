# Reactive Cluster KV/Raft Improvement Plan

Status: the first slice, action-owned convergence, bounded group commit and
shared follower replication are implemented. Remaining follow-up work is
identified below.

## Context

Sinomo's intended cluster shape is reactive and passive. A cluster may be made of
home PCs, laptops, and mobile devices where no node is guaranteed to be online
continuously. The KV/Raft layer therefore converges through the operation or
state transition that made work necessary. It has no generic maintenance task,
periodic sweep, or read-triggered convergence path. The goal is not to turn
`sinomo-kv` into a conventional always-on Raft service.

The implementation now combines the earlier persisted Raft hard state,
WAL-backed log entries, quorum-gated mutation paths, append conflict checks,
vote freshness checks, snapshots, durable watches, and host-provided transport
with the completed first slice: explicit local and authoritative consistency
contracts, current-term barriers, semantic-no-effect write fencing, stable
request identities and outcome recovery, action-owned reactive convergence,
peer progress hints, and restart-safe membership/config-change lifecycle
records.

## Delivery Status

The numbered sections below preserve the design and verification rationale that
guided the refactor. Their current status is:

| Item | Status |
|---|---|
| P0.1-P0.4 consistency, barriers, request identity, config lifecycle | Completed. |
| P1.5 action-owned reactive convergence | Completed; this replaces the withdrawn generic maintenance-tick design. |
| P1.6 intermittent-node leader recovery | Partial: bounded recovery, structured failure evidence, campaign suppression while a live remote leader is observed, and transient route-exhaustion evidence for replacing an unreachable stale leader exist; proactive freshest-voter preference and complete CLI/app recommendations remain follow-up work. |
| P1.7 peer replication progress hints | Completed; hints are persisted, consumed, term-validated, and safe to discard. |
| P1.8 uncertain-write recovery | Completed through the authoritative request-outcome route. |
| P2.9 noisy-write batching | Partial: concurrent ordinary proposals share bounded short-window WAL and ledger flushes and follower appends while each keeps its own log entry and result, and membership changes and election barriers keep individual admission (see [clustering architecture](../clustering-architecture.md#control-plane-kv)). Merging redundant high-churn updates into fewer commits remains planned; semantic no-op suppression and source-change coalescing are not that batching. |
| P2.10 keyspace read/projection behavior | Completed for explicit keyspace classification, critical-read authority defaults, and deferred materialized projections; broader retention tuning remains operational follow-up. |
| P2.11 resumable snapshots | Planned. |
| P2.12 diagnostics | Partial: structured authority/quorum evidence exists; complete last-success timestamps and CLI/app recovery presentation remain follow-up work. |
| P3.13-P3.15 full joint consensus, ubiquitous ReadIndex, autonomous loop | Deliberately deferred. |

Future-tense requirements under completed items describe the delivered contract;
they are retained as design history rather than an assertion that the work is
still missing.

## Design Principles

1. Preserve the passive cluster model.
   Every Raft action should be owned by the operation, committed transition, or
   concrete recovery signal that requires it. Daemon wakeups, app sessions,
   status reads, and generic timers are not work by themselves.

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
   A mutation or authoritative operation establishes the authority it needs;
   an actual replication gap initiates targeted catch-up; and an actual source
   change initiates publication. An idle node with no changed inputs performs
   no KV work.

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

### 5. Make reactive convergence action-owned

The original implementation introduced a reusable maintenance action that
coalesced daemon, app, CLI, status, health, Raft, and timer wakes into one broad
sweep. That design is withdrawn. It caused unrelated observations to schedule
authority barriers, catch-up, configuration reconciliation, and publication,
and allowed work completed by one sweep to wake another sweep. A fully reactive
KV does not need a generic maintenance task.

Convergence belongs at the exact edge that has both the evidence and authority
to perform it:

| Origin | Work owned by that origin |
|---|---|
| Successful commit | Apply the committed entries, deliver their watch events, fan them out to the exact peers selected for that commit, and persist peer-progress hints from the same fanout report. |
| Mutation or authoritative operation | Establish leadership and quorum evidence required by that operation; do not enqueue unrelated follow-up work. |
| Membership or Raft-configuration lifecycle operation | Reconcile its own durable transition before and after the operation. Startup may directly resume a known nonterminal transition, but must not start a general sweep. |
| Append gap, explicit startup recovery, or restored route to a known-behind peer | Run targeted log or snapshot recovery for that peer and gap. Snapshot transfer is not a periodic freshness probe. |
| WAL, log, or snapshot mutation | Compact when the owning store crosses its mutation or size threshold. Compaction is not timer-driven maintenance. |
| Local member, endpoint, or connectivity source change | Publish the changed semantic value, coalesced by its typed key. An unchanged source produces no RPC and no KV mutation. |
| Peer Raft RPC | Validate and apply or answer that request, including any exact catch-up evidence it reveals. It does not wake a broader coordinator. |
| Local status, health, app, or CLI read | Return the requested observation only. It never schedules KV convergence. An authoritative read owns only its requested quorum barrier. |

Retries remain bounded and attached to the originating operation or typed,
keyed source-change job. They must not be converted into a process-wide
"something may need maintenance" notification. Observation records what an
owned action attempted and why it stopped; observation is never itself a work
trigger.

Expected outcome: clusters converge from concrete changes without dedicated
server nodes, while a quiescent cluster performs no control-plane writes,
barriers, snapshots, or generic convergence passes.

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

Do not model daemon scheduling, source-change coalescing, retry backoff, routing,
snapshot RPCs, or app/CLI wiring in Quint. The reactive refactor changes which
concrete operation owns an existing Raft action, not the core consensus
algorithm. Model those implementation mechanics with deterministic Rust and
cluster tests.

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

The first slice and action-owned convergence require:

- focused tests proving local reads remain available without quorum while
  authoritative reads fail;
- tests proving ordinary writes and semantic no-op writes cannot return success
  without quorum;
- tests proving quorum-backed writes, authoritative reads, and exact committed
  retries return the expected revision/result;
- deterministic tests proving exact commit fanout and peer-progress persistence,
  operation-owned configuration reconciliation, threshold-owned compaction,
  targeted snapshot recovery, and source-change publication;
- tests proving local status, health, app, and CLI reads do not schedule KV work;
- an idle-cluster quietness gate proving that a quiescent control plane emits no
  commits, quorum barriers, snapshot pulls, or generic convergence passes; and
- a three-node intermittent-cluster scenario covering offline local reads,
  failed authoritative operations, quorum restoration, catch-up, and eventual
  convergence.

## First Implementation Slice

The slice requires:

1. A consistency contract expressed in docs and route/type names.
2. An authoritative-read barrier for correctness-sensitive callers.
3. Write success backed by quorum or durable prior-commit evidence, including
   semantic-no-effect writes.
4. Request IDs, bounded committed-request retention, and outcome records for
   KV transactions.
5. Action-owned convergence: exact commit fanout persists its own peer
   progress, configuration operations reconcile their own lifecycle, storage
   mutations own compaction, recovery signals target the affected peer, and
   changed member/endpoint sources own publication. There is no generic
   maintenance task or observational wake point.
6. Raft config changes expressed as explicit lifecycle operations with catch-up
   and terminal evidence plus restart reconciliation.

The small `raft_kv` contract refinement covers steps 2-3. Runtime and cluster
tests cover the action-ownership boundaries in step 5 instead of expanding the
Raft protocol model. Require coverage of the deliberately broken
no-quorum/no-effect witness and bounded verification of the corrected relation;
commands and toolchain configuration are described in the
[formal verification guide](../formal/cluster-verification.md).

This slice preserves Sinomo's passive design while addressing the most important
correctness and recovery gaps first.
