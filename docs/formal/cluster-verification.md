# Cluster Formal Verification

This directory starts the executable specification track for Sinomo cluster
infrastructure. The first models are intentionally small: a three-node
Raft-backed KV skeleton with one abstract KV cell and a bounded three-entry log,
an adversarial network layer with directed reachability and an in-flight message
buffer, network endpoint-selection/route-planning/delivery-retry layers, a
shared full-system smoke model, a KV-authorized membership layer, a
membership/KV bridge that uses the lower KV proof as an abstract commit
contract, a first DFS metadata-authority layer, a DFS authority-routing layer
for stale shard views, a DFS namespace transaction layer for first visible
publication, a DFS metadata journal source-validation layer, a DFS repair
authorization layer, a DFS replication-policy layer for N/W commits plus
asynchronous repair, a DFS no-conflict layer for normal-operation write
serialization, a DFS pending request lifecycle layer, a DFS repair
content-validation layer, a DFS chunked-transfer layer, a DFS repair
lost-authority suppression layer, and a DFS local content anti-entropy layer,
plus a sharded DFS namespace transaction router layer. The goal is to verify the
control-plane safety shape before adding leases, watches, snapshots, broader DFS
workflows, and rollout.

## Current Specs

- `specs/quint/common.qnt` defines the shared finite cluster model: three nodes,
  voter quorum helpers, online voter selection, and a small `Option` type.
- `specs/quint/membership_base.qnt` defines shared membership roles, states,
  initial membership values, and projection helpers.
- `specs/quint/raft_kv.qnt` models the first Raft/KV slice: leader election,
  node online/offline transitions, catch-up, leader proposals, quorum commit,
  committed-prefix agreement, and KV-state replay from the committed log.
- `specs/quint/network.qnt` models network mechanism: node process liveness,
  directed links, an adversarial in-flight message set, send, deliver, drop,
  link partition/heal, crash, restart, discovery observations, and message
  evidence carried as data.
- `specs/quint/network_endpoint_selection.qnt` models command-style
  main-daemon endpoint selection: authorized remote members, shadow/local-only
  filtering, identity-mismatch exclusion, dialability preference, and
  unique-node target reduction.
- `specs/quint/network_route_planning.qnt` models the route graph planner:
  direct target edges, relay paths, failed-edge exclusion, visited-node loop
  prevention, identity-mismatch suppression, and deterministic cost ordering.
- `specs/quint/network_delivery_retry.qnt` models endpoint attempt retry and
  feedback: stable delivery keys, fallback attempts, identity-mismatch
  suppression, TTL/deadline terminal failures, and replay-cache duplicate
  suppression.
- `specs/quint/system_base.qnt` defines the shared integration vocabulary for a
  full-system smoke model: membership, endpoint identity, connectivity
  observations, route selection, delivery keys, DFS metadata authority, and
  visible DFS writes.
- `specs/quint/system_guarantees.qnt` defines layered guarantee predicates over
  `system_base`: active route validity, identity-mismatch suppression, DFS
  primary routing, stale-response rejection, shadow-endpoint exclusion, replay
  cache behavior, and single-authority visibility.
- `specs/quint/system_smoke.qnt` composes `system_base` actions with
  `system_guarantees.systemSafety` for randomized whole-system exploration.
- `specs/quint/membership.qnt` models cluster membership authority: invites,
  local join intent, committed pending records, learner acceptance, catch-up,
  voter promotion, demotion, health-state changes, removal, and shadow endpoint
  observations that are not members.
- `specs/quint/membership_kv.qnt` models membership changes over the abstract KV
  commit contract. It deliberately does not re-model Raft internals; it consumes
  the lower-layer guarantee that the KV exposes one canonical append-only
  committed command prefix.
- `specs/quint/dfs_metadata.qnt` models a DFS metadata-shard authority slice:
  committed shard primary and epoch, visible object versions, write intents,
  replica receipts, primary handoff, and stale-intent rejection.
- `specs/quint/dfs_authority_routing.qnt` models requester-side DFS routing over
  stale metadata-shard views: cached primary selection, authority redirects,
  redirect limits, unroutable redirects, late or mismatched response discard, and
  the namespace lookup shortcut that serves locally after a redirect to the
  requester.
- `specs/quint/dfs_namespace_transaction.qnt` models shard-local first
  publication through `DfsNamespaceTransaction`: a durable transaction record,
  object-row materialization, visible-version-row materialization, interrupted
  materialization, and idempotent recovery/replay.
- `specs/quint/dfs_sharded_namespace_transaction.qnt` models the sharded
  metadata router above shard-local namespace transactions: target-shard
  transaction splitting, object-shard index updates, cross-shard tombstone
  cleanup, and partial-failure lookup semantics.
- `specs/quint/dfs_metadata_journal_source.qnt` models metadata journal and
  snapshot source validation across the strict standalone replica processor and
  the service-level routed catch-up handler.
- `specs/quint/dfs_repair_authorization.qnt` models the authorization predicate
  used by repair and chunked repair commands: durable intent authority,
  metadata-primary proof, local owner fallback, and local current-replica
  fallback after metadata-primary movement.
- `specs/quint/dfs_replication_policy.qnt` models the replicated-eventual
  N/W surface: valid `DfsReplicationPolicy` values, strict
  replicated-confirmed writes, desired-replica durable receipts, visible commit
  threshold, and repair jobs for desired replicas still missing receipts.
- `specs/quint/dfs_no_conflict.qnt` models normal quorum-backed write
  serialization: two writers racing on one object, stale-base rejection,
  duplicate-target rejection, pending-intent rejection, wrong-primary rejection,
  metadata-primary handoff, and visible commit without conflict records.
- `specs/quint/dfs_pending_request_lifecycle.qnt` models request-id ownership
  and response handling across pending reads, routed writes, primary-side remote
  write responses, routed namespace ops, stage transfers, and repair transfers.
- `specs/quint/dfs_repair_content_validation.qnt` models the content-identity
  gates before stage, repair, local-manifest, and hydrated backend receipts
  become durable DFS replica receipts.
- `specs/quint/dfs_chunked_transfer.qnt` models bounded chunk windows, receiver
  chunk validation, duplicate handling, resume indexes, and the final full-hash
  check before a chunked transfer can produce a durable response.
- `specs/quint/dfs_repair_lost_authority.qnt` models lost-authority repair
  suppression, stale-base retry classification, stale chunk-resume reset,
  transient route retry preservation, and restored repair-attempt records.
- `specs/quint/dfs_anti_entropy.qnt` models local content anti-entropy scans:
  object eligibility, hash-addressed blob fallback, issue cap reporting,
  metadata-repair reporting, and durable stale-location pruning only when the
  local service is allowed to write the metadata shard.

This maps to the repository boundary documented in
`docs/clustering-architecture.md` and `docs/network-architecture.md`:
`sinomo-kv` owns KV/Raft semantics and stays transport-free, `sinomo-network`
owns transport and routing mechanism without depending on cluster policy, and
`sinomo-cluster` bridges them by moving consensus and membership traffic over
network routes backed by authoritative cluster state.

## What The Models Check

The specs are layered. A higher-level model should consume the smallest
interface it needs from lower layers rather than re-verifying their internals.
For example, `membership_kv` assumes the `raft_kv` guarantee of a canonical
committed KV prefix and proves membership replay/config projection on top of
that prefix. A later DFS authority model should consume committed membership and
KV facts, not Raft election state or raw network buffers.

The `raft_kv.safety` invariant combines these properties:

- a node that claims a committed index agrees with the canonical committed log
  prefix;
- every globally committed slot is filled;
- at most one leader is recorded for each bounded term;
- the visible KV cell equals replay of the committed log prefix;
- the global commit index and next log index stay inside the model bound.

The model already caught one useful bug during drafting: an offline node was
allowed to come online without catch-up, then accept a later committed entry
without the earlier prefix. The Rust in-process cluster catches up a node when
`set_node_online(..., true)` is called, so the spec now models that behavior.

The `network.safety` invariant combines these properties:

- every delivered message was explicitly sent;
- every in-flight message was explicitly sent;
- network activity cannot grant or change cluster trust;
- discovery is only an observation about node identifiers;
- self-links stay disabled.

The important design choice is that `deliver` does not remove a message from
`inFlight`. That over-approximates duplicate delivery and retries. `drop` is the
separate adversarial action that removes a message. Reordering is modeled by
nondeterministically choosing any in-flight message for delivery or drop.

The `network_endpoint_selection.safety` invariant combines these properties:

- selected main-daemon command candidates are authorized remote members;
- shadow-daemon endpoints are excluded from main-daemon command candidates;
- remote `LocalOnly` endpoints are excluded;
- endpoints for offline nodes are excluded from this command-selection surface;
- identity-mismatch, degraded, and unreachable command endpoints are excluded;
- if any dialable candidate exists, the first candidate is dialable, so
  disabled module transports remain last-resort evidence rather than burning the
  preferred attempt budget;
- unique-node target selection keeps at most one endpoint per node and covers
  every node represented by the candidate set.

This maps to `ClusterEndpointSelector`, `try_cluster_main_daemon_command_endpoint_candidates_impl`,
`cluster_endpoint_can_carry_main_daemon_rpc`,
`cluster_endpoint_identifies_shadow_daemon_for_node`,
`cluster_daemon_command_endpoint_is_usable`, and
`deprioritize_endpoints_without_dialable_transport`. It deliberately models the
command-style main-daemon candidate surface first; data-plane and rollout
purpose-specific selectors still deserve separate refinements if their ranking
rules diverge.

The `network_route_planning.safety` invariant combines these properties:

- selected routes avoid already visited nodes;
- selected routes use no failed edge;
- selected routes use only usable edges;
- identity-mismatch edges are never selected;
- relay routes require a complete proven path from local to relay to target;
- direct routes require a usable direct edge;
- a no-more-expensive direct route is preferred over a relay route;
- after direct edge failure, a compatible relay route is selected when one
  exists;
- `NoRoute` is returned only when no compatible direct or relay path exists;
- selected paths contain no loop.

This maps to `ClusterDeliveryRouteGraph`, `cluster_delivery_route_candidates`,
`cluster_delivery_select_route_candidate`,
`cluster_delivery_route_candidate_is_compatible`,
`cluster_delivery_shortest_path`, and the connectivity cost helpers in
`route.rs`. It preserves an implementation nuance: an ordinary unreachable
direct target endpoint can remain as a high-cost last-resort fallback, but
identity-mismatch evidence and unreachable relay edges are excluded.

The `network_delivery_retry.safety` invariant combines these properties:

- a delivered response requires a successful endpoint attempt;
- delivered responses are entered in the replay cache;
- duplicate delivery-key replay does not redispatch the inner request;
- primary and fallback attempts use the original stable delivery key;
- failed and suppressed endpoints were attempted;
- identity-mismatch endpoint failures are suppressed and recorded as
  identity-mismatch feedback;
- endpoint feedback matches the observed attempt result;
- deadline and TTL terminal failures cannot produce a delivered response;
- a primary failure with a configured fallback and remaining budget cannot
  terminally fail with `AllEndpointsFailed` before attempting the fallback;
- delivered states are terminal and carry no terminal error.

This maps to the target/relay send retry paths in `route.rs`, including
`ClusterDeliveryFailedEdges`, `record_cluster_delivery_endpoint_identity_mismatch`,
the per-endpoint attempt loop, and `RelayEnvelope`'s stable inner request. It
models duplicate suppression as a replay-cache contract at the delivery-key
level; concrete daemon-auth nonce handling stays in Rust tests because each
endpoint attempt still needs a fresh transport nonce.

The full-system smoke model is intentionally different from the focused proof
modules. `system_base` owns one shared cross-layer state vocabulary, and
`system_guarantees` states integration-level contracts that are broad enough to
catch contradictions:

- removed members do not receive authorized routed work;
- shadow endpoints are never used for main-daemon routed work or DFS authority;
- active DFS writes route only to the current metadata primary;
- stale or mismatched responses do not commit visible DFS state;
- identity-mismatch endpoint observations suppress future active use;
- successful visible DFS writes record metadata authority at commit time;
- a DFS object is visible from at most one modeled metadata authority;
- duplicate delivery-key replay does not redispatch or create a new commit.

This model should normally be run with `quint run`, not treated as the main
proof unit. The first smoke draft found a useful cross-layer counterexample:
an endpoint could become `ConnUnreachable` while the model still treated a
pending DFS write as actively routed through it. The base model now clears the
active route when the observed endpoint becomes unreachable or
identity-mismatched, matching the retry-loop boundary before replanning.

The `membership.safety` invariant combines these properties:

- authorized members must have a committed KV membership record;
- pending and removed records are not authorized;
- discovery and unverified accepted responses cannot create membership
  authority;
- only voter, learner, and non-voting states enter the derived Raft config;
- voter promotion requires a committed learner or non-voting base, catch-up, and
  a distinct voter actor;
- membership mutations never leave the cluster without a voter;
- shadow daemon reachability is never modeled as a cluster member.

`NonMember` is a model-only state for "there is no `/cluster/members/<node_id>`
record yet." It is not a Rust `ClusterMemberState`; it lets the spec distinguish
unknown candidates from committed `Pending` records.

The `membership_kv.safety` invariant combines these properties:

- materialized membership state exactly equals replay of the committed KV
  membership-command prefix;
- derived Raft voters, learners, and non-voting members exactly equal the
  projection of committed membership records;
- every committed membership command was enabled in the replayed prefix before
  it;
- inactive states stay outside the derived Raft config;
- authorized members require committed KV records;
- voter promotion still requires committed membership, catch-up evidence, and a
  distinct promotion actor.

`membership_kv` has two transition surfaces. `step` covers the broader command
set for randomized exploration. `stepCore` covers the join-to-voter lifecycle
for symbolic verification. TLC is used for this finite refinement-style check
because Apalache struggles with the function-valued replay formulas in this
module.

The `dfs_metadata.safety` invariant combines these properties:

- the current metadata-shard primary is an active committed member;
- every primary epoch records the configured primary from the abstract KV
  control-plane record;
- visible object versions are dense, with no skipped committed version;
- every committed visible version was written by the configured primary for
  that version's epoch;
- committed versions meet the receipt threshold using eligible members;
- committed versions advance exactly one step from their base version;
- intents are commit-eligible only when their coordinator and epoch still match
  the current shard primary;
- normal quorum-backed operation creates no conflict records.

The first DFS model intentionally treats control-plane primary changes as an
abstract committed KV fact. It does not re-open Raft, membership, or networking.
Stale intents may remain after a primary handoff, but `commitVisibleVersion`
requires current primary and epoch, so stale intents must be retried or aborted
rather than becoming visible writes.

The `dfs_authority_routing.safety` invariant combines these properties:

- the current routed metadata-shard epoch records the configured primary from
  the abstract KV control-plane record;
- completed routed operations were served by the primary configured for the
  recorded completion epoch;
- the local namespace lookup shortcut is only used when the requester is the
  current configured primary for that epoch;
- pending redirect counters stay inside the Rust limit
  `DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT`;
- terminal completed or failed routes leave no pending routed state behind;
- a route cannot be both completed and failed;
- self-routes stay disabled;
- late or mismatched responses are observations only.

This maps to `NetworkedDfsService::handle_distributed_write_response`,
`route_namespace_lookup_path_to_metadata_primary`, and the
`sifs_routed_*_authority_redirect` integration tests. The model does not prove
progress: a route may time out, hit the redirect limit, or fail because the
redirected primary is unreachable.

The `dfs_namespace_transaction.safety` invariant combines these properties:

- materialized visible-version rows require matching object rows;
- materialized object rows from first publication require matching visible
  version rows;
- materialized object/version rows come from a recorded namespace transaction;
- a recorded transaction may be unmaterialized after an interrupted apply, but
  it may not be partially materialized with only the object row or only the
  version row;
- first-publication transactions are unique per object;
- recovery/replay observations refer only to recorded transactions.

This maps to `NetworkedDfsService::commit_visible_version_in_metadata_backend`
for the `backend.get_object(...).is_none()` path, the
`DfsNamespaceTransaction { objects, versions }` record shape, and the namespace
transaction table planners used by normal apply and recovery. It intentionally
models a single shard-local transaction; the sharded backend's routing of
objects and versions to per-shard transactions is a separate layer.

The `dfs_sharded_namespace_transaction.safety` invariant combines these
properties:

- service-level affected-shard routing for the modeled `/services/readme.md`
  move targets the services shard when a control plane is configured, and
  returns no shards when the transaction has no objects or no control plane;
- old-source cleanup is derived from the existing object-shard index, not from
  the target transaction's affected-route shard set;
- target rows require a recorded target shard transaction;
- an object-shard index can point at the target shard only after the target row
  exists, and once written it does not move back to the source shard;
- source tombstones are written only after the target row and target index are
  durable;
- a failed target-shard transaction preserves the source path and source shard
  index;
- if the target transaction succeeds but the object-shard index write fails, the
  target row is durable but hidden and the old source path remains the visible
  object;
- if source tombstone cleanup fails after the target index is written, object-id
  and target-path lookups use the target shard and the stale source row is not
  visible;
- idempotent target replay does not create a second visible path.

This maps to `LocalShardedMetadataBackend::apply_namespace_transaction`,
`write_transaction_object_shard_indexes`, `put_cross_shard_tombstone`,
`object_is_visible_from_shard`, and `get_object_by_path`, plus the regression
tests `sharded_backend_keeps_source_namespace_when_cross_shard_target_transaction_fails`
and `sharded_backend_hides_stale_source_path_when_cross_shard_tombstone_fails`.
The model deliberately tracks an implementation nuance that is not covered by
the current Rust tests: if the target shard transaction succeeds but the
object-shard index write fails, the API reports failure and sharded lookup still
resolves to the old source object while the target row remains hidden until an
explicit retry writes the index. This is not modeled as a safety violation, but
it is a recovery/operability gap to revisit if we want automatic convergence
after router-local index write failures.

The `dfs_metadata_journal_source.safety` invariant combines these properties:

- accepted and rejected observations match the implementation predicate for the
  modeled validation surface;
- service-level journal/snapshot handlers reject claimed-source/transport-source
  mismatches;
- standalone journal and snapshot processors accept only current-primary sources
  when replica context is present;
- service-level handlers with control-plane shard config accept only
  current-primary transport sources;
- the standalone processor requires the local node to be a configured replica;
- a configured primary does not apply its own replica feed;
- missing source ids are rejected on strict standalone-context paths and service
  snapshot install.

This maps to `validate_dfs_metadata_journal_batch_source`,
`validate_dfs_metadata_snapshot_install_source`,
`NetworkedDfsService::handle_metadata_journal_batch`, and
`NetworkedDfsService::handle_metadata_snapshot_install`. The model deliberately
captures the implementation distinction that service-level routed catch-up may
accept primary-sourced data for non-replica mirrors, while the standalone
replica processor requires configured replica membership. It also preserves the
implementation's no-context/no-control-plane escape hatches instead of treating
them as proof of configured cluster safety.

The `dfs_repair_authorization.safety` invariant combines these properties:

- the observed authorized/rejected decision matches the modeled
  `ensure_replica_command_authorized_with_metadata` predicate;
- a locally known replicated object can accept repair from a current replica
  after metadata-primary movement, even when the requester is not the current
  metadata primary;
- an unknown command-supplied object from a non-primary metadata source is
  rejected when no durable intent authorizes it;
- stored-intent authorization requires the local receiver to be a desired
  replica and requires either intent coordinator authority or metadata-primary
  authority with supplied object metadata;
- a metadata-primary proof can authorize command-supplied object metadata when
  there is no stored intent;
- known-object owner and current-replica fallbacks remain available only outside
  the stored-intent branch;
- missing local object metadata and missing command metadata is rejected.

This maps to `NetworkedDfsService::ensure_replica_command_authorized_with_metadata`
and the regression tests `repair_command_accepts_current_replica_after_metadata_primary_moves`,
`repair_command_accepts_metadata_primary_for_stored_intent`,
`repair_command_accepts_newer_authority_shard_when_receiver_view_is_stale`, and
`repair_command_rejects_unknown_object_from_non_primary_metadata_source`. It
does not model the later content-hash check, conflicting pending-write check, or
lost-authority retry suppression after a repair response.

The `dfs_replication_policy.safety` invariant combines these properties:

- active write policies are valid: `W > 0`, `N > 0`, `W <= N`, and
  replicated-confirmed writes require `W == N`;
- durable receipts can only come from desired replicas;
- a visible commit records the durable receipt set present at commit time and
  that set meets the `W` threshold;
- after commit, repair jobs are exactly `desiredReplicas - durableReceipts`;
- repair jobs are desired replicas and are disjoint from durable receipts;
- replicated-confirmed commits have no repair backlog;
- rejected pre-commit intents never become visible and carry no repair jobs;
- replicated-eventual commits can have repair backlog only when `W < N`.

This maps to `DfsReplicationPolicy::new`, `validate_replication_plan`,
`DistributedEventualCoordinator::is_commit_ready`,
`DistributedEventualCoordinator::record_durable_receipt`,
`DistributedEventualCoordinator::commit_intent`, and
`DistributedEventualCoordinator::rebuild_repair_jobs`. It also reflects the
service-level checks in
`NetworkedDfsService::apply_committed_intent_with_repair_mode` and
`NetworkedDfsService::stage_missing_desired_replicas_for_intent`. The model does
not prove repair progress, placement ranking, owner-inclusion normalization,
transport delivery, content-hash validation, or retry timing.

The `dfs_no_conflict.safety` invariant combines these properties:

- normal operation creates no conflict records;
- normal write outcomes never surface the model's `ConflictRecorded` outcome;
- the current metadata-shard epoch records the configured primary;
- visible versions stay inside the model bound;
- committed visible versions are dense, with no skipped version;
- committed versions advance exactly one step from their base version;
- committed versions were coordinated by the primary configured for their epoch;
- an active pending intent does not duplicate an already committed version;
- a pending intent made stale by primary handoff or base-version mismatch cannot
  be commit-ready.

This maps to `DistributedEventualCoordinator::validate_new_intent`,
`NetworkedDfsService::ensure_no_backend_pending_write_intent`,
`NetworkedDfsService::commit_visible_version_in_metadata_backend`, and
`NetworkedDfsService::reconcile_stale_write_intents_bounded`. It also reflects
the architecture rule in `docs/dfs-architecture.md` that conflict records are
reserved for future split-cluster recovery. The model does not cover the future
split-recovery action, path-level POSIX conflict surfacing, or request-map
cleanup.

The `dfs_pending_request_lifecycle.safety` invariant combines these properties:

- every active request id was issued by the model's `next_request_id`
  abstraction;
- settled request ids were previously issued;
- once a request settles, it is no longer active in read, namespace,
  primary-side remote-write-response, stage, or repair maps;
- routed-write terminal result cache entries are not active requests;
- a request id has at most one active owner across the modeled maps;
- stage and repair request ids are unique by target replica;
- no response can settle the wrong request;
- discarded response ids stay within the finite request-id universe.

This maps to `NetworkedDfsService::next_request_id`,
`NetworkedDfsService::handle_networked_read_response`,
`NetworkedDfsService::handle_distributed_write_response`,
`NetworkedDfsService::handle_namespace_response`,
`NetworkedDfsService::flush_completed_pending_remote_write_responses`,
`NetworkedDfsService::ensure_stage_response_request_matches_pending`, and the
stage/repair inflight cleanup helpers in `service_repair.rs`. The model
deliberately captures the implementation nuance that completed routed writes
may remain in `pending_routed_writes` as terminal cached results for
`write_outcome`; they are not active pending requests. It does not prove
eventual progress, wall-clock timeout fairness, chunk assembly correctness, or
full transfer-window accounting.

The `dfs_repair_content_validation.safety` invariant combines these properties:

- durable receipts can only come from desired replicas;
- durable receipts must have metadata receipts for the pending content hash;
- the last accepted receipt hash must match the pending write hash;
- current-location updates are a subset of durable receipts;
- current-location updates are made only while the target version is still the
  current object version and its content hash matches the pending hash;
- repair jobs are exactly `desiredReplicas - durableReceipts`;
- wrong-hash backend receipts do not suppress repair jobs;
- in-flight stage and repair targets are desired replicas;
- while the target version remains current, its content hash is stable.

This maps to `NetworkedDfsService::put_metadata_receipt`,
`NetworkedDfsService::mark_repaired_location`,
`NetworkedDfsService::record_local_pending_write_receipt_if_desired`,
`NetworkedDfsService::handle_stage_blob_response`,
`NetworkedDfsService::ensure_stage_response_matches_pending`,
`NetworkedDfsService::handle_repair_replica_response`,
`NetworkedDfsService::ensure_repair_response_matches_pending`, and
`NetworkedDfsService::handle_fetch_blob_response_chunk`. The model includes the
implementation nuance that a restored repair response may be accepted without a
tracked in-flight request when a matching repair job still exists. It also
models stale wrong-hash metadata receipts as backend state that must not become
durable repair evidence. It does not prove chunk assembly, transfer-window
accounting, retry fairness, or that every missing replica is eventually
repaired.

The `dfs_chunked_transfer.safety` invariant combines these properties:

- received chunks stay within the declared transfer shape;
- every received chunk has a recorded payload, and recorded payloads correspond
  to received chunks;
- a durable response requires the transfer to be committed;
- durable responses carry the expected full-content hash;
- committed transfers require every chunk to have arrived;
- committed transfers require the assembled full-object hash to match the
  expected content hash;
- malformed chunk shape, bad per-chunk hash, conflicting duplicate chunk, and
  final assembled-hash mismatch never send a durable response;
- receiver resume indexes are bounded and refer to a contiguous received prefix;
- sender windows contain only valid chunk indexes;
- sender `next_chunk_index` stays in range;
- sender `complete` implies `next_chunk_index == total_chunks`;
- an unroutable stopped sender window is not complete and resumes at the first
  unsent chunk;
- every sent chunk in the last modeled sender window is before
  `next_chunk_index`.

This maps to `validate_chunked_transfer_shape`, `validate_chunk_bytes`,
`validate_chunk_hash`, `ChunkedBlobAssembler::insert_chunk`,
`LocalContentStore::commit_chunked_object_version`,
`NetworkedDfsService::send_stage_blob_window_if_routable`,
`NetworkedDfsService::send_repair_replica_window_if_routable`,
`NetworkedDfsService::handle_stage_blob_chunk`,
`NetworkedDfsService::handle_repair_replica_chunk`, and
`NetworkedDfsService::handle_fetch_blob_response_chunk`. The model deliberately
separates sender window completion from receiver commit: a sender can report a
complete window, but the receiver can only commit after its assembler has all
chunks and the final content hash matches. It does not prove chunked patch
offset/base semantics, read-memory assembly details, network delivery fairness,
or eventual retry progress.

The `dfs_repair_lost_authority.safety` invariant combines these properties:

- removing committed pending repair state clears all pending repair/fetch state;
- suppressed repair replicas cannot remain active, keep repair resume indexes,
  or become retry-due;
- persisted lost-authority attempt records stay represented as suppressed
  retry state;
- lost-authority suppression clears fetch inflight, request, resume, failed, and
  incoming fetch-transfer state;
- lost-authority repair responses suppress the observed replica;
- lost-authority maintenance suppresses the repair backlog when the local node
  is no longer authoritative or the pending repair no longer targets current
  content;
- stale repair-base rejections stay retryable and do not suppress the replica;
- stale chunk-resume rejections clear request/resume state and make the repair
  immediately retryable from chunk zero;
- transient route failures stay retryable and do not suppress the replica;
- retry-due and repair-request replicas are known repair replicas and are not
  suppressed;
- fetch request state remains internally consistent.

This maps to `repair_rejection_indicates_lost_authority`,
`repair_rejection_indicates_stale_replica_base`,
`repair_rejection_indicates_stale_chunk_resume`,
`repair_error_indicates_transient_route_failure`,
`NetworkedDfsService::handle_repair_replica_response`,
`NetworkedDfsService::retry_pending_repairs_bounded_with_attempts`,
`NetworkedDfsService::restore_repair_attempt_records`,
`NetworkedDfsService::record_repair_retry_scheduled_with_error`,
`NetworkedDfsService::record_repair_resume_reset`, and
`NetworkedDfsService::suppress_repair_retries_for_lost_authority`. The model
explicitly separates permanent lost-authority suppression from stale
base/chunk sequencing failures and transient routing errors. It does not prove
that a newly authoritative primary will eventually take over repair, nor does
it prove network fairness or cross-primary repair handoff progress.

The `dfs_anti_entropy.safety` invariant combines these properties:

- no metadata backend means the anti-entropy run scans and reports nothing;
- completed backend-backed runs scan every modeled object;
- only file and symlink objects with the local node in `current_locations` and a
  content hash are checked;
- hash-addressed blobs with the expected size are treated as healthy even when
  the object/version manifest is missing;
- issues are recorded only for checked stale local locations;
- `max_issues` caps the stored issue list but not the total issue count;
- repair reporting is tied to `repair_metadata`, and each reported repair also
  records a transient prune;
- with `repair_metadata = false`, durable metadata locations are unchanged;
- durable backend/version location pruning happens only when the local service
  is allowed to persist metadata, i.e. no control plane is configured or the
  local node is the metadata primary for the object;
- a completed repair run prunes every persistable stale local location.

This maps to `NetworkedDfsService::run_local_content_anti_entropy`,
`NetworkedDfsService::local_content_anti_entropy_issue_reason`,
`record_anti_entropy_issue`,
`NetworkedDfsService::mark_remote_read_location_missing`, and
`NetworkedDfsService::should_persist_remote_read_location_prune`. The model is
deliberately precise about an implementation nuance: `repaired_location_count`
and issue `repaired = true` mean the service ran the repair path, but durable
metadata is pruned only if the local node can write the metadata shard. It does
not prove distributed repair scheduling, network fairness, cursor persistence,
or that repeated lifecycle maintenance will eventually revisit every page.

## Running

Fast local check:

```bash
bash scripts/formal/check-quint.sh
```

Whole-system randomized smoke exploration:

```bash
bash scripts/formal/run-quint-smoke.sh
```

`check-quint.sh` runs the full-system smoke model with a lighter default of 80
steps and 500 samples. `run-quint-smoke.sh` is the deeper local explorer; it
defaults to 160 steps and 5000 samples. Override either path with
`QUINT_SYSTEM_SMOKE_RUN_MAX_STEPS` and `QUINT_SYSTEM_SMOKE_RUN_MAX_SAMPLES`.
To reproduce a found trace, pass the reported seed:

```bash
QUINT_SYSTEM_SMOKE_SEED=0x58c1421ebe0d1421 bash scripts/formal/run-quint-smoke.sh
```

Equivalent commands:

```bash
quint typecheck specs/quint/raft_kv.qnt
quint typecheck specs/quint/network.qnt
quint typecheck specs/quint/network_endpoint_selection.qnt
quint typecheck specs/quint/network_route_planning.qnt
quint typecheck specs/quint/network_delivery_retry.qnt
quint typecheck specs/quint/system_base.qnt
quint typecheck specs/quint/system_guarantees.qnt
quint typecheck specs/quint/system_smoke.qnt
quint typecheck specs/quint/membership_base.qnt
quint typecheck specs/quint/membership.qnt
quint typecheck specs/quint/membership_kv.qnt
quint typecheck specs/quint/dfs_metadata.qnt
quint typecheck specs/quint/dfs_authority_routing.qnt
quint typecheck specs/quint/dfs_namespace_transaction.qnt
quint typecheck specs/quint/dfs_sharded_namespace_transaction.qnt
quint typecheck specs/quint/dfs_metadata_journal_source.qnt
quint typecheck specs/quint/dfs_repair_authorization.qnt
quint typecheck specs/quint/dfs_replication_policy.qnt
quint typecheck specs/quint/dfs_no_conflict.qnt
quint typecheck specs/quint/dfs_pending_request_lifecycle.qnt
quint typecheck specs/quint/dfs_repair_content_validation.qnt
quint typecheck specs/quint/dfs_chunked_transfer.qnt
quint typecheck specs/quint/dfs_repair_lost_authority.qnt
quint typecheck specs/quint/dfs_anti_entropy.qnt
quint run specs/quint/raft_kv.qnt --main=raft_kv --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/network.qnt --main=network --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/network_endpoint_selection.qnt --main=network_endpoint_selection --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/network_route_planning.qnt --main=network_route_planning --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/network_delivery_retry.qnt --main=network_delivery_retry --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/system_smoke.qnt --main=system_smoke --invariant=safety --max-steps=80 --max-samples=500 --verbosity=1
quint run specs/quint/membership.qnt --main=membership --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/membership_kv.qnt --main=membership_kv --step=step --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/dfs_metadata.qnt --main=dfs_metadata --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/dfs_authority_routing.qnt --main=dfs_authority_routing --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/dfs_namespace_transaction.qnt --main=dfs_namespace_transaction --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/dfs_sharded_namespace_transaction.qnt --main=dfs_sharded_namespace_transaction --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/dfs_metadata_journal_source.qnt --main=dfs_metadata_journal_source --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/dfs_repair_authorization.qnt --main=dfs_repair_authorization --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/dfs_replication_policy.qnt --main=dfs_replication_policy --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/dfs_no_conflict.qnt --main=dfs_no_conflict --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/dfs_pending_request_lifecycle.qnt --main=dfs_pending_request_lifecycle --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/dfs_repair_content_validation.qnt --main=dfs_repair_content_validation --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/dfs_chunked_transfer.qnt --main=dfs_chunked_transfer --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/dfs_repair_lost_authority.qnt --main=dfs_repair_lost_authority --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/dfs_anti_entropy.qnt --main=dfs_anti_entropy --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint verify specs/quint/raft_kv.qnt --main=raft_kv --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/network.qnt --main=network --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/network_endpoint_selection.qnt --main=network_endpoint_selection --invariant=safety --max-steps=4 --verbosity=1
quint verify specs/quint/network_route_planning.qnt --main=network_route_planning --invariant=safety --max-steps=4 --verbosity=1
quint verify specs/quint/network_delivery_retry.qnt --main=network_delivery_retry --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/membership.qnt --main=membership --invariant=safety --max-steps=4 --verbosity=1
quint verify specs/quint/membership_kv.qnt --backend=tlc --main=membership_kv --step=stepCore --invariant=safety --max-steps=4 --verbosity=1
quint verify specs/quint/dfs_metadata.qnt --main=dfs_metadata --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/dfs_authority_routing.qnt --main=dfs_authority_routing --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/dfs_namespace_transaction.qnt --main=dfs_namespace_transaction --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/dfs_sharded_namespace_transaction.qnt --main=dfs_sharded_namespace_transaction --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/dfs_metadata_journal_source.qnt --main=dfs_metadata_journal_source --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/dfs_repair_authorization.qnt --main=dfs_repair_authorization --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/dfs_replication_policy.qnt --main=dfs_replication_policy --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/dfs_no_conflict.qnt --main=dfs_no_conflict --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/dfs_pending_request_lifecycle.qnt --main=dfs_pending_request_lifecycle --invariant=safety --max-steps=3 --verbosity=1
quint verify specs/quint/dfs_repair_content_validation.qnt --main=dfs_repair_content_validation --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/dfs_chunked_transfer.qnt --main=dfs_chunked_transfer --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/dfs_repair_lost_authority.qnt --main=dfs_repair_lost_authority --invariant=safety --max-steps=4 --verbosity=1
quint verify specs/quint/dfs_anti_entropy.qnt --main=dfs_anti_entropy --invariant=safety --max-steps=5 --verbosity=1
```

The default symbolic bound is deliberately modest. On this checkout,
the Raft, network, DFS metadata, and DFS routing specs use
`QUINT_VERIFY_MAX_STEPS`, defaulting to 5. DFS routing can also be overridden
with `QUINT_DFS_ROUTING_VERIFY_MAX_STEPS`; its 5-step symbolic check passed on
this checkout but took about 53-81 seconds in local runs. The network endpoint
selection spec uses `QUINT_NETWORK_ENDPOINT_VERIFY_MAX_STEPS`, defaulting to 4;
its 4-step symbolic check passed in about 10 seconds. The network route
planning spec uses `QUINT_NETWORK_ROUTE_VERIFY_MAX_STEPS`, defaulting to 4; its
4-step symbolic check passed in about 5 seconds. The network delivery retry spec
uses `QUINT_NETWORK_DELIVERY_VERIFY_MAX_STEPS`, defaulting to 5; its isolated
6-step symbolic check passed in about 6 seconds. The namespace
transaction spec can be overridden with `QUINT_DFS_NAMESPACE_VERIFY_MAX_STEPS`;
its 5-step symbolic check passed in about 21 seconds. The journal source spec
can be overridden with `QUINT_DFS_JOURNAL_VERIFY_MAX_STEPS`; its 5-step symbolic
check passed in about 22-27 seconds. The sharded namespace transaction spec can
be overridden with `QUINT_DFS_SHARDED_NAMESPACE_VERIFY_MAX_STEPS`; its 5-step
symbolic check passed in about 6 seconds. The repair authorization spec can be
overridden with `QUINT_DFS_REPAIR_VERIFY_MAX_STEPS`; its 5-step symbolic check
passed in about 4-8 seconds. The replication policy spec can be overridden with
`QUINT_DFS_REPLICATION_VERIFY_MAX_STEPS`; its 5-step symbolic check passed in
about 4-9 seconds. The no-conflict spec can be overridden with
`QUINT_DFS_NO_CONFLICT_VERIFY_MAX_STEPS`; its 5-step symbolic check passed in
about 6-7 seconds. The pending request lifecycle spec can be overridden with
`QUINT_DFS_PENDING_VERIFY_MAX_STEPS`; its default is 3 because the map-heavy
state space is wider than the earlier DFS models. On this checkout, Apalache
3-step verification passed in about 65-74 seconds, TLC 4-step verification passed
in about 129 seconds, and interrupted Apalache 4/5-step attempts reported Z3
`UNKNOWN`. Membership uses `QUINT_MEMBERSHIP_VERIFY_MAX_STEPS`,
defaulting to 4; a 5-step membership check passed on this checkout but took
about 50 seconds by itself. The repair content-validation spec can be overridden
with `QUINT_DFS_REPAIR_CONTENT_VERIFY_MAX_STEPS`; its 5-step symbolic check
passed in about 30 seconds. The chunked-transfer spec can be overridden with
`QUINT_DFS_CHUNKED_VERIFY_MAX_STEPS`; its 5-step symbolic check passed in about
8 seconds. The repair lost-authority spec can be overridden with
`QUINT_DFS_REPAIR_LOST_AUTHORITY_VERIFY_MAX_STEPS`; its default is 4 because
the 5-step symbolic check passed but took about 135 seconds. The anti-entropy
spec can be overridden with `QUINT_DFS_ANTI_ENTROPY_VERIFY_MAX_STEPS`; its
5-step symbolic check passed in about 8 seconds. The membership/KV bridge uses
`QUINT_MEMBERSHIP_KV_VERIFY_MAX_STEPS`, defaulting to 4, and runs with TLC over
`stepCore`. The full-system smoke model is intentionally randomized in the
normal script path instead of symbolically verified as a routine gate; a
5-step symbolic smoke check has passed locally, but the useful signal is the
broader random exploration and seed-based replay.

Use larger bounds for local audit runs, not as a routine CI gate:

```bash
QUINT_VERIFY_MAX_STEPS=8 QUINT_NETWORK_ENDPOINT_VERIFY_MAX_STEPS=5 QUINT_NETWORK_ROUTE_VERIFY_MAX_STEPS=5 QUINT_NETWORK_DELIVERY_VERIFY_MAX_STEPS=6 QUINT_SYSTEM_SMOKE_RUN_MAX_STEPS=240 QUINT_SYSTEM_SMOKE_RUN_MAX_SAMPLES=10000 QUINT_DFS_ROUTING_VERIFY_MAX_STEPS=6 QUINT_DFS_NAMESPACE_VERIFY_MAX_STEPS=6 QUINT_DFS_SHARDED_NAMESPACE_VERIFY_MAX_STEPS=6 QUINT_DFS_JOURNAL_VERIFY_MAX_STEPS=6 QUINT_DFS_REPAIR_VERIFY_MAX_STEPS=6 QUINT_DFS_REPLICATION_VERIFY_MAX_STEPS=6 QUINT_DFS_NO_CONFLICT_VERIFY_MAX_STEPS=6 QUINT_DFS_PENDING_VERIFY_MAX_STEPS=4 QUINT_DFS_REPAIR_CONTENT_VERIFY_MAX_STEPS=6 QUINT_DFS_CHUNKED_VERIFY_MAX_STEPS=6 QUINT_DFS_REPAIR_LOST_AUTHORITY_VERIFY_MAX_STEPS=5 QUINT_DFS_ANTI_ENTROPY_VERIFY_MAX_STEPS=6 QUINT_MEMBERSHIP_VERIFY_MAX_STEPS=5 QUINT_MEMBERSHIP_KV_VERIFY_MAX_STEPS=5 bash scripts/formal/check-quint.sh
```

## Current Abstractions

This slice does not yet model:

- per-node durable hard-state fields such as `current_term`, `voted_for`, and
  `leader_id`;
- rejected append entries, follower log truncation, or concrete Raft RPCs over
  the network abstraction;
- WAL and snapshot restore;
- watches, leases, compare-and-swap transactions, and revision histories;
- concrete invite cryptography, certificate validation, and role-policy checks;
- concrete Raft config-change RPCs over the network abstraction;
- cluster leave and migration quarantine/import behavior;
- full-system smoke internals beyond the current abstract integration surface:
  real directory snapshots, concrete route-cache generations, raw transport
  frames, stream payloads, full KV log replay, and concrete DFS object-store
  contents;
- route-table cache invalidation, live `ClusterDirectory` read failures during
  an in-flight delivery, raw streamed response retry budgets, concrete transport
  nonce generation, and dynamic module enable/disable races;
- directory fragments, local-owned owner routing, owner-inclusion replica-set
  normalization, cross-primary repair handoff progress, router-local recovery
  after sharded namespace index write failures, chunked patch offset/base
  semantics, read-memory assembly details, distributed anti-entropy repair
  scheduling progress, wall-clock timeout fairness, and future split-cluster
  conflict recovery.

Those should be added as layered modules rather than folded into one large
state machine. The explicit DFS backlog in the pasted note is now covered by
focused safety slices; the next useful step is to choose one abstraction above
and deepen it, likely distributed anti-entropy repair scheduling under fairness
assumptions, router-local sharded namespace recovery, or patch assembly
semantics.

## Suggested Next Network Slices

1. **Route-cache invalidation.** Model `ClusterDeliveryRoutingTableCacheMark`,
   directory-version changes, local connectivity file marks, and module dialer
   snapshots. Prove that stale cached route plans are not reused after the
   inputs that affect route validity change.
2. **Purpose-specific endpoint selectors.** Refine `network_endpoint_selection`
   for rollout, Raft replication, member status, and data-plane selectors. Prove
   each purpose's special ranking still preserves the shared safety rules.
3. **Stream and raw-response retry budgets.** Extend `network_delivery_retry`
   with streamed routes and raw-response capability flags. Prove that fallback
   attempts cannot silently expand the caller's deadline or send raw streams to
   transports that cannot carry them.

## Suggested Next System Slices

1. **Projection contracts from focused specs.** Add small projection modules
   that map proven facts from `membership`, `network_endpoint_selection`,
   `network_route_planning`, `network_delivery_retry`, and the DFS authority
   specs into the names consumed by `system_guarantees`. This keeps the shared
   foundation model full while making the assume/guarantee boundary explicit.
2. **Directory and route-cache race model.** Extend `system_base` with
   directory snapshot versions, cache marks, and in-flight route plans. Prove
   that membership removals, identity mismatch feedback, and endpoint
   generation changes invalidate active or cached routes before new work is
   authorized.
3. **Bug-trace harnesses.** Encode the known fragile Rust networking scenarios
   as constrained smoke modules over `system_base`, then run both random
   exploration and fixed-seed replay. This gives us small, durable regression
   specs without duplicating the lower-layer algorithms.

## Suggested Next DFS Slices

The next verification work should stay layered: each new module should consume
the current DFS safety facts as assumptions instead of re-modeling routing,
request ownership, content validation, and lost-authority suppression from
scratch.

1. **Distributed anti-entropy repair progress under fairness.** Build on
   `dfs_anti_entropy`, `dfs_replication_policy`, and
   `dfs_repair_lost_authority` to model repair-job scheduling after stale
   location pruning. Prove that missing desired replicas eventually either gain
   durable receipts or remain explicitly suppressed for lost authority.
2. **Router-local sharded namespace recovery.** Build on
   `dfs_sharded_namespace_transaction` and add the failure window where the
   target shard transaction is durable but `write_transaction_object_shard_indexes`
   fails. Decide whether automatic reopen/recovery should write the missing
   index and source tombstone, or whether explicit retry is the intended
   recovery contract.
3. **Chunked patch offset/base semantics.** Build on `dfs_chunked_transfer` and
   model the patch-specific commit path:
   `commit_chunked_object_version_patch_with_*` plus the expected base identity.
   Prove that a patch response cannot commit against the wrong base version,
   content hash, offset, or final object hash.
4. **Read-memory and range-read assembly.** Model `pending_reads`,
   `incoming_read_memory_transfers`, stale-source rejection, source retry, and
   range chunk assembly. Prove that a completed read returns bytes matching the
   current object metadata, and that stale or mismatched read responses cannot
   populate local metadata.
5. **Replica placement and owner normalization.** Model
   `plan_distributed_eventual_replicas`, `normalize_desired_replica_nodes`, and
   `validate_replication_plan`. Prove that duplicate candidates, missing owner
   candidates, and policy widening still preserve the N/W contract consumed by
   `dfs_replication_policy`.
6. **Cross-primary repair handoff progress.** Use
   `dfs_repair_lost_authority` as the old-primary side of the proof, then add a
   new-primary model that can rediscover committed pending work and schedule
   repair. Prove quiescence on the old primary plus eventual scheduling on the
   new primary under fairness.
7. **POSIX multi-object namespace operations.** Model rename/unlink/mkdir sagas,
   directory locks, by-id resolution, tombstones, and rollback on aborted moves.
   Prove that failed operations release locks and leave the live namespace
   internally consistent.
