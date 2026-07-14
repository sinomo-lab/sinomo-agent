# Cluster Formal Verification

This reference explains Sinomo's executable Quint specifications and how to run
them. The models cover bounded slices of KV/Raft, adversarial networking,
membership, integrated system guarantees, and DFS metadata and replication.

Each model isolates one safety boundary and exposes guarantees to higher layers.
The goal is compositional evidence for the control plane, not one exhaustive
state machine that duplicates the Rust implementation. Future slices are
tracked in the [formal verification roadmap](../plans/formal-verification-roadmap.md).

## Current Specs

The Quint tree is organized by role instead of a flat list of executable
modules:

- `specs/quint/lib/` contains shared finite vocabulary such as nodes, quorum
  helpers, `Option`, membership roles, and membership projection helpers.
- `specs/quint/models/` contains algorithm transition systems. Model modules
  own state variables, helper projections, `init`, `step`, and domain actions.
  They intentionally do not define the executable `safety` invariant.
- `specs/quint/contracts/` contains reusable guarantee predicates consumed by
  higher layers. This includes timed delivery predicates and the full-system
  integration guarantees over the shared system model.
- `specs/quint/checks/` contains invariant suites. Each check imports one model
  plus the shared vocabulary it needs, defines the invariant predicates, and
  exposes the `safety` entrypoint used by `quint run` and `quint verify`.
- `specs/quint/scenarios/` contains composed exploration entrypoints and
  deterministic causal regressions. Randomized scenarios can import several
  models or contracts; deterministic scenarios are also symbolically checked
  at the full depth of their prescribed trace when registered by the harness.

The model inventory is split by subsystem:

- `models/kv/raft_kv.qnt` models the compact Raft/KV committed-prefix slice:
  leader election, node online/offline transitions, catch-up, leader proposals,
  quorum commit, and KV-state replay. `models/kv/raft_protocol.qnt` complements
  it with bounded per-node term, vote, log, replication, commit, and durable
  restart state. Their invariant entrypoints live under `checks/kv/`.
- `models/network/` contains the base network mechanism, endpoint selection,
  route planning, routing-table cache invalidation, delivery retry, endpoint
  attempt-budget, timed transport, and timed delivery-retry models. Matching
  invariant entrypoints live under `checks/network/`, and
  `scenarios/network/timed_delivery_smoke.qnt` composes
  the timed transport and retry layers for randomized exploration.
- `models/membership/` contains standalone membership authority, the
  membership/KV bridge over the abstract committed-prefix contract, and a
  focused Raft configuration-transition slice for promotion, safe voter-set
  shrink, quorum acknowledgements, and health/config separation. Matching
  invariant entrypoints live under `checks/membership/`.
- `models/system/base.qnt` defines the shared integration vocabulary for
  membership, endpoint identity, connectivity observations, route selection,
  delivery keys, DFS metadata authority, and visible DFS writes.
  `contracts/system/guarantees.qnt` defines the layered guarantee predicates,
  and `scenarios/system/smoke.qnt` composes them for randomized whole-system
  exploration.
- `models/dfs/` contains the DFS slices: metadata authority, ordinary canonical
  write eligibility across partitions and shard reconfiguration, authority routing,
  read freshness, routed namespace freshness, POSIX namespace/link/cross-shard
  behavior, whole-file advisory locks, file and directory sync barriers,
  lock-holding operation recovery across partitions, opt-in local-first file
  branches and same-shard namespace reconciliation, a local directory
  durability companion, a typed one-shard reconciliation publication gateway,
  direct file/namespace terminal-action compositions with that gateway, local
  failover-target selection,
  namespace transaction publication, sharded namespace routing,
  namespace-command replay and cancellation, metadata journal source
  validation, repair authorization, replication policy, no-conflict
  serialization, pending request lifecycle, repair content validation,
  lost-authority repair suppression, and local content anti-entropy.
  Matching invariant entrypoints live under `checks/dfs/`; deterministic
  advisory-lock fencing, file-sync recovery, directory-generation barriers,
  complete-holder/quorum-repair partitions, local-first reconciliation,
  local directory-durability recovery, reconciliation-gateway crash/fencing,
  composed branch/gateway crash and outcome traces, and retry-exhaustion traces
  live under `scenarios/dfs/`.

This maps to the repository boundary documented in
`docs/clustering-architecture.md` and `docs/network-architecture.md`:
`sinomo-kv` owns KV/Raft semantics and stays transport-free, `sinomo-network`
owns transport and routing mechanism without depending on cluster policy, and
`sinomo-cluster` bridges them by moving consensus and membership traffic over
network routes backed by authoritative cluster state.

### Evidence status

Every model must be read as one of three evidence classes:

- **Current-behavior refinement** mirrors a production decision surface and
  should have table- or trace-driven Rust conformance tests.
- **Abstract contract** deliberately hides lower-layer mechanics and proves
  only the named boundary. For example, `raft_kv` is a committed-prefix/KV
  refinement, not a proof of concrete RequestVote, AppendEntries, WAL, or
  snapshot behavior.
- **Target hardening** states behavior that the implementation does not yet
  enforce. The catch-up certificate required by the membership promotion model
  is currently in this class.

Passing a bounded check proves none of the omitted mechanics. New and existing
models should say which class they belong to, and target-hardening checks must
not be described as current implementation conformance until the corresponding
Rust gate exists.

## What The Models Check

The specs are layered. A higher-level model should consume the smallest
interface it needs from lower layers rather than re-verifying their internals.
For example, `membership_kv` assumes the `raft_kv` guarantee of a canonical
committed KV prefix and proves membership replay/config projection on top of
that prefix. A later DFS authority model should consume committed membership and
KV facts, not Raft election state or raw network buffers.

The `raft_kv_safety.safety` invariant combines these properties:

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

The `raft_protocol_safety.safety` invariant checks the deeper three-voter
protocol slice:

- each voter grants at most one candidate per term and candidates with stale
  logs are rejected;
- a recorded leader has a quorum and there is at most one leader per term;
- equal entry terms at an index imply matching log prefixes;
- AppendEntries prev-log mismatch backs up `nextIndex`, while an accepted
  conflicting entry truncates the follower's uncommitted suffix;
- commit requires quorum acknowledgements for a current-term entry, and every
  per-node committed prefix agrees with the canonical committed prefix;
- a later-term leader contains every earlier committed entry;
- each applied state machine equals replay of its committed local prefix;
- higher-term observations force candidate/leader stepdown; and
- crash/restart preserves durable term, vote, and log state.

This maps to `RaftRequestVoteRequest`, `RaftAppendEntriesRequest`,
`ClusterKvRaftStore::request_vote`, the AppendEntries preparation/application
path, `build_append_entries_request_from_next_index`, quorum replication and
commit helpers, and the persisted hard-state/log load path in `sinomo-kv`.
Three documented reachable protocol cuts expose divergent suffix repair and
leader-change safety without requiring every setup transition in the same
trace. All ten branch witnesses are exercised by deterministic randomized
coverage. This is a finite current-behavior refinement over abstract transport
and persistence actions, not a proof of the byte-level WAL or network RPC
implementation.

The `network_safety.safety` invariant combines these properties:

- every delivered message was explicitly sent;
- every in-flight message was explicitly sent;
- network activity cannot grant or change cluster trust;
- discovery is only an observation about node identifiers;
- self-links stay disabled.

The important design choice is that `deliver` does not remove a message from
`inFlight`. That over-approximates duplicate delivery and retries. `drop` is the
separate adversarial action that removes a message. Reordering is modeled by
nondeterministically choosing any in-flight message for delivery or drop.

The `network_endpoint_selection_safety.safety` invariant combines these properties:

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

The `network_route_planning_safety.safety` invariant combines these properties:

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

The `network_route_cache_safety.safety` invariant refines the production
routing-table cache boundary:

- reuse requires equality of the cluster-state mark, committed directory
  version, local-connectivity file mark and content generation, and enabled
  module-dialer snapshot;
- an unknown directory version disables both reuse and publication;
- the cached table's cluster and local-node identities must still match the
  caller;
- a table is not reused at or after its earliest connectivity expiry;
- committed membership removal changes the directory mark before an old target
  plan can be reused;
- endpoint identity mismatch eagerly invalidates the cache; and
- a table built while its before/after validity mark changes is discarded.

This maps to `ClusterDeliveryRoutingTableCacheMark`,
`cluster_delivery_route_plan_from_cached_routing_table`,
`cluster_delivery_routing_table_cache_mark`, the before/after build check in
`cluster_delivery_route_plan_from_routing_table_async`, and
`record_cluster_delivery_endpoint_identity_mismatch`. Named witnesses keep each
mark input, identity check, expiry path, membership removal, and build race
reachable. The slice proves safe reuse, not cache hit rate, eviction policy, or
fresh-route construction after a miss.

The `network_delivery_retry_safety.safety` invariant combines these properties:

- a delivered response requires a successful endpoint attempt;
- primary and fallback attempts preserve the caller's stable operation identity;
- each endpoint attempt uses a fresh authentication nonce;
- retry may dispatch the same domain operation more than once, but the number of
  dispatches remains bounded by the endpoint attempts;
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
does not claim generic network-level exactly-once delivery: generic object RPC
may duplicate a side-effecting request. Domain protocols such as DFS and KV must
enforce idempotency with the stable operation identity, while every concrete
endpoint attempt receives fresh transport authentication.

The `network_timed_transport_safety.safety` invariant combines these
properties:

- logical transport time stays within the finite model bound;
- in-flight, dropped, and delivered packet copies all refer to sent copies;
- sent copies have bounded send, deliverable, and expiry times;
- delivery cannot occur before send time, before the deliverable time, or after
  packet expiry;
- delivery records that source, target, and link were live at the delivery
  step;
- dropped copies are no longer in flight;
- if the original packet is dropped before any delivery and no duplicate was
  created before that drop, no later copy can produce delivery;
- duplicate copies cannot exist before the original and preserve the original
  packet expiry.

The `network_timed_delivery_retry_safety.safety` invariant combines these
properties:

- request time and global deadline stay inside the bounded logical clock;
- the request deadline is derived from the request start rather than model
  initialization;
- endpoint attempts require a prepared stable delivery key;
- each response due time is derived from that endpoint's attempt start;
- per-attempt timeout and accepted-attempt timeout never extend the global
  deadline;
- primary and fallback attempts reuse the original stable delivery key while
  using distinct transport attempt nonces;
- accepted responses arrive before both the global deadline and the current
  attempt timeout;
- accepted responses were due, came from an attempted endpoint, and came from an
  endpoint configured to respond;
- late responses do not dispatch the request;
- terminal `TimedDeadlineExpired` requires the logical clock to have reached the
  global deadline;
- `TimedAllEndpointsFailed` cannot skip a usable fallback while time remains;
- failed, timed-out, and suppressed endpoints were attempted;
- identity-mismatch endpoint failures suppress the endpoint;
- observed duplicate responses refer to an attempted endpoint without creating
  a generic exactly-once guarantee.

These timed models are still finite and safety-oriented. They model bounded
logical time, packet loss, delay, duplication, per-attempt timeout, late
responses, and fallback behavior. The timed smoke scenario couples attempt
start to packet creation, advances both clocks together, and requires packet
delivery before a response decision. It does not yet prove unbounded real-time
fairness or scheduler progress.

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

The `membership_safety.safety` invariant combines these properties:

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

The `membership_kv_safety.safety` invariant combines these properties:

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

The `raft_config_transition_safety.safety` invariant isolates the production
Raft configuration validator from the broader membership lifecycle:

- adding a voter requires that node to be a committed learner or non-voting
  member, and at most one voter may be added per transition;
- removing voters requires the retained set to satisfy both the old and new
  quorum sizes, rejecting the unsafe two-voter-to-one-voter transition;
- committing a valid proposal still requires acknowledgements from a quorum of
  the current configuration;
- health observations do not mutate the committed Raft configuration;
- focused initial cases cover learner and non-voting promotion, valid
  three-to-two shrink, invalid direct/multiple additions, and insufficient
  acknowledgement.

This maps to `validate_cluster_kv_raft_config_transition` and separates a
current-behavior configuration proof from the target catch-up certificate in
the broader membership model. The Rust table test
`cluster_kv_raft_config_transition_matches_bounded_model_cases` covers the same
learner/non-voting promotion, direct and multiple addition rejection, and
three-to-two versus two-to-one shrink cases.

The `dfs_metadata_safety.safety` invariant combines these properties:

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

The `dfs_canonical_write_eligibility_safety.safety` invariant refines the
ordinary, non-local-first canonical write gate:

- content durability is a DFS concern: a publish needs `W` exact-operation
  receipts from desired content replicas, not a per-file KV entry;
- the pending intent must still match the current metadata primary, fenced
  epoch, exact shard-assignment/config revisions, base, and next target;
- the ordinary fast path publishes after every replica in the current metadata
  shard assignment durably applies the hidden DFS prepare and returns a fresh
  acknowledgement for that exact operation; the production prepare contains
  intent/receipt metadata and any namespace transaction, never visible file
  metadata in KV;
- the exceptional partition path may instead use a permit bound to the same
  operation and current control-plane revisions, issued by a quorum of the
  committed cluster voter configuration;
- a component with `W` content receipts but neither complete fresh shard
  acknowledgements nor a matching quorum permit cannot publish;
- acknowledgements or permits from a stale operation, shard assignment, epoch,
  or cluster configuration cannot satisfy the gate; and
- a quorum-side membership update retains the current primary and does not
  activate a replacement writer. It first records a bounded pending handoff;
  that control-plane commit is not itself a retiring-primary write fence;
- after the retained primary observes the pending handoff, it acquires the
  same publication guard as canonical commits, installs a durable shard-local
  backend write freeze, and acknowledges the exact frozen journal head and
  fingerprint. The freeze survives restart and rejects all authoritative
  metadata-table mutations while still allowing replay and handoff transfer;
- activation copies that exact fenced prefix to the union of the retiring and
  desired metadata assignments, requires exact acknowledgements from every
  member of that union, and accepts only a completion proof equal to the
  immutable retiring-primary fence. This carries writes which finished after
  the quorum-side pending decision but before the retiring primary observed it;
- only after the union proof succeeds does the desired assignment become the
  active writer. The durable local marker advances to the activated epoch;
  only an instance rebound after observing that exact generation may write,
  so independently opened stale instances remain fenced. A proof-carrying
  primary promotion additionally requires the promoted replica's exact-state
  watermark through the retiring-primary fence; and
- visible versions form one dense history and canonical operation creates no
  conflict records.

This is a current-behavior refinement of the canonical publication decision,
not a model of storing file metadata in KV. Cluster KV supplies only committed
DFS configuration facts and the quorum authority fallback used when the exact
metadata shard cannot all acknowledge the operation. The bounded partition
cases deliberately set `W=1`, below cluster quorum, so the unsafe minority-side
case remains reachable. POSIX fsync/lock and all local-first models are kept
unchanged and outside this slice.

The `dfs_authority_routing_safety.safety` invariant combines these properties:

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

The `dfs_namespace_transaction_safety.safety` invariant combines these properties:

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

The `dfs_sharded_namespace_transaction_safety.safety` invariant combines these
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

The `dfs_metadata_journal_source_safety.safety` invariant combines these properties:

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

The `dfs_repair_authorization_safety.safety` invariant combines these properties:

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

The `dfs_replication_policy_safety.safety` invariant combines these properties:

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

The `dfs_no_conflict_safety.safety` invariant combines these properties:

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

The `dfs_posix_advisory_locks_safety.safety` invariant is a target-hardening
contract for a bounded whole-file advisory-lock manager:

- any number of shared locks may coexist, while an exclusive lock conflicts
  with every other session's lock;
- nonblocking conflict returns without enqueuing, while a blocking conflict
  records one bounded waiter;
- every grant token names the session, unique grant id, and current metadata
  authority epoch;
- unlock requires the exact current token, so a stale token cannot release a
  later same-session or new-epoch lock;
- close, session crash, finite lease expiry, and authority-epoch advance remove
  the affected grant and waiter;
- healthy sessions renew the same token on logical ticks, while only a
  session-local renewal outage counts its lease down toward expiry;
- lease expiry and epoch fencing record explicit lost-lock outcomes for
  holders, while an epoch change settles every blocking request as fenced
  before clearing the wait table;
- each session has at most one outstanding request, and starting its next
  request consumes the prior terminal response so later fencing cannot reuse
  stale settlement evidence;
- once cleanup makes the waiter grantable, the next model transition promotes
  it, preventing an obsolete grant from becoming a protocol deadlock.

These are advisory rather than mandatory locks. The slice assumes cooperating
clients and weakly fair logical-time ticks after renewal becomes unavailable.
It does not yet model concrete wall-clock drift/renewal RPC timing, byte ranges,
multiple objects, or arbitrary application-created wait cycles. The production FUSE
adapter now refines the bounded whole-file surface with distributed `getlk` and
`setlk`, asynchronous blocking waiters, exact tokens, close/cancel cleanup,
lease renewal, persisted FIFO state, metadata-quorum acknowledgement, and an
epoch warm-up fence. It deliberately rejects partial ranges and reports a
conflicting blocking lock conversion as `EDEADLK`; those restrictions keep it a
bounded refinement rather than full POSIX record-lock conformance.

The concrete lease mapping states its additional timing assumptions explicitly:
clients convert an authority-reported remaining duration into a monotonic local
deadline, authority changes add a five-minute maximum clock-skew fence, mounted
coordination RPCs are bounded to ten seconds, and a mount accepts at most two
distinct live or pending lock keys. Exceeding the mount bound returns `ENOLCK`;
exceeding the clock-skew assumption invalidates the refinement claim.

The `dfs_posix_fsync_safety.safety` invariant separates returned, visible,
receipt-durable, metadata-certified, and canonical file prefixes:

- `fsync` and `fdatasync` freeze the returned-write prefix at call time, so a
  later write is not accidentally covered by an earlier barrier;
- successful file sync requires the configured content-receipt threshold and a
  failover-recoverable metadata certificate for that frozen prefix;
- `fdatasync` covers data plus essential retrieval metadata but makes no claim
  about unrelated file metadata;
- full file `fsync` also covers file-integrity metadata, but neither file call
  claims parent-directory durability;
- directory `fsync` freezes and certifies the captured create/rename/unlink
  generation without claiming file-data durability;
- a reply lost after commit produces an outcome-unknown observation, while an
  exact same-id retry returns the durable result;
- the same sync id with a different immutable payload is rejected;
- an authority fence terminates an uncommitted sync and exposes only the
  previously certified prefix; and
- committed syncs survive authority change and repair copies only certified
  canonical data.

The bounded receipt policy is two of three nodes. Concrete device-specific
barriers, torn writes, and open-file-description behavior across rename/unlink
are outside this slice. The full-depth recovery regression
uses data holders N2/N3 and metadata prepare voters N1/N2 and asserts their N2
intersection explicitly. Rust now freezes exact file and directory targets,
validates the desired content receipts, and obtains fresh proofs of the
complete canonical materialized checkpoint at the exact journal head rather
than reusing stored replication cursors. It publishes an epoch-bound quorum
head before `fsync`, `fdatasync`, or directory `fsync` returns. Each probe uses
a restart-unique identity and an empty rolling-upgrade-safe journal batch. FUSE
`flush` and `release` remain visibility/close operations. `fdatasync` has a
distinct API kind but currently uses the stronger full-file physical metadata
barrier. Durable caller-selected sync ids and stored outcome-unknown responses
remain target hardening. The Rust checkpoint proof currently hashes the full
materialized shard state; an incremental authenticated checkpoint is a future
performance refinement, not a weaker fallback.

The `dfs_posix_partitioned_operation_safety.safety` invariant abstracts the
combined authority, lock, and sync contract around the requested partition
case; it is not a direct state-machine composition of the three focused models:

- a lock holder may commit during a partition only when its component has
  every touched current shard, the required validated content, and an
  unexpired strong permit, and the combined model has consumed an abstract
  lower-sync proof token for receipt and prepare-quorum prerequisites; the
  focused sync model separately verifies their bounded recovery-set
  intersection, and the commit transition atomically forms the
  failover-recoverable certificate;
- complete shard coverage alone cannot authorize a commit;
- a holder missing state or content suspends without changing the canonical
  version;
- a quorum component activates a higher epoch only after the old permit's
  fencing barrier and atomically invalidates the old lock;
- authority selection does not depend on the winner's content-shard count;
- a winner missing current bytes remains `PendingRepair` and cannot obtain the
  lock until a canonical checkpoint is validated;
- a stale holder token cannot unlock the quorum side's new-epoch lock; and
- both the complete-holder path and quorum-repair path end with no effective
  lock, retained stale token, pending repair, or waiter. Quorum commit does not
  wait for the offline client, but after that client rejoins, fence observation
  precedes terminal stutter.

`scenarios/dfs/posix_lock_partition_recovery.qnt` executes both outcomes as
deterministic regressions. The complete holder syncs before expiry, releases,
rejoins, and lets the other client finish. The incomplete holder suspends, is
fenced after two ticks, and the quorum repairs and commits while the old client
still physically retains an ineffective token. After rejoin, that client
observes the fence and discards the token. These scenarios and the invariant
that every nonterminal cut has a recovery class are bounded deadlock evidence,
not an unbounded liveness proof.
Eventual progress still assumes time advancement, stable quorum, a validated
committed-content source, and fair recovery scheduling.
Read serving is outside this focused slice; the prohibition on serving stale
bytes while repair is pending remains an assumed read-freshness contract rather
than a newly verified property here.

The local-first DFS extension is modeled beside this canonical partition
machine rather than adding provisional versions to it. The shared
`contracts/dfs/local_first_projection.qnt` contract projects an extended state
to authority epoch, journal head, canonical object and namespace revisions,
visible-authority/head counts, and the canonical conflict count. Local overlay,
local durability, planning, and purely local sidecar steps must stutter under
that projection. The focused file and namespace slices retain an
assume/guarantee interface: a strong or reconciliation publication consumes
seven compact facts for quorum, current authority, epoch, exact plan head,
content policy, metadata prepare, and atomic publication.

`contracts/dfs/local_first_reconcile_permit.qnt` now supplies a typed
refinement of those facts. Quorum, authority, content, and prepare certificates
must agree on the payload, configuration revision, authority epoch,
predecessor head, and successor journal index. The atomic publication envelope
must agree with the plan's canonical delta, result digest, and optional
conflict descriptor. `canonicalEvidenceFromPermit` is the sole adapter back to
the seven-field projection contract; none of its fields is asserted directly.
The standalone local-first branch models retain that assume/guarantee surface.
The file and namespace composition modules now replace it at their terminal
boundary: every branch action is paired with exactly one gateway action, and a
branch terminal result is created only in the same transition as the matching
gateway journal commit. The compositions are not yet connected to the
canonical advisory-lock, `fsync`, partition-recovery, no-conflict, or Raft
transition systems. Given either the standalone assumed evidence or the
composed derived permit, a publication advances one authority journal record
while preserving one visible authority, one visible head, and zero canonical
conflicts.

The `dfs_local_first_file_branch_safety.safety` invariant covers a bounded
regular-file branch:

- degraded branch creation requires the modeled opt-in policy flag, which is
  assumed to have come from previously certified strong metadata, and may occur
  while a quorum continues elsewhere but the local authority route is
  unavailable;
- global quorum loss/restoration is independent from local route reachability;
  strong publication stops without quorum, while arbitrary route return seals a
  synced dirty branch or permanently freezes an unsynced/clean open branch so a
  later route flap cannot re-enable degraded mutation;
- local writes and truncates change only the overlay, local `fsync` advances a
  separate durability generation, and crash recovery restores exactly the last
  locally synced content;
- sealing freezes the base head, symbolic content value, and local durability
  generation, and reconciliation retains that exact modeled value before
  planning; the slice does not model byte storage or a retention receipt;
- a plan applies when the canonical head still equals the base, converges when
  canonical content already equals the branch, and otherwise preserves the
  canonical winner plus a disjoint retained-content conflict sidecar;
- an authority-side change invalidates an older plan, and only a fresh plan may
  publish one canonical version; and
- durable result records survive a modeled process restart, make lost-reply
  retry exact, and reject reuse of the same reconciliation id with changed
  content, base head, or local sync generation. Canonical publication, conflict
  sidecar creation, and result recording are one abstract atomic transaction;
  direct refinement to the Rust journal is still future work.

The `dfs_local_first_namespace_safety.safety` invariant covers one bounded
same-shard namespace transaction while assuming branch eligibility and the
abstract publication evidence described above. Entries are keyed by stable
parent-directory object and name and carry exact stamps, preventing
delete/recreate ABA from satisfying stale unlink or rename preconditions.
Create, mkdir, unlink, rmdir, and no-replacement rename are staged atomically
in the local overlay. Reconciliation classifies them as applicable, converged,
conflicted, or superseded. Applicable rename changes source and destination
together, but its destination must be absent; POSIX rename replacement is not
modeled. A changed unlink target is never deleted, rmdir conflicts with a
concurrently added child, and directory children remain attached to the stable
directory object across rename. Conflict publication preserves modeled object
identities without changing canonical names; it does not prove retained
subtree bytes, storage receipts, or post-crash object reachability.

The `dfs_local_first_namespace_durability_safety.safety` companion stays below
reconciliation and covers one local namespace mutation. A returned mutation is
initially volatile, local directory `fsync` copies the exact overlay image into
the durable image, crash restores that durable image, and sealing requires the
synced mutation and generation. Every transition stutters under the canonical
projection. This slice does not distinguish file `fdatasync` from file
`fsync`, model a general local journal, or connect the durable image directly
to reconciliation publication.

The `dfs_local_first_reconcile_gateway_safety.safety` invariant covers one
shard, one active reconciliation request id, nine finite symbolic payload
digests, and four publication outcomes:

- a typed permit cannot be built until quorum acknowledgements, at least two
  symbolic content-holder receipts, and a metadata-prepare quorum exist for the
  exact frozen plan;
- append is canonical stuttering and remains discardable until commit;
- an unrelated canonical head advance or authority-epoch change makes the
  appended permit unusable, after which rejection clears the volatile slot
  without publishing it;
- commit is the sole local-first canonical linearization point and stores one
  envelope containing the effect, terminal result, and optional conflict
  descriptor;
- a crash before commit discards the append, while a crash after commit retains
  the envelope even if the materialized object/result/sidecar tables still lag;
- recovery materializes exactly that committed envelope, and an exact retry
  can read the journal result without repeating the effect while a changed
  payload is rejected; and
- Apply advances the object revision once; Converged, Conflict, and Superseded
  preserve it; Conflict alone creates a disjoint sidecar descriptor while the
  canonical conflict count stays zero.

The file and namespace gateway-composition safety suites prove the direct
terminal boundary:

- the branch journal head and composition revision remain aligned with the
  gateway canonical state;
- the file action surface stays inside its two one-sync manifests, and every
  nonterminal namespace state retains one bounded gateway journal slot for the
  terminal entry;
- gateway plans and committed envelopes name the exact admitted finite
  manifest, classification, predecessor head, and target revision;
- branch terminal state exists exactly when the gateway journal entry is
  committed, and both transitions occur together at the gateway linearization
  point;
- a precommit crash cannot create a result or sidecar, stale plans are rejected
  on both sides, and postcommit recovery materializes the exact durable
  envelope;
- file apply, converge, and conflict are covered, while namespace applicable,
  converged, conflict, and superseded are distinct; and
- Conflict alone creates a sidecar. Applicable alone changes the composed data
  revision; nonapplying outcomes preserve the canonical winner.

The namespace bridge uses one composition-owned mutation revision because the
gateway's generic object revision is not the namespace stamp total. Unrelated
strong namespace work advances only the common journal head; namespace-changing
strong work advances both counters. These are finite one-branch refinements,
not a proof that symbolic digest labels are collision-resistant hashes.

The gateway's content certificate is a finite receipt-set abstraction, not a
byte-level storage proof. Its strong-head and authority changes are bounded
environment transitions guarded by a quorum set, not direct refinement of the
Raft log. Lock tokens, sync certificates, arbitrary membership changes,
multiple journal entries, byte hashes, torn writes, snapshots, and Rust table
batches remain outside this slice.

`scenarios/dfs/local_first_file_reconcile.qnt` deterministically exercises
volatile-write loss, locally synced recovery, stale-plan rejection, divergent
content preservation, an outcome-unknown reply, process restart, and exact
replay.
`scenarios/dfs/local_first_namespace_reconcile.qnt` explores the four terminal
namespace classifications, including stale-plan reclassification and
idempotent terminal retry. The composed namespace slice binds all seven
reachable one-operation manifest shapes to distinct labels, but still fixes a
single request identity rather than modeling a general payload-keyed replay
table. These are target-hardening specifications.

`scenarios/dfs/local_first_namespace_durability_recovery.qnt` deterministically
loses an unsynced mutation, replays it with the same bounded mutation identity,
directory-syncs it, recovers it after a second crash, and seals that durable
image. `scenarios/dfs/local_first_reconcile_gateway_recovery.qnt` contains a
crash-boundary trace and a fencing trace. The first discards an uncommitted
append, then recovers a committed outcome-unknown conflict and checks exact and
changed retries. The second separately rejects a stale-head append and a
stale-epoch append. `scenarios/dfs/local_first_gateway_composition.qnt`
crosses both file gateway crash boundaries through the composed actions and
explores all four namespace terminal classifications through the paired commit.
The slices do not yet model hardlinks, symlinks,
cross-shard local work, branch gossip, service-defined semantic merge,
arbitrary byte offsets,
concrete byte-validated retention receipts, POSIX rename replacement, or Rust
refinement of the degraded path.

The cross-shard namespace slice additionally defines a mandatory forward
finalization for an exhausted `FailedRecoverable` state with durable destination
adoption. The transition clears both path reservations without consuming
another retry. Random exploration reaches this recovery class, and the
full-depth deterministic
`scenarios/dfs/posix_cross_shard_retry_exhaustion.qnt` check proves the exact
three-failure regression. This bounded evidence closes the previously observed
deadlocked trace; it is not an exhaustive proof over every cross-shard recovery
history or a Rust conformance result.

The `dfs_pending_request_lifecycle_safety.safety` invariant combines these properties:

- every active request id was issued by the model's `next_request_id`
  abstraction;
- settled request ids were previously issued;
- once a request settles, it is no longer active in read, namespace,
  primary-side remote-write-response, stage, or repair maps;
- routed-write terminal result cache entries are not active requests;
- a request id has at most one active owner across the modeled maps;
- stage and repair request ids are unique by target replica;
- a read response settles only when request id, expected source, and object
  identity all match;
- a wrong-source response is discarded while the original read remains pending;
- a wrong-object response from the expected source is terminally rejected and
  cannot enter the successful settled set;
- read identity failures retain expected and actual source/object audit facts;
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
eventual progress, wall-clock timeout fairness, or end-to-end byte-body
integrity across the DFS and network-session layers.

The `dfs_namespace_command_replay_safety.safety` invariant refines the
primary-side namespace replay cache and requester cancellation boundary:

- the replay key is the transport requester plus the complete
  `DfsNamespaceCommand`, not request id alone;
- an exact resident retry returns the original captured response without
  executing another namespace effect;
- changing the operation payload while retaining the request id is a new
  command and executes normally;
- cache insertion is FIFO bounded (two entries in the model, corresponding to
  the production 32-entry `VecDeque`);
- exact retry after FIFO eviction or process restart may execute the mutation
  again, so the cache is not a durable exactly-once guarantee;
- cancellation removes the requester-side wait but cannot retract a command
  already sent to the metadata primary; and
- a cancelled call retried with a fresh request id is a new replay key.

This maps to `NetworkedDfsService::handle_namespace_request`,
`served_namespace_replays`, `settle_routed_namespace_op_inner`, process-local
service-state initialization, and fresh request-id allocation. Named witnesses
make both the protected exact-retry path and the eviction, restart, changed
payload, and cancellation limitations executable. The existing Rust regression
`served_namespace_exact_retry_replays_completed_rename` covers the resident
exact-retry case; durable domain idempotency after cache loss remains a separate
hardening problem.

The `dfs_repair_content_validation_safety.safety` invariant combines these properties:

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
`NetworkedDfsService::handle_fetch_blob_response`. The model includes the
implementation nuance that a restored repair response may be accepted without a
tracked in-flight request when a matching repair job still exists. It also
models stale wrong-hash metadata receipts as backend state that must not become
durable repair evidence. Body framing, end-to-end finish attestation, flow
control, and replay-safe retry are now `sinomo-network` obligations and are not
proved by this DFS slice. It also does not prove retry fairness or that every
missing replica is eventually repaired.

The `dfs_repair_lost_authority_safety.safety` invariant combines these properties:

- removing committed pending repair state clears all pending repair/fetch state;
- suppressed repair replicas cannot remain active or become retry-due;
- persisted lost-authority attempt records stay represented as suppressed
  retry state;
- lost-authority suppression clears fetch inflight, request, and failed state;
- lost-authority repair responses suppress the observed replica;
- lost-authority maintenance suppresses the repair backlog when the local node
  is no longer authoritative or the pending repair no longer targets current
  content;
- stale repair-base rejections stay retryable and do not suppress the replica;
- transient route failures stay retryable and do not suppress the replica;
- retry-due and repair-request replicas are known repair replicas and are not
  suppressed;
- fetch request state remains internally consistent.

This maps to `repair_rejection_indicates_lost_authority`,
`repair_rejection_indicates_stale_replica_base`,
`repair_error_indicates_transient_route_failure`,
`NetworkedDfsService::handle_repair_replica_response`,
`NetworkedDfsService::retry_pending_repairs_bounded_with_attempts`,
`NetworkedDfsService::restore_repair_attempt_records`,
`NetworkedDfsService::record_repair_retry_scheduled_with_error`,
`NetworkedDfsService::suppress_repair_retries_for_lost_authority`. The model
explicitly separates permanent lost-authority suppression from stale-base
failures and transient routing errors. It does not prove
that a newly authoritative primary will eventually take over repair, nor does
it prove network fairness or cross-primary repair handoff progress.

The `dfs_anti_entropy_safety.safety` invariant combines these properties:

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

The formal toolchain is pinned to Quint 0.32.0 and Apalache 0.56.1. The checker
rejects another Quint version unless an intentional compatibility audit sets
`QUINT_ALLOW_VERSION_MISMATCH=1`. Symbolic runs are serialized through a local
lock because Apalache normally uses `localhost:8822`.

Use the smallest mode that answers the current question:

```bash
# All typechecks, focused runs, smoke scenarios, and symbolic checks.
bash scripts/formal/check-quint.sh all

# Typechecks, runs, smoke scenarios, and the faster symbolic tier.
bash scripts/formal/check-quint.sh fast

bash scripts/formal/check-quint.sh typecheck
bash scripts/formal/check-quint.sh run
bash scripts/formal/check-quint.sh smoke
bash scripts/formal/check-quint.sh verify-fast
bash scripts/formal/check-quint.sh verify-full
```

Focused cases live in one registry inside `check-quint.sh`. The harness fails if
that registry omits a `checks/**/*_safety.qnt` file. Select one or more cases
without editing the script:

```bash
QUINT_CASE_FILTER=network-timed-delivery,dfs-pending-requests \
  bash scripts/formal/check-quint.sh verify-full
```

Randomized runs use a fixed seed by default. Important branches expose named
witness predicates; coverage mode raises the sample count and fails if any
registered witness is not reached:

```bash
QUINT_CASE_FILTER=raft-protocol,network-route-cache,network-delivery,network-attempt-budget,network-timed-delivery,membership-raft-config,dfs-posix-cross-shard,dfs-posix-advisory-locks,dfs-posix-fsync,dfs-posix-partitioned-operation,dfs-local-first-file-gateway-composition,dfs-local-first-namespace-gateway-composition,dfs-pending-requests,dfs-namespace-command-replay \
  QUINT_COVERAGE_MAX_SAMPLES=5000 \
  bash scripts/formal/check-quint.sh coverage
```

Per-command timeouts default to 300 seconds and can be changed with
`QUINT_CASE_TIMEOUT_SECONDS`. Formal verification is intentionally run on
demand with the local harness rather than in GitLab CI: symbolic exploration is
too computationally expensive to repeat for every commit. Use the smallest
focused mode that covers the changed proof boundary, and reserve the full tier
for deliberate verification runs.

For deeper whole-system randomized exploration, use:

```bash
bash scripts/formal/run-quint-smoke.sh
QUINT_SYSTEM_SMOKE_SEED=0x58c1421ebe0d1421 bash scripts/formal/run-quint-smoke.sh
```

`run-quint-smoke.sh` defaults to 160 steps and 5000 samples. Override it with
`QUINT_SYSTEM_SMOKE_RUN_MAX_STEPS` and `QUINT_SYSTEM_SMOKE_RUN_MAX_SAMPLES`.

The checker is equivalent to these command classes:

```bash
while IFS= read -r -d '' file; do
  timeout --foreground 300s quint typecheck "$file"
done < <(find specs/quint -name '*.qnt' -print0 | sort -z)

quint run specs/quint/checks/kv/raft_kv_safety.qnt --main=raft_kv_safety --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/checks/network/route_planning_safety.qnt --main=network_route_planning_safety --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/checks/network/timed_delivery_retry_safety.qnt --main=network_timed_delivery_retry_safety --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/scenarios/network/timed_delivery_smoke.qnt --main=network_timed_delivery_smoke --invariant=safety --max-steps=80 --max-samples=500 --verbosity=1
quint run specs/quint/checks/dfs/posix_namespace_safety.qnt --main=dfs_posix_namespace_safety --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/scenarios/system/smoke.qnt --main=system_smoke --invariant=safety --max-steps=80 --max-samples=500 --verbosity=1

quint verify specs/quint/checks/kv/raft_kv_safety.qnt --main=raft_kv_safety --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/checks/network/route_planning_safety.qnt --main=network_route_planning_safety --invariant=safety --max-steps=4 --verbosity=1
quint verify specs/quint/checks/network/timed_delivery_retry_safety.qnt --main=network_timed_delivery_retry_safety --invariant=safety --max-steps=7 --verbosity=1
quint verify specs/quint/checks/dfs/posix_namespace_safety.qnt --main=dfs_posix_namespace_safety --invariant=safety --max-steps=2 --verbosity=1
```

`check-quint.sh` repeats this `checks/<subsystem>/*_safety.qnt` run/verify
pattern for every focused invariant suite, runs the timed network smoke
scenario from `scenarios/network/timed_delivery_smoke.qnt`, and runs the
full-system smoke scenario from `scenarios/system/smoke.qnt`.

The default symbolic bound is deliberately modest. On this checkout,
the Raft, network, DFS metadata, and DFS routing specs use
`QUINT_VERIFY_MAX_STEPS`, defaulting to 5. DFS routing can also be overridden
with `QUINT_DFS_ROUTING_VERIFY_MAX_STEPS`; its 5-step symbolic check passed on
this checkout but took about 53-81 seconds in local runs. The deeper per-node
Raft slice uses `QUINT_RAFT_PROTOCOL_VERIFY_MAX_STEPS`,
defaulting to 1 for routine initialization-state proof while fixed-seed witness
coverage exercises ten-step protocol traces. Its 1-step Apalache check passed in
about 53 seconds; a 3-step attempt was interrupted after about 230 seconds and
is not claimed as a pass. Scheduled CI raises this slice to 2 steps under a
900-second per-command timeout. The network endpoint
selection spec uses `QUINT_NETWORK_ENDPOINT_VERIFY_MAX_STEPS`, defaulting to 4;
its 4-step symbolic check passed in about 10 seconds. The network route
planning spec uses `QUINT_NETWORK_ROUTE_VERIFY_MAX_STEPS`, defaulting to 4; its
4-step symbolic check passed in about 5 seconds. The network route-cache spec
uses `QUINT_NETWORK_ROUTE_CACHE_VERIFY_MAX_STEPS`, defaulting to 4; its 4-step
symbolic check passed in about 14 seconds. The network delivery retry spec
uses `QUINT_NETWORK_DELIVERY_VERIFY_MAX_STEPS`, defaulting to 5; its isolated
6-step symbolic check passed in about 6 seconds. The network attempt-budget spec
uses `QUINT_NETWORK_ATTEMPT_BUDGET_VERIFY_MAX_STEPS`, defaulting to 3. The
timed transport and timed delivery-retry specs use
`QUINT_NETWORK_TIMED_TRANSPORT_VERIFY_MAX_STEPS` and
`QUINT_NETWORK_TIMED_DELIVERY_VERIFY_MAX_STEPS`, defaulting to 5 and 7,
respectively. These bounds reach packet delivery and fallback paths; named
witnesses independently guard randomized reachability. The
timed network smoke scenario uses `QUINT_NETWORK_TIMED_SMOKE_RUN_MAX_STEPS` and
`QUINT_NETWORK_TIMED_SMOKE_RUN_MAX_SAMPLES`, defaulting to 80 and 500. The
read-freshness, routed namespace-freshness, and metadata-primary failover specs
use `QUINT_DFS_READ_FRESHNESS_VERIFY_MAX_STEPS`,
`QUINT_DFS_ROUTED_NAMESPACE_VERIFY_MAX_STEPS`, and
`QUINT_DFS_PRIMARY_FAILOVER_VERIFY_MAX_STEPS`, respectively; each defaults to 3
because they are compact decision/protocol-slice models. The POSIX namespace,
links, cross-shard namespace, read/write visibility, advisory-lock, fsync, and
partitioned-operation specs use
`QUINT_DFS_POSIX_NAMESPACE_VERIFY_MAX_STEPS`,
`QUINT_DFS_POSIX_LINKS_VERIFY_MAX_STEPS`,
`QUINT_DFS_POSIX_CROSS_SHARD_VERIFY_MAX_STEPS`,
`QUINT_DFS_POSIX_READ_WRITE_VERIFY_MAX_STEPS`,
`QUINT_DFS_POSIX_ADVISORY_LOCKS_VERIFY_MAX_STEPS`,
`QUINT_DFS_POSIX_FSYNC_VERIFY_MAX_STEPS`, and
`QUINT_DFS_POSIX_PARTITIONED_OPERATION_VERIFY_MAX_STEPS`; the defaults are 2,
2, 3, 2, 3, 3, and 8, respectively. On this checkout, the advisory-lock,
fsync, and partitioned-operation default checks passed in about 78, 15, and 8
seconds. The local-first regular-file, namespace, and namespace-durability
checks use
`QUINT_DFS_LOCAL_FIRST_FILE_VERIFY_MAX_STEPS` and
`QUINT_DFS_LOCAL_FIRST_NAMESPACE_VERIFY_MAX_STEPS`, both defaulting to 4, and
`QUINT_DFS_LOCAL_FIRST_NAMESPACE_DURABILITY_VERIFY_MAX_STEPS`, defaulting to
8. The typed reconciliation gateway uses
`QUINT_DFS_LOCAL_FIRST_RECONCILE_GATEWAY_VERIFY_MAX_STEPS`, defaulting to 4.
The direct file and namespace composition checks use
`QUINT_DFS_LOCAL_FIRST_FILE_GATEWAY_COMPOSITION_VERIFY_MAX_STEPS` and
`QUINT_DFS_LOCAL_FIRST_NAMESPACE_GATEWAY_COMPOSITION_VERIFY_MAX_STEPS`, both
defaulting to 4. Their deterministic full-causal drivers run file apply at
depth 17 for both admitted manifests, file converge/conflict at depth 12, and
each namespace terminal classification and finite manifest at depth 11. The
namespace Applicable driver also runs in the fast tier so terminal atomicity is
not left beyond its routine depth-4 exploration bound.
On this checkout the focused file, namespace, and namespace-durability runs
passed in about 36, 59, and 7 seconds. The file conflict/restart/mismatch,
apply, and converge drivers passed at depths 19, 15, and 11 in about 32, 37,
and 42 seconds; the namespace and namespace-durability full-causal drivers
passed in about 71 and 6 seconds. The gateway additionally has deterministic
precommit-crash, postcommit-recovery, and head/epoch-fencing full-safety drivers
at depths 8, 12, and 16. On this checkout its nondeterministic depth-4 check
passed in about 116 seconds and the three full-causal drivers passed in about
100, 106, and 109 seconds. A 5,000-trace local-first coverage run reached every
currently registered local-first witness. The harness also registers separate
branch, gateway, and direct branch/gateway composition smoke scenarios.
A fixed-seed 5,000-trace coverage run reached the other then-registered witnesses,
and deterministic lock-fence, file-sync recovery, directory-generation sync,
partition-winner, and cross-shard exhaustion scenarios reached their clean
terminal states. The harness also symbolically verifies those fixed causal
traces at full depth, while keeping the wider nondeterministic models at their
documented routine bounds. The namespace model is deliberately wider because
it tracks a finite tree and previous-state immutability; its 2-step symbolic
check passed locally in about one minute, while a 3-step attempt progressed through many
obligations before Z3 reported `UNKNOWN`. The namespace
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
passed in about 30 seconds. The repair lost-authority spec can be overridden with
`QUINT_DFS_REPAIR_LOST_AUTHORITY_VERIFY_MAX_STEPS`; its default is 4 because
the 5-step symbolic check passed but took about 135 seconds. The anti-entropy
spec can be overridden with `QUINT_DFS_ANTI_ENTROPY_VERIFY_MAX_STEPS`; its
5-step symbolic check passed in about 8 seconds. The membership/KV bridge uses
`QUINT_MEMBERSHIP_KV_VERIFY_MAX_STEPS`, defaulting to 4, and runs with TLC over
`stepCore`. The full-system smoke model is intentionally randomized in the
normal script path instead of symbolically verified as a routine gate; a
5-step symbolic smoke check has passed locally, but the useful signal is the
broader random exploration and seed-based replay.

The namespace-command replay slice uses
`QUINT_DFS_NAMESPACE_REPLAY_VERIFY_MAX_STEPS`, defaulting to 5; its 5-step
Apalache check passed, while a fixed-seed 5,000-trace run reached all six
resident-replay and cache-loss/cancellation witnesses.

Use larger bounds for local audit runs, not as a routine CI gate:

```bash
QUINT_VERIFY_MAX_STEPS=8 QUINT_RAFT_PROTOCOL_VERIFY_MAX_STEPS=2 QUINT_NETWORK_ENDPOINT_VERIFY_MAX_STEPS=5 QUINT_NETWORK_ROUTE_VERIFY_MAX_STEPS=5 QUINT_NETWORK_ROUTE_CACHE_VERIFY_MAX_STEPS=5 QUINT_NETWORK_DELIVERY_VERIFY_MAX_STEPS=6 QUINT_NETWORK_ATTEMPT_BUDGET_VERIFY_MAX_STEPS=4 QUINT_NETWORK_TIMED_TRANSPORT_VERIFY_MAX_STEPS=6 QUINT_NETWORK_TIMED_DELIVERY_VERIFY_MAX_STEPS=8 QUINT_NETWORK_TIMED_SMOKE_RUN_MAX_STEPS=160 QUINT_NETWORK_TIMED_SMOKE_RUN_MAX_SAMPLES=5000 QUINT_SYSTEM_SMOKE_RUN_MAX_STEPS=240 QUINT_SYSTEM_SMOKE_RUN_MAX_SAMPLES=10000 QUINT_DFS_ROUTING_VERIFY_MAX_STEPS=6 QUINT_DFS_READ_FRESHNESS_VERIFY_MAX_STEPS=4 QUINT_DFS_ROUTED_NAMESPACE_VERIFY_MAX_STEPS=4 QUINT_DFS_POSIX_NAMESPACE_VERIFY_MAX_STEPS=2 QUINT_DFS_POSIX_LINKS_VERIFY_MAX_STEPS=2 QUINT_DFS_POSIX_CROSS_SHARD_VERIFY_MAX_STEPS=3 QUINT_DFS_POSIX_READ_WRITE_VERIFY_MAX_STEPS=2 QUINT_DFS_POSIX_ADVISORY_LOCKS_VERIFY_MAX_STEPS=4 QUINT_DFS_POSIX_FSYNC_VERIFY_MAX_STEPS=4 QUINT_DFS_POSIX_PARTITIONED_OPERATION_VERIFY_MAX_STEPS=10 QUINT_DFS_LOCAL_FIRST_FILE_VERIFY_MAX_STEPS=6 QUINT_DFS_LOCAL_FIRST_NAMESPACE_VERIFY_MAX_STEPS=6 QUINT_DFS_LOCAL_FIRST_NAMESPACE_DURABILITY_VERIFY_MAX_STEPS=8 QUINT_DFS_LOCAL_FIRST_RECONCILE_GATEWAY_VERIFY_MAX_STEPS=6 QUINT_DFS_PRIMARY_FAILOVER_VERIFY_MAX_STEPS=4 QUINT_DFS_NAMESPACE_REPLAY_VERIFY_MAX_STEPS=6 QUINT_DFS_NAMESPACE_VERIFY_MAX_STEPS=6 QUINT_DFS_SHARDED_NAMESPACE_VERIFY_MAX_STEPS=6 QUINT_DFS_JOURNAL_VERIFY_MAX_STEPS=6 QUINT_DFS_REPAIR_VERIFY_MAX_STEPS=6 QUINT_DFS_REPLICATION_VERIFY_MAX_STEPS=6 QUINT_DFS_NO_CONFLICT_VERIFY_MAX_STEPS=6 QUINT_DFS_PENDING_VERIFY_MAX_STEPS=4 QUINT_DFS_REPAIR_CONTENT_VERIFY_MAX_STEPS=6 QUINT_DFS_REPAIR_LOST_AUTHORITY_VERIFY_MAX_STEPS=5 QUINT_DFS_ANTI_ENTROPY_VERIFY_MAX_STEPS=6 QUINT_MEMBERSHIP_VERIFY_MAX_STEPS=5 QUINT_MEMBERSHIP_KV_VERIFY_MAX_STEPS=5 QUINT_MEMBERSHIP_RAFT_CONFIG_VERIFY_MAX_STEPS=5 bash scripts/formal/check-quint.sh all
```

## Current Abstractions

This slice does not yet model:

- byte-level WAL durability, snapshot restore, torn writes, or persistence
  failures beneath the abstract durable term/vote/log restart action;
- concrete Raft RPC frames, transport loss/reordering, and retry over the
  network abstraction;
- watches, leases, compare-and-swap transactions, and revision histories;
- concrete invite cryptography, certificate validation, and role-policy checks;
- concrete Raft config-change RPCs over the network abstraction;
- cluster leave and migration quarantine/import behavior;
- full-system smoke internals beyond the current abstract integration surface:
  real directory snapshots, composition with the focused route-cache model, raw
  transport frames, stream payloads, full KV log replay, and concrete DFS
  object-store contents;
- live `ClusterDirectory` read failures during an in-flight delivery,
  full-duplex session retry budgets, concrete transport nonce entropy, unbounded
  wall-clock fairness, and dynamic module enable/disable races;
- directory fragments, local-owned owner routing, owner-inclusion replica-set
  normalization, durable namespace-command idempotency across cache eviction or
  restart, cross-primary repair handoff progress, router-local recovery after
  sharded namespace index write failures, patch offset/base semantics,
  read-memory assembly details, distributed anti-entropy repair scheduling
  progress, wall-clock timeout fairness, concrete lock-lease clock bounds,
  byte-range and multi-object application lock cycles, open-handle behavior
  across rename/unlink, multi-operation local branch dependency graphs,
  hardlink/symlink and cross-shard local-first reconciliation, branch gossip,
  service-defined semantic merge, arbitrary request identities and multiple
  reconciliation entries, general payload-keyed namespace replay, POSIX rename
  replacement, concrete byte-validated retained-content/subtree receipts, and
  direct gateway composition with the canonical lock, sync, partition,
  no-conflict, and Raft models or the Rust journal.

Future models should remain layered instead of expanding one state machine.
The active verification backlog is maintained in the
[formal verification roadmap](../plans/formal-verification-roadmap.md).
