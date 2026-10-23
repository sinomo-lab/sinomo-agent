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
  helpers, `Option`, membership roles, membership projection helpers, and
  bounded logical network time.
- `specs/quint/models/` contains algorithm transition systems. Model modules
  own state variables, helper projections, `init`, `step`, and domain actions.
  They intentionally do not define the executable `safety` invariant.
- `specs/quint/contracts/` contains reusable guarantee predicates consumed by
  higher layers. This includes timed delivery predicates, the local-first
  canonical projection and typed reconciliation permit, and the full-system
  integration guarantees over the shared system model.
- `specs/quint/checks/` contains invariant suites. Each check imports one model
  plus the shared vocabulary it needs, defines the invariant predicates, and
  exposes the `safety` entrypoint used by `quint run` and `quint verify`.
- `specs/quint/scenarios/` contains composed exploration entrypoints and
  deterministic causal regressions. Randomized scenarios can import several
  models or contracts; deterministic scenarios are also symbolically checked
  at the full depth of their prescribed trace when registered by the harness.

The model inventory is split by subsystem:

- `models/kv/raft_kv.qnt` models the compact Raft/KV committed-prefix and result
  authority contract: leader election, node online/offline transitions, catch-up,
  local and authoritative reads, quorum-backed writes, exact committed-request
  replay, and KV-state replay. `models/kv/raft_protocol.qnt` complements it with
  bounded per-node term, vote, log, replication, commit, and durable restart
  state. Their invariant entrypoints live under `checks/kv/`.
- `models/network/` contains the transport-independent base network mechanism,
  endpoint selection, and timed transport model. Matching invariant entrypoints
  live under `checks/network/`. Route planning and routed-stream realization are
  currently covered by their executable Rust refinement tests.
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
- `models/dfs/` contains the DFS slices, named here by file stem:
  - metadata authority, publication, and routing: `metadata`,
    `canonical_write_eligibility`, `whole_shard`,
    `operation_authority_snapshot_reuse`, `authority_routing`,
    `read_freshness`, `routed_namespace_freshness`,
    `metadata_primary_failover` (local failover-target selection), and
    `no_conflict`;
  - metadata journal and storage: `metadata_journal_source`,
    `metadata_journal_tail_recovery`, `metadata_stale_head_reopen`,
    `metadata_ack_floor_log`, `authored_journal_batch`,
    `initial_prepare_batch`, `metadata_routing_hint_recovery`,
    `packed_index_append_batching`, and `metadata_checkpoint_fanout`;
  - shard handoff and replica catch-up: `metadata_handoff_union_sync`,
    `metadata_handoff_desired_catchup`, `metadata_handoff_maintenance_trigger`,
    `metadata_handoff_lane_rebind`, `metadata_handoff_probe_backend_selection`,
    `metadata_authority_ack_prefix_validation`,
    `metadata_retiring_lane_replica_ingest`,
    `metadata_primary_handoff_activation`,
    `metadata_primary_handoff_cursor_preflight`,
    `replication_plan_freshness`, and `replica_projection_refresh`;
  - namespace transactions: `namespace_transaction`,
    `namespace_transaction_body_transport`, `namespace_planner_equivalence`,
    `namespace_expectation_snapshot_validation`,
    `namespace_prepare_footprint_index_equivalence`,
    `namespace_tombstone_overlay_equivalence`,
    `namespace_create_transaction_identity`, `namespace_command_replay`, and
    `sharded_namespace_transaction`;
  - POSIX facade: `posix_namespace`, `posix_links`,
    `posix_cross_shard_namespace`, `posix_read_write_visibility`,
    `posix_advisory_locks`, `posix_fsync`, and `posix_partitioned_operation`;
  - opt-in local-first branches: `local_first_file_branch`,
    `local_first_namespace`, `local_first_namespace_durability`,
    `local_first_reconcile_gateway`, `local_first_file_gateway_composition`,
    and `local_first_namespace_gateway_composition`;
  - coordinator, content, and repair: `intent_index_equivalence`,
    `receipt_bucket_equivalence`, `replication_policy`,
    `requester_prestage_binding`, `content_binding_publication`,
    `content_binding_lifecycle`, `content_accounting_derived_stats`,
    `content_catalog_causal_cut`, `pending_request_lifecycle`,
    `repair_authorization`, `repair_content_validation`,
    `replica_quorum_fanout`, `replica_promotion_outbox`,
    `repair_lost_authority`, `repair_queue_refresh`, `bounded_intent_scan`,
    and `anti_entropy`;
  - daemon scheduling: `metadata_shard_concurrent_dispatch` and
    `daemon_read_blocking_admission`.

  Each model has a matching `checks/dfs/<stem>_safety.qnt` invariant suite.
  `scenarios/dfs/` holds deterministic canonical-write handoff and
  historical canonical-write traces, the ten registered whole-shard scenarios,
  pending-read identity, advisory-lock fencing, file-sync and
  directory-generation recovery, complete-holder/quorum-repair partitions,
  cross-shard retry exhaustion, local-first file/namespace reconciliation,
  local directory-durability recovery, reconciliation-gateway crash/fencing,
  and composed branch/gateway traces. `whole_shard_metadata_content.qnt` is
  not registered with the harness; `scripts/formal/test-shard-trace.py` uses
  it as a model-sensitivity witness.

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
  result-authority refinement, not a proof of concrete RequestVote,
  AppendEntries, WAL, snapshot, or daemon scheduling behavior.
- **Target hardening** states behavior that the implementation does not yet
  enforce. Individual model sections identify those remaining gaps.

The catch-up certificate required by the membership promotion model is now a
current-behavior refinement. The Rust lifecycle requires sufficient learner
match-index evidence before promotion, records terminal evidence, and performs
bounded restart reconciliation of interrupted changes.

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
- the global commit index and next log index stay inside the model bound;
- a local read succeeds independently of quorum and returns exactly the selected
  node's applied committed prefix, which may be older than the canonical prefix;
- an authoritative read succeeds only with a current-term quorum barrier and
  returns the canonical committed prefix, while failure returns no local
  fallback;
- a new effect-changing write reports success only with quorum-commit evidence;
- an exact retry may reuse success without live quorum only when its request id
  and command match a durable committed-request record; and
- a semantic no-effect write reports success only with a current-term quorum
  barrier or that exact durable prior-commit evidence.

The deliberately broken `brokenNoQuorumNoEffect` transition records the old
failure mode: local equality reports success while no quorum barrier and no
exact committed-request record exist. It is excluded from the refined `step`
relation and reached only through `coverageStep`.
`witnessBrokenNoQuorumNoEffectCounterexample` requires randomized coverage to
exhibit that contract violation before the refinement is accepted; symbolic
verification continues to check only the corrected `step` relation.

This slice intentionally does not model daemon scheduling, source-change
coalescing, retry backoff, routing, snapshot RPCs, or app/CLI wiring. The
action-owned reactive boundary determines which concrete operation runs an
existing Raft action; it is not quorum or freshness evidence. Observational
reads schedule no KV work. Those mechanics belong in deterministic Rust and
cluster tests rather than `raft_kv` or the deeper `raft_protocol` model.

The model already caught one useful bug during drafting: an offline node was
allowed to come online without catch-up, then accept a later committed entry
without the earlier prefix. The Rust in-process cluster catches up a node when
`set_node_online(..., true)` is called, so the spec now models that behavior.

Run the focused reactive-contract checks with the pinned Quint and Apalache
toolchain:

```bash
QUINT_CASE_FILTER=raft-kv QUINT_COVERAGE_MAX_SAMPLES=5000 \
  bash scripts/formal/check-quint.sh coverage

QUINT_CASE_FILTER=raft-kv QUINT_VERIFY_MAX_STEPS=5 \
  bash scripts/formal/check-quint.sh verify-fast
```

Require refined safety and the
`witnessBrokenNoQuorumNoEffectCounterexample` sensitivity witness. Record actual
coverage, bounds and elapsed time in the run artifacts.

Companion Rust evidence covers the mechanics intentionally excluded from this
Quint slice: local-read availability without quorum, authoritative-read
failure without a barrier, ordinary and semantic-no-effect write fencing,
exact committed-request replay and outcome recovery, maintenance wake
coalescing/backoff/shutdown/non-starvation, restart-safe config-change
reconciliation, and intermittent-cluster catch-up. The focused entrypoints are:

```bash
cargo test -p sinomo-kv --test kv
cargo test -p sinomo-cluster --lib
cargo test -p sinomo-cluster --test control_plane
```

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
`ClusterKvRaftStore::apply_vote`, `DurableRaftKvNode::request_vote`, the
AppendEntries preparation/application path,
`build_append_entries_request_from_next_index`, quorum replication and commit
helpers, and the persisted hard-state/log load path in `sinomo-kv`.
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

The former route-cache, delivery-retry, endpoint-budget, persistent-tunnel, and
body-spool proof slices were refinements of the removed delivery executor and
its carriers. They were deleted with the implementation rather
than retained as misleading verification. The current deterministic planner,
candidate ordering, loop prevention, capability filtering, multi-hop
realization, identity checks, and bounded candidate attempts are exercised in
`crates/sinomo-network/src/routing.rs`. A future formal routing slice must refine
`plan_routes` and `MultiHopRoutedConnector` directly.

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

The timed transport model remains finite and safety-oriented. It models packet
loss, delay, duplication, and expiry independently of any removed RPC carrier;
it does not prove unbounded real-time fairness or scheduler progress.

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
- removing voters requires the retained set to satisfy both quorum sizes and
  requires `Q_old + Q_new > |V_old union V_new|`, so every old quorum intersects
  every new quorum; this rejects both the conservative two-to-one case and the
  disjoint-quorum five-to-three counterexample;
- committing a valid proposal still requires acknowledgements from a quorum of
  the current configuration;
- health observations do not mutate the committed Raft configuration;
- focused initial cases cover learner and non-voting promotion, valid
  three-to-two shrink, invalid direct/multiple additions, and insufficient
  acknowledgement.

This maps to `validate_cluster_kv_raft_config_transition` and separates the
focused configuration-validator proof from the broader membership lifecycle's
catch-up certificate. Both are current-behavior refinements. The Rust table test
`cluster_kv_raft_config_transition_matches_bounded_model_cases` covers the same
learner/non-voting promotion, direct and multiple addition rejection, and
three-to-two versus two-to-one shrink cases. The Rust
`cluster_kv_raft_config_transition_enforces_quorum_intersection_inequality`
regression additionally covers the five-to-three rejection and a safe
six-to-four multi-removal transition.

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

The `dfs_canonical_write_eligibility_safety.safety` invariant focuses on the
ordinary, non-local-first canonical write gate. Its handoff transition retains
an older full-union restriction, so it omits accepted joint-majority activation
paths and does not establish complete current handoff conformance. The current
logical contract is:

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
- activation transfers the exact fenced prefix across the retiring and desired
  assignments and requires a majority of each assignment, explicit
  acknowledgements from both primaries, and no foreign acknowledger. The
  completion proof must equal the immutable retiring-primary fence and carry
  the captured operation floor and exact Raft watermark. This permits an
  offline configured replica while carrying writes admitted before the pending
  decision and finished before the retiring freeze;
- only after the joint proof succeeds does the desired assignment become the
  active writer. The durable local marker advances to the activated epoch;
  only an instance rebound after observing that exact generation may write,
  so independently opened stale instances remain fenced. A proof-carrying
  primary promotion additionally requires the promoted replica's exact-state
  watermark through the retiring-primary fence; and
- visible versions form one dense history and canonical operation creates no
  conflict records.

The `dfs-requester-prestage-binding` case isolates the requester content
shortcut. Its durable binding includes the exact object/version/hash manifest
and reservation, rather than blob bytes alone. Version or object rebasing
suppresses the original requester receipt until the existing fetch/stage path
durably binds the canonical tuple at a selected replica. The bounded model
covers requester exclusion, rejected namespace admission, fresh metadata
authority, caller cancellation, and a lost commit reply followed by requester
crash/restart. An unchanged W=1 create can reuse its requester binding without
staging through the metadata primary. This is a refinement target for the
manifest routing decision; namespace admission, concrete wire validation,
storage barriers, and canonical rebasing are additionally exercised in Rust.

This is a focused canonical publication contract with the handoff coverage
restriction above, not a model of storing file metadata in KV. Cluster KV supplies committed DFS
configuration facts plus the bounded operation permit and head watermark used
when the exact metadata shard cannot all acknowledge the operation. This
reuses the cluster's existing Raft election, commit index, and linearizable log;
DFS does not implement a second quorum-election protocol, and the DFS prepare,
journal, snapshot, and object state remain outside KV. The bounded partition
cases deliberately set `W=1`, below cluster quorum, so the unsafe minority-side
case remains reachable. POSIX fsync/lock and all local-first models are kept
unchanged and outside this slice.

### Whole-shard prepare, publication and handoff

The `dfs-whole-shard` case composes hidden `prepare(payload)`, digest-bound
publication, durable writer fencing, handoff and exact replay. Ordinary
publication has two authority classes: positively established local standalone
ownership, or exact prepare acknowledgements from **every assigned metadata
participant**. A metadata majority, content `W`, or KV voter quorum cannot
substitute for the full assigned set. A partial set leaves the prepared body
hidden and publication unavailable. There is no per-operation KV permit,
metadata operation floor or KV head watermark in this current boundary.

State and actions live in `models/dfs/whole_shard.qnt`, with the authority
predicate in `contracts/dfs/whole_shard_authority.qnt` and invariants/witnesses
in `checks/dfs/whole_shard_safety.qnt`. These are a **bounded abstract contract**;
passing a model check does not prove native execution. Current Rust conformance
requires the exact build, complete captured observations and enabled-action
replay. The local-standalone scenario is an explicit abstract owner profile;
a one-voter cluster remains clustered and still needs every assigned metadata
participant. It is never inferred from missing cluster evidence.

The bound is four eligible nodes, three operations, six semantic journal
positions, two clustered configuration epochs and one handoff. Each captured
native operation retains its own content target set and required `W`;
metadata-only bodies require no content receipts but still require full
metadata authority. Configuration and payload equality use injective finite
aliases. Multiple writer lanes on one node, more effects and voter-set changes
remain outside this bound. Unsupported inputs are unavailable, never coerced
into a smaller identity set or a literal genesis state.

Publication consumes actual participant proof and the retained coherent
configuration while the native writer, bound instance epoch and durable freeze
protect the kernel. Global handoff intent is separate from this ordinary
participant protocol: it does not introduce an ordinary per-operation KV read.
The retiring freeze waits for any admitted writer, captures the complete native
prefix and then prevents new retiring-epoch publication. A write that finishes
before that freeze must be included in its captured prefix. Activation alone
does not bind an old or unbound backend; the successor verifies the completion
generation and exact prefix before rebind. The instance binding survives release
of an ordinary publication guard and is lost only at the modeled restart.

Native KV retains its genuine global role. The model assumes its existing
quorum-backed decision and activation contract, without modeling Raft election
or adding another consensus mechanism. Handoff activation additionally needs
majorities of both metadata assignments, acknowledgements from both primaries,
and the exact retiring frozen prefix. Promotion requires its durable marker.
StorageImport may preserve primary and member sets while advancing the epoch;
its source cut, converted image and current target prefix remain independently
validated native inputs. Historical source ACK constraints are separate migration
obligations and are not ordinary publication grants.

Run the bounded simulations, deterministic witnesses and sensitivity controls
with:

```bash
QUINT_RUN_SEED=0x51a020261006 bash scripts/formal/check-quint.sh whole-shard
```

The harness pins Quint 0.32.0 and uses 1,000 random traces of 40 steps, plus ten
deterministic scenarios. They cover healthy all-assigned publication, explicit
standalone ownership, majority/minority denial with sufficient content `W`,
late-write capture, joint activation with an offline member, promotion/rebind,
stale epoch, frozen-writer denial, changed digest, divergent prefix and exact
lost-reply replay. The one-voter case proves that a KV quorum still cannot grant
partial metadata publication. Negative controls deliberately remove the full
assignment, joint certificate, freeze or digest gates and must reach invariant
violations; disabled/truncated exploration is not a counterexample.

The symbolic case remains deep-tier at depth eight with the repository's
Apalache 0.56.1 pin. Simulation, reachability, bounded symbolic proof and native
conformance are separate evidence classes. This model omits scheduling,
storage-byte barriers, wire validation, election and unbounded liveness.

Native conformance must join the actual admitted guard to the **same consumed
retained immutable configuration projection**, its native authority provenance
and current accepted generation. It does not require or manufacture a fresh
per-operation authoritative KV read. Exact operation/payload identity, actual
writer/data-owner custody, bound epoch/freeze, complete transaction footprint,
all authenticated participant responses, source prefix, incarnation and capture
coverage remain mandatory. Replica-ingest permits are separate receiver
capabilities and do not authorize ordinary authorship.

`scripts/formal/check-shard-trace.py` validates the versioned native envelope and
replays enabled Quint actions with independently observed post-state. Its model
binding covers the model, invariant suite and authority contract together.
Missing, dropped, discontinuous, truncated or unsupported observations fail
availability; a safe but disabled prefix is not success. Native integer values
stay lossless. The verifier requires executable/source manifests and a complete
terminal cursor. The collector, skeleton, assembler and projector package genuine
Rust observations, not external finite states. Synthetic checker-sensitivity
inputs and standalone model witnesses establish no native authority. No current
capture is accepted until the complete updated conformance boundary passes.

The older canonical-write and union-sync slices described adjacent to this
section retain historical target assumptions, including degraded KV coupling.
They are not current ordinary-publication conformance evidence. This bounded
correction does not silently relabel those older models as implemented behavior.

The `dfs_metadata_handoff_union_sync_safety.safety` invariant isolates the
older, stronger exact-union sync primitive after the retiring-primary fence.
Its removal-only full-union requirement below is not the current native
activation threshold; the native joint validator also admits partial unions
meeting both majorities and both primary acknowledgements:

- the retiring assignment is `{N1,N2}`, the desired assignment is `{N1,N3}`,
  and activation requires the complete union `{N1,N2,N3}`;
- the old counterexample gives the ordinary quorum fallback fresh
  acknowledgements from `{N1,N3}`. The retiring-epoch watermark rejects N3 as
  unconfigured, the invocation clears its fresh acknowledgement state, and a
  later N2 acknowledgement cannot complete that old attempt's union;
- the companion `{N1,N2}` old fallback may produce a valid ordinary retiring-
  epoch certificate, but it is still a partial handoff proof and cannot
  activate;
- the fallback decision is exercised for both algorithms on the same partial
  union. The hardened decision takes an explicit sync purpose: ordinary
  metadata sync may use quorum fallback, but membership-handoff sync may not;
- a removal-only handoff has retiring `{N1,N2,N3}` and desired `{N1,N2}`, so its
  required union equals the active retiring assignment. At partial `{N1,N2}`
  the proof makes ordinary-purpose fallback eligible while handoff-purpose
  fallback remains disabled, retains the ACKs, and waits for N3;
- the hardened path retains every fresh acknowledgement within the bounded
  attempt and keeps a partial union pending and retry-enabled; and
- a later final required acknowledgement makes the union exact, after which
  and only after which activation records a full-shard certificate and consumes
  the pending retry state.

This is target hardening for a handoff-specific full-union primitive. Each ACK
abstracts exact-prefix and fingerprint validation already covered by the
canonical handoff boundary. The slice proves safety and bounded reachability,
not unbounded transport fairness or a wall-clock completion guarantee.

The `dfs_metadata_handoff_maintenance_trigger_safety.safety` invariant isolates
the daemon-side discovery rule that schedules that reconcile:

- the old detector represents pending writes, namespace jobs, replica lag, and
  due or unattempted repairs, but omits membership handoffs;
- when that ordinary work is absent, a pending handoff contributes to the new
  detector exactly when N1 is its retiring primary;
- a nonlocal handoff does not request or enter a local reconcile, and neither
  detector activates without its relevant work;
- the counterexample begins a local handoff immediately after maintenance,
  before the long periodic timer is due: the old detector stays false while
  the new detector requests prompt work;
- consuming that trigger installs the retiring-primary fence before transport
  progress, and accepted retiring writes cannot advance after the fence; and
- a blocked bounded attempt times out to durable pending state without clearing
  the fence, is rediscovered and retriggered, and its second attempt reaches
  completion once transport becomes ready, all before the long periodic tick.

This trigger model changes only scheduling. It reuses the exact-union and
canonical fence assumptions above and does not assert transport fairness.

The `dfs_metadata_handoff_desired_catchup_safety.safety` invariant
(`dfs-metadata-handoff-desired-catchup`) covers a desired-only member that
starts behind the frozen retiring prefix. The old receiver admitted an exact
prefix probe but rejected the contiguous journal suffix needed to make that
probe eligible, so the handoff could stay frozen. The hardened rule treats the
exact pending handoff as a scoped replica-ingest permit: only the retiring
primary's contiguous frozen suffix for that filesystem, epoch, shard, and
desired member is accepted, and missing-handoff, other-member, forged-source,
stale-epoch, foreign-filesystem, and noncontiguous batches reject. The permit
binds no author authority; activation still needs the exact-prefix
acknowledgement and handoff proof.

The `dfs_metadata_handoff_lane_rebind_safety.safety` invariant isolates the
process-local reopen boundary after a completed handoff. Cluster activation and
the durable marker do not bind a newly opened service lane. Bootstrap succeeds
only after that lane verifies the exact successor epoch, active assignment,
completion proof, handoff id, journal head, fingerprint, and local marker.
Wrong proof, wrong epoch, or a still-pending marker leaves the lane unbound, and
rebinding one fresh lane never grants authority to an independently opened
stale lane. The extended slice also keeps a cached mutation lane opened unbound
before completion. Completion on another handle advances the authority
generation but never binds that cached handle directly. Missing, unchanged, or
mismatched generation observations cannot bind it, and observing the exact
changed generation is not itself an authority grant. Mutation succeeds only
after the cached lane consumes that observation together with the full exact
handoff proof at an explicit same-handle rebind or proof-bound reopen boundary.

The `dfs_metadata_ack_floor_log_safety.safety` invariant is a storage refinement
target for separate replica-local prefix and operation-authority ACK floors.
It keeps validated floor payloads opaque and models legacy baseline migration,
a durable legacy-reader rejection marker, append visibility before sync,
failed-sync retry, cross-handle cache invalidation, incomplete EOF recovery,
checkpoint replacement, restart, and missing or corrupt migrated logs. A
complete decoded record after failed fsync cannot satisfy an idempotent ACK
until a successful resync certifies its current file generation. Incomplete
unacknowledged tails can be truncated; complete malformed or wrong-kind records
and migrated log loss fail closed. A checkpoint retains every previously ACKed
floor, and publishing it requires the directory barrier before new ACKs.
The bounded slice uses two floor kinds and three abstract journal positions;
it does not prove payload/operation verification, byte framing, platform fsync
semantics, the exact file-size bound, or implementation conformance. Those
remain Rust and restart/corruption test obligations. The canonical replicated
metadata journal and the two logical floor meanings are unchanged.

The `dfs_authored_journal_batch_safety.safety` invariant refines final create
publication through the unchanged ordered namespace-commit and terminal-intent
records. Physical bytes, certified durability, table visibility, and the root
owner's admission certificate remain distinct. Failed append/sync, replacement
handles, process reopen, segment rollover, table loss and preapply rollback
cannot authorize a new reply before the required sources and projection are
restored. The initial intent/receipt/prepare prefix and operation authorization
are assumptions of this slice; it does not model initial-group batching or
prove filesystem/Rust conformance. The registered case is
`dfs-authored-journal-batch` (depth 10). Unrestricted bounded verification passed
at depth 10; depth 14 timed out without a result. All 23 witnesses were reached
in 5,000 simulation traces of 32 steps with seed `0x51a0f04d20260927`; longer
completion witnesses are simulation evidence. Rust fault tests additionally
cover checkpoint/legacy compatibility and pending semantic-floor admission.

The `dfs_initial_prepare_batch_safety.safety` invariant separately refines the
ordinary-create pending-intent, exact-receipt, hidden-prepare group (`I,R1,R2,P`).
It models two selected receipts (`W=2`), at most two attempts, partial complete
and torn prefixes, uncertain sync, new handles and process reopen, exact table
restoration, and trailing prepare-reference replay. The eligible create subset
prevalidates parent/exclusive expectations before append; initial records cannot
publish the namespace. Its base token may be absent or equal the supplied hidden
object version; durable child/path absence is independently checked. The model
abstracts that eligibility predicate rather than representing a base field. Existing compatible active intents and exact Prepared
retries may enter the group. Terminal/compacted history, history backfill, bulk,
retained/advisory state and attached completions retain opaque serial fallback;
this slice does not prove those fallback implementations. Full-shard operation
authority, byte framing and segment rollover remain separate obligations.

The registered case is `dfs-initial-prepare-batch`, with default verification
depth 8 and unrestricted `step`. That bound passed in 211.551 seconds (Apalache
reported no violation in 210142 ms) with a 2 GiB JVM and two active processors.
Depth 10 is **incomplete**: the first attempt failed translation because Apalache
requires constant integer ranges; after replacing the sequence-range equality
with an equivalent cardinality/bounds invariant, the unchanged unrestricted
model timed out at 300.002 seconds. Neither attempt is a depth-10 pass. A finite
set of `h` integers within `1..h`, with cardinality `h`, is exactly that range;
the separate frame cardinality still rejects duplicate sequence ownership.

Simulation with seed `0x51a0f04d20260927`, 5,000 traces and 32 steps passed safety.
The first unrestricted run reached 25/26 witnesses, missing healthy grouped
completion. The separate `coverageStep` run reached all 26; it biases unchanged
`step` actions and does not constrain verification. Longer grouped completion
and recovery witnesses remain simulation evidence, not depth-8 proof. The
registered copies change only comments and the import path from the checked
artifact inputs. Source/Rust conformance is a separate validation boundary.

```bash
QUINT_CASE_FILTER=dfs-initial-prepare-batch QUINT_REQUIRE_WITNESSES=1 \
  QUINT_RUN_MAX_SAMPLES=5000 QUINT_RUN_MAX_STEPS=32 \
  QUINT_RUN_SEED=0x51a0f04d20260927 bash scripts/formal/check-quint.sh coverage
_JAVA_OPTIONS='-Xmx2g -XX:ActiveProcessorCount=2' \
  QUINT_CASE_FILTER=dfs-initial-prepare-batch QUINT_CASE_TIMEOUT_SECONDS=300 \
  bash scripts/formal/check-quint.sh verify-fast
```

The `dfs_metadata_routing_hint_recovery_safety.safety` invariant is a target
refinement for unsynced creation of derived object-to-shard routing hints after
canonical pending intent and exact receipt durability. Missing, malformed,
foreign-object, and wrong-shard hints cannot supply or veto physical ownership;
real I/O failures remain errors. Recovery retains hidden creates and their
unique exact receipt owner, keeps compatible active intents after a move,
hides superseded source intents, and derives historical ownership from canonical
physical state. Reopen does not publish a namespace entry until the unchanged
parent/exclusive, receipt, prepare, and authority requirements are met. The
bounded two-shard slice abstracts those existing publication proofs and does
not prove byte-level filesystem behavior or Rust conformance. Its case is
`dfs-metadata-routing-hint-recovery` (depth 8); direct-read I/O, corrupted hints,
cross-shard ownership, and reopen-before-visible completion require Rust tests.

The `dfs_metadata_retiring_lane_replica_ingest_safety.safety` invariant
separates the retiring lane's author gate from its replica-maintenance gate.
N1's durable marker names N2 at activated epoch 3; after a chained committed
rotation names N3 at epoch 4, N1 remains a configured replica while staying
author-fenced. A contiguous journal suffix or exact authoritative snapshot from
the exact current N3/epoch-4 authority may advance N1's replica prefix without
binding author authority because the activated marker epoch is not newer than
the current source epoch. Pending-marker, epoch rollback, future/uncommitted
epoch, forged-node, removed-replica, foreign-shard, noncontiguous,
arbitrary-snapshot, foreign-filesystem permit, and mixed-filesystem payload
evidence is rejected without advancing the replica. The
Rust refinement additionally preserves the activated marker across snapshot
archival, so accepted repair cannot erase the durable fence. The refinement
exposes this exception only through a validated permit carrying the exact
filesystem, shard, source, and epoch; the raw replay and snapshot APIs retain
the ordinary frozen-author gate. Before mutation, every journal record or
snapshot record family must also belong to that permit's filesystem. Object-RPC
signer identity, in-service transport identity, wire filesystem/epoch identity,
and routed response dispatch/current assignment equality are checked before
that permit is constructed.

Two small target-hardening slices cover background replica maintenance.
`dfs-replication-plan-freshness` races background replication with
foreground authority acknowledgements: a delivery hint may retire obsolete
work but never acknowledges a probe, a rejected plan retries without a
snapshot when the primary has advanced, and unchanged divergence still
reaches snapshot repair. `dfs-replica-projection-refresh` lets a requester
observe replica progress written by another local handle; the notice is only
a hint, and dispatch requires the cursor and namespace guards to be replaced
together from exact durable history under the current authority epoch.
Transport, disk failure, payload replay, and concurrent threads remain Rust
test obligations.

The complementary content-binding slices are target refinements for small
content bindings. `dfs-content-binding-publication` publishes one complete
frame containing payload, exact manifest, and causal reservation with one
sync: a visible record after a failed sync is not certified, reopen validates
and synchronizes the surviving complete prefix, a torn tail truncates while a
complete malformed record fails closed, and index loss is rebuilt from
durable records. `dfs-content-binding-lifecycle` owns generations: layout
activation waits for admitted legacy writers and GC to drain, stale handles
cannot write the new layout, compaction keeps held readers on their old
generation until release, and new acknowledgements wait for directory
durability. Domain GC permission, freshness, and grace are explicit inputs.

The `dfs_content_accounting_derived_stats_safety.safety` invariant separates
authoritative quota admission from rebuildable observability counters. A status
read may persist a derived snapshot, but its existence cannot change no-quota
write admission or add synchronous accounting to the content-write path.
Quota-enabled admission still synchronously reserves the exact accepted bytes,
oversized requests leave content and accounting unchanged, and namespace
metadata cannot become visible before its blob is durable. Crash may discard
only derived counters; a durable-blob scan restores exact stats without
changing namespace visibility or quota authority.

The `dfs_repair_queue_refresh_safety.safety` invariant is a target refinement
for retaining the materialized node-local repair queue across maintenance
passes. A cached view is reusable only when its snapshot/log content
fingerprint matches the exact durable byte generation and no failed sync
remains unresolved. Fingerprints are not wall-clock timestamps or sequence
numbers. Replacement at the same sequence invalidates the resident observation
token, so a stale maintenance reconciliation cannot overwrite newer pending
records or retry state. The slice covers external append/replacement,
truncation/corruption, failed complete/torn append, resync before no-op retry,
restart, unsupported fingerprint evidence, and duplicate enqueue backoff.
Hash collision resistance and the existing serialized owner discipline are
assumptions; this model does not make independent simultaneous process writers
safe, bound real disk latency, or model scheduling fairness. The registered
case is `dfs-repair-queue-refresh` (depth 6; 5,000 simulation traces of 24 steps,
all 18 witnesses reached with seed `0x51a0f04d20260711`). The replay candidate
must hash its actual decoded snapshot and framed-log buffers, including covered
frames and recoverable tails; separate before/after scans cannot reject an
A-to-B-to-A substitution during replay. Eleven focused Rust regressions cover
these cache, observation, and retry boundaries; the full DFS library suite
passes with 2,038 tests and one ignored test.

The `dfs_bounded_intent_scan_safety.safety` invariant is a bounded target/refinement
for `dfs-bounded-intent-maintenance-scan`: one physical shard is enumerated per
invocation, and terminal, missing, rejected, and duplicate physical rows consume
the raw-record budget before active filtering. A per-shard upper key is captured
on entry so continuing appends cannot starve later shards; EOF wraps the sweep.
The advisory cursor belongs to the daemon cache owner and survives a cancelled
service shell. Completed-page compare-and-update cannot overwrite a different
current cursor or owner generation; cursor progress never supplies recovery
permission. Exact current authority and intent identity remain required before
an effect. Unrestricted safety passed through **6 transitions**; 5,000 simulation
traces of 40 steps reached all six witnesses with seed `0x51a0b00d20260928`.
Later complete sweeps and recovery are simulation evidence, not temporal liveness
proofs. The finite model uses two shards and dense key positions with raw budget
two. The Rust implementation uses physical ordered keys and at most one additional
lookahead; table opening/refresh, cross-shard listability checks and recovery I/O
are outside the enumeration work bound. Eventual revisit assumes eventually
stable topology/identity and sufficiently many noncancelled invocations; neither
arbitrary cancellation nor a generic filtered-scan fallback is proved bounded.

The `dfs_content_catalog_causal_cut_safety.safety` invariant is a target
refinement for reservation reconciliation against delayed catalog pages. A
store-incarnation-bound frontier advances durably before the first root page;
reservation writes share its local lock and record the current epoch. Negative
release requires a known matching incarnation and an epoch strictly older
than the catalog's original cut. Installation and continuation pages cannot
refresh that cut. The bounded slice exposes both the old installation-time
race and the remaining failure of a scan-start timestamp when a local clock
rolls back and recovers before installation. It also covers legacy positive
coverage, conservative unknown generations, duplicate single/batch roots,
remote clock skew, restart, rejected old writers, grace, and catalog expiry.
It does not model blob deletion, live-operation lease policy, metadata quorum,
or real-time grace measurement; those remain separate contracts. The registered
case is `dfs-content-catalog-causal-cut`.

The `dfs_packed_index_append_batching_safety.safety` invariant is the bounded
refinement gate for replacing the packed-blob index log's two logical append
calls per record with one contiguous append for the whole batch. Two records
each carry the concrete four-byte little-endian length field and a symbolic
two-byte body. The reference and optimized encoders produce the same byte at
all twelve positions. Every durable cut from zero through the full stream,
including cuts inside either length field or body, therefore presents the same
prefix to replay. Both readers retain exactly the longest complete-record
prefix and truncate the same final partial record: a torn first record recovers
nothing, an exact first boundary commits one record, and any torn second record
keeps only the first. The healthy two-record projection reduces four logical
append invocations to one while preserving the full stream.

The model does not make a single-syscall or atomic-write claim: `write_all` may
still retry after a partial operating-system write, and every byte cut is
conservatively admitted. The Rust refinement must concatenate the unchanged
`u32::to_le_bytes()` prefix and encoded record bytes in order, issue one
`write_all` for that contiguous buffer, preserve the existing synced-versus-
relaxed `sync_all` boundary, and retain the current replay/truncate behavior.
Variable record sizes, allocation failure, concurrent appenders, filesystem
ordering, packed-segment durability, and corruption before the final
incomplete record remain concrete conformance and separate proof obligations.

The `dfs_namespace_transaction_body_transport_safety.safety` invariant is the
bounded composition gate for carrying one same-shard
`DfsNamespaceTransaction` through the canonical authenticated object-RPC
session. Every modeled transaction uses that same session call with one
semantic attachment; neither encoded byte count nor a control-head limit
selects another RPC path. Only a complete length-, digest-, and SOBJ-valid
attachment can reconstruct the original transaction identity and exact
file/file/parent record order. Dispatch then applies the reconstructed
transaction at most once, with either all three records visible or none. A
corrupt, truncated, or cancelled attachment terminates without reconstruction
or namespace mutation, and a duplicate application attempt leaves the one
atomic result unchanged.

The Rust refinement is the composition of a `DfsRoutedCall` attachment with the
single `ObjectRpcCall` contract. The sender carries a typed
`CommitLocalEventualBatch` descriptor and its encoded namespace transaction as
the semantic request attachment. The receiver validates the attachment, the
authenticated caller-owned file transitions, their expected base versions,
and one durable content receipt per file before committing the receipts and
visible metadata atomically on one metadata shard. This is not a second
transfer API or a payload size optimization. Concrete transfer-frame chunking,
flow-window scheduling, relay authentication, retry behavior, resource
exhaustion, and metadata-backend crash recovery remain in their existing
network and namespace-transaction proof slices.

The `dfs_operation_authority_snapshot_reuse_safety.safety` invariant is the
core-algorithm target for removing repeated cluster-KV snapshot loads and local
daemon discovery from one canonical DFS operation. Each attempt captures one
immutable exact DFS-authority projection and one authenticated local-daemon
generation. Routing, metadata prepare, and publication reuse that context; the
authority validates its exact projection revision, primary, and epoch again at
prepare and publication. A stale context therefore fails closed without a
visible write, and only that typed fence permits one fresh-context retry of the
same operation. The projection revision advances only when DFS shard/handoff
authority changes, so unrelated global KV commits do not introduce false
refreshes. A two-rotation trace returns to the original primary name but is
still fenced by the newer projection revision and epoch. A daemon restart is
handled symmetrically by its local identity generation. This slice models only
the canonical strong path: it introduces no local-first or degraded mutation.

The `dfs_metadata_primary_handoff_activation_safety.safety` invariant refines a
role-only rotation over the unchanged member set `{N1,N2,N3}`:

- direct KV-only transfer and mixed primary-plus-membership changes are
  rejected; N2 must already be a replica in the exact topology-preserving ring;
- a write admitted after handoff begin but before N1's durable fence remains
  valid and is included in the frozen prefix;
- partial exact-prefix acknowledgements remain pending and never use ordinary
  quorum fallback;
- N2's acknowledgement counts only after its exact handoff-bound marker is
  durable and its response echoes the handoff identity;
- activation requires the complete member set, retiring fence, promoted marker,
  and exact proof. N1 records completion without binding and remains fenced;
- N2 remains unbound across crash, reopen, or a lost activation response until
  it freshly verifies the active successor and exact local evidence; and
- stale N1 lanes cannot consume N2's proof, completed inactive markers remain
  replaceable for a later reverse rotation, and a new-primary write is reachable
  only with one writable authority.

This focused model composes the already-proved retiring fence and exact-union
primitive with the missing promoted-instance activation boundary. It proves
bounded safety and reachability, not unbounded transport fairness.

The `dfs_metadata_primary_handoff_cursor_preflight_safety.safety` invariant
refines the healthy retiring-primary preflight that precedes that protocol.
After replica expansion, a later primary write may make the last certified KV
watermark stale even though ordinary replication has durably advanced the
promotion target's sender cursor to the live head. The optimized preflight may
use that cursor only to park a pending handoff; it does not move authority,
install a fence, or contribute an exact acknowledgement. A late admitted write
is captured by the later retiring-primary fence. The target then counts only
after a fresh exact-prefix probe at that fenced head, and activation still
requires the exact full configured ring. The model independently permits a
caught-up cursor with a wrong fingerprint, proves that its probe rejects, and
reaches activation only after authoritative snapshot repair and a new exact
probe. Cursor-only failover when the retiring primary is unavailable remains
outside and forbidden by the Rust refinement.

The `dfs_authority_routing_safety.safety` invariant combines these properties:

- the routing shard and optional same-primary companion dispatch shard each
  retain their committed primary by epoch;
- an explicit primary reported by a rejected response is followed only when a
  refreshed committed view names it for every shard in the captured dispatch
  set. A partial grouped-shard move remains uncorroborated;
- another affected shard may have a distinct primary without blocking a valid
  dispatch completion, and merely matching that foreign primary never
  corroborates the redirect;
- an uncorroborated namespace-lookup redirect may fall back only to an existing
  live local hit on a node that remains a replica in the refreshed dispatch
  assignment. Outsiders and removed replicas fail closed, as do missing and
  tombstoned local rows, so fallback cannot manufacture NotFound;
- remote and corroborated-local authority completions retain exact committed
  dispatch evidence for their recorded epochs;
- pending redirect counters stay inside the Rust limit
  `DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT`;
- terminal completed or failed routes leave no pending routed state behind;
- a route cannot be both completed and failed;
- self-routes stay disabled;
- late or mismatched responses are observations only.

This maps to routed namespace response settlement
(`validate_routed_namespace_authority_redirect`), refreshed dispatch-shard
corroboration (`refreshed_primary_after_authority_redirect`), and
`lookup_sifs_path_distributed_synced`. The current focused redirect regression is
`native_stale_authority_redirect_keeps_original_identity_after_lost_reply`;
no current focused test covers grouped-shard corroboration or the live-hit
fallback. The foreign affected
shard abstracts the destination half of a coordinated multi-shard operation;
it does not bypass that coordinator's own protocol. The model does not prove
progress: a route may time out, hit the redirect limit, or fail because the
corroborated primary is unreachable.

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

This maps to the first-visible-create branch of
`NetworkedDfsService::apply_committed_intent` (the
`backend.get_object(...).is_none()` test), which publishes through
`commit_ready_intent_batched` and the backend's V7 `commit_prepared_operation`;
the `DfsNamespaceTransaction { objects, versions }` record shape; and the
namespace transaction table planners used by normal apply and recovery. It
intentionally models a single shard-local transaction; the sharded backend's
routing of objects and versions to per-shard transactions is a separate layer.

The `dfs_namespace_planner_equivalence_safety.safety` invariant is a bounded
target-hardening proof for removing a metadata hot-path clone and replay:

- the reference planner clones the table state, applies a parent/child plan to
  scratch tables, then replays the prepared operations against the original;
- the optimized planner preapplies each prepared operation in place and appends
  the already-applied batch without replaying it;
- a directory move plans parent before child, while recursive deletion plans
  child before parent, so the second operation must observe the first one's
  table effects rather than a stale input snapshot;
- both planners produce identical prepared operations and planning state;
- after success they produce identical object, path, and dentry tables, plus
  exactly one journal entry and table batch;
- if the journal is durable but table-batch append fails, optimized rollback
  restores the original parent/child tables, leaves no transaction table row or
  partial visibility, and matches the reference journal-only failure state; and
- the optimized plan eliminates one whole-table clone and the second
  application pass in the bounded cost model.

The slice covers exactly one two-object parent/child transaction. Recovery from
the journal-only failure cut is the adjacent journal-tail model; version and
advisory-lock rows and wider transactions remain explicit non-goals until the
optimized Rust path exists.

The `dfs_namespace_expectation_snapshot_validation_safety.safety` invariant is
a bounded target-hardening proof for constructing namespace-prepare
expectations from one immutable shard-table snapshot:

- the object-version and path-occupant expectations are projected from the
  same snapshot;
- the commit-time validator accepts the unchanged relevant projection even if
  unrelated table state advanced;
- an intervening object-version or path-occupant difference rejects without a
  visible commit, including when both differ; and
- an exact relevant projection cannot be falsely rejected.

This maps to `namespace_prepare_expectations` and the
`validate_namespace_prepare_expectations` call under the consumed publication
guard's metadata lock in `commit_prepared_operation_owned`. The bounded slice
covers one object, one path, and distinct non-restored conflicting values. It
does not model ABA, wider transaction footprints, overlapping prepares,
owner-epoch fencing, journal durability, or namespace materialization.

The `dfs_namespace_prepare_footprint_index_equivalence_safety.safety`
invariant is a bounded refinement proof for replacing the retained prepare-
history scan in `validate_namespace_prepare_does_not_overlap`:

- the reference validator scans both retained operation rows, ignores its exact
  operation id, filters to `Prepared` rows in the candidate filesystem, and
  classifies object overlap before path overlap;
- the optimized validator performs one filesystem-qualified object-owner lookup
  and one filesystem-qualified normalized-path-owner lookup, with ownership by
  the same operation treated as an exact retry rather than a conflict;
- a newly prepared row installs both owners atomically, while rejected object
  or path conflicts mutate neither history nor either index;
- commit and abort retain their terminal prepare rows but release exactly their
  active footprint, allowing another operation to reuse the same keys;
- identical object and path ids in different filesystems remain independent;
  same-filesystem disjoint footprints may also proceed concurrently; and
- restart reconstructs both sparse maps from exactly the retained `Prepared`
  projection, so terminal rows cannot reacquire ownership.

The bounded profiles contain one object and one normalized path apiece and
cover object-only overlap, path-only overlap, full overlap, and full separation.
The cost projection replaces four worst-case history-key comparisons with two
direct owner lookups. Wider footprint sets, normalization conformance, payload
equality, persistence rollback, expectation validation, and epoch fencing are
unchanged prerequisites or follow-up refinements.

The Rust refinement maintains set-valued object/path owners incrementally when
a `PutPreparedOperationV7` table operation installs a prepared record
(`put_prepared_operation_indexed`), releases them when a record leaves
`Prepared` or is evicted, removes the prior state before exact-id replacement,
and rebuilds the derived maps after snapshot loading and preapplied table
rollback. Metadata publication uses
`PrepareOperationV7`/`CommitPreparedOperationV7`. The
tables carry a test-only history-scan oracle
(`namespace_prepare_footprint_indexes_match_history`) and admission counter,
but the focused reference-equivalence and snapshot-then-log-replay reopen
regressions are absent, so this index
currently has no Rust differential test.

The `dfs_metadata_journal_tail_recovery_safety.safety` invariant isolates
same-instance recovery after that durable-journal/table-materialization cut:

- the durable journal and materialized tables are separate contiguous prefixes
  of a bounded two-entry sequence, and materialization never outruns the
  journal;
- a healthy write admits from equal tails, durably appends EntryOne, and
  materializes the same exact prefix;
- if table-batch append fails after journal durability, preapplied live tables
  roll back to the previous materialized prefix while the durable EntryOne tail
  and its replay projection remain intact;
- the next begin-write detects that gap without increasing either the admission
  or append counter, then replays the exact durable prefix before admitting
  EntryTwo;
- every admitted write and durable append records equal journal/materialized
  base tails, so a new append cannot cross an older materialization gap. The
  append itself may create only the next one-entry gap while materialization is
  pending; and
- crash/reopen from the same cut and same-instance recovery both rebuild the
  materialized and live-table proof from the identical durable replay
  projection.

The slice proves safety and bounded causal reachability for one healthy append
and one failed-first-append/recovery path. It does not model torn journal
records, lower-level persistence failure, arbitrary replay failure, concurrent
writers, or more than two entries.

The `dfs_namespace_tombstone_overlay_equivalence_safety.safety` invariant is a
bounded target-hardening proof for removing the remaining full-table clone from
directory-tombstone precondition validation:

- the reference validator clones all materialized object metadata, replays
  transaction objects in the planner's exact tombstone-then-live order, skips
  compacted matching tombstones, and accepts only when the terminal direct-child
  set is empty;
- the sparse validator seeds only candidate directories' current direct-child
  path owners. Each non-skipped transaction object loses its prior child
  placement and evicts the current target-slot owner; a live object then owns
  that slot while a tombstone leaves it empty;
- reference terminal candidate-slot owners, direct-child sets, and
  accept/directory-not-empty outcomes are identical for parent-only delete,
  child-plus-parent delete, child move-out, live-in-place, move-in, new-child,
  same-path replacement, new-id tombstone and live replacement, duplicate-id,
  two-candidate-directory, and compacted-child cuts;
- candidate validation retains transaction order, so when two candidate
  directories are nonempty the same first directory error is selected;
- a replayed object removes a compacted marker at its target, and a later
  formerly matching tombstone is then replayed rather than incorrectly skipped;
- transaction input order is preserved within each planner partition, and all
  applied tombstones precede all applied live rows; and
- the sparse cost projection performs no whole-table clone and its only range
  scan is the candidate's initial direct-child dentry set, excluding two
  explicit unrelated metadata rows. Per-transaction-object lookups remain
  bounded point reads rather than unrelated metadata scans.

This slice fixes two candidate directories, two child slots per directory, one
outside slot, and three transaction slots. It covers duplicate ids across the
tombstone/live partitions and path-slot ownership, but advisory-lock validation,
arbitrary path normalization, and concrete dentry-key/hash collisions remain
explicit non-goals. Those require enforced canonical-path invariants or Rust
differential conformance tests around the optimized validator.

The `dfs_intent_index_equivalence_safety.safety` invariant is a bounded
refinement proof for replacing coordinator-wide historical intent scans with
sparse intent indexes:

- the reference validation scans every retained intent, while the optimized
  validation reads the full filesystem/object `awaitingByObject` view and
  `claimedByTarget`;
- exact object/target duplicates retain the reference error precedence over a
  broader pending-object conflict;
- the full-key awaiting-object index points to exactly the one intent awaiting
  receipts, the aggregate object-id set equals all awaiting intents across
  filesystems, and each target-claim set equals the retained
  awaiting-or-committed history;
- reference scan results and indexed lookup results agree for every bounded
  full key and target, and the filesystem-agnostic
  `has_open_intent_for_object` and `has_awaiting_intent_targeting` helpers
  agree for every object id and target after start, commit, abort, and
  failed-start rollback;
- commit removes the awaiting-object lookup but retains the target claim, since
  the current implementation rejects exact duplicates of historical committed
  intents;
- abort removes its entries from both indexes, while a failed durable start
  restores the intent history and both indexes to their pre-attempt values;
- hydration of previously unseen awaiting, committed, and aborted intent ids
  rebuilds exactly the applicable sparse views. The target view is set-valued
  because committed restore bypasses new-intent conflict validation, while
  awaiting restore still enforces one active intent per object; and
- same-object/target replacement of an existing durable id first removes the
  old state's index entries, then installs exactly those required by the new
  awaiting, committed, or aborted state; and
- two awaiting intents for the same object id in different filesystems remain
  distinct in the full-key view and both appear in the aggregate view; closing
  either one preserves the other helper-visible open intent.

The slice contains two bounded filesystems sharing one object id, plus a second
object id, so cross-filesystem aggregation is explicit. It does not model
committed-base validation, replica policy, receipts, repair jobs, or local-first
behavior; those remain unchanged prerequisites or separate models.

The `dfs_receipt_bucket_equivalence_safety.safety` invariant is a bounded
refinement proof for replacing coordinator-wide historical receipt scans with
per-intent receipt buckets:

- the reference state is a flat `(intent_id, replica_node_id)` map, while the
  optimized state is an intent-to-replica bucket map;
- every bounded receipt location and its full intent, object, version, replica,
  and durable-state content agree between the two representations;
- durable-replica sets, durable counts, and `receipts_for_intent` results agree,
  including the BTree replica-node ordering of returned receipts;
- recording or restoring the same deterministic durable receipt overwrites the
  same key without changing either representation or either count;
- a new receipt changes exactly one intent/replica location and leaves every
  other node and intent bucket unchanged;
- runtime recording rejects closed intents and unexpected replicas, durable
  restore may hydrate a retained closed intent, and every rejected operation
  leaves both representations unchanged; and
- the bounded cost projection narrows an intent query from all nine historical
  keys to the three replicas in one intent bucket.

The slice does not model replica-policy selection, intent commit, repair-job
maintenance, persistence failure, or local-first behavior. Those remain
unchanged prerequisites or separate proof layers.

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
- after successful cleanup, a migration-eviction tombstone that remains on the
  retired physical source shard cannot shadow the same logical path's live row
  on the path-selected target shard, even when a receiving replica's derived
  object-shard index is missing or still names the retired source;
- the bounded old service-overlay path retains the Docker counterexample: a
  global tombstone-first lookup treats the live `/services` directory as
  deleted after the stale root index also hides the selected services row,
  commits a resurrection that finally rewrites the local index, then reports
  the unrelated root-shard primary during the parent touch. The refreshed
  services-shard dispatch rejects that redirect, but the mutation has already
  committed;
- the shard-aware lookup makes the same mkdir-all a successful zero-mutation
  no-op with no authority redirect. It repairs a stale or missing replica index
  only after the path-selected live row wins, so that derived accelerator is a
  convergence output rather than visibility authority. Missing or mismatched
  target-row evidence remains governed by the existing sharded-backend failure
  states;
- idempotent target replay does not create a second visible path.

This maps to `LocalShardedMetadataBackend::commit_prepared_operation`, which
publishes the shard-local V7 operation and then runs
`reconcile_object_shard_indexes`; `object_is_visible_from_shard`;
`get_object_by_path`; the service-level `lookup_metadata_path_raw` overlay;
the cross-shard job's `publish_cross_shard_source_eviction`; and routed
namespace redirect settlement. The regression
`routed_mkdir_all_prefers_live_path_shard_over_late_source_tombstone`
covers a retired-source tombstone losing to the live path-selected row after a
move and mkdir-all. The earlier backend helpers
(`apply_namespace_transaction`, `write_transaction_object_shard_indexes`,
`put_cross_shard_tombstone`) and their target-failure and tombstone-failure
regressions are absent, so those failure cuts currently have no focused Rust test.

The model deliberately tracks a nuance of the former implementation: if the
target shard transaction succeeded but the object-shard index write failed,
the API reported failure and sharded lookup still resolved to the old source
object while the target row stayed hidden until an explicit retry wrote the
index. Whether the V7 publish-then-reconcile path retains that window has not
been re-verified. It is not modeled as a safety violation, but it is a
recovery/operability gap to revisit if we want automatic convergence after
router-local index write failures.

The replica extension is bounded to one moved object, the root/services shard
pair, and one mkdir-all observation. It proves visibility precedence and safe
derived-index repair, not unbounded repair scheduling, arbitrary shard graphs,
or any local-first/degraded mutation path.

The `dfs_namespace_create_transaction_identity_safety.safety` invariant
isolates the retained-prepare identity boundary exposed after a cross-shard
directory move. Both modeled create generations keep the same stable object id
and `/users/proj2` path, while the selected source tombstone is recreated at a
newer object version with a different immutable payload. The legacy
kind-plus-object/path id collides with the original retained mkdir and is
rejected without changing the tombstone or mutation count. The fixed id binds
the object version, so the resurrection is admitted under a distinct retained
id; an exact retry derives the same version-bound id and payload, is recognized
as replay, and adds no second mutation. The slice is bounded to one directory,
path, move/tombstone transition, resurrection, and exact retry. It does not
model arbitrary rename trees, history compaction, persistence failure, shard
authority changes, or local-first operation.

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
and the regression tests
`repair_command_accepts_current_replica_after_metadata_primary_moves`,
`repair_command_accepts_newer_authority_shard_when_receiver_view_is_stale`, and
`repair_command_rejects_unknown_object_from_non_primary_metadata_source`. The
former stored-intent regression
(`repair_command_accepts_metadata_primary_for_stored_intent`) is absent, so the stored-intent metadata-primary branch currently has
no focused Rust test. The model does not cover the later content-hash check,
conflicting pending-write check, or lost-authority retry suppression after a
repair response.

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
service-level checks in `NetworkedDfsService::apply_committed_intent`, which
retains the pending write's effective repair mode, and
`NetworkedDfsService::stage_missing_desired_replicas_for_intent`. The model
does not prove repair progress, placement ranking, owner-inclusion
normalization, transport delivery, content-hash validation, or retry timing.

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
the visible commit in `NetworkedDfsService::apply_committed_intent` and
`commit_stored_prepared_operation`, and
`NetworkedDfsService::reconcile_stale_write_intents_bounded`. It also reflects
the rule in `docs/dfs-architecture.md` that the canonical DFS has no
conflict-record path; conflict classification belongs only to the
unimplemented local-first extension. The model does not cover that extension,
path-level POSIX conflict surfacing, or request-map cleanup.

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

For its refinement to the current FUSE adapter, the model's returned-write
transition corresponds to successful canonical DFS publication; it does not
include a successful FUSE write syscall whose bytes remain only in the
process-local dirty buffer. That prepublication interval, including loss on
mount-process crash, is outside the invariant. FUSE `fdatasync` and `fsync`
drain those buffered writes before entering the modeled barrier.

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
a restart-unique identity and an empty rolling-upgrade-safe journal batch.
Ordinary FUSE writes can return from a process-local per-handle dirty buffer;
`flush` and `release` publish that handle's pending ranges but do not issue a
durability barrier. File `fdatasync` and `fsync` publish all dirty handles for
the object before entering the modeled barrier. `fdatasync` has a distinct API
kind but currently uses the stronger full-file physical metadata barrier.
Durable caller-selected sync ids and stored outcome-unknown responses remain
target hardening. The Rust checkpoint proof currently hashes the full
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

The `dfs_metadata_checkpoint_fanout_safety.safety` companion isolates the
work-conserving refinement inside those ordered durability barriers. It permits
both non-primary catch-up requests to remain in flight, accepts their responses
in either order, advances only from a majority acknowledgement set, and scopes
a late sibling response to its old checkpoint. The source-prepare,
destination-adopt, and source-finalize barriers remain strictly ordered; only
independent replica transport is concurrent. Rejected replicas may be retried,
and bounded witnesses cover two-slot fan-out, an N3-before-N2 acknowledgement,
quorum fallback with a late N2 response, stale-response isolation, retry, and
ordered completion. This finite three-node slice abstracts RPC bytes, timeouts,
membership changes, and the full-shard grace timer.
The Rust refinement is `send_metadata_journal_batches_direct`. Checkpoint
catch-up uses the two-slot `DFS_METADATA_CHECKPOINT_DIRECT_MAX_INFLIGHT` cap and
the short `DFS_METADATA_CHECKPOINT_DIRECT_FANOUT_BUDGET`; operation-bound
authority fan-out uses its cluster-sized inflight cap and a larger outer cap,
while the caller supplies the tighter small- or large-prepare deadline.

The `dfs_metadata_shard_concurrent_dispatch_safety.safety` companion defines
the daemon mutation-lane scheduler above the already-concurrent object-RPC
router. Requests carry immutable affected-shard footprints and FIFO tickets;
one atomic admission transition acquires every shard in a single-shard or
cross-shard footprint. Disjoint shards may execute concurrently, same-shard
requests retain ticket order, and a legacy request without an exact footprint
uses an all-shard exclusive lane. Topology advancement requires active lanes
to quiesce and cancels queued work from the old generation. Bounded witnesses
cover disjoint overlap, same-shard blocking and release, atomic cross-shard
ownership with an unrelated users-lane request, exclusive legacy fallback,
caller detachment retaining ownership until the blocking work finishes,
post-detachment release, and post-topology admission. This finite scheduler
model is symbolically checked through six scheduler steps and abstracts Tokio
permit wake order, request parsing, backend I/O, and RPC bytes.

The model also separates an exact request's shard reservation from its global
read fence. Once a global writer queues, an exact waiter may retain only its
shard reservation; it cannot extend the writer's wait with another read guard.
The `witnessQueuedGlobalBypassesShardOnlyWaiter` trace covers this ordering and
matches Tokio's write-preferring `RwLock` after the active readers quiesce.

The current daemon is a conservative refinement of the single-shard and global
parts of this model: validated authority-stamped requests use exact lanes,
same-shard waiters queue before blocking-worker dispatch, and ambiguous or
multi-path work uses the exclusive lane. Atomic multi-shard admission and
explicit queued-command cancellation across topology generations remain target
hardening; those witnesses must not yet be read as Rust conformance evidence.

The `dfs_daemon_read_blocking_admission_safety.safety` companion specifies the
target boundary between async object RPC and blocking DFS read-service lanes.
Its fixed bound has two read slots and two injective lanes: the first two reads
may enter distinct blocking workers, while a third remains an async waiter with
no worker or lane. An unrelated mutation retains an independent admission path
when reads saturate the pool. Caller detachment retains the read slot, lane, and
worker until the blocking closure actually finishes; only that completion lets
the waiting read reuse the released lane. Six-step symbolic checking covers the
bounded release trace. The model abstracts the configured production pool size,
Tokio wake fairness, lane construction, backend I/O, and the separate mutation-
shard scheduler above; it is a refinement contract, not a wall-clock liveness
proof.

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

The `dfs_namespace_command_replay_safety.safety` invariant refines durable
primary-side namespace-request admission and the requester cancellation
boundary:

- the durable ledger key is transport requester plus request id, while the row
  retains the semantic operation independently of response journal options;
- a fresh mutating request durably enters `Pending` before it receives the one
  execution permit;
- request-owned namespace prepares atomically materialize the visible
  transaction and `Completed` terminal result, while durable jobs and
  cross-shard sagas refine the same boundary through resumable progress;
- an exact retry returns the durable result without another namespace effect,
  including after process restart;
- reusing the same requester/request identity with a changed semantic mutation is
  rejected instead of being interpreted as new work;
- restart after admission but before a prepare leaves a recoverable `Pending`
  row; startup or maintenance recreates the execution permit for that exact
  operation without waiting for a requester;
- cancellation removes the requester-side wait but cannot retract an already
  admitted command; and
- a cancelled call retried with a fresh request id is explicitly new work.

This maps to `NetworkedDfsService::handle_namespace_request`,
`admit_durable_namespace_request`, `complete_durable_namespace_request`, the
metadata backend's namespace-request table, process restart, and fresh
request-id allocation. Named witnesses cover exact completed replay,
changed-payload rejection, cancellation, completed replay after restart, and
automatic interrupted-`Pending` recovery. The Rust regression
`durable_namespace_exact_retry_replays_completed_rename_and_rejects_id_reuse`
covers exact replay and id-reuse rejection, and
`native_namespace_request_replay_survives_actual_primary_restart` covers
completed replay across a primary restart. Startup and maintenance recovery
runs through `recover_pending_namespace_requests_bounded`, but the former
interrupted-`Pending` regression
(`durable_pending_namespace_request_recovers_automatically_after_restart`) has no current replacement.

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
- stage fan-out is work-conserving up to the fixed cap of two, so N2 and N3
  can be active concurrently but a third target cannot exceed the cap;
- each stage response is authorized by the request id stored for that specific
  target, so an out-of-order N3 success can complete while N2 remains in
  flight;
- an N2 failure clears and schedules retry for N2 only, leaving N3 active, and
  retrying N2 neither restarts nor cancels N3;
- visible publication snapshots the receipt set that satisfied `W` together
  with the committed target hash and version; and
- a later matching receipt from an already-issued desired-replica request may
  extend durable placement without changing the W receipt snapshot or the
  committed target identity;
- while the target version remains current, its content hash is stable.

The pre-existing receipt-validation portion maps to
`NetworkedDfsService::put_metadata_receipt`,
`NetworkedDfsService::record_local_pending_write_receipt_if_desired`,
`NetworkedDfsService::handle_stage_blob_response`,
`NetworkedDfsService::ensure_stage_response_matches_pending`,
`NetworkedDfsService::handle_repair_replica_response`,
`NetworkedDfsService::ensure_repair_response_matches_pending`, and
`NetworkedDfsService::handle_fetch_blob_response`. The current-location
updates were mapped to `NetworkedDfsService::mark_repaired_location`, which
is absent; that property has no current Rust
mapping. The model includes the implementation nuance that a restored repair
response may be accepted without a tracked in-flight request when a matching
repair job still exists. It also
models stale wrong-hash metadata receipts as backend state that must not become
durable repair evidence. Body framing, end-to-end finish attestation, flow
control, and replay-safe retry are now `sinomo-network` obligations and are not
proved by this DFS slice. It also does not prove retry fairness or that every
missing replica is eventually repaired.

The bounded fan-out maps to `DFS_REPLICA_BODY_FANOUT_CAP`, the bulk stage and
repair helpers in `service_outbound`, the stage/repair scheduling in
`service_repair` and `service_commit`, and the ordered per-target transport
outcomes in `message_transport` and `node_transport`. Production node, served,
and synchronous in-process transports preserve a direct zero/one-target path
and overlap up to two full-body transfers. Focused Rust tests exercise three
targets under the cap, out-of-order transport completion, and one-target failure
isolation in both sync and async implementations; retry, POSIX wait-for-commit,
and served foreground-write regressions also pass. Returned messages are still
merged after the bounded batch joins, so target-level completion overlaps even
though service processing observes the completed batch. This content-validation
slice does not decide whether a background batch may include work beyond `W`;
the quorum-aware scheduler below supplies that policy. Inline blobs and stage
patches remain sequential and outside this slice.

In the registered fixed-seed 5,000-trace run, the bounded N2/N3 fan-out,
out-of-order N3 success, isolated N2 failure, isolated N2 retry, and late N3
post-commit receipt witnesses were reached in 3,340, 1,663, 1,677, 1,677, and
1,660 traces respectively. The run found no safety violation in 6,066 ms at 824
traces/s. The registered depth-6 Apalache check also found no violation in
150,166 ms.

The `dfs_replica_quorum_fanout_safety.safety` invariant separately fixes the
scheduling policy around the visible `W` boundary. Its repair mode is the
effective mode retained by the metadata primary after transport and deadline
handling, not necessarily the caller's original flag. A caller-requested
foreground write may therefore execute as background repair at the primary
while an outer response layer owns any additional wait. The slice proves that:

- background mode reserves exactly the missing `W` stage slots rather than all
  `N - receipts` targets, has no stage sibling left when it commits, publishes
  the response at `W`, and only then starts `N-W` repair;
- foreground mode may use both transfer slots before commit, preserves an
  out-of-order healthy sibling, commits visibly at `W`, and withholds its own
  response until all `N` desired replicas are durable;
- stage and repair reservations are disjoint and each fan-out is bounded by
  two;
- `CommitVersion` is replica-cache promotion, not a durability receipt. Its
  detached dispatch begins only after the response-construction boundary,
  targets only durable staged copies, and uses a separate cap of two. A
  rejection returns the target to the pending outbox and a later dispatch
  retries it; and
- reaching all `N` receipts clears the complete write tail: stage and repair
  reservations, repair jobs, retry and attempt records, and retained pending
  write state are empty. Queued cache promotion is independent post-response
  work and is not write-tail state.

Per-target request and content-hash authorization remains the responsibility of
`dfs_repair_content_validation`; its late-stage witness is a target-isolation
case, not permission for background scheduling to reserve more than the exact
`W` deficit. This quorum-fanout slice has three nodes and bounded caps. It does
not prove unbounded replica-set progress, transport fairness, or wall-clock
throughput.

In the latest registered fixed-seed 5,000-trace coverage run, the background
`W`-deficit, background publication, background repair, foreground two-slot,
foreground wait, out-of-order sibling, foreground `N`, response-before-
promotion, promotion-start, two-slot promotion, rejected-promotion requeue,
rejected-promotion resend, and zero-write-tail witnesses were reached in 1,232,
1,232, 1,232, 2,528, 1,269, 1,259, 2,528, 1,232, 1,232, 1,269, 1,240, 1,240,
and 5,000 traces respectively. The run found no violation in 4,252 ms at 1,176
traces/s. A separate nondeterministic `step` run also found no violation across
5,000 traces of at most 20 steps in 3,890 ms at 1,285 traces/s. The registered
depth-6 Apalache check found no violation in 36,968 ms.

The companion `dfs_replica_promotion_outbox_safety.safety` invariant refines
the detached `CommitVersion` work after that response boundary. It proves that:

- each drain round attempts an exact identity at most once with at most two
  promotions in flight, so failed identities rotate behind every ready
  identity not yet attempted in that round;
- two failed identities cannot head-of-line block later healthy work;
- a `Durable` acknowledgement proves staging only and stays retryable, while
  only an exact `Committed` acknowledgement enters settled state;
- durable committed-intent and receipt evidence reconstructs the volatile
  outbox after repeated restarts without depending on a namespace read;
- cache generation replacement preserves the shared outbox rather than
  scheduling a replacement lane by cache key; and
- a repaired durable commit remains in the shared outbox when unrelated
  maintenance fails, and independent recovery still runs afterward;
- a process-retained authority observer advances the recovery token across a
  no-read topology change and forces a fresh scan;
- failed replay-safe sends enter retry cooling, cannot be dispatched before
  the delay transition, and do not block newly queued healthy work; and
- settled bookkeeping retains at most one current version per
  target/filesystem/object key, replacing an older version instead of growing
  without bound.

The registered fixed-seed 5,000-trace coverage run reached the failed-wave,
durable-requeue, durable-to-committed retry, restart gap, recovery queue,
recovered settlement, cache preservation, post-rebuild settlement,
maintenance-failure preservation, independent recovery, post-failure
settlement, topology-change scan, retained-observer token advance,
post-topology settlement, healthy-work retry bypass, delayed retry opening,
cooled retry settlement, and settled replacement witnesses in 623, 646, 646,
597, 597, 597, 615, 615, 616, 616, 616, 663, 663, 663, 633, 633, 633, and 607
traces respectively. It found no violation in 3,245 ms at 1,541 traces/s. A
separate nondeterministic `step` run found no violation across 5,000 traces of
at most 20 steps in 3,698 ms at 1,352 traces/s. The registered depth-8
Apalache check also found no violation. Two later witnesses, a not-desired
rejection that retires without commit and a repair-pending rejection that
retires after a durable prune, bring the registry to 20; the recorded run
predates them. This is a bounded four-identity
safety/refinement slice: retry cooling is an abstract delay transition, and it
does not prove transport fairness, recovery-worker liveness, wall-clock
timing, or unbounded queue throughput.

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
`NetworkedDfsService::mark_remote_read_location_missing`, and its durable-prune
gate `NetworkedDfsService::metadata_effect_scope_for_object`, which admits a
metadata effect only on the shard primary or without a control plane. The
model is deliberately precise about an implementation nuance:
`repaired_location_count` and issue `repaired = true` mean the service ran the
repair path, but durable metadata is pruned only if the local node can write
the metadata shard. It does not prove distributed repair scheduling, network
fairness, cursor persistence, or that repeated lifecycle maintenance will
eventually revisit every page.

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
QUINT_CASE_FILTER=network-timed-transport,dfs-pending-requests \
  bash scripts/formal/check-quint.sh verify-full
```

Randomized runs use a fixed seed by default. Important branches expose named
witness predicates; coverage mode raises the sample count and fails if any
registered witness is not reached:

```bash
QUINT_CASE_FILTER=raft-kv,raft-protocol,membership-raft-config,dfs-posix-cross-shard,dfs-posix-advisory-locks,dfs-posix-fsync,dfs-posix-partitioned-operation,dfs-local-first-file-gateway-composition,dfs-local-first-namespace-gateway-composition,dfs-pending-requests,dfs-namespace-command-replay \
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

quint run specs/quint/checks/kv/raft_kv_safety.qnt --main=raft_kv_safety --invariant=safety --step=coverageStep --max-steps=20 --max-samples=50 --witnesses witnessBrokenNoQuorumNoEffectCounterexample --verbosity=1
quint run specs/quint/checks/network/timed_transport_safety.qnt --main=network_timed_transport_safety --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/checks/dfs/posix_namespace_safety.qnt --main=dfs_posix_namespace_safety --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint run specs/quint/scenarios/system/smoke.qnt --main=system_smoke --invariant=safety --max-steps=80 --max-samples=500 --verbosity=1

quint verify specs/quint/checks/kv/raft_kv_safety.qnt --main=raft_kv_safety --invariant=safety --step=step --max-steps=5 --backend=apalache --apalache-version=0.56.1 --server-endpoint=localhost:8822 --verbosity=1
quint verify specs/quint/checks/network/timed_transport_safety.qnt --main=network_timed_transport_safety --invariant=safety --max-steps=5 --verbosity=1
quint verify specs/quint/checks/dfs/posix_namespace_safety.qnt --main=dfs_posix_namespace_safety --invariant=safety --max-steps=2 --verbosity=1
```

`check-quint.sh` repeats this `checks/<subsystem>/*_safety.qnt` run/verify
pattern for every focused invariant suite and runs the full-system smoke
scenario from `scenarios/system/smoke.qnt`.

The Docker refinement gate is deliberately stricter than per-node DFS
readiness. Before correctness or performance scenarios run,
`docker/test-cluster.sh` polls `cluster.dfs.shard-layout` for up to the
configured convergence deadline and requires the root, services, and users
lanes each to have five replicas and together use three distinct primaries.
Each clean observation also requires zero current metadata-membership-handoff
records and an explicit zero active-primary authoritative-write-frozen count on
all six nodes. The invariant sweep is bracketed by two layout reads with the
same filesystem/shard epoch signature. Two consecutive clean observations must
retain that same signature, so the gate cannot admit the instant between
serialized handoffs or a process-unbound active lane. A timeout fails closed
and retains the last probe artifact. This gate covers live control-plane
convergence; it also completes before `SINOMO_TEST_NETWORK_ONLY=1` hands a
kept cluster to the separate shard-performance harness. It does not widen the
bounded Quint claims.

The default symbolic bound is deliberately modest. On this checkout,
the Raft, network, and DFS metadata specs use `QUINT_VERIFY_MAX_STEPS`,
defaulting to 5. The focused DFS authority-routing refinement uses
`QUINT_DFS_ROUTING_VERIFY_MAX_STEPS`, defaulting to 4. Its fixed-seed 5,000-
trace coverage run reached all ten registered witnesses: single-dispatch
follow 501 times, grouped-dispatch exact follow 496, partial-group rejection
501, foreign-affected-primary rejection 492, outsider failure 1,487, current-
replica live-hit fallback 480, removed-replica rejection 478, missing-row
rejection 538, tombstone rejection 503, and distinct-foreign-primary dispatch
completion 517. The registered depth-4 Apalache check passed on this checkout
in about 282.4 seconds.

The immutable operation-context slice uses
`QUINT_DFS_OPERATION_AUTHORITY_SNAPSHOT_REUSE_VERIFY_MAX_STEPS`, defaulting to
4. Its fixed-seed 5,000-trace coverage run reached all nine registered
single-snapshot publication, daemon-handle reuse, unrelated-KV, pre/post-
prepare authority fence, authority retry, daemon restart/retry, and primary-
name ABA witnesses without a safety violation in about 2.8 seconds. The
registered depth-4 Apalache check passed on this checkout in about 17.9
seconds. A successful one-attempt trace performs three route/prepare/publish
context uses and three peer sends with exactly one authority projection load
and one daemon discovery probe; a typed stale-context retry performs exactly
two of each acquisition across its two attempts.

The deeper per-node Raft slice uses `QUINT_RAFT_PROTOCOL_VERIFY_MAX_STEPS`,
defaulting to 1 for routine initialization-state proof while fixed-seed witness
coverage exercises ten-step protocol traces. Its 1-step Apalache check passed in
about 53 seconds; a 3-step attempt was interrupted after about 230 seconds and
is not claimed as a pass. Deeper local audits may raise it to 2 steps with a
longer `QUINT_CASE_TIMEOUT_SECONDS`. The network endpoint selection spec uses
`QUINT_NETWORK_ENDPOINT_VERIFY_MAX_STEPS`, defaulting to 4; its 4-step symbolic
check passed in about 10 seconds. The timed transport spec uses
`QUINT_NETWORK_TIMED_TRANSPORT_VERIFY_MAX_STEPS`, defaulting to 5. Neither
network case registers coverage witnesses. The route-planning, route-cache,
delivery-retry, attempt-budget, timed delivery-retry, and timed network smoke
cases and their step variables were removed with the legacy transport stack.
The read-freshness, routed namespace-freshness, and metadata-primary failover
specs use `QUINT_DFS_READ_FRESHNESS_VERIFY_MAX_STEPS`,
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
its 5-step symbolic check passed in about 21 seconds. The two-object
namespace-planner equivalence spec uses
`QUINT_DFS_NAMESPACE_PLANNER_EQUIVALENCE_VERIFY_MAX_STEPS`, defaulting to 2;
that bound explores move/delete success and table-append rollback terminals.
The namespace expectation-snapshot slice uses
`QUINT_DFS_NAMESPACE_EXPECTATION_SNAPSHOT_VERIFY_MAX_STEPS`, defaulting to 4.
Its fixed-seed 5,000-trace run reached all five unchanged, unrelated-change,
object-conflict, path-conflict, and combined-conflict witnesses. The registered
depth-4 Apalache check passed on this checkout in about 6.6 seconds.
The active namespace-prepare footprint-index refinement uses
`QUINT_DFS_NAMESPACE_PREPARE_FOOTPRINT_INDEX_VERIFY_MAX_STEPS`, defaulting to
4. It compares the retained-history overlap scan with filesystem-qualified
object/path owner lookups across prepare acceptance and rejection, exact retry,
commit, abort, and atomic restart reconstruction. Terminal prepare rows remain
in history but release ownership. A fixed-seed 5,000-trace run reached all eight
registered non-overlap, object/path conflict, terminal-history, same-operation,
full-equivalence, cross-filesystem, and recovery witnesses. The registered
depth-4 Apalache check passed on this checkout in about 90 seconds.
The same-instance journal-tail recovery slice uses
`QUINT_DFS_METADATA_JOURNAL_TAIL_RECOVERY_VERIFY_MAX_STEPS`, defaulting to 6.
Its coverage scheduler calls the ordinary write, durable append,
materialization failure/rollback, gap-aware begin-write, replay, and reopen
actions; a fixed-seed 5,000-trace run reaches all four registered healthy,
failure-cut, recovered-admission, and reopen-equivalence witnesses. The
registered depth-6 Apalache check passed on this checkout in about 9.8
seconds.
The crash/reopen stale-head slice uses
`QUINT_DFS_METADATA_STALE_HEAD_REOPEN_VERIFY_MAX_STEPS`, defaulting to 4. It
treats the persisted next-sequence head as a cache and derives the admission
floor from the maximum durable segment tail and materialized proof. A fixed-
seed 5,000-trace run reached all four healthy-head, stale-head-observation,
repair-before-admission, and exact `N + 1` append witnesses. The registered
depth-4 Apalache check passed on this checkout in about 6.2 seconds.
The directory-tombstone overlay refinement uses
`QUINT_DFS_NAMESPACE_TOMBSTONE_OVERLAY_EQUIVALENCE_VERIFY_MAX_STEPS`, defaulting
to 2. Its fixed-seed coverage run exercises parent-only rejection,
child-plus-parent and move-out acceptance, live-in-place, move-in, and new-child
rejection, same-path replacement, compacted matching-tombstone skip, new-id
tombstone eviction, new-id live replacement, duplicate-id tombstone-before-live
ordering, deterministic first failure across two candidate directories, and
marker removal before a later compacted tombstone. The bounded proof compares
terminal path owners, direct-child sets, and outcomes while checking that the
overlay scans no unrelated metadata. A fixed-seed 5,000-trace run reached all
13 registered witnesses, and the 2-step Apalache check passed on this checkout
in about 88 seconds.
The coordinator intent-index refinement uses
`QUINT_DFS_INTENT_INDEX_EQUIVALENCE_VERIFY_MAX_STEPS`, defaulting to 2. Its
symbolic `proofStep` combines ordinary lifecycle actions with a bounded action
that constructs each valid one-row prior state and applies every permitted
same-id state replacement through the same unindex/reindex functions. It also
constructs the two-filesystem shared-object state and checks start/restore
addition, commit/abort close, and same-id replacement while preserving the
other filesystem's awaiting intent. This avoids hiding replacement or global
object-id aggregation beyond an intractable depth-3 search. The registered
2-step Apalache check passed on this checkout in about 161 seconds. The
fixed-seed coverage run exercises active lookup, duplicate and pending
classification, committed-claim retention, abort cleanup, and failed-start
rollback, plus awaiting/committed/aborted hydration and same-id replacement
witnesses, cross-filesystem aggregation, global target lookup, and closing one
shared-object intent while the other remains open.
The coordinator receipt-bucket refinement uses
`QUINT_DFS_RECEIPT_BUCKET_EQUIVALENCE_VERIFY_MAX_STEPS`, defaulting to 3. Its
fixed-seed coverage run exercises first insert, duplicate-record idempotency,
durable restore including a retained closed intent, count/node and ordered-view
equivalence, cross-intent isolation, and rejection stuttering. The registered
3-step Apalache check passed on this checkout in about 186 seconds.
The metadata handoff union-sync slice uses
`QUINT_DFS_METADATA_HANDOFF_UNION_SYNC_VERIFY_MAX_STEPS`, defaulting to 4. A
fixed-seed 5,000-trace run reached all seven registered old-fallback, retained-
partial, late-final-ACK, exact-activation, and removal-only purpose-separation
witnesses; its registered depth-4 Apalache check passed on this checkout in
about 5.8 seconds. The maintenance-
trigger slice uses
`QUINT_DFS_METADATA_HANDOFF_MAINTENANCE_TRIGGER_VERIFY_MAX_STEPS`, defaulting to
8. Its fixed-seed 5,000-trace coverage scheduler reached all fifteen
quiet/local/nonlocal detector, pre/post-fence write, transport-block, timeout,
prompt retrigger, second-attempt, and ready completion witnesses; the scheduler
calls the same individual actions while symbolic verification retains the
ordinary `step` relation. The registered depth-8 Apalache check passed in about
5.2 seconds.
The completed-handoff lane-rebind slice uses
`QUINT_DFS_METADATA_HANDOFF_LANE_REBIND_VERIFY_MAX_STEPS`, defaulting to 4,
and runs coverage through `coverageStep`. It accepts a freshly opened lane only
when the current durable prefix equals the activated marker or is its proven
contiguous extension; the same successor height with divergent ancestry
remains fenced. The case registers 23 witnesses: completed/fresh/stale lane,
mismatched-evidence, divergence-rejection, post-activation append,
fresh-rebind, and bootstrap cases, plus ten cached-lane generation cases. The
recorded fixed-seed 5,000-trace run reported thirteen witnesses and a depth-7
Apalache check passed in about 13 seconds; that evidence does not match the
current 23-witness, depth-4 registration and has not been re-recorded.
The exact-prefix authority-acknowledgement slice uses
`QUINT_DFS_METADATA_AUTHORITY_ACK_PREFIX_VERIFY_MAX_STEPS`, defaulting to 4. It
separates re-reading the named operation from equality of the receiver's
complete resulting prefix. A divergent predecessor reaches the same sequence
but rejects without persisting the authority floor, remains eligible for an
authoritative snapshot, and may acknowledge only after exact repair. Coverage
must include healthy ACK, divergent rejection, snapshot repair, repaired ACK,
and the two stale-catch-up witnesses (snapshot request and floor preservation).
The handoff probe backend-selection slice uses
`QUINT_DFS_METADATA_HANDOFF_PROBE_BACKEND_VERIFY_MAX_STEPS`, defaulting to 4.
It retains the bounded old-path counterexample where a stale pre-opened routed
lane repeatedly rejects despite an exact canonical durable backend. The fixed
ordinary probe uses the canonical direct backend and cannot be blocked by that
stale view; a real canonical mismatch still rejects. Promoted-primary probes
remain on the special routed/current-marker path. A fixed-seed 5,000-trace run
reached all five old-retry, direct-success, direct-mismatch, current-marker, and
stale-marker witnesses. The registered depth-4 Apalache check passed in about
5.9 seconds.
The retiring-lane replica-ingest slice uses
`QUINT_DFS_METADATA_RETIRING_LANE_REPLICA_INGEST_VERIFY_MAX_STEPS`, defaulting
to 4. It registers 24 witnesses: author rejection; pending-marker,
removed-replica, foreign-shard, forged/stale/rollback/future-epoch,
noncontiguous, foreign-permit, and mixed-filesystem rejection for both batches
and snapshots; exact contiguous-batch and exact-snapshot acceptance; and
convergence. The recorded fixed-seed 5,000-trace run reported twelve
witnesses and the depth-4 Apalache check passed in about 9.9 seconds; that run
has not been re-recorded for the current registry. Accepted
replica maintenance never binds the retiring author generation. In the Rust
refinement, only a validated filesystem/shard/source/epoch permit reaches that
maintenance gate, and the backend rejects any batch or snapshot containing a
foreign filesystem identity before mutation; unauthenticated, identity-less,
stale routed, and raw backend entrypoints remain fail-closed.
The proof-bound primary-rotation slice uses
`QUINT_DFS_METADATA_PRIMARY_HANDOFF_ACTIVATION_VERIFY_MAX_STEPS`, defaulting to
8. Its fixed-seed 5,000-trace run reached all sixteen direct-rejection,
pre-fence-write, exact-union, durable-target-marker, crash/reopen, lost-response,
old-primary-fencing, reverse-marker, and new-writer witnesses; the registered
depth-8 Apalache check passed in about 7.4 seconds.
The cursor-assisted preflight slice uses
`QUINT_DFS_METADATA_PRIMARY_HANDOFF_CURSOR_PREFLIGHT_VERIFY_MAX_STEPS`,
defaulting to 9. Its witnesses cover the stale-watermark stall, exact and
divergent caught-up cursors, pending-only admission, a late write captured by
the fence, partial-ring non-activation, authoritative repair, and activation
only after a fresh exact full-ring proof.
The journal source spec can be overridden with
`QUINT_DFS_JOURNAL_VERIFY_MAX_STEPS`; its 5-step symbolic
check passed in about 22-27 seconds. The sharded namespace transaction spec can
be overridden with `QUINT_DFS_SHARDED_NAMESPACE_VERIFY_MAX_STEPS`, defaulting
to 4. A fixed-seed 5,000-trace run reached six registered witnesses covering
the retired-source tombstone, post-commit false redirect, stale and missing
replica-index live winners, and derived-index repair without namespace mutation. Its
depth-4 symbolic check passed in about 7.2 seconds. The mkdir transaction
identity spec can be overridden with
`QUINT_DFS_NAMESPACE_CREATE_TRANSACTION_IDENTITY_VERIFY_MAX_STEPS`, defaulting
to 4. Its fixed-seed 5,000-trace run reached all four selected-tombstone,
legacy-collision, version-bound-acceptance, and exact-replay witnesses without
a safety violation; the registered depth-4 Apalache check passed in about 6.6
seconds. The repair
authorization spec can be overridden with `QUINT_DFS_REPAIR_VERIFY_MAX_STEPS`;
its 5-step symbolic check passed in about 4-8 seconds. The replication policy
spec can be overridden with `QUINT_DFS_REPLICATION_VERIFY_MAX_STEPS`; its
5-step symbolic check passed in about 4-9 seconds. The no-conflict spec can be
overridden with `QUINT_DFS_NO_CONFLICT_VERIFY_MAX_STEPS`; its 5-step symbolic
check passed in about 6-7 seconds. The pending request lifecycle spec can be
overridden with `QUINT_DFS_PENDING_VERIFY_MAX_STEPS`; its default is 3 because the map-heavy
state space is wider than the earlier DFS models. On this checkout, Apalache
3-step verification passed in about 65-74 seconds, TLC 4-step verification passed
in about 129 seconds, and interrupted Apalache 4/5-step attempts reported Z3
`UNKNOWN`. Membership uses `QUINT_MEMBERSHIP_VERIFY_MAX_STEPS`,
defaulting to 4; a 5-step membership check passed on this checkout but took
about 50 seconds by itself. The repair content-validation spec can be overridden
with `QUINT_DFS_REPAIR_CONTENT_VERIFY_MAX_STEPS`; its registered default is 6.
The fixed-seed 5,000-trace run found no violation in 6,066 ms at 824 traces/s,
and its depth-6 Apalache check found no violation in 150,166 ms. The quorum-
aware replica fan-out spec can be overridden with
`QUINT_DFS_REPLICA_QUORUM_FANOUT_VERIFY_MAX_STEPS`; its registered default is
6. Its latest 5,000-trace run reached all thirteen registered witnesses, and
its depth-6 Apalache check found no violation in 36,968 ms (see above). The
repair lost-authority spec can be overridden with
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
`QUINT_DFS_NAMESPACE_REPLAY_VERIFY_MAX_STEPS`, defaulting to 5. Its six named
witnesses cover durable completed replay, changed-payload rejection,
cancellation, automatic interrupted-pending recovery, replay after restart,
and a fresh request identity after cancellation.

Use larger bounds for local audit runs, not as a routine CI gate:

```bash
QUINT_VERIFY_MAX_STEPS=8 QUINT_RAFT_PROTOCOL_VERIFY_MAX_STEPS=2 QUINT_NETWORK_ENDPOINT_VERIFY_MAX_STEPS=5 QUINT_NETWORK_TIMED_TRANSPORT_VERIFY_MAX_STEPS=6 QUINT_SYSTEM_SMOKE_RUN_MAX_STEPS=240 QUINT_SYSTEM_SMOKE_RUN_MAX_SAMPLES=10000 QUINT_DFS_ROUTING_VERIFY_MAX_STEPS=6 QUINT_DFS_READ_FRESHNESS_VERIFY_MAX_STEPS=4 QUINT_DFS_ROUTED_NAMESPACE_VERIFY_MAX_STEPS=4 QUINT_DFS_POSIX_NAMESPACE_VERIFY_MAX_STEPS=2 QUINT_DFS_POSIX_LINKS_VERIFY_MAX_STEPS=2 QUINT_DFS_POSIX_CROSS_SHARD_VERIFY_MAX_STEPS=3 QUINT_DFS_POSIX_READ_WRITE_VERIFY_MAX_STEPS=2 QUINT_DFS_POSIX_ADVISORY_LOCKS_VERIFY_MAX_STEPS=4 QUINT_DFS_POSIX_FSYNC_VERIFY_MAX_STEPS=4 QUINT_DFS_POSIX_PARTITIONED_OPERATION_VERIFY_MAX_STEPS=10 QUINT_DFS_LOCAL_FIRST_FILE_VERIFY_MAX_STEPS=6 QUINT_DFS_LOCAL_FIRST_NAMESPACE_VERIFY_MAX_STEPS=6 QUINT_DFS_LOCAL_FIRST_NAMESPACE_DURABILITY_VERIFY_MAX_STEPS=8 QUINT_DFS_LOCAL_FIRST_RECONCILE_GATEWAY_VERIFY_MAX_STEPS=6 QUINT_DFS_PRIMARY_FAILOVER_VERIFY_MAX_STEPS=4 QUINT_DFS_NAMESPACE_REPLAY_VERIFY_MAX_STEPS=6 QUINT_DFS_NAMESPACE_VERIFY_MAX_STEPS=6 QUINT_DFS_SHARDED_NAMESPACE_VERIFY_MAX_STEPS=6 QUINT_DFS_JOURNAL_VERIFY_MAX_STEPS=6 QUINT_DFS_REPAIR_VERIFY_MAX_STEPS=6 QUINT_DFS_REPLICATION_VERIFY_MAX_STEPS=6 QUINT_DFS_NO_CONFLICT_VERIFY_MAX_STEPS=6 QUINT_DFS_PENDING_VERIFY_MAX_STEPS=4 QUINT_DFS_REPAIR_CONTENT_VERIFY_MAX_STEPS=6 QUINT_DFS_REPLICA_QUORUM_FANOUT_VERIFY_MAX_STEPS=8 QUINT_DFS_REPAIR_LOST_AUTHORITY_VERIFY_MAX_STEPS=5 QUINT_DFS_ANTI_ENTROPY_VERIFY_MAX_STEPS=6 QUINT_MEMBERSHIP_VERIFY_MAX_STEPS=5 QUINT_MEMBERSHIP_KV_VERIFY_MAX_STEPS=5 QUINT_MEMBERSHIP_RAFT_CONFIG_VERIFY_MAX_STEPS=5 bash scripts/formal/check-quint.sh all
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
  real directory snapshots, composition with a route-cache model, raw
  transport frames, stream payloads, full KV log replay, and concrete DFS
  object-store contents;
- live `ClusterDirectory` read failures during an in-flight delivery,
  full-duplex session retry budgets, concrete transport nonce entropy, unbounded
  wall-clock fairness, and dynamic module enable/disable races;
- directory fragments, local-owned owner routing, owner-inclusion replica-set
  normalization, cross-primary repair handoff progress, router-local recovery after
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
