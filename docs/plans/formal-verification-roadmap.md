# Formal Verification Roadmap

Status: active backlog. Last reviewed 2026-07-15.

This plan tracks the next bounded verification slices. Current models,
properties, and execution instructions live in the
[cluster formal verification guide](../formal/cluster-verification.md).

New models should consume established lower-layer guarantees rather than
re-modeling routing, authority, content validation, and retry semantics in one
large state machine.

## KV and Raft

Completed 2026-07-10: the per-node three-voter protocol slice now covers vote
freshness and vote-once, quorum election, AppendEntries backtracking and
divergent-suffix repair, acknowledgement-backed commit, leader completeness,
state-machine replay, higher-term stepdown, and durable term/vote/log restart.

1. **Persistence refinement.** Connect the abstract durable restart action to
   WAL append, hard-state persistence, snapshot restore, and failure rollback.
2. **Raft-over-network composition.** Carry concrete RequestVote and
   AppendEntries exchanges over the network retry contract, including loss,
   duplication, reordering, and higher-term responses.

## Network

Completed 2026-07-10: the focused route-cache slice now covers directory
versions, local-connectivity file/content generations, transport-module
snapshots, table identities, expiry, membership removal, identity mismatch, and
before/after build races.

Completed 2026-07-15: the bounded object-RPC body direct-delivery slice models
one decoded frame at a saturated two-slot application queue. It proves that a
finite grace retains the frame in memory, consumer progress delivers it without
spooling, expiry falls back to the existing spool escape hatch, and consumer
close ends the wait without disk I/O. All five registered witnesses were
reached in the 5,000-sample coverage gate, and the depth-5 Apalache check found
no violation. Concrete Rust tests cover the direct, progress, timeout, close,
and known-length collection-bound paths; the live DFS large-read gate remains
the performance-refinement obligation.

1. **Timed retry conformance.** Add deterministic virtual-time Rust scenarios
   for late responses, fallback after a primary timeout, delivery duplicates,
   packet loss, and identity-mismatch suppression.
2. **Purpose-specific endpoint selection.** Refine selection for rollout, Raft,
   member status, and data traffic while preserving the common authorization
   and reachability rules.
3. **Session retry budgets.** Extend delivery models with body replayability,
   progress, and finish-attestation constraints. Prove that fallback cannot
   expand the caller's deadline, replay a one-pass source, or select an
   incompatible transport.

## Integrated System

1. **Projection contracts.** Map guarantees from focused membership, routing,
   retry, and DFS authority specs into the shared system model through explicit
   assume/guarantee boundaries.
2. **Directory and cache races.** Model directory snapshot versions, route
   cache marks, and in-flight plans. Prove that membership removal, identity
   mismatch, and endpoint-generation changes invalidate affected routes before
   new work is authorized.
3. **Bug-trace harnesses.** Encode fragile networking scenarios as small smoke
   modules and retain both random exploration and fixed-seed replay.

## DFS

Updated 2026-07-21: the namespace-command replay slice now captures durable
`Pending`/`Completed` admission keyed by requester and request id, immutable
full-command identity, exact terminal-result replay across process restart,
changed-payload rejection, fail-safe interrupted-pending handling, and the
requester-local cancellation boundary.

Completed 2026-07-10: the canonical POSIX synchronization slices now cover
whole-file shared/exclusive advisory locks, exact epoch-fenced lock tokens,
healthy-session lease renewal, partitioned-session expiry, close/crash cleanup,
file `fsync`, `fdatasync`, directory `fsync`,
outcome-unknown sync retry, and the partition rule for a lock-holding operation.
A complete holder may finish only within an unexpired strong permit; otherwise
the quorum side fences it after the barrier, repairs the certified canonical
prefix, and proceeds. Deterministic traces cover clean terminal states for both
outcomes. Cross-shard retry exhaustion now finalizes forward instead of
stranding durable jobs with path reservations held.

Completed 2026-07-14: a bounded target-hardening slice compares the reference
namespace planner's whole-table clone plus operation replay with an in-place
preapplied planner for one parent/child move and one child/parent recursive
delete. It proves identical prepared plans and successful terminal tables, and
proves that failure after durable journal append rolls optimized in-memory
tables back to the same journal-only, no-partial-visibility state as the
reference path. Wider transactions remain follow-up proof; the immediately
following slice closes the same-instance recovery obligation for this failure
cut.

Completed 2026-07-14: a bounded metadata journal-tail recovery slice separates
the durable two-entry journal prefix from the materialized and live-table
prefixes. It proves that table-batch failure rolls preapplied tables back while
retaining the durable tail, that the next begin-write detects the gap without
admitting or appending, and that exact replay restores a contiguous proof before
the second write is admitted. It also proves that reopen and same-instance
recovery materialize the identical durable replay projection. Torn records,
replay failure, concurrent writers, and unbounded journal histories remain
separate proof work.

Completed 2026-07-14: a companion crash/reopen stale-head slice proves that a
persisted next-sequence hint behind both the durable segment tail and
materialized proof cannot authorize sequence reuse. Reopen first promotes the
allocator and persisted head to at least one beyond the maximum durable proof,
then admits the next append at `N + 1`. Torn records, proof disagreement,
integer exhaustion, concurrent writers, and unbounded histories remain outside
this bounded slice.

Completed 2026-07-15: a bounded packed-blob index append refinement compares
the existing per-record length/body writes with one contiguous batch write over
the identical two-record byte stream. It checks every crash cut across both
four-byte length fields and symbolic two-byte bodies. Replay and tail truncation
retain the same zero-, one-, or two-record committed prefix for both writers,
while the healthy projection reduces four logical append invocations to one.
The slice deliberately treats the contiguous write as non-atomic and does not
claim one kernel syscall. Variable record lengths, allocation failure,
concurrent appenders, filesystem ordering, and concrete Rust byte-conformance
tests remain refinement obligations.

Revised 2026-07-16: the namespace-transaction body-transport slice proves
that one same-shard transaction travels through the single authenticated
object-RPC call contract with a semantic attachment, independent of its
encoded byte count. The model contains no control-head limit, size threshold,
inline fallback, compatibility route, or alternate RPC API. A complete valid
attachment reconstructs the exact transaction identity and file/file/parent
order, then applies at most once with all records visible atomically. Corrupt,
truncated, and cancelled attachments never reconstruct or mutate namespace
state, and a duplicate application attempt cannot repeat the mutation.
Concrete SOBJ conformance and the remote-primary cluster probe remain Rust and
live-gate obligations; transfer flow control and backend crash recovery stay
owned by their existing proof slices.

Completed 2026-07-14: a bounded directory-tombstone overlay refinement compares
the reference full-table clone/replay emptiness check with sparse direct-child
path ownership. Both replay three transaction slots in tombstone-then-live
planner order, evict the prior object placement and current target-slot owner,
and dynamically skip compacted matching tombstones until their path marker is
removed. They agree on terminal owners, children, deterministic first-directory
error, and accept/directory-not-empty outcomes across parent/recursive delete,
moves, new children, same-path and new-id replacement, duplicate-id ordering,
two candidate directories, and compacted-marker cuts. The optimized cost
projection scans only candidate initial children and excludes unrelated
metadata. Canonical-path/dentry-key conformance remains Rust follow-up work.

Completed 2026-07-14: a bounded active namespace-prepare footprint-index
refinement compares the reference retained-history overlap scan with sparse,
filesystem-qualified object and normalized-path ownership. It proves identical
accept/object-conflict/path-conflict decisions, including exact same-operation
retry, while `Prepared` rows own their footprint and committed or aborted rows
remain in history without blocking reuse. Prepare admission, rejection
stuttering, both terminal transitions, and atomic restart reconstruction all
maintain the exact reference projection. The bounded one-object/one-path
profiles cover object-only overlap, path-only overlap, full overlap, same-
filesystem separation, and identical keys in different filesystems. Arbitrary-
width footprint sets and persistence-failure cuts remain follow-up proof work.

The corresponding Rust fast path now derives filesystem-scoped, set-valued
object/path owners from every `PutNamespacePrepare`, including structured
snapshot and table-log replay, and rebuilds them after preapplied rollback.
Focused reference-equivalence and reopen regressions cover wider concrete
footprints, exact retry, both terminal releases, and cross-filesystem isolation;
the production admission path records zero test-oracle history scans.

Completed 2026-07-14: a bounded mkdir transaction-identity slice preserves the
same stable directory object and path across create, cross-shard move with a
selected source tombstone, and newer-version resurrection. It retains the
original prepare record to witness the legacy kind-plus-object/path id
colliding with a different payload and rejecting without mutation. The fixed
identity includes object version, admits the resurrection under a distinct id,
and proves an exact same-generation retry keeps the id and payload while adding
no second mutation. Arbitrary rename trees, history compaction, persistence
cuts, authority changes, and local-first operations remain outside this slice.

Completed 2026-07-14: a bounded coordinator intent-index refinement compares
historical scans with sparse awaiting-object and active-or-committed target
indexes across start, commit, abort, and failed-start rollback. It proves exact
lookup and validation-result equivalence, uniqueness, committed duplicate-claim
retention, abort cleanup, all-view rollback, and sparse-view reconstruction for
awaiting, committed, and aborted hydration records, including exact same-id
state replacement. The refinement also proves the filesystem-agnostic object-id
aggregate against two filesystems sharing one object id, including open/target
lookup equivalence and closing one intent while the other remains open. Replica
policy, receipt, and repair-job transitions remain outside this algorithm-only
slice.

Completed 2026-07-14: a bounded coordinator receipt-bucket refinement compares
the flat historical `(intent, replica)` receipt map with per-intent replica
buckets. It proves exact receipt content, durable node/count and ordered-result
equivalence, duplicate overwrite idempotency, cross-intent isolation, retained
closed-intent restore, and no mutation on rejected record or restore. Policy,
commit, repair-job, persistence, and local-first transitions remain separate.

Completed 2026-07-15: the repair content-validation slice now models
work-conserving stage fan-out with a fixed cap of two. N2 and N3 may be active
concurrently, every response remains authorized by its target-specific request
identity, and N3 may complete out of order while N2 remains in flight. An N2
failure and retry affect only N2 and neither cancel nor restart N3. Publication
snapshots the receipts satisfying `W` and the committed target hash/version;
an already-issued desired-replica request may complete afterward and extend
durable placement without rewriting that snapshot or target. The fixed-seed
5,000-trace run reached the bounded fan-out, out-of-order success, isolated
failure, isolated retry, and late completion witnesses in 3,340, 1,663, 1,677,
1,677, and 1,660 traces respectively, with no violation in 6,066 ms at 824
traces/s. The depth-6 Apalache check found no violation in 150,166 ms. The Rust
mapping now uses a fixed cap of two for full-body stage and repair transfers,
validates ordered target-specific outcomes before per-target inflight/retry
bookkeeping, and has sync/async three-target overlap plus isolated-failure
regressions. This slice deliberately abstracts whether the primary should issue
a target before or after `W`; the companion below fixes that policy. Transport
responses are still merged after the bounded batch joins, and inline/patch
transfers remain sequential; those liveness/performance refinements and local-
first behavior are outside this completed safety slice.

Completed 2026-07-15: a companion quorum-aware fan-out slice separates the
effective repair mode retained by the metadata primary from the caller's outer
wait policy. Background mode reserves only the exact missing `W` stage slots,
publishes its response at `W`, and starts `N-W` repair afterward. Foreground
mode may fill both stage slots but withholds its own response until `N`, while
out-of-order siblings remain independent. Stage and repair reservations are
disjoint and capped at two. Detached `CommitVersion` cache promotion begins at
the response-construction boundary over durable staged replicas with its own
cap of two, not as a durability acknowledgement. A rejected promotion returns
to the outbox and is explicitly covered through resend. At `N`, repair jobs,
stage/repair inflight state, retry and attempt records, and the retained pending
write are all empty; promotion may remain queued independently. The fixed-seed
5,000-trace run reached the thirteen registered witnesses in 1,232, 1,232,
1,232, 2,528, 1,269, 1,259, 2,528, 1,232, 1,232, 1,269, 1,240, 1,240, and 5,000
traces, with no violation in 4,252 ms at 1,176 traces/s. A generic 5,000-trace,
20-step run also passed in 3,890 ms at 1,285 traces/s, and the depth-6 Apalache
check found no violation in 36,968 ms. Unbounded replica sets, transport
fairness, and wall-clock performance remain outside this bounded safety slice.

Completed 2026-07-15: a dedicated detached-promotion outbox slice refines the
post-response policy across multiple writes and daemon cache generations. A
drain attempts each identity once under a cap of two, keeps failed identities
retryable behind unattempted ready work, and accepts only an exact `Committed`
acknowledgement; `Durable` remains a staged-only retry. Durable intent/receipt
evidence reconstructs the volatile outbox after repeated restart without a
read, cache replacement preserves shared identity state, and bounded settled
bookkeeping replaces the older version for one target/filesystem/object key.
The strengthened slice also makes promotion recovery independent of unrelated
maintenance failure, retains the authority observer across no-read topology
changes, and cools failed retries while newly queued healthy work remains
dispatchable. The fixed-seed 5,000-trace run reached all eighteen registered
witnesses in 623, 646, 646, 597, 597, 597, 615, 615, 616, 616, 616, 663, 663,
663, 633, 633, 633, and 607 traces, with no violation in 3,245 ms at 1,541
traces/s. A nondeterministic 5,000-trace, 20-step run passed in 3,698 ms at
1,352 traces/s, and the depth-8 Apalache check found no violation. Retry
cooling is an abstract delay transition; transport fairness, recovery-worker
liveness, unbounded queues, and wall-clock timing remain outside this bounded
slice.

Completed 2026-07-14: a bounded metadata membership-handoff union-sync slice
captures the observed ordinary-fallback wedge. A partial `{retained primary,
desired replica}` quorum is rejected as a retiring-epoch watermark and loses
its fresh ACK state, while a retiring-only quorum is still too small to
activate. The hardened refinement never takes the ordinary fallback, retains a
partial exact union as pending and retry-enabled, and activates only after the
last required retiring member acknowledges. Ordinary fallback is derived from
an explicit sync purpose as well as the member sets. A removal-only handoff has
desired membership strictly below retiring membership, making its required
union equal the active retiring assignment; the proof still disables fallback
for membership-handoff purpose while showing that ordinary metadata sync over
those same sets remains eligible. Activation requires the exact retiring union
and its full-shard certificate. Transport fairness remains separate liveness.

Completed 2026-07-14: a companion maintenance-trigger slice proves that, with
ordinary maintenance work absent, a pending handoff contributes to the prompt
detector exactly on its local retiring primary. Nonlocal handoffs do not spur a
local coordinator, phantom triggers are excluded, the retiring write fence is
monotonic, and a blocked attempt returns to pending, is reprobed and retriggered,
then completes on its ready second attempt before the long periodic tick.
Wall-clock bounds and fair repeated delivery remain outside the slice.

Completed 2026-07-14: a completed-handoff lane-rebind slice proves that durable
activation does not implicitly bind fresh or independently opened service
handles. Bootstrap may bind a fresh lane only from an exact active assignment,
successor epoch, completion proof, and a current prefix that is marker-equal or
a proven contiguous extension after legitimate post-activation appends. Wrong,
still-pending, or sequence-height-matching divergent evidence stays fenced;
fresh reopen and bootstrap from the contiguous successor remain admissible.

Completed 2026-07-14: an exact-prefix authority-acknowledgement slice proves
that re-reading a named prepared operation at its expected sequence is not
enough to persist an authority floor. The receiver's complete resulting prefix
must equal the primary's prepared sequence and fingerprint. A divergent
receiver rejects without a floor, consumes an exact authoritative snapshot,
and may then acknowledge the repaired prefix.

Completed 2026-07-14: a purpose-separated handoff-probe slice captures the live
stale-lane retry wedge. Ordinary replication can update the canonical direct
backend while a pre-opened routed lane remains stale; ordinary handoff probes
therefore attest through a freshly refreshed direct durable view. Promoted-
primary probes retain their routed/current-marker path. The durable refresh is
cold synchronization and recovery work, not a mutation-hot-path operation.

Completed 2026-07-14: the Docker acceptance path now waits for two consecutive
live shard-layout observations with five replicas on each default root,
services, and users lane and three distinct primaries before correctness and
performance probes begin. A convergence timeout fails closed with the last
probe evidence instead of benchmarking a pending, write-frozen handoff.

Completed 2026-07-14: a proof-bound primary-rotation slice composes the retiring
write fence and exact-union synchronization with promoted-replica activation.
The member ring is unchanged, mixed membership movement is rejected, and the
promoted replica persists a handoff/head/fingerprint marker before its ACK can
count. The completion CAS leaves the retiring instance fenced; crash/reopen and
lost-response paths bind only the exact promoted generation, stale lanes never
write, and completed inactive markers can be replaced by a later reverse
rotation. Direct KV-only primary transfer remains fail-closed.

Completed 2026-07-15: a cursor-assisted primary-handoff preflight slice captures
the placement liveness failure where a post-expansion write advances the live
head beyond the last certified KV watermark. On the healthy retiring primary,
a durable sender cursor covering the observed head may begin a pending handoff,
but it contributes no authority or proof. The later publication fence captures
late writes, a divergent cursor-backed target rejects its fresh exact-prefix
probe and remains repairable, and activation still requires exact fresh
acknowledgements from the complete unchanged member ring. Replica failover is
unchanged and cannot use this preflight relaxation.

Completed 2026-07-14: a retiring-lane replica-ingest slice separates author
authority from replica maintenance after primary rotation. The activated old
primary remains unable to author, while an exact current-successor contiguous
journal batch or authoritative snapshot may advance its replica prefix without
binding authority. Forged sources, stale epochs, noncontiguous batches, and
arbitrary snapshots stutter. Foreign-filesystem permits and foreign records in
otherwise exact mixed journal batches or snapshots also reject without
advancing the replica. The Rust refinement keeps the activated marker in place
across snapshot archival and the Docker gate now requires zero pending handoffs,
zero frozen active-primary lanes on every node, and unchanged shard epochs
across two clean observations.

The implementation boundary is permit-bearing rather than a general
allow-frozen backend call. Direct object-RPC and in-service handlers bind the
claimed source to authenticated signer or transport identity, clustered journal
commands require exact filesystem and authority epoch identity, and routed
response replay requires the dispatch-time assignment to still equal the
current committed assignment before constructing the filesystem/shard/source/
epoch permit. The backend validates every journal payload or complete canonical
snapshot table against that filesystem before its writer lock. Raw journal and
snapshot mutation APIs remain author-fenced.

1. **Anti-entropy progress.** Under explicit fairness assumptions, prove that a
   missing desired replica eventually obtains a durable receipt or remains
   visibly suppressed because authority was lost.
2. **Sharded namespace recovery.** Model the failure window where the target
   shard transaction commits but its routing index does not. Decide and prove
   whether reopen repairs it automatically or explicit retry owns recovery.
3. **Chunked patch semantics.** Prove that patch commit validates base version,
   base content, offset, and final content hash.
4. **Range-read body integrity.** Model source retry, stale-source rejection,
   and canonical-session attachment verification. Prove that completed reads
   match current metadata and that mismatched responses cannot populate local
   state.
5. **Replica placement.** Prove that duplicate candidates, owner normalization,
   and policy widening preserve the configured commit and desired replica
   counts.
6. **Authority handoff liveness.** Compose the now-bounded local trigger,
   exact-union sync, and retiring-writer fence with explicit transport and
   daemon-loop fairness. Prove eventual retry after timeout/restart and that the
   activated primary rediscovers and schedules committed repair work.
7. **POSIX conformance.** The first Rust gate now includes persistent
   whole-file distributed advisory locks and distinct
   `fsync`/`fdatasync`/directory-sync routing with quorum head certificates.
   Add end-to-end partition traces and durable sync-id replay before promoting
   the combined model beyond target hardening. Then deepen directory, by-id
   lookup, open-handle rename/unlink, byte-range locks, application wait-cycle
   detection, and aborted-move reservation release.
8. **Local-first DFS extension.** Completed 2026-07-10 as separate
   target-hardening slices rather than weakening the canonical model. The
   regular-file slice covers local write/truncate/fsync, crash recovery,
   immutable sealing, fresh/stale reconciliation planning, apply/converge/
   conflict outcomes, a retained symbolic content value, independent quorum and
   route transitions, frozen reconnect behavior, and full-payload exact retry
   across a modeled process restart. The namespace slice covers one same-shard
   create, mkdir, unlink, rmdir, or atomic no-replacement rename using stable
   parent/object ids and exact entry stamps; its terminal retry is an idempotent
   stutter, not a
   payload-keyed replay protocol. A companion slice covers one returned local
   namespace mutation, directory `fsync`, crash restoration, and durable-only
   sealing. A shared stuttering projection requires local work to leave
   authority/journal/object/namespace counters unchanged and accepts a crossing
   publication only with supplied abstract quorum/current-authority/epoch/plan/
   durability evidence. Added 2026-07-11: a separate one-shard publication
   gateway derives that compact evidence from a typed permit binding quorum,
   authority, symbolic content receipts, metadata prepare, epoch, configuration,
   predecessor head, successor index, payload, and one atomic effect/result
   envelope. Its journal lifecycle covers append, commit, pre/post-commit crash,
   recovery materialization, exact retry, changed-payload rejection, and stale
   head/epoch fencing. Added later 2026-07-11: direct file and namespace
   composition modules now expose terminal branch actions only when paired with
   the gateway commit in the same transition. They align journal/data revisions,
   bind two exact one-sync file manifests and seven named finite namespace
   manifests to the permit, distinguish Applicable/Converged/Conflict/
   Superseded results, and allow physical materialization to lag the committed
   envelope. This removes assumed action-level atomic result recording at the
   bounded branch/gateway boundary, but it is not a Raft or Rust-journal
   refinement and still fixes one request identity. Next, in order: compose the
   gateway permit with canonical lock, sync, partition, no-conflict, and Raft
   evidence; refine symbolic holders into byte-validated file/subtree retention
   receipts; then add POSIX rename replacement, general payload-keyed namespace
   replay, open-handle rename/unlink, and only afterward multi-operation
   dependency graphs, cross-shard branches, hardlinks and symlinks, branch
   gossip, and explicit service-produced merge candidates before Rust
   implementation claims conformance.
