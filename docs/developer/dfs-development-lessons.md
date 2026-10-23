# DFS development lessons

DFS debugging should restore the required behavior while reducing unnecessary
state and dependencies. This guide explains recurring failure patterns and the
engineering decisions that avoid them. Use the [development workflow](distributed-development.md)
for ownership and parallel work, and the [debugging skill](../../.agents/skills/sinomo-dfs-debugging/SKILL.md)
for implementation entry points and experiments.

## Preserve requirements independently of implementation

Replicated-eventual writes separate W required durable copies from N desired
copies. The system must preserve one canonical history while allowing supported
surviving nodes to operate after abrupt disconnection. Metadata safety and content
durability are distinct obligations, but their composition must satisfy the same
end-to-end availability requirement.

A fix can accidentally narrow that behavior. For example, removing an unsafe
alternate authorization path and retaining only all-participant confirmation
makes missing peers mandatory again. Changing a partial-participation success
test into an expected rejection does not establish correctness against the
original requirement, even if the new test checks recovery more thoroughly.

Keep both the positive surviving-node scenario and the unsafe competing-history
negative. If the current algorithm cannot satisfy both, the algorithm needs
correction. Current code, a model derived from it and documentation describing it
are not independent reasons to redefine the requirement.

## Reassess a growing algorithm instead of fixing each new obligation

A narrow optimization can expand into new probes, acknowledgement floors,
activation fences, prefix proofs and ownership generations. Each addition can
create further restart, cancellation and compatibility obligations.

Before adding another mechanism, identify the existing owner of the fact being
protected. Compare reuse, removal and a new mechanism in terms of authority
sources, durable records, independent schedulers and serial waits. A small diff
can create a large lifecycle; fewer lines alone do not prove simplification.

After an unsuccessful patch, inspect the wait edges and state that patch added.
Do not assume each newly exposed failure is an unrelated old defect. A causal
control on the previous and candidate behavior can distinguish a necessary fix
from a workaround that should be removed.

## Verify a legacy producer before designing compatibility

An optional field, a permissive decoder or a negative fixture does not prove that
an actual supported writer emitted the corresponding record shape.

Consider a permit whose type allows absent cluster, node and runtime identities,
but whose ordinary constructor always supplies them. Building an anonymous-permit
drain protocol around a malformed test value creates routes, states and recovery
work for a nonexistent compatibility obligation. The right response is to verify
all real producers, preserve and reject invalid input, and represent the actual
producer contract in the current type.

Distinguish production write paths, retained operator DATA, conversion adapters
and manually assembled fixtures. An unreleased product does not need parallel
wire versions or indefinite old writers by default. Actual operator data still
needs an explicit preservation/conversion strategy; that does not require the
normal runtime to support every synthetic historical shape.

## Distinguish observation from mutation

Recovery callers may need a first effect, continuation of an uncertain effect,
or confirmation of an already committed effect. These are different operations.

For example, a retained transaction can contain the same mutation and original
caller comparisons while host-added authorization comparisons change on a later
submission. Adding caller bindings, a new replay API and another receipt ledger
may seem to reconcile that difference. If the caller only wants to confirm an
existing result, it should instead read the authoritative outcome, verify exact
identity and original command evidence, then validate current domain authority.

Use the existing domain recovery path for genuinely unfinished work. Preserve
unknown outcomes and original operation identity. An observation must not create
a second effect merely to obtain a success-shaped response.

## Fix ownership and coherence before adding another copy of state

A materialized store, requester cursor and background publisher can each observe
different stages of the same operation. Record which one owns the fact and which
are projections. Every projection needs a clear update/invalidation edge.

If one backend installs a snapshot while a requester retains an old cursor,
repeated catch-up is a coherence problem. Connect the requester to the existing
installation/refresh boundary before adding another repair loop. Likewise, if
one runtime root consumes another root's wake, scope the wake to its existing
owner instead of adding a second scheduler.

A derived queue or cursor may be necessary for efficient restart. Its purpose
and relationship to durable truth must be explicit. Two copies claiming the same
authority should not acquire a third coordinator merely to keep them consistent.

## Trace required progress across the whole operation

Missing confirmation can mean no dispatch, blocked admission, stale materialized
state, rejected authority or a reply that never reached its owner. The error
count alone cannot identify the cause.

Construct a wait-for graph with actual resources. A publisher holding shard work
can wait for an ACK whose ingress joins a global refresh that waits for the same
publisher. Another retry or lock cannot remove that cycle. Prefer the existing
exact replica path or a correctly scoped release/revalidation boundary.

Required FetchBlob work, completed replies and journal catch-up may execute in
background workers. Their worker placement does not make them optional. Audit
all message families sharing an advisory deferral decision when one is shown to
lose required progress, retaining ordinary network and receiver authorization.

## Build representative fixtures and informative validation

Use actual native ownership, admitted configuration, physical store generation
and production wire paths. A test must reach the phase it claims to exercise:
startup success, durable preparation, event emission, completed RPC and drained
owner are different observations.

Inventory all failures before selecting an experiment. Classify product, fixture,
environment and unknown causes with evidence. Use deterministic boundary controls
where possible; a later passing retry cannot explain an intermittent failure.
Preserve original uncertain operations while using fresh fixtures for independent
controls. Keep complete affected-target, process, retained-DATA and performance
results separate.

## Keep architecture independent of the debugging narrative

Architecture describes responsibilities, guarantees and constraints. Appending
each fix's ACK handling, lock order and state updates makes implementation choices
look like intended requirements. Tests adapted to those choices can then appear
to confirm a contract that was never intended.

A fix restoring an invariant usually needs code and a regression test, not an
architecture edit. Explain exact mechanics beside source or in an existing
reference; keep run evidence outside permanent docs. See the
[documentation rules](../README.md#document-status-rules).
