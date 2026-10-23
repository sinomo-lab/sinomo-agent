---
name: sinomo-dfs-debugging
description: Diagnose and simplify Sinomo DFS failures without growing duplicate state machines. Trace requirement drift, real record producers, authority and admission ownership, exact-operation recovery and unnecessary synchronization before changing the algorithm.
---

# DFS debugging by operation and owner

Use this for a concrete failing operation or a recurring family of DFS failures.
For a new authority protocol, also use the distributed-protocols skill. For broad
suite classification and fixture conversion, use [DFS validation](../sinomo-dfs-validation/SKILL.md).
The [development lessons](../../../docs/developer/dfs-development-lessons.md) explains
why these steps exist; it is not required reading on each invocation.

## Check the requirement and mechanism before adapting callers

The user required outage tolerance and W/N separation from the beginning. Current
all-participant metadata publication is a known implementation mismatch, not an
accepted exception to that requirement. Keep W commit durability, N desired
placement, and safe metadata authority distinct while preserving end-to-end
availability. Removing an unsafe fallback does not authorize making every peer
mandatory. A safety-only model or a rewritten architecture paragraph cannot
supersede user intent.

Before a fix adds a persisted field, coordinator, registry, cache, lock, retry
state or compatibility branch, read [mechanism review](references/mechanism-review.md).
It gives concrete procedures for the historical mistakes: anonymous test permits
treated as legacy DATA; receipt queries implemented as transaction resubmission;
KV caller-binding/replay APIs added for a DFS recovery issue; and partial-success
tests changed to require all participants.

Produce a small state-ownership/edit table, not a new runtime tracking system.
Locate the existing owner and the operation actually needed; check the simpler
reuse/removal option before implementing another layer. A regression after your
patch is evidence against that patch too: compare the state/lock/wait edges you
added before treating every new failure as an unrelated old bug.

## Select the missing boundary before selecting a fix

Name the exact request/write/intent, filesystem, object/version, metadata shard,
epoch, primary, assigned participants, content W/N, and observed terminal state.
Bind that to tested source, executable and retained DATA. Record unknown fields
as unknown; they determine the next observation rather than a guessed diagnosis.

At the current V7 boundary, distinguish four facts:

| Fact | Owner/evidence | It does not establish |
|---|---|---|
| Content is durable enough | Exact content receipts satisfying policy W | Metadata publication or delivery of the result |
| Metadata is publishable | Original prepared body, full assigned participant proof, coherent configuration and held writer/fence | Client received a reply |
| Outcome is known | Exact prepared/committed/terminal operation record | A new operation may replace an uncertain one |
| Transport delivered or may have delivered | Request/response identity and `NetworkDeliveryState` | The semantic write committed or failed |

`MetadataRecoveryQuorumUnavailable { verified, required }` names a failed proof;
it does not identify the subsystem that stalled. At the reviewed revision,
publication requires every assigned metadata participant and has no ordinary
Raft-permit fallback. This describes the existing rejection path, including its
availability defect. Verify the working revision; do not turn this description
into a reason to reject the user's required surviving-node behavior.

Locate the relevant path with the exact-symbol CodeGraph queries and links in
[source map](references/source-map.md). Use the symptom-specific experiments in
[failure patterns](references/failure-patterns.md); read only relevant sections.

## Reconstruct one operation end to end

Follow: caller → route/target → native ingress → preflight → admission → physical
writer → hidden prepare → participant dispatch/ingest/ACK → guarded commit →
receipt/reply owner → caller observation. Mark the last evidenced boundary and
the first missing one. For repair, include durable queue checkpoint, producer
capture, detached worker, delivery update and queue acknowledgement.

At every wait, record what the waiter still owns: global/shard admission,
writer/root lock, source-generation custody, cached service lane, response inbox,
or detached-worker exclusion. Draw a wait-for graph naming the actual owner and
the operation that releases it. A queued exclusive writer can block later shared
readers even before it acquires the lock. Different RPC names do not imply
independent progress.

Join cross-node evidence by exact request/operation IDs and process incarnation.
Timestamps alone cannot associate a reply, and remote monotonic clocks cannot
establish one-way latency. Observe admission *and* ingress/dispatch/receipt
boundaries: a client timeout alone cannot distinguish no send from blocked ACK.
Use the existing Observe/performance tooling when latency or CPU is the question.

## Run a discriminating experiment

Write competing hypotheses and the observation that separates them. Example:
“peer lacks the prepared prefix” versus “peer has it, but ACK waits for unrelated
configuration rebuild.” Hold the real rebuild using the native fixture hook,
send the real authority request, and observe whether exact replica ingress can
finish. Include wrong-source/epoch negative controls so progress cannot come
from bypassed authorization.

Prefer the existing shared `DfsTestRuntime` and real transport/codec. Pause at
actual ownership boundaries; use channels/barriers and the original absolute
deadline. Do not invent successful ACKs, replace real custody with numeric IDs,
or use sleeps to claim an ordering. If a fixture must change, demonstrate the
missing prerequisite and preserve its behavioral assertions.

Before trusting “no owner/no RPC/no work,” run a held-owner or known-RPC control
through the same capture. Export traces and omission/drop counts before fixture
cleanup. Instrumentation must not retain the lock/authority whose lifetime it
observes. An empty capture without coverage is an evidence gap.

## Repair the shared cause and audit its siblings

Prefer removing an unnecessary dependency or routing through an existing exact
path over adding retries, a new guard, queue or coordinator. Identify which owner
or state transition disappears or becomes simpler. For a new mechanism, explain
the invariant existing mechanisms cannot represent and its cancellation/restart
owner before implementing it.

Once a scheduling/custody cause is proved, inspect all paths sharing the decision.
Required FetchBlob, completed replies and journal suffix work can all be
parked by an incorrect advisory exclusion. Test that whole family plus
an optional-work negative control. Required work still uses ordinary authenticated
network transport, authority checks and deadlines.

Preserve the original uncertain operation and exact payload. Existing replay
and recovery helpers reconcile it; a fresh fixture can test a control but cannot
settle the old outcome. Never replay a historical possibly-applied write under
a new identity. A later pass does not erase the failed run.

Finish with the failed-to-passing causal control, affected full targets and the
needed native/Docker boundary checks on final source. State separately what is
fixed, which old outcomes remain unresolved and which release gates are unrun.
If the current protocol cannot meet an existing availability requirement, keep
that requirement visibly unmet and correct the algorithm within the task's
scope. Do not ask the user to specify the same behavior again or rename the gap
as a new requirement. Explain a concrete design tradeoff only when one remains
unsettled; neither weaken safety nor silently sacrifice availability.

Document the result at its proper level using
[sinomo-documentation](../sinomo-documentation/SKILL.md). A corrected ACK,
admission, replay or state-update path ordinarily needs source comments/tests
and a task receipt, not another paragraph in `docs/dfs-architecture.md`.
