# Experiments that would have shortened the DFS investigation

These patterns are diagnostic hypotheses, not rules that every similar error
has the same cause. Use the linked
[source map](source-map.md) for current symbols and test controls.

## Missing participant acknowledgement

First record the configured participant IDs and the actual missing member.
“Verified 2, required 3” is insufficient. For each member, collect request ID,
route/target, ingress, admission, durable journal prefix, materialized prefix,
prepared digest, persisted acknowledgement floor, response dispatch and caller
observation. Durable and materialized positions can legitimately differ.

Distinguishing observations:

| Observation | Next experiment |
|---|---|
| No actual dispatch, only enqueue | Trace admitted worker, wake, active flag and shutdown; keep original send identity |
| Ingress present, admission pending | Identify live gate owner and queued exclusive waiter; capture a known held-owner control |
| Exact shard proof exists, writer projection rebuilding | Pause that rebuild; test exact replica path with unchanged source/epoch checks |
| ACK persisted, response missing | Follow response inbox/outbox, cancellation and advisory eligibility |
| Replica prefix differs | Inspect exact journal/snapshot source and fingerprint; sequence height alone cannot prove common history |
| Healthy physical store reopened after provider delta | Compare storage-generation identity against observation/configuration revision; test projection change without structural store change |

The historical dependency cycle to look for is:

```text
publication owns shard work → waits for participant ACK
ACK ingress → joins global writer refresh / waits for global admission
queued global writer → waits for publication's shard work to finish
```

Background refresh can yield through the existing preemption path; exact replica
preflight can stay independent of unrelated writer rebuilds.
Do not generalize this to skipping cold initialization or authoring validation.
Test both warm exact ingest and cold/changed-context rejection/replan.

## Required work disappears behind advisory maintenance policy

Audit the shared decision for FetchBlob, completed write responses, nonempty
journal suffixes and necessary snapshot repair. A response produced by accepted
work can still run in a background-owned worker; the worker's name is not the
semantic classification of its message.

Use native nodes to produce the actual message. Apply an advisory exclusion,
then show required work reaches its normal receiver and optional promotion or
verification remains deferred. Assert original request/payload, target identity,
authority and deadline. No supplied “success” reply. Check cancellation and
restart retain delivery uncertainty and do not schedule duplicate effects.

A FetchBlob correction alone does not cover completed replies or required journal
work. Finding one should trigger a family audit, not a claim that all missing-
quorum symptoms are solved.

## Preparation repeatedly returns Pending

Separate contention, advancing bounded work, invalid source evidence, and a
retired generation. Trace stable session/slot identity, physical source lock,
cursor and phase across calls. A caller that recreates the preparation each
time can make every batch look busy without advancing.

Use a dataset crossing the actual batch boundary and hold a real competing
owner. Require prompt no-progress Pending while contended, then eventual Ready
after release, with exact data and no enlarged budget. Interleave protocol reads
through successive turns; read preemption must not reset already completed work.
Exercise cancellation, restart, aliases of the same root and generation rollover.

If preparation makes progress but misses a timing objective, report the observed
terminal result separately. If terminal visibility/receipt is unknown, classify
that uncertainty rather than calling it a timing-only result.

## A positive fixture fails after stronger ownership checks

Determine whether the fixture reached the phase named by the test. Examples from
the history: a bare service lacked a native Main owner; a migration image lacked
converter-produced ancestry; an expected cached head lagged the owning shard;
a setup mutation returned before the observed completion; teardown dropped an
owner before its deferred response arrived.

Correct the setup through shared production paths, and assert actual readiness
before entering the intended failure window. Preserve exact content, quorum,
scope, deadlines and negative controls. A constructor returning successfully,
an event being emitted, and caller receipt are different boundaries. A failing
real path must not be renamed “fixture” merely because a focused rerun passes.

## Repair queue is empty but N is still unmet

Inspect all relevant owners, not just the coordinator. Follow original intent →
captured producer work → durable queue → detached send → response → receipt →
settlement. Querying one empty local queue cannot establish remote completion.
After handoff, compare old/new owner records including request/delivery state;
regenerating work from desired targets may reset retry identity or forget an
uncertain send.

Use the existing read-only runtime probe:

```sh
sinomo test run cluster.dfs.placement-activity --require-cluster --json --param limit=16
```

It requires an authorized running cluster. Retain successive outputs and join
with original operations; it is an activity observation, not a repair trigger or
proof that the original write committed.

## Correctness passes but progress/availability does not

Separate these questions before patching:

- Did the operation become terminal under its original identity?
- Can independent operations progress while this one waits?
- Can the promised surviving-node configuration safely continue?
- Did the measured deadline or resource objective pass?

The reviewed all-participant implementation rejects partial metadata proof even
with enough content receipts; cooperative handoff waits for retiring-primary
evidence. Those restrictions conflict with the original outage requirement.
Current docs/models recording the restriction do not make it accepted behavior.
Use the [mechanism review](mechanism-review.md) to identify where success was
lost and how the algorithm can be corrected safely. Retries, weaker proof,
local-writer fallback or resurrected KV records are not substitutes. Independent
work can continue via the [development capability map](../../../../docs/developer/distributed-development.md#separate-development-dependencies-from-release-dependencies).
