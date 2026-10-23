---
name: sinomo-dfs-validation
description: Plan and execute Sinomo DFS validation after a refactor or across a broad failing suite. Build a failure inventory, use production-shaped native fixtures, retain old DATA and uncertain operations, and distinguish component, process, recovery and release evidence.
---

# DFS validation without rediscovering one prerequisite at a time

Use this for a broad DFS change, fixture migration, release handoff or repeated
suite failures. For the causal investigation of one operation, use
[DFS debugging](../sinomo-dfs-debugging/SKILL.md). For a specific Docker run or
deployment, use its existing skill; this skill does not authorize live actions.

## Preserve a requirement oracle independent of the patch

Record the already-stated successful and rejected behaviors before changing
implementation-facing tests. Current code, generated traces and models derived
from it are not independent evidence that the user requirement is satisfied.
For DFS, W must remain the required content durability and N the repair target;
the supported outage case must retain a safe metadata path. Mandatory participation
in current code is an implementation gap, not permission to rewrite the oracle.

A partial-participation lock test that is changed to expect rejection until all
members return can retain useful exact-recovery checks while losing availability
coverage. Keep a separate surviving-node success case under the intended safe
protocol. Do not call a success-to-error change a fixture repair without an actual
authorized change of user-visible semantics.

For every edited test, identify whether it changes setup, an internal mechanism,
or the expected product behavior. Stronger negative assertions do not compensate
for a removed positive. Compare candidate versus prior behavior for regressions
introduced during debugging; use the [mechanism review](../sinomo-dfs-debugging/references/mechanism-review.md)
before accepting new states solely because the new fixtures exercise them.

## Start from the complete failure and boundary inventory

Read the last complete relevant result and preserve the failed run. Inventory
all failed cases before following the first one. Record source/lock/dirty patch,
binary/profile/features, target/filter, worker/resource settings, data shape,
topology, phase reached, original operation IDs and evidence paths. An interrupted
suite has incomplete coverage, not zero failures. A filter matching no tests is
not a pass for the intended case.

For each failure, keep observed symptom, candidate cause, demonstrated cause,
product/fixture/environment/unclassified status, fix and remaining coverage.
Group by shared boundary rather than inventing one task per test. Typical groups:
native ownership/readiness, preparation/replay, metadata confirmation, detached
response, repair handoff, retained-state startup, and measurement/tooling.

Changed fixtures and actual progress bugs require separate causal evidence. A focused pass cannot classify an earlier whole-suite failure. Use a
baseline/candidate control or direct missing-prerequisite evidence. Preserve
intermittent failures until explained; report later passes as additional evidence.

## Check fixture representativeness before converting the suite

A clustered positive fixture needs the actual native Main/component owner,
cluster/node identity, admitted configuration, current storage generation and
real authority. Production default configuration is not necessarily the shard
topology required by the test. Configure that topology through the normal guarded
path and verify it before the workload.

Use the shared `DfsTestRuntime`; see the
[native fixture/source map](../sinomo-dfs-debugging/references/source-map.md#native-fixture-and-observation-tools).
Before changing many tests, exercise one vertical slice through actual encoded
RPC, hidden prepare, full proof, commit, caller observation and fixture shutdown.
For migration, obtain stages through the real converter/capture path; a hand-made
image can omit ancestry, ACK floors or source custody.

Wait on the boundary the test requires: durable preparation, published object,
completed RPC, or drained owner. Event emission alone does not imply caller
completion. Inject faults at these observed boundaries, not after an arbitrary
sleep. Test helpers may pause/observe genuine paths; they must not mint authority,
supply successful receipts or create a separate production algorithm.

## Cover safety, progress, isolation and old state

Select rows affected by the change; do not run every row for a spelling fix.

| Boundary | Positive case | Failure/control case |
|---|---|---|
| Prepare/commit | Exact original body commits and retry returns original result | Changed payload/predicates rejected; prepare hidden; lost reply resolved under same identity |
| Admission | Required ingress/reply progresses through native lanes | Hold real competing owner; queued global writer; cancellation while waiting; wrong epoch still denied |
| Configuration | Coherent retained writer view; exact replica scope | Delta loss/rebuild; provider-only change; physical generation rollover |
| Maintenance | Bounded turns eventually drain required work | Repeated reads/preemption, phase error, cancelled producer and restart preserve cursor/custody |
| Replication | W=1/N=2 and W=2/N=3 with actual effective policies | Missing target, delayed ACK, stale hint; original queue and receipt identity retained |
| Handoff | Exact prefix, successor binding and pending repair ownership | Reply loss, old owner returns, stale generation, partial transfer; abrupt-loss success remains a required distinct scenario |
| Retained DATA | Open/copy/convert representative existing stores | Multiple batches, old configuration encoding, current-format ledger, interruption/restart |
| Components | Core namespace/content with History/Attachments disabled | Existing grants/pins remain honored; routes/workers withdraw; dependencies pending |
| Isolation | Unrelated supported work remains usable | Failed/disabled dependency does not silently move authority or corrupt shared state |

Test startup, workload and teardown as separate phases. A test named for stat
that failed during cluster startup establishes no stat behavior. An exact
700-create test failing at create 696 is evidence about cumulative state or
interleaving; do not shrink it to a handful of successful writes. A smaller
causal control complements the original workload and cannot replace it.

For abrupt-loss requirements, compare the existing implementation with the
original requirement. Preserve a positive surviving-node case, old-primary
rejoin and accepted-but-unanswered writes. Current all-participant publication
and cooperative handoff do not satisfy it. Neither enough content copies nor a
graceful restart test alone proves the required complete behavior.

## Reuse build and evidence machinery

Use [recipes](references/recipes.md) for target discovery, exact executables,
fixture archives, Docker lanes and formal classification. Normal Cargo scheduling
is the default. Shared-host work uses the existing build/resource guard; an
explicit active-task exception takes precedence. Do not perpetuate historical
worker restrictions or invent a second lock. Resource serialization does not
require serializing source analysis or independently owned changes.

Build once at a stable source point. Record emitted executables and hashes;
rerun those artifacts for diagnostics when source and settings are unchanged.
After a code change, rebuild affected targets and bind results to the new source.
Verify all relevant targets were emitted, including required features. Neither
`cargo check` nor `cargo test --no-run` is execution evidence.

Keep originals immutable. Work on verified copies of historical DATA and retain
unique failure evidence on durable storage. Use the maintained fixture wrapper
and its verified archive/finalization path; never globally sweep temporary
directories by age. Export successful diagnostic traces before successful fixture
cleanup when those traces answer an intermittent failure.

## Choose the next check by the question it answers

Run the causal regression and sibling controls, then the affected complete
targets. Run native process/transport/old-state checks when those boundaries
changed. A broad run without a new hypothesis or changed source is useful only
when deliberately testing an identified intermittent schedule; state that purpose.

Docker verification is composed evidence, not proof about native old DATA or all
platforms. Conversely, native unit suites do not prove real transport/process
behavior. Formal bounded safety, native trace conformance, liveness and real
deployment acceptance need separate claims. Read the actual model classification
before using a formal pass as a handoff criterion.

Return a concise handoff: coherent fix, failed-to-passing evidence, full relevant
counts, source/binary identity, original unresolved operations, missing gates and
retained/running state. Use generated Docker/Observe reports. Clearly distinguish
timing-only misses with proven outcomes from unknown authority/receipt/integrity
state. The latter blocks the affected correctness claim.

Keep run-specific counts, source identities and failures in those artifacts.
Architecture does not become a record of the validation sequence; follow
[sinomo-documentation](../sinomo-documentation/SKILL.md) for durable prose.

Independent feature work can continue through the existing client/VFS seams;
use the [capability map](../../../docs/developer/distributed-development.md#separate-development-dependencies-from-release-dependencies)
instead of declaring all agent/app work blocked by the DFS release gate.
