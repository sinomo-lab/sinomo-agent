# Network verification suite

Status: first local harness slice implemented; broader coverage
below remains a backlog. No CI work is included.

Implemented: existing-harness case listing/preview/selection, strict prebuilt
checks, process/topology verification, stable all-pair traffic sweeps, bridge
partition/heal assertions, individual restart/performance selectors, and durable
per-attempt local evidence. The read-only connectivity runtime probe can diagnose
traffic without requiring KV quorum; normal authority and mutation checks remain.
See the [local usage guide](../../docker/README.md#focused-local-network-checks).

Still pending: expanded deterministic event scheduling, actual executed-route
traces and new owner-side fence/retry timeline events, alternate/asymmetric fault
topologies, automatic trace shrinking/replay, paired performance comparison,
soak profiles, and expanded native transport coverage. Current route diagnostics
are predicted paths. The existing two deterministic restart regressions remain
in the daemon/network tests; they are not a complete schedule explorer.

## Objective

Catch connectivity, routing, and data-path regressions at the smallest boundary
that can reproduce them, before they appear as a DFS timeout or an operator's
unreliable cluster. Every failure should identify the first broken contract,
preserve the attempted operation and state transitions, and supply a replay.

The primary availability assertion is: after a fault is removed and the
authorized topology remains stable, every required ordered pair can establish
a correctly target-bound stream and exchange verified bytes within a bounded
recovery interval. A membership row, a graph path, or one successful ping is
insufficient evidence of sustained recovery.

Keep the contracts in [network architecture](../network-architecture.md) and
the [data-path reference](../developer/network-data-path.md): direct links,
source-owned connectivity, routing, object RPC, and domain effects have distinct
owners. No test harness should introduce a replacement production carrier or
give network reachability authority over membership, KV, or DFS.

## Current coverage and gaps

| Existing surface | Reuse | Gap to close |
|---|---|---|
| [Network crate](../../crates/sinomo-network/src/lib.rs), including routing, owned mux, owned-path, connectivity-store, and layer-boundary tests | Existing identity, directed-path, graph realization, cancellation, credit, and no-replay assertions | Expose focused local contract selections; expand event-order and generated-topology coverage rather than duplicating examples |
| [Daemon maintenance](../../crates/sinomo-daemon/src/cluster.rs) and [cadence](../../crates/sinomo-cluster/src/network_maintenance.rs) | Real durable generation fences and scheduler transitions | Systematically interleave probes, command observations, publication, cancellation, and restart |
| [Six-node harness](../../docker/test-cluster.sh) | Exact-target RPCs from all six origins, latency/payload gates, quietness, DFS checks, diagnostics | Its network-only lane still includes KV/DFS setup; successful origins leave the readiness pending set and are not required to remain jointly healthy |
| [Routing scenario](../../docker/test-routing-scenario.sh) | Manual endpoints, stalled direct endpoint, fallback, routed payloads, concurrent DFS | Turn its useful scenarios into individually selectable, consistently reported cases |
| [Bridge recovery](../../docker/test-bridge-recovery.sh) | Real bridge disconnect/reconnect | Currently accepts recovery from health/doctor output; require verified traffic within each surviving component and across the restored bridge |
| [Restart lane](../../docker/test-file-history.py), `--case restart` | Simultaneous and staggered crashes, process hashes, current and retained bytes | Make individual restart cases easy to select, repeat, and replay locally; keep network recovery results separate from DFS outcomes |
| [Network formal checks](../formal/cluster-verification.md) | Existing base, endpoint, and timed-transport cases | Bounded model evidence complements implementation tests; it does not establish real socket or daemon scheduling behavior |

The last incident supplies two permanent regression seeds: ordinary connection
failure being classified as identity rejection, and a superseded durable probe
leaving speculative healthy cache entries without an observation retry.

## Local workflow and execution policy

Distributed verification runs on the developer's Docker-capable debugging host.
There are no CI jobs, merge-request gates, automatic schedules, or all-module
suite prerequisites in this proposal. The unit of work is a selected network
case and its necessary setup, not the workspace test matrix.

Improve the existing entrypoints:

- `sinomo test list` and `sinomo test run` remain the runtime probe interface;
- `docker/test-cluster.sh` owns segmented-cluster setup and selected network
  cases, retaining the existing full-suite entrypoint;
- `docker/test-routing-scenario.sh` owns its manual-endpoint/fallback topology;
- `docker/test-bridge-recovery.sh` gains actual traffic assertions;
- `docker/test-file-history.py --case restart` retains the DFS restart sentinel;
- existing network timeline capture and
  [summarization](../../scripts/debug/summarize-network-timeline.py) gain the
  missing retry/fence/route evidence.

Expose a case catalog and a preview before execution: case IDs, required
binaries/features/topology, affected processes, fault actions, fresh/reused state,
necessary setup, time/byte budgets, and artifact paths. Selecting one case must
not silently run every module's tests. The implemented
`SINOMO_TEST_NETWORK_CASES` selector supports `smoke`, `recovery`, and
`performance`; the additional profiles and richer previews below remain planned.

| Local profile | Use | Scope |
|---|---|---|
| `contracts` | Reproduce a suspected state or routing bug | Named tests in the owning crate; deterministic event sequences |
| `transport` | Investigate framing, flow control, or stream lifecycle | Selected real-socket and owned-carrier tests |
| `smoke` | Check a built binary's connectivity | Required ordered pairs, verified bytes, and actual route evidence |
| `recovery` | Verify a connectivity or maintenance change | Selectable bridge, fallback, and restart cases with stable recovery assertions |
| `performance` | Compare a data-path change | Selected direct/relay workloads and saved baseline/candidate artifacts |
| `soak` | Pursue an intermittent failure | Explicitly requested seed, duration, repetitions, and traffic budget |

Profiles group cases for convenience, not a compulsory sequence. A developer
may run the Docker reproducer immediately using an existing binary; Rust tests
are useful for isolating the cause, not a prerequisite to inspect a live failure.
For changed code, run the relevant regression and owning test target, then the
Docker cases exercising that boundary. No default `cargo test --workspace` or
all-module sweep. Report coverage omissions when a narrower selection is used.

### Build once and preserve the debugging session

Accept an absolute prebuilt binary and reuse it across compatible selected cases.
An explicit build action may perform an incremental `cargo build -p sinomo`;
case execution must not silently rebuild the workspace. A missing or mismatched
binary fails preflight. The existing `SINOMO_TEST_BUILD=0` path needs a strict
missing-binary check for this workflow, since the current harness may still
build when its default binary is absent.

Record source patch, profile, feature set, hash, and actual main/shadow process
identity. A restarted container can retain an old mounted inode or use its
persisted rollout binary; do not treat the CLI on the mount as process proof.
Make the tested executable an immutable per-run artifact. Release builds are
required for comparative performance, while debug builds remain useful for
correctness and traces. Report build/setup/case/capture durations separately and
show total wall time; exclude neither an expensive build nor setup from the
user's cost estimate.

Reuse the existing `SINOMO_TEST_REUSE`, `SINOMO_TEST_KEEP`, binary, project, and
Compose override controls, with consistent semantics across the focused scripts.
Validate topology, identities, binary hashes, active faults, and readiness before
reusing a cluster. Do not tear it down between independent cases when their
preconditions permit reuse. Fresh-formation cases still require fresh disposable
volumes and first-use cases require an explicitly cold caller/session. Reused
state must never be reported as fresh bootstrap evidence.

On failure, capture evidence and retain the owned debugging cluster when the
selected retention policy requests it. Always remove temporary injected faults
unless an explicit keep-fault mode was selected. Distinguish retaining the
cluster from retaining a network cut. Capture, inspect, rerun-one-case, and
cleanup must be available without rebuilding or restarting the whole suite.
Never reset a named retained project as an implicit prerequisite to debugging.

### Bounded local runs

Every case declares its workload and deadline before execution. Preserve the
existing protocol/readiness limits; changing a test duration must not relax
correctness assertions. The catalog should show measured recent durations for
matching build/topology, or mark estimates as unmeasured. Do not promise that a
small selector eliminates the daemon's transitive compilation cost.

A duration budget stops at a case boundary or records an interrupted case as
incomplete, restores faults, and saves evidence. It cannot silently skip required
assertions and report success. Resume revalidates binary identity and case
preconditions; it does not continue an ambiguous mutation blindly.

Use unique disposable projects, with faults confined to their namespaces.
Serialize performance conditions on the selected debugging host and record
background load. Missing Docker/fault-control prerequisites are explicit setup
failures. A host that cannot run Docker cannot provide distributed acceptance.

## 1. Deterministic connectivity and maintenance tests

Keep network mechanism tests in `sinomo-network`, cadence tests in
`sinomo-cluster`, and durable adapter tests in `sinomo-daemon`. Test-only helpers
may inject a clock, probe completion, and storage/publication barriers; they
must not add network-to-KV dependencies or production fault flags.

Use a small reference transition model independent of the implementation's
retry selector. Generate event sequences over: observation received, expiry,
probe selected, probe completed, source-set commit, publication completed,
directory changed, cancellation, and process reconstruction. Virtual time tests
backoff and expiry; explicit barriers force persistence races. A random seed
alone is not a reproducible concurrent schedule.

Required cases and oracles:

| Case family | Required result |
|---|---|
| Refusal, reset, connect timeout, expired handshake vs genuine identity rejection | Typed transport recovery remains eligible; diagnostic text cannot change classification; real identity rejection retains its protection |
| Probe completion races a newer local observation | The newer source observation survives; rejected speculative cache is discarded; bounded observation work is retried |
| Probe succeeds but quorum publication fails | Exactly the retained source generation owns publication retry; do not redial simply to retry publication |
| Directory changes or a peer is revoked during probe/dial/publication | Stale work cannot reintroduce a removed member or endpoint; next work uses current authorized inputs |
| Observation expiry, duplicate/out-of-order publication, endpoint reuse by another identity | No fresh fact is overwritten by older evidence; expired or wrong-owner facts cannot manufacture a usable edge |
| Cancellation at each await and restart before/after durable commit | Every incomplete obligation has a retry owner or explicit terminal result; no uncommitted cache establishes convergence |
| Failed edge A→B with healthy B→A and A→C | Only the source-owned edge changes; one failure cannot label a node globally unreachable |
| Repeated faults followed by stable reachability | Eventually restore healthy observations within the configured scheduling budget; failure bursts remain bounded and idle work settles |

Enumerate short schedules at all barrier pairs, then add seeded longer traces.
On failure, shrink to a minimal ordered event list and promote it to the fixed
corpus. Include a negative-control run that reproduces each historical bug
using a test mutation; the associated assertion must fail before the fix.

## 2. Routing planner and executor conformance

Extend the existing line/ring/star/mesh realization tests. Enumerate all directed
graphs up to four nodes for the pure planner, then sample bounded larger graphs
and explicit diamond, asymmetric, disconnected, and long-chain cases. Run the
expensive stream realization subset separately.

The independent oracle enumerates authorized simple paths using fixture-owned
edges, capability/scope constraints, freshness, hop limits, and the documented
recovery-candidate policy. It must not call the production planner, connectivity
selector, or ranking helper to determine expected results. Assert path safety
and existence before ranking; test documented tie-breaking separately. Paths
beyond configured hop/candidate bounds are explicit exclusions, not false
claims of unbounded connectivity.

For each required ordered pair:

- establish the actual stream, check its authenticated destination and process
  affinity, and exchange unique sequence-tagged bytes in both directions;
- record candidate attempts separately from the executed route; assert the
  expected direct/relay class and allowed intermediate nodes;
- make an attractive endpoint stall, then verify bounded alternate selection
  under one total connect deadline, including the maximum candidate set;
- remove a bridge in a diamond and prove new streams use the surviving path;
  remove the sole bridge in a line and prove bounded unavailability;
- reuse or revoke an endpoint while a pooled link exists and verify the current
  identity/directory contract, including main versus shadow affinity;
- break an already returned stream after bytes are sent: it must fail, never
  transparently replay or migrate application bytes to a new stream.

A returned stream is the boundary between safe route-attempt fallback and
domain-owned retry. Test `DefinitelyNotSent` versus `PossiblyApplied` with an
instrumented handler and a lost reply. Assert no hidden second handler
invocation for one application attempt. Test deduplication only in a separate
domain fixture with an explicit stable operation identity; do not imply the
network provides exactly-once effects.

## 3. Real transport, framing, and resource conformance

Exercise the production owned-chunk/mux/RPC path over real loopback TCP as well
as deterministic in-memory fault streams. The compatibility byte adapter is a
separate case, not a substitute for the owned path.

Use a test-only fault adapter with barriers for partial read/write, delayed
handshake, reset after N bytes, half-close, stopped consumer, and cancellation
while waiting for credit. It wraps the transport boundary and does not replace
authentication or object-RPC framing. Record the precise fault activation.

Cover inline/head/data-frame boundaries at limit−1, limit, and limit+1, and
multi-frame known/unknown-length bodies. Assert exact bytes, lengths, ordering,
bounded allocation, rejection of malformed/oversized frames, and cancellation
without collateral failure of unrelated streams. Bad identities, foreign
clusters, forged route endpoints, stale capability context, truncated heads,
and early EOF must fail at the correct boundary.

A valid application body exceeding one frame must continue streaming; only an
oversized individual wire frame or control head is rejected. Do not turn
resident buffer limits into application payload limits.

Run many small streams alongside bulk streams and a stalled consumer. Verify
credit conservation, prompt RESET/control progress, fairness, wakeups after
sub-threshold credit returns, and eventual release of stream slots and tasks.
Memory must stay within configured pool/carrier limits; it must not scale with
total transferred bytes. Establish baseline resource counts after pool warmup
and check post-drain retention bounds rather than requiring RSS to return to
its initial value.

Run portable transport tests on native Linux and Windows debugging hosts. Cross-building
does not prove runtime behavior. Cover Linux splice and portable relay paths,
default protection and explicitly selected AEAD, cold and pooled links. Keep
Iroh/native transport-module conformance as a separately provisioned lane with
declared discovery/relay infrastructure; the existing Iroh-disabled Docker lane
cannot establish its correctness.

## 4. Docker connectivity and recovery scenarios

Keep the six-node segmented topology as the primary regression environment.
Add a small diamond topology for alternate-route recovery: the existing single
bridge deliberately has no redundant cross-segment path. Fault controls operate
only inside the owned project's namespaces. Use network disconnect for complete
cuts and scoped `netem`/filter rules for directional drop, delay, and loss; record
and verify both injection and removal. An unactivated fault is a failed test.

The scenario manifest defines expected authorized nodes and traffic pairs before
probing. Never derive the denominator from currently reachable peers. Six healthy
nodes require all 30 ordered pairs. A partition requires success within each
surviving component, classified bounded failure across the cut, and recovery of
all 30 pairs after healing. TCP stream correctness is checked above TCP; packet
reordering/loss is injected below it rather than presented as reordered bytes.

Selectable fixed recovery corpus:

1. Fresh volumes and first-use calls without a successful warmup prerequisite.
2. Warm pool and cold caller against a stable cluster.
3. Simultaneous crash/restart of all nodes with volumes retained.
4. Delayed bridge restart; keep the known four-second case and cover delays
   around the configured startup-settle boundary in explicitly requested extended
   runs.
5. Bridge cut/heal, directional blackhole, and a stalled advertised endpoint.
6. Redundant route lost/restored while callers hold live streams.
7. A bounded mixed bulk/small-RPC workload during recovery, then quiescence.

Use representative fixed variants for a focused local check; opt-in extended
runs expand delay, loss, start order, load, and repeated cycles. Vary one factor first, then selected
pairs; do not start with an unmanageable Cartesian product. Log expected failure
windows before injecting faults so the classifier cannot excuse an unexpected
failure after the fact.

Preserve the current 60-second formation and 120-second network-readiness limits
for the standard six-node profile. Recovery time starts at verified fault
removal or the scheduled final process start, not at the first successful probe.
Require three complete all-pair sweeps at two-second intervals within that
budget; an earlier passing origin is rechecked. Report first success, first
complete sweep, and sustained recovery separately. Faulted calls still obey
their per-call deadlines; a recovery allowance does not excuse a hanging call.

Separate two experiments: active foreground traffic may legitimately help
discover recovery, while background recovery must be observed without foreground
probes during its observation interval and verified by a final traffic sweep.
Diagnostic commands must not secretly repair state. Preserve the full initial
attempt history; a later pass never erases first-use errors.

After networking passes, retain the existing DFS current/retained-byte sentinel
across distant nodes. Report a subsequent DFS or KV failure under its own layer.
Do not require DFS placement convergence to produce a network-only result. A
network-focused composition must retain the real directory/authentication and
maintenance paths even if domain components are disabled. Until that composition
exists, report setup costs separately and reuse the existing full daemon.

Finish with resource drain and the existing whole-container quietness gate.
Health labels and doctor output remain diagnostic corroboration, not traffic
acceptance criteria. Do not infer multi-voter partition safety from this default
seed-voter/learner topology; that belongs to the membership/KV suite.

## 5. Performance regression protocol

Reuse `cluster.network.ping-benchmark` and
`cluster.network.semantic-attachment-benchmark`; extend their evidence instead
of inventing a faster carrier for benchmarks. Keep separate workloads for small
typed RPCs, large typed values, and semantic attachments.

| Dimension | Required measurements |
|---|---|
| Cold vs pooled | Dial/authentication/route setup latency separately from warm call latency |
| Direct, one relay, two relays | Per-origin/target and actual route; never mix classes into one average |
| Payload and concurrency | 0-byte ping; 1 KiB, 64 KiB, 256 KiB, 4 MiB values; larger attachments; concurrency 1/8/32 in selected cells |
| Duplex and mixed load | Per-direction goodput, small-RPC tails during bulk traffic, queueing and admission delay |
| Resource efficiency | CPU time/GiB, payload-copy bytes/GiB by reason, pooled allocations/GiB, pool peak/retention, current and peak RSS, task/FD counts |
| Churn | Recovery duration, connection/handshake counts, retry/probe/publication rates, post-recovery idle work |

Keep existing direct/one-relay p50 and p95 ≤2 ms gates after the existing quiet
window, plus existing payload floors and correctness checks. Do not relabel a
failed established gate as noise. Treat inability to obtain a valid isolated
measurement as infrastructure failure, not a passing benchmark.

For new comparative gates, use baseline/candidate pairs on the same selected
debugging host with identical release profile, topology, workload, warmup, and resources.
Use at least five balanced AB/BA pairs, reset state per condition, and retain raw
samples including errors and timeouts. A fresh process per condition avoids
mistaking lifetime peak RSS for a window delta. Include source, relay, and
destination measurements where needed; label any unavailable attribution.

Provisional comparative thresholds are >15% worse latency or >10% lower goodput
relative to the paired baseline. Calibrate them against at least 20 unchanged
baseline runs before making them blocking. Compare confidence intervals of
run-level paired ratios, not individual correlated RPCs; a threshold crossing
whose interval is inconclusive requires more evidence, not an automatic pass.
Do not refresh the baseline from a failing candidate. Resource-limit violations,
byte corruption, wrong-target delivery, and unexpected request failures block
immediately regardless of comparative statistics.

Report p50/p95/p99/max, counts, failed/deadline-exceeded requests, and offered vs
completed throughput. Do not claim useful p99 precision from a handful of calls;
collect at least 10,000 measured small calls per reported tail cohort. Use both
closed-loop saturation and scheduled-arrival latency tests; report scheduled
arrival-to-completion delay so an overloaded sender cannot hide queueing by
issuing fewer requests. Bound the offered-load generator and report rejections.

Docker establishes regressions on one host, not 100 GbE capacity or WAN behavior.
Keep hardware saturation and native multi-host transport measurements separate.
Follow [experimental reproducibility](../developer/experimental-reproducibility.md).

## Failure artifacts and harness improvements

Extend the existing scripts with a shared, versioned case manifest and artifact
helpers. Extract repeated process/topology/fault mechanics only where multiple
harnesses need them. Preserve their public entrypoints and established gates;
do not introduce a parallel cluster manager or require a wholesale harness
rewrite. `docker/test-file-history.py` remains the owner of history assertions.

Case IDs should be stable across local selection, runtime-probe output, timelines,
and result bundles. A case records its prerequisites and intended assertions;
the harness owns process orchestration while production probes exercise the
real network path. Reuse `sinomo test` cases for traffic and extend their structured
output to identify the attempted and executed paths.

Each run produces:

- `manifest.json`: source/patch, binary and process hashes, build profile,
  toolchain, host, topology, identities without private keys, features,
  scenario/seed, expected pairs, deadlines, and baseline identity;
- `events.jsonl`: monotonic event sequence and elapsed times for injection,
  probe selection/completion, durable fence outcome, publication, cache/directory
  generations, route attempts, session establishment, and RPC result;
- `traffic.jsonl`: origin, authenticated destination, nonce/sequence, requested
  and executed route evidence, byte verification, deadline and delivery state;
- `result.json` plus a readable local summary: pass/fail/infrastructure-error/
  incomplete, first failing invariant, expected/completed counts, attempts and recovery times;
- bounded pre-failure event buffers, local source-record snapshots, container
  state/logs and resource series, and an exact replay command/event schedule.

Use source-local monotonic timestamps and causality IDs; do not order distributed
events by unsynchronized wall clocks alone. Add lightweight owner-side events
for missing fence/retry evidence rather than dumping private state on every
packet. Diagnostic captures are bounded and separate from performance samples.
Collect the first-failure bundle before teardown; a diagnostic rerun gets a new
result and cannot overwrite the original failure. Save artifacts locally on all
exits, including interruption, with an explicit incomplete result if work did
not finish.

Test the harness helpers themselves: skipped cases, missing targets, unsuccessful
fault activation, false healthy labels, stale process hashes, command timeouts, partial
JSON, and leftover fault rules must fail. Capture original exit status through
artifact collection and cleanup. Restore only resources owned by the run.

## Delivery sequence and acceptance

1. **Make current tools selective and reusable.** Add case listing/preview and
   selection to the existing harnesses, strict prebuilt-binary checks, consistent
   reuse/retention controls, and per-case local results. Acceptance: select a
   network case against a retained compatible cluster without rebuilding,
   resetting its volumes, or running unrelated module suites; a process hash
   mismatch is rejected before the workload.
2. **Close the recovery-observation gap.** Strengthen existing readiness and
   bridge-recovery checks with an all-pair stable traffic oracle, and expose
   simultaneous/staggered restart variants individually. Add deterministic
   maintenance interleavings and retry/fence evidence to the existing timeline.
   Acceptance: both historical restart defects have failing negative controls;
   healthy labels with broken streams cannot pass; the first failure is saved
   and can be inspected without recreating the cluster.
3. **Broaden faults and replay.** Add diamond/asymmetric topologies, framed-stream
   faults, resource cleanup, and a persisted seeded corpus. Acceptance: injected
   wrong-target delivery, hidden replay, credit leak, and stalled recovery each
   fail their named invariant with a replayable trace. Extended runs are opt-in.
4. **Improve existing benchmarks.** Add local paired release comparisons and
   calibrated summaries to current ping/attachment probes. Acceptance: a
   controlled throughput or payload-copy regression is detected while unchanged
   runs characterize the noise floor; existing absolute gates stay enforced.
5. **Expand environment coverage when needed.** Run selected cases on native
   Windows, Iroh/module transport fixtures, and separately provisioned hardware.
   Missing environments remain explicit coverage gaps; no all-platform run is a
   prerequisite for the focused local debugging loop.

Start with steps 1 and 2: improve the existing debugging loop and strengthen its
actual distributed assertions. Keep CI configuration outside this work.
