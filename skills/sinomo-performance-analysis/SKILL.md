---
name: sinomo-performance-analysis
description: Profile Sinomo operations across nodes and produce reproducible latency, network and disk I/O reports. Use for DFS write, history, placement, repair or other cluster performance analysis, regression comparisons and benchmark result reporting.
---

# Sinomo performance analysis

Every DFS performance report uses the standard tables produced by
[`report-performance.py`](../../scripts/debug/report-performance.py). Read the
measurement contract in [observability architecture](../../docs/observability-architecture.md#generic-stage-performance-capture)
when collecting or interpreting a new capture. An unavailable measurement stays
explicitly unavailable; never fill it with zero or an older run's result.

## Establish the experiment

Record the requested checkout, source revision and dirty patch, exact executable
hash and build profile, node identities and roles, topology, storage medium,
workload (sizes, create/overwrite, W/N, warmups, iterations, concurrency),
thresholds, and selected gates. Resolve actual shard primaries from live state.
Keep artifacts under `target/` or `benchmark-results/`, outside commits.

Stop only the task's Docker cluster before compiling. Use normal Cargo workers,
unset `CARGO_BUILD_JOBS`, and run one compilation task at a time. Do not solve
memory pressure by reducing Cargo workers. Use the existing Docker verification
skill for formation, immutable binaries, actual main/shadow process hashes,
retained-state ownership, recreation after binary replacement and cleanup. Do
not substitute Docker when the user requests the real cluster.

Keep uninstrumented acceptance and traced diagnosis as separate runs. Preserve
unchanged acceptance thresholds and retain every failed run. A diagnostic run
with relaxed thresholds cannot establish acceptance. Start with a small capture
to check overhead and ring coverage before increasing the workload.

## Capture and analyze

Use the authenticated Observe routes through an explicit node list. Include
callers, metadata authority members, content replicas, relay nodes and any node
performing repair. Hold the operator's observation-control slot for the run; do
not overwrite another capture or change filters concurrently.

[`capture-performance.py`](../../scripts/debug/capture-performance.py) starts one
leased capture on each specified node, samples trace/process snapshots around a
supplied workload, preserves stdout/stderr and restores owned filters in cleanup.
Its client command is a JSON argument array (for example an absolute Sinomo
executable or `docker exec CONTAINER /absolute/path/sinomo`). It never compiles,
forms, resets or tears down a cluster. Run `--help` for exact arguments.

1. Check source/binary identity, health and workload correctness first.
2. Pass all node IDs, revision, binary hash, profile and a fresh artifact path to
   the collector. Choose a short workload that fits the capture lease. Polling
   itself creates traffic, so record its cadence and use an idle baseline when
   comparing whole-node costs.
3. For a busy DFS node, use a short `--buffered` capture: it stops tracing before
   export and excludes export I/O from node windows. Configure an explicit
   `SINOMO_OBSERVE_TRACE_RECORDS` capacity when needed (maximum 32768), record it,
   and start with one or two operations. A larger ring does not excuse drops.
   Prefer `--clients-file` with a local client on each measured Docker node to
   avoid routing every export through one node. The collector uses per-process
   sequence cursors; keep raw snapshots even if a ring gap is detected.
4. Run the report generator on the capture directory. For ordinary file writes,
   also use `--root-stage dfs.browser.write --expected-roots N`, where N includes
   all warmup and measured creates/overwrites in this capture. Its JSON keeps individual
   spans, direct and inclusive I/O, outcomes, peers, process incarnations and
   coverage. Its nonzero exit reports incomplete evidence, not a workload pass.
5. If routing, authentication, serialization or transport wakeup cost needs finer
   detail, capture the existing `observe timeline network` on the same nodes and
   retain its JSON plus `scripts/debug/summarize-network-timeline.py` output.
   CPU profile through Observe when CPU cost is the unresolved question.
6. For a regression, compare identical workload/profile/storage settings and
   report both absolute and relative changes. Pool raw samples only within a
   meaningful population; never average or sum node p95 values.

Use trace identity and span ancestry for joins. When W3C context is absent, use
exact client/server RPC request IDs and reject ambiguous joins. Keep selected
operation coverage separate from unrelated background spans crossing the window. Remote monotonic clocks are
independent. Do not claim one-way network latency by subtracting clocks unless
synchronization error is measured and reported. Span lifetimes include waiting;
unknown outcomes can include cancelled work. Distinguish client end-to-end
latency from server-stage lifetimes. Do not sum overlapping child durations.

## Standard result

Include these tables in every DFS performance report, even when rows must say
`unavailable`:

- **Acceptance:** workload/lane, payload bytes, create/overwrite, W/N, concurrency,
  warmup/measured samples, successful/failed/timed-out operations, p50/p95/p99/max,
  throughput, configured target, pass/fail, artifact identity.
- **Per node and stage:** process/role, stage, samples, p50/p95/p99/max milliseconds,
  known/unknown outcomes, logical TX/RX bytes and operations, logical file
  read/write bytes and operations, sync count/time, peers. These rows overlap.
- **Node windows:** node/PID/incarnation, measured interval, kernel read/write
  bytes and syscall deltas, non-loopback namespace TX/RX bytes. Include baseline
  work and observation overhead; these are not per-request physical I/O.
- **Coverage:** expected/observed nodes, records and starts/closes, truncation,
  unfinished/censored spans, unattributed I/O, unavailable counters and clock
  assumptions. Unknown data must not disappear from the summary.

Lead the narrative with the measured bottleneck and its evidence, or state that
coverage prevents a conclusion. Report the cost of the critical path separately
from total distributed work. Link raw artifacts and machine-readable tables.
State which functions/API paths are instrumented and remaining blind spots.
Close with validation outcomes, remaining failures and retained/stopped cluster
state. Do not mark an unmet latency target achieved or manufacture measurements
when the environment prevents collection.
