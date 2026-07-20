# Observability Architecture

Sinomo observability is owned by `sinomo-observe`. The crate provides the shared
runtime surface for process-local logging, tracing, metrics capture, bounded
trace reads, and live trace control. Other crates should instrument their own
behavior, but cluster-wide observation should flow through the observe routes
instead of adding domain-specific debug transports.

## Runtime Shape

Each daemon initializes the observe runtime at startup:

- `tracing` is the logging and trace event API.
- `metrics` is the low-overhead counter, gauge, and histogram API.
- `tracing-subscriber` installs a reloadable filter, controlled at runtime by
  `/observe/control`.
- Recent trace events are retained in a bounded in-memory ring so a live node can
  be inspected without a separate collector.

The default filter is intentionally modest. Hot paths should emit `trace!` or
`debug!` events only behind the normal tracing level checks, and expensive field
construction should stay outside packet forwarding and storage commit loops.

For DFS algorithm measurements, a truthy `SINOMO_DFS_PERF_TRACE` value (`1`,
`true`, `yes`, or `on`) adds `sinomo.dfs.perf=trace` to the process-start filter.
An exact `sinomo.dfs.perf` directive already present in
`SINOMO_OBSERVE_FILTER` wins, including an explicit lower level or `off`, so the
convenience flag never overwrites operator intent. Runtime `/observe/control`
updates remain authoritative after startup.

## Cluster Interface

Observation is exposed as ordinary object RPC:

- `/observe/events` reads persisted operational events.
- `/observe/logs` reads persisted operational logs.
- `/observe/metrics` reads live runtime metrics plus registered snapshot
  exporters.
- `/observe/traces` reads the bounded trace ring.
- `/observe/control` reads or updates the active tracing filter.

Read routes are available to the local process and cluster members. Control is an
admin operation. The `sinomo observe` CLI uses the same routed object RPC path as
normal cluster traffic, so observing another node also exercises the production
network routing layer.

`sinomo observe ... --target <node-id>` reads one node. `--all` fans out over the
authorized cluster members known to the local node and prints per-node results.

## Operator Surfaces

The graphical app's Cluster surface is the built-in live console:

- **Overview** samples up to 32 authorized nodes (local first) every five
  seconds while that view is visible. It shows RPC throughput/errors/latency, KV latency and queue
  pressure, DFS backlog, agent work, and per-node observation freshness.
- **Topology** retains the cluster graph and node inspector.
- **Activity** combines topology warnings with recent observation failures,
  partial exporter results, and process restarts.

Overview retains at most 180 derived samples (about 15 minutes) and 100 activity
items in the app process. It never writes metric history to cluster KV or local
storage. Health and observation freshness remain separate: an online node can
have stale, partial, or unsupported metrics, and a failed observation does not
rewrite cluster health.

For retention, dashboards, or alerting, run the companion Prometheus bridge:

```sh
sinomo observe prometheus serve --all
sinomo observe prometheus serve --all --name sinomo.kv. --refresh 15
```

It listens on `127.0.0.1:9464` by default and exposes `/metrics` plus
`/-/healthy`. The listener is loopback-only; place an authenticated reverse
proxy in front before exposing it beyond the host. One background collector
refreshes an immutable OpenMetrics snapshot, so Prometheus scrapes do not each
trigger cluster RPC or DFS/KV snapshot scans. Partial fan-out remains
scrapeable: reachable node series are retained, per-target `target_up` reports
failures, and `collection_success` distinguishes a complete collection from a
partial one. Prometheus or Grafana owns durable history; Sinomo does not embed a
time-series database.

## Metrics Model

There are two metric sources:

1. Runtime metrics recorded through the `metrics` API.
2. Snapshot exporters registered with `sinomo-observe`.

Snapshot exporters are pull-based. They run only when `/observe/metrics` is
queried, which keeps durable state scans out of runtime hot paths. Exporters
declare the metric prefixes they own, and successful DFS/KV results are cached
for five seconds (agent snapshots for fifteen seconds). Filtered requests skip
unrelated exporters. Exporters return normal `ObserveMetricRecord` values and
may report exporter failures as `sinomo.observe.metric_exporter.errors`.

Metric responses carry observation time, process incarnation, process role,
node identity, partial/exporter status, and descriptors for the returned metric
families. Descriptors define type, help text, and units across the RPC boundary;
the OpenMetrics encoder therefore converts explicit microseconds to seconds
without guessing from a metric name. Older responses remain readable because
the added metadata is optional.

Runtime histograms retain bounded cumulative buckets in addition to count, sum,
minimum, and maximum. This keeps recorder memory fixed and permits clients to
derive interval rates and approximate percentiles from consecutive reads.
`sinomo observe metrics --watch <seconds>` performs that delta locally; add
`--samples <n>` for a bounded capture. Streaming JSON emits one compact object
per interval so it can be redirected to a JSONL artifact.

The daemon registers built-in DFS and KV exporters:

- `sinomo.dfs.*` reports DFS object, version, receipt, intent, conflict, repair,
  namespace job, and metadata shard gauges.
- `sinomo.kv.*` reports local cluster KV revision, compacted revision, entry,
  lease, retained event, Raft progress, durable file size, and bounded peer-lag
  gauges. Runtime counters and histograms separate command queueing, WAL append,
  quorum replication, local apply, compaction, cache fallback, and background
  fanout cost.

Application-level crates register through daemon service hooks. The agent hook
currently exports `sinomo.agent.*` gauges for persisted sessions, control
snapshots, active turns, buffered events, task statuses, and read errors.

### KV performance diagnosis

KV metrics use only bounded runtime labels: command class, phase, outcome,
read authority/source, and replication payload type. Raw keys, prefixes,
request IDs, routes, errors, and peer IDs never become hot-path labels. Peer IDs
appear only in pull-time gauges constructed from current membership and the
bounded Raft peer-progress ledger. KV telemetry is process-local observation; it
must never be written back into cluster KV.

The primary signatures are:

- high `sinomo.kv.command.phase.us{phase="commit_sequence_wait"}` plus
  `sinomo.kv.commit.queue_depth`: the ordered proposal stream is saturated;
- high `phase="quorum"` with low `phase="local_append"`: routing, voter latency,
  or follower catch-up dominates;
- high WAL, apply, snapshot, or compaction duration: local durable storage or
  write amplification dominates;
- frequent `source="durable_raft_open"` or
  `source="materialized_busy_fallback"`: the read-view cache is cold or Raft
  storage is contended;
- fast quorum with growing `sinomo.kv.peer.lag_entries`: asynchronous fanout is
  not keeping followers converged.

Use interval percentiles rather than lifetime maxima when deciding whether a
deployment is currently degraded:

```sh
sinomo observe metrics --name sinomo.kv. --all --limit 0 --watch 5
sinomo observe metrics --name sinomo.kv. --all --limit 0 --watch 5 --samples 12 --json
```

## Trace Context

Object RPC requests carry trace metadata in their existing metadata map:

- `traceparent`
- `tracestate`
- `sinomo-observe-sampled`

Callers that do not provide a trace context get one generated at the object RPC
boundary. Relay envelopes preserve trace metadata, so direct and multi-hop calls
can be inspected through the same observation surface.

## Performance Boundary

Packet forwarding, route planning, and graph maintenance must remain decoupled
from observation:

- forwarding uses the routing table and emits cheap metrics/tracing only after
  decisions are made;
- route planning updates its routing table when the graph changes;
- snapshot exporters inspect durable state only on explicit metrics reads.

This keeps normal packet forwarding independent from graph scans, metrics
exporter scans, and human-facing diagnostics.

`docker/measure-dfs-shards.sh` resolves the main daemon's container PID to one
host PID in the node cgroup and records process CPU separately from whole-cgroup
CPU, pressure, I/O, throttling, and network counters. Its utilization gates use
only main-daemon CPU. The sampled CPU ownership is exact, while the beginning
and end of a workload window remain approximate because client process liveness
plus a warmup interval bracket the remote server operation; DFS performance
traces are the finer-grained source for those launch edges.

The same timeline records daemon thread states, voluntary and involuntary
context switches, process I/O when the kernel exposes it, and interval deltas
for cgroup pressure, block I/O, and network traffic. A low-CPU sample is gated
as avoidable only when daemon work is runnable while no CPU pressure, I/O
pressure or progress, or network progress is observed; sleeping or externally
progressing samples remain diagnostic evidence rather than automatic failures.
The optional tmpfs compose overlay may remove durable-disk cost for an
algorithm-only measurement, but it is not a substitute for the disk-backed
restart, damage, or convergence gates.

The shard gate measures the serial metadata boundary, not every CPU in the
machine. One hot metadata-shard primary is expected to use roughly one core;
the default root, services, and users lanes have three distinct primaries and
target about three aggregate metadata cores under concurrent writes. Additional
hardware is used through more shards and writes or through content hashing,
replica transfer, and transport work. Passing the configurable utilization
floor means that this bounded target stayed work-conserving; it is not a claim
that one shard saturated the entire host.

The crate-level `network-blob` benchmark is a complementary tmpfs-backed,
in-process measurement when its root is under `/dev/shm`:

```sh
cargo bench -p sinomo-dfs --bench scale -- \
  --scenario network-blob --preset smoke \
  --root /dev/shm/sinomo-dfs-algorithm-bench \
  --network-blob-bytes 128MiB \
  --network-blob-desired-replicas 5 \
  --network-blob-write-quorum 1
```

It uses `InProcessNetwork`, but its `LocalContentStore` and metadata backends
still exercise filesystem calls against tmpfs; it is not a pure heap-only
filesystem. Reported effective cores are process-wide across the owner, replica
workers, and benchmark driver. The `W` duration is an upper bound at POSIX write
return, while the all-`N` duration ends at the latest replica commit. One-
millisecond sleeps in the benchmark are driver polling between
`process_incoming` calls, not DFS hot-path waits. Remove the explicit benchmark
root after collecting its JSONL output.

Use two separate Docker projects or fully tear down the first project between
the durability and algorithm measurements. Docker formation has a fixed,
non-configurable ten-second acceptance gate. Both performance projects must be
formed fresh with formation observation disabled; an observation-enabled
project is diagnostic evidence, not a benchmark environment. The disk-backed
suite remains the correctness, restart, damage, and convergence gate:

```sh
SINOMO_TEST_BUILD=0 SINOMO_TEST_FORMATION_OBSERVE=0 \
  SINOMO_DFS_PERF_TRACE=1 docker/test-cluster.sh
```

After it has stopped, form a fresh tmpfs-backed cluster whose network/KV
baselines, DFS readiness, stable shard layout, and live daemon mutation lanes
have all passed, measure the three live shard primaries, and tear it down with
the same compose files:

```sh
SINOMO_TEST_BUILD=0 SINOMO_TEST_FORMATION_OBSERVE=0 \
  SINOMO_TEST_COMPOSE_OVERRIDE=docker/docker-compose.dfs-tmpfs.yml \
  SINOMO_TEST_NETWORK_ONLY=1 SINOMO_TEST_KEEP=1 SINOMO_DFS_PERF_TRACE=1 \
  docker/test-cluster.sh

SINOMO_DFS_PERF_COMPOSE_OVERRIDE=docker/docker-compose.dfs-tmpfs.yml \
  docker/measure-dfs-shards.sh

docker compose -p sinomo-dfs-test -f docker/docker-compose.dev.yml \
  -f docker/docker-compose.dfs-tmpfs.yml down -v
```

Do not record a Docker acceptance result until those commands have actually
run. Preserve the disk suite under `target/cluster-test-artifacts/<run>/` and
the algorithm evidence under `target/dfs-shard-perf/<run>/`, especially
`timeline.csv`, `isolated-primary-summary.csv`, `aggregate-summary.tsv`,
`sample-classification-summary.csv`, `workloads.tsv`,
`main-daemon-stderr-workload.tsv`, the per-node
`main-daemon-<node>.workload.stderr.log` files, and the before/after health,
doctor, DFS-status, and shard-layout files. The daemon-stderr manifest records
the exact appended byte range or a best-effort full-log fallback when the file
was reset or an offset was unavailable. The timeline gate fails a runnable
low-CPU streak with no resource wait or progress; sleeping samples with no
observed external progress remain diagnostic, so inspect that class alongside
the preserved `sinomo.dfs.perf` traces before concluding that the hot path has
no unnecessary wait.
