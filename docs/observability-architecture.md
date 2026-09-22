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
`sinomo test run` initializes this explicit trace mode on stderr, preserving
its machine-readable `--json` stdout while retaining the client-side
`content_ms` and `send_ms` records. Long-lived daemons retain their supervised
observe runtime and normal log sink.

## Cluster Interface

Observation is exposed as ordinary object RPC:

- `/observe/events` reads persisted operational events.
- `/observe/logs` collects recent runtime logs and retained operational logs.
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

- **Overview** starts live sampling by default, collecting up to 32 authorized
  nodes (local first) every five seconds while Overview or Explore is visible.
  Summary tiles show agent work and failures, DFS repairs and conflicts, and
  online nodes. Charts are ordered by agent activity, DFS durability, cluster
  traffic and responsiveness, then node capacity. Expandable diagnostics contain
  queues, replication lag, exporter errors, and per-node observation freshness.
  Topology refreshes every 30 seconds during live sampling. Controls and scroll
  position survive updates. Pause freezes the displayed time; resuming starts
  fresh rate baselines. Every chart opens its underlying metric in Explore.
- **Explore** is a generic live metric browser over the descriptors returned by
  `/observe/metrics`. Operators can search and filter the registered catalog,
  select nodes and bounded label values, apply kind-safe counter or histogram
  transforms, compare live series, and inspect exact current/minimum/maximum
  values. Both views use fixed horizons chosen for the metric: one minute for
  CPU, five minutes for agent activity, rates and latency, and 15 minutes for
  storage, capacity and replication gauges. The full time axis remains visible
  while samples accumulate. Plots use local collection time, preserve reset and
  offline gaps, and report the actual retained span. Descriptor conflicts
  and high-cardinality results are surfaced in the UI instead of silently
  aggregating incompatible series.
  The catalog virtualizes its rows and preserves scroll and selection across
  samples. Value-only updates reuse shared catalog metadata; catalog searches
  do not rebuild the active chart. Dashboard baselines retain only their curated
  metric inputs, while Explore ingests the complete bounded response. Interval
  comparisons index series by name, kind, and labels instead of rescanning the
  previous batch for each record.
- **Topology** retains the cluster graph and node inspector.
- **Activity** combines topology warnings with recent observation failures,
  partial exporter results, and process restarts.
- **Logs** collects recent logs from all authorized nodes or a selected node
  through the connected daemon's ordinary `/observe/logs` route. Collection
  uses at most eight concurrent requests and 32 nodes per batch; omitted nodes
  remain individually selectable. Search, minimum severity, pause/resume and
  manual refresh apply to the selected scope. The virtualized list preserves
  controls and selection and exposes full messages and structured fields in a
  selectable detail pane. Node failures and source/retention limits stay visible.
  Polling runs every five seconds only while this view is visible and live.

### Log collection

`sinomo-observe` collects enabled tracing events from the current daemon process
in a separate log buffer. Clearing trace captures does not clear logs. The active
observe filter determines which runtime events are collected; viewing logs never
changes that filter. Runtime retention is bounded by 4,096 records and 4 MiB and
resets on process restart. Each record is bounded to 16 KiB with explicit
truncation metadata. This is recent diagnostic history, not an audit archive.

`/observe/logs` merges that buffer with the retained operational JSONL journals.
The disk tail reads at most 2 MiB on a blocking worker, uses the journal's shared
rotation lock, and reports busy/unreadable/corrupt sources without losing other
records. Filters run before tail limiting. Zero limit selects 200 records; the
service caps requests at 1,000 records and 512 KiB. Responses include process
identity, collection time, runtime availability, eviction/truncation information
and per-source errors. Older record and response payloads remain readable and
are identified as operational-only when runtime metadata is absent.

Each UI row is attributed to the node actually queried. The UI retains at most
1,000 rows and 2 MiB, ordered by reported timestamp with deterministic ties;
timestamps from different node clocks do not establish causal ordering. An
in-flight response from an earlier node/filter selection cannot overwrite the
current selection. Daemon-backed `sinomo observe logs`, `--target NODE` and
`--all` use this same collection route. Without a daemon RPC adapter, the CLI
retains its offline operational-journal reader.

The collection route is ordinary cluster-readable observation and does not need
the host remote-management permission. It collects daemon runtime and operational
logs, not arbitrary host files. The desktop Health action still opens the local
app log; desktop and shadow process file logs are not imported into the main
daemon's runtime buffer.

Overview retains at most 180 derived samples (about 15 minutes). Explore keeps
the latest catalog/current values bounded: collection reads at most 4,096
records per node, retains at most 4,096 families/current series, and exposes at
most 64 label keys and 64 current values per key. It retains at most 180 points
for up to 96 series in the active query scope; changing metric, node, or label
scope starts a fresh short history. Retained histogram samples are normalized
and limited to 256 buckets so one registered observation cannot amplify into an
unbounded history payload. The UI reports catalog, dimension, and series caps
when they are reached. The app also keeps at most 100 activity items. It never
writes metric history to cluster KV or local storage and is not a time-series
database.
Health and observation freshness remain separate: an online node can have
stale, partial, or unsupported metrics, and a failed observation does not
rewrite cluster health.

Dashboard snapshot counts use the maximum reported by a node, rather than adding
potentially replicated agent or DFS state. Disk capacity shows the least free
node; genuine counter rates sum across reporting nodes. Latency is the highest
route/node percentile, not a synthesized cluster percentile. Unavailable values
remain unknown, and each panel shows its reporting-node coverage.

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
families. Descriptors define type, help text, and units across the RPC boundary.
They can also declare a presentation domain, a safe cross-series aggregation,
and the bounded label keys intended for filtering. Those semantic hints are
optional and additive: older responses remain readable, and unknown metrics
remain explorable without guessing an unsafe aggregation. The OpenMetrics
encoder converts explicit microseconds to seconds without guessing from a
metric name. Conflicting kind or unit declarations fail closed in the app: the
family remains discoverable with a visible conflict, but its series are not
combined or transformed until sampled nodes agree.

Runtime histograms retain bounded cumulative buckets in addition to count, sum,
minimum, and maximum. This keeps recorder memory fixed and permits clients to
derive interval rates and approximate percentiles from consecutive reads.
`sinomo observe metrics --watch <seconds>` performs that delta locally; add
`--samples <n>` for a bounded capture. Streaming JSON emits one compact object
per interval so it can be redirected to a JSONL artifact.

The daemon registers built-in DFS, KV, and node-resource exporters:

- `sinomo.dfs.*` reports DFS object, version, receipt, intent, conflict, repair,
  namespace job, and metadata shard gauges. Runtime operation counters and
  histograms classify the bounded control/data-plane operation, outcome, and
  end-to-end handling time without using paths or object identifiers as labels.
- `sinomo.kv.*` reports local cluster KV revision, compacted revision, entry,
  lease, retained event, Raft progress, durable file size, and bounded peer-lag
  gauges. Runtime counters and histograms separate command queueing, WAL append,
  quorum replication, local apply, compaction, cache fallback, and background
  fanout cost.
- `sinomo.node.*` reports aggregate CPU, load, memory, swap, data-filesystem,
  daemon RSS, uptime, and network byte counters. It never exports hostnames,
  mount paths, interface names, addresses, or process lists. The sampler is
  initialized at daemon startup and its successful snapshots are cached for
  five seconds, so observation reads do not wait for a fresh CPU sampling
  interval.

The network crate records `sinomo.network.rpc.*` around the canonical endpoint,
loopback, routed, and relay call boundaries. A routed call is counted once even
when route selection or endpoint retry performs multiple internal attempts.
Labels contain only the bounded path and outcome class; routes, endpoint
addresses, node IDs, and error strings remain outside metric labels. Head-byte
counters intentionally exclude streamed attachment bodies rather than
presenting partial transfer sizes as whole-request throughput. Separate bounded
attempt counters, route-plan latency/cache outcome, and endpoint-open
latency/transport outcome distinguish routing churn from connection setup and
application handling.

`sinomo.observe.*` also reports the observation system itself: runtime metric
series, trace-buffer occupancy and drops, per-exporter record counts and latest
collection duration/outcome/cache state, total collection duration, returned
record count, and response truncation. These are assembled directly into the
current response, so a slow or failing snapshot exporter is visible on the same
read that experienced it rather than only on a later scrape.

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

The ignored `profile_metric_explorer_refresh` app test is a deterministic,
headless CPU probe for a 128-family, 4,096-series catalog. Run it with
`cargo test -p sinomo-app --lib profile_metric_explorer_refresh -- --ignored --nocapture`.
It reports ingestion, scene preparation, realized widget counts, and pointer
readout cost separately; these are development-build timings, not GPU frame
times or production throughput guarantees.

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
CPU, pressure, I/O, memory, throttling, and network counters. Concurrent
utilization gates use only main-daemon CPU; an isolated average below the
configured CPU floor remains diagnostic unless the ready-work classifier also
finds an avoidable idle streak. The sampled CPU ownership is exact, while the
beginning and end of a workload window remain approximate because client
process liveness plus a warmup interval bracket the remote server operation;
DFS performance traces are the finer-grained source for those launch edges.

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

Each `cluster.dfs.bulk-write` lane reports two completed logical small-file
publication rates. `data_write_files_per_second` measures the deferred bulk
write plus its authoritative metadata commit; it excludes the separately timed
one-off parent-directory setup and final publish scheduling. The end-to-end
rate retains those setup costs. The Docker gate requires every isolated lane's
bulk write phase to sustain at least 1,000 publications/s by default, then
requires the concurrent cluster to sustain 1,000 end-to-end publications/s
over a conservative wall-clock envelope from before the first lane launch
through the last lane exit. Concurrent per-lane rates remain visible but have
no default floor. These are logical bulk publications, not independently
durable POSIX syscalls. Override `SINOMO_DFS_PERF_MIN_FILES_PER_SECOND` only
when intentionally measuring a different storage regime, and retain the
configured floors with the artifacts.

The default `SINOMO_DFS_PERF_MODE=debug-gate` contract is unchanged by the
optional profiling modes:

- `release-ceiling` raises the default bulk call to 16,384 files and requires
  `SINOMO_DFS_PERF_BINARY_PROFILE=release`. The profile value is an operator
  assertion recorded in `config.tsv`; the script cannot prove how the
  already-running container binary was compiled. Form the cluster with an
  absolute `target/release/sinomo` path and retain the formation artifacts
  before describing the result as a release ceiling.
- `sustained-churn` keeps the normal isolated and concurrent gates, then runs
  eight back-to-back concurrent three-shard rounds by default. It applies the
  same command, retry, idle, throttle, health, memory, and OOM checks across
  the longer window and adds a conservative 1,000 aggregate publications/s
  sustained floor. `SINOMO_DFS_PERF_SUSTAINED_ROUNDS` and
  `SINOMO_DFS_PERF_MIN_SUSTAINED_AGGREGATE_FILES_PER_SECOND` are explicit
  overrides. The first sustained round creates one stable file set per shard
  lane; subsequent rounds pass `overwrite-existing=true` to overwrite those
  exact paths. The probe records that mode and the harness verifies it. This
  keeps the live namespace bounded while exercising version/history retention,
  metadata GC, retries, and replay across a high operation count.

For example:

```sh
# Form a fresh retained project with the exact release binary being measured.
cargo build --release -p sinomo
SINOMO_TEST_BUILD=0 SINOMO_TEST_FORMATION_OBSERVE=0 \
  SINOMO_TEST_BINARY="$PWD/target/release/sinomo" \
  SINOMO_TEST_COMPOSE_OVERRIDE=docker/docker-compose.dfs-tmpfs.yml \
  SINOMO_TEST_NETWORK_ONLY=1 SINOMO_TEST_KEEP=1 SINOMO_DFS_PERF_TRACE=1 \
  docker/test-cluster.sh

SINOMO_DFS_PERF_MODE=release-ceiling \
  SINOMO_DFS_PERF_BINARY_PROFILE=release \
  SINOMO_DFS_PERF_FILES=16384 \
  SINOMO_DFS_PERF_COMPOSE_OVERRIDE=docker/docker-compose.dfs-tmpfs.yml \
  docker/measure-dfs-shards.sh

SINOMO_DFS_PERF_MODE=sustained-churn \
  SINOMO_DFS_PERF_SUSTAINED_ROUNDS=12 \
  SINOMO_DFS_PERF_FILES=4096 \
  SINOMO_DFS_PERF_COMPOSE_OVERRIDE=docker/docker-compose.dfs-tmpfs.yml \
  docker/measure-dfs-shards.sh
```

`SINOMO_DFS_PERF_BATCH_SIZE_SWEEP=1024,4096,16384` runs the complete configured
gate once per files-per-bulk-call value and writes one child artifact directory
plus `batch-sweep-summary.tsv`. It stops on the first failed child. The sweep
uses the same live cluster without resetting its volumes, so order sizes from
small to large, start from a fresh project, and interpret later children as
higher-history measurements. Every child retains the configured 1,000 floors
and minimum measurement window; a very small batch that is too short to
measure is a failed gate, not silently accepted sweep data.

When DFS performance tracing was enabled when the daemons started, the shard
script captures the exact appended byte range of both
`/data/daemon/main.stdout.log` and `main.stderr.log`. It extracts only existing
`sinomo.dfs.perf` numeric records into `dfs-perf-timing-records.tsv` and writes
cluster percentiles to `dfs-perf-timing-summary.tsv`. The companion
`dfs-perf-timing-coverage.tsv` is explicit about the instrumentation boundary:

- `metadata-journal-segment` exposes authoritative journal encode, durable
  file append/sync, first-segment directory sync, bytes, and entry counts;
- `metadata-table-batch` exposes materialized replay-cache encode and unsynced
  append duration;
- `posix-bulk-local-write` and `posix-bulk-authority-commit` split content
  publication, prepare construction, per-authority commit, and final stat
  projection;
- `metadata-authority-confirmation` distinguishes replica-probe, assignment
  revalidation, and cluster-quorum permit time;
- `content-root-catalog-shard-refresh` and `content-root-catalog-refresh`
  expose exact-cut page/entry counts and shard/installed-generation latency;
- `packed-blob-lock` exposes packed-content lock wait and hold duration, while
  content ingest records expose loose/streamed write timing when those paths
  run;
- `metadata-publish-plan` exposes replication planning only.

There is currently no successful packed-blob append/sync duration record and no
successful metadata-journal transport latency record. The coverage artifact
marks those as `not-instrumented`; it never turns an absent record into a zero
duration. A supported record with no samples is `not-observed`, which can also
mean the workload missed that path or `SINOMO_DFS_PERF_TRACE` was not active in
the already-running daemon. Raw captured logs remain authoritative. Timing
summaries can be rebuilt without contacting Docker:

```sh
docker/measure-dfs-shards.sh --summarize-traces target/dfs-shard-perf/<run>
```

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
`memory-summary.csv`, `memory-phase-summary.csv`, `host-memory.csv`,
`host-memory-summary.tsv`,
`sustained-summary.tsv`, `sustained-rounds.tsv`,
`main-daemon-stderr-workload.tsv`, `main-daemon-stdout-workload.tsv`, the
per-node `main-daemon-<node>.workload.stderr.log` and
`main-daemon-<node>.workload.stdout.log` files,
`dfs-perf-timing-records.tsv`, `dfs-perf-timing-summary.tsv`,
`dfs-perf-timing-coverage.tsv`, and the before/after health, doctor, DFS-status,
and shard-layout files. The daemon-log manifests record the exact appended byte
range or a best-effort full-log fallback when a file was reset or an offset was
unavailable. The timeline gate fails a runnable low-CPU streak with no resource
wait or progress; sleeping samples with no observed external progress remain
diagnostic, so inspect that class alongside the preserved
`sinomo.dfs.perf` traces before concluding that the hot path has no unnecessary
wait.
