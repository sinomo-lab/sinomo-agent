# Observability Architecture

Sinomo observability is owned by `sinomo-observe`. The crate provides the shared
runtime surface for process-local logging, tracing, metrics capture, bounded
trace reads, and live trace control. Other crates should instrument their own
behavior, but cluster-wide observation should flow through the observe routes
instead of adding domain-specific debug transports.

## Runtime Shape

DFS background work exposes bounded-label `sinomo.dfs.background` counters for
wakes, started/completed passes, outcomes and hydrated objects/receipts/intents,
plus duration histograms. Default-cluster lifecycle passes use the same counters
with a distinct worker label. `dfs.background`, `dfs.background.lifecycle`,
`dfs.background.probe` and
`dfs.metadata.hydration` spans connect those passes to their storage and RPC
children. Counters survive trace-ring eviction; durations include waiting and
are not CPU time. An unfinished pass is not reported as successful.
`dfs.metadata.migration` separately reports catalog scan/migration counts,
completion, bounded continuation and waits for replica-prefix certification.

The generic buffered performance collector accepts `--metrics-prefix` to retain
counter snapshots beside trace/process windows. Its reporter requires complete
snapshots from the same process before computing deltas. Missing, newly
registered or reset counters retain raw endpoints and an unavailable delta.
Docker quietness runs `docker/profile-idle.py` alongside its continuous CPU gate,
producing the standard stage/I/O, node-window, activity and coverage report plus
CPU profiles. Main-process profiles diagnose work; whole-container CPU remains
the gate authority and includes shadows, helpers and collection overhead.
DFS metric export reads only the bounded resident status projection. A missing
or busy projection remains an explicit partial-export error; observation never
falls back to opening and decoding the metadata store. Requests for metric
prefixes outside that exporter's catalog skip status collection. Parallel
metadata-open workers retain their caller's trace context through the shared
blocking-work observation helper.
Observe's built-in samplers do not register a second DFS inventory reader;
the component-owned exporter is the single source for DFS inventory metrics.

DFS emits structured `sinomo.dfs.catchup` warnings when a routed write, namespace
operation, durability barrier or steady-state replica publisher falls back to a
snapshot or historical journal backfill (a full page, or replay from zero).
The publisher's initial synchronization and observed transport-loss recovery are
logged at info level until an exact successful synchronization converges. Unknown
contexts remain warnings; neither a zero cursor nor a background task silently
exempts an event. Fields identify node, peer, shard, operation, kind, cursor range
and cause.

Catch-up diagnostics emit at most once per minute per node/peer/shard/operation/
kind/severity, with a process-wide cap of 16 records per minute and bounded
256-entry maps. Subsequent records include per-key and process suppression
counts. The `sinomo.dfs.metadata_catchup` counter counts each observed catch-up
boundary, including suppressed logs, using bounded operation/kind/context labels.
Sender and receiver observations are not unique cluster-wide repair counts. Request IDs, paths
and free-form error text are excluded from deduplication keys and metric labels.
These observations do not affect routing, authority or recovery decisions.

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
  through the connected daemon's ordinary `/observe/logs` route. Remote
  observation requests require main-daemon process affinity, so a node's shadow
  daemon cannot silently replace its active service logs or metrics. Collection
  uses at most eight concurrent requests and 32 nodes per batch; omitted nodes
  remain individually selectable. Search, minimum severity, pause/resume and
  manual refresh apply to the selected scope. The virtualized stream preserves
  controls and scroll position. Every entry displays its full timestamp, node,
  process, component, operation ID, message and structured fields inline as
  selectable text; messages wrap and keep their line breaks. Node failures and
  source/retention limits stay visible.
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

## Generic stage performance capture

Observe collects generic stage lifetimes from `tracing` spans whose target is
`sinomo.perf` and whose `stage` field is a static bounded name. Start an explicit
`observe control --capture ID --filter '...,sinomo.perf=trace'` capture before
work, then read `observe traces --capture ID --limit 0 --json`. Existing cluster
member read and administrator control authorization still applies; this adds no
new route, durable control state or product composition dependency.

Each stage emits `span_started` and `span_closed` records into the bounded trace
ring. Records carry a process-unique sequence, a local monotonic timestamp,
root-to-leaf span contexts and the nearest nonempty inherited `traceparent`.
Explicit event traceparent fields take precedence. Span IDs are unique within a
process incarnation and are distinct from wire W3C span IDs. Span field updates
are retained; colliding ancestor fields are not flattened. Scope depth is bounded
at 32 and scope field content at 16 KiB; truncated context is explicit. Existing
per-event 16 KiB field limits and the 4,096-record ring remain in force. The
additive fields deserialize old exports with missing context as unavailable.

Stage close reports elapsed lifetime in nanoseconds, not CPU time. A stage
crossing capture expiry/clear is labelled `capture_complete=false`, retains its
original capture identity and must not be treated as an uncensored sample.
Comparing start/close IDs exposes unfinished work or lost start records. Closing
an async span does not establish operation success: use a bounded `outcome`
field where known and retain `unknown` otherwise (including possible cancellation).
Use `tracing::Instrument` across awaits and carry spans explicitly to spawned
or blocking tasks. Never hold an entered guard across await. Synchronous DFS
entrypoints install their carried RPC context in a generic `dfs.request` span.
The streamed RPC sender propagates the active span to its owned send task.

Current DFS stage boundaries cover admission and blocking-pool queue/work,
browser preparation/POSIX writes, local bytes/reader and
manifest writes, remote routing, content ingest, packed storage, journal prepare
and append, commit, authority, and fresh replica acknowledgement/probe work.
Network spans cover the routed call, service exchange, server stream, handler
and individual stream-record read/write waits. Existing RPC request IDs identify
client/server exchange pairs without changing the wire contract. Observation
RPCs are excluded from workload-stage summaries; their traffic remains in node
window counters.
The existing network timeline supplies finer routing, encoding, authentication
and dispatch phases when separately enabled. Existing `sinomo.dfs.perf` event
records now retain their admission/maintenance ancestor context as well.

The batched metadata commit exposes `dfs.commit.phase` spans for receipt readiness,
history/configuration refresh, planning, hidden-prepare persistence, operation
binding, prepare replication/authorization, fresh publication-authority validation,
visible publication, notification, coordinator completion, and cache/repair-root
updates. The reporter appends the bounded `phase` field to the stage name.
`dfs.history.refresh` further separates configuration fetch/cache, history-codec
capability validation, shard resolution and backend configuration synchronization.
Configuration admission's `dfs.metadata.codec_admission` includes one
`dfs.metadata.codec_member` per assigned node, with separate member-record and
capability-policy reads plus local validation. Ordinary writes use the admitted
codec and must not repeat this capability-read phase.
`dfs.control_plane.entries_fetch` identifies fresh authoritative reads and their
key counts, including deadline-bounded publication reads. These nested rows explain the commit
critical path; do not add them again to the enclosing commit duration. History
refresh binds publication to the current policy and does not imply that a previous
file version is retained or copied.
Shard lookup exposes `dfs.control_plane.shards` read-prefix/decode/sort phases,
with nested daemon authority synchronization, prefix-cache lookup and authoritative
prefix-fetch stages. These distinguish local projection work from a fresh cluster
read without recording configuration keys or values.

`sinomo-storage::io_trace` (through `sinomo_core::storage` for R1 callers) records
logical read/write bytes, operation outcomes and sync lifetimes. It instruments
the shared durable file paths, checksummed content frames, private ingest and
packed appends. File write counts include successful bytes before an error.
These are API operation counts, not syscall counts or physical storage traffic;
record/object reads can also include allocation, validation and seek cost.
Uninstrumented direct reads, mmap, metadata opens, renames and device I/O are
not inferred from missing records. No file contents or paths enter these hooks.
The disabled write wrapper uses the original standard-library write path.

Network I/O events count successful logical SOF2 frames and stream-native RPC
records, including body chunks, once at the RPC record boundary. Byte-stream
records include SOR2 prefixes; owned records count the complete logical record
handed to/from the reliable transport. Partial failed network transfers are not
counted as successful bytes. These figures exclude lower transport framing,
retransmission and relay forwarding. A missing peer/trace context remains an
attribution gap. Do not sum these with network-timeline bytes at other layers.

Every trace response includes node/process role, PID, startup time and an
observation monotonic time. On Linux it also samples `/proc/self/io` and
non-loopback `/proc/self/net/dev` counters. Missing or unsupported counters are
null, not zero. Process I/O covers background work and observation itself;
kernel writeback can fall outside the logical write window. Network-namespace
counters cover all processes and interfaces in that namespace, including relay
traffic; a forwarded packet can contribute on multiple interfaces. Separate
incarnations and counter resets when taking deltas. Remote monotonic timestamps
cannot be subtracted to infer one-way latency.

Use `scripts/debug/capture-performance.py --help` for a bounded multi-node
collector around an explicit workload. It refuses an existing capture, records
attempted starts before RPC completion (timeouts can mean applied), polls nodes
concurrently, and clears only its owned capture. The existing ten-minute lease
restores filters if the collector crashes. It does not start/reset clusters or
prove executable identity: retain independent main/shadow binary hashes and a
source patch with the supplied identity manifest. Use short buffered waves for
stage attribution. Docker acceptance keeps observation enabled and uses the
measured latency without overhead corrections or relaxed thresholds.

`scripts/debug/report-performance.py CAPTURE_DIR` emits `performance.json` and
`PERFORMANCE.md`. It deduplicates repeated snapshots by process/sequence, keeps
per-stage nearest-rank latency percentiles, direct and inclusive I/O, process
window deltas, individual causal span links and explicit coverage failures.
Inclusive stage rows overlap and must not be summed. Nodes with no samples remain
in the coverage table. A partial capture returns exit code 2. The repository
[performance analysis skill](../skills/sinomo-performance-analysis/SKILL.md)
defines the standard acceptance, stage, node-window and coverage report tables.

For a local recorder/I/O/report smoke test without a cluster:

```sh
cargo run -p sinomo-observe --example profile-file-io -- target/local-io-capture
python scripts/debug/report-performance.py target/local-io-capture
```

The output directory must be new. This performs 20 real 4 KiB overwrite, sync
and verified-read cycles; its timings are local instrumentation evidence and
cannot establish DFS or cluster performance.

### Incremental export and operation scope

Trace reads accept an exclusive `after_sequence` cursor, exposed by the CLI as
`observe traces --after-sequence N`. Responses provide `last_sequence` and
`first_available_sequence`. A caller advances its per-process cursor even when
filters return no records. Eviction of records already exported does not mark an
incremental response truncated; a cursor behind the retained ring, ahead of the
process counter, or an explicit result limit that omits records does. Cursor use
with CLI `--all` is rejected because each process has its own sequence space.
The collector retains process identity and stops on a daemon replacement.

The collector uses incremental reads automatically. `--clients-file` accepts a
JSON map from node IDs to client argument arrays, allowing local Docker exec or
other explicitly configured per-node clients instead of sending all exports
through one node. This reduces collection traffic on shared relay paths. Capture
start/stop and every process-window counter still include observation overhead.

RPC spans include bounded `rpc_route` fields and wire request IDs; neither is a
metric label. Reports exclude the complete forwarding ancestry of marked
observation calls. `report-performance.py CAPTURE --root-stage dfs.browser.write
--expected-roots N` writes separate `WORKLOAD.md`/`workload.json` files. It selects
foreground request envelopes, follows local span ancestry, and joins outgoing
service RPCs to remote server spans by exact request ID. Ambiguous or unmatched
joins and selected incomplete spans are explicit coverage failures. This works
without assuming that an optional W3C traceparent exists or that node clocks are
synchronized. The whole-capture table remains separate from selected-operation
coverage, and kernel/network-namespace windows always include all process work.

`sinomo_core::service::observed_blocking_task` carries both the tracing dispatcher
and parent span into a blocking closure and separates queue/work lifetimes. The
browser adapter and daemon service adapter use this shared wrapper; browser
execution admission has its own span. The wrapper does not own executor policy,
cancellation or durable capabilities. A `completed` field describes whether the
closure ran/returned, independently of the domain result. No entered guard is
held across an await.

Observe suppresses `sinomo.perf` TRACE instrumentation inside its own requests,
including forwarding and response-body transport. A single marker identifies
the request before a trace snapshot is read; suppression stops at the nearest
RPC server envelope and never marks a shared connection or daemon span. This
prevents trace export traffic from recursively filling the ring. Warnings and
other non-performance logging remain visible. Metadata/routed DFS fan-out workers
carry their parent context into both scoped threads and async tasks; worker-slot
and deadline ownership are unchanged. File EOF reads explicitly report zero
logical bytes rather than unknown bytes.

### Buffered short captures

For busy DFS workloads use `capture-performance.py --buffered`. Capture controls
start concurrently on the selected nodes, counter-only snapshots bracket the
workload, and filters are restored on all nodes before trace export. Export
snapshots are excluded from process I/O deltas. This avoids perturbing the
workload with repeated large trace responses. Start with one or two operations;
check truncation and root counts before increasing the window.

`SINOMO_OBSERVE_TRACE_RECORDS` sets the process-local trace ring capacity at
startup (default 4,096, hard maximum 32,768). Invalid values fail initialization;
ordinary processes retain the default. Trace responses report the configured
capacity. A collector may require a capacity with `--minimum-capacity`; it never
silently changes it or restarts nodes. The record, field-size and scope-depth
bounds remain in force, and a full ring still reports loss. Buffered export
cannot recover records evicted before the capture stopped.

Docker verification enables debug logging, DFS/generic tracing, a 32,768-record
ring and CPU profiler support by default. Gates retain their thresholds with
observation enabled; timings are not adjusted for estimated overhead. The write
matrix automatically generates detailed stage reports with
`docker/profile-dfs-writes.py`. Its prepared-fixture mode uses the same daemon
write probe and verifies W/N before writing. The reporter can select one root by
node and span ID so create and overwrite stay separate without joining clocks.

Buffered collection can add `--cpu-profile`: it starts 49 Hz sampling on every
node, checks ownership, stops profiles before bulk trace export and records the
final artifact paths. CPU sampling skips allocator/loader/system-library
instruction ranges to avoid signal-unwinder deadlocks; the exact list is in each
profile manifest. These CPU exclusions do not remove stage timings or kernel I/O
counters. Per-operation stage attribution and whole-window process/CPU metrics
remain distinct.
