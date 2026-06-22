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

## Metrics Model

There are two metric sources:

1. Runtime metrics recorded through the `metrics` API.
2. Snapshot exporters registered with `sinomo-observe`.

Snapshot exporters are pull-based. They run only when `/observe/metrics` is
queried, which keeps durable state scans out of runtime hot paths. Exporters
return normal `ObserveMetricRecord` values and may report exporter failures as
`sinomo.observe.metric_exporter.errors`.

The daemon registers built-in DFS and KV exporters:

- `sinomo.dfs.*` reports DFS object, version, receipt, intent, conflict, repair,
  namespace job, and metadata shard gauges.
- `sinomo.kv.*` reports local cluster KV revision, compacted revision, entry,
  lease, and retained event gauges.

Application-level crates register through daemon service hooks. The agent hook
currently exports `sinomo.agent.*` gauges for persisted sessions, control
snapshots, active turns, buffered events, task statuses, and read errors.

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
