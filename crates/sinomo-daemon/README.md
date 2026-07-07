# sinomo-daemon

`sinomo-daemon` is the R1 host shell for the local Sinomo daemon and shadow
daemon. It owns process lifecycle, daemon-local control protocol mechanics, and
the event loop that connects local clients, cluster peers, and service
implementations.

The daemon is intentionally not the owner of cluster, DFS, rollout, sandbox,
module, registry, or KV semantics. Those behaviors belong to their domain
crates. This crate should stay a thin event loop shell that adapts host
resources to service-owned logic.

See also:

- [`docs/architecture.md`](../../docs/architecture.md) for runtime planes,
  events, trust rings, and crate responsibility boundaries.
- [`docs/module-lifecycle-architecture.md`](../../docs/module-lifecycle-architecture.md)
  for module lifecycle ownership, cluster-authoritative desired state, and
  runtime assembly rules.
- [`../sinomo-service`](../sinomo-service) for the host-agnostic service
  registry used by daemon route dispatch.
- [`../sinomo-network`](../sinomo-network) for cluster routing, membership,
  connectivity graph interpretation, and transport orchestration.
- [`../sinomo-kv`](../sinomo-kv) for KV transactions, watches, snapshots, and
  durable Raft storage mechanics.
- [`../sinomo-dfs`](../sinomo-dfs) for DFS-owned filesystem and data-plane
  behavior.

## Ownership Contract

`sinomo-daemon` owns host process mechanics:

- Main and shadow daemon lifecycle: `run`, `start`, `status`, `stop`, process
  supervision, pid observation, lock files, and status files.
- Daemon-local control protocol framing, authentication checks, connection
  lifetime, response routing, and stream-coupled RPC plumbing.
- Runtime wiring from daemon state into `sinomo-service::ServiceContext`,
  including data directory handles, optional cluster clients, signer access,
  node identity, and response sinks.
- Event loop scheduling for timers, peer connections, local sockets, shutdown,
  and service dispatch.
- Host-specific adapters for filesystem paths, installed binaries, process
  spawning, boot integration, and OS-level observation.

`sinomo-daemon` does not own domain behavior:

- Cluster membership semantics, route selection, health evaluation, join
  reconciliation, Raft quorum logic, cluster KV rules, or peer graph algorithms.
- DFS namespace, object storage, metadata, journal, snapshot, maintenance, and
  data-plane protocol semantics.
- Rollout planning, evaluation, promotion, rollback, binary selection, and
  cluster rollout state machines.
- Sandbox policy, toolchain behavior, filesystem semantics, or execution
  rules.
- Module lifecycle, registry rules, service manifests, generated registration
  tables, or activation state semantics.
- Persistent storage layouts for cluster, DFS, rollout, sandbox, registry, or
  other domain crates.

If a behavior can be tested without starting the daemon process, it probably
belongs outside this crate.

## Route Handling Rules

New object-RPC routes should be registered through `sinomo-service` and owned by
the crate that owns the route semantics. The daemon may decode the daemon wire
command, create a `ServiceContext`, dispatch to the registry, and encode the
response. It should not implement the route's domain algorithm inline.

Hardcoded fallback routes in daemon control code are migration debt. They are
acceptable only when a route is still being moved to a service implementation or
when the route is inherently stream-coupled, process-coupled, or daemon-local.
Adding a new hardcoded route should be treated as exceptional and should include
a clear reason in code or tests.

When direct service implementation in the owning crate would create a dependency
cycle, keep the pure domain logic in the owning crate and add only a narrow
daemon adapter. If the adapter grows behavior or state, introduce a dedicated R1
crate rather than expanding `sinomo-daemon`.

## Dependency Rules

Domain crates must not depend on `sinomo-daemon`. They should expose domain APIs
or service implementations that the daemon can host.

Application and R2 crates should avoid importing daemon internals. Use
`sinomo-runtime` or the owning domain crate for domain behavior. Import
`sinomo-daemon` only for daemon lifecycle, status, local client, and control
protocol utilities.

Within this crate, prefer importing R0/R1 surfaces through `sinomo-runtime`
where that matches the workspace ring boundary model. Keep daemon-only types
private unless they are part of the daemon client/control API.

## Storage Boundary

Daemon-owned local state is limited to daemon process state, for example:

- `daemon/daemon.lock`
- `daemon/status.json`
- `shadow-daemon/daemon.lock`
- `shadow-daemon/status.json`
- daemon-local auth nonce or control protocol files

Cluster state, DFS content, rollout build logs, registry state, sandbox
artifacts, and module state belong to their corresponding crates, even when the
daemon opens the files or triggers maintenance.

## Target Network/KV Boundary

Cluster networking and KV storage are separate domains. Their coupling is real
but narrow: KV replication and KV RPC traffic use the cluster network for data
transmission, while network routing reads stable cluster facts through a small
directory surface, including the authoritative connectivity graph published in
cluster KV by `sinomo-cluster`. The local connectivity file is a bootstrap cache
used before the directory graph is readable. Those facts do not make either
crate the owner of the other's domain.

The target contract is:

- `sinomo-network` owns the cluster network and routing layer: route choice,
  endpoint selection, endpoint scoring, relay decisions, peer transport
  contracts, inbound peer authorization, connectivity graph interpretation,
  membership routing decisions, join-registration network validation, cluster
  health probe planning, and network maintenance cadence. Connectivity records
  are network-domain facts published to cluster KV by the cluster control plane
  and read by network through the directory interface. Local connectivity files
  remain only as bootstrap caches. Network must not apply KV commands, manage
  watches, own Raft leaders/elections, own Raft logs, compact snapshots, or
  define durable KV storage layout.
- `sinomo-kv` owns KV storage semantics: keyspace revisions, transactions,
  range reads, watches, snapshots, leases, Raft participant state,
  leader/election state, term/vote/log/snapshot behavior, compaction, range
  limits, and durable KV layout rules. It may use network-owned transport
  capabilities to replicate or serve KV traffic across nodes. KV decides what
  bytes and log entries must be replicated and when a node has won KV Raft
  leadership; network decides which peer route carries the vote/append traffic.
  KV must not choose peer routes, score endpoints, relay packets, or interpret
  cluster connectivity topology.
- `sinomo-daemon` owns host mechanics for both crates: daemon authentication
  context, local process probing, local identity/signing, data-directory access,
  generic daemon RPC transport, response routing, timers, and the small adapter
  objects needed to let network-owned and KV-owned logic run inside the daemon
  event loop.

The coupling must be represented as explicit interfaces, not shared ownership:

- KV-to-network coupling should be a transport capability that sends KV/Raft RPC
  envelopes over routes selected by `sinomo-network`. The transport capability
  can be supplied by the daemon or by a network-owned adapter, but KV logic should
  see it as a send/replicate interface, not as routing internals.
- Network-to-KV coupling should be a read-only connectivity graph query
  capability backed by KV state. The query returns network records; it must not
  expose transaction, watch, compaction, or Raft mechanics to routing code.
- KV route/category/body payload contracts should live in `sinomo-kv`; the
  network layer may wrap those payloads in its generic object-RPC envelope and
  choose the peer route, but should not be the source of truth for KV payload
  semantics.
- Local KV read/watch route dispatch should live in `sinomo-kv`. Cross-node
  watch-event fanout may still require network/cluster validation before events
  are appended locally, but the durable watch storage semantics should not live
  in the daemon.
- Shared wire payloads or graph data types should live in the owning crate or a
  focused contract crate. They should not cause `sinomo-network` to become a KV
  implementation or `sinomo-kv` to become a router. If a Rust crate dependency
  would imply the wrong ownership direction, introduce explicit traits or a small
  contract crate instead of hiding the dependency inside a broad module.

Current migration debt:

- `sinomo-network` still depends directly on `sinomo-kv` and still contains the
  backing filesystem implementation for cluster KV range/watch helpers, KV/Raft
  endpoint RPC envelope adapters, and some Raft helper orchestration because the
  existing daemon transport and storage layout grew there first. This is not the
  target model.
- Those surfaces should split into two explicit interfaces: a `sinomo-kv` service
  API with network transport traits for KV/Raft replication, and a
  network-owned connectivity graph query backed by KV state. `sinomo-network`
  should keep only routing, membership, endpoint, relay, connectivity graph,
  and health behavior.
- `sinomo-daemon` still contains relay transport adapters, KV route storage
  adapters, DFS adapters, and broad membership maintenance glue. Raft append,
  vote, and cluster membership object-RPC decode/encode now belongs to
  `sinomo-network`; the daemon service supplies signer/process context, host
  callbacks, and response routing. Later slices should reduce the remaining
  adapters until the daemon is mostly lifecycle, auth, socket/event-loop
  wiring, and service dispatch.
- The crate root should re-export only CLI/runtime entry points and stable daemon
  contracts. Control-loop helpers, frame codecs, periodic maintenance ticks,
  and low-level peer-send helpers should stay crate-internal unless another
  crate has a real integration need.

## Current DFS Boundary

DFS route and data-plane behavior belongs to `sinomo-dfs`. The current boundary
is:

- `sinomo-dfs` owns DFS control-plane route constants, control-plane service
  behavior, data-plane object-RPC message processing for a single inbound
  message, endpoint peer transport through `DfsEndpointTransport`, metadata
  replication endpoint transport through `DfsMetadataEndpointTransport`,
  metadata publish retry scheduling through `DfsMetadataPublishScheduler` and
  the default scheduler, DFS peer RPC request/response codecs for data-plane
  messages and metadata replication, the post-mutation metadata publish event
  gate, cluster data-plane service assembly through `DfsDataPlaneService`,
  cluster maintenance tick assembly through `run_cluster_dfs_maintenance_tick`,
  cluster smoke assembly through `run_cluster_dfs_smoke`, metadata shard
  primary-transfer readiness, metadata backend semantics,
  content-store semantics, namespace jobs, and lifecycle maintenance algorithms.
- `sinomo-daemon` owns host adapters: local data-dir setup, local node identity,
  anti-entropy cursor files, cluster KV client wiring, generic signed daemon RPC
  transport for peer frames and metadata journal replication, process counters,
  and publish callbacks for cluster-visible metadata. Its DFS maintenance role
  is to call DFS-owned APIs, supply daemon senders/adapters, and log or count
  host-level outcomes.
- DFS default cluster bootstrap still needs a daemon host adapter for cluster KV
  commits and daemon endpoint publication; the bootstrap decision logic belongs
  to `sinomo-dfs`.

## Transitional Areas

The daemon currently contains substantial logic that should move to owning
crates over time. The largest areas are:

- `cluster.rs`: remaining Raft driver behavior, relay handling, membership
  maintenance, and daemon-specific cluster transport adapters.
- `dfs.rs`: DFS peer transport fan-out, metadata publishing adapters,
  maintenance adapters, and default bootstrap host adapters.
- `rollout.rs`: binary rollout process glue mixed with rollout-state behavior.
- `control.rs`: hardcoded route fallbacks after service registry dispatch.
- `service_adapter.rs`: temporary bridge from synchronous daemon command
  handling to service-owned asynchronous route handlers.

Refactors should reduce these files by moving domain logic to the owning crate
or to a focused R1 crate, while keeping daemon adapters small and mechanical.

## Change Checklist

When adding daemon-adjacent functionality:

1. Identify the owning crate for the domain behavior before editing daemon
   handlers.
2. Put domain state transitions, validation, storage layout, and tests in that
   owning crate.
3. Register object-RPC routes through `sinomo-service` when the route is not
   daemon-local.
4. Keep daemon code to event loop wiring, authentication, transport adaptation,
   process lifecycle, and response routing.
5. Add daemon tests only for adapter behavior, control protocol behavior,
   process lifecycle, or integration across the local daemon boundary.
6. Run focused validation for the owning crate and `sinomo-daemon`; use
   workspace-level checks for cross-crate contract changes.

Useful validation commands:

```bash
cargo fmt --all
cargo check -p sinomo-daemon --all-targets
cargo test -p sinomo-daemon
cargo test -p <owning-crate>
```

## Migration Priorities

Good first slices are:

- Split cluster KV storage from network routing by moving KV range/watch
  handling, KV/Raft route codecs, and Raft storage helper orchestration toward
  `sinomo-kv`.
- Introduce explicit KV-to-network transport traits for KV/Raft traffic so KV can
  request replication without owning peer route selection.
- Keep route interpretation in `sinomo-network` while publishing source-owned
  connectivity records into cluster KV through the explicit directory/query
  seam; local connectivity files are bootstrap cache only.
- Finish moving cluster control-plane and Raft transport decisions out of
  daemon-owned files.
- Move remaining cluster membership reconciliation and maintenance into the
  owning network/cluster crate.
- Move DFS protocol, snapshot, journal, and maintenance algorithms behind
  `sinomo-dfs` APIs or services.
- Separate rollout binary host actions from rollout planning and state-machine
  logic.
- Shrink daemon public exports as callers move to `sinomo-runtime` or owning
  crates.
