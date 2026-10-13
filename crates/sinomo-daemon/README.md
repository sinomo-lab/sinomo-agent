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
- Daemon-local control protocol framing, authentication checks, replay
  protection, connection lifetime, response routing, and stream-coupled RPC
  plumbing, plus the server adapters for the `sinomo-api` daemon contracts.
- Component hosting: the live service-registry snapshot, the
  `sinomo_module::ComponentReconciler`, publication of the module runtime
  generation, the component event bus, and the generic host adapters handed to
  components (data directory, cluster access, target-addressed object-RPC
  transport, activity counters, node resources, and host role).
- Event loop scheduling for timers, peer connections, local sockets, shutdown,
  and service dispatch.
- Host-specific adapters for filesystem paths, installed binaries, process
  spawning, boot integration, and OS-level observation.

`sinomo-daemon` does not own domain behavior:

- Product composition. The caller supplies a `ComponentCatalog` (normally built
  by `sinomo-product-profile`); the daemon executes its factories,
  registrations, storage contributions, and status/recovery/migration hooks
  without keeping a built-in list of its own.
- Cluster membership semantics, route selection, health evaluation, join
  reconciliation, Raft quorum logic, cluster KV rules, or peer graph algorithms.
- DFS namespace, object storage, metadata, journal, snapshot, maintenance, and
  data-plane protocol semantics.
- Rollout planning, evaluation, promotion, rollback, binary selection, and
  cluster rollout state machines.
- Sandbox policy, toolchain behavior, filesystem semantics, or execution
  rules.
- Module lifecycle, registry rules, service manifests, native-dylib loading and
  invocation, or activation state semantics.
- Persistent storage layouts for cluster, DFS, rollout, sandbox, registry, or
  other domain crates.

If a behavior can be tested without starting the daemon process, it probably
belongs outside this crate.

## Route Handling Rules

New object-RPC routes should be registered through `sinomo-service` and owned by
the crate that owns the route semantics. The daemon may decode the daemon wire
command, create a `ServiceContext`, dispatch to the registry, and encode the
response. It should not implement the route's domain algorithm inline.

All ordinary object-RPC methods dispatch through the `sinomo-service` registry.
Daemon control may recognize transport preludes and bootstrap-only handshakes,
but it must pass a decoded service request to that same registry rather than
maintaining another route table.

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

Within this crate, import R0 surfaces through `sinomo-core` and name the few R1
dependencies directly (`sinomo-api`, `sinomo-config`, `sinomo-device`,
`sinomo-module`). The `sinomo-runtime` host profile re-exports this crate, so
the daemon must not depend on that facade. Domain crates such as `sinomo-dfs`,
`sinomo-rollout`, and `sinomo-sandbox` are dev-dependencies only. Keep
daemon-only types private unless they are part of the daemon client/control API.

## Storage Boundary

Daemon-owned local state is limited to daemon process state, registered by
`sinomo_daemon::register_storage_paths`:

- `daemon/status.sobj` and `shadow-daemon/status.sobj`
- `daemon/daemon.lock`, `daemon/data-owner.lock`, `daemon/sifs-mount.lock`, and
  `shadow-daemon/daemon.lock`
- `daemon/auth-nonces.sobj` and `.sobjlog` (and the shadow equivalents) for
  control-protocol replay protection
- `daemon/main.stdout.log` and `daemon/main.stderr.log`
- installed OS boot entries and launch scripts, registered as an escape path

Cluster state, DFS content, rollout build logs, registry state, sandbox
artifacts, and module state belong to their corresponding crates, even when the
daemon opens the files or triggers maintenance.

## Current Network/KV Boundary

Cluster networking and KV storage are separate R0 domains. `sinomo-network` and
`sinomo-kv` have no production dependency on one another, and `sinomo-cluster`
is the only bridge: it carries KV/Raft traffic over network-selected routes and
projects committed cluster facts through the read-only network directory. The
ownership table and invariants live in
[clustering architecture](../../docs/clustering-architecture.md#ownership-boundaries)
and [network architecture](../../docs/network-architecture.md); this README
does not restate them.

The daemon implements the host traits those crates declare, for example
`sinomo_core::cluster::ClusterControlPlaneTransport` and the KV quorum hosts, and
supplies authentication context, local identity and signing, data-directory
access, generic daemon RPC transport, response routing, and timers. The
cluster-network maintenance loop is the generic `sinomo-cluster` reactor; the
daemon implements only the host operations that touch its listener, process
identity, and transport. New Raft, routing, or membership semantics belong in
their owning crates rather than in daemon glue. The crate root re-exports only
CLI/runtime entry points and stable daemon contracts; control-loop helpers,
frame codecs, maintenance triggers, and low-level peer sends stay internal
unless another crate has a reviewed integration need.

## Component-Owned Domains

DFS, Sandbox, Observe, Build, and the DFS history and attachment features are
components in the product catalog. Each component scope owns its routes,
workers, storage contributions, and recovery; the daemon does not construct DFS
adapters, decode Sandbox requests, enumerate their routes, or schedule their
work. Route-specific checks such as exact-member identity are declared by the
owning module and enforced from generic service metadata.

Rollout routes, desired-state watching, shadow finalization, and process
activation live in the `sinomo-rollout` crate family; `sinomo-rollout-host` is
the trusted daemon-process adapter selected by the product profile. The daemon
keeps only generic process mechanics such as spawning the shadow rollout
recovery helper and requesting a supervised restart. Daemon-originated
operational records are published on the component event bus; the daemon never
writes Observe-owned files. See [architecture](../../docs/architecture.md) for
the component-composition contract.

## Transitional Areas

The daemon still contains substantial logic that should move to owning crates
over time. The largest areas are:

- `cluster.rs`: Raft append/vote transport and member catch-up, cluster control
  (init, invite, join, leave, remove, rename, role), connectivity probing,
  node-status publication, and daemon-specific cluster transport adapters.
- `control.rs`: listener lifecycle, bootstrap handshakes, KV committed-state
  observation, and daemon process coordination around service-registry
  dispatch.
- `service_adapter.rs`: the component/service-registry host, daemon context and
  process-state adapters, module hot unload, and asynchronous route handlers.
- `client.rs`: generic daemon and foundational cluster transport for callers.

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

Good next slices are:

- Finish moving cluster control-plane and Raft transport decisions out of
  daemon-owned files.
- Move remaining cluster membership reconciliation and maintenance into the
  owning network/cluster crate.
- Shrink daemon public exports as callers move to `sinomo-runtime` or owning
  crates.
