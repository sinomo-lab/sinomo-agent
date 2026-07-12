# R0/R1 Crate Boundary Refactor Plan

Status: completed and retained as an implementation record. Last reviewed
2026-07-09.

The facade migration flag is enabled and the workspace boundary test enforces
the resulting dependency graph. Stage descriptions below record the sequencing
and design reasoning at the time of the refactor; they are not an active task
list. Current rules live in [AGENTS.md](../../AGENTS.md) and the
[architecture overview](../architecture.md).

This plan reorganized the workspace crate boundary so that the trust-ring cut in
`docs/architecture.md` matches the crate graph precisely, turns `sinomo-network`
into a pure networking library with a runtime network-module system, and makes
the daemon the single cluster operator for CLI and app.

## Target ring model

| Ring tag | Crates | Rule |
|---|---|---|
| `r0` | `sinomo-base`, `sinomo-object`, `sinomo-storage`, `sinomo-local`, `sinomo-node`, `sinomo-events`, `sinomo-network`, `sinomo-kv`, `sinomo-cluster` (new), `sinomo-service` | May depend only on other `r0` crates. |
| `r0-boundary` | `sinomo-core` | The R0 facade: the minimal set of crates that defines the cluster. Depends on exactly the `r0` set. |
| `r1` | `sinomo-api`, `sinomo-daemon`, `sinomo-dfs`, `sinomo-dfs-cloud-files-api`, `sinomo-module`, `sinomo-module-api`, `sinomo-registry`, `sinomo-rollout`, `sinomo-policy`, `sinomo-eval`, `sinomo-config`, `sinomo-observe`, `sinomo-device`, `sinomo-supervision`, `sinomo-cli-registry`, `sinomo-test`, `sinomo-vfs`, `sinomo-vfs-sifs`, `sinomo-sandbox*`, modules/* packages | Depend on `sinomo-core` (never directly on `r0` crates once stage F flips `FACADE_MIGRATION_COMPLETE`) and on other `r1` crates. modules/ packages may also depend on `sinomo-runtime`. ABI exemption below. |
| `r1-boundary` | `sinomo-runtime` | The R1 facade. Depends on `sinomo-core` + `r1` crates only. Pure re-exports (behavior moves to `sinomo-daemon::registry` in stage E). |
| `r2` / `application` | unchanged | Cross the foundation only through `sinomo-runtime`. |

ABI exemption: `sinomo-module-api`, `sinomo-sandbox-wasix-api`,
`sinomo-dfs-cloud-files-api` may depend directly on `sinomo-object`.
`modules/sandbox/wasix` currently has direct r0 deps (network, kv, events,
node, object, storage); stage F migrates it to `sinomo-core` paths like every
other r1 crate.

Stage A (landed): `sinomo-base` holds the old `sinomo-core` sources;
`sinomo-core` is the R0 facade re-exporting base at the root plus
`object/storage/local/node/events/network/kv/service` modules (`cluster` joins
in stage B); R0 crates import `sinomo_base`, everything else keeps
`sinomo_core::...` paths; boundary tests rewritten with
`FACADE_MIGRATION_COMPLETE = false` until stage F.

## The three-crate cut: network / kv / cluster

The review established that the KV-coupling runs through files that must stay
in `sinomo-network` (endpoint_selector, route, route_graph_read_model,
control_plane_driver, daemon_auth, connectivity_store, network_info). The
resolution is a different partition than "kv absorbs kv_* files":

- **`sinomo-kv` does not change and gains no network dependency.** It is
  already transport-free: Raft state machine, models, WAL, snapshots, watches,
  route/auth policy constants, host traits (`ClusterKvRaft*Host`) that others
  implement.
- **`sinomo-cluster` (new r0 crate) owns the cluster control plane**: all
  transport-coupled orchestration that today lives in `sinomo-network`,
  covering BOTH the kv/raft drivers and membership. Deps: base, object, node,
  storage, local, events, network, kv.
- **`sinomo-network` keeps mechanism only** and loses its `sinomo-kv`
  dependency entirely. Everything it needs from cluster state arrives through
  provider traits it defines (below). KV and network no longer know each
  other; `sinomo-cluster` is the bridge that implements network's traits by
  reading KV. That is the "minimal, clearly defined intersection".

### Files moving from crates/sinomo-network/src to crates/sinomo-cluster/src

kv/raft control plane: `kv_forward.rs`, `kv_local_store.rs`,
`kv_read_model.rs`, `kv_semantics.rs`, `kv_snapshot.rs`, `kv_txn_quorum.rs`,
`raft_election.rs`, `raft_fanout.rs`, `raft_quorum.rs`, `raft_replication.rs`,
`raft_rpc.rs`, `control_plane_driver.rs` (the KV-typed
`ClusterControlPlaneTransport` + driver), `control_plane_adapters.rs`,
`control_plane_options.rs`, `state_authority_reachability.rs`.

membership/lifecycle: `invite.rs`, `invite_publish.rs`, `join_registration.rs`,
`member_mutation.rs`, `member_publish.rs`, `member_store.rs`,
`membership_forward.rs`, `membership_rpc.rs`, `local_membership.rs`,
`local_state_store.rs` (cluster bootstrap state I/O), `connectivity_publish.rs`
(KV sync half), `status_publish.rs`, `health.rs` (health rows/doctor; the
low-level ping RPC handler stays in network).

New in `sinomo-cluster`: object-RPC handlers for daemon-mediated cluster
control (`/cluster/control/{init,join,invite,rename,role,remove,leave}`)
wrapping the existing `*_with_host` drivers (stage G wires the CLI to them),
and the KV-backed implementations of network's provider traits.

`sinomo-cluster/src/lib.rs` starts with `pub use sinomo_base::cluster::*;` so
`sinomo_core::cluster::ClusterBootstrapState`-style paths resolve even though
the facade's `cluster` module shadows the base glob.

### Pure helpers that stay in network (moved between files, not crates)

These are record-shape predicates on network-owned types, used by staying
files; they move INTO the staying files so no network->cluster edge forms:

- `cluster_network_record_live`, `cluster_connectivity_stable_eq`,
  `validate_cluster_connectivity_records_for_source`,
  `MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS` -> `connectivity.rs`.
- `cluster_network_record_should_refresh` -> `network_info.rs`.
- `membership.rs::locally_learned_bridge_route_endpoint` and
  `cluster_is_settled_solo` -> `endpoint_records.rs` (file `membership.rs`
  dissolves).
- `keyspace.rs`: pure key builders -> `sinomo-node`;
  `cluster_node_status_snapshot_record(&KvSnapshot, ...)` -> `sinomo-cluster`
  (it takes a KvSnapshot; node must not depend on kv).
- `paths.rs` (`ClusterPaths`) STAYS in `sinomo-network` (moving it to
  sinomo-local creates local<->kv/network cycles). It drops its `kv_*` fields
  and `kv_paths()`; callers use `sinomo_kv::ClusterKvPaths::new(paths.root())`
  (grep-fix `.kv_paths()` call sites in cluster/daemon/cli).

### Provider traits (the network<->cluster/kv seam)

Defined in `sinomo-network`, implemented in `sinomo-cluster` (KV- or
bootstrap-state-backed). Sized from the review's evidence list — the trait set
must cover every read the staying files perform today:

```rust
/// Version stamp for cached routing state (KvRevision on the impl side).
pub struct DirectoryVersion(pub u64);

pub trait ClusterDirectory: Send + Sync {
    fn members(&self) -> Result<Vec<ClusterMemberRecord>, NetworkError>;
    fn endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError>;
    fn node_statuses(&self) -> Result<BTreeMap<NodeId, ClusterNodeStatusRecord>, NetworkError>;
    fn connectivity_records(&self) -> Result<Vec<ClusterConnectivityRecord>, NetworkError>;
    fn node_is_voter(&self, node: &NodeId) -> Result<bool, NetworkError>;
    fn version(&self) -> Result<DirectoryVersion, NetworkError>;
}
```

Consumers rewritten to take `&dyn ClusterDirectory` (or a generic) instead of
calling `kv_read_model`/`kv_local_store` directly: `endpoint_selector.rs`,
`route.rs` (cache mark uses `DirectoryVersion`), `route_graph_read_model.rs`.
`daemon_auth.rs` stops calling `read_cluster_state` at auth time; the signer
identity is passed in by the caller (daemon/cluster own that read).
`NetworkError` loses its `Kv(#[from] KvError)` variant; moved files convert
KvError into `sinomo-cluster`'s own `ClusterError` (with a lossy
`ClusterError -> NetworkError::Directory { detail }` mapping where trait impls
need it). The `sinomo_kv` block in network's prelude (lib.rs ~600) is deleted
first so the compiler enumerates every real usage during the split.

Routing/transport tests that exercise KV-backed state move with their features:
KV/raft/consensus scenarios -> `crates/sinomo-cluster/tests/` (and kv-specific
ones -> `crates/sinomo-kv/tests/`); the staying network tests may use
`sinomo-kv`/`sinomo-cluster` as dev-dependencies only (the boundary test parses
`[dependencies]` only — dev-deps are exempt by design; document this).

### Stage B consumer updates (verified list from the review)

- `sinomo-daemon` (cluster.rs ~80 call sites, control.rs, auth.rs, client.rs,
  service_adapter.rs, observe.rs), `sinomo-dfs` (default_cluster.rs,
  service.rs + 6 files), `sinomo-rollout` (cluster_kv.rs), `sinomo-sandbox`
  (cli.rs), `sinomo-config`/`sinomo-observe` if touched by compiler.
- `sinomo-runtime` gains `pub use sinomo_core::cluster;` (root, r0 and r1
  compat) — `sinomo-cli`/`sinomo-app`/`sinomo-evolution` reach moved items as
  `sinomo_runtime::cluster::...` (`ClusterHealthRow`, `cluster_doctor_findings`,
  `read_cluster_state`, `write_cluster_state`,
  `read_cluster_state_without_kv`, join/invite/member mutation drivers, ...).
- `sinomo-core` facade adds `pub mod cluster`.

## Network module system + iroh extraction

`sinomo-network` gains `src/network_module.rs`:

```rust
pub struct NetworkModuleDescriptor {
    pub id: String,                          // "iroh", "lan-tcp", "serial", "lora"
    pub transports: Vec<ClusterTransportKind>,
    pub address_schemes: Vec<String>,        // e.g. "iroh" for iroh:// — dispatch is
                                             // scheme-aware, not only kind-aware
    pub dialer: bool,
    pub listener: bool,
    pub supports_runtime_toggle: bool,
}

pub trait NetworkModule: Send + Sync {
    fn descriptor(&self) -> NetworkModuleDescriptor;
    fn start(&self, ctx: &NetworkModuleContext) -> Result<(), NetworkError>;
    fn stop(&self) -> Result<(), NetworkError>;
    fn open_rpc_stream(&self, endpoint: &ClusterEndpointRecord,
                       health: NetworkHealthPolicy)
        -> NetworkFuture<Box<dyn NetworkDuplexStream>>;
    // Bounded unary compatibility is an adapter over open_rpc_stream.
    fn dial_rpc(&self, endpoint: &ClusterEndpointRecord, request: &[u8],
                timeouts: DaemonRpcTimeouts) -> NetworkFuture<Vec<u8>>;
    fn local_endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError>;
}

pub struct NetworkModuleRegistry { /* arc-swap; register/set_enabled/dispatch */ }
```

- Dispatch: endpoint transport kind AND address scheme resolve to a module
  (today `iroh://` strings ride on LanTcp-tagged endpoints —
  roaming.rs:2289-2309). Loopback/LAN-TCP ship as built-in modules; their
  socket lifecycle remains internal to the built-in implementation, so no
  transport-specific framing leaks through the trait.
- `ClusterEndpointSelector` takes the registry's dialable-transport snapshot
  and filters/deprioritizes endpoints whose transport has no enabled dialer
  (review: otherwise iroh candidates burn per-endpoint timeout budgets when
  the module is absent).
- Runtime enable/disable via `NetworkModuleRegistry::set_enabled`; daemon
  persists the flag in module loader state and exposes `/network/modules`
  (list) + `/network/modules/set-enabled`. Disable stops the module but keeps
  the library loaded (registry holds the `Arc`; the dylib is never unloaded
  while vtable pointers are live).
- FFI: `sinomo-network::network_module_abi` defines the `repr(C)` vtable
  (`SinomoNetworkModuleVtableV1`, entry symbol
  `sinomo_network_module_create_v1`, ABI version const). Blocking dial calls
  wrapped in `spawn_blocking` by the host adapter.
- `sinomo-module` grows a raw-entry loader (`NativeLibrary` with
  content-hash verification and typed symbol resolution) alongside
  `NativeModule` — the existing loader hardcodes the six standard symbols and
  cannot resolve extra entry points (native_host.rs:96-156).
- `sinomo-module-api::NativeModuleSurfaces` gains
  `network_transports: Vec<String>` (name-keyed SOBJ, no deny_unknown_fields —
  backward compatible) and `sinomo-module` maps it into
  `ModuleSurfaces::network_transports`.

### Network-module FFI vtable (pinned for stage C)

`sinomo-network::network_module_abi` (new module) defines the dylib contract:

```rust
pub const SINOMO_NETWORK_MODULE_ABI_VERSION: u32 = 1;
pub const SINOMO_NETWORK_MODULE_ENTRY_SYMBOL: &[u8] = b"sinomo_network_module_create_v1";

#[repr(C)]
pub struct SinomoNetworkModuleConfigV1 {
    /// UTF-8 cluster data root (ClusterPaths::root).
    pub cluster_root_utf8: SinomoNetworkByteSlice,
    /// UTF-8 local node id.
    pub node_id_utf8: SinomoNetworkByteSlice,
    /// UTF-8 cluster id (empty when pre-cluster).
    pub cluster_id_utf8: SinomoNetworkByteSlice,
    /// UTF-8 local daemon control address for inbound forwarding.
    pub daemon_forward_address_utf8: SinomoNetworkByteSlice,
}

#[repr(C)]
pub struct SinomoNetworkModuleVtableV1 {
    pub abi_version: u32,
    /// SOBJ NetworkModuleDescriptor into a module-owned buffer.
    pub descriptor: unsafe extern "C" fn(*mut SinomoNetworkByteBuffer) -> i32,
    pub start: unsafe extern "C" fn(*mut c_void, *const SinomoNetworkModuleConfigV1) -> i32,
    pub stop: unsafe extern "C" fn(*mut c_void) -> i32,
    /// Open a raw bidirectional stream. Object-RPC framing stays host-owned.
    pub open_rpc_stream: unsafe extern "C" fn(
        *mut c_void,
        SinomoNetworkByteSlice,      // endpoint address utf8
        u64,                         // connect timeout ms
        u64,                         // no-progress timeout ms
        *mut SinomoNetworkRpcStreamV1,
        *mut SinomoNetworkByteBuffer // structured error
    ) -> i32,
    pub read_rpc_stream: unsafe extern "C" fn(/* stream, host buffer, ... */) -> i32,
    pub write_rpc_stream: unsafe extern "C" fn(/* stream, host bytes, ... */) -> i32,
    pub shutdown_rpc_stream_write: unsafe extern "C" fn(/* stream, ... */) -> i32,
    pub close_rpc_stream: unsafe extern "C" fn(SinomoNetworkRpcStreamV1),
    /// SOBJ Vec<ClusterEndpointRecord> the module currently advertises.
    pub local_endpoints: unsafe extern "C" fn(*mut c_void, *mut SinomoNetworkByteBuffer) -> i32,
    pub free_buffer: unsafe extern "C" fn(SinomoNetworkByteBuffer),
    pub destroy: unsafe extern "C" fn(*mut c_void),
}

/// Entry: `extern "C" fn(*mut *mut c_void, *mut SinomoNetworkModuleVtableV1) -> i32`
/// creating the instance and filling the vtable.
```

Byte slice/buffer structs mirror `sinomo-module-api`'s (`ptr/len[/cap]`).
Status codes mirror sinomo-module-api (0 ok, 1 invalid arg, 2 unsupported ABI,
3 runtime error). The host-side adapter (`sinomo-daemon::registry`) wraps the
vtable in the safe `NetworkModule` trait, holds the `libloading::Library`
alive for the registry's lifetime (disable = `stop()`, never unload), and
converts the descriptor/endpoint SOBJ payloads. Streamed object-RPC transfer is
a generic `sinomo-network` capability: built-in and dynamic transports expose
the same opaque full-duplex byte stream, while the host owns session framing,
flow control, retries, and relays. Modules do not advertise route- or
DFS-specific transfer capability flags.

### modules/network/iroh

Native-dylib package (cdylib+rlib, ring r1, not a default member,
`[package.metadata.sinomo.native-dylib]`). Depends on `sinomo-core` (links the
R0 graph — accepted; precedent: modules/sandbox/wasix) + `iroh` + `tokio`. It
receives the iroh-specific half of `roaming.rs`: endpoint builder/relay
config, secret-key load/create, proxy supervisor/accept loop/frame budget,
dial client, `iroh::EndpointAddr` plumbing. The proxy keeps forwarding inbound
frames to the daemon control address over TCP (roaming.rs:1479 -> 2352), so
the module only needs config, not host callbacks.

`sinomo-network` keeps `IrohEndpointRecord` (data + `iroh://` parse/format +
stable id + record file I/O — the on-disk record under `ClusterPaths::iroh` is
the pinned cross-ring contract), endpoint dedup/cleanup, selector ranking.
The `iroh` crate dep and `iroh-transport` feature are removed from
`sinomo-network` AND `sinomo-core` (which enables it today post-stage-A).
Direct in-process iroh dialing disappears from CLI paths: normal cluster
operations go through the daemon (stage G lands first), and direct debug/
repair commands are documented as LAN/loopback-only.
`sinomo-cli/src/tests.rs:6178` (start_iroh_daemon_proxy) moves/retires with
stage C.

## sinomo-service: registration, discovery, target selection

Keeps `Service`/`ServiceRegistry`/snapshot-store machinery and adds:

- `ServiceInstanceRecord { service, node_id, routes, capabilities, version,
  updated_at }` published under `/cluster/services/<service>/<node_id>`
  (daemon publishes on registry change + maintenance tick).
- `ServiceDiscovery`: read instances from a KV snapshot or `ClusterAccess`.
- `ServiceTargetSelector` with `TargetSelectionPolicy`
  (`LocalFirst`/`LowestCost`/`RoundRobin`/`All`) consuming
  `ClusterDirectory` + endpoint costs for generic routing target selection.

## sinomo-daemon: runtime registry

- `crates/sinomo-daemon/src/registry.rs` absorbs `sinomo-runtime::r1::modules`
  (internal module table, native-rust bindings, descriptor conversion,
  snapshot route install) and the `sinomo_data_dir_layout` composition.
  `sinomo-runtime` keeps the public paths alive as pure re-exports
  (`sinomo_runtime::modules`/`sinomo_runtime::storage` -> daemon::registry) —
  `sinomo-agent` (r2) uses `internal_module_registry()` in production
  (agent.rs AgentCatalog) and `sinomo-cli` uses `sinomo_data_dir_layout`
  (install.rs:127).
- `sinomo-network-module` is deleted in stage E: its adapter moves into
  `sinomo-daemon::registry`; the `sinomo_runtime::network_module` re-export is
  removed and `sinomo-agent`'s test at agent.rs:13448 is updated.
- Daemon startup: build module registry (internal + dylib loader states),
  run registration hooks into `ServiceRegistry`, install routes, load network
  modules into `NetworkModuleRegistry`, start drivers owned by their crates
  (cluster maintenance from `sinomo-cluster`, KV fanout from cluster's
  drivers, DFS maintenance from `sinomo-dfs`).
- Authorization becomes descriptor-driven from route catalogs; a hardcoded
  core remains for `/daemon/*` lifecycle routes.

## CLI and app: one daemon API

- `crates/sinomo-daemon/src/api.rs`: typed `DaemonApiClient` (discovery via
  `DaemonPaths` probe + signed object RPC): status/shutdown, cluster control
  (init/join/invite/rename/role/remove/leave), health/peers reads, DFS browser
  ops, rollout ops, config, agent-session passthrough. Exposed via
  `sinomo_runtime::daemon`; both CLI and app use it (app's RemoteConfigClient
  glue folds in).
- Through the daemon (breaking: daemon must be running): `cluster
  init/join/invite/rename/role/remove/leave`, `dfs services`,
  `dfs metadata-publish`, `rollout advance/cluster`, agent sessions, observe.
- Debug/repair (stay direct, documented): `cluster doctor`, `cluster
  connectivity`, `cluster repair-members`, `cluster
  cleanup-legacy-shadow-members`, `cluster state-authority-role`, `cluster
  migrate` planning, `debug *`, plus daemon/install/rollout local lifecycle.
- Purely local commands (models/providers/voice/agent package ops) unchanged.

## Boundary tests (crates/sinomo-core/tests/workspace_ring_boundaries.rs)

Landed in stage A (rules 1-7 with `FACADE_MIGRATION_COMPLETE = false`).
Stage B adds rule 8: `sinomo-network` must not depend on `sinomo-kv` or
`sinomo-cluster`, and `sinomo-kv` must not depend on `sinomo-network`
(`[dependencies]` only; dev-deps exempt). Stage F flips
`FACADE_MIGRATION_COMPLETE = true` after the r1 import sweep (including
modules/sandbox/wasix).

## Implementation stages (workspace builds at the end of each stage)

- **A. Facade foundation** — DONE.
- **B. Network split**: create `sinomo-cluster` per the partition above;
  purify `sinomo-network` (traits, error variant removal, prelude cleanup,
  helper relocation); update consumers (daemon, dfs, rollout, sandbox, config,
  observe, runtime facade `cluster` re-export, cli/app/evolution via
  `sinomo_runtime::cluster`); split tests.rs; boundary rule 8.
- **D. Service expansion**: instance records, discovery, target selection;
  daemon publishes instances.
- **G. CLI/app daemon API**: `DaemonApiClient`, `/cluster/control/*` routes
  registered by `sinomo-cluster`, CLI rewiring, app consolidation.
- **E. Daemon runtime registry**: move runtime behavior into
  `sinomo-daemon::registry` keeping `sinomo_runtime::{modules,storage}` paths;
  delete `sinomo-network-module`; descriptor-driven auth.
- **C. Network modules + iroh dylib** (after G so CLI no longer needs
  in-process iroh; after E so the daemon registry exists): registry + ABI +
  built-ins, `modules/network/iroh`, loader support in `sinomo-module`,
  surfaces in `sinomo-module-api`, enable/disable routes, remove `iroh` dep +
  feature from `sinomo-network`/`sinomo-core`.
- **F. R1 through the facade**: r1 crates (incl. modules/sandbox/wasix) drop
  direct r0 deps for `sinomo_core::...` paths; flip
  `FACADE_MIGRATION_COMPLETE`.
- **H. Docs**: architecture.md, network-architecture.md,
  clustering-architecture.md, module-lifecycle-architecture.md, AGENTS.md,
  README.
- **I. Verification**: workspace build, all tests, clippy, boundary tests,
  adversarial review.
