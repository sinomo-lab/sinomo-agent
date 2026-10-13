# Windows Cloud Files Module

`sinomo-dfs-cloud-files` is the trusted Windows native-dylib provider that
projects SIFS through CFAPI. It is an R1 leaf module with ID
`module.dfs.cloud-files` and current interface
`sinomo.dfs.cloud-files.v3`. It offers the provider-neutral
`sinomo.host-filesystem-provider/v1` interface to the host. The host-facing
architecture and operational contract live in the
[Windows Cloud Files architecture](../../../docs/cloud-files-architecture.md).

The module builds as both an `rlib` for tests and a `cdylib` for rollout. It is
Windows-only in production and runs inside the Sinomo daemon process; the ABI
is a version/rollout boundary, not a sandbox.

Hot unload uses the shared native-module lifecycle fence. The daemon withdraws
routes, signals the invocation cancellation handle, and waits for the writable
worker and CFAPI operation to return before calling `sinomo_module_shutdown`.
Shutdown refuses to clear host callbacks while an ABI invocation remains
active. If cancellation, disconnect, or shutdown cannot complete within the
drain deadline, the mapping is quarantined and a process restart is required;
durable registration and recovery evidence are never deleted as an unload
shortcut.

## Source Map

| Path | Owner |
|---|---|
| [`src/lib.rs`](src/lib.rs) | Native-module descriptor, ABI dispatch, typed operation routing, and placeholder identity encoding. |
| [`src/endpoint_transport.rs`](src/endpoint_transport.rs) | Provider DFS message transport; delegates clustered namespace and write requests to the resident daemon under bounded per-operation deadlines. |
| [`src/windows_impl.rs`](src/windows_impl.rs) | CFAPI registration, connection, callbacks, hydration, native registration ownership, and cursor-recovery state machine. |
| [`src/windows_adapter.rs`](src/windows_adapter.rs) | `ReadDirectoryChangesW`, stable local capture, concrete DFS namespace source, placeholder effects, and reactive worker wakeup. |
| [`src/windows_sync.rs`](src/windows_sync.rs) | Platform-neutral writable worker, bounded scan/feed orchestration, and adapter contracts. |
| [`src/windows_shell.rs`](src/windows_shell.rs) | Equality-cached projection of durable writeback/conflict state into Windows-owned File Explorer properties. |
| [`src/local_change_queue.rs`](src/local_change_queue.rs) | Durable local mutation queue, immutable staged blobs, retry evidence, and conflict retention. |
| [`src/sync_executor.rs`](src/sync_executor.rs) | Apply/reconcile ordering for local mutations and idempotent local completion. |
| [`src/namespace_reconciler.rs`](src/namespace_reconciler.rs) | Durable remote-feed intents, manifests, no-echo state, cursors, and cursor-recovery plans. |
| [`src/registration_journal.rs`](src/registration_journal.rs) | CFAPI registration phases, receipt validation, recovery classification, and retain/release policy. |
| [`Cargo.toml`](Cargo.toml) | R1 trust-ring and native-dylib identity, interface, source, and Windows platform declaration. |

The typed ABI is owned by
[`protocol.rs`](src/protocol.rs), which is private to this external module.
The provider-neutral main-daemon supervisor (mount consent, process lease, and
status) lives in
[`dfs_mount_supervisor.rs`](../../../crates/sinomo-cli/src/dfs_mount_supervisor.rs);
it owns no Cloud Files registration or recovery state.

## Declared Settings

The descriptor exports the shared editable `storage.sifs-mount` definition and
read-only `storage.sifs-mount-status` definition through
`NativeModuleDescriptor.surfaces.settings`. Both definitions identify this
module, include their full recursive `ConfigSchema`, and are SOBJ encoded at
the ABI boundary. The host validates that a native module cannot declare a
setting for another module.

The definitions deliberately reuse the existing DFS mount keys and mount
supervisor. The app therefore presents the sync root, SIFS root, enablement,
and synchronization fields in the **Mount settings** card of
**Settings > Storage**, without Cloud-Files-specific persistence or mutation
code; the Modules page links there. On Windows the DFS module does not
register these keys, so the active external component is their sole
module-registry owner; a second owner of the same settings key is rejected.
Unix hosts register them from the DFS module for FUSE. The current supervisor
accepts one mount mapping per node.

## Load-Bearing Invariants

- DFS is the only authoritative remote namespace and content history. The
  manifest and local queue are replay indexes and evidence, not alternate
  authorities.
- A placeholder names an exact mount, root, path, object, and version. Hydration
  never silently substitutes path-latest content.
- Local content is copied to a hash-verified immutable blob before a queued DFS
  mutation can be sent.
- `InFlight` is durable before send, ambiguous outcomes reconcile before retry,
  `RemoteApplied` is durable before local completion, and `Completed` follows
  that idempotent effect.
- Remote namespace intent is durable before the NTFS effect; the cursor advances
  only after the effect and manifest/no-echo state are durable.
- Historical test-conflict cleanup is a versioned one-shot migration. It may
  deduplicate terminal evidence and abandon an old test delete only before an
  authoritative rescan; it never replays that delete, consumes user paths, or
  applies to conflicts created after the migration fence is durable.
  The v2 fence includes the older `/services/real-cluster/` verification
  subtree without treating adjacent service or sandbox namespaces as tests.
- Startup absence is never deletion evidence; only a live directory-change
  removal owns that causal action. The v4 fence retires older deterministic
  startup-delete conflicts across all bounded authoritative scan pages. Later
  notification deletes are outside the migration, and ordinary scan deltas
  clear a terminal delete only after exact local/authoritative convergence or
  an explicit authoritative removal.
- Writable state opens only after exact registration ownership is durable.
- Registration, cursor recovery, and preserve/re-enable accept only finite,
  causal current/temporary cuts. Do not add generic repair or “latest wins”
  behavior to these classifiers.
- `CompletionAuthorized` is the irreversible completion cut. Disable before it
  may preserve into the next recovery cycle; disable after it must complete
  forward and cancel before ordinary writeback is released.
- The host selects the single active `host-filesystem-provider` component and
  keeps no provider pin. A successor adopts existing state only after the
  native journal and receipt match the observed sync root; mismatches fail
  closed and never recreate registration authority.
- Routine provider retirement disconnects callbacks and retains the persistent
  sync-root registration. Do not use state-preserving unregister as a daemon
  restart or retry primitive: Windows strips hydrated placeholder identities.
- Failed activation unregisters only a registration created by that attempt;
  pre-existing registration and durable state ownership remain for retry.
- Settled idle workers block on explicit notifications or an outstanding retry
  deadline. Do not add polling, periodic heartbeats, or idle status writes.
- File Explorer custom status is push-only and ancillary. Explorer must not
  load Sinomo code or query DFS/queue state, and a shell-property failure must
  not affect mount authority or create a retry loop.

## Editing Guidance

The native registration, preserve, and recovery machines form one protocol. A
new phase, field, or durable file normally requires all of the following in the
same change:

1. a canonical bounded encoding and validation rule;
2. an exact predecessor/successor transition;
3. deterministic temporary-file classification for every fsync cut;
4. pre-effect validation under the registration lease or physical root fence
   that owns the transition;
5. typed API evidence when authority crosses the module ABI;
6. restart, response-loss, reordered-cut, tamper, disable, and cancellation
   tests; and
7. updates to the architecture document when the external contract changes.

The supervisor signals cancellation and joins the native provider thread
without holding locks that provider callbacks or status publication need. Keep
cancellation and join outside any such lock so callbacks cannot deadlock.

Do not weaken ordinary context parity. A cursor-bound request requires the
matching native marker and proof. An ordinary request requires cursor authority
and cursor-provenance artifacts to be absent.

## Validation

Fast platform-neutral and API checks:

```sh
cargo test -p sinomo-dfs-cloud-files
```

Compile production and test surfaces for Windows:

```sh
cargo check -p sinomo-dfs-cloud-files --target x86_64-pc-windows-gnu
cargo test -p sinomo-dfs-cloud-files -p sinomo-cli \
  --target x86_64-pc-windows-gnu --no-run
```

Run formatting and workspace-boundary checks when Rust or manifests change:

```sh
cargo fmt --all -- --check
cargo test -p sinomo-core --test workspace_ring_boundaries
git diff --check
```

Cross-compilation does not run CFAPI. Changes to Windows callbacks, Explorer
interaction, registration, notifications, focus behavior, idle I/O, or power
usage require focused execution on a Windows host. Capture the exact binary,
config, `SINOMO_DATA_DIR`, `sinomo dfs mount --status --json`, and observations
before and after a disable/re-enable and process restart.
