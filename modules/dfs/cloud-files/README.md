# Windows Cloud Files Module

`sinomo-dfs-cloud-files` is the trusted Windows native-dylib provider that
projects SIFS through CFAPI. It is an R1 leaf module with ID
`module.dfs.cloud-files` and current interface
`sinomo.dfs.cloud-files.v3`. The host-facing architecture and operational
contract live in the [Windows Cloud Files architecture](../../../docs/cloud-files-architecture.md).

The module builds as both an `rlib` for tests and a `cdylib` for rollout. It is
Windows-only in production and runs inside the Sinomo daemon process; the ABI
is a version/rollout boundary, not a sandbox.

## Source Map

| Path | Owner |
|---|---|
| [`src/lib.rs`](src/lib.rs) | Native-module descriptor, ABI dispatch, typed operation routing, and placeholder identity encoding. |
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
The main-daemon supervisor and host-side recovery authority live in
[`dfs_mount_supervisor.rs`](../../../crates/sinomo-cli/src/dfs_mount_supervisor.rs).

## Declared Settings

The descriptor exports the shared editable `storage.sifs-mount` definition and
read-only `storage.sifs-mount-status` definition through
`NativeModuleDescriptor.surfaces.settings`. Both definitions identify this
module, include their full recursive `ConfigSchema`, and are SOBJ encoded at
the ABI boundary. The host validates that a native module cannot declare a
setting for another module.

The definitions deliberately reuse the existing DFS mount keys and mount
supervisor. The app therefore presents the host location, SIFS root,
enablement, and synchronization fields in its generic module settings editor
without introducing Cloud-Files-specific persistence or mutation code. On
Windows the host fallback catalog contains the identical definitions so the
editor remains available during module discovery; exact duplicates are
idempotent, while conflicting dynamic definitions cannot replace the static
catalog. The current supervisor accepts one mount mapping per node.

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
- Writable state opens only after exact registration ownership is durable.
- Registration, cursor recovery, and preserve/re-enable accept only finite,
  causal current/temporary cuts. Do not add generic repair or “latest wins”
  behavior to these classifiers.
- `AuthorityFinalized` is the irreversible completion cut. Disable before it may
  preserve into the next recovery cycle; disable after it must complete forward
  and cancel before ordinary writeback is released.
- A durable staged provider owns provider selection. Loader drift is not
  permission to replace it, and missing/tampered pins fail without fallback.
- Settled idle workers block on explicit notifications or an outstanding retry
  deadline. Do not add polling, periodic heartbeats, or idle status writes.
- File Explorer custom status is push-only and ancillary. Explorer must not
  load Sinomo code or query DFS/queue state, and a shell-property failure must
  not affect mount authority or create a retry loop.

## Editing Guidance

The host and native recovery machines form one protocol. A new phase, field,
or durable file normally requires all of the following in the same change:

1. a canonical bounded encoding and validation rule;
2. an exact predecessor/successor transition;
3. deterministic temporary-file classification for every fsync cut;
4. pre-effect validation under the registration lease, physical root fence, or
   host operation fence that owns the transition;
5. typed API evidence when authority crosses the module ABI;
6. restart, response-loss, reordered-cut, tamper, disable, and cancellation
   tests; and
7. updates to the architecture document when the external contract changes.

Never hold the shared host operation fence while stopping or joining the native
provider thread. The fence serializes the short decision/commit section; it is
released before cancellation and join so callbacks cannot deadlock.

Do not weaken ordinary context parity. A cursor-bound request requires the
matching native marker and proof. An ordinary request requires cursor authority
and cursor-provenance artifacts to be absent.

## Validation

Fast platform-neutral and API checks:

```sh
cargo test -p sinomo-dfs-cloud-files
cargo test -p sinomo-dfs-cloud-files
```

Compile production and test surfaces for Windows:

```sh
cargo check -p sinomo-dfs-cloud-files --target x86_64-pc-windows-gnu
cargo test -p sinomo-dfs-cloud-files \
  -p sinomo-dfs-cloud-files -p sinomo-cli \
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
