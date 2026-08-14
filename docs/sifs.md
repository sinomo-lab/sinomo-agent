# SIFS

SIFS is the default filesystem exposed by a Sinomo cluster. Users and services
share one namespace while each subtree retains an explicit ownership and
replication policy. Node-local files, shared user workspaces, service state, and
artifacts therefore remain distinguishable even when they are accessed through
the same filesystem.

SIFS is a named DFS configuration, not a separate storage engine. The
[DFS architecture](dfs-architecture.md) defines its metadata, content,
consistency, and repair behavior.

## Default Layout

The baseline SIFS layout is:

```text
/
/sinomo
/sinomo/nodes
/sinomo/nodes/<node_id>
/sinomo/nodes/<node_id>/files
/users
/services
```

`/sinomo` is reserved for cluster-operational state. Node-local filesystem exposure lives under `/sinomo/nodes/<node_id>/files`.

`/users` is reserved for cluster-level user data and user-facing shared workspaces.

`/services` is reserved for service-owned state. For example, a web service can publish and update files under `/services/web/...`.
Sinomo-managed agent skills live under `/services/sinomo/skills`: skill bodies
and bundled Agent Skills resources retain their portable directory layout,
while catalog indexes, active pointers, and conflict records are stored as SOBJ
metadata.
Cluster-wide custom and imported agent definitions live under
`/services/sinomo/agents` as JSON package artifacts. Nodes consume those
definitions as execution endpoints rather than owning local agent config files.

## Mixed Architecture

SIFS is a mixed-architecture filesystem. Different subtrees can use different DFS policies while still sharing one root namespace.

Current predefined policy model:

| Path | Policy | Meaning |
|---|---|---|
| `/sinomo/nodes/<node_id>/files` | `local-owned` | The node exposes its local `$SINOMO_DATA_DIR/files` directory into SIFS. The owner node is the write authority. |
| `/users` | `replicated-eventual` | Cluster-distributed user data. Writes commit according to distributed DFS policy and repair toward the desired replica set. |
| `/services` | `replicated-eventual` | Cluster-distributed service data suitable for services such as web servers. |

The parent directories `/`, `/sinomo`, `/sinomo/nodes`, and `/sinomo/nodes/<node_id>` are namespace anchors. They make the cluster layout inspectable and routable, while the policy boundary for local host-file exposure is the `files` subtree itself.

The default local exposure config lives at `$SINOMO_DATA_DIR/configs/dfs/expose.json` as a shared `sinomo-config` document. Existing `$SINOMO_DATA_DIR/config/dfs-expose.sobj` and `dfs-expose.json` files are read for migration, and explicit `.sobj` or `.json` paths remain supported for compatibility.

## KV Boundary

SIFS configuration and DFS service discovery are compact cluster control-plane records under the `sinomo-kv` `/dfs` keyspace:

```text
/dfs/filesystems/sifs/config
/dfs/filesystems/sifs/metadata-shards/<shard_id>
/dfs/services/metadata/<node_id>
/dfs/services/content/<node_id>
```

Per-path metadata, object metadata, object versions, write intents, replica receipts, conflicts, and content manifests do not live in KV. They are managed by DFS metadata shards and node-local DFS content stores.

## Access and Compatibility

SIFS is available through the DFS path API and platform filesystem adapters.
The public mount command is:

```sh
sinomo dfs mount <mount-point> [--root <sifs-path>]
```

Unix uses FUSE where supported. Windows uses the Cloud Files integration. Both
adapters preserve the same SIFS namespace and policy model.

Mounted SIFS adapters are reactive at rest. FUSE blocks in the kernel until a
filesystem request or shutdown arrives. The Windows Cloud Files projection
blocks on NTFS directory-change notifications, the daemon's DFS namespace
event stream, and explicit configuration, lifecycle, or shutdown signals. It
does not run a periodic scan, heartbeat, cancellation poll, or mount-config
poll. With no local access, no committed remote mutation beneath the mounted
root, and no retry or recovery caused by an earlier operation, the mount owns
no scheduled disk I/O. A local or remote namespace change wakes one bounded
reconciliation pass; a stable-read retry wakes only at the deadline created by
the write that required it.

Canonical DFS content writes and namespace mutations route to the responsible
metadata-shard primary. Once an adapter publishes an operation through that
path, a successful response carries enough metadata progress for the caller to
read its own write. A platform adapter can have an earlier, process-local
acknowledgement boundary; the current FUSE write buffer is described below.
Large recursive clears, directory renames, and cross-shard changes run as
bounded durable jobs so they can resume after restart.

SIFS is POSIX-like, with deliberate distributed-system differences:

- `.` and `..` are resolved lexically before symlink resolution for direct API
  callers.
- Read-triggered access-time updates may be skipped on replicas to preserve
  journal authority.
- Reading an object with an open recovery conflict fails with `ESTALE` until
  the conflict is resolved.
- A path locked by an in-progress cross-shard move returns a retryable error.
- Unix FUSE mounts support distributed whole-file shared and exclusive POSIX
  advisory locks. Partial byte ranges are rejected with `EOPNOTSUPP`; a lock
  that loses its authority epoch or expires poisons its associated handles and
  is reported as an I/O failure. A mount supports at most two distinct live or
  pending lock keys; further acquisitions fail with `ENOLCK`. Blocking lock
  waits are bounded by durable cancellation and leases, although the current
  `fuser` dependency does not expose kernel `FUSE_INTERRUPT` requests, so a
  signal cannot yet cancel an individual `F_SETLKW` wait immediately.
- Ordinary FUSE writes may return after copying bytes into a process-local,
  per-handle dirty buffer, before those bytes are published to canonical DFS.
  The current implementation bounds that buffer at 64 MiB per handle; pressure
  publishes existing ranges, while an individual write larger than the bound
  publishes directly. `flush` and close publish that handle's pending ranges
  and report publication failures, but they are not durability barriers.
  `O_DSYNC` and `O_SYNC` first publish earlier buffered writes for the object,
  publish the current write, and then invoke the file-data or full-file barrier
  respectively. Explicit `fdatasync` and `fsync` first publish every dirty
  handle for the object and then require the current metadata authority and a
  recovery-capable quorum certificate. Directory `fsync` applies the
  corresponding namespace barrier. These operations fail rather than
  weakening durability when quorum is unavailable. A mount-process crash can
  lose ordinary writes that returned while they existed only in the dirty
  buffer.
- Timestamps originate on individual nodes, so cross-node ordering is subject
  to clock skew.

Normal quorum-backed operation does not create divergent visible versions. Any
future split-cluster recovery mode must define explicit conflict and
reconciliation behavior before it can be enabled.
