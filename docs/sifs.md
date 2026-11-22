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

`/sinomo` is reserved for cluster-operational state. Each node's local-owned
files live under `/sinomo/nodes/<node_id>/files`.

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
| `/sinomo/nodes/<node_id>/files` | `local-owned` | Node-owned files. The owner node is the write authority; other nodes route reads and writes to it. |
| `/users` | `replicated-eventual` | Cluster-distributed user data. Writes commit according to distributed DFS policy and repair toward the desired replica set. |
| `/services` | `replicated-eventual` | Cluster-distributed service data suitable for services such as web servers. |

The parent directories `/`, `/sinomo`, `/sinomo/nodes`, and
`/sinomo/nodes/<node_id>` are namespace anchors. They make the cluster layout
inspectable and routable, while the local-owned policy boundary is the `files`
subtree itself. These files are ordinary DFS objects in the owner's content
store, not a mirror of a host directory; use a mount to present SIFS on the
host.

## KV Boundary

SIFS configuration and DFS service discovery are compact cluster control-plane records under the `sinomo-kv` `/dfs` keyspace:

```text
/dfs/filesystems/sifs/config
/dfs/filesystems/sifs/metadata-shards/<shard_id>
/dfs/services/metadata/<node_id>
/dfs/services/content/<node_id>
```

Per-path metadata, object metadata, object versions, write intents, replica
receipts, and content manifests do not live in KV. They are managed by DFS
metadata shards and node-local DFS content stores.

## Access and Compatibility

SIFS is available through the DFS path API and platform filesystem adapters.
The public mount command is:

```sh
sinomo dfs mount <mount-point> [--root <sifs-path>]
```

Unix uses FUSE where supported. Windows uses the Cloud Files integration. Both
adapters preserve the same SIFS namespace and policy model.

SIFS can also be served directly through the loopback NFSv3 adapter:

```sh
sinomo dfs nfs serve --bind 127.0.0.1:111 --export sifs --root /
```

On Windows Pro or Server with Client for NFS installed,
`sinomo dfs nfs mount S:` starts the foreground server and maps the export to a
drive letter without materializing a second host-side file tree. On Linux,
`sinomo dfs nfs mount /mnt/sinomo` provides an explicit kernel-NFS alternative
while `sinomo dfs mount /mnt/sinomo` selects FUSE by default. The NFS Mount
module's daemon-supervised controls and actionable status appear under
**Settings > Storage** beside the mount settings; its Modules entry links
there. See the [SIFS NFSv3 architecture](nfs-architecture.md) for its
durability, confinement, compatibility, and lifecycle contracts.

The [Windows Cloud Files architecture](cloud-files-architecture.md) defines
the provider's placeholder identities, hydration, durable local writeback,
remote-feed reconciliation, registration ownership, and crash recovery. It is
the normative adapter-specific document; this page owns the shared SIFS
namespace and policy contract.

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
- An operation whose observed parent, object, or path changed before it could
  publish fails with a conflict; FUSE reports it as `ESTALE`. Re-resolve the
  path and retry. Normal operation keeps no conflict records.
- A path locked by an in-progress cross-shard move returns a retryable error.
  While a cross-shard directory move runs, creates, writes, renames and
  unlinks under its destination return the same error until the move finishes
  or aborts; reads there are not blocked.
- A FUSE mount or NFS export adds no deadline of its own to a call. A path
  lookup, an attribute read, a content read, an advisory-lock command and a
  mutation each wait for their outcome: the answer, a definite error, the
  network's verdict that the node they need cannot be reached, a change of
  the shard's record, or the mount's close. A metadata-shard primary that is
  slow, unreachable or changing does not make the mount fail the call, and a
  path lookup or namespace mutation waiting for its shard's record holds none
  of the mount's locks: the mount serves other calls meanwhile. The
  only bound in time is the mount's operation budget, when it has one:
  `--operation-budget <duration>` on `sinomo dfs mount`, `sinomo dfs nfs
  serve` and `sinomo dfs nfs mount`, or `operation_budget` in the supervised
  mount's configuration (`storage.sifs-mount`, `storage.sifs-nfs-mount`). The
  duration is a whole number with a unit: `500ms`, `30s`, `5m` or `1h`. The
  budget covers the whole call, its waits for the mount's own locks and for
  the local daemon included, and travels with every request the call sends. A
  call that reaches its budget without a result returns `EIO`
  (`NFS3ERR_IO`): the change was not delivered, or its outcome is unknown and
  it can still commit. A definite refusal returns its own error; a shard whose
  storage is closed for preparation or conversion refuses with `EAGAIN`
  (`NFS3ERR_JUKEBOX`). No such error unmounts or fences the mount, which
  serves the next call.
- A FUSE mount or NFS export keeps serving while the local daemon does not
  answer. Each mutation hands its work to the daemon through a durable
  marker; a step the daemon has not acknowledged is sent again until it is,
  with no attempt limit, and the call waits for that within the mount's
  operation budget. A call whose budget ends first returns `EIO`
  (`NFS3ERR_IO`), and the mount keeps sending the step. A step the daemon
  definitely refuses ends its call with the same error and is not sent again.
  While 64 markers occupy the outbox, the next mutation waits for room. A
  marker's custody follows its lease: once the call releases the lease, after
  the daemon accepted its report or definitely refused it, the daemon's
  recovery owns the marker and can complete it while the mount stays live; the
  mount asks the daemon to look for it at once. Only an unmount or the end of
  the mount process ends a mount.
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
