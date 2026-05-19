# SIFS

SIFS, the Sinomo File System, is the root filesystem for a Sinomo cluster. Many cluster operations depend on paths in SIFS for node-local files, shared user state, service state, artifacts, and future operational workspaces.

From `sinomo-dfs`'s perspective, SIFS is intentionally not a special storage engine. It is the default filesystem that the cluster exposes. The DFS layer stores and routes SIFS metadata, content, receipts, repair state, and conflicts using the same object, metadata-shard, content-store, and policy mechanisms used by any other DFS filesystem.

What makes SIFS special is its role and its predefined layout.

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

## Mixed Architecture

SIFS is a mixed-architecture filesystem. Different subtrees can use different DFS policies while still sharing one root namespace.

Current predefined policy model:

| Path | Policy | Meaning |
|---|---|---|
| `/sinomo/nodes/<node_id>/files` | `local-owned` | The node exposes its local `$SINOMO_DATA_DIR/files` directory into SIFS. The owner node is the write authority. |
| `/users` | `replicated-eventual` | Cluster-distributed user data. Writes commit according to distributed DFS policy and repair toward the desired replica set. |
| `/services` | `replicated-eventual` | Cluster-distributed service data suitable for services such as web servers. |

The parent directories `/`, `/sinomo`, `/sinomo/nodes`, and `/sinomo/nodes/<node_id>` are namespace anchors. They make the cluster layout inspectable and routable, while the policy boundary for local host-file exposure is the `files` subtree itself.

## KV Boundary

SIFS configuration and DFS service discovery are compact cluster control-plane records under the `sinomo-kv` `/dfs` keyspace:

```text
/dfs/filesystems/sifs/config
/dfs/filesystems/sifs/metadata-shards/<shard_id>
/dfs/services/metadata/<node_id>
/dfs/services/content/<node_id>
```

Per-path metadata, object metadata, object versions, write intents, replica receipts, conflicts, and content manifests do not live in KV. They are managed by DFS metadata shards and node-local DFS content stores.

## Implementation Notes

`crates/sinomo-dfs/src/namespace.rs` defines the SIFS bootstrap layout and subtree policies.

`crates/sinomo-dfs/src/local_owned.rs` defines the local-owned node file exposure at `/sinomo/nodes/<node_id>/files`.

`crates/sinomo-dfs/src/posix.rs` exposes SIFS through a POSIX-like path API for mkdir, list, stat, read, write, rename, and unlink operations.
