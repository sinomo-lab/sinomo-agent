# sinomo-dfs

`sinomo-dfs` is Sinomo's local-first distributed filesystem layer. It provides the object, metadata, content, replication, repair, and POSIX-like path APIs used to expose SIFS, the cluster root filesystem.

The crate is not a replacement for the host filesystem, Git, or the cluster KV. It is a policy-aware workspace layer: file bytes stay in node-local content stores, high-cardinality filesystem metadata stays in DFS metadata backends, and the cluster KV is used only for compact DFS control-plane configuration.

See also:

- `docs/dfs-architecture.md` for the full distributed design.
- `docs/sifs.md` for the SIFS cluster root layout.
- `docs/storage-architecture.md` for data-directory ownership.

## Architecture

The DFS has four planes.

### Namespace and Object Metadata

DFS metadata records the stable identity and visible state of filesystem objects:

- filesystem ID,
- object ID,
- path,
- object kind,
- owner node,
- current version,
- current content hash,
- current replica locations,
- policy,
- conflicts,
- write intents and receipts.

Object identity is stable across path moves. Path operations update namespace metadata, not content identity.

The default filesystem is SIFS. From this crate's perspective SIFS is ordinary DFS metadata with a predefined layout:

```text
/
/sinomo
/sinomo/nodes
/sinomo/nodes/<node_id>
/sinomo/nodes/<node_id>/files
/users
/services
```

`/sinomo/nodes/<node_id>/files` is local-owned. `/users` and `/services` are distributed DFS subtrees.

### Content Store

`LocalContentStore` stores bytes under the local data directory as immutable content-addressed blobs plus object-version manifests.

Object versions are protected by hash. Rewriting the same `(object_id, version)` with different bytes is rejected. Storage writes use unique temporary files, atomic rename, and fsync of the file and parent directory.

### Metadata Backend

`DfsMetadataBackend` is the durable metadata interface. The current `LocalJsonMetadataBackend` stores shard-local records as Sinomo object files and provides:

- filesystem records,
- object and path records,
- version records,
- write-intent records,
- replica receipts,
- conflict records,
- namespace transactions,
- recoverable visible-version commits.

Visible metadata commits and namespace transactions are written as replayable records before materialization, so reopening the backend can complete an interrupted operation.

### Networked DFS Service

`NetworkedDfsService<T>` is the distributed object service over a `sinomo-network` transport. It combines:

- local metadata cache,
- local content store,
- optional control-plane KV client,
- optional durable metadata backend,
- distributed write coordinator,
- data-plane DFS network messages.

Distributed writes create a persisted intent, stage content to desired replicas, record durable receipts, commit the visible version after the policy threshold, and keep repair jobs until the desired replica set converges.

Replica-side commands are authorized against object ownership, metadata-shard primary state when available, desired replica membership, persisted intent records, and content hashes. Rejected replica commands do not create replica metadata.

Repair is restart-aware. On service hydration, persisted intents and receipts rebuild coordinator state and missing-replica jobs. Repair retries have backoff-like scheduling, and a recovered coordinator that no longer has local blob bytes can fetch the blob from another durable replica before repairing a missing node.

## Policies

The main object policies currently used by the crate are:

- `LocalOwned`: one owner node is the write authority. Other nodes route reads and writes to the owner.
- `ReplicatedEventual`: a write becomes visible once `min_commit_replicas` durable receipts exist, then repair converges toward `desired_replicas`.
- `ReplicatedConfirmed`: a stricter replicated policy where all desired replicas must confirm before commit.

The policy is attached to objects and inherited by higher-level path APIs. DFS does not hardcode behavior by path except for the SIFS bootstrap defaults.

## Storage Layout

The crate registers these data-directory areas through `register_storage_paths`:

```text
dfs/content/blobs
dfs/content/objects
dfs/content/staging
dfs/metadata/records
dfs/metadata/journal
dfs/metadata/root
dfs/operator
dfs/pins
files
```

The exact storage format is intentionally hidden behind crate APIs and metadata backend traits. Other crates should use the DFS interfaces instead of reading these files directly.

## Usage Examples

### Store a Local Object Version

```rust
use sinomo_core::{DfsFileSystemId, DfsObjectId, DfsObjectPolicy, DfsVersion, NodeId};
use sinomo_dfs::{DfsFileSystem, DfsObjectKind, DfsObjectMetadata, LocalContentStore, NetworkedDfsService};
use sinomo_network::InProcessNetwork;

fn main() -> Result<(), Box<dyn std::error::Error>> {
let node_id = NodeId::from_legacy_named("node_owner")?;
let file_system_id = DfsFileSystemId::from_legacy_named("sifs")?;
let object_id = DfsObjectId::from_legacy_named("obj_readme_example")?;
let version = DfsVersion::from_legacy_named("version_1")?;

let network = InProcessNetwork::new();
let content = LocalContentStore::new("/tmp/sinomo-dfs-example-owner")?;
let mut dfs = NetworkedDfsService::new(network.add_node(node_id.clone()), content);

dfs.add_file_system(DfsFileSystem {
    file_system_id: file_system_id.clone(),
    display_name: "SIFS".to_string(),
    owner_node_id: node_id.clone(),
    subtree_policies: Vec::new(),
})?;

let object = DfsObjectMetadata {
    object_id: object_id.clone(),
    file_system_id,
    path: "/users/example.txt".to_string(),
    kind: DfsObjectKind::File,
    owner_node_id: node_id,
    current_locations: Vec::new(),
    content_hash: None,
    link_target_object_id: None,
    version,
    policy: DfsObjectPolicy::LocalOwned,
    created_by_event_id: None,
    last_modified_by_event_id: None,
};

let content_hash = dfs.put_local_object_version(object, b"hello dfs")?;
let read = dfs.read_local_object(&object_id)?.expect("object bytes");
assert_eq!(read.bytes, b"hello dfs");
assert_eq!(read.content_hash.as_deref(), Some(content_hash.as_str()));
Ok(())
}
```

### Replicate an Eventual Object

```rust
use sinomo_core::{DfsFileSystemId, DfsObjectId, DfsObjectPolicy, DfsVersion, DfsWriteRequest, NodeId};
use sinomo_dfs::{
    DfsFileSystem, DfsObjectKind, DfsObjectMetadata, DfsReplicationPolicy, LocalContentStore,
    NetworkedDfsService, NetworkedDfsWriteOutcome,
};
use sinomo_network::InProcessNetwork;

fn main() -> Result<(), Box<dyn std::error::Error>> {
let owner = NodeId::from_legacy_named("node_owner")?;
let replica = NodeId::from_legacy_named("node_replica")?;
let file_system_id = DfsFileSystemId::from_legacy_named("sifs")?;
let object_id = DfsObjectId::from_legacy_named("obj_replicated")?;
let version_1 = DfsVersion::from_legacy_named("version_1")?;
let version_2 = DfsVersion::from_legacy_named("version_2")?;

let network = InProcessNetwork::new();
let mut owner_dfs = NetworkedDfsService::new(
    network.add_node(owner.clone()),
    LocalContentStore::new("/tmp/sinomo-dfs-owner")?,
);
let mut replica_dfs = NetworkedDfsService::new(
    network.add_node(replica.clone()),
    LocalContentStore::new("/tmp/sinomo-dfs-replica")?,
);

let file_system = DfsFileSystem {
    file_system_id: file_system_id.clone(),
    display_name: "SIFS".to_string(),
    owner_node_id: owner.clone(),
    subtree_policies: Vec::new(),
};
let object = DfsObjectMetadata {
    object_id: object_id.clone(),
    file_system_id: file_system_id.clone(),
    path: "/services/artifact.bin".to_string(),
    kind: DfsObjectKind::Artifact,
    owner_node_id: owner.clone(),
    current_locations: vec![owner.clone()],
    content_hash: None,
    link_target_object_id: None,
    version: version_1,
    policy: DfsObjectPolicy::ReplicatedEventual,
    created_by_event_id: None,
    last_modified_by_event_id: None,
};

owner_dfs.add_file_system(file_system.clone())?;
replica_dfs.add_file_system(file_system)?;
owner_dfs.add_object_metadata(object.clone())?;
replica_dfs.add_object_metadata(object)?;

let outcome = owner_dfs.write_distributed_eventual(
    DfsWriteRequest {
        file_system_id,
        object_id: object_id.clone(),
        target_version: version_2,
        requesting_node_id: owner,
    },
    b"replicated bytes".to_vec(),
    vec![owner_dfs.node_id().clone(), replica.clone()],
    DfsReplicationPolicy::new(2, 2)?,
)?;

assert!(matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }));
replica_dfs.process_incoming()?;
owner_dfs.process_incoming()?;
replica_dfs.process_incoming()?;

let read = replica_dfs.read_local_object(&object_id)?.expect("replica data");
assert_eq!(read.bytes, b"replicated bytes");
Ok(())
}
```

### Use the SIFS POSIX Facade

```rust
use sinomo_core::NodeId;
use sinomo_dfs::{DfsWriteOptions, LocalContentStore, NetworkedDfsService};
use sinomo_network::InProcessNetwork;

fn main() -> Result<(), Box<dyn std::error::Error>> {
let node = NodeId::from_legacy_named("node_posix")?;
let network = InProcessNetwork::new();
let mut dfs = NetworkedDfsService::new(
    network.add_node(node.clone()),
    LocalContentStore::new("/tmp/sinomo-dfs-posix")?,
);

dfs.bootstrap_distributed_sifs()?;
dfs.posix_mkdir_all("/users/example")?;
dfs.posix_write_file(
    "/users/example/hello.txt",
    b"hello from SIFS",
    DfsWriteOptions::replicated_eventual(vec![node]),
)?;

let entries = dfs.posix_list_dir("/users/example")?;
assert_eq!(entries[0].name, "hello.txt");
let bytes = dfs.posix_read_file("/users/example/hello.txt")?;
assert_eq!(bytes, b"hello from SIFS");
Ok(())
}
```

## Operational Notes

- Do not put large file bytes in `sinomo-kv`; DFS content stores carry bytes.
- Treat metadata backend records as the source of restart recovery.
- Use SIFS path APIs for filesystem-like workflows and object/version APIs for replication, repair, audit, and operator tooling.
- Prefer explicit conflict records over silent overwrite when base versions do not match.
- Local-owned subtrees remain useful for sensitive or node-specific files even inside the cluster root filesystem.
