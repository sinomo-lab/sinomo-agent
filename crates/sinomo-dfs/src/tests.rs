use super::*;
use sinomo_kv::InProcessRaftKvCluster;
use sinomo_network::InProcessNetwork;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
struct ReceiveOnlyTransport {
    local_node_id: NodeId,
    incoming: VecDeque<sinomo_network::InboundFrame>,
}

impl ReceiveOnlyTransport {
    fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            incoming: VecDeque::new(),
        }
    }

    fn push_message(
        &mut self,
        source_node_id: NodeId,
        target_node_id: NodeId,
        message: DfsNetworkMessage,
    ) {
        let protocol_message =
            ProtocolMessage::encode(DFS_PROTOCOL, &message).expect("protocol message");
        self.incoming.push_back(sinomo_network::InboundFrame {
            received_by: target_node_id.clone(),
            frame: NetworkFrame {
                frame_id: NetworkFrameId(1),
                source_node_id: source_node_id.clone(),
                target_node_id: target_node_id.clone(),
                message: MeshMessage {
                    message_id: MeshMessageId(1),
                    origin_node_id: source_node_id,
                    target_node_id,
                    body: MeshMessageBody::Protocol(protocol_message),
                },
            },
        });
    }
}

impl NetworkTransport for ReceiveOnlyTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    fn send(&mut self, target_node_id: &NodeId, _frame: NetworkFrame) -> Result<(), NetworkError> {
        Err(NetworkError::UnknownNode {
            node_id: target_node_id.clone(),
        })
    }

    fn drain_incoming(&mut self) -> Result<Vec<sinomo_network::InboundFrame>, NetworkError> {
        Ok(self.incoming.drain(..).collect())
    }
}

struct FailingSendTransport {
    local_node_id: NodeId,
}

impl NetworkTransport for FailingSendTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    fn send(&mut self, _target_node_id: &NodeId, _frame: NetworkFrame) -> Result<(), NetworkError> {
        Err(NetworkError::Io {
            operation: "test send".to_string(),
            detail: "unreachable".to_string(),
        })
    }

    fn drain_incoming(&mut self) -> Result<Vec<sinomo_network::InboundFrame>, NetworkError> {
        Ok(Vec::new())
    }
}

#[derive(Clone)]
struct CountingEmptyTransport {
    local_node_id: NodeId,
    drain_calls: Arc<Mutex<usize>>,
}

impl CountingEmptyTransport {
    fn new(local_node_id: NodeId, drain_calls: Arc<Mutex<usize>>) -> Self {
        Self {
            local_node_id,
            drain_calls,
        }
    }
}

impl NetworkTransport for CountingEmptyTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    fn send(&mut self, target_node_id: &NodeId, _frame: NetworkFrame) -> Result<(), NetworkError> {
        Err(NetworkError::UnknownNode {
            node_id: target_node_id.clone(),
        })
    }

    fn drain_incoming(&mut self) -> Result<Vec<sinomo_network::InboundFrame>, NetworkError> {
        *self.drain_calls.lock().expect("drain calls") += 1;
        Ok(Vec::new())
    }

    fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        Ok(Some(0))
    }
}

fn file_system(owner: NodeId) -> DfsFileSystem {
    DfsFileSystem {
        file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
        display_name: "repo".to_string(),
        owner_node_id: owner,
        subtree_policies: Vec::new(),
    }
}

fn file_metadata(owner: NodeId) -> DfsObjectMetadata {
    DfsObjectMetadata {
        object_id: DfsObjectId::from_legacy_named("obj_file").expect("object"),
        file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
        path: "src/lib.rs".to_string(),
        kind: DfsObjectKind::File,
        owner_node_id: owner.clone(),
        current_locations: vec![owner],
        content_hash: None,
        content_size_bytes: None,
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: DfsVersion::from_legacy_named("version_1").expect("version"),
        policy: DfsObjectPolicy::LocalOwned,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    }
}

#[test]
fn local_owned_dfs_process_incoming_skips_drain_when_transport_reports_empty() {
    let node = NodeId::from_legacy_named("node_local_owned_empty_hint").expect("node");
    let drain_calls = Arc::new(Mutex::new(0));
    let transport = CountingEmptyTransport::new(node, drain_calls.clone());
    let mut dfs = LocalOwnedDfsService::new(transport);

    assert!(
        dfs.process_incoming()
            .expect("process empty incoming")
            .is_empty()
    );
    assert_eq!(
        *drain_calls.lock().expect("drain calls"),
        0,
        "exact empty inbound hints should avoid LocalOwned DFS mailbox drains"
    );
}

#[test]
fn networked_dfs_process_incoming_skips_drain_when_transport_reports_empty() {
    let root = std::env::temp_dir().join(format!(
        "sinomo-dfs-empty-inbound-hint-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let node = NodeId::from_legacy_named("node_networked_empty_hint").expect("node");
    let drain_calls = Arc::new(Mutex::new(0));
    let transport = CountingEmptyTransport::new(node, drain_calls.clone());
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(transport, content);

    assert!(
        dfs.process_incoming()
            .expect("process empty incoming")
            .is_empty()
    );
    assert_eq!(
        *drain_calls.lock().expect("drain calls"),
        0,
        "exact empty inbound hints should avoid Networked DFS mailbox drains"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_store_path_index_matches_scan_semantics() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let mut store = DfsMetadataStore::new();
    store.add_file_system(file_system(owner.clone()));

    let mut file = file_metadata(owner.clone());
    file.object_id = DfsObjectId::from_legacy_named("obj_live").expect("object");
    file.kind = DfsObjectKind::File;
    file.path = "/users/example/./hello.txt".to_string();
    store.add_object(file.clone()).expect("add file");

    // Lookup uses the normalized key (".." and "." resolved), and a path that
    // does not normalize to the stored key does not match.
    let found = store
        .object_by_normalized_posix_path(&file_system_id, "/users/example/hello.txt")
        .expect("indexed object");
    assert_eq!(found.object_id, file.object_id);
    assert!(
        store
            .object_by_normalized_posix_path(&file_system_id, "/users/example/missing.txt")
            .is_none()
    );

    // Updating the same object id to a new path moves the index entry.
    let mut moved = file.clone();
    moved.path = "/users/example/renamed.txt".to_string();
    store.add_object(moved.clone()).expect("move file");
    assert!(
        store
            .object_by_normalized_posix_path(&file_system_id, "/users/example/hello.txt")
            .is_none(),
        "old path must no longer resolve after a path move"
    );
    assert_eq!(
        store
            .object_by_normalized_posix_path(&file_system_id, "/users/example/renamed.txt")
            .expect("moved object")
            .object_id,
        file.object_id
    );

    // A tombstone object sharing the path is skipped in favor of a live object,
    // regardless of object-id ordering.
    let mut tombstone = file_metadata(owner.clone());
    tombstone.object_id = DfsObjectId::from_legacy_named("obj_aaaa_tombstone").expect("object");
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.path = "/users/example/renamed.txt".to_string();
    store.add_object(tombstone.clone()).expect("add tombstone");
    assert_eq!(
        store
            .object_by_normalized_posix_path(&file_system_id, "/users/example/renamed.txt")
            .expect("live object preferred over tombstone")
            .object_id,
        file.object_id
    );

    // Removing the live object leaves only the tombstone, so the path no longer
    // resolves to a visible object.
    store.remove_object(&file.object_id);
    assert!(
        store
            .object_by_normalized_posix_path(&file_system_id, "/users/example/renamed.txt")
            .is_none(),
        "only a tombstone remains, which must not resolve"
    );

    // Removing the tombstone empties the bucket entirely.
    store.remove_object(&tombstone.object_id);
    assert!(store.objects_by_path.is_empty());
}

fn temp_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "sinomo-dfs-{name}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ))
}

fn networked_sifs_with_backend(
    root: &Path,
    node_id: NodeId,
) -> NetworkedDfsService<sinomo_network::InProcessTransport> {
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(node_id), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    dfs
}

fn drain_dfs_messages(
    transport: &mut sinomo_network::InProcessTransport,
) -> Vec<DfsNetworkMessage> {
    transport
        .drain_incoming()
        .expect("drain DFS frames")
        .into_iter()
        .filter_map(|frame| {
            let MeshMessageBody::Protocol(protocol) = frame.frame.message.body else {
                return None;
            };
            Some(protocol.decode::<DfsNetworkMessage>().expect("decode DFS"))
        })
        .collect()
}

fn test_chunked_assembler(
    root: &Path,
    name: &str,
    object_id: &DfsObjectId,
    version: &DfsVersion,
    content_hash: &str,
    total_size_bytes: u64,
    total_chunks: u32,
) -> (LocalContentStore, ChunkedBlobAssembler) {
    let store = LocalContentStore::new(root.join(name)).expect("chunk store");
    let assembler = store
        .begin_chunked_object_version(
            object_id.as_str(),
            version.as_str(),
            content_hash,
            total_size_bytes,
            total_chunks,
            DFS_BLOB_CHUNK_BYTES,
        )
        .expect("chunk assembler");
    (store, assembler)
}

#[test]
fn put_local_object_version_from_reader_streams_to_content_store() {
    let root = temp_root("put-local-object-version-from-reader");
    let node_id = NodeId::from_legacy_named("node_stream_local_object").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stream_local_object").expect("object");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let mut metadata = file_metadata(node_id);
    metadata.object_id = object_id.clone();
    metadata.path = "/services/streamed.bin".to_string();
    let size = 256 * 1024 + 31;
    let reader = std::io::repeat(0x42).take(size);

    let content_hash = dfs
        .put_local_object_version_from_reader(metadata, reader)
        .expect("stream object");

    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("streamed object metadata");
    assert_eq!(object.content_hash.as_deref(), Some(content_hash.as_str()));
    assert_eq!(object.content_size_bytes, Some(size));
    let bytes = dfs.content.read_blob(&content_hash).expect("streamed blob");
    assert_eq!(bytes.len() as u64, size);
    assert!(bytes.iter().all(|byte| *byte == 0x42));
    assert!(
        std::fs::read_dir(root.join("content/staging"))
            .expect("content staging")
            .next()
            .is_none(),
        "streaming local object writes must clean temporary staging files"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn lifecycle_maintenance_runs_version_blob_and_tombstone_collection() {
    let root = temp_root("lifecycle-maintenance-runs-collectors");
    let node_id = NodeId::from_legacy_named("node_lifecycle_maintenance").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_lifecycle_tombstone").expect("object");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let version_1 = dfs
        .content
        .put_object_version("object-lifecycle", "version_1", b"history bytes v1")
        .expect("put version 1");
    let version_2 = dfs
        .content
        .put_object_version("object-lifecycle", "version_2", b"history bytes v2")
        .expect("put version 2");

    let mut tombstone = file_metadata(node_id);
    tombstone.object_id = object_id.clone();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.path = "/services/lifecycle/deleted.txt".to_string();
    tombstone.content_hash = None;
    tombstone.content_size_bytes = None;
    tombstone.changed_at_unix_nanos = 10;
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_object(&tombstone)
        .expect("tombstone metadata");

    let report = dfs
        .run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            scrub_content: true,
            anti_entropy: None,
            version_gc: Some(ContentStoreVersionGcOptions::delete_eligible(
                std::time::Duration::from_nanos(0),
                1,
            )),
            orphan_gc: Some(ContentStoreGcOptions::delete_eligible(
                std::time::Duration::from_nanos(0),
            )),
            tombstone_compaction: Some(
                DfsTombstoneCompactionOptions::compact_eligible(std::time::Duration::from_nanos(
                    50,
                ))
                .with_now_unix_nanos(100),
            ),
        })
        .expect("lifecycle maintenance");

    assert!(
        report
            .content_scrub
            .as_ref()
            .expect("content scrub")
            .is_healthy()
    );
    let version_gc = report.version_gc.expect("version gc");
    assert_eq!(version_gc.deleted_version_count, 1);
    assert_eq!(version_gc.retained_latest_version_count, 1);
    let orphan_gc = report.orphan_gc.expect("orphan gc");
    assert_eq!(orphan_gc.deleted_orphan_count, 1);
    assert!(
        !dfs.content
            .has_blob(&version_1.content_hash)
            .expect("v1 reclaimed")
    );
    assert!(
        dfs.content
            .has_blob(&version_2.content_hash)
            .expect("v2 retained")
    );

    assert!(report.metadata_backend_configured);
    assert!(report.tombstone_compaction_requested);
    let tombstone_report = report.tombstone_compaction.expect("tombstone compaction");
    assert_eq!(tombstone_report.compacted_tombstone_count, 1);
    let backend = dfs.metadata_backend().expect("metadata backend");
    assert_eq!(
        backend.get_object(&object_id).expect("compacted object"),
        None
    );
    assert_eq!(
        backend
            .get_object_by_path(&sifs_file_system_id(), "/services/lifecycle/deleted.txt")
            .expect("path marker")
            .expect("compacted tombstone marker")
            .kind,
        DfsObjectKind::Tombstone
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn lifecycle_maintenance_reports_skipped_tombstones_without_backend() {
    let root = temp_root("lifecycle-maintenance-no-backend");
    let node_id = NodeId::from_legacy_named("node_lifecycle_no_backend").expect("node");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    let report = dfs
        .run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            tombstone_compaction: Some(DfsTombstoneCompactionOptions::compact_eligible(
                std::time::Duration::from_nanos(0),
            )),
            ..DfsLifecycleMaintenanceOptions::default()
        })
        .expect("lifecycle maintenance without backend");

    assert!(!report.metadata_backend_configured);
    assert!(report.tombstone_compaction_requested);
    assert!(report.tombstone_compaction.is_none());
    assert!(report.content_scrub.is_none());
    assert!(report.version_gc.is_none());
    assert!(report.orphan_gc.is_none());

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn local_content_anti_entropy_pages_and_repairs_stale_locations() {
    let root = temp_root("local-content-anti-entropy");
    let node_id = NodeId::from_legacy_named("node_anti_entropy").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("fs_anti_entropy").expect("fs");
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(node_id.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "anti entropy".to_string(),
        owner_node_id: node_id.clone(),
        subtree_policies: Vec::new(),
    };
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_file_system(&file_system)
        .expect("filesystem");

    let mut stale_a = file_metadata(node_id.clone());
    stale_a.file_system_id = file_system_id.clone();
    stale_a.object_id = DfsObjectId::from_legacy_named("obj_anti_entropy_stale_a").expect("object");
    stale_a.version = DfsVersion::from_legacy_named("version_anti_entropy_stale_a").expect("ver");
    stale_a.path = "/anti/000-stale-a.bin".to_string();
    stale_a.current_locations = vec![node_id.clone()];
    stale_a.content_hash = Some(content_hash(b"missing local bytes a"));
    stale_a.content_size_bytes = Some(21);

    let mut stale_b = file_metadata(node_id.clone());
    stale_b.file_system_id = file_system_id.clone();
    stale_b.object_id = DfsObjectId::from_legacy_named("obj_anti_entropy_stale_b").expect("object");
    stale_b.version = DfsVersion::from_legacy_named("version_anti_entropy_stale_b").expect("ver");
    stale_b.path = "/anti/001-stale-b.bin".to_string();
    stale_b.current_locations = vec![node_id.clone()];
    stale_b.content_hash = Some(content_hash(b"missing local bytes b"));
    stale_b.content_size_bytes = Some(21);

    let mut healthy = file_metadata(node_id.clone());
    healthy.file_system_id = file_system_id.clone();
    healthy.object_id = DfsObjectId::from_legacy_named("obj_anti_entropy_healthy").expect("object");
    healthy.version = DfsVersion::from_legacy_named("version_anti_entropy_healthy").expect("ver");
    healthy.path = "/anti/002-healthy.bin".to_string();
    let healthy_manifest = dfs
        .content
        .put_object_version(
            healthy.object_id.as_str(),
            healthy.version.as_str(),
            b"healthy local bytes",
        )
        .expect("healthy content");
    healthy.current_locations = vec![node_id.clone()];
    healthy.content_hash = Some(healthy_manifest.content_hash.clone());
    healthy.content_size_bytes = Some(healthy_manifest.size_bytes);

    {
        let backend = dfs.metadata_backend.as_mut().expect("metadata backend");
        backend.put_object(&stale_a).expect("stale a metadata");
        backend.put_object(&stale_b).expect("stale b metadata");
        backend.put_object(&healthy).expect("healthy metadata");
    }

    let first = dfs
        .run_local_content_anti_entropy(DfsAntiEntropyOptions {
            file_system_id: file_system_id.clone(),
            after: None,
            max_objects: 1,
            max_issues: 1,
            repair_metadata: true,
        })
        .expect("first anti-entropy page");
    assert_eq!(first.scanned_object_count, 1);
    assert_eq!(first.checked_local_location_count, 1);
    assert_eq!(first.stale_local_location_count, 1);
    assert_eq!(first.repaired_location_count, 1);
    assert_eq!(first.issue_count, 1);
    assert_eq!(first.issues.len(), 1);
    assert!(first.issues[0].repaired);
    assert!(first.next_cursor.is_some());

    let backend = dfs.metadata_backend().expect("metadata backend");
    let repaired_a = backend
        .get_object(&stale_a.object_id)
        .expect("stale a object")
        .expect("stale a after repair");
    assert!(!repaired_a.current_locations.contains(&node_id));
    let repaired_a_version = backend
        .get_version(&stale_a.object_id, &stale_a.version)
        .expect("stale a version")
        .expect("stale a version after repair");
    assert!(!repaired_a_version.locations.contains(&node_id));

    let second = dfs
        .run_local_content_anti_entropy(DfsAntiEntropyOptions {
            file_system_id: file_system_id.clone(),
            after: first.next_cursor,
            max_objects: 8,
            max_issues: 8,
            repair_metadata: true,
        })
        .expect("second anti-entropy page");
    assert_eq!(second.scanned_object_count, 2);
    assert_eq!(second.checked_local_location_count, 2);
    assert_eq!(second.stale_local_location_count, 1);
    assert_eq!(second.repaired_location_count, 1);
    assert!(second.next_cursor.is_none());

    let backend = dfs.metadata_backend().expect("metadata backend");
    let repaired_b = backend
        .get_object(&stale_b.object_id)
        .expect("stale b object")
        .expect("stale b after repair");
    assert!(!repaired_b.current_locations.contains(&node_id));
    let healthy_object = backend
        .get_object(&healthy.object_id)
        .expect("healthy object")
        .expect("healthy remains");
    assert!(healthy_object.current_locations.contains(&node_id));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn add_object_metadata_batch_persists_visible_versions_in_one_transaction() {
    let root = temp_root("add-object-metadata-batch-visible-versions");
    let owner = NodeId::from_legacy_named("node_batch_visible_owner").expect("node");
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");

    let objects = (0..3)
        .map(|index| {
            let mut object = file_metadata(owner.clone());
            object.object_id = DfsObjectId::from_legacy_named(format!("obj_batch_visible_{index}"))
                .expect("object");
            object.path = format!("/services/batch-visible/file-{index}.txt");
            object.content_hash = Some(format!("blake3:{index:064x}"));
            object.content_size_bytes = Some(128 + index);
            object.version =
                DfsVersion::from_legacy_named(format!("version_batch_visible_{index}"))
                    .expect("version");
            object
        })
        .collect::<Vec<_>>();

    dfs.add_object_metadata_batch(objects.clone())
        .expect("metadata batch");

    let backend = dfs.metadata_backend().expect("metadata backend");
    let entries = backend.journal_since(0).expect("journal");
    assert_eq!(
        entries.len(),
        2,
        "filesystem plus one namespace transaction"
    );
    let transaction = entries
        .iter()
        .find_map(|entry| match &entry.mutation {
            DfsMetadataMutation::ApplyNamespaceTransaction {
                transaction: Some(transaction),
                ..
            } => Some(transaction),
            _ => None,
        })
        .expect("namespace transaction");
    assert_eq!(transaction.objects.len(), objects.len());
    assert_eq!(transaction.versions.len(), objects.len());

    for object in &objects {
        assert_eq!(
            backend.get_object(&object.object_id).expect("object"),
            Some(object.clone())
        );
        let version = backend
            .get_version(&object.object_id, &object.version)
            .expect("version")
            .expect("visible version");
        assert_eq!(version.content_hash, object.content_hash);
        assert_eq!(version.size_bytes, object.content_size_bytes);
        assert!(version.visible);
    }

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_replication_cursor_advances_only_after_replica_ack() {
    let root = temp_root("metadata-replication-cursor-after-ack");
    let primary = NodeId::from_legacy_named("node_meta_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_meta_replica").expect("replica");
    let file_system = file_system(primary.clone());
    let shared_kv = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![primary.clone()]).expect("kv"),
    );
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: primary.clone(),
                replica_node_ids: vec![primary.clone(), replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });

    let network = InProcessNetwork::new();
    let mut drop_replica_mailbox = network.add_node(replica.clone());
    let primary_content = LocalContentStore::new(root.join("primary-content")).expect("content");
    let mut primary_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("primary-meta"))
            .expect("primary backend");
    primary_backend
        .put_file_system(&file_system)
        .expect("primary filesystem");
    let mut first = file_metadata(primary.clone());
    first.object_id = DfsObjectId::from_legacy_named("obj_meta_first").expect("object");
    first.path = "/services/metadata/first.txt".to_string();
    first.file_system_id = file_system.file_system_id.clone();
    first.policy = DfsObjectPolicy::ReplicatedEventual;
    primary_backend.put_object(&first).expect("first object");
    let mut primary_dfs =
        NetworkedDfsService::new(network.add_node(primary.clone()), primary_content)
            .with_control_plane(shared_kv.clone())
            .with_metadata_backend(primary_backend)
            .expect("primary dfs");

    let outcomes = primary_dfs
        .replicate_metadata_shard_to_configured_replicas(&file_system.file_system_id, "root")
        .expect("first replication send");
    assert!(
        matches!(
            outcomes.as_slice(),
            [DfsMetadataReplicationOutcome::Sent { .. }]
        ),
        "unexpected first replication outcomes: {outcomes:?}"
    );

    assert_eq!(
        NetworkTransport::drain_incoming(&mut drop_replica_mailbox)
            .expect("drop first batch")
            .len(),
        1,
        "the first metadata batch is intentionally lost before the replica applies it"
    );

    let mut second = file_metadata(primary.clone());
    second.object_id = DfsObjectId::from_legacy_named("obj_meta_second").expect("object");
    second.path = "/services/metadata/second.txt".to_string();
    second.file_system_id = file_system.file_system_id.clone();
    second.policy = DfsObjectPolicy::ReplicatedEventual;
    primary_dfs
        .add_object_metadata(second.clone())
        .expect("second object");

    let outcomes = primary_dfs
        .replicate_metadata_shard_to_configured_replicas(&file_system.file_system_id, "root")
        .expect("retry replication before ack");
    assert!(matches!(
        outcomes.as_slice(),
        [DfsMetadataReplicationOutcome::Sent { entries, .. }] if *entries >= 3
    ));

    let replica_content = LocalContentStore::new(root.join("replica-content")).expect("content");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("replica-meta"))
            .expect("replica backend");
    let mut replica_dfs =
        NetworkedDfsService::new(network.add_node(replica.clone()), replica_content)
            .with_metadata_backend(replica_backend)
            .expect("replica dfs");
    let events = replica_dfs
        .process_incoming()
        .expect("replica applies retry");
    assert!(
        matches!(
            events.as_slice(),
            [NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                applied_entries,
                ..
            }] if *applied_entries >= 3
        ),
        "unexpected replica events: {events:?}"
    );

    let replica_backend = replica_dfs
        .metadata_backend()
        .expect("replica metadata backend");
    assert_eq!(
        replica_backend
            .get_object(&first.object_id)
            .expect("first replicated")
            .as_ref()
            .map(|object| object.path.as_str()),
        Some(first.path.as_str())
    );
    assert_eq!(
        replica_backend
            .get_object(&second.object_id)
            .expect("second replicated")
            .as_ref()
            .map(|object| object.path.as_str()),
        Some(second.path.as_str())
    );

    let owner_events = primary_dfs.process_incoming().expect("owner handles ack");
    assert!(matches!(
        owner_events.as_slice(),
        [NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
            source_node_id,
            next_after_sequence,
            ..
        }] if source_node_id == &replica && *next_after_sequence >= 3
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_journal_intent_conflict_applies_incrementally_without_full_hydrate() {
    let root = temp_root("metadata-journal-intent-conflict-incremental");
    let owner = NodeId::from_legacy_named("node_meta_conflict_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_meta_conflict_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_meta_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let file_system = file_system(owner.clone());
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/services/metadata/conflict.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];

    let mut backend = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("object");
    backend
        .put_intent(&DfsWriteIntentRecord {
            intent_id: "dfs_intent_meta_conflict_first".to_string(),
            file_system_id: file_system.file_system_id.clone(),
            object_id: object_id.clone(),
            base_version: Some(version_1.clone()),
            target_version: version_2.clone(),
            coordinator_node_id: owner.clone(),
            owner_epoch: 0,
            desired_replica_node_ids: vec![owner.clone(), replica.clone()],
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: None,
            object_metadata: Some(object.clone()),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        })
        .expect("first intent");

    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner.clone()), content)
        .with_metadata_backend(backend)
        .expect("dfs");
    dfs.pending_reads.insert(
        "sentinel-read".to_string(),
        PendingNetworkedRead {
            object_id: object_id.clone(),
            version: version_1.clone(),
            content_hash: None,
            content_size_bytes: None,
            range_offset: None,
            range_max_bytes: None,
            next_chunk_index: 0,
            window_end_chunk_index: None,
            target_node_id: replica.clone(),
            next_retry_at: std::time::Instant::now(),
        },
    );

    let second_intent = DfsWriteIntentRecord {
        intent_id: "dfs_intent_meta_conflict_second".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        object_id: object_id.clone(),
        base_version: Some(version_1.clone()),
        target_version: version_3.clone(),
        coordinator_node_id: replica.clone(),
        owner_epoch: 0,
        desired_replica_node_ids: vec![owner.clone(), replica.clone()],
        policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: None,
        object_metadata: Some(object.clone()),
        created_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
        updated_at: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1)),
    };
    let entry = dfs
        .metadata_backend
        .as_mut()
        .expect("backend")
        .put_intent(&second_intent)
        .expect("second intent");
    let batch = DfsMetadataJournalBatch {
        shard_id: entry.shard_id.clone(),
        next_after_sequence: entry.sequence,
        entries: vec![entry],
    };

    dfs.apply_metadata_journal_batch_to_service_state(&batch)
        .expect("incremental apply");

    assert!(
        dfs.pending_reads.contains_key("sentinel-read"),
        "metadata journal conflict handling must not clear active read state with a full hydrate"
    );
    let stored_intent = dfs
        .metadata_backend()
        .expect("backend")
        .get_intent(&object_id, &second_intent.intent_id)
        .expect("stored intent")
        .expect("second intent remains");
    assert_eq!(stored_intent.status, DfsWriteIntentStatus::Conflict);
    assert!(
        !dfs.conflicts_for_object(&object_id)
            .expect("conflicts")
            .is_empty()
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_journal_missing_namespace_transaction_payload_uses_keyed_replay() {
    let root = temp_root("metadata-journal-namespace-transaction-keyed-replay");
    let owner = NodeId::from_legacy_named("node_namespace_tx_owner").expect("owner");
    let file_system = file_system(owner.clone());
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
    backend.put_file_system(&file_system).expect("filesystem");

    let mut unrelated = file_metadata(owner.clone());
    unrelated.object_id =
        DfsObjectId::from_legacy_named("obj_namespace_tx_unrelated").expect("object");
    unrelated.path = "/services/namespace-tx/unrelated.txt".to_string();
    backend.put_object(&unrelated).expect("unrelated object");

    let mut touched = file_metadata(owner.clone());
    touched.object_id = DfsObjectId::from_legacy_named("obj_namespace_tx_touched").expect("object");
    touched.path = "/services/namespace-tx/touched.txt".to_string();
    touched.version =
        DfsVersion::from_legacy_named("version_namespace_tx_touched").expect("touched version");
    let transaction = DfsNamespaceTransaction {
        transaction_id: "namespace_tx_keyed_replay".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        objects: vec![touched.clone()],
        versions: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    let mut entry = backend
        .apply_namespace_transaction(&transaction)
        .expect("transaction");
    if let DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } = &mut entry.mutation
    {
        *transaction = None;
    } else {
        panic!("expected namespace transaction journal entry");
    }

    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner), content);
    dfs.metadata.add_file_system(file_system);
    dfs.metadata_backend = Some(Box::new(backend));

    dfs.apply_metadata_journal_batch_to_service_state(&DfsMetadataJournalBatch {
        shard_id: entry.shard_id.clone(),
        next_after_sequence: entry.sequence,
        entries: vec![entry],
    })
    .expect("incremental namespace transaction replay");

    assert!(
        dfs.metadata.object(&touched.object_id).is_some(),
        "missing-payload namespace transaction replay should restore touched objects"
    );
    assert!(
        dfs.metadata.object(&unrelated.object_id).is_none(),
        "missing-payload namespace transaction replay should not scan and import the whole file system"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_journal_missing_namespace_transaction_record_fails_without_full_scan() {
    let root = temp_root("metadata-journal-namespace-transaction-missing-record");
    let owner = NodeId::from_legacy_named("node_namespace_tx_missing_owner").expect("owner");
    let file_system = file_system(owner.clone());
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
    backend.put_file_system(&file_system).expect("filesystem");

    let mut unrelated = file_metadata(owner.clone());
    unrelated.object_id =
        DfsObjectId::from_legacy_named("obj_namespace_tx_missing_unrelated").expect("object");
    unrelated.path = "/services/namespace-tx-missing/unrelated.txt".to_string();
    backend.put_object(&unrelated).expect("unrelated object");

    let entry = DfsMetadataJournalEntry {
        shard_id: DfsMetadataShardId::new("root"),
        sequence: 99,
        created_at: OffsetDateTime::UNIX_EPOCH,
        mutation: DfsMetadataMutation::ApplyNamespaceTransaction {
            file_system_id: file_system.file_system_id.clone(),
            transaction_id: "namespace_tx_missing_record".to_string(),
            object_count: 1,
            transaction: None,
        },
    };

    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner), content);
    dfs.metadata.add_file_system(file_system);
    dfs.metadata_backend = Some(Box::new(backend));

    let error = dfs
        .apply_metadata_journal_batch_to_service_state(&DfsMetadataJournalBatch {
            shard_id: entry.shard_id.clone(),
            next_after_sequence: entry.sequence,
            entries: vec![entry],
        })
        .expect_err("missing namespace transaction record should fail replay");
    assert!(
        error
            .to_string()
            .contains("cannot apply namespace transaction namespace_tx_missing_record without its transaction payload")
    );
    assert!(
        dfs.metadata.object(&unrelated.object_id).is_none(),
        "unrecoverable sparse namespace transaction replay must not import the whole file system"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn distributed_write_uses_path_selected_metadata_shard_epoch() {
    let root = temp_root("distributed-write-path-shard-epoch");
    let owner = NodeId::from_legacy_named("node_path_shard_owner").expect("owner");
    let root_primary =
        NodeId::from_legacy_named("node_path_shard_root_primary").expect("root primary");
    let replica = NodeId::from_legacy_named("node_path_shard_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_path_shard_epoch").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let file_system = file_system(owner.clone());
    let shard_configs = vec![
        DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: root_primary.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 7,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    ];
    let shared_kv = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![owner.clone()]).expect("kv"),
    );
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &shard_configs {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    });

    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_local_sharded_metadata_backend(root.join("metadata"), shard_configs)
        .expect("metadata");
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: owner.clone(),
        },
        content,
    )
    .with_control_plane(shared_kv)
    .with_metadata_backend(metadata)
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.file_system_id = file_system.file_system_id.clone();
    metadata.path = "/services/path-shard.txt".to_string();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata).expect("object");

    let write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system.file_system_id,
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            b"path shard bytes".to_vec(),
            vec![owner, replica],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("path shard write");

    let NetworkedDfsWriteOutcome::Committed { intent_id, .. } = write else {
        panic!("expected committed write, got {write:?}");
    };
    let intent = dfs
        .metadata_backend()
        .expect("metadata backend")
        .get_intent(&object_id, &intent_id)
        .expect("read intent")
        .expect("intent");
    assert_eq!(intent.owner_epoch, 7);
    let services_batch = dfs
        .metadata_backend()
        .expect("metadata backend")
        .journal_batch_for_shard_since_limited(
            &DfsMetadataShardId::new("services"),
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("services journal")
        .expect("services entries");
    assert!(
        services_batch.entries.iter().any(|entry| matches!(
            entry.mutation,
            DfsMetadataMutation::PutIntent { .. } | DfsMetadataMutation::PutVersion { .. }
        )),
        "distributed write metadata must be journaled on the selected services shard"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_write_response_tracks_remote_intent_alias() {
    let requester =
        NodeId::from_legacy_named("node_routed_write_alias_requester").expect("requester");
    let primary = NodeId::from_legacy_named("node_routed_write_alias_primary").expect("primary");
    let object_id = DfsObjectId::from_legacy_named("obj_routed_write_alias").expect("object");
    let version = DfsVersion::from_legacy_named("version_routed_write_alias").expect("version");
    let root = temp_root("routed-write-alias");
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: requester,
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.pending_routed_writes.insert(
        "dfs_net_req_1".to_string(),
        PendingRoutedWrite {
            object_id: object_id.clone(),
            primary_node_id: primary.clone(),
            remote_intent_id: None,
            durable_receipts: 0,
            required_receipts: 1,
            result: None,
        },
    );

    dfs.handle_distributed_write_response(
        primary.clone(),
        DfsDistributedWriteResponse {
            request_id: "dfs_net_req_1".to_string(),
            object_id: object_id.clone(),
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Pending {
                    intent_id: "intent_remote_alias".to_string(),
                    durable_receipts: 1,
                    required_receipts: 2,
                },
            },
        },
    )
    .expect("pending response");

    assert!(matches!(
        dfs.write_outcome("dfs_net_req_1").expect("request outcome"),
        NetworkedDfsWriteOutcome::Pending {
            intent_id,
            durable_receipts: 1,
            required_receipts: 2,
        } if intent_id == "intent_remote_alias"
    ));
    assert!(
        dfs.pending_routed_writes
            .contains_key("intent_remote_alias")
    );

    dfs.handle_distributed_write_response(
        primary,
        DfsDistributedWriteResponse {
            request_id: "intent_remote_alias".to_string(),
            object_id,
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Committed {
                    intent_id: "intent_remote_alias".to_string(),
                    version: version.clone(),
                    content_hash: "blake3:routed-write-alias".to_string(),
                    repair_jobs: Vec::new(),
                },
            },
        },
    )
    .expect("committed response");

    assert!(matches!(
        dfs.write_outcome("intent_remote_alias")
            .expect("alias outcome"),
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            version: committed_version,
            ..
        } if intent_id == "intent_remote_alias" && committed_version == version
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stage_response_commits_restored_pending_intent_without_inflight_request_id() {
    let root = temp_root("stage-response-commits-restored-pending-intent");
    let primary =
        NodeId::from_legacy_named("node_stage_response_restore_primary").expect("primary");
    let replica =
        NodeId::from_legacy_named("node_stage_response_restore_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_response_restore").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_stage_response_restore_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_stage_response_restore_2").expect("version");
    let bytes = b"stage response must survive stateless daemon handling";
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(primary.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut object = file_metadata(primary.clone());
    object.object_id = object_id.clone();
    object.path = "/services/stateless-stage-response.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![primary.clone()];
    dfs.add_object_metadata(object).expect("object metadata");

    let outcome = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: primary.clone(),
            },
            bytes.to_vec(),
            vec![primary.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("pending distributed write");
    let NetworkedDfsWriteOutcome::Pending {
        intent_id,
        durable_receipts,
        required_receipts,
    } = outcome
    else {
        panic!("expected pending write, got {outcome:?}");
    };
    assert_eq!(durable_receipts, 1);
    assert_eq!(required_receipts, 2);
    let content_hash = dfs
        .metadata_backend()
        .expect("metadata backend")
        .get_intent(&object_id, &intent_id)
        .expect("intent read")
        .expect("intent")
        .content_hash
        .expect("intent content hash");
    drop(dfs);

    let restarted_network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut restarted =
        NetworkedDfsService::new(restarted_network.add_node(primary.clone()), content)
            .with_metadata_backend(metadata)
            .expect("restored metadata backend");
    assert!(
        restarted
            .pending_writes
            .get(&intent_id)
            .is_some_and(|pending| pending.stage_inflight_request_ids.is_empty()),
        "restored daemon state should not rely on ephemeral inflight request ids"
    );

    let event = restarted
        .handle_stage_blob_response(
            replica.clone(),
            DfsStageBlobResponse {
                request_id: "lost-daemon-inflight-request-id".to_string(),
                intent_id: intent_id.clone(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                version: version_2.clone(),
                replica_node_id: replica.clone(),
                result: DfsBlobTransferResult::Durable {
                    content_hash: content_hash.clone(),
                },
            },
        )
        .expect("stage response")
        .expect("stage event");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobStaged {
            intent_id: ref event_intent_id,
            ref replica_node_id,
            ..
        } if event_intent_id == &intent_id && replica_node_id == &replica
    ));
    assert!(matches!(
        restarted.write_outcome(&intent_id).expect("write outcome"),
        NetworkedDfsWriteOutcome::Committed {
            version,
            content_hash: ref committed_hash,
            ..
        } if version == version_2 && committed_hash == &content_hash
    ));
    let committed = restarted
        .current_read_object_metadata(&object_id)
        .expect("committed object");
    assert_eq!(committed.version, version_2);
    assert_eq!(
        committed.content_hash.as_deref(),
        Some(content_hash.as_str())
    );
    assert!(committed.current_locations.contains(&primary));
    assert!(committed.current_locations.contains(&replica));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stage_response_restores_transient_intent_record_without_visible_object() {
    let root = temp_root("stage-response-restores-transient-intent-record");
    let primary =
        NodeId::from_legacy_named("node_stage_response_transient_primary").expect("primary");
    let replica =
        NodeId::from_legacy_named("node_stage_response_transient_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_response_transient").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_stage_response_transient_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_stage_response_transient_2").expect("version");
    let bytes = b"transient intent metadata restores stateless primary commit";
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let manifest = content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("content");
    let mut object = file_metadata(primary.clone());
    object.object_id = object_id.clone();
    object.path = "/services/stateless-transient-stage-response.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![primary.clone()];
    let intent_id = "dfs_intent_stage_response_transient".to_string();
    let mut backend = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    backend
        .put_file_system(&file_system(primary.clone()))
        .expect("filesystem");
    backend
        .put_intent_for_object(
            Some(&object),
            &DfsWriteIntentRecord {
                intent_id: intent_id.clone(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                base_version: Some(version_1),
                target_version: version_2.clone(),
                coordinator_node_id: primary.clone(),
                owner_epoch: 0,
                desired_replica_node_ids: vec![primary.clone(), replica.clone()],
                policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(manifest.content_hash.clone()),
                object_metadata: Some(object.clone()),
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            },
        )
        .expect("intent");
    backend
        .put_receipt_for_object(
            None,
            &DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    committed_version: version_2.clone(),
                    owner_node_id: primary.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: primary.clone(),
                content_hash: Some(manifest.content_hash.clone()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            },
        )
        .expect("primary receipt");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(network.add_node(primary.clone()), content)
        .with_metadata_backend(backend)
        .expect("metadata backend");
    assert!(
        dfs.coordinator.intent(&intent_id).is_none(),
        "intent should not be hydrated through visible-object enumeration"
    );
    assert!(dfs.current_read_object_metadata(&object_id).is_err());

    let event = dfs
        .handle_stage_blob_response(
            replica.clone(),
            DfsStageBlobResponse {
                request_id: "lost-daemon-inflight-request-id".to_string(),
                intent_id: intent_id.clone(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                version: version_2.clone(),
                replica_node_id: replica.clone(),
                result: DfsBlobTransferResult::Durable {
                    content_hash: manifest.content_hash.clone(),
                },
            },
        )
        .expect("stage response")
        .expect("stage event");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobStaged {
            intent_id: ref event_intent_id,
            ..
        } if event_intent_id == &intent_id
    ));
    assert!(matches!(
        dfs.write_outcome(&intent_id).expect("write outcome"),
        NetworkedDfsWriteOutcome::Committed {
            version,
            content_hash,
            ..
        } if version == version_2 && content_hash == manifest.content_hash
    ));
    let committed = dfs
        .current_read_object_metadata(&object_id)
        .expect("committed object");
    assert_eq!(committed.version, version_2);
    assert_eq!(
        committed.content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn posix_transient_create_routes_precommit_records_to_path_shard() {
    let root = temp_root("posix-transient-create-path-shard");
    let owner = NodeId::from_legacy_named("node_transient_path_shard_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_transient_path_shard_replica").expect("replica");
    let file_system = file_system(owner.clone());
    let shard_configs = vec![
        DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 7,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    ];
    let shared_kv = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![owner.clone()]).expect("kv"),
    );
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &shard_configs {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    });

    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_local_sharded_metadata_backend(root.join("metadata"), shard_configs)
        .expect("metadata");
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: owner.clone(),
        },
        content,
    )
    .with_control_plane(shared_kv)
    .with_metadata_backend(metadata)
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    dfs.posix_mkdir_all("/services/dfs-smoke")
        .expect("smoke directory");

    let stat = dfs
        .posix_write_file(
            "/services/dfs-smoke/transient-create.txt",
            b"path shard transient create",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
                replica_candidates: vec![owner.clone(), replica],
                wait_for_commit: true,
            },
        )
        .expect("transient create write");

    let backend = dfs.metadata_backend().expect("metadata backend");
    assert_eq!(
        backend
            .get_object(&stat.object_id)
            .expect("committed object")
            .as_ref()
            .map(|object| object.path.as_str()),
        Some("/services/dfs-smoke/transient-create.txt")
    );
    let services_batch = backend
        .journal_batch_for_shard_since_limited(
            &DfsMetadataShardId::new("services"),
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("services journal")
        .expect("services entries");
    assert!(
        services_batch
            .entries
            .iter()
            .any(|entry| matches!(entry.mutation, DfsMetadataMutation::PutIntent { .. })),
        "transient create intent must be journaled on the selected services shard"
    );
    assert!(
        services_batch
            .entries
            .iter()
            .any(|entry| matches!(entry.mutation, DfsMetadataMutation::PutReceipt { .. })),
        "transient create receipt must be journaled on the selected services shard"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn eventual_write_with_local_receipt_tolerates_unreachable_desired_replica() {
    let root = temp_root("eventual-write-tolerates-unreachable-replica");
    let owner = NodeId::from_legacy_named("node_eventual_tolerates_owner").expect("owner node");
    let replica =
        NodeId::from_legacy_named("node_eventual_tolerates_replica").expect("replica node");
    let object_id = DfsObjectId::from_legacy_named("obj_eventual_tolerates").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: owner.clone(),
        },
        content,
    )
    .with_metadata_backend(metadata)
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata).expect("object");

    let write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            b"eventual bytes".to_vec(),
            vec![owner, replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("write should commit locally");

    let NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } = write else {
        panic!("expected committed write, got {write:?}");
    };
    assert_eq!(repair_jobs.len(), 1);
    assert_eq!(repair_jobs[0].missing_replica_node_id, replica);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn receipt_updates_visible_current_locations_for_current_version() {
    let root = temp_root("receipt-updates-current-locations");
    let owner = NodeId::from_legacy_named("node_receipt_location_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_receipt_location_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_receipt_location").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    metadata.content_hash = Some("blake3:receipt-location".to_string());
    dfs.add_object_metadata(metadata).expect("metadata");

    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version,
        &replica,
        "blake3:receipt-location",
    )
    .expect("receipt");

    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object");
    assert_eq!(object.current_locations, vec![owner, replica]);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_clears_partial_blob_transfers() {
    let root = temp_root("hydrate-clears-partial-blob-transfers");
    let node_id = NodeId::from_legacy_named("node_hydrate_clear").expect("node");
    let requester = NodeId::from_legacy_named("node_requester").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_partial_transfer").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let mut dfs = networked_sifs_with_backend(&root, node_id);
    let content_hash = content_hash(b"partial1");
    let (_, assembler) = test_chunked_assembler(
        &root,
        "partial-transfer",
        &object_id,
        &version,
        &content_hash,
        8,
        1,
    );
    dfs.incoming_blob_transfers.insert(
        blob_transfer_key("stage", &requester, "intent_partial", "dfs_net_req_partial"),
        InProgressBlobTransfer::new(
            "dfs_net_req_partial".to_string(),
            "intent_partial".to_string(),
            sifs_file_system_id(),
            object_id,
            None,
            version,
            content_hash,
            8,
            1,
            assembler,
        )
        .expect("partial transfer"),
    );

    dfs.hydrate_from_metadata_backend().expect("rehydrate");

    assert!(
        dfs.incoming_blob_transfers.is_empty(),
        "rehydration must not keep chunk assemblers from the previous metadata view"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
#[ignore = "slow page-boundary hydration regression; run with `cargo test -p sinomo-dfs hydrate_from_metadata_backend_walks_multiple_object_pages -- --ignored`"]
fn hydrate_from_metadata_backend_walks_multiple_object_pages() {
    let root = temp_root("hydrate-multiple-object-pages");
    let node_id = NodeId::from_legacy_named("node_hydrate_pages").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let tail_index = DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE;
    let mut tail_object_id = None;
    for index in 0..=tail_index {
        let mut object = file_metadata(node_id.clone());
        object.object_id =
            DfsObjectId::from_legacy_named(format!("obj_hydrate_page_{index}")).expect("object");
        object.path = format!("/users/hydrate/page-{index:05}.txt");
        object.version = DfsVersion::from_legacy_named(format!("version_hydrate_page_{index}"))
            .expect("version");
        if index == tail_index {
            tail_object_id = Some(object.object_id.clone());
        }
        dfs.add_object_metadata(object).expect("metadata object");
    }

    let mut tombstone = file_metadata(node_id);
    tombstone.object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_page_tombstone").expect("tombstone");
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.path = "/users/hydrate/deleted.txt".to_string();
    dfs.add_object_metadata(tombstone.clone())
        .expect("tombstone metadata");

    dfs.metadata = DfsMetadataStore::new();
    dfs.tombstones_by_metadata_path.clear();
    dfs.hydrate_from_metadata_backend().expect("hydrate pages");

    let tail_object = dfs
        .current_read_object_metadata(&tail_object_id.expect("tail object"))
        .expect("tail object loaded");
    assert_eq!(
        tail_object.path,
        format!("/users/hydrate/page-{tail_index:05}.txt")
    );
    assert!(
        dfs.tombstones_by_metadata_path
            .contains_key(&(sifs_file_system_id(), metadata_path(&tombstone.path))),
        "paged hydration must restore tombstone tracking"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrated_tombstone_tracking_is_incremental_and_live_path_aware() {
    let root = temp_root("hydrate-incremental-tombstones");
    let node_id = NodeId::from_legacy_named("node_hydrate_tombstones").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.metadata = DfsMetadataStore::new();
    dfs.metadata.add_file_system(file_system(node_id.clone()));
    dfs.tombstones_by_metadata_path.clear();

    let mut live_first = file_metadata(node_id.clone());
    live_first.object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_live_first").expect("object");
    live_first.path = "/users/hydrate/live-first.txt".to_string();
    dfs.metadata
        .add_object(live_first.clone())
        .expect("live object");

    let mut late_tombstone = file_metadata(node_id.clone());
    late_tombstone.object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_late_tombstone").expect("object");
    late_tombstone.kind = DfsObjectKind::Tombstone;
    late_tombstone.path = live_first.path.clone();
    dfs.track_hydrated_metadata_tombstone(&late_tombstone);
    assert!(
        !dfs.tombstones_by_metadata_path
            .contains_key(&(sifs_file_system_id(), metadata_path(&late_tombstone.path))),
        "hydration must not retain a tombstone for a path already owned by a live object"
    );

    let mut early_tombstone = file_metadata(node_id.clone());
    early_tombstone.object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_early_tombstone").expect("object");
    early_tombstone.kind = DfsObjectKind::Tombstone;
    early_tombstone.path = "/users/hydrate/live-after-tombstone.txt".to_string();
    dfs.track_hydrated_metadata_tombstone(&early_tombstone);
    assert!(
        dfs.tombstones_by_metadata_path
            .contains_key(&(sifs_file_system_id(), metadata_path(&early_tombstone.path))),
        "a tombstone should be tracked until hydration sees a live object for the path"
    );

    let mut live_later = file_metadata(node_id);
    live_later.object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_live_later").expect("object");
    live_later.path = early_tombstone.path.clone();
    dfs.track_hydrated_metadata_tombstone(&live_later);
    assert!(
        !dfs.tombstones_by_metadata_path
            .contains_key(&(sifs_file_system_id(), metadata_path(&early_tombstone.path))),
        "a later live object should clear a previously tracked hydrated tombstone"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_walks_multiple_conflict_pages() {
    let root = temp_root("hydrate-multiple-conflict-pages");
    let node_id = NodeId::from_legacy_named("node_hydrate_conflict_pages").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_conflict_pages").expect("object");
    let mut object = file_metadata(node_id.clone());
    object.object_id = object_id.clone();
    object.path = "/users/hydrate/conflicted.txt".to_string();
    object.version =
        DfsVersion::from_legacy_named("version_hydrate_conflict_remote").expect("remote version");
    dfs.add_object_metadata(object.clone())
        .expect("metadata object");

    let conflict_count = DFS_CONFLICT_SCAN_PAGE_SIZE + 1;
    for index in 0..conflict_count {
        let conflict = DfsConflictRecord {
            conflict_id: format!("conflict_hydrate_page_{index:04}"),
            file_system_id: object.file_system_id.clone(),
            object_id: object_id.clone(),
            path: object.path.clone(),
            base_version: Some(
                DfsVersion::from_legacy_named("version_hydrate_conflict_base")
                    .expect("base version"),
            ),
            local_version: DfsVersion::from_legacy_named(format!(
                "version_hydrate_conflict_local_{index}"
            ))
            .expect("local version"),
            remote_version: object.version.clone(),
            detected_by_node_id: node_id.clone(),
            reason: "restart hydration must page conflict records".to_string(),
            local_content_hash: Some(format!("blake3:local-hydrate-conflict-{index}")),
            remote_content_hash: object.content_hash.clone(),
            created_by_intent_id: None,
            strategy: Some("branch".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::UNIX_EPOCH,
        };
        dfs.put_conflict_record(&conflict).expect("conflict record");
    }

    dfs.metadata = DfsMetadataStore::new();
    dfs.hydrate_from_metadata_backend()
        .expect("hydrate conflict pages");

    let loaded_conflicts = dfs
        .conflicts_for_object(&object_id)
        .expect("loaded conflicts");
    assert_eq!(
        loaded_conflicts.len(),
        conflict_count,
        "hydration must walk every conflict page for an object"
    );
    assert_eq!(
        loaded_conflicts.last().expect("tail conflict").conflict_id,
        format!("conflict_hydrate_page_{:04}", conflict_count - 1)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_does_not_resurrect_aborted_pending_write() {
    let root = temp_root("hydrate-skips-aborted-pending-write");
    let owner = NodeId::from_legacy_named("node_hydrate_abort_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_abort_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_abort").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata).expect("object");

    let write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"aborted pending bytes".to_vec(),
            vec![dfs.node_id().clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("pending write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    dfs.abort_pending_write(&intent_id).expect("abort");
    assert!(
        !dfs.pending_writes.contains_key(&intent_id),
        "abort should remove in-memory pending write"
    );

    dfs.hydrate_from_metadata_backend().expect("rehydrate");

    let restored_aborted_intent = dfs
        .metadata_backend()
        .expect("metadata backend")
        .get_intent(&object_id, &intent_id)
        .expect("aborted intent lookup")
        .expect("aborted intent remains durable");
    assert_eq!(
        restored_aborted_intent.status,
        DfsWriteIntentStatus::Aborted
    );
    assert!(
        dfs.coordinator().intent(&intent_id).is_none(),
        "closed aborted intents should advance restored ID state without occupying coordinator memory"
    );
    assert_eq!(
        dfs.metadata()
            .object(&object_id)
            .expect("visible object")
            .version,
        version_1
    );
    assert!(
        !dfs.pending_writes.contains_key(&intent_id),
        "rehydration must not resurrect pending state for aborted writes"
    );
    let next_write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner,
            },
            b"aborted pending bytes".to_vec(),
            vec![dfs.node_id().clone(), replica],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("retry pending write");
    let next_intent_id = match next_write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected retry pending write, got {other:?}"),
    };
    assert_ne!(
        next_intent_id, intent_id,
        "hydration must observe closed intent ids so new writes do not reuse them"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_converts_competing_pending_intent_to_conflict() {
    let root = temp_root("hydrate-competing-pending-intent-conflict");
    let owner = NodeId::from_legacy_named("node_hydrate_conflict_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_conflict_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let second_bytes = b"recovered competing pending write";
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let content_root = root.join("owner-content");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(&content_root).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let first = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"first pending write".to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("first pending write");
    let first_intent_id = match first {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    let second_hash = owner_dfs
        .content()
        .put_object_version(object_id.as_str(), version_3.as_str(), second_bytes)
        .expect("second content")
        .content_hash;
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("backend")
        .put_intent(&DfsWriteIntentRecord {
            intent_id: format!("dfs_intent_{}_99", owner.as_str()),
            file_system_id: sifs_file_system_id(),
            object_id: object_id.clone(),
            base_version: Some(version_1.clone()),
            target_version: version_3.clone(),
            coordinator_node_id: owner.clone(),
            owner_epoch: 0,
            desired_replica_node_ids: vec![owner.clone(), replica],
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some(second_hash.clone()),
            object_metadata: None,
            created_at: OffsetDateTime::now_utc() + time::Duration::seconds(1),
            updated_at: Some(OffsetDateTime::now_utc() + time::Duration::seconds(1)),
        })
        .expect("competing persisted intent");
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(content_root).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("competing pending intent should hydrate as conflict");

    assert!(
        recovered_owner
            .pending_writes
            .contains_key(&first_intent_id),
        "the earliest pending intent should remain repairable"
    );
    assert_eq!(
        recovered_owner
            .metadata()
            .object(&object_id)
            .expect("visible object")
            .version,
        version_1,
        "conflicting recovered intent must not publish a visible version"
    );
    let conflicts = recovered_owner
        .conflicts_for_object(&object_id)
        .expect("conflicts");
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].local_version, version_3);
    assert_eq!(
        conflicts[0].local_content_hash.as_deref(),
        Some(second_hash.as_str())
    );
    let recovered_intent = recovered_owner
        .metadata_backend()
        .expect("backend")
        .get_intent(&object_id, &format!("dfs_intent_{}_99", owner.as_str()))
        .expect("intent")
        .expect("intent");
    assert_eq!(recovered_intent.status, DfsWriteIntentStatus::Conflict);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_repairs_visible_target_pending_intent() {
    let root = temp_root("hydrate-repairs-visible-target-pending");
    let owner = NodeId::from_legacy_named("node_hydrate_visible_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_visible_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_visible").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"visible target already committed";
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let content_root = root.join("owner-content");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(&content_root).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let manifest = owner_dfs
        .content()
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("content manifest");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_2.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.content_hash = Some(manifest.content_hash.clone());
    metadata.content_size_bytes = Some(manifest.size_bytes);
    metadata.current_locations = vec![owner.clone(), replica.clone()];
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("visible object");
    let intent_id = format!("dfs_intent_{}_42", owner.as_str());
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_intent(&DfsWriteIntentRecord {
            intent_id: intent_id.clone(),
            file_system_id: sifs_file_system_id(),
            object_id: object_id.clone(),
            base_version: Some(version_1),
            target_version: version_2.clone(),
            coordinator_node_id: owner.clone(),
            owner_epoch: 0,
            desired_replica_node_ids: vec![owner.clone(), replica.clone()],
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some(manifest.content_hash.clone()),
            object_metadata: None,
            created_at: OffsetDateTime::now_utc(),
            updated_at: Some(OffsetDateTime::now_utc()),
        })
        .expect("stale pending intent");
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                committed_version: version_2.clone(),
                owner_node_id: owner.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: owner.clone(),
            content_hash: Some(manifest.content_hash.clone()),
            received_at: OffsetDateTime::now_utc(),
        })
        .expect("local receipt");
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(content_root).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("visible target stale pending intent should hydrate");

    let recovered_intent = recovered_owner
        .metadata_backend()
        .expect("backend")
        .get_intent(&object_id, &intent_id)
        .expect("intent")
        .expect("intent");
    assert_eq!(recovered_intent.status, DfsWriteIntentStatus::Committed);
    assert!(
        recovered_owner.pending_writes.contains_key(&intent_id),
        "outstanding repair work should retain pending write state after hydration"
    );
    assert_eq!(
        recovered_owner.coordinator().repair_jobs().len(),
        1,
        "unverified replica locations should remain repair work after hydration"
    );
    let recovered_receipts = recovered_owner
        .metadata_backend()
        .expect("backend")
        .receipts_for_version(&object_id, &version_2)
        .expect("receipts");
    assert!(
        recovered_receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == owner),
        "hydration should restore verified backend receipts"
    );
    assert!(
        !recovered_receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == replica),
        "hydration must not synthesize receipts for unverified remote replicas"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_skips_fully_repaired_committed_pending_write() {
    let root = temp_root("hydrate-skips-fully-repaired-committed");
    let owner = NodeId::from_legacy_named("node_hydrate_full_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_full_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_full").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    );
    let file_system = file_system(owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner fs");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica fs");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            b"fully repaired bytes".to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("pending write");
    assert!(matches!(write, NetworkedDfsWriteOutcome::Pending { .. }));
    replica_dfs.process_incoming().expect("replica stage");
    owner_dfs.process_incoming().expect("owner ack");
    replica_dfs.process_incoming().expect("replica commit");
    assert!(
        owner_dfs.pending_writes.is_empty(),
        "fully repaired committed writes should leave no local pending repair state"
    );
    assert!(owner_dfs.coordinator().repair_jobs().is_empty());
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("recovered metadata");

    assert!(
        recovered_owner.pending_writes.is_empty(),
        "fully receipted committed intents should not become pending writes after hydration"
    );
    assert!(recovered_owner.coordinator().repair_jobs().is_empty());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_restores_committed_pending_write_for_missing_repair() {
    let root = temp_root("hydrate-restores-committed-repair");
    let owner = NodeId::from_legacy_named("node_hydrate_repair_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_repair_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_repair").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id,
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            b"repair after restart bytes".to_vec(),
            vec![owner.clone(), replica],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("committed write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            repair_jobs,
            ..
        } => {
            assert_eq!(repair_jobs.len(), 1);
            intent_id
        }
        other => panic!("expected committed write with repair job, got {other:?}"),
    };
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("recovered metadata");

    assert!(
        recovered_owner.pending_writes.contains_key(&intent_id),
        "committed intents with missing replicas must keep pending state for repair"
    );
    assert_eq!(recovered_owner.coordinator().repair_jobs().len(), 1);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn operational_metrics_report_repair_backlog_and_content_footprint() {
    let root = temp_root("operational-metrics-repair-backlog");
    let owner = NodeId::from_legacy_named("node_metrics_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_metrics_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_metrics_repair").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"metrics should expose repair backlog and content footprint";
    let network = InProcessNetwork::new();
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("metadata"))
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            bytes.to_vec(),
            vec![owner.clone(), replica],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("committed write with missing repair");
    assert!(matches!(
        write,
        NetworkedDfsWriteOutcome::Committed {
            repair_jobs,
            ..
        } if repair_jobs.len() == 1
    ));

    let metrics = owner_dfs.operational_metrics().expect("metrics");
    assert_eq!(metrics.node_id, owner);
    assert!(metrics.metadata_backend_configured);
    assert_eq!(metrics.file_system_count, 1);
    assert_eq!(metrics.object_count, 1);
    assert_eq!(metrics.coordinator_intent_count, 1);
    assert_eq!(metrics.coordinator_receipt_count, 1);
    assert_eq!(metrics.coordinator_committed_version_count, 1);
    assert_eq!(metrics.repair_job_count, 1);
    assert_eq!(metrics.pending_write_count, 1);
    assert_eq!(metrics.pending_read_count, 0);
    assert_eq!(metrics.stage_retry_count, 0);
    assert_eq!(metrics.repair_retry_count, 1);
    assert_eq!(metrics.stage_inflight_count, 0);
    assert_eq!(metrics.repair_inflight_count, 0);
    assert_eq!(metrics.fetch_inflight_count, 0);
    assert_eq!(metrics.fetch_failed_source_count, 0);
    assert_eq!(metrics.stage_window_resume_count, 0);
    assert_eq!(metrics.stage_window_resume_bytes, 0);
    assert_eq!(metrics.repair_window_resume_count, 0);
    assert_eq!(metrics.repair_window_resume_bytes, 0);
    assert_eq!(metrics.fetch_window_resume_count, 0);
    assert_eq!(metrics.fetch_window_resume_bytes, 0);
    assert_eq!(metrics.pending_read_window_resume_count, 0);
    assert_eq!(metrics.pending_read_window_resume_bytes, 0);
    assert_eq!(metrics.incoming_transfer_bytes, 0);
    assert_eq!(metrics.incoming_blob_transfer_received_bytes, 0);
    assert_eq!(metrics.incoming_fetch_transfer_received_bytes, 0);
    assert_eq!(metrics.incoming_read_transfer_received_bytes, 0);
    assert_eq!(metrics.content.blob_count, 1);
    assert_eq!(metrics.content.manifest_count, 1);
    assert_eq!(metrics.content.logical_object_bytes, bytes.len() as u64);
    assert_eq!(metrics.content.staging_file_count, 0);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn operational_metrics_report_transfer_window_progress() {
    let root = temp_root("operational-metrics-transfer-window-progress");
    let owner = NodeId::from_legacy_named("node_metrics_window_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_metrics_window_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_metrics_window").expect("object");
    let target_version = DfsVersion::from_legacy_named("version_metrics_window").expect("version");
    let network = InProcessNetwork::new();
    let owner_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("owner"),
        root.join("owner-metadata"),
    )
    .expect("owner backend");
    let replica_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("replica"),
        root.join("replica-metadata"),
    )
    .expect("replica backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend");
    let file_system = file_system(owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner fs");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica fs");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let total_chunks = DFS_STAGE_CHUNK_WINDOW_SIZE + 2;
    let bytes = vec![0x5a; total_chunks as usize * DFS_BLOB_CHUNK_BYTES];
    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version,
                requesting_node_id: owner.clone(),
            },
            bytes,
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("windowed write");
    assert!(matches!(
        write,
        NetworkedDfsWriteOutcome::Pending {
            durable_receipts: 1,
            required_receipts: 2,
            ..
        }
    ));

    let first_window_bytes = u64::from(DFS_STAGE_CHUNK_WINDOW_SIZE) * DFS_BLOB_CHUNK_BYTES as u64;
    let owner_metrics = owner_dfs.operational_metrics().expect("owner metrics");
    assert_eq!(owner_metrics.stage_window_resume_count, 1);
    assert_eq!(owner_metrics.stage_window_resume_bytes, first_window_bytes);
    assert_eq!(owner_metrics.repair_window_resume_count, 0);
    assert_eq!(owner_metrics.fetch_window_resume_count, 0);
    assert_eq!(owner_metrics.pending_read_window_resume_count, 0);

    let replica_events = replica_dfs.process_incoming().expect("replica process");
    assert!(
        replica_events.is_empty(),
        "first stage window should not complete the blob"
    );
    let replica_metrics = replica_dfs.operational_metrics().expect("replica metrics");
    assert_eq!(replica_metrics.incoming_blob_transfer_count, 1);
    assert_eq!(
        replica_metrics.incoming_transfer_bytes,
        total_chunks as u64 * DFS_BLOB_CHUNK_BYTES as u64
    );
    assert_eq!(
        replica_metrics.incoming_blob_transfer_received_bytes,
        first_window_bytes
    );
    assert_eq!(replica_metrics.incoming_fetch_transfer_received_bytes, 0);
    assert_eq!(replica_metrics.incoming_read_transfer_received_bytes, 0);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_ignores_receipts_with_wrong_content_hash() {
    let root = temp_root("hydrate-ignores-wrong-hash-receipt");
    let owner = NodeId::from_legacy_named("node_hydrate_hash_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_hash_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_hash_receipt").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"receipt hash must match intent bytes";
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes.to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("committed write with missing repair");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            repair_jobs,
            ..
        } => {
            assert_eq!(repair_jobs.len(), 1);
            intent_id
        }
        other => panic!("expected committed write with repair job, got {other:?}"),
    };
    let wrong_hash = content_hash(b"receipt points at different bytes");
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                committed_version: version_2,
                owner_node_id: owner.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: replica.clone(),
            content_hash: Some(wrong_hash),
            received_at: OffsetDateTime::now_utc(),
        })
        .expect("stale wrong-hash receipt");
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("recovered metadata");

    assert!(
        recovered_owner.pending_writes.contains_key(&intent_id),
        "wrong-hash receipts must not suppress repair state after hydration"
    );
    assert_eq!(recovered_owner.coordinator().repair_jobs().len(), 1);
    assert_eq!(
        recovered_owner.coordinator().repair_jobs()[0].missing_replica_node_id,
        replica
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_ignores_unexpected_replica_receipts() {
    let root = temp_root("hydrate-ignores-unexpected-receipt");
    let owner = NodeId::from_legacy_named("node_hydrate_unexpected_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_unexpected_replica").expect("node");
    let object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_unexpected_receipt").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"unexpected receipt should not break recovery";
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes.to_vec(),
            vec![owner.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect("self-only committed write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Committed { intent_id, .. } => intent_id,
        other => panic!("expected committed write, got {other:?}"),
    };
    let content_hash = content_hash(bytes);
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                committed_version: version_2,
                owner_node_id: owner.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: replica.clone(),
            content_hash: Some(content_hash),
            received_at: OffsetDateTime::now_utc(),
        })
        .expect("unexpected stale receipt");
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("unexpected replica receipt should not break hydration");

    assert!(
        recovered_owner.pending_writes.get(&intent_id).is_none(),
        "fully repaired committed write should not be restored as pending"
    );
    assert!(recovered_owner.coordinator().repair_jobs().is_empty());
    assert!(
        recovered_owner
            .coordinator()
            .receipts_for_intent(&intent_id)
            .into_iter()
            .all(|receipt| receipt.replica_node_id != replica),
        "unexpected replica receipt must not be restored into coordinator state"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_applies_commit_ready_receipts_pending_intent() {
    let root = temp_root("hydrate-applies-commit-ready-receipts-pending");
    let owner = NodeId::from_legacy_named("node_hydrate_ready_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_ready_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_ready").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"commit ready after restart";
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes.to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("pending write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    let content_hash = content_hash(bytes);
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                committed_version: version_2.clone(),
                owner_node_id: owner.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: replica.clone(),
            content_hash: Some(content_hash.clone()),
            received_at: OffsetDateTime::now_utc(),
        })
        .expect("persist replica receipt without visible commit");
    assert_eq!(
        owner_dfs
            .metadata_backend()
            .expect("backend")
            .get_object(&object_id)
            .expect("object")
            .expect("object")
            .version,
        version_1,
        "test setup should leave visible metadata uncommitted before restart"
    );
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("commit-ready receipts-pending intent should hydrate");

    let object = recovered_owner
        .metadata_backend()
        .expect("backend")
        .get_object(&object_id)
        .expect("object")
        .expect("object");
    assert_eq!(object.version, version_2);
    assert_eq!(object.content_hash.as_deref(), Some(content_hash.as_str()));
    assert_eq!(
        object
            .current_locations
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([owner, replica])
    );
    assert!(
        recovered_owner.pending_writes.get(&intent_id).is_none(),
        "fully receipted commit-ready intent should be cleaned after visible commit recovery"
    );
    assert!(recovered_owner.coordinator().repair_jobs().is_empty());
    assert_eq!(
        recovered_owner
            .metadata_backend()
            .expect("backend")
            .get_intent(&object_id, &intent_id)
            .expect("intent")
            .expect("intent")
            .status,
        DfsWriteIntentStatus::Committed
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_does_not_mix_duplicate_intent_ids() {
    let root = temp_root("hydrate-duplicate-intent-ids");
    let owner = NodeId::from_legacy_named("node_hydrate_dup_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_dup_replica").expect("node");
    let other_replica = NodeId::from_legacy_named("node_hydrate_dup_other_replica").expect("node");
    let pending_object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_dup_pending").expect("object");
    let committed_object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_dup_committed").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let pending_version = DfsVersion::from_legacy_named("version_2").expect("version");
    let committed_version = DfsVersion::from_legacy_named("version_3").expect("version");
    let intent_id = format!("dfs_intent_{}_20", owner.as_str());
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let content_root = root.join("owner-content");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("backend");
    let file_system = file_system(owner.clone());
    backend.put_file_system(&file_system).expect("filesystem");

    let mut pending_object = file_metadata(owner.clone());
    pending_object.object_id = pending_object_id.clone();
    pending_object.path = "/services/dfs-smoke/pending-duplicate-intent.txt".to_string();
    pending_object.version = version_1.clone();
    pending_object.policy = DfsObjectPolicy::ReplicatedEventual;
    backend.put_object(&pending_object).expect("pending object");

    let content = LocalContentStore::new(&content_root).expect("content");
    let pending_manifest = content
        .put_object_version(
            pending_object_id.as_str(),
            pending_version.as_str(),
            b"pending duplicate intent bytes",
        )
        .expect("pending manifest");
    backend
        .put_intent(&DfsWriteIntentRecord {
            intent_id: intent_id.clone(),
            file_system_id: sifs_file_system_id(),
            object_id: pending_object_id.clone(),
            base_version: Some(version_1.clone()),
            target_version: pending_version.clone(),
            coordinator_node_id: owner.clone(),
            owner_epoch: 0,
            desired_replica_node_ids: vec![owner.clone(), replica.clone()],
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some(pending_manifest.content_hash.clone()),
            object_metadata: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        })
        .expect("pending intent");
    for node_id in [&owner, &replica] {
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: sifs_file_system_id(),
                    object_id: pending_object_id.clone(),
                    committed_version: pending_version.clone(),
                    owner_node_id: owner.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: node_id.clone(),
                content_hash: Some(pending_manifest.content_hash.clone()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("pending receipt");
    }

    let mut committed_object = file_metadata(owner.clone());
    committed_object.object_id = committed_object_id.clone();
    committed_object.path = "/services/dfs-smoke/committed-duplicate-intent.txt".to_string();
    committed_object.version = committed_version.clone();
    committed_object.policy = DfsObjectPolicy::ReplicatedEventual;
    committed_object.current_locations = vec![owner.clone(), other_replica.clone()];
    committed_object.content_hash = Some("blake3:committed-duplicate".to_string());
    committed_object.content_size_bytes = Some(3);
    backend
        .put_object(&committed_object)
        .expect("committed object");
    backend
        .put_version(&object_version_record(&committed_object, Some(3)))
        .expect("committed version");
    backend
        .put_intent(&DfsWriteIntentRecord {
            intent_id: intent_id.clone(),
            file_system_id: sifs_file_system_id(),
            object_id: committed_object_id.clone(),
            base_version: Some(version_1),
            target_version: committed_version,
            coordinator_node_id: owner.clone(),
            owner_epoch: 0,
            desired_replica_node_ids: vec![owner.clone(), other_replica.clone()],
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            status: DfsWriteIntentStatus::Committed,
            content_hash: committed_object.content_hash.clone(),
            object_metadata: None,
            created_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            updated_at: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1)),
        })
        .expect("committed duplicate intent");
    for node_id in [&owner, &other_replica] {
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: sifs_file_system_id(),
                    object_id: committed_object_id.clone(),
                    committed_version: committed_object.version.clone(),
                    owner_node_id: owner.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: node_id.clone(),
                content_hash: committed_object.content_hash.clone(),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("committed receipt");
    }
    drop(backend);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(content_root).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("duplicate intent ids should not poison hydration");

    let pending_object = recovered_owner
        .metadata_backend()
        .expect("backend")
        .get_object(&pending_object_id)
        .expect("pending object")
        .expect("pending object");
    assert_eq!(pending_object.version, pending_version);
    assert_eq!(
        pending_object
            .current_locations
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([owner, replica])
    );
    assert!(recovered_owner.pending_writes.get(&intent_id).is_none());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_restores_staging_pending_write_size_from_local_manifest() {
    let root = temp_root("hydrate-staging-size-from-manifest");
    let owner = NodeId::from_legacy_named("node_hydrate_staging_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_staging_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_staging_size").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"staging bytes survive restart";
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let content_root = root.join("owner-content");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(&content_root).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes.to_vec(),
            vec![owner.clone(), replica],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("staging write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write before replica receipt, got {other:?}"),
    };
    assert!(
        owner_dfs
            .metadata_backend()
            .expect("backend")
            .get_version(&object_id, &version_2)
            .expect("version lookup")
            .is_none(),
        "staging writes should not need a visible version record before commit"
    );
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(content_root).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("recovered metadata");

    assert_eq!(
        recovered_owner
            .pending_writes
            .get(&intent_id)
            .expect("restored pending write")
            .size_bytes,
        bytes.len() as u64
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn pending_write_size_hint_ignores_backend_version_with_wrong_hash() {
    let root = temp_root("pending-size-ignores-wrong-hash-version");
    let owner = NodeId::from_legacy_named("node_size_hint_owner").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_size_hint_wrong_hash").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"correct pending content size";
    let correct_hash = content_hash(bytes);
    let wrong_hash = content_hash(b"wrong content");
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut object = file_metadata(owner);
    object.object_id = object_id.clone();
    object.version = version.clone();
    object.content_hash = Some(wrong_hash.clone());
    object.content_size_bytes = Some(999);
    dfs.metadata_backend
        .as_mut()
        .expect("backend")
        .put_version(&object_version_record(&object, Some(999)))
        .expect("wrong hash version");
    dfs.content
        .put_object_version(object_id.as_str(), version.as_str(), bytes)
        .expect("correct local manifest");

    assert_eq!(
        dfs.pending_write_size_hint(&object_id, &version, &correct_hash)
            .expect("size hint"),
        Some(bytes.len() as u64)
    );
    assert_eq!(
        dfs.pending_write_size_hint(&object_id, &version, &wrong_hash)
            .expect("wrong-hash size hint"),
        Some(999)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn abort_pending_write_clears_inflight_fetch_transfers() {
    let root = temp_root("abort-clears-inflight-fetch-transfers");
    let owner = NodeId::from_legacy_named("node_abort_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_abort_replica").expect("node");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    let _replica_transport = network.add_node(replica.clone());
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_abort_fetch").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let mut metadata = file_metadata(owner.clone());
    metadata.file_system_id = file_system_id.clone();
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_file_system(DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    })
    .expect("filesystem");
    dfs.add_object_metadata(metadata).expect("object");

    let write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner,
            },
            b"pending bytes".to_vec(),
            vec![dfs.node_id().clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("pending write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    let request_id = "dfs_net_req_fetch_abort".to_string();
    let blob_request_id = "dfs_net_req_blob_abort".to_string();
    let fetch_hash = content_hash(b"repair");
    dfs.pending_writes
        .get_mut(&intent_id)
        .expect("pending")
        .fetch_inflight_request_ids
        .insert(replica.clone(), request_id.clone());
    let (_, fetch_assembler) = test_chunked_assembler(
        &root,
        "abort-fetch-transfer",
        &object_id,
        &version_2,
        &fetch_hash,
        b"repair".len() as u64,
        1,
    );
    dfs.incoming_fetch_transfers.insert(
        request_id.clone(),
        InProgressFetchBlobTransfer::new(
            object_id.clone(),
            version_2.clone(),
            fetch_hash.clone(),
            b"repair".len() as u64,
            1,
            fetch_assembler,
        )
        .expect("fetch transfer"),
    );
    let unrelated_intent_id = "dfs_intent_unrelated_abort".to_string();
    let unrelated_blob_request_id = "dfs_net_req_blob_unrelated".to_string();
    let (_, blob_assembler) = test_chunked_assembler(
        &root,
        "abort-blob-transfer",
        &object_id,
        &version_2,
        &fetch_hash,
        b"repair".len() as u64,
        1,
    );
    dfs.incoming_blob_transfers.insert(
        blob_transfer_key("stage", &replica, &intent_id, &blob_request_id),
        InProgressBlobTransfer::new(
            blob_request_id.clone(),
            intent_id.clone(),
            sifs_file_system_id(),
            object_id.clone(),
            None,
            version_2.clone(),
            fetch_hash.clone(),
            b"repair".len() as u64,
            1,
            blob_assembler,
        )
        .expect("blob transfer"),
    );
    let (_, unrelated_blob_assembler) = test_chunked_assembler(
        &root,
        "abort-unrelated-blob-transfer",
        &object_id,
        &version_2,
        &fetch_hash,
        b"repair".len() as u64,
        1,
    );
    dfs.incoming_blob_transfers.insert(
        blob_transfer_key(
            "stage",
            &replica,
            &unrelated_intent_id,
            &unrelated_blob_request_id,
        ),
        InProgressBlobTransfer::new(
            unrelated_blob_request_id,
            unrelated_intent_id.clone(),
            sifs_file_system_id(),
            object_id,
            None,
            version_2,
            fetch_hash,
            b"repair".len() as u64,
            1,
            unrelated_blob_assembler,
        )
        .expect("unrelated blob transfer"),
    );

    dfs.abort_pending_write(&intent_id).expect("abort");

    assert!(!dfs.pending_writes.contains_key(&intent_id));
    assert!(
        !dfs.incoming_fetch_transfers.contains_key(&request_id),
        "aborting a write must drop partial fetched-blob assemblers for that write"
    );
    assert!(
        !dfs.incoming_blob_transfers.contains_key(&blob_transfer_key(
            "stage",
            &replica,
            &intent_id,
            &blob_request_id
        )),
        "aborting a write must drop partial inbound blob assemblers for that intent"
    );
    assert!(
        dfs.incoming_blob_transfers.contains_key(&blob_transfer_key(
            "stage",
            &replica,
            &unrelated_intent_id,
            "dfs_net_req_blob_unrelated"
        )),
        "aborting one write must not drop unrelated inbound blob assemblers"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn committed_pending_write_cleanup_drops_stale_stage_inflight_state() {
    let root = temp_root("cleanup-drops-stale-stage-inflight");
    let owner = NodeId::from_legacy_named("node_cleanup_owner").expect("node");
    let stale_replica = NodeId::from_legacy_named("node_cleanup_stale_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_cleanup_stale_stage").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    let base_version = metadata.version.clone();
    dfs.add_object_metadata(metadata).expect("object");

    let content_hash = content_hash(b"cleanup bytes");
    let intent = dfs
        .coordinator
        .create_intent(
            sifs_file_system_id(),
            object_id.clone(),
            Some(base_version),
            version_2.clone(),
            dfs.node_id().clone(),
            vec![dfs.node_id().clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect("intent");
    let intent_id = intent.intent_id;
    dfs.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id,
            version: version_2,
            content_hash,
            size_bytes: b"cleanup bytes".len() as u64,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::from([(
                stale_replica,
                "dfs_net_req_stale_stage".to_string(),
            )]),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );
    dfs.coordinator
        .record_durable_receipt(&intent_id, dfs.node_id().clone())
        .expect("record receipt");
    dfs.apply_committed_intent(&intent_id)
        .expect("commit intent");
    assert!(dfs.coordinator.repair_jobs().is_empty());

    dfs.cleanup_committed_pending_write_if_repaired(&intent_id);

    assert!(
        !dfs.pending_writes.contains_key(&intent_id),
        "fully repaired committed writes must not retain stale stage in-flight state"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn apply_committed_intent_tolerates_missing_local_repair_blob() {
    let root = temp_root("apply-committed-missing-repair-blob");
    let owner = NodeId::from_legacy_named("node_apply_missing_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_apply_missing_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_apply_missing_blob").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"committed bytes missing locally";
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version_1.clone();
    dfs.add_object_metadata(metadata).expect("object");
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("manifest");
    let hex = manifest
        .content_hash
        .strip_prefix("blake3:")
        .expect("blake3 hash");
    std::fs::remove_file(root.join("content").join("blobs").join(&hex[..2]).join(hex))
        .expect("remove blob");
    let intent = dfs
        .coordinator
        .create_intent(
            sifs_file_system_id(),
            object_id.clone(),
            Some(version_1),
            version_2.clone(),
            owner.clone(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("intent");
    dfs.coordinator
        .record_durable_receipt(&intent.intent_id, owner.clone())
        .expect("commit with missing repair");
    dfs.pending_writes.insert(
        intent.intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version_2.clone(),
            content_hash: manifest.content_hash.clone(),
            size_bytes: manifest.size_bytes,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    dfs.apply_committed_intent(&intent.intent_id)
        .expect("commit applies even when eager repair bytes are missing");
    assert_eq!(dfs.coordinator.repair_jobs().len(), 1);

    let object = dfs.metadata().object(&object_id).expect("object");
    assert_eq!(object.version, version_2);
    assert_eq!(object.current_locations, vec![owner]);
    assert_eq!(dfs.coordinator.repair_jobs().len(), 1);
    let pending = dfs
        .pending_writes
        .get(&intent.intent_id)
        .expect("pending write");
    assert!(pending.repair_retries.contains_key(&replica));
    assert!(!pending.repair_inflight_request_ids.contains_key(&replica));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_blob_sources_use_backend_current_locations_over_stale_memory() {
    let root = temp_root("repair-sources-backend-current");
    let owner = NodeId::from_legacy_named("node_repair_source_owner").expect("node");
    let source = NodeId::from_legacy_named("node_repair_source_backend").expect("node");
    let missing = NodeId::from_legacy_named("node_repair_source_missing").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_source_backend").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"repair source bytes";
    let hash = content_hash(bytes);
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut backend_current = file_metadata(owner.clone());
    backend_current.object_id = object_id.clone();
    backend_current.version = version.clone();
    backend_current.policy = DfsObjectPolicy::ReplicatedEventual;
    backend_current.current_locations = vec![source.clone()];
    backend_current.content_hash = Some(hash.clone());
    backend_current.content_size_bytes = Some(bytes.len() as u64);
    dfs.add_object_metadata(backend_current.clone())
        .expect("backend object");

    let mut stale_memory = backend_current;
    stale_memory.current_locations = vec![owner.clone()];
    dfs.add_transient_object_metadata(stale_memory)
        .expect("stale memory object");
    let pending = PendingNetworkedWrite {
        object_id: object_id.clone(),
        version: version.clone(),
        content_hash: hash,
        size_bytes: bytes.len() as u64,
        patch: None,
        stage_retries: BTreeMap::new(),
        repair_retries: BTreeMap::new(),
        stage_inflight_request_ids: BTreeMap::new(),
        stage_next_chunk_indexes: BTreeMap::new(),
        repair_inflight_request_ids: BTreeMap::new(),
        repair_next_chunk_indexes: BTreeMap::new(),
        fetch_inflight_from: BTreeSet::new(),
        fetch_inflight_request_ids: BTreeMap::new(),
        fetch_inflight_started_ticks: BTreeMap::new(),
        fetch_next_chunk_indexes: BTreeMap::new(),
        fetch_window_end_chunk_indexes: BTreeMap::new(),
        fetch_failed_from: BTreeSet::new(),
    };
    let job = DistributedRepairJob {
        intent_id: "dfs_intent_repair_source_backend".to_string(),
        file_system_id: sifs_file_system_id(),
        object_id,
        version,
        missing_replica_node_id: missing,
    };

    assert_eq!(
        dfs.repair_blob_sources(&job, &pending)
            .expect("repair sources"),
        vec![source]
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn write_outcome_uses_backend_current_metadata_after_pending_state_is_removed() {
    let root = temp_root("write-outcome-backend-current");
    let owner = NodeId::from_legacy_named("node_write_outcome_owner").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_write_outcome_backend").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"write outcome backend bytes";
    let hash = content_hash(bytes);
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata.clone()).expect("object");
    let intent = dfs
        .coordinator
        .create_intent(
            sifs_file_system_id(),
            object_id.clone(),
            Some(version_1),
            version_2.clone(),
            owner.clone(),
            vec![owner.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect("intent");
    dfs.pending_writes.insert(
        intent.intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version_2.clone(),
            content_hash: hash.clone(),
            size_bytes: bytes.len() as u64,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );
    dfs.coordinator
        .record_durable_receipt(&intent.intent_id, owner.clone())
        .expect("record receipt");
    dfs.apply_committed_intent(&intent.intent_id)
        .expect("commit intent");
    let mut backend_current = metadata;
    backend_current.version = version_2.clone();
    backend_current.content_hash = Some(hash.clone());
    backend_current.content_size_bytes = Some(bytes.len() as u64);
    backend_current.current_locations = vec![owner];
    dfs.add_object_metadata(backend_current)
        .expect("backend current object");
    dfs.remove_transient_object_metadata(&object_id);
    assert!(
        dfs.metadata().object(&object_id).is_none(),
        "test requires backend-only committed metadata"
    );

    let outcome = dfs
        .write_outcome(&intent.intent_id)
        .expect("write outcome from backend metadata");
    assert!(matches!(
        outcome,
        NetworkedDfsWriteOutcome::Committed {
            version,
            content_hash,
            ..
        } if version == version_2 && content_hash == hash
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn malformed_dfs_protocol_frame_does_not_block_following_valid_frame() {
    let root = temp_root("malformed-dfs-frame-does-not-block-valid-frame");
    let requester = NodeId::from_legacy_named("node_bad_frame_requester").expect("node");
    let server = NodeId::from_legacy_named("node_bad_frame_server").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_bad_frame_read").expect("object");
    let bytes = b"valid frame after bad frame";
    let network = InProcessNetwork::new();
    let mut requester_transport = network.add_node(requester.clone());
    let mut dfs = NetworkedDfsService::new(
        network.add_node(server.clone()),
        LocalContentStore::new(root.join("server-content")).expect("server content"),
    );
    dfs.add_file_system(file_system(server.clone()))
        .expect("filesystem");
    let mut metadata = file_metadata(server.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![server.clone()];
    dfs.put_local_object_version(metadata, bytes)
        .expect("seed object");

    requester_transport
        .send(
            &server,
            NetworkFrame {
                frame_id: NetworkFrameId(1),
                source_node_id: requester.clone(),
                target_node_id: server.clone(),
                message: MeshMessage {
                    message_id: MeshMessageId(1),
                    origin_node_id: requester.clone(),
                    target_node_id: server.clone(),
                    body: MeshMessageBody::Protocol(ProtocolMessage::new(
                        DFS_PROTOCOL,
                        b"not a sinomo object payload".to_vec(),
                    )),
                },
            },
        )
        .expect("send malformed frame");
    requester_transport
        .send(
            &server,
            NetworkFrame {
                frame_id: NetworkFrameId(2),
                source_node_id: requester.clone(),
                target_node_id: server.clone(),
                message: MeshMessage {
                    message_id: MeshMessageId(2),
                    origin_node_id: requester.clone(),
                    target_node_id: server.clone(),
                    body: MeshMessageBody::Protocol(
                        ProtocolMessage::encode(
                            DFS_PROTOCOL,
                            &DfsNetworkMessage::ReadRequest(DfsReadRequest {
                                request_id: "dfs_net_req_after_bad_frame".to_string(),
                                object_id: object_id.clone(),
                                range_offset: None,
                                range_max_bytes: None,
                                start_chunk_index: 0,
                                max_chunks: 0,
                            }),
                        )
                        .expect("valid read request"),
                    ),
                },
            },
        )
        .expect("send valid frame");

    let events = dfs
        .process_incoming()
        .expect("malformed frame should be isolated");

    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadServed {
                request_id,
                object_id: served_object_id,
                result: DfsReadResult::Found(data),
                ..
            } if request_id == "dfs_net_req_after_bad_frame"
                && served_object_id == &object_id
                && data.bytes == bytes
        )
    }));
    let responses = requester_transport
        .drain_incoming()
        .expect("requester response mailbox");
    assert_eq!(responses.len(), 1);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn commit_version_applies_when_response_target_is_unroutable() {
    let root = temp_root("commit-response-target-unroutable");
    let owner = NodeId::from_legacy_named("node_commit_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_commit_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_commit_response").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let file_system_id = sifs_file_system_id();
    let bytes = b"commit response target is unavailable";
    let content_hash = content_hash(bytes);
    let content = LocalContentStore::new(root.join("content")).expect("content");
    content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("staged replica bytes");
    let mut transport = ReceiveOnlyTransport::new(replica.clone());
    let object_metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "commit-response.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone()],
        content_hash: None,
        content_size_bytes: None,
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    transport.push_message(
        owner.clone(),
        replica.clone(),
        DfsNetworkMessage::CommitVersion(DfsCommitVersionCommand {
            request_id: "dfs_net_req_commit_unroutable_response".to_string(),
            intent_id: "intent_commit_unroutable_response".to_string(),
            file_system_id,
            object_id: object_id.clone(),
            expected_base_version: Some(object_metadata.version.clone()),
            version: version_2.clone(),
            content_hash: content_hash.clone(),
            object_metadata: Some(object_metadata),
        }),
    );
    let mut dfs = NetworkedDfsService::new(transport, content);

    let events = dfs
        .process_incoming()
        .expect("commit should apply even if response cannot be routed");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::VersionCommitted {
                object_id: committed,
                version,
                content_hash: committed_hash,
                ..
            } if committed == &object_id && version == &version_2 && committed_hash == &content_hash
        )
    }));
    assert_eq!(
        dfs.read_local_object(&object_id)
            .expect("read committed object")
            .expect("committed data")
            .bytes,
        bytes
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn chunk_assemblers_ignore_identical_duplicate_chunks() {
    let root = temp_root("chunk-assemblers-ignore-duplicates");
    let object_id = DfsObjectId::from_legacy_named("obj_duplicate_chunk").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let bytes = (0..70_000)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let first_chunk = bytes[..DFS_BLOB_CHUNK_BYTES].to_vec();
    let second_chunk = bytes[DFS_BLOB_CHUNK_BYTES..].to_vec();
    let hash = content_hash(&bytes);
    let (blob_store, blob_assembler) = test_chunked_assembler(
        &root,
        "duplicate-blob-transfer",
        &object_id,
        &version,
        &hash,
        bytes.len() as u64,
        2,
    );

    let mut blob_transfer = InProgressBlobTransfer::new(
        "dfs_net_req_blob_duplicate".to_string(),
        "dfs_intent_blob_duplicate".to_string(),
        file_system_id,
        object_id.clone(),
        None,
        version.clone(),
        hash.clone(),
        bytes.len() as u64,
        2,
        blob_assembler,
    )
    .expect("blob transfer");
    assert!(
        !blob_transfer
            .insert_chunk(0, 2, first_chunk.clone())
            .expect("first blob chunk")
    );
    assert!(
        !blob_transfer
            .insert_chunk(0, 2, first_chunk.clone())
            .expect("duplicate blob chunk")
    );
    assert!(
        blob_transfer
            .insert_chunk(1, 2, second_chunk.clone())
            .expect("second blob chunk")
    );
    let manifest = blob_store
        .commit_chunked_object_version(blob_transfer.assembler)
        .expect("commit assembled blob");
    assert_eq!(
        blob_store
            .read_blob(&manifest.content_hash)
            .expect("assembled blob"),
        bytes
    );

    let (read_store, read_assembler) = test_chunked_assembler(
        &root,
        "duplicate-read-transfer",
        &object_id,
        &version,
        &hash,
        bytes.len() as u64,
        2,
    );
    let mut read_transfer = InProgressReadTransfer::new(
        object_id.clone(),
        version.clone(),
        hash.clone(),
        bytes.len() as u64,
        2,
        read_assembler,
    )
    .expect("read transfer");
    assert!(
        !read_transfer
            .insert_chunk(0, 2, first_chunk.clone())
            .expect("first read chunk")
    );
    assert!(
        !read_transfer
            .insert_chunk(0, 2, first_chunk.clone())
            .expect("duplicate read chunk")
    );
    assert!(
        read_transfer
            .insert_chunk(1, 2, second_chunk.clone())
            .expect("second read chunk")
    );
    let manifest = read_store
        .commit_chunked_object_version(read_transfer.assembler)
        .expect("commit assembled read");
    assert_eq!(
        read_store
            .read_blob(&manifest.content_hash)
            .expect("assembled read"),
        bytes
    );

    let (fetch_store, fetch_assembler) = test_chunked_assembler(
        &root,
        "duplicate-fetch-transfer",
        &object_id,
        &version,
        &hash,
        bytes.len() as u64,
        2,
    );
    let mut fetch_transfer = InProgressFetchBlobTransfer::new(
        object_id,
        version,
        hash,
        bytes.len() as u64,
        2,
        fetch_assembler,
    )
    .expect("fetch transfer");
    assert!(
        !fetch_transfer
            .insert_chunk(0, 2, first_chunk.clone())
            .expect("first fetch chunk")
    );
    assert!(
        !fetch_transfer
            .insert_chunk(0, 2, first_chunk)
            .expect("duplicate fetch chunk")
    );
    assert!(
        fetch_transfer
            .insert_chunk(1, 2, second_chunk)
            .expect("second fetch chunk")
    );
    let manifest = fetch_store
        .commit_chunked_object_version(fetch_transfer.assembler)
        .expect("commit assembled fetch");
    assert_eq!(
        fetch_store
            .read_blob(&manifest.content_hash)
            .expect("assembled fetch"),
        bytes
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn chunk_assemblers_reject_conflicting_duplicate_chunks() {
    let root = temp_root("conflicting-duplicate-read-chunk");
    let object_id =
        DfsObjectId::from_legacy_named("obj_conflicting_duplicate_chunk").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = (0..70_000)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let mut first_chunk = bytes[..DFS_BLOB_CHUNK_BYTES].to_vec();
    let mut conflicting_chunk = first_chunk.clone();
    conflicting_chunk[0] ^= 0xff;
    let hash = content_hash(&bytes);

    let (_, read_assembler) = test_chunked_assembler(
        &root,
        "conflicting-read-transfer",
        &object_id,
        &version,
        &hash,
        bytes.len() as u64,
        2,
    );
    let mut read_transfer = InProgressReadTransfer::new(
        object_id,
        version,
        hash,
        bytes.len() as u64,
        2,
        read_assembler,
    )
    .expect("read transfer");
    read_transfer
        .insert_chunk(0, 2, std::mem::take(&mut first_chunk))
        .expect("first read chunk");
    let error = read_transfer
        .insert_chunk(0, 2, conflicting_chunk)
        .expect_err("conflicting duplicate chunk is rejected");
    assert!(
        error
            .to_string()
            .contains("conflicting duplicate content chunk"),
        "{error}"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stale_pending_remote_read_can_be_retried() {
    let root = temp_root("remote-read-stale-pending-retry");
    let owner = NodeId::from_legacy_named("node_read_retry_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_read_retry_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut owner_transport = network.add_node(owner.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut object = file_metadata(owner.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    object.content_hash = Some(content_hash(b"remote retry bytes"));
    object.content_size_bytes = Some(b"remote retry bytes".len() as u64);
    let object_id = object.object_id.clone();
    reader_dfs.add_object_metadata(object).expect("object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("initial read request"),
        vec![owner.clone()]
    );
    assert_eq!(
        owner_transport
            .drain_incoming()
            .expect("initial owner mailbox")
            .len(),
        1
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("active pending read is deduplicated"),
        vec![owner.clone()]
    );
    assert!(
        owner_transport
            .drain_incoming()
            .expect("deduped owner mailbox")
            .is_empty()
    );

    for pending in reader_dfs.pending_reads.values_mut() {
        pending.next_retry_at = Instant::now() - Duration::from_millis(1);
    }
    assert!(
        !reader_dfs
            .has_pending_remote_read_for_current_object(&object_id)
            .expect("expired pending read is not active")
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("retry stale pending read"),
        vec![owner.clone()]
    );
    assert_eq!(
        owner_transport
            .drain_incoming()
            .expect("retried owner mailbox")
            .len(),
        1
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn chunked_remote_read_retry_resumes_from_received_prefix() {
    let root = temp_root("chunked-remote-read-retry-resumes-prefix");
    let owner = NodeId::from_legacy_named("node_read_resume_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_read_resume_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner")).expect("owner content"),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    let file_system = file_system(owner.clone());
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");

    let mut object = file_metadata(owner.clone());
    object.object_id = DfsObjectId::from_legacy_named("obj_read_resume_prefix").expect("object");
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    let object_id = object.object_id.clone();
    let total_chunks = DFS_READ_CHUNK_WINDOW_SIZE + 1;
    let bytes_len = DFS_BLOB_CHUNK_BYTES * (total_chunks as usize - 1) + 17;
    let bytes = (0..bytes_len)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    owner_dfs
        .put_local_object_version(object, &bytes)
        .expect("seed owner object");
    reader_dfs
        .add_object_metadata(
            owner_dfs
                .metadata()
                .object(&object_id)
                .expect("owner metadata")
                .clone(),
        )
        .expect("replicate metadata");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("initial read request"),
        vec![owner.clone()]
    );
    owner_dfs
        .process_incoming()
        .expect("owner sends first read window");
    let mut first_window = drain_dfs_messages(&mut reader_dfs.transport)
        .into_iter()
        .filter_map(|message| match message {
            DfsNetworkMessage::ReadResponseChunk(chunk) => Some(chunk),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(first_window.len(), DFS_READ_CHUNK_WINDOW_SIZE as usize);
    let first_chunk = first_window.remove(0);
    assert_eq!(first_chunk.chunk_index, 0);
    let request_id = first_chunk.request_id.clone();
    assert!(
        reader_dfs
            .handle_networked_read_response_chunk(owner.clone(), first_chunk)
            .expect("reader accepts first chunk")
            .is_none()
    );
    assert_eq!(
        reader_dfs
            .pending_reads
            .get(&request_id)
            .expect("pending read")
            .next_chunk_index,
        1
    );

    reader_dfs
        .pending_reads
        .get_mut(&request_id)
        .expect("pending read")
        .next_retry_at = Instant::now() - Duration::from_millis(1);
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("retry read request"),
        vec![owner.clone()]
    );
    let retry_request = drain_dfs_messages(&mut owner_dfs.transport)
        .into_iter()
        .find_map(|message| match message {
            DfsNetworkMessage::ReadRequest(request) => Some(request),
            _ => None,
        })
        .expect("retry read request");
    assert_eq!(retry_request.request_id, request_id);
    assert_eq!(retry_request.start_chunk_index, 1);
    assert_eq!(retry_request.max_chunks, DFS_READ_CHUNK_WINDOW_SIZE);

    owner_dfs
        .handle_networked_read_request(reader.clone(), retry_request)
        .expect("owner sends resumed read chunks");
    let completed = reader_dfs
        .process_incoming()
        .expect("reader completes resumed read");
    assert!(completed.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed_object_id,
                result: DfsReadResult::Found(DfsReadData { bytes, .. }),
                ..
            } if completed_object_id == &object_id && bytes.is_empty()
        )
    }));
    assert!(matches!(
        reader_dfs
            .read_local_object_with_state(&object_id, false)
            .expect("resumed read cached"),
        NetworkedDfsReadOutcome::Found { data, .. } if data.bytes == bytes
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn process_incoming_prunes_expired_pending_remote_reads() {
    let root = temp_root("remote-read-process-prunes-expired");
    let owner = NodeId::from_legacy_named("node_read_prune_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_read_prune_reader").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_read_prune").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let request_id = "dfs_net_req_read_prune".to_string();
    let hash = content_hash(b"expired read bytes");
    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(reader.clone()),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    dfs.pending_reads.insert(
        request_id.clone(),
        PendingNetworkedRead {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some(hash.clone()),
            content_size_bytes: Some(70_000),
            range_offset: None,
            range_max_bytes: None,
            next_chunk_index: 0,
            window_end_chunk_index: None,
            target_node_id: owner,
            next_retry_at: Instant::now()
                - DFS_CHUNK_TRANSFER_IDLE_TIMEOUT
                - Duration::from_millis(1),
        },
    );
    dfs.incoming_read_transfers.insert(request_id.clone(), {
        let (_, assembler) = test_chunked_assembler(
            &root,
            "expired-read-transfer",
            &object_id,
            &version,
            &hash,
            70_000,
            2,
        );
        InProgressReadTransfer {
            object_id,
            version,
            content_hash: hash,
            total_size_bytes: 70_000,
            last_updated_at: Instant::now(),
            assembler,
        }
    });

    assert!(
        dfs.process_incoming()
            .expect("process incoming without frames")
            .is_empty()
    );

    assert!(!dfs.pending_reads.contains_key(&request_id));
    assert!(!dfs.incoming_read_transfers.contains_key(&request_id));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn process_incoming_accepts_queued_remote_read_response_before_expiry_prune() {
    let root = temp_root("remote-read-process-accepts-queued-expired");
    let owner = NodeId::from_legacy_named("node_read_late_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_read_late_reader").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_read_late").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let request_id = "dfs_net_req_read_late".to_string();
    let bytes = b"late but queued read bytes".to_vec();
    let hash = content_hash(&bytes);
    let mut transport = ReceiveOnlyTransport::new(reader.clone());
    transport.push_message(
        owner.clone(),
        reader.clone(),
        DfsNetworkMessage::ReadResponse(DfsReadResponse {
            request_id: request_id.clone(),
            object_id: object_id.clone(),
            result: DfsReadResult::Found(DfsReadData {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some(hash.clone()),
                bytes: bytes.clone(),
            }),
        }),
    );
    let mut dfs = NetworkedDfsService::new(
        transport,
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    dfs.add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    object.content_hash = Some(hash.clone());
    object.content_size_bytes = Some(bytes.len() as u64);
    object.version = version.clone();
    dfs.add_object_metadata(object).expect("object");
    dfs.pending_reads.insert(
        request_id.clone(),
        PendingNetworkedRead {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some(hash.clone()),
            content_size_bytes: Some(bytes.len() as u64),
            range_offset: None,
            range_max_bytes: None,
            next_chunk_index: 0,
            window_end_chunk_index: None,
            target_node_id: owner.clone(),
            next_retry_at: Instant::now()
                - DFS_CHUNK_TRANSFER_IDLE_TIMEOUT
                - Duration::from_millis(1),
        },
    );

    let events = dfs.process_incoming().expect("process queued response");

    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: event_request_id,
                object_id: event_object_id,
                source_node_id,
                result: DfsReadResult::Found(_),
            } if event_request_id == &request_id
                && event_object_id == &object_id
                && source_node_id == &owner
        )
    }));
    assert!(!dfs.pending_reads.contains_key(&request_id));
    assert_eq!(
        dfs.read_local_object(&object_id)
            .expect("local read")
            .expect("cached data")
            .bytes,
        bytes
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn remote_read_planning_deduplicates_duplicate_locations() {
    let root = temp_root("remote-read-dedupes-duplicate-locations");
    let owner = NodeId::from_legacy_named("node_read_dedupe_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_read_dedupe_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut owner_transport = network.add_node(owner.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut object = file_metadata(owner.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone(), owner.clone()];
    object.content_hash = Some(content_hash(b"duplicate location bytes"));
    object.content_size_bytes = Some(b"duplicate location bytes".len() as u64);
    let object_id = object.object_id.clone();
    reader_dfs.add_object_metadata(object).expect("object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("deduped read request"),
        vec![owner.clone()]
    );
    assert_eq!(
        owner_transport
            .drain_incoming()
            .expect("owner mailbox")
            .len(),
        1
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn remote_read_planning_uses_single_active_source_and_falls_back_after_miss() {
    let root = temp_root("remote-read-single-active-fallback");
    let owner_a = NodeId::from_legacy_named("node_read_source_a").expect("node");
    let owner_b = NodeId::from_legacy_named("node_read_source_b").expect("node");
    let reader = NodeId::from_legacy_named("node_read_single_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut owner_a_transport = network.add_node(owner_a.clone());
    let mut owner_b_transport = network.add_node(owner_b.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(owner_a.clone()))
        .expect("filesystem");
    let mut object = file_metadata(owner_a.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner_a.clone(), owner_b.clone()];
    object.content_hash = Some(content_hash(b"single active read bytes"));
    object.content_size_bytes = Some(b"single active read bytes".len() as u64);
    let object_id = object.object_id.clone();
    reader_dfs.add_object_metadata(object).expect("object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("first read request"),
        vec![owner_a.clone()]
    );
    assert_eq!(
        owner_a_transport
            .drain_incoming()
            .expect("source a mailbox")
            .len(),
        1
    );
    assert!(
        owner_b_transport
            .drain_incoming()
            .expect("source b mailbox")
            .is_empty(),
        "remote reads should not fan out to every replica when one source is active"
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("active request is reused"),
        vec![owner_a.clone()]
    );
    let request_id = reader_dfs
        .pending_reads
        .iter()
        .find_map(|(request_id, pending)| {
            (pending.target_node_id == owner_a).then_some(request_id.clone())
        })
        .expect("pending source a read");
    reader_dfs
        .handle_networked_read_response(
            owner_a.clone(),
            DfsReadResponse {
                request_id,
                object_id: object_id.clone(),
                result: DfsReadResult::NotFound,
            },
        )
        .expect("source a miss is handled");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("fallback request after miss"),
        vec![owner_b.clone()]
    );
    assert_eq!(
        owner_b_transport
            .drain_incoming()
            .expect("source b fallback mailbox")
            .len(),
        1
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn pending_remote_read_to_removed_location_does_not_block_fallback() {
    let root = temp_root("remote-read-removed-location-not-active");
    let source_a = NodeId::from_legacy_named("node_read_removed_a").expect("node");
    let source_b = NodeId::from_legacy_named("node_read_removed_b").expect("node");
    let reader = NodeId::from_legacy_named("node_read_removed_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut source_a_transport = network.add_node(source_a.clone());
    let mut source_b_transport = network.add_node(source_b.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(source_a.clone()))
        .expect("filesystem");
    let mut object = file_metadata(source_a.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source_a.clone(), source_b.clone()];
    object.content_hash = Some(content_hash(b"removed location bytes"));
    object.content_size_bytes = Some(b"removed location bytes".len() as u64);
    let object_id = object.object_id.clone();
    reader_dfs
        .add_object_metadata(object.clone())
        .expect("object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("first read request"),
        vec![source_a.clone()]
    );
    assert_eq!(
        source_a_transport
            .drain_incoming()
            .expect("source a mailbox")
            .len(),
        1
    );
    let source_a_request_id = reader_dfs
        .pending_reads
        .iter()
        .find_map(|(request_id, pending)| {
            (pending.target_node_id == source_a).then(|| request_id.clone())
        })
        .expect("source a pending request");
    let transfer_hash = object.content_hash.clone().expect("content hash");
    let transfer_size = object.content_size_bytes.expect("size");
    reader_dfs
        .incoming_read_transfers
        .insert(source_a_request_id.clone(), {
            let (_, assembler) = test_chunked_assembler(
                &root,
                "removed-location-read-transfer",
                &object_id,
                &object.version,
                &transfer_hash,
                transfer_size,
                1,
            );
            InProgressReadTransfer {
                object_id: object_id.clone(),
                version: object.version.clone(),
                content_hash: transfer_hash,
                total_size_bytes: transfer_size,
                last_updated_at: Instant::now(),
                assembler,
            }
        });
    object.current_locations = vec![source_b.clone()];
    reader_dfs
        .add_object_metadata(object)
        .expect("advance object");

    assert!(
        !reader_dfs
            .has_pending_remote_read_for_current_object(&object_id)
            .expect("removed source should not count as active pending read")
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("fallback request after location removal"),
        vec![source_b.clone()]
    );
    assert!(
        !reader_dfs.pending_reads.contains_key(&source_a_request_id),
        "removed source pending read should be pruned when planning fallback"
    );
    assert!(
        !reader_dfs
            .incoming_read_transfers
            .contains_key(&source_a_request_id),
        "removed source partial transfer should be pruned with its pending read"
    );
    assert_eq!(
        source_b_transport
            .drain_incoming()
            .expect("source b mailbox")
            .len(),
        1
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn expired_remote_read_attempt_rotates_to_next_location_before_retrying_same_source() {
    let root = temp_root("remote-read-expired-rotates");
    let source_a = NodeId::from_legacy_named("node_read_expired_a").expect("node");
    let source_b = NodeId::from_legacy_named("node_read_expired_b").expect("node");
    let reader = NodeId::from_legacy_named("node_read_expired_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut source_a_transport = network.add_node(source_a.clone());
    let mut source_b_transport = network.add_node(source_b.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(source_a.clone()))
        .expect("filesystem");
    let mut object = file_metadata(source_a.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source_a.clone(), source_b.clone()];
    object.content_hash = Some(content_hash(b"expired rotate bytes"));
    object.content_size_bytes = Some(b"expired rotate bytes".len() as u64);
    let object_id = object.object_id.clone();
    reader_dfs.add_object_metadata(object).expect("object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("first request"),
        vec![source_a.clone()]
    );
    assert_eq!(
        source_a_transport
            .drain_incoming()
            .expect("source a first mailbox")
            .len(),
        1
    );
    for pending in reader_dfs.pending_reads.values_mut() {
        pending.next_retry_at = Instant::now() - Duration::from_millis(1);
    }

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("rotated request"),
        vec![source_b.clone()]
    );
    assert!(
        source_a_transport
            .drain_incoming()
            .expect("source a retry mailbox")
            .is_empty(),
        "expired read should try a later replica before retrying the same silent source"
    );
    assert_eq!(
        source_b_transport
            .drain_incoming()
            .expect("source b mailbox")
            .len(),
        1
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn late_remote_read_from_removed_location_does_not_clear_fallback_request() {
    let root = temp_root("remote-read-late-removed-location");
    let source_a = NodeId::from_legacy_named("node_read_late_a").expect("node");
    let source_b = NodeId::from_legacy_named("node_read_late_b").expect("node");
    let reader = NodeId::from_legacy_named("node_read_late_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut source_a_transport = network.add_node(source_a.clone());
    let mut source_b_transport = network.add_node(source_b.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(source_a.clone()))
        .expect("filesystem");
    let bytes = b"late removed source bytes";
    let mut object = file_metadata(source_a.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source_a.clone(), source_b.clone()];
    object.content_hash = Some(content_hash(bytes));
    object.content_size_bytes = Some(bytes.len() as u64);
    let object_id = object.object_id.clone();
    let version = object.version.clone();
    let content_hash = object.content_hash.clone();
    reader_dfs
        .add_object_metadata(object.clone())
        .expect("object");

    reader_dfs
        .request_remote_object_read(&object_id)
        .expect("request source a");
    assert_eq!(
        source_a_transport
            .drain_incoming()
            .expect("source a mailbox")
            .len(),
        1
    );
    let source_a_request_id = reader_dfs
        .pending_reads
        .iter()
        .find_map(|(request_id, pending)| {
            (pending.target_node_id == source_a).then_some(request_id.clone())
        })
        .expect("source a request");
    object.current_locations = vec![source_b.clone()];
    reader_dfs
        .add_object_metadata(object)
        .expect("advance object");
    reader_dfs
        .request_remote_object_read(&object_id)
        .expect("fallback source b");
    assert_eq!(
        source_b_transport
            .drain_incoming()
            .expect("source b mailbox")
            .len(),
        1
    );
    let source_b_request_id = reader_dfs
        .pending_reads
        .iter()
        .find_map(|(request_id, pending)| {
            (pending.target_node_id == source_b).then_some(request_id.clone())
        })
        .expect("source b request");
    assert!(
        !reader_dfs.pending_reads.contains_key(&source_a_request_id),
        "fallback planning should prune the stale source a request"
    );

    let event = reader_dfs
        .handle_networked_read_response(
            source_a.clone(),
            DfsReadResponse {
                request_id: source_a_request_id,
                object_id: object_id.clone(),
                result: DfsReadResult::Found(DfsReadData {
                    object_id: object_id.clone(),
                    version,
                    content_hash,
                    bytes: bytes.to_vec(),
                }),
            },
        )
        .expect("late source a response");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            source_node_id,
            result: DfsReadResult::Rejected { reason },
            ..
        } if source_node_id == source_a && reason.contains("unknown DFS read request")
    ));
    assert!(
        reader_dfs.pending_reads.contains_key(&source_b_request_id),
        "late response from removed source must not clear the fallback request"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn remote_read_rejection_from_stale_location_falls_back_to_next_source() {
    let root = temp_root("remote-read-rejection-fallback");
    let source_a = NodeId::from_legacy_named("node_read_reject_a").expect("node");
    let source_b = NodeId::from_legacy_named("node_read_reject_b").expect("node");
    let reader = NodeId::from_legacy_named("node_read_reject_reader").expect("node");
    let network = InProcessNetwork::new();
    let mut source_a_transport = network.add_node(source_a.clone());
    let mut source_b_transport = network.add_node(source_b.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("reader"), root.join("meta"))
            .expect("metadata backend"),
    )
    .expect("metadata backend");
    reader_dfs
        .add_file_system(file_system(source_a.clone()))
        .expect("filesystem");
    let mut object = file_metadata(source_a.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source_a.clone(), source_b.clone()];
    object.content_hash = Some(content_hash(b"stale rejection bytes"));
    object.content_size_bytes = Some(b"stale rejection bytes".len() as u64);
    let object_id = object.object_id.clone();
    let version = object.version.clone();
    let hash = object.content_hash.clone();
    reader_dfs.add_object_metadata(object).expect("object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("first read request"),
        vec![source_a.clone()]
    );
    assert_eq!(
        source_a_transport
            .drain_incoming()
            .expect("source a mailbox")
            .len(),
        1
    );
    let request_id = reader_dfs
        .pending_reads
        .iter()
        .find_map(|(request_id, pending)| {
            (pending.target_node_id == source_a).then_some(request_id.clone())
        })
        .expect("pending source a read");
    reader_dfs
        .handle_networked_read_response(
            source_a.clone(),
            DfsReadResponse {
                request_id,
                object_id: object_id.clone(),
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "node {source_a} is not a current location for DFS object {object_id}"
                    ),
                },
            },
        )
        .expect("stale-location rejection handled");

    assert_eq!(
        reader_dfs
            .current_read_object_metadata(&object_id)
            .expect("current metadata")
            .current_locations,
        vec![source_b.clone()]
    );
    assert_eq!(
        reader_dfs
            .metadata_backend
            .as_ref()
            .expect("backend")
            .get_version(&object_id, &version)
            .expect("version")
            .expect("version")
            .locations,
        vec![source_b.clone()]
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("fallback after stale-location rejection"),
        vec![source_b.clone()]
    );
    assert_eq!(
        source_b_transport
            .drain_incoming()
            .expect("source b mailbox")
            .len(),
        1
    );
    assert_eq!(
        hash,
        reader_dfs
            .current_read_object_metadata(&object_id)
            .expect("current metadata after fallback")
            .content_hash
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn inline_remote_read_wrong_object_is_isolated_and_falls_back() {
    let root = temp_root("remote-read-wrong-object-isolated");
    let source_a = NodeId::from_legacy_named("node_read_wrong_object_a").expect("node");
    let source_b = NodeId::from_legacy_named("node_read_wrong_object_b").expect("node");
    let reader = NodeId::from_legacy_named("node_read_wrong_object_reader").expect("node");
    let wrong_object_id =
        DfsObjectId::from_legacy_named("obj_wrong_read_response").expect("object");
    let network = InProcessNetwork::new();
    let mut source_a_transport = network.add_node(source_a.clone());
    let mut source_b_transport = network.add_node(source_b.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("reader"), root.join("meta"))
            .expect("metadata backend"),
    )
    .expect("metadata backend");
    reader_dfs
        .add_file_system(file_system(source_a.clone()))
        .expect("filesystem");
    let mut object = file_metadata(source_a.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source_a.clone(), source_b.clone()];
    object.content_hash = Some(content_hash(b"right remote bytes"));
    object.content_size_bytes = Some(b"right remote bytes".len() as u64);
    let object_id = object.object_id.clone();
    let version = object.version.clone();
    let hash = object.content_hash.clone();
    reader_dfs.add_object_metadata(object).expect("object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("first read request"),
        vec![source_a.clone()]
    );
    assert_eq!(
        source_a_transport
            .drain_incoming()
            .expect("source a mailbox")
            .len(),
        1
    );
    let request_id = reader_dfs
        .pending_reads
        .iter()
        .find_map(|(request_id, pending)| {
            (pending.target_node_id == source_a).then_some(request_id.clone())
        })
        .expect("pending source a read");

    let event = reader_dfs
        .handle_networked_read_response(
            source_a.clone(),
            DfsReadResponse {
                request_id: request_id.clone(),
                object_id: wrong_object_id.clone(),
                result: DfsReadResult::Found(DfsReadData {
                    object_id: wrong_object_id,
                    version: version.clone(),
                    content_hash: hash.clone(),
                    bytes: b"right remote bytes".to_vec(),
                }),
            },
        )
        .expect("wrong-object response is isolated");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: completed_request_id,
            object_id: completed_object_id,
            source_node_id,
            result: DfsReadResult::Rejected { reason },
        } if completed_request_id == request_id
            && completed_object_id != object_id
            && source_node_id == source_a
            && reason.contains("unexpected DFS read response object")
    ));
    assert!(!reader_dfs.pending_reads.contains_key(&request_id));
    assert_eq!(
        reader_dfs
            .current_read_object_metadata(&object_id)
            .expect("current metadata")
            .current_locations,
        vec![source_b.clone()]
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("fallback after wrong-object response"),
        vec![source_b.clone()]
    );
    assert_eq!(
        source_b_transport
            .drain_incoming()
            .expect("source b mailbox")
            .len(),
        1
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn inline_remote_read_wrong_payload_object_is_isolated_and_falls_back() {
    let root = temp_root("remote-read-wrong-payload-object-isolated");
    let source_a = NodeId::from_legacy_named("node_read_wrong_payload_a").expect("node");
    let source_b = NodeId::from_legacy_named("node_read_wrong_payload_b").expect("node");
    let reader = NodeId::from_legacy_named("node_read_wrong_payload_reader").expect("node");
    let wrong_object_id = DfsObjectId::from_legacy_named("obj_wrong_read_payload").expect("object");
    let network = InProcessNetwork::new();
    let mut source_a_transport = network.add_node(source_a.clone());
    let mut source_b_transport = network.add_node(source_b.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(root.join("reader")).expect("reader content"),
    );
    reader_dfs
        .add_file_system(file_system(source_a.clone()))
        .expect("filesystem");
    let mut object = file_metadata(source_a.clone());
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source_a.clone(), source_b.clone()];
    object.content_hash = Some(content_hash(b"right payload bytes"));
    object.content_size_bytes = Some(b"right payload bytes".len() as u64);
    let object_id = object.object_id.clone();
    let version = object.version.clone();
    let hash = object.content_hash.clone();
    reader_dfs.add_object_metadata(object).expect("object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("first read request"),
        vec![source_a.clone()]
    );
    assert_eq!(
        source_a_transport
            .drain_incoming()
            .expect("source a mailbox")
            .len(),
        1
    );
    let request_id = reader_dfs
        .pending_reads
        .iter()
        .find_map(|(request_id, pending)| {
            (pending.target_node_id == source_a).then_some(request_id.clone())
        })
        .expect("pending source a read");

    let event = reader_dfs
        .handle_networked_read_response(
            source_a.clone(),
            DfsReadResponse {
                request_id: request_id.clone(),
                object_id: object_id.clone(),
                result: DfsReadResult::Found(DfsReadData {
                    object_id: wrong_object_id.clone(),
                    version: version.clone(),
                    content_hash: hash.clone(),
                    bytes: b"right payload bytes".to_vec(),
                }),
            },
        )
        .expect("wrong-payload response is isolated");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: completed_request_id,
            object_id: completed_object_id,
            source_node_id,
            result: DfsReadResult::Rejected { reason },
        } if completed_request_id == request_id
            && completed_object_id == object_id
            && source_node_id == source_a
            && reason.contains("unexpected DFS read response payload object")
    ));
    assert!(!reader_dfs.pending_reads.contains_key(&request_id));
    assert_eq!(
        reader_dfs
            .current_read_object_metadata(&object_id)
            .expect("current metadata")
            .current_locations,
        vec![source_b.clone()]
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("fallback after wrong-payload response"),
        vec![source_b.clone()]
    );
    assert_eq!(
        source_b_transport
            .drain_incoming()
            .expect("source b mailbox")
            .len(),
        1
    );
    assert!(matches!(
        reader_dfs.read_local_object(&wrong_object_id),
        Err(DfsError::UnknownObject { .. })
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn object_metadata_normalizes_duplicate_current_locations() {
    let root = temp_root("metadata-normalizes-duplicate-locations");
    let owner = NodeId::from_legacy_named("node_location_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_location_replica").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut object = file_metadata(owner.clone());
    object.object_id = DfsObjectId::from_legacy_named("obj_duplicate_locations").expect("object");
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone(), replica.clone(), owner.clone()];

    dfs.add_object_metadata(object.clone())
        .expect("object metadata");

    assert_eq!(
        dfs.metadata()
            .object(&object.object_id)
            .expect("memory object")
            .current_locations,
        vec![owner.clone(), replica.clone()]
    );
    let backend_object = dfs
        .metadata_backend()
        .expect("backend")
        .get_object(&object.object_id)
        .expect("backend read")
        .expect("backend object");
    assert_eq!(
        backend_object.current_locations,
        vec![owner.clone(), replica.clone()]
    );
    let backend_version = dfs
        .metadata_backend()
        .expect("backend")
        .get_version(&object.object_id, &object.version)
        .expect("backend version read")
        .expect("backend version");
    assert_eq!(backend_version.locations, vec![owner, replica]);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn object_version_record_normalizes_duplicate_locations() {
    let owner = NodeId::from_legacy_named("node_version_location_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_version_location_replica").expect("node");
    let mut object = file_metadata(owner.clone());
    object.current_locations = vec![owner.clone(), replica.clone(), owner.clone()];

    let record = object_version_record(&object, None);

    assert_eq!(record.locations, vec![owner, replica]);
}

#[test]
fn remote_read_content_mismatch_prunes_stale_location() {
    let root = temp_root("remote-read-mismatch-prunes-location");
    let reader = NodeId::from_legacy_named("node_read_mismatch_reader").expect("node");
    let source = NodeId::from_legacy_named("node_read_mismatch_source").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_read_mismatch").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let expected_hash = content_hash(b"expected remote bytes");
    let request_id = "dfs_net_req_read_mismatch".to_string();
    let mut dfs = networked_sifs_with_backend(&root, reader.clone());
    let mut object = file_metadata(source.clone());
    object.object_id = object_id.clone();
    object.version = version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source.clone()];
    object.content_hash = Some(expected_hash.clone());
    object.content_size_bytes = Some(b"expected remote bytes".len() as u64);
    dfs.add_object_metadata(object).expect("object metadata");
    dfs.pending_reads.insert(
        request_id.clone(),
        PendingNetworkedRead {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some(expected_hash.clone()),
            content_size_bytes: Some(b"expected remote bytes".len() as u64),
            range_offset: None,
            range_max_bytes: None,
            next_chunk_index: 0,
            window_end_chunk_index: None,
            target_node_id: source.clone(),
            next_retry_at: Instant::now() + DFS_REMOTE_READ_RETRY_DELAY,
        },
    );

    let event = dfs
        .handle_networked_read_response(
            source.clone(),
            DfsReadResponse {
                request_id: request_id.clone(),
                object_id: object_id.clone(),
                result: DfsReadResult::Found(DfsReadData {
                    object_id: object_id.clone(),
                    version: version.clone(),
                    content_hash: Some(expected_hash),
                    bytes: b"corrupt remote bytes".to_vec(),
                }),
            },
        )
        .expect("mismatch handled");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: completed_request_id,
            source_node_id,
            result: DfsReadResult::Rejected { reason },
            ..
        } if completed_request_id == request_id
            && source_node_id == source
            && reason.contains("content hash mismatch")
    ));
    assert!(!dfs.pending_reads.contains_key(&request_id));
    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("current metadata");
    assert!(
        !object.current_locations.contains(&source),
        "source that returned bytes failing content validation should be pruned"
    );
    assert_eq!(
        dfs.request_remote_object_read(&object_id)
            .expect("no stale source to retry"),
        Vec::<NodeId>::new()
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn remote_read_location_prune_updates_stale_backend_version_record() {
    let root = temp_root("read-prune-updates-version-record");
    let reader = NodeId::from_legacy_named("node_read_prune_reader").expect("node");
    let source = NodeId::from_legacy_named("node_read_prune_source").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_read_prune_version").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let hash = content_hash(b"version-location bytes");
    let mut dfs = networked_sifs_with_backend(&root, reader);
    let mut object = file_metadata(source.clone());
    object.object_id = object_id.clone();
    object.version = version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations.clear();
    object.content_hash = Some(hash.clone());
    object.content_size_bytes = Some(b"version-location bytes".len() as u64);
    dfs.add_object_metadata(object.clone())
        .expect("object metadata");
    dfs.metadata_backend
        .as_mut()
        .expect("backend")
        .put_version(&DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some(hash.clone()),
            size_bytes: object.content_size_bytes,
            locations: vec![source.clone()],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        })
        .expect("stale version locations");

    dfs.mark_remote_read_location_missing(&object_id, &version, &Some(hash), &source)
        .expect("prune stale version location");

    assert_eq!(
        dfs.metadata_backend
            .as_ref()
            .expect("backend")
            .get_object(&object_id)
            .expect("backend object")
            .expect("backend object")
            .current_locations,
        Vec::<NodeId>::new()
    );
    assert_eq!(
        dfs.metadata_backend
            .as_ref()
            .expect("backend")
            .get_version(&object_id, &version)
            .expect("backend version")
            .expect("backend version")
            .locations,
        Vec::<NodeId>::new()
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn idle_chunk_transfer_pruning_releases_request_state() {
    let root = temp_root("idle-chunk-transfer-prune");
    let owner = NodeId::from_legacy_named("node_prune_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_prune_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_prune_transfer").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let content_hash = content_hash(b"prune bytes");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    let stale_at = Instant::now() - DFS_CHUNK_TRANSFER_IDLE_TIMEOUT - Duration::from_secs(1);

    let read_request_id = "dfs_net_req_prune_read".to_string();
    dfs.pending_reads.insert(
        read_request_id.clone(),
        PendingNetworkedRead {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some(content_hash.clone()),
            content_size_bytes: Some(70_000),
            range_offset: None,
            range_max_bytes: None,
            next_chunk_index: 0,
            window_end_chunk_index: None,
            target_node_id: replica.clone(),
            next_retry_at: Instant::now() + DFS_REMOTE_READ_RETRY_DELAY,
        },
    );
    dfs.incoming_read_transfers
        .insert(read_request_id.clone(), {
            let (_, assembler) = test_chunked_assembler(
                &root,
                "idle-prune-read",
                &object_id,
                &version,
                &content_hash,
                70_000,
                2,
            );
            InProgressReadTransfer {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: content_hash.clone(),
                total_size_bytes: 70_000,
                last_updated_at: stale_at,
                assembler,
            }
        });

    let fetch_request_id = "dfs_net_req_prune_fetch".to_string();
    let intent_id = "dfs_intent_prune".to_string();
    dfs.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: content_hash.clone(),
            size_bytes: 70_000,
            patch: None,
            stage_retries: BTreeMap::from([(
                replica.clone(),
                RepairRetryState {
                    attempts: 3,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            repair_retries: BTreeMap::from([(
                replica.clone(),
                RepairRetryState {
                    attempts: 3,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::from([replica.clone()]),
            fetch_inflight_request_ids: BTreeMap::from([(
                replica.clone(),
                fetch_request_id.clone(),
            )]),
            fetch_inflight_started_ticks: BTreeMap::from([(replica.clone(), 0)]),
            fetch_next_chunk_indexes: BTreeMap::from([(replica.clone(), 1)]),
            fetch_window_end_chunk_indexes: BTreeMap::from([(replica.clone(), 64)]),
            fetch_failed_from: BTreeSet::new(),
        },
    );
    dfs.incoming_fetch_transfers
        .insert(fetch_request_id.clone(), {
            let (_, assembler) = test_chunked_assembler(
                &root,
                "idle-prune-fetch",
                &object_id,
                &version,
                &content_hash,
                70_000,
                2,
            );
            let mut transfer = InProgressFetchBlobTransfer::new(
                object_id.clone(),
                version.clone(),
                content_hash.clone(),
                70_000,
                2,
                assembler,
            )
            .expect("fetch transfer");
            transfer.last_updated_at = stale_at;
            transfer
        });

    let blob_key = blob_transfer_key("stage", &replica, &intent_id, "dfs_net_req_prune_stage");
    dfs.incoming_blob_transfers.insert(blob_key.clone(), {
        let (_, assembler) = test_chunked_assembler(
            &root,
            "idle-prune-blob",
            &object_id,
            &version,
            &content_hash,
            70_000,
            2,
        );
        let mut transfer = InProgressBlobTransfer::new(
            "dfs_net_req_prune_stage".to_string(),
            intent_id.clone(),
            file_system_id,
            object_id,
            None,
            version,
            content_hash,
            70_000,
            2,
            assembler,
        )
        .expect("blob transfer");
        transfer.last_updated_at = stale_at;
        transfer
    });

    dfs.prune_idle_chunk_transfers();

    assert!(!dfs.pending_reads.contains_key(&read_request_id));
    assert!(!dfs.incoming_read_transfers.contains_key(&read_request_id));
    assert!(!dfs.incoming_fetch_transfers.contains_key(&fetch_request_id));
    let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
    assert!(!pending.fetch_inflight_from.contains(&replica));
    assert!(!pending.fetch_inflight_request_ids.contains_key(&replica));
    assert!(!pending.fetch_inflight_started_ticks.contains_key(&replica));
    assert!(!pending.fetch_next_chunk_indexes.contains_key(&replica));
    assert!(
        !pending
            .fetch_window_end_chunk_indexes
            .contains_key(&replica)
    );
    assert!(pending.fetch_failed_from.contains(&replica));
    assert_eq!(
        pending
            .stage_retries
            .get(&replica)
            .expect("stage retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .stage_retries
            .get(&replica)
            .expect("stage retry")
            .inflight
    );
    assert_eq!(
        pending
            .repair_retries
            .get(&replica)
            .expect("repair retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .repair_retries
            .get(&replica)
            .expect("repair retry")
            .inflight
    );
    assert!(!dfs.incoming_blob_transfers.contains_key(&blob_key));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn retry_pending_repairs_is_idle_noop_without_distributed_work() {
    let root = temp_root("retry-pending-repairs-idle-noop");
    let owner = NodeId::from_legacy_named("node_retry_idle").expect("node");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.repair_retry_tick = 41;

    let scheduled = dfs.retry_pending_repairs().expect("idle retry");

    assert!(scheduled.is_empty());
    assert_eq!(dfs.repair_retry_tick, 41);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn retry_pending_repairs_prunes_idle_reads_without_distributed_writes() {
    let root = temp_root("retry-pending-repairs-prunes-idle-reads");
    let owner = NodeId::from_legacy_named("node_retry_prune_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_retry_prune_reader").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_retry_prune").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let content_hash = content_hash(b"retry prune read bytes");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.repair_retry_tick = 17;
    let request_id = "dfs_net_req_retry_prune_read".to_string();
    let stale_at = Instant::now() - DFS_CHUNK_TRANSFER_IDLE_TIMEOUT - Duration::from_secs(1);
    dfs.pending_reads.insert(
        request_id.clone(),
        PendingNetworkedRead {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some(content_hash.clone()),
            content_size_bytes: Some(70_000),
            range_offset: None,
            range_max_bytes: None,
            next_chunk_index: 0,
            window_end_chunk_index: None,
            target_node_id: reader,
            next_retry_at: Instant::now() + DFS_REMOTE_READ_RETRY_DELAY,
        },
    );
    dfs.incoming_read_transfers.insert(request_id.clone(), {
        let (_, assembler) = test_chunked_assembler(
            &root,
            "retry-prune-read",
            &object_id,
            &version,
            &content_hash,
            70_000,
            2,
        );
        InProgressReadTransfer {
            object_id,
            version,
            content_hash,
            total_size_bytes: 70_000,
            last_updated_at: stale_at,
            assembler,
        }
    });

    let scheduled = dfs.retry_pending_repairs().expect("retry prune");

    assert!(scheduled.is_empty());
    assert_eq!(dfs.repair_retry_tick, 17);
    assert!(!dfs.pending_reads.contains_key(&request_id));
    assert!(!dfs.incoming_read_transfers.contains_key(&request_id));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stale_fetch_request_expiry_wakes_stage_and_repair_retries() {
    let root = temp_root("stale-fetch-request-expiry");
    let owner = NodeId::from_legacy_named("node_fetch_timeout_owner").expect("node");
    let source = NodeId::from_legacy_named("node_fetch_timeout_source").expect("node");
    let target = NodeId::from_legacy_named("node_fetch_timeout_target").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_fetch_timeout").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let intent_id = "dfs_intent_fetch_timeout".to_string();
    let request_id = "dfs_net_req_fetch_timeout".to_string();
    let content_hash = content_hash(b"fetch timeout bytes");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.repair_retry_tick = fetch_inflight_timeout_ticks();
    dfs.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id,
            version,
            content_hash,
            size_bytes: 0,
            patch: None,
            stage_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 1,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            repair_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 1,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::from([source.clone()]),
            fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
            fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    dfs.expire_stale_fetch_requests();

    let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
    assert!(!pending.fetch_inflight_from.contains(&source));
    assert_eq!(
        pending.fetch_inflight_request_ids.get(&source),
        Some(&request_id),
        "expired fetches keep their request id so a later retry can resume any partial chunks"
    );
    assert!(!pending.fetch_inflight_started_ticks.contains_key(&source));
    assert!(pending.fetch_failed_from.contains(&source));
    assert_eq!(
        pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .inflight
    );
    assert_eq!(
        pending
            .repair_retries
            .get(&target)
            .expect("repair retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .repair_retries
            .get(&target)
            .expect("repair retry")
            .inflight
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn fetch_rejection_wakes_stage_and_repair_retries() {
    let root = temp_root("fetch-rejection-wakes-retries");
    let owner = NodeId::from_legacy_named("node_fetch_reject_owner").expect("node");
    let source = NodeId::from_legacy_named("node_fetch_reject_source").expect("node");
    let target = NodeId::from_legacy_named("node_fetch_reject_target").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_fetch_reject").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let intent_id = "dfs_intent_fetch_reject".to_string();
    let request_id = "dfs_net_req_fetch_reject".to_string();
    let content_hash = content_hash(b"fetch reject bytes");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: content_hash.clone(),
            size_bytes: b"fetch reject bytes".len() as u64,
            patch: None,
            stage_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 2,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            repair_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 2,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::from([source.clone()]),
            fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
            fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    let event = dfs
        .handle_fetch_blob_response(
            source.clone(),
            DfsFetchBlobResponse {
                request_id,
                intent_id: intent_id.clone(),
                object_id,
                version,
                source_node_id: source.clone(),
                result: DfsFetchBlobResult::NotFound,
            },
        )
        .expect("fetch miss handled");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobFetchRejected {
            source_node_id,
            ..
        } if source_node_id == source
    ));
    let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
    assert!(!pending.fetch_inflight_from.contains(&source));
    assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
    assert!(pending.fetch_failed_from.contains(&source));
    assert_eq!(
        pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .inflight
    );
    assert_eq!(
        pending
            .repair_retries
            .get(&target)
            .expect("repair retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .repair_retries
            .get(&target)
            .expect("repair retry")
            .inflight
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn fetch_store_failure_wakes_stage_and_repair_retries() {
    let root = temp_root("fetch-store-failure-wakes-retries");
    let owner = NodeId::from_legacy_named("node_fetch_store_fail_owner").expect("node");
    let source = NodeId::from_legacy_named("node_fetch_store_fail_source").expect("node");
    let target = NodeId::from_legacy_named("node_fetch_store_fail_target").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_fetch_store_fail").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let intent_id = "dfs_intent_fetch_store_fail".to_string();
    let request_id = "dfs_net_req_fetch_store_fail".to_string();
    let expected_bytes = b"expected fetch bytes";
    let corrupt_bytes = b"corrupt fetch bytes";
    let content_hash = content_hash(expected_bytes);
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: content_hash.clone(),
            size_bytes: expected_bytes.len() as u64,
            patch: None,
            stage_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 2,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            repair_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 2,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::from([source.clone()]),
            fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
            fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    let event = dfs
        .handle_fetch_blob_response(
            source.clone(),
            DfsFetchBlobResponse {
                request_id,
                intent_id: intent_id.clone(),
                object_id: object_id.clone(),
                version: version.clone(),
                source_node_id: source.clone(),
                result: DfsFetchBlobResult::Found {
                    content_hash: content_hash.clone(),
                    bytes: corrupt_bytes.to_vec(),
                },
            },
        )
        .expect("hash mismatch is isolated to the fetch source");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobFetchRejected {
            source_node_id,
            reason,
            ..
        } if source_node_id == source && reason.contains("content hash mismatch")
    ));
    let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
    assert!(!pending.fetch_inflight_from.contains(&source));
    assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
    assert!(pending.fetch_failed_from.contains(&source));
    assert_eq!(
        pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .inflight
    );
    assert_eq!(
        pending
            .repair_retries
            .get(&target)
            .expect("repair retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .repair_retries
            .get(&target)
            .expect("repair retry")
            .inflight
    );
    let manifest_error = dfs
        .content
        .read_manifest(object_id.as_str(), version.as_str())
        .expect_err("rejected fetch must not leave a wrong object-version manifest");
    assert!(matches!(
        manifest_error,
        ContentStoreError::Io { source, .. }
            if source.kind() == std::io::ErrorKind::NotFound
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn fetch_response_reuses_existing_local_blob_without_rehashing_payload() {
    let root = temp_root("fetch-response-reuses-existing-local-blob");
    let owner = NodeId::from_legacy_named("node_fetch_reuse_owner").expect("node");
    let source = NodeId::from_legacy_named("node_fetch_reuse_source").expect("node");
    let target = NodeId::from_legacy_named("node_fetch_reuse_target").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_fetch_reuse").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let intent_id = "dfs_intent_fetch_reuse".to_string();
    let request_id = "dfs_net_req_fetch_reuse".to_string();
    let bytes = b"already fetched bytes";
    let mut stale_payload = bytes.to_vec();
    stale_payload[0] ^= 0xff;
    stale_payload.extend_from_slice(b" with a different duplicate payload length");
    let mut dfs = NetworkedDfsService::new(
        InProcessNetwork::new().add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version.as_str(), bytes)
        .expect("seed existing local blob");
    dfs.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: manifest.content_hash.clone(),
            size_bytes: bytes.len() as u64,
            patch: None,
            stage_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 2,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::from([source.clone()]),
            fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
            fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    let event = dfs
        .handle_fetch_blob_response(
            source.clone(),
            DfsFetchBlobResponse {
                request_id,
                intent_id: intent_id.clone(),
                object_id: object_id.clone(),
                version: version.clone(),
                source_node_id: source.clone(),
                result: DfsFetchBlobResult::Found {
                    content_hash: manifest.content_hash.clone(),
                    bytes: stale_payload,
                },
            },
        )
        .expect("existing local blob should satisfy fetch");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobFetched {
            source_node_id,
            ..
        } if source_node_id == source
    ));
    let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
    assert!(!pending.fetch_inflight_from.contains(&source));
    assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
    assert!(pending.fetch_failed_from.is_empty());
    assert_eq!(
        pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .next_retry_tick,
        0
    );
    assert_eq!(
        dfs.content
            .read_blob(&manifest.content_hash)
            .expect("existing blob"),
        bytes
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn chunked_fetch_store_failure_is_rejected_without_hard_error() {
    let root = temp_root("chunked-fetch-store-failure-isolated");
    let owner = NodeId::from_legacy_named("node_chunk_fetch_fail_owner").expect("node");
    let source = NodeId::from_legacy_named("node_chunk_fetch_fail_source").expect("node");
    let target = NodeId::from_legacy_named("node_chunk_fetch_fail_target").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_chunk_fetch_fail").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let intent_id = "dfs_intent_chunk_fetch_fail".to_string();
    let request_id = "dfs_net_req_chunk_fetch_fail".to_string();
    let expected_bytes = (0..(DFS_BLOB_CHUNK_BYTES + 7))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let mut corrupt_bytes = expected_bytes.clone();
    corrupt_bytes[DFS_BLOB_CHUNK_BYTES] ^= 0xff;
    let content_hash = content_hash(&expected_bytes);
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: content_hash.clone(),
            size_bytes: expected_bytes.len() as u64,
            patch: None,
            stage_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 2,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::from([source.clone()]),
            fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
            fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    let total_chunks = blob_chunk_count(&corrupt_bytes).expect("chunk count");
    for (chunk_index, chunk_bytes) in corrupt_bytes.chunks(DFS_BLOB_CHUNK_BYTES).enumerate() {
        let event = dfs
            .handle_fetch_blob_response_chunk(
                source.clone(),
                DfsFetchBlobResponseChunk {
                    request_id: request_id.clone(),
                    intent_id: intent_id.clone(),
                    object_id: object_id.clone(),
                    version: version.clone(),
                    source_node_id: source.clone(),
                    content_hash: content_hash.clone(),
                    total_size_bytes: corrupt_bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    chunk_hash: None,
                    bytes: chunk_bytes.to_vec(),
                },
            )
            .expect("corrupt fetch chunk is isolated");
        if chunk_index == 0 {
            assert!(event.is_none());
        } else {
            assert!(matches!(
                event,
                Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                    source_node_id,
                    reason,
                    ..
                }) if source_node_id == source && reason.contains("content hash mismatch")
            ));
        }
    }
    let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
    assert!(!pending.fetch_inflight_from.contains(&source));
    assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
    assert!(pending.fetch_failed_from.contains(&source));
    assert_eq!(
        pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .inflight
    );
    assert!(
        dfs.content
            .read_manifest(object_id.as_str(), version.as_str())
            .is_err(),
        "rejected chunked fetch must not leave a wrong object-version manifest"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn chunked_fetch_response_reuses_existing_local_blob_without_rehashing_payload() {
    let root = temp_root("chunked-fetch-reuses-existing-local-blob");
    let owner = NodeId::from_legacy_named("node_chunk_fetch_reuse_owner").expect("node");
    let source = NodeId::from_legacy_named("node_chunk_fetch_reuse_source").expect("node");
    let target = NodeId::from_legacy_named("node_chunk_fetch_reuse_target").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_chunk_fetch_reuse").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let intent_id = "dfs_intent_chunk_fetch_reuse".to_string();
    let request_id = "dfs_net_req_chunk_fetch_reuse".to_string();
    let bytes = (0..(DFS_BLOB_CHUNK_BYTES + 11))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let mut stale_payload = bytes.clone();
    stale_payload[DFS_BLOB_CHUNK_BYTES] ^= 0xff;
    stale_payload.extend_from_slice(b" with a different duplicate payload length");
    let mut dfs = NetworkedDfsService::new(
        InProcessNetwork::new().add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version.as_str(), &bytes)
        .expect("seed existing local blob");
    dfs.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: manifest.content_hash.clone(),
            size_bytes: bytes.len() as u64,
            patch: None,
            stage_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 2,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::from([source.clone()]),
            fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
            fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    let total_chunks = blob_chunk_count(&stale_payload).expect("chunk count");
    let first_chunk = stale_payload
        .chunks(DFS_BLOB_CHUNK_BYTES)
        .next()
        .expect("first chunk");
    let event = dfs
        .handle_fetch_blob_response_chunk(
            source.clone(),
            DfsFetchBlobResponseChunk {
                request_id: request_id.clone(),
                intent_id: intent_id.clone(),
                object_id: object_id.clone(),
                version: version.clone(),
                source_node_id: source.clone(),
                content_hash: manifest.content_hash.clone(),
                total_size_bytes: stale_payload.len() as u64,
                chunk_index: 0,
                total_chunks,
                chunk_hash: None,
                bytes: first_chunk.to_vec(),
            },
        )
        .expect("existing local blob should satisfy chunked fetch")
        .expect("chunked fetch should complete from the existing local blob immediately");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobFetched {
            source_node_id,
            ..
        } if source_node_id == source
    ));
    let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
    assert!(!pending.fetch_inflight_from.contains(&source));
    assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
    assert!(pending.fetch_failed_from.is_empty());
    assert_eq!(
        pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .next_retry_tick,
        0
    );
    assert_eq!(
        dfs.content
            .read_blob(&manifest.content_hash)
            .expect("existing blob"),
        bytes
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn chunked_fetch_response_teaches_unknown_pending_write_size() {
    let root = temp_root("chunked-fetch-teaches-size");
    let owner = NodeId::from_legacy_named("node_fetch_size_owner").expect("node");
    let source = NodeId::from_legacy_named("node_fetch_size_source").expect("node");
    let target = NodeId::from_legacy_named("node_fetch_size_target").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_fetch_size").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let intent_id = "dfs_intent_fetch_size".to_string();
    let request_id = "dfs_net_req_fetch_size".to_string();
    let bytes = (0..(DFS_BLOB_CHUNK_BYTES + 8192))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let content_hash = content_hash(&bytes);
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_version(&DfsObjectVersionRecord {
            file_system_id: sifs_file_system_id(),
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some(content_hash.clone()),
            size_bytes: None,
            locations: vec![source.clone()],
            created_by_event_id: None,
            created_at: OffsetDateTime::now_utc(),
            visible: false,
        })
        .expect("seed size-less version record");
    dfs.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: content_hash.clone(),
            size_bytes: 0,
            patch: None,
            stage_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 2,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::from([source.clone()]),
            fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
            fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    for (chunk_index, chunk_bytes) in bytes.chunks(DFS_BLOB_CHUNK_BYTES).enumerate() {
        let event = dfs
            .handle_fetch_blob_response_chunk(
                source.clone(),
                DfsFetchBlobResponseChunk {
                    request_id: request_id.clone(),
                    intent_id: intent_id.clone(),
                    object_id: object_id.clone(),
                    version: version.clone(),
                    source_node_id: source.clone(),
                    content_hash: content_hash.clone(),
                    total_size_bytes: bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks: blob_chunk_count(&bytes).expect("chunk count"),
                    chunk_hash: None,
                    bytes: chunk_bytes.to_vec(),
                },
            )
            .expect("chunk handled");
        if chunk_index == 0 {
            assert!(event.is_none());
        } else {
            assert!(matches!(
                event,
                Some(NetworkedDfsServiceEvent::BlobFetched {
                    intent_id: event_intent,
                    object_id: event_object_id,
                    source_node_id,
                    ..
                }) if event_intent == intent_id
                    && event_object_id == object_id
                    && source_node_id == source
            ));
        }
    }

    let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
    assert_eq!(pending.size_bytes, bytes.len() as u64);
    assert!(!pending.fetch_inflight_from.contains(&source));
    assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
    assert!(pending.fetch_failed_from.is_empty());
    assert_eq!(
        pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .next_retry_tick,
        0
    );
    assert_eq!(
        dfs.content.read_blob(&content_hash).expect("fetched blob"),
        bytes
    );
    assert_eq!(
        dfs.metadata_backend
            .as_ref()
            .expect("metadata backend")
            .get_version(&object_id, &version)
            .expect("version lookup")
            .expect("version record")
            .size_bytes,
        Some(bytes.len() as u64)
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_retry_schedule_does_not_double_count_completed_inflight_attempt() {
    let root = temp_root("repair-retry-no-double-count");
    let owner = NodeId::from_legacy_named("node_retry_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_retry_replica").expect("node");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    let intent_id = "dfs_intent_retry".to_string();
    dfs.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id: DfsObjectId::from_legacy_named("obj_retry").expect("object"),
            version: DfsVersion::from_legacy_named("version_2").expect("version"),
            content_hash: "blake3:retry".to_string(),
            size_bytes: 5,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    dfs.repair_retry_tick = 10;
    dfs.record_repair_inflight(&intent_id, &replica, "dfs_net_req_repair".to_string());
    dfs.record_repair_retry_scheduled(&intent_id, &replica);

    let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
    let state = pending
        .repair_retries
        .get(&replica)
        .expect("repair retry state");
    assert_eq!(state.attempts, 1);
    assert_eq!(state.next_retry_tick, 11);
    assert!(!state.inflight);
    assert!(!pending.repair_inflight_request_ids.contains_key(&replica));
    assert!(!dfs.repair_retry_is_due(&intent_id, &replica));
    dfs.repair_retry_tick = 11;
    assert!(dfs.repair_retry_is_due(&intent_id, &replica));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_response_conflicting_receipt_preserves_retry_state() {
    let root = temp_root("repair-conflicting-receipt-keeps-retry");
    let owner = NodeId::from_legacy_named("node_repair_conflict_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_repair_conflict_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"repair response with a durable conflicting receipt";
    let content_hash = content_hash(bytes);
    let request_id = "dfs_net_req_repair_conflicting_receipt".to_string();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata).expect("metadata");
    let intent = dfs
        .coordinator
        .create_intent(
            sifs_file_system_id(),
            object_id.clone(),
            Some(version_1),
            version_2.clone(),
            owner.clone(),
            vec![replica.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect("intent");
    dfs.pending_writes.insert(
        intent.intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version_2.clone(),
            content_hash: content_hash.clone(),
            size_bytes: bytes.len() as u64,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );
    dfs.record_repair_inflight(&intent.intent_id, &replica, request_id.clone());
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version_2,
        &replica,
        "blake3:stale-conflicting-repair-receipt",
    )
    .expect("seed conflicting receipt");

    let error = dfs
        .handle_repair_replica_response(
            replica.clone(),
            DfsRepairReplicaResponse {
                request_id: request_id.clone(),
                intent_id: intent.intent_id.clone(),
                file_system_id: sifs_file_system_id(),
                object_id,
                version: version_2,
                replica_node_id: replica.clone(),
                result: DfsBlobTransferResult::Durable { content_hash },
            },
        )
        .expect_err("conflicting durable receipt must reject repair response");
    assert!(
        error
            .to_string()
            .contains("conflicting DFS replica receipt"),
        "unexpected error: {error}"
    );

    let pending = dfs
        .pending_writes
        .get(&intent.intent_id)
        .expect("pending write");
    assert!(
        pending.repair_retries.contains_key(&replica),
        "rejected repair response must keep retry state"
    );
    assert_eq!(
        pending.repair_inflight_request_ids.get(&replica),
        Some(&request_id),
        "rejected repair response must not clear the in-flight request before durable acceptance"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_command_conflicting_receipt_does_not_commit_visible_metadata() {
    let root = temp_root("repair-command-conflicting-receipt-no-visible-commit");
    let owner = NodeId::from_legacy_named("node_repair_cmd_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_repair_cmd_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_cmd_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"repair command should not commit after receipt conflict";
    let content_hash = content_hash(bytes);
    let mut dfs = networked_sifs_with_backend(&root, replica.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(metadata).expect("metadata");
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version_2,
        &replica,
        "blake3:stale-conflicting-repair-command-receipt",
    )
    .expect("seed conflicting receipt");

    let event = dfs
        .handle_repair_replica(
            owner,
            DfsRepairReplicaCommand {
                request_id: "dfs_net_req_repair_cmd_conflict".to_string(),
                intent_id: "dfs_intent_repair_cmd_conflict".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1.clone()),
                version: version_2.clone(),
                content_hash,
                bytes: bytes.to_vec(),
                object_metadata: None,
            },
        )
        .expect("repair command response event");
    assert!(
        matches!(
            event,
            NetworkedDfsServiceEvent::RepairRejected {
                ref reason,
                ..
            } if reason.contains("conflicting DFS replica receipt")
        ),
        "repair command with conflicting receipt must be rejected, got {event:?}"
    );

    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("visible metadata");
    assert_eq!(
        object.version, version_1,
        "rejected repair command must not advance visible metadata"
    );
    assert!(
        !object.current_locations.contains(&replica),
        "rejected repair command must not add the local replica to visible locations"
    );
    let backend_object = dfs
        .metadata_backend()
        .expect("backend")
        .get_object(&object_id)
        .expect("backend object")
        .expect("backend object exists");
    assert_eq!(
        backend_object.version, version_1,
        "rejected repair command must not publish visible metadata to the backend"
    );
    assert!(
        dfs.content
            .read_manifest(object_id.as_str(), version_2.as_str())
            .is_err(),
        "receipt preflight should reject the repair before storing orphaned content"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stage_command_conflicting_receipt_does_not_store_orphan_content() {
    let root = temp_root("stage-command-conflicting-receipt-no-orphan-content");
    let owner = NodeId::from_legacy_named("node_stage_cmd_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stage_cmd_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_cmd_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"stage command should reject before storing orphan content";
    let content_hash = content_hash(bytes);
    let mut dfs = networked_sifs_with_backend(&root, replica.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(metadata).expect("metadata");
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version_2,
        &replica,
        "blake3:stale-conflicting-stage-command-receipt",
    )
    .expect("seed conflicting receipt");

    let event = dfs
        .handle_stage_blob(
            owner,
            DfsStageBlobCommand {
                request_id: "dfs_net_req_stage_cmd_conflict".to_string(),
                intent_id: "dfs_intent_stage_cmd_conflict".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1),
                version: version_2.clone(),
                content_hash,
                bytes: bytes.to_vec(),
                object_metadata: None,
            },
        )
        .expect("stage command response event");
    assert!(
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected {
                ref reason,
                ..
            } if reason.contains("conflicting DFS replica receipt")
        ),
        "stage command with conflicting receipt must be rejected, got {event:?}"
    );
    assert!(
        dfs.content
            .read_manifest(object_id.as_str(), version_2.as_str())
            .is_err(),
        "stage receipt preflight should reject before storing orphaned content"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stage_command_accepts_metadata_shard_primary_for_transient_object() {
    let root = temp_root("stage-command-accepts-shard-primary-transient");
    let owner = NodeId::from_legacy_named("node_stage_transient_owner").expect("owner");
    let primary = NodeId::from_legacy_named("node_stage_transient_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_stage_transient_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_transient_primary").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_stage_transient_1").expect("base version");
    let version_2 =
        DfsVersion::from_legacy_named("version_stage_transient_2").expect("target version");
    let file_system = file_system(owner.clone());
    let shared_kv = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![primary.clone()]).expect("kv"),
    );
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: primary.clone(),
                replica_node_ids: vec![owner.clone(), replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let network = InProcessNetwork::new();
    let _primary_mailbox = network.add_node(primary.clone());
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(replica.clone()), content)
        .with_control_plane(shared_kv)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    let mut object = file_metadata(owner);
    object.file_system_id = file_system.file_system_id.clone();
    object.object_id = object_id.clone();
    object.path = "/services/transient/stage.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![primary.clone()];
    let bytes = b"metadata shard primary can stage a transient object";
    let content_hash = content_hash(bytes);

    let event = dfs
        .handle_stage_blob(
            primary.clone(),
            DfsStageBlobCommand {
                request_id: "dfs_stage_transient_1".to_string(),
                intent_id: "dfs_intent_stage_transient_1".to_string(),
                file_system_id: file_system.file_system_id,
                object_id: object_id.clone(),
                expected_base_version: Some(version_1),
                version: version_2.clone(),
                content_hash: content_hash.clone(),
                bytes: bytes.to_vec(),
                object_metadata: Some(object),
            },
        )
        .expect("stage command");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobStaged {
            replica_node_id,
            ..
        } if replica_node_id == replica
    ));
    let manifest = dfs
        .content()
        .read_manifest(object_id.as_str(), version_2.as_str())
        .expect("staged manifest");
    assert_eq!(manifest.content_hash, content_hash);
    assert_eq!(
        dfs.content()
            .read_blob(&manifest.content_hash)
            .expect("staged blob"),
        bytes
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stage_chunk_existing_manifest_conflicting_receipt_returns_rejection() {
    let root = temp_root("stage-chunk-existing-manifest-conflicting-receipt");
    let owner = NodeId::from_legacy_named("node_stage_chunk_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stage_chunk_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_chunk_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"stage chunk duplicate fast path must reject receipt conflict";
    let mut dfs = networked_sifs_with_backend(&root, replica.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(metadata).expect("metadata");
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("existing manifest");
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version_2,
        &replica,
        "blake3:stale-conflicting-stage-chunk-receipt",
    )
    .expect("seed conflicting receipt");

    let event = dfs
        .handle_stage_blob_chunk(
            owner,
            DfsStageBlobChunkCommand {
                request_id: "dfs_net_req_stage_chunk_conflict".to_string(),
                intent_id: "dfs_intent_stage_chunk_conflict".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id,
                expected_base_version: Some(version_1),
                version: version_2,
                content_hash: manifest.content_hash,
                total_size_bytes: bytes.len() as u64,
                chunk_index: 0,
                total_chunks: 1,
                chunk_hash: None,
                bytes: bytes.to_vec(),
                object_metadata: None,
            },
        )
        .expect("stage chunk conflict should be isolated")
        .expect("stage chunk conflict event");
    assert!(
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected {
                ref reason,
                ..
            } if reason.contains("conflicting DFS replica receipt")
        ),
        "stage chunk duplicate fast path must reject receipt conflict, got {event:?}"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn committed_intent_applies_when_commit_notification_target_is_unroutable() {
    let root = temp_root("commit-notify-target-unroutable");
    let owner = NodeId::from_legacy_named("node_commit_notify_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_commit_notify_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_commit_notify").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata.clone()).expect("metadata");
    let bytes = b"committed bytes despite unroutable notify";
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("content");
    let intent = dfs
        .coordinator
        .create_intent(
            metadata.file_system_id.clone(),
            object_id.clone(),
            Some(version_1),
            version_2.clone(),
            owner,
            vec![replica.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect("intent");
    dfs.pending_writes.insert(
        intent.intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version_2.clone(),
            content_hash: manifest.content_hash.clone(),
            size_bytes: manifest.size_bytes,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );
    let committed = dfs
        .coordinator
        .record_durable_receipt(&intent.intent_id, replica.clone())
        .expect("durable receipt");
    assert!(matches!(
        committed.state,
        DfsWriteIntentState::AwaitingReceipts
    ));

    dfs.apply_committed_intent(&intent.intent_id)
        .expect("post-commit replica notification is best effort");

    let object = dfs.metadata().object(&object_id).expect("object");
    assert_eq!(object.version, version_2);
    assert_eq!(
        object.content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );
    assert_eq!(object.current_locations, vec![replica]);
    let read = dfs
        .read_local_object_with_state(&object_id, false)
        .expect("read state");
    assert!(matches!(
        read,
        NetworkedDfsReadOutcome::Found {
            data,
            freshness: NetworkedDfsReadFreshness::Current
        } if data.bytes == bytes
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stage_blob_persists_when_response_target_is_unroutable() {
    let root = temp_root("stage-response-target-unroutable");
    let owner = NodeId::from_legacy_named("node_stage_notify_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stage_notify_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_notify").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"stage durable despite lost requester";
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(replica.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    let event = dfs
        .handle_stage_blob(
            owner,
            DfsStageBlobCommand {
                request_id: "dfs_net_req_stage_unroutable".to_string(),
                intent_id: "dfs_intent_stage_unroutable".to_string(),
                file_system_id: metadata.file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: Some(metadata.version.clone()),
                version: version.clone(),
                content_hash: content_hash(bytes),
                bytes: bytes.to_vec(),
                object_metadata: Some(metadata),
            },
        )
        .expect("unroutable response target should not fail durable stage");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobStaged {
            object_id: staged_object_id,
            version: staged_version,
            replica_node_id,
            ..
        } if staged_object_id == object_id
            && staged_version == version
            && replica_node_id == replica
    ));
    let manifest = dfs
        .content
        .read_manifest(object_id.as_str(), version.as_str())
        .expect("stage manifest");
    assert_eq!(
        dfs.content.read_blob(&manifest.content_hash).expect("blob"),
        bytes
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn replica_stage_and_commit_do_not_append_authoritative_metadata() {
    let root = temp_root("replica-stage-commit-no-authoritative-metadata");
    let owner = NodeId::from_legacy_named("node_replica_cache_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica_cache_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_replica_cache").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"replica content cache without authoritative metadata append";
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    let shared_kv = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![owner.clone()]).expect("kv"),
    );
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig {
                file_system_id: sifs_file_system_id(),
                display_name: "SIFS".to_string(),
                owner_node_id: owner.clone(),
                subtree_policies: Vec::new(),
            })
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "root".to_string(),
                primary_node_id: owner.clone(),
                replica_node_ids: vec![owner.clone(), replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(replica.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(shared_kv)
    .with_metadata_backend(
        open_root_local_metadata_backend(root.join("metadata")).expect("metadata"),
    )
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let before_journal = dfs
        .metadata_backend()
        .expect("metadata backend")
        .metadata_shard_statuses()
        .expect("metadata status")
        .pop()
        .expect("root shard status")
        .journal_last_sequence;

    let stage_event = dfs
        .handle_stage_blob(
            owner.clone(),
            DfsStageBlobCommand {
                request_id: "dfs_net_req_replica_stage_cache".to_string(),
                intent_id: "dfs_intent_replica_cache".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1.clone()),
                version: version_2.clone(),
                content_hash: content_hash(bytes),
                bytes: bytes.to_vec(),
                object_metadata: Some(object.clone()),
            },
        )
        .expect("replica stage");
    assert!(matches!(
        stage_event,
        NetworkedDfsServiceEvent::BlobStaged { .. }
    ));

    let commit_event = dfs
        .handle_commit_version(
            owner,
            DfsCommitVersionCommand {
                request_id: "dfs_net_req_replica_commit_cache".to_string(),
                intent_id: "dfs_intent_replica_cache".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1),
                version: version_2.clone(),
                content_hash: content_hash(bytes),
                object_metadata: Some({
                    object.version = version_2.clone();
                    object.content_hash = Some(content_hash(bytes));
                    object.current_locations = vec![replica.clone()];
                    object
                }),
            },
        )
        .expect("replica commit");
    assert!(matches!(
        commit_event,
        NetworkedDfsServiceEvent::VersionCommitted { .. }
    ));
    assert_eq!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .metadata_shard_statuses()
            .expect("metadata status")
            .pop()
            .expect("root shard status")
            .journal_last_sequence,
        before_journal,
        "replica data-plane commands must wait for primary journal replay before mutating the durable metadata shard"
    );
    assert!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .get_object(&object_id)
            .expect("backend object")
            .is_none(),
        "replica cache metadata must not be published to the durable shard backend"
    );
    assert_eq!(
        dfs.current_read_object_metadata(&object_id)
            .expect("transient local cache")
            .version,
        version_2
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_replica_commits_when_response_target_is_unroutable() {
    let root = temp_root("repair-response-target-unroutable");
    let owner = NodeId::from_legacy_named("node_repair_notify_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_repair_notify_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_notify").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"repair committed despite lost requester";
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(replica.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    let event = dfs
        .handle_repair_replica(
            owner,
            DfsRepairReplicaCommand {
                request_id: "dfs_net_req_repair_unroutable".to_string(),
                intent_id: "dfs_intent_repair_unroutable".to_string(),
                file_system_id: metadata.file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: Some(metadata.version.clone()),
                version: version.clone(),
                content_hash: content_hash(bytes),
                bytes: bytes.to_vec(),
                object_metadata: Some(metadata),
            },
        )
        .expect("unroutable response target should not fail repair commit");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::ReplicaRepaired {
            object_id: repaired_object_id,
            version: repaired_version,
            replica_node_id,
            ..
        } if repaired_object_id == object_id
            && repaired_version == version
            && replica_node_id == replica
    ));
    let read = dfs
        .read_local_object_with_state(&object_id, false)
        .expect("read repaired object");
    assert!(matches!(
        read,
        NetworkedDfsReadOutcome::Found {
            data,
            freshness: NetworkedDfsReadFreshness::Current
        } if data.version == version && data.bytes == bytes
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_replica_accepts_stale_base_when_content_hash_matches_current() {
    // Regression: a metadata-only version bump (chmod/chown/utimes/hardlink/
    // rename) advances an object past the version a queued content repair
    // targets, while leaving content unchanged. The repair's base no longer
    // matches current, but its content hash equals what the current version
    // still references. Content is addressed by hash, so the repair must be
    // accepted -- otherwise the only copy of the content never reaches the
    // replica and reads fail with NotFound despite metadata being present.
    let root = temp_root("repair-stale-base-matching-content");
    let owner = NodeId::from_legacy_named("node_stale_base_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stale_base_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stale_base").expect("object");
    let base_version = DfsVersion::from_legacy_named("version_0").expect("version");
    let content_version = DfsVersion::from_legacy_named("version_1").expect("version");
    let current_version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"content unchanged across a metadata-only version bump";
    let hash = content_hash(bytes);

    // The replica's view of the object has already advanced to the
    // metadata-only bump (version_2), still referencing the same content. The
    // queued content repair, however, still targets the content write
    // (version_1) whose intent based off version_0.
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = current_version.clone();
    metadata.content_hash = Some(hash.clone());
    metadata.current_locations = vec![owner.clone()];

    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(replica.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    let event = dfs
        .handle_repair_replica(
            owner,
            DfsRepairReplicaCommand {
                request_id: "dfs_net_req_stale_base".to_string(),
                intent_id: "dfs_intent_stale_base".to_string(),
                file_system_id: metadata.file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: Some(base_version),
                version: content_version.clone(),
                content_hash: hash.clone(),
                bytes: bytes.to_vec(),
                object_metadata: Some(metadata),
            },
        )
        .expect("repair with matching content hash should be accepted");

    assert!(
        matches!(event, NetworkedDfsServiceEvent::ReplicaRepaired { .. }),
        "stale-base repair whose content matches current must commit"
    );

    let read = dfs
        .read_local_object_with_state(&object_id, false)
        .expect("read repaired object");
    assert!(
        matches!(read, NetworkedDfsReadOutcome::Found { data, .. } if data.bytes == bytes),
        "replica must serve the repaired content-addressed blob"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn chunked_stage_to_unroutable_target_stops_after_first_unsent_chunk() {
    let root = temp_root("chunked-stage-unroutable-stops");
    let owner = NodeId::from_legacy_named("node_chunk_stage_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_chunk_stage_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_chunk_stage").expect("object");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = (0..(DFS_BLOB_CHUNK_BYTES + 8192))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(owner),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    let sent = dfs
        .send_stage_blob_if_routable(
            &replica,
            "dfs_net_req_chunk_stage_unroutable".to_string(),
            "dfs_intent_chunk_stage_unroutable",
            &file_system_id,
            &object_id,
            None,
            &version,
            &content_hash(&bytes),
            &bytes,
            None,
        )
        .expect("send stage");

    assert!(!sent);
    assert_eq!(
        dfs.next_frame_id, 2,
        "large stage should stop after the first unroutable chunk send"
    );
    assert_eq!(dfs.next_message_id, 2);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn chunked_repair_to_unroutable_target_stops_after_first_unsent_chunk() {
    let root = temp_root("chunked-repair-unroutable-stops");
    let owner = NodeId::from_legacy_named("node_chunk_repair_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_chunk_repair_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_chunk_repair").expect("object");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = (0..(DFS_BLOB_CHUNK_BYTES + 8192))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(owner),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    let sent = dfs
        .send_repair_replica_if_routable(
            &replica,
            "dfs_net_req_chunk_repair_unroutable".to_string(),
            "dfs_intent_chunk_repair_unroutable",
            &file_system_id,
            &object_id,
            None,
            &version,
            &content_hash(&bytes),
            &bytes,
            None,
        )
        .expect("send repair");

    assert!(!sent);
    assert_eq!(
        dfs.next_frame_id, 2,
        "large repair should stop after the first unroutable chunk send"
    );
    assert_eq!(dfs.next_message_id, 2);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_replica_window_resumes_large_chunk_transfer() {
    let root = temp_root("repair-window-resumes-large-chunks");
    let owner = NodeId::from_legacy_named("node_repair_window_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_repair_window_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_window").expect("object");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let total_chunks = DFS_STAGE_CHUNK_WINDOW_SIZE + 2;
    let bytes_len = DFS_BLOB_CHUNK_BYTES * (total_chunks as usize - 1) + 17;
    let bytes = (0..bytes_len)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    let mut replica_transport = network.add_node(replica.clone());
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version.as_str(), &bytes)
        .expect("store large repair content");

    let first = dfs
        .send_repair_replica_window_if_routable(
            &replica,
            "dfs_net_req_repair_window".to_string(),
            "dfs_intent_repair_window",
            &file_system_id,
            &object_id,
            None,
            &version,
            &manifest.content_hash,
            manifest.size_bytes,
            0,
            DFS_STAGE_CHUNK_WINDOW_SIZE,
            None,
        )
        .expect("send first repair window");
    assert_eq!(
        first,
        StageBlobSendOutcome {
            sent: true,
            complete: false,
            next_chunk_index: DFS_STAGE_CHUNK_WINDOW_SIZE,
        }
    );
    let first_chunks = replica_transport
        .drain_incoming()
        .expect("first repair frames")
        .into_iter()
        .filter_map(|frame| {
            let MeshMessageBody::Protocol(protocol) = frame.frame.message.body else {
                return None;
            };
            match protocol.decode::<DfsNetworkMessage>().expect("decode DFS") {
                DfsNetworkMessage::RepairReplicaChunk(chunk) => Some(chunk),
                _ => None,
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(first_chunks.len(), DFS_STAGE_CHUNK_WINDOW_SIZE as usize);
    assert_eq!(first_chunks.first().expect("first chunk").chunk_index, 0);
    assert_eq!(
        first_chunks
            .last()
            .expect("last first-window chunk")
            .chunk_index,
        DFS_STAGE_CHUNK_WINDOW_SIZE - 1
    );
    assert!(
        first_chunks
            .iter()
            .all(|chunk| chunk.total_chunks == total_chunks)
    );

    let second = dfs
        .send_repair_replica_window_if_routable(
            &replica,
            "dfs_net_req_repair_window".to_string(),
            "dfs_intent_repair_window",
            &file_system_id,
            &object_id,
            None,
            &version,
            &manifest.content_hash,
            manifest.size_bytes,
            first.next_chunk_index,
            DFS_STAGE_CHUNK_WINDOW_SIZE,
            None,
        )
        .expect("send second repair window");
    assert_eq!(
        second,
        StageBlobSendOutcome {
            sent: true,
            complete: true,
            next_chunk_index: total_chunks,
        }
    );
    let second_chunks = replica_transport
        .drain_incoming()
        .expect("second repair frames")
        .into_iter()
        .filter_map(|frame| {
            let MeshMessageBody::Protocol(protocol) = frame.frame.message.body else {
                return None;
            };
            match protocol.decode::<DfsNetworkMessage>().expect("decode DFS") {
                DfsNetworkMessage::RepairReplicaChunk(chunk) => Some(chunk),
                _ => None,
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        second_chunks.len(),
        (total_chunks - DFS_STAGE_CHUNK_WINDOW_SIZE) as usize
    );
    assert_eq!(
        second_chunks
            .first()
            .expect("first resumed chunk")
            .chunk_index,
        DFS_STAGE_CHUNK_WINDOW_SIZE
    );
    assert_eq!(
        second_chunks
            .last()
            .expect("last second-window chunk")
            .chunk_index,
        total_chunks - 1
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn chunked_fetch_response_to_unroutable_requester_stops_after_first_unsent_chunk() {
    let root = temp_root("chunked-fetch-response-unroutable-stops");
    let source = NodeId::from_legacy_named("node_chunk_fetch_source").expect("node");
    let requester = NodeId::from_legacy_named("node_chunk_fetch_requester").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_chunk_fetch").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = (0..(DFS_BLOB_CHUNK_BYTES + 8192))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(source),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    dfs.send_fetch_blob_response_chunks(
        &requester,
        &DfsFetchBlobCommand {
            request_id: "dfs_net_req_fetch_response_unroutable".to_string(),
            intent_id: "dfs_intent_fetch_response_unroutable".to_string(),
            object_id,
            version,
            content_hash: content_hash(&bytes),
            start_chunk_index: 0,
            max_chunks: 0,
        },
        &bytes,
    )
    .expect("send fetch chunks");

    assert_eq!(
        dfs.next_frame_id, 2,
        "large fetch response should stop after the first unroutable chunk send"
    );
    assert_eq!(dfs.next_message_id, 2);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn chunked_fetch_retry_resumes_from_received_prefix() {
    let root = temp_root("chunked-fetch-retry-resumes-prefix");
    let owner = NodeId::from_legacy_named("node_fetch_resume_owner").expect("node");
    let source = NodeId::from_legacy_named("node_fetch_resume_source").expect("node");
    let replica = NodeId::from_legacy_named("node_fetch_resume_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_fetch_resume").expect("object");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let base_version = DfsVersion::from_legacy_named("version_1").expect("version");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let total_chunks = DFS_FETCH_CHUNK_WINDOW_SIZE + 1;
    let bytes_len = DFS_BLOB_CHUNK_BYTES * (total_chunks as usize - 1) + 17;
    let bytes = (0..bytes_len)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    );
    let mut source_dfs = NetworkedDfsService::new(
        network.add_node(source.clone()),
        LocalContentStore::new(root.join("source-content")).expect("source content"),
    );
    let manifest = source_dfs
        .content
        .put_object_version(object_id.as_str(), version.as_str(), &bytes)
        .expect("source content");

    let intent = owner_dfs
        .coordinator
        .create_intent(
            file_system_id.clone(),
            object_id.clone(),
            Some(base_version),
            version.clone(),
            owner.clone(),
            vec![owner.clone(), source.clone(), replica.clone()],
            DfsReplicationPolicy::new(3, 3).expect("policy"),
        )
        .expect("intent");
    owner_dfs
        .coordinator
        .record_durable_receipt(&intent.intent_id, source.clone())
        .expect("source receipt");
    owner_dfs.pending_writes.insert(
        intent.intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: manifest.content_hash.clone(),
            size_bytes: manifest.size_bytes,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    let intent_snapshot = owner_dfs
        .coordinator
        .intent(&intent.intent_id)
        .cloned()
        .expect("intent");
    let pending_snapshot = owner_dfs
        .pending_writes
        .get(&intent.intent_id)
        .cloned()
        .expect("pending write");
    assert!(
        owner_dfs
            .request_stage_blob_from_alternate(&intent_snapshot, &pending_snapshot)
            .expect("initial fetch request")
    );
    let first_request = source_dfs
        .transport
        .drain_incoming()
        .expect("source fetch request")
        .into_iter()
        .find_map(|frame| {
            let MeshMessageBody::Protocol(protocol) = frame.frame.message.body else {
                return None;
            };
            match protocol.decode::<DfsNetworkMessage>().expect("decode DFS") {
                DfsNetworkMessage::FetchBlob(command) => Some(command),
                _ => None,
            }
        })
        .expect("fetch command");
    assert_eq!(first_request.start_chunk_index, 0);
    assert_eq!(first_request.max_chunks, DFS_FETCH_CHUNK_WINDOW_SIZE);

    source_dfs
        .handle_fetch_blob(owner.clone(), first_request.clone())
        .expect("source sends first chunk stream");
    let mut first_responses = owner_dfs
        .transport
        .drain_incoming()
        .expect("owner first fetch chunks")
        .into_iter()
        .filter_map(|frame| {
            let MeshMessageBody::Protocol(protocol) = frame.frame.message.body else {
                return None;
            };
            match protocol.decode::<DfsNetworkMessage>().expect("decode DFS") {
                DfsNetworkMessage::FetchBlobResponseChunk(chunk) => Some(chunk),
                _ => None,
            }
        });
    let first_chunk = first_responses.next().expect("first chunk");
    assert_eq!(first_chunk.chunk_index, 0);
    assert!(
        first_responses.next().is_some(),
        "initial fetch stream should have had more chunks, which are lost in this test"
    );
    owner_dfs
        .handle_fetch_blob_response_chunk(source.clone(), first_chunk)
        .expect("owner accepts first fetch chunk");
    assert_eq!(
        owner_dfs
            .pending_writes
            .get(&intent.intent_id)
            .expect("pending")
            .fetch_next_chunk_indexes
            .get(&source),
        Some(&1)
    );

    owner_dfs.repair_retry_tick = fetch_inflight_timeout_ticks();
    owner_dfs.expire_stale_fetch_requests();
    let intent_snapshot = owner_dfs
        .coordinator
        .intent(&intent.intent_id)
        .cloned()
        .expect("intent");
    let pending_snapshot = owner_dfs
        .pending_writes
        .get(&intent.intent_id)
        .cloned()
        .expect("pending write");
    assert!(
        !owner_dfs
            .request_stage_blob_from_alternate(&intent_snapshot, &pending_snapshot)
            .expect("first retry clears failed source")
    );
    let intent_snapshot = owner_dfs
        .coordinator
        .intent(&intent.intent_id)
        .cloned()
        .expect("intent");
    let pending_snapshot = owner_dfs
        .pending_writes
        .get(&intent.intent_id)
        .cloned()
        .expect("pending write");
    assert!(
        owner_dfs
            .request_stage_blob_from_alternate(&intent_snapshot, &pending_snapshot)
            .expect("second retry sends resumed fetch")
    );
    let retry_request = source_dfs
        .transport
        .drain_incoming()
        .expect("source retry fetch request")
        .into_iter()
        .find_map(|frame| {
            let MeshMessageBody::Protocol(protocol) = frame.frame.message.body else {
                return None;
            };
            match protocol.decode::<DfsNetworkMessage>().expect("decode DFS") {
                DfsNetworkMessage::FetchBlob(command) => Some(command),
                _ => None,
            }
        })
        .expect("retry fetch command");
    assert_eq!(
        retry_request.request_id, first_request.request_id,
        "fetch retry should preserve the request id so the partial assembler can continue"
    );
    assert_eq!(retry_request.start_chunk_index, 1);
    assert_eq!(retry_request.max_chunks, DFS_FETCH_CHUNK_WINDOW_SIZE);

    source_dfs
        .handle_fetch_blob(owner.clone(), retry_request)
        .expect("source sends resumed chunks");
    let resumed_chunks = owner_dfs
        .transport
        .drain_incoming()
        .expect("owner resumed fetch chunks")
        .into_iter()
        .filter_map(|frame| {
            let MeshMessageBody::Protocol(protocol) = frame.frame.message.body else {
                return None;
            };
            match protocol.decode::<DfsNetworkMessage>().expect("decode DFS") {
                DfsNetworkMessage::FetchBlobResponseChunk(chunk) => Some(chunk),
                _ => None,
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(resumed_chunks.len(), (total_chunks - 1) as usize);
    let mut fetched = false;
    for chunk in resumed_chunks {
        if let Some(event) = owner_dfs
            .handle_fetch_blob_response_chunk(source.clone(), chunk)
            .expect("owner accepts resumed fetch chunk")
        {
            match event {
                NetworkedDfsServiceEvent::BlobFetched { .. } => fetched = true,
                NetworkedDfsServiceEvent::BlobFetchRejected { reason, .. } => {
                    panic!("resumed fetch was rejected: {reason}");
                }
                _ => {}
            }
        }
    }
    assert!(fetched, "resumed fetch should complete the object");

    assert_eq!(
        owner_dfs
            .content
            .read_object_version(object_id.as_str(), version.as_str())
            .expect("fetched object")
            .1,
        bytes
    );
    assert!(
        owner_dfs
            .pending_writes
            .get(&intent.intent_id)
            .expect("pending")
            .fetch_next_chunk_indexes
            .is_empty(),
        "completed fetch should clear resume state"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn chunked_fetch_response_windows_and_continues() {
    let root = temp_root("chunked-fetch-response-windows");
    let owner = NodeId::from_legacy_named("node_fetch_window_owner").expect("node");
    let source = NodeId::from_legacy_named("node_fetch_window_source").expect("node");
    let replica = NodeId::from_legacy_named("node_fetch_window_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_fetch_window").expect("object");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let base_version = DfsVersion::from_legacy_named("version_1").expect("version");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let total_chunks = DFS_FETCH_CHUNK_WINDOW_SIZE + 2;
    let bytes_len = DFS_BLOB_CHUNK_BYTES * (total_chunks as usize - 1) + 17;
    let bytes = (0..bytes_len)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    );
    let mut source_dfs = NetworkedDfsService::new(
        network.add_node(source.clone()),
        LocalContentStore::new(root.join("source-content")).expect("source content"),
    );
    let manifest = source_dfs
        .content
        .put_object_version(object_id.as_str(), version.as_str(), &bytes)
        .expect("source content");
    let intent = owner_dfs
        .coordinator
        .create_intent(
            file_system_id,
            object_id.clone(),
            Some(base_version),
            version.clone(),
            owner.clone(),
            vec![owner.clone(), source.clone(), replica.clone()],
            DfsReplicationPolicy::new(3, 3).expect("policy"),
        )
        .expect("intent");
    owner_dfs
        .coordinator
        .record_durable_receipt(&intent.intent_id, source.clone())
        .expect("source receipt");
    owner_dfs.pending_writes.insert(
        intent.intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: manifest.content_hash.clone(),
            size_bytes: manifest.size_bytes,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    let intent_snapshot = owner_dfs
        .coordinator
        .intent(&intent.intent_id)
        .cloned()
        .expect("intent");
    let pending_snapshot = owner_dfs
        .pending_writes
        .get(&intent.intent_id)
        .cloned()
        .expect("pending write");
    assert!(
        owner_dfs
            .request_stage_blob_from_alternate(&intent_snapshot, &pending_snapshot)
            .expect("initial fetch request")
    );
    let first_request = drain_dfs_messages(&mut source_dfs.transport)
        .into_iter()
        .find_map(|message| match message {
            DfsNetworkMessage::FetchBlob(command) => Some(command),
            _ => None,
        })
        .expect("first fetch request");
    assert_eq!(first_request.start_chunk_index, 0);
    assert_eq!(first_request.max_chunks, DFS_FETCH_CHUNK_WINDOW_SIZE);

    source_dfs
        .handle_fetch_blob(owner.clone(), first_request)
        .expect("source sends first fetch window");
    let first_window = drain_dfs_messages(&mut owner_dfs.transport)
        .into_iter()
        .filter_map(|message| match message {
            DfsNetworkMessage::FetchBlobResponseChunk(chunk) => Some(chunk),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(first_window.len(), DFS_FETCH_CHUNK_WINDOW_SIZE as usize);
    assert_eq!(first_window.first().expect("first chunk").chunk_index, 0);
    assert_eq!(
        first_window
            .last()
            .expect("last first-window chunk")
            .chunk_index,
        DFS_FETCH_CHUNK_WINDOW_SIZE - 1
    );
    for chunk in first_window {
        owner_dfs
            .handle_fetch_blob_response_chunk(source.clone(), chunk)
            .expect("owner accepts first fetch window");
    }
    assert!(
        owner_dfs
            .content
            .read_object_version(object_id.as_str(), version.as_str())
            .is_err(),
        "first window alone must not commit an incomplete object"
    );

    let next_request = drain_dfs_messages(&mut source_dfs.transport)
        .into_iter()
        .find_map(|message| match message {
            DfsNetworkMessage::FetchBlob(command) => Some(command),
            _ => None,
        })
        .expect("continuation fetch request");
    assert_eq!(next_request.start_chunk_index, DFS_FETCH_CHUNK_WINDOW_SIZE);
    assert_eq!(next_request.max_chunks, DFS_FETCH_CHUNK_WINDOW_SIZE);
    source_dfs
        .handle_fetch_blob(owner.clone(), next_request)
        .expect("source sends second fetch window");
    let second_window = drain_dfs_messages(&mut owner_dfs.transport)
        .into_iter()
        .filter_map(|message| match message {
            DfsNetworkMessage::FetchBlobResponseChunk(chunk) => Some(chunk),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(second_window.len(), 2);
    assert_eq!(
        second_window
            .first()
            .expect("first resumed chunk")
            .chunk_index,
        DFS_FETCH_CHUNK_WINDOW_SIZE
    );
    assert_eq!(
        second_window
            .last()
            .expect("last resumed chunk")
            .chunk_index,
        total_chunks - 1
    );
    for chunk in second_window {
        owner_dfs
            .handle_fetch_blob_response_chunk(source.clone(), chunk)
            .expect("owner accepts second fetch window");
    }

    assert_eq!(
        owner_dfs
            .content
            .read_object_version(object_id.as_str(), version.as_str())
            .expect("fetched object")
            .1,
        bytes
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn chunked_read_response_to_unroutable_requester_stops_after_first_unsent_chunk() {
    let root = temp_root("chunked-read-response-unroutable-stops");
    let source = NodeId::from_legacy_named("node_chunk_read_source").expect("node");
    let requester = NodeId::from_legacy_named("node_chunk_read_requester").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_chunk_read").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = (0..(DFS_BLOB_CHUNK_BYTES + 8192))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(source),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    dfs.send_networked_read_response_chunks(
        &requester,
        &DfsReadRequest {
            request_id: "dfs_net_req_read_response_unroutable".to_string(),
            object_id: object_id.clone(),
            range_offset: None,
            range_max_bytes: None,
            start_chunk_index: 0,
            max_chunks: 0,
        },
        &DfsReadData {
            object_id,
            version,
            content_hash: Some(content_hash(&bytes)),
            bytes,
        },
    )
    .expect("send read chunks");

    assert_eq!(
        dfs.next_frame_id, 2,
        "large read response should stop after the first unroutable chunk send"
    );
    assert_eq!(dfs.next_message_id, 2);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn read_current_object_uses_hash_addressed_blob_without_manifest_scan() {
    let root = temp_root("read-current-object-hash-addressed");
    let node_id = NodeId::from_legacy_named("node_hash_addressed").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hash_addressed").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"shared content-addressed bytes";
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let seed_manifest = dfs
        .content
        .put_object_version("obj_other_hash_owner", "version_1", bytes)
        .expect("seed blob");
    let mut metadata = file_metadata(node_id);
    metadata.object_id = object_id.clone();
    metadata.path = "services/hash-addressed.txt".to_string();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version.clone();
    metadata.content_hash = Some(seed_manifest.content_hash.clone());
    metadata.content_size_bytes = Some(bytes.len() as u64);
    dfs.add_object_metadata(metadata).expect("metadata");

    let outcome = dfs
        .read_local_object_with_state(&object_id, false)
        .expect("read");

    match outcome {
        NetworkedDfsReadOutcome::Found { data, freshness } => {
            assert_eq!(freshness, NetworkedDfsReadFreshness::Current);
            assert_eq!(data.object_id, object_id);
            assert_eq!(data.version, version);
            assert_eq!(
                data.content_hash.as_deref(),
                Some(seed_manifest.content_hash.as_str())
            );
            assert_eq!(data.bytes, bytes);
        }
        other => {
            panic!("expected current object bytes from hash-addressed blob, got {other:?}")
        }
    }
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn read_current_object_range_uses_bounded_hash_addressed_blob_slice() {
    let root = temp_root("read-current-object-range-hash-addressed");
    let node_id = NodeId::from_legacy_named("node_hash_range").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hash_range").expect("object");
    let version = DfsVersion::from_legacy_named("version_range").expect("version");
    let bytes = b"range-readable content-addressed bytes";
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let seed_manifest = dfs
        .content
        .put_object_version("obj_other_range_owner", "version_1", bytes)
        .expect("seed blob");
    let mut metadata = file_metadata(node_id.clone());
    metadata.object_id = object_id.clone();
    metadata.path = "/services/hash-range.txt".to_string();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version.clone();
    metadata.content_hash = Some(seed_manifest.content_hash.clone());
    metadata.content_size_bytes = Some(bytes.len() as u64);
    dfs.add_object_metadata(metadata).expect("metadata");

    let outcome = dfs
        .read_local_object_range_with_state(&object_id, 6, 8, false)
        .expect("range read");
    match outcome {
        NetworkedDfsRangeReadOutcome::Found { data, freshness } => {
            assert_eq!(freshness, NetworkedDfsReadFreshness::Current);
            assert_eq!(data.object_id, object_id);
            assert_eq!(data.version, version);
            assert_eq!(
                data.content_hash.as_deref(),
                Some(seed_manifest.content_hash.as_str())
            );
            assert_eq!(data.offset, 6);
            assert_eq!(data.total_size_bytes, bytes.len() as u64);
            assert_eq!(data.bytes, &bytes[6..14]);
        }
        other => panic!("expected current range bytes, got {other:?}"),
    }

    assert_eq!(
        dfs.posix_read_file_range("/services/hash-range.txt", 15, 7)
            .expect("posix range"),
        &bytes[15..22]
    );
    assert_eq!(
        dfs.posix_read_file_range("/services/hash-range.txt", bytes.len() as u64 + 1, 16)
            .expect("posix eof range"),
        Vec::<u8>::new()
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn missing_current_manifest_blob_returns_pending_replication() {
    let root = temp_root("read-missing-current-manifest-blob");
    let node_id = NodeId::from_legacy_named("node_missing_blob").expect("node");
    let replica = NodeId::from_legacy_named("node_missing_blob_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_missing_blob").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"missing local blob bytes";
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version.as_str(), bytes)
        .expect("seed manifest");
    let hex = manifest
        .content_hash
        .strip_prefix("blake3:")
        .expect("blake3 hash");
    std::fs::remove_file(root.join("content").join("blobs").join(&hex[..2]).join(hex))
        .expect("remove blob");
    let mut metadata = file_metadata(node_id);
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version.clone();
    metadata.current_locations = vec![replica.clone()];
    metadata.content_hash = Some(manifest.content_hash.clone());
    metadata.content_size_bytes = Some(bytes.len() as u64);
    dfs.add_object_metadata(metadata).expect("metadata");

    let outcome = dfs
        .read_local_object_with_state(&object_id, false)
        .expect("missing blob is treated as cache miss");

    assert!(matches!(
        outcome,
        NetworkedDfsReadOutcome::PendingReplication {
            object_id: pending_object_id,
            version: pending_version,
            content_hash,
            current_locations,
        } if pending_object_id == object_id
            && pending_version == version
            && content_hash.as_deref() == Some(manifest.content_hash.as_str())
            && current_locations == vec![replica]
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn reconcile_local_manifests_requires_current_blob_bytes() {
    let root = temp_root("reconcile-requires-current-blob");
    let node_id = NodeId::from_legacy_named("node_reconcile_missing_blob").expect("node");
    let remote = NodeId::from_legacy_named("node_reconcile_remote").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_reconcile_missing_blob").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"reconcile missing local bytes";
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version.as_str(), bytes)
        .expect("seed manifest");
    let hex = manifest
        .content_hash
        .strip_prefix("blake3:")
        .expect("blake3 hash");
    std::fs::remove_file(root.join("content").join("blobs").join(&hex[..2]).join(hex))
        .expect("remove blob");
    let mut metadata = file_metadata(node_id);
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version.clone();
    metadata.current_locations = vec![remote.clone()];
    metadata.content_hash = Some(manifest.content_hash.clone());
    metadata.content_size_bytes = Some(bytes.len() as u64);
    dfs.add_object_metadata(metadata).expect("metadata");

    let reconciliation = dfs
        .reconcile_local_manifests(&object_id)
        .expect("reconcile");

    assert_eq!(reconciliation.refreshed_receipts, 0);
    assert!(reconciliation.conflicts.is_empty());
    assert!(reconciliation.missing_current);
    assert!(
        dfs.metadata_backend()
            .expect("backend")
            .receipts_for_version(&object_id, &version)
            .expect("receipts")
            .is_empty()
    );
    assert_eq!(
        dfs.metadata()
            .object(&object_id)
            .expect("object")
            .current_locations,
        vec![remote]
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn commit_version_rejects_manifest_without_content_bytes() {
    let root = temp_root("commit-rejects-manifest-without-blob");
    let owner = NodeId::from_legacy_named("node_commit_missing_blob_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_commit_missing_blob_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_commit_missing_blob").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"missing commit blob bytes";
    let network = InProcessNetwork::new();
    let _owner_transport = network.add_node(owner.clone());
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica")).expect("replica content"),
    );
    replica_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version_1.clone();
    metadata.current_locations = vec![owner.clone()];
    replica_dfs
        .add_object_metadata(metadata.clone())
        .expect("metadata");
    let manifest = replica_dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("stage manifest");
    let hex = manifest
        .content_hash
        .strip_prefix("blake3:")
        .expect("blake3 hash");
    std::fs::remove_file(root.join("replica").join("blobs").join(&hex[..2]).join(hex))
        .expect("remove blob");

    let event = replica_dfs
        .handle_commit_version(
            owner,
            DfsCommitVersionCommand {
                request_id: "dfs_net_req_commit_missing_blob".to_string(),
                intent_id: "dfs_intent_commit_missing_blob".to_string(),
                file_system_id: metadata.file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: Some(metadata.version.clone()),
                version: version_2.clone(),
                content_hash: manifest.content_hash.clone(),
                object_metadata: Some(metadata),
            },
        )
        .expect("commit response");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RepairRejected {
            object_id: rejected_object_id,
            reason,
            ..
        } if rejected_object_id == object_id && reason.contains("committed content bytes are missing")
    ));
    assert!(matches!(
        replica_dfs.read_local_object_with_state(&object_id, false),
        Ok(NetworkedDfsReadOutcome::NotFound)
    ));
    let object = replica_dfs.metadata().object(&object_id).expect("object");
    assert_eq!(object.version, version_1);
    assert_eq!(object.content_hash, None);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn commit_version_conflicting_receipt_does_not_advance_visible_metadata() {
    let root = temp_root("commit-conflicting-receipt-no-visible-advance");
    let owner = NodeId::from_legacy_named("node_commit_conflict_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_commit_conflict_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_commit_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"commit must not advance over conflicting durable receipt";
    let mut dfs = networked_sifs_with_backend(&root, replica.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(metadata.clone()).expect("metadata");
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("local staged content");
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version_2,
        &replica,
        "blake3:stale-conflicting-commit-receipt",
    )
    .expect("seed conflicting receipt");

    let event = dfs
        .handle_commit_version(
            owner,
            DfsCommitVersionCommand {
                request_id: "dfs_net_req_commit_conflict".to_string(),
                intent_id: "dfs_intent_commit_conflict".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1.clone()),
                version: version_2.clone(),
                content_hash: manifest.content_hash,
                object_metadata: Some(metadata),
            },
        )
        .expect("commit conflict response");

    assert!(
        matches!(
            event,
            NetworkedDfsServiceEvent::RepairRejected {
                ref reason,
                ..
            } if reason.contains("conflicting DFS replica receipt")
        ),
        "commit with conflicting receipt must be rejected, got {event:?}"
    );
    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("visible metadata");
    assert_eq!(
        object.version, version_1,
        "rejected commit must not advance visible metadata"
    );
    let receipts = dfs
        .metadata_backend()
        .expect("backend")
        .receipts_for_version(&object_id, &version_2)
        .expect("receipts");
    assert_eq!(receipts.len(), 1);
    assert_eq!(
        receipts[0].content_hash.as_deref(),
        Some("blake3:stale-conflicting-commit-receipt")
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn commit_version_repairs_missing_local_receipt_when_backend_is_authoritative() {
    let root = temp_root("commit-repairs-missing-local-receipt");
    let owner = NodeId::from_legacy_named("node_commit_receipt_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_commit_receipt_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_commit_receipt").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"commit should publish missing local receipt";
    let mut dfs = networked_sifs_with_backend(&root, replica.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(metadata.clone()).expect("metadata");
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("local staged content");

    let event = dfs
        .handle_commit_version(
            owner,
            DfsCommitVersionCommand {
                request_id: "dfs_net_req_commit_receipt".to_string(),
                intent_id: "dfs_intent_commit_receipt".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1),
                version: version_2.clone(),
                content_hash: manifest.content_hash.clone(),
                object_metadata: Some(metadata),
            },
        )
        .expect("commit response");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::VersionCommitted {
            object_id: committed,
            ..
        } if committed == object_id
    ));
    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("visible metadata");
    assert_eq!(object.version, version_2);
    assert_eq!(
        object.content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );
    let receipts = dfs
        .metadata_backend()
        .expect("backend")
        .receipts_for_version(&object_id, &version_2)
        .expect("receipts");
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].replica_node_id, replica);
    assert_eq!(
        receipts[0].content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stale_read_skips_missing_stale_blobs() {
    let root = temp_root("read-stale-skips-missing-blob");
    let node_id = NodeId::from_legacy_named("node_stale_skip").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stale_skip").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let readable_stale = dfs
        .content
        .put_object_version(object_id.as_str(), version_1.as_str(), b"readable stale")
        .expect("readable stale");
    let missing_stale = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), b"missing stale")
        .expect("missing stale");
    let hex = missing_stale
        .content_hash
        .strip_prefix("blake3:")
        .expect("blake3 hash");
    std::fs::remove_file(root.join("content").join("blobs").join(&hex[..2]).join(hex))
        .expect("remove missing stale blob");
    let mut metadata = file_metadata(node_id);
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version_3;
    metadata.content_hash = Some(content_hash(b"unavailable current"));
    metadata.content_size_bytes = Some(b"unavailable current".len() as u64);
    dfs.add_object_metadata(metadata).expect("metadata");

    let outcome = dfs
        .read_local_object_with_state(&object_id, true)
        .expect("stale read skips missing blob");

    assert!(matches!(
        outcome,
        NetworkedDfsReadOutcome::Found {
            data,
            freshness: NetworkedDfsReadFreshness::Stale { .. },
        } if data.version == version_1
            && data.content_hash.as_deref() == Some(readable_stale.content_hash.as_str())
            && data.bytes == b"readable stale"
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn local_owned_write_routes_to_owner() {
    let owner = NodeId::from_legacy_named("node_desktop").expect("node");
    let remote = NodeId::from_legacy_named("node_mobile").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_file").expect("object");
    let mut store = DfsMetadataStore::new();
    store.add_file_system(file_system(owner.clone()));
    store
        .add_object(DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id,
            path: "src/lib.rs".to_string(),
            kind: DfsObjectKind::File,
            owner_node_id: owner.clone(),
            current_locations: vec![owner.clone()],
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named("version_1").expect("version"),
            policy: DfsObjectPolicy::LocalOwned,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        })
        .expect("add object");

    let route = store
        .route_write(
            DfsWriteRequest {
                file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
                object_id,
                target_version: DfsVersion::from_legacy_named("version_2").expect("version"),
                requesting_node_id: remote.clone(),
            },
            &remote,
        )
        .expect("route write");

    assert_eq!(
        route.action,
        WriteRouteAction::RouteToOwner {
            owner_node_id: owner
        }
    );
}

#[test]
fn sifs_recursive_clear_survives_metadata_backend_reopen() {
    let root = temp_root("sifs-recursive-clear-reopen");
    let node_id = NodeId::from_legacy_named("node_sifs_clear").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_clear/tmp/0/session-cache.txt";
    let clear_root = "/services/sandbox/instances/sandbox_clear/tmp/0";

    {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(clear_root).expect("mkdir");
        dfs.posix_write_file(
            file_path,
            b"tmp",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write");
        assert_eq!(dfs.posix_read_file(file_path).expect("read"), b"tmp");
        dfs.posix_clear_dir_recursive(clear_root)
            .expect("clear recursive");
        assert!(matches!(
            dfs.posix_read_file(file_path),
            Err(DfsPosixError::NotFound { .. })
        ));
    }

    {
        let dfs = networked_sifs_with_backend(&root, node_id.clone());
        assert!(matches!(
            dfs.posix_read_file(file_path),
            Err(DfsPosixError::NotFound { .. })
        ));
    }

    {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(clear_root)
            .expect("recreate cleared root");
        dfs.posix_write_file(
            file_path,
            b"fresh",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("rewrite after clear");
        assert_eq!(
            dfs.posix_read_file(file_path).expect("fresh read"),
            b"fresh"
        );
    }

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_recursive_clear_bounded_job_resumes_after_reopen() {
    let root = temp_root("sifs-recursive-clear-bounded-job");
    let node_id = NodeId::from_legacy_named("node_sifs_clear_job").expect("node");
    let clear_root = "/services/sandbox/instances/sandbox_clear_job/tmp/0";
    let nested_dir = format!("{clear_root}/nested/deeper");
    let files = [
        format!("{clear_root}/root-cache.txt"),
        format!("{clear_root}/nested/cache-a.txt"),
        format!("{nested_dir}/cache-b.txt"),
        format!("{nested_dir}/cache-c.txt"),
    ];
    let job_id = {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(&nested_dir).expect("mkdir nested");
        for path in &files {
            dfs.posix_write_file(
                path,
                path.as_bytes(),
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("write file");
        }

        let job = dfs
            .posix_clear_dir_recursive_bounded(clear_root, 1)
            .expect("first bounded clear batch");
        assert_eq!(job.status, DfsNamespaceJobStatus::Running);
        assert!(
            !job.pending_frames.is_empty(),
            "a one-step batch should persist remaining directory work"
        );
        assert!(
            dfs.metadata_backend
                .as_ref()
                .expect("backend")
                .get_namespace_job(&job.job_id)
                .expect("get persisted job")
                .is_some(),
            "bounded recursive clear progress should be durable"
        );
        job.job_id
    };

    {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        let mut job = dfs
            .posix_resume_namespace_job_bounded(&job_id, 2)
            .expect("resume bounded clear after reopen");
        while job.status != DfsNamespaceJobStatus::Completed {
            job = dfs
                .posix_resume_namespace_job_bounded(&job_id, 2)
                .expect("resume next batch");
        }
        assert!(job.processed_object_count >= files.len() as u64);
        for path in &files {
            assert!(matches!(
                dfs.posix_read_file(path),
                Err(DfsPosixError::NotFound { .. })
            ));
        }
        assert!(
            dfs.posix_list_dir(clear_root)
                .expect("cleared root list")
                .is_empty()
        );
    }

    {
        let dfs = networked_sifs_with_backend(&root, node_id);
        let job = dfs
            .metadata_backend
            .as_ref()
            .expect("backend")
            .get_namespace_job(&job_id)
            .expect("get completed job")
            .expect("job");
        assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
        assert!(job.completed_at.is_some());
        for path in &files {
            assert!(matches!(
                dfs.posix_read_file(path),
                Err(DfsPosixError::NotFound { .. })
            ));
        }
    }

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_namespace_job_maintenance_marks_invalid_job_failed() {
    let root = temp_root("sifs-namespace-job-maintenance-failed");
    let node_id = NodeId::from_legacy_named("node_sifs_namespace_job_maintenance").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, node_id);
    let now = time::OffsetDateTime::now_utc();
    let job = DfsNamespaceJobRecord {
        job_id: "invalid-clear-job".to_string(),
        file_system_id: sifs_file_system_id(),
        kind: DfsNamespaceJobKind::ClearDirectoryRecursive,
        status: DfsNamespaceJobStatus::Running,
        root_path: "/services/sandbox/instances/invalid-clear/work".to_string(),
        target_path: None,
        pending_frames: vec![DfsNamespaceJobFrame::RenameDirectoryStart {
            source_object_id: DfsObjectId::generate(),
        }],
        processed_object_count: 0,
        created_at: now,
        updated_at: now,
        completed_at: None,
        failure_reason: None,
    };
    dfs.metadata_backend
        .as_mut()
        .expect("backend")
        .put_namespace_job(&job)
        .expect("put invalid job");

    let report = dfs
        .posix_resume_namespace_jobs_bounded(4, 4)
        .expect("namespace job maintenance");

    assert_eq!(report.scanned_job_count, 1);
    assert_eq!(report.resumed_job_count, 1);
    assert_eq!(report.failed_job_count, 1);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].job_id, job.job_id);
    let failed = dfs
        .metadata_backend
        .as_ref()
        .expect("backend")
        .get_namespace_job(&job.job_id)
        .expect("get failed job")
        .expect("failed job");
    assert_eq!(failed.status, DfsNamespaceJobStatus::Failed);
    assert!(failed.failure_reason.as_deref().is_some_and(|reason| {
        reason.contains("invalid DFS path") || reason.contains("invalid-clear-job")
    }));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_recursive_clear_preserves_hardlinks_outside_cleared_tree() {
    let root = temp_root("sifs-recursive-clear-hardlink-outside");
    let node_id = NodeId::from_legacy_named("node_sifs_clear_hardlink").expect("node");
    let clear_root = "/services/sandbox/instances/sandbox_clear_hardlink/tmp/0";
    let file_path = "/services/sandbox/instances/sandbox_clear_hardlink/tmp/0/cache.txt";
    let outside_alias = "/services/sandbox/instances/sandbox_clear_hardlink/work/cache-alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(clear_root).expect("mkdir clear root");
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_clear_hardlink/work")
        .expect("mkdir work");
    dfs.posix_write_file(
        file_path,
        b"cache",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write cache");
    dfs.posix_set_times(file_path, Some(12_345), Some(67_890))
        .expect("old file times");
    let before = dfs.posix_stat(file_path).expect("file stat");
    let object_id = before.object_id.clone();
    dfs.posix_hardlink(file_path, outside_alias)
        .expect("hardlink outside");

    dfs.posix_clear_dir_recursive(clear_root)
        .expect("clear recursive");

    assert!(matches!(
        dfs.posix_stat(file_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    let alias_stat = dfs.posix_stat(outside_alias).expect("alias survives");
    assert_eq!(alias_stat.object_id, object_id);
    assert_eq!(alias_stat.link_count, 1);
    assert_eq!(
        alias_stat.modified_at_unix_nanos, before.modified_at_unix_nanos,
        "clearing a hardlinked path must not change file content mtime"
    );
    assert!(
        alias_stat.changed_at_unix_nanos > before.changed_at_unix_nanos,
        "promoting a surviving hardlink alias must advance ctime because the canonical path changed"
    );
    assert_ne!(
        alias_stat.version, before.version,
        "promoting a surviving hardlink alias must advance the metadata version"
    );
    assert_eq!(
        dfs.posix_read_file(outside_alias).expect("read alias"),
        b"cache"
    );

    let reloaded = networked_sifs_with_backend(&root, node_id);
    let alias_stat = reloaded
        .posix_stat(outside_alias)
        .expect("reloaded alias survives");
    assert_eq!(alias_stat.object_id, object_id);
    assert_eq!(alias_stat.link_count, 1);
    assert_eq!(
        reloaded
            .posix_read_file(outside_alias)
            .expect("reloaded read alias"),
        b"cache"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_recursive_clear_follows_directory_symlink_argument() {
    let root = temp_root("sifs-recursive-clear-directory-symlink");
    let node_id = NodeId::from_legacy_named("node_sifs_clear_symlink").expect("node");
    let base = "/services/sandbox/instances/sandbox_clear_symlink";
    let real_dir = format!("{base}/tmp/0");
    let link_dir = format!("{base}/work/current-tmp");
    let file_path = format!("{real_dir}/session-cache.txt");

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(&real_dir).expect("mkdir real tmp");
    dfs.posix_mkdir_all(&format!("{base}/work"))
        .expect("mkdir work");
    dfs.posix_write_file(
        &file_path,
        b"cache",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write cache");
    dfs.posix_symlink(
        "../tmp/0",
        &link_dir,
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("symlink directory");

    dfs.posix_clear_dir_recursive(&link_dir)
        .expect("clear via directory symlink");

    assert!(matches!(
        dfs.posix_read_file(&file_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_lstat(&link_dir)
            .expect("link survives clear")
            .kind,
        DfsObjectKind::Symlink
    );
    assert!(
        dfs.posix_list_dir(&real_dir)
            .expect("real dir survives")
            .is_empty()
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_recursive_clear_is_scoped_to_target_subtree() {
    let root = temp_root("sifs-recursive-clear-scoped-subtree");
    let node_id = NodeId::from_legacy_named("node_sifs_clear_scoped").expect("node");
    let target_root = "/services/sandbox/instances/sandbox_clear_scoped/tmp/0";
    let target_file = "/services/sandbox/instances/sandbox_clear_scoped/tmp/0/nested/cache.txt";
    let sibling_file = "/services/sandbox/instances/sandbox_clear_scoped/tmp/1/nested/cache.txt";
    let unrelated_file =
        "/services/sandbox/instances/sandbox_clear_scoped/work/unrelated/cache.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(&format!("{target_root}/nested"))
        .expect("mkdir target");
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_clear_scoped/tmp/1/nested")
        .expect("mkdir sibling");
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_clear_scoped/work/unrelated")
        .expect("mkdir unrelated");
    for path in [target_file, sibling_file, unrelated_file] {
        dfs.posix_write_file(
            path,
            path.as_bytes(),
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write file");
    }

    dfs.posix_clear_dir_recursive(target_root)
        .expect("clear target subtree");

    assert!(matches!(
        dfs.posix_read_file(target_file),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_read_file(sibling_file).expect("sibling survives"),
        sibling_file.as_bytes()
    );
    assert_eq!(
        dfs.posix_read_file(unrelated_file)
            .expect("unrelated survives"),
        unrelated_file.as_bytes()
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_self_only_eventual_write_uses_local_fast_path() {
    let root = temp_root("sifs-self-only-eventual-fast-path");
    let node_id = NodeId::from_legacy_named("node_sifs_fast_path").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_fast/work/site.py";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_fast/work")
        .expect("mkdir");
    let stat = dfs
        .posix_write_file(
            file_path,
            b"print('fast path')\n",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write");

    assert_eq!(stat.size_bytes, Some(19));
    assert_eq!(
        dfs.posix_read_file(file_path).expect("read"),
        b"print('fast path')\n"
    );
    assert!(
        dfs.pending_writes.is_empty(),
        "self-only eventual writes should not leave distributed intents"
    );
    assert!(dfs.coordinator.repair_jobs().is_empty());

    let journal_count = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();
    assert!(
        journal_count < 64,
        "self-only write should not emit per-file distributed intent/receipt/commit journals"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn visible_metadata_publication_is_idempotent() {
    let root = temp_root("visible-metadata-publication-idempotent");
    let node_id = NodeId::from_legacy_named("node_visible_metadata_idempotent").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_meta/work/site.py";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_meta/work")
        .expect("mkdir");
    let stat = dfs
        .posix_write_file(
            file_path,
            b"print('metadata')\n",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write");
    let object = dfs
        .current_read_object_metadata(&stat.object_id)
        .expect("current object");
    let journal_count_before = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();

    dfs.put_visible_object_in_metadata_backend(&object)
        .expect("repeat visible object");
    dfs.put_visible_object_in_metadata_backend(&object)
        .expect("repeat visible object again");
    dfs.put_object_version_size_in_metadata_backend(&object, stat.size_bytes)
        .expect("repeat visible version size");

    let journal_count_after = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();
    assert_eq!(
        journal_count_after, journal_count_before,
        "re-publishing unchanged object/version metadata should not emit journal entries"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn duplicate_commit_metadata_application_is_idempotent() {
    let root = temp_root("duplicate-commit-metadata-idempotent");
    let node_id = NodeId::from_legacy_named("node_duplicate_commit_meta").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_duplicate_commit_meta").expect("object");
    let bytes = b"duplicate commit bytes";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let mut metadata = file_metadata(node_id.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.last_modified_by_event_id =
        Some(EventId::from_legacy_named("evt_duplicate_commit_base").expect("event"));
    let version = metadata.version.clone();
    let hash = dfs
        .put_local_object_version(metadata, bytes)
        .expect("local version");
    let before = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object");
    let journal_count_before = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();

    let committed = dfs
        .commit_local_metadata(&object_id, &version, &hash, dfs_now_unix_nanos())
        .expect("duplicate commit metadata");
    dfs.put_visible_object_in_metadata_backend(&committed)
        .expect("duplicate visible publish");

    let after = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object after duplicate commit");
    assert_eq!(
        after.last_modified_by_event_id,
        before.last_modified_by_event_id
    );
    assert_eq!(after.modified_at_unix_nanos, before.modified_at_unix_nanos);
    assert_eq!(after.changed_at_unix_nanos, before.changed_at_unix_nanos);
    let journal_count_after = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();
    assert_eq!(
        journal_count_after, journal_count_before,
        "duplicate committed metadata application must not emit visible metadata churn"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn duplicate_committed_intent_application_does_not_recommit_visible_version() {
    let root = temp_root("duplicate-committed-intent-idempotent");
    let owner = NodeId::from_legacy_named("node_duplicate_intent_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_duplicate_intent_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_duplicate_intent_commit").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"duplicate committed intent bytes";

    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version_1.clone();
    dfs.add_object_metadata(metadata).expect("object");
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("manifest");
    let intent = dfs
        .coordinator
        .create_intent(
            sifs_file_system_id(),
            object_id.clone(),
            Some(version_1),
            version_2.clone(),
            owner.clone(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("intent");
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version_2,
        &owner,
        &manifest.content_hash,
    )
    .expect("backend receipt");
    dfs.coordinator
        .record_durable_receipt(&intent.intent_id, owner.clone())
        .expect("commit with repair");
    dfs.pending_writes.insert(
        intent.intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version_2,
            content_hash: manifest.content_hash.clone(),
            size_bytes: manifest.size_bytes,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    dfs.apply_committed_intent(&intent.intent_id)
        .expect("first commit apply");
    let before = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object after first apply");
    let journal_count_before = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();

    dfs.apply_committed_intent(&intent.intent_id)
        .expect("duplicate commit apply");

    let after = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object after duplicate apply");
    assert_eq!(after, before);
    let journal_count_after = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();
    assert_eq!(
        journal_count_after, journal_count_before,
        "reapplying an already-visible committed intent must not append a commit journal entry"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn committed_intent_repairs_already_current_stale_locations() {
    let root = temp_root("committed-intent-repairs-stale-locations");
    let owner = NodeId::from_legacy_named("node_stale_location_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stale_location_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stale_location_commit").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"already current with stale locations";

    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version_1.clone();
    dfs.add_object_metadata(metadata.clone())
        .expect("base visible object");
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("manifest");
    let intent = dfs
        .coordinator
        .create_intent(
            sifs_file_system_id(),
            object_id.clone(),
            Some(version_1),
            version_2.clone(),
            owner.clone(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("intent");
    let mut stale_visible = metadata;
    stale_visible.version = version_2.clone();
    stale_visible.current_locations = vec![owner.clone()];
    stale_visible.content_hash = Some(manifest.content_hash.clone());
    stale_visible.content_size_bytes = Some(manifest.size_bytes);
    dfs.add_object_metadata(stale_visible)
        .expect("stale visible object");
    for node_id in [&owner, &replica] {
        dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version_2,
            node_id,
            &manifest.content_hash,
        )
        .expect("backend receipt");
        dfs.coordinator
            .record_durable_receipt(&intent.intent_id, node_id.clone())
            .expect("durable receipt");
    }
    dfs.pending_writes.insert(
        intent.intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version_2,
            content_hash: manifest.content_hash.clone(),
            size_bytes: manifest.size_bytes,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    dfs.apply_committed_intent(&intent.intent_id)
        .expect("repair stale visible locations");

    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object");
    let current_locations = object
        .current_locations
        .into_iter()
        .collect::<BTreeSet<_>>();
    let expected_locations = [owner.clone(), replica.clone()].into_iter().collect();
    assert_eq!(
        current_locations, expected_locations,
        "already-current commits must still repair stale durable replica locations"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn duplicate_metadata_receipt_publication_is_idempotent() {
    let root = temp_root("metadata-receipt-publication-idempotent");
    let owner = NodeId::from_legacy_named("node_receipt_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_receipt_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_receipt_idempotent").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let content_hash = "blake3:receipt-idempotent";

    let mut dfs = networked_sifs_with_backend(&root, owner);
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version,
        &replica,
        content_hash,
    )
    .expect("first receipt");
    let journal_count_before = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();

    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version,
        &replica,
        content_hash,
    )
    .expect("duplicate receipt");

    let journal_count_after = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();
    assert_eq!(
        journal_count_after, journal_count_before,
        "duplicate replica receipts for the same content hash should not emit journal entries"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn conflicting_metadata_receipt_from_same_replica_is_rejected() {
    let root = temp_root("metadata-receipt-conflicting-hash-rejected");
    let owner = NodeId::from_legacy_named("node_receipt_conflict_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_receipt_conflict_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_receipt_conflict").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let content_hash = "blake3:receipt-original";
    let conflicting_hash = "blake3:receipt-conflicting";

    let mut dfs = networked_sifs_with_backend(&root, owner);
    assert!(
        dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version,
            &replica,
            content_hash,
        )
        .expect("first receipt")
    );
    let journal_count_before = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();

    let error = dfs
        .put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version,
            &replica,
            conflicting_hash,
        )
        .expect_err("conflicting receipt must be rejected");

    assert!(
        error
            .to_string()
            .contains("conflicting DFS replica receipt"),
        "unexpected error: {error}"
    );
    let receipts = dfs
        .metadata_backend()
        .expect("backend")
        .receipts_for_version(&object_id, &version)
        .expect("receipts");
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].content_hash.as_deref(), Some(content_hash));
    let journal_count_after = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();
    assert_eq!(
        journal_count_after, journal_count_before,
        "rejected conflicting receipt must not overwrite receipt metadata or emit journals"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_symlink_preserves_target_as_object_content() {
    let root = temp_root("sifs-posix-symlink");
    let node_id = NodeId::from_legacy_named("node_sifs_symlink").expect("node");
    let link_path = "/services/sandbox/instances/sandbox_link/work/lib";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_link/work")
        .expect("mkdir");
    let stat = dfs
        .posix_symlink(
            "../shared/lib",
            link_path,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("symlink");

    assert_eq!(stat.kind, DfsObjectKind::Symlink);
    assert_eq!(stat.size_bytes, Some("../shared/lib".len() as u64));
    assert_eq!(
        dfs.posix_readlink(link_path).expect("readlink"),
        "../shared/lib"
    );
    dfs.posix_set_symlink_times(link_path, Some(12_345), Some(67_890))
        .expect("old symlink times");
    let before_readlink = dfs.posix_lstat(link_path).expect("before readlink lstat");
    assert_eq!(
        dfs.posix_readlink_update_access(link_path)
            .expect("readlink update access"),
        "../shared/lib"
    );
    let stat = dfs.posix_lstat(link_path).expect("symlink lstat");
    assert!(stat.accessed_at_unix_nanos > 12_345);
    assert_eq!(stat.modified_at_unix_nanos, 67_890);
    assert_eq!(
        stat.changed_at_unix_nanos, before_readlink.changed_at_unix_nanos,
        "readlink access update must not advance symlink ctime"
    );
    assert_ne!(
        stat.version, before_readlink.version,
        "readlink access update must advance the DFS metadata version so it persists across distributed reconciliation"
    );
    assert!(matches!(
        dfs.posix_symlink(
            "other",
            link_path,
            DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()])
        ),
        Err(DfsPosixError::AlreadyExists { .. })
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_trailing_slash_requires_directory() {
    let root = temp_root("sifs-posix-trailing-slash-directory");
    let node_id = NodeId::from_legacy_named("node_sifs_trailing_slash").expect("node");
    let work = "/services/sandbox/instances/sandbox_trailing/work";
    let real_dir = "/services/sandbox/instances/sandbox_trailing/work/real-dir";
    let file_path = "/services/sandbox/instances/sandbox_trailing/work/file.txt";
    let link_dir = "/services/sandbox/instances/sandbox_trailing/work/link-dir";
    let link_file = "/services/sandbox/instances/sandbox_trailing/work/link-file";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(real_dir).expect("mkdir real dir");
    dfs.posix_write_file(
        file_path,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write file");
    dfs.posix_symlink(
        "real-dir",
        link_dir,
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("symlink dir");
    dfs.posix_symlink(
        "file.txt",
        link_file,
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("symlink file");

    assert_eq!(
        dfs.posix_lstat(link_dir).expect("plain lstat").kind,
        DfsObjectKind::Symlink
    );
    assert_eq!(
        dfs.posix_lstat(&format!("{link_dir}/"))
            .expect("trailing slash lstat follows directory symlink")
            .kind,
        DfsObjectKind::Directory
    );
    assert!(matches!(
        dfs.posix_read_file(&format!("{file_path}/")),
        Err(DfsPosixError::NotDirectory { .. })
    ));
    assert!(matches!(
        dfs.posix_read_file(&format!("{link_file}/")),
        Err(DfsPosixError::NotDirectory { .. })
    ));
    assert!(matches!(
        dfs.posix_readlink(&format!("{link_dir}/")),
        Err(DfsPosixError::NotSymlink { .. })
    ));
    assert!(matches!(
        dfs.posix_rename(&format!("{file_path}/"), &format!("{work}/renamed.txt")),
        Err(DfsPosixError::NotDirectory { .. })
    ));
    assert!(matches!(
        dfs.posix_rename(file_path, &format!("{work}/missing-destination/")),
        Err(DfsPosixError::NotDirectory { .. })
    ));
    assert!(matches!(
        dfs.posix_hardlink(&format!("{file_path}/"), &format!("{work}/file-alias.txt")),
        Err(DfsPosixError::NotDirectory { .. })
    ));
    assert!(matches!(
        dfs.posix_hardlink(file_path, &format!("{work}/missing-alias/")),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(matches!(
        dfs.posix_symlink(
            "file.txt",
            &format!("{work}/missing-link/"),
            DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()])
        ),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(matches!(
        dfs.posix_symlink(
            "file.txt",
            &format!("{real_dir}/"),
            DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()])
        ),
        Err(DfsPosixError::AlreadyExists { .. })
    ));
    assert!(matches!(
        dfs.posix_unlink(&format!("{link_dir}/")),
        Err(DfsPosixError::IsDirectory { .. })
    ));
    assert_eq!(
        dfs.posix_lstat(link_dir)
            .expect("symlink survives failed unlink")
            .kind,
        DfsObjectKind::Symlink
    );
    assert_eq!(
        dfs.posix_stat(real_dir).expect("target survives").kind,
        DfsObjectKind::Directory
    );
    assert_eq!(
        dfs.posix_list_dir(&format!("{work}/"))
            .expect("list with trailing slash")
            .len(),
        4
    );
    dfs.posix_mkdir_all(&format!("{work}/rename-source-dir"))
        .expect("mkdir rename source dir");
    dfs.posix_rename(
        &format!("{work}/rename-source-dir"),
        &format!("{work}/renamed-dir/"),
    )
    .expect("rename directory to trailing slash missing path");
    assert_eq!(
        dfs.posix_stat(&format!("{work}/renamed-dir"))
            .expect("renamed directory")
            .kind,
        DfsObjectKind::Directory
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rmdir_reports_directory_not_empty() {
    let root = temp_root("sifs-posix-rmdir-not-empty");
    let node_id = NodeId::from_legacy_named("node_sifs_rmdir").expect("node");
    let dir_path = "/services/sandbox/instances/sandbox_rmdir/work";
    let file_path = "/services/sandbox/instances/sandbox_rmdir/work/file.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(dir_path).expect("mkdir");
    dfs.posix_write_file(
        file_path,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write");

    assert!(matches!(
        dfs.posix_rmdir(dir_path),
        Err(DfsPosixError::DirectoryNotEmpty { .. })
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_empty_directory_checks_are_bounded_for_large_directories() {
    let root = temp_root("sifs-posix-bounded-empty-dir-check");
    let node_id = NodeId::from_legacy_named("node_sifs_bounded_empty_dir").expect("node");
    let source = "/services/sandbox/instances/sandbox_bounded_empty_dir/work/source";
    let nonempty = "/services/sandbox/instances/sandbox_bounded_empty_dir/work/nonempty";

    let mut dfs = networked_sifs_with_backend(&root, node_id);
    dfs.posix_mkdir_all(source).expect("mkdir source");
    for index in 0..160 {
        dfs.posix_mkdir_all(&format!("{nonempty}/child-{index:04}"))
            .expect("mkdir child");
    }

    assert!(matches!(
        dfs.posix_rmdir(nonempty),
        Err(DfsPosixError::DirectoryNotEmpty { .. })
    ));
    assert!(matches!(
        dfs.posix_rename(source, nonempty),
        Err(DfsPosixError::DirectoryNotEmpty { .. })
    ));
    assert!(dfs.posix_stat(source).is_ok());

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_stat_reports_directory_link_counts() {
    let root = temp_root("sifs-posix-link-count");
    let node_id = NodeId::from_legacy_named("node_sifs_link_count").expect("node");
    let parent = "/services/sandbox/instances/sandbox_links/work";
    let child_dir = "/services/sandbox/instances/sandbox_links/work/child";
    let nested_file = "/services/sandbox/instances/sandbox_links/work/file.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(child_dir).expect("mkdir child");
    dfs.posix_write_file(
        nested_file,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write");

    assert_eq!(dfs.posix_stat(parent).expect("parent stat").link_count, 3);
    assert_eq!(dfs.posix_stat(child_dir).expect("child stat").link_count, 2);
    assert_eq!(
        dfs.posix_stat(nested_file).expect("file stat").link_count,
        1
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_hardlink_shares_content_and_link_count() {
    let root = temp_root("sifs-posix-hardlink-shared-content");
    let node_id = NodeId::from_legacy_named("node_sifs_hardlink").expect("node");
    let original = "/services/sandbox/instances/sandbox_hardlink/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_hardlink/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_hardlink/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"original",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_set_times(original, Some(12_345), Some(67_890))
        .expect("set old times");
    dfs.posix_hardlink(original, link).expect("hardlink");

    let linked_ctime = dfs
        .posix_stat(original)
        .expect("original linked stat")
        .changed_at_unix_nanos;
    assert!(linked_ctime > 67_890);
    assert_eq!(
        dfs.posix_stat(original).expect("original stat").link_count,
        2
    );
    assert_eq!(dfs.posix_stat(link).expect("link stat").link_count, 2);
    dfs.posix_write_file(
        link,
        b"updated",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write link");
    assert_eq!(
        dfs.posix_read_file(original).expect("read original"),
        b"updated"
    );
    assert_eq!(dfs.posix_read_file(link).expect("read link"), b"updated");

    dfs.posix_unlink(link).expect("unlink alias");
    let remaining_stat = dfs.posix_stat(original).expect("remaining stat");
    assert_eq!(remaining_stat.link_count, 1);
    assert!(remaining_stat.changed_at_unix_nanos >= linked_ctime);
    assert!(matches!(
        dfs.posix_read_file(link),
        Err(DfsPosixError::NotFound { .. })
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_hardlink_preserves_symlink_object() {
    let root = temp_root("sifs-posix-hardlink-symlink");
    let node_id = NodeId::from_legacy_named("node_sifs_hardlink_symlink").expect("node");
    let work = "/services/sandbox/instances/sandbox_hardlink_symlink/work";
    let target = "/services/sandbox/instances/sandbox_hardlink_symlink/work/target.txt";
    let link = "/services/sandbox/instances/sandbox_hardlink_symlink/work/link.txt";
    let alias = "/services/sandbox/instances/sandbox_hardlink_symlink/work/link-alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        target,
        b"target",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write target");
    dfs.posix_symlink(
        "target.txt",
        link,
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("symlink");
    dfs.posix_hardlink(link, alias).expect("hardlink symlink");

    let link_stat = dfs.posix_lstat(link).expect("link lstat");
    let alias_stat = dfs.posix_lstat(alias).expect("alias lstat");
    assert_eq!(link_stat.kind, DfsObjectKind::Symlink);
    assert_eq!(alias_stat.kind, DfsObjectKind::Symlink);
    assert_eq!(link_stat.object_id, alias_stat.object_id);
    assert_eq!(link_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);
    assert_eq!(
        dfs.posix_readlink(alias).expect("alias readlink"),
        "target.txt"
    );

    dfs.posix_unlink(link).expect("unlink original symlink");
    assert!(matches!(
        dfs.posix_readlink(link),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_readlink(alias).expect("remaining alias readlink"),
        "target.txt"
    );
    assert_eq!(
        dfs.posix_lstat(alias).expect("remaining alias").link_count,
        1
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_unlink_promotes_hardlink_when_canonical_removed() {
    let root = temp_root("sifs-posix-hardlink-promote");
    let node_id = NodeId::from_legacy_named("node_sifs_hardlink_promote").expect("node");
    let original = "/services/sandbox/instances/sandbox_hardlink_promote/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_hardlink_promote/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_hardlink_promote/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write original");
    let original_object_id = dfs.posix_stat(original).expect("original stat").object_id;
    dfs.posix_hardlink(original, link).expect("hardlink");

    dfs.posix_unlink(original).expect("unlink canonical");
    assert!(matches!(
        dfs.posix_read_file(original),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_read_file(link).expect("read promoted"),
        b"content"
    );
    let promoted = dfs.posix_stat(link).expect("promoted stat");
    assert_eq!(promoted.object_id, original_object_id);
    assert_eq!(promoted.link_count, 1);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_records_old_path_tombstone() {
    let root = temp_root("sifs-posix-rename-tombstone");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_tombstone").expect("node");
    let old_path = "/services/sandbox/instances/sandbox_rename/work/old.txt";
    let new_path = "/services/sandbox/instances/sandbox_rename/work/new.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_rename/work")
        .expect("mkdir");
    dfs.posix_write_file(
        old_path,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write old");
    let old_version = dfs.posix_stat(old_path).expect("old stat").version;
    dfs.posix_rename(old_path, new_path).expect("rename");

    assert!(matches!(
        dfs.posix_stat(old_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(dfs.posix_read_file(new_path).expect("read new"), b"content");
    let tombstone = dfs
        .metadata
        .objects
        .values()
        .find(|object| object.path == old_path && object.kind == DfsObjectKind::Tombstone)
        .expect("old path tombstone");
    assert_ne!(tombstone.version, old_version);

    let reloaded = networked_sifs_with_backend(&root, node_id);
    assert!(matches!(
        reloaded.posix_stat(old_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        reloaded
            .posix_read_file(new_path)
            .expect("reloaded read new"),
        b"content"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_directory_bounded_job_resumes_after_reopen() {
    let root = temp_root("sifs-posix-rename-dir-bounded-job");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_dir_job").expect("node");
    let old_dir = "/services/sandbox/instances/sandbox_rename_job/work/old";
    let new_dir = "/services/sandbox/instances/sandbox_rename_job/work/new";
    let old_nested = format!("{old_dir}/nested/deeper");
    let old_files = [
        format!("{old_dir}/root.txt"),
        format!("{old_dir}/nested/a.txt"),
        format!("{old_nested}/b.txt"),
        format!("{old_nested}/c.txt"),
    ];
    let new_files = [
        format!("{new_dir}/root.txt"),
        format!("{new_dir}/nested/a.txt"),
        format!("{new_dir}/nested/deeper/b.txt"),
        format!("{new_dir}/nested/deeper/c.txt"),
    ];
    let job_id = {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(&old_nested).expect("mkdir old tree");
        for path in &old_files {
            dfs.posix_write_file(
                path,
                path.as_bytes(),
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("write old file");
        }

        let job = dfs
            .posix_rename_directory_bounded(old_dir, new_dir, 1)
            .expect("first bounded rename batch");
        assert_eq!(job.status, DfsNamespaceJobStatus::Running);
        assert!(
            !job.pending_frames.is_empty(),
            "a one-step directory rename should persist remaining subtree work"
        );
        assert!(matches!(
            dfs.posix_stat(old_dir),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(
            dfs.posix_stat(new_dir)
                .expect("new root after first batch")
                .kind,
            DfsObjectKind::Directory
        );
        job.job_id
    };

    {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        let mut job = dfs
            .posix_resume_namespace_job_bounded(&job_id, 2)
            .expect("resume bounded rename after reopen");
        while job.status != DfsNamespaceJobStatus::Completed {
            job = dfs
                .posix_resume_namespace_job_bounded(&job_id, 2)
                .expect("resume next rename batch");
        }
        assert!(job.processed_object_count >= old_files.len() as u64);
        for path in &old_files {
            assert!(matches!(
                dfs.posix_read_file(path),
                Err(DfsPosixError::NotFound { .. })
            ));
        }
        for (old_path, new_path) in old_files.iter().zip(new_files.iter()) {
            assert_eq!(
                dfs.posix_read_file(new_path).expect("new file content"),
                old_path.as_bytes()
            );
        }
    }

    {
        let dfs = networked_sifs_with_backend(&root, node_id);
        let job = dfs
            .metadata_backend
            .as_ref()
            .expect("backend")
            .get_namespace_job(&job_id)
            .expect("get completed rename job")
            .expect("job");
        assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
        assert!(job.completed_at.is_some());
        for new_path in &new_files {
            assert_eq!(
                dfs.posix_stat(new_path).expect("reloaded moved file").kind,
                DfsObjectKind::File
            );
        }
    }

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_directory_does_not_resurrect_deleted_children() {
    let root = temp_root("sifs-posix-rename-dir-deleted-child");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_dir_deleted_child").expect("node");
    let old_dir = "/services/sandbox/instances/sandbox_rename_deleted/work/old";
    let new_dir = "/services/sandbox/instances/sandbox_rename_deleted/work/new";
    let old_child = format!("{old_dir}/deleted.txt");
    let new_child = format!("{new_dir}/deleted.txt");

    {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(old_dir).expect("mkdir old dir");
        dfs.posix_write_file(
            &old_child,
            b"deleted",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write child");
        dfs.posix_unlink(&old_child).expect("unlink child");
        dfs.posix_rename(old_dir, new_dir).expect("rename dir");

        assert!(matches!(
            dfs.posix_read_file(&new_child),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert!(
            dfs.posix_list_dir(new_dir)
                .expect("new dir listing")
                .is_empty()
        );
    }

    let reloaded = networked_sifs_with_backend(&root, node_id);
    assert!(matches!(
        reloaded.posix_read_file(&new_child),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(
        reloaded
            .posix_list_dir(new_dir)
            .expect("reloaded new dir listing")
            .is_empty()
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_over_existing_file_survives_reload() {
    let root = temp_root("sifs-posix-rename-over-existing-file-reload");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_over_existing_reload").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_over_existing/work";
    let source = "/services/sandbox/instances/sandbox_rename_over_existing/work/source.txt";
    let dest = "/services/sandbox/instances/sandbox_rename_over_existing/work/dest.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        source,
        b"source",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write source");
    let source_object_id = dfs.posix_stat(source).expect("source stat").object_id;
    dfs.posix_write_file(
        dest,
        b"dest",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write dest");

    dfs.posix_rename(source, dest).expect("rename over dest");

    assert!(matches!(
        dfs.posix_stat(source),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(dfs.posix_read_file(dest).expect("read dest"), b"source");
    assert_eq!(
        dfs.posix_stat(dest).expect("dest stat").object_id,
        source_object_id
    );

    let reloaded = networked_sifs_with_backend(&root, node_id);
    assert!(matches!(
        reloaded.posix_stat(source),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        reloaded.posix_read_file(dest).expect("reloaded read dest"),
        b"source"
    );
    assert_eq!(
        reloaded
            .posix_stat(dest)
            .expect("reloaded dest stat")
            .object_id,
        source_object_id
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_hardlink_over_canonical_preserves_target() {
    let root = temp_root("sifs-posix-rename-hardlink-over-canonical");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_hardlink").expect("node");
    let canonical = "/services/sandbox/instances/sandbox_rename_hardlink/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_hardlink/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_rename_hardlink/work")
        .expect("mkdir");
    dfs.posix_write_file(
        canonical,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write canonical");
    let canonical_object_id = dfs.posix_stat(canonical).expect("canonical stat").object_id;
    dfs.posix_hardlink(canonical, alias).expect("hardlink");
    assert_eq!(dfs.posix_stat(alias).expect("alias stat").link_count, 2);

    dfs.posix_rename(alias, canonical)
        .expect("rename alias over canonical");

    let alias_stat = dfs.posix_stat(alias).expect("alias remains");
    let canonical_stat = dfs.posix_stat(canonical).expect("canonical remains");
    assert_eq!(canonical_stat.object_id, canonical_object_id);
    assert_eq!(alias_stat.object_id, canonical_object_id);
    assert_eq!(canonical_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);
    assert_eq!(
        dfs.posix_read_file(canonical).expect("read canonical"),
        b"content"
    );
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"content");

    let reloaded = networked_sifs_with_backend(&root, node_id);
    assert_eq!(
        reloaded
            .posix_stat(alias)
            .expect("reloaded alias")
            .link_count,
        2
    );
    assert_eq!(
        reloaded.posix_read_file(canonical).expect("reloaded read"),
        b"content"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_over_canonical_preserves_other_hardlinks() {
    let root = temp_root("sifs-posix-rename-over-canonical-hardlinks");
    let node_id =
        NodeId::from_legacy_named("node_sifs_rename_over_canonical_hardlinks").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work";
    let source = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/source.txt";
    let canonical = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/canonical.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        canonical,
        b"old",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write canonical");
    let old_object_id = dfs.posix_stat(canonical).expect("canonical stat").object_id;
    dfs.posix_hardlink(canonical, alias).expect("hardlink");
    dfs.posix_write_file(
        source,
        b"new",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write source");
    let source_object_id = dfs.posix_stat(source).expect("source stat").object_id;

    dfs.posix_rename(source, canonical)
        .expect("rename source over canonical");

    assert!(matches!(
        dfs.posix_stat(source),
        Err(DfsPosixError::NotFound { .. })
    ));
    let canonical_stat = dfs
        .posix_stat(canonical)
        .expect("canonical stat after rename");
    let alias_stat = dfs.posix_stat(alias).expect("alias stat after rename");
    assert_eq!(canonical_stat.object_id, source_object_id);
    assert_eq!(canonical_stat.link_count, 1);
    assert_eq!(alias_stat.object_id, old_object_id);
    assert_eq!(alias_stat.link_count, 1);
    assert_eq!(
        dfs.posix_read_file(canonical).expect("read canonical"),
        b"new"
    );
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"old");

    let reloaded = networked_sifs_with_backend(&root, node_id);
    assert_eq!(
        reloaded
            .posix_stat(alias)
            .expect("reloaded alias")
            .object_id,
        old_object_id
    );
    assert_eq!(
        reloaded
            .posix_read_file(canonical)
            .expect("reloaded canonical"),
        b"new"
    );
    assert_eq!(
        reloaded.posix_read_file(alias).expect("reloaded alias"),
        b"old"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_hardlink_over_canonical_preserves_both_targets() {
    let root = temp_root("sifs-posix-rename-hardlink-over-other-canonical");
    let node_id =
        NodeId::from_legacy_named("node_sifs_rename_hardlink_over_other_canonical").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work";
    let source_canonical =
        "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/source.txt";
    let source_alias =
        "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/source-alias.txt";
    let dest_canonical =
        "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/dest.txt";
    let dest_alias =
        "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/dest-alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        source_canonical,
        b"source",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write source");
    dfs.posix_hardlink(source_canonical, source_alias)
        .expect("source hardlink");
    let source_object_id = dfs
        .posix_stat(source_canonical)
        .expect("source stat")
        .object_id;
    dfs.posix_write_file(
        dest_canonical,
        b"dest",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write dest");
    dfs.posix_hardlink(dest_canonical, dest_alias)
        .expect("dest hardlink");
    let dest_object_id = dfs.posix_stat(dest_canonical).expect("dest stat").object_id;

    dfs.posix_rename(source_alias, dest_canonical)
        .expect("rename source alias over dest canonical");

    let source_stat = dfs
        .posix_stat(source_canonical)
        .expect("source canonical remains");
    let renamed_stat = dfs.posix_stat(dest_canonical).expect("renamed source");
    let dest_alias_stat = dfs.posix_stat(dest_alias).expect("dest alias remains");
    assert_eq!(source_stat.object_id, source_object_id);
    assert_eq!(renamed_stat.object_id, source_object_id);
    assert_eq!(source_stat.link_count, 2);
    assert_eq!(renamed_stat.link_count, 2);
    assert_eq!(dest_alias_stat.object_id, dest_object_id);
    assert_eq!(dest_alias_stat.link_count, 1);
    assert_eq!(
        dfs.posix_read_file(source_canonical)
            .expect("read source canonical"),
        b"source"
    );
    assert_eq!(
        dfs.posix_read_file(dest_canonical)
            .expect("read renamed source"),
        b"source"
    );
    assert_eq!(
        dfs.posix_read_file(dest_alias).expect("read dest alias"),
        b"dest"
    );
    assert!(matches!(
        dfs.posix_stat(source_alias),
        Err(DfsPosixError::NotFound { .. })
    ));

    let reloaded = networked_sifs_with_backend(&root, node_id);
    assert_eq!(
        reloaded
            .posix_stat(dest_canonical)
            .expect("reloaded renamed source")
            .object_id,
        source_object_id
    );
    assert_eq!(
        reloaded
            .posix_stat(dest_alias)
            .expect("reloaded dest alias")
            .object_id,
        dest_object_id
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_hardlink_alias_updates_target_ctime() {
    let root = temp_root("sifs-posix-rename-hardlink-alias-ctime");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_hardlink_alias_ctime").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_alias_ctime/work";
    let canonical = "/services/sandbox/instances/sandbox_rename_alias_ctime/work/canonical.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_alias_ctime/work/alias.txt";
    let renamed = "/services/sandbox/instances/sandbox_rename_alias_ctime/work/renamed-alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        canonical,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write canonical");
    dfs.posix_hardlink(canonical, alias).expect("hardlink");
    let before = dfs.posix_stat(canonical).expect("before stat");
    std::thread::sleep(std::time::Duration::from_millis(1));

    dfs.posix_rename(alias, renamed)
        .expect("rename hardlink alias");

    let canonical_after = dfs.posix_stat(canonical).expect("canonical remains");
    let renamed_after = dfs.posix_stat(renamed).expect("renamed alias");
    assert_eq!(canonical_after.object_id, before.object_id);
    assert_eq!(renamed_after.object_id, before.object_id);
    assert_eq!(canonical_after.link_count, 2);
    assert_eq!(renamed_after.link_count, 2);
    assert!(
        canonical_after.changed_at_unix_nanos > before.changed_at_unix_nanos,
        "renaming a hardlink alias must advance target inode ctime"
    );
    assert_eq!(
        canonical_after.changed_at_unix_nanos,
        renamed_after.changed_at_unix_nanos
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_over_hardlink_alias_updates_replaced_target_ctime() {
    let root = temp_root("sifs-posix-rename-over-hardlink-alias-ctime");
    let node_id =
        NodeId::from_legacy_named("node_sifs_rename_over_hardlink_alias_ctime").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work";
    let source = "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work/source.txt";
    let canonical =
        "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work/canonical.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        canonical,
        b"old",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write canonical");
    dfs.posix_hardlink(canonical, alias).expect("hardlink");
    let old_before = dfs.posix_stat(canonical).expect("old before");
    dfs.posix_write_file(
        source,
        b"new",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write source");
    let source_object_id = dfs.posix_stat(source).expect("source stat").object_id;
    std::thread::sleep(std::time::Duration::from_millis(1));

    dfs.posix_rename(source, alias)
        .expect("rename source over hardlink alias");

    let canonical_after = dfs.posix_stat(canonical).expect("canonical remains");
    let alias_after = dfs.posix_stat(alias).expect("alias replaced");
    assert_eq!(canonical_after.object_id, old_before.object_id);
    assert_eq!(canonical_after.link_count, 1);
    assert!(
        canonical_after.changed_at_unix_nanos > old_before.changed_at_unix_nanos,
        "replacing a hardlink alias must advance the replaced target inode ctime"
    );
    assert_eq!(alias_after.object_id, source_object_id);
    assert_eq!(alias_after.link_count, 1);
    assert_eq!(dfs.posix_read_file(canonical).expect("read old"), b"old");
    assert_eq!(dfs.posix_read_file(alias).expect("read new"), b"new");

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_set_times_updates_hardlink_target_metadata() {
    let root = temp_root("sifs-posix-set-times-hardlink");
    let node_id = NodeId::from_legacy_named("node_sifs_set_times").expect("node");
    let original = "/services/sandbox/instances/sandbox_set_times/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_set_times/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_set_times/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, link).expect("hardlink");

    dfs.posix_set_times(link, Some(12_345), Some(67_890))
        .expect("set times through link");
    let original_stat = dfs.posix_stat(original).expect("original stat");
    let link_stat = dfs.posix_stat(link).expect("link stat");
    assert_eq!(original_stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(original_stat.modified_at_unix_nanos, 67_890);
    assert_eq!(link_stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(link_stat.modified_at_unix_nanos, 67_890);
    assert!(original_stat.changed_at_unix_nanos > 67_890);
    assert_eq!(
        original_stat.changed_at_unix_nanos,
        link_stat.changed_at_unix_nanos
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_set_times_without_fields_is_noop() {
    let root = temp_root("sifs-posix-set-times-noop");
    let node_id = NodeId::from_legacy_named("node_sifs_set_times_noop").expect("node");
    let file = "/services/sandbox/instances/sandbox_set_times_noop/work/file.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_set_times_noop/work")
        .expect("mkdir");
    dfs.posix_write_file(
        file,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write file");
    dfs.posix_set_times(file, Some(12_345), Some(67_890))
        .expect("set baseline times");
    let before = dfs.posix_stat(file).expect("before stat");

    let after = dfs
        .posix_set_times(file, None, None)
        .expect("set no timestamp fields");

    assert_eq!(after.accessed_at_unix_nanos, before.accessed_at_unix_nanos);
    assert_eq!(after.modified_at_unix_nanos, before.modified_at_unix_nanos);
    assert_eq!(after.changed_at_unix_nanos, before.changed_at_unix_nanos);
    assert_eq!(
        after.version, before.version,
        "omitting both timestamps must not churn the distributed metadata version"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_set_owner_updates_hardlink_target_metadata() {
    let root = temp_root("sifs-posix-set-owner-hardlink");
    let node_id = NodeId::from_legacy_named("node_sifs_set_owner").expect("node");
    let original = "/services/sandbox/instances/sandbox_set_owner/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_set_owner/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_set_owner/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, link).expect("hardlink");

    dfs.posix_set_owner(link, 1001, 1002)
        .expect("set owner through link");
    let original_stat = dfs.posix_stat(original).expect("original stat");
    let link_stat = dfs.posix_stat(link).expect("link stat");
    assert_eq!(original_stat.uid, 1001);
    assert_eq!(original_stat.gid, 1002);
    assert_eq!(link_stat.uid, 1001);
    assert_eq!(link_stat.gid, 1002);
    assert_eq!(
        original_stat.changed_at_unix_nanos,
        link_stat.changed_at_unix_nanos
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_set_mode_zero_is_reported_as_zero() {
    let root = temp_root("sifs-posix-set-mode-zero");
    let node_id = NodeId::from_legacy_named("node_sifs_set_mode_zero").expect("node");
    let file = "/services/sandbox/instances/sandbox_set_mode_zero/work/file.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_set_mode_zero/work")
        .expect("mkdir");
    dfs.posix_write_file(
        file,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write file");

    let changed = dfs.posix_set_mode(file, 0).expect("chmod 0000");
    assert_eq!(changed.mode, 0);
    assert_eq!(dfs.posix_stat(file).expect("stat").mode, 0);
    assert!(
        dfs.posix_stat(file).expect("stat").changed_at_unix_nanos >= changed.changed_at_unix_nanos
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_metadata_only_versions_preserve_content_size() {
    let root = temp_root("sifs-posix-metadata-only-size");
    let node_id = NodeId::from_legacy_named("node_sifs_metadata_only_size").expect("node");
    let work = "/services/sandbox/instances/sandbox_metadata_size/work";
    let file = "/services/sandbox/instances/sandbox_metadata_size/work/file.txt";
    let renamed = "/services/sandbox/instances/sandbox_metadata_size/work/renamed.txt";
    let link = "/services/sandbox/instances/sandbox_metadata_size/work/link.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        file,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write file");
    assert_eq!(
        dfs.posix_set_mode(file, 0o600).expect("chmod").size_bytes,
        Some(7)
    );
    assert_eq!(
        dfs.posix_rename(file, renamed).expect("rename").size_bytes,
        Some(7)
    );

    dfs.posix_symlink(
        "renamed.txt",
        link,
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("symlink");
    assert_eq!(
        dfs.posix_set_symlink_times(link, Some(1), Some(2))
            .expect("set symlink times")
            .size_bytes,
        Some("renamed.txt".len() as u64)
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_recreate_file_refreshes_creation_metadata() {
    let root = temp_root("sifs-posix-recreate-file-times");
    let node_id = NodeId::from_legacy_named("node_sifs_recreate_file").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_recreate/work/file.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_recreate/work")
        .expect("mkdir");
    dfs.posix_write_file(
        file_path,
        b"old",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write old");
    dfs.posix_set_times(file_path, Some(12_345), Some(67_890))
        .expect("old times");
    dfs.posix_unlink(file_path).expect("unlink");

    let recreated = dfs
        .posix_write_file(
            file_path,
            b"new",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("recreate");

    assert_eq!(recreated.size_bytes, Some(3));
    assert!(recreated.accessed_at_unix_nanos > 12_345);
    assert!(recreated.modified_at_unix_nanos > 67_890);
    assert!(recreated.created_at_unix_nanos > 67_890);
    assert_eq!(dfs.posix_read_file(file_path).expect("read"), b"new");

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_recreate_directory_refreshes_creation_metadata() {
    let root = temp_root("sifs-posix-recreate-dir-times");
    let node_id = NodeId::from_legacy_named("node_sifs_recreate_dir").expect("node");
    let dir_path = "/services/sandbox/instances/sandbox_recreate_dir/work/dir";

    let mut dfs = networked_sifs_with_backend(&root, node_id);
    dfs.posix_mkdir_all(dir_path).expect("mkdir");
    dfs.posix_set_times(dir_path, Some(12_345), Some(67_890))
        .expect("old times");
    dfs.posix_rmdir(dir_path).expect("rmdir");
    let recreated = dfs.posix_mkdir(dir_path).expect("recreate dir");

    assert!(recreated.accessed_at_unix_nanos > 12_345);
    assert!(recreated.modified_at_unix_nanos > 67_890);
    assert!(recreated.created_at_unix_nanos > 67_890);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_updates_mtime_without_changing_atime() {
    let root = temp_root("sifs-posix-write-mtime");
    let node_id = NodeId::from_legacy_named("node_sifs_write_mtime").expect("node");
    let original = "/services/sandbox/instances/sandbox_write_mtime/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_write_mtime/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_write_mtime/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, link).expect("hardlink");
    dfs.posix_set_times(original, Some(12_345), Some(67_890))
        .expect("set old times");

    let stat = dfs
        .posix_write_file(
            link,
            b"updated",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write through hardlink");

    assert_eq!(stat.accessed_at_unix_nanos, 12_345);
    assert!(stat.modified_at_unix_nanos > 67_890);
    let original_stat = dfs.posix_stat(original).expect("original stat");
    let link_stat = dfs.posix_stat(link).expect("link stat");
    assert_eq!(original_stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(link_stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(
        original_stat.modified_at_unix_nanos,
        link_stat.modified_at_unix_nanos
    );
    assert!(original_stat.modified_at_unix_nanos > 67_890);
    assert_eq!(
        original_stat.changed_at_unix_nanos,
        original_stat.modified_at_unix_nanos
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_merges_bytes_and_preserves_hardlink_identity() {
    let root = temp_root("sifs-posix-write-at-hardlink");
    let node_id = NodeId::from_legacy_named("node_sifs_write_at").expect("node");
    let work = "/services/sandbox/instances/sandbox_write_at/work";
    let original = "/services/sandbox/instances/sandbox_write_at/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_write_at/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        original,
        b"abcdef",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, alias).expect("hardlink");

    let stat = dfs
        .posix_write_file_at(
            alias,
            2,
            b"XYZ",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write at hardlink");

    assert_eq!(stat.size_bytes, Some(6));
    assert_eq!(
        dfs.posix_read_file(original).expect("read original"),
        b"abXYZf"
    );
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"abXYZf");
    let original_stat = dfs.posix_stat(original).expect("original stat");
    let alias_stat = dfs.posix_stat(alias).expect("alias stat");
    assert_eq!(original_stat.object_id, alias_stat.object_id);
    assert_eq!(original_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);

    let stat = dfs
        .posix_write_file_at(
            original,
            8,
            b"!",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("sparse write");

    assert_eq!(stat.size_bytes, Some(9));
    assert_eq!(
        dfs.posix_read_file(alias).expect("read sparse alias"),
        b"abXYZf\0\0!"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_self_only_uses_extent_patch_manifest() {
    let root = temp_root("sifs-posix-write-at-extent-patch");
    let node_id = NodeId::from_legacy_named("node_sifs_write_at_extent").expect("node");
    let work = "/services/sandbox/instances/sandbox_write_at_extent/work";
    let file = "/services/sandbox/instances/sandbox_write_at_extent/work/file.bin";

    let mut initial = vec![0_u8; 256 * 1024 + 19];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    let initial_hash = content_hash(&initial);
    let patch = b"small-random-write";
    let patch_offset = 128 * 1024 + 11;
    let mut expected = initial.clone();
    expected[patch_offset..patch_offset + patch.len()].copy_from_slice(patch);
    let expected_hash = content_hash(&expected);

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        file,
        &initial,
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write initial");

    let stat = dfs
        .posix_write_file_at(
            file,
            patch_offset as u64,
            patch,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write extent patch");

    assert_eq!(stat.size_bytes, Some(expected.len() as u64));
    assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
    let manifest = dfs
        .content()
        .read_manifest(stat.object_id.as_str(), stat.version.as_str())
        .expect("patched manifest");
    assert_eq!(manifest.content_hash, expected_hash);
    assert!(
        !manifest.extents.is_empty(),
        "local self-only write_at should preserve unchanged bytes as extents"
    );
    assert!(
        manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == initial_hash)
    );
    assert!(
        !dfs.content()
            .has_blob(&manifest.content_hash)
            .expect("logical blob check"),
        "patched write_at should not materialize a full replacement blob"
    );
    assert_eq!(dfs.posix_read_file(file).expect("read patched"), expected);
    assert_eq!(
        dfs.posix_read_file_range(file, patch_offset as u64 - 8, patch.len() + 16)
            .expect("read patched range"),
        expected[patch_offset - 8..patch_offset + patch.len() + 8]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_from_reader_uses_extent_patch_manifest() {
    let root = temp_root("sifs-posix-write-at-reader-extent-patch");
    let node_id = NodeId::from_legacy_named("node_sifs_write_at_reader_extent").expect("node");
    let work = "/services/sandbox/instances/sandbox_write_at_reader_extent/work";
    let file = "/services/sandbox/instances/sandbox_write_at_reader_extent/work/file.bin";

    let mut initial = vec![0_u8; 320 * 1024 + 29];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    let initial_hash = content_hash(&initial);
    let patch = (0..80 * 1024 + 23)
        .map(|index| (index % 241) as u8)
        .collect::<Vec<_>>();
    let patch_hash = content_hash(&patch);
    let patch_offset = 160 * 1024 + 7;
    let mut expected = initial.clone();
    expected[patch_offset..patch_offset + patch.len()].copy_from_slice(&patch);
    let expected_hash = content_hash(&expected);

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        file,
        &initial,
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write initial");

    let stat = dfs
        .posix_write_file_at_from_reader(
            file,
            patch_offset as u64,
            std::io::Cursor::new(patch.clone()),
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("streaming write_at extent patch");

    assert_eq!(stat.size_bytes, Some(expected.len() as u64));
    assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
    let manifest = dfs
        .content()
        .read_manifest(stat.object_id.as_str(), stat.version.as_str())
        .expect("patched manifest");
    assert_eq!(manifest.content_hash, expected_hash);
    assert!(
        manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == initial_hash)
    );
    assert!(
        manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == patch_hash)
    );
    assert!(
        !dfs.content()
            .has_blob(&manifest.content_hash)
            .expect("logical blob check"),
        "streamed write_at should not materialize a full replacement blob"
    );
    assert_eq!(
        dfs.posix_read_file_range(file, patch_offset as u64 - 8, patch.len() + 16)
            .expect("read patched range"),
        expected[patch_offset - 8..patch_offset + patch.len() + 8]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_replicates_extent_patch_without_full_blob() {
    let root = temp_root("sifs-posix-write-at-replicated-extent-patch");
    let owner = NodeId::from_legacy_named("node_sifs_write_at_patch_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_write_at_patch_replica").expect("replica");
    let object_id =
        DfsObjectId::from_legacy_named("obj_sifs_write_at_patch_replicated").expect("object");
    let base_version =
        DfsVersion::from_legacy_named("version_sifs_write_at_patch_base").expect("base");
    let path = "/services/replicated-write-at-patch.bin";
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata"),
    )
    .expect("owner backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("replica"),
            root.join("replica-meta"),
        )
        .expect("replica metadata"),
    )
    .expect("replica backend");
    owner_dfs.bootstrap_distributed_sifs().expect("owner sifs");
    replica_dfs
        .bootstrap_distributed_sifs()
        .expect("replica sifs");

    let mut initial = vec![0_u8; 384 * 1024 + 37];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 239) as u8;
    }
    let owner_base = owner_dfs
        .content()
        .put_object_version(object_id.as_str(), base_version.as_str(), &initial)
        .expect("owner base content");
    let replica_base = replica_dfs
        .content()
        .put_object_version(object_id.as_str(), base_version.as_str(), &initial)
        .expect("replica base content");
    assert_eq!(owner_base.content_hash, replica_base.content_hash);
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.path = path.to_string();
    object.version = base_version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone(), replica.clone()];
    object.content_hash = Some(owner_base.content_hash.clone());
    object.content_size_bytes = Some(owner_base.size_bytes);
    owner_dfs
        .add_object_metadata(object.clone())
        .expect("owner object metadata");
    replica_dfs
        .add_object_metadata(object)
        .expect("replica object metadata");

    let patch = b"remote extent patch";
    let patch_offset = 192 * 1024 + 13;
    let mut expected = initial.clone();
    expected[patch_offset..patch_offset + patch.len()].copy_from_slice(patch);
    let expected_hash = content_hash(&expected);
    let pending_stat = owner_dfs
        .posix_write_file_at(
            path,
            patch_offset as u64,
            patch,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: false,
            },
        )
        .expect("owner patch write");
    assert_eq!(pending_stat.size_bytes, Some(expected.len() as u64));

    let mut committed = false;
    for _ in 0..16 {
        replica_dfs.process_incoming().expect("replica incoming");
        owner_dfs.process_incoming().expect("owner incoming");
        replica_dfs
            .process_incoming()
            .expect("replica commit incoming");
        let owner_object = owner_dfs
            .current_read_object_metadata(&object_id)
            .expect("owner object");
        let replica_object = replica_dfs
            .current_read_object_metadata(&object_id)
            .expect("replica object");
        if owner_object.content_hash.as_deref() == Some(expected_hash.as_str())
            && replica_object.content_hash.as_deref() == Some(expected_hash.as_str())
        {
            committed = true;
            break;
        }
    }
    assert!(committed, "replicated patch write did not commit");

    let committed_owner = owner_dfs
        .current_read_object_metadata(&object_id)
        .expect("committed owner metadata");
    assert_eq!(
        committed_owner.content_hash.as_deref(),
        Some(expected_hash.as_str())
    );
    assert_eq!(
        owner_dfs.posix_read_file(path).expect("owner read"),
        expected
    );
    assert_eq!(
        replica_dfs
            .current_read_object_metadata(&object_id)
            .expect("replica metadata")
            .current_locations,
        vec![owner.clone(), replica.clone()]
    );
    let replica_manifest = replica_dfs
        .content()
        .read_manifest(object_id.as_str(), committed_owner.version.as_str())
        .expect("replica patched manifest");
    assert_eq!(replica_manifest.content_hash, expected_hash);
    assert!(
        !replica_manifest.extents.is_empty(),
        "remote patch stage should preserve unchanged bytes as extents"
    );
    assert!(
        replica_manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == owner_base.content_hash)
    );
    assert!(
        replica_manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == content_hash(patch))
    );
    assert!(
        !replica_dfs
            .content()
            .has_blob(&replica_manifest.content_hash)
            .expect("logical blob check"),
        "replicated patch write should not materialize a full replacement blob on the replica"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_replicates_large_extent_patch_in_chunks_without_full_blob() {
    let root = temp_root("sifs-posix-write-at-large-patch-chunks");
    let owner = NodeId::from_legacy_named("node_sifs_write_at_large_patch_owner").expect("owner");
    let replica =
        NodeId::from_legacy_named("node_sifs_write_at_large_patch_replica").expect("replica");
    let object_id =
        DfsObjectId::from_legacy_named("obj_sifs_write_at_large_patch").expect("object");
    let base_version =
        DfsVersion::from_legacy_named("version_sifs_write_at_large_patch_base").expect("base");
    let path = "/services/replicated-write-at-large-patch.bin";
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata"),
    )
    .expect("owner backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("replica"),
            root.join("replica-meta"),
        )
        .expect("replica metadata"),
    )
    .expect("replica backend");
    owner_dfs.bootstrap_distributed_sifs().expect("owner sifs");
    replica_dfs
        .bootstrap_distributed_sifs()
        .expect("replica sifs");

    let mut initial = vec![0_u8; 768 * 1024 + 19];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 241) as u8;
    }
    let owner_base = owner_dfs
        .content()
        .put_object_version(object_id.as_str(), base_version.as_str(), &initial)
        .expect("owner base content");
    let replica_base = replica_dfs
        .content()
        .put_object_version(object_id.as_str(), base_version.as_str(), &initial)
        .expect("replica base content");
    assert_eq!(owner_base.content_hash, replica_base.content_hash);
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.path = path.to_string();
    object.version = base_version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone(), replica.clone()];
    object.content_hash = Some(owner_base.content_hash.clone());
    object.content_size_bytes = Some(owner_base.size_bytes);
    owner_dfs
        .add_object_metadata(object.clone())
        .expect("owner object metadata");
    replica_dfs
        .add_object_metadata(object)
        .expect("replica object metadata");

    let mut patch = vec![0_u8; 160 * 1024 + 17];
    for (index, byte) in patch.iter_mut().enumerate() {
        *byte = 255_u8.wrapping_sub((index % 251) as u8);
    }
    let patch_hash = content_hash(&patch);
    let patch_offset = 257 * 1024 + 3;
    let mut expected = initial.clone();
    expected[patch_offset..patch_offset + patch.len()].copy_from_slice(&patch);
    let expected_hash = content_hash(&expected);
    owner_dfs
        .posix_write_file_at(
            path,
            patch_offset as u64,
            &patch,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: false,
            },
        )
        .expect("owner large patch write");

    let mut committed = false;
    for _ in 0..16 {
        replica_dfs.process_incoming().expect("replica incoming");
        owner_dfs.process_incoming().expect("owner incoming");
        replica_dfs
            .process_incoming()
            .expect("replica commit incoming");
        let owner_object = owner_dfs
            .current_read_object_metadata(&object_id)
            .expect("owner object");
        let replica_object = replica_dfs
            .current_read_object_metadata(&object_id)
            .expect("replica object");
        if owner_object.content_hash.as_deref() == Some(expected_hash.as_str())
            && replica_object.content_hash.as_deref() == Some(expected_hash.as_str())
        {
            committed = true;
            break;
        }
    }
    assert!(committed, "chunked replicated patch write did not commit");

    let committed_owner = owner_dfs
        .current_read_object_metadata(&object_id)
        .expect("committed owner metadata");
    assert_eq!(
        owner_dfs
            .posix_read_file_range(path, patch_offset as u64, patch.len())
            .expect("owner patched range"),
        patch
    );
    let replica_manifest = replica_dfs
        .content()
        .read_manifest(object_id.as_str(), committed_owner.version.as_str())
        .expect("replica patched manifest");
    assert_eq!(replica_manifest.content_hash, expected_hash);
    assert!(
        replica_manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == owner_base.content_hash)
    );
    assert!(
        replica_manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == patch_hash)
    );
    assert!(
        !replica_dfs
            .content()
            .has_blob(&replica_manifest.content_hash)
            .expect("logical blob check"),
        "chunked patch stage should not materialize a full replacement blob"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_retries_lost_patch_stage_as_patch() {
    let root = temp_root("sifs-posix-write-at-patch-retry");
    let owner = NodeId::from_legacy_named("node_sifs_write_at_patch_retry_owner").expect("owner");
    let replica =
        NodeId::from_legacy_named("node_sifs_write_at_patch_retry_replica").expect("replica");
    let path = "/services/retry-write-at-patch.bin";
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata"),
    )
    .expect("owner backend");
    let mut replica_mailbox = network.add_node(replica.clone());
    owner_dfs.bootstrap_distributed_sifs().expect("owner sifs");

    let mut initial = vec![0_u8; 256 * 1024 + 31];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    owner_dfs
        .posix_write_file(
            path,
            &initial,
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("initial self write");

    let patch = b"lost patch stage should retry as patch";
    let patch_offset = 96 * 1024 + 5;
    owner_dfs
        .posix_write_file_at(
            path,
            patch_offset as u64,
            patch,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: false,
            },
        )
        .expect("patch write");

    let first_messages = drain_dfs_messages(&mut replica_mailbox);
    assert!(
        first_messages
            .iter()
            .any(|message| matches!(message, DfsNetworkMessage::StagePatch(_))),
        "first stage should send a patch"
    );
    assert!(
        first_messages
            .iter()
            .all(|message| !matches!(message, DfsNetworkMessage::StageBlobChunk(_))),
        "patch stage should not send full-object chunks"
    );

    owner_dfs.repair_retry_tick = 16;
    owner_dfs
        .retry_pending_repairs()
        .expect("retry pending patch stage");

    let retry_messages = drain_dfs_messages(&mut replica_mailbox);
    assert!(
        retry_messages
            .iter()
            .any(|message| matches!(message, DfsNetworkMessage::StagePatch(_))),
        "lost patch stage should be retried as a patch"
    );
    assert!(
        retry_messages
            .iter()
            .all(|message| !matches!(message, DfsNetworkMessage::StageBlobChunk(_))),
        "patch retry should not fall back to full-object chunks before an explicit patch rejection"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_retries_lost_large_patch_stage_as_patch_chunks() {
    let root = temp_root("sifs-posix-write-at-large-patch-chunk-retry");
    let owner =
        NodeId::from_legacy_named("node_sifs_write_at_large_patch_retry_owner").expect("owner");
    let replica =
        NodeId::from_legacy_named("node_sifs_write_at_large_patch_retry_replica").expect("replica");
    let path = "/services/retry-write-at-large-patch.bin";
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata"),
    )
    .expect("owner backend");
    let mut replica_mailbox = network.add_node(replica.clone());
    owner_dfs.bootstrap_distributed_sifs().expect("owner sifs");

    let mut initial = vec![0_u8; 512 * 1024 + 11];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    owner_dfs
        .posix_write_file(
            path,
            &initial,
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("initial self write");

    let mut patch = vec![0_u8; 160 * 1024 + 5];
    for (index, byte) in patch.iter_mut().enumerate() {
        *byte = (index % 239) as u8;
    }
    let patch_hash = content_hash(&patch);
    struct StreamingPatchReader {
        bytes: Vec<u8>,
        offset: usize,
        bulk_reads: std::rc::Rc<std::cell::Cell<usize>>,
    }
    impl std::io::Read for StreamingPatchReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.offset == 0 {
                if buffer.len() != 1 {
                    return Err(std::io::Error::other(format!(
                        "expected initial POSIX peek buffer, got {}",
                        buffer.len()
                    )));
                }
            } else if buffer.len() != 64 * 1024 {
                return Err(std::io::Error::other(format!(
                    "expected content-store streaming buffer, got {}",
                    buffer.len()
                )));
            } else {
                self.bulk_reads.set(self.bulk_reads.get() + 1);
            }
            if self.offset >= self.bytes.len() {
                return Ok(0);
            }
            let len = buffer.len().min(self.bytes.len() - self.offset);
            buffer[..len].copy_from_slice(&self.bytes[self.offset..self.offset + len]);
            self.offset += len;
            Ok(len)
        }
    }
    let bulk_reads = std::rc::Rc::new(std::cell::Cell::new(0));
    owner_dfs
        .posix_write_file_at_from_reader(
            path,
            96 * 1024 + 5,
            StreamingPatchReader {
                bytes: patch.clone(),
                offset: 0,
                bulk_reads: bulk_reads.clone(),
            },
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: false,
            },
        )
        .expect("large patch write");
    assert!(
        bulk_reads.get() > 1,
        "replicated write_at reader should stream the patch through the content store"
    );
    let pending = owner_dfs
        .pending_writes
        .values()
        .next()
        .expect("pending large patch write");
    let pending_patch = pending.patch.as_ref().expect("pending patch metadata");
    assert_eq!(pending_patch.patch_hash, patch_hash);
    assert_eq!(pending_patch.patch_size_bytes, patch.len() as u64);
    assert!(
        owner_dfs
            .content()
            .has_blob(&pending_patch.patch_hash)
            .expect("pending patch blob exists"),
        "pending patch retry should stream from the content-addressed patch blob"
    );

    let first_messages = drain_dfs_messages(&mut replica_mailbox);
    assert!(
        first_messages
            .iter()
            .any(|message| matches!(message, DfsNetworkMessage::StagePatchChunk(_))),
        "first large patch stage should send patch chunks"
    );
    assert!(
        first_messages
            .iter()
            .all(|message| !matches!(message, DfsNetworkMessage::StageBlobChunk(_))),
        "large patch stage should not send full-object chunks"
    );

    owner_dfs.repair_retry_tick = 16;
    owner_dfs
        .retry_pending_repairs()
        .expect("retry pending large patch stage");

    let retry_messages = drain_dfs_messages(&mut replica_mailbox);
    assert!(
        retry_messages
            .iter()
            .any(|message| matches!(message, DfsNetworkMessage::StagePatchChunk(_))),
        "lost large patch stage should be retried as patch chunks"
    );
    assert!(
        retry_messages
            .iter()
            .all(|message| !matches!(message, DfsNetworkMessage::StageBlobChunk(_))),
        "large patch retry should not fall back to full-object chunks before an explicit patch rejection"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_truncate_preserves_hardlink_identity() {
    let root = temp_root("sifs-posix-truncate-hardlink");
    let node_id = NodeId::from_legacy_named("node_sifs_truncate").expect("node");
    let work = "/services/sandbox/instances/sandbox_truncate/work";
    let original = "/services/sandbox/instances/sandbox_truncate/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_truncate/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        original,
        b"abcdef",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, alias).expect("hardlink");

    let stat = dfs
        .posix_truncate_file(
            alias,
            3,
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("truncate through hardlink");
    assert_eq!(stat.size_bytes, Some(3));
    assert_eq!(dfs.posix_read_file(original).expect("read"), b"abc");
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"abc");
    assert_eq!(
        dfs.posix_stat(original).expect("original stat").object_id,
        dfs.posix_stat(alias).expect("alias stat").object_id
    );

    let stat = dfs
        .posix_truncate_file(
            original,
            5,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("extend sparse through original");
    assert_eq!(stat.size_bytes, Some(5));
    assert_eq!(
        dfs.posix_read_file(alias).expect("read sparse alias"),
        b"abc\0\0"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_distributed_write_rejects_stale_backend_base_version() {
    let root = temp_root("sifs-stale-write-base");
    let node_id = NodeId::from_legacy_named("node_sifs_stale_write_base").expect("node");
    let work = "/services/sandbox/instances/sandbox_stale_write/work";
    let file_path = "/services/sandbox/instances/sandbox_stale_write/work/file.txt";

    let mut writer = networked_sifs_with_backend(&root, node_id.clone());
    writer.posix_mkdir_all(work).expect("mkdir");
    writer
        .posix_write_file(
            file_path,
            b"abcdef",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("initial write");

    let stale = networked_sifs_with_backend(&root, node_id.clone());
    let stale_stat = stale.posix_stat(file_path).expect("stale stat");

    writer
        .posix_write_file_at(
            file_path,
            0,
            b"XY",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("fresh offset write");
    let fresh_stat = writer.posix_stat(file_path).expect("fresh stat");
    let journal_count_before_conflict = writer
        .metadata_backend()
        .expect("writer metadata backend")
        .journal_since(0)
        .expect("journal before conflict")
        .len();

    let mut stale_writer = stale;
    let stale_bytes = b"stale overwrite".to_vec();
    let stale_hash = content_hash(&stale_bytes);
    let request = DfsWriteRequest {
        file_system_id: stale_stat.file_system_id.clone(),
        object_id: stale_stat.object_id.clone(),
        target_version: crate::namespace::next_version(&stale_stat.version),
        requesting_node_id: node_id.clone(),
    };
    let outcome = stale_writer
        .write_distributed_eventual_with_base(
            request,
            Some(stale_stat.version.clone()),
            stale_bytes,
            vec![node_id.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect("stale write is reported as conflict");
    let conflict = match outcome {
        NetworkedDfsWriteOutcome::Conflict { conflict } => conflict,
        other => panic!("stale write must conflict instead of overwriting: {other:?}"),
    };
    assert_eq!(
        conflict.local_content_hash.as_deref(),
        Some(stale_hash.as_str()),
        "stale writer conflict should preserve the losing writer content hash"
    );
    assert_eq!(
        stale_writer
            .content()
            .read_blob(&stale_hash)
            .expect("stale conflict content"),
        b"stale overwrite"
    );

    let journal_after_conflict = stale_writer
        .metadata_backend()
        .expect("stale writer metadata backend")
        .journal_since(0)
        .expect("journal after conflict");
    assert_eq!(
        journal_after_conflict.len(),
        journal_count_before_conflict + 1,
        "stale writer conflict must append to the shared metadata journal without overwriting fresh writer entries"
    );
    assert!(matches!(
        journal_after_conflict.last().map(|entry| &entry.mutation),
        Some(DfsMetadataMutation::PutConflict { .. })
    ));

    let mut reloaded = networked_sifs_with_backend(&root, node_id);
    assert_eq!(
        reloaded
            .posix_stat(file_path)
            .expect("stat after conflict")
            .content_hash,
        fresh_stat.content_hash,
        "stale conflict publication must not replace the committed content hash"
    );
    assert!(
        matches!(
            reloaded.posix_read_file(file_path),
            Err(DfsPosixError::Conflict { .. })
        ),
        "open DFS conflicts must not be reported as missing files"
    );
    reloaded
        .update_conflict_status(
            &conflict.object_id,
            &conflict.conflict_id,
            DfsConflictStatus::Resolved,
        )
        .expect("resolve conflict");
    assert_eq!(
        reloaded.posix_read_file(file_path).expect("read winner"),
        b"XYcdef"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_distributed_write_validates_replica_plan_before_storing_content() {
    let root = temp_root("sifs-invalid-replica-plan-no-content");
    let node_id = NodeId::from_legacy_named("node_sifs_invalid_plan").expect("node");
    let work = "/services/sandbox/instances/sandbox_invalid_plan/work";
    let file_path = "/services/sandbox/instances/sandbox_invalid_plan/work/file.txt";
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        file_path,
        b"initial",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("initial write");
    let stat = dfs.posix_stat(file_path).expect("stat");
    let rejected_bytes = b"replica plan is invalid";
    let rejected_hash = content_hash(rejected_bytes);
    let target_version = crate::namespace::next_version(&stat.version);

    let error = dfs
        .write_distributed_eventual_with_base(
            DfsWriteRequest {
                file_system_id: stat.file_system_id.clone(),
                object_id: stat.object_id.clone(),
                target_version: target_version.clone(),
                requesting_node_id: node_id.clone(),
            },
            Some(stat.version),
            rejected_bytes.to_vec(),
            vec![node_id],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect_err("invalid replica plan should fail before content write");

    assert!(
        matches!(error, DfsError::Distributed { .. }),
        "invalid replica plan should be reported as distributed validation failure: {error:?}"
    );
    assert!(
        !dfs.content
            .has_blob(&rejected_hash)
            .expect("rejected blob check"),
        "rejected write bytes must not be persisted before replica validation"
    );
    assert!(
        dfs.content
            .read_manifest(stat.object_id.as_str(), target_version.as_str())
            .is_err(),
        "rejected write must not publish a content manifest"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_relink_after_canonical_unlink_does_not_create_hardlink_cycle() {
    let root = temp_root("sifs-posix-relink-after-canonical-unlink");
    let node_id = NodeId::from_legacy_named("node_sifs_relink_after_unlink").expect("node");
    let work = "/services/sandbox/instances/sandbox_relink_after_unlink/work";
    let original = "/services/sandbox/instances/sandbox_relink_after_unlink/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_relink_after_unlink/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, alias).expect("hardlink alias");
    dfs.posix_write_file(
        alias,
        b"updated",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write alias");
    dfs.posix_unlink(original).expect("unlink canonical path");

    dfs.posix_hardlink(alias, original)
        .expect("relink original path");

    let original_stat = dfs.posix_stat(original).expect("original stat");
    let alias_stat = dfs.posix_stat(alias).expect("alias stat");
    assert_eq!(original_stat.object_id, alias_stat.object_id);
    assert_eq!(original_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);
    assert_eq!(
        dfs.posix_read_file(original).expect("read original"),
        b"updated"
    );
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"updated");

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_read_updates_atime_without_changing_mtime() {
    let root = temp_root("sifs-posix-read-atime");
    let node_id = NodeId::from_legacy_named("node_sifs_read_atime").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_read_atime/work/file.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_read_atime/work")
        .expect("mkdir");
    dfs.posix_write_file(
        file_path,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write");
    dfs.posix_set_times(file_path, Some(12_345), Some(67_890))
        .expect("old times");
    let before = dfs.posix_stat(file_path).expect("before stat");

    assert_eq!(
        dfs.posix_read_file_update_access(file_path)
            .expect("read update access"),
        b"content"
    );

    let stat = dfs.posix_stat(file_path).expect("stat");
    assert!(stat.accessed_at_unix_nanos > 12_345);
    assert_eq!(stat.modified_at_unix_nanos, 67_890);
    assert_ne!(
        stat.version, before.version,
        "atime update must advance the DFS metadata version so it persists across distributed reconciliation"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_list_updates_directory_atime_without_changing_mtime() {
    let root = temp_root("sifs-posix-list-atime");
    let node_id = NodeId::from_legacy_named("node_sifs_list_atime").expect("node");
    let dir_path = "/services/sandbox/instances/sandbox_list_atime/work";
    let file_path = "/services/sandbox/instances/sandbox_list_atime/work/file.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(dir_path).expect("mkdir");
    dfs.posix_write_file(
        file_path,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write");
    dfs.posix_set_times(dir_path, Some(12_345), Some(67_890))
        .expect("old times");

    let entries = dfs
        .posix_list_dir_update_access(dir_path)
        .expect("list update access");
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        vec!["file.txt"]
    );

    let stat = dfs.posix_stat(dir_path).expect("stat");
    assert!(stat.accessed_at_unix_nanos > 12_345);
    assert_eq!(stat.modified_at_unix_nanos, 67_890);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_list_dir_page_walks_large_directory_incrementally() {
    let root = temp_root("sifs-posix-list-page");
    let node_id = NodeId::from_legacy_named("node_sifs_list_page").expect("node");
    let dir_path = "/services/sandbox/instances/sandbox_list_page/work";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(dir_path).expect("mkdir");
    for index in 0..5 {
        dfs.posix_write_file(
            &format!("{dir_path}/file-{index}.txt"),
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write");
    }

    let mut cursor = None;
    let mut names = Vec::new();
    let mut pages = 0usize;
    loop {
        let page = dfs
            .posix_list_dir_page(dir_path, cursor.as_ref(), 2)
            .expect("list page");
        assert!(page.entries.len() <= 2);
        names.extend(page.entries.into_iter().map(|entry| entry.name));
        pages = pages.saturating_add(1);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    names.sort();
    assert_eq!(
        names,
        vec![
            "file-0.txt",
            "file-1.txt",
            "file-2.txt",
            "file-3.txt",
            "file-4.txt"
        ]
    );
    assert_eq!(pages, 3);

    let full_names = dfs
        .posix_list_dir(dir_path)
        .expect("full list")
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    assert_eq!(
        full_names,
        vec![
            "file-0.txt",
            "file-1.txt",
            "file-2.txt",
            "file-3.txt",
            "file-4.txt"
        ]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_namespace_mutations_update_parent_directory_mtime() {
    let root = temp_root("sifs-posix-parent-mtime");
    let node_id = NodeId::from_legacy_named("node_sifs_parent_mtime").expect("node");
    let work = "/services/sandbox/instances/sandbox_parent_mtime/work";
    let other = "/services/sandbox/instances/sandbox_parent_mtime/other";
    let file = "/services/sandbox/instances/sandbox_parent_mtime/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_parent_mtime/work/alias.txt";
    let link = "/services/sandbox/instances/sandbox_parent_mtime/work/link.txt";
    let renamed = "/services/sandbox/instances/sandbox_parent_mtime/other/renamed.txt";
    let dir = "/services/sandbox/instances/sandbox_parent_mtime/work/dir";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir work");
    dfs.posix_mkdir_all(other).expect("mkdir other");

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("old work times");
    let before_create = dfs.posix_stat(work).expect("work before create");
    std::thread::sleep(std::time::Duration::from_millis(1));
    dfs.posix_write_file(
        file,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("create file");
    let after_create = dfs.posix_stat(work).expect("work after create");
    assert!(after_create.modified_at_unix_nanos > 67_890);
    assert!(
        after_create.changed_at_unix_nanos > before_create.changed_at_unix_nanos,
        "creating a directory entry must advance parent directory ctime"
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_write_file(
        file,
        b"updated",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("overwrite file");
    assert_eq!(
        dfs.posix_stat(work)
            .expect("work after overwrite")
            .modified_at_unix_nanos,
        67_890,
        "overwriting existing file content must not change parent directory mtime"
    );

    dfs.posix_hardlink(file, alias).expect("hardlink");
    assert!(
        dfs.posix_stat(work)
            .expect("work after hardlink")
            .modified_at_unix_nanos
            > 67_890
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_symlink(
        "file.txt",
        link,
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("symlink");
    assert!(
        dfs.posix_stat(work)
            .expect("work after symlink")
            .modified_at_unix_nanos
            > 67_890
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_mkdir(dir).expect("mkdir child");
    assert!(
        dfs.posix_stat(work)
            .expect("work after mkdir")
            .modified_at_unix_nanos
            > 67_890
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_rmdir(dir).expect("rmdir child");
    assert!(
        dfs.posix_stat(work)
            .expect("work after rmdir")
            .modified_at_unix_nanos
            > 67_890
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_unlink(alias).expect("unlink alias");
    assert!(
        dfs.posix_stat(work)
            .expect("work after unlink")
            .modified_at_unix_nanos
            > 67_890
    );

    dfs.posix_set_times(work, Some(12_345), Some(67_890))
        .expect("reset work times");
    dfs.posix_set_times(other, Some(22_222), Some(88_888))
        .expect("old other times");
    dfs.posix_rename(file, renamed).expect("rename across dirs");
    assert!(
        dfs.posix_stat(work)
            .expect("work after rename")
            .modified_at_unix_nanos
            > 67_890
    );
    assert!(
        dfs.posix_stat(other)
            .expect("other after rename")
            .modified_at_unix_nanos
            > 88_888
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_updates_moved_object_ctime_without_changing_mtime() {
    let root = temp_root("sifs-posix-rename-ctime");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_ctime").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_ctime/work";
    let source = "/services/sandbox/instances/sandbox_rename_ctime/work/source.txt";
    let renamed = "/services/sandbox/instances/sandbox_rename_ctime/work/renamed.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir work");
    dfs.posix_write_file(
        source,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write source");
    dfs.posix_set_times(source, Some(12_345), Some(67_890))
        .expect("old source times");

    let before = dfs.posix_stat(source).expect("before stat");
    dfs.posix_rename(source, renamed).expect("rename");
    let after = dfs.posix_stat(renamed).expect("after stat");

    assert_eq!(after.object_id, before.object_id);
    assert_eq!(after.created_at_unix_nanos, before.created_at_unix_nanos);
    assert_eq!(
        after.modified_at_unix_nanos, before.modified_at_unix_nanos,
        "rename must not change file content mtime"
    );
    assert!(
        after.changed_at_unix_nanos > before.changed_at_unix_nanos,
        "rename must advance ctime because the path metadata changed"
    );
    assert_ne!(
        after.version, before.version,
        "rename metadata update must advance the object metadata version"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn local_owned_read_is_routed_to_owner_node() {
    let owner = NodeId::from_legacy_named("node_desktop").expect("node");
    let remote = NodeId::from_legacy_named("node_mobile").expect("node");
    let network = InProcessNetwork::new();
    let mut owner_dfs = LocalOwnedDfsService::new(network.add_node(owner.clone()));
    let mut remote_dfs = LocalOwnedDfsService::new(network.add_node(remote.clone()));
    let metadata = file_metadata(owner.clone());
    let object_id = metadata.object_id.clone();

    owner_dfs.add_file_system(file_system(owner.clone()));
    remote_dfs.add_file_system(file_system(owner.clone()));
    owner_dfs
        .put_local_object(metadata.clone(), b"owned bytes".to_vec())
        .expect("seed owner object");
    remote_dfs
        .add_object_metadata(metadata)
        .expect("replicate metadata");

    let read = remote_dfs.read_object(&object_id).expect("read routes");
    let request_id = match read {
        DfsReadOutcome::Routed {
            request_id,
            owner_node_id,
        } => {
            assert_eq!(owner_node_id, owner);
            request_id
        }
        DfsReadOutcome::Local { .. } => panic!("remote read should route to owner"),
    };

    let owner_events = owner_dfs.process_incoming().expect("owner handles read");
    assert!(matches!(
        &owner_events[..],
        [DfsServiceEvent::ReadRequestHandled {
            result: DfsReadResult::Found(_),
            ..
        }]
    ));

    let remote_events = remote_dfs
        .process_incoming()
        .expect("remote receives read response");
    assert!(matches!(
        &remote_events[..],
        [DfsServiceEvent::ReadResponseReceived {
            request_id: received_request_id,
            result: DfsReadResult::Found(DfsReadData { bytes, .. }),
            ..
        }] if received_request_id == &request_id && bytes == b"owned bytes"
    ));
}

#[test]
fn local_owned_write_is_routed_to_owner_node_and_committed() {
    let owner = NodeId::from_legacy_named("node_desktop").expect("node");
    let remote = NodeId::from_legacy_named("node_mobile").expect("node");
    let network = InProcessNetwork::new();
    let mut owner_dfs = LocalOwnedDfsService::new(network.add_node(owner.clone()));
    let mut remote_dfs = LocalOwnedDfsService::new(network.add_node(remote.clone()));
    let metadata = file_metadata(owner.clone());
    let object_id = metadata.object_id.clone();
    let file_system_id = metadata.file_system_id.clone();

    owner_dfs.add_file_system(file_system(owner.clone()));
    remote_dfs.add_file_system(file_system(owner.clone()));
    owner_dfs
        .put_local_object(metadata.clone(), b"old bytes".to_vec())
        .expect("seed owner object");
    remote_dfs
        .add_object_metadata(metadata)
        .expect("replicate metadata");

    let target_version = DfsVersion::from_legacy_named("version_2").expect("version");
    let write = remote_dfs
        .write_object(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: target_version.clone(),
                requesting_node_id: remote.clone(),
            },
            b"updated bytes".to_vec(),
        )
        .expect("write routes");
    let request_id = match write {
        DfsWriteOutcome::Routed {
            request_id,
            owner_node_id,
        } => {
            assert_eq!(owner_node_id, owner);
            request_id
        }
        DfsWriteOutcome::Local { .. } => panic!("remote write should route to owner"),
    };

    let owner_events = owner_dfs.process_incoming().expect("owner handles write");
    assert!(matches!(
        &owner_events[..],
        [DfsServiceEvent::WriteRequestHandled {
            result: DfsWriteResult::Committed { .. },
            ..
        }]
    ));

    let owner_read = owner_dfs
        .read_object(&object_id)
        .expect("owner reads local");
    assert!(matches!(
        owner_read,
        DfsReadOutcome::Local {
            data: Some(DfsReadData { bytes, version, .. }),
            ..
        } if bytes == b"updated bytes" && version == target_version
    ));

    let remote_events = remote_dfs
        .process_incoming()
        .expect("remote receives write response");
    assert!(matches!(
        &remote_events[..],
        [DfsServiceEvent::WriteResponseReceived {
            request_id: received_request_id,
            result: DfsWriteResult::Committed { receipt, .. },
            ..
        }] if received_request_id == &request_id
            && receipt.committed_version == target_version
            && receipt.owner_node_id == owner
    ));
    assert_eq!(
        remote_dfs
            .metadata()
            .object(&object_id)
            .expect("metadata updated")
            .version,
        target_version
    );
}

#[test]
fn dfs_policy_violation_event_is_structured_for_supervision() {
    let owner = NodeId::from_legacy_named("node_desktop").expect("node");
    let remote = NodeId::from_legacy_named("node_mobile").expect("node");
    let event = DfsPolicyViolation {
        requesting_node_id: remote,
        executing_node_id: owner.clone(),
        file_system_id: Some(DfsFileSystemId::from_legacy_named("sifs").expect("filesystem")),
        object_id: Some(DfsObjectId::from_legacy_named("obj_file").expect("object")),
        reason: "write attempted against non-owner".to_string(),
    }
    .event();

    assert_eq!(event.kind.as_str(), "dfs.policy.violation");
    assert_eq!(event.target_node_id, Some(owner));
    assert_eq!(
        event.payload["file_system_id"],
        DfsFileSystemId::from_legacy_named("sifs")
            .expect("filesystem")
            .as_str()
    );
}

#[test]
fn explicit_dfs_roots_are_preserved() {
    let root = PathBuf::from("/tmp/sinomo-dfs-explicit-root");

    assert_eq!(
        resolve_dfs_metadata_root(Some(root.join("metadata"))).expect("metadata root"),
        root.join("metadata")
    );
    assert_eq!(
        resolve_dfs_pin_root(Some(root.join("pins"))).expect("pin root"),
        root.join("pins")
    );
}
