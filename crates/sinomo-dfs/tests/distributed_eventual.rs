use sinomo_core::{
    DfsObjectId, DfsObjectPolicy, DfsVersion, DfsWriteReceipt, EventId, NodeId, WorkspaceId,
};
use sinomo_dfs::{
    DfsObjectKind, DfsObjectMetadata, DfsReplicaReceiptRecord, DfsReplicationPolicy,
    DfsVersionRecord, DfsWriteIntentState, DistributedEventualCoordinator, KvDfsMetadataStore,
    LocalContentStore, Workspace,
};
use sinomo_kv::InProcessKvStore;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;

#[test]
fn distributed_eventual_commit_advances_metadata_then_repairs_missing_replica() {
    let workspace_id = WorkspaceId::new("workspace_repo").expect("workspace");
    let object_id = DfsObjectId::new("obj_artifact").expect("object");
    let version_1 = DfsVersion::new("version_1").expect("version");
    let version_2 = DfsVersion::new("version_2").expect("version");
    let node_a = NodeId::new("node_a").expect("node");
    let node_b = NodeId::new("node_b").expect("node");
    let root = temp_root("distributed_eventual_commit");
    let store_a = LocalContentStore::new(root.join("node-a")).expect("store a");
    let store_b = LocalContentStore::new(root.join("node-b")).expect("store b");
    let mut kv = InProcessKvStore::new();

    let workspace = Workspace {
        workspace_id: workspace_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: node_a.clone(),
        mounts: Vec::new(),
    };
    let initial_object = DfsObjectMetadata {
        object_id: object_id.clone(),
        workspace_id: workspace_id.clone(),
        path: "artifacts/build.log".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: node_a.clone(),
        current_locations: vec![node_a.clone()],
        content_hash: Some("blake3:old".to_string()),
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };

    {
        let mut metadata = KvDfsMetadataStore::new(&mut kv);
        metadata.put_workspace(&workspace).expect("workspace");
        metadata.put_object(&initial_object).expect("object");
    }

    let mut coordinator = DistributedEventualCoordinator::new();
    coordinator.set_committed_version(workspace_id.clone(), object_id.clone(), version_1.clone());
    let intent = coordinator
        .create_intent(
            workspace_id.clone(),
            object_id.clone(),
            Some(version_1.clone()),
            version_2.clone(),
            node_a.clone(),
            vec![node_a.clone(), node_b.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("intent");

    let manifest = store_a
        .put_object_version(object_id.as_str(), version_2.as_str(), b"replicated bytes")
        .expect("put object");
    let committed_intent = coordinator
        .record_durable_receipt(&intent.intent_id, node_a.clone())
        .expect("receipt");

    assert_eq!(committed_intent.state, DfsWriteIntentState::Committed);
    assert_eq!(coordinator.repair_jobs().len(), 1);
    assert_eq!(coordinator.repair_jobs()[0].missing_replica_node_id, node_b);

    let mut committed_object = initial_object.clone();
    committed_object.version = version_2.clone();
    committed_object.content_hash = Some(manifest.content_hash.clone());
    committed_object.current_locations = vec![node_a.clone()];
    let version_record = DfsVersionRecord {
        workspace_id: workspace_id.clone(),
        object_id: object_id.clone(),
        version: version_2.clone(),
        content_hash: Some(manifest.content_hash.clone()),
        locations: vec![node_a.clone()],
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    let receipt_record = DfsReplicaReceiptRecord {
        receipt: DfsWriteReceipt {
            workspace_id: workspace_id.clone(),
            object_id: object_id.clone(),
            committed_version: version_2.clone(),
            owner_node_id: node_a.clone(),
            event_id: EventId::new("evt_dfs_commit").expect("event"),
        },
        replica_node_id: node_a.clone(),
        content_hash: Some(manifest.content_hash.clone()),
        received_at: OffsetDateTime::UNIX_EPOCH,
    };

    {
        let mut metadata = KvDfsMetadataStore::new(&mut kv);
        metadata.put_receipt(&receipt_record).expect("receipt");
        let required_receipt_key =
            sinomo_dfs::kv_metadata::receipt_key(&workspace_id, &object_id, &version_2, &node_a);
        assert!(
            metadata
                .commit_visible_version(
                    &initial_object,
                    &committed_object,
                    &version_record,
                    &[required_receipt_key],
                )
                .expect("commit visible")
        );
        assert_eq!(
            metadata
                .get_object(&workspace_id, &object_id)
                .expect("object")
                .expect("object exists")
                .version,
            version_2
        );
    }

    let repaired_bytes = store_a
        .read_blob(&manifest.content_hash)
        .expect("read source");
    let repaired_manifest = store_b
        .put_object_version(object_id.as_str(), version_2.as_str(), &repaired_bytes)
        .expect("repair target");
    assert_eq!(repaired_manifest.content_hash, manifest.content_hash);
    assert!(
        store_b
            .has_blob(&manifest.content_hash)
            .expect("has repaired")
    );

    cleanup(&root);
}

fn temp_root(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("sinomo-dfs-{name}-{nanos}"))
}

fn cleanup(root: &Path) {
    if root.exists() {
        fs::remove_dir_all(root).expect("cleanup temp dir");
    }
}
