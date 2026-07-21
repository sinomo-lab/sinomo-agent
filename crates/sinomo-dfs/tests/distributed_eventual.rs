use sinomo_core::DfsObjectPolicy;
use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsReplicationPolicy,
    DfsVersion, DfsWriteIntentState, DistributedEventualCoordinator, LocalContentStore,
};
use std::fs;
use std::path::Path;

#[test]
fn distributed_eventual_commit_advances_metadata_then_repairs_missing_replica() {
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let root = temp_root("distributed_eventual_commit");
    let store_a = LocalContentStore::new(root.join("node-a")).expect("store a");
    let store_b = LocalContentStore::new(root.join("node-b")).expect("store b");

    let initial_object = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifacts/build.log".to_string(),
        kind: DfsObjectKind::Artifact,
        tombstone_provenance: None,
        owner_node_id: node_a.clone(),
        current_locations: vec![node_a.clone()],
        content_hash: Some("blake3:old".to_string()),
        content_size_bytes: None,
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };

    let mut coordinator = DistributedEventualCoordinator::new();
    coordinator.set_committed_version(file_system_id.clone(), object_id.clone(), version_1.clone());
    let intent = coordinator
        .create_intent(
            file_system_id.clone(),
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
    coordinator
        .record_durable_receipt(&intent.intent_id, node_a.clone())
        .expect("receipt");
    coordinator
        .finalize_commit_if_ready(&intent.intent_id)
        .expect("commit intent");
    let committed_intent = coordinator
        .intent(&intent.intent_id)
        .expect("intent")
        .clone();

    assert_eq!(committed_intent.state, DfsWriteIntentState::Committed);
    assert_eq!(coordinator.repair_jobs().len(), 1);
    assert_eq!(coordinator.repair_jobs()[0].missing_replica_node_id, node_b);

    let mut committed_object = initial_object;
    committed_object.version = version_2.clone();
    committed_object.content_hash = Some(manifest.content_hash.clone());
    committed_object.current_locations = vec![node_a.clone()];
    assert_eq!(committed_object.version, version_2);
    assert_eq!(
        committed_object.content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );

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

fn temp_root(name: &str) -> sinomo_testdir::TestDir {
    sinomo_testdir::TestDir::new(&format!("sinomo-dfs-{name}"))
}

fn cleanup(root: &Path) {
    if root.exists() {
        fs::remove_dir_all(root).expect("cleanup temp dir");
    }
}
