use super::*;
use crate::test_runtime::DfsTestRuntime;

#[test]
fn native_genesis_generation_activates_once_and_survives_restart() {
    let (mut native, nodes) =
        DfsTestRuntime::start_cluster("native-current-generation-restart", 1).unwrap();
    let node = nodes[0].clone();
    let shards = native.metadata_shards().unwrap();
    let headers = shards
        .iter()
        .map(|shard| {
            let id = DfsMetadataShardId::new(&shard.shard_id);
            let root = crate::local_dfs_metadata_shard_path(native.data_dir(&node), &id);
            let header = read_generation(&root).unwrap();
            assert_eq!(header.identity.file_system_id, shard.file_system_id);
            assert_eq!(header.identity.shard_id, id);
            assert_eq!(
                header.identity.origin,
                DfsMetadataGenerationOrigin::NativeGenesis
            );
            assert!(matches!(header.state, GenerationState::Active { .. }));
            drop(crate::LocalSobjMetadataBackend::new(id, &root).unwrap());
            (root, header)
        })
        .collect::<Vec<_>>();
    assert!(!headers.is_empty());
    native.stop_node(&node).unwrap();
    native.reopen_node(node.clone()).unwrap();
    for (root, header) in headers {
        assert_eq!(
            read_generation(&root).unwrap(),
            header,
            "restart retains exact committed generation identity"
        );
        drop(crate::LocalSobjMetadataBackend::new(header.identity.shard_id, root).unwrap());
    }
    native.close().unwrap();
}

#[test]
fn native_retained_staged_genesis_restarts_and_completes_original_body() {
    let (mut native, nodes) =
        DfsTestRuntime::start_cluster("native-staged-generation-restart", 1).unwrap();
    let node = nodes[0].clone();
    let phase = native
        .begin_deferred_native_genesis("generation-restart-scope", &crate::sifs_node_path(&node))
        .unwrap();
    let id = DfsMetadataShardId::new(&phase.shard().shard_id);
    let root = crate::local_dfs_metadata_shard_path(native.data_dir(&node), &id);
    let before = read_generation(&root).unwrap();
    assert!(matches!(before.state, GenerationState::Staged { .. }));
    assert!(matches!(
        crate::LocalSobjMetadataBackend::new(id.clone(), &root),
        Err(MetadataBackendError::StoragePreparationPending { .. })
    ));
    let original = phase.original_record().clone();
    native.stop_node(&node).unwrap();
    native.reopen_node(node.clone()).unwrap();
    assert_eq!(read_generation(&root).unwrap(), before);
    assert_eq!(
        native.read_deferred_native_genesis(&phase).unwrap(),
        original
    );
    let committed = native.complete_deferred_native_genesis(&phase).unwrap();
    assert_eq!(committed.digest, original.digest);
    assert_eq!(committed.payload, original.payload);
    assert_eq!(
        committed.status,
        crate::DfsPreparedOperationStatus::Committed
    );
    assert_eq!(
        committed.committed_entry.as_ref().unwrap().sequence,
        original.digest.prepare_sequence + 1
    );
    let active = read_generation(&root).unwrap();
    assert_eq!(active.identity, before.identity);
    assert!(matches!(active.state, GenerationState::Active { .. }));
    drop(crate::LocalSobjMetadataBackend::new(id, root).unwrap());
    drop(phase);
    native.close().unwrap();
}

#[test]
fn native_membership_snapshot_bootstraps_current_replica_generation() {
    let (mut native, nodes) =
        DfsTestRuntime::start_cluster("native-current-replica-generation", 3).unwrap();
    let shard = native.root_shard().unwrap();
    assert_eq!(
        shard.replica_node_ids.len(),
        2,
        "normal membership expansion completed"
    );
    assert!(shard.epoch > 1);
    let id = DfsMetadataShardId::new(&shard.shard_id);
    let primary_root =
        crate::local_dfs_metadata_shard_path(native.data_dir(&shard.primary_node_id), &id);
    let primary = read_generation(&primary_root).unwrap();
    assert!(matches!(primary.state, GenerationState::Active { .. }));
    for replica in &shard.replica_node_ids {
        let root = crate::local_dfs_metadata_shard_path(native.data_dir(replica), &id);
        assert_eq!(
            read_generation(&root).unwrap(),
            primary,
            "actual snapshot ingress installs the same current generation and genesis receipt"
        );
        drop(crate::LocalSobjMetadataBackend::new(id.clone(), root).unwrap());
    }
    let replica = shard.replica_node_ids[0].clone();
    native.stop_node(&replica).unwrap();
    native.reopen_node(replica.clone()).unwrap();
    let root = crate::local_dfs_metadata_shard_path(native.data_dir(&replica), &id);
    assert_eq!(read_generation(&root).unwrap(), primary);
    assert_eq!(nodes.len(), 3);
    native.close().unwrap();
}

#[test]
fn native_genesis_records_each_shards_active_storage_beside_its_assignment() {
    let (native, nodes) =
        DfsTestRuntime::start_cluster("native-genesis-shard-storage-record", 1).unwrap();
    let node = nodes[0].clone();
    let shards = native.metadata_shards().unwrap();
    assert!(!shards.is_empty());
    let shared = native.shared_control_plane().unwrap();
    for shard in &shards {
        let id = DfsMetadataShardId::new(&shard.shard_id);
        let root = crate::local_dfs_metadata_shard_path(native.data_dir(&node), &id);
        let header = read_generation(&root).unwrap();
        let key = crate::metadata_shard_storage_key(&shard.file_system_id, &shard.shard_id);
        let entry = shared
            .with_client(|kv| kv.as_mut().get(&key))
            .unwrap()
            .expect("a native birth records the shard's storage");
        let record: DfsMetadataShardStorage = entry.decode().unwrap();
        record.validate_against(shard).unwrap();
        assert_eq!(record.generation, header.identity);
        assert_eq!(
            record.state,
            DfsMetadataShardStorageState::Active {
                activation: DfsMetadataShardActivation::NativeGenesis,
            }
        );
        assert!(record.admits_header(&header));
    }
    drop(shared);
    native.close().unwrap();
}
