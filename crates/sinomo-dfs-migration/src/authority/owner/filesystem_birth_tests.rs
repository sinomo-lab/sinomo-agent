//! New shard birth after actual owner conversion. No Active configuration or
//! generation receipt is manufactured by this fixture.
use super::filesystem_tests::{complete_filesystem_owner, native_filesystem_owner_fixture};
use super::*;
use sinomo_dfs::{
    DfsMetadataBackend, DfsMetadataGenerationOrigin, DfsMetadataGenerationState,
    DfsPreparedMetadataEffect, DfsPreparedOperationPayload, DfsPreparedOperationScope,
};

#[test]
fn native_converted_filesystem_birth_retains_original_activation_through_compaction_and_restart() {
    let (mut native, node, hold) =
        native_filesystem_owner_fixture("native-converted-filesystem-birth");
    complete_filesystem_owner(&native, &node, &hold);
    let fs = sinomo_dfs::sifs_file_system_id();
    let mut publisher = native.configuration_publisher(&node);
    let before: sinomo_dfs::DfsFileSystemConfig = publisher
        .get_authoritative_consistent(&[sinomo_dfs::file_system_config_key(&fs)])
        .unwrap()
        .remove(0)
        .unwrap()
        .decode()
        .unwrap();
    let activation = before
        .metadata_storage
        .as_ref()
        .and_then(|state| state.active_descriptor())
        .unwrap();
    let shard = native
        .bootstrap_empty_metadata_prefix(
            "after-conversion-birth",
            &sinomo_dfs::sifs_node_path(&node),
        )
        .unwrap();
    let id = sinomo_dfs::DfsMetadataShardId::new(&shard.shard_id);
    let root = native
        .data_dir(&node)
        .dfs_dir()
        .join("metadata")
        .join(&shard.shard_id);
    let header = sinomo_dfs::inspect_metadata_storage_generation(&root).unwrap();
    assert_eq!(header.identity.generation, activation.generation);
    assert_eq!(
        header.identity.origin,
        DfsMetadataGenerationOrigin::NativeGenesisInGeneration {
            activation: activation.clone()
        }
    );
    assert!(matches!(
        header.state,
        DfsMetadataGenerationState::Active { .. }
    ));
    let operation_id = format!("genesis:{}:{}:1", fs, shard.shard_id);
    let original = native
        .with_main_maintenance_service(&node, |service| {
            service
                .metadata_backend()
                .unwrap()
                .prepared_operation_record(&operation_id)
                .unwrap()
                .unwrap()
        })
        .unwrap();
    let expected =
        sinomo_dfs::DfsPreparedOperationEviction::from_terminal_record(&original).unwrap();
    assert_eq!(expected.genesis_storage_activation, Some(activation));
    let payload = DfsPreparedOperationPayload::for_metadata_effects(
        DfsPreparedOperationScope {
            file_system_id: fs.clone(),
            shard_id: id.clone(),
            owner_epoch: shard.epoch,
            operation_id: "collect-original-converted-birth".into(),
        },
        Vec::new(),
        Vec::new(),
        vec![DfsPreparedMetadataEffect::RemovePreparedOperation {
            digest: original.digest.clone(),
        }],
        time::OffsetDateTime::now_utc(),
    )
    .unwrap();
    native
        .with_main_maintenance_service(&node, |service| {
            sinomo_dfs::test_support::publish_current_payload(service, payload).unwrap();
            let backend = service.metadata_backend().unwrap();
            assert!(
                backend
                    .prepared_operation_record(&operation_id)
                    .unwrap()
                    .is_none()
            );
            let snapshot = backend.export_shard_table_snapshot(&id).unwrap().unwrap();
            assert_eq!(
                snapshot
                    .tables
                    .prepared_operation_evictions
                    .iter()
                    .find(|row| row.digest.operation_id == operation_id),
                Some(&expected)
            );
        })
        .unwrap();
    assert_eq!(
        sinomo_dfs::inspect_metadata_storage_generation(&root).unwrap(),
        header
    );
    drop(publisher);
    native.stop_node(&node).unwrap();
    native.reopen_node(node.clone()).unwrap();
    native.wait_startup_completion().unwrap();
    assert_eq!(
        sinomo_dfs::inspect_metadata_storage_generation(&root).unwrap(),
        header
    );
    native
        .with_main_maintenance_service(&node, |service| {
            let backend = service.metadata_backend().unwrap();
            assert!(
                backend
                    .prepared_operation_record(&operation_id)
                    .unwrap()
                    .is_none()
            );
            let snapshot = backend.export_shard_table_snapshot(&id).unwrap().unwrap();
            assert_eq!(
                snapshot
                    .tables
                    .prepared_operation_evictions
                    .iter()
                    .find(|row| row.digest.operation_id == operation_id),
                Some(&expected)
            );
        })
        .unwrap();
    let path = format!(
        "{}/after-generation-birth",
        sinomo_dfs::sifs_node_path(&node)
    );
    {
        let mut client = native.client(&node).unwrap();
        client
            .posix_write_file(
                &path,
                b"new scope after real cutover",
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        assert_eq!(
            client.posix_read_file_distributed(&path).unwrap(),
            b"new scope after real cutover"
        );
    }
    native.close().unwrap();
}
