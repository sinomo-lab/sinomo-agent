//! Native-produced inputs for backend member/floor validation. These controls
//! do not claim general failover availability or authorize numeric test permits.
use super::*;

pub(super) struct NativeSnapshots {
    pub runtime: crate::test_support::DfsTestRuntime,
    pub legacy_context: crate::DfsMetadataReplicaShardContext,
    pub oldest_context: crate::DfsMetadataReplicaShardContext,
    pub before_context: crate::DfsMetadataReplicaShardContext,
    pub current_context: crate::DfsMetadataReplicaShardContext,
    pub before_cut: DfsMetadataShardRecordCut,
    pub before_image: crate::DfsMetadataSnapshotImage,
    pub current_image: crate::DfsMetadataSnapshotImage,
}

fn capture(
    runtime: &crate::test_support::DfsTestRuntime,
    receiver: &NodeId,
    config: &crate::DfsMetadataShardConfig,
) -> (
    crate::DfsMetadataReplicaShardContext,
    crate::DfsMetadataSnapshotImage,
) {
    // A committed switch can precede publication into this node's immutable
    // configuration. Await the existing ReadIndex/watch boundary, not a flag
    // inserted into a test permission.
    runtime
        .wait_published_configuration_current(receiver)
        .unwrap();
    let mut reader = runtime.local_shard_copy_service(receiver, config).unwrap();
    reader.refresh_control_plane_latest().unwrap();
    let context = reader
        .retained_metadata_replica_context(
            &config.file_system_id,
            &DfsMetadataShardId::new(&config.shard_id),
        )
        .unwrap()
        .unwrap();
    assert_eq!(context.shard, *config);
    assert!(context.namespace_catalog.is_some());
    drop(reader);
    let image = runtime
        .node_metadata(&config.primary_node_id)
        .unwrap()
        .export_shard_snapshot_image(&DfsMetadataShardId::new(&config.shard_id))
        .unwrap()
        .unwrap();
    (context, image)
}

fn record_cut(
    runtime: &crate::test_support::DfsTestRuntime,
    config: &crate::DfsMetadataShardConfig,
) -> DfsMetadataShardRecordCut {
    let nodes = std::iter::once(config.primary_node_id.clone())
        .chain(config.replica_node_ids.clone())
        .collect::<Vec<_>>();
    let mut control = runtime.configuration_publisher(&config.primary_node_id);
    crate::DfsControlPlaneStore::new(control.as_mut())
        .read_metadata_shard_record_cut(&config.file_system_id, &config.shard_id, &nodes)
        .unwrap()
        .unwrap()
}

impl NativeSnapshots {
    pub fn new(name: &str) -> Self {
        let (runtime, source, original) =
            crate::test_support::current_native_metadata_service(name, 3, "/").unwrap();
        drop(source);
        let receiver = original.replica_node_ids[1].clone();
        let next_primary = original.replica_node_ids[0].clone();
        let (legacy_context, _) = capture(&runtime, &receiver, &original);
        let members = std::iter::once(original.primary_node_id.clone())
            .chain(original.replica_node_ids.clone())
            .collect::<std::collections::BTreeSet<_>>();
        for member in &members {
            runtime
                .advertise_metadata_write_set_capability(member)
                .unwrap();
        }
        let before = record_cut(&runtime, &original);
        let switch = crate::build_metadata_write_set_switch(&before, &members).unwrap();
        let mut control = runtime.configuration_publisher(&original.primary_node_id);
        assert_eq!(
            crate::DfsControlPlaneStore::new(control.as_mut())
                .submit_metadata_write_set_change(&switch),
            crate::DfsMetadataWriteSetTxnOutcome::Applied
        );
        drop(control);
        assert!(
            record_cut(&runtime, &original)
                .write_set_state()
                .switched()
                .is_some(),
            "the actual committed native switch must exist before its published receiver context is used"
        );
        let (oldest_context, _) = capture(&runtime, &receiver, &original);
        assert!(oldest_context.floor_epoch_scope().is_some());
        let middle = runtime
            .reassign_metadata_shard(
                &original.file_system_id,
                &original.shard_id,
                next_primary,
                vec![original.primary_node_id.clone(), receiver.clone()],
            )
            .unwrap();
        let (before_context, before_image) = capture(&runtime, &receiver, &middle);
        let before_cut = record_cut(&runtime, &middle);
        let current = runtime
            .reassign_metadata_shard(
                &original.file_system_id,
                &original.shard_id,
                original.primary_node_id.clone(),
                original.replica_node_ids.clone(),
            )
            .unwrap();
        let (current_context, current_image) = capture(&runtime, &receiver, &current);
        assert!(
            legacy_context.shard.epoch < before_context.shard.epoch
                && before_context.shard.epoch < current.epoch
        );
        Self {
            runtime,
            legacy_context,
            oldest_context,
            before_context,
            current_context,
            before_cut,
            before_image,
            current_image,
        }
    }

    pub fn permit(
        &self,
        context: &crate::DfsMetadataReplicaShardContext,
        image: &crate::DfsMetadataSnapshotImage,
    ) -> DfsMetadataReplicaIngestPermit {
        let command = crate::DfsMetadataSnapshotInstallCommand {
            request_id: "native-member-state-snapshot".into(),
            source_node_id: context.shard.primary_node_id.clone(),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            primary_handoff_id: None,
            shard_id: DfsMetadataShardId::new(&context.shard.shard_id),
            next_after_sequence: image.materialized_through_sequence,
            snapshot: image.descriptor().clone(),
            image: Some(image.clone()),
            reason: "backend member state with actual native source/catalog".into(),
        };
        crate::metadata_replication::metadata_snapshot_install_permit(context, &command).unwrap()
    }
}
