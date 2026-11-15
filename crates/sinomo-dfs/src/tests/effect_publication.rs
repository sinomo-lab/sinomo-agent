//! Publication of a change committed through a bounded effect service.

use super::*;
use std::time::{Duration, Instant};

/// A change committed through the bounded effect opener schedules the
/// publication that carries its commit record, as a served commit does: the
/// followers reach `Committed` without another write.
#[test]
fn native_effect_commit_publishes_to_followers() {
    let name = "native-effect-publication";
    let (runtime, mut client, _) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    client.posix_mkdir_all("/services/effect").unwrap();
    drop(client);
    let config = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    let mut effect = crate::runtime_host::open_dfs_runtime_service_on_shard(
        runtime.data_dir(&config.primary_node_id),
        Some(crate::test_runtime::executor().handle()),
        &config,
    )
    .unwrap();
    let current = effect
        .lookup_sifs_path("/services/effect")
        .unwrap()
        .unwrap();
    let mut changed = current.clone();
    changed.version = DfsVersion::generate();
    changed.mode = 0o640;
    let payload = crate::DfsPreparedOperationPayload::for_namespace_transaction(
        crate::DfsPreparedOperationScope {
            file_system_id: current.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(config.shard_id.clone()),
            owner_epoch: config.epoch,
            operation_id: "effect-publication".into(),
        },
        vec![crate::DfsNamespaceObjectVersionExpectation {
            object_id: current.object_id.clone(),
            expected_version: Some(current.version.clone()),
        }],
        vec![crate::DfsNamespacePathOccupantExpectation {
            path: current.path.clone(),
            expected_object_id: Some(current.object_id.clone()),
        }],
        crate::DfsNamespaceTransaction {
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
            transaction_id: "transaction:effect-publication".into(),
            file_system_id: current.file_system_id.clone(),
            objects: vec![changed],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        },
        Vec::new(),
        None,
        None,
        Vec::new(),
        None,
        None,
    )
    .unwrap();
    assert!(
        effect
            .publish_prepared_operation(payload)
            .unwrap()
            .published
    );
    drop(effect);
    let status = |node: &NodeId| {
        use crate::DfsMetadataBackend as _;
        crate::LocalSobjMetadataBackend::open_for_inspection(
            DfsMetadataShardId::new(config.shard_id.clone()),
            crate::default_dfs_metadata_shards_root(runtime.data_dir(node))
                .join(crate::metadata_keys::key_segment(&config.shard_id)),
        )
        .ok()?
        .prepared_operation_record("effect-publication")
        .ok()
        .flatten()
        .map(|record| record.status)
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    while !config
        .replica_node_ids
        .iter()
        .all(|node| status(node) == Some(crate::DfsPreparedOperationStatus::Committed))
    {
        assert!(
            Instant::now() < deadline,
            "the followers reach Committed: {:?}",
            config
                .replica_node_ids
                .iter()
                .map(|node| (node.clone(), status(node)))
                .collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    runtime.close().unwrap();
}
