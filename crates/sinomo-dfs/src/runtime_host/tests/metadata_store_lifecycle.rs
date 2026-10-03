//! Committed policy deltas must update projections without cold-recovering stores.

use super::*;

#[test]
fn committed_placement_policy_retains_resident_lanes_and_updates_configuration() {
    let root = temp_root("dfs-policy-retains-store-owner");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let owner = Arc::new(DaemonDataOwnerLock::acquire(&paths).unwrap());
    register_daemon_dfs_data_owner_lock(&paths, &owner).unwrap();
    let local = NodeId::from_legacy_named("policy-store-owner-local").unwrap();
    let remote = NodeId::from_legacy_named("policy-store-owner-remote").unwrap();
    write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "127.0.0.1:9");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let runtime = daemon_test_runtime("dfs-policy-retains-store-owner");
    let file_system = distributed_sifs(local.clone());
    let mut config = DfsFileSystemConfig::from(&file_system);
    let mut shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".into(),
        primary_node_id: local.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![
                sinomo_core::kv::KvOp::put_serde(
                    file_system_config_key(&config.file_system_id),
                    &config,
                    None,
                )
                .unwrap(),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&config.file_system_id, "root"),
                    &shard,
                    None,
                )
                .unwrap(),
            ],
            failure: Vec::new(),
        }),
    )
    .unwrap();
    let lookup = |frame| {
        handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command(
                format!("policy-store-lookup-{frame}"),
                test_dfs_lookup_routed_message(&remote, &local, frame, "/services/absent"),
            ),
            runtime.handle(),
        )
        .unwrap()
    };
    lookup(1);
    let (first_lane, scan_owner, first_signature) = {
        let cache = daemon_dfs_message_service_cache().lock().unwrap();
        let entry = cache.get(&data_dir.root()).unwrap();
        assert!(entry.resident_metadata_initialized && entry.resident_metadata_backend.is_some());
        (
            Arc::clone(entry.primary_service.as_ref().unwrap()),
            Arc::clone(&entry.write_intent_maintenance_scan),
            entry.signature.clone(),
        )
    };
    let opens = daemon_dfs_test_message_service_lane_open_count(&data_dir);
    config.subtree_policies[2].min_commit_replicas = Some(2);
    config.subtree_policies[2].desired_replicas = Some(3);
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                file_system_config_key(&config.file_system_id),
                &config,
                None,
            )
            .unwrap(),
        ),
    )
    .unwrap();
    lookup(2);
    {
        let cache = daemon_dfs_message_service_cache().lock().unwrap();
        let entry = cache.get(&data_dir.root()).unwrap();
        assert!(Arc::ptr_eq(
            &first_lane,
            entry.primary_service.as_ref().unwrap()
        ));
        assert!(Arc::ptr_eq(
            &scan_owner,
            &entry.write_intent_maintenance_scan
        ));
        assert!(
            entry.signature.metadata_authority_generation
                > first_signature.metadata_authority_generation
        );
        assert_eq!(
            entry.signature.metadata_storage_generation,
            first_signature.metadata_storage_generation
        );
    }
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        opens
    );
    let projected = first_lane
        .lock()
        .unwrap()
        .service
        .refresh_file_history_configuration(&config.file_system_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        projected.subtree_policies, config.subtree_policies,
        "retained service must use the newly committed W/N policy"
    );
    drop(first_lane);

    // Maintenance uses the same lifecycle distinction, including a policy edge
    // that arrives while it temporarily owns the resident global service.
    let cluster = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths).unwrap();
    let preflight = runtime
        .block_on(warm_daemon_dfs_metadata_authority_for_identity_async(
            &cluster_paths,
            &cluster.cluster_id,
            &local,
            runtime.handle(),
        ))
        .unwrap();
    let scheduler = daemon_dfs_mutation_scheduler(&data_dir).unwrap();
    let admission = runtime
        .block_on(scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive))
        .unwrap();
    let retained = take_daemon_dfs_message_mutation_service_for_maintenance(
        &data_dir,
        &cluster_paths,
        &cluster,
        runtime.handle(),
        Some(&preflight),
    )
    .unwrap();
    assert!(Arc::ptr_eq(
        &scan_owner,
        &retained.lane.service.write_intent_maintenance_scan
    ));
    config.display_name = "Updated during maintenance".into();
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                file_system_config_key(&config.file_system_id),
                &config,
                None,
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert!(
        restore_daemon_dfs_message_mutation_service_after_maintenance(
            &data_dir,
            &cluster_paths,
            runtime.handle(),
            retained,
        )
        .unwrap(),
        "configuration delta must not discard the taken maintenance service"
    );
    drop(admission);

    // A real assignment/epoch transition still replaces the physical owner.
    shard.epoch += 1;
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&config.file_system_id, "root"),
                &shard,
                None,
            )
            .unwrap(),
        ),
    )
    .unwrap();
    lookup(3);
    {
        let cache = daemon_dfs_message_service_cache().lock().unwrap();
        let entry = cache.get(&data_dir.root()).unwrap();
        assert!(
            entry.signature.metadata_storage_generation
                > first_signature.metadata_storage_generation
        );
        assert!(!Arc::ptr_eq(
            &scan_owner,
            &entry.write_intent_maintenance_scan
        ));
    }
    evict_daemon_dfs_message_service_cache(&data_dir).unwrap();
}
