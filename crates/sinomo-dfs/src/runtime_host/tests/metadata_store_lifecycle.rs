//! Committed policy deltas must update projections without cold-recovering stores.

use super::*;

#[test]
fn immutable_fetch_inspection_never_creates_or_recertifies_an_owner() {
    let root = temp_root("dfs-immutable-fetch-owner-proof");
    let data_dir = SinomoDataDir::new(root.join("data"));
    let shard_id = crate::DfsMetadataShardId::new("root");
    let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir)
        .join(crate::metadata_keys::key_segment(&shard_id.0));
    let inspect = || {
        crate::local_state::with_current_owned_dfs_shard_inspection(
            &data_dir,
            &shard_id,
            |backend| Ok(backend.metadata_prefix_proof(&shard_id)?),
        )
    };
    assert!(inspect().unwrap().is_none());
    assert!(
        !metadata_root.exists(),
        "read admission must not create a metadata root"
    );
    let local = NodeId::from_legacy_named("fetch-proof-owner").unwrap();
    let mut writer =
        crate::LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root).unwrap();
    writer.put_file_system(&distributed_sifs(local)).unwrap();
    assert!(inspect().unwrap().flatten().is_some());
    let guard = crate::metadata_lock::metadata_write_lock(
        metadata_root.clone(),
        crate::metadata_cursor::journal_write_lock_path(&metadata_root),
    )
    .unwrap();
    assert!(
        inspect().unwrap().is_none(),
        "busy writer must remain fenced"
    );
    assert!(
        inspect().unwrap().is_none(),
        "failed inspection must not release another owner's path guard"
    );
    drop(guard);
    assert!(inspect().unwrap().flatten().is_some());
    crate::metadata_journal::admission::mark_uncertain(&metadata_root).unwrap();
    assert!(inspect().unwrap().is_none());
    assert!(
        crate::metadata_journal::admission::current_owned_materialized_prefix(
            &metadata_root,
            &shard_id,
        )
        .unwrap()
        .is_none(),
        "observational reads must not clear uncertainty"
    );
}

#[test]
fn verified_blob_fetch_serves_while_global_maintenance_owns_metadata() {
    let root = temp_root("dfs-verified-fetch-content-lane");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let owner = Arc::new(DaemonDataOwnerLock::acquire(&paths).unwrap());
    register_daemon_dfs_data_owner_lock(&paths, &owner).unwrap();
    let local = NodeId::from_legacy_named("verified-fetch-local").unwrap();
    let remote = NodeId::from_legacy_named("verified-fetch-remote").unwrap();
    write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "127.0.0.1:9");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let runtime = daemon_test_runtime("dfs-verified-fetch-content-lane");
    let file_system = distributed_sifs(local.clone());
    let config = DfsFileSystemConfig::from(&file_system);
    let shard = DfsMetadataShardConfig {
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
            failure: Vec::new(),
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
        }),
    )
    .unwrap();
    let bytes = b"leased immutable fetch bytes";
    let mut object = pending_object_metadata_for_test(
        DfsObjectId::from_legacy_named("verified-fetch-object").unwrap(),
        config.file_system_id.clone(),
        DfsVersion::from_legacy_named("verified-fetch-version").unwrap(),
        local.clone(),
        "/services/verified-fetch",
    );
    object.content_hash = Some(crate::content_store::content_hash(bytes));
    object.content_size_bytes = Some(bytes.len() as u64);
    let mut intent = pending_write_intent_for_test(
        "verified-fetch-intent",
        object.file_system_id.clone(),
        object.object_id.clone(),
        object.version.clone(),
        remote.clone(),
        vec![local.clone()],
        1,
    );
    intent.content_hash = object.content_hash.clone();
    intent.object_metadata = Some(object.clone());
    let shard_id = crate::DfsMetadataShardId::new("root");
    let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir)
        .join(crate::metadata_keys::key_segment(&shard_id.0));
    let mut writer =
        crate::LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root).unwrap();
    writer.put_file_system(&file_system).unwrap();
    writer.put_object(&object).unwrap();
    writer.put_intent(&intent).unwrap();
    let content = crate::LocalContentStore::new(data_dir.dfs_dir().join("content")).unwrap();
    content
        .put_object_version(object.object_id.as_str(), object.version.as_str(), bytes)
        .unwrap();
    drop(writer);
    // Warm the actual global resident generation before maintenance takes it.
    handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command(
            "verified-fetch-warm",
            test_dfs_lookup_routed_message(&remote, &local, 1, "/services/verified-fetch"),
        ),
        runtime.handle(),
    )
    .unwrap();
    let peer = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
        .unwrap()
        .unwrap();
    let preflight = runtime
        .block_on(warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
            &cluster_paths,
            &peer,
            runtime.handle(),
        ))
        .unwrap();
    let fetch = crate::DfsFetchBlobCommand {
        request_id: "verified-fetch".into(),
        intent_id: intent.intent_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone().unwrap(),
        authority_shard: Some(shard.clone()),
    };
    let command_for = |fetch: crate::DfsFetchBlobCommand| {
        test_dfs_message_command(
            fetch.request_id.clone(),
            DfsRoutedMessage {
                source_node_id: remote.clone(),
                target_node_id: local.clone(),
                frame_id: 2,
                message_id: 2,
                message: DfsNetworkMessage::FetchBlob(fetch),
            },
        )
    };
    let scheduler = daemon_dfs_mutation_scheduler(&data_dir).unwrap();
    let admission = runtime
        .block_on(scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive))
        .unwrap();
    let cluster = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths).unwrap();
    let retained = take_daemon_dfs_message_mutation_service_for_maintenance(
        &data_dir,
        &cluster_paths,
        &cluster,
        runtime.handle(),
        Some(&preflight),
    )
    .unwrap();
    assert!(scheduler.try_admit_global().is_none());
    let mut response = runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(2),
            handle_daemon_dfs_message_async(
                &paths,
                command_for(fetch.clone()),
                None,
                None,
                runtime.handle(),
            ),
        )
        .await
        .expect("verified fetch must bypass the held global writer")
        .unwrap()
    });
    assert!(matches!(&response.response.outbound_messages[0].message,
        DfsNetworkMessage::FetchBlobResponse(crate::DfsFetchBlobResponse {
            result: crate::DfsFetchBlobResult::Found { content_hash, bytes }, ..
        }) if content_hash == &fetch.content_hash && bytes.is_empty()));
    assert!(
        scheduler.try_admit_global().is_none(),
        "fetch must not release maintenance's guard"
    );
    // An unknown/stale/forged stamp cannot construct the read-lane capability.
    for mode in 0..4 {
        let mut unproven = fetch.clone();
        match mode {
            0 => unproven.intent_id = "unknown-intent".into(),
            1 => unproven.authority_shard = None,
            2 => unproven.authority_shard.as_mut().unwrap().epoch = 0,
            _ => unproven.authority_shard.as_mut().unwrap().primary_node_id = remote.clone(),
        }
        assert!(
            prepare_daemon_dfs_verified_blob_fetch(
                &data_dir,
                &cluster_paths,
                &peer,
                &command_for(unproven),
                &preflight,
                runtime.handle(),
            )
            .unwrap()
            .is_none()
        );
    }
    // The original handler rejects the mismatched hash; the sealed lane must
    // return the same typed rejection without opening or returning content.
    let mut wrong_hash = fetch.clone();
    wrong_hash.request_id = "verified-fetch-wrong-hash".into();
    wrong_hash.content_hash = crate::content_store::content_hash(b"different bytes");
    let rejected = runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(2),
            handle_daemon_dfs_message_async(
                &paths,
                command_for(wrong_hash),
                None,
                None,
                runtime.handle(),
            ),
        )
        .await
        .unwrap()
        .unwrap()
    });
    assert!(rejected.attachment.is_none());
    assert!(matches!(&rejected.response.outbound_messages[0].message,
        DfsNetworkMessage::FetchBlobResponse(crate::DfsFetchBlobResponse {
            result: crate::DfsFetchBlobResult::Rejected { reason }, ..
        }) if reason.contains("hash")));
    let stage = test_dfs_message_command(
        "verified-fetch-mutation",
        DfsRoutedMessage {
            source_node_id: remote.clone(),
            target_node_id: local.clone(),
            frame_id: 3,
            message_id: 3,
            message: DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                request_id: "verified-fetch-mutation".into(),
                intent_id: intent.intent_id.clone(),
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                expected_base_version: None,
                version: object.version.clone(),
                content_hash: fetch.content_hash.clone(),
                bytes: bytes.to_vec(),
                object_metadata: Some(object.clone()),
                authority_shard: Some(shard),
            }),
        },
    );
    assert_eq!(
        dfs_message_execution_scope(&stage),
        DfsMessageExecutionScope::GlobalExclusive
    );
    assert!(
        runtime
            .block_on(async {
                tokio::time::timeout(
                    Duration::from_millis(50),
                    handle_daemon_dfs_message_async(&paths, stage, None, None, runtime.handle()),
                )
                .await
            })
            .is_err(),
        "mutable transfer must remain fenced by global maintenance"
    );
    restore_daemon_dfs_message_mutation_service_after_maintenance(
        &data_dir,
        &cluster_paths,
        runtime.handle(),
        retained,
    )
    .unwrap();
    drop(admission);
    let sealed = prepare_daemon_dfs_verified_blob_fetch(
        &data_dir,
        &cluster_paths,
        &peer,
        &command_for(fetch.clone()),
        &preflight,
        runtime.handle(),
    )
    .unwrap()
    .expect("current owner can seal another reader");
    preflight.authority.observe_discontinuity_hint(None);
    let mut no_admission = None;
    let rejected = attempt_daemon_dfs_message_command_on_runtime(
        &data_dir,
        &cluster_paths,
        &peer,
        command_for(fetch.clone()),
        None,
        Some(sealed),
        &DfsMessageExecutionScope::ReadOnly,
        false,
        false,
        Some(preflight.clone()),
        Duration::ZERO,
        &mut no_admission,
        runtime.handle(),
        None,
    );
    assert!(
        matches!(
            rejected,
            Ok(DaemonDfsMessageDispatchAttempt::AuthorityPreflightStale(_))
        ),
        "withdrawn authority must not serve an already sealed reader"
    );
    evict_daemon_dfs_message_service_cache(&data_dir).unwrap();
    let attachment = response
        .attachment
        .take()
        .expect("normal served attachment custody");
    assert_eq!(
        runtime
            .block_on(attachment.source.collect_to_vec(4096))
            .unwrap(),
        bytes,
        "reader must retain its content lease after service eviction"
    );
}

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
