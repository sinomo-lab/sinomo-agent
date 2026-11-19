//! Committed policy deltas must update projections without cold-recovering stores.

use super::*;

#[test]
fn immutable_fetch_inspection_never_creates_or_recertifies_an_owner() {
    let local =
        crate::test_support::DfsTestRuntime::ordered_node_ids("dfs-immutable-fetch-owner-proof", 1)
            .remove(0);
    let mut native =
        crate::test_support::DfsTestRuntime::new("dfs-immutable-fetch-owner-proof", local.clone())
            .unwrap();
    let data_dir = native.data_dir(&local).clone();
    let shard_id = crate::DfsMetadataShardId::new("root");
    let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir)
        .join(crate::metadata_keys::key_segment(&shard_id.0));
    let inspect = |resident: Option<&crate::ResidentLocalMetadataShardLease>| {
        crate::local_state::with_current_owned_dfs_shard_inspection(
            &data_dir,
            &shard_id,
            resident,
            |backend| Ok(backend.metadata_prefix_proof(&shard_id)?),
        )
    };
    assert!(inspect(None).unwrap().is_none());
    assert!(
        !metadata_root.exists(),
        "read admission must not create a metadata root"
    );
    native.start_nodes().unwrap();
    let mut client = native.client(&local).unwrap();
    client
        .posix_write_file(
            "/immutable-inspection",
            b"actual owned bytes",
            crate::DfsWriteOptions {
                policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![local.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(client);
    let scheduler = daemon_dfs_mutation_scheduler(&data_dir).unwrap();
    let admission = crate::test_runtime::executor()
        .block_on(scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive))
        .unwrap();
    let cluster_paths = ClusterPaths::new(&data_dir);
    let peer = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
        .unwrap()
        .unwrap();
    let handle = daemon_dfs_cached_resident_metadata_shard_handle(
        &data_dir,
        &peer,
        &crate::sifs_file_system_id(),
        &shard_id,
        &cluster_paths,
        crate::test_runtime::executor().handle(),
    )
    .unwrap();
    let resident = handle
        .try_checkout_open()
        .unwrap()
        .expect("actual resident owner");
    for view in [None, Some(&resident)] {
        assert!(inspect(view).unwrap().flatten().is_some());
    }
    let guard = crate::metadata_lock::metadata_write_lock(
        metadata_root.clone(),
        crate::metadata_cursor::journal_write_lock_path(&metadata_root),
    )
    .unwrap();
    for view in [None, Some(&resident)] {
        assert!(
            inspect(view).unwrap().is_none(),
            "busy writer must remain fenced"
        );
        assert!(
            inspect(view).unwrap().is_none(),
            "failed inspection must not release another owner's path guard"
        );
    }
    drop(guard);
    for view in [None, Some(&resident)] {
        assert!(inspect(view).unwrap().flatten().is_some());
    }
    crate::metadata_journal::admission::mark_uncertain(&metadata_root).unwrap();
    for view in [None, Some(&resident)] {
        assert!(inspect(view).unwrap().is_none());
    }
    assert!(
        crate::metadata_journal::admission::current_owned_materialized_prefix(
            &metadata_root,
            &shard_id,
        )
        .unwrap()
        .is_none(),
        "observational reads must not clear uncertainty"
    );
    drop(resident);
    drop(admission);
    native.close().unwrap();
}

#[test]
fn verified_blob_fetch_serves_while_global_maintenance_owns_metadata() {
    let local =
        crate::test_support::DfsTestRuntime::ordered_node_ids("dfs-verified-fetch-content-lane", 1)
            .remove(0);
    let mut native =
        crate::test_support::DfsTestRuntime::new("dfs-verified-fetch-content-lane", local.clone())
            .unwrap();
    native.start_nodes().unwrap();
    let data_dir = native.data_dir(&local).clone();
    let paths = DaemonPaths::with_root(data_dir.root().join("daemon"));
    let cluster_paths = ClusterPaths::new(&data_dir);
    let runtime = crate::test_runtime::executor();
    let bytes = b"leased immutable fetch bytes";
    let mut client = native.client(&local).unwrap();
    let stat = client
        .posix_write_file(
            "/verified-fetch",
            bytes,
            crate::DfsWriteOptions {
                policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![local.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let node_store = native.node_metadata(&local).unwrap();
    let object = node_store.get_object(&stat.object_id).unwrap().unwrap();
    let intent = node_store
        .intents_for_object(&stat.object_id)
        .unwrap()
        .into_iter()
        .find(|intent| {
            intent.target_version == stat.version
                && intent.status == crate::DfsWriteIntentStatus::Committed
        })
        .expect("actual sealed committed write outcome");
    let shard = native.root_shard().unwrap();
    let remote = intent.coordinator_node_id.clone();
    assert_eq!(
        remote, local,
        "native write retained its actual coordinator"
    );
    let forged_primary = NodeId::generate();
    drop(node_store);
    drop(client);
    // Warm the actual global resident generation before maintenance takes it.
    handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command(
            "verified-fetch-warm",
            test_dfs_lookup_routed_message(&remote, &local, 1, "/verified-fetch"),
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
    // Keep other Main turns from checking out the shard during the warm-open
    // measurement. Below, the same admission takes the metadata checkout and
    // exercises the distinct busy-owner fetch path.
    let scheduler = daemon_dfs_mutation_scheduler(&data_dir).unwrap();
    let admission = runtime
        .block_on(scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive))
        .unwrap();
    // Fetch authorization must reuse the already-open physical shard. Evict
    // the separate inspection cache before each call so it cannot mask a cold
    // metadata reopen on the ordinary resident path.
    crate::metadata_backend::reset_metadata_backend_open_calls();
    for _ in 0..3 {
        crate::local_state::evict_cluster_aware_dfs_metadata_inspection_cache(&data_dir);
        assert!(
            prepare_daemon_dfs_verified_blob_fetch(
                &data_dir,
                &cluster_paths,
                &peer,
                &command_for(fetch.clone()),
                &preflight,
                runtime.handle(),
            )
            .unwrap()
            .is_some()
        );
    }
    assert_eq!(
        crate::metadata_backend::metadata_backend_open_calls(),
        0,
        "a warm fetch must not decode a second metadata backend"
    );
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
            _ => {
                unproven.authority_shard.as_mut().unwrap().primary_node_id = forged_primary.clone()
            }
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
                receipt_generation: 0,
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
    native.close().unwrap();
}

#[test]
fn committed_placement_policy_retains_resident_lanes_and_updates_configuration() {
    let nodes =
        crate::test_support::DfsTestRuntime::ordered_node_ids("dfs-policy-retains-store-owner", 2);
    let local = nodes[0].clone();
    let remote = nodes[1].clone();
    let mut native =
        crate::test_support::DfsTestRuntime::new("dfs-policy-retains-store-owner", local.clone())
            .unwrap();
    native.define_node(remote.clone()).unwrap();
    native.start_nodes().unwrap();
    let data_dir = native.data_dir(&local).clone();
    let paths = DaemonPaths::with_root(data_dir.root().join("daemon"));
    let cluster_paths = ClusterPaths::new(&data_dir);
    let runtime = crate::test_runtime::executor();
    let control = native.shared_control_plane().unwrap();
    let mut config = control
        .with_client(|kv| {
            crate::DfsControlPlaneStore::new(kv.as_mut())
                .get_file_system_config(&native.file_system().file_system_id)
        })
        .unwrap()
        .expect("actual current filesystem configuration");
    let shard = native.root_shard().unwrap();
    let lookup = |frame| {
        handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command(
                format!("policy-store-lookup-{frame}"),
                test_dfs_lookup_routed_message(&remote, &local, frame, "/absent"),
            ),
            runtime.handle(),
        )
        .unwrap()
    };
    lookup(1);
    let first = {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&data_dir.root())
            .expect("actual root cache slot");
        let cache = cache_slot.lock().unwrap();
        cache.as_ref().map(|entry| {
            (
                entry.primary_service.clone(),
                Arc::clone(&entry.write_intent_maintenance_scan),
                entry.signature.clone(),
                entry.resident_metadata_initialized && entry.resident_metadata_backend.is_some(),
            )
        })
    };
    let (first_lane, scan_owner, first_signature, initialized) =
        first.expect("mounted cache entry");
    assert!(initialized);
    let first_lane = first_lane.expect("actual primary service");
    let opens = daemon_dfs_test_message_service_lane_open_count(&data_dir);
    let expected_config = config.clone();
    let path = "/services";
    crate::upsert_placement_rule(
        &mut config.subtree_policies,
        crate::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2)
            .unwrap()
            .into_rule(path.into(), crate::DfsPlacementScope::SelfOnly),
    );
    let mut placement_client = native.client(&local).unwrap();
    let candidates = placement_client.placement_content_candidates().unwrap();
    drop(placement_client);
    let job = crate::control_plane::build_placement_reconciliation_job(
        "native-policy-retains-store-owner".into(),
        &config,
        path.into(),
        crate::DfsPlacementScope::SelfOnly,
        std::collections::BTreeSet::new(),
        candidates,
        None,
        crate::metadata_records::dfs_now_unix_nanos(),
    )
    .unwrap();
    assert!(
        control
            .with_client(|kv| {
                crate::DfsControlPlaneStore::new(kv.as_mut()).compare_and_put_placement_update(
                    &expected_config,
                    &config,
                    &job,
                )
            })
            .unwrap()
            .is_some(),
        "normal placement Apply commits its actual checked policy transition"
    );
    lookup(2);
    let after_policy = {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&data_dir.root())
            .expect("actual root cache slot");
        let cache = cache_slot.lock().unwrap();
        cache.as_ref().map(|entry| {
            (
                entry.primary_service.clone(),
                Arc::clone(&entry.write_intent_maintenance_scan),
                entry.signature.clone(),
            )
        })
    };
    // Assertions run after the shared cache lock is released, so a failed
    // behavioral check cannot poison unrelated mounted owners.
    let (policy_lane, policy_scan, policy_signature) = after_policy.expect("mounted cache entry");
    assert!(Arc::ptr_eq(
        &first_lane,
        &policy_lane.expect("actual primary service")
    ));
    assert!(Arc::ptr_eq(&scan_owner, &policy_scan));
    assert!(
        policy_signature.metadata_authority_generation
            > first_signature.metadata_authority_generation
    );
    assert_eq!(
        policy_signature.metadata_storage_generation,
        first_signature.metadata_storage_generation
    );
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
    control
        .with_client(|kv| {
            crate::DfsControlPlaneStore::new(kv.as_mut()).put_file_system_config(&config)
        })
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
    assert!(
        !shard.replica_node_ids.is_empty(),
        "actual native membership transition requires old peers"
    );
    native
        .reassign_metadata_shard(
            &config.file_system_id,
            &shard.shard_id,
            local.clone(),
            Vec::new(),
        )
        .unwrap();
    lookup(3);
    let after_handoff = {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&data_dir.root())
            .expect("actual root cache slot");
        let cache = cache_slot.lock().unwrap();
        cache.as_ref().map(|entry| {
            (
                entry.signature.clone(),
                Arc::clone(&entry.write_intent_maintenance_scan),
            )
        })
    };
    let (handoff_signature, handoff_scan) = after_handoff.expect("mounted cache entry");
    assert!(
        handoff_signature.metadata_storage_generation > first_signature.metadata_storage_generation
    );
    assert!(!Arc::ptr_eq(&scan_owner, &handoff_scan));
    evict_daemon_dfs_message_service_cache(&data_dir).unwrap();
    native.close().unwrap();
}
