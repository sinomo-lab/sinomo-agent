use super::*;

#[test]
fn current_replica_keeps_exact_storage_scope_across_provider_configuration_change() {
    exercise_replica_storage_scope(false);
}

#[test]
fn current_replica_with_primary_publisher_keeps_exact_storage_scope() {
    exercise_replica_storage_scope(true);
}

fn exercise_replica_storage_scope(primary_publisher: bool) {
    let fixture = RetryFixture::new(if primary_publisher {
        "replica-exact-storage-with-primary"
    } else {
        "replica-exact-storage-provider-change"
    });
    if primary_publisher {
        let native = fixture.native.lock().unwrap();
        let native = native.as_ref().unwrap();
        let other = native
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|shard| shard.shard_id != fixture.shard.shard_id)
            .unwrap();
        native
            .reassign_metadata_shard(
                &other.file_system_id,
                &other.shard_id,
                fixture.remote.clone(),
                vec![fixture.local.clone()],
            )
            .unwrap();
    }
    let (data_dir, paths, context) = current_direct_replica_context(&fixture);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let peer = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
        .unwrap()
        .unwrap();
    let preflight = fixture
        .runtime
        .block_on(warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
            &cluster_paths,
            &peer,
            fixture.runtime.handle(),
        ))
        .unwrap();
    let (mut source, file_system, other_shard) = {
        let native = fixture.native.lock().unwrap();
        let native = native.as_ref().unwrap();
        let other = native
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|shard| {
                shard.shard_id != context.shard.shard_id
                    && (shard.primary_node_id == fixture.remote
                        || shard.replica_node_ids.contains(&fixture.remote))
            })
            .expect("hold another actual configured physical shard");
        (
            native.metadata_primary_service("/").unwrap(),
            native.file_system(),
            other,
        )
    };
    let shard_id = DfsMetadataShardId::new(&context.shard.shard_id);
    let prepared = source
        .metadata_backend
        .as_mut()
        .unwrap()
        .prepare_operation(
            crate::DfsPreparedOperationPayload::for_metadata_effects(
                crate::DfsPreparedOperationScope {
                    file_system_id: context.shard.file_system_id.clone(),
                    shard_id: shard_id.clone(),
                    owner_epoch: context.shard.epoch,
                    operation_id: "replica-storage-original-prepare".into(),
                },
                Vec::new(),
                Vec::new(),
                vec![crate::DfsPreparedMetadataEffect::PutFileSystem { file_system }],
                time::OffsetDateTime::now_utc(),
            )
            .unwrap(),
            None,
        )
        .unwrap();
    let backend = source.metadata_backend.as_ref().unwrap();
    let logical = backend
        .journal_batch_for_shard_since_limited(
            &shard_id,
            0,
            crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    assert_eq!(logical.next_after_sequence, prepared.prepare_sequence);
    let frames = crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(
        backend.as_ref(),
        &logical,
    )
    .unwrap();
    let prefix = backend.metadata_prefix_proof(&shard_id).unwrap().unwrap();
    let command = |label: &str| crate::DfsMetadataJournalBatchCommand {
        request_id: label.into(),
        source_node_id: Some(fixture.local.clone()),
        file_system_id: Some(context.shard.file_system_id.clone()),
        authority_epoch: Some(context.shard.epoch),
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: Some(crate::DfsMetadataAuthorityProbe {
            probe_id: label.into(),
            expected_shard: context.shard.clone(),
            operation: crate::DfsMetadataAuthorityOperation::PreparedOperation {
                operation_id: prepared.operation_id.clone(),
                file_system_id: prepared.file_system_id.clone(),
                shard_id: prepared.shard_id.clone(),
                owner_epoch: prepared.owner_epoch,
                prepare_sequence: prepared.prepare_sequence,
                payload_digest: prepared.payload_digest.clone(),
            },
            prepared_prefix: prefix.clone(),
        }),
        durable_replication: None,
        batch: frames.clone(),
    };
    let call = |command: &crate::DfsMetadataJournalBatchCommand| {
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            "/dfs/metadata/journal-batch",
            crate::DFS_RPC_CATEGORY,
            command,
        )
        .unwrap();
        fixture
            .native
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .rpc_request(
                &fixture.local,
                &fixture.remote,
                request,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
    };
    let warm = command("replica-storage-warm");
    let warmed: crate::DfsMetadataJournalBatchResponse =
        call(&warm).unwrap().decode_body().unwrap();
    assert_eq!(warmed.result, crate::DfsMetadataJournalBatchResult::Applied);

    let scheduler = daemon_dfs_mutation_scheduler(&data_dir).unwrap();
    let other_scope = DfsMessageExecutionScope::ExactShard {
        file_system_id: other_shard.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new(&other_shard.shard_id),
    };
    let other_owner = fixture
        .runtime
        .block_on(scheduler.admit(&other_scope))
        .unwrap();
    assert!(other_owner.is_exact());
    let before = preflight.authority.lifecycle_generations().unwrap();
    let cached = {
        let slot = daemon_dfs_message_service_cache()
            .existing_root(&data_dir.root())
            .unwrap()
            .unwrap();
        let cache = slot.lock().unwrap();
        let entry = cache.as_ref().unwrap();
        assert!(entry.resident_metadata_initialized);
        assert!(Arc::ptr_eq(
            &entry.metadata_authority_generation,
            &preflight.authority
        ));
        assert_eq!(
            entry
                .resident_metadata_backend
                .as_ref()
                .unwrap()
                .shard_config(&shard_id)
                .unwrap(),
            context.shard
        );
        entry.signature.clone()
    };
    assert_eq!(cached.metadata_authority_generation, before.0);
    assert_eq!(cached.metadata_storage_generation, before.1);
    let mut advertisement = observed_provider_advertisement(&preflight, &fixture.remote).unwrap();
    advertisement.zone = Some("same-storage-successor-provider-zone".into());
    let operation = sinomo_core::kv::KvOp::put_serde(
        crate::content_service_key(&fixture.remote),
        &advertisement,
        None,
    )
    .unwrap();
    let sinomo_core::kv::KvOp::Put { key, value, .. } = &operation else {
        unreachable!();
    };
    let expected_key = key.clone();
    let expected_value = value.clone();
    let (delivered_tx, delivered_rx) = std::sync::mpsc::channel();
    let subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
        &cluster_paths,
        move |event| {
            if event.applied_index.is_none() {
                return;
            }
            let sinomo_core::kv::KvCommittedObservationValue::Delta(delta) = &event.value else {
                return;
            };
            if delta.final_key_states.iter().any(|state| {
                state.key == expected_key
                    && state
                        .entry
                        .as_ref()
                        .is_some_and(|entry| entry.value == expected_value)
            }) {
                let _ = delivered_tx.send(());
            }
        },
    );
    commit_current_configuration_delta(
        &cluster_paths,
        &fixture.remote,
        sinomo_core::kv::KvCommand::Op(operation),
    )
    .unwrap();
    delivered_rx
        .recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    drop(subscription);
    let after = preflight.authority.lifecycle_generations().unwrap();
    assert!(after.0 > before.0);
    assert_eq!(
        after.1, before.1,
        "actual provider delta does not replace stores"
    );
    assert!(
        preflight
            .authority
            .replica_context_state(&context)
            .unwrap()
            .1
    );
    let (general_scope_eligible, status_available) = {
        let slot = daemon_dfs_message_service_cache()
            .existing_root(&data_dir.root())
            .unwrap()
            .unwrap();
        let cache = slot.lock().unwrap();
        let entry = cache.as_ref().unwrap();
        (
            entry
                .exact_admission_handle(
                    &peer,
                    &preflight.authority,
                    after.0,
                    &DaemonDfsMutationLaneKey {
                        file_system_id: context.shard.file_system_id.clone(),
                        shard_id: shard_id.clone(),
                    },
                )
                .unwrap()
                .is_some(),
            entry.bounded_status_resident_current().is_ok(),
        )
    };
    let eligible = daemon_dfs_message_cache_exact_admission_handle_for_replica_context(
        &data_dir,
        &cluster_paths,
        &peer,
        &context,
        fixture.runtime.handle(),
    )
    .unwrap()
    .is_some();
    // A new probe correlation verifies the SAME actual immutable prepare;
    // it cannot borrow the first response or start another mutation identity.
    let current = command("replica-storage-current");
    let confirmation = crate::service_replica::metadata_authority_probe_confirmation_id(
        current.authority_probe.as_ref().unwrap(),
    )
    .unwrap();
    let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let response = call(&current).and_then(|response| {
        response
            .decode_body::<crate::DfsMetadataJournalBatchResponse>()
            .map_err(|error| error.to_string())
    });
    let global_still_owned = scheduler.try_admit_global().is_none();
    drop(other_owner);
    // A yielded refresh must remain owned and eventually update ordinary
    // views. Observe the real cache without warming it or supplying a wake.
    let background_refreshed = response.is_ok()
        && fixture.runtime.block_on(async {
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
                loop {
                    let slot = daemon_dfs_message_service_cache()
                        .existing_root(&data_dir.root())
                        .unwrap()
                        .unwrap();
                    let ready = slot.try_lock().ok().is_some_and(|cache| {
                        cache.as_ref().is_some_and(|entry| {
                            entry.signature.shares_metadata_storage(&cached)
                                && entry.bounded_status_resident_current().is_ok()
                        })
                    });
                    if ready {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .is_ok()
        });
    let observation = serde_json::json!({
        "physical_root": data_dir.root(), "local_node": fixture.remote,
        "primary_publisher": primary_publisher,
        "held_configured_shard": other_shard, "requested_context": format!("{context:?}"),
        "before_generations": before, "after_generations": after,
        "cached_authority_generation": cached.metadata_authority_generation,
        "cached_storage_generation": cached.metadata_storage_generation,
        "exact_cache_eligible": eligible, "global_still_owned": global_still_owned,
        "general_scope_eligible": general_scope_eligible, "status_available": status_available,
        "background_refreshed_within_original_deadline": background_refreshed,
        "original_prepare": prepared, "request": current.request_id,
        "response": format!("{response:?}"),
        "qualification": "Actual registered owner, native committed provider delta, configured unrelated admission and real authenticated replica RPC. No readiness, authority or response supplied."
    });
    let path = data_dir
        .root()
        .join("boundary-diagnostic-replica-storage-scope.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&observation).unwrap()).unwrap();
    eprintln!("replica storage scope observation: {}", path.display());
    drop(source);
    drop(preflight);
    drop(scheduler);
    drop(fixture);
    assert!(
        global_still_owned,
        "the unrelated owner stayed active through the measured call"
    );
    assert!(
        !general_scope_eligible,
        "generic service views remain generation-fenced"
    );
    assert!(
        !status_available,
        "replica progress cannot label an old global view current"
    );
    assert!(
        eligible,
        "a provider-only authority delta must not turn current replica storage into global work"
    );
    let response =
        response.expect("replica progress must not wait for the unrelated configured shard");
    assert_eq!(
        response.result,
        crate::DfsMetadataJournalBatchResult::Applied
    );
    assert_eq!(response.verified_authority_probe_id, Some(confirmation));
    assert_eq!(response.next_after_sequence, prefix.journal_sequence);
    assert!(
        background_refreshed,
        "yielded generation refresh must remain owned and complete"
    );
}
