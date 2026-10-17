//! Authority regression tests.

use super::*;

#[test]
fn native_explicit_primary_handoff_preserves_an_unfinished_metadata_prepare() {
    let (mut runtime, requester, retiring) = crate::test_support::current_native_metadata_service(
        "native-handoff-unfinished-prepare",
        3,
        "/services",
    )
    .unwrap();
    drop(requester);
    let control = runtime.shared_control_plane().unwrap();
    let configs = runtime.metadata_shards().unwrap();
    let promoted = retiring.replica_node_ids[0].clone();
    let shard_id = DfsMetadataShardId::new(retiring.shard_id.clone());
    let mut source = runtime.metadata_primary_service("/services").unwrap();
    let filesystem = source
        .metadata_backend()
        .unwrap()
        .get_file_system_on_shard(&shard_id, &retiring.file_system_id)
        .unwrap()
        .unwrap();
    let scope = DfsPreparedOperationScope {
        file_system_id: retiring.file_system_id.clone(),
        shard_id: shard_id.clone(),
        owner_epoch: retiring.epoch,
        operation_id: format!("native-handoff-prepare:{}", sinomo_core::Id::generate()),
    };
    let payload = DfsPreparedOperationPayload::for_metadata_effects(
        scope.clone(),
        Vec::new(),
        Vec::new(),
        vec![DfsPreparedMetadataEffect::PutFileSystem {
            file_system: filesystem.clone(),
        }],
        OffsetDateTime::now_utc(),
    )
    .unwrap();
    // Stop at the ordinary producer's durable Prepare boundary. The SAME
    // native provider owns admission and the physical writer; no receipt,
    // authority probe, handoff proof or persisted packet is supplied by the test.
    let execution = source.acquire_prepared_execution(&scope).unwrap();
    let digest = source
        .prepare_payload_for_publication(payload.clone())
        .unwrap();
    drop(execution);
    let original = source
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&digest.operation_id)
        .unwrap()
        .unwrap();
    original.validate().unwrap();
    assert_eq!(original.status, DfsPreparedOperationStatus::Prepared);
    assert_eq!(original.payload.as_ref(), &payload);
    let handoff = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut())
                .request_metadata_shard_primary_handoff(&retiring, &promoted)
        })
        .unwrap()
        .unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let explicit = source.reconcile_pending_metadata_shard_membership_handoffs(
        &configs,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    );
    loop {
        if let Some(active) = runtime
            .observe_completed_native_handoff(&control, &handoff, deadline)
            .unwrap()
        {
            assert_eq!(active, handoff.desired_shard);
            break;
        }
        let (active, pending) = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut()).retiring_metadata_shard_handoff_view_before(
                    &retiring.file_system_id,
                    &retiring.shard_id,
                    deadline,
                )
            })
            .unwrap();
        assert!(
            pending
                .as_ref()
                .is_some_and(|value| value.handoff_id == handoff.handoff_id)
                || active.as_ref() == Some(&handoff.desired_shard),
            "the exact requested transfer cannot disappear while draining a prepare: active={active:?}, pending={pending:?}, explicit={explicit:?}"
        );
        assert!(
            Instant::now() < deadline,
            "the original native handoff must finish within its existing deadline: {explicit:?}"
        );
        std::thread::yield_now();
    }
    assert!(
        source.commit_stored_prepared_operation(&digest).is_err(),
        "the retiring writer cannot commit its original prepare after activation"
    );
    drop(source);
    // Restart the actual promoted owner before its existing recovery executor
    // reads the transferred durable prepares. This staged Core producer has no
    // foreground request latch; opening a lazy service alone is not execution.
    runtime.stop_node(&promoted).unwrap();
    runtime.reopen_node(promoted.clone()).unwrap();
    let mut successor = runtime.metadata_primary_service("/services").unwrap();
    assert_eq!(successor.node_id(), &promoted);
    successor.recover_durable_namespace_prepares().unwrap();
    let backend = successor.metadata_backend().unwrap();
    let old = backend
        .prepared_operation_record(&digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(old.digest, digest);
    assert_eq!(old.payload, original.payload);
    assert_eq!(old.status, DfsPreparedOperationStatus::Superseded);
    let adopted_id = format!(
        "recover:{}:{}",
        digest.operation_id, handoff.desired_shard.epoch
    );
    let adopted = backend
        .prepared_operation_record(&adopted_id)
        .unwrap()
        .unwrap();
    let mut expected = payload;
    expected.operation_id = adopted_id;
    expected.owner_epoch = handoff.desired_shard.epoch;
    expected
        .metadata_effects
        .push(DfsPreparedMetadataEffect::ResolvePreparedOperation {
            digest,
            status: DfsPreparedOperationStatus::Superseded,
        });
    assert_eq!(adopted.payload.as_ref(), &expected);
    assert_eq!(adopted.status, DfsPreparedOperationStatus::Committed);
    assert_eq!(
        backend
            .get_file_system_on_shard(&shard_id, &retiring.file_system_id)
            .unwrap(),
        Some(filesystem)
    );
    drop(successor);
    runtime.close().unwrap();
}

#[test]
fn ingested_manifest_counts_verified_local_copy_after_native_replanning() {
    let (runtime, mut requester, shard) = crate::test_support::current_native_metadata_service(
        "native-ingested-replan-local-receipt",
        3,
        "/services",
    )
    .unwrap();
    let path = "/services/ingested-replan.bin";
    let initial = requester
        .posix_write_file(
            path,
            b"initial",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![shard.primary_node_id.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(requester);
    let tick = runtime
        .maintenance_tick(
            &shard.primary_node_id,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap();
    assert!(
        tick.outcome.is_ok(),
        "native placement observation: {:?}",
        tick.outcome
    );
    let mut owner = runtime.metadata_primary_service(path).unwrap();
    assert!(
        owner.placement_snapshot_cache().snapshot().is_some(),
        "actual native placement view"
    );
    let previous_selection = vec![shard.replica_node_ids[0].clone()];
    let policy = DfsReplicationPolicy::new(1, 1).unwrap();
    // Select input DATA using the real observed planner, before any write.
    // No receipt, assignment, authority or durable outcome is constructed.
    let request = (0..128)
        .find_map(|_| {
            let request = DfsWriteRequest {
                file_system_id: initial.file_system_id.clone(),
                object_id: initial.object_id.clone(),
                target_version: DfsVersion::generate(),
                requesting_node_id: shard.replica_node_ids[0].clone(),
            };
            let plan = owner
                .plan_distributed_eventual_replicas(
                    &request,
                    previous_selection.clone(),
                    policy.clone(),
                )
                .unwrap();
            (plan.desired_replica_nodes == vec![shard.primary_node_id.clone()]).then_some(request)
        })
        .expect("the real native planner selects this metadata primary for an input version");
    let bytes = b"actually ingested before the primary replans";
    let manifest = owner
        .content()
        .put_object_version(
            initial.object_id.as_str(),
            request.target_version.as_str(),
            bytes,
        )
        .unwrap();
    let result = owner
        .write_stable_ingested_distributed_manifest(
            request.clone(),
            Some(initial.version),
            manifest.content_hash.clone(),
            manifest.size_bytes,
            previous_selection,
            policy,
            "native-ingested-replan-original".into(),
            DfsDistributedWriteOptions {
                repair: DfsWriteRepair::Background,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(
        matches!(result, NetworkedDfsWriteOutcome::Committed { .. }),
        "a verified local copy selected by the final native plan must satisfy W: {result:?}"
    );
    let current = owner
        .metadata_backend()
        .unwrap()
        .get_object(&initial.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.version, request.target_version);
    assert_eq!(current.current_locations, vec![shard.primary_node_id]);
    assert_eq!(
        owner
            .content()
            .read_object_version(current.object_id.as_str(), current.version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(owner);
    runtime.close().unwrap();
}

#[test]
fn native_create_fetches_and_stages_when_requester_and_primary_are_outside_the_plan() {
    let (runtime, initial, shard) = crate::test_support::current_native_metadata_service(
        "native-unselected-upload-source",
        3,
        "/services",
    )
    .unwrap();
    drop(initial);
    let primary = shard.primary_node_id.clone();
    let requester_node = shard.replica_node_ids[0].clone();
    let destination = shard.replica_node_ids[1].clone();
    let path = "/services/unselected-upload-source.bin";
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    placement.required_node_ids = vec![destination.clone()];
    placement.forbidden_node_ids = vec![primary.clone(), requester_node.clone()];
    runtime
        .apply_placement_policy(&primary, path, DfsPlacementScope::SelfOnly, placement)
        .unwrap();
    let mut requester = runtime.client(&requester_node).unwrap();
    let (_, _, policy) = requester
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 1)
        .unwrap();
    let before = runtime.rpc_observations().len();
    let bytes = b"only the selected replica can satisfy W";
    let original = requester.posix_write_file_with_operation_id(
        path,
        bytes,
        DfsWriteOptions {
            policy,
            replica_candidates: vec![destination.clone()],
            wait_for_commit: false,
            foreground_replica_repair: false,
        },
        "native-unselected-source-original",
    );
    let maintenance = runtime
        .maintenance_tick(&primary, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(
        maintenance.outcome.is_ok(),
        "normal Main maintenance: {:?}",
        maintenance.outcome
    );
    let mut inspect = runtime.metadata_primary_service(path).unwrap();
    let intents = inspect
        .metadata_backend()
        .unwrap()
        .write_intents_page(None, 128)
        .unwrap()
        .intents;
    let stat = inspect.posix_stat(path).unwrap_or_else(|error| panic!(
        "original={original:?}; actual canonical stat={error}; durable intents={intents:?}; calls={:?}",
        &runtime.rpc_observations()[before..],
    ));
    assert_eq!(
        stat.content_hash,
        Some(format!("blake3:{}", blake3::hash(bytes).to_hex()))
    );
    assert_eq!(stat.current_locations, vec![destination.clone()]);
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .any(|call| call.source == primary
                && call.target == destination
                && call.route == crate::DFS_DATA_STAGE_BLOB_ROUTE
                && call.phase == crate::test_runtime::RpcPhase::BeforeDispatch)
    );
    let receiver = runtime.client(&destination).unwrap();
    assert_eq!(
        receiver
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(receiver);
    drop(inspect);
    drop(requester);
    runtime.close().unwrap();
}

#[test]
fn committed_intent_replay_preserves_original_capture_after_replica_withdrawal() {
    let (runtime, mut requester, shard) = crate::test_support::current_native_metadata_service(
        "native-terminal-replay-after-withdrawal",
        3,
        "/services",
    )
    .unwrap();
    let path = "/services/terminal-replay";
    let participants = vec![
        shard.primary_node_id.clone(),
        shard.replica_node_ids[0].clone(),
    ];
    let stat = requester
        .posix_write_file(
            path,
            b"original immutable bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: participants.clone(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(requester);
    let mut owner = runtime.metadata_primary_service(path).unwrap();
    let recorded = owner
        .metadata_backend()
        .unwrap()
        .write_intents_page(None, 128)
        .unwrap()
        .intents
        .into_iter()
        .find(|intent| {
            intent.object_id == stat.object_id && intent.status == DfsWriteIntentStatus::Committed
        })
        .unwrap();
    owner
        .mark_remote_read_location_missing(
            &stat.object_id,
            &stat.version,
            &stat.content_hash,
            &participants[1],
        )
        .unwrap();
    let current = owner.current_read_object_metadata(&stat.object_id).unwrap();
    assert_ne!(recorded.object_metadata.as_ref(), Some(&current));
    let intent = crate::intent::distributed_intent_from_record(&recorded);
    let hash = stat.content_hash.as_deref().unwrap();
    owner
        .put_metadata_intent_and_receipts(&intent, hash, &participants[..1])
        .unwrap();
    assert_eq!(
        owner
            .metadata_backend()
            .unwrap()
            .get_intent(&stat.object_id, &intent.intent_id)
            .unwrap()
            .unwrap(),
        recorded
    );
    let prefix = owner
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&DfsMetadataShardId::new("services"))
        .unwrap();
    let mut changed = intent;
    changed.policy = DfsReplicationPolicy::new(1, 2).unwrap();
    assert!(
        owner
            .put_metadata_intent_and_receipts(&changed, hash, &participants[..1])
            .is_err()
    );
    assert_eq!(
        owner
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&DfsMetadataShardId::new("services"))
            .unwrap(),
        prefix
    );
    drop(owner);
    runtime.close().unwrap();
}

#[test]
fn recovered_prepare_restores_the_original_resident_write_outcome() {
    check_recovered_original_write(false);
}

#[test]
fn explicit_original_write_retry_recovers_its_complete_prepare() {
    check_recovered_original_write(true);
}

#[test]
fn replayed_local_receipt_finishes_exact_prepare_after_ack_failure() {
    let (mut runtime, mut requester, shard) = crate::test_support::current_native_metadata_service(
        "native-replayed-receipt-publication",
        3,
        "/",
    )
    .unwrap();
    let path = "/replayed-receipt-publication";
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).unwrap(),
        replica_candidates: vec![shard.primary_node_id.clone()],
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    let initial = requester
        .posix_write_file(
            path,
            b"before interrupted receipt completion",
            options.clone(),
        )
        .unwrap();
    drop(requester);
    let mut owner = runtime.metadata_primary_service(path).unwrap();
    let interrupted = shard.replica_node_ids[0].clone();
    runtime.stop_node(&interrupted).unwrap();
    let operation = "replayed-receipt-original-operation";
    assert!(
        owner
            .posix_write_file_with_operation_id(
                path,
                b"exact bytes retained across the publication failure",
                options,
                operation,
            )
            .is_err()
    );
    let prepared = owner
        .metadata_backend()
        .unwrap()
        .list_prepared_operations()
        .unwrap()
        .into_iter()
        .find(|record| {
            record
                .payload
                .prepared_intent
                .as_ref()
                .is_some_and(|intent| intent.operation_id.as_deref() == Some(operation))
        })
        .expect("the ordinary writer retained its exact complete prepare");
    assert_eq!(prepared.status, DfsPreparedOperationStatus::Prepared);
    let intent = prepared.payload.prepared_intent.as_ref().unwrap();
    let hash = intent.content_hash.as_deref().unwrap();
    assert_eq!(
        owner
            .coordinator
            .receipts_for_intent(&intent.intent_id)
            .len(),
        1
    );
    assert!(
        owner
            .coordinator
            .is_commit_ready(&intent.intent_id)
            .unwrap()
    );

    // Fetch/receipt completion may be retried after the receipt succeeded but
    // the complete-shard publication failed. Receipt deduplication must not
    // turn that unfinished work into a successful no-op, nor waive the missing
    // participant while it remains stopped.
    assert!(
        owner
            .record_local_pending_write_receipt_if_desired(
                &intent.intent_id,
                &intent.object_id,
                &intent.target_version,
                hash,
            )
            .is_err(),
        "a retained receipt is not a completed or authorized publication"
    );
    assert_eq!(
        owner
            .metadata_backend()
            .unwrap()
            .get_object(&intent.object_id)
            .unwrap()
            .unwrap()
            .version,
        initial.version
    );
    runtime.reopen_node(interrupted).unwrap();
    owner
        .record_local_pending_write_receipt_if_desired(
            &intent.intent_id,
            &intent.object_id,
            &intent.target_version,
            hash,
        )
        .unwrap();
    let committed = owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&prepared.digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(committed.digest, prepared.digest);
    assert_eq!(committed.payload, prepared.payload);
    assert_eq!(committed.status, DfsPreparedOperationStatus::Committed);
    assert!(matches!(owner.write_outcome(&intent.intent_id).unwrap(),
        NetworkedDfsWriteOutcome::Committed { version, content_hash, .. }
        if version == intent.target_version && content_hash == hash));
    let prefix = owner
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&prepared.digest.shard_id)
        .unwrap();
    owner
        .record_local_pending_write_receipt_if_desired(
            &intent.intent_id,
            &intent.object_id,
            &intent.target_version,
            hash,
        )
        .unwrap();
    assert_eq!(
        owner
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&prepared.digest.shard_id)
            .unwrap(),
        prefix,
        "an exact completed receipt replay must append no new publication"
    );
    drop(owner);
    runtime.close().unwrap();
}

fn check_recovered_original_write(explicit_retry: bool) {
    let name = if explicit_retry {
        "native-explicit-original-retry"
    } else {
        "native-recovered-write-outcome"
    };
    let (mut runtime, mut requester, shard) =
        crate::test_support::current_native_metadata_service(name, 3, "/").unwrap();
    let path = "/recovered-write-outcome";
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).unwrap(),
        replica_candidates: vec![shard.primary_node_id.clone()],
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    let initial = requester
        .posix_write_file(path, b"original", options.clone())
        .unwrap();
    drop(requester);
    let mut owner = runtime.metadata_primary_service(path).unwrap();
    let interrupted = shard.replica_node_ids[0].clone();
    runtime.stop_node(&interrupted).unwrap();
    let operation = "native-original-partition-write";
    assert!(
        owner
            .posix_write_file_with_operation_id(path, b"recovered", options.clone(), operation)
            .is_err()
    );
    let prepared = owner
        .metadata_backend()
        .unwrap()
        .list_prepared_operations()
        .unwrap()
        .into_iter()
        .find(|record| {
            record
                .payload
                .prepared_intent
                .as_ref()
                .is_some_and(|intent| intent.operation_id.as_deref() == Some(operation))
        })
        .unwrap();
    assert_eq!(prepared.status, DfsPreparedOperationStatus::Prepared);
    assert_eq!(
        owner
            .metadata_backend()
            .unwrap()
            .get_object(&initial.object_id)
            .unwrap()
            .unwrap()
            .version,
        initial.version
    );
    let intent = prepared.payload.prepared_intent.as_ref().unwrap().clone();
    runtime.reopen_node(interrupted).unwrap();
    if explicit_retry {
        let outcome = owner
            .posix_write_file_with_operation_id(path, b"recovered", options, operation)
            .expect("the original explicit retry must return its recovered committed outcome");
        assert_eq!(outcome.version, intent.target_version);
    } else {
        owner
            .recover_one_prepared_operation(prepared.clone())
            .unwrap();
    }
    let committed = owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&prepared.digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(committed.digest, prepared.digest);
    assert_eq!(committed.status, DfsPreparedOperationStatus::Committed);
    assert_eq!(
        owner
            .metadata_backend()
            .unwrap()
            .get_object(&initial.object_id)
            .unwrap()
            .unwrap()
            .version,
        intent.target_version
    );
    assert!(
        matches!(owner.write_outcome(&intent.intent_id).unwrap(),
        NetworkedDfsWriteOutcome::Committed { version, content_hash, .. }
        if version == intent.target_version && Some(content_hash.as_str()) == intent.content_hash.as_deref()),
        "durable recovery must also finish the original resident outcome"
    );
    drop(owner);
    runtime.close().unwrap();
}

#[test]
fn posix_create_cleans_transient_metadata_when_publication_authorization_is_unavailable() {
    use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcPhase};
    let nodes = DfsTestRuntime::ordered_node_ids("native-create-publication-unavailable", 3);
    let node_id = nodes[0].clone();
    let parent = "/authority-not-ready-cleanup";
    let path = "/authority-not-ready-cleanup/record.json";
    let payload = b"immutable chat metadata";
    let mut runtime = DfsTestRuntime::new(
        "posix_create_cleans_transient_metadata_when_publication_authorization_is_unavailable",
        node_id.clone(),
    )
    .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    assert_eq!(shard.primary_node_id, node_id);
    assert_eq!(
        shard
            .replica_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        nodes[1..].iter().cloned().collect()
    );
    // The ordinary exact CoreEffect opener reaches the guard's typed error
    // without a same-node Main RPC erasing it into delivery uncertainty.
    let mut dfs = runtime.metadata_primary_service(path).unwrap();
    dfs.posix_mkdir_all(parent).expect("fixture parent");
    let observations_before = runtime.rpc_observations().len();
    for replica in &shard.replica_node_ids {
        for route in [
            crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
            crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
        ] {
            runtime.install_fault(FaultRule {
                source: Some(node_id.clone()),
                target: Some(replica.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: None,
                remaining_hits: 32,
                action: FaultAction::BudgetExhausted,
            });
        }
    }
    // The real prepared-publication guard needs operation-bound metadata acknowledgements,
    // including every assigned participant. A browser planning getter is unrelated to
    // an explicit-options writer and supplies no publication authorization.
    let options = DfsWriteOptions::replicated_eventual(vec![node_id.clone()]);
    let operation_id = sinomo_core::Id::generate().to_string();
    let error = dfs
        .posix_write_file_with_operation_id(path, payload, options.clone(), &operation_id)
        .expect_err("unavailable operation-bound metadata quorum must reject the create");
    assert!(
        matches!(&error, DfsPosixError::Dfs(DfsError::MetadataRecoveryQuorumUnavailable {
        file_system_id, shard_id, required, verified: 1,
    }) if file_system_id == &shard.file_system_id && shard_id == &shard.shard_id
        && *required == shard.replica_node_ids.len() + 1),
        "unexpected authorization error: {error}"
    );
    for replica in &shard.replica_node_ids {
        assert!(
            runtime.rpc_observations()[observations_before..]
                .iter()
                .any(|event| {
                    event.source == node_id
                        && &event.target == replica
                        && matches!(
                            event.route.as_str(),
                            crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                                | crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE
                        )
                        && event.phase == RpcPhase::EndedWithoutResponse
                }),
            "no actual failed metadata carrier observation for {replica}"
        );
    }
    assert!(
        dfs.lookup_sifs_path(path)
            .expect("lookup after rejected create")
            .is_none(),
        "a rejected create must not remain visible as contentless transient metadata"
    );
    assert!(
        dfs.transient_metadata_object_ids.is_empty(),
        "a rejected create must release its transient namespace overlay"
    );
    // Overlay cleanup does not revoke the actual retained preparation. This
    // foreground Core write first prepares its exact pending intent/receipt.
    let snapshot = dfs
        .metadata_backend()
        .unwrap()
        .export_table_snapshot()
        .unwrap();
    let retained: Vec<_> = snapshot
        .tables
        .prepared_operations_v7
        .iter()
        .filter(|record| {
            record.payload.metadata_effects.iter().any(|effect| {
                matches!(effect,
            crate::DfsPreparedMetadataEffect::PutIntent { intent }
            if intent.operation_id.as_deref() == Some(operation_id.as_str())
                && intent.object_metadata.as_ref().is_some_and(|object| object.path == path))
            })
        })
        .cloned()
        .collect();
    assert_eq!(
        retained.len(),
        1,
        "the actual write owns one retained preparation"
    );
    let retained = &retained[0];
    assert_eq!(retained.status, crate::DfsPreparedOperationStatus::Prepared);
    assert!(retained.committed_entry.is_none());
    let intent = retained
        .payload
        .metadata_effects
        .iter()
        .find_map(|effect| match effect {
            crate::DfsPreparedMetadataEffect::PutIntent { intent } => Some(intent),
            _ => None,
        })
        .unwrap();
    assert_eq!(intent.file_system_id, shard.file_system_id);
    assert_eq!(intent.owner_epoch, shard.epoch);
    assert_eq!(intent.coordinator_node_id, node_id);
    assert_eq!(
        (
            intent.policy.min_commit_replicas,
            intent.policy.desired_replicas
        ),
        (1, 1)
    );
    let expected_hash = format!("blake3:{}", blake3::hash(payload).to_hex());
    assert_eq!(intent.content_hash.as_deref(), Some(expected_hash.as_str()));
    runtime.clear_faults();
    dfs.refresh_control_plane_latest()
        .expect("ordinary configuration recovery");
    // Normal hydration recovers the SAME captured body with fresh real
    // operation-bound authority; no test-created guard or abort is supplied.
    dfs.hydrate_from_metadata_backend()
        .expect("ordinary retained prepare recovery");
    let recovered = dfs
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&retained.digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(recovered.digest, retained.digest);
    assert_eq!(recovered.payload, retained.payload);
    assert_eq!(
        recovered.status,
        crate::DfsPreparedOperationStatus::Committed
    );
    assert!(recovered.committed_entry.is_some());
    let committed = dfs
        .posix_write_file_with_operation_id(path, payload, options, &operation_id)
        .expect("retry after publication authority recovery");
    assert_eq!(committed.path, path);
    assert_eq!(
        dfs.posix_read_file(path).expect("read recovered write"),
        payload
    );
    drop(dfs);
    runtime.close().expect("ordinary native close");
}

#[test]
fn dfs_repair_maintenance_gate_tracks_authority_owned_write_work() {
    let authority = NodeId::from_legacy_named("node_dfs_repair_gate_authority").expect("authority");
    let requester = NodeId::from_legacy_named("node_dfs_repair_gate_requester").expect("requester");
    let replica = NodeId::from_legacy_named("node_dfs_repair_gate_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_dfs_repair_gate").expect("object");
    let version = DfsVersion::from_legacy_named("version_dfs_repair_gate").expect("version");
    let file_system_id = sifs_file_system_id();
    let repair_job = DistributedRepairJob {
        intent_id: "intent_dfs_repair_gate".to_string(),
        file_system_id,
        object_id: object_id.clone(),
        version: version.clone(),
        missing_replica_node_id: replica,
    };
    let pending = DfsDistributedWriteResult::Outcome {
        outcome: NetworkedDfsWriteOutcome::Pending {
            intent_id: "intent_dfs_repair_gate_pending".to_string(),
            durable_receipts: 1,
            required_receipts: 2,
        },
    };
    let committed_with_repair = DfsDistributedWriteResult::Outcome {
        outcome: NetworkedDfsWriteOutcome::Committed {
            intent_id: repair_job.intent_id.clone(),
            version: version.clone(),
            content_hash: "blake3:repair-gate".to_string(),
            repair_jobs: vec![repair_job],
        },
    };
    let committed_without_repair = DfsDistributedWriteResult::Outcome {
        outcome: NetworkedDfsWriteOutcome::Committed {
            intent_id: "intent_dfs_repair_gate_complete".to_string(),
            version,
            content_hash: "blake3:repair-gate-complete".to_string(),
            repair_jobs: Vec::new(),
        },
    };

    assert!(dfs_events_should_wake_repair_maintenance(&[
        NetworkedDfsServiceEvent::RemoteWriteServed {
            request_id: "repair_gate_pending".to_string(),
            object_id: object_id.clone(),
            requester_node_id: requester.clone(),
            result: pending,
        },
    ]));
    assert!(dfs_events_should_wake_repair_maintenance(&[
        NetworkedDfsServiceEvent::RemoteWriteServed {
            request_id: "repair_gate_committed".to_string(),
            object_id: object_id.clone(),
            requester_node_id: requester.clone(),
            result: committed_with_repair.clone(),
        },
    ]));
    assert!(!dfs_events_should_wake_repair_maintenance(&[
        NetworkedDfsServiceEvent::RemoteWriteServed {
            request_id: "repair_gate_complete".to_string(),
            object_id: object_id.clone(),
            requester_node_id: requester.clone(),
            result: committed_without_repair,
        },
    ]));
    assert!(!dfs_events_should_wake_repair_maintenance(&[
        NetworkedDfsServiceEvent::RemoteWriteCompleted {
            request_id: "repair_gate_requester_completion".to_string(),
            object_id: object_id.clone(),
            primary_node_id: authority,
            result: committed_with_repair,
        },
    ]));
    assert!(!dfs_events_should_wake_repair_maintenance(&[
        NetworkedDfsServiceEvent::RemoteWriteServed {
            request_id: "repair_gate_rejected".to_string(),
            object_id,
            requester_node_id: requester,
            result: DfsDistributedWriteResult::Rejected {
                reason: "not authoritative".to_string(),
                current_primary_node_id: None,
            },
        },
    ]));
}

#[test]
fn metadata_journal_publish_suppresses_proven_primary_mismatch_for_epoch() {
    let root = temp_root("metadata-journal-publish-suppresses-topology-mismatch");
    let local = NodeId::from_legacy_named("node_meta_suppress_local").expect("local");
    let replica = NodeId::from_legacy_named("node_meta_suppress_replica").expect("replica");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(local.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![replica.clone()],
        epoch: 7,
        assignment: DfsMetadataShardAssignment::Root,

        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let suppression = DfsMetadataReplicationSuppressionRecord {
        code: Some(DfsMetadataRejectionCode::PrimaryMismatch),
        target_node_id: replica.clone(),
        shard_id: shard_id.clone(),
        shard_epoch: 7,
        reason: format!(
            "metadata journal source {local} is not shard root primary {}",
            NodeId::from_legacy_named("node_meta_suppress_other_primary").expect("other primary")
        ),
        updated_at_unix_nanos: dfs_now_unix_nanos(),
    };
    let mut host = RecordingMetadataJournalPublisherHost::new(root.join("suppressed"));

    let (outcomes, targets) = prepare_dfs_metadata_journal_publish_with_host(
        file_system.file_system_id.clone(),
        &local,
        vec![shard.clone()],
        &mut host,
        std::slice::from_ref(&suppression),
        DfsMetadataJournalReplicationOptions::default(),
    )
    .expect("prepare suppressed publish");

    assert!(targets.is_empty());
    assert_eq!(host.opened_shards, vec![shard_id.clone()]);
    assert!(
        host.cursor_requests.is_empty(),
        "suppressed target must not read/update replication cursor"
    );
    assert_eq!(
        outcomes,
        vec![
            DfsMetadataJournalReplicationOutcome::TopologyMismatchSuppressed {
                target_node_id: replica.clone(),
                shard_id: shard_id.clone(),
                epoch: 7,
                reason: suppression.reason.clone(),
            }
        ]
    );

    let mut host = RecordingMetadataJournalPublisherHost::new(root.join("epoch-changed"));
    let mut changed_epoch_shard = shard;
    changed_epoch_shard.epoch = 8;
    let (outcomes, targets) = prepare_dfs_metadata_journal_publish_with_host(
        file_system.file_system_id,
        &local,
        vec![changed_epoch_shard],
        &mut host,
        &[suppression],
        DfsMetadataJournalReplicationOptions::default(),
    )
    .expect("prepare changed-epoch publish");

    assert!(targets.is_empty());
    assert_eq!(
        host.cursor_requests,
        vec![(replica.clone(), shard_id.clone())],
        "shard epoch changes must invalidate topology suppression"
    );
    assert_eq!(
        outcomes,
        vec![DfsMetadataJournalReplicationOutcome::NoChanges {
            target_node_id: replica,
            shard_id,
            after_sequence: 0,
        }]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn clustered_metadata_journal_batch_requires_exact_authority_identity() {
    let primary = NodeId::from_legacy_named("node_meta_identity_primary").expect("primary node id");
    let replica = NodeId::from_legacy_named("node_meta_identity_replica").expect("replica node id");
    let file_system = file_system(primary.clone());
    let context = DfsMetadataReplicaShardContext {
        local_node_id: replica.clone(),
        pending_membership_handoff: None,
        shard: DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: primary.clone(),
            replica_node_ids: vec![replica],
            epoch: 9,
            assignment: DfsMetadataShardAssignment::Root,

            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    };
    let exact = DfsMetadataJournalBatchCommand {
        request_id: "request-exact-journal-authority".to_string(),
        source_node_id: Some(primary),
        file_system_id: Some(file_system.file_system_id),
        authority_epoch: Some(context.shard.epoch),
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: None,
        durable_replication: None,
        batch: DfsMetadataJournalFrameBatch {
            shard_id: DfsMetadataShardId::new("root"),
            journal_frames: Vec::new(),
            next_after_sequence: 0,
        },
    };

    assert_eq!(
        validate_dfs_metadata_journal_batch_source(Some(&context), &exact),
        None
    );

    let mut missing = exact.clone();
    missing.file_system_id = None;
    missing.authority_epoch = None;
    assert!(matches!(
        validate_dfs_metadata_journal_batch_source(Some(&context), &missing),
        Some(DfsMetadataJournalBatchResponse {
            result: DfsMetadataJournalBatchResult::Rejected { ref reason , ..},
            ..
        }) if reason.contains("authority identity")
    ));

    let mut stale = exact;
    stale.authority_epoch = Some(context.shard.epoch - 1);
    assert!(matches!(
        validate_dfs_metadata_journal_batch_source(Some(&context), &stale),
        Some(DfsMetadataJournalBatchResponse {
            result: DfsMetadataJournalBatchResult::Rejected { ref reason , ..},
            ..
        }) if reason.contains("authority identity")
    ));
}

#[test]
fn metadata_authority_probe_rejects_a_gap_before_the_hidden_prepare() {
    use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcOrigin};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    let nodes = DfsTestRuntime::ordered_node_ids("native-authority-gap", 3);
    let primary = nodes[0].clone();
    let replica = nodes[1].clone();
    let mut runtime = DfsTestRuntime::new("native-authority-gap", primary.clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let file_system = runtime.file_system();
    let mut source = runtime.metadata_primary_service("/").unwrap();
    let dispatch = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(FaultRule {
        source: Some(primary.clone()),
        target: Some(replica.clone()),
        route: None,
        call_id: None,
        origin: Some(RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: FaultAction::PauseBeforeDispatch(dispatch.clone()),
    });
    let receiver = LocalSobjMetadataBackend::new(
        shard_id.clone(),
        default_dfs_metadata_shards_root(runtime.data_dir(&replica)).join(&shard_id.0),
    )
    .unwrap();
    let call = |command: &DfsMetadataJournalBatchCommand| {
        let request = dfs_metadata_journal_batch_rpc_request(command).unwrap();
        decode_dfs_metadata_journal_batch_rpc_response(
            &runtime
                .rpc_request(&primary, &replica, request, Duration::from_secs(5))
                .unwrap(),
        )
        .unwrap()
    };
    // Synchronize the existing real prefix before introducing two new hidden
    // operations. Automatic traffic remains paused; explicit calls still use
    // the authenticated native registry and persisted receiver.
    let mut base_head = 0;
    loop {
        let backend = source.metadata_backend.as_ref().unwrap();
        let Some(batch) = backend
            .journal_batch_for_shard_since_limited(
                &shard_id,
                base_head,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .unwrap()
        else {
            break;
        };
        let frames = crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(
            backend.as_ref(),
            &batch,
        )
        .unwrap();
        let command = crate::metadata_replication::dfs_metadata_durable_journal_batch_command(
            &primary,
            &replica,
            &shard.file_system_id,
            shard.epoch,
            frames,
            false,
        );
        let response = call(&command);
        assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
        assert_eq!(
            response.verified_authority_probe_id,
            Some(
                command
                    .durable_replication
                    .as_ref()
                    .unwrap()
                    .confirmation_id
                    .clone()
            )
        );
        base_head = batch.next_after_sequence;
    }
    let settle = Instant::now();
    while receiver.metadata_replay_cursor(&shard_id).unwrap() < base_head {
        assert!(
            settle.elapsed() < Duration::from_secs(5),
            "native baseline did not materialize"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let prepare = |service: &mut NetworkedDfsService<DfsNodeTransport>, operation_id: &str| {
        service
            .metadata_backend
            .as_mut()
            .unwrap()
            .prepare_operation(
                DfsPreparedOperationPayload::for_metadata_effects(
                    DfsPreparedOperationScope {
                        file_system_id: shard.file_system_id.clone(),
                        shard_id: shard_id.clone(),
                        owner_epoch: shard.epoch,
                        operation_id: operation_id.into(),
                    },
                    Vec::new(),
                    Vec::new(),
                    vec![DfsPreparedMetadataEffect::PutFileSystem {
                        file_system: file_system.clone(),
                    }],
                    OffsetDateTime::now_utc(),
                )
                .unwrap(),
                None,
            )
            .unwrap()
    };
    let first = prepare(&mut source, "authority-gap-missing-prefix");
    let second = prepare(&mut source, "authority-gap-hidden-prepare");
    let later = prepare(&mut source, "authority-gap-later-independent-prepare");
    assert_eq!(first.prepare_sequence, base_head + 1);
    assert_eq!(second.prepare_sequence, first.prepare_sequence + 1);
    assert_eq!(later.prepare_sequence, second.prepare_sequence + 1);
    let backend = source.metadata_backend.as_ref().unwrap();
    let suffix = backend
        .journal_batch_for_shard_since_limited(
            &shard_id,
            first.prepare_sequence,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    assert_eq!(suffix.entries.len(), 2);
    assert_eq!(suffix.next_after_sequence, later.prepare_sequence);
    let frames = crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(
        backend.as_ref(),
        &suffix,
    )
    .unwrap();
    let prepared_prefix = backend.metadata_prefix_proof(&shard_id).unwrap().unwrap();
    assert_eq!(prepared_prefix.journal_sequence, later.prepare_sequence);
    assert!(second.prepare_sequence < prepared_prefix.journal_sequence);
    let probe = DfsMetadataAuthorityProbe {
        probe_id: "native-authority-gap-probe".into(),
        expected_shard: shard.clone(),
        operation: DfsMetadataAuthorityOperation::PreparedOperation {
            operation_id: second.operation_id.clone(),
            file_system_id: second.file_system_id.clone(),
            shard_id: second.shard_id.clone(),
            owner_epoch: second.owner_epoch,
            prepare_sequence: second.prepare_sequence,
            payload_digest: second.payload_digest.clone(),
        },
        prepared_prefix: prepared_prefix.clone(),
    };
    let command = DfsMetadataJournalBatchCommand {
        request_id: "native-authority-gap-request".into(),
        source_node_id: Some(primary.clone()),
        file_system_id: Some(shard.file_system_id.clone()),
        authority_epoch: Some(shard.epoch),
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: Some(probe.clone()),
        durable_replication: None,
        batch: frames,
    };
    let rejected = call(&command);
    assert!(matches!(
        rejected.result,
        DfsMetadataJournalBatchResult::Rejected { .. }
    ));
    assert_eq!(rejected.verified_authority_probe_id, None);
    assert_eq!(
        receiver.metadata_replay_cursor(&shard_id).unwrap(),
        base_head
    );
    assert_eq!(
        receiver
            .prepared_operation_record(&second.operation_id)
            .unwrap(),
        None
    );
    // A prior materializer can have published base_head while its task is
    // still draining. A new request then joins that existing worker instead
    // of registering the pause. Exclude actual producers while observing idle
    // and installing the next boundary, then release admission before the RPC.
    let pause_deadline = Instant::now() + Duration::from_secs(5);
    let materializer = crate::runtime_host::daemon_dfs_metadata_materialization_lane_scheduler(
        runtime.data_dir(&replica),
        &shard.file_system_id,
        &shard_id,
    )
    .unwrap();
    let scheduler =
        crate::runtime_host::daemon_dfs_mutation_scheduler(runtime.data_dir(&replica)).unwrap();
    let scope = crate::DfsMessageExecutionScope::ExactShard {
        file_system_id: shard.file_system_id.clone(),
        shard_id: shard_id.clone(),
    };
    let pause = loop {
        let remaining = pause_deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "native materializer must drain inside the original pause budget"
        );
        scheduler.request_foreground_admission();
        let admission = crate::test_runtime::executor().block_on(async {
            tokio::time::timeout(remaining, scheduler.admit(&scope))
                .await
                .expect("original materialization pause admission budget")
                .unwrap()
        });
        if materializer.is_idle_while_producers_excluded() {
            let pause = runtime
                .pause_replica_materialization(&replica, &shard_id)
                .unwrap();
            drop(admission);
            break pause;
        }
        drop(admission);
        std::thread::sleep(Duration::from_millis(1));
    };
    let missing = backend
        .journal_batch_for_shard_since_limited(
            &shard_id,
            base_head,
            1,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    assert_eq!(missing.entries[0].sequence, first.prepare_sequence);
    let catchup = crate::metadata_replication::dfs_metadata_durable_journal_batch_command(
        &primary,
        &replica,
        &shard.file_system_id,
        shard.epoch,
        crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(
            backend.as_ref(),
            &missing,
        )
        .unwrap(),
        false,
    );
    let ack = call(&catchup);
    assert_eq!(ack.result, DfsMetadataJournalBatchResult::Applied);
    assert_eq!(
        ack.verified_authority_probe_id,
        Some(
            catchup
                .durable_replication
                .as_ref()
                .unwrap()
                .confirmation_id
                .clone()
        )
    );
    let capture = runtime
        .wait_paused_materialization(
            &pause,
            pause_deadline.saturating_duration_since(Instant::now()),
        )
        .unwrap();
    assert_eq!(capture.unavailable, None);
    assert_eq!(capture.durable_sequence, Some(first.prepare_sequence));
    assert_eq!(capture.materialized_sequence, Some(base_head));
    assert_eq!(
        receiver.metadata_replay_cursor(&shard_id).unwrap(),
        base_head
    );
    assert_eq!(
        receiver
            .metadata_prefix_ack_floor(&shard_id)
            .unwrap()
            .unwrap()
            .journal_sequence,
        first.prepare_sequence
    );
    // Each negative contains the earlier named operation and ALL later slots.
    // A wrong original tuple or a poisoned/forked later frame must reject before
    // persisting either preparation or advancing any local acknowledgement.
    let before_prefix = receiver.metadata_prefix_proof().unwrap();
    let before_authority_floor = receiver.metadata_authority_ack_floor(&shard_id).unwrap();
    let before_prefix_floor = receiver.metadata_prefix_ack_floor(&shard_id).unwrap();
    for variant in 0..8 {
        let mut invalid = command.clone();
        invalid.request_id = format!("native-current-cut-original-negative-{variant}");
        let candidate = invalid.authority_probe.as_mut().unwrap();
        let DfsMetadataAuthorityOperation::PreparedOperation {
            operation_id,
            payload_digest,
            prepare_sequence,
            ..
        } = &mut candidate.operation;
        match variant {
            0 => operation_id.push_str("-missing"),
            1 => {
                let last = payload_digest.pop().unwrap();
                payload_digest.push(if last == '0' { '1' } else { '0' });
            }
            2 => *prepare_sequence = later.prepare_sequence,
            3 => *prepare_sequence = later.prepare_sequence + 1,
            4 | 5 => {
                let frame = invalid.batch.journal_frames.last_mut().unwrap();
                assert_eq!(frame.sequence, later.prepare_sequence);
                let mut entry =
                    crate::metadata_journal::decode_metadata_journal_frame_record(frame).unwrap();
                let DfsMetadataMutation::PrepareOperationV7 { record, .. } = &mut entry.mutation
                else {
                    panic!("actual later hidden preparation")
                };
                let payload = Arc::make_mut(&mut record.payload);
                let DfsPreparedMetadataEffect::PutFileSystem { file_system } =
                    &mut payload.metadata_effects[0]
                else {
                    panic!("actual later filesystem effect")
                };
                file_system.display_name.push_str("-foreign-later-body");
                if variant == 4 {
                    // Fully valid current frame encoding and record, but a fork
                    // of the sealed later frame. The complete prefix must fail.
                    record.digest = DfsPreparedOperationDigest::at_sequence(
                        &record.payload,
                        later.prepare_sequence,
                    )
                    .unwrap();
                    record.validate().unwrap();
                }
                *frame =
                    crate::metadata_journal::encode_metadata_journal_frame_record(&entry).unwrap();
            }
            6 => {
                invalid.batch.journal_frames.pop();
            }
            7 => {
                let last = candidate.prepared_prefix.digest.pop().unwrap();
                candidate
                    .prepared_prefix
                    .digest
                    .push(if last == '0' { '1' } else { '0' });
            }
            _ => unreachable!(),
        }
        let rejected = call(&invalid);
        assert!(
            matches!(
                rejected.result,
                DfsMetadataJournalBatchResult::Rejected { .. }
            ),
            "current cut/original obligation negative {variant}: {rejected:?}"
        );
        assert_eq!(rejected.verified_authority_probe_id, None);
        assert_eq!(rejected.applied_entries, 0);
        assert_eq!(receiver.metadata_prefix_proof().unwrap(), before_prefix);
        assert_eq!(
            receiver.metadata_authority_ack_floor(&shard_id).unwrap(),
            before_authority_floor
        );
        assert_eq!(
            receiver.metadata_prefix_ack_floor(&shard_id).unwrap(),
            before_prefix_floor
        );
        assert!(
            receiver
                .journal_batch_for_shard_since_limited(
                    &shard_id,
                    first.prepare_sequence,
                    usize::MAX,
                    usize::MAX
                )
                .unwrap()
                .is_none(),
            "rejected invocation must not append the named or later frame: {variant}"
        );
    }
    // The real authority receiver accepts the complete current suffix while
    // its rebuildable projection remains paused. Its report must validate the
    // named SECOND original body, rather than the last THIRD preparation.
    let accepted = call(&command);
    assert_eq!(accepted.result, DfsMetadataJournalBatchResult::Applied);
    assert_eq!(
        accepted.verified_authority_probe_id,
        crate::service_replica::metadata_authority_probe_confirmation_id(&probe)
    );
    assert_eq!(
        receiver
            .metadata_authority_ack_floor(&shard_id)
            .unwrap()
            .unwrap()
            .journal_sequence,
        later.prepare_sequence
    );
    assert_eq!(
        receiver.metadata_replay_cursor(&shard_id).unwrap(),
        base_head
    );
    pause.release();
    drop(pause);
    dispatch.close();
    runtime.clear_faults();
    drop(source);
    drop(receiver);
    drop(call);
    runtime
        .close()
        .expect("join actual native ownership and cleanup");
}

#[test]
fn metadata_authority_empty_probe_confirms_only_exact_snapshot_covered_prepare() {
    use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcOrigin};
    use std::sync::Arc;
    use std::time::Duration;

    let name = "native-empty-authority-snapshot-prepare";
    let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
    let primary = nodes[0].clone();
    let mut runtime = DfsTestRuntime::new(name, primary.clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    assert_eq!(shard.primary_node_id, primary);
    let replica = shard.replica_node_ids[0].clone();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let file_system = runtime.file_system();
    let mut source = runtime
        .metadata_primary_service("/native-empty-authority")
        .unwrap();
    // Retain the real normal admission across this physical hidden prepare and
    // its queries. No background author can commit or advance this source cut.
    let scope = DfsPreparedOperationScope {
        file_system_id: shard.file_system_id.clone(),
        shard_id: shard_id.clone(),
        owner_epoch: shard.epoch,
        operation_id: "native-snapshot-covered-hidden-prepare".into(),
    };
    let execution = source.acquire_prepared_execution(&scope).unwrap();
    let dispatch = Arc::new(tokio::sync::Semaphore::new(0));
    for route in [
        DFS_METADATA_JOURNAL_BATCH_ROUTE,
        DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
    ] {
        runtime.install_fault(FaultRule {
            source: Some(primary.clone()),
            target: Some(replica.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: FaultAction::PauseBeforeDispatch(Arc::clone(&dispatch)),
        });
    }
    let digest = source
        .metadata_backend_mut()
        .unwrap()
        .prepare_operation(
            DfsPreparedOperationPayload::for_metadata_effects(
                scope,
                Vec::new(),
                Vec::new(),
                vec![DfsPreparedMetadataEffect::PutFileSystem {
                    file_system: file_system.clone(),
                }],
                OffsetDateTime::now_utc(),
            )
            .unwrap(),
            None,
        )
        .unwrap();
    let later = source
        .metadata_backend_mut()
        .unwrap()
        .prepare_operation(
            DfsPreparedOperationPayload::for_metadata_effects(
                DfsPreparedOperationScope {
                    file_system_id: shard.file_system_id.clone(),
                    shard_id: shard_id.clone(),
                    owner_epoch: shard.epoch,
                    operation_id: "native-snapshot-covered-later-prepare".into(),
                },
                Vec::new(),
                Vec::new(),
                vec![DfsPreparedMetadataEffect::PutFileSystem { file_system }],
                OffsetDateTime::now_utc(),
            )
            .unwrap(),
            None,
        )
        .unwrap();
    assert_eq!(later.prepare_sequence, digest.prepare_sequence + 1);
    let backend = source.metadata_backend().unwrap();
    let original = backend
        .prepared_operation_record(&digest.operation_id)
        .unwrap()
        .unwrap();
    original.validate().unwrap();
    assert_eq!(original.status, DfsPreparedOperationStatus::Prepared);
    let image = backend
        .export_shard_snapshot_image(&shard_id)
        .unwrap()
        .unwrap();
    assert_eq!(image.materialized_through_sequence, later.prepare_sequence);
    let prepared_prefix = image.prefix_proof.clone();
    assert!(prepared_prefix.is_well_formed());
    assert_eq!(prepared_prefix.journal_sequence, later.prepare_sequence);
    let expected = backend
        .export_shard_table_snapshot(&shard_id)
        .unwrap()
        .unwrap();
    let snapshot = DfsMetadataSnapshotInstallCommand {
        request_id: "native-empty-authority-install".into(),
        source_node_id: primary.clone(),
        file_system_id: Some(shard.file_system_id.clone()),
        authority_epoch: Some(shard.epoch),
        primary_handoff_id: None,
        shard_id: shard_id.clone(),
        next_after_sequence: image.materialized_through_sequence,
        snapshot: image.descriptor().clone(),
        image: Some(image),
        reason: "actual native snapshot covers the original hidden preparation".into(),
    };
    let installed = decode_dfs_metadata_snapshot_install_rpc_response(
        &runtime
            .rpc_call(
                &primary,
                &replica,
                dfs_metadata_snapshot_install_rpc_call(&snapshot).unwrap(),
                Duration::from_secs(5),
            )
            .unwrap(),
    )
    .unwrap();
    assert!(
        matches!(
            installed.result,
            DfsMetadataSnapshotInstallResult::Installed { .. }
        ),
        "normal authenticated snapshot receiver: {installed:?}"
    );
    let receiver_root =
        default_dfs_metadata_shards_root(runtime.data_dir(&replica)).join(&shard_id.0);
    let inspect = || {
        // A fresh inspection handle follows the real installed incarnation;
        // never use a stale pre-exchange backend to assert frame absence.
        let mut receiver =
            LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), &receiver_root)
                .unwrap();
        receiver.begin_inspection_snapshot().unwrap();
        let tables = receiver.export_table_snapshot();
        let authority_floor = receiver.metadata_authority_ack_floor(&shard_id).unwrap();
        let prefix_floor = receiver.metadata_prefix_ack_floor(&shard_id).unwrap();
        let frames = receiver
            .journal_since_limited(0, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT)
            .unwrap();
        receiver.end_inspection_snapshot();
        (tables, authority_floor, prefix_floor, frames)
    };
    let before = inspect();
    assert_eq!(before.0, expected);
    assert!(
        before.3.is_empty(),
        "snapshot-covered original frames must be absent from live journal"
    );
    assert_eq!(
        before
            .0
            .tables
            .prepared_operations_v7
            .iter()
            .find(|row| row.digest.operation_id == digest.operation_id),
        Some(&original)
    );
    let probe = DfsMetadataAuthorityProbe {
        probe_id: "native-empty-authority-known".into(),
        expected_shard: shard.clone(),
        operation: DfsMetadataAuthorityOperation::PreparedOperation {
            operation_id: digest.operation_id.clone(),
            file_system_id: digest.file_system_id.clone(),
            shard_id: digest.shard_id.clone(),
            owner_epoch: digest.owner_epoch,
            prepare_sequence: digest.prepare_sequence,
            payload_digest: digest.payload_digest.clone(),
        },
        prepared_prefix: prepared_prefix.clone(),
    };
    let command = DfsMetadataJournalBatchCommand {
        request_id: "native-empty-authority-query".into(),
        source_node_id: Some(primary.clone()),
        file_system_id: Some(shard.file_system_id.clone()),
        authority_epoch: Some(shard.epoch),
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: Some(probe.clone()),
        durable_replication: None,
        batch: DfsMetadataJournalFrameBatch {
            shard_id: shard_id.clone(),
            next_after_sequence: later.prepare_sequence,
            journal_frames: Vec::new(),
        },
    };
    let call = |command: &DfsMetadataJournalBatchCommand| {
        decode_dfs_metadata_journal_batch_rpc_response(
            &runtime
                .rpc_request(
                    &primary,
                    &replica,
                    dfs_metadata_journal_batch_rpc_request(command).unwrap(),
                    Duration::from_secs(5),
                )
                .unwrap(),
        )
        .unwrap()
    };
    for variant in 0..7 {
        let mut rejected = command.clone();
        rejected.request_id = format!("native-empty-authority-negative-{variant}");
        let candidate = rejected.authority_probe.as_mut().unwrap();
        let DfsMetadataAuthorityOperation::PreparedOperation {
            operation_id,
            payload_digest,
            owner_epoch,
            prepare_sequence,
            ..
        } = &mut candidate.operation;
        match variant {
            0 => operation_id.push_str("-unknown"),
            1 => {
                let last = payload_digest.pop().unwrap();
                payload_digest.push(if last == '0' { '1' } else { '0' });
            }
            2 => *owner_epoch += 1,
            3 => {
                let last = candidate.prepared_prefix.digest.pop().unwrap();
                candidate
                    .prepared_prefix
                    .digest
                    .push(if last == '0' { '1' } else { '0' });
            }
            4 => rejected.authority_probe = None,
            5 => *prepare_sequence = later.prepare_sequence,
            6 => *prepare_sequence = later.prepare_sequence + 1,
            _ => unreachable!(),
        }
        let response = call(&rejected);
        assert!(
            matches!(
                response.result,
                DfsMetadataJournalBatchResult::Rejected { .. }
            ),
            "unknown/changed/foreign or purposeless empty confirmation must reject: {variant}: {response:?}"
        );
        assert_eq!(response.verified_authority_probe_id, None);
        assert_eq!(response.applied_entries, 0);
        let after = inspect();
        assert_eq!(
            after, before,
            "rejected empty authority query must not change tables/prefix/journal/ACK floors: {variant}"
        );
    }
    let confirmed = call(&command);
    assert_eq!(confirmed.result, DfsMetadataJournalBatchResult::Applied);
    assert_eq!(confirmed.applied_entries, 0);
    assert!(!confirmed.verified_prefix_only);
    assert_eq!(confirmed.next_after_sequence, later.prepare_sequence);
    assert_eq!(
        confirmed.verified_authority_probe_id,
        crate::service_replica::metadata_authority_probe_confirmation_id(&probe)
    );
    assert!(confirmed.verified_authority_probe_id.is_some());
    let after = inspect();
    assert_eq!(after.0, before.0);
    assert_eq!(after.2, before.2);
    assert!(after.3.is_empty());
    assert_eq!(
        after.1,
        Some(DfsMetadataAuthorityAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: later.prepare_sequence,
            durable_prefix_proof: prepared_prefix,
        })
    );
    drop(call);
    drop(inspect);
    dispatch.close();
    runtime.clear_faults();
    drop(execution);
    drop(source);
    runtime
        .close()
        .expect("join real native owners after empty confirmation");
}

#[test]
fn pending_membership_handoff_authorizes_exact_suffix_and_prefix_for_new_replica() {
    let primary = NodeId::from_legacy_named("node_meta_handoff_primary").expect("primary node id");
    let retiring_replica = NodeId::from_legacy_named("node_meta_handoff_retiring_replica")
        .expect("retiring replica node id");
    let desired_replica = NodeId::from_legacy_named("node_meta_handoff_desired_replica")
        .expect("desired replica node id");
    let file_system_id =
        DfsFileSystemId::from_legacy_named("metadata_handoff_receiver").expect("filesystem id");
    let shard_id = DfsMetadataShardId::new("root");
    let retiring_shard = DfsMetadataShardConfig {
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![retiring_replica],
        epoch: 8,
        assignment: DfsMetadataShardAssignment::Root,

        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let desired_shard = DfsMetadataShardConfig {
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![desired_replica.clone()],
        epoch: 9,
        assignment: DfsMetadataShardAssignment::Root,

        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let context = DfsMetadataReplicaShardContext {
        local_node_id: desired_replica,
        shard: retiring_shard.clone(),
        pending_membership_handoff: Some(DfsMetadataShardMembershipHandoff {
            cause: crate::DfsMetadataShardHandoffCause::Membership,
            handoff_id: "membership-handoff-receiver".to_string(),
            retiring_shard,
            desired_shard,
            required_prepared_sequence: 0,
            required_head_watermark: None,
            retiring_primary_fence: None,
        }),
    };
    let exact_prefix_probe = DfsMetadataJournalBatchCommand {
        request_id: "request-membership-handoff-prefix".to_string(),
        source_node_id: Some(primary),
        file_system_id: Some(file_system_id.clone()),
        authority_epoch: Some(context.shard.epoch),
        verify_prefix_only: true,
        prefix_probe: Some(DfsMetadataPrefixProbe {
            barrier_id: "barrier-membership-handoff-prefix".to_string(),
            file_system_id: Some(file_system_id),
            authority_epoch: Some(context.shard.epoch),
            shard_id: shard_id.clone(),
            journal_sequence: 4,
            state_fingerprint: "blake3:membership-handoff-state".to_string(),
            primary_handoff_id: None,
        }),
        authority_probe: None,
        durable_replication: None,
        batch: DfsMetadataJournalFrameBatch {
            shard_id,
            journal_frames: Vec::new(),
            next_after_sequence: 4,
        },
    };

    assert_eq!(
        validate_dfs_metadata_journal_batch_source(Some(&context), &exact_prefix_probe),
        None,
        "the pending desired replica must accept the exact retiring-primary prefix proof"
    );

    let mut ordinary_batch = exact_prefix_probe.clone();
    ordinary_batch.verify_prefix_only = false;
    ordinary_batch.prefix_probe = None;
    assert_eq!(
        validate_dfs_metadata_journal_batch_source(Some(&context), &ordinary_batch),
        None,
        "the exact pending handoff must permit the retiring-primary suffix before the prefix proof"
    );

    let mut unfenced_context = context;
    unfenced_context.pending_membership_handoff = None;
    assert!(matches!(
        validate_dfs_metadata_journal_batch_source(Some(&unfenced_context), &exact_prefix_probe),
        Some(DfsMetadataJournalBatchResponse {
            result: DfsMetadataJournalBatchResult::Rejected { ref reason , ..},
            ..
        }) if reason.contains("neither a configured replica nor an exact pending membership-handoff catch-up target")
    ));
    assert!(matches!(
        validate_dfs_metadata_journal_batch_source(Some(&unfenced_context), &ordinary_batch),
        Some(DfsMetadataJournalBatchResponse {
            result: DfsMetadataJournalBatchResult::Rejected { ref reason , ..},
            ..
        }) if reason.contains("neither a configured replica nor an exact pending membership-handoff catch-up target")
    ));
}

#[test]
fn distributed_write_uses_path_selected_metadata_shard_epoch() {
    let cluster = sifs_two_primary_cluster("native-path-shard-write-epoch");
    let SifsTwoPrimaryCluster {
        _runtime: runtime,
        services_primary_node,
        mut services_primary,
        replica_node,
        ..
    } = cluster;
    let config = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|cfg| cfg.shard_id == "services")
        .unwrap();
    let written = services_primary
        .posix_write_file(
            "/services/path-shard.txt",
            b"path shard bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                replica_candidates: vec![services_primary_node, replica_node],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let backend = services_primary.metadata_backend().unwrap();
    let intent = backend
        .intents_for_object(&written.object_id)
        .unwrap()
        .into_iter()
        .find(|intent| intent.target_version == written.version)
        .unwrap();
    assert_eq!(intent.owner_epoch, config.epoch);
    let record = backend
        .prepared_operation_record(&format!("write:{}", intent.intent_id))
        .unwrap()
        .unwrap();
    assert_eq!(record.digest.shard_id, DfsMetadataShardId::new("services"));
    assert_eq!(record.digest.owner_epoch, config.epoch);
    assert_eq!(record.status, DfsPreparedOperationStatus::Committed);
    drop(services_primary);
    runtime.close().unwrap();
}

#[test]
fn retry_pending_repairs_skips_sender_that_lost_metadata_primary_authority() {
    let name = "native-repair-lost-primary-sender";
    let (runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let owner = config.primary_node_id;
    drop(initial);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let peers = nodes
        .iter()
        .filter(|node| **node != owner)
        .cloned()
        .collect::<Vec<_>>();
    let new_primary = peers[0].clone();
    let replica = peers[1].clone();
    let path = "/services/native-lost-primary-repair.bin";
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    policy.required_node_ids = vec![owner.clone(), replica.clone()];
    runtime
        .apply_placement_policy(&owner, path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    for route in [
        crate::DFS_DATA_STAGE_BLOB_ROUTE,
        crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
    ] {
        runtime.install_fault(crate::test_support::DfsTestFaultRule {
            source: Some(owner.clone()),
            target: Some(replica.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
        });
    }
    // This fixture owns the actual Core producer, rather than the browser
    // client whose same-node write is delegated to Main.
    let mut old = runtime.metadata_primary_service(path).unwrap();
    let (_, _, replication) = old
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 2)
        .unwrap();
    assert_eq!(replication.min_commit_replicas, 1);
    assert_eq!(replication.desired_replicas, 2);
    assert_eq!(
        replication
            .required_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        [owner.clone(), replica.clone()].into_iter().collect(),
        "the native plan must retain this fixture's actual required missing target"
    );
    let stat = old
        .posix_write_file(
            path,
            b"actual old-primary repair bytes",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), replica.clone()],
                policy: replication,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let job = old
        .coordinator()
        .repair_jobs()
        .iter()
        .find(|job| {
            job.object_id == stat.object_id
                && job.version == stat.version
                && job.missing_replica_node_id == replica
        })
        .unwrap()
        .clone();
    assert!(old.writes.pending_writes.contains_key(&job.intent_id));
    let activated = runtime
        .reassign_metadata_shard(
            &stat.file_system_id,
            "services",
            new_primary.clone(),
            nodes
                .iter()
                .filter(|node| **node != new_primary)
                .cloned()
                .collect(),
        )
        .unwrap();
    assert_eq!(activated.primary_node_id, new_primary);
    let before = runtime.rpc_observations().len();
    assert!(old.retry_pending_repairs_bounded(1).unwrap().is_empty());
    assert!(!old.writes.pending_writes.contains_key(&job.intent_id));
    assert!(
        !runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && [
                    crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
                    crate::DFS_DATA_FETCH_BLOB_ROUTE
                ]
                .contains(&event.route.as_str())
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch),
        "an actual retired metadata primary never dispatches repair after the normal handoff"
    );
    drop(old);
    runtime.clear_faults();
    runtime.close().unwrap();
}

fn native_negative_lost_authority_reply(
    name: &str,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NodeId,
    NodeId,
    NetworkedDfsService<DfsNodeTransport>,
    DfsStat,
    String,
    Option<impl Send + use<>>,
    DfsRepairReplicaResponse,
) {
    // Preserve the actual missing target until this Core producer consumes
    // its genuine Repair response. The existing hook denies only background
    // claims; it neither alters queue custody nor blocks this foreground call.
    let (runtime, owner, replica, service, stat, intent_id, seed_interception) =
        super::repair::native_committed_repair_backlog(name, true);
    drop(service);
    runtime.clear_faults();
    let captured = Arc::new(Mutex::new(Vec::<DfsRoutedMessage>::new()));
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::CaptureTransferReplyAndLoseAfterDispatch {
            intent_id: Some(intent_id.clone()),
            captured: Arc::clone(&captured),
        },
    });
    // The real Main producer checkpoints its named transfer before dispatch.
    // Losing its genuine returned reply preserves that SAME physical claim;
    // a direct Core helper must not insert a request after the carrier ran.
    drop(seed_interception);
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let tick = runtime
        .maintenance_tick(&owner, deadline.saturating_duration_since(Instant::now()))
        .unwrap();
    assert!(
        tick.outcome.is_ok(),
        "actual Main repair producer: {:?}",
        tick.outcome
    );
    let mut original = crate::test_runtime::executor()
        .block_on(async {
            tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), async {
                loop {
                    if let Some(reply) = captured
                        .lock()
                        .unwrap()
                        .iter()
                        .find(|routed| {
                            matches!(&routed.message,
                        DfsNetworkMessage::RepairReplicaResponse(response)
                            if response.intent_id == intent_id
                                && response.object_id == stat.object_id
                                && response.version == stat.version)
                        })
                        .cloned()
                    {
                        break reply;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
        })
        .expect("the selected real Main Repair returns within its unchanged native window");
    let background_interception = Some(
        runtime
            .deny_background_replica_before_claim(&owner)
            .unwrap(),
    );
    runtime.clear_faults();
    let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
        runtime.data_dir(&owner),
    ));
    let captured_records = queue
        .records_for_targets(&intent_id, std::slice::from_ref(&replica))
        .unwrap();
    assert_eq!(captured_records.len(), 1);
    let mut service = runtime.metadata_primary_service(&stat.path).unwrap();
    assert_eq!(service.node_id(), &owner);
    assert!(
        service
            .retry_pending_repairs_bounded_with_attempts(0, &captured_records)
            .unwrap()
            .is_empty()
    );
    let current = service
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    let shard = service
        .local_metadata_owner_shard_hint_for_object(&current)
        .unwrap()
        .unwrap();
    let execution = service
        .acquire_prepared_execution(&DfsPreparedOperationScope {
            file_system_id: stat.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(shard.shard_id),
            owner_epoch: shard.epoch,
            operation_id: intent_id.clone(),
        })
        .unwrap()
        .expect("the mounted current Core owner supplies actual admission for reply consumption");
    assert_eq!(original.source_node_id, replica);
    assert_eq!(original.target_node_id, owner);
    let DfsNetworkMessage::RepairReplicaResponse(response) = &mut original.message else {
        unreachable!()
    };
    assert_eq!(response.file_system_id, stat.file_system_id);
    assert_eq!(response.object_id, stat.object_id);
    assert_eq!(response.version, stat.version);
    assert_eq!(response.replica_node_id, replica);
    let claimed = &captured_records[0];
    assert_eq!(claimed.intent_id, intent_id);
    assert_eq!(claimed.object_id, stat.object_id);
    assert_eq!(claimed.version, stat.version);
    assert_eq!(
        Some(claimed.content_hash.as_str()),
        stat.content_hash.as_deref()
    );
    assert_eq!(claimed.missing_replica_node_id, replica);
    assert_eq!(
        claimed.transfer_kind,
        Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair)
    );
    assert_eq!(
        claimed.transfer_request_id.as_deref(),
        Some(response.request_id.as_str())
    );
    assert_eq!(claimed.completed_transfer_request_id, None);
    assert_eq!(
        service
            .writes
            .pending_writes
            .get(&intent_id)
            .unwrap()
            .repair_inflight_request_ids
            .get(&replica),
        Some(&response.request_id)
    );
    assert!(
        matches!(&response.result, DfsBlobTransferResult::Committed { content_hash: hash }
        | DfsBlobTransferResult::Durable { content_hash: hash }
        if Some(hash.as_str()) == stat.content_hash.as_deref())
    );
    // Negative reply fault only. This does not claim that a handoff happened
    // or construct a different healthy authority tuple, receipt or permission.
    response.result = DfsBlobTransferResult::Rejected {
        reason: "source is not current metadata shard primary".into(),
    };
    let rejected = response.clone();
    let events = service
        .process_routed_messages_with_options(vec![original], true)
        .unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::RepairRejected { replica_node_id, reason, .. }
            if replica_node_id == &replica && reason.contains("source is not current metadata shard primary"))));
    service.observe_repair_maintenance_events(&events);
    let terminal = queue
        .records_for_targets(&intent_id, std::slice::from_ref(&replica))
        .unwrap();
    assert_eq!(terminal.len(), 1);
    assert_eq!(terminal[0].transfer_request_id, None);
    assert_eq!(
        terminal[0].completed_transfer_request_id.as_deref(),
        Some(rejected.request_id.as_str())
    );
    assert!(!terminal[0].in_flight);
    drop(execution);
    drop(queue);
    for route in [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE] {
        runtime.install_fault(crate::test_runtime::FaultRule {
            source: Some(owner.clone()),
            target: Some(replica.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    }
    (
        runtime,
        owner,
        replica,
        service,
        stat,
        intent_id,
        background_interception,
        rejected,
    )
}

#[test]
fn repair_authority_rejection_suppresses_stale_retry_loop() {
    let (
        runtime,
        owner,
        replica,
        mut service,
        _stat,
        intent_id,
        background_interception,
        _rejected,
    ) = native_negative_lost_authority_reply("native-negative-authority-suppression");
    let before = runtime.rpc_observations().len();
    for _ in 0..100 {
        assert!(service.retry_pending_repairs_bounded(1).unwrap().is_empty());
    }
    let pending = service.writes.pending_writes.get(&intent_id).unwrap();
    let retry = pending.repair_retries.get(&replica).unwrap();
    assert_eq!(retry.next_retry_tick, DFS_REPAIR_RETRY_SUPPRESSED_TICK);
    assert!(!retry.inflight);
    assert!(!pending.repair_inflight_request_ids.contains_key(&replica));
    assert!(!service.repair_retry_is_due(&intent_id, &replica));
    // Pure wake classification on a detached copy of the actual suppressed
    // state exercises the same helper used by Fetch rejection. It cannot mint
    // a fetch request or change any live runtime completion/authority state.
    let mut wake_input = retry.clone();
    crate::transfer::wake_repair_retry_state(&mut wake_input);
    assert_eq!(wake_input.next_retry_tick, DFS_REPAIR_RETRY_SUPPRESSED_TICK);
    assert!(!wake_input.inflight);
    assert!(runtime.rpc_observations()[before..].iter().all(|event| {
        !(event.source == owner
            && event.target == replica
            && [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE]
                .contains(&event.route.as_str())
            && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch)
    }));
    drop(service);
    drop(background_interception);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn repair_attempt_restore_preserves_lost_authority_suppression() {
    let (mut runtime, owner, replica, service, stat, intent_id, background_interception, rejected) =
        native_negative_lost_authority_reply("native-negative-authority-restore");
    let records = service.active_repair_attempt_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].intent_id, intent_id);
    assert_eq!(records[0].missing_replica_node_id, replica);
    assert_eq!(records[0].object_id, stat.object_id);
    assert_eq!(
        records[0].next_retry_unix_nanos,
        u64::MAX,
        "the actual current negative-reply consumer produced lost-authority suppression"
    );
    assert!(
        records[0]
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("source is not current metadata shard primary"))
    );
    // This direct Core consumer does not run Main's report checkpoint. Save
    // its actual terminal suppression through the existing observed queue CAS
    // before the normal restarted Main can borrow that scheduling state.
    let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
        runtime.data_dir(&owner),
    ));
    let (mut saved, observed) = queue.view_for_maintenance().unwrap();
    let before = saved
        .records
        .iter_mut()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .expect("actual native Repair terminal remains in the owned queue");
    assert_eq!(before.object_id, records[0].object_id);
    assert_eq!(before.version, records[0].version);
    assert_eq!(before.content_hash, records[0].content_hash);
    assert_eq!(before.transfer_kind, records[0].transfer_kind);
    assert_eq!(rejected.intent_id, intent_id);
    assert_eq!(rejected.file_system_id, stat.file_system_id);
    assert_eq!(rejected.object_id, stat.object_id);
    assert_eq!(rejected.version, stat.version);
    assert_eq!(rejected.replica_node_id, replica);
    assert!(matches!(
        &rejected.result,
        DfsBlobTransferResult::Rejected { reason }
            if records[0].last_error.as_deref() == Some(reason.as_str())
    ));
    assert_eq!(before.transfer_request_id, None);
    assert_eq!(
        before.completed_transfer_request_id.as_deref(),
        Some(rejected.request_id.as_str())
    );
    assert!(!before.in_flight);
    assert_eq!(records[0].transfer_request_id, None);
    assert_eq!(
        records[0].completed_transfer_request_id.as_deref(),
        Some(rejected.request_id.as_str())
    );
    assert_eq!(
        crate::service_repair::join_transfer_delivery(
            before.transfer_delivery_state,
            records[0].transfer_delivery_state,
        ),
        records[0].transfer_delivery_state,
        "actual SAME-request reply can advance delivery; old PossiblyApplied cannot be erased"
    );
    assert!(!records[0].in_flight);
    // The real admitted consumer already checkpointed this SAME terminal.
    // Save only its actual later report's suppression schedule/error through
    // the normal observed queue CAS, retaining every other physical row.
    *before = records[0].clone();
    assert!(matches!(
        queue.reconcile_observed(&observed, &saved.records).unwrap(),
        crate::repair_queue_store::DfsRepairQueueObservedReconcileOutcome::Applied(_)
    ));
    assert_eq!(queue.view().unwrap().records, saved.records);
    drop(queue);
    drop(service);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut restored = runtime.metadata_primary_service(&stat.path).unwrap();
    assert_eq!(restored.node_id(), &owner);
    let before = runtime.rpc_observations().len();
    assert!(
        restored
            .retry_pending_repairs_bounded_with_attempts(0, &records)
            .unwrap()
            .is_empty()
    );
    assert_eq!(restored.active_repair_attempt_records(), records);
    let scheduled = restored
        .retry_pending_repairs_bounded_with_attempts(1, &records)
        .unwrap();
    assert!(scheduled.is_empty());
    assert!(!restored.repair_retry_is_due(&intent_id, &replica));
    let retained = restored.active_repair_attempt_records();
    assert_eq!(retained.len(), 1);
    assert_eq!(
        retained[0].next_retry_unix_nanos,
        records[0].next_retry_unix_nanos
    );
    assert_eq!(retained[0].last_error, records[0].last_error);
    assert!(runtime.rpc_observations()[before..].iter().all(|event| {
        !(event.source == owner
            && event.target == replica
            && [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE]
                .contains(&event.route.as_str())
            && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch)
    }));
    drop(restored);
    drop(background_interception);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn repair_command_accepts_newer_authority_shard_when_receiver_view_is_stale() {
    let name = "native-repair-new-authority-projection-gap";
    let (runtime, initial, old_config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let old_primary = old_config.primary_node_id.clone();
    drop(initial);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let peers = nodes
        .iter()
        .filter(|node| **node != old_primary)
        .cloned()
        .collect::<Vec<_>>();
    let current_primary = peers[0].clone();
    let target = peers[1].clone();
    let path = "/services/native-stale-configuration-repair.bin";
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    policy.required_node_ids = vec![old_primary.clone(), target.clone()];
    policy.forbidden_node_ids = vec![current_primary.clone()];
    runtime
        .apply_placement_policy(&old_primary, path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    deny_native_read_repair(&runtime, &old_primary, &target);
    let mut writer = runtime.client(&old_primary).unwrap();
    let (resolved, _, replication) = writer
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 2)
        .unwrap();
    assert_eq!(
        resolved.policy.consistency,
        DfsObjectPolicy::ReplicatedEventual
    );
    assert_eq!(replication.min_commit_replicas, 1);
    assert_eq!(replication.desired_replicas, 2);
    assert_eq!(
        replication
            .required_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        [old_primary.clone(), target.clone()].into_iter().collect()
    );
    assert_eq!(
        replication.forbidden_node_ids,
        vec![current_primary.clone()]
    );
    let bytes = vec![0x79; 2 * 1024 * 1024 + 4096];
    let stat = writer
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions {
                replica_candidates: vec![old_primary.clone(), target.clone()],
                policy: replication,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(writer);
    // Main's ordinary committed-work publication makes real replica metadata
    // available. A distributed stat overlay alone is not physical replay.
    let published = runtime
        .maintenance_tick(&old_primary, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(
        published.outcome.is_ok(),
        "ordinary Main publication: {:?}",
        published.outcome
    );
    let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
        runtime.data_dir(&old_primary),
    ));
    let captured = queue
        .view()
        .unwrap()
        .records
        .into_iter()
        .filter(|row| {
            row.object_id == stat.object_id
                && row.version == stat.version
                && Some(row.content_hash.as_str()) == stat.content_hash.as_deref()
                && row.missing_replica_node_id == target
        })
        .collect::<Vec<_>>();
    assert_eq!(captured.len(), 1);
    let mut source = runtime.metadata_primary_service(path).unwrap();
    assert!(
        source
            .retry_pending_repairs_bounded_with_attempts(0, &captured)
            .unwrap()
            .is_empty()
    );
    let job = source
        .coordinator()
        .repair_jobs()
        .iter()
        .find(|job| {
            job.object_id == stat.object_id
                && job.version == stat.version
                && job.missing_replica_node_id == target
        })
        .unwrap()
        .clone();
    let intent = source
        .metadata_backend()
        .unwrap()
        .get_intent(&stat.object_id, &job.intent_id)
        .unwrap()
        .unwrap();
    let mut next = runtime.client(&current_primary).unwrap();
    // A content hint cannot substitute for the normal authoritative lookup.
    // Synchronize the real published object before requesting its cache bytes.
    let observed = next.posix_stat_distributed(path).unwrap();
    assert_eq!(observed.object_id, stat.object_id);
    assert_eq!(observed.version, stat.version);
    assert_eq!(observed.content_hash, stat.content_hash);
    assert_eq!(
        next.request_remote_object_read(&stat.object_id).unwrap(),
        vec![old_primary.clone()]
    );
    next.process_incoming().unwrap();
    assert_eq!(
        next.content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes,
        "the future primary obtains only ordinary verified cache bytes before authority changes"
    );
    let mut target_before = runtime.client(&target).unwrap();
    let target_observed = target_before.posix_stat_distributed(path).unwrap();
    assert_eq!(target_observed.object_id, stat.object_id);
    assert_eq!(target_observed.version, stat.version);
    assert_eq!(target_observed.content_hash, stat.content_hash);
    let target_content = target_before.content().clone();
    let target_head = target_before
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let configs = runtime.metadata_shards().unwrap();
    let retiring = configs
        .iter()
        .find(|config| config.shard_id == "services")
        .unwrap()
        .clone();
    assert_eq!(retiring.primary_node_id, old_primary);
    let key = crate::control_plane::metadata_shard_membership_handoff_key(
        &stat.file_system_id,
        "services",
    );
    let gap = runtime
        .lose_configuration_delta_and_pause_rebuild(&target, key.clone())
        .unwrap();
    let handoff = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut())
                .request_metadata_shard_primary_handoff(&retiring, &current_primary)
        })
        .unwrap()
        .unwrap();
    // The third receiver owns no fabricated replacement view. Normal joint
    // handoff can activate through the two actual assignment majorities and
    // both real primaries while its configuration rebuild remains withheld.
    let handoff_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let reconciled = source.reconcile_pending_metadata_shard_membership_handoffs(
        &configs,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    );
    let completed = runtime
        .observe_completed_native_handoff(&control, &handoff, handoff_deadline)
        .unwrap();
    assert_eq!(
        completed.as_ref(),
        Some(&handoff.desired_shard),
        "the exact original handoff must have native proof and activated retiring-owner freeze; explicit caller={reconciled:?}"
    );
    let mut divergent_request_head = handoff.clone();
    divergent_request_head
        .required_head_watermark
        .as_mut()
        .unwrap()
        .state_fingerprint = format!("dfs-metadata-prefix-v2:blake3:{}", "0".repeat(64));
    assert!(
        runtime
            .observe_completed_native_handoff(&control, &divergent_request_head, handoff_deadline,)
            .unwrap()
            .is_none(),
        "a later sequence cannot conceal a forked original prefix"
    );
    gap.wait_rebuild(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(
        gap.dropped_delta()
            .unwrap()
            .events
            .iter()
            .any(|event| event.key == key)
    );
    let activated = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut())
                .get_metadata_shard(&stat.file_system_id, "services")
        })
        .unwrap()
        .unwrap();
    assert_eq!(activated, handoff.desired_shard);
    assert_eq!(activated.primary_node_id, current_primary);
    assert!(activated.epoch > retiring.epoch);
    assert!(
        target_content
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .is_err()
    );
    next.rebind_completed_sifs_metadata_shard_membership_handoffs()
        .unwrap();
    let current = next.current_read_object_metadata(&stat.object_id).unwrap();
    let request = next.next_request_id();
    let targets = vec![(target.clone(), request.clone())];
    let producer_intent = intent.clone();
    let producer_stat = stat.clone();
    let producer_bytes = bytes.len() as u64;
    let observations = runtime.rpc_observation_reader();
    let replies_before = runtime.rpc_observations().len();
    let repair = std::thread::spawn(move || {
        let sent = next.send_repair_replica_attachments_if_routable(
            &targets,
            &producer_intent.intent_id,
            &producer_stat.file_system_id,
            &producer_stat.object_id,
            producer_intent.base_version,
            &producer_stat.version,
            producer_stat.content_hash.as_deref().unwrap(),
            producer_bytes,
            Some(current),
        );
        (next, sent)
    });
    let reached = crate::test_runtime::executor().block_on(async {
        tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async {
            loop {
                if observations()[replies_before..].iter().any(|event| {
                    event.source == current_primary
                        && event.target == target
                        && event.route == crate::DFS_DATA_REPAIR_REPLICA_ROUTE
                        && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch
                }) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
    });
    let missing_before_refresh = target_content
        .read_object_version(stat.object_id.as_str(), stat.version.as_str())
        .is_err();
    // Only the real paused rebuild is released. The already-issued normal
    // current Repair must wait for native readiness/ReadIndex before ingest.
    gap.release();
    let (mut next, sent) = repair.join().unwrap();
    reached.unwrap();
    assert!(
        missing_before_refresh,
        "new authority hints cannot bypass the receiver's withheld current read"
    );
    assert!(sent.unwrap()[0].sent);
    let returned = next.transport.drain_incoming_messages().unwrap();
    assert!(returned.iter().any(|routed| routed.source_node_id == target
        && matches!(&routed.message, DfsNetworkMessage::RepairReplicaResponse(response)
            if response.request_id == request && response.object_id == stat.object_id
                && matches!(&response.result, DfsBlobTransferResult::Committed { content_hash } if Some(content_hash) == stat.content_hash.as_ref()))));
    let refreshed_target = runtime.client(&target).unwrap();
    assert!(
        runtime.rpc_observations()[replies_before..]
            .iter()
            .any(|event| event.source == current_primary
                && event.target == target
                && event.route == crate::DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    assert_eq!(
        target_content
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes
    );
    assert_eq!(
        refreshed_target
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .unwrap()
            .version,
        target_head.version,
        "receiver repair cannot invent another authoritative visible generation"
    );
    runtime.clear_faults();
    drop(refreshed_target);
    drop(target_before);
    drop(next);
    drop(source);
    drop(control);
    drop(target_content);
    drop(gap);
    runtime.close().unwrap();
}

#[test]
fn sifs_cluster_bulk_create_refreshes_stale_parent_before_publication() {
    let mut cluster = sifs_two_primary_cluster("sifs-bulk-stale-parent-rebase");
    let parent_path = "/services/bulk-stale-parent";
    let child_path = "/services/bulk-stale-parent/created.txt";
    cluster
        .services_primary
        .posix_mkdir_all(parent_path)
        .expect("authoritative bulk parent");
    sifs_two_primary_publish_until(&mut cluster, "initial bulk parent", |cluster| {
        cluster.replica.posix_stat(parent_path).is_ok()
    });
    let replica_delivery = Arc::new(tokio::sync::Semaphore::new(0));
    cluster
        ._runtime
        .install_fault(crate::test_runtime::FaultRule {
            source: Some(cluster.services_primary_node.clone()),
            target: Some(cluster.replica_node.clone()),
            route: Some("/dfs/metadata/journal-batch".to_string()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::PauseBeforeDispatch(Arc::clone(
                &replica_delivery,
            )),
        });
    // Advance the actual primary through the current kernel while delivery to
    // this requester is paused. The third live metadata member supplies the
    // genuine recovery-majority evidence; no replica table is rewritten.
    cluster
        .services_primary
        .posix_mkdir_all(&format!("{parent_path}/authority-advance"))
        .expect("real authoritative parent advance");
    let stale_parent_version = cluster
        .replica
        .posix_stat(parent_path)
        .expect("replica bulk parent")
        .version;

    let authority_parent_before = cluster
        .services_primary
        .posix_stat(parent_path)
        .expect("advanced authoritative bulk parent");
    assert_ne!(
        authority_parent_before.version, stale_parent_version,
        "test must retain a stale requester parent"
    );
    assert_eq!(
        cluster
            .replica
            .posix_stat(parent_path)
            .expect("still-stale replica bulk parent")
            .version,
        stale_parent_version,
        "requester must not process the queued parent advances before preparing the batch"
    );
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
        root,
        services_primary,
        mut replica,
        ..
    } = cluster;
    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    let bulk_capture = _runtime
        .capture_namespace_requests(&services_primary_node, 32)
        .expect("actual Main bulk request capture");

    let stats = replica
        .posix_create_files_local_eventual_with_known_parents([(
            child_path,
            b"stale parent bulk payload".as_slice(),
        )])
        .expect("bulk create from stale-parent requester");
    assert_eq!(stats.len(), 1);

    stop.store(true, Ordering::SeqCst);
    let services_primary = services_thread.join().expect("services primary thread");
    let authority_parent_after = services_primary
        .posix_stat(parent_path)
        .expect("authoritative parent after bulk create");
    assert_ne!(
        authority_parent_after.version, authority_parent_before.version,
        "the successful retry must advance the authoritative parent"
    );
    assert_eq!(
        services_primary
            .posix_stat(child_path)
            .expect("authoritative bulk child")
            .content_hash
            .as_deref(),
        Some(content_hash(b"stale parent bulk payload").as_str())
    );
    // The public client does not own Main's current request rows, and completed
    // rows may already be compact receipts. Inspect the actual owning shard,
    // bound to the exact native requests captured in this invocation.
    let current_owner = _runtime.metadata_primary_service(parent_path).unwrap();
    let backend = current_owner.metadata_backend().unwrap();
    let mut bulk_results = Vec::new();
    for routed in bulk_capture.records() {
        let DfsNetworkMessage::NamespaceRequest(command) = routed.message else {
            continue;
        };
        if !matches!(
            command.operation,
            DfsNamespaceOperation::CommitLocalEventualBatch { .. }
        ) {
            continue;
        }
        let DfsNamespaceOperation::CommitLocalEventualBatch { writes, .. } = &command.operation
        else {
            unreachable!()
        };
        assert_eq!(writes.len(), 1);
        assert_eq!(
            writes[0].expected_parent_object_id.as_ref(),
            Some(&authority_parent_before.object_id)
        );
        assert_eq!(
            writes[0].expected_parent_version.as_ref(),
            Some(&authority_parent_before.version)
        );
        assert_ne!(
            writes[0].expected_parent_version.as_ref(),
            Some(&stale_parent_version)
        );
        let expected_digest =
            crate::service_routed_namespace::namespace_command_digest(&command).unwrap();
        if let Some(receipt) = backend
            .get_namespace_request_receipt(&routed.source_node_id, &command.request_id)
            .unwrap()
        {
            assert_eq!(receipt.command_digest, expected_digest);
            bulk_results.push(receipt.result);
        } else {
            let request = backend
                .get_namespace_request(&routed.source_node_id, &command.request_id)
                .unwrap()
                .expect("same native bulk request's durable outcome");
            assert_eq!(request.command_digest, expected_digest);
            bulk_results.push(request.result.expect("actual bulk request completed"));
        }
    }
    assert_eq!(
        bulk_results,
        vec![DfsNamespaceResult::Applied],
        "the normal browser resolves the current owner before preparing its one bulk command"
    );
    _runtime.clear_faults();
    replica_delivery.add_permits(64);
    drop(current_owner);
    drop(bulk_capture);
    drop(services_primary);
    drop(replica);
    drop(shared_kv);
    _runtime.close().expect("normal fixture shutdown");
}

#[test]
fn routed_namespace_rejects_stale_authority_stamp_before_mutation() {
    let cluster = sifs_two_primary_cluster("sifs-namespace-stale-authority-stamp");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
        root,
        replica_node,
        mut services_primary,
        replica,
        ..
    } = cluster;
    let file_system_id = sifs_file_system_id();
    let path = "/services/stale-authority-stamp";
    let current_primary = services_primary.node_id().clone();
    let operation = DfsNamespaceOperation::MkdirAll {
        path: path.to_string(),
    };
    let mut authority_shards = services_primary
        .affected_namespace_shards(&file_system_id, &operation)
        .expect("current namespace authority shards");
    assert_eq!(authority_shards.len(), 1, "test path must be single-shard");
    authority_shards[0].epoch = authority_shards[0].epoch.saturating_sub(1);

    let event = services_primary
        .handle_namespace_request(
            replica_node.clone(),
            DfsNamespaceCommand {
                request_id: "dfs_net_req_stale_namespace_stamp".to_string(),
                file_system_id,
                requesting_node_id: replica_node,
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards,
                operation,
            },
        )
        .expect("serve stale-stamped namespace request");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result: DfsNamespaceResult::Rejected {
                ref reason,
                current_primary_node_id: Some(ref primary),
                ..
            },
            ..
        } if reason.contains("authority shard stamp") && primary == &current_primary
    ));
    assert!(
        matches!(
            services_primary.posix_stat(path),
            Err(DfsPosixError::NotFound { .. })
        ),
        "a stale admission stamp must be rejected before namespace mutation"
    );

    drop(replica);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_authority_local_follower_bootstrap_stays_on_incremental_stream() {
    let name = "native-authority-local-follower-incremental";
    let (runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/users").unwrap();
    let primary_node = config.primary_node_id;
    drop(initial);
    let follower_node = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != primary_node)
        .unwrap();
    let path = "/users/native-bootstrap-incremental";
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
    policy.required_node_ids = vec![primary_node.clone()];
    runtime
        .apply_placement_policy(&primary_node, path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    let mut primary = runtime.client(&primary_node).unwrap();
    let mut follower = runtime.client(&follower_node).unwrap();
    let before_bootstrap = follower
        .metadata_backend()
        .unwrap()
        .metadata_shard_statuses()
        .unwrap();
    follower.bootstrap_distributed_sifs().unwrap();
    assert_eq!(
        follower
            .metadata_backend()
            .unwrap()
            .metadata_shard_statuses()
            .unwrap(),
        before_bootstrap,
        "normal follower bootstrap must retain actual inherited prefixes without authoring a divergent namespace"
    );
    primary.posix_mkdir(path).unwrap();
    let before = runtime.rpc_observations().len();
    for (index, bytes) in [
        (0, b"first actual incremental payload".as_slice()),
        (1, b"second actual incremental payload".as_slice()),
    ] {
        let file = format!("{path}/record-{index}");
        let stat = primary
            .posix_write_file(
                &file,
                bytes,
                DfsWriteOptions::replicated_eventual(vec![primary_node.clone()]),
            )
            .unwrap();
        let publication = runtime
            .maintenance_tick(&primary_node, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap();
        assert!(publication.outcome.is_ok(), "{:?}", publication.outcome);
        assert_eq!(publication.omitted_kv_event_count, 0);
        assert_eq!(follower.posix_stat(&file).unwrap().version, stat.version);
        assert_eq!(follower.posix_read_file_distributed(&file).unwrap(), bytes);
    }
    let calls = runtime.rpc_observations()[before..].to_vec();
    assert!(calls.iter().any(|event| {
        event.source == primary_node
            && event.target == follower_node
            && [
                crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
                crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
            ]
            .contains(&event.route.as_str())
            && event.phase == crate::test_support::DfsTestRpcPhase::Completed
    }));
    assert!(
        !calls.iter().any(|event| event.source == primary_node
            && event.target == follower_node
            && [
                crate::DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
                crate::DFS_DATA_METADATA_SNAPSHOT_INSTALL_ROUTE
            ]
            .contains(&event.route.as_str())
            && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch),
        "actual authority-local bootstrap keeps normal follow-up writes on incremental publication"
    );
    drop(follower);
    drop(primary);
    runtime.close().unwrap();
}

#[test]
fn served_routed_write_committed_work_tracks_only_authority_owned_repairs() {
    for desired in [1, 2] {
        let name = format!("native-authority-served-work-n{desired}");
        let (runtime, initial, shard) =
            crate::test_support::current_native_metadata_service(&name, 3, "/services").unwrap();
        let primary = shard.primary_node_id;
        drop(initial);
        let peers = crate::test_runtime::DfsTestRuntime::ordered_node_ids(&name, 3)
            .into_iter()
            .filter(|node| *node != primary)
            .collect::<Vec<_>>();
        let replica = peers[0].clone();
        let requester = peers[1].clone();
        let path = format!("/services/{name}.bin");
        let nodes = if desired == 1 {
            vec![requester.clone()]
        } else {
            vec![requester.clone(), replica.clone()]
        };
        let mut placement =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, desired).unwrap();
        placement.required_node_ids = nodes.clone();
        runtime
            .apply_placement_policy(&primary, &path, DfsPlacementScope::Subtree, placement)
            .unwrap();
        // Create the predecessor under the intended current Eventual policy.
        // Updating managed configuration cannot rewrite an existing Confirmed
        // object inherited from the read fixture.
        let mut seed = runtime.client(&requester).unwrap();
        let mut seed_policy = DfsReplicationPolicy::new(1, desired).unwrap();
        seed_policy.required_node_ids = nodes.clone();
        let base = seed
            .posix_write_file(
                &path,
                b"actual routed predecessor",
                DfsWriteOptions {
                    replica_candidates: nodes.clone(),
                    policy: seed_policy,
                    wait_for_commit: true,
                    foreground_replica_repair: true,
                },
            )
            .unwrap();
        assert_eq!(
            seed.current_read_object_metadata(&base.object_id)
                .unwrap()
                .policy,
            DfsObjectPolicy::ReplicatedEventual
        );
        drop(seed);
        deny_native_read_repair(&runtime, &primary, &replica);
        let capture = runtime.capture_served_work(&primary, 32).unwrap();
        let mut client = runtime.client(&requester).unwrap();
        let request = DfsWriteRequest {
            file_system_id: base.file_system_id,
            object_id: base.object_id,
            target_version: crate::namespace::next_version(&base.version),
            requesting_node_id: requester.clone(),
        };
        let bytes = vec![0x77; 2 * 1024 * 1024 + 19];
        let operation = sinomo_core::Id::generate().to_string();
        let mut policy = DfsReplicationPolicy::new(1, desired).unwrap();
        policy.required_node_ids = nodes.clone();
        client
            .write_distributed_bytes_with_base(
                request.clone(),
                Some(base.version),
                bytes.clone(),
                nodes,
                policy,
                DfsDistributedWriteOptions {
                    operation_id: Some(operation.clone()),
                    repair: DfsWriteRepair::Background,
                    ..DfsDistributedWriteOptions::default()
                },
            )
            .unwrap();
        let observed = capture.records().into_iter().find(|row| matches!(
            &row.command.routed_message.message,
            DfsNetworkMessage::DistributedWriteRequest(command)
                if command.request_id == operation && command.request.object_id == request.object_id))
            .expect("same native routed operation's actual Main report");
        assert_eq!(observed.command.routed_message.source_node_id, requester);
        assert_eq!(observed.command.routed_message.target_node_id, primary);
        assert!(observed.succeeded);
        assert!(observed.committed_work.publish_metadata);
        assert_eq!(observed.committed_work.repair_maintenance, desired == 2);
        assert_eq!(observed.repair_record_count, usize::from(desired == 2));
        assert!(observed.new_repair_records_unattempted);
        assert!(!observed.committed_work.metadata_replica_progress);
        let terminal = client
            .transport
            .drain_incoming_messages()
            .unwrap()
            .into_iter()
            .find_map(|routed| match routed.message {
                DfsNetworkMessage::DistributedWriteResponse(response)
                    if response.request_id == operation =>
                {
                    Some(response)
                }
                _ => None,
            })
            .unwrap();
        let DfsDistributedWriteResult::Outcome {
            outcome:
                NetworkedDfsWriteOutcome::Committed {
                    intent_id,
                    repair_jobs,
                    version,
                    content_hash: hash,
                },
        } = &terminal.result
        else {
            panic!("actual W1 native publication required")
        };
        assert_eq!(version, &request.target_version);
        assert_eq!(hash, &content_hash(&bytes));
        assert_eq!(repair_jobs.len(), usize::from(desired == 2));
        let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
            runtime.data_dir(&primary),
        ))
        .view()
        .unwrap();
        let owned = queue
            .records
            .iter()
            .filter(|record| {
                record.intent_id == *intent_id
                    && record.object_id == request.object_id
                    && record.version == request.target_version
            })
            .collect::<Vec<_>>();
        assert_eq!(owned.len(), usize::from(desired == 2));
        if let Some(record) = owned.first() {
            assert_eq!(record.missing_replica_node_id, replica);
        }
        drop(capture);
        drop(client);
        runtime.clear_faults();
        runtime.close().unwrap();
    }
}

#[test]
fn served_replica_stage_does_not_claim_authority_repair_work() {
    let name = "native-replica-stage-served-work";
    let (runtime, initial, shard) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let primary = shard.primary_node_id;
    let replica = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != primary)
        .unwrap();
    drop(initial);
    let path = "/services/native-replica-work.bin";
    let mut first_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
    first_policy.required_node_ids = vec![primary.clone()];
    runtime
        .apply_placement_policy(&primary, path, DfsPlacementScope::Subtree, first_policy)
        .unwrap();
    let mut first = runtime.client(&primary).unwrap();
    let base = first
        .posix_write_file(
            path,
            b"actual stage predecessor",
            DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
        )
        .unwrap();
    drop(first);
    let nodes = vec![primary.clone(), replica.clone()];
    let mut target_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    target_policy.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&primary, path, DfsPlacementScope::Subtree, target_policy)
        .unwrap();
    let capture = runtime.capture_served_work(&replica, 32).unwrap();
    // The actual Core producer retains the intermediate Stage reply; the
    // browser client receives the final routed completion from Main instead.
    let mut owner = runtime.metadata_primary_service(path).unwrap();
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: primary.clone(),
    };
    let bytes = vec![0x65; 2 * 1024 * 1024 + 29];
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    assert!(matches!(
        owner
            .write_distributed_reader_with_candidates(
                request.clone(),
                std::io::Cursor::new(bytes.clone()),
                nodes,
                policy,
                DfsDistributedWriteOptions::default()
            )
            .unwrap(),
        NetworkedDfsWriteOutcome::Pending { .. }
    ));
    let original = owner
        .transport
        .drain_incoming_messages()
        .unwrap()
        .into_iter()
        .find_map(|routed| match routed.message {
            DfsNetworkMessage::StageBlobResponse(response)
                if response.object_id == request.object_id =>
            {
                Some(response)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(original.replica_node_id, replica);
    assert_eq!(original.version, request.target_version);
    assert!(
        matches!(&original.result, DfsBlobTransferResult::Durable { content_hash: hash }
        if hash == &content_hash(&bytes))
    );
    let observed = capture
        .records()
        .into_iter()
        .find(|row| {
            matches!(
        &row.command.routed_message.message,
        DfsNetworkMessage::StageBlob(command) if command.request_id == original.request_id
            && command.intent_id == original.intent_id && command.object_id == request.object_id)
        })
        .expect("same actual Stage RPC's Main report");
    assert_eq!(observed.command.routed_message.source_node_id, primary);
    assert_eq!(observed.command.routed_message.target_node_id, replica);
    assert!(observed.succeeded);
    assert!(!observed.committed_work.repair_maintenance);
    assert_eq!(observed.repair_record_count, 0);
    let received = runtime.client(&replica).unwrap();
    assert_eq!(
        received
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(received);
    drop(capture);
    drop(owner);
    runtime.close().unwrap();
}

#[test]
fn served_requester_completion_does_not_claim_authority_repair_work() {
    let (runtime, primary, requester, replica, mut client, request, _bytes, original) =
        super::repair::native_routed_committed_backlog("native-requester-work-completion", false);
    let DfsNetworkMessage::DistributedWriteResponse(response) = &original.message else {
        unreachable!()
    };
    let request_id = response.request_id.clone();
    assert_eq!(original.source_node_id, primary);
    assert_eq!(original.target_node_id, requester);
    assert_eq!(response.object_id, request.object_id);
    assert!(
        client
            .writes
            .pending_routed_writes
            .contains_key(&request_id)
    );
    let DfsDistributedWriteResult::Outcome {
        outcome: NetworkedDfsWriteOutcome::Committed { repair_jobs, .. },
    } = &response.result
    else {
        panic!("actual current primary committed response required")
    };
    assert_eq!(repair_jobs.len(), 1);
    assert_eq!(repair_jobs[0].missing_replica_node_id, replica);
    // The response and pending owner both came from the same normal native
    // request. Use the same work capture and normal consumer as the served lane.
    client.begin_message_committed_work();
    let DfsNetworkMessage::DistributedWriteResponse(response) = original.message else {
        unreachable!()
    };
    let event = client
        .handle_distributed_write_response(primary.clone(), response)
        .unwrap();
    let committed_work = client.take_message_committed_work();
    assert!(matches!(event,
        NetworkedDfsServiceEvent::RemoteWriteCompleted { request_id: id, .. }
            if id == request_id));
    assert!(
        !committed_work.repair_maintenance,
        "observing another node's actual repair backlog does not grant requester ownership"
    );
    assert!(client.coordinator().repair_jobs().is_empty());
    let mut authority = runtime.client(&primary).unwrap();
    authority.hydrate_from_metadata_backend().unwrap();
    assert!(authority.coordinator().repair_jobs().iter().any(|job|
        job.object_id == request.object_id && job.missing_replica_node_id == replica));
    drop(authority);
    drop(client);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn metadata_receipt_retry_recovers_original_prepare_and_receipt_identity() {
    let (mut runtime, mut requester, shard) = crate::test_support::current_native_metadata_service(
        "native-metadata-receipt-exact-retry",
        3,
        "/",
    )
    .unwrap();
    let path = "/receipt-exact-retry";
    let original = requester
        .posix_write_file(
            path,
            b"visible original",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![shard.primary_node_id.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(requester);
    let mut owner = runtime.metadata_primary_service(path).unwrap();
    let version = DfsVersion::generate();
    // Actual native durable content precedes the producer's receipt. This test
    // exercises receipt publication alone and never makes this version visible.
    let manifest = owner
        .content()
        .put_object_version(
            original.object_id.as_str(),
            version.as_str(),
            b"native content awaiting receipt publication",
        )
        .unwrap();
    let stopped = shard.replica_node_ids[0].clone();
    runtime.stop_node(&stopped).unwrap();
    assert!(
        owner
            .put_metadata_receipt(
                &original.file_system_id,
                &original.object_id,
                &version,
                &shard.primary_node_id,
                &manifest.content_hash
            )
            .is_err()
    );
    let prepared = owner.metadata_backend().unwrap().list_prepared_operations().unwrap().into_iter().find(|record| {
        matches!(record.payload.metadata_effects.as_slice(), [DfsPreparedMetadataEffect::PutReceipt { receipt }]
            if receipt.receipt.object_id == original.object_id && receipt.receipt.committed_version == version)
    }).expect("the original native receipt publication is durably prepared");
    assert_eq!(prepared.status, DfsPreparedOperationStatus::Prepared);
    assert!(
        owner
            .backend_receipt_for_replica(&original.object_id, &version, &shard.primary_node_id)
            .unwrap()
            .is_none()
    );
    let before = owner
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&prepared.digest.shard_id)
        .unwrap();
    assert!(
        owner
            .put_metadata_receipt(
                &original.file_system_id,
                &original.object_id,
                &version,
                &shard.primary_node_id,
                &crate::content_hash(b"changed receipt input")
            )
            .is_err()
    );
    assert_eq!(
        owner
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&prepared.digest.shard_id)
            .unwrap(),
        before,
        "a conflicting retry must publish nothing"
    );
    runtime.reopen_node(stopped).unwrap();
    assert!(
        owner
            .put_metadata_receipt(
                &original.file_system_id,
                &original.object_id,
                &version,
                &shard.primary_node_id,
                &manifest.content_hash
            )
            .unwrap()
    );
    let terminal = owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&prepared.digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(terminal.status, DfsPreparedOperationStatus::Committed);
    assert_eq!(terminal.digest, prepared.digest);
    assert_eq!(terminal.payload, prepared.payload);
    let DfsPreparedMetadataEffect::PutReceipt { receipt: expected } =
        &prepared.payload.metadata_effects[0]
    else {
        unreachable!()
    };
    assert_eq!(
        owner
            .backend_receipt_for_replica(&original.object_id, &version, &shard.primary_node_id)
            .unwrap()
            .as_ref(),
        Some(expected),
        "original event ID and timestamp survive exact replay"
    );
    assert_eq!(
        owner
            .metadata_backend()
            .unwrap()
            .get_object(&original.object_id)
            .unwrap()
            .unwrap()
            .version,
        original.version
    );
    assert_eq!(
        owner
            .content()
            .read_object_version(original.object_id.as_str(), version.as_str())
            .unwrap()
            .1,
        b"native content awaiting receipt publication"
    );
    let after = owner
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&prepared.digest.shard_id)
        .unwrap();
    assert!(
        !owner
            .put_metadata_receipt(
                &original.file_system_id,
                &original.object_id,
                &version,
                &shard.primary_node_id,
                &manifest.content_hash
            )
            .unwrap()
    );
    assert_eq!(
        owner
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&prepared.digest.shard_id)
            .unwrap(),
        after
    );
    drop(owner);
    runtime.close().unwrap();
}

fn check_compound_receipt_publication_retry(capture_filesystem: bool) {
    let name = if capture_filesystem {
        "native-receipt-location-filesystem-retry"
    } else {
        "native-receipt-location-retry"
    };
    let (mut runtime, owner, replica, service, stat, intent_id, seed_interception) =
        super::repair::native_committed_repair_backlog_with_nodes(name, true, 3);
    drop(service);
    runtime.clear_faults();
    let captured = Arc::new(Mutex::new(Vec::<DfsRoutedMessage>::new()));
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::CaptureTransferReplyAndLoseAfterDispatch {
            intent_id: Some(intent_id.clone()),
            captured: Arc::clone(&captured),
        },
    });
    drop(seed_interception);
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let tick = runtime
        .maintenance_tick(&owner, deadline.saturating_duration_since(Instant::now()))
        .unwrap();
    assert!(
        tick.outcome.is_ok(),
        "actual native repair producer: {:?}",
        tick.outcome
    );
    let original = crate::test_runtime::executor().block_on(async {
        tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), async {
            loop {
                if let Some(reply) = captured.lock().unwrap().iter().find(|routed| {
                    matches!(&routed.message, DfsNetworkMessage::RepairReplicaResponse(r)
                        if r.intent_id == intent_id && r.object_id == stat.object_id && r.version == stat.version)
                }).cloned() { break reply; }
                tokio::task::yield_now().await;
            }
        }).await
    }).expect("genuine repair reply within the unchanged native window");
    let background = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    runtime.clear_faults();
    assert_eq!(original.source_node_id, replica);
    assert_eq!(original.target_node_id, owner);
    let DfsNetworkMessage::RepairReplicaResponse(response) = &original.message else {
        unreachable!()
    };
    assert_eq!(response.replica_node_id, replica);
    assert_eq!(response.file_system_id, stat.file_system_id);
    let hash = match &response.result {
        DfsBlobTransferResult::Durable { content_hash }
        | DfsBlobTransferResult::Committed { content_hash } => content_hash.clone(),
        other => panic!("actual receiver did not retain durable bytes: {other:?}"),
    };
    assert_eq!(Some(hash.as_str()), stat.content_hash.as_deref());
    let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
        runtime.data_dir(&owner),
    ));
    let records = queue
        .records_for_targets(&intent_id, std::slice::from_ref(&replica))
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].transfer_request_id.as_deref(),
        Some(response.request_id.as_str())
    );
    assert_eq!(records[0].object_id, stat.object_id);
    assert_eq!(records[0].version, stat.version);
    assert_eq!(records[0].content_hash, hash);
    if capture_filesystem {
        runtime
            .apply_placement_policy(
                &owner,
                &format!("/services/{name}-unrelated"),
                DfsPlacementScope::Subtree,
                DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap(),
            )
            .unwrap();
    }
    let current_configuration = runtime
        .shared_control_plane()
        .unwrap()
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut()).get_file_system_config(&stat.file_system_id)
        })
        .unwrap()
        .unwrap();
    let mut source = runtime.metadata_primary_service(&stat.path).unwrap();
    source
        .retry_pending_repairs_bounded_with_attempts(0, &records)
        .unwrap();
    let current = source
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    let shard = source
        .local_metadata_owner_shard_hint_for_object(&current)
        .unwrap()
        .unwrap();
    let mut projection_execution = if capture_filesystem {
        let execution = source
            .acquire_prepared_execution(&DfsPreparedOperationScope {
                file_system_id: stat.file_system_id.clone(),
                shard_id: DfsMetadataShardId::new(shard.shard_id.clone()),
                owner_epoch: shard.epoch,
                operation_id: intent_id.clone(),
            })
            .unwrap()
            .expect("actual scoped Core owner");
        // The ordinary bootstrap's projection step uses this SAME actual
        // native configuration. Do not bootstrap foreign roots through this
        // services-only backend, or manufacture an authority/receipt.
        source.clear_metadata_lookup_cache();
        source
            .metadata
            .add_file_system(DfsFileSystem::from(current_configuration.clone()));
        source.clear_metadata_lookup_cache();
        Some(execution)
    } else {
        None
    };
    let missing = shard
        .replica_node_ids
        .iter()
        .find(|node| **node != replica)
        .unwrap()
        .clone();
    assert_ne!(missing, owner);
    assert_ne!(missing, replica);
    runtime.stop_node(&missing).unwrap();
    assert!(
        source
            .put_metadata_receipt(
                &stat.file_system_id,
                &stat.object_id,
                &stat.version,
                &replica,
                &hash
            )
            .is_err()
    );
    // Yield the fixture's actual admission at the producer-call boundary.
    // The durable prepare and genuine reply retain recovery custody.
    drop(projection_execution.take());
    let prepared = source.metadata_backend().unwrap().list_prepared_operations().unwrap().into_iter().find(|record|
        record.metadata_effects.iter().any(|effect| matches!(effect,
            DfsPreparedMetadataEffect::PutReceipt { receipt } if receipt.receipt.object_id == stat.object_id
                && receipt.receipt.committed_version == stat.version && receipt.replica_node_id == replica)))
        .expect("genuine returned receipt retained its original compound publication");
    assert!(prepared.metadata_effects.iter().any(|effect| matches!(effect, DfsPreparedMetadataEffect::PutObject { object }
        if object.object_id == stat.object_id && object.version == stat.version && object.current_locations.contains(&replica))));
    assert_eq!(
        prepared
            .metadata_effects
            .iter()
            .any(|effect| matches!(effect, DfsPreparedMetadataEffect::PutFileSystem { .. })),
        capture_filesystem,
        "exercise the exact observed compound form"
    );
    assert!(
        source
            .backend_receipt_for_replica(&stat.object_id, &stat.version, &replica)
            .unwrap()
            .is_none()
    );
    assert!(
        !source
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .unwrap()
            .current_locations
            .contains(&replica)
    );
    let prefix = source
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&prepared.digest.shard_id)
        .unwrap();
    assert!(
        source
            .put_metadata_receipt(
                &stat.file_system_id,
                &stat.object_id,
                &stat.version,
                &replica,
                &crate::content_hash(b"wrong receipt bytes")
            )
            .is_err()
    );
    assert_eq!(
        source
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&prepared.digest.shard_id)
            .unwrap(),
        prefix
    );
    runtime.reopen_node(missing).unwrap();
    // A different caller read-set must never be hidden by matching the receipt.
    let mut conflicting = (*prepared.payload).clone();
    conflicting.expected_object_versions[0].expected_version = Some(DfsVersion::generate());
    assert!(source.publish_metadata_effect_payload(conflicting).is_err());
    assert_eq!(
        source
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&prepared.digest.shard_id)
            .unwrap(),
        prefix
    );
    let proof_captures = std::iter::once(&shard.primary_node_id)
        .chain(shard.replica_node_ids.iter())
        .map(|node| {
            (
                node.clone(),
                crate::test_support::capture_native_shard_trace(
                    &runtime.data_dir(node).root(),
                    prepared.digest.shard_id.clone(),
                )
                .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let replay = source.put_metadata_receipt(
        &stat.file_system_id,
        &stat.object_id,
        &stat.version,
        &replica,
        &hash,
    );
    if let Err(error) = &replay {
        eprintln!("compound-receipt retry error: {error:?}");
        let observations = runtime.rpc_observations();
        for observation in observations.iter().rev().take(24).rev() {
            eprintln!("compound-receipt rpc: {observation:?}");
        }
    }
    for (node, capture) in proof_captures {
        capture.close();
        if replay.is_err() {
            let path = runtime
                .data_dir(&node)
                .root()
                .join("compound-receipt-proof.json");
            capture
                .export_raw(std::fs::File::create(&path).unwrap())
                .unwrap();
            eprintln!("compound-receipt native proof: {}", path.display());
        }
    }
    assert!(replay.unwrap());
    let terminal = source
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&prepared.digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(terminal.status, DfsPreparedOperationStatus::Committed);
    assert_eq!(terminal.digest, prepared.digest);
    assert_eq!(terminal.payload, prepared.payload);
    let expected_receipt = prepared
        .metadata_effects
        .iter()
        .find_map(|effect| match effect {
            DfsPreparedMetadataEffect::PutReceipt { receipt } => Some(receipt),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        source
            .backend_receipt_for_replica(&stat.object_id, &stat.version, &replica)
            .unwrap()
            .as_ref(),
        Some(expected_receipt)
    );
    let expected_object = prepared
        .metadata_effects
        .iter()
        .find_map(|effect| match effect {
            DfsPreparedMetadataEffect::PutObject { object } => Some(object),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        source
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .as_ref(),
        Some(expected_object)
    );
    assert_eq!(
        runtime
            .shared_control_plane()
            .unwrap()
            .with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .get_file_system_config(&stat.file_system_id))
            .unwrap()
            .as_ref(),
        Some(&current_configuration)
    );
    let after = source
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&prepared.digest.shard_id)
        .unwrap();
    assert!(
        !source
            .put_metadata_receipt(
                &stat.file_system_id,
                &stat.object_id,
                &stat.version,
                &replica,
                &hash
            )
            .unwrap()
    );
    assert_eq!(
        source
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&prepared.digest.shard_id)
            .unwrap(),
        after
    );
    // Consume the unchanged genuine reply through its actual retained claim.
    let execution = source
        .acquire_prepared_execution(&DfsPreparedOperationScope {
            file_system_id: stat.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(shard.shard_id),
            owner_epoch: shard.epoch,
            operation_id: intent_id.clone(),
        })
        .unwrap()
        .expect("actual Core response-consumption owner");
    let events = source
        .process_routed_messages_with_options(vec![original], true)
        .unwrap();
    source.observe_repair_maintenance_events(&events);
    drop(execution);
    drop(projection_execution);
    drop(source);
    drop(queue);
    drop(background);
    runtime.close().unwrap();
}

#[test]
fn compound_receipt_location_retry_preserves_original_publication() {
    check_compound_receipt_publication_retry(false);
}

#[test]
fn compound_receipt_filesystem_retry_preserves_original_publication() {
    check_compound_receipt_publication_retry(true);
}
fn check_retained_intent_opening(reconcile_only: bool, restart_owner: bool) {
    let name = if reconcile_only {
        "native-retained-opening-reconcile"
    } else if restart_owner {
        "native-retained-opening-restart-retry"
    } else {
        "native-retained-opening-exact-retry"
    };
    let (mut runtime, mut client, shard) =
        crate::test_support::current_native_metadata_service(name, 3, "/").unwrap();
    let path = "/retained-intent-opening";
    let initial = client
        .posix_write_file(
            path,
            b"visible before the interrupted opening",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![shard.primary_node_id.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(client);
    let mut owner = runtime.metadata_primary_service(path).unwrap();
    let stopped = shard.replica_node_ids[0].clone();
    let content_replica = shard.replica_node_ids[1].clone();
    // Keep both required content targets live. Only the separate metadata
    // participant is missing, so W2 admission must not fail at replica setup.
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = vec![shard.primary_node_id.clone(), content_replica.clone()];
    let options = DfsWriteOptions {
        policy,
        replica_candidates: vec![shard.primary_node_id.clone(), content_replica.clone()],
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    let operation = "retained-opening-original-operation";
    let bytes = b"exact native bytes before intent publication";
    runtime.stop_node(&stopped).unwrap();
    assert!(
        owner
            .posix_write_file_with_operation_id(path, bytes, options.clone(), operation)
            .is_err()
    );
    let prepared = owner
        .metadata_backend()
        .unwrap()
        .list_prepared_operations()
        .unwrap()
        .into_iter()
        .find(|record| {
            record.payload.metadata_effects.iter().any(|effect| {
                matches!(effect, DfsPreparedMetadataEffect::PutIntent { intent }
                if intent.operation_id.as_deref() == Some(operation))
            })
        })
        .expect("actual opening prepare must survive its missing native ACK");
    assert_eq!(prepared.status, DfsPreparedOperationStatus::Prepared);
    assert!(
        prepared.payload.prepared_intent.is_none(),
        "this is the intent-opening namespace body, not a final write body"
    );
    let record = prepared
        .payload
        .metadata_effects
        .iter()
        .find_map(|effect| match effect {
            DfsPreparedMetadataEffect::PutIntent { intent }
                if intent.operation_id.as_deref() == Some(operation) =>
            {
                Some(intent.clone())
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(record.policy.min_commit_replicas, 2);
    assert_eq!(record.policy.desired_replicas, 2);
    assert!(
        owner
            .metadata_backend()
            .unwrap()
            .get_intent(&record.object_id, &record.intent_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        owner
            .metadata_backend()
            .unwrap()
            .get_object(&initial.object_id)
            .unwrap()
            .unwrap()
            .version,
        initial.version
    );
    assert_eq!(
        owner
            .content()
            .read_blob(&crate::content_hash(bytes))
            .unwrap(),
        bytes
    );
    assert!(
        owner
            .content()
            .content_reservation_roots()
            .unwrap()
            .reservations
            .iter()
            .any(
                |reservation| reservation.identity.object_id == record.object_id.as_str()
                    && reservation.identity.version == record.target_version.as_str()
                    && reservation.identity.content_hash == crate::content_hash(bytes)
            ),
        "the unpublished input retains its exact durable object/version/hash custody"
    );
    if reconcile_only {
        let prefix = owner
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&prepared.digest.shard_id)
            .unwrap();
        let outcome = owner.reconcile_stable_write_operation(
            path,
            &record.object_id,
            operation,
            &crate::content_hash(bytes),
            bytes.len() as u64,
        );
        match outcome {
            Ok(DfsStableWriteOperationReconciliation::AmbiguousOrExpired { .. }) => {}
            Err(DfsError::MetadataRecoveryQuorumUnavailable {
                required: 3,
                verified: 2,
                ..
            }) => {}
            other => panic!(
                "a retained uncommitted opening is neither authoritative absence nor successful publication; only the actual missing native proof may block this read: {other:?}"
            ),
        }
        let still_prepared = owner
            .metadata_backend()
            .unwrap()
            .prepared_operation_record(&prepared.digest.operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(still_prepared.digest, prepared.digest);
        assert_eq!(still_prepared.payload, prepared.payload);
        assert_eq!(still_prepared.status, prepared.status);
        assert_eq!(
            owner
                .metadata_backend()
                .unwrap()
                .metadata_prefix_proof(&prepared.digest.shard_id)
                .unwrap(),
            prefix
        );
    } else {
        let prefix = owner
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&prepared.digest.shard_id)
            .unwrap();
        assert!(
            owner
                .posix_write_file_with_operation_id(
                    path,
                    b"different input",
                    options.clone(),
                    operation
                )
                .is_err()
        );
        assert_eq!(
            owner
                .metadata_backend()
                .unwrap()
                .metadata_prefix_proof(&prepared.digest.shard_id)
                .unwrap(),
            prefix
        );
        assert_eq!(
            owner
                .content()
                .read_blob(&crate::content_hash(bytes))
                .unwrap(),
            bytes
        );
        assert!(
            owner
                .content()
                .content_reservation_roots()
                .unwrap()
                .reservations
                .iter()
                .any(
                    |reservation| reservation.identity.object_id == record.object_id.as_str()
                        && reservation.identity.version == record.target_version.as_str()
                        && reservation.identity.content_hash == crate::content_hash(bytes)
                ),
            "the unpublished input retains its exact durable object/version/hash custody"
        );
        runtime.reopen_node(stopped.clone()).unwrap();
        if restart_owner {
            drop(owner);
            runtime.stop_node(&shard.primary_node_id).unwrap();
            runtime.reopen_node(shard.primary_node_id.clone()).unwrap();
            owner = runtime.metadata_primary_service(path).unwrap();
        }

        let written = owner
            .posix_write_file_with_operation_id(path, bytes, options.clone(), operation)
            .unwrap();
        assert_eq!(written.object_id, record.object_id);
        assert_eq!(written.version, record.target_version);
        let terminal = owner
            .metadata_backend()
            .unwrap()
            .prepared_operation_record(&prepared.digest.operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(terminal.status, DfsPreparedOperationStatus::Committed);
        assert_eq!(terminal.digest, prepared.digest);
        assert_eq!(terminal.payload, prepared.payload);
        let prefix = owner
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&prepared.digest.shard_id)
            .unwrap();
        owner
            .posix_write_file_with_operation_id(path, bytes, options, operation)
            .unwrap();
        assert_eq!(
            owner
                .metadata_backend()
                .unwrap()
                .metadata_prefix_proof(&prepared.digest.shard_id)
                .unwrap(),
            prefix
        );
        assert_eq!(owner.posix_read_file(path).unwrap(), bytes);
    }
    drop(owner);
    runtime.close().unwrap();
}

#[test]
fn prepared_intent_opening_is_not_authoritative_absence() {
    check_retained_intent_opening(true, false);
}

#[test]
fn exact_retry_resumes_original_intent_opening_after_ack_failure() {
    check_retained_intent_opening(false, false);
}

#[test]
fn exact_retry_resumes_original_intent_opening_after_restart() {
    check_retained_intent_opening(false, true);
}

#[test]
fn stored_prepare_retry_observes_commit_before_its_execution_admission() {
    let (mut runtime, requester, shard) = crate::test_support::current_native_metadata_service(
        "native-prepared-replay-admission-race",
        3,
        "/",
    )
    .unwrap();
    drop(requester);
    let mut source = runtime.metadata_primary_service("/").unwrap();
    let mut competing = runtime.metadata_primary_service("/").unwrap();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let file_system = source
        .metadata_backend()
        .unwrap()
        .get_file_system_on_shard(&shard_id, &shard.file_system_id)
        .unwrap()
        .unwrap();
    let scope = DfsPreparedOperationScope {
        file_system_id: shard.file_system_id.clone(),
        shard_id: shard_id.clone(),
        owner_epoch: shard.epoch,
        operation_id: format!("native-admission-race:{}", sinomo_core::Id::generate()),
    };
    let payload = DfsPreparedOperationPayload::for_metadata_effects(
        scope.clone(),
        Vec::new(),
        Vec::new(),
        vec![DfsPreparedMetadataEffect::PutFileSystem { file_system }],
        OffsetDateTime::now_utc(),
    )
    .unwrap();
    let execution = source.acquire_prepared_execution(&scope).unwrap();
    let digest = source
        .prepare_payload_for_publication(payload.clone())
        .unwrap();
    drop(execution);
    let original = source
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(original.status, DfsPreparedOperationStatus::Prepared);
    let committed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = Arc::clone(&committed);
    let competing_digest = digest.clone();
    crate::publication_guard::BEFORE_STORED_PREPARE_EXECUTION.with(|hook| {
        assert!(hook.borrow().is_none());
        *hook.borrow_mut() = Some(Box::new(move || {
            // An actual second native owner projection completes the same
            // immutable body with full participant proofs, after the caller's
            // read and before its execution admission. No result is injected.
            let publication = competing
                .commit_stored_prepared_operation(&competing_digest)
                .unwrap();
            assert!(publication.published);
            observed.store(true, std::sync::atomic::Ordering::SeqCst);
        }));
    });
    let replay = source.commit_stored_prepared_operation(&digest).unwrap();
    assert!(committed.load(std::sync::atomic::Ordering::SeqCst));
    assert!(
        !replay.published,
        "an observed terminal replay must not publish twice"
    );
    assert_eq!(replay.payload, original.payload);
    let terminal = source
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(terminal.status, DfsPreparedOperationStatus::Committed);
    assert_eq!(terminal.digest, original.digest);
    assert_eq!(terminal.payload, original.payload);
    let prefix = source
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        prefix.journal_sequence,
        digest.prepare_sequence + 1,
        "the exact native commit is the only new journal record"
    );
    drop(source);
    runtime.close().unwrap();
}
