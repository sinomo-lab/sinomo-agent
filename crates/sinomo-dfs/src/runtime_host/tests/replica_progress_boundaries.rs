use super::*;
use crate::runtime_host::message_host::{
    DaemonDfsPreDispatchTestHook, DaemonDfsPreDispatchTestPoint,
    install_daemon_dfs_pre_dispatch_test_hook,
};
use crate::test_runtime::{FaultAction, FaultRule, RpcPhase};
use std::sync::Condvar;
use std::sync::atomic::{AtomicUsize, Ordering};

async fn poll_queued_admission_once<F: std::future::Future>(
    mut future: std::pin::Pin<&mut F>,
) -> std::task::Poll<F::Output> {
    std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(future.as_mut(), cx))
    })
    .await
}

#[tokio::test]
async fn required_replica_wait_preempts_a_later_maintenance_turn_after_shard_release() {
    use crate::runtime_admission::{
        DfsMutationScheduler, acquire_dfs_maintenance_admission_until_foreground_waits,
    };
    use std::task::Poll;

    let scheduler = Arc::new(DfsMutationScheduler::new());
    let root = DfsMessageExecutionScope::ExactShard {
        file_system_id: crate::sifs_file_system_id(),
        shard_id: DfsMetadataShardId::new("root"),
    };
    let services = DfsMessageExecutionScope::ExactShard {
        file_system_id: crate::sifs_file_system_id(),
        shard_id: DfsMetadataShardId::new("services"),
    };
    let global = DfsMessageExecutionScope::GlobalExclusive;
    let services_owner = scheduler.admit(&services).await.unwrap();
    let earlier_root_owner = scheduler.admit(&root).await.unwrap();
    let earlier_turn = scheduler.begin_maintenance_interest();
    let mut required = Box::pin(admit_daemon_dfs_message(&scheduler, &root, true));
    assert!(
        poll_queued_admission_once(required.as_mut())
            .await
            .is_pending()
    );
    assert!(earlier_turn.is_requested());
    drop(earlier_turn);

    // The required RPC already requested preemption but is still waiting on
    // another root operation. A later Routine turn queues its real global writer.
    let later_turn = scheduler.begin_maintenance_interest();
    let mut maintenance = Box::pin(acquire_dfs_maintenance_admission_until_foreground_waits(
        &later_turn,
        scheduler.admit(&global),
    ));
    assert!(
        poll_queued_admission_once(maintenance.as_mut())
            .await
            .is_pending()
    );
    drop(earlier_root_owner);
    // The same RPC now reaches the global read queue behind that writer.
    assert!(
        poll_queued_admission_once(required.as_mut())
            .await
            .is_pending()
    );
    let maintenance_result = poll_queued_admission_once(maintenance.as_mut()).await;
    let required_result = poll_queued_admission_once(required.as_mut()).await;
    let yielded = matches!(&maintenance_result, Poll::Ready(Ok(None)));
    let admitted = matches!(&required_result, Poll::Ready(Ok(owner)) if owner.is_exact());
    let completed_request_left_no_interest = !scheduler.begin_maintenance_interest().is_requested();

    // Drain actual futures and returned permits before the failing assertion.
    // The services owner remains held until after the required RPC's observation.
    drop(required_result);
    drop(maintenance_result);
    drop(required);
    drop(maintenance);
    drop(later_turn);
    drop(services_owner);
    assert!(
        scheduler.try_admit_global().is_some(),
        "all actual owners drained"
    );
    assert!(
        yielded,
        "a later maintenance turn missed the still-waiting required RPC"
    );
    assert!(
        admitted,
        "an independent services owner must not strand root replication"
    );
    assert!(completed_request_left_no_interest);
}

#[tokio::test]
async fn cancelled_replica_wait_does_not_preempt_a_later_maintenance_turn() {
    use crate::runtime_admission::{
        DfsMutationScheduler, acquire_dfs_maintenance_admission_until_foreground_waits,
    };
    use std::task::Poll;

    let scheduler = Arc::new(DfsMutationScheduler::new());
    let root = DfsMessageExecutionScope::ExactShard {
        file_system_id: crate::sifs_file_system_id(),
        shard_id: DfsMetadataShardId::new("root"),
    };
    let global = DfsMessageExecutionScope::GlobalExclusive;
    let root_owner = scheduler.admit(&root).await.unwrap();
    let earlier_turn = scheduler.begin_maintenance_interest();
    let mut required = Box::pin(admit_daemon_dfs_message(&scheduler, &root, true));
    assert!(
        poll_queued_admission_once(required.as_mut())
            .await
            .is_pending()
    );
    assert!(earlier_turn.is_requested());
    drop(required);
    drop(earlier_turn);

    let later_turn = scheduler.begin_maintenance_interest();
    assert!(!later_turn.is_requested());
    let mut maintenance = Box::pin(acquire_dfs_maintenance_admission_until_foreground_waits(
        &later_turn,
        scheduler.admit(&global),
    ));
    assert!(
        poll_queued_admission_once(maintenance.as_mut())
            .await
            .is_pending()
    );
    drop(root_owner);
    let outcome = poll_queued_admission_once(maintenance.as_mut()).await;
    let admitted = matches!(&outcome, Poll::Ready(Ok(Some(owner))) if owner.is_global());
    drop(outcome);
    drop(maintenance);
    drop(later_turn);
    assert!(
        scheduler.try_admit_global().is_some(),
        "cancelled waiter drained"
    );
    assert!(
        admitted,
        "cancelled RPC cannot suppress the next maintenance turn"
    );
}

#[test]
fn remote_source_placement_preserves_repair_custody_until_actual_receipts() {
    exercise_remote_source_placement_progress(false);
}

#[test]
fn remote_source_placement_ready_custody_survives_native_restart() {
    exercise_remote_source_placement_progress(true);
}

fn exercise_remote_source_placement_progress(restart_on_ready: bool) {
    let (name, path) = if restart_on_ready {
        (
            "native-remote-source-placement-restart",
            "/services/native-remote-source-placement-restart",
        )
    } else {
        (
            "native-remote-source-placement-progress",
            "/services/native-remote-source-placement-progress",
        )
    };
    let (mut native, initial, original_shard) =
        crate::test_support::current_native_metadata_service(name, 3, path).unwrap();
    drop(initial);
    let target = original_shard.primary_node_id.clone();
    let primary = original_shard.replica_node_ids[0].clone();
    let holder = original_shard.replica_node_ids[1].clone();
    // Match the real split: root owns placement coordination, another node
    // owns services, and the content initially resides on a third node.
    let shard = native
        .reassign_metadata_shard(
            &original_shard.file_system_id,
            &original_shard.shard_id,
            primary.clone(),
            vec![target.clone(), holder.clone()],
        )
        .unwrap();
    let bytes = vec![0x5a; 4096];
    let mut writer = native.client(&holder).unwrap();
    let mut initial_policy = crate::DfsReplicationPolicy::new(1, 1).unwrap();
    initial_policy.required_node_ids = vec![holder.clone()];
    let stat = writer
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions {
                policy: initial_policy,
                replica_candidates: vec![holder.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert_eq!(writer.posix_read_file(path).unwrap(), bytes);
    drop(writer);
    let owner = native.metadata_primary_service(path).unwrap();
    assert!(
        owner
            .local_object_version_manifest_size_if_matches(
                &stat.object_id,
                &stat.version,
                stat.content_hash.as_ref().unwrap(),
            )
            .is_none(),
        "the real metadata owner must fetch this existing remote content"
    );
    drop(owner);
    // Use the existing negative before-claim hook for the restart control.
    // It cannot manufacture a ready row or a successful reply: the control must
    // observe actual ready custody before stopping its owner. The hook retains
    // no DATA owner or admission permit.
    let mut before_claim = restart_on_ready.then(|| {
        native
            .deny_background_replica_before_claim(&primary)
            .unwrap()
    });
    let desired = vec![primary.clone(), target.clone()];
    let mut policy =
        crate::DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 2)
            .unwrap();
    policy.required_node_ids = desired.clone();
    let accepted = native
        .apply_placement_policy(&primary, path, crate::DfsPlacementScope::SelfOnly, policy)
        .unwrap();
    let queue = crate::LocalDfsRepairQueueStore::new(
        crate::default_dfs_repair_attempt_snapshot_path(native.data_dir(&primary)),
    );
    let root =
        crate::default_dfs_metadata_shards_root(native.data_dir(&primary)).join(&shard.shard_id);
    // This is the existing routed-status end-to-end bound. Each normal native
    // owner invocation also keeps its existing network budget and retry policy.
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    let mut cuts = Vec::new();
    let mut previous_head = None;
    let mut completed = false;
    let mut failure = None;
    let mut restarted = false;
    native.checkpoint_rpc_observation_window().unwrap();
    while std::time::Instant::now() < deadline {
        for node in [&target, &primary] {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let observed = native
                .maintenance_tick(
                    node,
                    remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
                )
                .unwrap();
            native.checkpoint_rpc_observation_window().unwrap();
            assert_eq!(observed.omitted_kv_event_count, 0);
            if let Err(error) = observed.outcome {
                failure = Some(error);
                break;
            }
        }
        if failure.is_some() {
            break;
        }
        let cut = queue.view().unwrap();
        if previous_head != Some(cut.head_sequence) {
            previous_head = Some(cut.head_sequence);
            cuts.push(serde_json::json!({
                "head": cut.head_sequence,
                "records": cut.records.iter().filter(|record| record.object_id == stat.object_id).collect::<Vec<_>>(),
            }));
        }
        if restart_on_ready
            && !restarted
            && cut.records.iter().any(|record| {
                record.object_id == stat.object_id
                    && record.missing_replica_node_id == target
                    && !record.in_flight
                    && record.transfer_request_id.is_none()
                    && record.completed_transfer_request_id.is_none()
                    && record.transfer_delivery_state
                        == Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
            })
        {
            native.stop_node(&primary).unwrap();
            native.checkpoint_rpc_observation_window().unwrap();
            // A real process restart loses this rebuildable read cache. The
            // durable original queue and every operation identity stay intact.
            queue.reload_from_disk_on_next_access();
            let stopped = queue.view().unwrap();
            cuts.push(serde_json::json!({
                "phase": "stopped",
                "head": stopped.head_sequence,
                "records": stopped.records,
            }));
            drop(before_claim.take());
            native.reopen_node(primary.clone()).unwrap();
            native.checkpoint_rpc_observation_window().unwrap();
            restarted = true;
            continue;
        }
        if cut.records.iter().any(|record| {
            record.object_id == stat.object_id
                && record
                    .intent_id
                    .starts_with(&format!("placement:{}:", accepted.job_id))
                && record.transfer_request_id.is_none()
                && record.completed_transfer_request_id.is_none()
                && record.transfer_delivery_state.is_none()
        }) {
            failure =
                Some("fresh native placement lost its known pre-dispatch custody".to_string());
            break;
        }
        // Inspection reads actual materialized rows. It neither runs recovery
        // nor grants a replacement writer or marks an unavailable replica ready.
        let inspected = crate::LocalSobjMetadataBackend::open_for_inspection(
            DfsMetadataShardId::new(shard.shard_id.clone()),
            &root,
        )
        .unwrap();
        let receipts = inspected
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap();
        completed = desired.iter().all(|node| {
            receipts.iter().any(|receipt| {
                &receipt.replica_node_id == node
                    && receipt.receipt.file_system_id == stat.file_system_id
                    && receipt.receipt.object_id == stat.object_id
                    && receipt.receipt.committed_version == stat.version
                    && receipt.content_hash.as_ref() == stat.content_hash.as_ref()
            })
        });
        if completed {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    let reads = if completed {
        desired
            .iter()
            .map(|node| native.client(node).unwrap().posix_read_file(path).unwrap())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let omitted_rpcs = native.observation_dropped_count();
    eprintln!(
        "native placement fixture={name} job={} original_object={} version={} queue_cuts={}",
        accepted.job_id,
        stat.object_id,
        stat.version,
        serde_json::to_string(&cuts).unwrap()
    );
    drop(queue);
    drop(before_claim);
    native.checkpoint_rpc_observation_window().unwrap();
    let closed = native.close();
    eprintln!(
        "native placement fixture={name} completion={completed} failure={failure:?} drain_and_coverage={closed:?}"
    );
    closed.unwrap();
    assert_eq!(omitted_rpcs, 0);
    assert!(failure.is_none(), "ordinary native repair: {failure:?}");
    assert!(
        !restart_on_ready || restarted,
        "the negative claim boundary must retain actual ready work for restart"
    );
    assert!(
        completed,
        "the unchanged existing deadline expired without both actual desired receipts"
    );
    assert!(reads.iter().all(|read| read == &bytes));
}

#[test]
fn routed_authority_ingress_preserves_source_target_and_epoch_checks() {
    let path = "/services/native-routed-probe-identities";
    let (native, client, shard) = crate::test_support::current_native_metadata_service(
        "native-routed-probe-identities",
        3,
        path,
    )
    .unwrap();
    drop(client);
    let primary = shard.primary_node_id.clone();
    let replica = shard.replica_node_ids[0].clone();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let mut source = native.metadata_primary_service(path).unwrap();
    let file_system = native.file_system();
    let prepared = source
        .metadata_backend
        .as_mut()
        .unwrap()
        .prepare_operation(
            crate::DfsPreparedOperationPayload::for_metadata_effects(
                crate::DfsPreparedOperationScope {
                    file_system_id: shard.file_system_id.clone(),
                    shard_id: shard_id.clone(),
                    owner_epoch: shard.epoch,
                    operation_id: "native-routed-probe-identities".into(),
                },
                Vec::new(),
                Vec::new(),
                vec![crate::DfsPreparedMetadataEffect::PutFileSystem { file_system }],
                OffsetDateTime::now_utc(),
            )
            .unwrap(),
            None,
        )
        .unwrap();
    let backend = source.metadata_backend.as_ref().unwrap();
    let batch = backend
        .journal_batch_for_shard_since_limited(
            &shard_id,
            0,
            crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    assert_eq!(batch.next_after_sequence, prepared.prepare_sequence);
    let probe = crate::DfsMetadataAuthorityProbe {
        probe_id: "native-routed-probe-identities".into(),
        expected_shard: shard.clone(),
        operation: crate::DfsMetadataAuthorityOperation::PreparedOperation {
            operation_id: prepared.operation_id,
            file_system_id: prepared.file_system_id,
            shard_id: prepared.shard_id,
            owner_epoch: prepared.owner_epoch,
            prepare_sequence: prepared.prepare_sequence,
            payload_digest: prepared.payload_digest,
        },
        prepared_prefix: backend.metadata_prefix_proof(&shard_id).unwrap().unwrap(),
    };
    let confirmation_id =
        crate::service_replica::metadata_authority_probe_confirmation_id(&probe).unwrap();
    let batch = DfsMetadataJournalBatchCommand {
        request_id: "native-routed-probe-identities".into(),
        source_node_id: Some(primary.clone()),
        file_system_id: Some(shard.file_system_id.clone()),
        authority_epoch: Some(shard.epoch),
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: Some(probe.clone()),
        durable_replication: None,
        batch: crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(
            backend.as_ref(),
            &batch,
        )
        .unwrap(),
    };
    let command = DfsMessageCommand {
        request_id: "native-routed-probe-identities".into(),
        routed_message: crate::routes::build_dfs_routed_message(
            &primary,
            &replica,
            1,
            1,
            DfsNetworkMessage::MetadataJournalBatch(batch),
        ),
    };
    let call = |command: &DfsMessageCommand| {
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
            crate::DFS_RPC_CATEGORY,
            command,
        )
        .unwrap();
        native.rpc_request(
            &primary,
            &replica,
            request,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
    };
    for variant in 0..4 {
        let mut invalid = command.clone();
        match variant {
            0 => invalid.routed_message.source_node_id = replica.clone(),
            1 => invalid.routed_message.target_node_id = primary.clone(),
            _ => {
                let DfsNetworkMessage::MetadataJournalBatch(batch) =
                    &mut invalid.routed_message.message
                else {
                    unreachable!()
                };
                if variant == 2 {
                    batch.source_node_id = Some(replica.clone());
                } else {
                    batch.authority_epoch = Some(shard.epoch.saturating_sub(1));
                }
            }
        }
        if let Ok(response) = call(&invalid) {
            let response: crate::DfsMessageResponse = response.decode_body().unwrap();
            assert_eq!(response.outbound_messages.len(), 1);
            assert!(
                matches!(&response.outbound_messages[0].message,
                DfsNetworkMessage::MetadataJournalBatchResponse(response)
                if matches!(response.result, DfsMetadataJournalBatchResult::Rejected { .. })
                && response.applied_entries == 0 && response.verified_authority_probe_id.is_none()),
                "invalid identity variant {variant}: {response:?}"
            );
        }
    }
    let response: crate::DfsMessageResponse = call(&command).unwrap().decode_body().unwrap();
    assert_eq!(response.request_id, command.request_id);
    assert_eq!(response.outbound_messages.len(), 1);
    let returned = &response.outbound_messages[0];
    assert_eq!(returned.source_node_id, replica);
    assert_eq!(returned.target_node_id, primary);
    assert!(
        matches!(&returned.message,
        DfsNetworkMessage::MetadataJournalBatchResponse(response)
        if response.result == DfsMetadataJournalBatchResult::Applied
        && response.applied_entries > 0
        && response.verified_authority_probe_id.as_ref() == Some(&confirmation_id)),
        "only the actual authenticated preparation may be acknowledged: {returned:?}"
    );
    drop(source);
    native.close().unwrap();
}

#[test]
fn routed_authority_probe_does_not_wait_for_general_request_view() {
    let path = "/services/native-routed-authority-ingress";
    let (native, mut client, shard) = crate::test_support::current_native_metadata_service(
        "native-routed-authority-ingress",
        3,
        path,
    )
    .unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![shard.primary_node_id.clone()]);
    client
        .posix_write_file(path, b"original", options.clone())
        .unwrap();
    client
        .posix_sync_file(path, DfsPosixSyncKind::File)
        .unwrap();
    let replica = shard.replica_node_ids[0].clone();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let reached = Arc::new(AtomicUsize::new(0));
    let held = Arc::clone(&gate);
    let observed = Arc::clone(&reached);
    let hook: Arc<DaemonDfsPreDispatchTestHook> = Arc::new(move |point, command, _| {
        if point != DaemonDfsPreDispatchTestPoint::BeforePreflightFence {
            return Ok(());
        }
        let Some(command) = command else {
            return Ok(());
        };
        let DfsNetworkMessage::MetadataJournalBatch(batch) = &command.routed_message.message else {
            return Ok(());
        };
        if !batch.authority_probe.as_ref().is_some_and(|probe| {
            matches!(&probe.operation, crate::DfsMetadataAuthorityOperation::PreparedOperation {
                operation_id, ..
            } if operation_id.starts_with("write:"))
        }) {
            return Ok(());
        }
        observed.fetch_add(1, Ordering::SeqCst);
        let (lock, changed) = &*held;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
        Ok(())
    });
    let hook_guard = install_daemon_dfs_pre_dispatch_test_hook(native.data_dir(&replica), hook);
    native.install_fault(FaultRule {
        source: Some(shard.primary_node_id.clone()),
        target: Some(replica.clone()),
        route: Some(crate::DFS_METADATA_JOURNAL_BATCH_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: FaultAction::BudgetExhausted,
    });
    // The ordinary direct accelerator is unavailable. The SAME preparation
    // must reach the real replica through routed fallback within its existing
    // deadline, without constructing an unrelated mutable request view.
    let result = client.posix_write_file_with_operation_id(
        path,
        b"successor",
        options,
        "native-routed-authority-ingress-successor",
    );
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    drop(hook_guard);
    native.clear_faults();
    let data = result
        .as_ref()
        .ok()
        .map(|_| client.posix_read_file_distributed(path));
    let generic_view_entries = reached.load(Ordering::SeqCst);
    let calls = native.rpc_observations();
    let routed_calls = calls
        .iter()
        .filter(|call| {
            call.source == shard.primary_node_id
                && call.target == replica
                && call.phase == RpcPhase::BeforeDispatch
                && call.route == crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE
        })
        .count();
    drop(client);
    // The old failed request is released and drained before assertions.
    native.close().unwrap();
    assert!(result.is_ok(), "actual routed quorum failed: {result:?}");
    assert_eq!(data.unwrap().unwrap(), b"successor");
    assert!(
        routed_calls > 0,
        "the control must use actual routed fallback"
    );
    assert_eq!(
        generic_view_entries, 0,
        "replica proof must not depend on the general request-view constructor"
    );
}

#[test]
fn required_blob_fetch_progresses_while_advisory_maintenance_is_parked() {
    let path = "/services/native-required-fetch";
    let (native, mut client, shard) =
        crate::test_support::current_native_metadata_service("native-required-fetch", 2, path)
            .unwrap();
    let primary = shard.primary_node_id.clone();
    let source = shard.replica_node_ids[0].clone();
    let bytes = b"real durable bytes for the protocol fetch";
    let stat = client
        .posix_write_file(
            path,
            bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: vec![primary.clone(), source.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    client
        .posix_sync_file(path, DfsPosixSyncKind::File)
        .unwrap();
    drop(client);
    let mut owner = native.metadata_primary_service(path).unwrap();
    let object = owner
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    let intent = owner
        .metadata_backend()
        .unwrap()
        .intents_for_object(&stat.object_id)
        .unwrap()
        .into_iter()
        .find(|row| {
            row.target_version == stat.version
                && row.status == crate::DfsWriteIntentStatus::Committed
        })
        .unwrap();
    let fetch = crate::routes::build_dfs_routed_message(
        &primary,
        &source,
        1,
        1,
        DfsNetworkMessage::FetchBlob(crate::DfsFetchBlobCommand {
            request_id: "native-required-fetch-request".into(),
            intent_id: intent.intent_id.clone(),
            object_id: stat.object_id.clone(),
            version: stat.version.clone(),
            content_hash: stat.content_hash.clone().unwrap(),
            authority_shard: Some(shard.clone()),
        }),
    );
    let promotion = crate::routes::build_dfs_routed_message(
        &primary,
        &source,
        2,
        2,
        DfsNetworkMessage::CommitVersion(crate::DfsCommitVersionCommand {
            request_id: "native-advisory-parked-promotion".into(),
            intent_id: intent.intent_id,
            file_system_id: stat.file_system_id.clone(),
            object_id: stat.object_id.clone(),
            expected_base_version: intent.base_version,
            version: stat.version.clone(),
            content_hash: stat.content_hash.clone().unwrap(),
            object_metadata: Some(object),
            authority_shard: Some(shard),
        }),
    );
    owner
        .transport
        .send_detached_routed_message(&source, promotion)
        .unwrap();
    owner
        .transport
        .send_detached_routed_message(&source, fetch)
        .unwrap();
    let mut worker = owner.prepare_message_deferred_work();
    let replies = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&replies);
    worker.set_background_response_handler(move |message, _| {
        observed.lock().unwrap().push(message);
        Ok(())
    });
    // This is the observed NEGATIVE advisory hint, not fabricated availability
    // or authority. Every successful reply still comes from real native ingress.
    worker.set_deferred_target_node_ids(BTreeSet::from([source.clone()]));
    let ready = worker.scheduling_snapshot().unwrap().0;
    let before = native.rpc_observations().len();
    worker.drain_exclusive().unwrap();
    let before_policy_release = native.rpc_observations()[before..].to_vec();
    let fetched = replies.lock().unwrap().clone();
    let parked = worker.has_pending().unwrap();
    worker.set_deferred_target_node_ids(BTreeSet::new());
    worker.drain_exclusive().unwrap();
    drop(worker);
    drop(owner);
    native.close().unwrap();
    assert!(
        ready,
        "required protocol work must be eligible despite a maintenance hint"
    );
    assert!(parked, "optional post-commit promotion must remain parked");
    assert!(before_policy_release.iter().any(|call| {
        call.source == primary
            && call.target == source
            && call.route == crate::DFS_DATA_FETCH_BLOB_ROUTE
            && call.phase == RpcPhase::Completed
    }));
    assert!(!before_policy_release.iter().any(|call| {
        call.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE && call.phase == RpcPhase::BeforeDispatch
    }));
    assert!(fetched.iter().any(|message| matches!(&message.message,
        DfsNetworkMessage::FetchBlobResponse(response)
        if response.object_id == stat.object_id && response.version == stat.version
        && matches!(&response.result, crate::DfsFetchBlobResult::Found {
            content_hash, bytes: actual
        } if Some(content_hash) == stat.content_hash.as_ref() && actual == bytes))));
}
