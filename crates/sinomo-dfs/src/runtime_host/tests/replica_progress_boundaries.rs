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
fn routed_authority_ingress_preserves_source_target_and_epoch_checks() {
    native_authority_ingress(false);
}

#[test]
fn native_replica_answer_progresses_behind_queued_materialization() {
    native_authority_ingress(true);
}

fn native_authority_ingress(queued_materialization: bool) {
    let path = "/services/native-routed-probe-identities";
    let (native, client, shard) = crate::test_support::current_native_metadata_service(
        if queued_materialization {
            "native-replica-queued-materialization"
        } else {
            "native-routed-probe-identities"
        },
        3,
        path,
    )
    .unwrap();
    drop(client);
    let primary = shard.primary_node_id.clone();
    let replica = shard.replica_node_ids[0].clone();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let receiver_head = native.node_metadata(&replica).unwrap()
        .metadata_prefix_proof(&shard_id).unwrap().unwrap().journal_sequence;
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
            receiver_head,
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
    let response: crate::DfsMessageResponse = if queued_materialization {
        let scheduler = daemon_dfs_mutation_scheduler(native.data_dir(&replica)).unwrap();
        let other = native
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|other| other.shard_id != shard.shard_id)
            .unwrap();
        let other_scope = DfsMessageExecutionScope::ExactShard {
            file_system_id: other.file_system_id,
            shard_id: DfsMetadataShardId::new(other.shard_id),
        };
        let replica_scope = dfs_message_execution_scope(&command);
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
            crate::DFS_RPC_CATEGORY,
            &command,
        )
        .unwrap();
        let answer = native.rpc_result_future(
            &primary,
            &replica,
            sinomo_core::network::ObjectRpcCall::value(request),
            Duration::from_secs(3),
        );
        native.runtime_handle().block_on(async {
            // Hold real native admission on an unrelated shard. Both queued
            // futures are polled to their actual wait, without timing sleeps.
            let holding = scheduler.admit(&other_scope).await.unwrap();
            let mut global = Box::pin(scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive));
            assert!(
                poll_queued_admission_once(global.as_mut())
                    .await
                    .is_pending()
            );
            let mut materialization = Box::pin(scheduler.admit(&replica_scope));
            assert!(
                poll_queued_admission_once(materialization.as_mut())
                    .await
                    .is_pending()
            );
            let result = tokio::time::timeout(Duration::from_secs(2), answer).await;
            drop(materialization);
            drop(global);
            drop(holding);
            result
                .expect("real authority ACK must pass queued materialization")
                .unwrap()
                .into_materialized_value_response(64 * 1024)
                .await
                .unwrap()
                .decode_body()
                .unwrap()
        })
    } else {
        call(&command).unwrap().decode_body().unwrap()
    };
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
fn native_content_stage_answers_while_an_unrelated_shard_publishes() {
    let path = "/services/native-required-content-stage";
    let (native, mut client, shard) = crate::test_support::current_native_metadata_service(
        "native-required-content-stage",
        3,
        path,
    )
    .unwrap();
    let primary = shard.primary_node_id.clone();
    let replica = shard.replica_node_ids[0].clone();
    let bytes = b"native content and original durable intent";
    let stat = client
        .posix_write_file(
            path,
            bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 3).unwrap(),
                replica_candidates: std::iter::once(primary.clone())
                    .chain(shard.replica_node_ids.iter().cloned())
                    .collect(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    client
        .posix_sync_file(path, DfsPosixSyncKind::File)
        .unwrap();
    drop(client);
    let owner = native.metadata_primary_service(path).unwrap();
    let backend = owner.metadata_backend().unwrap();
    let object = backend.get_object(&stat.object_id).unwrap().unwrap();
    let intent = backend
        .intents_for_object(&stat.object_id)
        .unwrap()
        .into_iter()
        .find(|row| {
            row.target_version == stat.version
                && row.status == crate::DfsWriteIntentStatus::Committed
        })
        .unwrap();
    let stage = crate::DfsStageBlobCommand {
        request_id: "native-required-content-stage-replay".into(),
        intent_id: intent.intent_id.clone(),
        file_system_id: stat.file_system_id.clone(),
        object_id: stat.object_id.clone(),
        expected_base_version: intent.base_version,
        version: stat.version.clone(),
        content_hash: stat.content_hash.clone().unwrap(),
        bytes: bytes.to_vec(),
        object_metadata: Some(object),
        authority_shard: Some(shard.clone()),
        receipt_generation: 0,
    };
    drop(owner);
    let routed = crate::routes::build_dfs_routed_message(
        &primary,
        &replica,
        1,
        1,
        DfsNetworkMessage::StageBlob(stage.clone()),
    );
    let command = crate::DfsMessageCommand {
        request_id: stage.request_id.clone(),
        routed_message: routed,
    };
    let request = sinomo_core::network::ObjectRpcRequest::encode_body(
        crate::DFS_DATA_STAGE_BLOB_ROUTE,
        crate::DFS_RPC_CATEGORY,
        &command,
    )
    .unwrap();
    let scheduler = daemon_dfs_mutation_scheduler(native.data_dir(&replica)).unwrap();
    let cached = daemon_dfs_message_service_cache()
        .existing_root(&native.data_dir(&replica).root())
        .unwrap()
        .unwrap();
    let original_owner = cached
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .primary_service
        .as_ref()
        .unwrap()
        .clone();
    let other = native
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|other| other.shard_id == "users")
        .unwrap();
    let scope = DfsMessageExecutionScope::ExactShard {
        file_system_id: other.file_system_id,
        shard_id: DfsMetadataShardId::new(other.shard_id),
    };
    let answer = native.rpc_result_future(
        &primary,
        &replica,
        sinomo_core::network::ObjectRpcCall::value(request),
        Duration::from_secs(3),
    );
    let result = native.runtime_handle().block_on(async {
        let placement = scheduler.admit(&scope).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), answer).await;
        drop(placement);
        match result {
            Ok(Ok(reply)) => Some(
                reply
                    .into_materialized_value_response(64 * 1024)
                    .await
                    .unwrap()
                    .decode_body::<crate::DfsMessageResponse>()
                    .unwrap(),
            ),
            _ => None,
        }
    });
    let response = result.expect("durable stage answer must pass unrelated placement admission");
    let current_owner = cached
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .primary_service
        .as_ref()
        .unwrap()
        .clone();
    assert!(
        Arc::ptr_eq(&original_owner, &current_owner),
        "same transfer-state owner"
    );
    {
        let lane = current_owner.lock().unwrap();
        let active = lane
            .service
            .writes
            .active_replica_stages
            .get(&stage.object_id)
            .unwrap();
        assert_eq!(active.intent_id, stage.intent_id);
        assert_eq!(active.content_hash, stage.content_hash);
        assert!(
            lane.service.metadata_backend.is_none(),
            "scoped checkout returned"
        );
        assert!(matches!(
            lane.resident_metadata_scope,
            Some(DaemonDfsResidentMetadataScope::Global(_))
        ));
    }
    drop(original_owner);
    drop(current_owner);
    drop(cached);
    native.close().unwrap();
    assert_eq!(response.request_id, command.request_id);
    assert!(response.outbound_messages.iter().any(|message| matches!(
        &message.message, DfsNetworkMessage::StageBlobResponse(reply)
            if reply.intent_id == stage.intent_id && reply.object_id == stage.object_id
                && reply.version == stage.version && reply.replica_node_id == replica
                && matches!(&reply.result, crate::DfsBlobTransferResult::Durable { content_hash }
                    if content_hash == &stage.content_hash)
    )));
}

#[test]
fn native_first_write_stages_during_unrelated_publication() {
    let path = "/services/native-first-stage-progress";
    let (native, mut client, shard) = crate::test_support::current_native_metadata_service(
        "native-first-stage-progress",
        3,
        path,
    )
    .unwrap();
    let replica = shard.replica_node_ids[0].clone();
    let scheduler = daemon_dfs_mutation_scheduler(native.data_dir(&replica)).unwrap();
    let other = native
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|other| other.shard_id == "users")
        .unwrap();
    let scope = DfsMessageExecutionScope::ExactShard {
        file_system_id: other.file_system_id,
        shard_id: DfsMetadataShardId::new(other.shard_id),
    };
    let placement = native
        .runtime_handle()
        .block_on(scheduler.admit(&scope))
        .unwrap();
    let (sent, received) = std::sync::mpsc::sync_channel(1);
    let (completed, (mut client, result)) = std::thread::scope(|threads| {
        let writer = threads.spawn(move || {
            let result = client.posix_write_file_with_operation_id(
                path,
                b"first version with its real nonterminal stage intent",
                DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(3, 3).unwrap(),
                    replica_candidates: std::iter::once(shard.primary_node_id)
                        .chain(shard.replica_node_ids)
                        .collect(),
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
                "native-first-stage-progress-operation",
            );
            let _ = sent.send(());
            (client, result)
        });
        let completed = received.recv_timeout(Duration::from_secs(5)).is_ok();
        if !completed {
            let observations = native.rpc_observations();
            eprintln!(
                "first-stage waiting RPCs: {:#?}",
                &observations[observations.len().saturating_sub(12)..]
            );
        }
        drop(placement);
        (completed, writer.join().unwrap())
    });
    let bytes = result
        .as_ref()
        .ok()
        .map(|_| client.posix_read_file_distributed(path));
    // The cold stage initialized one shard only. A real later global owner
    // must finish its ordinary hydration before borrowing general state.
    crate::runtime_host::message_host::with_daemon_dfs_main_maintenance_service(
        native.data_dir(&replica),
        &native.runtime_handle(),
        |service| {
            assert!(
                service
                    .metadata_backend
                    .as_ref()
                    .unwrap()
                    .resident_exact_shard_id()
                    .is_none()
            );
        },
    )
    .unwrap();
    let cached = daemon_dfs_message_service_cache()
        .existing_root(&native.data_dir(&replica).root())
        .unwrap()
        .unwrap();
    let global = cached
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .primary_service
        .clone()
        .unwrap();
    assert!(!global.lock().unwrap().needs_global_hydration);
    drop(global);
    drop(cached);
    drop(client);
    native.close().unwrap();
    assert!(
        completed,
        "actual first write must finish before unrelated admission is released"
    );
    assert!(
        result.is_ok(),
        "actual producer and original operation: {result:?}"
    );
    assert_eq!(
        bytes.unwrap().unwrap(),
        b"first version with its real nonterminal stage intent"
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
