mod dispatch_isolation;
mod observed_receipt_gap;

use super::*;

type ActualRepairReply = (
    crate::test_runtime::DfsTestRuntime,
    NodeId,
    NodeId,
    NetworkedDfsService<DfsNodeTransport>,
    DfsStat,
    String,
    crate::repair_queue_store::RepairOriginalBirth,
    DfsRepairReplicaResponse,
    crate::publication_guard::DfsPublicationContextRestore,
    crate::runtime_host::DaemonDfsPreDispatchTestHookGuard,
);

fn actual_repair_reply(name: &str, node_count: usize) -> ActualRepairReply {
    actual_repair_reply_delivery(name, node_count, false)
}

fn actual_repair_reply_delivery(
    name: &str,
    node_count: usize,
    lose_reply: bool,
) -> ActualRepairReply {
    let (runtime, owner, replica, mut core, stat, intent_id, background) =
        native_committed_repair_backlog_with_nodes(name, true, node_count);
    let (execution, hold) = native_private_repair_owner(&runtime, &core, &stat.path, background);
    runtime.clear_faults();
    let pending = core.writes.pending_writes[&intent_id].clone();
    let request_id = core
        .original_repair_request_for_target(&intent_id, &replica, &pending)
        .unwrap()
        .expect("the real producer still owns its open or first Original");
    let intent = core.coordinator().intent(&intent_id).unwrap().clone();
    let object = core.current_read_object_metadata(&stat.object_id).unwrap();
    let prepared = core
        .prepare_current_background_repair_replica_sends(
            &[(replica.clone(), request_id.clone())],
            &intent,
            &pending,
            object,
        )
        .unwrap();
    let lost_replies = Arc::new(Mutex::new(Vec::<DfsRoutedMessage>::new()));
    if lose_reply {
        runtime.install_fault(crate::test_runtime::FaultRule {
            source: Some(owner.clone()),
            target: Some(replica.clone()),
            route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 1,
            action: crate::test_runtime::FaultAction::CaptureTransferReplyAndLoseAfterDispatch {
                intent_id: Some(intent_id.clone()),
                captured: Arc::clone(&lost_replies),
            },
        });
    }
    let outcomes = crate::test_runtime::executor()
        .block_on(core.dispatch_prepared_repair_replica_sends(prepared, false))
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    let returned = if lose_reply {
        assert_eq!(
            outcomes[0].delivery_state,
            Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
        );
        assert!(
            core.transport.drain_incoming_messages().unwrap().iter().all(|message|
                !matches!(&message.message, DfsNetworkMessage::RepairReplicaResponse(reply)
                    if reply.request_id == request_id && reply.intent_id == intent_id)),
            "the lost reply cannot also be delivered to its sender"
        );
        let captured = lost_replies.lock().unwrap();
        assert_eq!(
            captured.len(),
            1,
            "one actual completed receiver reply was lost"
        );
        captured.clone()
    } else {
        assert!(outcomes[0].sent, "the actual native receiver must return");
        core.transport.drain_incoming_messages().unwrap()
    };
    runtime.clear_faults();
    let reply = returned
        .into_iter()
        .find_map(|message| match message.message {
            DfsNetworkMessage::RepairReplicaResponse(reply)
                if reply.request_id == request_id && reply.intent_id == intent_id =>
            {
                assert_eq!(message.source_node_id, replica);
                assert_eq!(message.target_node_id, owner);
                Some(reply)
            }
            _ => None,
        })
        .expect("the encoded native repair returned its own correlated response");
    assert!(
        matches!(&reply.result, DfsBlobTransferResult::Committed { content_hash }
        if Some(content_hash) == stat.content_hash.as_ref())
    );
    let queue = core.owned_repair_queue_store().unwrap().unwrap();
    let birth = queue
        .original_repair_for_slot(&intent_id, &replica)
        .unwrap()
        .unwrap();
    assert_eq!(birth.original.source_node_id, owner);
    assert_eq!(birth.original.target_node_id, replica);
    assert!(birth.observed.is_empty() && birth.closed_at_sequence.is_none());
    let DfsNetworkMessage::RepairReplica(command) = &birth.original.message else {
        unreachable!("actual Original is a Repair");
    };
    assert!(
        core.metadata_backend()
            .unwrap()
            .get_receipt_on_shard(
                &DfsMetadataShardId::new(
                    command.authority_shard.as_ref().unwrap().shard_id.clone()
                ),
                &stat.object_id,
                &stat.version,
                &replica,
            )
            .unwrap()
            .is_none()
    );
    checkpoint_actual_native_custody(&runtime, &owner, &core.durable_repair_queue_records());
    (
        runtime, owner, replica, core, stat, intent_id, birth, reply, execution, hold,
    )
}

fn receipt_prepare(
    core: &NetworkedDfsService<DfsNodeTransport>,
    response: &DfsRepairReplicaResponse,
) -> Option<DfsPreparedOperationRecord> {
    core.metadata_backend()
        .unwrap()
        .list_prepared_operations()
        .unwrap()
        .into_iter()
        .find(|record| {
            record.payload.metadata_effects.iter().any(|effect| {
                matches!(effect, DfsPreparedMetadataEffect::PutReceipt { receipt }
                    if receipt.receipt.file_system_id == response.file_system_id
                        && receipt.receipt.object_id == response.object_id
                        && receipt.receipt.committed_version == response.version
                        && receipt.replica_node_id == response.replica_node_id)
            })
        })
}

#[test]
fn native_observed_repair_recovers_only_its_hidden_receipt_publication() {
    let (mut runtime, owner, replica, mut core, stat, intent_id, birth, reply, execution, hold) =
        actual_repair_reply("native-original-receipt-prepare-recovery", 2);
    runtime.stop_node(&replica).unwrap();
    assert!(
        core.handle_repair_replica_response(replica.clone(), reply.clone())
            .is_err()
    );
    let original = receipt_prepare(&core, &reply)
        .expect("failed native confirmation must retain its actual hidden receipt body");
    let queue = core.owned_repair_queue_store().unwrap().unwrap();
    let observed = queue
        .original_repair_for_slot(&intent_id, &replica)
        .unwrap()
        .unwrap();
    assert_eq!(observed.observed, vec![reply.clone()]);
    assert!(observed.closed_at_sequence.is_none());
    let before = runtime.rpc_observations().len();
    runtime.reopen_node(replica.clone()).unwrap();
    let pending = core.writes.pending_writes[&intent_id].clone();
    assert!(
        core.original_repair_request_for_target(&intent_id, &replica, &pending)
            .unwrap()
            .is_none()
    );
    let committed = core
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&original.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(committed.status, DfsPreparedOperationStatus::Committed);
    assert_eq!(committed.digest, original.digest);
    assert_eq!(committed.payload, original.payload);
    let closed = queue.original_repair(&birth.original).unwrap().unwrap();
    assert!(closed.closed_at_sequence.is_some());
    assert_eq!(closed.original, birth.original);
    assert!(
        core.current_read_object_metadata(&stat.object_id)
            .unwrap()
            .current_locations
            .contains(&replica)
    );
    assert!(
        !runtime.rpc_observations()[before..].iter().any(|event| {
            event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
        }),
        "recovery confirms the original metadata body without retransferring content"
    );
    assert_eq!(runtime.observation_dropped_count(), 0);
    drop(execution);
    drop(core);
    drop(hold);
    runtime.close().unwrap();
}

#[test]
fn native_observed_repair_without_a_prepare_cannot_author_receipt_metadata() {
    let (runtime, owner, replica, mut core, stat, intent_id, birth, reply, execution, hold) =
        actual_repair_reply("native-original-receipt-no-prepare", 2);
    let queue = core.owned_repair_queue_store().unwrap().unwrap();
    queue
        .observe_original_repair_result(&owner, &reply)
        .unwrap()
        .unwrap();
    assert!(receipt_prepare(&core, &reply).is_none());
    let shard = DfsMetadataShardId::new("services");
    let before = core
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard)
        .unwrap();
    let pending = core.writes.pending_writes[&intent_id].clone();
    assert!(
        core.original_repair_request_for_target(&intent_id, &replica, &pending)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        core.metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&shard)
            .unwrap(),
        before
    );
    assert!(
        core.backend_receipt_for_replica(&stat.object_id, &stat.version, &replica)
            .unwrap()
            .is_none()
    );
    let retained = queue.original_repair(&birth.original).unwrap().unwrap();
    assert_eq!(retained.observed, vec![reply]);
    assert!(retained.closed_at_sequence.is_none());
    drop(execution);
    drop(core);
    drop(hold);
    runtime.close().unwrap();
}

#[test]
fn native_old_repair_receipt_prepare_cannot_undo_a_later_withdrawal() {
    let (runtime, _owner, replica, mut core, stat, _intent_id, birth, reply, execution, hold) =
        actual_repair_reply("native-original-receipt-later-withdrawal", 2);
    core.handle_repair_replica_response(replica.clone(), reply)
        .unwrap();
    let object = core.current_read_object_metadata(&stat.object_id).unwrap();
    assert!(object.current_locations.contains(&replica));
    let remote = runtime.client(&replica).unwrap();
    remote
        .content()
        .debug_remove_loose_blob(stat.content_hash.as_deref().unwrap())
        .unwrap();
    assert!(
        remote
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .is_err()
    );
    drop(remote);
    // The normal missing-content path captures its actual coherent prefix and
    // exact location-withdrawal preconditions under the native writer.
    core.mark_remote_read_location_missing(
        &stat.object_id,
        &stat.version,
        &stat.content_hash,
        &replica,
    )
    .unwrap();
    let shard = DfsMetadataShardId::new("services");
    let before = core
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard)
        .unwrap();
    assert!(
        !core
            .recover_original_repair_receipt_prepare(&birth)
            .unwrap()
    );
    assert_eq!(
        core.metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&shard)
            .unwrap(),
        before
    );
    assert!(
        core.backend_receipt_for_replica(&stat.object_id, &stat.version, &replica)
            .unwrap()
            .is_none()
    );
    assert!(
        !core
            .current_read_object_metadata(&stat.object_id)
            .unwrap()
            .current_locations
            .contains(&replica)
    );
    drop(execution);
    drop(core);
    drop(hold);
    runtime.close().unwrap();
}

#[test]
fn native_handoff_uncovered_original_parks_only_its_target() {
    let (runtime, owner, replica, core, stat, intent_id, birth, _reply, execution, old_hold) =
        actual_repair_reply_delivery("native-handoff-uncovered-original-isolation", 3, true);
    let successor = crate::test_runtime::DfsTestRuntime::ordered_node_ids(
        "native-handoff-uncovered-original-isolation",
        3,
    )
    .into_iter()
    .find(|node| node != &owner && node != &replica)
    .unwrap();
    let successor_hold = runtime
        .deny_background_replica_before_claim(&successor)
        .unwrap();
    let original_queue = core.owned_repair_queue_store().unwrap().unwrap();
    let original_row = original_queue
        .view()
        .unwrap()
        .records
        .into_iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap();
    assert_eq!(
        original_row.transfer_request_id.as_deref(),
        Some(birth.root.subject.request_id.as_str())
    );
    assert_eq!(
        original_row.transfer_delivery_state,
        Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
    );
    // Replace the broad setup hold while the actual shard admission still
    // excludes Main. Reconciliation can run, but it cannot claim and settle
    // the genuinely unanswered repair before the handoff captures its queue.
    drop(old_hold);
    let old_claim_hold = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    // The reply remains withheld from its old metadata owner. Export exactly
    // this real pending producer through the ordinary joint handoff.
    drop(execution);
    drop(core);
    let actual = runtime
        .reassign_metadata_shard(
            &stat.file_system_id,
            "services",
            successor.clone(),
            vec![owner.clone(), replica.clone()],
        )
        .unwrap();
    assert_eq!(actual.primary_node_id, successor);
    // Configuration activation stages custody. The ordinary current-primary
    // opener validates its completion proof and installs the staged page while
    // rebinding its actual native writer before the queue can be inspected.
    let activated_owner = runtime.metadata_primary_service(&stat.path).unwrap();
    assert_eq!(activated_owner.node_id(), &successor);
    drop(activated_owner);
    let queue = crate::LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
        runtime.data_dir(&successor),
    ));
    let inherited = queue
        .view()
        .unwrap()
        .records
        .into_iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .expect("the actual handoff retained the original retry DATA");
    assert_eq!(
        inherited.transfer_request_id,
        original_row.transfer_request_id
    );
    assert_eq!(
        inherited.transfer_delivery_state,
        original_row.transfer_delivery_state
    );
    assert!(
        queue
            .original_repair_for_slot(&intent_id, &replica)
            .unwrap()
            .is_none()
    );
    assert!(
        queue
            .legacy_repair_is_uncovered(&intent_id, &replica)
            .unwrap()
    );

    for route in [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE] {
        runtime.install_fault(crate::test_runtime::FaultRule {
            source: Some(successor.clone()),
            target: Some(replica.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    }
    let path = "/services/native-handoff-unrelated-ready.bin";
    let mut client = runtime.client(&successor).unwrap();
    let healthy = client
        .posix_write_file(
            path,
            b"unrelated current owner work",
            DfsWriteOptions {
                replica_candidates: vec![successor.clone(), replica.clone()],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(client);
    let mut core = runtime.metadata_primary_service(path).unwrap();
    let (execution, successor_hold) =
        native_private_repair_owner(&runtime, &core, path, successor_hold);
    runtime.clear_faults();
    let captured = queue.view().unwrap().records;
    core.retry_pending_repairs_bounded_with_attempts(0, &captured)
        .unwrap();
    let pending = core.writes.pending_writes[&intent_id].clone();
    let before = queue
        .view()
        .unwrap()
        .records
        .into_iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap();
    assert!(
        core.original_repair_request_for_target(&intent_id, &replica, &pending)
            .unwrap()
            .is_none(),
        "a named handoff retry cannot invent a new Original or abort the shared turn"
    );
    let deadline = Instant::now() + crate::DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
    loop {
        assert!(
            Instant::now() < deadline,
            "unrelated repair did not finish within its normal sync budget"
        );
        core.retry_pending_repairs_bounded(32)
            .expect("the inherited unknown target cannot abort other repairs");
        let incoming = core.transport.drain_incoming_messages().unwrap();
        core.process_routed_messages_with_options(incoming, true)
            .unwrap();
        if core
            .backend_receipt_for_replica(&healthy.object_id, &healthy.version, &replica)
            .unwrap()
            .is_some()
        {
            break;
        }
        std::thread::yield_now();
    }
    let after = queue
        .view()
        .unwrap()
        .records
        .into_iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap();
    assert_eq!(
        after, before,
        "the uncertain handoff record remains byte-for-byte scheduling DATA"
    );
    assert_eq!(
        original_queue
            .original_repair(&birth.original)
            .unwrap()
            .unwrap()
            .original,
        birth.original
    );
    assert!(runtime.rpc_observations().iter().any(|event| {
        event.source == successor
            && event.target == replica
            && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
            && event.dfs_object_id.as_ref() == Some(&healthy.object_id)
            && event.phase == crate::test_runtime::RpcPhase::Completed
    }));
    assert!(
        !runtime.rpc_observations().iter().any(|event| {
            event.source == successor
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.dfs_intent_id.as_deref() == Some(intent_id.as_str())
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
        }),
        "the successor never replays the historical repair"
    );
    assert_eq!(runtime.observation_dropped_count(), 0);
    drop(execution);
    drop(core);
    drop(successor_hold);
    drop(old_claim_hold);
    runtime.close().unwrap();
}
