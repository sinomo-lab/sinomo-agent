use super::*;
use crate::runtime_host::*;
use crate::{DfsControlPlaneStore, DfsPlacementPolicy, DfsPlacementScope, DfsWriteOptions};
use crate::service_write::{DfsDistributedWriteOptions, DfsWriteRepair};
use sinomo_core::DfsObjectPolicy;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy, Debug)]
enum Negative { None, TargetError, Authority, FetchRequest }

fn queue_snapshot(service: &NetworkedDfsService<DfsNodeTransport>) -> serde_json::Value {
    serde_json::json!({
        "owned": service.transport.incoming_messages,
        "served": service.transport.served_message.as_ref().map(|state| &state.inbound_messages),
        "inbox": service.transport.response_inbox.as_ref().map(|owner| owner.inbox_identity_for_test().to_string()),
    })
}

fn finish_apply(native: &crate::test_runtime::DfsTestRuntime,
    accepted: &crate::DfsPlacementReconciliationJob) {
    let control = native.shared_control_plane().unwrap();
    // The actual filesystem root primary advances accepted jobs; object-side
    // effects are routed to their path primaries by that normal coordinator.
    let coordinator = native.root_shard().unwrap();
    assert_eq!(coordinator.file_system_id, accepted.file_system_id);
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let current = control.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
            .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)).unwrap().unwrap();
        assert_eq!(current.accepted_config, accepted.accepted_config);
        assert_eq!(current.candidate_snapshot, accepted.candidate_snapshot);
        if current.state == crate::DfsPlacementReconciliationState::Completed { break; }
        assert!(Instant::now() < deadline, "ordinary fixture Apply remains {current:?}");
        let tick = native.maintenance_tick(&coordinator.primary_node_id,
            deadline.saturating_duration_since(Instant::now())).unwrap();
        assert!(tick.outcome.is_ok(), "ordinary fixture Apply: {tick:?}");
    }
}

// Main serves committed setup repairs promptly. Finish that work, and the
// maintenance turns carrying it, before the cut: the Stage's own committed
// wake then starts its automatic Routine from a completed turn.
fn settle_maintenance(native: &crate::test_runtime::DfsTestRuntime, nodes: &[&sinomo_core::node::NodeId]) {
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    for node in nodes {
        loop {
            let budget = deadline.saturating_duration_since(Instant::now());
            assert!(!budget.is_zero(), "setup maintenance on {node} did not settle");
            let tick = native.maintenance_tick(node, budget).unwrap();
            if matches!(tick.outcome, Ok(crate::DfsMaintenanceTickOutcome::Completed { reached_limit: false }))
                && matches!(&tick.queue_after,
                    Ok(crate::DfsRepairQueueBoundedStatus::Hydrated(view)) if view.record_count == 0) { break; }
        }
    }
}

// One genuine Fetch response is produced by the SAME cached Main lane before
// an unrelated Root Stage arrives. No response, prepare or queue row is seeded.
fn native_stage_case(name: &str, negative: Negative) {
    let root_path = "/cluster-tests/native-stage-current.bin";
    let old_path = "/services/native-stage-old-fetch.bin";
    let (native, initial, root) = crate::test_support::current_native_metadata_service(name, 3, root_path).unwrap();
    drop(initial);
    let source = root.primary_node_id.clone();
    let receiver = root.replica_node_ids[0].clone();
    let other = root.replica_node_ids[1].clone();
    let services = native.metadata_shards().unwrap().into_iter().find(|s| s.shard_id == "services").unwrap();
    let services = if services.primary_node_id == receiver { services } else {
        native.reassign_metadata_shard(&services.file_system_id, &services.shard_id,
            receiver.clone(), vec![source.clone(), other.clone()]).unwrap()
    };
    drop(native.metadata_primary_service(old_path).unwrap());
    // Qualify the final policy while the path is empty. Changing the seed's
    // replica set afterward would create an unrelated eventual placement row.
    let mut old_policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 2, 2).unwrap();
    old_policy.required_node_ids = vec![source.clone(), receiver.clone()];
    let apply = native.apply_placement_policy(&source, old_path, DfsPlacementScope::SelfOnly, old_policy).unwrap();
    finish_apply(&native, &apply);
    let mut root_policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 2, 3).unwrap();
    root_policy.required_node_ids = vec![source.clone(), receiver.clone(), other.clone()];
    let apply = native.apply_placement_policy(&source, root_path, DfsPlacementScope::SelfOnly, root_policy).unwrap();
    finish_apply(&native, &apply);
    let mut writer = native.client(&source).unwrap();
    writer.posix_mkdir_all("/cluster-tests").unwrap();
    let resolved_old = writer.resolved_placement_policy_for_path(
        old_path, DfsObjectPolicy::ReplicatedEventual, 2).unwrap();
    assert_eq!(resolved_old.policy.min_commit_replicas, 2);
    assert_eq!(resolved_old.policy.desired_replicas, 2);
    let old_base = writer.posix_write_file(old_path, b"old native predecessor",
        DfsWriteOptions {
            policy: resolved_old.policy.replication_policy(Default::default()).unwrap(),
            replica_candidates: vec![source.clone(), receiver.clone()],
            wait_for_commit: true, foreground_replica_repair: false,
        }).unwrap();
    let resolved_root = writer.resolved_placement_policy_for_path(
        root_path, DfsObjectPolicy::ReplicatedEventual, 3).unwrap();
    assert_eq!(resolved_root.policy.min_commit_replicas, 2);
    assert_eq!(resolved_root.policy.desired_replicas, 3);
    let root_options = DfsWriteOptions { policy: resolved_root.policy.replication_policy(Default::default()).unwrap(),
        replica_candidates: vec![source.clone(), receiver.clone(), other.clone()],
        wait_for_commit: true, foreground_replica_repair: false };
    let root_base = writer.posix_write_file(root_path, b"Root native predecessor", root_options).unwrap();
    writer.posix_sync_file(root_path, crate::DfsPosixSyncKind::File).unwrap();
    drop(writer);
    settle_maintenance(&native, &[&source, &receiver, &other]);
    let mut source_core = native.metadata_primary_service(root_path).unwrap();
    let old_reader = native.metadata_primary_service(old_path).unwrap();
    let old_bytes = vec![0x73; 4096];
    let old_version = crate::namespace::next_version(&old_base.version);
    // This is the ordinary requester's actual native durable prestaged body.
    let manifest = source_core.content().put_object_version(
        old_base.object_id.as_str(), old_version.as_str(), &old_bytes).unwrap();
    let data = native.data_dir(&receiver).clone();
    let paths = sinomo_core::network::ClusterPaths::new(&data);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths).unwrap();
    let runtime = crate::test_runtime::executor();
    let peer = read_daemon_dfs_message_peer_snapshot(&paths).unwrap().unwrap();
    let preflight = runtime.block_on(warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
        &paths, &peer, runtime.handle())).unwrap();
    let admission = runtime.block_on(daemon_dfs_mutation_scheduler(&data).unwrap()
        .admit(&crate::DfsMessageExecutionScope::GlobalExclusive)).unwrap();
    let owners = crate::publication_guard::DfsPublicationExecutionOwners::from_runtime(
        data.root(), crate::DfsMessageExecutionScope::GlobalExclusive, admission,
        require_daemon_dfs_data_owner_lock(&DfsRuntimePaths::new(&data)).unwrap());
    let mut retained = take_daemon_dfs_message_mutation_service_for_maintenance(
        &data, &paths, &state, runtime.handle(), Some(&preflight)).unwrap();
    let provider = crate::DfsPublicationExecutionProvider::from_execution_owners(
        &owners, Arc::clone(&retained.metadata_authority_generation), runtime.handle().clone()).unwrap();
    let execution = retained.lane.service.publication_execution.install(Arc::clone(&owners));
    let provider_scope = retained.lane.service.publication_provider.install(provider);
    let mut replication = crate::DfsReplicationPolicy::new(2, 2).unwrap();
    replication.required_node_ids = vec![source.clone(), receiver.clone()];
    let outcome = retained.lane.service.write_remote_distributed_manifest(
        sinomo_core::DfsWriteRequest { file_system_id: old_base.file_system_id.clone(),
            object_id: old_base.object_id.clone(), target_version: old_version.clone(),
            requesting_node_id: source.clone() }, Some(old_base.version.clone()),
        manifest.content_hash.clone(), manifest.size_bytes,
        vec![source.clone(), receiver.clone()], replication,
        DfsDistributedWriteOptions { repair: DfsWriteRepair::Background, ..Default::default() }).unwrap();
    let crate::NetworkedDfsWriteOutcome::Pending { intent_id: old_intent, .. } = outcome else {
        panic!("a real received Fetch awaits the ordinary owner consumer")
    };
    let old_reply = retained.lane.service.transport.incoming_messages.iter().find(|message| {
        matches!(&message.message, DfsNetworkMessage::FetchBlobResponse(reply)
            if reply.intent_id == old_intent && reply.object_id == old_base.object_id
                && reply.version == old_version && matches!(&reply.result,
                    crate::DfsFetchBlobResult::Found { content_hash, .. } if content_hash == &manifest.content_hash))
    }).cloned().expect("normal native Fetch must supply the exact old callback");
    let DfsNetworkMessage::FetchBlobResponse(received) = &old_reply.message else { unreachable!() };
    let crate::DfsFetchBlobResult::Found { bytes: received_bytes, .. } = &received.result else { unreachable!() };
    if received_bytes.is_empty() {
        assert_eq!(retained.lane.service.content().read_object_version(
            old_base.object_id.as_str(), old_version.as_str()).unwrap().1, old_bytes);
    } else {
        assert_eq!(received_bytes, &old_bytes, "the genuine received Fetch owns its exact original body");
    }
    assert_eq!(crate::LocalDfsRepairQueueStore::new(crate::default_dfs_repair_attempt_snapshot_path(&data))
        .view().unwrap().records.len(), 0);
    let expected_queues = queue_snapshot(&retained.lane.service);
    let old_prefix = retained.lane.service.metadata_backend().unwrap()
        .metadata_prefix_proof(&crate::DfsMetadataShardId::new("services")).unwrap();
    let allow_routine = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(Mutex::new(None));
    let hook_allowed = Arc::clone(&allow_routine);
    let hook_observed = Arc::clone(&observed);
    let hook_data = data.clone();
    let hook_root_object = root_base.object_id.clone();
    let hook_expected = expected_queues.clone();
    let hook_old_reply = old_reply.clone();
    let hook_old_prefix = old_prefix.clone();
    let hook = install_daemon_dfs_pre_dispatch_test_hook(&data, Arc::new(move |point, command, _| {
        if point == DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight
            && command.is_none() && !hook_allowed.load(Ordering::Acquire) {
            return Err(crate::DfsError::Cancelled { operation: "hold old callback until actual Stage reports" });
        }
        let Some(command) = command else { return Ok(()); };
        if !matches!(&command.routed_message.message, DfsNetworkMessage::StageBlob(stage)
            if stage.object_id == hook_root_object) { return Ok(()); }
        let DaemonDfsPreDispatchTestPoint::AfterReport { committed_work, repair_record_count, succeeded, .. } = point else {
            return Ok(());
        };
        assert!(succeeded && committed_work.repair_maintenance);
        assert_eq!(repair_record_count, 0);
        let slot = crate::runtime_message::dfs_runtime_message_cache()
            .existing_root(&hook_data.root()).unwrap().expect("the actual Stage owns an existing cache root");
        let handle = slot.lock().unwrap().as_ref().unwrap().primary_service.as_ref().unwrap().clone();
        let mut lane = handle.lock().unwrap();
        assert_eq!(queue_snapshot(&lane.service), hook_expected);
        let scope = lane.resident_metadata_scope.clone().unwrap();
        scope.attach(&mut lane.service).unwrap();
        assert_eq!(lane.service.metadata_backend().unwrap()
            .metadata_prefix_proof(&crate::DfsMetadataShardId::new("services")).unwrap(), hook_old_prefix);
        let mut negative_reports = Vec::new();
        if matches!(negative, Negative::TargetError | Negative::Authority) {
            let mut variants = Vec::new();
            if matches!(negative, Negative::TargetError) {
                let mut bad = command.clone();
                bad.routed_message.target_node_id = bad.routed_message.source_node_id.clone();
                variants.push(bad);
            } else {
                for wrong_root in [false, true] {
                    let mut bad = command.clone();
                    let DfsNetworkMessage::StageBlob(stage) = &mut bad.routed_message.message else { unreachable!() };
                    let authority = stage.authority_shard.as_mut().unwrap();
                    if wrong_root { authority.shard_id = "services".into(); }
                    else { authority.epoch += 1; }
                    variants.push(bad);
                }
            }
            for bad in variants {
                // Round trip the actual negative header through the native codec.
                let request = crate::dfs_message_rpc_request(&bad).unwrap();
                let decoded: crate::DfsMessageCommand = request.decode_body().unwrap();
                let report = lane.service.process_message_command_report(decoded);
                if matches!(negative, Negative::TargetError) { assert!(report.outcome.is_err()); }
                else { assert!(report.outcome.as_ref().unwrap().response.outbound_messages.iter().any(|message|
                    matches!(&message.message, DfsNetworkMessage::StageBlobResponse(reply)
                        if matches!(&reply.result, crate::DfsBlobTransferResult::Rejected { .. })))); }
                negative_reports.push(format!("{:?}", report.outcome));
                assert_eq!(queue_snapshot(&lane.service), hook_expected);
            }
        }
        if matches!(negative, Negative::FetchRequest) {
            let mut bad = hook_old_reply.clone();
            let DfsNetworkMessage::FetchBlobResponse(reply) = &mut bad.message else { unreachable!() };
            reply.request_id.push_str("-wrong-request");
            let event = lane.service.handle_fetch_blob_response(bad.source_node_id, reply.clone()).unwrap();
            assert!(matches!(event, crate::NetworkedDfsServiceEvent::BlobFetchRejected { .. }));
            assert_eq!(queue_snapshot(&lane.service), hook_expected);
            negative_reports.push(format!("{event:?}"));
        }
        assert_eq!(lane.service.metadata_backend().unwrap()
            .metadata_prefix_proof(&crate::DfsMetadataShardId::new("services")).unwrap(), hook_old_prefix);
        lane.service.release_resident_metadata_backend().unwrap();
        *hook_observed.lock().unwrap() = Some(serde_json::json!({
            "command": command, "restored_queues": hook_expected,
            "existing_repair_wake": committed_work.repair_maintenance,
            "repair_queue_records": repair_record_count, "negative_results": negative_reports,
        }));
        hook_allowed.store(true, Ordering::Release);
        Ok(())
    }));
    drop(provider_scope); drop(execution);
    assert!(restore_daemon_dfs_message_mutation_service_after_maintenance(
        &data, &paths, runtime.handle(), retained).unwrap());
    drop(owners);
    native.checkpoint_rpc_observation_window().unwrap();
    let source_data = native.data_dir(&source).clone();
    let source_admission = runtime.block_on(daemon_dfs_mutation_scheduler(&source_data).unwrap()
        .admit(&crate::DfsMessageExecutionScope::GlobalExclusive)).unwrap();
    let source_owners = crate::publication_guard::DfsPublicationExecutionOwners::from_runtime(
        source_data.root(), crate::DfsMessageExecutionScope::GlobalExclusive, source_admission,
        require_daemon_dfs_data_owner_lock(&DfsRuntimePaths::new(&source_data)).unwrap());
    let source_execution = source_core.publication_execution.install(Arc::clone(&source_owners));
    let root_version = crate::namespace::next_version(&root_base.version);
    let root_bytes = vec![0x42; 4096];
    let started = Instant::now();
    let deadline = started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    source_core.transport.set_operation_deadline(Some(deadline));
    let mut root_replication = crate::DfsReplicationPolicy::new(2, 3).unwrap();
    root_replication.required_node_ids = vec![source.clone(), receiver.clone(), other.clone()];
    // Exercise this receiver on every version: the existing foreground sender
    // reserves all N Stage targets, while W remains two durable receipts.
    let root_outcome = source_core.write_distributed_reader_with_candidates(
        sinomo_core::DfsWriteRequest { file_system_id: root.file_system_id,
            object_id: root_base.object_id.clone(), target_version: root_version.clone(), requesting_node_id: source.clone() },
        std::io::Cursor::new(&root_bytes), vec![source.clone(), receiver.clone(), other], root_replication,
        DfsDistributedWriteOptions { repair: DfsWriteRepair::Foreground, ..Default::default() }).unwrap();
    assert!(Instant::now() < deadline, "Stage must return inside its unchanged native deadline");
    assert!(daemon_dfs_mutation_scheduler(&source_data).unwrap().try_admit_global().is_none());
    let replies = source_core.transport.drain_incoming_messages().unwrap();
    let captured = observed.lock().unwrap().clone().expect("actual Main Stage report observed before reply");
    let received: crate::DfsMessageCommand = serde_json::from_value(captured["command"].clone()).unwrap();
    let DfsNetworkMessage::StageBlob(expected_stage) = &received.routed_message.message else { unreachable!() };
    assert!(replies.iter().any(|message| message.source_node_id == receiver && message.target_node_id == source
        && matches!(&message.message,
        DfsNetworkMessage::StageBlobResponse(reply) if reply.object_id == root_base.object_id
            && reply.version == root_version && reply.request_id == expected_stage.request_id
            && reply.intent_id == expected_stage.intent_id && reply.replica_node_id == receiver
            && matches!(&reply.result,
                crate::DfsBlobTransferResult::Durable { content_hash } if content_hash == &crate::content_hash(&root_bytes)))));
    drop(source_execution); drop(source_owners);
    source_core.transport.set_operation_deadline(None);
    // Observe automatic Routine progress only. No maintenance RPC, fresh write,
    // retry-clock change, wake injection or other network operation is issued.
    let completion_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let terminal = loop {
        let record = old_reader.metadata_backend().unwrap().get_intent(&old_base.object_id, &old_intent).unwrap().unwrap();
        if record.status == crate::DfsWriteIntentStatus::Committed { break record; }
        assert!(Instant::now() < completion_deadline, "the existing wake must run the actual callback owner");
        std::thread::yield_now();
    };
    assert_eq!(terminal.target_version, old_version);
    assert_eq!(terminal.content_hash.as_deref(), Some(manifest.content_hash.as_str()));
    assert!(old_reader.metadata_backend().unwrap().get_receipt_on_shard(
        &crate::DfsMetadataShardId::new(services.shard_id), &old_base.object_id, &old_version, &receiver).unwrap().is_some());
    let diagnostics = serde_json::json!({ "old_fetch": old_reply, "after_stage": captured,
        "root_outcome": format!("{root_outcome:?}"), "root_replies": replies,
        "old_terminal": terminal, "old_original_body": old_bytes,
        "qualification": "actual same-lane callback custody and ordinary automatic Routine wake; no fake receipt or mutation replay" });
    std::fs::write(data.root().parent().unwrap().join("stage-callback-state.json"), serde_json::to_vec_pretty(&diagnostics).unwrap()).unwrap();
    native.checkpoint_rpc_observation_window().unwrap();
    drop(old_reader); drop(source_core); drop(hook);
    native.close().unwrap();
}

#[test]
fn native_stage_callback_returns_current_reply_and_wakes_its_old_fetch_owner() {
    native_stage_case("native-stage-old-fetch-owner", Negative::None);
}
#[test]
fn native_stage_callback_custody_survives_current_target_error() {
    native_stage_case("native-stage-old-fetch-target-error", Negative::TargetError);
}
#[test]
fn native_stage_callback_custody_survives_current_root_and_epoch_rejection() {
    native_stage_case("native-stage-old-fetch-authority", Negative::Authority);
}
#[test]
fn native_stage_callback_custody_does_not_authorize_a_wrong_fetch_request() {
    native_stage_case("native-stage-old-fetch-wrong-request", Negative::FetchRequest);
}
