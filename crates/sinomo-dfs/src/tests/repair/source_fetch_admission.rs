use super::*;

struct NativeSourceFetch {
    primary: NodeId,
    holder: NodeId,
    target: NodeId,
    shard: DfsMetadataShardConfig,
    stat: DfsStat,
    bytes: Vec<u8>,
    intent: String,
    queue: LocalDfsRepairQueueStore,
    execution: crate::publication_guard::DfsPublicationContextRestore,
    core: NetworkedDfsService<DfsNodeTransport>,
    caller: NetworkedDfsService<DfsNodeTransport>,
    holds: Vec<crate::runtime_host::DaemonDfsPreDispatchTestHookGuard>,
    native: crate::test_runtime::DfsTestRuntime,
}

impl NativeSourceFetch {
    fn new(name: &str) -> Self {
        let path = format!("/services/{name}.bin");
        let (native, initial, original) =
            crate::test_support::current_native_metadata_service(name, 3, &path).unwrap();
        drop(initial);
        let target = original.primary_node_id.clone();
        let primary = original.replica_node_ids[0].clone();
        let holder = original.replica_node_ids[1].clone();
        let shard = native
            .reassign_metadata_shard(
                &original.file_system_id,
                &original.shard_id,
                primary.clone(),
                vec![target.clone(), holder.clone()],
            )
            .unwrap();
        let bytes = vec![0x6b; 4096];
        let mut writer = native.client(&holder).unwrap();
        let mut initial_policy = DfsReplicationPolicy::new(1, 1).unwrap();
        initial_policy.required_node_ids = vec![holder.clone()];
        let stat = writer
            .posix_write_file(
                &path,
                &bytes,
                DfsWriteOptions {
                    policy: initial_policy,
                    replica_candidates: vec![holder.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        assert_eq!(writer.posix_read_file(&path).unwrap(), bytes);
        drop(writer);
        // Keep repair claims parked while the normal configuration owner
        // observes Apply. A resident-preflight hold here would also prevent
        // that owner from making the newly published job visible.
        let background = [&primary, &target]
            .into_iter()
            .map(|node| native.deny_background_replica_before_claim(node).unwrap())
            .collect::<Vec<_>>();
        // Fetch precedes a repair claim. Preserve the absent-primary-content
        // prerequisite through real failed sends while ordinary owners catch up.
        native.install_fault(crate::test_runtime::FaultRule {
            source: Some(primary.clone()),
            target: Some(holder.clone()),
            route: Some(DFS_DATA_FETCH_BLOB_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::TransientRouteFailureBeforeDispatch,
        });
        let mut placement =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
        placement.required_node_ids = vec![target.clone()];
        let accepted = native
            .apply_placement_policy(&primary, &path, DfsPlacementScope::SelfOnly, placement)
            .unwrap();
        let mut publisher = native.configuration_publisher(&primary);
        let published = DfsControlPlaneStore::new(publisher.as_mut())
            .placement_reconciliation(&stat.file_system_id, &accepted.job_id)
            .unwrap()
            .expect("ordinary Apply stored its actual authoritative placement job");
        assert_eq!(published.accepted_config, accepted.accepted_config);
        assert_eq!(published.candidate_snapshot, accepted.candidate_snapshot);
        drop(publisher);
        // Reopen through normal startup completion after the actual publication,
        // as the placement producer requires. No projection or job is seeded.
        let mut caller = native.client(&target).unwrap();
        let mut core = native.metadata_primary_service(&path).unwrap();
        let retained = DfsControlPlaneStore::new(core.control_plane.as_mut().unwrap().as_mut())
            .placement_reconciliation(&stat.file_system_id, &accepted.job_id)
            .unwrap()
            .expect("the ordinary current owner observes the published placement job");
        assert_eq!(retained.accepted_config, accepted.accepted_config);
        assert_eq!(retained.candidate_snapshot, accepted.candidate_snapshot);
        let mut policy = DfsReplicationPolicy::new(1, 1).unwrap();
        policy.required_node_ids = vec![target.clone()];
        // Use the actual accepted policy and the ordinary encoded placement
        // producer, rather than seeding a repair row or a replica receipt.
        caller
            .route_namespace_operation(
                primary.clone(),
                DfsNamespaceOperation::ReconcilePlacement {
                    job_id: accepted.job_id,
                    path: path.clone(),
                    object_id: stat.object_id.clone(),
                    expected_version: stat.version.clone(),
                    object_policy: DfsObjectPolicy::ReplicatedEventual,
                    replication_policy: policy,
                    desired_replica_node_ids: vec![target.clone()],
                },
            )
            .unwrap();
        let execution = native_fixture_write_admission(&native, &core, &path);
        // The admitted Core now excludes competing Main work while replacing
        // the narrow setup holds. All real dispatch and receiver guards remain.
        drop(background);
        let holds = [&primary, &target].into_iter().map(|node| {
            crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
                native.data_dir(node), Arc::new(|point, command, _| {
                    if matches!(point, crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight)
                        && command.is_none() {
                        return Err(DfsError::Cancelled { operation: "private native source-fetch maintenance turn" });
                    }
                    Ok(())
                }),
            )
        }).collect::<Vec<_>>();
        native.remove_faults_matching(|rule| {
            rule.source.as_ref() == Some(&primary)
                && rule.target.as_ref() == Some(&holder)
                && rule.route.as_deref() == Some(DFS_DATA_FETCH_BLOB_ROUTE)
                && rule.call_id.is_none()
                && rule.origin == Some(crate::test_runtime::RpcOrigin::RegisteredHost)
                && matches!(
                    &rule.action,
                    crate::test_runtime::FaultAction::TransientRouteFailureBeforeDispatch
                )
        });
        core.metadata_backend
            .as_mut()
            .unwrap()
            .refresh_metadata_shard_attestation_view(&DfsMetadataShardId::new(&shard.shard_id))
            .unwrap();
        let object = core.current_read_object_metadata(&stat.object_id).unwrap();
        // The source must know the real committed placement intent before
        // exercising its intent-permission checks. Use normal native prefix
        // synchronization, with actual receiver acknowledgements only.
        let metadata_target = core.capture_metadata_quorum_sync_target(&object).unwrap();
        let metadata_sync = core
            .sync_metadata_shard_to_quorum(
                &metadata_target,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap();
        assert!(metadata_sync.acknowledged_node_ids.contains(&holder));
        assert_eq!(object.current_locations, vec![holder.clone()]);
        assert!(
            core.local_object_version_manifest_size_if_matches(
                &stat.object_id,
                &stat.version,
                stat.content_hash.as_deref().unwrap(),
            )
            .is_none()
        );
        assert!(
            core.backend_receipt_for_replica(&stat.object_id, &stat.version, &target)
                .unwrap()
                .is_none()
        );
        let intent = core
            .metadata_backend()
            .unwrap()
            .intents_for_object(&stat.object_id)
            .unwrap()
            .into_iter()
            .find(|intent| {
                intent.status == DfsWriteIntentStatus::Committed
                    && intent.desired_replica_node_ids == vec![target.clone()]
            })
            .expect("the real placement producer committed its target obligation")
            .intent_id;
        let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
            native.data_dir(&primary),
        ));
        assert!(
            queue
                .view()
                .unwrap()
                .records
                .iter()
                .any(|row| row.intent_id == intent
                    && row.missing_replica_node_id == target
                    && row.transfer_request_id.is_none()
                    && row.transfer_delivery_state
                        == Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent))
        );
        native.checkpoint_rpc_observation_window().unwrap();
        Self {
            primary,
            holder,
            target,
            shard,
            stat,
            bytes,
            intent,
            queue,
            execution,
            core,
            caller,
            holds,
            native,
        }
    }

    fn page(
        &mut self,
        excluded: &[NodeId],
    ) -> Result<crate::service_repair::DfsRepairPageProgress, DfsError> {
        let records = self.queue.view()?.records;
        let mut progress =
            crate::test_runtime::executor().block_on(self.core.retry_pending_repair_page(
                8,
                &records,
                &crate::DfsMaintenanceExecutionExclusions::new(excluded.iter().cloned()),
                None,
                None,
                true,
            ))?;
        if let Some(error) = progress.error.take() {
            return Err(error);
        }
        Ok(progress)
    }

    fn incoming(&mut self) {
        let messages = self.core.transport.drain_incoming_messages().unwrap();
        self.core
            .process_routed_messages_with_options(messages, true)
            .unwrap();
    }

    fn target_has_receipt(&self) -> bool {
        self.core
            .backend_receipt_for_replica(&self.stat.object_id, &self.stat.version, &self.target)
            .unwrap()
            .is_some()
    }

    fn close(self) {
        let Self {
            execution,
            core,
            caller,
            holds,
            native,
            ..
        } = self;
        drop(execution);
        drop(core);
        drop(caller);
        drop(holds);
        native.close().unwrap();
    }
}

#[test]
fn native_async_repair_fetches_recorded_excluded_source_before_actual_target_repair() {
    let mut case = NativeSourceFetch::new("native-async-recorded-source");
    let mut fetch_replies = Vec::new();
    let mut verified_fetch = false;
    for _ in 0..8 {
        case.page(&[case.holder.clone()]).unwrap();
        let expected_request = case.core.writes.pending_writes[&case.intent]
            .fetch_inflight_request_ids
            .get(&case.holder)
            .cloned();
        let messages = case.core.transport.drain_incoming_messages().unwrap();
        verified_fetch |= messages.iter().any(|message| {
            message.source_node_id == case.holder
                && message.target_node_id == case.primary
                && matches!(&message.message, DfsNetworkMessage::FetchBlobResponse(response)
                    if Some(&response.request_id) == expected_request.as_ref()
                        && response.intent_id == case.intent
                        && response.object_id == case.stat.object_id
                        && response.version == case.stat.version
                        && matches!(&response.result, DfsFetchBlobResult::Found { content_hash, .. }
                            if Some(content_hash) == case.stat.content_hash.as_ref()))
        });
        fetch_replies.extend(messages.iter().cloned());
        case.core
            .process_routed_messages_with_options(messages, true)
            .unwrap();
        // Stop before another ordinary Core page can select synchronous Repair.
        if verified_fetch {
            break;
        }
    }
    assert!(verified_fetch, "the original native Fetch returned its matching body hash");
    assert_eq!(
        case.core.content().read_object_version(
            case.stat.object_id.as_str(), case.stat.version.as_str(),
        ).unwrap().1,
        case.bytes,
        "the actual fetched body must materialize before the repair producer runs"
    );
    assert!(!case.target_has_receipt());
    assert!(case.queue.original_repair_for_slot(&case.intent, &case.target).unwrap().is_none());
    assert!(!case.native.rpc_observations().iter().any(|call| {
        call.source == case.primary
            && call.target == case.target
            && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE
            && call.phase == crate::test_runtime::RpcPhase::BeforeDispatch
    }));
    let before_repair = case.queue.view().unwrap().records;
    assert!(before_repair.iter().any(|row| row.intent_id == case.intent
        && row.missing_replica_node_id == case.target
        && row.transfer_request_id.is_none()
        && row.completed_transfer_request_id.is_none()
        && row.transfer_delivery_state
            == Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)));

    // This effect opener has no served Main background transport. Reuse the
    // actual background Repair producer under this same native execution;
    // false selects synchronous delivery of its unchanged background command.
    let pending = case.core.writes.pending_writes[&case.intent].clone();
    assert!(!pending.repair_inflight_request_ids.contains_key(&case.target));
    let request_id = case.core
        .original_repair_request_for_target(&case.intent, &case.target, &pending)
        .unwrap().expect("the real nameless NotSent obligation can create its first Original");
    let intent = case.core.coordinator().intent(&case.intent).unwrap().clone();
    let object = case.core.current_read_object_metadata(&case.stat.object_id).unwrap();
    let prepared = case.core.prepare_current_background_repair_replica_sends(
        &[(case.target.clone(), request_id.clone())], &intent, &pending, object,
    ).unwrap();
    let original = case.queue.original_repair_for_slot(&case.intent, &case.target)
        .unwrap().expect("the native producer enrolled its actual immutable Original");
    assert!(matches!(&original.original.message, DfsNetworkMessage::RepairReplica(command)
        if command.background_repair && command.request_id == request_id));
    let outcomes = crate::test_runtime::executor().block_on(
        case.core.dispatch_prepared_repair_replica_sends(prepared, false),
    ).unwrap();
    assert_eq!(outcomes.len(), 1);
    assert!(outcomes[0].sent);
    let repair_replies = case.core.transport.drain_incoming_messages().unwrap();
    let committed_reply = repair_replies.iter().any(|message| {
        message.source_node_id == case.target && message.target_node_id == case.primary
            && matches!(&message.message, DfsNetworkMessage::RepairReplicaResponse(response)
                if response.request_id == request_id && response.intent_id == case.intent
                    && response.object_id == case.stat.object_id && response.version == case.stat.version
                    && matches!(&response.result, DfsBlobTransferResult::Committed { content_hash }
                        if Some(content_hash) == case.stat.content_hash.as_ref()))
    });
    case.core.process_routed_messages_with_options(repair_replies.clone(), true).unwrap();
    let repaired = case.target_has_receipt();
    let cache_credited = case
        .core
        .backend_receipt_for_replica(&case.stat.object_id, &case.stat.version, &case.primary)
        .unwrap()
        .is_some();
    let physical = case
        .caller
        .content()
        .read_object_version(case.stat.object_id.as_str(), case.stat.version.as_str());
    let expected = case.bytes.clone();
    let calls = case.native.rpc_observations();
    let fetched = calls.iter().any(|call| {
        call.source == case.primary
            && call.target == case.holder
            && call.route == DFS_DATA_FETCH_BLOB_ROUTE
            && call.phase == crate::test_runtime::RpcPhase::Decoded
            && call.dfs_object_id.as_ref() == Some(&case.stat.object_id)
    });
    let actual_repair = calls.iter().any(|call| {
        call.source == case.primary
            && call.target == case.target
            && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE
            && call.phase == crate::test_runtime::RpcPhase::Completed
            && call.dfs_object_id.as_ref() == Some(&case.stat.object_id)
    });
    let settled_original = case.queue.original_repair(&original.original).unwrap();
    let closed = settled_original.as_ref().is_some_and(|birth| {
        birth.original == original.original && birth.closed_at_sequence.is_some()
    });
    let omitted = case.native.observation_dropped_count();
    let diagnostics = serde_json::json!({
        "scope": "async source-fetch and native background Repair producer integration",
        "verified_fetch": verified_fetch, "fetch_replies": fetch_replies,
        "repair_replies": repair_replies, "repair_outcomes": format!("{outcomes:?}"),
        "actual_repair": actual_repair, "committed_reply": committed_reply,
        "target_receipt": case.core.backend_receipt_for_replica(
            &case.stat.object_id, &case.stat.version, &case.target,
        ).unwrap(),
        "repaired": repaired, "cache_credited": cache_credited, "closed": closed,
        "birth": original, "settled_original": settled_original,
        "queue_before_repair": before_repair, "queue_after": case.queue.view().unwrap().records,
        "pending_before_repair": format!("{pending:?}"),
        "pending_after": format!("{:?}", case.core.writes.pending_writes.get(&case.intent)),
        "current_object": case.core.current_read_object_metadata(&case.stat.object_id).unwrap(),
        "prepared_operations": case.core.metadata_backend().unwrap().list_prepared_operations().unwrap(),
        "physical_result": format!("{physical:?}"), "omitted_rpc_observations": omitted,
    });
    let diagnostics_path = case.native.data_dir(&case.primary).root().parent().unwrap()
        .join("source-fetch-positive-state.json");
    std::fs::write(&diagnostics_path, serde_json::to_vec_pretty(&diagnostics).unwrap()).unwrap();
    case.native.checkpoint_rpc_observation_window().unwrap();
    case.close();
    assert!(
        fetched,
        "the actual recorded remote holder must receive the encoded fetch"
    );
    assert!(
        actual_repair && committed_reply && repaired && closed,
        "actual Repair, guarded receipt publication and Original closure are required"
    );
    assert_eq!(physical.unwrap().1, expected);
    assert!(
        !cache_credited,
        "reading the source must not credit the non-desired metadata owner as a replica"
    );
    assert_eq!(omitted, 0);
}

#[test]
fn native_async_repair_keeps_destination_excluded_even_with_a_live_source() {
    let mut case = NativeSourceFetch::new("native-async-excluded-target");
    case.page(&[case.holder.clone(), case.target.clone()])
        .unwrap();
    case.incoming();
    let repaired = case.target_has_receipt();
    let original = case
        .queue
        .original_repair_for_slot(&case.intent, &case.target)
        .unwrap();
    let sent = case.native.rpc_observations().iter().any(|call| {
        call.source == case.primary
            && matches!(
                call.route.as_str(),
                DFS_DATA_FETCH_BLOB_ROUTE | DFS_DATA_REPAIR_REPLICA_ROUTE
            )
            && call.phase == crate::test_runtime::RpcPhase::BeforeDispatch
    });
    case.close();
    assert!(
        !repaired && original.is_none() && !sent,
        "destination exclusion must prevent any repair admission"
    );
}

#[test]
fn native_async_repair_unroutable_source_cannot_create_a_target_receipt() {
    let mut case = NativeSourceFetch::new("native-async-unroutable-source");
    case.native.install_fault(crate::test_runtime::FaultRule {
        source: Some(case.primary.clone()),
        target: Some(case.holder.clone()),
        route: Some(DFS_DATA_FETCH_BLOB_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::TransientRouteFailureBeforeDispatch,
    });
    case.page(&[case.holder.clone()]).unwrap();
    case.incoming();
    let repaired = case.target_has_receipt();
    let original = case
        .queue
        .original_repair_for_slot(&case.intent, &case.target)
        .unwrap();
    let target_send = case.native.rpc_observations().iter().any(|call| {
        call.source == case.primary
            && call.target == case.target
            && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE
            && call.phase == crate::test_runtime::RpcPhase::BeforeDispatch
    });
    let failed_fetch = case.native.rpc_observations().iter().any(|call| {
        call.source == case.primary
            && call.target == case.holder
            && call.route == DFS_DATA_FETCH_BLOB_ROUTE
            && call.outcome_error.is_some()
    });
    case.native.clear_faults();
    case.close();
    assert!(
        failed_fetch && !repaired && original.is_none() && !target_send,
        "a failed real fetch cannot manufacture target durability"
    );
}

#[test]
fn native_fetch_rejects_a_member_without_original_intent_permission() {
    let case = NativeSourceFetch::new("native-async-unauthorized-fetch");
    let command = DfsMessageCommand {
        request_id: "native-unauthorized-source-fetch".into(),
        routed_message: crate::routes::build_dfs_routed_message(
            &case.holder,
            &case.holder,
            1,
            1,
            DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                request_id: "native-unauthorized-source-fetch".into(),
                intent_id: case.intent.clone(),
                object_id: case.stat.object_id.clone(),
                version: case.stat.version.clone(),
                content_hash: case.stat.content_hash.clone().unwrap(),
                authority_shard: Some(case.shard.clone()),
            }),
        ),
    };
    let response: DfsMessageResponse = case
        .native
        .rpc_request(
            &case.holder,
            &case.holder,
            crate::dfs_message_rpc_request(&command).unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap()
        .decode_body()
        .unwrap();
    let rejected = response.outbound_messages.iter().any(|message| {
        matches!(&message.message, DfsNetworkMessage::FetchBlobResponse(reply)
            if reply.request_id == "native-unauthorized-source-fetch"
                && matches!(reply.result, DfsFetchBlobResult::Rejected { .. }))
    });
    let repaired = case.target_has_receipt();
    let original = case
        .queue
        .original_repair_for_slot(&case.intent, &case.target)
        .unwrap();
    case.close();
    assert!(
        rejected && !repaired && original.is_none(),
        "authenticated membership alone must not authorize this fetch"
    );
}
