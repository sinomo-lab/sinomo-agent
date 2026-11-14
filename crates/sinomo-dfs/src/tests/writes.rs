//! Writes regression tests.

use super::*;

#[test]
fn native_six_member_writes_keep_exact_completion_and_policy() {
    let name = "native-six-member-write-completion";
    let (runtime, initial, shard) =
        crate::test_support::current_native_metadata_service(name, 6, "/services").unwrap();
    assert_eq!(
        shard.replica_node_ids.len(),
        5,
        "actual six-member assignment"
    );
    drop(initial);
    runtime.checkpoint_rpc_observation_window().unwrap();
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 6);
    let origin = nodes
        .iter()
        .find(|node| **node != shard.primary_node_id)
        .unwrap();
    for (w, n) in [(1, 2), (2, 3)] {
        runtime
            .apply_placement_policy(
                origin,
                "/services",
                DfsPlacementScope::Subtree,
                DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, w, n).unwrap(),
            )
            .unwrap();
        runtime.checkpoint_rpc_observation_window().unwrap();
        let mut writer = runtime.client(origin).unwrap();
        let mut reader = runtime.client(&shard.primary_node_id).unwrap();
        let options = DfsWriteOptions {
            policy: DfsReplicationPolicy::new(w, n).unwrap(),
            replica_candidates: nodes.clone(),
            wait_for_commit: true,
            foreground_replica_repair: false,
        };
        for index in 0..100 {
            let path = format!("/services/six-member-w{w}-{index}.bin");
            for (operation, value) in [("create", 0x41), ("overwrite", 0x62)] {
                let operation_id = format!("six-member-w{w}-{operation}-{index}");
                let bytes = vec![value; 4096];
                let outcome = writer
                    .posix_write_file_with_operation_id(
                        &path,
                        &bytes,
                        options.clone(),
                        &operation_id,
                    )
                    .unwrap_or_else(|error| {
                        panic!("exact operation {operation_id} on {path}: {error}")
                    });
                runtime.checkpoint_rpc_observation_window().unwrap();
                assert_eq!(reader.posix_read_file_distributed(&path).unwrap(), bytes);
                runtime.checkpoint_rpc_observation_window().unwrap();
                let reconciled = writer
                    .reconcile_stable_write_operation(
                        &path,
                        &outcome.object_id,
                        &operation_id,
                        outcome.content_hash.as_deref().unwrap(),
                        4096,
                    )
                    .unwrap();
                assert!(
                    matches!(
                        reconciled,
                        DfsStableWriteOperationReconciliation::Applied { .. }
                    ),
                    "exact operation {operation_id}: {reconciled:?}"
                );
                // Each public operation may include bounded catch-up traffic;
                // seal it before the next operation instead of dropping DATA.
                runtime.checkpoint_rpc_observation_window().unwrap();
            }
        }
        drop(reader);
        drop(writer);
    }
    runtime.close().unwrap();
}

#[test]
fn node_transport_completes_synchronous_replica_write() {
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids("sync-replica-write", 2);
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "node-route-transport-replica-write",
        nodes[0].clone(),
    )
    .expect("normal native runtime");
    runtime
        .define_node(nodes[1].clone())
        .expect("actual replica");
    runtime.start_nodes().expect("native component startup");
    let control = runtime
        .shared_control_plane()
        .expect("native control plane");
    let configs = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut()).list_metadata_shards(&sifs_file_system_id())
        })
        .expect("actual assignments");
    let local = configs
        .iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap()
        .primary_node_id
        .clone();
    let remote = nodes.iter().find(|node| **node != local).unwrap().clone();
    let mut dfs = runtime.client(&local).expect("normal primary client");
    let stat = dfs
        .posix_write_file(
            "/services/node_route-transport.txt",
            b"replicated bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
                replica_candidates: vec![local.clone(), remote.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("replicated write");

    assert_eq!(stat.current_locations, nodes);
    assert_eq!(stat.size_bytes, Some(b"replicated bytes".len() as u64));
    let owner = runtime
        .metadata_primary_service("/services/node_route-transport.txt")
        .expect("actual current Core owns the sealed publication");
    let backend = owner.metadata_backend().expect("current native backend");
    let record = backend
        .export_table_snapshot()
        .expect("entire actual sealed publication table")
        .tables
        .prepared_operations_v7
        .into_iter()
        .find(|record| {
            record.terminal_intent.as_ref().is_some_and(|intent| {
                intent.object_id == stat.object_id && intent.target_version == stat.version
            })
        })
        .expect("sealed write outcome");
    record
        .validate()
        .expect("entire original committed digest/body");
    assert_eq!(record.status, DfsPreparedOperationStatus::Committed);
    assert!(
        backend
            .committed_prepared_operation(&record.digest)
            .unwrap()
            .is_some()
    );
    let remote_receipts = record
        .content_receipts
        .iter()
        .filter(|receipt| receipt.replica_node_id == remote)
        .collect::<Vec<_>>();
    assert_eq!(
        remote_receipts.len(),
        1,
        "one actual durable remote reply completes the synchronous write"
    );
    let replica = runtime
        .client(&remote)
        .expect("actual remote storage owner");
    assert_eq!(
        replica
            .content
            .read_blob(remote_receipts[0].content_hash.as_deref().unwrap())
            .unwrap(),
        b"replicated bytes"
    );
    drop(owner);
    drop(replica);
    drop(dfs);
    drop(control);
    runtime.close().expect("native fixture shutdown");
}

#[test]
fn default_cluster_dfs_helper_writes_default_records_to_cluster_kv() {
    let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let (runtime, service, _) = crate::test_support::current_native_metadata_service(
        "default-native-records",
        1,
        "/services",
    )
    .expect("normal component initialization");
    let control = runtime.shared_control_plane().unwrap();
    // Metadata startup and the first physical storage sample are independent
    // native owners. This positive content fixture must observe the latter;
    // the initial NeverSampled advertisement correctly fails closed.
    let (changed, wake) = std::sync::mpsc::sync_channel(1);
    let subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
        &sinomo_core::network::ClusterPaths::new(runtime.data_dir(service.node_id())),
        move |_| {
            let _ = changed.try_send(());
        },
    );
    let read_content = || {
        control.with_client(|kv| {
            // Content advertisements are outside the retained filesystem
            // prefix. A committed-observation wake does not invalidate this
            // client's exact-key cache. Read the actual publisher's record
            // before the original startup deadline, including on the first
            // read after subscribing (publication may already have finished).
            let entries = kv
                .get_authoritative_consistent_before(
                    &[crate::content_service_key(service.node_id())],
                    deadline,
                )
                .expect("authoritative storage advertisement within the startup budget");
            entries[0]
                .as_ref()
                .expect("normal startup published its content record")
                .decode::<crate::DfsNodeAdvertisement>()
                .expect("actual content advertisement")
        })
    };
    let initial = read_content();
    let mut sampled = initial.clone();
    while sampled.storage_freshness == crate::DfsObservationFreshness::NeverSampled {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "native storage publication startup deadline"
        );
        // No client, admission, or cache lock is held while its actual owner
        // publishes. Wakeups may coalesce; each read observes the real record.
        wake.recv_timeout(remaining)
            .expect("first actual storage advertisement within the existing startup budget");
        sampled = read_content();
    }
    std::fs::write(
        runtime.data_dir(service.node_id()).root().join("boundary-diagnostic-native-content-ready.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "initial": initial, "sampled": sampled,
            "qualification": "Actual committed advertisements; notification wakeups coalesce. No synthetic capacity, readiness, or authority."
        })).unwrap(),
    ).unwrap();
    drop(subscription);
    control.with_client(|kv| {
        let store = DfsControlPlaneStore::new(kv.as_mut());
        let config = store
            .get_file_system_config(&sifs_file_system_id())
            .unwrap()
            .unwrap();
        assert_eq!(config.owner_node_id, *service.node_id());
        let shards = store.list_metadata_shards(&sifs_file_system_id()).unwrap();
        assert_eq!(shards.len(), 3);
        assert!(
            shards
                .iter()
                .all(|shard| shard.primary_node_id == *service.node_id())
        );
        assert!(
            store
                .metadata_service(service.node_id())
                .unwrap()
                .unwrap()
                .accepts_metadata
        );
        assert!(
            store
                .content_service(service.node_id())
                .unwrap()
                .unwrap()
                .accepts_content
        );
    });
    drop((service, control));
    runtime.close().expect("normal owner stopped");
}

#[test]
fn control_plane_default_bootstrap_host_writes_default_dfs_records() {
    let (mut runtime, mut service, _) = crate::test_support::current_native_metadata_service(
        "native-bootstrap-policy-restart",
        1,
        "/services",
    )
    .expect("normal component initialization");
    let node = service.node_id().clone();
    let rule = normalize_placement_rule(
        "/managed",
        DfsPlacementScope::Subtree,
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap(),
    )
    .unwrap();
    runtime
        .apply_placement_policy(
            &node,
            "/managed",
            DfsPlacementScope::Subtree,
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap(),
        )
        .expect("normal guarded placement configuration producer");
    drop(service);
    runtime.stop_node(&node).unwrap();
    runtime.reopen_node(node.clone()).unwrap();
    let control = runtime.shared_control_plane().unwrap();
    control.with_client(|kv| {
        let store = DfsControlPlaneStore::new(kv.as_mut());
        let current = store
            .get_file_system_config(&sifs_file_system_id())
            .unwrap()
            .unwrap();
        assert!(
            current.subtree_policies.contains(&rule),
            "normal restart preserves configured placement"
        );
        assert_eq!(
            store
                .list_metadata_shards(&sifs_file_system_id())
                .unwrap()
                .len(),
            3
        );
        assert_eq!(store.content_service(&node).unwrap().unwrap().node_id, node);
    });
    drop(control);
    runtime.close().expect("normal owner stopped");
}

#[test]
fn default_sifs_metadata_materialization_writes_node_directories_and_visible_versions() {
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-node-directories", 2);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-node-directories", nodes[0].clone())
            .unwrap();
    runtime.define_node(nodes[1].clone()).unwrap();
    runtime
        .start_nodes()
        .expect("normal guarded materialization");
    // Materialization is a property of the node's durable store; the client
    // delegating to its Main keeps none.
    let backend = runtime.node_metadata(&nodes[0]).unwrap();
    for node in &nodes {
        let path = sifs_node_files_path(node);
        let object = backend
            .get_object_by_path(&sifs_file_system_id(), &path)
            .unwrap()
            .expect("normal node files directory");
        assert_eq!(object.kind, DfsObjectKind::Directory);
        let version = backend
            .get_version(&object.object_id, &object.version)
            .unwrap()
            .unwrap();
        assert!(version.visible);
        assert_eq!(version.locations, object.current_locations);
    }
    drop(backend);
    runtime.close().expect("normal owner stopped");
}

#[test]
fn restored_pending_stage_rejects_old_ack_and_commits_exact_fresh_transfer() {
    let name = "native-stage-reply-restored-owner";
    let (mut runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id.clone();
    drop(initial);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
    let replica = nodes.iter().find(|node| **node != owner).unwrap().clone();
    let path = format!("/services/{name}.bin");
    let mut first_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
    first_policy.required_node_ids = vec![owner.clone()];
    let first_apply = runtime
        .apply_placement_policy(&owner, &path, DfsPlacementScope::Subtree, first_policy)
        .unwrap();
    finish_native_placement_fixture_apply(&runtime, &first_apply);
    let mut writer = runtime.metadata_primary_service(&path).unwrap();
    let (_, _, base_policy) = writer
        .managed_replication_policy_for_path(&path, DfsObjectPolicy::ReplicatedConfirmed, 1)
        .unwrap();
    let base = writer
        .posix_write_file(
            &path,
            b"actual original write head",
            DfsWriteOptions {
                policy: base_policy,
                replica_candidates: vec![owner.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    drop(writer);
    let mut next_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    next_policy.required_node_ids = nodes.clone();
    let apply = runtime
        .apply_placement_policy(&owner, &path, DfsPlacementScope::Subtree, next_policy)
        .unwrap();
    // Finish the actual managed Apply before the intended Pending write.
    // Its real predecessor copy and mutation permit are setup work, not an
    // arbitrary Busy outcome to retry during the ACK identity test.
    let control = runtime.shared_control_plane().unwrap();
    let apply_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let current = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&base.file_system_id, &apply.job_id)
            })
            .unwrap()
            .unwrap();
        if current.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        assert!(
            matches!(
                current.state,
                DfsPlacementReconciliationState::Pending
                    | DfsPlacementReconciliationState::Scanning
                    | DfsPlacementReconciliationState::Repairing
            ),
            "actual setup Apply failed: {current:?}"
        );
        let remaining = apply_deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "normal setup Apply did not complete: {current:?}"
        );
        let tick = runtime.maintenance_tick(&owner, remaining).unwrap();
        assert!(
            tick.outcome.is_ok(),
            "actual normal setup maintenance failed: {tick:#?}"
        );
        std::thread::yield_now();
    }
    drop(control);
    let current = runtime.metadata_primary_service(&path).unwrap();
    let current_base = current.posix_stat(&path).unwrap();
    assert_eq!(current_base.object_id, base.object_id);
    assert_eq!(current_base.content_hash, base.content_hash);
    assert_eq!(
        current
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&current_base.object_id, &current_base.version)
            .unwrap()
            .into_iter()
            .map(|receipt| receipt.replica_node_id)
            .collect::<BTreeSet<_>>(),
        nodes.iter().cloned().collect()
    );
    drop(current);
    let base = current_base;
    let background = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    let mut writer = runtime.metadata_primary_service(&path).unwrap();
    let bytes = vec![0x73; 2 * 1024 * 1024 + 4096];
    let version = crate::namespace::next_version(&base.version);
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id.clone(),
        object_id: base.object_id.clone(),
        target_version: version.clone(),
        requesting_node_id: owner.clone(),
    };
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    let outcome = writer
        .write_distributed_reader_with_candidates(
            request.clone(),
            std::io::Cursor::new(&bytes),
            nodes,
            policy,
            DfsDistributedWriteOptions::default(),
        )
        .unwrap();
    let NetworkedDfsWriteOutcome::Pending {
        intent_id,
        durable_receipts,
        required_receipts,
    } = outcome
    else {
        panic!("actual Stage reply waits in its native incoming queue before W2")
    };
    assert_eq!((durable_receipts, required_receipts), (1, 2));
    let received = writer.transport.drain_incoming_messages().unwrap();
    let originals = received
        .iter()
        .filter(|routed| {
            routed.source_node_id == replica
                && routed.target_node_id == owner
                && matches!(&routed.message, DfsNetworkMessage::StageBlobResponse(response)
            if response.intent_id == intent_id && response.file_system_id == request.file_system_id
                && response.object_id == request.object_id && response.version == version
                && response.replica_node_id == replica
                && matches!(&response.result, DfsBlobTransferResult::Durable { content_hash: hash }
                    if hash == &content_hash(&bytes)))
        })
        .cloned()
        .collect::<Vec<_>>();
    let [original] = originals.as_slice() else {
        panic!("one canonical original Stage response required");
    };
    let DfsNetworkMessage::StageBlobResponse(original_response) = &original.message else {
        unreachable!()
    };
    writer
        .ensure_stage_response_matches_pending(original_response, &content_hash(&bytes))
        .unwrap();
    let stored = writer
        .metadata_backend()
        .unwrap()
        .get_intent(&request.object_id, &intent_id)
        .unwrap()
        .unwrap();
    assert_ne!(stored.status, DfsWriteIntentStatus::Committed);
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
    drop(writer);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut restored = runtime.metadata_primary_service(&path).unwrap();
    restored.hydrate_from_metadata_backend().unwrap();
    assert!(
        restored
            .writes
            .pending_writes
            .get(&intent_id)
            .unwrap()
            .stage_inflight_request_ids
            .is_empty()
    );
    assert_ne!(
        restored
            .metadata_backend()
            .unwrap()
            .get_intent(&request.object_id, &intent_id)
            .unwrap()
            .unwrap()
            .status,
        DfsWriteIntentStatus::Committed
    );
    let before = restored
        .metadata_backend()
        .unwrap()
        .export_table_snapshot()
        .unwrap();
    let before_intent = restored.coordinator().intent(&intent_id).unwrap().clone();
    assert_eq!(
        restored
            .metadata_backend()
            .unwrap()
            .get_intent(&request.object_id, &intent_id)
            .unwrap()
            .as_ref(),
        Some(&stored)
    );
    let rejection = restored
        .ensure_stage_response_matches_pending(original_response, &content_hash(&bytes))
        .unwrap_err();
    assert!(matches!(rejection, DfsError::Distributed { .. }));
    let work = DfsCommittedWorkTracker::new();
    restored.set_committed_work_tracker(work.clone());
    let events = restored
        .process_routed_messages_with_options(vec![original.clone()], false)
        .unwrap();
    let [
        NetworkedDfsServiceEvent::BlobStageRejected {
            intent_id: rejected_intent,
            object_id,
            replica_node_id,
            reason,
        },
    ] = events.as_slice()
    else {
        panic!("old uncorrelated ACK must have one typed Stage rejection: {events:?}");
    };
    assert_eq!(rejected_intent, &intent_id);
    assert_eq!(object_id, &request.object_id);
    assert_eq!(replica_node_id, &replica);
    assert_eq!(reason, &rejection.to_string());
    assert_eq!(work.take(), DfsMessageCommittedWork::default());
    assert_eq!(
        restored
            .metadata_backend()
            .unwrap()
            .export_table_snapshot()
            .unwrap(),
        before,
        "the old ACK changes neither the full durable tables nor their exact prefix"
    );
    assert_eq!(
        restored.coordinator().intent(&intent_id),
        Some(&before_intent)
    );
    assert!(
        restored
            .writes
            .pending_writes
            .get(&intent_id)
            .unwrap()
            .stage_inflight_request_ids
            .is_empty()
    );
    assert!(matches!(
        restored.write_outcome(&intent_id).unwrap(),
        NetworkedDfsWriteOutcome::Pending {
            durable_receipts: 1,
            required_receipts: 2,
            ..
        }
    ));

    // Use the normal existing foreground retry producer, not the prior ACK or
    // a fabricated request reservation. Main background claims remain denied.
    runtime.clear_faults();
    let capture = runtime.capture_served_work(&replica, 32).unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let prior_deadline = restored.transport.set_operation_deadline(Some(deadline));
    restored.retry_pending_stages(8).unwrap();
    let fresh_request = restored
        .writes
        .pending_writes
        .get(&intent_id)
        .unwrap()
        .stage_inflight_request_ids
        .get(&replica)
        .cloned()
        .expect("normal retry must own a fresh actual Stage request");
    assert_ne!(fresh_request, original_response.request_id);
    let fresh = restored.transport.drain_incoming_messages().unwrap();
    let replies = fresh
        .iter()
        .filter_map(|routed| {
            if routed.source_node_id != replica || routed.target_node_id != owner {
                return None;
            }
            match &routed.message {
                DfsNetworkMessage::StageBlobResponse(response)
                    if response.request_id == fresh_request && response.intent_id == intent_id =>
                {
                    Some(response)
                }
                _ => None,
            }
        })
        .collect::<Vec<_>>();
    let [reply] = replies.as_slice() else {
        panic!("one actual fresh Stage acknowledgement required");
    };
    let mut expected = original_response.clone();
    expected.request_id = fresh_request.clone();
    assert_eq!(
        *reply, &expected,
        "only the current transfer request changes the original exact reply tuple"
    );
    restored
        .ensure_stage_response_matches_pending(reply, &content_hash(&bytes))
        .unwrap();
    let stages = capture
        .records()
        .into_iter()
        .filter(|record| {
            matches!(
        &record.command.routed_message.message, DfsNetworkMessage::StageBlob(command)
            if command.request_id == fresh_request && command.intent_id == intent_id)
        })
        .collect::<Vec<_>>();
    let [stage] = stages.as_slice() else {
        panic!("one genuine fresh Stage receiver required");
    };
    assert!(stage.succeeded);
    assert_eq!(stage.command.routed_message.source_node_id, owner);
    assert_eq!(stage.command.routed_message.target_node_id, replica);
    let DfsNetworkMessage::StageBlob(command) = &stage.command.routed_message.message else {
        unreachable!()
    };
    assert_eq!(command.file_system_id, request.file_system_id);
    assert_eq!(command.object_id, request.object_id);
    assert_eq!(command.version, request.target_version);
    assert_eq!(command.expected_base_version, stored.base_version);
    assert_eq!(command.content_hash, content_hash(&bytes));
    assert_eq!(command.authority_shard.as_ref(), Some(&config));
    let events = restored
        .process_routed_messages_with_options(fresh, false)
        .unwrap();
    assert!(events.iter().any(|event| matches!(event, NetworkedDfsServiceEvent::BlobStaged {
        intent_id: current_intent, object_id, version: current_version, replica_node_id, content_hash: hash,
    } if current_intent == &intent_id && object_id == &request.object_id && current_version == &version
        && replica_node_id == &replica && hash == &content_hash(&bytes))));
    assert!(
        matches!(restored.write_outcome(&intent_id).unwrap(), NetworkedDfsWriteOutcome::Committed {
        version: current, content_hash: hash, .. } if current == version && hash == content_hash(&bytes))
    );
    assert!(
        Instant::now() <= deadline,
        "the fresh owned transfer completes inside the unchanged default timeout"
    );
    restored.transport.set_operation_deadline(prior_deadline);
    let committed = restored
        .metadata_backend()
        .unwrap()
        .export_table_snapshot()
        .unwrap();
    let record = committed
        .tables
        .prepared_operations_v7
        .iter()
        .find(|record| {
            record.payload.prepared_intent.as_ref() == Some(&stored)
                && record
                    .payload
                    .terminal_intent
                    .as_ref()
                    .is_some_and(|intent| {
                        intent.intent_id == intent_id
                            && intent.file_system_id == request.file_system_id
                            && intent.object_id == request.object_id
                            && intent.target_version == version
                    })
        })
        .expect("fresh receipt commits the complete original nonterminal body");
    record.validate().unwrap();
    assert_eq!(record.status, DfsPreparedOperationStatus::Committed);
    assert!(
        restored
            .metadata_backend()
            .unwrap()
            .committed_prepared_operation(&record.digest)
            .unwrap()
            .is_some()
    );
    let current = restored
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.version, version);
    assert_eq!(
        current
            .current_locations
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([owner.clone(), replica.clone()])
    );
    let actual = runtime.client(&replica).unwrap();
    assert_eq!(
        actual
            .content()
            .read_object_version(request.object_id.as_str(), version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(actual);
    drop(capture);
    drop(restored);
    drop(background);
    runtime.clear_faults();
    runtime.close().unwrap();
}


#[test]
fn posix_local_primary_detached_manifest_write_persists_visible_metadata_without_self_rpc() {
    let (mut runtime, initial, shard) = crate::test_support::current_native_metadata_service(
        "native-local-primary-detached-manifest",
        2,
        "/services",
    )
    .unwrap();
    let local = shard.primary_node_id;
    let peer = crate::test_runtime::DfsTestRuntime::ordered_node_ids(
        "native-local-primary-detached-manifest",
        2,
    )
    .into_iter()
    .find(|node| *node != local)
    .unwrap();
    drop(initial);
    let path = "/services/local-primary-large.bin";
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    placement.required_node_ids = vec![local.clone(), peer.clone()];
    // The original write must carry the constraints of this installed rule.
    let mut policy = DfsReplicationPolicy::new(1, 2).unwrap();
    policy.required_node_ids = placement.required_node_ids.clone();
    runtime
        .apply_placement_policy(&local, path, DfsPlacementScope::Subtree, placement)
        .unwrap();
    for route in [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE] {
        runtime.install_fault(crate::test_runtime::FaultRule {
            source: Some(local.clone()),
            target: Some(peer.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    }
    let background = runtime
        .deny_background_replica_before_claim(&local)
        .unwrap();
    let mut writer = runtime.metadata_primary_service(path).unwrap();
    let bytes = vec![0x63; 2 * 1024 * 1024 + 8192];
    let expected_hash = content_hash(&bytes);
    let before = runtime.rpc_observations().len();
    let execution = super::repair::native_fixture_write_admission(&runtime, &writer, path);
    let stat = writer
        .posix_write_file_with_known_parent_deferred_local_replay(
            path,
            &bytes,
            DfsWriteOptions {
                policy,
                replica_candidates: vec![local.clone(), peer.clone()],
                wait_for_commit: false,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(execution);
    assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
    assert_eq!(writer.posix_read_file(path).unwrap(), bytes);
    let actual = writer
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(actual.version, stat.version);
    assert_eq!(actual.content_hash, stat.content_hash);
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .all(|event| !(event.source == local
                && event.target == local
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE)),
        "the actual local authority publishes the captured manifest without a self write RPC"
    );
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .all(|event| !(event.source == local
                && event.target == peer
                && [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE]
                    .contains(&event.route.as_str())
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch)),
        "detached local replay does not inline remote content transfer"
    );
    drop(writer);
    runtime.stop_node(&local).unwrap();
    runtime.reopen_node(local.clone()).unwrap();
    let mut reloaded = runtime.metadata_primary_service(path).unwrap();
    let actual = reloaded.posix_stat(path).unwrap();
    assert_eq!(actual.object_id, stat.object_id);
    assert_eq!(actual.version, stat.version);
    assert_eq!(actual.content_hash.as_deref(), Some(expected_hash.as_str()));
    assert_eq!(reloaded.posix_read_file(path).unwrap(), bytes);
    drop(reloaded);
    drop(background);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn posix_non_foreground_write_reports_only_durable_locations() {
    let name = "posix-non-foreground-current-locations";
    let (runtime, initial, shard) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let local = shard.primary_node_id.clone();
    let remote = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| node != &local)
        .unwrap();
    drop(initial);
    let path = "/services/async-repair.txt";
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    placement.required_node_ids = vec![local.clone(), remote.clone()];
    let applied = runtime
        .apply_placement_policy(&local, path, DfsPlacementScope::SelfOnly, placement.clone())
        .unwrap();
    // Finish the accepted setup job before creating the object whose original
    // write repair is measured below. Otherwise that independent job can also
    // schedule a placement repair for the same newly created version.
    finish_native_placement_fixture_apply(&runtime, &applied);
    for route in [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE] {
        runtime.install_fault(crate::test_runtime::FaultRule {
            source: Some(local.clone()),
            target: Some(remote.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    }
    let mut dfs = runtime.client(&local).unwrap();
    let stat = dfs
        .posix_write_file(
            path,
            b"commit locally and repair the remote replica later",
            DfsWriteOptions {
                policy: placement.replication_policy(Default::default()).unwrap(),
                replica_candidates: vec![local.clone(), remote.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .expect("non-foreground write");
    assert_eq!(
        stat.current_locations,
        vec![local.clone()],
        "the returned POSIX stat must not claim an async repair target is durable"
    );
    let owner = runtime.metadata_primary_service(path).unwrap();
    let owned = owner
        .metadata_backend()
        .unwrap()
        .metadata_summary_for_local_primary_shards()
        .unwrap();
    assert_eq!(
        owned.pending_repair_count, 1,
        "the actual committed metadata must retain its missing desired replica"
    );
    assert_eq!(
        super::repair::indexed_repair_needs(&runtime, &owner, &stat)
            .into_iter()
            .map(|(target, _)| target)
            .collect::<Vec<_>>(),
        vec![remote.clone()],
        "the remote desired replica must remain repair work in the shard tables' index"
    );
    drop(owner);
    drop(dfs);
    runtime.clear_faults();
    runtime.close().expect("normal owner drain");
}


#[test]
fn posix_transient_create_routes_precommit_records_to_path_shard() {
    let (runtime, mut service, config) = crate::test_support::current_native_metadata_service(
        "native-transient-create-shard",
        2,
        "/services",
    )
    .expect("normal current runtime");
    drop(service);
    let mut service = runtime
        .metadata_primary_service("/services/transient-create.txt")
        .expect("actual owning Core producer and body inspector");
    let stat = service
        .posix_write_file(
            "/services/transient-create.txt",
            b"path shard transient create",
            DfsWriteOptions::replicated_eventual(vec![service.node_id().clone()]),
        )
        .expect("normal transient creation");
    let shard = DfsMetadataShardId::new(config.shard_id);
    let backend = service.metadata_backend().unwrap();
    // The active-preparation index intentionally excludes terminal bodies.
    // This assertion inspects the actual complete committed record instead.
    let record = backend
        .export_table_snapshot()
        .unwrap()
        .tables
        .prepared_operations_v7
        .into_iter()
        .find(|record| {
            record
                .payload
                .terminal_intent
                .as_ref()
                .is_some_and(|intent| {
                    intent.file_system_id == stat.file_system_id
                        && intent.object_id == stat.object_id
                        && intent.target_version == stat.version
                })
        })
        .expect("complete data publication body");
    record
        .validate()
        .expect("actual immutable body/digest correspondence");
    assert!(
        backend
            .committed_prepared_operation(&record.digest)
            .unwrap()
            .is_some(),
        "the exact exported body must have its actual terminal commit marker"
    );
    assert_eq!(record.payload.shard_id, shard);
    assert_eq!(record.status, DfsPreparedOperationStatus::Committed);
    assert!(!record.payload.content_receipts.is_empty());
    let entries = backend
        .journal_batch_for_shard_since_limited(&shard, 0, usize::MAX, usize::MAX)
        .unwrap()
        .unwrap()
        .entries;
    assert!(entries.iter().any(|entry| matches!(
        &entry.mutation,
        DfsMetadataMutation::PrepareOperationV7 { .. }
    )));
    assert!(entries.iter().any(|entry| matches!(
        &entry.mutation,
        DfsMetadataMutation::CommitPreparedOperationV7 { .. }
    )));
    assert_eq!(
        service.posix_read_file(&stat.path).unwrap(),
        b"path shard transient create"
    );
    drop(service);
    runtime.close().expect("normal owner stopped");
}

#[test]
fn remote_manifest_write_defers_failed_post_commit_fetch() {
    super::repair::assert_native_remote_manifest_fetch_deferral(
        "native-manifest-post-commit-fetch-failure",
        true,
    );
}

#[test]
fn hydrate_from_metadata_backend_does_not_resurrect_aborted_pending_write() {
    let name = "native-hydration-aborted-pending-write";
    let path = "/services/native-hydration-abort.bin";
    let (runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 2, path).unwrap();
    let owner = config.primary_node_id;
    drop(initial);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
    let replica = nodes.iter().find(|node| **node != owner).unwrap().clone();
    let mut base_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    base_policy.required_node_ids = vec![owner.clone()];
    runtime
        .apply_placement_policy(&owner, path, DfsPlacementScope::SelfOnly, base_policy)
        .unwrap();
    let mut main = runtime.client(&owner).unwrap();
    let base = main
        .posix_write_file(
            path,
            b"actual predecessor before abort",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    drop(main);
    let object_id = base.object_id;
    let version_1 = base.version;
    let version_2 = crate::namespace::next_version(&version_1);
    let mut target_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 2, 2).unwrap();
    target_policy.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&owner, path, DfsPlacementScope::SelfOnly, target_policy)
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
    let mut dfs = runtime.metadata_primary_service(path).unwrap();
    let write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"aborted pending bytes".to_vec(),
            vec![dfs.node_id().clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("pending write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    dfs.abort_pending_write(&intent_id).expect("abort");
    assert!(
        !dfs.writes.pending_writes.contains_key(&intent_id),
        "abort should remove in-memory pending write"
    );

    dfs.hydrate_from_metadata_backend().expect("rehydrate");

    let restored_aborted_intent = dfs
        .metadata_backend()
        .expect("metadata backend")
        .get_intent(&object_id, &intent_id)
        .expect("aborted intent lookup")
        .expect("aborted intent remains durable");
    assert_eq!(
        restored_aborted_intent.status,
        DfsWriteIntentStatus::Aborted
    );
    assert!(
        dfs.coordinator().intent(&intent_id).is_none(),
        "closed aborted intents should advance restored ID state without occupying coordinator memory"
    );
    assert_eq!(
        dfs.metadata()
            .object(&object_id)
            .expect("visible object")
            .version,
        version_1
    );
    assert!(
        !dfs.writes.pending_writes.contains_key(&intent_id),
        "rehydration must not resurrect pending state for aborted writes"
    );
    let next_write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner,
            },
            b"aborted pending bytes".to_vec(),
            vec![dfs.node_id().clone(), replica],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("retry pending write");
    let next_intent_id = match next_write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected retry pending write, got {other:?}"),
    };
    assert_ne!(
        next_intent_id, intent_id,
        "hydration must observe closed intent ids so new writes do not reuse them"
    );
    drop(dfs);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn hydrate_from_metadata_backend_keeps_incomplete_new_object_intent_invisible() {
    let name = "native-hydration-invisible-new-write";
    let (mut runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id;
    drop(initial);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
    let replica = nodes.iter().find(|node| **node != owner).unwrap().clone();
    let path = format!("/services/{name}.bin");
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&owner, &path, DfsPlacementScope::Subtree, policy)
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
    let mut writer = runtime.metadata_primary_service(&path).unwrap();
    let bytes = vec![0x67; 2 * 1024 * 1024 + 17];
    let started = writer.posix_write_file(
        &path,
        &bytes,
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(2, 2).unwrap(),
            replica_candidates: nodes,
            wait_for_commit: false,
            foreground_replica_repair: true,
        },
    );
    let (object_id, target_version) = match started {
        Ok(stat) => (stat.object_id, stat.version),
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => {
            let actual = writer
                .coordinator()
                .intent(&intent_id)
                .expect("actual owning producer retained its pending intent");
            (actual.object_id.clone(), actual.target_version.clone())
        }
        other => panic!("actual W2 Stage interruption must retain pending data: {other:?}"),
    };
    let backend = writer.metadata_backend().unwrap();
    let intent = backend
        .write_intents_page(None, 64)
        .unwrap()
        .intents
        .into_iter()
        .find(|intent| intent.object_id == object_id && intent.target_version == target_version)
        .expect("actual hidden native intent");
    assert_ne!(intent.status, DfsWriteIntentStatus::Committed);
    assert!(backend.get_object(&object_id).unwrap().is_none());
    drop(writer);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut restored = runtime.metadata_primary_service(&path).unwrap();
    restored.hydrate_from_metadata_backend().unwrap();
    assert!(restored.metadata().object(&object_id).is_none());
    assert!(
        restored
            .metadata_backend()
            .unwrap()
            .get_object(&object_id)
            .unwrap()
            .is_none()
    );
    assert_ne!(
        restored
            .metadata_backend()
            .unwrap()
            .get_intent(&object_id, &intent.intent_id)
            .unwrap()
            .unwrap()
            .status,
        DfsWriteIntentStatus::Committed
    );
    drop(restored);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn hydrate_from_metadata_backend_restores_staging_pending_write_size_from_local_manifest() {
    let name = "native-hydration-staging-manifest-size";
    let (mut runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id;
    drop(initial);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
    let replica = nodes.iter().find(|node| **node != owner).unwrap().clone();
    let path = "/services/native-hydration-size.bin";
    let mut base_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
    base_policy.required_node_ids = vec![owner.clone()];
    runtime
        .apply_placement_policy(&owner, path, DfsPlacementScope::Subtree, base_policy)
        .unwrap();
    let mut writer = runtime.client(&owner).unwrap();
    let base = writer
        .posix_write_file(
            path,
            b"actual predecessor",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    drop(writer);
    let mut current_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    current_policy.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&owner, path, DfsPlacementScope::Subtree, current_policy)
        .unwrap();
    let mut writer = runtime.metadata_primary_service(path).unwrap();
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
    let bytes = vec![0x68; 2 * 1024 * 1024 + 37];
    let target = crate::namespace::next_version(&base.version);
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id.clone(),
        target_version: target.clone(),
        requesting_node_id: owner.clone(),
    };
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    let outcome = writer
        .write_distributed_reader_with_candidates(
            request,
            std::io::Cursor::new(&bytes),
            nodes,
            policy,
            DfsDistributedWriteOptions::default(),
        )
        .unwrap();
    let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = outcome else {
        panic!("actual denied Stage keeps W2 pending")
    };
    assert!(
        writer
            .metadata_backend()
            .unwrap()
            .get_version(&base.object_id, &target)
            .unwrap()
            .is_none()
    );
    let manifest = writer
        .content()
        .read_manifest(base.object_id.as_str(), target.as_str())
        .unwrap();
    assert_eq!(manifest.size_bytes, bytes.len() as u64);
    drop(writer);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut restored = runtime.metadata_primary_service(path).unwrap();
    restored.hydrate_from_metadata_backend().unwrap();
    let pending = restored.writes.pending_writes.get(&intent_id).unwrap();
    assert_eq!(pending.size_bytes, manifest.size_bytes);
    assert_eq!(pending.content_hash, manifest.content_hash);
    assert_eq!(
        restored
            .content()
            .read_object_version(base.object_id.as_str(), target.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(restored);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn pending_write_size_hint_ignores_backend_version_with_wrong_hash() {
    let node = NodeId::generate();
    let (runtime, mut service) = native_sifs_service("native-size-hash-binding", node.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![node]);
    let first = service
        .posix_write_file("/services/size-first", b"first", options.clone())
        .unwrap();
    let second = service
        .posix_write_file(
            "/services/size-second",
            b"unrelated longer content",
            options,
        )
        .unwrap();
    assert_eq!(
        service
            .pending_write_size_hint(
                &first.object_id,
                &first.version,
                first.content_hash.as_deref().unwrap()
            )
            .unwrap(),
        first.size_bytes
    );
    assert_eq!(
        service
            .pending_write_size_hint(
                &first.object_id,
                &first.version,
                second.content_hash.as_deref().unwrap()
            )
            .unwrap(),
        None,
        "an unrelated durable blob cannot borrow a different object's bound metadata or manifest size"
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn pending_write_size_hint_uses_verified_hash_addressed_blob_without_manifest() {
    let root = temp_root("pending-size-uses-hash-addressed-blob");
    let owner = NodeId::from_legacy_named("node_size_hint_blob_owner").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_size_hint_hash_blob").expect("object");
    let version = DfsVersion::from_legacy_named("version_size_hint_hash_blob").expect("version");
    let bytes = b"pending content retained only as a hash-addressed blob";
    let (_runtime, dfs) = native_sifs_service(
        "pending_write_size_hint_uses_verified_hash_addressed_blob_without_manifest",
        owner,
    );
    let manifest = dfs
        .content
        .put_object_version_blob_only(object_id.as_str(), version.as_str(), bytes)
        .expect("hash-addressed blob");

    assert!(
        dfs.content
            .read_manifest(object_id.as_str(), version.as_str())
            .is_err(),
        "the regression requires a collected per-version manifest"
    );
    assert_eq!(
        dfs.pending_write_size_hint(&object_id, &version, &manifest.content_hash)
            .expect("size hint"),
        Some(bytes.len() as u64)
    );

    std::fs::remove_dir_all(root).ok();
}

fn native_owned_pending_write(
    name: &str,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NodeId,
    NodeId,
    NetworkedDfsService<DfsNodeTransport>,
    DfsWriteRequest,
    Vec<u8>,
    String,
) {
    let (runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id;
    drop(initial);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
    let replica = nodes.iter().find(|node| **node != owner).unwrap().clone();
    let path = format!("/services/{name}.bin");
    let mut base_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
    base_policy.required_node_ids = vec![owner.clone()];
    runtime
        .apply_placement_policy(&owner, &path, DfsPlacementScope::Subtree, base_policy)
        .unwrap();
    let mut writer = runtime.metadata_primary_service(&path).unwrap();
    let base = writer
        .posix_write_file(
            &path,
            b"actual manual-fixture predecessor",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    drop(writer);
    let mut target_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    target_policy.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&owner, &path, DfsPlacementScope::Subtree, target_policy)
        .unwrap();
    let mut writer = runtime.metadata_primary_service(&path).unwrap();
    let bytes = vec![0x7b; 4096];
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: owner.clone(),
    };
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    let outcome = writer
        .write_distributed_reader_with_candidates(
            request.clone(),
            std::io::Cursor::new(&bytes),
            nodes,
            policy,
            DfsDistributedWriteOptions::default(),
        )
        .unwrap();
    let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = outcome else {
        panic!("real queued Stage ACK must await its ordinary consumer")
    };
    (runtime, owner, replica, writer, request, bytes, intent_id)
}

#[test]
fn committed_pending_write_cleanup_drops_stale_stage_inflight_state() {
    let (runtime, _, replica, mut writer, request, bytes, intent) =
        native_owned_pending_write("native-committed-stage-cleanup");
    assert!(
        !writer.transport.delegates_local_namespace_authority(),
        "Stage cleanup must inspect the actual Core coordinator owner"
    );
    let pending = writer.writes.pending_writes.get(&intent).unwrap();
    assert!(
        pending.stage_inflight_request_ids.contains_key(&replica),
        "normal producer owns an actual native Stage request"
    );
    writer.process_incoming().unwrap();
    assert!(
        matches!(writer.write_outcome(&intent).unwrap(), NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } if repair_jobs.is_empty())
    );
    assert!(writer.coordinator().repair_jobs().is_empty());
    writer.cleanup_committed_pending_write_if_repaired(&intent);
    assert!(
        !writer.writes.pending_writes.contains_key(&intent),
        "normal fully-repaired receipt completion clears every obsolete Stage reservation"
    );
    let peer = runtime.client(&replica).unwrap();
    assert_eq!(
        peer.content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(peer);
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn write_outcome_uses_backend_current_metadata_after_pending_state_is_removed() {
    let (runtime, _, _, mut writer, request, bytes, intent) =
        native_owned_pending_write("native-outcome-backend-after-cleanup");
    writer.process_incoming().unwrap();
    assert!(!writer.writes.pending_writes.contains_key(&intent));
    let actual = writer
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(actual.version, request.target_version);
    // Ordinary transient-view withdrawal is a negative cache eviction only.
    // Durable metadata and the actual producer's committed coordinator survive.
    writer.remove_transient_object_metadata(&request.object_id);
    assert!(writer.metadata().object(&request.object_id).is_none());
    assert_eq!(
        writer
            .metadata_backend()
            .unwrap()
            .get_object(&request.object_id)
            .unwrap()
            .unwrap(),
        actual
    );
    assert!(
        matches!(writer.write_outcome(&intent).unwrap(), NetworkedDfsWriteOutcome::Committed { version, content_hash: hash, .. }
        if version == request.target_version && hash == content_hash(&bytes))
    );
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn commit_version_applies_when_response_target_is_unroutable() {
    let name = "native-commit-lost-real-reply";
    let (runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id.clone();
    drop(initial);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
    let replica = nodes.iter().find(|node| **node != owner).unwrap().clone();
    let path = format!("/services/{name}.bin");
    let mut placement =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    placement.required_node_ids = nodes.clone();
    let accepted = runtime
        .apply_placement_policy(&owner, &path, DfsPlacementScope::SelfOnly, placement)
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let read_apply = || {
        control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .unwrap()
            .expect("the exact native empty-path Apply job")
    };
    let setup_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    // Native maintenance can defer completion across turns; wait for this
    // exact job within the original deadline rather than assuming two ticks.
    loop {
        let current = read_apply();
        assert_eq!(current.accepted_config, accepted.accepted_config);
        assert_eq!(current.path, path);
        assert_eq!(current.scanned_object_count, 0);
        assert_eq!(current.scheduled_repair_count, 0);
        assert!(current.last_error.is_none());
        if current.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        assert!(matches!(
            current.state,
            DfsPlacementReconciliationState::Pending
                | DfsPlacementReconciliationState::Scanning
                | DfsPlacementReconciliationState::Repairing
        ));
        let remaining = setup_deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero());
        runtime
            .maintenance_tick(&owner, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
    assert_eq!(
        read_apply().state,
        DfsPlacementReconciliationState::Completed
    );
    assert!(Instant::now() < setup_deadline);
    drop(control);
    let mut browser = runtime.client(&owner).unwrap();
    let base = browser
        .posix_write_file(
            &path,
            b"actual manual-fixture predecessor",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    drop(browser);
    let mut writer = runtime.metadata_primary_service(&path).unwrap();
    writer.flush_detached_transport_sends().unwrap();
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id.clone(),
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: owner.clone(),
    };
    let bytes = vec![0x7b; 4096];
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    let outcome = writer
        .write_distributed_reader_with_candidates(
            request.clone(),
            std::io::Cursor::new(&bytes),
            nodes,
            policy,
            DfsDistributedWriteOptions::default(),
        )
        .unwrap();
    let NetworkedDfsWriteOutcome::Pending {
        intent_id: intent, ..
    } = outcome
    else {
        panic!("the actual owning Core has not consumed its genuine Stage reply")
    };
    let pending = writer
        .writes
        .pending_writes
        .get(&intent)
        .expect("actual producer's pending body");
    assert_eq!(pending.version, request.target_version);
    assert!(pending.stage_inflight_request_ids.contains_key(&replica));
    let receiver = runtime.capture_served_work(&replica, 32).unwrap();
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(crate::DFS_DATA_COMMIT_VERSION_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::LoseReplyAfterDispatch,
    });
    let before = runtime.rpc_observations().len();
    writer.process_incoming().unwrap();
    assert!(matches!(
        writer.write_outcome(&intent).unwrap(),
        NetworkedDfsWriteOutcome::Committed { .. }
    ));
    let delivery = writer.flush_detached_transport_sends();
    assert!(
        matches!(
            &delivery,
            Ok(())
                | Err(DfsError::NetworkDelivery {
                    delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                    ..
                })
        ),
        "unexpected outbox result: {delivery:?}"
    );
    let observed = runtime.rpc_observations();
    let attempts = observed[before..]
        .iter()
        .filter(|event| {
            event.source == owner
                && event.target == replica
                && event.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::EndedWithoutResponse
        })
        .collect::<Vec<_>>();
    assert_eq!(
        attempts.len(),
        1,
        "the original producer's selected native notification must fail once"
    );
    let call_id = attempts[0].call_id;
    assert!(
        observed[before..]
            .iter()
            .any(|event| event.call_id == call_id
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch)
    );
    let committed = writer
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(committed.version, request.target_version);
    assert_eq!(
        committed.content_hash.as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    let captured = receiver.records();
    let delivered = captured
        .iter()
        .filter_map(|record| match &record.command.routed_message.message {
            DfsNetworkMessage::CommitVersion(command)
                if command.intent_id == intent
                    && command.object_id == request.object_id
                    && command.version == request.target_version =>
            {
                assert_eq!(record.command.routed_message.source_node_id, owner);
                assert_eq!(record.command.routed_message.target_node_id, replica);
                assert_eq!(command.file_system_id, request.file_system_id);
                assert_eq!(command.content_hash, content_hash(&bytes));
                assert_eq!(command.object_metadata.as_ref(), Some(&committed));
                assert_eq!(command.authority_shard.as_ref(), Some(&config));
                assert!(record.succeeded);
                Some(command.clone())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        delivered.len(),
        1,
        "the genuine receiver executed before its reply was lost"
    );
    let removed = runtime.remove_faults_matching(|rule| {
        rule.source.as_ref() == Some(&owner)
            && rule.target.as_ref() == Some(&replica)
            && rule.route.as_deref() == Some(crate::DFS_DATA_COMMIT_VERSION_ROUTE)
            && rule.call_id.is_none()
            && rule.origin == Some(crate::test_runtime::RpcOrigin::RegisteredHost)
            && rule.remaining_hits == 0
            && matches!(
                &rule.action,
                crate::test_runtime::FaultAction::LoseReplyAfterDispatch
            )
    });
    assert_eq!(
        removed, 1,
        "remove only this verified native notification fault"
    );
    runtime.assert_fault_budgets().unwrap();
    let acknowledged = Arc::new(Mutex::new(Vec::<DfsRoutedMessage>::new()));
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(crate::DFS_DATA_COMMIT_VERSION_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::CaptureCommitReplyAfterDispatch(Arc::clone(
            &acknowledged,
        )),
    });
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let original_receipts = writer
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&request.object_id, &request.target_version)
        .unwrap();
    let original_write = writer
        .metadata_backend()
        .unwrap()
        .export_table_snapshot()
        .unwrap()
        .tables
        .prepared_operations_v7
        .into_iter()
        .find(|record| {
            record.terminal_intent.as_ref().is_some_and(|terminal| {
                terminal.intent_id == intent
                    && terminal.object_id == request.object_id
                    && terminal.target_version == request.target_version
            })
        })
        .expect("actual committed immutable write body");
    original_write.validate().unwrap();
    assert_eq!(original_write.status, DfsPreparedOperationStatus::Committed);
    let target = writer
        .capture_metadata_quorum_sync_target(&committed)
        .unwrap();
    let certificate = writer
        .sync_metadata_shard_to_quorum(&target, deadline.saturating_duration_since(Instant::now()))
        .unwrap();
    assert_eq!(
        certificate
            .acknowledged_node_ids
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([owner.clone(), replica.clone()])
    );
    // The failed original private outbox cools for five seconds. End that
    // completed producer normally, then exercise the existing recovery entry
    // from its actual durable intent/receipts rather than resetting that clock.
    drop(writer);
    let mut writer = runtime.metadata_primary_service(&path).unwrap();
    writer
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .unwrap();
    assert_eq!(
        writer
            .metadata_backend()
            .unwrap()
            .get_object(&request.object_id)
            .unwrap(),
        Some(committed.clone())
    );
    assert_eq!(
        writer
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&request.object_id, &request.target_version)
            .unwrap(),
        original_receipts
    );
    assert_eq!(
        writer
            .metadata_backend()
            .unwrap()
            .prepared_operation_record(&original_write.operation_id)
            .unwrap(),
        Some(original_write.clone())
    );
    loop {
        if Instant::now() >= deadline {
            break;
        }
        writer.flush_detached_transport_sends().unwrap();
        if acknowledged.lock().unwrap().iter().any(|routed| matches!(&routed.message,
            DfsNetworkMessage::CommitVersionResponse(response) if response.intent_id == intent
                && response.object_id == request.object_id && response.version == request.target_version
                && matches!(&response.result, DfsBlobTransferResult::Committed { content_hash: hash }
                    if hash == &content_hash(&bytes)))) { break; }
        std::thread::yield_now();
    }
    let recovered_reports = receiver.records();
    let recovered_replies = acknowledged.lock().unwrap().clone();
    let custody = serde_json::json!({
        "intent_id": intent,
        "write_request": request,
        "committed_object": committed,
        "original_write": original_write,
        "original_receipts": original_receipts,
        "original_delivered_commands": delivered,
        "recovered_reports": recovered_reports.iter().map(|report| serde_json::json!({
            "command": report.command, "succeeded": report.succeeded,
        })).collect::<Vec<_>>(),
        "recovered_replies": recovered_replies,
        "completed_within_original_deadline": Instant::now() < deadline,
    });
    std::fs::write(
        runtime
            .data_dir(&owner)
            .root()
            .join("notification-recovery-custody.json"),
        serde_json::to_vec(&custody).unwrap(),
    )
    .unwrap();
    assert!(Instant::now() < deadline);
    let replies = acknowledged
        .lock()
        .unwrap()
        .iter()
        .filter(|routed| {
            matches!(&routed.message,
        DfsNetworkMessage::CommitVersionResponse(response) if response.intent_id == intent
            && response.object_id == request.object_id && response.version == request.target_version
            && matches!(&response.result, DfsBlobTransferResult::Committed { content_hash: hash }
                if hash == &content_hash(&bytes)))
        })
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(replies.len(), 1);
    let DfsNetworkMessage::CommitVersionResponse(response) = &replies[0].message else {
        unreachable!()
    };
    assert_eq!(replies[0].source_node_id, replica);
    assert_eq!(replies[0].target_node_id, owner);
    assert_eq!(response.intent_id, intent);
    assert_eq!(response.file_system_id, request.file_system_id);
    assert_eq!(response.object_id, request.object_id);
    assert_eq!(response.version, request.target_version);
    assert_eq!(response.replica_node_id, replica);
    assert!(
        matches!(&response.result, DfsBlobTransferResult::Committed { content_hash: hash }
        if hash == &content_hash(&bytes))
    );
    let successful = receiver
        .records()
        .into_iter()
        .filter_map(|record| match record.command.routed_message.message {
            DfsNetworkMessage::CommitVersion(command)
                if command.request_id == response.request_id =>
            {
                Some(command)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(!successful.is_empty());
    assert!(successful.iter().all(|command| command == &successful[0]));
    // A new recovery producer owns a new transport request ID. The durable
    // write identity and every semantic notification field remain original.
    assert_eq!(successful[0].intent_id, delivered[0].intent_id);
    assert_eq!(successful[0].file_system_id, delivered[0].file_system_id);
    assert_eq!(successful[0].object_id, delivered[0].object_id);
    assert_eq!(
        successful[0].expected_base_version,
        delivered[0].expected_base_version
    );
    assert_eq!(successful[0].version, delivered[0].version);
    assert_eq!(successful[0].content_hash, delivered[0].content_hash);
    assert_eq!(successful[0].object_metadata, delivered[0].object_metadata);
    assert_eq!(successful[0].authority_shard, delivered[0].authority_shard);
    assert_eq!(runtime.remove_faults_matching(|rule| {
        rule.source.as_ref() == Some(&owner) && rule.target.as_ref() == Some(&replica)
            && rule.route.as_deref() == Some(crate::DFS_DATA_COMMIT_VERSION_ROUTE)
            && matches!(&rule.action, crate::test_runtime::FaultAction::CaptureCommitReplyAfterDispatch(actual)
                if Arc::ptr_eq(actual, &acknowledged))
    }), 1);
    assert_eq!(
        writer
            .metadata_backend()
            .unwrap()
            .get_object(&request.object_id)
            .unwrap(),
        Some(committed.clone())
    );
    let actual = runtime.client(&replica).unwrap();
    assert_eq!(
        actual
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        bytes
    );
    assert_eq!(
        runtime
            .node_metadata(&replica)
            .unwrap()
            .get_object(&request.object_id)
            .unwrap()
            .unwrap()
            .version,
        request.target_version
    );
    drop(actual);
    drop(receiver);
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn committed_intent_applies_when_commit_notification_target_is_unroutable() {
    let name = "native-commit-unsent-real-notification";
    let (runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id.clone();
    drop(initial);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
    let replica = nodes.iter().find(|node| **node != owner).unwrap().clone();
    let path = format!("/services/{name}.bin");
    let mut placement =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    placement.required_node_ids = nodes.clone();
    let accepted = runtime
        .apply_placement_policy(&owner, &path, DfsPlacementScope::SelfOnly, placement)
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let read_apply = || {
        control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .unwrap()
            .expect("the exact native empty-path Apply job")
    };
    let setup_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    // Native maintenance can defer completion across turns; wait for this
    // exact job within the original deadline rather than assuming two ticks.
    loop {
        let current = read_apply();
        assert_eq!(current.accepted_config, accepted.accepted_config);
        assert_eq!(current.path, path);
        assert_eq!(current.scanned_object_count, 0);
        assert_eq!(current.scheduled_repair_count, 0);
        assert!(current.last_error.is_none());
        if current.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        assert!(matches!(
            current.state,
            DfsPlacementReconciliationState::Pending
                | DfsPlacementReconciliationState::Scanning
                | DfsPlacementReconciliationState::Repairing
        ));
        let remaining = setup_deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero());
        runtime
            .maintenance_tick(&owner, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
    assert_eq!(
        read_apply().state,
        DfsPlacementReconciliationState::Completed
    );
    assert!(Instant::now() < setup_deadline);
    drop(control);
    let mut browser = runtime.client(&owner).unwrap();
    let base = browser
        .posix_write_file(
            &path,
            b"actual manual-fixture predecessor",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    drop(browser);
    let mut writer = runtime.metadata_primary_service(&path).unwrap();
    writer.flush_detached_transport_sends().unwrap();
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id.clone(),
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: owner.clone(),
    };
    let bytes = vec![0x7b; 4096];
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    let outcome = writer
        .write_distributed_reader_with_candidates(
            request.clone(),
            std::io::Cursor::new(&bytes),
            nodes,
            policy,
            DfsDistributedWriteOptions::default(),
        )
        .unwrap();
    let NetworkedDfsWriteOutcome::Pending {
        intent_id: intent, ..
    } = outcome
    else {
        panic!("the actual owning Core has not consumed its genuine Stage reply")
    };
    let pending = writer
        .writes
        .pending_writes
        .get(&intent)
        .expect("actual producer's pending body");
    assert_eq!(pending.version, request.target_version);
    assert!(pending.stage_inflight_request_ids.contains_key(&replica));
    let receiver = runtime.capture_served_work(&replica, 32).unwrap();
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(crate::DFS_DATA_COMMIT_VERSION_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::BudgetExhausted,
    });
    let before = runtime.rpc_observations().len();
    writer.process_incoming().unwrap();
    assert!(matches!(
        writer.write_outcome(&intent).unwrap(),
        NetworkedDfsWriteOutcome::Committed { .. }
    ));
    let delivery = writer.flush_detached_transport_sends();
    assert!(
        matches!(
            &delivery,
            Ok(())
                | Err(DfsError::NetworkDelivery {
                    delivery_state: sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                    ..
                })
        ),
        "unexpected outbox result: {delivery:?}"
    );
    let observed = runtime.rpc_observations();
    let attempts = observed[before..]
        .iter()
        .filter(|event| {
            event.source == owner
                && event.target == replica
                && event.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::EndedWithoutResponse
        })
        .collect::<Vec<_>>();
    assert_eq!(
        attempts.len(),
        1,
        "the original producer's selected native notification must fail once"
    );
    let call_id = attempts[0].call_id;
    assert!(
        observed[before..]
            .iter()
            .any(|event| event.call_id == call_id
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch)
    );
    let committed = writer
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(committed.version, request.target_version);
    assert_eq!(
        committed.content_hash.as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    let captured = receiver.records();
    let delivered = captured
        .iter()
        .filter_map(|record| match &record.command.routed_message.message {
            DfsNetworkMessage::CommitVersion(command)
                if command.intent_id == intent
                    && command.object_id == request.object_id
                    && command.version == request.target_version =>
            {
                assert_eq!(record.command.routed_message.source_node_id, owner);
                assert_eq!(record.command.routed_message.target_node_id, replica);
                assert_eq!(command.file_system_id, request.file_system_id);
                assert_eq!(command.content_hash, content_hash(&bytes));
                assert_eq!(command.object_metadata.as_ref(), Some(&committed));
                assert_eq!(command.authority_shard.as_ref(), Some(&config));
                assert!(record.succeeded);
                Some(command.clone())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        delivered.is_empty(),
        "the denied original call never reached the real receiver"
    );
    let removed = runtime.remove_faults_matching(|rule| {
        rule.source.as_ref() == Some(&owner)
            && rule.target.as_ref() == Some(&replica)
            && rule.route.as_deref() == Some(crate::DFS_DATA_COMMIT_VERSION_ROUTE)
            && rule.call_id.is_none()
            && rule.origin == Some(crate::test_runtime::RpcOrigin::RegisteredHost)
            && rule.remaining_hits == 0
            && matches!(
                &rule.action,
                crate::test_runtime::FaultAction::BudgetExhausted
            )
    });
    assert_eq!(
        removed, 1,
        "remove only this verified native notification fault"
    );
    runtime.assert_fault_budgets().unwrap();
    let acknowledged = Arc::new(Mutex::new(Vec::<DfsRoutedMessage>::new()));
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(crate::DFS_DATA_COMMIT_VERSION_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::CaptureCommitReplyAfterDispatch(Arc::clone(
            &acknowledged,
        )),
    });
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let original_receipts = writer
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&request.object_id, &request.target_version)
        .unwrap();
    let original_write = writer
        .metadata_backend()
        .unwrap()
        .export_table_snapshot()
        .unwrap()
        .tables
        .prepared_operations_v7
        .into_iter()
        .find(|record| {
            record.terminal_intent.as_ref().is_some_and(|terminal| {
                terminal.intent_id == intent
                    && terminal.object_id == request.object_id
                    && terminal.target_version == request.target_version
            })
        })
        .expect("actual committed immutable write body");
    original_write.validate().unwrap();
    assert_eq!(original_write.status, DfsPreparedOperationStatus::Committed);
    let target = writer
        .capture_metadata_quorum_sync_target(&committed)
        .unwrap();
    let certificate = writer
        .sync_metadata_shard_to_quorum(&target, deadline.saturating_duration_since(Instant::now()))
        .unwrap();
    assert_eq!(
        certificate
            .acknowledged_node_ids
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([owner.clone(), replica.clone()])
    );
    // The failed original private outbox cools for five seconds. End that
    // completed producer normally, then exercise the existing recovery entry
    // from its actual durable intent/receipts rather than resetting that clock.
    drop(writer);
    let mut writer = runtime.metadata_primary_service(&path).unwrap();
    writer
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .unwrap();
    assert_eq!(
        writer
            .metadata_backend()
            .unwrap()
            .get_object(&request.object_id)
            .unwrap(),
        Some(committed.clone())
    );
    assert_eq!(
        writer
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&request.object_id, &request.target_version)
            .unwrap(),
        original_receipts
    );
    assert_eq!(
        writer
            .metadata_backend()
            .unwrap()
            .prepared_operation_record(&original_write.operation_id)
            .unwrap(),
        Some(original_write.clone())
    );
    loop {
        if Instant::now() >= deadline {
            break;
        }
        writer.flush_detached_transport_sends().unwrap();
        if acknowledged.lock().unwrap().iter().any(|routed| matches!(&routed.message,
            DfsNetworkMessage::CommitVersionResponse(response) if response.intent_id == intent
                && response.object_id == request.object_id && response.version == request.target_version
                && matches!(&response.result, DfsBlobTransferResult::Committed { content_hash: hash }
                    if hash == &content_hash(&bytes)))) { break; }
        std::thread::yield_now();
    }
    let recovered_reports = receiver.records();
    let recovered_replies = acknowledged.lock().unwrap().clone();
    let custody = serde_json::json!({
        "intent_id": intent,
        "write_request": request,
        "committed_object": committed,
        "original_write": original_write,
        "original_receipts": original_receipts,
        "original_delivered_commands": delivered,
        "recovered_reports": recovered_reports.iter().map(|report| serde_json::json!({
            "command": report.command, "succeeded": report.succeeded,
        })).collect::<Vec<_>>(),
        "recovered_replies": recovered_replies,
        "completed_within_original_deadline": Instant::now() < deadline,
    });
    std::fs::write(
        runtime
            .data_dir(&owner)
            .root()
            .join("notification-recovery-custody.json"),
        serde_json::to_vec(&custody).unwrap(),
    )
    .unwrap();
    assert!(Instant::now() < deadline);
    let replies = acknowledged
        .lock()
        .unwrap()
        .iter()
        .filter(|routed| {
            matches!(&routed.message,
        DfsNetworkMessage::CommitVersionResponse(response) if response.intent_id == intent
            && response.object_id == request.object_id && response.version == request.target_version
            && matches!(&response.result, DfsBlobTransferResult::Committed { content_hash: hash }
                if hash == &content_hash(&bytes)))
        })
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(replies.len(), 1);
    let DfsNetworkMessage::CommitVersionResponse(response) = &replies[0].message else {
        unreachable!()
    };
    assert_eq!(replies[0].source_node_id, replica);
    assert_eq!(replies[0].target_node_id, owner);
    assert_eq!(response.intent_id, intent);
    assert_eq!(response.file_system_id, request.file_system_id);
    assert_eq!(response.object_id, request.object_id);
    assert_eq!(response.version, request.target_version);
    assert_eq!(response.replica_node_id, replica);
    assert!(
        matches!(&response.result, DfsBlobTransferResult::Committed { content_hash: hash }
        if hash == &content_hash(&bytes))
    );
    let successful = receiver
        .records()
        .into_iter()
        .filter_map(|record| match record.command.routed_message.message {
            DfsNetworkMessage::CommitVersion(command)
                if command.request_id == response.request_id =>
            {
                Some(command)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(!successful.is_empty());
    assert!(successful.iter().all(|command| command == &successful[0]));
    assert_eq!(
        successful[0].expected_base_version,
        Some(base.version.clone())
    );
    assert_eq!(runtime.remove_faults_matching(|rule| {
        rule.source.as_ref() == Some(&owner) && rule.target.as_ref() == Some(&replica)
            && rule.route.as_deref() == Some(crate::DFS_DATA_COMMIT_VERSION_ROUTE)
            && matches!(&rule.action, crate::test_runtime::FaultAction::CaptureCommitReplyAfterDispatch(actual)
                if Arc::ptr_eq(actual, &acknowledged))
    }), 1);
    assert_eq!(
        writer
            .metadata_backend()
            .unwrap()
            .get_object(&request.object_id)
            .unwrap(),
        Some(committed.clone())
    );
    let actual = runtime.client(&replica).unwrap();
    assert_eq!(
        actual
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        bytes
    );
    assert_eq!(
        runtime
            .node_metadata(&replica)
            .unwrap()
            .get_object(&request.object_id)
            .unwrap()
            .unwrap()
            .version,
        request.target_version
    );
    drop(actual);
    drop(receiver);
    drop(writer);
    runtime.close().unwrap();
}



#[test]
fn replica_stage_and_commit_do_not_append_authoritative_metadata() {
    fn metadata(
        data_dir: &sinomo_core::local::SinomoDataDir,
        shard_id: &DfsMetadataShardId,
    ) -> LocalSobjMetadataBackend {
        LocalSobjMetadataBackend::open_for_inspection(
            shard_id.clone(),
            crate::default_dfs_metadata_shards_root(data_dir).join(&shard_id.0),
        )
        .expect("read-only actual replica metadata")
    }
    fn complete_journal_tail(
        backend: &LocalSobjMetadataBackend,
        materialized: u64,
    ) -> Vec<crate::DfsMetadataJournalEntry> {
        let tail = backend
            .journal_since_limited(materialized, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT)
            .unwrap();
        assert!(
            tail.len() < DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            "the complete native effect observation must not omit pending journal entries"
        );
        tail
    }
    fn prefix(
        data_dir: &sinomo_core::local::SinomoDataDir,
        shard_id: &DfsMetadataShardId,
    ) -> (u64, DfsMetadataPrefixProof) {
        let backend = metadata(data_dir, shard_id);
        let materialized = backend
            .export_table_snapshot()
            .materialized_through_sequence;
        let tail = complete_journal_tail(&backend, materialized);
        (
            tail.last().map_or(materialized, |entry| entry.sequence),
            backend.metadata_prefix_proof().unwrap(),
        )
    }
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct ReplicaState {
        prefix: (u64, DfsMetadataPrefixProof),
        tables: DfsMetadataTableSnapshot,
        journal_tail: Vec<crate::DfsMetadataJournalEntry>,
        storage_generation: u64,
        publish_requests: usize,
    }
    fn state(
        data_dir: &sinomo_core::local::SinomoDataDir,
        lane: &DfsMutationLaneKey,
    ) -> ReplicaState {
        let backend = metadata(data_dir, &lane.shard_id);
        let tables = backend.export_table_snapshot();
        // ACKed native entries may await the asynchronous table materializer.
        // Capture the complete bounded durable tail under this actual receiver
        // admission as well; observing it neither applies nor authors entries.
        let journal_tail = complete_journal_tail(&backend, tables.materialized_through_sequence);
        ReplicaState {
            prefix: (
                journal_tail
                    .last()
                    .map_or(tables.materialized_through_sequence, |entry| entry.sequence),
                backend.metadata_prefix_proof().unwrap(),
            ),
            tables,
            journal_tail,
            storage_generation:
                crate::runtime_host::daemon_dfs_storage_environment_dirty_generation(data_dir),
            publish_requests: crate::runtime_host::daemon_dfs_metadata_publish_lane_request_count(
                data_dir, lane,
            ),
        }
    }
    #[derive(Clone)]
    struct Report {
        command: DfsMessageCommand,
        work: DfsMessageCommittedWork,
        succeeded: bool,
    }
    #[derive(Clone)]
    struct CommitObservation {
        command: DfsMessageCommand,
        before: ReplicaState,
        after: Option<ReplicaState>,
    }
    struct Release(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    let name = "native-replica-stage-commit-no-authoring";
    let (mut runtime, initial, shard) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    assert_eq!(
        shard.replica_node_ids.len(),
        2,
        "three actual metadata members"
    );
    let owner = shard.primary_node_id.clone();
    let replica = shard.replica_node_ids.last().unwrap().clone();
    assert_ne!(owner, replica);
    drop(initial);
    let path = "/services/native-replica-cache.bin";
    let content_nodes = vec![owner.clone(), replica.clone()];
    let mut placement =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    placement.required_node_ids = content_nodes.clone();
    runtime
        .apply_placement_policy(&owner, path, DfsPlacementScope::Subtree, placement)
        .unwrap();
    let mut writer = runtime.metadata_primary_service(path).unwrap();
    // This native client supplies actual content-store inspection only.
    let observer = runtime.client(&replica).unwrap();
    let replica_data = runtime.data_dir(&replica).clone();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let before_prefix = prefix(&replica_data, &shard_id);
    let lane_key = DfsMutationLaneKey {
        file_system_id: shard.file_system_id.clone(),
        shard_id: shard_id.clone(),
    };
    let reports = Arc::new(Mutex::new(Vec::<Report>::new()));
    let commits = Arc::new(Mutex::new(Vec::<CommitObservation>::new()));
    let observed_reports = Arc::clone(&reports);
    let observed_commits = Arc::clone(&commits);
    let observed_data = replica_data.clone();
    let observed_lane = lane_key.clone();
    let source_node = owner.clone();
    let target_node = replica.clone();
    // Use the existing actual Main hook for both work reports and the exact
    // Commit effect's physical boundary. This supplies no execution authority.
    let hook: Arc<crate::runtime_host::DaemonDfsPreDispatchTestHook> =
        Arc::new(move |point, command, _preflight| {
            let Some(command) = command else {
                return Ok(());
            };
            if command.routed_message.source_node_id != source_node
                || command.routed_message.target_node_id != target_node
                || !matches!(
                    &command.routed_message.message,
                    DfsNetworkMessage::StageBlob(_) | DfsNetworkMessage::CommitVersion(_)
                )
            {
                return Ok(());
            }
            match point {
                crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeExecute {
                    admission_held,
                } => {
                    assert!(
                        admission_held,
                        "actual Commit receiver retains its native admission"
                    );
                    let mut captured = observed_commits.lock().unwrap();
                    assert!(captured.len() < 32, "bounded actual Commit capture");
                    captured.push(CommitObservation {
                        command: command.clone(),
                        before: state(&observed_data, &observed_lane),
                        after: None,
                    });
                }
                crate::runtime_host::DaemonDfsPreDispatchTestPoint::AfterReport {
                    committed_work,
                    succeeded,
                    ..
                } => {
                    {
                        let mut captured = observed_commits.lock().unwrap();
                        let observation = captured
                            .iter_mut()
                            .rev()
                            .find(|observation| {
                                observation.command == *command && observation.after.is_none()
                            })
                            .expect("same actual Commit has a BeforeExecute observation");
                        observation.after = Some(state(&observed_data, &observed_lane));
                    }
                    let mut captured = observed_reports.lock().unwrap();
                    assert!(captured.len() < 32, "bounded actual Main work capture");
                    captured.push(Report {
                        command: command.clone(),
                        work: committed_work,
                        succeeded,
                    });
                }
                _ => {}
            }
            Ok(())
        });
    let capture =
        crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(&replica_data, hook);
    // Hidden source prepares also require every metadata participant. Observe
    // each replica effect under its actual admission instead of denying that
    // legitimate primary journal before the Create boundary can be reached.
    let committed_replies = Arc::new(Mutex::new(Vec::new()));
    let bytes = vec![0x63; 2 * 1024 * 1024 + 4096];
    let expected_hash = content_hash(&bytes);
    let (_, _, replication) = writer
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedConfirmed, 2)
        .unwrap();
    assert_eq!(replication.min_commit_replicas, 2);
    assert_eq!(replication.desired_replicas, 2);
    assert_eq!(
        replication
            .required_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        content_nodes.iter().cloned().collect(),
        "the actual Stage receiver must be required by this fixture's effective policy"
    );
    let options = DfsWriteOptions {
        policy: replication,
        replica_candidates: content_nodes,
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let (captured_tx, captured_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (writer, payload, stage) = std::thread::scope(|scope| {
        // Unwinding any paused-phase assertion releases only this test gate;
        // the scoped source thread then joins before runtime teardown.
        let release = Release(Some(release_tx));
        let source_bytes = bytes.clone();
        let publisher = scope.spawn(move || {
            crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| {
                assert!(hook.borrow().is_none());
                *hook.borrow_mut() = Some(Box::new(move |payload| {
                    captured_tx.send(payload.clone()).unwrap();
                    let _ = release_rx.recv();
                }));
            });
            let result = writer.posix_write_file(path, &source_bytes, options);
            crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| {
                hook.borrow_mut().take();
            });
            let flushed = writer.flush_detached_transport_sends();
            (writer, result, flushed)
        });
        let payload = captured_rx
            .recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .expect("genuine W2 Stage durability before the source's Create prepare");
        payload.validate().unwrap();
        let intent = payload.prepared_intent.as_ref().unwrap();
        let stages = reports
            .lock().unwrap().clone()
            .into_iter()
            .filter(|row| matches!(&row.command.routed_message.message,
                DfsNetworkMessage::StageBlob(command) if command.object_id == intent.object_id
                    && command.intent_id == intent.intent_id && command.version == intent.target_version))
            .collect::<Vec<_>>();
        let [stage_report] = stages.as_slice() else {
            panic!("one actual first Stage effect must precede this source prepare");
        };
        assert!(stage_report.succeeded);
        assert!(
            !stage_report.work.publish_metadata,
            "content staging cannot schedule a metadata publication"
        );
        assert_eq!(stage_report.command.routed_message.source_node_id, owner);
        assert_eq!(stage_report.command.routed_message.target_node_id, replica);
        let DfsNetworkMessage::StageBlob(stage) = &stage_report.command.routed_message.message
        else {
            unreachable!()
        };
        assert_eq!(stage.file_system_id, shard.file_system_id);
        assert_eq!(stage.authority_shard.as_ref(), Some(&shard));
        assert_eq!(stage.expected_base_version, intent.base_version);
        assert_eq!(stage.content_hash, expected_hash);
        assert_eq!(
            observer.content().read_blob(&stage.content_hash).unwrap(),
            bytes
        );
        let stage_observation = commits
            .lock()
            .unwrap()
            .iter()
            .find(|row| row.command == stage_report.command)
            .unwrap()
            .clone();
        let stage_after = stage_observation
            .after
            .as_ref()
            .expect("actual Stage AfterReport");
        assert_eq!(
            stage_observation.before.prefix, stage_after.prefix,
            "Stage cannot append the actual target journal"
        );
        assert_eq!(
            stage_observation.before.tables, stage_after.tables,
            "Stage cannot publish or change authoritative metadata records"
        );
        assert_eq!(
            stage_observation.before.journal_tail, stage_after.journal_tail,
            "Stage cannot author or change any acknowledged pending metadata entry"
        );
        assert_eq!(
            stage_observation.before.publish_requests,
            stage_after.publish_requests
        );
        assert!(
            metadata(&replica_data, &shard_id)
                .get_object(&stage.object_id)
                .unwrap()
                .is_none(),
            "the first Stage effect has no durable namespace object"
        );

        let stage = stage.clone();
        // The real assigned participants stay available for source Create.
        runtime.install_fault(crate::test_runtime::FaultRule {
            source: Some(owner.clone()),
            target: Some(replica.clone()),
            route: Some(DFS_DATA_COMMIT_VERSION_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 1,
            action: crate::test_runtime::FaultAction::CaptureCommitReplyAfterDispatch(Arc::clone(
                &committed_replies,
            )),
        });
        // Release before joining or reading source authority, including unwinds.
        drop(release);
        let (writer, result, flushed) = publisher.join().expect("normal source producer");
        let stat = result.expect("genuine full-assigned native metadata publication");
        flushed.expect("deliver actual queued CommitVersion");
        assert_eq!(stat.object_id, stage.object_id);
        assert_eq!(stat.version, stage.version);
        assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
        (writer, payload, stage)
    });
    let stored = writer
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&payload.operation_id)
        .unwrap()
        .unwrap();
    stored.validate().unwrap();
    assert_eq!(stored.status, crate::DfsPreparedOperationStatus::Committed);
    let mut normalized = payload;
    normalized.normalize();
    assert_eq!(
        stored.payload.as_ref(),
        &normalized,
        "the source commits its exact genuinely produced Create body"
    );
    assert!(
        writer
            .metadata_backend()
            .unwrap()
            .committed_prepared_operation(&stored.digest)
            .unwrap()
            .is_some()
    );
    let commit_reports = reports
        .lock()
        .unwrap()
        .clone()
        .into_iter()
        .filter(|row| {
            matches!(&row.command.routed_message.message,
            DfsNetworkMessage::CommitVersion(command) if command.object_id == stage.object_id
                && command.intent_id == stage.intent_id && command.version == stage.version)
        })
        .collect::<Vec<_>>();
    let [commit_report] = commit_reports.as_slice() else {
        panic!("one actual CommitVersion receiver report required");
    };
    assert!(commit_report.succeeded);
    assert_eq!(commit_report.command.routed_message.source_node_id, owner);
    assert_eq!(commit_report.command.routed_message.target_node_id, replica);
    assert_eq!(
        commit_report.work,
        crate::DfsMessageCommittedWork::default(),
        "replica commit notification performs no durable metadata or content mutation"
    );
    let DfsNetworkMessage::CommitVersion(commit) = &commit_report.command.routed_message.message
    else {
        unreachable!()
    };
    assert_eq!(commit.file_system_id, stage.file_system_id);
    assert_eq!(commit.authority_shard.as_ref(), Some(&shard));
    assert_eq!(commit.content_hash, expected_hash);
    let replies = committed_replies.lock().unwrap().clone();
    let [reply] = replies.as_slice() else {
        panic!("one genuine CommitVersion reply required");
    };
    assert_eq!(reply.source_node_id, replica);
    assert_eq!(reply.target_node_id, owner);
    assert!(
        matches!(&reply.message, DfsNetworkMessage::CommitVersionResponse(response)
        if response.request_id == commit.request_id && response.intent_id == stage.intent_id
            && response.object_id == stage.object_id && response.version == stage.version
            && matches!(&response.result, DfsBlobTransferResult::Committed { content_hash: hash }
                if hash == &expected_hash)),
        "actual receiver must commit the genuinely staged bytes"
    );
    let observations = commits
        .lock()
        .unwrap()
        .iter()
        .filter(|row| {
            matches!(
                row.command.routed_message.message,
                DfsNetworkMessage::CommitVersion(_)
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    let [observation] = observations.as_slice() else {
        panic!("one actual Commit effect boundary required");
    };
    assert_eq!(observation.command, commit_report.command);
    let after = observation
        .after
        .as_ref()
        .expect("actual Commit AfterReport");
    assert!(
        observation.before.prefix.0 > before_prefix.0,
        "the restored participant receives the actual primary preparation before Commit"
    );
    let target_prepare = observation
        .before
        .tables
        .tables
        .prepared_operations_v7
        .iter()
        .find(|record| record.digest == stored.digest)
        .or_else(|| observation.before.journal_tail.iter().find_map(|entry| {
            match &entry.mutation {
                crate::DfsMetadataMutation::PrepareOperationV7 { record, .. }
                    if record.digest == stored.digest => Some(record.as_ref()),
                _ => None,
            }
        }))
        .expect("the actual target holds the same primary-authored complete body in its acknowledged journal or materialized table");
    target_prepare.validate().unwrap();
    assert_eq!(target_prepare.payload, stored.payload);
    assert_eq!(
        after, &observation.before,
        "Commit changes neither the target's real journal/tables, content generation nor publication work"
    );
    assert_eq!(observer.content().read_blob(&expected_hash).unwrap(), bytes);
    assert_eq!(
        observer
            .content()
            .read_object_version(stage.object_id.as_str(), stage.version.as_str())
            .unwrap()
            .1,
        bytes,
        "actual Stage bytes remain bound after the canonical Commit acknowledgement"
    );
    drop(capture);
    drop(observer);
    drop(writer);
    runtime.clear_faults();
    runtime.close().expect("native Stage/Commit owners drained");
}

#[test]
fn commit_version_rejects_manifest_without_content_bytes() {
    native_missing_commit_content_case(false);
}

#[test]
fn rejected_commit_observes_the_real_publication_after_delayed_replica_materialization() {
    native_missing_commit_content_case(true);
}

fn native_missing_commit_content_case(delay_materialization: bool) {
    let name = if delay_materialization {
        "native-commit-rejects-lost-bytes-after-delayed-replay"
    } else {
        "native-commit-rejects-lost-actual-bytes"
    };
    let (mut runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id;
    drop(initial);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
    let replica = nodes.iter().find(|node| **node != owner).unwrap().clone();
    let path = format!("/services/{name}.bin");
    let mut first_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
    first_policy.required_node_ids = vec![owner.clone()];
    runtime
        .apply_placement_policy(&owner, &path, DfsPlacementScope::Subtree, first_policy)
        .unwrap();
    let mut writer = runtime.metadata_primary_service(&path).unwrap();
    let base = writer
        .posix_write_file(
            &path,
            b"actual original write head",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    drop(writer);
    let mut next_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    next_policy.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&owner, &path, DfsPlacementScope::Subtree, next_policy)
        .unwrap();
    let mut writer = runtime.metadata_primary_service(&path).unwrap();
    let bytes = vec![0x73; 2 * 1024 * 1024 + 4096];
    let version = crate::namespace::next_version(&base.version);
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id.clone(),
        object_id: base.object_id.clone(),
        target_version: version.clone(),
        requesting_node_id: owner.clone(),
    };
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    let shard = DfsMetadataShardId::new("services");
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let paused = delay_materialization.then(|| {
        // Setup publication may be materialized while its coalesced worker is
        // still draining. Install the pause only at the real idle boundary,
        // with producers excluded, so the next actual worker owns the hook.
        let materializer = crate::runtime_host::daemon_dfs_metadata_materialization_lane_scheduler(
            runtime.data_dir(&replica), &request.file_system_id, &shard,
        ).unwrap();
        let scheduler = crate::runtime_host::daemon_dfs_mutation_scheduler(
            runtime.data_dir(&replica),
        ).unwrap();
        let scope = crate::DfsMessageExecutionScope::ExactShard {
            file_system_id: request.file_system_id.clone(), shard_id: shard.clone(),
        };
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "setup materializer must drain inside original deadline");
            scheduler.request_foreground_admission();
            let admission = crate::test_runtime::executor().block_on(async {
                tokio::time::timeout(remaining, scheduler.admit(&scope)).await
                    .expect("original materialization pause admission deadline").unwrap()
            });
            if materializer.is_idle_while_producers_excluded() {
                let pause = runtime.pause_replica_materialization(&replica, &shard).unwrap();
                drop(admission);
                break pause;
            }
            drop(admission);
            std::thread::yield_now();
        }
    });
    let outcome = writer
        .write_distributed_reader_with_candidates(
            request.clone(),
            std::io::Cursor::new(&bytes),
            nodes,
            policy,
            DfsDistributedWriteOptions::default(),
        )
        .unwrap();
    let NetworkedDfsWriteOutcome::Pending {
        intent_id,
        durable_receipts,
        required_receipts,
    } = outcome
    else {
        panic!("actual Stage reply waits in its native incoming queue before W2")
    };
    assert_eq!((durable_receipts, required_receipts), (1, 2));
    writer.process_incoming().unwrap();
    assert!(matches!(
        writer.write_outcome(&intent_id).unwrap(),
        NetworkedDfsWriteOutcome::Committed { .. }
    ));
    let committed = writer
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&format!("write:{intent_id}"))
        .unwrap()
        .unwrap();
    committed.validate().unwrap();
    let published_sequence = committed.committed_entry.as_ref().unwrap().sequence;
    let mut delayed_view = None;
    if paused.is_some() {
        let observed = crate::runtime_host::materialization_test::observe_existing_materialization(
            runtime.data_dir(&replica), &request.file_system_id, &shard, &runtime.runtime_handle(),
        ).unwrap().expect("actual pre-publication replica");
        assert!(observed.durable_sequence.unwrap() < published_sequence, "the controlled original commit has not been sent yet: {observed:?}");
        let old = runtime.node_metadata(&replica).unwrap().get_object(&request.object_id).unwrap().unwrap();
        assert_ne!(old.version, version, "the primary committed while this actual replica still has the earlier cut");
        delayed_view = Some(serde_json::json!({"object": old, "before_original_publication": format!("{observed:?}")}));
    }
    // Settle the actual primary's already committed journal through the normal
    // authenticated replica route. The queued negative CommitVersion remains
    // untouched; no new logical mutation or replacement write identity is made.
    let backend = writer.metadata_backend().unwrap();
    let batch = backend
        .journal_batch_for_shard_since_limited(
            &shard,
            0,
            crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    assert!(batch.next_after_sequence >= published_sequence);
    let publication = crate::metadata_replication::dfs_metadata_durable_journal_batch_command(
        &owner,
        &replica,
        &request.file_system_id,
        committed.digest.owner_epoch,
        crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(backend, &batch)
            .unwrap(),
        false,
    );
    let response: crate::DfsMetadataJournalBatchResponse = runtime
        .rpc_request(
            &owner,
            &replica,
            sinomo_core::network::ObjectRpcRequest::encode_body(
                "/dfs/metadata/journal-batch",
                crate::DFS_RPC_CATEGORY,
                &publication,
            )
            .unwrap(),
            deadline.saturating_duration_since(Instant::now()),
        )
        .unwrap()
        .decode_body()
        .unwrap();
    assert_eq!(
        response.result,
        crate::DfsMetadataJournalBatchResult::Applied
    );
    assert_eq!(response.request_id, publication.request_id);
    assert_eq!(response.next_after_sequence, batch.next_after_sequence);
    std::fs::write(
        runtime
            .data_dir(&owner)
            .root()
            .join("boundary-diagnostic-negative-commit-publication.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "original_prepare": committed.digest,
            "original_commit_sequence": published_sequence,
            "normal_replica_publication": publication,
            "actual_response": response,
        }))
        .unwrap(),
    )
    .unwrap();
    if let Some(pause) = paused {
        runtime.wait_paused_materialization(
            &pause, deadline.saturating_duration_since(Instant::now()),
        ).expect("actual scheduled materializer reached the installed pause");
        // Hold only the ordinary scheduled materializer, before admission.
        // Actual ingress must durably receive the original commit; no ACK,
        // current view or replacement authority is supplied by this control.
        loop {
            assert!(
                Instant::now() < deadline,
                "original commit did not reach its actual replica"
            );
            if let Some(observed) =
                crate::runtime_host::materialization_test::observe_existing_materialization(
                    runtime.data_dir(&replica),
                    &request.file_system_id,
                    &shard,
                    &runtime.runtime_handle(),
                )
                .unwrap()
                && observed.durable_sequence.unwrap() >= published_sequence
            {
                assert!(
                    observed.materialized_sequence.unwrap() < published_sequence,
                    "controlled materializer must still be pending: {observed:?}"
                );
                delayed_view.as_mut().unwrap()["after_original_publication"] =
                    serde_json::json!(format!("{observed:?}"));
                break;
            }
            std::thread::yield_now();
        }
        pause.release();
        drop(pause);
    }
    // W2 commits the primary journal before the queued CommitVersion RPC.
    // Observe that SAME original commit's actual replica materialization before
    // capturing a negative operation's precondition. This never ingests a
    // journal through the caller or gives the rejected command publication credit.
    let ready = crate::runtime_host::materialization_test::wait_for_existing_materialization(
        runtime.data_dir(&replica),
        &request.file_system_id,
        &shard,
        &runtime.runtime_handle(),
        published_sequence,
        deadline,
        |_| {},
    )
    .unwrap();
    let target = runtime.client(&replica).unwrap();
    let before = runtime
        .node_metadata(&replica)
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(before.version, version);
    std::fs::write(
        runtime.data_dir(&replica).root().join("boundary-diagnostic-negative-commit-readiness.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "original_intent": intent_id, "original_prepare": committed.digest,
            "original_commit_sequence": published_sequence,
            "delayed_view_before_any_negative_command": delayed_view,
            "ready": format!("{ready:?}"), "before_rejected_command": before,
            "qualification": "Actual original metadata publication completes before content removal and the negative CommitVersion. Original rejection and unchanged-view assertions follow."
        })).unwrap(),
    ).unwrap();
    let manifest = target
        .content()
        .read_manifest(request.object_id.as_str(), version.as_str())
        .unwrap();
    assert_eq!(manifest.content_hash, content_hash(&bytes));
    // Exclude repairing traffic before the destructive control; otherwise
    // maintenance can restore the real blob before the negative Commit RPC.
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
    target
        .content()
        .debug_remove_loose_blob(&manifest.content_hash)
        .unwrap();
    assert!(
        target
            .content()
            .read_object_version(request.object_id.as_str(), version.as_str())
            .is_err()
    );
    drop(target);
    let captured = Arc::new(Mutex::new(Vec::new()));
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(crate::DFS_DATA_COMMIT_VERSION_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::CaptureCommitReplyAfterDispatch(Arc::clone(
            &captured,
        )),
    });
    // The normal W2 receipt handler already queued its genuine CommitVersion.
    // Its actual receiver must reject after losing those real staged bytes.
    let _ = writer.flush_detached_transport_sends();
    let replies = captured.lock().unwrap();
    assert_eq!(replies.len(), 1);
    assert!(
        matches!(&replies[0].message, DfsNetworkMessage::CommitVersionResponse(response)
        if response.intent_id == intent_id && response.object_id == request.object_id && response.version == version
            && matches!(&response.result, DfsBlobTransferResult::Rejected { reason } if reason.contains("committed content bytes are missing"))),
        "actual captured CommitVersion reply: {:?}", *replies
    );
    drop(replies);
    let target = runtime.client(&replica).unwrap();
    let after = runtime
        .node_metadata(&replica)
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(after.version, before.version);
    assert_eq!(after.content_hash, before.content_hash);
    assert_eq!(after.content_size_bytes, before.content_size_bytes);
    assert!(
        target
            .content()
            .read_object_version(request.object_id.as_str(), version.as_str())
            .is_err()
    );
    drop(target);
    drop(writer);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn local_owned_write_routes_to_owner() {
    let owner = NodeId::from_legacy_named("node_desktop").expect("node");
    let remote = NodeId::from_legacy_named("node_mobile").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_file").expect("object");
    let mut store = DfsMetadataStore::new();
    store.add_file_system(file_system(owner.clone()));
    store
        .add_object(DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id,
            path: "src/lib.rs".to_string(),
            namespace_link_count: (DfsObjectKind::File).initial_namespace_link_count(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: owner.clone(),
            current_locations: vec![owner.clone()],
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named("version_1").expect("version"),
            policy: DfsObjectPolicy::LocalOwned,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        })
        .expect("add object");

    let route = store
        .route_write(
            DfsWriteRequest {
                file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
                object_id,
                target_version: DfsVersion::from_legacy_named("version_2").expect("version"),
                requesting_node_id: remote.clone(),
            },
            &remote,
        )
        .expect("route write");

    assert_eq!(
        route.action,
        WriteRouteAction::RouteToOwner {
            owner_node_id: owner
        }
    );
}

#[test]
fn sifs_self_only_eventual_write_uses_local_fast_path() {
    let node_id = NodeId::from_legacy_named("node_sifs_fast_path").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_fast/work/site.py";

    let (runtime, mut dfs) = native_sifs_service(
        "sifs_self_only_eventual_write_uses_local_fast_path",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_fast/work")
        .expect("mkdir");
    let stat = dfs
        .posix_write_file(
            file_path,
            b"print('fast path')\n",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write");

    assert_eq!(stat.size_bytes, Some(19));
    assert_eq!(
        dfs.posix_read_file_distributed(file_path).expect("read"),
        b"print('fast path')\n"
    );
    // The client delegates to its Main; inspect the node's durable store.
    let backend = runtime
        .node_metadata(dfs.node_id())
        .expect("actual current physical backend");
    let status = DfsOperatorInspector::new(&backend)
        .status()
        .expect("actual Main durable status after self-only commit");
    assert!(status.incomplete_reasons.is_empty());
    assert!(status.bounded_omissions.is_empty());
    assert_eq!(
        status.pending_write_count, 0,
        "self-only eventual writes should not leave durable pending intents"
    );
    assert_eq!(
        status.pending_repair_count, 0,
        "self-only commit must leave no repair work"
    );

    let journal_count = backend
        .journal_since_limited(0, 64)
        .expect("actual current native journal")
        .len();
    assert!(
        journal_count < 64,
        "self-only write should not emit per-file distributed intent/receipt/commit journals"
    );

    drop(backend);
    drop(dfs);
    runtime
        .close()
        .expect("stop the actual native self-only owner");
}


#[test]
fn duplicate_commit_metadata_application_is_idempotent() {
    let node_id = NodeId::from_legacy_named("node_duplicate_commit_meta").expect("node");
    let path = "/services/native-duplicate-commit-metadata.bin";
    let bytes = b"duplicate commit bytes";
    let (runtime, mut main) = native_sifs_service(
        "duplicate_commit_metadata_application_is_idempotent",
        node_id.clone(),
    );
    let actual = main
        .posix_write_file(
            path,
            bytes,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .unwrap();
    drop(main);
    let mut dfs = runtime.metadata_primary_service(path).unwrap();
    let object_id = actual.object_id;
    let version = actual.version;
    let hash = actual
        .content_hash
        .expect("actual canonical committed hash");
    let before = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object");
    let shard = DfsMetadataShardId::new("services");
    let prefix_before = dfs
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard)
        .unwrap();
    let committed = dfs
        .commit_local_metadata(&object_id, &version, &hash, dfs_now_unix_nanos())
        .expect("duplicate commit metadata");
    dfs.put_visible_object_in_metadata_backend(&committed)
        .expect("duplicate visible publish");

    let after = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object after duplicate commit");
    assert_eq!(
        after.last_modified_by_event_id,
        before.last_modified_by_event_id
    );
    assert_eq!(after.modified_at_unix_nanos, before.modified_at_unix_nanos);
    assert_eq!(after.changed_at_unix_nanos, before.changed_at_unix_nanos);
    let backend = dfs.metadata_backend_mut().unwrap();
    backend
        .begin_consistent_read_snapshot_for_path(&before.file_system_id, path)
        .unwrap();
    let prefix_after = backend.metadata_prefix_proof(&shard).unwrap();
    backend.end_consistent_read_snapshot_for_path(&before.file_system_id, path);
    assert_eq!(
        prefix_after, prefix_before,
        "duplicate committed metadata application must not append canonical visible effects"
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn duplicate_committed_intent_application_does_not_recommit_visible_version() {
    let (runtime, _, _, mut writer, request, _, intent) =
        native_owned_pending_write("native-duplicate-actual-commit");
    writer.process_incoming().unwrap();
    assert!(matches!(
        writer.write_outcome(&intent).unwrap(),
        NetworkedDfsWriteOutcome::Committed { .. }
    ));
    let before = writer
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    let shard = DfsMetadataShardId::new("services");
    let prefix = writer
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard)
        .unwrap();
    writer.apply_committed_intent(&intent).unwrap();
    assert_eq!(
        writer
            .metadata_backend()
            .unwrap()
            .get_object(&request.object_id)
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(
        writer
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&shard)
            .unwrap(),
        prefix,
        "normal duplicate committed application never appends another visible canonical effect"
    );
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn sifs_posix_exclusive_create_never_overwrites_an_existing_file() {
    let root = temp_root("sifs-posix-exclusive-create");
    let node_id = NodeId::from_legacy_named("node_sifs_exclusive_create").expect("node");
    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_exclusive_create_never_overwrites_an_existing_file",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/exclusive-create")
        .expect("mkdir parent");
    let options = DfsWriteOptions::replicated_eventual(vec![node_id]);
    let path = "/services/exclusive-create/file";
    dfs.posix_write_file_with_create_attrs(
        path,
        b"keep me",
        options.clone(),
        DfsPosixCreateAttrs::new(0o640, 1001, 1002),
    )
    .expect("seed existing file");

    assert!(matches!(
        dfs.posix_create_file_with_attrs(
            path,
            options.clone(),
            DfsPosixCreateAttrs::new(0o600, 2001, 2002),
        ),
        Err(DfsPosixError::AlreadyExists { .. })
    ));
    assert_eq!(
        dfs.posix_read_file_distributed(path)
            .expect("existing bytes"),
        b"keep me"
    );
    let existing = dfs.posix_stat_distributed(path).expect("existing attrs");
    assert_eq!(
        (existing.mode, existing.uid, existing.gid),
        (0o640, 1001, 1002)
    );

    dfs.posix_unlink(path).expect("unlink existing file");
    let recreated = dfs
        .posix_create_file_with_attrs(path, options, DfsPosixCreateAttrs::new(0o600, 2001, 2002))
        .expect("exclusive create over tombstone");
    assert_eq!(recreated.size_bytes, Some(0));
    assert_eq!(
        (recreated.mode, recreated.uid, recreated.gid),
        (0o600, 2001, 2002)
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_updates_mtime_without_changing_atime() {
    let root = temp_root("sifs-posix-write-mtime");
    let node_id = NodeId::from_legacy_named("node_sifs_write_mtime").expect("node");
    let original = "/services/sandbox/instances/sandbox_write_mtime/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_write_mtime/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_write_updates_mtime_without_changing_atime",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_write_mtime/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, link).expect("hardlink");
    dfs.posix_set_times(original, Some(12_345), Some(67_890))
        .expect("set old times");

    let stat = dfs
        .posix_write_file(
            link,
            b"updated",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write through hardlink");

    assert_eq!(stat.accessed_at_unix_nanos, 12_345);
    assert!(stat.modified_at_unix_nanos > 67_890);
    let original_stat = dfs.posix_stat_distributed(original).expect("original stat");
    let link_stat = dfs.posix_stat_distributed(link).expect("link stat");
    assert_eq!(original_stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(link_stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(
        original_stat.modified_at_unix_nanos,
        link_stat.modified_at_unix_nanos
    );
    assert!(original_stat.modified_at_unix_nanos > 67_890);
    assert_eq!(
        original_stat.changed_at_unix_nanos,
        original_stat.modified_at_unix_nanos
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_bulk_local_eventual_writes_visible_files() {
    let root = temp_root("sifs-posix-bulk-local-eventual");
    let node_id = NodeId::from_legacy_named("node_sifs_bulk_local").expect("node");
    let work = "/services/sandbox/instances/sandbox_bulk_local/work";
    let path_a = "/services/sandbox/instances/sandbox_bulk_local/work/a.txt";
    let path_b = "/services/sandbox/instances/sandbox_bulk_local/work/b.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_bulk_local_eventual_writes_visible_files",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    let stats = dfs
        .posix_write_files_local_eventual_with_known_parents(vec![
            (path_a, b"bulk alpha".as_slice()),
            (path_b, b"bulk beta".as_slice()),
        ])
        .expect("bulk write");

    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].path, path_a);
    assert_eq!(stats[0].current_locations, vec![node_id.clone()]);
    assert_eq!(stats[0].size_bytes, Some("bulk alpha".len() as u64));
    assert_eq!(stats[1].path, path_b);
    assert_eq!(stats[1].current_locations, vec![node_id]);
    assert_eq!(stats[1].size_bytes, Some("bulk beta".len() as u64));
    assert_eq!(
        dfs.posix_read_file_distributed(path_a).expect("read a"),
        b"bulk alpha"
    );
    assert_eq!(
        dfs.posix_read_file_distributed(path_b).expect("read b"),
        b"bulk beta"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_bulk_overwrite_pins_metadata_generation_during_preparation() {
    let root = temp_root("sifs-posix-bulk-overwrite-pinned-metadata");
    let node_id =
        NodeId::from_legacy_named("node_sifs_bulk_overwrite_pinned_metadata").expect("node");
    let work = "/services/sandbox/instances/sandbox_bulk_overwrite_pinned_metadata/work";
    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_bulk_overwrite_pins_metadata_generation_during_preparation",
        node_id,
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    let owned_files = (0..64)
        .map(|index| {
            (
                format!("{work}/file-{index:03}.txt"),
                format!("initial-{index:03}").into_bytes(),
                format!("overwritten-{index:03}").into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    dfs.posix_create_files_local_eventual_with_known_parents(
        owned_files
            .iter()
            .map(|(path, initial, _)| (path.as_str(), initial.as_slice())),
    )
    .expect("initial bulk create");
    drop(dfs);
    // The probe counters are thread-local and the pinned metadata generation
    // belongs to the service preparing the bulk write. A caller delegating to
    // its Main prepares nothing locally, so the store-backed opener runs the
    // bulk preparation and its later local stat on this thread.
    let mut dfs = _runtime.metadata_primary_service(work).unwrap();
    let versions_before = owned_files
        .iter()
        .map(|(path, _, _)| dfs.posix_stat(path).expect("stat initial").version)
        .collect::<Vec<_>>();

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    crate::posix::reset_posix_bulk_prep_durable_table_probes();
    crate::posix::reset_posix_metadata_raw_lookup_count();
    let stats = dfs
        .posix_write_files_local_eventual_with_known_parents(
            owned_files
                .iter()
                .map(|(path, _, overwritten)| (path.as_str(), overwritten.as_slice())),
        )
        .expect("bulk overwrite");

    assert_eq!(stats.len(), owned_files.len());
    assert_eq!(
        crate::posix::posix_bulk_prep_durable_table_probes(),
        0,
        "bulk overwrite child discovery must reuse the pinned metadata generation \
         instead of polling checkpoint, snapshot, and table-log files per object"
    );
    let parent_component_count = work.trim_matches('/').split('/').count();
    assert!(
        crate::posix::posix_bulk_prep_table_snapshot_calls()
            <= owned_files.len() + parent_component_count + 1,
        "bulk overwrite performed {} metadata table reads for {} child objects; \
         expected one pinned ancestor/parent revalidation plus one read per \
         child, without hydrating receipt-derived locations that the publisher \
         replaces",
        crate::posix::posix_bulk_prep_table_snapshot_calls(),
        owned_files.len(),
    );
    let raw_lookup_bound = owned_files.len() + parent_component_count * 2 + 3;
    assert!(
        crate::posix::posix_metadata_raw_lookup_count() <= raw_lookup_bound,
        "bulk overwrite performed {} raw path lookups for {} children below one \
         already-resolved parent; expected no more than {raw_lookup_bound}",
        crate::posix::posix_metadata_raw_lookup_count(),
        owned_files.len(),
    );
    for (((path, _, overwritten), version_before), stat) in
        owned_files.iter().zip(versions_before).zip(stats)
    {
        assert_eq!(stat.path, *path);
        assert_ne!(stat.version, version_before);
        assert_eq!(
            dfs.posix_read_file(path).expect("read overwritten file"),
            *overwritten
        );
    }

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    dfs.posix_stat(&owned_files[0].0)
        .expect("stat after bulk overwrite");
    assert!(
        crate::metadata_read_snapshot::metadata_durable_table_probes() > 0,
        "bulk overwrite must release its consistent-read scope before returning"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_distributed_write_rejects_stale_backend_base_version() {
    let node_id = NodeId::from_legacy_named("node_sifs_stale_write_base").expect("node");
    let work = "/services/sandbox/instances/sandbox_stale_write/work";
    let file_path = "/services/sandbox/instances/sandbox_stale_write/work/file.txt";

    let (runtime, mut writer) = native_sifs_service("native-stale-backend", node_id.clone());
    writer.posix_mkdir_all(work).expect("mkdir");
    writer
        .posix_write_file(
            file_path,
            b"abcdef",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("initial write");

    // The private CAS producer is the actual owning Core, not Main's routed
    // requester. Its earlier stat is genuine; the kernel must reject that base
    // after a later ordinary publication advances the native version.
    let mut stale_writer = runtime.metadata_primary_service(file_path).unwrap();
    let stale_stat = stale_writer.posix_stat(file_path).expect("stale stat");

    writer
        .posix_write_file_at(
            file_path,
            0,
            b"XY",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("fresh offset write");
    let fresh_stat = writer
        .posix_stat_distributed(file_path)
        .expect("fresh stat");
    let journal_count_before_conflict = stale_writer
        .metadata_backend()
        .expect("writer metadata backend")
        .journal_since(0)
        .expect("journal before conflict")
        .len();

    let request = DfsWriteRequest {
        file_system_id: stale_stat.file_system_id.clone(),
        object_id: stale_stat.object_id.clone(),
        target_version: crate::namespace::next_version(&stale_stat.version),
        requesting_node_id: node_id.clone(),
    };
    let error = stale_writer
        .write_distributed_eventual_with_base(
            request,
            Some(stale_stat.version.clone()),
            b"stale overwrite".to_vec(),
            vec![node_id.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect_err("stale write must reject");
    assert!(
        error.to_string().contains("stale DFS write base"),
        "unexpected stale write error: {error}"
    );

    let journal_after_rejection = stale_writer
        .metadata_backend()
        .expect("stale writer metadata backend")
        .journal_since(0)
        .expect("journal after rejection");
    assert_eq!(
        journal_after_rejection.len(),
        journal_count_before_conflict,
        "stale writer rejection must not append conflict metadata or overwrite fresh writer entries"
    );

    let mut reloaded = runtime.client(&node_id).unwrap();
    assert_eq!(
        reloaded
            .posix_stat_distributed(file_path)
            .expect("stat after stale rejection")
            .content_hash,
        fresh_stat.content_hash,
        "stale rejection must not replace the committed content hash"
    );
    assert_eq!(
        reloaded
            .posix_read_file_distributed(file_path)
            .expect("read winner"),
        b"XYcdef"
    );
}

#[test]
fn sifs_distributed_write_validates_replica_plan_before_storing_content() {
    let root = temp_root("sifs-invalid-replica-plan-no-content");
    let node_id = NodeId::from_legacy_named("node_sifs_invalid_plan").expect("node");
    let work = "/services/sandbox/instances/sandbox_invalid_plan/work";
    let file_path = "/services/sandbox/instances/sandbox_invalid_plan/work/file.txt";
    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_distributed_write_validates_replica_plan_before_storing_content",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        file_path,
        b"initial",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("initial write");
    let stat = dfs.posix_stat_distributed(file_path).expect("stat");
    let rejected_bytes = b"replica plan is invalid";
    let rejected_hash = content_hash(rejected_bytes);
    let target_version = crate::namespace::next_version(&stat.version);

    let error = dfs
        .write_distributed_eventual_with_base(
            DfsWriteRequest {
                file_system_id: stat.file_system_id.clone(),
                object_id: stat.object_id.clone(),
                target_version: target_version.clone(),
                requesting_node_id: node_id.clone(),
            },
            Some(stat.version),
            rejected_bytes.to_vec(),
            vec![node_id],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect_err("invalid replica plan should fail before content write");

    assert!(
        matches!(error, DfsError::Distributed { .. }),
        "invalid replica plan should be reported as distributed validation failure: {error:?}"
    );
    assert!(
        !dfs.content
            .has_blob(&rejected_hash)
            .expect("rejected blob check"),
        "rejected write bytes must not be persisted before replica validation"
    );
    assert!(
        dfs.content
            .read_manifest(stat.object_id.as_str(), target_version.as_str())
            .is_err(),
        "rejected write must not publish a content manifest"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_primary_detached_large_write_is_readable_without_follow_up_flush() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-primary-detached-large", "/services");
    let path = "/services/primary-detached-large.bin";
    let bytes = (0..(2 * 1024 * 1024 + 8192))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let expected_hash = content_hash(&bytes);
    let mut options = sifs_cluster_write_options(&cluster.primary_node.clone());
    options.foreground_replica_repair = false;

    let stat = cluster
        .primary
        .posix_write_file(path, &bytes, options)
        .expect("primary detached large write");

    assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
    assert_eq!(
        cluster
            .primary
            .content
            .read_blob(&expected_hash)
            .expect("durable blob is readable when metadata becomes visible"),
        bytes
    );

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_contract_concurrent_whole_file_overwrite_many_readers() {
    let mut cluster = sifs_posix_cluster("sifs-contract-overwrite-readers", "/users");
    let scenario = "concurrent-whole-file-overwrite-many-readers";
    let path = "/users/contract-overwrite.txt";
    let base = vec![b'A'; 4096];
    let writer_bytes = vec![b'B'; 4096];
    let observer_bytes = vec![b'C'; 4096];
    let valid_hashes = [
        read_content_hash(&base),
        read_content_hash(&writer_bytes),
        read_content_hash(&observer_bytes),
    ];
    let mut observations = Vec::new();

    sifs_cluster_routed_posix_write(&mut cluster, path, &base);
    sifs_cluster_publish_metadata_until(
        &mut cluster,
        "overwrite observer seed catch-up",
        |cluster| {
            super::cross_shard::node_store_object_at(&cluster._runtime, &cluster.observer_node, path)
                .is_some_and(|object| {
                    object.content_hash.as_deref() == Some(valid_hashes[0].as_str())
                })
        },
    );
    let base_stat = cluster
        .writer
        .posix_stat_distributed(path)
        .expect("base stat");
    record_posix_observation(
        &mut observations,
        scenario,
        "seed",
        &cluster.writer_node,
        path,
        Instant::now(),
        "committed",
        Some(&base_stat),
    );

    let writer_request = match cluster.writer.posix_write_file(
        path,
        &writer_bytes,
        sifs_cluster_write_options(&cluster.writer_node.clone()),
    ) {
        Ok(stat) => {
            record_posix_observation(
                &mut observations,
                scenario,
                "writer-overwrite",
                &cluster.writer_node,
                path,
                Instant::now(),
                "committed-immediately",
                Some(&stat),
            );
            None
        }
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => Some(intent_id),
        Err(error) => panic!("writer overwrite failed unexpectedly: {error}"),
    };
    let observer_request = match cluster.observer.posix_write_file(
        path,
        &observer_bytes,
        sifs_cluster_write_options(&cluster.observer_node.clone()),
    ) {
        Ok(stat) => {
            record_posix_observation(
                &mut observations,
                scenario,
                "observer-overwrite",
                &cluster.observer_node,
                path,
                Instant::now(),
                "committed-immediately",
                Some(&stat),
            );
            None
        }
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => Some(intent_id),
        Err(DfsPosixError::Dfs(DfsError::Busy { .. })) => None,
        Err(DfsPosixError::Conflict { .. }) => {
            panic!("observer overwrite created a DFS conflict during normal operation")
        }
        Err(error) => panic!("observer overwrite failed unexpectedly: {error}"),
    };

    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        for (op, node, read) in [
            (
                "reader-writer",
                cluster.writer_node.clone(),
                cluster.writer.posix_stat_distributed(path),
            ),
            (
                "reader-observer",
                cluster.observer_node.clone(),
                cluster.observer.posix_stat_distributed(path),
            ),
            (
                "reader-primary",
                cluster.primary_node.clone(),
                cluster.primary.posix_stat_distributed(path),
            ),
        ] {
            if let Ok(stat) = read {
                assert!(
                    stat.content_hash
                        .as_ref()
                        .is_none_or(|hash| valid_hashes.contains(hash)),
                    "{op} observed a non-serialized content hash: {stat:?}"
                );
                record_posix_observation(
                    &mut observations,
                    scenario,
                    op,
                    &node,
                    path,
                    Instant::now(),
                    "stat",
                    Some(&stat),
                );
            }
        }
        let writer_settled = writer_request.as_ref().is_none_or(|intent| {
            !matches!(
                cluster.writer.write_outcome(intent),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        });
        let observer_settled = observer_request.as_ref().is_none_or(|intent| {
            !matches!(
                cluster.observer.write_outcome(intent),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        });
        if writer_settled && observer_settled {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "concurrent overwrite did not settle; observations: {observations:#?}"
        );
        sifs_posix_cluster_pump_round(&mut cluster);
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }

    let final_stat = cluster
        .primary
        .posix_stat_distributed(path)
        .expect("primary final stat");
    assert!(
        final_stat
            .content_hash
            .as_ref()
            .is_some_and(|hash| valid_hashes.contains(hash)),
        "final visible version must be one complete serialized overwrite: {final_stat:?}"
    );
    let final_bytes = cluster
        .primary
        .posix_read_file_distributed(path)
        .expect("primary final read");
    assert!(
        [&base, &writer_bytes, &observer_bytes]
            .iter()
            .any(|candidate| candidate.as_slice() == final_bytes.as_slice()),
        "final read must be one complete candidate, not mixed bytes"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    drop(cluster);
}

#[test]
fn sifs_cluster_posix_contract_large_streamed_write_visibility() {
    let mut cluster = sifs_posix_cluster("sifs-contract-large-visibility", "/services");
    let scenario = "large-streamed-write-visibility";
    let path = "/services/contract-large-visible.bin";
    let old = vec![0x11; 128 * 1024];
    let new = (0..(4 * 1024 * 1024 + 17))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let old_hash = content_hash(&old);
    let new_hash = content_hash(&new);
    let mut observations = Vec::new();
    let holders = vec![cluster.writer_node.clone(), cluster.observer_node.clone()];
    let mut placement =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    placement.required_node_ids = holders.clone();
    cluster
        ._runtime
        .apply_placement_policy(
            &cluster.writer_node,
            path,
            DfsPlacementScope::SelfOnly,
            placement.clone(),
        )
        .unwrap();
    cluster.writer = cluster._runtime.client(&cluster.writer_node).unwrap();
    let old_stat = cluster
        .writer
        .posix_write_file(
            path,
            &old,
            DfsWriteOptions {
                policy: placement.replication_policy(Default::default()).unwrap(),
                replica_candidates: holders.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    record_posix_observation(
        &mut observations,
        scenario,
        "seed",
        &cluster.writer_node,
        path,
        Instant::now(),
        "committed",
        Some(&old_stat),
    );
    let config = cluster
        ._runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|row| row.shard_id == "services")
        .unwrap();
    assert_eq!(config.primary_node_id, cluster.primary_node);
    let root = default_dfs_metadata_shards_root(cluster._runtime.data_dir(&cluster.primary_node))
        .join(&config.shard_id);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    struct Release(Arc<tokio::sync::Semaphore>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.add_permits(32);
        }
    }
    let capture = cluster
        ._runtime
        .capture_served_work(&cluster.observer_node, 16)
        .unwrap();
    cluster
        ._runtime
        .install_fault(crate::test_runtime::FaultRule {
            source: Some(cluster.primary_node.clone()),
            target: Some(cluster.observer_node.clone()),
            route: Some(DFS_DATA_STAGE_BLOB_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::PauseAfterDispatch(Arc::clone(&gate)),
        });
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    let report = std::thread::scope(|scope| {
        // This guard unwinds before scope joins the actual blocked producer.
        let release = Release(Arc::clone(&gate));
        let writer = &mut cluster.writer;
        let bytes = &new;
        let policy = placement.replication_policy(Default::default()).unwrap();
        let candidates = holders.clone();
        let producer = scope.spawn(move || {
            writer.posix_write_file(
                path,
                bytes,
                DfsWriteOptions {
                    policy,
                    replica_candidates: candidates,
                    wait_for_commit: true,
                    foreground_replica_repair: true,
                },
            )
        });
        let report = loop {
            if let Some(row) = capture.records().into_iter().find(|row| matches!(
                &row.command.routed_message.message, DfsNetworkMessage::StageBlob(stage) if stage.content_hash == new_hash)) { break row; }
            assert!(
                Instant::now() < deadline,
                "actual streamed Stage did not reach its receiver"
            );
            std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
        };
        assert!(report.succeeded);
        let DfsNetworkMessage::StageBlob(stage) = &report.command.routed_message.message else {
            unreachable!()
        };
        assert_eq!(
            report.command.routed_message.source_node_id,
            cluster.primary_node
        );
        assert_eq!(
            report.command.routed_message.target_node_id,
            cluster.observer_node
        );
        assert_eq!(stage.content_hash, new_hash);
        assert_eq!(stage.object_id, old_stat.object_id);
        assert_eq!(stage.expected_base_version, Some(old_stat.version.clone()));
        assert_eq!(
            cluster
                .observer
                .content()
                .read_blob(&stage.content_hash)
                .unwrap(),
            new,
            "all actual stream bytes are durable before the held W2 response"
        );
        // Read the actual native materialized head without requesting another
        // execution from the Main producer whose real response is held here.
        let inspection = LocalSobjMetadataBackend::open_for_inspection(
            DfsMetadataShardId::new(config.shard_id.clone()),
            &root,
        )
        .unwrap();
        let current = inspection.get_object(&old_stat.object_id).unwrap().unwrap();
        assert_eq!(current.version, old_stat.version);
        assert_eq!(current.content_hash.as_deref(), Some(old_hash.as_str()));
        assert_ne!(
            current.content_hash.as_deref(),
            Some(new_hash.as_str()),
            "large chunked bytes cannot become visible before the required response"
        );
        assert!(
            inspection
                .get_version(&stage.object_id, &stage.version)
                .unwrap()
                .is_none()
        );
        drop(inspection);
        record_posix_observation(
            &mut observations,
            scenario,
            "reader-before-commit",
            &cluster.primary_node,
            path,
            Instant::now(),
            "actual durable precommit head",
            Some(&old_stat),
        );
        assert_eq!(cluster._runtime.remove_faults_matching(|rule| {
            rule.source.as_ref() == Some(&cluster.primary_node) && rule.target.as_ref() == Some(&cluster.observer_node)
            && rule.route.as_deref() == Some(DFS_DATA_STAGE_BLOB_ROUTE)
            && matches!(&rule.action, crate::test_runtime::FaultAction::PauseAfterDispatch(g) if Arc::ptr_eq(g, &gate))
        }), 1);
        drop(release);
        let written = producer
            .join()
            .expect("normal native write producer")
            .expect("held real reply completes the write");
        assert_eq!(written.version, stage.version);
        assert_eq!(written.content_hash.as_deref(), Some(new_hash.as_str()));
        assert!(
            Instant::now() < deadline,
            "actual commit exceeded the original budget"
        );
        report
    });
    let committed = cluster
        .writer
        .posix_stat_distributed(path)
        .expect("current committed stat");
    assert_eq!(committed.content_hash.as_deref(), Some(new_hash.as_str()));
    assert_ne!(old_hash, new_hash);
    assert_eq!(
        cluster.observer.posix_read_file_distributed(path).unwrap(),
        new
    );
    assert!(capture.records().iter().any(
        |row| row.command.routed_message.message_id == report.command.routed_message.message_id
    ));
    drop(capture);
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    drop(cluster);
}

#[test]
fn sifs_cluster_posix_contract_rename_over_existing_with_concurrent_write() {
    let mut cluster = sifs_posix_cluster("sifs-contract-rename-over-write", "/users");
    let scenario = "rename-over-existing-with-concurrent-write";
    let source = "/users/contract-rename-over-src.txt";
    let destination = "/users/contract-rename-over-dst.txt";
    let source_bytes = b"source wins if rename serializes last".to_vec();
    let destination_bytes = b"destination original".to_vec();
    let writer_bytes = b"concurrent destination writer".to_vec();
    let valid_hashes = [
        content_hash(&source_bytes),
        content_hash(&destination_bytes),
        content_hash(&writer_bytes),
    ];
    let mut observations = Vec::new();

    sifs_cluster_primary_posix_write(&mut cluster, source, &source_bytes);
    sifs_cluster_primary_posix_write(&mut cluster, destination, &destination_bytes);
    let write_result = cluster.writer.posix_write_file(
        destination,
        &writer_bytes,
        sifs_cluster_write_options(&cluster.writer_node.clone()),
    );
    let write_request = match &write_result {
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => Some(intent_id.clone()),
        _ => None,
    };
    record_posix_observation(
        &mut observations,
        scenario,
        "write-destination",
        &cluster.writer_node,
        destination,
        Instant::now(),
        format!("{write_result:?}"),
        write_result.as_ref().ok(),
    );
    let rename_result = cluster.primary.posix_rename(source, destination);
    record_posix_observation(
        &mut observations,
        scenario,
        "rename-over",
        &cluster.primary_node,
        destination,
        Instant::now(),
        format!("{rename_result:?}"),
        rename_result.as_ref().ok(),
    );
    sifs_posix_cluster_pump_until(&mut cluster, "rename/write race settle", |cluster| {
        write_request.as_ref().is_none_or(|intent| {
            !matches!(
                cluster.writer.write_outcome(intent),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        }) && cluster.primary.posix_stat_distributed(destination).is_ok()
    });

    assert!(matches!(
        cluster.primary.posix_stat_distributed(source),
        Err(DfsPosixError::NotFound { .. })
    ));
    let final_stat = cluster
        .primary
        .posix_stat_distributed(destination)
        .expect("destination stat");
    assert!(
        final_stat
            .content_hash
            .as_ref()
            .is_some_and(|hash| valid_hashes.contains(hash)),
        "destination must reference one serialized participant: {final_stat:?}"
    );
    let source_entries = cluster
        .primary
        .posix_list_dir_distributed("/users")
        .expect("list users");
    assert_eq!(
        source_entries
            .iter()
            .filter(|entry| entry.path == source || entry.path == destination)
            .count(),
        1,
        "rename-over race must leave exactly one live destination entry: {source_entries:?}"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    drop(cluster);
}

#[test]
fn sharded_lazy_reader_sees_sibling_visible_overwrite() {
    let name = "native-sharded-lazy-reader-overwrite";
    let (runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 1, "/services").unwrap();
    let node = config.primary_node_id;
    drop(initial);
    let path = "/services/current-read-refresh.txt";
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
    policy.required_node_ids = vec![node.clone()];
    runtime
        .apply_placement_policy(&node, path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    let mut writer = runtime.client(&node).unwrap();
    let old = b"old actual sharded lazy reader payload";
    let new = b"new actual sharded lazy reader payload";
    let first = writer
        .posix_write_file(
            path,
            old,
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    let mut reader = runtime.client(&node).unwrap();
    assert!(
        reader.metadata().object(&first.object_id).is_none(),
        "normal client begins with lazy metadata attachment"
    );
    assert_eq!(reader.posix_read_file_distributed(path).unwrap(), old);
    let current = writer
        .posix_write_file(path, new, DfsWriteOptions::replicated_eventual(vec![node]))
        .unwrap();
    assert_ne!(current.version, first.version);
    assert_eq!(reader.posix_read_file_distributed(path).unwrap(), new);
    assert_eq!(
        reader.posix_stat_distributed(path).unwrap().version,
        current.version
    );
    drop(reader);
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn sifs_cluster_posix_blocking_write_and_read_with_live_peers() {
    // End-to-end through the blocking POSIX surface that the FUSE adapter and
    // CLI use: `wait_for_commit: true` writes on a non-primary node and
    // blocking distributed reads through the registered native runtime.
    let cluster = sifs_posix_cluster("sifs-cluster-blocking", "/users");
    let path = "/users/blocking.txt";
    let bytes = b"blocking write through live peers".to_vec();
    let SifsPosixCluster {
        root,
        primary_node,
        writer_node,
        observer_node: _,
        primary,
        mut writer,
        mut observer,
        rejections: _,
        _runtime,
    } = cluster;

    // The production write path: blocks until the routed write commits.
    let stat = writer
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
        )
        .expect("blocking routed write");
    assert_eq!(
        stat.content_hash.as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));

    // Read-your-write immediately after the blocking write returns.
    assert_eq!(
        writer
            .posix_read_file_distributed(path)
            .expect("read your write"),
        bytes
    );

    assert_eq!(
        observer.posix_stat_distributed(path).unwrap().version,
        stat.version
    );
    let mut primary = primary;
    // The blocking distributed read consults the metadata primary and fetches
    // content from the writer replica while both peers keep serving frames.
    let read = sifs_cluster_blocking_distributed_read_with_pump(
        &mut observer,
        path,
        "blocking distributed read",
        || {
            primary.process_incoming().expect("primary pump");
            writer.process_incoming().expect("writer pump");
        },
    );
    assert_eq!(read, bytes);

    drop(primary);
    drop(writer);
    drop(observer);
    _runtime.close().expect("normal runtime owner drain");
}

#[test]
fn sifs_cluster_metadata_less_bulk_overwrite_prefetches_authoritative_children() {
    let mut cluster = sifs_two_primary_cluster("sifs-bulk-metadata-less-overwrite");
    let remote_dir = "/services/bulk-metadata-less-overwrite";
    cluster
        .services_primary
        .posix_mkdir_all(remote_dir)
        .expect("remote bulk parent");
    sifs_two_primary_publish_until(&mut cluster, "metadata-less overwrite parent", |cluster| {
        super::cross_shard::node_store_object_at(&cluster._runtime, &cluster.replica_node, remote_dir)
            .is_some()
    });

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

    let files = (0..16)
        .map(|index| {
            (
                format!("{remote_dir}/file-{index:03}.bin"),
                format!("initial-{index:03}").into_bytes(),
                format!("replacement-{index:03}").into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    let created = replica
        .posix_create_files_local_eventual_with_known_parents(
            files
                .iter()
                .map(|(path, initial, _)| (path.as_str(), initial.as_slice())),
        )
        .expect("initial remote-primary bulk create");
    let created_versions = created
        .iter()
        .map(|stat| (stat.object_id.clone(), stat.version.clone()))
        .collect::<BTreeMap<_, _>>();
    let parent_object_id = replica
        .posix_stat_distributed(remote_dir)
        .expect("metadata-less overwrite parent before eviction")
        .object_id;

    // Match a newly opened one-shot CLI requester: retain the content store,
    // but remove the parent and every child from the memory overlay and drop
    // the durable metadata backend. The overwrite must discover the exact
    // parent and child object/version identities from the metadata authority
    // in one bounded LookupPaths request.
    replica.metadata.remove_object(&parent_object_id);
    for stat in &created {
        replica.metadata.remove_object(&stat.object_id);
    }
    replica.metadata_backend = None;
    assert!(
        replica
            .metadata
            .object_by_normalized_posix_path(&sifs_file_system_id(), remote_dir)
            .is_none(),
        "test requester unexpectedly retained parent metadata"
    );
    for (path, _, _) in &files {
        assert!(
            replica
                .metadata
                .object_by_normalized_posix_path(&sifs_file_system_id(), path)
                .is_none(),
            "test requester unexpectedly retained child metadata for {path}"
        );
    }

    let overwritten = replica
        .posix_write_files_local_eventual_with_known_parents(
            files
                .iter()
                .map(|(path, _, replacement)| (path.as_str(), replacement.as_slice())),
        )
        .expect("metadata-less remote-primary bulk overwrite");
    assert_eq!(overwritten.len(), files.len());
    for ((path, _, replacement), stat) in files.iter().zip(&overwritten) {
        assert_eq!(stat.path, *path);
        assert_eq!(
            stat.object_id,
            created
                .iter()
                .find(|created| created.path == *path)
                .expect("created object")
                .object_id
        );
        assert_ne!(
            stat.version,
            *created_versions
                .get(&stat.object_id)
                .expect("created object version")
        );
        assert_eq!(
            stat.content_hash.as_deref(),
            Some(content_hash(replacement).as_str()),
            "metadata-less overwrite returned the wrong content for {path}"
        );
    }

    stop.store(true, Ordering::SeqCst);
    let mut services_primary = services_thread.join().expect("services primary thread");
    for (path, _, replacement) in &files {
        assert_eq!(
            services_primary
                .posix_stat_distributed(path)
                .expect("stat authoritative overwrite")
                .content_hash
                .as_deref(),
            Some(content_hash(replacement).as_str()),
            "metadata authority retained the wrong content generation for {path}"
        );
    }

    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_metadata_less_bulk_overwrite_follows_authoritative_symlink() {
    let mut cluster = sifs_two_primary_cluster("sifs-bulk-metadata-less-symlink");
    let remote_dir = "/services/bulk-metadata-less-symlink";
    let target = "/services/bulk-metadata-less-symlink/target.txt";
    let link = "/services/bulk-metadata-less-symlink/link.txt";
    cluster
        .services_primary
        .posix_mkdir_all(remote_dir)
        .expect("remote symlink parent");
    sifs_two_primary_publish_until(&mut cluster, "metadata-less symlink parent", |cluster| {
        super::cross_shard::node_store_object_at(&cluster._runtime, &cluster.replica_node, remote_dir)
            .is_some()
    });

    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
        root,
        services_primary,
        replica_node,
        mut replica,
        ..
    } = cluster;
    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));
    let options = DfsWriteOptions::replicated_eventual(vec![replica_node]);
    replica
        .posix_write_file(target, b"before", options.clone())
        .expect("write symlink target");
    replica
        .posix_symlink("target.txt", link, options)
        .expect("create symlink");
    let target_before = replica
        .posix_stat_distributed(target)
        .expect("stat symlink target");
    let link_before = replica
        .posix_lstat_distributed(link)
        .expect("lstat symlink before metadata eviction");
    assert_eq!(link_before.kind, DfsObjectKind::Symlink);
    let parent_id = replica
        .posix_stat_distributed(remote_dir)
        .expect("stat symlink parent")
        .object_id;

    for object_id in [&parent_id, &target_before.object_id, &link_before.object_id] {
        replica.metadata.remove_object(object_id);
    }
    replica.metadata_backend = None;

    let stat = replica
        .posix_write_files_local_eventual_with_known_parents([(
            link,
            b"through authoritative symlink".as_slice(),
        )])
        .expect("metadata-less write through authoritative symlink")
        .pop()
        .expect("symlink write stat");
    assert_eq!(stat.path, link);
    assert_eq!(stat.object_id, target_before.object_id);
    assert_eq!(
        replica
            .posix_read_file_distributed(target)
            .expect("read symlink target"),
        b"through authoritative symlink"
    );
    let link_after = replica
        .posix_lstat_distributed(link)
        .expect("lstat symlink after overwrite");
    assert_eq!(link_after.kind, DfsObjectKind::Symlink);
    assert_eq!(link_after.object_id, link_before.object_id);

    stop.store(true, Ordering::SeqCst);
    let mut services_primary = services_thread.join().expect("services primary thread");
    assert_eq!(
        services_primary
            .posix_stat_distributed(target)
            .expect("authoritative symlink target stat")
            .content_hash
            .as_deref(),
        Some(content_hash(b"through authoritative symlink").as_str())
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_non_replica_requester_routes_namespace_op_with_read_your_write() {
    let cluster = sifs_outsider_cluster("sifs-outsider-routed-namespace");
    let SifsPosixCluster {
        root,
        primary,
        mut observer,
        _runtime,
        ..
    } = cluster;

    // The public Main client retains the real routed result and reads through
    // the owner; it does not author a second local journal mirror.
    observer
        .posix_mkdir_all("/users/outsider/nested")
        .expect("routed mkdir from non-replica requester");

    // Read-your-write through the normal current owner with no private pump.
    assert_eq!(
        observer
            .posix_stat_distributed("/users/outsider/nested")
            .expect("outsider reads its own routed namespace write")
            .kind,
        DfsObjectKind::Directory
    );

    drop(primary);
    drop(observer);
    _runtime.close().expect("normal native owner drain");
}

pub(super) fn finish_native_placement_fixture_apply(
    runtime: &crate::test_runtime::DfsTestRuntime,
    accepted: &DfsPlacementReconciliationJob,
) {
    let coordinator = runtime.root_shard().unwrap().primary_node_id;
    let control = runtime.shared_control_plane().unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let job = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .unwrap()
            .unwrap();
        assert_eq!(job.accepted_config, accepted.accepted_config);
        assert!(job.last_error.is_none(), "actual fixture Apply: {job:?}");
        if job.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "finish the accepted fixture policy before its dependent operation: {job:?}"
        );
        runtime
            .maintenance_tick(&coordinator, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
}

struct NativeMainAbortFixture {
    runtime: crate::test_runtime::DfsTestRuntime,
    writer: NetworkedDfsService<DfsNodeTransport>,
    main: NodeId,
    requester: NodeId,
    path: String,
    base: DfsStat,
    request: DfsWriteRequest,
    operation_id: String,
    intent_id: String,
    bytes: Vec<u8>,
    exchange: crate::test_runtime::CapturedStageExchange,
    shard: DfsMetadataShardId,
}
impl NativeMainAbortFixture {
    fn new(name: &str) -> Self {
        Self::with_node_count(name, 2)
    }
    fn with_node_count(name: &str, node_count: usize) -> Self {
        let (runtime, initial, config) =
            crate::test_support::current_native_metadata_service(name, node_count, "/services")
                .unwrap();
        let main = config.primary_node_id.clone();
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, node_count);
        let requester = nodes.iter().find(|node| **node != main).unwrap().clone();
        drop(initial);
        let content_nodes = vec![main.clone(), requester.clone()];
        let path = format!("/services/{name}.bin");
        let mut first =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
        first.required_node_ids = vec![main.clone()];
        let first_apply = runtime
            .apply_placement_policy(&requester, &path, DfsPlacementScope::SelfOnly, first)
            .unwrap();
        finish_native_placement_fixture_apply(&runtime, &first_apply);
        let mut writer = runtime.client(&requester).unwrap();
        let (resolved, _, base_policy) = writer
            .managed_replication_policy_for_path(&path, DfsObjectPolicy::ReplicatedConfirmed, 1)
            .unwrap();
        assert_eq!(resolved.policy.required_node_ids, vec![main.clone()]);
        let base = writer
            .posix_write_file(
                &path,
                b"original visible bytes",
                DfsWriteOptions {
                    policy: base_policy,
                    replica_candidates: vec![main.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: true,
                },
            )
            .unwrap();
        drop(writer);
        let mut both = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
        both.required_node_ids = content_nodes.clone();
        let both_apply = runtime
            .apply_placement_policy(&requester, &path, DfsPlacementScope::SelfOnly, both)
            .unwrap();
        finish_native_placement_fixture_apply(&runtime, &both_apply);
        let current = runtime.metadata_primary_service(&path).unwrap();
        let current_base = current.posix_stat(&path).unwrap();
        assert_eq!(current_base.object_id, base.object_id);
        assert_eq!(current_base.content_hash, base.content_hash);
        let holders = current
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&current_base.object_id, &current_base.version)
            .unwrap()
            .into_iter()
            .map(|receipt| receipt.replica_node_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            holders,
            content_nodes.iter().cloned().collect(),
            "the predecessor's actual W2 placement must complete before the lost-Stage case"
        );
        drop(current);
        let base = current_base;
        let capture = Arc::new(Mutex::new(
            crate::test_runtime::StageExchangeCapture::default(),
        ));
        runtime.install_fault(crate::test_runtime::FaultRule {
            source: Some(main.clone()),
            target: Some(requester.clone()),
            route: Some(crate::DFS_DATA_STAGE_BLOB_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::CaptureStageExchangeAndLoseAfterDispatch(
                Arc::clone(&capture),
            ),
        });
        let mut writer = runtime.client(&requester).unwrap();
        let operation_id = format!("abort_{}", sinomo_core::Id::generate());
        let request = DfsWriteRequest {
            file_system_id: base.file_system_id.clone(),
            object_id: base.object_id.clone(),
            target_version: crate::intent::stable_write_target_version(
                &requester,
                &base.object_id,
                &operation_id,
            ),
            requesting_node_id: requester.clone(),
        };
        let bytes = vec![0x49; 2 * 1024 * 1024 + 4096];
        let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
        policy.required_node_ids = content_nodes.clone();
        assert!(matches!(
            writer
                .write_distributed_reader_with_candidates(
                    request.clone(),
                    std::io::Cursor::new(&bytes),
                    content_nodes,
                    policy,
                    DfsDistributedWriteOptions {
                        operation_id: Some(operation_id.clone()),
                        ..Default::default()
                    },
                )
                .unwrap(),
            NetworkedDfsWriteOutcome::Pending { .. }
        ));
        writer.process_incoming().unwrap();
        let exchange = {
            let captured = capture.lock().unwrap();
            assert_eq!(captured.dropped_count, 0);
            assert!(
                !captured.exchanges.is_empty(),
                "the normal Main must dispatch its genuine Stage"
            );
            captured.exchanges[0].clone()
        };
        let DfsNetworkMessage::StageBlob(stage) = &exchange.command.routed_message.message else {
            panic!("normal current write must produce its actual current Stage command")
        };
        assert_eq!(stage.object_id, request.object_id);
        assert_eq!(stage.version, request.target_version);
        assert_eq!(exchange.source_node_id, main);
        assert_eq!(exchange.target_node_id, requester);
        assert_eq!(exchange.body_bytes, Some(bytes.len() as u64));
        let intent_id = stage.intent_id.clone();
        let tracked = writer
            .writes
            .pending_routed_writes
            .get(&intent_id)
            .expect("actual Main Pending response registered its real intent alias");
        assert!(
            tracked.command.is_none(),
            "aliases never duplicate the canonical body"
        );
        let canonical = writer
            .writes
            .pending_routed_writes
            .get(&tracked.canonical_request_id)
            .expect("actual alias still names the original canonical request");
        assert_eq!(canonical.command.as_ref().unwrap().request_id, operation_id);
        assert_eq!(
            canonical.remote_intent_id.as_deref(),
            Some(intent_id.as_str())
        );
        assert!(canonical.result.is_none());
        assert!(
            canonical.compact_retained_at.is_none(),
            "actual Pending is not a terminal outcome"
        );
        let observed = runtime.node_metadata(&main).unwrap();
        let record = observed
            .get_intent(&request.object_id, &intent_id)
            .unwrap()
            .unwrap();
        assert_eq!(record.operation_id.as_deref(), Some(operation_id.as_str()));
        assert_eq!(record.coordinator_node_id, requester);
        assert!(crate::intent::write_intent_status_is_active(&record.status));
        assert_eq!(
            (
                record.policy.min_commit_replicas,
                record.policy.desired_replicas
            ),
            (2, 2)
        );
        let receipts = observed
            .receipts_for_version(&request.object_id, &request.target_version)
            .unwrap();
        let holders = receipts
            .iter()
            .filter(|receipt| {
                receipt.content_hash.as_deref() == Some(content_hash(&bytes).as_str())
            })
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            holders,
            BTreeSet::from([main.clone()]),
            "lost genuine Stage delivery cannot become a Main receipt"
        );
        drop(observed);
        Self {
            runtime,
            writer,
            main,
            requester,
            path,
            base,
            request,
            operation_id,
            intent_id,
            bytes,
            exchange,
            shard: DfsMetadataShardId::new(config.shard_id),
        }
    }
    fn record(&self) -> DfsWriteIntentRecord {
        self.runtime
            .node_metadata(&self.main)
            .unwrap()
            .get_intent(&self.request.object_id, &self.intent_id)
            .unwrap()
            .unwrap()
    }
    fn prefix(&self) -> DfsMetadataPrefixProof {
        self.runtime
            .node_metadata(&self.main)
            .unwrap()
            .metadata_prefix_proof(&self.shard)
            .unwrap()
            .expect("actual native prefix")
    }
    fn abort_operation(&self) -> DfsNamespaceOperation {
        DfsNamespaceOperation::AbortPendingWrite {
            path: self.path.clone(),
            object_id: self.request.object_id.clone(),
            target_version: self.request.target_version.clone(),
            intent_id: self.intent_id.clone(),
            write_operation_id: self.operation_id.clone(),
        }
    }
    fn assert_terminal_caller_body_cleared(&self) {
        assert!(
            !self
                .writer
                .writes
                .pending_routed_writes
                .values()
                .any(|pending| pending.canonical_request_id == self.operation_id
                    || pending.remote_intent_id.as_deref() == Some(self.intent_id.as_str())
                    || pending
                        .command
                        .as_ref()
                        .is_some_and(|command| command.request_id == self.operation_id)),
            "only an actual terminal abort response retires all copies of its canonical command"
        );
    }
    fn assert_current_unchanged(&mut self) {
        let current = self.writer.posix_stat_distributed(&self.path).unwrap();
        assert_eq!(current.object_id, self.base.object_id);
        assert_eq!(current.version, self.base.version);
        assert_eq!(current.content_hash, self.base.content_hash);
        assert_eq!(
            self.writer.posix_read_file_distributed(&self.path).unwrap(),
            b"original visible bytes"
        );
    }
    fn close(self) {
        drop(self.writer);
        self.runtime.clear_faults();
        self.runtime.close().unwrap();
    }
}

#[test]
fn normal_main_abort_replays_actual_durable_outcome_after_lost_reply_and_restart() {
    let mut case = NativeMainAbortFixture::new("native-main-abort-lost-reply");
    case.runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(case.requester.clone()),
        target: Some(case.main.clone()),
        route: Some(crate::DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::LoseReplyAfterDispatch,
    });
    let uncertain = case
        .writer
        .abort_pending_write(&case.intent_id)
        .unwrap_err();
    assert!(
        matches!(
            uncertain,
            DfsError::NetworkDelivery {
                delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                ..
            }
        ),
        "lost actual abort reply must retain delivery uncertainty: {uncertain}"
    );
    assert!(
        case.writer
            .writes
            .pending_routed_writes
            .values()
            .any(|pending| pending
                .command
                .as_ref()
                .is_some_and(|command| command.request_id == case.operation_id))
    );
    let durable = case.record();
    assert_eq!(durable.status, DfsWriteIntentStatus::Aborted);
    // The genuine loss and durable abort are now observed. An exhausted
    // one-hit rule deliberately fails later matching requests, so heal this
    // completed negative phase before ordinary inspection/restart/replay.
    case.runtime.assert_fault_budgets().unwrap();
    case.runtime.clear_faults();
    let after_abort = case.prefix();
    case.assert_current_unchanged();
    case.runtime.stop_node(&case.main).unwrap();
    case.runtime.reopen_node(case.main.clone()).unwrap();
    let replayed = case.writer.abort_pending_write(&case.intent_id).unwrap();
    assert_eq!(
        replayed,
        crate::intent::distributed_intent_from_record(&durable)
    );
    case.assert_terminal_caller_body_cleared();
    assert_eq!(case.record(), durable);
    assert_eq!(
        case.prefix(),
        after_abort,
        "durable abort replay must not author another native frame"
    );
    case.assert_current_unchanged();
    case.close();
}

#[test]
fn normal_main_abort_rejects_other_actor_and_fences_actual_late_stage_reply() {
    let mut case = NativeMainAbortFixture::new("native-main-abort-actor-and-late-stage");
    let before = case.prefix();
    let unknown = format!("{}:unknown-alias", case.intent_id);
    assert!(
        case.writer
            .abort_pending_write(&unknown)
            .unwrap_err()
            .to_string()
            .contains("has no retained original request")
    );
    assert_eq!(case.prefix(), before);
    let canonical_id = case
        .writer
        .writes
        .pending_routed_writes
        .get(&case.intent_id)
        .unwrap()
        .canonical_request_id
        .clone();
    let canonical = case
        .writer
        .writes
        .pending_routed_writes
        .remove(&canonical_id)
        .expect("actual canonical caller obligation");
    // Negative caller-state loss: the genuine alias survives but its original
    // body is unavailable. Main intent/guard/receipts are never altered.
    assert!(
        case.writer
            .abort_pending_write(&case.intent_id)
            .unwrap_err()
            .to_string()
            .contains("has no retained original request")
    );
    assert_eq!(case.prefix(), before);
    assert!(crate::intent::write_intent_status_is_active(
        &case.record().status
    ));
    case.writer
        .writes
        .pending_routed_writes
        .insert(canonical_id, canonical);
    let operation = case.abort_operation();
    let mut other = case.runtime.client(&case.main).unwrap();
    let response = other
        .route_namespace_operation_to_metadata_primary_with_request_id_and_options(
            format!("wrong-actor:{}", case.operation_id),
            case.request.file_system_id.clone(),
            case.main.clone(),
            operation,
            crate::DFS_ROUTED_NAMESPACE_OP_TIMEOUT,
            false,
            false,
        )
        .unwrap();
    assert!(
        matches!(response.result, DfsNamespaceResult::Rejected { reason, .. }
        if reason.contains("original durable actor or intent binding"))
    );
    assert_eq!(case.prefix(), before);
    assert!(crate::intent::write_intent_status_is_active(
        &case.record().status
    ));
    drop(other);
    // Keep a genuine earlier owning-Core view across the normal Main abort.
    // No coordinator status, durable record, epoch or receipt is fabricated.
    let mut earlier = case.runtime.metadata_primary_service(&case.path).unwrap();
    earlier.hydrate_from_metadata_backend().unwrap();
    assert!(matches!(
        earlier.coordinator.intent(&case.intent_id).unwrap().state,
        DfsWriteIntentState::AwaitingReceipts
    ));
    let aborted = case.writer.abort_pending_write(&case.intent_id).unwrap();
    assert_eq!(aborted.state, DfsWriteIntentState::Aborted);
    case.assert_terminal_caller_body_cleared();
    let prefix = case.prefix();
    let terminal = case.record();
    assert_eq!(terminal.status, DfsWriteIntentStatus::Aborted);
    assert!(matches!(
        earlier.coordinator.intent(&case.intent_id).unwrap().state,
        DfsWriteIntentState::AwaitingReceipts
    ));
    let replay = earlier.capture_mounted_mutation(|service| {
        service.process_routed_messages_with_options(vec![case.exchange.response.clone()], false)
    });
    let events = replay
        .outcome
        .expect("actual closed tuple rejects the original late response");
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::BlobStageRejected { intent_id, object_id, replica_node_id, .. }
        if intent_id == &case.intent_id && object_id == &case.request.object_id
            && replica_node_id == &case.requester)));
    assert_eq!(replay.committed_work, DfsCommittedWork::default());
    assert_eq!(
        case.record(),
        terminal,
        "exact aborted body remains unchanged"
    );
    assert_eq!(
        case.prefix(),
        prefix,
        "late receipt cannot append an authoritative frame"
    );
    drop(earlier);
    let mut recipient = case.runtime.client(&case.requester).unwrap();
    recipient
        .transport
        .send_routed_call(
            &case.main,
            crate::DfsRoutedCall::value(case.exchange.response.clone()),
        )
        .unwrap();
    drop(recipient);
    assert_eq!(case.record().status, DfsWriteIntentStatus::Aborted);
    assert_eq!(
        case.prefix(),
        prefix,
        "the actual late Stage ACK cannot publish aborted content"
    );
    case.assert_current_unchanged();
    let actual_receiver = case.runtime.client(&case.requester).unwrap();
    assert_eq!(
        actual_receiver
            .content()
            .read_blob(&content_hash(&case.bytes))
            .unwrap(),
        case.bytes,
        "abort fences publication and does not claim erasure of a real received copy"
    );
    drop(actual_receiver);
    case.close();
}

#[test]
fn normal_main_abort_publication_failure_retains_pending_until_actual_recovery() {
    let name = "native-main-abort-publication-failure";
    let mut case = NativeMainAbortFixture::with_node_count(name, 3);
    let original = case.record();
    let config = case
        .runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|config| {
            config.file_system_id == case.request.file_system_id && config.shard_id == case.shard.0
        })
        .unwrap();
    assert_eq!(config.primary_node_id, case.main);
    let participants = std::iter::once(config.primary_node_id.clone())
        .chain(config.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    assert_eq!(participants.len(), 3);
    let unavailable = participants
        .iter()
        .find(|node| **node != case.main && **node != case.requester)
        .unwrap()
        .clone();
    let mut owner = case.runtime.metadata_primary_service(&case.path).unwrap();
    assert_eq!(
        owner
            .metadata_backend()
            .unwrap()
            .get_intent(&case.request.object_id, &case.intent_id)
            .unwrap(),
        Some(original.clone())
    );
    for route in [
        crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
        crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
    ] {
        case.runtime.install_fault(crate::test_runtime::FaultRule {
            source: Some(case.main.clone()),
            target: Some(unavailable.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    }
    // The original Main producer supplied the genuine pending actor/body.
    // Enter its ordinary typed owner-side abort so a native participant denial
    // is not converted into the namespace RPC's formatted rejection string.
    let rejected = owner
        .abort_routed_pending_write(
            &case.requester,
            &case.request.file_system_id,
            &case.abort_operation(),
        )
        .unwrap_err();
    assert!(
        matches!(&rejected, DfsError::MetadataRecoveryQuorumUnavailable {
        file_system_id, shard_id, required, verified,
    } if file_system_id == &case.request.file_system_id && shard_id == &case.shard.0
        && *required == participants.len() && *verified + 1 == *required),
        "{rejected:?}"
    );
    assert_eq!(
        owner
            .metadata_backend()
            .unwrap()
            .get_intent(&case.request.object_id, &case.intent_id)
            .unwrap(),
        Some(original.clone()),
        "failed ALL-assigned publication cannot install terminal intent effects"
    );
    let aborted_prepares = owner.metadata_backend().unwrap().list_prepared_operations().unwrap().into_iter()
        .filter(|record| record.file_system_id == case.request.file_system_id && record.shard_id == case.shard
            && record.metadata_effects.iter().any(|effect| matches!(effect,
                DfsPreparedMetadataEffect::PutIntent { intent }
                if intent.intent_id == case.intent_id && intent.object_id == case.request.object_id
                    && intent.target_version == case.request.target_version && intent.status == DfsWriteIntentStatus::Aborted)))
        .collect::<Vec<_>>();
    assert_eq!(
        aborted_prepares.len(),
        1,
        "the exact failed abort remains durably Prepared"
    );
    let prepared = &aborted_prepares[0];
    prepared.validate().unwrap();
    assert_eq!(prepared.status, DfsPreparedOperationStatus::Prepared);
    assert_eq!(prepared.owner_epoch, config.epoch);
    assert!(
        owner
            .metadata_backend()
            .unwrap()
            .committed_prepared_operation(&prepared.digest)
            .unwrap()
            .is_none()
    );
    let candidate = prepared
        .metadata_effects
        .iter()
        .find_map(|effect| match effect {
            DfsPreparedMetadataEffect::PutIntent { intent }
                if intent.intent_id == case.intent_id =>
            {
                Some(intent.clone())
            }
            _ => None,
        })
        .unwrap();
    let mut unchanged = candidate.clone();
    unchanged.status = original.status.clone();
    unchanged.updated_at = original.updated_at;
    assert_eq!(
        unchanged, original,
        "the candidate retains every original pending body field"
    );
    assert_eq!(case.record(), original);
    assert!(
        case.writer
            .writes
            .pending_routed_writes
            .values()
            .any(|pending| pending
                .command
                .as_ref()
                .is_some_and(|command| command.request_id == case.operation_id))
    );
    case.assert_current_unchanged();
    assert_eq!(
        case.runtime.remove_faults_matching(|rule| {
            rule.source.as_ref() == Some(&case.main)
                && rule.target.as_ref() == Some(&unavailable)
                && rule.call_id.is_none()
                && rule.origin.is_none()
                && matches!(
                    rule.route.as_deref(),
                    Some(
                        crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                            | crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE
                    )
                )
                && matches!(
                    &rule.action,
                    crate::test_runtime::FaultAction::BudgetExhausted
                )
        }),
        2,
        "heal only the actual unavailable participant's metadata routes"
    );
    case.runtime.assert_fault_budgets().unwrap();
    drop(owner);
    case.runtime
        .request_repair_reconciliation(&case.main)
        .unwrap();
    case.runtime
        .maintenance_tick(&case.main, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap()
        .outcome
        .unwrap();
    let recovered = case.runtime.metadata_primary_service(&case.path).unwrap();
    let terminal = recovered
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&prepared.operation_id)
        .unwrap()
        .unwrap();
    terminal.validate().unwrap();
    assert_eq!(terminal.status, DfsPreparedOperationStatus::Committed);
    assert_eq!(terminal.digest, prepared.digest);
    assert_eq!(
        terminal.payload, prepared.payload,
        "normal recovery commits the SAME sealed abort body and ID"
    );
    assert!(
        recovered
            .metadata_backend()
            .unwrap()
            .committed_prepared_operation(&prepared.digest)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        recovered
            .metadata_backend()
            .unwrap()
            .get_intent(&case.request.object_id, &case.intent_id)
            .unwrap(),
        Some(candidate)
    );
    drop(recovered);
    let aborted = case.writer.abort_pending_write(&case.intent_id).unwrap();
    assert_eq!(aborted.state, DfsWriteIntentState::Aborted);
    case.assert_terminal_caller_body_cleared();
    assert_eq!(case.record().status, DfsWriteIntentStatus::Aborted);
    case.assert_current_unchanged();
    case.close();
}

#[test]
fn normal_main_foreign_target_collision_preserves_the_actual_unprepared_claim() {
    let mut case = NativeMainAbortFixture::new("native-main-foreign-target-collision");
    let original = case.record();
    let operation = format!("write:{}", case.intent_id);
    let owner = case.runtime.node_metadata(&case.main).unwrap();
    assert!(
        owner
            .prepared_operation_record(&operation)
            .unwrap()
            .is_none(),
        "lost actual Stage reply leaves no complete write prepare"
    );
    drop(owner);
    let journal = LocalSobjMetadataBackend::open_for_inspection(
        case.shard.clone(),
        default_dfs_metadata_shards_root(case.runtime.data_dir(&case.main))
            .join(crate::metadata_keys::key_segment(&case.shard.0)),
    )
    .unwrap();
    let limit = DfsMetadataJournalReplicationOptions::default().batch_entry_limit;
    let before = journal.journal_since_limited(0, limit).unwrap();
    assert!(
        before.len() < limit,
        "complete bounded journal before collision"
    );
    let mut foreign = case.runtime.client(&case.main).unwrap();
    let mut request = case.request.clone();
    request.requesting_node_id = case.main.clone();
    let collision_id = format!("foreign_{}", sinomo_core::Id::generate());
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = vec![case.main.clone(), case.requester.clone()];
    let requested_deadline =
        std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let previous_deadline = foreign.transport.operation_deadline();
    foreign.transport.set_operation_deadline(Some(
        previous_deadline.map_or(requested_deadline, |previous| {
            previous.min(requested_deadline)
        }),
    ));
    let dispatched = foreign.write_distributed_reader_with_candidates(
        request,
        std::io::Cursor::new(&case.bytes),
        vec![case.main.clone(), case.requester.clone()],
        policy,
        DfsDistributedWriteOptions {
            operation_id: Some(collision_id),
            ..Default::default()
        },
    );
    // A queued Pending is not the Main's decision. Consume the same real
    // caller inbox through the normal settling path and unchanged network budget.
    let received =
        dispatched.and_then(|outcome| foreign.settle_ready_write_outcome(outcome, false));
    foreign.transport.set_operation_deadline(previous_deadline);
    let rejection = received.unwrap_err();
    assert!(
        rejection.to_string().contains(&case.intent_id),
        "{rejection}"
    );
    assert_eq!(case.record(), original);
    let owner = case.runtime.node_metadata(&case.main).unwrap();
    assert!(
        owner
            .prepared_operation_record(&operation)
            .unwrap()
            .is_none()
    );
    drop(owner);
    let after = journal.journal_since_limited(0, limit).unwrap();
    assert!(
        after.len() < limit,
        "complete bounded journal after collision"
    );
    assert_eq!(
        after, before,
        "foreign collision must not author a terminal intent"
    );
    case.assert_current_unchanged();
    drop(foreign);
    case.close();
}
