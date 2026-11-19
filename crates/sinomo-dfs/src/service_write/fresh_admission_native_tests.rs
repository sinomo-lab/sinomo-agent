use super::*;
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcOrigin, RpcPhase};
use std::time::Duration;

fn fresh_admission_read_only_facts(
    runtime: &DfsTestRuntime,
    writer: &NetworkedDfsService<DfsNodeTransport>,
    roles: [(&str, &NodeId); 3],
    operation: &str,
    identity: (&DfsObjectId, &DfsVersion, &str, &str),
    rpc_after: usize,
) -> serde_json::Value {
    // This is a bounded fixture diagnostic, not recovery or a durability
    // certifier. Existing read views may refresh resident projections; these
    // calls never enter a writer, sync, maintenance or publication API.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (object, version, intent, hash) = identity;
        let result = |value: Result<serde_json::Value, String>| match value {
            Ok(value) => serde_json::json!({"ok": value}),
            Err(error) => serde_json::json!({"unavailable": error.chars().take(2048).collect::<String>(), "error_truncated": error.chars().count() > 2048}),
        };
        let manifest_probe = |store: &LocalContentStore| result(store.read_manifest(object.as_str(), version.as_str())
            .map(|manifest| serde_json::json!({"object": manifest.object_id, "version": manifest.version, "hash": manifest.content_hash, "size": manifest.size_bytes, "extent_count": manifest.extents.len()}))
            .map_err(|error| error.to_string()));
        let blob_probe = |store: &LocalContentStore| result(store.read_blob_range(hash, 0, 4096)
            .map(|bytes| serde_json::json!({"bytes": bytes.len(), "hash": crate::content_hash(&bytes), "matches_fixture_input": bytes == vec![0x6d; 4096]}))
            .map_err(|error| error.to_string()));
        let metadata = writer.metadata_backend().map_or_else(|| serde_json::json!({"unavailable": "existing requester has no metadata read owner"}), |backend| {
            serde_json::json!({
                "object": result(backend.get_object(object).map(|value| serde_json::json!(value)).map_err(|error| error.to_string())),
                "intent": result(backend.get_intent(object, intent).map(|value| serde_json::json!(value)).map_err(|error| error.to_string())),
                "receipts": result(backend.receipts_for_version(object, version).map(|values| serde_json::json!({"count": values.len(), "first_four": values.iter().take(4).collect::<Vec<_>>(), "omitted": values.len().saturating_sub(4)})).map_err(|error| error.to_string())),
            })
        });
        let roots = roles.into_iter().map(|(role, node)| {
            let data_dir = runtime.data_dir(node);
            let root = data_dir.dfs_dir().join("content");
            let owner = crate::runtime_host::publication_runtime_owner(
                &crate::default_dfs_metadata_shards_root(data_dir),
            ).map(|owner| owner.map(|owner| serde_json::json!({
                "physical_data_root": owner.physical_data_root(),
                "held_owner_file_identity": result(owner.storage_owner_file_identity().map(|value| serde_json::json!(value)).map_err(|error| error.to_string())),
            }))).map_err(|error| error.to_string());
            let inspection = LocalContentStore::open_for_inspection(&root);
            serde_json::json!({"role": role, "node": node, "expected_content_root": root,
                "retained_native_owner": result(owner.map(|value| serde_json::json!(value))),
                "exact_version_manifest": manifest_probe(&inspection), "bounded_blob_read": blob_probe(&inspection),
                "content_store_incarnation": "not exposed by this read API; runner preserves raw sentinel/catalog/segments"})
        }).collect::<Vec<_>>();
        let matching_pending = writer.writes.pending_routed_writes.iter()
            .filter(|(_, pending)| pending.canonical_request_id == operation)
            .collect::<Vec<_>>();
        let pending = matching_pending.iter().take(4).map(|(key, pending)| serde_json::json!({
                "key": key, "canonical_request": pending.canonical_request_id, "object": pending.object_id,
                "primary": pending.primary_node_id, "authority_shard": pending.authority_shard,
                "remote_intent": pending.remote_intent_id, "durable_receipts": pending.durable_receipts,
                "required_receipts": pending.required_receipts, "command_present": pending.command.is_some(),
                "attachment_present": pending.attachment_bytes.is_some(),
                "result_preview": format!("{:?}", pending.result).chars().take(2048).collect::<String>(),
                "result_preview_limit_chars": 2048,
            })).collect::<Vec<_>>();
        let calls = runtime.rpc_observations();
        let matching = calls.iter().skip(rpc_after).filter(|call| {
            call.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                || call.dfs_object_id.as_ref() == Some(object)
        }).collect::<Vec<_>>();
        serde_json::json!({"expected_identity": {"operation": operation, "object": object, "version": version, "intent": intent, "hash": hash},
            "metadata_from_existing_read_owner": metadata, "roots": roots,
            "existing_requester_content": {"manifest": manifest_probe(writer.content()), "bounded_blob": blob_probe(writer.content())},
            "pending_matching_count": matching_pending.len(), "pending_first_four": pending, "pending_omitted": matching_pending.len().saturating_sub(4), "matching_rpc_count": matching.len(),
            "matching_rpc_last_sixteen": matching.iter().rev().take(16).map(|call| serde_json::json!({
                "source": call.source, "target": call.target, "call": call.call_id, "route": call.route,
                "phase": format!("{:?}", call.phase), "request": call.dfs_request_id, "intent": call.dfs_intent_id,
                "object": call.dfs_object_id, "version": call.dfs_version, "error": call.outcome_error,
            })).collect::<Vec<_>>(), "rpc_observations_dropped": runtime.observation_dropped_count(),
            "qualification": "Observations only: readable bytes do not prove current holder authority; no sync/recovery/publication was requested."})
    })).unwrap_or_else(|_| serde_json::json!({"unavailable": "read-only fixture diagnostic panicked; original test verdict retained"}))
}

#[test]
fn native_fresh_background_write_keeps_n_three_with_two_content_receipts_and_heals() {
    let name = "native-fresh-background-content-admission";
    let path = "/services/fresh-background-content-admission.bin";
    let operation = "fresh-background-content-admission-operation";
    let (runtime, mut writer, shard) =
        crate::test_support::current_native_metadata_service(name, 3, path).unwrap();
    let b = shard.primary_node_id.clone();
    assert_eq!(
        shard.replica_node_ids.len(),
        2,
        "actual three-member metadata assignment"
    );
    let a = shard.replica_node_ids[0].clone();
    let c = shard.replica_node_ids[1].clone();
    let bytes = vec![0x6d; 4096];
    let hash = crate::content_hash(&bytes);
    let original_object = crate::intent::stable_create_object_id(&b, path, operation);
    let original_version =
        crate::intent::stable_write_target_version(&b, &original_object, operation);
    let original_intent = crate::intent::stable_write_intent_id(&b, &original_object, operation);
    let mut facts = serde_json::json!({"original_reply_lost": false, "original_operation_reconciled": false, "durable_deficit_reopened": false, "healed": false});
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut managed =
            crate::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 2, 3).unwrap();
        // Ordinary managed constraints select B/C before optional A. Denying
        // A's content RPC does not establish whole-node path unavailability.
        managed.required_node_ids = vec![b.clone(), c.clone()];
        runtime
            .apply_placement_policy(&b, path, crate::DfsPlacementScope::Subtree, managed)
            .unwrap();
        let mut policy_owner = runtime.metadata_primary_service(path).unwrap();
        let policy = policy_owner
            .managed_write_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 3)
            .unwrap();
        assert_eq!(
            (policy.min_commit_replicas, policy.desired_replicas),
            (2, 3)
        );
        assert_eq!(policy.required_node_ids, vec![b.clone(), c.clone()]);
        drop(policy_owner);
        assert!(writer.transport.delegates_local_namespace_authority());
        let actual_snapshot = writer.placement_snapshot_cache().snapshot();
        facts["actual_initial_placement_observations"] = serde_json::json!({
            "snapshot_generation": actual_snapshot.as_ref().map(|snapshot| snapshot.generation),
            "paths": actual_snapshot.as_ref().map(|snapshot| snapshot.paths.iter().map(|path| {
                serde_json::json!({"source": &path.source_node_id, "target": &path.target_node_id,
                    "state": format!("{:?}", path.state)})
            }).collect::<Vec<_>>()),
            "fixture_published_path_observations": false,
        });
        for route in [
            crate::DFS_DATA_STAGE_BLOB_ROUTE,
            crate::DFS_DATA_STAGE_PATCH_ROUTE,
            crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
        ] {
            runtime.install_fault(FaultRule {
                source: None,
                target: Some(a.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: Some(RpcOrigin::RegisteredHost),
                remaining_hits: 16,
                action: FaultAction::TransientRouteFailureBeforeDispatch,
            });
        }
        let before_calls = runtime.rpc_observations().len();
        let options = crate::DfsWriteOptions {
            policy,
            replica_candidates: vec![b.clone(), c.clone(), a.clone()],
            wait_for_commit: true,
            foreground_replica_repair: false,
        };
        let served = runtime.capture_served_work(&b, 32).unwrap();
        runtime.install_fault(FaultRule {
            source: Some(b.clone()),
            target: Some(b.clone()),
            route: Some(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into()),
            call_id: None,
            origin: Some(RpcOrigin::RegisteredHost),
            remaining_hits: 8,
            action: FaultAction::LoseReplyAfterDispatch,
        });
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let previous = writer.set_transport_operation_deadline(Some(deadline));
        let written =
            writer.posix_create_file_with_operation_id(path, &bytes, options.clone(), operation);
        let lost = written.expect_err("actual original Main write reply must be lost");
        assert!(
            matches!(
                &lost,
                crate::DfsPosixError::Dfs(DfsError::NetworkDelivery {
                    delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                    ..
                })
            ),
            "original transport uncertainty must be preserved: {lost}"
        );
        facts["original_reply_lost"] = serde_json::json!(true);
        facts["original_write_error"] = serde_json::json!(lost.to_string());
        let original_calls = runtime.rpc_observations();
        assert!(original_calls[before_calls..].iter().any(|call| {
            call.source == b
                && call.target == b
                && call.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && call.phase == RpcPhase::EndedWithoutResponse
                && call.outcome_error.as_deref().is_some_and(|error| {
                    error.contains("PossiblyApplied") && error.contains("lost response")
                })
        }));
        let commands = served.records();
        let originals = commands
            .iter()
            .filter_map(|work| match &work.command.routed_message.message {
                crate::DfsNetworkMessage::DistributedWriteRequest(command)
                | crate::DfsNetworkMessage::ExclusiveCreateRequest(command)
                    if command.request_id == operation =>
                {
                    Some(command)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            !originals.is_empty(),
            "normal Main captured the actual original head"
        );
        for command in originals {
            assert_eq!(command.request.requesting_node_id, b);
            assert_eq!(command.request.object_id, original_object);
            assert_eq!(command.request.target_version, original_version);
            assert!(command.expected_base_version.is_none());
            assert_eq!(
                crate::intent::routed_write_command_content_hash(command),
                hash
            );
        }
        drop(served);
        runtime.assert_fault_budgets().unwrap();
        assert_eq!(
            runtime.remove_faults_matching(|rule| {
                rule.source.as_ref() == Some(&b)
                    && rule.target.as_ref() == Some(&b)
                    && rule.route.as_deref() == Some(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE)
                    && matches!(rule.action, FaultAction::LoseReplyAfterDispatch)
            }),
            1,
            "heal only the completed reply-loss phase; A content stays denied"
        );
        let before_reconciliation = runtime.rpc_observations().len();
        let reconciled = loop {
            assert!(
                Instant::now() < deadline,
                "same original caller bound includes reconciliation"
            );
            match writer
                .reconcile_stable_create_file_operation(path, operation, &hash, bytes.len() as u64)
                .expect("ordinary owner reconciliation of the original operation")
            {
                crate::DfsStableWriteOperationReconciliation::Applied { receipt } => break receipt,
                crate::DfsStableWriteOperationReconciliation::AmbiguousOrExpired { .. } => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                other => panic!("original operation must retain its admitted outcome: {other:?}"),
            }
        };
        assert!(
            Instant::now() <= deadline,
            "existing five-second caller bound"
        );
        assert_eq!(reconciled.operation_id, operation);
        assert_eq!(reconciled.intent_id, original_intent);
        assert_eq!(reconciled.object_id, original_object);
        assert_eq!(reconciled.target_version, original_version);
        assert_eq!(reconciled.content_hash.as_deref(), Some(hash.as_str()));
        assert_eq!(reconciled.content_size_bytes, Some(bytes.len() as u64));
        assert_eq!(reconciled.status, crate::DfsWriteIntentStatus::Committed);
        assert!(
            !runtime.rpc_observations()[before_reconciliation..]
                .iter()
                .any(|call| { call.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE }),
            "read-only reconciliation never reissues the write under any identity"
        );
        facts["original_operation_reconciled"] = serde_json::json!(reconciled);
        let stat = writer.posix_stat_distributed(path).unwrap();
        assert_eq!(stat.object_id, original_object);
        assert_eq!(stat.version, original_version);
        assert_eq!(stat.content_hash.as_deref(), Some(hash.as_str()));
        facts["stat"] = serde_json::json!({"object_id": &stat.object_id, "version": &stat.version, "hash": &stat.content_hash});
        facts["before_replay"] = fresh_admission_read_only_facts(
            &runtime,
            &writer,
            [("B", &b), ("C", &c), ("A", &a)],
            operation,
            (&original_object, &original_version, &original_intent, &hash),
            before_calls,
        );
        let prepared_id = format!("write:{original_intent}");
        let original_prepared = runtime
            .node_metadata(&b)
            .unwrap()
            .prepared_operation_record(&prepared_id)
            .unwrap()
            .expect("existing Main read owner supplies the original committed prepared body");
        original_prepared.validate().unwrap();
        assert_eq!(
            original_prepared.status,
            crate::DfsPreparedOperationStatus::Committed
        );
        assert!(original_prepared.committed_entry.is_some());
        let original_record = runtime
            .node_metadata(&b)
            .unwrap()
            .get_intent(&original_object, &original_intent)
            .unwrap()
            .unwrap();
        facts["prepared_before_replay"] = serde_json::json!({"digest": &original_prepared.digest, "committed_entry": &original_prepared.committed_entry});
        // Reconciliation is read-only. The normal exact-operation Main reply
        // owns requester-local completion. This is the same original caller
        // deadline, bytes, options and operation, without a replacement ID.
        let replay_served = runtime.capture_served_work(&b, 32).unwrap();
        let replay_calls_before = runtime.rpc_observations().len();
        assert!(
            Instant::now() < deadline,
            "same caller budget remains before replay"
        );
        let replayed = writer.posix_create_file_with_operation_id(path, &bytes, options, operation);
        writer.set_transport_operation_deadline(previous);
        facts["ordinary_replay_result"] = match &replayed {
            Ok(stat) => {
                serde_json::json!({"object": &stat.object_id, "version": &stat.version, "hash": &stat.content_hash, "size": stat.size_bytes})
            }
            Err(error) => serde_json::json!({"error": error.to_string()}),
        };
        let replayed =
            replayed.expect("same operation receives its ordinary validated Main completion");
        assert!(
            Instant::now() <= deadline,
            "original five-second caller bound includes replay"
        );
        assert_eq!(replayed.object_id, original_object);
        assert_eq!(replayed.version, original_version);
        assert_eq!(replayed.content_hash.as_deref(), Some(hash.as_str()));
        assert_eq!(replayed.size_bytes, Some(bytes.len() as u64));
        let validated_reply = writer.last_events().iter().any(|event| matches!(event,
            crate::NetworkedDfsServiceEvent::RemoteWriteCompleted {
                request_id, object_id, primary_node_id,
                result: crate::DfsDistributedWriteResult::Outcome {
                    outcome: crate::NetworkedDfsWriteOutcome::Committed { intent_id, version, content_hash, .. }
                },
            } if request_id == operation && object_id == &original_object && primary_node_id == &b
                && intent_id == &original_intent && version == &original_version && content_hash == &hash
        ));
        facts["validated_committed_reply_seen"] = serde_json::json!(validated_reply);
        assert!(
            validated_reply,
            "ordinary response handler validated the exact committed result"
        );
        let replay_reports = replay_served.records();
        assert!(replay_reports.iter().any(|work| {
            matches!(&work.command.routed_message.message,
                crate::DfsNetworkMessage::ExclusiveCreateRequest(command)
                if work.succeeded && command.request_id == operation && command.request.object_id == original_object
                    && command.request.target_version == original_version && command.request.requesting_node_id == b
                    && command.exclusive_create && crate::intent::routed_write_command_content_hash(command) == hash)
        }), "actual registered Main receives the same exclusive-create operation");
        drop(replay_served);
        assert!(
            runtime.rpc_observations()[replay_calls_before..]
                .iter()
                .any(|call| {
                    call.source == b
                        && call.target == b
                        && call.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                        && call.phase == RpcPhase::Completed
                        && call.outcome_error.is_none()
                }),
            "real same-node replay RPC completes while A content remains denied"
        );
        let after_prepared = runtime
            .node_metadata(&b)
            .unwrap()
            .prepared_operation_record(&prepared_id)
            .unwrap()
            .unwrap();
        assert_eq!(after_prepared.digest, original_prepared.digest);
        assert_eq!(after_prepared.payload, original_prepared.payload);
        assert_eq!(
            after_prepared.committed_entry,
            original_prepared.committed_entry
        );
        assert_eq!(after_prepared.status, original_prepared.status);
        assert_eq!(
            runtime
                .node_metadata(&b)
                .unwrap()
                .get_intent(&original_object, &original_intent)
                .unwrap(),
            Some(original_record)
        );
        facts["original_sealed_outcome_unchanged_after_replay"] = serde_json::json!(true);
        let expected_receipts = BTreeSet::from([b.clone(), c.clone()]);
        for holder in [&b, &c] {
            let actual = runtime.client(holder).unwrap();
            let (manifest, stored) = actual
                .content()
                .read_object_version(stat.object_id.as_str(), stat.version.as_str())
                .unwrap();
            assert_eq!(Some(&manifest.content_hash), stat.content_hash.as_ref());
            assert_eq!(stored, bytes);
        }
        let absent = runtime.client(&a).unwrap();
        assert!(
            absent
                .content()
                .read_object_version(stat.object_id.as_str(), stat.version.as_str())
                .is_err()
        );
        drop(absent);
        let core = runtime.metadata_primary_service(path).unwrap();
        let backend = core.metadata_backend().unwrap();
        let version = backend
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap();
        let durability = version.content_durability.as_ref().unwrap();
        assert_eq!(
            (
                durability.policy.min_commit_replicas,
                durability.policy.desired_replicas
            ),
            (2, 3)
        );
        assert_eq!(
            durability
                .desired_replica_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([a.clone(), b.clone(), c.clone()])
        );
        let receipts = backend
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap();
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.replica_node_id.clone())
                .collect::<BTreeSet<_>>(),
            expected_receipts
        );
        assert!(
            receipts
                .iter()
                .all(|receipt| receipt.content_hash == stat.content_hash)
        );
        let intents = backend.intents_for_object(&stat.object_id).unwrap();
        let matching = intents
            .iter()
            .filter(|intent| intent.operation_id.as_deref() == Some(operation))
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].status, crate::DfsWriteIntentStatus::Committed);
        assert_eq!(matching[0].desired_replica_node_ids.len(), 3);
        assert!(matching[0].desired_replica_node_ids.contains(&a));
        let intent_id = matching[0].intent_id.clone();
        drop(core);
        // Fresh owning Core, same real native owner/store. This is not an OS
        // process restart or an abrupt-loss durability claim.
        let reopened = runtime.metadata_primary_service(path).unwrap();
        let recorded = reopened
            .metadata_backend()
            .unwrap()
            .get_intent(&stat.object_id, &intent_id)
            .unwrap()
            .unwrap();
        assert_eq!(recorded.policy.desired_replicas, 3);
        assert!(recorded.desired_replica_node_ids.contains(&a));
        assert_eq!(
            reopened
                .metadata_backend()
                .unwrap()
                .receipts_for_version(&stat.object_id, &stat.version)
                .unwrap()
                .len(),
            2
        );
        drop(reopened);
        facts["durable_deficit_reopened"] = serde_json::json!(true);
        let calls = runtime.rpc_observations();
        let window = &calls[before_calls..];
        assert!(
            !window.iter().any(|call| call.target == a
                && matches!(
                    call.route.as_str(),
                    crate::DFS_DATA_STAGE_BLOB_ROUTE | crate::DFS_DATA_STAGE_PATCH_ROUTE
                )),
            "initial background W reservation must choose required C before optional A"
        );
        assert!(
            window.iter().any(|call| call.target == a
                && matches!(
                    call.route.as_str(),
                    crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                        | crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE
                )
                && call.phase == RpcPhase::Completed
                && call.outcome_error.is_none()),
            "A metadata transport stays healthy during its content outage"
        );
        assert_eq!(
            runtime.observation_dropped_count(),
            0,
            "actual RPC observation window remains complete"
        );
        runtime.clear_faults();
        // The denied content RPC earned a retry backoff. Model the end of that
        // delay before starting the caller clock. Fsync must still obtain the
        // actual third receipt and certify the same revision within its bound.
        crate::repair_executor::elapse_repair_executor_backoffs_for_test(&runtime.data_dir(&b).root());
        let heal_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let previous = writer.set_transport_operation_deadline(Some(heal_deadline));
        let healed = writer.posix_sync_file(path, crate::DfsPosixSyncKind::File);
        writer.set_transport_operation_deadline(previous);
        let healed = healed.expect("ordinary all-N sync after content route heals");
        assert!(Instant::now() <= heal_deadline);
        assert_eq!(healed.target.version, stat.version);
        assert_eq!(
            healed
                .content_evidence
                .receipt_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([a.clone(), b.clone(), c.clone()])
        );
        let actual = runtime.client(&a).unwrap();
        assert_eq!(
            actual
                .content()
                .read_object_version(stat.object_id.as_str(), stat.version.as_str())
                .unwrap()
                .1,
            bytes
        );
        let current = runtime.metadata_primary_service(path).unwrap();
        let same = current
            .metadata_backend()
            .unwrap()
            .get_intent(&stat.object_id, &intent_id)
            .unwrap()
            .unwrap();
        assert_eq!(same.operation_id.as_deref(), Some(operation));
        assert_eq!(same.target_version, stat.version);
        assert_eq!(same.policy.desired_replicas, 3);
        assert_eq!(
            runtime
                .metadata_shards()
                .unwrap()
                .into_iter()
                .find(|current| current.file_system_id == shard.file_system_id
                    && current.shard_id == shard.shard_id),
            Some(shard.clone())
        );
        facts["healed"] = serde_json::json!(true);
    }));
    if outcome.is_err() {
        facts["failure_before_close"] = fresh_admission_read_only_facts(
            &runtime,
            &writer,
            [("B", &b), ("C", &c), ("A", &a)],
            operation,
            (&original_object, &original_version, &original_intent, &hash),
            0,
        );
    }
    eprintln!(
        "FRESH_CONTENT_ADMISSION_NATIVE_FACTS {}",
        serde_json::json!({"assertions_passed": outcome.is_ok(), "facts": facts,
        "qualification": "Authored native in-process transport/disk original-create reply loss, read-only reconciliation, then the same registered-Main operation completion within its original five-second deadline. A content stays denied through replay. Observed-base/carrier envelopes may refresh; no byte-identical wire, general KnownUnreachable retry, abrupt process stop, quorum, overwrite or production availability claim."})
    );
    runtime.clear_faults();
    drop(writer);
    let closed = runtime.close();
    if let Err(error) = outcome {
        if let Err(close_error) = closed {
            eprintln!("fresh admission native cleanup also failed: {close_error}");
        }
        std::panic::resume_unwind(error);
    }
    closed.expect("ordinary native fixture drain");
}
