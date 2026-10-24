use super::*;
use crate::test_runtime::{FaultAction, FaultRule, RpcOrigin, RpcPhase};
use std::time::Duration;

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
        let bytes = vec![0x6d; 4096];
        let hash = crate::content_hash(&bytes);
        let original_object = crate::intent::stable_create_object_id(&b, path, operation);
        let original_version =
            crate::intent::stable_write_target_version(&b, &original_object, operation);
        let original_intent =
            crate::intent::stable_write_intent_id(&b, &original_object, operation);
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
        let written = writer.posix_create_file_with_operation_id(
            path,
            &bytes,
            crate::DfsWriteOptions {
                policy,
                replica_candidates: vec![b.clone(), c.clone(), a.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
            operation,
        );
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
        writer.set_transport_operation_deadline(previous);
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
    eprintln!(
        "FRESH_CONTENT_ADMISSION_NATIVE_FACTS {}",
        serde_json::json!({"assertions_passed": outcome.is_ok(), "facts": facts,
        "qualification": "Authored native in-process transport/disk original-create reply loss and read-only operation reconciliation with healthy metadata. A content stays denied through reconciliation; no direct write-API retry, abrupt process stop, quorum, general overwrite or production availability claim."})
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
