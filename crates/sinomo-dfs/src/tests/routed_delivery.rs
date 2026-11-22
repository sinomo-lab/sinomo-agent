//! Delivery failures on the shared embedded runtime and real receiver.
use super::*;
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcPhase};
use sinomo_core::network::NetworkDeliveryState;

const PAYLOAD: &[u8] = b"one immutable routed write payload";

#[derive(Clone, Copy)]
enum Body {
    Bytes,
    Manifest,
    Reader,
}

fn route(
    client: &mut NetworkedDfsService<DfsNodeTransport>,
    object: &DfsObjectMetadata,
    shard: &DfsMetadataShardConfig,
    version: &DfsVersion,
    operation: &str,
    body: Body,
) -> Result<NetworkedDfsWriteOutcome, DfsError> {
    let request = DfsWriteRequest {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        target_version: version.clone(),
        requesting_node_id: client.node_id.clone(),
    };
    let replicas = vec![client.node_id.clone(), shard.primary_node_id.clone()];
    let policy = DfsReplicationPolicy::new(1, 2).unwrap();
    let options = DfsDistributedWriteOptions {
        operation_id: Some(operation.into()),
        repair: crate::service_write::DfsWriteRepair::Background,
        ..Default::default()
    };
    match body {
        Body::Bytes => client.route_distributed_bytes(
            request,
            Some(object.version.clone()),
            PAYLOAD.to_vec(),
            replicas,
            policy,
            object.clone(),
            shard.clone(),
            options,
        ),
        Body::Manifest => {
            client.content.put_object_version(
                object.object_id.as_str(),
                version.as_str(),
                PAYLOAD,
            )?;
            client.route_distributed_manifest(
                request,
                Some(object.version.clone()),
                content_hash(PAYLOAD),
                PAYLOAD.len() as u64,
                replicas,
                policy,
                object.clone(),
                shard.clone(),
                options,
            )
        }
        Body::Reader => client.route_distributed_reader(
            request,
            Some(object.version.clone()),
            std::io::Cursor::new(PAYLOAD),
            replicas,
            policy,
            object.clone(),
            shard.clone(),
            options,
        ),
    }
}

fn native_delivery_case(body: Body, uncertain: bool, close_owner: bool) {
    let nodes = DfsTestRuntime::ordered_node_ids("native-routed-delivery", 3);
    let mut runtime = DfsTestRuntime::new("native-routed-delivery", nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    let case_id = match (body, uncertain, close_owner) {
        (Body::Bytes, true, false) => "native-attachment-lost-reply",
        (Body::Manifest, true, false) => "native-manifest-lost-reply",
        (Body::Reader, true, false) => "native-reader-lost-reply",
        (_, false, _) => "native-unsent-no-effect",
        (_, true, true) => "native-closed-response-owner",
    };
    let trace = crate::test_support::NativeShardTraceArtifactSession::from_environment(
        case_id,
        nodes
            .iter()
            .map(|node| (node.clone(), runtime.data_dir(node).root().to_owned())),
        DfsMetadataShardId::new("root"),
    )
    .expect("actual normal loss capture setup before mount");
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    let primary = shard.primary_node_id.clone();
    let requester = nodes.iter().find(|node| **node != primary).unwrap().clone();
    let mut authority = runtime.client(&primary).unwrap();
    let original = authority
        .posix_write_file(
            "/routed-delivery",
            b"original",
            DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
        )
        .unwrap();
    let object = runtime
        .node_metadata(&primary)
        .unwrap()
        .get_object(&original.object_id)
        .unwrap()
        .unwrap();
    let mut client = runtime.client(&requester).unwrap();
    let client_calls = client.transport.observe_routed_calls_for_test();
    let target = DfsVersion::generate();
    let operation = sinomo_core::Id::generate().to_string();
    let baseline = runtime.rpc_observations().len();
    runtime.install_fault(FaultRule {
        source: Some(requester.clone()),
        target: Some(primary.clone()),
        route: Some(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 8,
        action: if uncertain {
            FaultAction::LoseReplyAfterDispatch
        } else {
            FaultAction::BudgetExhausted
        },
    });
    let error = route(&mut client, &object, &shard, &target, &operation, body).unwrap_err();
    assert!(
        client_calls.load(Ordering::SeqCst) > 0,
        "the actual first routed write calibrates its own transport observation"
    );
    assert!(
        matches!(error, DfsError::NetworkDelivery { delivery_state, .. } if delivery_state == if uncertain { NetworkDeliveryState::PossiblyApplied } else { NetworkDeliveryState::DefinitelyNotSent })
    );
    assert!(
        runtime.rpc_observations()[baseline..]
            .iter()
            .any(|event| event.source == requester
                && event.target == primary
                && event.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && event.phase == RpcPhase::EndedWithoutResponse
                && event
                    .outcome_error
                    .as_deref()
                    .is_some_and(|detail| detail.contains(if uncertain {
                        "PossiblyApplied"
                    } else {
                        "DefinitelyNotSent"
                    }) && detail.contains(if uncertain {
                        "lost response"
                    } else {
                        "before-dispatch fault"
                    }))),
        "bounded native diagnostics retain the actual returned delivery error"
    );
    if uncertain {
        let pending = client.writes.pending_routed_writes[&operation].clone();
        assert!(pending.command.is_none() && pending.attachment_bytes.is_none());
        assert_eq!(
            pending.durable_receipts, 0,
            "a lost reply supplies no receipt"
        );
        runtime.clear_faults();
        if close_owner {
            if let Some(trace) = &trace {
                trace.close().unwrap();
            }
            runtime.stop_node(&requester).unwrap();
            let previous = client.writes.pending_routed_writes.clone();
            let before = runtime.rpc_observations().len();
            let calls_before = client_calls.load(Ordering::SeqCst);
            assert!(route(&mut client, &object, &shard, &target, &operation, body).is_err());
            assert_eq!(client.writes.pending_routed_writes, previous);
            let after = runtime.rpc_observations();
            assert_eq!(
                client_calls.load(Ordering::SeqCst),
                calls_before,
                "closed response custody rejects before dispatch; new observations={:?}",
                &after[before..]
            );
        } else {
            runtime.install_fault(FaultRule {
                source: Some(requester.clone()),
                target: Some(primary.clone()),
                route: Some(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into()),
                call_id: None,
                origin: None,
                remaining_hits: 8,
                action: FaultAction::BudgetExhausted,
            });
            let previous = client.writes.pending_routed_writes.clone();
            assert!(matches!(
                route(&mut client, &object, &shard, &target, &operation, body).unwrap_err(),
                DfsError::NetworkDelivery {
                    delivery_state: NetworkDeliveryState::PossiblyApplied,
                    ..
                }
            ));
            assert_eq!(client.writes.pending_routed_writes, previous);
            runtime.clear_faults();
            route(&mut client, &object, &shard, &target, &operation, body).unwrap();
            let deadline =
                std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            loop {
                client
                    .process_incoming_without_deferred_parent_touches()
                    .unwrap();
                if matches!(client.write_outcome(&operation).unwrap(), NetworkedDfsWriteOutcome::Committed { ref version, .. } if version == &target)
                {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "exact retry must resolve the actual receiver's outcome"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            let alias =
                crate::intent::stable_write_intent_id(&requester, &object.object_id, &operation);
            assert_eq!(
                client.write_outcome(&alias).unwrap(),
                client.write_outcome(&operation).unwrap()
            );
            assert_eq!(
                client
                    .posix_read_file_distributed("/routed-delivery")
                    .unwrap(),
                PAYLOAD
            );
        }
    } else {
        assert!(client.writes.pending_routed_writes.is_empty());
        assert_eq!(
            authority
                .posix_read_file_distributed("/routed-delivery")
                .unwrap(),
            b"original"
        );
        assert!(
            !runtime
                .rpc_observations()
                .iter()
                .skip(baseline)
                .any(
                    |event| event.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                        && event.phase == RpcPhase::AfterDispatch
                )
        );
    }
    runtime.clear_faults();
    runtime.assert_fault_budgets().unwrap();
    assert_eq!(runtime.observation_dropped_count(), 0);
    drop((client, authority));
    if let Some(trace) = &trace {
        trace.close().unwrap();
    }
    runtime.close().unwrap();
    if let Some(trace) = trace {
        trace.export().unwrap();
    }
}

#[test]
fn native_attachment_lost_reply_and_exact_retry() {
    native_delivery_case(Body::Bytes, true, false);
}
#[test]
fn native_manifest_lost_reply_and_exact_retry() {
    native_delivery_case(Body::Manifest, true, false);
}
#[test]
fn native_reader_lost_reply_and_exact_retry() {
    native_delivery_case(Body::Reader, true, false);
}
#[test]
fn native_unsent_write_has_no_receiver_effect() {
    native_delivery_case(Body::Bytes, false, false);
}
#[test]
fn native_closed_response_owner_preserves_uncertain_write() {
    native_delivery_case(Body::Bytes, true, true);
}

#[test]
fn native_stale_authority_redirect_keeps_original_identity_after_lost_reply() {
    let nodes = DfsTestRuntime::ordered_node_ids("native-routed-handoff", 3);
    let mut runtime = DfsTestRuntime::new("native-routed-handoff", nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let retired = runtime.root_shard().unwrap();
    let old_primary = retired.primary_node_id.clone();
    let new_primary = nodes
        .iter()
        .find(|node| **node != old_primary)
        .unwrap()
        .clone();
    let requester = nodes
        .iter()
        .find(|node| **node != old_primary && **node != new_primary)
        .unwrap()
        .clone();
    let mut old = runtime.client(&old_primary).unwrap();
    let original = old
        .posix_write_file(
            "/handoff-delivery",
            b"original",
            DfsWriteOptions::replicated_eventual(vec![old_primary.clone()]),
        )
        .unwrap();
    let object = runtime
        .node_metadata(&old_primary)
        .unwrap()
        .get_object(&original.object_id)
        .unwrap()
        .unwrap();
    let mut client = runtime.client(&requester).unwrap();
    let active = runtime
        .reassign_metadata_shard(
            &retired.file_system_id,
            &retired.shard_id,
            new_primary.clone(),
            nodes
                .iter()
                .filter(|node| **node != new_primary)
                .cloned()
                .collect(),
        )
        .unwrap();
    runtime.install_fault(FaultRule {
        source: Some(requester.clone()),
        target: Some(new_primary.clone()),
        route: Some(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 8,
        action: FaultAction::LoseReplyAfterDispatch,
    });
    let operation = sinomo_core::Id::generate().to_string();
    let target = DfsVersion::generate();
    route(
        &mut client,
        &object,
        &retired,
        &target,
        &operation,
        Body::Bytes,
    )
    .unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        match client.process_incoming_without_deferred_parent_touches() {
            Err(DfsError::NetworkDelivery {
                delivery_state: NetworkDeliveryState::PossiblyApplied,
                ..
            }) => break,
            Err(error) => panic!("native redirect failed unexpectedly: {error}"),
            Ok(_) => assert!(
                Instant::now() < deadline,
                "native retired primary must redirect"
            ),
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let pending = &client.writes.pending_routed_writes[&operation];
    assert_eq!(pending.primary_node_id, new_primary);
    assert_eq!(pending.authority_shard, active);
    assert!(pending.command.is_none() && pending.attachment_bytes.is_none());
    assert_eq!(pending.durable_receipts, 0);
    runtime.clear_faults();
    route(
        &mut client,
        &object,
        &active,
        &target,
        &operation,
        Body::Bytes,
    )
    .unwrap();
    loop {
        client
            .process_incoming_without_deferred_parent_touches()
            .unwrap();
        if matches!(client.write_outcome(&operation).unwrap(), NetworkedDfsWriteOutcome::Committed { ref version, .. } if version == &target)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the original exact retry must settle after handoff"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        client
            .posix_read_file_distributed("/handoff-delivery")
            .unwrap(),
        PAYLOAD
    );
    runtime.assert_fault_budgets().unwrap();
    assert_eq!(runtime.observation_dropped_count(), 0);
    drop((old, client));
    runtime.close().unwrap();
}

/// A commit the dispatch primary reports after this requester's projection
/// moved past the record the write was sent under is still that write's
/// result: the caller is answered with the commit, not with an error about
/// the record.
#[test]
fn native_late_commit_after_a_record_change_completes_with_its_result() {
    let nodes = DfsTestRuntime::ordered_node_ids("native-routed-late-commit", 3);
    let mut runtime = DfsTestRuntime::new("native-routed-late-commit", nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    let primary = shard.primary_node_id.clone();
    let requester = nodes.iter().find(|node| **node != primary).unwrap().clone();
    let successor = nodes
        .iter()
        .find(|node| **node != primary && **node != requester)
        .unwrap()
        .clone();
    let path = "/late-commit";
    let mut authority = runtime.client(&primary).unwrap();
    let original = authority
        .posix_write_file(
            path,
            b"original",
            DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
        )
        .unwrap();
    let object = runtime
        .node_metadata(&primary)
        .unwrap()
        .get_object(&original.object_id)
        .unwrap()
        .unwrap();
    let mut client = runtime.client(&requester).unwrap();
    let operation = sinomo_core::Id::generate().to_string();
    let target = DfsVersion::generate();
    // The primary's answers to this write are captured and never delivered.
    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    runtime.install_fault(FaultRule {
        source: Some(requester.clone()),
        target: Some(primary.clone()),
        route: Some(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: FaultAction::CaptureWriteReplyAndLoseAfterDispatch(Arc::clone(&captured)),
    });
    let committed = |captured: &std::sync::Mutex<Vec<crate::DfsRoutedMessage>>| {
        captured
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find_map(|message| match &message.message {
                DfsNetworkMessage::DistributedWriteResponse(response)
                    if matches!(
                        &response.result,
                        crate::DfsDistributedWriteResult::Outcome {
                            outcome: NetworkedDfsWriteOutcome::Committed { .. },
                        }
                    ) =>
                {
                    Some(response.clone())
                }
                _ => None,
            })
    };
    // The same write is sent, under its one id, until the primary answers it
    // with its commit; every answer is lost on the way back.
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT * 4;
    let late = loop {
        let lost = route(
            &mut client,
            &object,
            &shard,
            &target,
            &operation,
            Body::Bytes,
        );
        assert!(
            matches!(
                lost,
                Err(DfsError::NetworkDelivery {
                    delivery_state: NetworkDeliveryState::PossiblyApplied,
                    ..
                })
            ),
            "the answer is lost: {lost:?}"
        );
        if let Some(late) = committed(&captured) {
            break late;
        }
        assert!(Instant::now() < deadline, "the primary commits the write");
        std::thread::sleep(Duration::from_millis(20));
    };
    runtime.clear_faults();
    assert!(
        client.writes.pending_routed_writes[&operation]
            .result
            .is_none(),
        "no answer reached the requester"
    );
    // The record moves on, and the requester's projection shows it.
    let active = runtime
        .reassign_metadata_shard(
            &shard.file_system_id,
            &shard.shard_id,
            successor.clone(),
            nodes
                .iter()
                .filter(|node| **node != successor)
                .cloned()
                .collect(),
        )
        .unwrap();
    assert_ne!(active, shard);
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT * 4;
    while client
        .projected_shard_record(&shard.file_system_id, &shard.shard_id)
        .unwrap()
        .as_ref()
        != Some(&active)
    {
        assert!(
            Instant::now() < deadline,
            "the requester projects the newer record"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    // The old primary's commit arrives late.
    client
        .handle_distributed_write_response(primary.clone(), late)
        .unwrap();
    let outcome = client.write_outcome(&operation);
    assert!(
        matches!(&outcome, Ok(NetworkedDfsWriteOutcome::Committed { version, .. }) if version == &target),
        "the late commit is the write's result: {outcome:?}"
    );
    drop((authority, client));
    runtime.close().unwrap();
}

/// A refusal for the write's authority is followed only to a strictly newer
/// record of the requester's own projection. While that projection shows
/// none, the one call waits: it is not answered with the refusal, it does
/// not go to another node, and only the re-send timer sends it again. Its
/// budget ends the wait with the refusal.
#[test]
fn native_redirect_without_a_newer_record_waits_and_does_not_loop() {
    let nodes = DfsTestRuntime::ordered_node_ids("native-routed-redirect-wait", 3);
    let mut runtime = DfsTestRuntime::new("native-routed-redirect-wait", nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    let primary = shard.primary_node_id.clone();
    let requester = nodes.iter().find(|node| **node != primary).unwrap().clone();
    let mut authority = runtime.client(&primary).unwrap();
    let original = authority
        .posix_write_file(
            "/redirect-wait",
            b"original",
            DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
        )
        .unwrap();
    let object = runtime
        .node_metadata(&primary)
        .unwrap()
        .get_object(&original.object_id)
        .unwrap()
        .unwrap();
    let mut client = runtime.client(&requester).unwrap();
    // A stamp ahead of every record: the primary refuses it for its
    // authority, and no projection will show a record newer than it.
    let mut ahead = shard.clone();
    ahead.epoch += 1;
    let operation = sinomo_core::Id::generate().to_string();
    let target = DfsVersion::generate();
    runtime.checkpoint_rpc_observation_window().unwrap();
    let budget = Duration::from_secs(3);
    let started = Instant::now();
    client.set_transport_operation_deadline(Some(started + budget));
    client.writes.follow_write(&object.object_id, &target, true);
    let pending = route(
        &mut client,
        &object,
        &ahead,
        &target,
        &operation,
        Body::Bytes,
    )
    .unwrap();
    let outcome = client.settle_ready_write_outcome(pending, false);
    client.set_transport_operation_deadline(None);
    assert!(
        started.elapsed() >= budget,
        "the call waited for a newer record for its whole budget: {:?} {outcome:?}",
        started.elapsed()
    );
    assert!(
        matches!(&outcome, Err(DfsError::Distributed { message }) if !message.contains("exceeded")),
        "the budget ends the wait with the refusal: {outcome:?}"
    );
    let sends = runtime
        .rpc_observations()
        .into_iter()
        .filter(|call| {
            call.source == requester
                && call.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && call.phase == RpcPhase::BeforeDispatch
        })
        .map(|call| call.target)
        .collect::<Vec<_>>();
    assert!(
        sends.iter().all(|target| target == &primary),
        "the write never went to a node the record does not name: {sends:?}"
    );
    assert!(
        (2..=6).contains(&sends.len()),
        "only the re-send timer sent the write again: {} sends",
        sends.len()
    );
    assert_eq!(
        authority
            .posix_read_file_distributed("/redirect-wait")
            .unwrap(),
        b"original",
        "a refused write applied nothing"
    );
    drop((authority, client));
    runtime.close().unwrap();
}

/// Recruitment and retirement can change the exact shard stamp without
/// changing its epoch or replacing the primary.
#[test]
fn native_resend_refreshes_the_replica_list_at_the_same_primary_and_epoch() {
    let (runtime, nodes) = DfsTestRuntime::start_cluster("same-epoch-routed-write", 3).unwrap();
    let shard = runtime.root_shard().unwrap();
    let primary = &shard.primary_node_id;
    let requester = nodes.iter().find(|node| *node != primary).unwrap();
    let mut authority = runtime.client(primary).unwrap();
    let path = "/same-epoch-routed-write";
    let initial = authority
        .posix_write_file(
            path,
            b"original",
            DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
        )
        .unwrap();
    let object = runtime
        .node_metadata(primary)
        .unwrap()
        .get_object(&initial.object_id)
        .unwrap()
        .unwrap();
    let mut stale = shard.clone();
    stale
        .replica_node_ids
        .pop()
        .expect("the fixture has a replica");
    let mut client = runtime.client(requester).unwrap();
    client.set_transport_operation_deadline(Some(Instant::now() + Duration::from_secs(5)));
    let operation = sinomo_core::Id::generate().to_string();
    let target = DfsVersion::generate();
    let pending = route(
        &mut client,
        &object,
        &stale,
        &target,
        &operation,
        Body::Bytes,
    )
    .unwrap();
    assert!(
        matches!(client.settle_ready_write_outcome(pending, false).unwrap(),
        NetworkedDfsWriteOutcome::Committed { version, .. } if version == target)
    );
    assert_eq!(
        authority.posix_read_file_distributed(path).unwrap(),
        PAYLOAD
    );
    drop((authority, client));
    runtime.close().unwrap();
}

#[test]
fn unsent_namespace_retry_preserves_an_earlier_possible_commit() {
    check_failed_call_after_mkdir("namespace-retry-outcome", false);
}

#[test]
fn unsent_stat_after_acknowledged_mkdir_reports_the_mutation_may_have_applied() {
    check_failed_call_after_mkdir("namespace-postcommit-stat", true);
}

fn check_failed_call_after_mkdir(name: &str, acknowledge_mkdir: bool) {
    let (runtime, nodes) = DfsTestRuntime::start_cluster(name, 3).unwrap();
    let primary = runtime.root_shard().unwrap().primary_node_id;
    let requester = nodes.iter().find(|node| **node != primary).unwrap();
    let mut client = runtime.client(requester).unwrap();
    let mut authority = runtime.client(&primary).unwrap();
    let rule = |action| FaultRule {
        source: Some(requester.clone()),
        target: Some(primary.clone()),
        route: Some(DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action,
    };
    let reply = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(rule(if acknowledge_mkdir {
        FaultAction::PauseAfterDispatch(Arc::clone(&reply))
    } else {
        FaultAction::LoseReplyAfterDispatch
    }));
    let path = "/namespace-retry-outcome";
    let deadline = Instant::now() + Duration::from_secs(3);
    client.set_transport_operation_deadline(Some(deadline));
    let outcome = std::thread::scope(|scope| {
        let call = scope.spawn(|| client.posix_mkdir_with_operation_id(path, "mkdir-outcome"));
        while !runtime.rpc_observations().iter().any(|rpc| {
            rpc.source == *requester
                && rpc.target == primary
                && rpc.route == DFS_DATA_NAMESPACE_ROUTE
                && rpc.phase == RpcPhase::AfterDispatch
        }) {
            assert!(
                Instant::now() < deadline,
                "the primary executed the first request"
            );
            std::thread::yield_now();
        }
        runtime.clear_faults();
        runtime.install_fault(rule(FaultAction::BudgetExhausted));
        reply.add_permits(1);
        call.join().unwrap()
    });
    assert!(
        matches!(
            outcome,
            Err(DfsPosixError::Dfs(DfsError::NetworkDelivery {
                delivery_state: NetworkDeliveryState::PossiblyApplied,
                ..
            }))
        ),
        "{outcome:?}"
    );
    runtime.clear_faults();
    assert_eq!(
        authority.posix_stat_distributed(path).unwrap().kind,
        DfsObjectKind::Directory
    );
    drop((client, authority));
    runtime.close().unwrap();
}
