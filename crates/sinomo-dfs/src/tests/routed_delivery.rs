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
    let object = authority
        .metadata_backend()
        .unwrap()
        .get_object(&original.object_id)
        .unwrap()
        .unwrap();
    let mut client = runtime.client(&requester).unwrap();
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
        matches!(error, DfsError::NetworkDelivery { delivery_state, .. } if delivery_state == if uncertain { NetworkDeliveryState::PossiblyApplied } else { NetworkDeliveryState::DefinitelyNotSent })
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
            assert!(route(&mut client, &object, &shard, &target, &operation, body).is_err());
            assert_eq!(client.writes.pending_routed_writes, previous);
            let after = runtime.rpc_observations();
            assert_eq!(
                after.len(),
                before,
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
    let object = old
        .metadata_backend()
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
