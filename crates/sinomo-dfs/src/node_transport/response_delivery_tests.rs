//! AFTER-only checks that transport forks cannot consume another owner's reply.

use super::*;
use crate::{DfsDistributedWriteResponse, DfsDistributedWriteResult, NetworkedDfsWriteOutcome};

struct DeliveryPeer(Arc<crate::DfsRoutedResponseDelivery>);

impl DfsPeerTransport for DeliveryPeer {
    fn response_delivery_owner(&self) -> Option<Arc<crate::DfsRoutedResponseDelivery>> {
        Some(Arc::clone(&self.0))
    }

    fn call_object_rpc(
        &self,
        _target: &NodeId,
        _call: ObjectRpcCall,
        _options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        panic!("custody tests must not send a new operation")
    }
}

impl AsyncDfsPeerTransport for DeliveryPeer {
    fn call_object_rpc_async<'a>(
        &'a self,
        _target: &'a NodeId,
        _call: ObjectRpcCall,
        _options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
        Box::pin(async { panic!("custody tests must not send a new operation") })
    }
}

fn fixture() -> (Arc<crate::DfsRoutedResponseDelivery>, DfsNodeTransport) {
    let delivery = Arc::new(crate::DfsRoutedResponseDelivery::new());
    let peer = Arc::new(DeliveryPeer(Arc::clone(&delivery)));
    let transport = DfsNodeTransport::with_peer_transports_for_nodes(
        NodeId::from_legacy_named("transport-response-local").unwrap(),
        [NodeId::from_legacy_named("transport-response-primary").unwrap()],
        Duration::from_secs(1),
        peer.clone(),
        peer,
    );
    (delivery, transport)
}

fn response(request: &str) -> DfsRoutedMessage {
    DfsRoutedMessage {
        source_node_id: NodeId::from_legacy_named("transport-response-primary").unwrap(),
        target_node_id: NodeId::from_legacy_named("transport-response-local").unwrap(),
        frame_id: 1,
        message_id: 1,
        message: DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
            request_id: request.into(),
            object_id: object_id(),
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Pending {
                    intent_id: "transport-response-intent".into(),
                    durable_receipts: 0,
                    required_receipts: 1,
                },
            },
            metadata_journal_batches: Vec::new(),
        }),
    }
}

fn object_id() -> crate::DfsObjectId {
    crate::DfsObjectId::from_legacy_named("transport-response-object").unwrap()
}

#[test]
fn response_transport_sync_async_count_drain_and_wait_share_the_owned_inbox() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let (delivery, mut transport) = fixture();
    transport
        .register_routed_write_response_owner("request", &object_id(), &["alias".into()])
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(delivery.try_deliver(&response("alias")).unwrap());
    assert_eq!(transport.pending_incoming_count().unwrap(), Some(1));
    assert!(transport.wait_for_incoming(Duration::ZERO).unwrap());
    runtime.block_on(async {
        assert_eq!(
            transport.pending_incoming_count_async().await.unwrap(),
            Some(1)
        );
        assert_eq!(
            transport.drain_incoming_messages_async().await.unwrap(),
            vec![response("alias")]
        );
    });
    assert_eq!(transport.pending_incoming_count().unwrap(), Some(0));
    runtime.block_on(async {
        let mut waiting = Box::pin(transport.wait_for_incoming_async(Duration::from_secs(30)));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
        assert!(delivery.try_deliver(&response("request")).unwrap());
        assert!(waiting.await.unwrap());
    });
    assert_eq!(
        transport.drain_incoming_messages().unwrap(),
        vec![response("request")]
    );
    assert_eq!(
        runtime
            .block_on(transport.pending_incoming_count_async())
            .unwrap(),
        Some(0)
    );
}

#[test]
fn response_transport_clone_and_every_worker_fork_leave_original_custody_intact() {
    let (delivery, mut original) = fixture();
    original
        .register_routed_write_response_owner("request", &object_id(), &["alias".into()])
        .unwrap();
    assert!(delivery.try_deliver(&response("alias")).unwrap());
    let forks = [
        original.clone(),
        original.fork_read_only_request(),
        original.fork_replica_call_send(),
        original.fork_detached_send_worker().transport,
    ];
    for mut fork in forks {
        assert_eq!(fork.pending_incoming_count().unwrap(), Some(0));
        assert!(fork.drain_incoming_messages().unwrap().is_empty());
        assert!(
            fork.register_routed_write_response_owner("request", &object_id(), &["alias".into()])
                .is_err()
        );
        assert_eq!(original.pending_incoming_count().unwrap(), Some(1));
        // Drop cannot revoke registrations owned by the original transport.
    }
    assert!(delivery.try_deliver(&response("request")).unwrap());
    assert_eq!(
        original.drain_incoming_messages().unwrap(),
        vec![response("alias"), response("request")]
    );
}

#[test]
fn response_transport_registration_rollback_removes_only_new_aliases() {
    let (delivery, mut transport) = fixture();
    transport
        .register_routed_write_response_owner("request", &object_id(), &["old-alias".into()])
        .unwrap();
    let checkpoint = transport.routed_write_response_registration_checkpoint("request");
    transport
        .register_routed_write_response_owner("request", &object_id(), &["new-alias".into()])
        .unwrap();
    transport.rollback_routed_write_response_registration("request", checkpoint);
    assert!(!delivery.try_deliver(&response("new-alias")).unwrap());
    assert!(delivery.try_deliver(&response("old-alias")).unwrap());
    assert!(delivery.try_deliver(&response("request")).unwrap());
    assert_eq!(transport.drain_incoming_messages().unwrap().len(), 2);
    transport.release_routed_write_response_owner("request");
    assert!(!delivery.try_deliver(&response("old-alias")).unwrap());
    assert!(!delivery.try_deliver(&response("request")).unwrap());
}

fn served_fixture(delivery: Arc<crate::DfsRoutedResponseDelivery>) -> DfsNodeTransport {
    let peer: Arc<dyn DfsPeerTransport> = Arc::new(DeliveryPeer(delivery));
    DfsNodeTransport::new_with_message_sender_for_nodes(
        NodeId::from_legacy_named("transport-response-local").unwrap(),
        [NodeId::from_legacy_named("transport-response-primary").unwrap()],
        dfs_message_sender_from_peer_transport(Arc::clone(&peer)),
        Some(peer),
        Duration::from_secs(1),
    )
}

fn served_send(
    transport: &mut DfsNodeTransport,
    message: DfsRoutedMessage,
    asynchronous: bool,
) -> Result<(), NetworkError> {
    let target = message.target_node_id.clone();
    if asynchronous {
        crate::test_runtime::executor().block_on(AsyncDfsMessageTransport::send_routed_call_async(
            transport,
            &target,
            DfsRoutedCall::value(message),
        ))
    } else {
        DfsMessageTransport::send_routed_call(transport, &target, DfsRoutedCall::value(message))
    }
}

// These negative custody controls use Pending DTOs only; they establish no
// committed effect. The sibling native test obtains its terminal from Main.
#[test]
fn served_same_node_response_preserves_inline_caller_and_unknown_owner_fallbacks() {
    for asynchronous in [false, true] {
        let (delivery, mut original) = fixture();
        original
            .register_routed_write_response_owner("request", &object_id(), &["alias".into()])
            .unwrap();
        let mut served = served_fixture(Arc::clone(&delivery));
        let message = response("alias");

        // A current same-node served caller retains the ordinary inline reply.
        let mut incoming = message.clone();
        incoming.source_node_id = incoming.target_node_id.clone();
        served.begin_message(&incoming).unwrap();
        served_send(&mut served, message.clone(), asynchronous).unwrap();
        assert_eq!(served.finish_message(), vec![message.clone()]);
        assert_eq!(original.pending_incoming_count().unwrap(), Some(0));

        // With no current caller (the Routine shape), its original canonical
        // registration receives the response instead of the Main inbound queue.
        assert!(served.served().return_node_id.is_none());
        served_send(&mut served, message.clone(), asynchronous).unwrap();
        assert!(served.served().inbound_messages.is_empty());
        assert!(original.wait_for_incoming(Duration::ZERO).unwrap());
        assert_eq!(original.drain_incoming_messages().unwrap(), vec![message]);
        assert!(!original.wait_for_incoming(Duration::ZERO).unwrap());

        let mut wrong_object = response("request");
        let DfsNetworkMessage::DistributedWriteResponse(value) = &mut wrong_object.message else {
            unreachable!()
        };
        value.object_id = crate::DfsObjectId::from_legacy_named("other-object").unwrap();
        let unknown_request = response("unregistered-request");
        for rejected in [wrong_object, unknown_request] {
            served_send(&mut served, rejected.clone(), asynchronous).unwrap();
            assert_eq!(served.drain_queued_messages(), vec![rejected]);
            assert_eq!(original.pending_incoming_count().unwrap(), Some(0));
        }

        // Another physical root has a different existing delivery owner even
        // when node/object/request identifiers happen to coincide.
        let mut other_root = served_fixture(Arc::new(crate::DfsRoutedResponseDelivery::new()));
        let wrong_root = response("request");
        served_send(&mut other_root, wrong_root.clone(), asynchronous).unwrap();
        assert_eq!(other_root.drain_queued_messages(), vec![wrong_root]);
        assert_eq!(original.pending_incoming_count().unwrap(), Some(0));
    }
}

#[test]
fn served_same_node_full_inbox_propagates_failure_without_loopback_or_lease_loss() {
    for asynchronous in [false, true] {
        let (delivery, mut original) = fixture();
        original
            .register_routed_write_response_owner("request", &object_id(), &[])
            .unwrap();
        let mut served = served_fixture(delivery);
        let mut accepted = 0;
        let failure = loop {
            assert!(
                accepted < 512,
                "the existing inbox bound must reject admission"
            );
            match served_send(&mut served, response("request"), asynchronous) {
                Ok(()) => accepted += 1,
                Err(error) => break error,
            }
        };
        assert!(matches!(failure, NetworkError::Io { operation, detail }
            if operation == "DFS routed response custody" && detail.contains("inbox is full")));
        assert!(served.served().inbound_messages.is_empty());
        assert!(served.served().outbound_messages.is_empty());
        assert_eq!(original.drain_incoming_messages().unwrap().len(), accepted);
        // Error propagation leaves the original registration usable, so the
        // sending service may retain and later transfer the very same response.
        served_send(&mut served, response("request"), asynchronous).unwrap();
        assert_eq!(
            original.drain_incoming_messages().unwrap(),
            vec![response("request")]
        );
    }
}

#[test]
fn served_same_node_routine_completion_reaches_original_native_browser_caller_once() {
    use crate::service_write::{DfsDistributedWriteOptions, DfsWriteRepair};
    use crate::test_runtime::{FaultAction, FaultRule, RpcOrigin, RpcPhase};
    use crate::{
        DfsReplicationPolicy, DfsWriteIntentStatus, DfsWriteOptions, DfsWritePublication,
        NetworkedDfsServiceEvent, DFS_DATA_DISTRIBUTED_WRITE_ROUTE,
        DFS_DATA_REPAIR_REPLICA_ROUTE, DFS_DATA_STAGE_BLOB_ROUTE,
    };

    use sinomo_core::DfsWriteRequest;

    let name = "served-same-node-routine-custody";
    let (mut runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    drop(initial);
    let primary = config.primary_node_id;
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let browser = crate::DfsBrowserBackend::for_component_data_dir(
        Arc::new(runtime.data_dir(&primary).clone()),
        &runtime.runtime_handle(),
    )
    .unwrap();
    let path = format!("/services/{name}.bin");
    let base = browser
        .with_mutation_service(|service| {
            Ok(service
                .posix_write_file(
                    &path,
                    b"actual original native head",
                    DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
                )
                .unwrap())
        })
        .unwrap();

    // Negatively withhold actual content transfer; neither a receipt nor a
    // terminal result is supplied by the fixture. Main must first return W2
    // Pending to its separate same-node browser caller.
    for target in nodes.iter().filter(|node| **node != primary) {
        for route in [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE] {
            runtime.install_fault(FaultRule {
                source: Some(primary.clone()),
                target: Some(target.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: Some(RpcOrigin::RegisteredHost),
                remaining_hits: 32,
                action: FaultAction::BudgetExhausted,
            });
        }
    }
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id.clone(),
        object_id: base.object_id.clone(),
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: primary.clone(),
    };
    let bytes = b"actual native same-node overwrite, committed by ordinary Main maintenance";
    let dispatch_start = runtime.rpc_observations().len();
    let (route_id, intent_id, manifest) = browser
        .with_mutation_service(|service| {
            assert!(service.transport.delegates_local_namespace_authority());
            let manifest = service
                .content()
                .put_object_version_blob_only(
                    request.object_id.as_str(),
                    request.target_version.as_str(),
                    bytes,
                )
                .unwrap();
            let mut policy = DfsReplicationPolicy::new(2, 3).unwrap();
            policy.required_node_ids = nodes.clone();
            let initial = service
                .write_distributed_manifest_with_candidates(
                    request.clone(),
                    Some(base.version.clone()),
                    manifest.content_hash.clone(),
                    manifest.size_bytes,
                    nodes.clone(),
                    policy,
                    DfsDistributedWriteOptions {
                        publication: DfsWritePublication::Current,
                        repair: DfsWriteRepair::Background,
                        ..DfsDistributedWriteOptions::default()
                    },
                )
                .unwrap();
            let NetworkedDfsWriteOutcome::Pending {
                intent_id: route_id,
                ..
            } = initial
            else {
                panic!("real denied transfers must leave the original caller Pending: {initial:?}")
            };
            service.process_incoming().unwrap();
            let progress = service.write_outcome(&route_id).unwrap();
            let NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts: 1,
                required_receipts: 2,
            } = progress
            else {
                panic!("real canonical W2 progress must remain 1/2: {progress:?}")
            };
            assert_eq!(service.transport.pending_incoming_count().unwrap(), Some(0));
            Ok((route_id, intent_id, manifest))
        })
        .unwrap();
    let inspect = runtime.metadata_primary_service("/services").unwrap();
    let pending = inspect
        .metadata_backend()
        .unwrap()
        .get_intent(&request.object_id, &intent_id)
        .unwrap()
        .unwrap();
    assert_ne!(pending.status, DfsWriteIntentStatus::Committed);
    assert_eq!(pending.operation_id.as_deref(), Some(route_id.as_str()));
    assert_eq!(pending.target_version, request.target_version);
    drop(inspect);

    runtime.clear_faults();
    runtime.request_repair_reconciliation(&primary).unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "Main committed reply did not wake its original canonical inbox"
        );
        runtime
            .maintenance_tick(&primary, remaining)
            .unwrap()
            .outcome
            .unwrap();
        let ready = browser
            .with_mutation_service(|service| {
                Ok(service.transport.wait_for_incoming(Duration::ZERO).unwrap())
            })
            .unwrap();
        if ready {
            break;
        }
    }
    let events = browser.with_mutation_service(|service| {
        let events = service.process_incoming().unwrap();
        assert_eq!(service.transport.pending_incoming_count().unwrap(), Some(0));
        let outcome = service.take_write_outcome(&route_id).unwrap();
        assert!(matches!(outcome, NetworkedDfsWriteOutcome::Committed { ref version, ref content_hash, .. }
            if version == &request.target_version && content_hash == &manifest.content_hash));
        Ok(events)
    }).unwrap();
    assert_eq!(events.iter().filter(|event| matches!(event,
        NetworkedDfsServiceEvent::RemoteWriteCompleted { request_id, object_id, primary_node_id,
            result: DfsDistributedWriteResult::Outcome { outcome: NetworkedDfsWriteOutcome::Committed {
                version, content_hash, .. } } }
            if request_id == &route_id && object_id == &request.object_id
                && primary_node_id == &primary && version == &request.target_version
                && content_hash == &manifest.content_hash)).count(), 1);

    let inspect = runtime.metadata_primary_service("/services").unwrap();
    let committed = inspect
        .metadata_backend()
        .unwrap()
        .get_intent(&request.object_id, &intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(committed.status, DfsWriteIntentStatus::Committed);
    assert_eq!(committed.operation_id.as_deref(), Some(route_id.as_str()));
    assert_eq!(committed.target_version, request.target_version);
    let receipts = inspect
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&request.object_id, &request.target_version)
        .unwrap();
    assert!(receipts.len() >= 2);
    assert!(receipts
        .iter()
        .all(|receipt| receipt.content_hash.as_deref() == Some(manifest.content_hash.as_str())));
    assert_eq!(
        inspect
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(inspect);
    runtime
        .maintenance_tick(&primary, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap()
        .outcome
        .unwrap();
    browser.with_mutation_service(|service| {
        assert!(!service.transport.wait_for_incoming(Duration::ZERO).unwrap());
        assert!(!service.process_incoming().unwrap().iter().any(|event| matches!(event,
            NetworkedDfsServiceEvent::RemoteWriteCompleted { request_id, .. } if request_id == &route_id)));
        Ok(())
    }).unwrap();
    assert_eq!(
        runtime.rpc_observations()[dispatch_start..]
            .iter()
            .filter(|event| event.source == primary
                && event.target == primary
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && event.phase == RpcPhase::BeforeDispatch)
            .count(),
        1,
        "the original caller dispatched one semantic native write"
    );
    drop(browser);
    runtime.close().unwrap();
}
