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
