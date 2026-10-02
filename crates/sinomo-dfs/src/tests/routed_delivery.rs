//! Requester ownership survives an ambiguous routed-write dispatch.

use super::*;
use sinomo_core::network::NetworkDeliveryState;

struct RoutedWriteDeliveryFailureTransport {
    node_id: NodeId,
    failures: VecDeque<NetworkDeliveryState>,
    observed: Vec<DfsDistributedWriteCommand>,
}

impl DfsTransportIdentity for RoutedWriteDeliveryFailureTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.node_id
    }
}

impl DfsMessageTransport for RoutedWriteDeliveryFailureTransport {
    fn send_routed_call(
        &mut self,
        _target_node_id: &NodeId,
        call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        let DfsNetworkMessage::DistributedWriteRequest(command) = call.message.message else {
            panic!("only the original write may be dispatched");
        };
        self.observed.push(command);
        let state = self.failures.pop_front().expect("one caller-owned attempt");
        Err(NetworkError::delivery_failed(
            "injected routed-write delivery",
            state,
            "request bytes may have reached the primary before the reply was lost",
        ))
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(Vec::new())
    }
}

fn routed_delivery_fixture(
    label: &str,
    failures: impl IntoIterator<Item = NetworkDeliveryState>,
) -> (
    NetworkedDfsService<RoutedWriteDeliveryFailureTransport>,
    DfsObjectMetadata,
    DfsMetadataShardConfig,
    sinomo_testdir::TestDir,
) {
    let root = temp_root(label);
    let requester = NodeId::from_legacy_named(format!("{label}-requester")).unwrap();
    let primary = NodeId::from_legacy_named(format!("{label}-primary")).unwrap();
    let shard = test_routed_authority_shard(&primary, 1);
    let mut object = file_metadata(primary);
    object.file_system_id = sifs_file_system_id();
    object.path = "/routed-delivery.bin".into();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    let service = NetworkedDfsService::new(
        RoutedWriteDeliveryFailureTransport {
            node_id: requester,
            failures: failures.into_iter().collect(),
            observed: Vec::new(),
        },
        LocalContentStore::new(root.join("content")).unwrap(),
    )
    .with_control_plane(test_routed_authority_control_plane(&shard));
    (service, object, shard, root)
}

fn routed_delivery_request(
    service: &NetworkedDfsService<RoutedWriteDeliveryFailureTransport>,
    object: &DfsObjectMetadata,
) -> DfsWriteRequest {
    DfsWriteRequest {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        target_version: DfsVersion::from_legacy_named("routed-delivery-target").unwrap(),
        requesting_node_id: service.node_id.clone(),
    }
}

fn route_delivery_bytes(
    service: &mut NetworkedDfsService<RoutedWriteDeliveryFailureTransport>,
    object: &DfsObjectMetadata,
    shard: &DfsMetadataShardConfig,
) -> Result<NetworkedDfsWriteOutcome, DfsError> {
    service.route_distributed_bytes(
        routed_delivery_request(service, object),
        Some(object.version.clone()),
        b"one immutable write payload".to_vec(),
        vec![service.node_id.clone(), shard.primary_node_id.clone()],
        DfsReplicationPolicy::new(1, 2).unwrap(),
        object.clone(),
        shard.clone(),
        DfsDistributedWriteOptions {
            operation_id: Some("routed-delivery-operation".into()),
            ..Default::default()
        },
    )
}

fn deliver_late_commit(
    service: &mut NetworkedDfsService<RoutedWriteDeliveryFailureTransport>,
    object: &DfsObjectMetadata,
    shard: &DfsMetadataShardConfig,
) {
    let request = routed_delivery_request(service, object);
    let outcome = NetworkedDfsWriteOutcome::Committed {
        intent_id: "remote-durable-intent".into(),
        version: request.target_version,
        content_hash: content_hash(b"one immutable write payload"),
        repair_jobs: Vec::new(),
    };
    assert!(matches!(
        service
            .handle_distributed_write_response(
                shard.primary_node_id.clone(),
                DfsDistributedWriteResponse {
                    request_id: "routed-delivery-operation".into(),
                    object_id: object.object_id.clone(),
                    result: DfsDistributedWriteResult::Outcome {
                        outcome: outcome.clone(),
                    },
                    metadata_journal_batches: Vec::new(),
                },
            )
            .unwrap(),
        NetworkedDfsServiceEvent::RemoteWriteCompleted { .. }
    ));
    assert_eq!(
        service.write_outcome("routed-delivery-operation").unwrap(),
        outcome
    );
}

#[test]
fn ambiguous_attachment_write_preserves_compact_owner_and_accepts_late_commit() {
    let (mut service, object, shard, root) = routed_delivery_fixture(
        "ambiguous-attachment",
        [NetworkDeliveryState::PossiblyApplied],
    );
    let error = route_delivery_bytes(&mut service, &object, &shard).unwrap_err();
    assert!(matches!(
        error,
        DfsError::NetworkDelivery {
            delivery_state: NetworkDeliveryState::PossiblyApplied,
            ref message,
        } if message.contains("routed-delivery-operation")
    ));
    let pending = &service.writes.pending_routed_writes["routed-delivery-operation"];
    assert!(pending.command.is_none() && pending.attachment_bytes.is_none());
    assert!(pending.result.is_none() && pending.compact_retained_at.is_some());
    assert_eq!(pending.durable_receipts, 0);
    assert_eq!(
        service.transport.observed.len(),
        1,
        "no implicit fresh retry"
    );
    deliver_late_commit(&mut service, &object, &shard);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn definitely_unsent_attachment_write_releases_requester_owner() {
    let (mut service, object, shard, root) = routed_delivery_fixture(
        "unsent-attachment",
        [NetworkDeliveryState::DefinitelyNotSent],
    );
    assert!(matches!(
        route_delivery_bytes(&mut service, &object, &shard).unwrap_err(),
        DfsError::NetworkDelivery {
            delivery_state: NetworkDeliveryState::DefinitelyNotSent,
            ..
        }
    ));
    assert!(service.writes.pending_routed_writes.is_empty());
    assert_eq!(service.transport.observed.len(), 1);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn unsent_explicit_exact_id_retry_cannot_erase_earlier_ambiguous_write() {
    let (mut service, object, shard, root) = routed_delivery_fixture(
        "ambiguous-then-unsent",
        [
            NetworkDeliveryState::PossiblyApplied,
            NetworkDeliveryState::DefinitelyNotSent,
        ],
    );
    route_delivery_bytes(&mut service, &object, &shard).unwrap_err();
    let previous = service.writes.pending_routed_writes["routed-delivery-operation"].clone();
    assert!(matches!(
        route_delivery_bytes(&mut service, &object, &shard).unwrap_err(),
        DfsError::NetworkDelivery {
            delivery_state: NetworkDeliveryState::PossiblyApplied,
            ..
        }
    ));
    assert_eq!(
        service.writes.pending_routed_writes["routed-delivery-operation"],
        previous
    );
    assert_eq!(service.transport.observed.len(), 2);
    assert_eq!(service.transport.observed[0], service.transport.observed[1]);
    deliver_late_commit(&mut service, &object, &shard);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn manifest_and_reader_dispatch_failure_retain_exact_identity_without_counting_receipts() {
    for reader in [false, true] {
        let (mut service, object, shard, root) = routed_delivery_fixture(
            if reader {
                "ambiguous-reader"
            } else {
                "ambiguous-manifest"
            },
            [NetworkDeliveryState::PossiblyApplied],
        );
        let request = routed_delivery_request(&service, &object);
        let bytes = b"one immutable write payload".to_vec();
        let options = DfsDistributedWriteOptions {
            operation_id: Some("routed-delivery-operation".into()),
            ..Default::default()
        };
        let replicas = vec![service.node_id.clone(), shard.primary_node_id.clone()];
        let policy = DfsReplicationPolicy::new(2, 2).unwrap();
        let result = if reader {
            service.route_distributed_reader(
                request,
                Some(object.version.clone()),
                std::io::Cursor::new(bytes),
                replicas,
                policy,
                object.clone(),
                shard.clone(),
                options,
            )
        } else {
            service.route_distributed_manifest(
                request,
                Some(object.version.clone()),
                content_hash(&bytes),
                bytes.len() as u64,
                replicas,
                policy,
                object.clone(),
                shard.clone(),
                options,
            )
        };
        assert!(matches!(
            result,
            Err(DfsError::NetworkDelivery {
                delivery_state: NetworkDeliveryState::PossiblyApplied,
                ..
            })
        ));
        let pending = &service.writes.pending_routed_writes["routed-delivery-operation"];
        assert_eq!(pending.durable_receipts, 0);
        assert!(pending.command.is_none());
        assert_eq!(service.transport.observed.len(), 1);
        assert_eq!(
            service.transport.observed[0].request_id,
            "routed-delivery-operation"
        );
        deliver_late_commit(&mut service, &object, &shard);
        std::fs::remove_dir_all(root).ok();
    }
}

#[test]
fn authority_redirect_lost_reply_preserves_new_target_for_late_completion() {
    let (mut service, object, retiring_shard, root) = routed_delivery_fixture(
        "redirect-lost-reply",
        [NetworkDeliveryState::PossiblyApplied],
    );
    let mut active_shard = retiring_shard.clone();
    active_shard.primary_node_id = NodeId::from_legacy_named("redirect-active-primary").unwrap();
    active_shard.epoch += 1;
    service.control_plane = Some(Box::new(test_routed_authority_control_plane(&active_shard)));
    let mut pending = test_pending_routed_write(
        "routed-delivery-operation",
        &service.node_id,
        &object.object_id,
        &retiring_shard.primary_node_id,
        retiring_shard.clone(),
    );
    let command = pending.command.as_mut().unwrap();
    command.request = routed_delivery_request(&service, &object);
    command.object_metadata = Some(object.clone());
    command.authority_shard = Some(retiring_shard.clone());
    command.content_hash = None;
    command.content_size_bytes = None;
    command.requester_blob_pre_staged = false;
    pending.attachment_bytes = Some(bytes::Bytes::from_static(b"one immutable write payload"));
    service
        .writes
        .pending_routed_writes
        .insert("routed-delivery-operation".into(), pending);

    let error = service
        .handle_distributed_write_response(
            retiring_shard.primary_node_id,
            DfsDistributedWriteResponse {
                request_id: "routed-delivery-operation".into(),
                object_id: object.object_id.clone(),
                result: DfsDistributedWriteResult::Rejected {
                    reason: "metadata authority retired".into(),
                    current_primary_node_id: Some(active_shard.primary_node_id.clone()),
                },
                metadata_journal_batches: Vec::new(),
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        DfsError::NetworkDelivery {
            delivery_state: NetworkDeliveryState::PossiblyApplied,
            ..
        }
    ));
    let pending = &service.writes.pending_routed_writes["routed-delivery-operation"];
    assert_eq!(pending.primary_node_id, active_shard.primary_node_id);
    assert_eq!(pending.authority_shard, active_shard);
    assert!(pending.result.is_none() && pending.command.is_none());
    assert!(pending.attachment_bytes.is_none());
    assert_eq!(pending.durable_receipts, 0);
    assert_eq!(service.transport.observed.len(), 1);
    assert_eq!(
        service.transport.observed[0].request_id,
        "routed-delivery-operation"
    );
    assert_eq!(
        service.transport.observed[0].authority_shard,
        Some(active_shard.clone())
    );
    deliver_late_commit(&mut service, &object, &active_shard);
    std::fs::remove_dir_all(root).ok();
}
