//! Durable transfer ownership crosses exact/global service lanes and restart.

use super::*;

const PAYLOAD: &[u8] = b"excluded requester bytes cross a reconstructed receiver lane";

struct PendingTransferFixture {
    fixture: PrestageFixture,
    replicas: Vec<PrestageService>,
    resident: crate::ResidentLocalShardedMetadataBackend,
    control_plane: SharedDfsControlPlaneKvClient<InProcessKvStore>,
    shard: DfsMetadataShardConfig,
    intent: DfsWriteIntentRecord,
    response: DfsFetchBlobResponse,
    desired: Vec<NodeId>,
}

fn pending_transfer_fixture(root: &Path, w: usize, n: usize) -> PendingTransferFixture {
    let mut fixture = prestage_fixture(root);
    let shard = fixture
        .primary
        .select_metadata_shard_for_object(&fixture.current)
        .unwrap()
        .unwrap();
    let file_system = file_system(fixture.primary_id.clone());
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .unwrap();
        control.put_metadata_shard(&shard).unwrap();
    });
    fixture.primary.control_plane = Some(Box::new(control_plane.clone()));
    fixture.requester.control_plane = Some(Box::new(control_plane.clone()));

    // Apply the initial write through an actual exact resident shard lease.
    fixture.primary.metadata_backend.take();
    let resident =
        LocalShardedMetadataBackend::new(root.join("primary-metadata"), vec![shard.clone()])
            .unwrap()
            .into_resident();
    let exact = resident
        .scope_shard(
            &file_system.file_system_id,
            &DfsMetadataShardId::new("root"),
        )
        .unwrap()
        .checkout()
        .unwrap();
    fixture
        .primary
        .attach_resident_metadata_shard_backend(exact)
        .unwrap();
    assert!(
        fixture
            .primary
            .metadata_backend
            .as_ref()
            .unwrap()
            .resident_exact_shard_id()
            .is_some()
    );

    let mut desired = vec![fixture.primary_id.clone()];
    let mut replicas = Vec::new();
    for index in 1..n {
        let node =
            NodeId::from_legacy_named(format!("reconstructed-fetch-replica-{index}")).unwrap();
        desired.push(node.clone());
        let mut metadata =
            open_root_local_metadata_backend(root.join(format!("replica-{index}-metadata")))
                .unwrap();
        metadata.put_file_system(&file_system).unwrap();
        metadata.put_object(&fixture.current).unwrap();
        replicas.push(
            NetworkedDfsService::new(
                fixture.network.add_node(node),
                LocalContentStore::new(root.join(format!("replica-{index}-content"))).unwrap(),
            )
            .with_control_plane(control_plane.clone())
            .with_metadata_backend(metadata)
            .unwrap(),
        );
    }
    let target = DfsVersion::from_legacy_named("reconstructed-fetch-target").unwrap();
    fixture
        .requester
        .content
        .put_object_version(fixture.current.object_id.as_str(), target.as_str(), PAYLOAD)
        .unwrap();
    let mut policy = DfsReplicationPolicy::new(w, n).unwrap();
    policy.required_node_ids = desired.clone();
    policy.forbidden_node_ids.push(fixture.requester_id.clone());
    let outcome = fixture
        .primary
        .write_stable_remote_distributed_manifest(
            DfsWriteRequest {
                file_system_id: fixture.current.file_system_id.clone(),
                object_id: fixture.current.object_id.clone(),
                target_version: target,
                requesting_node_id: fixture.requester_id.clone(),
            },
            Some(fixture.current.version.clone()),
            content_hash(PAYLOAD),
            PAYLOAD.len() as u64,
            true,
            desired.clone(),
            policy,
            "reconstructed-fetch-operation".into(),
            DfsDistributedWriteOptions {
                repair: DfsWriteRepair::Background,
                ..Default::default()
            },
        )
        .unwrap();
    let NetworkedDfsWriteOutcome::Pending {
        intent_id,
        durable_receipts,
        required_receipts,
    } = outcome
    else {
        panic!("excluded requester must not satisfy W: {outcome:?}");
    };
    assert_eq!((durable_receipts, required_receipts), (0, w));
    let intent = fixture
        .primary
        .metadata_backend
        .as_ref()
        .unwrap()
        .get_intent(&fixture.current.object_id, &intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(intent.coordinator_node_id, fixture.requester_id);
    assert_eq!(intent.status, DfsWriteIntentStatus::ReceiptsPending);
    assert!(
        !intent
            .desired_replica_node_ids
            .contains(&fixture.requester_id)
    );
    let mut source_metadata =
        open_root_local_metadata_backend(root.join("source-pending-metadata")).unwrap();
    source_metadata.put_file_system(&file_system).unwrap();
    source_metadata.put_object(&fixture.current).unwrap();
    source_metadata.put_intent(&intent).unwrap();
    fixture
        .requester
        .set_metadata_backend_unhydrated(source_metadata);
    fixture
        .requester
        .process_incoming_without_deferred_parent_touches()
        .unwrap();
    let messages = fixture.primary.transport.drain_incoming_messages().unwrap();
    let response = messages
        .into_iter()
        .find_map(|message| match message.message {
            DfsNetworkMessage::FetchBlobResponse(response) => Some(response),
            _ => None,
        })
        .expect("the real requester fetch must produce a streamed response");
    assert!(
        matches!(&response.result, DfsFetchBlobResult::Found { bytes, .. } if bytes == PAYLOAD)
    );
    fixture.primary.release_resident_metadata_backend().unwrap();
    PendingTransferFixture {
        fixture,
        replicas,
        resident,
        control_plane,
        shard,
        intent,
        response,
        desired,
    }
}

fn fresh_receiver(transfer: &PendingTransferFixture, root: &Path, reopen: bool) -> PrestageService {
    let service = NetworkedDfsService::new(
        transfer
            .fixture
            .network
            .add_node(transfer.fixture.primary_id.clone()),
        LocalContentStore::new(root.join("primary-content")).unwrap(),
    )
    .with_control_plane(transfer.control_plane.clone());
    let receiver = if reopen {
        service
            .with_metadata_backend_lazy(
                open_local_sharded_metadata_backend(
                    root.join("primary-metadata"),
                    vec![transfer.shard.clone()],
                )
                .unwrap(),
            )
            .unwrap()
    } else {
        service
            .with_resident_sharded_metadata_backend_lazy(
                transfer.resident.checkout_global().unwrap(),
            )
            .unwrap()
    };
    assert!(receiver.writes.pending_writes.is_empty());
    assert!(
        receiver
            .metadata_backend
            .as_ref()
            .unwrap()
            .resident_exact_shard_id()
            .is_none()
    );
    receiver
}

#[test]
fn excluded_requester_fetch_crosses_fresh_global_and_reopened_receiver_standard_w_n() {
    for reopen in [false, true] {
        for (w, n) in [(1, 2), (2, 3)] {
            let root = temp_root(&format!("fetch-reconstruct-w{w}-n{n}-reopen-{reopen}"));
            let mut transfer = pending_transfer_fixture(root.path(), w, n);
            let mut receiver = fresh_receiver(&transfer, root.path(), reopen);
            let event = receiver
                .handle_fetch_blob_response(
                    transfer.fixture.requester_id.clone(),
                    transfer.response.clone(),
                )
                .unwrap();
            assert!(
                matches!(event, NetworkedDfsServiceEvent::BlobFetched { .. }),
                "{event:?}"
            );
            assert_eq!(
                receiver
                    .coordinator
                    .durable_receipt_count(&transfer.intent.intent_id),
                1
            );
            assert!(
                receiver
                    .coordinator
                    .receipts_for_intent(&transfer.intent.intent_id)
                    .iter()
                    .all(|receipt| receipt.replica_node_id == transfer.fixture.primary_id)
            );
            if w == 2 {
                assert_eq!(
                    receiver
                        .current_object_for_write(&transfer.intent.object_id)
                        .unwrap()
                        .version,
                    transfer.fixture.current.version,
                    "publication must wait for the second selected replica"
                );
            }
            let mut complete = false;
            for _ in 0..32 {
                receiver.retry_pending_repairs().unwrap();
                for replica in &mut transfer.replicas {
                    replica
                        .process_incoming_without_deferred_parent_touches()
                        .unwrap();
                }
                receiver
                    .process_incoming_without_deferred_parent_touches()
                    .unwrap();
                if receiver
                    .coordinator
                    .durable_receipt_count(&transfer.intent.intent_id)
                    == n
                {
                    complete = true;
                    break;
                }
            }
            assert!(
                complete,
                "reactive repair must complete all N={n} selected copies"
            );
            assert!(matches!(
                receiver.write_outcome(&transfer.intent.intent_id).unwrap(),
                NetworkedDfsWriteOutcome::Committed { .. }
            ));
            let receipts = receiver
                .coordinator
                .receipts_for_intent(&transfer.intent.intent_id);
            assert_eq!(receipts.len(), n);
            assert!(
                receipts
                    .iter()
                    .all(|receipt| transfer.desired.contains(&receipt.replica_node_id))
            );
            assert!(
                receipts
                    .iter()
                    .all(|receipt| receipt.replica_node_id != transfer.fixture.requester_id)
            );
            assert_durable_binding(
                &root.join("primary-content"),
                &transfer.intent.object_id,
                &transfer.intent.target_version,
                PAYLOAD,
            );
            for (index, _) in transfer.replicas.iter().enumerate() {
                assert_durable_binding(
                    &root.join(format!("replica-{}-content", index + 1)),
                    &transfer.intent.object_id,
                    &transfer.intent.target_version,
                    PAYLOAD,
                );
            }
        }
    }
}

#[test]
fn reconstructed_requester_response_requires_current_authority_active_exact_intent_and_valid_bytes()
{
    for invalid in [
        "no-projection",
        "foreign-primary",
        "stale-epoch",
        "aborted",
        "committed",
        "retained",
        "object",
        "version",
        "hash",
        "bytes",
        "source",
        "frame-source",
    ] {
        let root = temp_root(&format!("fetch-reconstruct-reject-{invalid}"));
        let mut transfer = pending_transfer_fixture(root.path(), 1, 2);
        let mut receiver = fresh_receiver(&transfer, root.path(), false);
        let mut response = transfer.response.clone();
        let mut frame_source = transfer.fixture.requester_id.clone();
        match invalid {
            "no-projection" => {
                receiver.control_plane.take();
            }
            "foreign-primary" => {
                transfer.shard.primary_node_id =
                    NodeId::from_legacy_named("foreign-fetch-primary").unwrap();
                transfer.control_plane.with_client(|kv| {
                    DfsControlPlaneStore::new(kv)
                        .put_metadata_shard(&transfer.shard)
                        .unwrap()
                });
            }
            "stale-epoch" | "aborted" | "committed" | "retained" => {
                let mut record = transfer.intent.clone();
                match invalid {
                    "stale-epoch" => record.owner_epoch += 1,
                    "aborted" => record.status = DfsWriteIntentStatus::Aborted,
                    "committed" => record.status = DfsWriteIntentStatus::Committed,
                    "retained" => {
                        // Retained repair is committed maintenance ownership,
                        // never a pending write request. Use its valid durable
                        // shape while proving it cannot grant this Current
                        // write's requester-source fallback.
                        record.publication = DfsWritePublication::RetainedRepair;
                        record.status = DfsWriteIntentStatus::Committed;
                        record.base_version = None;
                        record.operation_id = None;
                        record.operation_order_token = None;
                        record.touch_parent_on_create = false;
                        record.create_predecessor_object_id = None;
                        record.object_metadata = None;
                        // A durable intent's publication purpose is immutable.
                        // Maintenance has its own identity, rather than
                        // rewriting the active Current request in this fixture.
                        record.intent_id = "history-repair:reconstructed-fetch-negative".into();
                        response.intent_id.clone_from(&record.intent_id);
                        assert!(
                            receiver
                                .local_pending_write_authority_shard(&record)
                                .unwrap()
                                .is_none()
                        );
                    }
                    _ => unreachable!(),
                }
                receiver
                    .metadata_backend
                    .as_mut()
                    .unwrap()
                    .put_intent(&record)
                    .unwrap();
            }
            "object" => {
                response.object_id = DfsObjectId::from_legacy_named("other-fetch-object").unwrap()
            }
            "version" => {
                response.version = DfsVersion::from_legacy_named("other-fetch-version").unwrap()
            }
            "hash" => {
                response.result = DfsFetchBlobResult::Found {
                    content_hash: content_hash(b"different claimed payload"),
                    bytes: PAYLOAD.to_vec(),
                }
            }
            "bytes" => {
                response.result = DfsFetchBlobResult::Found {
                    content_hash: content_hash(PAYLOAD),
                    bytes: b"corrupted response bytes".to_vec(),
                }
            }
            "source" => {
                frame_source = NodeId::from_legacy_named("unauthorized-fetch-source").unwrap();
                response.source_node_id = frame_source.clone();
            }
            "frame-source" => {
                frame_source = NodeId::from_legacy_named("mismatched-fetch-source").unwrap()
            }
            _ => unreachable!(),
        }
        let result = receiver.handle_fetch_blob_response(frame_source, response);
        assert!(
            !matches!(result, Ok(NetworkedDfsServiceEvent::BlobFetched { .. })),
            "{invalid} must not be accepted: {result:?}"
        );
        assert!(
            receiver
                .coordinator
                .receipts_for_intent(&transfer.intent.intent_id)
                .is_empty(),
            "{invalid} must not contribute W receipts"
        );
        assert_eq!(
            receiver
                .metadata_backend
                .as_ref()
                .unwrap()
                .get_object(&transfer.intent.object_id)
                .unwrap()
                .unwrap()
                .version,
            transfer.fixture.current.version,
            "{invalid} must not publish the target"
        );
        assert!(
            !receiver.content.has_blob(&content_hash(PAYLOAD)).unwrap(),
            "{invalid} must not install the exact target blob"
        );
    }
}
