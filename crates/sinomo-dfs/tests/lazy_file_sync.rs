use std::sync::{Arc, Mutex};
use std::time::Duration;

use sinomo_core::network::NetworkError;
use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsMessageTransport, DfsMetadataBackend, DfsMetadataShardId, DfsNetworkMessage, DfsPosixError,
    DfsPosixSyncKind, DfsReplicationPolicy, DfsRoutedCall, DfsRoutedMessage, DfsTransportIdentity,
    DfsWriteOptions, LocalContentStore, LocalSobjMetadataBackend, NetworkedDfsService,
};

struct OfflineTransport {
    node: NodeId,
    repaired_objects: Arc<Mutex<Vec<sinomo_dfs::DfsObjectId>>>,
}

#[test]
fn file_sync_certifies_a_completed_placement_plan_without_lowering_replica_count() {
    let root = sinomo_testdir::TestDir::new("file-sync-placement-plan");
    let node = NodeId::from_legacy_named("sync_placement_owner").unwrap();
    let old_replica = NodeId::from_legacy_named("sync_placement_old").unwrap();
    let new_replica = NodeId::from_legacy_named("sync_placement_new").unwrap();
    let repaired_objects = Arc::new(Mutex::new(Vec::new()));
    let transport = || OfflineTransport {
        node: node.clone(),
        repaired_objects: Arc::clone(&repaired_objects),
    };
    let backend = || {
        LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.path().join("metadata"),
        )
        .unwrap()
    };
    let content = || LocalContentStore::new(root.path().join("content")).unwrap();
    let mut service = NetworkedDfsService::new(transport(), content())
        .with_metadata_backend(backend())
        .unwrap();
    service.bootstrap_distributed_sifs().unwrap();
    service
        .posix_write_file(
            "/services/moved.txt",
            b"frozen bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                replica_candidates: vec![node.clone(), old_replica],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let target = service
        .posix_capture_file_sync_target("/services/moved.txt", DfsPosixSyncKind::File)
        .unwrap();
    drop(service);

    let mut store = backend();
    let mut placement = store
        .intents_for_object(&target.object_id)
        .unwrap()
        .into_iter()
        .find(|intent| intent.target_version == target.version)
        .unwrap();
    placement.intent_id = "placement:sync-regression".into();
    placement.desired_replica_node_ids = vec![node.clone(), new_replica.clone()];
    let mut weaker = placement.clone();
    weaker.intent_id = "placement:weaker-regression".into();
    weaker.policy = DfsReplicationPolicy::new(1, 1).unwrap();
    weaker.desired_replica_node_ids = vec![node.clone()];
    store.put_intent(&weaker).unwrap();
    drop(store);
    let service = NetworkedDfsService::new(transport(), content())
        .with_metadata_backend_lazy(backend())
        .unwrap();
    assert!(matches!(
        service.posix_validate_file_sync_target_local(&target),
        Err(DfsPosixError::PendingWrite {
            required_receipts: 2,
            ..
        })
    ));
    drop(service);

    let mut store = backend();
    store.put_intent(&placement).unwrap();
    let mut receipt = store
        .receipts_for_version(&target.object_id, &target.version)
        .unwrap()
        .into_iter()
        .find(|receipt| receipt.replica_node_id == node)
        .unwrap();
    receipt.replica_node_id = new_replica.clone();
    store.put_receipt(&receipt).unwrap();
    drop(store);
    let mut service = NetworkedDfsService::new(transport(), content())
        .with_metadata_backend_lazy(backend())
        .unwrap();
    let evidence = service
        .posix_validate_file_sync_target_local(&target)
        .unwrap();
    assert_eq!(evidence.intent_id, placement.intent_id);
    assert_eq!(evidence.required_receipts, 2);
    assert!(evidence.receipt_node_ids.contains(&node));
    assert!(evidence.receipt_node_ids.contains(&new_replica));
    let certificate = service
        .posix_sync_file_local(&target, Duration::ZERO)
        .unwrap();
    assert_eq!(certificate.content_evidence, evidence);
    assert!(
        certificate
            .metadata_certificate
            .acknowledged_node_ids
            .contains(&node)
    );
    let mut wrong_hash = target;
    wrong_hash.content_hash = "blake3:wrong".into();
    assert!(
        service
            .posix_validate_file_sync_target_local(&wrong_hash)
            .is_err()
    );
}

impl DfsTransportIdentity for OfflineTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.node
    }
}

impl DfsMessageTransport for OfflineTransport {
    fn send_routed_call(
        &mut self,
        target: &NodeId,
        call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        if let DfsNetworkMessage::RepairReplica(command) = call.message.message {
            self.repaired_objects
                .lock()
                .unwrap()
                .push(command.object_id);
        }
        Err(NetworkError::UnknownNode {
            node_id: target.clone(),
        })
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(Vec::new())
    }

    fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        Ok(Some(0))
    }
}

#[test]
fn lazy_file_sync_restores_only_its_pending_replica_repair() {
    let root = sinomo_testdir::TestDir::new("lazy-file-sync-repair");
    let node = NodeId::from_legacy_named("lazy_sync_owner").unwrap();
    let replica = NodeId::from_legacy_named("lazy_sync_offline").unwrap();
    let repaired_objects = Arc::new(Mutex::new(Vec::new()));
    let open_service = |lazy| {
        let backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.path().join("metadata"),
        )
        .unwrap();
        let service = NetworkedDfsService::new(
            OfflineTransport {
                node: node.clone(),
                repaired_objects: Arc::clone(&repaired_objects),
            },
            LocalContentStore::new(root.path().join("content")).unwrap(),
        );
        if lazy {
            service.with_metadata_backend_lazy(backend).unwrap()
        } else {
            service.with_metadata_backend(backend).unwrap()
        }
    };
    let mut service = open_service(false);
    service.bootstrap_distributed_sifs().unwrap();
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 2).unwrap(),
        replica_candidates: vec![node.clone(), replica],
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    for path in ["/services/sync.txt", "/services/unrelated.txt"] {
        service
            .posix_write_file(path, b"committed prefix", options.clone())
            .unwrap();
    }
    let target = service
        .posix_capture_file_sync_target("/services/sync.txt", DfsPosixSyncKind::File)
        .unwrap();
    drop(service);
    repaired_objects.lock().unwrap().clear();

    let mut service = open_service(true);
    assert!(service.coordinator().repair_jobs().is_empty());
    let error = service
        .posix_sync_file_local(&target, Duration::ZERO)
        .unwrap_err();
    assert!(matches!(
        error,
        DfsPosixError::PendingWrite {
            durable_receipts: 1,
            required_receipts: 2,
            ..
        }
    ));
    let repaired = repaired_objects.lock().unwrap();
    assert!(
        !repaired.is_empty(),
        "sync must drive its durable repair after lazy reopen"
    );
    assert!(repaired.iter().all(|object| object == &target.object_id));
}
