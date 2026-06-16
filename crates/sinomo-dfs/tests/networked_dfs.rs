use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt, DfsWriteRequest};
use sinomo_dfs::{
    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, DFS_PROTOCOL, DfsBlobTransferResult,
    DfsCommitVersionCommand, DfsConfirmedTimeoutAction, DfsConfirmedTimeoutOutcome,
    DfsConflictStatus, DfsControlPlaneStore, DfsDistributedWriteResult, DfsFetchBlobResponse,
    DfsFetchBlobResponseChunk, DfsFetchBlobResult, DfsFileSystem, DfsFileSystemId,
    DfsFrameTransport, DfsMetadataBackend, DfsMetadataMutation, DfsMetadataReplicationOutcome,
    DfsMetadataShardAssignment, DfsMetadataShardConfig, DfsMetadataShardId, DfsNetworkMessage,
    DfsNodeAdvertisement, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsObjectVersionRecord,
    DfsOwnerHandoffOutcome, DfsReadData, DfsReadResponse, DfsReadResponseChunk, DfsReadResult,
    DfsRepairReplicaChunkCommand, DfsRepairReplicaCommand, DfsRepairReplicaResponse,
    DfsReplicaReceiptRecord, DfsReplicationPolicy, DfsStageBlobChunkCommand, DfsStageBlobCommand,
    DfsStageBlobResponse, DfsVersion, DfsWriteIntentState, DfsWriteIntentStatus,
    LocalSobjMetadataBackend, NetworkedDfsReadFreshness, NetworkedDfsReadOutcome,
    NetworkedDfsService, NetworkedDfsServiceEvent, NetworkedDfsWriteOutcome,
    SharedDfsControlPlaneKvClient, file_system_config_key, sifs_node_files_path,
};
use sinomo_events::EventId;
use sinomo_kv::InProcessRaftKvCluster;
use sinomo_network::{
    InProcessNetwork, MeshMessage, MeshMessageBody, MeshMessageId, NetworkFrame, NetworkFrameId,
    ProtocolMessage,
};
use sinomo_node::NodeId;
use std::fs;
use std::io::{Read, Result as IoResult};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;

const TEST_DFS_CHUNK_BYTES: usize = 1024 * 1024;

fn chunked_test_bytes(extra_bytes: usize) -> Vec<u8> {
    (0..(TEST_DFS_CHUNK_BYTES + extra_bytes))
        .map(|index| (index % 251) as u8)
        .collect()
}

#[test]
fn networked_dfs_commits_after_stage_ack_and_replica_reads_content() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let attacker = NodeId::from_legacy_named("node_attacker").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_commit_after_stage_ack");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let mut attacker_transport = network.add_node(attacker.clone());
    let mut replica_forge_transport = network.add_node(replica.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner DfsFileSystem");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica DfsFileSystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"networked bytes".to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("write");

    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending {
            intent_id,
            durable_receipts,
            required_receipts,
        } => {
            assert_eq!(durable_receipts, 1);
            assert_eq!(required_receipts, 2);
            intent_id
        }
        NetworkedDfsWriteOutcome::Committed { .. } | NetworkedDfsWriteOutcome::Conflict { .. } => {
            panic!("write should wait for replica ack")
        }
    };

    let forged_hash = content_hash(b"networked bytes");
    send_dfs_test_message(
        &mut attacker_transport,
        &attacker,
        &owner,
        DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
            request_id: "dfs_net_req_forged".to_string(),
            intent_id: intent_id.clone(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            version: version_2.clone(),
            replica_node_id: replica.clone(),
            result: DfsBlobTransferResult::Durable {
                content_hash: forged_hash,
            },
        }),
    );
    let forged_events = owner_dfs
        .process_incoming()
        .expect("owner rejects forged ack");
    assert!(forged_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected { replica_node_id, reason, .. }
                if replica_node_id == &attacker && reason.contains("source mismatch")
        )
    }));
    assert_eq!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("owner metadata before real ack")
            .version,
        version_1
    );
    send_dfs_test_message(
        &mut replica_forge_transport,
        &replica,
        &owner,
        DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
            request_id: "dfs_net_req_rejected_wrong".to_string(),
            intent_id: intent_id.clone(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            version: version_2.clone(),
            replica_node_id: replica.clone(),
            result: DfsBlobTransferResult::Rejected {
                reason: "temporary replica failure".to_string(),
            },
        }),
    );
    let stale_reject_events = owner_dfs
        .process_incoming()
        .expect("stale rejected stage response is isolated");
    assert!(stale_reject_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected { replica_node_id, reason, .. }
                if replica_node_id == &replica && reason.contains("stage response request id")
        )
    }));
    assert_eq!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("owner metadata after stale rejection")
            .version,
        version_1
    );
    let wrong_request_hash = content_hash(b"networked bytes");
    send_dfs_test_message(
        &mut replica_forge_transport,
        &replica,
        &owner,
        DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
            request_id: "dfs_net_req_wrong".to_string(),
            intent_id: intent_id.clone(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            version: version_2.clone(),
            replica_node_id: replica.clone(),
            result: DfsBlobTransferResult::Durable {
                content_hash: wrong_request_hash,
            },
        }),
    );
    let wrong_request_events = owner_dfs
        .process_incoming()
        .expect("wrong stage request id is isolated");
    assert!(
        wrong_request_events.iter().any(|event| matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected { replica_node_id, reason, .. }
                if replica_node_id == &replica && reason.contains("stage response request id")
        )),
        "{wrong_request_events:?}"
    );
    assert_eq!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("owner metadata before real ack")
            .version,
        version_1
    );

    let replica_events = replica_dfs.process_incoming().expect("replica stage");
    assert_eq!(replica_events.len(), 1);
    assert!(
        replica_dfs
            .content()
            .read_manifest(object_id.as_str(), version_2.as_str())
            .is_ok()
    );

    let owner_events = owner_dfs.process_incoming().expect("owner commit");
    assert!(owner_events
        .iter()
        .any(|event| matches!(event, sinomo_dfs::NetworkedDfsServiceEvent::BlobStaged { intent_id: id, .. } if id == &intent_id)));
    assert_eq!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("owner metadata")
            .version,
        version_2
    );

    let replica_commit_events = replica_dfs.process_incoming().expect("replica commit");
    assert!(replica_commit_events.iter().any(|event| {
        matches!(
            event,
            sinomo_dfs::NetworkedDfsServiceEvent::VersionCommitted { intent_id: id, .. }
                if id == &intent_id
        )
    }));
    let read = replica_dfs
        .read_local_object(&object_id)
        .expect("read")
        .expect("data");
    assert_eq!(read.bytes, b"networked bytes");
    assert_eq!(read.version, version_2);

    cleanup(&root);
}

#[test]
fn networked_dfs_supports_three_nodes_two_replicas_with_cross_reads() {
    let node_a = NodeId::from_legacy_named("node_three_two_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_three_two_b").expect("node");
    let node_c = NodeId::from_legacy_named("node_three_two_c").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_a = DfsObjectId::from_legacy_named("obj_three_two_a").expect("object");
    let object_b = DfsObjectId::from_legacy_named("obj_three_two_b").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_three_nodes_two_replicas_cross_reads");
    let network = InProcessNetwork::new();
    let mut dfs_a = NetworkedDfsService::new(
        network.add_node(node_a.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("node-a")).expect("node a store"),
    );
    let mut dfs_b = NetworkedDfsService::new(
        network.add_node(node_b.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("node-b")).expect("node b store"),
    );
    let mut dfs_c = NetworkedDfsService::new(
        network.add_node(node_c.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("node-c")).expect("node c store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: node_a.clone(),
        subtree_policies: Vec::new(),
    };
    for dfs in [&mut dfs_a, &mut dfs_b, &mut dfs_c] {
        dfs.add_file_system(file_system.clone())
            .expect("filesystem metadata");
    }

    let metadata_a = DfsObjectMetadata {
        object_id: object_a.clone(),
        file_system_id: file_system_id.clone(),
        path: "owned-by-a.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: node_a.clone(),
        current_locations: vec![node_a.clone()],
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    let metadata_b = DfsObjectMetadata {
        object_id: object_b.clone(),
        file_system_id: file_system_id.clone(),
        path: "owned-by-b.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: node_b.clone(),
        current_locations: vec![node_b.clone()],
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    for dfs in [&mut dfs_a, &mut dfs_b, &mut dfs_c] {
        dfs.add_object_metadata(metadata_a.clone())
            .expect("object a metadata");
        dfs.add_object_metadata(metadata_b.clone())
            .expect("object b metadata");
    }

    let bytes_a = b"node a replicated bytes".to_vec();
    let bytes_b = b"node b replicated bytes".to_vec();
    let write_a = dfs_a
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_a.clone(),
                target_version: version_2.clone(),
                requesting_node_id: node_a.clone(),
            },
            bytes_a.clone(),
            vec![node_a.clone(), node_c.clone()],
            DfsReplicationPolicy::new(2, 2).expect("two replicas"),
        )
        .expect("node a write");
    let write_b = dfs_b
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_b.clone(),
                target_version: version_2.clone(),
                requesting_node_id: node_b.clone(),
            },
            bytes_b.clone(),
            vec![node_b.clone(), node_c.clone()],
            DfsReplicationPolicy::new(2, 2).expect("two replicas"),
        )
        .expect("node b write");
    let intent_a = match write_a {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("node a write should wait for node c, got {other:?}"),
    };
    let intent_b = match write_b {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("node b write should wait for node c, got {other:?}"),
    };

    let stage_events = dfs_c
        .process_incoming()
        .expect("shared replica stages both writes");
    assert!(stage_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStaged {
                intent_id,
                replica_node_id,
                ..
            } if intent_id == &intent_a && replica_node_id == &node_c
        )
    }));
    assert!(stage_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStaged {
                intent_id,
                replica_node_id,
                ..
            } if intent_id == &intent_b && replica_node_id == &node_c
        )
    }));

    let events_a = dfs_a.process_incoming().expect("node a commits");
    assert!(events_a.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStaged { intent_id, .. } if intent_id == &intent_a
        )
    }));
    let events_b = dfs_b.process_incoming().expect("node b commits");
    assert!(events_b.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStaged { intent_id, .. } if intent_id == &intent_b
        )
    }));
    let commit_events = dfs_c
        .process_incoming()
        .expect("shared replica applies commits");
    assert!(commit_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::VersionCommitted { intent_id, .. } if intent_id == &intent_a
        )
    }));
    assert!(commit_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::VersionCommitted { intent_id, .. } if intent_id == &intent_b
        )
    }));

    assert_eq!(
        dfs_c
            .read_local_object(&object_a)
            .expect("node c reads object a")
            .expect("object a data")
            .bytes,
        bytes_a
    );
    assert_eq!(
        dfs_c
            .read_local_object(&object_b)
            .expect("node c reads object b")
            .expect("object b data")
            .bytes,
        bytes_b
    );

    dfs_a
        .add_object_metadata(
            dfs_b
                .metadata()
                .object(&object_b)
                .expect("node b committed metadata")
                .clone(),
        )
        .expect("replicate object b metadata to node a");
    dfs_b
        .add_object_metadata(
            dfs_a
                .metadata()
                .object(&object_a)
                .expect("node a committed metadata")
                .clone(),
        )
        .expect("replicate object a metadata to node b");

    let object_b_read_sources = dfs_a
        .request_remote_object_read(&object_b)
        .expect("node a requests object b");
    assert_eq!(object_b_read_sources.len(), 1);
    assert!(
        [node_b.clone(), node_c.clone()].contains(&object_b_read_sources[0]),
        "object b should be read from one of its two replicas"
    );
    let object_a_read_sources = dfs_b
        .request_remote_object_read(&object_a)
        .expect("node b requests object a");
    assert_eq!(object_a_read_sources.len(), 1);
    assert!(
        [node_a.clone(), node_c.clone()].contains(&object_a_read_sources[0]),
        "object a should be read from one of its two replicas"
    );
    for _ in 0..3 {
        dfs_a.process_incoming().expect("node a processes reads");
        dfs_b.process_incoming().expect("node b processes reads");
        dfs_c.process_incoming().expect("node c processes reads");
    }

    assert_eq!(
        dfs_a
            .read_local_object(&object_b)
            .expect("node a local read of object b")
            .expect("object b data")
            .bytes,
        bytes_b
    );
    assert_eq!(
        dfs_b
            .read_local_object(&object_a)
            .expect("node b local read of object a")
            .expect("object a data")
            .bytes,
        bytes_a
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_retries_stage_after_replica_rejection() {
    let owner = NodeId::from_legacy_named("node_stage_retry_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stage_retry_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_retry").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_stage_retry");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let mut replica_control = network.add_node(replica.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "stage-retry.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica filesystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let bytes = b"retry replicated bytes".to_vec();
    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes.clone(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };

    assert_eq!(
        replica_control
            .drain_incoming()
            .expect("drain original stage")
            .len(),
        1,
        "the original stage is withheld so the rejected response drives the retry path"
    );
    send_dfs_test_message(
        &mut replica_control,
        &replica,
        &owner,
        DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
            request_id: "dfs_net_req_1".to_string(),
            intent_id: intent_id.clone(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            version: version_2.clone(),
            replica_node_id: replica.clone(),
            result: DfsBlobTransferResult::Rejected {
                reason: "temporary stage failure".to_string(),
            },
        }),
    );
    let rejected_events = owner_dfs
        .process_incoming()
        .expect("owner records rejected stage");
    assert!(rejected_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected { replica_node_id, reason, .. }
                if replica_node_id == &replica && reason == "temporary stage failure"
        )
    }));

    let repair_jobs = owner_dfs
        .retry_pending_repairs()
        .expect("stage retry is scheduled before repair jobs");
    assert!(
        repair_jobs.is_empty(),
        "pre-commit stage retries must not create repair jobs"
    );
    let replica_events = replica_dfs
        .process_incoming()
        .expect("replica accepts retried stage");
    assert!(replica_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStaged { intent_id: event_intent, replica_node_id, .. }
                if event_intent == &intent_id && replica_node_id == &replica
        )
    }));
    let owner_events = owner_dfs
        .process_incoming()
        .expect("owner commits after retried stage");
    assert!(owner_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStaged { intent_id: event_intent, replica_node_id, .. }
                if event_intent == &intent_id && replica_node_id == &replica
        )
    }));
    assert_eq!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("owner committed object")
            .version,
        version_2
    );
    let replica_commit_events = replica_dfs
        .process_incoming()
        .expect("replica applies commit after retried stage");
    assert!(replica_commit_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::VersionCommitted { intent_id: event_intent, .. }
                if event_intent == &intent_id
        )
    }));
    let replica_read = replica_dfs
        .read_local_object(&object_id)
        .expect("replica stores retried stage bytes")
        .expect("replica data");
    assert_eq!(replica_read.bytes, bytes);

    cleanup(&root);
}

#[test]
fn networked_dfs_does_not_duplicate_inflight_stage_on_first_maintenance_tick() {
    let owner = NodeId::from_legacy_named("node_stage_inflight_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stage_inflight_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_inflight").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_stage_inflight_no_duplicate");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let _replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let mut replica_control = network.add_node(replica.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "stage-inflight.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system)
        .expect("owner filesystem");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id,
                target_version: version_2,
                requesting_node_id: owner,
            },
            b"inflight stage bytes".to_vec(),
            vec![owner_dfs.node_id().clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("write");
    assert!(matches!(write, NetworkedDfsWriteOutcome::Pending { .. }));

    assert!(
        owner_dfs
            .retry_pending_repairs()
            .expect("first maintenance pass")
            .is_empty()
    );
    assert_eq!(
        replica_control
            .drain_incoming()
            .expect("drain replica mailbox")
            .len(),
        1,
        "the original in-flight stage should not be resent on the first maintenance tick"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_retries_lost_inflight_stage_after_retry_delay() {
    let owner = NodeId::from_legacy_named("node_stage_lost_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stage_lost_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_lost").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_stage_lost_retry");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let mut replica_control = network.add_node(replica.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "stage-lost.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica filesystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let bytes = chunked_test_bytes(4_464);
    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes.clone(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    assert_eq!(
        replica_control
            .drain_incoming()
            .expect("drop original stage")
            .len(),
        2
    );

    owner_dfs
        .retry_pending_repairs()
        .expect("first maintenance pass");
    assert!(
        replica_control
            .drain_incoming()
            .expect("no immediate retry")
            .is_empty(),
        "first maintenance pass should not duplicate a still-fresh in-flight stage"
    );
    let mut staged_after_retry = false;
    for _ in 0..4 {
        owner_dfs
            .retry_pending_repairs()
            .expect("maintenance eventually resends lost stage");
        let replica_events = replica_dfs
            .process_incoming()
            .expect("replica checks for retried lost stage");
        if replica_events.iter().any(|event| {
            matches!(event, NetworkedDfsServiceEvent::BlobStaged { intent_id: event_intent, .. } if event_intent == &intent_id)
        }) {
            staged_after_retry = true;
            break;
        }
    }
    assert!(
        staged_after_retry,
        "lost in-flight stage should be resent after the retry delay"
    );
    owner_dfs
        .process_incoming()
        .expect("owner commits retried lost stage");
    replica_dfs
        .process_incoming()
        .expect("replica applies retried lost stage commit");
    let read = replica_dfs
        .read_local_object(&object_id)
        .expect("replica reads retried lost stage")
        .expect("replica data");
    assert_eq!(read.version, version_2);
    assert_eq!(read.bytes, bytes);

    cleanup(&root);
}

#[test]
fn networked_dfs_fetches_lost_stage_blob_from_durable_replica_before_commit() {
    let owner = NodeId::from_legacy_named("node_stage_fetch_owner").expect("node");
    let replica_a = NodeId::from_legacy_named("node_stage_fetch_replica_a").expect("node");
    let replica_b = NodeId::from_legacy_named("node_stage_fetch_replica_b").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_fetch").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_stage_fetch_before_commit");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut replica_a_dfs = NetworkedDfsService::new(
        network.add_node(replica_a.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica-a")).expect("replica a store"),
    );
    let mut replica_b_dfs = NetworkedDfsService::new(
        network.add_node(replica_b.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica-b")).expect("replica b store"),
    );
    let mut replica_b_control = network.add_node(replica_b.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "stage-fetch.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    for dfs in [&mut owner_dfs, &mut replica_a_dfs, &mut replica_b_dfs] {
        dfs.add_file_system(file_system.clone())
            .expect("filesystem");
        dfs.add_object_metadata(metadata.clone()).expect("object");
    }

    let bytes = chunked_test_bytes(4_464);
    let hash = content_hash(&bytes);
    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes.clone(),
            vec![owner.clone(), replica_a.clone(), replica_b.clone()],
            DfsReplicationPolicy::new(3, 3).expect("policy"),
        )
        .expect("write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    assert!(replica_a_dfs
        .process_incoming()
        .expect("replica a stages initial write")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobStaged { intent_id: event_intent, .. } if event_intent == &intent_id)));
    assert!(
        !replica_b_control
            .drain_incoming()
            .expect("drop initial replica b stage")
            .is_empty()
    );
    owner_dfs
        .process_incoming()
        .expect("owner records replica a receipt");
    let owner_blob = blob_path_for_hash(&root.join("owner"), &hash);
    fs::remove_file(&owner_blob).expect("remove owner blob to force alternate fetch");

    owner_dfs
        .retry_pending_repairs()
        .expect("first maintenance pass leaves fresh stage in flight");
    owner_dfs
        .retry_pending_repairs()
        .expect("second maintenance pass requests fetch from durable replica");
    assert!(replica_a_dfs
        .process_incoming()
        .expect("replica a serves pre-commit fetch")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobFetched { intent_id: event_intent, .. } if event_intent == &intent_id)));
    assert!(owner_dfs
        .process_incoming()
        .expect("owner stores fetched pre-commit blob")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobFetched { intent_id: event_intent, .. } if event_intent == &intent_id)));

    owner_dfs
        .retry_pending_repairs()
        .expect("stage retry after pre-commit fetch");
    assert!(replica_b_dfs
        .process_incoming()
        .expect("replica b stages fetched retry")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobStaged { intent_id: event_intent, .. } if event_intent == &intent_id)));
    owner_dfs
        .process_incoming()
        .expect("owner commits after fetched retry");
    replica_b_dfs
        .process_incoming()
        .expect("replica b applies commit");
    let read = replica_b_dfs
        .read_local_object(&object_id)
        .expect("replica b read")
        .expect("replica b data");
    assert_eq!(read.version, version_2);
    assert_eq!(read.bytes, bytes);

    cleanup(&root);
}

#[test]
fn networked_dfs_stage_fetch_rotates_after_durable_source_misses_blob() {
    let owner = NodeId::from_legacy_named("node_stage_rotate_owner").expect("node");
    let mut stage_sources = vec![
        NodeId::from_legacy_named("node_stage_rotate_a_missing").expect("node"),
        NodeId::from_legacy_named("node_stage_rotate_b_found").expect("node"),
    ];
    stage_sources.sort();
    let replica_missing = stage_sources[0].clone();
    let replica_found = stage_sources[1].clone();
    let replica_target = NodeId::from_legacy_named("node_stage_rotate_c_target").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_rotate").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_stage_fetch_rotates_after_miss");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut missing_dfs = NetworkedDfsService::new(
        network.add_node(replica_missing.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("missing")).expect("missing store"),
    );
    let mut found_dfs = NetworkedDfsService::new(
        network.add_node(replica_found.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("found")).expect("found store"),
    );
    let mut target_dfs = NetworkedDfsService::new(
        network.add_node(replica_target.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("target")).expect("target store"),
    );
    let mut target_control = network.add_node(replica_target.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "stage-rotate.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    for dfs in [
        &mut owner_dfs,
        &mut missing_dfs,
        &mut found_dfs,
        &mut target_dfs,
    ] {
        dfs.add_file_system(file_system.clone())
            .expect("filesystem");
        dfs.add_object_metadata(metadata.clone()).expect("object");
    }

    let bytes = chunked_test_bytes(4_464);
    let hash = content_hash(&bytes);
    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes.clone(),
            vec![
                owner.clone(),
                replica_missing.clone(),
                replica_found.clone(),
                replica_target.clone(),
            ],
            DfsReplicationPolicy::new(4, 4).expect("policy"),
        )
        .expect("write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    missing_dfs
        .process_incoming()
        .expect("missing source stages initial write");
    found_dfs
        .process_incoming()
        .expect("found source stages initial write");
    assert!(
        !target_control
            .drain_incoming()
            .expect("drop target initial stage")
            .is_empty()
    );
    owner_dfs
        .process_incoming()
        .expect("owner records durable receipts");
    fs::remove_file(blob_path_for_hash(&root.join("owner"), &hash)).expect("remove owner blob");
    fs::remove_file(blob_path_for_hash(&root.join("missing"), &hash))
        .expect("remove first durable source blob");

    owner_dfs
        .retry_pending_repairs()
        .expect("first maintenance pass leaves fresh target stage in flight");
    owner_dfs
        .retry_pending_repairs()
        .expect("request fetch from first durable source");
    assert!(missing_dfs
        .process_incoming()
        .expect("first durable source misses blob")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobFetchRejected { intent_id: event_intent, .. } if event_intent == &intent_id)));
    owner_dfs
        .process_incoming()
        .expect("owner records first source miss");

    owner_dfs
        .retry_pending_repairs()
        .expect("request fetch from second durable source");
    assert!(found_dfs
        .process_incoming()
        .expect("second durable source serves blob")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobFetched { intent_id: event_intent, .. } if event_intent == &intent_id)));
    owner_dfs
        .process_incoming()
        .expect("owner stores second source blob");
    owner_dfs
        .retry_pending_repairs()
        .expect("stage target after rotated fetch");
    target_dfs
        .process_incoming()
        .expect("target stages after rotated fetch");
    owner_dfs
        .process_incoming()
        .expect("owner commits after rotated fetch");
    target_dfs
        .process_incoming()
        .expect("target applies commit");
    let read = target_dfs
        .read_local_object(&object_id)
        .expect("target read")
        .expect("target data");
    assert_eq!(read.version, version_2);
    assert_eq!(read.bytes, bytes);

    cleanup(&root);
}

#[test]
fn networked_dfs_transfers_large_writes_in_chunks() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_large").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_large_chunked_write");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "large.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica filesystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let bytes = chunked_test_bytes(4_464);
    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner,
            },
            bytes.clone(),
            vec![owner_dfs.node_id().clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("large write");
    assert!(matches!(write, NetworkedDfsWriteOutcome::Pending { .. }));

    let replica_events = replica_dfs.process_incoming().expect("replica chunks");
    assert!(replica_events.iter().any(|event| {
        matches!(event, NetworkedDfsServiceEvent::BlobStaged { object_id: staged, .. } if staged == &object_id)
    }));
    owner_dfs.process_incoming().expect("owner ack");
    replica_dfs.process_incoming().expect("replica commit");

    let read = replica_dfs
        .read_local_object(&object_id)
        .expect("read replica")
        .expect("replica object");
    assert_eq!(read.version, version_2);
    assert_eq!(read.bytes, bytes);

    cleanup(&root);
}

#[test]
fn networked_dfs_streams_large_writes_from_reader() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_large_reader").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_large_reader_write");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "large-reader.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica filesystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let byte_count = TEST_DFS_CHUNK_BYTES + 4_464;
    let write = owner_dfs
        .write_distributed_eventual_from_reader(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner,
            },
            PatternReader::new(byte_count),
            vec![owner_dfs.node_id().clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("large reader write");
    assert!(matches!(write, NetworkedDfsWriteOutcome::Pending { .. }));

    let replica_events = replica_dfs.process_incoming().expect("replica chunks");
    assert!(replica_events.iter().any(|event| {
        matches!(event, NetworkedDfsServiceEvent::BlobStaged { object_id: staged, .. } if staged == &object_id)
    }));
    owner_dfs.process_incoming().expect("owner ack");
    replica_dfs.process_incoming().expect("replica commit");

    let read = replica_dfs
        .read_local_object(&object_id)
        .expect("read replica")
        .expect("replica object");
    assert_eq!(read.version, version_2);
    assert_eq!(read.bytes, pattern_bytes(byte_count));

    cleanup(&root);
}

#[test]
fn networked_dfs_reader_write_resumes_stage_windows() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_large_reader_window").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_large_reader_write_window");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "large-reader-window.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica filesystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let byte_count = 257 * 64 * 1024 + 17;
    let write = owner_dfs
        .write_distributed_eventual_from_reader(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner,
            },
            PatternReader::new(byte_count),
            vec![owner_dfs.node_id().clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("windowed reader write");
    assert!(matches!(write, NetworkedDfsWriteOutcome::Pending { .. }));

    let first_replica_events = replica_dfs.process_incoming().expect("first stage window");
    assert!(
        !first_replica_events.iter().any(|event| {
            matches!(event, NetworkedDfsServiceEvent::BlobStaged { object_id: staged, .. } if staged == &object_id)
        }),
        "the first stage window should not complete a transfer larger than the window"
    );

    owner_dfs
        .retry_pending_repairs()
        .expect("send next stage window");
    let second_replica_events = replica_dfs.process_incoming().expect("second stage window");
    assert!(second_replica_events.iter().any(|event| {
        matches!(event, NetworkedDfsServiceEvent::BlobStaged { object_id: staged, .. } if staged == &object_id)
    }));
    owner_dfs.process_incoming().expect("owner ack");
    replica_dfs.process_incoming().expect("replica commit");

    let read = replica_dfs
        .read_local_object(&object_id)
        .expect("read replica")
        .expect("replica object");
    assert_eq!(read.version, version_2);
    assert_eq!(read.bytes, pattern_bytes(byte_count));

    cleanup(&root);
}

#[test]
fn networked_dfs_large_stage_chunks_are_self_describing() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_large_stage_metadata").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_large_stage_metadata");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut replica_transport = network.add_node(replica.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    owner_dfs
        .add_file_system(file_system)
        .expect("owner filesystem");
    owner_dfs
        .add_object_metadata(DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: "large-stage-metadata.bin".to_string(),
            kind: DfsObjectKind::Artifact,
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
            version: version_1,
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        })
        .expect("owner object");

    let bytes = chunked_test_bytes(4_464);
    owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            bytes,
            vec![owner, replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("large write");

    let frames = replica_transport
        .drain_incoming()
        .expect("stage chunks sent to replica");
    let chunks = frames
        .iter()
        .map(|frame| {
            let MeshMessageBody::Protocol(protocol) = &frame.frame.message.body else {
                panic!("expected protocol frame");
            };
            protocol.decode::<DfsNetworkMessage>().expect("decode DFS")
        })
        .filter_map(|message| match message {
            DfsNetworkMessage::StageBlobChunk(chunk) => Some(chunk),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        chunks.len() > 1,
        "large write should be split into multiple stage chunks"
    );
    assert!(
        chunks.iter().all(|chunk| chunk.object_metadata.is_some()),
        "every stage chunk must carry metadata so out-of-order delivery can be authorized"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_recovers_committed_repair_jobs_from_metadata_backend() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_recovered").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_recovered_repairs");
    let network = InProcessNetwork::new();
    let owner_meta = root.join("owner-meta");
    let owner_content = root.join("owner-content");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };

    {
        let owner_backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &owner_meta)
                .expect("owner metadata");
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            sinomo_dfs::LocalContentStore::new(&owner_content).expect("owner content"),
        )
        .with_metadata_backend(owner_backend)
        .expect("owner metadata backend");
        owner_dfs
            .add_file_system(file_system.clone())
            .expect("owner filesystem");
        owner_dfs
            .add_object_metadata(metadata.clone())
            .expect("owner object");

        let write = owner_dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    target_version: version_2.clone(),
                    requesting_node_id: owner.clone(),
                },
                b"recovered repair bytes".to_vec(),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(1, 2).expect("policy"),
            )
            .expect("write");
        assert!(matches!(
            write,
            NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } if repair_jobs.len() == 1
        ));
    }

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &owner_meta)
            .expect("recovered metadata");
    let mut recovered_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(&owner_content).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("recovered metadata backend");
    assert_eq!(
        recovered_owner
            .metadata()
            .object(&object_id)
            .expect("recovered object")
            .version,
        version_2
    );

    let replica_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("replica"),
        root.join("replica-meta"),
    )
    .expect("replica metadata");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica filesystem");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    assert_eq!(
        recovered_owner
            .retry_pending_repairs()
            .expect("retry recovered repair")
            .len(),
        1
    );
    let events = replica_dfs.process_incoming().expect("replica repair");
    assert!(events.iter().any(|event| {
        matches!(event, NetworkedDfsServiceEvent::ReplicaRepaired { object_id: repaired, .. } if repaired == &object_id)
    }));
    let read = replica_dfs
        .read_local_object(&object_id)
        .expect("read replica")
        .expect("replica content");
    assert_eq!(read.bytes, b"recovered repair bytes");

    cleanup(&root);
}

#[test]
fn networked_dfs_fetches_repair_blob_from_alternate_replica_after_restart() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let missing = NodeId::from_legacy_named("node_missing").expect("node");
    let attacker = NodeId::from_legacy_named("node_attacker").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_alternate_repair").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_alternate_repair_fetch");
    let network = InProcessNetwork::new();
    let owner_meta = root.join("owner-meta");
    let owner_content = root.join("owner-content");
    let replica_content = root.join("replica-content");
    let alternate_repair_bytes = (0..180_000)
        .map(|index| (index % 239) as u8)
        .collect::<Vec<_>>();
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };

    {
        let owner_backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &owner_meta)
                .expect("owner metadata");
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            sinomo_dfs::LocalContentStore::new(&owner_content).expect("owner content"),
        )
        .with_metadata_backend(owner_backend)
        .expect("owner metadata backend");
        let mut replica_dfs = NetworkedDfsService::new(
            network.add_node(replica.clone()),
            sinomo_dfs::LocalContentStore::new(&replica_content).expect("replica content"),
        );
        owner_dfs
            .add_file_system(file_system.clone())
            .expect("owner filesystem");
        replica_dfs
            .add_file_system(file_system.clone())
            .expect("replica filesystem");
        owner_dfs
            .add_object_metadata(metadata.clone())
            .expect("owner object");
        replica_dfs
            .add_object_metadata(metadata.clone())
            .expect("replica object");

        let write = owner_dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    target_version: version_2.clone(),
                    requesting_node_id: owner.clone(),
                },
                alternate_repair_bytes.clone(),
                vec![owner.clone(), replica.clone(), missing.clone()],
                DfsReplicationPolicy::new(2, 3).expect("policy"),
            )
            .expect("write");
        assert!(matches!(
            write,
            NetworkedDfsWriteOutcome::Pending {
                durable_receipts: 1,
                required_receipts: 2,
                ..
            }
        ));
        replica_dfs.process_incoming().expect("replica stage");
        let owner_events = owner_dfs.process_incoming().expect("owner commit");
        assert!(owner_events.iter().any(|event| {
            matches!(event, NetworkedDfsServiceEvent::BlobStaged { object_id: staged, .. } if staged == &object_id)
        }));
        replica_dfs.process_incoming().expect("replica commit");
        assert_eq!(
            replica_dfs
                .read_local_object(&object_id)
                .expect("replica read")
                .expect("replica content")
                .bytes,
            alternate_repair_bytes
        );
    }

    fs::remove_dir_all(&owner_content).expect("remove owner content");
    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &owner_meta)
            .expect("recovered metadata");
    let mut recovered_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(&owner_content).expect("recovered owner content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("recovered owner metadata");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(&replica_content).expect("replica content"),
    );
    replica_dfs
        .add_file_system(file_system.clone())
        .expect("replica filesystem");
    let mut replica_metadata = metadata.clone();
    replica_metadata.version = version_2.clone();
    replica_metadata.content_hash = recovered_owner
        .metadata()
        .object(&object_id)
        .expect("recovered object")
        .content_hash
        .clone();
    replica_metadata.current_locations = vec![owner.clone(), replica.clone()];
    replica_dfs
        .add_object_metadata(replica_metadata)
        .expect("replica visible object");
    let mut missing_dfs = NetworkedDfsService::new(
        network.add_node(missing.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("missing-content")).expect("missing content"),
    );
    missing_dfs
        .add_file_system(file_system)
        .expect("missing filesystem");
    missing_dfs
        .add_object_metadata(metadata)
        .expect("missing object");

    assert_eq!(
        recovered_owner
            .retry_pending_repairs()
            .expect("request alternate fetch")
            .len(),
        0
    );
    let recovered_intent_id = recovered_owner
        .metadata_backend()
        .expect("recovered metadata backend")
        .intents_for_object(&object_id)
        .expect("recovered intents")
        .into_iter()
        .find(|intent| intent.target_version == version_2)
        .expect("recovered pending repair intent")
        .intent_id;
    let mut attacker_transport = network.add_node(attacker.clone());
    let mut replica_forge_transport = network.add_node(replica.clone());
    let expected_hash = recovered_owner
        .metadata()
        .object(&object_id)
        .expect("recovered object")
        .content_hash
        .clone()
        .expect("content hash");
    send_dfs_test_message(
        &mut replica_forge_transport,
        &replica,
        &owner,
        DfsNetworkMessage::FetchBlobResponse(DfsFetchBlobResponse {
            request_id: "dfs_net_req_wrong".to_string(),
            intent_id: recovered_intent_id.clone(),
            object_id: object_id.clone(),
            version: version_2.clone(),
            source_node_id: replica.clone(),
            result: DfsFetchBlobResult::Found {
                content_hash: expected_hash.clone(),
                bytes: alternate_repair_bytes.clone(),
            },
        }),
    );
    assert!(
        recovered_owner
            .process_incoming()
            .expect("reject wrong fetch request id")
            .iter()
            .any(|event| matches!(
                event,
                NetworkedDfsServiceEvent::BlobFetchRejected { source_node_id, reason, .. }
                    if source_node_id == &replica && reason.contains("request id")
            ))
    );
    send_dfs_test_message(
        &mut attacker_transport,
        &attacker,
        &owner,
        DfsNetworkMessage::FetchBlobResponse(DfsFetchBlobResponse {
            request_id: "dfs_net_req_forged".to_string(),
            intent_id: recovered_intent_id.clone(),
            object_id: object_id.clone(),
            version: version_2.clone(),
            source_node_id: replica.clone(),
            result: DfsFetchBlobResult::Found {
                content_hash: expected_hash,
                bytes: alternate_repair_bytes.clone(),
            },
        }),
    );
    assert!(
        recovered_owner
            .process_incoming()
            .expect("reject forged fetch response")
            .iter()
            .any(|event| matches!(
                event,
                NetworkedDfsServiceEvent::BlobFetchRejected { source_node_id, reason, .. }
                    if source_node_id == &attacker && reason.contains("source mismatch")
            ))
    );
    let expected_hash = recovered_owner
        .metadata()
        .object(&object_id)
        .expect("recovered object")
        .content_hash
        .clone()
        .expect("content hash");
    send_dfs_test_message(
        &mut attacker_transport,
        &attacker,
        &owner,
        DfsNetworkMessage::FetchBlobResponseChunk(DfsFetchBlobResponseChunk {
            request_id: "dfs_net_req_forged_chunk".to_string(),
            intent_id: recovered_intent_id.clone(),
            object_id: object_id.clone(),
            version: version_2.clone(),
            source_node_id: replica.clone(),
            content_hash: expected_hash,
            total_size_bytes: alternate_repair_bytes.len() as u64,
            chunk_index: 0,
            total_chunks: 1,
            chunk_hash: None,
            bytes: alternate_repair_bytes.clone(),
        }),
    );
    assert!(
        recovered_owner
            .process_incoming()
            .expect("reject forged fetch chunk")
            .iter()
            .any(|event| matches!(
                event,
                NetworkedDfsServiceEvent::BlobFetchRejected { source_node_id, reason, .. }
                    if source_node_id == &attacker && reason.contains("source mismatch")
            ))
    );
    assert!(replica_dfs
        .process_incoming()
        .expect("serve fetch")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobFetched { object_id: fetched, .. } if fetched == &object_id)));
    assert!(recovered_owner
        .process_incoming()
        .expect("store fetched blob")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobFetched { object_id: fetched, source_node_id, .. } if fetched == &object_id && source_node_id == &replica)));
    assert_eq!(
        recovered_owner
            .retry_pending_repairs()
            .expect("repair missing replica")
            .len(),
        1
    );
    let mut missing_forge_transport = network.add_node(missing.clone());
    let repaired_hash = recovered_owner
        .metadata()
        .object(&object_id)
        .expect("recovered object")
        .content_hash
        .clone()
        .expect("content hash");
    send_dfs_test_message(
        &mut missing_forge_transport,
        &missing,
        &owner,
        DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
            request_id: "dfs_net_req_rejected_wrong_repair".to_string(),
            intent_id: recovered_intent_id.clone(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            version: version_2.clone(),
            replica_node_id: missing.clone(),
            result: DfsBlobTransferResult::Rejected {
                reason: "temporary repair failure".to_string(),
            },
        }),
    );
    assert!(
        recovered_owner
            .process_incoming()
            .expect("stale rejected repair response is isolated")
            .iter()
            .any(|event| matches!(
                event,
                NetworkedDfsServiceEvent::RepairRejected { replica_node_id, reason, .. }
                    if replica_node_id == &missing && reason.contains("repair response request id")
            ))
    );
    send_dfs_test_message(
        &mut missing_forge_transport,
        &missing,
        &owner,
        DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
            request_id: "dfs_net_req_wrong_repair".to_string(),
            intent_id: recovered_intent_id.clone(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            version: version_2.clone(),
            replica_node_id: missing.clone(),
            result: DfsBlobTransferResult::Committed {
                content_hash: repaired_hash.clone(),
            },
        }),
    );
    let wrong_request_events = recovered_owner
        .process_incoming()
        .expect("wrong repair request id is isolated");
    assert!(
        wrong_request_events.iter().any(|event| matches!(
            event,
            NetworkedDfsServiceEvent::RepairRejected { replica_node_id, reason, .. }
                if replica_node_id == &missing && reason.contains("repair response request id")
        )),
        "{wrong_request_events:?}"
    );
    send_dfs_test_message(
        &mut attacker_transport,
        &attacker,
        &owner,
        DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
            request_id: "dfs_net_req_forged_repair".to_string(),
            intent_id: recovered_intent_id,
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            version: version_2.clone(),
            replica_node_id: missing.clone(),
            result: DfsBlobTransferResult::Committed {
                content_hash: repaired_hash,
            },
        }),
    );
    assert!(
        recovered_owner
            .process_incoming()
            .expect("reject forged repair response")
            .iter()
            .any(|event| matches!(
                event,
                NetworkedDfsServiceEvent::RepairRejected { replica_node_id, reason, .. }
                    if replica_node_id == &attacker && reason.contains("source mismatch")
            ))
    );
    assert!(missing_dfs
        .process_incoming()
        .expect("repair missing")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::ReplicaRepaired { object_id: repaired, .. } if repaired == &object_id)));
    let read = missing_dfs
        .read_local_object(&object_id)
        .expect("missing read")
        .expect("missing content");
    assert_eq!(read.version, version_2);
    assert_eq!(read.bytes, alternate_repair_bytes);

    cleanup(&root);
}

#[test]
fn networked_dfs_repair_fetch_rotates_after_durable_source_misses_blob() {
    let owner = NodeId::from_legacy_named("node_repair_rotate_owner").expect("node");
    let mut repair_sources = vec![
        NodeId::from_legacy_named("node_repair_rotate_a_missing").expect("node"),
        NodeId::from_legacy_named("node_repair_rotate_b_found").expect("node"),
    ];
    repair_sources.sort();
    let replica_missing = repair_sources[0].clone();
    let replica_found = repair_sources[1].clone();
    let repair_target = NodeId::from_legacy_named("node_repair_rotate_c_target").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_rotate").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_repair_fetch_rotates_after_miss");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut missing_dfs = NetworkedDfsService::new(
        network.add_node(replica_missing.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("missing")).expect("missing store"),
    );
    let mut found_dfs = NetworkedDfsService::new(
        network.add_node(replica_found.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("found")).expect("found store"),
    );
    let mut target_dfs = NetworkedDfsService::new(
        network.add_node(repair_target.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("target")).expect("target store"),
    );
    let mut target_control = network.add_node(repair_target.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "repair-rotate.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    for dfs in [
        &mut owner_dfs,
        &mut missing_dfs,
        &mut found_dfs,
        &mut target_dfs,
    ] {
        dfs.add_file_system(file_system.clone())
            .expect("filesystem");
        dfs.add_object_metadata(metadata.clone()).expect("object");
    }

    let bytes = chunked_test_bytes(4_464);
    let hash = content_hash(&bytes);
    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes.clone(),
            vec![
                owner.clone(),
                replica_missing.clone(),
                replica_found.clone(),
                repair_target.clone(),
            ],
            DfsReplicationPolicy::new(3, 4).expect("policy"),
        )
        .expect("write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    missing_dfs
        .process_incoming()
        .expect("first durable source stages");
    found_dfs
        .process_incoming()
        .expect("second durable source stages");
    assert!(
        !target_control
            .drain_incoming()
            .expect("drop repair target initial stage")
            .is_empty()
    );
    let owner_events = owner_dfs
        .process_incoming()
        .expect("owner commits with repair job");
    assert!(owner_events.iter().any(|event| {
        matches!(event, NetworkedDfsServiceEvent::BlobStaged { intent_id: event_intent, .. } if event_intent == &intent_id)
    }));
    target_control
        .drain_incoming()
        .expect("drop repair target commit before repair");
    fs::remove_file(blob_path_for_hash(&root.join("owner"), &hash)).expect("remove owner blob");
    fs::remove_file(blob_path_for_hash(&root.join("missing"), &hash))
        .expect("remove first durable source blob");

    assert!(
        owner_dfs
            .retry_pending_repairs()
            .expect("request fetch from first durable source")
            .is_empty()
    );
    assert!(missing_dfs
        .process_incoming()
        .expect("first durable source misses repair blob")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobFetchRejected { intent_id: event_intent, .. } if event_intent == &intent_id)));
    owner_dfs
        .process_incoming()
        .expect("owner records first repair source miss");

    assert!(
        owner_dfs
            .retry_pending_repairs()
            .expect("request fetch from second durable source")
            .is_empty()
    );
    assert!(found_dfs
        .process_incoming()
        .expect("second durable source serves repair blob")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobFetched { intent_id: event_intent, .. } if event_intent == &intent_id)));
    owner_dfs
        .process_incoming()
        .expect("owner stores rotated repair blob");
    assert_eq!(
        owner_dfs
            .retry_pending_repairs()
            .expect("repair target after rotated fetch")
            .len(),
        1
    );
    assert!(target_dfs
        .process_incoming()
        .expect("target repairs after rotated fetch")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::ReplicaRepaired { intent_id: event_intent, .. } if event_intent == &intent_id)));
    let read = target_dfs
        .read_local_object(&object_id)
        .expect("target read")
        .expect("target data");
    assert_eq!(read.version, version_2);
    assert_eq!(read.bytes, bytes);

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_stage_blob_from_non_owner() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let attacker = NodeId::from_legacy_named("node_attacker").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_auth").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_rejects_stage_from_non_owner");
    let network = InProcessNetwork::new();
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    let mut attacker_dfs = NetworkedDfsService::new(
        network.add_node(attacker.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("attacker")).expect("attacker content"),
    );
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica content"),
    );
    attacker_dfs
        .add_file_system(file_system.clone())
        .expect("attacker filesystem");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica filesystem");
    attacker_dfs
        .add_object_metadata(metadata.clone())
        .expect("attacker object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let unauthorized_bytes = (0..180_000)
        .map(|index| (index % 233) as u8)
        .collect::<Vec<_>>();
    let write = attacker_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: attacker,
            },
            unauthorized_bytes,
            vec![attacker_dfs.node_id().clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("attacker write");
    assert!(matches!(write, NetworkedDfsWriteOutcome::Pending { .. }));

    let events = replica_dfs.process_incoming().expect("replica stage");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected { reason, .. }
                if reason.contains("unauthorized DFS replica command")
        )
    }));
    let attacker_events = attacker_dfs
        .process_incoming()
        .expect("attacker receives chunked stage rejection");
    assert!(attacker_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected { replica_node_id, reason, .. }
                if replica_node_id == &replica && reason.contains("unauthorized DFS replica command")
        )
    }));
    assert!(
        replica_dfs
            .content()
            .read_manifest(object_id.as_str(), version_2.as_str())
            .is_err()
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_uses_kv_only_for_control_plane_file_system_config() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_control_plane_config");
    let network = InProcessNetwork::new();
    let control_plane = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![owner.clone(), replica.clone()])
            .expect("control-plane kv cluster"),
    );
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    )
    .with_control_plane(control_plane.clone());
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner DfsFileSystem");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica DfsFileSystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"control plane does not store object metadata".to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("write");
    assert!(matches!(write, NetworkedDfsWriteOutcome::Pending { .. }));

    replica_dfs.process_incoming().expect("replica stage");
    owner_dfs.process_incoming().expect("owner commit");

    assert_eq!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("owner metadata")
            .version,
        version_2
    );
    control_plane.with_client(|kv| {
        let control = DfsControlPlaneStore::new(kv);
        assert_eq!(
            control
                .get_file_system_config(&file_system_id)
                .expect("get filesystem config")
                .expect("filesystem config")
                .file_system_id,
            file_system_id
        );
        let leader_store = kv.store(kv.leader_id()).expect("leader store");
        let dfs_keys: Vec<_> = leader_store
            .range_prefix("/dfs/")
            .into_iter()
            .map(|entry| entry.key)
            .collect();
        assert_eq!(dfs_keys, vec![file_system_config_key(&file_system_id)]);
    });

    cleanup(&root);
}

#[test]
fn distributed_sifs_bootstrap_exposes_cluster_root_users_and_services_in_metadata_not_kv() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let root = temp_root("distributed_sifs_bootstrap");
    let network = InProcessNetwork::new();
    let control_plane = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![owner.clone(), replica.clone()])
            .expect("control-plane kv cluster"),
    );
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata backend");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("content")).expect("content store"),
    )
    .with_control_plane(control_plane.clone())
    .with_metadata_backend(backend)
    .expect("metadata backend");

    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .advertise_metadata_service(&DfsNodeAdvertisement::metadata(
                owner.clone(),
                "loopback://metadata",
            ))
            .expect("metadata advertisement");
        control
            .advertise_content_service(&DfsNodeAdvertisement::content(
                owner.clone(),
                "loopback://content",
            ))
            .expect("content advertisement");
        let leader = kv.store(kv.leader_id()).expect("leader store");
        let dfs_keys: Vec<_> = leader
            .range_prefix("/dfs/")
            .into_iter()
            .map(|entry| entry.key)
            .collect();
        assert!(dfs_keys.len() >= 3);
        assert!(dfs_keys.iter().all(|key| {
            key.starts_with("/dfs/filesystems/")
                || key.starts_with("/dfs/services/metadata/")
                || key.starts_with("/dfs/services/content/")
        }));
    });

    assert_eq!(
        dfs.posix_stat("/users").expect("users stat").kind,
        DfsObjectKind::Directory
    );
    assert_eq!(
        dfs.posix_stat("/services").expect("services stat").kind,
        DfsObjectKind::Directory
    );
    assert_eq!(
        dfs.posix_stat("/sinomo").expect("sinomo stat").kind,
        DfsObjectKind::Directory
    );
    assert_eq!(
        dfs.posix_stat("/sinomo/nodes").expect("nodes stat").kind,
        DfsObjectKind::Directory
    );
    let node_files = dfs
        .posix_stat(&sifs_node_files_path(&owner))
        .expect("node files stat");
    assert_eq!(node_files.kind, DfsObjectKind::Directory);
    assert_eq!(node_files.policy, DfsObjectPolicy::LocalOwned);

    cleanup(&root);
}

#[test]
fn networked_dfs_applies_metadata_journal_batches_from_peer() {
    let owner = NodeId::from_legacy_named("node_metadata_journal_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_metadata_journal_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_metadata_journal").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_metadata_journal_batches");
    let network = InProcessNetwork::new();
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata");
    let replica_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("replica"),
        root.join("replica-meta"),
    )
    .expect("replica metadata");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend");

    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "/services/metadata-journal.txt".to_string(),
        kind: DfsObjectKind::File,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone()],
        content_hash: Some("blake3:metadata-journal".to_string()),
        content_size_bytes: Some(16),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner metadata");
    assert!(
        replica_dfs
            .metadata_backend()
            .expect("replica metadata backend")
            .get_object(&object_id)
            .expect("replica object before replay")
            .is_none()
    );

    let batch = owner_dfs
        .send_metadata_journal_since(&replica, 0)
        .expect("send journal")
        .expect("nonempty batch");
    assert_eq!(batch.entries.len(), 3);
    let events = replica_dfs.process_incoming().expect("replica incoming");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                source_node_id,
                applied_entries: 3,
                ..
            } if source_node_id == &owner
        )
    }));
    assert_eq!(
        replica_dfs
            .metadata_backend()
            .expect("replica metadata backend")
            .get_file_system(&file_system_id)
            .expect("replica filesystem"),
        Some(file_system)
    );
    assert_eq!(
        replica_dfs
            .metadata_backend()
            .expect("replica metadata backend")
            .get_object(&object_id)
            .expect("replica object"),
        Some(metadata.clone())
    );
    assert_eq!(replica_dfs.metadata().object(&object_id), Some(&metadata));

    owner_dfs
        .send_metadata_journal_since(&replica, 0)
        .expect("resend journal")
        .expect("nonempty repeated batch");
    let repeat_events = replica_dfs.process_incoming().expect("repeat incoming");
    assert!(repeat_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                source_node_id,
                applied_entries: 0,
                ..
            } if source_node_id == &owner
        )
    }));

    cleanup(&root);
}

#[test]
fn networked_dfs_applies_metadata_journal_incrementally_without_full_hydrate() {
    let owner = NodeId::from_legacy_named("node_metadata_incremental_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_metadata_incremental_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let replicated_object_id =
        DfsObjectId::from_legacy_named("obj_metadata_incremental_replicated").expect("object");
    let transient_object_id =
        DfsObjectId::from_legacy_named("obj_metadata_incremental_transient").expect("object");
    let version = DfsVersion::from_legacy_named("version_metadata_incremental").expect("version");
    let root = temp_root("networked_metadata_incremental");
    let network = InProcessNetwork::new();
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata");
    let replica_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("replica"),
        root.join("replica-meta"),
    )
    .expect("replica metadata");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend");

    owner_dfs
        .add_file_system(DfsFileSystem {
            file_system_id: file_system_id.clone(),
            display_name: "repo".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        })
        .expect("owner filesystem");

    let transient_metadata = DfsObjectMetadata {
        object_id: transient_object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "/services/transient-only-on-replica.txt".to_string(),
        kind: DfsObjectKind::File,
        owner_node_id: replica.clone(),
        current_locations: vec![replica.clone()],
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
        version: version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    replica_dfs
        .add_transient_object_metadata(transient_metadata.clone())
        .expect("replica transient metadata");
    assert!(
        replica_dfs
            .metadata_backend()
            .expect("replica backend")
            .get_object(&transient_object_id)
            .expect("transient backend lookup")
            .is_none(),
        "transient object is deliberately absent from the replica backend"
    );

    let replicated_metadata = DfsObjectMetadata {
        object_id: replicated_object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "/services/replicated-incremental.txt".to_string(),
        kind: DfsObjectKind::File,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone()],
        content_hash: Some("blake3:metadata-incremental".to_string()),
        content_size_bytes: Some(12),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_object_metadata(replicated_metadata.clone())
        .expect("owner metadata");

    owner_dfs
        .send_metadata_journal_since(&replica, 0)
        .expect("send journal")
        .expect("nonempty batch");
    let events = replica_dfs.process_incoming().expect("replica incoming");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                source_node_id,
                applied_entries,
                ..
            } if source_node_id == &owner && *applied_entries > 0
        )
    }));

    assert_eq!(
        replica_dfs.metadata().object(&replicated_object_id),
        Some(&replicated_metadata)
    );
    assert_eq!(
        replica_dfs.metadata().object(&transient_object_id),
        Some(&transient_metadata),
        "incremental metadata replay must not clear unrelated in-memory service state"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_sends_metadata_journal_in_bounded_batches() {
    let owner = NodeId::from_legacy_named("node_metadata_bounded_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_metadata_bounded_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let root = temp_root("networked_metadata_bounded_batches");
    let network = InProcessNetwork::new();
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    let _replica_transport = network.add_node(replica.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    owner_dfs
        .add_file_system(file_system)
        .expect("owner filesystem");
    for index in 0..DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT + 7 {
        let metadata = DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named(format!("obj_metadata_bounded_{index}"))
                .expect("object"),
            file_system_id: file_system_id.clone(),
            path: format!("/services/bounded-{index}.txt"),
            kind: DfsObjectKind::File,
            owner_node_id: owner.clone(),
            current_locations: vec![owner.clone()],
            content_hash: Some(format!("blake3:bounded-{index}")),
            content_size_bytes: Some(index as u64),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named(format!("version_bounded_{index}"))
                .expect("version"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        owner_dfs
            .add_object_metadata(metadata)
            .expect("owner metadata");
    }

    let first = owner_dfs
        .send_metadata_journal_since(&replica, 0)
        .expect("first journal send")
        .expect("first batch");
    assert_eq!(
        first.entries.len(),
        DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT
    );
    assert_eq!(
        first.next_after_sequence,
        DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT as u64
    );
    let second = owner_dfs
        .send_metadata_journal_since(&replica, first.next_after_sequence)
        .expect("second journal send")
        .expect("second batch");
    assert_eq!(
        second.entries.len(),
        DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT
    );
    assert_eq!(
        second.next_after_sequence,
        (DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT * 2) as u64
    );
    let third = owner_dfs
        .send_metadata_journal_since(&replica, second.next_after_sequence)
        .expect("third journal send")
        .expect("third batch");
    assert_eq!(third.entries.len(), 15);
    assert_eq!(
        third.next_after_sequence,
        (DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT * 2 + 15) as u64
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_routes_non_primary_write_to_metadata_shard_primary() {
    let writer = NodeId::from_legacy_named("node_route_writer").expect("node");
    let primary = NodeId::from_legacy_named("node_route_primary").expect("node");
    let witness = NodeId::from_legacy_named("node_route_witness").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_route_primary").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_route_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_route_2").expect("version");
    let root = temp_root("networked_route_non_primary_write");
    let network = InProcessNetwork::new();
    let control_plane = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![writer.clone(), primary.clone(), witness])
            .expect("control-plane kv cluster"),
    );
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: primary.clone(),
                replica_node_ids: vec![writer.clone()],
                epoch: 3,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let primary_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("services"),
        root.join("primary-meta"),
    )
    .expect("primary metadata backend");
    let mut writer_dfs = NetworkedDfsService::new(
        network.add_node(writer.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("writer-content")).expect("writer content"),
    )
    .with_control_plane(control_plane.clone());
    let mut primary_dfs = NetworkedDfsService::new(
        network.add_node(primary.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("primary-content")).expect("primary content"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(primary_backend)
    .expect("primary metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "sifs".to_string(),
        owner_node_id: primary.clone(),
        subtree_policies: Vec::new(),
    };
    let object = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "/services/routed.txt".to_string(),
        kind: DfsObjectKind::File,
        owner_node_id: primary.clone(),
        current_locations: vec![writer.clone()],
        content_hash: Some(content_hash(b"old routed bytes")),
        content_size_bytes: Some(b"old routed bytes".len() as u64),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    writer_dfs
        .add_file_system(file_system.clone())
        .expect("writer filesystem");
    primary_dfs
        .add_file_system(file_system)
        .expect("primary filesystem");
    writer_dfs
        .add_object_metadata(object)
        .expect("writer object metadata");

    let routed = writer_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: writer.clone(),
            },
            b"routed primary bytes".to_vec(),
            vec![primary.clone(), writer.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("routed write request");
    assert!(matches!(
        routed,
        NetworkedDfsWriteOutcome::Pending {
            durable_receipts: 0,
            required_receipts: 1,
            ..
        }
    ));

    let primary_events = primary_dfs.process_incoming().expect("primary incoming");
    assert!(primary_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteWriteServed {
                requester_node_id,
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { .. },
                    },
                ..
            } if requester_node_id == &writer
        )
    }));
    let writer_events = writer_dfs.process_incoming().expect("writer incoming");
    assert!(writer_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteWriteCompleted {
                primary_node_id,
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { .. },
                    },
                ..
            } if primary_node_id == &primary
        )
    }));
    let primary_object = primary_dfs
        .metadata_backend()
        .expect("primary metadata backend")
        .get_object(&object_id)
        .expect("primary object")
        .expect("primary object");
    let expected_hash = content_hash(b"routed primary bytes");
    assert_eq!(primary_object.version, version_2);
    assert_eq!(
        primary_object.content_hash.as_deref(),
        Some(expected_hash.as_str())
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_strict_routed_write_completion_resolves_remote_intent_alias() {
    let writer = NodeId::from_legacy_named("node_strict_route_writer").expect("node");
    let primary = NodeId::from_legacy_named("node_strict_route_primary").expect("node");
    let witness = NodeId::from_legacy_named("node_strict_route_witness").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_strict_route_primary").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_strict_route_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_strict_route_2").expect("version");
    let root = temp_root("networked_strict_route_alias");
    let network = InProcessNetwork::new();
    let control_plane = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![writer.clone(), primary.clone(), witness])
            .expect("control-plane kv cluster"),
    );
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: primary.clone(),
                replica_node_ids: vec![writer.clone()],
                epoch: 3,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let primary_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("services"),
        root.join("primary-meta"),
    )
    .expect("primary metadata backend");
    let mut writer_dfs = NetworkedDfsService::new(
        network.add_node(writer.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("writer-content")).expect("writer content"),
    )
    .with_control_plane(control_plane.clone());
    let mut primary_dfs = NetworkedDfsService::new(
        network.add_node(primary.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("primary-content")).expect("primary content"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(primary_backend)
    .expect("primary metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "sifs".to_string(),
        owner_node_id: primary.clone(),
        subtree_policies: Vec::new(),
    };
    let object = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "/services/strict-routed.txt".to_string(),
        kind: DfsObjectKind::File,
        owner_node_id: primary.clone(),
        current_locations: vec![writer.clone()],
        content_hash: Some(content_hash(b"old strict routed bytes")),
        content_size_bytes: Some(b"old strict routed bytes".len() as u64),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    writer_dfs
        .add_file_system(file_system.clone())
        .expect("writer filesystem");
    primary_dfs
        .add_file_system(file_system)
        .expect("primary filesystem");
    writer_dfs
        .add_object_metadata(object)
        .expect("writer object metadata");

    let routed = writer_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: writer.clone(),
            },
            b"strict routed primary bytes".to_vec(),
            vec![primary.clone(), writer.clone()],
            DfsReplicationPolicy::new(2, 2).expect("strict policy"),
        )
        .expect("routed write request");
    assert!(matches!(
        routed,
        NetworkedDfsWriteOutcome::Pending {
            durable_receipts: 0,
            required_receipts: 1,
            ..
        }
    ));

    let primary_events = primary_dfs.process_incoming().expect("primary incoming");
    assert!(primary_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteWriteServed {
                requester_node_id,
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome:
                            NetworkedDfsWriteOutcome::Pending {
                                durable_receipts: 1,
                                required_receipts: 2,
                                ..
                            },
                    },
                ..
            } if requester_node_id == &writer
        )
    }));

    let writer_events = writer_dfs.process_incoming().expect("writer stages");
    let remote_intent_id = writer_events
        .iter()
        .find_map(|event| match event {
            NetworkedDfsServiceEvent::RemoteWriteCompleted {
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome:
                            NetworkedDfsWriteOutcome::Pending {
                                intent_id,
                                durable_receipts: 1,
                                required_receipts: 2,
                            },
                    },
                ..
            } => Some(intent_id.clone()),
            _ => None,
        })
        .expect("writer receives remote pending intent");
    assert!(matches!(
        writer_dfs
            .handle_confirmed_write_timeout(
                &remote_intent_id,
                DfsConfirmedTimeoutAction::KeepPending,
            )
            .expect("remote intent pending"),
        DfsConfirmedTimeoutOutcome::StillPending {
            durable_receipts: 1,
            required_receipts: 2,
            ..
        }
    ));

    let primary_events = primary_dfs
        .process_incoming()
        .expect("primary receives strict receipt");
    assert!(primary_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteWriteServed {
                requester_node_id,
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { .. },
                    },
                ..
            } if requester_node_id == &writer
        )
    }));
    let writer_events = writer_dfs
        .process_incoming()
        .expect("writer receives strict completion");
    assert!(writer_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteWriteCompleted {
                primary_node_id,
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { .. },
                    },
                ..
            } if primary_node_id == &primary
        )
    }));
    assert!(matches!(
        writer_dfs
            .handle_confirmed_write_timeout(
                &remote_intent_id,
                DfsConfirmedTimeoutAction::KeepPending,
            )
            .expect("remote intent committed"),
        DfsConfirmedTimeoutOutcome::Committed(NetworkedDfsWriteOutcome::Committed { .. })
    ));

    cleanup(&root);
}

#[test]
fn routed_write_response_does_not_fail_when_requester_becomes_metadata_primary() {
    let writer = NodeId::from_legacy_named("node_route_flip_writer").expect("node");
    let primary = NodeId::from_legacy_named("node_route_flip_primary").expect("node");
    let witness = NodeId::from_legacy_named("node_route_flip_witness").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_route_flip_primary").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_route_flip_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_route_flip_2").expect("version");
    let root = temp_root("routed_write_response_requester_becomes_primary");
    let network = InProcessNetwork::new();
    let control_plane = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![writer.clone(), primary.clone(), witness])
            .expect("control-plane kv cluster"),
    );
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: primary.clone(),
                replica_node_ids: vec![writer.clone()],
                epoch: 3,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let primary_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("services"),
        root.join("primary-meta"),
    )
    .expect("primary metadata backend");
    let mut writer_dfs = NetworkedDfsService::new(
        network.add_node(writer.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("writer-content")).expect("writer content"),
    )
    .with_control_plane(control_plane.clone());
    let mut primary_dfs = NetworkedDfsService::new(
        network.add_node(primary.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("primary-content")).expect("primary content"),
    )
    .with_control_plane(control_plane.clone())
    .with_metadata_backend(primary_backend)
    .expect("primary metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "sifs".to_string(),
        owner_node_id: primary.clone(),
        subtree_policies: Vec::new(),
    };
    let object = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "/services/routed-flip.txt".to_string(),
        kind: DfsObjectKind::File,
        owner_node_id: primary.clone(),
        current_locations: vec![writer.clone()],
        content_hash: Some(content_hash(b"old routed flip bytes")),
        content_size_bytes: Some(b"old routed flip bytes".len() as u64),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    writer_dfs
        .add_file_system(file_system.clone())
        .expect("writer filesystem");
    primary_dfs
        .add_file_system(file_system)
        .expect("primary filesystem");
    writer_dfs
        .add_object_metadata(object)
        .expect("writer object metadata");

    let routed = writer_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: writer.clone(),
            },
            b"routed primary response after authority flip".to_vec(),
            vec![primary.clone(), writer.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("routed write request");
    assert!(matches!(
        routed,
        NetworkedDfsWriteOutcome::Pending {
            durable_receipts: 0,
            required_receipts: 1,
            ..
        }
    ));

    let primary_events = primary_dfs.process_incoming().expect("primary incoming");
    assert!(primary_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteWriteServed {
                requester_node_id,
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { .. },
                    },
                ..
            } if requester_node_id == &writer
        )
    }));
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: writer.clone(),
                replica_node_ids: vec![primary.clone()],
                epoch: 4,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard transfer");
    });

    let writer_events = writer_dfs
        .process_incoming()
        .expect("writer incoming after authority flip");
    assert!(writer_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteWriteCompleted {
                primary_node_id,
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { .. },
                    },
                ..
            } if primary_node_id == &primary
        )
    }));
    assert!(
        !writer_events.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::RemoteResponseDiscarded { reason, .. }
                    if reason.contains("metadata primary must not apply a replica journal batch")
            )
        }),
        "authority transfer race should not fail the completed routed write: {writer_events:?}"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_routes_reader_write_to_metadata_primary_via_fetch() {
    let writer = NodeId::from_legacy_named("node_route_reader_writer").expect("node");
    let primary = NodeId::from_legacy_named("node_route_reader_primary").expect("node");
    let witness = NodeId::from_legacy_named("node_route_reader_witness").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_route_reader_primary").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_route_reader_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_route_reader_2").expect("version");
    let root = temp_root("networked_route_reader_primary_write");
    let network = InProcessNetwork::new();
    let control_plane = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![writer.clone(), primary.clone(), witness])
            .expect("control-plane kv cluster"),
    );
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: primary.clone(),
                replica_node_ids: vec![writer.clone()],
                epoch: 4,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let primary_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("services"),
        root.join("primary-meta"),
    )
    .expect("primary metadata backend");
    let mut writer_dfs = NetworkedDfsService::new(
        network.add_node(writer.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("writer-content")).expect("writer content"),
    )
    .with_control_plane(control_plane.clone());
    let mut primary_dfs = NetworkedDfsService::new(
        network.add_node(primary.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("primary-content")).expect("primary content"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(primary_backend)
    .expect("primary metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "sifs".to_string(),
        owner_node_id: primary.clone(),
        subtree_policies: Vec::new(),
    };
    let object = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "/services/routed-reader.bin".to_string(),
        kind: DfsObjectKind::File,
        owner_node_id: primary.clone(),
        current_locations: vec![writer.clone()],
        content_hash: Some(content_hash(b"old routed reader bytes")),
        content_size_bytes: Some(b"old routed reader bytes".len() as u64),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    writer_dfs
        .add_file_system(file_system.clone())
        .expect("writer filesystem");
    primary_dfs
        .add_file_system(file_system)
        .expect("primary filesystem");
    writer_dfs
        .add_object_metadata(object)
        .expect("writer object metadata");

    let byte_count = 65 * 1024 + 17;
    let routed = writer_dfs
        .write_distributed_eventual_from_reader(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: writer.clone(),
            },
            PatternReader::new(byte_count),
            vec![writer.clone(), primary.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("routed reader write request");
    assert!(matches!(
        routed,
        NetworkedDfsWriteOutcome::Pending {
            durable_receipts: 0,
            required_receipts: 1,
            ..
        }
    ));

    let primary_events = primary_dfs
        .process_incoming()
        .expect("primary routed write");
    assert!(primary_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteWriteServed {
                requester_node_id,
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Pending { .. },
                    },
                ..
            } if requester_node_id == &writer
        )
    }));
    let writer_events = writer_dfs.process_incoming().expect("writer fetch");
    assert!(writer_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobFetched {
                source_node_id,
                object_id: fetched,
                ..
            } if source_node_id == &writer && fetched == &object_id
        )
    }));
    let primary_events = primary_dfs
        .process_incoming()
        .expect("primary fetch response");
    assert!(primary_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobFetched {
                source_node_id,
                object_id: fetched,
                ..
            } if source_node_id == &writer && fetched == &object_id
        )
    }));
    assert!(primary_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteWriteServed {
                requester_node_id,
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { .. },
                    },
                ..
            } if requester_node_id == &writer
        )
    }));
    let writer_events = writer_dfs
        .process_incoming()
        .expect("writer committed routed response");
    assert!(writer_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteWriteCompleted {
                primary_node_id,
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { .. },
                    },
                ..
            } if primary_node_id == &primary
        )
    }));
    let primary_object = primary_dfs
        .metadata_backend()
        .expect("primary metadata backend")
        .get_object(&object_id)
        .expect("primary object")
        .expect("primary object");
    let expected_bytes = pattern_bytes(byte_count);
    let expected_hash = content_hash(&expected_bytes);
    assert_eq!(primary_object.version, version_2);
    assert_eq!(
        primary_object.content_hash.as_deref(),
        Some(expected_hash.as_str())
    );
    assert_eq!(primary_object.content_size_bytes, Some(byte_count as u64));
    assert!(primary_object.current_locations.contains(&writer));
    assert!(primary_object.current_locations.contains(&primary));
    assert_eq!(
        primary_dfs
            .read_local_object(&object_id)
            .expect("primary read")
            .expect("primary data")
            .bytes,
        expected_bytes
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_configured_metadata_replication_drains_bounded_batches() {
    let owner = NodeId::from_legacy_named("node_metadata_bounded_config_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_metadata_bounded_config_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let root = temp_root("networked_metadata_bounded_configured");
    let network = InProcessNetwork::new();
    let control_plane = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![owner.clone(), replica.clone()])
            .expect("control-plane kv cluster"),
    );
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: owner.clone(),
                replica_node_ids: vec![replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("owner-meta"))
            .expect("owner metadata");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("replica-meta"))
            .expect("replica metadata");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_control_plane(control_plane.clone())
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    let object_count = DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT / 2 + 4;
    let mut last_object_id = None;
    for index in 0..object_count {
        let object_id =
            DfsObjectId::from_legacy_named(format!("obj_metadata_config_batch_{index}"))
                .expect("object");
        let metadata = DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: format!("/services/config-bounded-{index}.txt"),
            kind: DfsObjectKind::File,
            owner_node_id: owner.clone(),
            current_locations: vec![owner.clone()],
            content_hash: Some(format!("blake3:config-bounded-{index}")),
            content_size_bytes: Some(index as u64),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named(format!("version_config_bounded_{index}"))
                .expect("version"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        owner_dfs
            .add_object_metadata(metadata)
            .expect("owner metadata");
        last_object_id = Some(object_id);
    }

    let expected_entries = 1 + object_count * 2;
    let mut replicated_entries = 0usize;
    let mut batches = 0usize;
    while replicated_entries < expected_entries {
        let outcomes = owner_dfs
            .replicate_metadata_shard_to_configured_replicas(&file_system_id, "root")
            .expect("metadata replication send");
        let [
            DfsMetadataReplicationOutcome::Sent {
                entries,
                next_after_sequence,
                ..
            },
        ] = outcomes.as_slice()
        else {
            panic!("unexpected replication outcomes: {outcomes:?}");
        };
        assert!(*entries <= DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT);
        assert!(*next_after_sequence as usize > replicated_entries);
        replicated_entries = *next_after_sequence as usize;
        batches += 1;

        let replica_events = replica_dfs
            .process_incoming()
            .expect("replica applies metadata batch");
        assert!(replica_events.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                    source_node_id,
                    next_after_sequence: applied_after,
                    ..
                } if source_node_id == &owner && applied_after == next_after_sequence
            )
        }));
        let owner_events = owner_dfs
            .process_incoming()
            .expect("owner applies metadata ack");
        assert!(owner_events.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                    source_node_id,
                    next_after_sequence: ack_after,
                    ..
                } if source_node_id == &replica && ack_after == next_after_sequence
            )
        }));
    }
    assert!(
        batches > 1,
        "the configured replication backlog should require multiple bounded batches"
    );
    assert_eq!(replicated_entries, expected_entries);
    assert_eq!(
        replica_dfs
            .metadata_backend()
            .expect("replica metadata backend")
            .get_file_system(&file_system_id)
            .expect("replica filesystem"),
        Some(file_system)
    );
    assert!(
        replica_dfs
            .metadata_backend()
            .expect("replica metadata backend")
            .get_object(&last_object_id.expect("last object"))
            .expect("replica last object")
            .is_some()
    );
    let final_outcomes = owner_dfs
        .replicate_metadata_shard_to_configured_replicas(&file_system_id, "root")
        .expect("final metadata replication check");
    assert!(matches!(
        final_outcomes.as_slice(),
        [DfsMetadataReplicationOutcome::NoChanges { after_sequence, .. }]
            if *after_sequence as usize == expected_entries
    ));

    drop(owner_dfs);
    let restarted_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("owner-meta"))
            .expect("restarted owner metadata");
    let mut restarted_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content-restarted"))
            .expect("restarted owner content"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(restarted_backend)
    .expect("restarted owner metadata backend");
    let restarted_outcomes = restarted_owner
        .replicate_metadata_shard_to_configured_replicas(&file_system_id, "root")
        .expect("restarted metadata replication check");
    assert!(
        matches!(
            restarted_outcomes.as_slice(),
            [DfsMetadataReplicationOutcome::NoChanges { after_sequence, .. }]
                if *after_sequence as usize == expected_entries
        ),
        "restarted owner should load the durable replication cursor instead of resending the full journal: {restarted_outcomes:?}"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_replicates_configured_metadata_shard_to_replicas() {
    let owner = NodeId::from_legacy_named("node_metadata_config_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_metadata_config_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_metadata_config").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_metadata_configured_replicas");
    let network = InProcessNetwork::new();
    let control_plane = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![owner.clone(), replica.clone()])
            .expect("control-plane kv cluster"),
    );
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: owner.clone(),
                replica_node_ids: vec![replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard config");
    });

    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("owner-meta"))
            .expect("owner metadata");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("replica-meta"))
            .expect("replica metadata");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_control_plane(control_plane.clone())
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend");

    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "/services/configured-metadata.txt".to_string(),
        kind: DfsObjectKind::File,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone()],
        content_hash: Some("blake3:configured-metadata".to_string()),
        content_size_bytes: Some(19),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system)
        .expect("owner filesystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner metadata");

    let outcomes = owner_dfs
        .replicate_configured_metadata_shards(&file_system_id)
        .expect("replicate metadata shard");
    assert_eq!(
        outcomes,
        vec![DfsMetadataReplicationOutcome::Sent {
            target_node_id: replica.clone(),
            shard_id: DfsMetadataShardId::new("root"),
            entries: 3,
            next_after_sequence: 3,
        }]
    );
    let events = replica_dfs.process_incoming().expect("replica incoming");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                source_node_id,
                applied_entries: 3,
                ..
            } if source_node_id == &owner
        )
    }));
    assert_eq!(
        replica_dfs
            .metadata_backend()
            .expect("replica metadata backend")
            .get_object(&object_id)
            .expect("replica object"),
        Some(metadata)
    );

    let second_outcomes = owner_dfs
        .replicate_metadata_shard_to_configured_replicas(&file_system_id, "root")
        .expect("replicate metadata shard again");
    assert_eq!(
        second_outcomes,
        vec![DfsMetadataReplicationOutcome::InFlight {
            target_node_id: replica.clone(),
            shard_id: DfsMetadataShardId::new("root"),
            next_after_sequence: 3,
        }]
    );
    let owner_events = owner_dfs
        .process_incoming()
        .expect("owner applies metadata ack");
    assert!(owner_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                source_node_id,
                next_after_sequence: 3,
                ..
            } if source_node_id == &replica
        )
    }));
    let third_outcomes = owner_dfs
        .replicate_metadata_shard_to_configured_replicas(&file_system_id, "root")
        .expect("replicate metadata shard after ack");
    assert_eq!(
        third_outcomes,
        vec![DfsMetadataReplicationOutcome::NoChanges {
            target_node_id: replica,
            shard_id: DfsMetadataShardId::new("root"),
            after_sequence: 3,
        }]
    );

    cleanup(&root);
}

#[test]
fn posix_facade_supports_service_file_workflow_and_repairs_offline_replica() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let root = temp_root("posix_service_workflow");
    let network = InProcessNetwork::new();
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    let replica_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("replica"),
        root.join("replica-meta"),
    )
    .expect("replica metadata");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend");

    owner_dfs
        .bootstrap_distributed_sifs()
        .expect("owner bootstrap");
    replica_dfs
        .bootstrap_distributed_sifs()
        .expect("replica bootstrap");
    owner_dfs
        .posix_mkdir_all("/services/web")
        .expect("mkdir service");
    let stat = owner_dfs
        .posix_write_file(
            "/services/web/index.html",
            b"<h1>Sinomo</h1>",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone(), replica.clone()]),
        )
        .expect("write service file");
    assert_eq!(stat.size_bytes, Some(15));
    assert_eq!(
        owner_dfs
            .posix_read_file("/services/web/index.html")
            .expect("read service file"),
        b"<h1>Sinomo</h1>"
    );

    let repairs = replica_dfs.process_incoming().expect("replica repair");
    assert!(repairs.iter().any(|event| {
        matches!(event, NetworkedDfsServiceEvent::ReplicaRepaired { object_id, .. } if object_id == &stat.object_id)
    }));
    let listing = owner_dfs
        .posix_list_dir("/services/web")
        .expect("list service dir");
    assert_eq!(listing.len(), 1);
    assert_eq!(listing[0].name, "index.html");
    owner_dfs
        .posix_rename("/services/web/index.html", "/services/web/current.html")
        .expect("rename service file");
    assert_eq!(
        owner_dfs
            .posix_read_file("/services/web/current.html")
            .expect("read renamed service file"),
        b"<h1>Sinomo</h1>"
    );
    assert!(
        owner_dfs
            .posix_stat("/services/web/index.html")
            .expect_err("old path hidden after rename")
            .to_string()
            .contains("not found")
    );
    owner_dfs
        .posix_write_file(
            "/services/web/next.html",
            b"<h1>Next</h1>",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("write replacement candidate");
    owner_dfs
        .posix_rename("/services/web/next.html", "/services/web/current.html")
        .expect("atomic replace service file");
    assert_eq!(
        owner_dfs
            .posix_read_file("/services/web/current.html")
            .expect("read replaced service file"),
        b"<h1>Next</h1>"
    );
    owner_dfs
        .posix_unlink("/services/web/current.html")
        .expect("unlink service file");
    assert!(
        owner_dfs
            .posix_stat("/services/web/current.html")
            .expect_err("stat after unlink")
            .to_string()
            .contains("not found")
    );
    assert!(
        owner_dfs
            .posix_list_dir("/services/web")
            .expect("list after unlink")
            .is_empty()
    );

    cleanup(&root);
}

#[test]
fn posix_rename_directory_over_empty_directory_moves_subtree_atomically() {
    let owner = NodeId::from_legacy_named("node_posix_rename_dir_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_posix_rename_dir_replica").expect("node");
    let root = temp_root("posix_rename_directory_over_empty_directory");
    let network = InProcessNetwork::new();
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    );
    owner_dfs.bootstrap_distributed_sifs().expect("bootstrap");
    replica_dfs
        .bootstrap_distributed_sifs()
        .expect("replica bootstrap");

    owner_dfs
        .posix_mkdir_all("/services/releases/current")
        .expect("mkdir current");
    owner_dfs
        .posix_mkdir_all("/services/releases/next/nested")
        .expect("mkdir next nested");
    owner_dfs
        .posix_write_file(
            "/services/releases/next/nested/app.txt",
            b"next",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone(), replica.clone()]),
        )
        .expect("write nested file");
    replica_dfs
        .process_incoming()
        .expect("replicate nested file");

    owner_dfs
        .posix_rename("/services/releases/next", "/services/releases/current")
        .expect("replace empty current directory");

    assert_eq!(
        owner_dfs
            .posix_read_file("/services/releases/current/nested/app.txt")
            .expect("read moved nested file"),
        b"next"
    );
    assert!(matches!(
        owner_dfs.posix_stat("/services/releases/next"),
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
    ));
    assert!(matches!(
        owner_dfs.posix_stat("/services/releases/next/nested/app.txt"),
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        owner_dfs
            .posix_list_dir("/services/releases")
            .expect("list releases")
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>(),
        vec!["current".to_string()]
    );

    let mut reloaded_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("reload metadata"),
    )
    .expect("reload metadata backend");
    reloaded_dfs
        .bootstrap_distributed_sifs()
        .expect("reload bootstrap");
    assert_eq!(
        reloaded_dfs
            .posix_read_file("/services/releases/current/nested/app.txt")
            .expect("read moved nested file after reload"),
        b"next"
    );
    assert!(matches!(
        reloaded_dfs.posix_stat("/services/releases/next/nested/app.txt"),
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
    ));

    cleanup(&root);
}

#[test]
fn posix_paths_treat_backslash_as_filename_character() {
    let owner = NodeId::from_legacy_named("node_posix_backslash_owner").expect("node");
    let root = temp_root("posix_backslash_filename");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata"),
    )
    .expect("owner metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/names/a")
        .expect("mkdir slash parent");

    let backslash_stat = dfs
        .posix_write_file(
            r"/services/names/a\b.txt",
            b"backslash",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("write backslash filename");
    let slash_stat = dfs
        .posix_write_file(
            "/services/names/a/b.txt",
            b"slash",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("write slash path");

    assert_ne!(backslash_stat.object_id, slash_stat.object_id);
    assert_eq!(
        dfs.posix_read_file(r"/services/names/a\b.txt")
            .expect("read backslash filename"),
        b"backslash"
    );
    assert_eq!(
        dfs.posix_read_file("/services/names/a/b.txt")
            .expect("read slash path"),
        b"slash"
    );
    assert_eq!(
        dfs.posix_list_dir("/services/names")
            .expect("list names")
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>(),
        vec!["a".to_string(), r"a\b.txt".to_string()]
    );

    let mut reloaded = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("reload metadata"),
    )
    .expect("reload metadata backend");
    reloaded
        .bootstrap_distributed_sifs()
        .expect("reload bootstrap");
    assert_eq!(
        reloaded
            .posix_read_file(r"/services/names/a\b.txt")
            .expect("read backslash after reload"),
        b"backslash"
    );
    assert_eq!(
        reloaded
            .posix_read_file("/services/names/a/b.txt")
            .expect("read slash after reload"),
        b"slash"
    );

    cleanup(&root);
}

#[test]
fn posix_paths_resolve_parent_components_within_sifs_root() {
    let owner = NodeId::from_legacy_named("node_posix_parent_components_owner").expect("node");
    let root = temp_root("posix_parent_components");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata"),
    )
    .expect("owner metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/paths")
        .expect("mkdir services paths");

    let stat = dfs
        .posix_write_file(
            "/services/paths/tmp/../file.txt",
            b"normalized",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("write through parent component");

    assert_eq!(stat.path, "/services/paths/file.txt");
    assert_eq!(
        dfs.posix_read_file("/../services/paths/./file.txt")
            .expect("read with root parent and current component"),
        b"normalized"
    );
    assert!(
        matches!(
            dfs.posix_stat("/services/paths/tmp/file.txt"),
            Err(sinomo_dfs::DfsPosixError::NotFound { .. })
        ),
        "parent components must be resolved instead of becoming path data"
    );
    assert_eq!(
        dfs.posix_list_dir("/services/paths/../paths")
            .expect("list normalized path")
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>(),
        vec!["file.txt".to_string()]
    );

    let mut reloaded = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("reload metadata"),
    )
    .expect("reload metadata backend");
    reloaded
        .bootstrap_distributed_sifs()
        .expect("reload bootstrap");
    assert_eq!(
        reloaded
            .posix_read_file("/services/paths/../paths/file.txt")
            .expect("read normalized path after reload"),
        b"normalized"
    );

    cleanup(&root);
}

#[test]
fn posix_paths_reject_nul_bytes_before_metadata_mutation() {
    let owner = NodeId::from_legacy_named("node_posix_nul_owner").expect("node");
    let root = temp_root("posix_nul_path");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata"),
    )
    .expect("owner metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/nul")
        .expect("mkdir nul parent");

    for result in [
        dfs.posix_write_file(
            "/services/nul/bad\0name.txt",
            b"bad",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        ),
        dfs.posix_mkdir("/services/nul/bad\0dir"),
        dfs.posix_symlink(
            "target",
            "/services/nul/bad\0link",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        ),
    ] {
        assert!(matches!(
            result,
            Err(sinomo_dfs::DfsPosixError::InvalidPath { .. })
        ));
    }

    assert!(
        dfs.posix_list_dir("/services/nul")
            .expect("list nul parent")
            .is_empty(),
        "invalid paths must not create metadata entries"
    );

    let reloaded = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("reload metadata"),
    )
    .expect("reload metadata backend");
    assert!(
        reloaded
            .posix_list_dir("/services/nul")
            .expect("reload list nul parent")
            .is_empty()
    );

    cleanup(&root);
}

#[test]
fn posix_symlink_rejects_invalid_targets_before_metadata_mutation() {
    let owner = NodeId::from_legacy_named("node_posix_bad_symlink_target_owner").expect("node");
    let root = temp_root("posix_bad_symlink_target");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata"),
    )
    .expect("owner metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/links")
        .expect("mkdir links parent");

    for (target, link_path) in [
        ("", "/services/links/empty-target"),
        ("bad\0target", "/services/links/nul-target"),
    ] {
        assert!(matches!(
            dfs.posix_symlink(
                target,
                link_path,
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()])
            ),
            Err(sinomo_dfs::DfsPosixError::InvalidPath { .. })
        ));
    }

    assert!(
        dfs.posix_list_dir("/services/links")
            .expect("list links parent")
            .is_empty(),
        "invalid symlink targets must not create metadata entries"
    );

    let mut reloaded = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("reload metadata"),
    )
    .expect("reload metadata backend");
    reloaded
        .bootstrap_distributed_sifs()
        .expect("reload bootstrap");
    assert!(
        reloaded
            .posix_list_dir("/services/links")
            .expect("reload list links parent")
            .is_empty()
    );

    cleanup(&root);
}

#[test]
fn posix_stat_read_and_write_follow_symlink_targets() {
    let owner = NodeId::from_legacy_named("node_posix_symlink_follow_owner").expect("node");
    let root = temp_root("posix_symlink_follow_targets");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata"),
    )
    .expect("owner metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/symlink-targets/real-dir")
        .expect("mkdir symlink targets");
    dfs.posix_write_file(
        "/services/symlink-targets/target.txt",
        b"target",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("write target");
    dfs.posix_symlink(
        "target.txt",
        "/services/symlink-targets/link.txt",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("create file symlink");

    assert_eq!(
        dfs.posix_lstat("/services/symlink-targets/link.txt")
            .expect("lstat link")
            .kind,
        DfsObjectKind::Symlink
    );
    assert_eq!(
        dfs.posix_stat("/services/symlink-targets/link.txt")
            .expect("stat follows link")
            .kind,
        DfsObjectKind::File
    );
    assert_eq!(
        dfs.posix_read_file("/services/symlink-targets/link.txt")
            .expect("read follows link"),
        b"target"
    );
    dfs.posix_write_file(
        "/services/symlink-targets/link.txt",
        b"updated",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("write follows link");
    assert_eq!(
        dfs.posix_read_file("/services/symlink-targets/target.txt")
            .expect("read updated target"),
        b"updated"
    );
    assert_eq!(
        dfs.posix_readlink("/services/symlink-targets/link.txt")
            .expect("readlink preserves target"),
        "target.txt"
    );
    dfs.posix_set_mode("/services/symlink-targets/link.txt", 0o600)
        .expect("chmod follows link");
    assert_eq!(
        dfs.posix_stat("/services/symlink-targets/target.txt")
            .expect("target mode")
            .mode,
        0o600
    );
    assert_ne!(
        dfs.posix_lstat("/services/symlink-targets/link.txt")
            .expect("link mode remains independent")
            .mode,
        0o600
    );
    dfs.posix_set_times(
        "/services/symlink-targets/link.txt",
        Some(11_111),
        Some(22_222),
    )
    .expect("utimens follows link");
    assert_eq!(
        dfs.posix_stat("/services/symlink-targets/target.txt")
            .expect("target atime")
            .accessed_at_unix_nanos,
        11_111
    );
    dfs.posix_set_symlink_times(
        "/services/symlink-targets/link.txt",
        Some(33_333),
        Some(44_444),
    )
    .expect("lutimens updates link");
    assert_eq!(
        dfs.posix_lstat("/services/symlink-targets/link.txt")
            .expect("link atime")
            .accessed_at_unix_nanos,
        33_333
    );
    assert_eq!(
        dfs.posix_stat("/services/symlink-targets/target.txt")
            .expect("target atime unchanged by lutimens")
            .accessed_at_unix_nanos,
        11_111
    );

    dfs.posix_write_file(
        "/services/symlink-targets/real-dir/nested.txt",
        b"nested",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("write nested target");
    dfs.posix_symlink(
        "real-dir",
        "/services/symlink-targets/dir-link",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("create directory symlink");
    assert_eq!(
        dfs.posix_read_file("/services/symlink-targets/dir-link/nested.txt")
            .expect("read through intermediate symlink"),
        b"nested"
    );
    assert_eq!(
        dfs.posix_list_dir("/services/symlink-targets/dir-link")
            .expect("list follows directory symlink")
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>(),
        vec!["nested.txt".to_string()]
    );

    let mut reloaded = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("reload metadata"),
    )
    .expect("reload metadata backend");
    reloaded
        .bootstrap_distributed_sifs()
        .expect("reload bootstrap");
    assert_eq!(
        reloaded
            .posix_read_file("/services/symlink-targets/link.txt")
            .expect("reloaded read follows link"),
        b"updated"
    );
    assert_eq!(
        reloaded
            .posix_read_file("/services/symlink-targets/dir-link/nested.txt")
            .expect("reloaded read through intermediate symlink"),
        b"nested"
    );
    assert_eq!(
        reloaded
            .posix_list_dir("/services/symlink-targets/dir-link")
            .expect("reloaded list follows directory symlink")
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>(),
        vec!["nested.txt".to_string()]
    );
    assert_eq!(
        reloaded
            .posix_lstat("/services/symlink-targets/link.txt")
            .expect("reloaded lstat link")
            .kind,
        DfsObjectKind::Symlink
    );

    cleanup(&root);
}

#[test]
fn posix_operations_follow_intermediate_parent_symlinks() {
    let owner = NodeId::from_legacy_named("node_posix_parent_symlink_owner").expect("node");
    let root = temp_root("posix_parent_symlink_operations");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata"),
    )
    .expect("owner metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/parents/real")
        .expect("mkdir real parent");
    dfs.posix_symlink(
        "real",
        "/services/parents/link",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("create parent symlink");

    dfs.posix_mkdir("/services/parents/link/created")
        .expect("mkdir through parent symlink");
    dfs.posix_write_file(
        "/services/parents/real/original.txt",
        b"original",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("write hardlink target");
    dfs.posix_hardlink(
        "/services/parents/link/original.txt",
        "/services/parents/link/original-alias.txt",
    )
    .expect("hardlink through parent symlink");
    assert_eq!(
        dfs.posix_stat("/services/parents/real/original.txt")
            .expect("original link count")
            .link_count,
        2
    );
    assert_eq!(
        dfs.posix_read_file("/services/parents/real/original-alias.txt")
            .expect("read hardlink alias"),
        b"original"
    );
    assert_eq!(
        dfs.posix_stat("/services/parents/real/created")
            .expect("created target dir")
            .kind,
        DfsObjectKind::Directory
    );
    assert_eq!(
        dfs.posix_stat("/services/parents/link/created")
            .expect("stat through parent symlink")
            .kind,
        DfsObjectKind::Directory
    );

    dfs.posix_symlink(
        "created",
        "/services/parents/link/created-link",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("create symlink through parent symlink");
    assert_eq!(
        dfs.posix_lstat("/services/parents/link/created-link")
            .expect("lstat follows parent symlink only")
            .kind,
        DfsObjectKind::Symlink
    );
    assert_eq!(
        dfs.posix_lstat("/services/parents/real/created-link")
            .expect("canonical lstat")
            .kind,
        DfsObjectKind::Symlink
    );

    dfs.posix_rename(
        "/services/parents/link/created",
        "/services/parents/link/renamed",
    )
    .expect("rename through parent symlink");
    assert!(matches!(
        dfs.posix_stat("/services/parents/real/created"),
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_stat("/services/parents/real/renamed")
            .expect("renamed target dir")
            .kind,
        DfsObjectKind::Directory
    );

    dfs.posix_unlink("/services/parents/link/created-link")
        .expect("unlink through parent symlink");
    assert!(matches!(
        dfs.posix_lstat("/services/parents/real/created-link"),
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
    ));

    let mut reloaded = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("reload metadata"),
    )
    .expect("reload metadata backend");
    reloaded
        .bootstrap_distributed_sifs()
        .expect("reload bootstrap");
    assert_eq!(
        reloaded
            .posix_stat("/services/parents/link/renamed")
            .expect("reloaded stat through parent symlink")
            .kind,
        DfsObjectKind::Directory
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_persists_committed_metadata_through_backend() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_metadata_backend_commit");
    let network = InProcessNetwork::new();
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner-shard"), root.join("meta"))
            .expect("metadata backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner DfsFileSystem");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica DfsFileSystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"metadata backend bytes".to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        NetworkedDfsWriteOutcome::Committed { .. } | NetworkedDfsWriteOutcome::Conflict { .. } => {
            panic!("write should wait for replica ack")
        }
    };

    replica_dfs.process_incoming().expect("replica stage");
    owner_dfs.process_incoming().expect("owner commit");

    let backend = owner_dfs.metadata_backend().expect("metadata backend");
    let object = backend
        .get_object(&object_id)
        .expect("get object")
        .expect("object");
    assert_eq!(object.version, version_2);
    assert!(
        object
            .content_hash
            .as_deref()
            .is_some_and(|hash| hash.starts_with("blake3:"))
    );
    assert!(
        backend
            .get_version(&object_id, &version_2)
            .expect("get version")
            .is_some()
    );
    let receipts = backend
        .receipts_for_version(&object_id, &version_2)
        .expect("receipts");
    assert_eq!(receipts.len(), 2);
    assert!(
        receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == owner)
    );
    assert!(
        receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == replica)
    );
    let intent = backend
        .get_intent(&object_id, &intent_id)
        .expect("get intent")
        .expect("intent");
    assert_eq!(intent.status, DfsWriteIntentStatus::Committed);
    assert_eq!(intent.target_version, version_2);
    let journal = backend.journal_since(0).expect("journal");
    assert!(
        journal
            .iter()
            .any(|entry| matches!(entry.mutation, DfsMetadataMutation::PutIntent { .. }))
    );
    assert!(journal.iter().any(|entry| matches!(
        entry.mutation,
        DfsMetadataMutation::CommitVisibleVersion { .. }
    )));

    cleanup(&root);
}

#[test]
fn networked_dfs_threshold_commit_stages_missing_desired_replica() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_threshold_stage");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner DfsFileSystem");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica DfsFileSystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner,
            },
            b"repair bytes".to_vec(),
            vec![
                NodeId::from_legacy_named("node_owner").expect("node"),
                NodeId::from_legacy_named("node_replica").expect("node"),
            ],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("write");

    assert!(matches!(
        write,
        NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } if repair_jobs.len() == 1
    ));

    let stage_events = replica_dfs.process_incoming().expect("stage");
    assert!(stage_events.iter().any(|event| {
        matches!(
            event,
            sinomo_dfs::NetworkedDfsServiceEvent::BlobStaged { .. }
        )
    }));
    owner_dfs.process_incoming().expect("stage response");
    let owner_metrics = owner_dfs.operational_metrics().expect("owner metrics");
    assert_eq!(owner_metrics.repair_job_count, 0);
    assert_eq!(owner_metrics.pending_write_count, 0);
    let read = replica_dfs
        .read_local_object(&object_id)
        .expect("read")
        .expect("data");
    assert_eq!(read.bytes, b"repair bytes");
    assert_eq!(read.version, version_2);

    cleanup(&root);
}

#[test]
fn networked_dfs_large_repair_chunks_are_self_describing() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_large_repair_metadata").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_large_repair_metadata");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut replica_transport = network.add_node(replica.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    owner_dfs
        .add_file_system(file_system)
        .expect("owner filesystem");
    owner_dfs
        .add_object_metadata(DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: "large-repair-metadata.bin".to_string(),
            kind: DfsObjectKind::Artifact,
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
            version: version_1,
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        })
        .expect("owner object");

    let bytes = chunked_test_bytes(4_464);
    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            bytes,
            vec![owner, replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("large threshold write");
    assert!(matches!(
        write,
        NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } if repair_jobs.len() == 1
    ));

    let frames = replica_transport
        .drain_incoming()
        .expect("repair chunks sent to replica");
    let chunks = frames
        .iter()
        .map(|frame| {
            let MeshMessageBody::Protocol(protocol) = &frame.frame.message.body else {
                panic!("expected protocol frame");
            };
            protocol.decode::<DfsNetworkMessage>().expect("decode DFS")
        })
        .filter_map(|message| match message {
            DfsNetworkMessage::RepairReplicaChunk(chunk) => Some(chunk),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        chunks.len() > 1,
        "large repair should be split into multiple chunks"
    );
    assert!(
        chunks.iter().all(|chunk| chunk.object_metadata.is_some()),
        "every repair chunk must carry metadata so out-of-order delivery can be authorized"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_plans_replicas_from_candidates_before_write() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_placement_write");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner DfsFileSystem");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica DfsFileSystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let request = DfsWriteRequest {
        file_system_id: file_system_id.clone(),
        object_id: object_id.clone(),
        target_version: version_2.clone(),
        requesting_node_id: owner.clone(),
    };
    let policy = DfsReplicationPolicy::new(2, 2).expect("policy");
    let plan = owner_dfs
        .plan_distributed_eventual_replicas(
            &request,
            vec![replica.clone(), owner.clone()],
            policy.clone(),
        )
        .expect("plan");
    assert_eq!(plan.desired_replica_nodes[0], owner);
    assert_eq!(plan.desired_replica_nodes.len(), 2);

    let write = owner_dfs
        .write_distributed_eventual_with_candidates(
            request,
            b"planned bytes".to_vec(),
            vec![
                replica.clone(),
                NodeId::from_legacy_named("node_owner").expect("node"),
            ],
            policy,
        )
        .expect("write");
    assert!(matches!(
        write,
        NetworkedDfsWriteOutcome::Pending {
            durable_receipts: 1,
            required_receipts: 2,
            ..
        }
    ));

    replica_dfs.process_incoming().expect("replica stage");
    owner_dfs.process_incoming().expect("owner commit");
    replica_dfs.process_incoming().expect("replica commit");
    assert_eq!(
        replica_dfs
            .read_local_object(&object_id)
            .expect("read")
            .expect("data")
            .bytes,
        b"planned bytes"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_replica_superset_before_staging() {
    let owner = NodeId::from_legacy_named("node_superset_owner").expect("node");
    let replica_a = NodeId::from_legacy_named("node_superset_replica_a").expect("node");
    let replica_b = NodeId::from_legacy_named("node_superset_replica_b").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_superset").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_replica_superset");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut replica_a_control = network.add_node(replica_a.clone());
    let mut replica_b_control = network.add_node(replica_b.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system)
        .expect("owner filesystem");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let bytes = b"must not stage to a replica superset".to_vec();
    let rejected_hash = content_hash(&bytes);
    let error = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes,
            vec![owner, replica_a.clone(), replica_b.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect_err("two-replica write must reject a three-node target set");
    assert!(
        matches!(error, sinomo_dfs::DfsError::Distributed { .. }),
        "replica superset should fail as distributed validation: {error:?}"
    );
    assert!(
        !owner_dfs
            .content()
            .has_blob(&rejected_hash)
            .expect("rejected blob check"),
        "invalid replica target sets must not persist local content"
    );
    assert!(
        owner_dfs
            .content()
            .read_manifest(object_id.as_str(), version_2.as_str())
            .is_err(),
        "invalid replica target sets must not publish manifests"
    );
    assert!(
        replica_a_control
            .drain_incoming()
            .expect("replica a mailbox")
            .is_empty()
    );
    assert!(
        replica_b_control
            .drain_incoming()
            .expect("replica b mailbox")
            .is_empty()
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_self_only_fast_path_rejects_duplicate_pending_intent() {
    let owner = NodeId::from_legacy_named("node_fast_duplicate_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_fast_duplicate_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_fast_duplicate").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_self_only_duplicate_pending");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut replica_control = network.add_node(replica.clone());
    owner_dfs
        .add_file_system(DfsFileSystem {
            file_system_id: file_system_id.clone(),
            display_name: "repo".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        })
        .expect("owner filesystem");
    owner_dfs
        .add_object_metadata(DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: "artifact.bin".to_string(),
            kind: DfsObjectKind::Artifact,
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
            version: version_1.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        })
        .expect("owner object");

    let pending = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"pending two replica write".to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("pending write");
    let pending_intent_id = match pending {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    assert_eq!(
        replica_control
            .drain_incoming()
            .expect("drop staged write")
            .len(),
        1
    );

    let duplicate = owner_dfs
        .write_distributed_eventual_with_base(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            Some(version_1.clone()),
            b"duplicate self-only write".to_vec(),
            vec![owner],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect("duplicate write should be isolated as conflict");
    assert!(matches!(
        duplicate,
        NetworkedDfsWriteOutcome::Conflict { conflict }
            if conflict.remote_version == version_1
                && conflict.local_version == version_2
                && conflict.reason.contains("duplicate DFS write intent")
    ));
    assert_eq!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("visible object")
            .version,
        version_1,
        "self-only duplicate fast path must not publish over an awaiting distributed intent"
    );
    assert_eq!(
        owner_dfs
            .coordinator()
            .intent(&pending_intent_id)
            .expect("pending intent")
            .state,
        DfsWriteIntentState::AwaitingReceipts
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_second_writer_conflicts_with_existing_pending_write() {
    let owner = NodeId::from_legacy_named("node_pending_conflict_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_pending_conflict_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_pending_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let root = temp_root("networked_pending_write_conflict");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner-shard"), root.join("meta"))
            .expect("metadata backend"),
    )
    .expect("metadata backend");
    let mut replica_control = network.add_node(replica.clone());
    owner_dfs
        .add_file_system(DfsFileSystem {
            file_system_id: file_system_id.clone(),
            display_name: "repo".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        })
        .expect("owner filesystem");
    owner_dfs
        .add_object_metadata(DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: "artifact.bin".to_string(),
            kind: DfsObjectKind::Artifact,
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
            version: version_1.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        })
        .expect("owner object");

    let pending = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"pending version two".to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("pending write");
    assert!(matches!(pending, NetworkedDfsWriteOutcome::Pending { .. }));
    assert_eq!(
        replica_control
            .drain_incoming()
            .expect("drop first stage")
            .len(),
        1
    );

    let second = owner_dfs
        .write_distributed_eventual_with_base(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_3.clone(),
                requesting_node_id: owner.clone(),
            },
            Some(version_1.clone()),
            b"competing version three".to_vec(),
            vec![owner.clone(), replica],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("second write should become conflict");
    assert!(matches!(
        second,
        NetworkedDfsWriteOutcome::Conflict { conflict }
            if conflict.remote_version == version_1
                && conflict.local_version == version_3
                && conflict.reason.contains("pending DFS write intent")
    ));
    assert_eq!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("visible object")
            .version,
        version_1,
        "competing pending write must not publish a new visible version"
    );
    let conflicts = owner_dfs
        .conflicts_for_object(&object_id)
        .expect("conflicts");
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].local_version, version_3);

    cleanup(&root);
}

#[test]
fn networked_dfs_three_nodes_two_replicas_supports_multiple_readers_and_writers() {
    let node_a = NodeId::from_legacy_named("node_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_b").expect("node");
    let node_c = NodeId::from_legacy_named("node_c").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_multi_node").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let version_4 = DfsVersion::from_legacy_named("version_4").expect("version");
    let root = temp_root("networked_three_nodes_two_replicas");
    let network = InProcessNetwork::new();
    let mut dfs_a = NetworkedDfsService::new(
        network.add_node(node_a.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("a")).expect("node a store"),
    );
    let mut dfs_b = NetworkedDfsService::new(
        network.add_node(node_b.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("b")).expect("node b store"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("node-b-shard"), root.join("b-meta"))
            .expect("node b metadata backend"),
    )
    .expect("node b metadata backend");
    let mut dfs_c = NetworkedDfsService::new(
        network.add_node(node_c.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("c")).expect("node c store"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("node-c-shard"), root.join("c-meta"))
            .expect("node c metadata backend"),
    )
    .expect("node c metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: node_a.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "multi-node.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: node_a.clone(),
        current_locations: vec![node_b.clone(), node_c.clone()],
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    for dfs in [&mut dfs_a, &mut dfs_b, &mut dfs_c] {
        dfs.add_file_system(file_system.clone())
            .expect("filesystem");
        dfs.add_object_metadata(metadata.clone()).expect("object");
    }

    let write_ab = dfs_a
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: node_a.clone(),
            },
            b"version two bytes".to_vec(),
            vec![node_a.clone(), node_b.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("write a+b");
    assert!(matches!(
        write_ab,
        NetworkedDfsWriteOutcome::Pending {
            durable_receipts: 1,
            required_receipts: 2,
            ..
        }
    ));
    assert!(dfs_b
        .process_incoming()
        .expect("b stages v2")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobStaged { object_id: staged, .. } if staged == &object_id)));
    assert!(dfs_a
        .process_incoming()
        .expect("a commits v2")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobStaged { object_id: staged, .. } if staged == &object_id)));
    assert!(dfs_b
        .process_incoming()
        .expect("b applies v2 commit")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::VersionCommitted { object_id: committed, .. } if committed == &object_id)));

    let committed_v2 = dfs_a
        .metadata()
        .object(&object_id)
        .expect("a v2 metadata")
        .clone();
    dfs_c
        .add_object_metadata(committed_v2.clone())
        .expect("c receives v2 metadata");
    assert_eq!(
        committed_v2.current_locations,
        vec![node_a.clone(), node_b.clone()]
    );
    assert_eq!(
        dfs_b
            .read_local_object(&object_id)
            .expect("b local v2")
            .expect("b data")
            .bytes,
        b"version two bytes"
    );
    assert_eq!(
        dfs_c
            .request_remote_object_read(&object_id)
            .expect("c requests v2"),
        vec![node_a.clone()]
    );
    dfs_a.process_incoming().expect("a serves c v2");
    let c_v2_events = dfs_c.process_incoming().expect("c stores v2");
    assert_eq!(
        c_v2_events
            .iter()
            .filter(
                |event| matches!(event, NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed,
                result: DfsReadResult::Found(_),
                ..
            } if completed == &object_id)
            )
            .count(),
        1
    );
    assert_eq!(
        c_v2_events
            .iter()
            .filter(|event| matches!(
                event,
                NetworkedDfsServiceEvent::RemoteReadCompleted {
                    object_id: completed,
                    result: DfsReadResult::Rejected { .. },
                    ..
                } if completed == &object_id
            ))
            .count(),
        0
    );
    assert_eq!(
        dfs_c
            .read_local_object(&object_id)
            .expect("c remote v2")
            .expect("c data")
            .bytes,
        b"version two bytes"
    );
    let c_cached_v2 = dfs_c
        .metadata()
        .object(&object_id)
        .expect("c cached v2 metadata");
    assert_eq!(
        c_cached_v2.current_locations,
        vec![node_a.clone(), node_b.clone()]
    );
    let c_backend_v2 = dfs_c
        .metadata_backend()
        .expect("c metadata backend")
        .get_object(&object_id)
        .expect("c backend v2 object")
        .expect("c backend v2 visible object");
    assert_eq!(
        c_backend_v2.current_locations,
        vec![node_a.clone(), node_b.clone()]
    );

    let write_bc = dfs_b
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_3.clone(),
                requesting_node_id: node_b.clone(),
            },
            b"version three bytes".to_vec(),
            vec![node_b.clone(), node_c.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("write b+c");
    assert!(matches!(
        write_bc,
        NetworkedDfsWriteOutcome::Pending {
            durable_receipts: 1,
            required_receipts: 2,
            ..
        }
    ));
    assert!(dfs_c
        .process_incoming()
        .expect("c stages v3")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobStaged { object_id: staged, .. } if staged == &object_id)));
    assert!(dfs_b
        .process_incoming()
        .expect("b commits v3")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobStaged { object_id: staged, .. } if staged == &object_id)));
    assert!(dfs_c
        .process_incoming()
        .expect("c applies v3 commit")
        .iter()
        .any(|event| matches!(event, NetworkedDfsServiceEvent::VersionCommitted { object_id: committed, .. } if committed == &object_id)));

    let committed_v3 = dfs_b
        .metadata()
        .object(&object_id)
        .expect("b v3 metadata")
        .clone();
    assert_eq!(
        committed_v3.current_locations,
        vec![node_b.clone(), node_c.clone()]
    );
    let backend_v3 = dfs_b
        .metadata_backend()
        .expect("b metadata backend")
        .get_object(&object_id)
        .expect("backend v3 object")
        .expect("backend v3 visible object");
    assert_eq!(backend_v3.version, version_3);
    assert_eq!(
        backend_v3.current_locations,
        vec![node_b.clone(), node_c.clone()]
    );
    dfs_a
        .add_object_metadata(committed_v3)
        .expect("a receives v3 metadata");
    assert!(matches!(
        dfs_a
            .read_local_object_with_state(&object_id, false)
            .expect("a sees remote-only v3"),
        NetworkedDfsReadOutcome::PendingReplication { version, .. } if version == version_3
    ));
    assert_eq!(
        dfs_a
            .request_remote_object_read(&object_id)
            .expect("a requests v3"),
        vec![node_b.clone()]
    );
    dfs_b.process_incoming().expect("b serves a v3");
    dfs_a.process_incoming().expect("a stores v3");
    for (name, dfs) in [("a", &dfs_a), ("b", &dfs_b), ("c", &dfs_c)] {
        let read = dfs
            .read_local_object(&object_id)
            .unwrap_or_else(|error| panic!("{name} reads v3: {error}"))
            .unwrap_or_else(|| panic!("{name} missing v3 data"));
        assert_eq!(read.version, version_3);
        assert_eq!(read.bytes, b"version three bytes");
    }

    let stale_writer = dfs_a
        .write_distributed_eventual_with_base(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_4,
                requesting_node_id: node_a.clone(),
            },
            Some(version_2),
            b"stale writer bytes".to_vec(),
            vec![node_a.clone(), node_b],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("stale writer is handled");
    assert!(matches!(
        stale_writer,
        NetworkedDfsWriteOutcome::Conflict { conflict }
            if conflict.remote_version == version_3
                && conflict.local_version == DfsVersion::from_legacy_named("version_4").expect("version")
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_replica_accepts_second_version_for_same_object() {
    // Regression: a replica records an active stage when it receives content, but
    // must clear it once the version commits. Otherwise a later write of a new
    // version of the same object to the same replica is wrongly rejected as a
    // "conflicting active DFS stage", and the per-object stage map grows forever.
    let owner = NodeId::from_legacy_named("node_owner_seq").expect("node");
    let replica = NodeId::from_legacy_named("node_replica_seq").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_seq_versions").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let root = temp_root("networked_replica_second_version");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "sequential.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone(), replica.clone()],
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    for dfs in [&mut owner_dfs, &mut replica_dfs] {
        dfs.add_file_system(file_system.clone())
            .expect("filesystem");
        dfs.add_object_metadata(metadata.clone()).expect("object");
    }

    // First replicated write commits v2 to {owner, replica}.
    assert!(matches!(
        owner_dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    target_version: version_2.clone(),
                    requesting_node_id: owner.clone(),
                },
                b"version two bytes".to_vec(),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect("write v2"),
        NetworkedDfsWriteOutcome::Pending { .. }
    ));
    replica_dfs.process_incoming().expect("replica stages v2");
    owner_dfs.process_incoming().expect("owner commits v2");
    replica_dfs
        .process_incoming()
        .expect("replica applies v2 commit");
    assert_eq!(
        replica_dfs
            .read_local_object(&object_id)
            .expect("replica v2")
            .expect("v2 data")
            .bytes,
        b"version two bytes"
    );

    // Second replicated write to the SAME replica set must not be rejected by a
    // stale active-stage guard left over from v2.
    assert!(matches!(
        owner_dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id,
                    object_id: object_id.clone(),
                    target_version: version_3.clone(),
                    requesting_node_id: owner.clone(),
                },
                b"version three bytes".to_vec(),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect("write v3"),
        NetworkedDfsWriteOutcome::Pending { .. }
    ));
    let replica_v3_events = replica_dfs
        .process_incoming()
        .expect("replica handles v3 stage");
    assert!(
        !replica_v3_events.iter().any(|event| matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected { object_id: rejected, .. }
                if rejected == &object_id
        )),
        "replica must accept the v3 stage after v2 committed: {replica_v3_events:?}"
    );
    assert!(replica_v3_events.iter().any(|event| matches!(
        event,
        NetworkedDfsServiceEvent::BlobStaged { object_id: staged, .. } if staged == &object_id
    )));
    owner_dfs.process_incoming().expect("owner commits v3");
    replica_dfs
        .process_incoming()
        .expect("replica applies v3 commit");
    assert_eq!(
        replica_dfs
            .read_local_object(&object_id)
            .expect("replica v3")
            .expect("v3 data")
            .bytes,
        b"version three bytes"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_confirmed_write_keeps_strict_threshold_when_owner_is_added() {
    // A replicated-confirmed write must wait for every desired replica. When the
    // owner is not in the caller's replica set it is injected by normalization,
    // and the commit threshold must grow with the set so the write does not commit
    // before all replicas are durable.
    let owner = NodeId::from_legacy_named("node_confirmed_owner").expect("node");
    let replica_b = NodeId::from_legacy_named("node_confirmed_b").expect("node");
    let replica_c = NodeId::from_legacy_named("node_confirmed_c").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_confirmed_strict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_confirmed_strict_threshold");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut dfs_b = NetworkedDfsService::new(
        network.add_node(replica_b.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("b")).expect("b store"),
    );
    let mut dfs_c = NetworkedDfsService::new(
        network.add_node(replica_c.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("c")).expect("c store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "confirmed.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone(), replica_b.clone(), replica_c.clone()],
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedConfirmed,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    for dfs in [&mut owner_dfs, &mut dfs_b, &mut dfs_c] {
        dfs.add_file_system(file_system.clone())
            .expect("filesystem");
        dfs.add_object_metadata(metadata.clone()).expect("object");
    }

    // Owner is deliberately excluded from the requested replica set; normalization
    // injects it, widening the set from 2 to 3.
    let outcome = owner_dfs
        .write_replicated_confirmed(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"confirmed bytes".to_vec(),
            vec![replica_b.clone(), replica_c.clone()],
        )
        .expect("confirmed write starts");
    assert!(
        matches!(
            outcome,
            NetworkedDfsWriteOutcome::Pending {
                durable_receipts: 1,
                required_receipts: 3,
                ..
            }
        ),
        "confirmed write must require all 3 replicas after the owner is added: {outcome:?}"
    );

    // Only one replica confirms: still must not commit (owner + b = 2 of 3).
    dfs_b.process_incoming().expect("b stages");
    owner_dfs
        .process_incoming()
        .expect("owner records b receipt");
    assert_eq!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("owner object")
            .version,
        version_1,
        "confirmed write must not commit before every replica confirms"
    );

    // Last replica confirms: now it commits.
    dfs_c.process_incoming().expect("c stages");
    owner_dfs
        .process_incoming()
        .expect("owner records c receipt and commits");
    assert_eq!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("owner object committed")
            .version,
        version_2,
        "confirmed write commits once all replicas confirm"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_cross_staged_concurrent_writers_before_split_brain_commit() {
    let node_a = NodeId::from_legacy_named("node_concurrent_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_concurrent_b").expect("node");
    let node_c = NodeId::from_legacy_named("node_concurrent_c").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_concurrent_writers").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_b = DfsVersion::from_legacy_named("version_b").expect("version");
    let version_c = DfsVersion::from_legacy_named("version_c").expect("version");
    let root = temp_root("networked_concurrent_writers_no_split_brain");
    let network = InProcessNetwork::new();
    let mut dfs_a = NetworkedDfsService::new(
        network.add_node(node_a.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("a")).expect("node a store"),
    );
    let mut dfs_b = NetworkedDfsService::new(
        network.add_node(node_b.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("b")).expect("node b store"),
    );
    let mut dfs_c = NetworkedDfsService::new(
        network.add_node(node_c.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("c")).expect("node c store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: node_a.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "concurrent.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: node_a.clone(),
        current_locations: vec![node_b.clone(), node_c.clone()],
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    for dfs in [&mut dfs_a, &mut dfs_b, &mut dfs_c] {
        dfs.add_file_system(file_system.clone())
            .expect("filesystem");
        dfs.add_object_metadata(metadata.clone()).expect("object");
    }

    assert!(matches!(
        dfs_b
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    target_version: version_b.clone(),
                    requesting_node_id: node_b.clone(),
                },
                b"writer b bytes".to_vec(),
                vec![node_b.clone(), node_c.clone()],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect("writer b starts"),
        NetworkedDfsWriteOutcome::Pending { .. }
    ));
    assert!(matches!(
        dfs_c
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id,
                    object_id: object_id.clone(),
                    target_version: version_c.clone(),
                    requesting_node_id: node_c.clone(),
                },
                b"writer c bytes".to_vec(),
                vec![node_c.clone(), node_b.clone()],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect("writer c starts"),
        NetworkedDfsWriteOutcome::Pending { .. }
    ));

    let c_events = dfs_c.process_incoming().expect("c rejects b stage");
    assert!(c_events.iter().any(|event| matches!(
        event,
        NetworkedDfsServiceEvent::BlobStageRejected {
            object_id: rejected,
            reason,
            ..
        } if rejected == &object_id && reason.contains("conflicting pending DFS stage")
    )));
    assert!(!c_events.iter().any(|event| matches!(
        event,
        NetworkedDfsServiceEvent::VersionCommitted { object_id: committed, .. }
            if committed == &object_id
    )));

    let b_events = dfs_b.process_incoming().expect("b rejects c stage");
    assert!(b_events.iter().any(|event| matches!(
        event,
        NetworkedDfsServiceEvent::BlobStageRejected {
            object_id: rejected,
            reason,
            ..
        } if rejected == &object_id && reason.contains("conflicting pending DFS stage")
    )));
    assert!(!b_events.iter().any(|event| matches!(
        event,
        NetworkedDfsServiceEvent::VersionCommitted { object_id: committed, .. }
            if committed == &object_id
    )));

    let c_response_events = dfs_c
        .process_incoming()
        .expect("c handles b rejection response");
    assert!(c_response_events.iter().any(|event| matches!(
        event,
        NetworkedDfsServiceEvent::BlobStageRejected {
            object_id: rejected,
            reason,
            ..
        } if rejected == &object_id && reason.contains("conflicting pending DFS stage")
    )));
    assert!(!c_response_events.iter().any(|event| matches!(
        event,
        NetworkedDfsServiceEvent::VersionCommitted { object_id: committed, .. }
            if committed == &object_id
    )));

    for (name, dfs) in [("b", &dfs_b), ("c", &dfs_c)] {
        let object = dfs
            .metadata()
            .object(&object_id)
            .unwrap_or_else(|| panic!("{name} object metadata"));
        assert_eq!(
            object.version, version_1,
            "{name} must not commit a competing version after cross-staging"
        );
        assert_eq!(object.content_hash, None);
    }

    cleanup(&root);
}

#[test]
fn networked_dfs_read_state_distinguishes_pending_and_stale_cache() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_read_state");
    let network = InProcessNetwork::new();
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let initial = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone(), replica.clone()],
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    replica_dfs
        .add_file_system(file_system)
        .expect("filesystem");
    replica_dfs
        .put_local_object_version(initial, b"old cached bytes")
        .expect("seed stale cache");

    replica_dfs
        .add_object_metadata(DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: "artifact.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: owner,
            current_locations: vec![NodeId::from_legacy_named("node_owner").expect("node")],
            content_hash: Some(format!("blake3:{}", blake3::hash(b"new bytes").to_hex())),
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version_2.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        })
        .expect("metadata update");

    let pending = replica_dfs
        .read_local_object_with_state(&object_id, false)
        .expect("pending read");
    assert!(matches!(
        pending,
        NetworkedDfsReadOutcome::PendingReplication {
            version,
            ..
        } if version == version_2
    ));

    let stale = replica_dfs
        .read_local_object_with_state(&object_id, true)
        .expect("stale read");
    assert!(matches!(
        stale,
        NetworkedDfsReadOutcome::Found {
            data,
            freshness: NetworkedDfsReadFreshness::Stale {
                current_version,
                ..
            },
        } if data.version == version_1
            && data.bytes == b"old cached bytes"
            && current_version == version_2
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_deduplicates_inflight_remote_object_reads() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_read_dedupe");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id,
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");
    owner_dfs
        .put_local_object_version(metadata.clone(), b"remote bytes")
        .expect("seed owner");
    reader_dfs
        .add_object_metadata(
            owner_dfs
                .metadata()
                .object(&object_id)
                .expect("owner metadata")
                .clone(),
        )
        .expect("replicate metadata");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("first request"),
        vec![owner.clone()]
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("deduped request"),
        vec![owner.clone()]
    );
    let owner_events = owner_dfs.process_incoming().expect("owner handles reads");
    assert_eq!(
        owner_events
            .iter()
            .filter(|event| matches!(
                event,
                NetworkedDfsServiceEvent::RemoteReadServed {
                    object_id: served_object_id,
                    result: sinomo_dfs::DfsReadResult::Found(_),
                    ..
                } if served_object_id == &object_id
            ))
            .count(),
        1
    );
    reader_dfs
        .process_incoming()
        .expect("reader handles response");
    assert!(matches!(
        reader_dfs
            .read_local_object_with_state(&object_id, false)
            .expect("cached remote read"),
        NetworkedDfsReadOutcome::Found { data, .. } if data.bytes == b"remote bytes"
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_ignores_stale_remote_read_responses_after_metadata_advances() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_read_stale_response");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");
    owner_dfs
        .put_local_object_version(metadata, b"old remote bytes")
        .expect("seed owner");
    let mut reader_metadata = owner_dfs
        .metadata()
        .object(&object_id)
        .expect("owner metadata")
        .clone();
    reader_dfs
        .add_object_metadata(reader_metadata.clone())
        .expect("replicate metadata");
    reader_dfs
        .request_remote_object_read(&object_id)
        .expect("request old version");
    owner_dfs
        .process_incoming()
        .expect("owner sends old version");

    reader_metadata.version = version_2.clone();
    reader_metadata.content_hash = Some(format!("blake3:{}", blake3::hash(b"new bytes").to_hex()));
    reader_metadata.current_locations = vec![owner.clone()];
    reader_dfs
        .add_object_metadata(reader_metadata)
        .expect("metadata advances before response");
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("advanced metadata starts a fresh request"),
        vec![owner.clone()]
    );
    let owner_events = owner_dfs
        .process_incoming()
        .expect("owner receives fresh read request");
    assert_eq!(
        owner_events
            .iter()
            .filter(|event| matches!(
                event,
                NetworkedDfsServiceEvent::RemoteReadServed {
                    object_id: served_object_id,
                    ..
                } if served_object_id == &object_id
            ))
            .count(),
        1
    );
    let events = reader_dfs
        .process_incoming()
        .expect("stale response is handled");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed_object_id,
                result: sinomo_dfs::DfsReadResult::Rejected { reason },
                ..
            } if completed_object_id == &object_id && reason.contains("stale DFS read response")
        )
    }));
    assert!(matches!(
        reader_dfs
            .read_local_object_with_state(&object_id, false)
            .expect("current object is still pending"),
        NetworkedDfsReadOutcome::PendingReplication { version, .. } if version == version_2
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_ignores_stale_remote_read_responses_after_backend_metadata_advances() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_backend_advanced").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_read_backend_stale_response");
    let network = InProcessNetwork::new();
    let reader_backend_path = root.join("reader-meta");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let reader_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("reader-shard"),
        &reader_backend_path,
    )
    .expect("reader metadata backend");
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    )
    .with_metadata_backend(reader_backend)
    .expect("reader metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system.clone())
        .expect("reader filesystem");
    owner_dfs
        .put_local_object_version(metadata, b"old remote bytes")
        .expect("seed owner");
    let mut reader_metadata = owner_dfs
        .metadata()
        .object(&object_id)
        .expect("owner metadata")
        .clone();
    reader_dfs
        .add_object_metadata(reader_metadata.clone())
        .expect("replicate metadata");
    reader_dfs
        .request_remote_object_read(&object_id)
        .expect("request old version");
    owner_dfs
        .process_incoming()
        .expect("owner sends old version");

    reader_metadata.version = version_2.clone();
    reader_metadata.content_hash = Some(content_hash(b"new backend bytes"));
    reader_metadata.content_size_bytes = Some(b"new backend bytes".len() as u64);
    reader_metadata.current_locations = vec![owner.clone()];
    LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("reader-shard"),
        &reader_backend_path,
    )
    .expect("external reader metadata backend")
    .put_object(&reader_metadata)
    .expect("advance backend metadata");

    let events = reader_dfs
        .process_incoming()
        .expect("stale backend response is handled");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed_object_id,
                result: DfsReadResult::Rejected { reason },
                ..
            } if completed_object_id == &object_id
                && reason.contains("stale DFS read response: object metadata advanced")
        )
    }));
    assert_eq!(
        reader_dfs
            .metadata_backend()
            .expect("reader backend")
            .get_object(&object_id)
            .expect("backend object")
            .expect("backend object")
            .version,
        version_2
    );
    assert!(matches!(
        reader_dfs
            .read_local_object_with_state(&object_id, false)
            .expect("backend advanced object is still pending locally"),
        NetworkedDfsReadOutcome::PendingReplication { version, .. } if version == version_2
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_remote_read_planning_uses_backend_current_locations() {
    let old_owner = NodeId::from_legacy_named("node_old_owner").expect("node");
    let new_owner = NodeId::from_legacy_named("node_new_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_backend_locations").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_read_backend_current_locations");
    let network = InProcessNetwork::new();
    let mut old_owner_transport = network.add_node(old_owner.clone());
    let mut new_owner_transport = network.add_node(new_owner.clone());
    let reader_backend_path = root.join("reader-meta");
    let reader_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("reader-shard"),
        &reader_backend_path,
    )
    .expect("reader metadata backend");
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    )
    .with_metadata_backend(reader_backend)
    .expect("reader metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: old_owner.clone(),
        subtree_policies: Vec::new(),
    };
    let memory_object = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "backend-locations.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: old_owner.clone(),
        current_locations: vec![old_owner.clone()],
        content_hash: Some(content_hash(b"old bytes")),
        content_size_bytes: Some(b"old bytes".len() as u64),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    reader_dfs
        .add_file_system(file_system.clone())
        .expect("reader filesystem");
    reader_dfs
        .add_object_metadata(memory_object.clone())
        .expect("reader stale memory object");

    let mut backend_object = memory_object;
    backend_object.version = version_2.clone();
    backend_object.content_hash = Some(content_hash(b"new bytes"));
    backend_object.content_size_bytes = Some(b"new bytes".len() as u64);
    backend_object.owner_node_id = new_owner.clone();
    backend_object.current_locations = vec![new_owner.clone()];
    LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("reader-shard"),
        &reader_backend_path,
    )
    .expect("external reader metadata backend")
    .put_object(&backend_object)
    .expect("advance backend object locations");

    assert!(matches!(
        reader_dfs
            .read_local_object_with_state(&object_id, false)
            .expect("backend object is authoritative for read state"),
        NetworkedDfsReadOutcome::PendingReplication {
            version,
            current_locations,
            ..
        } if version == version_2 && current_locations == vec![new_owner.clone()]
    ));
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("request follows backend locations"),
        vec![new_owner.clone()]
    );
    assert!(
        old_owner_transport
            .drain_incoming()
            .expect("old owner mailbox")
            .is_empty(),
        "remote read planning must not use stale in-memory locations"
    );
    assert_eq!(
        new_owner_transport
            .drain_incoming()
            .expect("new owner mailbox")
            .len(),
        1
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_wrong_source_remote_read_response_without_consuming_request() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let attacker = NodeId::from_legacy_named("node_attacker").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_read_wrong_source");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    let mut attacker_transport = network.add_node(attacker.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id,
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");
    owner_dfs
        .put_local_object_version(metadata, b"owner bytes")
        .expect("seed owner");
    let reader_metadata = owner_dfs
        .metadata()
        .object(&object_id)
        .expect("owner metadata")
        .clone();
    reader_dfs
        .add_object_metadata(reader_metadata.clone())
        .expect("replicate metadata");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("request read"),
        vec![owner.clone()]
    );
    let request_id = "dfs_net_req_1".to_string();
    let forged_response = DfsNetworkMessage::ReadResponse(DfsReadResponse {
        request_id: request_id.clone(),
        object_id: object_id.clone(),
        result: DfsReadResult::Found(DfsReadData {
            object_id: object_id.clone(),
            version,
            content_hash: reader_metadata.content_hash.clone(),
            bytes: b"forged bytes".to_vec(),
        }),
    });
    let protocol_message =
        ProtocolMessage::encode(DFS_PROTOCOL, &forged_response).expect("protocol message");
    attacker_transport
        .send(
            &reader,
            NetworkFrame {
                frame_id: NetworkFrameId(1),
                source_node_id: attacker.clone(),
                target_node_id: reader.clone(),
                message: MeshMessage {
                    message_id: MeshMessageId(1),
                    origin_node_id: attacker.clone(),
                    target_node_id: reader.clone(),
                    body: MeshMessageBody::Protocol(protocol_message),
                },
            },
        )
        .expect("send forged response");
    let forged_events = reader_dfs
        .process_incoming()
        .expect("reader rejects forged response");
    assert!(forged_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: completed_request_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
                ..
            } if completed_request_id == &request_id
                && source_node_id == &attacker
                && reason.contains("unexpected DFS read response source")
        )
    }));
    assert!(matches!(
        reader_dfs
            .read_local_object_with_state(&object_id, false)
            .expect("forged bytes not stored"),
        NetworkedDfsReadOutcome::PendingReplication { .. }
    ));

    owner_dfs
        .process_incoming()
        .expect("owner serves real read");
    reader_dfs
        .process_incoming()
        .expect("reader accepts real response");
    assert!(matches!(
        reader_dfs
            .read_local_object_with_state(&object_id, false)
            .expect("real bytes stored"),
        NetworkedDfsReadOutcome::Found { data, .. } if data.bytes == b"owner bytes"
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_write_uses_backend_current_metadata_over_transient_memory() {
    let node = NodeId::from_legacy_named("node_writer").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_backend_write_current").expect("object");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let version_4 = DfsVersion::from_legacy_named("version_4").expect("version");
    let root = temp_root("networked_write_backend_current");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("writer")).expect("writer store"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("writer-shard"), root.join("meta"))
            .expect("metadata backend"),
    )
    .expect("metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: node.clone(),
        subtree_policies: Vec::new(),
    };
    dfs.add_file_system(file_system).expect("filesystem");
    let backend_current = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: node.clone(),
        current_locations: vec![node.clone()],
        content_hash: Some(content_hash(b"backend current")),
        content_size_bytes: Some(15),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_2.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    dfs.add_object_metadata(backend_current.clone())
        .expect("backend object");
    let mut transient_memory = backend_current.clone();
    transient_memory.version = version_3.clone();
    transient_memory.content_hash = Some(content_hash(b"transient local"));
    transient_memory.content_size_bytes = Some(15);
    dfs.add_transient_object_metadata(transient_memory)
        .expect("transient memory object");

    let outcome = dfs
        .write_distributed_eventual_with_base(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_4,
                requesting_node_id: node.clone(),
            },
            Some(version_3),
            b"must not publish from transient memory".to_vec(),
            vec![node.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect("write outcome");
    assert!(matches!(
        outcome,
        NetworkedDfsWriteOutcome::Conflict { conflict }
            if conflict.remote_version == version_2
    ));
    assert_eq!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .get_object(&object_id)
            .expect("backend object")
            .expect("backend object")
            .version,
        version_2
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_stale_commit_after_backend_metadata_advances() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_stale_commit").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let root = temp_root("networked_stale_commit_backend_current");
    let network = InProcessNetwork::new();
    let mut owner_transport = network.add_node(owner.clone());
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("replica-shard"), root.join("meta"))
            .expect("metadata backend"),
    )
    .expect("metadata backend");
    replica_dfs
        .add_file_system(DfsFileSystem {
            file_system_id: file_system_id.clone(),
            display_name: "repo".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        })
        .expect("filesystem");
    let v3_manifest = replica_dfs
        .content()
        .put_object_version(object_id.as_str(), version_3.as_str(), b"version three")
        .expect("v3 content");
    let mut current_metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone(), replica.clone()],
        content_hash: Some(v3_manifest.content_hash.clone()),
        content_size_bytes: Some(v3_manifest.size_bytes),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_3.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    replica_dfs
        .add_object_metadata(current_metadata.clone())
        .expect("current backend metadata");
    let v2_manifest = replica_dfs
        .content()
        .put_object_version(object_id.as_str(), version_2.as_str(), b"version two")
        .expect("stale v2 content");
    current_metadata.version = version_2.clone();
    current_metadata.content_hash = Some(v2_manifest.content_hash.clone());
    current_metadata.content_size_bytes = Some(v2_manifest.size_bytes);

    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::CommitVersion(DfsCommitVersionCommand {
            request_id: "dfs_net_req_stale_commit".to_string(),
            intent_id: "dfs_intent_stale_commit".to_string(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            expected_base_version: Some(version_1),
            version: version_2.clone(),
            content_hash: v2_manifest.content_hash,
            object_metadata: Some(current_metadata),
        }),
    );
    let events = replica_dfs
        .process_incoming()
        .expect("stale commit is rejected");
    assert!(
        events.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::RepairRejected {
                    object_id: rejected,
                    reason,
                    ..
                } if rejected == &object_id && reason.contains("stale DFS commit base")
            )
        }),
        "expected stale commit rejection, got {events:?}"
    );
    assert_eq!(
        replica_dfs
            .metadata_backend()
            .expect("backend")
            .get_object(&object_id)
            .expect("backend object")
            .expect("backend object")
            .version,
        version_3
    );
    assert_eq!(
        replica_dfs
            .read_local_object(&object_id)
            .expect("read")
            .expect("current data")
            .bytes,
        b"version three"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_stale_stage_after_backend_metadata_advances() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_stale_stage").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let root = temp_root("networked_stale_stage_backend_current");
    let network = InProcessNetwork::new();
    let mut owner_transport = network.add_node(owner.clone());
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("replica-shard"), root.join("meta"))
            .expect("metadata backend"),
    )
    .expect("metadata backend");
    replica_dfs
        .add_file_system(DfsFileSystem {
            file_system_id: file_system_id.clone(),
            display_name: "repo".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        })
        .expect("filesystem");
    let v3_manifest = replica_dfs
        .content()
        .put_object_version(object_id.as_str(), version_3.as_str(), b"version three")
        .expect("v3 content");
    let mut stale_metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone(), replica.clone()],
        content_hash: Some(v3_manifest.content_hash.clone()),
        content_size_bytes: Some(v3_manifest.size_bytes),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_3.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    replica_dfs
        .add_object_metadata(stale_metadata.clone())
        .expect("current backend metadata");
    let stale_bytes = b"version two";
    stale_metadata.version = version_2.clone();
    stale_metadata.content_hash = Some(content_hash(stale_bytes));
    stale_metadata.content_size_bytes = Some(stale_bytes.len() as u64);

    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
            request_id: "dfs_net_req_stale_stage".to_string(),
            intent_id: "dfs_intent_stale_stage".to_string(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            expected_base_version: Some(version_1),
            version: version_2.clone(),
            content_hash: content_hash(stale_bytes),
            bytes: stale_bytes.to_vec(),
            object_metadata: Some(stale_metadata),
        }),
    );
    let events = replica_dfs
        .process_incoming()
        .expect("stale stage is rejected");
    assert!(
        events.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::BlobStageRejected {
                    object_id: rejected,
                    reason,
                    ..
                } if rejected == &object_id && reason.contains("stale DFS stage base")
            )
        }),
        "expected stale stage rejection, got {events:?}"
    );
    assert!(
        replica_dfs
            .metadata_backend()
            .expect("backend")
            .receipts_for_version(&object_id, &version_2)
            .expect("receipts")
            .is_empty(),
        "stale stage must not create a durable receipt"
    );
    assert!(matches!(
        replica_dfs.read_local_object_with_state(&object_id, false),
        Ok(NetworkedDfsReadOutcome::Found {
            data,
            ..
        }) if data.version == version_3 && data.bytes == b"version three"
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_stage_from_stale_owner_after_backend_handoff() {
    let old_owner = NodeId::from_legacy_named("node_old_owner").expect("node");
    let new_owner = NodeId::from_legacy_named("node_new_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_old_owner").expect("object");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let root = temp_root("networked_stage_stale_owner_backend_current");
    let network = InProcessNetwork::new();
    let mut old_owner_transport = network.add_node(old_owner.clone());
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("replica-shard"), root.join("meta"))
            .expect("metadata backend"),
    )
    .expect("metadata backend");
    replica_dfs
        .add_file_system(DfsFileSystem {
            file_system_id: file_system_id.clone(),
            display_name: "repo".to_string(),
            owner_node_id: new_owner.clone(),
            subtree_policies: Vec::new(),
        })
        .expect("filesystem");
    let current_manifest = replica_dfs
        .content()
        .put_object_version(object_id.as_str(), version_2.as_str(), b"current bytes")
        .expect("current content");
    let backend_current = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: new_owner.clone(),
        current_locations: vec![new_owner.clone(), replica.clone()],
        content_hash: Some(current_manifest.content_hash.clone()),
        content_size_bytes: Some(current_manifest.size_bytes),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_2.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    replica_dfs
        .add_object_metadata(backend_current.clone())
        .expect("backend current metadata");
    let mut stale_memory = backend_current;
    stale_memory.owner_node_id = old_owner.clone();
    stale_memory.current_locations = vec![old_owner.clone(), replica.clone()];
    replica_dfs
        .add_transient_object_metadata(stale_memory.clone())
        .expect("stale memory metadata");

    let stale_bytes = b"old owner next version";
    stale_memory.version = version_3.clone();
    stale_memory.content_hash = Some(content_hash(stale_bytes));
    stale_memory.content_size_bytes = Some(stale_bytes.len() as u64);
    send_dfs_test_message(
        &mut old_owner_transport,
        &old_owner,
        &replica,
        DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
            request_id: "dfs_net_req_stale_owner_stage".to_string(),
            intent_id: "dfs_intent_stale_owner_stage".to_string(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            expected_base_version: Some(version_2.clone()),
            version: version_3.clone(),
            content_hash: content_hash(stale_bytes),
            bytes: stale_bytes.to_vec(),
            object_metadata: Some(stale_memory),
        }),
    );

    let events = replica_dfs
        .process_incoming()
        .expect("stale owner stage is rejected");
    assert!(
        events.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::BlobStageRejected {
                    object_id: rejected,
                    reason,
                    ..
                } if rejected == &object_id
                    && reason.contains("source is neither object owner nor current replica")
            )
        }),
        "expected stale owner rejection, got {events:?}"
    );
    assert!(
        replica_dfs
            .metadata_backend()
            .expect("backend")
            .receipts_for_version(&object_id, &version_3)
            .expect("receipts")
            .is_empty(),
        "stale owner stage must not create a durable receipt"
    );
    let backend_object = replica_dfs
        .metadata_backend()
        .expect("backend")
        .get_object(&object_id)
        .expect("backend object")
        .expect("backend object");
    assert_eq!(backend_object.owner_node_id, new_owner);
    assert_eq!(backend_object.version, version_2);

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_stale_repair_after_backend_metadata_advances() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_stale_repair").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let root = temp_root("networked_stale_repair_backend_current");
    let network = InProcessNetwork::new();
    let mut owner_transport = network.add_node(owner.clone());
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("replica-shard"), root.join("meta"))
            .expect("metadata backend"),
    )
    .expect("metadata backend");
    replica_dfs
        .add_file_system(DfsFileSystem {
            file_system_id: file_system_id.clone(),
            display_name: "repo".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        })
        .expect("filesystem");
    let v3_manifest = replica_dfs
        .content()
        .put_object_version(object_id.as_str(), version_3.as_str(), b"version three")
        .expect("v3 content");
    let mut stale_metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone(), replica.clone()],
        content_hash: Some(v3_manifest.content_hash.clone()),
        content_size_bytes: Some(v3_manifest.size_bytes),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_3.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    replica_dfs
        .add_object_metadata(stale_metadata.clone())
        .expect("current backend metadata");
    let stale_bytes = b"version two repair";
    stale_metadata.version = version_2.clone();
    stale_metadata.content_hash = Some(content_hash(stale_bytes));
    stale_metadata.content_size_bytes = Some(stale_bytes.len() as u64);

    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
            request_id: "dfs_net_req_stale_repair".to_string(),
            intent_id: "dfs_intent_stale_repair".to_string(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            expected_base_version: Some(version_1),
            version: version_2.clone(),
            content_hash: content_hash(stale_bytes),
            bytes: stale_bytes.to_vec(),
            object_metadata: Some(stale_metadata),
        }),
    );
    let events = replica_dfs
        .process_incoming()
        .expect("stale repair is rejected");
    assert!(
        events.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::RepairRejected {
                    object_id: rejected,
                    reason,
                    ..
                } if rejected == &object_id && reason.contains("stale DFS repair base")
            )
        }),
        "expected stale repair rejection, got {events:?}"
    );
    assert!(
        replica_dfs
            .metadata_backend()
            .expect("backend")
            .receipts_for_version(&object_id, &version_2)
            .expect("receipts")
            .is_empty(),
        "stale repair must not create a durable receipt"
    );
    assert!(matches!(
        replica_dfs.read_local_object_with_state(&object_id, false),
        Ok(NetworkedDfsReadOutcome::Found {
            data,
            ..
        }) if data.version == version_3 && data.bytes == b"version three"
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_remote_read_accepts_backend_current_despite_stale_memory() {
    let old_owner = NodeId::from_legacy_named("node_old_owner").expect("node");
    let new_owner = NodeId::from_legacy_named("node_new_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_backend_response_current").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_read_backend_current_response");
    let network = InProcessNetwork::new();
    let _old_owner_transport = network.add_node(old_owner.clone());
    let mut new_owner_dfs = NetworkedDfsService::new(
        network.add_node(new_owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("new-owner")).expect("new owner store"),
    );
    let reader_backend_path = root.join("reader-meta");
    let reader_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("reader-shard"),
        &reader_backend_path,
    )
    .expect("reader metadata backend");
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    )
    .with_metadata_backend(reader_backend)
    .expect("reader metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: old_owner.clone(),
        subtree_policies: Vec::new(),
    };
    let memory_object = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "backend-response.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: old_owner.clone(),
        current_locations: vec![old_owner.clone()],
        content_hash: Some(content_hash(b"old bytes")),
        content_size_bytes: Some(b"old bytes".len() as u64),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    reader_dfs
        .add_file_system(file_system.clone())
        .expect("reader filesystem");
    new_owner_dfs
        .add_file_system(file_system)
        .expect("new owner filesystem");
    reader_dfs
        .add_object_metadata(memory_object.clone())
        .expect("reader stale memory object");

    let mut backend_object = memory_object;
    backend_object.version = version_2.clone();
    backend_object.content_hash = Some(content_hash(b"new backend bytes"));
    backend_object.content_size_bytes = Some(b"new backend bytes".len() as u64);
    backend_object.owner_node_id = new_owner.clone();
    backend_object.current_locations = vec![new_owner.clone()];
    new_owner_dfs
        .put_local_object_version(backend_object.clone(), b"new backend bytes")
        .expect("seed new owner");
    LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("reader-shard"),
        &reader_backend_path,
    )
    .expect("external reader metadata backend")
    .put_object(&backend_object)
    .expect("advance reader backend object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("request follows backend current"),
        vec![new_owner.clone()]
    );
    new_owner_dfs.process_incoming().expect("serve read");
    let events = reader_dfs
        .process_incoming()
        .expect("reader accepts backend-current response");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed,
                result: DfsReadResult::Found(_),
                ..
            } if completed == &object_id
        )
    }));
    assert_eq!(
        reader_dfs
            .read_local_object(&object_id)
            .expect("read cached")
            .expect("cached bytes")
            .bytes,
        b"new backend bytes"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_remote_read_serves_backend_only_current_metadata() {
    let owner = NodeId::from_legacy_named("node_backend_only_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_backend_only_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_backend_only_serve").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_read_backend_only_serves");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("owner-shard"),
            root.join("owner-meta"),
        )
        .expect("owner metadata backend"),
    )
    .expect("owner metadata backend");
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "backend-only.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .put_local_object_version(metadata, b"backend-only bytes")
        .expect("seed owner backend and content");
    let visible = owner_dfs
        .metadata_backend()
        .expect("owner backend")
        .get_object(&object_id)
        .expect("owner backend object")
        .expect("owner backend object");
    owner_dfs.remove_transient_object_metadata(&object_id);
    assert!(
        owner_dfs.metadata().object(&object_id).is_none(),
        "test requires backend-only owner metadata"
    );
    reader_dfs
        .add_object_metadata(visible)
        .expect("reader metadata");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("request backend-only owner"),
        vec![owner.clone()]
    );
    owner_dfs
        .process_incoming()
        .expect("backend-only owner serves read");
    let events = reader_dfs
        .process_incoming()
        .expect("reader receives backend-only read");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed,
                result: DfsReadResult::Found(_),
                ..
            } if completed == &object_id
        )
    }));
    assert_eq!(
        reader_dfs
            .read_local_object(&object_id)
            .expect("reader cached read")
            .expect("reader cached bytes")
            .bytes,
        b"backend-only bytes"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_remote_read_stores_backend_only_current_metadata() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_backend_only_read").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_read_backend_only_current");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let reader_backend_path = root.join("reader-meta");
    let reader_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("reader-shard"),
        &reader_backend_path,
    )
    .expect("reader metadata backend");
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    )
    .with_metadata_backend(reader_backend)
    .expect("reader metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "backend-only.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");
    owner_dfs
        .put_local_object_version(metadata, b"backend only bytes")
        .expect("seed owner");
    let backend_object = owner_dfs
        .metadata()
        .object(&object_id)
        .expect("owner metadata")
        .clone();
    LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("reader-shard"),
        &reader_backend_path,
    )
    .expect("external reader metadata backend")
    .put_object(&backend_object)
    .expect("reader backend object");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("request from backend-only metadata"),
        vec![owner.clone()]
    );
    owner_dfs.process_incoming().expect("owner serves read");
    let events = reader_dfs
        .process_incoming()
        .expect("reader stores backend-only read");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed,
                result: DfsReadResult::Found(_),
                ..
            } if completed == &object_id
        )
    }));
    assert_eq!(
        reader_dfs
            .read_local_object(&object_id)
            .expect("read cached object")
            .expect("cached bytes")
            .bytes,
        b"backend only bytes"
    );

    cleanup(&root);
}

#[test]
fn networked_remote_read_cache_records_size_without_rewriting_object_metadata() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_read_cache_size").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_read_cache_size_no_object_rewrite");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let reader_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("reader"), root.join("reader-meta"))
            .expect("reader backend");
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    )
    .with_metadata_backend(reader_backend)
    .expect("reader metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");
    owner_dfs
        .put_local_object_version(metadata, b"owner bytes")
        .expect("seed owner");
    let mut reader_metadata = owner_dfs
        .metadata()
        .object(&object_id)
        .expect("owner metadata")
        .clone();
    reader_metadata.content_size_bytes = None;
    reader_dfs
        .add_object_metadata(reader_metadata)
        .expect("replicate metadata");

    let before_read_sequence = reader_dfs
        .metadata_backend()
        .expect("reader backend")
        .journal_since(0)
        .expect("initial journal")
        .last()
        .map(|entry| entry.sequence)
        .unwrap_or(0);
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("request read"),
        vec![owner.clone()]
    );
    owner_dfs.process_incoming().expect("owner serves read");
    reader_dfs
        .process_incoming()
        .expect("reader stores remote read cache");

    let backend = reader_dfs.metadata_backend().expect("reader backend");
    assert_eq!(
        backend
            .journal_since(before_read_sequence)
            .expect("read cache metadata journal")
            .iter()
            .map(|entry| &entry.mutation)
            .collect::<Vec<_>>(),
        vec![&DfsMetadataMutation::PutVersion {
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            version: version.clone(),
            version_record: backend
                .get_version(&object_id, &version)
                .expect("cached version")
                .or_else(|| panic!("cached version missing")),
        }]
    );
    let backend_object = backend
        .get_object(&object_id)
        .expect("backend object")
        .expect("object");
    assert_eq!(backend_object.content_size_bytes, None);
    assert_eq!(backend_object.current_locations, vec![owner.clone()]);
    assert_eq!(
        backend
            .get_version(&object_id, &version)
            .expect("backend version")
            .expect("version")
            .size_bytes,
        Some(b"owner bytes".len() as u64)
    );
    assert_eq!(
        reader_dfs
            .metadata()
            .object(&object_id)
            .expect("reader memory object")
            .content_size_bytes,
        Some(b"owner bytes".len() as u64)
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_large_remote_read_uses_chunked_response() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_large_artifact").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_read_large_chunked");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id,
        path: "large.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    let large_bytes = (0..200_000)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");
    owner_dfs
        .put_local_object_version(metadata, &large_bytes)
        .expect("seed owner");
    reader_dfs
        .add_object_metadata(
            owner_dfs
                .metadata()
                .object(&object_id)
                .expect("owner metadata")
                .clone(),
        )
        .expect("replicate metadata");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("request large read"),
        vec![owner.clone()]
    );
    let served = owner_dfs.process_incoming().expect("owner serves chunks");
    assert!(served.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadServed {
                object_id: served_object_id,
                result: DfsReadResult::Found(DfsReadData { bytes, .. }),
                ..
            } if served_object_id == &object_id && bytes.is_empty()
        )
    }));
    let completed = reader_dfs
        .process_incoming()
        .expect("reader assembles chunks");
    assert!(completed.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed_object_id,
                result: DfsReadResult::Found(DfsReadData { bytes, .. }),
                ..
            } if completed_object_id == &object_id && bytes.is_empty()
        )
    }));
    assert!(matches!(
        reader_dfs
            .read_local_object_with_state(&object_id, false)
            .expect("large read cached"),
        NetworkedDfsReadOutcome::Found { data, .. } if data.bytes == large_bytes
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_remote_range_read_returns_slice_without_full_cache() {
    let owner = NodeId::from_legacy_named("node_range_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_range_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_range_artifact").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_read_range_slice");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id,
        path: "range.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    let chunk_size = 64 * 1024;
    let bytes = (0..(chunk_size * 258 + 17))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let offset = 70_003_u64;
    let max_bytes = 4096_usize;
    let expected = bytes[offset as usize..offset as usize + max_bytes].to_vec();
    let chunked_offset = 17_u64;
    let chunked_max_bytes = chunk_size * 256 + 4096;
    let chunked_expected =
        bytes[chunked_offset as usize..chunked_offset as usize + chunked_max_bytes].to_vec();

    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");
    owner_dfs
        .put_local_object_version(metadata, &bytes)
        .expect("seed owner");
    let owner_metadata = owner_dfs
        .metadata()
        .object(&object_id)
        .expect("owner metadata")
        .clone();
    reader_dfs
        .add_object_metadata(owner_metadata.clone())
        .expect("replicate metadata");

    assert_eq!(
        reader_dfs
            .request_remote_object_range_read(&object_id, offset, max_bytes)
            .expect("request range read"),
        vec![owner.clone()]
    );
    let served = owner_dfs.process_incoming().expect("owner serves range");
    assert!(served.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadServed {
                object_id: served_object_id,
                result: DfsReadResult::RangeFound(data),
                ..
            } if served_object_id == &object_id
                && data.object_id == object_id
                && data.version == version
                && data.content_hash == owner_metadata.content_hash
                && data.offset == offset
                && data.total_size_bytes == bytes.len() as u64
                && data.bytes == expected
        )
    }));

    let completed = reader_dfs
        .process_incoming()
        .expect("reader receives range");
    assert!(completed.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed_object_id,
                result: DfsReadResult::RangeFound(data),
                ..
            } if completed_object_id == &object_id
                && data.object_id == object_id
                && data.version == version
                && data.content_hash == owner_metadata.content_hash
                && data.offset == offset
                && data.total_size_bytes == bytes.len() as u64
                && data.bytes == expected
        )
    }));
    assert!(
        reader_dfs
            .read_local_object(&object_id)
            .expect("range read should not cache full object")
            .is_none()
    );

    assert_eq!(
        reader_dfs
            .request_remote_object_range_read(&object_id, chunked_offset, chunked_max_bytes)
            .expect("request chunked range read"),
        vec![owner.clone()]
    );
    let first_served = owner_dfs
        .process_incoming()
        .expect("owner serves first range window");
    assert!(first_served.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadServed {
                object_id: served_object_id,
                result: DfsReadResult::RangeFound(data),
                ..
            } if served_object_id == &object_id
                && data.object_id == object_id
                && data.version == version
                && data.content_hash == owner_metadata.content_hash
                && data.offset == chunked_offset
                && data.total_size_bytes == bytes.len() as u64
                && data.bytes.is_empty()
        )
    }));
    let first_reader_events = reader_dfs
        .process_incoming()
        .expect("reader accepts first range window");
    assert!(
        !first_reader_events.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::RemoteReadCompleted {
                    object_id: completed_object_id,
                    result: DfsReadResult::RangeFound(_),
                    ..
                } if completed_object_id == &object_id
            )
        }),
        "first range window must not complete the read"
    );
    let mut chunked_actual = Vec::new();
    let mut saw_range_complete = false;
    for event in &first_reader_events {
        if let NetworkedDfsServiceEvent::RemoteRangeReadChunkReceived {
            object_id: chunk_object_id,
            source_node_id,
            version: chunk_version,
            content_hash,
            range_offset,
            range_size_bytes,
            object_total_size_bytes,
            bytes: chunk_bytes,
            complete,
            ..
        } = event
        {
            assert_eq!(chunk_object_id, &object_id);
            assert_eq!(source_node_id, &owner);
            assert_eq!(chunk_version, &version);
            assert_eq!(content_hash, &owner_metadata.content_hash);
            assert_eq!(*range_offset, chunked_offset);
            assert_eq!(*range_size_bytes, chunked_expected.len() as u64);
            assert_eq!(*object_total_size_bytes, bytes.len() as u64);
            chunked_actual.extend_from_slice(chunk_bytes);
            saw_range_complete |= *complete;
        }
    }
    assert_eq!(chunked_actual.len(), chunk_size * 256);
    assert!(!saw_range_complete);

    let second_served = owner_dfs
        .process_incoming()
        .expect("owner serves continuation range window");
    assert!(
        second_served.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::RemoteReadServed {
                    object_id: served_object_id,
                    result: DfsReadResult::RangeFound(data),
                    ..
                } if served_object_id == &object_id && data.bytes.is_empty()
            )
        }),
        "reader should request a range continuation after the first window, got {second_served:?}"
    );
    let second_reader_events = reader_dfs
        .process_incoming()
        .expect("reader accepts continuation range window");
    for event in &second_reader_events {
        if let NetworkedDfsServiceEvent::RemoteRangeReadChunkReceived {
            object_id: chunk_object_id,
            bytes: chunk_bytes,
            complete,
            ..
        } = event
        {
            assert_eq!(chunk_object_id, &object_id);
            chunked_actual.extend_from_slice(chunk_bytes);
            saw_range_complete |= *complete;
        }
    }
    assert!(saw_range_complete);
    assert_eq!(chunked_actual, chunked_expected);
    assert!(
        reader_dfs
            .read_local_object(&object_id)
            .expect("chunked range read should not cache full object")
            .is_none()
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_large_remote_read_windows_and_continues() {
    let owner = NodeId::from_legacy_named("node_windowed_read_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_windowed_read_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_windowed_remote_read").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_read_windowed");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id,
        path: "windowed-read.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    let chunk_size = 64 * 1024;
    let total_chunks = 258;
    let bytes = (0..(chunk_size * (total_chunks - 1) + 17))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");
    owner_dfs
        .put_local_object_version(metadata, &bytes)
        .expect("seed owner");
    reader_dfs
        .add_object_metadata(
            owner_dfs
                .metadata()
                .object(&object_id)
                .expect("owner metadata")
                .clone(),
        )
        .expect("replicate metadata");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("request windowed read"),
        vec![owner.clone()]
    );
    let first_served = owner_dfs
        .process_incoming()
        .expect("owner serves first read window");
    assert!(first_served.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadServed {
                object_id: served_object_id,
                result: DfsReadResult::Found(DfsReadData { bytes, .. }),
                ..
            } if served_object_id == &object_id && bytes.is_empty()
        )
    }));
    let first_reader_events = reader_dfs
        .process_incoming()
        .expect("reader accepts first read window");
    assert!(
        !first_reader_events.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::RemoteReadCompleted {
                    object_id: completed_object_id,
                    result: DfsReadResult::Found(_),
                    ..
                } if completed_object_id == &object_id
            )
        }),
        "first window must not complete the read"
    );

    let second_served = owner_dfs
        .process_incoming()
        .expect("owner serves continuation read window");
    assert!(
        second_served.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::RemoteReadServed {
                    object_id: served_object_id,
                    result: DfsReadResult::Found(DfsReadData { bytes, .. }),
                    ..
                } if served_object_id == &object_id && bytes.is_empty()
            )
        }),
        "reader should request a continuation after the first window, got {second_served:?}"
    );
    let completed = reader_dfs
        .process_incoming()
        .expect("reader assembles continuation chunks");
    assert!(completed.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed_object_id,
                result: DfsReadResult::Found(DfsReadData { bytes, .. }),
                ..
            } if completed_object_id == &object_id && bytes.is_empty()
        )
    }));
    assert!(matches!(
        reader_dfs
            .read_local_object_with_state(&object_id, false)
            .expect("windowed read cached"),
        NetworkedDfsReadOutcome::Found { data, .. } if data.bytes == bytes
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_read_chunks_after_content_size_metadata_advances() {
    let owner = NodeId::from_legacy_named("node_size_advance_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_size_advance_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_size_advance_read").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_read_chunk_size_metadata_advanced");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let reader_backend_path = root.join("reader-meta");
    let reader_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("reader"), &reader_backend_path)
            .expect("reader backend");
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    )
    .with_metadata_backend(reader_backend)
    .expect("reader backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id,
        path: "size-advance.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    let large_bytes = (0..140_000)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");
    owner_dfs
        .put_local_object_version(metadata, &large_bytes)
        .expect("seed owner");
    let mut reader_object = owner_dfs
        .metadata()
        .object(&object_id)
        .expect("owner metadata")
        .clone();
    reader_dfs
        .add_object_metadata(reader_object.clone())
        .expect("reader metadata");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("request large read"),
        vec![owner.clone()]
    );
    reader_object.content_size_bytes = Some(large_bytes.len() as u64 + 1);
    LocalSobjMetadataBackend::new(DfsMetadataShardId::new("reader"), &reader_backend_path)
        .expect("external reader backend")
        .put_object(&reader_object)
        .expect("advance reader size metadata");
    owner_dfs.process_incoming().expect("owner sends chunks");
    let events = reader_dfs
        .process_incoming()
        .expect("reader rejects stale-size chunks");
    assert!(
        events.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::RemoteReadCompleted {
                    object_id: completed,
                    result: DfsReadResult::Rejected { reason },
                    ..
                } if completed == &object_id
                    && reason.contains("stale DFS read response: object metadata advanced")
            )
        }),
        "expected stale metadata rejection, got {events:?}"
    );
    assert!(
        reader_dfs
            .read_local_object(&object_id)
            .expect("read after stale chunks")
            .is_none(),
        "stale chunk response must not populate local content"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_read_chunk_size_that_disagrees_with_metadata() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_bad_read_chunk").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_read_bad_chunk_size");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    let mut owner_transport = network.add_node(owner.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id,
        path: "large.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    let large_bytes = (0..140_000)
        .map(|index| (index % 241) as u8)
        .collect::<Vec<_>>();
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");
    owner_dfs
        .put_local_object_version(metadata, &large_bytes)
        .expect("seed owner");
    let reader_metadata = owner_dfs
        .metadata()
        .object(&object_id)
        .expect("owner metadata")
        .clone();
    reader_dfs
        .add_object_metadata(reader_metadata.clone())
        .expect("replicate metadata");

    reader_dfs
        .request_remote_object_read(&object_id)
        .expect("request read");
    assert_eq!(
        owner_transport
            .drain_incoming()
            .expect("drain original read request")
            .len(),
        1
    );
    let bad_chunk = DfsNetworkMessage::ReadResponseChunk(DfsReadResponseChunk {
        request_id: "dfs_net_req_1".to_string(),
        object_id: object_id.clone(),
        version,
        content_hash: reader_metadata.content_hash,
        range_offset: None,
        object_total_size_bytes: None,
        total_size_bytes: large_bytes.len() as u64 + 1,
        chunk_index: 0,
        total_chunks: 1,
        chunk_hash: None,
        bytes: large_bytes,
    });
    let protocol_message =
        ProtocolMessage::encode(DFS_PROTOCOL, &bad_chunk).expect("protocol message");
    owner_transport
        .send(
            &reader,
            NetworkFrame {
                frame_id: NetworkFrameId(1),
                source_node_id: owner.clone(),
                target_node_id: reader.clone(),
                message: MeshMessage {
                    message_id: MeshMessageId(1),
                    origin_node_id: owner.clone(),
                    target_node_id: reader.clone(),
                    body: MeshMessageBody::Protocol(protocol_message),
                },
            },
        )
        .expect("send bad chunk");
    let events = reader_dfs
        .process_incoming()
        .expect("bad chunk size is isolated");
    assert!(
        events.iter().any(|event| matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed_object_id,
                result: DfsReadResult::Rejected { reason },
                ..
            } if completed_object_id == &object_id
                && reason.contains("chunked DFS read response size does not match object metadata")
        )),
        "expected rejected read event, got {events:?}"
    );
    assert!(
        !reader_dfs
            .metadata()
            .object(&object_id)
            .expect("reader metadata")
            .current_locations
            .contains(&owner),
        "a source that sends a chunk with impossible size metadata should be pruned"
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("bad source should not be retried"),
        Vec::<NodeId>::new()
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_read_chunk_count_that_disagrees_with_declared_size() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_bad_read_chunk_count").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_read_bad_chunk_count");
    let network = InProcessNetwork::new();
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    let mut owner_transport = network.add_node(owner.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let content_hash = format!("blake3:{}", blake3::hash(&[7_u8]).to_hex());
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");
    reader_dfs
        .add_object_metadata(DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id,
            path: "large.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: owner.clone(),
            current_locations: vec![owner.clone()],
            content_hash: Some(content_hash.clone()),
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        })
        .expect("reader metadata");
    reader_dfs
        .request_remote_object_read(&object_id)
        .expect("request read");
    assert_eq!(
        owner_transport
            .drain_incoming()
            .expect("drain original read request")
            .len(),
        1
    );
    let bad_chunk = DfsNetworkMessage::ReadResponseChunk(DfsReadResponseChunk {
        request_id: "dfs_net_req_1".to_string(),
        object_id: object_id.clone(),
        version,
        content_hash: Some(content_hash),
        range_offset: None,
        object_total_size_bytes: None,
        total_size_bytes: 1,
        chunk_index: 0,
        total_chunks: u32::MAX,
        chunk_hash: None,
        bytes: vec![7],
    });
    let protocol_message =
        ProtocolMessage::encode(DFS_PROTOCOL, &bad_chunk).expect("protocol message");
    owner_transport
        .send(
            &reader,
            NetworkFrame {
                frame_id: NetworkFrameId(1),
                source_node_id: owner.clone(),
                target_node_id: reader.clone(),
                message: MeshMessage {
                    message_id: MeshMessageId(1),
                    origin_node_id: owner.clone(),
                    target_node_id: reader.clone(),
                    body: MeshMessageBody::Protocol(protocol_message),
                },
            },
        )
        .expect("send bad chunk count");
    let events = reader_dfs
        .process_incoming()
        .expect("bad chunk count is isolated");
    assert!(
        events.iter().any(|event| matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed_object_id,
                result: DfsReadResult::Rejected { reason },
                ..
            } if completed_object_id == &object_id
                && reason.contains("chunked DFS read response chunk count does not match size")
        )),
        "expected rejected read event, got {events:?}"
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("malformed source should not be retried"),
        Vec::<NodeId>::new()
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_read_chunk_hash_mismatch() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_bad_read_chunk_hash").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_read_bad_chunk_hash");
    let network = InProcessNetwork::new();
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    let mut owner_transport = network.add_node(owner.clone());
    let bytes = b"read bytes with a corrupt chunk checksum".to_vec();
    let expected_content_hash = content_hash(&bytes);
    reader_dfs
        .add_file_system(DfsFileSystem {
            file_system_id: file_system_id.clone(),
            display_name: "repo".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        })
        .expect("reader filesystem");
    reader_dfs
        .add_object_metadata(DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id,
            path: "bad-read-chunk-hash.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: owner.clone(),
            current_locations: vec![owner.clone()],
            content_hash: Some(expected_content_hash.clone()),
            content_size_bytes: Some(bytes.len() as u64),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        })
        .expect("reader metadata");
    reader_dfs
        .request_remote_object_read(&object_id)
        .expect("request read");
    assert_eq!(
        owner_transport
            .drain_incoming()
            .expect("drain original read request")
            .len(),
        1
    );
    let bad_chunk = DfsNetworkMessage::ReadResponseChunk(DfsReadResponseChunk {
        request_id: "dfs_net_req_1".to_string(),
        object_id: object_id.clone(),
        version,
        content_hash: Some(expected_content_hash),
        range_offset: None,
        object_total_size_bytes: None,
        total_size_bytes: bytes.len() as u64,
        chunk_index: 0,
        total_chunks: 1,
        chunk_hash: Some(content_hash(b"different read chunk")),
        bytes,
    });
    send_dfs_test_message(&mut owner_transport, &owner, &reader, bad_chunk);
    let events = reader_dfs
        .process_incoming()
        .expect("bad read chunk checksum is isolated");
    assert!(
        events.iter().any(|event| matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed_object_id,
                result: DfsReadResult::Rejected { reason },
                ..
            } if completed_object_id == &object_id && reason.contains("content hash mismatch")
        )),
        "expected rejected read checksum event, got {events:?}"
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("bad checksum source should not be retried"),
        Vec::<NodeId>::new()
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_stage_chunk_count_that_disagrees_with_declared_size() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_bad_stage_chunk_count").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_stage_bad_chunk_count");
    let network = InProcessNetwork::new();
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let mut owner_transport = network.add_node(owner.clone());
    let bad_chunk = DfsNetworkMessage::StageBlobChunk(DfsStageBlobChunkCommand {
        request_id: "dfs_net_req_1".to_string(),
        intent_id: "intent_bad_stage_chunk".to_string(),
        file_system_id,
        object_id: object_id.clone(),
        expected_base_version: None,
        version,
        content_hash: content_hash(&[9]),
        total_size_bytes: 1,
        chunk_index: 0,
        total_chunks: u32::MAX,
        chunk_hash: None,
        bytes: vec![9],
        object_metadata: None,
    });
    send_dfs_test_message(&mut owner_transport, &owner, &replica, bad_chunk);
    let events = replica_dfs
        .process_incoming()
        .expect("bad stage chunk count is isolated");
    assert!(
        events.iter().any(|event| matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected {
                object_id: rejected_object_id,
                reason,
                ..
            } if rejected_object_id == &object_id
                && reason.contains("chunked DFS blob transfer chunk count does not match size")
        )),
        "expected rejected stage event, got {events:?}"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_stage_chunk_hash_mismatch() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_bad_stage_chunk_hash").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_stage_bad_chunk_hash");
    let network = InProcessNetwork::new();
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let mut owner_transport = network.add_node(owner.clone());
    let bytes = b"stage bytes with a corrupt chunk checksum".to_vec();
    let bad_chunk = DfsNetworkMessage::StageBlobChunk(DfsStageBlobChunkCommand {
        request_id: "dfs_net_req_bad_stage_chunk_hash".to_string(),
        intent_id: "intent_bad_stage_chunk_hash".to_string(),
        file_system_id,
        object_id: object_id.clone(),
        expected_base_version: None,
        version: version.clone(),
        content_hash: content_hash(&bytes),
        total_size_bytes: bytes.len() as u64,
        chunk_index: 0,
        total_chunks: 1,
        chunk_hash: Some(content_hash(b"different stage chunk")),
        bytes,
        object_metadata: None,
    });
    send_dfs_test_message(&mut owner_transport, &owner, &replica, bad_chunk);
    let events = replica_dfs
        .process_incoming()
        .expect("bad stage chunk checksum is isolated");
    assert!(
        events.iter().any(|event| matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected {
                object_id: rejected_object_id,
                reason,
                ..
            } if rejected_object_id == &object_id && reason.contains("content hash mismatch")
        )),
        "expected rejected stage checksum event, got {events:?}"
    );
    assert!(
        replica_dfs
            .content()
            .read_manifest(object_id.as_str(), version.as_str())
            .is_err(),
        "bad checksum chunks must not leave a committed manifest"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_rejects_repair_chunk_count_that_disagrees_with_declared_size() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_bad_repair_chunk_count").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_repair_bad_chunk_count");
    let network = InProcessNetwork::new();
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let mut owner_transport = network.add_node(owner.clone());
    let bad_chunk = DfsNetworkMessage::RepairReplicaChunk(DfsRepairReplicaChunkCommand {
        request_id: "dfs_net_req_1".to_string(),
        intent_id: "intent_bad_repair_chunk".to_string(),
        file_system_id,
        object_id: object_id.clone(),
        expected_base_version: None,
        version,
        content_hash: content_hash(&[11]),
        total_size_bytes: 1,
        chunk_index: 0,
        total_chunks: u32::MAX,
        chunk_hash: None,
        bytes: vec![11],
        object_metadata: None,
    });
    send_dfs_test_message(&mut owner_transport, &owner, &replica, bad_chunk);
    let events = replica_dfs
        .process_incoming()
        .expect("bad repair chunk count is isolated");
    assert!(
        events.iter().any(|event| matches!(
            event,
            NetworkedDfsServiceEvent::RepairRejected {
                object_id: rejected_object_id,
                reason,
                ..
            } if rejected_object_id == &object_id
                && reason.contains("chunked DFS blob transfer chunk count does not match size")
        )),
        "expected rejected repair event, got {events:?}"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_chunked_repair_auth_failure_notifies_sender() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let attacker = NodeId::from_legacy_named("node_attacker").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_auth").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_repair_chunk_auth_failure_response");
    let network = InProcessNetwork::new();
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let mut attacker_transport = network.add_node(attacker.clone());
    let object_metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "repair-auth.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    let command = DfsRepairReplicaChunkCommand {
        request_id: "dfs_net_req_repair_auth".to_string(),
        intent_id: "dfs_intent_repair_auth".to_string(),
        file_system_id,
        object_id: object_id.clone(),
        expected_base_version: None,
        version: version.clone(),
        content_hash: content_hash(b"repair auth bytes"),
        total_size_bytes: b"repair auth bytes".len() as u64,
        chunk_index: 0,
        total_chunks: 1,
        chunk_hash: None,
        bytes: b"repair auth bytes".to_vec(),
        object_metadata: Some(object_metadata),
    };
    send_dfs_test_message(
        &mut attacker_transport,
        &attacker,
        &replica,
        DfsNetworkMessage::RepairReplicaChunk(command),
    );

    let replica_events = replica_dfs
        .process_incoming()
        .expect("replica rejects unauthorized repair chunk");
    assert!(replica_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RepairRejected { replica_node_id, reason, .. }
                if replica_node_id == &replica && reason.contains("unauthorized DFS replica command")
        )
    }));
    let responses = attacker_transport
        .drain_incoming()
        .expect("attacker receives repair rejection response");
    assert_eq!(responses.len(), 1);
    let MeshMessageBody::Protocol(protocol) = &responses[0].frame.message.body else {
        panic!("expected protocol message");
    };
    let response: DfsNetworkMessage = protocol.decode().expect("decode repair rejection");
    assert!(matches!(
        response,
        DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
            request_id,
            replica_node_id,
            result: DfsBlobTransferResult::Rejected { reason },
            ..
        }) if request_id == "dfs_net_req_repair_auth"
            && replica_node_id == replica
            && reason.contains("unauthorized DFS replica command")
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_chunked_stage_accepts_out_of_order_chunks() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_out_of_order").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_stage_out_of_order");
    let network = InProcessNetwork::new();
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let mut owner_transport = network.add_node(owner.clone());
    let bytes = chunked_test_bytes(4_464);
    let hash = content_hash(&bytes);
    let chunk_size = TEST_DFS_CHUNK_BYTES;
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "stage-out-of-order.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    let second = DfsStageBlobChunkCommand {
        request_id: "dfs_net_req_stage_out_of_order".to_string(),
        intent_id: "intent_stage_out_of_order".to_string(),
        file_system_id: file_system_id.clone(),
        object_id: object_id.clone(),
        expected_base_version: None,
        version: version.clone(),
        content_hash: hash.clone(),
        total_size_bytes: bytes.len() as u64,
        chunk_index: 1,
        total_chunks: 2,
        chunk_hash: None,
        bytes: bytes[chunk_size..].to_vec(),
        object_metadata: Some(metadata.clone()),
    };
    let first = DfsStageBlobChunkCommand {
        chunk_index: 0,
        bytes: bytes[..chunk_size].to_vec(),
        ..second.clone()
    };

    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::StageBlobChunk(second),
    );
    assert!(
        replica_dfs
            .process_incoming()
            .expect("second chunk starts transfer")
            .is_empty()
    );
    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::StageBlobChunk(first),
    );
    let events = replica_dfs
        .process_incoming()
        .expect("first chunk completes out-of-order stage");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStaged {
                object_id: staged,
                version: staged_version,
                content_hash,
                ..
            } if staged == &object_id && staged_version == &version && content_hash == &hash
        )
    }));
    cleanup(&root);
}

#[test]
fn networked_dfs_chunked_repair_accepts_out_of_order_chunks() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_out_of_order").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_repair_out_of_order");
    let network = InProcessNetwork::new();
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let mut owner_transport = network.add_node(owner.clone());
    let bytes = chunked_test_bytes(4_464);
    let hash = content_hash(&bytes);
    let chunk_size = TEST_DFS_CHUNK_BYTES;
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "repair-out-of-order.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    let second = DfsRepairReplicaChunkCommand {
        request_id: "dfs_net_req_repair_out_of_order".to_string(),
        intent_id: "intent_repair_out_of_order".to_string(),
        file_system_id: file_system_id.clone(),
        object_id: object_id.clone(),
        expected_base_version: None,
        version: version.clone(),
        content_hash: hash.clone(),
        total_size_bytes: bytes.len() as u64,
        chunk_index: 1,
        total_chunks: 2,
        chunk_hash: None,
        bytes: bytes[chunk_size..].to_vec(),
        object_metadata: Some(metadata.clone()),
    };
    let first = DfsRepairReplicaChunkCommand {
        chunk_index: 0,
        bytes: bytes[..chunk_size].to_vec(),
        ..second.clone()
    };

    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::RepairReplicaChunk(second),
    );
    assert!(
        replica_dfs
            .process_incoming()
            .expect("second chunk starts repair transfer")
            .is_empty()
    );
    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::RepairReplicaChunk(first),
    );
    let events = replica_dfs
        .process_incoming()
        .expect("first chunk completes out-of-order repair");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::ReplicaRepaired {
                object_id: repaired,
                version: repaired_version,
                content_hash,
                ..
            } if repaired == &object_id && repaired_version == &version && content_hash == &hash
        )
    }));
    assert_eq!(
        replica_dfs
            .read_local_object(&object_id)
            .expect("repaired object read")
            .expect("repaired bytes")
            .bytes,
        bytes
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_chunked_stage_retry_uses_latest_request_id() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_retry_request").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_stage_retry_request");
    let network = InProcessNetwork::new();
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let mut owner_transport = network.add_node(owner.clone());
    let bytes = chunked_test_bytes(4_464);
    let hash = content_hash(&bytes);
    let chunk_size = TEST_DFS_CHUNK_BYTES;
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "stage-retry-request.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    let first_attempt = DfsStageBlobChunkCommand {
        request_id: "dfs_net_req_stage_retry_old".to_string(),
        intent_id: "intent_stage_retry_request".to_string(),
        file_system_id: file_system_id.clone(),
        object_id: object_id.clone(),
        expected_base_version: None,
        version: version.clone(),
        content_hash: hash.clone(),
        total_size_bytes: bytes.len() as u64,
        chunk_index: 0,
        total_chunks: 2,
        chunk_hash: None,
        bytes: bytes[..chunk_size].to_vec(),
        object_metadata: Some(metadata.clone()),
    };
    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::StageBlobChunk(first_attempt),
    );
    assert!(
        replica_dfs
            .process_incoming()
            .expect("old partial stage starts transfer")
            .is_empty()
    );

    let retry_first = DfsStageBlobChunkCommand {
        request_id: "dfs_net_req_stage_retry_new".to_string(),
        intent_id: "intent_stage_retry_request".to_string(),
        file_system_id: file_system_id.clone(),
        object_id: object_id.clone(),
        expected_base_version: None,
        version: version.clone(),
        content_hash: hash.clone(),
        total_size_bytes: bytes.len() as u64,
        chunk_index: 0,
        total_chunks: 2,
        chunk_hash: None,
        bytes: bytes[..chunk_size].to_vec(),
        object_metadata: Some(metadata.clone()),
    };
    let retry_second = DfsStageBlobChunkCommand {
        chunk_index: 1,
        bytes: bytes[chunk_size..].to_vec(),
        ..retry_first.clone()
    };
    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::StageBlobChunk(retry_first),
    );
    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::StageBlobChunk(retry_second),
    );
    replica_dfs
        .process_incoming()
        .expect("retry stage completes independently");
    let responses = owner_transport
        .drain_incoming()
        .expect("owner receives stage response");
    assert!(responses.iter().any(|frame| {
        let MeshMessageBody::Protocol(protocol) = &frame.frame.message.body else {
            return false;
        };
        matches!(
            protocol.decode::<DfsNetworkMessage>(),
            Ok(DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id,
                result: DfsBlobTransferResult::Durable { .. },
                ..
            })) if request_id == "dfs_net_req_stage_retry_new"
        )
    }));

    cleanup(&root);
}

#[test]
fn networked_dfs_chunked_repair_retry_uses_latest_request_id() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_retry_request").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_repair_retry_request");
    let network = InProcessNetwork::new();
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let mut owner_transport = network.add_node(owner.clone());
    let bytes = chunked_test_bytes(4_464);
    let hash = content_hash(&bytes);
    let chunk_size = TEST_DFS_CHUNK_BYTES;
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "repair-retry-request.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    let first_attempt = DfsRepairReplicaChunkCommand {
        request_id: "dfs_net_req_repair_retry_old".to_string(),
        intent_id: "intent_repair_retry_request".to_string(),
        file_system_id: file_system_id.clone(),
        object_id: object_id.clone(),
        expected_base_version: None,
        version: version.clone(),
        content_hash: hash.clone(),
        total_size_bytes: bytes.len() as u64,
        chunk_index: 0,
        total_chunks: 2,
        chunk_hash: None,
        bytes: bytes[..chunk_size].to_vec(),
        object_metadata: Some(metadata.clone()),
    };
    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::RepairReplicaChunk(first_attempt),
    );
    assert!(
        replica_dfs
            .process_incoming()
            .expect("old partial repair starts transfer")
            .is_empty()
    );

    let retry_first = DfsRepairReplicaChunkCommand {
        request_id: "dfs_net_req_repair_retry_new".to_string(),
        intent_id: "intent_repair_retry_request".to_string(),
        file_system_id: file_system_id.clone(),
        object_id: object_id.clone(),
        expected_base_version: None,
        version: version.clone(),
        content_hash: hash.clone(),
        total_size_bytes: bytes.len() as u64,
        chunk_index: 0,
        total_chunks: 2,
        chunk_hash: None,
        bytes: bytes[..chunk_size].to_vec(),
        object_metadata: Some(metadata),
    };
    let retry_second = DfsRepairReplicaChunkCommand {
        chunk_index: 1,
        bytes: bytes[chunk_size..].to_vec(),
        ..retry_first.clone()
    };
    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::RepairReplicaChunk(retry_first),
    );
    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::RepairReplicaChunk(retry_second),
    );
    replica_dfs
        .process_incoming()
        .expect("retry repair completes independently");
    let responses = owner_transport
        .drain_incoming()
        .expect("owner receives repair response");
    assert!(responses.iter().any(|frame| {
        let MeshMessageBody::Protocol(protocol) = &frame.frame.message.body else {
            return false;
        };
        matches!(
            protocol.decode::<DfsNetworkMessage>(),
            Ok(DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                request_id,
                result: DfsBlobTransferResult::Committed { .. },
                ..
            })) if request_id == "dfs_net_req_repair_retry_new"
        )
    }));

    cleanup(&root);
}

#[test]
fn networked_dfs_stage_chunk_duplicate_is_idempotent() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_duplicate_retry").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_stage_duplicate_retry");
    let network = InProcessNetwork::new();
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let mut owner_transport = network.add_node(owner.clone());
    let bytes = chunked_test_bytes(4_464);
    let hash = content_hash(&bytes);
    let chunk_size = TEST_DFS_CHUNK_BYTES;
    let first_chunk = bytes[..chunk_size].to_vec();
    let second_chunk = bytes[chunk_size..].to_vec();
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "duplicate-retry.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };

    let first = DfsStageBlobChunkCommand {
        request_id: "dfs_net_req_stage_duplicate_retry".to_string(),
        intent_id: "intent_stage_duplicate_retry".to_string(),
        file_system_id: file_system_id.clone(),
        object_id: object_id.clone(),
        expected_base_version: None,
        version: version.clone(),
        content_hash: hash.clone(),
        total_size_bytes: bytes.len() as u64,
        chunk_index: 0,
        total_chunks: 2,
        chunk_hash: None,
        bytes: first_chunk.clone(),
        object_metadata: Some(metadata),
    };
    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::StageBlobChunk(first.clone()),
    );
    assert!(
        replica_dfs
            .process_incoming()
            .expect("first chunk starts transfer")
            .is_empty()
    );
    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::StageBlobChunk(first.clone()),
    );
    assert!(
        replica_dfs
            .process_incoming()
            .expect("duplicate chunk is ignored")
            .is_empty()
    );
    let second = DfsStageBlobChunkCommand {
        request_id: "dfs_net_req_stage_duplicate_retry".to_string(),
        intent_id: "intent_stage_duplicate_retry".to_string(),
        file_system_id,
        object_id: object_id.clone(),
        expected_base_version: None,
        version: version.clone(),
        content_hash: hash.clone(),
        total_size_bytes: bytes.len() as u64,
        chunk_index: 1,
        total_chunks: 2,
        chunk_hash: None,
        bytes: second_chunk,
        object_metadata: None,
    };
    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::StageBlobChunk(second.clone()),
    );
    let events = replica_dfs
        .process_incoming()
        .expect("retry completes after duplicate rejection");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStaged {
                object_id: staged,
                version: staged_version,
                content_hash,
                ..
            } if staged == &object_id && staged_version == &version && content_hash == &hash
        )
    }));

    send_dfs_test_message(
        &mut owner_transport,
        &owner,
        &replica,
        DfsNetworkMessage::StageBlobChunk(second),
    );
    let duplicate_events = replica_dfs
        .process_incoming()
        .expect("late duplicate chunk is handled from durable state");
    assert!(duplicate_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStaged {
                object_id: staged,
                version: staged_version,
                content_hash,
                ..
            } if staged == &object_id && staged_version == &version && content_hash == &hash
        )
    }));

    cleanup(&root);
}

#[test]
fn networked_sifs_remote_read_returns_after_all_locations_miss() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let root = temp_root("networked_sifs_read_miss");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    owner_dfs
        .bootstrap_distributed_sifs()
        .expect("owner bootstrap");
    reader_dfs
        .bootstrap_distributed_sifs()
        .expect("reader bootstrap");
    let path = "/services/missing-content.txt";
    let object_id = sinomo_dfs::namespace::object_id_for_path(path);
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
        path: path.to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone()],
        content_hash: Some(format!(
            "blake3:{}",
            blake3::hash(b"missing content").to_hex()
        )),
        content_size_bytes: Some(b"missing content".len() as u64),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: DfsVersion::from_legacy_named("version_1").expect("version"),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner metadata");
    reader_dfs
        .add_object_metadata(metadata)
        .expect("reader metadata");
    let owner_dfs = Arc::new(Mutex::new(owner_dfs));
    let owner_worker = {
        let owner_dfs = Arc::clone(&owner_dfs);
        let object_id = object_id.clone();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let events = owner_dfs
                    .lock()
                    .expect("owner dfs lock")
                    .process_incoming()
                    .expect("owner handles missing read");
                if events.iter().any(|event| {
                    matches!(
                        event,
                        NetworkedDfsServiceEvent::RemoteReadServed {
                            object_id: served_object_id,
                            result: sinomo_dfs::DfsReadResult::NotFound,
                            ..
                        } if served_object_id == &object_id
                    )
                }) {
                    return;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner did not receive missing-content read request"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };
    let started = std::time::Instant::now();
    assert!(matches!(
        reader_dfs.posix_read_file_distributed(path),
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
    ));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "read-through miss should not wait for the full remote-read timeout"
    );
    assert_eq!(
        reader_dfs
            .metadata()
            .object(&object_id)
            .expect("reader metadata after miss")
            .current_locations,
        Vec::<NodeId>::new(),
        "a current location that explicitly misses the advertised content should be pruned locally"
    );
    assert!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("retry after all locations miss")
            .is_empty(),
        "read retry should not contact a node that already denied having the content"
    );
    owner_worker.join().expect("owner worker");

    cleanup(&root);
}

#[test]
fn networked_dfs_remote_read_miss_persists_stale_location_prune() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_backend_read_miss").expect("object");
    let version = DfsVersion::from_legacy_named("version_1").expect("version");
    let root = temp_root("networked_backend_read_miss_location_prune");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let reader_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("reader"), root.join("reader-meta"))
            .expect("reader backend");
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    )
    .with_metadata_backend(reader_backend)
    .expect("reader metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "backend-read-miss.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone()],
        content_hash: Some(content_hash(b"missing backend bytes")),
        content_size_bytes: Some(b"missing backend bytes".len() as u64),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    reader_dfs
        .add_file_system(file_system)
        .expect("reader filesystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner stale metadata");
    reader_dfs
        .add_object_metadata(metadata)
        .expect("reader stale metadata");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("request stale location"),
        vec![owner.clone()]
    );
    owner_dfs
        .process_incoming()
        .expect("owner reports missing content");
    reader_dfs
        .process_incoming()
        .expect("reader prunes missing location");

    assert_eq!(
        reader_dfs
            .metadata()
            .object(&object_id)
            .expect("memory object")
            .current_locations,
        Vec::<NodeId>::new()
    );
    assert_eq!(
        reader_dfs
            .metadata_backend()
            .expect("reader backend")
            .get_object(&object_id)
            .expect("backend object")
            .expect("backend object")
            .current_locations,
        Vec::<NodeId>::new()
    );
    assert_eq!(
        reader_dfs
            .metadata_backend()
            .expect("reader backend")
            .get_version(&object_id, &version)
            .expect("backend version")
            .expect("backend version")
            .locations,
        Vec::<NodeId>::new()
    );
    assert!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("retry after persisted prune")
            .is_empty()
    );

    cleanup(&root);
}

#[test]
fn networked_sifs_remote_read_retries_lost_request_within_readthrough_budget() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let root = temp_root("networked_sifs_readthrough_retry_lost_request");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut owner_control = network.add_node(owner.clone());
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    owner_dfs
        .bootstrap_distributed_sifs()
        .expect("owner bootstrap");
    reader_dfs
        .bootstrap_distributed_sifs()
        .expect("reader bootstrap");
    owner_dfs
        .posix_mkdir_all("/services/readthrough-retry")
        .expect("owner mkdir");
    reader_dfs
        .posix_mkdir_all("/services/readthrough-retry")
        .expect("reader mkdir");
    let bytes = vec![0x5a; TEST_DFS_CHUNK_BYTES + 4_464];
    let stat = owner_dfs
        .posix_write_file(
            "/services/readthrough-retry/file.txt",
            &bytes,
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("owner write");
    reader_dfs
        .add_object_metadata(
            owner_dfs
                .metadata()
                .object(&stat.object_id)
                .expect("owner metadata")
                .clone(),
        )
        .expect("replicate metadata");

    let owner_dfs = Arc::new(Mutex::new(owner_dfs));
    let owner_worker = {
        let owner_dfs = Arc::clone(&owner_dfs);
        let object_id = stat.object_id.clone();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let dropped = owner_control
                    .drain_incoming()
                    .expect("drain first read request");
                if !dropped.is_empty() {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner did not receive the first remote read request"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            loop {
                let events = owner_dfs
                    .lock()
                    .expect("owner dfs lock")
                    .process_incoming()
                    .expect("owner serves retried read");
                if events.iter().any(|event| {
                    matches!(
                        event,
                        NetworkedDfsServiceEvent::RemoteReadServed {
                            object_id: served_object_id,
                            result: sinomo_dfs::DfsReadResult::Found(_),
                            ..
                        } if served_object_id == &object_id
                    )
                }) {
                    return;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner did not receive retried remote read request"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };

    let read = reader_dfs
        .posix_read_file_distributed("/services/readthrough-retry/file.txt")
        .expect("read-through retries lost remote request");
    assert_eq!(read, bytes);
    assert!(matches!(
        reader_dfs
            .read_local_object_with_state(&stat.object_id, false)
            .expect("reader local state after no-cache read-through"),
        NetworkedDfsReadOutcome::PendingReplication { .. }
    ));
    owner_worker.join().expect("owner worker");

    cleanup(&root);
}

#[test]
fn networked_sifs_remote_read_falls_back_after_stale_location_rejection() {
    let stale = NodeId::from_legacy_named("node_stale").expect("node");
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let root = temp_root("networked_sifs_readthrough_stale_location_rejection");
    let network = InProcessNetwork::new();
    let mut stale_dfs = NetworkedDfsService::new(
        network.add_node(stale.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("stale")).expect("stale store"),
    );
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    for dfs in [&mut stale_dfs, &mut owner_dfs, &mut reader_dfs] {
        dfs.bootstrap_distributed_sifs().expect("bootstrap");
        dfs.posix_mkdir_all("/services/readthrough-reject")
            .expect("mkdir");
    }
    let path = "/services/readthrough-reject/file.txt";
    let stat = owner_dfs
        .posix_write_file(
            path,
            b"fallback remote bytes",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("owner write");
    let mut reader_metadata = owner_dfs
        .metadata()
        .object(&stat.object_id)
        .expect("owner metadata")
        .clone();
    reader_metadata.current_locations = vec![stale.clone(), owner.clone()];
    reader_dfs
        .add_object_metadata(reader_metadata)
        .expect("reader metadata");

    let mut stale_metadata = owner_dfs
        .metadata()
        .object(&stat.object_id)
        .expect("owner metadata")
        .clone();
    stale_metadata.current_locations = vec![owner.clone()];
    stale_dfs
        .add_object_metadata(stale_metadata)
        .expect("stale metadata");

    let stale_dfs = Arc::new(Mutex::new(stale_dfs));
    let owner_dfs = Arc::new(Mutex::new(owner_dfs));
    let stale_worker = {
        let stale_dfs = Arc::clone(&stale_dfs);
        let object_id = stat.object_id.clone();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let events = stale_dfs
                    .lock()
                    .expect("stale dfs lock")
                    .process_incoming()
                    .expect("stale rejects read");
                if events.iter().any(|event| {
                    matches!(
                        event,
                        NetworkedDfsServiceEvent::RemoteReadServed {
                            object_id: served_object_id,
                            result: DfsReadResult::NotFound,
                            ..
                        } if served_object_id == &object_id
                    ) || matches!(
                        event,
                        NetworkedDfsServiceEvent::RemoteReadServed {
                            object_id: served_object_id,
                            result: DfsReadResult::Rejected { reason },
                            ..
                        } if served_object_id == &object_id
                            && reason.contains("not a current location")
                    )
                }) {
                    return;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "stale node did not receive remote read request"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };
    let owner_worker = {
        let owner_dfs = Arc::clone(&owner_dfs);
        let object_id = stat.object_id.clone();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let events = owner_dfs
                    .lock()
                    .expect("owner dfs lock")
                    .process_incoming()
                    .expect("owner serves read");
                if events.iter().any(|event| {
                    matches!(
                        event,
                        NetworkedDfsServiceEvent::RemoteReadServed {
                            object_id: served_object_id,
                            result: DfsReadResult::Found(_),
                            ..
                        } if served_object_id == &object_id
                    )
                }) {
                    return;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner did not receive fallback remote read request"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };

    let started = std::time::Instant::now();
    assert_eq!(
        reader_dfs
            .posix_read_file_distributed(path)
            .expect("read-through falls back after stale location rejection"),
        b"fallback remote bytes"
    );
    assert!(matches!(
        reader_dfs
            .read_local_object_with_state(&stat.object_id, false)
            .expect("reader local state after inline no-cache read-through"),
        NetworkedDfsReadOutcome::PendingReplication { .. }
    ));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "read-through should not spend the full remote-read budget on a stale location rejection"
    );
    assert_eq!(
        reader_dfs
            .metadata()
            .object(&stat.object_id)
            .expect("reader metadata")
            .current_locations,
        vec![owner.clone()]
    );
    stale_worker.join().expect("stale worker");
    owner_worker.join().expect("owner worker");

    cleanup(&root);
}

#[test]
fn networked_dfs_remote_read_prunes_stale_version_source_before_retry() {
    let stale = NodeId::from_legacy_named("node_stale_version").expect("node");
    let owner = NodeId::from_legacy_named("node_current_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_version_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_stale_version").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let stale_bytes = b"old replica bytes";
    let current_bytes = b"current replica bytes";
    let root = temp_root("networked_remote_read_prunes_stale_version");
    let network = InProcessNetwork::new();
    let mut stale_dfs = NetworkedDfsService::new(
        network.add_node(stale.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("stale")).expect("stale store"),
    );
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    for dfs in [&mut stale_dfs, &mut owner_dfs, &mut reader_dfs] {
        dfs.add_file_system(file_system.clone())
            .expect("filesystem");
    }

    stale_dfs
        .content()
        .put_object_version(object_id.as_str(), version_1.as_str(), stale_bytes)
        .expect("stale content");
    owner_dfs
        .content()
        .put_object_version(object_id.as_str(), version_2.as_str(), current_bytes)
        .expect("current content");

    let base_metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "stale-version.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone()],
        content_hash: Some(content_hash(current_bytes)),
        content_size_bytes: Some(current_bytes.len() as u64),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_2.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_object_metadata(base_metadata.clone())
        .expect("owner metadata");
    let mut stale_metadata = base_metadata.clone();
    stale_metadata.version = version_1.clone();
    stale_metadata.content_hash = Some(content_hash(stale_bytes));
    stale_metadata.content_size_bytes = Some(stale_bytes.len() as u64);
    stale_metadata.current_locations = vec![stale.clone()];
    stale_dfs
        .add_object_metadata(stale_metadata)
        .expect("stale metadata");
    let mut reader_metadata = base_metadata;
    reader_metadata.current_locations = vec![stale.clone(), owner.clone()];
    reader_dfs
        .add_object_metadata(reader_metadata)
        .expect("reader metadata");

    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("reader requests first source"),
        vec![stale.clone()]
    );
    assert!(
        stale_dfs
            .process_incoming()
            .expect("stale node serves stale version")
            .iter()
            .any(|event| matches!(
                event,
                NetworkedDfsServiceEvent::RemoteReadServed {
                    object_id: served_object_id,
                    result: DfsReadResult::NotFound,
                    ..
                } if served_object_id == &object_id
            ))
    );
    assert!(
        reader_dfs
            .process_incoming()
            .expect("reader prunes stale version source")
            .iter()
            .any(|event| matches!(
                event,
                NetworkedDfsServiceEvent::RemoteReadCompleted {
                    object_id: completed_object_id,
                    result: DfsReadResult::NotFound,
                    ..
                } if completed_object_id == &object_id
            ))
    );
    assert_eq!(
        reader_dfs
            .metadata()
            .object(&object_id)
            .expect("reader metadata after stale prune")
            .current_locations,
        vec![owner.clone()]
    );
    assert_eq!(
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("reader retries after pruning stale source"),
        vec![owner.clone()]
    );
    owner_dfs
        .process_incoming()
        .expect("owner serves current version");
    reader_dfs
        .process_incoming()
        .expect("reader stores current version");
    let read = reader_dfs
        .read_local_object(&object_id)
        .expect("reader local read")
        .expect("current data");
    assert_eq!(read.version, version_2);
    assert_eq!(read.bytes, current_bytes);

    cleanup(&root);
}

#[test]
fn networked_sifs_remote_read_rotates_after_silent_first_location() {
    let silent = NodeId::from_legacy_named("node_silent").expect("node");
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let root = temp_root("networked_sifs_readthrough_silent_location_rotation");
    let network = InProcessNetwork::new();
    let mut silent_transport = network.add_node(silent.clone());
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    owner_dfs
        .bootstrap_distributed_sifs()
        .expect("owner bootstrap");
    reader_dfs
        .bootstrap_distributed_sifs()
        .expect("reader bootstrap");
    owner_dfs
        .posix_mkdir_all("/services/readthrough-silent")
        .expect("owner mkdir");
    reader_dfs
        .posix_mkdir_all("/services/readthrough-silent")
        .expect("reader mkdir");
    let path = "/services/readthrough-silent/file.txt";
    let stat = owner_dfs
        .posix_write_file(
            path,
            b"silent fallback bytes",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("owner write");
    let mut reader_metadata = owner_dfs
        .metadata()
        .object(&stat.object_id)
        .expect("owner metadata")
        .clone();
    reader_metadata.current_locations = vec![silent.clone(), owner.clone()];
    reader_dfs
        .add_object_metadata(reader_metadata)
        .expect("reader metadata");

    let owner_dfs = Arc::new(Mutex::new(owner_dfs));
    let owner_worker = {
        let owner_dfs = Arc::clone(&owner_dfs);
        let object_id = stat.object_id.clone();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let events = owner_dfs
                    .lock()
                    .expect("owner dfs lock")
                    .process_incoming()
                    .expect("owner serves rotated read");
                if events.iter().any(|event| {
                    matches!(
                        event,
                        NetworkedDfsServiceEvent::RemoteReadServed {
                            object_id: served_object_id,
                            result: DfsReadResult::Found(_),
                            ..
                        } if served_object_id == &object_id
                    )
                }) {
                    return;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner did not receive rotated remote read request"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };

    let started = std::time::Instant::now();
    assert_eq!(
        reader_dfs
            .posix_read_file_distributed(path)
            .expect("read-through rotates after silent location"),
        b"silent fallback bytes"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "read-through should rotate after one retry delay, not spend the full budget on a silent location"
    );
    assert_eq!(
        silent_transport
            .drain_incoming()
            .expect("silent mailbox")
            .len(),
        1,
        "silent first location should receive only the initial request before rotation"
    );
    owner_worker.join().expect("owner worker");

    cleanup(&root);
}

#[test]
fn networked_sifs_remote_read_fetches_current_content_for_posix_path() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let root = temp_root("networked_sifs_remote_posix_read");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("reader store"),
    );
    owner_dfs
        .bootstrap_distributed_sifs()
        .expect("owner bootstrap");
    reader_dfs
        .bootstrap_distributed_sifs()
        .expect("reader bootstrap");
    owner_dfs
        .posix_mkdir_all("/services/readthrough")
        .expect("owner mkdir");
    reader_dfs
        .posix_mkdir_all("/services/readthrough")
        .expect("reader mkdir");
    let stat = owner_dfs
        .posix_write_file(
            "/services/readthrough/file.txt",
            b"remote bytes",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("owner write");
    let file_metadata = owner_dfs
        .metadata()
        .object(&stat.object_id)
        .expect("owner metadata")
        .clone();
    let link_stat = owner_dfs
        .posix_symlink(
            "file.txt",
            "/services/readthrough/link.txt",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("owner symlink");
    let link_metadata = owner_dfs
        .metadata()
        .object(&link_stat.object_id)
        .expect("owner symlink metadata")
        .clone();
    let dir_link_stat = owner_dfs
        .posix_symlink(
            "readthrough",
            "/services/readthrough-link",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("owner directory symlink");
    let dir_link_metadata = owner_dfs
        .metadata()
        .object(&dir_link_stat.object_id)
        .expect("owner directory symlink metadata")
        .clone();
    let overwrite_stat = owner_dfs
        .posix_write_file(
            "/services/readthrough/overwrite.txt",
            b"remote base",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("owner overwrite base");
    let overwrite_metadata = owner_dfs
        .metadata()
        .object(&overwrite_stat.object_id)
        .expect("owner overwrite metadata")
        .clone();
    let truncate_stat = owner_dfs
        .posix_write_file(
            "/services/readthrough/truncate.txt",
            b"truncate me",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("owner truncate base");
    let truncate_metadata = owner_dfs
        .metadata()
        .object(&truncate_stat.object_id)
        .expect("owner truncate metadata")
        .clone();
    assert_eq!(file_metadata.current_locations, vec![owner.clone()]);
    assert_eq!(link_metadata.current_locations, vec![owner.clone()]);
    assert_eq!(dir_link_metadata.current_locations, vec![owner.clone()]);
    assert_eq!(overwrite_metadata.current_locations, vec![owner.clone()]);
    assert_eq!(truncate_metadata.current_locations, vec![owner.clone()]);
    reader_dfs
        .add_object_metadata(file_metadata)
        .expect("replicate metadata");
    reader_dfs
        .add_object_metadata(link_metadata)
        .expect("replicate symlink metadata");
    reader_dfs
        .add_object_metadata(dir_link_metadata)
        .expect("replicate directory symlink metadata");
    reader_dfs
        .add_object_metadata(overwrite_metadata)
        .expect("replicate overwrite metadata");
    reader_dfs
        .add_object_metadata(truncate_metadata)
        .expect("replicate truncate metadata");
    assert_eq!(
        reader_dfs
            .posix_stat("/services/readthrough/file.txt")
            .expect("metadata-only remote file stat")
            .size_bytes,
        Some(b"remote bytes".len() as u64)
    );
    assert_eq!(
        reader_dfs
            .posix_lstat("/services/readthrough/link.txt")
            .expect("metadata-only remote symlink stat")
            .size_bytes,
        Some("file.txt".len() as u64)
    );

    let owner_dfs = Arc::new(Mutex::new(owner_dfs));
    let owner_worker = {
        let owner_dfs = Arc::clone(&owner_dfs);
        let stat = stat.clone();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let events = owner_dfs
                    .lock()
                    .expect("owner dfs lock")
                    .process_incoming()
                    .expect("owner serves read");
                if events.iter().any(|event| {
                    matches!(
                        event,
                        NetworkedDfsServiceEvent::RemoteReadServed {
                            object_id,
                            result: sinomo_dfs::DfsReadResult::Found(_),
                            ..
                        } if object_id == &stat.object_id
                    )
                }) {
                    return events;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner did not receive remote read request"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };
    assert_eq!(
        reader_dfs
            .posix_read_file_update_access("/services/readthrough/file.txt")
            .expect("distributed POSIX read with atime update fetches remote content"),
        b"remote bytes"
    );
    let owner_events = owner_worker.join().expect("owner worker");
    assert!(owner_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadServed {
                object_id,
                result: sinomo_dfs::DfsReadResult::Found(_),
                ..
            } if object_id == &stat.object_id
        )
    }));
    assert_eq!(
        reader_dfs
            .posix_read_file("/services/readthrough/file.txt")
            .expect("posix read after cached read-through"),
        b"remote bytes"
    );
    assert_eq!(
        reader_dfs
            .metadata()
            .object(&stat.object_id)
            .expect("reader metadata")
            .current_locations,
        vec![owner.clone()]
    );

    let owner_worker = {
        let owner_dfs = Arc::clone(&owner_dfs);
        let dir_link_stat = dir_link_stat.clone();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let events = owner_dfs
                    .lock()
                    .expect("owner dfs lock")
                    .process_incoming()
                    .expect("owner serves intermediate symlink read");
                if events.iter().any(|event| {
                    matches!(
                        event,
                        NetworkedDfsServiceEvent::RemoteReadServed {
                            object_id,
                            result: sinomo_dfs::DfsReadResult::Found(_),
                            ..
                        } if object_id == &dir_link_stat.object_id
                    )
                }) {
                    return events;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner did not receive remote intermediate symlink read request"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };
    assert_eq!(
        reader_dfs
            .posix_read_file_distributed("/services/readthrough-link/file.txt")
            .expect("distributed POSIX path resolution fetches intermediate symlink content"),
        b"remote bytes"
    );
    let owner_events = owner_worker
        .join()
        .expect("owner intermediate symlink worker");
    assert!(owner_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadServed {
                object_id,
                result: sinomo_dfs::DfsReadResult::Found(_),
                ..
            } if object_id == &dir_link_stat.object_id
        )
    }));
    assert_eq!(
        reader_dfs
            .posix_read_file("/services/readthrough-link/file.txt")
            .expect("posix read through cached intermediate symlink"),
        b"remote bytes"
    );

    let owner_worker = {
        let owner_dfs = Arc::clone(&owner_dfs);
        let link_stat = link_stat.clone();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let events = owner_dfs
                    .lock()
                    .expect("owner dfs lock")
                    .process_incoming()
                    .expect("owner serves symlink read");
                if events.iter().any(|event| {
                    matches!(
                        event,
                        NetworkedDfsServiceEvent::RemoteReadServed {
                            object_id,
                            result: sinomo_dfs::DfsReadResult::Found(_),
                            ..
                        } if object_id == &link_stat.object_id
                    )
                }) {
                    return events;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner did not receive remote symlink read request"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };
    assert_eq!(
        reader_dfs
            .posix_readlink_update_access("/services/readthrough/link.txt")
            .expect("distributed POSIX readlink with atime update fetches remote symlink content"),
        "file.txt"
    );
    let owner_events = owner_worker.join().expect("owner symlink worker");
    assert!(owner_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadServed {
                object_id,
                result: sinomo_dfs::DfsReadResult::Found(_),
                ..
            } if object_id == &link_stat.object_id
        )
    }));
    assert_eq!(
        reader_dfs
            .posix_readlink("/services/readthrough/link.txt")
            .expect("posix readlink after cached read-through"),
        "file.txt"
    );
    assert_eq!(
        reader_dfs
            .metadata()
            .object(&link_stat.object_id)
            .expect("reader symlink metadata")
            .current_locations,
        vec![owner.clone()]
    );

    let owner_worker = {
        let owner_dfs = Arc::clone(&owner_dfs);
        let overwrite_stat = overwrite_stat.clone();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let events = owner_dfs
                    .lock()
                    .expect("owner dfs lock")
                    .process_incoming()
                    .expect("owner serves overwrite base read");
                if events.iter().any(|event| {
                    matches!(
                        event,
                        NetworkedDfsServiceEvent::RemoteReadServed {
                            object_id,
                            result: sinomo_dfs::DfsReadResult::Found(_),
                            ..
                        } if object_id == &overwrite_stat.object_id
                    )
                }) {
                    return events;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner did not receive remote overwrite base read request"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };
    let overwrite_result = reader_dfs
        .posix_write_file_at(
            "/services/readthrough/overwrite.txt",
            7,
            b"DFS",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![reader.clone()]),
        )
        .expect("distributed POSIX write-at fetches remote base content");
    let owner_events = owner_worker.join().expect("owner overwrite worker");
    assert!(owner_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadServed {
                object_id,
                result: sinomo_dfs::DfsReadResult::Found(_),
                ..
            } if object_id == &overwrite_stat.object_id
        )
    }));
    assert_eq!(
        reader_dfs
            .posix_read_file("/services/readthrough/overwrite.txt")
            .expect("write-at result is locally readable"),
        b"remote DFSe"
    );
    assert_eq!(
        overwrite_result.size_bytes,
        Some(b"remote DFSe".len() as u64)
    );

    let owner_worker = {
        let owner_dfs = Arc::clone(&owner_dfs);
        let truncate_stat = truncate_stat.clone();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let events = owner_dfs
                    .lock()
                    .expect("owner dfs lock")
                    .process_incoming()
                    .expect("owner serves truncate base read");
                if events.iter().any(|event| {
                    matches!(
                        event,
                        NetworkedDfsServiceEvent::RemoteReadServed {
                            object_id,
                            result: sinomo_dfs::DfsReadResult::Found(_),
                            ..
                        } if object_id == &truncate_stat.object_id
                    )
                }) {
                    return events;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner did not receive remote truncate base read request"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };
    let truncate_result = reader_dfs
        .posix_truncate_file(
            "/services/readthrough/truncate.txt",
            8,
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![reader.clone()]),
        )
        .expect("distributed POSIX truncate fetches remote base content");
    let owner_events = owner_worker.join().expect("owner truncate worker");
    assert!(owner_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadServed {
                object_id,
                result: sinomo_dfs::DfsReadResult::Found(_),
                ..
            } if object_id == &truncate_stat.object_id
        )
    }));
    assert_eq!(
        reader_dfs
            .posix_read_file("/services/readthrough/truncate.txt")
            .expect("truncate result is locally readable"),
        b"truncate"
    );
    assert_eq!(truncate_result.size_bytes, Some(8));

    cleanup(&root);
}

#[test]
fn networked_dfs_retries_offline_repair_when_replica_appears() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_offline_repair_retry");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner DfsFileSystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");

    let bytes = chunked_test_bytes(4_464);
    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes.clone(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("threshold write");
    assert!(matches!(
        write,
        NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } if repair_jobs.len() == 1
    ));

    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    replica_dfs
        .add_file_system(file_system)
        .expect("replica DfsFileSystem");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let scheduled = owner_dfs.retry_pending_repairs().expect("retry repair");
    assert_eq!(scheduled.len(), 1);
    let replica_events = replica_dfs.process_incoming().expect("replica repair");
    assert!(
        replica_events
            .iter()
            .any(|event| matches!(event, NetworkedDfsServiceEvent::ReplicaRepaired { .. }))
    );
    let owner_events = owner_dfs.process_incoming().expect("owner repair ack");
    assert!(
        owner_events
            .iter()
            .any(|event| matches!(event, NetworkedDfsServiceEvent::ReplicaRepaired { .. }))
    );
    assert!(owner_dfs.coordinator().repair_jobs().is_empty());
    assert!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("owner object")
            .current_locations
            .contains(&replica)
    );
    assert_eq!(
        replica_dfs
            .read_local_object(&object_id)
            .expect("read")
            .expect("data")
            .bytes,
        bytes
    );

    cleanup(&root);
}

#[test]
fn posix_write_file_does_not_expose_new_file_until_content_commits() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let offline = NodeId::from_legacy_named("node_offline").expect("node");
    let root = temp_root("posix_new_file_pending_hidden");
    let network = InProcessNetwork::new();
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(backend)
    .expect("metadata backend");

    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    dfs.posix_mkdir_all("/services/web").expect("mkdir service");
    let mut options = sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner, offline]);
    options.policy = DfsReplicationPolicy::new(2, 2).expect("strict eventual policy");
    options.wait_for_commit = false;

    assert!(matches!(
        dfs.posix_write_file("/services/web/pending.html", b"pending", options),
        Err(sinomo_dfs::DfsPosixError::PendingWrite { .. })
    ));
    assert!(matches!(
        dfs.posix_stat("/services/web/pending.html"),
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
    ));
    assert!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .get_object_by_path(
                &sinomo_dfs::sifs_file_system_id(),
                "/services/web/pending.html"
            )
            .expect("backend lookup")
            .is_none()
    );

    cleanup(&root);
}

#[test]
fn posix_write_file_from_reader_streams_content() {
    let owner = NodeId::from_legacy_named("node_posix_reader_owner").expect("node");
    let root = temp_root("posix_write_file_from_reader");
    let network = InProcessNetwork::new();
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(backend)
    .expect("metadata backend");

    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    dfs.posix_mkdir_all("/services/web").expect("mkdir service");

    let bytes = 128 * 1024 + 17;
    let stat = dfs
        .posix_write_file_from_reader(
            "/services/web/stream.bin",
            PatternReader::new(bytes),
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("streaming POSIX write");

    assert_eq!(stat.size_bytes, Some(bytes as u64));
    assert_eq!(
        dfs.posix_read_file("/services/web/stream.bin")
            .expect("read streamed POSIX file"),
        pattern_bytes(bytes)
    );
    let backend_object = dfs
        .metadata_backend()
        .expect("metadata backend")
        .get_object_by_path(
            &sinomo_dfs::sifs_file_system_id(),
            "/services/web/stream.bin",
        )
        .expect("backend path lookup")
        .expect("streamed object metadata");
    assert_eq!(backend_object.content_size_bytes, Some(bytes as u64));
    assert_eq!(backend_object.content_hash, stat.content_hash);

    cleanup(&root);
}

#[test]
fn posix_open_file_stream_reads_file_and_range_incrementally() {
    let owner = NodeId::from_legacy_named("node_posix_stream_reader_owner").expect("node");
    let root = temp_root("posix_open_file_stream_reader");
    let network = InProcessNetwork::new();
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(backend)
    .expect("metadata backend");

    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    dfs.posix_mkdir_all("/services/web").expect("mkdir service");
    let bytes = 256 * 1024 + 31;
    let stat = dfs
        .posix_write_file_from_reader(
            "/services/web/stream-read.bin",
            PatternReader::new(bytes),
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("streaming POSIX write");

    let mut stream = dfs
        .posix_open_file_stream("/services/web/stream-read.bin")
        .expect("open file stream");
    assert_eq!(stream.object_id, stat.object_id);
    assert_eq!(stream.version, stat.version);
    assert_eq!(stream.content_hash, stat.content_hash);
    assert_eq!(stream.size_bytes, bytes as u64);
    let mut streamed = Vec::new();
    let mut buffer = [0_u8; 4093];
    loop {
        let read = stream.reader.read(&mut buffer).expect("read stream");
        if read == 0 {
            break;
        }
        streamed.extend_from_slice(&buffer[..read]);
    }
    assert_eq!(streamed, pattern_bytes(bytes));

    let offset = 4097_u64;
    let range_len = 8192 + 13;
    let mut range_stream = dfs
        .posix_open_file_range_stream("/services/web/stream-read.bin", offset, range_len)
        .expect("open range stream");
    assert_eq!(range_stream.object_id, stat.object_id);
    assert_eq!(range_stream.version, stat.version);
    assert_eq!(range_stream.content_hash, stat.content_hash);
    assert_eq!(range_stream.offset, offset);
    assert_eq!(range_stream.total_size_bytes, bytes as u64);
    assert_eq!(range_stream.range_size_bytes, range_len as u64);
    let mut streamed_range = Vec::new();
    range_stream
        .reader
        .read_to_end(&mut streamed_range)
        .expect("read range stream");
    let expected = pattern_bytes(bytes);
    let range_start = offset as usize;
    assert_eq!(
        streamed_range,
        expected[range_start..range_start + range_len].to_vec()
    );

    cleanup(&root);
}

#[test]
fn posix_self_only_create_publishes_object_and_version_without_namespace_transaction() {
    let owner = NodeId::from_legacy_named("node_posix_create_grouped_owner").expect("node");
    let root = temp_root("posix_create_grouped_metadata");
    let network = InProcessNetwork::new();
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(backend)
    .expect("metadata backend");

    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    dfs.posix_mkdir_all("/services/web").expect("mkdir service");
    let before_sequence = dfs
        .metadata_backend()
        .expect("metadata backend")
        .journal_since(0)
        .expect("initial journal")
        .last()
        .map(|entry| entry.sequence)
        .unwrap_or(0);

    let bytes = 4096;
    let stat = dfs
        .posix_write_file_from_reader(
            "/services/web/grouped.bin",
            PatternReader::new(bytes),
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("streaming POSIX write");

    let write_entries = dfs
        .metadata_backend()
        .expect("metadata backend")
        .journal_since(before_sequence)
        .expect("write journal");
    let file_publish_entries = write_entries
        .iter()
        .filter(|entry| match &entry.mutation {
            DfsMetadataMutation::PutObject { path, .. } => path == "/services/web/grouped.bin",
            DfsMetadataMutation::PutVersion {
                version_record: Some(version),
                ..
            } => version.content_hash == stat.content_hash,
            DfsMetadataMutation::ApplyNamespaceTransaction {
                transaction: Some(transaction),
                ..
            } => {
                transaction
                    .objects
                    .iter()
                    .any(|object| object.path == "/services/web/grouped.bin")
                    || transaction
                        .versions
                        .iter()
                        .any(|version| version.content_hash == stat.content_hash)
            }
            _ => false,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        file_publish_entries.len(),
        2,
        "self-only create should publish file object and version without extra file metadata journal entries"
    );
    assert!(matches!(
        file_publish_entries[0].mutation,
        DfsMetadataMutation::PutObject { .. }
    ));
    assert!(matches!(
        file_publish_entries[1].mutation,
        DfsMetadataMutation::PutVersion { .. }
    ));
    assert!(!write_entries.iter().any(|entry| {
        matches!(
            entry.mutation,
            DfsMetadataMutation::ApplyNamespaceTransaction { .. }
        )
    }));

    cleanup(&root);
}

#[test]
fn networked_dfs_rejected_stage_blob_does_not_create_replica_metadata() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let attacker = NodeId::from_legacy_named("node_attacker").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_auth_unknown").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_rejected_stage_no_metadata");
    let network = InProcessNetwork::new();
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner],
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
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    let mut attacker_dfs = NetworkedDfsService::new(
        network.add_node(attacker.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("attacker")).expect("attacker content"),
    );
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica content"),
    );
    attacker_dfs
        .add_file_system(file_system.clone())
        .expect("attacker filesystem");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica filesystem");
    attacker_dfs
        .add_object_metadata(metadata)
        .expect("attacker object");

    let write = attacker_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: attacker,
            },
            b"unauthorized bytes".to_vec(),
            vec![attacker_dfs.node_id().clone(), replica],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("attacker write");
    assert!(matches!(write, NetworkedDfsWriteOutcome::Pending { .. }));

    let events = replica_dfs.process_incoming().expect("replica stage");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected { reason, .. }
                if reason.contains("unauthorized DFS replica command")
        )
    }));
    assert!(replica_dfs.metadata().object(&object_id).is_none());

    cleanup(&root);
}

#[test]
fn networked_dfs_replicated_confirmed_commits_after_all_receipts_and_reads_confirmed() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_confirmed_commit");
    let network = InProcessNetwork::new();
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner-shard"), root.join("meta"))
            .expect("metadata backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedConfirmed,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner DfsFileSystem");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica DfsFileSystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let write = owner_dfs
        .write_replicated_confirmed(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"confirmed bytes".to_vec(),
            vec![owner.clone(), replica.clone()],
        )
        .expect("confirmed write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending {
            intent_id,
            durable_receipts,
            required_receipts,
        } => {
            assert_eq!(durable_receipts, 1);
            assert_eq!(required_receipts, 2);
            intent_id
        }
        NetworkedDfsWriteOutcome::Committed { .. } | NetworkedDfsWriteOutcome::Conflict { .. } => {
            panic!("confirmed write should wait")
        }
    };

    replica_dfs.process_incoming().expect("replica stage");
    owner_dfs.process_incoming().expect("owner commit");
    assert_eq!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("owner object")
            .version,
        version_2
    );
    assert_eq!(
        owner_dfs
            .metadata_backend()
            .expect("metadata backend")
            .receipts_for_version(&object_id, &version_2)
            .expect("receipts")
            .len(),
        2
    );
    assert_eq!(
        owner_dfs
            .coordinator()
            .intent(&intent_id)
            .expect("intent")
            .state,
        DfsWriteIntentState::Committed
    );
    let confirmed = owner_dfs
        .read_local_object_confirmed(&object_id, &[owner.clone(), replica.clone()])
        .expect("confirmed read");
    assert!(matches!(
        confirmed,
        NetworkedDfsReadOutcome::Found {
            data,
            freshness: NetworkedDfsReadFreshness::Confirmed { replica_node_ids },
        } if data.bytes == b"confirmed bytes"
            && replica_node_ids == vec![owner.clone(), replica.clone()]
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_confirmed_read_uses_backend_current_metadata() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let reader = NodeId::from_legacy_named("node_reader").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_backend_confirmed").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"confirmed backend bytes";
    let hash = content_hash(bytes);
    let root = temp_root("networked_confirmed_backend_current");
    let network = InProcessNetwork::new();
    let backend_path = root.join("meta");
    let content = sinomo_dfs::LocalContentStore::new(root.join("reader")).expect("content");
    content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("content");
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("reader-shard"), &backend_path)
            .expect("metadata backend");
    let mut reader_dfs = NetworkedDfsService::new(network.add_node(reader.clone()), content)
        .with_metadata_backend(backend)
        .expect("reader metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    reader_dfs
        .add_file_system(file_system.clone())
        .expect("filesystem");

    let stale_memory = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone()],
        content_hash: Some(content_hash(b"stale bytes")),
        content_size_bytes: Some(11),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    reader_dfs
        .add_transient_object_metadata(stale_memory)
        .expect("stale memory");

    let current_backend = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone(), replica.clone()],
        content_hash: Some(hash.clone()),
        content_size_bytes: Some(bytes.len() as u64),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_2.clone(),
        policy: DfsObjectPolicy::ReplicatedConfirmed,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    let mut backend_writer =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("reader-shard"), &backend_path)
            .expect("metadata backend writer");
    backend_writer
        .put_object(&current_backend)
        .expect("backend object");
    backend_writer
        .put_version(&DfsObjectVersionRecord {
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            version: version_2.clone(),
            content_hash: Some(hash.clone()),
            size_bytes: Some(bytes.len() as u64),
            locations: vec![owner.clone(), replica.clone()],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        })
        .expect("backend version");
    for node_id in [&owner, &replica] {
        backend_writer
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    committed_version: version_2.clone(),
                    owner_node_id: owner.clone(),
                    event_id: EventId::from_legacy_named(&format!("evt_{node_id}")).expect("event"),
                },
                replica_node_id: node_id.clone(),
                content_hash: Some(hash.clone()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("backend receipt");
    }

    let confirmed = reader_dfs
        .read_local_object_confirmed(&object_id, &[owner.clone(), replica.clone()])
        .expect("confirmed read");
    assert!(matches!(
        confirmed,
        NetworkedDfsReadOutcome::Found {
            data,
            freshness: NetworkedDfsReadFreshness::Confirmed { replica_node_ids },
        } if data.bytes == bytes
            && replica_node_ids == vec![owner.clone(), replica.clone()]
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_replicated_confirmed_timeout_can_abort_pending_intent() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_confirmed_abort");
    let network = InProcessNetwork::new();
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner-shard"), root.join("meta"))
            .expect("metadata backend");
    let mut replica_transport = network.add_node(replica.clone());
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedConfirmed,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs.add_file_system(file_system).expect("filesystem");
    owner_dfs.add_object_metadata(metadata).expect("object");

    assert!(matches!(
        owner_dfs.write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"bad policy".to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        ),
        Err(sinomo_dfs::DfsError::InvalidReplicationPolicy { .. })
    ));

    let write = owner_dfs
        .write_replicated_confirmed(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"confirmed pending bytes".to_vec(),
            vec![owner.clone(), replica.clone()],
        )
        .expect("confirmed write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        NetworkedDfsWriteOutcome::Committed { .. } | NetworkedDfsWriteOutcome::Conflict { .. } => {
            panic!("offline replica should stay pending")
        }
    };
    assert!(matches!(
        owner_dfs
            .handle_confirmed_write_timeout(&intent_id, DfsConfirmedTimeoutAction::KeepPending)
            .expect("keep pending"),
        DfsConfirmedTimeoutOutcome::StillPending {
            durable_receipts: 1,
            required_receipts: 2,
            ..
        }
    ));
    assert!(matches!(
        owner_dfs
            .handle_confirmed_write_timeout(&intent_id, DfsConfirmedTimeoutAction::Abort)
            .expect("abort"),
        DfsConfirmedTimeoutOutcome::Aborted { .. }
    ));
    assert_eq!(
        owner_dfs
            .coordinator()
            .intent(&intent_id)
            .expect("intent")
            .state,
        DfsWriteIntentState::Aborted
    );
    assert_eq!(
        owner_dfs
            .metadata_backend()
            .expect("metadata backend")
            .get_intent(&object_id, &intent_id)
            .expect("get intent")
            .expect("intent")
            .status,
        DfsWriteIntentStatus::Aborted
    );
    assert_eq!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("object")
            .version,
        version_1
    );
    send_dfs_test_message(
        &mut replica_transport,
        &replica,
        &owner,
        DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
            request_id: "dfs_net_req_1".to_string(),
            intent_id: intent_id.clone(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            version: version_2,
            replica_node_id: replica.clone(),
            result: DfsBlobTransferResult::Durable {
                content_hash: content_hash(b"confirmed pending bytes"),
            },
        }),
    );
    let late_events = owner_dfs
        .process_incoming()
        .expect("late stage response after abort is isolated");
    assert!(late_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected { reason, .. }
                if reason.contains("missing pending write for stage response")
        )
    }));
    assert_eq!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("object after late stage")
            .version,
        version_1
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_replicated_confirmed_can_downgrade_then_repair() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_confirmed_downgrade");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    );
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedConfirmed,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner DfsFileSystem");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");

    let write = owner_dfs
        .write_replicated_confirmed(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"downgraded bytes".to_vec(),
            vec![owner.clone(), replica.clone()],
        )
        .expect("confirmed write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        NetworkedDfsWriteOutcome::Committed { .. } | NetworkedDfsWriteOutcome::Conflict { .. } => {
            panic!("offline replica should stay pending")
        }
    };

    let downgraded = owner_dfs
        .handle_confirmed_write_timeout(
            &intent_id,
            DfsConfirmedTimeoutAction::DowngradeToEventual {
                min_commit_replicas: 1,
            },
        )
        .expect("downgrade");
    assert!(matches!(
        downgraded,
        DfsConfirmedTimeoutOutcome::Downgraded(NetworkedDfsWriteOutcome::Committed {
            repair_jobs,
            ..
        }) if repair_jobs.len() == 1
    ));
    assert_eq!(
        owner_dfs
            .metadata()
            .object(&object_id)
            .expect("owner object")
            .version,
        version_2
    );
    assert!(matches!(
        owner_dfs
            .read_local_object_confirmed(&object_id, &[owner.clone(), replica.clone()])
            .expect("confirmed read before repair"),
        NetworkedDfsReadOutcome::NotConfirmed { .. }
    ));

    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica store"),
    );
    replica_dfs
        .add_file_system(file_system)
        .expect("replica DfsFileSystem");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");
    assert_eq!(
        owner_dfs
            .retry_pending_repairs()
            .expect("retry repair")
            .len(),
        1
    );
    replica_dfs.process_incoming().expect("replica repair");
    owner_dfs.process_incoming().expect("owner repair ack");
    let confirmed = owner_dfs
        .read_local_object_confirmed(&object_id, &[owner.clone(), replica.clone()])
        .expect("confirmed read after repair");
    assert!(matches!(
        confirmed,
        NetworkedDfsReadOutcome::Found {
            data,
            freshness: NetworkedDfsReadFreshness::Confirmed { .. },
        } if data.bytes == b"downgraded bytes"
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_stale_replicated_eventual_write_creates_conflict_read_view() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let root = temp_root("networked_conflict_write");
    let network = InProcessNetwork::new();
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner-shard"), root.join("meta"))
            .expect("metadata backend");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    )
    .with_metadata_backend(backend)
    .expect("metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
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
        version: version_2.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    dfs.add_file_system(file_system).expect("filesystem");
    dfs.put_local_object_version(metadata, b"visible bytes")
        .expect("seed visible");

    let write = dfs
        .write_distributed_eventual_with_base(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_3.clone(),
                requesting_node_id: owner.clone(),
            },
            Some(version_1),
            b"stale candidate bytes".to_vec(),
            vec![owner.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect("stale write becomes conflict");
    let conflict = match write {
        NetworkedDfsWriteOutcome::Conflict { conflict } => conflict,
        other => panic!("expected conflict outcome, got {other:?}"),
    };
    assert_eq!(conflict.local_version, version_3);
    assert_eq!(conflict.remote_version, version_2);
    assert_eq!(conflict.status, DfsConflictStatus::Open);
    assert_eq!(
        dfs.metadata_backend()
            .expect("backend")
            .conflicts_for_object(&object_id)
            .expect("conflicts")
            .len(),
        1
    );

    let read = dfs
        .read_local_object_with_state(&object_id, false)
        .expect("conflicted read");
    assert!(matches!(
        read,
        NetworkedDfsReadOutcome::Conflicted {
            visible_version,
            conflicts,
            ..
        } if visible_version == version_2 && conflicts.len() == 1
    ));
    dfs.update_conflict_status(
        &object_id,
        &conflict.conflict_id,
        DfsConflictStatus::Resolved,
    )
    .expect("resolve conflict")
    .expect("conflict");
    assert!(matches!(
        dfs.read_local_object_with_state(&object_id, false)
            .expect("resolved read"),
        NetworkedDfsReadOutcome::Found { .. }
    ));

    cleanup(&root);
}

#[test]
fn networked_dfs_reconnect_reconciliation_refreshes_receipt_and_records_conflict() {
    let node = NodeId::from_legacy_named("node_laptop").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3_local").expect("version");
    let root = temp_root("networked_reconnect_reconcile");
    let network = InProcessNetwork::new();
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner-shard"), root.join("meta"))
            .expect("metadata backend");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("node")).expect("node store"),
    )
    .with_metadata_backend(backend)
    .expect("metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: node.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: node.clone(),
        current_locations: vec![node.clone()],
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
        version: version_2.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    dfs.add_file_system(file_system).expect("filesystem");
    dfs.put_local_object_version(metadata, b"visible bytes")
        .expect("seed visible");
    dfs.content()
        .put_object_version(
            object_id.as_str(),
            version_3.as_str(),
            b"offline edit bytes",
        )
        .expect("offline candidate");

    let reconciliation = dfs
        .reconcile_local_manifests(&object_id)
        .expect("reconcile");
    assert_eq!(reconciliation.refreshed_receipts, 1);
    assert!(!reconciliation.missing_current);
    assert_eq!(reconciliation.conflicts.len(), 1);
    assert_eq!(
        reconciliation.conflicts[0].local_version,
        DfsVersion::from_legacy_named("version_3_local").expect("version")
    );
    assert_eq!(
        dfs.metadata_backend()
            .expect("backend")
            .receipts_for_version(&object_id, &version_2)
            .expect("receipts")
            .len(),
        1
    );
    assert!(matches!(
        dfs.read_local_object_with_state(&object_id, false)
            .expect("conflicted read"),
        NetworkedDfsReadOutcome::Conflicted { .. }
    ));
    let journal_count_before_repeat = dfs
        .metadata_backend()
        .expect("backend")
        .journal_since(0)
        .expect("journal before repeat reconcile")
        .len();
    let repeated_reconciliation = dfs
        .reconcile_local_manifests(&object_id)
        .expect("repeat reconcile");
    let journal_count_after_repeat = dfs
        .metadata_backend()
        .expect("backend")
        .journal_since(0)
        .expect("journal after repeat reconcile")
        .len();
    assert_eq!(
        repeated_reconciliation.refreshed_receipts, 0,
        "repeat reconnect reconciliation should not report unchanged receipts as refreshed"
    );
    assert_eq!(repeated_reconciliation.conflicts.len(), 1);
    assert_eq!(
        repeated_reconciliation.conflicts[0].conflict_id, reconciliation.conflicts[0].conflict_id,
        "repeat reconnect reconciliation should report the same durable conflict"
    );
    assert_eq!(
        journal_count_after_repeat, journal_count_before_repeat,
        "repeat reconnect reconciliation must not republish unchanged conflicts or receipts"
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_reconnect_reconciliation_ignores_historical_manifests() {
    let node = NodeId::from_legacy_named("node_reconcile_history").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_reconcile_history").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    assert!(version_1 < version_2);
    let root = temp_root("networked_reconnect_historical_manifest");
    let network = InProcessNetwork::new();
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner-shard"), root.join("meta"))
            .expect("metadata backend");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("node")).expect("node store"),
    )
    .with_metadata_backend(backend)
    .expect("metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: node.clone(),
        subtree_policies: Vec::new(),
    };
    let mut metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact-history.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: node.clone(),
        current_locations: vec![node.clone()],
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    dfs.add_file_system(file_system).expect("filesystem");
    dfs.put_local_object_version(metadata.clone(), b"historical committed bytes")
        .expect("seed historical visible version");
    metadata.version = version_2.clone();
    metadata.content_hash = None;
    metadata.content_size_bytes = None;
    dfs.put_local_object_version(metadata, b"visible bytes")
        .expect("seed current visible version");

    let reconciliation = dfs
        .reconcile_local_manifests(&object_id)
        .expect("reconcile historical manifest");

    assert_eq!(reconciliation.refreshed_receipts, 1);
    assert!(!reconciliation.missing_current);
    assert!(
        reconciliation.conflicts.is_empty(),
        "reconnect must not treat retained older object versions as conflicts"
    );
    assert!(matches!(
        dfs.read_local_object_with_state(&object_id, false)
            .expect("read after historical reconcile"),
        NetworkedDfsReadOutcome::Found { .. }
    ));
    let repeated = dfs
        .reconcile_local_manifests(&object_id)
        .expect("repeat reconcile historical manifest");
    assert_eq!(
        repeated.refreshed_receipts, 0,
        "known historical manifests should not cause repeat receipt refresh reports"
    );
    assert!(repeated.conflicts.is_empty());

    cleanup(&root);
}

#[test]
fn networked_dfs_reconnect_reconciliation_uses_backend_current_metadata() {
    let node = NodeId::from_legacy_named("node_reconcile_local").expect("node");
    let owner = NodeId::from_legacy_named("node_reconcile_owner").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_reconcile_backend").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"backend-current reconnect bytes";
    let hash = content_hash(bytes);
    let root = temp_root("networked_reconnect_backend_current");
    let network = InProcessNetwork::new();
    let backend_path = root.join("meta");
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("node-shard"), &backend_path)
            .expect("metadata backend");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("node")).expect("node store"),
    )
    .with_metadata_backend(backend)
    .expect("metadata backend");
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    dfs.add_file_system(file_system).expect("filesystem");
    dfs.content()
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("seed current local content");
    dfs.add_transient_object_metadata(DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone()],
        content_hash: Some(content_hash(b"stale bytes")),
        content_size_bytes: Some(11),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    })
    .expect("stale memory metadata");

    let mut backend_writer =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("node-shard"), &backend_path)
            .expect("metadata backend writer");
    backend_writer
        .put_object(&DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: "artifact.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: owner.clone(),
            current_locations: vec![owner.clone()],
            content_hash: Some(hash.clone()),
            content_size_bytes: Some(bytes.len() as u64),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version_2.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        })
        .expect("backend current object");

    let reconciliation = dfs
        .reconcile_local_manifests(&object_id)
        .expect("reconcile backend-current object");
    assert_eq!(reconciliation.refreshed_receipts, 1);
    assert!(!reconciliation.missing_current);
    assert!(reconciliation.conflicts.is_empty());

    let backend_object = dfs
        .metadata_backend()
        .expect("backend")
        .get_object(&object_id)
        .expect("backend object")
        .expect("backend object");
    assert_eq!(backend_object.version, version_2);
    assert_eq!(backend_object.content_hash.as_deref(), Some(hash.as_str()));
    assert_eq!(backend_object.current_locations, vec![owner, node.clone()]);
    assert_eq!(
        dfs.metadata_backend()
            .expect("backend")
            .receipts_for_version(&object_id, &version_2)
            .expect("receipts")
            .len(),
        1
    );
    assert_eq!(
        dfs.metadata()
            .object(&object_id)
            .expect("memory object")
            .version,
        version_2
    );

    cleanup(&root);
}

#[test]
fn networked_dfs_owner_handoff_requires_latest_content_and_routes_old_owner() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let candidate = NodeId::from_legacy_named("node_candidate").expect("node");
    let witness = NodeId::from_legacy_named("node_witness").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_owner_handoff");
    let network = InProcessNetwork::new();
    let control_plane = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![owner.clone(), candidate.clone(), witness])
            .expect("control-plane kv cluster"),
    );
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: owner.clone(),
                replica_node_ids: vec![candidate.clone()],
                epoch: 7,
                assignment: DfsMetadataShardAssignment::Root,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("shard");
    });
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner")).expect("owner store"),
    )
    .with_control_plane(control_plane.clone());
    let mut candidate_dfs = NetworkedDfsService::new(
        network.add_node(candidate.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("candidate")).expect("candidate store"),
    )
    .with_control_plane(control_plane.clone());
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "artifact.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone(), candidate.clone()],
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
        version: version_1.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner DfsFileSystem");
    candidate_dfs
        .add_file_system(file_system)
        .expect("candidate DfsFileSystem");
    owner_dfs
        .put_local_object_version(metadata.clone(), b"handoff bytes")
        .expect("owner content");
    candidate_dfs
        .put_local_object_version(metadata, b"handoff bytes")
        .expect("candidate content");

    let handoff = candidate_dfs
        .handoff_object_owner(&object_id, "root", 7)
        .expect("handoff");
    assert!(matches!(
        handoff,
        DfsOwnerHandoffOutcome::Transferred {
            previous_owner_node_id,
            new_owner_node_id,
            previous_epoch: 7,
            new_epoch: 8,
            ..
        } if previous_owner_node_id == owner && new_owner_node_id == candidate
    ));
    control_plane.with_client(|kv| {
        let shard = DfsControlPlaneStore::new(kv)
            .get_metadata_shard(&file_system_id, "root")
            .expect("shard")
            .expect("shard");
        assert_eq!(shard.primary_node_id, candidate);
        assert_eq!(shard.epoch, 8);
    });
    assert_eq!(
        candidate_dfs
            .metadata()
            .object(&object_id)
            .expect("candidate object")
            .owner_node_id,
        candidate
    );

    assert!(matches!(
        owner_dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id,
                    object_id: object_id.clone(),
                    target_version: version_2,
                    requesting_node_id: owner.clone(),
                },
                b"routed after handoff bytes".to_vec(),
                vec![candidate.clone()],
                DfsReplicationPolicy::new(1, 1).expect("policy"),
            )
            .expect("old owner routed write"),
        NetworkedDfsWriteOutcome::Pending { .. }
    ));
    let candidate_events = candidate_dfs
        .process_incoming()
        .expect("candidate routed write");
    assert!(candidate_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteWriteServed {
                requester_node_id,
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { .. },
                    },
                ..
            } if requester_node_id == &owner
        )
    }));
    let owner_events = owner_dfs.process_incoming().expect("owner routed response");
    assert!(owner_events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteWriteCompleted {
                primary_node_id,
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { .. },
                    },
                ..
            } if primary_node_id == &candidate
        )
    }));

    cleanup(&root);
}

#[test]
fn networked_dfs_owner_handoff_uses_backend_current_metadata() {
    let owner = NodeId::from_legacy_named("node_handoff_backend_owner").expect("node");
    let candidate = NodeId::from_legacy_named("node_handoff_backend_candidate").expect("node");
    let witness = NodeId::from_legacy_named("node_handoff_backend_witness").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_handoff_backend").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"backend current handoff bytes";
    let hash = content_hash(bytes);
    let root = temp_root("networked_owner_handoff_backend_current");
    let network = InProcessNetwork::new();
    let control_plane = SharedDfsControlPlaneKvClient::new(
        InProcessRaftKvCluster::new(vec![owner.clone(), candidate.clone(), witness])
            .expect("control-plane kv cluster"),
    );
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: owner.clone(),
                replica_node_ids: vec![candidate.clone()],
                epoch: 11,
                assignment: DfsMetadataShardAssignment::Root,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("shard");
    });
    let backend_path = root.join("candidate-meta");
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("candidate-shard"), &backend_path)
            .expect("candidate metadata backend");
    let mut candidate_dfs = NetworkedDfsService::new(
        network.add_node(candidate.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("candidate")).expect("candidate store"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(backend)
    .expect("candidate metadata backend");
    candidate_dfs
        .add_file_system(DfsFileSystem {
            file_system_id: file_system_id.clone(),
            display_name: "repo".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        })
        .expect("candidate filesystem");
    candidate_dfs
        .add_object_metadata(DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: "artifact.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: owner.clone(),
            current_locations: vec![owner.clone()],
            content_hash: Some(content_hash(b"stale handoff bytes")),
            content_size_bytes: Some(b"stale handoff bytes".len() as u64),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version_1,
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        })
        .expect("stale object metadata");
    candidate_dfs
        .content()
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("candidate latest content");

    let mut backend_writer =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("candidate-shard"), &backend_path)
            .expect("backend writer");
    backend_writer
        .put_object(&DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: "artifact.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: owner.clone(),
            current_locations: vec![owner.clone(), candidate.clone()],
            content_hash: Some(hash.clone()),
            content_size_bytes: Some(bytes.len() as u64),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version_2.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        })
        .expect("backend current object");

    let handoff = candidate_dfs
        .handoff_object_owner(&object_id, "root", 11)
        .expect("handoff");
    assert!(matches!(
        handoff,
        DfsOwnerHandoffOutcome::Transferred {
            previous_owner_node_id,
            new_owner_node_id,
            previous_epoch: 11,
            new_epoch: 12,
            ..
        } if previous_owner_node_id == owner && new_owner_node_id == candidate
    ));
    let candidate_object = candidate_dfs
        .metadata()
        .object(&object_id)
        .expect("candidate memory object");
    assert_eq!(candidate_object.version, version_2);
    assert_eq!(
        candidate_object.content_hash.as_deref(),
        Some(hash.as_str())
    );
    assert_eq!(candidate_object.owner_node_id, candidate);
    assert!(candidate_object.current_locations.contains(&candidate));

    cleanup(&root);
}

fn temp_root(name: &str) -> sinomo_testdir::TestDir {
    sinomo_testdir::TestDir::new(&format!("sinomo-dfs-{name}"))
}

fn content_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

struct PatternReader {
    remaining: usize,
    index: usize,
}

impl PatternReader {
    fn new(bytes: usize) -> Self {
        Self {
            remaining: bytes,
            index: 0,
        }
    }
}

impl Read for PatternReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if self.remaining == 0 || buf.is_empty() {
            return Ok(0);
        }
        let to_fill = buf.len().min(self.remaining);
        for byte in &mut buf[..to_fill] {
            *byte = (self.index % 251) as u8;
            self.index = self.index.saturating_add(1);
        }
        self.remaining -= to_fill;
        Ok(to_fill)
    }
}

fn pattern_bytes(bytes: usize) -> Vec<u8> {
    (0..bytes).map(|index| (index % 251) as u8).collect()
}

fn blob_path_for_hash(root: &Path, hash: &str) -> PathBuf {
    let hex = hash.strip_prefix("blake3:").expect("blake3 hash");
    root.join("blobs").join(&hex[..2]).join(hex)
}

fn send_dfs_test_message<T: DfsFrameTransport>(
    transport: &mut T,
    source: &NodeId,
    target: &NodeId,
    message: DfsNetworkMessage,
) {
    let protocol_message = ProtocolMessage::encode(DFS_PROTOCOL, &message).expect("protocol");
    transport
        .send(
            target,
            NetworkFrame {
                frame_id: NetworkFrameId(1),
                source_node_id: source.clone(),
                target_node_id: target.clone(),
                message: MeshMessage {
                    message_id: MeshMessageId(1),
                    origin_node_id: source.clone(),
                    target_node_id: target.clone(),
                    body: MeshMessageBody::Protocol(protocol_message),
                },
            },
        )
        .expect("send DFS test message");
}

fn cleanup(root: &Path) {
    if root.exists() {
        fs::remove_dir_all(root).expect("cleanup temp dir");
    }
}
