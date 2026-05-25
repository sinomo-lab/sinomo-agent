use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};
use sinomo_dfs::{
    DfsConfirmedTimeoutAction, DfsConfirmedTimeoutOutcome, DfsConflictStatus, DfsControlPlaneStore,
    DfsFileSystem, DfsFileSystemId, DfsMetadataMutation, DfsMetadataShardAssignment,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsNodeAdvertisement, DfsObjectId, DfsObjectKind,
    DfsObjectMetadata, DfsOwnerHandoffOutcome, DfsReplicationPolicy, DfsVersion,
    DfsWriteIntentState, DfsWriteIntentStatus, LocalJsonMetadataBackend, NetworkedDfsReadFreshness,
    NetworkedDfsReadOutcome, NetworkedDfsService, NetworkedDfsServiceEvent,
    NetworkedDfsWriteOutcome, SharedDfsControlPlaneKvClient, file_system_config_key,
    sifs_node_files_path,
};
use sinomo_kv::InProcessRaftKvCluster;
use sinomo_network::InProcessNetwork;
use sinomo_node::NodeId;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn networked_dfs_commits_after_stage_ack_and_replica_reads_content() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
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

    let bytes: Vec<u8> = (0..150_000).map(|index| (index % 251) as u8).collect();
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
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &owner_meta)
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &owner_meta)
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

    let replica_backend = LocalJsonMetadataBackend::new(
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
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_alternate_repair").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_alternate_repair_fetch");
    let network = InProcessNetwork::new();
    let owner_meta = root.join("owner-meta");
    let owner_content = root.join("owner-content");
    let replica_content = root.join("replica-content");
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
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &owner_meta)
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
                b"alternate repair bytes".to_vec(),
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
            b"alternate repair bytes"
        );
    }

    fs::remove_dir_all(&owner_content).expect("remove owner content");
    let recovered_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &owner_meta)
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
    assert_eq!(read.bytes, b"alternate repair bytes");

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

    let write = attacker_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: attacker,
            },
            b"unauthorized bytes".to_vec(),
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
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
fn posix_facade_supports_service_file_workflow_and_repairs_offline_replica() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let root = temp_root("posix_service_workflow");
    let network = InProcessNetwork::new();
    let owner_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
            .expect("owner metadata");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    let replica_backend = LocalJsonMetadataBackend::new(
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner-shard"), root.join("meta"))
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
fn networked_dfs_threshold_commit_repairs_missing_desired_replica() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_artifact").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let root = temp_root("networked_threshold_repair");
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

    let repair_events = replica_dfs.process_incoming().expect("repair");
    assert!(repair_events.iter().any(|event| {
        matches!(
            event,
            sinomo_dfs::NetworkedDfsServiceEvent::ReplicaRepaired { .. }
        )
    }));
    let read = replica_dfs
        .read_local_object(&object_id)
        .expect("read")
        .expect("data");
    assert_eq!(read.bytes, b"repair bytes");
    assert_eq!(read.version, version_2);

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
    assert!(
        reader_dfs
            .metadata()
            .object(&stat.object_id)
            .expect("reader metadata")
            .current_locations
            .contains(&reader)
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
    assert!(
        reader_dfs
            .metadata()
            .object(&link_stat.object_id)
            .expect("reader symlink metadata")
            .current_locations
            .contains(&reader)
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

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"retry repair bytes".to_vec(),
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
        b"retry repair bytes"
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("owner-meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner-shard"), root.join("meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner-shard"), root.join("meta"))
            .expect("metadata backend");
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
            vec![owner.clone(), replica],
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner-shard"), root.join("meta"))
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
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner-shard"), root.join("meta"))
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

    cleanup(&root);
}

#[test]
fn networked_dfs_owner_handoff_requires_latest_content_and_fences_old_owner() {
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
        owner_dfs.write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"stale owner bytes".to_vec(),
            vec![owner.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        ),
        Err(sinomo_dfs::DfsError::StaleOwnerEpoch { .. })
    ));
    assert!(matches!(
        candidate_dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id,
                    object_id,
                    target_version: version_2,
                    requesting_node_id: candidate.clone(),
                },
                b"new owner bytes".to_vec(),
                vec![candidate],
                DfsReplicationPolicy::new(1, 1).expect("policy"),
            )
            .expect("new owner write"),
        NetworkedDfsWriteOutcome::Committed { .. }
    ));

    cleanup(&root);
}

fn temp_root(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("sinomo-dfs-{name}-{nanos}"))
}

fn cleanup(root: &Path) {
    if root.exists() {
        fs::remove_dir_all(root).expect("cleanup temp dir");
    }
}
