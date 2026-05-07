use sinomo_core::{
    DfsFileSystemId, DfsObjectId, DfsObjectPolicy, DfsVersion, DfsWriteRequest, NodeId,
};
use sinomo_dfs::{
    DfsConfirmedTimeoutAction, DfsConfirmedTimeoutOutcome, DfsConflictStatus, DfsControlPlaneStore,
    DfsFileSystem, DfsMetadataMutation, DfsMetadataShardAssignment, DfsMetadataShardConfig,
    DfsMetadataShardId, DfsNodeAdvertisement, DfsObjectKind, DfsObjectMetadata,
    DfsOwnerHandoffOutcome, DfsReplicationPolicy, DfsWriteIntentState, DfsWriteIntentStatus,
    LocalJsonMetadataBackend, NetworkedDfsReadFreshness, NetworkedDfsReadOutcome,
    NetworkedDfsService, NetworkedDfsServiceEvent, NetworkedDfsWriteOutcome,
    SharedDfsControlPlaneKvClient, file_system_config_key, sifs_node_files_path,
};
use sinomo_kv::InProcessRaftKvCluster;
use sinomo_network::InProcessNetwork;
use std::fs;
use std::path::{Path, PathBuf};
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
    .with_metadata_backend(backend);

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
    .with_metadata_backend(owner_backend);
    let replica_backend = LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("replica"),
        root.join("replica-meta"),
    )
    .expect("replica metadata");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend);

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
    .with_metadata_backend(owner_backend);
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
    .with_metadata_backend(owner_backend);
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
    .with_metadata_backend(owner_backend);
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
    .with_metadata_backend(backend);
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
    .with_metadata_backend(backend);
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
