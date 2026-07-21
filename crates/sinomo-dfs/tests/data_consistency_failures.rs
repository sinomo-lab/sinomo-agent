//! Integration tests for DFS data consistency under common distributed failure modes.
//!
//! These tests exercise write coordination, metadata commit ordering, hydration,
//! concurrent writers, partial failures, and repair convergence.

use sinomo_core::network::{InProcessNetwork, InProcessTransport};
use sinomo_core::node::NodeId;
use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};
use sinomo_dfs::{
    DfsFileSystem, DfsFileSystemId, DfsMetadataShardId, DfsObjectId, DfsObjectKind,
    DfsObjectMetadata, DfsReplicationPolicy, DfsVersion, DfsWriteIntentState, DfsWriteIntentStatus,
    LocalSobjMetadataBackend, NetworkedDfsReadOutcome, NetworkedDfsService,
    NetworkedDfsServiceEvent, NetworkedDfsWriteOutcome,
};
use std::fs;
use std::path::Path;

#[test]
fn coordinator_stays_awaiting_receipts_until_metadata_commit_applies() {
    let owner = node("owner");
    let replica = node("replica");
    let file_system_id = fs_id();
    let object_id = object_id("obj_commit_order");
    let version_1 = version("version_1");
    let version_2 = version("version_2");
    let bytes = b"commit ordering bytes";
    let root = temp_root("commit_order");

    let mut fixture = ClusterFixture::new(&root, &[owner.clone(), replica.clone()]);
    fixture.seed_replicated_object(
        &owner,
        object_id.clone(),
        file_system_id.clone(),
        version_1.clone(),
    );

    let write = fixture
        .service_mut(&owner)
        .write_distributed_eventual(
            write_request(&file_system_id, &object_id, &version_2, &owner),
            bytes.to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("start write");

    let intent_id = pending_intent_id(write);
    let owner_dfs = fixture.service(&owner);
    assert_eq!(
        owner_dfs.coordinator().intent(&intent_id).unwrap().state,
        DfsWriteIntentState::AwaitingReceipts,
        "coordinator must not commit before durable metadata advance"
    );
    assert_eq!(
        visible_version(owner_dfs, &object_id),
        version_1,
        "visible metadata must stay at base version until commit applies"
    );
    assert!(
        !owner_dfs.coordinator().is_commit_ready(&intent_id).unwrap(),
        "commit threshold must not be met until the remote replica acknowledges"
    );

    fixture
        .service_mut(&replica)
        .process_incoming()
        .expect("replica");
    fixture
        .service_mut(&owner)
        .process_incoming()
        .expect("owner ack");
    let owner_dfs = fixture.service(&owner);
    assert_eq!(
        owner_dfs.coordinator().intent(&intent_id).unwrap().state,
        DfsWriteIntentState::Committed,
        "coordinator should commit only after metadata apply"
    );
    assert_consistent_committed_object(owner_dfs, &object_id, &version_2, bytes, &[owner.clone()]);

    cleanup(&root);
}

#[test]
fn concurrent_disjoint_replica_writers_do_not_fork_visible_version() {
    let owner = node("owner");
    let writer_b = node("writer_b");
    let writer_c = node("writer_c");
    let file_system_id = fs_id();
    let object_id = object_id("obj_disjoint_replicas");
    let version_1 = version("version_1");
    let version_b = version("version_b");
    let version_c = version("version_c");
    let root = temp_root("disjoint_replicas");

    let mut fixture =
        ClusterFixture::new(&root, &[owner.clone(), writer_b.clone(), writer_c.clone()]);
    fixture.seed_replicated_object_with_locations(
        &owner,
        object_id.clone(),
        file_system_id.clone(),
        version_1.clone(),
        vec![writer_b.clone(), writer_c.clone()],
    );

    assert!(matches!(
        fixture
            .service_mut(&writer_b)
            .write_distributed_eventual(
                write_request(&file_system_id, &object_id, &version_b, &writer_b),
                b"writer b".to_vec(),
                vec![writer_b.clone(), writer_c.clone()],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect("writer b"),
        NetworkedDfsWriteOutcome::Pending { .. }
    ));
    assert!(matches!(
        fixture
            .service_mut(&writer_c)
            .write_distributed_eventual(
                write_request(&file_system_id, &object_id, &version_c, &writer_c),
                b"writer c".to_vec(),
                vec![writer_c.clone(), writer_b.clone()],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect("writer c"),
        NetworkedDfsWriteOutcome::Pending { .. }
    ));

    let c_events = fixture
        .service_mut(&writer_c)
        .process_incoming()
        .expect("writer c");
    assert!(c_events.iter().any(|event| matches!(
        event,
        NetworkedDfsServiceEvent::BlobStageRejected { reason, .. }
            if reason.contains("conflicting pending DFS stage")
    )));

    let b_events = fixture
        .service_mut(&writer_b)
        .process_incoming()
        .expect("writer b");
    assert!(b_events.iter().any(|event| matches!(
        event,
        NetworkedDfsServiceEvent::BlobStageRejected { reason, .. }
            if reason.contains("conflicting pending DFS stage")
    )));

    for (name, node_id) in [
        ("owner", &owner),
        ("writer_b", &writer_b),
        ("writer_c", &writer_c),
    ] {
        let object = fixture
            .service(node_id)
            .metadata()
            .object(&object_id)
            .unwrap_or_else(|| panic!("{name} metadata"));
        assert_eq!(
            object.version, version_1,
            "{name} must not observe a forked visible version"
        );
        assert!(
            object.content_hash.is_none(),
            "{name} must not observe speculative content hash"
        );
    }

    cleanup(&root);
}

#[test]
fn restart_hydration_restores_backend_visible_version_without_fake_receipts() {
    let owner = node("owner");
    let replica = node("replica");
    let file_system_id = fs_id();
    let object_id = object_id("obj_hydrate_restart");
    let version_1 = version("version_1");
    let version_2 = version("version_2");
    let bytes = b"hydrated visible version";
    let root = temp_root("hydrate_restart");
    let metadata_root = root.join("metadata");
    let content_root = root.join("content");

    let network = InProcessNetwork::new();
    let backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
        .expect("backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(&content_root).expect("content"),
    )
    .with_metadata_backend(backend)
    .expect("metadata backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica content"),
    );
    seed_replicated_object_on(
        &mut owner_dfs,
        &mut replica_dfs,
        owner.clone(),
        object_id.clone(),
        file_system_id.clone(),
        version_1.clone(),
    );

    let write = owner_dfs
        .write_distributed_eventual(
            write_request(&file_system_id, &object_id, &version_2, &owner),
            bytes.to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("write");
    assert!(matches!(write, NetworkedDfsWriteOutcome::Committed { .. }));
    // Leave the remote replica un-staged so hydration cannot verify it.
    drop(owner_dfs);
    drop(replica_dfs);

    let recovered = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(&content_root).expect("recovered content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend"),
    )
    .expect("hydrate");

    assert_consistent_committed_object(&recovered, &object_id, &version_2, bytes, &[owner.clone()]);
    let backend_receipts = recovered
        .metadata_backend()
        .expect("backend")
        .receipts_for_version(&object_id, &version_2)
        .expect("receipts");
    assert!(
        backend_receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == owner),
        "hydration should restore verified owner receipt"
    );
    assert!(
        !backend_receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == replica),
        "hydration must not invent receipts for unverified remote replicas"
    );
    assert_eq!(
        recovered.coordinator().repair_jobs().len(),
        1,
        "missing remote replica should remain explicit repair work"
    );

    cleanup(&root);
}

#[test]
fn metadata_backend_pending_intent_blocks_second_write() {
    let owner = node("owner");
    let replica = node("replica");
    let file_system_id = fs_id();
    let object_id = object_id("obj_pending_intent_block");
    let version_1 = version("version_1");
    let version_2 = version("version_2");
    let version_3 = version("version_3");
    let root = temp_root("pending_intent_block");
    let metadata_root = root.join("metadata");

    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("backend"),
    )
    .expect("metadata backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        sinomo_dfs::LocalContentStore::new(root.join("replica")).expect("replica content"),
    );

    seed_replicated_object_on(
        &mut owner_dfs,
        &mut replica_dfs,
        owner.clone(),
        object_id.clone(),
        file_system_id.clone(),
        version_1.clone(),
    );

    assert!(matches!(
        owner_dfs
            .write_distributed_eventual(
                write_request(&file_system_id, &object_id, &version_2, &owner),
                b"first write".to_vec(),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect("first write"),
        NetworkedDfsWriteOutcome::Pending { .. }
    ));

    let second = owner_dfs.write_distributed_eventual(
        write_request(&file_system_id, &object_id, &version_3, &owner),
        b"second write".to_vec(),
        vec![owner.clone(), replica.clone()],
        DfsReplicationPolicy::new(2, 2).expect("policy"),
    );
    assert!(
        second.is_err(),
        "second write should reject while backend still records the first intent, got {second:?}"
    );
    assert!(
        owner_dfs
            .open_conflicts_for_object(&object_id)
            .expect("open conflicts")
            .is_empty(),
        "pending-intent rejection must not create a DFS conflict"
    );
    assert_eq!(
        visible_version(&owner_dfs, &object_id),
        version_1,
        "visible version must remain the base version while the first write is pending"
    );

    cleanup(&root);
}

#[test]
fn stale_base_version_write_is_rejected_without_metadata_advance() {
    let owner = node("owner");
    let file_system_id = fs_id();
    let object_id = object_id("obj_stale_base");
    let version_1 = version("version_1");
    let version_2 = version("version_2");
    let version_3 = version("version_3");
    let root = temp_root("stale_base");

    let mut fixture = ClusterFixture::new(&root, &[owner.clone()]);
    fixture.seed_replicated_object(
        &owner,
        object_id.clone(),
        file_system_id.clone(),
        version_1.clone(),
    );

    assert!(matches!(
        fixture
            .service_mut(&owner)
            .write_distributed_eventual(
                write_request(&file_system_id, &object_id, &version_2, &owner),
                b"current write".to_vec(),
                vec![owner.clone()],
                DfsReplicationPolicy::new(1, 1).expect("policy"),
            )
            .expect("current write"),
        NetworkedDfsWriteOutcome::Committed { .. }
    ));

    let stale = fixture
        .service_mut(&owner)
        .write_distributed_eventual_with_base(
            write_request(&file_system_id, &object_id, &version_3, &owner),
            Some(version_1.clone()),
            b"stale write".to_vec(),
            vec![owner.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect_err("stale write must reject");

    match stale {
        sinomo_dfs::DfsError::Distributed { message } => {
            assert!(
                message.contains("stale DFS write base"),
                "unexpected stale write error: {message}"
            );
        }
        other => panic!("expected stale base rejection, got {other:?}"),
    }
    assert!(
        fixture
            .service(&owner)
            .open_conflicts_for_object(&object_id)
            .expect("open conflicts")
            .is_empty(),
        "stale base rejection must not create a DFS conflict"
    );
    assert_consistent_committed_metadata(
        fixture.service(&owner),
        &object_id,
        &version_2,
        b"current write",
        &[owner.clone()],
    );

    cleanup(&root);
}

#[test]
fn partial_receipt_threshold_defers_visible_version_until_repairable_commit() {
    let owner = node("owner");
    let replica_a = node("replica_a");
    let replica_b = node("replica_b");
    let file_system_id = fs_id();
    let object_id = object_id("obj_partial_threshold");
    let version_1 = version("version_1");
    let version_2 = version("version_2");
    let bytes = b"threshold commit bytes";
    let root = temp_root("partial_threshold");

    let mut fixture = ClusterFixture::new(
        &root,
        &[owner.clone(), replica_a.clone(), replica_b.clone()],
    );
    fixture.seed_replicated_object(
        &owner,
        object_id.clone(),
        file_system_id.clone(),
        version_1.clone(),
    );

    let write = fixture
        .service_mut(&owner)
        .write_distributed_eventual(
            write_request(&file_system_id, &object_id, &version_2, &owner),
            bytes.to_vec(),
            vec![owner.clone(), replica_a.clone(), replica_b.clone()],
            DfsReplicationPolicy::new(3, 3).expect("policy"),
        )
        .expect("write");
    let intent_id = pending_intent_id(write);
    assert_eq!(
        visible_version(fixture.service(&owner), &object_id),
        version_1
    );

    fixture
        .service_mut(&replica_a)
        .process_incoming()
        .expect("a");
    fixture
        .service_mut(&owner)
        .process_incoming()
        .expect("owner after a");
    assert_eq!(
        fixture
            .service(&owner)
            .coordinator()
            .intent(&intent_id)
            .unwrap()
            .state,
        DfsWriteIntentState::AwaitingReceipts
    );
    assert_eq!(
        visible_version(fixture.service(&owner), &object_id),
        version_1
    );

    fixture
        .service_mut(&replica_b)
        .process_incoming()
        .expect("b");
    fixture
        .service_mut(&owner)
        .process_incoming()
        .expect("owner after b");
    assert_eq!(
        fixture
            .service(&owner)
            .coordinator()
            .intent(&intent_id)
            .unwrap()
            .state,
        DfsWriteIntentState::Committed
    );
    assert_consistent_committed_object(
        fixture.service(&owner),
        &object_id,
        &version_2,
        bytes,
        &[owner.clone(), replica_a.clone(), replica_b.clone()],
    );

    cleanup(&root);
}

#[test]
fn offline_replica_converges_after_deferred_post_commit_promotion() {
    let owner = node("owner");
    let online = node("online");
    let offline = node("offline");
    let file_system_id = fs_id();
    let object_id = object_id("obj_offline_repair");
    let version_1 = version("version_1");
    let version_2 = version("version_2");
    let bytes = b"repair convergence bytes";
    let root = temp_root("offline_repair");

    let mut fixture = ClusterFixture::new(&root, &[owner.clone(), online.clone(), offline.clone()]);
    fixture.seed_replicated_object(
        &owner,
        object_id.clone(),
        file_system_id.clone(),
        version_1.clone(),
    );

    let write = fixture
        .service_mut(&owner)
        .write_distributed_eventual(
            write_request(&file_system_id, &object_id, &version_2, &owner),
            bytes.to_vec(),
            vec![owner.clone(), online.clone(), offline.clone()],
            DfsReplicationPolicy::new(2, 3).expect("policy"),
        )
        .expect("write");
    let intent_id = pending_intent_id(write);

    fixture
        .service_mut(&online)
        .process_incoming()
        .expect("online");
    fixture
        .service_mut(&owner)
        .process_incoming()
        .expect("owner");
    assert_eq!(
        fixture
            .service(&owner)
            .coordinator()
            .intent(&intent_id)
            .unwrap()
            .state,
        DfsWriteIntentState::Committed
    );
    assert_eq!(fixture.service(&owner).coordinator().repair_jobs().len(), 1);

    for _ in 0..8 {
        fixture
            .service_mut(&owner)
            .retry_pending_repairs()
            .expect("retry");
        fixture
            .service_mut(&offline)
            .process_incoming()
            .expect("offline");
        fixture
            .service_mut(&owner)
            .process_incoming()
            .expect("owner");
        if fixture
            .service(&offline)
            .content()
            .read_manifest(object_id.as_str(), version_2.as_str())
            .is_ok()
        {
            break;
        }
    }

    let staged_read = fixture
        .service(&offline)
        .read_local_object_with_state(&object_id, false)
        .expect("offline staged read");
    assert!(
        !matches!(staged_read, NetworkedDfsReadOutcome::Found { .. }),
        "a durable late stage must not become visible before deferred CommitVersion promotion"
    );
    fixture
        .service_mut(&offline)
        .process_incoming()
        .expect("offline deferred promotion");
    let promoted_read = fixture
        .service(&offline)
        .read_local_object_with_state(&object_id, false)
        .expect("offline promoted read");
    assert!(matches!(
        promoted_read,
        NetworkedDfsReadOutcome::Found { .. }
    ));
    assert_eq!(
        fixture.service(&owner).coordinator().repair_jobs().len(),
        0,
        "repair should converge offline replica to committed version"
    );

    cleanup(&root);
}

#[test]
fn duplicate_replica_ack_does_not_change_visible_version() {
    let owner = node("owner");
    let replica = node("replica");
    let file_system_id = fs_id();
    let object_id = object_id("obj_duplicate_ack");
    let version_1 = version("version_1");
    let version_2 = version("version_2");
    let bytes = b"duplicate ack bytes";
    let root = temp_root("duplicate_ack");

    let mut fixture = ClusterFixture::new(&root, &[owner.clone(), replica.clone()]);
    fixture.seed_replicated_object(
        &owner,
        object_id.clone(),
        file_system_id.clone(),
        version_1.clone(),
    );

    let write = fixture
        .service_mut(&owner)
        .write_distributed_eventual(
            write_request(&file_system_id, &object_id, &version_2, &owner),
            bytes.to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("write");
    let intent_id = pending_intent_id(write);

    fixture
        .service_mut(&replica)
        .process_incoming()
        .expect("replica");
    fixture
        .service_mut(&owner)
        .process_incoming()
        .expect("owner");
    assert_consistent_committed_object(
        fixture.service(&owner),
        &object_id,
        &version_2,
        bytes,
        &[owner.clone(), replica.clone()],
    );

    fixture.service_mut(&owner).process_incoming().expect("dup");
    fixture.service_mut(&owner).process_incoming().expect("dup");
    assert_consistent_committed_object(
        fixture.service(&owner),
        &object_id,
        &version_2,
        bytes,
        &[owner.clone(), replica.clone()],
    );
    assert_eq!(
        fixture
            .service(&owner)
            .coordinator()
            .durable_receipt_count(&intent_id),
        2
    );

    cleanup(&root);
}

// --- helpers ---

struct ClusterFixture {
    _network: InProcessNetwork,
    services: Vec<(NodeId, NetworkedDfsService<InProcessTransport>)>,
}

impl ClusterFixture {
    fn new(root: &Path, node_ids: &[NodeId]) -> Self {
        let network = InProcessNetwork::new();
        let services = node_ids
            .iter()
            .enumerate()
            .map(|(index, node_id)| {
                let dfs = NetworkedDfsService::new(
                    network.add_node(node_id.clone()),
                    sinomo_dfs::LocalContentStore::new(root.join(format!("node-{index}")))
                        .expect("content store"),
                );
                (node_id.clone(), dfs)
            })
            .collect();
        Self {
            _network: network,
            services,
        }
    }

    fn service(&self, node_id: &NodeId) -> &NetworkedDfsService<InProcessTransport> {
        self.services
            .iter()
            .find(|(id, _)| id == node_id)
            .map(|(_, dfs)| dfs)
            .unwrap_or_else(|| panic!("unknown node {node_id}"))
    }

    fn service_mut(&mut self, node_id: &NodeId) -> &mut NetworkedDfsService<InProcessTransport> {
        self.services
            .iter_mut()
            .find(|(id, _)| id == node_id)
            .map(|(_, dfs)| dfs)
            .unwrap_or_else(|| panic!("unknown node {node_id}"))
    }

    fn seed_replicated_object_with_locations(
        &mut self,
        owner: &NodeId,
        object_id: DfsObjectId,
        file_system_id: DfsFileSystemId,
        version: DfsVersion,
        locations: Vec<NodeId>,
    ) {
        let file_system = DfsFileSystem {
            file_system_id: file_system_id.clone(),
            display_name: "repo".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        };
        let metadata = replicated_object_metadata_with_locations(
            object_id,
            file_system_id,
            owner.clone(),
            version,
            "consistency.bin",
            locations,
        );
        for (_, dfs) in &mut self.services {
            dfs.add_file_system(file_system.clone())
                .expect("filesystem");
            dfs.add_object_metadata(metadata.clone()).expect("object");
        }
    }

    fn seed_replicated_object(
        &mut self,
        owner: &NodeId,
        object_id: DfsObjectId,
        file_system_id: DfsFileSystemId,
        version: DfsVersion,
    ) {
        let file_system = DfsFileSystem {
            file_system_id: file_system_id.clone(),
            display_name: "repo".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        };
        let metadata = replicated_object_metadata(
            object_id,
            file_system_id,
            owner.clone(),
            version,
            "consistency.bin",
        );
        for (_, dfs) in &mut self.services {
            dfs.add_file_system(file_system.clone())
                .expect("filesystem");
            dfs.add_object_metadata(metadata.clone()).expect("object");
        }
    }

    #[allow(dead_code)]
    fn drain_once(&mut self) {
        for (_, dfs) in &mut self.services {
            let _ = dfs.process_incoming();
        }
    }
}

fn seed_replicated_object_on(
    owner_dfs: &mut NetworkedDfsService<InProcessTransport>,
    replica_dfs: &mut NetworkedDfsService<InProcessTransport>,
    owner: NodeId,
    object_id: DfsObjectId,
    file_system_id: DfsFileSystemId,
    version: DfsVersion,
) {
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata =
        replicated_object_metadata(object_id, file_system_id, owner, version, "consistency.bin");
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner fs");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica fs");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");
}

#[allow(dead_code)]
fn seed_replicated_object_on_single(
    owner_dfs: &mut NetworkedDfsService<InProcessTransport>,
    owner: NodeId,
    object_id: DfsObjectId,
    file_system_id: DfsFileSystemId,
    version: DfsVersion,
) {
    let file_system = DfsFileSystem {
        file_system_id: file_system_id.clone(),
        display_name: "repo".to_string(),
        owner_node_id: owner.clone(),
        subtree_policies: Vec::new(),
    };
    let metadata =
        replicated_object_metadata(object_id, file_system_id, owner, version, "consistency.bin");
    owner_dfs.add_file_system(file_system).expect("filesystem");
    owner_dfs.add_object_metadata(metadata).expect("object");
}

fn replicated_object_metadata(
    object_id: DfsObjectId,
    file_system_id: DfsFileSystemId,
    owner: NodeId,
    version: DfsVersion,
    path: &str,
) -> DfsObjectMetadata {
    replicated_object_metadata_with_locations(
        object_id,
        file_system_id,
        owner.clone(),
        version,
        path,
        vec![owner],
    )
}

fn replicated_object_metadata_with_locations(
    object_id: DfsObjectId,
    file_system_id: DfsFileSystemId,
    owner: NodeId,
    version: DfsVersion,
    path: &str,
    locations: Vec<NodeId>,
) -> DfsObjectMetadata {
    DfsObjectMetadata {
        object_id,
        file_system_id,
        path: path.to_string(),
        kind: DfsObjectKind::Artifact,
        tombstone_provenance: None,
        owner_node_id: owner.clone(),
        current_locations: locations,
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
    }
}

fn assert_consistent_committed_metadata(
    dfs: &NetworkedDfsService<InProcessTransport>,
    object_id: &DfsObjectId,
    expected_version: &DfsVersion,
    expected_bytes: &[u8],
    expected_locations: &[NodeId],
) {
    let object = dfs
        .metadata()
        .object(object_id)
        .unwrap_or_else(|| panic!("missing object metadata for {object_id}"));
    assert_eq!(&object.version, expected_version);
    let expected_hash = content_hash(expected_bytes);
    assert_eq!(object.content_hash.as_deref(), Some(expected_hash.as_str()));
    for location in expected_locations {
        assert!(
            object.current_locations.contains(location),
            "missing durable location {location}"
        );
    }
    if let Some(backend) = dfs.metadata_backend() {
        let backend_object = backend
            .get_object(object_id)
            .expect("backend object")
            .unwrap_or_else(|| panic!("backend missing object {object_id}"));
        assert_eq!(&backend_object.version, expected_version);
        assert_eq!(
            backend_object.content_hash.as_deref(),
            Some(expected_hash.as_str())
        );
    }
}

fn assert_consistent_committed_object(
    dfs: &NetworkedDfsService<InProcessTransport>,
    object_id: &DfsObjectId,
    expected_version: &DfsVersion,
    expected_bytes: &[u8],
    expected_locations: &[NodeId],
) {
    let object = dfs
        .metadata()
        .object(object_id)
        .unwrap_or_else(|| panic!("missing object metadata for {object_id}"));
    assert_eq!(&object.version, expected_version);
    let expected_hash = content_hash(expected_bytes);
    assert_eq!(object.content_hash.as_deref(), Some(expected_hash.as_str()));
    for location in expected_locations {
        assert!(
            object.current_locations.contains(location),
            "missing durable location {location}"
        );
    }
    if let Some(backend) = dfs.metadata_backend() {
        let backend_object = backend
            .get_object(object_id)
            .expect("backend object")
            .unwrap_or_else(|| panic!("backend missing object {object_id}"));
        assert_eq!(&backend_object.version, expected_version);
        assert_eq!(
            backend_object.content_hash.as_deref(),
            Some(expected_hash.as_str())
        );
        if let Some(intent) = backend
            .intents_for_object(object_id)
            .expect("intents")
            .into_iter()
            .find(|intent| intent.target_version == *expected_version)
        {
            assert_eq!(intent.status, DfsWriteIntentStatus::Committed);
        }
    }
    let read = dfs
        .read_local_object_with_state(object_id, false)
        .expect("local read");
    match read {
        NetworkedDfsReadOutcome::Found { data, .. } => {
            assert_eq!(data.bytes, expected_bytes);
            assert_eq!(data.version, *expected_version);
        }
        other => panic!("expected current local bytes, got {other:?}"),
    }
}

fn visible_version(
    dfs: &NetworkedDfsService<InProcessTransport>,
    object_id: &DfsObjectId,
) -> DfsVersion {
    dfs.metadata()
        .object(object_id)
        .unwrap_or_else(|| panic!("missing object {object_id}"))
        .version
        .clone()
}

fn pending_intent_id(outcome: NetworkedDfsWriteOutcome) -> String {
    match outcome {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        NetworkedDfsWriteOutcome::Committed { intent_id, .. } => intent_id,
        NetworkedDfsWriteOutcome::Conflict { .. } => {
            panic!("expected pending or committed write outcome")
        }
    }
}

fn write_request(
    file_system_id: &DfsFileSystemId,
    object_id: &DfsObjectId,
    target_version: &DfsVersion,
    requester: &NodeId,
) -> DfsWriteRequest {
    DfsWriteRequest {
        file_system_id: file_system_id.clone(),
        object_id: object_id.clone(),
        target_version: target_version.clone(),
        requesting_node_id: requester.clone(),
    }
}

fn node(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).expect("node id")
}

fn fs_id() -> DfsFileSystemId {
    DfsFileSystemId::from_legacy_named("sifs").expect("filesystem id")
}

fn object_id(name: &str) -> DfsObjectId {
    DfsObjectId::from_legacy_named(name).expect("object id")
}

fn version(name: &str) -> DfsVersion {
    DfsVersion::from_legacy_named(name).expect("version")
}

fn content_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

fn temp_root(name: &str) -> sinomo_testdir::TestDir {
    sinomo_testdir::TestDir::new(&format!("sinomo-dfs-{name}"))
}

fn cleanup(root: &Path) {
    if root.exists() {
        fs::remove_dir_all(root).expect("cleanup temp dir");
    }
}
