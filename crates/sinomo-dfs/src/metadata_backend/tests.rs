use super::*;
use crate::LocalShardedMetadataBackend;
use crate::advisory_lock::prepare_advisory_lock_state_for_migration;
use crate::metadata_journal::{
    JOURNAL_SEGMENT_ENTRY_LIMIT, append_segmented_journal_entry, metadata_journal_batch_encoded_len,
};
use crate::metadata_keys::{
    dentry_key, key_segment, normalize_path, path_hash, path_record_for_object,
};
use crate::{
    DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_ENTRIES, DfsAdvisoryLockMode, DfsAdvisoryLockOutcome,
    DfsAdvisoryLockRequestId, DfsAdvisoryLockSessionId, DfsContentRootCatalogShardIdentity,
    DfsMetadataAuthorityOperation, DfsMetadataAuthorityProbe, DfsMetadataShardAssignment,
    DfsNamespaceOperation, DfsNamespaceResult, DfsObjectKind,
    dfs_content_root_catalog_topology_digest,
};
use sinomo_core::DfsObjectPolicy;
use std::time::Duration;

fn node(value: &str) -> NodeId {
    NodeId::from_legacy_named(value).expect("valid node id")
}

fn file_system_id() -> DfsFileSystemId {
    DfsFileSystemId::from_legacy_named("sifs").expect("valid filesystem id")
}

fn object_id() -> DfsObjectId {
    DfsObjectId::from_legacy_named("obj_readme").expect("valid object id")
}

fn version(value: &str) -> DfsVersion {
    DfsVersion::from_legacy_named(value).expect("valid version")
}

fn file_system() -> DfsFileSystem {
    DfsFileSystem {
        file_history: Default::default(),
        file_system_id: file_system_id(),
        display_name: "repo".to_string(),
        owner_node_id: node("node_owner"),
        subtree_policies: Vec::new(),
    }
}

fn object(version: DfsVersion) -> DfsObjectMetadata {
    DfsObjectMetadata {
        object_id: object_id(),
        file_system_id: file_system_id(),
        path: "docs/readme.md".to_string(),
        kind: DfsObjectKind::File,
        tombstone_provenance: None,
        owner_node_id: node("node_owner"),
        current_locations: vec![node("node_owner")],
        content_hash: Some("blake3:old".to_string()),
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

fn object_version_record(
    object: &DfsObjectMetadata,
    version: DfsVersion,
    created_at: OffsetDateTime,
) -> DfsObjectVersionRecord {
    DfsObjectVersionRecord {
        history_order: 0,
        retained: None,
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version,
        content_hash: object.content_hash.clone(),
        size_bytes: object.content_size_bytes,
        locations: object.current_locations.clone(),
        created_by_event_id: object.created_by_event_id.clone(),
        created_at,
        visible: true,
    }
}

fn replica_receipt_record(
    object: &DfsObjectMetadata,
    version: DfsVersion,
    received_at: OffsetDateTime,
) -> DfsReplicaReceiptRecord {
    DfsReplicaReceiptRecord {
        receipt: DfsWriteReceipt {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            committed_version: version,
            owner_node_id: object.owner_node_id.clone(),
            event_id: EventId::generate(),
        },
        replica_node_id: object.owner_node_id.clone(),
        content_hash: object.content_hash.clone(),
        received_at,
    }
}

fn namespace_job(index: usize, root_path: &str) -> DfsNamespaceJobRecord {
    let timestamp = OffsetDateTime::from_unix_timestamp(index as i64).expect("valid timestamp");
    DfsNamespaceJobRecord {
        job_id: format!("job_namespace_page_{index:02}"),
        file_system_id: file_system_id(),
        origin: None,
        kind: DfsNamespaceJobKind::ClearDirectoryRecursive,
        status: DfsNamespaceJobStatus::Running,
        root_path: root_path.to_string(),
        target_path: None,
        pending_frames: Vec::new(),
        processed_object_count: 0,
        created_at: timestamp,
        updated_at: timestamp,
        completed_at: None,
        failure_reason: None,
    }
}

fn namespace_request(index: usize, status: DfsNamespaceRequestStatus) -> DfsNamespaceRequestRecord {
    let timestamp = OffsetDateTime::from_unix_timestamp(index as i64).expect("valid timestamp");
    let completed = status == DfsNamespaceRequestStatus::Completed;
    DfsNamespaceRequestRecord {
        file_system_id: file_system_id(),
        requester_node_id: node(&format!("node_namespace_request_{index:02}")),
        request_id: format!("namespace-request-{index:02}"),
        operation_id: format!("namespace-operation-{index:02}"),
        authority_shard_id: DfsMetadataShardId::new("root"),
        operation: DfsNamespaceOperation::SetMode {
            path: format!("/requests/{index}"),
            mode: 0o644,
        },
        command_digest: format!("blake3:namespace-request-{index:02}"),
        status,
        result: completed.then_some(DfsNamespaceResult::Applied),
        posix_coordination_result: None,
        recovery_attempts: 0,
        last_recovery_error: None,
        created_at: timestamp,
        updated_at: timestamp,
    }
}

fn namespace_prepare(
    operation_id: &str,
    shard_id: &str,
    current: &DfsObjectMetadata,
    committed: DfsObjectMetadata,
) -> DfsNamespacePrepareRecord {
    let mut prepare = DfsNamespacePrepareRecord {
        operation_id: operation_id.to_string(),
        file_system_id: current.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new(shard_id),
        owner_epoch: 7,
        operation_digest: String::new(),
        expected_object_versions: vec![DfsNamespaceObjectVersionExpectation {
            object_id: current.object_id.clone(),
            expected_version: Some(current.version.clone()),
        }],
        expected_path_occupants: vec![DfsNamespacePathOccupantExpectation {
            path: current.path.clone(),
            expected_object_id: Some(current.object_id.clone()),
        }],
        content_receipts: Vec::new(),
        transaction: DfsNamespaceTransaction {
            file_version_pins: None,
            transaction_id: format!("transaction:{operation_id}"),
            file_system_id: current.file_system_id.clone(),
            objects: vec![committed],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        },
        completed_request: None,
        completed_request_completion: None,
        status: DfsNamespacePrepareStatus::Prepared,
    };
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("canonical namespace prepare digest");
    prepare
}

fn shard_config(shard_id: &str, assignment: DfsMetadataShardAssignment) -> DfsMetadataShardConfig {
    DfsMetadataShardConfig {
        file_system_id: file_system_id(),
        shard_id: shard_id.to_string(),
        primary_node_id: node("node_owner"),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    }
}

fn root_shard_config_at_epoch(shard_id: &str, epoch: u64) -> DfsMetadataShardConfig {
    let mut config = shard_config(shard_id, DfsMetadataShardAssignment::Root);
    config.epoch = epoch;
    config
}

fn assert_retained_sharded_backend_rebinds_prepare_codec(
    retiring_codec: DfsMetadataNamespacePrepareCodec,
    active_codec: DfsMetadataNamespacePrepareCodec,
    expect_packed: bool,
) {
    let label = if expect_packed {
        "legacy-to-packed"
    } else {
        "packed-to-legacy"
    };
    let root = temp_root(&format!("membership-handoff-codec-{label}"));
    let shard_id = DfsMetadataShardId::new("root");
    let local_node_id = node("node_owner");
    let mut retiring = root_shard_config_at_epoch(&shard_id.0, 7);
    retiring.namespace_prepare_codec = retiring_codec;
    retiring.replica_node_ids = vec![node("old-replica")];
    let mut active = retiring.clone();
    active.epoch = 8;
    active.namespace_prepare_codec = active_codec;
    active.replica_node_ids = vec![node("new-replica")];

    let mut backend =
        LocalShardedMetadataBackend::new(&root, vec![retiring.clone()]).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    let current = object(version(&format!("version_{label}_1")));
    backend.put_object(&current).expect("current object");
    let mut rebind_current = current.clone();
    rebind_current.object_id =
        DfsObjectId::from_legacy_named(format!("obj_{label}_rebind")).expect("object id");
    rebind_current.path = format!("/services/{label}/rebind");
    rebind_current.version = version(&format!("version_{label}_rebind_1"));
    backend
        .put_object(&rebind_current)
        .expect("rebind current object");
    let mut rebind_committed = rebind_current.clone();
    rebind_committed.version = version(&format!("version_{label}_rebind_2"));
    rebind_committed.mode = 0o600;
    let retiring_prepare = namespace_prepare(
        &format!("prepare-{label}-rebind"),
        &shard_id.0,
        &rebind_current,
        rebind_committed,
    );
    backend
        .prepare_namespace_transaction(&retiring_prepare)
        .expect("retiring-generation hidden prepare");

    let handoff_id = format!("handoff-codec-{label}");
    let freeze = backend
        .freeze_metadata_shard_for_membership_handoff(&shard_id, &handoff_id, retiring.epoch)
        .expect("freeze retiring codec generation")
        .expect("configured local shard");
    let proof = crate::DfsMetadataShardMembershipHandoffProof {
        handoff_id: handoff_id.clone(),
        retiring_shard: retiring,
        desired_shard: active.clone(),
        journal_sequence: freeze.journal_sequence,
        state_fingerprint: freeze.state_fingerprint.clone(),
        acknowledged_node_ids: [local_node_id.clone()].into_iter().collect(),
    };
    assert!(
        crate::service_sync::rebind_completed_metadata_shard_membership_handoff(
            &mut backend,
            &local_node_id,
            &active,
            None,
            Some(&proof),
        )
        .expect("proof-bound retained-backend rebind")
    );

    let status = backend
        .metadata_shard_statuses()
        .expect("shard status")
        .into_iter()
        .find(|status| status.shard_id == shard_id)
        .expect("rebound shard status");
    assert_eq!(status.file_system_id, Some(active.file_system_id.clone()));
    assert_eq!(status.primary_node_id, Some(active.primary_node_id.clone()));
    assert_eq!(status.replica_node_ids, active.replica_node_ids);
    assert_eq!(status.epoch, Some(active.epoch));

    let assert_active_codec = |entry: DfsMetadataJournalEntry, context: &str| {
        let DfsMetadataMutation::PutNamespacePrepare {
            prepare: legacy,
            packed_prepare_v1: packed,
            ..
        } = entry.mutation
        else {
            panic!("{context} must journal a namespace prepare");
        };
        assert_eq!(
            (legacy.is_some(), packed.is_some()),
            (!expect_packed, expect_packed),
            "{context} must use the active generation's prepare codec"
        );
    };

    let mut rebound_prepare = retiring_prepare.clone();
    rebound_prepare.owner_epoch = active.epoch;
    rebound_prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&rebound_prepare)
            .expect("rebound prepare digest");
    let rebound_entry = backend
        .rebind_prepared_namespace_transaction(
            &retiring_prepare.operation_id,
            &retiring_prepare.operation_digest,
            retiring_prepare.owner_epoch,
            &rebound_prepare,
        )
        .expect("pre-freeze prepare rebind uses active generation");
    assert_active_codec(rebound_entry, "pre-freeze prepare rebind");

    let mut committed = current.clone();
    committed.version = version(&format!("version_{label}_2"));
    committed.mode = 0o600;
    let mut prepare = namespace_prepare(
        &format!("prepare-{label}"),
        &shard_id.0,
        &current,
        committed,
    );
    prepare.owner_epoch = active.epoch;
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("active-generation prepare digest");
    let entry = backend
        .prepare_namespace_transaction(&prepare)
        .expect("retained backend uses active prepare generation");
    assert_active_codec(entry, "fresh active prepare");

    cleanup(&root);
}

fn table_path_record(
    backend: &LocalSobjMetadataBackend,
    file_system_id: &DfsFileSystemId,
    path: &str,
) -> DfsPathRecord {
    backend
        .tables
        .paths
        .get(&(file_system_id.clone(), normalize_path(path)))
        .cloned()
        .expect("path table record")
}

fn table_path_exists(
    backend: &LocalSobjMetadataBackend,
    file_system_id: &DfsFileSystemId,
    path: &str,
) -> bool {
    backend
        .tables
        .paths
        .contains_key(&(file_system_id.clone(), normalize_path(path)))
}

fn table_dentry_exists(backend: &LocalSobjMetadataBackend, path_record: &DfsPathRecord) -> bool {
    backend
        .tables
        .dentries
        .contains_key(&dentry_key(path_record))
}

fn table_dirfrag(
    backend: &LocalSobjMetadataBackend,
    dir_inode_id: &DfsInodeId,
    dirfrag_id: &DfsDirFragmentId,
) -> DfsDirFragmentRecord {
    backend
        .tables
        .dirfrags
        .get(&(dir_inode_id.clone(), dirfrag_id.clone()))
        .cloned()
        .expect("dirfrag table record")
}

fn table_file_system_object_exists(
    backend: &LocalSobjMetadataBackend,
    file_system_id: &DfsFileSystemId,
    object_id: &DfsObjectId,
) -> bool {
    backend
        .tables
        .file_system_objects
        .contains_key(&(file_system_id.clone(), object_id.clone()))
}

fn table_hardlink_exists(
    backend: &LocalSobjMetadataBackend,
    target_object_id: &DfsObjectId,
    hardlink_object_id: &DfsObjectId,
) -> bool {
    backend
        .tables
        .hardlinks
        .contains_key(&(target_object_id.clone(), hardlink_object_id.clone()))
}

fn force_table_replay_from_journal(
    backend: &LocalSobjMetadataBackend,
    root: &Path,
    last_materialized_sequence: u64,
) {
    remove_file_if_exists(&metadata_table_path(root)).expect("remove metadata table log");
    remove_file_if_exists(&metadata_table_snapshot_path(root))
        .expect("remove metadata table snapshot");
    write_sobj_atomic(
        &backend.local_materialization_cursor_path(),
        &DfsMetadataReplayCursor {
            shard_id: backend.shard_id.clone(),
            last_sequence: last_materialized_sequence,
        },
    )
    .expect("rewind materialization cursor");
}

fn install_empty_replay_checkpoint(
    backend: &mut LocalSobjMetadataBackend,
    source_shard_id: &DfsMetadataShardId,
    journal_sequence: u64,
) {
    assert!(
        !backend.tables.has_materialized_state(),
        "the historical replay checkpoint helper requires an empty replica"
    );
    let mut snapshot = DfsMetadataTableSnapshot {
        format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
        materialized_through_sequence: Some(journal_sequence),
        tables: Some(Box::new(metadata_tables_snapshot(&backend.tables))),
        ops: Vec::new(),
        semantic_state_fingerprint: None,
        prefix_proof: Some(
            backend
                .metadata_prefix_checkpoint_for_shard(source_shard_id, journal_sequence)
                .expect("historical replay prefix checkpoint"),
        ),
    };
    snapshot.semantic_state_fingerprint = Some(
        dfs_metadata_table_snapshot_state_fingerprint(&snapshot)
            .expect("historical replay semantic checkpoint"),
    );
    backend
        .replace_with_replicated_snapshot(source_shard_id.clone(), journal_sequence, snapshot)
        .expect("install historical replay checkpoint");
}

fn metadata_table_batch_count(root: &Path) -> usize {
    metadata_table_batches(root).len()
}

fn metadata_table_batches(root: &Path) -> Vec<DfsMetadataTableBatch> {
    use std::io::Read as _;

    let path = metadata_table_path(root);
    let mut file = match fs::File::open(&path) {
        Ok(file) => file,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Vec::new(),
        Err(source) => panic!("open metadata table log {}: {source}", path.display()),
    };
    let mut batches = Vec::new();
    loop {
        let mut length_bytes = [0_u8; 4];
        match file.read_exact(&mut length_bytes) {
            Ok(()) => {}
            Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(source) => panic!("read metadata table length {}: {source}", path.display()),
        }
        let length = u32::from_le_bytes(length_bytes) as usize;
        let mut bytes = vec![0_u8; length];
        file.read_exact(&mut bytes).unwrap_or_else(|source| {
            panic!("read metadata table batch {}: {source}", path.display())
        });
        batches.push(
            sinomo_object_serde::from_bytes(&bytes).unwrap_or_else(|source| {
                panic!("decode metadata table batch {}: {source}", path.display())
            }),
        );
    }
    batches
}

fn block_shard_journal_writes(root: &Path, shard_id: &str) {
    let segments = root
        .join(key_segment(&shard_id))
        .join("journal")
        .join("segments");
    fs::remove_dir_all(&segments).expect("remove journal segments directory");
    fs::write(&segments, b"not a directory").expect("block journal segment writes");
}

fn temp_root(name: &str) -> sinomo_testdir::TestDir {
    sinomo_testdir::TestDir::new(&format!("sinomo-dfs-metadata-{name}"))
}

fn cleanup(root: &Path) {
    metadata_table_apply::wait_for_metadata_table_checkpoint_workers_for_test(root);
    if root.exists() {
        fs::remove_dir_all(root).expect("cleanup temp dir");
    }
}

// Shared fixtures remain in this parent so each focused suite can use
// private transport and storage helpers without widening production APIs.
mod authority;
mod content;
mod cross_shard;
mod file_history;
mod locks;
mod maintenance;
mod namespace;
mod namespace_prepare;
mod namespace_requests;
mod pins;
mod protocol;
mod reads;
mod recovery;
mod replication;
mod writes;
