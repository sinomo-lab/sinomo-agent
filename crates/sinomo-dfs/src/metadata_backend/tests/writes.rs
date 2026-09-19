//! Writes regression tests.

use super::*;

#[test]
fn local_backend_rebinds_exact_prepared_write_intent_with_record_fence() {
    let root = temp_root("local_backend_rebinds_prepared_write_intent");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let current = object(version("version_intent_rebind_1"));
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("object");
    let created_at = OffsetDateTime::UNIX_EPOCH;
    let intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "intent-rebind".to_string(),
        file_system_id: current.file_system_id.clone(),
        object_id: current.object_id.clone(),
        base_version: Some(current.version.clone()),
        target_version: version("version_intent_rebind_2"),
        coordinator_node_id: node("node_requester"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 7,
        desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
        policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: Some("blake3:prepared".to_string()),
        object_metadata: Some(current.clone()),
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at,
        updated_at: Some(created_at),
    };
    backend.put_intent(&intent).expect("prepared intent");
    let mut rebound = intent.clone();
    rebound.owner_epoch += 1;
    rebound.updated_at = Some(created_at + time::Duration::seconds(1));

    let mut changed_payload = rebound.clone();
    changed_payload.target_version = version("version_intent_different_target");
    let payload_error = backend
        .rebind_prepared_write_intent(Some(&current), &intent, &changed_payload)
        .expect_err("rebind must preserve write payload");
    assert!(payload_error.to_string().contains("payload or status"));

    backend
        .rebind_prepared_write_intent(Some(&current), &intent, &rebound)
        .expect("rebind write intent");
    backend
        .rebind_prepared_write_intent(Some(&current), &intent, &rebound)
        .expect("exact lost-response write rebind retry is idempotent");
    let mut concurrent = rebound.clone();
    concurrent.owner_epoch += 1;
    concurrent.updated_at = Some(created_at + time::Duration::seconds(2));
    let concurrent_error = backend
        .rebind_prepared_write_intent(Some(&current), &intent, &concurrent)
        .expect_err("consumed old record fence must reject concurrent rebind");
    assert!(
        concurrent_error
            .to_string()
            .contains("record fence mismatch")
    );
    assert_eq!(
        backend
            .get_intent(&intent.object_id, &intent.intent_id)
            .expect("intent"),
        Some(rebound)
    );
    cleanup(&root);
}

#[test]
fn sharded_backend_updates_existing_intent_without_object_shard_index() {
    let root = temp_root("sharded_backend_updates_existing_orphan_intent");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let file_system = file_system();
    let mut object = object(version("version_1"));
    object.path = "/services/orphan-intent.txt".to_string();
    let mut intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "dfs_intent_orphan_sharded".to_string(),
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        base_version: Some(object.version.clone()),
        target_version: version("version_2"),
        coordinator_node_id: node("node_owner"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 0,
        desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
        policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: Some("blake3:orphan-intent".to_string()),
        object_metadata: Some(object.clone()),
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::now_utc(),
        updated_at: Some(OffsetDateTime::now_utc()),
    };

    backend.put_file_system(&file_system).expect("filesystem");
    backend
        .put_intent_for_object(Some(&object), &intent)
        .expect("intent");
    std::fs::remove_file(backend.object_shard_index_path(&object.object_id))
        .expect("remove shard index");

    intent.status = DfsWriteIntentStatus::Aborted;
    intent.updated_at = Some(OffsetDateTime::now_utc());
    backend
        .put_intent(&intent)
        .expect("update existing intent without object shard index");

    assert_eq!(
        backend
            .get_intent(&object.object_id, &intent.intent_id)
            .expect("intent")
            .expect("intent")
            .status,
        DfsWriteIntentStatus::Aborted
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_create_commit_creates_missing_object_shard_index() {
    let root = temp_root("sharded_backend_create_commit_new_index");
    let file_system = file_system();
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let mut child = object(version("version_1"));
    child.path = "/services/content-commit.bin".to_string();
    child.content_hash = Some("blake3:content-commit".to_string());
    child.content_size_bytes = Some(4096);
    let version_record = DfsObjectVersionRecord {
        history_order: 0,
        retained: None,
        file_system_id: child.file_system_id.clone(),
        object_id: child.object_id.clone(),
        version: child.version.clone(),
        content_hash: child.content_hash.clone(),
        size_bytes: child.content_size_bytes,
        locations: child.current_locations.clone(),
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    let mut parent = object(version("version_1"));
    parent.object_id = DfsObjectId::from_legacy_named("obj_services_parent").expect("parent");
    parent.path = "/services".to_string();
    parent.kind = DfsObjectKind::Directory;
    parent.content_hash = None;
    parent.content_size_bytes = None;
    parent.mode = 0o755;
    let parent_version = DfsObjectVersionRecord {
        history_order: 0,
        retained: None,
        file_system_id: parent.file_system_id.clone(),
        object_id: parent.object_id.clone(),
        version: parent.version.clone(),
        content_hash: None,
        size_bytes: None,
        locations: parent.current_locations.clone(),
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    let mut touched_parent = parent.clone();
    touched_parent.version = crate::namespace::next_version(&parent.version);
    touched_parent.modified_at_unix_nanos = 1;
    touched_parent.changed_at_unix_nanos = 1;
    touched_parent.last_modified_by_event_id =
        Some(EventId::from_legacy_named("evt_content_create_parent_touch").expect("parent event"));
    let touched_parent_version = DfsObjectVersionRecord {
        history_order: 0,
        retained: None,
        file_system_id: touched_parent.file_system_id.clone(),
        object_id: touched_parent.object_id.clone(),
        version: touched_parent.version.clone(),
        content_hash: None,
        size_bytes: None,
        locations: touched_parent.current_locations.clone(),
        created_by_event_id: touched_parent.last_modified_by_event_id.clone(),
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    let committed_intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "intent-content-commit-new-index".to_string(),
        file_system_id: child.file_system_id.clone(),
        object_id: child.object_id.clone(),
        base_version: None,
        target_version: child.version.clone(),
        coordinator_node_id: node("node_owner"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 1,
        desired_replica_node_ids: child.current_locations.clone(),
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        status: DfsWriteIntentStatus::Committed,
        content_hash: child.content_hash.clone(),
        object_metadata: Some(child.clone()),
        touch_parent_on_create: true,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: Some(OffsetDateTime::UNIX_EPOCH),
    };
    let mut pending_intent = committed_intent.clone();
    pending_intent.status = DfsWriteIntentStatus::ReceiptsPending;
    let receipt = DfsReplicaReceiptRecord {
        receipt: DfsWriteReceipt {
            file_system_id: child.file_system_id.clone(),
            object_id: child.object_id.clone(),
            committed_version: child.version.clone(),
            owner_node_id: node("node_owner"),
            event_id: EventId::from_legacy_named("evt_content_commit_new_index").expect("event"),
        },
        replica_node_id: node("node_owner"),
        content_hash: child.content_hash.clone(),
        received_at: OffsetDateTime::UNIX_EPOCH,
    };
    backend.put_file_system(&file_system).expect("filesystem");
    backend
        .put_object_and_version(&parent, &parent_version)
        .expect("parent object and version");
    let transaction = DfsNamespaceTransaction {
        file_version_pins: None,
        transaction_id: "transaction-content-create-new-index".to_string(),
        file_system_id: child.file_system_id.clone(),
        objects: vec![child.clone(), touched_parent.clone()],
        versions: vec![version_record, touched_parent_version],
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };

    let entries = backend
        .commit_create_intent_receipts_and_namespace(
            &pending_intent,
            std::slice::from_ref(&receipt),
            &transaction,
            &committed_intent,
        )
        .expect("create commit batch");
    assert!(matches!(
        entries.first().map(|entry| &entry.mutation),
        Some(DfsMetadataMutation::PutIntent {
            status: DfsWriteIntentStatus::ReceiptsPending,
            ..
        })
    ));
    assert!(matches!(
        entries.get(1).map(|entry| &entry.mutation),
        Some(DfsMetadataMutation::PutReceipt { .. })
    ));
    assert!(matches!(
        entries.get(2).map(|entry| &entry.mutation),
        Some(DfsMetadataMutation::ApplyNamespaceTransaction { .. })
    ));
    assert!(matches!(
        entries.get(3).map(|entry| &entry.mutation),
        Some(DfsMetadataMutation::PutIntent {
            status: DfsWriteIntentStatus::Committed,
            ..
        })
    ));

    assert_eq!(
        backend
            .read_object_shard_index(&child.object_id)
            .expect("object shard index")
            .as_ref()
            .map(|index| (&index.path, &index.shard_id)),
        Some((
            &normalize_path(&child.path),
            &DfsMetadataShardId::new("services")
        )),
        "create commits should publish the new object's lookup index"
    );
    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, &child.path)
            .expect("path lookup"),
        Some(child.clone()),
        "the create batch must leave path lookup correct"
    );
    assert_eq!(
        backend
            .get_object(&touched_parent.object_id)
            .expect("touched parent"),
        Some(touched_parent.clone())
    );
    let duplicate_error = backend
        .commit_create_intent_receipts_and_namespace(
            &pending_intent,
            std::slice::from_ref(&receipt),
            &transaction,
            &committed_intent,
        )
        .expect_err("a create batch must compare-and-publish the child exactly once");
    assert!(
        matches!(
            duplicate_error,
            MetadataBackendError::AlreadyExists { ref path } if path == &child.path
        ),
        "duplicate child publication should return typed path occupancy: {duplicate_error}"
    );

    drop(backend);
    let reopened = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("reopen sharded backend");
    assert_eq!(
        reopened
            .get_object_by_path(&file_system.file_system_id, &child.path)
            .expect("reopened child path"),
        Some(child),
        "the committed child must survive journal/table reopen"
    );
    assert_eq!(
        reopened
            .get_object(&touched_parent.object_id)
            .expect("reopened touched parent"),
        Some(touched_parent),
        "the parent touch must survive the same create transaction"
    );

    cleanup(&root);
}

#[cfg(unix)]
#[test]
fn sharded_backend_content_commit_skips_matching_object_shard_index_rewrite() {
    use std::os::unix::fs::PermissionsExt;

    let root = temp_root("sharded_backend_content_commit_skip_index_rewrite");
    let file_system = file_system();
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let mut object = object(version("version_1"));
    object.path = "/services/existing-content.bin".to_string();
    object.content_hash = Some("blake3:old-content".to_string());
    object.content_size_bytes = Some(128);
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("initial object");

    let mut committed = object.clone();
    committed.version = version("version_2");
    committed.content_hash = Some("blake3:new-content".to_string());
    committed.content_size_bytes = Some(4096);
    let version_record = DfsObjectVersionRecord {
        history_order: 0,
        retained: None,
        file_system_id: committed.file_system_id.clone(),
        object_id: committed.object_id.clone(),
        version: committed.version.clone(),
        content_hash: committed.content_hash.clone(),
        size_bytes: committed.content_size_bytes,
        locations: committed.current_locations.clone(),
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    let intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "intent-content-commit-existing-index".to_string(),
        file_system_id: committed.file_system_id.clone(),
        object_id: committed.object_id.clone(),
        base_version: Some(object.version.clone()),
        target_version: committed.version.clone(),
        coordinator_node_id: node("node_owner"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 1,
        desired_replica_node_ids: committed.current_locations.clone(),
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        status: DfsWriteIntentStatus::Committed,
        content_hash: committed.content_hash.clone(),
        object_metadata: Some(committed.clone()),
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: Some(OffsetDateTime::UNIX_EPOCH),
    };
    let receipt = DfsReplicaReceiptRecord {
        receipt: DfsWriteReceipt {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            committed_version: committed.version.clone(),
            owner_node_id: node("node_owner"),
            event_id: EventId::from_legacy_named("evt_content_commit_existing_index")
                .expect("event"),
        },
        replica_node_id: node("node_owner"),
        content_hash: committed.content_hash.clone(),
        received_at: OffsetDateTime::UNIX_EPOCH,
    };
    let mut insufficient_receipts_intent = intent.clone();
    insufficient_receipts_intent
        .desired_replica_node_ids
        .push(node("node_replica"));
    insufficient_receipts_intent.policy =
        DfsReplicationPolicy::new(2, 2).expect("two-replica policy");
    let insufficient_error = backend
        .put_object_version_intent_and_receipts(
            &committed,
            &version_record,
            &insufficient_receipts_intent,
            std::slice::from_ref(&receipt),
        )
        .expect_err("visible commit must require W distinct durable receipts");
    assert!(
        matches!(insufficient_error, MetadataBackendError::Serde(_)),
        "receipt-threshold rejection should be explicit: {insufficient_error:?}"
    );
    assert_eq!(
        backend
            .get_object(&object.object_id)
            .expect("object after insufficient receipt rejection"),
        Some(object.clone()),
        "a rejected commit must retain the prior visible version"
    );

    let mut stale_intent = intent.clone();
    stale_intent.base_version = Some(version("version_0"));
    let stale_error = backend
        .put_object_version_intent_and_receipts(
            &committed,
            &version_record,
            &stale_intent,
            std::slice::from_ref(&receipt),
        )
        .expect_err("visible commit must compare-and-swap its base version");
    assert!(
        matches!(stale_error, MetadataBackendError::Conflict { .. }),
        "stale-base rejection should report a conflict: {stale_error:?}"
    );
    assert_eq!(
        backend
            .get_object(&object.object_id)
            .expect("object after stale commit rejection"),
        Some(object.clone()),
        "a stale commit must retain the prior visible version"
    );

    let index_parent = backend
        .object_shard_index_path(&committed.object_id)
        .parent()
        .expect("index parent")
        .to_path_buf();
    let original_permissions = fs::metadata(&index_parent)
        .expect("index parent metadata")
        .permissions();
    let mut read_only_permissions = original_permissions.clone();
    read_only_permissions.set_mode(0o500);
    fs::set_permissions(&index_parent, read_only_permissions)
        .expect("make shard index directory read-only");

    let result = backend.put_object_version_intent_and_receipts(
        &committed,
        &version_record,
        &intent,
        std::slice::from_ref(&receipt),
    );

    fs::set_permissions(&index_parent, original_permissions)
        .expect("restore shard index directory permissions");
    let entries = result.expect("content commit should not rewrite an already-correct shard index");
    assert!(matches!(
        entries.first().map(|entry| &entry.mutation),
        Some(DfsMetadataMutation::PutIntent {
            status: DfsWriteIntentStatus::ReceiptsPending,
            ..
        })
    ));
    assert!(matches!(
        entries.get(1).map(|entry| &entry.mutation),
        Some(DfsMetadataMutation::PutReceipt { .. })
    ));
    assert!(matches!(
        entries.get(2).map(|entry| &entry.mutation),
        Some(DfsMetadataMutation::CommitVisibleVersion { .. })
    ));
    assert!(matches!(
        entries.get(3).map(|entry| &entry.mutation),
        Some(DfsMetadataMutation::PutIntent {
            status: DfsWriteIntentStatus::Committed,
            ..
        })
    ));
    assert_eq!(
        backend
            .get_object(&committed.object_id)
            .expect("object by id"),
        Some(committed),
        "content commit should still advance the object metadata"
    );

    cleanup(&root);
}

#[test]
fn local_backend_open_does_not_rewrite_existing_metadata_table() {
    let root = temp_root("local_backend_open_preserves_metadata_table");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend
        .put_object(&object(version("version_1")))
        .expect("object");
    let table_path = metadata_table_path(&root);
    let before_len = fs::metadata(&table_path).expect("metadata table").len();

    LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopen backend");

    let after_len = fs::metadata(&table_path)
        .expect("metadata table after reopen")
        .len();
    assert_eq!(
        before_len, after_len,
        "opening a healthy backend must not rewrite the materialized metadata table"
    );
    cleanup(&root);
}

#[test]
fn local_backend_serializes_concurrent_metadata_writers() {
    let root = temp_root("local_backend_serializes_concurrent_metadata_writers");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    drop(backend);

    let writer_count = 8usize;
    let writes_per_writer = 16usize;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(writer_count));
    let mut handles = Vec::new();
    for writer_index in 0..writer_count {
        let root = root.to_path_buf();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let mut backend =
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                    .expect("backend");
            barrier.wait();
            for object_index in 0..writes_per_writer {
                let mut object = object(version("version_1"));
                object.object_id = DfsObjectId::from_legacy_named(format!(
                    "obj_concurrent_{writer_index}_{object_index}"
                ))
                .expect("object");
                object.path = format!("/docs/concurrent-{writer_index}-{object_index}.md");
                backend.put_object(&object).expect("object");
            }
        }));
    }
    for handle in handles {
        handle.join().expect("writer thread");
    }

    let backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopen");
    let journal = backend.journal_since(0).expect("journal");
    assert_eq!(journal.len(), 1 + writer_count * writes_per_writer);
    let sequences = journal
        .iter()
        .map(|entry| entry.sequence)
        .collect::<Vec<_>>();
    assert_eq!(
        sequences,
        (1..=journal.len() as u64).collect::<Vec<_>>(),
        "concurrent metadata writers must allocate contiguous journal sequences"
    );
    assert_eq!(
        backend
            .objects_for_file_system(&file_system_id())
            .expect("objects")
            .len(),
        writer_count * writes_per_writer,
        "concurrent materialization must not lose object indexes"
    );

    cleanup(&root);
}

#[test]
fn local_backend_stale_open_writer_refreshes_next_sequence_from_table_log() {
    let root = temp_root("local_backend_stale_open_writer_refreshes_next_sequence");
    let mut stale_writer =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("stale writer");
    stale_writer
        .put_file_system(&file_system())
        .expect("filesystem");

    let mut fresh_writer =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("fresh writer");
    let mut fresh_object = object(version("version_1"));
    fresh_object.object_id =
        DfsObjectId::from_legacy_named("obj_fresh_writer_sequence").expect("object");
    fresh_object.path = "/docs/fresh-writer-sequence.md".to_string();
    fresh_writer
        .put_object(&fresh_object)
        .expect("fresh writer object");

    let mut stale_object = object(version("version_1"));
    stale_object.object_id =
        DfsObjectId::from_legacy_named("obj_stale_writer_sequence").expect("object");
    stale_object.path = "/docs/stale-writer-sequence.md".to_string();
    let stale_entry = stale_writer
        .put_object(&stale_object)
        .expect("stale writer object");
    assert_eq!(
        stale_entry.sequence, 3,
        "a stale open handle must advance from materialized table state before writing"
    );

    let sequences = stale_writer
        .journal_since(0)
        .expect("journal")
        .into_iter()
        .map(|entry| entry.sequence)
        .collect::<Vec<_>>();
    assert_eq!(sequences, vec![1, 2, 3]);
    assert_eq!(
        stale_writer
            .get_object(&fresh_object.object_id)
            .expect("fresh object"),
        Some(fresh_object)
    );
    assert_eq!(
        stale_writer
            .get_object(&stale_object.object_id)
            .expect("stale object"),
        Some(stale_object)
    );

    cleanup(&root);
}

#[test]
fn local_backend_content_update_does_not_rewrite_stable_namespace_indexes() {
    let root = temp_root("local_backend_content_update_skips_stable_indexes");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let mut readme = object(version("version_1"));
    readme.path = "/docs/readme.md".to_string();
    backend.put_object(&readme).expect("object");
    let path_record = table_path_record(&backend, &readme.file_system_id, &readme.path);
    let filesystem_object_record = backend
        .tables
        .file_system_objects
        .get(&(readme.file_system_id.clone(), readme.object_id.clone()))
        .cloned()
        .expect("filesystem object index");
    let dentry_record = backend
        .tables
        .dentries
        .get(&dentry_key(&path_record))
        .cloned()
        .expect("dentry index");
    let dirfrag = table_dirfrag(
        &backend,
        &path_record.parent_inode_id,
        &path_record.dirfrag_id,
    );
    let object_before = backend
        .tables
        .objects
        .get(&readme.object_id)
        .cloned()
        .expect("object record");
    let inode_before = backend
        .tables
        .inodes
        .get(&path_record.inode_id)
        .cloned()
        .expect("inode record");

    readme.content_hash = Some("blake3:new-content".to_string());
    readme.version = version("version_2");
    backend.put_object(&readme).expect("content update");

    assert_eq!(
        backend
            .tables
            .file_system_objects
            .get(&(readme.file_system_id.clone(), readme.object_id.clone()))
            .cloned(),
        Some(filesystem_object_record),
        "content-only updates should keep the filesystem-object namespace index stable"
    );
    assert_eq!(
        table_path_record(&backend, &readme.file_system_id, &readme.path),
        path_record,
        "content-only updates should keep the path namespace index stable"
    );
    assert_eq!(
        backend
            .tables
            .dentries
            .get(&dentry_key(&path_record))
            .cloned(),
        Some(dentry_record),
        "content-only updates should keep the dentry namespace index stable"
    );
    assert_eq!(
        table_dirfrag(
            &backend,
            &path_record.parent_inode_id,
            &path_record.dirfrag_id
        ),
        dirfrag,
        "content-only updates should not change directory fragment accounting"
    );
    assert_ne!(
        backend.tables.objects.get(&readme.object_id).cloned(),
        Some(object_before),
        "the primary object record should still update when content metadata changes"
    );
    assert_ne!(
        backend.tables.inodes.get(&path_record.inode_id).cloned(),
        Some(inode_before),
        "the inode record should still update when visible object metadata changes"
    );

    cleanup(&root);
}

#[test]
fn local_backend_persists_intents_and_polls_subscription_batches() {
    let root = temp_root("local_backend_persists_intents_and_polls_subscription_batches");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "dfs_intent_1".to_string(),
        file_system_id: file_system_id(),
        object_id: object_id(),
        base_version: Some(version("version_1")),
        target_version: version("version_2"),
        coordinator_node_id: node("node_owner"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 0,
        desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
        policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: Some("blake3:new".to_string()),
        object_metadata: None,
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: Some(OffsetDateTime::UNIX_EPOCH),
    };

    let entry = backend.put_intent(&intent).expect("intent");
    assert_eq!(entry.sequence, 1);
    assert!(matches!(
        entry.mutation,
        DfsMetadataMutation::PutIntent {
            status: DfsWriteIntentStatus::ReceiptsPending,
            intent: Some(ref journal_intent),
            ..
        } if journal_intent == &intent
    ));
    assert_eq!(
        backend
            .get_intent(&intent.object_id, &intent.intent_id)
            .expect("get intent"),
        Some(intent)
    );

    let mut subscription = backend.subscribe(0);
    let first_batch = backend
        .poll_subscription(&mut subscription)
        .expect("first batch");
    assert_eq!(first_batch.entries.len(), 1);
    assert_eq!(first_batch.next_after_sequence, 1);
    assert_eq!(subscription.next_after_sequence, 1);

    let second_batch = backend
        .poll_subscription(&mut subscription)
        .expect("second batch");
    assert!(second_batch.entries.is_empty());
    assert_eq!(second_batch.next_after_sequence, 1);

    fs::write(
        root.join("journal").join("00000000000000000001.sobj"),
        b"not sobj",
    )
    .expect("corrupt already-consumed journal entry");
    let third_batch = backend
        .poll_subscription(&mut subscription)
        .expect("already-consumed corrupted journal entry is not reparsed");
    assert!(third_batch.entries.is_empty());
    assert_eq!(third_batch.next_after_sequence, 1);

    cleanup(&root);
}
