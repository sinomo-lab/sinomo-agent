//! Protocol regression tests.

use super::*;

#[test]
fn local_backend_rejects_request_owned_prepare_without_completion_attachment() {
    let root = temp_root("local_backend_request_prepare_requires_completion");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let file_system = file_system();
    let current = object(version("version_request_prepare_1"));
    let mut committed = current.clone();
    committed.version = version("version_request_prepare_2");
    let prepare = namespace_prepare(
        "namespace-request:mkdir:attempt-1",
        &shard_id.0,
        &current,
        committed,
    );
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&current).expect("current object");

    let error = backend
        .prepare_namespace_transaction(&prepare)
        .expect_err("request-owned prepare without completion must fail closed");

    assert!(
        error
            .to_string()
            .contains("request-owned but has no completed request attachment")
    );
    cleanup(&root);
}

#[test]
fn request_owned_prepare_fused_admission_rejects_different_request_digest() {
    let root = temp_root("local_backend_request_prepare_fused_digest_conflict");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let current = object(version("version_request_prepare_fused_digest_1"));
    let mut committed = current.clone();
    committed.version = version("version_request_prepare_fused_digest_2");
    committed.mode = 0o600;
    let mut completed_request = DfsNamespaceRequestRecord {
        file_system_id: current.file_system_id.clone(),
        requester_node_id: node("node_request_prepare_fused_digest_requester"),
        request_id: "request-prepare-fused-digest".to_string(),
        operation_id: "req-request-prepare-fused-digest".to_string(),
        authority_shard_id: shard_id.clone(),
        operation: DfsNamespaceOperation::SetMode {
            path: current.path.clone(),
            mode: committed.mode,
        },
        command_digest: "blake3:request-prepare-fused-digest".to_string(),
        status: DfsNamespaceRequestStatus::Completed,
        result: Some(DfsNamespaceResult::Applied),
        posix_coordination_result: None,
        recovery_attempts: 0,
        last_recovery_error: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::from_unix_timestamp(1).expect("completion timestamp"),
    };
    let mut prepare = namespace_prepare(
        &format!(
            "namespace-request:{}:transaction-request-prepare-fused-digest",
            completed_request.operation_id
        ),
        &shard_id.0,
        &current,
        committed.clone(),
    );
    prepare.completed_request = Some(completed_request.clone());
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("request-owned prepare digest");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("initial fused admission");

    completed_request.command_digest = "blake3:different-request-prepare-fused-digest".to_string();
    let mut conflicting_prepare = namespace_prepare(
        &format!(
            "namespace-request:{}:transaction-request-prepare-fused-digest-conflict",
            completed_request.operation_id
        ),
        &shard_id.0,
        &current,
        committed,
    );
    conflicting_prepare.completed_request = Some(completed_request.clone());
    conflicting_prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&conflicting_prepare)
            .expect("conflicting request-owned prepare digest");

    let error = backend
        .prepare_namespace_transaction(&conflicting_prepare)
        .expect_err("a fused admission must not replace a different request digest");

    assert!(
        error.to_string().contains("completed request conflicts"),
        "unexpected error: {error}"
    );
    assert!(
        backend
            .get_namespace_prepare(&conflicting_prepare.operation_id)
            .expect("conflicting prepare lookup")
            .is_none()
    );
    assert_eq!(
        backend
            .get_namespace_request(
                &completed_request.requester_node_id,
                &completed_request.request_id,
            )
            .expect("original pending request")
            .map(|request| request.command_digest),
        Some("blake3:request-prepare-fused-digest".to_string())
    );
    cleanup(&root);
}

#[test]
fn local_backend_location_merge_cannot_resurrect_superseded_object() {
    let root = temp_root("local_backend_location_merge_version_fence");
    let shard_id = DfsMetadataShardId::new("users");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let file_system = file_system();
    let mut live = object(version("version_1"));
    live.path = "/users/repair-race.txt".to_string();
    live.changed_at_unix_nanos = 1;
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&live).expect("live object");

    let mut tombstone = live.clone();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.version = version("version_2");
    tombstone.changed_at_unix_nanos = 2;
    backend.put_object(&tombstone).expect("move tombstone");

    assert_eq!(
        backend
            .add_object_location_if_current(
                &live.object_id,
                &live.version,
                live.content_hash.as_deref().expect("content hash"),
                &node("node_late_replica"),
            )
            .expect("conditional location merge"),
        None,
        "a delayed old-version repair must lose to the move tombstone"
    );
    assert_eq!(
        backend.get_object(&live.object_id).expect("current object"),
        Some(tombstone.clone())
    );

    drop(backend);
    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");
    assert_eq!(
        reopened
            .get_object(&live.object_id)
            .expect("reopened current object"),
        Some(tombstone),
        "recovery must preserve the superseding tombstone"
    );

    cleanup(&root);
}

#[test]
fn local_backend_batches_object_metadata_into_one_table_batch() {
    let root = temp_root("local_backend_batches_object_metadata_into_one_table_batch");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    backend.put_file_system(&file_system).expect("filesystem");

    let objects = (0..3)
        .map(|index| {
            let mut object = object(version(&format!("version_batch_{index}")));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_batch_{index}")).expect("object");
            object.path = format!("docs/file-{index}.md");
            object
        })
        .collect::<Vec<_>>();
    let entries = backend.put_objects(&objects).expect("object batch");

    assert_eq!(entries.len(), objects.len());
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    assert_eq!(
        backend.journal_since(1).expect("journal").len(),
        objects.len()
    );
    assert_eq!(
        metadata_table_batch_count(&root),
        2,
        "filesystem write should use one table batch and the object group should use one more"
    );
    for object in &objects {
        assert_eq!(
            backend.get_object(&object.object_id).expect("get object"),
            Some(object.clone())
        );
    }

    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopen");
    assert_eq!(
        reopened
            .count_objects_for_file_system(&file_system.file_system_id)
            .expect("count objects"),
        objects.len()
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_routes_object_and_version_to_selected_path_shard() {
    let root = temp_root("sharded_backend_routes_object_and_version");
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
    object.path = "/services/readme.md".to_string();
    let version_record = DfsObjectVersionRecord {
        content_durability: None,
        metadata_format: crate::DfsRevisionMetadataFormat::Legacy,
        retention: Default::default(),
        module_metadata: Default::default(),
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        size_bytes: object.content_size_bytes,
        locations: object.current_locations.clone(),
        created_by_event_id: object.created_by_event_id.clone(),
        created_at: OffsetDateTime::now_utc(),
        visible: true,
    };

    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("object");
    backend.put_version(&version_record).expect("version");

    assert_eq!(
        backend
            .get_object(&object.object_id)
            .expect("get object")
            .as_ref()
            .map(|object| object.path.as_str()),
        Some("/services/readme.md")
    );
    assert_eq!(
        backend
            .get_version(&object.object_id, &object.version)
            .expect("get version"),
        Some(version_record)
    );
    let services_batch = backend
        .journal_batch_for_shard_since_limited(
            &DfsMetadataShardId::new("services"),
            0,
            16,
            usize::MAX,
        )
        .expect("services batch")
        .expect("services changes");
    assert!(
        services_batch.entries.iter().any(|entry| matches!(
            entry.mutation,
            DfsMetadataMutation::PutObject { .. } | DfsMetadataMutation::PutVersion { .. }
        )),
        "path-selected shard should own object and version journal entries"
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_evicts_explicit_source_without_object_index() {
    let root = temp_root("sharded_backend_explicit_source_eviction");
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
            shard_config(
                "users",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/users".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let file_system = file_system();
    let mut source = object(version("version_1"));
    source.path = "/users/move-src.txt".to_string();
    source.changed_at_unix_nanos = 1;
    let mut moved = source.clone();
    moved.path = "/services/move-dst.txt".to_string();
    moved.version = version("version_2");
    moved.changed_at_unix_nanos = 2;
    backend.put_file_system(&file_system).expect("filesystem");
    backend
        .shard_backend_mut(&DfsMetadataShardId::new("users"))
        .expect("users shard")
        .put_object(&source)
        .expect("source object");
    backend
        .shard_backend_mut(&DfsMetadataShardId::new("services"))
        .expect("services shard")
        .put_object(&moved)
        .expect("destination object");
    crate::metadata_shard_index::remove_object_shard_index(&root, &source.object_id)
        .expect("remove derived index");

    backend
        .evict_object_record_for_foreign_shard_move(&moved, &source.path)
        .expect("source eviction");

    assert_eq!(
        backend
            .shard_backend(&DfsMetadataShardId::new("users"))
            .expect("users shard")
            .get_object(&source.object_id)
            .expect("source record")
            .map(|object| (object.kind, object.version)),
        Some((DfsObjectKind::Tombstone, moved.version.clone())),
        "eviction must derive the source shard from the old path, not a missing local index"
    );
    assert_eq!(
        backend
            .read_object_shard_index(&source.object_id)
            .expect("destination index")
            .map(|index| index.shard_id),
        Some(DfsMetadataShardId::new("services"))
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_never_promotes_source_eviction_after_path_routes_back_to_source() {
    let root = temp_root("sharded_backend_source_eviction_reroute");
    let initial_configs = vec![
        shard_config("root", DfsMetadataShardAssignment::Root),
        shard_config(
            "services",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
        ),
    ];
    let mut backend =
        LocalShardedMetadataBackend::new(&root, initial_configs).expect("sharded backend");
    let file_system = file_system();
    backend.put_file_system(&file_system).expect("filesystem");

    let mut live = object(version("version_source_eviction_reroute"));
    live.path = "/services/rerouted-live.txt".to_string();
    live.changed_at_unix_nanos = 10;
    backend
        .shard_backend_mut(&DfsMetadataShardId::new("services"))
        .expect("services shard")
        .put_object(&live)
        .expect("live target");

    let mut source_eviction = live.clone();
    source_eviction.kind = DfsObjectKind::Tombstone;
    source_eviction.changed_at_unix_nanos = 11;
    source_eviction.tombstone_provenance = Some(crate::DfsTombstoneProvenance::SourceEviction {
        source_shard_id: DfsMetadataShardId::new("root"),
        target_shard_id: DfsMetadataShardId::new("services"),
        pin_transfer_id: None,
    });
    backend
        .shard_backend_mut(&DfsMetadataShardId::new("root"))
        .expect("root shard")
        .put_object(&source_eviction)
        .expect("source eviction marker");
    crate::metadata_shard_index::write_object_shard_index(
        &root,
        &source_eviction,
        &DfsMetadataShardId::new("root"),
    )
    .expect("stale source index");
    drop(backend);

    // A later topology generation routes /services through root again.
    // The newer physical eviction marker must remain non-authoritative;
    // the live target is the only logical namespace record until the
    // migration lane moves it to the new route.
    let rerouted_configs = vec![
        shard_config("root", DfsMetadataShardAssignment::Root),
        shard_config(
            "services",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: "/retired-services".to_string(),
            },
        ),
    ];
    let backend = LocalShardedMetadataBackend::new(&root, rerouted_configs)
        .expect("rerouted sharded backend");
    assert_eq!(
        backend.get_object(&live.object_id).expect("object by id"),
        Some(live.clone()),
        "a source-eviction marker is never authoritative deletion evidence"
    );
    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, &live.path)
            .expect("object by path"),
        Some(live.clone()),
        "a newly path-selected eviction marker must not suppress the live fallback"
    );
    assert_eq!(
        backend
            .objects_for_file_system(&file_system.file_system_id)
            .expect("merged objects"),
        vec![live]
    );

    cleanup(&root);
}

#[test]
fn conditional_source_eviction_yields_to_concurrent_family_append() {
    let root = temp_root("conditional_source_eviction_family_fence");
    let source_shard_id = DfsMetadataShardId::new("root");
    let target_shard_id = DfsMetadataShardId::new("services");
    let mut source =
        LocalSobjMetadataBackend::new(source_shard_id.clone(), &root).expect("source backend");
    source.put_file_system(&file_system()).expect("filesystem");
    let expected_source = object(version("version_family_fence_source"));
    source.put_object(&expected_source).expect("source object");
    let fenced_sequence = source.journal_last_sequence();

    let mut concurrent = LocalSobjMetadataBackend::new(source_shard_id.clone(), &root)
        .expect("concurrent source writer");
    concurrent
        .put_version(&DfsObjectVersionRecord {
            content_durability: None,
            metadata_format: crate::DfsRevisionMetadataFormat::Legacy,
            retention: Default::default(),
            module_metadata: Default::default(),
            file_system_id: expected_source.file_system_id.clone(),
            object_id: expected_source.object_id.clone(),
            version: version("version_family_fence_late"),
            content_hash: Some("blake3:family-fence-late".to_string()),
            size_bytes: Some(1),
            locations: vec![node("node_owner")],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        })
        .expect("concurrent family append");
    drop(concurrent);

    let mut target_object = expected_source.clone();
    target_object.path = "/services/readme.md".to_string();
    assert_eq!(
        source
            .put_cross_shard_tombstone_if_unchanged(
                &expected_source,
                &target_object,
                &source_shard_id,
                &target_shard_id,
                fenced_sequence,
                None,
            )
            .expect("conditional eviction"),
        DfsConditionalSourceEviction::JournalAdvanced,
        "a family append between tail observation and eviction must win the source lock fence"
    );
    assert_eq!(
        source
            .get_object(&expected_source.object_id)
            .expect("source object after fence"),
        Some(expected_source),
        "the source remains live until the late family entry is replayed"
    );

    cleanup(&root);
}

#[test]
fn sharded_metadata_history_projection_inventories_replica_without_scheduling_it() {
    let root = temp_root("sharded_metadata_history_projection_replica_inventory");
    let local_node_id = node("node_owner");
    let root_shard = DfsMetadataShardId::new("root");
    let services_shard = DfsMetadataShardId::new("services");
    let root_config = shard_config(&root_shard.0, DfsMetadataShardAssignment::Root);
    let mut services_config = shard_config(
        &services_shard.0,
        DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        },
    );
    services_config.primary_node_id = node("node_remote_primary");
    services_config.replica_node_ids = vec![local_node_id.clone()];
    let mut backend = LocalShardedMetadataBackend::new_with_replica_repair(
        &root,
        vec![root_config, services_config],
        &local_node_id,
    )
    .expect("sharded backend with local replica");

    let file_system = file_system();
    let mut current = object(version("version_replica_projection_current"));
    current.path = "/services/replica-history.txt".to_string();
    let old_version = version("version_replica_projection_old");
    let old_record =
        object_version_record(&current, old_version.clone(), OffsetDateTime::UNIX_EPOCH);
    let current_record = object_version_record(
        &current,
        current.version.clone(),
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
    );
    let replica = backend
        .shard_backend_mut(&services_shard)
        .expect("services replica backend");
    replica
        .put_file_system(&file_system)
        .expect("replica filesystem");
    replica
        .put_object(&current)
        .expect("replica current object");
    replica
        .put_version(&old_record)
        .expect("replica old version");
    replica
        .put_version(&current_record)
        .expect("replica current version");

    let projection = backend
        .metadata_history_gc_projection(
            DfsMetadataHistoryGcOptions::dry_run(Duration::ZERO, 1)
                .with_now_unix_nanos(2_000_000_000),
        )
        .expect("sharded GC projection");
    let replica_stats = projection
        .shards
        .iter()
        .find(|stats| stats.shard_id == services_shard)
        .expect("replica shard inventory");
    assert!(
        projection
            .shards
            .iter()
            .find(|stats| stats.shard_id == root_shard)
            .is_some_and(|stats| stats.collection_authorized),
        "the local authoritative shard must remain eligible to contribute collection pressure"
    );
    assert!(
        !replica_stats.collection_authorized,
        "replica inventory must be explicitly excluded from collection pressure"
    );
    assert_eq!(
        replica_stats
            .tables
            .iter()
            .find(|table| table.table == "versions")
            .map(|table| table.record_count),
        Some(2)
    );
    assert_eq!(
        projection.eligible_version_count, 0,
        "replica history must remain observable without becoming authoritative scheduled work"
    );
    let gc = backend
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(2_000_000_000),
        )
        .expect("primary-only sharded GC");
    assert_eq!(gc.applied_record_count(), 0);
    assert_eq!(
        backend
            .shard_backend(&services_shard)
            .expect("services replica")
            .get_version(&current.object_id, &old_version)
            .expect("replica old version"),
        Some(old_record)
    );

    backend
        .shard_backend_mut(&root_shard)
        .expect("root primary")
        .freeze_metadata_shard_for_membership_handoff(&root_shard, "projection-pressure-freeze", 1)
        .expect("freeze root primary")
        .expect("local root shard");
    let frozen_projection = backend
        .metadata_history_gc_projection(
            DfsMetadataHistoryGcOptions::dry_run(Duration::ZERO, 1)
                .with_now_unix_nanos(2_000_000_000),
        )
        .expect("frozen sharded GC projection");
    assert!(
        frozen_projection
            .shards
            .iter()
            .all(|stats| !stats.collection_authorized),
        "handoff-frozen and replica shards must remain inventory-only"
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_pages_object_local_records_with_shard_cursors() {
    let root = temp_root("sharded_backend_pages_object_local_records");
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
    backend.put_file_system(&file_system).expect("filesystem");

    let mut file = object(version("version_0"));
    file.path = "/services/readme.md".to_string();
    backend.put_object(&file).expect("object");

    for index in 0..3 {
        let version_record = DfsObjectVersionRecord {
            content_durability: None,
            metadata_format: crate::DfsRevisionMetadataFormat::Legacy,
            retention: Default::default(),
            module_metadata: Default::default(),
            file_system_id: file.file_system_id.clone(),
            object_id: file.object_id.clone(),
            version: version(&format!("version_{index}")),
            content_hash: Some(format!("blake3:version-{index}")),
            size_bytes: Some(index),
            locations: vec![node("node_owner")],
            created_by_event_id: None,
            created_at: OffsetDateTime::now_utc(),
            visible: index == 0,
        };
        backend.put_version(&version_record).expect("version");
    }

    let first_version_page = backend
        .versions_for_object_page(&file.object_id, None, 1)
        .expect("first version page");
    assert_eq!(first_version_page.versions.len(), 1);
    assert_eq!(
        first_version_page
            .next_cursor
            .as_ref()
            .and_then(|cursor| cursor.shard_id.as_ref())
            .map(|shard_id| shard_id.0.as_str()),
        Some("services")
    );
    let first_version_scan = backend
        .version_records_page(None, 2)
        .expect("first version scan page");
    assert_eq!(first_version_scan.versions.len(), 2);
    assert!(first_version_scan.next_cursor.is_some());
    let second_version_scan = backend
        .version_records_page(first_version_scan.next_cursor.as_ref(), 2)
        .expect("second version scan page");
    assert_eq!(second_version_scan.versions.len(), 1);
    assert!(second_version_scan.next_cursor.is_none());
    let scanned_versions = first_version_scan
        .versions
        .into_iter()
        .chain(second_version_scan.versions)
        .map(|version_record| version_record.version)
        .collect::<Vec<_>>();
    let mut expected_versions = (0..3)
        .map(|index| version(&format!("version_{index}")))
        .collect::<Vec<_>>();
    expected_versions.sort();
    assert_eq!(scanned_versions, expected_versions);

    for index in 0..3 {
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                committed_version: version("version_0"),
                owner_node_id: node("node_owner"),
                event_id: EventId::generate(),
            },
            replica_node_id: node(&format!("node_replica_{index}")),
            content_hash: Some("blake3:version-0".to_string()),
            received_at: OffsetDateTime::now_utc(),
        };
        backend.put_receipt(&receipt).expect("receipt");
    }
    let first_receipt_page = backend
        .receipts_for_version_page(&file.object_id, &version("version_0"), None, 1)
        .expect("first receipt page");
    assert_eq!(first_receipt_page.receipts.len(), 1);
    assert_eq!(
        first_receipt_page
            .next_cursor
            .as_ref()
            .and_then(|cursor| cursor.shard_id.as_ref())
            .map(|shard_id| shard_id.0.as_str()),
        Some("services")
    );
    let first_receipt_scan = backend
        .receipt_records_page(None, 2)
        .expect("first receipt scan page");
    assert_eq!(first_receipt_scan.receipts.len(), 2);
    assert!(first_receipt_scan.next_cursor.is_some());
    let second_receipt_scan = backend
        .receipt_records_page(first_receipt_scan.next_cursor.as_ref(), 2)
        .expect("second receipt scan page");
    assert_eq!(second_receipt_scan.receipts.len(), 1);
    assert!(second_receipt_scan.next_cursor.is_none());
    let scanned_receipts = first_receipt_scan
        .receipts
        .into_iter()
        .chain(second_receipt_scan.receipts)
        .map(|receipt| receipt.replica_node_id)
        .collect::<Vec<_>>();
    let mut expected_receipts = (0..3)
        .map(|index| node(&format!("node_replica_{index}")))
        .collect::<Vec<_>>();
    expected_receipts.sort();
    assert_eq!(scanned_receipts, expected_receipts);

    for index in 0..3 {
        let intent = DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: format!("intent-{index}"),
            file_system_id: file.file_system_id.clone(),
            object_id: file.object_id.clone(),
            base_version: Some(version("version_0")),
            target_version: version(&format!("version_{}", index + 1)),
            coordinator_node_id: node("node_owner"),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some(format!("blake3:intent-{index}")),
            object_metadata: Some(file.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::now_utc(),
            updated_at: None,
        };
        backend.put_intent(&intent).expect("intent");
    }
    let first_intent_page = backend
        .intents_for_object_page(&file.object_id, None, 1)
        .expect("first intent page");
    assert_eq!(first_intent_page.intents.len(), 1);
    assert_eq!(
        first_intent_page
            .next_cursor
            .as_ref()
            .and_then(|cursor| cursor.shard_id.as_ref())
            .map(|shard_id| shard_id.0.as_str()),
        Some("services")
    );
    let first_scan_page = backend
        .write_intents_page(None, 2)
        .expect("first write intent scan page");
    assert_eq!(first_scan_page.intents.len(), 2);
    assert!(first_scan_page.next_cursor.is_some());
    let second_scan_page = backend
        .write_intents_page(first_scan_page.next_cursor.as_ref(), 2)
        .expect("second write intent scan page");
    assert_eq!(second_scan_page.intents.len(), 1);
    assert!(second_scan_page.next_cursor.is_none());
    let scanned_intent_ids = first_scan_page
        .intents
        .into_iter()
        .chain(second_scan_page.intents)
        .map(|intent| intent.intent_id)
        .collect::<Vec<_>>();
    assert_eq!(scanned_intent_ids, vec!["intent-0", "intent-1", "intent-2"]);

    let target_version = version("version_shared_target");
    for index in 0..3 {
        let intent = DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: format!("intent-shared-target-{index}"),
            file_system_id: file.file_system_id.clone(),
            object_id: file.object_id.clone(),
            base_version: Some(version("version_0")),
            target_version: target_version.clone(),
            coordinator_node_id: node("node_owner"),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some(format!("blake3:shared-target-{index}")),
            object_metadata: Some(file.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::now_utc(),
            updated_at: None,
        };
        backend.put_intent(&intent).expect("shared target intent");
    }
    let first_target_page = backend
        .intents_for_object_version_page(&file.object_id, &target_version, None, 2)
        .expect("first target intent page");
    assert_eq!(first_target_page.intents.len(), 2);
    assert_eq!(
        first_target_page
            .next_cursor
            .as_ref()
            .and_then(|cursor| cursor.shard_id.as_ref())
            .map(|shard_id| shard_id.0.as_str()),
        Some("services")
    );
    let second_target_page = backend
        .intents_for_object_version_page(
            &file.object_id,
            &target_version,
            first_target_page.next_cursor.as_ref(),
            2,
        )
        .expect("second target intent page");
    assert_eq!(second_target_page.intents.len(), 1);
    assert!(second_target_page.next_cursor.is_none());
    let shared_target_ids = first_target_page
        .intents
        .into_iter()
        .chain(second_target_page.intents)
        .map(|intent| intent.intent_id)
        .collect::<Vec<_>>();
    assert_eq!(
        shared_target_ids,
        vec![
            "intent-shared-target-0".to_string(),
            "intent-shared-target-1".to_string(),
            "intent-shared-target-2".to_string()
        ]
    );

    let mut transient_object = file.clone();
    transient_object.object_id = DfsObjectId::from_legacy_named("obj_sharded_transient_intent")
        .expect("transient object id");
    transient_object.path = "/services/transient-create.txt".to_string();
    transient_object.version = version("version_transient_1");
    let transient_intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "intent-transient-create".to_string(),
        file_system_id: transient_object.file_system_id.clone(),
        object_id: transient_object.object_id.clone(),
        base_version: None,
        target_version: version("version_transient_2"),
        coordinator_node_id: node("node_owner"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 1,
        desired_replica_node_ids: vec![node("node_owner")],
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        status: DfsWriteIntentStatus::Staging,
        content_hash: Some("blake3:transient".to_string()),
        object_metadata: Some(transient_object.clone()),
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::now_utc(),
        updated_at: None,
    };
    backend
        .put_intent_for_object(Some(&transient_object), &transient_intent)
        .expect("transient intent");
    assert_eq!(
        backend
            .intents_for_object_version_page(
                &transient_object.object_id,
                &transient_intent.target_version,
                None,
                1,
            )
            .expect("transient target intent page")
            .intents
            .into_iter()
            .map(|intent| intent.intent_id)
            .collect::<Vec<_>>(),
        vec!["intent-transient-create"]
    );

    let mut moved_object = object(version("version_1"));
    moved_object.object_id = DfsObjectId::from_legacy_named("obj_sharded_stale_intent_source")
        .expect("stale intent object id");
    moved_object.path = "/stale-intent-source.txt".to_string();
    backend.put_object(&moved_object).expect("root object");
    let stale_source_intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "intent-stale-source".to_string(),
        file_system_id: moved_object.file_system_id.clone(),
        object_id: moved_object.object_id.clone(),
        base_version: Some(moved_object.version.clone()),
        target_version: version("version_stale_source_2"),
        coordinator_node_id: node("node_owner"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 1,
        desired_replica_node_ids: vec![node("node_owner")],
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: Some("blake3:stale-source".to_string()),
        object_metadata: Some(moved_object.clone()),
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::now_utc(),
        updated_at: None,
    };
    backend
        .put_intent(&stale_source_intent)
        .expect("source shard intent");
    let mut moved_target_object = moved_object.clone();
    moved_target_object.path = "/services/stale-intent-source.txt".to_string();
    moved_target_object.version = version("version_stale_source_3");
    backend
        .put_object(&moved_target_object)
        .expect("move object to services");
    assert!(
        backend
            .intents_for_object_version_page(
                &moved_object.object_id,
                &stale_source_intent.target_version,
                None,
                1,
            )
            .expect("stale source target intent page")
            .intents
            .is_empty(),
        "target-version lookups should hide source-shard intents after the object index moves"
    );

    let mut scan_cursor = None;
    let mut visible_scan_ids = BTreeSet::new();
    loop {
        let page = backend
            .write_intents_page(scan_cursor.as_ref(), 2)
            .expect("visible intent scan page");
        assert!(page.intents.len() <= 2);
        visible_scan_ids.extend(page.intents.into_iter().map(|intent| intent.intent_id));
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        scan_cursor = Some(next_cursor);
    }
    assert!(
        visible_scan_ids.contains("intent-transient-create"),
        "transient create intents should remain visible in global scans"
    );
    assert!(
        !visible_scan_ids.contains("intent-stale-source"),
        "global scans should hide source-shard intents after the object index moves"
    );
    assert_eq!(
        backend
            .all_intents()
            .expect("all intents")
            .into_iter()
            .map(|intent| intent.intent_id)
            .collect::<BTreeSet<_>>(),
        visible_scan_ids
    );

    for index in 0..3 {
        let conflict = DfsConflictRecord {
            conflict_id: format!("conflict-{index}"),
            file_system_id: file.file_system_id.clone(),
            object_id: file.object_id.clone(),
            path: file.path.clone(),
            base_version: Some(version("version_0")),
            local_version: version(&format!("version_local_{index}")),
            remote_version: version(&format!("version_remote_{index}")),
            detected_by_node_id: node("node_owner"),
            reason: format!("conflict {index}"),
            local_content_hash: Some(format!("blake3:local-{index}")),
            remote_content_hash: Some(format!("blake3:remote-{index}")),
            created_by_intent_id: None,
            strategy: Some("branch".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_conflict(&conflict).expect("conflict");
    }
    let first_conflict_scan = backend
        .conflict_records_page(None, 2)
        .expect("first conflict scan page");
    assert_eq!(first_conflict_scan.conflicts.len(), 2);
    assert!(first_conflict_scan.next_cursor.is_some());
    let second_conflict_scan = backend
        .conflict_records_page(first_conflict_scan.next_cursor.as_ref(), 2)
        .expect("second conflict scan page");
    assert_eq!(second_conflict_scan.conflicts.len(), 1);
    assert!(second_conflict_scan.next_cursor.is_none());
    let scanned_conflicts = first_conflict_scan
        .conflicts
        .into_iter()
        .chain(second_conflict_scan.conflicts)
        .map(|conflict| conflict.conflict_id)
        .collect::<Vec<_>>();
    assert_eq!(
        scanned_conflicts,
        vec!["conflict-0", "conflict-1", "conflict-2"]
    );

    cleanup(&root);
}

#[test]
fn sharded_global_scans_ignore_stale_source_family_without_index() {
    let root = temp_root("sharded_global_scan_stale_source_family");
    let source_shard_id = DfsMetadataShardId::new("a-root");
    let target_shard_id = DfsMetadataShardId::new("z-services");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config(&source_shard_id.0, DfsMetadataShardAssignment::Root),
            shard_config(
                &target_shard_id.0,
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let file_system = file_system();
    backend.put_file_system(&file_system).expect("filesystem");

    let mut moved_object = object(version("version_moved"));
    moved_object.object_id =
        DfsObjectId::from_legacy_named("obj_global_scan_moved").expect("object");
    moved_object.path = "/services/moved.txt".to_string();
    moved_object.content_hash = Some("blake3:moved".to_string());
    let version_record = DfsObjectVersionRecord {
        content_durability: None,
        metadata_format: crate::DfsRevisionMetadataFormat::Legacy,
        retention: Default::default(),
        module_metadata: Default::default(),
        file_system_id: moved_object.file_system_id.clone(),
        object_id: moved_object.object_id.clone(),
        version: moved_object.version.clone(),
        content_hash: moved_object.content_hash.clone(),
        size_bytes: Some(5),
        locations: vec![node("node_owner")],
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    let receipt = DfsReplicaReceiptRecord {
        receipt: DfsWriteReceipt {
            file_system_id: moved_object.file_system_id.clone(),
            object_id: moved_object.object_id.clone(),
            committed_version: moved_object.version.clone(),
            owner_node_id: node("node_owner"),
            event_id: EventId::generate(),
        },
        replica_node_id: node("node_owner"),
        content_hash: moved_object.content_hash.clone(),
        received_at: OffsetDateTime::UNIX_EPOCH,
    };
    let conflict = DfsConflictRecord {
        conflict_id: "conflict-moved".to_string(),
        file_system_id: moved_object.file_system_id.clone(),
        object_id: moved_object.object_id.clone(),
        path: moved_object.path.clone(),
        base_version: None,
        local_version: version("version_local"),
        remote_version: moved_object.version.clone(),
        detected_by_node_id: node("node_owner"),
        reason: "stale source conflict".to_string(),
        local_content_hash: Some("blake3:local".to_string()),
        remote_content_hash: moved_object.content_hash.clone(),
        created_by_intent_id: None,
        strategy: Some("branch".to_string()),
        status: DfsConflictStatus::Open,
        detected_at: OffsetDateTime::UNIX_EPOCH,
    };
    {
        let source = backend
            .shard_backend_mut(&source_shard_id)
            .expect("source shard");
        source.put_object(&moved_object).expect("source object");
        source.put_version(&version_record).expect("source version");
        source.put_receipt(&receipt).expect("source receipt");
        source.put_conflict(&conflict).expect("source conflict");
    }

    let migration = backend
        .migrate_objects_to_configured_shards(&file_system.file_system_id, 1)
        .expect("migrate object family");
    assert_eq!(migration.migrated_objects, 1);
    assert_eq!(
        backend
            .shard_backend(&source_shard_id)
            .expect("source shard")
            .get_object(&moved_object.object_id)
            .expect("source object")
            .expect("source tombstone")
            .kind,
        DfsObjectKind::Tombstone
    );
    assert_eq!(
        backend
            .shard_backend(&target_shard_id)
            .expect("target shard")
            .get_object(&moved_object.object_id)
            .expect("target object"),
        Some(moved_object.clone())
    );

    // Model the recovery edge where destination installation succeeded but
    // the source eviction tombstone did not. Both physical shards now have
    // a live record, but only the destination is routed for this path.
    backend
        .shard_backend_mut(&source_shard_id)
        .expect("source shard")
        .put_object(&moved_object)
        .expect("restore stale live source record");
    assert_ne!(
        backend
            .shard_backend(&source_shard_id)
            .expect("source shard")
            .get_object(&moved_object.object_id)
            .expect("source object")
            .expect("stale source object")
            .kind,
        DfsObjectKind::Tombstone
    );

    std::fs::remove_file(backend.object_shard_index_path(&moved_object.object_id))
        .expect("model an index-less metadata replica");
    backend
        .shard_backend_mut(&target_shard_id)
        .expect("target shard")
        .remove_receipt(
            &moved_object.object_id,
            &moved_object.version,
            &receipt.replica_node_id,
        )
        .expect("remove target receipt")
        .expect("target receipt existed");
    let mut resolved_conflict = conflict.clone();
    resolved_conflict.status = DfsConflictStatus::Resolved;
    resolved_conflict.reason = "resolved on live destination".to_string();
    backend
        .shard_backend_mut(&target_shard_id)
        .expect("target shard")
        .put_conflict(&resolved_conflict)
        .expect("update target conflict");

    let receipt_page = backend
        .receipt_records_page(None, 8)
        .expect("global receipt scan");
    assert!(
        receipt_page.receipts.is_empty(),
        "removed destination receipt must not be resurrected from the stale source shard"
    );
    let conflict_page = backend
        .conflict_records_page(None, 8)
        .expect("global conflict scan");
    assert_eq!(conflict_page.conflicts, vec![resolved_conflict]);

    cleanup(&root);
}

#[test]
fn sharded_backend_migrates_existing_root_object_to_new_path_shard() {
    let root = temp_root("sharded_backend_migrates_existing_root_object");
    let file_system = file_system();
    let mut object = object(version("version_1"));
    object.path = "/services/readme.md".to_string();
    object.content_hash = Some("blake3:migrated".to_string());
    object.content_size_bytes = Some(8);
    let version_record = DfsObjectVersionRecord {
        content_durability: None,
        metadata_format: crate::DfsRevisionMetadataFormat::Legacy,
        retention: Default::default(),
        module_metadata: Default::default(),
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        size_bytes: object.content_size_bytes,
        locations: object.current_locations.clone(),
        created_by_event_id: object.created_by_event_id.clone(),
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    let receipt = DfsReplicaReceiptRecord {
        receipt: DfsWriteReceipt {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            committed_version: object.version.clone(),
            owner_node_id: node("node_owner"),
            event_id: EventId::from_legacy_named("evt_migrate_receipt").expect("event"),
        },
        replica_node_id: node("node_owner"),
        content_hash: object.content_hash.clone(),
        received_at: OffsetDateTime::UNIX_EPOCH,
    };
    let intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "dfs_intent_migrate".to_string(),
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        base_version: Some(object.version.clone()),
        target_version: version("version_2"),
        coordinator_node_id: node("node_owner"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 1,
        desired_replica_node_ids: vec![node("node_owner")],
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: object.content_hash.clone(),
        object_metadata: None,
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: Some(OffsetDateTime::UNIX_EPOCH),
    };
    let conflict = DfsConflictRecord {
        conflict_id: "conflict_migrate".to_string(),
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        path: object.path.clone(),
        base_version: Some(object.version.clone()),
        local_version: version("version_local"),
        remote_version: version("version_remote"),
        detected_by_node_id: node("node_owner"),
        reason: "migration coverage".to_string(),
        local_content_hash: Some("blake3:local".to_string()),
        remote_content_hash: Some("blake3:remote".to_string()),
        created_by_intent_id: Some(intent.intent_id.clone()),
        strategy: Some("manual".to_string()),
        status: DfsConflictStatus::Open,
        detected_at: OffsetDateTime::UNIX_EPOCH,
    };
    let mut expected_lock_state;
    {
        let mut root_only = LocalShardedMetadataBackend::new(
            &root,
            vec![shard_config("root", DfsMetadataShardAssignment::Root)],
        )
        .expect("root-only sharded backend");
        root_only.put_file_system(&file_system).expect("filesystem");
        root_only.put_object(&object).expect("root object");
        root_only.put_version(&version_record).expect("version");
        root_only.put_receipt(&receipt).expect("receipt");
        root_only.put_intent(&intent).expect("intent");
        root_only.put_conflict(&conflict).expect("conflict");
        root_only
            .apply_advisory_lock_command(
                &object.file_system_id,
                &object.object_id,
                DfsAdvisoryLockTransitionContext {
                    authority_epoch: 1,
                    now_unix_nanos: 1,
                    lease_duration_nanos: 100,
                    max_clock_skew_nanos: 0,
                },
                &DfsAdvisoryLockCommand::Acquire {
                    request_id: DfsAdvisoryLockRequestId::new("migration-lock"),
                    session_id: DfsAdvisoryLockSessionId::new("migration-session"),
                    mode: DfsAdvisoryLockMode::Exclusive,
                    blocking: false,
                    process_id: None,
                },
            )
            .expect("lock state");
        expected_lock_state = root_only
            .get_advisory_lock_state(&object.file_system_id, &object.object_id)
            .expect("lock state")
            .expect("lock aggregate");
    }
    expected_lock_state.authority_rebind_required = true;

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
    let report = backend
        .migrate_objects_to_configured_shards(&file_system.file_system_id, 16)
        .expect("migrate objects");

    assert_eq!(
        report,
        DfsMetadataShardMigrationReport {
            file_system_id: file_system.file_system_id.clone(),
            scanned_objects: 1,
            indexed_objects: 0,
            migrated_objects: 1,
            skipped_stale_sources: 0,
            reached_limit: false,
        }
    );
    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, &object.path)
            .expect("migrated path"),
        Some(object.clone())
    );
    assert_eq!(
        backend
            .get_version(&object.object_id, &object.version)
            .expect("migrated version"),
        Some(version_record.clone())
    );
    assert_eq!(
        backend
            .receipts_for_version(&object.object_id, &object.version)
            .expect("migrated receipts"),
        vec![receipt.clone()]
    );
    assert_eq!(
        backend
            .get_intent(&object.object_id, &intent.intent_id)
            .expect("migrated intent"),
        Some(intent)
    );
    assert_eq!(
        backend
            .get_conflict(&object.object_id, &conflict.conflict_id)
            .expect("migrated conflict"),
        Some(conflict)
    );
    assert_eq!(
        backend
            .get_advisory_lock_state(&object.file_system_id, &object.object_id)
            .expect("migrated lock state"),
        Some(expected_lock_state),
        "same-node shard migration must preserve the object lock aggregate"
    );
    assert_eq!(
        backend
            .shard_backend(&DfsMetadataShardId::new("root"))
            .expect("root shard")
            .get_object(&object.object_id)
            .expect("root object")
            .as_ref()
            .map(|object| object.kind.clone()),
        Some(DfsObjectKind::Tombstone),
        "source shard should retain an explicit tombstone for path-history replay"
    );
    assert_eq!(
        backend
            .read_object_shard_index(&object.object_id)
            .expect("object index")
            .as_ref()
            .map(|index| (&index.path, &index.shard_id)),
        Some((
            &normalize_path(&object.path),
            &DfsMetadataShardId::new("services")
        ))
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_bounds_configured_shard_migration() {
    let root = temp_root("sharded_backend_bounds_configured_shard_migration");
    let file_system = file_system();
    let mut first = object(version("version_1"));
    first.path = "/services/a.txt".to_string();
    let mut second = object(version("version_1"));
    second.object_id = DfsObjectId::from_legacy_named("obj_second").expect("object");
    second.path = "/users/b.txt".to_string();
    {
        let mut root_only = LocalShardedMetadataBackend::new(
            &root,
            vec![shard_config("root", DfsMetadataShardAssignment::Root)],
        )
        .expect("root-only sharded backend");
        root_only.put_file_system(&file_system).expect("filesystem");
        root_only.put_object(&first).expect("first root object");
        root_only.put_object(&second).expect("second root object");
    }

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
            shard_config(
                "users",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/users".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let report = backend
        .migrate_objects_to_configured_shards(&file_system.file_system_id, 1)
        .expect("bounded migration");

    assert_eq!(
        report.scanned_objects, 1,
        "bounded migration should not scan the rest of the shard after reaching the migration limit"
    );
    assert_eq!(report.migrated_objects, 1);
    assert!(report.reached_limit);
    assert_eq!(
        backend
            .objects_for_file_system(&file_system.file_system_id)
            .expect("objects")
            .into_iter()
            .filter(|object| object.kind != DfsObjectKind::Tombstone)
            .count(),
        2,
        "bounded migration must keep both live objects visible through the sharded object index"
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_falls_back_to_source_path_during_shard_activation() {
    let root = temp_root("sharded_backend_activation_path_fallback");
    let file_system = file_system();
    let mut object = object(version("version_1"));
    object.path = "/services/pre-migration.txt".to_string();
    {
        let mut root_only = LocalShardedMetadataBackend::new(
            &root,
            vec![shard_config("root", DfsMetadataShardAssignment::Root)],
        )
        .expect("root-only sharded backend");
        root_only.put_file_system(&file_system).expect("filesystem");
        root_only.put_object(&object).expect("root object");
    }

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
            shard_config(
                "users",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/users".to_string(),
                },
            ),
            shard_config(
                "agents",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/agents".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");

    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, &object.path)
            .expect("activation fallback path"),
        Some(object),
        "publishing a new path-prefix shard before migration must not make existing root-shard paths disappear"
    );

    reset_metadata_current_tables_snapshot_calls();
    assert_eq!(
        backend
            .get_object_by_path(
                &file_system.file_system_id,
                "/services/not-created-during-migration.txt",
            )
            .expect("bounded migration miss"),
        None
    );
    assert_eq!(
        metadata_current_tables_snapshot_calls(),
        2,
        "a create miss should inspect the selected shard plus the one previous-topology source, not every configured shard"
    );

    let migration = backend
        .migrate_objects_to_configured_shards(&file_system.file_system_id, 8)
        .expect("finish bounded migration");
    assert!(!migration.reached_limit);
    reset_metadata_current_tables_snapshot_calls();
    assert_eq!(
        backend
            .get_object_by_path(
                &file_system.file_system_id,
                "/services/not-created-after-migration.txt",
            )
            .expect("steady-state miss"),
        None
    );
    assert_eq!(
        metadata_current_tables_snapshot_calls(),
        1,
        "a completed migration should retire its fallback topology"
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_ignores_index_for_unconfigured_local_shard() {
    let root = temp_root("sharded_backend_unconfigured_index");
    let file_system = file_system();
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![shard_config("root", DfsMetadataShardAssignment::Root)],
    )
    .expect("root-only sharded backend");
    let mut object = object(version("version_1"));
    object.path = "/services/indexed-before-kv.txt".to_string();
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("root object");
    backend
        .write_object_shard_index(&object, &DfsMetadataShardId::new("services"))
        .expect("stale object shard index");

    assert_eq!(
        backend.get_object(&object.object_id).expect("object by id"),
        Some(object.clone()),
        "an index that names an unopened shard must not break object-id reads"
    );
    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, &object.path)
            .expect("object by path"),
        Some(object),
        "an index that names an unopened shard must not hide source-shard path reads"
    );

    cleanup(&root);
}

#[test]
fn local_backend_rejects_same_generation_table_log_shrink_below_cached_offset() {
    let root = temp_root("local_backend_rejects_external_table_log_truncation");
    let mut writer = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("writer backend");
    let file_system = file_system();
    writer.put_file_system(&file_system).expect("filesystem");

    let mut stale = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("stale backend");
    let stale_offset = stale.table_log_offset;
    assert!(
        stale_offset > 1,
        "stale handle needs a cached table-log cursor"
    );

    let table_path = metadata_table_path(&root);
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&table_path)
        .expect("open table log");
    file.set_len(stale_offset - 1)
        .and_then(|()| file.sync_all())
        .expect("shrink table log below cached cursor");
    let corrupted_log = fs::read(&table_path).expect("capture corrupt table log");
    let journal_head = stale.journal_last_sequence();
    assert!(
        matches!(stale.current_tables_snapshot(), Err(MetadataBackendError::Serde(ref message))
            if message.contains("without a generation change")),
        "same-file shrink cannot return a silently older lock-free snapshot"
    );
    let mut attempted = file_system.clone();
    attempted.display_name = "must not publish after table-log corruption".into();
    assert!(
        matches!(stale.put_file_system(&attempted), Err(MetadataBackendError::Serde(ref message))
            if message.contains("without a generation change")),
        "same-file corruption must fence the writer before another effect"
    );
    assert_eq!(stale.journal_last_sequence(), journal_head);
    assert_eq!(stale.table_log_offset, stale_offset);
    assert_eq!(
        fs::read(&table_path).expect("table log remains untouched"),
        corrupted_log,
        "rejection must not truncate, reset or repair durable records"
    );

    cleanup(&root);
}

#[test]
fn local_backend_syncs_metadata_directories() {
    let root = temp_root("local_backend_syncs_metadata_directories");
    fs::create_dir_all(&root).expect("root dir");

    sync_dir(&root).expect("directory sync should work on supported host filesystems");

    cleanup(&root);
}

#[test]
fn local_backend_lists_objects_from_filesystem_object_index() {
    let root = temp_root("local_backend_lists_objects_from_filesystem_object_index");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    let other_file_system = DfsFileSystem {
        metadata_format: crate::DfsRevisionMetadataFormat::Legacy,
        retention_policies: Default::default(),
        module_metadata: Default::default(),
        file_system_id: DfsFileSystemId::from_legacy_named("otherfs").expect("filesystem"),
        display_name: "other".to_string(),
        owner_node_id: node("node_owner"),
        subtree_policies: Vec::new(),
    };
    backend.put_file_system(&file_system).expect("filesystem");
    backend
        .put_file_system(&other_file_system)
        .expect("other filesystem");

    let mut readme = object(version("version_1"));
    readme.path = "/docs/readme.md".to_string();
    let mut changelog = object(version("version_1"));
    changelog.object_id = DfsObjectId::from_legacy_named("obj_changelog").expect("object");
    changelog.path = "/docs/changelog.md".to_string();
    let mut other = object(version("version_1"));
    other.object_id = DfsObjectId::from_legacy_named("obj_other").expect("object");
    other.file_system_id = other_file_system.file_system_id.clone();
    other.path = "/other.txt".to_string();
    for object in [&readme, &changelog, &other] {
        backend.put_object(object).expect("object");
    }
    assert!(table_file_system_object_exists(
        &backend,
        &file_system_id(),
        &readme.object_id
    ));

    let objects = backend
        .objects_for_file_system(&file_system_id())
        .expect("objects");
    assert_eq!(
        objects
            .iter()
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/docs/changelog.md", "/docs/readme.md"]
    );

    assert!(table_file_system_object_exists(
        &backend,
        &file_system_id(),
        &changelog.object_id
    ));
    changelog.file_system_id = other_file_system.file_system_id.clone();
    changelog.path = "/moved-changelog.md".to_string();
    backend.put_object(&changelog).expect("move filesystem");
    assert!(
        !table_file_system_object_exists(&backend, &file_system_id(), &changelog.object_id),
        "moving an object to another filesystem should remove its old filesystem-object index"
    );
    assert_eq!(
        backend
            .objects_for_file_system(&file_system_id())
            .expect("objects")
            .iter()
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/docs/readme.md"]
    );

    cleanup(&root);
}

#[test]
fn replica_batch_rejects_duplicate_sequences_before_validation_or_persistence() {
    let root = temp_root("replica_batch_rejects_duplicate_sequences");
    let shard_id = DfsMetadataShardId::new("shard-duplicate-sequence");
    let mut source =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source")).expect("source");
    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("replica")).expect("replica");
    source.put_file_system(&file_system()).expect("filesystem");
    let entry = source
        .journal_since(0)
        .expect("source journal")
        .into_iter()
        .next()
        .expect("filesystem entry");
    let duplicate_sequence = entry.sequence;
    let duplicate_batch = DfsMetadataJournalBatch {
        shard_id: shard_id.clone(),
        entries: vec![entry.clone(), entry],
        next_after_sequence: duplicate_sequence,
    };
    let initial_prefix = replica.metadata_prefix_proof().expect("initial prefix");

    let error = replica
        .apply_journal_batch(&duplicate_batch)
        .expect_err("duplicate sequence must fail ordinary replica apply");
    assert!(
        error.to_string().contains("not strictly sequence-ordered"),
        "unexpected duplicate-sequence error: {error}"
    );
    assert!(
        replica
            .journal_since(0)
            .expect("replica journal")
            .is_empty()
    );
    assert_eq!(
        replica.metadata_prefix_proof().expect("unchanged prefix"),
        initial_prefix
    );

    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        file_system_id(),
        shard_id,
        node("node_duplicate_sequence_source"),
        1,
    );
    let error = replica
        .apply_journal_batch_with_replica_permit(&permit, &duplicate_batch)
        .expect_err("duplicate sequence must fail before permit payload proofs are indexed");
    assert!(error.to_string().contains("not strictly sequence-ordered"));
    assert!(
        replica
            .journal_since(0)
            .expect("replica journal")
            .is_empty()
    );
    assert_eq!(
        replica.metadata_prefix_proof().expect("unchanged prefix"),
        initial_prefix
    );

    cleanup(&root);
}

#[test]
fn durable_replica_ack_materializes_a_batch_over_the_prospective_lag_bound() {
    let root = temp_root("durable_replica_ack_prospective_lag_bound");
    let shard_id = DfsMetadataShardId::new("root");
    let source_node_id = node("node_owner");
    let mut source =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source")).expect("source");
    source.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV2);

    let mut source_file_system = file_system();
    source
        .put_file_system(&source_file_system)
        .expect("source filesystem");
    let current = object(version("version_prospective_lag_1"));
    source.put_object(&current).expect("source object");
    for index in 0..MAX_DEFERRED_REPLICA_ENTRIES.saturating_sub(2) {
        source_file_system.display_name = format!("prospective lag entry {index}");
        source
            .put_file_system(&source_file_system)
            .expect("source lag entry");
    }
    let mut committed = current.clone();
    committed.version = version("version_prospective_lag_2");
    committed.mode = 0o600;
    let prepare = namespace_prepare(
        "operation-prospective-lag-bound",
        &shard_id.0,
        &current,
        committed,
    );
    source
        .prepare_namespace_transaction(&prepare)
        .expect("source namespace prepare");

    let batch = source
        .journal_batch_since_limited(
            0,
            MAX_DEFERRED_REPLICA_ENTRIES.saturating_add(1) as usize,
            usize::MAX,
        )
        .expect("source journal batch")
        .expect("source journal entries");
    assert_eq!(
        batch.entries.len(),
        MAX_DEFERRED_REPLICA_ENTRIES.saturating_add(1) as usize,
        "one incoming batch must cross the deferred materialization bound"
    );
    let exact_frames =
        DfsMetadataBackend::journal_frames_for_batch(&source, &batch).expect("exact frames");
    let source_prefix = source.metadata_prefix_proof().expect("source prefix");
    let target_sequence = batch.next_after_sequence;

    let mut shard = root_shard_config_at_epoch(&shard_id.0, prepare.owner_epoch);
    shard.namespace_prepare_codec = DfsMetadataNamespacePrepareCodec::PackedV2;
    let operation = crate::service_replica::metadata_authority_operation_for_namespace_prepare(
        &prepare,
        target_sequence,
    )
    .expect("namespace authority operation");
    let probe = DfsMetadataAuthorityProbe {
        probe_id: "probe-prospective-lag-bound".to_string(),
        expected_shard: shard.clone(),
        operation,
        durable_journal_ack: true,
        journal_frames: exact_frames.clone(),
        prepared_prefix: Some(source_prefix.clone()),
    };
    let expected_confirmation =
        crate::service_replica::metadata_authority_probe_confirmation_id(&probe)
            .expect("authority confirmation");
    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        source_file_system.file_system_id.clone(),
        shard_id.clone(),
        source_node_id,
        prepare.owner_epoch,
    );
    let wire_batch = DfsMetadataJournalBatch {
        shard_id: shard_id.clone(),
        entries: Vec::new(),
        next_after_sequence: target_sequence,
    };
    let mut replica =
        LocalSobjMetadataBackend::new(shard_id, root.join("replica")).expect("replica");

    let report = replica
        .persist_authority_journal_frames_with_replica_permit_report(
            &permit,
            &wire_batch,
            &exact_frames,
            &source_prefix,
        )
        .expect("durable exact-frame ingest");

    assert_eq!(report.applied_entries(), exact_frames.len());
    assert!(
        !report.materialization_pending(),
        "an ACK over the prospective lag bound must wait for table materialization"
    );
    assert_eq!(
        crate::service_replica::verify_persisted_metadata_authority_probe_after_apply(
            &replica, &shard, &probe, &report,
        )
        .expect("verify exact authority proof"),
        expected_confirmation,
    );
    assert_eq!(replica.journal_last_sequence(), target_sequence);
    assert_eq!(replica.materialized_through_sequence, target_sequence);
    assert_eq!(
        replica
            .get_namespace_prepare(&prepare.operation_id)
            .expect("materialized prepare"),
        Some(prepare),
    );
    assert_eq!(
        replica.metadata_prefix_proof().expect("replica prefix"),
        source_prefix,
    );
    assert_eq!(
        DfsMetadataBackend::journal_frames_for_batch(&replica, &batch)
            .expect("retained exact frames"),
        exact_frames,
        "bounded synchronous materialization must retain the authority's immutable bytes"
    );
    assert_eq!(
        replica
            .materialize_pending_replica_journal()
            .expect("no pending materialization"),
        0,
    );

    cleanup(&root);
}

#[test]
fn local_backend_normalizes_replica_locations_at_persistence_boundary() {
    let root = temp_root("local_backend_normalizes_replica_locations");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let owner = node("node_owner");
    let replica = node("node_replica");
    backend.put_file_system(&file_system()).expect("filesystem");

    let mut object = object(version("version_1"));
    object.current_locations = vec![owner.clone(), replica.clone(), owner.clone()];
    backend.put_object(&object).expect("object");
    assert_eq!(
        backend
            .get_object(&object.object_id)
            .expect("get object")
            .expect("object")
            .current_locations,
        vec![owner.clone(), replica.clone()]
    );

    let mut version_record = DfsObjectVersionRecord {
        content_durability: None,
        metadata_format: crate::DfsRevisionMetadataFormat::Legacy,
        retention: Default::default(),
        module_metadata: Default::default(),
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        size_bytes: Some(3),
        locations: vec![owner.clone(), replica.clone(), owner.clone()],
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    backend.put_version(&version_record).expect("version");
    assert_eq!(
        backend
            .get_version(&object.object_id, &object.version)
            .expect("get version")
            .expect("version")
            .locations,
        vec![owner.clone(), replica.clone()]
    );

    let version_2 = version("version_2");
    let mut committed = object.clone();
    committed.version = version_2.clone();
    committed.current_locations = vec![owner.clone(), replica.clone(), owner.clone()];
    version_record.version = version_2;
    version_record.locations = committed.current_locations.clone();
    for node_id in [&owner, &replica] {
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: committed.file_system_id.clone(),
                    object_id: committed.object_id.clone(),
                    committed_version: committed.version.clone(),
                    owner_node_id: owner.clone(),
                    event_id: EventId::from_legacy_named(format!("evt_{node_id}")).expect("event"),
                },
                replica_node_id: node_id.clone(),
                content_hash: committed.content_hash.clone(),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("receipt");
    }
    backend
        .commit_visible_version(
            &object.version,
            &committed,
            &version_record,
            &[owner.clone(), replica.clone(), owner.clone()],
        )
        .expect("visible commit");
    assert_eq!(
        backend
            .get_object(&committed.object_id)
            .expect("get committed object")
            .expect("committed object")
            .current_locations,
        vec![owner.clone(), replica.clone()]
    );
    assert_eq!(
        backend
            .get_version(&committed.object_id, &committed.version)
            .expect("get committed version")
            .expect("committed version")
            .locations,
        vec![owner, replica]
    );

    cleanup(&root);
}

#[test]
fn local_backend_persists_lists_and_updates_conflicts() {
    let root = temp_root("local_backend_persists_lists_and_updates_conflicts");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let conflict = DfsConflictRecord {
        conflict_id: "conflict_a".to_string(),
        file_system_id: file_system_id(),
        object_id: object_id(),
        path: "docs/readme.md".to_string(),
        base_version: Some(version("version_1")),
        local_version: version("version_2_local"),
        remote_version: version("version_2_remote"),
        detected_by_node_id: node("node_laptop"),
        reason: "stale reconnect write".to_string(),
        local_content_hash: Some("blake3:local".to_string()),
        remote_content_hash: Some("blake3:remote".to_string()),
        created_by_intent_id: None,
        strategy: Some("branch".to_string()),
        status: DfsConflictStatus::Open,
        detected_at: OffsetDateTime::UNIX_EPOCH,
    };
    let mut conflict_b = conflict.clone();
    conflict_b.conflict_id = "conflict_b".to_string();

    let entry = backend.put_conflict(&conflict).expect("put conflict");
    backend.put_conflict(&conflict_b).expect("put conflict b");
    assert!(matches!(
        entry.mutation,
        DfsMetadataMutation::PutConflict {
            conflict: Some(ref journal_conflict),
            ..
        } if journal_conflict == &conflict
    ));
    assert_eq!(
        backend
            .get_conflict(&conflict.object_id, &conflict.conflict_id)
            .expect("get conflict"),
        Some(conflict.clone())
    );
    assert_eq!(
        backend
            .conflicts_for_object(&conflict.object_id)
            .expect("list conflicts"),
        vec![conflict.clone(), conflict_b.clone()]
    );
    let first_page = backend
        .conflicts_for_object_page(&conflict.object_id, None, 1)
        .expect("first conflict page");
    assert_eq!(first_page.conflicts, vec![conflict.clone()]);
    let second_page = backend
        .conflicts_for_object_page(&conflict.object_id, first_page.next_cursor.as_ref(), 1)
        .expect("second conflict page");
    assert_eq!(second_page.conflicts, vec![conflict_b.clone()]);
    assert!(second_page.next_cursor.is_none());
    assert!(
        backend
            .conflicts_for_object_page(&conflict.object_id, None, 0)
            .expect("empty conflict page")
            .conflicts
            .is_empty()
    );
    let first_scan_page = backend
        .conflict_records_page(None, 1)
        .expect("first conflict scan page");
    assert_eq!(first_scan_page.conflicts, vec![conflict.clone()]);
    let second_scan_page = backend
        .conflict_records_page(first_scan_page.next_cursor.as_ref(), 1)
        .expect("second conflict scan page");
    assert_eq!(second_scan_page.conflicts, vec![conflict_b.clone()]);
    assert!(second_scan_page.next_cursor.is_none());
    assert!(
        backend
            .conflict_records_page(None, 0)
            .expect("empty conflict scan")
            .conflicts
            .is_empty()
    );
    assert_eq!(
        backend.all_conflicts().expect("all conflicts"),
        vec![conflict.clone(), conflict_b.clone()]
    );

    let resolved = backend
        .update_conflict_status(
            &conflict.object_id,
            &conflict.conflict_id,
            DfsConflictStatus::Resolved,
        )
        .expect("resolve conflict")
        .expect("conflict");
    assert_eq!(resolved.status, DfsConflictStatus::Resolved);
    assert_eq!(
        backend
            .get_conflict(&conflict.object_id, &conflict.conflict_id)
            .expect("get resolved")
            .expect("conflict")
            .status,
        DfsConflictStatus::Resolved
    );
    assert_eq!(
        backend
            .remove_conflict(&conflict.object_id, &conflict.conflict_id)
            .expect("remove resolved conflict")
            .as_ref()
            .map(|conflict| conflict.status.clone()),
        Some(DfsConflictStatus::Resolved)
    );
    assert_eq!(
        backend
            .remove_conflict(&conflict_b.object_id, &conflict_b.conflict_id)
            .expect("remove second conflict"),
        Some(conflict_b.clone())
    );
    assert_eq!(
        backend
            .get_conflict(&conflict.object_id, &conflict.conflict_id)
            .expect("resolved conflict removed"),
        None
    );
    assert!(
        backend
            .all_conflicts()
            .expect("all conflicts after removal")
            .is_empty()
    );
    let journal = backend.journal_since(0).expect("journal after removals");
    assert!(matches!(
        journal.last().map(|entry| &entry.mutation),
        Some(DfsMetadataMutation::PutConflict {
            conflict_id,
            conflict: None,
            ..
        }) if conflict_id == &conflict_b.conflict_id
    ));
    drop(backend);

    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopen backend after conflict removals");
    assert!(
        reopened
            .all_conflicts()
            .expect("reopened all conflicts")
            .is_empty()
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_routes_lock_aggregate_with_the_stable_object() {
    let root = temp_root("sharded_backend_routes_lock_aggregate");
    let configs = vec![
        shard_config("root", DfsMetadataShardAssignment::Root),
        shard_config(
            "projects",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: "/projects".to_string(),
            },
        ),
    ];
    let mut backend = LocalShardedMetadataBackend::new(&root, configs).expect("sharded");
    backend.put_file_system(&file_system()).expect("filesystem");
    let mut project = object(version("project-lock-v1"));
    project.object_id =
        DfsObjectId::from_legacy_named("project_lock_object").expect("project object id");
    project.path = "/projects/repo/readme.md".to_string();
    backend.put_object(&project).expect("project object");
    let applied = backend
        .apply_advisory_lock_command(
            &project.file_system_id,
            &project.object_id,
            DfsAdvisoryLockTransitionContext {
                authority_epoch: 1,
                now_unix_nanos: 1,
                lease_duration_nanos: 10,
                max_clock_skew_nanos: 0,
            },
            &DfsAdvisoryLockCommand::Acquire {
                request_id: DfsAdvisoryLockRequestId::new("project-acquire"),
                session_id: DfsAdvisoryLockSessionId::new("project-session"),
                mode: DfsAdvisoryLockMode::Shared,
                blocking: false,
                process_id: None,
            },
        )
        .expect("project lock");
    assert_eq!(applied.shard_id, DfsMetadataShardId::new("projects"));
    assert!(
        backend
            .get_advisory_lock_state(&project.file_system_id, &project.object_id)
            .expect("routed lock state")
            .is_some()
    );

    cleanup(&root);
}

#[test]
fn retained_sharded_backend_rebinds_legacy_prepare_codec_to_packed_v1() {
    assert_retained_sharded_backend_rebinds_prepare_codec(
        DfsMetadataNamespacePrepareCodec::Legacy,
        DfsMetadataNamespacePrepareCodec::PackedV1,
        true,
    );
}

#[test]
fn retained_sharded_backend_rebinds_packed_v1_prepare_codec_to_legacy() {
    assert_retained_sharded_backend_rebinds_prepare_codec(
        DfsMetadataNamespacePrepareCodec::PackedV1,
        DfsMetadataNamespacePrepareCodec::Legacy,
        false,
    );
}

#[test]
fn completed_inactive_marker_can_be_replaced_by_later_reverse_rotation() {
    let root = temp_root("membership-handoff-inactive-marker-replacement");
    let metadata_root = root.join("metadata");
    let shard_id = DfsMetadataShardId::new("root");
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    backend
        .freeze_metadata_shard_for_membership_handoff(&shard_id, "handoff-away-from-local", 31)
        .expect("freeze first retiring generation")
        .expect("local shard");
    backend
        .mark_metadata_shard_membership_handoff_activated(&shard_id, "handoff-away-from-local", 32)
        .expect("mark first handoff activated");
    assert_eq!(backend.instance_authority_epoch, None);

    // The node can later participate in a reverse rotation after the
    // control plane has advanced beyond its last locally completed marker.
    // Replacing that marker installs another pending fence; it does not
    // bind this stale instance to either generation.
    let reverse = backend
        .freeze_metadata_shard_for_membership_handoff(&shard_id, "handoff-back-to-local", 34)
        .expect("replace completed inactive marker")
        .expect("local shard");
    assert_eq!(reverse.handoff_id, "handoff-back-to-local");
    assert_eq!(reverse.retiring_epoch, 34);
    assert_eq!(reverse.activated_epoch, None);
    assert_eq!(backend.instance_authority_epoch, None);
    assert_eq!(
        backend
            .metadata_shard_write_freeze(&shard_id)
            .expect("read reverse-rotation marker"),
        Some(reverse)
    );
    assert!(matches!(
        backend.put_object(&object(version("version_reverse_rotation_stays_frozen"))),
        Err(MetadataBackendError::MembershipHandoffFrozen {
            ref handoff_id,
            ..
        }) if handoff_id == "handoff-back-to-local"
    ));

    cleanup(&root);
}
