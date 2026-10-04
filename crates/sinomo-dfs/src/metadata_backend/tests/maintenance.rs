//! Maintenance regression tests.

use super::*;

#[test]
fn compact_namespace_commit_replica_table_append_failure_retries_without_rollback_materialization()
{
    let root = temp_root("local_backend_compact_commit_replica_table_append_failure");
    let source_root = root.join("source");
    let replica_root = root.join("replica");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let current = object(version("version_compact_commit_append_failure_1"));
    let mut committed = current.clone();
    committed.version = version("version_compact_commit_append_failure_2");
    committed.mode = 0o600;
    let prepare = namespace_prepare(
        "operation-compact-commit-append-failure",
        &shard_id.0,
        &current,
        committed.clone(),
    );

    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), &source_root).expect("source");
    source.put_file_system(&file_system()).expect("filesystem");
    source.put_object(&current).expect("current object");
    source
        .prepare_namespace_transaction(&prepare)
        .expect("prepared transaction");
    source
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("compact commit");
    let mut entries = source.journal_since(0).expect("source journal");
    let commit_entry = entries.pop().expect("compact commit journal entry");
    assert!(matches!(
        commit_entry.mutation,
        DfsMetadataMutation::CommitNamespacePrepare { .. }
    ));
    let prepared_sequence = entries.last().expect("prepared prefix").sequence;

    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root).expect("replica");
    replica
        .apply_journal_batch(&DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            next_after_sequence: prepared_sequence,
            entries,
        })
        .expect("replicate prepared prefix");
    replica
        .persist_current_metadata_checkpoint_unlocked()
        .expect("checkpoint prepared prefix before injected table failure");

    let table_path = metadata_table_path(&replica_root);
    let writable_permissions = fs::metadata(&table_path)
        .expect("empty materialized table log metadata")
        .permissions();
    let mut readonly_permissions = writable_permissions.clone();
    readonly_permissions.set_readonly(true);
    fs::set_permissions(&table_path, readonly_permissions)
        .expect("block compact commit table append");
    crate::metadata_tables::reset_namespace_commit_materialization_counters();
    let commit_batch = DfsMetadataJournalBatch {
        shard_id: shard_id.clone(),
        next_after_sequence: commit_entry.sequence,
        entries: vec![commit_entry.clone()],
    };
    let error = replica
        .apply_journal_batch(&commit_batch)
        .expect_err("blocked table append must reject compact replica commit");
    assert!(matches!(error, MetadataBackendError::Io { .. }));
    assert_eq!(
        crate::metadata_tables::namespace_commit_materialization_counters(),
        (0, 0),
        "a failed table append must not materialize or roll back the compact commit"
    );
    assert_eq!(
        replica.tables.objects.get(&current.object_id),
        Some(&current),
        "the failed append must leave the preexisting object visible"
    );
    assert_eq!(
        replica
            .tables
            .namespace_prepares
            .get(&prepare.operation_id)
            .map(|record| record.status.clone()),
        Some(DfsNamespacePrepareStatus::Prepared),
        "the failed append must leave the durable prepare uncommitted"
    );
    assert!(replica.tables.namespace_prepare_receipts.is_empty());
    assert!(replica.tables.namespace_transaction_receipts.is_empty());
    assert_eq!(
        replica.materialized_through_sequence, prepared_sequence,
        "a journal-only compact commit must not advance table materialization"
    );
    assert_eq!(
        replica.journal_last_sequence(),
        commit_entry.sequence,
        "the authoritative replica journal remains durable for retry"
    );

    fs::set_permissions(&table_path, writable_permissions)
        .expect("restore materialized table log permissions");
    replica
        .apply_journal_batch(&commit_batch)
        .expect("same-instance compact commit retry");
    assert_eq!(
        crate::metadata_tables::namespace_commit_materialization_counters(),
        (0, 1),
        "the successful retry must use one post-append materialization"
    );
    assert_eq!(
        replica
            .get_object(&current.object_id)
            .expect("visible object after compact commit retry"),
        Some(committed.clone())
    );
    assert!(
        !replica
            .tables
            .namespace_prepares
            .contains_key(&prepare.operation_id)
    );
    assert!(
        !replica
            .tables
            .namespace_transactions
            .contains_key(&prepare.transaction.transaction_id)
    );
    assert!(
        replica
            .tables
            .namespace_prepare_receipts
            .contains_key(&prepare.operation_id)
    );
    assert!(
        replica
            .tables
            .namespace_transaction_receipts
            .contains_key(&prepare.transaction.transaction_id)
    );

    drop(replica);
    let replica = LocalSobjMetadataBackend::new(shard_id, &replica_root).expect("reopened replica");
    assert_eq!(
        replica
            .get_object(&current.object_id)
            .expect("reopened compact commit object"),
        Some(committed)
    );
    assert!(
        !replica
            .tables
            .namespace_prepares
            .contains_key(&prepare.operation_id)
    );
    assert!(
        replica
            .tables
            .namespace_prepare_receipts
            .contains_key(&prepare.operation_id)
    );
    cleanup(&root);
}

#[test]
fn compact_namespace_commit_mutation_stays_bounded_for_4096_objects() {
    let shard_id = DfsMetadataShardId::new("shard-root");
    let current = object(version("version_compact_commit_size_base"));
    let mut prepare = namespace_prepare(
        "operation-compact-commit-size",
        &shard_id.0,
        &current,
        current.clone(),
    );
    prepare.expected_object_versions.clear();
    prepare.expected_path_occupants.clear();
    prepare.transaction.objects.clear();
    for index in 0..4096 {
        let mut object = current.clone();
        object.object_id =
            DfsObjectId::from_legacy_named(format!("obj_compact_commit_size_{index}"))
                .expect("object id");
        object.version =
            DfsVersion::from_legacy_named(format!("version_compact_commit_size_{index}"))
                .expect("version");
        object.path = format!("/services/compact-commit-size/{index}");
        prepare
            .expected_object_versions
            .push(DfsNamespaceObjectVersionExpectation {
                object_id: object.object_id.clone(),
                expected_version: Some(current.version.clone()),
            });
        prepare.transaction.objects.push(object);
    }
    let mut receipt_only_object = current.clone();
    receipt_only_object.object_id =
        DfsObjectId::from_legacy_named("obj_compact_commit_receipt_only")
            .expect("receipt-only object id");
    receipt_only_object.version =
        DfsVersion::from_legacy_named("version_compact_commit_receipt_only")
            .expect("receipt-only version");
    prepare.content_receipts.push(replica_receipt_record(
        &receipt_only_object,
        receipt_only_object.version.clone(),
        OffsetDateTime::UNIX_EPOCH,
    ));
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("4096-object prepare digest");

    let full = DfsMetadataMutation::put_namespace_prepare(&prepare);
    let packed =
        DfsMetadataMutation::put_namespace_prepare_packed_v1(&prepare).expect("pack prepare");
    let compact = DfsMetadataMutation::commit_namespace_prepare(&prepare);
    assert_eq!(packed.affected_object_ids().len(), 4096);
    assert_eq!(compact.affected_object_ids().len(), 4097);
    assert!(
        compact
            .affected_object_ids()
            .contains(&receipt_only_object.object_id),
        "compact commits must retain independently valid receipt objects"
    );
    let full_bytes = serde_json::to_vec(&full).expect("encode full prepare");
    let compact_bytes = serde_json::to_vec(&compact).expect("encode compact commit");
    let full_wire_bytes =
        sinomo_object_serde::to_bytes(&full, 1, sinomo_core::object::LengthMode::U32)
            .expect("encode full prepare wire mutation");
    let packed_wire_bytes =
        sinomo_object_serde::to_bytes(&packed, 1, sinomo_core::object::LengthMode::U32)
            .expect("encode packed prepare wire mutation");
    assert!(
        packed_wire_bytes.len().saturating_mul(4) < full_wire_bytes.len().saturating_mul(3),
        "packed prepare should save at least 25%: full={} packed={}",
        full_wire_bytes.len(),
        packed_wire_bytes.len()
    );
    assert!(
        compact_bytes.len() < 512 * 1024,
        "4096-object compact marker unexpectedly exceeds 512 KiB: {}",
        compact_bytes.len()
    );
    assert!(
        compact_bytes.len().saturating_mul(4) < full_bytes.len(),
        "compact marker should be at least 4x smaller: full={} compact={}",
        full_bytes.len(),
        compact_bytes.len()
    );
    assert_eq!(
        serde_json::from_slice::<DfsMetadataMutation>(&compact_bytes)
            .expect("round-trip compact commit"),
        compact
    );

    let entry = DfsMetadataJournalEntry {
        shard_id,
        sequence: 1,
        created_at: OffsetDateTime::UNIX_EPOCH,
        mutation: full,
    };
    let full_table_batch = DfsMetadataTableBatch {
        sequence: entry.sequence,
        ops: vec![DfsMetadataTableOp::put_namespace_prepare(prepare.clone())],
        prefix_proof: None,
        journal_reference_replay_cost_bytes: 0,
    };
    let referenced_table_batch = DfsMetadataTableBatch {
        sequence: entry.sequence,
        ops: namespace_prepare_persisted_table_ops(&entry, &full_table_batch.ops)
            .expect("journal-referenced prepare table operation"),
        prefix_proof: None,
        journal_reference_replay_cost_bytes: 0,
    };
    assert!(matches!(
        referenced_table_batch.ops.as_slice(),
        [DfsMetadataTableOp::PutNamespacePrepareJournalReference { .. }]
    ));
    let full_table_bytes =
        sinomo_object_serde::to_bytes(&full_table_batch, 1, sinomo_core::object::LengthMode::U32)
            .expect("encode full prepare table batch");
    let referenced_table_bytes = sinomo_object_serde::to_bytes(
        &referenced_table_batch,
        1,
        sinomo_core::object::LengthMode::U32,
    )
    .expect("encode referenced prepare table batch");
    assert!(
        referenced_table_bytes.len().saturating_mul(100) < full_table_bytes.len(),
        "journal-referenced prepare table batch should be at least 100x smaller: full={} referenced={}",
        full_table_bytes.len(),
        referenced_table_bytes.len()
    );
}

#[test]
fn sharded_global_receipt_scan_keeps_indexless_pending_create_receipts() {
    let root = temp_root("sharded_global_receipts_pending_create");
    let services_shard_id = DfsMetadataShardId::new("services");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                &services_shard_id.0,
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let file_system = file_system();
    backend.put_file_system(&file_system).expect("filesystem");

    let mut pending_object = object(version("version_pending_base"));
    pending_object.object_id =
        DfsObjectId::from_legacy_named("obj_pending_create_receipt_scan").expect("object");
    pending_object.path = "/services/pending-create.txt".to_string();
    let target_version = version("version_pending_target");
    let intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "intent-pending-create-receipt-scan".to_string(),
        file_system_id: pending_object.file_system_id.clone(),
        object_id: pending_object.object_id.clone(),
        base_version: None,
        target_version: target_version.clone(),
        coordinator_node_id: node("node_owner"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 1,
        desired_replica_node_ids: vec![node("node_owner")],
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: Some("blake3:pending-create".to_string()),
        object_metadata: Some(pending_object.clone()),
        touch_parent_on_create: true,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: None,
    };
    let receipt = DfsReplicaReceiptRecord {
        receipt: DfsWriteReceipt {
            file_system_id: pending_object.file_system_id.clone(),
            object_id: pending_object.object_id.clone(),
            committed_version: target_version,
            owner_node_id: node("node_owner"),
            event_id: EventId::generate(),
        },
        replica_node_id: node("node_owner"),
        content_hash: intent.content_hash.clone(),
        received_at: OffsetDateTime::UNIX_EPOCH,
    };
    backend
        .put_intent_and_receipts_for_object(
            Some(&pending_object),
            &intent,
            std::slice::from_ref(&receipt),
        )
        .expect("pending create intent and receipt");
    assert!(
        backend
            .get_object(&pending_object.object_id)
            .expect("object lookup")
            .is_none(),
        "pending create must not be visible before receipt recovery commits it"
    );

    let index_path = backend.object_shard_index_path(&pending_object.object_id);
    assert!(index_path.exists(), "writer should leave a routing index");
    std::fs::remove_file(index_path).expect("model an index-less metadata replica");

    let mut misrouted_object = pending_object.clone();
    misrouted_object.object_id =
        DfsObjectId::from_legacy_named("obj_misrouted_pending_receipt").expect("object");
    misrouted_object.path = "/misrouted-pending-create.txt".to_string();
    let misrouted_version = version("version_misrouted_pending");
    let misrouted_intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "intent-misrouted-pending-receipt".to_string(),
        file_system_id: misrouted_object.file_system_id.clone(),
        object_id: misrouted_object.object_id.clone(),
        base_version: None,
        target_version: misrouted_version.clone(),
        coordinator_node_id: node("node_owner"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 1,
        desired_replica_node_ids: vec![node("node_owner")],
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: Some("blake3:misrouted-pending".to_string()),
        object_metadata: Some(misrouted_object.clone()),
        touch_parent_on_create: true,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: None,
    };
    let misrouted_receipt = DfsReplicaReceiptRecord {
        receipt: DfsWriteReceipt {
            file_system_id: misrouted_object.file_system_id.clone(),
            object_id: misrouted_object.object_id.clone(),
            committed_version: misrouted_version,
            owner_node_id: node("node_owner"),
            event_id: EventId::generate(),
        },
        replica_node_id: node("node_owner"),
        content_hash: misrouted_intent.content_hash.clone(),
        received_at: OffsetDateTime::UNIX_EPOCH,
    };
    {
        let services = backend
            .shard_backend_mut(&services_shard_id)
            .expect("services shard");
        services
            .put_intent(&misrouted_intent)
            .expect("misrouted intent fixture");
        services
            .put_receipt(&misrouted_receipt)
            .expect("misrouted receipt fixture");
    }

    let page = backend
        .receipt_records_page(None, 8)
        .expect("global receipt scan");
    assert_eq!(page.receipts, vec![receipt]);
    assert!(page.next_cursor.is_none());

    cleanup(&root);
}

#[test]
fn local_backend_open_for_inspection_does_not_repair_materialization_cursor() {
    let root = temp_root("local_backend_open_for_inspection_is_read_only");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    let object = object(version("version_1"));
    backend.put_object(&object).expect("object");

    let cursor_path = backend.local_materialization_cursor_path();
    remove_file_if_exists(&cursor_path).expect("remove materialization cursor");
    assert!(
        !cursor_path.exists(),
        "test fixture should remove the recovery cursor before inspection open"
    );
    drop(backend);

    let inspected =
        LocalSobjMetadataBackend::open_for_inspection(shard_id, &root).expect("inspection open");
    assert!(
        !cursor_path.exists(),
        "inspection open must not repair or recreate recovery cursors"
    );
    let summary = inspected.metadata_summary().expect("summary");
    assert_eq!(summary.object_count, 1);
    assert!(
        inspected
            .metadata_shard_statuses()
            .expect("shard status")
            .first()
            .is_some_and(|status| status.journal_next_sequence >= 1)
    );

    cleanup(&root);
}

#[test]
fn local_backend_pages_object_versions_receipts_and_intents() {
    let root = temp_root("local_backend_pages_object_local_records");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let file = object(version("version_0"));
    backend.put_object(&file).expect("object");

    for index in 0..5 {
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

    let mut version_cursor = None;
    let mut paged_versions = Vec::new();
    loop {
        let page = backend
            .versions_for_object_page(&file.object_id, version_cursor.as_ref(), 2)
            .expect("version page");
        assert!(page.versions.len() <= 2);
        paged_versions.extend(page.versions.into_iter().map(|version| version.version));
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        version_cursor = Some(next_cursor);
    }
    assert_eq!(
        paged_versions,
        backend
            .versions_for_object(&file.object_id)
            .expect("full versions")
            .into_iter()
            .map(|version| version.version)
            .collect::<Vec<_>>()
    );
    let mut version_scan_cursor = None;
    let mut scanned_versions = Vec::new();
    loop {
        let page = backend
            .version_records_page(version_scan_cursor.as_ref(), 2)
            .expect("version scan page");
        assert!(page.versions.len() <= 2);
        scanned_versions.extend(page.versions.into_iter().map(|version| version.version));
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        version_scan_cursor = Some(next_cursor);
    }
    assert_eq!(scanned_versions, paged_versions);

    for index in 0..5 {
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

    let mut receipt_cursor = None;
    let mut paged_receipts = Vec::new();
    loop {
        let page = backend
            .receipts_for_version_page(
                &file.object_id,
                &version("version_0"),
                receipt_cursor.as_ref(),
                2,
            )
            .expect("receipt page");
        assert!(page.receipts.len() <= 2);
        paged_receipts.extend(
            page.receipts
                .into_iter()
                .map(|receipt| receipt.replica_node_id),
        );
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        receipt_cursor = Some(next_cursor);
    }
    assert_eq!(
        paged_receipts,
        backend
            .receipts_for_version(&file.object_id, &version("version_0"))
            .expect("full receipts")
            .into_iter()
            .map(|receipt| receipt.replica_node_id)
            .collect::<Vec<_>>()
    );
    let mut receipt_scan_cursor = None;
    let mut scanned_receipts = Vec::new();
    loop {
        let page = backend
            .receipt_records_page(receipt_scan_cursor.as_ref(), 2)
            .expect("receipt scan page");
        assert!(page.receipts.len() <= 2);
        scanned_receipts.extend(
            page.receipts
                .into_iter()
                .map(|receipt| receipt.replica_node_id),
        );
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        receipt_scan_cursor = Some(next_cursor);
    }
    assert_eq!(scanned_receipts, paged_receipts);

    for index in 0..5 {
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

    let mut intent_cursor = None;
    let mut paged_intents = Vec::new();
    loop {
        let page = backend
            .intents_for_object_page(&file.object_id, intent_cursor.as_ref(), 2)
            .expect("intent page");
        assert!(page.intents.len() <= 2);
        paged_intents.extend(page.intents.into_iter().map(|intent| intent.intent_id));
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        intent_cursor = Some(next_cursor);
    }
    assert_eq!(
        paged_intents,
        (0..5)
            .map(|index| format!("intent-{index}"))
            .collect::<Vec<_>>()
    );

    let mut scan_cursor = None;
    let mut scanned_intents = Vec::new();
    loop {
        let page = backend
            .write_intents_page(scan_cursor.as_ref(), 2)
            .expect("intent scan page");
        assert!(page.intents.len() <= 2);
        scanned_intents.extend(page.intents.into_iter().map(|intent| intent.intent_id));
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        scan_cursor = Some(next_cursor);
    }
    assert_eq!(
        scanned_intents,
        (0..5)
            .map(|index| format!("intent-{index}"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        backend
            .all_intents()
            .expect("all intents")
            .into_iter()
            .map(|intent| intent.intent_id)
            .collect::<Vec<_>>(),
        scanned_intents
    );

    cleanup(&root);
}

#[test]
fn local_backend_compacts_retention_eligible_tombstones_to_path_markers() {
    let root = temp_root("local_backend_compacts_tombstones_to_markers");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    let mut tombstone = object(version("version_2"));
    tombstone.path = "/docs/deleted.md".to_string();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.content_hash = None;
    tombstone.content_size_bytes = None;
    tombstone.changed_at_unix_nanos = 10;
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&tombstone).expect("tombstone");

    let dry_run = backend
        .compact_tombstones(
            DfsTombstoneCompactionOptions::dry_run(Duration::from_nanos(50))
                .with_now_unix_nanos(100),
        )
        .expect("dry run");
    assert_eq!(dry_run.scanned_tombstone_count, 1);
    assert_eq!(dry_run.eligible_tombstone_count, 1);
    assert_eq!(dry_run.compacted_tombstone_count, 0);
    assert_eq!(
        backend
            .get_object(&tombstone.object_id)
            .expect("dry-run object"),
        Some(tombstone.clone())
    );

    let report = backend
        .compact_tombstones(
            DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(50))
                .with_now_unix_nanos(100),
        )
        .expect("compact");
    assert_eq!(report.scanned_tombstone_count, 1);
    assert_eq!(report.eligible_tombstone_count, 1);
    assert_eq!(report.compacted_tombstone_count, 1);
    assert_eq!(report.removed_object_record_count, 1);
    assert_eq!(report.removed_inode_record_count, 1);
    assert_eq!(report.removed_file_system_object_record_count, 1);
    assert_eq!(report.preserved_path_marker_count, 1);
    assert!(table_path_exists(
        &backend,
        &file_system.file_system_id,
        "/docs/deleted.md"
    ));
    assert_eq!(
        backend
            .get_object(&tombstone.object_id)
            .expect("compacted object"),
        None
    );
    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, "/docs/deleted.md")
            .expect("path lookup")
            .expect("compacted marker")
            .kind,
        DfsObjectKind::Tombstone
    );
    assert!(
        backend
            .objects_for_file_system(&file_system.file_system_id)
            .expect("objects")
            .is_empty(),
        "compacted tombstones should not hydrate through object scans"
    );
    reset_metadata_current_tables_snapshot_calls();
    let compacted = backend
        .compacted_tombstones_for_object_ids(&BTreeSet::from([
            tombstone.object_id.clone(),
            DfsObjectId::from_legacy_named("obj_unrelated_compacted_lookup")
                .expect("unrelated object id"),
        ]))
        .expect("bounded compacted evidence");
    assert_eq!(compacted, vec![tombstone.clone()]);
    assert_eq!(
        metadata_current_tables_snapshot_calls(),
        1,
        "a bounded evidence set must be joined against one shard snapshot"
    );

    drop(backend);
    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopen");
    assert_eq!(
        reopened
            .get_object(&tombstone.object_id)
            .expect("reopened object"),
        None
    );
    assert_eq!(
        reopened
            .get_object_by_path(&file_system.file_system_id, "/docs/deleted.md")
            .expect("reopened path")
            .expect("compacted marker")
            .kind,
        DfsObjectKind::Tombstone
    );
    assert!(
        reopened
            .objects_for_file_system(&file_system.file_system_id)
            .expect("reopened objects")
            .is_empty()
    );

    cleanup(&root);
}

#[test]
fn local_backend_recreating_compacted_tombstone_path_removes_marker() {
    let root = temp_root("local_backend_recreate_compacted_tombstone_path");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    let mut tombstone = object(version("version_2"));
    tombstone.path = "/docs/recreate.md".to_string();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.content_hash = None;
    tombstone.changed_at_unix_nanos = 1;
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&tombstone).expect("tombstone");
    backend
        .compact_tombstones(
            DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(0))
                .with_now_unix_nanos(100),
        )
        .expect("compact");

    let mut replacement = object(version("version_1"));
    replacement.object_id =
        DfsObjectId::from_legacy_named("obj_recreated_path").expect("object id");
    replacement.path = tombstone.path.clone();
    replacement.kind = DfsObjectKind::File;
    replacement.content_hash = Some("blake3:new".to_string());
    backend.put_object(&replacement).expect("replacement");

    assert!(backend.tables.compacted_tombstones.is_empty());
    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, &replacement.path)
            .expect("path lookup"),
        Some(replacement.clone())
    );
    assert_eq!(
        backend
            .objects_for_file_system(&file_system.file_system_id)
            .expect("objects")
            .into_iter()
            .map(|object| object.object_id)
            .collect::<Vec<_>>(),
        vec![replacement.object_id]
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_compacts_tombstone_and_removes_object_index() {
    let root = temp_root("sharded_backend_compacts_tombstone");
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
    let mut tombstone = object(version("version_2"));
    tombstone.path = "/services/deleted.md".to_string();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.content_hash = None;
    tombstone.changed_at_unix_nanos = 1;
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&tombstone).expect("tombstone");
    assert!(
        backend
            .object_shard_index_path(&tombstone.object_id)
            .is_file()
    );

    let report = backend
        .compact_tombstones(
            DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(0))
                .with_now_unix_nanos(100),
        )
        .expect("compact");
    assert_eq!(report.compacted_tombstone_count, 1);
    assert!(
        !backend
            .object_shard_index_path(&tombstone.object_id)
            .exists(),
        "compaction should remove stale object shard routing indexes"
    );
    assert_eq!(
        backend
            .get_object(&tombstone.object_id)
            .expect("object lookup"),
        None
    );
    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, "/services/deleted.md")
            .expect("path lookup")
            .expect("compacted marker")
            .kind,
        DfsObjectKind::Tombstone
    );

    cleanup(&root);
}

#[test]
fn node_aware_sharded_maintenance_excludes_local_replica_shards() {
    let root = temp_root("node_aware_sharded_maintenance_primary_only");
    let local_node_id = node("node_gc_local_primary");
    let remote_node_id = node("node_gc_remote_primary");
    let root_shard_id = DfsMetadataShardId::new("root");
    let services_shard_id = DfsMetadataShardId::new("services");
    let mut root_config = shard_config("root", DfsMetadataShardAssignment::Root);
    root_config.primary_node_id = local_node_id.clone();
    root_config.replica_node_ids = vec![remote_node_id.clone()];
    let mut services_config = shard_config(
        "services",
        DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        },
    );
    services_config.primary_node_id = remote_node_id;
    services_config.replica_node_ids = vec![local_node_id.clone()];
    let configs = vec![root_config, services_config];

    let mut primary_tombstone = object(version("version_gc_primary_tombstone"));
    primary_tombstone.path = "/deleted-primary.md".to_string();
    primary_tombstone.kind = DfsObjectKind::Tombstone;
    primary_tombstone.content_hash = None;
    primary_tombstone.content_size_bytes = None;
    primary_tombstone.changed_at_unix_nanos = 1;
    let mut replica_tombstone = object(version("version_gc_replica_tombstone"));
    replica_tombstone.object_id =
        DfsObjectId::from_legacy_named("obj_gc_replica_tombstone").expect("object id");
    replica_tombstone.path = "/services/deleted-replica.md".to_string();
    replica_tombstone.kind = DfsObjectKind::Tombstone;
    replica_tombstone.content_hash = None;
    replica_tombstone.content_size_bytes = None;
    replica_tombstone.changed_at_unix_nanos = 1;

    {
        let mut seed =
            LocalShardedMetadataBackend::new(&root, configs.clone()).expect("seed sharded backend");
        seed.put_file_system(&file_system()).expect("filesystem");
        seed.put_object(&primary_tombstone)
            .expect("primary tombstone");
        seed.put_object(&replica_tombstone)
            .expect("replica tombstone");
    }

    let inspection = LocalShardedMetadataBackend::open_for_inspection_at_node(
        &root,
        configs.clone(),
        &local_node_id,
    )
    .expect("node-aware inspection");
    let tombstone_projection = inspection
        .tombstone_compaction_projection(
            DfsTombstoneCompactionOptions::dry_run(Duration::ZERO).with_now_unix_nanos(100),
        )
        .expect("tombstone projection");
    assert_eq!(tombstone_projection.scanned_tombstone_count, 1);
    assert_eq!(tombstone_projection.eligible_tombstone_count, 1);
    let history_projection = inspection
        .metadata_history_gc_projection(
            DfsMetadataHistoryGcOptions::dry_run(Duration::ZERO, 1).with_now_unix_nanos(100),
        )
        .expect("metadata history projection");
    assert_eq!(
        history_projection
            .shards
            .iter()
            .map(|stats| (stats.shard_id.clone(), stats.collection_authorized))
            .collect::<Vec<_>>(),
        vec![
            (root_shard_id.clone(), true),
            (services_shard_id.clone(), false),
        ],
        "inspection must inventory local replicas without authorizing maintenance work"
    );
    drop(inspection);

    let mut maintenance =
        LocalShardedMetadataBackend::new_with_replica_repair(&root, configs, &local_node_id)
            .expect("node-aware maintenance backend");
    let replica_journal_head = maintenance
        .shard_backend(&services_shard_id)
        .expect("replica shard")
        .journal_last_sequence();

    let report = maintenance
        .compact_tombstones(
            DfsTombstoneCompactionOptions::compact_eligible(Duration::ZERO)
                .with_now_unix_nanos(100),
        )
        .expect("primary-only tombstone compaction");
    assert_eq!(report.compacted_tombstone_count, 1);
    assert_eq!(
        maintenance
            .shard_backend(&root_shard_id)
            .expect("primary shard")
            .get_object(&primary_tombstone.object_id)
            .expect("primary object lookup"),
        None
    );
    assert_eq!(
        maintenance
            .shard_backend(&services_shard_id)
            .expect("replica shard")
            .get_object(&replica_tombstone.object_id)
            .expect("replica object lookup"),
        Some(replica_tombstone),
        "compaction must leave local replica metadata untouched"
    );
    assert_eq!(
        maintenance
            .shard_backend(&services_shard_id)
            .expect("replica shard")
            .journal_last_sequence(),
        replica_journal_head,
        "local replica journal must not advance during maintenance"
    );

    cleanup(&root);
}

#[test]
fn commit_visible_version_requires_expected_version_and_receipts() {
    let root = temp_root("commit_visible_version_requires_expected_version_and_receipts");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    let initial = object(version("version_1"));
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&initial).expect("object");

    let mut committed = initial.clone();
    committed.version = version("version_2");
    committed.content_hash = Some("blake3:new".to_string());
    committed.current_locations.push(node("node_replica"));
    let version_record = DfsObjectVersionRecord {
        content_durability: None,
        metadata_format: crate::DfsRevisionMetadataFormat::Legacy,
        retention: Default::default(),
        module_metadata: Default::default(),
        file_system_id: committed.file_system_id.clone(),
        object_id: committed.object_id.clone(),
        version: committed.version.clone(),
        content_hash: committed.content_hash.clone(),
        size_bytes: Some(3),
        locations: committed.current_locations.clone(),
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    let receipt = DfsReplicaReceiptRecord {
        receipt: DfsWriteReceipt {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            committed_version: committed.version.clone(),
            owner_node_id: node("node_owner"),
            event_id: EventId::from_legacy_named("evt_commit").expect("event"),
        },
        replica_node_id: node("node_replica"),
        content_hash: committed.content_hash.clone(),
        received_at: OffsetDateTime::UNIX_EPOCH,
    };

    assert!(matches!(
        backend.commit_visible_version(
            &initial.version,
            &committed,
            &version_record,
            std::slice::from_ref(&receipt.replica_node_id),
        ),
        Err(MetadataBackendError::Conflict { .. })
    ));

    backend.put_receipt(&receipt).expect("receipt");
    let table_batches_before_commit = metadata_table_batch_count(&root);
    let commit = backend
        .commit_visible_version(
            &initial.version,
            &committed,
            &version_record,
            std::slice::from_ref(&receipt.replica_node_id),
        )
        .expect("commit");
    assert!(matches!(
        commit.mutation,
        DfsMetadataMutation::CommitVisibleVersion { .. }
    ));
    assert_eq!(
        metadata_table_batch_count(&root),
        table_batches_before_commit + 1,
        "one visible-version journal entry should materialize into one table batch"
    );
    assert_eq!(
        backend
            .get_object(&committed.object_id)
            .expect("get object")
            .expect("object")
            .version,
        committed.version
    );
    assert_eq!(
        backend
            .get_version(&committed.object_id, &committed.version)
            .expect("get version"),
        Some(version_record)
    );
    assert_eq!(
        backend
            .receipts_for_version(&committed.object_id, &committed.version)
            .expect("receipts")
            .len(),
        1
    );

    cleanup(&root);
}

#[test]
fn local_backend_batches_write_intent_and_receipts() {
    let root = temp_root("local_backend_batches_intent_receipts");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system();
    let file = object(version("version_0"));
    let intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "intent-batched".to_string(),
        file_system_id: file.file_system_id.clone(),
        object_id: file.object_id.clone(),
        base_version: Some(file.version.clone()),
        target_version: version("version_1"),
        coordinator_node_id: node("node_owner"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 1,
        desired_replica_node_ids: vec![node("node_owner"), node("node_remote")],
        policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: Some("blake3:batched".to_string()),
        object_metadata: Some(file.clone()),
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: Some(OffsetDateTime::UNIX_EPOCH),
    };
    let receipts = ["node_owner", "node_remote"]
        .into_iter()
        .map(|replica| DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                committed_version: intent.target_version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named(format!("evt_batch_{replica}"))
                    .expect("event"),
            },
            replica_node_id: node(replica),
            content_hash: intent.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        })
        .collect::<Vec<_>>();

    {
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&file).expect("object");
        let entries = backend
            .put_intent_and_receipts_for_object(Some(&file), &intent, &receipts)
            .expect("batched intent receipts");
        assert_eq!(entries.len(), 3);
        assert_eq!(
            backend
                .get_intent(&file.object_id, &intent.intent_id)
                .expect("intent"),
            Some(intent.clone())
        );
        assert_eq!(
            backend
                .receipts_for_version(&file.object_id, &intent.target_version)
                .expect("receipts")
                .len(),
            2
        );
    }

    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");
    assert_eq!(
        reopened
            .get_intent(&file.object_id, &intent.intent_id)
            .expect("reopened intent"),
        Some(intent)
    );
    assert_eq!(
        reopened
            .receipts_for_version(&file.object_id, &version("version_1"))
            .expect("reopened receipts")
            .len(),
        2
    );

    cleanup(&root);
}

#[test]
fn metadata_history_gc_compacts_then_evicts_intent_receipt_with_durable_floor() {
    let root = temp_root("metadata-history-gc-intent-floor");
    let shard_id = DfsMetadataShardId::new("root");
    let coordinator_node_id = node("node_intent_gc_coordinator");
    let intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "intent_gc_42".to_string(),
        file_system_id: file_system_id(),
        object_id: object_id(),
        base_version: Some(version("version_intent_gc_base")),
        target_version: version("version_intent_gc_target"),
        coordinator_node_id: coordinator_node_id.clone(),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 7,
        desired_replica_node_ids: vec![node("node_owner")],
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        status: DfsWriteIntentStatus::Aborted,
        content_hash: Some("blake3:intent-gc".to_string()),
        object_metadata: None,
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: Some(OffsetDateTime::UNIX_EPOCH),
    };
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_intent(&intent).expect("terminal intent");

    let compacted = backend
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(1)
                .with_max_removals(1),
        )
        .expect("compact terminal intent");
    assert_eq!(compacted.compacted_intent_count, 1);
    assert!(
        !backend
            .tables
            .intents
            .contains_key(&(intent.object_id.clone(), intent.intent_id.clone()))
    );
    assert!(
        backend
            .tables
            .intent_receipts
            .contains_key(&(intent.object_id.clone(), intent.intent_id.clone()))
    );
    assert_eq!(
        backend
            .get_intent(&intent.object_id, &intent.intent_id)
            .expect("compacted intent lookup"),
        Some(intent.clone())
    );
    let retry = backend
        .put_intent(&intent)
        .expect("exact compacted intent retry");
    assert!(matches!(
        retry.mutation,
        DfsMetadataMutation::PutIntentReceipt { .. }
    ));

    let evicted = backend
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(2)
                .with_max_removals(1),
        )
        .expect("evict compact intent receipt");
    assert_eq!(evicted.removed_intent_receipt_count, 1);
    assert_eq!(
        backend
            .write_intent_counter_floor()
            .expect("intent counter floor"),
        42
    );
    assert!(
        backend
            .put_intent(&intent)
            .expect_err("intent retry after receipt eviction must fail closed")
            .to_string()
            .contains("history expired")
    );
    drop(backend);

    let mut reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen backend");
    assert_eq!(
        reopened
            .write_intent_counter_floor()
            .expect("reopened intent counter floor"),
        42
    );
    assert!(
        reopened
            .put_intent(&intent)
            .expect_err("reopened intent retry after eviction must fail closed")
            .to_string()
            .contains("history expired")
    );

    cleanup(&root);
}

#[test]
fn metadata_history_gc_compacts_then_evicts_request_receipt_with_durable_floor() {
    let root = temp_root("metadata-history-gc-request-floor");
    let shard_id = DfsMetadataShardId::new("root");
    let mut request = namespace_request(93, DfsNamespaceRequestStatus::Completed);
    request.request_id = "namespace-request_01J0000000000000000000000Z".to_string();
    request.created_at = OffsetDateTime::UNIX_EPOCH;
    request.updated_at = OffsetDateTime::UNIX_EPOCH;
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend
        .put_namespace_request_on_shard(&shard_id, &request)
        .expect("completed request");

    let compacted = backend
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(1)
                .with_max_removals(1),
        )
        .expect("compact request body");
    assert_eq!(compacted.compacted_namespace_request_count, 1);
    assert!(
        backend
            .get_namespace_request(&request.requester_node_id, &request.request_id)
            .expect("full request lookup")
            .is_none()
    );
    assert!(
        backend
            .get_namespace_request_receipt(&request.requester_node_id, &request.request_id)
            .expect("request receipt lookup")
            .is_some()
    );
    let retry = backend
        .put_namespace_request_on_shard(&shard_id, &request)
        .expect("exact compacted request retry");
    assert!(matches!(
        retry.mutation,
        DfsMetadataMutation::PutNamespaceRequestReceipt { .. }
    ));

    let evicted = backend
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(2)
                .with_max_removals(1),
        )
        .expect("evict compact request receipt");
    assert_eq!(evicted.removed_namespace_request_receipt_count, 1);
    assert!(
        backend
            .get_namespace_request_receipt(&request.requester_node_id, &request.request_id)
            .expect("evicted receipt lookup")
            .is_none()
    );
    assert!(
        backend
            .namespace_request_history_expired_on_shard(
                &shard_id,
                &request.requester_node_id,
                &request.request_id,
            )
            .expect("request retention floor")
    );
    assert!(
        backend
            .put_namespace_request_on_shard(&shard_id, &request)
            .expect_err("retry after receipt eviction must fail closed")
            .to_string()
            .contains("history expired")
    );
    drop(backend);

    let mut reopened =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopen backend");
    assert!(
        reopened
            .namespace_request_history_expired_on_shard(
                &shard_id,
                &request.requester_node_id,
                &request.request_id,
            )
            .expect("reopened request retention floor")
    );
    assert!(
        reopened
            .put_namespace_request_on_shard(&shard_id, &request)
            .expect_err("reopened retry after eviction must fail closed")
            .to_string()
            .contains("history expired")
    );

    let mut delayed_unrelated = namespace_request(94, DfsNamespaceRequestStatus::Pending);
    delayed_unrelated.request_id = "namespace-request_01J00000000000000000000000".to_string();
    assert_ne!(
        delayed_unrelated.requester_node_id,
        request.requester_node_id
    );
    assert!(
        !reopened
            .namespace_request_history_expired_on_shard(
                &shard_id,
                &delayed_unrelated.requester_node_id,
                &delayed_unrelated.request_id,
            )
            .expect("unrelated requester retention floor"),
        "a higher durable request floor for one requester must not poison a delayed lower request from another requester"
    );
    reopened
        .put_namespace_request_on_shard(&shard_id, &delayed_unrelated)
        .expect("admit delayed request from unrelated requester");

    cleanup(&root);
}

#[test]
fn metadata_history_gc_compacts_then_evicts_transaction_receipt_with_durable_floor() {
    let root = temp_root("metadata-history-gc-transaction-floor");
    let shard_id = DfsMetadataShardId::new("root");
    let transaction = DfsNamespaceTransaction {
        content_reference_identity: None,
        contribution_digest: None,
        extension_expectations: Vec::new(),
        file_version_pins: None,
        transaction_id: "transaction:metadata-history-gc-floor".to_string(),
        file_system_id: file_system_id(),
        objects: Vec::new(),
        versions: Vec::new(),
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend
        .apply_namespace_transaction(&transaction)
        .expect("completed namespace transaction");

    let compacted = backend
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(1)
                .with_max_removals(1),
        )
        .expect("compact transaction body");
    assert_eq!(compacted.compacted_namespace_transaction_count, 1);
    assert!(
        backend
            .get_namespace_transaction(&transaction.transaction_id)
            .expect("full transaction lookup")
            .is_none()
    );
    assert!(
        backend
            .tables
            .namespace_transaction_receipts
            .contains_key(&transaction.transaction_id)
    );
    let retry = backend
        .apply_namespace_transaction(&transaction)
        .expect("exact compacted transaction retry");
    assert!(matches!(
        retry.mutation,
        DfsMetadataMutation::PutNamespaceTransactionReceipt { .. }
    ));

    let evicted = backend
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(2)
                .with_max_removals(1),
        )
        .expect("evict compact transaction receipt");
    assert_eq!(evicted.removed_namespace_transaction_receipt_count, 1);
    assert!(
        !backend
            .tables
            .namespace_transaction_receipts
            .contains_key(&transaction.transaction_id)
    );
    assert!(
        backend
            .tables
            .namespace_receipt_retention
            .transaction_floor
            .is_some()
    );
    assert!(
        backend
            .apply_namespace_transaction(&transaction)
            .expect_err("transaction retry after receipt eviction must fail closed")
            .to_string()
            .contains("history expired")
    );
    drop(backend);

    let mut reopened =
        LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen transaction floor");
    assert!(
        reopened
            .tables
            .namespace_receipt_retention
            .transaction_floor
            .is_some()
    );
    assert!(
        reopened
            .apply_namespace_transaction(&transaction)
            .expect_err("reopened transaction retry after eviction must fail closed")
            .to_string()
            .contains("history expired")
    );

    cleanup(&root);
}

#[test]
fn metadata_history_gc_compacts_then_evicts_prepare_receipt_with_durable_floor() {
    let root = temp_root("metadata-history-gc-prepare-floor");
    let shard_id = DfsMetadataShardId::new("root");
    let current = object(version("version_gc_prepare_floor_current"));
    let mut committed = current.clone();
    committed.version = version("version_gc_prepare_floor_committed");
    committed.mode = 0o600;
    let prepare = namespace_prepare(
        "namespace-prepare:metadata-history-gc-floor",
        "root",
        &current,
        committed,
    );
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("prepare namespace transaction");
    backend
        .abort_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("abort namespace prepare");

    let compacted = backend
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(1)
                .with_max_removals(1),
        )
        .expect("compact prepare body");
    assert_eq!(compacted.compacted_namespace_prepare_count, 1);
    assert!(
        backend
            .get_namespace_prepare(&prepare.operation_id)
            .expect("full prepare lookup")
            .is_none()
    );
    assert!(
        backend
            .tables
            .namespace_prepare_receipts
            .contains_key(&prepare.operation_id)
    );
    let retry = backend
        .abort_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("exact compacted abort retry");
    assert!(matches!(
        retry.mutation,
        DfsMetadataMutation::PutNamespacePrepareReceipt { .. }
    ));

    let evicted = backend
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(2)
                .with_max_removals(1),
        )
        .expect("evict compact prepare receipt");
    assert_eq!(evicted.removed_namespace_prepare_receipt_count, 1);
    assert!(
        !backend
            .tables
            .namespace_prepare_receipts
            .contains_key(&prepare.operation_id)
    );
    assert!(
        backend
            .tables
            .namespace_receipt_retention
            .prepare_floor
            .is_some()
    );
    assert!(
        backend
            .abort_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect_err("prepare retry after receipt eviction must fail closed")
            .to_string()
            .contains("history expired")
    );
    drop(backend);

    let mut reopened =
        LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen prepare floor");
    assert!(
        reopened
            .tables
            .namespace_receipt_retention
            .prepare_floor
            .is_some()
    );
    assert!(
        reopened
            .abort_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect_err("reopened prepare retry after eviction must fail closed")
            .to_string()
            .contains("history expired")
    );

    cleanup(&root);
}

#[test]
fn metadata_history_gc_evicts_immediate_prepare_receipts_and_preserves_floors() {
    let root = temp_root("metadata-history-gc-immediate-prepare-receipts");
    let shard_id = DfsMetadataShardId::new("root");
    let current = object(version("version_gc_prepare_request_current"));
    let mut committed = current.clone();
    committed.version = version("version_gc_prepare_request_committed");
    committed.mode = 0o600;
    committed.changed_at_unix_nanos = 2;
    let mut completed_request = namespace_request(94, DfsNamespaceRequestStatus::Completed);
    completed_request.authority_shard_id = shard_id.clone();
    completed_request.operation = DfsNamespaceOperation::SetMode {
        path: current.path.clone(),
        mode: committed.mode,
    };
    let mut prepare = namespace_prepare(
        &format!(
            "namespace-request:{}:metadata-history-gc",
            completed_request.operation_id
        ),
        &shard_id.0,
        &current,
        committed.clone(),
    );
    prepare.completed_request = Some(completed_request.clone());
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("canonical request-owned prepare digest");

    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("fused request admission and prepare");
    let commit_entry = backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("commit request-owned prepare");
    assert!(
        backend
            .get_namespace_prepare(&prepare.operation_id)
            .expect("committed prepare body")
            .is_none()
    );
    assert!(
        backend
            .get_namespace_transaction(&prepare.transaction.transaction_id)
            .expect("committed transaction body")
            .is_none()
    );
    assert!(
        backend
            .tables
            .namespace_prepare_receipts
            .contains_key(&prepare.operation_id)
    );
    assert!(
        backend
            .tables
            .namespace_transaction_receipts
            .contains_key(&prepare.transaction.transaction_id)
    );
    assert_eq!(
        backend
            .get_namespace_request(
                &completed_request.requester_node_id,
                &completed_request.request_id,
            )
            .expect("completed request"),
        Some(completed_request.clone())
    );
    backend
        .compact_metadata_table_log_if_needed(0)
        .expect("checkpoint compact terminal identities");
    drop(backend);

    let mut reopened = LocalSobjMetadataBackend::new(shard_id.clone(), &root)
        .expect("reopen compact terminal identities");
    assert_eq!(
        reopened
            .get_object(&committed.object_id)
            .expect("committed transaction object"),
        Some(committed)
    );
    assert!(
        reopened
            .tables
            .namespace_prepare_receipts
            .contains_key(&prepare.operation_id)
    );
    assert!(
        reopened
            .tables
            .namespace_transaction_receipts
            .contains_key(&prepare.transaction.transaction_id)
    );

    let now_unix_nanos = u64::try_from(commit_entry.created_at.unix_timestamp_nanos())
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut compacted_request = false;
    let mut removed_prepare_receipt = false;
    let mut removed_transaction_receipt = false;
    let mut removed_request_receipt = false;
    for _ in 0..32 {
        let report = reopened
            .gc_metadata_history_authoritative(
                DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                    .with_now_unix_nanos(now_unix_nanos)
                    .with_max_removals(1),
            )
            .expect("drain compact terminal history");
        compacted_request |= report.compacted_namespace_request_count != 0;
        removed_prepare_receipt |= report.removed_namespace_prepare_receipt_count != 0;
        removed_transaction_receipt |= report.removed_namespace_transaction_receipt_count != 0;
        removed_request_receipt |= report.removed_namespace_request_receipt_count != 0;
        if report.applied_record_count() == 0 {
            break;
        }
    }
    assert!(compacted_request);
    assert!(removed_prepare_receipt);
    assert!(removed_transaction_receipt);
    assert!(removed_request_receipt);
    assert!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("drained prepare")
            .is_none()
    );
    assert!(
        !reopened
            .tables
            .namespace_prepare_receipts
            .contains_key(&prepare.operation_id),
        "the compact prepare receipt must be bounded"
    );
    assert!(
        reopened
            .get_namespace_request(
                &completed_request.requester_node_id,
                &completed_request.request_id,
            )
            .expect("drained request body")
            .is_none()
    );
    assert!(
        reopened
            .get_namespace_request_receipt(
                &completed_request.requester_node_id,
                &completed_request.request_id,
            )
            .expect("drained request receipt")
            .is_none()
    );
    drop(reopened);

    let mut reopened =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopen drained floors");
    assert!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("reopened drained prepare")
            .is_none()
    );
    assert!(
        reopened
            .get_namespace_request(
                &completed_request.requester_node_id,
                &completed_request.request_id,
            )
            .expect("reopened drained request")
            .is_none()
    );
    assert!(
        reopened
            .put_namespace_request_on_shard(&shard_id, &completed_request)
            .expect_err("evicted request retry must fail closed")
            .to_string()
            .contains("history expired")
    );
    assert!(
        reopened
            .commit_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect_err("evicted prepare retry must fail closed")
            .to_string()
            .contains("history expired")
    );
    assert!(
        reopened
            .apply_namespace_transaction(&prepare.transaction)
            .expect_err("evicted transaction retry must fail closed")
            .to_string()
            .contains("history expired")
    );

    cleanup(&root);
}

#[test]
fn metadata_history_gc_compacts_then_evicts_job_receipt_with_durable_floor() {
    let root = temp_root("metadata-history-gc-job-floor");
    let shard_id = DfsMetadataShardId::new("root");
    let mut job = namespace_job(93, "/users/job-floor");
    job.status = DfsNamespaceJobStatus::Completed;
    job.created_at = OffsetDateTime::UNIX_EPOCH;
    job.updated_at = OffsetDateTime::UNIX_EPOCH;
    job.completed_at = Some(OffsetDateTime::UNIX_EPOCH);

    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_namespace_job(&job).expect("terminal job");

    let compacted = backend
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(1)
                .with_max_removals(1),
        )
        .expect("compact job body");
    assert_eq!(compacted.compacted_namespace_job_count, 1);
    assert!(
        backend
            .get_namespace_job(&job.job_id)
            .expect("full job lookup")
            .is_none()
    );
    assert!(
        backend
            .get_namespace_job_receipt(&job.job_id)
            .expect("job receipt lookup")
            .is_some()
    );
    backend
        .put_namespace_job(&job)
        .expect("exact terminal retry uses compact receipt");

    let evicted = backend
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(2)
                .with_max_removals(1),
        )
        .expect("evict compact job receipt");
    assert_eq!(evicted.removed_namespace_job_receipt_count, 1);
    assert!(
        backend
            .get_namespace_job_receipt(&job.job_id)
            .expect("evicted receipt lookup")
            .is_none()
    );
    assert!(
        backend
            .put_namespace_job(&job)
            .expect_err("job retry after receipt eviction must fail closed")
            .to_string()
            .contains("history expired")
    );
    drop(backend);

    let mut reopened =
        LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen metadata backend");
    assert!(
        reopened
            .put_namespace_job(&job)
            .expect_err("reopened job retry after receipt eviction must fail closed")
            .to_string()
            .contains("history expired")
    );

    let later_job = namespace_job(94, "/users/later-job");
    reopened
        .put_namespace_job(&later_job)
        .expect("a later-created job remains admissible above the durable floor");

    cleanup(&root);
}
