//! Recovery regression tests.

use super::*;

#[test]
fn request_owned_prepare_fuses_admission_and_survives_reopen_before_commit() {
    let root = temp_root("local_backend_request_prepare_fused_admission");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let current = object(version("version_request_prepare_fused_1"));
    let mut committed = current.clone();
    committed.version = version("version_request_prepare_fused_2");
    committed.mode = 0o600;
    let admitted_at = OffsetDateTime::UNIX_EPOCH;
    let completed_at = OffsetDateTime::from_unix_timestamp(1).expect("completion timestamp");
    let completed_request = DfsNamespaceRequestRecord {
        file_system_id: current.file_system_id.clone(),
        requester_node_id: node("node_request_prepare_fused_requester"),
        request_id: "request-prepare-fused".to_string(),
        operation_id: "req-request-prepare-fused".to_string(),
        authority_shard_id: shard_id.clone(),
        operation: DfsNamespaceOperation::SetMode {
            path: current.path.clone(),
            mode: committed.mode,
        },
        command_digest: "blake3:request-prepare-fused".to_string(),
        status: DfsNamespaceRequestStatus::Completed,
        result: Some(DfsNamespaceResult::Applied),
        posix_coordination_result: None,
        recovery_attempts: 0,
        last_recovery_error: None,
        created_at: admitted_at,
        updated_at: completed_at,
    };
    let pending_request = pending_namespace_request_admission_from_completion(&completed_request);
    let mut prepare = namespace_prepare(
        &format!(
            "namespace-request:{}:transaction-request-prepare-fused",
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

    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    assert!(
        backend
            .get_namespace_request(
                &completed_request.requester_node_id,
                &completed_request.request_id,
            )
            .expect("request before prepare")
            .is_none()
    );
    let sequence_before_prepare = backend.journal_last_sequence();

    backend
        .prepare_namespace_transaction(&prepare)
        .expect("fused request admission and hidden prepare");

    assert_eq!(
        backend.journal_last_sequence(),
        sequence_before_prepare.saturating_add(1),
        "admission and prepare must share one journal mutation"
    );
    assert_eq!(
        backend
            .get_namespace_request(
                &pending_request.requester_node_id,
                &pending_request.request_id,
            )
            .expect("derived pending request"),
        Some(pending_request.clone())
    );
    assert_eq!(
        backend
            .get_namespace_prepare(&prepare.operation_id)
            .expect("hidden prepare"),
        Some(prepare.clone())
    );
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("object remains hidden"),
        Some(current.clone())
    );

    // Rewind the materialized table so reopen must derive the Pending row
    // from the prepare journal payload, matching replica/replay behavior.
    force_table_replay_from_journal(&backend, &root, 0);
    drop(backend);
    let mut reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopened backend");
    assert_eq!(
        reopened
            .get_namespace_request(
                &pending_request.requester_node_id,
                &pending_request.request_id,
            )
            .expect("reopened pending request"),
        Some(pending_request.clone())
    );
    assert_eq!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("reopened hidden prepare"),
        Some(prepare.clone())
    );
    assert_eq!(
        reopened
            .get_object(&current.object_id)
            .expect("reopened hidden object"),
        Some(current.clone())
    );

    let mut conflicting_pending_request = pending_request.clone();
    conflicting_pending_request.command_digest =
        "blake3:conflicting-request-while-prepare-waits".to_string();
    reopened
        .tables
        .apply(DfsMetadataTableOp::put_namespace_request(
            conflicting_pending_request,
        ));
    let conflict = reopened
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect_err("commit must revalidate current request state");
    assert!(
        conflict.to_string().contains("completed request conflicts"),
        "unexpected request-state error: {conflict}"
    );
    reopened
        .tables
        .apply(DfsMetadataTableOp::put_namespace_request(pending_request));

    reopened
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("commit request-owned prepare");

    assert_eq!(
        reopened
            .get_namespace_request(
                &completed_request.requester_node_id,
                &completed_request.request_id,
            )
            .expect("completed request"),
        Some(completed_request)
    );
    assert_eq!(
        reopened
            .get_object(&current.object_id)
            .expect("committed object"),
        Some(committed)
    );
    assert!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("committed prepare")
            .is_none(),
        "the committed prepare body must be compacted immediately"
    );
    assert_eq!(
        reopened
            .tables
            .namespace_prepare_receipts
            .get(&prepare.operation_id)
            .map(|receipt| &receipt.status),
        Some(&DfsNamespacePrepareStatus::Committed)
    );
    assert!(
        reopened
            .tables
            .namespace_transaction_receipts
            .contains_key(&prepare.transaction.transaction_id)
    );
    cleanup(&root);
}

#[test]
fn request_owned_prepare_recovery_repairs_post_prepare_rejection() {
    let root = temp_root("local_backend_request_prepare_rejected_after_prepare");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let current = object(version("version_request_prepare_rejected_1"));
    let mut committed = current.clone();
    committed.version = version("version_request_prepare_rejected_2");
    committed.mode = 0o600;
    let admitted_at = OffsetDateTime::UNIX_EPOCH;
    let pending_request = DfsNamespaceRequestRecord {
        file_system_id: current.file_system_id.clone(),
        requester_node_id: node("node_request_prepare_rejected_requester"),
        request_id: "request-prepare-rejected".to_string(),
        operation_id: "req-request-prepare-rejected".to_string(),
        authority_shard_id: shard_id.clone(),
        operation: DfsNamespaceOperation::SetMode {
            path: current.path.clone(),
            mode: committed.mode,
        },
        command_digest: "blake3:request-prepare-rejected".to_string(),
        status: DfsNamespaceRequestStatus::Pending,
        result: None,
        posix_coordination_result: None,
        recovery_attempts: 0,
        last_recovery_error: None,
        created_at: admitted_at,
        updated_at: admitted_at,
    };
    let mut prepared_completion = pending_request.clone();
    prepared_completion.status = DfsNamespaceRequestStatus::Completed;
    prepared_completion.result = Some(DfsNamespaceResult::Applied);
    prepared_completion.updated_at =
        OffsetDateTime::from_unix_timestamp(1).expect("completion timestamp");
    let mut prepare = namespace_prepare(
        &format!(
            "namespace-request:{}:transaction-request-prepare-rejected",
            pending_request.operation_id
        ),
        &shard_id.0,
        &current,
        committed.clone(),
    );
    prepare.completed_request = Some(prepared_completion.clone());
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("request-owned prepare digest");

    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .put_namespace_request_on_shard(&shard_id, &pending_request)
        .expect("pending request");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("durable hidden prepare");

    // This is the old crash-window failure: an outer operation reported a
    // transport error after the hidden prepare became durable and wrote a
    // contradictory terminal rejection.
    let mut rejected_completion = pending_request;
    rejected_completion.status = DfsNamespaceRequestStatus::Completed;
    rejected_completion.result = Some(DfsNamespaceResult::Rejected {
        reason: "ambiguous downstream timeout".to_string(),
        posix_error: None,
        current_primary_node_id: None,
    });
    rejected_completion.updated_at =
        OffsetDateTime::from_unix_timestamp(2).expect("rejection timestamp");
    backend
        .put_namespace_request_on_shard(&shard_id, &rejected_completion)
        .expect("old contradictory rejection");

    backend
        .prepare_namespace_transaction(&prepare)
        .expect("recover the exact existing hidden prepare");
    backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("commit recovered prepare");

    assert_eq!(
        backend
            .get_namespace_request(
                &prepared_completion.requester_node_id,
                &prepared_completion.request_id,
            )
            .expect("completed request"),
        Some(prepared_completion.clone()),
        "the durable prepare attachment is the canonical terminal result"
    );
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("committed object"),
        Some(committed)
    );

    force_table_replay_from_journal(&backend, &root, 0);
    drop(backend);
    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen repaired request");
    assert_eq!(
        reopened
            .get_namespace_request(
                &prepared_completion.requester_node_id,
                &prepared_completion.request_id,
            )
            .expect("replayed completed request"),
        Some(prepared_completion),
        "journal-only semantic commit replay must retain the prepared canonical outcome"
    );
    cleanup(&root);
}

#[test]
fn local_backend_rmdir_transaction_recovers_after_table_apply_crash_and_exact_retry() {
    let root = temp_root("local_backend_rmdir_table_apply_crash");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let file_system = file_system();
    let mut parent = object(version("version_1"));
    parent.object_id =
        DfsObjectId::from_legacy_named("obj_rmdir_apply_parent").expect("parent object id");
    parent.path = "/docs".to_string();
    parent.kind = DfsObjectKind::Directory;
    parent.content_hash = None;
    parent.content_size_bytes = None;
    let mut child = object(version("version_1"));
    child.object_id =
        DfsObjectId::from_legacy_named("obj_rmdir_apply_child").expect("child object id");
    child.path = "/docs/empty".to_string();
    child.kind = DfsObjectKind::Directory;
    child.content_hash = None;
    child.content_size_bytes = None;
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&parent).expect("parent");
    backend.put_object(&child).expect("child");
    backend
        .persist_current_metadata_checkpoint_unlocked()
        .expect("checkpoint before injected table failure");

    let mut tombstone = child.clone();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.version = version("version_2");
    tombstone.changed_at_unix_nanos = child.changed_at_unix_nanos.saturating_add(1);
    let mut touched_parent = parent.clone();
    touched_parent.version = version("version_2");
    touched_parent.modified_at_unix_nanos = parent.modified_at_unix_nanos.saturating_add(1);
    touched_parent.changed_at_unix_nanos = parent.changed_at_unix_nanos.saturating_add(1);
    let transaction = DfsNamespaceTransaction {
        content_reference_identity: None,
        contribution_digest: None,
        extension_expectations: Vec::new(),
        file_version_pins: None,
        transaction_id: "namespace_rmdir_apply_crash".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        objects: vec![tombstone.clone(), touched_parent.clone()],
        versions: vec![
            object_version_record(
                &tombstone,
                tombstone.version.clone(),
                OffsetDateTime::UNIX_EPOCH,
            ),
            object_version_record(
                &touched_parent,
                touched_parent.version.clone(),
                OffsetDateTime::UNIX_EPOCH,
            ),
        ],
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };

    let table_path = metadata_table_path(&root);
    let error = {
        let _lock = backend.begin_metadata_write().expect("metadata write lock");
        let durable_transaction = normalized_namespace_transaction(&transaction);
        validate_namespace_transaction_preconditions(&backend.tables, &durable_transaction)
            .expect("rmdir transaction preconditions");
        let entry = backend
            .write_journal_unlocked(DfsMetadataMutation::ApplyNamespaceTransaction {
                file_system_id: durable_transaction.file_system_id.clone(),
                transaction_id: durable_transaction.transaction_id.clone(),
                object_count: durable_transaction.objects.len(),
                operation_id: None,
                transaction: Some(durable_transaction.clone()),
            })
            .expect("durable namespace transaction append");
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        backend
            .plan_namespace_transaction_table_ops(
                &durable_transaction,
                entry.created_at,
                &mut table_ops,
                &mut rollback,
            )
            .expect("preapply rmdir table plan");
        assert_eq!(
            backend.tables.objects.get(&tombstone.object_id),
            Some(&tombstone),
            "the injected failure starts after the tombstone preapply"
        );
        assert_eq!(
            backend.tables.objects.get(&touched_parent.object_id),
            Some(&touched_parent),
            "the injected failure starts after the parent-touch preapply"
        );
        fs::remove_file(&table_path).expect("remove empty materialized table log");
        fs::create_dir(&table_path).expect("block table apply");
        backend
            .append_preapplied_table_ops(entry.sequence, table_ops, rollback)
            .expect_err("table apply failure after durable journal append")
    };
    assert!(matches!(error, MetadataBackendError::Io { .. }));
    assert_eq!(
        backend.tables.objects.get(&child.object_id),
        Some(&child),
        "failed table apply must roll back the in-memory tombstone"
    );
    assert_eq!(
        backend.tables.objects.get(&parent.object_id),
        Some(&parent),
        "failed table apply must roll back the in-memory parent touch"
    );
    assert!(
        backend
            .journal_since(0)
            .expect("durable journal")
            .iter()
            .any(|entry| matches!(
                &entry.mutation,
                DfsMetadataMutation::ApplyNamespaceTransaction {
                    transaction_id,
                    ..
                } if transaction_id == &transaction.transaction_id
            ))
    );

    fs::remove_dir(&table_path).expect("restore table log path");
    drop(backend);
    let mut reopened =
        LocalSobjMetadataBackend::new(shard_id, &root).expect("recover durable transaction");
    assert_eq!(
        reopened
            .get_object(&tombstone.object_id)
            .expect("recovered tombstone"),
        Some(tombstone.clone())
    );
    assert_eq!(
        reopened
            .get_object(&touched_parent.object_id)
            .expect("recovered parent"),
        Some(touched_parent.clone())
    );

    reopened
        .apply_namespace_transaction(&transaction)
        .expect("exact retry after recovery");
    assert_eq!(
        reopened
            .get_object(&tombstone.object_id)
            .expect("retried tombstone"),
        Some(tombstone)
    );
    assert_eq!(
        reopened
            .get_object(&touched_parent.object_id)
            .expect("retried parent"),
        Some(touched_parent)
    );

    cleanup(&root);
}

#[test]
fn local_backend_recovers_stale_materialization_cursor_without_rewriting_table_log() {
    let root = temp_root("local_backend_stale_materialization_cursor_no_rewrite");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let object = object(version("version_1"));
    let (last_sequence, table_len_before) = {
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        let entry = backend.put_object(&object).expect("object");
        let table_len = fs::metadata(metadata_table_path(&root))
            .expect("metadata table")
            .len();
        (entry.sequence, table_len)
    };
    write_sobj_atomic(
        &root
            .join("records")
            .join("local-materialization")
            .join("cursor.sobj"),
        &DfsMetadataReplayCursor {
            shard_id: shard_id.clone(),
            last_sequence: 0,
        },
    )
    .expect("stale materialization cursor");

    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");

    assert_eq!(
        reopened
            .get_object(&object.object_id)
            .expect("object lookup"),
        Some(object),
        "stale cursor recovery must preserve the materialized object"
    );
    assert_eq!(
        read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
            .expect("materialization cursor")
            .last_sequence,
        last_sequence
    );
    assert_eq!(
        fs::metadata(metadata_table_path(&root))
            .expect("metadata table")
            .len(),
        table_len_before,
        "idempotent cursor recovery must not append duplicate table batches"
    );

    cleanup(&root);
}

fn deferred_materialization_hint_fixture(
    root: &std::path::Path,
) -> (
    LocalSobjMetadataBackend,
    DfsFileSystem,
    DfsMetadataPrefixProof,
) {
    let shard_id = DfsMetadataShardId::new("root");
    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source"))
        .expect("source backend");
    let mut expected = file_system();
    for generation in 0..4 {
        expected.display_name = format!("deferred hint generation {generation}");
        source.put_file_system(&expected).expect("source append");
    }
    let mut entries = source.journal_since(0).expect("source journal");
    let prefix = source.metadata_prefix_proof().expect("source exact prefix");
    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        expected.file_system_id.clone(),
        shard_id.clone(),
        node("hint_source"),
        1,
    );
    let mut replica = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("replica"))
        .expect("replica backend");
    let first = entries.remove(0);
    replica
        .apply_journal_batch_with_replica_permit(
            &permit,
            &DfsMetadataJournalBatch {
                shard_id: shard_id.clone(),
                next_after_sequence: first.sequence,
                entries: vec![first],
            },
        )
        .expect("materialized baseline");
    replica
        .persist_current_metadata_checkpoint_unlocked()
        .expect("durable exact baseline");
    let report = replica
        .persist_journal_batch_with_replica_permit_report(
            &permit,
            &DfsMetadataJournalBatch {
                shard_id,
                next_after_sequence: 4,
                entries,
            },
        )
        .expect("durable deferred suffix");
    assert!(report.materialization_pending());
    assert_eq!(replica.materialized_through_sequence, 1);
    assert_eq!(replica.journal_last_sequence(), 4);
    (replica, expected, prefix)
}

#[test]
fn revision_expectations_reject_journal_ahead_reads_and_accept_a_refreshed_cut() {
    const FOREIGN_NAMESPACE: &str = "vendor.read-refresh.foreign";
    const OWNED_NAMESPACE: &str = "vendor.read-refresh.update";
    for refresh_before_read in [false, true] {
        let root = temp_root(&format!("revision_read_refresh_{refresh_before_read}"));
        let source_root = root.join("source");
        let replica_root = root.join("replica");
        let shard_id = DfsMetadataShardId::new("root");
        let mut source =
            LocalSobjMetadataBackend::new(shard_id.clone(), &source_root).expect("source backend");
        source.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
        let mut fs = file_system();
        fs.metadata_format = crate::DfsRevisionMetadataFormat::NamespacedV6;
        source.put_file_system(&fs).expect("source filesystem");
        let mut current = object(version("revision-read-refresh-source"));
        current.content_hash = Some(format!(
            "blake3:{}",
            blake3::hash(b"immutable revision bytes").to_hex()
        ));
        current.content_size_bytes = Some(b"immutable revision bytes".len() as u64);
        current
            .current_locations
            .push(node("read-refresh-location"));
        current.current_locations.sort();
        current.current_locations.dedup();
        let created_at = OffsetDateTime::from_unix_timestamp(1_700_000_001)
            .unwrap()
            .replace_nanosecond(691_861_654)
            .unwrap()
            .to_offset(time::UtcOffset::from_whole_seconds(3_600).unwrap());
        let mut r0 = object_version_record(&current, current.version.clone(), created_at);
        r0.metadata_format = crate::DfsRevisionMetadataFormat::NamespacedV6;
        r0.module_metadata.insert(
            FOREIGN_NAMESPACE.into(),
            crate::DfsModuleMetadataValue {
                schema_version: 1,
                payload: serde_json::json!({"flag": false, "opaque": ["preserved", 7]}),
            },
        );
        source.put_object(&current).expect("source object");
        source.put_version(&r0).expect("source R0");
        assert_eq!(
            source.get_version(&r0.object_id, &r0.version).unwrap(),
            Some(r0.clone())
        );
        let baseline_entries = source.journal_since(0).expect("normal source baseline");
        assert_eq!(baseline_entries.len(), 3);
        let baseline_head = source.journal_last_sequence();
        let baseline_prefix = source.metadata_prefix_proof().unwrap();
        let permit = DfsMetadataReplicaIngestPermit::new_validated(
            fs.file_system_id.clone(),
            shard_id.clone(),
            node("read-refresh-source"),
            1,
        );
        let mut ingester = LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root)
            .expect("replica ingester");
        ingester.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
        ingester
            .apply_journal_batch_with_replica_permit(
                &permit,
                &DfsMetadataJournalBatch {
                    shard_id: shard_id.clone(),
                    next_after_sequence: baseline_head,
                    entries: baseline_entries,
                },
            )
            .expect("validated materialized baseline");
        ingester
            .persist_current_metadata_checkpoint_unlocked()
            .expect("checkpoint exact baseline");
        assert_eq!(ingester.materialized_through_sequence, baseline_head);
        assert_eq!(ingester.metadata_prefix_proof().unwrap(), baseline_prefix);
        let table_generation = ingester.table_storage_generation.clone();
        let table_batches = metadata_table_batch_count(&ingester.root);
        // Opening after the suffix would recover it and lose this observed cut.
        let mut reader = LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root)
            .expect("second reader opens before the suffix");
        reader.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
        assert_eq!(reader.journal_last_sequence(), baseline_head);
        assert_eq!(
            reader.get_version(&r0.object_id, &r0.version).unwrap(),
            Some(r0.clone())
        );

        let mut r1 = r0.clone();
        r1.module_metadata
            .get_mut(FOREIGN_NAMESPACE)
            .unwrap()
            .payload = serde_json::json!({"flag": true, "opaque": ["preserved", 7]});
        let suffix_entry = source.put_version(&r1).expect("normal source R1 update");
        assert_eq!(suffix_entry.sequence, baseline_head + 1);
        let suffix_prefix = source.metadata_prefix_proof().unwrap();
        let report = ingester
            .persist_journal_batch_with_replica_permit_report(
                &permit,
                &DfsMetadataJournalBatch {
                    shard_id: shard_id.clone(),
                    next_after_sequence: suffix_entry.sequence,
                    entries: vec![suffix_entry],
                },
            )
            .expect("validated durable journal-only suffix");
        assert!(report.materialization_pending());
        assert_eq!(ingester.journal_last_sequence(), baseline_head + 1);
        assert_eq!(ingester.materialized_through_sequence, baseline_head);
        assert_eq!(ingester.table_storage_generation, table_generation);
        assert_eq!(metadata_table_batch_count(&ingester.root), table_batches);
        assert_eq!(ingester.metadata_prefix_proof().unwrap(), baseline_prefix);
        let plain = reader
            .get_version(&r0.object_id, &r0.version)
            .unwrap()
            .unwrap();
        assert_eq!(plain, r0, "ordinary table observation must reproduce R0");
        assert_eq!(plain.created_at.nanosecond(), 691_861_654);
        assert_eq!(plain.created_at.offset().whole_seconds(), 3_600);
        assert_eq!(reader.journal_last_sequence(), baseline_head);
        assert_eq!(reader.materialized_through_sequence, baseline_head);

        let observed = if refresh_before_read {
            reader.begin_consistent_read_snapshot().unwrap();
            let row = reader
                .get_version(&r1.object_id, &r1.version)
                .unwrap()
                .unwrap();
            assert_eq!(row, r1, "consistent-read refresh must materialize exact R1");
            assert_eq!(row.created_at, created_at);
            assert_eq!(row.created_at.offset().whole_seconds(), 3_600);
            assert_eq!(reader.journal_last_sequence(), baseline_head + 1);
            assert_eq!(reader.materialized_through_sequence, baseline_head + 1);
            assert_eq!(reader.metadata_prefix_proof().unwrap(), suffix_prefix);
            reader.end_consistent_read_snapshot();
            row
        } else {
            plain
        };
        let mut proposed = observed.clone();
        proposed.module_metadata.insert(
            OWNED_NAMESPACE.into(),
            crate::DfsModuleMetadataValue {
                schema_version: 1,
                payload: serde_json::json!({"update": "requested"}),
            },
        );
        let transaction = DfsNamespaceTransaction {
            content_reference_identity: None,
            contribution_digest: None,
            transaction_id: format!("revision-read-refresh:{refresh_before_read}"),
            file_system_id: fs.file_system_id.clone(),
            objects: Vec::new(),
            versions: vec![proposed.clone()],
            file_version_pins: None,
            advisory_lock_states: Vec::new(),
            extension_expectations: vec![crate::DfsRevisionMetadataExpectation {
                object_id: observed.object_id.clone(),
                version: observed.version.clone(),
                expected: Some(Box::new(observed)),
            }],
            created_at,
        };
        let final_row = if refresh_before_read {
            let committed = reader
                .apply_namespace_transaction(&transaction)
                .expect("exact refreshed R1 before-image accepts the update");
            assert_eq!(committed.sequence, baseline_head + 2);
            assert_eq!(reader.journal_last_sequence(), baseline_head + 2);
            assert!(
                reader
                    .get_namespace_transaction(&transaction.transaction_id)
                    .unwrap()
                    .is_some()
                    || reader
                        .get_namespace_transaction_receipt(&transaction.transaction_id)
                        .unwrap()
                        .is_some()
            );
            proposed
        } else {
            let rejected = reader
                .apply_namespace_transaction(&transaction)
                .expect_err("writer refresh must reject the stale R0 observation");
            assert!(
                matches!(&rejected, MetadataBackendError::Serde(reason)
                    if reason.contains("revision metadata changed before publication")),
                "unexpected stale-row rejection: {rejected}"
            );
            assert_eq!(reader.journal_last_sequence(), baseline_head + 1);
            assert_eq!(reader.materialized_through_sequence, baseline_head + 1);
            assert_eq!(reader.metadata_prefix_proof().unwrap(), suffix_prefix);
            assert!(
                reader
                    .get_namespace_transaction(&transaction.transaction_id)
                    .unwrap()
                    .is_none()
            );
            assert!(
                reader
                    .get_namespace_transaction_receipt(&transaction.transaction_id)
                    .unwrap()
                    .is_none()
            );
            r1.clone()
        };
        assert_eq!(
            reader.get_object(&current.object_id).unwrap(),
            Some(current.clone())
        );
        assert_eq!(
            reader.get_version(&r1.object_id, &r1.version).unwrap(),
            Some(final_row.clone())
        );
        assert_eq!(final_row.created_at, created_at);
        assert_eq!(final_row.locations, r0.locations);
        assert_eq!(final_row.retention, r0.retention);
        assert_eq!(
            final_row.module_metadata.get(FOREIGN_NAMESPACE),
            r1.module_metadata.get(FOREIGN_NAMESPACE)
        );
        assert!(
            reader
                .version_pins_for_object(&current.object_id)
                .unwrap()
                .is_empty()
        );
        let final_head = reader.journal_last_sequence();
        drop(reader);
        drop(ingester);
        let reopened = LocalSobjMetadataBackend::new(shard_id, &replica_root)
            .expect("reopen exact accepted or rejected outcome");
        assert_eq!(reopened.journal_last_sequence(), final_head);
        assert_eq!(
            reopened.get_version(&r1.object_id, &r1.version).unwrap(),
            Some(final_row)
        );
        if !refresh_before_read {
            assert!(
                reopened
                    .get_namespace_transaction(&transaction.transaction_id)
                    .unwrap()
                    .is_none()
            );
            assert!(
                reopened
                    .get_namespace_transaction_receipt(&transaction.transaction_id)
                    .unwrap()
                    .is_none()
            );
        }
        cleanup(&root);
    }
}

fn optional_materialization_hint_bytes(path: &std::path::Path) -> Option<Vec<u8>> {
    match fs::read(path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("read materialization hint {}: {error}", path.display()),
    }
}

#[test]
fn deferred_materialization_uses_exact_table_head_without_rewriting_recovery_hints() {
    for hint in ["behind", "ahead", "missing"] {
        for live_recovery in [false, true] {
            let root = temp_root(&format!("deferred_hint_{hint}_live_{live_recovery}"));
            let (mut replica, expected, prefix) = deferred_materialization_hint_fixture(&root);
            let hint_path = replica.local_materialization_cursor_path();
            if hint == "missing" {
                fs::remove_file(&hint_path).expect("remove derived hint");
            } else {
                write_sobj_atomic(
                    &hint_path,
                    &DfsMetadataReplayCursor {
                        shard_id: replica.shard_id.clone(),
                        last_sequence: if hint == "ahead" { 9000 } else { 0 },
                    },
                )
                .expect("perturb recovery hint");
            }
            if live_recovery {
                let before = optional_materialization_hint_bytes(&hint_path);
                let batches_before = metadata_table_batch_count(&replica.root);
                assert_eq!(
                    replica
                        .materialize_pending_replica_journal()
                        .expect("recover from exact table proof, not the hint"),
                    3
                );
                assert_eq!(
                    optional_materialization_hint_bytes(&hint_path),
                    before,
                    "a suffix below the existing 512-entry checkpoint interval must not rewrite its hint"
                );
                assert!(
                    metadata_table_batch_count(&replica.root) - batches_before <= 1,
                    "the exact suffix is materialized in one batch"
                );
                assert_eq!(
                    replica
                        .metadata_prefix_proof()
                        .expect("live recovered prefix"),
                    prefix
                );
                assert_eq!(
                    replica
                        .get_file_system(&expected.file_system_id)
                        .expect("live materialization"),
                    Some(expected.clone())
                );
            }
            drop(replica);
            let reopened = LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("root"),
                root.join("replica"),
            )
            .expect("reopen repairs hints from durable table and journal prefixes");
            assert_eq!(reopened.materialized_through_sequence, 4);
            assert_eq!(reopened.journal_last_sequence(), 4);
            assert_eq!(
                reopened
                    .metadata_prefix_proof()
                    .expect("reopened exact prefix"),
                prefix,
                "crash/reopen and live recovery must restore the identical journal proof"
            );
            assert_eq!(
                reopened
                    .get_file_system(&expected.file_system_id)
                    .expect("reopened materialization"),
                Some(expected)
            );
            cleanup(&root);
        }
    }
}

#[test]
fn deferred_materialization_repairs_malformed_hint_only_after_exact_replay() {
    for hint in ["malformed", "foreign-shard"] {
        let root = temp_root(&format!("deferred_materialization_{hint}_hint"));
        let (mut replica, expected, prefix) = deferred_materialization_hint_fixture(&root);
        let hint_path = replica.local_materialization_cursor_path();
        if hint == "malformed" {
            fs::write(&hint_path, b"torn SOBJ hint").expect("malformed derived hint");
        } else {
            write_sobj_atomic(
                &hint_path,
                &DfsMetadataReplayCursor {
                    shard_id: DfsMetadataShardId::new("foreign"),
                    last_sequence: 9000,
                },
            )
            .expect("foreign-shard hint");
        }
        let invalid_hint = fs::read(&hint_path).expect("invalid hint bytes");
        let table_path = metadata_table_path(&replica.root);
        if table_path.exists() {
            fs::remove_file(&table_path).expect("remove active table log");
        }
        fs::create_dir(&table_path).expect("block table materialization");
        assert!(matches!(
            replica.materialize_pending_replica_journal(),
            Err(MetadataBackendError::Io { .. })
        ));
        assert_eq!(
            fs::read(&hint_path).expect("hint after failed table append"),
            invalid_hint,
            "a failed replay must not replace the hint before restoring its exact table proof"
        );
        fs::remove_dir(&table_path).expect("unblock same-instance materialization retry");
        assert_eq!(
            replica
                .materialize_pending_replica_journal()
                .expect("replay derives its start from exact table proof"),
            3
        );
        assert_eq!(
            read_sobj::<DfsMetadataReplayCursor>(&hint_path).expect("repaired hint"),
            DfsMetadataReplayCursor {
                shard_id: replica.shard_id.clone(),
                last_sequence: 4
            }
        );
        assert_eq!(
            replica.metadata_prefix_proof().expect("recovered proof"),
            prefix
        );
        drop(replica);
        let reopened =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("replica"))
                .expect("reopen repaired derived hint");
        assert_eq!(
            reopened.metadata_prefix_proof().expect("reopened proof"),
            prefix
        );
        assert_eq!(
            reopened
                .get_file_system(&expected.file_system_id)
                .expect("reopened state"),
            Some(expected)
        );
        cleanup(&root);
    }
}

#[test]
fn deferred_materialization_requires_exact_table_proof_and_preserves_hint_io_errors() {
    let root = temp_root("deferred_materialization_requires_table_proof");
    let (mut replica, _, _) = deferred_materialization_hint_fixture(&root);
    let hint_path = replica.local_materialization_cursor_path();
    let hint_before = fs::read(&hint_path).expect("original hint");
    let exact_proof = replica.prefix_proof.take();
    assert!(
        replica
            .recover_unmaterialized_local_journal_entries_from_table_head()
            .is_err()
    );
    replica.prefix_proof = exact_proof.clone();
    replica.prefix_proof.as_mut().unwrap().journal_sequence = 4;
    assert!(
        replica
            .recover_unmaterialized_local_journal_entries_from_table_head()
            .is_err()
    );
    assert_eq!(replica.materialized_through_sequence, 1);
    assert_eq!(fs::read(&hint_path).expect("unchanged hint"), hint_before);
    replica.prefix_proof = exact_proof;
    fs::remove_file(&hint_path).expect("remove cursor file");
    fs::create_dir(&hint_path).expect("unreadable hint path");
    assert!(
        matches!(
            replica.materialize_pending_replica_journal(),
            Err(MetadataBackendError::Io { .. })
        ),
        "real filesystem errors are not malformed derived hints"
    );
    assert!(
        hint_path.is_dir(),
        "recovery must not replace an unreadable filesystem entry"
    );
    cleanup(&root);
}

#[test]
fn deferred_materialization_rejects_hint_parent_that_is_a_regular_file() {
    let root = temp_root("deferred_materialization_hint_parent_file");
    let (mut replica, _, _) = deferred_materialization_hint_fixture(&root);
    let hint_path = replica.local_materialization_cursor_path();
    let hint_parent = hint_path.parent().expect("hint parent");
    fs::remove_file(&hint_path).expect("remove derived cursor");
    fs::remove_dir(hint_parent).expect("remove cursor directory");
    fs::write(hint_parent, b"unreadable hint parent").expect("block hint parent with a file");
    assert!(
        !hint_path.exists(),
        "the optional reader would mistake this error for absence"
    );

    let error = replica
        .materialize_pending_replica_journal()
        .expect_err("live recovery must report an invalid hint parent");
    assert!(
        matches!(error, MetadataBackendError::Io { ref path, ref source }
        if path == &hint_path && source.kind() == std::io::ErrorKind::NotADirectory)
    );
    assert_eq!(
        fs::read(hint_parent).expect("blocking file remains"),
        b"unreadable hint parent"
    );
    cleanup(&root);
}

#[test]
fn local_backend_checkpoints_materialization_cursor_instead_of_every_write() {
    let root = temp_root("local_backend_deferred_materialization_cursor");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut expected_objects = Vec::new();
    let (last_sequence, table_len_before) = {
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        let mut last_sequence = 0;
        for index in 0..8 {
            let mut object = object(version("version_1"));
            object.object_id = DfsObjectId::from_legacy_named(format!("obj_cursor_defer_{index}"))
                .expect("object id");
            object.path = format!("/services/cursor-defer/{index}.txt");
            let entry = backend.put_object(&object).expect("object");
            last_sequence = entry.sequence;
            expected_objects.push(object);
        }
        let cursor =
            read_sobj::<DfsMetadataReplayCursor>(&backend.local_materialization_cursor_path())
                .expect("materialization cursor");
        assert_eq!(
            cursor.last_sequence, 0,
            "short local write bursts should not force a cursor fsync for every metadata mutation"
        );
        let table_len = fs::metadata(metadata_table_path(&root))
            .expect("metadata table")
            .len();
        (last_sequence, table_len)
    };

    let reopened = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopen backend");

    for object in expected_objects {
        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object),
            "reopen should recover objects whose materialization cursor was not checkpointed"
        );
    }
    assert_eq!(
        read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
            .expect("materialization cursor")
            .last_sequence,
        last_sequence,
        "reopen recovery should advance the cursor after idempotent replay"
    );
    assert_eq!(
        fs::metadata(metadata_table_path(&root))
            .expect("metadata table")
            .len(),
        table_len_before,
        "idempotent replay from a deferred cursor must not duplicate materialized table batches"
    );

    cleanup(&root);
}

#[test]
fn local_backend_recovers_lost_relaxed_table_log_after_cursor_checkpoint() {
    let root = temp_root("local_backend_lost_relaxed_table_log");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    let mut expected_edge_objects = Vec::new();
    for index in 0..520 {
        let mut object = object(version("version_1"));
        object.object_id = DfsObjectId::from_legacy_named(format!("obj_relaxed_table_{index}"))
            .expect("object id");
        object.path = format!("/services/relaxed-table/{index}.txt");
        backend.put_object(&object).expect("object");
        if matches!(index, 0 | 519) {
            expected_edge_objects.push(object);
        }
    }
    let final_sequence = backend
        .journal_since(0)
        .expect("journal")
        .last()
        .expect("journal entry")
        .sequence;
    let checkpoint =
        read_sobj::<DfsMetadataReplayCursor>(&backend.local_materialization_cursor_path())
            .expect("materialization cursor");
    assert!(
        checkpoint.last_sequence > 0 && checkpoint.last_sequence <= final_sequence,
        "the test must lose a table log after a durable cursor checkpoint"
    );
    drop(backend);

    fs::remove_file(metadata_table_path(&root)).expect("simulate lost rebuildable table log");

    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("recover from journal");
    for expected in expected_edge_objects {
        assert_eq!(
            reopened
                .get_object(&expected.object_id)
                .expect("recovered object"),
            Some(expected)
        );
    }
    assert_eq!(reopened.materialized_through_sequence, final_sequence);
    assert_eq!(
        reopened
            .metadata_prefix_proof()
            .expect("recovered prefix proof")
            .journal_sequence,
        final_sequence
    );

    cleanup(&root);
}

#[test]
fn local_backend_unchanged_reopen_skips_deferred_recovery_scans() {
    let root = temp_root("local_backend_unchanged_reopen_skips_recovery_scans");
    let shard_id = DfsMetadataShardId::new("shard-root");
    {
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend
            .put_object(&object(version("version_1")))
            .expect("object");
    }

    reset_metadata_recovery_scan_calls();
    drop(
        LocalSobjMetadataBackend::new(shard_id.clone(), &root)
            .expect("first reopen publishes current clean recovery watermark"),
    );
    assert_eq!(
        metadata_recovery_scan_calls(),
        (1, 1, 1),
        "a journal/table generation newer than the previous clean watermark must run every recovery pass"
    );
    assert!(
        root.join("records")
            .join("tables")
            .join(METADATA_RECOVERY_WATERMARK_FILE_NAME)
            .is_file(),
        "successful recovery should durably publish its clean watermark"
    );

    reset_metadata_recovery_scan_calls();
    drop(LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("unchanged clean reopen"));
    assert_eq!(
        metadata_recovery_scan_calls(),
        (0, 0, 0),
        "an exact unchanged journal and table generation should skip all deferred recovery scans"
    );

    remove_file_if_exists(
        &root
            .join("records")
            .join("tables")
            .join(METADATA_RECOVERY_WATERMARK_FILE_NAME),
    )
    .expect("simulate crash before clean watermark publication");
    reset_metadata_recovery_scan_calls();
    drop(
        LocalSobjMetadataBackend::new(shard_id, &root)
            .expect("missing watermark must fall back to recovery"),
    );
    assert_eq!(
        metadata_recovery_scan_calls(),
        (1, 1, 1),
        "a crash before watermark publication must conservatively rescan"
    );

    let watermark_path = root
        .join("records")
        .join("tables")
        .join(METADATA_RECOVERY_WATERMARK_FILE_NAME);
    remove_file_if_exists(&watermark_path).expect("remove clean watermark");
    fs::create_dir(&watermark_path).expect("block atomic watermark publication");
    for attempt in 0..2 {
        reset_metadata_recovery_scan_calls();
        drop(
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("watermark publication failure must not hide completed recovery"),
        );
        assert_eq!(
            metadata_recovery_scan_calls(),
            (1, 1, 1),
            "watermark write failure attempt {attempt} must leave the next reopen fail-closed to recovery"
        );
    }
    cleanup(&root);
}

#[test]
fn local_backend_inspection_neither_consumes_nor_publishes_recovery_watermark() {
    let root = temp_root("local_backend_inspection_ignores_recovery_watermark");
    let shard_id = DfsMetadataShardId::new("shard-root");
    {
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
    }
    let watermark_path = root
        .join("records")
        .join("tables")
        .join(METADATA_RECOVERY_WATERMARK_FILE_NAME);
    let corrupt_watermark = b"inspection-must-not-consume-this-marker";
    fs::write(&watermark_path, corrupt_watermark).expect("corrupt writer-only watermark");

    let inspected = LocalSobjMetadataBackend::open_for_inspection(shard_id, &root)
        .expect("inspection must not parse writer recovery watermark");
    assert_eq!(
        inspected
            .metadata_summary()
            .expect("inspection summary")
            .file_system_count,
        1
    );
    assert_eq!(
        fs::read(&watermark_path).expect("watermark after inspection"),
        corrupt_watermark,
        "inspection must not repair, remove, or publish a writer recovery watermark"
    );
    cleanup(&root);
}

#[test]
fn local_backend_reopen_truncates_torn_table_log_tail() {
    let root = temp_root("local_backend_truncates_torn_table_log");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let object_a = object(version("version_1"));
    {
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend.put_object(&object_a).expect("object");
    }
    let table_path = metadata_table_path(&root);
    let good_len = fs::metadata(&table_path).expect("table log").len();
    assert!(good_len > 0);

    // Crash mid-append: a full length prefix promising more bytes than
    // the writer managed to flush.
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&table_path)
        .expect("open table log");
    file.write_all(&512_u32.to_le_bytes())
        .and_then(|()| file.write_all(&[0xAB; 17]))
        .expect("torn append");
    drop(file);

    let mut object_b = object(version("version_2"));
    object_b.object_id =
        DfsObjectId::from_legacy_named("obj_after_crash").expect("valid object id");
    object_b.path = "docs/after-crash.md".to_string();
    {
        let mut reopened = LocalSobjMetadataBackend::new(shard_id.clone(), &root)
            .expect("reopen tolerates torn tail");
        assert_eq!(
            reopened
                .get_object(&object_a.object_id)
                .expect("object lookup"),
            Some(object_a.clone()),
            "complete records before the torn tail must replay"
        );
        assert_eq!(
            fs::metadata(&table_path).expect("table log").len(),
            good_len,
            "open must cut the torn record off the log so later appends stay framed"
        );
        // The append that lands after the crash: without the truncation it
        // would be entombed behind the torn record and break every later
        // replay with an sobj framing error.
        reopened.put_object(&object_b).expect("post-crash append");
    }

    let reopened =
        LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen after post-crash append");
    assert_eq!(
        reopened
            .get_object(&object_a.object_id)
            .expect("object lookup"),
        Some(object_a)
    );
    assert_eq!(
        reopened
            .get_object(&object_b.object_id)
            .expect("object lookup"),
        Some(object_b)
    );
    cleanup(&root);
}

#[test]
fn local_backend_reopen_truncates_invalid_table_log_tail() {
    let root = temp_root("local_backend_truncates_invalid_table_log_tail");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let object_a = object(version("version_1"));
    {
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend.put_object(&object_a).expect("object");
    }
    let table_path = metadata_table_path(&root);
    let good_len = fs::metadata(&table_path).expect("table log").len();

    let invalid_payload = b"not-sobj";
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&table_path)
        .expect("open table log");
    file.write_all(&(invalid_payload.len() as u32).to_le_bytes())
        .and_then(|()| file.write_all(invalid_payload))
        .expect("invalid tail append");
    drop(file);

    let reopened =
        LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen tolerates invalid tail");
    assert_eq!(
        reopened
            .get_object(&object_a.object_id)
            .expect("object lookup"),
        Some(object_a),
        "complete records before the malformed tail must replay"
    );
    assert_eq!(
        fs::metadata(&table_path).expect("table log").len(),
        good_len,
        "open must cut a fully length-prefixed but malformed tail before later appends"
    );
    cleanup(&root);
}

#[test]
fn local_backend_readers_never_observe_torn_metadata_during_writes() {
    let root = temp_root("local_backend_readers_during_writes");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    drop(backend);

    let total_writes = 256usize;
    let reader_count = 4usize;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Start readers and the writer together so the reads genuinely overlap the
    // appends rather than racing thread startup.
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(reader_count + 1));

    // Readers use their own backend handle and read continuously while a writer
    // appends. The read path (`current_tables_snapshot`) is lock-free, so this
    // exercises its torn-tail safety: every read must succeed and never observe
    // more objects than the writer has committed.
    let mut readers = Vec::new();
    for _ in 0..reader_count {
        let root = root.to_path_buf();
        let stop = stop.clone();
        let barrier = barrier.clone();
        readers.push(std::thread::spawn(move || {
            let backend =
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                    .expect("reader backend");
            barrier.wait();
            // Read at least once, then keep reading until the writer signals stop.
            let mut reads = 0u64;
            loop {
                let objects = backend
                    .objects_for_file_system(&file_system_id())
                    .expect("read must never fail on a torn tail");
                assert!(
                    objects.len() <= total_writes,
                    "reader saw {} objects, more than the {total_writes} ever written",
                    objects.len()
                );
                reads += 1;
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
            }
            reads
        }));
    }

    let mut writer = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("writer backend");
    barrier.wait();
    for i in 0..total_writes {
        let mut object = object(version("version_1"));
        object.object_id =
            DfsObjectId::from_legacy_named(format!("obj_reader_race_{i}")).expect("object id");
        object.path = format!("/docs/reader-race-{i}.md");
        writer.put_object(&object).expect("write object");
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);

    for reader in readers {
        let reads = reader.join().expect("reader thread");
        assert!(reads > 0, "reader must have completed at least one read");
    }

    let backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopen");
    assert_eq!(
        backend
            .objects_for_file_system(&file_system_id())
            .expect("final objects")
            .len(),
        total_writes,
        "every committed write must be visible after the race"
    );

    cleanup(&root);
}

#[test]
fn local_backend_reopen_scans_only_the_highest_segment() {
    let root = temp_root("local_backend_reopen_uses_head_and_cursor");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let file_system = file_system();
    let object = object(version("version_1"));
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("object");
    backend
        .compact_metadata_table_log_if_needed(1)
        .expect("compact table snapshot");

    let high_sequence = JOURNAL_SEGMENT_ENTRY_LIMIT + 1;
    append_segmented_journal_entry(
        &root,
        &DfsMetadataJournalEntry {
            shard_id: shard_id.clone(),
            sequence: high_sequence,
            created_at: OffsetDateTime::now_utc(),
            mutation: DfsMetadataMutation::PutFileSystem {
                file_system_id: file_system.file_system_id.clone(),
                file_system: Some(file_system.clone()),
            },
        },
    )
    .expect("high segment entry");
    write_sobj_atomic(
        &root.join("journal").join("head.sobj"),
        &DfsMetadataJournalHead {
            next_sequence: high_sequence + 1,
        },
    )
    .expect("journal head");
    write_sobj_atomic(
        &backend.local_materialization_cursor_path(),
        &DfsMetadataReplayCursor {
            shard_id: shard_id.clone(),
            last_sequence: high_sequence,
        },
    )
    .expect("local cursor");
    // The bounded reopen scan reads only the highest segment. Do not ask a
    // prefix at 2 to skip unproven history up to the synthetic sequence.
    backend.materialized_through_sequence = high_sequence;
    backend.next_sequence = high_sequence + 1;
    backend.prefix_proof = Some(
        backend
            .metadata_prefix_checkpoint_for_shard(&shard_id, high_sequence)
            .expect("high-sequence installed checkpoint"),
    );
    backend
        .persist_current_metadata_checkpoint_unlocked()
        .expect("persist high-sequence installed checkpoint");
    fs::write(
        root.join("journal")
            .join("segments")
            .join("00000000000000000001.slog"),
        [1_u8, 0, 0, 0, 0xff],
    )
    .expect("corrupt consumed segment");
    drop(backend);

    let reopened = LocalSobjMetadataBackend::new(shard_id, &root)
        .expect("reopen should not scan consumed journal segments");
    assert_eq!(
        reopened.get_object(&object.object_id).expect("object"),
        Some(object)
    );
    assert!(
        reopened
            .journal_since(high_sequence)
            .expect("journal after head")
            .is_empty()
    );

    cleanup(&root);
}

#[test]
fn local_backend_reopen_tolerates_torn_fully_materialized_current_segment() {
    let root = temp_root("local_backend_reopen_with_corrupt_current_segment");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let file_system = file_system();
    let object = object(version("version_1"));
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("object");
    backend
        .compact_metadata_table_log_if_needed(1)
        .expect("compact table snapshot");
    write_sobj_atomic(
        &root.join("journal").join("head.sobj"),
        &DfsMetadataJournalHead { next_sequence: 3 },
    )
    .expect("journal head");
    write_sobj_atomic(
        &backend.local_materialization_cursor_path(),
        &DfsMetadataReplayCursor {
            shard_id: shard_id.clone(),
            last_sequence: 2,
        },
    )
    .expect("local cursor");
    fs::write(
        root.join("journal")
            .join("segments")
            .join("00000000000000000001.slog"),
        [2_u8, 0, 0, 0, 0xff],
    )
    .expect("corrupt consumed current segment");
    drop(backend);

    // The reopen scan still examines the current segment. The segmented
    // reader may truncate a torn consumed tail because the exact table
    // checkpoint already proves the materialized prefix through two.
    let reopened = LocalSobjMetadataBackend::new(shard_id, &root)
        .expect("reopen from the exact materialized checkpoint");
    assert_eq!(
        reopened.get_object(&object.object_id).expect("object"),
        Some(object)
    );
    assert!(
        reopened
            .journal_since(2)
            .expect("journal after materialized prefix")
            .is_empty()
    );

    cleanup(&root);
}

#[test]
fn local_backend_reopen_repairs_matching_stale_head_and_cursor() {
    let root = temp_root("local_backend_reopen_repairs_matching_stale_hints");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let file_system = file_system();
    let first_object = object(version("version_1"));
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    assert_eq!(
        backend
            .put_file_system(&file_system)
            .expect("filesystem")
            .sequence,
        1
    );
    assert_eq!(
        backend.put_object(&first_object).expect("object").sequence,
        2
    );

    // Reproduce the Docker restart cut: both deferred hints agree at one
    // while the segment and exact materialized prefix are already at two.
    write_sobj_atomic(
        &root.join("journal").join("head.sobj"),
        &DfsMetadataJournalHead { next_sequence: 2 },
    )
    .expect("stale journal head");
    write_sobj_atomic(
        &backend.local_materialization_cursor_path(),
        &DfsMetadataReplayCursor {
            shard_id: shard_id.clone(),
            last_sequence: 1,
        },
    )
    .expect("stale local cursor");
    drop(backend);

    let mut reopened = LocalSobjMetadataBackend::new(shard_id.clone(), &root)
        .expect("reopen repairs the stale hints from the durable segment");
    assert_eq!(reopened.journal_last_sequence(), 2);
    assert_eq!(
        reopened
            .metadata_prefix_proof()
            .expect("exact repaired prefix")
            .journal_sequence,
        2
    );
    let mut successor = object(version("version_2"));
    successor.object_id =
        DfsObjectId::from_legacy_named("obj_after_stale_head_repair").expect("object id");
    successor.path = "/docs/after-stale-head-repair.md".to_string();
    assert_eq!(
        reopened
            .put_object(&successor)
            .expect("successor append")
            .sequence,
        3,
        "reopen must not reuse the already durable sequence two"
    );

    cleanup(&root);
}

#[test]
fn local_backend_reopen_recovers_next_sequence_from_segment_tail() {
    let root = temp_root("local_backend_reopen_recovers_next_sequence_from_segment_tail");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    append_segmented_journal_entry(
        &root,
        &DfsMetadataJournalEntry {
            shard_id: DfsMetadataShardId::new("shard-root"),
            sequence: 2,
            created_at: OffsetDateTime::UNIX_EPOCH,
            mutation: DfsMetadataMutation::PutObject {
                file_system_id: file_system_id(),
                object_id: object_id(),
                path: "docs/readme.md".to_string(),
                object: None,
                routing_change: None,
            },
        },
    )
    .expect("append segment entry without advancing head");

    let mut reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopen");
    let mut object = object(version("version_1"));
    object.object_id = DfsObjectId::from_legacy_named("obj_after_tail_recovery").expect("object");
    object.path = "docs/after-tail-recovery.md".to_string();
    let entry = reopened.put_object(&object).expect("object");
    assert_eq!(
        entry.sequence, 3,
        "reopen must scan segment tails so a crash after append but before head update cannot reuse a sequence"
    );

    cleanup(&root);
}

#[test]
fn local_backend_pages_intents_by_target_version_after_reopen() {
    let root = temp_root("local_backend_pages_intents_by_target_version");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let file = object(version("version_0"));
    backend.put_object(&file).expect("object");
    for index in 0..6 {
        let target_version = if index % 2 == 0 {
            version("version_even")
        } else {
            version("version_odd")
        };
        let intent = DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: format!("intent-target-{index}"),
            file_system_id: file.file_system_id.clone(),
            object_id: file.object_id.clone(),
            base_version: Some(version("version_0")),
            target_version,
            coordinator_node_id: node("node_owner"),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some(format!("blake3:intent-target-{index}")),
            object_metadata: Some(file.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: None,
        };
        backend.put_intent(&intent).expect("intent");
    }

    let first_even_page = backend
        .intents_for_object_version_page(&file.object_id, &version("version_even"), None, 2)
        .expect("first even intent page");
    assert_eq!(
        first_even_page
            .intents
            .iter()
            .map(|intent| intent.intent_id.as_str())
            .collect::<Vec<_>>(),
        vec!["intent-target-0", "intent-target-2"]
    );
    assert!(first_even_page.next_cursor.is_some());
    let second_even_page = backend
        .intents_for_object_version_page(
            &file.object_id,
            &version("version_even"),
            first_even_page.next_cursor.as_ref(),
            2,
        )
        .expect("second even intent page");
    assert_eq!(
        second_even_page
            .intents
            .iter()
            .map(|intent| intent.intent_id.as_str())
            .collect::<Vec<_>>(),
        vec!["intent-target-4"]
    );
    assert!(second_even_page.next_cursor.is_none());

    drop(backend);
    let reopened =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("reopen");
    let mut cursor = None;
    let mut odd_intents = Vec::new();
    loop {
        let page = reopened
            .intents_for_object_version_page(
                &file.object_id,
                &version("version_odd"),
                cursor.as_ref(),
                1,
            )
            .expect("odd intent page");
        assert!(page.intents.len() <= 1);
        odd_intents.extend(page.intents.into_iter().map(|intent| intent.intent_id));
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    assert_eq!(
        odd_intents,
        vec![
            "intent-target-1".to_string(),
            "intent-target-3".to_string(),
            "intent-target-5".to_string()
        ]
    );

    cleanup(&root);
}

#[test]
fn local_backend_tombstone_compaction_retains_young_and_referenced_tombstones() {
    let root = temp_root("local_backend_tombstone_compaction_retains");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    backend.put_file_system(&file_system).expect("filesystem");

    let mut young = object(version("version_2"));
    young.path = "/docs/young.md".to_string();
    young.kind = DfsObjectKind::Tombstone;
    young.content_hash = None;
    young.changed_at_unix_nanos = 90;
    backend.put_object(&young).expect("young tombstone");

    let mut referenced = object(version("version_3"));
    referenced.object_id =
        DfsObjectId::from_legacy_named("obj_referenced_tombstone").expect("object id");
    referenced.path = "/docs/referenced.md".to_string();
    referenced.kind = DfsObjectKind::Tombstone;
    referenced.content_hash = None;
    referenced.changed_at_unix_nanos = 1;
    backend
        .put_object(&referenced)
        .expect("referenced tombstone");
    backend
        .put_version(&DfsObjectVersionRecord {
            content_durability: None,
            metadata_format: crate::DfsRevisionMetadataFormat::Legacy,
            retention: Default::default(),
            module_metadata: Default::default(),
            file_system_id: referenced.file_system_id.clone(),
            object_id: referenced.object_id.clone(),
            version: referenced.version.clone(),
            content_hash: None,
            size_bytes: None,
            locations: Vec::new(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: false,
        })
        .expect("version ref");

    let report = backend
        .compact_tombstones(
            DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(50))
                .with_now_unix_nanos(100),
        )
        .expect("compact");
    assert_eq!(report.scanned_tombstone_count, 2);
    assert_eq!(report.eligible_tombstone_count, 0);
    assert_eq!(report.retained_young_tombstone_count, 1);
    assert_eq!(report.retained_referenced_tombstone_count, 1);
    assert_eq!(report.compacted_tombstone_count, 0);
    assert_eq!(
        backend.get_object(&young.object_id).expect("young object"),
        Some(young)
    );
    assert_eq!(
        backend
            .get_object(&referenced.object_id)
            .expect("referenced object"),
        Some(referenced)
    );

    cleanup(&root);
}

#[test]
fn local_backend_recovers_secondary_indexes_for_legacy_object_records() {
    let root = temp_root("local_backend_recovers_secondary_indexes_for_legacy_object_records");
    {
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let target = object(version("version_1"));
        let mut alias = object(version("version_1"));
        alias.object_id = DfsObjectId::from_legacy_named("obj_legacy_alias").expect("object");
        alias.path = "/docs/readme.alias".to_string();
        alias.kind = DfsObjectKind::Hardlink;
        alias.content_hash = None;
        alias.link_target_object_id = Some(target.object_id.clone());
        for object in [&target, &alias] {
            backend.put_object(object).expect("object");
        }
    }

    for index_dir in ["filesystem-objects", "paths", "dentries", "hardlinks"] {
        fs::remove_dir_all(root.join("records").join(index_dir)).ok();
    }

    let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("recovered backend");
    assert_eq!(
        recovered
            .objects_for_file_system(&file_system_id())
            .expect("objects")
            .iter()
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/docs/readme.alias", "docs/readme.md"]
    );
    assert_eq!(
        recovered
            .child_objects_for_directory(&file_system_id(), "/docs")
            .expect("children")
            .iter()
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/docs/readme.alias", "docs/readme.md"]
    );
    assert_eq!(
        recovered
            .hardlinks_to_object(&file_system_id(), &object_id())
            .expect("hardlinks")
            .iter()
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/docs/readme.alias"]
    );

    cleanup(&root);
}

#[test]
fn local_backend_recovers_object_locations_from_visible_version() {
    let root = temp_root("local_backend_recovers_object_locations_from_visible_version");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let owner = node("node_owner");
    let replica = node("node_replica");
    backend.put_file_system(&file_system()).expect("filesystem");
    let mut object = object(version("version_2"));
    object.current_locations = vec![owner.clone()];
    object.content_hash = Some("blake3:recover-locations".to_string());
    backend.put_object(&object).expect("object");
    let version_record = DfsObjectVersionRecord {
        content_durability: None,
        metadata_format: crate::DfsRevisionMetadataFormat::Legacy,
        retention: Default::default(),
        module_metadata: Default::default(),
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        size_bytes: Some(18),
        locations: vec![owner.clone(), replica.clone()],
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    backend.put_version(&version_record).expect("version");

    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopen");

    assert_eq!(
        reopened
            .get_object(&object.object_id)
            .expect("object")
            .expect("object")
            .current_locations,
        vec![owner, replica]
    );
    cleanup(&root);
}

#[test]
fn local_backend_recovers_interrupted_visible_version_commit() {
    let root = temp_root("local_backend_recovers_interrupted_visible_version_commit");
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
    backend.put_receipt(&receipt).expect("receipt");
    backend
        .commit_visible_version(
            &initial.version,
            &committed,
            &version_record,
            std::slice::from_ref(&receipt.replica_node_id),
        )
        .expect("commit");

    force_table_replay_from_journal(&backend, &root, 0);
    drop(backend);

    let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("recovered backend");
    assert_eq!(
        recovered
            .get_object(&committed.object_id)
            .expect("get object")
            .expect("object")
            .version,
        committed.version
    );
    assert_eq!(
        recovered
            .get_version(&committed.object_id, &committed.version)
            .expect("get version"),
        Some(version_record)
    );

    cleanup(&root);
}

#[test]
fn healthy_visible_commit_recovery_refreshes_once_for_all_retained_commits() {
    let root = temp_root("healthy_visible_commit_recovery_refreshes_once");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let replica_node_id = node("node_replica");
    let mut current = object(version("version_1"));
    backend.put_object(&current).expect("initial object");
    for index in 2..=9 {
        let mut committed = current.clone();
        committed.version = version(&format!("version_{index}"));
        committed.content_hash = Some(format!("blake3:grouped-recovery-{index}"));
        committed.content_size_bytes = Some(index);
        committed.current_locations = vec![replica_node_id.clone()];
        let visible_version = DfsObjectVersionRecord {
            content_durability: None,
            metadata_format: crate::DfsRevisionMetadataFormat::Legacy,
            retention: Default::default(),
            module_metadata: Default::default(),
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
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                committed_version: committed.version.clone(),
                owner_node_id: committed.owner_node_id.clone(),
                event_id: EventId::from_legacy_named(format!("evt_grouped_recovery_{index}"))
                    .expect("event"),
            },
            replica_node_id: replica_node_id.clone(),
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_receipt(&receipt).expect("receipt");
        backend
            .commit_visible_version(
                &current.version,
                &committed,
                &visible_version,
                std::slice::from_ref(&replica_node_id),
            )
            .expect("visible commit");
        current = committed;
    }

    assert_eq!(backend.tables.visible_commits.len(), 8);
    let journal_sequence = backend.journal_last_sequence();
    reset_metadata_begin_write_calls();

    backend
        .recover_visible_version_commits()
        .expect("healthy recovery pass");

    assert_eq!(
        metadata_begin_write_calls(),
        1,
        "one healthy recovery pass should refresh once, independent of retained commit count"
    );
    assert_eq!(
        backend.journal_last_sequence(),
        journal_sequence,
        "healthy retained commits must not be re-journaled"
    );
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("current object"),
        Some(current)
    );

    cleanup(&root);
}

#[test]
fn local_backend_recovers_latest_visible_version_without_resurrecting_deleted_objects() {
    let root = temp_root(
        "local_backend_recovers_latest_visible_version_without_resurrecting_deleted_objects",
    );
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    let initial = object(version("version_1"));
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&initial).expect("object");

    let mut version_2 = initial.clone();
    version_2.version = version("version_2");
    version_2.content_hash = Some("blake3:two".to_string());
    let version_2_record = DfsObjectVersionRecord {
        content_durability: None,
        metadata_format: crate::DfsRevisionMetadataFormat::Legacy,
        retention: Default::default(),
        module_metadata: Default::default(),
        file_system_id: version_2.file_system_id.clone(),
        object_id: version_2.object_id.clone(),
        version: version_2.version.clone(),
        content_hash: version_2.content_hash.clone(),
        size_bytes: Some(3),
        locations: version_2.current_locations.clone(),
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    let version_2_receipt = DfsReplicaReceiptRecord {
        receipt: DfsWriteReceipt {
            file_system_id: version_2.file_system_id.clone(),
            object_id: version_2.object_id.clone(),
            committed_version: version_2.version.clone(),
            owner_node_id: node("node_owner"),
            event_id: EventId::from_legacy_named("evt_commit_two").expect("event"),
        },
        replica_node_id: node("node_owner"),
        content_hash: version_2.content_hash.clone(),
        received_at: OffsetDateTime::UNIX_EPOCH,
    };
    backend.put_receipt(&version_2_receipt).expect("receipt");
    backend
        .commit_visible_version(
            &initial.version,
            &version_2,
            &version_2_record,
            std::slice::from_ref(&version_2_receipt.replica_node_id),
        )
        .expect("commit version 2");

    let mut version_3 = version_2.clone();
    version_3.version = version("version_3");
    version_3.content_hash = Some("blake3:three".to_string());
    let version_3_record = DfsObjectVersionRecord {
        content_durability: None,
        metadata_format: crate::DfsRevisionMetadataFormat::Legacy,
        retention: Default::default(),
        module_metadata: Default::default(),
        file_system_id: version_3.file_system_id.clone(),
        object_id: version_3.object_id.clone(),
        version: version_3.version.clone(),
        content_hash: version_3.content_hash.clone(),
        size_bytes: Some(5),
        locations: version_3.current_locations.clone(),
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
        visible: true,
    };
    let version_3_receipt = DfsReplicaReceiptRecord {
        receipt: DfsWriteReceipt {
            file_system_id: version_3.file_system_id.clone(),
            object_id: version_3.object_id.clone(),
            committed_version: version_3.version.clone(),
            owner_node_id: node("node_owner"),
            event_id: EventId::from_legacy_named("evt_commit_three").expect("event"),
        },
        replica_node_id: node("node_owner"),
        content_hash: version_3.content_hash.clone(),
        received_at: OffsetDateTime::UNIX_EPOCH,
    };
    backend.put_receipt(&version_3_receipt).expect("receipt");
    backend
        .commit_visible_version(
            &version_2.version,
            &version_3,
            &version_3_record,
            std::slice::from_ref(&version_3_receipt.replica_node_id),
        )
        .expect("commit version 3");

    backend.put_object(&version_2).expect("stale object");
    drop(backend);
    let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("recovered backend");
    assert_eq!(
        recovered
            .get_object(&version_3.object_id)
            .expect("get recovered")
            .expect("object")
            .version,
        version_3.version
    );
    drop(recovered);

    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let mut tombstone = version_3.clone();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.version = version("version_4");
    tombstone.content_hash = None;
    tombstone.content_size_bytes = None;
    backend.put_object(&tombstone).expect("tombstone");
    drop(backend);

    let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("recovered backend");
    assert_eq!(
        recovered
            .get_object(&tombstone.object_id)
            .expect("get tombstone")
            .expect("object")
            .kind,
        DfsObjectKind::Tombstone
    );

    cleanup(&root);
}

#[test]
fn successful_membership_rebind_checkpoints_current_prefix_for_next_restart() {
    let root = temp_root("membership-handoff-rebind-prefix-checkpoint");
    let shard_id = DfsMetadataShardId::new("root");
    let local_node_id = node("node_owner");
    let retiring_shard = root_shard_config_at_epoch(&shard_id.0, 17);
    let active_shard = root_shard_config_at_epoch(&shard_id.0, 18);
    let source_root = root.join("source");
    let rebound_root = root.join("rebound");
    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), &source_root)
        .expect("source metadata backend");
    let mut rebound = LocalSobjMetadataBackend::new(shard_id.clone(), &rebound_root)
        .expect("rebound metadata backend");

    source
        .put_file_system(&file_system())
        .expect("source filesystem");
    let initial = source
        .journal_batch_since_limited(0, 16, usize::MAX)
        .expect("initial source batch")
        .expect("initial source entries");
    rebound
        .apply_journal_batch(&initial)
        .expect("seed rebound replica");
    let freeze = rebound
        .freeze_metadata_shard_for_membership_handoff(
            &shard_id,
            "handoff-rebind-prefix-checkpoint",
            17,
        )
        .expect("freeze retiring generation")
        .expect("local shard");
    let proof = crate::DfsMetadataShardMembershipHandoffProof {
        handoff_id: "handoff-rebind-prefix-checkpoint".to_string(),
        retiring_shard,
        desired_shard: active_shard.clone(),
        journal_sequence: freeze.journal_sequence,
        state_fingerprint: freeze.state_fingerprint.clone(),
        acknowledged_node_ids: vec![local_node_id.clone()],
    };
    rebound
        .mark_metadata_shard_membership_handoff_activated(
            &shard_id,
            "handoff-rebind-prefix-checkpoint",
            18,
        )
        .expect("mark successor generation activated");

    source
        .put_object(&object(version("version_rebind_prefix_replica")))
        .expect("successor object");
    let batch = source
        .journal_batch_since_limited(initial.next_after_sequence, 16, usize::MAX)
        .expect("successor journal batch")
        .expect("successor journal entries");
    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        file_system_id(),
        shard_id.clone(),
        node("active-successor"),
        18,
    );
    rebound
        .apply_journal_batch_with_replica_permit(&permit, &batch)
        .expect("replicate successor prefix while local authority remains fenced");
    let rebound_prefix = rebound
        .metadata_prefix_proof()
        .expect("current replicated prefix");
    assert!(rebound_prefix.journal_sequence > freeze.journal_sequence);
    assert_ne!(
        rebound
            .metadata_shard_write_freeze(&shard_id)
            .expect("activated marker")
            .and_then(|marker| marker.rebind_prefix_proof),
        Some(rebound_prefix.clone()),
        "replica ingest alone must not bind or advance the local authority marker"
    );

    assert!(
        crate::service_sync::rebind_completed_metadata_shard_membership_handoff(
            &mut rebound,
            &local_node_id,
            &active_shard,
            None,
            Some(&proof),
        )
        .expect("bind validated successor prefix")
    );
    assert_eq!(
        rebound
            .metadata_shard_write_freeze(&shard_id)
            .expect("rebound marker")
            .and_then(|marker| marker.rebind_prefix_proof),
        Some(rebound_prefix),
        "successful authority rebind must durably checkpoint the exact current prefix"
    );

    rebound
        .put_object(&object(version("version_rebind_prefix_first_v2_write")))
        .expect("first write after rebind");
    drop(rebound);

    let mut restarted = LocalSobjMetadataBackend::new(shard_id.clone(), &rebound_root)
        .expect("restart rebound metadata backend");
    assert!(
        restarted
            .metadata_shard_authoritative_writes_frozen(&shard_id)
            .expect("fresh handle remains fenced before proof-bound rebind")
    );
    assert!(
        crate::service_sync::rebind_completed_metadata_shard_membership_handoff(
            &mut restarted,
            &local_node_id,
            &active_shard,
            None,
            Some(&proof),
        )
        .expect("proof-bound rebind after the first post-handoff write")
    );
    restarted
        .put_object(&object(version("version_rebind_prefix_second_restart")))
        .expect("second restart may append after rebind");

    cleanup(&root);
}

#[test]
fn table_checkpoint_preserves_compact_terminal_namespace_history() {
    let root = temp_root("terminal_namespace_history_checkpoint");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system();
    let current = object(version("version_namespace_receipt_1"));
    let mut committed = current.clone();
    committed.version = version("version_namespace_receipt_2");
    committed.mode = 0o600;

    let now = OffsetDateTime::now_utc();
    let mut pending_request = namespace_request(91, DfsNamespaceRequestStatus::Pending);
    pending_request.created_at = now;
    pending_request.updated_at = now;
    let mut completed_request = pending_request.clone();
    completed_request.status = DfsNamespaceRequestStatus::Completed;
    completed_request.result = Some(DfsNamespaceResult::Applied);
    let mut prepare = namespace_prepare(
        &format!("namespace-request:{}:commit", pending_request.operation_id),
        "root",
        &current,
        committed,
    );
    prepare.transaction.created_at = OffsetDateTime::UNIX_EPOCH;
    prepare.completed_request = Some(completed_request.clone());
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("canonical request-owned prepare digest");
    let transaction = prepare.transaction.clone();

    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .put_namespace_request_on_shard(&shard_id, &pending_request)
        .expect("pending request");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("hidden prepare");
    let commit_entry = backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("commit prepare");

    assert!(
        backend
            .get_namespace_transaction(&transaction.transaction_id)
            .expect("durable transaction lookup")
            .is_none()
    );
    assert!(
        backend
            .get_namespace_prepare(&prepare.operation_id)
            .expect("durable prepare lookup")
            .is_none()
    );
    let durable_transaction_receipt = backend
        .tables
        .namespace_transaction_receipts
        .get(&transaction.transaction_id)
        .cloned()
        .expect("durable compact transaction receipt");
    let durable_prepare_receipt = backend
        .tables
        .namespace_prepare_receipts
        .get(&prepare.operation_id)
        .cloned()
        .expect("durable compact prepare receipt");
    assert_eq!(
        durable_transaction_receipt.retained_at, commit_entry.created_at,
        "transaction retry retention starts at the authoritative commit"
    );
    assert_eq!(
        durable_prepare_receipt.retained_at, commit_entry.created_at,
        "prepare retry retention starts at the authoritative commit"
    );
    let durable_request = backend
        .get_namespace_request(
            &completed_request.requester_node_id,
            &completed_request.request_id,
        )
        .expect("durable request lookup")
        .expect("full durable request");
    backend
        .compact_metadata_table_log_if_needed(0)
        .expect("force exact table checkpoint");

    assert!(
        backend
            .get_namespace_transaction(&transaction.transaction_id)
            .expect("checkpoint transaction lookup")
            .is_none()
    );
    assert!(
        backend
            .get_namespace_prepare(&prepare.operation_id)
            .expect("checkpoint prepare lookup")
            .is_none()
    );
    assert_eq!(
        backend
            .get_namespace_request(
                &completed_request.requester_node_id,
                &completed_request.request_id,
            )
            .expect("checkpoint request lookup"),
        Some(durable_request.clone())
    );
    assert_eq!(
        backend
            .tables
            .namespace_transaction_receipts
            .get(&transaction.transaction_id),
        Some(&durable_transaction_receipt)
    );
    assert_eq!(
        backend
            .tables
            .namespace_prepare_receipts
            .get(&prepare.operation_id),
        Some(&durable_prepare_receipt)
    );
    assert!(backend.tables.namespace_request_receipts.is_empty());

    drop(backend);
    let mut reopened =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopen checkpoint");
    assert!(
        reopened
            .get_namespace_transaction(&transaction.transaction_id)
            .expect("reopened transaction lookup")
            .is_none()
    );
    assert!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("reopened prepare lookup")
            .is_none()
    );
    assert_eq!(
        reopened
            .tables
            .namespace_transaction_receipts
            .get(&transaction.transaction_id),
        Some(&durable_transaction_receipt)
    );
    assert_eq!(
        reopened
            .tables
            .namespace_prepare_receipts
            .get(&prepare.operation_id),
        Some(&durable_prepare_receipt)
    );
    assert_eq!(
        reopened
            .get_namespace_request(
                &completed_request.requester_node_id,
                &completed_request.request_id,
            )
            .expect("reopened request lookup"),
        Some(durable_request)
    );

    let commit_retry = reopened
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("exact committed prepare retry");
    assert!(matches!(
        commit_retry.mutation,
        DfsMetadataMutation::PutNamespacePrepareReceipt { .. }
    ));
    let prepare_retry = reopened
        .prepare_namespace_transaction(&prepare)
        .expect("exact prepared body retry from compact receipt");
    assert!(matches!(
        prepare_retry.mutation,
        DfsMetadataMutation::PutNamespacePrepareReceipt { .. }
    ));
    let request_retry = reopened
        .put_namespace_request_on_shard(&shard_id, &completed_request)
        .expect("exact completed request retry");
    assert!(matches!(
        request_retry.mutation,
        DfsMetadataMutation::PutNamespaceRequest { .. }
    ));
    let transaction_retry = reopened
        .apply_namespace_transaction(&transaction)
        .expect("exact materialized transaction retry");
    assert!(matches!(
        transaction_retry.mutation,
        DfsMetadataMutation::PutNamespaceTransactionReceipt { .. }
    ));

    let mut conflicting_transaction = transaction.clone();
    conflicting_transaction.objects[0].mode ^= 0o111;
    assert!(
        reopened
            .apply_namespace_transaction(&conflicting_transaction)
            .expect_err("transaction id reuse must remain fenced")
            .to_string()
            .contains("different payload")
    );
    assert!(
        reopened
            .commit_prepared_namespace_transaction(
                &prepare.operation_id,
                "blake3:different-prepare",
                prepare.owner_epoch,
            )
            .expect_err("prepare digest reuse must remain fenced")
            .to_string()
            .contains("fence mismatch")
    );
    let mut conflicting_request = completed_request;
    conflicting_request.command_digest = "blake3:different-request".to_string();
    assert!(
        reopened
            .put_namespace_request_on_shard(&shard_id, &conflicting_request)
            .expect_err("request id reuse must remain fenced")
            .to_string()
            .contains("different payload")
    );

    cleanup(&root);
}

#[test]
fn metadata_history_gc_defers_full_checkpoint_until_bounded_drain_tail() {
    let root = temp_root("metadata-history-gc-deferred-checkpoint");
    let shard_id = DfsMetadataShardId::new("root");
    let current = object(version("version_gc_deferred_current"));
    let first_old = object_version_record(
        &current,
        version("version_gc_deferred_old_1"),
        OffsetDateTime::UNIX_EPOCH,
    );
    let second_old = object_version_record(
        &current,
        version("version_gc_deferred_old_2"),
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
    );
    let current_record = object_version_record(
        &current,
        current.version.clone(),
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(2),
    );
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend.put_version(&first_old).expect("first old version");
    backend
        .put_version(&second_old)
        .expect("second old version");
    backend
        .put_version(&current_record)
        .expect("current version");
    backend
        .compact_metadata_table_log_if_needed(0)
        .expect("baseline checkpoint");
    let snapshot_path = metadata_table_snapshot_path(&root);
    let snapshot_before = fs::read(&snapshot_path).expect("baseline snapshot bytes");

    let partial = backend
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(3_000_000_000)
                .with_max_removals(1),
        )
        .expect("partial bounded GC pass");
    assert!(partial.max_removals_reached);
    assert_eq!(partial.removed_version_count, 1);
    assert_eq!(
        fs::read(&snapshot_path).expect("snapshot after partial pass"),
        snapshot_before,
        "a partial drain must rely on its durable journal/table suffix instead of serializing the full shard"
    );

    drop(backend);
    let mut reopened =
        LocalSobjMetadataBackend::new(shard_id, &root).expect("replay partial GC suffix");
    assert_eq!(reopened.tables.versions.len(), 2);
    let tail = reopened
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(3_000_000_000)
                .with_max_removals(1),
        )
        .expect("bounded GC tail");
    assert!(!tail.max_removals_reached);
    assert_eq!(tail.removed_version_count, 1);
    assert_eq!(reopened.tables.versions.len(), 1);
    assert_ne!(
        fs::read(&snapshot_path).expect("snapshot after drain tail"),
        snapshot_before,
        "the final batch must publish the authoritative post-GC checkpoint"
    );

    cleanup(&root);
}
