use super::*;

#[test]
fn native_replica_validation_restores_all_rows_after_success_and_late_rejection() {
    let (runtime, mut service, config) = native_case("native-replica-validation-rollback", 1);
    directory(&mut service, "/services/validation-rollback");
    let shard = DfsMetadataShardId::new(&config.shard_id);
    let root =
        crate::local_dfs_metadata_shard_path(runtime.data_dir(&config.primary_node_id), &shard);
    // An actual earlier inspection cut supplies tables, never author authority.
    // Validation below is in-memory only; native Main produces every input.
    let mut earlier = LocalSobjMetadataBackend::open_for_inspection(shard.clone(), &root).unwrap();
    let before_sequence = earlier.journal_last_sequence();
    let before_tables = earlier.tables.clone();
    let before_prefix = earlier.prefix_proof.clone();
    let before_materialized = earlier.materialized_through_sequence;
    let first = directory(&mut service, "/services/validation-rollback/first");
    let second = directory(&mut service, "/services/validation-rollback/second");
    let suffix = entries(&service, &shard)
        .into_iter()
        .filter(|entry| entry.sequence > before_sequence)
        .collect::<Vec<_>>();
    assert!(suffix.len() >= 4, "two actual prepare/commit pairs");
    let prepare_index = suffix
        .iter()
        .rposition(|entry| {
            matches!(
                entry.mutation,
                DfsMetadataMutation::PrepareOperationV7 { .. }
            )
        })
        .unwrap();
    let DfsMetadataMutation::PrepareOperationV7 { record, .. } = &suffix[prepare_index].mutation
    else {
        unreachable!()
    };
    let digest = record.digest.clone();
    let operation = crate::DfsMetadataAuthorityOperation::PreparedOperation {
        operation_id: digest.operation_id.clone(),
        file_system_id: digest.file_system_id.clone(),
        shard_id: digest.shard_id.clone(),
        owner_epoch: digest.owner_epoch,
        prepare_sequence: digest.prepare_sequence,
        payload_digest: digest.payload_digest.clone(),
    };
    let assert_restored = |backend: &LocalSobjMetadataBackend| {
        assert_eq!(
            backend.tables, before_tables,
            "all rows and derived indexes remain exact"
        );
        assert_eq!(backend.journal_last_sequence(), before_sequence);
        assert_eq!(backend.materialized_through_sequence, before_materialized);
        assert_eq!(backend.prefix_proof, before_prefix);
        assert!(!backend.tables.objects.contains_key(&first.object_id));
        assert!(!backend.tables.objects.contains_key(&second.object_id));
    };
    for _ in 0..2 {
        assert_eq!(
            earlier
                .validate_replica_journal_suffix(&suffix, Some(&operation), config.epoch)
                .unwrap(),
            Some(digest.clone())
        );
        assert_restored(&earlier);
    }
    // The whole batch has passed checked replay before this proof rejection.
    let mut wrong_operation = operation.clone();
    let crate::DfsMetadataAuthorityOperation::PreparedOperation { owner_epoch, .. } =
        &mut wrong_operation;
    *owner_epoch += 1;
    assert!(
        earlier
            .validate_replica_journal_suffix(&suffix, Some(&wrong_operation), config.epoch)
            .is_err()
    );
    assert_restored(&earlier);
    let mut missing_prepare = suffix.clone();
    missing_prepare.remove(prepare_index);
    assert!(
        earlier
            .validate_replica_journal_suffix(&missing_prepare, None, config.epoch)
            .is_err()
    );
    assert_restored(&earlier);
    let mut changed_commit = suffix.clone();
    let DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } =
        &mut changed_commit.last_mut().unwrap().mutation
    else {
        panic!("actual terminal native commit");
    };
    digest.payload_digest.push_str("-corrupt");
    assert!(
        earlier
            .validate_replica_journal_suffix(&changed_commit, None, config.epoch)
            .is_err()
    );
    assert_restored(&earlier);
    assert_eq!(
        earlier
            .validate_replica_journal_suffix(&suffix, None, config.epoch)
            .unwrap(),
        None
    );
    assert_restored(&earlier);
    // An old-style current-epoch producer omits counts. Its immutable body
    // remains structurally decodable, but fresh native ingress cannot accept
    // that body and erase the actual count-bearing predecessor.
    let mut missing_count = suffix.clone();
    let DfsMetadataMutation::PrepareOperationV7 { record, .. } = &mut missing_count[prepare_index].mutation else { unreachable!() };
    let mut payload = (*record.payload).clone();
    payload.transaction.objects[0].namespace_link_count = None;
    record.payload = std::sync::Arc::new(payload);
    record.digest = crate::DfsPreparedOperationDigest::at_sequence(&record.payload, record.digest.prepare_sequence).unwrap();
    record.validate().unwrap();
    let changed_digest = record.digest.clone();
    let DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } = &mut missing_count.last_mut().unwrap().mutation else { unreachable!() };
    *digest = changed_digest;
    assert!(earlier.validate_replica_journal_suffix(&missing_count, None, config.epoch).is_err());
    assert_restored(&earlier);
    // Neither failed nor successful validation alters the actual producer.
    assert_eq!(
        directory(&mut service, "/services/validation-rollback/first"),
        first
    );
    assert_eq!(
        directory(&mut service, "/services/validation-rollback/second"),
        second
    );
    drop((earlier, service));
    runtime.close().unwrap();
}

#[test]
fn native_commit_body_lookup_uses_exact_resident_content_without_current_view() {
    let (runtime, mut service, config) = native_case("native-immutable-commit-body", 1);
    directory(&mut service, "/services/body-lookup");
    let shard = DfsMetadataShardId::new("services");
    let root =
        crate::local_dfs_metadata_shard_path(runtime.data_dir(&config.primary_node_id), &shard);
    let stale_reader = LocalSobjMetadataBackend::open_for_inspection(shard.clone(), &root).unwrap();
    let before = stale_reader.journal_last_sequence();
    let stat = service
        .posix_write_file(
            "/services/body-lookup/actual",
            b"actual immutable committed content",
            crate::DfsWriteOptions {
                policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![service.node_id().clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let reader = LocalSobjMetadataBackend::open_for_inspection(shard.clone(), &root).unwrap();
    let actual = reader
        .tables
        .prepared_operations_v7
        .values()
        .find(|record| {
            record.terminal_intent.as_ref().is_some_and(|intent| {
                intent.object_id == stat.object_id && intent.target_version == stat.version
            })
        })
        .unwrap()
        .clone();
    let commit = actual.committed_entry.as_ref().unwrap();
    let mut batch = service
        .metadata_backend()
        .unwrap()
        .journal_batch_for_shard_since_limited(
            &shard,
            before,
            crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    batch
        .entries
        .retain(|entry| entry.sequence == commit.sequence);
    assert_eq!(
        batch.entries.len(),
        1,
        "actual compact commit without its inline prepare"
    );
    crate::metadata_backend::reset_metadata_current_tables_snapshot_calls();
    let payloads =
        crate::metadata_replication::committed_payloads_for_batch(&reader, &batch).unwrap();
    let current_view_calls = crate::metadata_backend::metadata_current_tables_snapshot_calls();
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0], actual.payload);

    // A genuinely older inspection view lacks this body. Its normal current
    // reader still supplies the actual later native record when needed.
    assert!(
        !stale_reader
            .tables
            .prepared_operations_v7
            .contains_key(&actual.operation_id)
    );
    crate::metadata_backend::reset_metadata_current_tables_snapshot_calls();
    let fallback =
        crate::metadata_replication::committed_payloads_for_batch(&stale_reader, &batch).unwrap();
    assert_eq!(fallback, payloads);
    assert!(crate::metadata_backend::metadata_current_tables_snapshot_calls() > 0);

    let mut changed = batch.clone();
    let DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } =
        &mut changed.entries[0].mutation
    else {
        unreachable!()
    };
    digest.owner_epoch += 1;
    assert!(crate::metadata_replication::committed_payloads_for_batch(&reader, &changed).is_err());
    let mut changed = batch.clone();
    let DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } =
        &mut changed.entries[0].mutation
    else {
        unreachable!()
    };
    digest.shard_id = DfsMetadataShardId::new("foreign");
    assert!(crate::metadata_replication::committed_payloads_for_batch(&reader, &changed).is_err());
    let mut wrong_slot = actual.digest.clone();
    wrong_slot.prepare_sequence -= 1;
    assert!(
        reader
            .prepared_operation_payload_for_digest(&wrong_slot)
            .is_err()
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .prepared_operation_payload_for_digest(&actual.digest)
            .unwrap(),
        Some(Arc::clone(&actual.payload)),
    );
    // Current-state callers still refresh the real current view.
    crate::metadata_backend::reset_metadata_current_tables_snapshot_calls();
    assert_eq!(
        reader
            .prepared_operation_record(&actual.operation_id)
            .unwrap(),
        Some(actual.clone())
    );
    assert!(crate::metadata_backend::metadata_current_tables_snapshot_calls() > 0);
    // Content integrity remains checked even when the resident digest matches.
    let mut corrupt_reader = reader.clone();
    Arc::make_mut(
        &mut corrupt_reader
            .tables
            .prepared_operations_v7
            .get_mut(&actual.operation_id)
            .unwrap()
            .payload,
    )
    .owner_epoch += 1;
    assert!(
        corrupt_reader
            .prepared_operation_payload_for_digest(&actual.digest)
            .is_err()
    );
    drop(corrupt_reader);
    std::fs::write(
        runtime.data_dir(&config.primary_node_id).root()
            .join("boundary-diagnostic-immutable-commit-body.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "original_prepare": actual.digest, "commit": commit,
            "current_view_calls_for_resident_body": current_view_calls,
            "missing_resident_body_used_real_current_fallback": true,
            "changed_epoch_and_scope_rejected": true,
            "wrong_prepare_slot_and_corrupt_resident_body_rejected": true,
            "current_record_read_still_refreshes": true,
            "qualification": "Actual native immutable body and commit. No authority or current-state claim follows from returning its exact content."
        })).unwrap(),
    ).unwrap();
    drop((reader, stale_reader, service));
    runtime.close().unwrap();
    assert_eq!(
        current_view_calls, 0,
        "immutable body classification must not rebuild unrelated current tables"
    );
}
