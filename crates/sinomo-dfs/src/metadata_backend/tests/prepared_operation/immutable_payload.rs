use super::*;

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
