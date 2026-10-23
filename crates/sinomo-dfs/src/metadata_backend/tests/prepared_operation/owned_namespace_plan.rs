use super::*;

#[test]
fn native_commit_replay_uses_owned_namespace_plan_and_exact_rollback() {
    let (runtime, mut service, config) = native_case("native-owned-namespace-plan", 1);
    directory(&mut service, "/services/owned-plan");
    let options = crate::DfsWriteOptions {
        policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
        replica_candidates: vec![service.node_id().clone()],
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    for index in 0..32 {
        service
            .posix_write_file(
                &format!("/services/owned-plan/history-{index}"),
                b"unrelated retained history",
                options.clone(),
            )
            .unwrap();
    }
    let shard = DfsMetadataShardId::new("services");
    let root =
        crate::local_dfs_metadata_shard_path(runtime.data_dir(&config.primary_node_id), &shard);
    let before = crate::metadata_table_store::read_metadata_table_state_lenient(&root).unwrap();
    let before_sequence = before.materialized_through_sequence.unwrap();
    let stat = service
        .posix_write_file(
            "/services/owned-plan/target",
            b"the actual file and parent commit",
            options,
        )
        .unwrap();
    let after = crate::metadata_table_store::read_metadata_table_state_lenient(&root).unwrap();
    let actual = after
        .tables
        .prepared_operations_v7
        .values()
        .find(|record| {
            record.terminal_intent.as_ref().is_some_and(|intent| {
                intent.object_id == stat.object_id && intent.target_version == stat.version
            })
        })
        .expect("actual complete native write");
    assert!(
        actual.transaction.objects.len() >= 2,
        "real file and parent effects"
    );
    let batch = service
        .metadata_backend()
        .unwrap()
        .journal_batch_for_shard_since_limited(
            &shard,
            before_sequence,
            crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        Some(batch.next_after_sequence),
        after.materialized_through_sequence
    );
    let mut replay = before.tables.clone();
    let mut rollback = crate::metadata_tables::DfsMetadataTableRollback::default();
    let clone_before =
        crate::metadata_namespace_transaction::namespace_reference_plan_clone_count();
    for entry in &batch.entries {
        require_journal_entry_replay_payloads(entry).unwrap();
        let op = match &entry.mutation {
            DfsMetadataMutation::PrepareOperationV7 { record, .. } => {
                DfsMetadataTableOp::PutPreparedOperationV7 {
                    record: record.clone(),
                }
            }
            DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } => {
                DfsMetadataTableOp::CommitPreparedOperationV7 {
                    digest: digest.clone(),
                    committed_entry: Box::new(
                        crate::DfsPreparedOperationCommitReference::from_journal_entry(entry)
                            .unwrap(),
                    ),
                }
            }
        };
        replay
            .apply_checked_with_rollback(op, &mut rollback)
            .unwrap();
    }
    let full_clones = crate::metadata_namespace_transaction::namespace_reference_plan_clone_count()
        - clone_before;
    assert_eq!(
        replay, after.tables,
        "checked replay equals the real native committed state"
    );
    let duplicate = DfsMetadataTableOp::CommitPreparedOperationV7 {
        digest: actual.digest.clone(),
        committed_entry: actual.committed_entry.clone().unwrap(),
    };
    replay.apply_checked(duplicate).unwrap();
    assert_eq!(
        replay, after.tables,
        "exact committed replay stays idempotent"
    );
    let mut changed_entry = actual.committed_entry.clone().unwrap();
    changed_entry.sequence += 1;
    assert!(
        replay
            .apply_checked(DfsMetadataTableOp::CommitPreparedOperationV7 {
                digest: actual.digest.clone(),
                committed_entry: changed_entry,
            })
            .is_err(),
        "another commit slot cannot reuse the same operation"
    );
    assert_eq!(replay, after.tables, "rejected replay changes no state");
    rollback.rollback(&mut replay);
    assert_eq!(
        replay, before.tables,
        "undo restores every original row and index"
    );
    std::fs::write(
        runtime.data_dir(&config.primary_node_id).root()
            .join("boundary-diagnostic-owned-namespace-plan.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "original_prepare": actual.digest,
            "commit": actual.committed_entry,
            "objects_in_transaction": actual.transaction.objects.len(),
            "before_sequence": before_sequence,
            "after_sequence": after.materialized_through_sequence,
            "journal_entries": batch.entries.len(),
            "unrelated_prepared_history": before.tables.prepared_operations_v7.len(),
            "reference_planner_full_clones": full_clones,
            "exact_replay_duplicate_rejection_and_rollback": true,
            "qualification": "Real native write; pure checked replay of its exact durable suffix. No authority or positive receipt supplied."
        })).unwrap(),
    ).unwrap();
    drop(service);
    runtime.close().unwrap();
    assert_eq!(
        full_clones, 0,
        "owned commit must not copy unrelated table history"
    );
}
