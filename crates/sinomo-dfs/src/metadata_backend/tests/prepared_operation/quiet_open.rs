use super::*;

#[test]
fn clean_native_reopen_keeps_a_lagging_scheduling_hint_unchanged() {
    let (runtime, mut service, config) = native_case("native-clean-reopen-cursor", 1);
    let written = service
        .posix_write_file(
            "/services/clean-reopen-cursor",
            b"durable native content",
            crate::DfsWriteOptions::replicated_eventual(vec![config.primary_node_id.clone()]),
        )
        .unwrap();
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let held = service
        .metadata_backend()
        .unwrap()
        .native_held_snapshot_for_scope(&config.file_system_id, &shard)
        .unwrap();
    let root = held.backend_root.clone();
    drop(service);
    let backend = LocalSobjMetadataBackend::new(shard.clone(), &root).unwrap();
    let exact = backend.prefix_proof.clone().unwrap();
    assert!(exact.journal_sequence > 0);
    assert_eq!(
        backend.materialized_through_sequence,
        exact.journal_sequence
    );
    assert_eq!(backend.next_sequence, exact.journal_sequence + 1);
    assert_eq!(
        backend
            .tables
            .objects
            .get(&written.object_id)
            .unwrap()
            .version,
        written.version
    );
    drop(backend);

    // A lagging scheduling hint is permitted by the existing 512-entry flush
    // policy. It is not a journal, materialized-prefix proof, ACK, or writer grant.
    crate::metadata_cursor::write_local_materialization_cursor(
        &root,
        &crate::metadata_journal::DfsMetadataReplayCursor {
            shard_id: shard.clone(),
            last_sequence: exact.journal_sequence - 1,
        },
    )
    .unwrap();
    let cursor = crate::metadata_cursor::local_materialization_cursor_path(&root);
    let before = std::fs::read(&cursor).unwrap();
    let before_mark = std::fs::metadata(&cursor).unwrap().modified().unwrap();
    let tables_before =
        crate::metadata_table_store::metadata_table_storage_generation(&root).unwrap();
    let reopened = LocalSobjMetadataBackend::new(shard, &root).unwrap();
    assert_eq!(reopened.prefix_proof.as_ref(), Some(&exact));
    assert_eq!(
        reopened.materialized_through_sequence,
        exact.journal_sequence
    );
    assert_eq!(reopened.next_sequence, exact.journal_sequence + 1);
    assert_eq!(
        reopened
            .tables
            .objects
            .get(&written.object_id)
            .unwrap()
            .version,
        written.version
    );
    drop(reopened);
    let tables_after =
        crate::metadata_table_store::metadata_table_storage_generation(&root).unwrap();
    let after = std::fs::read(&cursor).unwrap();
    let after_mark = std::fs::metadata(&cursor).unwrap().modified().unwrap();
    // The fixture owns its temporary data root. Drain it only after retaining
    // the observation, so closing the native runtime cannot remove our input.
    runtime.close().unwrap();
    assert_eq!(tables_after, tables_before);
    assert_eq!(
        after, before,
        "a clean handle reopen must not persist a scheduling-only cursor advance"
    );
    assert_eq!(after_mark, before_mark);
}
