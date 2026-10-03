//! Authored group-sync recovery and namespace-commit terminalization.
use super::*;
use crate::metadata_journal::{
    JournalAppendFault, fail_journal_append_for_test, fail_journal_certifications_for_test,
    journal_append_sync_calls_for_test, reset_journal_append_sync_calls_for_test,
};

fn fixture(
    root: &Path,
) -> (
    LocalSobjMetadataBackend,
    DfsObjectMetadata,
    DfsObjectMetadata,
    DfsNamespacePrepareRecord,
    DfsWriteIntentRecord,
) {
    let shard = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), root).unwrap();
    backend.put_file_system(&file_system()).unwrap();
    let mut parent = object(version("final_batch_parent_base"));
    parent.object_id = DfsObjectId::from_legacy_named("final_batch_parent").unwrap();
    parent.path = "/".into();
    parent.kind = DfsObjectKind::Directory;
    parent.content_hash = None;
    parent.content_size_bytes = None;
    backend.put_object(&parent).unwrap();
    let mut hidden_child = object(version("final_batch_hidden_child"));
    hidden_child.path = "/created.txt".into();
    hidden_child.content_hash = None;
    let mut committed = hidden_child.clone();
    committed.version = version("final_batch_target");
    committed.content_hash = Some(crate::content_store::content_hash(b"final-batch"));
    committed.content_size_bytes = Some(11);
    let pending = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "intent-final-batch".into(),
        file_system_id: committed.file_system_id.clone(),
        object_id: committed.object_id.clone(),
        base_version: None,
        target_version: committed.version.clone(),
        coordinator_node_id: committed.owner_node_id.clone(),
        operation_id: Some("operation-final-batch".into()),
        operation_order_token: None,
        owner_epoch: 7,
        desired_replica_node_ids: committed.current_locations.clone(),
        policy: DfsReplicationPolicy::new(1, 1).unwrap(),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: committed.content_hash.clone(),
        object_metadata: Some(hidden_child.clone()),
        touch_parent_on_create: true,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: None,
    };
    let receipt = replica_receipt_record(
        &committed,
        committed.version.clone(),
        OffsetDateTime::UNIX_EPOCH,
    );
    backend
        .put_intent_and_receipts_for_object(Some(&hidden_child), &pending, &[receipt])
        .unwrap();
    let mut touched_parent = parent.clone();
    touched_parent.version = version("final_batch_parent_touched");
    touched_parent.modified_at_unix_nanos = 1;
    touched_parent.changed_at_unix_nanos = 1;
    touched_parent.last_modified_by_event_id = Some(EventId::generate());
    let mut parent_version = object_version_record(
        &touched_parent,
        touched_parent.version.clone(),
        OffsetDateTime::UNIX_EPOCH,
    );
    parent_version.created_by_event_id = touched_parent.last_modified_by_event_id.clone();
    let mut prepare = namespace_prepare(
        "operation-final-batch",
        "root",
        &parent,
        touched_parent.clone(),
    );
    prepare
        .expected_object_versions
        .push(DfsNamespaceObjectVersionExpectation {
            object_id: committed.object_id.clone(),
            expected_version: None,
        });
    prepare
        .expected_path_occupants
        .push(DfsNamespacePathOccupantExpectation {
            path: committed.path.clone(),
            expected_object_id: None,
        });
    prepare.transaction.objects = vec![committed.clone(), touched_parent];
    prepare.transaction.versions = vec![
        object_version_record(
            &committed,
            committed.version.clone(),
            OffsetDateTime::UNIX_EPOCH,
        ),
        parent_version,
    ];
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare).unwrap();
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV2);
    backend.prepare_namespace_transaction(&prepare).unwrap();
    assert!(
        backend.get_object(&committed.object_id).unwrap().is_none(),
        "prepared child must remain absent"
    );
    assert_eq!(
        backend.get_object(&parent.object_id).unwrap(),
        Some(parent.clone())
    );
    let mut terminal = pending;
    terminal.status = DfsWriteIntentStatus::Committed;
    terminal.object_metadata = Some(committed.clone());
    terminal.updated_at = Some(OffsetDateTime::UNIX_EPOCH);
    (backend, parent, committed, prepare, terminal)
}

fn commit(
    backend: &mut LocalSobjMetadataBackend,
    object: &DfsObjectMetadata,
    prepare: &DfsNamespacePrepareRecord,
    intent: &DfsWriteIntentRecord,
) -> Result<Vec<DfsMetadataJournalEntry>> {
    backend.commit_prepared_namespace_transaction_and_intent_on_shard(
        &prepare.shard_id,
        &prepare.operation_id,
        &prepare.operation_digest,
        prepare.owner_epoch,
        object,
        intent,
    )
}

fn assert_complete(
    backend: &LocalSobjMetadataBackend,
    object: &DfsObjectMetadata,
    terminal: &DfsWriteIntentRecord,
) {
    assert_eq!(
        backend.get_object(&object.object_id).unwrap().as_ref(),
        Some(object)
    );
    assert_eq!(
        backend
            .get_intent(&object.object_id, &terminal.intent_id)
            .unwrap()
            .unwrap()
            .status,
        DfsWriteIntentStatus::Committed
    );
    let entries = backend.journal_since(0).unwrap();
    assert!(
        entries
            .windows(2)
            .all(|pair| pair[1].sequence == pair[0].sequence + 1)
    );
    assert_eq!(
        backend.metadata_prefix_proof().unwrap().journal_sequence,
        backend.journal_last_sequence()
    );
}

#[test]
fn final_namespace_commit_and_terminal_intent_share_one_sync_and_replay_after_table_loss() {
    let root = temp_root("final_namespace_batch");
    let (mut backend, parent, committed, prepare, terminal) = fixture(&root);
    let touched_parent = prepare
        .transaction
        .objects
        .iter()
        .find(|object| object.object_id == parent.object_id)
        .unwrap()
        .clone();
    reset_journal_append_sync_calls_for_test();
    let entries = commit(&mut backend, &committed, &prepare, &terminal).unwrap();
    assert_eq!(entries.len(), 2);
    assert!(matches!(
        entries[0].mutation,
        DfsMetadataMutation::CommitNamespacePrepare { .. }
    ));
    assert!(matches!(
        entries[1].mutation,
        DfsMetadataMutation::PutIntent {
            status: DfsWriteIntentStatus::Committed,
            ..
        }
    ));
    assert_eq!(journal_append_sync_calls_for_test(), 1);
    assert_eq!(
        backend.get_object(&parent.object_id).unwrap(),
        Some(touched_parent.clone())
    );
    assert!(
        backend
            .get_version(&committed.object_id, &committed.version)
            .unwrap()
            .is_some()
    );
    assert!(
        backend
            .get_version(&touched_parent.object_id, &touched_parent.version)
            .unwrap()
            .is_some()
    );
    assert_complete(&backend, &committed, &terminal);
    drop(backend);
    fs::remove_file(metadata_table_path(&root)).unwrap();
    crate::metadata_journal::admission::forget_for_test(&root);
    let reopened = LocalSobjMetadataBackend::new(prepare.shard_id, &root).unwrap();
    assert_complete(&reopened, &committed, &terminal);
    assert_eq!(
        reopened.get_object(&parent.object_id).unwrap(),
        Some(touched_parent)
    );
    cleanup(&root);
}

#[test]
fn authored_final_batch_recovers_every_complete_and_partial_frame_cut_without_sequence_reuse() {
    for (complete_frames, partial_frame_bytes) in
        [(0, 0), (0, 2), (0, 7), (1, 0), (1, 2), (1, 7), (2, 0)]
    {
        let root = temp_root("final_namespace_batch_cut");
        let (mut backend, _, committed, prepare, terminal) = fixture(&root);
        fail_journal_append_for_test(JournalAppendFault {
            skip_appends: 0,
            complete_frames,
            partial_frame_bytes,
        });
        assert!(commit(&mut backend, &committed, &prepare, &terminal).is_err());
        commit(&mut backend, &committed, &prepare, &terminal).unwrap();
        assert_complete(&backend, &committed, &terminal);
        drop(backend);
        crate::metadata_journal::admission::forget_for_test(&root);
        let reopened = LocalSobjMetadataBackend::new(prepare.shard_id, &root).unwrap();
        assert_complete(&reopened, &committed, &terminal);
        cleanup(&root);
    }
}

#[test]
fn failed_authored_recovery_fences_other_handle_reopen_and_ack_floor_until_sync_succeeds() {
    let root = temp_root("authored_root_uncertainty");
    let (mut backend, _, committed, prepare, terminal) = fixture(&root);
    let mut other = LocalSobjMetadataBackend::new(prepare.shard_id.clone(), &root).unwrap();
    let prefix = backend.metadata_prefix_proof().unwrap();
    let floor = DfsMetadataPrefixAckFloor {
        shard_id: prepare.shard_id.clone(),
        journal_sequence: prefix.journal_sequence,
        state_fingerprint: String::new(),
        durable_prefix_proof: Some(prefix),
    };
    fail_journal_append_for_test(JournalAppendFault {
        skip_appends: 0,
        complete_frames: 1,
        partial_frame_bytes: 0,
    });
    fail_journal_certifications_for_test(4);
    assert!(commit(&mut backend, &committed, &prepare, &terminal).is_err());
    assert!(other.put_metadata_prefix_ack_floor(&floor).is_err());
    assert!(commit(&mut other, &committed, &prepare, &terminal).is_err());
    drop(backend);
    assert!(LocalSobjMetadataBackend::new(prepare.shard_id.clone(), &root).is_err());
    let mut reopened = LocalSobjMetadataBackend::new(prepare.shard_id.clone(), &root).unwrap();
    commit(&mut reopened, &committed, &prepare, &terminal).unwrap();
    assert_complete(&reopened, &committed, &terminal);
    cleanup(&root);
}

#[test]
fn final_batch_rejects_changed_terminal_identity_before_append() {
    let root = temp_root("final_batch_rejections");
    let (mut backend, current, committed, prepare, terminal) = fixture(&root);
    let before = backend.journal_last_sequence();
    let mut changed = terminal.clone();
    changed.coordinator_node_id = node("other-coordinator");
    assert!(commit(&mut backend, &committed, &prepare, &changed).is_err());
    changed = terminal.clone();
    changed.owner_epoch += 1;
    assert!(commit(&mut backend, &committed, &prepare, &changed).is_err());
    changed = terminal.clone();
    changed.policy = DfsReplicationPolicy::new(2, 2).unwrap();
    assert!(commit(&mut backend, &committed, &prepare, &changed).is_err());
    let mut wrong = prepare.clone();
    wrong.operation_digest = "wrong-digest".into();
    assert!(commit(&mut backend, &committed, &wrong, &terminal).is_err());
    assert_eq!(backend.journal_last_sequence(), before);
    assert_eq!(
        backend.get_object(&current.object_id).unwrap(),
        Some(current)
    );
    cleanup(&root);
}

#[test]
fn final_batch_committed_retry_and_aborted_prepare_keep_existing_serial_contract() {
    let root = temp_root("final_batch_retry");
    let (mut backend, _, committed, prepare, terminal) = fixture(&root);
    commit(&mut backend, &committed, &prepare, &terminal).unwrap();
    reset_journal_append_sync_calls_for_test();
    commit(&mut backend, &committed, &prepare, &terminal).unwrap();
    assert_eq!(
        journal_append_sync_calls_for_test(),
        2,
        "committed retry keeps its established serial record forms"
    );
    assert_complete(&backend, &committed, &terminal);
    cleanup(&root);
    let root = temp_root("final_batch_aborted");
    let (mut backend, current, committed, prepare, terminal) = fixture(&root);
    backend
        .abort_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .unwrap();
    let before = backend.journal_last_sequence();
    assert!(commit(&mut backend, &committed, &prepare, &terminal).is_err());
    assert_eq!(backend.journal_last_sequence(), before);
    assert_eq!(
        backend.get_object(&current.object_id).unwrap(),
        Some(current)
    );
    cleanup(&root);
}

#[test]
fn journal_certification_rejects_complete_malformed_tail_without_truncation() {
    let root = temp_root("journal_certification_corruption");
    let (backend, _, _, prepare, _) = fixture(&root);
    drop(backend);
    let path = root.join("journal/segments/00000000000000000001.slog");
    let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&8_u32.to_le_bytes()).unwrap();
    file.write_all(b"not-sobj").unwrap();
    let len = file.metadata().unwrap().len();
    drop(file);
    crate::metadata_journal::admission::forget_for_test(&root);
    assert!(LocalSobjMetadataBackend::new(prepare.shard_id, &root).is_err());
    assert_eq!(fs::metadata(&path).unwrap().len(), len);
    cleanup(&root);
}

#[test]
fn journal_certification_recertifies_snapshot_only_floor_before_writable_open() {
    let root = temp_root("journal_certification_snapshot");
    let (mut backend, _, committed, prepare, terminal) = fixture(&root);
    commit(&mut backend, &committed, &prepare, &terminal).unwrap();
    backend
        .persist_current_metadata_checkpoint_unlocked()
        .unwrap();
    drop(backend);
    for entry in fs::read_dir(root.join("journal/segments")).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    crate::metadata_journal::admission::forget_for_test(&root);
    fail_journal_certifications_for_test(1);
    assert!(LocalSobjMetadataBackend::new(prepare.shard_id.clone(), &root).is_err());
    let reopened = LocalSobjMetadataBackend::new(prepare.shard_id, &root).unwrap();
    assert_eq!(
        reopened.get_object(&committed.object_id).unwrap(),
        Some(committed)
    );
    cleanup(&root);
}

#[test]
fn journal_certification_does_not_reuse_equal_length_replacement_or_foreign_shard() {
    let root = temp_root("journal_certification_replacement");
    let (backend, _, _, prepare, _) = fixture(&root);
    drop(backend);
    let path = root.join("journal/segments/00000000000000000001.slog");
    let bytes = fs::read(&path).unwrap();
    crate::metadata_io::write_file_atomic(&path, &bytes).unwrap();
    fail_journal_certifications_for_test(1);
    assert!(LocalSobjMetadataBackend::new(prepare.shard_id.clone(), &root).is_err());
    assert!(LocalSobjMetadataBackend::new(DfsMetadataShardId::new("foreign"), &root).is_err());
    LocalSobjMetadataBackend::new(prepare.shard_id, &root).unwrap();
    cleanup(&root);
}

#[cfg(unix)]
#[test]
fn journal_admission_alias_shares_uncertainty_and_new_process_requires_resync() {
    let root = temp_root("journal_admission_alias");
    let data = root.join("data");
    let alias = root.join("alias");
    let (backend, _, _, prepare, _) = fixture(&data);
    drop(backend);
    std::os::unix::fs::symlink(&data, &alias).unwrap();
    crate::metadata_journal::admission::mark_uncertain(&data).unwrap();
    fail_journal_certifications_for_test(1);
    assert!(LocalSobjMetadataBackend::new(prepare.shard_id.clone(), &alias).is_err());
    LocalSobjMetadataBackend::new(prepare.shard_id.clone(), &alias).unwrap();
    crate::metadata_journal::admission::forget_for_test(&data);
    fail_journal_certifications_for_test(1);
    assert!(LocalSobjMetadataBackend::new(prepare.shard_id, &data).is_err());
    cleanup(&root);
}

#[test]
fn final_batch_table_failure_preserves_durable_owner_and_replays_before_retry() {
    let root = temp_root("final_batch_table_failure");
    let (mut backend, current, committed, prepare, terminal) = fixture(&root);
    backend
        .persist_current_metadata_checkpoint_unlocked()
        .unwrap();
    let table_path = metadata_table_path(&root);
    let blocked_path = table_path.clone();
    crate::metadata_backend::api::final_namespace_batch_before_materialize_for_test(move || {
        fs::remove_file(&blocked_path).unwrap();
        fs::create_dir(&blocked_path).unwrap();
    });
    assert!(commit(&mut backend, &committed, &prepare, &terminal).is_err());
    assert_eq!(
        backend.tables.objects.get(&current.object_id),
        Some(&current),
        "failed table append rolls back resident namespace state"
    );
    fs::remove_dir(&table_path).unwrap();
    commit(&mut backend, &committed, &prepare, &terminal).unwrap();
    assert_complete(&backend, &committed, &terminal);
    cleanup(&root);
}

fn move_fixture_to_rollover_checkpoint(backend: &mut LocalSobjMetadataBackend, root: &Path) {
    let sequence = JOURNAL_SEGMENT_ENTRY_LIMIT - 1;
    backend.materialized_through_sequence = sequence;
    backend.next_sequence = sequence + 1;
    backend.prefix_proof = Some(
        backend
            .metadata_prefix_checkpoint_for_shard(&backend.shard_id, sequence)
            .unwrap(),
    );
    backend
        .persist_current_metadata_checkpoint_unlocked()
        .unwrap();
    for entry in fs::read_dir(root.join("journal/segments")).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    crate::metadata_journal::admission::invalidate(root).unwrap();
}

#[test]
fn final_batch_rollover_preserves_order_and_syncs_each_segment() {
    let root = temp_root("final_batch_rollover");
    let (mut backend, _, committed, prepare, terminal) = fixture(&root);
    move_fixture_to_rollover_checkpoint(&mut backend, &root);
    reset_journal_append_sync_calls_for_test();
    let entries = commit(&mut backend, &committed, &prepare, &terminal).unwrap();
    assert_eq!(entries[0].sequence, JOURNAL_SEGMENT_ENTRY_LIMIT);
    assert_eq!(entries[1].sequence, JOURNAL_SEGMENT_ENTRY_LIMIT + 1);
    assert_eq!(journal_append_sync_calls_for_test(), 2);
    assert_complete(&backend, &committed, &terminal);
    drop(backend);
    crate::metadata_journal::admission::forget_for_test(&root);
    assert_complete(
        &LocalSobjMetadataBackend::new(prepare.shard_id.clone(), &root).unwrap(),
        &committed,
        &terminal,
    );
    fs::remove_file(root.join("journal/segments/00000000000000000001.slog")).unwrap();
    crate::metadata_journal::admission::forget_for_test(&root);
    assert!(
        LocalSobjMetadataBackend::new(prepare.shard_id, &root).is_err(),
        "a cross-segment gap above the exact checkpoint cannot be certified"
    );
    cleanup(&root);
}

#[test]
fn final_batch_rollover_failure_recovers_complete_first_chunk_and_fences_torn_second() {
    let root = temp_root("final_batch_rollover_failure");
    let (mut backend, _, committed, prepare, terminal) = fixture(&root);
    move_fixture_to_rollover_checkpoint(&mut backend, &root);
    fail_journal_append_for_test(JournalAppendFault {
        skip_appends: 1,
        complete_frames: 0,
        partial_frame_bytes: 2,
    });
    assert!(commit(&mut backend, &committed, &prepare, &terminal).is_err());
    commit(&mut backend, &committed, &prepare, &terminal).unwrap();
    assert_complete(&backend, &committed, &terminal);
    cleanup(&root);
}

#[test]
fn healthy_same_root_reopen_reuses_exact_owner_certificate_without_resync() {
    let root = temp_root("journal_certification_healthy_reopen");
    let (backend, _, _, prepare, _) = fixture(&root);
    drop(backend);
    fail_journal_certifications_for_test(1);
    LocalSobjMetadataBackend::new(prepare.shard_id.clone(), &root).unwrap();
    crate::metadata_journal::admission::forget_for_test(&root);
    assert!(
        LocalSobjMetadataBackend::new(prepare.shard_id, &root).is_err(),
        "the unused fault fires only after the owner certificate is removed"
    );
    cleanup(&root);
}

#[test]
fn final_batch_rechecks_removed_parent_and_exclusive_path_occupant_before_append() {
    for parent_removed in [true, false] {
        let root = temp_root("final_batch_namespace_race");
        let (mut backend, parent, committed, prepare, terminal) = fixture(&root);
        let mut intruder = if parent_removed {
            parent.clone()
        } else {
            committed.clone()
        };
        intruder.version = version("namespace-raced-version");
        if parent_removed {
            intruder.kind = DfsObjectKind::Tombstone;
        } else {
            intruder.object_id = DfsObjectId::from_legacy_named("exclusive-occupant").unwrap();
        }
        backend.put_object(&intruder).unwrap();
        let before = backend.journal_last_sequence();
        assert!(commit(&mut backend, &committed, &prepare, &terminal).is_err());
        assert_eq!(backend.journal_last_sequence(), before);
        assert!(backend.get_object(&committed.object_id).unwrap().is_none());
        assert_eq!(
            backend.get_object(&intruder.object_id).unwrap(),
            Some(intruder)
        );
        cleanup(&root);
    }
}

#[test]
fn final_batch_rechecks_exact_receipt_and_matching_w2_threshold_before_append() {
    for missing_receipt in [true, false] {
        let root = temp_root("final_batch_receipt_fence");
        let (mut backend, parent, committed, prepare, mut terminal) = fixture(&root);
        if missing_receipt {
            backend
                .remove_receipt(
                    &committed.object_id,
                    &committed.version,
                    &committed.owner_node_id,
                )
                .unwrap();
        } else {
            let mut pending = backend
                .get_intent(&committed.object_id, &terminal.intent_id)
                .unwrap()
                .unwrap();
            pending.policy = DfsReplicationPolicy::new(2, 2).unwrap();
            pending
                .desired_replica_node_ids
                .push(node("node-second-receipt"));
            terminal.policy = pending.policy.clone();
            terminal.desired_replica_node_ids = pending.desired_replica_node_ids.clone();
            backend.put_intent(&pending).unwrap();
        }
        let before = backend.journal_last_sequence();
        let error = commit(&mut backend, &committed, &prepare, &terminal).unwrap_err();
        assert!(error.to_string().contains(if missing_receipt {
            "exact durable content receipt"
        } else {
            "required durable replica threshold"
        }));
        assert_eq!(backend.journal_last_sequence(), before);
        assert_eq!(backend.get_object(&parent.object_id).unwrap(), Some(parent));
        assert!(backend.get_object(&committed.object_id).unwrap().is_none());
        cleanup(&root);
    }
}

#[test]
fn all_frames_uncertain_cannot_become_terminal_noop_through_handle_replacement() {
    let root = temp_root("final_batch_all_frames_uncertain");
    let (mut backend, _, committed, prepare, terminal) = fixture(&root);
    fail_journal_append_for_test(JournalAppendFault {
        skip_appends: 0,
        complete_frames: 2,
        partial_frame_bytes: 0,
    });
    fail_journal_certifications_for_test(2);
    assert!(commit(&mut backend, &committed, &prepare, &terminal).is_err());
    drop(backend);
    assert!(
        LocalSobjMetadataBackend::new(prepare.shard_id.clone(), &root).is_err(),
        "decode-only reopen cannot accept complete uncertain terminal bytes"
    );
    let mut reopened = LocalSobjMetadataBackend::new(prepare.shard_id.clone(), &root).unwrap();
    assert_complete(&reopened, &committed, &terminal);
    commit(&mut reopened, &committed, &prepare, &terminal).unwrap();
    assert_complete(&reopened, &committed, &terminal);
    cleanup(&root);
}

// A retained legacy semantic ACK can survive loss/deferment of its derived
// table expansion. This fixture constructs that exact durable-journal/table
// cut through normal replica ingest, then restores the retained legacy floor
// under the shard writer lock and requires cold certification on next access.
fn pending_semantic_floor_replica_fixture(
    source_root: &Path,
    replica_root: &Path,
    matching_semantic_floor: bool,
) -> (
    LocalSobjMetadataBackend,
    DfsMetadataReplicaIngestPermit,
    DfsMetadataJournalEntry,
    DfsMetadataJournalEntry,
) {
    let shard = DfsMetadataShardId::new("root");
    let mut source = LocalSobjMetadataBackend::new(shard.clone(), source_root).unwrap();
    let mut filesystem = file_system();
    let first = source.put_file_system(&filesystem).unwrap();
    filesystem.display_name = "retained semantic floor state".into();
    let retained = source.put_file_system(&filesystem).unwrap();
    let snapshot = source.export_table_snapshot();
    let actual_fingerprint =
        crate::metadata_records::dfs_metadata_table_snapshot_state_fingerprint(&snapshot).unwrap();
    let fingerprint = if matching_semantic_floor {
        actual_fingerprint.clone()
    } else {
        let mismatched = format!("blake3:{}", "f".repeat(64));
        assert_ne!(mismatched, actual_fingerprint);
        mismatched
    };
    assert!(
        DfsMetadataPrefixProof::from_digest(retained.sequence, fingerprint.clone()).is_none(),
        "the fixture must exercise raw semantic state, not encoded exact-prefix evidence"
    );
    filesystem.display_name = "later namespace-only state".into();
    let successor = source.put_file_system(&filesystem).unwrap();
    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        filesystem.file_system_id.clone(),
        shard.clone(),
        node("node-semantic-floor-source"),
        1,
    );
    let mut replica = LocalSobjMetadataBackend::new(shard.clone(), replica_root).unwrap();
    replica
        .apply_journal_batch_with_replica_permit(
            &permit,
            &DfsMetadataJournalBatch {
                shard_id: shard.clone(),
                entries: vec![first.clone()],
                next_after_sequence: first.sequence,
            },
        )
        .unwrap();
    let report = replica
        .persist_journal_batch_with_replica_permit_report(
            &permit,
            &DfsMetadataJournalBatch {
                shard_id: shard.clone(),
                entries: vec![retained.clone()],
                next_after_sequence: retained.sequence,
            },
        )
        .unwrap();
    assert!(
        report.materialization_pending(),
        "normal no-pending namespace-only ingestion must keep its deferred ACK path"
    );
    assert_eq!(replica.materialized_through_sequence, first.sequence);
    {
        let _lock = replica.metadata_write_lock().unwrap();
        metadata_cursor::put_metadata_prefix_ack_floor(
            replica_root,
            &crate::metadata_records::DfsMetadataPrefixAckFloor {
                shard_id: shard,
                journal_sequence: retained.sequence,
                state_fingerprint: fingerprint,
                durable_prefix_proof: None,
            },
        )
        .unwrap();
        crate::metadata_journal::admission::mark_uncertain(replica_root).unwrap();
    }
    (replica, permit, retained, successor)
}

#[test]
fn pending_semantic_floor_blocks_namespace_only_noop_and_advance_before_ack() {
    for advancing in [false, true] {
        let root = temp_root(&format!("pending-semantic-floor-reject-{advancing}"));
        let replica_root = root.join("replica");
        let (mut replica, permit, retained, successor) =
            pending_semantic_floor_replica_fixture(&root.join("source"), &replica_root, false);
        let submitted = if advancing {
            successor
        } else {
            retained.clone()
        };
        let batch = DfsMetadataJournalBatch {
            shard_id: retained.shard_id.clone(),
            next_after_sequence: submitted.sequence,
            entries: vec![submitted],
        };
        let before = replica.journal_since(0).unwrap();
        for _ in 0..2 {
            let error = replica
                .persist_journal_batch_with_replica_permit_report(&permit, &batch)
                .expect_err("pending semantic mismatch must prevent a durable-ingest ACK report");
            assert!(
                error.to_string().contains("semantic acknowledgement"),
                "{error}"
            );
            assert_eq!(
                replica.journal_since(0).unwrap(),
                before,
                "no successor journal append may precede exact-floor restoration"
            );
            assert_eq!(replica.next_sequence, retained.sequence + 1);
            assert_eq!(
                metadata_cursor::metadata_prefix_ack_floor(&replica_root, &retained.shard_id)
                    .unwrap()
                    .unwrap()
                    .journal_sequence,
                retained.sequence
            );
        }
        cleanup(&root);
    }
}

#[test]
fn pending_semantic_floor_restores_exact_state_then_keeps_normal_deferred_ack() {
    for advancing in [false, true] {
        let root = temp_root(&format!("pending-semantic-floor-restore-{advancing}"));
        let replica_root = root.join("replica");
        let (mut replica, permit, retained, successor) =
            pending_semantic_floor_replica_fixture(&root.join("source"), &replica_root, true);
        assert!(replica.materialized_through_sequence < retained.sequence);
        let submitted = if advancing {
            successor
        } else {
            retained.clone()
        };
        let target = submitted.sequence;
        let report = replica
            .persist_journal_batch_with_replica_permit_report(
                &permit,
                &DfsMetadataJournalBatch {
                    shard_id: retained.shard_id.clone(),
                    entries: vec![submitted],
                    next_after_sequence: target,
                },
            )
            .expect("valid raw semantic floor must be restored before ACK admission");
        assert_eq!(
            replica.materialized_through_sequence, retained.sequence,
            "the exceptional legacy floor is restored even for a duplicate namespace-only batch"
        );
        assert_eq!(
            report.materialization_pending(),
            advancing,
            "after consuming the floor, a new small namespace-only suffix stays deferred"
        );
        assert_eq!(replica.next_sequence, target + 1);
        assert_eq!(
            replica
                .get_file_system(&file_system_id())
                .unwrap()
                .unwrap()
                .display_name,
            "retained semantic floor state"
        );
        cleanup(&root);
    }
}
