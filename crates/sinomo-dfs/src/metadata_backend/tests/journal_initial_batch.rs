//! Initial I/R/P group sync preserves hidden namespace and exact retry ownership.
use super::*;
use crate::metadata_journal::{
    JournalAppendFault, fail_journal_append_for_test, fail_journal_certifications_for_test,
    journal_append_sync_calls_for_test, reset_journal_append_sync_calls_for_test,
};

struct Create {
    parent: DfsObjectMetadata,
    hidden: DfsObjectMetadata,
    child: DfsObjectMetadata,
    intent: DfsWriteIntentRecord,
    receipts: Vec<DfsReplicaReceiptRecord>,
    prepare: DfsNamespacePrepareRecord,
}

fn fixture(root: &Path) -> (LocalSobjMetadataBackend, Create) {
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root).unwrap();
    backend.put_file_system(&file_system()).unwrap();
    let mut parent = object(version("initial_parent_base"));
    parent.object_id = DfsObjectId::from_legacy_named("initial_parent").unwrap();
    parent.path = "/".into();
    parent.kind = DfsObjectKind::Directory;
    parent.content_hash = None;
    backend.put_object(&parent).unwrap();
    let mut hidden = object(version("initial_hidden_child"));
    hidden.path = "/created.txt".into();
    hidden.content_hash = None;
    let mut child = hidden.clone();
    child.version = version("initial_target");
    child.content_hash = Some(crate::content_store::content_hash(b"initial-batch"));
    child.content_size_bytes = Some(13);
    child.current_locations = vec![node("node_owner"), node("node_second")];
    child.current_locations.sort();
    let intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "intent-initial-batch".into(),
        file_system_id: child.file_system_id.clone(),
        object_id: child.object_id.clone(),
        base_version: None,
        target_version: child.version.clone(),
        coordinator_node_id: child.owner_node_id.clone(),
        operation_id: Some("write-initial".into()),
        operation_order_token: None,
        owner_epoch: 7,
        desired_replica_node_ids: child.current_locations.clone(),
        policy: DfsReplicationPolicy::new(2, 2).unwrap(),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: child.content_hash.clone(),
        object_metadata: Some(hidden.clone()),
        touch_parent_on_create: true,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: None,
    };
    let receipts = child
        .current_locations
        .iter()
        .map(|node| {
            let mut receipt =
                replica_receipt_record(&child, child.version.clone(), OffsetDateTime::UNIX_EPOCH);
            receipt.replica_node_id = node.clone();
            receipt
        })
        .collect();
    let mut touched_parent = parent.clone();
    touched_parent.version = version("initial_parent_touched");
    touched_parent.modified_at_unix_nanos = 1;
    touched_parent.changed_at_unix_nanos = 1;
    let mut prepare = namespace_prepare(
        &format!("namespace:create:{}", intent.intent_id),
        "root",
        &parent,
        touched_parent.clone(),
    );
    prepare
        .expected_object_versions
        .push(DfsNamespaceObjectVersionExpectation {
            object_id: child.object_id.clone(),
            expected_version: None,
        });
    prepare
        .expected_path_occupants
        .push(DfsNamespacePathOccupantExpectation {
            path: child.path.clone(),
            expected_object_id: None,
        });
    prepare.transaction.objects = vec![child.clone(), touched_parent.clone()];
    prepare.transaction.versions = vec![
        object_version_record(&child, child.version.clone(), OffsetDateTime::UNIX_EPOCH),
        object_version_record(
            &touched_parent,
            touched_parent.version.clone(),
            OffsetDateTime::UNIX_EPOCH,
        ),
    ];
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare).unwrap();
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV2);
    (
        backend,
        Create {
            parent,
            hidden,
            child,
            intent,
            receipts,
            prepare,
        },
    )
}

fn initial(
    backend: &mut dyn DfsMetadataBackend,
    create: &Create,
) -> Result<Vec<DfsMetadataJournalEntry>> {
    backend.prepare_intent_receipts_and_namespace(
        Some(&create.hidden),
        &create.intent,
        &create.receipts,
        &create.prepare,
    )
}

fn assert_hidden(backend: &dyn DfsMetadataBackend, create: &Create) {
    assert!(
        backend
            .get_object(&create.child.object_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        backend.get_object(&create.parent.object_id).unwrap(),
        Some(create.parent.clone())
    );
}

fn assert_prepared(backend: &dyn DfsMetadataBackend, create: &Create) {
    assert_hidden(backend, create);
    let prepare = backend
        .get_namespace_prepare(&create.prepare.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(prepare, normalized_namespace_prepare(&create.prepare));
    assert_eq!(
        backend
            .get_intent(&create.child.object_id, &create.intent.intent_id)
            .unwrap()
            .unwrap()
            .status,
        DfsWriteIntentStatus::ReceiptsPending
    );
    for receipt in &create.receipts {
        assert!(
            backend
                .get_receipt_on_shard(
                    &create.prepare.shard_id,
                    &create.child.object_id,
                    &create.child.version,
                    &receipt.replica_node_id
                )
                .unwrap()
                .is_some()
        );
    }
}

fn terminal(create: &Create) -> DfsWriteIntentRecord {
    let mut terminal = create.intent.clone();
    terminal.status = DfsWriteIntentStatus::Committed;
    terminal.object_metadata = Some(create.child.clone());
    terminal.updated_at = Some(OffsetDateTime::UNIX_EPOCH);
    terminal
}

#[test]
fn initial_create_shares_one_sync_and_restores_trailing_prepare_reference_after_table_loss() {
    let root = temp_root("initial_create_one_sync");
    let (mut backend, create) = fixture(&root);
    reset_journal_append_sync_calls_for_test();
    let entries = initial(&mut backend, &create).unwrap();
    assert_eq!(journal_append_sync_calls_for_test(), 1);
    assert_eq!(entries.len(), 4);
    assert!(matches!(
        entries[0].mutation,
        DfsMetadataMutation::PutIntent { .. }
    ));
    assert!(
        entries[1..3]
            .iter()
            .all(|entry| matches!(entry.mutation, DfsMetadataMutation::PutReceipt { .. }))
    );
    assert!(matches!(
        entries[3].mutation,
        DfsMetadataMutation::PutNamespacePrepare { .. }
    ));
    assert_prepared(&backend, &create);
    let replay_cost = backend.table_log_journal_reference_replay_cost_bytes;
    assert!(replay_cost > 0);
    let batches = metadata_table_batches(&root);
    let last = batches.last().unwrap();
    assert!(last.ops.iter().any(|op| matches!(op,
        DfsMetadataTableOp::PutNamespacePrepareJournalReference { journal_sequence, .. }
            if *journal_sequence == entries[3].sequence)));
    assert_eq!(last.journal_reference_replay_cost_bytes, replay_cost);
    drop(backend);
    fs::remove_file(metadata_table_path(&root)).unwrap();
    crate::metadata_journal::admission::forget_for_test(&root);
    let mut reopened =
        LocalSobjMetadataBackend::new(create.prepare.shard_id.clone(), &root).unwrap();
    assert_prepared(&reopened, &create);
    // Losing the complete table image triggers prefix bootstrap and a new
    // self-contained checkpoint after journal replay. That checkpoint consumes
    // the rebuilt journal-reference debt rather than retaining it in the log.
    let checkpoint =
        crate::metadata_table_store::read_metadata_table_snapshot_for_replica_repair(&root)
            .expect("read the recovered self-contained checkpoint");
    assert_eq!(
        checkpoint.materialized_through_sequence,
        Some(entries[3].sequence)
    );
    assert_eq!(
        checkpoint.prefix_proof,
        Some(reopened.metadata_prefix_proof().unwrap())
    );
    assert_eq!(
        checkpoint
            .tables
            .namespace_prepares
            .get(&create.prepare.operation_id),
        Some(&normalized_namespace_prepare(&create.prepare))
    );
    assert_eq!(reopened.table_log_journal_reference_replay_cost_bytes, 0);
    reopened
        .commit_prepared_namespace_transaction_and_intent_on_shard(
            &create.prepare.shard_id,
            &create.prepare.operation_id,
            &create.prepare.operation_digest,
            create.prepare.owner_epoch,
            &create.child,
            &terminal(&create),
        )
        .unwrap();
    assert_eq!(
        reopened.get_object(&create.child.object_id).unwrap(),
        Some(create.child.clone())
    );
    let touched = create
        .prepare
        .transaction
        .objects
        .iter()
        .find(|object| object.object_id == create.parent.object_id)
        .unwrap();
    assert_eq!(
        reopened
            .get_object(&create.parent.object_id)
            .unwrap()
            .as_ref(),
        Some(touched)
    );
    drop(reopened);
    let reopened = LocalSobjMetadataBackend::new(create.prepare.shard_id.clone(), &root).unwrap();
    assert_eq!(
        reopened.get_object(&create.child.object_id).unwrap(),
        Some(create.child)
    );
    cleanup(&root);
}

#[test]
fn initial_partial_prefixes_retry_without_reusing_sequences_or_publishing_namespace() {
    for reopen in [false, true] {
        for (complete_frames, partial_frame_bytes) in [
            (0, 2),
            (0, 9),
            (1, 0),
            (1, 3),
            (2, 0),
            (3, 0),
            (3, 9),
            (4, 0),
        ] {
            let root = temp_root(&format!(
                "initial_cut_{reopen}_{complete_frames}_{partial_frame_bytes}"
            ));
            let (mut backend, mut create) = fixture(&root);
            let before = backend.journal_last_sequence();
            fail_journal_append_for_test(JournalAppendFault {
                skip_appends: 0,
                complete_frames,
                partial_frame_bytes,
            });
            assert!(initial(&mut backend, &create).is_err());
            assert_hidden(&backend, &create);
            if reopen {
                drop(backend);
                crate::metadata_journal::admission::forget_for_test(&root);
                backend =
                    LocalSobjMetadataBackend::new(create.prepare.shard_id.clone(), &root).unwrap();
            }
            // A retry may refresh observation timestamps but cannot rewrite any
            // already-consumed sequence with its newly encoded intent/receipt.
            create.intent.created_at += time::Duration::nanoseconds(1);
            for receipt in &mut create.receipts {
                receipt.receipt.event_id = EventId::generate();
            }
            let entries = initial(&mut backend, &create).unwrap();
            assert_eq!(entries[0].sequence, before + complete_frames as u64 + 1);
            assert_prepared(&backend, &create);
            let journal = backend.journal_since(0).unwrap();
            assert!(
                journal
                    .windows(2)
                    .all(|pair| pair[1].sequence == pair[0].sequence + 1)
            );
            cleanup(&root);
        }
    }
}

#[test]
fn initial_complete_uncertain_prepare_cannot_become_a_noop_ack_on_replacement() {
    let root = temp_root("initial_uncertain_replacement");
    let (mut backend, create) = fixture(&root);
    fail_journal_append_for_test(JournalAppendFault {
        skip_appends: 0,
        complete_frames: 4,
        partial_frame_bytes: 0,
    });
    fail_journal_certifications_for_test(3);
    assert!(initial(&mut backend, &create).is_err());
    assert!(initial(&mut backend, &create).is_err());
    drop(backend);
    assert!(LocalSobjMetadataBackend::new(create.prepare.shard_id.clone(), &root).is_err());
    let mut reopened =
        LocalSobjMetadataBackend::new(create.prepare.shard_id.clone(), &root).unwrap();
    assert_prepared(&reopened, &create);
    initial(&mut reopened, &create).unwrap();
    assert_prepared(&reopened, &create);
    cleanup(&root);
}

#[test]
fn initial_table_failure_rolls_back_resident_prepare_and_replays_exact_durable_batch() {
    let root = temp_root("initial_table_failure");
    let (mut backend, create) = fixture(&root);
    backend
        .persist_current_metadata_checkpoint_unlocked()
        .unwrap();
    let blocked = metadata_table_path(&root);
    let hook_path = blocked.clone();
    crate::metadata_backend::api::initial_namespace_batch_before_materialize_for_test(move || {
        fs::remove_file(&hook_path).unwrap();
        fs::create_dir(&hook_path).unwrap();
    });
    assert!(initial(&mut backend, &create).is_err());
    assert!(backend.tables.namespace_prepares.is_empty());
    assert!(backend.tables.intents.is_empty());
    assert!(backend.tables.receipts.is_empty());
    fs::remove_dir(&blocked).unwrap();
    initial(&mut backend, &create).unwrap();
    assert_prepared(&backend, &create);
    cleanup(&root);
}

#[test]
fn initial_batch_rejects_parent_occupant_digest_epoch_and_receipt_changes_before_append() {
    for case in [
        "parent-removed",
        "parent-file",
        "exclusive-occupant",
        "digest",
        "epoch",
        "receipt-hash",
        "receipt-w",
        "receipt-duplicate",
    ] {
        let root = temp_root(&format!("initial_reject_{case}"));
        let (mut backend, mut create) = fixture(&root);
        match case {
            "parent-removed" => {
                let mut parent = create.parent.clone();
                parent.kind = DfsObjectKind::Tombstone;
                backend.put_object(&parent).unwrap();
            }
            "parent-file" => {
                let mut parent = create.parent.clone();
                parent.kind = DfsObjectKind::File;
                backend.put_object(&parent).unwrap();
            }
            "exclusive-occupant" => {
                let mut occupant = create.child.clone();
                occupant.object_id =
                    DfsObjectId::from_legacy_named("initial_other_occupant").unwrap();
                backend.put_object(&occupant).unwrap();
            }
            "digest" => create.prepare.operation_digest.push('x'),
            "epoch" => create.prepare.owner_epoch += 1,
            "receipt-hash" => create.receipts[0].content_hash = Some("blake3:wrong".into()),
            "receipt-w" => {
                create.receipts.pop();
            }
            "receipt-duplicate" => create.receipts[1] = create.receipts[0].clone(),
            _ => unreachable!(),
        }
        let before = backend.journal_last_sequence();
        assert!(initial(&mut backend, &create).is_err(), "{case}");
        assert_eq!(backend.journal_last_sequence(), before, "{case}");
        assert!(
            backend
                .get_object(&create.child.object_id)
                .unwrap()
                .is_none(),
            "{case}"
        );
        assert!(
            backend
                .get_intent(&create.child.object_id, &create.intent.intent_id)
                .unwrap()
                .is_none(),
            "{case}"
        );
        cleanup(&root);
    }
}

#[test]
fn initial_exact_prepared_retry_remains_one_group_and_terminal_retry_uses_serial_fallback() {
    let root = temp_root("initial_retry_fallback");
    let (mut backend, create) = fixture(&root);
    initial(&mut backend, &create).unwrap();
    reset_journal_append_sync_calls_for_test();
    initial(&mut backend, &create).unwrap();
    assert_eq!(journal_append_sync_calls_for_test(), 1);
    backend
        .commit_prepared_namespace_transaction_and_intent_on_shard(
            &create.prepare.shard_id,
            &create.prepare.operation_id,
            &create.prepare.operation_digest,
            create.prepare.owner_epoch,
            &create.child,
            &terminal(&create),
        )
        .unwrap();
    reset_journal_append_sync_calls_for_test();
    initial(&mut backend, &create).unwrap();
    assert_eq!(journal_append_sync_calls_for_test(), 2);
    assert_eq!(
        backend
            .get_intent(&create.child.object_id, &create.intent.intent_id)
            .unwrap()
            .unwrap()
            .status,
        DfsWriteIntentStatus::Committed
    );
    assert_eq!(
        backend
            .tables
            .namespace_prepare_receipts
            .get(&create.prepare.operation_id)
            .unwrap()
            .status,
        DfsNamespacePrepareStatus::Committed
    );
    let receipt =
        DfsWriteIntentReceipt::from_intent(&terminal(&create), OffsetDateTime::UNIX_EPOCH).unwrap();
    backend
        .put_intent_receipt_for_object(Some(&create.child), &receipt)
        .unwrap();
    reset_journal_append_sync_calls_for_test();
    let entries = initial(&mut backend, &create).unwrap();
    assert_eq!(journal_append_sync_calls_for_test(), 2);
    assert!(matches!(
        entries[0].mutation,
        DfsMetadataMutation::PutIntentReceipt { .. }
    ));
    assert!(matches!(
        entries.last().unwrap().mutation,
        DfsMetadataMutation::PutNamespacePrepareReceipt { .. }
    ));
    cleanup(&root);
}

#[test]
fn initial_rollover_preserves_order_and_one_sync_per_segment() {
    let root = temp_root("initial_rollover");
    let (mut backend, create) = fixture(&root);
    let checkpoint = JOURNAL_SEGMENT_ENTRY_LIMIT - 2;
    backend.materialized_through_sequence = checkpoint;
    backend.next_sequence = checkpoint + 1;
    backend.prefix_proof = Some(
        backend
            .metadata_prefix_checkpoint_for_shard(&backend.shard_id, checkpoint)
            .unwrap(),
    );
    backend
        .persist_current_metadata_checkpoint_unlocked()
        .unwrap();
    for entry in fs::read_dir(root.join("journal/segments")).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    crate::metadata_journal::admission::invalidate(&root).unwrap();
    reset_journal_append_sync_calls_for_test();
    let entries = initial(&mut backend, &create).unwrap();
    assert_eq!(journal_append_sync_calls_for_test(), 2);
    assert_eq!(entries.first().unwrap().sequence, checkpoint + 1);
    assert_eq!(entries.last().unwrap().sequence, checkpoint + 4);
    drop(backend);
    crate::metadata_journal::admission::forget_for_test(&root);
    let reopened = LocalSobjMetadataBackend::new(create.prepare.shard_id.clone(), &root).unwrap();
    assert_prepared(&reopened, &create);
    cleanup(&root);
}

#[test]
fn sharded_initial_batch_routes_exactly_and_publishes_hidden_intent_hint_after_durability() {
    let fixture_root = temp_root("initial_sharded_fixture");
    let (_, create) = fixture(&fixture_root);
    let root = temp_root("initial_sharded");
    let mut config = root_shard_config_at_epoch("root", 7);
    config.namespace_prepare_codec = DfsMetadataNamespacePrepareCodec::PackedV2;
    let mut backend = LocalShardedMetadataBackend::new(&root, vec![config]).unwrap();
    backend.put_file_system(&file_system()).unwrap();
    backend.put_object(&create.parent).unwrap();
    reset_journal_append_sync_calls_for_test();
    let entries = initial(&mut backend, &create).unwrap();
    assert_eq!(journal_append_sync_calls_for_test(), 1);
    assert!(
        entries
            .iter()
            .all(|entry| entry.shard_id == create.prepare.shard_id)
    );
    assert_prepared(&backend, &create);
    assert_eq!(
        backend
            .read_object_shard_index(&create.child.object_id)
            .unwrap()
            .unwrap()
            .shard_id,
        create.prepare.shard_id
    );
    cleanup(&root);
    cleanup(&fixture_root);
}

#[test]
fn initial_history_policy_preserves_serial_preparation_path() {
    let root = temp_root("initial_history_fallback");
    let (mut backend, create) = fixture(&root);
    let mut fs = file_system();
    fs.file_history = crate::DfsFileHistoryPolicy {
        rules: vec![crate::DfsFileHistoryRule {
            path: "/unrelated-history".into(),
            retention: crate::DfsFileHistoryRetention {
                keep_last: 2,
                keep_for_seconds: 60,
            },
        }],
    };
    backend.put_file_system(&fs).unwrap();
    reset_journal_append_sync_calls_for_test();
    let entries = initial(&mut backend, &create).unwrap();
    assert_eq!(journal_append_sync_calls_for_test(), 2);
    assert_eq!(entries.len(), 4);
    assert_prepared(&backend, &create);
    cleanup(&root);
}

#[test]
fn initial_existing_prepare_keeps_exact_identity_and_abort_cannot_be_reopened() {
    let root = temp_root("initial_existing_prepare");
    let (mut backend, mut create) = fixture(&root);
    initial(&mut backend, &create).unwrap();
    let exact = create.prepare.clone();
    create
        .prepare
        .transaction
        .objects
        .iter_mut()
        .find(|object| object.object_id == create.child.object_id)
        .unwrap()
        .mode ^= 0o100;
    create.prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&create.prepare).unwrap();
    let before = backend.journal_last_sequence();
    assert!(initial(&mut backend, &create).is_err());
    assert_eq!(backend.journal_last_sequence(), before);
    create.prepare = exact;
    backend
        .abort_prepared_namespace_transaction(
            &create.prepare.operation_id,
            &create.prepare.operation_digest,
            create.prepare.owner_epoch,
        )
        .unwrap();
    assert!(initial(&mut backend, &create).is_err());
    assert_hidden(&backend, &create);
    assert_eq!(
        backend
            .get_namespace_prepare(&create.prepare.operation_id)
            .expect("read durable aborted prepare")
            .expect("abort retains the full prepare until history GC")
            .status,
        DfsNamespacePrepareStatus::Aborted
    );
    cleanup(&root);
}
