//! Explicit captures are part of metadata publication, replay and collection.
use super::*;
use crate::{
    DfsFileHistoryPolicy, DfsFileHistoryRetention, DfsFileHistoryRule, DfsFileVersionSource,
};

fn history_file(name: &str, hash: &str, at: u64) -> DfsObjectMetadata {
    let mut value = object(version(name));
    value.path = "/docs/readme.md".into();
    value.content_hash = Some(hash.into());
    value.content_size_bytes = Some(5);
    value.changed_at_unix_nanos = at;
    value.modified_at_unix_nanos = at;
    value
}

fn history_fs(keep_last: u32, keep_for_seconds: u64) -> DfsFileSystem {
    let mut fs = file_system();
    fs.file_history = DfsFileHistoryPolicy {
        rules: vec![DfsFileHistoryRule {
            path: "/docs".into(),
            retention: DfsFileHistoryRetention {
                keep_last,
                keep_for_seconds,
            },
        }],
    };
    fs
}

fn publish(backend: &mut LocalSobjMetadataBackend, object: &DfsObjectMetadata) {
    let version = crate::metadata_records::object_version_record(object, object.content_size_bytes);
    backend
        .put_object_and_version(object, &version)
        .expect("publish generation");
}

#[test]
fn file_history_gc_preserves_policy_with_bounded_capability_gated_journal_entries() {
    let root = temp_root("history-gc-policy-owner");
    let shard = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), &root).unwrap();
    backend.put_file_system(&file_system()).unwrap();
    let original = history_file("gc-policy-original", "blake3:original", 1);
    publish(&mut backend, &original);
    let intent = DfsWriteIntentRecord {
        publication: crate::DfsWritePublication::Current,
        intent_id: "original-policy-witness".into(),
        file_system_id: original.file_system_id.clone(),
        object_id: original.object_id.clone(),
        base_version: None,
        target_version: original.version.clone(),
        coordinator_node_id: original.owner_node_id.clone(),
        operation_id: Some("original-write".into()),
        operation_order_token: None,
        owner_epoch: 0,
        desired_replica_node_ids: vec![
            original.owner_node_id.clone(),
            node("other-a"),
            node("other-b"),
        ],
        policy: DfsReplicationPolicy::new(1, 3).unwrap(),
        status: DfsWriteIntentStatus::Committed,
        content_hash: original.content_hash.clone(),
        object_metadata: Some(original.clone()),
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: Some(OffsetDateTime::UNIX_EPOCH),
    };
    let mut invalid_removal = backend.put_intent(&intent).unwrap();
    invalid_removal.sequence += 1;
    invalid_removal.mutation = DfsMetadataMutation::RemoveRetainedRepairOwner {
        file_system_id: original.file_system_id.clone(),
        object_id: original.object_id.clone(),
        intent_id: intent.intent_id.clone(),
    };
    let before = backend.journal_last_sequence();
    let invalid_batch = DfsMetadataJournalBatch {
        shard_id: shard.clone(),
        entries: vec![invalid_removal.clone()],
        next_after_sequence: invalid_removal.sequence,
    };
    assert!(backend.apply_journal_batch(&invalid_batch).is_err());
    assert_eq!(backend.journal_last_sequence(), before);
    let mut original_prefix = backend
        .journal_batch_since_limited(0, 128, usize::MAX)
        .unwrap()
        .unwrap();
    original_prefix.entries.extend(invalid_batch.entries);
    original_prefix.next_after_sequence = invalid_batch.next_after_sequence;
    let mut rejected = LocalSobjMetadataBackend::new(shard.clone(), root.join("rejected")).unwrap();
    assert!(rejected.apply_journal_batch(&original_prefix).is_err());
    assert_eq!(rejected.journal_last_sequence(), 0);
    let mut capture = crate::metadata_records::object_version_record(&original, Some(5));
    capture.retained = Some(Box::new(crate::DfsRetainedFileVersion {
        object: Box::new(original.clone()),
        captured_at_unix_nanos: 2,
        source: DfsFileVersionSource::Checkpoint {
            operation_id: "capture-policy".into(),
        },
        policy_capture: None,
    }));
    let pin = DfsMetadataVersionPinRecord {
        file_system_id: original.file_system_id.clone(),
        object_id: original.object_id.clone(),
        version: original.version.clone(),
        pin_id: "checkpoint".into(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    backend
        .capture_file_version(&capture, &pin, Some(&original.version))
        .unwrap();
    let current = history_file("gc-policy-current", "blake3:current", 3);
    publish(&mut backend, &current);
    backend
        .remove_version_pin(
            &pin.file_system_id,
            &pin.object_id,
            &pin.version,
            &pin.pin_id,
        )
        .unwrap();
    let gc = || {
        crate::DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0).with_max_removals(1)
    };
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV3);
    let before = backend.journal_last_sequence();
    let held = backend.gc_metadata_history(gc()).unwrap();
    assert_eq!(held.applied_record_count(), 0);
    assert_eq!(held.retained_repair_policy_witness_count, 1);
    assert_eq!(
        held.eligible_record_count(),
        0,
        "an upgrade dependency cannot cause a maintenance spin"
    );
    assert_eq!(backend.journal_last_sequence(), before);

    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV4);
    let first = backend.gc_metadata_history(gc()).unwrap();
    assert_eq!(first.applied_record_count(), 1);
    assert_eq!(first.materialized_retained_repair_owner_count, 1);
    assert_eq!(first.compacted_intent_count, 0);
    assert!(first.max_removals_reached);
    let owner_id = crate::file_history::retained_repair_intent_id(&capture);
    let owner = backend
        .get_intent(&original.object_id, &owner_id)
        .unwrap()
        .unwrap();
    assert_eq!(owner.policy, intent.policy);
    assert_eq!(
        owner.desired_replica_node_ids,
        intent.desired_replica_node_ids
    );
    let batch = backend
        .journal_batch_since_limited(before, 8, usize::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(batch.entries.len(), 1);
    assert!(
        matches!(&batch.entries[0].mutation, DfsMetadataMutation::PutIntent { intent: Some(record), .. } if record.publication.is_retained_repair())
    );
    let second = backend.gc_metadata_history(gc()).unwrap();
    assert_eq!(second.compacted_intent_count, 1);
    assert_eq!(second.applied_record_count(), 1);
    let third = backend.gc_metadata_history(gc()).unwrap();
    assert_eq!(third.removed_intent_receipt_count, 1);
    assert_eq!(third.applied_record_count(), 1);

    let all = backend
        .journal_batch_since_limited(0, 128, usize::MAX)
        .unwrap()
        .unwrap();
    let mut replica = LocalSobjMetadataBackend::new(shard.clone(), root.join("replica")).unwrap();
    for entry in &all.entries {
        replica
            .apply_journal_batch(&DfsMetadataJournalBatch {
                shard_id: shard.clone(),
                entries: vec![entry.clone()],
                next_after_sequence: entry.sequence,
            })
            .unwrap();
    }
    assert_eq!(
        replica.get_intent(&original.object_id, &owner_id).unwrap(),
        Some(owner.clone())
    );
    assert!(
        replica
            .get_intent_receipt(&original.object_id, &intent.intent_id)
            .unwrap()
            .is_none()
    );
    drop(backend);
    let mut backend = LocalSobjMetadataBackend::new(shard, &root).unwrap();
    assert_eq!(
        backend.get_intent(&original.object_id, &owner_id).unwrap(),
        Some(owner.clone())
    );
    let mut changed = owner;
    changed.policy = DfsReplicationPolicy::new(1, 2).unwrap();
    changed.desired_replica_node_ids.truncate(2);
    let before = backend.journal_last_sequence();
    assert!(backend.put_intent(&changed).is_err());
    assert_eq!(backend.journal_last_sequence(), before);
    let mut changed_entry = all.entries.last().unwrap().clone();
    changed_entry.sequence += 1;
    changed_entry.mutation = DfsMetadataMutation::put_intent(&changed);
    let mut poisoned = all.clone();
    poisoned.next_after_sequence = changed_entry.sequence;
    poisoned.entries.push(changed_entry);
    assert!(rejected.apply_journal_batch(&poisoned).is_err());
    assert_eq!(rejected.journal_last_sequence(), 0);
    assert!(
        crate::DfsWriteIntentReceipt::from_intent(&changed, OffsetDateTime::now_utc()).is_none()
    );
    for _ in 0..2 {
        assert_eq!(
            backend
                .gc_metadata_history(gc())
                .unwrap()
                .applied_record_count(),
            1
        );
    }
    let collected = backend
        .journal_batch_since_limited(all.next_after_sequence, 128, usize::MAX)
        .unwrap()
        .unwrap();
    assert!(collected.entries.iter().any(|entry| matches!(
        entry.mutation,
        DfsMetadataMutation::RemoveRetainedRepairOwner { .. }
    )));
    replica.apply_journal_batch(&collected).unwrap();
    assert!(
        replica
            .get_intent(&original.object_id, &owner_id)
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .get_version(&original.object_id, &original.version)
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .get_intent(&original.object_id, &owner_id)
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .get_intent_receipt(&original.object_id, &owner_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        backend.get_object(&current.object_id).unwrap(),
        Some(current)
    );
    cleanup(&root);
}

#[test]
fn file_history_prepare_requires_the_exact_intent_and_durable_w_evidence() {
    let root = temp_root("retained-prepare-proof");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).unwrap();
    backend.put_file_system(&file_system()).unwrap();
    let current = history_file("retained-proof-current", "blake3:current", 1);
    publish(&mut backend, &current);
    let mut captured = current.clone();
    captured.version = version("retained-proof-local");
    captured.content_hash = Some(crate::content_hash(b"saved"));
    let nodes = vec![node("node_owner"), node("node_second")];
    captured.current_locations = nodes.clone();
    let mut coordinator = crate::DistributedEventualCoordinator::new();
    let intent = coordinator
        .create_intent_for_publication(
            current.file_system_id.clone(),
            current.object_id.clone(),
            None,
            captured.version.clone(),
            node("node_owner"),
            7,
            nodes.clone(),
            DfsReplicationPolicy::new(2, 2).unwrap(),
            false,
            None,
            Some("preserved-proof-intent".into()),
            Some("preserved-proof-operation".into()),
            None,
            crate::DfsWritePublication::Retained {
                capture: Box::new(crate::DfsRetainedWritePublication {
                    pin_id: "proof-pin".into(),
                    source: DfsFileVersionSource::LocalOperation {
                        operation_id: "local-work".into(),
                        base_version: Some(current.version.clone()),
                    },
                }),
            },
        )
        .unwrap();
    backend
        .put_intent(&crate::intent::write_intent_record(
            &intent,
            captured.content_hash.as_deref(),
            Some(captured.clone()),
        ))
        .unwrap();
    let mut record = crate::metadata_records::object_version_record(&captured, Some(5));
    record.visible = false;
    record.retained = Some(Box::new(crate::DfsRetainedFileVersion {
        object: Box::new(captured.clone()),
        captured_at_unix_nanos: 1,
        source: DfsFileVersionSource::LocalOperation {
            operation_id: "local-work".into(),
            base_version: Some(current.version.clone()),
        },
        policy_capture: None,
    }));
    let transaction_id = format!("file-history-content:{}", intent.intent_id);
    let mut prepare = namespace_prepare(
        &format!("namespace:{transaction_id}"),
        "root",
        &current,
        current.clone(),
    );
    prepare.transaction.transaction_id = transaction_id;
    prepare.transaction.objects.clear();
    prepare.transaction.versions = vec![record];
    prepare.transaction.file_version_pins = Some(crate::DfsFileVersionPinMutation {
        add: vec![DfsMetadataVersionPinRecord {
            file_system_id: current.file_system_id.clone(),
            object_id: current.object_id.clone(),
            version: captured.version.clone(),
            pin_id: "proof-pin".into(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        }],
        remove: Vec::new(),
    });
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare).unwrap();
    let before = backend.journal_last_sequence();
    assert!(
        backend
            .prepare_namespace_transaction(&prepare)
            .unwrap_err()
            .to_string()
            .contains("W receipts")
    );
    assert_eq!(backend.journal_last_sequence(), before);
    prepare.content_receipts = nodes
        .into_iter()
        .map(|replica_node_id| DfsReplicaReceiptRecord {
            receipt: sinomo_core::DfsWriteReceipt {
                file_system_id: current.file_system_id.clone(),
                object_id: current.object_id.clone(),
                committed_version: captured.version.clone(),
                owner_node_id: current.owner_node_id.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id,
            content_hash: captured.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        })
        .collect();
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare).unwrap();
    assert!(
        backend
            .prepare_namespace_transaction(&prepare)
            .unwrap_err()
            .to_string()
            .contains("W receipts"),
        "declaring receipts without their durable prefix must fail"
    );
    assert_eq!(backend.journal_last_sequence(), before);
    for receipt in &prepare.content_receipts {
        backend.put_receipt(receipt).unwrap();
    }
    backend.prepare_namespace_transaction(&prepare).unwrap();
    assert_eq!(
        backend.get_object(&current.object_id).unwrap(),
        Some(current.clone())
    );
    assert!(
        backend
            .get_version(&current.object_id, &captured.version)
            .unwrap()
            .is_none(),
        "prepare remains hidden until the ordinary publication barrier"
    );
    cleanup(&root);
}

#[test]
fn file_history_intent_publication_is_immutable_through_journal_and_compaction() {
    let root = temp_root("file-history-publication-purpose");
    let shard = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), &root).unwrap();
    backend.put_file_system(&file_system()).unwrap();
    let current = history_file("purpose-current", "blake3:current", 1);
    publish(&mut backend, &current);
    let mut intent = DfsWriteIntentRecord {
        publication: crate::DfsWritePublication::Retained {
            capture: Box::new(crate::DfsRetainedWritePublication {
                pin_id: "local-operation".into(),
                source: DfsFileVersionSource::LocalOperation {
                    operation_id: "preserve-local".into(),
                    base_version: Some(current.version.clone()),
                },
            }),
        },
        intent_id: "explicit-retained-content".into(),
        file_system_id: current.file_system_id.clone(),
        object_id: current.object_id.clone(),
        base_version: Some(current.version.clone()),
        target_version: version("purpose-local"),
        coordinator_node_id: current.owner_node_id.clone(),
        operation_id: Some("preserve-local".into()),
        operation_order_token: None,
        owner_epoch: 0,
        desired_replica_node_ids: vec![current.owner_node_id.clone()],
        policy: DfsReplicationPolicy::new(1, 1).unwrap(),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: Some("blake3:local".into()),
        object_metadata: Some(current.clone()),
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: Some(OffsetDateTime::UNIX_EPOCH),
    };
    let entry = backend.put_intent(&intent).unwrap();
    let sequence = backend.journal_last_sequence();
    let mut changed = intent.clone();
    changed.publication = crate::DfsWritePublication::Current;
    assert!(backend.put_intent(&changed).is_err());
    assert_eq!(backend.journal_last_sequence(), sequence);
    let mut bad_entry = entry;
    bad_entry.sequence = sequence + 1;
    bad_entry.mutation = DfsMetadataMutation::put_intent(&changed);
    let batch = DfsMetadataJournalBatch {
        shard_id: shard.clone(),
        entries: vec![bad_entry],
        next_after_sequence: sequence + 1,
    };
    assert!(backend.apply_journal_batch(&batch).is_err());
    assert_eq!(
        backend.journal_last_sequence(),
        sequence,
        "rejected purpose changes must not enter the durable journal"
    );
    assert_eq!(
        backend
            .get_intent(&intent.object_id, &intent.intent_id)
            .unwrap(),
        Some(intent.clone())
    );

    let mut combined = backend
        .journal_batch_since_limited(0, 64, usize::MAX)
        .unwrap()
        .unwrap();
    combined.entries.extend(batch.entries.clone());
    combined.next_after_sequence = batch.next_after_sequence;
    let mut empty_replica =
        LocalSobjMetadataBackend::new(shard.clone(), root.join("malformed-replica")).unwrap();
    assert!(
        empty_replica.apply_journal_batch(&combined).is_err(),
        "the first intent and an incompatible retry can arrive in one batch"
    );
    assert_eq!(
        empty_replica.journal_last_sequence(),
        0,
        "validation must reject the whole batch before persistence"
    );

    intent.status = DfsWriteIntentStatus::Committed;
    backend.put_intent(&intent).unwrap();
    let receipt =
        crate::DfsWriteIntentReceipt::from_intent(&intent, OffsetDateTime::UNIX_EPOCH).unwrap();
    backend
        .put_intent_receipt_for_object(Some(&current), &receipt)
        .unwrap();
    drop(backend);
    let mut reopened = LocalSobjMetadataBackend::new(shard, &root).unwrap();
    assert_eq!(
        reopened
            .get_intent(&intent.object_id, &intent.intent_id)
            .unwrap()
            .unwrap()
            .publication,
        intent.publication
    );
    changed.status = DfsWriteIntentStatus::Committed;
    assert!(reopened.put_intent(&changed).is_err());
    assert_eq!(
        reopened.get_object(&current.object_id).unwrap(),
        Some(current)
    );
    cleanup(&root);
}

#[test]
fn file_history_default_does_not_capture_overwrites_or_deletion() {
    let root = temp_root("file-history-default");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).unwrap();
    backend.put_file_system(&file_system()).unwrap();
    let first = history_file("history-first", "blake3:first", 1);
    let second = history_file("history-second", "blake3:second", 2);
    publish(&mut backend, &first);
    publish(&mut backend, &second);
    assert!(
        backend
            .retained_file_versions_page(&first.file_system_id, None, Some("/"), None, 64)
            .unwrap()
            .0
            .is_empty()
    );
    let mut deleted = second.clone();
    deleted.kind = DfsObjectKind::Tombstone;
    deleted.version = version("history-deleted");
    deleted.content_hash = None;
    backend.put_object(&deleted).unwrap();
    assert!(
        backend
            .versions_for_object(&first.object_id)
            .unwrap()
            .iter()
            .all(|v| v.retained.is_none())
    );
    assert!(
        backend
            .conflicts_for_object(&first.object_id)
            .unwrap()
            .is_empty()
    );
    let mut options = DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0);
    options.now_unix_nanos = Some(u64::MAX);
    backend.gc_metadata_history(options).unwrap();
    assert!(
        backend
            .get_version(&first.object_id, &first.version)
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .get_version(&second.object_id, &second.version)
            .unwrap()
            .is_none()
    );
    cleanup(&root);
}

#[test]
fn file_history_policy_capture_survives_replay_checkpoint_and_age_gc() {
    let root = temp_root("file-history-replay");
    let shard = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), root.join("source")).unwrap();
    backend.put_file_system(&history_fs(0, 10)).unwrap();
    let first = history_file("history-old", "blake3:old", 1);
    let next = history_file("history-next", "blake3:next", 1_000_000_000_000);
    publish(&mut backend, &first);
    publish(&mut backend, &next);
    let retained = backend
        .get_version(&first.object_id, &first.version)
        .unwrap()
        .unwrap();
    let capture = retained.retained.as_ref().expect("explicit policy capture");
    assert_eq!(capture.source, DfsFileVersionSource::RetentionRule);
    let captured_at = capture
        .policy_capture
        .as_ref()
        .unwrap()
        .captured_at_unix_nanos;
    assert!(
        captured_at > next.changed_at_unix_nanos,
        "retention starts at publication, not a file timestamp"
    );
    assert_eq!(
        backend.get_object(&first.object_id).unwrap(),
        Some(next.clone())
    );
    let mut replica = LocalSobjMetadataBackend::new(shard.clone(), root.join("replica")).unwrap();
    let batch = backend
        .journal_batch_since_limited(0, 64, usize::MAX)
        .unwrap()
        .unwrap();
    replica.apply_journal_batch(&batch).unwrap();
    assert_eq!(
        replica
            .get_version(&first.object_id, &first.version)
            .unwrap(),
        Some(retained.clone())
    );
    backend.compact_metadata_table_log_if_needed(0).unwrap();
    drop(backend);
    let mut backend = LocalSobjMetadataBackend::new(shard, root.join("source")).unwrap();
    assert_eq!(
        backend
            .get_version(&first.object_id, &first.version)
            .unwrap(),
        Some(retained)
    );
    let before_expiry = DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
        .with_now_unix_nanos(captured_at + 9_000_000_000);
    backend.gc_metadata_history(before_expiry).unwrap();
    assert!(
        backend
            .get_version(&first.object_id, &first.version)
            .unwrap()
            .is_some()
    );
    let after_expiry = DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
        .with_now_unix_nanos(captured_at + 10_000_000_000);
    backend.gc_metadata_history(after_expiry).unwrap();
    assert!(
        backend
            .get_version(&first.object_id, &first.version)
            .unwrap()
            .is_none()
    );
    assert_eq!(backend.get_object(&first.object_id).unwrap(), Some(next));
    cleanup(&root);
}

#[test]
fn file_history_captures_delete_but_not_attributes_or_source_eviction() {
    let root = temp_root("file-history-delete");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).unwrap();
    backend.put_file_system(&history_fs(2, 0)).unwrap();
    let first = history_file("history-initial", "blake3:old", 1);
    publish(&mut backend, &first);
    let mut attrs = first.clone();
    attrs.version = version("history-attrs");
    attrs.mode = 0o600;
    publish(&mut backend, &attrs);
    assert!(
        backend
            .get_version(&first.object_id, &first.version)
            .unwrap()
            .unwrap()
            .retained
            .is_none()
    );
    let mut removed = attrs.clone();
    removed.kind = DfsObjectKind::Tombstone;
    removed.version = version("history-removed");
    removed.content_hash = None;
    removed.changed_at_unix_nanos = 20;
    backend.put_object(&removed).unwrap();
    let captured = backend
        .get_version(&attrs.object_id, &attrs.version)
        .unwrap()
        .unwrap();
    assert_eq!(captured.retained.as_ref().unwrap().object.mode, 0o600);
    assert_eq!(captured.content_hash, attrs.content_hash);
    let mut eviction_tables = DfsMetadataTables::default();
    eviction_tables
        .file_systems
        .insert(first.file_system_id.clone(), history_fs(2, 0));
    eviction_tables
        .objects
        .insert(first.object_id.clone(), first.clone());
    removed.tombstone_provenance = Some(crate::DfsTombstoneProvenance::SourceEviction {
        source_shard_id: DfsMetadataShardId::new("root"),
        target_shard_id: DfsMetadataShardId::new("target"),
        pin_transfer_id: None,
    });
    assert!(
        crate::file_history::policy_capture_table_ops(&eviction_tables, &removed, Some(&first))
            .is_empty()
    );
    cleanup(&root);
}

#[test]
fn file_history_stale_commit_does_not_create_a_capture_or_conflict() {
    let root = temp_root("file-history-stale-commit");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).unwrap();
    backend.put_file_system(&history_fs(2, 0)).unwrap();
    let first = history_file("history-current", "blake3:current", 1);
    publish(&mut backend, &first);
    let rejected = history_file("history-rejected", "blake3:rejected", 2);
    let sequence = backend.journal_last_sequence();
    assert!(
        backend
            .commit_visible_version(
                &version("history-stale"),
                &rejected,
                &crate::metadata_records::object_version_record(&rejected, Some(5)),
                &[]
            )
            .is_err()
    );
    assert_eq!(backend.journal_last_sequence(), sequence);
    assert!(
        backend
            .get_version(&first.object_id, &first.version)
            .unwrap()
            .unwrap()
            .retained
            .is_none()
    );
    assert!(
        backend
            .get_version(&first.object_id, &rejected.version)
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .conflicts_for_object(&first.object_id)
            .unwrap()
            .is_empty()
    );
    cleanup(&root);
}

#[test]
fn file_history_explicit_capture_and_pin_are_one_replayable_mutation() {
    let root = temp_root("file-history-explicit-pin");
    let shard = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), root.join("source")).unwrap();
    backend.put_file_system(&file_system()).unwrap();
    let current = history_file("history-checkpoint", "blake3:checkpoint", 1);
    publish(&mut backend, &current);
    let mut capture = backend
        .get_version(&current.object_id, &current.version)
        .unwrap()
        .unwrap();
    capture.retained = Some(Box::new(crate::DfsRetainedFileVersion {
        object: Box::new(current.clone()),
        captured_at_unix_nanos: 5,
        source: DfsFileVersionSource::Checkpoint {
            operation_id: "checkpoint-operation".into(),
        },
        policy_capture: None,
    }));
    let pin = DfsMetadataVersionPinRecord {
        file_system_id: current.file_system_id.clone(),
        object_id: current.object_id.clone(),
        version: current.version.clone(),
        pin_id: "before-upgrade".into(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    let before = backend.journal_last_sequence();
    backend
        .capture_file_version(&capture, &pin, Some(&current.version))
        .unwrap();
    assert_eq!(backend.journal_last_sequence(), before + 1);
    let history = backend
        .retained_file_versions_page(
            &current.file_system_id,
            Some(&current.object_id),
            Some("/docs"),
            None,
            64,
        )
        .unwrap()
        .0;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].pins, vec!["before-upgrade"]);
    let next = history_file("history-after-checkpoint", "blake3:after", 10);
    publish(&mut backend, &next);
    assert_eq!(
        backend
            .capture_file_version(&capture, &pin, Some(&current.version))
            .unwrap(),
        None
    );
    let mut replica = LocalSobjMetadataBackend::new(shard, root.join("replica")).unwrap();
    replica
        .apply_journal_batch(
            &backend
                .journal_batch_since_limited(0, 64, usize::MAX)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(replica.get_object(&current.object_id).unwrap(), Some(next));
    assert_eq!(
        replica
            .get_version(&current.object_id, &current.version)
            .unwrap(),
        Some(capture.clone())
    );
    assert_eq!(
        replica.version_pins_for_object(&current.object_id).unwrap(),
        vec![pin.clone()]
    );
    backend
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(u64::MAX),
        )
        .unwrap();
    assert_eq!(
        backend
            .get_version(&current.object_id, &current.version)
            .unwrap(),
        Some(capture)
    );
    backend
        .remove_version_pin(
            &pin.file_system_id,
            &pin.object_id,
            &pin.version,
            &pin.pin_id,
        )
        .unwrap();
    assert!(
        backend
            .retained_file_versions_page(&current.file_system_id, None, Some("/"), None, 64)
            .unwrap()
            .0
            .is_empty()
    );
    backend
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(u64::MAX),
        )
        .unwrap();
    assert!(
        backend
            .get_version(&current.object_id, &current.version)
            .unwrap()
            .is_none()
    );
    cleanup(&root);
}

#[test]
fn file_history_capture_rejects_mismatched_content_without_partial_pins() {
    let root = temp_root("file-history-invalid-capture");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).unwrap();
    backend.put_file_system(&file_system()).unwrap();
    let current = history_file("history-current", "blake3:current", 1);
    publish(&mut backend, &current);
    let mut capture = backend
        .get_version(&current.object_id, &current.version)
        .unwrap()
        .unwrap();
    let mut forged = current.clone();
    forged.content_hash = Some("blake3:forged".into());
    capture.retained = Some(Box::new(crate::DfsRetainedFileVersion {
        object: Box::new(forged),
        captured_at_unix_nanos: 5,
        source: DfsFileVersionSource::Checkpoint {
            operation_id: "capture".into(),
        },
        policy_capture: None,
    }));
    let pin = DfsMetadataVersionPinRecord {
        file_system_id: current.file_system_id.clone(),
        object_id: current.object_id.clone(),
        version: current.version.clone(),
        pin_id: "forged".into(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    let before = backend.journal_last_sequence();
    assert!(
        backend
            .capture_file_version(&capture, &pin, Some(&current.version))
            .is_err()
    );
    assert_eq!(before, backend.journal_last_sequence());
    assert!(
        backend
            .version_pins_for_object(&current.object_id)
            .unwrap()
            .is_empty()
    );
    cleanup(&root);
}

#[test]
fn file_history_explicit_local_operation_uses_history_without_an_alternate_head() {
    let root = temp_root("file-history-local-operation");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).unwrap();
    backend.put_file_system(&file_system()).unwrap();
    let current = history_file("history-current", "blake3:current", 1);
    publish(&mut backend, &current);
    let candidate = history_file("history-local-candidate", "blake3:local", 2);
    let mut capture = crate::metadata_records::object_version_record(&candidate, Some(5));
    capture.visible = false;
    capture.retained = Some(Box::new(crate::DfsRetainedFileVersion {
        object: Box::new(candidate.clone()),
        captured_at_unix_nanos: 2,
        source: DfsFileVersionSource::LocalOperation {
            operation_id: "preserve-local-edit".into(),
            base_version: Some(current.version.clone()),
        },
        policy_capture: None,
    }));
    let pin = DfsMetadataVersionPinRecord {
        file_system_id: candidate.file_system_id.clone(),
        object_id: candidate.object_id.clone(),
        version: candidate.version.clone(),
        pin_id: "local-edit".into(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    backend
        .capture_file_version(&capture, &pin, Some(&current.version))
        .unwrap();
    assert_eq!(
        backend.get_object(&current.object_id).unwrap(),
        Some(current.clone())
    );
    assert_eq!(
        backend
            .get_object_by_path(&current.file_system_id, &current.path)
            .unwrap(),
        Some(current.clone())
    );
    assert_eq!(
        backend
            .get_version(&current.object_id, &candidate.version)
            .unwrap(),
        Some(capture)
    );
    assert!(
        backend
            .conflicts_for_object(&current.object_id)
            .unwrap()
            .is_empty()
    );
    cleanup(&root);
}

#[test]
fn file_history_count_uses_authoritative_order_across_equal_and_reversed_clocks() {
    for last_time in [90, 100] {
        let root = temp_root(&format!("history-clock-order-{last_time}"));
        let shard = DfsMetadataShardId::new("root");
        let mut source = LocalSobjMetadataBackend::new(shard.clone(), root.join("source")).unwrap();
        source.put_file_system(&history_fs(1, 0)).unwrap();
        let first = history_file("history-order-a", "blake3:a", 1);
        let second = history_file("history-order-b", "blake3:b", 2);
        let third = history_file("history-order-c", "blake3:c", 3);
        for object in [&first, &second, &third] {
            publish(&mut source, object);
        }
        let mut batch = source
            .journal_batch_since_limited(0, 128, usize::MAX)
            .unwrap()
            .unwrap();
        for entry in &mut batch.entries {
            if let DfsMetadataMutation::PutObject {
                object: Some(object),
                ..
            } = &entry.mutation
            {
                let seconds = if object.version == third.version {
                    last_time
                } else {
                    100
                };
                entry.created_at = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(seconds);
            }
        }
        let mut replica =
            LocalSobjMetadataBackend::new(shard.clone(), root.join("replica")).unwrap();
        replica.apply_journal_batch(&batch).unwrap();
        let a = replica
            .get_version(&first.object_id, &first.version)
            .unwrap()
            .unwrap();
        let b = replica
            .get_version(&second.object_id, &second.version)
            .unwrap()
            .unwrap();
        assert_eq!(
            a.retained
                .as_ref()
                .unwrap()
                .policy_capture
                .as_ref()
                .unwrap()
                .order,
            1
        );
        assert_eq!(
            b.retained
                .as_ref()
                .unwrap()
                .policy_capture
                .as_ref()
                .unwrap()
                .order,
            2
        );
        assert!(
            a.retained.as_ref().unwrap().captured_at_unix_nanos
                >= b.retained.as_ref().unwrap().captured_at_unix_nanos
        );
        replica
            .gc_metadata_history(
                DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                    .with_now_unix_nanos(u64::MAX),
            )
            .unwrap();
        assert!(
            replica
                .get_version(&first.object_id, &first.version)
                .unwrap()
                .is_none()
        );
        assert!(
            replica
                .get_version(&second.object_id, &second.version)
                .unwrap()
                .is_some()
        );
        replica.compact_metadata_table_log_if_needed(0).unwrap();
        drop(replica);
        let replica = LocalSobjMetadataBackend::new(shard, root.join("replica")).unwrap();
        assert_eq!(
            replica
                .get_version(&third.object_id, &third.version)
                .unwrap()
                .unwrap()
                .history_order,
            2
        );
        cleanup(&root);
    }
}

#[test]
fn file_history_counter_survives_policy_changes_collection_gaps_and_migration() {
    let root = temp_root("history-counter-gaps");
    let mut source =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("source"), root.join("source"))
            .unwrap();
    source.put_file_system(&history_fs(2, 0)).unwrap();
    let versions = [
        history_file("history-gap-a", "blake3:a", 1),
        history_file("history-gap-b", "blake3:b", 2),
        history_file("history-gap-c", "blake3:c", 3),
        history_file("history-gap-d", "blake3:d", 4),
    ];
    publish(&mut source, &versions[0]);
    publish(&mut source, &versions[1]);
    let pin = DfsMetadataVersionPinRecord {
        file_system_id: versions[0].file_system_id.clone(),
        object_id: versions[0].object_id.clone(),
        version: versions[0].version.clone(),
        pin_id: "old-independent-pin".into(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    source.put_version_pin(&pin).unwrap();
    let config = history_fs(0, 1);
    source.put_file_system(&config).unwrap();
    publish(&mut source, &versions[2]);
    publish(&mut source, &versions[3]);
    source
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(u64::MAX),
        )
        .unwrap();
    assert!(
        source
            .get_version(&versions[1].object_id, &versions[1].version)
            .unwrap()
            .is_none()
    );
    assert!(
        source
            .get_version(&versions[2].object_id, &versions[2].version)
            .unwrap()
            .is_none()
    );
    let bundle = crate::metadata_shard_migration::collect_object_shard_migration_bundle(
        &source,
        &versions[0].object_id,
        16,
    )
    .unwrap();
    let target_shard = DfsMetadataShardId::new("target");
    let mut target =
        LocalSobjMetadataBackend::new(target_shard.clone(), root.join("target")).unwrap();
    crate::metadata_shard_migration::install_object_shard_migration_bundle(
        &mut target,
        &config,
        &bundle,
        &target_shard,
        16,
    )
    .unwrap();
    assert_eq!(
        target
            .get_version(&versions[3].object_id, &versions[3].version)
            .unwrap()
            .unwrap()
            .history_order,
        3
    );
    target
        .remove_version_pin(
            &pin.file_system_id,
            &pin.object_id,
            &pin.version,
            &pin.pin_id,
        )
        .unwrap();
    target
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(u64::MAX),
        )
        .unwrap();
    assert!(
        target
            .get_version(&versions[0].object_id, &versions[0].version)
            .unwrap()
            .is_none(),
        "collecting newer captures must not renew an old count grant"
    );
    cleanup(&root);
}

#[test]
fn file_history_exhausted_order_rejects_before_publishing() {
    let root = temp_root("history-order-exhausted");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).unwrap();
    backend.put_file_system(&history_fs(1, 0)).unwrap();
    let current = history_file("history-full-counter", "blake3:current", 1);
    publish(&mut backend, &current);
    let mut record = backend
        .get_version(&current.object_id, &current.version)
        .unwrap()
        .unwrap();
    record.history_order = u64::MAX;
    backend.put_version(&record).unwrap();
    let next = history_file("history-overflow", "blake3:next", 2);
    let head = backend.journal_last_sequence();
    assert!(
        backend
            .put_object(&next)
            .unwrap_err()
            .to_string()
            .contains("counter is exhausted")
    );
    assert_eq!(backend.journal_last_sequence(), head);
    assert_eq!(
        backend.get_object(&current.object_id).unwrap(),
        Some(current)
    );
    cleanup(&root);
}

#[test]
fn file_history_age_starts_at_prepared_publication_and_replays_exactly() {
    let root = temp_root("history-prepared-publication-time");
    let shard = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), root.join("source")).unwrap();
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV3);
    backend.put_file_system(&history_fs(0, 10)).unwrap();
    let current = history_file("history-before-delayed-prepare", "blake3:before", 1);
    let next = history_file("history-after-delayed-prepare", "blake3:after", 2);
    publish(&mut backend, &current);
    let prepare = namespace_prepare("history-delayed-prepare", "root", &current, next);
    backend.prepare_namespace_transaction(&prepare).unwrap();
    assert!(
        backend
            .get_version(&current.object_id, &current.version)
            .unwrap()
            .unwrap()
            .retained
            .is_none()
    );
    let commit = backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .unwrap();
    let captured = backend
        .get_version(&current.object_id, &current.version)
        .unwrap()
        .unwrap();
    let captured_at = captured
        .retained
        .as_ref()
        .unwrap()
        .policy_capture
        .as_ref()
        .unwrap()
        .captured_at_unix_nanos;
    assert_eq!(
        captured_at,
        u64::try_from(commit.created_at.unix_timestamp_nanos()).unwrap()
    );
    assert!(
        captured_at > u64::try_from(prepare.transaction.created_at.unix_timestamp_nanos()).unwrap()
    );
    let mut replica = LocalSobjMetadataBackend::new(shard, root.join("replica")).unwrap();
    replica
        .apply_journal_batch(
            &backend
                .journal_batch_since_limited(0, 128, usize::MAX)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        replica
            .get_version(&current.object_id, &current.version)
            .unwrap(),
        Some(captured.clone())
    );
    replica.compact_metadata_table_log_if_needed(0).unwrap();
    drop(replica);
    let replica =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("replica"))
            .unwrap();
    assert_eq!(
        replica
            .get_version(&current.object_id, &current.version)
            .unwrap(),
        Some(captured)
    );
    cleanup(&root);
}

#[test]
fn file_history_deleted_counter_expires_after_the_last_retained_generation() {
    let root = temp_root("history-deleted-counter");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).unwrap();
    backend.put_file_system(&history_fs(0, 1)).unwrap();
    let first = history_file("history-deleted-counter-first", "blake3:first", 1);
    let next = history_file("history-deleted-counter-next", "blake3:next", 2);
    publish(&mut backend, &first);
    publish(&mut backend, &next);
    let mut tombstone = next.clone();
    tombstone.version = version("history-deleted-counter-tombstone");
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.content_hash = None;
    tombstone.content_size_bytes = None;
    backend.put_object(&tombstone).unwrap();
    assert_eq!(
        backend
            .get_version(&tombstone.object_id, &tombstone.version)
            .unwrap()
            .unwrap()
            .history_order,
        2
    );
    for _ in 0..2 {
        backend
            .gc_metadata_history(
                DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                    .with_now_unix_nanos(u64::MAX),
            )
            .unwrap();
    }
    assert!(
        backend
            .versions_for_object(&tombstone.object_id)
            .unwrap()
            .is_empty()
    );
    let report = backend
        .compact_tombstones(
            DfsTombstoneCompactionOptions::compact_eligible(Duration::ZERO)
                .with_now_unix_nanos(u64::MAX),
        )
        .unwrap();
    assert_eq!(report.compacted_tombstone_count, 1);
    cleanup(&root);
}
