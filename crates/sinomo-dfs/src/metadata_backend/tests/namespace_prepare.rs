//! Namespace prepare regression tests.

use super::*;

#[test]
fn namespace_prepare_table_log_references_authoritative_journal_and_reopens() {
    let root = temp_root("local_backend_prepare_table_journal_reference");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let current = object(version("version_prepare_table_reference_1"));
    let mut committed = current.clone();
    committed.version = version("version_prepare_table_reference_2");
    committed.mode = 0o600;
    let prepare = namespace_prepare(
        "operation-prepare-table-reference",
        &shard_id.0,
        &current,
        committed,
    );

    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV1);
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    let entry = backend
        .prepare_namespace_transaction(&prepare)
        .expect("durable hidden prepare");
    assert!(matches!(
        &entry.mutation,
        DfsMetadataMutation::PutNamespacePrepare {
            prepare: None,
            packed_prepare_v1: Some(_),
            ..
        }
    ));
    let authoritative_sequence = backend.journal_last_sequence();
    let authoritative_snapshot = backend.export_table_snapshot();

    let prepare_batch = metadata_table_batches(&root)
        .into_iter()
        .find(|batch| batch.sequence == entry.sequence)
        .expect("prepare table batch");
    assert!(
        prepare_batch.ops.iter().any(|op| matches!(
            op,
            DfsMetadataTableOp::PutNamespacePrepareJournalReference {
                shard_id: referenced_shard_id,
                journal_sequence,
                operation_id,
                operation_digest,
                owner_epoch,
                status: DfsNamespacePrepareStatus::Prepared,
                ..
            } if referenced_shard_id == &shard_id
                && *journal_sequence == entry.sequence
                && operation_id == &prepare.operation_id
                && operation_digest == &prepare.operation_digest
                && *owner_epoch == prepare.owner_epoch
        )),
        "the rebuildable table batch must retain a fenced journal reference"
    );
    assert!(
        !prepare_batch
            .ops
            .iter()
            .any(|op| matches!(op, DfsMetadataTableOp::PutNamespacePrepare { .. })),
        "the table log must not encode the full prepared transaction a second time"
    );
    let authoritative_record_bytes =
        sinomo_object_serde::to_bytes(&entry, 1, sinomo_core::object::LengthMode::U32)
            .expect("encode authoritative prepare for test accounting")
            .len()
            .saturating_add(std::mem::size_of::<u32>());
    assert!(
        prepare_batch.journal_reference_replay_cost_bytes
            >= u64::try_from(authoritative_record_bytes).unwrap_or(u64::MAX),
        "the compact table batch must retain at least the authoritative encoded replay cost"
    );

    drop(backend);
    let reopened =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopened backend");
    assert_eq!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("reopened prepare"),
        Some(prepare.clone()),
        "table replay must resolve the complete prepare from the journal"
    );
    drop(reopened);

    let segment_id = crate::metadata_journal::journal_segment_id(entry.sequence);
    fs::remove_file(
        root.join("journal")
            .join("segments")
            .join(format!("{segment_id:020}.slog")),
    )
    .expect("remove referenced journal segment");
    let error = match LocalSobjMetadataBackend::new(shard_id.clone(), &root) {
        Ok(_) => panic!("missing referenced journal entry must fail closed"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("cannot resolve namespace prepare"),
        "unexpected missing journal reference error: {error}"
    );

    let repair_only = LocalSobjMetadataBackend::open_for_replica_repair(shard_id.clone(), &root)
        .expect("non-primary repair-only open must ignore the unresolved table-log suffix");
    assert_eq!(
        repair_only
            .get_namespace_prepare(&prepare.operation_id)
            .expect("repair-only snapshot read"),
        None,
        "repair-only reads must expose only the last self-contained snapshot"
    );
    drop(repair_only);

    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        prepare.file_system_id.clone(),
        shard_id.clone(),
        node("node_prepare_table_repair_source"),
        1,
    );
    let (repaired, archived) =
        LocalSobjMetadataBackend::replace_unreadable_with_replicated_snapshot(
            &permit,
            shard_id.clone(),
            &root,
            shard_id.clone(),
            authoritative_sequence,
            authoritative_snapshot,
        )
        .expect("install authoritative snapshot over unresolved table reference");
    assert!(archived);
    assert_eq!(
        repaired
            .get_namespace_prepare(&prepare.operation_id)
            .expect("repaired prepare"),
        Some(prepare.clone())
    );
    drop(repaired);

    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen repaired backend");
    assert_eq!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("reopened repaired prepare"),
        Some(prepare)
    );
    cleanup(&root);
}

#[test]
fn packed_namespace_prepare_replays_with_exact_prefix_and_canonical_snapshot() {
    let root = temp_root("packed_namespace_prepare_replay");
    let source_root = root.join("source");
    let replica_root = root.join("replica");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let current = object(version("version_packed_prepare_base"));
    let mut committed = current.clone();
    committed.version = version("version_packed_prepare_committed");
    committed.mode = 0o600;
    let prepare = namespace_prepare("operation-packed-prepare", &shard_id.0, &current, committed);
    let mut config = shard_config(&shard_id.0, DfsMetadataShardAssignment::Root);
    config.epoch = prepare.owner_epoch;
    config.namespace_prepare_codec = DfsMetadataNamespacePrepareCodec::PackedV2;

    let mut source =
        LocalShardedMetadataBackend::new(&source_root, vec![config.clone()]).expect("source");
    source.put_file_system(&file_system()).expect("filesystem");
    source.put_object(&current).expect("current object");
    crate::metadata_records::reset_namespace_prepare_payload_validation_passes();
    source
        .prepare_namespace_transaction(&prepare)
        .expect("packed namespace prepare");
    assert_eq!(
        crate::metadata_records::namespace_prepare_payload_validation_passes(),
        0,
        "trusted packed-v2 construction and its journal-reference table op must not rescan the freshly built body"
    );

    let entries = source.journal_since(0).expect("source journal");
    let prepare_entry = entries.last().expect("prepare journal entry");
    let DfsMetadataMutation::PutNamespacePrepare {
        prepare: canonical,
        packed_prepare_v2,
        ..
    } = &prepare_entry.mutation
    else {
        panic!("last journal entry must be a namespace prepare");
    };
    assert!(
        canonical.is_none(),
        "packed activation must not retain a second canonical body"
    );
    assert!(
        packed_prepare_v2.is_some(),
        "packed activation must emit the packed v2 body"
    );
    let catch_up_batch = source
        .shard_backend(&shard_id)
        .expect("source shard backend")
        .journal_batch_since_limited(prepare_entry.sequence.saturating_sub(1), 1, 8 * 1024 * 1024)
        .expect("hydrate packed-v2 catch-up batch")
        .expect("packed-v2 catch-up batch");
    assert!(matches!(
        catch_up_batch.entries.as_slice(),
        [DfsMetadataJournalEntry {
            mutation: DfsMetadataMutation::PutNamespacePrepare {
                prepare: None,
                packed_prepare_v1: None,
                packed_prepare_v2: Some(_),
                ..
            },
            ..
        }]
    ));
    require_journal_entry_replay_payloads(&catch_up_batch.entries[0])
        .expect("hydration must preserve the sole packed-v2 representation");
    assert_eq!(
        prepare_entry
            .mutation
            .namespace_prepare_payload()
            .expect("decode packed prepare")
            .as_deref(),
        Some(&prepare)
    );
    let source_prefix = DfsMetadataBackend::metadata_prefix_proof(&source, &shard_id)
        .expect("source prefix proof")
        .expect("source shard prefix proof");
    let operation = crate::service_replica::metadata_authority_operation_for_namespace_prepare(
        &prepare,
        prepare_entry.sequence,
    )
    .expect("namespace authority operation");
    let probe = DfsMetadataAuthorityProbe {
        probe_id: "probe-packed-prepare".to_string(),
        expected_shard: config.clone(),
        operation,
        durable_journal_ack: false,
        journal_frames: Vec::new(),
        prepared_prefix: Some(source_prefix.clone()),
    };
    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        config.file_system_id.clone(),
        shard_id.clone(),
        config.primary_node_id.clone(),
        config.epoch,
    );
    let replay_batch = DfsMetadataJournalBatch {
        shard_id: shard_id.clone(),
        next_after_sequence: prepare_entry.sequence,
        entries: entries.clone(),
    };

    let mut replica =
        LocalShardedMetadataBackend::new(&replica_root, vec![config.clone()]).expect("replica");
    crate::metadata_records::reset_namespace_prepare_payload_validation_passes();
    crate::service_replica::reset_namespace_prepare_canonical_digest_passes();
    let apply_report = replica
        .apply_journal_batch_with_replica_permit_report(&permit, &replay_batch)
        .expect("incremental packed replay");
    assert_eq!(apply_report.applied_entries(), replay_batch.entries.len());
    assert_eq!(
        crate::metadata_records::namespace_prepare_payload_validation_passes(),
        1,
        "replica persistence, materialization, and journal-reference planning must share one packed-v2 validation proof"
    );
    assert_eq!(
        crate::service_replica::namespace_prepare_canonical_digest_passes(),
        1,
        "a newly materialized packed prepare needs one independent canonical digest"
    );
    let confirmation_id =
        crate::service_replica::verify_persisted_metadata_authority_probe_after_apply(
            &replica,
            &config,
            &probe,
            &apply_report,
        )
        .expect("fresh replica authority proof");
    assert_eq!(
        Some(confirmation_id),
        crate::service_replica::metadata_authority_probe_confirmation_id(&probe)
    );
    assert_eq!(
        crate::service_replica::namespace_prepare_canonical_digest_passes(),
        1,
        "the immediate authority ACK must reuse the apply digest instead of rehashing the table record"
    );
    let mut mismatched_probe = probe.clone();
    let DfsMetadataAuthorityOperation::NamespaceMutation { payload_digest, .. } =
        &mut mismatched_probe.operation
    else {
        unreachable!("fixture creates a namespace operation")
    };
    *payload_digest = "blake3:mismatched-packed-prepare".to_string();
    crate::service_replica::reset_namespace_prepare_canonical_digest_passes();
    let mismatch_error =
        crate::service_replica::verify_persisted_metadata_authority_probe_after_apply(
            &replica,
            &config,
            &mismatched_probe,
            &apply_report,
        )
        .expect_err("mismatched compact proof must use and fail durable verification");
    assert!(
        mismatch_error
            .to_string()
            .contains("mismatched operation digest")
    );
    assert_eq!(
        crate::service_replica::namespace_prepare_canonical_digest_passes(),
        1,
        "compact evidence must never bypass durable verification for a different operation"
    );
    assert_eq!(
        DfsMetadataBackend::metadata_prefix_proof(&replica, &shard_id)
            .expect("replica prefix proof")
            .expect("replica shard prefix proof"),
        source_prefix,
        "replica prefix hashing must preserve the exact packed journal bytes"
    );
    assert_eq!(
        replica
            .get_namespace_prepare(&prepare.operation_id)
            .expect("replica prepare"),
        Some(prepare.clone())
    );

    crate::metadata_records::reset_namespace_prepare_payload_validation_passes();
    crate::service_replica::reset_namespace_prepare_canonical_digest_passes();
    let duplicate_report = replica
        .apply_journal_batch_with_replica_permit_report(&permit, &replay_batch)
        .expect("duplicate packed replay");
    assert_eq!(duplicate_report.applied_entries(), 0);
    crate::service_replica::verify_persisted_metadata_authority_probe_after_apply(
        &replica,
        &config,
        &probe,
        &duplicate_report,
    )
    .expect("duplicate replay durable fallback");
    assert_eq!(
        crate::service_replica::namespace_prepare_canonical_digest_passes(),
        1,
        "a duplicate apply has no fresh proof and must independently verify durable state"
    );

    let snapshot = source
        .export_shard_table_snapshot(&shard_id)
        .expect("export shard snapshot")
        .expect("source shard snapshot");
    assert!(
        snapshot
            .tables
            .as_ref()
            .expect("structured snapshot tables")
            .namespace_prepares
            .contains(&prepare),
        "snapshots must remain canonical across journal codec changes"
    );

    drop(replica);
    crate::metadata_records::reset_namespace_prepare_payload_validation_passes();
    crate::service_replica::reset_namespace_prepare_canonical_digest_passes();
    let replica = LocalShardedMetadataBackend::new(&replica_root, vec![config.clone()])
        .expect("reopen replica");
    assert!(
        crate::metadata_records::namespace_prepare_payload_validation_passes() <= 1,
        "reopen must never validate a journal-referenced packed-v2 prepare more than once"
    );
    assert_eq!(
        replica
            .get_namespace_prepare(&prepare.operation_id)
            .expect("reopened replica prepare"),
        Some(prepare.clone()),
        "journal-referenced table replay must resolve packed prepare bodies"
    );
    crate::service_replica::verify_persisted_metadata_authority_probe_after_apply(
        &replica,
        &config,
        &probe,
        &DfsMetadataReplicaApplyReport::default(),
    )
    .expect("reopened replica durable authority proof");
    assert_eq!(
        crate::service_replica::namespace_prepare_canonical_digest_passes(),
        1,
        "recovery must retain the independent durable-table verification path"
    );

    let mut ambiguous = prepare_entry.clone();
    let DfsMetadataMutation::PutNamespacePrepare {
        prepare: canonical,
        packed_prepare_v2,
        ..
    } = &mut ambiguous.mutation
    else {
        panic!("prepare entry changed mutation kind");
    };
    assert!(packed_prepare_v2.is_some());
    *canonical = Some(Box::new(prepare));
    let error = require_journal_entry_replay_payloads(&ambiguous)
        .expect_err("dual prepare representations must fail replay");
    assert!(
        error.to_string().contains("both canonical and packed"),
        "unexpected ambiguous prepare error: {error}"
    );

    cleanup(&root);
}

#[test]
fn local_backend_namespace_prepare_is_hidden_atomic_and_snapshot_durable() {
    let root = temp_root("local_backend_namespace_prepare_hidden_atomic_snapshot");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let file_system = file_system();
    let current = object(version("version_prepare_1"));
    let mut committed = current.clone();
    committed.version = version("version_prepare_2");
    committed.changed_at_unix_nanos = 2;
    let prepare = namespace_prepare(
        "operation-hidden-commit",
        &shard_id.0,
        &current,
        committed.clone(),
    );
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&current).expect("current object");

    crate::metadata_tables::reset_namespace_prepare_footprint_constructions();
    let prepare_entry = backend
        .prepare_namespace_transaction(&prepare)
        .expect("prepare namespace transaction");
    assert_eq!(
        crate::metadata_tables::namespace_prepare_footprint_constructions(),
        1,
        "prepare admission must construct its validated footprint once"
    );
    assert!(matches!(
        prepare_entry.mutation,
        DfsMetadataMutation::PutNamespacePrepare {
            status: DfsNamespacePrepareStatus::Prepared,
            ..
        }
    ));
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("visible object"),
        Some(current.clone()),
        "installing a hidden prepare must not apply its namespace transaction"
    );
    assert_eq!(
        backend
            .get_namespace_prepare(&prepare.operation_id)
            .expect("durable prepare"),
        Some(prepare.clone())
    );

    backend
        .compact_metadata_table_log_if_needed(0)
        .expect("force prepare snapshot");
    drop(backend);
    let mut reopened =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopen prepared");
    assert_eq!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("snapshotted prepare"),
        Some(prepare.clone()),
        "structured snapshots must preserve hidden prepares"
    );
    assert!(
        reopened
            .tables
            .namespace_prepare_footprint_indexes_match_history(),
        "structured snapshot hydration must reconstruct active prepare ownership"
    );
    assert_eq!(
        reopened
            .get_object(&current.object_id)
            .expect("reopened visible object"),
        Some(current.clone())
    );

    let fence_error = reopened
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            "blake3:different-operation",
            prepare.owner_epoch,
        )
        .expect_err("digest reuse must be fenced");
    assert!(fence_error.to_string().contains("digest/epoch fence"));
    assert_eq!(
        reopened
            .get_object(&current.object_id)
            .expect("visible object after bad fence"),
        Some(current.clone())
    );

    reset_namespace_prepared_commit_reference_planner_calls();
    crate::metadata_tables::reset_namespace_prepare_footprint_constructions();
    let commit_entry = reopened
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("commit prepared namespace transaction");
    assert_eq!(
        crate::metadata_tables::namespace_prepare_footprint_constructions(),
        0,
        "normal Prepared -> Committed publication must not rebuild a validated footprint"
    );
    assert_eq!(
        namespace_prepared_commit_reference_planner_calls(),
        0,
        "the normal prepared commit must preapply directly instead of using the reference clone planner"
    );
    assert!(matches!(
        commit_entry.mutation,
        DfsMetadataMutation::CommitNamespacePrepare { .. }
    ));
    assert_eq!(
        reopened
            .get_object(&current.object_id)
            .expect("committed object"),
        Some(committed.clone())
    );
    assert!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("committed prepare")
            .is_none(),
        "committing must release the full prepared body"
    );
    let prepare_receipt = reopened
        .tables
        .namespace_prepare_receipts
        .get(&prepare.operation_id)
        .cloned()
        .expect("compact committed prepare receipt");
    assert_eq!(prepare_receipt.status, DfsNamespacePrepareStatus::Committed);
    assert_eq!(prepare_receipt.retained_at, commit_entry.created_at);
    assert!(
        reopened
            .tables
            .namespace_prepare_footprint_indexes_match_history(),
        "Prepared -> Committed table apply must release active ownership"
    );
    assert!(
        reopened
            .get_namespace_transaction(&prepare.transaction.transaction_id)
            .expect("committed transaction")
            .is_none(),
        "the canonical table state and compact receipt replace the full transaction"
    );
    let transaction_receipt = reopened
        .tables
        .namespace_transaction_receipts
        .get(&prepare.transaction.transaction_id)
        .cloned()
        .expect("compact committed transaction receipt");
    assert!(
        transaction_receipt
            .matches_transaction(&prepare.transaction)
            .expect("transaction receipt identity")
    );
    assert_eq!(transaction_receipt.retained_at, commit_entry.created_at);
    let repeated_commit = reopened
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("repeated committed transaction uses the idempotent recovery path");
    assert!(matches!(
        repeated_commit.mutation,
        DfsMetadataMutation::PutNamespacePrepareReceipt { .. }
    ));
    assert_eq!(
        reopened
            .get_object(&current.object_id)
            .expect("object after repeated commit"),
        Some(committed.clone())
    );

    drop(reopened);
    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen committed");
    assert_eq!(
        reopened
            .get_object(&current.object_id)
            .expect("durable committed object"),
        Some(committed)
    );
    assert!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("durable committed prepare")
            .is_none()
    );
    assert_eq!(
        reopened
            .tables
            .namespace_prepare_receipts
            .get(&prepare.operation_id),
        Some(&prepare_receipt)
    );
    assert_eq!(
        reopened
            .tables
            .namespace_transaction_receipts
            .get(&prepare.transaction.transaction_id),
        Some(&transaction_receipt)
    );
    assert!(
        reopened
            .tables
            .namespace_prepare_footprint_indexes_match_history(),
        "snapshot plus table-log replay must retain terminal release"
    );
    let mut reuse_after_reopen = prepare;
    reuse_after_reopen.operation_id = "operation-reuse-after-reopen".to_string();
    reuse_after_reopen.transaction.transaction_id = "transaction:reuse-after-reopen".to_string();
    reset_namespace_prepare_history_scan_calls();
    let footprint = namespace_prepare_footprint(&reuse_after_reopen);
    validate_namespace_prepare_does_not_overlap(&reopened.tables, &reuse_after_reopen, &footprint)
        .expect("terminal history must not block its released footprint after reopen");
    assert_eq!(namespace_prepare_history_scan_calls(), 0);

    cleanup(&root);
}

#[test]
fn local_backend_namespace_prepare_abort_is_fenced_durable_and_releases_footprint() {
    let root = temp_root("local_backend_namespace_prepare_abort");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let file_system = file_system();
    let current = object(version("version_prepare_abort_1"));
    let mut hidden = current.clone();
    hidden.version = version("version_prepare_abort_2");
    hidden.changed_at_unix_nanos = 2;
    let prepare = namespace_prepare("operation-abort", &shard_id.0, &current, hidden.clone());
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("prepare namespace transaction");

    let fence_error = backend
        .abort_prepared_namespace_transaction(
            &prepare.operation_id,
            "blake3:different-operation",
            prepare.owner_epoch,
        )
        .expect_err("digest reuse must be fenced");
    assert!(fence_error.to_string().contains("digest/epoch fence"));

    let abort_entry = backend
        .abort_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("abort namespace transaction");
    assert!(matches!(
        abort_entry.mutation,
        DfsMetadataMutation::PutNamespacePrepare {
            status: DfsNamespacePrepareStatus::Aborted,
            ..
        }
    ));
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("visible object"),
        Some(current.clone()),
        "aborting a hidden prepare must not publish its transaction"
    );
    assert!(
        backend
            .tables
            .namespace_prepare_footprint_indexes_match_history()
    );

    let commit_error = backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect_err("an aborted prepare cannot commit");
    assert!(commit_error.to_string().contains("is aborted"));
    backend
        .abort_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("repeated abort is idempotent");

    let mut replacement = prepare.clone();
    replacement.operation_id = "operation-after-abort".to_string();
    replacement.transaction.transaction_id = "transaction:after-abort".to_string();
    replacement.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&replacement)
            .expect("replacement digest");
    backend
        .prepare_namespace_transaction(&replacement)
        .expect("aborted footprint is reusable");

    drop(backend);
    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen backend");
    assert_eq!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("aborted prepare")
            .map(|record| record.status),
        Some(DfsNamespacePrepareStatus::Aborted)
    );
    assert!(
        reopened
            .tables
            .namespace_prepare_footprint_indexes_match_history()
    );
    cleanup(&root);
}

#[test]
fn local_backend_rebinds_exact_prepared_namespace_with_epoch_fences() {
    let root = temp_root("local_backend_rebinds_prepared_namespace");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let current = object(version("version_rebind_prepare_1"));
    let mut committed = current.clone();
    committed.version = version("version_rebind_prepare_2");
    committed.changed_at_unix_nanos = 2;
    let prepare = namespace_prepare("operation-rebind", &shard_id.0, &current, committed.clone());
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("initial prepare");

    let mut rebound = prepare.clone();
    rebound.owner_epoch = prepare.owner_epoch + 1;
    rebound.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&rebound)
            .expect("rebound digest");

    let stale_fence = backend
        .rebind_prepared_namespace_transaction(
            &prepare.operation_id,
            "blake3:stale",
            prepare.owner_epoch,
            &rebound,
        )
        .expect_err("stale digest fence must reject rebind");
    assert!(stale_fence.to_string().contains("fence mismatch"));
    assert_eq!(
        backend
            .get_namespace_prepare(&prepare.operation_id)
            .expect("prepare"),
        Some(prepare.clone())
    );

    let mut changed_payload = rebound.clone();
    changed_payload.transaction.objects[0].mode ^= 0o100;
    changed_payload.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&changed_payload)
            .expect("changed digest");
    let payload_error = backend
        .rebind_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
            &changed_payload,
        )
        .expect_err("rebind must preserve the exact namespace payload");
    assert!(payload_error.to_string().contains("operation payload"));

    backend
        .rebind_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
            &rebound,
        )
        .expect("rebind prepared namespace");
    backend
        .rebind_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
            &rebound,
        )
        .expect("exact lost-response rebind retry is idempotent");

    let mut concurrent = rebound.clone();
    concurrent.owner_epoch += 1;
    concurrent.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&concurrent)
            .expect("concurrent digest");
    let concurrent_error = backend
        .rebind_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
            &concurrent,
        )
        .expect_err("the consumed old fence must reject a concurrent rebind");
    assert!(concurrent_error.to_string().contains("fence mismatch"));
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("hidden object"),
        Some(current.clone()),
        "rebinding must not make the transaction visible"
    );
    backend
        .commit_prepared_namespace_transaction(
            &rebound.operation_id,
            &rebound.operation_digest,
            rebound.owner_epoch,
        )
        .expect("commit rebound prepare");
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("visible object"),
        Some(committed)
    );
    cleanup(&root);
}

#[test]
fn local_backend_namespace_prepare_rejects_stale_overlap_and_commit_races() {
    let root = temp_root("local_backend_namespace_prepare_stale_overlap_commit_race");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let current = object(version("version_prepare_race_1"));
    let mut committed = current.clone();
    committed.version = version("version_prepare_race_2");
    committed.changed_at_unix_nanos = 2;
    let prepare = namespace_prepare("operation-race", &shard_id.0, &current, committed);
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("first prepare");

    let mut stale = prepare.clone();
    stale.operation_id = "operation-stale".to_string();
    stale.operation_digest = "blake3:operation-stale".to_string();
    stale.transaction.transaction_id = "transaction:operation-stale".to_string();
    stale.expected_object_versions[0].expected_version = Some(version("version_prepare_stale"));
    let stale_error = backend
        .prepare_namespace_transaction(&stale)
        .expect_err("stale object version must reject prepare");
    assert!(matches!(
        stale_error,
        MetadataBackendError::Conflict {
            ref object_id,
            ref expected,
            current: ref current_version,
        } if object_id == &current.object_id
            && expected == &version("version_prepare_stale")
            && current_version == &version("version_prepare_race_1")
    ));

    let mut overlap = prepare.clone();
    overlap.operation_id = "operation-overlap".to_string();
    overlap.operation_digest = "blake3:operation-overlap".to_string();
    overlap.transaction.transaction_id = "transaction:operation-overlap".to_string();
    reset_namespace_prepare_history_scan_calls();
    let overlap_error = backend
        .prepare_namespace_transaction(&overlap)
        .expect_err("active footprint overlap must reject prepare");
    assert_eq!(
        namespace_prepare_history_scan_calls(),
        0,
        "ordinary prepare admission must not scan retained prepare history"
    );
    assert!(
        overlap_error
            .to_string()
            .contains("overlaps active prepare")
    );

    let mut raced = current.clone();
    raced.version = version("version_prepare_race_3");
    raced.changed_at_unix_nanos = 3;
    backend
        .put_object(&raced)
        .expect("concurrent visible mutation");
    let commit_error = backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect_err("commit must revalidate the prepared expectations");
    assert!(matches!(
        commit_error,
        MetadataBackendError::Conflict {
            ref object_id,
            ref expected,
            current: ref current_version,
        } if object_id == &raced.object_id
            && expected == &version("version_prepare_race_1")
            && current_version == &version("version_prepare_race_3")
    ));
    assert_eq!(
        backend.get_object(&raced.object_id).expect("raced object"),
        Some(raced)
    );
    assert_eq!(
        backend
            .get_namespace_prepare(&prepare.operation_id)
            .expect("still prepared")
            .map(|record| record.status),
        Some(DfsNamespacePrepareStatus::Prepared),
        "a failed commit must leave the hidden prepare and visible namespace unchanged"
    );

    cleanup(&root);
}

#[test]
fn local_backend_namespace_prepare_commit_revalidates_transaction_identity() {
    let root = temp_root("local_backend_namespace_prepare_transaction_identity_race");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let current_a = object(version("version_prepare_transaction_a_1"));
    let mut committed_a = current_a.clone();
    committed_a.version = version("version_prepare_transaction_a_2");
    let prepare_a = namespace_prepare(
        "operation-transaction-identity-a",
        &shard_id.0,
        &current_a,
        committed_a,
    );

    let mut current_b = object(version("version_prepare_transaction_b_1"));
    current_b.object_id =
        DfsObjectId::from_legacy_named("obj_prepare_transaction_b").expect("object id");
    current_b.path = "/docs/prepare-transaction-b".to_string();
    let mut committed_b = current_b.clone();
    committed_b.version = version("version_prepare_transaction_b_2");
    let mut prepare_b = namespace_prepare(
        "operation-transaction-identity-b",
        &shard_id.0,
        &current_b,
        committed_b,
    );
    prepare_b.transaction.transaction_id = prepare_a.transaction.transaction_id.clone();
    prepare_b.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare_b)
            .expect("second prepare digest");

    backend.put_object(&current_a).expect("current object a");
    backend.put_object(&current_b).expect("current object b");
    backend
        .prepare_namespace_transaction(&prepare_a)
        .expect("first disjoint prepare");
    backend
        .prepare_namespace_transaction(&prepare_b)
        .expect("second disjoint prepare with unresolved transaction id");
    backend
        .commit_prepared_namespace_transaction(
            &prepare_a.operation_id,
            &prepare_a.operation_digest,
            prepare_a.owner_epoch,
        )
        .expect("first transaction commit");

    let conflict = backend
        .commit_prepared_namespace_transaction(
            &prepare_b.operation_id,
            &prepare_b.operation_digest,
            prepare_b.owner_epoch,
        )
        .expect_err("second commit must revalidate transaction identity");
    assert!(
        conflict.to_string().contains("reuses transaction id"),
        "unexpected transaction identity error: {conflict}"
    );
    assert_eq!(
        backend
            .get_namespace_prepare(&prepare_b.operation_id)
            .expect("second prepare")
            .map(|prepare| prepare.status),
        Some(DfsNamespacePrepareStatus::Prepared)
    );
    assert_eq!(
        backend.get_object(&current_b.object_id).expect("object b"),
        Some(current_b)
    );
    cleanup(&root);
}

#[test]
fn local_backend_namespace_prepare_commit_revalidates_advisory_lock_state() {
    let root = temp_root("local_backend_namespace_prepare_advisory_lock_race");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let current = object(version("version_prepare_lock_1"));
    let mut committed = current.clone();
    committed.version = version("version_prepare_lock_2");
    let mut prepare = namespace_prepare(
        "operation-advisory-lock-race",
        &shard_id.0,
        &current,
        committed,
    );
    let prepared_lock = DfsAdvisoryLockStateRecord::empty(
        current.file_system_id.clone(),
        current.object_id.clone(),
        7,
    );
    prepare
        .transaction
        .advisory_lock_states
        .push(prepared_lock.clone());
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("prepare digest");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("prepare with advisory-lock state");

    let mut raced_lock = prepared_lock;
    raced_lock.authority_epoch += 1;
    backend
        .tables
        .apply(DfsMetadataTableOp::PutAdvisoryLockState { state: raced_lock });
    let conflict = backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect_err("commit must revalidate advisory-lock state");
    assert!(
        matches!(&conflict, MetadataBackendError::AdvisoryLock(_)),
        "unexpected advisory-lock error: {conflict}"
    );
    assert_eq!(
        backend
            .get_namespace_prepare(&prepare.operation_id)
            .expect("prepare after lock race")
            .map(|prepare| prepare.status),
        Some(DfsNamespacePrepareStatus::Prepared)
    );
    cleanup(&root);
}

#[test]
fn namespace_prepare_footprint_index_matches_reference_lifecycle_and_scope() {
    fn assert_overlap_equivalent(
        tables: &DfsMetadataTables,
        candidate: &DfsNamespacePrepareRecord,
        expected_accept: bool,
    ) {
        reset_namespace_prepare_history_scan_calls();
        let footprint = namespace_prepare_footprint(candidate);
        let indexed =
            validate_namespace_prepare_does_not_overlap(tables, candidate, &footprint).is_ok();
        assert_eq!(
            namespace_prepare_history_scan_calls(),
            0,
            "the indexed validator must never call the history-scan oracle"
        );
        let reference =
            validate_namespace_prepare_does_not_overlap_reference(tables, candidate).is_ok();
        assert_eq!(indexed, reference, "indexed/reference decision mismatch");
        assert_eq!(indexed, expected_accept, "unexpected overlap decision");
    }

    let shard_id = DfsMetadataShardId::new("shard-root");
    let current = object(version("version_footprint_active_1"));
    let mut committed = current.clone();
    committed.version = version("version_footprint_active_2");
    let active = namespace_prepare(
        "operation-footprint-active",
        &shard_id.0,
        &current,
        committed,
    );
    let mut tables = DfsMetadataTables::default();
    tables.apply(DfsMetadataTableOp::put_namespace_prepare(active.clone()));
    assert!(tables.namespace_prepare_footprint_indexes_match_history());

    assert_overlap_equivalent(&tables, &active, true);

    let mut same_object = current.clone();
    same_object.path = "/docs/object-conflict-only.md".to_string();
    let mut same_object_target = same_object.clone();
    same_object_target.version = version("version_footprint_object_conflict");
    let object_conflict = namespace_prepare(
        "operation-footprint-object-conflict",
        &shard_id.0,
        &same_object,
        same_object_target,
    );
    assert_overlap_equivalent(&tables, &object_conflict, false);

    let mut same_path = current.clone();
    same_path.object_id =
        DfsObjectId::from_legacy_named("obj_footprint_path_conflict").expect("object id");
    let mut same_path_target = same_path.clone();
    same_path_target.version = version("version_footprint_path_conflict");
    let path_conflict = namespace_prepare(
        "operation-footprint-path-conflict",
        &shard_id.0,
        &same_path,
        same_path_target,
    );
    assert_overlap_equivalent(&tables, &path_conflict, false);

    let mut disjoint = same_path.clone();
    disjoint.path = "/docs/disjoint.md".to_string();
    let mut disjoint_target = disjoint.clone();
    disjoint_target.version = version("version_footprint_disjoint");
    let disjoint_prepare = namespace_prepare(
        "operation-footprint-disjoint",
        &shard_id.0,
        &disjoint,
        disjoint_target,
    );
    assert_overlap_equivalent(&tables, &disjoint_prepare, true);

    let mut other_file_system = current.clone();
    other_file_system.file_system_id =
        DfsFileSystemId::from_legacy_named("sifs-footprint-other").expect("filesystem id");
    let mut other_file_system_target = other_file_system.clone();
    other_file_system_target.version = version("version_footprint_other_fs");
    let cross_file_system = namespace_prepare(
        "operation-footprint-other-fs",
        &shard_id.0,
        &other_file_system,
        other_file_system_target,
    );
    assert_overlap_equivalent(&tables, &cross_file_system, true);
    tables.apply(DfsMetadataTableOp::put_namespace_prepare(cross_file_system));
    assert!(tables.namespace_prepare_footprint_indexes_match_history());

    let mut committed_active = active.clone();
    committed_active.status = DfsNamespacePrepareStatus::Committed;
    tables.apply(DfsMetadataTableOp::put_namespace_prepare(committed_active));
    let mut replacement = active.clone();
    replacement.operation_id = "operation-footprint-replacement".to_string();
    replacement.transaction.transaction_id = "transaction:footprint-replacement".to_string();
    assert_overlap_equivalent(&tables, &replacement, true);
    tables.apply(DfsMetadataTableOp::put_namespace_prepare(
        replacement.clone(),
    ));

    let mut aborted_replacement = replacement.clone();
    aborted_replacement.status = DfsNamespacePrepareStatus::Aborted;
    tables.apply(DfsMetadataTableOp::put_namespace_prepare(
        aborted_replacement,
    ));
    let mut after_abort = replacement;
    after_abort.operation_id = "operation-footprint-after-abort".to_string();
    after_abort.transaction.transaction_id = "transaction:footprint-after-abort".to_string();
    assert_overlap_equivalent(&tables, &after_abort, true);
    assert!(tables.namespace_prepare_footprint_indexes_match_history());

    let before_preapply = tables.clone();
    let mut rollback = DfsMetadataTableRollback::default();
    tables.apply_with_rollback(
        DfsMetadataTableOp::put_namespace_prepare(after_abort.clone()),
        &mut rollback,
    );
    let mut blocked_during_preapply = after_abort;
    blocked_during_preapply.operation_id = "operation-footprint-preapplied-conflict".to_string();
    blocked_during_preapply.transaction.transaction_id =
        "transaction:footprint-preapplied-conflict".to_string();
    assert_overlap_equivalent(&tables, &blocked_during_preapply, false);
    rollback.rollback(&mut tables);
    assert_eq!(tables, before_preapply);
    assert!(tables.namespace_prepare_footprint_indexes_match_history());
}

#[test]
fn sharded_backend_namespace_prepare_routes_one_shard_and_commits_indexes() {
    let root = temp_root("sharded_backend_namespace_prepare_route_commit");
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
    let mut current = object(version("version_sharded_prepare_1"));
    current.path = "/services/prepared.txt".to_string();
    let mut committed = current.clone();
    committed.version = version("version_sharded_prepare_2");
    committed.changed_at_unix_nanos = 2;
    let mut prepare =
        namespace_prepare("operation-sharded", "services", &current, committed.clone());
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    crate::metadata_api::reset_namespace_prepare_normalization_passes();
    backend
        .prepare_namespace_transaction_in_place(&mut prepare)
        .expect("sharded prepare");
    assert_eq!(
        crate::metadata_api::namespace_prepare_normalization_passes(),
        1,
        "the in-place router and local store must consume the caller's one canonicalization"
    );
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("hidden object"),
        Some(current)
    );
    backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("sharded commit");
    assert_eq!(
        backend
            .get_object_by_path(&committed.file_system_id, &committed.path)
            .expect("committed path"),
        Some(committed.clone())
    );
    assert_eq!(
        backend
            .read_object_shard_index(&committed.object_id)
            .expect("object shard index")
            .map(|index| index.shard_id),
        Some(DfsMetadataShardId::new("services"))
    );

    cleanup(&root);
}

#[test]
fn sharded_namespace_prepare_expectations_use_one_exact_table_snapshot() {
    let root = temp_root("sharded_namespace_prepare_expectations_one_snapshot");
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
    backend.put_file_system(&file_system()).expect("filesystem");

    let mut moving = object(version("version_snapshot_moving_1"));
    moving.path = "/services/from.txt".to_string();
    let mut occupant = object(version("version_snapshot_occupant_1"));
    occupant.object_id =
        DfsObjectId::from_legacy_named("obj_namespace_snapshot_occupant").expect("object id");
    occupant.path = "/services/to.txt".to_string();
    backend.put_object(&moving).expect("moving object");
    backend.put_object(&occupant).expect("target occupant");

    let mut moved = moving.clone();
    moved.path = occupant.path.clone();
    moved.version = version("version_snapshot_moving_2");
    let transaction = DfsNamespaceTransaction {
        file_version_pins: None,
        transaction_id: "transaction:one-snapshot".to_string(),
        file_system_id: moving.file_system_id.clone(),
        objects: vec![moved],
        versions: Vec::new(),
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };

    reset_metadata_current_tables_snapshot_calls();
    let (object_expectations, path_expectations, paths) = backend
        .namespace_prepare_expectations(&DfsMetadataShardId::new("services"), &transaction)
        .expect("namespace expectations");
    assert_eq!(
        metadata_current_tables_snapshot_calls(),
        1,
        "the sharded/local fast path must project every expectation from one table snapshot"
    );
    assert_eq!(
        paths,
        std::collections::BTreeSet::from([moving.path.clone(), occupant.path.clone()]),
        "the immutable snapshot must contribute the moving object's current path"
    );
    assert_eq!(object_expectations.len(), 2);
    assert_eq!(path_expectations.len(), 2);
    assert_eq!(
        object_expectations
            .iter()
            .find(|expected| expected.object_id == moving.object_id)
            .and_then(|expected| expected.expected_version.as_ref()),
        Some(&moving.version)
    );
    assert_eq!(
        object_expectations
            .iter()
            .find(|expected| expected.object_id == occupant.object_id)
            .and_then(|expected| expected.expected_version.as_ref()),
        Some(&occupant.version),
        "the target-path occupant version must come from the same snapshot"
    );
    assert_eq!(
        path_expectations
            .iter()
            .find(|expected| expected.path == moving.path)
            .and_then(|expected| expected.expected_object_id.as_ref()),
        Some(&moving.object_id)
    );
    assert_eq!(
        path_expectations
            .iter()
            .find(|expected| expected.path == occupant.path)
            .and_then(|expected| expected.expected_object_id.as_ref()),
        Some(&occupant.object_id)
    );

    cleanup(&root);
}
