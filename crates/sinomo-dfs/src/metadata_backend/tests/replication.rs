//! Replication regression tests.

use super::*;

#[test]
fn local_backend_persists_file_system_object_path_and_journal() {
    let root = temp_root("local_backend_persists_file_system_object_path_and_journal");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    let object = object(version("version_1"));

    let file_system_entry = backend.put_file_system(&file_system).expect("filesystem");
    let object_entry = backend.put_object(&object).expect("object");

    assert_eq!(file_system_entry.sequence, 1);
    assert_eq!(object_entry.sequence, 2);
    assert_eq!(
        file_system_entry.mutation,
        DfsMetadataMutation::PutFileSystem {
            file_system_id: file_system.file_system_id.clone(),
            file_system: Some(file_system.clone()),
        },
        "filesystem journal entries should carry replay-ready record payloads"
    );
    assert_eq!(
        object_entry.mutation,
        DfsMetadataMutation::PutObject {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: object.path.clone(),
            object: Some(object.clone()),
        },
        "object journal entries should carry replay-ready record payloads"
    );
    assert_eq!(
        backend
            .get_file_system(&file_system.file_system_id)
            .expect("get DfsFileSystem"),
        Some(file_system)
    );
    assert_eq!(
        backend.get_object(&object.object_id).expect("get object"),
        Some(object.clone())
    );
    assert_eq!(
        backend
            .get_object_by_path(&object.file_system_id, "docs/readme.md")
            .expect("get by path"),
        Some(object.clone())
    );
    assert_eq!(backend.journal_since(0).expect("journal").len(), 2);
    let durable_prefix = backend.metadata_prefix_proof().expect("prefix proof");
    assert_eq!(durable_prefix.journal_sequence, 2);
    assert!(
        metadata_table_path(&root).is_file(),
        "new metadata writes should materialize through the packed table log"
    );
    assert!(
        !root
            .join("records")
            .join("objects")
            .join(key_segment(&object.object_id))
            .join("object.sobj")
            .exists(),
        "new object writes should not create one file per object metadata record"
    );
    assert!(
        !root
            .join("records")
            .join("paths")
            .join(key_segment(&object.file_system_id))
            .join(format!("{}.sobj", path_hash(&object.path)))
            .exists(),
        "new object writes should not create one file per path index record"
    );

    crate::metadata_records::reset_dfs_metadata_full_snapshot_fingerprint_calls();
    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopen");
    assert_eq!(reopened.journal_since(1).expect("journal").len(), 1);
    assert_eq!(
        reopened.metadata_prefix_proof().expect("reopened proof"),
        durable_prefix,
        "restart must recover the durable chain head without reseeding it"
    );
    assert_eq!(
        crate::metadata_records::dfs_metadata_full_snapshot_fingerprint_calls(),
        0,
        "healthy restart must load the checkpoint instead of hashing the full shard"
    );

    cleanup(&root);
}

#[test]
fn ensure_file_system_does_not_journal_distinct_policy_reordering() {
    let root = temp_root("ensure_file_system_policy_reordering_noop");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let mut file_system = file_system();
    file_system.subtree_policies = vec![
        crate::DfsSubtreePolicy::legacy("/users", DfsObjectPolicy::ReplicatedEventual),
        crate::DfsSubtreePolicy::legacy("/services", DfsObjectPolicy::ReplicatedEventual),
    ];
    let first = backend
        .ensure_file_system(&file_system)
        .expect("initial filesystem")
        .expect("initial journal entry");
    assert_eq!(first.sequence, 1);
    let first_proof = backend.metadata_prefix_proof().expect("initial proof");

    let mut reordered = file_system.clone();
    reordered.subtree_policies.reverse();
    assert!(
        backend
            .ensure_file_system(&reordered)
            .expect("semantically identical filesystem")
            .is_none(),
        "discovery order alone must not author a PutFileSystem mutation"
    );
    assert_eq!(backend.journal_last_sequence(), 1);
    assert_eq!(
        backend.metadata_prefix_proof().expect("unchanged proof"),
        first_proof
    );
    assert_eq!(
        backend
            .get_file_system(&file_system.file_system_id)
            .expect("stored filesystem"),
        Some(file_system.clone()),
        "a semantic no-op must not rewrite the durable representation"
    );

    reordered.subtree_policies[0].policy = DfsObjectPolicy::ReplicatedConfirmed;
    let changed = backend
        .ensure_file_system(&reordered)
        .expect("changed filesystem")
        .expect("changed policy journal entry");
    assert_eq!(changed.sequence, 2);
    assert!(matches!(
        changed.mutation,
        DfsMetadataMutation::PutFileSystem { .. }
    ));

    cleanup(&root);
}

#[test]
fn replicated_contiguous_journal_entries_converge_on_exact_prefix_proof() {
    let root = temp_root("replicated_contiguous_prefix_proof");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut source =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source")).expect("source");
    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("replica")).expect("replica");

    source
        .put_file_system(&file_system())
        .expect("source filesystem");
    let first = source
        .journal_batch_since_limited(0, 16, usize::MAX)
        .expect("first source batch")
        .expect("first batch");
    replica
        .apply_journal_batch(&first)
        .expect("apply first batch");
    let source_first_proof = source.metadata_prefix_proof().expect("source first proof");
    assert_eq!(
        replica.metadata_prefix_proof().expect("replica proof"),
        source_first_proof
    );

    source
        .put_object(&object(version("version_prefix_2")))
        .expect("source object");
    let second = source
        .journal_batch_since_limited(first.next_after_sequence, 16, usize::MAX)
        .expect("second source batch")
        .expect("second batch");
    replica
        .apply_journal_batch(&second)
        .expect("apply second batch");
    assert_eq!(
        replica.metadata_prefix_proof().expect("replica proof"),
        source.metadata_prefix_proof().expect("source proof")
    );

    let mut divergent =
        LocalSobjMetadataBackend::new(shard_id, root.join("divergent")).expect("divergent");
    let mut different_file_system = file_system();
    different_file_system.display_name = "divergent".to_string();
    divergent
        .put_file_system(&different_file_system)
        .expect("divergent filesystem");
    assert_ne!(
        divergent
            .metadata_prefix_proof()
            .expect("divergent proof")
            .digest,
        source_first_proof.digest,
        "different journal payloads must not share a proof"
    );

    cleanup(&root);
}

#[test]
fn compact_request_owned_prepare_survives_replay_and_closes_exact_retry_with_receipt() {
    let root = temp_root("local_backend_compact_request_prepare_replay");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let current = object(version("version_compact_request_prepare_1"));
    let mut committed = current.clone();
    committed.version = version("version_compact_request_prepare_2");
    committed.mode = 0o600;
    let request_operation_id = "req-compact-request-prepare";
    let mut prepare = namespace_prepare(
        &format!("namespace-request:{request_operation_id}:transaction-compact-request-prepare"),
        &shard_id.0,
        &current,
        committed.clone(),
    );
    let completed_request = DfsNamespaceRequestRecord {
        file_system_id: current.file_system_id.clone(),
        requester_node_id: node("node_compact_request_prepare_requester"),
        request_id: "compact-request-prepare".to_string(),
        operation_id: request_operation_id.to_string(),
        authority_shard_id: shard_id.clone(),
        operation: DfsNamespaceOperation::CommitLocalEventualBatch {
            transaction: prepare.transaction.clone(),
            writes: Vec::new(),
        },
        command_digest: "blake3:compact-request-prepare".to_string(),
        status: DfsNamespaceRequestStatus::Completed,
        result: Some(DfsNamespaceResult::Applied),
        posix_coordination_result: None,
        recovery_attempts: 0,
        last_recovery_error: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::from_unix_timestamp(1).expect("completion timestamp"),
    };
    let completion = DfsNamespaceRequestCompletion::from_completed_request(&completed_request)
        .expect("compact completion");
    let expected_receipt = DfsNamespaceRequestReceipt::from_completion(&completion);
    prepare.completed_request_completion = Some(completion.clone());
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("compact request-owned prepare digest");

    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    let prepared_entry = backend
        .prepare_namespace_transaction(&prepare)
        .expect("compact request-owned prepare");

    assert!(
        backend
            .get_namespace_request(&completion.requester_node_id, &completion.request_id,)
            .expect("full request lookup")
            .is_none(),
        "compact fused admission must not materialize the repeated operation body"
    );
    assert_eq!(
        backend
            .get_prepared_namespace_prepare_for_request(
                &completion.requester_node_id,
                &completion.request_id,
            )
            .expect("prepared owner lookup"),
        Some(prepare.clone())
    );

    force_table_replay_from_journal(&backend, &root, 0);
    drop(backend);
    let mut reopened =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopened backend");
    assert!(
        reopened
            .get_namespace_request(&completion.requester_node_id, &completion.request_id,)
            .expect("reopened full request lookup")
            .is_none()
    );
    assert_eq!(
        reopened
            .get_prepared_namespace_prepare_for_request(
                &completion.requester_node_id,
                &completion.request_id,
            )
            .expect("reopened prepared owner lookup"),
        Some(prepare.clone())
    );

    let commit_entry = reopened
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("commit compact request-owned prepare");
    assert_eq!(
        commit_entry.mutation,
        DfsMetadataMutation::commit_namespace_prepare(&prepare)
    );
    let prepared_bytes =
        serde_json::to_vec(&prepared_entry.mutation).expect("encode prepared mutation");
    let commit_bytes =
        serde_json::to_vec(&commit_entry.mutation).expect("encode compact commit mutation");
    assert!(
        commit_bytes.len().saturating_mul(2) < prepared_bytes.len(),
        "compact journal commit should not repeat the prepared body: prepared={} commit={}",
        prepared_bytes.len(),
        commit_bytes.len()
    );
    assert_eq!(
        reopened
            .get_namespace_request_receipt(&completion.requester_node_id, &completion.request_id,)
            .expect("compact terminal receipt"),
        Some(expected_receipt.clone())
    );
    assert_eq!(
        reopened
            .get_prepared_namespace_prepare_for_request(
                &completion.requester_node_id,
                &completion.request_id,
            )
            .expect("committed owner lookup"),
        None
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
            .expect("committed full prepare")
            .is_none()
    );
    assert!(
        reopened
            .get_namespace_transaction(&prepare.transaction.transaction_id)
            .expect("committed full transaction")
            .is_none()
    );
    let prepare_receipt = reopened
        .tables
        .namespace_prepare_receipts
        .get(&prepare.operation_id)
        .cloned()
        .expect("compact prepare receipt");
    let transaction_receipt = reopened
        .tables
        .namespace_transaction_receipts
        .get(&prepare.transaction.transaction_id)
        .cloned()
        .expect("compact transaction receipt");

    force_table_replay_from_journal(&reopened, &root, 0);
    drop(reopened);
    let mut recovered =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("recovered backend");
    assert_eq!(
        recovered
            .get_namespace_request_receipt(&completion.requester_node_id, &completion.request_id,)
            .expect("replayed compact terminal receipt"),
        Some(expected_receipt)
    );
    assert_eq!(
        recovered
            .tables
            .namespace_prepare_receipts
            .get(&prepare.operation_id),
        Some(&prepare_receipt)
    );
    assert_eq!(
        recovered
            .tables
            .namespace_transaction_receipts
            .get(&prepare.transaction.transaction_id),
        Some(&transaction_receipt)
    );
    recovered
        .put_namespace_request_on_shard(&shard_id, &completed_request)
        .expect("exact retry replays compact receipt");
    assert!(
        recovered
            .get_namespace_request(&completion.requester_node_id, &completion.request_id,)
            .expect("request remains compact after exact retry")
            .is_none()
    );
    let mut conflicting_retry = completed_request;
    conflicting_retry.command_digest = "blake3:different-compact-request".to_string();
    let error = recovered
        .put_namespace_request_on_shard(&shard_id, &conflicting_retry)
        .expect_err("request-id reuse with a different digest must fail");
    assert!(
        error.to_string().contains("reused after compaction"),
        "unexpected exact-retry rejection: {error}"
    );
    cleanup(&root);
}

#[test]
fn compact_namespace_commit_incremental_replica_replay_is_exact() {
    let root = temp_root("local_backend_compact_commit_incremental_replica");
    let source_root = root.join("source");
    let replica_root = root.join("replica");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let current = object(version("version_compact_commit_replica_1"));
    let mut committed = current.clone();
    committed.version = version("version_compact_commit_replica_2");
    committed.mode = 0o600;
    let prepare = namespace_prepare(
        "operation-compact-commit-replica",
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
    let commit_entry = source
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("compact commit");
    assert_eq!(
        commit_entry.mutation,
        DfsMetadataMutation::commit_namespace_prepare(&prepare)
    );

    let entries = source.journal_since(0).expect("source journal");
    let mut corrupt = LocalSobjMetadataBackend::new(
        shard_id.clone(),
        root.join("corrupt-affected-object-replica"),
    )
    .expect("corrupt replica");
    for entry in &entries[..entries.len().saturating_sub(1)] {
        corrupt
            .apply_journal_entry(entry)
            .expect("replay dependency prefix");
    }
    let mut corrupt_commit = entries.last().expect("commit journal entry").clone();
    let DfsMetadataMutation::CommitNamespacePrepare { object_ids, .. } =
        &mut corrupt_commit.mutation
    else {
        panic!("last entry must be a compact namespace commit");
    };
    object_ids.clear();
    let error = corrupt
        .apply_journal_entry(&corrupt_commit)
        .expect_err("affected-object fence mismatch must fail replay");
    assert!(
        error.to_string().contains("affected-object fence mismatch"),
        "unexpected corrupt compact commit error: {error}"
    );

    let config = shard_config(&shard_id.0, DfsMetadataShardAssignment::Root);
    let mut replica = LocalShardedMetadataBackend::new(&replica_root, vec![config.clone()])
        .expect("sharded replica");
    crate::metadata_tables::reset_namespace_commit_materialization_counters();
    replica
        .apply_journal_batch(&DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            next_after_sequence: entries.last().expect("source journal entry").sequence,
            entries,
        })
        .expect("incremental sharded replica replay");
    assert_eq!(
        crate::metadata_tables::namespace_commit_materialization_counters(),
        (0, 1),
        "a trailing compact replica commit must materialize only after its table marker is durable"
    );
    assert_eq!(
        replica
            .get_object(&current.object_id)
            .expect("replica object"),
        Some(committed.clone())
    );
    assert!(
        replica
            .get_namespace_prepare(&prepare.operation_id)
            .expect("replica prepare")
            .is_none(),
        "replica commit must compact the full prepare body"
    );
    assert!(
        replica
            .get_namespace_transaction(&prepare.transaction.transaction_id)
            .expect("replica transaction")
            .is_none(),
        "replica commit must compact the full transaction body"
    );
    let commit_retry = replica
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("replica exact commit retry from compact receipt");
    assert!(matches!(
        commit_retry.mutation,
        DfsMetadataMutation::PutNamespacePrepareReceipt { .. }
    ));
    let transaction_retry = replica
        .shard_backend_mut(&shard_id)
        .expect("replica shard backend")
        .apply_namespace_transaction(&prepare.transaction)
        .expect("replica exact transaction retry from compact receipt");
    assert!(matches!(
        transaction_retry.mutation,
        DfsMetadataMutation::PutNamespaceTransactionReceipt { .. }
    ));
    assert_eq!(
        replica
            .read_object_shard_index(&current.object_id)
            .expect("replica object shard index")
            .map(|index| (index.shard_id, index.path)),
        Some((shard_id.clone(), committed.path.clone())),
        "compact commit affected IDs must reconcile sharded placement indexes"
    );

    drop(replica);
    let replica =
        LocalShardedMetadataBackend::new(&replica_root, vec![config]).expect("reopened replica");
    assert_eq!(
        replica
            .get_object(&current.object_id)
            .expect("reopened replica object"),
        Some(committed)
    );
    cleanup(&root);
}

#[test]
fn incremental_replica_replay_applies_commit_before_trailing_prepare() {
    let root = temp_root("replica_commit_then_trailing_prepare");
    let source_root = root.join("source");
    let replica_root = root.join("replica");
    let shard_id = DfsMetadataShardId::new("shard-root");

    let current_a = object(version("version_commit_then_prepare_a_1"));
    let mut committed_a = current_a.clone();
    committed_a.version = version("version_commit_then_prepare_a_2");
    committed_a.mode = 0o600;
    let prepare_a = namespace_prepare(
        "operation-commit-then-prepare-a",
        &shard_id.0,
        &current_a,
        committed_a.clone(),
    );

    let mut current_b = object(version("version_commit_then_prepare_b_1"));
    current_b.object_id =
        DfsObjectId::from_legacy_named("obj_commit_then_prepare_b").expect("object id");
    current_b.path = "services/commit-then-prepare-b".to_string();
    let mut prepared_b = current_b.clone();
    prepared_b.version = version("version_commit_then_prepare_b_2");
    prepared_b.mode = 0o640;
    let prepare_b = namespace_prepare(
        "operation-commit-then-prepare-b",
        &shard_id.0,
        &current_b,
        prepared_b,
    );

    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), &source_root).expect("source");
    source.put_file_system(&file_system()).expect("filesystem");
    source.put_object(&current_a).expect("current object a");
    source.put_object(&current_b).expect("current object b");
    source
        .prepare_namespace_transaction(&prepare_a)
        .expect("prepare transaction a");
    source
        .commit_prepared_namespace_transaction(
            &prepare_a.operation_id,
            &prepare_a.operation_digest,
            prepare_a.owner_epoch,
        )
        .expect("commit transaction a");
    source
        .prepare_namespace_transaction(&prepare_b)
        .expect("prepare transaction b");

    let mut entries = source.journal_since(0).expect("source journal");
    let tail = entries.split_off(entries.len().saturating_sub(2));
    assert!(matches!(
        tail[0].mutation,
        DfsMetadataMutation::CommitNamespacePrepare { .. }
    ));
    assert!(matches!(
        tail[1].mutation,
        DfsMetadataMutation::PutNamespacePrepare { .. }
    ));
    let prefix_sequence = entries.last().expect("replica prefix").sequence;

    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root).expect("replica");
    replica
        .apply_journal_batch(&DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            next_after_sequence: prefix_sequence,
            entries,
        })
        .expect("replicate dependency prefix");
    let tail_sequence = tail.last().expect("replay tail").sequence;
    replica
        .apply_journal_batch(&DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            next_after_sequence: tail_sequence,
            entries: tail,
        })
        .expect("replay commit followed by trailing prepare");
    assert_eq!(
        replica.get_object(&current_a.object_id).expect("object a"),
        Some(committed_a.clone())
    );
    assert_eq!(
        replica
            .get_namespace_prepare(&prepare_b.operation_id)
            .expect("prepare b")
            .map(|record| record.status),
        Some(DfsNamespacePrepareStatus::Prepared)
    );
    assert!(
        !replica
            .tables
            .namespace_prepares
            .contains_key(&prepare_a.operation_id)
    );
    assert!(
        !replica
            .tables
            .namespace_transactions
            .contains_key(&prepare_a.transaction.transaction_id)
    );
    assert!(
        replica
            .tables
            .namespace_prepare_receipts
            .contains_key(&prepare_a.operation_id)
    );
    assert!(
        replica
            .tables
            .namespace_transaction_receipts
            .contains_key(&prepare_a.transaction.transaction_id)
    );

    drop(replica);
    let replica = LocalSobjMetadataBackend::new(shard_id, &replica_root).expect("reopened replica");
    assert_eq!(
        replica
            .get_object(&current_a.object_id)
            .expect("reopened object a"),
        Some(committed_a)
    );
    assert_eq!(
        replica
            .get_namespace_prepare(&prepare_b.operation_id)
            .expect("reopened prepare b")
            .map(|record| record.status),
        Some(DfsNamespacePrepareStatus::Prepared)
    );
    assert!(
        !replica
            .tables
            .namespace_prepares
            .contains_key(&prepare_a.operation_id)
    );
    assert!(
        replica
            .tables
            .namespace_prepare_receipts
            .contains_key(&prepare_a.operation_id)
    );
    cleanup(&root);
}

#[test]
fn incremental_replica_replay_applies_commit_before_trailing_commit() {
    let root = temp_root("replica_commit_then_trailing_commit");
    let source_root = root.join("source");
    let replica_root = root.join("replica");
    let shard_id = DfsMetadataShardId::new("shard-root");

    let current_a = object(version("version_two_commits_a_1"));
    let mut committed_a = current_a.clone();
    committed_a.version = version("version_two_commits_a_2");
    committed_a.mode = 0o600;
    let prepare_a = namespace_prepare(
        "operation-two-commits-a",
        &shard_id.0,
        &current_a,
        committed_a.clone(),
    );

    let mut current_b = object(version("version_two_commits_b_1"));
    current_b.object_id = DfsObjectId::from_legacy_named("obj_two_commits_b").expect("object id");
    current_b.path = "services/two-commits-b".to_string();
    let mut committed_b = current_b.clone();
    committed_b.version = version("version_two_commits_b_2");
    committed_b.mode = 0o640;
    let prepare_b = namespace_prepare(
        "operation-two-commits-b",
        &shard_id.0,
        &current_b,
        committed_b.clone(),
    );

    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), &source_root).expect("source");
    source.put_file_system(&file_system()).expect("filesystem");
    source.put_object(&current_a).expect("current object a");
    source.put_object(&current_b).expect("current object b");
    source
        .prepare_namespace_transaction(&prepare_a)
        .expect("prepare transaction a");
    source
        .prepare_namespace_transaction(&prepare_b)
        .expect("prepare transaction b");
    source
        .commit_prepared_namespace_transaction(
            &prepare_a.operation_id,
            &prepare_a.operation_digest,
            prepare_a.owner_epoch,
        )
        .expect("commit transaction a");
    source
        .commit_prepared_namespace_transaction(
            &prepare_b.operation_id,
            &prepare_b.operation_digest,
            prepare_b.owner_epoch,
        )
        .expect("commit transaction b");

    let mut entries = source.journal_since(0).expect("source journal");
    let tail = entries.split_off(entries.len().saturating_sub(2));
    assert!(tail.iter().all(|entry| matches!(
        entry.mutation,
        DfsMetadataMutation::CommitNamespacePrepare { .. }
    )));
    let prefix_sequence = entries.last().expect("replica prefix").sequence;

    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root).expect("replica");
    replica
        .apply_journal_batch(&DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            next_after_sequence: prefix_sequence,
            entries,
        })
        .expect("replicate dependency prefix");
    let tail_sequence = tail.last().expect("replay tail").sequence;
    replica
        .apply_journal_batch(&DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            next_after_sequence: tail_sequence,
            entries: tail,
        })
        .expect("replay two compact commits");
    assert_eq!(
        replica.get_object(&current_a.object_id).expect("object a"),
        Some(committed_a.clone())
    );
    assert_eq!(
        replica.get_object(&current_b.object_id).expect("object b"),
        Some(committed_b.clone())
    );

    drop(replica);
    let replica = LocalSobjMetadataBackend::new(shard_id, &replica_root).expect("reopened replica");
    assert_eq!(
        replica
            .get_object(&current_a.object_id)
            .expect("reopened object a"),
        Some(committed_a)
    );
    assert_eq!(
        replica
            .get_object(&current_b.object_id)
            .expect("reopened object b"),
        Some(committed_b)
    );
    cleanup(&root);
}

#[test]
fn large_prepare_only_replica_replay_materializes_after_table_append_without_rollback() {
    const OBJECT_COUNT: usize = 1024;

    let root = temp_root("large_prepare_only_post_append_replica_replay");
    let source_root = root.join("source");
    let replica_root = root.join("replica");
    let shard_id = DfsMetadataShardId::new("shard-root");

    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), &source_root).expect("source");
    source.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV2);
    source.put_file_system(&file_system()).expect("filesystem");
    let initial_entries = source.journal_since(0).expect("initial source journal");
    let initial_sequence = initial_entries
        .last()
        .expect("initial journal head")
        .sequence;

    let mut prepare = DfsNamespacePrepareRecord {
        operation_id: "operation-large-prepare-only-replay".to_string(),
        file_system_id: file_system_id(),
        shard_id: shard_id.clone(),
        owner_epoch: 7,
        operation_digest: String::new(),
        expected_object_versions: Vec::with_capacity(OBJECT_COUNT),
        expected_path_occupants: Vec::with_capacity(OBJECT_COUNT),
        content_receipts: Vec::new(),
        transaction: DfsNamespaceTransaction {
            file_version_pins: None,
            transaction_id: "transaction:large-prepare-only-replay".to_string(),
            file_system_id: file_system_id(),
            objects: Vec::with_capacity(OBJECT_COUNT),
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        },
        completed_request: None,
        completed_request_completion: None,
        status: DfsNamespacePrepareStatus::Prepared,
    };
    for index in 0..OBJECT_COUNT {
        let mut prepared_object =
            object(version(&format!("version_large_prepare_only_{index:04}")));
        prepared_object.object_id =
            DfsObjectId::from_legacy_named(format!("obj_large_prepare_only_{index:04}"))
                .expect("prepared object id");
        prepared_object.path = format!("/services/large-prepare-only/{index:04}");
        prepare
            .expected_object_versions
            .push(DfsNamespaceObjectVersionExpectation {
                object_id: prepared_object.object_id.clone(),
                expected_version: None,
            });
        prepare
            .expected_path_occupants
            .push(DfsNamespacePathOccupantExpectation {
                path: prepared_object.path.clone(),
                expected_object_id: None,
            });
        prepare.transaction.objects.push(prepared_object);
    }
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("large prepare digest");
    crate::metadata_tables::reset_namespace_prepare_footprint_constructions();
    crate::metadata_tables::reset_namespace_prepare_materialization_counters();
    source
        .prepare_namespace_transaction(&prepare)
        .expect("source large prepare");
    assert_eq!(
        crate::metadata_tables::namespace_prepare_footprint_constructions(),
        1,
        "foreground admission, overlap validation, and table indexing must reuse one large-prepare footprint"
    );
    assert_eq!(
        crate::metadata_tables::namespace_prepare_materialization_counters(),
        (0, 1),
        "foreground prepare must run one post-append table-index materialization"
    );
    let prepare_entries = source
        .journal_since(initial_sequence)
        .expect("large prepare source journal");
    assert_eq!(prepare_entries.len(), 1);
    assert!(matches!(
        prepare_entries[0].mutation,
        DfsMetadataMutation::PutNamespacePrepare {
            prepare: None,
            packed_prepare_v2: Some(_),
            status: DfsNamespacePrepareStatus::Prepared,
            ..
        }
    ));

    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root).expect("replica");
    replica
        .apply_journal_batch(&DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            next_after_sequence: initial_sequence,
            entries: initial_entries,
        })
        .expect("replicate initial filesystem");
    replica
        .persist_current_metadata_checkpoint_unlocked()
        .expect("checkpoint initial replica state");

    crate::metadata_tables::reset_namespace_prepare_materialization_counters();
    let prepare_sequence = prepare_entries[0].sequence;
    replica
        .apply_journal_batch(&DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            next_after_sequence: prepare_sequence,
            entries: prepare_entries,
        })
        .expect("replicate large prepare only batch");
    assert_eq!(
        crate::metadata_tables::namespace_prepare_materialization_counters(),
        (0, 1),
        "the trailing full prepare must not build an O(objects) rollback image"
    );
    let installed = replica
        .tables
        .namespace_prepares
        .get(&prepare.operation_id)
        .expect("installed hidden prepare");
    assert_eq!(installed.status, DfsNamespacePrepareStatus::Prepared);
    assert_eq!(installed.transaction.objects.len(), OBJECT_COUNT);
    assert!(
        !replica
            .tables
            .objects
            .contains_key(&prepare.transaction.objects[0].object_id),
        "prepared objects must remain hidden"
    );
    assert_eq!(replica.materialized_through_sequence, prepare_sequence);

    drop(replica);
    let replica = LocalSobjMetadataBackend::new(shard_id, &replica_root).expect("reopened replica");
    let reopened = replica
        .tables
        .namespace_prepares
        .get(&prepare.operation_id)
        .expect("reopened hidden prepare");
    assert_eq!(reopened.status, DfsNamespacePrepareStatus::Prepared);
    assert_eq!(reopened.transaction.objects.len(), OBJECT_COUNT);
    assert!(
        !replica
            .tables
            .objects
            .contains_key(&prepare.transaction.objects[0].object_id),
        "table reference replay must preserve hidden prepare semantics"
    );
    cleanup(&root);
}

#[test]
fn packed_replica_prepare_commit_prices_replay_before_checkpointing() {
    let root = temp_root("packed_replica_prepare_commit_replay_cost");
    let source_root = root.join("source");
    let replica_root = root.join("replica");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let current = object(version("version_packed_replica_cost_base"));
    let mut committed = current.clone();
    committed.version = version("version_packed_replica_cost_committed");
    committed.mode = 0o600;

    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), &source_root).expect("source");
    source.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV1);
    source.put_file_system(&file_system()).expect("filesystem");
    source.put_object(&current).expect("current object");
    let initial_entries = source.journal_since(0).expect("initial source journal");
    let initial_sequence = initial_entries
        .last()
        .expect("initial journal head")
        .sequence;

    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root).expect("replica");
    replica
        .apply_journal_batch(&DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            next_after_sequence: initial_sequence,
            entries: initial_entries,
        })
        .expect("replicate initial namespace");
    replica
        .compact_metadata_table_log_if_needed(0)
        .expect("checkpoint initial replica state");
    let baseline_snapshot =
        fs::read(metadata_table_snapshot_path(&replica_root)).expect("initial replica snapshot");
    assert_eq!(replica.table_log_offset, 0);

    let mut prepare = namespace_prepare(
        "operation-packed-replica-cost",
        &shard_id.0,
        &current,
        committed.clone(),
    );
    for index in 0..64 {
        prepare
            .expected_path_occupants
            .push(DfsNamespacePathOccupantExpectation {
                path: format!("/absent/packed-replica-cost/{index:02}"),
                expected_object_id: None,
            });
    }
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("expanded prepare digest");
    source
        .prepare_namespace_transaction(&prepare)
        .expect("packed source prepare");
    source
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("compact source commit");
    let terminal_entries = source
        .journal_since(initial_sequence)
        .expect("terminal source journal");
    assert_eq!(terminal_entries.len(), 2);
    assert!(matches!(
        terminal_entries[0].mutation,
        DfsMetadataMutation::PutNamespacePrepare {
            prepare: None,
            packed_prepare_v1: Some(_),
            ..
        }
    ));

    replica
        .apply_journal_batch(&DfsMetadataJournalBatch {
            shard_id,
            next_after_sequence: terminal_entries
                .last()
                .expect("terminal journal head")
                .sequence,
            entries: terminal_entries,
        })
        .expect("replicate packed prepare and compact commit");
    let expected_replay_cost = metadata_table_apply::namespace_prepare_replay_cost_bytes(&prepare);
    assert_eq!(
        replica.table_log_journal_reference_replay_cost_bytes, expected_replay_cost,
        "replica replay must price the already-decoded prepare reference"
    );
    assert_eq!(
        replica.table_log_unpriced_namespace_prepare_references, 0,
        "a freshly replicated prepare reference must never be marked unpriced"
    );
    assert!(
        replica.table_log_offset < expected_replay_cost,
        "the fixture must distinguish logical replay pressure from physical table-log bytes"
    );
    assert!(expected_replay_cost < DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES);

    metadata_table_apply::wait_for_metadata_table_checkpoint_workers_for_test(&replica_root);
    assert_eq!(
        fs::read(metadata_table_snapshot_path(&replica_root))
            .expect("snapshot after below-threshold replay"),
        baseline_snapshot,
        "a terminal replicated prepare must not checkpoint solely because its reference was unpriced"
    );

    metadata_table_apply::schedule_metadata_table_checkpoint_if_needed(
        &replica_root,
        &replica.tables,
        expected_replay_cost.saturating_sub(1),
        replica.table_log_offset,
        replica.table_log_journal_reference_replay_cost_bytes,
        replica.table_log_unpriced_namespace_prepare_references,
    )
    .expect("schedule accumulated replay-pressure checkpoint");
    metadata_table_apply::wait_for_metadata_table_checkpoint_workers_for_test(&replica_root);
    assert_ne!(
        fs::read(metadata_table_snapshot_path(&replica_root))
            .expect("snapshot after accumulated replay pressure"),
        baseline_snapshot,
        "the ordinary accumulated replay-cost threshold must still checkpoint terminal state"
    );

    cleanup(&root);
}

#[test]
fn request_owned_prepare_replay_accepts_matching_terminal_outcome_bookkeeping() {
    let root = temp_root("local_backend_request_prepare_terminal_replay");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let file_system = file_system();
    let current = object(version("version_request_prepare_replay_1"));
    let mut committed = current.clone();
    committed.version = version("version_request_prepare_replay_2");
    committed.mode = 0o600;
    let pending_at = OffsetDateTime::UNIX_EPOCH;
    let pending_request = DfsNamespaceRequestRecord {
        file_system_id: current.file_system_id.clone(),
        requester_node_id: node("node_request_prepare_replay_requester"),
        request_id: "request-prepare-replay".to_string(),
        operation_id: "req-request-prepare-replay".to_string(),
        authority_shard_id: shard_id.clone(),
        operation: DfsNamespaceOperation::SetMode {
            path: current.path.clone(),
            mode: committed.mode,
        },
        command_digest: "blake3:request-prepare-replay".to_string(),
        status: DfsNamespaceRequestStatus::Pending,
        result: None,
        posix_coordination_result: None,
        recovery_attempts: 0,
        last_recovery_error: None,
        created_at: pending_at,
        updated_at: pending_at,
    };
    let mut prepared_completion = pending_request.clone();
    prepared_completion.status = DfsNamespaceRequestStatus::Completed;
    prepared_completion.result = Some(DfsNamespaceResult::Applied);
    prepared_completion.updated_at =
        OffsetDateTime::from_unix_timestamp(1).expect("completion timestamp");
    let mut prepare = namespace_prepare(
        &format!(
            "namespace-request:{}:transaction-request-prepare-replay",
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

    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .put_namespace_request_on_shard(&shard_id, &pending_request)
        .expect("pending request");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("durable hidden prepare");
    assert_eq!(
        backend
            .get_prepared_namespace_prepare_for_request(
                &pending_request.requester_node_id,
                &pending_request.request_id,
            )
            .expect("request-owned prepare index"),
        Some(prepare.clone())
    );

    // A retry can durably observe the same terminal outcome while updating
    // only recovery diagnostics. Replaying the older prepare must compare
    // the terminal semantic result, not reject harmless bookkeeping drift.
    let mut later_completion = prepared_completion;
    later_completion.recovery_attempts = 2;
    later_completion.updated_at =
        OffsetDateTime::from_unix_timestamp(2).expect("later completion timestamp");
    backend
        .put_namespace_request_on_shard(&shard_id, &later_completion)
        .expect("matching later completion");

    backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("replay matching completed request");
    assert!(
        backend
            .get_prepared_namespace_prepare_for_request(
                &later_completion.requester_node_id,
                &later_completion.request_id,
            )
            .expect("committed request prepare index")
            .is_none(),
        "committing a prepare must remove it from the pending request index"
    );

    assert_eq!(
        backend
            .get_namespace_request(
                &later_completion.requester_node_id,
                &later_completion.request_id,
            )
            .expect("completed request"),
        Some(later_completion),
        "replay must retain newer diagnostics for the same terminal outcome"
    );
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("committed object"),
        Some(committed)
    );
    cleanup(&root);
}

#[test]
fn migrated_intent_records_route_to_the_destination_without_advancing_the_source_journal() {
    let root = temp_root("migrated_intent_records_route_to_destination");
    let root_shard_id = DfsMetadataShardId::new("root");
    let services_shard_id = DfsMetadataShardId::new("services");
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
    let mut source_object = object(version("version_1"));
    source_object.object_id = DfsObjectId::from_legacy_named("obj_migrated_intent_routing")
        .expect("migrated intent routing object id");
    source_object.path = "/intent-source.txt".to_string();
    let pending_intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "intent-migrated-pending".to_string(),
        file_system_id: source_object.file_system_id.clone(),
        object_id: source_object.object_id.clone(),
        base_version: Some(source_object.version.clone()),
        target_version: version("version_2"),
        coordinator_node_id: node("node_owner"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 1,
        desired_replica_node_ids: vec![node("node_owner")],
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: Some("blake3:migrated-pending".to_string()),
        object_metadata: Some(source_object.clone()),
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::now_utc(),
        updated_at: None,
    };
    let mut terminal_intent = pending_intent.clone();
    terminal_intent.intent_id = "intent-migrated-terminal".to_string();
    terminal_intent.target_version = version("version_3");
    terminal_intent.status = DfsWriteIntentStatus::Committed;
    terminal_intent.content_hash = Some("blake3:migrated-terminal".to_string());
    terminal_intent.updated_at = Some(OffsetDateTime::now_utc());
    let terminal_receipt =
        DfsWriteIntentReceipt::from_intent(&terminal_intent, OffsetDateTime::now_utc())
            .expect("terminal intent receipt");

    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&source_object).expect("source object");
    backend
        .put_intent(&pending_intent)
        .expect("source pending intent");
    backend
        .put_intent(&terminal_intent)
        .expect("source terminal intent");
    let source_head = backend
        .shard_backend(&root_shard_id)
        .expect("root shard")
        .journal_last_sequence();

    let mut destination_object = source_object.clone();
    destination_object.path = "/services/intent-source.txt".to_string();
    destination_object.version = version("version_4");

    backend
        .put_migrated_intent_for_object(Some(&destination_object), &pending_intent)
        .expect("migrate pending intent");
    backend
        .put_intent_receipt_for_object(Some(&destination_object), &terminal_receipt)
        .expect("migrate terminal receipt");

    assert_eq!(
        backend
            .shard_backend(&root_shard_id)
            .expect("root shard")
            .journal_last_sequence(),
        source_head,
        "destination adoption must not advance the fenced source journal"
    );
    let destination = backend
        .shard_backend(&services_shard_id)
        .expect("services shard");
    assert_eq!(
        destination
            .get_intent(&pending_intent.object_id, &pending_intent.intent_id)
            .expect("destination pending intent"),
        Some(pending_intent)
    );
    assert_eq!(
        destination
            .get_intent_receipt(&terminal_receipt.object_id, &terminal_receipt.intent_id)
            .expect("destination terminal receipt"),
        Some(terminal_receipt)
    );

    cleanup(&root);
}

#[test]
fn sharded_combined_snapshot_preserves_compact_gc_receipts_and_floors() {
    let root = temp_root("sharded_combined_snapshot_gc_receipts");
    let root_shard = DfsMetadataShardId::new("root");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config(&root_shard.0, DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let object_id =
        DfsObjectId::from_legacy_named("obj_sharded_snapshot_receipts").expect("object id");
    let retained_at = OffsetDateTime::from_unix_timestamp(20).expect("receipt retention timestamp");
    let intent_receipt = DfsWriteIntentReceipt {
        publication: Default::default(),
        intent_id: "intent_sharded_snapshot_42".to_string(),
        file_system_id: file_system_id(),
        object_id: object_id.clone(),
        base_version: Some(version("version_sharded_snapshot_base")),
        target_version: version("version_sharded_snapshot_target"),
        coordinator_node_id: node("node_owner"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 1,
        desired_replica_node_ids: vec![node("node_owner")],
        policy: DfsReplicationPolicy::new(1, 1).expect("replication policy"),
        status: DfsWriteIntentStatus::Committed,
        content_hash: Some("blake3:sharded-snapshot-intent".to_string()),
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        terminal_at: OffsetDateTime::UNIX_EPOCH,
        retained_at,
    };
    let evicted_intent_receipt = DfsWriteIntentReceipt {
        publication: Default::default(),
        intent_id: "intent_sharded_snapshot_41".to_string(),
        ..intent_receipt.clone()
    };
    let job_receipt = DfsNamespaceJobReceipt {
        file_restore: None,
        job_id: "job_sharded_snapshot_receipt".to_string(),
        file_system_id: file_system_id(),
        origin: None,
        kind: DfsNamespaceJobKind::ClearDirectoryRecursive,
        status: DfsNamespaceJobStatus::Completed,
        root_path: "/snapshot-job".to_string(),
        target_path: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        terminal_at: OffsetDateTime::UNIX_EPOCH,
        retained_at,
    };
    let job_floor = DfsNamespaceReceiptOrderKey {
        created_at: OffsetDateTime::UNIX_EPOCH,
        identity: "job_sharded_snapshot_evicted".to_string(),
    };
    let advisory_lock_state =
        DfsAdvisoryLockStateRecord::empty(file_system_id(), intent_receipt.object_id.clone(), 9);
    {
        let shard = backend
            .shard_backend_mut(&root_shard)
            .expect("root shard backend");
        shard.tables.intent_receipts.insert(
            (object_id, intent_receipt.intent_id.clone()),
            intent_receipt.clone(),
        );
        shard.tables.intent_receipt_retention.merge_conservative(
            DfsWriteIntentReceiptRetentionState::eviction_delta(&evicted_intent_receipt),
        );
        shard
            .tables
            .namespace_job_receipts
            .insert(job_receipt.job_id.clone(), job_receipt.clone());
        shard.tables.namespace_receipt_retention.job_floor = Some(job_floor.clone());
        shard.tables.advisory_lock_states.insert(
            advisory_lock_state.object_id.clone(),
            advisory_lock_state.clone(),
        );
    }

    let snapshot = DfsMetadataBackend::export_table_snapshot(&backend).expect("combined snapshot");
    let restored = metadata_tables_from_table_snapshot(snapshot, "combined sharded snapshot")
        .expect("restore combined snapshot tables");
    assert_eq!(
        restored.intent_receipts.get(&(
            intent_receipt.object_id.clone(),
            intent_receipt.intent_id.clone()
        )),
        Some(&intent_receipt)
    );
    assert!(
        restored
            .intent_receipt_retention
            .history_expired(&evicted_intent_receipt.to_intent_record())
    );
    assert_eq!(
        restored.namespace_job_receipts.get(&job_receipt.job_id),
        Some(&job_receipt)
    );
    assert_eq!(
        restored.namespace_receipt_retention.job_floor,
        Some(job_floor)
    );
    assert_eq!(
        restored
            .advisory_lock_states
            .get(&advisory_lock_state.object_id),
        Some(&advisory_lock_state)
    );

    cleanup(&root);
}

#[test]
fn local_backend_does_not_materialize_object_when_journal_append_fails() {
    let root = temp_root("local_backend_journal_failure_does_not_materialize_object");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    let object = object(version("version_1"));
    backend.put_file_system(&file_system).expect("filesystem");
    fs::remove_dir_all(root.join("journal").join("segments")).expect("remove segments dir");
    fs::write(root.join("journal").join("segments"), b"not a directory")
        .expect("block journal segment writes");

    let error = backend
        .put_object(&object)
        .expect_err("object write should fail before materialization when the journal fails");

    assert!(
        matches!(error, MetadataBackendError::Io { .. }),
        "journal append failure should be surfaced as I/O: {error:?}"
    );
    assert_eq!(
        backend
            .get_object(&object.object_id)
            .expect("object lookup"),
        None,
        "object metadata must not become visible without a journal entry"
    );
    assert!(
        !table_path_exists(&backend, &object.file_system_id, &object.path),
        "path index must not become visible without a journal entry"
    );

    fs::remove_file(root.join("journal").join("segments")).ok();
    cleanup(&root);
}

#[test]
fn local_backend_rmdir_transaction_retries_after_journal_append_failure() {
    let root = temp_root("local_backend_rmdir_journal_append_retry");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    let mut parent = object(version("version_1"));
    parent.object_id =
        DfsObjectId::from_legacy_named("obj_rmdir_append_parent").expect("parent object id");
    parent.path = "/docs".to_string();
    parent.kind = DfsObjectKind::Directory;
    parent.content_hash = None;
    parent.content_size_bytes = None;
    let mut child = object(version("version_1"));
    child.object_id =
        DfsObjectId::from_legacy_named("obj_rmdir_append_child").expect("child object id");
    child.path = "/docs/empty".to_string();
    child.kind = DfsObjectKind::Directory;
    child.content_hash = None;
    child.content_size_bytes = None;
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&parent).expect("parent");
    backend.put_object(&child).expect("child");

    let mut tombstone = child.clone();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.version = version("version_2");
    tombstone.changed_at_unix_nanos = child.changed_at_unix_nanos.saturating_add(1);
    let mut touched_parent = parent.clone();
    touched_parent.version = version("version_2");
    touched_parent.modified_at_unix_nanos = parent.modified_at_unix_nanos.saturating_add(1);
    touched_parent.changed_at_unix_nanos = parent.changed_at_unix_nanos.saturating_add(1);
    let transaction = DfsNamespaceTransaction {
        file_version_pins: None,
        transaction_id: "namespace_rmdir_append_retry".to_string(),
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

    fs::remove_dir_all(root.join("journal").join("segments")).expect("remove journal segments");
    fs::write(root.join("journal").join("segments"), b"not a directory")
        .expect("block journal segment append");
    let error = backend
        .apply_namespace_transaction(&transaction)
        .expect_err("journal append failure");
    assert!(matches!(error, MetadataBackendError::Io { .. }));
    assert_eq!(
        backend.get_object(&child.object_id).expect("child lookup"),
        Some(child),
        "a failed journal append must not expose the tombstone"
    );
    assert_eq!(
        backend
            .get_object(&parent.object_id)
            .expect("parent lookup"),
        Some(parent),
        "a failed journal append must not expose the parent touch"
    );

    fs::remove_file(root.join("journal").join("segments")).expect("remove journal blocker");
    fs::create_dir_all(root.join("journal").join("segments")).expect("restore journal segments");
    backend
        .apply_namespace_transaction(&transaction)
        .expect("retry exact rmdir transaction");
    assert_eq!(
        backend.get_object(&tombstone.object_id).expect("tombstone"),
        Some(tombstone)
    );
    assert_eq!(
        backend
            .get_object(&touched_parent.object_id)
            .expect("touched parent"),
        Some(touched_parent)
    );

    cleanup(&root);
}

#[test]
fn local_backend_recovers_durable_journal_tail_before_same_instance_retry() {
    let root = temp_root("local_backend_same_instance_journal_tail_recovery");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend
        .persist_current_metadata_checkpoint_unlocked()
        .expect("checkpoint table head before injected append failure");

    let mut durable_only = object(version("version_1"));
    durable_only.object_id =
        DfsObjectId::from_legacy_named("obj_durable_table_gap").expect("object id");
    durable_only.path = "/docs/durable-table-gap.md".to_string();
    let table_path = metadata_table_path(&root);
    let error = {
        let _lock = backend.begin_metadata_write().expect("metadata write lock");
        let durable_only = normalized_object_metadata(&durable_only);
        let previous = backend
            .get_object(&durable_only.object_id)
            .expect("previous object");
        let entry = backend
            .write_journal_unlocked(DfsMetadataMutation::put_object(&durable_only))
            .expect("durable journal append");
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        backend
            .plan_object_index_table_ops_with_previous(
                &durable_only,
                previous.as_ref(),
                entry.created_at,
                &mut table_ops,
                &mut rollback,
            )
            .expect("preapplied table plan");
        assert!(
            backend.tables.objects.contains_key(&durable_only.object_id),
            "the injected failure cut starts after in-memory preapply"
        );
        fs::remove_file(&table_path).expect("remove empty table log");
        fs::create_dir(&table_path).expect("block table-log append with a directory");
        backend
            .append_preapplied_table_ops(entry.sequence, table_ops, rollback)
            .expect_err("table append fails after the durable journal append")
    };
    assert!(matches!(error, MetadataBackendError::Io { .. }));
    assert!(
        !backend.tables.objects.contains_key(&durable_only.object_id),
        "failed materialization must not expose the durable-only object"
    );
    assert_eq!(
        backend.journal_since(0).expect("durable journal").len(),
        2,
        "the failed operation must remain durable after the filesystem record"
    );

    fs::remove_dir(&table_path).expect("restore table-log path");
    let mut successor = object(version("version_2"));
    successor.object_id = DfsObjectId::from_legacy_named("obj_after_table_gap").expect("object id");
    successor.path = "/docs/after-table-gap.md".to_string();
    reset_metadata_durable_table_reloads();
    backend
        .put_object(&successor)
        .expect("same handle recovers the durable tail before admitting a successor");
    assert_eq!(
        metadata_durable_table_reloads(),
        1,
        "an ambiguous journal-ahead proof must reload the durable table head before replay"
    );

    assert_eq!(
        backend
            .get_object(&durable_only.object_id)
            .expect("recovered object"),
        Some(durable_only.clone())
    );
    assert_eq!(
        backend
            .get_object(&successor.object_id)
            .expect("successor object"),
        Some(successor.clone())
    );
    let journal = backend.journal_since(0).expect("contiguous journal");
    assert_eq!(
        journal
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(backend.materialized_through_sequence, 3);
    assert_eq!(
        backend
            .metadata_prefix_proof()
            .expect("recovered prefix proof")
            .journal_sequence,
        3
    );

    drop(backend);
    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen backend");
    assert_eq!(
        reopened
            .get_object(&durable_only.object_id)
            .expect("reopened recovered object"),
        Some(durable_only)
    );
    assert_eq!(
        reopened
            .get_object(&successor.object_id)
            .expect("reopened successor object"),
        Some(successor)
    );

    cleanup(&root);
}

#[test]
fn local_backend_replays_journal_entries_after_materialization_cursor() {
    let root = temp_root("local_backend_replays_after_materialization_cursor");
    let object = object(version("version_1"));
    let journaled_sequence = {
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        let entry = backend
            .write_journal(DfsMetadataMutation::PutObject {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                path: object.path.clone(),
                object: Some(object.clone()),
            })
            .expect("journal object without materializing");
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object lookup"),
            None
        );
        entry.sequence
    };

    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopen");

    assert_eq!(
        reopened
            .get_object(&object.object_id)
            .expect("object lookup"),
        Some(object.clone()),
        "reopen should replay journaled metadata that was not yet materialized"
    );
    assert_eq!(
        read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
            .expect("materialization cursor")
            .last_sequence,
        journaled_sequence
    );

    cleanup(&root);
}

#[test]
fn local_backend_replays_already_materialized_visible_commits_after_newer_head() {
    fn commit_test_version(
        backend: &mut LocalSobjMetadataBackend,
        previous: &DfsObjectMetadata,
        committed_version: &str,
        content_hash: &str,
        event_id: &str,
    ) -> (DfsObjectMetadata, DfsMetadataJournalEntry) {
        let replica_node_id = node("node_replica");
        let mut committed = previous.clone();
        committed.version = version(committed_version);
        committed.content_hash = Some(content_hash.to_string());
        committed.content_size_bytes = Some(content_hash.len() as u64);
        committed.current_locations = vec![replica_node_id.clone()];
        let visible_version = DfsObjectVersionRecord {
            content_durability: None,
            history_order: 0,
            retained: None,
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
                event_id: EventId::from_legacy_named(event_id).expect("event"),
            },
            replica_node_id,
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_receipt(&receipt).expect("receipt");
        let entry = backend
            .commit_visible_version(
                &previous.version,
                &committed,
                &visible_version,
                std::slice::from_ref(&receipt.replica_node_id),
            )
            .expect("commit visible version");
        (committed, entry)
    }

    let root = temp_root("local_backend_replays_chained_visible_commits_after_newer_head");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let (final_object, final_sequence) = {
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        let initial = object(version("version_1"));
        backend.put_object(&initial).expect("initial object");
        let (committed_v2, commit_v2) = commit_test_version(
            &mut backend,
            &initial,
            "version_2",
            "blake3:version-2",
            "evt_replay_commit_v2",
        );
        let (committed_v3, commit_v3) = commit_test_version(
            &mut backend,
            &committed_v2,
            "version_3",
            "blake3:version-3",
            "evt_replay_commit_v3",
        );

        write_sobj_atomic(
            &metadata_table_snapshot_path(&root),
            &DfsMetadataTableSnapshot {
                format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
                materialized_through_sequence: None,
                tables: Some(Box::new(metadata_tables_snapshot(&backend.tables))),
                ops: Vec::new(),
                semantic_state_fingerprint: None,
                prefix_proof: None,
            },
        )
        .expect("legacy structured snapshot without materialized sequence");
        truncate_metadata_table_log(&root).expect("truncate table log");
        write_sobj_atomic(
            &backend.local_materialization_cursor_path(),
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: commit_v2.sequence.saturating_sub(1),
            },
        )
        .expect("stale materialization cursor before visible commits");
        (committed_v3, commit_v3.sequence)
    };

    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");

    assert_eq!(
        reopened
            .get_object(&final_object.object_id)
            .expect("object lookup"),
        Some(final_object),
        "replay must not reject or roll back an already-materialized older visible commit"
    );
    assert_eq!(
        read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
            .expect("materialization cursor")
            .last_sequence,
        final_sequence
    );
    assert_eq!(
        reopened.materialized_through_sequence, final_sequence,
        "legacy snapshots should learn the durable materialization sequence from replay"
    );
    assert_eq!(
        fs::metadata(metadata_table_path(&root))
            .expect("metadata table")
            .len(),
        0,
        "idempotent replay should not rewrite table batches for already-materialized commits"
    );

    cleanup(&root);
}

#[test]
fn local_backend_recovery_watermark_invalidates_on_journal_and_table_generations() {
    let root = temp_root("local_backend_recovery_watermark_generation_binding");
    let shard_id = DfsMetadataShardId::new("shard-root");
    {
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
    }
    drop(
        LocalSobjMetadataBackend::new(shard_id.clone(), &root)
            .expect("publish clean watermark after seed mutation"),
    );

    {
        let mut writer =
            LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("open clean writer");
        writer
            .put_object(&object(version("version_1")))
            .expect("advance exact journal prefix");
    }
    reset_metadata_recovery_scan_calls();
    drop(
        LocalSobjMetadataBackend::new(shard_id.clone(), &root)
            .expect("journal generation change must recover"),
    );
    assert_eq!(
        metadata_recovery_scan_calls(),
        (1, 1, 1),
        "an advanced journal prefix must invalidate the clean watermark"
    );

    // Replace the active table generation with byte-for-byte identical
    // content. Prefix-only validation would incorrectly accept the old
    // watermark; the physical generation identity must force a rescan.
    let table_path = metadata_table_path(&root);
    let table_bytes = fs::read(&table_path).expect("read materialized table generation");
    crate::metadata_io::write_file_atomic(&table_path, &table_bytes)
        .expect("atomically replace materialized table generation");
    reset_metadata_recovery_scan_calls();
    drop(
        LocalSobjMetadataBackend::new(shard_id.clone(), &root)
            .expect("table generation replacement must recover"),
    );
    assert_eq!(
        metadata_recovery_scan_calls(),
        (1, 1, 1),
        "a physical table replacement must invalidate the clean watermark even when its bytes and journal prefix are unchanged"
    );

    {
        let backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root)
            .expect("open clean writer before checkpoint rotation");
        let _lock = backend.metadata_write_lock().expect("table writer lock");
        assert!(
            crate::metadata_table_store::rotate_metadata_table_log_for_checkpoint(&root, 0)
                .expect("rotate materialized log into checkpoint generation"),
            "the non-empty materialized log should rotate"
        );
    }
    reset_metadata_recovery_scan_calls();
    drop(
        LocalSobjMetadataBackend::new(shard_id.clone(), &root)
            .expect("checkpoint generation rotation must recover"),
    );
    assert_eq!(
        metadata_recovery_scan_calls(),
        (1, 1, 1),
        "checkpoint/active-log rotation must invalidate the clean watermark"
    );

    fs::write(
        root.join("records")
            .join("tables")
            .join(METADATA_RECOVERY_WATERMARK_FILE_NAME),
        b"not-a-valid-sobj",
    )
    .expect("corrupt clean recovery watermark");
    reset_metadata_recovery_scan_calls();
    drop(
        LocalSobjMetadataBackend::new(shard_id, &root).expect("corrupt watermark must be ignored"),
    );
    assert_eq!(
        metadata_recovery_scan_calls(),
        (1, 1, 1),
        "a parse failure must fail closed to the full recovery scans"
    );
    cleanup(&root);
}

#[test]
fn local_backend_compacts_materialized_table_log_to_snapshot() {
    let root = temp_root("local_backend_compacts_materialized_table_log");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    backend.put_file_system(&file_system).expect("filesystem");
    let mut expected_objects = Vec::new();
    for index in 0..8 {
        let mut object = object(version("version_1"));
        object.object_id =
            DfsObjectId::from_legacy_named(format!("obj_compacted_metadata_table_{index}"))
                .expect("object");
        object.path = format!("/docs/compacted-{index}.md");
        backend.put_object(&object).expect("object");
        expected_objects.push(object);
    }
    let table_path = metadata_table_path(&root);
    assert!(
        fs::metadata(&table_path).expect("metadata table").len() > 0,
        "writes should first append to the materialized table log"
    );
    let prefix_before_compaction = backend.metadata_prefix_proof().expect("prefix proof");

    backend
        .compact_metadata_table_log_if_needed(1)
        .expect("compact metadata table");

    assert!(
        metadata_table_snapshot_path(&root).is_file(),
        "compaction should persist a compact materialized table snapshot"
    );
    let snapshot = crate::metadata_table_store::read_metadata_table_state(&root)
        .expect("metadata table snapshot");
    assert_eq!(
        snapshot.snapshot_format_version,
        Some(crate::metadata_snapshot_migration::SNAPSHOT_FORMAT)
    );
    assert_eq!(
        snapshot.prefix_proof,
        Some(prefix_before_compaction.clone())
    );
    assert_eq!(
        fs::metadata(&table_path)
            .expect("truncated metadata table")
            .len(),
        0,
        "compaction should truncate historical table ops after the snapshot is durable"
    );
    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopen compacted backend");
    assert_eq!(
        reopened.metadata_prefix_proof().expect("reopened proof"),
        prefix_before_compaction,
        "compaction must carry the exact journal-chain head"
    );
    for object in expected_objects {
        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object)
        );
    }
    cleanup(&root);
}

#[test]
fn local_backend_reports_corrupt_snapshot_path() {
    let root = temp_root("local_backend_reports_corrupt_snapshot_path");
    let snapshot_path = metadata_table_snapshot_path(&root);
    fs::create_dir_all(snapshot_path.parent().expect("snapshot parent")).expect("snapshot parent");
    fs::write(&snapshot_path, [0x1f, 0x8b, 0x08]).expect("truncated snapshot");

    let error = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect_err("corrupt snapshot should fail to open");
    let message = error.to_string();
    assert!(
        message.contains(snapshot_path.to_string_lossy().as_ref()),
        "error should name the corrupt snapshot path: {message}"
    );
    assert!(
        message.contains("failed to decode"),
        "error should describe the decode failure: {message}"
    );
    cleanup(&root);
}

#[test]
fn local_backend_migrates_legacy_op_snapshot_to_structured_snapshot() {
    let root = temp_root("local_backend_migrates_legacy_op_snapshot");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let object = object(version("version_1"));
    {
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend.put_object(&object).expect("object");
        write_sobj_atomic(
            &metadata_table_snapshot_path(&root),
            &DfsMetadataTableSnapshot {
                format_version: LEGACY_METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
                materialized_through_sequence: None,
                tables: None,
                ops: metadata_table_snapshot_ops(&backend.tables),
                semantic_state_fingerprint: None,
                prefix_proof: None,
            },
        )
        .expect("legacy metadata table snapshot");
        truncate_metadata_table_log(&root).expect("truncate table log");
    }

    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");

    assert_eq!(
        reopened
            .get_object(&object.object_id)
            .expect("object lookup"),
        Some(object)
    );
    crate::metadata_table_apply::wait_for_metadata_table_checkpoint_workers_for_test(&root);
    let snapshot =
        crate::metadata_table_store::read_metadata_table_state(&root).expect("migrated snapshot");
    assert_eq!(
        snapshot.snapshot_format_version,
        Some(crate::metadata_snapshot_migration::SNAPSHOT_FORMAT)
    );
    assert_eq!(
        fs::metadata(metadata_table_path(&root))
            .expect("metadata table")
            .len(),
        0,
        "snapshot migration should compact any table tail into the structured image"
    );

    cleanup(&root);
}

#[test]
fn local_backend_allocates_journal_sequences_across_open_handles() {
    let root = temp_root("local_backend_allocates_journal_sequences_across_open_handles");
    let mut first = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("first backend");
    let mut second = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("second backend");
    let file_system = file_system();
    let mut object = object(version("version_1"));
    object.object_id = DfsObjectId::from_legacy_named("obj_second_handle").expect("object");
    object.path = "/docs/second.md".to_string();

    let first_entry = first.put_file_system(&file_system).expect("filesystem");
    let second_entry = second
        .put_object(&object)
        .expect("object from second handle");

    assert_eq!(first_entry.sequence, 1);
    assert_eq!(
        second_entry.sequence, 2,
        "a second backend opened before the first write must not reuse the first sequence"
    );
    let journal = first.journal_since(0).expect("journal");
    assert_eq!(journal.len(), 2);
    assert!(matches!(
        journal[0].mutation,
        DfsMetadataMutation::PutFileSystem { .. }
    ));
    assert!(matches!(
        journal[1].mutation,
        DfsMetadataMutation::PutObject { .. }
    ));

    cleanup(&root);
}

#[test]
fn local_backend_reloads_snapshot_when_other_handle_compacts_table_log() {
    let root = temp_root("local_backend_reloads_snapshot_after_external_compaction");
    let mut writer = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("writer backend");
    writer.put_file_system(&file_system()).expect("filesystem");
    persist_metadata_table_snapshot(
        &root,
        &writer.tables,
        Some(writer.materialized_through_sequence),
        writer.prefix_proof.as_ref(),
    )
    .expect("snapshot filesystem before table-log shrink");
    truncate_metadata_table_log(&root).expect("truncate table log after snapshot");
    drop(writer);
    let mut writer = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("writer backend after snapshot");
    let mut logged_object = object(version("version_1"));
    logged_object.object_id =
        DfsObjectId::from_legacy_named("obj_before_table_log_shrink").expect("object");
    logged_object.path = "/docs/before-table-log-shrink.md".to_string();
    writer.put_object(&logged_object).expect("logged object");

    let mut stale = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("stale backend");
    let stale_offset = stale.table_log_offset;
    assert!(
        stale_offset > 0,
        "stale handle needs an old table-log cursor"
    );

    for index in 0..8 {
        let mut object = object(version("version_1"));
        object.object_id =
            DfsObjectId::from_legacy_named(format!("obj_pre_compact_{index}")).expect("object");
        object.path = format!("/docs/pre-compact-{index}.md");
        writer.put_object(&object).expect("pre compact object");
    }
    writer
        .compact_metadata_table_log_if_needed(1)
        .expect("force compaction");

    let mut index = 0usize;
    while fs::metadata(metadata_table_path(&root))
        .map(|metadata| metadata.len())
        .unwrap_or(0)
        <= stale_offset
    {
        let mut object = object(version("version_1"));
        object.object_id =
            DfsObjectId::from_legacy_named(format!("obj_post_compact_{index}")).expect("object");
        object.path = format!("/docs/post-compact-{index}.md");
        writer.put_object(&object).expect("post compact object");
        index += 1;
    }

    let mut object = object(version("version_1"));
    object.object_id =
        DfsObjectId::from_legacy_named("obj_stale_handle_after_compact").expect("object");
    object.path = "/docs/stale-handle-after-compact.md".to_string();
    stale
        .put_object(&object)
        .expect("stale handle should refresh from the new snapshot before writing");
    assert_eq!(
        stale
            .get_object(&object.object_id)
            .expect("object lookup after stale write"),
        Some(object)
    );

    cleanup(&root);
}

#[test]
fn local_backend_incrementally_advances_the_external_read_snapshot_cache() {
    let root = temp_root("local_backend_advances_external_read_snapshot_cache");
    let mut writer = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("writer backend");
    writer.put_file_system(&file_system()).expect("filesystem");
    let mut reader = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reader backend");

    let mut first = object(version("version_1"));
    first.object_id =
        DfsObjectId::from_legacy_named("obj_cached_snapshot_first").expect("object id");
    first.path = "/docs/cached-snapshot-first.md".to_string();
    writer.put_object(&first).expect("first object");
    assert_eq!(
        reader.get_object(&first.object_id).expect("first read"),
        Some(first)
    );
    let (first_tables, first_offset) = {
        let cache = reader.read_snapshot_cache.lock().expect("snapshot cache");
        let cache = cache.as_ref().expect("populated snapshot cache");
        (Arc::as_ptr(&cache.tables), cache.table_log_offset)
    };

    let mut second = object(version("version_1"));
    second.object_id =
        DfsObjectId::from_legacy_named("obj_cached_snapshot_second").expect("object id");
    second.path = "/docs/cached-snapshot-second.md".to_string();
    writer.put_object(&second).expect("second object");
    assert_eq!(
        reader.get_object(&second.object_id).expect("second read"),
        Some(second.clone())
    );
    let (second_tables, second_offset) = {
        let cache = reader.read_snapshot_cache.lock().expect("snapshot cache");
        let cache = cache.as_ref().expect("advanced snapshot cache");
        (Arc::as_ptr(&cache.tables), cache.table_log_offset)
    };

    assert_eq!(
        first_tables, second_tables,
        "sequential external reads should reuse the cached table allocation"
    );
    assert!(second_offset > first_offset);
    assert_eq!(
        reader
            .materialize_pending_replica_journal()
            .expect("refresh live tables"),
        2,
        "the retained reader should catch up both external object batches"
    );
    assert!(
        reader
            .read_snapshot_cache
            .lock()
            .expect("snapshot cache")
            .is_none(),
        "catching the live table image up must release the obsolete full snapshot"
    );
    assert_eq!(
        reader.get_object(&second.object_id).expect("live read"),
        Some(second)
    );
    assert!(
        reader
            .read_snapshot_cache
            .lock()
            .expect("snapshot cache")
            .is_none(),
        "a caught-up handle should borrow its live tables instead of rebuilding the cache"
    );

    cleanup(&root);
}

#[test]
fn local_backend_consistent_read_snapshot_polls_once_and_resumes_external_freshness() {
    let root = temp_root("local_backend_consistent_read_snapshot");
    let mut writer = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("writer backend");
    writer.put_file_system(&file_system()).expect("filesystem");
    let mut reader = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reader backend");

    reader
        .begin_consistent_read_snapshot()
        .expect("pin consistent read snapshot");
    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    for _ in 0..128 {
        assert_eq!(
            reader.get_object(&object_id()).expect("pinned lookup"),
            None
        );
    }
    assert_eq!(
        crate::metadata_read_snapshot::metadata_durable_table_probes(),
        0,
        "bulk reads inside one coherent snapshot must not poll durable table files per record"
    );

    let external = object(version("version_external"));
    writer.put_object(&external).expect("external object");
    assert_eq!(
        reader
            .get_object(&external.object_id)
            .expect("pinned external lookup"),
        None,
        "the bounded read scope must retain its coherent starting generation"
    );
    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    reader.end_consistent_read_snapshot();
    assert_eq!(
        reader
            .get_object(&external.object_id)
            .expect("fresh external lookup"),
        Some(external),
        "ordinary cross-process freshness checks must resume after hydration"
    );
    assert_eq!(
        crate::metadata_read_snapshot::metadata_durable_table_probes(),
        1
    );

    cleanup(&root);
}

#[test]
fn local_backend_appends_journal_entries_to_segment_files() {
    let root = temp_root("local_backend_appends_journal_entries_to_segment_files");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    let object = object(version("version_1"));

    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("object");

    let head = read_sobj::<DfsMetadataJournalHead>(&root.join("journal").join("head.sobj"))
        .expect("journal head");
    assert_eq!(
        head.next_sequence, 1,
        "local appends should leave the journal head as a recovery cache instead of rewriting it on every mutation"
    );
    assert!(
        root.join("journal")
            .join("segments")
            .join("00000000000000000001.slog")
            .is_file(),
        "new journal entries should be appended to a segment file"
    );
    assert!(
        !root
            .join("journal")
            .join("00000000000000000001.sobj")
            .exists(),
        "new journal entries should not create one object file per mutation"
    );
    assert_eq!(backend.journal_since(0).expect("journal").len(), 2);
    assert_eq!(backend.journal_since(1).expect("journal").len(), 1);

    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("reopen");
    assert_eq!(reopened.journal_since(0).expect("journal").len(), 2);

    cleanup(&root);
}

#[test]
fn local_backend_inspection_snapshot_refreshes_in_place_without_repairing_torn_tail() {
    let root = temp_root("local_backend_inspection_snapshot_is_lenient");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut writer = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("writer");
    writer.put_file_system(&file_system()).expect("filesystem");
    let mut reader =
        LocalSobjMetadataBackend::open_for_inspection(shard_id, &root).expect("reader");

    let mut first = object(version("version_1"));
    first.object_id =
        DfsObjectId::from_legacy_named("obj_inspection_refresh_first").expect("object id");
    first.path = "docs/inspection-refresh-first.md".to_string();
    writer.put_object(&first).expect("first object");
    assert_eq!(
        reader.get_object(&first.object_id).expect("external read"),
        Some(first.clone())
    );
    assert!(
        reader
            .read_snapshot_cache
            .lock()
            .expect("snapshot cache")
            .is_some(),
        "an unpinned external read should populate the secondary snapshot cache"
    );

    let mut second = object(version("version_1"));
    second.object_id =
        DfsObjectId::from_legacy_named("obj_inspection_refresh_second").expect("object id");
    second.path = "docs/inspection-refresh-second.md".to_string();
    writer.put_object(&second).expect("second object");

    reader
        .begin_inspection_snapshot()
        .expect("refresh and pin inspection image");
    assert!(
        reader
            .read_snapshot_cache
            .lock()
            .expect("snapshot cache")
            .is_none(),
        "in-place inspection refresh should release the second full table image"
    );
    assert_eq!(
        reader.get_object(&second.object_id).expect("pinned read"),
        Some(second.clone())
    );
    reader.end_inspection_snapshot();

    writer
        .compact_metadata_table_log_if_needed(1)
        .expect("publish compacted snapshot");
    let snapshot_verification_path = root
        .join("records")
        .join("tables")
        .join("snapshot.verified.sobj");
    remove_file_if_exists(&snapshot_verification_path)
        .expect("remove snapshot verification fixture");
    reader
        .begin_inspection_snapshot()
        .expect("lenient snapshot-generation reload");
    assert_eq!(
        reader
            .get_object(&second.object_id)
            .expect("snapshot-generation read"),
        Some(second.clone())
    );
    reader.end_inspection_snapshot();
    assert!(
        !snapshot_verification_path.exists(),
        "inspection refresh must not persist missing snapshot verification"
    );
    drop(writer);

    // Model a checkpoint rotation that crashed partway through its last
    // record. This forces the full-reload branch: an inspection refresh
    // must replay the complete prefix but leave the durable torn bytes
    // untouched for an authoritative writer to repair later.
    let table_path = metadata_table_path(&root);
    let checkpoint_path = crate::metadata_table_store::metadata_table_checkpoint_log_path(&root);
    fs::rename(&table_path, &checkpoint_path).expect("rotate active table log");
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&checkpoint_path)
        .expect("open checkpoint log");
    file.write_all(&512_u32.to_le_bytes())
        .and_then(|()| file.write_all(&[0xAB; 17]))
        .expect("append crash-torn checkpoint record");
    file.sync_all().expect("sync torn fixture");
    drop(file);
    let torn_len = fs::metadata(&checkpoint_path)
        .expect("checkpoint metadata")
        .len();

    reader
        .begin_inspection_snapshot()
        .expect("lenient full inspection reload");
    assert_eq!(
        reader.get_object(&second.object_id).expect("reloaded read"),
        Some(second)
    );
    reader.end_inspection_snapshot();
    assert_eq!(
        fs::metadata(&checkpoint_path)
            .expect("checkpoint metadata after inspection")
            .len(),
        torn_len,
        "inspection refresh must not truncate a crash-torn checkpoint tail"
    );

    cleanup(&root);
}

#[test]
fn local_backend_reopen_repairs_journal_only_tail_behind_matching_hints() {
    let root = temp_root("local_backend_reopen_repairs_journal_only_stale_hints");
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
    write_sobj_atomic(
        &root.join("journal").join("head.sobj"),
        &DfsMetadataJournalHead { next_sequence: 2 },
    )
    .expect("journal head");
    write_sobj_atomic(
        &backend.local_materialization_cursor_path(),
        &DfsMetadataReplayCursor {
            shard_id: shard_id.clone(),
            last_sequence: 1,
        },
    )
    .expect("local cursor");
    append_segmented_journal_entry(
        &root,
        &DfsMetadataJournalEntry {
            shard_id: shard_id.clone(),
            sequence: 2,
            created_at: OffsetDateTime::now_utc(),
            mutation: DfsMetadataMutation::put_object(&first_object),
        },
    )
    .expect("durable journal-only tail");
    drop(backend);

    let mut reopened = LocalSobjMetadataBackend::new(shard_id, &root)
        .expect("reopen discovers and materializes the journal-only tail");
    assert_eq!(
        reopened
            .get_object(&first_object.object_id)
            .expect("object"),
        Some(first_object.clone())
    );
    assert_eq!(reopened.journal_last_sequence(), 2);
    let mut successor = object(version("version_2"));
    successor.object_id =
        DfsObjectId::from_legacy_named("obj_after_journal_tail_repair").expect("object id");
    successor.path = "/docs/after-journal-tail-repair.md".to_string();
    assert_eq!(
        reopened
            .put_object(&successor)
            .expect("successor append")
            .sequence,
        3
    );

    cleanup(&root);
}

#[test]
fn local_backend_reads_bounded_journal_batches() {
    let root = temp_root("local_backend_reads_bounded_journal_batches");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    backend.put_file_system(&file_system).expect("filesystem");
    for index in 0..5 {
        let mut object = object(version("version_1"));
        object.object_id =
            DfsObjectId::from_legacy_named(format!("obj_bounded_journal_{index}")).expect("object");
        object.path = format!("/docs/bounded-{index}.md");
        backend.put_object(&object).expect("object");
    }

    let first_batch = backend
        .journal_since_limited(0, 3)
        .expect("first bounded batch");
    assert_eq!(
        first_batch
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let second_batch = backend
        .journal_since_limited(3, 3)
        .expect("second bounded batch");
    assert_eq!(
        second_batch
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5, 6]
    );
    assert!(
        backend
            .journal_since_limited(6, 3)
            .expect("empty bounded batch")
            .is_empty()
    );

    cleanup(&root);
}

#[test]
fn local_backend_journal_reader_ignores_invalid_tail_record() {
    let root = temp_root("local_backend_journal_reader_ignores_invalid_tail_record");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend
        .put_object(&object(version("version_1")))
        .expect("object");
    let expected = backend.journal_since(0).expect("journal before tail");

    let segment_path = root
        .join("journal")
        .join("segments")
        .join("00000000000000000001.slog");
    let invalid_payload = b"not-sobj";
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&segment_path)
        .expect("open segment");
    file.write_all(&(invalid_payload.len() as u32).to_le_bytes())
        .and_then(|()| file.write_all(invalid_payload))
        .expect("invalid tail append");
    drop(file);

    assert_eq!(
        backend
            .journal_since(0)
            .expect("journal after invalid tail"),
        expected,
        "journal polling must ignore a malformed trailing record and retry it later"
    );
    cleanup(&root);
}

#[test]
fn local_backend_journal_reader_waits_for_writer_lock() {
    let root = temp_root("local_backend_journal_reader_waits_for_writer_lock");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend
        .put_object(&object(version("version_1")))
        .expect("object");
    let expected = backend.journal_since(0).expect("journal before tail");
    let segment_path = root
        .join("journal")
        .join("segments")
        .join("00000000000000000001.slog");
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let root_for_writer = root.to_path_buf();
    let segment_for_writer = segment_path.clone();
    let writer = std::thread::spawn(move || {
        let writer_backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root_for_writer)
                .expect("writer backend");
        let _lock = writer_backend.metadata_write_lock().expect("writer lock");
        locked_tx.send(()).expect("signal lock acquired");
        std::thread::sleep(Duration::from_millis(100));
        let invalid_payload = b"not-sobj";
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&segment_for_writer)
            .expect("open segment");
        file.write_all(&(invalid_payload.len() as u32).to_le_bytes())
            .and_then(|()| file.write_all(invalid_payload))
            .expect("invalid tail append");
    });
    locked_rx.recv().expect("writer acquired lock");

    let started = std::time::Instant::now();
    assert_eq!(
        backend
            .journal_since(0)
            .expect("journal reader waits for lock"),
        expected,
        "journal polling must not read while a writer is appending a record"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(50),
        "journal reader should wait for the in-flight metadata writer"
    );
    writer.join().expect("writer thread");
    cleanup(&root);
}

#[test]
fn local_backend_open_waits_for_writer_lock_before_table_replay() {
    let root = temp_root("local_backend_open_waits_for_writer_lock");
    let mut seed = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("seed backend");
    seed.put_file_system(&file_system()).expect("filesystem");
    drop(seed);

    let mut object = object(version("version_1"));
    object.object_id =
        DfsObjectId::from_legacy_named("obj_open_waits_for_writer_lock").expect("object");
    object.path = "/docs/open-waits-for-writer-lock.md".to_string();
    let object_id = object.object_id.clone();
    let expected_object = object.clone();
    let root_for_writer = root.to_path_buf();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let mut writer_backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), root_for_writer)
                .expect("writer backend");
        let _lock = writer_backend.metadata_write_lock().expect("writer lock");
        locked_tx.send(()).expect("signal lock acquired");
        std::thread::sleep(Duration::from_millis(100));
        let entry = writer_backend
            .write_journal_unlocked(DfsMetadataMutation::put_object(&object))
            .expect("append object journal entry while lock is held");
        writer_backend
            .append_and_apply_table_ops(
                entry.sequence,
                vec![DfsMetadataTableOp::PutObject { object }],
            )
            .expect("append table batch while lock is held");
    });
    locked_rx.recv().expect("writer acquired lock");

    let started = std::time::Instant::now();
    let opened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("opener backend");
    assert!(
        started.elapsed() >= Duration::from_millis(50),
        "backend open must wait for in-flight metadata writers before table replay"
    );
    assert_eq!(
        opened
            .get_object(&object_id)
            .expect("object lookup")
            .as_ref(),
        Some(&expected_object),
        "backend open must replay the complete table batch after the writer releases the lock"
    );

    writer.join().expect("writer thread");
    cleanup(&root);
}

#[test]
fn local_backend_reads_byte_bounded_journal_batches() {
    let root = temp_root("local_backend_reads_byte_bounded_journal_batches");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    for index in 0..3 {
        let mut object = object(version("version_1"));
        object.object_id =
            DfsObjectId::from_legacy_named(format!("obj_byte_bounded_journal_{index}"))
                .expect("object");
        object.path = format!("/docs/{}/bounded-{index}.md", "wide".repeat(128));
        backend.put_object(&object).expect("object");
    }

    let first_unbounded = backend
        .journal_batch_since_limited(1, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, usize::MAX)
        .expect("unbounded batch")
        .expect("batch");
    let first_entry = first_unbounded.entries[0].clone();
    let one_entry_limit = metadata_journal_batch_encoded_len(&DfsMetadataJournalBatch {
        shard_id: shard_id.clone(),
        next_after_sequence: first_entry.sequence,
        entries: vec![first_entry],
    })
    .expect("encoded length");

    let first_batch = backend
        .journal_batch_since_limited(
            1,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            one_entry_limit,
        )
        .expect("byte bounded batch")
        .expect("batch");
    assert_eq!(
        first_batch
            .entries
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(first_batch.next_after_sequence, 2);
    assert!(
        metadata_journal_batch_encoded_len(&first_batch).expect("encoded length")
            <= one_entry_limit
    );

    let oversized_first = backend
        .journal_batch_since_limited(1, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, 1)
        .expect("oversized first batch")
        .expect("batch");
    assert_eq!(oversized_first.entries.len(), 1);
    assert_eq!(oversized_first.next_after_sequence, 2);

    cleanup(&root);
}

#[test]
fn local_backend_migrates_legacy_journal_files_to_segments() {
    let root = temp_root("local_backend_migrates_legacy_journal_files_to_segments");
    fs::create_dir_all(root.join("journal")).expect("journal dir");
    let legacy_path = root.join("journal").join("00000000000000000001.sobj");
    let legacy_entry = DfsMetadataJournalEntry {
        shard_id: DfsMetadataShardId::new("shard-root"),
        sequence: 1,
        created_at: OffsetDateTime::UNIX_EPOCH,
        mutation: DfsMetadataMutation::PutFileSystem {
            file_system_id: file_system_id(),
            file_system: Some(file_system()),
        },
    };
    write_sobj_atomic(&legacy_path, &legacy_entry).expect("legacy journal entry");

    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    assert!(
        !legacy_path.exists(),
        "opening the backend should migrate legacy root journal files out of the live path"
    );
    assert!(
        root.join("journal")
            .join("segments")
            .join("00000000000000000001.slog")
            .is_file(),
        "legacy journal entries should be appended to the segmented journal"
    );
    assert_eq!(
        backend.journal_since(0).expect("journal"),
        vec![legacy_entry]
    );
    let next = backend.put_file_system(&file_system()).expect("filesystem");
    assert_eq!(
        next.sequence, 2,
        "sequence allocation should continue after migrated legacy entries"
    );

    cleanup(&root);
}

#[test]
fn local_backend_hydrates_legacy_journal_payloads_from_records() {
    let root = temp_root("local_backend_hydrates_legacy_journal_payloads_from_records");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend
        .put_object(&object(version("version_1")))
        .expect("object");
    backend
        .put_version(&DfsObjectVersionRecord {
            content_durability: None,
            history_order: 0,
            retained: None,
            file_system_id: file_system_id(),
            object_id: object_id(),
            version: version("version_1"),
            content_hash: Some("blake3:old".to_string()),
            size_bytes: None,
            locations: vec![node("node_owner")],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        })
        .expect("version");
    let mut committed_object = object(version("version_2"));
    committed_object.content_hash = Some("blake3:new".to_string());
    committed_object.content_size_bytes = Some(128);
    let visible_version = DfsObjectVersionRecord {
        content_durability: None,
        history_order: 0,
        retained: None,
        file_system_id: file_system_id(),
        object_id: object_id(),
        version: version("version_2"),
        content_hash: committed_object.content_hash.clone(),
        size_bytes: committed_object.content_size_bytes,
        locations: vec![node("node_owner")],
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    backend
        .commit_visible_version(
            &version("version_1"),
            &committed_object,
            &visible_version,
            &[],
        )
        .expect("commit visible version");
    let mut transaction_object = object(version("version_namespace_tx"));
    transaction_object.object_id =
        DfsObjectId::from_legacy_named("obj_hydrated_namespace_tx").expect("object");
    transaction_object.path = "docs/transaction.md".to_string();
    transaction_object.content_hash = None;
    transaction_object.content_size_bytes = None;
    let transaction = DfsNamespaceTransaction {
        file_version_pins: None,
        transaction_id: "namespace_tx_hydrate_legacy".to_string(),
        file_system_id: file_system_id(),
        objects: vec![transaction_object],
        versions: Vec::new(),
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    backend
        .apply_namespace_transaction(&transaction)
        .expect("namespace transaction");

    let legacy_entries = backend
        .journal_since(0)
        .expect("journal")
        .into_iter()
        .map(|mut entry| {
            match &mut entry.mutation {
                DfsMetadataMutation::PutFileSystem { file_system, .. } => {
                    *file_system = None;
                }
                DfsMetadataMutation::PutObject { object, .. } => {
                    *object = None;
                }
                DfsMetadataMutation::PutVersion { version_record, .. } => {
                    *version_record = None;
                }
                DfsMetadataMutation::CommitVisibleVersion {
                    committed_object,
                    visible_version,
                    ..
                } => {
                    *committed_object = None;
                    *visible_version = None;
                }
                DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } => {
                    *transaction = None;
                }
                _ => {}
            }
            entry
        })
        .collect::<Vec<_>>();

    let hydrated = backend
        .hydrate_journal_entry_payloads(legacy_entries)
        .expect("hydrate legacy entries");
    assert!(matches!(
        &hydrated[0].mutation,
        DfsMetadataMutation::PutFileSystem {
            file_system: Some(_),
            ..
        }
    ));
    assert!(matches!(
        &hydrated[1].mutation,
        DfsMetadataMutation::PutObject {
            object: Some(_),
            ..
        }
    ));
    assert!(matches!(
        &hydrated[2].mutation,
        DfsMetadataMutation::PutVersion {
            version_record: Some(_),
            ..
        }
    ));
    assert!(hydrated.iter().any(|entry| matches!(
        &entry.mutation,
        DfsMetadataMutation::CommitVisibleVersion {
            committed_object: Some(object),
            visible_version: Some(version_record),
            ..
        } if object.version == version("version_2")
            && version_record.version == version("version_2")
    )));
    assert!(hydrated.iter().any(|entry| matches!(
        &entry.mutation,
        DfsMetadataMutation::ApplyNamespaceTransaction {
            transaction: Some(transaction),
            ..
        } if transaction.transaction_id == "namespace_tx_hydrate_legacy"
    )));

    cleanup(&root);
}

#[test]
fn content_root_catalog_multi_page_drain_builds_one_exact_snapshot() {
    let root = temp_root("content-root-catalog-page-cache");
    let shard_id = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    let file = object(version("version_catalog_current"));
    backend.put_object(&file).expect("object");
    for index in 0..150 {
        let mut record = object_version_record(
            &file,
            version(&format!("version_catalog_{index:03}")),
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(index),
        );
        record.content_hash = Some(format!("blake3:catalog-{index:03}"));
        backend.put_version(&record).expect("catalog version");
    }

    let shard = DfsMetadataShardConfig {
        file_system_id: file.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: node("node_owner"),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let mut request = DfsContentRootCatalogPageRequest {
        identity: DfsContentRootCatalogShardIdentity::from_config(&shard),
        expected_topology_digest: dfs_content_root_catalog_topology_digest(&[shard])
            .expect("topology digest"),
        minimum_journal_sequence: 0,
        minimum_state_fingerprint: None,
        after: None,
        max_entries: DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_ENTRIES,
    };
    let mut page_count = 0;
    loop {
        let page = backend
            .content_root_catalog_page(&request)
            .expect("content-root page");
        page.validate_for_request(&request)
            .expect("valid content-root page");
        page_count += 1;
        let Some(next) = page.next_cursor else {
            break;
        };
        request.minimum_journal_sequence = next.journal_sequence;
        request.minimum_state_fingerprint = Some(next.state_fingerprint.clone());
        request.after = Some(next);
    }
    assert!(page_count >= 2, "fixture must drain multiple pages");
    assert_eq!(
        backend
            .content_root_catalog_snapshot_builds
            .load(Ordering::Relaxed),
        1,
        "continuation pages must reuse one exact sorted root snapshot"
    );

    let mut later = object_version_record(
        &file,
        version("version_catalog_later"),
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(151),
    );
    later.content_hash = Some("blake3:catalog-later".to_string());
    backend.put_version(&later).expect("later version");
    request.minimum_journal_sequence = 0;
    request.minimum_state_fingerprint = None;
    request.after = None;
    backend
        .content_root_catalog_page(&request)
        .expect("page after durable mutation");
    assert_eq!(
        backend
            .content_root_catalog_snapshot_builds
            .load(Ordering::Relaxed),
        2,
        "a new durable cut must rebuild the root snapshot"
    );
    cleanup(&root);
}

#[test]
fn local_backend_rebuilds_dirfrag_counts_during_table_replay() {
    let root = temp_root("local_backend_rebuilds_dirfrag_counts");
    {
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut readme = object(version("version_1"));
        readme.path = "/docs/readme.md".to_string();
        let mut changelog = object(version("version_1"));
        changelog.object_id = DfsObjectId::from_legacy_named("obj_changelog").expect("object");
        changelog.path = "/docs/changelog.md".to_string();
        for object in [&readme, &changelog] {
            backend.put_object(object).expect("object");
        }

        let path_record = path_record_for_object(&readme);
        assert_eq!(
            table_dirfrag(
                &backend,
                &path_record.parent_inode_id,
                &path_record.dirfrag_id
            )
            .entry_count,
            2
        );
        force_table_replay_from_journal(&backend, &root, 0);
    }

    let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("recovered backend");
    let path_record = path_record_for_object(&object(version("version_1")));
    assert_eq!(
        table_dirfrag(
            &recovered,
            &path_record.parent_inode_id,
            &path_record.dirfrag_id
        )
        .entry_count,
        2
    );

    cleanup(&root);
}

#[test]
fn local_backend_replays_journal_batch_into_follower_backend() {
    let leader_root = temp_root("local_backend_replays_journal_batch_into_follower_leader");
    let follower_root = temp_root("local_backend_replays_journal_batch_into_follower_follower");
    let shard_id = DfsMetadataShardId::new("shard-replay");
    let mut leader =
        LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("leader backend");
    let mut follower =
        LocalSobjMetadataBackend::new(shard_id.clone(), &follower_root).expect("follower backend");

    let file_system = file_system();
    let initial = object(version("version_1"));
    leader.put_file_system(&file_system).expect("filesystem");
    leader.put_object(&initial).expect("object");

    let mut committed = initial.clone();
    committed.version = version("version_2");
    committed.content_hash = Some("blake3:new".to_string());
    committed.current_locations.push(node("node_replica"));
    let visible_version = DfsObjectVersionRecord {
        content_durability: None,
        history_order: 0,
        retained: None,
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
            event_id: EventId::from_legacy_named("evt_replay_commit").expect("event"),
        },
        replica_node_id: node("node_replica"),
        content_hash: committed.content_hash.clone(),
        received_at: OffsetDateTime::UNIX_EPOCH,
    };
    leader.put_receipt(&receipt).expect("receipt");
    leader
        .commit_visible_version(
            &initial.version,
            &committed,
            &visible_version,
            std::slice::from_ref(&receipt.replica_node_id),
        )
        .expect("commit");

    let mut subscription = leader.subscribe(0);
    let mut batch = leader
        .poll_subscription(&mut subscription)
        .expect("journal batch");
    assert_eq!(batch.entries.len(), 4);
    let source_sequence_base = JOURNAL_SEGMENT_ENTRY_LIMIT + 64;
    install_empty_replay_checkpoint(
        &mut follower,
        &batch.shard_id,
        source_sequence_base.saturating_sub(1),
    );
    for (offset, entry) in batch.entries.iter_mut().enumerate() {
        entry.sequence = source_sequence_base + offset as u64;
    }
    batch.next_after_sequence = batch
        .entries
        .last()
        .expect("non-empty replay batch")
        .sequence;

    assert_eq!(
        follower
            .apply_journal_batch(&batch)
            .expect("apply journal batch"),
        4
    );
    assert_eq!(
        follower.journal_since(0).expect("follower journal"),
        batch.entries,
        "replicated metadata replay should persist source journal entries instead of appending follower-local replacements"
    );
    assert_eq!(
        follower
            .get_file_system(&file_system.file_system_id)
            .expect("filesystem"),
        Some(file_system)
    );
    assert_eq!(
        follower
            .get_object(&committed.object_id)
            .expect("object")
            .expect("object")
            .version,
        committed.version
    );
    assert_eq!(
        follower
            .get_version(&committed.object_id, &committed.version)
            .expect("version"),
        Some(normalized_version_record(&visible_version))
    );
    assert_eq!(
        follower
            .receipts_for_version(&committed.object_id, &committed.version)
            .expect("receipt"),
        vec![receipt]
    );

    assert_eq!(
        follower
            .apply_journal_batch(&batch)
            .expect("reapply journal batch"),
        0
    );
    assert_eq!(
        follower.journal_since(0).expect("follower journal reapply"),
        batch.entries,
        "idempotent replays must not append duplicate follower journal entries"
    );
    assert_eq!(
        read_sobj::<DfsMetadataReplayCursor>(&follower.replay_cursor_path(&shard_id))
            .expect("replay cursor")
            .last_sequence,
        source_sequence_base.saturating_sub(1),
        "a small replicated batch should leave the recovery-hint cursor at its checkpoint"
    );
    assert_eq!(
        follower
            .metadata_replay_cursor(&shard_id)
            .expect("authoritative replay position"),
        batch.next_after_sequence,
        "the durable prefix must cover a checkpointed cursor between flushes"
    );
    let status = follower
        .metadata_shard_statuses()
        .expect("metadata shard status")
        .pop()
        .expect("follower shard status");
    assert_eq!(
        status.journal_last_sequence, batch.next_after_sequence,
        "a replayed source batch must advance the local journal head to the source sequence"
    );
    assert_eq!(
        status.journal_next_sequence,
        batch.next_after_sequence.saturating_add(1)
    );
    assert_eq!(
        status.replay_cursors,
        vec![DfsMetadataReplayCursorStatus {
            source_shard_id: shard_id.clone(),
            replayed_through_sequence: batch.next_after_sequence,
        }]
    );

    drop(follower);
    let mut reopened = LocalSobjMetadataBackend::new(shard_id.clone(), &follower_root)
        .expect("reopen follower with a lagging replay hint");
    assert_eq!(
        reopened
            .metadata_replay_cursor(&shard_id)
            .expect("reopened authoritative replay position"),
        batch.next_after_sequence,
        "reopen must recover the authoritative replay position from the durable prefix proof"
    );
    assert_eq!(
        reopened
            .replay_cursor_statuses()
            .expect("reopened replay cursor status"),
        vec![DfsMetadataReplayCursorStatus {
            source_shard_id: shard_id,
            replayed_through_sequence: batch.next_after_sequence,
        }],
        "operator status must not regress to the stale checkpoint after reopen"
    );
    assert_eq!(
        reopened
            .apply_journal_batch(&batch)
            .expect("reapply batch after reopen"),
        0,
        "the exact durable prefix, not the lagging cursor hint, owns replay idempotence"
    );
    assert_eq!(
        reopened
            .get_object(&committed.object_id)
            .expect("reopened object")
            .expect("reopened object")
            .version,
        committed.version
    );

    cleanup(&leader_root);
    cleanup(&follower_root);
}

#[test]
fn deferred_replica_materialization_batches_a_replayable_suffix_once() {
    let root = temp_root("deferred_replica_materialization_batches_suffix");
    let source_root = root.join("source");
    let replica_root = root.join("replica");
    let shard_id = DfsMetadataShardId::new("root");
    let mut source =
        LocalSobjMetadataBackend::new(shard_id.clone(), &source_root).expect("source backend");
    let mut file_system = file_system();
    source
        .put_file_system(&file_system)
        .expect("source filesystem");
    for index in 1..4 {
        file_system.display_name = format!("replica materialization generation {index}");
        source
            .put_file_system(&file_system)
            .expect("source filesystem update");
    }
    let mut entries = source.journal_since(0).expect("source journal suffix");
    assert_eq!(entries.len(), 4);

    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        file_system.file_system_id.clone(),
        shard_id.clone(),
        node("node_deferred_materialization_source"),
        1,
    );
    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root).expect("replica backend");
    let baseline = entries.remove(0);
    replica
        .apply_journal_batch_with_replica_permit(
            &permit,
            &DfsMetadataJournalBatch {
                shard_id: shard_id.clone(),
                next_after_sequence: baseline.sequence,
                entries: vec![baseline],
            },
        )
        .expect("materialized replica baseline");
    for entry in entries {
        let sequence = entry.sequence;
        let report = replica
            .persist_journal_batch_with_replica_permit_report(
                &permit,
                &DfsMetadataJournalBatch {
                    shard_id: shard_id.clone(),
                    entries: vec![entry],
                    next_after_sequence: sequence,
                },
            )
            .expect("durable replica suffix entry");
        assert!(report.materialization_pending());
    }

    assert_eq!(replica.materialized_through_sequence, 1);
    let table_batches_before = metadata_table_batch_count(&replica_root);
    reset_metadata_durable_table_reloads();
    assert_eq!(
        replica
            .materialize_pending_replica_journal()
            .expect("materialize replayable suffix"),
        3,
    );
    let table_batches_after = metadata_table_batch_count(&replica_root);
    assert!(
        table_batches_after.saturating_sub(table_batches_before) <= 1,
        "one replayable suffix should persist at most one active table batch before an asynchronous checkpoint fold; before={table_batches_before}, after={table_batches_after}"
    );
    assert_eq!(
        metadata_durable_table_reloads(),
        0,
        "an exact resident table proof must materialize its deferred suffix without decoding the same durable snapshot again"
    );
    assert_eq!(
        replica
            .get_file_system(&file_system.file_system_id)
            .expect("materialized filesystem"),
        Some(file_system.clone()),
    );
    drop(replica);

    let reopened = LocalSobjMetadataBackend::new(shard_id, &replica_root)
        .expect("reopen materialized replica");
    assert_eq!(
        reopened
            .get_file_system(&file_system.file_system_id)
            .expect("reopened filesystem"),
        Some(file_system),
        "the batched materialization must survive either the active log or its folded checkpoint"
    );

    cleanup(&root);
}

#[test]
fn local_backend_rejects_replication_from_foreign_logical_shard() {
    let root = temp_root("local_backend_rejects_foreign_replication_shard");
    let source_shard_id = DfsMetadataShardId::new("source-shard");
    let target_shard_id = DfsMetadataShardId::new("target-shard");
    let mut source = LocalSobjMetadataBackend::new(source_shard_id.clone(), root.join("source"))
        .expect("source backend");
    let mut target = LocalSobjMetadataBackend::new(target_shard_id.clone(), root.join("target"))
        .expect("target backend");
    let file_system = file_system();
    source
        .put_file_system(&file_system)
        .expect("source filesystem");
    let batch = source
        .journal_batch_since_limited(0, 16, usize::MAX)
        .expect("source journal batch")
        .expect("source journal entries");
    let snapshot = source.export_table_snapshot();

    let batch_error = target
        .apply_journal_batch(&batch)
        .expect_err("foreign-shard journal replay must fail");
    assert!(
        batch_error.to_string().contains(target_shard_id.0.as_str())
            && batch_error.to_string().contains(source_shard_id.0.as_str()),
        "identity rejection must name both shards: {batch_error}"
    );
    let snapshot_error = target
        .replace_with_replicated_snapshot(
            source_shard_id.clone(),
            batch.next_after_sequence,
            snapshot,
        )
        .expect_err("foreign-shard snapshot install must fail");
    assert!(
        snapshot_error
            .to_string()
            .contains(target_shard_id.0.as_str())
            && snapshot_error
                .to_string()
                .contains(source_shard_id.0.as_str()),
        "identity rejection must name both shards: {snapshot_error}"
    );
    assert!(
        target
            .metadata_replay_cursor(&source_shard_id)
            .expect_err("foreign-shard cursor query must fail")
            .to_string()
            .contains(source_shard_id.0.as_str()),
        "cursor lookup must enforce the same logical-shard identity"
    );
    assert_eq!(
        target.journal_last_sequence(),
        0,
        "rejected foreign replication must not advance the target journal"
    );
    assert!(
        target
            .get_file_system(&file_system.file_system_id)
            .expect("target filesystem lookup")
            .is_none(),
        "rejected foreign replication must not mutate target tables"
    );

    cleanup(&root);
}

#[test]
fn local_backend_replays_authoritative_commit_without_receipt_entry() {
    let leader_root = temp_root("local_backend_replays_authoritative_commit_leader");
    let follower_root = temp_root("local_backend_replays_authoritative_commit_follower");
    let shard_id = DfsMetadataShardId::new("shard-authoritative-commit");
    let mut leader =
        LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("leader backend");
    let mut follower =
        LocalSobjMetadataBackend::new(shard_id, &follower_root).expect("follower backend");

    let file_system = file_system();
    let initial = object(version("version_1"));
    leader.put_file_system(&file_system).expect("filesystem");
    leader.put_object(&initial).expect("object");

    let mut committed = initial.clone();
    committed.version = version("version_2");
    committed.content_hash = Some("blake3:new".to_string());
    committed.current_locations.push(node("node_replica"));
    let visible_version = DfsObjectVersionRecord {
        content_durability: None,
        history_order: 0,
        retained: None,
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
            event_id: EventId::from_legacy_named("evt_missing_receipt_commit").expect("event"),
        },
        replica_node_id: node("node_replica"),
        content_hash: committed.content_hash.clone(),
        received_at: OffsetDateTime::UNIX_EPOCH,
    };
    leader.put_receipt(&receipt).expect("receipt");
    leader
        .commit_visible_version(
            &initial.version,
            &committed,
            &visible_version,
            std::slice::from_ref(&receipt.replica_node_id),
        )
        .expect("commit");

    let mut subscription = leader.subscribe(0);
    let mut batch = leader
        .poll_subscription(&mut subscription)
        .expect("journal batch");
    batch
        .entries
        .retain(|entry| !matches!(entry.mutation, DfsMetadataMutation::PutReceipt { .. }));
    assert_eq!(batch.entries.len(), 3);
    let source_sequence_base = JOURNAL_SEGMENT_ENTRY_LIMIT + 128;
    install_empty_replay_checkpoint(
        &mut follower,
        &batch.shard_id,
        source_sequence_base.saturating_sub(1),
    );
    for (offset, entry) in batch.entries.iter_mut().enumerate() {
        entry.sequence = source_sequence_base + offset as u64;
    }
    batch.next_after_sequence = batch
        .entries
        .last()
        .expect("non-empty replay batch")
        .sequence;

    assert_eq!(
        follower
            .apply_journal_batch(&batch)
            .expect("apply commit without receipt"),
        3
    );
    assert_eq!(
        follower
            .get_object(&committed.object_id)
            .expect("object")
            .expect("object")
            .version,
        committed.version
    );
    assert_eq!(
        follower
            .get_version(&committed.object_id, &committed.version)
            .expect("version"),
        Some(normalized_version_record(&visible_version))
    );
    assert!(
        follower
            .receipts_for_version(&committed.object_id, &committed.version)
            .expect("receipts")
            .is_empty(),
        "the commit entry alone should make the write visible; receipts may replay later"
    );

    cleanup(&leader_root);
    cleanup(&follower_root);
}

#[test]
fn local_backend_replaces_divergent_replica_with_replicated_snapshot() {
    let leader_root = temp_root("local_backend_snapshot_repair_leader");
    let follower_root = temp_root("local_backend_snapshot_repair_follower");
    let shard_id = DfsMetadataShardId::new("root");
    let mut leader =
        LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("leader backend");
    let mut follower =
        LocalSobjMetadataBackend::new(shard_id.clone(), &follower_root).expect("follower backend");

    let leader_file_system = file_system();
    leader
        .put_file_system(&leader_file_system)
        .expect("leader filesystem");
    let mut follower_file_system = leader_file_system.clone();
    follower_file_system.display_name = "diverged".to_string();
    follower
        .put_file_system(&follower_file_system)
        .expect("follower filesystem");
    let mut follower_object = object(version("follower-v1"));
    follower_object.content_hash = Some("blake3:follower-diverged".to_string());
    follower
        .put_object(&follower_object)
        .expect("follower object");
    assert!(
        follower
            .apply_journal_batch(
                &leader
                    .journal_batch_since_limited(0, 1, usize::MAX)
                    .expect("leader batch")
                    .expect("leader batch")
            )
            .expect_err("conflicting sequence should reject before snapshot repair")
            .to_string()
            .contains("already exists with a different entry")
    );

    let archived = follower
        .replace_with_replicated_snapshot(
            shard_id.clone(),
            leader.journal_last_sequence(),
            leader.export_table_snapshot(),
        )
        .expect("replace snapshot");
    assert!(archived);
    assert_eq!(
        follower.metadata_prefix_proof().expect("follower proof"),
        leader.metadata_prefix_proof().expect("leader proof"),
        "snapshot install must adopt the validated authority chain head"
    );
    assert_eq!(
        follower
            .get_file_system(&leader_file_system.file_system_id)
            .expect("follower filesystem"),
        Some(leader_file_system)
    );
    assert!(
        follower_root.join("replica-repair-archives").exists(),
        "divergent replica state should be archived before replacement"
    );
    let archive_dir = read_dir(&follower_root.join("replica-repair-archives"))
        .expect("repair archives")
        .into_iter()
        .next()
        .expect("repair archive")
        .path();
    assert!(
        archive_dir.join("journal").join("segments").exists(),
        "journal segments should be archived without moving the live journal lock directory"
    );
    assert!(
        follower_root.join("journal").join("write.lock").exists(),
        "snapshot repair must keep the live journal directory in place for Windows"
    );
    let leader_object = object(version("leader-v1"));
    leader.put_object(&leader_object).expect("leader object");
    follower
        .apply_journal_batch(
            &leader
                .journal_batch_since_limited(1, 1, usize::MAX)
                .expect("leader object batch")
                .expect("leader object batch"),
        )
        .expect("repaired follower should accept future primary sequences");
    assert_eq!(
        follower.metadata_prefix_proof().expect("follower proof"),
        leader.metadata_prefix_proof().expect("leader proof"),
        "the next contiguous entry must extend the installed chain identically"
    );
    assert_eq!(
        follower
            .get_object(&leader_object.object_id)
            .expect("follower object"),
        Some(leader_object)
    );
    let status = follower
        .metadata_shard_statuses()
        .expect("metadata status")
        .pop()
        .expect("status");
    assert_eq!(status.journal_last_sequence, leader.journal_last_sequence());
    assert_eq!(
        status.replay_cursors,
        vec![DfsMetadataReplayCursorStatus {
            source_shard_id: shard_id.clone(),
            replayed_through_sequence: leader.journal_last_sequence(),
        }]
    );

    cleanup(&leader_root);
    cleanup(&follower_root);
}

#[test]
fn unreadable_replica_can_install_authoritative_snapshot() {
    let leader_root = temp_root("unreadable_snapshot_repair_leader");
    let replica_root = temp_root("unreadable_snapshot_repair_replica");
    let shard_id = DfsMetadataShardId::new("root");
    let mut leader = LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("leader");
    let file_system = file_system();
    leader.put_file_system(&file_system).expect("filesystem");
    let genesis_snapshot = leader.export_table_snapshot();
    let genesis_sequence = leader.journal_last_sequence();
    let object = object(version("authoritative-v1"));
    let object_entry = leader.put_object(&object).expect("object");
    let authoritative_sequence = leader.journal_last_sequence();
    let authoritative_snapshot = leader.export_table_snapshot();

    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root).expect("replica");
    replica
        .replace_with_replicated_snapshot(shard_id.clone(), genesis_sequence, genesis_snapshot)
        .expect("install genesis checkpoint");
    append_segmented_journal_entry(&replica_root, &object_entry)
        .expect("append authoritative entry");
    let mut conflicting_entry = object_entry.clone();
    conflicting_entry.created_at += time::Duration::nanoseconds(1);
    append_segmented_journal_entry(&replica_root, &conflicting_entry)
        .expect("append conflicting duplicate sequence");
    write_sobj_atomic(
        &replica_root.join("journal").join("head.sobj"),
        &DfsMetadataJournalHead {
            next_sequence: authoritative_sequence.saturating_add(1),
        },
    )
    .expect("journal head");
    drop(replica);

    let open_error = LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root)
        .expect_err("conflicting duplicate must make ordinary replay fail");
    assert!(
        matches!(
            &open_error,
            MetadataBackendError::Serde(detail)
                if detail.contains(&format!(
                    "conflicting duplicate sequence {}",
                    object_entry.sequence
                ))
        ),
        "unexpected conflicting duplicate error: {open_error}"
    );

    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        file_system.file_system_id.clone(),
        shard_id.clone(),
        node("node_unreadable_snapshot_source"),
        1,
    );
    let (repaired, archived) =
        LocalSobjMetadataBackend::replace_unreadable_with_replicated_snapshot(
            &permit,
            shard_id.clone(),
            &replica_root,
            shard_id.clone(),
            authoritative_sequence,
            authoritative_snapshot,
        )
        .expect("repair unreadable replica");
    assert!(archived);
    assert_eq!(
        repaired.get_object(&object.object_id).expect("object"),
        Some(object.clone())
    );
    drop(repaired);

    let reopened =
        LocalSobjMetadataBackend::new(shard_id, &replica_root).expect("reopen repaired replica");
    assert_eq!(
        reopened
            .get_object(&object.object_id)
            .expect("reopened object"),
        Some(object)
    );

    cleanup(&leader_root);
    cleanup(&replica_root);
}

#[test]
fn local_backend_persists_metadata_replication_cursor_monotonically() {
    let root = temp_root("local_backend_metadata_replication_cursor");
    let target_node_id = node("node_cursor_target");
    let shard_id = DfsMetadataShardId::new("root");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
    assert_eq!(
        backend
            .metadata_replication_cursor(&target_node_id, &shard_id)
            .expect("initial cursor"),
        0
    );

    backend
        .put_metadata_replication_cursor(&target_node_id, &shard_id, 42)
        .expect("write cursor");
    let cursor_path = backend.metadata_replication_cursor_path(&target_node_id, &shard_id);
    let metadata_before_stale_ack = fs::metadata(&cursor_path).expect("cursor metadata");
    let modified_before_stale_ack = metadata_before_stale_ack
        .modified()
        .expect("cursor modified time");
    std::thread::sleep(Duration::from_millis(25));
    backend
        .put_metadata_replication_cursor(&target_node_id, &shard_id, 7)
        .expect("stale cursor");
    let metadata_after_stale_ack = fs::metadata(&cursor_path).expect("cursor metadata");
    assert_eq!(
        metadata_after_stale_ack.len(),
        metadata_before_stale_ack.len(),
        "stale acknowledgements should not rewrite the durable replication cursor"
    );
    assert_eq!(
        metadata_after_stale_ack
            .modified()
            .expect("cursor modified time"),
        modified_before_stale_ack,
        "stale acknowledgements should leave the durable replication cursor untouched"
    );
    let reopened =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("reopen");

    assert_eq!(
        reopened
            .metadata_replication_cursor(&target_node_id, &shard_id)
            .expect("reopened cursor"),
        42,
        "older acknowledgements must not move a durable replication cursor backwards"
    );
    cleanup(&root);
}

#[test]
fn metadata_replication_cursor_put_skips_rotated_table_generation_refresh() {
    let root = temp_root("local_backend_metadata_replication_cursor_rotated");
    let target_node_id = node("node_rotated_cursor_target");
    let shard_id = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend
        .put_file_system(&file_system())
        .expect("materialize table log before rotation");
    {
        let _lock = backend.metadata_write_lock().expect("metadata writer lock");
        assert!(
            crate::metadata_table_store::rotate_metadata_table_log_for_checkpoint(&root, 0)
                .expect("rotate table generation")
        );
    }
    assert!(metadata_table_checkpoint_log_path(&root).is_file());
    let durable_generation_before =
        metadata_table_storage_generation(&root).expect("durable table generation");
    let cached_generation_before = backend.table_storage_generation.clone();
    let tables_before = backend.tables.clone();
    let table_log_offset_before = backend.table_log_offset;
    let replay_cost_before = backend.table_log_journal_reference_replay_cost_bytes;
    let unpriced_references_before = backend.table_log_unpriced_namespace_prepare_references;
    let materialized_sequence_before = backend.materialized_through_sequence;
    let prefix_proof_before = backend.prefix_proof.clone();
    let snapshot_fingerprint_before = backend.table_snapshot_fingerprint.clone();

    crate::metadata_read_snapshot::reset_metadata_checkpoint_snapshot_decodes();
    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    backend
        .put_metadata_replication_cursor(&target_node_id, &shard_id, 42)
        .expect("advance cursor without table refresh");
    backend
        .put_metadata_replication_cursor(&target_node_id, &shard_id, 7)
        .expect("ignore stale cursor without table refresh");

    assert_eq!(
        crate::metadata_read_snapshot::metadata_checkpoint_snapshot_decodes(),
        0,
        "cursor sidecar updates must not decode a rotated table checkpoint"
    );
    assert_eq!(
        crate::metadata_read_snapshot::metadata_durable_table_probes(),
        0,
        "cursor sidecar updates must not poll or refresh materialized tables"
    );
    assert_eq!(
        metadata_table_storage_generation(&root).expect("unchanged durable generation"),
        durable_generation_before,
        "cursor sidecars must not replace snapshot, checkpoint, or active table files"
    );
    assert_eq!(backend.table_storage_generation, cached_generation_before);
    assert_eq!(backend.tables, tables_before);
    assert_eq!(backend.table_log_offset, table_log_offset_before);
    assert_eq!(
        backend.table_log_journal_reference_replay_cost_bytes,
        replay_cost_before
    );
    assert_eq!(
        backend.table_log_unpriced_namespace_prepare_references,
        unpriced_references_before
    );
    assert_eq!(
        backend.materialized_through_sequence,
        materialized_sequence_before
    );
    assert_eq!(backend.prefix_proof, prefix_proof_before);
    assert_eq!(
        backend.table_snapshot_fingerprint,
        snapshot_fingerprint_before
    );
    assert_eq!(
        backend
            .metadata_replication_cursor(&target_node_id, &shard_id)
            .expect("monotonic rotated cursor"),
        42
    );

    cleanup(&root);
}

#[test]
fn local_backend_replays_semantically_duplicate_receipt_as_idempotent() {
    let root = temp_root("local_backend_duplicate_receipt_semantic_idempotent");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    let object = object(version("version_1"));
    let receipt = DfsReplicaReceiptRecord {
        receipt: DfsWriteReceipt {
            file_system_id: file_system.file_system_id.clone(),
            object_id: object.object_id.clone(),
            committed_version: object.version.clone(),
            owner_node_id: node("node_owner"),
            event_id: EventId::from_legacy_named("evt_receipt_a").expect("event"),
        },
        replica_node_id: node("node_replica"),
        content_hash: object.content_hash.clone(),
        received_at: OffsetDateTime::UNIX_EPOCH,
    };
    backend.put_receipt(&receipt).expect("receipt");
    let mut replayed = receipt.clone();
    replayed.receipt.event_id = EventId::from_legacy_named("evt_receipt_b").expect("event");
    replayed.received_at = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1);
    let batch = DfsMetadataJournalBatch {
        shard_id: DfsMetadataShardId::new("shard-root"),
        entries: vec![DfsMetadataJournalEntry {
            shard_id: DfsMetadataShardId::new("shard-root"),
            sequence: 2,
            created_at: OffsetDateTime::UNIX_EPOCH,
            mutation: DfsMetadataMutation::PutReceipt {
                file_system_id: file_system.file_system_id,
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                replica_node_id: receipt.replica_node_id.clone(),
                receipt: Some(replayed),
            },
        }],
        next_after_sequence: 2,
    };

    assert_eq!(
        backend
            .apply_journal_batch(&batch)
            .expect("duplicate receipt replay"),
        0
    );
    assert_eq!(
        backend
            .receipts_for_version(&object.object_id, &object.version)
            .expect("receipts"),
        vec![receipt]
    );
    cleanup(&root);
}

#[test]
fn local_backend_rejects_replay_entries_without_record_payloads() {
    let root = temp_root("local_backend_rejects_replay_entries_without_record_payloads");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let entry = DfsMetadataJournalEntry {
        shard_id: DfsMetadataShardId::new("shard-leader"),
        sequence: 7,
        created_at: OffsetDateTime::UNIX_EPOCH,
        mutation: DfsMetadataMutation::PutFileSystem {
            file_system_id: file_system_id(),
            file_system: None,
        },
    };

    let error = backend
        .apply_journal_entry(&entry)
        .expect_err("missing payload should fail replay");
    assert!(
        error
            .to_string()
            .contains("cannot replay put-file-system without its record payload")
    );

    cleanup(&root);
}

#[test]
fn metadata_history_gc_journals_version_removals_and_replica_restart_converges() {
    let root = temp_root("metadata-history-gc-replica-restart");
    let shard_id = DfsMetadataShardId::new("root");
    let mut source =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source")).expect("source");
    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("replica")).expect("replica");
    let old_version = version("version_gc_old");
    let current_version = version("version_gc_current");
    let current = object(current_version.clone());
    let old_record =
        object_version_record(&current, old_version.clone(), OffsetDateTime::UNIX_EPOCH);
    let current_record = object_version_record(
        &current,
        current_version.clone(),
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
    );
    let old_receipt =
        replica_receipt_record(&current, old_version.clone(), OffsetDateTime::UNIX_EPOCH);

    source.put_file_system(&file_system()).expect("filesystem");
    source.put_object(&current).expect("current object");
    source.put_version(&old_record).expect("old version");
    source
        .put_receipt(&old_receipt)
        .expect("old replica receipt");
    source
        .put_version(&current_record)
        .expect("current version");

    let initial = source
        .journal_batch_since_limited(0, 32, usize::MAX)
        .expect("initial journal read")
        .expect("initial journal batch");
    replica.apply_journal_batch(&initial).expect("seed replica");
    let projection = source
        .metadata_history_gc_projection(
            DfsMetadataHistoryGcOptions::dry_run(Duration::ZERO, 1)
                .with_now_unix_nanos(2_000_000_000),
        )
        .expect("GC projection");
    assert_eq!(
        projection.eligible_version_count, 0,
        "a version remains rooted until its dependent replica receipts are durably removed"
    );
    assert_eq!(projection.eligible_receipt_count, 1);
    assert!(projection.shards[0].journal_size_bytes > 0);
    assert_eq!(projection.shards[0].journal_segment_count, 1);
    assert_eq!(
        projection.shards[0].journal_head_sequence,
        source.journal_last_sequence()
    );
    assert_eq!(
        projection.shards[0]
            .tables
            .iter()
            .find(|table| table.table == "versions")
            .map(|table| table.record_count),
        Some(2)
    );
    assert_eq!(
        projection.shards[0]
            .tables
            .iter()
            .find(|table| table.table == "versions")
            .and_then(|table| table.oldest_record_age_nanos),
        Some(2_000_000_000)
    );

    let receipt_report = source
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(2_000_000_000)
                .with_max_removals(2),
        )
        .expect("authoritative replica-receipt GC");
    assert_eq!(receipt_report.removed_version_count, 0);
    assert_eq!(receipt_report.removed_receipt_count, 1);
    assert_eq!(receipt_report.applied_record_count(), 1);
    assert!(
        source
            .get_version(&current.object_id, &old_version)
            .expect("old version remains until its receipt removal is durable")
            .is_some()
    );
    let version_report = source
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(2_000_000_000)
                .with_max_removals(2),
        )
        .expect("authoritative unreferenced-version GC");
    assert_eq!(version_report.removed_version_count, 1);
    assert_eq!(version_report.removed_receipt_count, 0);
    assert_eq!(version_report.applied_record_count(), 1);
    assert_eq!(
        source
            .get_version(&current.object_id, &old_version)
            .expect("source old version"),
        None
    );
    assert!(
        source
            .receipts_for_version(&current.object_id, &old_version)
            .expect("source old receipts")
            .is_empty()
    );

    let gc_batch = source
        .journal_batch_since_limited(initial.next_after_sequence, 32, usize::MAX)
        .expect("GC journal read")
        .expect("GC journal batch");
    assert!(
        gc_batch
            .entries
            .iter()
            .any(|entry| matches!(entry.mutation, DfsMetadataMutation::RemoveVersion { .. }))
    );
    assert!(
        gc_batch
            .entries
            .iter()
            .any(|entry| matches!(entry.mutation, DfsMetadataMutation::RemoveReceipt { .. }))
    );
    replica
        .apply_journal_batch(&gc_batch)
        .expect("replicate GC removals");
    assert_eq!(
        replica
            .get_version(&current.object_id, &old_version)
            .expect("replica old version"),
        None
    );
    assert!(
        replica
            .receipts_for_version(&current.object_id, &old_version)
            .expect("replica old receipts")
            .is_empty()
    );

    let post_gc_projection = source
        .metadata_history_gc_projection(
            DfsMetadataHistoryGcOptions::dry_run(Duration::ZERO, 1)
                .with_now_unix_nanos(2_000_000_001),
        )
        .expect("post-GC projection");
    assert!(
        post_gc_projection.shards[0].snapshot_size_bytes.is_some(),
        "authoritative GC publishes a durable post-GC checkpoint before pruning history"
    );
    drop(source);
    drop(replica);

    let reopened_source = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source"))
        .expect("reopen source");
    let reopened_replica =
        LocalSobjMetadataBackend::new(shard_id, root.join("replica")).expect("reopen replica");
    for backend in [&reopened_source, &reopened_replica] {
        assert_eq!(
            backend
                .get_version(&current.object_id, &old_version)
                .expect("reopened old version"),
            None
        );
        assert_eq!(
            backend
                .get_version(&current.object_id, &current_version)
                .expect("reopened current version"),
            Some(current_record.clone())
        );
    }

    cleanup(&root);
}

#[test]
fn lagging_replica_installs_authoritative_post_gc_snapshot_after_journal_gap() {
    let root = temp_root("metadata-history-gc-lagging-snapshot");
    let shard_id = DfsMetadataShardId::new("root");
    let current_version = version("version_gc_gap_current");
    let current = object(current_version.clone());
    let current_record = object_version_record(
        &current,
        current_version.clone(),
        OffsetDateTime::UNIX_EPOCH + time::Duration::days(1),
    );
    let mut source =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source")).expect("source");
    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("replica")).expect("replica");
    source.put_file_system(&file_system()).expect("filesystem");
    source.put_object(&current).expect("current object");
    source
        .put_version(&current_record)
        .expect("current version");
    let initial = source
        .journal_batch_since_limited(0, 16, usize::MAX)
        .expect("initial journal read")
        .expect("initial journal batch");
    replica
        .apply_journal_batch(&initial)
        .expect("seed soon-to-be-offline replica");

    let historical_count = JOURNAL_SEGMENT_ENTRY_LIMIT * 2 + 8;
    let historical_mutations = (0..historical_count)
        .map(|index| {
            DfsMetadataMutation::put_version(&object_version_record(
                &current,
                version(&format!("version_gc_gap_old_{index:04}")),
                OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(index as i64),
            ))
        })
        .collect::<Vec<_>>();
    {
        let _lock = source.begin_metadata_write().expect("metadata write lock");
        let entries = source
            .write_journal_entries_unlocked(historical_mutations)
            .expect("append offline-replica history");
        source
            .materialize_written_journal_entries_unlocked(&entries)
            .expect("materialize offline-replica history");
    }
    let gc_report = source
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(u64::MAX),
        )
        .expect("sweep history and publish post-GC checkpoint");
    assert_eq!(gc_report.removed_version_count, historical_count);
    let gap_batch = source
        .journal_batch_since_limited(initial.next_after_sequence, 16, usize::MAX)
        .expect("read retained post-checkpoint suffix")
        .expect("retained journal gap witness");
    assert!(
        replica.apply_journal_batch(&gap_batch).is_err(),
        "an offline replica must reject the discontinuous suffix and switch to snapshot install"
    );

    let authoritative_sequence = source.journal_last_sequence();
    replica
        .replace_with_replicated_snapshot(
            shard_id,
            authoritative_sequence,
            source.export_table_snapshot(),
        )
        .expect("install authoritative post-GC snapshot");
    assert_eq!(
        replica
            .get_version(&current.object_id, &current_version)
            .expect("replica current version"),
        Some(current_record)
    );
    assert_eq!(
        replica
            .versions_for_object(&current.object_id)
            .expect("replica bounded versions")
            .len(),
        1
    );
    assert_eq!(
        replica.metadata_prefix_proof().expect("replica prefix"),
        source.metadata_prefix_proof().expect("source prefix")
    );

    cleanup(&root);
}

#[test]
fn metadata_history_gc_does_not_apply_when_journal_append_fails() {
    let root = temp_root("metadata-history-gc-journal-append-failure");
    let shard_id = DfsMetadataShardId::new("root");
    let current = object(version("version_gc_append_current"));
    let old_version = version("version_gc_append_old");
    let old_record =
        object_version_record(&current, old_version.clone(), OffsetDateTime::UNIX_EPOCH);
    let current_record = object_version_record(
        &current,
        current.version.clone(),
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
    );
    let mut backend = LocalSobjMetadataBackend::new(shard_id, &root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend.put_version(&old_record).expect("old version");
    backend
        .put_version(&current_record)
        .expect("current version");
    let journal_head = backend.journal_last_sequence();
    fs::remove_dir_all(root.join("journal").join("segments")).expect("remove journal segments");
    fs::write(root.join("journal").join("segments"), b"not a directory")
        .expect("block journal append");

    assert!(matches!(
        backend.gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(2_000_000_000)
                .with_max_removals(1),
        ),
        Err(MetadataBackendError::Io { .. })
    ));
    assert_eq!(backend.journal_last_sequence(), journal_head);
    assert_eq!(
        backend
            .get_version(&current.object_id, &old_version)
            .expect("old version after append failure"),
        Some(old_record)
    );

    fs::remove_file(root.join("journal").join("segments")).ok();
    cleanup(&root);
}

#[test]
fn metadata_history_gc_replays_durable_removal_after_table_apply_failure() {
    let root = temp_root("metadata-history-gc-table-apply-failure");
    let shard_id = DfsMetadataShardId::new("root");
    let current = object(version("version_gc_table_current"));
    let old_version = version("version_gc_table_old");
    let old_record =
        object_version_record(&current, old_version.clone(), OffsetDateTime::UNIX_EPOCH);
    let current_record = object_version_record(
        &current,
        current.version.clone(),
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
    );
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend.put_version(&old_record).expect("old version");
    backend
        .put_version(&current_record)
        .expect("current version");
    backend
        .compact_metadata_table_log_if_needed(0)
        .expect("checkpoint pre-GC state");
    let table_path = metadata_table_path(&root);
    let error = {
        let _lock = backend.begin_metadata_write().expect("metadata write lock");
        let entry = backend
            .write_journal_unlocked(DfsMetadataMutation::remove_version(
                current.file_system_id.clone(),
                current.object_id.clone(),
                old_version.clone(),
            ))
            .expect("durable GC removal append");
        let planned = backend
            .plan_written_journal_entries_table_ops(std::slice::from_ref(&entry))
            .expect("preapply GC removal");
        fs::remove_file(&table_path).expect("remove empty materialized table log");
        fs::create_dir(&table_path).expect("block table apply");
        backend
            .append_preapplied_table_ops_for_written_entries(
                std::slice::from_ref(&entry),
                planned.last_materialized_sequence,
                planned.table_ops,
                planned.rollback,
            )
            .expect_err("table apply fails after durable GC removal")
    };
    assert!(matches!(error, MetadataBackendError::Io { .. }));
    assert_eq!(
        backend
            .tables
            .versions
            .get(&(current.object_id.clone(), old_version.clone())),
        Some(&old_record),
        "failed table materialization must roll the in-memory sweep back"
    );
    assert!(
        backend
            .journal_since(0)
            .expect("durable journal")
            .iter()
            .any(|entry| matches!(entry.mutation, DfsMetadataMutation::RemoveVersion { .. }))
    );

    fs::remove_dir(&table_path).expect("restore materialized table log path");
    drop(backend);
    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("replay durable GC tail");
    assert_eq!(
        reopened
            .get_version(&current.object_id, &old_version)
            .expect("replayed old version"),
        None
    );
    assert_eq!(
        reopened
            .get_version(&current.object_id, &current.version)
            .expect("replayed current version"),
        Some(current_record)
    );

    cleanup(&root);
}

#[test]
fn metadata_history_gc_survives_snapshot_publication_failure() {
    let root = temp_root("metadata-history-gc-snapshot-publication-failure");
    let shard_id = DfsMetadataShardId::new("root");
    let current = object(version("version_gc_snapshot_current"));
    let old_version = version("version_gc_snapshot_old");
    let old_record =
        object_version_record(&current, old_version.clone(), OffsetDateTime::UNIX_EPOCH);
    let current_record = object_version_record(
        &current,
        current.version.clone(),
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
    );
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend.put_version(&old_record).expect("old version");
    backend
        .put_version(&current_record)
        .expect("current version");
    let snapshot_path = metadata_table_snapshot_path(&root);
    if snapshot_path.is_file() {
        fs::remove_file(&snapshot_path).expect("remove prior snapshot");
    }
    fs::create_dir(&snapshot_path).expect("block snapshot publication");

    assert!(matches!(
        backend.gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(2_000_000_000)
                .with_max_removals(1),
        ),
        Err(MetadataBackendError::Io { .. })
    ));
    assert_eq!(
        backend
            .get_version(&current.object_id, &old_version)
            .expect("materialized old version"),
        None,
        "the journaled removal remains materialized even when checkpoint publication fails"
    );

    fs::remove_dir(&snapshot_path).expect("restore snapshot path");
    drop(backend);
    let reopened =
        LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen after snapshot failure");
    assert_eq!(
        reopened
            .get_version(&current.object_id, &old_version)
            .expect("reopened old version"),
        None
    );
    assert_eq!(
        reopened
            .get_version(&current.object_id, &current.version)
            .expect("reopened current version"),
        Some(current_record)
    );

    cleanup(&root);
}

#[test]
fn metadata_history_gc_recovers_from_durable_snapshot_after_journal_prune_failure() {
    let root = temp_root("metadata-history-gc-journal-prune-failure");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system();
    let current = object(version("version_gc_prune_current"));
    let old_version = version("version_gc_prune_old");
    let old_record =
        object_version_record(&current, old_version.clone(), OffsetDateTime::UNIX_EPOCH);
    let current_record = object_version_record(
        &current,
        current.version.clone(),
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
    );
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend.put_version(&old_record).expect("old version");
    backend
        .put_version(&current_record)
        .expect("current version");

    // Advance into segment two without growing the materialized tables.
    // The GC removal will checkpoint in segment two and attempt to prune
    // segment one only after publishing the authoritative snapshot.
    let filler = (0..JOURNAL_SEGMENT_ENTRY_LIMIT)
        .map(|_| DfsMetadataMutation::put_file_system(&file_system))
        .collect::<Vec<_>>();
    {
        let _lock = backend.begin_metadata_write().expect("metadata write lock");
        let entries = backend
            .write_journal_entries_unlocked(filler)
            .expect("append checkpoint-boundary filler");
        backend
            .materialize_written_journal_entries_unlocked(&entries)
            .expect("materialize checkpoint-boundary filler");
    }
    let segments = root.join("journal").join("segments");
    let first_segment = segments.join(format!("{:020}.slog", 1));
    assert!(first_segment.is_file());

    // A directory at the segment path makes deletion fail after snapshot
    // publication. The original segment is intentionally discarded: the
    // test proves that the durable post-GC snapshot is sufficient recovery
    // state even when pruning reports an error.
    fs::remove_file(&first_segment).expect("remove first segment");
    fs::create_dir(&first_segment).expect("block first-segment pruning");
    let error = backend
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(2_000_000_000)
                .with_max_removals(1),
        )
        .expect_err("journal pruning must report the injected failure");
    assert!(matches!(
        error,
        MetadataBackendError::Io { ref path, .. } if path == &first_segment
    ));
    assert_eq!(
        backend
            .get_version(&current.object_id, &old_version)
            .expect("materialized old version"),
        None,
        "the journaled GC removal is already materialized at the prune boundary"
    );
    assert!(
        metadata_table_snapshot_path(&root).is_file(),
        "the authoritative post-GC snapshot must precede pruning"
    );

    fs::remove_dir(&first_segment).expect("restore segment path");
    drop(backend);
    let reopened =
        LocalSobjMetadataBackend::new(shard_id, &root).expect("recover from post-GC snapshot");
    assert_eq!(
        reopened
            .get_version(&current.object_id, &old_version)
            .expect("reopened old version"),
        None
    );
    assert_eq!(
        reopened
            .get_version(&current.object_id, &current.version)
            .expect("reopened current version"),
        Some(current_record)
    );
    assert_eq!(
        reopened
            .versions_for_object(&current.object_id)
            .expect("bounded reopened versions")
            .len(),
        1
    );

    cleanup(&root);
}

#[test]
fn grouped_replication_cursors_are_durable_and_never_rewind() {
    let root = temp_root("grouped-replication-cursors");
    let shard = DfsMetadataShardId::new("root");
    let a = NodeId::from_legacy_named("cursor-a").unwrap();
    let b = NodeId::from_legacy_named("cursor-b").unwrap();
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), &root).unwrap();
    backend
        .put_metadata_replication_cursor(&a, &shard, 40)
        .unwrap();
    backend
        .put_metadata_replication_cursors(
            &shard,
            &[(a.clone(), 20), (b.clone(), 70), (b.clone(), 60)],
        )
        .unwrap();
    drop(backend);
    let backend = LocalSobjMetadataBackend::new(shard.clone(), &root).unwrap();
    assert_eq!(backend.metadata_replication_cursor(&a, &shard).unwrap(), 40);
    assert_eq!(backend.metadata_replication_cursor(&b, &shard).unwrap(), 70);
    assert_eq!(
        backend.journal_last_sequence(),
        0,
        "hints do not append authoritative history"
    );
}
