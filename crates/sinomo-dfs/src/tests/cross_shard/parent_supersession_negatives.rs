//! Adversarial DATA validation derived from an actual native held preparation.
//! Hypothetical newer-epoch bodies below are never submitted or used as proof
//! of retirement. Native certified multi-handoff publication is tested separately.
use super::*;
use crate::metadata_tables::DfsMetadataTables;
use std::collections::BTreeSet;
use std::sync::Arc;

type Record = crate::DfsPreparedOperationRecord;
type Payload = crate::DfsPreparedOperationPayload;
type Effect = crate::DfsPreparedMetadataEffect;
type Status = crate::DfsPreparedOperationStatus;

fn descendant(
    original: &Record,
    participant: &crate::DfsParentCountParticipant,
    epoch: u64,
    sequence: u64,
) -> Record {
    let mut payload = (*original.payload).clone();
    payload.operation_id =
        participant.operation_id_at_epoch(&crate::DfsParentCountAction::Apply, epoch);
    payload.owner_epoch = epoch;
    payload
        .metadata_effects
        .push(Effect::ResolvePreparedOperation {
            digest: original.digest.clone(),
            status: Status::Superseded,
        });
    record(payload, sequence)
}

fn record(payload: Payload, sequence: u64) -> Record {
    Record {
        digest: crate::DfsPreparedOperationDigest::at_sequence(&payload, sequence).unwrap(),
        payload: Arc::new(payload),
        status: Status::Prepared,
        committed_entry: None,
    }
}

fn rejects(tables: &DfsMetadataTables, payload: &Payload, label: &str) {
    let before = tables.clone();
    assert!(
        tables.parent_supersession_chain(payload).is_err(),
        "{label}"
    );
    assert_eq!(
        *tables, before,
        "DATA rejection cannot shadow or mutate any retained row: {label}"
    );
}

#[test]
fn native_parent_supersession_rejects_missing_foreign_forked_and_changed_ancestry() {
    let mut cluster = super::parent_counts::foreign_parent_cluster("native-parent-chain-negatives");
    let root =
        crate::default_dfs_metadata_shards_root(cluster._runtime.data_dir(&cluster.replica_node))
            .join("root");
    let hold = crate::posix::hold_parent_apply_commit(&root, None);
    let request = sinomo_core::Id::generate().to_string();
    let operation = DfsNamespaceOperation::Rename {
        from: "/users".into(),
        to: "/services/chain-negative-users".into(),
    };
    cluster.replica.single_attempt_for_test(true);
    assert!(
        cluster
            .replica
            .route_namespace_operation_with_request_id(
                request.clone(),
                cluster.users_primary_node.clone(),
                operation.clone()
            )
            .is_err()
    );
    let coordinator =
        hardlink_read_set_snapshot(&cluster._runtime, &cluster.users_primary_node, "users")
            .unwrap();
    let job = coordinator
        .tables
        .namespace_jobs
        .iter()
        .find(|job| {
            job.kind == DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                && job
                    .origin
                    .as_ref()
                    .is_some_and(|origin| origin.request_id == request)
        })
        .unwrap();
    let participant = job
        .directory_move_counts()
        .unwrap()
        .unwrap()
        .parent_participants
        .as_ref()
        .unwrap()
        .participants[0]
        .clone();
    let original_id = participant.operation_id(&crate::DfsParentCountAction::Apply);
    let before =
        hardlink_read_set_snapshot(&cluster._runtime, &cluster.replica_node, "root").unwrap();
    let original = before
        .tables
        .prepared_operations_v7
        .iter()
        .find(|row| row.operation_id == original_id)
        .unwrap()
        .clone();
    assert_eq!(original.status, Status::Prepared);
    assert!(original.committed_entry.is_none());
    assert!(
        before
            .tables
            .objects
            .iter()
            .any(|row| row == &participant.expected)
    );
    {
        let mut actual_owner = cluster._runtime.metadata_primary_service("/").unwrap();
        let shard = DfsMetadataShardId::new("root");
        let prefix_before = actual_owner
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&shard)
            .unwrap();
        let mut changed_same_attempt = (*original.payload).clone();
        changed_same_attempt.transaction.created_at += time::Duration::nanoseconds(1);
        assert!(
            actual_owner
                .publish_metadata_effect_payload(changed_same_attempt)
                .is_err(),
            "an actual current owner cannot reauthor an existing native attempt"
        );
        assert_eq!(
            actual_owner
                .metadata_backend()
                .unwrap()
                .metadata_prefix_proof(&shard)
                .unwrap(),
            prefix_before,
            "immutable-attempt refusal precedes every journal append"
        );
        assert_eq!(
            actual_owner
                .metadata_backend()
                .unwrap()
                .prepared_operation_record(&original_id)
                .unwrap(),
            Some(original.clone())
        );
    }
    let mut tables =
        crate::metadata_table_store::metadata_tables_from_snapshot(*before.tables).unwrap();

    // Pure structural controls only: these bodies are not retired-authority
    // evidence, are not appended, and cannot grant a publication guard.
    let middle = descendant(
        &original,
        &participant,
        original.owner_epoch + 1,
        original.digest.prepare_sequence + 10,
    );
    let head = descendant(
        &middle,
        &participant,
        original.owner_epoch + 2,
        original.digest.prepare_sequence + 20,
    );
    tables
        .prepared_operations_v7
        .insert(middle.operation_id.clone(), middle.clone());
    assert_eq!(
        tables.parent_supersession_chain(&head.payload).unwrap(),
        Some(BTreeSet::from([
            original.operation_id.clone(),
            middle.operation_id.clone()
        ]))
    );

    for absent in [&original.operation_id, &middle.operation_id] {
        let mut missing = tables.clone();
        missing.prepared_operations_v7.remove(absent);
        rejects(
            &missing,
            &head.payload,
            "every exact ancestor body must be present",
        );
    }
    let mut wrong_digest = (*head.payload).clone();
    if let Effect::ResolvePreparedOperation { digest, .. } =
        wrong_digest.metadata_effects.last_mut().unwrap()
    {
        digest.payload_digest.push('0');
    }
    rejects(
        &tables,
        &wrong_digest,
        "the direct edge must match its stored digest exactly",
    );
    let mut omitted = (*head.payload).clone();
    omitted.metadata_effects.retain(|effect| !matches!(effect, Effect::ResolvePreparedOperation { digest, .. } if digest.operation_id == original.operation_id));
    rejects(
        &tables,
        &omitted,
        "a head cannot discard an inherited dependency",
    );
    let mut duplicate = (*head.payload).clone();
    duplicate
        .metadata_effects
        .push(duplicate.metadata_effects.last().unwrap().clone());
    rejects(
        &tables,
        &duplicate,
        "a repeated direct edge is not one removable edge",
    );
    let mut wrong_decision = (*head.payload).clone();
    if let Effect::ResolvePreparedOperation { status, .. } =
        wrong_decision.metadata_effects.last_mut().unwrap()
    {
        *status = Status::Aborted;
    }
    rejects(
        &tables,
        &wrong_decision,
        "supersession cannot be replaced by an abort decision",
    );

    let mut changed_ancestor = tables.clone();
    let old = changed_ancestor
        .prepared_operations_v7
        .get_mut(&original.operation_id)
        .unwrap();
    Arc::make_mut(&mut old.payload).transaction.created_at += time::Duration::nanoseconds(1);
    rejects(
        &changed_ancestor,
        &head.payload,
        "changed stored body cannot retain its prior digest",
    );
    let mut terminal = tables.clone();
    terminal
        .prepared_operations_v7
        .get_mut(&original.operation_id)
        .unwrap()
        .status = Status::Superseded;
    rejects(
        &terminal,
        &head.payload,
        "already resolved ancestry cannot be silently reused",
    );
    let mut committed = tables.clone();
    let terminal_record = committed
        .prepared_operations_v7
        .get_mut(&original.operation_id)
        .unwrap();
    terminal_record.status = Status::Committed;
    terminal_record.committed_entry = Some(Box::new(crate::DfsPreparedOperationCommitReference {
        shard_id: terminal_record.shard_id.clone(),
        sequence: terminal_record.digest.prepare_sequence + 1,
        created_at: terminal_record.transaction.created_at,
        digest: terminal_record.digest.clone(),
    }));
    terminal_record.validate().unwrap();
    rejects(
        &committed,
        &head.payload,
        "even well-shaped committed ancestor DATA cannot be treated as no-commit evidence",
    );

    let mut foreign = participant.clone();
    foreign.binding.job_id.push_str("-foreign-job");
    foreign.participant_id = foreign.identity().unwrap();
    foreign.validate().unwrap();
    let mut foreign_payload = (*original.payload).clone();
    foreign_payload.operation_id =
        foreign.operation_id_at_epoch(&crate::DfsParentCountAction::Apply, original.owner_epoch);
    foreign_payload.metadata_effects = vec![Effect::PutNamespaceJob {
        job: foreign.job(crate::DfsParentCountState::Applied),
    }];
    let foreign_record = record(foreign_payload, original.digest.prepare_sequence + 30);
    foreign_record.validate().unwrap();
    let mut foreign_tables = tables.clone();
    foreign_tables
        .prepared_operations_v7
        .insert(foreign_record.operation_id.clone(), foreign_record.clone());
    let mut foreign_edge = (*head.payload).clone();
    foreign_edge
        .metadata_effects
        .push(Effect::ResolvePreparedOperation {
            digest: foreign_record.digest,
            status: Status::Superseded,
        });
    rejects(
        &foreign_tables,
        &foreign_edge,
        "identical parent rows do not authorize another participant's ancestry",
    );

    let mut wrong_scope = (*head.payload).clone();
    wrong_scope.shard_id = DfsMetadataShardId::new("foreign-parent");
    rejects(&tables, &wrong_scope, "head physical scope cannot change");
    let mut wrong_epoch = (*head.payload).clone();
    wrong_epoch.owner_epoch = middle.owner_epoch;
    wrong_epoch.operation_id = participant
        .operation_id_at_epoch(&crate::DfsParentCountAction::Apply, wrong_epoch.owner_epoch);
    rejects(
        &tables,
        &wrong_epoch,
        "every edge must go to a strictly earlier epoch",
    );
    let mut wrong_phase = (*head.payload).clone();
    wrong_phase.operation_id =
        participant.operation_id_at_epoch(&crate::DfsParentCountAction::Reserve, head.owner_epoch);
    rejects(
        &tables,
        &wrong_phase,
        "native operation identity is bound to the same parent phase",
    );
    let mut changed_successor = (*head.payload).clone();
    changed_successor.transaction.objects[0].namespace_link_count = Some(999);
    rejects(
        &tables,
        &changed_successor,
        "count successor is the original absolute row, not a recomputed delta",
    );
    let mut changed_read = (*head.payload).clone();
    changed_read.expected_object_versions[0].expected_version = None;
    rejects(
        &tables,
        &changed_read,
        "original object predicates are immutable",
    );
    let mut changed_path = (*head.payload).clone();
    changed_path.expected_path_occupants[0].expected_object_id = None;
    rejects(
        &tables,
        &changed_path,
        "original path predicates are immutable",
    );
    let mut changed_time = (*head.payload).clone();
    changed_time.transaction.created_at += time::Duration::nanoseconds(1);
    rejects(
        &tables,
        &changed_time,
        "original transaction time cannot be replanned",
    );
    let mut changed_native_causality = (*head.payload).clone();
    changed_native_causality.journal_operation_id = Some("another-caller".into());
    rejects(
        &tables,
        &changed_native_causality,
        "original journal/request causality cannot be replaced",
    );
    let mut changed_transaction = (*head.payload).clone();
    changed_transaction
        .transaction
        .transaction_id
        .push_str("-new");
    rejects(
        &tables,
        &changed_transaction,
        "logical transaction identity is preserved across native attempts",
    );
    let mut changed_cut = (*head.payload).clone();
    changed_cut.expected_journal_sequence = Some(123);
    rejects(
        &tables,
        &changed_cut,
        "the captured journal predicate cannot be rebound to a newer view",
    );
    let mut changed_job_request = (*head.payload).clone();
    let job = changed_job_request
        .metadata_effects
        .iter_mut()
        .find_map(|effect| match effect {
            Effect::PutNamespaceJob { job } => Some(job),
            _ => None,
        })
        .unwrap();
    job.origin
        .as_mut()
        .expect("genuine routed caller")
        .request_id
        .push_str("-foreign");
    rejects(
        &tables,
        &changed_job_request,
        "original caller identity remains sealed in the parent job",
    );
    let mut fork = (*head.payload).clone();
    let sibling = descendant(
        &original,
        &participant,
        head.owner_epoch + 1,
        head.digest.prepare_sequence + 10,
    );
    let mut fork_tables = tables.clone();
    fork_tables
        .prepared_operations_v7
        .insert(sibling.operation_id.clone(), sibling.clone());
    fork.owner_epoch = sibling.owner_epoch + 1;
    fork.operation_id =
        participant.operation_id_at_epoch(&crate::DfsParentCountAction::Apply, fork.owner_epoch);
    fork.metadata_effects
        .push(Effect::ResolvePreparedOperation {
            digest: sibling.digest.clone(),
            status: Status::Superseded,
        });
    rejects(
        &fork_tables,
        &fork,
        "two branch tips cannot be collapsed into an invented linear chain",
    );

    // Inspect the genuine owner: all hypothetical DATA stayed outside it.
    let actual =
        hardlink_read_set_snapshot(&cluster._runtime, &cluster.replica_node, "root").unwrap();
    assert_eq!(
        actual
            .tables
            .prepared_operations_v7
            .iter()
            .find(|row| row.operation_id == original_id),
        Some(&original)
    );
    assert!(
        actual
            .tables
            .objects
            .iter()
            .any(|row| row == &participant.expected)
    );
    assert!(
        !actual
            .tables
            .prepared_operations_v7
            .iter()
            .any(|row| row.operation_id == head.operation_id
                || row.operation_id == middle.operation_id)
    );
    drop(hold);
    cluster.replica.single_attempt_for_test(false);
    cluster
        .replica
        .route_namespace_operation_with_request_id(
            request,
            cluster.users_primary_node.clone(),
            operation,
        )
        .unwrap();
    let completed =
        hardlink_read_set_snapshot(&cluster._runtime, &cluster.replica_node, "root").unwrap();
    assert!(
        completed
            .tables
            .objects
            .iter()
            .any(|row| row == &participant.successor)
    );
    cluster.close();
}
