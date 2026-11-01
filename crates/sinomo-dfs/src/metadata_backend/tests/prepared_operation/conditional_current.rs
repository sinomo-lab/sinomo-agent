//! Native producer/prefix controls for first conditional publication. Altered
//! bodies below are negative DATA inputs; they grant no writer or authority.
use super::*;

fn apply_entry(tables: &mut DfsMetadataTables, entry: &DfsMetadataJournalEntry) {
    let operation = match &entry.mutation {
        DfsMetadataMutation::PrepareOperationV7 { record, .. } => {
            DfsMetadataTableOp::PutPreparedOperationV7 {
                record: record.clone(),
            }
        }
        DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } => {
            DfsMetadataTableOp::CommitPreparedOperationV7 {
                digest: digest.clone(),
                committed_entry: Box::new(
                    crate::DfsPreparedOperationCommitReference::from_journal_entry(entry).unwrap(),
                ),
            }
        }
    };
    tables.apply_checked(operation).unwrap();
}

fn prepared(
    mut payload: crate::DfsPreparedOperationPayload,
    sequence: u64,
) -> crate::DfsPreparedOperationRecord {
    payload.normalize();
    crate::DfsPreparedOperationRecord {
        digest: crate::DfsPreparedOperationDigest::at_sequence(&payload, sequence).unwrap(),
        payload: Arc::new(payload),
        status: crate::DfsPreparedOperationStatus::Prepared,
        committed_entry: None,
    }
}

#[test]
fn native_conditional_kernel_seals_original_guard_and_preserves_committed_replay() {
    let (runtime, mut service, config) = native_case("native-conditional-kernel", 3);
    let parent_path = "/services/native-conditional-kernel";
    directory(&mut service, parent_path);
    let opened = service.posix_stat_distributed(parent_path).unwrap();
    let captured = Arc::new(std::sync::Mutex::new(None));
    let observer = Arc::clone(&captured);
    crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |payload| {
            *observer.lock().unwrap() = Some(payload.clone());
        }));
    });
    let candidates = std::iter::once(config.primary_node_id.clone())
        .chain(config.replica_node_ids.iter().cloned())
        .collect();
    let created = service
        .posix_create_file_at_opened_directory_no_links(
            &opened,
            "note.md",
            b"actual conditional native publication",
            crate::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 3).unwrap(),
                replica_candidates: candidates,
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let mut payload = captured
        .lock()
        .unwrap()
        .take()
        .expect("passively observed actual native conditional producer");
    payload.normalize();
    let condition = payload
        .prepared_intent
        .as_ref()
        .unwrap()
        .publication
        .current_precondition()
        .unwrap();
    assert!(
        matches!(condition, crate::DfsCurrentWritePrecondition::ParentOnly { parent, .. }
        if parent.object_id == opened.object_id && parent.version == opened.version)
    );
    let shard = DfsMetadataShardId::new(&config.shard_id);
    let actual = entries(&service, &shard);
    let original = actual
        .iter()
        .find_map(|entry| match &entry.mutation {
            DfsMetadataMutation::PrepareOperationV7 { record, .. }
                if record.operation_id == payload.operation_id =>
            {
                Some(record.as_ref().clone())
            }
            _ => None,
        })
        .expect("actual durable preparation for the captured body");
    assert_eq!(original.payload.as_ref(), &payload);
    let mut before = DfsMetadataTables::default();
    for entry in actual
        .iter()
        .take_while(|entry| entry.sequence < original.digest.prepare_sequence)
    {
        apply_entry(&mut before, entry);
    }
    let validate = |tables: &DfsMetadataTables, record: &crate::DfsPreparedOperationRecord| {
        let mut copy = tables.clone();
        let result =
            crate::metadata_backend::prepared_operation::validate_preparation_against_tables(
                &mut copy, &shard, record,
            );
        assert_eq!(
            &copy, tables,
            "validation rolls back its actual provisional rows on every result"
        );
        result
    };
    validate(&before, &original).unwrap();
    let mut ready = before.clone();
    ready
        .apply_checked(DfsMetadataTableOp::PutPreparedOperationV7 {
            record: Box::new(original.clone()),
        })
        .unwrap();
    crate::metadata_backend::prepared_operation::validate_prepared_operation_commit(
        &ready, &original,
    )
    .unwrap();

    let mut missing = payload.clone();
    missing
        .expected_object_versions
        .retain(|row| row.object_id != opened.object_id);
    missing.expected_path_occupants.retain(|row| {
        crate::namespace::metadata_path(&row.path) != crate::namespace::metadata_path(parent_path)
    });
    let missing = prepared(missing, original.digest.prepare_sequence);
    assert!(
        matches!(crate::metadata_backend::prepared_operation::validate_prepared_operation_content(&ready, &missing),
        Err(MetadataBackendError::Serde(message)) if message.contains("original predicate"))
    );

    // An actual later native mutation changes the old predicate. The raw
    // first-publication validator must report its typed cause before generic
    // namespace read-set conflict. The ordinary exact committed replay below
    // must never enter that first-publication check again.
    let changed = service.posix_set_mode(parent_path, 0o750).unwrap();
    assert_ne!(changed.version, opened.version);
    let mut later = DfsMetadataTables::default();
    for entry in entries(&service, &shard) {
        apply_entry(&mut later, &entry);
    }
    assert!(
        matches!(crate::metadata_backend::prepared_operation::validate_prepared_operation_commit(&later, &original),
        Err(MetadataBackendError::WritePreconditionFailed { failure: crate::DfsWritePreconditionFailure::Directory { path, actual: Some(actual), .. } })
            if path == parent_path && actual.object_id == changed.object_id && actual.version == changed.version)
    );
    let sequence = entries(&service, &shard).last().unwrap().sequence;
    let replay = commit_kernel(&mut service, &original.digest).unwrap();
    assert!(!replay.published);
    assert_eq!(entries(&service, &shard).last().unwrap().sequence, sequence);
    assert_eq!(
        service.posix_stat_distributed(parent_path).unwrap().version,
        changed.version
    );
    assert_eq!(
        service
            .posix_stat_distributed(&created.path)
            .unwrap()
            .version,
        created.version
    );

    let active = before
        .intent_record(
            &created.object_id,
            &payload.prepared_intent.as_ref().unwrap().intent_id,
        )
        .expect("conditional producer durably staged its original before final prepare");
    assert!(matches!(
        active.status,
        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
    ));
    let failure = crate::DfsWritePreconditionFailure::Directory {
        path: parent_path.to_string(),
        expected: crate::DfsNamespaceObjectPrecondition {
            object_id: opened.object_id.clone(),
            version: opened.version.clone(),
        },
        actual: None,
        actual_kind: None,
    };
    let mut terminal = active.clone();
    terminal.status = DfsWriteIntentStatus::PreconditionFailed {
        failure: failure.clone(),
    };
    terminal.updated_at = Some(OffsetDateTime::now_utc());
    let mut objects = vec![DfsNamespaceObjectVersionExpectation {
        object_id: active.object_id.clone(),
        expected_version: before
            .objects
            .get(&active.object_id)
            .map(|object| object.version.clone()),
    }];
    let mut paths = Vec::new();
    crate::write_precondition::bind_failure(&failure, &mut objects, &mut paths).unwrap();
    let negative = crate::DfsPreparedOperationPayload::for_metadata_effects(
        crate::DfsPreparedOperationScope {
            file_system_id: active.file_system_id.clone(),
            shard_id: shard.clone(),
            owner_epoch: config.epoch,
            operation_id: format!("precondition-failed:{}", active.intent_id),
        },
        objects,
        paths,
        vec![crate::DfsPreparedMetadataEffect::PutIntent { intent: terminal }],
        OffsetDateTime::now_utc(),
    )
    .unwrap();
    let negative = prepared(negative, original.digest.prepare_sequence);
    // A destination's real prefix before the original staging publication has
    // no local terminal. Ordinary metadata-copy spelling does not grant an
    // author permission to invent failed history without native source custody.
    let first_intent = actual.iter().find(|entry| match &entry.mutation {
        DfsMetadataMutation::PrepareOperationV7 { record, .. } => {
            record.prepared_intent.as_ref().is_some_and(|intent| intent.intent_id == active.intent_id)
                || record.metadata_effects.iter().any(|effect| matches!(effect,
                    crate::DfsPreparedMetadataEffect::PutIntent { intent } if intent.intent_id == active.intent_id))
        }
        _ => false,
    }).unwrap().sequence;
    let mut no_original = DfsMetadataTables::default();
    for entry in actual
        .iter()
        .take_while(|entry| entry.sequence < first_intent)
    {
        apply_entry(&mut no_original, entry);
    }
    assert!(
        no_original
            .intent_record(&active.object_id, &active.intent_id)
            .is_none()
    );
    let mut source_less_copy = negative.payload.as_ref().clone();
    source_less_copy.operation_id = "native-source-less-negative-copy".into();
    source_less_copy.transaction.transaction_id =
        "metadata:native-source-less-negative-copy".into();
    let source_less_copy = prepared(source_less_copy, first_intent);
    assert!(
        matches!(crate::metadata_backend::prepared_operation::validate_metadata_effect_content_bindings(
        &no_original, &source_less_copy, None), Err(MetadataBackendError::Serde(message))
        if message.contains("native source custody")),
        "ordinary effect IDs never bypass the actual author source boundary"
    );
    assert!(
        matches!(validate(&before, &negative), Err(MetadataBackendError::Serde(message))
        if message.contains("actual native failure observation")),
        "a well-typed invented failure cannot mint a no-effect outcome"
    );
    assert!(
        matches!(validate(&ready, &negative), Err(MetadataBackendError::Serde(message))
        if message.contains("original content preparation owns reconciliation")),
        "an actual prepared write is not an absent effect"
    );
    let committed = later
        .prepared_operations_v7
        .get(&original.operation_id)
        .unwrap();
    let eviction = crate::DfsPreparedOperationEviction {
        digest: committed.digest.clone(),
        status: committed.status,
        committed_entry: committed.committed_entry.clone(),
    };
    eviction.validate().unwrap();
    let mut evicted = before.clone();
    evicted
        .prepared_operation_evictions
        .insert(original.operation_id.clone(), eviction);
    assert!(
        matches!(validate(&evicted, &negative), Err(MetadataBackendError::Serde(message))
        if message.contains("original content preparation owns reconciliation")),
        "actual evicted terminal evidence cannot be reinterpreted as no effect"
    );
    drop(service);
    runtime.close().unwrap();
}
