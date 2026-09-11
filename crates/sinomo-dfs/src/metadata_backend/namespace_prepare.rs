//! Namespace prepare for the local metadata backend.

use super::*;

#[cfg(test)]
pub(super) fn namespace_prepared_commit_reference_planner_calls() -> usize {
    NAMESPACE_PREPARED_COMMIT_REFERENCE_PLANNER_CALLS.get()
}

#[cfg(test)]
pub(super) fn namespace_prepare_history_scan_calls() -> usize {
    NAMESPACE_PREPARE_HISTORY_SCAN_CALLS.get()
}

pub(super) fn namespace_prepare_payload_matches(
    left: &DfsNamespacePrepareRecord,
    right: &DfsNamespacePrepareRecord,
) -> bool {
    left.operation_id == right.operation_id
        && left.file_system_id == right.file_system_id
        && left.shard_id == right.shard_id
        && left.owner_epoch == right.owner_epoch
        && left.operation_digest == right.operation_digest
        && left.expected_object_versions == right.expected_object_versions
        && left.expected_path_occupants == right.expected_path_occupants
        && left.content_receipts == right.content_receipts
        && left.transaction == right.transaction
        && left.completed_request == right.completed_request
        && left.completed_request_completion == right.completed_request_completion
}

pub(super) fn namespace_prepare_rebind_payload_matches(
    left: &DfsNamespacePrepareRecord,
    right: &DfsNamespacePrepareRecord,
) -> bool {
    left.operation_id == right.operation_id
        && left.file_system_id == right.file_system_id
        && left.shard_id == right.shard_id
        && left.expected_object_versions == right.expected_object_versions
        && left.expected_path_occupants == right.expected_path_occupants
        && left.content_receipts == right.content_receipts
        && left.transaction == right.transaction
        && left.completed_request == right.completed_request
        && left.completed_request_completion == right.completed_request_completion
        && left.status == DfsNamespacePrepareStatus::Prepared
        && right.status == DfsNamespacePrepareStatus::Prepared
}

pub(super) fn namespace_request_immutable_payload_matches(
    left: &DfsNamespaceRequestRecord,
    right: &DfsNamespaceRequestRecord,
) -> bool {
    left.file_system_id == right.file_system_id
        && left.requester_node_id == right.requester_node_id
        && left.request_id == right.request_id
        && left.operation_id == right.operation_id
        && left.authority_shard_id == right.authority_shard_id
        && left.operation == right.operation
        && left.command_digest == right.command_digest
        && left.created_at == right.created_at
}

pub(super) fn completed_namespace_request_outcome_matches(
    left: &DfsNamespaceRequestRecord,
    right: &DfsNamespaceRequestRecord,
) -> bool {
    namespace_request_immutable_payload_matches(left, right)
        && left.status == DfsNamespaceRequestStatus::Completed
        && right.status == DfsNamespaceRequestStatus::Completed
        && left.result == right.result
        && left.posix_coordination_result == right.posix_coordination_result
}

pub(super) fn validate_namespace_request_shape(request: &DfsNamespaceRequestRecord) -> Result<()> {
    if request.request_id.trim().is_empty()
        || request.operation_id.trim().is_empty()
        || request.command_digest.trim().is_empty()
        || request.authority_shard_id.0.trim().is_empty()
    {
        return Err(MetadataBackendError::Serde(
            "namespace request requires non-empty request, operation, digest, and authority shard ids"
                .to_string(),
        ));
    }
    let pending_shape = request.status == DfsNamespaceRequestStatus::Pending
        && request.result.is_none()
        && request.posix_coordination_result.is_none();
    let completed_shape = request.status == DfsNamespaceRequestStatus::Completed
        && request.result.is_some()
        && request.last_recovery_error.is_none();
    if !pending_shape && !completed_shape {
        return Err(MetadataBackendError::Serde(format!(
            "namespace request {}/{} has an invalid {:?} payload",
            request.requester_node_id, request.request_id, request.status
        )));
    }
    Ok(())
}

pub(super) fn validate_namespace_request_completion_shape(
    completion: &DfsNamespaceRequestCompletion,
) -> Result<()> {
    if completion.request_id.trim().is_empty()
        || completion.operation_id.trim().is_empty()
        || completion.command_digest.trim().is_empty()
        || completion.authority_shard_id.0.trim().is_empty()
        || completion.transaction_id.trim().is_empty()
    {
        return Err(MetadataBackendError::Serde(
            "compact namespace request completion requires non-empty request, operation, digest, authority shard, and transaction ids"
                .to_string(),
        ));
    }
    if completion.result != DfsNamespaceResult::Applied
        || completion.posix_coordination_result.is_some()
        || completion.completed_at < completion.created_at
    {
        return Err(MetadataBackendError::Serde(format!(
            "compact namespace request completion {}/{} has an invalid terminal payload",
            completion.requester_node_id, completion.request_id
        )));
    }
    Ok(())
}

pub(super) fn namespace_request_identity_matches_completion(
    request: &DfsNamespaceRequestRecord,
    completion: &DfsNamespaceRequestCompletion,
) -> bool {
    request.file_system_id == completion.file_system_id
        && request.requester_node_id == completion.requester_node_id
        && request.request_id == completion.request_id
        && request.operation_id == completion.operation_id
        && request.authority_shard_id == completion.authority_shard_id
        && request.command_digest == completion.command_digest
        && request.created_at == completion.created_at
        && matches!(
            &request.operation,
            DfsNamespaceOperation::CommitLocalEventualBatch { transaction, .. }
                if transaction.transaction_id == completion.transaction_id
        )
}

pub(super) fn namespace_request_transition_is_valid(
    existing: &DfsNamespaceRequestRecord,
    request: &DfsNamespaceRequestRecord,
) -> bool {
    if !namespace_request_immutable_payload_matches(existing, request) {
        return false;
    }
    match (&existing.status, &request.status) {
        (DfsNamespaceRequestStatus::Pending, DfsNamespaceRequestStatus::Pending) => {
            request.result.is_none()
                && request.posix_coordination_result.is_none()
                && request.recovery_attempts >= existing.recovery_attempts
        }
        (DfsNamespaceRequestStatus::Pending, DfsNamespaceRequestStatus::Completed) => true,
        (DfsNamespaceRequestStatus::Completed, DfsNamespaceRequestStatus::Completed) => {
            completed_namespace_request_outcome_matches(existing, request)
        }
        (DfsNamespaceRequestStatus::Completed, DfsNamespaceRequestStatus::Pending) => false,
    }
}

pub(super) fn pending_namespace_request_admission_from_completion(
    completion: &DfsNamespaceRequestRecord,
) -> DfsNamespaceRequestRecord {
    let mut pending = completion.clone();
    pending.status = DfsNamespaceRequestStatus::Pending;
    pending.result = None;
    pending.posix_coordination_result = None;
    pending.recovery_attempts = 0;
    pending.last_recovery_error = None;
    pending.updated_at = pending.created_at;
    pending
}

pub(super) fn namespace_prepare_can_repair_terminal_request(
    tables: &DfsMetadataTables,
    prepare: &DfsNamespacePrepareRecord,
    existing_request: &DfsNamespaceRequestRecord,
    prepared_completion: &DfsNamespaceRequestRecord,
) -> bool {
    existing_request.status == DfsNamespaceRequestStatus::Completed
        && prepared_completion.status == DfsNamespaceRequestStatus::Completed
        && matches!(
            prepared_completion.result,
            Some(DfsNamespaceResult::Applied)
        )
        && tables
            .namespace_prepares
            .get(&prepare.operation_id)
            .is_some_and(|existing_prepare| {
                existing_prepare.status != DfsNamespacePrepareStatus::Aborted
                    && namespace_prepare_payload_matches(existing_prepare, prepare)
            })
}

pub(super) fn prepared_write_intent_rebind_payload_matches(
    left: &DfsWriteIntentRecord,
    right: &DfsWriteIntentRecord,
) -> bool {
    left.intent_id == right.intent_id
        && left.file_system_id == right.file_system_id
        && left.object_id == right.object_id
        && left.base_version == right.base_version
        && left.target_version == right.target_version
        && left.coordinator_node_id == right.coordinator_node_id
        && left.desired_replica_node_ids == right.desired_replica_node_ids
        && left.policy == right.policy
        && left.status == right.status
        && left.content_hash == right.content_hash
        && left.object_metadata == right.object_metadata
        && left.touch_parent_on_create == right.touch_parent_on_create
        && left.create_predecessor_object_id == right.create_predecessor_object_id
        && left.created_at == right.created_at
}

pub(super) fn validate_namespace_prepare_shape(
    tables: &DfsMetadataTables,
    shard_id: &DfsMetadataShardId,
    prepare: &DfsNamespacePrepareRecord,
) -> Result<ValidatedNamespacePreparePlan> {
    if prepare.operation_id.trim().is_empty() || prepare.operation_digest.trim().is_empty() {
        return Err(MetadataBackendError::Serde(
            "namespace prepare requires a non-empty operation id and digest".to_string(),
        ));
    }
    if prepare.owner_epoch == 0 {
        return Err(MetadataBackendError::Serde(format!(
            "namespace prepare {} requires a non-zero owner epoch",
            prepare.operation_id
        )));
    }
    if &prepare.shard_id != shard_id {
        return Err(MetadataBackendError::Serde(format!(
            "namespace prepare {} targets shard {}, not local shard {}",
            prepare.operation_id, prepare.shard_id.0, shard_id.0
        )));
    }
    if prepare.transaction.file_system_id != prepare.file_system_id {
        return Err(MetadataBackendError::Serde(format!(
            "namespace prepare {} filesystem {} does not match transaction filesystem {}",
            prepare.operation_id, prepare.file_system_id, prepare.transaction.file_system_id
        )));
    }
    if !tables.file_systems.contains_key(&prepare.file_system_id) {
        return Err(MetadataBackendError::UnknownFileSystem {
            file_system_id: prepare.file_system_id.clone(),
        });
    }
    if prepare.transaction.objects.is_empty()
        && prepare.transaction.versions.is_empty()
        && prepare.transaction.advisory_lock_states.is_empty()
    {
        return Err(MetadataBackendError::Serde(format!(
            "namespace prepare {} contains an empty transaction",
            prepare.operation_id
        )));
    }
    if prepare
        .transaction
        .objects
        .iter()
        .any(|object| object.file_system_id != prepare.file_system_id)
        || prepare
            .transaction
            .versions
            .iter()
            .any(|version| version.file_system_id != prepare.file_system_id)
        || prepare
            .transaction
            .advisory_lock_states
            .iter()
            .any(|state| state.file_system_id != prepare.file_system_id)
        || prepare
            .content_receipts
            .iter()
            .any(|receipt| receipt.receipt.file_system_id != prepare.file_system_id)
    {
        return Err(MetadataBackendError::Serde(format!(
            "namespace prepare {} contains records outside filesystem {}",
            prepare.operation_id, prepare.file_system_id
        )));
    }

    let mut expected_object_ids = BTreeSet::new();
    for expected in &prepare.expected_object_versions {
        if !expected_object_ids.insert(expected.object_id.clone()) {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} repeats object expectation {}",
                prepare.operation_id, expected.object_id
            )));
        }
    }
    if let Some(object_id) = prepare
        .transaction
        .objects
        .iter()
        .map(|object| &object.object_id)
        .chain(
            prepare
                .transaction
                .versions
                .iter()
                .map(|version| &version.object_id),
        )
        .chain(
            prepare
                .transaction
                .advisory_lock_states
                .iter()
                .map(|state| &state.object_id),
        )
        .filter(|object_id| !expected_object_ids.contains(*object_id))
        .min()
    {
        return Err(MetadataBackendError::Serde(format!(
            "namespace prepare {} is missing the version expectation for object {}",
            prepare.operation_id, object_id
        )));
    }

    let mut expected_paths = BTreeSet::new();
    for expected in &prepare.expected_path_occupants {
        let path = normalize_path(&expected.path);
        if expected_paths.contains(&path) {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} repeats path expectation {}",
                prepare.operation_id, path
            )));
        }
        expected_paths.insert(path);
    }
    if let Some(path) = prepare
        .transaction
        .objects
        .iter()
        .map(|object| object.path.trim_matches('/'))
        .filter(|path| !expected_paths.contains(*path))
        .min()
    {
        return Err(MetadataBackendError::Serde(format!(
            "namespace prepare {} is missing the occupant expectation for path {}",
            prepare.operation_id, path
        )));
    }
    let transaction_objects = prepare
        .transaction
        .objects
        .iter()
        .map(|object| (&object.object_id, object))
        .collect::<BTreeMap<&DfsObjectId, _>>();
    let transaction_versions = prepare
        .transaction
        .versions
        .iter()
        .map(|version| ((&version.object_id, &version.version), version))
        .collect::<BTreeMap<(&DfsObjectId, &DfsVersion), _>>();
    let mut receipt_keys = BTreeSet::new();
    for receipt in &prepare.content_receipts {
        let key = (
            &receipt.receipt.object_id,
            &receipt.receipt.committed_version,
            &receipt.replica_node_id,
        );
        if !receipt_keys.insert(key) {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} repeats a durable content receipt for object {} version {} replica {}",
                prepare.operation_id,
                receipt.receipt.object_id,
                receipt.receipt.committed_version,
                receipt.replica_node_id
            )));
        }
        let Some(object) = transaction_objects.get(&&receipt.receipt.object_id) else {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} has a content receipt for object {} outside its transaction",
                prepare.operation_id, receipt.receipt.object_id
            )));
        };
        let version_key = (
            &receipt.receipt.object_id,
            &receipt.receipt.committed_version,
        );
        let Some(version) = transaction_versions.get(&version_key) else {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} has a content receipt without a matching visible version for object {}",
                prepare.operation_id, receipt.receipt.object_id
            )));
        };
        if object.version != receipt.receipt.committed_version
            || object.content_hash != receipt.content_hash
            || version.content_hash != receipt.content_hash
            || !object.current_locations.contains(&receipt.replica_node_id)
            || !version.locations.contains(&receipt.replica_node_id)
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} content receipt does not match visible object {}",
                prepare.operation_id, receipt.receipt.object_id
            )));
        }
    }
    let completion_attachment_count = usize::from(prepare.completed_request.is_some())
        + usize::from(prepare.completed_request_completion.is_some());
    if completion_attachment_count > 1 {
        return Err(MetadataBackendError::Serde(format!(
            "namespace prepare {} carries both full and compact completed request attachments",
            prepare.operation_id
        )));
    }
    if prepare.operation_id.starts_with("namespace-request:") && completion_attachment_count == 0 {
        return Err(MetadataBackendError::Serde(format!(
            "namespace prepare {} is request-owned but has no completed request attachment",
            prepare.operation_id
        )));
    }
    if let Some(completed_request) = &prepare.completed_request {
        validate_namespace_request_shape(completed_request)?;
        if completed_request.status != DfsNamespaceRequestStatus::Completed
            || completed_request.file_system_id != prepare.file_system_id
            || completed_request.authority_shard_id != prepare.shard_id
            || !prepare.operation_id.starts_with(&format!(
                "namespace-request:{}:",
                completed_request.operation_id
            ))
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} has an invalid completed request attachment",
                prepare.operation_id
            )));
        }
    }
    if let Some(completion) = &prepare.completed_request_completion {
        validate_namespace_request_completion_shape(completion)?;
        if completion.file_system_id != prepare.file_system_id
            || completion.authority_shard_id != prepare.shard_id
            || completion.transaction_id != prepare.transaction.transaction_id
            || !prepare
                .operation_id
                .starts_with(&format!("namespace-request:{}:", completion.operation_id))
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} has an invalid compact completed request attachment",
                prepare.operation_id
            )));
        }
    }
    validate_namespace_prepare_current_state(tables, prepare, &prepare.status)?;
    Ok(ValidatedNamespacePreparePlan {
        footprint: DfsNamespacePrepareFootprint::from_validated_parts(
            prepare,
            expected_object_ids,
            expected_paths,
        ),
    })
}

/// Validate the table-dependent portion of a namespace prepare separately from
/// its immutable payload shape. Admission calls this before installing the
/// hidden record; commit calls it again after replica acknowledgement because
/// receipts, transaction/request identities, and terminal outcomes can change
/// while the Prepared footprint remains reserved.
pub(super) fn validate_namespace_prepare_current_state(
    tables: &DfsMetadataTables,
    prepare: &DfsNamespacePrepareRecord,
    effective_status: &DfsNamespacePrepareStatus,
) -> Result<()> {
    for receipt in &prepare.content_receipts {
        if let Some(existing) = tables.receipts.get(&(
            receipt.receipt.object_id.clone(),
            receipt.receipt.committed_version.clone(),
            receipt.replica_node_id.clone(),
        )) && !replay_receipt_matches_existing(existing, receipt)
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} conflicts with the durable content receipt for object {} version {} replica {}",
                prepare.operation_id,
                receipt.receipt.object_id,
                receipt.receipt.committed_version,
                receipt.replica_node_id
            )));
        }
    }
    if let Some(existing) = tables
        .namespace_transactions
        .get(&prepare.transaction.transaction_id)
        && existing != &prepare.transaction
    {
        return Err(MetadataBackendError::Serde(format!(
            "namespace prepare {} reuses transaction id {} with a different payload",
            prepare.operation_id, prepare.transaction.transaction_id
        )));
    }
    if let Some(completed_request) = &prepare.completed_request {
        let request_key = (
            completed_request.requester_node_id.clone(),
            completed_request.request_id.clone(),
        );
        if let Some(receipt) = tables.namespace_request_receipts.get(&request_key) {
            if !receipt.matches_completed_request(completed_request) {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {} completed request conflicts with compacted receipt {}/{}",
                    prepare.operation_id,
                    completed_request.requester_node_id,
                    completed_request.request_id
                )));
            }
        } else {
            match tables.namespace_requests.get(&request_key) {
                Some(existing)
                    if namespace_request_transition_is_valid(existing, completed_request) => {}
                Some(existing)
                    if namespace_prepare_can_repair_terminal_request(
                        tables,
                        prepare,
                        existing,
                        completed_request,
                    ) =>
                {
                    // Once the exact hidden request-owned prepare is durable, its
                    // attached outcome owns crash recovery. An outer ambiguous
                    // failure used to record a contradictory terminal rejection
                    // after this point. Permit only that already-durable prepare
                    // to repair the request when it commits; a newly introduced
                    // prepare still cannot rewrite a terminal request.
                }
                Some(_) => {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {} completed request conflicts with {}/{}",
                        prepare.operation_id,
                        completed_request.requester_node_id,
                        completed_request.request_id
                    )));
                }
                None if effective_status == &DfsNamespacePrepareStatus::Prepared => {
                    // A request-owned prepare is itself the first durable
                    // operation boundary for the fused admission path. The table
                    // planner below materializes a derived Pending admission in
                    // the same atomic batch as this hidden prepare. Other prepare
                    // states still require that admission to exist already.
                }
                None => {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {} has no durable admission for completed request {}/{}",
                        prepare.operation_id,
                        completed_request.requester_node_id,
                        completed_request.request_id
                    )));
                }
            }
        }
    }
    if let Some(completion) = &prepare.completed_request_completion {
        let request_key = completion.request_key();
        if let Some(receipt) = tables.namespace_request_receipts.get(&request_key) {
            if !receipt.matches_completion(completion) {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {} compact completion conflicts with receipt {}/{}",
                    prepare.operation_id, completion.requester_node_id, completion.request_id
                )));
            }
        } else {
            match tables.namespace_requests.get(&request_key) {
                Some(existing)
                    if namespace_request_identity_matches_completion(existing, completion)
                        && (existing.status == DfsNamespaceRequestStatus::Pending
                            || completion.matches_completed_request(existing)) => {}
                Some(_) => {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {} compact completion conflicts with {}/{}",
                        prepare.operation_id, completion.requester_node_id, completion.request_id
                    )));
                }
                None if effective_status == &DfsNamespacePrepareStatus::Prepared => {
                    // The compact attachment and hidden prepare are the first
                    // durable boundary. Unlike the legacy full attachment, no
                    // duplicate Pending request body is materialized.
                }
                None if tables
                    .namespace_prepares
                    .get(&prepare.operation_id)
                    .is_some_and(|existing_prepare| {
                        existing_prepare.status != DfsNamespacePrepareStatus::Aborted
                            && namespace_prepare_payload_matches(existing_prepare, prepare)
                    }) =>
                {
                    // Commit/abort validation may run against the durable
                    // Prepared record before the compact terminal receipt is
                    // atomically materialized.
                }
                None => {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {} has no durable owner for compact completion {}/{}",
                        prepare.operation_id, completion.requester_node_id, completion.request_id
                    )));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn validate_prepared_namespace_commit_state(
    tables: &DfsMetadataTables,
    prepare: &DfsNamespacePrepareRecord,
) -> Result<()> {
    validate_namespace_prepare_current_state(tables, prepare, &DfsNamespacePrepareStatus::Committed)
}

pub(super) fn namespace_prepare_request_completion_is_materialized(
    tables: &DfsMetadataTables,
    prepare: &DfsNamespacePrepareRecord,
) -> bool {
    let full_completion_materialized =
        prepare.completed_request.as_ref().is_none_or(|completion| {
            let key = (
                completion.requester_node_id.clone(),
                completion.request_id.clone(),
            );
            tables.namespace_requests.get(&key).is_some_and(|existing| {
                completed_namespace_request_outcome_matches(existing, completion)
            }) || tables
                .namespace_request_receipts
                .get(&key)
                .is_some_and(|receipt| receipt.matches_completed_request(completion))
        });
    let compact_completion_materialized =
        prepare
            .completed_request_completion
            .as_ref()
            .is_none_or(|completion| {
                let key = completion.request_key();
                tables
                    .namespace_requests
                    .get(&key)
                    .is_some_and(|request| completion.matches_completed_request(request))
                    || tables
                        .namespace_request_receipts
                        .get(&key)
                        .is_some_and(|receipt| receipt.matches_completion(completion))
            });
    full_completion_materialized && compact_completion_materialized
}

pub(super) fn namespace_prepare_content_receipts_are_complete(
    tables: &DfsMetadataTables,
    prepare: &DfsNamespacePrepareRecord,
) -> bool {
    prepare.content_receipts.iter().all(|receipt| {
        tables
            .receipts
            .get(&(
                receipt.receipt.object_id.clone(),
                receipt.receipt.committed_version.clone(),
                receipt.replica_node_id.clone(),
            ))
            .is_some_and(|existing| replay_receipt_matches_existing(existing, receipt))
    })
}

pub(super) fn missing_namespace_prepare_content_receipt_ops(
    tables: &DfsMetadataTables,
    prepare: &DfsNamespacePrepareRecord,
) -> Vec<DfsMetadataTableOp> {
    prepare
        .content_receipts
        .iter()
        .filter(|receipt| {
            !tables
                .receipts
                .get(&(
                    receipt.receipt.object_id.clone(),
                    receipt.receipt.committed_version.clone(),
                    receipt.replica_node_id.clone(),
                ))
                .is_some_and(|existing| replay_receipt_matches_existing(existing, receipt))
        })
        .cloned()
        .map(|receipt| DfsMetadataTableOp::PutReceipt { receipt })
        .collect()
}

pub(super) fn validate_namespace_prepare_expectations(
    tables: &DfsMetadataTables,
    prepare: &DfsNamespacePrepareRecord,
) -> Result<()> {
    for expected in &prepare.expected_object_versions {
        let current = tables
            .objects
            .get(&expected.object_id)
            .map(|object| object.version.clone());
        if current != expected.expected_version {
            if let (Some(expected_version), Some(current_version)) =
                (&expected.expected_version, &current)
            {
                return Err(MetadataBackendError::Conflict {
                    object_id: expected.object_id.clone(),
                    expected: expected_version.clone(),
                    current: current_version.clone(),
                });
            }
            return Err(MetadataBackendError::replica_diverged(format!(
                "namespace prepare {} object {} version expectation changed: expected {:?}, current {:?}",
                prepare.operation_id, expected.object_id, expected.expected_version, current
            )));
        }
    }
    for expected in &prepare.expected_path_occupants {
        let current = object_by_path_from_tables(tables, &prepare.file_system_id, &expected.path)
            .map(|object| object.object_id);
        if current != expected.expected_object_id {
            return Err(MetadataBackendError::replica_diverged(format!(
                "namespace prepare {} path {} occupant expectation changed: expected {:?}, current {:?}",
                prepare.operation_id, expected.path, expected.expected_object_id, current
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_namespace_prepare_does_not_overlap(
    tables: &DfsMetadataTables,
    prepare: &DfsNamespacePrepareRecord,
    footprint: &DfsNamespacePrepareFootprint,
) -> Result<()> {
    for object_id in footprint.object_ids() {
        if let Some(existing_operation_id) = tables.namespace_prepare_object_owner_other_than(
            &prepare.file_system_id,
            object_id,
            &prepare.operation_id,
        ) {
            return Err(MetadataBackendError::replica_diverged(format!(
                "namespace prepare {} overlaps active prepare {}",
                prepare.operation_id, existing_operation_id
            )));
        }
    }
    for path in footprint.normalized_paths() {
        if let Some(existing_operation_id) = tables
            .namespace_prepare_normalized_path_owner_other_than(
                &prepare.file_system_id,
                path,
                &prepare.operation_id,
            )
        {
            return Err(MetadataBackendError::replica_diverged(format!(
                "namespace prepare {} overlaps active prepare {}",
                prepare.operation_id, existing_operation_id
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn validate_namespace_prepare_does_not_overlap_reference(
    tables: &DfsMetadataTables,
    prepare: &DfsNamespacePrepareRecord,
) -> Result<()> {
    NAMESPACE_PREPARE_HISTORY_SCAN_CALLS
        .set(NAMESPACE_PREPARE_HISTORY_SCAN_CALLS.get().saturating_add(1));
    let footprint = namespace_prepare_footprint(prepare);
    for existing in tables.namespace_prepares.values().filter(|existing| {
        existing.status == DfsNamespacePrepareStatus::Prepared
            && existing.operation_id != prepare.operation_id
            && existing.file_system_id == prepare.file_system_id
    }) {
        let existing_footprint = namespace_prepare_footprint(existing);
        if !footprint
            .object_ids()
            .is_disjoint(existing_footprint.object_ids())
            || !footprint
                .normalized_paths()
                .is_disjoint(existing_footprint.normalized_paths())
        {
            return Err(MetadataBackendError::replica_diverged(format!(
                "namespace prepare {} overlaps active prepare {}",
                prepare.operation_id, existing.operation_id
            )));
        }
    }
    Ok(())
}

pub(super) fn namespace_prepare_table_ops_for_record(
    tables: &DfsMetadataTables,
    shard_id: &DfsMetadataShardId,
    prepare: &DfsNamespacePrepareRecord,
) -> Result<Vec<DfsMetadataTableOp>> {
    namespace_prepare_table_plan_for_payload(tables, shard_id, Cow::Borrowed(prepare))
        .map(|plan| plan.ops)
}

pub(super) fn namespace_prepare_table_ops_for_owned_record(
    tables: &DfsMetadataTables,
    shard_id: &DfsMetadataShardId,
    prepare: DfsNamespacePrepareRecord,
) -> Result<Vec<DfsMetadataTableOp>> {
    namespace_prepare_table_plan_for_payload(tables, shard_id, Cow::Owned(prepare))
        .map(|plan| plan.ops)
}

pub(super) fn namespace_prepare_table_plan_for_payload(
    tables: &DfsMetadataTables,
    shard_id: &DfsMetadataShardId,
    prepare: Cow<'_, DfsNamespacePrepareRecord>,
) -> Result<NamespacePrepareTablePlan> {
    let validated = validate_namespace_prepare_shape(tables, shard_id, prepare.as_ref())?;
    let existing = tables.namespace_prepares.get(&prepare.operation_id);
    match &prepare.status {
        DfsNamespacePrepareStatus::Prepared => {
            if let Some(existing) = existing {
                if !namespace_prepare_payload_matches(existing, prepare.as_ref()) {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {} reuses an operation id with a different payload",
                        prepare.operation_id
                    )));
                }
                if existing.status != DfsNamespacePrepareStatus::Prepared {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {} cannot move from {:?} back to prepared",
                        prepare.operation_id, existing.status
                    )));
                }
            }
            validate_namespace_prepare_expectations(tables, prepare.as_ref())?;
            validate_namespace_prepare_does_not_overlap(
                tables,
                prepare.as_ref(),
                &validated.footprint,
            )?;
            validate_namespace_transaction_preconditions(tables, &prepare.transaction)?;
            let mut ops = Vec::with_capacity(2);
            if let Some(completion) = prepare.completed_request.as_ref() {
                let request_key = (
                    completion.requester_node_id.clone(),
                    completion.request_id.clone(),
                );
                if !tables.namespace_requests.contains_key(&request_key) {
                    ops.push(DfsMetadataTableOp::put_namespace_request(
                        pending_namespace_request_admission_from_completion(completion),
                    ));
                }
            }
            ops.push(DfsMetadataTableOp::put_namespace_prepare(
                prepare.into_owned(),
            ));
            Ok(NamespacePrepareTablePlan {
                ops,
                prepared_footprint: Some(validated.footprint),
            })
        }
        DfsNamespacePrepareStatus::Committed => {
            let existing = existing.ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "namespace prepare {} cannot commit before it is prepared",
                    prepare.operation_id
                ))
            })?;
            if !namespace_prepare_payload_matches(existing, prepare.as_ref()) {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {} commit payload does not match the durable prepare",
                    prepare.operation_id
                )));
            }
            if existing.status == DfsNamespacePrepareStatus::Aborted {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {} is aborted",
                    prepare.operation_id
                )));
            }
            if existing.status == DfsNamespacePrepareStatus::Committed
                && existing == prepare.as_ref()
                && namespace_transaction_recovery_is_complete(tables, &prepare.transaction)
                && namespace_prepare_content_receipts_are_complete(tables, prepare.as_ref())
                && namespace_prepare_request_completion_is_materialized(tables, prepare.as_ref())
            {
                return Ok(NamespacePrepareTablePlan {
                    ops: Vec::new(),
                    prepared_footprint: None,
                });
            }
            if existing.status != DfsNamespacePrepareStatus::Committed {
                validate_namespace_prepare_expectations(tables, prepare.as_ref())?;
                validate_namespace_prepare_does_not_overlap(
                    tables,
                    prepare.as_ref(),
                    &validated.footprint,
                )?;
                validate_namespace_transaction_preconditions(tables, &prepare.transaction)?;
                return Ok(NamespacePrepareTablePlan {
                    ops: vec![DfsMetadataTableOp::CommitNamespacePrepare {
                        operation_id: prepare.operation_id.clone(),
                        operation_digest: prepare.operation_digest.clone(),
                        owner_epoch: prepare.owner_epoch,
                        retained_at: None,
                    }],
                    prepared_footprint: None,
                });
            }
            let mut ops = missing_namespace_prepare_content_receipt_ops(tables, prepare.as_ref());
            ops.extend(plan_namespace_transaction_recovery_table_ops(
                tables,
                &prepare.transaction,
            ));
            let prepare_insert_index = ops.len();
            if let Some(request) = &prepare.completed_request {
                let key = (
                    request.requester_node_id.clone(),
                    request.request_id.clone(),
                );
                let materialized = tables.namespace_requests.get(&key).is_some_and(|existing| {
                    completed_namespace_request_outcome_matches(existing, request)
                }) || tables
                    .namespace_request_receipts
                    .get(&key)
                    .is_some_and(|receipt| receipt.matches_completed_request(request));
                if !materialized {
                    ops.push(DfsMetadataTableOp::put_namespace_request(request.clone()));
                }
            }
            if let Some(completion) = &prepare.completed_request_completion {
                let key = completion.request_key();
                let materialized = tables
                    .namespace_requests
                    .get(&key)
                    .is_some_and(|request| completion.matches_completed_request(request))
                    || tables
                        .namespace_request_receipts
                        .get(&key)
                        .is_some_and(|receipt| receipt.matches_completion(completion));
                if !materialized {
                    ops.push(DfsMetadataTableOp::put_namespace_request_receipt(
                        DfsNamespaceRequestReceipt::from_completion(completion),
                    ));
                }
            }
            ops.insert(
                prepare_insert_index,
                DfsMetadataTableOp::put_namespace_prepare(prepare.into_owned()),
            );
            Ok(NamespacePrepareTablePlan {
                ops,
                prepared_footprint: None,
            })
        }
        DfsNamespacePrepareStatus::Aborted => {
            let existing = existing.ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "namespace prepare {} cannot abort before it is prepared",
                    prepare.operation_id
                ))
            })?;
            if !namespace_prepare_payload_matches(existing, prepare.as_ref()) {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {} abort payload does not match the durable prepare",
                    prepare.operation_id
                )));
            }
            if existing.status == DfsNamespacePrepareStatus::Committed {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {} is already committed",
                    prepare.operation_id
                )));
            }
            if existing == prepare.as_ref() {
                return Ok(NamespacePrepareTablePlan {
                    ops: Vec::new(),
                    prepared_footprint: None,
                });
            }
            Ok(NamespacePrepareTablePlan {
                ops: vec![DfsMetadataTableOp::put_namespace_prepare(
                    prepare.into_owned(),
                )],
                prepared_footprint: None,
            })
        }
    }
}

pub(super) fn namespace_prepare_journal_reference_from_validated(
    entry: &DfsMetadataJournalEntry,
    prepare: &DfsNamespacePrepareRecord,
    journal_prepare: &DfsNamespacePreparePayloadRef<'_>,
) -> Result<DfsMetadataTableOp> {
    let DfsMetadataMutation::PutNamespacePrepare {
        file_system_id,
        operation_id,
        status,
        ..
    } = &entry.mutation
    else {
        return Err(MetadataBackendError::Serde(format!(
            "journal entry {} cannot back namespace prepare {} table materialization",
            entry.sequence, prepare.operation_id
        )));
    };
    if entry.sequence == 0
        || entry.shard_id != prepare.shard_id
        || file_system_id != &prepare.file_system_id
        || operation_id != &prepare.operation_id
        || status != &prepare.status
        || !journal_prepare.matches_prepare_identity(prepare)
    {
        return Err(MetadataBackendError::Serde(format!(
            "journal entry {} does not match namespace prepare {} table reference fences",
            entry.sequence, prepare.operation_id
        )));
    }
    Ok(DfsMetadataTableOp::PutNamespacePrepareJournalReference {
        shard_id: prepare.shard_id.clone(),
        journal_sequence: entry.sequence,
        file_system_id: prepare.file_system_id.clone(),
        operation_id: prepare.operation_id.clone(),
        operation_digest: prepare.operation_digest.clone(),
        owner_epoch: prepare.owner_epoch,
        status: prepare.status.clone(),
    })
}

pub(super) fn namespace_prepare_persisted_table_ops(
    entry: &DfsMetadataJournalEntry,
    apply_ops: &[DfsMetadataTableOp],
) -> Result<Vec<DfsMetadataTableOp>> {
    // This helper is called only for an entry just authored from the same
    // canonical prepare that produced `apply_ops`. Packing constructed a
    // structurally valid representation, so preserve that proof instead of
    // rescanning the complete body before replacing the table record with its
    // compact journal reference.
    let journal_prepare = entry
        .mutation
        .namespace_prepare_payload_ref_prevalidated()
        .ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "journal entry {} cannot back namespace prepare table materialization without one payload",
                entry.sequence
            ))
        })?;
    let mut prepare_references = 0usize;
    let mut persisted_ops = Vec::with_capacity(apply_ops.len());
    for op in apply_ops {
        match op {
            DfsMetadataTableOp::PutNamespacePrepare { prepare } => {
                persisted_ops.push(namespace_prepare_journal_reference_from_validated(
                    entry,
                    prepare,
                    &journal_prepare,
                )?);
                prepare_references = prepare_references.saturating_add(1);
            }
            op => persisted_ops.push(op.clone()),
        }
    }
    if prepare_references != 1 {
        return Err(MetadataBackendError::Serde(format!(
            "journal entry {} namespace prepare table materialization produced {prepare_references} prepare references",
            entry.sequence
        )));
    }
    Ok(persisted_ops)
}

pub(super) fn namespace_prepare_commit_table_ops_for_reference(
    tables: &DfsMetadataTables,
    shard_id: &DfsMetadataShardId,
    file_system_id: &DfsFileSystemId,
    operation_id: &str,
    operation_digest: &str,
    owner_epoch: u64,
    object_ids: &[DfsObjectId],
    retained_at: OffsetDateTime,
) -> Result<Vec<DfsMetadataTableOp>> {
    if object_ids.windows(2).any(|ids| ids[0] >= ids[1]) {
        return Err(MetadataBackendError::Serde(format!(
            "compact namespace commit {operation_id} object ids are not sorted and unique"
        )));
    }
    if let Some(receipt) = tables.namespace_prepare_receipts.get(operation_id) {
        if receipt.file_system_id != *file_system_id
            || receipt.operation_id != operation_id
            || receipt.operation_digest != operation_digest
            || receipt.owner_epoch != owner_epoch
        {
            return Err(MetadataBackendError::Serde(format!(
                "compact namespace commit {operation_id} conflicts with its terminal receipt"
            )));
        }
        if receipt.status == DfsNamespacePrepareStatus::Aborted {
            return Err(MetadataBackendError::Serde(format!(
                "compact namespace commit {operation_id} refers to an aborted prepare"
            )));
        }
        if receipt.status != DfsNamespacePrepareStatus::Committed {
            return Err(MetadataBackendError::Serde(format!(
                "compact namespace commit {operation_id} has a non-terminal prepare receipt"
            )));
        }
        return Ok(Vec::new());
    }

    let mut prepare = tables
        .namespace_prepares
        .get(operation_id)
        .cloned()
        .ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "compact namespace commit {operation_id} has no preceding durable prepare"
            ))
        })?;
    if prepare.file_system_id != *file_system_id
        || prepare.operation_id != operation_id
        || prepare.shard_id != *shard_id
        || prepare.operation_digest != operation_digest
        || prepare.owner_epoch != owner_epoch
    {
        return Err(MetadataBackendError::Serde(format!(
            "compact namespace commit {operation_id} scope/digest/epoch fence mismatch"
        )));
    }
    let DfsMetadataMutation::CommitNamespacePrepare {
        object_ids: expected_object_ids,
        ..
    } = DfsMetadataMutation::commit_namespace_prepare(&prepare)
    else {
        unreachable!("namespace prepare commit constructor returned another mutation");
    };
    if object_ids != expected_object_ids {
        return Err(MetadataBackendError::Serde(format!(
            "compact namespace commit {operation_id} affected-object fence mismatch"
        )));
    }
    if prepare.status == DfsNamespacePrepareStatus::Aborted {
        return Err(MetadataBackendError::Serde(format!(
            "compact namespace commit {operation_id} refers to an aborted prepare"
        )));
    }
    prepare.status = DfsNamespacePrepareStatus::Committed;
    let mut ops = namespace_prepare_table_ops_for_record(tables, shard_id, &prepare)?;
    for op in &mut ops {
        if let DfsMetadataTableOp::CommitNamespacePrepare {
            retained_at: marker_retained_at,
            ..
        } = op
        {
            *marker_retained_at = Some(retained_at);
        }
    }
    Ok(ops)
}
