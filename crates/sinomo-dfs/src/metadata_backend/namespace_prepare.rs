//! Current immutable-operation and namespace precondition validation.
use super::*;

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
    if !existing.matches_immutable_payload(request) {
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
            existing.matches_completed_outcome(request)
        }
        (DfsNamespaceRequestStatus::Completed, DfsNamespaceRequestStatus::Pending) => false,
    }
}

pub(super) fn validate_prepared_operation_shape(
    tables: &DfsMetadataTables,
    shard_id: &DfsMetadataShardId,
    prepare: &DfsPreparedOperationRecord,
) -> Result<ValidatedNamespacePreparePlan> {
    prepare.validate()?;
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
    prepare
        .transaction
        .validate_content_reference_annotation()
        .map_err(MetadataBackendError::Serde)?;
    if prepare.transaction.objects.is_empty()
        && prepare.transaction.versions.is_empty()
        && prepare.transaction.advisory_lock_states.is_empty()
        && prepare.metadata_effects.is_empty()
    {
        // A V6 reference release can definitively do nothing while still
        // committing its immutable retry outcome. Require its annotation,
        // logical input digest and original source-head observation; neither
        // arbitrary empty transactions nor pin-only effects use this path.
        let witnessed_noop = prepare
            .transaction
            .content_reference_identity
            .as_ref()
            .is_some_and(|identity| {
                prepare.content_receipts.is_empty()
                    && prepare
                        .transaction
                        .file_version_pins
                        .as_ref()
                        .is_none_or(|pins| pins.add.is_empty() && pins.remove.is_empty())
                    && prepare.expected_object_versions.iter().any(|expected| {
                        expected.object_id == identity.object_id
                            && expected.expected_version.is_some()
                    })
            });
        if !witnessed_noop {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} contains an empty transaction",
                prepare.operation_id
            )));
        }
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
    let retained_content = prepare.prepared_intent.as_ref().is_some_and(|intent| {
        matches!(
            intent.publication,
            crate::DfsWritePublication::Retained { .. }
        )
    });
    if !retained_content
        && prepare.transaction.objects.is_empty()
        && prepare.transaction.versions.iter().any(|version| {
            version.has_retention_grant()
                && !tables
                    .versions
                    .contains_key(&(version.object_id.clone(), version.version.clone()))
        })
    {
        return Err(MetadataBackendError::Serde(
            "new retained content requires a complete sealed write body".into(),
        ));
    }
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
        let object = transaction_objects.get(&&receipt.receipt.object_id).copied()
            .or_else(|| retained_content.then(|| version.retention.values().find_map(|state|state.grant.as_ref().map(|grant|grant.object.as_ref()))).flatten())
            .ok_or_else(|| MetadataBackendError::Serde(format!(
                "namespace prepare {} has a content receipt for object {} outside its transaction",
                prepare.operation_id, receipt.receipt.object_id
            )))?;
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
    let attached_identity_matches = |attached_operation_id: &str| {
        if let Some(journal_id) = prepare.journal_operation_id.as_deref() {
            journal_id == attached_operation_id
                && prepare.operation_id
                    == crate::metadata_records::compound_prepared_operation_id(
                        journal_id,
                        &prepare.transaction.transaction_id,
                    )
        } else {
            prepare.operation_id
                == format!(
                    "namespace-request:{attached_operation_id}:{}",
                    prepare.transaction.transaction_id
                )
        }
    };
    if let Some(completed_request) = &prepare.completed_request {
        validate_namespace_request_shape(completed_request)?;
        if completed_request.status != DfsNamespaceRequestStatus::Completed
            || completed_request.file_system_id != prepare.file_system_id
            || completed_request.authority_shard_id != prepare.shard_id
            || !attached_identity_matches(&completed_request.operation_id)
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
            || !attached_identity_matches(&completion.operation_id)
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
    prepare: &DfsPreparedOperationRecord,
    effective_status: &DfsPreparedOperationStatus,
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
                Some(_) => {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {} completed request conflicts with {}/{}",
                        prepare.operation_id,
                        completed_request.requester_node_id,
                        completed_request.request_id
                    )));
                }
                None if effective_status == &DfsPreparedOperationStatus::Prepared => {
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
                None if effective_status == &DfsPreparedOperationStatus::Prepared => {
                    // The compact attachment and hidden prepare are the first
                    // durable boundary. Unlike the legacy full attachment, no
                    // duplicate Pending request body is materialized.
                }
                None if tables
                    .prepared_operations_v7
                    .get(&prepare.operation_id)
                    .is_some_and(|existing_prepare| {
                        existing_prepare.status != DfsPreparedOperationStatus::Aborted
                            && existing_prepare.digest == prepare.digest
                            && existing_prepare.payload == prepare.payload
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
    prepare: &DfsPreparedOperationRecord,
) -> Result<()> {
    validate_namespace_prepare_current_state(
        tables,
        prepare,
        &DfsPreparedOperationStatus::Committed,
    )
}

pub(super) fn validate_namespace_prepare_expectations(
    tables: &DfsMetadataTables,
    prepare: &DfsPreparedOperationRecord,
) -> Result<()> {
    validate_namespace_read_set(
        tables,
        &prepare.file_system_id,
        &prepare.operation_id,
        &prepare.expected_object_versions,
        &prepare.expected_path_occupants,
    )
}

pub(super) fn validate_namespace_read_set(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    operation_id: &str,
    expected_objects: &[DfsNamespaceObjectVersionExpectation],
    expected_paths: &[DfsNamespacePathOccupantExpectation],
) -> Result<()> {
    for expected in expected_objects {
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
                operation_id, expected.object_id, expected.expected_version, current
            )));
        }
    }
    for expected in expected_paths {
        let current = object_by_path_from_tables(tables, file_system_id, &expected.path)
            .map(|object| object.object_id);
        if current != expected.expected_object_id {
            return Err(MetadataBackendError::replica_diverged(format!(
                "namespace prepare {} path {} occupant expectation changed: expected {:?}, current {:?}",
                operation_id, expected.path, expected.expected_object_id, current
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_namespace_prepare_does_not_overlap(
    tables: &DfsMetadataTables,
    prepare: &DfsPreparedOperationRecord,
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
