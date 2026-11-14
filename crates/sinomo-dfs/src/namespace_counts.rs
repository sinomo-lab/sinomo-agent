//! Admission checks for newly authored maintained-count metadata. Historical
//! immutable prepared bodies remain decodable and retain their original digest.
use crate::*;

pub(crate) const COUNT_PREDECESSOR_BINDING_PREFIX: &str = "namespace-count-initialization:";

pub(crate) fn cohort_binding_components<'a>(
    operation_id: &'a str,
    shard: &DfsMetadataShardId,
) -> Option<(&'a str, &'a str)> {
    let body = operation_id
        .strip_prefix("namespace-count-cohort:")?
        .strip_suffix(&format!(":{}", shard.0))?;
    let (initialization, predecessor_hash) = body.rsplit_once(':')?;
    (!initialization.is_empty()
        && predecessor_hash.len() == 64
        && predecessor_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then_some((initialization, predecessor_hash))
}

/// Parse the scoped DATA identifier; this alone proves neither a binding nor
/// successful activation. The complete canonical payload/digest is also checked
/// before core treats a stored record as a readiness obligation.
pub fn namespace_count_cohort_binding_initialization_id<'a>(
    operation_id: &'a str,
    shard: &DfsMetadataShardId,
) -> Option<&'a str> {
    cohort_binding_components(operation_id, shard).map(|(initialization, _)| initialization)
}

fn canonical_cohort_binding(
    digest: &DfsPreparedOperationDigest,
    at: time::OffsetDateTime,
) -> Option<DfsPreparedOperationPayload> {
    let (_, predecessor_hash) = cohort_binding_components(&digest.operation_id, &digest.shard_id)?;
    let mut payload = DfsPreparedOperationPayload::for_metadata_effects(
        DfsPreparedOperationScope {
            file_system_id: digest.file_system_id.clone(),
            shard_id: digest.shard_id.clone(),
            owner_epoch: digest.owner_epoch,
            operation_id: digest.operation_id.clone(),
        },
        Vec::new(),
        Vec::new(),
        Vec::new(),
        at,
    )
    .ok()?;
    payload.protocol_version = digest.protocol_version;
    payload.expected_journal_sequence = Some(digest.prepare_sequence.checked_sub(1)?);
    payload.journal_operation_id = Some(format!(
        "{COUNT_PREDECESSOR_BINDING_PREFIX}{predecessor_hash}"
    ));
    (DfsPreparedOperationDigest::at_sequence(&payload, digest.prepare_sequence).ok()? == *digest)
        .then_some(payload)
}

fn claims_cohort_type(operation_id: &str) -> bool {
    let Some((identity_and_hash, scope)) = operation_id
        .strip_prefix("namespace-count-cohort:")
        .and_then(|body| body.rsplit_once(':'))
    else {
        return false;
    };
    let Some((initialization, hash)) = identity_and_hash.rsplit_once(':') else {
        return false;
    };
    !initialization.is_empty()
        && !scope.is_empty()
        && hash.len() == 64
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn binding_payload_hint(payload: &DfsPreparedOperationPayload) -> bool {
    payload
        .journal_operation_id
        .as_ref()
        .is_some_and(|binding| binding.starts_with(COUNT_PREDECESSOR_BINDING_PREFIX))
        && payload.metadata_effects.is_empty()
        && payload.transaction.objects.is_empty()
        && payload.transaction.versions.is_empty()
        && payload.prepared_intent.is_none()
        && payload.terminal_intent.is_none()
        && payload.completed_request.is_none()
}

fn invalid_cohort_binding() -> MetadataBackendError {
    MetadataBackendError::Serde("malformed claimed namespace-count cohort binding".into())
}

/// Validated conversion obligations in already-decoded snapshot DATA. This is
/// neither an activation receipt nor a certificate that storage may serve.
pub fn namespace_count_cohort_initialization_ids(
    snapshot: &DfsMetadataTableSnapshot,
    shard: &DfsMetadataShardId,
) -> Result<std::collections::BTreeSet<String>, MetadataBackendError> {
    Ok(namespace_count_cohort_obligations(snapshot, shard)?
        .into_iter()
        .map(|(_, initialization)| initialization)
        .collect())
}

/// Preserve the filesystem binding when comparing obligations with current
/// authoritative activation. Malformed claimed protocol records refuse Ready;
/// an unrelated operation sharing only the prefix remains ordinary DATA.
pub fn namespace_count_cohort_obligations(
    snapshot: &DfsMetadataTableSnapshot,
    shard: &DfsMetadataShardId,
) -> Result<std::collections::BTreeSet<(DfsFileSystemId, String)>, MetadataBackendError> {
    collect_cohort_obligations(
        snapshot.tables.prepared_operations_v7.iter(),
        snapshot.tables.prepared_operation_evictions.iter(),
        shard,
    )
}

fn collect_cohort_obligations<'a>(
    records: impl Iterator<Item = &'a DfsPreparedOperationRecord>,
    evictions: impl Iterator<Item = &'a DfsPreparedOperationEviction>,
    shard: &DfsMetadataShardId,
) -> Result<std::collections::BTreeSet<(DfsFileSystemId, String)>, MetadataBackendError> {
    let mut obligations = std::collections::BTreeSet::new();
    for record in records {
        if !claims_cohort_type(&record.digest.operation_id)
            && !claims_cohort_type(&record.payload.operation_id)
            && !binding_payload_hint(&record.payload)
        {
            continue;
        }
        record.validate()?;
        let payload = canonical_cohort_binding(&record.digest, record.transaction.created_at)
            .ok_or_else(invalid_cohort_binding)?;
        if payload != *record.payload {
            return Err(invalid_cohort_binding());
        }
        if &record.digest.shard_id != shard {
            continue;
        }
        if matches!(
            record.status,
            DfsPreparedOperationStatus::Aborted | DfsPreparedOperationStatus::Superseded
        ) {
            continue;
        }
        let initialization =
            namespace_count_cohort_binding_initialization_id(&record.operation_id, shard)
                .ok_or_else(invalid_cohort_binding)?;
        obligations.insert((record.file_system_id.clone(), initialization.to_owned()));
    }
    for record in evictions {
        if !claims_cohort_type(&record.digest.operation_id)
            && !record
                .committed_entry
                .as_ref()
                .is_some_and(|commit| claims_cohort_type(&commit.digest.operation_id))
        {
            continue;
        }
        let initialization = namespace_count_cohort_binding_initialization_id(
            &record.digest.operation_id,
            &record.digest.shard_id,
        )
        .ok_or_else(invalid_cohort_binding)?;
        if matches!(
            record.status,
            DfsPreparedOperationStatus::Aborted | DfsPreparedOperationStatus::Superseded
        ) {
            if record.committed_entry.is_some() {
                return Err(invalid_cohort_binding());
            }
            continue;
        }
        if record.status != DfsPreparedOperationStatus::Committed
            || !record.committed_entry.as_ref().is_some_and(|commit| {
                commit.digest == record.digest
                    && commit.shard_id == record.digest.shard_id
                    && record.digest.prepare_sequence.checked_add(1) == Some(commit.sequence)
                    && canonical_cohort_binding(&record.digest, commit.created_at).is_some()
            })
        {
            return Err(invalid_cohort_binding());
        }
        if &record.digest.shard_id != shard {
            continue;
        }
        obligations.insert((
            record.digest.file_system_id.clone(),
            initialization.to_owned(),
        ));
    }
    Ok(obligations)
}

pub(crate) fn namespace_count_cohort_obligations_from_tables(
    tables: &crate::metadata_tables::DfsMetadataTables,
    shard: &DfsMetadataShardId,
) -> Result<std::collections::BTreeSet<(DfsFileSystemId, String)>, MetadataBackendError> {
    collect_cohort_obligations(
        tables.prepared_operations_v7.values(),
        tables.prepared_operation_evictions.values(),
        shard,
    )
}

pub(crate) fn validate_new_counted_publication(
    tables: &crate::metadata_tables::DfsMetadataTables,
    payload: &DfsPreparedOperationPayload,
) -> Result<(), MetadataBackendError> {
    let validate = |object: &DfsObjectMetadata| {
        let valid = match object.kind {
            DfsObjectKind::Hardlink => object.namespace_link_count.is_none(),
            DfsObjectKind::Tombstone if object.tombstone_provenance.is_some() => true,
            DfsObjectKind::Tombstone => object.namespace_link_count == Some(0),
            DfsObjectKind::Directory => object.namespace_link_count.is_some_and(|n| n >= 2),
            _ => object.namespace_link_count.is_some_and(|n| n > 0),
        };
        if !valid {
            return Err(MetadataBackendError::Serde(format!(
                "object {} has no valid maintained namespace count; explicit metadata maintenance is required",
                object.object_id,
            )));
        }
        if let Some(previous) = tables.objects.get(&object.object_id)
            && !matches!(
                previous.kind,
                DfsObjectKind::Hardlink | DfsObjectKind::Tombstone
            )
            && previous.namespace_link_count.is_none()
        {
            return Err(MetadataBackendError::Serde(
                "uninitialized namespace counts require explicit maintenance, not a normal mutation".into(),
            ));
        }
        if let Some(previous) = tables.objects.get(&object.object_id)
            && previous.namespace_link_count.is_some()
            && previous.namespace_link_count != object.namespace_link_count
            && previous.version == object.version
            && object.tombstone_provenance.is_none()
        {
            return Err(MetadataBackendError::Serde(
                "namespace count changes must advance the guarded object generation".into(),
            ));
        }
        Ok(())
    };
    for object in &payload.transaction.objects {
        validate(object)?;
    }
    for effect in &payload.metadata_effects {
        match effect {
            DfsPreparedMetadataEffect::PutObject { object } => {
                validate(object)?;
                let previous = tables.objects.get(&object.object_id);
                let changes_count = previous
                    .is_some_and(|old| old.namespace_link_count != object.namespace_link_count);
                let changes_alias = match previous {
                    Some(old) if old.kind == DfsObjectKind::Hardlink => {
                        object.kind != DfsObjectKind::Hardlink
                            || old.link_target_object_id != object.link_target_object_id
                    }
                    _ => object.kind == DfsObjectKind::Hardlink,
                };
                let changes_directory_membership = match previous {
                    Some(old) if old.kind == DfsObjectKind::Directory => {
                        object.kind != DfsObjectKind::Directory || old.path != object.path
                    }
                    _ => object.kind == DfsObjectKind::Directory,
                };
                let replaces_live_name = tables
                    .paths
                    .get(&(
                        object.file_system_id.clone(),
                        crate::metadata_keys::normalize_path(&object.path),
                    ))
                    .is_some_and(|path| {
                        path.object_id != object.object_id && path.kind != DfsObjectKind::Tombstone
                    });
                if changes_count
                    || changes_alias
                    || changes_directory_membership
                    || replaces_live_name
                {
                    return Err(MetadataBackendError::Serde(
                        "namespace membership/count changes require the owning namespace transaction".into(),
                    ));
                }
            }
            DfsPreparedMetadataEffect::FinalizeCrossShardSourceEviction { tombstone, .. } => {
                validate(tombstone)?
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(object: DfsObjectMetadata) -> DfsPreparedOperationPayload {
        DfsPreparedOperationPayload::for_metadata_effects(
            DfsPreparedOperationScope {
                file_system_id: object.file_system_id.clone(),
                shard_id: DfsMetadataShardId::new("root"),
                owner_epoch: 1,
                operation_id: "count-admission-data".into(),
            },
            Vec::new(),
            Vec::new(),
            vec![DfsPreparedMetadataEffect::PutObject { object }],
            time::OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap()
    }

    #[test]
    fn maintained_count_unknown_legacy_cannot_be_guessed_by_a_mutation() {
        let mut object = crate::directory_object("/legacy", NodeId::generate());
        object.namespace_link_count = None;
        let mut tables = crate::metadata_tables::DfsMetadataTables::default();
        tables
            .objects
            .insert(object.object_id.clone(), object.clone());
        object.namespace_link_count = Some(2);
        assert!(validate_new_counted_publication(&tables, &payload(object)).is_err());
    }

    #[test]
    fn maintained_count_metadata_only_updates_cannot_reset_a_count() {
        let object = crate::directory_object("/counted", NodeId::generate());
        let mut tables = crate::metadata_tables::DfsMetadataTables::default();
        tables
            .objects
            .insert(object.object_id.clone(), object.clone());
        let mut successor = object.clone();
        successor.namespace_link_count = Some(3);
        successor.version = DfsVersion::generate();
        let mut update = payload(successor.clone());
        assert!(validate_new_counted_publication(&tables, &update).is_err());
        // This validator checks count shape; the native kernel separately
        // authenticates namespace scope, immutable body and expected versions.
        update.metadata_effects.clear();
        update.transaction.objects.push(successor);
        assert!(validate_new_counted_publication(&tables, &update).is_ok());
    }

    #[test]
    fn maintained_count_absence_preserves_historical_prepared_encoding() {
        let mut object = crate::directory_object("/legacy", NodeId::generate());
        object.namespace_link_count = None;
        let original = payload(object);
        let encoded = serde_json::to_vec(&original).unwrap();
        assert!(!String::from_utf8_lossy(&encoded).contains("namespace_link_count"));
        let decoded: DfsPreparedOperationPayload = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            original.canonical_digest().unwrap(),
            decoded.canonical_digest().unwrap()
        );
        assert_eq!(encoded, serde_json::to_vec(&decoded).unwrap());
        assert!(
            validate_new_counted_publication(
                &crate::metadata_tables::DfsMetadataTables::default(),
                &decoded,
            )
            .is_err()
        );
    }

    #[test]
    fn maintained_count_kind_changes_cannot_bypass_namespace_admission() {
        let mut original = crate::directory_object("/target", NodeId::generate());
        original.kind = DfsObjectKind::File;
        original.namespace_link_count = Some(2);
        let mut tables = crate::metadata_tables::DfsMetadataTables::default();
        tables
            .objects
            .insert(original.object_id.clone(), original.clone());
        let mut alias = original.clone();
        alias.kind = DfsObjectKind::Hardlink;
        alias.namespace_link_count = None;
        alias.version = DfsVersion::generate();
        alias.link_target_object_id = Some(DfsObjectId::generate());
        assert!(validate_new_counted_publication(&tables, &payload(alias)).is_err());
        let mut retired = original;
        retired.kind = DfsObjectKind::Tombstone;
        retired.namespace_link_count = Some(0);
        retired.version = DfsVersion::generate();
        assert!(validate_new_counted_publication(&tables, &payload(retired)).is_err());
    }
}
