//! Admission checks for newly authored maintained-count metadata. Current
//! immutable prepared bodies retain their original digest. Legacy
//! conversion and count initialization belong to sinomo-dfs-migration.
use crate::*;

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
