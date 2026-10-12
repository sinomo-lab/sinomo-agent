//! History owns the interpretation of its opaque version namespace.

use crate::*;

pub fn capture_from_version(
    version: &DfsObjectVersionRecord,
) -> Result<Option<DfsRetainedFileVersion>, MetadataBackendError> {
    let Some(grant) = version.retention_grant(HISTORY_MODULE_ID) else {
        return Ok(None);
    };
    let source = if let Some(metadata) = version.module_metadata.get(HISTORY_MODULE_ID) {
        if metadata.schema_version != 1 {
            return Err(MetadataBackendError::Serde(
                "unsupported History metadata version".into(),
            ));
        }
        let source = metadata.payload.clone();
        serde_json::from_value(source)
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?
    } else if grant.origin == DfsRetentionGrantOrigin::Rule {
        DfsFileVersionSource::RetentionRule
    } else {
        return Err(MetadataBackendError::Serde(
            "History capture has no module provenance".into(),
        ));
    };
    let capture = DfsRetainedFileVersion {
        object: grant.object.clone(),
        captured_at_unix_nanos: grant.granted_at_unix_nanos,
        source,
        policy_capture: grant
            .minimum
            .as_ref()
            .map(|minimum| DfsFileVersionPolicyCapture {
                order: minimum.order,
                captured_at_unix_nanos: minimum.granted_at_unix_nanos,
                retention: DfsFileHistoryRetention {
                    keep_last: minimum.minimum.keep_last,
                    keep_for_seconds: minimum.minimum.keep_for_seconds,
                },
            }),
    };
    capture
        .validate_for_version(version)
        .map_err(MetadataBackendError::Serde)?;
    Ok(Some(capture))
}

pub(crate) fn entry_from_descriptor(
    descriptor: DfsRetainedRevisionDescriptor,
) -> Result<DfsFileHistoryEntry, MetadataBackendError> {
    let version = &descriptor.version;
    let captured = capture_from_version(version)?.ok_or_else(|| {
        MetadataBackendError::Serde("History query returned another namespace's revision".into())
    })?;
    let pins = descriptor
        .references
        .iter()
        .filter_map(|reference| {
            if reference.namespace != HISTORY_MODULE_ID {
                return None;
            }
            reference
                .pin_id
                .strip_prefix(&format!("ref/{HISTORY_MODULE_ID}/"))
                .map(str::to_string)
        })
        .collect();
    Ok(DfsFileHistoryEntry {
        identity: DfsFileVersionIdentity {
            file_system_id: version.file_system_id.clone(),
            object_id: version.object_id.clone(),
            version: version.version.clone(),
        },
        captured,
        content_hash: version.content_hash.clone().ok_or_else(|| {
            MetadataBackendError::Serde("History revision has no content hash".into())
        })?,
        size_bytes: version.size_bytes.ok_or_else(|| {
            MetadataBackendError::Serde("History revision has no content size".into())
        })?,
        pins,
        is_current: descriptor.current_object.kind != DfsObjectKind::Tombstone
            && descriptor.current_object.version == version.version,
        current_path: normalize_semantic_path_lossy(&descriptor.current_object.path),
        deleted: descriptor.current_object.kind == DfsObjectKind::Tombstone,
    })
}

pub fn core_policy(policy: &DfsFileHistoryPolicy) -> DfsRetentionPolicy {
    DfsRetentionPolicy {
        rules: policy
            .rules
            .iter()
            .map(|rule| DfsRetentionRule {
                path: rule.path.clone(),
                minimum: DfsRetentionMinimum {
                    keep_last: rule.retention.keep_last,
                    keep_for_seconds: rule.retention.keep_for_seconds,
                },
            })
            .collect(),
    }
}

pub fn history_policy(policy: Option<&DfsRetentionPolicy>) -> DfsFileHistoryPolicy {
    DfsFileHistoryPolicy {
        rules: policy
            .map(|policy| {
                policy
                    .rules
                    .iter()
                    .map(|rule| DfsFileHistoryRule {
                        path: rule.path.clone(),
                        retention: DfsFileHistoryRetention {
                            keep_last: rule.minimum.keep_last,
                            keep_for_seconds: rule.minimum.keep_for_seconds,
                        },
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

/// Set the History namespace without changing other configured contributions.
pub fn install_history_policy(
    file_system: &mut DfsFileSystem,
    policy: DfsFileHistoryPolicy,
) -> Result<(), MetadataBackendError> {
    policy.validate().map_err(MetadataBackendError::Serde)?;
    if policy.is_empty() {
        file_system.retention_policies.remove(HISTORY_MODULE_ID);
    } else {
        file_system
            .retention_policies
            .insert(HISTORY_MODULE_ID.into(), core_policy(&policy));
    }
    Ok(())
}

/// Construct current module-owned revision metadata without publishing it.
/// Core publication still validates scope and immutable grants.
pub fn install_history_capture(
    version: &mut DfsObjectVersionRecord,
    capture: DfsRetainedFileVersion,
    high_water: u64,
) -> Result<(), MetadataBackendError> {
    let state = version
        .retention
        .entry(HISTORY_MODULE_ID.into())
        .or_default();
    state.order = state.order.max(high_water);
    if let Some(minimum) = &capture.policy_capture {
        state.order = state.order.max(minimum.order);
    }
    state.grant = Some(Box::new(DfsRevisionRetentionGrant {
        object: capture.object.clone(),
        granted_at_unix_nanos: capture.captured_at_unix_nanos,
        origin: if matches!(capture.source, DfsFileVersionSource::RetentionRule) {
            DfsRetentionGrantOrigin::Rule
        } else {
            DfsRetentionGrantOrigin::ExplicitOperation
        },
        minimum: capture
            .policy_capture
            .as_ref()
            .map(|minimum| DfsOrderedRetentionGrant {
                order: minimum.order,
                granted_at_unix_nanos: minimum.captured_at_unix_nanos,
                minimum: DfsRetentionMinimum {
                    keep_last: minimum.retention.keep_last,
                    keep_for_seconds: minimum.retention.keep_for_seconds,
                },
            }),
    }));
    version.module_metadata.insert(
        HISTORY_MODULE_ID.into(),
        DfsModuleMetadataValue {
            schema_version: 1,
            payload: serde_json::to_value(&capture.source)
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))?,
        },
    );
    version
        .validate_extension_metadata()
        .map_err(MetadataBackendError::Serde)
}
