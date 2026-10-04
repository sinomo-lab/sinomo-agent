//! History validation at module response boundaries.

use crate::*;

pub(crate) fn file_restore_job_id(requester: &NodeId, operation_id: &str) -> String {
    let key = format!("{requester}\0{operation_id}");
    format!("file-restore:{}", blake3::hash(key.as_bytes()).to_hex())
}

pub(crate) fn validate_file_version_record(
    version: &DfsObjectVersionRecord,
) -> Result<(), MetadataBackendError> {
    if let Some(plan) = &version.content_durability {
        validate_replication_plan(&plan.desired_replica_node_ids, &plan.policy)
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        if version.content_hash.is_none() || version.size_bytes.is_none() {
            return Err(MetadataBackendError::Serde(
                "content durability requires immutable content identity".into(),
            ));
        }
    }
    version
        .validate_extension_metadata()
        .map_err(MetadataBackendError::Serde)?;
    crate::metadata::capture_from_version(version)?;
    Ok(())
}
