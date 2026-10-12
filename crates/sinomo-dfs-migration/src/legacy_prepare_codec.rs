//! Source-only packed expansion and current conversion-image scope checks.
pub(crate) mod original;
use crate::legacy_wire::DfsNamespacePrepareRecord;
use serde::{Deserialize, Serialize};
use sinomo_dfs::DfsFileSystemId;
use time::{OffsetDateTime, UtcOffset};
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PackedTimeV2 {
    #[serde(rename = "s")]
    unix_seconds: i64,
    #[serde(rename = "n")]
    nanosecond: u32,
    #[serde(rename = "o", default)]
    offset_seconds: i32,
}
fn unpack_time(packed: PackedTimeV2, _field: &str, _row: usize) -> Result<OffsetDateTime, String> {
    let instant = OffsetDateTime::from_unix_timestamp(packed.unix_seconds)
        .map_err(|error| error.to_string())?
        .replace_nanosecond(packed.nanosecond)
        .map_err(|error| error.to_string())?;
    let offset =
        UtcOffset::from_whole_seconds(packed.offset_seconds).map_err(|error| error.to_string())?;
    Ok(instant.to_offset(offset))
}
pub(crate) fn validate_namespace_prepare_file_system_scope(
    prepare: &DfsNamespacePrepareRecord,
    expected: &DfsFileSystemId,
) -> Result<(), String> {
    prepare.validate_default_layout_migration_fence()?;
    require_file_system("prepare", expected, &prepare.file_system_id)?;
    require_file_system("transaction", expected, &prepare.transaction.file_system_id)?;
    if let Some(pins) = &prepare.transaction.file_version_pins {
        crate::legacy_wire::validate_pin_scope(pins, expected)?;
    }
    for (index, object) in prepare.transaction.objects.iter().enumerate() {
        require_indexed_file_system(
            "transaction.objects",
            index,
            expected,
            &object.file_system_id,
        )?;
    }
    for (index, version) in prepare.transaction.versions.iter().enumerate() {
        if let Some(plan) = &version.content_durability {
            sinomo_dfs::validate_replication_plan(&plan.desired_replica_node_ids, &plan.policy)
                .map_err(|error| error.to_string())?;
            if version.content_hash.is_none() || version.size_bytes.is_none() {
                return Err("content durability requires immutable content identity".into());
            }
        }
        version.validate_extension_metadata()?;
        require_indexed_file_system(
            "transaction.versions",
            index,
            expected,
            &version.file_system_id,
        )?;
    }
    for (index, lock) in prepare.transaction.advisory_lock_states.iter().enumerate() {
        require_indexed_file_system(
            "transaction.advisory_lock_states",
            index,
            expected,
            &lock.file_system_id,
        )?;
    }
    for (index, receipt) in prepare.content_receipts.iter().enumerate() {
        require_indexed_file_system(
            "content_receipts",
            index,
            expected,
            &receipt.receipt.file_system_id,
        )?;
    }
    if let Some(request) = &prepare.completed_request {
        require_file_system("completed_request", expected, &request.file_system_id)?;
    }
    if let Some(completion) = &prepare.completed_request_completion {
        require_file_system(
            "completed_request_completion",
            expected,
            &completion.file_system_id,
        )?;
    }
    Ok(())
}

fn require_indexed_file_system(
    field: &str,
    index: usize,
    expected: &DfsFileSystemId,
    actual: &DfsFileSystemId,
) -> Result<(), String> {
    require_file_system(&format!("{field}[{index}]"), expected, actual)
}
fn require_file_system(
    field: &str,
    expected: &DfsFileSystemId,
    actual: &DfsFileSystemId,
) -> Result<(), String> {
    if expected == actual {
        Ok(())
    } else {
        Err(format!(
            "source {field} crosses filesystem {expected} -> {actual}"
        ))
    }
}
