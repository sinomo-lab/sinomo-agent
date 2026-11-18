//! Which shard's conversion takes the rows, operations and native work of a
//! combined copy that the scope mapping does not place in one shard by path.
//!
//! Each rule is decided from the copy alone, so every shard that captures
//! the copy decides it the same way, and `root`'s own capture keeps the
//! whole copy as it stands:
//!
//! - Original native work goes with the shard that owns it
//!   ([`keeps_native_work`]). A request belongs to its original authority
//!   shard and a pending adoption also to the current coordinator of its
//!   move, as the complete-filesystem mapping attributes them; a prepared
//!   adoption goes with its prepared operation.
//! - An unsettled original operation belongs to the shard that owns it
//!   ([`unsettled_operation_owner`]): only that shard's authority commits or
//!   aborts it, and it applies the outcome to that shard's own rows, so
//!   another shard's rows are what the capture holds whatever the outcome.
//!   That shard's conversion binds it with the rest of its own work.
//! - A retry floor or outcome receipt of an object no captured row places in
//!   any shard is kept by every shard ([`keep_shared_row`]). Such a row only
//!   refuses or answers a retry, so a shard holding it for an object it does
//!   not hold loses nothing, and the shard that owns the path a retry names
//!   always holds it. The lock aggregate of such an object stays in the copy:
//!   the object is in no shard's namespace, so no lock request reaches it.
use super::*;
use crate::metadata::scope_mapping::RequiredNativeWork;
use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsContentReferenceOutcomeRetention, DfsMetadataTableSnapshotTables, DfsObjectId,
    DfsPreparedOperationStatus, DfsWriteIntentReceipt,
};

/// Whether the capture of `target` keeps the original native work `work`.
/// `jobs` holds the copy's namespace jobs, which name a pending adoption's
/// coordinator, and `operations` the original operations the capture keeps.
/// Work no rule attributes is kept, and stays unresolved.
pub(super) fn keeps_native_work(
    work: &RequiredNativeWork,
    jobs: &DfsMetadataTableSnapshotTables,
    assignments: &[DfsMetadataShardConfig],
    target: &DfsMetadataShardConfig,
    operations: &[Value],
) -> bool {
    if work.family == "prepared_operations.old_adoption" {
        return operations
            .iter()
            .any(|operation| operation.get("source_adoption_original") == Some(&work.source_row));
    }
    crate::metadata::scope_mapping::keeps_request_capsule(work, jobs, assignments, target)
        .unwrap_or(true)
}

/// A scope as an original operation records it.
#[derive(Deserialize)]
struct RecordedScope {
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
}

/// The catalog shard that owns the unsettled original operation `row`, an
/// operation of the copy or the mapping's row for it: its witnessed original
/// scope, otherwise the scope its own body names. A write intent carries no
/// witnessed scope, so it belongs to the one shard its object belongs to.
/// `None` for a settled operation, and for one whose owner is unknown or not
/// in the catalog; such an operation stays with every capture that keeps it.
pub(super) fn unsettled_operation_owner<'a>(
    row: &Value,
    assignments: &'a [DfsMetadataShardConfig],
) -> Option<&'a DfsMetadataShardConfig> {
    let status = row.get("source_status").or_else(|| row.get("status"))?;
    if serde_json::from_value::<DfsPreparedOperationStatus>(status.clone()).ok()?
        != DfsPreparedOperationStatus::Prepared
    {
        return None;
    }
    let scope: RecordedScope = if let Some(scope) = row.get("original_source_scope") {
        serde_json::from_value(scope.clone()).ok()?
    } else if row.get("source_intent_only").and_then(Value::as_bool) == Some(true) {
        let scopes: Vec<RecordedScope> =
            serde_json::from_value(row.get("target_scopes")?.clone()).ok()?;
        let [scope] = <[RecordedScope; 1]>::try_from(scopes).ok()?;
        scope
    } else {
        serde_json::from_value(
            row.get("source_payload")
                .or_else(|| row.get("payload"))?
                .clone(),
        )
        .ok()?
    };
    assignments.iter().find(|config| {
        config.file_system_id == scope.file_system_id && config.shard_id == scope.shard_id.0
    })
}

/// Keep in `tables` the row of `work` when it is a retry floor or outcome
/// receipt the mapping could not place, as every capture of the copy does.
/// Returns whether `work` was such a row or a lock aggregate left in the copy.
pub(super) fn keep_shared_row(
    tables: &mut DfsMetadataTableSnapshotTables,
    work: &RequiredNativeWork,
    target: &DfsMetadataShardConfig,
) -> Result<bool> {
    fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T> {
        serde_json::from_value(value.clone()).map_err(error)
    }
    let row = &work.source_row;
    let retention = &mut tables.intent_receipt_retention;
    match work.family.as_str() {
        "intent_receipts" => {
            let receipt: DfsWriteIntentReceipt = decode(row)?;
            if receipt.file_system_id == target.file_system_id {
                tables.intent_receipts.push(receipt);
            }
        }
        "intent_receipt_retention.max_evicted_counter_by_object_and_coordinator" => {
            let floors: BTreeMap<NodeId, u64> = decode(&row["state"])?;
            let kept = retention
                .max_evicted_counter_by_object_and_coordinator
                .entry(decode(&row["object_id"])?)
                .or_default();
            for (coordinator, floor) in floors {
                let current = kept.entry(coordinator).or_insert(floor);
                *current = (*current).max(floor);
            }
        }
        "intent_receipt_retention.max_evicted_operation_order_token_by_object_and_coordinator" => {
            let floors: BTreeMap<NodeId, String> = decode(&row["state"])?;
            let kept = retention
                .max_evicted_operation_order_token_by_object_and_coordinator
                .entry(decode(&row["object_id"])?)
                .or_default();
            for (coordinator, floor) in floors {
                let current = kept.entry(coordinator).or_insert_with(|| floor.clone());
                if floor > *current {
                    *current = floor;
                }
            }
        }
        "intent_receipt_retention.opaque_objects" => {
            let object: DfsObjectId = decode(row)?;
            retention
                .opaque_intent_history_evicted_object_ids
                .insert(object);
        }
        "content_reference_outcome_retention" => {
            let state: DfsContentReferenceOutcomeRetention = decode(&row["state"])?;
            if state.file_system_id == target.file_system_id {
                tables
                    .namespace_receipt_retention
                    .content_reference_outcome_retention
                    .entry(decode(&row["object_id"])?)
                    .or_insert(state);
            }
        }
        "advisory_lock_states" => {}
        _ => return Ok(false),
    }
    Ok(true)
}
