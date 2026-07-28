//! Durable DFS metadata shard backend.
//!
//! This is the first concrete metadata-plane surface. It intentionally stores
//! file/object metadata outside `sinomo-kv`: materialized shard state lives in
//! packed table batches, and every mutation is mirrored into an append-only
//! journal that can later be replicated or replayed by metadata shard peers.

use crate::DfsNamespaceResult;
#[cfg(test)]
use crate::DfsReplicationPolicy;
use crate::advisory_lock::{
    DfsAdvisoryLockApplyResult, DfsAdvisoryLockCommand, DfsAdvisoryLockStateRecord,
    DfsAdvisoryLockTransitionContext, transition_advisory_lock_state,
};
#[cfg(test)]
use crate::control_plane::DfsMetadataShardConfig;
use crate::metadata_api::{
    DfsMetadataBackend, DfsMetadataReplicaIngestPermit, DfsMetadataShardWriteFreeze,
    namespace_prepare_expectations_from_lookups,
};
use crate::metadata_cursor;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::{create_dir_all, read_sobj_optional, write_sobj_atomic};
#[cfg(test)]
use crate::metadata_io::{read_dir, read_sobj, remove_file_if_exists, sync_dir};
#[cfg(test)]
use crate::metadata_journal::DfsMetadataJournalHead;
#[cfg(test)]
use crate::metadata_journal::persist_replayed_journal_entry;
use crate::metadata_journal::{
    DfsMetadataReplayCursor, bounded_metadata_journal_batch, durable_journal_entries_since_limited,
    journal_entries_since_limited_with_next_sequence, migrate_legacy_journal_entries,
    persist_replayed_journal_entries, read_next_journal_sequence_lenient,
    recover_next_journal_sequence, write_local_journal_entries, write_local_journal_entry,
};
use crate::metadata_keys::normalize_path;
use crate::metadata_limits::{
    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, OBJECT_LOCAL_METADATA_PAGE_SIZE,
};
use crate::metadata_lock::{MetadataWriteLock, metadata_write_lock};
use crate::metadata_namespace_transaction::{
    namespace_transaction_recovery_is_complete, plan_namespace_transaction_recovery_table_ops,
    plan_namespace_transaction_table_ops as namespace_transaction_table_ops,
    preapply_namespace_transaction_table_ops, validate_namespace_transaction_preconditions,
};
use crate::metadata_object_index::object_index_table_ops_with_previous as plan_object_index_table_ops_with_previous;
use crate::metadata_pages::{
    DfsDirectoryPageEntries, child_objects_for_directory_from_tables,
    child_objects_for_directory_page_entries_from_tables, collect_metadata_pages,
    conflict_records_page_from_tables, conflicts_for_object_from_tables,
    conflicts_for_object_page_from_tables, count_objects_for_file_system_from_tables,
    first_hardlink_to_object_outside_prefix_from_tables, hardlink_count_to_object_from_tables,
    hardlinks_to_object_page_from_tables, intents_for_object_page_from_tables,
    intents_for_object_version_page_from_tables, namespace_jobs_page_from_tables,
    objects_for_file_system_page_from_tables, pending_namespace_requests_page_from_tables,
    receipt_records_page_from_tables, receipts_for_version_page_from_tables,
    version_records_page_from_tables, versions_for_object_page_from_tables,
    write_intents_page_from_tables,
};
use crate::metadata_read_snapshot;
use crate::metadata_receipt::{receipt_for_replica_from_backend, replay_receipt_matches_existing};
use crate::metadata_records::*;
use crate::metadata_replay::{
    hydrate_journal_entry_payloads, journal_entry_has_replay_payloads,
    require_journal_entry_replay_payloads, require_replica_journal_entry_file_system,
    required_replay_payload,
};
use crate::metadata_summary::{
    metadata_summary_for_file_system_from_tables, metadata_summary_from_tables,
};
use crate::metadata_table_apply::{self, TableMaterializationState};
#[cfg(test)]
use crate::metadata_table_store::{
    LEGACY_METADATA_TABLE_SNAPSHOT_FORMAT_VERSION, metadata_table_path,
    metadata_table_snapshot_ops, metadata_table_snapshot_path, truncate_metadata_table_log,
};
use crate::metadata_table_store::{
    METADATA_TABLE_SNAPSHOT_FORMAT_VERSION, archive_metadata_backend_state,
    load_or_migrate_metadata_tables, metadata_table_snapshot_fingerprint,
    metadata_tables_from_table_snapshot, metadata_tables_snapshot, persist_metadata_table_snapshot,
    read_metadata_table_state_lenient,
};
use crate::metadata_tables::{
    DfsMetadataTableRollback, DfsMetadataTables, DfsMetadataTablesSnapshot,
    MetadataTableReadSnapshotCache, MetadataTableSnapshotFingerprint, namespace_prepare_footprint,
};
use crate::metadata_tombstone::{metadata_now_unix_nanos, plan_tombstone_compaction};
use crate::metadata_visible_commit::{
    object_by_path_from_tables, visible_commit_already_materialized,
    visible_commit_record_from_parts, visible_commit_recovery_order, visible_commit_should_recover,
    visible_commit_table_ops, visible_object_location_recovery_ops,
};
use crate::{DfsFileSystem, DfsObjectKind, DfsObjectMetadata, namespace_transaction_id_for_batch};
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
#[cfg(test)]
use sinomo_core::DfsWriteReceipt;
#[cfg(test)]
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
#[cfg(test)]
use sinomo_core::object::serde_ext as sinomo_object_serde;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::io;
#[cfg(test)]
use std::io::Write;
use std::ops::Bound::{Excluded, Included, Unbounded};
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use time::OffsetDateTime;

const DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES: u64 = 64 * 1024 * 1024;

struct PlannedWrittenJournalTableOps {
    applied: usize,
    last_materialized_sequence: u64,
    table_ops: Vec<DfsMetadataTableOp>,
    rollback: DfsMetadataTableRollback,
}

fn metadata_backend_perf_trace(args: std::fmt::Arguments<'_>) {
    if !tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE) {
        return;
    }
    let traceparent = crate::current_dfs_traceparent().unwrap_or_default();
    tracing::trace!(
        target: "sinomo.dfs.perf",
        traceparent = %traceparent,
        detail = %args,
        "dfs metadata backend perf"
    );
}

#[cfg(test)]
thread_local! {
    static METADATA_BEGIN_WRITE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static METADATA_CURRENT_TABLES_SNAPSHOT_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static METADATA_BACKEND_OPEN_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static METADATA_EXPORT_TABLE_SNAPSHOT_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static METADATA_JOURNAL_WRITE_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static NAMESPACE_RECOVERY_PLANNER_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static NAMESPACE_PREPARED_COMMIT_REFERENCE_PLANNER_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static NAMESPACE_PREPARE_HISTORY_SCAN_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_metadata_begin_write_calls() {
    METADATA_BEGIN_WRITE_CALLS.set(0);
}

#[cfg(test)]
fn metadata_begin_write_calls() -> usize {
    METADATA_BEGIN_WRITE_CALLS.get()
}

#[cfg(test)]
pub(crate) fn reset_metadata_current_tables_snapshot_calls() {
    METADATA_CURRENT_TABLES_SNAPSHOT_CALLS.set(0);
}

#[cfg(test)]
pub(crate) fn metadata_current_tables_snapshot_calls() -> usize {
    METADATA_CURRENT_TABLES_SNAPSHOT_CALLS.get()
}

#[cfg(test)]
pub(crate) fn reset_metadata_backend_open_calls() {
    METADATA_BACKEND_OPEN_CALLS.set(0);
}

#[cfg(test)]
pub(crate) fn metadata_backend_open_calls() -> usize {
    METADATA_BACKEND_OPEN_CALLS.get()
}

#[cfg(test)]
pub(crate) fn reset_metadata_export_table_snapshot_calls() {
    METADATA_EXPORT_TABLE_SNAPSHOT_CALLS.set(0);
}

#[cfg(test)]
pub(crate) fn metadata_export_table_snapshot_calls() -> usize {
    METADATA_EXPORT_TABLE_SNAPSHOT_CALLS.get()
}

#[cfg(test)]
pub(crate) fn reset_metadata_journal_write_count() {
    METADATA_JOURNAL_WRITE_COUNT.set(0);
}

#[cfg(test)]
pub(crate) fn metadata_journal_write_count() -> usize {
    METADATA_JOURNAL_WRITE_COUNT.get()
}

#[cfg(test)]
fn reset_namespace_recovery_planner_calls() {
    NAMESPACE_RECOVERY_PLANNER_CALLS.set(0);
}

#[cfg(test)]
fn namespace_recovery_planner_calls() -> usize {
    NAMESPACE_RECOVERY_PLANNER_CALLS.get()
}

#[cfg(test)]
fn reset_namespace_prepared_commit_reference_planner_calls() {
    NAMESPACE_PREPARED_COMMIT_REFERENCE_PLANNER_CALLS.set(0);
}

#[cfg(test)]
fn namespace_prepared_commit_reference_planner_calls() -> usize {
    NAMESPACE_PREPARED_COMMIT_REFERENCE_PLANNER_CALLS.get()
}

#[cfg(test)]
fn reset_namespace_prepare_history_scan_calls() {
    NAMESPACE_PREPARE_HISTORY_SCAN_CALLS.set(0);
}

#[cfg(test)]
fn namespace_prepare_history_scan_calls() -> usize {
    NAMESPACE_PREPARE_HISTORY_SCAN_CALLS.get()
}

fn namespace_prepare_payload_matches(
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
}

fn namespace_prepare_rebind_payload_matches(
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
        && left.status == DfsNamespacePrepareStatus::Prepared
        && right.status == DfsNamespacePrepareStatus::Prepared
}

fn namespace_request_immutable_payload_matches(
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

fn completed_namespace_request_outcome_matches(
    left: &DfsNamespaceRequestRecord,
    right: &DfsNamespaceRequestRecord,
) -> bool {
    namespace_request_immutable_payload_matches(left, right)
        && left.status == DfsNamespaceRequestStatus::Completed
        && right.status == DfsNamespaceRequestStatus::Completed
        && left.result == right.result
        && left.posix_coordination_result == right.posix_coordination_result
}

fn validate_namespace_request_shape(request: &DfsNamespaceRequestRecord) -> Result<()> {
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

fn namespace_request_transition_is_valid(
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

fn pending_namespace_request_admission_from_completion(
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

fn namespace_prepare_can_repair_terminal_request(
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

fn prepared_write_intent_rebind_payload_matches(
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

fn validate_namespace_prepare_shape(
    tables: &DfsMetadataTables,
    shard_id: &DfsMetadataShardId,
    prepare: &DfsNamespacePrepareRecord,
) -> Result<()> {
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
    let transaction_object_ids = prepare
        .transaction
        .objects
        .iter()
        .map(|object| object.object_id.clone())
        .chain(
            prepare
                .transaction
                .versions
                .iter()
                .map(|version| version.object_id.clone()),
        )
        .chain(
            prepare
                .transaction
                .advisory_lock_states
                .iter()
                .map(|state| state.object_id.clone()),
        )
        .collect::<BTreeSet<_>>();
    if let Some(object_id) = transaction_object_ids
        .iter()
        .find(|object_id| !expected_object_ids.contains(*object_id))
    {
        return Err(MetadataBackendError::Serde(format!(
            "namespace prepare {} is missing the version expectation for object {}",
            prepare.operation_id, object_id
        )));
    }

    let mut expected_paths = BTreeSet::new();
    for expected in &prepare.expected_path_occupants {
        let path = normalize_path(&expected.path);
        if !expected_paths.insert(path.clone()) {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} repeats path expectation {}",
                prepare.operation_id, path
            )));
        }
    }
    if let Some(path) = prepare
        .transaction
        .objects
        .iter()
        .map(|object| normalize_path(&object.path))
        .find(|path| !expected_paths.contains(path))
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
        .map(|object| (object.object_id.clone(), object))
        .collect::<BTreeMap<_, _>>();
    let transaction_versions = prepare
        .transaction
        .versions
        .iter()
        .map(|version| {
            (
                (version.object_id.clone(), version.version.clone()),
                version,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut receipt_keys = BTreeSet::new();
    for receipt in &prepare.content_receipts {
        let key = (
            receipt.receipt.object_id.clone(),
            receipt.receipt.committed_version.clone(),
            receipt.replica_node_id.clone(),
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
        let Some(object) = transaction_objects.get(&receipt.receipt.object_id) else {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} has a content receipt for object {} outside its transaction",
                prepare.operation_id, receipt.receipt.object_id
            )));
        };
        let version_key = (
            receipt.receipt.object_id.clone(),
            receipt.receipt.committed_version.clone(),
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
    if prepare.operation_id.starts_with("namespace-request:") && prepare.completed_request.is_none()
    {
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
                None if prepare.status == DfsNamespacePrepareStatus::Prepared => {
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
    Ok(())
}

fn namespace_prepare_request_completion_is_materialized(
    tables: &DfsMetadataTables,
    prepare: &DfsNamespacePrepareRecord,
) -> bool {
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
    })
}

fn namespace_prepare_content_receipts_are_complete(
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

fn missing_namespace_prepare_content_receipt_ops(
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

fn validate_namespace_prepare_expectations(
    tables: &DfsMetadataTables,
    prepare: &DfsNamespacePrepareRecord,
) -> Result<()> {
    for expected in &prepare.expected_object_versions {
        let current = tables
            .objects
            .get(&expected.object_id)
            .map(|object| object.version.clone());
        if current != expected.expected_version {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} object {} version expectation changed: expected {:?}, current {:?}",
                prepare.operation_id, expected.object_id, expected.expected_version, current
            )));
        }
    }
    for expected in &prepare.expected_path_occupants {
        let current = object_by_path_from_tables(tables, &prepare.file_system_id, &expected.path)
            .map(|object| object.object_id);
        if current != expected.expected_object_id {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} path {} occupant expectation changed: expected {:?}, current {:?}",
                prepare.operation_id, expected.path, expected.expected_object_id, current
            )));
        }
    }
    Ok(())
}

fn validate_namespace_prepare_does_not_overlap(
    tables: &DfsMetadataTables,
    prepare: &DfsNamespacePrepareRecord,
) -> Result<()> {
    let (object_ids, paths) = namespace_prepare_footprint(prepare);
    for object_id in &object_ids {
        if let Some(existing_operation_id) = tables.namespace_prepare_object_owner_other_than(
            &prepare.file_system_id,
            object_id,
            &prepare.operation_id,
        ) {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} overlaps active prepare {}",
                prepare.operation_id, existing_operation_id
            )));
        }
    }
    for path in &paths {
        if let Some(existing_operation_id) = tables
            .namespace_prepare_normalized_path_owner_other_than(
                &prepare.file_system_id,
                path,
                &prepare.operation_id,
            )
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} overlaps active prepare {}",
                prepare.operation_id, existing_operation_id
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
fn validate_namespace_prepare_does_not_overlap_reference(
    tables: &DfsMetadataTables,
    prepare: &DfsNamespacePrepareRecord,
) -> Result<()> {
    NAMESPACE_PREPARE_HISTORY_SCAN_CALLS
        .set(NAMESPACE_PREPARE_HISTORY_SCAN_CALLS.get().saturating_add(1));
    let (object_ids, paths) = namespace_prepare_footprint(prepare);
    for existing in tables.namespace_prepares.values().filter(|existing| {
        existing.status == DfsNamespacePrepareStatus::Prepared
            && existing.operation_id != prepare.operation_id
            && existing.file_system_id == prepare.file_system_id
    }) {
        let (existing_object_ids, existing_paths) = namespace_prepare_footprint(existing);
        if !object_ids.is_disjoint(&existing_object_ids) || !paths.is_disjoint(&existing_paths) {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} overlaps active prepare {}",
                prepare.operation_id, existing.operation_id
            )));
        }
    }
    Ok(())
}

fn namespace_prepare_table_ops_for_record(
    tables: &DfsMetadataTables,
    shard_id: &DfsMetadataShardId,
    prepare: &DfsNamespacePrepareRecord,
) -> Result<Vec<DfsMetadataTableOp>> {
    validate_namespace_prepare_shape(tables, shard_id, prepare)?;
    let existing = tables.namespace_prepares.get(&prepare.operation_id);
    match prepare.status {
        DfsNamespacePrepareStatus::Prepared => {
            if let Some(existing) = existing {
                if !namespace_prepare_payload_matches(existing, prepare) {
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
            validate_namespace_prepare_expectations(tables, prepare)?;
            validate_namespace_prepare_does_not_overlap(tables, prepare)?;
            validate_namespace_transaction_preconditions(tables, &prepare.transaction)?;
            let mut ops = Vec::with_capacity(2);
            if let Some(completion) = prepare.completed_request.as_ref() {
                let request_key = (
                    completion.requester_node_id.clone(),
                    completion.request_id.clone(),
                );
                if !tables.namespace_requests.contains_key(&request_key) {
                    ops.push(DfsMetadataTableOp::PutNamespaceRequest {
                        request: pending_namespace_request_admission_from_completion(completion),
                    });
                }
            }
            ops.push(DfsMetadataTableOp::PutNamespacePrepare {
                prepare: prepare.clone(),
            });
            Ok(ops)
        }
        DfsNamespacePrepareStatus::Committed => {
            let existing = existing.ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "namespace prepare {} cannot commit before it is prepared",
                    prepare.operation_id
                ))
            })?;
            if !namespace_prepare_payload_matches(existing, prepare) {
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
                && existing == prepare
                && namespace_transaction_recovery_is_complete(tables, &prepare.transaction)
                && namespace_prepare_content_receipts_are_complete(tables, prepare)
                && namespace_prepare_request_completion_is_materialized(tables, prepare)
            {
                return Ok(Vec::new());
            }
            let mut ops = missing_namespace_prepare_content_receipt_ops(tables, prepare);
            if existing.status == DfsNamespacePrepareStatus::Committed {
                ops.extend(plan_namespace_transaction_recovery_table_ops(
                    tables,
                    &prepare.transaction,
                ));
            } else {
                validate_namespace_prepare_expectations(tables, prepare)?;
                validate_namespace_prepare_does_not_overlap(tables, prepare)?;
                validate_namespace_transaction_preconditions(tables, &prepare.transaction)?;
                #[cfg(test)]
                NAMESPACE_PREPARED_COMMIT_REFERENCE_PLANNER_CALLS.set(
                    NAMESPACE_PREPARED_COMMIT_REFERENCE_PLANNER_CALLS
                        .get()
                        .saturating_add(1),
                );
                ops.extend(namespace_transaction_table_ops(
                    tables,
                    &prepare.transaction,
                ));
            }
            ops.push(DfsMetadataTableOp::PutNamespacePrepare {
                prepare: prepare.clone(),
            });
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
                    ops.push(DfsMetadataTableOp::PutNamespaceRequest {
                        request: request.clone(),
                    });
                }
            }
            Ok(ops)
        }
        DfsNamespacePrepareStatus::Aborted => {
            let existing = existing.ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "namespace prepare {} cannot abort before it is prepared",
                    prepare.operation_id
                ))
            })?;
            if !namespace_prepare_payload_matches(existing, prepare) {
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
            if existing == prepare {
                return Ok(Vec::new());
            }
            Ok(vec![DfsMetadataTableOp::PutNamespacePrepare {
                prepare: prepare.clone(),
            }])
        }
    }
}

fn require_replica_snapshot_record_file_system(
    record_kind: &str,
    actual: &DfsFileSystemId,
    expected: &DfsFileSystemId,
) -> Result<()> {
    if actual == expected {
        return Ok(());
    }
    Err(MetadataBackendError::Serde(format!(
        "replicated metadata snapshot {record_kind} filesystem {actual} does not match permit filesystem {expected}"
    )))
}

fn require_replica_snapshot_namespace_transaction_file_system(
    record_kind: &str,
    transaction: &DfsNamespaceTransaction,
    expected: &DfsFileSystemId,
) -> Result<()> {
    require_replica_snapshot_record_file_system(
        record_kind,
        &transaction.file_system_id,
        expected,
    )?;
    for object in &transaction.objects {
        require_replica_snapshot_record_file_system(
            "namespace transaction object",
            &object.file_system_id,
            expected,
        )?;
    }
    for version in &transaction.versions {
        require_replica_snapshot_record_file_system(
            "namespace transaction version",
            &version.file_system_id,
            expected,
        )?;
    }
    for state in &transaction.advisory_lock_states {
        require_replica_snapshot_record_file_system(
            "namespace transaction advisory-lock state",
            &state.file_system_id,
            expected,
        )?;
    }
    Ok(())
}

/// Validate the complete canonical table image before replica snapshot ingest
/// acquires the backend writer lock. Derived indexes are included so a legacy
/// op snapshot and a structured snapshot are checked at the same boundary.
fn require_replica_snapshot_tables_file_system(
    tables: &DfsMetadataTables,
    expected: &DfsFileSystemId,
) -> Result<()> {
    for file_system in tables.file_systems.values() {
        require_replica_snapshot_record_file_system(
            "filesystem record",
            &file_system.file_system_id,
            expected,
        )?;
    }
    for object in tables.objects.values() {
        require_replica_snapshot_record_file_system(
            "object record",
            &object.file_system_id,
            expected,
        )?;
    }
    for path_record in tables.file_system_objects.values() {
        require_replica_snapshot_record_file_system(
            "filesystem-object index",
            &path_record.file_system_id,
            expected,
        )?;
    }
    for path_record in tables.paths.values() {
        require_replica_snapshot_record_file_system(
            "path record",
            &path_record.file_system_id,
            expected,
        )?;
    }
    for inode in tables.inodes.values() {
        require_replica_snapshot_record_file_system(
            "inode record",
            &inode.file_system_id,
            expected,
        )?;
    }
    for dirfrag in tables.dirfrags.values() {
        require_replica_snapshot_record_file_system(
            "directory-fragment record",
            &dirfrag.file_system_id,
            expected,
        )?;
    }
    for path_record in tables.dentries.values() {
        require_replica_snapshot_record_file_system(
            "dentry record",
            &path_record.file_system_id,
            expected,
        )?;
    }
    for path_record in tables.hardlinks.values() {
        require_replica_snapshot_record_file_system(
            "hardlink record",
            &path_record.file_system_id,
            expected,
        )?;
    }
    for path_record in tables.hardlink_paths.values() {
        require_replica_snapshot_record_file_system(
            "hardlink-path index",
            &path_record.file_system_id,
            expected,
        )?;
    }
    for object in tables.compacted_tombstones.values() {
        require_replica_snapshot_record_file_system(
            "compacted tombstone",
            &object.file_system_id,
            expected,
        )?;
    }
    for (file_system_id, _) in tables.compacted_tombstone_paths.keys() {
        require_replica_snapshot_record_file_system(
            "compacted-tombstone path index",
            file_system_id,
            expected,
        )?;
    }
    for version in tables.versions.values() {
        require_replica_snapshot_record_file_system(
            "version record",
            &version.file_system_id,
            expected,
        )?;
    }
    for receipt in tables.receipts.values() {
        require_replica_snapshot_record_file_system(
            "receipt record",
            &receipt.receipt.file_system_id,
            expected,
        )?;
    }
    for intent in tables.intents.values() {
        require_replica_snapshot_record_file_system(
            "intent record",
            &intent.file_system_id,
            expected,
        )?;
        if let Some(object) = intent.object_metadata.as_ref() {
            require_replica_snapshot_record_file_system(
                "intent object record",
                &object.file_system_id,
                expected,
            )?;
        }
    }
    for conflict in tables.conflicts.values() {
        require_replica_snapshot_record_file_system(
            "conflict record",
            &conflict.file_system_id,
            expected,
        )?;
    }
    for state in tables.advisory_lock_states.values() {
        require_replica_snapshot_record_file_system(
            "advisory-lock state",
            &state.file_system_id,
            expected,
        )?;
    }
    for commit in tables.visible_commits.values() {
        require_replica_snapshot_record_file_system(
            "visible-commit object record",
            &commit.committed_object.file_system_id,
            expected,
        )?;
        require_replica_snapshot_record_file_system(
            "visible-commit version record",
            &commit.visible_version.file_system_id,
            expected,
        )?;
    }
    for transaction in tables.namespace_transactions.values() {
        require_replica_snapshot_namespace_transaction_file_system(
            "namespace transaction",
            transaction,
            expected,
        )?;
    }
    for receipt in tables.namespace_transaction_receipts.values() {
        require_replica_snapshot_record_file_system(
            "namespace transaction receipt",
            &receipt.file_system_id,
            expected,
        )?;
    }
    for prepare in tables.namespace_prepares.values() {
        require_replica_snapshot_record_file_system(
            "namespace prepare",
            &prepare.file_system_id,
            expected,
        )?;
        require_replica_snapshot_namespace_transaction_file_system(
            "namespace prepare transaction",
            &prepare.transaction,
            expected,
        )?;
    }
    for receipt in tables.namespace_prepare_receipts.values() {
        require_replica_snapshot_record_file_system(
            "namespace prepare receipt",
            &receipt.file_system_id,
            expected,
        )?;
    }
    for job in tables.namespace_jobs.values() {
        require_replica_snapshot_record_file_system(
            "namespace job",
            &job.file_system_id,
            expected,
        )?;
    }
    for request in tables.namespace_requests.values() {
        require_replica_snapshot_record_file_system(
            "namespace request",
            &request.file_system_id,
            expected,
        )?;
    }
    for receipt in tables.namespace_request_receipts.values() {
        require_replica_snapshot_record_file_system(
            "namespace request receipt",
            &receipt.file_system_id,
            expected,
        )?;
    }
    Ok(())
}

/// Shard-local SOBJ metadata backend.
///
/// This backend owns one durable metadata shard: packed materialized tables,
/// append-only mutation journal, replay cursors, and snapshot repair state.
#[cfg(test)]
#[derive(Debug, Default)]
struct MetadataReceiptScanCounters {
    global_pages: AtomicUsize,
    object_version_pages: AtomicUsize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DfsConditionalSourceEviction {
    Applied { sequence: u64 },
    AlreadyApplied,
    JournalAdvanced,
    SourceChanged,
}

#[derive(Clone, Debug)]
pub struct LocalSobjMetadataBackend {
    shard_id: DfsMetadataShardId,
    root: PathBuf,
    next_sequence: u64,
    tables: DfsMetadataTables,
    table_log_offset: u64,
    materialized_through_sequence: u64,
    prefix_proof: Option<DfsMetadataPrefixProof>,
    /// Authority generation observed by this backend instance after a durable
    /// membership handoff activation. This is deliberately instance-local:
    /// another already-open process must not inherit the completing process's
    /// authority merely because both share the same metadata directory.
    instance_authority_epoch: Option<u64>,
    table_snapshot_fingerprint: Option<MetadataTableSnapshotFingerprint>,
    read_snapshot_cache: Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
    #[cfg(test)]
    receipt_scan_counters: Arc<MetadataReceiptScanCounters>,
}

impl LocalSobjMetadataBackend {
    /// Open or initialize one durable metadata shard under `root`.
    ///
    /// Construction performs local recovery before returning: it migrates any
    /// legacy journal layout, loads the materialized table state, replays local
    /// journal entries not yet reflected in tables, and finishes interrupted
    /// namespace or visible-version commits.
    pub fn new(shard_id: DfsMetadataShardId, root: impl Into<PathBuf>) -> Result<Self> {
        #[cfg(test)]
        METADATA_BACKEND_OPEN_CALLS.set(METADATA_BACKEND_OPEN_CALLS.get().saturating_add(1));
        let trace_started = Instant::now();
        let root = root.into();
        let create_dirs_started = Instant::now();
        create_dir_all(root.join("records"))?;
        create_dir_all(root.join("journal"))?;
        create_dir_all(root.join("journal").join("segments"))?;
        let create_dirs_ms = create_dirs_started.elapsed().as_millis();
        let mut backend = Self {
            shard_id,
            root,
            next_sequence: 1,
            tables: DfsMetadataTables::default(),
            table_log_offset: 0,
            materialized_through_sequence: 0,
            prefix_proof: None,
            instance_authority_epoch: None,
            table_snapshot_fingerprint: None,
            read_snapshot_cache: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            receipt_scan_counters: Arc::new(MetadataReceiptScanCounters::default()),
        };
        let local_recovery_started = Instant::now();
        {
            let _lock = backend.metadata_write_lock()?;
            migrate_legacy_journal_entries(&backend.root)?;
            backend.next_sequence = recover_next_journal_sequence(&backend.root)?;
            let state = load_or_migrate_metadata_tables(&backend.root)?;
            let materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
            backend.tables = state.tables;
            backend.table_log_offset = state.table_log_offset;
            backend.materialized_through_sequence = materialized_through_sequence;
            backend.prefix_proof = state.prefix_proof;
            backend.table_snapshot_fingerprint = state.snapshot_fingerprint;
            backend.ensure_local_materialization_cursor()?;
            // Legacy snapshots did not name their materialized journal head.
            // Their durable cursor is the compatibility checkpoint: replay
            // starts after it and every successor is still hash chained.
            if backend.prefix_proof.is_none()
                && state.materialized_through_sequence.is_none()
                && backend.tables.has_materialized_state()
            {
                backend.materialized_through_sequence = backend
                    .local_materialization_cursor()?
                    .last_sequence
                    .min(backend.next_sequence.saturating_sub(1));
            }
            backend.reconcile_local_materialization_cursor(materialized_through_sequence)?;
            let bootstrapped_prefix = backend.ensure_metadata_prefix_proof_checkpoint_unlocked()?;
            backend.rewind_local_materialization_cursor_to_table_head_if_ahead()?;
            backend.recover_unmaterialized_local_journal_entries()?;
            if bootstrapped_prefix
                && (backend.materialized_through_sequence > 0
                    || backend.tables.has_materialized_state())
            {
                backend.persist_current_metadata_checkpoint_unlocked()?;
            }
        }
        let local_recovery_ms = local_recovery_started.elapsed().as_millis();
        let frozen_for_membership_handoff = backend.membership_handoff_write_freeze()?.is_some();
        let (namespace_recovery_ms, visible_recovery_ms, location_recovery_ms) =
            if frozen_for_membership_handoff {
                // Existing journal replay/materialization above is safe and
                // needed for exact snapshots. Recovery below may append new
                // authoritative records, so defer it until control-plane
                // activation explicitly rebinds this backend instance.
                (0, 0, 0)
            } else {
                let namespace_recovery_started = Instant::now();
                backend.recover_namespace_transactions()?;
                let namespace_recovery_ms = namespace_recovery_started.elapsed().as_millis();
                let visible_recovery_started = Instant::now();
                backend.recover_visible_version_commits()?;
                let visible_recovery_ms = visible_recovery_started.elapsed().as_millis();
                let location_recovery_started = Instant::now();
                backend.recover_object_locations_from_visible_versions()?;
                let location_recovery_ms = location_recovery_started.elapsed().as_millis();
                (
                    namespace_recovery_ms,
                    visible_recovery_ms,
                    location_recovery_ms,
                )
            };
        let traceparent = crate::current_dfs_traceparent().unwrap_or_default();
        tracing::trace!(
            target: "sinomo.dfs.perf",
            traceparent = %traceparent,
            shard = %backend.shard_id.0,
            root = %backend.root.display(),
            next_sequence = backend.next_sequence,
            materialized_through_sequence = backend.materialized_through_sequence,
            create_dirs_ms,
            local_recovery_ms,
            namespace_recovery_ms,
            visible_recovery_ms,
            location_recovery_ms,
            total_ms = trace_started.elapsed().as_millis(),
            "metadata-backend-open"
        );
        metadata_table_apply::schedule_metadata_table_checkpoint_if_needed(
            &backend.root,
            DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES,
        )?;
        Ok(backend)
    }

    /// Open a shard for read-only inspection/status.
    ///
    /// Unlike [`Self::new`], this does not take write locks, repair cursors,
    /// migrate legacy tables, or replay recovery transactions. Status RPCs use
    /// this path so probing a live daemon does not turn into a recovery pass.
    pub fn open_for_inspection(
        shard_id: DfsMetadataShardId,
        root: impl Into<PathBuf>,
    ) -> Result<Self> {
        #[cfg(test)]
        METADATA_BACKEND_OPEN_CALLS.set(METADATA_BACKEND_OPEN_CALLS.get().saturating_add(1));
        let root = root.into();
        let state = read_metadata_table_state_lenient(&root)?;
        let materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
        let next_sequence = read_next_journal_sequence_lenient(&root)?
            .max(materialized_through_sequence.saturating_add(1))
            .max(1);
        Ok(Self {
            shard_id,
            root,
            next_sequence,
            tables: state.tables,
            table_log_offset: state.table_log_offset,
            materialized_through_sequence,
            prefix_proof: state.prefix_proof,
            instance_authority_epoch: None,
            table_snapshot_fingerprint: state.snapshot_fingerprint,
            read_snapshot_cache: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            receipt_scan_counters: Arc::new(MetadataReceiptScanCounters::default()),
        })
    }

    /// Refresh a retained read-only inspection handle from the durable table
    /// log without running writer recovery or acquiring authority. Reactive
    /// bounded migration keeps these handles across yielded continuations so
    /// a 256-record page does not rehydrate every shard catalog.
    pub(crate) fn refresh_for_inspection(&mut self) -> Result<()> {
        self.refresh_metadata_tables()?;
        self.next_sequence = self
            .next_sequence
            .max(self.materialized_through_sequence.saturating_add(1))
            .max(1);
        Ok(())
    }

    /// Open the last durable table image without replaying an unreadable
    /// journal. Callers may use this only for a non-primary mirror so the
    /// service can reject the next conflicting batch and complete the normal
    /// authenticated snapshot-repair handshake.
    pub(crate) fn open_for_replica_repair(
        shard_id: DfsMetadataShardId,
        root: impl Into<PathBuf>,
    ) -> Result<Self> {
        let root = root.into();
        create_dir_all(root.join("records"))?;
        create_dir_all(root.join("journal").join("segments"))?;
        let _lock = metadata_write_lock(
            root.clone(),
            metadata_cursor::journal_write_lock_path(&root),
        )?;
        let state = read_metadata_table_state_lenient(&root)?;
        let materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
        let mut backend = Self {
            shard_id,
            root,
            // Ignore the unreadable journal suffix until the primary either
            // sends a clean continuation or replaces it with a snapshot.
            next_sequence: materialized_through_sequence.saturating_add(1).max(1),
            tables: state.tables,
            table_log_offset: state.table_log_offset,
            materialized_through_sequence,
            prefix_proof: state.prefix_proof,
            instance_authority_epoch: None,
            table_snapshot_fingerprint: state.snapshot_fingerprint,
            read_snapshot_cache: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            receipt_scan_counters: Arc::new(MetadataReceiptScanCounters::default()),
        };
        backend.ensure_local_materialization_cursor()?;
        backend.reconcile_local_materialization_cursor(materialized_through_sequence)?;
        backend.ensure_metadata_prefix_proof_checkpoint_unlocked()?;
        Ok(backend)
    }

    /// Metadata shard owned by this local backend handle.
    pub fn shard_id(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }

    #[cfg(test)]
    pub(crate) fn test_receipt_scan_counts(&self) -> (usize, usize) {
        (
            self.receipt_scan_counters
                .global_pages
                .load(Ordering::Relaxed),
            self.receipt_scan_counters
                .object_version_pages
                .load(Ordering::Relaxed),
        )
    }

    #[cfg(test)]
    pub(crate) fn reset_test_receipt_scan_counts(&self) {
        self.receipt_scan_counters
            .global_pages
            .store(0, Ordering::Relaxed);
        self.receipt_scan_counters
            .object_version_pages
            .store(0, Ordering::Relaxed);
    }

    /// Install an exact lock aggregate while migrating an object's metadata
    /// family between local shards.
    ///
    /// Normal lock callers must use `apply_advisory_lock_command`; this path
    /// exists only so a path-based shard move cannot silently drop live lock
    /// state or its fencing history.
    pub(crate) fn install_advisory_lock_state_for_migration(
        &mut self,
        state: &DfsAdvisoryLockStateRecord,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        let (state, source_fingerprint) =
            crate::advisory_lock::prepare_advisory_lock_state_for_migration(state)
                .map_err(|source| MetadataBackendError::AdvisoryLock(source.to_string()))?;
        let object = self.tables.objects.get(&state.object_id).ok_or_else(|| {
            MetadataBackendError::UnknownObject {
                object_id: state.object_id.clone(),
            }
        })?;
        if object.file_system_id != state.file_system_id {
            return Err(MetadataBackendError::AdvisoryLock(format!(
                "migrated lock state for {}/{} does not match target object filesystem {}",
                state.file_system_id, state.object_id, object.file_system_id
            )));
        }
        if let Some(existing) = self.tables.advisory_lock_states.get(&state.object_id) {
            if existing == &state
                || existing
                    .migration_source_fingerprints
                    .contains(&source_fingerprint)
            {
                return Ok(None);
            }
            return Err(MetadataBackendError::AdvisoryLock(format!(
                "target shard {} has conflicting advisory-lock state for object {}",
                self.shard_id.0, state.object_id
            )));
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_advisory_lock_state(&state),
            vec![DfsMetadataTableOp::PutAdvisoryLockState { state }],
        )
        .map(Some)
    }

    /// Persist and materialize a replicated journal batch from a metadata peer.
    ///
    /// The peer is another replica of this backend's logical shard; a batch
    /// naming a different shard is rejected before acquiring the write lock.
    /// Entries whose replay cursor is already past their sequence are skipped.
    /// New entries are written to the local replicated journal, materialized
    /// into table batches, and then acknowledged by advancing replay cursors.
    pub fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize> {
        self.require_replication_source_shard(&batch.shard_id)?;
        let _lock = self.begin_metadata_write()?;
        self.apply_journal_batch_unlocked(batch)
    }

    /// Apply a validated replica batch across only the exact activated author
    /// fence named by `permit`. This path never changes process-local
    /// authority binding.
    pub fn apply_journal_batch_with_replica_permit(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<usize> {
        for entry in &batch.entries {
            require_replica_journal_entry_file_system(entry, permit.file_system_id())?;
        }
        let _lock = self.begin_metadata_replica_ingest(permit, &batch.shard_id)?;
        self.apply_journal_batch_unlocked(batch)
    }

    fn apply_journal_batch_unlocked(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize> {
        if batch.entries.is_empty() {
            return Ok(0);
        }
        for entry in &batch.entries {
            if entry.shard_id != batch.shard_id {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata journal batch for shard {} contains entry from shard {}",
                    batch.shard_id.0, entry.shard_id.0
                )));
            }
            require_journal_entry_replay_payloads(entry)?;
        }

        let mut candidate_prefix_proof = self.prefix_proof.clone().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "metadata shard {} has no exact prefix checkpoint",
                self.shard_id.0
            ))
        })?;
        let replay_cursor_exists =
            metadata_cursor::replay_cursor_path(&self.root, &batch.shard_id).is_file();
        let mut source_cursor = metadata_cursor::read_replay_cursor(&self.root, &batch.shard_id)?;
        if !replay_cursor_exists {
            // This backend and every replicated journal batch share one
            // logical shard identity, so the local durable prefix is the exact
            // baseline even before the checkpointed replay cursor exists.
            source_cursor.last_sequence = candidate_prefix_proof.journal_sequence;
        }
        if source_cursor.last_sequence > candidate_prefix_proof.journal_sequence {
            return Err(MetadataBackendError::Serde(format!(
                "metadata replay cursor for shard {} is at {} beyond durable prefix {}",
                batch.shard_id.0,
                source_cursor.last_sequence,
                candidate_prefix_proof.journal_sequence
            )));
        }
        // A table batch/snapshot can become durable before its replay cursor.
        // The exact prefix is authoritative in that crash window.
        source_cursor.last_sequence = candidate_prefix_proof.journal_sequence;
        let persisted_through_sequence = source_cursor.last_sequence;

        let mut local_cursor = self.local_materialization_cursor()?;
        let mut local_cursor_advanced = false;
        let mut replay_entries = Vec::new();
        for entry in &batch.entries {
            if entry.sequence <= source_cursor.last_sequence {
                continue;
            }
            let expected_sequence = source_cursor.last_sequence.saturating_add(1);
            if entry.sequence != expected_sequence {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata journal batch for shard {} starts unseen history at sequence {} but durable replay requires {}",
                    batch.shard_id.0, entry.sequence, expected_sequence
                )));
            }
            candidate_prefix_proof = self.prefix_proof_after_entry_for_shard(
                &candidate_prefix_proof,
                entry,
                &batch.shard_id,
            )?;
            replay_entries.push(entry);
            if entry.sequence > local_cursor.last_sequence {
                local_cursor.last_sequence = entry.sequence;
                local_cursor_advanced = true;
            }
            source_cursor.last_sequence = entry.sequence;
        }

        // Include already-acknowledged entries in the idempotence check so a
        // stale replay cannot hide a fork behind its cursor.
        let all_entries = batch.entries.iter().collect::<Vec<_>>();
        self.persist_replayed_journal_entries_unlocked(&all_entries, persisted_through_sequence)?;
        if replay_entries.is_empty() {
            return Ok(0);
        }

        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let (applied, table_sequence) = match self.plan_replayed_journal_entries_into_batch(
            replay_entries.iter().copied(),
            &mut table_ops,
            &mut rollback,
        ) {
            Ok(result) => result,
            Err(error) => {
                rollback.rollback(&mut self.tables);
                return Err(error);
            }
        };
        let previous_prefix_proof = self.prefix_proof.replace(candidate_prefix_proof);
        if let Err(error) = self.append_preapplied_table_ops(table_sequence, table_ops, rollback) {
            self.prefix_proof = previous_prefix_proof;
            return Err(error);
        }
        if local_cursor_advanced {
            self.materialized_through_sequence = self
                .materialized_through_sequence
                .max(local_cursor.last_sequence);
            self.checkpoint_local_materialization_cursor_if_due(local_cursor.last_sequence)?;
        }
        // Both cursors are recovery hints. The durable journal, table log, and
        // exact prefix proof are authoritative, so checkpointing them in
        // batches avoids two atomic file rewrites on every replicated commit.
        metadata_cursor::checkpoint_replay_cursor_if_due(&self.root, &source_cursor)?;
        Ok(applied)
    }

    /// Last local or replayed journal sequence visible to this shard.
    pub fn journal_last_sequence(&self) -> u64 {
        self.next_sequence.saturating_sub(1)
    }

    /// Read a bounded slice from the durable journal tail, including entries
    /// appended by another backend instance after this one opened.
    pub(crate) fn durable_journal_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let _lock = self.metadata_write_lock()?;
        durable_journal_entries_since_limited(&self.root, after_sequence, max_entries)
    }

    /// Export the current materialized table state for replica snapshot repair.
    pub fn export_table_snapshot(&self) -> DfsMetadataTableSnapshot {
        #[cfg(test)]
        METADATA_EXPORT_TABLE_SNAPSHOT_CALLS
            .set(METADATA_EXPORT_TABLE_SNAPSHOT_CALLS.get().saturating_add(1));
        let mut snapshot = DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            materialized_through_sequence: Some(self.materialized_through_sequence),
            tables: Some(metadata_tables_snapshot(&self.tables)),
            ops: Vec::new(),
            semantic_state_fingerprint: None,
            prefix_proof: self.prefix_proof.clone(),
        };
        snapshot.semantic_state_fingerprint = Some(
            dfs_metadata_table_snapshot_state_fingerprint(&snapshot)
                .expect("typed metadata table snapshots have a canonical JSON encoding"),
        );
        snapshot
    }

    /// Replace local shard tables with an authoritative replicated snapshot.
    ///
    /// The snapshot source must name this backend's logical shard. Cross-shard
    /// movement uses the explicit migration protocol rather than replication.
    /// The previous shard state is archived before replacement. The return
    /// value reports whether existing local state was archived.
    pub fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        self.require_replication_source_shard(&source_shard_id)?;
        let _lock = self.begin_metadata_write()?;
        self.replace_with_replicated_snapshot_unlocked(
            source_shard_id,
            replayed_through_sequence,
            snapshot,
        )
    }

    /// Install a validated replica snapshot across only the exact activated
    /// author fence named by `permit`. The durable marker is excluded from the
    /// repair archive and this path never changes process-local authority.
    pub fn replace_with_replicated_snapshot_with_replica_permit(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        let candidate_tables =
            metadata_tables_from_table_snapshot(snapshot.clone(), "replicated metadata snapshot")?;
        require_replica_snapshot_tables_file_system(&candidate_tables, permit.file_system_id())?;
        let _lock = self.begin_metadata_replica_ingest(permit, &source_shard_id)?;
        self.replace_with_replicated_snapshot_unlocked(
            source_shard_id,
            replayed_through_sequence,
            snapshot,
        )
    }

    /// Install an authoritative snapshot when ordinary backend open/replay is
    /// impossible. Source identity and snapshot filesystem are still bounded
    /// by the exact replica-ingest permit, and any readable durable handoff
    /// marker continues to fence a stale source epoch. Only table replay is
    /// bypassed before the unreadable state is archived and replaced.
    pub(crate) fn replace_unreadable_with_replicated_snapshot(
        permit: &DfsMetadataReplicaIngestPermit,
        shard_id: DfsMetadataShardId,
        root: impl Into<PathBuf>,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<(Self, bool)> {
        if permit.shard_id() != &source_shard_id || source_shard_id != shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata replica snapshot permit for filesystem {} shard {} source {} epoch {} cannot replace unreadable shard {} from source shard {}",
                permit.file_system_id(),
                permit.shard_id().0,
                permit.source_node_id(),
                permit.source_epoch(),
                shard_id.0,
                source_shard_id.0,
            )));
        }
        let candidate_tables =
            metadata_tables_from_table_snapshot(snapshot.clone(), "replicated metadata snapshot")?;
        require_replica_snapshot_tables_file_system(&candidate_tables, permit.file_system_id())?;
        let mut backend = Self {
            shard_id,
            root: root.into(),
            next_sequence: 1,
            tables: DfsMetadataTables::default(),
            table_log_offset: 0,
            materialized_through_sequence: 0,
            prefix_proof: None,
            instance_authority_epoch: None,
            table_snapshot_fingerprint: None,
            read_snapshot_cache: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            receipt_scan_counters: Arc::new(MetadataReceiptScanCounters::default()),
        };
        let lock = backend.metadata_write_lock()?;
        if let Some(freeze) = backend.membership_handoff_write_freeze()? {
            if !permit.admits_write_freeze(&freeze) {
                return Err(MetadataBackendError::MembershipHandoffFrozen {
                    shard_id: backend.shard_id.clone(),
                    handoff_id: freeze.handoff_id,
                });
            }
        }
        let archived_existing = backend.replace_with_replicated_snapshot_unlocked(
            source_shard_id,
            replayed_through_sequence,
            snapshot,
        )?;
        drop(lock);
        Ok((backend, archived_existing))
    }

    fn replace_with_replicated_snapshot_unlocked(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        if snapshot.materialized_through_sequence != Some(replayed_through_sequence) {
            return Err(MetadataBackendError::Serde(format!(
                "replicated metadata snapshot embedded sequence {:?} does not match install sequence {replayed_through_sequence}",
                snapshot.materialized_through_sequence
            )));
        }
        let semantic_fingerprint = dfs_metadata_table_snapshot_state_fingerprint(&snapshot)
            .map_err(|source| {
                MetadataBackendError::Serde(format!(
                    "encode replicated metadata snapshot semantic checksum: {source}"
                ))
            })?;
        if let Some(expected) = snapshot.semantic_state_fingerprint.as_ref()
            && expected != &semantic_fingerprint
        {
            return Err(MetadataBackendError::Serde(format!(
                "replicated metadata snapshot semantic checksum mismatch for shard {} sequence {replayed_through_sequence}",
                source_shard_id.0
            )));
        }
        let replacement_prefix_proof = match snapshot.prefix_proof.as_ref() {
            Some(proof)
                if proof.is_well_formed()
                    && proof.journal_sequence == replayed_through_sequence =>
            {
                proof.clone()
            }
            Some(proof) => {
                return Err(MetadataBackendError::Serde(format!(
                    "replicated metadata snapshot prefix proof sequence {} is invalid for shard {} sequence {replayed_through_sequence}",
                    proof.journal_sequence, source_shard_id.0
                )));
            }
            None => dfs_metadata_prefix_proof_seed(
                &source_shard_id,
                replayed_through_sequence,
                &semantic_fingerprint,
            ),
        };
        let acknowledged_floor =
            metadata_cursor::metadata_prefix_ack_floor(&self.root, &source_shard_id)?;
        let authority_floor =
            metadata_cursor::metadata_authority_ack_floor(&self.root, &source_shard_id)?;
        if let Some(floor) = authority_floor.as_ref()
            && replayed_through_sequence <= floor.journal_sequence
        {
            return Err(MetadataBackendError::Serde(format!(
                "replicated metadata snapshot sequence {replayed_through_sequence} would replace shard {} at or behind locally acknowledged authority operation {}",
                source_shard_id.0, floor.journal_sequence
            )));
        }
        if let Some(floor) = acknowledged_floor.as_ref() {
            if replayed_through_sequence < floor.journal_sequence {
                return Err(MetadataBackendError::Serde(format!(
                    "replicated metadata snapshot sequence {replayed_through_sequence} would roll shard {} behind locally acknowledged checkpoint {}",
                    source_shard_id.0, floor.journal_sequence
                )));
            }
            if replayed_through_sequence == floor.journal_sequence {
                let matches = dfs_metadata_table_snapshot_matches_checkpoint(
                    &snapshot,
                    replayed_through_sequence,
                    &floor.state_fingerprint,
                )
                .map_err(|source| {
                    MetadataBackendError::Serde(format!(
                        "encode replicated metadata snapshot acknowledgement check: {source}"
                    ))
                })?;
                if !matches {
                    return Err(MetadataBackendError::Serde(format!(
                        "replicated metadata snapshot conflicts with locally acknowledged checkpoint for shard {} sequence {replayed_through_sequence}",
                        source_shard_id.0
                    )));
                }
            }
        }
        let tables = metadata_tables_from_table_snapshot(snapshot, "replicated metadata snapshot")?;
        let archived_existing = archive_metadata_backend_state(&self.root)?;
        create_dir_all(self.root.join("records"))?;
        if let Some(floor) = acknowledged_floor.as_ref() {
            metadata_cursor::put_metadata_prefix_ack_floor(&self.root, floor)?;
        }
        create_dir_all(self.root.join("journal").join("segments"))?;
        self.tables = tables;
        self.table_log_offset = 0;
        self.next_sequence = replayed_through_sequence.saturating_add(1).max(1);
        self.materialized_through_sequence = replayed_through_sequence;
        self.prefix_proof = Some(replacement_prefix_proof);
        persist_metadata_table_snapshot(
            &self.root,
            &self.tables,
            Some(self.materialized_through_sequence),
            self.prefix_proof.as_ref(),
        )?;
        self.table_snapshot_fingerprint = metadata_table_snapshot_fingerprint(&self.root)?;
        metadata_cursor::write_replay_cursor(
            &self.root,
            &DfsMetadataReplayCursor {
                shard_id: source_shard_id,
                last_sequence: replayed_through_sequence,
            },
        )?;
        self.write_local_materialization_cursor(&DfsMetadataReplayCursor {
            shard_id: self.shard_id.clone(),
            last_sequence: replayed_through_sequence,
        })?;
        metadata_cursor::write_journal_head(&self.root, self.next_sequence)?;
        Ok(archived_existing)
    }

    #[cfg(test)]
    pub(crate) fn apply_journal_entry(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        require_journal_entry_replay_payloads(entry)?;
        let candidate_prefix_proof = self.prefix_proof_after_entry(
            self.prefix_proof.as_ref().ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "metadata shard {} has no exact prefix checkpoint",
                    self.shard_id.0
                ))
            })?,
            entry,
        )?;
        self.persist_replayed_journal_entry_unlocked(entry)?;
        let previous_prefix_proof = self.prefix_proof.replace(candidate_prefix_proof);
        let changed = match self.materialize_replayed_journal_entry_unlocked(entry) {
            Ok(changed) => changed,
            Err(error) => {
                self.prefix_proof = previous_prefix_proof;
                return Err(error);
            }
        };
        if !changed {
            self.append_and_apply_table_ops(entry.sequence, Vec::new())?;
        }
        if changed {
            self.advance_local_materialization_cursor(entry.sequence)?;
            Ok(Some(entry.clone()))
        } else {
            self.advance_local_materialization_cursor(entry.sequence)?;
            Ok(None)
        }
    }

    #[cfg(test)]
    fn persist_replayed_journal_entry_unlocked(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<()> {
        if let Some(sequence) = persist_replayed_journal_entry(&self.root, entry)? {
            self.advance_journal_head_after_replayed_entry(sequence)?;
        }
        Ok(())
    }

    fn persist_replayed_journal_entries_unlocked(
        &mut self,
        entries: &[&DfsMetadataJournalEntry],
        persisted_through_sequence: u64,
    ) -> Result<()> {
        if let Some(max_sequence) =
            persist_replayed_journal_entries(&self.root, entries, persisted_through_sequence)?
        {
            self.advance_journal_head_after_replayed_entry(max_sequence)?;
        }
        Ok(())
    }

    fn advance_journal_head_after_replayed_entry(&mut self, sequence: u64) -> Result<()> {
        let next_sequence = sequence.saturating_add(1).max(1).max(self.next_sequence);
        if next_sequence == self.next_sequence {
            return Ok(());
        }
        self.next_sequence = next_sequence;
        metadata_cursor::write_journal_head(&self.root, self.next_sequence)
    }

    fn materialize_replayed_journal_entry_unlocked(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<bool> {
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let changed = match self.materialize_replayed_journal_entry_into_batch(
            entry,
            &mut table_ops,
            &mut rollback,
        ) {
            Ok(changed) => changed,
            Err(error) => {
                rollback.rollback(&mut self.tables);
                return Err(error);
            }
        };
        if !changed {
            return Ok(false);
        }
        self.append_preapplied_table_ops(entry.sequence, table_ops, rollback)?;
        Ok(true)
    }

    fn plan_replayed_journal_entries_into_batch<'a>(
        &mut self,
        entries: impl IntoIterator<Item = &'a DfsMetadataJournalEntry>,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<(usize, u64)> {
        let mut changed_count = 0usize;
        let mut last_materialized_sequence = 0u64;
        for entry in entries {
            last_materialized_sequence = entry.sequence;
            if self.materialize_replayed_journal_entry_into_batch(entry, table_ops, rollback)? {
                changed_count = changed_count.saturating_add(1);
            }
        }
        Ok((changed_count, last_materialized_sequence))
    }

    fn plan_written_journal_entries_table_ops(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
    ) -> Result<PlannedWrittenJournalTableOps> {
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let (applied, last_materialized_sequence) = match self
            .plan_replayed_journal_entries_into_batch(entries, &mut table_ops, &mut rollback)
        {
            Ok(result) => result,
            Err(error) => {
                rollback.rollback(&mut self.tables);
                return Err(error);
            }
        };
        Ok(PlannedWrittenJournalTableOps {
            applied,
            last_materialized_sequence,
            table_ops,
            rollback,
        })
    }

    fn materialize_written_journal_entries_unlocked(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
    ) -> Result<usize> {
        let planned = self.plan_written_journal_entries_table_ops(entries)?;
        let applied = planned.applied;
        self.append_preapplied_table_ops_for_written_entries(
            entries,
            planned.last_materialized_sequence,
            planned.table_ops,
            planned.rollback,
        )?;
        Ok(applied)
    }

    fn append_preapplied_table_ops_for_written_entries(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
        table_sequence: u64,
        table_ops: Vec<DfsMetadataTableOp>,
        rollback: DfsMetadataTableRollback,
    ) -> Result<()> {
        self.append_preapplied_table_ops(table_sequence, table_ops, rollback)?;
        if let Some(last_sequence) = entries.last().map(|entry| entry.sequence) {
            self.checkpoint_local_materialization_cursor_if_due(last_sequence)?;
        }
        Ok(())
    }

    fn materialize_replayed_journal_entry_into_batch(
        &mut self,
        entry: &DfsMetadataJournalEntry,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<bool> {
        match &entry.mutation {
            DfsMetadataMutation::PutFileSystem {
                file_system_id,
                file_system,
            } => {
                let file_system =
                    required_replay_payload(entry, "put-file-system", file_system.as_ref())?;
                if self.get_file_system(file_system_id)?.as_ref() == Some(file_system) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutFileSystem {
                        file_system: file_system.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutObject {
                object_id, object, ..
            } => {
                let object = required_replay_payload(entry, "put-object", object.as_ref())?;
                let object = normalized_object_metadata(object);
                if self.get_object(object_id)? == Some(object.clone())
                    || self.tables.compacted_tombstone_matches(&object)
                {
                    return Ok(false);
                }
                if self.get_file_system(&object.file_system_id)?.is_none() {
                    return Err(MetadataBackendError::UnknownFileSystem {
                        file_system_id: object.file_system_id.clone(),
                    });
                }
                let previous = self.get_object(object_id)?;
                self.plan_object_index_table_ops_with_previous(
                    &object,
                    previous.as_ref(),
                    table_ops,
                    rollback,
                )?;
                Ok(true)
            }
            DfsMetadataMutation::PutVersion {
                object_id,
                version,
                version_record,
                ..
            } => {
                let version_record =
                    required_replay_payload(entry, "put-version", version_record.as_ref())?;
                let version_record = normalized_version_record(version_record);
                if self.get_version(object_id, version)? == Some(version_record.clone()) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutVersion {
                        version: version_record,
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutReceipt {
                object_id,
                version,
                replica_node_id,
                receipt,
                ..
            } => {
                let receipt = required_replay_payload(entry, "put-receipt", receipt.as_ref())?;
                if let Some(existing) = receipt_for_replica_from_backend(
                    self,
                    object_id,
                    version,
                    replica_node_id,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )? {
                    if replay_receipt_matches_existing(&existing, receipt) {
                        return Ok(false);
                    }
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} conflicts with an existing receipt for replica {}",
                        entry.shard_id.0, entry.sequence, replica_node_id
                    )));
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutReceipt {
                        receipt: receipt.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
                ..
            } => {
                if receipt_for_replica_from_backend(
                    self,
                    object_id,
                    version,
                    replica_node_id,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?
                .is_none()
                {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveReceipt {
                        object_id: object_id.clone(),
                        version: version.clone(),
                        replica_node_id: replica_node_id.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutIntent {
                object_id,
                intent_id,
                intent,
                ..
            } => {
                let intent = required_replay_payload(entry, "put-intent", intent.as_ref())?;
                if self.get_intent(object_id, intent_id)?.as_ref() == Some(intent) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutIntent {
                        intent: intent.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::CommitVisibleVersion {
                object_id,
                previous_version,
                committed_version,
                committed_object,
                visible_version,
                required_replica_node_ids,
                ..
            } => {
                let committed_object = required_replay_payload(
                    entry,
                    "commit-visible-version committed object",
                    committed_object.as_ref(),
                )?;
                let visible_version = required_replay_payload(
                    entry,
                    "commit-visible-version visible version",
                    visible_version.as_ref(),
                )?;
                let commit_record = visible_commit_record_from_parts(
                    previous_version,
                    committed_object,
                    visible_version,
                    required_replica_node_ids,
                    entry.created_at,
                );
                if self
                    .get_object(object_id)?
                    .as_ref()
                    .is_some_and(|object| &object.version == committed_version)
                    && self.get_version(object_id, committed_version)?.as_ref()
                        == Some(&normalized_version_record(visible_version))
                {
                    return Ok(false);
                }
                if visible_commit_already_materialized(
                    &self.tables,
                    object_id,
                    committed_version,
                    &commit_record,
                ) {
                    return Ok(false);
                }
                let current = self.get_object(object_id)?.ok_or_else(|| {
                    MetadataBackendError::UnknownObject {
                        object_id: object_id.clone(),
                    }
                })?;
                if &current.version != previous_version {
                    return Err(MetadataBackendError::Conflict {
                        object_id: object_id.clone(),
                        expected: previous_version.clone(),
                        current: current.version,
                    });
                }
                // A replayed commit entry is authoritative after source validation. Short
                // read-your-write replay windows can arrive without every receipt entry; the
                // commit payload is still enough to make the object/version visible locally.
                self.plan_visible_version_commit_table_ops(&commit_record, table_ops, rollback)?;
                Ok(true)
            }
            DfsMetadataMutation::PutConflict {
                object_id,
                conflict_id,
                conflict,
                ..
            } => {
                if let Some(conflict) = conflict.as_ref() {
                    if self.get_conflict(object_id, conflict_id)?.as_ref() == Some(conflict) {
                        return Ok(false);
                    }
                    self.plan_preapplied_table_op(
                        DfsMetadataTableOp::PutConflict {
                            conflict: conflict.clone(),
                        },
                        table_ops,
                        rollback,
                    );
                } else {
                    if self.get_conflict(object_id, conflict_id)?.is_none() {
                        return Ok(false);
                    }
                    self.plan_preapplied_table_op(
                        DfsMetadataTableOp::RemoveConflict {
                            object_id: object_id.clone(),
                            conflict_id: conflict_id.clone(),
                        },
                        table_ops,
                        rollback,
                    );
                }
                Ok(true)
            }
            DfsMetadataMutation::PutAdvisoryLockState {
                file_system_id,
                object_id,
                state,
            } => {
                let state =
                    required_replay_payload(entry, "put-advisory-lock-state", state.as_ref())?;
                if state.file_system_id != *file_system_id || state.object_id != *object_id {
                    return Err(MetadataBackendError::AdvisoryLock(format!(
                        "journal scope {file_system_id}/{object_id} does not match payload scope {}/{}",
                        state.file_system_id, state.object_id
                    )));
                }
                let object = self.get_object(object_id)?.ok_or_else(|| {
                    MetadataBackendError::UnknownObject {
                        object_id: object_id.clone(),
                    }
                })?;
                if object.file_system_id != *file_system_id {
                    return Err(MetadataBackendError::AdvisoryLock(format!(
                        "journal lock state for {file_system_id}/{object_id} does not match object filesystem {}",
                        object.file_system_id
                    )));
                }
                if self
                    .tables
                    .advisory_lock_states
                    .get(object_id)
                    .is_some_and(|existing| existing == state)
                {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutAdvisoryLockState {
                        state: state.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::ApplyNamespaceTransaction {
                transaction_id,
                transaction,
                ..
            } => {
                let transaction = required_replay_payload(
                    entry,
                    "apply-namespace-transaction",
                    transaction.as_ref(),
                )?;
                let normalized = normalized_namespace_transaction(transaction);
                if let Some(receipt) = self
                    .tables
                    .namespace_transaction_receipts
                    .get(transaction_id)
                {
                    if receipt
                        .matches_transaction(&normalized)
                        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?
                    {
                        return Ok(false);
                    }
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace transaction {transaction_id} conflicts with its compacted receipt"
                    )));
                }
                if self.tables.namespace_transactions.get(transaction_id) == Some(&normalized) {
                    return Ok(false);
                }
                self.plan_namespace_transaction_table_ops(&normalized, table_ops, rollback)?;
                Ok(true)
            }
            DfsMetadataMutation::PutNamespaceTransactionReceipt {
                file_system_id,
                transaction_id,
                receipt,
            } => {
                if receipt.file_system_id != *file_system_id
                    || receipt.transaction_id != *transaction_id
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace transaction receipt scope {file_system_id}/{transaction_id} does not match its payload"
                    )));
                }
                if self
                    .tables
                    .namespace_transaction_receipts
                    .get(transaction_id)
                    == Some(receipt)
                {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutNamespaceTransactionReceipt {
                        receipt: receipt.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutNamespacePrepare {
                file_system_id,
                operation_id,
                status,
                prepare,
            } => {
                let prepare =
                    required_replay_payload(entry, "put-namespace-prepare", prepare.as_ref())?;
                if prepare.file_system_id != *file_system_id
                    || prepare.operation_id != *operation_id
                    || prepare.status != *status
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace prepare scope {file_system_id}/{operation_id}/{status:?} does not match its payload"
                    )));
                }
                let prepare = normalized_namespace_prepare(prepare);
                if let Some(receipt) = self.tables.namespace_prepare_receipts.get(operation_id) {
                    if receipt.status == prepare.status
                        && receipt.matches_prepare_identity(&prepare)
                    {
                        return Ok(false);
                    }
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace prepare {operation_id} conflicts with its compacted receipt"
                    )));
                }
                let ops =
                    namespace_prepare_table_ops_for_record(&self.tables, &self.shard_id, &prepare)?;
                if ops.is_empty() {
                    return Ok(false);
                }
                for op in ops {
                    self.plan_preapplied_table_op(op, table_ops, rollback);
                }
                Ok(true)
            }
            DfsMetadataMutation::PutNamespacePrepareReceipt {
                file_system_id,
                operation_id,
                receipt,
            } => {
                if receipt.file_system_id != *file_system_id
                    || receipt.operation_id != *operation_id
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace prepare receipt scope {file_system_id}/{operation_id} does not match its payload"
                    )));
                }
                if self.tables.namespace_prepare_receipts.get(operation_id) == Some(receipt) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutNamespacePrepareReceipt {
                        receipt: receipt.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutNamespaceJob { job_id, job, .. } => {
                let job = required_replay_payload(entry, "put-namespace-job", job.as_ref())?;
                if job.job_id != *job_id {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace job id {job_id} does not match payload {}",
                        job.job_id
                    )));
                }
                if self.tables.namespace_jobs.get(job_id) == Some(job) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutNamespaceJob { job: job.clone() },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutNamespaceRequest {
                requester_node_id,
                request_id,
                request,
                ..
            } => {
                if request.requester_node_id != *requester_node_id
                    || request.request_id != *request_id
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace request key {requester_node_id}/{request_id} does not match its payload"
                    )));
                }
                let key = (requester_node_id.clone(), request_id.clone());
                if let Some(receipt) = self.tables.namespace_request_receipts.get(&key) {
                    if receipt.matches_completed_request(request) {
                        return Ok(false);
                    }
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace request {requester_node_id}/{request_id} conflicts with its compacted receipt"
                    )));
                }
                if self.tables.namespace_requests.get(&key) == Some(request) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutNamespaceRequest {
                        request: request.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutNamespaceRequestReceipt {
                file_system_id,
                requester_node_id,
                request_id,
                receipt,
            } => {
                if receipt.file_system_id != *file_system_id
                    || receipt.requester_node_id != *requester_node_id
                    || receipt.request_id != *request_id
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace request receipt scope {file_system_id}/{requester_node_id}/{request_id} does not match its payload"
                    )));
                }
                let key = (requester_node_id.clone(), request_id.clone());
                if self.tables.namespace_request_receipts.get(&key) == Some(receipt) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutNamespaceRequestReceipt {
                        receipt: receipt.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn hydrate_journal_entry_payloads(
        &self,
        entries: Vec<DfsMetadataJournalEntry>,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        hydrate_journal_entry_payloads(self, &self.tables, entries, OBJECT_LOCAL_METADATA_PAGE_SIZE)
    }

    /// Read at most `max_entries` journal entries after `after_sequence`.
    ///
    /// This lower-level reader is mainly for diagnostics and compatibility.
    /// Replication should prefer `journal_batch_since_limited`, which hydrates
    /// record payloads and enforces encoded-size bounds.
    pub fn journal_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let _lock = self.metadata_write_lock()?;
        journal_entries_since_limited_with_next_sequence(
            &self.root,
            after_sequence,
            max_entries,
            self.next_sequence,
        )
    }

    /// Build a bounded, payload-complete journal batch after `after_sequence`.
    ///
    /// The returned batch is suitable for metadata replication. Legacy journal
    /// entries are hydrated from materialized records before byte bounds are
    /// applied so receivers do not need the source record store.
    pub fn journal_batch_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        let entries = self.journal_since_limited(after_sequence, max_entries)?;
        let hydrated = hydrate_journal_entry_payloads(
            self,
            &self.tables,
            entries,
            OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        bounded_metadata_journal_batch(
            self.shard_id.clone(),
            after_sequence,
            hydrated,
            max_entries,
            max_encoded_bytes,
        )
    }

    pub(crate) fn replay_cursor_statuses(&self) -> Result<Vec<DfsMetadataReplayCursorStatus>> {
        let mut cursors = metadata_cursor::replay_cursor_statuses(&self.root)?;
        if let Some(prefix_proof) = self.prefix_proof.as_ref()
            && let Some(cursor) = cursors
                .iter_mut()
                .find(|cursor| cursor.source_shard_id == self.shard_id)
        {
            // The cursor file is a checkpointed recovery hint. The table
            // prefix proof is the authoritative durable replay position and
            // must drive operator/failover status between checkpoints.
            cursor.replayed_through_sequence = cursor
                .replayed_through_sequence
                .max(prefix_proof.journal_sequence);
        }
        Ok(cursors)
    }

    fn ensure_metadata_prefix_proof_checkpoint_unlocked(&mut self) -> Result<bool> {
        if let Some(proof) = self.prefix_proof.as_ref() {
            if !proof.is_well_formed()
                || proof.journal_sequence != self.materialized_through_sequence
            {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata shard {} durable prefix proof sequence {} does not match materialized sequence {}",
                    self.shard_id.0, proof.journal_sequence, self.materialized_through_sequence
                )));
            }
            return Ok(false);
        }

        // Rolling upgrade / legacy state: hash the complete exact shard once,
        // establish a deterministic seed, and durably checkpoint it in the
        // already-fsynced table log. Healthy writes never take this path.
        self.prefix_proof = Some(self.metadata_prefix_checkpoint_for_shard(
            &self.shard_id,
            self.materialized_through_sequence,
        )?);
        Ok(true)
    }

    fn metadata_prefix_checkpoint_for_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        journal_sequence: u64,
    ) -> Result<DfsMetadataPrefixProof> {
        let checkpoint = DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            materialized_through_sequence: Some(journal_sequence),
            tables: Some(metadata_tables_snapshot(&self.tables)),
            ops: Vec::new(),
            semantic_state_fingerprint: None,
            prefix_proof: None,
        };
        let semantic_fingerprint = dfs_metadata_table_snapshot_state_fingerprint(&checkpoint)
            .map_err(|source| {
                MetadataBackendError::Serde(format!(
                    "bootstrap exact metadata prefix proof for shard {}: {source}",
                    shard_id.0
                ))
            })?;
        Ok(dfs_metadata_prefix_proof_seed(
            shard_id,
            journal_sequence,
            &semantic_fingerprint,
        ))
    }

    fn persist_current_metadata_checkpoint_unlocked(&mut self) -> Result<()> {
        persist_metadata_table_snapshot(
            &self.root,
            &self.tables,
            Some(self.materialized_through_sequence),
            self.prefix_proof.as_ref(),
        )?;
        self.table_log_offset = 0;
        self.table_snapshot_fingerprint = metadata_table_snapshot_fingerprint(&self.root)?;
        Ok(())
    }

    fn prefix_proof_after_entry(
        &self,
        previous: &DfsMetadataPrefixProof,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<DfsMetadataPrefixProof> {
        self.prefix_proof_after_entry_for_shard(previous, entry, &self.shard_id)
    }

    fn prefix_proof_after_entry_for_shard(
        &self,
        previous: &DfsMetadataPrefixProof,
        entry: &DfsMetadataJournalEntry,
        expected_shard_id: &DfsMetadataShardId,
    ) -> Result<DfsMetadataPrefixProof> {
        if !previous.is_well_formed()
            || previous.journal_sequence.saturating_add(1) != entry.sequence
            || &entry.shard_id != expected_shard_id
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} cannot extend prefix {} with journal entry {}:{}",
                expected_shard_id.0, previous.journal_sequence, entry.shard_id.0, entry.sequence
            )));
        }
        dfs_metadata_prefix_proof_after(previous, entry).map_err(|source| {
            MetadataBackendError::Serde(format!(
                "encode metadata shard {} journal prefix entry {}: {source}",
                expected_shard_id.0, entry.sequence
            ))
        })
    }

    fn advance_metadata_prefix_proof_unlocked(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
    ) -> Result<()> {
        let mut proof = self.prefix_proof.clone().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "metadata shard {} has no exact prefix checkpoint",
                self.shard_id.0
            ))
        })?;
        for entry in entries {
            proof = self.prefix_proof_after_entry(&proof, entry)?;
        }
        self.prefix_proof = Some(proof);
        Ok(())
    }

    pub(crate) fn metadata_prefix_proof(&self) -> Result<DfsMetadataPrefixProof> {
        let proof = self.prefix_proof.clone().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "metadata shard {} has no exact durable prefix proof",
                self.shard_id.0
            ))
        })?;
        if !proof.is_well_formed() || proof.journal_sequence != self.materialized_through_sequence {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} exact prefix proof is not at its materialized head {}",
                self.shard_id.0, self.materialized_through_sequence
            )));
        }
        Ok(proof)
    }

    #[cfg(test)]
    fn write_journal(&mut self, mutation: DfsMetadataMutation) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        self.write_journal_unlocked(mutation)
    }

    fn write_journal_unlocked(
        &mut self,
        mutation: DfsMetadataMutation,
    ) -> Result<DfsMetadataJournalEntry> {
        let entry = write_local_journal_entry(
            &self.root,
            &self.shard_id,
            &mut self.next_sequence,
            mutation,
        )?;
        #[cfg(test)]
        METADATA_JOURNAL_WRITE_COUNT.set(METADATA_JOURNAL_WRITE_COUNT.get().saturating_add(1));
        self.advance_metadata_prefix_proof_unlocked(std::slice::from_ref(&entry))?;
        Ok(entry)
    }

    fn write_journal_entry_and_apply_table_ops_unlocked(
        &mut self,
        mutation: DfsMetadataMutation,
        table_ops: Vec<DfsMetadataTableOp>,
    ) -> Result<DfsMetadataJournalEntry> {
        self.write_journal_entry_and_materialize_unlocked(mutation, |_| Ok(table_ops))
    }

    fn write_journal_entry_and_materialize_unlocked(
        &mut self,
        mutation: DfsMetadataMutation,
        table_ops: impl FnOnce(&mut Self) -> Result<Vec<DfsMetadataTableOp>>,
    ) -> Result<DfsMetadataJournalEntry> {
        let entry = self.write_journal_unlocked(mutation)?;
        let table_ops = table_ops(self)?;
        self.append_and_apply_table_ops(entry.sequence, table_ops)?;
        self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
        Ok(entry)
    }

    fn write_journal_entries_unlocked(
        &mut self,
        mutations: Vec<DfsMetadataMutation>,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let entries = write_local_journal_entries(
            &self.root,
            &self.shard_id,
            &mut self.next_sequence,
            mutations,
        )?;
        #[cfg(test)]
        METADATA_JOURNAL_WRITE_COUNT.set(
            METADATA_JOURNAL_WRITE_COUNT
                .get()
                .saturating_add(entries.len()),
        );
        self.advance_metadata_prefix_proof_unlocked(&entries)?;
        Ok(entries)
    }

    fn ensure_local_materialization_cursor(&self) -> Result<()> {
        metadata_cursor::ensure_local_materialization_cursor(
            &self.root,
            &self.shard_id,
            self.next_sequence,
            self.tables.has_materialized_state(),
        )
    }

    fn local_materialization_cursor(&self) -> Result<DfsMetadataReplayCursor> {
        metadata_cursor::local_materialization_cursor(
            &self.root,
            &self.shard_id,
            self.next_sequence,
        )
    }

    #[cfg(test)]
    fn advance_local_materialization_cursor(&self, sequence: u64) -> Result<()> {
        metadata_cursor::advance_local_materialization_cursor(
            &self.root,
            &self.shard_id,
            self.next_sequence,
            sequence,
        )
    }

    fn reconcile_local_materialization_cursor(&self, materialized_sequence: u64) -> Result<()> {
        metadata_cursor::reconcile_local_materialization_cursor(
            &self.root,
            &self.shard_id,
            self.next_sequence,
            materialized_sequence,
        )
    }

    fn rewind_local_materialization_cursor_to_table_head_if_ahead(&self) -> Result<bool> {
        let cursor = self.local_materialization_cursor()?;
        if cursor.last_sequence <= self.materialized_through_sequence {
            return Ok(false);
        }
        self.write_local_materialization_cursor(&DfsMetadataReplayCursor {
            shard_id: self.shard_id.clone(),
            last_sequence: self.materialized_through_sequence,
        })?;
        Ok(true)
    }

    fn checkpoint_local_materialization_cursor_if_due(&self, sequence: u64) -> Result<()> {
        metadata_cursor::checkpoint_local_materialization_cursor_if_due(
            &self.root,
            &self.shard_id,
            self.next_sequence,
            sequence,
        )
    }

    fn write_local_materialization_cursor(&self, cursor: &DfsMetadataReplayCursor) -> Result<()> {
        metadata_cursor::write_local_materialization_cursor(&self.root, cursor)
    }

    fn recover_unmaterialized_local_journal_entries(&mut self) -> Result<()> {
        let mut cursor = self.local_materialization_cursor()?;
        if cursor.last_sequence >= self.next_sequence.saturating_sub(1) {
            return Ok(());
        }
        loop {
            let entries = journal_entries_since_limited_with_next_sequence(
                &self.root,
                cursor.last_sequence,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                self.next_sequence,
            )?;
            if entries.is_empty() {
                break;
            }
            let mut advanced = false;
            for entry in entries {
                self.advance_metadata_prefix_proof_unlocked(std::slice::from_ref(&entry))?;
                if journal_entry_has_replay_payloads(&entry) {
                    self.materialize_replayed_journal_entry_unlocked(&entry)?;
                } else {
                    self.materialize_local_journal_entry(&entry)?;
                }
                if entry.sequence > cursor.last_sequence {
                    cursor.last_sequence = entry.sequence;
                    advanced = true;
                }
            }
            if advanced {
                if self
                    .prefix_proof
                    .as_ref()
                    .is_some_and(|proof| proof.journal_sequence == cursor.last_sequence)
                {
                    self.append_and_apply_table_ops(cursor.last_sequence, Vec::new())?;
                }
                self.materialized_through_sequence =
                    self.materialized_through_sequence.max(cursor.last_sequence);
                self.write_local_materialization_cursor(&cursor)?;
            }
        }
        Ok(())
    }

    fn materialize_local_journal_entry(&mut self, entry: &DfsMetadataJournalEntry) -> Result<()> {
        let table_ops = match &entry.mutation {
            DfsMetadataMutation::PutFileSystem {
                file_system_id: _,
                file_system,
            } => {
                let Some(file_system) = file_system.as_ref() else {
                    return Ok(());
                };
                vec![DfsMetadataTableOp::PutFileSystem {
                    file_system: file_system.clone(),
                }]
            }
            DfsMetadataMutation::PutObject { object, .. } => {
                let Some(object) = object.as_ref() else {
                    return Ok(());
                };
                if self.tables.compacted_tombstone_matches(object) {
                    return Ok(());
                }
                if self.get_file_system(&object.file_system_id)?.is_none() {
                    return Err(MetadataBackendError::UnknownFileSystem {
                        file_system_id: object.file_system_id.clone(),
                    });
                }
                let previous = self.get_object(&object.object_id)?;
                self.object_index_table_ops_with_previous(object, previous.as_ref())?
            }
            DfsMetadataMutation::PutVersion { version_record, .. } => {
                let Some(version_record) = version_record.as_ref() else {
                    return Ok(());
                };
                vec![DfsMetadataTableOp::PutVersion {
                    version: normalized_version_record(version_record),
                }]
            }
            DfsMetadataMutation::PutReceipt { receipt, .. } => {
                let Some(receipt) = receipt.as_ref() else {
                    return Ok(());
                };
                vec![DfsMetadataTableOp::PutReceipt {
                    receipt: receipt.clone(),
                }]
            }
            DfsMetadataMutation::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
                ..
            } => {
                vec![DfsMetadataTableOp::RemoveReceipt {
                    object_id: object_id.clone(),
                    version: version.clone(),
                    replica_node_id: replica_node_id.clone(),
                }]
            }
            DfsMetadataMutation::PutIntent { intent, .. } => {
                let Some(intent) = intent.as_ref() else {
                    return Ok(());
                };
                vec![DfsMetadataTableOp::PutIntent {
                    intent: intent.clone(),
                }]
            }
            DfsMetadataMutation::CommitVisibleVersion {
                previous_version,
                committed_object,
                visible_version,
                required_replica_node_ids,
                ..
            } => {
                let Some(committed_object) = committed_object.as_ref() else {
                    return Ok(());
                };
                let Some(visible_version) = visible_version.as_ref() else {
                    return Ok(());
                };
                let commit_record = visible_commit_record_from_parts(
                    previous_version,
                    committed_object,
                    visible_version,
                    required_replica_node_ids,
                    entry.created_at,
                );
                visible_commit_table_ops(&self.tables, &commit_record)
            }
            DfsMetadataMutation::PutConflict {
                object_id,
                conflict_id,
                conflict,
                ..
            } => {
                if let Some(conflict) = conflict.as_ref() {
                    vec![DfsMetadataTableOp::PutConflict {
                        conflict: conflict.clone(),
                    }]
                } else {
                    vec![DfsMetadataTableOp::RemoveConflict {
                        object_id: object_id.clone(),
                        conflict_id: conflict_id.clone(),
                    }]
                }
            }
            DfsMetadataMutation::PutAdvisoryLockState { state, .. } => {
                let Some(state) = state.as_ref() else {
                    return Ok(());
                };
                vec![DfsMetadataTableOp::PutAdvisoryLockState {
                    state: state.clone(),
                }]
            }
            DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } => {
                let Some(transaction) = transaction.as_ref() else {
                    return Ok(());
                };
                let transaction = normalized_namespace_transaction(transaction);
                namespace_transaction_table_ops(&self.tables, &transaction)
            }
            DfsMetadataMutation::PutNamespaceTransactionReceipt { receipt, .. } => {
                vec![DfsMetadataTableOp::PutNamespaceTransactionReceipt {
                    receipt: receipt.clone(),
                }]
            }
            DfsMetadataMutation::PutNamespacePrepare { prepare, .. } => {
                let Some(prepare) = prepare.as_ref() else {
                    return Ok(());
                };
                let prepare = normalized_namespace_prepare(prepare);
                namespace_prepare_table_ops_for_record(&self.tables, &self.shard_id, &prepare)?
            }
            DfsMetadataMutation::PutNamespacePrepareReceipt { receipt, .. } => {
                vec![DfsMetadataTableOp::PutNamespacePrepareReceipt {
                    receipt: receipt.clone(),
                }]
            }
            DfsMetadataMutation::PutNamespaceJob { job, .. } => {
                let Some(job) = job.as_ref() else {
                    return Ok(());
                };
                vec![DfsMetadataTableOp::PutNamespaceJob { job: job.clone() }]
            }
            DfsMetadataMutation::PutNamespaceRequest { request, .. } => {
                vec![DfsMetadataTableOp::PutNamespaceRequest {
                    request: request.clone(),
                }]
            }
            DfsMetadataMutation::PutNamespaceRequestReceipt { receipt, .. } => {
                vec![DfsMetadataTableOp::PutNamespaceRequestReceipt {
                    receipt: receipt.clone(),
                }]
            }
        };
        self.append_and_apply_table_ops(entry.sequence, table_ops)
    }

    #[cfg(test)]
    fn replay_cursor_path(&self, shard_id: &DfsMetadataShardId) -> PathBuf {
        metadata_cursor::replay_cursor_path(&self.root, shard_id)
    }

    #[cfg(test)]
    fn local_materialization_cursor_path(&self) -> PathBuf {
        metadata_cursor::local_materialization_cursor_path(&self.root)
    }

    #[cfg(test)]
    fn metadata_replication_cursor_path(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> PathBuf {
        metadata_cursor::metadata_replication_cursor_path(&self.root, target_node_id, shard_id)
    }

    fn journal_write_lock_path(&self) -> PathBuf {
        metadata_cursor::journal_write_lock_path(&self.root)
    }

    fn membership_handoff_write_freeze_path(&self) -> PathBuf {
        self.root
            .join("journal")
            .join("membership-handoff-write-freeze.sobj")
    }

    fn membership_handoff_write_freeze(&self) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        read_sobj_optional(&self.membership_handoff_write_freeze_path())
    }

    fn metadata_write_lock(&self) -> Result<MetadataWriteLock> {
        metadata_write_lock(self.root.clone(), self.journal_write_lock_path())
    }

    fn reload_durable_metadata_table_state(&mut self) -> Result<()> {
        let state = read_metadata_table_state_lenient(&self.root)?;
        self.tables = state.tables;
        self.table_log_offset = state.table_log_offset;
        self.materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
        self.prefix_proof = state.prefix_proof;
        self.table_snapshot_fingerprint = state.snapshot_fingerprint;
        if let Ok(mut cache) = self.read_snapshot_cache.lock() {
            *cache = None;
        }
        Ok(())
    }

    fn refresh_after_metadata_write_lock(&mut self) -> Result<()> {
        #[cfg(test)]
        METADATA_BEGIN_WRITE_CALLS.set(METADATA_BEGIN_WRITE_CALLS.get().saturating_add(1));
        // `next_sequence` advances only after the local journal record is
        // durably appended. Preserve that live exact tail across refresh. A
        // newly opened backend obtains the same value from the scanning
        // recovery path. Healthy writes from another handle advance the table
        // head below; recovery after a different live handle fails between
        // journal and table durability is outside this single-writer slice.
        let durable_next_sequence = self.next_sequence.max(1);
        let previous_materialized_sequence = self.materialized_through_sequence;
        self.refresh_metadata_tables()?;
        let durable_next_sequence = durable_next_sequence
            .max(self.materialized_through_sequence.saturating_add(1))
            .max(1);
        let durable_journal_tail = durable_next_sequence.saturating_sub(1);
        let materialization_rewound =
            self.materialized_through_sequence < previous_materialized_sequence;
        if durable_journal_tail > self.materialized_through_sequence {
            // The journal is the durability boundary. A prior append may have
            // succeeded while table-batch materialization failed and rolled
            // back this live handle. Reload the exact durable table proof so a
            // same-instance retry does not carry the journal-ahead proof into
            // replay, then recover the gap before admitting another write.
            self.reload_durable_metadata_table_state()?;
        }
        self.next_sequence = durable_next_sequence
            .max(self.materialized_through_sequence.saturating_add(1))
            .max(1);
        let has_unmaterialized_durable_tail =
            durable_journal_tail > self.materialized_through_sequence;
        let bootstrapped_prefix = self.ensure_metadata_prefix_proof_checkpoint_unlocked()?;
        if has_unmaterialized_durable_tail {
            // Deferred cursor checkpoints are scheduling hints. Recovery must
            // begin at the exact durable table head whose prefix proof was
            // just loaded, whether the hint was behind or ahead of that head.
            self.write_local_materialization_cursor(&DfsMetadataReplayCursor {
                shard_id: self.shard_id.clone(),
                last_sequence: self.materialized_through_sequence,
            })?;
            self.recover_unmaterialized_local_journal_entries()?;
        } else if materialization_rewound {
            self.rewind_local_materialization_cursor_to_table_head_if_ahead()?;
            self.recover_unmaterialized_local_journal_entries()?;
        }
        if (bootstrapped_prefix || materialization_rewound)
            && (self.materialized_through_sequence > 0 || self.tables.has_materialized_state())
        {
            self.persist_current_metadata_checkpoint_unlocked()?;
        }
        Ok(())
    }

    fn begin_metadata_write_allow_frozen(&mut self) -> Result<MetadataWriteLock> {
        let lock = self.metadata_write_lock()?;
        self.refresh_after_metadata_write_lock()?;
        Ok(lock)
    }

    fn require_replication_source_shard(&self, source_shard_id: &DfsMetadataShardId) -> Result<()> {
        if source_shard_id != &self.shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata backend shard {} cannot ingest replicated state for source shard {}",
                self.shard_id.0, source_shard_id.0
            )));
        }
        Ok(())
    }

    fn begin_metadata_replica_ingest(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        source_shard_id: &DfsMetadataShardId,
    ) -> Result<MetadataWriteLock> {
        if permit.shard_id() != source_shard_id || source_shard_id != &self.shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata replica ingest permit for filesystem {} shard {} source {} epoch {} cannot mutate backend shard {} from source shard {}",
                permit.file_system_id(),
                permit.shard_id().0,
                permit.source_node_id(),
                permit.source_epoch(),
                self.shard_id.0,
                source_shard_id.0,
            )));
        }
        let lock = self.metadata_write_lock()?;
        if let Some(freeze) = self.membership_handoff_write_freeze()? {
            if !permit.admits_write_freeze(&freeze) {
                return Err(MetadataBackendError::MembershipHandoffFrozen {
                    shard_id: self.shard_id.clone(),
                    handoff_id: freeze.handoff_id,
                });
            }
        }
        self.refresh_after_metadata_write_lock()?;
        Ok(lock)
    }

    fn begin_metadata_write(&mut self) -> Result<MetadataWriteLock> {
        let lock = self.metadata_write_lock()?;
        if let Some(freeze) = self.membership_handoff_write_freeze()? {
            let instance_matches_active_generation = freeze
                .activated_epoch
                .is_some_and(|epoch| self.instance_authority_epoch == Some(epoch));
            if !instance_matches_active_generation {
                return Err(MetadataBackendError::MembershipHandoffFrozen {
                    shard_id: self.shard_id.clone(),
                    handoff_id: freeze.handoff_id,
                });
            }
        }
        self.refresh_after_metadata_write_lock()?;
        Ok(lock)
    }

    fn freeze_for_membership_handoff(
        &mut self,
        handoff_id: &str,
        retiring_epoch: u64,
    ) -> Result<DfsMetadataShardWriteFreeze> {
        if handoff_id.is_empty() {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} membership write freeze has an empty handoff id",
                self.shard_id.0
            )));
        }
        let _lock = self.begin_metadata_write_allow_frozen()?;
        let existing = self.membership_handoff_write_freeze()?;
        if let Some(existing) = existing.as_ref() {
            let same_pending_handoff = existing.activated_epoch.is_none()
                && existing.handoff_id == handoff_id
                && existing.retiring_epoch == retiring_epoch;
            // A completed marker is a durable fence, not an authority grant.
            // A later control-plane-authorized handoff may supersede it even
            // when this backend instance deliberately stayed unbound after
            // recording the prior activation. Installing the new pending
            // marker below clears any instance binding again.
            let replacing_completed_generation = existing
                .activated_epoch
                .is_some_and(|activated_epoch| activated_epoch <= retiring_epoch);
            if !same_pending_handoff && !replacing_completed_generation {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata shard {} is already fenced by membership handoff {} retiring epoch {} active epoch {:?}",
                    self.shard_id.0,
                    existing.handoff_id,
                    existing.retiring_epoch,
                    existing.activated_epoch
                )));
            }
        }
        let journal_sequence = self.journal_last_sequence();
        let prefix_proof = self.prefix_proof.as_ref().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "metadata shard {} membership freeze has no exact prefix proof",
                self.shard_id.0
            ))
        })?;
        if !prefix_proof.is_well_formed() || prefix_proof.journal_sequence != journal_sequence {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} membership freeze prefix sequence {} does not match head {}",
                self.shard_id.0, prefix_proof.journal_sequence, journal_sequence
            )));
        }
        let state_fingerprint = prefix_proof.digest.clone();
        if let Some(existing) = existing.as_ref()
            && (journal_sequence < existing.journal_sequence
                || (journal_sequence == existing.journal_sequence
                    && state_fingerprint != existing.state_fingerprint))
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} membership freeze state regressed or diverged at sequence {}",
                self.shard_id.0, journal_sequence
            )));
        }
        let freeze = DfsMetadataShardWriteFreeze {
            handoff_id: handoff_id.to_string(),
            retiring_epoch,
            activated_epoch: None,
            journal_sequence,
            state_fingerprint,
        };
        write_sobj_atomic(&self.membership_handoff_write_freeze_path(), &freeze)?;
        self.instance_authority_epoch = None;
        Ok(freeze)
    }

    fn mark_membership_handoff_write_freeze_activated_unlocked(
        &mut self,
        handoff_id: &str,
        activated_epoch: u64,
    ) -> Result<bool> {
        let Some(existing) = self.membership_handoff_write_freeze()? else {
            return Ok(false);
        };
        if existing.handoff_id != handoff_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} membership freeze {} does not match completed handoff {}",
                self.shard_id.0, existing.handoff_id, handoff_id
            )));
        }
        let expected_activated_epoch = existing.retiring_epoch.checked_add(1).ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "metadata shard {} membership handoff {} retiring epoch is exhausted",
                self.shard_id.0, handoff_id
            ))
        })?;
        if activated_epoch != expected_activated_epoch {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} membership handoff {} retiring epoch {} can activate only exact successor epoch {}, not {}",
                self.shard_id.0,
                handoff_id,
                existing.retiring_epoch,
                expected_activated_epoch,
                activated_epoch
            )));
        }
        if existing.activated_epoch == Some(activated_epoch) {
            return Ok(true);
        }
        if existing
            .activated_epoch
            .is_some_and(|current| current != activated_epoch)
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} cannot advance membership handoff {} from retiring epoch {} active epoch {:?} to epoch {}",
                self.shard_id.0,
                handoff_id,
                existing.retiring_epoch,
                existing.activated_epoch,
                activated_epoch
            )));
        }
        let mut activated = existing;
        activated.activated_epoch = Some(activated_epoch);
        write_sobj_atomic(&self.membership_handoff_write_freeze_path(), &activated)?;
        Ok(true)
    }

    fn mark_membership_handoff_write_freeze_activated(
        &mut self,
        handoff_id: &str,
        activated_epoch: u64,
    ) -> Result<bool> {
        let _lock = self.begin_metadata_write_allow_frozen()?;
        self.mark_membership_handoff_write_freeze_activated_unlocked(handoff_id, activated_epoch)
    }

    fn clear_membership_handoff_write_freeze(
        &mut self,
        handoff_id: &str,
        activated_epoch: u64,
    ) -> Result<bool> {
        let lock = self.begin_metadata_write_allow_frozen()?;
        if !self
            .mark_membership_handoff_write_freeze_activated_unlocked(handoff_id, activated_epoch)?
        {
            return Ok(false);
        }
        let previous_instance_authority_epoch =
            self.instance_authority_epoch.replace(activated_epoch);
        drop(lock);

        // Backend open deliberately defers append-producing recovery while a
        // freeze survives a crash. The durable activated-generation marker
        // remains in place throughout recovery: only this freshly rebound
        // instance can write, while independently opened stale instances stay
        // fenced. Roll the local binding back if recovery is incomplete so a
        // later fresh reconciliation must retry it.
        let recovery = self
            .recover_namespace_transactions()
            .and_then(|_| self.recover_visible_version_commits())
            .and_then(|_| self.recover_object_locations_from_visible_versions());
        if let Err(error) = recovery {
            self.instance_authority_epoch = previous_instance_authority_epoch;
            return Err(error);
        }
        Ok(true)
    }

    fn append_and_apply_table_ops(
        &mut self,
        sequence: u64,
        ops: Vec<DfsMetadataTableOp>,
    ) -> Result<()> {
        let root = self.root.clone();
        metadata_table_apply::append_and_apply_table_ops(
            &root,
            self.table_materialization_state(),
            sequence,
            ops,
            DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES,
        )
    }

    fn append_preapplied_table_ops(
        &mut self,
        sequence: u64,
        ops: Vec<DfsMetadataTableOp>,
        rollback: DfsMetadataTableRollback,
    ) -> Result<()> {
        let root = self.root.clone();
        metadata_table_apply::append_preapplied_table_ops(
            &root,
            self.table_materialization_state(),
            sequence,
            ops,
            DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES,
            rollback,
        )
    }

    fn compact_metadata_table_log_if_needed(&mut self, max_log_bytes: u64) -> Result<()> {
        let root = self.root.clone();
        metadata_table_apply::compact_metadata_table_log_if_needed(
            &root,
            self.table_materialization_state(),
            max_log_bytes,
        )
    }

    #[cfg(test)]
    fn compact_metadata_table_log_with_namespace_retention(
        &mut self,
        max_log_bytes: u64,
        checkpoint_time: OffsetDateTime,
        retention_limits: crate::metadata_tables::DfsNamespaceReceiptRetentionLimits,
    ) -> Result<()> {
        let root = self.root.clone();
        metadata_table_apply::compact_metadata_table_log_if_needed_with_retention(
            &root,
            self.table_materialization_state(),
            max_log_bytes,
            checkpoint_time,
            retention_limits,
        )
    }

    fn table_materialization_state(&mut self) -> TableMaterializationState<'_> {
        TableMaterializationState {
            tables: &mut self.tables,
            table_log_offset: &mut self.table_log_offset,
            materialized_through_sequence: &mut self.materialized_through_sequence,
            table_snapshot_fingerprint: &mut self.table_snapshot_fingerprint,
            prefix_proof: &mut self.prefix_proof,
        }
    }

    fn refresh_metadata_tables(&mut self) -> Result<()> {
        metadata_read_snapshot::refresh_metadata_tables(
            &self.root,
            &mut self.tables,
            &mut self.table_log_offset,
            &mut self.materialized_through_sequence,
            &mut self.table_snapshot_fingerprint,
            &mut self.prefix_proof,
        )
    }

    pub(crate) fn current_tables_snapshot(&self) -> Result<DfsMetadataTablesSnapshot<'_>> {
        #[cfg(test)]
        METADATA_CURRENT_TABLES_SNAPSHOT_CALLS.set(
            METADATA_CURRENT_TABLES_SNAPSHOT_CALLS
                .get()
                .saturating_add(1),
        );
        metadata_read_snapshot::current_tables_snapshot(
            &self.root,
            &self.tables,
            self.table_log_offset,
            self.materialized_through_sequence,
            &self.table_snapshot_fingerprint,
            &self.read_snapshot_cache,
        )
    }

    /// Collect retained compacted deletion evidence for a bounded set of
    /// object IDs while acquiring only one current table snapshot.
    ///
    /// This is kept on the concrete local backend rather than the general
    /// metadata API: user-facing object reads must continue to treat compacted
    /// tombstones as absent. Point lookups are deliberate here: walking the
    /// compacted map from its beginning makes a late migration cursor perform
    /// work proportional to the already-settled prefix on every pass.
    pub(crate) fn compacted_tombstones_for_object_ids(
        &self,
        object_ids: &BTreeSet<DfsObjectId>,
    ) -> Result<Vec<DfsObjectMetadata>> {
        if object_ids.is_empty() {
            return Ok(Vec::new());
        }
        let tables = self.current_tables_snapshot()?;
        Ok(object_ids
            .iter()
            .filter_map(|object_id| tables.compacted_tombstones.get(object_id).cloned())
            .collect())
    }

    pub(crate) fn object_or_compacted_tombstone(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .objects
            .get(object_id)
            .or_else(|| tables.compacted_tombstones.get(object_id))
            .cloned())
    }

    /// Install a source-eviction marker only if both the complete source
    /// journal prefix and object generation still match the migration fence.
    ///
    /// The journal check and tombstone append share one metadata write lock.
    /// A family mutation that races the final tail read therefore either lands
    /// before the check (and asks the caller to replay) or after the durable
    /// eviction; it cannot be silently stranded between them.
    pub(crate) fn put_cross_shard_tombstone_if_unchanged(
        &mut self,
        expected_source: &DfsObjectMetadata,
        target_object: &DfsObjectMetadata,
        source_shard_id: &DfsMetadataShardId,
        target_shard_id: &DfsMetadataShardId,
        expected_journal_sequence: u64,
    ) -> Result<DfsConditionalSourceEviction> {
        let _lock = self.begin_metadata_write()?;
        let current = self
            .tables
            .objects
            .get(&expected_source.object_id)
            .or_else(|| {
                self.tables
                    .compacted_tombstones
                    .get(&expected_source.object_id)
            })
            .cloned();
        if current.as_ref().is_some_and(|current| {
            matches!(
                current.tombstone_provenance.as_ref(),
                Some(crate::DfsTombstoneProvenance::SourceEviction {
                    source_shard_id: recorded_source,
                    target_shard_id: recorded_target,
                }) if recorded_source == source_shard_id && recorded_target == target_shard_id
            )
        }) {
            return Ok(DfsConditionalSourceEviction::AlreadyApplied);
        }
        if self.journal_last_sequence() != expected_journal_sequence {
            return Ok(DfsConditionalSourceEviction::JournalAdvanced);
        }
        if current.as_ref() != Some(expected_source) {
            return Ok(DfsConditionalSourceEviction::SourceChanged);
        }
        let Some(tombstone) = crate::metadata_shard_migration::cross_shard_eviction_tombstone(
            expected_source,
            target_object,
            source_shard_id,
            target_shard_id,
        ) else {
            return Ok(DfsConditionalSourceEviction::AlreadyApplied);
        };
        let tombstone = normalized_object_metadata(&tombstone);
        let table_ops = self.object_index_table_ops_with_previous(&tombstone, current.as_ref())?;
        let entry = self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_object(&tombstone),
            table_ops,
        )?;
        Ok(DfsConditionalSourceEviction::Applied {
            sequence: entry.sequence,
        })
    }

    /// Exact receipt lookup for bounded migration/install work.
    ///
    /// The public metadata trait exposes receipt pages because ordinary
    /// callers scan sets. Migration already has the complete composite key;
    /// paging from the first replica for every equality check would turn a
    /// bounded copy into quadratic work for objects with many receipts.
    pub(crate) fn receipt_for_replica(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsReplicaReceiptRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .receipts
            .get(&(object_id.clone(), version.clone(), replica_node_id.clone()))
            .cloned())
    }

    /// Return one object-id-ordered page from a single shard snapshot.
    ///
    /// Default-shard migration must merge duplicate object generations across
    /// authorities before choosing a canonical record. Path-ordered pages
    /// cannot do that without retaining the whole catalog because the same
    /// object may have different paths after an interrupted rename. The
    /// filesystem/object secondary table gives the migration lane a stable,
    /// bounded k-way cursor without changing the public metadata API.
    pub(crate) fn objects_for_file_system_by_id_page(
        &self,
        file_system_id: &DfsFileSystemId,
        after_object_id: Option<&DfsObjectId>,
        max_entries: usize,
    ) -> Result<(Vec<DfsObjectMetadata>, Option<DfsObjectId>)> {
        if max_entries == 0 {
            return Ok((Vec::new(), None));
        }
        let tables = self.current_tables_snapshot()?;
        let start = after_object_id.map_or_else(
            || {
                Included((
                    file_system_id.clone(),
                    crate::metadata_keys::id_lower_bound(),
                ))
            },
            |object_id| Excluded((file_system_id.clone(), object_id.clone())),
        );
        let mut objects = Vec::with_capacity(max_entries.saturating_add(1));
        for ((record_file_system_id, object_id), path_record) in tables
            .file_system_objects
            .range((start, Unbounded))
            .take_while(|((record_file_system_id, _), _)| record_file_system_id == file_system_id)
        {
            if record_file_system_id != file_system_id
                || path_record.file_system_id != *file_system_id
                || path_record.object_id != *object_id
            {
                continue;
            }
            let Some(object) = tables.objects.get(object_id) else {
                continue;
            };
            if object.file_system_id != *file_system_id
                || normalize_path(&object.path) != path_record.path
            {
                continue;
            }
            objects.push(object.clone());
            if objects.len() > max_entries {
                break;
            }
        }
        let has_more = objects.len() > max_entries;
        if has_more {
            objects.truncate(max_entries);
        }
        let next_after = has_more
            .then(|| objects.last().map(|object| object.object_id.clone()))
            .flatten();
        Ok((objects, next_after))
    }

    pub(crate) fn child_objects_for_directory_page_entries(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPageEntries> {
        let tables = self.current_tables_snapshot()?;
        Ok(child_objects_for_directory_page_entries_from_tables(
            &tables,
            file_system_id,
            directory_path,
            after,
            max_entries,
        ))
    }
}

impl DfsMetadataBackend for LocalSobjMetadataBackend {
    fn put_file_system(&mut self, file_system: &DfsFileSystem) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_file_system(file_system),
            vec![DfsMetadataTableOp::PutFileSystem {
                file_system: file_system.clone(),
            }],
        )
    }

    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.file_systems.get(file_system_id).cloned())
    }

    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.file_systems.values().cloned().collect())
    }

    fn put_object(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let object = normalized_object_metadata(object);
        if self.get_file_system(&object.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: object.file_system_id.clone(),
            });
        }

        let previous = self.get_object(&object.object_id)?;
        self.write_journal_entry_and_materialize_unlocked(
            DfsMetadataMutation::put_object(&object),
            |backend| backend.object_index_table_ops_with_previous(&object, previous.as_ref()),
        )
    }

    fn add_object_location_if_current(
        &mut self,
        object_id: &DfsObjectId,
        expected_version: &DfsVersion,
        expected_content_hash: &str,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsObjectMetadata>> {
        let _lock = self.begin_metadata_write()?;
        let Some(mut object) = self.get_object(object_id)? else {
            return Ok(None);
        };
        if object.kind == DfsObjectKind::Tombstone
            || &object.version != expected_version
            || object.content_hash.as_deref() != Some(expected_content_hash)
        {
            return Ok(None);
        }

        if !object.current_locations.contains(replica_node_id) {
            object.current_locations.push(replica_node_id.clone());
        }
        object = normalized_object_metadata(&object);

        let existing_version = self.get_version(object_id, expected_version)?;
        let mut version = existing_version
            .clone()
            .unwrap_or_else(|| object_version_record(&object, object.content_size_bytes));
        version.locations = object.current_locations.clone();
        version = normalized_version_record(&version);
        if self.get_object(object_id)?.as_ref() == Some(&object)
            && existing_version.as_ref() == Some(&version)
        {
            return Ok(Some(object));
        }

        let transaction = normalized_namespace_transaction(&DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id_for_batch(
                "location-repair",
                &object.file_system_id,
                std::slice::from_ref(&object),
                std::slice::from_ref(&version),
            ),
            file_system_id: object.file_system_id.clone(),
            objects: vec![object.clone()],
            versions: vec![version],
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::now_utc(),
        });
        validate_namespace_transaction_preconditions(&self.tables, &transaction)?;
        let table_ops = namespace_transaction_table_ops(&self.tables, &transaction);
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::ApplyNamespaceTransaction {
                file_system_id: transaction.file_system_id.clone(),
                transaction_id: transaction.transaction_id.clone(),
                object_count: transaction.objects.len(),
                transaction: Some(transaction),
            },
            table_ops,
        )?;
        Ok(Some(object))
    }

    fn put_objects(
        &mut self,
        objects: &[DfsObjectMetadata],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        if objects.is_empty() {
            return Ok(Vec::new());
        }
        let _lock = self.begin_metadata_write()?;
        let objects = objects
            .iter()
            .map(normalized_object_metadata)
            .collect::<Vec<_>>();
        for object in &objects {
            if self.get_file_system(&object.file_system_id)?.is_none() {
                return Err(MetadataBackendError::UnknownFileSystem {
                    file_system_id: object.file_system_id.clone(),
                });
            }
        }
        let entries = self.write_journal_entries_unlocked(
            objects
                .iter()
                .map(DfsMetadataMutation::put_object)
                .collect(),
        )?;
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        for object in &objects {
            let previous = self.get_object(&object.object_id)?;
            if let Err(error) = self.plan_object_index_table_ops_with_previous(
                object,
                previous.as_ref(),
                &mut table_ops,
                &mut rollback,
            ) {
                rollback.rollback(&mut self.tables);
                return Err(error);
            }
        }
        let last_sequence = entries
            .last()
            .map(|entry| entry.sequence)
            .unwrap_or_else(|| self.next_sequence.saturating_sub(1));
        self.append_preapplied_table_ops_for_written_entries(
            &entries,
            last_sequence,
            table_ops,
            rollback,
        )?;
        Ok(entries)
    }

    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.objects.get(object_id).cloned())
    }

    fn get_advisory_lock_state(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsAdvisoryLockStateRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .advisory_lock_states
            .get(object_id)
            .filter(|state| state.file_system_id == *file_system_id)
            .cloned())
    }

    fn apply_advisory_lock_command(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        context: DfsAdvisoryLockTransitionContext,
        command: &DfsAdvisoryLockCommand,
    ) -> Result<DfsAdvisoryLockApplyResult> {
        let _lock = self.begin_metadata_write()?;
        let object = self.tables.objects.get(object_id).ok_or_else(|| {
            MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            }
        })?;
        if object.file_system_id != *file_system_id {
            return Err(MetadataBackendError::AdvisoryLock(format!(
                "object {object_id} belongs to filesystem {}, not {file_system_id}",
                object.file_system_id
            )));
        }
        let transition = transition_advisory_lock_state(
            self.tables.advisory_lock_states.get(object_id),
            file_system_id,
            object_id,
            context,
            command,
        )
        .map_err(|error| MetadataBackendError::AdvisoryLock(error.to_string()))?;
        let journal_entry = if transition.changed {
            Some(self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_advisory_lock_state(&transition.state),
                vec![DfsMetadataTableOp::PutAdvisoryLockState {
                    state: transition.state,
                }],
            )?)
        } else {
            None
        };
        let durable_through_sequence = journal_entry
            .as_ref()
            .map(|entry| entry.sequence)
            .unwrap_or_else(|| self.journal_last_sequence());
        Ok(DfsAdvisoryLockApplyResult {
            outcome: transition.outcome,
            shard_id: self.shard_id.clone(),
            durable_through_sequence,
            journal_entry,
        })
    }

    fn install_advisory_lock_state_for_migration(
        &mut self,
        state: &DfsAdvisoryLockStateRecord,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        LocalSobjMetadataBackend::install_advisory_lock_state_for_migration(self, state)
    }

    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(object_by_path_from_tables(&tables, file_system_id, path))
    }

    fn namespace_prepare_expectations(
        &self,
        _shard_id: &DfsMetadataShardId,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<(
        Vec<DfsNamespaceObjectVersionExpectation>,
        Vec<DfsNamespacePathOccupantExpectation>,
        BTreeSet<String>,
    )> {
        let tables = self.current_tables_snapshot()?;
        namespace_prepare_expectations_from_lookups(
            transaction,
            |object_id| Ok(tables.objects.get(object_id).cloned()),
            |file_system_id, path| Ok(object_by_path_from_tables(&tables, file_system_id, path)),
        )
    }

    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        collect_metadata_pages(|cursor| {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.objects, page.next_cursor))
        })
    }

    fn objects_for_file_system_page(
        &self,
        file_system_id: &DfsFileSystemId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(objects_for_file_system_page_from_tables(
            &tables,
            file_system_id,
            after,
            max_entries,
        ))
    }

    fn count_objects_for_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<usize> {
        let tables = self.current_tables_snapshot()?;
        Ok(count_objects_for_file_system_from_tables(
            &tables,
            file_system_id,
        ))
    }

    fn child_tombstones_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        let directory_path = normalize_path(directory_path);
        let prefix = if directory_path.is_empty() {
            String::new()
        } else {
            format!("{directory_path}/")
        };
        let is_direct_child = |path: &str| {
            let Some(rest) = path.strip_prefix(&prefix) else {
                return false;
            };
            !rest.is_empty() && !rest.contains('/')
        };
        let mut tombstones = BTreeMap::<String, DfsObjectMetadata>::new();
        for ((_record_file_system_id, path), path_record) in tables
            .paths
            .range((
                Included((file_system_id.clone(), prefix.clone())),
                Unbounded,
            ))
            .take_while(|((record_file_system_id, path), _)| {
                record_file_system_id == file_system_id
                    && (prefix.is_empty() || path.starts_with(&prefix))
            })
        {
            if !is_direct_child(path) {
                continue;
            }
            let object = tables
                .objects
                .get(&path_record.object_id)
                .cloned()
                .or_else(|| tables.compacted_tombstone_by_path(file_system_id, path));
            let Some(object) = object.filter(|object| object.kind == DfsObjectKind::Tombstone)
            else {
                continue;
            };
            tombstones.insert(path.clone(), object);
        }
        for ((record_file_system_id, path), object_id) in tables
            .compacted_tombstone_paths
            .range((
                Included((file_system_id.clone(), prefix.clone())),
                Unbounded,
            ))
            .take_while(|((record_file_system_id, path), _)| {
                record_file_system_id == file_system_id
                    && (prefix.is_empty() || path.starts_with(&prefix))
            })
        {
            if !is_direct_child(path) {
                continue;
            }
            if let Some(object) = tables.compacted_tombstones.get(object_id)
                && object.file_system_id == *record_file_system_id
                && object.kind == DfsObjectKind::Tombstone
            {
                tombstones
                    .entry(path.clone())
                    .or_insert_with(|| object.clone());
            }
        }
        Ok(tombstones.into_values().collect())
    }

    fn child_objects_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(child_objects_for_directory_from_tables(
            &tables,
            file_system_id,
            directory_path,
        ))
    }

    fn child_objects_for_directory_page(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        let page = self.child_objects_for_directory_page_entries(
            file_system_id,
            directory_path,
            after,
            max_entries,
        )?;
        Ok(DfsDirectoryPage {
            objects: page.entries.into_iter().map(|(object, _)| object).collect(),
            next_cursor: page.next_cursor,
        })
    }

    fn hardlinks_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        collect_metadata_pages(|cursor| {
            let page = self.hardlinks_to_object_page(
                file_system_id,
                target_object_id,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.objects, page.next_cursor))
        })
    }

    fn hardlinks_to_object_page(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(hardlinks_to_object_page_from_tables(
            &tables,
            file_system_id,
            target_object_id,
            after,
            max_entries,
        ))
    }

    fn hardlink_count_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<usize> {
        let tables = self.current_tables_snapshot()?;
        Ok(hardlink_count_to_object_from_tables(
            &tables,
            file_system_id,
            target_object_id,
        ))
    }

    fn first_hardlink_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        Ok(self
            .hardlinks_to_object_page(file_system_id, target_object_id, None, 1)?
            .objects
            .into_iter()
            .next())
    }

    fn first_hardlink_to_object_outside_prefix(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(first_hardlink_to_object_outside_prefix_from_tables(
            &tables,
            file_system_id,
            target_object_id,
            prefix,
        ))
    }

    fn put_version(&mut self, version: &DfsObjectVersionRecord) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let version = normalized_version_record(version);
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_version(&version),
            vec![DfsMetadataTableOp::PutVersion { version }],
        )
    }

    fn put_object_and_version(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        let object = normalized_object_metadata(object);
        let version = normalized_version_record(version);
        let metadata_matches = object.file_system_id == version.file_system_id
            && object.object_id == version.object_id;
        if !metadata_matches {
            return Err(MetadataBackendError::Serde(format!(
                "object {} {} does not match version {} {}",
                object.file_system_id, object.object_id, version.file_system_id, version.object_id
            )));
        }
        if self.get_file_system(&object.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: object.file_system_id.clone(),
            });
        }
        let previous = self.get_object(&object.object_id)?;
        let entries = self.write_journal_entries_unlocked(vec![
            DfsMetadataMutation::put_object(&object),
            DfsMetadataMutation::put_version(&version),
        ])?;
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        if let Err(error) = self.plan_object_index_table_ops_with_previous(
            &object,
            previous.as_ref(),
            &mut table_ops,
            &mut rollback,
        ) {
            rollback.rollback(&mut self.tables);
            return Err(error);
        }
        self.plan_preapplied_table_op(
            DfsMetadataTableOp::PutVersion { version },
            &mut table_ops,
            &mut rollback,
        );
        let last_sequence = entries
            .last()
            .map(|entry| entry.sequence)
            .unwrap_or_else(|| self.next_sequence.saturating_sub(1));
        self.append_preapplied_table_ops_for_written_entries(
            &entries,
            last_sequence,
            table_ops,
            rollback,
        )?;
        Ok(entries)
    }

    fn put_object_version_intent_and_receipts(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let trace_started = Instant::now();
        let trace_object_id = object.object_id.clone();
        let trace_version = version.version.clone();
        let lock_started = Instant::now();
        let _lock = self.begin_metadata_write()?;
        let lock_ms = lock_started.elapsed().as_millis();
        let validate_started = Instant::now();
        let object = normalized_object_metadata(object);
        let version = normalized_version_record(version);
        let expected_version = intent.base_version.as_ref().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "combined content commit for object {} requires an existing base version",
                object.object_id
            ))
        })?;
        if object.file_system_id != version.file_system_id
            || object.object_id != version.object_id
            || object.file_system_id != intent.file_system_id
            || object.object_id != intent.object_id
            || object.version != version.version
            || object.version != intent.target_version
            || intent.status != DfsWriteIntentStatus::Committed
            || intent.touch_parent_on_create
        {
            return Err(MetadataBackendError::Serde(format!(
                "combined object/version/intent metadata mismatch for object {}",
                object.object_id
            )));
        }
        let current = self.get_object(&object.object_id)?.ok_or_else(|| {
            MetadataBackendError::UnknownObject {
                object_id: object.object_id.clone(),
            }
        })?;
        if &current.version != expected_version {
            return Err(MetadataBackendError::Conflict {
                object_id: object.object_id.clone(),
                expected: expected_version.clone(),
                current: current.version,
            });
        }
        let Some(expected_content_hash) = intent.content_hash.as_deref() else {
            return Err(MetadataBackendError::Serde(format!(
                "combined content commit for object {} is missing its content hash",
                object.object_id
            )));
        };
        if object.content_hash.as_deref() != Some(expected_content_hash)
            || version.content_hash.as_deref() != Some(expected_content_hash)
            || !version.visible
        {
            return Err(MetadataBackendError::Serde(format!(
                "combined content commit hash/version mismatch for object {}",
                object.object_id
            )));
        }
        let mut required_replica_node_ids = BTreeSet::new();
        for receipt in receipts {
            if receipt.receipt.file_system_id != object.file_system_id
                || receipt.receipt.object_id != object.object_id
                || receipt.receipt.committed_version != version.version
                || receipt.content_hash.as_deref() != Some(expected_content_hash)
                || !intent
                    .desired_replica_node_ids
                    .contains(&receipt.replica_node_id)
            {
                return Err(MetadataBackendError::Serde(format!(
                    "combined receipt metadata mismatch for object {}",
                    object.object_id
                )));
            }
            let receipt_key = (
                object.object_id.clone(),
                version.version.clone(),
                receipt.replica_node_id.clone(),
            );
            if self
                .tables
                .receipts
                .get(&receipt_key)
                .is_some_and(|existing| existing.content_hash != receipt.content_hash)
            {
                return Err(MetadataBackendError::Serde(format!(
                    "combined content commit conflicts with the durable receipt for object {} replica {}",
                    object.object_id, receipt.replica_node_id
                )));
            }
            required_replica_node_ids.insert(receipt.replica_node_id.clone());
        }
        if required_replica_node_ids.len() < intent.policy.min_commit_replicas {
            return Err(MetadataBackendError::Serde(format!(
                "combined content commit for object {} has {} durable receipts; {} required",
                object.object_id,
                required_replica_node_ids.len(),
                intent.policy.min_commit_replicas
            )));
        }
        if self.get_file_system(&object.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: object.file_system_id.clone(),
            });
        }
        let validate_ms = validate_started.elapsed().as_millis();
        let build_started = Instant::now();
        let required_replica_node_ids = required_replica_node_ids.into_iter().collect::<Vec<_>>();
        let mut pending_intent = intent.clone();
        pending_intent.status = DfsWriteIntentStatus::ReceiptsPending;
        pending_intent.object_metadata = Some(current);
        pending_intent.updated_at = Some(OffsetDateTime::now_utc());
        let visible_commit = visible_commit_record_from_parts(
            expected_version,
            &object,
            &version,
            &required_replica_node_ids,
            OffsetDateTime::now_utc(),
        );
        let mut mutations = Vec::with_capacity(3 + receipts.len());
        // Replay prefixes before CommitVisibleVersion retain the old visible
        // version. A prefix ending at the commit already contains the matching
        // pending intent and W receipts; recovery can terminalize that intent.
        mutations.push(DfsMetadataMutation::put_intent(&pending_intent));
        mutations.extend(receipts.iter().map(DfsMetadataMutation::put_receipt));
        mutations.push(DfsMetadataMutation::commit_visible_version(&visible_commit));
        mutations.push(DfsMetadataMutation::put_intent(intent));
        let build_ms = build_started.elapsed().as_millis();
        let journal_started = Instant::now();
        let entries = self.write_journal_entries_unlocked(mutations)?;
        let journal_ms = journal_started.elapsed().as_millis();
        let materialize_started = Instant::now();
        let planned = self.plan_written_journal_entries_table_ops(&entries)?;
        let materialize_ms = materialize_started.elapsed().as_millis();
        let table_append_started = Instant::now();
        self.append_preapplied_table_ops(
            planned.last_materialized_sequence,
            planned.table_ops,
            planned.rollback,
        )?;
        let table_append_ms = table_append_started.elapsed().as_millis();
        let checkpoint_started = Instant::now();
        if let Some(last_sequence) = entries.last().map(|entry| entry.sequence) {
            self.checkpoint_local_materialization_cursor_if_due(last_sequence)?;
        }
        let checkpoint_ms = checkpoint_started.elapsed().as_millis();
        metadata_backend_perf_trace(format_args!(
            "metadata-commit-object-version-intent-receipts shard={} object={} version={} receipts={} entries={} lock_ms={} validate_ms={} build_ms={} journal_ms={} materialize_ms={} table_append_ms={} checkpoint_ms={} total_ms={}",
            self.shard_id.0,
            trace_object_id,
            trace_version,
            receipts.len(),
            entries.len(),
            lock_ms,
            validate_ms,
            build_ms,
            journal_ms,
            materialize_ms,
            table_append_ms,
            checkpoint_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(entries)
    }

    fn commit_create_intent_receipts_and_namespace(
        &mut self,
        pending_intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
        transaction: &DfsNamespaceTransaction,
        committed_intent: &DfsWriteIntentRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let trace_started = Instant::now();
        let _lock = self.begin_metadata_write()?;
        let transaction = normalized_namespace_transaction(transaction);
        if self.get_file_system(&transaction.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: transaction.file_system_id.clone(),
            });
        }
        if pending_intent.status != DfsWriteIntentStatus::ReceiptsPending
            || committed_intent.status != DfsWriteIntentStatus::Committed
            || !pending_intent.touch_parent_on_create
            || !committed_intent.touch_parent_on_create
            || pending_intent.intent_id != committed_intent.intent_id
            || pending_intent.file_system_id != committed_intent.file_system_id
            || pending_intent.object_id != committed_intent.object_id
            || pending_intent.base_version != committed_intent.base_version
            || pending_intent.target_version != committed_intent.target_version
            || pending_intent.owner_epoch != committed_intent.owner_epoch
            || pending_intent.desired_replica_node_ids != committed_intent.desired_replica_node_ids
            || pending_intent.policy != committed_intent.policy
            || pending_intent.content_hash != committed_intent.content_hash
            || pending_intent.create_predecessor_object_id
                != committed_intent.create_predecessor_object_id
            || pending_intent.file_system_id != transaction.file_system_id
        {
            return Err(MetadataBackendError::Serde(format!(
                "combined create intent transition mismatch for object {}",
                pending_intent.object_id
            )));
        }
        let child = transaction
            .objects
            .iter()
            .find(|object| object.object_id == pending_intent.object_id)
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "combined create transaction {} is missing child object {}",
                    transaction.transaction_id, pending_intent.object_id
                ))
            })?;
        let child_version = transaction
            .versions
            .iter()
            .find(|version| {
                version.object_id == pending_intent.object_id
                    && version.version == pending_intent.target_version
            })
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "combined create transaction {} is missing child version {}",
                    transaction.transaction_id, pending_intent.target_version
                ))
            })?;
        if self.get_object(&child.object_id)?.is_some() {
            return Err(MetadataBackendError::Serde(format!(
                "combined create object {} is already durable",
                child.object_id
            )));
        }
        let Some(expected_content_hash) = pending_intent.content_hash.as_deref() else {
            return Err(MetadataBackendError::Serde(format!(
                "combined create for object {} is missing its content hash",
                pending_intent.object_id
            )));
        };
        if child.version != pending_intent.target_version
            || child.file_system_id != pending_intent.file_system_id
            || child.content_hash.as_deref() != Some(expected_content_hash)
            || child_version.file_system_id != child.file_system_id
            || child_version.content_hash.as_deref() != Some(expected_content_hash)
            || child_version.size_bytes != child.content_size_bytes
            || child_version.locations != child.current_locations
            || child_version.created_by_event_id != child.last_modified_by_event_id
            || !child_version.visible
        {
            return Err(MetadataBackendError::Serde(format!(
                "combined create child/version mismatch for object {}",
                pending_intent.object_id
            )));
        }
        let normalized_child_path = normalize_path(&child.path);
        let child_path_occupant = object_by_path_from_tables(
            &self.tables,
            &pending_intent.file_system_id,
            &normalized_child_path,
        );
        if child_path_occupant.as_ref().is_some_and(|occupant| {
            occupant.kind != DfsObjectKind::Tombstone
                || occupant.object_id == child.object_id
                || pending_intent.create_predecessor_object_id.as_ref() != Some(&occupant.object_id)
        }) {
            return Err(MetadataBackendError::Serde(format!(
                "combined create path {} is already occupied",
                child.path
            )));
        }
        let parent_path = normalized_child_path
            .rsplit_once('/')
            .map(|(parent, _)| parent)
            .unwrap_or("");
        let durable_parent =
            object_by_path_from_tables(&self.tables, &pending_intent.file_system_id, parent_path)
                .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "combined create parent {} is not durable",
                    parent_path
                ))
            })?;
        let touched_parent = transaction
            .objects
            .iter()
            .find(|object| normalize_path(&object.path) == parent_path)
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "combined create transaction {} is missing parent {}",
                    transaction.transaction_id, parent_path
                ))
            })?;
        let mut parent_transition_base = touched_parent.clone();
        parent_transition_base.version = durable_parent.version.clone();
        parent_transition_base.modified_at_unix_nanos = durable_parent.modified_at_unix_nanos;
        parent_transition_base.changed_at_unix_nanos = durable_parent.changed_at_unix_nanos;
        parent_transition_base.last_modified_by_event_id =
            durable_parent.last_modified_by_event_id.clone();
        let touched_parent_version = transaction.versions.iter().find(|version| {
            version.object_id == touched_parent.object_id
                && version.version == touched_parent.version
                && version.visible
        });
        if durable_parent.kind != DfsObjectKind::Directory
            || touched_parent.kind != DfsObjectKind::Directory
            || touched_parent.object_id != durable_parent.object_id
            || touched_parent.version == durable_parent.version
            || touched_parent.modified_at_unix_nanos <= durable_parent.modified_at_unix_nanos
            || touched_parent.changed_at_unix_nanos <= durable_parent.changed_at_unix_nanos
            || touched_parent.last_modified_by_event_id.is_none()
            || parent_transition_base != durable_parent
            || touched_parent_version.is_none_or(|version| {
                version.file_system_id != touched_parent.file_system_id
                    || version.content_hash != touched_parent.content_hash
                    || version.size_bytes != touched_parent.content_size_bytes
                    || version.locations != touched_parent.current_locations
                    || version.created_by_event_id != touched_parent.last_modified_by_event_id
            })
        {
            return Err(MetadataBackendError::Serde(format!(
                "combined create parent transition mismatch for {}: durable={:?} {} {}, touched={:?} {} {}, version_record={}",
                parent_path,
                durable_parent.kind,
                durable_parent.object_id,
                durable_parent.version,
                touched_parent.kind,
                touched_parent.object_id,
                touched_parent.version,
                touched_parent_version.is_some()
            )));
        }
        let mut durable_replica_node_ids = BTreeSet::new();
        for receipt in receipts {
            if receipt.receipt.file_system_id != pending_intent.file_system_id
                || receipt.receipt.object_id != pending_intent.object_id
                || receipt.receipt.committed_version != pending_intent.target_version
                || receipt.content_hash.as_deref() != Some(expected_content_hash)
                || !pending_intent
                    .desired_replica_node_ids
                    .contains(&receipt.replica_node_id)
            {
                return Err(MetadataBackendError::Serde(format!(
                    "combined create receipt mismatch for object {}",
                    pending_intent.object_id
                )));
            }
            let receipt_key = (
                pending_intent.object_id.clone(),
                pending_intent.target_version.clone(),
                receipt.replica_node_id.clone(),
            );
            if self
                .tables
                .receipts
                .get(&receipt_key)
                .is_some_and(|existing| existing.content_hash != receipt.content_hash)
            {
                return Err(MetadataBackendError::Serde(format!(
                    "combined create conflicts with the durable receipt for object {} replica {}",
                    pending_intent.object_id, receipt.replica_node_id
                )));
            }
            durable_replica_node_ids.insert(receipt.replica_node_id.clone());
        }
        if durable_replica_node_ids.len() < pending_intent.policy.min_commit_replicas {
            return Err(MetadataBackendError::Serde(format!(
                "combined create for object {} has {} durable receipts; {} required",
                pending_intent.object_id,
                durable_replica_node_ids.len(),
                pending_intent.policy.min_commit_replicas
            )));
        }
        validate_namespace_transaction_preconditions(&self.tables, &transaction)?;

        let mut mutations = Vec::with_capacity(receipts.len() + 3);
        mutations.push(DfsMetadataMutation::put_intent(pending_intent));
        mutations.extend(receipts.iter().map(DfsMetadataMutation::put_receipt));
        mutations.push(DfsMetadataMutation::ApplyNamespaceTransaction {
            file_system_id: transaction.file_system_id.clone(),
            transaction_id: transaction.transaction_id.clone(),
            object_count: transaction.objects.len(),
            transaction: Some(transaction),
        });
        mutations.push(DfsMetadataMutation::put_intent(committed_intent));
        let entries = self.write_journal_entries_unlocked(mutations)?;
        let planned = self.plan_written_journal_entries_table_ops(&entries)?;
        self.append_preapplied_table_ops_for_written_entries(
            &entries,
            planned.last_materialized_sequence,
            planned.table_ops,
            planned.rollback,
        )?;
        metadata_backend_perf_trace(format_args!(
            "metadata-commit-create-intent-receipts-namespace shard={} object={} version={} receipts={} entries={} total_ms={}",
            self.shard_id.0,
            pending_intent.object_id,
            pending_intent.target_version,
            receipts.len(),
            entries.len(),
            trace_started.elapsed().as_millis()
        ));
        Ok(entries)
    }

    fn get_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsObjectVersionRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .versions
            .get(&(object_id.clone(), version.clone()))
            .cloned())
    }

    fn get_object_version_pairs(
        &self,
        keys: &[(DfsObjectId, DfsVersion)],
    ) -> Result<Vec<(Option<DfsObjectMetadata>, Option<DfsObjectVersionRecord>)>> {
        let tables = self.current_tables_snapshot()?;
        Ok(keys
            .iter()
            .map(|(object_id, version)| {
                (
                    tables.objects.get(object_id).cloned(),
                    tables
                        .versions
                        .get(&(object_id.clone(), version.clone()))
                        .cloned(),
                )
            })
            .collect())
    }

    fn versions_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsObjectVersionRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.versions_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.versions, page.next_cursor))
        })
    }

    fn versions_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsObjectVersionPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(versions_for_object_page_from_tables(
            &tables,
            object_id,
            after,
            max_entries,
        ))
    }

    fn version_records_page(
        &self,
        after: Option<&DfsObjectVersionScanCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionScanPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(version_records_page_from_tables(
            &tables,
            after,
            max_entries,
        ))
    }

    fn receipt_records_page(
        &self,
        after: Option<&DfsReplicaReceiptScanCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptScanPage> {
        #[cfg(test)]
        self.receipt_scan_counters
            .global_pages
            .fetch_add(1, Ordering::Relaxed);
        let tables = self.current_tables_snapshot()?;
        Ok(receipt_records_page_from_tables(
            &tables,
            after,
            max_entries,
        ))
    }

    fn all_intents(&self) -> Result<Vec<DfsWriteIntentRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.write_intents_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.intents, page.next_cursor))
        })
    }

    fn conflict_records_page(
        &self,
        after: Option<&DfsConflictScanCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictScanPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(conflict_records_page_from_tables(
            &tables,
            after,
            max_entries,
        ))
    }

    fn metadata_summary(&self) -> Result<DfsMetadataSummary> {
        let tables = self.current_tables_snapshot()?;
        metadata_summary_from_tables(&tables, |_| Ok(true))
    }

    fn metadata_summary_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsMetadataSummary>> {
        let tables = self.current_tables_snapshot()?;
        metadata_summary_for_file_system_from_tables(&tables, file_system_id, |_| Ok(true))
    }

    fn put_receipt(
        &mut self,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_receipt(receipt),
            vec![DfsMetadataTableOp::PutReceipt {
                receipt: receipt.clone(),
            }],
        )
    }

    fn remove_receipt(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        let Some(existing) = self
            .tables
            .receipts
            .get(&(object_id.clone(), version.clone(), replica_node_id.clone()))
            .cloned()
        else {
            return Ok(None);
        };
        let entry = self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::RemoveReceipt {
                file_system_id: existing.receipt.file_system_id,
                object_id: object_id.clone(),
                version: version.clone(),
                replica_node_id: replica_node_id.clone(),
            },
            vec![DfsMetadataTableOp::RemoveReceipt {
                object_id: object_id.clone(),
                version: version.clone(),
                replica_node_id: replica_node_id.clone(),
            }],
        )?;
        Ok(Some(entry))
    }

    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.receipts_for_version_page(
                object_id,
                version,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.receipts, page.next_cursor))
        })
    }

    fn receipts_for_version_page(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        after: Option<&DfsReplicaReceiptPageCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptPage> {
        #[cfg(test)]
        self.receipt_scan_counters
            .object_version_pages
            .fetch_add(1, Ordering::Relaxed);
        let tables = self.current_tables_snapshot()?;
        Ok(receipts_for_version_page_from_tables(
            &tables,
            object_id,
            version,
            after,
            max_entries,
        ))
    }

    fn put_intent(&mut self, intent: &DfsWriteIntentRecord) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_intent(intent),
            vec![DfsMetadataTableOp::PutIntent {
                intent: intent.clone(),
            }],
        )
    }

    fn rebind_prepared_write_intent(
        &mut self,
        _object: Option<&DfsObjectMetadata>,
        expected_intent: &DfsWriteIntentRecord,
        rebound_intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let key = (
            expected_intent.object_id.clone(),
            expected_intent.intent_id.clone(),
        );
        let current = self.tables.intents.get(&key).cloned().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "write intent {} does not exist for object {}",
                expected_intent.intent_id, expected_intent.object_id
            ))
        })?;
        if current == *rebound_intent {
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_intent(rebound_intent),
                vec![DfsMetadataTableOp::PutIntent {
                    intent: rebound_intent.clone(),
                }],
            );
        }
        if current != *expected_intent {
            return Err(MetadataBackendError::Serde(format!(
                "write intent {} rebind record fence mismatch",
                expected_intent.intent_id
            )));
        }
        if !matches!(
            current.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        ) || !prepared_write_intent_rebind_payload_matches(&current, rebound_intent)
        {
            return Err(MetadataBackendError::Serde(format!(
                "write intent {} rebind changes the prepared operation payload or status",
                expected_intent.intent_id
            )));
        }
        if rebound_intent.owner_epoch <= current.owner_epoch {
            return Err(MetadataBackendError::Serde(format!(
                "write intent {} rebind epoch {} must be newer than {}",
                expected_intent.intent_id, rebound_intent.owner_epoch, current.owner_epoch
            )));
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_intent(rebound_intent),
            vec![DfsMetadataTableOp::PutIntent {
                intent: rebound_intent.clone(),
            }],
        )
    }

    fn put_intent_and_receipts_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let _ = object;
        let _lock = self.begin_metadata_write()?;
        let mut mutations = Vec::with_capacity(1 + receipts.len());
        mutations.push(DfsMetadataMutation::put_intent(intent));
        mutations.extend(receipts.iter().map(DfsMetadataMutation::put_receipt));
        let entries = self.write_journal_entries_unlocked(mutations)?;
        self.materialize_written_journal_entries_unlocked(&entries)?;
        Ok(entries)
    }

    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .intents
            .get(&(object_id.clone(), intent_id.to_string()))
            .cloned())
    }

    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.intents_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.intents, page.next_cursor))
        })
    }

    fn intents_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(intents_for_object_page_from_tables(
            &tables,
            object_id,
            after,
            max_entries,
        ))
    }

    fn intents_for_object_version_page(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(intents_for_object_version_page_from_tables(
            &tables,
            object_id,
            target_version,
            after,
            max_entries,
        ))
    }

    fn write_intents_page(
        &self,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentScanPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(write_intents_page_from_tables(&tables, after, max_entries))
    }

    fn write_intents_on_shard_page(
        &self,
        shard_id: &DfsMetadataShardId,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<Option<DfsWriteIntentScanPage>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        self.write_intents_page(after, max_entries).map(Some)
    }

    fn commit_visible_version(
        &mut self,
        expected_version: &DfsVersion,
        committed_object: &DfsObjectMetadata,
        visible_version: &DfsObjectVersionRecord,
        required_replica_node_ids: &[NodeId],
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let current = self
            .get_object(&committed_object.object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: committed_object.object_id.clone(),
            })?;
        if &current.version != expected_version {
            return Err(MetadataBackendError::Conflict {
                object_id: committed_object.object_id.clone(),
                expected: expected_version.clone(),
                current: current.version,
            });
        }

        for node_id in required_replica_node_ids {
            if !self.tables.receipts.contains_key(&(
                committed_object.object_id.clone(),
                committed_object.version.clone(),
                node_id.clone(),
            )) {
                return Err(MetadataBackendError::Conflict {
                    object_id: committed_object.object_id.clone(),
                    expected: expected_version.clone(),
                    current: current.version.clone(),
                });
            }
        }

        let commit_record = visible_commit_record_from_parts(
            expected_version,
            committed_object,
            visible_version,
            required_replica_node_ids,
            OffsetDateTime::now_utc(),
        );
        let table_ops = visible_commit_table_ops(&self.tables, &commit_record);
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::commit_visible_version(&commit_record),
            table_ops,
        )
    }

    fn apply_namespace_transaction(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        if self.get_file_system(&transaction.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: transaction.file_system_id.clone(),
            });
        }
        if transaction
            .objects
            .iter()
            .any(|object| object.file_system_id != transaction.file_system_id)
        {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: transaction.file_system_id.clone(),
            });
        }

        let transaction = normalized_namespace_transaction(transaction);
        if let Some(receipt) = self
            .tables
            .namespace_transaction_receipts
            .get(&transaction.transaction_id)
            .cloned()
        {
            if !receipt
                .matches_transaction(&transaction)
                .map_err(|source| MetadataBackendError::Serde(source.to_string()))?
            {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace transaction {} reuses a compacted transaction id with a different payload",
                    transaction.transaction_id
                )));
            }
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_namespace_transaction_receipt(&receipt),
                vec![DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt }],
            );
        }
        if self
            .tables
            .namespace_transaction_history_expired(&transaction)
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace transaction {} idempotency history expired or is unknown",
                transaction.transaction_id
            )));
        }
        validate_namespace_transaction_preconditions(&self.tables, &transaction)?;
        let entry =
            self.write_journal_unlocked(DfsMetadataMutation::ApplyNamespaceTransaction {
                file_system_id: transaction.file_system_id.clone(),
                transaction_id: transaction.transaction_id.clone(),
                object_count: transaction.objects.len(),
                transaction: Some(transaction),
            })?;
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let transaction = match &entry.mutation {
            DfsMetadataMutation::ApplyNamespaceTransaction {
                transaction: Some(transaction),
                ..
            } => transaction,
            _ => unreachable!("the namespace transaction journal entry preserves its payload"),
        };
        self.plan_namespace_transaction_table_ops(transaction, &mut table_ops, &mut rollback)?;
        self.append_preapplied_table_ops_for_written_entries(
            std::slice::from_ref(&entry),
            entry.sequence,
            table_ops,
            rollback,
        )?;
        Ok(entry)
    }

    fn prepare_namespace_transaction(
        &mut self,
        prepare: &DfsNamespacePrepareRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        if prepare.status != DfsNamespacePrepareStatus::Prepared {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} must enter with prepared status",
                prepare.operation_id
            )));
        }
        let prepare = normalized_namespace_prepare(prepare);
        if let Some(receipt) = self
            .tables
            .namespace_prepare_receipts
            .get(&prepare.operation_id)
            .cloned()
        {
            let canonical_digest =
                crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                    .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
            if prepare.operation_digest != canonical_digest
                || !receipt.matches_prepare_identity(&prepare)
            {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {} reuses a compacted operation id with a different payload",
                    prepare.operation_id
                )));
            }
            if receipt.status == DfsNamespacePrepareStatus::Aborted {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {} is aborted",
                    prepare.operation_id
                )));
            }
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_namespace_prepare_receipt(&receipt),
                vec![DfsMetadataTableOp::PutNamespacePrepareReceipt { receipt }],
            );
        }
        if self.tables.namespace_prepare_history_expired(&prepare) {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} idempotency history expired or is unknown",
                prepare.operation_id
            )));
        }
        if let Some(existing) = self
            .tables
            .namespace_prepares
            .get(&prepare.operation_id)
            .cloned()
            && existing.status == DfsNamespacePrepareStatus::Committed
            && namespace_prepare_payload_matches(&existing, &prepare)
            && namespace_transaction_recovery_is_complete(&self.tables, &existing.transaction)
            && namespace_prepare_content_receipts_are_complete(&self.tables, &existing)
            && namespace_prepare_request_completion_is_materialized(&self.tables, &existing)
        {
            // A lost response may retry the exact prepared operation after its
            // visible transaction already committed. Replicate the durable
            // committed marker again instead of attempting the forbidden
            // Committed -> Prepared transition.
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_namespace_prepare(&existing),
                vec![DfsMetadataTableOp::PutNamespacePrepare { prepare: existing }],
            );
        }
        let mut table_ops =
            namespace_prepare_table_ops_for_record(&self.tables, &self.shard_id, &prepare)?;
        if table_ops.is_empty() {
            table_ops.push(DfsMetadataTableOp::PutNamespacePrepare {
                prepare: prepare.clone(),
            });
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_namespace_prepare(&prepare),
            table_ops,
        )
    }

    fn rebind_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
        rebound_prepare: &DfsNamespacePrepareRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let current = self
            .tables
            .namespace_prepares
            .get(operation_id)
            .cloned()
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "namespace prepare {operation_id} does not exist"
                ))
            })?;
        let rebound = normalized_namespace_prepare(rebound_prepare);
        if rebound.operation_id != operation_id {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare rebind id {operation_id} does not match rebound id {}",
                rebound.operation_id
            )));
        }
        let canonical_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&rebound)
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        if rebound.operation_digest != canonical_digest {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} rebound digest is not canonical"
            )));
        }
        // A lost response may repeat the exact rebind after the first journal
        // entry became durable. Re-journal the same prepared record without
        // weakening the old-fence compare for any different replacement.
        if current == rebound {
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_namespace_prepare(&rebound),
                vec![DfsMetadataTableOp::PutNamespacePrepare { prepare: rebound }],
            );
        }
        if current.status != DfsNamespacePrepareStatus::Prepared
            || current.operation_digest != expected_operation_digest
            || current.owner_epoch != expected_owner_epoch
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} rebind digest/epoch fence mismatch"
            )));
        }
        if !namespace_prepare_rebind_payload_matches(&current, &rebound) {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} rebind changes the prepared operation payload"
            )));
        }
        if rebound.owner_epoch <= current.owner_epoch {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} rebind epoch {} must be newer than {}",
                rebound.owner_epoch, current.owner_epoch
            )));
        }
        validate_namespace_prepare_shape(&self.tables, &self.shard_id, &rebound)?;
        validate_namespace_prepare_expectations(&self.tables, &rebound)?;
        validate_namespace_prepare_does_not_overlap(&self.tables, &rebound)?;
        validate_namespace_transaction_preconditions(&self.tables, &rebound.transaction)?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_namespace_prepare(&rebound),
            vec![DfsMetadataTableOp::PutNamespacePrepare { prepare: rebound }],
        )
    }

    fn commit_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        if let Some(receipt) = self
            .tables
            .namespace_prepare_receipts
            .get(operation_id)
            .cloned()
        {
            if receipt.operation_digest != expected_operation_digest
                || receipt.owner_epoch != expected_owner_epoch
            {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {operation_id} digest/epoch fence mismatch"
                )));
            }
            if receipt.status == DfsNamespacePrepareStatus::Aborted {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {operation_id} is aborted"
                )));
            }
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_namespace_prepare_receipt(&receipt),
                vec![DfsMetadataTableOp::PutNamespacePrepareReceipt { receipt }],
            );
        }
        let mut prepare = self
            .tables
            .namespace_prepares
            .get(operation_id)
            .cloned()
            .ok_or_else(|| {
                if self.tables.namespace_prepare_history_has_evictions() {
                    MetadataBackendError::Serde(format!(
                        "namespace prepare {operation_id} idempotency history expired or is unknown"
                    ))
                } else {
                    MetadataBackendError::Serde(format!(
                        "namespace prepare {operation_id} does not exist"
                    ))
                }
            })?;
        if prepare.operation_digest != expected_operation_digest
            || prepare.owner_epoch != expected_owner_epoch
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} digest/epoch fence mismatch"
            )));
        }
        if prepare.status == DfsNamespacePrepareStatus::Aborted {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} is aborted"
            )));
        }
        let transitions_from_prepared = prepare.status == DfsNamespacePrepareStatus::Prepared;
        prepare.status = DfsNamespacePrepareStatus::Committed;

        if transitions_from_prepared {
            // Validate against the still-hidden namespace before making the
            // committed marker durable. Once the journal append succeeds,
            // materialize the transaction directly into the live tables and
            // capture rollback state for the table-batch append. This keeps
            // the normal Prepared -> Committed path off the full-table clone
            // used by replay/recovery while preserving the durable journal as
            // the source of truth across an append failure or crash.
            validate_namespace_prepare_shape(&self.tables, &self.shard_id, &prepare)?;
            validate_namespace_prepare_expectations(&self.tables, &prepare)?;
            validate_namespace_prepare_does_not_overlap(&self.tables, &prepare)?;
            validate_namespace_transaction_preconditions(&self.tables, &prepare.transaction)?;

            let entry =
                self.write_journal_unlocked(DfsMetadataMutation::put_namespace_prepare(&prepare))?;
            let mut table_ops = Vec::new();
            let mut rollback = DfsMetadataTableRollback::default();
            for receipt in &prepare.content_receipts {
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutReceipt {
                        receipt: receipt.clone(),
                    },
                    &mut table_ops,
                    &mut rollback,
                );
            }
            if let Err(error) = self.plan_namespace_transaction_table_ops(
                &prepare.transaction,
                &mut table_ops,
                &mut rollback,
            ) {
                rollback.rollback(&mut self.tables);
                return Err(error);
            }
            self.plan_preapplied_table_op(
                DfsMetadataTableOp::PutNamespacePrepare {
                    prepare: prepare.clone(),
                },
                &mut table_ops,
                &mut rollback,
            );
            if let Some(request) = prepare.completed_request.as_ref() {
                let key = (
                    request.requester_node_id.clone(),
                    request.request_id.clone(),
                );
                let materialized =
                    self.tables
                        .namespace_requests
                        .get(&key)
                        .is_some_and(|existing| {
                            completed_namespace_request_outcome_matches(existing, request)
                        })
                        || self
                            .tables
                            .namespace_request_receipts
                            .get(&key)
                            .is_some_and(|receipt| receipt.matches_completed_request(request));
                if !materialized {
                    self.plan_preapplied_table_op(
                        DfsMetadataTableOp::PutNamespaceRequest {
                            request: request.clone(),
                        },
                        &mut table_ops,
                        &mut rollback,
                    );
                }
            }
            self.append_preapplied_table_ops_for_written_entries(
                std::slice::from_ref(&entry),
                entry.sequence,
                table_ops,
                rollback,
            )?;
            return Ok(entry);
        }

        // A repeated commit can be repairing a durable Committed marker whose
        // namespace table batch was interrupted. Keep that idempotent recovery
        // path on the recovery/reference planner.
        let mut table_ops =
            namespace_prepare_table_ops_for_record(&self.tables, &self.shard_id, &prepare)?;
        if table_ops.is_empty() {
            table_ops.push(DfsMetadataTableOp::PutNamespacePrepare {
                prepare: prepare.clone(),
            });
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_namespace_prepare(&prepare),
            table_ops,
        )
    }

    fn abort_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        if let Some(receipt) = self
            .tables
            .namespace_prepare_receipts
            .get(operation_id)
            .cloned()
        {
            if receipt.operation_digest != expected_operation_digest
                || receipt.owner_epoch != expected_owner_epoch
            {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {operation_id} digest/epoch fence mismatch"
                )));
            }
            if receipt.status == DfsNamespacePrepareStatus::Committed {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {operation_id} is already committed"
                )));
            }
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_namespace_prepare_receipt(&receipt),
                vec![DfsMetadataTableOp::PutNamespacePrepareReceipt { receipt }],
            );
        }
        let mut prepare = self
            .tables
            .namespace_prepares
            .get(operation_id)
            .cloned()
            .ok_or_else(|| {
                if self.tables.namespace_prepare_history_has_evictions() {
                    MetadataBackendError::Serde(format!(
                        "namespace prepare {operation_id} idempotency history expired or is unknown"
                    ))
                } else {
                    MetadataBackendError::Serde(format!(
                        "namespace prepare {operation_id} does not exist"
                    ))
                }
            })?;
        if prepare.operation_digest != expected_operation_digest
            || prepare.owner_epoch != expected_owner_epoch
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} digest/epoch fence mismatch"
            )));
        }
        if prepare.status == DfsNamespacePrepareStatus::Committed {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} is already committed"
            )));
        }
        prepare.status = DfsNamespacePrepareStatus::Aborted;
        let mut table_ops =
            namespace_prepare_table_ops_for_record(&self.tables, &self.shard_id, &prepare)?;
        if table_ops.is_empty() {
            table_ops.push(DfsMetadataTableOp::PutNamespacePrepare {
                prepare: prepare.clone(),
            });
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_namespace_prepare(&prepare),
            table_ops,
        )
    }

    fn get_namespace_prepare(
        &self,
        operation_id: &str,
    ) -> Result<Option<DfsNamespacePrepareRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_prepares.get(operation_id).cloned())
    }

    fn list_prepared_namespace_prepares(&self) -> Result<Vec<DfsNamespacePrepareRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .namespace_prepares
            .values()
            .filter(|prepare| prepare.status == DfsNamespacePrepareStatus::Prepared)
            .cloned()
            .collect())
    }

    fn tombstone_compaction_projection(
        &self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        let tables = self.current_tables_snapshot()?;
        let now = options
            .now_unix_nanos
            .unwrap_or_else(metadata_now_unix_nanos);
        Ok(plan_tombstone_compaction(&tables, &options, now).report)
    }

    fn compact_tombstones(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        self.compact_tombstones_with_ids(options)
            .map(|(report, _)| report)
    }

    fn put_namespace_job(&mut self, job: &DfsNamespaceJobRecord) -> Result<()> {
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_namespace_job(job),
            vec![DfsMetadataTableOp::PutNamespaceJob { job: job.clone() }],
        )
        .map(drop)
    }

    fn put_namespace_request_on_shard(
        &mut self,
        shard_id: &DfsMetadataShardId,
        request: &DfsNamespaceRequestRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        if shard_id != &self.shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "namespace request shard {} does not match backend shard {}",
                shard_id.0, self.shard_id.0
            )));
        }
        validate_namespace_request_shape(request)?;
        let _lock = self.begin_metadata_write()?;
        let key = (
            request.requester_node_id.clone(),
            request.request_id.clone(),
        );
        if let Some(receipt) = self.tables.namespace_request_receipts.get(&key).cloned() {
            if !receipt.matches_completed_request(request) {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace request id {}/{} was reused after compaction with a different payload",
                    request.requester_node_id, request.request_id
                )));
            }
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_namespace_request_receipt(&receipt),
                vec![DfsMetadataTableOp::PutNamespaceRequestReceipt { receipt }],
            );
        }
        if let Some(existing) = self.tables.namespace_requests.get(&key)
            && !namespace_request_transition_is_valid(existing, request)
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace request id {}/{} was reused with a different payload or invalid status transition",
                request.requester_node_id, request.request_id
            )));
        }
        if !self.tables.namespace_requests.contains_key(&key)
            && self
                .tables
                .namespace_request_history_expired(&request.request_id)
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace request {}/{} idempotency history expired or is unknown",
                request.requester_node_id, request.request_id
            )));
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_namespace_request(request),
            vec![DfsMetadataTableOp::PutNamespaceRequest {
                request: request.clone(),
            }],
        )
    }

    fn get_namespace_request(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespaceRequestRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .namespace_requests
            .get(&(requester_node_id.clone(), request_id.to_string()))
            .cloned())
    }

    fn get_namespace_request_receipt(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespaceRequestReceipt>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .namespace_request_receipts
            .get(&(requester_node_id.clone(), request_id.to_string()))
            .cloned())
    }

    fn namespace_request_history_expired_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        request_id: &str,
    ) -> Result<bool> {
        if shard_id != &self.shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "namespace request retention shard {} does not match backend shard {}",
                shard_id.0, self.shard_id.0
            )));
        }
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_request_history_expired(request_id))
    }

    fn list_namespace_requests(&self) -> Result<Vec<DfsNamespaceRequestRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_requests.values().cloned().collect())
    }

    fn list_pending_namespace_requests_page(
        &self,
        after: Option<&DfsPendingNamespaceRequestPageCursor>,
        max_entries: usize,
    ) -> Result<DfsPendingNamespaceRequestPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(pending_namespace_requests_page_from_tables(
            &tables,
            after,
            max_entries,
        ))
    }

    fn get_prepared_namespace_prepare_for_request(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespacePrepareRecord>> {
        let tables = self.current_tables_snapshot()?;
        let request_key = (requester_node_id.clone(), request_id.to_string());
        let Some(operation_ids) = tables.namespace_prepare_request_owners.get(&request_key) else {
            return Ok(None);
        };
        if operation_ids.len() != 1 {
            return Err(MetadataBackendError::Serde(format!(
                "pending namespace request {requester_node_id}/{request_id} owns {} prepared transactions",
                operation_ids.len()
            )));
        }
        let operation_id = operation_ids
            .first()
            .expect("non-empty request prepare owner index");
        let prepare = tables
            .namespace_prepares
            .get(operation_id)
            .cloned()
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "pending namespace request {requester_node_id}/{request_id} indexes missing prepare {operation_id}"
                ))
            })?;
        Ok(Some(prepare))
    }

    fn get_namespace_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Option<DfsNamespaceTransaction>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_transactions.get(transaction_id).cloned())
    }

    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_jobs.get(job_id).cloned())
    }

    fn list_namespace_jobs(&self) -> Result<Vec<DfsNamespaceJobRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.list_namespace_jobs_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.jobs, page.next_cursor))
        })
    }

    fn list_namespace_jobs_page(
        &self,
        after: Option<&DfsNamespaceJobPageCursor>,
        max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(namespace_jobs_page_from_tables(&tables, after, max_entries))
    }

    fn put_conflict(&mut self, conflict: &DfsConflictRecord) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::PutConflict {
                file_system_id: conflict.file_system_id.clone(),
                object_id: conflict.object_id.clone(),
                conflict_id: conflict.conflict_id.clone(),
                conflict: Some(conflict.clone()),
            },
            vec![DfsMetadataTableOp::PutConflict {
                conflict: conflict.clone(),
            }],
        )
    }

    fn get_conflict(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .conflicts
            .get(&(object_id.clone(), conflict_id.to_string()))
            .cloned())
    }

    fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsConflictRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(conflicts_for_object_from_tables(&tables, object_id))
    }

    fn conflicts_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsConflictPageCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(conflicts_for_object_page_from_tables(
            &tables,
            object_id,
            after,
            max_entries,
        ))
    }

    fn remove_conflict(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(existing) = self.get_conflict(object_id, conflict_id)? else {
            return Ok(None);
        };
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::PutConflict {
                file_system_id: existing.file_system_id.clone(),
                object_id: object_id.clone(),
                conflict_id: conflict_id.to_string(),
                conflict: None,
            },
            vec![DfsMetadataTableOp::RemoveConflict {
                object_id: object_id.clone(),
                conflict_id: conflict_id.to_string(),
            }],
        )?;
        Ok(Some(existing))
    }

    fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(mut conflict) = self.get_conflict(object_id, conflict_id)? else {
            return Ok(None);
        };
        conflict.status = status;
        self.put_conflict(&conflict)?;
        Ok(Some(conflict))
    }

    fn journal_since(&self, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>> {
        LocalSobjMetadataBackend::journal_since_limited(self, after_sequence, usize::MAX)
    }

    fn journal_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        LocalSobjMetadataBackend::journal_since_limited(self, after_sequence, max_entries)
    }

    fn journal_batch_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        LocalSobjMetadataBackend::journal_batch_since_limited(
            self,
            after_sequence,
            max_entries,
            max_encoded_bytes,
        )
    }

    fn journal_batch_for_shard_since_limited(
        &self,
        shard_id: &DfsMetadataShardId,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        LocalSobjMetadataBackend::journal_batch_since_limited(
            self,
            after_sequence,
            max_entries,
            max_encoded_bytes,
        )
    }

    fn journal_last_sequence(&self) -> Result<u64> {
        Ok(LocalSobjMetadataBackend::journal_last_sequence(self))
    }

    fn export_table_snapshot(&self) -> Result<DfsMetadataTableSnapshot> {
        Ok(LocalSobjMetadataBackend::export_table_snapshot(self))
    }

    fn export_shard_table_snapshot(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataTableSnapshot>> {
        if shard_id == &self.shard_id {
            return Ok(Some(LocalSobjMetadataBackend::export_table_snapshot(self)));
        }
        Ok(None)
    }

    fn metadata_prefix_proof(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixProof>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        LocalSobjMetadataBackend::metadata_prefix_proof(self).map(Some)
    }

    fn refresh_metadata_shard_durable_view(&mut self, shard_id: &DfsMetadataShardId) -> Result<()> {
        if shard_id != &self.shard_id {
            return Ok(());
        }
        let _lock = self.metadata_write_lock()?;
        self.refresh_after_metadata_write_lock()
    }

    fn metadata_shard_write_freeze(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        self.membership_handoff_write_freeze()
    }

    fn has_any_metadata_shard_write_freeze(&self) -> Result<bool> {
        Ok(self.membership_handoff_write_freeze()?.is_some())
    }

    fn metadata_shard_authoritative_writes_frozen(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        if shard_id != &self.shard_id {
            return Ok(false);
        }
        let Some(freeze) = self.membership_handoff_write_freeze()? else {
            return Ok(false);
        };
        Ok(!freeze
            .activated_epoch
            .is_some_and(|epoch| self.instance_authority_epoch == Some(epoch)))
    }

    fn freeze_metadata_shard_for_membership_handoff(
        &mut self,
        shard_id: &DfsMetadataShardId,
        handoff_id: &str,
        retiring_epoch: u64,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        self.freeze_for_membership_handoff(handoff_id, retiring_epoch)
            .map(Some)
    }

    fn mark_metadata_shard_membership_handoff_activated(
        &mut self,
        shard_id: &DfsMetadataShardId,
        handoff_id: &str,
        activated_epoch: u64,
    ) -> Result<bool> {
        if shard_id != &self.shard_id {
            return Ok(false);
        }
        self.mark_membership_handoff_write_freeze_activated(handoff_id, activated_epoch)
    }

    fn clear_metadata_shard_membership_handoff_freeze(
        &mut self,
        shard_id: &DfsMetadataShardId,
        handoff_id: &str,
        activated_epoch: u64,
    ) -> Result<bool> {
        if shard_id != &self.shard_id {
            return Ok(false);
        }
        self.clear_membership_handoff_write_freeze(handoff_id, activated_epoch)
    }

    fn metadata_replication_cursor(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64> {
        metadata_cursor::metadata_replication_cursor(&self.root, target_node_id, shard_id)
    }

    fn put_metadata_replication_cursor(
        &mut self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        next_after_sequence: u64,
    ) -> Result<()> {
        let _lock = self.begin_metadata_write_allow_frozen()?;
        metadata_cursor::put_metadata_replication_cursor(
            &self.root,
            target_node_id,
            shard_id,
            next_after_sequence,
        )
    }

    fn metadata_prefix_ack_floor(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixAckFloor>> {
        metadata_cursor::metadata_prefix_ack_floor(&self.root, shard_id)
    }

    fn put_metadata_prefix_ack_floor(&mut self, floor: &DfsMetadataPrefixAckFloor) -> Result<()> {
        let _lock = self.begin_metadata_write_allow_frozen()?;
        metadata_cursor::put_metadata_prefix_ack_floor(&self.root, floor)
    }

    fn metadata_authority_ack_floor(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataAuthorityAckFloor>> {
        metadata_cursor::metadata_authority_ack_floor(&self.root, shard_id)
    }

    fn put_metadata_authority_ack_floor(
        &mut self,
        floor: &DfsMetadataAuthorityAckFloor,
    ) -> Result<()> {
        let _lock = self.begin_metadata_write_allow_frozen()?;
        metadata_cursor::put_metadata_authority_ack_floor(&self.root, floor)
    }

    fn metadata_replay_cursor(&self, source_shard_id: &DfsMetadataShardId) -> Result<u64> {
        self.require_replication_source_shard(source_shard_id)?;
        let checkpoint =
            metadata_cursor::read_replay_cursor(&self.root, source_shard_id)?.last_sequence;
        Ok(self
            .prefix_proof
            .as_ref()
            .filter(|_| &self.shard_id == source_shard_id)
            .map(|proof| proof.journal_sequence.max(checkpoint))
            .unwrap_or(checkpoint))
    }

    fn metadata_shard_statuses(&self) -> Result<Vec<DfsMetadataShardStatus>> {
        let journal_next_sequence = self.next_sequence;
        Ok(vec![DfsMetadataShardStatus {
            shard_id: self.shard_id.clone(),
            file_system_id: None,
            primary_node_id: None,
            replica_node_ids: Vec::new(),
            epoch: None,
            journal_next_sequence,
            journal_last_sequence: journal_next_sequence.saturating_sub(1),
            replica_lag: Vec::new(),
            replay_cursors: self.replay_cursor_statuses()?,
        }])
    }

    fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize> {
        LocalSobjMetadataBackend::apply_journal_batch(self, batch)
    }

    fn apply_journal_batch_with_replica_permit(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<usize> {
        LocalSobjMetadataBackend::apply_journal_batch_with_replica_permit(self, permit, batch)
    }

    fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        LocalSobjMetadataBackend::replace_with_replicated_snapshot(
            self,
            source_shard_id,
            replayed_through_sequence,
            snapshot,
        )
    }

    fn replace_with_replicated_snapshot_with_replica_permit(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        LocalSobjMetadataBackend::replace_with_replicated_snapshot_with_replica_permit(
            self,
            permit,
            source_shard_id,
            replayed_through_sequence,
            snapshot,
        )
    }

    fn subscribe(&self, after_sequence: u64) -> DfsMetadataSubscription {
        DfsMetadataSubscription::new(self.shard_id.clone(), after_sequence)
    }

    fn poll_subscription(
        &self,
        subscription: &mut DfsMetadataSubscription,
    ) -> Result<DfsMetadataJournalBatch> {
        let entries = self.journal_since(subscription.next_after_sequence)?;
        if let Some(last) = entries.last() {
            subscription.next_after_sequence = last.sequence;
        }
        Ok(DfsMetadataJournalBatch {
            shard_id: self.shard_id.clone(),
            entries,
            next_after_sequence: subscription.next_after_sequence,
        })
    }
}

impl LocalSobjMetadataBackend {
    fn recover_namespace_transactions(&mut self) -> Result<()> {
        let trace_started = Instant::now();
        let _lock = self.begin_metadata_write()?;
        let transactions_scanned = self.tables.namespace_transactions.len();
        let scan_started = Instant::now();
        let recovery_candidates = self
            .tables
            .namespace_transactions
            .values()
            .filter(|transaction| {
                !namespace_transaction_recovery_is_complete(&self.tables, transaction)
            })
            .cloned()
            .collect::<Vec<_>>();
        let scan_ms = scan_started.elapsed().as_millis();
        let recovery_candidate_count = recovery_candidates.len();
        let mut transactions_applied = 0usize;
        for transaction in recovery_candidates {
            if self.materialize_namespace_transaction_for_recovery(&transaction)? {
                transactions_applied = transactions_applied.saturating_add(1);
            }
        }
        let traceparent = crate::current_dfs_traceparent().unwrap_or_default();
        tracing::trace!(
            target: "sinomo.dfs.perf",
            traceparent = %traceparent,
            shard = %self.shard_id.0,
            transactions_scanned,
            recovery_candidates = recovery_candidate_count,
            transactions_applied,
            scan_ms,
            total_ms = trace_started.elapsed().as_millis(),
            "metadata-namespace-recovery"
        );
        Ok(())
    }

    fn plan_namespace_transaction_table_ops(
        &mut self,
        transaction: &DfsNamespaceTransaction,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        let trace_started = Instant::now();
        let planned_ops_before = table_ops.len();
        let metadata_objects_before = self.tables.objects.len();
        preapply_namespace_transaction_table_ops(
            &mut self.tables,
            transaction,
            table_ops,
            rollback,
        );
        metadata_backend_perf_trace(format_args!(
            "metadata-namespace-plan planner=preapplied shard={} transaction={} objects={} metadata_objects_before={} planned_ops={} plan_us={}",
            self.shard_id.0,
            transaction.transaction_id,
            transaction.objects.len(),
            metadata_objects_before,
            table_ops.len().saturating_sub(planned_ops_before),
            trace_started.elapsed().as_micros()
        ));
        Ok(())
    }

    fn materialize_namespace_transaction_for_recovery(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<bool> {
        #[cfg(test)]
        NAMESPACE_RECOVERY_PLANNER_CALLS
            .set(NAMESPACE_RECOVERY_PLANNER_CALLS.get().saturating_add(1));
        let sequence = self.next_sequence.saturating_sub(1);
        let table_ops = plan_namespace_transaction_recovery_table_ops(&self.tables, transaction);
        if table_ops.is_empty() {
            return Ok(false);
        }
        self.append_and_apply_table_ops(sequence, table_ops)?;
        Ok(true)
    }

    pub(crate) fn compact_tombstones_with_ids(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<(DfsTombstoneCompactionReport, Vec<DfsObjectId>)> {
        let _lock = self.begin_metadata_write()?;
        let now = options
            .now_unix_nanos
            .unwrap_or_else(metadata_now_unix_nanos);
        let plan = plan_tombstone_compaction(&self.tables, &options, now);
        if plan.table_ops.is_empty() {
            return Ok((plan.report, plan.compacted_object_ids));
        }
        let sequence = self.next_sequence.saturating_sub(1);
        self.append_and_apply_table_ops(sequence, plan.table_ops)?;
        self.compact_metadata_table_log_if_needed(0)?;
        Ok((plan.report, plan.compacted_object_ids))
    }

    fn plan_object_index_table_ops_with_previous(
        &mut self,
        object: &DfsObjectMetadata,
        previous: Option<&DfsObjectMetadata>,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        let object_ops = self.object_index_table_ops_with_previous(object, previous)?;
        for op in object_ops {
            self.plan_preapplied_table_op(op, table_ops, rollback);
        }
        Ok(())
    }

    fn plan_preapplied_table_op(
        &mut self,
        op: DfsMetadataTableOp,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) {
        self.tables.apply_with_rollback(op.clone(), rollback);
        table_ops.push(op);
    }

    fn plan_visible_version_commit_table_ops(
        &mut self,
        commit: &DfsVisibleVersionCommitRecord,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        for op in visible_commit_table_ops(&self.tables, commit) {
            self.plan_preapplied_table_op(op, table_ops, rollback);
        }
        Ok(())
    }

    fn object_index_table_ops_with_previous(
        &self,
        object: &DfsObjectMetadata,
        previous: Option<&DfsObjectMetadata>,
    ) -> Result<Vec<DfsMetadataTableOp>> {
        Ok(plan_object_index_table_ops_with_previous(
            &self.tables,
            object,
            previous,
        ))
    }

    fn recover_visible_version_commits(&mut self) -> Result<()> {
        // One recovery pass owns one refreshed table view. Taking and
        // refreshing the shard lock once per retained (usually already
        // materialized) commit turns every backend open into an O(commits)
        // sequence of lock/open syscalls even when there is no work to do.
        let _lock = self.begin_metadata_write()?;
        for commit in visible_commit_recovery_order(&self.tables) {
            self.recover_visible_version_commit_unlocked(&commit)?;
        }
        Ok(())
    }

    fn recover_object_locations_from_visible_versions(&mut self) -> Result<()> {
        let _lock = self.begin_metadata_write()?;
        let table_ops = visible_object_location_recovery_ops(&self.tables);
        if table_ops.is_empty() {
            return Ok(());
        }
        let sequence = self.next_sequence.saturating_sub(1);
        self.append_and_apply_table_ops(sequence, table_ops)
    }

    fn recover_visible_version_commit_unlocked(
        &mut self,
        commit: &DfsVisibleVersionCommitRecord,
    ) -> Result<()> {
        if !visible_commit_should_recover(&self.tables, commit) {
            return Ok(());
        }

        let entry =
            self.write_journal_unlocked(DfsMetadataMutation::commit_visible_version(commit))?;
        self.materialize_visible_version_commit(commit, entry.sequence)?;
        Ok(())
    }

    fn materialize_visible_version_commit(
        &mut self,
        commit: &DfsVisibleVersionCommitRecord,
        sequence: u64,
    ) -> Result<()> {
        self.append_and_apply_table_ops(sequence, visible_commit_table_ops(&self.tables, commit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalShardedMetadataBackend;
    use crate::advisory_lock::prepare_advisory_lock_state_for_migration;
    use crate::metadata_journal::{
        JOURNAL_SEGMENT_ENTRY_LIMIT, append_segmented_journal_entry,
        metadata_journal_batch_encoded_len,
    };
    use crate::metadata_keys::{
        dentry_key, key_segment, normalize_path, path_hash, path_record_for_object,
    };
    use crate::{
        DfsAdvisoryLockMode, DfsAdvisoryLockOutcome, DfsAdvisoryLockRequestId,
        DfsAdvisoryLockSessionId, DfsMetadataShardAssignment, DfsNamespaceOperation,
        DfsNamespaceResult, DfsObjectKind,
    };
    use sinomo_core::DfsObjectPolicy;
    use std::time::Duration;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("sifs").expect("valid filesystem id")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::from_legacy_named("obj_readme").expect("valid object id")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("valid version")
    }

    fn file_system() -> DfsFileSystem {
        DfsFileSystem {
            file_system_id: file_system_id(),
            display_name: "repo".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        }
    }

    fn object(version: DfsVersion) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "docs/readme.md".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:old".to_string()),
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version,
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn namespace_job(index: usize, root_path: &str) -> DfsNamespaceJobRecord {
        let timestamp = OffsetDateTime::from_unix_timestamp(index as i64).expect("valid timestamp");
        DfsNamespaceJobRecord {
            job_id: format!("job_namespace_page_{index:02}"),
            file_system_id: file_system_id(),
            kind: DfsNamespaceJobKind::ClearDirectoryRecursive,
            status: DfsNamespaceJobStatus::Running,
            root_path: root_path.to_string(),
            target_path: None,
            pending_frames: Vec::new(),
            processed_object_count: 0,
            created_at: timestamp,
            updated_at: timestamp,
            completed_at: None,
            failure_reason: None,
        }
    }

    fn namespace_request(
        index: usize,
        status: DfsNamespaceRequestStatus,
    ) -> DfsNamespaceRequestRecord {
        let timestamp = OffsetDateTime::from_unix_timestamp(index as i64).expect("valid timestamp");
        let completed = status == DfsNamespaceRequestStatus::Completed;
        DfsNamespaceRequestRecord {
            file_system_id: file_system_id(),
            requester_node_id: node(&format!("node_namespace_request_{index:02}")),
            request_id: format!("namespace-request-{index:02}"),
            operation_id: format!("namespace-operation-{index:02}"),
            authority_shard_id: DfsMetadataShardId::new("root"),
            operation: DfsNamespaceOperation::SetMode {
                path: format!("/requests/{index}"),
                mode: 0o644,
            },
            command_digest: format!("blake3:namespace-request-{index:02}"),
            status,
            result: completed.then_some(DfsNamespaceResult::Applied),
            posix_coordination_result: None,
            recovery_attempts: 0,
            last_recovery_error: None,
            created_at: timestamp,
            updated_at: timestamp,
        }
    }

    fn namespace_prepare(
        operation_id: &str,
        shard_id: &str,
        current: &DfsObjectMetadata,
        committed: DfsObjectMetadata,
    ) -> DfsNamespacePrepareRecord {
        let mut prepare = DfsNamespacePrepareRecord {
            operation_id: operation_id.to_string(),
            file_system_id: current.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(shard_id),
            owner_epoch: 7,
            operation_digest: String::new(),
            expected_object_versions: vec![DfsNamespaceObjectVersionExpectation {
                object_id: current.object_id.clone(),
                expected_version: Some(current.version.clone()),
            }],
            expected_path_occupants: vec![DfsNamespacePathOccupantExpectation {
                path: current.path.clone(),
                expected_object_id: Some(current.object_id.clone()),
            }],
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                transaction_id: format!("transaction:{operation_id}"),
                file_system_id: current.file_system_id.clone(),
                objects: vec![committed],
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
            completed_request: None,
            status: DfsNamespacePrepareStatus::Prepared,
        };
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("canonical namespace prepare digest");
        prepare
    }

    fn shard_config(
        shard_id: &str,
        assignment: DfsMetadataShardAssignment,
    ) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: file_system_id(),
            shard_id: shard_id.to_string(),
            primary_node_id: node("node_owner"),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn table_path_record(
        backend: &LocalSobjMetadataBackend,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> DfsPathRecord {
        backend
            .tables
            .paths
            .get(&(file_system_id.clone(), normalize_path(path)))
            .cloned()
            .expect("path table record")
    }

    fn table_path_exists(
        backend: &LocalSobjMetadataBackend,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> bool {
        backend
            .tables
            .paths
            .contains_key(&(file_system_id.clone(), normalize_path(path)))
    }

    fn table_dentry_exists(
        backend: &LocalSobjMetadataBackend,
        path_record: &DfsPathRecord,
    ) -> bool {
        backend
            .tables
            .dentries
            .contains_key(&dentry_key(path_record))
    }

    fn table_dirfrag(
        backend: &LocalSobjMetadataBackend,
        dir_inode_id: &DfsInodeId,
        dirfrag_id: &DfsDirFragmentId,
    ) -> DfsDirFragmentRecord {
        backend
            .tables
            .dirfrags
            .get(&(dir_inode_id.clone(), dirfrag_id.clone()))
            .cloned()
            .expect("dirfrag table record")
    }

    fn table_file_system_object_exists(
        backend: &LocalSobjMetadataBackend,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> bool {
        backend
            .tables
            .file_system_objects
            .contains_key(&(file_system_id.clone(), object_id.clone()))
    }

    fn table_hardlink_exists(
        backend: &LocalSobjMetadataBackend,
        target_object_id: &DfsObjectId,
        hardlink_object_id: &DfsObjectId,
    ) -> bool {
        backend
            .tables
            .hardlinks
            .contains_key(&(target_object_id.clone(), hardlink_object_id.clone()))
    }

    fn force_table_replay_from_journal(
        backend: &LocalSobjMetadataBackend,
        root: &Path,
        last_materialized_sequence: u64,
    ) {
        remove_file_if_exists(&metadata_table_path(root)).expect("remove metadata table log");
        remove_file_if_exists(&metadata_table_snapshot_path(root))
            .expect("remove metadata table snapshot");
        write_sobj_atomic(
            &backend.local_materialization_cursor_path(),
            &DfsMetadataReplayCursor {
                shard_id: backend.shard_id.clone(),
                last_sequence: last_materialized_sequence,
            },
        )
        .expect("rewind materialization cursor");
    }

    fn install_empty_replay_checkpoint(
        backend: &mut LocalSobjMetadataBackend,
        source_shard_id: &DfsMetadataShardId,
        journal_sequence: u64,
    ) {
        assert!(
            !backend.tables.has_materialized_state(),
            "the historical replay checkpoint helper requires an empty replica"
        );
        let mut snapshot = DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            materialized_through_sequence: Some(journal_sequence),
            tables: Some(metadata_tables_snapshot(&backend.tables)),
            ops: Vec::new(),
            semantic_state_fingerprint: None,
            prefix_proof: Some(
                backend
                    .metadata_prefix_checkpoint_for_shard(source_shard_id, journal_sequence)
                    .expect("historical replay prefix checkpoint"),
            ),
        };
        snapshot.semantic_state_fingerprint = Some(
            dfs_metadata_table_snapshot_state_fingerprint(&snapshot)
                .expect("historical replay semantic checkpoint"),
        );
        backend
            .replace_with_replicated_snapshot(source_shard_id.clone(), journal_sequence, snapshot)
            .expect("install historical replay checkpoint");
    }

    fn metadata_table_batch_count(root: &Path) -> usize {
        use std::io::Read as _;

        let path = metadata_table_path(root);
        let mut file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return 0,
            Err(source) => panic!("open metadata table log {}: {source}", path.display()),
        };
        let mut count = 0usize;
        loop {
            let mut length_bytes = [0_u8; 4];
            match file.read_exact(&mut length_bytes) {
                Ok(()) => {}
                Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(source) => panic!("read metadata table length {}: {source}", path.display()),
            }
            let length = u32::from_le_bytes(length_bytes) as usize;
            let mut bytes = vec![0_u8; length];
            file.read_exact(&mut bytes).unwrap_or_else(|source| {
                panic!("read metadata table batch {}: {source}", path.display())
            });
            let _: DfsMetadataTableBatch =
                sinomo_object_serde::from_bytes(&bytes).unwrap_or_else(|source| {
                    panic!("decode metadata table batch {}: {source}", path.display())
                });
            count = count.saturating_add(1);
        }
        count
    }

    fn block_shard_journal_writes(root: &Path, shard_id: &str) {
        let segments = root
            .join(key_segment(&shard_id))
            .join("journal")
            .join("segments");
        fs::remove_dir_all(&segments).expect("remove journal segments directory");
        fs::write(&segments, b"not a directory").expect("block journal segment writes");
    }

    #[test]
    fn local_backend_persists_file_system_object_path_and_journal() {
        let root = temp_root("local_backend_persists_file_system_object_path_and_journal");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let object = object(version("version_1"));

        let file_system_entry = backend.put_file_system(&file_system).expect("filesystem");
        let object_entry = backend.put_object(&object).expect("object");

        assert_eq!(file_system_entry.sequence, 1);
        assert_eq!(object_entry.sequence, 2);
        assert_eq!(
            file_system_entry.mutation,
            DfsMetadataMutation::PutFileSystem {
                file_system_id: file_system.file_system_id.clone(),
                file_system: Some(file_system.clone()),
            },
            "filesystem journal entries should carry replay-ready record payloads"
        );
        assert_eq!(
            object_entry.mutation,
            DfsMetadataMutation::PutObject {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                path: object.path.clone(),
                object: Some(object.clone()),
            },
            "object journal entries should carry replay-ready record payloads"
        );
        assert_eq!(
            backend
                .get_file_system(&file_system.file_system_id)
                .expect("get DfsFileSystem"),
            Some(file_system)
        );
        assert_eq!(
            backend.get_object(&object.object_id).expect("get object"),
            Some(object.clone())
        );
        assert_eq!(
            backend
                .get_object_by_path(&object.file_system_id, "docs/readme.md")
                .expect("get by path"),
            Some(object.clone())
        );
        assert_eq!(backend.journal_since(0).expect("journal").len(), 2);
        let durable_prefix = backend.metadata_prefix_proof().expect("prefix proof");
        assert_eq!(durable_prefix.journal_sequence, 2);
        assert!(
            metadata_table_path(&root).is_file(),
            "new metadata writes should materialize through the packed table log"
        );
        assert!(
            !root
                .join("records")
                .join("objects")
                .join(key_segment(&object.object_id))
                .join("object.sobj")
                .exists(),
            "new object writes should not create one file per object metadata record"
        );
        assert!(
            !root
                .join("records")
                .join("paths")
                .join(key_segment(&object.file_system_id))
                .join(format!("{}.sobj", path_hash(&object.path)))
                .exists(),
            "new object writes should not create one file per path index record"
        );

        crate::metadata_records::reset_dfs_metadata_full_snapshot_fingerprint_calls();
        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(reopened.journal_since(1).expect("journal").len(), 1);
        assert_eq!(
            reopened.metadata_prefix_proof().expect("reopened proof"),
            durable_prefix,
            "restart must recover the durable chain head without reseeding it"
        );
        assert_eq!(
            crate::metadata_records::dfs_metadata_full_snapshot_fingerprint_calls(),
            0,
            "healthy restart must load the checkpoint instead of hashing the full shard"
        );

        cleanup(&root);
    }

    #[test]
    fn ensure_file_system_does_not_journal_distinct_policy_reordering() {
        let root = temp_root("ensure_file_system_policy_reordering_noop");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let mut file_system = file_system();
        file_system.subtree_policies = vec![
            crate::DfsSubtreePolicy::legacy("/users", DfsObjectPolicy::ReplicatedEventual),
            crate::DfsSubtreePolicy::legacy("/services", DfsObjectPolicy::ReplicatedEventual),
        ];
        let first = backend
            .ensure_file_system(&file_system)
            .expect("initial filesystem")
            .expect("initial journal entry");
        assert_eq!(first.sequence, 1);
        let first_proof = backend.metadata_prefix_proof().expect("initial proof");

        let mut reordered = file_system.clone();
        reordered.subtree_policies.reverse();
        assert!(
            backend
                .ensure_file_system(&reordered)
                .expect("semantically identical filesystem")
                .is_none(),
            "discovery order alone must not author a PutFileSystem mutation"
        );
        assert_eq!(backend.journal_last_sequence(), 1);
        assert_eq!(
            backend.metadata_prefix_proof().expect("unchanged proof"),
            first_proof
        );
        assert_eq!(
            backend
                .get_file_system(&file_system.file_system_id)
                .expect("stored filesystem"),
            Some(file_system.clone()),
            "a semantic no-op must not rewrite the durable representation"
        );

        reordered.subtree_policies[0].policy = DfsObjectPolicy::ReplicatedConfirmed;
        let changed = backend
            .ensure_file_system(&reordered)
            .expect("changed filesystem")
            .expect("changed policy journal entry");
        assert_eq!(changed.sequence, 2);
        assert!(matches!(
            changed.mutation,
            DfsMetadataMutation::PutFileSystem { .. }
        ));

        cleanup(&root);
    }

    #[test]
    fn replicated_contiguous_journal_entries_converge_on_exact_prefix_proof() {
        let root = temp_root("replicated_contiguous_prefix_proof");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut source =
            LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source")).expect("source");
        let mut replica =
            LocalSobjMetadataBackend::new(shard_id.clone(), root.join("replica")).expect("replica");

        source
            .put_file_system(&file_system())
            .expect("source filesystem");
        let first = source
            .journal_batch_since_limited(0, 16, usize::MAX)
            .expect("first source batch")
            .expect("first batch");
        replica
            .apply_journal_batch(&first)
            .expect("apply first batch");
        let source_first_proof = source.metadata_prefix_proof().expect("source first proof");
        assert_eq!(
            replica.metadata_prefix_proof().expect("replica proof"),
            source_first_proof
        );

        source
            .put_object(&object(version("version_prefix_2")))
            .expect("source object");
        let second = source
            .journal_batch_since_limited(first.next_after_sequence, 16, usize::MAX)
            .expect("second source batch")
            .expect("second batch");
        replica
            .apply_journal_batch(&second)
            .expect("apply second batch");
        assert_eq!(
            replica.metadata_prefix_proof().expect("replica proof"),
            source.metadata_prefix_proof().expect("source proof")
        );

        let mut divergent =
            LocalSobjMetadataBackend::new(shard_id, root.join("divergent")).expect("divergent");
        let mut different_file_system = file_system();
        different_file_system.display_name = "divergent".to_string();
        divergent
            .put_file_system(&different_file_system)
            .expect("divergent filesystem");
        assert_ne!(
            divergent
                .metadata_prefix_proof()
                .expect("divergent proof")
                .digest,
            source_first_proof.digest,
            "different journal payloads must not share a proof"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_rejects_request_owned_prepare_without_completion_attachment() {
        let root = temp_root("local_backend_request_prepare_requires_completion");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        let file_system = file_system();
        let current = object(version("version_request_prepare_1"));
        let mut committed = current.clone();
        committed.version = version("version_request_prepare_2");
        let prepare = namespace_prepare(
            "namespace-request:mkdir:attempt-1",
            &shard_id.0,
            &current,
            committed,
        );
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&current).expect("current object");

        let error = backend
            .prepare_namespace_transaction(&prepare)
            .expect_err("request-owned prepare without completion must fail closed");

        assert!(
            error
                .to_string()
                .contains("request-owned but has no completed request attachment")
        );
        cleanup(&root);
    }

    #[test]
    fn request_owned_prepare_fuses_admission_and_survives_reopen_before_commit() {
        let root = temp_root("local_backend_request_prepare_fused_admission");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let current = object(version("version_request_prepare_fused_1"));
        let mut committed = current.clone();
        committed.version = version("version_request_prepare_fused_2");
        committed.mode = 0o600;
        let admitted_at = OffsetDateTime::UNIX_EPOCH;
        let completed_at = OffsetDateTime::from_unix_timestamp(1).expect("completion timestamp");
        let completed_request = DfsNamespaceRequestRecord {
            file_system_id: current.file_system_id.clone(),
            requester_node_id: node("node_request_prepare_fused_requester"),
            request_id: "request-prepare-fused".to_string(),
            operation_id: "req-request-prepare-fused".to_string(),
            authority_shard_id: shard_id.clone(),
            operation: DfsNamespaceOperation::SetMode {
                path: current.path.clone(),
                mode: committed.mode,
            },
            command_digest: "blake3:request-prepare-fused".to_string(),
            status: DfsNamespaceRequestStatus::Completed,
            result: Some(DfsNamespaceResult::Applied),
            posix_coordination_result: None,
            recovery_attempts: 0,
            last_recovery_error: None,
            created_at: admitted_at,
            updated_at: completed_at,
        };
        let pending_request =
            pending_namespace_request_admission_from_completion(&completed_request);
        let mut prepare = namespace_prepare(
            &format!(
                "namespace-request:{}:transaction-request-prepare-fused",
                completed_request.operation_id
            ),
            &shard_id.0,
            &current,
            committed.clone(),
        );
        prepare.completed_request = Some(completed_request.clone());
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("request-owned prepare digest");

        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend.put_object(&current).expect("current object");
        assert!(
            backend
                .get_namespace_request(
                    &completed_request.requester_node_id,
                    &completed_request.request_id,
                )
                .expect("request before prepare")
                .is_none()
        );
        let sequence_before_prepare = backend.journal_last_sequence();

        backend
            .prepare_namespace_transaction(&prepare)
            .expect("fused request admission and hidden prepare");

        assert_eq!(
            backend.journal_last_sequence(),
            sequence_before_prepare.saturating_add(1),
            "admission and prepare must share one journal mutation"
        );
        assert_eq!(
            backend
                .get_namespace_request(
                    &pending_request.requester_node_id,
                    &pending_request.request_id,
                )
                .expect("derived pending request"),
            Some(pending_request.clone())
        );
        assert_eq!(
            backend
                .get_namespace_prepare(&prepare.operation_id)
                .expect("hidden prepare"),
            Some(prepare.clone())
        );
        assert_eq!(
            backend
                .get_object(&current.object_id)
                .expect("object remains hidden"),
            Some(current.clone())
        );

        // Rewind the materialized table so reopen must derive the Pending row
        // from the prepare journal payload, matching replica/replay behavior.
        force_table_replay_from_journal(&backend, &root, 0);
        drop(backend);
        let mut reopened =
            LocalSobjMetadataBackend::new(shard_id, &root).expect("reopened backend");
        assert_eq!(
            reopened
                .get_namespace_request(
                    &pending_request.requester_node_id,
                    &pending_request.request_id,
                )
                .expect("reopened pending request"),
            Some(pending_request)
        );
        assert_eq!(
            reopened
                .get_namespace_prepare(&prepare.operation_id)
                .expect("reopened hidden prepare"),
            Some(prepare.clone())
        );
        assert_eq!(
            reopened
                .get_object(&current.object_id)
                .expect("reopened hidden object"),
            Some(current.clone())
        );

        reopened
            .commit_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect("commit request-owned prepare");

        assert_eq!(
            reopened
                .get_namespace_request(
                    &completed_request.requester_node_id,
                    &completed_request.request_id,
                )
                .expect("completed request"),
            Some(completed_request)
        );
        assert_eq!(
            reopened
                .get_object(&current.object_id)
                .expect("committed object"),
            Some(committed)
        );
        assert_eq!(
            reopened
                .get_namespace_prepare(&prepare.operation_id)
                .expect("committed prepare")
                .map(|record| record.status),
            Some(DfsNamespacePrepareStatus::Committed)
        );
        cleanup(&root);
    }

    #[test]
    fn request_owned_prepare_fused_admission_rejects_different_request_digest() {
        let root = temp_root("local_backend_request_prepare_fused_digest_conflict");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        let current = object(version("version_request_prepare_fused_digest_1"));
        let mut committed = current.clone();
        committed.version = version("version_request_prepare_fused_digest_2");
        committed.mode = 0o600;
        let mut completed_request = DfsNamespaceRequestRecord {
            file_system_id: current.file_system_id.clone(),
            requester_node_id: node("node_request_prepare_fused_digest_requester"),
            request_id: "request-prepare-fused-digest".to_string(),
            operation_id: "req-request-prepare-fused-digest".to_string(),
            authority_shard_id: shard_id.clone(),
            operation: DfsNamespaceOperation::SetMode {
                path: current.path.clone(),
                mode: committed.mode,
            },
            command_digest: "blake3:request-prepare-fused-digest".to_string(),
            status: DfsNamespaceRequestStatus::Completed,
            result: Some(DfsNamespaceResult::Applied),
            posix_coordination_result: None,
            recovery_attempts: 0,
            last_recovery_error: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::from_unix_timestamp(1).expect("completion timestamp"),
        };
        let mut prepare = namespace_prepare(
            &format!(
                "namespace-request:{}:transaction-request-prepare-fused-digest",
                completed_request.operation_id
            ),
            &shard_id.0,
            &current,
            committed.clone(),
        );
        prepare.completed_request = Some(completed_request.clone());
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("request-owned prepare digest");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend.put_object(&current).expect("current object");
        backend
            .prepare_namespace_transaction(&prepare)
            .expect("initial fused admission");

        completed_request.command_digest =
            "blake3:different-request-prepare-fused-digest".to_string();
        let mut conflicting_prepare = namespace_prepare(
            &format!(
                "namespace-request:{}:transaction-request-prepare-fused-digest-conflict",
                completed_request.operation_id
            ),
            &shard_id.0,
            &current,
            committed,
        );
        conflicting_prepare.completed_request = Some(completed_request.clone());
        conflicting_prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&conflicting_prepare)
                .expect("conflicting request-owned prepare digest");

        let error = backend
            .prepare_namespace_transaction(&conflicting_prepare)
            .expect_err("a fused admission must not replace a different request digest");

        assert!(
            error.to_string().contains("completed request conflicts"),
            "unexpected error: {error}"
        );
        assert!(
            backend
                .get_namespace_prepare(&conflicting_prepare.operation_id)
                .expect("conflicting prepare lookup")
                .is_none()
        );
        assert_eq!(
            backend
                .get_namespace_request(
                    &completed_request.requester_node_id,
                    &completed_request.request_id,
                )
                .expect("original pending request")
                .map(|request| request.command_digest),
            Some("blake3:request-prepare-fused-digest".to_string())
        );
        cleanup(&root);
    }

    #[test]
    fn request_owned_prepare_replay_accepts_matching_terminal_outcome_bookkeeping() {
        let root = temp_root("local_backend_request_prepare_terminal_replay");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        let file_system = file_system();
        let current = object(version("version_request_prepare_replay_1"));
        let mut committed = current.clone();
        committed.version = version("version_request_prepare_replay_2");
        committed.mode = 0o600;
        let pending_at = OffsetDateTime::UNIX_EPOCH;
        let pending_request = DfsNamespaceRequestRecord {
            file_system_id: current.file_system_id.clone(),
            requester_node_id: node("node_request_prepare_replay_requester"),
            request_id: "request-prepare-replay".to_string(),
            operation_id: "req-request-prepare-replay".to_string(),
            authority_shard_id: shard_id.clone(),
            operation: DfsNamespaceOperation::SetMode {
                path: current.path.clone(),
                mode: committed.mode,
            },
            command_digest: "blake3:request-prepare-replay".to_string(),
            status: DfsNamespaceRequestStatus::Pending,
            result: None,
            posix_coordination_result: None,
            recovery_attempts: 0,
            last_recovery_error: None,
            created_at: pending_at,
            updated_at: pending_at,
        };
        let mut prepared_completion = pending_request.clone();
        prepared_completion.status = DfsNamespaceRequestStatus::Completed;
        prepared_completion.result = Some(DfsNamespaceResult::Applied);
        prepared_completion.updated_at =
            OffsetDateTime::from_unix_timestamp(1).expect("completion timestamp");
        let mut prepare = namespace_prepare(
            &format!(
                "namespace-request:{}:transaction-request-prepare-replay",
                pending_request.operation_id
            ),
            &shard_id.0,
            &current,
            committed.clone(),
        );
        prepare.completed_request = Some(prepared_completion.clone());
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("request-owned prepare digest");

        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&current).expect("current object");
        backend
            .put_namespace_request_on_shard(&shard_id, &pending_request)
            .expect("pending request");
        backend
            .prepare_namespace_transaction(&prepare)
            .expect("durable hidden prepare");
        assert_eq!(
            backend
                .get_prepared_namespace_prepare_for_request(
                    &pending_request.requester_node_id,
                    &pending_request.request_id,
                )
                .expect("request-owned prepare index"),
            Some(prepare.clone())
        );

        // A retry can durably observe the same terminal outcome while updating
        // only recovery diagnostics. Replaying the older prepare must compare
        // the terminal semantic result, not reject harmless bookkeeping drift.
        let mut later_completion = prepared_completion;
        later_completion.recovery_attempts = 2;
        later_completion.updated_at =
            OffsetDateTime::from_unix_timestamp(2).expect("later completion timestamp");
        backend
            .put_namespace_request_on_shard(&shard_id, &later_completion)
            .expect("matching later completion");

        backend
            .commit_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect("replay matching completed request");
        assert!(
            backend
                .get_prepared_namespace_prepare_for_request(
                    &later_completion.requester_node_id,
                    &later_completion.request_id,
                )
                .expect("committed request prepare index")
                .is_none(),
            "committing a prepare must remove it from the pending request index"
        );

        assert_eq!(
            backend
                .get_namespace_request(
                    &later_completion.requester_node_id,
                    &later_completion.request_id,
                )
                .expect("completed request"),
            Some(later_completion),
            "replay must retain newer diagnostics for the same terminal outcome"
        );
        assert_eq!(
            backend
                .get_object(&current.object_id)
                .expect("committed object"),
            Some(committed)
        );
        cleanup(&root);
    }

    #[test]
    fn request_owned_prepare_recovery_repairs_post_prepare_rejection() {
        let root = temp_root("local_backend_request_prepare_rejected_after_prepare");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        let current = object(version("version_request_prepare_rejected_1"));
        let mut committed = current.clone();
        committed.version = version("version_request_prepare_rejected_2");
        committed.mode = 0o600;
        let admitted_at = OffsetDateTime::UNIX_EPOCH;
        let pending_request = DfsNamespaceRequestRecord {
            file_system_id: current.file_system_id.clone(),
            requester_node_id: node("node_request_prepare_rejected_requester"),
            request_id: "request-prepare-rejected".to_string(),
            operation_id: "req-request-prepare-rejected".to_string(),
            authority_shard_id: shard_id.clone(),
            operation: DfsNamespaceOperation::SetMode {
                path: current.path.clone(),
                mode: committed.mode,
            },
            command_digest: "blake3:request-prepare-rejected".to_string(),
            status: DfsNamespaceRequestStatus::Pending,
            result: None,
            posix_coordination_result: None,
            recovery_attempts: 0,
            last_recovery_error: None,
            created_at: admitted_at,
            updated_at: admitted_at,
        };
        let mut prepared_completion = pending_request.clone();
        prepared_completion.status = DfsNamespaceRequestStatus::Completed;
        prepared_completion.result = Some(DfsNamespaceResult::Applied);
        prepared_completion.updated_at =
            OffsetDateTime::from_unix_timestamp(1).expect("completion timestamp");
        let mut prepare = namespace_prepare(
            &format!(
                "namespace-request:{}:transaction-request-prepare-rejected",
                pending_request.operation_id
            ),
            &shard_id.0,
            &current,
            committed.clone(),
        );
        prepare.completed_request = Some(prepared_completion.clone());
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("request-owned prepare digest");

        backend.put_file_system(&file_system()).expect("filesystem");
        backend.put_object(&current).expect("current object");
        backend
            .put_namespace_request_on_shard(&shard_id, &pending_request)
            .expect("pending request");
        backend
            .prepare_namespace_transaction(&prepare)
            .expect("durable hidden prepare");

        // This is the old crash-window failure: an outer operation reported a
        // transport error after the hidden prepare became durable and wrote a
        // contradictory terminal rejection.
        let mut rejected_completion = pending_request;
        rejected_completion.status = DfsNamespaceRequestStatus::Completed;
        rejected_completion.result = Some(DfsNamespaceResult::Rejected {
            reason: "ambiguous downstream timeout".to_string(),
            posix_error: None,
            current_primary_node_id: None,
        });
        rejected_completion.updated_at =
            OffsetDateTime::from_unix_timestamp(2).expect("rejection timestamp");
        backend
            .put_namespace_request_on_shard(&shard_id, &rejected_completion)
            .expect("old contradictory rejection");

        backend
            .prepare_namespace_transaction(&prepare)
            .expect("recover the exact existing hidden prepare");
        backend
            .commit_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect("commit recovered prepare");

        assert_eq!(
            backend
                .get_namespace_request(
                    &prepared_completion.requester_node_id,
                    &prepared_completion.request_id,
                )
                .expect("completed request"),
            Some(prepared_completion),
            "the durable prepare attachment is the canonical terminal result"
        );
        assert_eq!(
            backend
                .get_object(&current.object_id)
                .expect("committed object"),
            Some(committed)
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_namespace_prepare_is_hidden_atomic_and_snapshot_durable() {
        let root = temp_root("local_backend_namespace_prepare_hidden_atomic_snapshot");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        let file_system = file_system();
        let current = object(version("version_prepare_1"));
        let mut committed = current.clone();
        committed.version = version("version_prepare_2");
        committed.changed_at_unix_nanos = 2;
        let prepare = namespace_prepare(
            "operation-hidden-commit",
            &shard_id.0,
            &current,
            committed.clone(),
        );
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&current).expect("current object");

        let prepare_entry = backend
            .prepare_namespace_transaction(&prepare)
            .expect("prepare namespace transaction");
        assert!(matches!(
            prepare_entry.mutation,
            DfsMetadataMutation::PutNamespacePrepare {
                status: DfsNamespacePrepareStatus::Prepared,
                ..
            }
        ));
        assert_eq!(
            backend
                .get_object(&current.object_id)
                .expect("visible object"),
            Some(current.clone()),
            "installing a hidden prepare must not apply its namespace transaction"
        );
        assert_eq!(
            backend
                .get_namespace_prepare(&prepare.operation_id)
                .expect("durable prepare"),
            Some(prepare.clone())
        );

        backend
            .compact_metadata_table_log_if_needed(0)
            .expect("force prepare snapshot");
        drop(backend);
        let mut reopened =
            LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopen prepared");
        assert_eq!(
            reopened
                .get_namespace_prepare(&prepare.operation_id)
                .expect("snapshotted prepare"),
            Some(prepare.clone()),
            "structured snapshots must preserve hidden prepares"
        );
        assert!(
            reopened
                .tables
                .namespace_prepare_footprint_indexes_match_history(),
            "structured snapshot hydration must reconstruct active prepare ownership"
        );
        assert_eq!(
            reopened
                .get_object(&current.object_id)
                .expect("reopened visible object"),
            Some(current.clone())
        );

        let fence_error = reopened
            .commit_prepared_namespace_transaction(
                &prepare.operation_id,
                "blake3:different-operation",
                prepare.owner_epoch,
            )
            .expect_err("digest reuse must be fenced");
        assert!(fence_error.to_string().contains("digest/epoch fence"));
        assert_eq!(
            reopened
                .get_object(&current.object_id)
                .expect("visible object after bad fence"),
            Some(current.clone())
        );

        reset_namespace_prepared_commit_reference_planner_calls();
        let commit_entry = reopened
            .commit_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect("commit prepared namespace transaction");
        assert_eq!(
            namespace_prepared_commit_reference_planner_calls(),
            0,
            "the normal prepared commit must preapply directly instead of using the reference clone planner"
        );
        assert!(matches!(
            commit_entry.mutation,
            DfsMetadataMutation::PutNamespacePrepare {
                status: DfsNamespacePrepareStatus::Committed,
                ..
            }
        ));
        assert_eq!(
            reopened
                .get_object(&current.object_id)
                .expect("committed object"),
            Some(committed.clone())
        );
        assert_eq!(
            reopened
                .get_namespace_prepare(&prepare.operation_id)
                .expect("committed prepare")
                .map(|record| record.status),
            Some(DfsNamespacePrepareStatus::Committed)
        );
        assert!(
            reopened
                .tables
                .namespace_prepare_footprint_indexes_match_history(),
            "Prepared -> Committed table apply must release active ownership"
        );
        assert_eq!(
            reopened
                .get_namespace_transaction(&prepare.transaction.transaction_id)
                .expect("committed transaction"),
            Some(prepare.transaction.clone()),
            "the visible transaction and committed marker must share one table batch"
        );
        reopened
            .commit_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect("repeated committed transaction uses the idempotent recovery path");
        assert_eq!(
            reopened
                .get_object(&current.object_id)
                .expect("object after repeated commit"),
            Some(committed.clone())
        );

        drop(reopened);
        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen committed");
        assert_eq!(
            reopened
                .get_object(&current.object_id)
                .expect("durable committed object"),
            Some(committed)
        );
        assert_eq!(
            reopened
                .get_namespace_prepare(&prepare.operation_id)
                .expect("durable committed prepare")
                .map(|record| record.status),
            Some(DfsNamespacePrepareStatus::Committed)
        );
        assert!(
            reopened
                .tables
                .namespace_prepare_footprint_indexes_match_history(),
            "snapshot plus table-log replay must retain terminal release"
        );
        let mut reuse_after_reopen = prepare;
        reuse_after_reopen.operation_id = "operation-reuse-after-reopen".to_string();
        reuse_after_reopen.transaction.transaction_id =
            "transaction:reuse-after-reopen".to_string();
        reset_namespace_prepare_history_scan_calls();
        validate_namespace_prepare_does_not_overlap(&reopened.tables, &reuse_after_reopen)
            .expect("terminal history must not block its released footprint after reopen");
        assert_eq!(namespace_prepare_history_scan_calls(), 0);

        cleanup(&root);
    }

    #[test]
    fn local_backend_namespace_prepare_abort_is_fenced_durable_and_releases_footprint() {
        let root = temp_root("local_backend_namespace_prepare_abort");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        let file_system = file_system();
        let current = object(version("version_prepare_abort_1"));
        let mut hidden = current.clone();
        hidden.version = version("version_prepare_abort_2");
        hidden.changed_at_unix_nanos = 2;
        let prepare = namespace_prepare("operation-abort", &shard_id.0, &current, hidden.clone());
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&current).expect("current object");
        backend
            .prepare_namespace_transaction(&prepare)
            .expect("prepare namespace transaction");

        let fence_error = backend
            .abort_prepared_namespace_transaction(
                &prepare.operation_id,
                "blake3:different-operation",
                prepare.owner_epoch,
            )
            .expect_err("digest reuse must be fenced");
        assert!(fence_error.to_string().contains("digest/epoch fence"));

        let abort_entry = backend
            .abort_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect("abort namespace transaction");
        assert!(matches!(
            abort_entry.mutation,
            DfsMetadataMutation::PutNamespacePrepare {
                status: DfsNamespacePrepareStatus::Aborted,
                ..
            }
        ));
        assert_eq!(
            backend
                .get_object(&current.object_id)
                .expect("visible object"),
            Some(current.clone()),
            "aborting a hidden prepare must not publish its transaction"
        );
        assert!(
            backend
                .tables
                .namespace_prepare_footprint_indexes_match_history()
        );

        let commit_error = backend
            .commit_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect_err("an aborted prepare cannot commit");
        assert!(commit_error.to_string().contains("is aborted"));
        backend
            .abort_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect("repeated abort is idempotent");

        let mut replacement = prepare.clone();
        replacement.operation_id = "operation-after-abort".to_string();
        replacement.transaction.transaction_id = "transaction:after-abort".to_string();
        replacement.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&replacement)
                .expect("replacement digest");
        backend
            .prepare_namespace_transaction(&replacement)
            .expect("aborted footprint is reusable");

        drop(backend);
        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen backend");
        assert_eq!(
            reopened
                .get_namespace_prepare(&prepare.operation_id)
                .expect("aborted prepare")
                .map(|record| record.status),
            Some(DfsNamespacePrepareStatus::Aborted)
        );
        assert!(
            reopened
                .tables
                .namespace_prepare_footprint_indexes_match_history()
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_rebinds_exact_prepared_namespace_with_epoch_fences() {
        let root = temp_root("local_backend_rebinds_prepared_namespace");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        let current = object(version("version_rebind_prepare_1"));
        let mut committed = current.clone();
        committed.version = version("version_rebind_prepare_2");
        committed.changed_at_unix_nanos = 2;
        let prepare =
            namespace_prepare("operation-rebind", &shard_id.0, &current, committed.clone());
        backend.put_file_system(&file_system()).expect("filesystem");
        backend.put_object(&current).expect("current object");
        backend
            .prepare_namespace_transaction(&prepare)
            .expect("initial prepare");

        let mut rebound = prepare.clone();
        rebound.owner_epoch = prepare.owner_epoch + 1;
        rebound.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&rebound)
                .expect("rebound digest");

        let stale_fence = backend
            .rebind_prepared_namespace_transaction(
                &prepare.operation_id,
                "blake3:stale",
                prepare.owner_epoch,
                &rebound,
            )
            .expect_err("stale digest fence must reject rebind");
        assert!(stale_fence.to_string().contains("fence mismatch"));
        assert_eq!(
            backend
                .get_namespace_prepare(&prepare.operation_id)
                .expect("prepare"),
            Some(prepare.clone())
        );

        let mut changed_payload = rebound.clone();
        changed_payload.transaction.objects[0].mode ^= 0o100;
        changed_payload.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&changed_payload)
                .expect("changed digest");
        let payload_error = backend
            .rebind_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
                &changed_payload,
            )
            .expect_err("rebind must preserve the exact namespace payload");
        assert!(payload_error.to_string().contains("operation payload"));

        backend
            .rebind_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
                &rebound,
            )
            .expect("rebind prepared namespace");
        backend
            .rebind_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
                &rebound,
            )
            .expect("exact lost-response rebind retry is idempotent");

        let mut concurrent = rebound.clone();
        concurrent.owner_epoch += 1;
        concurrent.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&concurrent)
                .expect("concurrent digest");
        let concurrent_error = backend
            .rebind_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
                &concurrent,
            )
            .expect_err("the consumed old fence must reject a concurrent rebind");
        assert!(concurrent_error.to_string().contains("fence mismatch"));
        assert_eq!(
            backend
                .get_object(&current.object_id)
                .expect("hidden object"),
            Some(current.clone()),
            "rebinding must not make the transaction visible"
        );
        backend
            .commit_prepared_namespace_transaction(
                &rebound.operation_id,
                &rebound.operation_digest,
                rebound.owner_epoch,
            )
            .expect("commit rebound prepare");
        assert_eq!(
            backend
                .get_object(&current.object_id)
                .expect("visible object"),
            Some(committed)
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_rebinds_exact_prepared_write_intent_with_record_fence() {
        let root = temp_root("local_backend_rebinds_prepared_write_intent");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let current = object(version("version_intent_rebind_1"));
        backend.put_file_system(&file_system()).expect("filesystem");
        backend.put_object(&current).expect("object");
        let created_at = OffsetDateTime::UNIX_EPOCH;
        let intent = DfsWriteIntentRecord {
            intent_id: "intent-rebind".to_string(),
            file_system_id: current.file_system_id.clone(),
            object_id: current.object_id.clone(),
            base_version: Some(current.version.clone()),
            target_version: version("version_intent_rebind_2"),
            coordinator_node_id: node("node_requester"),
            owner_epoch: 7,
            desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some("blake3:prepared".to_string()),
            object_metadata: Some(current.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at,
            updated_at: Some(created_at),
        };
        backend.put_intent(&intent).expect("prepared intent");
        let mut rebound = intent.clone();
        rebound.owner_epoch += 1;
        rebound.updated_at = Some(created_at + time::Duration::seconds(1));

        let mut changed_payload = rebound.clone();
        changed_payload.target_version = version("version_intent_different_target");
        let payload_error = backend
            .rebind_prepared_write_intent(Some(&current), &intent, &changed_payload)
            .expect_err("rebind must preserve write payload");
        assert!(payload_error.to_string().contains("payload or status"));

        backend
            .rebind_prepared_write_intent(Some(&current), &intent, &rebound)
            .expect("rebind write intent");
        backend
            .rebind_prepared_write_intent(Some(&current), &intent, &rebound)
            .expect("exact lost-response write rebind retry is idempotent");
        let mut concurrent = rebound.clone();
        concurrent.owner_epoch += 1;
        concurrent.updated_at = Some(created_at + time::Duration::seconds(2));
        let concurrent_error = backend
            .rebind_prepared_write_intent(Some(&current), &intent, &concurrent)
            .expect_err("consumed old record fence must reject concurrent rebind");
        assert!(
            concurrent_error
                .to_string()
                .contains("record fence mismatch")
        );
        assert_eq!(
            backend
                .get_intent(&intent.object_id, &intent.intent_id)
                .expect("intent"),
            Some(rebound)
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_namespace_prepare_rejects_stale_overlap_and_commit_races() {
        let root = temp_root("local_backend_namespace_prepare_stale_overlap_commit_race");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        let current = object(version("version_prepare_race_1"));
        let mut committed = current.clone();
        committed.version = version("version_prepare_race_2");
        committed.changed_at_unix_nanos = 2;
        let prepare = namespace_prepare("operation-race", &shard_id.0, &current, committed);
        backend.put_file_system(&file_system()).expect("filesystem");
        backend.put_object(&current).expect("current object");
        backend
            .prepare_namespace_transaction(&prepare)
            .expect("first prepare");

        let mut stale = prepare.clone();
        stale.operation_id = "operation-stale".to_string();
        stale.operation_digest = "blake3:operation-stale".to_string();
        stale.transaction.transaction_id = "transaction:operation-stale".to_string();
        stale.expected_object_versions[0].expected_version = Some(version("version_prepare_stale"));
        let stale_error = backend
            .prepare_namespace_transaction(&stale)
            .expect_err("stale object version must reject prepare");
        assert!(
            stale_error
                .to_string()
                .contains("version expectation changed")
        );

        let mut overlap = prepare.clone();
        overlap.operation_id = "operation-overlap".to_string();
        overlap.operation_digest = "blake3:operation-overlap".to_string();
        overlap.transaction.transaction_id = "transaction:operation-overlap".to_string();
        reset_namespace_prepare_history_scan_calls();
        let overlap_error = backend
            .prepare_namespace_transaction(&overlap)
            .expect_err("active footprint overlap must reject prepare");
        assert_eq!(
            namespace_prepare_history_scan_calls(),
            0,
            "ordinary prepare admission must not scan retained prepare history"
        );
        assert!(
            overlap_error
                .to_string()
                .contains("overlaps active prepare")
        );

        let mut raced = current.clone();
        raced.version = version("version_prepare_race_3");
        raced.changed_at_unix_nanos = 3;
        backend
            .put_object(&raced)
            .expect("concurrent visible mutation");
        let commit_error = backend
            .commit_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect_err("commit must revalidate the prepared expectations");
        assert!(
            commit_error
                .to_string()
                .contains("version expectation changed")
        );
        assert_eq!(
            backend.get_object(&raced.object_id).expect("raced object"),
            Some(raced)
        );
        assert_eq!(
            backend
                .get_namespace_prepare(&prepare.operation_id)
                .expect("still prepared")
                .map(|record| record.status),
            Some(DfsNamespacePrepareStatus::Prepared),
            "a failed commit must leave the hidden prepare and visible namespace unchanged"
        );

        cleanup(&root);
    }

    #[test]
    fn namespace_prepare_footprint_index_matches_reference_lifecycle_and_scope() {
        fn assert_overlap_equivalent(
            tables: &DfsMetadataTables,
            candidate: &DfsNamespacePrepareRecord,
            expected_accept: bool,
        ) {
            reset_namespace_prepare_history_scan_calls();
            let indexed = validate_namespace_prepare_does_not_overlap(tables, candidate).is_ok();
            assert_eq!(
                namespace_prepare_history_scan_calls(),
                0,
                "the indexed validator must never call the history-scan oracle"
            );
            let reference =
                validate_namespace_prepare_does_not_overlap_reference(tables, candidate).is_ok();
            assert_eq!(indexed, reference, "indexed/reference decision mismatch");
            assert_eq!(indexed, expected_accept, "unexpected overlap decision");
        }

        let shard_id = DfsMetadataShardId::new("shard-root");
        let current = object(version("version_footprint_active_1"));
        let mut committed = current.clone();
        committed.version = version("version_footprint_active_2");
        let active = namespace_prepare(
            "operation-footprint-active",
            &shard_id.0,
            &current,
            committed,
        );
        let mut tables = DfsMetadataTables::default();
        tables.apply(DfsMetadataTableOp::PutNamespacePrepare {
            prepare: active.clone(),
        });
        assert!(tables.namespace_prepare_footprint_indexes_match_history());

        assert_overlap_equivalent(&tables, &active, true);

        let mut same_object = current.clone();
        same_object.path = "/docs/object-conflict-only.md".to_string();
        let mut same_object_target = same_object.clone();
        same_object_target.version = version("version_footprint_object_conflict");
        let object_conflict = namespace_prepare(
            "operation-footprint-object-conflict",
            &shard_id.0,
            &same_object,
            same_object_target,
        );
        assert_overlap_equivalent(&tables, &object_conflict, false);

        let mut same_path = current.clone();
        same_path.object_id =
            DfsObjectId::from_legacy_named("obj_footprint_path_conflict").expect("object id");
        let mut same_path_target = same_path.clone();
        same_path_target.version = version("version_footprint_path_conflict");
        let path_conflict = namespace_prepare(
            "operation-footprint-path-conflict",
            &shard_id.0,
            &same_path,
            same_path_target,
        );
        assert_overlap_equivalent(&tables, &path_conflict, false);

        let mut disjoint = same_path.clone();
        disjoint.path = "/docs/disjoint.md".to_string();
        let mut disjoint_target = disjoint.clone();
        disjoint_target.version = version("version_footprint_disjoint");
        let disjoint_prepare = namespace_prepare(
            "operation-footprint-disjoint",
            &shard_id.0,
            &disjoint,
            disjoint_target,
        );
        assert_overlap_equivalent(&tables, &disjoint_prepare, true);

        let mut other_file_system = current.clone();
        other_file_system.file_system_id =
            DfsFileSystemId::from_legacy_named("sifs-footprint-other").expect("filesystem id");
        let mut other_file_system_target = other_file_system.clone();
        other_file_system_target.version = version("version_footprint_other_fs");
        let cross_file_system = namespace_prepare(
            "operation-footprint-other-fs",
            &shard_id.0,
            &other_file_system,
            other_file_system_target,
        );
        assert_overlap_equivalent(&tables, &cross_file_system, true);
        tables.apply(DfsMetadataTableOp::PutNamespacePrepare {
            prepare: cross_file_system,
        });
        assert!(tables.namespace_prepare_footprint_indexes_match_history());

        let mut committed_active = active.clone();
        committed_active.status = DfsNamespacePrepareStatus::Committed;
        tables.apply(DfsMetadataTableOp::PutNamespacePrepare {
            prepare: committed_active,
        });
        let mut replacement = active.clone();
        replacement.operation_id = "operation-footprint-replacement".to_string();
        replacement.transaction.transaction_id = "transaction:footprint-replacement".to_string();
        assert_overlap_equivalent(&tables, &replacement, true);
        tables.apply(DfsMetadataTableOp::PutNamespacePrepare {
            prepare: replacement.clone(),
        });

        let mut aborted_replacement = replacement.clone();
        aborted_replacement.status = DfsNamespacePrepareStatus::Aborted;
        tables.apply(DfsMetadataTableOp::PutNamespacePrepare {
            prepare: aborted_replacement,
        });
        let mut after_abort = replacement;
        after_abort.operation_id = "operation-footprint-after-abort".to_string();
        after_abort.transaction.transaction_id = "transaction:footprint-after-abort".to_string();
        assert_overlap_equivalent(&tables, &after_abort, true);
        assert!(tables.namespace_prepare_footprint_indexes_match_history());

        let before_preapply = tables.clone();
        let mut rollback = DfsMetadataTableRollback::default();
        tables.apply_with_rollback(
            DfsMetadataTableOp::PutNamespacePrepare {
                prepare: after_abort.clone(),
            },
            &mut rollback,
        );
        let mut blocked_during_preapply = after_abort;
        blocked_during_preapply.operation_id =
            "operation-footprint-preapplied-conflict".to_string();
        blocked_during_preapply.transaction.transaction_id =
            "transaction:footprint-preapplied-conflict".to_string();
        assert_overlap_equivalent(&tables, &blocked_during_preapply, false);
        rollback.rollback(&mut tables);
        assert_eq!(tables, before_preapply);
        assert!(tables.namespace_prepare_footprint_indexes_match_history());
    }

    #[test]
    fn sharded_backend_namespace_prepare_routes_one_shard_and_commits_indexes() {
        let root = temp_root("sharded_backend_namespace_prepare_route_commit");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let mut current = object(version("version_sharded_prepare_1"));
        current.path = "/services/prepared.txt".to_string();
        let mut committed = current.clone();
        committed.version = version("version_sharded_prepare_2");
        committed.changed_at_unix_nanos = 2;
        let prepare =
            namespace_prepare("operation-sharded", "services", &current, committed.clone());
        backend.put_file_system(&file_system()).expect("filesystem");
        backend.put_object(&current).expect("current object");
        backend
            .prepare_namespace_transaction(&prepare)
            .expect("sharded prepare");
        assert_eq!(
            backend
                .get_object(&current.object_id)
                .expect("hidden object"),
            Some(current)
        );
        backend
            .commit_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect("sharded commit");
        assert_eq!(
            backend
                .get_object_by_path(&committed.file_system_id, &committed.path)
                .expect("committed path"),
            Some(committed.clone())
        );
        assert_eq!(
            backend
                .read_object_shard_index(&committed.object_id)
                .expect("object shard index")
                .map(|index| index.shard_id),
            Some(DfsMetadataShardId::new("services"))
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_namespace_prepare_expectations_use_one_exact_table_snapshot() {
        let root = temp_root("sharded_namespace_prepare_expectations_one_snapshot");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut moving = object(version("version_snapshot_moving_1"));
        moving.path = "/services/from.txt".to_string();
        let mut occupant = object(version("version_snapshot_occupant_1"));
        occupant.object_id =
            DfsObjectId::from_legacy_named("obj_namespace_snapshot_occupant").expect("object id");
        occupant.path = "/services/to.txt".to_string();
        backend.put_object(&moving).expect("moving object");
        backend.put_object(&occupant).expect("target occupant");

        let mut moved = moving.clone();
        moved.path = occupant.path.clone();
        moved.version = version("version_snapshot_moving_2");
        let transaction = DfsNamespaceTransaction {
            transaction_id: "transaction:one-snapshot".to_string(),
            file_system_id: moving.file_system_id.clone(),
            objects: vec![moved],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };

        reset_metadata_current_tables_snapshot_calls();
        let (object_expectations, path_expectations, paths) = backend
            .namespace_prepare_expectations(&DfsMetadataShardId::new("services"), &transaction)
            .expect("namespace expectations");
        assert_eq!(
            metadata_current_tables_snapshot_calls(),
            1,
            "the sharded/local fast path must project every expectation from one table snapshot"
        );
        assert_eq!(
            paths,
            std::collections::BTreeSet::from([moving.path.clone(), occupant.path.clone()]),
            "the immutable snapshot must contribute the moving object's current path"
        );
        assert_eq!(object_expectations.len(), 2);
        assert_eq!(path_expectations.len(), 2);
        assert_eq!(
            object_expectations
                .iter()
                .find(|expected| expected.object_id == moving.object_id)
                .and_then(|expected| expected.expected_version.as_ref()),
            Some(&moving.version)
        );
        assert_eq!(
            object_expectations
                .iter()
                .find(|expected| expected.object_id == occupant.object_id)
                .and_then(|expected| expected.expected_version.as_ref()),
            Some(&occupant.version),
            "the target-path occupant version must come from the same snapshot"
        );
        assert_eq!(
            path_expectations
                .iter()
                .find(|expected| expected.path == moving.path)
                .and_then(|expected| expected.expected_object_id.as_ref()),
            Some(&moving.object_id)
        );
        assert_eq!(
            path_expectations
                .iter()
                .find(|expected| expected.path == occupant.path)
                .and_then(|expected| expected.expected_object_id.as_ref()),
            Some(&occupant.object_id)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_location_merge_cannot_resurrect_superseded_object() {
        let root = temp_root("local_backend_location_merge_version_fence");
        let shard_id = DfsMetadataShardId::new("users");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        let file_system = file_system();
        let mut live = object(version("version_1"));
        live.path = "/users/repair-race.txt".to_string();
        live.changed_at_unix_nanos = 1;
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&live).expect("live object");

        let mut tombstone = live.clone();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.version = version("version_2");
        tombstone.changed_at_unix_nanos = 2;
        backend.put_object(&tombstone).expect("move tombstone");

        assert_eq!(
            backend
                .add_object_location_if_current(
                    &live.object_id,
                    &live.version,
                    live.content_hash.as_deref().expect("content hash"),
                    &node("node_late_replica"),
                )
                .expect("conditional location merge"),
            None,
            "a delayed old-version repair must lose to the move tombstone"
        );
        assert_eq!(
            backend.get_object(&live.object_id).expect("current object"),
            Some(tombstone.clone())
        );

        drop(backend);
        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");
        assert_eq!(
            reopened
                .get_object(&live.object_id)
                .expect("reopened current object"),
            Some(tombstone),
            "recovery must preserve the superseding tombstone"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_batches_object_metadata_into_one_table_batch() {
        let root = temp_root("local_backend_batches_object_metadata_into_one_table_batch");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let objects = (0..3)
            .map(|index| {
                let mut object = object(version(&format!("version_batch_{index}")));
                object.object_id =
                    DfsObjectId::from_legacy_named(format!("obj_batch_{index}")).expect("object");
                object.path = format!("docs/file-{index}.md");
                object
            })
            .collect::<Vec<_>>();
        let entries = backend.put_objects(&objects).expect("object batch");

        assert_eq!(entries.len(), objects.len());
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert_eq!(
            backend.journal_since(1).expect("journal").len(),
            objects.len()
        );
        assert_eq!(
            metadata_table_batch_count(&root),
            2,
            "filesystem write should use one table batch and the object group should use one more"
        );
        for object in &objects {
            assert_eq!(
                backend.get_object(&object.object_id).expect("get object"),
                Some(object.clone())
            );
        }

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(
            reopened
                .count_objects_for_file_system(&file_system.file_system_id)
                .expect("count objects"),
            objects.len()
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_routes_object_and_version_to_selected_path_shard() {
        let root = temp_root("sharded_backend_routes_object_and_version");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.path = "/services/readme.md".to_string();
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: object.created_by_event_id.clone(),
            created_at: OffsetDateTime::now_utc(),
            visible: true,
        };

        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend.put_version(&version_record).expect("version");

        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("get object")
                .as_ref()
                .map(|object| object.path.as_str()),
            Some("/services/readme.md")
        );
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("get version"),
            Some(version_record)
        );
        let services_batch = backend
            .journal_batch_for_shard_since_limited(
                &DfsMetadataShardId::new("services"),
                0,
                16,
                usize::MAX,
            )
            .expect("services batch")
            .expect("services changes");
        assert!(
            services_batch.entries.iter().any(|entry| matches!(
                entry.mutation,
                DfsMetadataMutation::PutObject { .. } | DfsMetadataMutation::PutVersion { .. }
            )),
            "path-selected shard should own object and version journal entries"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_updates_existing_intent_without_object_shard_index() {
        let root = temp_root("sharded_backend_updates_existing_orphan_intent");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.path = "/services/orphan-intent.txt".to_string();
        let mut intent = DfsWriteIntentRecord {
            intent_id: "dfs_intent_orphan_sharded".to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: version("version_2"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 0,
            desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some("blake3:orphan-intent".to_string()),
            object_metadata: Some(object.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::now_utc(),
            updated_at: Some(OffsetDateTime::now_utc()),
        };

        backend.put_file_system(&file_system).expect("filesystem");
        backend
            .put_intent_for_object(Some(&object), &intent)
            .expect("intent");
        std::fs::remove_file(backend.object_shard_index_path(&object.object_id))
            .expect("remove shard index");

        intent.status = DfsWriteIntentStatus::Aborted;
        intent.updated_at = Some(OffsetDateTime::now_utc());
        backend
            .put_intent(&intent)
            .expect("update existing intent without object shard index");

        assert_eq!(
            backend
                .get_intent(&object.object_id, &intent.intent_id)
                .expect("intent")
                .expect("intent")
                .status,
            DfsWriteIntentStatus::Aborted
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_moves_object_between_path_shards_without_scanning_on_read() {
        let root = temp_root("sharded_backend_moves_object_between_path_shards");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
                shard_config(
                    "users",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/users".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.path = "/services/readme.md".to_string();
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("services object");

        let mut moved = object.clone();
        moved.path = "/users/readme.md".to_string();
        moved.version = version("version_2");
        backend.put_object(&moved).expect("moved object");

        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("get moved object")
                .as_ref()
                .map(|object| object.path.as_str()),
            Some("/users/readme.md")
        );
        assert_eq!(
            backend
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects")
                .into_iter()
                .map(|object| object.path)
                .collect::<Vec<_>>(),
            vec!["/users/readme.md".to_string()]
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, "/services/readme.md")
                .expect("old path")
                .map(|object| object.kind),
            None,
            "source-eviction storage evidence is not a logical namespace tombstone"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_evicts_explicit_source_without_object_index() {
        let root = temp_root("sharded_backend_explicit_source_eviction");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
                shard_config(
                    "users",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/users".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let mut source = object(version("version_1"));
        source.path = "/users/move-src.txt".to_string();
        source.changed_at_unix_nanos = 1;
        let mut moved = source.clone();
        moved.path = "/services/move-dst.txt".to_string();
        moved.version = version("version_2");
        moved.changed_at_unix_nanos = 2;
        backend.put_file_system(&file_system).expect("filesystem");
        backend
            .shard_backend_mut(&DfsMetadataShardId::new("users"))
            .expect("users shard")
            .put_object(&source)
            .expect("source object");
        backend
            .shard_backend_mut(&DfsMetadataShardId::new("services"))
            .expect("services shard")
            .put_object(&moved)
            .expect("destination object");
        crate::metadata_shard_index::remove_object_shard_index(&root, &source.object_id)
            .expect("remove derived index");

        backend
            .evict_object_record_for_foreign_shard_move(&moved, &source.path)
            .expect("source eviction");

        assert_eq!(
            backend
                .shard_backend(&DfsMetadataShardId::new("users"))
                .expect("users shard")
                .get_object(&source.object_id)
                .expect("source record")
                .map(|object| (object.kind, object.version)),
            Some((DfsObjectKind::Tombstone, moved.version.clone())),
            "eviction must derive the source shard from the old path, not a missing local index"
        );
        assert_eq!(
            backend
                .read_object_shard_index(&source.object_id)
                .expect("destination index")
                .map(|index| index.shard_id),
            Some(DfsMetadataShardId::new("services"))
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_never_promotes_source_eviction_after_path_routes_back_to_source() {
        let root = temp_root("sharded_backend_source_eviction_reroute");
        let initial_configs = vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ];
        let mut backend =
            LocalShardedMetadataBackend::new(&root, initial_configs).expect("sharded backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let mut live = object(version("version_source_eviction_reroute"));
        live.path = "/services/rerouted-live.txt".to_string();
        live.changed_at_unix_nanos = 10;
        backend
            .shard_backend_mut(&DfsMetadataShardId::new("services"))
            .expect("services shard")
            .put_object(&live)
            .expect("live target");

        let mut source_eviction = live.clone();
        source_eviction.kind = DfsObjectKind::Tombstone;
        source_eviction.changed_at_unix_nanos = 11;
        source_eviction.tombstone_provenance =
            Some(crate::DfsTombstoneProvenance::SourceEviction {
                source_shard_id: DfsMetadataShardId::new("root"),
                target_shard_id: DfsMetadataShardId::new("services"),
            });
        backend
            .shard_backend_mut(&DfsMetadataShardId::new("root"))
            .expect("root shard")
            .put_object(&source_eviction)
            .expect("source eviction marker");
        crate::metadata_shard_index::write_object_shard_index(
            &root,
            &source_eviction,
            &DfsMetadataShardId::new("root"),
        )
        .expect("stale source index");
        drop(backend);

        // A later topology generation routes /services through root again.
        // The newer physical eviction marker must remain non-authoritative;
        // the live target is the only logical namespace record until the
        // migration lane moves it to the new route.
        let rerouted_configs = vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/retired-services".to_string(),
                },
            ),
        ];
        let backend = LocalShardedMetadataBackend::new(&root, rerouted_configs)
            .expect("rerouted sharded backend");
        assert_eq!(
            backend.get_object(&live.object_id).expect("object by id"),
            Some(live.clone()),
            "a source-eviction marker is never authoritative deletion evidence"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &live.path)
                .expect("object by path"),
            Some(live.clone()),
            "a newly path-selected eviction marker must not suppress the live fallback"
        );
        assert_eq!(
            backend
                .objects_for_file_system(&file_system.file_system_id)
                .expect("merged objects"),
            vec![live]
        );

        cleanup(&root);
    }

    #[test]
    fn conditional_source_eviction_yields_to_concurrent_family_append() {
        let root = temp_root("conditional_source_eviction_family_fence");
        let source_shard_id = DfsMetadataShardId::new("root");
        let target_shard_id = DfsMetadataShardId::new("services");
        let mut source =
            LocalSobjMetadataBackend::new(source_shard_id.clone(), &root).expect("source backend");
        source.put_file_system(&file_system()).expect("filesystem");
        let expected_source = object(version("version_family_fence_source"));
        source.put_object(&expected_source).expect("source object");
        let fenced_sequence = source.journal_last_sequence();

        let mut concurrent = LocalSobjMetadataBackend::new(source_shard_id.clone(), &root)
            .expect("concurrent source writer");
        concurrent
            .put_version(&DfsObjectVersionRecord {
                file_system_id: expected_source.file_system_id.clone(),
                object_id: expected_source.object_id.clone(),
                version: version("version_family_fence_late"),
                content_hash: Some("blake3:family-fence-late".to_string()),
                size_bytes: Some(1),
                locations: vec![node("node_owner")],
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: true,
            })
            .expect("concurrent family append");
        drop(concurrent);

        let mut target_object = expected_source.clone();
        target_object.path = "/services/readme.md".to_string();
        assert_eq!(
            source
                .put_cross_shard_tombstone_if_unchanged(
                    &expected_source,
                    &target_object,
                    &source_shard_id,
                    &target_shard_id,
                    fenced_sequence,
                )
                .expect("conditional eviction"),
            DfsConditionalSourceEviction::JournalAdvanced,
            "a family append between tail observation and eviction must win the source lock fence"
        );
        assert_eq!(
            source
                .get_object(&expected_source.object_id)
                .expect("source object after fence"),
            Some(expected_source),
            "the source remains live until the late family entry is replayed"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_replay_converges_cross_shard_move_independent_of_batch_order() {
        let users_leader_root = temp_root("sharded_replay_move_users_leader");
        let services_leader_root = temp_root("sharded_replay_move_services_leader");
        let first_replica_root = temp_root("sharded_replay_move_first_replica");
        let second_replica_root = temp_root("sharded_replay_move_second_replica");
        let file_system = file_system();
        let mut source = object(version("version_1"));
        source.path = "/users/replay-move-src.txt".to_string();
        source.changed_at_unix_nanos = 1;
        let mut moved = source.clone();
        moved.path = "/services/replay-move-dst.txt".to_string();
        moved.version = version("version_2");
        moved.changed_at_unix_nanos = 2;
        let mut source_eviction = source.clone();
        source_eviction.kind = DfsObjectKind::Tombstone;
        source_eviction.version = moved.version.clone();
        source_eviction.changed_at_unix_nanos = moved.changed_at_unix_nanos;

        let mut users_leader =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("users"), &users_leader_root)
                .expect("users leader");
        users_leader
            .put_file_system(&file_system)
            .expect("users filesystem");
        users_leader.put_object(&source).expect("source object");
        users_leader
            .put_object(&source_eviction)
            .expect("source eviction");
        users_leader
            .put_object(&source)
            .expect("simulated stale location rewrite");
        let mut users_subscription = users_leader.subscribe(0);
        let users_batch = users_leader
            .poll_subscription(&mut users_subscription)
            .expect("users batch");

        let mut services_leader = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("services"),
            &services_leader_root,
        )
        .expect("services leader");
        services_leader
            .put_file_system(&file_system)
            .expect("services filesystem");
        services_leader
            .put_object(&moved)
            .expect("destination object");
        let mut services_subscription = services_leader.subscribe(0);
        let services_batch = services_leader
            .poll_subscription(&mut services_subscription)
            .expect("services batch");

        let configs = || {
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
                shard_config(
                    "users",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/users".to_string(),
                    },
                ),
            ]
        };
        for (replica_root, services_first) in [
            (first_replica_root.path(), true),
            (second_replica_root.path(), false),
        ] {
            let mut replica =
                LocalShardedMetadataBackend::new(replica_root, configs()).expect("replica backend");
            // Each source stream begins at its true genesis, including the
            // filesystem record. Do not invent a high starting sequence or a
            // locally authored sequence-1 entry: either would be an invalid
            // exact-prefix fixture and must be rejected as a gap/fork.
            let ordered_batches = if services_first {
                [&services_batch, &users_batch]
            } else {
                [&users_batch, &services_batch]
            };
            for batch in ordered_batches {
                replica.apply_journal_batch(batch).expect("replay batch");
            }
            for batch in ordered_batches {
                assert_eq!(
                    replica
                        .apply_journal_batch(batch)
                        .expect("idempotent replay"),
                    0
                );
            }
            assert_eq!(
                replica
                    .read_object_shard_index(&source.object_id)
                    .expect("reconciled index")
                    .map(|index| index.shard_id),
                Some(DfsMetadataShardId::new("services"))
            );
            assert_eq!(
                replica
                    .get_object(&source.object_id)
                    .expect("current object"),
                Some(moved.clone())
            );
            assert_eq!(
                replica
                    .get_object_by_path(&file_system.file_system_id, &source.path)
                    .expect("source path"),
                None
            );
            assert_eq!(
                replica
                    .get_object_by_path(&file_system.file_system_id, &moved.path)
                    .expect("destination path"),
                Some(moved.clone())
            );

            drop(replica);
            let reopened = LocalShardedMetadataBackend::new(replica_root, configs())
                .expect("reopened replica");
            assert_eq!(
                reopened
                    .get_object(&source.object_id)
                    .expect("reopened object"),
                Some(moved.clone())
            );
            assert_eq!(
                reopened
                    .get_object_by_path(&file_system.file_system_id, &source.path)
                    .expect("reopened source path"),
                None
            );
        }

        cleanup(&users_leader_root);
        cleanup(&services_leader_root);
        cleanup(&first_replica_root);
        cleanup(&second_replica_root);
    }

    #[test]
    fn sharded_backend_pages_objects_and_directory_children_with_shard_cursors() {
        let root = temp_root("sharded_backend_pages_with_shard_cursors");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let mut root_file = object(version("version_1"));
        root_file.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_page_root_file").expect("root file");
        root_file.path = "/root.txt".to_string();

        let mut users_dir = object(version("version_1"));
        users_dir.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_page_users").expect("users dir");
        users_dir.path = "/users".to_string();
        users_dir.kind = DfsObjectKind::Directory;
        users_dir.content_hash = None;
        users_dir.content_size_bytes = None;

        let mut services_dir = object(version("version_1"));
        services_dir.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_page_services").expect("services dir");
        services_dir.path = "/services".to_string();
        services_dir.kind = DfsObjectKind::Directory;
        services_dir.content_hash = None;
        services_dir.content_size_bytes = None;

        let mut service_file = object(version("version_1"));
        service_file.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_page_service_file").expect("service file");
        service_file.path = "/services/app.txt".to_string();

        for object in [&root_file, &users_dir, &services_dir, &service_file] {
            backend.put_object(object).expect("object");
        }

        let mut object_cursor = None;
        let mut paged_object_ids = BTreeSet::new();
        let mut saw_object_shard_cursor = false;
        loop {
            let page = backend
                .objects_for_file_system_page(
                    &file_system.file_system_id,
                    object_cursor.as_ref(),
                    2,
                )
                .expect("object page");
            paged_object_ids.extend(page.objects.into_iter().map(|object| object.object_id));
            if page
                .next_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.shard_id.is_some())
            {
                saw_object_shard_cursor = true;
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            object_cursor = Some(next_cursor);
        }
        let full_object_ids = backend
            .objects_for_file_system(&file_system.file_system_id)
            .expect("full objects")
            .into_iter()
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(paged_object_ids, full_object_ids);
        assert!(
            saw_object_shard_cursor,
            "sharded object pages should carry shard cursors for bounded continuation"
        );

        let mut dir_cursor = None;
        let mut root_child_names = Vec::new();
        let mut saw_directory_shard_cursor = false;
        loop {
            let page = backend
                .child_objects_for_directory_page(
                    &file_system.file_system_id,
                    "/",
                    dir_cursor.as_ref(),
                    1,
                )
                .expect("directory page");
            root_child_names.extend(page.objects.into_iter().map(|object| {
                object
                    .path
                    .rsplit('/')
                    .next()
                    .expect("child name")
                    .to_string()
            }));
            if page
                .next_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.shard_id.is_some())
            {
                saw_directory_shard_cursor = true;
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            dir_cursor = Some(next_cursor);
        }
        root_child_names.sort();
        assert_eq!(root_child_names, vec!["root.txt", "services", "users"]);
        assert!(
            saw_directory_shard_cursor,
            "sharded directory pages should carry shard cursors for bounded continuation"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_pages_namespace_jobs() {
        let root = temp_root("sharded_backend_pages_namespace_jobs");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");

        for index in 0..6 {
            let root_path = if index % 2 == 0 {
                format!("/jobs/{index}")
            } else {
                format!("/services/jobs/{index}")
            };
            backend
                .put_namespace_job(&namespace_job(index, &root_path))
                .expect("namespace job");
        }

        let mut cursor = None;
        let mut job_ids = Vec::new();
        let mut pages = 0usize;
        loop {
            let page = backend
                .list_namespace_jobs_page(cursor.as_ref(), 2)
                .expect("job page");
            assert!(page.jobs.len() <= 2);
            job_ids.extend(page.jobs.iter().map(|job| job.job_id.clone()));
            pages = pages.saturating_add(1);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        assert_eq!(
            job_ids,
            (0..6)
                .map(|index| format!("job_namespace_page_{index:02}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(pages, 3);
        assert_eq!(
            backend
                .get_namespace_job("job_namespace_page_05")
                .expect("get job")
                .expect("job")
                .root_path,
            "/services/jobs/5"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_merges_pending_namespace_request_pages() {
        let root = temp_root("sharded_backend_pages_pending_namespace_requests");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");

        for index in 0..6 {
            let status = if index == 3 {
                DfsNamespaceRequestStatus::Completed
            } else {
                DfsNamespaceRequestStatus::Pending
            };
            let mut request = namespace_request(index, status);
            let shard_id = if index % 2 == 0 {
                DfsMetadataShardId::new("root")
            } else {
                DfsMetadataShardId::new("services")
            };
            request.authority_shard_id = shard_id.clone();
            backend
                .put_namespace_request_on_shard(&shard_id, &request)
                .expect("namespace request");
        }

        let mut cursor = None;
        let mut request_ids = Vec::new();
        loop {
            let page = backend
                .list_pending_namespace_requests_page(cursor.as_ref(), 2)
                .expect("pending request page");
            request_ids.extend(
                page.requests
                    .iter()
                    .map(|request| request.request_id.clone()),
            );
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        assert_eq!(
            request_ids,
            [0, 1, 2, 4, 5]
                .into_iter()
                .map(|index| format!("namespace-request-{index:02}"))
                .collect::<Vec<_>>()
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_gets_namespace_transaction_by_shard_transaction_id() {
        let root = temp_root("sharded_backend_get_namespace_transaction");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        let mut object = object(version("version_1"));
        object.path = "/services/namespace-transaction.txt".to_string();

        let entry = backend
            .apply_namespace_transaction(&DfsNamespaceTransaction {
                transaction_id: "move-to-services".to_string(),
                file_system_id: file_system.file_system_id.clone(),
                objects: vec![object.clone()],
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("namespace transaction");
        let transaction_id = match &entry.mutation {
            DfsMetadataMutation::ApplyNamespaceTransaction { transaction_id, .. } => {
                transaction_id.clone()
            }
            other => panic!("expected namespace transaction journal entry, got {other:?}"),
        };

        let recovered = backend
            .get_namespace_transaction(&transaction_id)
            .expect("lookup transaction")
            .expect("transaction");
        assert_eq!(recovered.transaction_id, transaction_id);
        assert_eq!(
            recovered.objects.first().map(|object| &object.object_id),
            Some(&object.object_id)
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_pages_object_local_records_with_shard_cursors() {
        let root = temp_root("sharded_backend_pages_object_local_records");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let mut file = object(version("version_0"));
        file.path = "/services/readme.md".to_string();
        backend.put_object(&file).expect("object");

        for index in 0..3 {
            let version_record = DfsObjectVersionRecord {
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                version: version(&format!("version_{index}")),
                content_hash: Some(format!("blake3:version-{index}")),
                size_bytes: Some(index),
                locations: vec![node("node_owner")],
                created_by_event_id: None,
                created_at: OffsetDateTime::now_utc(),
                visible: index == 0,
            };
            backend.put_version(&version_record).expect("version");
        }

        let first_version_page = backend
            .versions_for_object_page(&file.object_id, None, 1)
            .expect("first version page");
        assert_eq!(first_version_page.versions.len(), 1);
        assert_eq!(
            first_version_page
                .next_cursor
                .as_ref()
                .and_then(|cursor| cursor.shard_id.as_ref())
                .map(|shard_id| shard_id.0.as_str()),
            Some("services")
        );
        let first_version_scan = backend
            .version_records_page(None, 2)
            .expect("first version scan page");
        assert_eq!(first_version_scan.versions.len(), 2);
        assert!(first_version_scan.next_cursor.is_some());
        let second_version_scan = backend
            .version_records_page(first_version_scan.next_cursor.as_ref(), 2)
            .expect("second version scan page");
        assert_eq!(second_version_scan.versions.len(), 1);
        assert!(second_version_scan.next_cursor.is_none());
        let scanned_versions = first_version_scan
            .versions
            .into_iter()
            .chain(second_version_scan.versions)
            .map(|version_record| version_record.version)
            .collect::<Vec<_>>();
        let mut expected_versions = (0..3)
            .map(|index| version(&format!("version_{index}")))
            .collect::<Vec<_>>();
        expected_versions.sort();
        assert_eq!(scanned_versions, expected_versions);

        for index in 0..3 {
            let receipt = DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file.file_system_id.clone(),
                    object_id: file.object_id.clone(),
                    committed_version: version("version_0"),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::generate(),
                },
                replica_node_id: node(&format!("node_replica_{index}")),
                content_hash: Some("blake3:version-0".to_string()),
                received_at: OffsetDateTime::now_utc(),
            };
            backend.put_receipt(&receipt).expect("receipt");
        }
        let first_receipt_page = backend
            .receipts_for_version_page(&file.object_id, &version("version_0"), None, 1)
            .expect("first receipt page");
        assert_eq!(first_receipt_page.receipts.len(), 1);
        assert_eq!(
            first_receipt_page
                .next_cursor
                .as_ref()
                .and_then(|cursor| cursor.shard_id.as_ref())
                .map(|shard_id| shard_id.0.as_str()),
            Some("services")
        );
        let first_receipt_scan = backend
            .receipt_records_page(None, 2)
            .expect("first receipt scan page");
        assert_eq!(first_receipt_scan.receipts.len(), 2);
        assert!(first_receipt_scan.next_cursor.is_some());
        let second_receipt_scan = backend
            .receipt_records_page(first_receipt_scan.next_cursor.as_ref(), 2)
            .expect("second receipt scan page");
        assert_eq!(second_receipt_scan.receipts.len(), 1);
        assert!(second_receipt_scan.next_cursor.is_none());
        let scanned_receipts = first_receipt_scan
            .receipts
            .into_iter()
            .chain(second_receipt_scan.receipts)
            .map(|receipt| receipt.replica_node_id)
            .collect::<Vec<_>>();
        let mut expected_receipts = (0..3)
            .map(|index| node(&format!("node_replica_{index}")))
            .collect::<Vec<_>>();
        expected_receipts.sort();
        assert_eq!(scanned_receipts, expected_receipts);

        for index in 0..3 {
            let intent = DfsWriteIntentRecord {
                intent_id: format!("intent-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                base_version: Some(version("version_0")),
                target_version: version(&format!("version_{}", index + 1)),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner")],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(format!("blake3:intent-{index}")),
                object_metadata: Some(file.clone()),
                touch_parent_on_create: false,
                create_predecessor_object_id: None,
                created_at: OffsetDateTime::now_utc(),
                updated_at: None,
            };
            backend.put_intent(&intent).expect("intent");
        }
        let first_intent_page = backend
            .intents_for_object_page(&file.object_id, None, 1)
            .expect("first intent page");
        assert_eq!(first_intent_page.intents.len(), 1);
        assert_eq!(
            first_intent_page
                .next_cursor
                .as_ref()
                .and_then(|cursor| cursor.shard_id.as_ref())
                .map(|shard_id| shard_id.0.as_str()),
            Some("services")
        );
        let first_scan_page = backend
            .write_intents_page(None, 2)
            .expect("first write intent scan page");
        assert_eq!(first_scan_page.intents.len(), 2);
        assert!(first_scan_page.next_cursor.is_some());
        let second_scan_page = backend
            .write_intents_page(first_scan_page.next_cursor.as_ref(), 2)
            .expect("second write intent scan page");
        assert_eq!(second_scan_page.intents.len(), 1);
        assert!(second_scan_page.next_cursor.is_none());
        let scanned_intent_ids = first_scan_page
            .intents
            .into_iter()
            .chain(second_scan_page.intents)
            .map(|intent| intent.intent_id)
            .collect::<Vec<_>>();
        assert_eq!(scanned_intent_ids, vec!["intent-0", "intent-1", "intent-2"]);

        let target_version = version("version_shared_target");
        for index in 0..3 {
            let intent = DfsWriteIntentRecord {
                intent_id: format!("intent-shared-target-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                base_version: Some(version("version_0")),
                target_version: target_version.clone(),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner")],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(format!("blake3:shared-target-{index}")),
                object_metadata: Some(file.clone()),
                touch_parent_on_create: false,
                create_predecessor_object_id: None,
                created_at: OffsetDateTime::now_utc(),
                updated_at: None,
            };
            backend.put_intent(&intent).expect("shared target intent");
        }
        let first_target_page = backend
            .intents_for_object_version_page(&file.object_id, &target_version, None, 2)
            .expect("first target intent page");
        assert_eq!(first_target_page.intents.len(), 2);
        assert_eq!(
            first_target_page
                .next_cursor
                .as_ref()
                .and_then(|cursor| cursor.shard_id.as_ref())
                .map(|shard_id| shard_id.0.as_str()),
            Some("services")
        );
        let second_target_page = backend
            .intents_for_object_version_page(
                &file.object_id,
                &target_version,
                first_target_page.next_cursor.as_ref(),
                2,
            )
            .expect("second target intent page");
        assert_eq!(second_target_page.intents.len(), 1);
        assert!(second_target_page.next_cursor.is_none());
        let shared_target_ids = first_target_page
            .intents
            .into_iter()
            .chain(second_target_page.intents)
            .map(|intent| intent.intent_id)
            .collect::<Vec<_>>();
        assert_eq!(
            shared_target_ids,
            vec![
                "intent-shared-target-0".to_string(),
                "intent-shared-target-1".to_string(),
                "intent-shared-target-2".to_string()
            ]
        );

        let mut transient_object = file.clone();
        transient_object.object_id = DfsObjectId::from_legacy_named("obj_sharded_transient_intent")
            .expect("transient object id");
        transient_object.path = "/services/transient-create.txt".to_string();
        transient_object.version = version("version_transient_1");
        let transient_intent = DfsWriteIntentRecord {
            intent_id: "intent-transient-create".to_string(),
            file_system_id: transient_object.file_system_id.clone(),
            object_id: transient_object.object_id.clone(),
            base_version: None,
            target_version: version("version_transient_2"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Staging,
            content_hash: Some("blake3:transient".to_string()),
            object_metadata: Some(transient_object.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::now_utc(),
            updated_at: None,
        };
        backend
            .put_intent_for_object(Some(&transient_object), &transient_intent)
            .expect("transient intent");
        assert_eq!(
            backend
                .intents_for_object_version_page(
                    &transient_object.object_id,
                    &transient_intent.target_version,
                    None,
                    1,
                )
                .expect("transient target intent page")
                .intents
                .into_iter()
                .map(|intent| intent.intent_id)
                .collect::<Vec<_>>(),
            vec!["intent-transient-create"]
        );

        let mut moved_object = object(version("version_1"));
        moved_object.object_id = DfsObjectId::from_legacy_named("obj_sharded_stale_intent_source")
            .expect("stale intent object id");
        moved_object.path = "/stale-intent-source.txt".to_string();
        backend.put_object(&moved_object).expect("root object");
        let stale_source_intent = DfsWriteIntentRecord {
            intent_id: "intent-stale-source".to_string(),
            file_system_id: moved_object.file_system_id.clone(),
            object_id: moved_object.object_id.clone(),
            base_version: Some(moved_object.version.clone()),
            target_version: version("version_stale_source_2"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some("blake3:stale-source".to_string()),
            object_metadata: Some(moved_object.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::now_utc(),
            updated_at: None,
        };
        backend
            .put_intent(&stale_source_intent)
            .expect("source shard intent");
        let mut moved_target_object = moved_object.clone();
        moved_target_object.path = "/services/stale-intent-source.txt".to_string();
        moved_target_object.version = version("version_stale_source_3");
        backend
            .put_object(&moved_target_object)
            .expect("move object to services");
        assert!(
            backend
                .intents_for_object_version_page(
                    &moved_object.object_id,
                    &stale_source_intent.target_version,
                    None,
                    1,
                )
                .expect("stale source target intent page")
                .intents
                .is_empty(),
            "target-version lookups should hide source-shard intents after the object index moves"
        );

        let mut scan_cursor = None;
        let mut visible_scan_ids = BTreeSet::new();
        loop {
            let page = backend
                .write_intents_page(scan_cursor.as_ref(), 2)
                .expect("visible intent scan page");
            assert!(page.intents.len() <= 2);
            visible_scan_ids.extend(page.intents.into_iter().map(|intent| intent.intent_id));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            scan_cursor = Some(next_cursor);
        }
        assert!(
            visible_scan_ids.contains("intent-transient-create"),
            "transient create intents should remain visible in global scans"
        );
        assert!(
            !visible_scan_ids.contains("intent-stale-source"),
            "global scans should hide source-shard intents after the object index moves"
        );
        assert_eq!(
            backend
                .all_intents()
                .expect("all intents")
                .into_iter()
                .map(|intent| intent.intent_id)
                .collect::<BTreeSet<_>>(),
            visible_scan_ids
        );

        for index in 0..3 {
            let conflict = DfsConflictRecord {
                conflict_id: format!("conflict-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                path: file.path.clone(),
                base_version: Some(version("version_0")),
                local_version: version(&format!("version_local_{index}")),
                remote_version: version(&format!("version_remote_{index}")),
                detected_by_node_id: node("node_owner"),
                reason: format!("conflict {index}"),
                local_content_hash: Some(format!("blake3:local-{index}")),
                remote_content_hash: Some(format!("blake3:remote-{index}")),
                created_by_intent_id: None,
                strategy: Some("branch".to_string()),
                status: DfsConflictStatus::Open,
                detected_at: OffsetDateTime::UNIX_EPOCH,
            };
            backend.put_conflict(&conflict).expect("conflict");
        }
        let first_conflict_scan = backend
            .conflict_records_page(None, 2)
            .expect("first conflict scan page");
        assert_eq!(first_conflict_scan.conflicts.len(), 2);
        assert!(first_conflict_scan.next_cursor.is_some());
        let second_conflict_scan = backend
            .conflict_records_page(first_conflict_scan.next_cursor.as_ref(), 2)
            .expect("second conflict scan page");
        assert_eq!(second_conflict_scan.conflicts.len(), 1);
        assert!(second_conflict_scan.next_cursor.is_none());
        let scanned_conflicts = first_conflict_scan
            .conflicts
            .into_iter()
            .chain(second_conflict_scan.conflicts)
            .map(|conflict| conflict.conflict_id)
            .collect::<Vec<_>>();
        assert_eq!(
            scanned_conflicts,
            vec!["conflict-0", "conflict-1", "conflict-2"]
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_global_receipt_scan_keeps_indexless_pending_create_receipts() {
        let root = temp_root("sharded_global_receipts_pending_create");
        let services_shard_id = DfsMetadataShardId::new("services");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    &services_shard_id.0,
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let mut pending_object = object(version("version_pending_base"));
        pending_object.object_id =
            DfsObjectId::from_legacy_named("obj_pending_create_receipt_scan").expect("object");
        pending_object.path = "/services/pending-create.txt".to_string();
        let target_version = version("version_pending_target");
        let intent = DfsWriteIntentRecord {
            intent_id: "intent-pending-create-receipt-scan".to_string(),
            file_system_id: pending_object.file_system_id.clone(),
            object_id: pending_object.object_id.clone(),
            base_version: None,
            target_version: target_version.clone(),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some("blake3:pending-create".to_string()),
            object_metadata: Some(pending_object.clone()),
            touch_parent_on_create: true,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: None,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: pending_object.file_system_id.clone(),
                object_id: pending_object.object_id.clone(),
                committed_version: target_version,
                owner_node_id: node("node_owner"),
                event_id: EventId::generate(),
            },
            replica_node_id: node("node_owner"),
            content_hash: intent.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend
            .put_intent_and_receipts_for_object(
                Some(&pending_object),
                &intent,
                std::slice::from_ref(&receipt),
            )
            .expect("pending create intent and receipt");
        assert!(
            backend
                .get_object(&pending_object.object_id)
                .expect("object lookup")
                .is_none(),
            "pending create must not be visible before receipt recovery commits it"
        );

        let index_path = backend.object_shard_index_path(&pending_object.object_id);
        assert!(index_path.exists(), "writer should leave a routing index");
        std::fs::remove_file(index_path).expect("model an index-less metadata replica");

        let mut misrouted_object = pending_object.clone();
        misrouted_object.object_id =
            DfsObjectId::from_legacy_named("obj_misrouted_pending_receipt").expect("object");
        misrouted_object.path = "/misrouted-pending-create.txt".to_string();
        let misrouted_version = version("version_misrouted_pending");
        let misrouted_intent = DfsWriteIntentRecord {
            intent_id: "intent-misrouted-pending-receipt".to_string(),
            file_system_id: misrouted_object.file_system_id.clone(),
            object_id: misrouted_object.object_id.clone(),
            base_version: None,
            target_version: misrouted_version.clone(),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some("blake3:misrouted-pending".to_string()),
            object_metadata: Some(misrouted_object.clone()),
            touch_parent_on_create: true,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: None,
        };
        let misrouted_receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: misrouted_object.file_system_id.clone(),
                object_id: misrouted_object.object_id.clone(),
                committed_version: misrouted_version,
                owner_node_id: node("node_owner"),
                event_id: EventId::generate(),
            },
            replica_node_id: node("node_owner"),
            content_hash: misrouted_intent.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        {
            let services = backend
                .shard_backend_mut(&services_shard_id)
                .expect("services shard");
            services
                .put_intent(&misrouted_intent)
                .expect("misrouted intent fixture");
            services
                .put_receipt(&misrouted_receipt)
                .expect("misrouted receipt fixture");
        }

        let page = backend
            .receipt_records_page(None, 8)
            .expect("global receipt scan");
        assert_eq!(page.receipts, vec![receipt]);
        assert!(page.next_cursor.is_none());

        cleanup(&root);
    }

    #[test]
    fn sharded_global_scans_ignore_stale_source_family_without_index() {
        let root = temp_root("sharded_global_scan_stale_source_family");
        let source_shard_id = DfsMetadataShardId::new("a-root");
        let target_shard_id = DfsMetadataShardId::new("z-services");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config(&source_shard_id.0, DfsMetadataShardAssignment::Root),
                shard_config(
                    &target_shard_id.0,
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let mut moved_object = object(version("version_moved"));
        moved_object.object_id =
            DfsObjectId::from_legacy_named("obj_global_scan_moved").expect("object");
        moved_object.path = "/services/moved.txt".to_string();
        moved_object.content_hash = Some("blake3:moved".to_string());
        let version_record = DfsObjectVersionRecord {
            file_system_id: moved_object.file_system_id.clone(),
            object_id: moved_object.object_id.clone(),
            version: moved_object.version.clone(),
            content_hash: moved_object.content_hash.clone(),
            size_bytes: Some(5),
            locations: vec![node("node_owner")],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: moved_object.file_system_id.clone(),
                object_id: moved_object.object_id.clone(),
                committed_version: moved_object.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::generate(),
            },
            replica_node_id: node("node_owner"),
            content_hash: moved_object.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        let conflict = DfsConflictRecord {
            conflict_id: "conflict-moved".to_string(),
            file_system_id: moved_object.file_system_id.clone(),
            object_id: moved_object.object_id.clone(),
            path: moved_object.path.clone(),
            base_version: None,
            local_version: version("version_local"),
            remote_version: moved_object.version.clone(),
            detected_by_node_id: node("node_owner"),
            reason: "stale source conflict".to_string(),
            local_content_hash: Some("blake3:local".to_string()),
            remote_content_hash: moved_object.content_hash.clone(),
            created_by_intent_id: None,
            strategy: Some("branch".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::UNIX_EPOCH,
        };
        {
            let source = backend
                .shard_backend_mut(&source_shard_id)
                .expect("source shard");
            source.put_object(&moved_object).expect("source object");
            source.put_version(&version_record).expect("source version");
            source.put_receipt(&receipt).expect("source receipt");
            source.put_conflict(&conflict).expect("source conflict");
        }

        let migration = backend
            .migrate_objects_to_configured_shards(&file_system.file_system_id, 1)
            .expect("migrate object family");
        assert_eq!(migration.migrated_objects, 1);
        assert_eq!(
            backend
                .shard_backend(&source_shard_id)
                .expect("source shard")
                .get_object(&moved_object.object_id)
                .expect("source object")
                .expect("source tombstone")
                .kind,
            DfsObjectKind::Tombstone
        );
        assert_eq!(
            backend
                .shard_backend(&target_shard_id)
                .expect("target shard")
                .get_object(&moved_object.object_id)
                .expect("target object"),
            Some(moved_object.clone())
        );

        // Model the recovery edge where destination installation succeeded but
        // the source eviction tombstone did not. Both physical shards now have
        // a live record, but only the destination is routed for this path.
        backend
            .shard_backend_mut(&source_shard_id)
            .expect("source shard")
            .put_object(&moved_object)
            .expect("restore stale live source record");
        assert_ne!(
            backend
                .shard_backend(&source_shard_id)
                .expect("source shard")
                .get_object(&moved_object.object_id)
                .expect("source object")
                .expect("stale source object")
                .kind,
            DfsObjectKind::Tombstone
        );

        std::fs::remove_file(backend.object_shard_index_path(&moved_object.object_id))
            .expect("model an index-less metadata replica");
        backend
            .shard_backend_mut(&target_shard_id)
            .expect("target shard")
            .remove_receipt(
                &moved_object.object_id,
                &moved_object.version,
                &receipt.replica_node_id,
            )
            .expect("remove target receipt")
            .expect("target receipt existed");
        let mut resolved_conflict = conflict.clone();
        resolved_conflict.status = DfsConflictStatus::Resolved;
        resolved_conflict.reason = "resolved on live destination".to_string();
        backend
            .shard_backend_mut(&target_shard_id)
            .expect("target shard")
            .put_conflict(&resolved_conflict)
            .expect("update target conflict");

        let receipt_page = backend
            .receipt_records_page(None, 8)
            .expect("global receipt scan");
        assert!(
            receipt_page.receipts.is_empty(),
            "removed destination receipt must not be resurrected from the stale source shard"
        );
        let conflict_page = backend
            .conflict_records_page(None, 8)
            .expect("global conflict scan");
        assert_eq!(conflict_page.conflicts, vec![resolved_conflict]);

        cleanup(&root);
    }

    #[test]
    fn sharded_path_and_hydration_reads_ignore_stale_or_missing_replica_index() {
        let root = temp_root("sharded_path_stale_replica_index");
        let file_system = file_system();
        let mut services = object(version("version_services_anchor"));
        services.object_id =
            DfsObjectId::from_legacy_named("obj_services_anchor").expect("object id");
        services.path = "/services".to_string();
        services.kind = DfsObjectKind::Directory;
        services.content_hash = None;
        services.content_size_bytes = None;

        {
            let mut root_only = LocalShardedMetadataBackend::new(
                &root,
                vec![shard_config("root", DfsMetadataShardAssignment::Root)],
            )
            .expect("root backend");
            root_only.put_file_system(&file_system).expect("filesystem");
            root_only.put_object(&services).expect("root anchor");
        }

        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        backend
            .migrate_objects_to_configured_shards(&file_system.file_system_id, 8)
            .expect("migrate anchor");
        let root_shard_id = DfsMetadataShardId::new("root");
        let services_shard_id = DfsMetadataShardId::new("services");
        assert_eq!(
            backend
                .shard_backend(&root_shard_id)
                .expect("root shard")
                .get_object(&services.object_id)
                .expect("root row")
                .expect("source tombstone")
                .kind,
            DfsObjectKind::Tombstone
        );
        assert_eq!(
            backend
                .shard_backend(&services_shard_id)
                .expect("services shard")
                .get_object(&services.object_id)
                .expect("services row"),
            Some(services.clone())
        );

        for replica_index in [Some(root_shard_id.clone()), None] {
            match replica_index {
                Some(ref shard_id) => backend
                    .write_object_shard_index(&services, shard_id)
                    .expect("stale replica index"),
                None => std::fs::remove_file(backend.object_shard_index_path(&services.object_id))
                    .expect("remove replica index"),
            }
            assert_eq!(
                backend
                    .get_object_by_path(&file_system.file_system_id, &services.path)
                    .expect("path lookup"),
                Some(services.clone()),
                "path-selected live row must not depend on a derived replica index"
            );
            assert_eq!(
                backend.get_object(&services.object_id).expect("id lookup"),
                Some(services.clone()),
                "by-id lookup must not select the retired source tombstone"
            );
            assert_eq!(
                backend
                    .objects_for_file_system(&file_system.file_system_id)
                    .expect("hydration scan"),
                vec![services.clone()],
                "merged hydration must expose only the routed live row"
            );
        }

        backend
            .write_object_shard_index(&services, &root_shard_id)
            .expect("stale index before refresh");
        backend
            .refresh_metadata_shard_durable_view(&services_shard_id)
            .expect("refresh services view");
        assert_eq!(
            backend
                .read_object_shard_index(&services.object_id)
                .expect("repaired index")
                .map(|index| index.shard_id),
            Some(services_shard_id),
            "durable-view refresh must repair derived placement after visibility is established"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_pages_and_counts_hardlinks_across_shards() {
        let root = temp_root("sharded_backend_pages_hardlinks");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let target = object(version("version_1"));
        let mut root_alias = object(version("version_1"));
        root_alias.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_root_alias").expect("root alias");
        root_alias.path = "/z-root.alias".to_string();
        root_alias.kind = DfsObjectKind::Hardlink;
        root_alias.content_hash = None;
        root_alias.link_target_object_id = Some(target.object_id.clone());
        let mut service_alias = root_alias.clone();
        service_alias.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_service_alias").expect("service alias");
        service_alias.path = "/services/a-service.alias".to_string();

        for object in [&target, &root_alias, &service_alias] {
            backend.put_object(object).expect("object");
        }

        assert_eq!(
            backend
                .hardlink_count_to_object(&file_system.file_system_id, &target.object_id)
                .expect("hardlink count"),
            2
        );
        assert_eq!(
            backend
                .first_hardlink_to_object(&file_system.file_system_id, &target.object_id)
                .expect("first hardlink")
                .expect("first hardlink")
                .path,
            "/services/a-service.alias"
        );
        assert_eq!(
            backend
                .first_hardlink_to_object_outside_prefix(
                    &file_system.file_system_id,
                    &target.object_id,
                    "/services",
                )
                .expect("outside prefix")
                .expect("outside prefix")
                .path,
            "/z-root.alias"
        );

        let mut cursor = None;
        let mut paged_ids = BTreeSet::new();
        let mut saw_hardlink_shard_cursor = false;
        loop {
            let page = backend
                .hardlinks_to_object_page(
                    &file_system.file_system_id,
                    &target.object_id,
                    cursor.as_ref(),
                    1,
                )
                .expect("hardlink page");
            paged_ids.extend(page.objects.into_iter().map(|object| object.object_id));
            if page
                .next_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.shard_id.is_some())
            {
                saw_hardlink_shard_cursor = true;
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        assert_eq!(
            paged_ids,
            [root_alias.object_id, service_alias.object_id]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
        assert!(
            saw_hardlink_shard_cursor,
            "sharded hardlink pages should carry shard cursors for bounded continuation"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_keeps_source_object_when_cross_shard_target_write_fails() {
        let root = temp_root("sharded_backend_cross_shard_target_failure");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let object = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("root object");
        block_shard_journal_writes(&root, "services");

        let mut moved = object.clone();
        moved.path = "/services/readme.md".to_string();
        moved.version = version("version_2");
        let error = backend
            .put_object(&moved)
            .expect_err("target shard journal failure should reject move");

        assert!(
            matches!(error, MetadataBackendError::Io { .. }),
            "target journal append failure should be surfaced as I/O: {error:?}"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("source path")
                .as_ref()
                .map(|object| (&object.path, &object.version, &object.kind)),
            Some((&object.path, &object.version, &DfsObjectKind::File)),
            "failed target write must not tombstone or move the source object"
        );
        assert_eq!(
            backend
                .read_object_shard_index(&object.object_id)
                .expect("object shard index")
                .as_ref()
                .map(|index| (&index.path, &index.shard_id)),
            Some((&object.path, &DfsMetadataShardId::new("root"))),
            "failed target write must leave the object shard index on the source shard"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_keeps_source_namespace_when_cross_shard_target_transaction_fails() {
        let root = temp_root("sharded_backend_cross_shard_transaction_failure");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let object = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("root object");
        block_shard_journal_writes(&root, "services");

        let mut moved = object.clone();
        moved.path = "/services/readme.md".to_string();
        moved.version = version("version_2");
        let error = backend
            .apply_namespace_transaction(&DfsNamespaceTransaction {
                transaction_id: "move-to-services".to_string(),
                file_system_id: file_system.file_system_id.clone(),
                objects: vec![moved],
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::now_utc(),
            })
            .expect_err("target shard journal failure should reject namespace move");

        assert!(
            matches!(error, MetadataBackendError::Io { .. }),
            "target namespace journal append failure should be surfaced as I/O: {error:?}"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("source path")
                .as_ref()
                .map(|object| (&object.path, &object.version, &object.kind)),
            Some((&object.path, &object.version, &DfsObjectKind::File)),
            "failed target namespace transaction must keep the source object visible"
        );
        assert_eq!(
            backend
                .read_object_shard_index(&object.object_id)
                .expect("object shard index")
                .as_ref()
                .map(|index| (&index.path, &index.shard_id)),
            Some((&object.path, &DfsMetadataShardId::new("root"))),
            "failed target namespace transaction must leave the object shard index on the source shard"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_hides_stale_source_path_when_cross_shard_tombstone_fails() {
        let root = temp_root("sharded_backend_cross_shard_tombstone_failure");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let object = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("root object");
        block_shard_journal_writes(&root, "root");

        let mut moved = object.clone();
        moved.path = "/services/readme.md".to_string();
        moved.version = version("version_2");
        let error = backend
            .put_object(&moved)
            .expect_err("source tombstone journal failure should be surfaced");

        assert!(
            matches!(error, MetadataBackendError::Io { .. }),
            "source tombstone journal failure should be surfaced as I/O: {error:?}"
        );
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object by id")
                .as_ref()
                .map(|object| (&object.path, &object.version)),
            Some((&moved.path, &moved.version)),
            "object id lookups should use the target shard after the target write and index update"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("old source path"),
            None,
            "a stale non-tombstoned source object must not remain visible once the index points at the target shard"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &moved.path)
                .expect("target path")
                .as_ref()
                .map(|object| (&object.path, &object.version)),
            Some((&moved.path, &moved.version))
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_migrates_existing_root_object_to_new_path_shard() {
        let root = temp_root("sharded_backend_migrates_existing_root_object");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.path = "/services/readme.md".to_string();
        object.content_hash = Some("blake3:migrated".to_string());
        object.content_size_bytes = Some(8);
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: object.created_by_event_id.clone(),
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                committed_version: object.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_migrate_receipt").expect("event"),
            },
            replica_node_id: node("node_owner"),
            content_hash: object.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        let intent = DfsWriteIntentRecord {
            intent_id: "dfs_intent_migrate".to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: version("version_2"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: object.content_hash.clone(),
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };
        let conflict = DfsConflictRecord {
            conflict_id: "conflict_migrate".to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: object.path.clone(),
            base_version: Some(object.version.clone()),
            local_version: version("version_local"),
            remote_version: version("version_remote"),
            detected_by_node_id: node("node_owner"),
            reason: "migration coverage".to_string(),
            local_content_hash: Some("blake3:local".to_string()),
            remote_content_hash: Some("blake3:remote".to_string()),
            created_by_intent_id: Some(intent.intent_id.clone()),
            strategy: Some("manual".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::UNIX_EPOCH,
        };
        let mut expected_lock_state;
        {
            let mut root_only = LocalShardedMetadataBackend::new(
                &root,
                vec![shard_config("root", DfsMetadataShardAssignment::Root)],
            )
            .expect("root-only sharded backend");
            root_only.put_file_system(&file_system).expect("filesystem");
            root_only.put_object(&object).expect("root object");
            root_only.put_version(&version_record).expect("version");
            root_only.put_receipt(&receipt).expect("receipt");
            root_only.put_intent(&intent).expect("intent");
            root_only.put_conflict(&conflict).expect("conflict");
            root_only
                .apply_advisory_lock_command(
                    &object.file_system_id,
                    &object.object_id,
                    DfsAdvisoryLockTransitionContext {
                        authority_epoch: 1,
                        now_unix_nanos: 1,
                        lease_duration_nanos: 100,
                        max_clock_skew_nanos: 0,
                    },
                    &DfsAdvisoryLockCommand::Acquire {
                        request_id: DfsAdvisoryLockRequestId::new("migration-lock"),
                        session_id: DfsAdvisoryLockSessionId::new("migration-session"),
                        mode: DfsAdvisoryLockMode::Exclusive,
                        blocking: false,
                        process_id: None,
                    },
                )
                .expect("lock state");
            expected_lock_state = root_only
                .get_advisory_lock_state(&object.file_system_id, &object.object_id)
                .expect("lock state")
                .expect("lock aggregate");
        }
        expected_lock_state.authority_rebind_required = true;

        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let report = backend
            .migrate_objects_to_configured_shards(&file_system.file_system_id, 16)
            .expect("migrate objects");

        assert_eq!(
            report,
            DfsMetadataShardMigrationReport {
                file_system_id: file_system.file_system_id.clone(),
                scanned_objects: 1,
                indexed_objects: 0,
                migrated_objects: 1,
                skipped_stale_sources: 0,
                reached_limit: false,
            }
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("migrated path"),
            Some(object.clone())
        );
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("migrated version"),
            Some(version_record.clone())
        );
        assert_eq!(
            backend
                .receipts_for_version(&object.object_id, &object.version)
                .expect("migrated receipts"),
            vec![receipt.clone()]
        );
        assert_eq!(
            backend
                .get_intent(&object.object_id, &intent.intent_id)
                .expect("migrated intent"),
            Some(intent)
        );
        assert_eq!(
            backend
                .get_conflict(&object.object_id, &conflict.conflict_id)
                .expect("migrated conflict"),
            Some(conflict)
        );
        assert_eq!(
            backend
                .get_advisory_lock_state(&object.file_system_id, &object.object_id)
                .expect("migrated lock state"),
            Some(expected_lock_state),
            "same-node shard migration must preserve the object lock aggregate"
        );
        assert_eq!(
            backend
                .shard_backend(&DfsMetadataShardId::new("root"))
                .expect("root shard")
                .get_object(&object.object_id)
                .expect("root object")
                .as_ref()
                .map(|object| object.kind.clone()),
            Some(DfsObjectKind::Tombstone),
            "source shard should retain an explicit tombstone for path-history replay"
        );
        assert_eq!(
            backend
                .read_object_shard_index(&object.object_id)
                .expect("object index")
                .as_ref()
                .map(|index| (&index.path, &index.shard_id)),
            Some((
                &normalize_path(&object.path),
                &DfsMetadataShardId::new("services")
            ))
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_skips_stale_source_when_target_shard_already_has_object() {
        let root = temp_root("sharded_backend_skips_stale_source_target_object");
        let file_system = file_system();
        let mut stale = object(version("version_1"));
        stale.path = "/services/readme.md".to_string();
        let mut current = stale.clone();
        current.content_hash = Some("blake3:current-object".to_string());
        current.content_size_bytes = Some(7);
        let stale_version = DfsObjectVersionRecord {
            file_system_id: stale.file_system_id.clone(),
            object_id: stale.object_id.clone(),
            version: stale.version.clone(),
            content_hash: Some("blake3:stale-version".to_string()),
            size_bytes: Some(13),
            locations: stale.current_locations.clone(),
            created_by_event_id: stale.created_by_event_id.clone(),
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let current_version = DfsObjectVersionRecord {
            content_hash: current.content_hash.clone(),
            size_bytes: current.content_size_bytes,
            ..stale_version.clone()
        };
        {
            let mut root_only = LocalShardedMetadataBackend::new(
                &root,
                vec![shard_config("root", DfsMetadataShardAssignment::Root)],
            )
            .expect("root-only sharded backend");
            root_only.put_file_system(&file_system).expect("filesystem");
            root_only.put_object(&stale).expect("stale root object");
            root_only
                .put_version(&stale_version)
                .expect("stale root version");
        }

        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        {
            let services = backend
                .shard_backend_mut(&DfsMetadataShardId::new("services"))
                .expect("services shard");
            services
                .put_file_system(&file_system)
                .expect("services filesystem");
            services
                .put_object(&current)
                .expect("current services object");
            services
                .put_version(&current_version)
                .expect("current services version");
        }

        let report = backend
            .migrate_objects_to_configured_shards(&file_system.file_system_id, 16)
            .expect("migration skips stale root copy");

        assert_eq!(
            report,
            DfsMetadataShardMigrationReport {
                file_system_id: file_system.file_system_id.clone(),
                scanned_objects: 1,
                indexed_objects: 1,
                migrated_objects: 0,
                skipped_stale_sources: 1,
                reached_limit: false,
            }
        );
        assert_eq!(
            backend
                .get_object(&current.object_id)
                .expect("current object"),
            Some(current.clone())
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &current.path)
                .expect("current path"),
            Some(current.clone())
        );
        assert_eq!(
            backend
                .read_object_shard_index(&current.object_id)
                .expect("object index")
                .as_ref()
                .map(|index| (&index.path, &index.shard_id)),
            Some((
                &normalize_path(&current.path),
                &DfsMetadataShardId::new("services")
            ))
        );
        assert_eq!(
            backend
                .get_version(&current.object_id, &current.version)
                .expect("current version"),
            Some(current_version)
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_bounds_configured_shard_migration() {
        let root = temp_root("sharded_backend_bounds_configured_shard_migration");
        let file_system = file_system();
        let mut first = object(version("version_1"));
        first.path = "/services/a.txt".to_string();
        let mut second = object(version("version_1"));
        second.object_id = DfsObjectId::from_legacy_named("obj_second").expect("object");
        second.path = "/users/b.txt".to_string();
        {
            let mut root_only = LocalShardedMetadataBackend::new(
                &root,
                vec![shard_config("root", DfsMetadataShardAssignment::Root)],
            )
            .expect("root-only sharded backend");
            root_only.put_file_system(&file_system).expect("filesystem");
            root_only.put_object(&first).expect("first root object");
            root_only.put_object(&second).expect("second root object");
        }

        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
                shard_config(
                    "users",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/users".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let report = backend
            .migrate_objects_to_configured_shards(&file_system.file_system_id, 1)
            .expect("bounded migration");

        assert_eq!(
            report.scanned_objects, 1,
            "bounded migration should not scan the rest of the shard after reaching the migration limit"
        );
        assert_eq!(report.migrated_objects, 1);
        assert!(report.reached_limit);
        assert_eq!(
            backend
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects")
                .into_iter()
                .filter(|object| object.kind != DfsObjectKind::Tombstone)
                .count(),
            2,
            "bounded migration must keep both live objects visible through the sharded object index"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_falls_back_to_source_path_during_shard_activation() {
        let root = temp_root("sharded_backend_activation_path_fallback");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.path = "/services/pre-migration.txt".to_string();
        {
            let mut root_only = LocalShardedMetadataBackend::new(
                &root,
                vec![shard_config("root", DfsMetadataShardAssignment::Root)],
            )
            .expect("root-only sharded backend");
            root_only.put_file_system(&file_system).expect("filesystem");
            root_only.put_object(&object).expect("root object");
        }

        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
                shard_config(
                    "users",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/users".to_string(),
                    },
                ),
                shard_config(
                    "agents",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/agents".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");

        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("activation fallback path"),
            Some(object),
            "publishing a new path-prefix shard before migration must not make existing root-shard paths disappear"
        );

        reset_metadata_current_tables_snapshot_calls();
        assert_eq!(
            backend
                .get_object_by_path(
                    &file_system.file_system_id,
                    "/services/not-created-during-migration.txt",
                )
                .expect("bounded migration miss"),
            None
        );
        assert_eq!(
            metadata_current_tables_snapshot_calls(),
            2,
            "a create miss should inspect the selected shard plus the one previous-topology source, not every configured shard"
        );

        let migration = backend
            .migrate_objects_to_configured_shards(&file_system.file_system_id, 8)
            .expect("finish bounded migration");
        assert!(!migration.reached_limit);
        reset_metadata_current_tables_snapshot_calls();
        assert_eq!(
            backend
                .get_object_by_path(
                    &file_system.file_system_id,
                    "/services/not-created-after-migration.txt",
                )
                .expect("steady-state miss"),
            None
        );
        assert_eq!(
            metadata_current_tables_snapshot_calls(),
            1,
            "a completed migration should retire its fallback topology"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_ignores_index_for_unconfigured_local_shard() {
        let root = temp_root("sharded_backend_unconfigured_index");
        let file_system = file_system();
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![shard_config("root", DfsMetadataShardAssignment::Root)],
        )
        .expect("root-only sharded backend");
        let mut object = object(version("version_1"));
        object.path = "/services/indexed-before-kv.txt".to_string();
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("root object");
        backend
            .write_object_shard_index(&object, &DfsMetadataShardId::new("services"))
            .expect("stale object shard index");

        assert_eq!(
            backend.get_object(&object.object_id).expect("object by id"),
            Some(object.clone()),
            "an index that names an unopened shard must not break object-id reads"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("object by path"),
            Some(object),
            "an index that names an unopened shard must not hide source-shard path reads"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_create_commit_creates_missing_object_shard_index() {
        let root = temp_root("sharded_backend_create_commit_new_index");
        let file_system = file_system();
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let mut child = object(version("version_1"));
        child.path = "/services/content-commit.bin".to_string();
        child.content_hash = Some("blake3:content-commit".to_string());
        child.content_size_bytes = Some(4096);
        let version_record = DfsObjectVersionRecord {
            file_system_id: child.file_system_id.clone(),
            object_id: child.object_id.clone(),
            version: child.version.clone(),
            content_hash: child.content_hash.clone(),
            size_bytes: child.content_size_bytes,
            locations: child.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let mut parent = object(version("version_1"));
        parent.object_id = DfsObjectId::from_legacy_named("obj_services_parent").expect("parent");
        parent.path = "/services".to_string();
        parent.kind = DfsObjectKind::Directory;
        parent.content_hash = None;
        parent.content_size_bytes = None;
        parent.mode = 0o755;
        let parent_version = DfsObjectVersionRecord {
            file_system_id: parent.file_system_id.clone(),
            object_id: parent.object_id.clone(),
            version: parent.version.clone(),
            content_hash: None,
            size_bytes: None,
            locations: parent.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let mut touched_parent = parent.clone();
        touched_parent.version = crate::namespace::next_version(&parent.version);
        touched_parent.modified_at_unix_nanos = 1;
        touched_parent.changed_at_unix_nanos = 1;
        touched_parent.last_modified_by_event_id = Some(
            EventId::from_legacy_named("evt_content_create_parent_touch").expect("parent event"),
        );
        let touched_parent_version = DfsObjectVersionRecord {
            file_system_id: touched_parent.file_system_id.clone(),
            object_id: touched_parent.object_id.clone(),
            version: touched_parent.version.clone(),
            content_hash: None,
            size_bytes: None,
            locations: touched_parent.current_locations.clone(),
            created_by_event_id: touched_parent.last_modified_by_event_id.clone(),
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let committed_intent = DfsWriteIntentRecord {
            intent_id: "intent-content-commit-new-index".to_string(),
            file_system_id: child.file_system_id.clone(),
            object_id: child.object_id.clone(),
            base_version: None,
            target_version: child.version.clone(),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: child.current_locations.clone(),
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Committed,
            content_hash: child.content_hash.clone(),
            object_metadata: Some(child.clone()),
            touch_parent_on_create: true,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };
        let mut pending_intent = committed_intent.clone();
        pending_intent.status = DfsWriteIntentStatus::ReceiptsPending;
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: child.file_system_id.clone(),
                object_id: child.object_id.clone(),
                committed_version: child.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_content_commit_new_index")
                    .expect("event"),
            },
            replica_node_id: node("node_owner"),
            content_hash: child.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend
            .put_object_and_version(&parent, &parent_version)
            .expect("parent object and version");
        let transaction = DfsNamespaceTransaction {
            transaction_id: "transaction-content-create-new-index".to_string(),
            file_system_id: child.file_system_id.clone(),
            objects: vec![child.clone(), touched_parent.clone()],
            versions: vec![version_record, touched_parent_version],
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };

        let entries = backend
            .commit_create_intent_receipts_and_namespace(
                &pending_intent,
                std::slice::from_ref(&receipt),
                &transaction,
                &committed_intent,
            )
            .expect("create commit batch");
        assert!(matches!(
            entries.first().map(|entry| &entry.mutation),
            Some(DfsMetadataMutation::PutIntent {
                status: DfsWriteIntentStatus::ReceiptsPending,
                ..
            })
        ));
        assert!(matches!(
            entries.get(1).map(|entry| &entry.mutation),
            Some(DfsMetadataMutation::PutReceipt { .. })
        ));
        assert!(matches!(
            entries.get(2).map(|entry| &entry.mutation),
            Some(DfsMetadataMutation::ApplyNamespaceTransaction { .. })
        ));
        assert!(matches!(
            entries.get(3).map(|entry| &entry.mutation),
            Some(DfsMetadataMutation::PutIntent {
                status: DfsWriteIntentStatus::Committed,
                ..
            })
        ));

        assert_eq!(
            backend
                .read_object_shard_index(&child.object_id)
                .expect("object shard index")
                .as_ref()
                .map(|index| (&index.path, &index.shard_id)),
            Some((
                &normalize_path(&child.path),
                &DfsMetadataShardId::new("services")
            )),
            "create commits should publish the new object's lookup index"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &child.path)
                .expect("path lookup"),
            Some(child.clone()),
            "the create batch must leave path lookup correct"
        );
        assert_eq!(
            backend
                .get_object(&touched_parent.object_id)
                .expect("touched parent"),
            Some(touched_parent.clone())
        );
        let duplicate_error = backend
            .commit_create_intent_receipts_and_namespace(
                &pending_intent,
                std::slice::from_ref(&receipt),
                &transaction,
                &committed_intent,
            )
            .expect_err("a create batch must compare-and-publish the child exactly once");
        assert!(
            duplicate_error.to_string().contains("already durable"),
            "duplicate child publication should fail at the object-id precondition: {duplicate_error}"
        );

        drop(backend);
        let reopened = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("reopen sharded backend");
        assert_eq!(
            reopened
                .get_object_by_path(&file_system.file_system_id, &child.path)
                .expect("reopened child path"),
            Some(child),
            "the committed child must survive journal/table reopen"
        );
        assert_eq!(
            reopened
                .get_object(&touched_parent.object_id)
                .expect("reopened touched parent"),
            Some(touched_parent),
            "the parent touch must survive the same create transaction"
        );

        cleanup(&root);
    }

    #[cfg(unix)]
    #[test]
    fn sharded_backend_content_commit_skips_matching_object_shard_index_rewrite() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_root("sharded_backend_content_commit_skip_index_rewrite");
        let file_system = file_system();
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let mut object = object(version("version_1"));
        object.path = "/services/existing-content.bin".to_string();
        object.content_hash = Some("blake3:old-content".to_string());
        object.content_size_bytes = Some(128);
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("initial object");

        let mut committed = object.clone();
        committed.version = version("version_2");
        committed.content_hash = Some("blake3:new-content".to_string());
        committed.content_size_bytes = Some(4096);
        let version_record = DfsObjectVersionRecord {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            version: committed.version.clone(),
            content_hash: committed.content_hash.clone(),
            size_bytes: committed.content_size_bytes,
            locations: committed.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let intent = DfsWriteIntentRecord {
            intent_id: "intent-content-commit-existing-index".to_string(),
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: committed.version.clone(),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: committed.current_locations.clone(),
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Committed,
            content_hash: committed.content_hash.clone(),
            object_metadata: Some(committed.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                committed_version: committed.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_content_commit_existing_index")
                    .expect("event"),
            },
            replica_node_id: node("node_owner"),
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        let mut insufficient_receipts_intent = intent.clone();
        insufficient_receipts_intent
            .desired_replica_node_ids
            .push(node("node_replica"));
        insufficient_receipts_intent.policy =
            DfsReplicationPolicy::new(2, 2).expect("two-replica policy");
        let insufficient_error = backend
            .put_object_version_intent_and_receipts(
                &committed,
                &version_record,
                &insufficient_receipts_intent,
                std::slice::from_ref(&receipt),
            )
            .expect_err("visible commit must require W distinct durable receipts");
        assert!(
            matches!(insufficient_error, MetadataBackendError::Serde(_)),
            "receipt-threshold rejection should be explicit: {insufficient_error:?}"
        );
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object after insufficient receipt rejection"),
            Some(object.clone()),
            "a rejected commit must retain the prior visible version"
        );

        let mut stale_intent = intent.clone();
        stale_intent.base_version = Some(version("version_0"));
        let stale_error = backend
            .put_object_version_intent_and_receipts(
                &committed,
                &version_record,
                &stale_intent,
                std::slice::from_ref(&receipt),
            )
            .expect_err("visible commit must compare-and-swap its base version");
        assert!(
            matches!(stale_error, MetadataBackendError::Conflict { .. }),
            "stale-base rejection should report a conflict: {stale_error:?}"
        );
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object after stale commit rejection"),
            Some(object.clone()),
            "a stale commit must retain the prior visible version"
        );

        let index_parent = backend
            .object_shard_index_path(&committed.object_id)
            .parent()
            .expect("index parent")
            .to_path_buf();
        let original_permissions = fs::metadata(&index_parent)
            .expect("index parent metadata")
            .permissions();
        let mut read_only_permissions = original_permissions.clone();
        read_only_permissions.set_mode(0o500);
        fs::set_permissions(&index_parent, read_only_permissions)
            .expect("make shard index directory read-only");

        let result = backend.put_object_version_intent_and_receipts(
            &committed,
            &version_record,
            &intent,
            std::slice::from_ref(&receipt),
        );

        fs::set_permissions(&index_parent, original_permissions)
            .expect("restore shard index directory permissions");
        let entries =
            result.expect("content commit should not rewrite an already-correct shard index");
        assert!(matches!(
            entries.first().map(|entry| &entry.mutation),
            Some(DfsMetadataMutation::PutIntent {
                status: DfsWriteIntentStatus::ReceiptsPending,
                ..
            })
        ));
        assert!(matches!(
            entries.get(1).map(|entry| &entry.mutation),
            Some(DfsMetadataMutation::PutReceipt { .. })
        ));
        assert!(matches!(
            entries.get(2).map(|entry| &entry.mutation),
            Some(DfsMetadataMutation::CommitVisibleVersion { .. })
        ));
        assert!(matches!(
            entries.get(3).map(|entry| &entry.mutation),
            Some(DfsMetadataMutation::PutIntent {
                status: DfsWriteIntentStatus::Committed,
                ..
            })
        ));
        assert_eq!(
            backend
                .get_object(&committed.object_id)
                .expect("object by id"),
            Some(committed),
            "content commit should still advance the object metadata"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_does_not_materialize_object_when_journal_append_fails() {
        let root = temp_root("local_backend_journal_failure_does_not_materialize_object");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let object = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        fs::remove_dir_all(root.join("journal").join("segments")).expect("remove segments dir");
        fs::write(root.join("journal").join("segments"), b"not a directory")
            .expect("block journal segment writes");

        let error = backend
            .put_object(&object)
            .expect_err("object write should fail before materialization when the journal fails");

        assert!(
            matches!(error, MetadataBackendError::Io { .. }),
            "journal append failure should be surfaced as I/O: {error:?}"
        );
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object lookup"),
            None,
            "object metadata must not become visible without a journal entry"
        );
        assert!(
            !table_path_exists(&backend, &object.file_system_id, &object.path),
            "path index must not become visible without a journal entry"
        );

        fs::remove_file(root.join("journal").join("segments")).ok();
        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_durable_journal_tail_before_same_instance_retry() {
        let root = temp_root("local_backend_same_instance_journal_tail_recovery");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend
            .persist_current_metadata_checkpoint_unlocked()
            .expect("checkpoint table head before injected append failure");

        let mut durable_only = object(version("version_1"));
        durable_only.object_id =
            DfsObjectId::from_legacy_named("obj_durable_table_gap").expect("object id");
        durable_only.path = "/docs/durable-table-gap.md".to_string();
        let table_path = metadata_table_path(&root);
        let error = {
            let _lock = backend.begin_metadata_write().expect("metadata write lock");
            let durable_only = normalized_object_metadata(&durable_only);
            let previous = backend
                .get_object(&durable_only.object_id)
                .expect("previous object");
            let entry = backend
                .write_journal_unlocked(DfsMetadataMutation::put_object(&durable_only))
                .expect("durable journal append");
            let mut table_ops = Vec::new();
            let mut rollback = DfsMetadataTableRollback::default();
            backend
                .plan_object_index_table_ops_with_previous(
                    &durable_only,
                    previous.as_ref(),
                    &mut table_ops,
                    &mut rollback,
                )
                .expect("preapplied table plan");
            assert!(
                backend.tables.objects.contains_key(&durable_only.object_id),
                "the injected failure cut starts after in-memory preapply"
            );
            fs::remove_file(&table_path).expect("remove empty table log");
            fs::create_dir(&table_path).expect("block table-log append with a directory");
            backend
                .append_preapplied_table_ops(entry.sequence, table_ops, rollback)
                .expect_err("table append fails after the durable journal append")
        };
        assert!(matches!(error, MetadataBackendError::Io { .. }));
        assert!(
            !backend.tables.objects.contains_key(&durable_only.object_id),
            "failed materialization must not expose the durable-only object"
        );
        assert_eq!(
            backend.journal_since(0).expect("durable journal").len(),
            2,
            "the failed operation must remain durable after the filesystem record"
        );

        fs::remove_dir(&table_path).expect("restore table-log path");
        let mut successor = object(version("version_2"));
        successor.object_id =
            DfsObjectId::from_legacy_named("obj_after_table_gap").expect("object id");
        successor.path = "/docs/after-table-gap.md".to_string();
        backend
            .put_object(&successor)
            .expect("same handle recovers the durable tail before admitting a successor");

        assert_eq!(
            backend
                .get_object(&durable_only.object_id)
                .expect("recovered object"),
            Some(durable_only.clone())
        );
        assert_eq!(
            backend
                .get_object(&successor.object_id)
                .expect("successor object"),
            Some(successor.clone())
        );
        let journal = backend.journal_since(0).expect("contiguous journal");
        assert_eq!(
            journal
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(backend.materialized_through_sequence, 3);
        assert_eq!(
            backend
                .metadata_prefix_proof()
                .expect("recovered prefix proof")
                .journal_sequence,
            3
        );

        drop(backend);
        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen backend");
        assert_eq!(
            reopened
                .get_object(&durable_only.object_id)
                .expect("reopened recovered object"),
            Some(durable_only)
        );
        assert_eq!(
            reopened
                .get_object(&successor.object_id)
                .expect("reopened successor object"),
            Some(successor)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_replays_journal_entries_after_materialization_cursor() {
        let root = temp_root("local_backend_replays_after_materialization_cursor");
        let object = object(version("version_1"));
        let journaled_sequence = {
            let mut backend =
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                    .expect("backend");
            let file_system = file_system();
            backend.put_file_system(&file_system).expect("filesystem");
            let entry = backend
                .write_journal(DfsMetadataMutation::PutObject {
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    path: object.path.clone(),
                    object: Some(object.clone()),
                })
                .expect("journal object without materializing");
            assert_eq!(
                backend
                    .get_object(&object.object_id)
                    .expect("object lookup"),
                None
            );
            entry.sequence
        };

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");

        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object.clone()),
            "reopen should replay journaled metadata that was not yet materialized"
        );
        assert_eq!(
            read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
                .expect("materialization cursor")
                .last_sequence,
            journaled_sequence
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_stale_materialization_cursor_without_rewriting_table_log() {
        let root = temp_root("local_backend_stale_materialization_cursor_no_rewrite");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let object = object(version("version_1"));
        let (last_sequence, table_len_before) = {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            let entry = backend.put_object(&object).expect("object");
            let table_len = fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len();
            (entry.sequence, table_len)
        };
        write_sobj_atomic(
            &root
                .join("records")
                .join("local-materialization")
                .join("cursor.sobj"),
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: 0,
            },
        )
        .expect("stale materialization cursor");

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");

        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object),
            "stale cursor recovery must preserve the materialized object"
        );
        assert_eq!(
            read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
                .expect("materialization cursor")
                .last_sequence,
            last_sequence
        );
        assert_eq!(
            fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len(),
            table_len_before,
            "idempotent cursor recovery must not append duplicate table batches"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_checkpoints_materialization_cursor_instead_of_every_write() {
        let root = temp_root("local_backend_deferred_materialization_cursor");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut expected_objects = Vec::new();
        let (last_sequence, table_len_before) = {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            let mut last_sequence = 0;
            for index in 0..8 {
                let mut object = object(version("version_1"));
                object.object_id =
                    DfsObjectId::from_legacy_named(format!("obj_cursor_defer_{index}"))
                        .expect("object id");
                object.path = format!("/services/cursor-defer/{index}.txt");
                let entry = backend.put_object(&object).expect("object");
                last_sequence = entry.sequence;
                expected_objects.push(object);
            }
            let cursor =
                read_sobj::<DfsMetadataReplayCursor>(&backend.local_materialization_cursor_path())
                    .expect("materialization cursor");
            assert_eq!(
                cursor.last_sequence, 0,
                "short local write bursts should not force a cursor fsync for every metadata mutation"
            );
            let table_len = fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len();
            (last_sequence, table_len)
        };

        let reopened =
            LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopen backend");

        for object in expected_objects {
            assert_eq!(
                reopened
                    .get_object(&object.object_id)
                    .expect("object lookup"),
                Some(object),
                "reopen should recover objects whose materialization cursor was not checkpointed"
            );
        }
        assert_eq!(
            read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
                .expect("materialization cursor")
                .last_sequence,
            last_sequence,
            "reopen recovery should advance the cursor after idempotent replay"
        );
        assert_eq!(
            fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len(),
            table_len_before,
            "idempotent replay from a deferred cursor must not duplicate materialized table batches"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_lost_relaxed_table_log_after_cursor_checkpoint() {
        let root = temp_root("local_backend_lost_relaxed_table_log");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        let mut expected_edge_objects = Vec::new();
        for index in 0..520 {
            let mut object = object(version("version_1"));
            object.object_id = DfsObjectId::from_legacy_named(format!("obj_relaxed_table_{index}"))
                .expect("object id");
            object.path = format!("/services/relaxed-table/{index}.txt");
            backend.put_object(&object).expect("object");
            if matches!(index, 0 | 519) {
                expected_edge_objects.push(object);
            }
        }
        let final_sequence = backend
            .journal_since(0)
            .expect("journal")
            .last()
            .expect("journal entry")
            .sequence;
        let checkpoint =
            read_sobj::<DfsMetadataReplayCursor>(&backend.local_materialization_cursor_path())
                .expect("materialization cursor");
        assert!(
            checkpoint.last_sequence > 0 && checkpoint.last_sequence <= final_sequence,
            "the test must lose a table log after a durable cursor checkpoint"
        );
        drop(backend);

        fs::remove_file(metadata_table_path(&root)).expect("simulate lost rebuildable table log");

        let reopened =
            LocalSobjMetadataBackend::new(shard_id, &root).expect("recover from journal");
        for expected in expected_edge_objects {
            assert_eq!(
                reopened
                    .get_object(&expected.object_id)
                    .expect("recovered object"),
                Some(expected)
            );
        }
        assert_eq!(reopened.materialized_through_sequence, final_sequence);
        assert_eq!(
            reopened
                .metadata_prefix_proof()
                .expect("recovered prefix proof")
                .journal_sequence,
            final_sequence
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_replays_already_materialized_visible_commits_after_newer_head() {
        fn commit_test_version(
            backend: &mut LocalSobjMetadataBackend,
            previous: &DfsObjectMetadata,
            committed_version: &str,
            content_hash: &str,
            event_id: &str,
        ) -> (DfsObjectMetadata, DfsMetadataJournalEntry) {
            let replica_node_id = node("node_replica");
            let mut committed = previous.clone();
            committed.version = version(committed_version);
            committed.content_hash = Some(content_hash.to_string());
            committed.content_size_bytes = Some(content_hash.len() as u64);
            committed.current_locations = vec![replica_node_id.clone()];
            let visible_version = DfsObjectVersionRecord {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                version: committed.version.clone(),
                content_hash: committed.content_hash.clone(),
                size_bytes: committed.content_size_bytes,
                locations: committed.current_locations.clone(),
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: true,
            };
            let receipt = DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: committed.file_system_id.clone(),
                    object_id: committed.object_id.clone(),
                    committed_version: committed.version.clone(),
                    owner_node_id: committed.owner_node_id.clone(),
                    event_id: EventId::from_legacy_named(event_id).expect("event"),
                },
                replica_node_id,
                content_hash: committed.content_hash.clone(),
                received_at: OffsetDateTime::UNIX_EPOCH,
            };
            backend.put_receipt(&receipt).expect("receipt");
            let entry = backend
                .commit_visible_version(
                    &previous.version,
                    &committed,
                    &visible_version,
                    std::slice::from_ref(&receipt.replica_node_id),
                )
                .expect("commit visible version");
            (committed, entry)
        }

        let root = temp_root("local_backend_replays_chained_visible_commits_after_newer_head");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let (final_object, final_sequence) = {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            let initial = object(version("version_1"));
            backend.put_object(&initial).expect("initial object");
            let (committed_v2, commit_v2) = commit_test_version(
                &mut backend,
                &initial,
                "version_2",
                "blake3:version-2",
                "evt_replay_commit_v2",
            );
            let (committed_v3, commit_v3) = commit_test_version(
                &mut backend,
                &committed_v2,
                "version_3",
                "blake3:version-3",
                "evt_replay_commit_v3",
            );

            write_sobj_atomic(
                &metadata_table_snapshot_path(&root),
                &DfsMetadataTableSnapshot {
                    format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
                    materialized_through_sequence: None,
                    tables: Some(metadata_tables_snapshot(&backend.tables)),
                    ops: Vec::new(),
                    semantic_state_fingerprint: None,
                    prefix_proof: None,
                },
            )
            .expect("legacy structured snapshot without materialized sequence");
            truncate_metadata_table_log(&root).expect("truncate table log");
            write_sobj_atomic(
                &backend.local_materialization_cursor_path(),
                &DfsMetadataReplayCursor {
                    shard_id: shard_id.clone(),
                    last_sequence: commit_v2.sequence.saturating_sub(1),
                },
            )
            .expect("stale materialization cursor before visible commits");
            (committed_v3, commit_v3.sequence)
        };

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");

        assert_eq!(
            reopened
                .get_object(&final_object.object_id)
                .expect("object lookup"),
            Some(final_object),
            "replay must not reject or roll back an already-materialized older visible commit"
        );
        assert_eq!(
            read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
                .expect("materialization cursor")
                .last_sequence,
            final_sequence
        );
        assert_eq!(
            reopened.materialized_through_sequence, final_sequence,
            "legacy snapshots should learn the durable materialization sequence from replay"
        );
        assert_eq!(
            fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len(),
            0,
            "idempotent replay should not rewrite table batches for already-materialized commits"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_open_does_not_rewrite_existing_metadata_table() {
        let root = temp_root("local_backend_open_preserves_metadata_table");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend
            .put_object(&object(version("version_1")))
            .expect("object");
        let table_path = metadata_table_path(&root);
        let before_len = fs::metadata(&table_path).expect("metadata table").len();

        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen backend");

        let after_len = fs::metadata(&table_path)
            .expect("metadata table after reopen")
            .len();
        assert_eq!(
            before_len, after_len,
            "opening a healthy backend must not rewrite the materialized metadata table"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_compacts_materialized_table_log_to_snapshot() {
        let root = temp_root("local_backend_compacts_materialized_table_log");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        let mut expected_objects = Vec::new();
        for index in 0..8 {
            let mut object = object(version("version_1"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_compacted_metadata_table_{index}"))
                    .expect("object");
            object.path = format!("/docs/compacted-{index}.md");
            backend.put_object(&object).expect("object");
            expected_objects.push(object);
        }
        let table_path = metadata_table_path(&root);
        assert!(
            fs::metadata(&table_path).expect("metadata table").len() > 0,
            "writes should first append to the materialized table log"
        );
        let prefix_before_compaction = backend.metadata_prefix_proof().expect("prefix proof");

        backend
            .compact_metadata_table_log_if_needed(1)
            .expect("compact metadata table");

        assert!(
            metadata_table_snapshot_path(&root).is_file(),
            "compaction should persist a compact materialized table snapshot"
        );
        let snapshot: DfsMetadataTableSnapshot =
            read_sobj(&metadata_table_snapshot_path(&root)).expect("metadata table snapshot");
        assert_eq!(
            snapshot.format_version, METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            "new compactions should use structured metadata table snapshots"
        );
        assert!(snapshot.tables.is_some());
        assert!(snapshot.ops.is_empty());
        assert!(snapshot.semantic_state_fingerprint.is_some());
        assert_eq!(
            snapshot.prefix_proof,
            Some(prefix_before_compaction.clone())
        );
        assert_eq!(
            fs::metadata(&table_path)
                .expect("truncated metadata table")
                .len(),
            0,
            "compaction should truncate historical table ops after the snapshot is durable"
        );
        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen compacted backend");
        assert_eq!(
            reopened.metadata_prefix_proof().expect("reopened proof"),
            prefix_before_compaction,
            "compaction must carry the exact journal-chain head"
        );
        for object in expected_objects {
            assert_eq!(
                reopened
                    .get_object(&object.object_id)
                    .expect("object lookup"),
                Some(object)
            );
        }
        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_truncates_torn_table_log_tail() {
        let root = temp_root("local_backend_truncates_torn_table_log");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let object_a = object(version("version_1"));
        {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            backend.put_object(&object_a).expect("object");
        }
        let table_path = metadata_table_path(&root);
        let good_len = fs::metadata(&table_path).expect("table log").len();
        assert!(good_len > 0);

        // Crash mid-append: a full length prefix promising more bytes than
        // the writer managed to flush.
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&table_path)
            .expect("open table log");
        file.write_all(&512_u32.to_le_bytes())
            .and_then(|()| file.write_all(&[0xAB; 17]))
            .expect("torn append");
        drop(file);

        let mut object_b = object(version("version_2"));
        object_b.object_id =
            DfsObjectId::from_legacy_named("obj_after_crash").expect("valid object id");
        object_b.path = "docs/after-crash.md".to_string();
        {
            let mut reopened = LocalSobjMetadataBackend::new(shard_id.clone(), &root)
                .expect("reopen tolerates torn tail");
            assert_eq!(
                reopened
                    .get_object(&object_a.object_id)
                    .expect("object lookup"),
                Some(object_a.clone()),
                "complete records before the torn tail must replay"
            );
            assert_eq!(
                fs::metadata(&table_path).expect("table log").len(),
                good_len,
                "open must cut the torn record off the log so later appends stay framed"
            );
            // The append that lands after the crash: without the truncation it
            // would be entombed behind the torn record and break every later
            // replay with an sobj framing error.
            reopened.put_object(&object_b).expect("post-crash append");
        }

        let reopened =
            LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen after post-crash append");
        assert_eq!(
            reopened
                .get_object(&object_a.object_id)
                .expect("object lookup"),
            Some(object_a)
        );
        assert_eq!(
            reopened
                .get_object(&object_b.object_id)
                .expect("object lookup"),
            Some(object_b)
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_truncates_invalid_table_log_tail() {
        let root = temp_root("local_backend_truncates_invalid_table_log_tail");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let object_a = object(version("version_1"));
        {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            backend.put_object(&object_a).expect("object");
        }
        let table_path = metadata_table_path(&root);
        let good_len = fs::metadata(&table_path).expect("table log").len();

        let invalid_payload = b"not-sobj";
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&table_path)
            .expect("open table log");
        file.write_all(&(invalid_payload.len() as u32).to_le_bytes())
            .and_then(|()| file.write_all(invalid_payload))
            .expect("invalid tail append");
        drop(file);

        let reopened =
            LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen tolerates invalid tail");
        assert_eq!(
            reopened
                .get_object(&object_a.object_id)
                .expect("object lookup"),
            Some(object_a),
            "complete records before the malformed tail must replay"
        );
        assert_eq!(
            fs::metadata(&table_path).expect("table log").len(),
            good_len,
            "open must cut a fully length-prefixed but malformed tail before later appends"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_reports_corrupt_snapshot_path() {
        let root = temp_root("local_backend_reports_corrupt_snapshot_path");
        let snapshot_path = metadata_table_snapshot_path(&root);
        fs::create_dir_all(snapshot_path.parent().expect("snapshot parent"))
            .expect("snapshot parent");
        fs::write(&snapshot_path, [0x1f, 0x8b, 0x08]).expect("truncated snapshot");

        let error = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect_err("corrupt snapshot should fail to open");
        let message = error.to_string();
        assert!(
            message.contains(snapshot_path.to_string_lossy().as_ref()),
            "error should name the corrupt snapshot path: {message}"
        );
        assert!(
            message.contains("failed to decode"),
            "error should describe the decode failure: {message}"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_migrates_legacy_op_snapshot_to_structured_snapshot() {
        let root = temp_root("local_backend_migrates_legacy_op_snapshot");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let object = object(version("version_1"));
        {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            backend.put_object(&object).expect("object");
            write_sobj_atomic(
                &metadata_table_snapshot_path(&root),
                &DfsMetadataTableSnapshot {
                    format_version: LEGACY_METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
                    materialized_through_sequence: None,
                    tables: None,
                    ops: metadata_table_snapshot_ops(&backend.tables),
                    semantic_state_fingerprint: None,
                    prefix_proof: None,
                },
            )
            .expect("legacy metadata table snapshot");
            truncate_metadata_table_log(&root).expect("truncate table log");
        }

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");

        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object)
        );
        let snapshot: DfsMetadataTableSnapshot =
            read_sobj(&metadata_table_snapshot_path(&root)).expect("migrated snapshot");
        assert_eq!(
            snapshot.format_version, METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            "opening a legacy op snapshot should migrate it to the structured snapshot format"
        );
        assert!(snapshot.tables.is_some());
        assert!(snapshot.ops.is_empty());
        assert_eq!(
            fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len(),
            0,
            "snapshot migration should compact any table tail into the structured image"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_allocates_journal_sequences_across_open_handles() {
        let root = temp_root("local_backend_allocates_journal_sequences_across_open_handles");
        let mut first = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("first backend");
        let mut second =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("second backend");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.object_id = DfsObjectId::from_legacy_named("obj_second_handle").expect("object");
        object.path = "/docs/second.md".to_string();

        let first_entry = first.put_file_system(&file_system).expect("filesystem");
        let second_entry = second
            .put_object(&object)
            .expect("object from second handle");

        assert_eq!(first_entry.sequence, 1);
        assert_eq!(
            second_entry.sequence, 2,
            "a second backend opened before the first write must not reuse the first sequence"
        );
        let journal = first.journal_since(0).expect("journal");
        assert_eq!(journal.len(), 2);
        assert!(matches!(
            journal[0].mutation,
            DfsMetadataMutation::PutFileSystem { .. }
        ));
        assert!(matches!(
            journal[1].mutation,
            DfsMetadataMutation::PutObject { .. }
        ));

        cleanup(&root);
    }

    #[test]
    fn local_backend_readers_see_table_batches_from_other_open_handles() {
        let root = temp_root("local_backend_readers_see_table_batches_from_other_open_handles");
        let mut first = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("first backend");
        let second = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("second backend");
        let file_system = file_system();
        let object = object(version("version_1"));

        first.put_file_system(&file_system).expect("filesystem");
        first.put_object(&object).expect("object");

        assert_eq!(
            second
                .get_file_system(&file_system.file_system_id)
                .expect("filesystem from second handle"),
            Some(file_system.clone())
        );
        assert_eq!(
            second
                .get_object(&object.object_id)
                .expect("object from second handle"),
            Some(object.clone())
        );
        assert_eq!(
            second
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects from second handle"),
            vec![object]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reloads_snapshot_when_other_handle_compacts_table_log() {
        let root = temp_root("local_backend_reloads_snapshot_after_external_compaction");
        let mut writer =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("writer backend");
        writer.put_file_system(&file_system()).expect("filesystem");
        persist_metadata_table_snapshot(
            &root,
            &writer.tables,
            Some(writer.materialized_through_sequence),
            writer.prefix_proof.as_ref(),
        )
        .expect("snapshot filesystem before table-log shrink");
        truncate_metadata_table_log(&root).expect("truncate table log after snapshot");
        drop(writer);
        let mut writer =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("writer backend after snapshot");
        let mut logged_object = object(version("version_1"));
        logged_object.object_id =
            DfsObjectId::from_legacy_named("obj_before_table_log_shrink").expect("object");
        logged_object.path = "/docs/before-table-log-shrink.md".to_string();
        writer.put_object(&logged_object).expect("logged object");

        let mut stale = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("stale backend");
        let stale_offset = stale.table_log_offset;
        assert!(
            stale_offset > 0,
            "stale handle needs an old table-log cursor"
        );

        for index in 0..8 {
            let mut object = object(version("version_1"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_pre_compact_{index}")).expect("object");
            object.path = format!("/docs/pre-compact-{index}.md");
            writer.put_object(&object).expect("pre compact object");
        }
        writer
            .compact_metadata_table_log_if_needed(1)
            .expect("force compaction");

        let mut index = 0usize;
        while fs::metadata(metadata_table_path(&root))
            .map(|metadata| metadata.len())
            .unwrap_or(0)
            <= stale_offset
        {
            let mut object = object(version("version_1"));
            object.object_id = DfsObjectId::from_legacy_named(format!("obj_post_compact_{index}"))
                .expect("object");
            object.path = format!("/docs/post-compact-{index}.md");
            writer.put_object(&object).expect("post compact object");
            index += 1;
        }

        let mut object = object(version("version_1"));
        object.object_id =
            DfsObjectId::from_legacy_named("obj_stale_handle_after_compact").expect("object");
        object.path = "/docs/stale-handle-after-compact.md".to_string();
        stale
            .put_object(&object)
            .expect("stale handle should refresh from the new snapshot before writing");
        assert_eq!(
            stale
                .get_object(&object.object_id)
                .expect("object lookup after stale write"),
            Some(object)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reloads_when_table_log_shrinks_below_cached_offset() {
        let root = temp_root("local_backend_reloads_after_external_table_log_truncation");
        let mut writer =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("writer backend");
        let file_system = file_system();
        writer.put_file_system(&file_system).expect("filesystem");

        let mut stale = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("stale backend");
        let stale_offset = stale.table_log_offset;
        assert!(
            stale_offset > 1,
            "stale handle needs a cached table-log cursor"
        );

        let table_path = metadata_table_path(&root);
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&table_path)
            .expect("open table log");
        file.set_len(stale_offset - 1)
            .and_then(|()| file.sync_all())
            .expect("shrink table log below cached cursor");

        stale
            .current_tables_snapshot()
            .expect("lock-free read should reload instead of seeking past EOF");
        stale
            .put_file_system(&file_system)
            .expect("stale writer should reload after table log shrink");
        assert_eq!(
            stale
                .get_file_system(&file_system.file_system_id)
                .expect("filesystem lookup after stale write"),
            Some(file_system)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_incrementally_advances_the_external_read_snapshot_cache() {
        let root = temp_root("local_backend_advances_external_read_snapshot_cache");
        let mut writer =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("writer backend");
        writer.put_file_system(&file_system()).expect("filesystem");
        let reader = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reader backend");

        let mut first = object(version("version_1"));
        first.object_id =
            DfsObjectId::from_legacy_named("obj_cached_snapshot_first").expect("object id");
        first.path = "/docs/cached-snapshot-first.md".to_string();
        writer.put_object(&first).expect("first object");
        assert_eq!(
            reader.get_object(&first.object_id).expect("first read"),
            Some(first)
        );
        let (first_tables, first_offset) = {
            let cache = reader.read_snapshot_cache.lock().expect("snapshot cache");
            let cache = cache.as_ref().expect("populated snapshot cache");
            (Arc::as_ptr(&cache.tables), cache.table_log_offset)
        };

        let mut second = object(version("version_1"));
        second.object_id =
            DfsObjectId::from_legacy_named("obj_cached_snapshot_second").expect("object id");
        second.path = "/docs/cached-snapshot-second.md".to_string();
        writer.put_object(&second).expect("second object");
        assert_eq!(
            reader.get_object(&second.object_id).expect("second read"),
            Some(second)
        );
        let (second_tables, second_offset) = {
            let cache = reader.read_snapshot_cache.lock().expect("snapshot cache");
            let cache = cache.as_ref().expect("advanced snapshot cache");
            (Arc::as_ptr(&cache.tables), cache.table_log_offset)
        };

        assert_eq!(
            first_tables, second_tables,
            "sequential external reads should reuse the cached table allocation"
        );
        assert!(second_offset > first_offset);

        cleanup(&root);
    }

    #[test]
    fn local_backend_serializes_concurrent_metadata_writers() {
        let root = temp_root("local_backend_serializes_concurrent_metadata_writers");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        drop(backend);

        let writer_count = 8usize;
        let writes_per_writer = 16usize;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(writer_count));
        let mut handles = Vec::new();
        for writer_index in 0..writer_count {
            let root = root.to_path_buf();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                let mut backend =
                    LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                        .expect("backend");
                barrier.wait();
                for object_index in 0..writes_per_writer {
                    let mut object = object(version("version_1"));
                    object.object_id = DfsObjectId::from_legacy_named(format!(
                        "obj_concurrent_{writer_index}_{object_index}"
                    ))
                    .expect("object");
                    object.path = format!("/docs/concurrent-{writer_index}-{object_index}.md");
                    backend.put_object(&object).expect("object");
                }
            }));
        }
        for handle in handles {
            handle.join().expect("writer thread");
        }

        let backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        let journal = backend.journal_since(0).expect("journal");
        assert_eq!(journal.len(), 1 + writer_count * writes_per_writer);
        let sequences = journal
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>();
        assert_eq!(
            sequences,
            (1..=journal.len() as u64).collect::<Vec<_>>(),
            "concurrent metadata writers must allocate contiguous journal sequences"
        );
        assert_eq!(
            backend
                .objects_for_file_system(&file_system_id())
                .expect("objects")
                .len(),
            writer_count * writes_per_writer,
            "concurrent materialization must not lose object indexes"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_stale_open_writer_refreshes_next_sequence_from_table_log() {
        let root = temp_root("local_backend_stale_open_writer_refreshes_next_sequence");
        let mut stale_writer =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("stale writer");
        stale_writer
            .put_file_system(&file_system())
            .expect("filesystem");

        let mut fresh_writer =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("fresh writer");
        let mut fresh_object = object(version("version_1"));
        fresh_object.object_id =
            DfsObjectId::from_legacy_named("obj_fresh_writer_sequence").expect("object");
        fresh_object.path = "/docs/fresh-writer-sequence.md".to_string();
        fresh_writer
            .put_object(&fresh_object)
            .expect("fresh writer object");

        let mut stale_object = object(version("version_1"));
        stale_object.object_id =
            DfsObjectId::from_legacy_named("obj_stale_writer_sequence").expect("object");
        stale_object.path = "/docs/stale-writer-sequence.md".to_string();
        let stale_entry = stale_writer
            .put_object(&stale_object)
            .expect("stale writer object");
        assert_eq!(
            stale_entry.sequence, 3,
            "a stale open handle must advance from materialized table state before writing"
        );

        let sequences = stale_writer
            .journal_since(0)
            .expect("journal")
            .into_iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>();
        assert_eq!(sequences, vec![1, 2, 3]);
        assert_eq!(
            stale_writer
                .get_object(&fresh_object.object_id)
                .expect("fresh object"),
            Some(fresh_object)
        );
        assert_eq!(
            stale_writer
                .get_object(&stale_object.object_id)
                .expect("stale object"),
            Some(stale_object)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_readers_never_observe_torn_metadata_during_writes() {
        let root = temp_root("local_backend_readers_during_writes");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        drop(backend);

        let total_writes = 256usize;
        let reader_count = 4usize;
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        // Start readers and the writer together so the reads genuinely overlap the
        // appends rather than racing thread startup.
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(reader_count + 1));

        // Readers use their own backend handle and read continuously while a writer
        // appends. The read path (`current_tables_snapshot`) is lock-free, so this
        // exercises its torn-tail safety: every read must succeed and never observe
        // more objects than the writer has committed.
        let mut readers = Vec::new();
        for _ in 0..reader_count {
            let root = root.to_path_buf();
            let stop = stop.clone();
            let barrier = barrier.clone();
            readers.push(std::thread::spawn(move || {
                let backend =
                    LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                        .expect("reader backend");
                barrier.wait();
                // Read at least once, then keep reading until the writer signals stop.
                let mut reads = 0u64;
                loop {
                    let objects = backend
                        .objects_for_file_system(&file_system_id())
                        .expect("read must never fail on a torn tail");
                    assert!(
                        objects.len() <= total_writes,
                        "reader saw {} objects, more than the {total_writes} ever written",
                        objects.len()
                    );
                    reads += 1;
                    if stop.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                }
                reads
            }));
        }

        let mut writer =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("writer backend");
        barrier.wait();
        for i in 0..total_writes {
            let mut object = object(version("version_1"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_reader_race_{i}")).expect("object id");
            object.path = format!("/docs/reader-race-{i}.md");
            writer.put_object(&object).expect("write object");
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);

        for reader in readers {
            let reads = reader.join().expect("reader thread");
            assert!(reads > 0, "reader must have completed at least one read");
        }

        let backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(
            backend
                .objects_for_file_system(&file_system_id())
                .expect("final objects")
                .len(),
            total_writes,
            "every committed write must be visible after the race"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_appends_journal_entries_to_segment_files() {
        let root = temp_root("local_backend_appends_journal_entries_to_segment_files");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let object = object(version("version_1"));

        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");

        let head = read_sobj::<DfsMetadataJournalHead>(&root.join("journal").join("head.sobj"))
            .expect("journal head");
        assert_eq!(
            head.next_sequence, 1,
            "local appends should leave the journal head as a recovery cache instead of rewriting it on every mutation"
        );
        assert!(
            root.join("journal")
                .join("segments")
                .join("00000000000000000001.slog")
                .is_file(),
            "new journal entries should be appended to a segment file"
        );
        assert!(
            !root
                .join("journal")
                .join("00000000000000000001.sobj")
                .exists(),
            "new journal entries should not create one object file per mutation"
        );
        assert_eq!(backend.journal_since(0).expect("journal").len(), 2);
        assert_eq!(backend.journal_since(1).expect("journal").len(), 1);

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(reopened.journal_since(0).expect("journal").len(), 2);

        cleanup(&root);
    }

    #[test]
    fn local_backend_open_for_inspection_does_not_repair_materialization_cursor() {
        let root = temp_root("local_backend_open_for_inspection_is_read_only");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        let object = object(version("version_1"));
        backend.put_object(&object).expect("object");

        let cursor_path = backend.local_materialization_cursor_path();
        remove_file_if_exists(&cursor_path).expect("remove materialization cursor");
        assert!(
            !cursor_path.exists(),
            "test fixture should remove the recovery cursor before inspection open"
        );
        drop(backend);

        let inspected = LocalSobjMetadataBackend::open_for_inspection(shard_id, &root)
            .expect("inspection open");
        assert!(
            !cursor_path.exists(),
            "inspection open must not repair or recreate recovery cursors"
        );
        let summary = inspected.metadata_summary().expect("summary");
        assert_eq!(summary.object_count, 1);
        assert!(
            inspected
                .metadata_shard_statuses()
                .expect("shard status")
                .first()
                .is_some_and(|status| status.journal_next_sequence >= 1)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_scans_only_the_highest_segment() {
        let root = temp_root("local_backend_reopen_uses_head_and_cursor");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let file_system = file_system();
        let object = object(version("version_1"));
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend
            .compact_metadata_table_log_if_needed(1)
            .expect("compact table snapshot");

        let high_sequence = JOURNAL_SEGMENT_ENTRY_LIMIT + 1;
        append_segmented_journal_entry(
            &root,
            &DfsMetadataJournalEntry {
                shard_id: shard_id.clone(),
                sequence: high_sequence,
                created_at: OffsetDateTime::now_utc(),
                mutation: DfsMetadataMutation::PutFileSystem {
                    file_system_id: file_system.file_system_id.clone(),
                    file_system: Some(file_system.clone()),
                },
            },
        )
        .expect("high segment entry");
        write_sobj_atomic(
            &root.join("journal").join("head.sobj"),
            &DfsMetadataJournalHead {
                next_sequence: high_sequence + 1,
            },
        )
        .expect("journal head");
        write_sobj_atomic(
            &backend.local_materialization_cursor_path(),
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: high_sequence,
            },
        )
        .expect("local cursor");
        // The bounded reopen scan reads only the highest segment. Do not ask a
        // prefix at 2 to skip unproven history up to the synthetic sequence.
        backend.materialized_through_sequence = high_sequence;
        backend.prefix_proof = Some(
            backend
                .metadata_prefix_checkpoint_for_shard(&shard_id, high_sequence)
                .expect("high-sequence installed checkpoint"),
        );
        backend
            .persist_current_metadata_checkpoint_unlocked()
            .expect("persist high-sequence installed checkpoint");
        fs::write(
            root.join("journal")
                .join("segments")
                .join("00000000000000000001.slog"),
            [1_u8, 0, 0, 0, 0xff],
        )
        .expect("corrupt consumed segment");
        drop(backend);

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root)
            .expect("reopen should not scan consumed journal segments");
        assert_eq!(
            reopened.get_object(&object.object_id).expect("object"),
            Some(object)
        );
        assert!(
            reopened
                .journal_since(high_sequence)
                .expect("journal after head")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_tolerates_torn_fully_materialized_current_segment() {
        let root = temp_root("local_backend_reopen_with_corrupt_current_segment");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let file_system = file_system();
        let object = object(version("version_1"));
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend
            .compact_metadata_table_log_if_needed(1)
            .expect("compact table snapshot");
        write_sobj_atomic(
            &root.join("journal").join("head.sobj"),
            &DfsMetadataJournalHead { next_sequence: 3 },
        )
        .expect("journal head");
        write_sobj_atomic(
            &backend.local_materialization_cursor_path(),
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: 2,
            },
        )
        .expect("local cursor");
        fs::write(
            root.join("journal")
                .join("segments")
                .join("00000000000000000001.slog"),
            [1_u8, 0, 0, 0, 0xff],
        )
        .expect("corrupt consumed current segment");
        drop(backend);

        // The reopen scan still examines the current segment. The segmented
        // reader may truncate a torn consumed tail because the exact table
        // checkpoint already proves the materialized prefix through two.
        let reopened = LocalSobjMetadataBackend::new(shard_id, &root)
            .expect("reopen from the exact materialized checkpoint");
        assert_eq!(
            reopened.get_object(&object.object_id).expect("object"),
            Some(object)
        );
        assert!(
            reopened
                .journal_since(2)
                .expect("journal after materialized prefix")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_repairs_matching_stale_head_and_cursor() {
        let root = temp_root("local_backend_reopen_repairs_matching_stale_hints");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let file_system = file_system();
        let first_object = object(version("version_1"));
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        assert_eq!(
            backend
                .put_file_system(&file_system)
                .expect("filesystem")
                .sequence,
            1
        );
        assert_eq!(
            backend.put_object(&first_object).expect("object").sequence,
            2
        );

        // Reproduce the Docker restart cut: both deferred hints agree at one
        // while the segment and exact materialized prefix are already at two.
        write_sobj_atomic(
            &root.join("journal").join("head.sobj"),
            &DfsMetadataJournalHead { next_sequence: 2 },
        )
        .expect("stale journal head");
        write_sobj_atomic(
            &backend.local_materialization_cursor_path(),
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: 1,
            },
        )
        .expect("stale local cursor");
        drop(backend);

        let mut reopened = LocalSobjMetadataBackend::new(shard_id.clone(), &root)
            .expect("reopen repairs the stale hints from the durable segment");
        assert_eq!(reopened.journal_last_sequence(), 2);
        assert_eq!(
            reopened
                .metadata_prefix_proof()
                .expect("exact repaired prefix")
                .journal_sequence,
            2
        );
        let mut successor = object(version("version_2"));
        successor.object_id =
            DfsObjectId::from_legacy_named("obj_after_stale_head_repair").expect("object id");
        successor.path = "/docs/after-stale-head-repair.md".to_string();
        assert_eq!(
            reopened
                .put_object(&successor)
                .expect("successor append")
                .sequence,
            3,
            "reopen must not reuse the already durable sequence two"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_repairs_journal_only_tail_behind_matching_hints() {
        let root = temp_root("local_backend_reopen_repairs_journal_only_stale_hints");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let file_system = file_system();
        let first_object = object(version("version_1"));
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        assert_eq!(
            backend
                .put_file_system(&file_system)
                .expect("filesystem")
                .sequence,
            1
        );
        write_sobj_atomic(
            &root.join("journal").join("head.sobj"),
            &DfsMetadataJournalHead { next_sequence: 2 },
        )
        .expect("journal head");
        write_sobj_atomic(
            &backend.local_materialization_cursor_path(),
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: 1,
            },
        )
        .expect("local cursor");
        append_segmented_journal_entry(
            &root,
            &DfsMetadataJournalEntry {
                shard_id: shard_id.clone(),
                sequence: 2,
                created_at: OffsetDateTime::now_utc(),
                mutation: DfsMetadataMutation::put_object(&first_object),
            },
        )
        .expect("durable journal-only tail");
        drop(backend);

        let mut reopened = LocalSobjMetadataBackend::new(shard_id, &root)
            .expect("reopen discovers and materializes the journal-only tail");
        assert_eq!(
            reopened
                .get_object(&first_object.object_id)
                .expect("object"),
            Some(first_object.clone())
        );
        assert_eq!(reopened.journal_last_sequence(), 2);
        let mut successor = object(version("version_2"));
        successor.object_id =
            DfsObjectId::from_legacy_named("obj_after_journal_tail_repair").expect("object id");
        successor.path = "/docs/after-journal-tail-repair.md".to_string();
        assert_eq!(
            reopened
                .put_object(&successor)
                .expect("successor append")
                .sequence,
            3
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reads_bounded_journal_batches() {
        let root = temp_root("local_backend_reads_bounded_journal_batches");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        for index in 0..5 {
            let mut object = object(version("version_1"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_bounded_journal_{index}"))
                    .expect("object");
            object.path = format!("/docs/bounded-{index}.md");
            backend.put_object(&object).expect("object");
        }

        let first_batch = backend
            .journal_since_limited(0, 3)
            .expect("first bounded batch");
        assert_eq!(
            first_batch
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let second_batch = backend
            .journal_since_limited(3, 3)
            .expect("second bounded batch");
        assert_eq!(
            second_batch
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![4, 5, 6]
        );
        assert!(
            backend
                .journal_since_limited(6, 3)
                .expect("empty bounded batch")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_journal_reader_ignores_invalid_tail_record() {
        let root = temp_root("local_backend_journal_reader_ignores_invalid_tail_record");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend
            .put_object(&object(version("version_1")))
            .expect("object");
        let expected = backend.journal_since(0).expect("journal before tail");

        let segment_path = root
            .join("journal")
            .join("segments")
            .join("00000000000000000001.slog");
        let invalid_payload = b"not-sobj";
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&segment_path)
            .expect("open segment");
        file.write_all(&(invalid_payload.len() as u32).to_le_bytes())
            .and_then(|()| file.write_all(invalid_payload))
            .expect("invalid tail append");
        drop(file);

        assert_eq!(
            backend
                .journal_since(0)
                .expect("journal after invalid tail"),
            expected,
            "journal polling must ignore a malformed trailing record and retry it later"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_journal_reader_waits_for_writer_lock() {
        let root = temp_root("local_backend_journal_reader_waits_for_writer_lock");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend
            .put_object(&object(version("version_1")))
            .expect("object");
        let expected = backend.journal_since(0).expect("journal before tail");
        let segment_path = root
            .join("journal")
            .join("segments")
            .join("00000000000000000001.slog");
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let root_for_writer = root.to_path_buf();
        let segment_for_writer = segment_path.clone();
        let writer = std::thread::spawn(move || {
            let writer_backend = LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("shard-root"),
                &root_for_writer,
            )
            .expect("writer backend");
            let _lock = writer_backend.metadata_write_lock().expect("writer lock");
            locked_tx.send(()).expect("signal lock acquired");
            std::thread::sleep(Duration::from_millis(100));
            let invalid_payload = b"not-sobj";
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&segment_for_writer)
                .expect("open segment");
            file.write_all(&(invalid_payload.len() as u32).to_le_bytes())
                .and_then(|()| file.write_all(invalid_payload))
                .expect("invalid tail append");
        });
        locked_rx.recv().expect("writer acquired lock");

        let started = std::time::Instant::now();
        assert_eq!(
            backend
                .journal_since(0)
                .expect("journal reader waits for lock"),
            expected,
            "journal polling must not read while a writer is appending a record"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "journal reader should wait for the in-flight metadata writer"
        );
        writer.join().expect("writer thread");
        cleanup(&root);
    }

    #[test]
    fn local_backend_open_waits_for_writer_lock_before_table_replay() {
        let root = temp_root("local_backend_open_waits_for_writer_lock");
        let mut seed = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("seed backend");
        seed.put_file_system(&file_system()).expect("filesystem");
        drop(seed);

        let mut object = object(version("version_1"));
        object.object_id =
            DfsObjectId::from_legacy_named("obj_open_waits_for_writer_lock").expect("object");
        object.path = "/docs/open-waits-for-writer-lock.md".to_string();
        let object_id = object.object_id.clone();
        let expected_object = object.clone();
        let root_for_writer = root.to_path_buf();
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let mut writer_backend = LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("shard-root"),
                root_for_writer,
            )
            .expect("writer backend");
            let _lock = writer_backend.metadata_write_lock().expect("writer lock");
            locked_tx.send(()).expect("signal lock acquired");
            std::thread::sleep(Duration::from_millis(100));
            let entry = writer_backend
                .write_journal_unlocked(DfsMetadataMutation::put_object(&object))
                .expect("append object journal entry while lock is held");
            writer_backend
                .append_and_apply_table_ops(
                    entry.sequence,
                    vec![DfsMetadataTableOp::PutObject { object }],
                )
                .expect("append table batch while lock is held");
        });
        locked_rx.recv().expect("writer acquired lock");

        let started = std::time::Instant::now();
        let opened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("opener backend");
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "backend open must wait for in-flight metadata writers before table replay"
        );
        assert_eq!(
            opened
                .get_object(&object_id)
                .expect("object lookup")
                .as_ref(),
            Some(&expected_object),
            "backend open must replay the complete table batch after the writer releases the lock"
        );

        writer.join().expect("writer thread");
        cleanup(&root);
    }

    #[test]
    fn local_backend_reads_byte_bounded_journal_batches() {
        let root = temp_root("local_backend_reads_byte_bounded_journal_batches");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        for index in 0..3 {
            let mut object = object(version("version_1"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_byte_bounded_journal_{index}"))
                    .expect("object");
            object.path = format!("/docs/{}/bounded-{index}.md", "wide".repeat(128));
            backend.put_object(&object).expect("object");
        }

        let first_unbounded = backend
            .journal_batch_since_limited(1, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, usize::MAX)
            .expect("unbounded batch")
            .expect("batch");
        let first_entry = first_unbounded.entries[0].clone();
        let one_entry_limit = metadata_journal_batch_encoded_len(&DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            next_after_sequence: first_entry.sequence,
            entries: vec![first_entry],
        })
        .expect("encoded length");

        let first_batch = backend
            .journal_batch_since_limited(
                1,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                one_entry_limit,
            )
            .expect("byte bounded batch")
            .expect("batch");
        assert_eq!(
            first_batch
                .entries
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(first_batch.next_after_sequence, 2);
        assert!(
            metadata_journal_batch_encoded_len(&first_batch).expect("encoded length")
                <= one_entry_limit
        );

        let oversized_first = backend
            .journal_batch_since_limited(1, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, 1)
            .expect("oversized first batch")
            .expect("batch");
        assert_eq!(oversized_first.entries.len(), 1);
        assert_eq!(oversized_first.next_after_sequence, 2);

        cleanup(&root);
    }

    #[test]
    fn local_backend_migrates_legacy_journal_files_to_segments() {
        let root = temp_root("local_backend_migrates_legacy_journal_files_to_segments");
        fs::create_dir_all(root.join("journal")).expect("journal dir");
        let legacy_path = root.join("journal").join("00000000000000000001.sobj");
        let legacy_entry = DfsMetadataJournalEntry {
            shard_id: DfsMetadataShardId::new("shard-root"),
            sequence: 1,
            created_at: OffsetDateTime::UNIX_EPOCH,
            mutation: DfsMetadataMutation::PutFileSystem {
                file_system_id: file_system_id(),
                file_system: Some(file_system()),
            },
        };
        write_sobj_atomic(&legacy_path, &legacy_entry).expect("legacy journal entry");

        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        assert!(
            !legacy_path.exists(),
            "opening the backend should migrate legacy root journal files out of the live path"
        );
        assert!(
            root.join("journal")
                .join("segments")
                .join("00000000000000000001.slog")
                .is_file(),
            "legacy journal entries should be appended to the segmented journal"
        );
        assert_eq!(
            backend.journal_since(0).expect("journal"),
            vec![legacy_entry]
        );
        let next = backend.put_file_system(&file_system()).expect("filesystem");
        assert_eq!(
            next.sequence, 2,
            "sequence allocation should continue after migrated legacy entries"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_hydrates_legacy_journal_payloads_from_records() {
        let root = temp_root("local_backend_hydrates_legacy_journal_payloads_from_records");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend
            .put_object(&object(version("version_1")))
            .expect("object");
        backend
            .put_version(&DfsObjectVersionRecord {
                file_system_id: file_system_id(),
                object_id: object_id(),
                version: version("version_1"),
                content_hash: Some("blake3:old".to_string()),
                size_bytes: None,
                locations: vec![node("node_owner")],
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: true,
            })
            .expect("version");
        let mut committed_object = object(version("version_2"));
        committed_object.content_hash = Some("blake3:new".to_string());
        committed_object.content_size_bytes = Some(128);
        let visible_version = DfsObjectVersionRecord {
            file_system_id: file_system_id(),
            object_id: object_id(),
            version: version("version_2"),
            content_hash: committed_object.content_hash.clone(),
            size_bytes: committed_object.content_size_bytes,
            locations: vec![node("node_owner")],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        backend
            .commit_visible_version(
                &version("version_1"),
                &committed_object,
                &visible_version,
                &[],
            )
            .expect("commit visible version");
        let mut transaction_object = object(version("version_namespace_tx"));
        transaction_object.object_id =
            DfsObjectId::from_legacy_named("obj_hydrated_namespace_tx").expect("object");
        transaction_object.path = "docs/transaction.md".to_string();
        transaction_object.content_hash = None;
        transaction_object.content_size_bytes = None;
        let transaction = DfsNamespaceTransaction {
            transaction_id: "namespace_tx_hydrate_legacy".to_string(),
            file_system_id: file_system_id(),
            objects: vec![transaction_object],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend
            .apply_namespace_transaction(&transaction)
            .expect("namespace transaction");

        let legacy_entries = backend
            .journal_since(0)
            .expect("journal")
            .into_iter()
            .map(|mut entry| {
                match &mut entry.mutation {
                    DfsMetadataMutation::PutFileSystem { file_system, .. } => {
                        *file_system = None;
                    }
                    DfsMetadataMutation::PutObject { object, .. } => {
                        *object = None;
                    }
                    DfsMetadataMutation::PutVersion { version_record, .. } => {
                        *version_record = None;
                    }
                    DfsMetadataMutation::CommitVisibleVersion {
                        committed_object,
                        visible_version,
                        ..
                    } => {
                        *committed_object = None;
                        *visible_version = None;
                    }
                    DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } => {
                        *transaction = None;
                    }
                    _ => {}
                }
                entry
            })
            .collect::<Vec<_>>();

        let hydrated = backend
            .hydrate_journal_entry_payloads(legacy_entries)
            .expect("hydrate legacy entries");
        assert!(matches!(
            &hydrated[0].mutation,
            DfsMetadataMutation::PutFileSystem {
                file_system: Some(_),
                ..
            }
        ));
        assert!(matches!(
            &hydrated[1].mutation,
            DfsMetadataMutation::PutObject {
                object: Some(_),
                ..
            }
        ));
        assert!(matches!(
            &hydrated[2].mutation,
            DfsMetadataMutation::PutVersion {
                version_record: Some(_),
                ..
            }
        ));
        assert!(hydrated.iter().any(|entry| matches!(
            &entry.mutation,
            DfsMetadataMutation::CommitVisibleVersion {
                committed_object: Some(object),
                visible_version: Some(version_record),
                ..
            } if object.version == version("version_2")
                && version_record.version == version("version_2")
        )));
        assert!(hydrated.iter().any(|entry| matches!(
            &entry.mutation,
            DfsMetadataMutation::ApplyNamespaceTransaction {
                transaction: Some(transaction),
                ..
            } if transaction.transaction_id == "namespace_tx_hydrate_legacy"
        )));

        cleanup(&root);
    }

    #[test]
    fn local_backend_syncs_metadata_directories() {
        let root = temp_root("local_backend_syncs_metadata_directories");
        fs::create_dir_all(&root).expect("root dir");

        sync_dir(&root).expect("directory sync should work on supported host filesystems");

        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_recovers_next_sequence_from_segment_tail() {
        let root = temp_root("local_backend_reopen_recovers_next_sequence_from_segment_tail");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        append_segmented_journal_entry(
            &root,
            &DfsMetadataJournalEntry {
                shard_id: DfsMetadataShardId::new("shard-root"),
                sequence: 2,
                created_at: OffsetDateTime::UNIX_EPOCH,
                mutation: DfsMetadataMutation::PutObject {
                    file_system_id: file_system_id(),
                    object_id: object_id(),
                    path: "docs/readme.md".to_string(),
                    object: None,
                },
            },
        )
        .expect("append segment entry without advancing head");

        let mut reopened =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("reopen");
        let mut object = object(version("version_1"));
        object.object_id =
            DfsObjectId::from_legacy_named("obj_after_tail_recovery").expect("object");
        object.path = "docs/after-tail-recovery.md".to_string();
        let entry = reopened.put_object(&object).expect("object");
        assert_eq!(
            entry.sequence, 3,
            "reopen must scan segment tails so a crash after append but before head update cannot reuse a sequence"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_lists_directory_children_from_dentry_index() {
        let root = temp_root("local_backend_lists_directory_children_from_dentry_index");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut root_dir = object(version("version_1"));
        root_dir.object_id = DfsObjectId::from_legacy_named("obj_root_dir").expect("object");
        root_dir.path = "/".to_string();
        root_dir.kind = DfsObjectKind::Directory;
        root_dir.content_hash = None;
        root_dir.content_size_bytes = None;
        let mut docs_dir = object(version("version_1"));
        docs_dir.object_id = DfsObjectId::from_legacy_named("obj_docs_dir").expect("object");
        docs_dir.path = "/docs".to_string();
        docs_dir.kind = DfsObjectKind::Directory;
        docs_dir.content_hash = None;
        docs_dir.content_size_bytes = None;
        let mut readme = object(version("version_1"));
        readme.path = "/docs/readme.md".to_string();
        let mut changelog = object(version("version_1"));
        changelog.object_id = DfsObjectId::from_legacy_named("obj_changelog").expect("object");
        changelog.path = "/docs/changelog.md".to_string();
        let mut nested = object(version("version_1"));
        nested.object_id = DfsObjectId::from_legacy_named("obj_setup").expect("object");
        nested.path = "/docs/guides/setup.md".to_string();
        let mut outside = object(version("version_1"));
        outside.object_id = DfsObjectId::from_legacy_named("obj_lib").expect("object");
        outside.path = "/src/lib.rs".to_string();
        for object in [&root_dir, &docs_dir, &readme, &changelog, &nested, &outside] {
            backend.put_object(object).expect("object");
        }
        let mut changelog_tombstone = changelog.clone();
        changelog_tombstone.kind = DfsObjectKind::Tombstone;
        changelog_tombstone.version = version("version_2");
        changelog_tombstone.content_hash = None;
        backend.put_object(&changelog_tombstone).expect("tombstone");

        let children = backend
            .child_objects_for_directory(&file_system_id(), "/docs")
            .expect("children");
        assert_eq!(
            children
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.md"]
        );

        let root_children = backend
            .child_objects_for_directory(&file_system_id(), "/")
            .expect("root children");
        assert_eq!(
            root_children
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs"]
        );

        let root_page = backend
            .child_objects_for_directory_page(&file_system_id(), "/", None, 10)
            .expect("root page");
        assert_eq!(
            root_page
                .objects
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs"]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_routes_new_large_directory_entries_to_hash_fragments() {
        let root = temp_root("local_backend_routes_new_large_directory_entries_to_hash_fragments");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut existing = object(version("version_1"));
        existing.path = "/docs/existing.md".to_string();
        backend.put_object(&existing).expect("existing object");
        let existing_record = table_path_record(&backend, &existing.file_system_id, &existing.path);
        assert_eq!(existing_record.dirfrag_id, DfsDirFragmentId::new("root"));

        let mut root_dirfrag = table_dirfrag(
            &backend,
            &existing_record.parent_inode_id,
            &DfsDirFragmentId::new("root"),
        );
        root_dirfrag.entry_count = root_dirfrag.split_after_count;
        // Test-only threshold setup is not a metadata mutation and therefore
        // must not invent a journal sequence without a matching prefix proof.
        backend.tables.apply(DfsMetadataTableOp::PutDirFragment {
            dirfrag: root_dirfrag,
        });

        let mut bucketed = object(version("version_1"));
        bucketed.object_id = DfsObjectId::from_legacy_named("obj_bucketed").expect("object");
        bucketed.path = "/docs/bucketed.md".to_string();
        backend.put_object(&bucketed).expect("bucketed object");

        let bucketed_record = table_path_record(&backend, &bucketed.file_system_id, &bucketed.path);
        assert_ne!(
            bucketed_record.dirfrag_id,
            DfsDirFragmentId::new("root"),
            "large directories should route new entries away from the root dentry fragment"
        );
        assert!(
            table_dentry_exists(&backend, &bucketed_record),
            "bucketed dentry record should be materialized in its selected fragment"
        );

        let children = backend
            .child_objects_for_directory(&file_system_id(), "/docs")
            .expect("children");
        assert_eq!(
            children
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/bucketed.md", "/docs/existing.md"],
            "directory listing must merge root and bucket fragments"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_pages_filesystem_objects_and_directory_children() {
        let root = temp_root("local_backend_pages_objects_and_children");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut dir = object(version("version_1"));
        dir.object_id = DfsObjectId::from_legacy_named("obj_page_dir").expect("dir object");
        dir.path = "/paged".to_string();
        dir.kind = DfsObjectKind::Directory;
        dir.content_hash = None;
        dir.content_size_bytes = None;
        backend.put_object(&dir).expect("dir");

        for index in 0..5 {
            let mut file = object(version("version_1"));
            file.object_id =
                DfsObjectId::from_legacy_named(format!("obj_page_file_{index}")).expect("object");
            file.path = format!("/paged/file-{index}.txt");
            file.kind = DfsObjectKind::File;
            backend.put_object(&file).expect("file");
        }

        let first_object_page = backend
            .objects_for_file_system_page(&file_system_id(), None, 3)
            .expect("first object page");
        assert_eq!(first_object_page.objects.len(), 3);
        assert!(first_object_page.next_cursor.is_some());
        let second_object_page = backend
            .objects_for_file_system_page(
                &file_system_id(),
                first_object_page.next_cursor.as_ref(),
                3,
            )
            .expect("second object page");
        assert_eq!(second_object_page.objects.len(), 3);
        assert!(second_object_page.next_cursor.is_none());
        assert_eq!(
            backend
                .count_objects_for_file_system(&file_system_id())
                .expect("count objects"),
            6
        );

        let mut cursor = None;
        let mut child_names = Vec::new();
        let mut pages = 0usize;
        loop {
            let page = backend
                .child_objects_for_directory_page(&file_system_id(), "/paged", cursor.as_ref(), 2)
                .expect("child page");
            assert!(page.objects.len() <= 2);
            child_names.extend(
                page.objects
                    .iter()
                    .map(|object| object.path.rsplit('/').next().unwrap().to_string()),
            );
            pages = pages.saturating_add(1);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        child_names.sort();
        assert_eq!(
            child_names,
            vec![
                "file-0.txt",
                "file-1.txt",
                "file-2.txt",
                "file-3.txt",
                "file-4.txt"
            ]
        );
        assert_eq!(pages, 3);

        cleanup(&root);
    }

    #[test]
    fn local_backend_pages_namespace_jobs() {
        let root = temp_root("local_backend_pages_namespace_jobs");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        for index in 0..5 {
            backend
                .put_namespace_job(&namespace_job(index, &format!("/jobs/{index}")))
                .expect("namespace job");
        }

        let empty = backend
            .list_namespace_jobs_page(None, 0)
            .expect("empty page");
        assert!(empty.jobs.is_empty());
        assert!(empty.next_cursor.is_none());

        let mut cursor = None;
        let mut job_ids = Vec::new();
        let mut pages = 0usize;
        loop {
            let page = backend
                .list_namespace_jobs_page(cursor.as_ref(), 2)
                .expect("job page");
            assert!(page.jobs.len() <= 2);
            job_ids.extend(page.jobs.iter().map(|job| job.job_id.clone()));
            pages = pages.saturating_add(1);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        assert_eq!(
            job_ids,
            (0..5)
                .map(|index| format!("job_namespace_page_{index:02}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(pages, 3);
        assert_eq!(backend.list_namespace_jobs().expect("legacy list").len(), 5);

        cleanup(&root);
    }

    #[test]
    fn local_backend_pages_only_pending_namespace_requests_and_rebuilds_index() {
        let root = temp_root("local_backend_pages_pending_namespace_requests");
        let shard_id = DfsMetadataShardId::new("root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        let pending_later = namespace_request(2, DfsNamespaceRequestStatus::Pending);
        let pending_earlier = namespace_request(1, DfsNamespaceRequestStatus::Pending);
        let completed = namespace_request(0, DfsNamespaceRequestStatus::Completed);
        for request in [&pending_later, &completed, &pending_earlier] {
            backend
                .put_namespace_request_on_shard(&shard_id, request)
                .expect("namespace request");
        }

        let first = backend
            .list_pending_namespace_requests_page(None, 1)
            .expect("first pending request page");
        assert_eq!(
            first
                .requests
                .iter()
                .map(|request| request.request_id.as_str())
                .collect::<Vec<_>>(),
            vec![pending_earlier.request_id.as_str()]
        );
        let second = backend
            .list_pending_namespace_requests_page(first.next_cursor.as_ref(), 1)
            .expect("second pending request page");
        assert_eq!(
            second
                .requests
                .iter()
                .map(|request| request.request_id.as_str())
                .collect::<Vec<_>>(),
            vec![pending_later.request_id.as_str()]
        );
        assert!(second.next_cursor.is_none());

        let mut retried = pending_earlier.clone();
        retried.recovery_attempts = 1;
        retried.updated_at = OffsetDateTime::from_unix_timestamp(3).expect("retry timestamp");
        backend
            .put_namespace_request_on_shard(&shard_id, &retried)
            .expect("retry diagnostics");
        let reordered = backend
            .list_pending_namespace_requests_page(None, 8)
            .expect("reordered pending requests");
        assert_eq!(
            reordered
                .requests
                .iter()
                .map(|request| request.request_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                pending_later.request_id.as_str(),
                pending_earlier.request_id.as_str()
            ],
            "updating recovery diagnostics must replace the old index key"
        );

        let mut completed_later = pending_later.clone();
        completed_later.status = DfsNamespaceRequestStatus::Completed;
        completed_later.result = Some(DfsNamespaceResult::Applied);
        completed_later.updated_at =
            OffsetDateTime::from_unix_timestamp(4).expect("completion timestamp");
        backend
            .put_namespace_request_on_shard(&shard_id, &completed_later)
            .expect("completed request");
        drop(backend);

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen backend");
        let pending = reopened
            .list_pending_namespace_requests_page(None, 8)
            .expect("pending requests after reopen");
        assert_eq!(pending.requests, vec![retried]);
        assert!(pending.next_cursor.is_none());
        assert_eq!(
            reopened
                .list_namespace_requests()
                .expect("retained idempotency requests")
                .len(),
            3,
            "terminal requests remain available for exact retry replay"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_pages_object_versions_receipts_and_intents() {
        let root = temp_root("local_backend_pages_object_local_records");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let file = object(version("version_0"));
        backend.put_object(&file).expect("object");

        for index in 0..5 {
            let version_record = DfsObjectVersionRecord {
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                version: version(&format!("version_{index}")),
                content_hash: Some(format!("blake3:version-{index}")),
                size_bytes: Some(index),
                locations: vec![node("node_owner")],
                created_by_event_id: None,
                created_at: OffsetDateTime::now_utc(),
                visible: index == 0,
            };
            backend.put_version(&version_record).expect("version");
        }

        let mut version_cursor = None;
        let mut paged_versions = Vec::new();
        loop {
            let page = backend
                .versions_for_object_page(&file.object_id, version_cursor.as_ref(), 2)
                .expect("version page");
            assert!(page.versions.len() <= 2);
            paged_versions.extend(page.versions.into_iter().map(|version| version.version));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            version_cursor = Some(next_cursor);
        }
        assert_eq!(
            paged_versions,
            backend
                .versions_for_object(&file.object_id)
                .expect("full versions")
                .into_iter()
                .map(|version| version.version)
                .collect::<Vec<_>>()
        );
        let mut version_scan_cursor = None;
        let mut scanned_versions = Vec::new();
        loop {
            let page = backend
                .version_records_page(version_scan_cursor.as_ref(), 2)
                .expect("version scan page");
            assert!(page.versions.len() <= 2);
            scanned_versions.extend(page.versions.into_iter().map(|version| version.version));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            version_scan_cursor = Some(next_cursor);
        }
        assert_eq!(scanned_versions, paged_versions);

        for index in 0..5 {
            let receipt = DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file.file_system_id.clone(),
                    object_id: file.object_id.clone(),
                    committed_version: version("version_0"),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::generate(),
                },
                replica_node_id: node(&format!("node_replica_{index}")),
                content_hash: Some("blake3:version-0".to_string()),
                received_at: OffsetDateTime::now_utc(),
            };
            backend.put_receipt(&receipt).expect("receipt");
        }

        let mut receipt_cursor = None;
        let mut paged_receipts = Vec::new();
        loop {
            let page = backend
                .receipts_for_version_page(
                    &file.object_id,
                    &version("version_0"),
                    receipt_cursor.as_ref(),
                    2,
                )
                .expect("receipt page");
            assert!(page.receipts.len() <= 2);
            paged_receipts.extend(
                page.receipts
                    .into_iter()
                    .map(|receipt| receipt.replica_node_id),
            );
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            receipt_cursor = Some(next_cursor);
        }
        assert_eq!(
            paged_receipts,
            backend
                .receipts_for_version(&file.object_id, &version("version_0"))
                .expect("full receipts")
                .into_iter()
                .map(|receipt| receipt.replica_node_id)
                .collect::<Vec<_>>()
        );
        let mut receipt_scan_cursor = None;
        let mut scanned_receipts = Vec::new();
        loop {
            let page = backend
                .receipt_records_page(receipt_scan_cursor.as_ref(), 2)
                .expect("receipt scan page");
            assert!(page.receipts.len() <= 2);
            scanned_receipts.extend(
                page.receipts
                    .into_iter()
                    .map(|receipt| receipt.replica_node_id),
            );
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            receipt_scan_cursor = Some(next_cursor);
        }
        assert_eq!(scanned_receipts, paged_receipts);

        for index in 0..5 {
            let intent = DfsWriteIntentRecord {
                intent_id: format!("intent-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                base_version: Some(version("version_0")),
                target_version: version(&format!("version_{}", index + 1)),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner")],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(format!("blake3:intent-{index}")),
                object_metadata: Some(file.clone()),
                touch_parent_on_create: false,
                create_predecessor_object_id: None,
                created_at: OffsetDateTime::now_utc(),
                updated_at: None,
            };
            backend.put_intent(&intent).expect("intent");
        }

        let mut intent_cursor = None;
        let mut paged_intents = Vec::new();
        loop {
            let page = backend
                .intents_for_object_page(&file.object_id, intent_cursor.as_ref(), 2)
                .expect("intent page");
            assert!(page.intents.len() <= 2);
            paged_intents.extend(page.intents.into_iter().map(|intent| intent.intent_id));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            intent_cursor = Some(next_cursor);
        }
        assert_eq!(
            paged_intents,
            (0..5)
                .map(|index| format!("intent-{index}"))
                .collect::<Vec<_>>()
        );

        let mut scan_cursor = None;
        let mut scanned_intents = Vec::new();
        loop {
            let page = backend
                .write_intents_page(scan_cursor.as_ref(), 2)
                .expect("intent scan page");
            assert!(page.intents.len() <= 2);
            scanned_intents.extend(page.intents.into_iter().map(|intent| intent.intent_id));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            scan_cursor = Some(next_cursor);
        }
        assert_eq!(
            scanned_intents,
            (0..5)
                .map(|index| format!("intent-{index}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            backend
                .all_intents()
                .expect("all intents")
                .into_iter()
                .map(|intent| intent.intent_id)
                .collect::<Vec<_>>(),
            scanned_intents
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_pages_intents_by_target_version_after_reopen() {
        let root = temp_root("local_backend_pages_intents_by_target_version");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let file = object(version("version_0"));
        backend.put_object(&file).expect("object");
        for index in 0..6 {
            let target_version = if index % 2 == 0 {
                version("version_even")
            } else {
                version("version_odd")
            };
            let intent = DfsWriteIntentRecord {
                intent_id: format!("intent-target-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                base_version: Some(version("version_0")),
                target_version,
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner")],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(format!("blake3:intent-target-{index}")),
                object_metadata: Some(file.clone()),
                touch_parent_on_create: false,
                create_predecessor_object_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: None,
            };
            backend.put_intent(&intent).expect("intent");
        }

        let first_even_page = backend
            .intents_for_object_version_page(&file.object_id, &version("version_even"), None, 2)
            .expect("first even intent page");
        assert_eq!(
            first_even_page
                .intents
                .iter()
                .map(|intent| intent.intent_id.as_str())
                .collect::<Vec<_>>(),
            vec!["intent-target-0", "intent-target-2"]
        );
        assert!(first_even_page.next_cursor.is_some());
        let second_even_page = backend
            .intents_for_object_version_page(
                &file.object_id,
                &version("version_even"),
                first_even_page.next_cursor.as_ref(),
                2,
            )
            .expect("second even intent page");
        assert_eq!(
            second_even_page
                .intents
                .iter()
                .map(|intent| intent.intent_id.as_str())
                .collect::<Vec<_>>(),
            vec!["intent-target-4"]
        );
        assert!(second_even_page.next_cursor.is_none());

        drop(backend);
        let reopened =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("reopen");
        let mut cursor = None;
        let mut odd_intents = Vec::new();
        loop {
            let page = reopened
                .intents_for_object_version_page(
                    &file.object_id,
                    &version("version_odd"),
                    cursor.as_ref(),
                    1,
                )
                .expect("odd intent page");
            assert!(page.intents.len() <= 1);
            odd_intents.extend(page.intents.into_iter().map(|intent| intent.intent_id));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        assert_eq!(
            odd_intents,
            vec![
                "intent-target-1".to_string(),
                "intent-target-3".to_string(),
                "intent-target-5".to_string()
            ]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_content_update_does_not_rewrite_stable_namespace_indexes() {
        let root = temp_root("local_backend_content_update_skips_stable_indexes");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut readme = object(version("version_1"));
        readme.path = "/docs/readme.md".to_string();
        backend.put_object(&readme).expect("object");
        let path_record = table_path_record(&backend, &readme.file_system_id, &readme.path);
        let filesystem_object_record = backend
            .tables
            .file_system_objects
            .get(&(readme.file_system_id.clone(), readme.object_id.clone()))
            .cloned()
            .expect("filesystem object index");
        let dentry_record = backend
            .tables
            .dentries
            .get(&dentry_key(&path_record))
            .cloned()
            .expect("dentry index");
        let dirfrag = table_dirfrag(
            &backend,
            &path_record.parent_inode_id,
            &path_record.dirfrag_id,
        );
        let object_before = backend
            .tables
            .objects
            .get(&readme.object_id)
            .cloned()
            .expect("object record");
        let inode_before = backend
            .tables
            .inodes
            .get(&path_record.inode_id)
            .cloned()
            .expect("inode record");

        readme.content_hash = Some("blake3:new-content".to_string());
        readme.version = version("version_2");
        backend.put_object(&readme).expect("content update");

        assert_eq!(
            backend
                .tables
                .file_system_objects
                .get(&(readme.file_system_id.clone(), readme.object_id.clone()))
                .cloned(),
            Some(filesystem_object_record),
            "content-only updates should keep the filesystem-object namespace index stable"
        );
        assert_eq!(
            table_path_record(&backend, &readme.file_system_id, &readme.path),
            path_record,
            "content-only updates should keep the path namespace index stable"
        );
        assert_eq!(
            backend
                .tables
                .dentries
                .get(&dentry_key(&path_record))
                .cloned(),
            Some(dentry_record),
            "content-only updates should keep the dentry namespace index stable"
        );
        assert_eq!(
            table_dirfrag(
                &backend,
                &path_record.parent_inode_id,
                &path_record.dirfrag_id
            ),
            dirfrag,
            "content-only updates should not change directory fragment accounting"
        );
        assert_ne!(
            backend.tables.objects.get(&readme.object_id).cloned(),
            Some(object_before),
            "the primary object record should still update when content metadata changes"
        );
        assert_ne!(
            backend.tables.inodes.get(&path_record.inode_id).cloned(),
            Some(inode_before),
            "the inode record should still update when visible object metadata changes"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_removes_stale_path_and_dentry_indexes() {
        let root = temp_root("local_backend_removes_stale_path_and_dentry_indexes");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut readme = object(version("version_1"));
        readme.path = "/docs/readme.md".to_string();
        backend.put_object(&readme).expect("object");
        let old_path_record = path_record_for_object(&readme);
        assert!(table_path_exists(
            &backend,
            &file_system_id(),
            "/docs/readme.md"
        ));
        assert!(table_dentry_exists(&backend, &old_path_record));
        assert_eq!(
            table_dirfrag(
                &backend,
                &old_path_record.parent_inode_id,
                &DfsDirFragmentId::new("root")
            )
            .entry_count,
            1
        );

        readme.path = "/docs/renamed.md".to_string();
        readme.version = version("version_2");
        backend.put_object(&readme).expect("renamed object");
        assert!(
            !table_path_exists(&backend, &file_system_id(), "/docs/readme.md"),
            "renaming should remove the obsolete path index"
        );
        assert!(
            !table_dentry_exists(&backend, &old_path_record),
            "renaming should remove the obsolete directory entry"
        );
        assert!(table_path_exists(
            &backend,
            &file_system_id(),
            "/docs/renamed.md"
        ));
        let renamed_path_record = path_record_for_object(&readme);
        assert!(table_dentry_exists(&backend, &renamed_path_record));
        assert_eq!(
            backend
                .child_objects_for_directory(&file_system_id(), "/docs")
                .expect("children")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/renamed.md"]
        );
        assert_eq!(
            table_dirfrag(
                &backend,
                &old_path_record.parent_inode_id,
                &DfsDirFragmentId::new("root")
            )
            .entry_count,
            1
        );

        readme.kind = DfsObjectKind::Tombstone;
        readme.content_hash = None;
        readme.version = version("version_3");
        backend.put_object(&readme).expect("tombstone object");
        assert!(
            !table_dentry_exists(&backend, &renamed_path_record),
            "tombstoning should remove the visible directory entry"
        );
        assert!(
            table_path_exists(&backend, &file_system_id(), "/docs/renamed.md"),
            "tombstone path record should remain addressable"
        );
        assert!(
            backend
                .child_objects_for_directory(&file_system_id(), "/docs")
                .expect("children after tombstone")
                .is_empty()
        );
        assert_eq!(
            table_dirfrag(
                &backend,
                &old_path_record.parent_inode_id,
                &DfsDirFragmentId::new("root")
            )
            .entry_count,
            0
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_compacts_retention_eligible_tombstones_to_path_markers() {
        let root = temp_root("local_backend_compacts_tombstones_to_markers");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let mut tombstone = object(version("version_2"));
        tombstone.path = "/docs/deleted.md".to_string();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.content_hash = None;
        tombstone.content_size_bytes = None;
        tombstone.changed_at_unix_nanos = 10;
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&tombstone).expect("tombstone");

        let dry_run = backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::dry_run(Duration::from_nanos(50))
                    .with_now_unix_nanos(100),
            )
            .expect("dry run");
        assert_eq!(dry_run.scanned_tombstone_count, 1);
        assert_eq!(dry_run.eligible_tombstone_count, 1);
        assert_eq!(dry_run.compacted_tombstone_count, 0);
        assert_eq!(
            backend
                .get_object(&tombstone.object_id)
                .expect("dry-run object"),
            Some(tombstone.clone())
        );

        let report = backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(50))
                    .with_now_unix_nanos(100),
            )
            .expect("compact");
        assert_eq!(report.scanned_tombstone_count, 1);
        assert_eq!(report.eligible_tombstone_count, 1);
        assert_eq!(report.compacted_tombstone_count, 1);
        assert_eq!(report.removed_object_record_count, 1);
        assert_eq!(report.removed_inode_record_count, 1);
        assert_eq!(report.removed_file_system_object_record_count, 1);
        assert_eq!(report.preserved_path_marker_count, 1);
        assert!(table_path_exists(
            &backend,
            &file_system.file_system_id,
            "/docs/deleted.md"
        ));
        assert_eq!(
            backend
                .get_object(&tombstone.object_id)
                .expect("compacted object"),
            None
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, "/docs/deleted.md")
                .expect("path lookup")
                .expect("compacted marker")
                .kind,
            DfsObjectKind::Tombstone
        );
        assert!(
            backend
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects")
                .is_empty(),
            "compacted tombstones should not hydrate through object scans"
        );
        reset_metadata_current_tables_snapshot_calls();
        let compacted = backend
            .compacted_tombstones_for_object_ids(&BTreeSet::from([
                tombstone.object_id.clone(),
                DfsObjectId::from_legacy_named("obj_unrelated_compacted_lookup")
                    .expect("unrelated object id"),
            ]))
            .expect("bounded compacted evidence");
        assert_eq!(compacted, vec![tombstone.clone()]);
        assert_eq!(
            metadata_current_tables_snapshot_calls(),
            1,
            "a bounded evidence set must be joined against one shard snapshot"
        );

        drop(backend);
        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(
            reopened
                .get_object(&tombstone.object_id)
                .expect("reopened object"),
            None
        );
        assert_eq!(
            reopened
                .get_object_by_path(&file_system.file_system_id, "/docs/deleted.md")
                .expect("reopened path")
                .expect("compacted marker")
                .kind,
            DfsObjectKind::Tombstone
        );
        assert!(
            reopened
                .objects_for_file_system(&file_system.file_system_id)
                .expect("reopened objects")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_tombstone_compaction_retains_young_and_referenced_tombstones() {
        let root = temp_root("local_backend_tombstone_compaction_retains");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let mut young = object(version("version_2"));
        young.path = "/docs/young.md".to_string();
        young.kind = DfsObjectKind::Tombstone;
        young.content_hash = None;
        young.changed_at_unix_nanos = 90;
        backend.put_object(&young).expect("young tombstone");

        let mut referenced = object(version("version_3"));
        referenced.object_id =
            DfsObjectId::from_legacy_named("obj_referenced_tombstone").expect("object id");
        referenced.path = "/docs/referenced.md".to_string();
        referenced.kind = DfsObjectKind::Tombstone;
        referenced.content_hash = None;
        referenced.changed_at_unix_nanos = 1;
        backend
            .put_object(&referenced)
            .expect("referenced tombstone");
        backend
            .put_version(&DfsObjectVersionRecord {
                file_system_id: referenced.file_system_id.clone(),
                object_id: referenced.object_id.clone(),
                version: referenced.version.clone(),
                content_hash: None,
                size_bytes: None,
                locations: Vec::new(),
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: false,
            })
            .expect("version ref");

        let report = backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(50))
                    .with_now_unix_nanos(100),
            )
            .expect("compact");
        assert_eq!(report.scanned_tombstone_count, 2);
        assert_eq!(report.eligible_tombstone_count, 0);
        assert_eq!(report.retained_young_tombstone_count, 1);
        assert_eq!(report.retained_referenced_tombstone_count, 1);
        assert_eq!(report.compacted_tombstone_count, 0);
        assert_eq!(
            backend.get_object(&young.object_id).expect("young object"),
            Some(young)
        );
        assert_eq!(
            backend
                .get_object(&referenced.object_id)
                .expect("referenced object"),
            Some(referenced)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recreating_compacted_tombstone_path_removes_marker() {
        let root = temp_root("local_backend_recreate_compacted_tombstone_path");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let mut tombstone = object(version("version_2"));
        tombstone.path = "/docs/recreate.md".to_string();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.content_hash = None;
        tombstone.changed_at_unix_nanos = 1;
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&tombstone).expect("tombstone");
        backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(0))
                    .with_now_unix_nanos(100),
            )
            .expect("compact");

        let mut replacement = object(version("version_1"));
        replacement.object_id =
            DfsObjectId::from_legacy_named("obj_recreated_path").expect("object id");
        replacement.path = tombstone.path.clone();
        replacement.kind = DfsObjectKind::File;
        replacement.content_hash = Some("blake3:new".to_string());
        backend.put_object(&replacement).expect("replacement");

        assert!(backend.tables.compacted_tombstones.is_empty());
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &replacement.path)
                .expect("path lookup"),
            Some(replacement.clone())
        );
        assert_eq!(
            backend
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects")
                .into_iter()
                .map(|object| object.object_id)
                .collect::<Vec<_>>(),
            vec![replacement.object_id]
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_compacts_tombstone_and_removes_object_index() {
        let root = temp_root("sharded_backend_compacts_tombstone");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let mut tombstone = object(version("version_2"));
        tombstone.path = "/services/deleted.md".to_string();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.content_hash = None;
        tombstone.changed_at_unix_nanos = 1;
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&tombstone).expect("tombstone");
        assert!(
            backend
                .object_shard_index_path(&tombstone.object_id)
                .is_file()
        );

        let report = backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(0))
                    .with_now_unix_nanos(100),
            )
            .expect("compact");
        assert_eq!(report.compacted_tombstone_count, 1);
        assert!(
            !backend
                .object_shard_index_path(&tombstone.object_id)
                .exists(),
            "compaction should remove stale object shard routing indexes"
        );
        assert_eq!(
            backend
                .get_object(&tombstone.object_id)
                .expect("object lookup"),
            None
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, "/services/deleted.md")
                .expect("path lookup")
                .expect("compacted marker")
                .kind,
            DfsObjectKind::Tombstone
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_compaction_skips_activated_retiring_shard() {
        let root = temp_root("sharded_backend_compaction_skips_retiring_shard");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let mut root_tombstone = object(version("version_compaction_active_root"));
        root_tombstone.path = "/deleted.md".to_string();
        root_tombstone.kind = DfsObjectKind::Tombstone;
        root_tombstone.content_hash = None;
        root_tombstone.changed_at_unix_nanos = 1;
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&root_tombstone).expect("root tombstone");

        let services = DfsMetadataShardId::new("services");
        backend
            .freeze_metadata_shard_for_membership_handoff(&services, "handoff-retired-services", 1)
            .expect("freeze services shard")
            .expect("configured services shard");
        backend
            .mark_metadata_shard_membership_handoff_activated(
                &services,
                "handoff-retired-services",
                2,
            )
            .expect("mark successor activation");

        let report = backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::ZERO)
                    .with_now_unix_nanos(100),
            )
            .expect("an unrelated retired shard must not abort compaction");
        assert_eq!(report.compacted_tombstone_count, 1);
        assert_eq!(
            backend
                .get_object(&root_tombstone.object_id)
                .expect("compacted root tombstone"),
            None
        );
        assert!(matches!(
            backend
                .shard_backend_mut(&services)
                .expect("services backend")
                .put_object(&object(version("version_retired_shard_stays_frozen"))),
            Err(MetadataBackendError::MembershipHandoffFrozen { .. })
        ));

        cleanup(&root);
    }

    #[test]
    fn local_backend_rebuilds_dirfrag_counts_during_table_replay() {
        let root = temp_root("local_backend_rebuilds_dirfrag_counts");
        {
            let mut backend =
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                    .expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");

            let mut readme = object(version("version_1"));
            readme.path = "/docs/readme.md".to_string();
            let mut changelog = object(version("version_1"));
            changelog.object_id = DfsObjectId::from_legacy_named("obj_changelog").expect("object");
            changelog.path = "/docs/changelog.md".to_string();
            for object in [&readme, &changelog] {
                backend.put_object(object).expect("object");
            }

            let path_record = path_record_for_object(&readme);
            assert_eq!(
                table_dirfrag(
                    &backend,
                    &path_record.parent_inode_id,
                    &path_record.dirfrag_id
                )
                .entry_count,
                2
            );
            force_table_replay_from_journal(&backend, &root, 0);
        }

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        let path_record = path_record_for_object(&object(version("version_1")));
        assert_eq!(
            table_dirfrag(
                &recovered,
                &path_record.parent_inode_id,
                &path_record.dirfrag_id
            )
            .entry_count,
            2
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_lists_objects_from_filesystem_object_index() {
        let root = temp_root("local_backend_lists_objects_from_filesystem_object_index");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let other_file_system = DfsFileSystem {
            file_system_id: DfsFileSystemId::from_legacy_named("otherfs").expect("filesystem"),
            display_name: "other".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend
            .put_file_system(&other_file_system)
            .expect("other filesystem");

        let mut readme = object(version("version_1"));
        readme.path = "/docs/readme.md".to_string();
        let mut changelog = object(version("version_1"));
        changelog.object_id = DfsObjectId::from_legacy_named("obj_changelog").expect("object");
        changelog.path = "/docs/changelog.md".to_string();
        let mut other = object(version("version_1"));
        other.object_id = DfsObjectId::from_legacy_named("obj_other").expect("object");
        other.file_system_id = other_file_system.file_system_id.clone();
        other.path = "/other.txt".to_string();
        for object in [&readme, &changelog, &other] {
            backend.put_object(object).expect("object");
        }
        assert!(table_file_system_object_exists(
            &backend,
            &file_system_id(),
            &readme.object_id
        ));

        let objects = backend
            .objects_for_file_system(&file_system_id())
            .expect("objects");
        assert_eq!(
            objects
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/changelog.md", "/docs/readme.md"]
        );

        assert!(table_file_system_object_exists(
            &backend,
            &file_system_id(),
            &changelog.object_id
        ));
        changelog.file_system_id = other_file_system.file_system_id.clone();
        changelog.path = "/moved-changelog.md".to_string();
        backend.put_object(&changelog).expect("move filesystem");
        assert!(
            !table_file_system_object_exists(&backend, &file_system_id(), &changelog.object_id),
            "moving an object to another filesystem should remove its old filesystem-object index"
        );
        assert_eq!(
            backend
                .objects_for_file_system(&file_system_id())
                .expect("objects")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.md"]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_removes_stale_path_index_when_object_moves_filesystems_without_renaming() {
        let root =
            temp_root("local_backend_removes_stale_path_index_when_object_moves_filesystems");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let other_file_system = DfsFileSystem {
            file_system_id: DfsFileSystemId::from_legacy_named("otherfs").expect("filesystem"),
            display_name: "other".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend
            .put_file_system(&other_file_system)
            .expect("other filesystem");

        let mut object = object(version("version_1"));
        object.path = "/services/shared-name.txt".to_string();
        backend.put_object(&object).expect("object");
        assert!(table_path_exists(
            &backend,
            &file_system.file_system_id,
            &object.path
        ));

        object.file_system_id = other_file_system.file_system_id.clone();
        backend
            .put_object(&object)
            .expect("move filesystem without rename");

        assert!(
            !table_path_exists(&backend, &file_system.file_system_id, &object.path),
            "moving an object between filesystems must remove the old filesystem path index even when the path string is unchanged"
        );
        assert!(
            backend
                .get_object_by_path(&file_system.file_system_id, "/services/shared-name.txt")
                .expect("old filesystem lookup")
                .is_none()
        );
        assert_eq!(
            backend
                .get_object_by_path(
                    &other_file_system.file_system_id,
                    "/services/shared-name.txt"
                )
                .expect("new filesystem lookup")
                .expect("object")
                .object_id,
            object.object_id
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_secondary_indexes_for_legacy_object_records() {
        let root = temp_root("local_backend_recovers_secondary_indexes_for_legacy_object_records");
        {
            let mut backend =
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                    .expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");

            let target = object(version("version_1"));
            let mut alias = object(version("version_1"));
            alias.object_id = DfsObjectId::from_legacy_named("obj_legacy_alias").expect("object");
            alias.path = "/docs/readme.alias".to_string();
            alias.kind = DfsObjectKind::Hardlink;
            alias.content_hash = None;
            alias.link_target_object_id = Some(target.object_id.clone());
            for object in [&target, &alias] {
                backend.put_object(object).expect("object");
            }
        }

        for index_dir in ["filesystem-objects", "paths", "dentries", "hardlinks"] {
            fs::remove_dir_all(root.join("records").join(index_dir)).ok();
        }

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .objects_for_file_system(&file_system_id())
                .expect("objects")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.alias", "docs/readme.md"]
        );
        assert_eq!(
            recovered
                .child_objects_for_directory(&file_system_id(), "/docs")
                .expect("children")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.alias", "docs/readme.md"]
        );
        assert_eq!(
            recovered
                .hardlinks_to_object(&file_system_id(), &object_id())
                .expect("hardlinks")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.alias"]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_lists_hardlinks_from_target_index() {
        let root = temp_root("local_backend_lists_hardlinks_from_target_index");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let target = object(version("version_1"));
        let mut alias_a = object(version("version_1"));
        alias_a.object_id = DfsObjectId::from_legacy_named("obj_alias_a").expect("object");
        alias_a.path = "/docs/readme.alias-a".to_string();
        alias_a.kind = DfsObjectKind::Hardlink;
        alias_a.content_hash = None;
        alias_a.link_target_object_id = Some(target.object_id.clone());
        let mut alias_b = alias_a.clone();
        alias_b.object_id = DfsObjectId::from_legacy_named("obj_alias_b").expect("object");
        alias_b.path = "/docs/readme.alias-b".to_string();
        let mut alias_c = alias_a.clone();
        alias_c.object_id = DfsObjectId::from_legacy_named("obj_alias_c").expect("object");
        alias_c.path = "/archive/readme.alias-c".to_string();
        let mut unrelated = object(version("version_1"));
        unrelated.object_id = DfsObjectId::from_legacy_named("obj_unrelated").expect("object");
        unrelated.path = "/docs/unrelated".to_string();

        for object in [&target, &alias_b, &unrelated, &alias_a, &alias_c] {
            backend.put_object(object).expect("object");
        }
        let mut stale_alias = alias_b.clone();
        stale_alias.kind = DfsObjectKind::Tombstone;
        stale_alias.link_target_object_id = None;
        backend
            .put_object(&stale_alias)
            .expect("tombstone stale alias");

        let hardlinks = backend
            .hardlinks_to_object(&file_system_id(), &target.object_id)
            .expect("hardlinks");
        assert_eq!(
            hardlinks
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/archive/readme.alias-c", "/docs/readme.alias-a"]
        );
        assert_eq!(
            backend
                .hardlink_count_to_object(&file_system_id(), &target.object_id)
                .expect("hardlink count"),
            2
        );
        assert_eq!(
            backend
                .first_hardlink_to_object(&file_system_id(), &target.object_id)
                .expect("first hardlink")
                .expect("first hardlink")
                .path,
            "/archive/readme.alias-c"
        );
        assert_eq!(
            backend
                .first_hardlink_to_object_outside_prefix(
                    &file_system_id(),
                    &target.object_id,
                    "/archive",
                )
                .expect("outside prefix")
                .expect("outside prefix")
                .path,
            "/docs/readme.alias-a"
        );

        let first_page = backend
            .hardlinks_to_object_page(&file_system_id(), &target.object_id, None, 1)
            .expect("first page");
        assert_eq!(
            first_page
                .objects
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/archive/readme.alias-c"]
        );
        let second_page = backend
            .hardlinks_to_object_page(
                &file_system_id(),
                &target.object_id,
                first_page.next_cursor.as_ref(),
                1,
            )
            .expect("second page");
        assert_eq!(
            second_page
                .objects
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.alias-a"]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_removes_stale_hardlink_target_index_entries() {
        let root = temp_root("local_backend_removes_stale_hardlink_target_index_entries");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let target_a = object(version("version_1"));
        let mut target_b = object(version("version_1"));
        target_b.object_id = DfsObjectId::from_legacy_named("obj_target_b").expect("object");
        target_b.path = "/docs/target-b.md".to_string();
        let mut alias = object(version("version_1"));
        alias.object_id = DfsObjectId::from_legacy_named("obj_alias").expect("object");
        alias.path = "/docs/readme.alias".to_string();
        alias.kind = DfsObjectKind::Hardlink;
        alias.content_hash = None;
        alias.link_target_object_id = Some(target_a.object_id.clone());

        for object in [&target_a, &target_b, &alias] {
            backend.put_object(object).expect("object");
        }
        assert!(table_hardlink_exists(
            &backend,
            &target_a.object_id,
            &alias.object_id
        ));

        alias.link_target_object_id = Some(target_b.object_id.clone());
        backend.put_object(&alias).expect("retarget alias");
        assert!(
            !table_hardlink_exists(&backend, &target_a.object_id, &alias.object_id),
            "retargeting a hardlink should remove its old target index entry"
        );
        assert!(table_hardlink_exists(
            &backend,
            &target_b.object_id,
            &alias.object_id
        ));

        alias.kind = DfsObjectKind::Tombstone;
        alias.link_target_object_id = None;
        backend.put_object(&alias).expect("tombstone alias");
        assert!(
            !table_hardlink_exists(&backend, &target_b.object_id, &alias.object_id),
            "tombstoning a hardlink should remove its target index entry"
        );
        assert!(
            backend
                .hardlinks_to_object(&file_system_id(), &target_b.object_id)
                .expect("hardlinks")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_persists_intents_and_polls_subscription_batches() {
        let root = temp_root("local_backend_persists_intents_and_polls_subscription_batches");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let intent = DfsWriteIntentRecord {
            intent_id: "dfs_intent_1".to_string(),
            file_system_id: file_system_id(),
            object_id: object_id(),
            base_version: Some(version("version_1")),
            target_version: version("version_2"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 0,
            desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
            policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some("blake3:new".to_string()),
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };

        let entry = backend.put_intent(&intent).expect("intent");
        assert_eq!(entry.sequence, 1);
        assert!(matches!(
            entry.mutation,
            DfsMetadataMutation::PutIntent {
                status: DfsWriteIntentStatus::ReceiptsPending,
                intent: Some(ref journal_intent),
                ..
            } if journal_intent == &intent
        ));
        assert_eq!(
            backend
                .get_intent(&intent.object_id, &intent.intent_id)
                .expect("get intent"),
            Some(intent)
        );

        let mut subscription = backend.subscribe(0);
        let first_batch = backend
            .poll_subscription(&mut subscription)
            .expect("first batch");
        assert_eq!(first_batch.entries.len(), 1);
        assert_eq!(first_batch.next_after_sequence, 1);
        assert_eq!(subscription.next_after_sequence, 1);

        let second_batch = backend
            .poll_subscription(&mut subscription)
            .expect("second batch");
        assert!(second_batch.entries.is_empty());
        assert_eq!(second_batch.next_after_sequence, 1);

        fs::write(
            root.join("journal").join("00000000000000000001.sobj"),
            b"not sobj",
        )
        .expect("corrupt already-consumed journal entry");
        let third_batch = backend
            .poll_subscription(&mut subscription)
            .expect("already-consumed corrupted journal entry is not reparsed");
        assert!(third_batch.entries.is_empty());
        assert_eq!(third_batch.next_after_sequence, 1);

        cleanup(&root);
    }

    #[test]
    fn commit_visible_version_requires_expected_version_and_receipts() {
        let root = temp_root("commit_visible_version_requires_expected_version_and_receipts");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let initial = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&initial).expect("object");

        let mut committed = initial.clone();
        committed.version = version("version_2");
        committed.content_hash = Some("blake3:new".to_string());
        committed.current_locations.push(node("node_replica"));
        let version_record = DfsObjectVersionRecord {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            version: committed.version.clone(),
            content_hash: committed.content_hash.clone(),
            size_bytes: Some(3),
            locations: committed.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                committed_version: committed.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_commit").expect("event"),
            },
            replica_node_id: node("node_replica"),
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };

        assert!(matches!(
            backend.commit_visible_version(
                &initial.version,
                &committed,
                &version_record,
                std::slice::from_ref(&receipt.replica_node_id),
            ),
            Err(MetadataBackendError::Conflict { .. })
        ));

        backend.put_receipt(&receipt).expect("receipt");
        let table_batches_before_commit = metadata_table_batch_count(&root);
        let commit = backend
            .commit_visible_version(
                &initial.version,
                &committed,
                &version_record,
                std::slice::from_ref(&receipt.replica_node_id),
            )
            .expect("commit");
        assert!(matches!(
            commit.mutation,
            DfsMetadataMutation::CommitVisibleVersion { .. }
        ));
        assert_eq!(
            metadata_table_batch_count(&root),
            table_batches_before_commit + 1,
            "one visible-version journal entry should materialize into one table batch"
        );
        assert_eq!(
            backend
                .get_object(&committed.object_id)
                .expect("get object")
                .expect("object")
                .version,
            committed.version
        );
        assert_eq!(
            backend
                .get_version(&committed.object_id, &committed.version)
                .expect("get version"),
            Some(version_record)
        );
        assert_eq!(
            backend
                .receipts_for_version(&committed.object_id, &committed.version)
                .expect("receipts")
                .len(),
            1
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_replays_journal_batch_into_follower_backend() {
        let leader_root = temp_root("local_backend_replays_journal_batch_into_follower_leader");
        let follower_root = temp_root("local_backend_replays_journal_batch_into_follower_follower");
        let shard_id = DfsMetadataShardId::new("shard-replay");
        let mut leader =
            LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("leader backend");
        let mut follower = LocalSobjMetadataBackend::new(shard_id.clone(), &follower_root)
            .expect("follower backend");

        let file_system = file_system();
        let initial = object(version("version_1"));
        leader.put_file_system(&file_system).expect("filesystem");
        leader.put_object(&initial).expect("object");

        let mut committed = initial.clone();
        committed.version = version("version_2");
        committed.content_hash = Some("blake3:new".to_string());
        committed.current_locations.push(node("node_replica"));
        let visible_version = DfsObjectVersionRecord {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            version: committed.version.clone(),
            content_hash: committed.content_hash.clone(),
            size_bytes: Some(3),
            locations: committed.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                committed_version: committed.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_replay_commit").expect("event"),
            },
            replica_node_id: node("node_replica"),
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        leader.put_receipt(&receipt).expect("receipt");
        leader
            .commit_visible_version(
                &initial.version,
                &committed,
                &visible_version,
                std::slice::from_ref(&receipt.replica_node_id),
            )
            .expect("commit");

        let mut subscription = leader.subscribe(0);
        let mut batch = leader
            .poll_subscription(&mut subscription)
            .expect("journal batch");
        assert_eq!(batch.entries.len(), 4);
        let source_sequence_base = JOURNAL_SEGMENT_ENTRY_LIMIT + 64;
        install_empty_replay_checkpoint(
            &mut follower,
            &batch.shard_id,
            source_sequence_base.saturating_sub(1),
        );
        for (offset, entry) in batch.entries.iter_mut().enumerate() {
            entry.sequence = source_sequence_base + offset as u64;
        }
        batch.next_after_sequence = batch
            .entries
            .last()
            .expect("non-empty replay batch")
            .sequence;

        assert_eq!(
            follower
                .apply_journal_batch(&batch)
                .expect("apply journal batch"),
            4
        );
        assert_eq!(
            follower.journal_since(0).expect("follower journal"),
            batch.entries,
            "replicated metadata replay should persist source journal entries instead of appending follower-local replacements"
        );
        assert_eq!(
            follower
                .get_file_system(&file_system.file_system_id)
                .expect("filesystem"),
            Some(file_system)
        );
        assert_eq!(
            follower
                .get_object(&committed.object_id)
                .expect("object")
                .expect("object")
                .version,
            committed.version
        );
        assert_eq!(
            follower
                .get_version(&committed.object_id, &committed.version)
                .expect("version"),
            Some(normalized_version_record(&visible_version))
        );
        assert_eq!(
            follower
                .receipts_for_version(&committed.object_id, &committed.version)
                .expect("receipt"),
            vec![receipt]
        );

        assert_eq!(
            follower
                .apply_journal_batch(&batch)
                .expect("reapply journal batch"),
            0
        );
        assert_eq!(
            follower.journal_since(0).expect("follower journal reapply"),
            batch.entries,
            "idempotent replays must not append duplicate follower journal entries"
        );
        assert_eq!(
            read_sobj::<DfsMetadataReplayCursor>(&follower.replay_cursor_path(&shard_id))
                .expect("replay cursor")
                .last_sequence,
            source_sequence_base.saturating_sub(1),
            "a small replicated batch should leave the recovery-hint cursor at its checkpoint"
        );
        assert_eq!(
            follower
                .metadata_replay_cursor(&shard_id)
                .expect("authoritative replay position"),
            batch.next_after_sequence,
            "the durable prefix must cover a checkpointed cursor between flushes"
        );
        let status = follower
            .metadata_shard_statuses()
            .expect("metadata shard status")
            .pop()
            .expect("follower shard status");
        assert_eq!(
            status.journal_last_sequence, batch.next_after_sequence,
            "a replayed source batch must advance the local journal head to the source sequence"
        );
        assert_eq!(
            status.journal_next_sequence,
            batch.next_after_sequence.saturating_add(1)
        );
        assert_eq!(
            status.replay_cursors,
            vec![DfsMetadataReplayCursorStatus {
                source_shard_id: shard_id.clone(),
                replayed_through_sequence: batch.next_after_sequence,
            }]
        );

        drop(follower);
        let mut reopened = LocalSobjMetadataBackend::new(shard_id.clone(), &follower_root)
            .expect("reopen follower with a lagging replay hint");
        assert_eq!(
            reopened
                .metadata_replay_cursor(&shard_id)
                .expect("reopened authoritative replay position"),
            batch.next_after_sequence,
            "reopen must recover the authoritative replay position from the durable prefix proof"
        );
        assert_eq!(
            reopened
                .replay_cursor_statuses()
                .expect("reopened replay cursor status"),
            vec![DfsMetadataReplayCursorStatus {
                source_shard_id: shard_id,
                replayed_through_sequence: batch.next_after_sequence,
            }],
            "operator status must not regress to the stale checkpoint after reopen"
        );
        assert_eq!(
            reopened
                .apply_journal_batch(&batch)
                .expect("reapply batch after reopen"),
            0,
            "the exact durable prefix, not the lagging cursor hint, owns replay idempotence"
        );
        assert_eq!(
            reopened
                .get_object(&committed.object_id)
                .expect("reopened object")
                .expect("reopened object")
                .version,
            committed.version
        );

        cleanup(&leader_root);
        cleanup(&follower_root);
    }

    #[test]
    fn local_backend_rejects_replication_from_foreign_logical_shard() {
        let root = temp_root("local_backend_rejects_foreign_replication_shard");
        let source_shard_id = DfsMetadataShardId::new("source-shard");
        let target_shard_id = DfsMetadataShardId::new("target-shard");
        let mut source =
            LocalSobjMetadataBackend::new(source_shard_id.clone(), root.join("source"))
                .expect("source backend");
        let mut target =
            LocalSobjMetadataBackend::new(target_shard_id.clone(), root.join("target"))
                .expect("target backend");
        let file_system = file_system();
        source
            .put_file_system(&file_system)
            .expect("source filesystem");
        let batch = source
            .journal_batch_since_limited(0, 16, usize::MAX)
            .expect("source journal batch")
            .expect("source journal entries");
        let snapshot = source.export_table_snapshot();

        let batch_error = target
            .apply_journal_batch(&batch)
            .expect_err("foreign-shard journal replay must fail");
        assert!(
            batch_error.to_string().contains(target_shard_id.0.as_str())
                && batch_error.to_string().contains(source_shard_id.0.as_str()),
            "identity rejection must name both shards: {batch_error}"
        );
        let snapshot_error = target
            .replace_with_replicated_snapshot(
                source_shard_id.clone(),
                batch.next_after_sequence,
                snapshot,
            )
            .expect_err("foreign-shard snapshot install must fail");
        assert!(
            snapshot_error
                .to_string()
                .contains(target_shard_id.0.as_str())
                && snapshot_error
                    .to_string()
                    .contains(source_shard_id.0.as_str()),
            "identity rejection must name both shards: {snapshot_error}"
        );
        assert!(
            target
                .metadata_replay_cursor(&source_shard_id)
                .expect_err("foreign-shard cursor query must fail")
                .to_string()
                .contains(source_shard_id.0.as_str()),
            "cursor lookup must enforce the same logical-shard identity"
        );
        assert_eq!(
            target.journal_last_sequence(),
            0,
            "rejected foreign replication must not advance the target journal"
        );
        assert!(
            target
                .get_file_system(&file_system.file_system_id)
                .expect("target filesystem lookup")
                .is_none(),
            "rejected foreign replication must not mutate target tables"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_replays_authoritative_commit_without_receipt_entry() {
        let leader_root = temp_root("local_backend_replays_authoritative_commit_leader");
        let follower_root = temp_root("local_backend_replays_authoritative_commit_follower");
        let shard_id = DfsMetadataShardId::new("shard-authoritative-commit");
        let mut leader =
            LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("leader backend");
        let mut follower =
            LocalSobjMetadataBackend::new(shard_id, &follower_root).expect("follower backend");

        let file_system = file_system();
        let initial = object(version("version_1"));
        leader.put_file_system(&file_system).expect("filesystem");
        leader.put_object(&initial).expect("object");

        let mut committed = initial.clone();
        committed.version = version("version_2");
        committed.content_hash = Some("blake3:new".to_string());
        committed.current_locations.push(node("node_replica"));
        let visible_version = DfsObjectVersionRecord {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            version: committed.version.clone(),
            content_hash: committed.content_hash.clone(),
            size_bytes: Some(3),
            locations: committed.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                committed_version: committed.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_missing_receipt_commit").expect("event"),
            },
            replica_node_id: node("node_replica"),
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        leader.put_receipt(&receipt).expect("receipt");
        leader
            .commit_visible_version(
                &initial.version,
                &committed,
                &visible_version,
                std::slice::from_ref(&receipt.replica_node_id),
            )
            .expect("commit");

        let mut subscription = leader.subscribe(0);
        let mut batch = leader
            .poll_subscription(&mut subscription)
            .expect("journal batch");
        batch
            .entries
            .retain(|entry| !matches!(entry.mutation, DfsMetadataMutation::PutReceipt { .. }));
        assert_eq!(batch.entries.len(), 3);
        let source_sequence_base = JOURNAL_SEGMENT_ENTRY_LIMIT + 128;
        install_empty_replay_checkpoint(
            &mut follower,
            &batch.shard_id,
            source_sequence_base.saturating_sub(1),
        );
        for (offset, entry) in batch.entries.iter_mut().enumerate() {
            entry.sequence = source_sequence_base + offset as u64;
        }
        batch.next_after_sequence = batch
            .entries
            .last()
            .expect("non-empty replay batch")
            .sequence;

        assert_eq!(
            follower
                .apply_journal_batch(&batch)
                .expect("apply commit without receipt"),
            3
        );
        assert_eq!(
            follower
                .get_object(&committed.object_id)
                .expect("object")
                .expect("object")
                .version,
            committed.version
        );
        assert_eq!(
            follower
                .get_version(&committed.object_id, &committed.version)
                .expect("version"),
            Some(normalized_version_record(&visible_version))
        );
        assert!(
            follower
                .receipts_for_version(&committed.object_id, &committed.version)
                .expect("receipts")
                .is_empty(),
            "the commit entry alone should make the write visible; receipts may replay later"
        );

        cleanup(&leader_root);
        cleanup(&follower_root);
    }

    #[test]
    fn local_backend_replaces_divergent_replica_with_replicated_snapshot() {
        let leader_root = temp_root("local_backend_snapshot_repair_leader");
        let follower_root = temp_root("local_backend_snapshot_repair_follower");
        let shard_id = DfsMetadataShardId::new("root");
        let mut leader =
            LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("leader backend");
        let mut follower = LocalSobjMetadataBackend::new(shard_id.clone(), &follower_root)
            .expect("follower backend");

        let leader_file_system = file_system();
        leader
            .put_file_system(&leader_file_system)
            .expect("leader filesystem");
        let mut follower_file_system = leader_file_system.clone();
        follower_file_system.display_name = "diverged".to_string();
        follower
            .put_file_system(&follower_file_system)
            .expect("follower filesystem");
        let mut follower_object = object(version("follower-v1"));
        follower_object.content_hash = Some("blake3:follower-diverged".to_string());
        follower
            .put_object(&follower_object)
            .expect("follower object");
        assert!(
            follower
                .apply_journal_batch(
                    &leader
                        .journal_batch_since_limited(0, 1, usize::MAX)
                        .expect("leader batch")
                        .expect("leader batch")
                )
                .expect_err("conflicting sequence should reject before snapshot repair")
                .to_string()
                .contains("already exists with a different entry")
        );

        let archived = follower
            .replace_with_replicated_snapshot(
                shard_id.clone(),
                leader.journal_last_sequence(),
                leader.export_table_snapshot(),
            )
            .expect("replace snapshot");
        assert!(archived);
        assert_eq!(
            follower.metadata_prefix_proof().expect("follower proof"),
            leader.metadata_prefix_proof().expect("leader proof"),
            "snapshot install must adopt the validated authority chain head"
        );
        assert_eq!(
            follower
                .get_file_system(&leader_file_system.file_system_id)
                .expect("follower filesystem"),
            Some(leader_file_system)
        );
        assert!(
            follower_root.join("replica-repair-archives").exists(),
            "divergent replica state should be archived before replacement"
        );
        let archive_dir = read_dir(&follower_root.join("replica-repair-archives"))
            .expect("repair archives")
            .into_iter()
            .next()
            .expect("repair archive")
            .path();
        assert!(
            archive_dir.join("journal").join("segments").exists(),
            "journal segments should be archived without moving the live journal lock directory"
        );
        assert!(
            follower_root.join("journal").join("write.lock").exists(),
            "snapshot repair must keep the live journal directory in place for Windows"
        );
        let leader_object = object(version("leader-v1"));
        leader.put_object(&leader_object).expect("leader object");
        follower
            .apply_journal_batch(
                &leader
                    .journal_batch_since_limited(1, 1, usize::MAX)
                    .expect("leader object batch")
                    .expect("leader object batch"),
            )
            .expect("repaired follower should accept future primary sequences");
        assert_eq!(
            follower.metadata_prefix_proof().expect("follower proof"),
            leader.metadata_prefix_proof().expect("leader proof"),
            "the next contiguous entry must extend the installed chain identically"
        );
        assert_eq!(
            follower
                .get_object(&leader_object.object_id)
                .expect("follower object"),
            Some(leader_object)
        );
        let status = follower
            .metadata_shard_statuses()
            .expect("metadata status")
            .pop()
            .expect("status");
        assert_eq!(status.journal_last_sequence, leader.journal_last_sequence());
        assert_eq!(
            status.replay_cursors,
            vec![DfsMetadataReplayCursorStatus {
                source_shard_id: shard_id.clone(),
                replayed_through_sequence: leader.journal_last_sequence(),
            }]
        );

        cleanup(&leader_root);
        cleanup(&follower_root);
    }

    #[test]
    fn unreadable_replica_can_install_authoritative_snapshot() {
        let leader_root = temp_root("unreadable_snapshot_repair_leader");
        let replica_root = temp_root("unreadable_snapshot_repair_replica");
        let shard_id = DfsMetadataShardId::new("root");
        let mut leader =
            LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("leader");
        let file_system = file_system();
        leader.put_file_system(&file_system).expect("filesystem");
        let genesis_snapshot = leader.export_table_snapshot();
        let genesis_sequence = leader.journal_last_sequence();
        let object = object(version("authoritative-v1"));
        let object_entry = leader.put_object(&object).expect("object");
        let authoritative_sequence = leader.journal_last_sequence();
        let authoritative_snapshot = leader.export_table_snapshot();

        let mut replica =
            LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root).expect("replica");
        replica
            .replace_with_replicated_snapshot(shard_id.clone(), genesis_sequence, genesis_snapshot)
            .expect("install genesis checkpoint");
        append_segmented_journal_entry(&replica_root, &object_entry)
            .expect("append authoritative entry");
        let mut conflicting_entry = object_entry.clone();
        conflicting_entry.created_at += time::Duration::nanoseconds(1);
        append_segmented_journal_entry(&replica_root, &conflicting_entry)
            .expect("append conflicting duplicate sequence");
        write_sobj_atomic(
            &replica_root.join("journal").join("head.sobj"),
            &DfsMetadataJournalHead {
                next_sequence: authoritative_sequence.saturating_add(1),
            },
        )
        .expect("journal head");
        drop(replica);

        let open_error = LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root)
            .expect_err("conflicting duplicate must make ordinary replay fail");
        assert!(open_error.to_string().contains("cannot extend prefix"));

        let permit = DfsMetadataReplicaIngestPermit::new_validated(
            file_system.file_system_id.clone(),
            shard_id.clone(),
            node("node_unreadable_snapshot_source"),
            1,
        );
        let (repaired, archived) =
            LocalSobjMetadataBackend::replace_unreadable_with_replicated_snapshot(
                &permit,
                shard_id.clone(),
                &replica_root,
                shard_id.clone(),
                authoritative_sequence,
                authoritative_snapshot,
            )
            .expect("repair unreadable replica");
        assert!(archived);
        assert_eq!(
            repaired.get_object(&object.object_id).expect("object"),
            Some(object.clone())
        );
        drop(repaired);

        let reopened = LocalSobjMetadataBackend::new(shard_id, &replica_root)
            .expect("reopen repaired replica");
        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("reopened object"),
            Some(object)
        );

        cleanup(&leader_root);
        cleanup(&replica_root);
    }

    #[test]
    fn local_backend_persists_metadata_replication_cursor_monotonically() {
        let root = temp_root("local_backend_metadata_replication_cursor");
        let target_node_id = node("node_cursor_target");
        let shard_id = DfsMetadataShardId::new("root");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        assert_eq!(
            backend
                .metadata_replication_cursor(&target_node_id, &shard_id)
                .expect("initial cursor"),
            0
        );

        backend
            .put_metadata_replication_cursor(&target_node_id, &shard_id, 42)
            .expect("write cursor");
        let cursor_path = backend.metadata_replication_cursor_path(&target_node_id, &shard_id);
        let metadata_before_stale_ack = fs::metadata(&cursor_path).expect("cursor metadata");
        let modified_before_stale_ack = metadata_before_stale_ack
            .modified()
            .expect("cursor modified time");
        std::thread::sleep(Duration::from_millis(25));
        backend
            .put_metadata_replication_cursor(&target_node_id, &shard_id, 7)
            .expect("stale cursor");
        let metadata_after_stale_ack = fs::metadata(&cursor_path).expect("cursor metadata");
        assert_eq!(
            metadata_after_stale_ack.len(),
            metadata_before_stale_ack.len(),
            "stale acknowledgements should not rewrite the durable replication cursor"
        );
        assert_eq!(
            metadata_after_stale_ack
                .modified()
                .expect("cursor modified time"),
            modified_before_stale_ack,
            "stale acknowledgements should leave the durable replication cursor untouched"
        );
        let reopened =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("reopen");

        assert_eq!(
            reopened
                .metadata_replication_cursor(&target_node_id, &shard_id)
                .expect("reopened cursor"),
            42,
            "older acknowledgements must not move a durable replication cursor backwards"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_replays_semantically_duplicate_receipt_as_idempotent() {
        let root = temp_root("local_backend_duplicate_receipt_semantic_idempotent");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let object = object(version("version_1"));
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: file_system.file_system_id.clone(),
                object_id: object.object_id.clone(),
                committed_version: object.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_receipt_a").expect("event"),
            },
            replica_node_id: node("node_replica"),
            content_hash: object.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_receipt(&receipt).expect("receipt");
        let mut replayed = receipt.clone();
        replayed.receipt.event_id = EventId::from_legacy_named("evt_receipt_b").expect("event");
        replayed.received_at = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1);
        let batch = DfsMetadataJournalBatch {
            shard_id: DfsMetadataShardId::new("shard-root"),
            entries: vec![DfsMetadataJournalEntry {
                shard_id: DfsMetadataShardId::new("shard-root"),
                sequence: 2,
                created_at: OffsetDateTime::UNIX_EPOCH,
                mutation: DfsMetadataMutation::PutReceipt {
                    file_system_id: file_system.file_system_id,
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    replica_node_id: receipt.replica_node_id.clone(),
                    receipt: Some(replayed),
                },
            }],
            next_after_sequence: 2,
        };

        assert_eq!(
            backend
                .apply_journal_batch(&batch)
                .expect("duplicate receipt replay"),
            0
        );
        assert_eq!(
            backend
                .receipts_for_version(&object.object_id, &object.version)
                .expect("receipts"),
            vec![receipt]
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_object_locations_from_visible_version() {
        let root = temp_root("local_backend_recovers_object_locations_from_visible_version");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let owner = node("node_owner");
        let replica = node("node_replica");
        backend.put_file_system(&file_system()).expect("filesystem");
        let mut object = object(version("version_2"));
        object.current_locations = vec![owner.clone()];
        object.content_hash = Some("blake3:recover-locations".to_string());
        backend.put_object(&object).expect("object");
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: Some(18),
            locations: vec![owner.clone(), replica.clone()],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        backend.put_version(&version_record).expect("version");

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");

        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object")
                .expect("object")
                .current_locations,
            vec![owner, replica]
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_rejects_replay_entries_without_record_payloads() {
        let root = temp_root("local_backend_rejects_replay_entries_without_record_payloads");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let entry = DfsMetadataJournalEntry {
            shard_id: DfsMetadataShardId::new("shard-leader"),
            sequence: 7,
            created_at: OffsetDateTime::UNIX_EPOCH,
            mutation: DfsMetadataMutation::PutFileSystem {
                file_system_id: file_system_id(),
                file_system: None,
            },
        };

        let error = backend
            .apply_journal_entry(&entry)
            .expect_err("missing payload should fail replay");
        assert!(
            error
                .to_string()
                .contains("cannot replay put-file-system without its record payload")
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_normalizes_replica_locations_at_persistence_boundary() {
        let root = temp_root("local_backend_normalizes_replica_locations");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let owner = node("node_owner");
        let replica = node("node_replica");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut object = object(version("version_1"));
        object.current_locations = vec![owner.clone(), replica.clone(), owner.clone()];
        backend.put_object(&object).expect("object");
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("get object")
                .expect("object")
                .current_locations,
            vec![owner.clone(), replica.clone()]
        );

        let mut version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: Some(3),
            locations: vec![owner.clone(), replica.clone(), owner.clone()],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        backend.put_version(&version_record).expect("version");
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("get version")
                .expect("version")
                .locations,
            vec![owner.clone(), replica.clone()]
        );

        let version_2 = version("version_2");
        let mut committed = object.clone();
        committed.version = version_2.clone();
        committed.current_locations = vec![owner.clone(), replica.clone(), owner.clone()];
        version_record.version = version_2;
        version_record.locations = committed.current_locations.clone();
        for node_id in [&owner, &replica] {
            backend
                .put_receipt(&DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: committed.file_system_id.clone(),
                        object_id: committed.object_id.clone(),
                        committed_version: committed.version.clone(),
                        owner_node_id: owner.clone(),
                        event_id: EventId::from_legacy_named(&format!("evt_{node_id}"))
                            .expect("event"),
                    },
                    replica_node_id: node_id.clone(),
                    content_hash: committed.content_hash.clone(),
                    received_at: OffsetDateTime::UNIX_EPOCH,
                })
                .expect("receipt");
        }
        backend
            .commit_visible_version(
                &object.version,
                &committed,
                &version_record,
                &[owner.clone(), replica.clone(), owner.clone()],
            )
            .expect("visible commit");
        assert_eq!(
            backend
                .get_object(&committed.object_id)
                .expect("get committed object")
                .expect("committed object")
                .current_locations,
            vec![owner.clone(), replica.clone()]
        );
        assert_eq!(
            backend
                .get_version(&committed.object_id, &committed.version)
                .expect("get committed version")
                .expect("committed version")
                .locations,
            vec![owner, replica]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_interrupted_visible_version_commit() {
        let root = temp_root("local_backend_recovers_interrupted_visible_version_commit");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let initial = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&initial).expect("object");

        let mut committed = initial.clone();
        committed.version = version("version_2");
        committed.content_hash = Some("blake3:new".to_string());
        committed.current_locations.push(node("node_replica"));
        let version_record = DfsObjectVersionRecord {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            version: committed.version.clone(),
            content_hash: committed.content_hash.clone(),
            size_bytes: Some(3),
            locations: committed.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                committed_version: committed.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_commit").expect("event"),
            },
            replica_node_id: node("node_replica"),
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_receipt(&receipt).expect("receipt");
        backend
            .commit_visible_version(
                &initial.version,
                &committed,
                &version_record,
                std::slice::from_ref(&receipt.replica_node_id),
            )
            .expect("commit");

        force_table_replay_from_journal(&backend, &root, 0);
        drop(backend);

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .get_object(&committed.object_id)
                .expect("get object")
                .expect("object")
                .version,
            committed.version
        );
        assert_eq!(
            recovered
                .get_version(&committed.object_id, &committed.version)
                .expect("get version"),
            Some(version_record)
        );

        cleanup(&root);
    }

    #[test]
    fn healthy_visible_commit_recovery_refreshes_once_for_all_retained_commits() {
        let root = temp_root("healthy_visible_commit_recovery_refreshes_once");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let replica_node_id = node("node_replica");
        let mut current = object(version("version_1"));
        backend.put_object(&current).expect("initial object");
        for index in 2..=9 {
            let mut committed = current.clone();
            committed.version = version(&format!("version_{index}"));
            committed.content_hash = Some(format!("blake3:grouped-recovery-{index}"));
            committed.content_size_bytes = Some(index);
            committed.current_locations = vec![replica_node_id.clone()];
            let visible_version = DfsObjectVersionRecord {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                version: committed.version.clone(),
                content_hash: committed.content_hash.clone(),
                size_bytes: committed.content_size_bytes,
                locations: committed.current_locations.clone(),
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: true,
            };
            let receipt = DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: committed.file_system_id.clone(),
                    object_id: committed.object_id.clone(),
                    committed_version: committed.version.clone(),
                    owner_node_id: committed.owner_node_id.clone(),
                    event_id: EventId::from_legacy_named(format!("evt_grouped_recovery_{index}"))
                        .expect("event"),
                },
                replica_node_id: replica_node_id.clone(),
                content_hash: committed.content_hash.clone(),
                received_at: OffsetDateTime::UNIX_EPOCH,
            };
            backend.put_receipt(&receipt).expect("receipt");
            backend
                .commit_visible_version(
                    &current.version,
                    &committed,
                    &visible_version,
                    std::slice::from_ref(&replica_node_id),
                )
                .expect("visible commit");
            current = committed;
        }

        assert_eq!(backend.tables.visible_commits.len(), 8);
        let journal_sequence = backend.journal_last_sequence();
        reset_metadata_begin_write_calls();

        backend
            .recover_visible_version_commits()
            .expect("healthy recovery pass");

        assert_eq!(
            metadata_begin_write_calls(),
            1,
            "one healthy recovery pass should refresh once, independent of retained commit count"
        );
        assert_eq!(
            backend.journal_last_sequence(),
            journal_sequence,
            "healthy retained commits must not be re-journaled"
        );
        assert_eq!(
            backend
                .get_object(&current.object_id)
                .expect("current object"),
            Some(current)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_latest_visible_version_without_resurrecting_deleted_objects() {
        let root = temp_root(
            "local_backend_recovers_latest_visible_version_without_resurrecting_deleted_objects",
        );
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let initial = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&initial).expect("object");

        let mut version_2 = initial.clone();
        version_2.version = version("version_2");
        version_2.content_hash = Some("blake3:two".to_string());
        let version_2_record = DfsObjectVersionRecord {
            file_system_id: version_2.file_system_id.clone(),
            object_id: version_2.object_id.clone(),
            version: version_2.version.clone(),
            content_hash: version_2.content_hash.clone(),
            size_bytes: Some(3),
            locations: version_2.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let version_2_receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: version_2.file_system_id.clone(),
                object_id: version_2.object_id.clone(),
                committed_version: version_2.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_commit_two").expect("event"),
            },
            replica_node_id: node("node_owner"),
            content_hash: version_2.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_receipt(&version_2_receipt).expect("receipt");
        backend
            .commit_visible_version(
                &initial.version,
                &version_2,
                &version_2_record,
                std::slice::from_ref(&version_2_receipt.replica_node_id),
            )
            .expect("commit version 2");

        let mut version_3 = version_2.clone();
        version_3.version = version("version_3");
        version_3.content_hash = Some("blake3:three".to_string());
        let version_3_record = DfsObjectVersionRecord {
            file_system_id: version_3.file_system_id.clone(),
            object_id: version_3.object_id.clone(),
            version: version_3.version.clone(),
            content_hash: version_3.content_hash.clone(),
            size_bytes: Some(5),
            locations: version_3.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            visible: true,
        };
        let version_3_receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: version_3.file_system_id.clone(),
                object_id: version_3.object_id.clone(),
                committed_version: version_3.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_commit_three").expect("event"),
            },
            replica_node_id: node("node_owner"),
            content_hash: version_3.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_receipt(&version_3_receipt).expect("receipt");
        backend
            .commit_visible_version(
                &version_2.version,
                &version_3,
                &version_3_record,
                std::slice::from_ref(&version_3_receipt.replica_node_id),
            )
            .expect("commit version 3");

        backend.put_object(&version_2).expect("stale object");
        drop(backend);
        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .get_object(&version_3.object_id)
                .expect("get recovered")
                .expect("object")
                .version,
            version_3.version
        );
        drop(recovered);

        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let mut tombstone = version_3.clone();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.version = version("version_4");
        tombstone.content_hash = None;
        tombstone.content_size_bytes = None;
        backend.put_object(&tombstone).expect("tombstone");
        drop(backend);

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .get_object(&tombstone.object_id)
                .expect("get tombstone")
                .expect("object")
                .kind,
            DfsObjectKind::Tombstone
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_interrupted_namespace_transaction() {
        let root = temp_root("local_backend_recovers_interrupted_namespace_transaction");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let mut source = object(version("version_1"));
        source.path = "/services/web/index.html".to_string();
        let mut destination = object(version("version_1"));
        destination.object_id = DfsObjectId::from_legacy_named("obj_current").expect("object");
        destination.path = "/services/web/current.html".to_string();
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&source).expect("source");
        backend.put_object(&destination).expect("destination");

        source.path = "/services/web/current.html".to_string();
        destination.kind = DfsObjectKind::Tombstone;
        destination.version = version("version_2");
        let transaction = DfsNamespaceTransaction {
            transaction_id: "namespace_test_rename".to_string(),
            file_system_id: file_system_id(),
            objects: vec![source.clone(), destination.clone()],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend
            .apply_namespace_transaction(&transaction)
            .expect("transaction");
        assert_eq!(
            metadata_table_batch_count(&root),
            4,
            "namespace transactions should materialize all table mutations for one journal entry in one table batch"
        );

        force_table_replay_from_journal(&backend, &root, 0);
        drop(backend);

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .get_object_by_path(&file_system_id(), "/services/web/current.html")
                .expect("lookup current")
                .expect("current object")
                .object_id,
            source.object_id
        );
        assert!(
            recovered
                .get_object_by_path(&file_system_id(), "/services/web/index.html")
                .expect("lookup old")
                .is_none()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_does_not_rejournal_healthy_namespace_transactions() {
        let root = temp_root("local_backend_reopen_does_not_rejournal_namespace_transactions");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let mut source = object(version("version_1"));
        source.path = "/services/web/index.html".to_string();
        let mut destination = object(version("version_1"));
        destination.object_id = DfsObjectId::from_legacy_named("obj_current").expect("object");
        destination.path = "/services/web/current.html".to_string();
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&source).expect("source");
        backend.put_object(&destination).expect("destination");

        source.path = "/services/web/current.html".to_string();
        destination.kind = DfsObjectKind::Tombstone;
        destination.version = version("version_2");
        let transaction = DfsNamespaceTransaction {
            transaction_id: "namespace_test_rename".to_string(),
            file_system_id: file_system_id(),
            objects: vec![source, destination],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend
            .apply_namespace_transaction(&transaction)
            .expect("transaction");
        let journal_len = backend.journal_since(0).expect("journal").len();
        drop(backend);

        reset_namespace_recovery_planner_calls();
        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopened backend");

        assert_eq!(
            namespace_recovery_planner_calls(),
            0,
            "healthy transaction history must not clone tables through the recovery planner"
        );
        assert_eq!(
            reopened.journal_since(0).expect("journal").len(),
            journal_len,
            "opening a healthy backend must not append duplicate namespace transaction journal entries"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_put_object_and_version_uses_one_table_batch_without_namespace_transaction() {
        let root = temp_root("local_backend_object_version_grouped_table_batch");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        let mut object = object(version("version_2"));
        object.path = "/services/web/grouped.bin".to_string();
        object.content_hash = Some("blake3:grouped-object-version".to_string());
        object.content_size_bytes = Some(4096);
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let table_batches_before = metadata_table_batch_count(&root);

        let entries = backend
            .put_object_and_version(&object, &version_record)
            .expect("grouped object version");

        assert_eq!(entries.len(), 2);
        assert!(matches!(
            entries[0].mutation,
            DfsMetadataMutation::PutObject { .. }
        ));
        assert!(matches!(
            entries[1].mutation,
            DfsMetadataMutation::PutVersion { .. }
        ));
        assert_eq!(
            metadata_table_batch_count(&root),
            table_batches_before + 1,
            "object and visible version should materialize in one table batch"
        );
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object.clone())
        );
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("version lookup"),
            Some(normalized_version_record(&version_record))
        );
        assert!(backend.tables.namespace_transactions.is_empty());
        let journal_len = backend.journal_since(0).expect("journal").len();
        drop(backend);

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopened backend");
        assert_eq!(
            reopened.journal_since(0).expect("journal").len(),
            journal_len,
            "grouped object/version write must not create a recoverable namespace transaction"
        );
        cleanup(&root);
    }

    #[test]
    fn namespace_transaction_publishes_object_and_version_in_one_table_batch() {
        let root = temp_root("namespace_transaction_object_version_single_batch");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        let mut object = object(version("version_2"));
        object.path = "/services/web/app.bin".to_string();
        object.content_hash = Some("blake3:namespace-version".to_string());
        object.content_size_bytes = Some(16 * 1024 * 1024 * 1024);
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let table_batches_before = metadata_table_batch_count(&root);

        backend
            .apply_namespace_transaction(&DfsNamespaceTransaction {
                transaction_id: "namespace_object_version_single_batch".to_string(),
                file_system_id: object.file_system_id.clone(),
                objects: vec![object.clone()],
                versions: vec![version_record.clone()],
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("transaction");

        assert_eq!(
            metadata_table_batch_count(&root),
            table_batches_before + 1,
            "creating an object and its visible version should materialize in one table batch"
        );
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object.clone())
        );
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("version lookup"),
            Some(normalized_version_record(&version_record))
        );
        let table_batches_after_transaction = metadata_table_batch_count(&root);
        drop(backend);

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen backend");

        assert_eq!(
            reopened
                .get_version(&object.object_id, &object.version)
                .expect("reopened version lookup"),
            Some(normalized_version_record(&version_record))
        );
        assert_eq!(
            metadata_table_batch_count(&root),
            table_batches_after_transaction,
            "replaying a deferred local cursor must not duplicate namespace transaction table batches"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_persists_lists_and_updates_conflicts() {
        let root = temp_root("local_backend_persists_lists_and_updates_conflicts");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let conflict = DfsConflictRecord {
            conflict_id: "conflict_a".to_string(),
            file_system_id: file_system_id(),
            object_id: object_id(),
            path: "docs/readme.md".to_string(),
            base_version: Some(version("version_1")),
            local_version: version("version_2_local"),
            remote_version: version("version_2_remote"),
            detected_by_node_id: node("node_laptop"),
            reason: "stale reconnect write".to_string(),
            local_content_hash: Some("blake3:local".to_string()),
            remote_content_hash: Some("blake3:remote".to_string()),
            created_by_intent_id: None,
            strategy: Some("branch".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::UNIX_EPOCH,
        };
        let mut conflict_b = conflict.clone();
        conflict_b.conflict_id = "conflict_b".to_string();

        let entry = backend.put_conflict(&conflict).expect("put conflict");
        backend.put_conflict(&conflict_b).expect("put conflict b");
        assert!(matches!(
            entry.mutation,
            DfsMetadataMutation::PutConflict {
                conflict: Some(ref journal_conflict),
                ..
            } if journal_conflict == &conflict
        ));
        assert_eq!(
            backend
                .get_conflict(&conflict.object_id, &conflict.conflict_id)
                .expect("get conflict"),
            Some(conflict.clone())
        );
        assert_eq!(
            backend
                .conflicts_for_object(&conflict.object_id)
                .expect("list conflicts"),
            vec![conflict.clone(), conflict_b.clone()]
        );
        let first_page = backend
            .conflicts_for_object_page(&conflict.object_id, None, 1)
            .expect("first conflict page");
        assert_eq!(first_page.conflicts, vec![conflict.clone()]);
        let second_page = backend
            .conflicts_for_object_page(&conflict.object_id, first_page.next_cursor.as_ref(), 1)
            .expect("second conflict page");
        assert_eq!(second_page.conflicts, vec![conflict_b.clone()]);
        assert!(second_page.next_cursor.is_none());
        assert!(
            backend
                .conflicts_for_object_page(&conflict.object_id, None, 0)
                .expect("empty conflict page")
                .conflicts
                .is_empty()
        );
        let first_scan_page = backend
            .conflict_records_page(None, 1)
            .expect("first conflict scan page");
        assert_eq!(first_scan_page.conflicts, vec![conflict.clone()]);
        let second_scan_page = backend
            .conflict_records_page(first_scan_page.next_cursor.as_ref(), 1)
            .expect("second conflict scan page");
        assert_eq!(second_scan_page.conflicts, vec![conflict_b.clone()]);
        assert!(second_scan_page.next_cursor.is_none());
        assert!(
            backend
                .conflict_records_page(None, 0)
                .expect("empty conflict scan")
                .conflicts
                .is_empty()
        );
        assert_eq!(
            backend.all_conflicts().expect("all conflicts"),
            vec![conflict.clone(), conflict_b.clone()]
        );

        let resolved = backend
            .update_conflict_status(
                &conflict.object_id,
                &conflict.conflict_id,
                DfsConflictStatus::Resolved,
            )
            .expect("resolve conflict")
            .expect("conflict");
        assert_eq!(resolved.status, DfsConflictStatus::Resolved);
        assert_eq!(
            backend
                .get_conflict(&conflict.object_id, &conflict.conflict_id)
                .expect("get resolved")
                .expect("conflict")
                .status,
            DfsConflictStatus::Resolved
        );
        assert_eq!(
            backend
                .remove_conflict(&conflict.object_id, &conflict.conflict_id)
                .expect("remove resolved conflict")
                .as_ref()
                .map(|conflict| conflict.status.clone()),
            Some(DfsConflictStatus::Resolved)
        );
        assert_eq!(
            backend
                .remove_conflict(&conflict_b.object_id, &conflict_b.conflict_id)
                .expect("remove second conflict"),
            Some(conflict_b.clone())
        );
        assert_eq!(
            backend
                .get_conflict(&conflict.object_id, &conflict.conflict_id)
                .expect("resolved conflict removed"),
            None
        );
        assert!(
            backend
                .all_conflicts()
                .expect("all conflicts after removal")
                .is_empty()
        );
        let journal = backend.journal_since(0).expect("journal after removals");
        assert!(matches!(
            journal.last().map(|entry| &entry.mutation),
            Some(DfsMetadataMutation::PutConflict {
                conflict_id,
                conflict: None,
                ..
            }) if conflict_id == &conflict_b.conflict_id
        ));
        drop(backend);

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen backend after conflict removals");
        assert!(
            reopened
                .all_conflicts()
                .expect("reopened all conflicts")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_batches_write_intent_and_receipts() {
        let root = temp_root("local_backend_batches_intent_receipts");
        let shard_id = DfsMetadataShardId::new("root");
        let file_system = file_system();
        let file = object(version("version_0"));
        let intent = DfsWriteIntentRecord {
            intent_id: "intent-batched".to_string(),
            file_system_id: file.file_system_id.clone(),
            object_id: file.object_id.clone(),
            base_version: Some(file.version.clone()),
            target_version: version("version_1"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner"), node("node_remote")],
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some("blake3:batched".to_string()),
            object_metadata: Some(file.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };
        let receipts = ["node_owner", "node_remote"]
            .into_iter()
            .map(|replica| DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file.file_system_id.clone(),
                    object_id: file.object_id.clone(),
                    committed_version: intent.target_version.clone(),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::from_legacy_named(&format!("evt_batch_{replica}"))
                        .expect("event"),
                },
                replica_node_id: node(replica),
                content_hash: intent.content_hash.clone(),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .collect::<Vec<_>>();

        {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system).expect("filesystem");
            backend.put_object(&file).expect("object");
            let entries = backend
                .put_intent_and_receipts_for_object(Some(&file), &intent, &receipts)
                .expect("batched intent receipts");
            assert_eq!(entries.len(), 3);
            assert_eq!(
                backend
                    .get_intent(&file.object_id, &intent.intent_id)
                    .expect("intent"),
                Some(intent.clone())
            );
            assert_eq!(
                backend
                    .receipts_for_version(&file.object_id, &intent.target_version)
                    .expect("receipts")
                    .len(),
                2
            );
        }

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");
        assert_eq!(
            reopened
                .get_intent(&file.object_id, &intent.intent_id)
                .expect("reopened intent"),
            Some(intent)
        );
        assert_eq!(
            reopened
                .receipts_for_version(&file.object_id, &version("version_1"))
                .expect("reopened receipts")
                .len(),
            2
        );

        cleanup(&root);
    }

    #[test]
    fn advisory_lock_state_is_journaled_idempotent_and_replica_replayable() {
        let leader_root = temp_root("advisory_lock_state_leader");
        let follower_root = temp_root("advisory_lock_state_follower");
        let snapshot_root = temp_root("advisory_lock_state_snapshot");
        let shard_id = DfsMetadataShardId::new("root");
        let mut leader =
            LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("leader");
        leader.put_file_system(&file_system()).expect("filesystem");
        let object = object(version("lock-v1"));
        leader.put_object(&object).expect("object");
        let command = DfsAdvisoryLockCommand::Acquire {
            request_id: DfsAdvisoryLockRequestId::new("acquire-a"),
            session_id: DfsAdvisoryLockSessionId::new("session-a"),
            mode: DfsAdvisoryLockMode::Exclusive,
            blocking: false,
            process_id: Some(42),
        };
        let context = DfsAdvisoryLockTransitionContext {
            authority_epoch: 3,
            now_unix_nanos: 100,
            lease_duration_nanos: 1_000,
            max_clock_skew_nanos: 0,
        };
        let applied = leader
            .apply_advisory_lock_command(
                &object.file_system_id,
                &object.object_id,
                context,
                &command,
            )
            .expect("acquire");
        assert_eq!(applied.shard_id, shard_id);
        assert_eq!(applied.durable_through_sequence, 3);
        assert!(applied.journal_entry.is_some());
        assert!(matches!(
            &applied.outcome,
            DfsAdvisoryLockOutcome::Acquired { .. }
        ));

        let retry = leader
            .apply_advisory_lock_command(
                &object.file_system_id,
                &object.object_id,
                context,
                &command,
            )
            .expect("idempotent retry");
        assert_eq!(retry.outcome, applied.outcome);
        assert!(retry.journal_entry.is_none());
        assert_eq!(retry.durable_through_sequence, 3);

        let expected_state = leader
            .get_advisory_lock_state(&object.file_system_id, &object.object_id)
            .expect("state")
            .expect("lock aggregate");
        drop(leader);
        let leader =
            LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("reopen leader");
        assert_eq!(
            leader
                .get_advisory_lock_state(&object.file_system_id, &object.object_id)
                .expect("reopened state"),
            Some(expected_state.clone())
        );

        let mut follower =
            LocalSobjMetadataBackend::new(shard_id.clone(), &follower_root).expect("follower");
        let batch = leader
            .journal_batch_since_limited(0, 16, usize::MAX)
            .expect("journal batch")
            .expect("non-empty journal batch");
        follower.apply_journal_batch(&batch).expect("replay");
        assert_eq!(
            follower
                .get_advisory_lock_state(&object.file_system_id, &object.object_id)
                .expect("follower state"),
            Some(expected_state)
        );

        let mut snapshot_replica =
            LocalSobjMetadataBackend::new(shard_id.clone(), &snapshot_root).expect("snapshot");
        snapshot_replica
            .replace_with_replicated_snapshot(
                shard_id,
                leader.journal_last_sequence(),
                leader.export_table_snapshot(),
            )
            .expect("install snapshot");
        assert_eq!(
            snapshot_replica
                .get_advisory_lock_state(&object.file_system_id, &object.object_id)
                .expect("snapshot state"),
            follower
                .get_advisory_lock_state(&object.file_system_id, &object.object_id)
                .expect("follower state")
        );

        cleanup(&leader_root);
        cleanup(&follower_root);
        cleanup(&snapshot_root);
    }

    #[test]
    fn sharded_backend_routes_lock_aggregate_with_the_stable_object() {
        let root = temp_root("sharded_backend_routes_lock_aggregate");
        let configs = vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "projects",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/projects".to_string(),
                },
            ),
        ];
        let mut backend = LocalShardedMetadataBackend::new(&root, configs).expect("sharded");
        backend.put_file_system(&file_system()).expect("filesystem");
        let mut project = object(version("project-lock-v1"));
        project.object_id =
            DfsObjectId::from_legacy_named("project_lock_object").expect("project object id");
        project.path = "/projects/repo/readme.md".to_string();
        backend.put_object(&project).expect("project object");
        let applied = backend
            .apply_advisory_lock_command(
                &project.file_system_id,
                &project.object_id,
                DfsAdvisoryLockTransitionContext {
                    authority_epoch: 1,
                    now_unix_nanos: 1,
                    lease_duration_nanos: 10,
                    max_clock_skew_nanos: 0,
                },
                &DfsAdvisoryLockCommand::Acquire {
                    request_id: DfsAdvisoryLockRequestId::new("project-acquire"),
                    session_id: DfsAdvisoryLockSessionId::new("project-session"),
                    mode: DfsAdvisoryLockMode::Shared,
                    blocking: false,
                    process_id: None,
                },
            )
            .expect("project lock");
        assert_eq!(applied.shard_id, DfsMetadataShardId::new("projects"));
        assert!(
            backend
                .get_advisory_lock_state(&project.file_system_id, &project.object_id)
                .expect("routed lock state")
                .is_some()
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_namespace_transaction_moves_lock_aggregate_with_the_stable_object() {
        let root = temp_root("sharded_namespace_transaction_moves_lock_aggregate");
        let configs = vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "projects",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/projects".to_string(),
                },
            ),
        ];
        let mut backend =
            LocalShardedMetadataBackend::new(&root, configs.clone()).expect("sharded");
        backend.put_file_system(&file_system()).expect("filesystem");
        let mut project = object(version("project-lock-move-v1"));
        project.object_id =
            DfsObjectId::from_legacy_named("project_lock_move_object").expect("project object id");
        project.path = "/projects/repo/readme.md".to_string();
        backend.put_object(&project).expect("project object");
        backend
            .apply_advisory_lock_command(
                &project.file_system_id,
                &project.object_id,
                DfsAdvisoryLockTransitionContext {
                    authority_epoch: 1,
                    now_unix_nanos: 1,
                    lease_duration_nanos: 10,
                    max_clock_skew_nanos: 0,
                },
                &DfsAdvisoryLockCommand::Acquire {
                    request_id: DfsAdvisoryLockRequestId::new("project-move-acquire"),
                    session_id: DfsAdvisoryLockSessionId::new("project-move-session"),
                    mode: DfsAdvisoryLockMode::Exclusive,
                    blocking: false,
                    process_id: None,
                },
            )
            .expect("project lock");
        let source_state = backend
            .get_advisory_lock_state(&project.file_system_id, &project.object_id)
            .expect("source lock state")
            .expect("source lock aggregate");
        let (expected_state, _) = prepare_advisory_lock_state_for_migration(&source_state)
            .expect("prepare expected migration state");

        let mut moved_project = project.clone();
        moved_project.path = "/archive/readme.md".to_string();
        backend
            .apply_namespace_transaction(&DfsNamespaceTransaction {
                transaction_id: "txn_project_lock_move".to_string(),
                file_system_id: project.file_system_id.clone(),
                objects: vec![moved_project.clone()],
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("move namespace transaction");

        assert_eq!(
            backend
                .get_object(&project.object_id)
                .expect("moved object")
                .as_ref()
                .map(|object| object.path.as_str()),
            Some(moved_project.path.as_str())
        );
        assert_eq!(
            backend
                .get_advisory_lock_state(&project.file_system_id, &project.object_id)
                .expect("moved lock state"),
            Some(expected_state.clone()),
            "a local cross-shard namespace transaction must not strand the lock aggregate on the source shard"
        );

        drop(backend);
        let reopened = LocalShardedMetadataBackend::new(&root, configs).expect("reopen sharded");
        assert_eq!(
            reopened
                .get_advisory_lock_state(&project.file_system_id, &project.object_id)
                .expect("reopened moved lock state"),
            Some(expected_state)
        );

        cleanup(&root);
    }

    #[test]
    fn membership_handoff_freeze_survives_reopen_and_blocks_new_journal_entries() {
        let root = temp_root("membership-handoff-freeze-reopen");
        let metadata_root = root.join("metadata");
        let shard_id = DfsMetadataShardId::new("root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
            .expect("metadata backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        let frozen_head = backend.journal_last_sequence();
        let freeze = backend
            .freeze_metadata_shard_for_membership_handoff(&shard_id, "handoff-freeze-reopen", 7)
            .expect("install membership freeze")
            .expect("matching local shard");
        assert_eq!(freeze.journal_sequence, frozen_head);
        assert_eq!(freeze.activated_epoch, None);
        assert!(!freeze.state_fingerprint.is_empty());

        assert!(matches!(
            backend.put_object(&object(version("version_frozen_write"))),
            Err(MetadataBackendError::MembershipHandoffFrozen {
                ref handoff_id,
                ..
            }) if handoff_id == "handoff-freeze-reopen"
        ));
        assert_eq!(backend.journal_last_sequence(), frozen_head);

        drop(backend);
        let mut reopened = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
            .expect("a frozen backend must reopen for handoff reconciliation");
        assert_eq!(
            reopened
                .metadata_shard_write_freeze(&shard_id)
                .expect("read durable freeze"),
            Some(freeze)
        );
        assert_eq!(reopened.journal_last_sequence(), frozen_head);
        assert!(matches!(
            reopened.put_object(&object(version("version_frozen_reopened_write"))),
            Err(MetadataBackendError::MembershipHandoffFrozen { .. })
        ));
        assert_eq!(reopened.journal_last_sequence(), frozen_head);

        assert!(
            reopened
                .clear_metadata_shard_membership_handoff_freeze(&shard_id, "different-handoff", 8,)
                .is_err(),
            "a completion for another handoff must not release this marker"
        );
        assert!(
            reopened
                .clear_metadata_shard_membership_handoff_freeze(
                    &shard_id,
                    "handoff-freeze-reopen",
                    9,
                )
                .is_err(),
            "a completion must bind the exact successor authority epoch"
        );
        assert!(matches!(
            reopened.put_object(&object(version("version_after_wrong_completion"))),
            Err(MetadataBackendError::MembershipHandoffFrozen { .. })
        ));

        assert!(
            reopened
                .clear_metadata_shard_membership_handoff_freeze(
                    &shard_id,
                    "handoff-freeze-reopen",
                    8,
                )
                .expect("advance handoff marker to activated generation")
        );
        assert_eq!(
            reopened
                .metadata_shard_write_freeze(&shard_id)
                .expect("read durable active generation")
                .and_then(|fence| fence.activated_epoch),
            Some(8)
        );
        reopened
            .put_object(&object(version("version_after_handoff_activation")))
            .expect("normal metadata append resumes after activation");
        assert!(reopened.journal_last_sequence() > frozen_head);

        cleanup(&root);
    }

    #[test]
    fn activated_membership_marker_does_not_bind_retiring_backend() {
        let root = temp_root("membership-handoff-nonbinding-activation");
        let metadata_root = root.join("metadata");
        let shard_id = DfsMetadataShardId::new("root");
        let mut retiring = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
            .expect("retiring metadata backend");
        retiring
            .put_file_system(&file_system())
            .expect("filesystem");
        let frozen_head = retiring.journal_last_sequence();

        retiring
            .freeze_metadata_shard_for_membership_handoff(
                &shard_id,
                "handoff-nonbinding-activation",
                17,
            )
            .expect("freeze retiring generation")
            .expect("local shard");
        assert!(
            retiring
                .mark_metadata_shard_membership_handoff_activated(
                    &shard_id,
                    "handoff-nonbinding-activation",
                    18,
                )
                .expect("durably mark activated generation")
        );
        assert_eq!(retiring.instance_authority_epoch, None);
        assert_eq!(
            retiring
                .metadata_shard_write_freeze(&shard_id)
                .expect("read activated marker")
                .and_then(|marker| marker.activated_epoch),
            Some(18)
        );
        assert!(matches!(
            retiring.put_object(&object(version("version_retiring_stays_frozen"))),
            Err(MetadataBackendError::MembershipHandoffFrozen {
                ref handoff_id,
                ..
            }) if handoff_id == "handoff-nonbinding-activation"
        ));
        assert_eq!(retiring.journal_last_sequence(), frozen_head);

        assert!(
            retiring
                .clear_metadata_shard_membership_handoff_freeze(
                    &shard_id,
                    "handoff-nonbinding-activation",
                    18,
                )
                .expect("fresh authority observer binds exact activated generation")
        );
        assert_eq!(retiring.instance_authority_epoch, Some(18));
        retiring
            .put_object(&object(version("version_after_explicit_bind")))
            .expect("explicitly rebound generation may append");

        cleanup(&root);
    }

    #[test]
    fn activated_retiring_backend_accepts_replica_ingest_without_binding_authority() {
        let root = temp_root("membership-handoff-retiring-replica-ingest");
        let shard_id = DfsMetadataShardId::new("root");
        let active_source_node_id = node("active-successor");
        let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source"))
            .expect("source metadata backend");
        let mut retiring = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("retiring"))
            .expect("retiring metadata backend");

        source
            .put_file_system(&file_system())
            .expect("source filesystem");
        let initial = source
            .journal_batch_since_limited(0, 16, usize::MAX)
            .expect("initial source batch")
            .expect("initial source entries");
        retiring
            .apply_journal_batch(&initial)
            .expect("seed retiring replica");
        assert_eq!(
            retiring.metadata_prefix_proof().expect("retiring proof"),
            source.metadata_prefix_proof().expect("source proof")
        );

        retiring
            .freeze_metadata_shard_for_membership_handoff(
                &shard_id,
                "handoff-retiring-replica-ingest",
                17,
            )
            .expect("freeze retiring generation")
            .expect("local shard");

        let replicated_object = object(version("version_retiring_replica_batch"));
        source
            .put_object(&replicated_object)
            .expect("successor object");
        let batch = source
            .journal_batch_since_limited(initial.next_after_sequence, 16, usize::MAX)
            .expect("successor journal batch")
            .expect("successor journal entries");
        let exact_permit = DfsMetadataReplicaIngestPermit::new_validated(
            file_system_id(),
            shard_id.clone(),
            active_source_node_id.clone(),
            18,
        );
        let later_current_permit = DfsMetadataReplicaIngestPermit::new_validated(
            file_system_id(),
            shard_id.clone(),
            active_source_node_id.clone(),
            20,
        );
        assert!(matches!(
            retiring.apply_journal_batch(&batch),
            Err(MetadataBackendError::MembershipHandoffFrozen { .. })
        ));
        assert!(matches!(
            retiring.apply_journal_batch_with_replica_permit(&exact_permit, &batch),
            Err(MetadataBackendError::MembershipHandoffFrozen { .. })
        ));

        assert!(
            retiring
                .mark_metadata_shard_membership_handoff_activated(
                    &shard_id,
                    "handoff-retiring-replica-ingest",
                    18,
                )
                .expect("mark successor generation activated")
        );
        assert_eq!(retiring.instance_authority_epoch, None);
        assert!(
            retiring
                .metadata_shard_authoritative_writes_frozen(&shard_id)
                .expect("author fence")
        );
        assert!(matches!(
            retiring.apply_journal_batch(&batch),
            Err(MetadataBackendError::MembershipHandoffFrozen { .. })
        ));
        let wrong_shard_permit = DfsMetadataReplicaIngestPermit::new_validated(
            file_system_id(),
            DfsMetadataShardId::new("services"),
            active_source_node_id.clone(),
            18,
        );
        assert!(matches!(
            retiring.apply_journal_batch_with_replica_permit(&wrong_shard_permit, &batch),
            Err(MetadataBackendError::Serde(_))
        ));
        let stale_epoch_permit = DfsMetadataReplicaIngestPermit::new_validated(
            file_system_id(),
            shard_id.clone(),
            active_source_node_id.clone(),
            17,
        );
        assert!(matches!(
            retiring.apply_journal_batch_with_replica_permit(&stale_epoch_permit, &batch),
            Err(MetadataBackendError::MembershipHandoffFrozen { .. })
        ));

        let foreign_file_system_id =
            DfsFileSystemId::from_legacy_named("fs_foreign_replica_ingest")
                .expect("foreign filesystem");
        let wrong_file_system_permit = DfsMetadataReplicaIngestPermit::new_validated(
            foreign_file_system_id.clone(),
            shard_id.clone(),
            active_source_node_id,
            18,
        );
        let before_rejected_batches = retiring.export_table_snapshot();
        let before_rejected_batch_head = retiring.journal_last_sequence();
        reset_metadata_begin_write_calls();

        let wrong_file_system_error = retiring
            .apply_journal_batch_with_replica_permit(&wrong_file_system_permit, &batch)
            .expect_err("foreign-filesystem permit must reject a target batch");
        assert!(
            wrong_file_system_error
                .to_string()
                .contains("permit filesystem")
        );

        let mut embedded_mismatch_batch = batch.clone();
        let DfsMetadataMutation::PutObject {
            object: Some(embedded_object),
            ..
        } = &mut embedded_mismatch_batch.entries[0].mutation
        else {
            panic!("expected replicated object mutation");
        };
        embedded_object.file_system_id = foreign_file_system_id.clone();
        let embedded_mismatch_error = retiring
            .apply_journal_batch_with_replica_permit(&exact_permit, &embedded_mismatch_batch)
            .expect_err("outer and embedded filesystem mismatch must reject");
        assert!(
            embedded_mismatch_error
                .to_string()
                .contains("object record filesystem")
        );

        let mut mixed_file_system_batch = batch.clone();
        let mut foreign_entry = mixed_file_system_batch.entries[0].clone();
        foreign_entry.sequence = foreign_entry.sequence.saturating_add(1);
        let DfsMetadataMutation::PutObject {
            file_system_id,
            object_id,
            path,
            object: Some(foreign_object),
        } = &mut foreign_entry.mutation
        else {
            panic!("expected replicated object mutation");
        };
        *file_system_id = foreign_file_system_id.clone();
        *object_id =
            DfsObjectId::from_legacy_named("obj_foreign_replica_ingest").expect("foreign object");
        *path = "docs/foreign-replica-ingest.md".to_string();
        foreign_object.file_system_id = foreign_file_system_id.clone();
        foreign_object.object_id = object_id.clone();
        foreign_object.path = path.clone();
        mixed_file_system_batch.next_after_sequence = foreign_entry.sequence;
        mixed_file_system_batch.entries.push(foreign_entry);
        let mixed_file_system_error = retiring
            .apply_journal_batch_with_replica_permit(&exact_permit, &mixed_file_system_batch)
            .expect_err("mixed-filesystem batch must reject atomically");
        assert!(
            mixed_file_system_error
                .to_string()
                .contains("outer mutation filesystem")
        );

        assert_eq!(
            metadata_begin_write_calls(),
            0,
            "filesystem validation must reject before writer-lock refresh"
        );
        assert_eq!(retiring.export_table_snapshot(), before_rejected_batches);
        assert_eq!(retiring.journal_last_sequence(), before_rejected_batch_head);
        assert_eq!(
            retiring
                .get_object(&replicated_object.object_id)
                .expect("rejected object lookup"),
            None,
            "no valid prefix of a mixed batch may materialize"
        );
        assert_eq!(
            retiring
                .apply_journal_batch_with_replica_permit(&later_current_permit, &batch)
                .expect("later current successor batch crosses older activated author fence"),
            1
        );
        assert_eq!(
            retiring.metadata_prefix_proof().expect("retiring proof"),
            source.metadata_prefix_proof().expect("successor proof")
        );
        assert_eq!(retiring.instance_authority_epoch, None);
        assert!(matches!(
            retiring.put_object(&object(version(
                "version_retiring_author_rejected_after_batch"
            ))),
            Err(MetadataBackendError::MembershipHandoffFrozen { .. })
        ));

        let snapshotted_object = {
            let mut object = object(version("version_retiring_replica_snapshot"));
            object.object_id =
                DfsObjectId::from_legacy_named("obj_retiring_replica_snapshot").expect("object");
            object.path = "docs/replica-snapshot.md".to_string();
            object
        };
        source
            .put_object(&snapshotted_object)
            .expect("successor snapshot object");
        let snapshot_sequence = source.journal_last_sequence();
        let snapshot = source.export_table_snapshot();
        assert!(matches!(
            retiring.replace_with_replicated_snapshot(
                shard_id.clone(),
                snapshot_sequence,
                snapshot.clone(),
            ),
            Err(MetadataBackendError::MembershipHandoffFrozen { .. })
        ));
        let before_rejected_snapshots = retiring.export_table_snapshot();
        let before_rejected_snapshot_head = retiring.journal_last_sequence();
        reset_metadata_begin_write_calls();

        let wrong_file_system_snapshot_error = retiring
            .replace_with_replicated_snapshot_with_replica_permit(
                &wrong_file_system_permit,
                shard_id.clone(),
                snapshot_sequence,
                snapshot.clone(),
            )
            .expect_err("foreign-filesystem permit must reject a target snapshot");
        assert!(
            wrong_file_system_snapshot_error
                .to_string()
                .contains("permit filesystem")
        );

        let mut mixed_file_system_snapshot = snapshot.clone();
        let mut foreign_snapshot_object = snapshotted_object.clone();
        foreign_snapshot_object.file_system_id = foreign_file_system_id;
        foreign_snapshot_object.object_id =
            DfsObjectId::from_legacy_named("obj_foreign_replica_snapshot")
                .expect("foreign snapshot object");
        foreign_snapshot_object.path = "docs/foreign-replica-snapshot.md".to_string();
        mixed_file_system_snapshot
            .tables
            .as_mut()
            .expect("structured snapshot tables")
            .objects
            .push(foreign_snapshot_object);
        let mixed_snapshot_fingerprint =
            dfs_metadata_table_snapshot_state_fingerprint(&mixed_file_system_snapshot)
                .expect("mixed snapshot semantic fingerprint");
        mixed_file_system_snapshot.semantic_state_fingerprint = Some(mixed_snapshot_fingerprint);
        let mixed_file_system_snapshot_error = retiring
            .replace_with_replicated_snapshot_with_replica_permit(
                &exact_permit,
                shard_id.clone(),
                snapshot_sequence,
                mixed_file_system_snapshot,
            )
            .expect_err("mixed-filesystem snapshot must reject atomically");
        assert!(
            mixed_file_system_snapshot_error
                .to_string()
                .contains("object record filesystem")
        );

        assert_eq!(
            metadata_begin_write_calls(),
            0,
            "snapshot filesystem validation must reject before writer-lock refresh"
        );
        assert_eq!(retiring.export_table_snapshot(), before_rejected_snapshots);
        assert_eq!(
            retiring.journal_last_sequence(),
            before_rejected_snapshot_head
        );
        assert_eq!(
            retiring
                .get_object(&snapshotted_object.object_id)
                .expect("rejected snapshot object lookup"),
            None,
            "a rejected mixed snapshot must not replace any tables"
        );
        retiring
            .replace_with_replicated_snapshot_with_replica_permit(
                &later_current_permit,
                shard_id.clone(),
                snapshot_sequence,
                snapshot,
            )
            .expect("later current successor snapshot crosses older activated author fence");
        assert_eq!(
            retiring.metadata_prefix_proof().expect("retiring proof"),
            source.metadata_prefix_proof().expect("snapshot proof")
        );
        assert_eq!(
            retiring
                .get_object(&snapshotted_object.object_id)
                .expect("snapshotted object"),
            Some(snapshotted_object)
        );
        assert_eq!(retiring.instance_authority_epoch, None);
        assert_eq!(
            retiring
                .metadata_shard_write_freeze(&shard_id)
                .expect("preserved activated marker")
                .and_then(|marker| marker.activated_epoch),
            Some(18),
            "snapshot repair must preserve the durable author fence"
        );
        assert!(
            retiring
                .metadata_shard_authoritative_writes_frozen(&shard_id)
                .expect("author fence after snapshot")
        );
        assert!(matches!(
            retiring.put_object(&object(version(
                "version_retiring_author_rejected_after_snapshot"
            ))),
            Err(MetadataBackendError::MembershipHandoffFrozen { .. })
        ));

        cleanup(&root);
    }

    #[test]
    fn completed_inactive_marker_can_be_replaced_by_later_reverse_rotation() {
        let root = temp_root("membership-handoff-inactive-marker-replacement");
        let metadata_root = root.join("metadata");
        let shard_id = DfsMetadataShardId::new("root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
            .expect("metadata backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        backend
            .freeze_metadata_shard_for_membership_handoff(&shard_id, "handoff-away-from-local", 31)
            .expect("freeze first retiring generation")
            .expect("local shard");
        backend
            .mark_metadata_shard_membership_handoff_activated(
                &shard_id,
                "handoff-away-from-local",
                32,
            )
            .expect("mark first handoff activated");
        assert_eq!(backend.instance_authority_epoch, None);

        // The node can later participate in a reverse rotation after the
        // control plane has advanced beyond its last locally completed marker.
        // Replacing that marker installs another pending fence; it does not
        // bind this stale instance to either generation.
        let reverse = backend
            .freeze_metadata_shard_for_membership_handoff(&shard_id, "handoff-back-to-local", 34)
            .expect("replace completed inactive marker")
            .expect("local shard");
        assert_eq!(reverse.handoff_id, "handoff-back-to-local");
        assert_eq!(reverse.retiring_epoch, 34);
        assert_eq!(reverse.activated_epoch, None);
        assert_eq!(backend.instance_authority_epoch, None);
        assert_eq!(
            backend
                .metadata_shard_write_freeze(&shard_id)
                .expect("read reverse-rotation marker"),
            Some(reverse)
        );
        assert!(matches!(
            backend.put_object(&object(version("version_reverse_rotation_stays_frozen"))),
            Err(MetadataBackendError::MembershipHandoffFrozen {
                ref handoff_id,
                ..
            }) if handoff_id == "handoff-back-to-local"
        ));

        cleanup(&root);
    }

    #[test]
    fn activated_membership_generation_keeps_independently_opened_stale_backend_fenced() {
        let root = temp_root("membership-handoff-stale-open-instance");
        let metadata_root = root.join("metadata");
        let shard_id = DfsMetadataShardId::new("root");
        let mut completing = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
            .expect("completing metadata backend");
        completing
            .put_file_system(&file_system())
            .expect("filesystem");
        let mut stale = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
            .expect("independently opened stale backend");

        completing
            .freeze_metadata_shard_for_membership_handoff(
                &shard_id,
                "handoff-cross-process-generation",
                11,
            )
            .expect("freeze retiring generation")
            .expect("local shard");
        assert!(
            completing
                .clear_metadata_shard_membership_handoff_freeze(
                    &shard_id,
                    "handoff-cross-process-generation",
                    12,
                )
                .expect("activate generation")
        );
        completing
            .put_object(&object(version("version_generation_12")))
            .expect("completing instance is rebound to generation 12");

        assert!(matches!(
            stale.put_object(&object(version("version_stale_generation_11"))),
            Err(MetadataBackendError::MembershipHandoffFrozen {
                ref handoff_id,
                ..
            }) if handoff_id == "handoff-cross-process-generation"
        ));
        stale
            .clear_metadata_shard_membership_handoff_freeze(
                &shard_id,
                "handoff-cross-process-generation",
                12,
            )
            .expect("fresh control-plane observer rebinds stale instance");
        stale
            .put_object(&object(version("version_rebound_generation_12")))
            .expect("rebound instance may write the active generation");

        // The same active marker can become the next pending generation only
        // on an instance already rebound to its exact retiring epoch.
        let next = stale
            .freeze_metadata_shard_for_membership_handoff(&shard_id, "handoff-next-generation", 12)
            .expect("install next generation freeze")
            .expect("local shard");
        assert_eq!(next.activated_epoch, None);
        assert_eq!(next.retiring_epoch, 12);

        cleanup(&root);
    }

    #[test]
    fn table_checkpoint_compacts_terminal_namespace_history_without_losing_exact_retries() {
        let root = temp_root("terminal_namespace_receipt_checkpoint");
        let shard_id = DfsMetadataShardId::new("root");
        let file_system = file_system();
        let current = object(version("version_namespace_receipt_1"));
        let mut committed = current.clone();
        committed.version = version("version_namespace_receipt_2");
        committed.mode = 0o600;

        let now = OffsetDateTime::now_utc();
        let mut pending_request = namespace_request(91, DfsNamespaceRequestStatus::Pending);
        pending_request.created_at = now;
        pending_request.updated_at = now;
        let mut completed_request = pending_request.clone();
        completed_request.status = DfsNamespaceRequestStatus::Completed;
        completed_request.result = Some(DfsNamespaceResult::Applied);
        let mut prepare = namespace_prepare(
            &format!("namespace-request:{}:commit", pending_request.operation_id),
            "root",
            &current,
            committed,
        );
        prepare.transaction.created_at = now;
        prepare.completed_request = Some(completed_request.clone());
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("canonical request-owned prepare digest");
        let transaction = prepare.transaction.clone();

        let mut backend =
            LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&current).expect("current object");
        backend
            .put_namespace_request_on_shard(&shard_id, &pending_request)
            .expect("pending request");
        backend
            .prepare_namespace_transaction(&prepare)
            .expect("hidden prepare");
        backend
            .commit_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect("commit prepare");

        let full_request_bytes = serde_json::to_vec(&completed_request)
            .expect("encode full request")
            .len();
        backend
            .compact_metadata_table_log_if_needed(0)
            .expect("force exact table checkpoint");

        assert!(
            backend
                .get_namespace_transaction(&transaction.transaction_id)
                .unwrap()
                .is_none()
        );
        assert!(
            backend
                .get_namespace_prepare(&prepare.operation_id)
                .unwrap()
                .is_none()
        );
        assert!(
            backend
                .get_namespace_request(
                    &completed_request.requester_node_id,
                    &completed_request.request_id,
                )
                .unwrap()
                .is_none()
        );
        let request_receipt = backend
            .get_namespace_request_receipt(
                &completed_request.requester_node_id,
                &completed_request.request_id,
            )
            .expect("request receipt")
            .expect("compacted request receipt");
        assert_eq!(request_receipt.result, DfsNamespaceResult::Applied);
        assert!(
            serde_json::to_vec(&request_receipt)
                .expect("encode compact request receipt")
                .len()
                < full_request_bytes,
            "terminal receipt must discard the semantic operation and recovery diagnostics"
        );
        assert_eq!(backend.tables.namespace_transaction_receipts.len(), 1);
        assert_eq!(backend.tables.namespace_prepare_receipts.len(), 1);
        assert_eq!(backend.tables.namespace_request_receipts.len(), 1);

        drop(backend);
        let mut reopened =
            LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopen checkpoint");
        assert_eq!(
            reopened
                .get_namespace_request_receipt(
                    &completed_request.requester_node_id,
                    &completed_request.request_id,
                )
                .expect("reopened request receipt"),
            Some(request_receipt)
        );

        let commit_retry = reopened
            .commit_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect("exact committed prepare retry");
        assert!(matches!(
            commit_retry.mutation,
            DfsMetadataMutation::PutNamespacePrepareReceipt { .. }
        ));
        let request_retry = reopened
            .put_namespace_request_on_shard(&shard_id, &completed_request)
            .expect("exact completed request retry");
        assert!(matches!(
            request_retry.mutation,
            DfsMetadataMutation::PutNamespaceRequestReceipt { .. }
        ));
        let transaction_retry = reopened
            .apply_namespace_transaction(&transaction)
            .expect("exact materialized transaction retry");
        assert!(matches!(
            transaction_retry.mutation,
            DfsMetadataMutation::PutNamespaceTransactionReceipt { .. }
        ));

        let mut conflicting_transaction = transaction.clone();
        conflicting_transaction.objects[0].mode ^= 0o111;
        assert!(
            reopened
                .apply_namespace_transaction(&conflicting_transaction)
                .expect_err("transaction id reuse must remain fenced")
                .to_string()
                .contains("different payload")
        );
        assert!(
            reopened
                .commit_prepared_namespace_transaction(
                    &prepare.operation_id,
                    "blake3:different-prepare",
                    prepare.owner_epoch,
                )
                .expect_err("prepare digest reuse must remain fenced")
                .to_string()
                .contains("fence mismatch")
        );
        let mut conflicting_request = completed_request;
        conflicting_request.command_digest = "blake3:different-request".to_string();
        assert!(
            reopened
                .put_namespace_request_on_shard(&shard_id, &conflicting_request)
                .expect_err("request id reuse must remain fenced")
                .to_string()
                .contains("different payload")
        );

        cleanup(&root);
    }

    #[test]
    fn bounded_namespace_receipt_retention_preserves_live_recovery_and_expires_evicted_retries() {
        let root = temp_root("bounded_namespace_receipt_retention");
        let shard_id = DfsMetadataShardId::new("root");
        let file_system = file_system();
        let current = object(version("version_namespace_retention_1"));
        let mut committed = current.clone();
        committed.version = version("version_namespace_retention_2");
        committed.mode = 0o600;

        let mut pending_current = object(version("version_namespace_pending_1"));
        pending_current.object_id =
            DfsObjectId::from_legacy_named("object_namespace_pending_retention")
                .expect("pending object");
        pending_current.path = "/pending-retention".to_string();
        let mut pending_committed = pending_current.clone();
        pending_committed.version = version("version_namespace_pending_2");

        let terminal_request = namespace_request(92, DfsNamespaceRequestStatus::Pending);
        let mut completed_request = terminal_request.clone();
        completed_request.status = DfsNamespaceRequestStatus::Completed;
        completed_request.result = Some(DfsNamespaceResult::Applied);
        let mut terminal_prepare = namespace_prepare(
            &format!("namespace-request:{}:commit", terminal_request.operation_id),
            "root",
            &current,
            committed,
        );
        terminal_prepare.completed_request = Some(completed_request.clone());
        terminal_prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&terminal_prepare)
                .expect("canonical terminal prepare digest");
        let terminal_transaction = terminal_prepare.transaction.clone();

        let pending_request = namespace_request(93, DfsNamespaceRequestStatus::Pending);
        let pending_prepare = namespace_prepare(
            "namespace-retention-still-prepared",
            "root",
            &pending_current,
            pending_committed,
        );

        let mut backend =
            LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("metadata backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&current).expect("terminal current");
        backend
            .put_object(&pending_current)
            .expect("pending current");
        backend
            .put_namespace_request_on_shard(&shard_id, &terminal_request)
            .expect("terminal request admission");
        backend
            .prepare_namespace_transaction(&terminal_prepare)
            .expect("terminal prepare");
        backend
            .commit_prepared_namespace_transaction(
                &terminal_prepare.operation_id,
                &terminal_prepare.operation_digest,
                terminal_prepare.owner_epoch,
            )
            .expect("terminal commit");
        backend
            .put_namespace_request_on_shard(&shard_id, &pending_request)
            .expect("pending request");
        backend
            .prepare_namespace_transaction(&pending_prepare)
            .expect("pending prepare");

        backend
            .compact_metadata_table_log_with_namespace_retention(
                0,
                OffsetDateTime::now_utc(),
                crate::metadata_tables::DfsNamespaceReceiptRetentionLimits {
                    max_transaction_receipts: 0,
                    max_prepare_receipts: 0,
                    max_request_receipts: 0,
                    max_age: time::Duration::days(365_000),
                },
            )
            .expect("checkpoint with zero terminal receipt capacity");

        assert!(backend.tables.namespace_transaction_receipts.is_empty());
        assert!(backend.tables.namespace_prepare_receipts.is_empty());
        assert!(backend.tables.namespace_request_receipts.is_empty());
        assert!(
            backend
                .tables
                .namespace_receipt_retention
                .transaction_floor
                .is_some()
        );
        assert!(
            backend
                .tables
                .namespace_receipt_retention
                .prepare_floor
                .is_some()
        );
        assert!(
            backend
                .tables
                .namespace_receipt_retention
                .opaque_request_history_evicted
        );
        assert_eq!(
            backend
                .get_namespace_request(
                    &pending_request.requester_node_id,
                    &pending_request.request_id,
                )
                .expect("pending request lookup"),
            Some(pending_request.clone())
        );
        assert_eq!(
            backend
                .get_namespace_prepare(&pending_prepare.operation_id)
                .expect("pending prepare lookup"),
            Some(normalized_namespace_prepare(&pending_prepare))
        );

        drop(backend);
        let mut reopened =
            LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopen checkpoint");
        assert!(
            reopened
                .tables
                .namespace_receipt_retention
                .opaque_request_history_evicted,
            "the expiry fence must survive reopen"
        );
        assert!(
            reopened
                .apply_namespace_transaction(&terminal_transaction)
                .expect_err("evicted transaction retry must not reapply")
                .to_string()
                .contains("idempotency history expired or is unknown")
        );
        assert!(
            reopened
                .commit_prepared_namespace_transaction(
                    &terminal_prepare.operation_id,
                    &terminal_prepare.operation_digest,
                    terminal_prepare.owner_epoch,
                )
                .expect_err("evicted prepare retry must not reapply")
                .to_string()
                .contains("idempotency history expired or is unknown")
        );
        assert!(
            reopened
                .put_namespace_request_on_shard(&shard_id, &completed_request)
                .expect_err("evicted request retry must not reapply")
                .to_string()
                .contains("idempotency history expired or is unknown")
        );
        reopened
            .put_namespace_request_on_shard(&shard_id, &pending_request)
            .expect("pending request remains exactly retryable");
        reopened
            .prepare_namespace_transaction(&pending_prepare)
            .expect("prepared transaction remains exactly retryable");

        cleanup(&root);
    }

    #[test]
    fn namespace_receipt_retention_evicts_oldest_by_age_then_count() {
        let mut tables = DfsMetadataTables::default();
        let fs_id = file_system_id();
        let shard_id = DfsMetadataShardId::new("root");
        for (index, day) in [(0, 0), (1, 5), (2, 9), (3, 10)] {
            if index == 3 {
                continue;
            }
            let created_at =
                OffsetDateTime::from_unix_timestamp(day * 86_400).expect("receipt timestamp");
            tables.namespace_transaction_receipts.insert(
                format!("transaction-{index}"),
                DfsNamespaceTransactionReceipt {
                    transaction_id: format!("transaction-{index}"),
                    file_system_id: fs_id.clone(),
                    transaction_digest: format!("blake3:transaction-{index}"),
                    created_at,
                    retained_at: created_at,
                },
            );
            tables.namespace_prepare_receipts.insert(
                format!("prepare-{index}"),
                DfsNamespacePrepareReceipt {
                    operation_id: format!("prepare-{index}"),
                    file_system_id: fs_id.clone(),
                    shard_id: shard_id.clone(),
                    owner_epoch: 1,
                    operation_digest: format!("blake3:prepare-{index}"),
                    transaction_id: format!("transaction-{index}"),
                    status: DfsNamespacePrepareStatus::Committed,
                    created_at,
                    retained_at: created_at,
                },
            );
            let requester_node_id = node(&format!("node_namespace_retention_{index}"));
            let request_id = format!("opaque-request-{index}");
            tables.namespace_request_receipts.insert(
                (requester_node_id.clone(), request_id.clone()),
                DfsNamespaceRequestReceipt {
                    file_system_id: fs_id.clone(),
                    requester_node_id,
                    request_id,
                    operation_id: format!("operation-{index}"),
                    authority_shard_id: shard_id.clone(),
                    command_digest: format!("blake3:request-{index}"),
                    result: DfsNamespaceResult::Applied,
                    posix_coordination_result: None,
                    created_at,
                    retained_at: created_at,
                },
            );
        }

        tables
            .compact_terminal_namespace_history(
                OffsetDateTime::from_unix_timestamp(10 * 86_400).expect("checkpoint timestamp"),
                crate::metadata_tables::DfsNamespaceReceiptRetentionLimits {
                    max_transaction_receipts: 2,
                    max_prepare_receipts: 2,
                    max_request_receipts: 2,
                    max_age: time::Duration::days(7),
                },
            )
            .expect("age checkpoint");
        assert_eq!(
            tables
                .namespace_transaction_receipts
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["transaction-1".to_string(), "transaction-2".to_string()]
        );
        assert_eq!(tables.namespace_prepare_receipts.len(), 2);
        assert_eq!(tables.namespace_request_receipts.len(), 2);

        let created_at =
            OffsetDateTime::from_unix_timestamp(10 * 86_400).expect("new receipt timestamp");
        tables.namespace_transaction_receipts.insert(
            "transaction-3".to_string(),
            DfsNamespaceTransactionReceipt {
                transaction_id: "transaction-3".to_string(),
                file_system_id: fs_id.clone(),
                transaction_digest: "blake3:transaction-3".to_string(),
                created_at,
                retained_at: created_at,
            },
        );
        tables.namespace_prepare_receipts.insert(
            "prepare-3".to_string(),
            DfsNamespacePrepareReceipt {
                operation_id: "prepare-3".to_string(),
                file_system_id: fs_id.clone(),
                shard_id: shard_id.clone(),
                owner_epoch: 1,
                operation_digest: "blake3:prepare-3".to_string(),
                transaction_id: "transaction-3".to_string(),
                status: DfsNamespacePrepareStatus::Committed,
                created_at,
                retained_at: created_at,
            },
        );
        let requester_node_id = node("node_namespace_retention_3");
        tables.namespace_request_receipts.insert(
            (requester_node_id.clone(), "opaque-request-3".to_string()),
            DfsNamespaceRequestReceipt {
                file_system_id: fs_id,
                requester_node_id,
                request_id: "opaque-request-3".to_string(),
                operation_id: "operation-3".to_string(),
                authority_shard_id: shard_id,
                command_digest: "blake3:request-3".to_string(),
                result: DfsNamespaceResult::Applied,
                posix_coordination_result: None,
                created_at,
                retained_at: created_at,
            },
        );
        tables
            .compact_terminal_namespace_history(
                created_at,
                crate::metadata_tables::DfsNamespaceReceiptRetentionLimits {
                    max_transaction_receipts: 2,
                    max_prepare_receipts: 2,
                    max_request_receipts: 2,
                    max_age: time::Duration::days(365_000),
                },
            )
            .expect("count checkpoint");

        assert_eq!(
            tables
                .namespace_transaction_receipts
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["transaction-2".to_string(), "transaction-3".to_string()],
            "count pressure must retain the newest terminal identities"
        );
        assert_eq!(
            tables
                .namespace_receipt_retention
                .transaction_floor
                .as_ref()
                .map(|key| key.identity.as_str()),
            Some("transaction-1")
        );
        assert_eq!(
            tables
                .namespace_prepare_receipts
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["prepare-2".to_string(), "prepare-3".to_string()]
        );
        assert_eq!(tables.namespace_request_receipts.len(), 2);
        assert!(
            tables
                .namespace_receipt_retention
                .opaque_request_history_evicted
        );

        let mut sortable_tables = DfsMetadataTables::default();
        let requester_node_id = node("node_sortable_namespace_retention");
        let old_request_id = "dfs_net_req_1_00000000000000000000000000".to_string();
        let retained_request_id = "dfs_net_req_2_00000000000000000000000001".to_string();
        for (request_id, second) in [
            (old_request_id.clone(), 0),
            (retained_request_id.clone(), 1),
        ] {
            sortable_tables.namespace_request_receipts.insert(
                (requester_node_id.clone(), request_id.clone()),
                DfsNamespaceRequestReceipt {
                    file_system_id: file_system_id(),
                    requester_node_id: requester_node_id.clone(),
                    request_id,
                    operation_id: format!("sortable-operation-{second}"),
                    authority_shard_id: DfsMetadataShardId::new("root"),
                    command_digest: format!("blake3:sortable-{second}"),
                    result: DfsNamespaceResult::Applied,
                    posix_coordination_result: None,
                    created_at: OffsetDateTime::from_unix_timestamp(second)
                        .expect("sortable timestamp"),
                    retained_at: OffsetDateTime::from_unix_timestamp(second)
                        .expect("sortable retention timestamp"),
                },
            );
        }
        sortable_tables
            .compact_terminal_namespace_history(
                OffsetDateTime::from_unix_timestamp(2).expect("sortable checkpoint"),
                crate::metadata_tables::DfsNamespaceReceiptRetentionLimits {
                    max_transaction_receipts: 1,
                    max_prepare_receipts: 1,
                    max_request_receipts: 1,
                    max_age: time::Duration::days(365_000),
                },
            )
            .expect("sortable checkpoint");
        assert!(sortable_tables.namespace_request_history_expired(&old_request_id));
        assert!(
            !sortable_tables.namespace_request_history_expired(&retained_request_id),
            "a retained or newer generated request identity must remain admissible"
        );
        assert!(
            !sortable_tables
                .namespace_request_history_expired("dfs_net_req_3_00000000000000000000000002"),
            "the durable floor must not reject identities newer than the evicted prefix"
        );
    }

    fn temp_root(name: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(&format!("sinomo-dfs-metadata-{name}"))
    }

    fn cleanup(root: &Path) {
        if root.exists() {
            fs::remove_dir_all(root).expect("cleanup temp dir");
        }
    }
}
