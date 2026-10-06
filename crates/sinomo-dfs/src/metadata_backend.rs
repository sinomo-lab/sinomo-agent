//! Durable DFS metadata shard backend.
//!
//! This is the first concrete metadata-plane surface. It intentionally stores
//! file/object metadata outside `sinomo-kv`: materialized shard state lives in
//! packed table batches, and every mutation is mirrored into an append-only
//! journal that can later be replicated or replayed by metadata shard peers.

#[cfg(test)]
use crate::DfsReplicationPolicy;
use crate::advisory_lock::{
    DfsAdvisoryLockApplyResult, DfsAdvisoryLockCommand, DfsAdvisoryLockStateRecord,
    DfsAdvisoryLockTransitionContext, transition_advisory_lock_state,
};
use crate::content_root_catalog::{
    DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_BYTES, DFS_CONTENT_ROOT_CATALOG_TARGET_PAGE_BYTES,
    DfsContentRootCatalogCoverage, DfsContentRootCatalogCursor, DfsContentRootCatalogEntry,
    DfsContentRootCatalogIncompleteReason, DfsContentRootCatalogPage,
    DfsContentRootCatalogPageRequest, dfs_content_root_catalog_entries_from_canonical_protection,
};
use crate::control_plane::{DfsMetadataNamespacePrepareCodec, DfsMetadataShardConfig};
use crate::metadata_api::{
    DfsMetadataBackend, DfsMetadataReplicaApplyReport, DfsMetadataReplicaIngestPermit,
    DfsMetadataShardWriteFreeze, DfsNamespaceRequestAdmissionSnapshot,
    DfsNormalizedNamespacePrepare, DfsObjectLocationUpdate, DfsValidatedNamespaceAuthorityProof,
    namespace_prepare_expectations_from_lookups,
};
use crate::metadata_cursor;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_history_gc::{
    DfsMetadataContentProtection, DfsMetadataHistoryGcOptions, DfsMetadataHistoryGcReport,
    DfsMetadataHistoryRemoval, metadata_content_protection_from_tables, plan_metadata_history_gc,
};
use crate::metadata_io::{
    create_dir_all, read_sobj_optional, remove_file_if_exists, write_sobj_atomic,
};
#[cfg(test)]
use crate::metadata_io::{read_dir, read_sobj, sync_dir};
#[cfg(test)]
use crate::metadata_journal::DfsMetadataJournalHead;
#[cfg(test)]
use crate::metadata_journal::persist_replayed_journal_entry;
use crate::metadata_journal::{
    DfsMetadataReplayCursor, bounded_metadata_journal_batch, durable_journal_entries_since_limited,
    durable_journal_entries_since_limits, journal_entries_since_limited_with_next_sequence,
    metadata_prefix_proof_after_exact_frame, migrate_legacy_journal_entries,
    persist_replayed_journal_entries, persist_replayed_journal_frames,
    read_next_journal_sequence_lenient, read_segmented_journal_frame_at_sequence,
    read_segmented_journal_frames_at_sequences, write_local_journal_entries,
    write_local_journal_entry_with_frame_bytes,
};
use crate::metadata_keys::{
    directory_page_cursor_with_dentry, normalize_path, path_record_for_object,
};
use crate::metadata_limits::{
    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, MAX_DURABLE_VERSION_PINS_PER_OBJECT,
    OBJECT_LOCAL_METADATA_PAGE_SIZE,
};
use crate::metadata_lock::{MetadataWriteLock, metadata_write_lock};
use crate::metadata_namespace_transaction::{
    namespace_transaction_recovery_is_complete, plan_namespace_transaction_recovery_table_ops,
    plan_namespace_transaction_table_ops as namespace_transaction_table_ops,
    validate_namespace_transaction_preconditions,
};
use crate::metadata_object_index::{
    object_index_table_ops_with_previous as plan_object_index_table_ops_with_previous,
    path_record_for_new_dentry,
};
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
    retained_revision_records_page_from_tables, version_pins_for_object_page_from_tables,
    version_records_page_from_tables, versions_for_object_page_from_tables,
    write_intents_page_from_tables,
};
use crate::metadata_read_snapshot;
use crate::metadata_receipt::replay_receipt_matches_existing;
use crate::metadata_records::*;
use crate::metadata_replay::{
    decoded_normalized_namespace_prepare_payload,
    decoded_normalized_namespace_prepare_payload_from_validated_ref,
    decoded_normalized_namespace_prepare_payload_with_ref, hydrate_journal_entry_payloads,
    journal_entry_has_replay_payloads, require_journal_entry_replay_payloads,
    require_replica_journal_entry_file_system, required_replay_payload,
};
use crate::metadata_summary::{
    metadata_summary_for_file_system_from_tables, metadata_summary_from_tables,
};
use crate::metadata_table_apply::{self, TableMaterializationState};
#[cfg(test)]
use crate::metadata_table_store::{
    LEGACY_METADATA_TABLE_SNAPSHOT_FORMAT_VERSION, metadata_table_snapshot_ops,
    truncate_metadata_table_log,
};
use crate::metadata_table_store::{
    METADATA_TABLE_SNAPSHOT_FORMAT_VERSION, MetadataTableStorageGeneration,
    archive_metadata_backend_state, load_or_migrate_metadata_tables,
    metadata_table_checkpoint_log_path, metadata_table_path, metadata_table_snapshot_fingerprint,
    metadata_table_snapshot_path, metadata_table_storage_generation,
    metadata_tables_from_table_snapshot, metadata_tables_snapshot, persist_metadata_table_snapshot,
    read_metadata_table_snapshot_for_replica_repair, read_metadata_table_state_lenient,
};
#[cfg(test)]
use crate::metadata_tables::namespace_prepare_footprint;
use crate::metadata_tables::{
    DfsMetadataTableRollback, DfsMetadataTables, DfsMetadataTablesSnapshot,
    DfsNamespacePrepareFootprint, MetadataTableReadSnapshotCache, MetadataTableSnapshotFingerprint,
    namespace_job_receipt_eviction_delta, namespace_prepare_receipt_eviction_delta,
    namespace_request_receipt_eviction_delta, namespace_transaction_receipt_eviction_delta,
};
use crate::metadata_tombstone::{metadata_now_unix_nanos, plan_tombstone_compaction};
use crate::metadata_visible_commit::{
    object_by_path_from_tables, visible_commit_already_materialized,
    visible_commit_record_from_parts, visible_commit_recovery_order, visible_commit_should_recover,
    visible_commit_table_ops, visible_object_location_recovery_ops,
};
use crate::{
    ContentStoreGcOptions, ContentStoreVersionGcOptions, DfsError, DfsFileSystem, DfsObjectKind,
    DfsObjectMetadata, LocalContentStore, namespace_transaction_id_for_batch,
};
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
use crate::{DfsNamespaceOperation, DfsNamespaceResult};
use sinomo_core::DfsObjectPolicy;
#[cfg(test)]
use sinomo_core::DfsWriteReceipt;
#[cfg(test)]
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
#[cfg(test)]
use sinomo_core::object::serde_ext as sinomo_object_serde;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
#[cfg(test)]
use std::io;
#[cfg(test)]
use std::io::Write;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};
use time::OffsetDateTime;

// Table-log replay is paid by every full SIFS service open. Keep the active
// append generation bounded enough that cold hydration does not spend seconds
// rebuilding a shard, while the checkpoint worker still rotates it in the
// background so foreground writes are not blocked on snapshot encoding.
const DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES: u64 = 8 * 1024 * 1024;
const METADATA_RECOVERY_WATERMARK_FORMAT_VERSION: u16 = 1;
const METADATA_RECOVERY_WATERMARK_FILE_NAME: &str = "recovery-clean.sobj";
const METADATA_RECOVERY_WATERMARK_MAX_BYTES: u64 = 16 * 1024;
const MAX_DEFERRED_REPLICA_ENTRIES: u64 = 8;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, Eq, PartialEq)]
struct DfsMetadataRecoveryFileFingerprint {
    len: u64,
    modified_unix_nanos: Option<u64>,
    created_unix_nanos: Option<u64>,
    /// File-system generation identity. On Unix, device/inode/change-time
    /// distinguishes an atomic replacement even when its length and modified
    /// timestamp were preserved. Windows uses its volume and file index;
    /// platforms without native identity retain the portable fields.
    platform_identity: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, Eq, PartialEq)]
struct DfsMetadataRecoveryTableGeneration {
    snapshot: Option<DfsMetadataRecoveryFileFingerprint>,
    checkpoint_log: Option<DfsMetadataRecoveryFileFingerprint>,
    materialized_log: Option<DfsMetadataRecoveryFileFingerprint>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, Eq, PartialEq)]
struct DfsMetadataRecoveryWatermark {
    format_version: u16,
    shard_id: DfsMetadataShardId,
    journal_next_sequence: u64,
    materialized_through_sequence: u64,
    prefix_proof: DfsMetadataPrefixProof,
    table_generation: DfsMetadataRecoveryTableGeneration,
}

#[derive(Clone, Copy, Debug, Default)]
struct MetadataRecoveryPassTimings {
    namespace_ms: u128,
    visible_ms: u128,
    location_ms: u128,
    skipped: bool,
}

struct PlannedWrittenJournalTableOps {
    applied: usize,
    last_materialized_sequence: u64,
    journal_reference_replay_cost_bytes: u64,
    table_ops: Vec<DfsMetadataTableOp>,
    post_append_ops: Vec<DfsMetadataTableOp>,
    rollback: DfsMetadataTableRollback,
}

#[derive(Default)]
struct MetadataReplayApplyEvidence {
    collect_namespace_authority_proof: bool,
    namespace_authority_proof: Option<DfsValidatedNamespaceAuthorityProof>,
}

impl MetadataReplayApplyEvidence {
    fn collecting_namespace_authority_proof() -> Self {
        Self {
            collect_namespace_authority_proof: true,
            namespace_authority_proof: None,
        }
    }
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

fn metadata_recovery_file_fingerprint(
    path: &Path,
) -> Result<Option<DfsMetadataRecoveryFileFingerprint>> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(MetadataBackendError::Io {
                path: path.to_owned(),
                source,
            });
        }
    };
    let modified_unix_nanos = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .and_then(|duration| u64::try_from(duration.as_nanos()).ok());
    let created_unix_nanos = metadata
        .created()
        .ok()
        .and_then(|created| created.duration_since(UNIX_EPOCH).ok())
        .and_then(|duration| u64::try_from(duration.as_nanos()).ok());
    #[cfg(unix)]
    let platform_identity = {
        use std::os::unix::fs::MetadataExt as _;
        Some(format!(
            "unix:{}:{}:{}:{}",
            metadata.dev(),
            metadata.ino(),
            metadata.ctime(),
            metadata.ctime_nsec()
        ))
    };
    #[cfg(windows)]
    let platform_identity = crate::metadata_io::windows_path_identity(path);
    #[cfg(not(any(unix, windows)))]
    let platform_identity = None;
    Ok(Some(DfsMetadataRecoveryFileFingerprint {
        len: metadata.len(),
        modified_unix_nanos,
        created_unix_nanos,
        platform_identity,
    }))
}

#[cfg(test)]
thread_local! {
    static METADATA_COMPACTION_BEFORE_SNAPSHOT_HOOK: std::cell::Cell<Option<Box<dyn FnOnce()>>> = const { std::cell::Cell::new(None) };
    static METADATA_BEGIN_WRITE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static METADATA_DURABLE_TABLE_RELOADS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static METADATA_CURRENT_TABLES_SNAPSHOT_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static METADATA_BACKEND_OPEN_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static METADATA_EXPORT_TABLE_SNAPSHOT_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static METADATA_JOURNAL_WRITE_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static METADATA_NAMESPACE_RECOVERY_SCAN_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static METADATA_VISIBLE_RECOVERY_SCAN_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static METADATA_LOCATION_RECOVERY_SCAN_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
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
fn reset_metadata_durable_table_reloads() {
    METADATA_DURABLE_TABLE_RELOADS.set(0);
}

#[cfg(test)]
fn metadata_durable_table_reloads() -> usize {
    METADATA_DURABLE_TABLE_RELOADS.get()
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
pub(crate) fn record_metadata_backend_open_calls_from_workers(count: usize) {
    METADATA_BACKEND_OPEN_CALLS.set(METADATA_BACKEND_OPEN_CALLS.get().saturating_add(count));
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
fn reset_metadata_recovery_scan_calls() {
    METADATA_NAMESPACE_RECOVERY_SCAN_CALLS.set(0);
    METADATA_VISIBLE_RECOVERY_SCAN_CALLS.set(0);
    METADATA_LOCATION_RECOVERY_SCAN_CALLS.set(0);
}

#[cfg(test)]
fn metadata_recovery_scan_calls() -> (usize, usize, usize) {
    (
        METADATA_NAMESPACE_RECOVERY_SCAN_CALLS.get(),
        METADATA_VISIBLE_RECOVERY_SCAN_CALLS.get(),
        METADATA_LOCATION_RECOVERY_SCAN_CALLS.get(),
    )
}

#[cfg(test)]
fn reset_namespace_prepared_commit_reference_planner_calls() {
    NAMESPACE_PREPARED_COMMIT_REFERENCE_PLANNER_CALLS.set(0);
}

#[cfg(test)]
fn reset_namespace_prepare_history_scan_calls() {
    NAMESPACE_PREPARE_HISTORY_SCAN_CALLS.set(0);
}

struct ValidatedNamespacePreparePlan {
    footprint: DfsNamespacePrepareFootprint,
}

struct NamespacePrepareTablePlan {
    ops: Vec<DfsMetadataTableOp>,
    prepared_footprint: Option<DfsNamespacePrepareFootprint>,
}

fn validate_replicated_journal_batch_entry_order(batch: &DfsMetadataJournalBatch) -> Result<()> {
    let mut previous_sequence = None;
    for (index, entry) in batch.entries.iter().enumerate() {
        if entry.shard_id != batch.shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata journal batch for shard {} contains entry from shard {}",
                batch.shard_id.0, entry.shard_id.0
            )));
        }
        if let Some(previous_sequence) = previous_sequence
            && entry.sequence <= previous_sequence
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata journal batch for shard {} is not strictly sequence-ordered at entry {index}: {} follows {}",
                batch.shard_id.0, entry.sequence, previous_sequence
            )));
        }
        previous_sequence = Some(entry.sequence);
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
struct DfsContentRootCatalogSnapshotCache {
    prefix_proof_version: u32,
    journal_sequence: u64,
    state_fingerprint: String,
    entries: Arc<Vec<DfsContentRootCatalogEntry>>,
}

impl DfsContentRootCatalogSnapshotCache {
    fn satisfies_request(&self, request: &DfsContentRootCatalogPageRequest) -> bool {
        match request.after.as_ref() {
            Some(after) => {
                self.journal_sequence == after.journal_sequence
                    && self.state_fingerprint == after.state_fingerprint
            }
            // A new catalog generation must re-observe the live durable head.
            // Reusing an old cut here could indefinitely retain post-cut
            // reservations if a quorum watermark stopped advancing.
            None => false,
        }
    }
}

fn content_root_catalog_page_from_snapshot(
    request: &DfsContentRootCatalogPageRequest,
    generated_at_unix_nanos: u64,
    snapshot: &DfsContentRootCatalogSnapshotCache,
) -> Result<DfsContentRootCatalogPage> {
    let entries = snapshot.entries.as_ref();
    let journal_sequence = snapshot.journal_sequence;
    let state_fingerprint = &snapshot.state_fingerprint;
    let start = request
        .after
        .as_ref()
        .map(|after| entries.partition_point(|entry| entry <= &after.last_entry))
        .unwrap_or(0);
    let maximum_end = start.saturating_add(request.max_entries).min(entries.len());
    let page_for_end = |end: usize| {
        let page_entries = entries[start..end].to_vec();
        let next_cursor = (end < entries.len()).then(|| DfsContentRootCatalogCursor {
            journal_sequence,
            state_fingerprint: state_fingerprint.clone(),
            generated_at_unix_nanos,
            last_entry: page_entries
                .last()
                .cloned()
                .expect("a continued content-root page contains an entry"),
        });
        DfsContentRootCatalogPage {
            identity: request.identity.clone(),
            topology_digest: request.expected_topology_digest.clone(),
            generated_at_unix_nanos,
            coverage: DfsContentRootCatalogCoverage::Complete {
                prefix_proof_version: snapshot.prefix_proof_version,
                journal_sequence,
                state_fingerprint: state_fingerprint.clone(),
            },
            entries: page_entries,
            next_cursor,
        }
    };
    let mut end = maximum_end;
    let mut page = page_for_end(end);
    let mut encoded_page_bytes = None;
    while end > start.saturating_add(1) {
        let encoded_bytes = page.encoded_size_bytes()?;
        if encoded_bytes <= DFS_CONTENT_ROOT_CATALOG_TARGET_PAGE_BYTES {
            encoded_page_bytes = Some(encoded_bytes);
            break;
        }
        let current_entries = end.saturating_sub(start);
        let scaled_entries = DFS_CONTENT_ROOT_CATALOG_TARGET_PAGE_BYTES
            .saturating_mul(current_entries)
            .checked_div(encoded_bytes)
            .unwrap_or(1)
            .clamp(1, current_entries.saturating_sub(1));
        end = start.saturating_add(scaled_entries);
        page = page_for_end(end);
    }
    let encoded_page_bytes = match encoded_page_bytes {
        Some(encoded_page_bytes) => encoded_page_bytes,
        None => page.encoded_size_bytes()?,
    };
    if encoded_page_bytes > DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_BYTES {
        return Err(MetadataBackendError::Serde(format!(
            "DFS content-root catalog page encodes to {encoded_page_bytes} bytes, exceeding the {}-byte transport budget",
            DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_BYTES
        )));
    }
    Ok(page)
}

#[derive(Clone, Debug)]
pub struct LocalSobjMetadataBackend {
    shard_id: DfsMetadataShardId,
    root: PathBuf,
    replica_progress: crate::DfsMetadataReplicaObserver,
    next_sequence: u64,
    tables: DfsMetadataTables,
    table_log_offset: u64,
    table_log_journal_reference_replay_cost_bytes: u64,
    table_log_unpriced_namespace_prepare_references: u64,
    materialized_through_sequence: u64,
    prefix_proof: Option<DfsMetadataPrefixProof>,
    /// Authority generation observed by this backend instance after a durable
    /// membership handoff activation. This is deliberately instance-local:
    /// another already-open process must not inherit the completing process's
    /// authority merely because both share the same metadata directory.
    instance_authority_epoch: Option<u64>,
    table_snapshot_fingerprint: Option<MetadataTableSnapshotFingerprint>,
    namespaced_reader_floor_ready: bool,
    table_storage_generation: Option<MetadataTableStorageGeneration>,
    read_snapshot_cache: Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
    content_root_catalog_snapshot_cache: Option<Arc<DfsContentRootCatalogSnapshotCache>>,
    content_root_catalog_previous_snapshot_cache: Option<Arc<DfsContentRootCatalogSnapshotCache>>,
    consistent_read_snapshot_depth: usize,
    /// This non-primary handle intentionally ignored an unreadable rebuildable
    /// table-log suffix. It may serve only its last self-contained snapshot
    /// until authenticated replica repair installs a new authoritative image.
    replica_repair_only: bool,
    /// Persisted prepare representation selected by the quorum-committed shard
    /// configuration. Direct and legacy backends remain fail-safe legacy.
    namespace_prepare_codec: DfsMetadataNamespacePrepareCodec,
    /// Assigned shards have a capability-validated codec. Standalone stores
    /// use this binary's semantics without a mixed-member contract.
    namespace_prepare_codec_configured: bool,
    /// Cursor values already observed by this open backend for bounded
    /// resident status. The durable sidecars remain authoritative; this
    /// projection is seeded once when the backend enters the resident pool
    /// and advanced by the ordinary cursor read/write paths. In particular,
    /// returning a mutation lease never has to rescan the replay-cursor
    /// directory or reopen every replica cursor sidecar.
    resident_cursor_status_projection: Arc<Mutex<ResidentMetadataCursorStatusProjection>>,
    #[cfg(test)]
    receipt_scan_counters: Arc<MetadataReceiptScanCounters>,
    #[cfg(test)]
    content_root_catalog_snapshot_builds: Arc<AtomicUsize>,
}

#[derive(Clone, Debug, Default)]
struct ResidentMetadataCursorStatusProjection {
    initialized: bool,
    replication_cursors: BTreeMap<(NodeId, DfsMetadataShardId), u64>,
    replay_cursors: BTreeMap<DfsMetadataShardId, u64>,
}

/// A narrow, cloneable writer for the resident replica-cursor projection.
///
/// Prepared publication releases its resident shard lease before peer I/O.
/// Keeping only this projection writer lets the post-ACK path publish the
/// committed cursor without reopening or checking out the metadata backend.
#[derive(Clone, Debug)]
pub(crate) struct ResidentMetadataReplicationCursorProjectionObserver {
    projection: Arc<Mutex<ResidentMetadataCursorStatusProjection>>,
}

impl ResidentMetadataReplicationCursorProjectionObserver {
    pub(crate) fn observe(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        sequence: u64,
    ) {
        observe_resident_metadata_replication_cursor_projection(
            &self.projection,
            target_node_id,
            shard_id,
            sequence,
        );
    }
}

fn observe_resident_metadata_replication_cursor_projection(
    projection: &Mutex<ResidentMetadataCursorStatusProjection>,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    sequence: u64,
) {
    let mut projection = projection.lock().unwrap_or_else(|error| error.into_inner());
    if projection.initialized {
        let current = projection
            .replication_cursors
            .entry((target_node_id.clone(), shard_id.clone()))
            .or_default();
        *current = (*current).max(sequence);
    }
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
            replica_progress: crate::metadata_replica_observer::subscribe(&root),
            root,
            next_sequence: 1,
            tables: DfsMetadataTables::default(),
            table_log_offset: 0,
            table_log_journal_reference_replay_cost_bytes: 0,
            table_log_unpriced_namespace_prepare_references: 0,
            materialized_through_sequence: 0,
            prefix_proof: None,
            instance_authority_epoch: None,
            table_snapshot_fingerprint: None,
            namespaced_reader_floor_ready: false,
            table_storage_generation: None,
            read_snapshot_cache: Arc::new(Mutex::new(None)),
            content_root_catalog_snapshot_cache: None,
            content_root_catalog_previous_snapshot_cache: None,
            consistent_read_snapshot_depth: 0,
            replica_repair_only: false,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::Legacy,
            namespace_prepare_codec_configured: false,
            resident_cursor_status_projection: Arc::new(Mutex::new(
                ResidentMetadataCursorStatusProjection::default(),
            )),
            #[cfg(test)]
            receipt_scan_counters: Arc::new(MetadataReceiptScanCounters::default()),
            #[cfg(test)]
            content_root_catalog_snapshot_builds: Arc::new(AtomicUsize::new(0)),
        };
        let local_recovery_started = Instant::now();
        {
            let _lock = backend.metadata_write_lock()?;
            migrate_legacy_journal_entries(&backend.root)?;
            backend.next_sequence = crate::metadata_journal::admission::certify_for_write(
                &backend.root,
                &backend.shard_id,
            )?;
            let state = load_or_migrate_metadata_tables(&backend.root)?;
            let materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
            backend.tables = state.tables;
            backend.table_log_offset = state.table_log_offset;
            backend.table_log_journal_reference_replay_cost_bytes =
                state.table_log_journal_reference_replay_cost_bytes;
            backend.table_log_unpriced_namespace_prepare_references =
                state.table_log_unpriced_namespace_prepare_references;
            backend.materialized_through_sequence = materialized_through_sequence;
            backend.prefix_proof = state.prefix_proof;
            backend.namespaced_reader_floor_ready = state.snapshot_format_version
                == Some(crate::metadata_snapshot_migration::NAMESPACED_READER_FLOOR);
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
            require_version_pin_table_invariants(&backend.tables)?;
            if bootstrapped_prefix {
                // Persist the bounded empty seed too. A deferred reader-floor
                // migration must have an exact durable prefix even when the
                // first semantic operation is still awaiting admission and
                // no compatible journal suffix ever arrives.
                backend.persist_current_metadata_checkpoint_unlocked()?;
            }
            crate::metadata_journal::admission::validate_materialized_prefix(
                &backend.root,
                &backend.shard_id,
                backend.prefix_proof.as_ref(),
            )?;
            backend.table_storage_generation =
                Some(metadata_table_storage_generation(&backend.root)?);
        }
        let local_recovery_ms = local_recovery_started.elapsed().as_millis();
        let frozen_for_membership_handoff = backend.membership_handoff_write_freeze()?.is_some();
        let recovery_timings = if frozen_for_membership_handoff {
            // Existing journal replay/materialization above is safe and
            // needed for exact snapshots. Recovery below may append new
            // authoritative records, so defer it until control-plane
            // activation explicitly rebinds this backend instance.
            MetadataRecoveryPassTimings::default()
        } else {
            backend.recover_deferred_metadata_state()?
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
            namespace_recovery_ms = recovery_timings.namespace_ms,
            visible_recovery_ms = recovery_timings.visible_ms,
            location_recovery_ms = recovery_timings.location_ms,
            recovery_watermark_hit = recovery_timings.skipped,
            total_ms = trace_started.elapsed().as_millis(),
            "metadata-backend-open"
        );
        metadata_table_apply::request_metadata_table_format_migration(&backend.root)?;
        metadata_table_apply::schedule_metadata_table_checkpoint_if_needed(
            &backend.root,
            &backend.tables,
            DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES,
            backend.table_log_offset,
            backend.table_log_journal_reference_replay_cost_bytes,
            backend.table_log_unpriced_namespace_prepare_references,
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
        let storage_generation_before = metadata_table_storage_generation(&root)?;
        let state = read_metadata_table_state_lenient(&root)?;
        let storage_generation_after = metadata_table_storage_generation(&root)?;
        let table_storage_generation = (storage_generation_before == storage_generation_after)
            .then_some(storage_generation_after);
        require_version_pin_table_invariants(&state.tables)?;
        let materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
        let next_sequence = read_next_journal_sequence_lenient(&root)?
            .max(materialized_through_sequence.saturating_add(1))
            .max(1);
        Ok(Self {
            shard_id,
            replica_progress: crate::metadata_replica_observer::subscribe(&root),
            root,
            next_sequence,
            tables: state.tables,
            table_log_offset: state.table_log_offset,
            table_log_journal_reference_replay_cost_bytes: state
                .table_log_journal_reference_replay_cost_bytes,
            table_log_unpriced_namespace_prepare_references: state
                .table_log_unpriced_namespace_prepare_references,
            materialized_through_sequence,
            prefix_proof: state.prefix_proof,
            instance_authority_epoch: None,
            namespaced_reader_floor_ready: state.snapshot_format_version
                == Some(crate::metadata_snapshot_migration::NAMESPACED_READER_FLOOR),
            table_snapshot_fingerprint: state.snapshot_fingerprint,
            table_storage_generation,
            read_snapshot_cache: Arc::new(Mutex::new(None)),
            content_root_catalog_snapshot_cache: None,
            content_root_catalog_previous_snapshot_cache: None,
            consistent_read_snapshot_depth: 0,
            replica_repair_only: false,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::Legacy,
            namespace_prepare_codec_configured: false,
            resident_cursor_status_projection: Arc::new(Mutex::new(
                ResidentMetadataCursorStatusProjection::default(),
            )),
            #[cfg(test)]
            receipt_scan_counters: Arc::new(MetadataReceiptScanCounters::default()),
            #[cfg(test)]
            content_root_catalog_snapshot_builds: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Refresh a retained read-only inspection handle from the durable table
    /// log without running writer recovery or acquiring authority. Reactive
    /// bounded migration keeps these handles across yielded continuations so
    /// a 256-record page does not rehydrate every shard catalog.
    pub(crate) fn refresh_for_inspection(&mut self) -> Result<()> {
        let storage_generation_before = metadata_table_storage_generation(&self.root)?;
        metadata_read_snapshot::ensure_table_log_cursor_generation(
            &self.root,
            &self.table_storage_generation,
            &storage_generation_before,
            self.table_log_offset,
        )?;
        metadata_read_snapshot::refresh_metadata_tables_lenient(
            &self.root,
            &mut self.tables,
            &mut self.table_log_offset,
            &mut self.table_log_journal_reference_replay_cost_bytes,
            &mut self.table_log_unpriced_namespace_prepare_references,
            &mut self.materialized_through_sequence,
            &mut self.table_snapshot_fingerprint,
            &mut self.prefix_proof,
        )?;
        self.release_obsolete_read_snapshot_cache_after_refresh();
        let storage_generation_after = metadata_table_storage_generation(&self.root)?;
        self.table_storage_generation = (storage_generation_before == storage_generation_after)
            .then_some(storage_generation_after);
        require_version_pin_table_invariants(&self.tables)?;
        self.next_sequence = read_next_journal_sequence_lenient(&self.root)?
            .max(self.materialized_through_sequence.saturating_add(1))
            .max(1);
        Ok(())
    }

    /// Refresh and pin one coherent inspection-only table image.
    ///
    /// This deliberately does not use [`DfsMetadataBackend::begin_consistent_read_snapshot`]:
    /// that writer-facing contract takes the shard write lock and may replay
    /// authoritative recovery. The caller must have constructed this handle
    /// with [`Self::open_for_inspection`].
    pub(crate) fn begin_inspection_snapshot(&mut self) -> Result<()> {
        if self.consistent_read_snapshot_depth == 0 {
            self.refresh_for_inspection()?;
        }
        self.consistent_read_snapshot_depth = self.consistent_read_snapshot_depth.saturating_add(1);
        Ok(())
    }

    pub(crate) fn end_inspection_snapshot(&mut self) {
        self.consistent_read_snapshot_depth = self.consistent_read_snapshot_depth.saturating_sub(1);
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
        let state = match read_metadata_table_state_lenient(&root) {
            Ok(state) => state,
            Err(error) => {
                tracing::warn!(
                    target: "sinomo.dfs",
                    shard = %shard_id.0,
                    root = %root.display(),
                    error = %error,
                    "opening non-primary metadata replica from its last self-contained snapshot; authenticated snapshot repair is required"
                );
                read_metadata_table_snapshot_for_replica_repair(&root)?
            }
        };
        require_version_pin_table_invariants(&state.tables)?;
        let materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
        let mut backend = Self {
            shard_id,
            replica_progress: crate::metadata_replica_observer::subscribe(&root),
            root,
            // Ignore the unreadable journal suffix until the primary either
            // sends a clean continuation or replaces it with a snapshot.
            next_sequence: materialized_through_sequence.saturating_add(1).max(1),
            tables: state.tables,
            table_log_offset: state.table_log_offset,
            table_log_journal_reference_replay_cost_bytes: state
                .table_log_journal_reference_replay_cost_bytes,
            table_log_unpriced_namespace_prepare_references: state
                .table_log_unpriced_namespace_prepare_references,
            materialized_through_sequence,
            prefix_proof: state.prefix_proof,
            instance_authority_epoch: None,
            namespaced_reader_floor_ready: state.snapshot_format_version
                == Some(crate::metadata_snapshot_migration::NAMESPACED_READER_FLOOR),
            table_snapshot_fingerprint: state.snapshot_fingerprint,
            table_storage_generation: None,
            read_snapshot_cache: Arc::new(Mutex::new(None)),
            content_root_catalog_snapshot_cache: None,
            content_root_catalog_previous_snapshot_cache: None,
            consistent_read_snapshot_depth: 0,
            replica_repair_only: true,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::Legacy,
            namespace_prepare_codec_configured: false,
            resident_cursor_status_projection: Arc::new(Mutex::new(
                ResidentMetadataCursorStatusProjection::default(),
            )),
            #[cfg(test)]
            receipt_scan_counters: Arc::new(MetadataReceiptScanCounters::default()),
            #[cfg(test)]
            content_root_catalog_snapshot_builds: Arc::new(AtomicUsize::new(0)),
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

    /// Read-only progress of this node's physical metadata migration.
    pub fn storage_migration_status(
        &self,
    ) -> Result<Option<sinomo_core::storage::migration::MigrationJob>> {
        crate::metadata_snapshot_migration::status(&self.root)
    }

    pub(crate) fn set_namespace_prepare_codec(&mut self, codec: DfsMetadataNamespacePrepareCodec) {
        self.namespace_prepare_codec = codec;
        self.namespace_prepare_codec_configured = true;
    }

    fn supports_retained_repair_owners(&self) -> bool {
        !self.namespace_prepare_codec_configured
            || self
                .namespace_prepare_codec
                .supports_retained_repair_owners()
    }

    /// Borrow a prepare from this backend's exact materialized generation.
    /// Sharded write paths use this to avoid cloning the complete transaction
    /// through the public snapshot-oriented lookup before and after a commit.
    pub(crate) fn namespace_prepare_ref(
        &self,
        operation_id: &str,
    ) -> Option<&DfsNamespacePrepareRecord> {
        self.tables.namespace_prepares.get(operation_id)
    }

    pub(crate) fn namespace_prepare_receipt_ref(
        &self,
        operation_id: &str,
    ) -> Option<&DfsNamespacePrepareReceipt> {
        self.tables.namespace_prepare_receipts.get(operation_id)
    }

    pub(crate) fn has_current_namespace_prepare(&self, operation_id: &str) -> Result<bool> {
        Ok(self
            .current_tables_snapshot()?
            .namespace_prepares
            .contains_key(operation_id))
    }

    pub(crate) fn has_current_namespace_prepare_identity(
        &self,
        operation_id: &str,
    ) -> Result<bool> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_prepares.contains_key(operation_id)
            || tables.namespace_prepare_receipts.contains_key(operation_id))
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

    pub(crate) fn persist_authority_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
        expected_target_prefix: &DfsMetadataPrefixProof,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.persist_exact_journal_frames_with_replica_permit_report(
            permit,
            batch,
            frames,
            Some(expected_target_prefix),
        )
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

    pub(crate) fn durable_journal_since_limits(
        &self,
        after_sequence: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<crate::metadata_journal::DfsDurableJournalRead> {
        let _lock = self.metadata_write_lock()?;
        durable_journal_entries_since_limits(&self.root, after_sequence, max_entries, max_bytes)
    }

    /// Export the current materialized table state for replica snapshot repair.
    pub fn export_table_snapshot(&self) -> DfsMetadataTableSnapshot {
        #[cfg(test)]
        METADATA_EXPORT_TABLE_SNAPSHOT_CALLS
            .set(METADATA_EXPORT_TABLE_SNAPSHOT_CALLS.get().saturating_add(1));
        let mut snapshot = DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            materialized_through_sequence: Some(self.materialized_through_sequence),
            tables: Some(Box::new(metadata_tables_snapshot(&self.tables))),
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
        let archived = self.replace_with_replicated_snapshot_unlocked(
            source_shard_id,
            replayed_through_sequence,
            snapshot,
        )?;
        if let Some(prefix) = self.prefix_proof.as_ref() {
            self.replica_progress.publish(permit, prefix, true);
        }
        Ok(archived)
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
        let root = root.into();
        let mut backend = Self {
            shard_id,
            replica_progress: crate::metadata_replica_observer::subscribe(&root),
            root,
            next_sequence: 1,
            tables: DfsMetadataTables::default(),
            table_log_offset: 0,
            table_log_journal_reference_replay_cost_bytes: 0,
            table_log_unpriced_namespace_prepare_references: 0,
            materialized_through_sequence: 0,
            prefix_proof: None,
            instance_authority_epoch: None,
            table_snapshot_fingerprint: None,
            namespaced_reader_floor_ready: false,
            table_storage_generation: None,
            read_snapshot_cache: Arc::new(Mutex::new(None)),
            content_root_catalog_snapshot_cache: None,
            content_root_catalog_previous_snapshot_cache: None,
            consistent_read_snapshot_depth: 0,
            replica_repair_only: false,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::Legacy,
            namespace_prepare_codec_configured: false,
            resident_cursor_status_projection: Arc::new(Mutex::new(
                ResidentMetadataCursorStatusProjection::default(),
            )),
            #[cfg(test)]
            receipt_scan_counters: Arc::new(MetadataReceiptScanCounters::default()),
            #[cfg(test)]
            content_root_catalog_snapshot_builds: Arc::new(AtomicUsize::new(0)),
        };
        let lock = backend.metadata_write_lock()?;
        if let Some(freeze) = backend.membership_handoff_write_freeze()?
            && !permit.admits_write_freeze(&freeze)
        {
            return Err(MetadataBackendError::MembershipHandoffFrozen {
                shard_id: backend.shard_id.clone(),
                handoff_id: freeze.handoff_id,
            });
        }
        let archived_existing = backend.replace_with_replicated_snapshot_unlocked(
            source_shard_id,
            replayed_through_sequence,
            snapshot,
        )?;
        if let Some(prefix) = backend.prefix_proof.as_ref() {
            backend.replica_progress.publish(permit, prefix, true);
        }
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
            return Err(MetadataBackendError::replica_diverged(format!(
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
        if let Some(floor) = authority_floor.as_ref() {
            if replayed_through_sequence < floor.journal_sequence {
                return Err(MetadataBackendError::Serde(format!(
                    "replicated metadata snapshot sequence {replayed_through_sequence} would replace shard {} behind locally acknowledged authority operation {}",
                    source_shard_id.0, floor.journal_sequence
                )));
            }
            if replayed_through_sequence == floor.journal_sequence
                && floor.durable_prefix_proof.as_ref() != Some(&replacement_prefix_proof)
            {
                return Err(MetadataBackendError::Serde(format!(
                    "replicated metadata snapshot prefix conflicts with locally acknowledged authority operation for shard {} sequence {replayed_through_sequence}",
                    source_shard_id.0
                )));
            }
        }
        if let Some(floor) = acknowledged_floor.as_ref() {
            if replayed_through_sequence < floor.journal_sequence {
                return Err(MetadataBackendError::Serde(format!(
                    "replicated metadata snapshot sequence {replayed_through_sequence} would roll shard {} behind locally acknowledged checkpoint {}",
                    source_shard_id.0, floor.journal_sequence
                )));
            }
            if replayed_through_sequence == floor.journal_sequence {
                if let Some(durable_prefix) = floor.durable_prefix_proof.as_ref()
                    && durable_prefix != &replacement_prefix_proof
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "replicated metadata snapshot journal prefix conflicts with locally acknowledged checkpoint for shard {} sequence {replayed_through_sequence}",
                        source_shard_id.0
                    )));
                }
                if !floor.state_fingerprint.is_empty() {
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
        }
        let tables = metadata_tables_from_table_snapshot(snapshot, "replicated metadata snapshot")?;
        require_version_pin_table_invariants(&tables)?;
        let preserve_reader_floor =
            crate::metadata_snapshot_migration::reader_floor_requested(&self.root)?
                || crate::metadata_snapshot_migration::reader_floor_installed(&self.root)?
                || crate::metadata_reader_floor::tables_require_floor(&tables);
        crate::metadata_journal::admission::invalidate(&self.root)?;
        self.namespaced_reader_floor_ready = false;
        let archived_existing = archive_metadata_backend_state(&self.root)?;
        create_dir_all(self.root.join("records"))?;
        if preserve_reader_floor {
            crate::metadata_snapshot_migration::request_reader_floor(&self.root)?;
        }

        if let Some(floor) = acknowledged_floor.as_ref() {
            metadata_cursor::put_metadata_prefix_ack_floor(&self.root, floor)?;
        }
        create_dir_all(self.root.join("journal").join("segments"))?;
        self.tables = tables;
        self.replica_repair_only = false;
        self.table_log_offset = 0;
        self.table_log_journal_reference_replay_cost_bytes = 0;
        self.table_log_unpriced_namespace_prepare_references = 0;
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
                shard_id: source_shard_id.clone(),
                last_sequence: replayed_through_sequence,
            },
        )?;
        self.observe_resident_metadata_replay_cursor(&source_shard_id, replayed_through_sequence);
        self.write_local_materialization_cursor(&DfsMetadataReplayCursor {
            shard_id: self.shard_id.clone(),
            last_sequence: replayed_through_sequence,
        })?;
        metadata_cursor::write_journal_head(&self.root, self.next_sequence)?;
        Ok(archived_existing)
    }

    fn append_preapplied_table_ops_for_written_entries(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
        table_sequence: u64,
        table_ops: Vec<DfsMetadataTableOp>,
        rollback: DfsMetadataTableRollback,
    ) -> Result<()> {
        self.append_preapplied_table_ops_for_written_entries_with_journal_reference_replay_cost(
            entries,
            table_sequence,
            table_ops,
            Vec::new(),
            rollback,
            0,
        )
    }

    fn append_preapplied_table_ops_for_written_entries_with_journal_reference_replay_cost(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
        table_sequence: u64,
        table_ops: Vec<DfsMetadataTableOp>,
        post_append_ops: Vec<DfsMetadataTableOp>,
        rollback: DfsMetadataTableRollback,
        journal_reference_replay_cost_bytes: u64,
    ) -> Result<()> {
        self.append_preapplied_table_ops_with_journal_reference_replay_cost_and_post_append_ops(
            table_sequence,
            table_ops,
            post_append_ops,
            journal_reference_replay_cost_bytes,
            rollback,
        )?;
        if let Some(last_sequence) = entries.last().map(|entry| entry.sequence) {
            self.checkpoint_local_materialization_cursor_if_due(last_sequence)?;
        }
        Ok(())
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
        let mut projection = self
            .resident_cursor_status_projection
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if projection.initialized {
            projection.replay_cursors = cursors
                .iter()
                .map(|cursor| {
                    (
                        cursor.source_shard_id.clone(),
                        cursor.replayed_through_sequence,
                    )
                })
                .collect();
        }
        Ok(cursors)
    }

    /// Seed the bounded resident cursor projection from the durable sidecars.
    ///
    /// This is intentionally an open/attach operation, not a status or lease
    /// return operation. Once seeded, normal cursor transitions maintain the
    /// projection incrementally.
    pub(crate) fn initialize_resident_metadata_cursor_status_projection(
        &self,
        target_node_ids: &[NodeId],
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        let mut replication_cursors = BTreeMap::new();
        for target_node_id in target_node_ids {
            replication_cursors.insert(
                (target_node_id.clone(), shard_id.clone()),
                metadata_cursor::metadata_replication_cursor(&self.root, target_node_id, shard_id)?,
            );
        }
        let replay_cursors = self
            .replay_cursor_statuses()?
            .into_iter()
            .map(|cursor| (cursor.source_shard_id, cursor.replayed_through_sequence))
            .collect();
        let mut projection = self
            .resident_cursor_status_projection
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        projection.initialized = true;
        projection.replication_cursors = replication_cursors;
        projection.replay_cursors = replay_cursors;
        Ok(())
    }

    pub(crate) fn resident_metadata_cursor_status_projection(
        &self,
        target_node_ids: &[NodeId],
        shard_id: &DfsMetadataShardId,
    ) -> Option<(Vec<(NodeId, u64)>, Vec<DfsMetadataReplayCursorStatus>)> {
        let projection = self
            .resident_cursor_status_projection
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !projection.initialized {
            return None;
        }
        let replication_cursors = target_node_ids
            .iter()
            .map(|target_node_id| {
                projection
                    .replication_cursors
                    .get(&(target_node_id.clone(), shard_id.clone()))
                    .copied()
                    .map(|sequence| (target_node_id.clone(), sequence))
            })
            .collect::<Option<Vec<_>>>()?;
        let replay_cursors = projection
            .replay_cursors
            .iter()
            .map(
                |(source_shard_id, replayed_through_sequence)| DfsMetadataReplayCursorStatus {
                    source_shard_id: source_shard_id.clone(),
                    replayed_through_sequence: *replayed_through_sequence,
                },
            )
            .collect();
        Some((replication_cursors, replay_cursors))
    }

    fn observe_resident_metadata_replication_cursor(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        sequence: u64,
    ) {
        observe_resident_metadata_replication_cursor_projection(
            &self.resident_cursor_status_projection,
            target_node_id,
            shard_id,
            sequence,
        );
    }

    pub(crate) fn resident_metadata_replication_cursor_projection_observer(
        &self,
    ) -> ResidentMetadataReplicationCursorProjectionObserver {
        ResidentMetadataReplicationCursorProjectionObserver {
            projection: Arc::clone(&self.resident_cursor_status_projection),
        }
    }

    /// Reconcile the resident replication-cursor projection to one current
    /// shard topology generation.
    ///
    /// Global membership handoff is the only path which changes the replica
    /// set of an already resident slot. Read the newly configured targets'
    /// durable sidecars once at that boundary, then keep ordinary bounded
    /// status and lease return entirely projection-backed.
    pub(crate) fn reconcile_resident_metadata_replication_cursor_status_projection(
        &self,
        target_node_ids: &[NodeId],
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        let target_node_ids = target_node_ids.iter().cloned().collect::<BTreeSet<_>>();
        let mut durable_cursors = Vec::with_capacity(target_node_ids.len());
        for target_node_id in &target_node_ids {
            durable_cursors.push((
                target_node_id.clone(),
                metadata_cursor::metadata_replication_cursor(&self.root, target_node_id, shard_id)?,
            ));
        }

        let mut projection = self
            .resident_cursor_status_projection
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !projection.initialized {
            return Ok(());
        }
        projection
            .replication_cursors
            .retain(|(target_node_id, projected_shard_id), _| {
                projected_shard_id != shard_id || target_node_ids.contains(target_node_id)
            });
        for (target_node_id, durable_sequence) in durable_cursors {
            let current = projection
                .replication_cursors
                .entry((target_node_id, shard_id.clone()))
                .or_default();
            *current = (*current).max(durable_sequence);
        }
        Ok(())
    }

    fn observe_resident_metadata_replay_cursor(
        &self,
        source_shard_id: &DfsMetadataShardId,
        sequence: u64,
    ) {
        let mut projection = self
            .resident_cursor_status_projection
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if projection.initialized {
            let current = projection
                .replay_cursors
                .entry(source_shard_id.clone())
                .or_default();
            *current = (*current).max(sequence);
        }
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
            tables: Some(Box::new(metadata_tables_snapshot(&self.tables))),
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

    #[cfg(test)]
    pub(crate) fn persist_current_metadata_checkpoint_for_test(&mut self) -> Result<()> {
        let _lock = self.begin_metadata_write_allow_frozen()?;
        self.persist_current_metadata_checkpoint_unlocked()
    }

    fn persist_current_metadata_checkpoint_unlocked(&mut self) -> Result<()> {
        persist_metadata_table_snapshot(
            &self.root,
            &self.tables,
            Some(self.materialized_through_sequence),
            self.prefix_proof.as_ref(),
        )?;
        self.table_log_offset = 0;
        self.table_log_journal_reference_replay_cost_bytes = 0;
        self.table_log_unpriced_namespace_prepare_references = 0;
        self.table_snapshot_fingerprint = metadata_table_snapshot_fingerprint(&self.root)?;
        self.record_owned_metadata_materialization()
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
            return Err(MetadataBackendError::replica_diverged(format!(
                "metadata shard {} cannot extend prefix {} with journal entry {}:{}",
                expected_shard_id.0, previous.journal_sequence, entry.shard_id.0, entry.sequence
            )));
        }
        let frame = read_segmented_journal_frame_at_sequence(&self.root, entry.sequence)?
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "metadata shard {} has no exact journal frame at sequence {}",
                    expected_shard_id.0, entry.sequence
                ))
            })?;
        metadata_prefix_proof_after_exact_frame(
            previous,
            entry,
            expected_shard_id,
            frame.exact_bytes(),
        )
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.metadata.historical_prefix",
        skip_all,
        fields(stage = "dfs.metadata.historical_prefix")
    )]
    fn verify_historical_prefix_against_durable_floor(
        &self,
        expected_prefix: &DfsMetadataPrefixProof,
        later_floor: &DfsMetadataPrefixAckFloor,
        persisted_through_sequence: u64,
        expected_shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        if &later_floor.shard_id != expected_shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} historical authority prefix cannot use durable floor for shard {}",
                expected_shard_id.0, later_floor.shard_id.0
            )));
        }
        let later_prefix = later_floor
            .durable_prefix_proof
            .as_ref()
            .filter(|proof| {
                proof.is_well_formed()
                    && proof.journal_sequence == later_floor.journal_sequence
                    && proof.journal_sequence > expected_prefix.journal_sequence
                    && proof.journal_sequence <= persisted_through_sequence
            })
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "metadata shard {} cannot authenticate historical authority prefix {} without a later durable exact prefix floor",
                    expected_shard_id.0, expected_prefix.journal_sequence
                ))
            })?;

        let mut reconstructed = expected_prefix.clone();
        while reconstructed.journal_sequence < later_prefix.journal_sequence {
            let entries = journal_entries_since_limited_with_next_sequence(
                &self.root,
                reconstructed.journal_sequence,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                later_prefix.journal_sequence.saturating_add(1),
            )?;
            if entries.is_empty() {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata shard {} durable journal cannot extend historical authority prefix {} through exact prefix floor {}",
                    expected_shard_id.0,
                    expected_prefix.journal_sequence,
                    later_prefix.journal_sequence
                )));
            }
            let before = reconstructed.journal_sequence;
            for entry in entries {
                if entry.sequence > later_prefix.journal_sequence {
                    break;
                }
                reconstructed = self.prefix_proof_after_entry_for_shard(
                    &reconstructed,
                    &entry,
                    expected_shard_id,
                )?;
            }
            if reconstructed.journal_sequence == before {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata shard {} durable journal did not advance while authenticating historical authority prefix {}",
                    expected_shard_id.0, expected_prefix.journal_sequence
                )));
            }
        }
        if &reconstructed != later_prefix {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} historical authority prefix {} does not extend to durable exact prefix floor {}",
                expected_shard_id.0,
                expected_prefix.journal_sequence,
                later_prefix.journal_sequence
            )));
        }
        Ok(())
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

    fn ensure_namespaced_reader_floor(&mut self) -> Result<()> {
        if self.namespaced_reader_floor_ready {
            return Ok(());
        }
        if crate::metadata_snapshot_migration::reader_floor_installed(&self.root)? {
            self.namespaced_reader_floor_ready = true;
            return Ok(());
        }
        crate::metadata_snapshot_migration::request_reader_floor(&self.root)?;
        let generation = metadata_table_storage_generation(&self.root)?;
        let prices_are_current = self.table_storage_generation.as_ref() == Some(&generation);
        if prices_are_current
            && crate::metadata_reader_floor::small_durable_source(
                &self.root,
                self.table_log_journal_reference_replay_cost_bytes,
                self.table_log_unpriced_namespace_prepare_references,
            )?
        {
            // Read the durable source rather than a potentially preapplied
            // working overlay. The physical read budget above is fixed.
            let state = read_metadata_table_state_lenient(&self.root)?;
            crate::metadata_snapshot_migration::install_bounded_reader_floor(
                &self.root,
                &state,
                self.prefix_proof.as_ref(),
            )?;
            self.namespaced_reader_floor_ready = true;
            self.table_snapshot_fingerprint = metadata_table_snapshot_fingerprint(&self.root)?;
            self.table_storage_generation = Some(metadata_table_storage_generation(&self.root)?);
            self.release_obsolete_read_snapshot_cache_after_refresh();
            return Ok(());
        }
        metadata_table_apply::schedule_metadata_table_checkpoint_if_needed(
            &self.root,
            &self.tables,
            DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES,
            self.table_log_offset,
            self.table_log_journal_reference_replay_cost_bytes,
            self.table_log_unpriced_namespace_prepare_references,
        )?;
        Err(MetadataBackendError::ReaderFloorUpgradePending {
            target_format: crate::metadata_snapshot_migration::NAMESPACED_READER_FLOOR,
        })
    }

    fn write_journal_unlocked(
        &mut self,
        mutation: DfsMetadataMutation,
    ) -> Result<DfsMetadataJournalEntry> {
        self.write_journal_unlocked_with_record_bytes(mutation)
            .map(|(entry, _)| entry)
    }

    fn write_journal_unlocked_with_record_bytes(
        &mut self,
        mut mutation: DfsMetadataMutation,
    ) -> Result<(DfsMetadataJournalEntry, u64)> {
        if crate::metadata_reader_floor::mutation_requires_floor(&self.tables, &mutation) {
            self.ensure_namespaced_reader_floor()?;
        }
        crate::file_history::validate_intent_publication_mutation(&self.tables, &mutation)?;
        self.record_authored_object_routing_change(&mut mutation);
        crate::metadata_journal::admission::mark_uncertain(&self.root)?;
        let (entry, record_bytes, exact_bytes) = match write_local_journal_entry_with_frame_bytes(
            &self.root,
            &self.shard_id,
            &mut self.next_sequence,
            mutation,
        ) {
            Ok(written) => written,
            Err(error) => return Err(self.recover_after_replayed_journal_persistence_error(error)),
        };
        #[cfg(test)]
        METADATA_JOURNAL_WRITE_COUNT.set(METADATA_JOURNAL_WRITE_COUNT.get().saturating_add(1));
        let previous = self.prefix_proof.clone().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "metadata shard {} has no exact prefix checkpoint",
                self.shard_id.0
            ))
        })?;
        self.prefix_proof = Some(metadata_prefix_proof_after_exact_frame(
            &previous,
            &entry,
            &self.shard_id,
            exact_bytes.as_ref(),
        )?);
        crate::metadata_journal::admission::record_certified(
            &self.root,
            &self.shard_id,
            self.next_sequence,
            self.prefix_proof.clone(),
        )?;
        Ok((entry, record_bytes))
    }

    fn write_journal_entry_and_apply_table_ops_unlocked(
        &mut self,
        mutation: DfsMetadataMutation,
        table_ops: Vec<DfsMetadataTableOp>,
    ) -> Result<DfsMetadataJournalEntry> {
        self.write_journal_entry_and_materialize_unlocked(mutation, |_, _| Ok(table_ops))
    }

    /// Persist recovered plans before the publication which may capture them.
    /// This enriches existing generations without adding retention or changing
    /// namespace heads. Prepared operations replicate this journal prefix before
    /// they can acquire publication authority. The caller holds the write lock.
    fn backfill_file_history_durability_unlocked(
        &mut self,
        candidates: Vec<DfsObjectVersionRecord>,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        if candidates.is_empty()
            || (self.namespace_prepare_codec_configured
                && !self
                    .namespace_prepare_codec
                    .supports_file_history_mutations())
        {
            return Ok(Vec::new());
        }
        let mut by_file_system = BTreeMap::<DfsFileSystemId, Vec<DfsObjectVersionRecord>>::new();
        for mut version in candidates {
            let Some(plan) = crate::content_durability::recover_from_backend(&version, self)?
            else {
                continue;
            };
            version.content_durability = Some(plan);
            crate::file_history::validate_file_version_record(&version)?;
            by_file_system
                .entry(version.file_system_id.clone())
                .or_default()
                .push(version);
        }
        let mut entries = Vec::new();
        for (file_system_id, versions) in by_file_system {
            let transaction = DfsNamespaceTransaction {
                content_reference_identity: None,
                contribution_digest: None,
                extension_expectations: Vec::new(),
                transaction_id: format!("history-durability_{}", sinomo_core::Id::generate()),
                file_system_id: file_system_id.clone(),
                objects: Vec::new(),
                versions,
                file_version_pins: None,
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::now_utc(),
            };
            validate_namespace_transaction_preconditions(&self.tables, &transaction)?;
            let entry = self.write_journal_entry_and_materialize_unlocked(
                DfsMetadataMutation::ApplyNamespaceTransaction {
                    file_system_id,
                    transaction_id: transaction.transaction_id.clone(),
                    object_count: 0,
                    operation_id: None,
                    transaction: Some(transaction.clone()),
                },
                |backend, _| {
                    Ok(namespace_transaction_table_ops(
                        &backend.tables,
                        &transaction,
                    ))
                },
            )?;
            entries.push(entry);
        }
        Ok(entries)
    }

    fn write_journal_entry_and_materialize_unlocked(
        &mut self,
        mutation: DfsMetadataMutation,
        table_ops: impl FnOnce(&mut Self, &DfsMetadataJournalEntry) -> Result<Vec<DfsMetadataTableOp>>,
    ) -> Result<DfsMetadataJournalEntry> {
        let entry = self.write_journal_unlocked(mutation)?;
        let table_ops = table_ops(self, &entry)?;
        self.append_and_apply_table_ops(entry.sequence, table_ops)?;
        self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
        Ok(entry)
    }

    fn write_namespace_prepare_and_materialize_unlocked(
        &mut self,
        prepare: &DfsNamespacePrepareRecord,
        apply_ops: Vec<DfsMetadataTableOp>,
    ) -> Result<DfsMetadataJournalEntry> {
        self.write_namespace_prepare_and_materialize_with_footprint_unlocked(
            prepare, apply_ops, None,
        )
    }

    fn namespace_prepare_journal_mutation(
        &self,
        prepare: &DfsNamespacePrepareRecord,
    ) -> Result<DfsMetadataMutation> {
        Ok(match self.namespace_prepare_codec {
            DfsMetadataNamespacePrepareCodec::Legacy => {
                DfsMetadataMutation::put_namespace_prepare(prepare)
            }
            DfsMetadataNamespacePrepareCodec::PackedV1 => {
                DfsMetadataMutation::put_namespace_prepare_packed_v1(prepare).map_err(|source| {
                    MetadataBackendError::Serde(format!(
                        "pack namespace prepare {} for journal: {source}",
                        prepare.operation_id
                    ))
                })?
            }
            DfsMetadataNamespacePrepareCodec::PackedV2 => {
                DfsMetadataMutation::put_namespace_prepare_packed_v2(prepare).map_err(|source| {
                    MetadataBackendError::Serde(format!(
                        "pack namespace prepare {} for journal: {source}",
                        prepare.operation_id
                    ))
                })?
            }
            DfsMetadataNamespacePrepareCodec::PackedV3 => {
                DfsMetadataMutation::put_namespace_prepare_packed_v3(prepare)
                    .map_err(|source| MetadataBackendError::Serde(source.to_string()))?
            }
            DfsMetadataNamespacePrepareCodec::PackedV6 => {
                DfsMetadataMutation::put_namespace_prepare_packed_v6(prepare)
                    .map_err(|source| MetadataBackendError::Serde(source.to_string()))?
            }
            DfsMetadataNamespacePrepareCodec::LegacyPackedV4
            | DfsMetadataNamespacePrepareCodec::LegacyPackedV5 => {
                return Err(MetadataBackendError::Serde("interim history codec is read-only; complete the capability-validated V6 shard upgrade".into()));
            }
        })
    }

    fn write_namespace_prepare_and_materialize_with_footprint_unlocked(
        &mut self,
        prepare: &DfsNamespacePrepareRecord,
        apply_ops: Vec<DfsMetadataTableOp>,
        prepared_footprint: Option<DfsNamespacePrepareFootprint>,
    ) -> Result<DfsMetadataJournalEntry> {
        let trace_started = Instant::now();
        let mutation = self.namespace_prepare_journal_mutation(prepare)?;
        let pack_ms = trace_started.elapsed().as_millis();
        let journal_started = Instant::now();
        let (entry, journal_record_bytes) =
            self.write_journal_unlocked_with_record_bytes(mutation)?;
        let journal_ms = journal_started.elapsed().as_millis();
        let persisted_started = Instant::now();
        let apply_op_count = apply_ops.len();
        let persisted_ops = namespace_prepare_persisted_table_ops(&entry, &apply_ops)?;
        let persisted_ms = persisted_started.elapsed().as_millis();
        let table_started = Instant::now();
        match prepared_footprint {
            Some(footprint) => self.append_and_apply_table_ops_with_namespace_prepare_footprint(
                entry.sequence,
                persisted_ops,
                apply_ops,
                journal_record_bytes,
                footprint,
            )?,
            None => self.append_and_apply_table_ops_with_persisted_ops(
                entry.sequence,
                persisted_ops,
                apply_ops,
                journal_record_bytes,
            )?,
        }
        let table_ms = table_started.elapsed().as_millis();
        let checkpoint_started = Instant::now();
        self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
        let checkpoint_ms = checkpoint_started.elapsed().as_millis();
        metadata_backend_perf_trace(format_args!(
            "metadata-write-namespace-prepare shard={} operation={} objects={} versions={} receipts={} apply_ops={} journal_bytes={} pack_ms={} journal_ms={} persisted_ms={} table_ms={} checkpoint_ms={} total_ms={}",
            self.shard_id.0,
            prepare.operation_id,
            prepare.transaction.objects.len(),
            prepare.transaction.versions.len(),
            prepare.content_receipts.len(),
            apply_op_count,
            journal_record_bytes,
            pack_ms,
            journal_ms,
            persisted_ms,
            table_ms,
            checkpoint_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(entry)
    }

    fn write_journal_entries_unlocked(
        &mut self,
        mut mutations: Vec<DfsMetadataMutation>,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        if mutations.iter().any(|mutation| {
            crate::metadata_reader_floor::mutation_requires_floor(&self.tables, mutation)
        }) {
            self.ensure_namespaced_reader_floor()?;
        }
        crate::file_history::validate_intent_publication_batch(&self.tables, &mutations)?;
        let mut preceding_objects = BTreeSet::new();
        for mutation in &mut mutations {
            if let DfsMetadataMutation::PutObject { object_id, .. }
            | DfsMetadataMutation::CommitVisibleVersion { object_id, .. } = &*mutation
            {
                if preceding_objects.contains(object_id) {
                    // The batch has not materialized its earlier object writes.
                    // Do not assert an unchanged route from that older table row.
                    mutation.record_object_routing_change(None);
                } else {
                    self.record_authored_object_routing_change(mutation);
                }
            }
            if mutation.changes_visible_object_placement() {
                preceding_objects.extend(mutation.affected_object_ids());
            }
        }
        crate::metadata_journal::admission::mark_uncertain(&self.root)?;
        let entries = match write_local_journal_entries(
            &self.root,
            &self.shard_id,
            &mut self.next_sequence,
            mutations,
        ) {
            Ok(entries) => entries,
            Err(error) => return Err(self.recover_after_replayed_journal_persistence_error(error)),
        };
        #[cfg(test)]
        METADATA_JOURNAL_WRITE_COUNT.set(
            METADATA_JOURNAL_WRITE_COUNT
                .get()
                .saturating_add(entries.len()),
        );
        self.advance_metadata_prefix_proof_unlocked(&entries)?;
        crate::metadata_journal::admission::record_certified(
            &self.root,
            &self.shard_id,
            self.next_sequence,
            self.prefix_proof.clone(),
        )?;
        Ok(entries)
    }

    fn record_authored_object_routing_change(&self, mutation: &mut DfsMetadataMutation) {
        let previous = match &*mutation {
            DfsMetadataMutation::PutObject { object_id, .. }
            | DfsMetadataMutation::CommitVisibleVersion { object_id, .. } => {
                self.tables.objects.get(object_id)
            }
            _ => return,
        };
        mutation.record_object_routing_change(previous);
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
        let cursor = self.local_materialization_cursor()?;
        self.recover_unmaterialized_local_journal_entries_from(cursor, false)
    }

    fn recover_unmaterialized_local_journal_entries_from_table_head(&mut self) -> Result<()> {
        if !self.prefix_proof.as_ref().is_some_and(|proof| {
            proof.is_well_formed() && proof.journal_sequence == self.materialized_through_sequence
        }) {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} cannot recover a live journal suffix without an exact materialized prefix at {}",
                self.shard_id.0, self.materialized_through_sequence
            )));
        }
        self.recover_unmaterialized_local_journal_entries_from(
            DfsMetadataReplayCursor {
                shard_id: self.shard_id.clone(),
                last_sequence: self.materialized_through_sequence,
            },
            true,
        )
    }

    fn checkpoint_recovered_local_materialization_cursor_if_due(
        &self,
        sequence: u64,
    ) -> Result<()> {
        // Do not use the optional hint reader here: Path::exists suppresses
        // permission and NotADirectory errors. This live recovery path must
        // distinguish an absent hint from an unreadable filesystem entry.
        match crate::metadata_io::read_sobj::<DfsMetadataReplayCursor>(
            &metadata_cursor::local_materialization_cursor_path(&self.root),
        ) {
            Ok(cursor) if cursor.shard_id == self.shard_id => {
                metadata_cursor::checkpoint_local_materialization_cursor_from_if_due(
                    &self.root, cursor, sequence,
                )
            }
            Err(MetadataBackendError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(())
            }
            Ok(_) | Err(MetadataBackendError::Serde(_)) => {
                // A malformed hint must not prevent replay from a validated
                // exact table prefix. Repair it only after that replay has
                // restored the proof; ordinary checkpoints keep the existing
                // 512-entry policy. Filesystem errors remain visible.
                self.write_local_materialization_cursor(&DfsMetadataReplayCursor {
                    shard_id: self.shard_id.clone(),
                    last_sequence: sequence,
                })
            }
            Err(error) => Err(error),
        }
    }

    fn recover_unmaterialized_local_journal_entries_from(
        &mut self,
        mut cursor: DfsMetadataReplayCursor,
        defer_cursor_checkpoint: bool,
    ) -> Result<()> {
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
            let mut index = 0usize;
            while index < entries.len() {
                if journal_entry_has_replay_payloads(&entries[index]) {
                    let start = index;
                    while index < entries.len()
                        && journal_entry_has_replay_payloads(&entries[index])
                    {
                        index = index.saturating_add(1);
                    }
                    let replayable = &entries[start..index];
                    self.advance_metadata_prefix_proof_unlocked(replayable)?;
                    if defer_cursor_checkpoint {
                        self.materialize_written_journal_entries_without_cursor_checkpoint_unlocked(
                            replayable,
                        )?;
                    } else {
                        self.materialize_written_journal_entries_unlocked(replayable)?;
                    }
                    let last_sequence = replayable
                        .last()
                        .expect("non-empty replayable metadata journal suffix")
                        .sequence;
                    if last_sequence > cursor.last_sequence {
                        cursor.last_sequence = last_sequence;
                        advanced = true;
                    }
                    continue;
                }

                // Legacy journal records can omit their replay payload. Keep
                // their compatibility materializer one record at a time so
                // the durable table proof never advances past an entry whose
                // state still has to be reconstructed from the old layout.
                let entry = &entries[index];
                self.advance_metadata_prefix_proof_unlocked(std::slice::from_ref(entry))?;
                self.materialize_local_journal_entry(entry)?;
                if entry.sequence > cursor.last_sequence {
                    cursor.last_sequence = entry.sequence;
                    advanced = true;
                }
                index = index.saturating_add(1);
            }
            if advanced {
                if self.materialized_through_sequence < cursor.last_sequence
                    && self
                        .prefix_proof
                        .as_ref()
                        .is_some_and(|proof| proof.journal_sequence == cursor.last_sequence)
                {
                    // A no-op or legacy replay may advance the exact prefix
                    // without producing table operations. Persist that proof
                    // only when the replay batch above did not already do so.
                    self.append_and_apply_table_ops(cursor.last_sequence, Vec::new())?;
                }
                self.materialized_through_sequence =
                    self.materialized_through_sequence.max(cursor.last_sequence);
                if defer_cursor_checkpoint {
                    self.checkpoint_recovered_local_materialization_cursor_if_due(
                        cursor.last_sequence,
                    )?;
                } else {
                    self.write_local_materialization_cursor(&cursor)?;
                }
            }
        }
        Ok(())
    }

    fn materialize_local_journal_entry(&mut self, entry: &DfsMetadataJournalEntry) -> Result<()> {
        crate::file_history::validate_intent_publication_mutation(&self.tables, &entry.mutation)?;
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
                let mut ops =
                    self.object_index_table_ops_with_previous(object, previous.as_ref())?;
                crate::file_history::stamp_policy_captures(
                    &self.tables,
                    &mut ops,
                    entry.created_at,
                );
                ops
            }
            DfsMetadataMutation::PutVersion { version_record, .. } => {
                let Some(version_record) = version_record.as_ref() else {
                    return Ok(());
                };
                crate::file_history::validate_retained_version_update(
                    self.tables.versions.get(&(
                        version_record.object_id.clone(),
                        version_record.version.clone(),
                    )),
                    version_record,
                )?;
                vec![DfsMetadataTableOp::PutVersion {
                    version: normalized_version_record(version_record),
                }]
            }
            DfsMetadataMutation::RemoveVersion {
                object_id, version, ..
            } => {
                if self
                    .tables
                    .version_pins
                    .keys()
                    .any(|(pin_object_id, pin_version, _)| {
                        pin_object_id == object_id && pin_version == version
                    })
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} cannot remove pinned version {object_id}/{version}",
                        entry.shard_id.0, entry.sequence
                    )));
                }
                vec![DfsMetadataTableOp::RemoveVersion {
                    object_id: object_id.clone(),
                    version: version.clone(),
                }]
            }
            DfsMetadataMutation::CaptureFileVersion { record, pin, .. } => {
                pin_validation::file_version_capture_table_ops(&self.tables, record, pin)?
            }
            DfsMetadataMutation::PutVersionPin {
                file_system_id,
                object_id,
                version,
                pin_id,
                pin,
            } => {
                validate_version_pin_payload(file_system_id, object_id, version, pin_id, pin)?;
                let pinned_version = self.get_version(object_id, version)?.ok_or_else(|| {
                    MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} pins missing version {object_id}/{version}",
                        entry.shard_id.0, entry.sequence
                    ))
                })?;
                if pinned_version.file_system_id != *file_system_id {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} pin filesystem {file_system_id} does not match version filesystem {}",
                        entry.shard_id.0, entry.sequence, pinned_version.file_system_id
                    )));
                }
                if let Some(owner) = self
                    .tables
                    .cross_shard_pin_transfer_fence(object_id, version, pin_id)
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} cannot put pin {pin_id} for {object_id}/{version} while transfer {} owns it",
                        entry.shard_id.0, entry.sequence, owner.transfer_id
                    )));
                }
                let key = (object_id.clone(), version.clone(), pin_id.clone());
                if let Some(existing) = self.tables.version_pins.get(&key)
                    && existing != pin
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} conflicts with durable version pin {pin_id} for {object_id}/{version}",
                        entry.shard_id.0, entry.sequence
                    )));
                }
                if !self.tables.version_pins.contains_key(&key) {
                    require_version_pin_capacity_for_new(&self.tables, object_id)?;
                }
                vec![DfsMetadataTableOp::PutVersionPin { pin: pin.clone() }]
            }
            DfsMetadataMutation::RemoveVersionPin {
                file_system_id,
                object_id,
                version,
                pin_id,
            } => {
                if let Some(owner) = self
                    .tables
                    .cross_shard_pin_transfer_fence(object_id, version, pin_id)
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} cannot remove pin {pin_id} for {object_id}/{version} while transfer {} owns it",
                        entry.shard_id.0, entry.sequence, owner.transfer_id
                    )));
                }
                if let Some(existing) = self.tables.version_pins.get(&(
                    object_id.clone(),
                    version.clone(),
                    pin_id.clone(),
                )) && existing.file_system_id != *file_system_id
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} removes pin {pin_id} for {object_id}/{version} with filesystem {file_system_id}, but the stored pin has filesystem {}",
                        entry.shard_id.0, entry.sequence, existing.file_system_id
                    )));
                }
                vec![DfsMetadataTableOp::RemoveVersionPin {
                    object_id: object_id.clone(),
                    version: version.clone(),
                    pin_id: pin_id.clone(),
                }]
            }
            DfsMetadataMutation::ReconcileCrossShardPinTransfer {
                file_system_id,
                object_id,
                transfer_id,
                authoritative_migration,
                versions,
                transfer,
            } => {
                if validate_cross_shard_pin_transfer_table_transition(
                    &self.tables,
                    file_system_id,
                    object_id,
                    transfer_id,
                    *authoritative_migration,
                    versions,
                    transfer,
                )? {
                    Vec::new()
                } else {
                    vec![DfsMetadataTableOp::ReconcileCrossShardPinTransfer {
                        versions: versions.clone(),
                        transfer: transfer.clone(),
                    }]
                }
            }
            DfsMetadataMutation::RemoveCrossShardPinTransfer {
                file_system_id,
                object_id,
                transfer_id,
            } => {
                if let Some(existing) = self
                    .tables
                    .cross_shard_pin_transfers
                    .get(&(object_id.clone(), transfer_id.clone()))
                    && existing.file_system_id != *file_system_id
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} removes cross-shard pin transfer {} with the wrong filesystem",
                        entry.shard_id.0, entry.sequence, transfer_id
                    )));
                }
                vec![DfsMetadataTableOp::RemoveCrossShardPinTransfer {
                    object_id: object_id.clone(),
                    transfer_id: transfer_id.clone(),
                }]
            }
            DfsMetadataMutation::FinalizeCrossShardSourceEviction {
                file_system_id,
                object_id,
                tombstone,
                removed_pins,
                removed_pin_transfers,
            } => {
                if validate_cross_shard_source_eviction_table_transition(
                    &self.tables,
                    file_system_id,
                    object_id,
                    tombstone,
                    removed_pins,
                    removed_pin_transfers,
                )? {
                    Vec::new()
                } else {
                    vec![DfsMetadataTableOp::FinalizeCrossShardSourceEviction {
                        tombstone: tombstone.clone(),
                        removed_pins: removed_pins.clone(),
                        removed_pin_transfers: removed_pin_transfers.clone(),
                    }]
                }
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
            DfsMetadataMutation::PutIntentReceipt { receipt, .. } => {
                vec![DfsMetadataTableOp::PutIntentReceipt {
                    receipt: receipt.clone(),
                }]
            }
            DfsMetadataMutation::RemoveRetainedRepairOwner {
                object_id,
                intent_id,
                ..
            } => {
                crate::file_history::validate_intent_publication_mutation(
                    &self.tables,
                    &entry.mutation,
                )?;
                vec![DfsMetadataTableOp::RemoveRetainedRepairOwner {
                    object_id: object_id.clone(),
                    intent_id: intent_id.clone(),
                }]
            }
            DfsMetadataMutation::RemoveIntentReceipt {
                object_id,
                intent_id,
                retention,
                ..
            } => {
                vec![DfsMetadataTableOp::RemoveIntentReceipt {
                    object_id: object_id.clone(),
                    intent_id: intent_id.clone(),
                    retention: retention.clone(),
                }]
            }
            DfsMetadataMutation::MergeIntentReceiptRetention {
                object_id,
                retention,
                ..
            } => {
                vec![DfsMetadataTableOp::MergeIntentReceiptRetention {
                    retention: retention.for_object(object_id),
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
            DfsMetadataMutation::RemoveVisibleCommit {
                object_id, version, ..
            } => {
                vec![DfsMetadataTableOp::RemoveVisibleCommit {
                    object_id: object_id.clone(),
                    version: version.clone(),
                }]
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
                crate::metadata_namespace_transaction::plan_namespace_transaction_table_ops_at(
                    &self.tables,
                    &transaction,
                    entry.created_at,
                )
            }
            DfsMetadataMutation::PutNamespaceTransactionReceipt { receipt, .. } => {
                vec![DfsMetadataTableOp::PutNamespaceTransactionReceipt {
                    receipt: receipt.clone(),
                }]
            }
            DfsMetadataMutation::RemoveNamespaceTransactionReceipt {
                transaction_id,
                retention,
                ..
            } => {
                vec![DfsMetadataTableOp::RemoveNamespaceTransactionReceipt {
                    transaction_id: transaction_id.clone(),
                    retention: retention.clone(),
                }]
            }
            DfsMetadataMutation::PutNamespacePrepare { .. } => {
                let Some(prepare) = decoded_normalized_namespace_prepare_payload(entry)? else {
                    return Ok(());
                };
                namespace_prepare_table_ops_for_owned_record(
                    &self.tables,
                    &self.shard_id,
                    prepare.into_owned(),
                )?
            }
            DfsMetadataMutation::CommitNamespacePrepare {
                file_system_id,
                operation_id,
                operation_digest,
                owner_epoch,
                object_ids,
            } => namespace_prepare_commit_table_ops_for_reference(
                &self.tables,
                &self.shard_id,
                file_system_id,
                operation_id,
                operation_digest,
                *owner_epoch,
                object_ids,
                entry.created_at,
            )?,
            DfsMetadataMutation::PutNamespacePrepareReceipt { receipt, .. } => {
                vec![DfsMetadataTableOp::PutNamespacePrepareReceipt {
                    receipt: receipt.clone(),
                }]
            }
            DfsMetadataMutation::RemoveNamespacePrepareReceipt {
                operation_id,
                retention,
                ..
            } => {
                vec![DfsMetadataTableOp::RemoveNamespacePrepareReceipt {
                    operation_id: operation_id.clone(),
                    retention: retention.clone(),
                }]
            }
            DfsMetadataMutation::PutNamespaceJob { job, .. } => {
                let Some(job) = job.as_ref() else {
                    return Ok(());
                };
                vec![DfsMetadataTableOp::PutNamespaceJob { job: job.clone() }]
            }
            DfsMetadataMutation::PutNamespaceJobReceipt { receipt, .. } => {
                vec![DfsMetadataTableOp::PutNamespaceJobReceipt {
                    receipt: receipt.clone(),
                }]
            }
            DfsMetadataMutation::RemoveNamespaceJobReceipt {
                job_id, retention, ..
            } => {
                vec![DfsMetadataTableOp::RemoveNamespaceJobReceipt {
                    job_id: job_id.clone(),
                    retention: retention.clone(),
                }]
            }
            DfsMetadataMutation::PutNamespaceRequest { request, .. } => {
                vec![DfsMetadataTableOp::put_namespace_request(
                    request.as_ref().clone(),
                )]
            }
            DfsMetadataMutation::PutNamespaceRequestReceipt { receipt, .. } => {
                vec![DfsMetadataTableOp::PutNamespaceRequestReceipt {
                    receipt: receipt.clone(),
                }]
            }
            DfsMetadataMutation::RemoveNamespaceRequestReceipt {
                requester_node_id,
                request_id,
                retention,
                ..
            } => {
                vec![DfsMetadataTableOp::RemoveNamespaceRequestReceipt {
                    requester_node_id: requester_node_id.clone(),
                    request_id: request_id.clone(),
                    retention: retention.clone(),
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

    fn metadata_write_lock(&self) -> Result<MetadataWriteLock> {
        metadata_write_lock(self.root.clone(), self.journal_write_lock_path())
    }

    fn reload_durable_metadata_table_state(&mut self) -> Result<()> {
        #[cfg(test)]
        METADATA_DURABLE_TABLE_RELOADS.set(METADATA_DURABLE_TABLE_RELOADS.get().saturating_add(1));
        let state = read_metadata_table_state_lenient(&self.root)?;
        self.tables = state.tables;
        self.table_log_offset = state.table_log_offset;
        self.table_log_journal_reference_replay_cost_bytes =
            state.table_log_journal_reference_replay_cost_bytes;
        self.table_log_unpriced_namespace_prepare_references =
            state.table_log_unpriced_namespace_prepare_references;
        self.materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
        self.prefix_proof = state.prefix_proof;
        self.namespaced_reader_floor_ready = state.snapshot_format_version
            == Some(crate::metadata_snapshot_migration::NAMESPACED_READER_FLOOR);
        self.table_snapshot_fingerprint = state.snapshot_fingerprint;
        self.release_obsolete_read_snapshot_cache_after_refresh();
        self.table_storage_generation = Some(metadata_table_storage_generation(&self.root)?);
        Ok(())
    }

    fn release_obsolete_read_snapshot_cache(&self) -> bool {
        self.read_snapshot_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .is_some()
    }

    fn release_obsolete_read_snapshot_cache_after_refresh(&self) {
        if self.release_obsolete_read_snapshot_cache() {
            metadata_backend_perf_trace(format_args!(
                "metadata-read-snapshot-cache-released shard={} materialized_sequence={}",
                self.shard_id.0, self.materialized_through_sequence,
            ));
        }
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.metadata.refresh",
        skip_all,
        fields(stage = "dfs.metadata.refresh")
    )]
    fn refresh_after_metadata_write_lock(&mut self) -> Result<()> {
        #[cfg(test)]
        METADATA_BEGIN_WRITE_CALLS.set(METADATA_BEGIN_WRITE_CALLS.get().saturating_add(1));
        // `next_sequence` advances only after the local journal record is
        // durably appended. Preserve that live exact tail across refresh. A
        // newly opened backend obtains the same value from the scanning
        // recovery path. Healthy writes from another handle advance the table
        // head below. Root-owned certification also exposes a durable tail
        // left by a different handle after journal success/table failure.
        let certified_next_sequence =
            crate::metadata_journal::admission::certify_for_write(&self.root, &self.shard_id)?;
        if certified_next_sequence < self.next_sequence {
            return Err(MetadataBackendError::replica_diverged(
                "resident journal owner is ahead of the replaced or regressed durable source"
                    .into(),
            ));
        }
        let durable_next_sequence = self.next_sequence.max(certified_next_sequence).max(1);
        let previous_materialized_sequence = self.materialized_through_sequence;
        self.refresh_metadata_tables()?;
        self.release_obsolete_read_snapshot_cache_after_refresh();
        let durable_next_sequence = durable_next_sequence
            .max(self.materialized_through_sequence.saturating_add(1))
            .max(1);
        let durable_journal_tail = durable_next_sequence.saturating_sub(1);
        let materialization_rewound =
            self.materialized_through_sequence < previous_materialized_sequence;
        let live_table_prefix_is_exact = self.prefix_proof.as_ref().is_some_and(|proof| {
            proof.is_well_formed() && proof.journal_sequence == self.materialized_through_sequence
        });
        if durable_journal_tail > self.materialized_through_sequence && !live_table_prefix_is_exact
        {
            // The journal is the durability boundary. A prior append may have
            // succeeded while table-batch materialization failed and rolled
            // back this live handle. Reload the exact durable table proof so a
            // same-instance retry does not carry a journal-ahead proof into
            // replay. A deferred replica suffix leaves the live table proof
            // exactly at its materialized head; reloading that same large
            // snapshot would only duplicate the resident image before the
            // bounded suffix can be materialized in one batch.
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
            // Carry that head directly into replay instead of synchronously
            // rewriting and rereading the derived hint before every suffix.
            self.recover_unmaterialized_local_journal_entries_from_table_head()?;
        } else if materialization_rewound {
            self.rewind_local_materialization_cursor_to_table_head_if_ahead()?;
            self.recover_unmaterialized_local_journal_entries()?;
        }
        if (bootstrapped_prefix || materialization_rewound)
            && (self.materialized_through_sequence > 0 || self.tables.has_materialized_state())
        {
            self.persist_current_metadata_checkpoint_unlocked()?;
        }
        crate::metadata_journal::admission::validate_materialized_prefix(
            &self.root,
            &self.shard_id,
            self.prefix_proof.as_ref(),
        )?;
        crate::metadata_journal::admission::record_certified(
            &self.root,
            &self.shard_id,
            self.next_sequence,
            self.prefix_proof.clone(),
        )?;
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
        if let Some(freeze) = self.membership_handoff_write_freeze()?
            && !permit.admits_write_freeze(&freeze)
        {
            return Err(MetadataBackendError::MembershipHandoffFrozen {
                shard_id: self.shard_id.clone(),
                handoff_id: freeze.handoff_id,
            });
        }
        self.refresh_after_metadata_write_lock()?;
        Ok(lock)
    }

    fn begin_metadata_replica_durable_ingest(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        source_shard_id: &DfsMetadataShardId,
    ) -> Result<MetadataWriteLock> {
        if permit.shard_id() != source_shard_id || source_shard_id != &self.shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata replica durable-ingest permit for filesystem {} shard {} source {} epoch {} cannot mutate backend shard {} from source shard {}",
                permit.file_system_id(),
                permit.shard_id().0,
                permit.source_node_id(),
                permit.source_epoch(),
                self.shard_id.0,
                source_shard_id.0,
            )));
        }
        let lock = self.metadata_write_lock()?;
        if let Some(freeze) = self.membership_handoff_write_freeze()?
            && !permit.admits_write_freeze(&freeze)
        {
            return Err(MetadataBackendError::MembershipHandoffFrozen {
                shard_id: self.shard_id.clone(),
                handoff_id: freeze.handoff_id,
            });
        }
        self.next_sequence =
            self.next_sequence
                .max(crate::metadata_journal::admission::certify_for_write(
                    &self.root,
                    &self.shard_id,
                )?);
        if crate::metadata_journal::admission::has_pending_semantic_floor(&self.root)? {
            // A legacy semantic floor names an exact table state. Restore it
            // before a new durable suffix can move the head beyond that state;
            // the common exact-proof path still defers table materialization.
            self.refresh_after_metadata_write_lock()?;
        }
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

    /// Adopt a table generation produced by this writer while its shard lock
    /// remains held. The canonical journal was already certified before table
    /// materialization; an owned snapshot initialization or checkpoint must not
    /// turn the following ACK into another journal decode and table replay.
    fn record_owned_metadata_materialization(&mut self) -> Result<()> {
        let generation = metadata_table_storage_generation(&self.root)?;
        crate::metadata_journal::admission::record_owned_materialization(
            &self.root,
            &self.shard_id,
            self.next_sequence,
            self.prefix_proof.clone(),
        )?;
        self.table_storage_generation = Some(generation);
        Ok(())
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
        )?;
        self.record_owned_metadata_materialization()
    }

    fn append_and_apply_table_ops_with_persisted_ops(
        &mut self,
        sequence: u64,
        persisted_ops: Vec<DfsMetadataTableOp>,
        apply_ops: Vec<DfsMetadataTableOp>,
        journal_reference_replay_cost_bytes: u64,
    ) -> Result<()> {
        let root = self.root.clone();
        metadata_table_apply::append_and_apply_table_ops_with_persisted_ops(
            &root,
            self.table_materialization_state(),
            sequence,
            persisted_ops,
            apply_ops,
            Some(journal_reference_replay_cost_bytes),
            DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES,
        )?;
        self.record_owned_metadata_materialization()
    }

    fn append_and_apply_table_ops_with_namespace_prepare_footprint(
        &mut self,
        sequence: u64,
        persisted_ops: Vec<DfsMetadataTableOp>,
        apply_ops: Vec<DfsMetadataTableOp>,
        journal_reference_replay_cost_bytes: u64,
        footprint: DfsNamespacePrepareFootprint,
    ) -> Result<()> {
        let root = self.root.clone();
        metadata_table_apply::append_and_apply_table_ops_with_namespace_prepare_footprint(
            &root,
            self.table_materialization_state(),
            sequence,
            persisted_ops,
            apply_ops,
            Some(journal_reference_replay_cost_bytes),
            DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES,
            footprint,
        )?;
        self.record_owned_metadata_materialization()
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
        )?;
        self.record_owned_metadata_materialization()
    }

    fn append_preapplied_table_ops_with_journal_reference_replay_cost(
        &mut self,
        sequence: u64,
        ops: Vec<DfsMetadataTableOp>,
        journal_reference_replay_cost_bytes: u64,
        rollback: DfsMetadataTableRollback,
    ) -> Result<()> {
        if journal_reference_replay_cost_bytes == 0 {
            return self.append_preapplied_table_ops(sequence, ops, rollback);
        }
        let root = self.root.clone();
        metadata_table_apply::append_preapplied_table_ops_with_journal_reference_replay_cost(
            &root,
            self.table_materialization_state(),
            sequence,
            ops,
            journal_reference_replay_cost_bytes,
            DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES,
            rollback,
        )?;
        self.record_owned_metadata_materialization()
    }

    fn append_preapplied_table_ops_with_journal_reference_replay_cost_and_post_append_ops(
        &mut self,
        sequence: u64,
        ops: Vec<DfsMetadataTableOp>,
        mut post_append_ops: Vec<DfsMetadataTableOp>,
        journal_reference_replay_cost_bytes: u64,
        rollback: DfsMetadataTableRollback,
    ) -> Result<()> {
        if post_append_ops.is_empty() {
            return self.append_preapplied_table_ops_with_journal_reference_replay_cost(
                sequence,
                ops,
                journal_reference_replay_cost_bytes,
                rollback,
            );
        }
        if post_append_ops.len() != 1 {
            rollback.rollback(&mut self.tables);
            return Err(MetadataBackendError::Serde(format!(
                "metadata table batch {sequence} cannot defer more than one post-append operation"
            )));
        }
        let trailing_apply_op = post_append_ops
            .pop()
            .expect("one post-append operation checked above");
        let root = self.root.clone();
        metadata_table_apply::append_preapplied_table_ops_then_apply_trailing_resolved_op(
            &root,
            self.table_materialization_state(),
            sequence,
            ops,
            trailing_apply_op,
            journal_reference_replay_cost_bytes,
            DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES,
            rollback,
        )?;
        self.record_owned_metadata_materialization()
    }

    fn compact_metadata_table_log_if_needed(&mut self, max_log_bytes: u64) -> Result<()> {
        #[cfg(test)]
        if let Some(hook) = METADATA_COMPACTION_BEFORE_SNAPSHOT_HOOK.take() {
            hook();
        }
        let root = self.root.clone();
        metadata_table_apply::compact_metadata_table_log_if_needed(
            &root,
            self.table_materialization_state(),
            max_log_bytes,
        )?;
        self.record_owned_metadata_materialization()
    }

    fn table_materialization_state(&mut self) -> TableMaterializationState<'_> {
        TableMaterializationState {
            tables: &mut self.tables,
            table_log_offset: &mut self.table_log_offset,
            table_log_journal_reference_replay_cost_bytes: &mut self
                .table_log_journal_reference_replay_cost_bytes,
            table_log_unpriced_namespace_prepare_references: &mut self
                .table_log_unpriced_namespace_prepare_references,
            materialized_through_sequence: &mut self.materialized_through_sequence,
            table_snapshot_fingerprint: &mut self.table_snapshot_fingerprint,
            prefix_proof: &mut self.prefix_proof,
        }
    }

    fn refresh_metadata_tables(&mut self) -> Result<()> {
        if self.replica_repair_only {
            return Ok(());
        }
        let current_storage_generation = metadata_table_storage_generation(&self.root)?;
        metadata_read_snapshot::ensure_table_log_cursor_generation(
            &self.root,
            &self.table_storage_generation,
            &current_storage_generation,
            self.table_log_offset,
        )?;
        if self
            .table_storage_generation
            .as_ref()
            .is_some_and(|loaded| loaded.snapshot_source_changed(&current_storage_generation))
        {
            // Equal sequence/length/mtime is not an immutable checkpoint identity.
            // A replaced source must refresh its actual tables before admission.
            return self.reload_durable_metadata_table_state();
        }
        let live_borrow_kind = self.table_storage_generation.as_ref().and_then(|loaded| {
            loaded.live_table_borrow_kind(&current_storage_generation, self.table_log_offset)
        });
        if let Some(live_borrow_kind) = live_borrow_kind
            && self.prefix_proof.as_ref().is_some_and(|proof| {
                proof.is_well_formed()
                    && proof.journal_sequence == self.materialized_through_sequence
            })
        {
            // A checkpoint worker may have atomically renamed the exact active
            // log represented by these live tables into the immutable
            // checkpoint pathname. The storage-generation proof above binds
            // that rename by file identity and exact length. Re-adopt the new
            // representation instead of decoding the rotated log (and every
            // large namespace-prepare reference in it) before the next write.
            if live_borrow_kind
                == crate::metadata_table_store::MetadataTableLiveBorrowKind::CheckpointRotation
            {
                // The represented prefix now lives in the immutable checkpoint
                // log, while the current active log is proven empty. Rebase all
                // active-suffix accounting before the next append; retaining
                // the pre-rotation offset or replay pressure would double-count
                // the checkpoint body and immediately schedule another fold.
                self.table_log_offset = 0;
                self.table_log_journal_reference_replay_cost_bytes = 0;
                self.table_log_unpriced_namespace_prepare_references = 0;
            }
            self.table_storage_generation = Some(current_storage_generation);
            return Ok(());
        }
        if self
            .table_storage_generation
            .as_ref()
            .is_some_and(|loaded| !loaded.is_compatible_with(&current_storage_generation))
        {
            return self.reload_durable_metadata_table_state();
        }
        metadata_read_snapshot::refresh_metadata_tables(
            &self.root,
            &mut self.tables,
            &mut self.table_log_offset,
            &mut self.table_log_journal_reference_replay_cost_bytes,
            &mut self.table_log_unpriced_namespace_prepare_references,
            &mut self.materialized_through_sequence,
            &mut self.table_snapshot_fingerprint,
            &mut self.prefix_proof,
        )?;
        self.table_storage_generation = Some(metadata_table_storage_generation(&self.root)?);
        Ok(())
    }

    pub(crate) fn current_tables_snapshot(&self) -> Result<DfsMetadataTablesSnapshot<'_>> {
        #[cfg(test)]
        METADATA_CURRENT_TABLES_SNAPSHOT_CALLS.set(
            METADATA_CURRENT_TABLES_SNAPSHOT_CALLS
                .get()
                .saturating_add(1),
        );
        if self.consistent_read_snapshot_depth > 0 || self.replica_repair_only {
            return Ok(DfsMetadataTablesSnapshot::Borrowed(&self.tables));
        }
        metadata_read_snapshot::current_tables_snapshot(
            &self.root,
            &self.tables,
            self.table_log_offset,
            self.materialized_through_sequence,
            &self.table_snapshot_fingerprint,
            &self.table_storage_generation,
            &self.prefix_proof,
            &self.read_snapshot_cache,
        )
    }

    /// Read the operator projection owned by this already-resident table
    /// generation. The projection is maintained while table operations are
    /// applied, so this never walks the materialized metadata maps.
    pub(crate) fn resident_metadata_status_summary(&self) -> DfsMetadataSummary {
        self.tables.metadata_status_summary()
    }

    pub(crate) fn cached_table_generation(
        &self,
    ) -> (
        u64,
        u64,
        Option<MetadataTableSnapshotFingerprint>,
        Option<DfsMetadataPrefixProof>,
    ) {
        (
            self.table_log_offset,
            self.materialized_through_sequence,
            self.table_snapshot_fingerprint.clone(),
            self.prefix_proof.clone(),
        )
    }

    /// IDs in this handle's current materialized cut. A durable-view refresh
    /// uses them to reconcile indexes without exporting or hashing all records.
    pub(crate) fn cached_object_ids(&self) -> impl Iterator<Item = &DfsObjectId> {
        self.tables.objects.keys()
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

    /// Hold the source shard writer fence while capturing one complete object
    /// family. The returned lock must remain alive through target install,
    /// authority-index publication, and conditional source eviction.
    pub(crate) fn lock_and_capture_object_shard_migration_bundle(
        &mut self,
        object_id: &DfsObjectId,
        page_size: usize,
    ) -> Result<(
        MetadataWriteLock,
        crate::metadata_shard_migration::DfsObjectShardMigrationBundle,
    )> {
        let lock = self.begin_metadata_write()?;
        let bundle = crate::metadata_shard_migration::collect_object_shard_migration_bundle(
            self, object_id, page_size,
        )?;
        Ok((lock, bundle))
    }

    pub(crate) fn capture_cross_shard_source_pin_fence(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsCrossShardVersionPinFence> {
        let _lock = self.begin_metadata_write()?;
        let source_object = self.tables.objects.get(object_id).cloned().ok_or_else(|| {
            MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            }
        })?;
        Ok(DfsCrossShardVersionPinFence {
            source_object,
            source_journal_sequence: self.journal_last_sequence(),
            version_pins: self
                .tables
                .version_pins
                .values()
                .filter(|pin| &pin.object_id == object_id)
                .cloned()
                .collect(),
        })
    }

    pub(crate) fn capture_cross_shard_source_family(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsCrossShardSourceFamily> {
        let (_lock, bundle) = self.lock_and_capture_object_shard_migration_bundle(
            object_id,
            OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        Ok(bundle.into())
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
        pin_transfer_id: Option<&str>,
    ) -> Result<DfsConditionalSourceEviction> {
        let lock = self.begin_metadata_write()?;
        self.put_cross_shard_tombstone_if_unchanged_while_locked(
            &lock,
            expected_source,
            target_object,
            source_shard_id,
            target_shard_id,
            expected_journal_sequence,
            pin_transfer_id,
        )
    }

    pub(crate) fn put_cross_shard_tombstone_if_unchanged_while_locked(
        &mut self,
        _lock: &MetadataWriteLock,
        expected_source: &DfsObjectMetadata,
        target_object: &DfsObjectMetadata,
        source_shard_id: &DfsMetadataShardId,
        target_shard_id: &DfsMetadataShardId,
        expected_journal_sequence: u64,
        pin_transfer_id: Option<&str>,
    ) -> Result<DfsConditionalSourceEviction> {
        self.put_cross_shard_tombstone_if_unchanged_with_allowed_fence(
            expected_source,
            target_object,
            source_shard_id,
            target_shard_id,
            expected_journal_sequence,
            pin_transfer_id,
            None,
        )
    }

    /// The layout owner can evict only under its exact durable reservation.
    /// Ordinary eviction cannot bypass another operation's prepared footprint.
    pub(crate) fn put_cross_shard_tombstone_if_unchanged_with_fence(
        &mut self,
        expected_source: &DfsObjectMetadata,
        target_object: &DfsObjectMetadata,
        source_shard_id: &DfsMetadataShardId,
        target_shard_id: &DfsMetadataShardId,
        expected_journal_sequence: u64,
        own_fence: &DfsNamespacePrepareRecord,
    ) -> Result<DfsConditionalSourceEviction> {
        let _lock = self.begin_metadata_write()?;
        own_fence
            .validate_default_layout_migration_fence()
            .map_err(MetadataBackendError::Serde)?;
        if !crate::metadata_records::is_default_layout_migration_fence(&own_fence.operation_id)
            || own_fence.status != DfsNamespacePrepareStatus::Prepared
            || own_fence.file_system_id != expected_source.file_system_id
            || own_fence.shard_id != *source_shard_id
            || self.shard_id != *source_shard_id
            || own_fence.expected_object_versions[0].object_id != expected_source.object_id
            || own_fence.expected_object_versions[0]
                .expected_version
                .as_ref()
                != Some(&expected_source.version)
            || crate::metadata_keys::normalize_path(&own_fence.expected_path_occupants[0].path)
                != crate::metadata_keys::normalize_path(&expected_source.path)
            || self.tables.namespace_prepares.get(&own_fence.operation_id)
                != Some(&crate::metadata_records::normalized_namespace_prepare(
                    own_fence,
                ))
        {
            return Err(MetadataBackendError::Serde(
                "source eviction lacks its exact active default layout migration fence".into(),
            ));
        }
        self.put_cross_shard_tombstone_if_unchanged_with_allowed_fence(
            expected_source,
            target_object,
            source_shard_id,
            target_shard_id,
            expected_journal_sequence,
            None,
            Some(&own_fence.operation_id),
        )
    }

    fn put_cross_shard_tombstone_if_unchanged_with_allowed_fence(
        &mut self,
        expected_source: &DfsObjectMetadata,
        target_object: &DfsObjectMetadata,
        source_shard_id: &DfsMetadataShardId,
        target_shard_id: &DfsMetadataShardId,
        expected_journal_sequence: u64,
        pin_transfer_id: Option<&str>,
        allowed_fence: Option<&str>,
    ) -> Result<DfsConditionalSourceEviction> {
        let excluded = allowed_fence.unwrap_or("");
        if self
            .tables
            .namespace_prepare_object_owner_other_than(
                &expected_source.file_system_id,
                &expected_source.object_id,
                excluded,
            )
            .is_some()
            || self
                .tables
                .namespace_prepare_normalized_path_owner_other_than(
                    &expected_source.file_system_id,
                    &crate::metadata_keys::normalize_path(&expected_source.path),
                    excluded,
                )
                .is_some()
        {
            return Err(MetadataBackendError::Serde(
                "source eviction overlaps an active namespace prepare".into(),
            ));
        }
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
        let already_evicted = current.as_ref().is_some_and(|current| {
            matches!(
                current.tombstone_provenance.as_ref(),
                Some(crate::DfsTombstoneProvenance::SourceEviction {
                    source_shard_id: recorded_source,
                    target_shard_id: recorded_target,
                    ..
                }) if recorded_source == source_shard_id && recorded_target == target_shard_id
            )
        });
        let current_pins = self
            .tables
            .version_pins
            .values()
            .filter(|pin| pin.object_id == expected_source.object_id)
            .cloned()
            .collect::<Vec<_>>();
        let current_pin_transfers = self
            .tables
            .cross_shard_pin_transfers
            .values()
            .filter(|transfer| transfer.object_id == expected_source.object_id)
            .cloned()
            .collect::<Vec<_>>();
        if already_evicted && current_pins.is_empty() && current_pin_transfers.is_empty() {
            return Ok(DfsConditionalSourceEviction::AlreadyApplied);
        }
        if !already_evicted && self.journal_last_sequence() != expected_journal_sequence {
            return Ok(DfsConditionalSourceEviction::JournalAdvanced);
        }
        if !already_evicted && current.as_ref() != Some(expected_source) {
            return Ok(DfsConditionalSourceEviction::SourceChanged);
        }
        let tombstone = if already_evicted {
            current.expect("already-evicted source has a tombstone")
        } else {
            let Some(tombstone) = crate::metadata_shard_migration::cross_shard_eviction_tombstone(
                expected_source,
                target_object,
                source_shard_id,
                target_shard_id,
                pin_transfer_id,
            ) else {
                return Ok(DfsConditionalSourceEviction::AlreadyApplied);
            };
            normalized_object_metadata(&tombstone)
        };
        let entry = self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::finalize_cross_shard_source_eviction(
                &tombstone,
                current_pins.clone(),
                current_pin_transfers.clone(),
            ),
            vec![DfsMetadataTableOp::FinalizeCrossShardSourceEviction {
                tombstone,
                removed_pins: current_pins,
                removed_pin_transfers: current_pin_transfers,
            }],
        )?;
        Ok(DfsConditionalSourceEviction::Applied {
            sequence: entry.sequence,
        })
    }

    /// Recover a layout owner's saved reservation by its stable object key,
    /// including after the destination has advanced to another head.
    pub(crate) fn default_layout_migration_fence_for_object(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsNamespacePrepareRecord>> {
        let tables = self.current_tables_snapshot()?;
        let Some(operation_id) =
            tables.namespace_prepare_object_owner_other_than(file_system_id, object_id, "")
        else {
            return Ok(None);
        };
        if tables
            .namespace_prepare_object_owner_other_than(file_system_id, object_id, operation_id)
            .is_some()
        {
            return Err(MetadataBackendError::Serde(
                "default layout migration overlaps multiple active namespace prepares".into(),
            ));
        }
        if !crate::metadata_records::is_default_layout_migration_fence(operation_id) {
            return Err(MetadataBackendError::Serde(format!(
                "default layout migration overlaps unrelated active namespace prepare {operation_id}",
            )));
        }
        let fence = tables.namespace_prepares.get(operation_id).ok_or_else(|| {
            MetadataBackendError::Serde(
                "default layout migration reservation lost its durable prepare".into(),
            )
        })?;
        fence
            .validate_default_layout_migration_fence()
            .map_err(MetadataBackendError::Serde)?;
        if fence.status != DfsNamespacePrepareStatus::Prepared
            || fence.file_system_id != *file_system_id
            || fence.shard_id != self.shard_id
            || fence.expected_object_versions[0].object_id != *object_id
        {
            return Err(MetadataBackendError::Serde(
                "default layout migration reservation has foreign source identity".into(),
            ));
        }
        Ok(Some(fence.clone()))
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

    pub(crate) fn indexed_directory_page_cursor_for_object(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsDirectoryPageCursor> {
        let tables = self.current_tables_snapshot()?;
        let base_path_record = path_record_for_object(object);
        let stored_live_path_record = tables
            .paths
            .get(&(object.file_system_id.clone(), base_path_record.path.clone()))
            .filter(|path_record| {
                tables
                    .objects
                    .get(&path_record.object_id)
                    .is_some_and(|stored| {
                        stored.kind != DfsObjectKind::Tombstone
                            && stored.file_system_id == object.file_system_id
                            && normalize_path(&stored.path) == path_record.path
                    })
            })
            .cloned();
        let path_record = stored_live_path_record
            .unwrap_or_else(|| path_record_for_new_dentry(&tables, base_path_record).0);
        Ok(directory_page_cursor_with_dentry(
            object,
            path_record.dirfrag_id,
            path_record.name_hash,
            path_record.name,
        ))
    }

    /// Apply one object-family history mutation copied from a shard that is
    /// still authoritative while the object is moving. The source journal
    /// sequence is local to that shard, so the target records an equivalent
    /// mutation in its own journal before changing its materialized tables.
    pub(crate) fn apply_migrated_object_family_gc_mutation(
        &mut self,
        mutation: &DfsMetadataMutation,
    ) -> Result<()> {
        if !matches!(
            mutation,
            DfsMetadataMutation::RemoveVersion { .. }
                | DfsMetadataMutation::PutIntentReceipt { .. }
                | DfsMetadataMutation::RemoveIntentReceipt { .. }
                | DfsMetadataMutation::RemoveRetainedRepairOwner { .. }
                | DfsMetadataMutation::MergeIntentReceiptRetention { .. }
                | DfsMetadataMutation::RemoveVisibleCommit { .. }
        ) {
            return Err(MetadataBackendError::Serde(
                "unsupported migrated object-family GC mutation".to_string(),
            ));
        }
        let _lock = self.begin_metadata_write()?;
        let entry = self.write_journal_unlocked(mutation.clone())?;
        self.materialize_written_journal_entries_unlocked(std::slice::from_ref(&entry))?;
        Ok(())
    }
}

impl LocalSobjMetadataBackend {
    fn populate_metadata_history_storage_stats(
        &self,
        report: &mut DfsMetadataHistoryGcReport,
    ) -> Result<()> {
        let Some(shard) = report
            .shards
            .iter_mut()
            .find(|shard| shard.shard_id == self.shard_id)
        else {
            return Ok(());
        };
        shard.journal_head_sequence = self.journal_last_sequence();
        let journal_root = self.root.join("journal");
        if !journal_root.exists() {
            return Ok(());
        }
        let root_entries =
            fs::read_dir(&journal_root).map_err(|source| MetadataBackendError::Io {
                path: journal_root.clone(),
                source,
            })?;
        for entry in root_entries {
            let entry = entry.map_err(|source| MetadataBackendError::Io {
                path: journal_root.clone(),
                source,
            })?;
            let path = entry.path();
            let metadata = entry
                .metadata()
                .map_err(|source| MetadataBackendError::Io {
                    path: path.clone(),
                    source,
                })?;
            if metadata.is_file() {
                shard.journal_size_bytes = shard.journal_size_bytes.saturating_add(metadata.len());
            }
        }
        let segments_root = journal_root.join("segments");
        if !segments_root.exists() {
            return Ok(());
        }
        let segment_entries =
            fs::read_dir(&segments_root).map_err(|source| MetadataBackendError::Io {
                path: segments_root.clone(),
                source,
            })?;
        for entry in segment_entries {
            let entry = entry.map_err(|source| MetadataBackendError::Io {
                path: segments_root.clone(),
                source,
            })?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("slog") {
                continue;
            }
            let metadata = entry
                .metadata()
                .map_err(|source| MetadataBackendError::Io {
                    path: path.clone(),
                    source,
                })?;
            if metadata.is_file() {
                shard.journal_segment_count = shard.journal_segment_count.saturating_add(1);
                shard.journal_size_bytes = shard.journal_size_bytes.saturating_add(metadata.len());
            }
        }
        Ok(())
    }

    pub(crate) fn gc_metadata_history_authoritative(
        &mut self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<DfsMetadataHistoryGcReport> {
        let _lock = self.begin_metadata_write()?;
        let snapshot_size_bytes = fs::metadata(metadata_table_snapshot_path(&self.root))
            .ok()
            .map(|metadata| metadata.len());
        let mut plan = plan_metadata_history_gc(
            self.shard_id.clone(),
            &self.tables,
            &options,
            snapshot_size_bytes,
            self.supports_retained_repair_owners(),
        )?;
        self.populate_metadata_history_storage_stats(&mut plan.report)?;
        if plan.removals.is_empty() && plan.retained_repair_owners.is_empty() {
            return Ok(plan.report);
        }

        let retained_at = options
            .now_unix_nanos
            .and_then(|now| OffsetDateTime::from_unix_timestamp_nanos(i128::from(now)).ok())
            .unwrap_or_else(OffsetDateTime::now_utc);
        let mut mutations =
            Vec::with_capacity(plan.removals.len() + plan.retained_repair_owners.len());
        // These explicit journal entries precede compaction/eviction. Replica
        // replay never has to infer new semantics from an old GC mutation.
        for owner in &plan.retained_repair_owners {
            mutations.push(DfsMetadataMutation::put_intent(owner));
        }
        for removal in &plan.removals {
            let mutation = match removal {
                DfsMetadataHistoryRemoval::Receipt {
                    file_system_id,
                    object_id,
                    version,
                    replica_node_id,
                } => DfsMetadataMutation::RemoveReceipt {
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    version: version.clone(),
                    replica_node_id: replica_node_id.clone(),
                },
                DfsMetadataHistoryRemoval::Version {
                    file_system_id,
                    object_id,
                    version,
                } => DfsMetadataMutation::remove_version(
                    file_system_id.clone(),
                    object_id.clone(),
                    version.clone(),
                ),
                DfsMetadataHistoryRemoval::Intent {
                    file_system_id,
                    object_id,
                    intent_id,
                    ..
                } => {
                    let intent = self
                        .tables
                        .intents
                        .get(&(object_id.clone(), intent_id.clone()))
                        .ok_or_else(|| {
                            MetadataBackendError::Serde(format!(
                                "metadata GC lost planned intent {object_id}/{intent_id} while holding the shard write lock"
                            ))
                        })?;
                    if intent.publication.is_retained_repair() {
                        DfsMetadataMutation::RemoveRetainedRepairOwner {
                            file_system_id: file_system_id.clone(),
                            object_id: object_id.clone(),
                            intent_id: intent_id.clone(),
                        }
                    } else {
                        let receipt = DfsWriteIntentReceipt::from_intent(intent, retained_at)
                        .ok_or_else(|| {
                            MetadataBackendError::Serde(format!(
                                "metadata GC selected non-terminal intent {object_id}/{intent_id}"
                            ))
                        })?;
                        DfsMetadataMutation::put_intent_receipt(&receipt)
                    }
                }
                DfsMetadataHistoryRemoval::IntentReceipt {
                    file_system_id,
                    object_id,
                    intent_id,
                } => {
                    let receipt = self
                        .tables
                        .intent_receipts
                        .get(&(object_id.clone(), intent_id.clone()))
                        .ok_or_else(|| {
                            MetadataBackendError::Serde(format!(
                                "metadata GC lost planned intent receipt {object_id}/{intent_id} while holding the shard write lock"
                            ))
                        })?;
                    DfsMetadataMutation::remove_intent_receipt(
                        file_system_id.clone(),
                        object_id.clone(),
                        intent_id.clone(),
                        DfsWriteIntentReceiptRetentionState::eviction_delta(receipt),
                    )
                }
                DfsMetadataHistoryRemoval::Conflict {
                    file_system_id,
                    object_id,
                    conflict_id,
                } => DfsMetadataMutation::PutConflict {
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    conflict_id: conflict_id.clone(),
                    conflict: None,
                },
                DfsMetadataHistoryRemoval::VisibleCommit {
                    file_system_id,
                    object_id,
                    version,
                } => DfsMetadataMutation::RemoveVisibleCommit {
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    version: version.clone(),
                },
                DfsMetadataHistoryRemoval::NamespaceJob { job_id, .. } => {
                    let job = self.tables.namespace_jobs.get(job_id).ok_or_else(|| {
                        MetadataBackendError::Serde(format!(
                            "metadata GC lost planned namespace job {job_id} while holding the shard write lock"
                        ))
                    })?;
                    let receipt =
                        DfsNamespaceJobReceipt::from_job(job, retained_at).ok_or_else(|| {
                            MetadataBackendError::Serde(format!(
                                "metadata GC selected running namespace job {job_id}"
                            ))
                        })?;
                    DfsMetadataMutation::put_namespace_job_receipt(&receipt)
                }
                DfsMetadataHistoryRemoval::NamespaceTransaction { transaction_id, .. } => {
                    let transaction = self
                        .tables
                        .namespace_transactions
                        .get(transaction_id)
                        .ok_or_else(|| {
                            MetadataBackendError::Serde(format!(
                                "metadata GC lost planned namespace transaction {transaction_id} while holding the shard write lock"
                            ))
                        })?;
                    let receipt =
                        DfsNamespaceTransactionReceipt::from_transaction(transaction, retained_at)
                            .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
                    DfsMetadataMutation::put_namespace_transaction_receipt(&receipt)
                }
                DfsMetadataHistoryRemoval::NamespacePrepare { operation_id, .. } => {
                    let prepare = self
                        .tables
                        .namespace_prepares
                        .get(operation_id)
                        .ok_or_else(|| {
                            MetadataBackendError::Serde(format!(
                                "metadata GC lost planned namespace prepare {operation_id} while holding the shard write lock"
                            ))
                        })?;
                    let receipt = DfsNamespacePrepareReceipt::from_prepare(prepare, retained_at)
                        .ok_or_else(|| {
                            MetadataBackendError::Serde(format!(
                                "metadata GC selected non-terminal namespace prepare {operation_id}"
                            ))
                        })?;
                    DfsMetadataMutation::put_namespace_prepare_receipt(&receipt)
                }
                DfsMetadataHistoryRemoval::NamespaceRequest {
                    requester_node_id,
                    request_id,
                    ..
                } => {
                    let key = (requester_node_id.clone(), request_id.clone());
                    let request = self.tables.namespace_requests.get(&key).ok_or_else(|| {
                        MetadataBackendError::Serde(format!(
                            "metadata GC lost planned namespace request {requester_node_id}/{request_id} while holding the shard write lock"
                        ))
                    })?;
                    let receipt = DfsNamespaceRequestReceipt::from_request(request, retained_at)
                        .ok_or_else(|| {
                            MetadataBackendError::Serde(format!(
                                "metadata GC selected non-terminal namespace request {requester_node_id}/{request_id}"
                            ))
                        })?;
                    DfsMetadataMutation::put_namespace_request_receipt(&receipt)
                }
                DfsMetadataHistoryRemoval::NamespaceJobReceipt {
                    file_system_id,
                    job_id,
                } => {
                    let receipt =
                        self.tables
                            .namespace_job_receipts
                            .get(job_id)
                            .ok_or_else(|| {
                                MetadataBackendError::Serde(format!(
                                    "metadata GC lost planned namespace job receipt {job_id} while holding the shard write lock"
                                ))
                            })?;
                    DfsMetadataMutation::RemoveNamespaceJobReceipt {
                        file_system_id: file_system_id.clone(),
                        job_id: job_id.clone(),
                        retention: namespace_job_receipt_eviction_delta(receipt),
                    }
                }
                DfsMetadataHistoryRemoval::NamespaceTransactionReceipt {
                    file_system_id,
                    transaction_id,
                } => {
                    let receipt = self
                        .tables
                        .namespace_transaction_receipts
                        .get(transaction_id)
                        .ok_or_else(|| {
                            MetadataBackendError::Serde(format!(
                                "metadata GC lost planned namespace transaction receipt {transaction_id} while holding the shard write lock"
                            ))
                        })?;
                    DfsMetadataMutation::RemoveNamespaceTransactionReceipt {
                        file_system_id: file_system_id.clone(),
                        transaction_id: transaction_id.clone(),
                        retention: namespace_transaction_receipt_eviction_delta(receipt),
                    }
                }
                DfsMetadataHistoryRemoval::NamespacePrepareReceipt {
                    file_system_id,
                    operation_id,
                } => {
                    let receipt = self
                        .tables
                        .namespace_prepare_receipts
                        .get(operation_id)
                        .ok_or_else(|| {
                            MetadataBackendError::Serde(format!(
                                "metadata GC lost planned namespace prepare receipt {operation_id} while holding the shard write lock"
                            ))
                        })?;
                    DfsMetadataMutation::RemoveNamespacePrepareReceipt {
                        file_system_id: file_system_id.clone(),
                        operation_id: operation_id.clone(),
                        retention: namespace_prepare_receipt_eviction_delta(receipt),
                    }
                }
                DfsMetadataHistoryRemoval::NamespaceRequestReceipt {
                    file_system_id,
                    requester_node_id,
                    request_id,
                } => {
                    let key = (requester_node_id.clone(), request_id.clone());
                    let receipt = self
                        .tables
                        .namespace_request_receipts
                        .get(&key)
                        .ok_or_else(|| {
                            MetadataBackendError::Serde(format!(
                                "metadata GC lost planned namespace request receipt {requester_node_id}/{request_id} while holding the shard write lock"
                            ))
                        })?;
                    DfsMetadataMutation::RemoveNamespaceRequestReceipt {
                        file_system_id: file_system_id.clone(),
                        requester_node_id: requester_node_id.clone(),
                        request_id: request_id.clone(),
                        retention: namespace_request_receipt_eviction_delta(receipt),
                    }
                }
            };
            mutations.push(mutation);
        }
        let entries = self.write_journal_entries_unlocked(mutations)?;
        self.materialize_written_journal_entries_unlocked(&entries)?;
        // A bounded lifecycle drain may need hundreds of journaled batches.
        // Publishing a complete snapshot after every partial batch makes that
        // drain quadratic in the retained table size. The journal and
        // materialized table log are already durable at this point, so defer
        // the forced checkpoint until this shard's current candidate set fits
        // in the configured batch. The normal table-log byte threshold still
        // schedules an earlier checkpoint when the incremental suffix grows
        // large.
        if !plan.report.max_removals_reached {
            self.compact_metadata_table_log_if_needed(0)?;
        }
        Ok(plan.report)
    }

    fn metadata_recovery_watermark_path(&self) -> PathBuf {
        self.root
            .join("records")
            .join("tables")
            .join(METADATA_RECOVERY_WATERMARK_FILE_NAME)
    }

    fn metadata_recovery_table_generation(&self) -> Result<DfsMetadataRecoveryTableGeneration> {
        Ok(DfsMetadataRecoveryTableGeneration {
            snapshot: metadata_recovery_file_fingerprint(&metadata_table_snapshot_path(
                &self.root,
            ))?,
            checkpoint_log: metadata_recovery_file_fingerprint(
                &metadata_table_checkpoint_log_path(&self.root),
            )?,
            materialized_log: metadata_recovery_file_fingerprint(&metadata_table_path(&self.root))?,
        })
    }

    fn current_metadata_recovery_watermark(&self) -> Result<Option<DfsMetadataRecoveryWatermark>> {
        let Some(prefix_proof) = self.prefix_proof.clone() else {
            return Ok(None);
        };
        if !prefix_proof.is_well_formed()
            || prefix_proof.journal_sequence != self.materialized_through_sequence
            || self.next_sequence != self.materialized_through_sequence.saturating_add(1).max(1)
        {
            return Ok(None);
        }
        Ok(Some(DfsMetadataRecoveryWatermark {
            format_version: METADATA_RECOVERY_WATERMARK_FORMAT_VERSION,
            shard_id: self.shard_id.clone(),
            journal_next_sequence: self.next_sequence,
            materialized_through_sequence: self.materialized_through_sequence,
            prefix_proof,
            table_generation: self.metadata_recovery_table_generation()?,
        }))
    }

    fn metadata_recovery_watermark_matches(&self) -> bool {
        let expected = match self.current_metadata_recovery_watermark() {
            Ok(Some(expected)) => expected,
            Ok(None) => return false,
            Err(error) => {
                metadata_backend_perf_trace(format_args!(
                    "metadata-recovery-watermark action=probe-generation-failed shard={} error={error}",
                    self.shard_id.0
                ));
                return false;
            }
        };
        let path = self.metadata_recovery_watermark_path();
        match fs::metadata(&path) {
            Ok(metadata)
                if !metadata.is_file()
                    || metadata.len() > METADATA_RECOVERY_WATERMARK_MAX_BYTES =>
            {
                return false;
            }
            Ok(_) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return false,
            Err(source) => {
                metadata_backend_perf_trace(format_args!(
                    "metadata-recovery-watermark action=stat-failed shard={} error={source}",
                    self.shard_id.0
                ));
                return false;
            }
        }
        match read_sobj_optional::<DfsMetadataRecoveryWatermark>(&path) {
            Ok(Some(observed)) => observed == expected,
            Ok(None) => false,
            Err(error) => {
                metadata_backend_perf_trace(format_args!(
                    "metadata-recovery-watermark action=read-failed shard={} error={error}",
                    self.shard_id.0
                ));
                false
            }
        }
    }

    fn publish_metadata_recovery_watermark(&self) {
        let path = self.metadata_recovery_watermark_path();
        let watermark = match self.current_metadata_recovery_watermark() {
            Ok(Some(watermark)) => watermark,
            Ok(None) => {
                let _ = remove_file_if_exists(&path);
                return;
            }
            Err(error) => {
                metadata_backend_perf_trace(format_args!(
                    "metadata-recovery-watermark action=build-failed shard={} error={error}",
                    self.shard_id.0
                ));
                let _ = remove_file_if_exists(&path);
                return;
            }
        };
        if let Err(error) = write_sobj_atomic(&path, &watermark) {
            // The watermark is only a recovery optimization. Leave no
            // possibly-current marker after a failed publication: this open
            // already completed recovery, and the next one must scan again.
            let invalidation = remove_file_if_exists(&path);
            metadata_backend_perf_trace(format_args!(
                "metadata-recovery-watermark action=publish-failed shard={} error={error} invalidation={:?}",
                self.shard_id.0,
                invalidation.as_ref().err()
            ));
        }
    }

    fn recover_deferred_metadata_state(&mut self) -> Result<MetadataRecoveryPassTimings> {
        // One writer-owned critical section binds the clean proof to the exact
        // journal prefix and physical table generation inspected by all three
        // passes. A crash before the final atomic publication leaves no usable
        // proof, so the next writer repeats recovery.
        let _lock = self.begin_metadata_write()?;
        if self.metadata_recovery_watermark_matches() {
            return Ok(MetadataRecoveryPassTimings {
                skipped: true,
                ..MetadataRecoveryPassTimings::default()
            });
        }
        // Prevent a stale but parseable marker from surviving a failed
        // replacement when recovery itself does not need to change tables.
        if let Err(error) = remove_file_if_exists(&self.metadata_recovery_watermark_path()) {
            metadata_backend_perf_trace(format_args!(
                "metadata-recovery-watermark action=invalidate-failed shard={} error={error}",
                self.shard_id.0
            ));
        }

        let namespace_started = Instant::now();
        self.recover_namespace_transactions_unlocked()?;
        let namespace_ms = namespace_started.elapsed().as_millis();
        let visible_started = Instant::now();
        self.recover_visible_version_commits_unlocked()?;
        let visible_ms = visible_started.elapsed().as_millis();
        let location_started = Instant::now();
        self.recover_object_locations_from_visible_versions_unlocked()?;
        let location_ms = location_started.elapsed().as_millis();
        self.publish_metadata_recovery_watermark();
        Ok(MetadataRecoveryPassTimings {
            namespace_ms,
            visible_ms,
            location_ms,
            skipped: false,
        })
    }

    fn recover_namespace_transactions_unlocked(&mut self) -> Result<()> {
        let trace_started = Instant::now();
        #[cfg(test)]
        METADATA_NAMESPACE_RECOVERY_SCAN_CALLS.set(
            METADATA_NAMESPACE_RECOVERY_SCAN_CALLS
                .get()
                .saturating_add(1),
        );
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
        committed_at: OffsetDateTime,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        let trace_started = Instant::now();
        let planned_ops_before = table_ops.len();
        let metadata_objects_before = self.tables.objects.len();
        crate::metadata_namespace_transaction::preapply_namespace_transaction_table_ops_at(
            &mut self.tables,
            transaction,
            committed_at,
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
        committed_at: OffsetDateTime,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        let mut object_ops = self.object_index_table_ops_with_previous(object, previous)?;
        crate::file_history::stamp_policy_captures(&self.tables, &mut object_ops, committed_at);
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
        crate::file_history::validate_file_history_transition(&self.tables, object)?;
        Ok(plan_object_index_table_ops_with_previous(
            &self.tables,
            object,
            previous,
        ))
    }

    #[cfg(test)]
    fn recover_visible_version_commits(&mut self) -> Result<()> {
        // One recovery pass owns one refreshed table view. Taking and
        // refreshing the shard lock once per retained (usually already
        // materialized) commit turns every backend open into an O(commits)
        // sequence of lock/open syscalls even when there is no work to do.
        let _lock = self.begin_metadata_write()?;
        self.recover_visible_version_commits_unlocked()
    }

    fn recover_visible_version_commits_unlocked(&mut self) -> Result<()> {
        #[cfg(test)]
        METADATA_VISIBLE_RECOVERY_SCAN_CALLS
            .set(METADATA_VISIBLE_RECOVERY_SCAN_CALLS.get().saturating_add(1));
        for commit in visible_commit_recovery_order(&self.tables) {
            self.recover_visible_version_commit_unlocked(&commit)?;
        }
        Ok(())
    }

    fn recover_object_locations_from_visible_versions_unlocked(&mut self) -> Result<()> {
        #[cfg(test)]
        METADATA_LOCATION_RECOVERY_SCAN_CALLS.set(
            METADATA_LOCATION_RECOVERY_SCAN_CALLS
                .get()
                .saturating_add(1),
        );
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

    pub(crate) fn run_history_gc_runtime_probe(
        root: &Path,
        options: crate::DfsMetadataHistoryGcRuntimeProbeOptions,
    ) -> std::result::Result<crate::DfsMetadataHistoryGcRuntimeProbeReport, DfsError> {
        const MAX_RETAINED_METADATA_RECORDS: u64 = 32;
        const MAX_RETAINED_METADATA_BYTES: u64 = 256 * 1024;
        const MAX_JOURNAL_BYTES: u64 = 16 * 1024 * 1024;
        const MAX_SNAPSHOT_BYTES: u64 = 1024 * 1024;
        const MAX_CONTENT_STORE_BYTES: u64 = 4 * 1024 * 1024;

        let failed = |message: String| DfsError::Metadata { message };
        if options.overwrite_count < 2 {
            return Err(failed(
                "metadata-history GC runtime probe requires at least two overwrites".to_string(),
            ));
        }
        if options.content_batch_size == 0 || options.gc_batch_size == 0 {
            return Err(failed(
                "metadata-history GC runtime probe batch sizes must be positive".to_string(),
            ));
        }
        if root.exists()
            && fs::read_dir(root)
                .map_err(|source| failed(format!("read probe root {}: {source}", root.display())))?
                .next()
                .is_some()
        {
            return Err(failed(format!(
                "metadata-history GC runtime probe requires an empty root: {}",
                root.display()
            )));
        }

        let total_started = Instant::now();
        let node = NodeId::from_legacy_named("node_metadata_history_runtime_probe")
            .map_err(|source| failed(format!("construct probe node id: {source}")))?;
        let file_system_id = DfsFileSystemId::from_legacy_named("sifs")
            .map_err(|source| failed(format!("construct probe filesystem id: {source}")))?;
        let object_id = DfsObjectId::from_legacy_named("obj_metadata_history_runtime_probe")
            .map_err(|source| failed(format!("construct probe object id: {source}")))?;
        let shard_id = DfsMetadataShardId::new("root");
        let file_system = DfsFileSystem {
            metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: file_system_id.clone(),
            display_name: "metadata history runtime probe".to_string(),
            owner_node_id: node.clone(),
            subtree_policies: Vec::new(),
        };
        let initial_version = DfsVersion::from_legacy_named("version_churn_000000")
            .map_err(|source| failed(format!("construct initial probe version: {source}")))?;
        let mut current = DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: "/runtime-probes/metadata-history.bin".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: node.clone(),
            current_locations: vec![node.clone()],
            content_hash: None,
            content_size_bytes: Some(128),
            link_target_object_id: None,
            mode: 0o600,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: initial_version,
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };

        let content_root = root.join("content");
        let content = LocalContentStore::new(&content_root)?;
        let mut mutations = Vec::with_capacity(
            options
                .overwrite_count
                .saturating_add(options.namespace_request_count),
        );
        let content_write_started = Instant::now();
        let mut distinct_content_hashes = BTreeSet::new();
        for batch_start in (0..options.overwrite_count).step_by(options.content_batch_size) {
            let batch_end = batch_start
                .saturating_add(options.content_batch_size)
                .min(options.overwrite_count);
            let versions = (batch_start..batch_end)
                .map(|index| {
                    DfsVersion::from_legacy_named(format!("version_churn_{index:06}"))
                        .map_err(|source| failed(format!("construct probe version: {source}")))
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let payloads = (batch_start..batch_end)
                .map(|index| {
                    let mut payload = [0_u8; 128];
                    payload[..8].copy_from_slice(&(index as u64).to_le_bytes());
                    for (offset, byte) in payload[8..].iter_mut().enumerate() {
                        *byte = (index.wrapping_mul(131).wrapping_add(offset).wrapping_add(1)
                            as u8)
                            .rotate_left((offset % 8) as u32);
                    }
                    payload
                })
                .collect::<Vec<_>>();
            let manifests = content.put_object_versions(versions.iter().zip(&payloads).map(
                |(version, payload)| (object_id.as_str(), version.as_str(), payload.as_slice()),
            ))?;
            for (((index, version), payload), manifest) in (batch_start..batch_end)
                .zip(versions)
                .zip(payloads)
                .zip(manifests)
            {
                if !distinct_content_hashes.insert(manifest.content_hash.clone()) {
                    return Err(failed(format!(
                        "metadata-history GC overwrite {index} did not produce unique content"
                    )));
                }
                if index + 1 == options.overwrite_count {
                    current.version = version.clone();
                    current.content_hash = Some(manifest.content_hash.clone());
                }
                mutations.push(DfsMetadataMutation::put_version(&DfsObjectVersionRecord {
                    metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
                    content_durability: None,
                    retention: Default::default(),
                    module_metadata: Default::default(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    version,
                    content_hash: Some(manifest.content_hash),
                    size_bytes: Some(payload.len() as u64),
                    locations: current.current_locations.clone(),
                    created_by_event_id: None,
                    created_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(index as i64),
                    visible: true,
                }));
            }
        }
        if distinct_content_hashes.len() != options.overwrite_count {
            return Err(failed(format!(
                "metadata-history GC produced {} unique hashes for {} overwrites",
                distinct_content_hashes.len(),
                options.overwrite_count
            )));
        }
        for index in 0..options.namespace_request_count {
            let timestamp = OffsetDateTime::from_unix_timestamp(index as i64)
                .map_err(|source| failed(format!("construct probe timestamp: {source}")))?;
            let request = DfsNamespaceRequestRecord {
                file_system_id: file_system_id.clone(),
                requester_node_id: node.clone(),
                request_id: format!("runtime-probe-request-{index:06}"),
                operation_id: format!("runtime-probe-operation-{index:06}"),
                authority_shard_id: shard_id.clone(),
                operation: DfsNamespaceOperation::SetMode {
                    path: format!("/runtime-probes/requests/{index}"),
                    mode: 0o644,
                },
                command_digest: format!("blake3:runtime-probe-request-{index:06}"),
                status: DfsNamespaceRequestStatus::Completed,
                result: Some(DfsNamespaceResult::Applied),
                posix_coordination_result: None,
                recovery_attempts: 0,
                last_recovery_error: None,
                created_at: timestamp,
                updated_at: timestamp,
            };
            mutations.push(DfsMetadataMutation::put_namespace_request(&request));
        }
        let content_write_elapsed = content_write_started.elapsed();

        let metadata_write_started = Instant::now();
        let mut backend = Self::new(shard_id.clone(), root)?;
        backend.put_file_system(&file_system)?;
        backend.put_object(&current)?;
        {
            let _lock = backend.begin_metadata_write()?;
            let entries = backend.write_journal_entries_unlocked(mutations)?;
            backend.materialize_written_journal_entries_unlocked(&entries)?;
        }
        let metadata_write_elapsed = metadata_write_started.elapsed();
        let journal_bytes_before = runtime_probe_tree_file_bytes(&root.join("journal"))
            .map_err(|source| failed(format!("measure pre-GC journal: {source}")))?;
        backend.compact_metadata_table_log_if_needed(0)?;
        let snapshot_bytes_before = fs::metadata(metadata_table_snapshot_path(root))
            .map_err(|source| failed(format!("measure pre-GC snapshot: {source}")))?
            .len();
        let content_bytes_before = runtime_probe_tree_file_bytes(&content_root)
            .map_err(|source| failed(format!("measure pre-GC content: {source}")))?;

        let gc_started = Instant::now();
        let mut metadata_gc_passes = 0_u64;
        loop {
            let report = backend.gc_metadata_history_authoritative(
                DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                    .with_now_unix_nanos(u64::MAX)
                    .with_max_removals(options.gc_batch_size),
            )?;
            metadata_gc_passes = metadata_gc_passes.saturating_add(1);
            if report.applied_record_count() == 0 {
                break;
            }
            if metadata_gc_passes >= 1_000 {
                return Err(failed(
                    "metadata-history GC did not converge within 1,000 bounded passes".to_string(),
                ));
            }
        }
        let metadata_gc_elapsed = gc_started.elapsed();
        let protection = backend.metadata_content_protection()?;

        let reservation_release_started = Instant::now();
        content.release_all_content_reservations_for_runtime_probe()?;
        let reservation_release_elapsed = reservation_release_started.elapsed();
        let content_gc_started = Instant::now();
        let version_gc = content.collect_versions(
            ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_min_absence_age(Duration::ZERO)
                .with_catalog_observation("runtime-probe-metadata-catalog-generation-1", 1)
                .with_protected_versions(
                    protection
                        .versions
                        .iter()
                        .map(|identity| (identity.object_id.as_str(), identity.version.as_str())),
                ),
        )?;
        let version_gc =
            if version_gc.deleted_version_count == 0 {
                content.collect_versions(
                    ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                        .with_min_absence_age(Duration::ZERO)
                        .with_catalog_observation(
                            "runtime-probe-metadata-catalog-generation-2",
                            u64::MAX,
                        )
                        .with_protected_versions(protection.versions.iter().map(|identity| {
                            (identity.object_id.as_str(), identity.version.as_str())
                        })),
                )?
            } else {
                version_gc
            };
        let retained_manifest_count =
            (options.overwrite_count as u64).saturating_sub(version_gc.deleted_version_count);
        if !(1..=2).contains(&retained_manifest_count) {
            return Err(failed(format!(
                "metadata-history content GC retained {retained_manifest_count} manifests, expected one or two: {version_gc:?}"
            )));
        }

        let mut orphan_gc_passes = 0_u64;
        let mut deleted_orphan_count = 0_u64;
        let mut reclaimed_packed_segment_bytes = 0_u64;
        loop {
            let catalog_sequence = orphan_gc_passes.saturating_add(1);
            let catalog_observed_at_unix_nanos =
                u64::MAX.saturating_sub(64).saturating_add(catalog_sequence);
            let report = content.collect_garbage(
                ContentStoreGcOptions::delete_eligible(Duration::ZERO)
                    .with_catalog_observation(
                        format!("runtime-probe-content-catalog-generation-{catalog_sequence}"),
                        catalog_observed_at_unix_nanos,
                    )
                    .with_protected_content_hashes(
                        protection.content_hashes.iter().map(String::as_str),
                    ),
            )?;
            orphan_gc_passes = orphan_gc_passes.saturating_add(1);
            deleted_orphan_count = deleted_orphan_count.saturating_add(report.deleted_orphan_count);
            reclaimed_packed_segment_bytes = reclaimed_packed_segment_bytes
                .saturating_add(report.reclaimed_packed_segment_bytes);
            let work_remains = report.newly_observed_orphan_count != 0
                || report.eligible_orphan_count != 0
                || report.stale_orphan_observation_count != 0
                || report.eligible_packed_segment_count != 0
                || report.pending_inline_compaction_count != 0;
            if !work_remains {
                break;
            }
            if orphan_gc_passes >= 64 {
                return Err(failed(format!(
                    "metadata-history packed content GC did not converge within 64 passes: {report:?}"
                )));
            }
        }
        let content_gc_elapsed = content_gc_started.elapsed();
        let post_gc = backend.metadata_history_gc_projection(
            DfsMetadataHistoryGcOptions::dry_run(Duration::ZERO, 1).with_now_unix_nanos(u64::MAX),
        )?;
        let shard = post_gc.shards.first().ok_or_else(|| {
            failed("metadata-history GC projection omitted the root shard".to_string())
        })?;
        if post_gc.eligible_record_count() != 0
            || backend.tables.versions.len() != 1
            || !backend.tables.namespace_requests.is_empty()
            || !backend.tables.namespace_request_receipts.is_empty()
            || backend.tables.namespace_receipt_retention.is_empty()
        {
            return Err(failed(format!(
                "metadata-history GC retained unexpected logical history: projection={post_gc:?} versions={} requests={} receipts={} retention_empty={}",
                backend.tables.versions.len(),
                backend.tables.namespace_requests.len(),
                backend.tables.namespace_request_receipts.len(),
                backend.tables.namespace_receipt_retention.is_empty()
            )));
        }
        if shard.total_record_count > MAX_RETAINED_METADATA_RECORDS
            || shard.estimated_serialized_bytes > MAX_RETAINED_METADATA_BYTES
        {
            return Err(failed(format!(
                "metadata-history GC exceeded retained metadata bounds: {shard:?}"
            )));
        }

        let journal_bytes_after = runtime_probe_tree_file_bytes(&root.join("journal"))
            .map_err(|source| failed(format!("measure post-GC journal: {source}")))?;
        let materialized_table_bytes = fs::metadata(metadata_table_path(root))
            .map_err(|source| failed(format!("measure post-GC table: {source}")))?
            .len();
        let snapshot_bytes_after = fs::metadata(metadata_table_snapshot_path(root))
            .map_err(|source| failed(format!("measure post-GC snapshot: {source}")))?
            .len();
        let content_bytes_after = runtime_probe_tree_file_bytes(&content_root)
            .map_err(|source| failed(format!("measure post-GC content: {source}")))?;
        if (journal_bytes_before > MAX_JOURNAL_BYTES && journal_bytes_after >= journal_bytes_before)
            || (snapshot_bytes_before > MAX_SNAPSHOT_BYTES
                && snapshot_bytes_after >= snapshot_bytes_before)
            || (content_bytes_before > MAX_CONTENT_STORE_BYTES
                && content_bytes_after >= content_bytes_before)
            || shard.journal_segment_count != 1
            || journal_bytes_after > MAX_JOURNAL_BYTES
            || materialized_table_bytes != 0
            || snapshot_bytes_after > MAX_SNAPSHOT_BYTES
            || content_bytes_after > MAX_CONTENT_STORE_BYTES
        {
            return Err(failed(format!(
                "metadata-history GC physical bounds failed: journal={journal_bytes_before}->{journal_bytes_after} segments={} table={materialized_table_bytes} snapshot={snapshot_bytes_before}->{snapshot_bytes_after} content={content_bytes_before}->{content_bytes_after}",
                shard.journal_segment_count
            )));
        }
        let content_projection = content.collect_garbage(
            ContentStoreGcOptions::dry_run(Duration::ZERO).with_protected_content_hashes(
                protection.content_hashes.iter().map(String::as_str),
            ),
        )?;
        if content_projection.newly_observed_orphan_count != 0
            || content_projection.eligible_orphan_count != 0
            || content_projection.stale_orphan_observation_count != 0
            || content_projection.eligible_packed_segment_count != 0
            || content_projection.pending_inline_compaction_count != 0
        {
            return Err(failed(format!(
                "metadata-history GC left collectible content: {content_projection:?}"
            )));
        }
        let content_stats = content.stats()?;
        if content_stats.manifest_count != retained_manifest_count
            || content_stats.blob_count != retained_manifest_count
            || content_stats.logical_object_bytes != retained_manifest_count.saturating_mul(128)
        {
            return Err(failed(format!(
                "metadata-history GC content stats are inconsistent: {content_stats:?} retained_manifests={retained_manifest_count}"
            )));
        }

        drop(backend);
        drop(content);
        let restart_started = Instant::now();
        let reopened = Self::new(shard_id, root)?;
        let reopened_content = LocalContentStore::new(&content_root)?;
        let restart_elapsed = restart_started.elapsed();
        if reopened.tables.versions.len() != 1
            || !reopened.tables.namespace_requests.is_empty()
            || !reopened.tables.namespace_request_receipts.is_empty()
        {
            return Err(failed(
                "metadata-history GC reopened with unbounded logical history".to_string(),
            ));
        }
        let current_hash = current.content_hash.as_deref().ok_or_else(|| {
            failed("metadata-history current content hash is missing".to_string())
        })?;
        let reopened_manifest =
            reopened_content.read_manifest(object_id.as_str(), current.version.as_str())?;
        if reopened_manifest.content_hash != current_hash {
            return Err(failed(format!(
                "metadata-history current manifest hash changed across restart: expected {current_hash}, actual {}",
                reopened_manifest.content_hash
            )));
        }
        let current_index = options.overwrite_count - 1;
        let mut expected_current_payload = [0_u8; 128];
        expected_current_payload[..8].copy_from_slice(&(current_index as u64).to_le_bytes());
        for (offset, byte) in expected_current_payload[8..].iter_mut().enumerate() {
            *byte = (current_index
                .wrapping_mul(131)
                .wrapping_add(offset)
                .wrapping_add(1) as u8)
                .rotate_left((offset % 8) as u32);
        }
        if reopened_content.read_blob(current_hash)? != expected_current_payload {
            return Err(failed(
                "metadata-history current payload changed across restart".to_string(),
            ));
        }

        Ok(crate::DfsMetadataHistoryGcRuntimeProbeReport {
            overwrite_count: options.overwrite_count as u64,
            namespace_request_count: options.namespace_request_count as u64,
            unique_content_hash_count: distinct_content_hashes.len() as u64,
            metadata_gc_passes,
            orphan_gc_passes,
            content_write_elapsed_ms: duration_millis(content_write_elapsed),
            reservation_release_elapsed_ms: duration_millis(reservation_release_elapsed),
            metadata_write_elapsed_ms: duration_millis(metadata_write_elapsed),
            metadata_gc_elapsed_ms: duration_millis(metadata_gc_elapsed),
            content_gc_elapsed_ms: duration_millis(content_gc_elapsed),
            restart_elapsed_ms: duration_millis(restart_elapsed),
            total_elapsed_ms: duration_millis(total_started.elapsed()),
            deleted_orphan_count,
            reclaimed_packed_segment_bytes,
            retained_manifest_count,
            retained_metadata_record_count: shard.total_record_count,
            retained_metadata_bytes: shard.estimated_serialized_bytes,
            journal_bytes_before,
            journal_bytes_after,
            materialized_table_bytes,
            snapshot_bytes_before,
            snapshot_bytes_after,
            content_bytes_before,
            content_bytes_after,
        })
    }

    fn materialize_visible_version_commit(
        &mut self,
        commit: &DfsVisibleVersionCommitRecord,
        sequence: u64,
    ) -> Result<()> {
        self.append_and_apply_table_ops(sequence, visible_commit_table_ops(&self.tables, commit))
    }
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn runtime_probe_tree_file_bytes(path: &Path) -> std::io::Result<u64> {
    let metadata = fs::metadata(path)?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    let mut bytes = 0_u64;
    for entry in fs::read_dir(path)? {
        bytes = bytes.saturating_add(runtime_probe_tree_file_bytes(&entry?.path())?);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests;

mod replay;

mod handoff;

mod namespace_prepare;
use namespace_prepare::*;

mod pin_validation;
use pin_validation::*;

mod snapshot_validation;
use snapshot_validation::*;

mod api;
mod reference_outcomes;

#[cfg(test)]
mod reader_floor_regressions {
    use super::*;

    #[test]
    fn compacted_contribution_receipt_requires_floor_on_fresh_snapshot_install() {
        let mut tables = DfsMetadataTables::default();
        tables.namespace_transaction_receipts.insert(
            "contribution".into(),
            DfsNamespaceTransactionReceipt {
                content_reference_identity: None,
                contribution_digest: Some("blake3:input".into()),
                transaction_id: "contribution".into(),
                file_system_id: DfsFileSystemId::generate(),
                transaction_digest: "blake3:transaction".into(),
                created_at: OffsetDateTime::UNIX_EPOCH,
                retained_at: OffsetDateTime::UNIX_EPOCH,
            },
        );
        assert!(crate::metadata_reader_floor::tables_require_floor(&tables));
        tables.namespace_transaction_receipts.clear();
        tables
            .namespace_receipt_retention
            .content_reference_outcome_retention
            .insert(
                DfsObjectId::generate(),
                crate::DfsContentReferenceOutcomeRetention {
                    file_system_id: DfsFileSystemId::generate(),
                    max_evicted_operation_order_token: None,
                    opaque_operation_history_evicted: true,
                },
            );
        assert!(
            crate::metadata_reader_floor::tables_require_floor(&tables),
            "expiry-only snapshots must reject downgrade even after every witness was evicted"
        );
    }

    #[test]
    fn stale_accounted_generation_cannot_take_foreground_floor_fast_path() {
        let root = sinomo_testdir::TestDir::new("dfs-reader-floor-stale-accounting");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.path()).unwrap();
        let _lock = backend.begin_metadata_write().unwrap();
        backend.table_storage_generation = None;
        assert!(matches!(
            backend.ensure_namespaced_reader_floor(),
            Err(MetadataBackendError::ReaderFloorUpgradePending { target_format: 6 })
        ));
        assert!(
            !backend.namespaced_reader_floor_ready,
            "stale prices must not claim a synchronous completed upgrade"
        );
        drop(_lock);
        crate::metadata_table_apply::wait_for_metadata_table_checkpoint_workers_for_test(
            root.path(),
        );
        assert!(crate::metadata_snapshot_migration::reader_floor_installed(root.path()).unwrap());
    }

    #[test]
    fn bounded_floor_promotion_records_verified_completion_and_loaded_cache_tracks_format() {
        let root = sinomo_testdir::TestDir::new("dfs-reader-floor-bounded-job");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.path()).unwrap();
        let _lock = backend.begin_metadata_write().unwrap();
        backend.ensure_namespaced_reader_floor().unwrap();
        let job = backend.storage_migration_status().unwrap().unwrap();
        assert_eq!(job.identity.target_version, 6);
        assert_eq!(
            job.phase,
            sinomo_core::storage::migration::MigrationPhase::Complete
        );
        assert!(backend.namespaced_reader_floor_ready);
        // Simulate the empty state observed after an interrupted archive. The
        // loaded floor is tied to this checked generation, not handle lifetime.
        crate::metadata_table_store::archive_metadata_backend_state(root.path()).unwrap();
        backend.reload_durable_metadata_table_state().unwrap();
        assert!(!backend.namespaced_reader_floor_ready);
    }
}
