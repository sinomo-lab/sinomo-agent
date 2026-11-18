//! Durable DFS metadata shard backend.
//!
//! This is the first concrete metadata-plane surface. It intentionally stores
//! file/object metadata outside `sinomo-kv`: materialized shard state lives in
//! packed table batches, and every mutation is mirrored into an append-only
//! journal that can later be replicated or replayed by metadata shard peers.

#[cfg(test)]
use crate::DfsReplicationPolicy;
use crate::advisory_lock::DfsAdvisoryLockStateRecord;
use crate::content_root_catalog::{
    DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_BYTES, DFS_CONTENT_ROOT_CATALOG_TARGET_PAGE_BYTES,
    DfsContentRootCatalogCoverage, DfsContentRootCatalogCursor, DfsContentRootCatalogEntry,
    DfsContentRootCatalogIncompleteReason, DfsContentRootCatalogPage,
    DfsContentRootCatalogPageRequest, dfs_content_root_catalog_entries_from_canonical_protection,
};
use crate::control_plane::DfsMetadataShardConfig;
use crate::metadata_api::{
    DfsMetadataBackend, DfsMetadataReplicaApplyReport, DfsMetadataReplicaIngestPermit,
    DfsMetadataShardWriteFreeze, DfsNamespaceRequestAdmissionSnapshot,
    namespace_prepare_expectations_from_lookups,
};
use crate::metadata_cursor;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_history_gc::{
    DfsMetadataContentProtection, DfsMetadataHistoryGcOptions, DfsMetadataHistoryGcReport,
    DfsMetadataHistoryRemoval, metadata_content_protection_from_tables, plan_metadata_history_gc,
};
use crate::metadata_io::create_dir_all;
use crate::metadata_journal::{
    DfsMetadataReplayCursor, bounded_metadata_journal_batch,
    journal_entries_since_limited_with_next_sequence, metadata_prefix_proof_after_exact_frame,
    persist_replayed_journal_frames, read_next_journal_sequence_lenient,
    read_segmented_journal_frame_at_sequence, read_segmented_journal_frames_at_sequences,
    write_local_journal_frame_with_bytes,
};
use crate::metadata_keys::{
    directory_page_cursor_with_dentry, normalize_path, path_record_for_object,
};
use crate::metadata_limits::{
    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, MAX_DURABLE_VERSION_PINS_PER_OBJECT,
    OBJECT_LOCAL_METADATA_PAGE_SIZE,
};
use crate::metadata_lock::{MetadataWriteLock, metadata_write_lock};
use crate::metadata_namespace_transaction::validate_namespace_transaction_preconditions;
use crate::metadata_object_index::path_record_for_new_dentry;
use crate::metadata_pages::object_by_path_from_tables;
use crate::metadata_pages::{
    DfsDirectoryPageEntries, child_objects_for_directory_from_tables,
    child_objects_for_directory_page_entries_from_tables, collect_metadata_pages,
    count_objects_for_file_system_from_tables, first_hardlink_to_object_outside_prefix_from_tables,
    hardlink_count_to_object_from_tables, hardlinks_to_object_page_from_tables,
    intents_for_object_page_from_tables, intents_for_object_version_page_from_tables,
    namespace_jobs_page_from_tables, objects_for_file_system_page_from_tables,
    pending_namespace_requests_page_from_tables, receipt_records_page_from_tables,
    receipts_for_version_page_from_tables, retained_revision_records_page_from_tables,
    version_pins_for_object_page_from_tables, version_records_page_from_tables,
    versions_for_object_page_from_tables, write_intents_page_from_tables,
};
use crate::metadata_read_snapshot;
use crate::metadata_receipt::replay_receipt_matches_existing;
use crate::metadata_records::DfsPreparedOperationDigest;
use crate::metadata_records::*;
use crate::metadata_replay::{
    require_journal_entry_replay_payloads, require_replica_journal_entry_file_system,
};
use crate::metadata_summary::{
    metadata_summary_for_file_system_from_tables, metadata_summary_from_tables,
};
use crate::metadata_table_apply::{self, TableMaterializationState};
#[cfg(test)]
use crate::metadata_table_store::metadata_table_path;
use crate::metadata_table_store::{
    METADATA_TABLE_SNAPSHOT_FORMAT_VERSION, MetadataTableStorageGeneration,
    archive_metadata_backend_state, load_metadata_tables, metadata_table_snapshot_fingerprint,
    metadata_table_snapshot_path, metadata_table_storage_generation, metadata_tables_snapshot,
    persist_metadata_table_snapshot, read_metadata_table_snapshot_for_replica_repair,
    read_metadata_table_state_lenient,
};
use crate::metadata_tables::{
    DfsMetadataTableRollback, DfsMetadataTables, DfsMetadataTablesSnapshot,
    DfsNamespacePrepareFootprint, MetadataTableReadSnapshotCache, MetadataTableSnapshotFingerprint,
    namespace_job_receipt_eviction_delta, namespace_request_receipt_eviction_delta,
    namespace_transaction_receipt_eviction_delta,
};
use crate::metadata_tombstone::{metadata_now_unix_nanos, plan_tombstone_compaction};
use crate::{DfsFileSystem, DfsObjectKind, DfsObjectMetadata};
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
use crate::{DfsNamespaceOperation, DfsNamespaceResult};
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
#[cfg(test)]
use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use time::OffsetDateTime;

// Table-log replay is paid by every full SIFS service open. Keep the active
// append generation bounded enough that cold hydration does not spend seconds
// rebuilding a shard, while the checkpoint worker still rotates it in the
// background so foreground writes are not blocked on snapshot encoding.
const DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES: u64 = 8 * 1024 * 1024;
const MAX_DEFERRED_REPLICA_ENTRIES: u64 = 8;

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
    // A selected-only slow-boundary fault after real marker synchronization.
    // The callback observes the existing clock; it cannot replace its deadline.
    pub(crate) static SELECTED_INSTALL_AFTER_MARKER_SYNC: std::cell::RefCell<Option<(PathBuf, String, Box<dyn FnOnce(Instant)>)>> = const { std::cell::RefCell::new(None) };
    static METADATA_COMPACTION_BEFORE_SNAPSHOT_HOOK: std::cell::Cell<Option<Box<dyn FnOnce()>>> = const { std::cell::Cell::new(None) };
    static METADATA_SNAPSHOT_INSTALL_FAULT: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
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

struct ValidatedNamespacePreparePlan {
    footprint: DfsNamespacePrepareFootprint,
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
    selected_original_data_layout: Option<(DfsFileSystemId, Arc<crate::DfsSelectedOriginalDataLayout>)>,
    replica_progress: crate::DfsMetadataReplicaObserver,
    next_sequence: u64,
    tables: DfsMetadataTables,
    table_log_offset: u64,

    materialized_through_sequence: u64,
    prefix_proof: Option<DfsMetadataPrefixProof>,
    #[cfg(any(test, feature = "test-support"))]
    native_observed_write_freeze:
        crate::native_shard_trace::ObservedNative<DfsMetadataShardWriteFreeze>,
    #[cfg(any(test, feature = "test-support"))]
    native_observed_exact_frame: Option<(DfsMetadataJournalEntry, Vec<u8>)>,
    /// Authority generation observed by this backend instance after a durable
    /// membership handoff activation. This is deliberately instance-local:
    /// another already-open process must not inherit the completing process's
    /// authority merely because both share the same metadata directory.
    instance_authority_epoch: Option<u64>,
    table_snapshot_fingerprint: Option<MetadataTableSnapshotFingerprint>,
    table_storage_generation: Option<MetadataTableStorageGeneration>,
    read_snapshot_cache: Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
    content_root_catalog_snapshot_cache: Option<Arc<DfsContentRootCatalogSnapshotCache>>,
    content_root_catalog_previous_snapshot_cache: Option<Arc<DfsContentRootCatalogSnapshotCache>>,
    consistent_read_snapshot_depth: usize,
    stat_read_snapshots: Vec<stat_snapshot::StatReadSnapshot>,
    /// This non-primary handle intentionally ignored an unreadable rebuildable
    /// table-log suffix. It may serve only its last self-contained snapshot
    /// until authenticated replica repair installs a new authoritative image.
    replica_repair_only: bool,
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
    /// current journal layout, loads the materialized table state, replays local
    /// journal entries not yet reflected in tables, and finishes interrupted
    /// namespace or visible-version commits.
    pub fn new(shard_id: DfsMetadataShardId, root: impl Into<PathBuf>) -> Result<Self> {
        Self::new_with_expected_layout(shard_id, root.into(), None, false, false)
    }

    pub(crate) fn new_with_selected_layout(
        filesystem: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
        root: PathBuf,
        layout: Arc<crate::DfsSelectedOriginalDataLayout>,
    ) -> Result<Self> {
        #[cfg(any(test, feature = "test-support"))]
        layout.admit_stage(&filesystem, &shard_id, &root)?;
        Self::new_with_expected_layout(shard_id, root, Some((filesystem, layout)), false, false)
    }

    pub(crate) fn try_new_with_selected_layout(
        shard_id: DfsMetadataShardId,
        root: PathBuf,
        selected: Option<(DfsFileSystemId, Arc<crate::DfsSelectedOriginalDataLayout>)>,
    ) -> Result<Self> {
        #[cfg(any(test, feature = "test-support"))]
        if let Some((filesystem, layout)) = &selected {
            layout.admit_stage(filesystem, &shard_id, &root)?;
        }
        Self::new_with_expected_layout(shard_id, root, selected, false, true)
    }

    pub(crate) fn open_staged_native_genesis(
        shard: &crate::DfsMetadataShardConfig,
        root: PathBuf,
        layout: Option<Arc<crate::DfsSelectedOriginalDataLayout>>,
    ) -> Result<Self> {
        Self::open_staged_native_genesis_with_admission(shard, root, layout, false)
    }

    pub(crate) fn open_staged_native_genesis_with_admission(
        shard: &crate::DfsMetadataShardConfig,
        root: PathBuf,
        layout: Option<Arc<crate::DfsSelectedOriginalDataLayout>>,
        try_writer: bool,
    ) -> Result<Self> {
        if shard.epoch != 1 || !shard.replica_node_ids.is_empty() {
            return Err(MetadataBackendError::Serde("staged genesis open requires its initial singleton".into()));
        }
        let generation = crate::storage_generation::read_generation(&root)?;
        if generation.identity.file_system_id != shard.file_system_id {
            return Err(MetadataBackendError::Serde("staged genesis belongs to another filesystem".into()));
        }
        Self::new_with_expected_layout(
            DfsMetadataShardId::new(shard.shard_id.clone()), root,
            layout.map(|layout| (shard.file_system_id.clone(), layout)), true, try_writer,
        )
    }

    fn validate_expected_layout(
        shard_id: &DfsMetadataShardId,
        root: &Path,
        selected: &Option<(DfsFileSystemId, Arc<crate::DfsSelectedOriginalDataLayout>)>,
    ) -> Result<()> {
        if let Some((filesystem, layout)) = selected {
            layout.stage(filesystem, shard_id, root)?;
        } else {
            crate::selected_fixture::SelectedOriginalFixtureIdentity::reject_default_root(root)
                .map_err(|error| MetadataBackendError::Rejected {
                    code: crate::DfsMetadataRejectionCode::Other,
                    message: error.to_string(),
                })?;
        }
        Ok(())
    }

    fn new_with_expected_layout(
        shard_id: DfsMetadataShardId,
        root: PathBuf,
        selected_original_data_layout: Option<(DfsFileSystemId, Arc<crate::DfsSelectedOriginalDataLayout>)>,
        native_genesis_staging: bool,
        try_writer: bool,
    ) -> Result<Self> {
        Self::validate_expected_layout(&shard_id, &root, &selected_original_data_layout)?;
        #[cfg(test)]
        METADATA_BACKEND_OPEN_CALLS.set(METADATA_BACKEND_OPEN_CALLS.get().saturating_add(1));
        let trace_started = Instant::now();
        if !native_genesis_staging {
            crate::storage_preparation::prepare_storage_root_before_open(
                &root,
                &crate::DfsStoragePreparationScope::Metadata(shard_id.clone()),
            )?;
        }
        let generation = crate::storage_generation::read_generation(&root)?;
        if generation.identity.shard_id != shard_id
            || (matches!(generation.state, crate::storage_generation::GenerationState::Staged { .. })
                && !(native_genesis_staging && generation.identity.is_native_genesis()))
        {
            return Err(MetadataBackendError::StoragePreparationPending { root, processed_records: 0 });
        }
        if selected_original_data_layout.is_some() {
            Self::validate_expected_layout(&shard_id, &root, &selected_original_data_layout)?;
        }
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

            materialized_through_sequence: 0,
            prefix_proof: None,
            #[cfg(any(test, feature = "test-support"))]
            native_observed_write_freeze: crate::native_shard_trace::ObservedNative::NotObserved,
            #[cfg(any(test, feature = "test-support"))]
            native_observed_exact_frame: None,
            instance_authority_epoch: None,
            selected_original_data_layout,
            table_snapshot_fingerprint: None,
            table_storage_generation: None,
            read_snapshot_cache: Arc::new(Mutex::new(None)),
            content_root_catalog_snapshot_cache: None,
            content_root_catalog_previous_snapshot_cache: None,
            consistent_read_snapshot_depth: 0,
            stat_read_snapshots: Vec::new(),
            replica_repair_only: false,
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
            if backend.selected_original_data_layout.is_some() {
                Self::validate_expected_layout(&backend.shard_id, &backend.root, &backend.selected_original_data_layout)?;
            }
            // A resident try-checkout can hold publication/startup admission
            // needed by the migration owner retaining this physical fence.
            // Ordinary open retains its existing wait for short-lived writers.
            let _lock = if try_writer {
                crate::metadata_lock::try_metadata_write_lock(
                    backend.root.clone(), backend.journal_write_lock_path(),
                )?.ok_or_else(|| MetadataBackendError::StoragePreparationPending {
                    root: backend.root.clone(), processed_records: 0,
                })?
            } else {
                backend.metadata_write_lock()?
            };
            // Preparation may exchange this root before custody is acquired.
            // Re-enter admission before recovering or writing the new pathname.
            if crate::storage_generation::read_generation(&backend.root)? != generation {
                return Err(MetadataBackendError::StoragePreparationPending {
                    root: backend.root.clone(), processed_records: 0,
                });
            }
            if backend.selected_original_data_layout.is_some() {
                Self::validate_expected_layout(&backend.shard_id, &backend.root, &backend.selected_original_data_layout)?;
            }
            backend.next_sequence = crate::metadata_journal::admission::certify_for_write(
                &backend.root,
                &backend.shard_id,
            )?;
            let state = load_metadata_tables(&backend.root)?;
            if state.generation.is_none() && !native_genesis_staging {
                return Err(MetadataBackendError::Serde("active metadata unit has no current generation checkpoint".into()));
            }
            if state.generation.as_ref().is_some_and(|identity| identity != &generation.identity) {
                return Err(MetadataBackendError::Serde("snapshot and root storage generation identities differ".into()));
            }
            let materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
            backend.tables = state.tables;
            backend.tables.clear_repair_index_changes();
            backend.table_log_offset = state.table_log_offset;

            backend.materialized_through_sequence = materialized_through_sequence;
            backend.prefix_proof = state.prefix_proof;
            backend.table_snapshot_fingerprint = state.snapshot_fingerprint;
            backend.ensure_local_materialization_cursor()?;
            // A lagging cursor is only a scheduling hint. The exact table prefix
            // below roots replay, so reopening a clean resident handle must not
            // persist an otherwise unnecessary cursor advance. Missing/ahead
            // hints and genuine unapplied journal tails still use recovery.
            let bootstrapped_prefix = backend.ensure_metadata_prefix_proof_checkpoint_unlocked()?;
            backend.rewind_local_materialization_cursor_to_table_head_if_ahead()?;
            backend.recover_unmaterialized_local_journal_entries()?;
            if !native_genesis_staging && generation.identity.is_native_genesis() {
                let actual = crate::storage_generation::native_genesis_activation_digest(&generation.identity, &backend.tables)?;
                if generation.state != (crate::storage_generation::GenerationState::Active { activation_digest: actual }) {
                    return Err(MetadataBackendError::Serde("native storage generation receipt differs from committed genesis".into()));
                }
            }
            require_version_pin_table_invariants(&backend.tables)?;
            if bootstrapped_prefix {
                // Persist the genuine current empty prefix before the first
                // operation, so a cold reopen starts from the same anchor.
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
            total_ms = trace_started.elapsed().as_millis(),
            "metadata-backend-open"
        );
        if backend.selected_original_data_layout.is_some() {
            Self::validate_expected_layout(&backend.shard_id, &backend.root, &backend.selected_original_data_layout)?;
        }
        metadata_table_apply::schedule_metadata_table_checkpoint_with_layout(
            &backend.root,
            &backend.tables,
            DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES,
            backend.table_log_offset,
            Some(&backend.read_snapshot_cache),
            backend.selected_original_data_layout.as_ref().map(|(_, layout)| Arc::clone(layout)),
        )?;
        if backend.selected_original_data_layout.is_some() {
            Self::validate_expected_layout(&backend.shard_id, &backend.root, &backend.selected_original_data_layout)?;
        }
        Ok(backend)
    }

    /// Open a shard for read-only inspection/status.
    ///
    /// Unlike [`Self::new`], this does not take write locks, repair cursors,
    /// author semantic writes. Status RPCs use
    /// this path so probing a live daemon does not turn into a recovery pass.
    pub fn open_for_inspection(
        shard_id: DfsMetadataShardId,
        root: impl Into<PathBuf>,
    ) -> Result<Self> {
        Self::open_for_inspection_with_layout(shard_id, root.into(), None)
    }

    pub(crate) fn open_for_inspection_with_layout(
        shard_id: DfsMetadataShardId,
        root: PathBuf,
        selected_original_data_layout: Option<(DfsFileSystemId, Arc<crate::DfsSelectedOriginalDataLayout>)>,
    ) -> Result<Self> {
        Self::validate_expected_layout(&shard_id, &root, &selected_original_data_layout)?;
        #[cfg(test)]
        METADATA_BACKEND_OPEN_CALLS.set(METADATA_BACKEND_OPEN_CALLS.get().saturating_add(1));
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
        if selected_original_data_layout.is_some() {
            Self::validate_expected_layout(&shard_id, &root, &selected_original_data_layout)?;
        }
        Ok(Self {
            shard_id,
            replica_progress: crate::metadata_replica_observer::subscribe(&root),
            root,
            next_sequence,
            tables: state.tables,
            table_log_offset: state.table_log_offset,

            materialized_through_sequence,
            prefix_proof: state.prefix_proof,
            #[cfg(any(test, feature = "test-support"))]
            native_observed_write_freeze: crate::native_shard_trace::ObservedNative::NotObserved,
            #[cfg(any(test, feature = "test-support"))]
            native_observed_exact_frame: None,
            instance_authority_epoch: None,
            selected_original_data_layout,
            table_snapshot_fingerprint: state.snapshot_fingerprint,
            table_storage_generation,
            read_snapshot_cache: Arc::new(Mutex::new(None)),
            content_root_catalog_snapshot_cache: None,
            content_root_catalog_previous_snapshot_cache: None,
            consistent_read_snapshot_depth: 0,
            stat_read_snapshots: Vec::new(),
            replica_repair_only: false,
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
        Self::open_for_replica_repair_with_layout(shard_id, root.into(), None)
    }

    pub(crate) fn open_for_replica_repair_with_layout(
        shard_id: DfsMetadataShardId,
        root: PathBuf,
        selected_original_data_layout: Option<(DfsFileSystemId, Arc<crate::DfsSelectedOriginalDataLayout>)>,
    ) -> Result<Self> {
        Self::validate_expected_layout(&shard_id, &root, &selected_original_data_layout)?;
        let root = root;
        create_dir_all(root.join("records"))?;
        create_dir_all(root.join("journal").join("segments"))?;
        if selected_original_data_layout.is_some() {
            Self::validate_expected_layout(&shard_id, &root, &selected_original_data_layout)?;
        }
        let _lock = metadata_write_lock(
            root.clone(),
            metadata_cursor::journal_write_lock_path(&root),
        )?;
        if selected_original_data_layout.is_some() {
            Self::validate_expected_layout(&shard_id, &root, &selected_original_data_layout)?;
        }
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

            materialized_through_sequence,
            prefix_proof: state.prefix_proof,
            #[cfg(any(test, feature = "test-support"))]
            native_observed_write_freeze: crate::native_shard_trace::ObservedNative::NotObserved,
            #[cfg(any(test, feature = "test-support"))]
            native_observed_exact_frame: None,
            instance_authority_epoch: None,
            selected_original_data_layout,
            table_snapshot_fingerprint: state.snapshot_fingerprint,
            table_storage_generation: None,
            read_snapshot_cache: Arc::new(Mutex::new(None)),
            content_root_catalog_snapshot_cache: None,
            content_root_catalog_previous_snapshot_cache: None,
            consistent_read_snapshot_depth: 0,
            stat_read_snapshots: Vec::new(),
            replica_repair_only: true,
            resident_cursor_status_projection: Arc::new(Mutex::new(
                ResidentMetadataCursorStatusProjection::default(),
            )),
            #[cfg(test)]
            receipt_scan_counters: Arc::new(MetadataReceiptScanCounters::default()),
            #[cfg(test)]
            content_root_catalog_snapshot_builds: Arc::new(AtomicUsize::new(0)),
        };
        if backend.selected_original_data_layout.is_some() {
            Self::validate_expected_layout(&backend.shard_id, &backend.root, &backend.selected_original_data_layout)?;
        }
        backend.ensure_local_materialization_cursor()?;
        backend.reconcile_local_materialization_cursor(materialized_through_sequence)?;
        backend.ensure_metadata_prefix_proof_checkpoint_unlocked()?;
        if backend.selected_original_data_layout.is_some() {
            Self::validate_expected_layout(&backend.shard_id, &backend.root, &backend.selected_original_data_layout)?;
        }
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

    pub(crate) fn persist_authority_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
        expected_target_prefix: &DfsMetadataPrefixProof,
        expected_operation: &crate::DfsMetadataAuthorityOperation,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.persist_exact_journal_frames_with_replica_permit_report(
            permit,
            batch,
            frames,
            Some(expected_target_prefix),
            Some(expected_operation),
        )
    }

    /// Last local or replayed journal sequence visible to this shard.
    pub fn journal_last_sequence(&self) -> u64 {
        self.next_sequence.saturating_sub(1)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn replica_materialization_boundary_state(
        &self,
    ) -> (u64, u64, Option<DfsMetadataPrefixProof>) {
        (
            self.next_sequence.saturating_sub(1),
            self.materialized_through_sequence,
            self.prefix_proof.clone(),
        )
    }

    /// Observe only an existing certified physical cut. A foreign writer,
    /// changed table generation or absent certificate is unavailable, never an
    /// invitation to open storage, refresh tables or replay its journal.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn observe_existing_materialization_cut(
        &self,
    ) -> Result<Option<(u64, u64, Option<DfsMetadataPrefixProof>)>> {
        let Some(_fence) = crate::metadata_lock::try_metadata_inspection_lock(
            self.root.clone(), self.journal_write_lock_path(),
        )? else { return Ok(None); };
        if self.table_storage_generation.as_ref()
            != Some(&metadata_table_storage_generation(&self.root)?)
        { return Ok(None); }
        let Some(durable) = crate::metadata_journal::admission::current_owned_materialized_prefix(
            &self.root, &self.shard_id,
        )? else { return Ok(None); };
        let Some(materialized) = self.prefix_proof.as_ref().filter(|prefix|
            prefix.is_well_formed() && prefix.journal_sequence == self.materialized_through_sequence
                && prefix.journal_sequence <= durable.journal_sequence)
        else { return Ok(None); };
        Ok(Some((durable.journal_sequence, materialized.journal_sequence, Some(materialized.clone()))))
    }

    /// Read a bounded slice from the durable journal tail, including entries
    /// appended by another backend instance after this one opened.

    /// Export the current materialized table state for replica snapshot repair.
    /// Inspection handles without a certified prefix cannot produce this DATA.
    pub fn export_table_snapshot(&self) -> Result<DfsMetadataTableSnapshot> {
        #[cfg(test)]
        METADATA_EXPORT_TABLE_SNAPSHOT_CALLS
            .set(METADATA_EXPORT_TABLE_SNAPSHOT_CALLS.get().saturating_add(1));
        let mut snapshot = DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            materialized_through_sequence: self.materialized_through_sequence,
            tables: Box::new(metadata_tables_snapshot(&self.tables)),
            semantic_state_fingerprint: String::new(),
            prefix_proof: self
                .prefix_proof
                .clone()
                .ok_or_else(|| MetadataBackendError::Serde(
                    "current snapshot export requires a certified prefix".into(),
                ))?,
        };
        snapshot.semantic_state_fingerprint =
            dfs_metadata_table_snapshot_state_fingerprint(&snapshot)
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        Ok(snapshot)
    }

    pub fn export_snapshot_image(&self) -> Result<crate::DfsMetadataSnapshotImage> {
        let proof = self.prefix_proof.as_ref().ok_or_else(|| {
            MetadataBackendError::Serde("current image export has no certified prefix".into())
        })?;
        crate::DfsMetadataSnapshotImage::capture(
            &self.root,
            &self.tables,
            self.materialized_through_sequence,
            proof,
        )
    }

    /// Replace local shard tables with an authoritative replicated snapshot.
    ///
    /// The snapshot source must name this backend's logical shard. Cross-shard
    /// movement uses the explicit migration protocol rather than replication.
    /// The previous shard state is archived before replacement. The return
    /// value reports whether existing local state was archived.
    /// Install a validated replica snapshot across only the exact activated
    /// author fence named by `permit`. The durable marker is excluded from the
    /// repair archive and this path never changes process-local authority.
    pub fn replace_with_replicated_snapshot_with_replica_permit(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: crate::DfsMetadataSnapshotImage,
    ) -> Result<bool> {
        let candidate = snapshot.state()?;
        require_replica_snapshot_tables_file_system(&candidate.tables, permit.file_system_id())?;
        permit.validate_counted_snapshot(&candidate.tables, candidate.generation.as_ref())?;
        drop(candidate);
        let _lock = self.begin_metadata_replica_ingest(permit, &source_shard_id)?;
        let archived = self.replace_with_replicated_snapshot_unlocked(
            permit,
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
        snapshot: crate::DfsMetadataSnapshotImage,
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
        let candidate = snapshot.state()?;
        require_replica_snapshot_tables_file_system(&candidate.tables, permit.file_system_id())?;
        permit.validate_counted_snapshot(&candidate.tables, candidate.generation.as_ref())?;
        drop(candidate);
        let root = root.into();
        let mut backend = Self {
            shard_id,
            replica_progress: crate::metadata_replica_observer::subscribe(&root),
            root,
            next_sequence: 1,
            tables: DfsMetadataTables::default(),
            table_log_offset: 0,

            materialized_through_sequence: 0,
            prefix_proof: None,
            #[cfg(any(test, feature = "test-support"))]
            native_observed_write_freeze: crate::native_shard_trace::ObservedNative::NotObserved,
            #[cfg(any(test, feature = "test-support"))]
            native_observed_exact_frame: None,
            instance_authority_epoch: None,
            selected_original_data_layout: None,
            table_snapshot_fingerprint: None,
            table_storage_generation: None,
            read_snapshot_cache: Arc::new(Mutex::new(None)),
            content_root_catalog_snapshot_cache: None,
            content_root_catalog_previous_snapshot_cache: None,
            consistent_read_snapshot_depth: 0,
            stat_read_snapshots: Vec::new(),
            replica_repair_only: false,
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
            && !backend.clear_freeze_obsoleted_by_write_set_source(permit, &freeze)?
        {
            return Err(MetadataBackendError::MembershipHandoffFrozen {
                shard_id: backend.shard_id.clone(),
                handoff_id: freeze.handoff_id,
            });
        }
        backend.require_source_epoch_accepted(Some(permit.source_epoch()))?;
        let archived_existing = backend.replace_with_replicated_snapshot_unlocked(
            permit,
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
        permit: &DfsMetadataReplicaIngestPermit,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: crate::DfsMetadataSnapshotImage,
    ) -> Result<bool> {
        if snapshot.materialized_through_sequence != replayed_through_sequence {
            return Err(MetadataBackendError::Serde(format!(
                "replicated metadata snapshot embedded sequence {:?} does not match install sequence {replayed_through_sequence}",
                snapshot.materialized_through_sequence
            )));
        }
        snapshot.validate()?;
        let replacement_prefix_proof = snapshot.prefix_proof.clone();
        if !replacement_prefix_proof.is_well_formed()
            || replacement_prefix_proof.journal_sequence != replayed_through_sequence
        {
            return Err(MetadataBackendError::Serde(
                "replicated current snapshot prefix invalid".into(),
            ));
        }
        let stored_acknowledged_floor =
            metadata_cursor::metadata_prefix_ack_floor(&self.root, &source_shard_id)?;
        let stored_authority_floor =
            metadata_cursor::metadata_authority_ack_floor(&self.root, &source_shard_id)?;
        let stored = (stored_acknowledged_floor.is_some(), stored_authority_floor.is_some());
        let acknowledged_floor =
            permit.binding_ack_floor(stored_acknowledged_floor, |floor| floor.authority_epoch)?
                .filter(|floor| !permit.snapshot_replaces_converted_prefix(&floor.durable_prefix_proof, snapshot.descriptor()));
        let authority_floor =
            permit.binding_ack_floor(stored_authority_floor, |floor| floor.authority_epoch)?
                .filter(|floor| !permit.snapshot_replaces_converted_prefix(&floor.durable_prefix_proof, snapshot.descriptor()));
        // A nonbinding floor belongs to an earlier epoch or the exact source
        // clock superseded by this committed native storage cut.
        let superseded_floor = (stored.0 && acknowledged_floor.is_none())
            || (stored.1 && authority_floor.is_none());
        if let Some(floor) = authority_floor.as_ref() {
            if replayed_through_sequence < floor.journal_sequence {
                return Err(MetadataBackendError::Serde(format!(
                    "replicated metadata snapshot sequence {replayed_through_sequence} would replace shard {} behind locally acknowledged authority operation {}",
                    source_shard_id.0, floor.journal_sequence
                )));
            }
            if replayed_through_sequence == floor.journal_sequence
                && floor.durable_prefix_proof != replacement_prefix_proof
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
                if floor.durable_prefix_proof != replacement_prefix_proof {
                    return Err(MetadataBackendError::Serde(format!(
                        "replicated metadata snapshot journal prefix conflicts with locally acknowledged checkpoint for shard {} sequence {replayed_through_sequence}",
                        source_shard_id.0
                    )));
                }
                if !floor.state_fingerprint.is_empty() {
                    let matches = snapshot
                        .descriptor()
                        .matches_checkpoint(replayed_through_sequence, &floor.state_fingerprint);
                    if !matches {
                        return Err(MetadataBackendError::Serde(format!(
                            "replicated metadata snapshot conflicts with locally acknowledged checkpoint for shard {} sequence {replayed_through_sequence}",
                            source_shard_id.0
                        )));
                    }
                }
            }
        }
        let tables = snapshot.state()?.tables.clone();
        require_version_pin_table_invariants(&tables)?;
        let active_generation = permit.snapshot_generation_envelope(&snapshot.generation, &tables)?;
        if superseded_floor {
            // Replace superseded floors before the state they describe
            // is archived, so a restart in between never compares the new
            // state with them. Epoch-major merge lets the new floors replace
            // a higher sequence, including an epoch that starts empty.
            metadata_cursor::put_metadata_prefix_ack_floor(
                &self.root,
                &DfsMetadataPrefixAckFloor {
                    shard_id: source_shard_id.clone(),
                    journal_sequence: replayed_through_sequence,
                    state_fingerprint: String::new(),
                    durable_prefix_proof: replacement_prefix_proof.clone(),
                    authority_epoch: Some(permit.source_epoch()),
                },
            )?;
            metadata_cursor::put_metadata_authority_ack_floor(
                &self.root,
                &crate::DfsMetadataAuthorityAckFloor {
                    shard_id: source_shard_id.clone(),
                    journal_sequence: replayed_through_sequence,
                    durable_prefix_proof: replacement_prefix_proof.clone(),
                    authority_epoch: Some(permit.source_epoch()),
                },
            )?;
        }
        // An identical snapshot is already installed when this held writer
        // still owns a fully materialized certificate for the exact physical
        // generation. Replacing it would unnecessarily retire journal bytes
        // needed by an in-flight exact replay. Equality alone is insufficient:
        // uncertainty, repair-only state, changed sources or a different cut
        // retain the existing replacement/recovery path below.
        if !self.replica_repair_only
            && self.materialized_through_sequence == replayed_through_sequence
            && self.next_sequence == replayed_through_sequence.saturating_add(1).max(1)
            && self.prefix_proof.as_ref() == Some(&replacement_prefix_proof)
            && self.tables == tables
            && crate::metadata_journal::admission::current_owned_materialized_prefix(
                &self.root,
                &self.shard_id,
            )?
            .as_ref()
                == Some(&replacement_prefix_proof)
        {
            // Preserve the snapshot RPC's progress-hint postconditions. These
            // hints do not replace or advance the already-validated proof.
            metadata_cursor::write_replay_cursor(
                &self.root,
                &DfsMetadataReplayCursor {
                    shard_id: source_shard_id.clone(),
                    last_sequence: replayed_through_sequence,
                },
            )?;
            self.observe_resident_metadata_replay_cursor(
                &source_shard_id,
                replayed_through_sequence,
            );
            self.write_local_materialization_cursor(&DfsMetadataReplayCursor {
                shard_id: self.shard_id.clone(),
                last_sequence: replayed_through_sequence,
            })?;
            metadata_cursor::write_journal_head(&self.root, self.next_sequence)?;
            return Ok(false);
        }
        let staged_snapshot = snapshot.stage_candidate_copy(&self.root)?;
        crate::metadata_journal::admission::invalidate(&self.root)?;
        let archived_existing = archive_metadata_backend_state(&self.root)?;
        #[cfg(test)]
        snapshot_install_fault(&self.root, "after-archive")?;
        // The header remains absent until the whole validated image is durable.
        // A crash therefore enters authenticated replica repair, rather than
        // masquerading as an unsealed migration/genesis candidate.
        create_dir_all(self.root.join("records/tables"))?;
        if let Some(floor) = acknowledged_floor.as_ref() {
            metadata_cursor::put_metadata_prefix_ack_floor(&self.root, floor)?;
        }
        create_dir_all(self.root.join("journal").join("segments"))?;
        let repair_index_changed = self.tables.repair_index() != tables.repair_index();
        self.tables = tables;
        self.tables.clear_repair_index_changes();
        if repair_index_changed {
            // An installed snapshot replaces the index wholesale; the
            // primary re-reads the shard's index.
            crate::repair_executor::repair_index_changed(
                &self.root,
                &self.shard_id,
                crate::repair_executor::DfsRepairIndexChange::Replaced,
            );
        }
        self.replica_repair_only = false;
        self.table_log_offset = 0;

        self.next_sequence = replayed_through_sequence.saturating_add(1).max(1);
        self.materialized_through_sequence = replayed_through_sequence;
        self.prefix_proof = Some(replacement_prefix_proof);
        staged_snapshot.install(&self.root)?;
        crate::metadata_table_store::truncate_metadata_table_log(&self.root)?;
        crate::metadata_table_store::remove_metadata_table_checkpoint_log(&self.root)?;
        self.table_snapshot_fingerprint = metadata_table_snapshot_fingerprint(&self.root)?;
        #[cfg(test)]
        snapshot_install_fault(&self.root, "after-snapshot")?;
        crate::storage_generation::write_generation(&self.root, &active_generation)?;
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
        self.append_preapplied_table_ops(table_sequence, table_ops, rollback)?;
        if let Some(entry) = entries.last() {
            self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
        }
        Ok(())
    }

    /// Read at most `max_entries` journal entries after `after_sequence`.
    ///
    /// This lower-level reader is for bounded diagnostics.
    /// Replication uses `journal_batch_since_limited`, which returns complete
    /// current payloads and enforces encoded-size bounds.
    pub fn journal_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let _lock = self.metadata_write_lock()?;
        // Another owner-backed handle can append after this reader opened.
        // The existing durable reader observes that suffix without treating a
        // cached writer sequence or deferred head hint as proof of absence.
        crate::metadata_journal::durable_journal_entries_since_limited(
            &self.root,
            after_sequence,
            max_entries,
        )
    }

    /// Build a bounded, payload-complete journal batch after `after_sequence`.
    ///
    /// Current entries are self-contained; encoded byte bounds are applied
    /// directly to their durable bodies and digest-only markers.
    pub fn journal_batch_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        let entries = self.journal_since_limited(after_sequence, max_entries)?;
        bounded_metadata_journal_batch(
            self.shard_id.clone(),
            after_sequence,
            entries,
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

        if self.materialized_through_sequence != 0 || self.tables.has_materialized_state() {
            return Err(MetadataBackendError::Serde("existing metadata lacks a certified current exact prefix; external preparation required".into()));
        }
        self.prefix_proof = Some(crate::metadata_records::dfs_metadata_prefix_proof_genesis(
            &self.shard_id,
        ));
        Ok(true)
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
        let later_prefix = &later_floor.durable_prefix_proof;
        if !later_prefix.is_well_formed()
            || later_prefix.journal_sequence != later_floor.journal_sequence
            || later_prefix.journal_sequence <= expected_prefix.journal_sequence
            || later_prefix.journal_sequence > persisted_through_sequence
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} cannot authenticate historical authority prefix {} without a later durable exact prefix floor",
                expected_shard_id.0, expected_prefix.journal_sequence
            )));
        }

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
        if let Some(view) = self.stat_read_snapshots.last() {
            return Ok(view.prefix.clone());
        }
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

    fn write_journal_unlocked(
        &mut self,
        mutation: DfsMetadataMutation,
    ) -> Result<DfsMetadataJournalEntry> {
        self.write_journal_unlocked_with_record_bytes(mutation)
            .map(|(entry, _)| entry)
    }

    fn write_journal_unlocked_with_record_bytes(
        &mut self,
        mutation: DfsMetadataMutation,
    ) -> Result<(DfsMetadataJournalEntry, u64)> {
        let frame = crate::metadata_journal::DfsMetadataJournalFrame::local(
            &self.shard_id,
            self.next_sequence,
            mutation,
        )?;
        self.write_journal_frame_unlocked(&frame, &mut None)
    }

    fn write_journal_frame_unlocked(
        &mut self,
        frame: &crate::metadata_journal::DfsMetadataJournalFrame<'_>,
        rollback: &mut Option<DfsMetadataTableRollback>,
    ) -> Result<(DfsMetadataJournalEntry, u64)> {
        if let Err(error) = crate::metadata_journal::admission::mark_uncertain(&self.root) {
            if let Some(rollback) = rollback.take() {
                rollback.rollback(&mut self.tables);
            }
            return Err(error);
        }
        let record_bytes = match write_local_journal_frame_with_bytes(
            &self.root,
            &mut self.next_sequence,
            frame,
        ) {
            Ok(written) => written,
            Err(error) => {
                // Restore the actual pre-plan view BEFORE recovery can replay
                // a complete or uncertain commit frame from durable storage.
                if let Some(rollback) = rollback.take() {
                    rollback.rollback(&mut self.tables);
                }
                return Err(self.recover_after_replayed_journal_persistence_error(error));
            }
        };
        let entry = frame.entry().clone();
        let exact_bytes = frame.exact_bytes_arc();
        #[cfg(test)]
        METADATA_JOURNAL_WRITE_COUNT.set(METADATA_JOURNAL_WRITE_COUNT.get().saturating_add(1));
        let certified = (|| -> Result<()> {
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
            #[cfg(any(test, feature = "test-support"))]
            if crate::native_shard_trace::has_interest(&self.root, &self.shard_id) {
                self.native_observed_exact_frame =
                    Some((entry.clone(), exact_bytes.as_ref().to_vec()));
            }
            Ok(())
        })();
        if let Err(error) = certified {
            if let Some(rollback) = rollback.take() {
                rollback.rollback(&mut self.tables);
            }
            return Err(self.recover_after_replayed_journal_persistence_error(error));
        }
        Ok((entry, record_bytes))
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
        let prefix = self.prefix_proof.as_ref().filter(|proof| {
            proof.is_well_formed() && proof.journal_sequence == self.materialized_through_sequence
        }).ok_or_else(|| MetadataBackendError::Serde(format!(
            "metadata shard {} cannot recover a journal suffix without an exact materialized prefix at {}",
            self.shard_id.0, self.materialized_through_sequence,
        )))?;
        if cursor.shard_id != self.shard_id {
            return Err(MetadataBackendError::Serde(
                "local materialization hint names a foreign metadata shard".into(),
            ));
        }
        // The cursor is a scheduling hint and may lag a newer table checkpoint.
        // Only the independently certified table cut can root exact replay;
        // replay and final journal admission still verify every suffix frame.
        cursor.last_sequence = prefix.journal_sequence;
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
            for entry in &entries {
                require_journal_entry_replay_payloads(entry)?;
            }
            self.advance_metadata_prefix_proof_unlocked(&entries)?;
            if defer_cursor_checkpoint {
                self.materialize_written_journal_entries_without_cursor_checkpoint_unlocked(
                    &entries,
                )?;
            } else {
                self.materialize_written_journal_entries_unlocked(&entries)?;
            }
            let last_sequence = entries
                .last()
                .expect("nonempty current journal suffix")
                .sequence;
            let advanced = last_sequence > cursor.last_sequence;
            cursor.last_sequence = cursor.last_sequence.max(last_sequence);
            if advanced {
                if self.materialized_through_sequence < cursor.last_sequence
                    && self
                        .prefix_proof
                        .as_ref()
                        .is_some_and(|proof| proof.journal_sequence == cursor.last_sequence)
                {
                    // Ordered replay may advance the exact prefix
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

    fn journal_write_lock_path(&self) -> PathBuf {
        metadata_cursor::journal_write_lock_path(&self.root)
    }

    fn metadata_write_lock(&self) -> Result<MetadataWriteLock> {
        if !self.stat_read_snapshots.is_empty() {
            return Err(MetadataBackendError::Serde(
                "a stat read view grants no writer custody".into(),
            ));
        }
        metadata_write_lock(self.root.clone(), self.journal_write_lock_path())
    }

    /// A resident maintenance reader must not wait on an adopted source's
    /// physical fence while retaining the node's mutation admission. A held
    /// fence is Pending; a successful attempt performs the ordinary exact
    /// writer-fenced refresh before pinning the same coherent read scope.
    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.metadata.read_snapshot",
        skip_all,
        fields(stage = "dfs.metadata.read_snapshot")
    )]
    #[cfg(test)]
    pub(crate) fn try_begin_resident_read_snapshot(&mut self) -> Result<bool> {
        if self.consistent_read_snapshot_depth == 0
            && self.try_materialize_resident_journal()?.is_none()
        {
            tracing::trace!(target: "sinomo.dfs.perf",
                stage = "dfs.repair.receipt_projection.native_cut", outcome = "source_fence_busy",
                backend = %format_args!("{:p}", self), root = %self.root.display(),
                shard_id = ?self.shard_id, actual_prefix = ?self.prefix_proof,
                actual_materialized_sequence = self.materialized_through_sequence,
                actual_snapshot_depth = self.consistent_read_snapshot_depth,
                "existing resident snapshot opener did not acquire a coherent cut");
            return Ok(false);
        }
        self.consistent_read_snapshot_depth = self.consistent_read_snapshot_depth.saturating_add(1);
        tracing::trace!(target: "sinomo.dfs.perf",
            stage = "dfs.repair.receipt_projection.native_cut", outcome = "ready",
            backend = %format_args!("{:p}", self), root = %self.root.display(),
            shard_id = ?self.shard_id, actual_prefix = ?self.prefix_proof,
            actual_materialized_sequence = self.materialized_through_sequence,
            actual_snapshot_depth = self.consistent_read_snapshot_depth,
            "borrowed native prefix after existing resident snapshot admission");
        Ok(true)
    }

    /// Preserve the exact ordinary refresh/replay boundary, but let a native
    /// background owner relinquish admission while another source holds the
    /// physical fence. None is no acquired view and no replay completion.
    pub(crate) fn try_materialize_resident_journal(&mut self) -> Result<Option<usize>> {
        let before = self.materialized_through_sequence;
        let Some(_lock) = crate::metadata_lock::try_metadata_write_lock(
            self.root.clone(),
            self.journal_write_lock_path(),
        )?
        else {
            return Ok(None);
        };
        self.refresh_after_metadata_write_lock()?;
        Ok(Some(
            self.materialized_through_sequence.saturating_sub(before) as usize,
        ))
    }

    fn reload_durable_metadata_table_state(&mut self) -> Result<()> {
        #[cfg(test)]
        METADATA_DURABLE_TABLE_RELOADS.set(METADATA_DURABLE_TABLE_RELOADS.get().saturating_add(1));
        let state = read_metadata_table_state_lenient(&self.root)?;
        self.tables = state.tables;
        self.tables.clear_repair_index_changes();
        self.table_log_offset = state.table_log_offset;

        self.materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
        self.prefix_proof = state.prefix_proof;
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
        let observed_freeze = self.membership_handoff_write_freeze()?;
        #[cfg(any(test, feature = "test-support"))]
        crate::native_shard_trace::observe_replica_write_freeze_read(
            &lock,
            permit,
            &observed_freeze,
            self,
        );
        if let Some(freeze) = observed_freeze
            && !permit.admits_write_freeze(&freeze)
            && !self.clear_freeze_obsoleted_by_write_set_source(permit, &freeze)?
        {
            return Err(MetadataBackendError::MembershipHandoffFrozen {
                shard_id: self.shard_id.clone(),
                handoff_id: freeze.handoff_id,
            });
        }
        // A member fenced at a later epoch, by its own takeover promise,
        // ingests nothing more from this source.
        self.require_source_epoch_accepted(Some(permit.source_epoch()))?;
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
        let observed_freeze = self.membership_handoff_write_freeze()?;
        #[cfg(any(test, feature = "test-support"))]
        crate::native_shard_trace::observe_replica_write_freeze_read(
            &lock,
            permit,
            &observed_freeze,
            self,
        );
        if let Some(freeze) = observed_freeze
            && !permit.admits_write_freeze(&freeze)
            && !self.clear_freeze_obsoleted_by_write_set_source(permit, &freeze)?
        {
            return Err(MetadataBackendError::MembershipHandoffFrozen {
                shard_id: self.shard_id.clone(),
                handoff_id: freeze.handoff_id,
            });
        }
        // A member fenced at a later epoch, by its own takeover promise,
        // ingests nothing more from this source.
        self.require_source_epoch_accepted(Some(permit.source_epoch()))?;
        self.next_sequence =
            self.next_sequence
                .max(crate::metadata_journal::admission::certify_for_write(
                    &self.root,
                    &self.shard_id,
                )?);

        Ok(lock)
    }

    fn begin_metadata_write(&mut self) -> Result<MetadataWriteLock> {
        let lock = self.metadata_write_lock()?;
        self.validate_retained_metadata_writer()?;
        Ok(lock)
    }

    /// Called only while the existing ordinary or sealed source writer is held.
    fn validate_retained_metadata_writer(&mut self) -> Result<()> {
        let observed_freeze = self.membership_handoff_write_freeze()?;
        #[cfg(any(test, feature = "test-support"))]
        {
            self.native_observed_write_freeze =
                if crate::native_shard_trace::has_interest(&self.root, &self.shard_id) {
                    observed_freeze.clone().into()
                } else {
                    crate::native_shard_trace::ObservedNative::NotObserved
                };
        }
        if let Some(freeze) = observed_freeze {
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
        // The native empty-genesis cut certifies a wholly empty shard. A
        // certified imported zero cut has a different exact prefix and may
        // retain rows. Check all current tables after refreshing under the
        // actual writer, before allowing a fresh genesis over retained data.
        if self.next_sequence == 1
            && self.prefix_proof.as_ref()
                == Some(&crate::metadata_records::dfs_metadata_prefix_proof_genesis(
                    &self.shard_id,
                ))
            && self.tables != DfsMetadataTables::default()
        {
            return Err(MetadataBackendError::Serde(
                "native empty-genesis cut contains retained metadata; certified import or repair is required".into(),
            ));
        }
        Ok(())
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
        self.record_owned_metadata_materialization()?;
        self.announce_repair_index_change();
        Ok(())
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
        self.record_owned_metadata_materialization()?;
        self.announce_repair_index_change();
        Ok(())
    }

    /// The shard writer and the replica applier both append through the two
    /// wrappers above: one wake per applied batch that changed repair needs,
    /// naming the objects whose needs changed.
    fn announce_repair_index_change(&mut self) {
        let objects = self.tables.take_repair_index_changed();
        if !objects.is_empty() {
            crate::repair_executor::repair_index_changed(
                &self.root,
                &self.shard_id,
                crate::repair_executor::DfsRepairIndexChange::Objects(objects),
            );
        }
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
            selected_original_data_layout: self.selected_original_data_layout.as_ref().map(|(_, layout)| Arc::clone(layout)),
            read_snapshot_cache: Some(&self.read_snapshot_cache),
            tables: &mut self.tables,
            table_log_offset: &mut self.table_log_offset,

            materialized_through_sequence: &mut self.materialized_through_sequence,
            table_snapshot_fingerprint: &mut self.table_snapshot_fingerprint,
            prefix_proof: &mut self.prefix_proof,
        }
    }

    fn refresh_metadata_tables(&mut self) -> Result<()> {
        if self.replica_repair_only {
            // Another normal replica owner may have materialized the durable
            // tail since this handle captured its last self-contained snapshot.
            // Load that exact current table cut under the already-held writer
            // before suffix recovery; an unreadable cut remains an error.
            // Only authenticated snapshot installation clears repair-only mode.
            return self.reload_durable_metadata_table_state();
        }
        let current_storage_generation = metadata_table_storage_generation(&self.root)?;
        metadata_read_snapshot::ensure_table_log_cursor_generation(
            &self.root,
            &self.table_storage_generation,
            &current_storage_generation,
            self.table_log_offset,
        )?;
        // A read-only lookup may already have decoded this durable generation.
        // Under the writer fence, adopt the fully validated physical image. An
        // equal sequence also requires the same exact prefix: checkpoint
        // rotation alone must not force a second decode of that same cut.
        // Incremental-only reader overlays lack complete replay accounting and
        // remain ineligible. Journal certification/recovery still runs below.
        let cached = if let Ok(mut cache) = self.read_snapshot_cache.lock() {
            let eligible = cache.as_ref().is_some_and(|cache| {
                cache.storage_generation.as_ref() == Some(&current_storage_generation)
                    && cache.snapshot_format_version.is_some()
                    && cache.materialized_through_sequence.is_some_and(|sequence| {
                        (sequence > self.materialized_through_sequence
                            || (sequence == self.materialized_through_sequence
                                && cache.prefix_proof == self.prefix_proof))
                            && cache.prefix_proof.as_ref().is_some_and(|proof| {
                                proof.is_well_formed() && proof.journal_sequence == sequence
                            })
                    })
                    && self.prefix_proof.as_ref().is_some_and(|proof| {
                        proof.is_well_formed()
                            && proof.journal_sequence == self.materialized_through_sequence
                    })
            });
            if eligible {
                let candidate = cache.as_ref().expect("eligible read snapshot cache");
                // A lock-free read can observe a same-file shrink after replay
                // and stamp that shorter generation onto its longer cursor.
                // Reject that cut before replacing any live writer state.
                metadata_read_snapshot::ensure_table_log_cursor_generation(
                    &self.root,
                    &candidate.storage_generation,
                    &current_storage_generation,
                    candidate.table_log_offset,
                )?;
                cache.take()
            } else {
                None
            }
        } else {
            None
        };
        if let Some(cache) = cached {
            self.tables = Arc::unwrap_or_clone(cache.tables);
            self.tables.clear_repair_index_changes();
            self.table_log_offset = cache.table_log_offset;

            self.materialized_through_sequence = cache.materialized_through_sequence.unwrap();
            self.prefix_proof = cache.prefix_proof;
            self.table_snapshot_fingerprint = cache.snapshot_fingerprint;
            self.table_storage_generation = Some(current_storage_generation.clone());
            metadata_backend_perf_trace(format_args!(
                "metadata-read-snapshot-cache-adopted shard={} materialized_sequence={} table_log_offset={}",
                self.shard_id.0, self.materialized_through_sequence, self.table_log_offset,
            ));
        }
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
        if let Some(view) = self.stat_read_snapshots.last() {
            return Ok(match &view.tables {
                Some(tables) => DfsMetadataTablesSnapshot::Shared {
                    tables: Arc::clone(tables),
                    prefix_proof: Some(view.prefix.clone()),
                    materialized_through_sequence: Some(view.prefix.journal_sequence),
                },
                None => DfsMetadataTablesSnapshot::Borrowed(&self.tables),
            });
        }
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

    fn plan_cross_shard_pin_transfer_current(
        &self,
        transfer_id: &str,
        object: &DfsObjectMetadata,
        source_path: &str,
        versions: &[DfsObjectVersionRecord],
        source_pins: &[DfsMetadataVersionPinRecord],
    ) -> Result<crate::metadata_api::DfsCrossShardPinTransferPlan> {
        let source_path = normalize_path(source_path);
        let target_path = normalize_path(&object.path);
        if transfer_id.trim().is_empty()
            || transfer_id.len() > 256
            || source_path == target_path
            || source_pins.len() > MAX_DURABLE_VERSION_PINS_PER_OBJECT
        {
            return Err(MetadataBackendError::Serde(format!(
                "invalid bounded cross-shard pin transfer {} for {} -> {}",
                transfer_id, source_path, target_path
            )));
        }
        let versions = versions
            .iter()
            .map(normalized_version_record)
            .collect::<Vec<_>>();
        for version in &versions {
            if version.object_id != object.object_id
                || version.file_system_id != object.file_system_id
            {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} received unrelated version {}/{}",
                    transfer_id, version.object_id, version.version
                )));
            }
        }

        let tables = self.current_tables_snapshot()?;
        let transfer_key = (object.object_id.clone(), transfer_id.to_string());
        let previous = tables.cross_shard_pin_transfers.get(&transfer_key).cloned();
        if source_pins.is_empty() && previous.is_none() {
            return Ok(crate::metadata_api::DfsCrossShardPinTransferPlan {
                effects: Vec::new(),
                active: false,
            });
        }
        if tables
            .cross_shard_pin_transfers
            .values()
            .any(|other| other.object_id == object.object_id && other.transfer_id != transfer_id)
        {
            return Err(MetadataBackendError::Serde(format!(
                "object {} already has another active cross-shard pin transfer",
                object.object_id
            )));
        }
        if let Some(previous) = previous.as_ref()
            && (previous.file_system_id != object.file_system_id
                || previous.object_id != object.object_id
                || normalize_path(&previous.source_path) != source_path
                || normalize_path(&previous.target_path) != target_path
                || previous.target_version != object.version)
        {
            return Err(MetadataBackendError::Serde(format!(
                "cross-shard pin transfer {} was reused with a different handoff identity",
                transfer_id
            )));
        }

        let previous_owned = previous
            .as_ref()
            .map(|record| record.copied_pins.iter().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        let mut source_keys = BTreeSet::new();
        let mut owned_pins = Vec::new();
        for pin in source_pins {
            pin_validation::validate_version_pin_payload(
                &pin.file_system_id,
                &pin.object_id,
                &pin.version,
                &pin.pin_id,
                pin,
            )?;
            if pin.file_system_id != object.file_system_id || pin.object_id != object.object_id {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} received unrelated source pin {} for {}/{}",
                    transfer_id, pin.pin_id, pin.object_id, pin.version
                )));
            }
            let key = (
                pin.object_id.clone(),
                pin.version.clone(),
                pin.pin_id.clone(),
            );
            if !source_keys.insert(key.clone()) {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} repeats source pin {} for {}/{}",
                    transfer_id, pin.pin_id, pin.object_id, pin.version
                )));
            }
            let pinned_version = tables
                .versions
                .get(&(pin.object_id.clone(), pin.version.clone()))
                .or_else(|| {
                    versions
                        .iter()
                        .find(|version| version.version == pin.version)
                })
                .ok_or_else(|| {
                    MetadataBackendError::Serde(format!(
                        "cross-shard pin transfer {} is missing destination version {}/{}",
                        transfer_id, pin.object_id, pin.version
                    ))
                })?;
            if pinned_version.file_system_id != pin.file_system_id {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} source pin filesystem mismatch",
                    transfer_id
                )));
            }
            if let Some(owner) =
                tables.cross_shard_pin_transfer_owner(&pin.object_id, &pin.version, &pin.pin_id)
                && owner.transfer_id != transfer_id
            {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin {} for {}/{} is already owned by transfer {}",
                    pin.pin_id, pin.object_id, pin.version, owner.transfer_id
                )));
            }
            match tables.version_pins.get(&key) {
                Some(existing) if existing != pin => {
                    return Err(MetadataBackendError::Serde(format!(
                        "cross-shard pin transfer {} conflicts with target pin {} for {}/{}",
                        transfer_id, pin.pin_id, pin.object_id, pin.version
                    )));
                }
                Some(_) if previous_owned.contains(pin) => owned_pins.push(pin.clone()),
                Some(_) => {
                    // An exact pre-existing target pin is independent state.
                    // It satisfies the source consumer without becoming
                    // transfer-owned or eligible for stale-copy removal.
                }
                None => owned_pins.push(pin.clone()),
            }
        }
        owned_pins.sort();
        let mut acknowledged_pins = source_pins.to_vec();
        acknowledged_pins.sort();
        let now = time::OffsetDateTime::now_utc();
        let unchanged = previous.as_ref().is_some_and(|previous| {
            previous.copied_pins == owned_pins && previous.source_pins == acknowledged_pins
        });
        let transfer = DfsCrossShardPinTransferRecord {
            transfer_id: transfer_id.to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            source_path,
            target_path,
            target_version: object.version.clone(),
            source_pins: acknowledged_pins,
            copied_pins: owned_pins,
            created_at: previous
                .as_ref()
                .map(|previous| previous.created_at)
                .unwrap_or(now),
            updated_at: if unchanged {
                previous
                    .as_ref()
                    .map(|previous| previous.updated_at)
                    .unwrap_or(now)
            } else {
                now
            },
        };
        if pin_validation::validate_cross_shard_pin_transfer_table_transition(
            &tables,
            &object.file_system_id,
            &object.object_id,
            transfer_id,
            false,
            &versions,
            &transfer,
        )? {
            return Ok(crate::metadata_api::DfsCrossShardPinTransferPlan {
                effects: Vec::new(),
                active: true,
            });
        }
        Ok(crate::metadata_api::DfsCrossShardPinTransferPlan {
            effects: vec![DfsPreparedMetadataEffect::ReconcileCrossShardPinTransfer {
                versions,
                transfer,
            }],
            active: true,
        })
    }

    fn plan_foreign_shard_source_eviction_current(
        &self,
        moved_object: &DfsObjectMetadata,
        old_path: &str,
        captured: &DfsCrossShardSourceFamily,
        transfer_id: Option<&str>,
        target_shard_id: &DfsMetadataShardId,
    ) -> Result<crate::metadata_api::DfsForeignShardSourceEvictionPlan> {
        if moved_object.object_id != captured.source_object.object_id
            || moved_object.file_system_id != captured.source_object.file_system_id
            || normalize_path(old_path) != normalize_path(&captured.source_object.path)
        {
            return Err(MetadataBackendError::Serde(
                "source eviction capture changes object/filesystem/path identity".into(),
            ));
        }
        let tables = self.current_tables_snapshot()?;
        let current = tables
            .objects
            .get(&moved_object.object_id)
            .or_else(|| tables.compacted_tombstones.get(&moved_object.object_id))
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: moved_object.object_id.clone(),
            })?;
        if current != &captured.source_object
            || self.journal_last_sequence() != captured.source_journal_sequence
        {
            return Err(MetadataBackendError::Serde(
                "source eviction family changed after capture".into(),
            ));
        }
        let current_pins = tables
            .version_pins
            .values()
            .filter(|pin| pin.object_id == moved_object.object_id)
            .cloned()
            .collect::<BTreeSet<_>>();
        let captured_pins = captured
            .version_pins
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let current_transfers = tables
            .cross_shard_pin_transfers
            .values()
            .filter(|transfer| transfer.object_id == moved_object.object_id)
            .cloned()
            .collect::<Vec<_>>();
        let captured_transfers = captured.pin_transfers.iter().collect::<Vec<_>>();
        if current_pins != captured_pins
            || current_transfers.len() != captured_transfers.len()
            || current_transfers
                .iter()
                .any(|transfer| !captured_transfers.contains(&transfer))
        {
            return Err(MetadataBackendError::Serde(
                "source eviction exact pin/transfer set changed".into(),
            ));
        }
        let mut plan = crate::metadata_api::DfsForeignShardSourceEvictionPlan {
            physical_source_shard: self.shard_id.clone(),
            expected_objects: vec![DfsNamespaceObjectVersionExpectation {
                object_id: current.object_id.clone(),
                expected_version: Some(current.version.clone()),
            }],
            expected_paths: vec![DfsNamespacePathOccupantExpectation {
                path: current.path.clone(),
                expected_object_id: object_by_path_from_tables(
                    &tables,
                    &current.file_system_id,
                    &current.path,
                )
                .map(|occupant| occupant.object_id),
            }],
            expected_journal_sequence: captured.source_journal_sequence,
            effects: Vec::new(),
        };
        if let Some(tombstone) = crate::metadata_shard_migration::cross_shard_eviction_tombstone(
            current,
            moved_object,
            &self.shard_id,
            target_shard_id,
            transfer_id,
        ) {
            pin_validation::validate_cross_shard_source_eviction_table_transition(
                &tables,
                &moved_object.file_system_id,
                &moved_object.object_id,
                &tombstone,
                &captured.version_pins,
                &captured.pin_transfers,
            )?;
            plan.effects.push(
                DfsPreparedMetadataEffect::FinalizeCrossShardSourceEviction {
                    tombstone,
                    removed_pins: captured.version_pins.clone(),
                    removed_pin_transfers: captured.pin_transfers.clone(),
                },
            );
        }
        Ok(plan)
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

    pub(crate) fn plan_metadata_history_effects(
        &mut self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<crate::metadata_api::DfsMetadataHistoryEffectsPlan> {
        use crate::DfsPreparedMetadataEffect as E;
        let _lock = self.begin_metadata_write()?;
        let mut plan = plan_metadata_history_gc(
            self.shard_id.clone(),
            &self.tables,
            &options,
            fs::metadata(metadata_table_snapshot_path(&self.root))
                .ok()
                .map(|m| m.len()),
        )?;
        self.populate_metadata_history_storage_stats(&mut plan.report)?;
        let at = options
            .now_unix_nanos
            .and_then(|n| OffsetDateTime::from_unix_timestamp_nanos(i128::from(n)).ok())
            .unwrap_or_else(OffsetDateTime::now_utc);
        let mut effects = BTreeMap::<DfsFileSystemId, Vec<E>>::new();
        for intent in plan.retained_repair_owners {
            effects
                .entry(intent.file_system_id.clone())
                .or_default()
                .push(E::PutIntent { intent });
        }
        for removal in &plan.removals {
            let (fs, effect) = match removal {
                DfsMetadataHistoryRemoval::Receipt {
                    file_system_id,
                    object_id,
                    version,
                    replica_node_id,
                } => (
                    file_system_id.clone(),
                    E::RemoveReceipt {
                        object_id: object_id.clone(),
                        version: version.clone(),
                        replica_node_id: replica_node_id.clone(),
                    },
                ),
                DfsMetadataHistoryRemoval::Version {
                    file_system_id,
                    object_id,
                    version,
                } => (
                    file_system_id.clone(),
                    E::RemoveVersion {
                        object_id: object_id.clone(),
                        version: version.clone(),
                    },
                ),
                DfsMetadataHistoryRemoval::Intent {
                    file_system_id,
                    object_id,
                    intent_id,
                } => {
                    let intent = self
                        .tables
                        .intents
                        .get(&(object_id.clone(), intent_id.clone()))
                        .ok_or_else(|| {
                            MetadataBackendError::Serde("GC selected absent intent".into())
                        })?;
                    let effect = if intent.publication.is_retained_repair() {
                        E::RemoveRetainedRepairOwner {
                            object_id: object_id.clone(),
                            intent_id: intent_id.clone(),
                        }
                    } else {
                        E::PutIntentReceipt {
                            receipt: DfsWriteIntentReceipt::from_intent(intent, at).ok_or_else(
                                || {
                                    MetadataBackendError::Serde(
                                        "GC selected nonterminal intent".into(),
                                    )
                                },
                            )?,
                        }
                    };
                    (file_system_id.clone(), effect)
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
                            MetadataBackendError::Serde("GC selected absent intent receipt".into())
                        })?;
                    (
                        file_system_id.clone(),
                        E::RemoveIntentReceipt {
                            object_id: object_id.clone(),
                            intent_id: intent_id.clone(),
                            retention: DfsWriteIntentReceiptRetentionState::eviction_delta(receipt),
                        },
                    )
                }
                DfsMetadataHistoryRemoval::NamespaceJob {
                    file_system_id,
                    job_id,
                } => {
                    let job = self.tables.namespace_jobs.get(job_id).ok_or_else(|| {
                        MetadataBackendError::Serde("GC selected absent job".into())
                    })?;
                    (
                        file_system_id.clone(),
                        E::PutNamespaceJobReceipt {
                            receipt: DfsNamespaceJobReceipt::from_job(job, at).ok_or_else(
                                || MetadataBackendError::Serde("GC selected active job".into()),
                            )?,
                        },
                    )
                }
                DfsMetadataHistoryRemoval::PreparedOperation {
                    file_system_id,
                    operation_id,
                } => {
                    let record = self
                        .tables
                        .prepared_operations_v7
                        .get(operation_id)
                        .ok_or_else(|| {
                            MetadataBackendError::Serde(
                                "GC selected absent prepared operation".into(),
                            )
                        })?;
                    (
                        file_system_id.clone(),
                        E::RemovePreparedOperation {
                            digest: record.digest.clone(),
                        },
                    )
                }
                DfsMetadataHistoryRemoval::NamespaceRequest {
                    file_system_id,
                    requester_node_id,
                    request_id,
                } => {
                    let request = self
                        .tables
                        .namespace_requests
                        .get(&(requester_node_id.clone(), request_id.clone()))
                        .ok_or_else(|| {
                            MetadataBackendError::Serde("GC selected absent request".into())
                        })?;
                    (
                        file_system_id.clone(),
                        E::PutNamespaceRequestReceipt {
                            receipt: Box::new(
                                DfsNamespaceRequestReceipt::from_request(request, at).ok_or_else(
                                    || {
                                        MetadataBackendError::Serde(
                                            "GC selected pending request".into(),
                                        )
                                    },
                                )?,
                            ),
                        },
                    )
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
                                MetadataBackendError::Serde("GC selected absent job receipt".into())
                            })?;
                    (
                        file_system_id.clone(),
                        E::RemoveNamespaceJobReceipt {
                            job_id: job_id.clone(),
                            retention: namespace_job_receipt_eviction_delta(receipt),
                        },
                    )
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
                            MetadataBackendError::Serde(
                                "GC selected absent transaction receipt".into(),
                            )
                        })?;
                    (
                        file_system_id.clone(),
                        E::RemoveNamespaceTransactionReceipt {
                            transaction_id: transaction_id.clone(),
                            retention: namespace_transaction_receipt_eviction_delta(receipt),
                        },
                    )
                }
                DfsMetadataHistoryRemoval::NamespaceRequestReceipt {
                    file_system_id,
                    requester_node_id,
                    request_id,
                } => {
                    let receipt = self
                        .tables
                        .namespace_request_receipts
                        .get(&(requester_node_id.clone(), request_id.clone()))
                        .ok_or_else(|| {
                            MetadataBackendError::Serde("GC selected absent request receipt".into())
                        })?;
                    (
                        file_system_id.clone(),
                        E::RemoveNamespaceRequestReceipt {
                            requester_node_id: requester_node_id.clone(),
                            request_id: request_id.clone(),
                            retention: namespace_request_receipt_eviction_delta(receipt),
                        },
                    )
                }
            };
            effects.entry(fs).or_default().push(effect);
        }
        Ok(crate::metadata_api::DfsMetadataHistoryEffectsPlan {
            report: plan.report,
            effects_by_scope: effects
                .into_iter()
                .map(|(fs, effects)| ((fs, self.shard_id.clone()), effects))
                .collect(),
        })
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
}

#[cfg(test)]
fn snapshot_install_fault(root: &Path, point: &'static str) -> Result<()> {
    if METADATA_SNAPSHOT_INSTALL_FAULT.get() == Some(point) {
        METADATA_SNAPSHOT_INSTALL_FAULT.set(None);
        return Err(MetadataBackendError::Io { path: root.to_owned(),
            source: std::io::Error::new(std::io::ErrorKind::Interrupted, point) });
    }
    Ok(())
}

#[cfg(test)]
mod tests;

mod replay;

mod alignment;
mod handoff;
#[cfg(test)]
pub(crate) mod native_prefix_history;
mod storage_generation;

mod write_set_member_state;
pub use write_set_member_state::{
    DFS_METADATA_WRITE_SET_MEMBER_STATE_FORMAT, DfsMetadataPendingWriteSetAdd,
    DfsMetadataTakeoverPromise, DfsMetadataTakeoverPromiseEnd, DfsMetadataWriteSetMemberState,
};

mod source_adoption;
pub(crate) use source_adoption::DfsSourceAdoptionWriterCustody;
pub use source_adoption::{
    DfsSourceAdoptionCapture, DfsSourceAdoptionJobCapture, DfsSourceAdoptionSourceCapture,
};

mod namespace_prepare;
pub(crate) mod prepared_operation;

mod pin_validation;
use pin_validation::*;

pub(crate) mod snapshot_validation;
use snapshot_validation::*;

mod api;
mod reference_outcomes;
mod stat_snapshot;
