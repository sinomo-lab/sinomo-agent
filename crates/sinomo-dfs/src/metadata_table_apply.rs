//! Applying metadata table mutations to the durable table log.
//!
//! The backend plans table operations in several places: local writes,
//! journal replay, recovery, and repair. This module centralizes the common
//! append/apply/compact flow so those paths share the same durability behavior.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use fs2::FileExt;

use crate::DfsMetadataShardWriteFreeze;
use crate::metadata_cursor::journal_write_lock_path;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::{read_sobj_optional, write_sobj_atomic};
use crate::metadata_journal::{
    durable_journal_entries_since_limited, metadata_prefix_proof_after_exact_frame,
    prune_segmented_journal_before_checkpoint, read_segmented_journal_frame_at_sequence,
};
use crate::metadata_lock::{
    MetadataWriteActivity, metadata_checkpoint_write_lock, metadata_write_activity,
};
#[cfg(test)]
use crate::metadata_records::dfs_metadata_prefix_proof_after;
use crate::metadata_records::{
    DfsMetadataPrefixProof, DfsMetadataTableBatch, DfsMetadataTableOp, DfsNamespacePrepareRecord,
};
use crate::metadata_table_store::{
    append_metadata_table_batch, current_metadata_table_log_replay,
    metadata_table_checkpoint_log_path, metadata_table_path, metadata_table_snapshot_fingerprint,
    persist_metadata_table_snapshot, read_metadata_table_checkpoint_state,
    remove_metadata_table_checkpoint_log,
    rotate_metadata_table_log_for_checkpoint_at_replay_boundary,
};
#[cfg(test)]
use crate::metadata_table_store::{
    encode_metadata_table_snapshot, install_encoded_metadata_table_snapshot,
    rotate_metadata_table_log_for_checkpoint,
};
use crate::metadata_tables::{
    DfsMetadataTableRollback, DfsMetadataTables, DfsNamespacePrepareFootprint,
    MetadataTableSnapshotFingerprint,
};

static METADATA_TABLE_CHECKPOINT_WORKERS: OnceLock<MetadataTableCheckpointWorkerRegistry> =
    OnceLock::new();
static METADATA_TABLE_CHECKPOINT_FOLD: OnceLock<Mutex<()>> = OnceLock::new();
static METADATA_TABLE_CHECKPOINT_LAST_FOLD_FINISHED: OnceLock<Mutex<Option<Instant>>> =
    OnceLock::new();
const METADATA_TABLE_CHECKPOINT_COORDINATION_LOCK_ENV: &str =
    "SINOMO_DFS_METADATA_CHECKPOINT_COORDINATION_LOCK";
#[cfg(not(test))]
const METADATA_TABLE_CHECKPOINT_COORDINATION_WAIT: Duration = Duration::from_millis(500);
#[cfg(test)]
const METADATA_TABLE_CHECKPOINT_COORDINATION_WAIT: Duration = Duration::from_millis(25);
const METADATA_TABLE_CHECKPOINT_COORDINATION_POLL: Duration = Duration::from_millis(10);
#[cfg(test)]
static METADATA_TABLE_CHECKPOINT_FOLDS_ACTIVE: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static METADATA_TABLE_CHECKPOINT_FOLDS_MAX: AtomicUsize = AtomicUsize::new(0);

#[cfg(not(test))]
const METADATA_TABLE_LOGICAL_CHECKPOINT_QUIET_PERIOD: Duration = Duration::from_secs(30);
#[cfg(test)]
const METADATA_TABLE_LOGICAL_CHECKPOINT_QUIET_PERIOD: Duration = Duration::from_millis(10);
#[cfg(not(test))]
const METADATA_TABLE_LOGICAL_CHECKPOINT_STAGGER_MAX: Duration = Duration::from_secs(45);
#[cfg(test)]
const METADATA_TABLE_LOGICAL_CHECKPOINT_STAGGER_MAX: Duration = Duration::from_millis(40);
#[cfg(not(test))]
const METADATA_TABLE_LOGICAL_CHECKPOINT_HARD_STAGGER_MAX: Duration = Duration::from_secs(15);
#[cfg(test)]
const METADATA_TABLE_LOGICAL_CHECKPOINT_HARD_STAGGER_MAX: Duration = Duration::from_millis(20);
#[cfg(not(test))]
const METADATA_TABLE_LOGICAL_CHECKPOINT_PROCESS_SPACING: Duration = Duration::from_secs(15);
#[cfg(test)]
const METADATA_TABLE_LOGICAL_CHECKPOINT_PROCESS_SPACING: Duration = Duration::ZERO;
#[cfg(not(test))]
const METADATA_TABLE_PHYSICAL_CHECKPOINT_STAGGER_MAX: Duration = Duration::from_secs(4);
#[cfg(test)]
const METADATA_TABLE_PHYSICAL_CHECKPOINT_STAGGER_MAX: Duration = Duration::from_millis(40);
const METADATA_TABLE_PHYSICAL_CHECKPOINT_HARD_PRESSURE_MULTIPLIER: u64 = 2;
const METADATA_TABLE_LOGICAL_CHECKPOINT_HARD_PRESSURE_MULTIPLIER: u64 = 2;
const METADATA_TABLE_LOGICAL_CHECKPOINT_EMERGENCY_PRESSURE_MULTIPLIER: u64 = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetadataTableCheckpointTrigger {
    Physical,
    ReplayPressureSoft,
    ReplayPressureHard,
    ReplayPressureEmergency,
}

#[derive(Debug)]
struct MetadataTableCheckpointCoordinationGuard {
    file: fs::File,
}

#[derive(Debug)]
enum MetadataTableCheckpointCoordination {
    Disabled,
    Acquired(MetadataTableCheckpointCoordinationGuard),
    Contended,
}

impl Drop for MetadataTableCheckpointCoordinationGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn metadata_table_checkpoint_coordination_file(path: &Path) -> Result<fs::File> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| MetadataBackendError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|source| MetadataBackendError::Io {
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(test)]
fn metadata_table_checkpoint_coordination_lock_at(
    path: &Path,
) -> Result<MetadataTableCheckpointCoordinationGuard> {
    let file = metadata_table_checkpoint_coordination_file(path)?;
    FileExt::lock_exclusive(&file).map_err(|source| MetadataBackendError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(MetadataTableCheckpointCoordinationGuard { file })
}

fn metadata_table_checkpoint_coordination_lock() -> Result<MetadataTableCheckpointCoordination> {
    let Some(path) = std::env::var_os(METADATA_TABLE_CHECKPOINT_COORDINATION_LOCK_ENV)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
    else {
        return Ok(MetadataTableCheckpointCoordination::Disabled);
    };
    let file = metadata_table_checkpoint_coordination_file(&path)?;
    let deadline = Instant::now() + METADATA_TABLE_CHECKPOINT_COORDINATION_WAIT;
    loop {
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => {
                return Ok(MetadataTableCheckpointCoordination::Acquired(
                    MetadataTableCheckpointCoordinationGuard { file },
                ));
            }
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    return Ok(MetadataTableCheckpointCoordination::Contended);
                };
                std::thread::sleep(METADATA_TABLE_CHECKPOINT_COORDINATION_POLL.min(remaining));
            }
            Err(source) => {
                return Err(MetadataBackendError::Io {
                    path: path.clone(),
                    source,
                });
            }
        }
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub(crate) fn release_metadata_allocator_pages() -> bool {
    unsafe extern "C" {
        fn malloc_trim(pad: usize) -> std::ffi::c_int;
    }

    // Table folds and replica replay temporarily own large decoded metadata
    // bodies. glibc otherwise keeps those thread arenas at the high-water
    // mark, so co-located shards accumulate their RSS after the values drop.
    unsafe { malloc_trim(0) != 0 }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub(crate) fn release_metadata_allocator_pages() -> bool {
    false
}

impl MetadataTableCheckpointTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::Physical => "physical",
            Self::ReplayPressureSoft => "replay-pressure-soft",
            Self::ReplayPressureHard => "replay-pressure-hard",
            Self::ReplayPressureEmergency => "replay-pressure-emergency",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct MetadataTableCheckpointWorkerState {
    revision: u64,
    requested_at: Instant,
    last_activity_at: Instant,
    has_pending_namespace_prepares: bool,
    physical_checkpoint_required: bool,
    physical_checkpoint_not_before: Instant,
    replay_pressure_boundary: Option<u64>,
    replay_cost_bytes: u64,
    unpriced_namespace_prepare_references: u64,
    max_log_bytes: u64,
    logical_checkpoint_stagger: Duration,
    logical_hard_checkpoint_not_before: Option<Instant>,
    logical_emergency_pressure: bool,
}

#[derive(Default)]
struct MetadataTableCheckpointWorkerRegistry {
    workers: Mutex<BTreeMap<PathBuf, MetadataTableCheckpointWorkerState>>,
    changed: Condvar,
}

#[derive(Clone, Copy, Debug)]
struct MetadataTableCheckpointWorkerRequest {
    revision: u64,
    requested_at: Instant,
    eligible_at: Instant,
    trigger: MetadataTableCheckpointTrigger,
    deferral_reason: &'static str,
    replay_pressure_boundary: Option<u64>,
    replay_cost_bytes: u64,
    unpriced_namespace_prepare_references: u64,
    max_log_bytes: u64,
    hard_threshold_bytes: u64,
    emergency_threshold_bytes: u64,
    process_spacing_applied: bool,
}

#[cfg(test)]
struct MetadataTableCheckpointFoldObservation;

#[cfg(test)]
impl MetadataTableCheckpointFoldObservation {
    fn enter() -> Self {
        let active = METADATA_TABLE_CHECKPOINT_FOLDS_ACTIVE
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        METADATA_TABLE_CHECKPOINT_FOLDS_MAX.fetch_max(active, Ordering::SeqCst);
        Self
    }
}

#[cfg(test)]
impl Drop for MetadataTableCheckpointFoldObservation {
    fn drop(&mut self) {
        METADATA_TABLE_CHECKPOINT_FOLDS_ACTIVE.fetch_sub(1, Ordering::SeqCst);
    }
}

// This is a decode/allocation work estimate, not an encoded-size promise.
// Counting already-built prepare records avoids rebuilding or serializing the
// full authoritative body on the foreground path.
const NAMESPACE_PREPARE_REPLAY_BASE_BYTES: u64 = 1_024;
const NAMESPACE_PREPARE_REPLAY_BYTES_PER_RECORD: u64 = 256;

fn metadata_table_checkpoint_perf_trace(args: std::fmt::Arguments<'_>) {
    tracing::trace!(
        target: "sinomo.dfs.perf",
        detail = %args,
        "dfs metadata table checkpoint perf"
    );
}

fn metadata_table_checkpoint_stagger_for_identity(
    identity: &[u8],
    max_stagger: Duration,
) -> Duration {
    let bucket_count = max_stagger.as_millis().min(u64::MAX as u128) as u64;
    if bucket_count == 0 {
        return Duration::ZERO;
    }
    let digest = blake3::hash(identity);
    let mut first_word = [0_u8; 8];
    first_word.copy_from_slice(&digest.as_bytes()[..8]);
    Duration::from_millis(u64::from_le_bytes(first_word) % bucket_count)
}

fn metadata_table_checkpoint_stagger_for_root(
    root: &Path,
    domain: &[u8],
    max_stagger: Duration,
) -> Duration {
    let mut identity = blake3::Hasher::new();
    identity.update(domain);
    identity.update(root.to_string_lossy().as_bytes());
    for variable in ["HOSTNAME", "COMPUTERNAME", "NODE_NAME"] {
        if let Some(value) = std::env::var_os(variable) {
            identity.update(variable.as_bytes());
            identity.update(value.to_string_lossy().as_bytes());
        }
    }
    if let Ok(metadata) = fs::metadata(root) {
        identity.update(&metadata.len().to_le_bytes());
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            identity.update(&metadata.dev().to_le_bytes());
            identity.update(&metadata.ino().to_le_bytes());
            identity.update(&metadata.ctime().to_le_bytes());
            identity.update(&metadata.ctime_nsec().to_le_bytes());
        }
        if let Ok(modified) = metadata.modified()
            && let Ok(since_epoch) = modified.duration_since(std::time::UNIX_EPOCH)
        {
            identity.update(&since_epoch.as_nanos().to_le_bytes());
        }
        if let Ok(created) = metadata.created()
            && let Ok(since_epoch) = created.duration_since(std::time::UNIX_EPOCH)
        {
            identity.update(&since_epoch.as_nanos().to_le_bytes());
        }
    }
    metadata_table_checkpoint_stagger_for_identity(identity.finalize().as_bytes(), max_stagger)
}

fn metadata_table_physical_checkpoint_stagger(root: &Path) -> Duration {
    metadata_table_checkpoint_stagger_for_root(
        root,
        b"physical",
        METADATA_TABLE_PHYSICAL_CHECKPOINT_STAGGER_MAX,
    )
}

fn metadata_table_logical_checkpoint_stagger(root: &Path) -> Duration {
    metadata_table_checkpoint_stagger_for_root(
        root,
        b"logical-soft",
        METADATA_TABLE_LOGICAL_CHECKPOINT_STAGGER_MAX,
    )
}

fn metadata_table_logical_hard_checkpoint_stagger(root: &Path) -> Duration {
    metadata_table_checkpoint_stagger_for_root(
        root,
        b"logical-hard",
        METADATA_TABLE_LOGICAL_CHECKPOINT_HARD_STAGGER_MAX,
    )
}

fn metadata_table_logical_hard_threshold(max_log_bytes: u64) -> u64 {
    max_log_bytes.saturating_mul(METADATA_TABLE_LOGICAL_CHECKPOINT_HARD_PRESSURE_MULTIPLIER)
}

fn metadata_table_logical_emergency_threshold(max_log_bytes: u64) -> u64 {
    max_log_bytes.saturating_mul(METADATA_TABLE_LOGICAL_CHECKPOINT_EMERGENCY_PRESSURE_MULTIPLIER)
}

fn metadata_table_logical_checkpoint_pressure(
    replay_cost_bytes: u64,
    unpriced_namespace_prepare_references: u64,
    max_log_bytes: u64,
) -> Option<MetadataTableCheckpointTrigger> {
    if unpriced_namespace_prepare_references > 0 {
        return Some(MetadataTableCheckpointTrigger::ReplayPressureEmergency);
    }
    if replay_cost_bytes <= max_log_bytes {
        return None;
    }
    if replay_cost_bytes >= metadata_table_logical_emergency_threshold(max_log_bytes) {
        return Some(MetadataTableCheckpointTrigger::ReplayPressureEmergency);
    }
    if replay_cost_bytes >= metadata_table_logical_hard_threshold(max_log_bytes) {
        return Some(MetadataTableCheckpointTrigger::ReplayPressureHard);
    }
    (replay_cost_bytes > max_log_bytes)
        .then_some(MetadataTableCheckpointTrigger::ReplayPressureSoft)
}

fn metadata_table_physical_checkpoint_is_hard_pressure(
    table_log_bytes: u64,
    max_log_bytes: u64,
) -> bool {
    table_log_bytes
        >= max_log_bytes.saturating_mul(METADATA_TABLE_PHYSICAL_CHECKPOINT_HARD_PRESSURE_MULTIPLIER)
}

fn metadata_table_physical_checkpoint_not_before(
    root: &Path,
    requested_at: Instant,
    table_log_bytes: u64,
    max_log_bytes: u64,
    checkpoint_generation_present: bool,
) -> Instant {
    if checkpoint_generation_present
        || metadata_table_physical_checkpoint_is_hard_pressure(table_log_bytes, max_log_bytes)
    {
        return requested_at;
    }
    requested_at
        .checked_add(metadata_table_physical_checkpoint_stagger(root))
        .unwrap_or(requested_at)
}

fn metadata_table_checkpoint_process_not_before() -> Option<Instant> {
    METADATA_TABLE_CHECKPOINT_LAST_FOLD_FINISHED
        .get()
        .and_then(|last| *last.lock().unwrap_or_else(|poisoned| poisoned.into_inner()))
        .and_then(|finished| {
            finished.checked_add(METADATA_TABLE_LOGICAL_CHECKPOINT_PROCESS_SPACING)
        })
}

fn metadata_table_checkpoint_apply_process_spacing(
    trigger: MetadataTableCheckpointTrigger,
    root_eligible_at: Instant,
    process_not_before: Option<Instant>,
) -> (Instant, bool) {
    if matches!(
        trigger,
        MetadataTableCheckpointTrigger::Physical
            | MetadataTableCheckpointTrigger::ReplayPressureEmergency
    ) {
        return (root_eligible_at, false);
    }
    let process_spacing_applied =
        process_not_before.is_some_and(|not_before| not_before > root_eligible_at);
    let eligible_at = process_not_before.map_or(root_eligible_at, |not_before| {
        not_before.max(root_eligible_at)
    });
    (eligible_at, process_spacing_applied)
}

fn record_metadata_table_checkpoint_fold_finished() {
    let mut last = METADATA_TABLE_CHECKPOINT_LAST_FOLD_FINISHED
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *last = Some(Instant::now());
    drop(last);
    if let Some(registry) = METADATA_TABLE_CHECKPOINT_WORKERS.get() {
        registry.changed.notify_all();
    }
}

fn rearm_metadata_table_soft_checkpoint_after_preemption(
    root: &Path,
    request: MetadataTableCheckpointWorkerRequest,
) {
    if request.trigger != MetadataTableCheckpointTrigger::ReplayPressureSoft {
        return;
    }
    let Some(registry) = METADATA_TABLE_CHECKPOINT_WORKERS.get() else {
        return;
    };
    let mut workers = registry
        .workers
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(worker) = workers.get_mut(root) else {
        return;
    };
    if !worker.physical_checkpoint_required && worker.revision == request.revision {
        worker.revision = worker.revision.saturating_add(1);
        worker.last_activity_at = Instant::now();
    }
    drop(workers);
    registry.changed.notify_all();
}

fn metadata_table_logical_soft_not_before(worker: &MetadataTableCheckpointWorkerState) -> Instant {
    worker
        .last_activity_at
        .checked_add(METADATA_TABLE_LOGICAL_CHECKPOINT_QUIET_PERIOD)
        .and_then(|quiet| quiet.checked_add(worker.logical_checkpoint_stagger))
        .unwrap_or(worker.last_activity_at)
}

fn metadata_table_logical_checkpoint_admission(
    worker: &MetadataTableCheckpointWorkerState,
) -> (MetadataTableCheckpointTrigger, Instant, &'static str) {
    if worker.logical_emergency_pressure {
        return (
            MetadataTableCheckpointTrigger::ReplayPressureEmergency,
            worker.requested_at,
            if worker.unpriced_namespace_prepare_references > 0 {
                "unpriced-reference-immediate"
            } else {
                "emergency-cap-immediate"
            },
        );
    }
    let soft_not_before = metadata_table_logical_soft_not_before(worker);
    if let Some(hard_not_before) = worker.logical_hard_checkpoint_not_before
        && hard_not_before <= soft_not_before
    {
        return (
            MetadataTableCheckpointTrigger::ReplayPressureHard,
            hard_not_before,
            "hard-cap-node-stagger",
        );
    }
    (
        MetadataTableCheckpointTrigger::ReplayPressureSoft,
        soft_not_before,
        "idle-plus-node-stagger",
    )
}

fn update_metadata_table_logical_checkpoint_pressure(
    root: &Path,
    worker: &mut MetadataTableCheckpointWorkerState,
    observed_at: Instant,
    table_log_offset: u64,
    replay_cost_bytes: u64,
    unpriced_namespace_prepare_references: u64,
    max_log_bytes: u64,
) {
    worker.replay_pressure_boundary = Some(table_log_offset);
    worker.replay_cost_bytes = replay_cost_bytes;
    worker.unpriced_namespace_prepare_references = unpriced_namespace_prepare_references;
    worker.max_log_bytes = max_log_bytes;
    match metadata_table_logical_checkpoint_pressure(
        replay_cost_bytes,
        unpriced_namespace_prepare_references,
        max_log_bytes,
    ) {
        Some(MetadataTableCheckpointTrigger::ReplayPressureEmergency) => {
            worker.logical_emergency_pressure = true;
            worker.logical_hard_checkpoint_not_before = Some(observed_at);
        }
        Some(MetadataTableCheckpointTrigger::ReplayPressureHard) => {
            if worker.logical_hard_checkpoint_not_before.is_none() {
                worker.logical_hard_checkpoint_not_before = Some(
                    observed_at
                        .checked_add(metadata_table_logical_hard_checkpoint_stagger(root))
                        .unwrap_or(observed_at),
                );
            }
        }
        Some(MetadataTableCheckpointTrigger::ReplayPressureSoft) | None => {}
        Some(MetadataTableCheckpointTrigger::Physical) => unreachable!("logical pressure helper"),
    }
}

fn preserve_metadata_table_logical_checkpoint_deadline(
    worker: &mut MetadataTableCheckpointWorkerState,
    completed_checkpoint_generation: bool,
    previous_hard_checkpoint_not_before: Option<Instant>,
    previous_emergency_pressure: bool,
) {
    if completed_checkpoint_generation {
        return;
    }
    worker.logical_emergency_pressure |= previous_emergency_pressure;
    worker.logical_hard_checkpoint_not_before = match (
        previous_hard_checkpoint_not_before,
        worker.logical_hard_checkpoint_not_before,
    ) {
        (Some(previous), Some(current)) => Some(previous.min(current)),
        (Some(previous), None) => Some(previous),
        (None, current) => current,
    };
}

pub(crate) struct TableMaterializationState<'a> {
    pub(crate) tables: &'a mut DfsMetadataTables,
    pub(crate) table_log_offset: &'a mut u64,
    pub(crate) table_log_journal_reference_replay_cost_bytes: &'a mut u64,
    pub(crate) table_log_unpriced_namespace_prepare_references: &'a mut u64,
    pub(crate) materialized_through_sequence: &'a mut u64,
    pub(crate) table_snapshot_fingerprint: &'a mut Option<MetadataTableSnapshotFingerprint>,
    pub(crate) prefix_proof: &'a mut Option<DfsMetadataPrefixProof>,
}

pub(crate) fn append_and_apply_table_ops(
    root: &Path,
    state: TableMaterializationState<'_>,
    sequence: u64,
    ops: Vec<DfsMetadataTableOp>,
    max_log_bytes: u64,
) -> Result<()> {
    append_table_batch(
        root,
        state,
        sequence,
        ops,
        None,
        None,
        max_log_bytes,
        None,
        None,
    )
}

/// Append compact rebuildable operations while applying their already
/// validated, self-contained equivalents to the live tables.
///
/// This is used only after the authoritative journal entry is durable. A
/// crash before the live apply is recovered by resolving the persisted
/// journal reference during table-log replay.
pub(crate) fn append_and_apply_table_ops_with_persisted_ops(
    root: &Path,
    state: TableMaterializationState<'_>,
    sequence: u64,
    persisted_ops: Vec<DfsMetadataTableOp>,
    apply_ops: Vec<DfsMetadataTableOp>,
    journal_reference_replay_cost_bytes: Option<u64>,
    max_log_bytes: u64,
) -> Result<()> {
    append_table_batch(
        root,
        state,
        sequence,
        persisted_ops,
        Some(apply_ops),
        journal_reference_replay_cost_bytes,
        max_log_bytes,
        None,
        None,
    )
}

/// Persist compact prepare references while reusing the footprint produced by
/// foreground shape validation for the live table-index apply.
pub(crate) fn append_and_apply_table_ops_with_namespace_prepare_footprint(
    root: &Path,
    state: TableMaterializationState<'_>,
    sequence: u64,
    persisted_ops: Vec<DfsMetadataTableOp>,
    apply_ops: Vec<DfsMetadataTableOp>,
    journal_reference_replay_cost_bytes: Option<u64>,
    max_log_bytes: u64,
    footprint: DfsNamespacePrepareFootprint,
) -> Result<()> {
    append_table_batch(
        root,
        state,
        sequence,
        persisted_ops,
        Some(apply_ops),
        journal_reference_replay_cost_bytes,
        max_log_bytes,
        None,
        Some(footprint),
    )
}

pub(crate) fn append_preapplied_table_ops(
    root: &Path,
    state: TableMaterializationState<'_>,
    sequence: u64,
    ops: Vec<DfsMetadataTableOp>,
    max_log_bytes: u64,
    rollback: DfsMetadataTableRollback,
) -> Result<()> {
    append_table_batch(
        root,
        state,
        sequence,
        ops,
        None,
        None,
        max_log_bytes,
        Some(rollback),
        None,
    )
}

/// Append compact rebuildable operations whose semantic equivalents are
/// already validated and applied, retaining a bounded replay-cost estimate
/// for any journal-referenced namespace prepare.
///
/// Replica replay uses this after decoding the authoritative prepare once. It
/// must not re-encode or clone that large body merely to price future table-log
/// replay, but a zero price would conservatively force every terminal prepare
/// into an immediate checkpoint.
pub(crate) fn append_preapplied_table_ops_with_journal_reference_replay_cost(
    root: &Path,
    state: TableMaterializationState<'_>,
    sequence: u64,
    ops: Vec<DfsMetadataTableOp>,
    journal_reference_replay_cost_bytes: u64,
    max_log_bytes: u64,
    rollback: DfsMetadataTableRollback,
) -> Result<()> {
    append_table_batch(
        root,
        state,
        sequence,
        ops,
        None,
        Some(journal_reference_replay_cost_bytes),
        max_log_bytes,
        Some(rollback),
        None,
    )
}

/// Commit an already-preapplied replay prefix together with one trailing
/// resolved semantic operation, then materialize that operation only after
/// the complete table batch is durable.
///
/// The authoritative journal is already durable on this path. If the table
/// append fails, `rollback` restores only the preapplied prefix and the
/// trailing prepare/commit remains hidden. After a successful append its
/// compact marker can be replayed after a crash, so retaining an
/// O(transaction) rollback image is both unnecessary and expensive.
pub(crate) fn append_preapplied_table_ops_then_apply_trailing_resolved_op(
    root: &Path,
    state: TableMaterializationState<'_>,
    sequence: u64,
    persisted_ops: Vec<DfsMetadataTableOp>,
    trailing_apply_op: DfsMetadataTableOp,
    journal_reference_replay_cost_bytes: u64,
    max_log_bytes: u64,
    rollback: DfsMetadataTableRollback,
) -> Result<()> {
    append_table_batch(
        root,
        state,
        sequence,
        persisted_ops,
        Some(vec![trailing_apply_op]),
        (journal_reference_replay_cost_bytes != 0).then_some(journal_reference_replay_cost_bytes),
        max_log_bytes,
        Some(rollback),
        None,
    )
}

fn trailing_resolved_op_matches_persisted_marker(
    sequence: u64,
    persisted: &DfsMetadataTableOp,
    apply: &DfsMetadataTableOp,
) -> bool {
    match (persisted, apply) {
        (
            DfsMetadataTableOp::CommitNamespacePrepare { .. },
            DfsMetadataTableOp::CommitNamespacePrepare { .. },
        ) => persisted == apply,
        (
            DfsMetadataTableOp::PutNamespacePrepareJournalReference {
                shard_id,
                journal_sequence,
                file_system_id,
                operation_id,
                operation_digest,
                owner_epoch,
                status,
                ..
            },
            DfsMetadataTableOp::PutNamespacePrepare { prepare },
        ) => {
            *journal_sequence == sequence
                && &prepare.shard_id == shard_id
                && &prepare.file_system_id == file_system_id
                && &prepare.operation_id == operation_id
                && &prepare.operation_digest == operation_digest
                && prepare.owner_epoch == *owner_epoch
                && &prepare.status == status
        }
        _ => false,
    }
}

pub(crate) fn compact_metadata_table_log_if_needed(
    root: &Path,
    state: TableMaterializationState<'_>,
    max_log_bytes: u64,
) -> Result<()> {
    let path = metadata_table_path(root);
    let has_rotated_checkpoint = metadata_table_checkpoint_log_path(root).is_file();
    let len = match fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(source) if source.kind() == io::ErrorKind::NotFound && !has_rotated_checkpoint => {
            return Ok(());
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => 0,
        Err(source) => {
            return Err(MetadataBackendError::Io {
                path: path.clone(),
                source,
            });
        }
    };
    if len <= max_log_bytes
        && *state.table_log_journal_reference_replay_cost_bytes <= max_log_bytes
        && *state.table_log_unpriced_namespace_prepare_references == 0
        && !has_rotated_checkpoint
    {
        return Ok(());
    }
    // Observable history transitions are authoritative journal mutations.
    // Snapshot-only body folding would leave an incremental replica with old
    // recovery records capable of resurrecting metadata after promotion.
    persist_metadata_table_snapshot(
        root,
        state.tables,
        Some(*state.materialized_through_sequence),
        state.prefix_proof.as_ref(),
    )?;
    *state.table_snapshot_fingerprint = metadata_table_snapshot_fingerprint(root)?;
    *state.table_log_offset = fs::metadata(&path)
        .map(|metadata| metadata.len())
        .map_err(|source| MetadataBackendError::Io { path, source })?;
    *state.table_log_journal_reference_replay_cost_bytes = 0;
    *state.table_log_unpriced_namespace_prepare_references = 0;
    let checkpoint_proof = state.prefix_proof.as_ref().ok_or_else(|| {
        MetadataBackendError::Serde(format!(
            "metadata table checkpoint at {} has no exact journal-prefix proof",
            root.display()
        ))
    })?;
    advance_activated_handoff_rebind_checkpoint(root, checkpoint_proof)?;
    // Snapshot publication and table-log truncation are durable before any
    // source history disappears. Lagging replicas observe the retained
    // checkpoint segment as a discontinuity and use snapshot repair.
    prune_segmented_journal_before_checkpoint(root, *state.materialized_through_sequence)?;
    Ok(())
}

/// Schedule table checkpointing after the foreground mutation has durably
/// appended its small materialization batch.
///
/// One worker per root captures a complete durable boundary under the metadata
/// write lock, releases the lock while folding/serializing that owned image,
/// and reacquires it only to install the snapshot plus any records appended
/// after the captured boundary.
/// Startup admission for existing snapshots. Only writer/recovery opens call
/// this; read-only inspection never schedules or migrates storage.
pub(crate) fn request_metadata_table_format_migration(root: &Path) -> Result<()> {
    let path = crate::metadata_table_store::metadata_table_snapshot_path(root);
    let needs_migration = || -> Result<bool> {
        if path.is_file() {
            return sinomo_core::storage::migration::stream::is_stream(&path)
                .map(|current| !current)
                .map_err(|source| MetadataBackendError::Io {
                    path: path.clone(),
                    source,
                });
        }
        // Legacy log-only replicas need conversion below the ordinary threshold.
        // Current-format log-only stores retain their normal compaction cadence.
        Ok(
            crate::metadata_table_store::metadata_table_log_needs_format_migration(
                &metadata_table_checkpoint_log_path(root),
            )? || crate::metadata_table_store::metadata_table_log_needs_format_migration(
                &metadata_table_path(root),
            )?,
        )
    };
    if !needs_migration()? {
        return Ok(());
    }
    let _lock = metadata_checkpoint_write_lock(root.to_path_buf(), journal_write_lock_path(root))?;
    if !needs_migration()? {
        return Ok(());
    }
    if !rotate_metadata_table_log_for_checkpoint_at_replay_boundary(root, 0, None)? {
        // A snapshot without a nonempty suffix still needs conversion. This
        // empty sealed generation durably admits checkpoint-owned work.
        crate::metadata_io::write_file_atomic(&metadata_table_checkpoint_log_path(root), &[])?;
    }
    Ok(())
}

pub(crate) fn schedule_metadata_table_checkpoint_if_needed(
    root: &Path,
    tables: &DfsMetadataTables,
    max_log_bytes: u64,
    table_log_offset: u64,
    journal_reference_replay_cost_bytes: u64,
    unpriced_namespace_prepare_references: u64,
) -> Result<()> {
    let logical_pressure = metadata_table_logical_checkpoint_pressure(
        journal_reference_replay_cost_bytes,
        unpriced_namespace_prepare_references,
        max_log_bytes,
    );
    let replay_pressure_exceeded = logical_pressure.is_some();
    let checkpoint_generation_present = metadata_table_checkpoint_log_path(root).is_file();
    let path = metadata_table_path(root);
    let len = match fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(source)
            if source.kind() == io::ErrorKind::NotFound && checkpoint_generation_present =>
        {
            0
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(MetadataBackendError::Io {
                path: path.clone(),
                source,
            });
        }
    };
    let physical_checkpoint_required = len > max_log_bytes;
    let has_pending_namespace_prepares = tables
        .namespace_prepares
        .values()
        .any(|prepare| prepare.status == crate::DfsNamespacePrepareStatus::Prepared);
    if !checkpoint_generation_present && !physical_checkpoint_required && !replay_pressure_exceeded
    {
        // A worker can already be waiting because a prepare landed in the
        // active suffix while it folded the previous generation. Deliver the
        // terminal transition and foreground activity even when that small
        // suffix does not independently cross a checkpoint threshold.
        if let Some(registry) = METADATA_TABLE_CHECKPOINT_WORKERS.get() {
            let mut workers = registry
                .workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(worker) = workers.get_mut(root) {
                worker.revision = worker.revision.saturating_add(1);
                worker.last_activity_at = Instant::now();
                worker.has_pending_namespace_prepares = has_pending_namespace_prepares;
                registry.changed.notify_all();
            }
        }
        return Ok(());
    }
    let root = root.to_path_buf();
    let registry = METADATA_TABLE_CHECKPOINT_WORKERS
        .get_or_init(MetadataTableCheckpointWorkerRegistry::default);
    {
        let mut workers = registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(worker) = workers.get_mut(&root) {
            let now = Instant::now();
            let was_physical_checkpoint_required = worker.physical_checkpoint_required;
            worker.revision = worker.revision.saturating_add(1);
            worker.last_activity_at = now;
            worker.has_pending_namespace_prepares = has_pending_namespace_prepares;
            worker.replay_cost_bytes = journal_reference_replay_cost_bytes;
            worker.unpriced_namespace_prepare_references = unpriced_namespace_prepare_references;
            worker.max_log_bytes = max_log_bytes;
            // A checkpoint generation already owned by this process is not a
            // second physical trigger. New appends during its fold land in
            // the active suffix and merely extend the logical quiet period.
            if physical_checkpoint_required {
                worker.physical_checkpoint_required = true;
                worker.replay_pressure_boundary = None;
                worker.logical_hard_checkpoint_not_before = None;
                worker.logical_emergency_pressure = false;
                if !was_physical_checkpoint_required {
                    worker.requested_at = now;
                    worker.physical_checkpoint_not_before =
                        metadata_table_physical_checkpoint_not_before(
                            &root,
                            now,
                            len,
                            max_log_bytes,
                            false,
                        );
                } else if metadata_table_physical_checkpoint_is_hard_pressure(len, max_log_bytes) {
                    worker.physical_checkpoint_not_before = now;
                }
            } else if replay_pressure_exceeded && !worker.physical_checkpoint_required {
                update_metadata_table_logical_checkpoint_pressure(
                    &root,
                    worker,
                    now,
                    table_log_offset,
                    journal_reference_replay_cost_bytes,
                    unpriced_namespace_prepare_references,
                    max_log_bytes,
                );
            }
            registry.changed.notify_all();
            return Ok(());
        }
        if !checkpoint_generation_present
            && !physical_checkpoint_required
            && has_pending_namespace_prepares
            && logical_pressure == Some(MetadataTableCheckpointTrigger::ReplayPressureSoft)
        {
            // A compact prepare reference is only an intermediate recovery
            // dependency. Folding it while the transaction is still hidden
            // makes every replica compete with the commit that will make it
            // terminal. The commit/abort append observes the same accumulated
            // pressure and schedules the eventual checkpoint.
            return Ok(());
        }
        let requested_at = Instant::now();
        let initial_physical_checkpoint_required =
            checkpoint_generation_present || physical_checkpoint_required;
        let mut worker = MetadataTableCheckpointWorkerState {
            revision: 1,
            requested_at,
            last_activity_at: requested_at,
            has_pending_namespace_prepares,
            physical_checkpoint_required: initial_physical_checkpoint_required,
            physical_checkpoint_not_before: if initial_physical_checkpoint_required {
                metadata_table_physical_checkpoint_not_before(
                    &root,
                    requested_at,
                    len,
                    max_log_bytes,
                    checkpoint_generation_present,
                )
            } else {
                requested_at
            },
            replay_pressure_boundary: None,
            replay_cost_bytes: journal_reference_replay_cost_bytes,
            unpriced_namespace_prepare_references,
            max_log_bytes,
            logical_checkpoint_stagger: metadata_table_logical_checkpoint_stagger(&root),
            logical_hard_checkpoint_not_before: None,
            logical_emergency_pressure: false,
        };
        if !initial_physical_checkpoint_required && replay_pressure_exceeded {
            update_metadata_table_logical_checkpoint_pressure(
                &root,
                &mut worker,
                requested_at,
                table_log_offset,
                journal_reference_replay_cost_bytes,
                unpriced_namespace_prepare_references,
                max_log_bytes,
            );
        }
        workers.insert(root.clone(), worker);
    }

    start_metadata_table_checkpoint_worker_with(
        &root,
        max_log_bytes,
        |worker_root, max_log_bytes| {
            spawn_metadata_table_checkpoint_worker(worker_root, max_log_bytes)
        },
    );
    Ok(())
}

fn spawn_metadata_table_checkpoint_worker(
    worker_root: PathBuf,
    max_log_bytes: u64,
) -> io::Result<()> {
    std::thread::Builder::new()
        .name("sinomo-dfs-metadata-checkpoint".to_string())
        .spawn(move || {
            let mut registration =
                MetadataTableCheckpointWorkerRegistration::new(worker_root.clone());
            let registry = METADATA_TABLE_CHECKPOINT_WORKERS
                .get()
                .expect("checkpoint worker registry was installed before spawn");
            loop {
                let attempt_revision =
                    metadata_table_checkpoint_worker_revision(registry, &worker_root);
                match run_coordinated_metadata_table_checkpoint_worker(
                    &worker_root,
                    max_log_bytes,
                ) {
                    Ok(_) => {
                        // A successful worker retires its registry entry with
                        // the same revision fence used to decide that the
                        // active suffix is idle. Do not let this old
                        // registration remove a replacement worker spawned
                        // immediately after that atomic retirement.
                        registration.disarm();
                        break;
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: "sinomo.dfs",
                            root = %worker_root.display(),
                            %error,
                            "background metadata table checkpoint failed; durable journal replay remains authoritative"
                        );
                        let Some(attempt_revision) = attempt_revision else {
                            registration.disarm();
                            break;
                        };
                        if retire_metadata_table_checkpoint_worker_if_unchanged(
                            registry,
                            &worker_root,
                            attempt_revision,
                        ) {
                            registration.disarm();
                            break;
                        }
                        // A foreground append revised the entry while this
                        // attempt failed. It deliberately did not spawn a
                        // replacement because this worker was registered, so
                        // keep ownership and service the newer request here.
                        std::thread::yield_now();
                    }
                }
            }
        })
        .map(drop)
}

fn start_metadata_table_checkpoint_worker_with(
    root: &Path,
    max_log_bytes: u64,
    mut spawn: impl FnMut(PathBuf, u64) -> io::Result<()>,
) {
    let Some(registry) = METADATA_TABLE_CHECKPOINT_WORKERS.get() else {
        return;
    };
    let Some(mut attempted_revision) = metadata_table_checkpoint_worker_revision(registry, root)
    else {
        return;
    };
    loop {
        match spawn(root.to_path_buf(), max_log_bytes) {
            Ok(()) => return,
            Err(source) => {
                if retire_metadata_table_checkpoint_worker_if_unchanged(
                    registry,
                    root,
                    attempted_revision,
                ) {
                    tracing::warn!(
                        target: "sinomo.dfs",
                        root = %root.display(),
                        %source,
                        "could not start background metadata table checkpoint; a later write will retry"
                    );
                    return;
                }
                tracing::warn!(
                    target: "sinomo.dfs",
                    root = %root.display(),
                    %source,
                    "background metadata table checkpoint spawn raced newer foreground work; retrying worker start"
                );
                let Some(revised) = metadata_table_checkpoint_worker_revision(registry, root)
                else {
                    return;
                };
                attempted_revision = revised;
                std::thread::yield_now();
            }
        }
    }
}

struct MetadataTableCheckpointWorkerRegistration {
    root: PathBuf,
    registered: bool,
}

impl MetadataTableCheckpointWorkerRegistration {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            registered: true,
        }
    }

    fn unregister(&mut self) {
        if !self.registered {
            return;
        }
        if let Some(registry) = METADATA_TABLE_CHECKPOINT_WORKERS.get() {
            registry
                .workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&self.root);
            registry.changed.notify_all();
        }
        self.registered = false;
    }

    fn disarm(&mut self) {
        self.registered = false;
    }
}

impl Drop for MetadataTableCheckpointWorkerRegistration {
    fn drop(&mut self) {
        self.unregister();
    }
}

#[cfg(test)]
pub(crate) fn wait_for_metadata_table_checkpoint_workers_for_test(root: &Path) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let running = METADATA_TABLE_CHECKPOINT_WORKERS
            .get()
            .is_some_and(|registry| {
                registry
                    .workers
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .keys()
                    .any(|worker_root| worker_root.starts_with(root))
            });
        if !running {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "background checkpoint under {} did not finish",
            root.display()
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn next_metadata_table_checkpoint_worker_request(
    root: &Path,
) -> Option<MetadataTableCheckpointWorkerRequest> {
    let registry = METADATA_TABLE_CHECKPOINT_WORKERS
        .get_or_init(MetadataTableCheckpointWorkerRegistry::default);
    let mut workers = registry
        .workers
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    loop {
        let worker = *workers.get(root)?;
        if worker.physical_checkpoint_required {
            let now = Instant::now();
            if now >= worker.physical_checkpoint_not_before {
                return Some(MetadataTableCheckpointWorkerRequest {
                    revision: worker.revision,
                    requested_at: worker.requested_at,
                    eligible_at: worker.physical_checkpoint_not_before,
                    trigger: MetadataTableCheckpointTrigger::Physical,
                    deferral_reason: if worker.physical_checkpoint_not_before == worker.requested_at
                    {
                        "physical-immediate"
                    } else {
                        "physical-node-stagger"
                    },
                    replay_pressure_boundary: None,
                    replay_cost_bytes: worker.replay_cost_bytes,
                    unpriced_namespace_prepare_references: worker
                        .unpriced_namespace_prepare_references,
                    max_log_bytes: worker.max_log_bytes,
                    hard_threshold_bytes: metadata_table_logical_hard_threshold(
                        worker.max_log_bytes,
                    ),
                    emergency_threshold_bytes: metadata_table_logical_emergency_threshold(
                        worker.max_log_bytes,
                    ),
                    process_spacing_applied: false,
                });
            }
            let wait = worker
                .physical_checkpoint_not_before
                .saturating_duration_since(now);
            let (next_workers, _) = registry
                .changed
                .wait_timeout(workers, wait)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            workers = next_workers;
            continue;
        }
        let (trigger, root_eligible_at, deferral_reason) =
            metadata_table_logical_checkpoint_admission(&worker);
        if worker.has_pending_namespace_prepares
            && trigger == MetadataTableCheckpointTrigger::ReplayPressureSoft
        {
            workers = registry
                .changed
                .wait(workers)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            continue;
        }
        let (eligible_at, process_spacing_applied) =
            metadata_table_checkpoint_apply_process_spacing(
                trigger,
                root_eligible_at,
                metadata_table_checkpoint_process_not_before(),
            );
        let now = Instant::now();
        if now >= eligible_at {
            return Some(MetadataTableCheckpointWorkerRequest {
                revision: worker.revision,
                requested_at: worker.requested_at,
                eligible_at,
                trigger,
                deferral_reason,
                replay_pressure_boundary: worker.replay_pressure_boundary,
                replay_cost_bytes: worker.replay_cost_bytes,
                unpriced_namespace_prepare_references: worker.unpriced_namespace_prepare_references,
                max_log_bytes: worker.max_log_bytes,
                hard_threshold_bytes: metadata_table_logical_hard_threshold(worker.max_log_bytes),
                emergency_threshold_bytes: metadata_table_logical_emergency_threshold(
                    worker.max_log_bytes,
                ),
                process_spacing_applied,
            });
        }
        let wait = eligible_at.saturating_duration_since(now);
        let (next_workers, _) = registry
            .changed
            .wait_timeout(workers, wait)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        workers = next_workers;
    }
}

fn claim_metadata_table_checkpoint_worker_request(
    root: &Path,
    request: MetadataTableCheckpointWorkerRequest,
) -> Option<MetadataTableCheckpointWorkerRequest> {
    let registry = METADATA_TABLE_CHECKPOINT_WORKERS.get()?;
    let mut workers = registry
        .workers
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let worker = workers.get_mut(root)?;
    if worker.physical_checkpoint_required {
        if request.trigger != MetadataTableCheckpointTrigger::Physical {
            return None;
        }
        if Instant::now() < worker.physical_checkpoint_not_before {
            return None;
        }
        // Consume this request before rotation. A later append that grows the
        // new active suffix past the physical threshold sets it again and
        // keeps this worker alive for one more immediate fold.
        worker.physical_checkpoint_required = false;
        return Some(MetadataTableCheckpointWorkerRequest {
            revision: worker.revision,
            requested_at: worker.requested_at,
            eligible_at: worker.physical_checkpoint_not_before,
            trigger: MetadataTableCheckpointTrigger::Physical,
            deferral_reason: request.deferral_reason,
            replay_pressure_boundary: None,
            replay_cost_bytes: worker.replay_cost_bytes,
            unpriced_namespace_prepare_references: worker.unpriced_namespace_prepare_references,
            max_log_bytes: worker.max_log_bytes,
            hard_threshold_bytes: metadata_table_logical_hard_threshold(worker.max_log_bytes),
            emergency_threshold_bytes: metadata_table_logical_emergency_threshold(
                worker.max_log_bytes,
            ),
            process_spacing_applied: false,
        });
    }
    if request.trigger == MetadataTableCheckpointTrigger::Physical {
        return None;
    }
    let (trigger, root_eligible_at, deferral_reason) =
        metadata_table_logical_checkpoint_admission(worker);
    if trigger != request.trigger
        || (trigger == MetadataTableCheckpointTrigger::ReplayPressureSoft
            && (worker.revision != request.revision || worker.has_pending_namespace_prepares))
    {
        return None;
    }
    let (eligible_at, process_spacing_applied) = metadata_table_checkpoint_apply_process_spacing(
        trigger,
        root_eligible_at,
        metadata_table_checkpoint_process_not_before(),
    );
    if Instant::now() < eligible_at {
        return None;
    }
    Some(MetadataTableCheckpointWorkerRequest {
        revision: worker.revision,
        requested_at: worker.requested_at,
        eligible_at,
        trigger,
        deferral_reason,
        replay_pressure_boundary: worker.replay_pressure_boundary,
        replay_cost_bytes: worker.replay_cost_bytes,
        unpriced_namespace_prepare_references: worker.unpriced_namespace_prepare_references,
        max_log_bytes: worker.max_log_bytes,
        hard_threshold_bytes: metadata_table_logical_hard_threshold(worker.max_log_bytes),
        emergency_threshold_bytes: metadata_table_logical_emergency_threshold(worker.max_log_bytes),
        process_spacing_applied,
    })
}

fn metadata_table_checkpoint_worker_revision(
    registry: &MetadataTableCheckpointWorkerRegistry,
    root: &Path,
) -> Option<u64> {
    registry
        .workers
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(root)
        .map(|worker| worker.revision)
}

/// Retire the worker only when no append revised its registry state after the
/// caller sampled the active suffix.
///
/// Returning `false` asks the caller to re-sample. Returning `true` means the
/// old registration no longer owns a registry entry, whether this call removed
/// it or another actor already did.
fn retire_metadata_table_checkpoint_worker_if_unchanged(
    registry: &MetadataTableCheckpointWorkerRegistry,
    root: &Path,
    observed_revision: u64,
) -> bool {
    let mut workers = registry
        .workers
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(worker) = workers.get(root) else {
        return true;
    };
    if worker.revision != observed_revision {
        return false;
    }
    workers.remove(root);
    registry.changed.notify_all();
    true
}

fn refresh_metadata_table_checkpoint_worker_for_active_suffix(
    root: &Path,
    max_log_bytes: u64,
    completed_checkpoint_generation: bool,
) -> Result<bool> {
    let Some(registry) = METADATA_TABLE_CHECKPOINT_WORKERS.get() else {
        return Ok(false);
    };
    loop {
        // Bind the filesystem sample below to the worker revision which was
        // current before that sample. Every append updates this revision while
        // holding the same registry mutex after it writes the active suffix.
        let Some(observed_revision) = metadata_table_checkpoint_worker_revision(registry, root)
        else {
            return Ok(false);
        };
        let checkpoint_generation_present = metadata_table_checkpoint_log_path(root).is_file();
        let replay = current_metadata_table_log_replay(root)?;
        let physical_checkpoint_required = replay.offset > max_log_bytes;
        let replay_pressure_exceeded = metadata_table_logical_checkpoint_pressure(
            replay.journal_reference_replay_cost_bytes,
            replay.unpriced_namespace_prepare_references,
            max_log_bytes,
        )
        .is_some();

        if !checkpoint_generation_present
            && !physical_checkpoint_required
            && !replay_pressure_exceeded
        {
            if retire_metadata_table_checkpoint_worker_if_unchanged(
                registry,
                root,
                observed_revision,
            ) {
                return Ok(false);
            }
            continue;
        }

        let mut workers = registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(worker) = workers.get_mut(root) else {
            return Ok(false);
        };
        if worker.revision != observed_revision {
            // An append landed at the sample/retirement boundary. Re-sample
            // its exact active suffix instead of either overwriting the newer
            // request state or allowing this worker to unregister it.
            continue;
        }

        let previous_hard_checkpoint_not_before = worker.logical_hard_checkpoint_not_before;
        let previous_emergency_pressure = worker.logical_emergency_pressure;
        let requested_at = Instant::now();
        worker.revision = worker.revision.saturating_add(1);
        worker.requested_at = requested_at;
        worker.last_activity_at = requested_at;
        worker.physical_checkpoint_required =
            checkpoint_generation_present || physical_checkpoint_required;
        worker.physical_checkpoint_not_before = if worker.physical_checkpoint_required {
            metadata_table_physical_checkpoint_not_before(
                root,
                requested_at,
                replay.offset,
                max_log_bytes,
                checkpoint_generation_present,
            )
        } else {
            requested_at
        };
        worker.replay_pressure_boundary = None;
        worker.replay_cost_bytes = replay.journal_reference_replay_cost_bytes;
        worker.unpriced_namespace_prepare_references = replay.unpriced_namespace_prepare_references;
        worker.max_log_bytes = max_log_bytes;
        worker.logical_checkpoint_stagger = metadata_table_logical_checkpoint_stagger(root);
        worker.logical_hard_checkpoint_not_before = None;
        worker.logical_emergency_pressure = false;
        if !worker.physical_checkpoint_required && replay_pressure_exceeded {
            update_metadata_table_logical_checkpoint_pressure(
                root,
                worker,
                requested_at,
                replay.offset,
                replay.journal_reference_replay_cost_bytes,
                replay.unpriced_namespace_prepare_references,
                max_log_bytes,
            );
            preserve_metadata_table_logical_checkpoint_deadline(
                worker,
                completed_checkpoint_generation,
                previous_hard_checkpoint_not_before,
                previous_emergency_pressure,
            );
        }
        registry.changed.notify_all();
        return Ok(true);
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct MetadataTableCheckpointRun {
    checkpointed: bool,
    preempted: bool,
    preemption_reason: Option<&'static str>,
    allocator_trimmed: bool,
    allocator_trim_ms: u128,
    checkpoint_log_bytes: u64,
    snapshot_bytes: usize,
    rotate_ms: u128,
    decode_ms: u128,
    encode_ms: u128,
    stage_ms: u128,
    install_lock_wait_ms: u128,
    publish_ms: u128,
    install_ms: u128,
    total_ms: u128,
}

fn run_coordinated_metadata_table_checkpoint_worker(
    root: &Path,
    max_log_bytes: u64,
) -> Result<bool> {
    loop {
        let Some(request) = next_metadata_table_checkpoint_worker_request(root) else {
            return Ok(false);
        };
        // This worker has independent ownership. Never manufacture a parent
        // maintenance/GLOBAL scope from the thread which scheduled its request.
        let checkpoint_span = tracing::trace_span!(target: "sinomo.perf", parent: None,
            "dfs.metadata.checkpoint", stage = "dfs.metadata.checkpoint",
            root = %root.display(), trigger = request.trigger.as_str(),
            replay_cost_bytes = request.replay_cost_bytes,
            unpriced_references = request.unpriced_namespace_prepare_references,
            checkpoint_log_bytes = tracing::field::Empty, snapshot_bytes = tracing::field::Empty,
            checkpointed = tracing::field::Empty, preempted = tracing::field::Empty,
            outcome = "error");
        let _checkpoint_stage = checkpoint_span.enter();
        let admission_started = Instant::now();
        let fold = METADATA_TABLE_CHECKPOINT_FOLD
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let admission_wait_ms = admission_started.elapsed().as_millis();
        let coordination_started = Instant::now();
        let coordination = metadata_table_checkpoint_coordination_lock()?;
        let coordination_wait_ms = coordination_started.elapsed().as_millis();
        let (coordination, coordination_lock_configured) = match coordination {
            MetadataTableCheckpointCoordination::Disabled => (None, false),
            MetadataTableCheckpointCoordination::Acquired(coordination) => {
                (Some(coordination), true)
            }
            MetadataTableCheckpointCoordination::Contended => {
                checkpoint_span.record("outcome", "contended");
                metadata_table_checkpoint_perf_trace(format_args!(
                    "metadata-table-checkpoint-coordination-contended root={} trigger={} wait_ms={}",
                    root.display(),
                    request.trigger.as_str(),
                    coordination_wait_ms,
                ));
                drop(fold);
                std::thread::yield_now();
                continue;
            }
        };
        // Admission may have changed while this process waited. Claiming
        // re-derives the current trigger without blocking, so an ineligible
        // soft fold never sleeps while holding the process-wide and host-wide
        // coordination locks.
        let Some(request) = claim_metadata_table_checkpoint_worker_request(root, request) else {
            checkpoint_span.record("outcome", "superseded");
            drop(coordination);
            drop(fold);
            continue;
        };
        checkpoint_span.record("trigger", request.trigger.as_str());
        checkpoint_span.record("replay_cost_bytes", request.replay_cost_bytes);
        checkpoint_span.record(
            "unpriced_references",
            request.unpriced_namespace_prepare_references,
        );
        let queued_ms = request.requested_at.elapsed().as_millis();
        let eligible_queue_ms = request.eligible_at.elapsed().as_millis();
        metadata_table_checkpoint_perf_trace(format_args!(
            "metadata-table-checkpoint-admitted root={} trigger={} deferral_reason={} queued_ms={} eligible_queue_ms={} admission_wait_ms={} coordination_wait_ms={} coordination_lock_configured={} replay_cost_bytes={} max_log_bytes={}",
            root.display(),
            request.trigger.as_str(),
            request.deferral_reason,
            queued_ms,
            eligible_queue_ms,
            admission_wait_ms,
            coordination_wait_ms,
            coordination_lock_configured,
            request.replay_cost_bytes,
            request.max_log_bytes,
        ));
        #[cfg(test)]
        let _fold_observation = MetadataTableCheckpointFoldObservation::enter();
        let mut result = run_metadata_table_checkpoint_worker(root, max_log_bytes, request);
        #[cfg(test)]
        drop(_fold_observation);
        let should_trim = result.as_ref().map_or(true, |run| run.snapshot_bytes > 0);
        let allocator_trim_started = Instant::now();
        let allocator_trimmed = should_trim && release_metadata_allocator_pages();
        let allocator_trim_ms = if should_trim {
            allocator_trim_started.elapsed().as_millis()
        } else {
            0
        };
        if let Ok(run) = result.as_mut() {
            run.allocator_trimmed = allocator_trimmed;
            run.allocator_trim_ms = allocator_trim_ms;
            run.total_ms = run.total_ms.saturating_add(allocator_trim_ms);
        }
        if result.as_ref().is_ok_and(|run| run.snapshot_bytes > 0) {
            record_metadata_table_checkpoint_fold_finished();
        }
        drop(coordination);
        drop(fold);
        match result {
            Ok(run) => {
                checkpoint_span.record("checkpoint_log_bytes", run.checkpoint_log_bytes);
                checkpoint_span.record("snapshot_bytes", run.snapshot_bytes);
                checkpoint_span.record("checkpointed", run.checkpointed);
                checkpoint_span.record("preempted", run.preempted);
                checkpoint_span.record(
                    "outcome",
                    if run.preempted {
                        "preempted"
                    } else if run.checkpointed {
                        "ok"
                    } else {
                        "not_checkpointed"
                    },
                );
                metadata_table_checkpoint_perf_trace(format_args!(
                    "metadata-table-checkpoint root={} trigger={} deferral_reason={} checkpointed={} preempted={} preemption_reason={} queued_ms={} eligible_queue_ms={} admission_wait_ms={} coordination_wait_ms={} coordination_lock_configured={} process_spacing_applied={} replay_cost_bytes={} unpriced_references={} max_log_bytes={} hard_threshold_bytes={} emergency_threshold_bytes={} checkpoint_log_bytes={} snapshot_bytes={} rotate_ms={} decode_ms={} encode_ms={} stage_ms={} install_lock_wait_ms={} publish_ms={} install_ms={} allocator_trimmed={} allocator_trim_ms={} total_ms={}",
                    root.display(),
                    request.trigger.as_str(),
                    request.deferral_reason,
                    run.checkpointed,
                    run.preempted,
                    run.preemption_reason.unwrap_or("none"),
                    queued_ms,
                    eligible_queue_ms,
                    admission_wait_ms,
                    coordination_wait_ms,
                    coordination_lock_configured,
                    request.process_spacing_applied,
                    request.replay_cost_bytes,
                    request.unpriced_namespace_prepare_references,
                    request.max_log_bytes,
                    request.hard_threshold_bytes,
                    request.emergency_threshold_bytes,
                    run.checkpoint_log_bytes,
                    run.snapshot_bytes,
                    run.rotate_ms,
                    run.decode_ms,
                    run.encode_ms,
                    run.stage_ms,
                    run.install_lock_wait_ms,
                    run.publish_ms,
                    run.install_ms,
                    run.allocator_trimmed,
                    run.allocator_trim_ms,
                    run.total_ms,
                ));
                if run.preempted {
                    rearm_metadata_table_soft_checkpoint_after_preemption(root, request);
                    continue;
                }
                if !refresh_metadata_table_checkpoint_worker_for_active_suffix(
                    root,
                    max_log_bytes,
                    run.checkpointed,
                )? {
                    return Ok(run.checkpointed);
                }
                if !run.checkpointed {
                    std::thread::yield_now();
                }
            }
            Err(error) => return Err(error),
        }
    }
}

fn metadata_table_checkpoint_preemption_reason(
    root: &Path,
    request: MetadataTableCheckpointWorkerRequest,
    activity_before_lock: MetadataWriteActivity,
) -> Option<&'static str> {
    if request.trigger != MetadataTableCheckpointTrigger::ReplayPressureSoft {
        // Hard/emergency replay bounds and physical/crash recovery are safety
        // valves. They are deliberately not starved by foreground churn.
        return None;
    }
    let activity_after_lock = metadata_write_activity(root);
    if activity_after_lock.pending_writers > 0 {
        return Some("foreground-writer-waiting");
    }
    if activity_after_lock.epoch != activity_before_lock.epoch {
        return Some("foreground-writer-arrived");
    }
    let Some(registry) = METADATA_TABLE_CHECKPOINT_WORKERS.get() else {
        return Some("worker-unregistered");
    };
    let workers = registry
        .workers
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(worker) = workers.get(root) else {
        return Some("worker-unregistered");
    };
    if worker.physical_checkpoint_required {
        return Some("physical-pressure-priority");
    }
    if worker.revision != request.revision {
        return Some("foreground-revision");
    }
    if worker.has_pending_namespace_prepares {
        return Some("pending-namespace-prepare");
    }
    let (trigger, eligible_at, _) = metadata_table_logical_checkpoint_admission(worker);
    if trigger != MetadataTableCheckpointTrigger::ReplayPressureSoft {
        return Some("logical-pressure-upgraded");
    }
    if Instant::now() < eligible_at {
        return Some("idle-window-reset");
    }
    None
}

fn run_metadata_table_checkpoint_worker(
    root: &Path,
    max_log_bytes: u64,
    request: MetadataTableCheckpointWorkerRequest,
) -> Result<MetadataTableCheckpointRun> {
    let total_started = Instant::now();
    let rotate_started = Instant::now();
    let activity_before_lock = metadata_write_activity(root);
    if request.trigger == MetadataTableCheckpointTrigger::ReplayPressureSoft
        && activity_before_lock.pending_writers > 0
    {
        return Ok(MetadataTableCheckpointRun {
            preempted: true,
            preemption_reason: Some("foreground-writer-waiting"),
            rotate_ms: rotate_started.elapsed().as_millis(),
            total_ms: total_started.elapsed().as_millis(),
            ..MetadataTableCheckpointRun::default()
        });
    }
    let (snapshot_fingerprint, checkpoint_fingerprint) = {
        let _write_lock =
            metadata_checkpoint_write_lock(root.to_path_buf(), journal_write_lock_path(root))?;
        if let Some(reason) =
            metadata_table_checkpoint_preemption_reason(root, request, activity_before_lock)
        {
            return Ok(MetadataTableCheckpointRun {
                preempted: true,
                preemption_reason: Some(reason),
                rotate_ms: rotate_started.elapsed().as_millis(),
                total_ms: total_started.elapsed().as_millis(),
                ..MetadataTableCheckpointRun::default()
            });
        }
        if !rotate_metadata_table_log_for_checkpoint_at_replay_boundary(
            root,
            max_log_bytes,
            request.replay_pressure_boundary,
        )? {
            return Ok(MetadataTableCheckpointRun {
                rotate_ms: rotate_started.elapsed().as_millis(),
                total_ms: total_started.elapsed().as_millis(),
                ..MetadataTableCheckpointRun::default()
            });
        }
        (
            metadata_table_snapshot_fingerprint(root)?,
            crate::metadata_table_store::metadata_table_checkpoint_fingerprint(root)?,
        )
    };
    let rotate_ms = rotate_started.elapsed().as_millis();
    let checkpoint_log_bytes = fs::metadata(metadata_table_checkpoint_log_path(root))
        .map(|metadata| metadata.len())
        .unwrap_or(0);

    // The rotated generation is immutable. Decode, hash, and encode it without
    // holding the shard writer lock. Checkpointing must preserve the exact
    // journal-materialized tables; history compaction is a journal mutation.
    let decode_started = Instant::now();
    let state = read_metadata_table_checkpoint_state(root)?;
    let decode_ms = decode_started.elapsed().as_millis();
    let checkpoint_sequence = state.materialized_through_sequence.unwrap_or(0);
    let Some(prefix_proof) = state.prefix_proof else {
        return Err(MetadataBackendError::Serde(format!(
            "metadata table checkpoint at {} has no exact journal-prefix proof",
            root.display()
        )));
    };
    if !prefix_proof.is_well_formed() || prefix_proof.journal_sequence != checkpoint_sequence {
        return Err(MetadataBackendError::Serde(format!(
            "metadata table checkpoint at {} has mismatched sequence {} and prefix proof {}",
            root.display(),
            checkpoint_sequence,
            prefix_proof.journal_sequence
        )));
    }
    let tables = state.tables;
    let encode_started = Instant::now();
    let mut migration = crate::metadata_snapshot_migration::SnapshotMigration::new(
        root,
        &tables,
        checkpoint_sequence,
        &prefix_proof,
        snapshot_fingerprint,
        u32::from(state.snapshot_format_version.unwrap_or(1)),
    )?;
    let job = migration.run()?;
    let encode_ms = encode_started.elapsed().as_millis();
    let snapshot_len = usize::try_from(job.copied_bytes).unwrap_or(usize::MAX);
    let stage_ms = 0;
    let install_started = Instant::now();
    let install_lock_started = Instant::now();
    let _write_lock =
        metadata_checkpoint_write_lock(root.to_path_buf(), journal_write_lock_path(root))?;
    let install_lock_wait_ms = install_lock_started.elapsed().as_millis();
    let publish_started = Instant::now();
    if !migration.is_installed()?
        || crate::metadata_table_store::metadata_table_checkpoint_fingerprint(root)?
            != checkpoint_fingerprint
    {
        return Ok(MetadataTableCheckpointRun {
            checkpoint_log_bytes,
            snapshot_bytes: snapshot_len,
            rotate_ms,
            decode_ms,
            encode_ms,
            stage_ms,
            install_lock_wait_ms,
            publish_ms: publish_started.elapsed().as_millis(),
            install_ms: install_started.elapsed().as_millis(),
            total_ms: total_started.elapsed().as_millis(),
            ..MetadataTableCheckpointRun::default()
        });
    }
    drop(migration);
    drop(tables);
    remove_metadata_table_checkpoint_log(root)?;
    advance_activated_handoff_rebind_checkpoint(root, &prefix_proof)?;
    // The installed snapshot and newer table-log suffix are durable before
    // source history is pruned. Retaining the checkpoint journal segment
    // leaves a discontinuity that forces lagging replicas through snapshot
    // installation rather than reporting a false empty suffix.
    prune_segmented_journal_before_checkpoint(root, checkpoint_sequence)?;
    let publish_ms = publish_started.elapsed().as_millis();
    Ok(MetadataTableCheckpointRun {
        checkpointed: true,
        preempted: false,
        preemption_reason: None,
        allocator_trimmed: false,
        allocator_trim_ms: 0,
        checkpoint_log_bytes,
        snapshot_bytes: snapshot_len,
        rotate_ms,
        decode_ms,
        encode_ms,
        stage_ms,
        install_lock_wait_ms,
        publish_ms,
        install_ms: install_started.elapsed().as_millis(),
        total_ms: total_started.elapsed().as_millis(),
    })
}

fn membership_handoff_write_freeze_path(root: &Path) -> PathBuf {
    root.join("journal")
        .join("membership-handoff-write-freeze.sobj")
}

/// Advance an activated handoff's restart proof before checkpoint pruning can
/// remove the journal entries that connect the previous proof to this one.
///
/// The marker write is atomic and happens while the caller holds the shard
/// write lock. A crash before this write leaves the old journal history
/// available; a crash after it can safely restart validation at the new exact
/// prefix.
fn advance_activated_handoff_rebind_checkpoint(
    root: &Path,
    checkpoint: &DfsMetadataPrefixProof,
) -> Result<()> {
    let path = membership_handoff_write_freeze_path(root);
    let Some(mut freeze) = read_sobj_optional::<DfsMetadataShardWriteFreeze>(&path)? else {
        return Ok(());
    };
    if freeze.activated_epoch.is_none() {
        return Ok(());
    }
    if !checkpoint.is_well_formed() {
        return Err(MetadataBackendError::Serde(format!(
            "metadata table checkpoint at {} has a malformed exact prefix proof",
            root.display()
        )));
    }

    let base = DfsMetadataPrefixProof::from_digest(
        freeze.journal_sequence,
        freeze.state_fingerprint.clone(),
    )
    .ok_or_else(|| {
        MetadataBackendError::Serde(format!(
            "metadata shard handoff {} at {} has a malformed marker prefix",
            freeze.handoff_id,
            root.display()
        ))
    })?;
    let mut reconstructed = freeze
        .rebind_prefix_proof
        .clone()
        .unwrap_or_else(|| base.clone());
    if !base.is_well_formed()
        || !reconstructed.is_well_formed()
        || reconstructed.journal_sequence < base.journal_sequence
        || (reconstructed.journal_sequence == base.journal_sequence
            && reconstructed.digest != base.digest)
    {
        return Err(MetadataBackendError::Serde(format!(
            "metadata shard handoff {} at {} has an invalid rebind prefix checkpoint",
            freeze.handoff_id,
            root.display()
        )));
    }
    if checkpoint.journal_sequence < reconstructed.journal_sequence {
        // A background worker may finish an older captured generation after a
        // newer synchronous checkpoint has already advanced the marker.
        return Ok(());
    }
    if checkpoint.journal_sequence == reconstructed.journal_sequence {
        if checkpoint.digest != reconstructed.digest {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard handoff {} at {} has divergent prefix proofs at sequence {}",
                freeze.handoff_id,
                root.display(),
                checkpoint.journal_sequence
            )));
        }
    } else {
        while reconstructed.journal_sequence < checkpoint.journal_sequence {
            let entries =
                durable_journal_entries_since_limited(root, reconstructed.journal_sequence, 1024)?;
            if entries.is_empty() {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata shard handoff {} at {} cannot advance rebind proof from sequence {} to checkpoint {}",
                    freeze.handoff_id,
                    root.display(),
                    reconstructed.journal_sequence,
                    checkpoint.journal_sequence
                )));
            }
            let previous_sequence = reconstructed.journal_sequence;
            for entry in entries {
                if entry.sequence > checkpoint.journal_sequence {
                    break;
                }
                if entry.sequence != reconstructed.journal_sequence.saturating_add(1) {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata shard handoff {} at {} has a journal gap after sequence {}",
                        freeze.handoff_id,
                        root.display(),
                        reconstructed.journal_sequence
                    )));
                }
                let frame = read_segmented_journal_frame_at_sequence(root, entry.sequence)?
                    .ok_or_else(|| {
                        MetadataBackendError::Serde(format!(
                            "metadata shard handoff {} at {} has no exact frame for rebind entry {}",
                            freeze.handoff_id,
                            root.display(),
                            entry.sequence
                        ))
                    })?;
                if frame.entry() != &entry {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata shard handoff {} at {} has divergent exact frame for rebind entry {}",
                        freeze.handoff_id,
                        root.display(),
                        entry.sequence
                    )));
                }
                reconstructed = metadata_prefix_proof_after_exact_frame(
                    &reconstructed,
                    &entry,
                    &entry.shard_id,
                    frame.exact_bytes(),
                )?;
            }
            if reconstructed.journal_sequence <= previous_sequence {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata shard handoff {} at {} made no rebind checkpoint progress",
                    freeze.handoff_id,
                    root.display()
                )));
            }
        }
        if &reconstructed != checkpoint {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard handoff {} at {} checkpoint proof diverges at sequence {}",
                freeze.handoff_id,
                root.display(),
                checkpoint.journal_sequence
            )));
        }
    }

    if freeze.rebind_prefix_proof.as_ref() != Some(checkpoint) {
        freeze.rebind_prefix_proof = Some(checkpoint.clone());
        write_sobj_atomic(&path, &freeze)?;
    }
    Ok(())
}

pub(crate) fn namespace_prepare_replay_cost_bytes(prepare: &DfsNamespacePrepareRecord) -> u64 {
    let record_count = prepare
        .expected_object_versions
        .len()
        .saturating_add(prepare.expected_path_occupants.len())
        .saturating_add(prepare.content_receipts.len())
        .saturating_add(prepare.transaction.objects.len())
        .saturating_add(prepare.transaction.versions.len())
        .saturating_add(prepare.transaction.advisory_lock_states.len())
        .saturating_add(usize::from(prepare.completed_request.is_some()))
        .saturating_add(usize::from(prepare.completed_request_completion.is_some()));
    NAMESPACE_PREPARE_REPLAY_BASE_BYTES.saturating_add(
        u64::try_from(record_count)
            .unwrap_or(u64::MAX)
            .saturating_mul(NAMESPACE_PREPARE_REPLAY_BYTES_PER_RECORD),
    )
}

fn namespace_prepare_reference_count(ops: &[DfsMetadataTableOp]) -> u64 {
    u64::try_from(
        ops.iter()
            .filter(|op| {
                matches!(
                    op,
                    DfsMetadataTableOp::PutNamespacePrepareJournalReference { .. }
                )
            })
            .count(),
    )
    .unwrap_or(u64::MAX)
}

fn journal_reference_replay_cost_bytes(
    persisted_ops: &[DfsMetadataTableOp],
    semantic_ops: &[DfsMetadataTableOp],
) -> u64 {
    let reference_count = namespace_prepare_reference_count(persisted_ops);
    if reference_count == 0 {
        return 0;
    }
    let mut prepare_count = 0u64;
    let mut replay_cost_bytes = 0u64;
    for op in semantic_ops {
        let DfsMetadataTableOp::PutNamespacePrepare { prepare } = op else {
            continue;
        };
        prepare_count = prepare_count.saturating_add(1);
        replay_cost_bytes =
            replay_cost_bytes.saturating_add(namespace_prepare_replay_cost_bytes(prepare));
    }
    if prepare_count != reference_count {
        // Persist a zero price for an unexpected reference shape. Replay will
        // recognize it as unpriced and checkpoint the generation
        // conservatively rather than trusting an incomplete estimate.
        return 0;
    }
    replay_cost_bytes
}

fn reconcile_table_log_generation_after_background_checkpoint(
    root: &Path,
    state: &mut TableMaterializationState<'_>,
) -> Result<()> {
    let path = metadata_table_path(root);
    let active_log_len = match fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(source) if source.kind() == io::ErrorKind::NotFound => 0,
        Err(source) => {
            return Err(MetadataBackendError::Io {
                path: path.clone(),
                source,
            });
        }
    };
    let snapshot_fingerprint = metadata_table_snapshot_fingerprint(root)?;
    if active_log_len == *state.table_log_offset
        && snapshot_fingerprint == *state.table_snapshot_fingerprint
    {
        return Ok(());
    }
    let replay = current_metadata_table_log_replay(root)?;
    if replay.offset != active_log_len {
        return Err(MetadataBackendError::Serde(format!(
            "active metadata table log at {} has a torn tail at {} of {} bytes while rebasing after a background checkpoint",
            path.display(),
            replay.offset,
            active_log_len
        )));
    }
    *state.table_log_offset = replay.offset;
    *state.table_log_journal_reference_replay_cost_bytes =
        replay.journal_reference_replay_cost_bytes;
    *state.table_log_unpriced_namespace_prepare_references =
        replay.unpriced_namespace_prepare_references;
    *state.table_snapshot_fingerprint = snapshot_fingerprint;
    Ok(())
}

fn append_table_batch(
    root: &Path,
    mut state: TableMaterializationState<'_>,
    sequence: u64,
    persisted_ops: Vec<DfsMetadataTableOp>,
    apply_ops: Option<Vec<DfsMetadataTableOp>>,
    measured_journal_reference_replay_cost_bytes: Option<u64>,
    max_log_bytes: u64,
    mut rollback: Option<DfsMetadataTableRollback>,
    mut validated_namespace_prepare_footprint: Option<DfsNamespacePrepareFootprint>,
) -> Result<()> {
    let preapplied_with_trailing_resolved_op = rollback.is_some()
        && apply_ops.as_ref().is_some_and(|apply_ops| {
            matches!(apply_ops.as_slice(), [_])
                && persisted_ops.last().is_some_and(|persisted| {
                    trailing_resolved_op_matches_persisted_marker(
                        sequence,
                        persisted,
                        &apply_ops[0],
                    )
                })
        });
    if rollback.is_some() && apply_ops.is_some() && !preapplied_with_trailing_resolved_op {
        if let Some(rollback) = rollback.take() {
            rollback.rollback(state.tables);
        }
        return Err(MetadataBackendError::Serde(format!(
            "metadata table batch {sequence} cannot combine preapplied rollback with separate apply operations"
        )));
    }
    if let Err(error) = reconcile_table_log_generation_after_background_checkpoint(root, &mut state)
    {
        if let Some(rollback) = rollback.take() {
            rollback.rollback(state.tables);
        }
        return Err(error);
    }
    let validation_ops = apply_ops.as_deref().unwrap_or(&persisted_ops);
    if let Some(footprint) = validated_namespace_prepare_footprint.as_ref() {
        let mut matching_prepares = validation_ops.iter().filter_map(|op| match op {
            DfsMetadataTableOp::PutNamespacePrepare { prepare }
                if footprint.matches_prepare_identity(prepare) =>
            {
                Some(prepare)
            }
            _ => None,
        });
        if matching_prepares.next().is_none() || matching_prepares.next().is_some() {
            if let Some(rollback) = rollback.take() {
                rollback.rollback(state.tables);
            }
            return Err(MetadataBackendError::Serde(format!(
                "metadata table batch {sequence} validated prepare footprint does not name exactly one apply operation"
            )));
        }
    }
    let ordered_ops = if rollback.is_some() {
        persisted_ops.as_slice()
    } else {
        validation_ops
    };
    let reference_count = namespace_prepare_reference_count(&persisted_ops);
    if reference_count == 0 && measured_journal_reference_replay_cost_bytes.is_some() {
        if let Some(rollback) = rollback.take() {
            rollback.rollback(state.tables);
        }
        return Err(MetadataBackendError::Serde(format!(
            "metadata table batch {sequence} cannot price journal-reference replay without a journal reference"
        )));
    }
    let structural_replay_cost_bytes =
        journal_reference_replay_cost_bytes(&persisted_ops, validation_ops);
    let journal_reference_replay_cost_bytes = measured_journal_reference_replay_cost_bytes
        .map_or(structural_replay_cost_bytes, |measured| {
            measured.max(structural_replay_cost_bytes)
        });
    let unpriced_namespace_prepare_references = if journal_reference_replay_cost_bytes == 0 {
        reference_count
    } else {
        0
    };
    if let Err(error) =
        validate_version_pin_removal_order(state.tables, ordered_ops, rollback.is_some())
    {
        if let Some(rollback) = rollback.take() {
            rollback.rollback(state.tables);
        }
        return Err(error);
    }
    let semantic_op_count = validation_ops
        .iter()
        .filter(|op| matches!(op, DfsMetadataTableOp::CommitNamespacePrepare { .. }))
        .count();
    // Foreground callers must isolate a semantic marker so dependency
    // validation observes the exact preceding durable table state. Replica
    // replay can legitimately preapply several journal entries in order and
    // then append one table batch containing Prepared plus Commit (or multiple
    // independent commits); its rollback proves that ordered validation has
    // already succeeded.
    if rollback.is_none()
        && semantic_op_count > 0
        && (semantic_op_count != 1 || validation_ops.len() != 1)
    {
        if let Some(rollback) = rollback.take() {
            rollback.rollback(state.tables);
        }
        return Err(MetadataBackendError::Serde(format!(
            "metadata table batch {sequence} must isolate its compact namespace commit marker"
        )));
    }
    if rollback.is_none() || preapplied_with_trailing_resolved_op {
        for op in validation_ops {
            if let Err(error) = state.tables.validate_checked(op) {
                if let Some(rollback) = rollback.take() {
                    rollback.rollback(state.tables);
                }
                return Err(error);
            }
        }
    }
    let Some(prefix_proof) = state.prefix_proof.as_ref() else {
        if let Some(rollback) = rollback {
            rollback.rollback(state.tables);
        }
        return Err(MetadataBackendError::Serde(format!(
            "metadata table batch {sequence} cannot commit without an exact journal-prefix proof"
        )));
    };
    if !prefix_proof.is_well_formed() || prefix_proof.journal_sequence != sequence {
        if let Some(rollback) = rollback {
            rollback.rollback(state.tables);
        }
        return Err(MetadataBackendError::Serde(format!(
            "metadata table batch {sequence} prefix proof names sequence {}",
            prefix_proof.journal_sequence
        )));
    }
    let apply_after_append = rollback.is_none() || preapplied_with_trailing_resolved_op;
    let batch = DfsMetadataTableBatch {
        sequence,
        ops: persisted_ops,
        prefix_proof: Some(prefix_proof.clone()),
        journal_reference_replay_cost_bytes,
    };
    let written = match append_metadata_table_batch(root, &batch) {
        Ok(written) => written,
        Err(error) => {
            if let Some(rollback) = rollback {
                rollback.rollback(state.tables);
            }
            return Err(error);
        }
    };
    *state.table_log_offset = state.table_log_offset.saturating_add(written);
    *state.table_log_journal_reference_replay_cost_bytes = state
        .table_log_journal_reference_replay_cost_bytes
        .saturating_add(batch.journal_reference_replay_cost_bytes);
    *state.table_log_unpriced_namespace_prepare_references = state
        .table_log_unpriced_namespace_prepare_references
        .saturating_add(unpriced_namespace_prepare_references);
    if apply_after_append {
        // The complete table batch now owns crash recovery for the trailing
        // semantic transition. Never retain or run the preappend rollback
        // after this boundary.
        drop(rollback.take());
        for op in apply_ops.unwrap_or(batch.ops) {
            if matches!(&op, DfsMetadataTableOp::PutNamespacePrepare { .. })
                && let Some(footprint) = validated_namespace_prepare_footprint.take()
            {
                state
                    .tables
                    .apply_checked_with_namespace_prepare_footprint(op, footprint)?;
            } else {
                state.tables.apply_checked(op)?;
            }
        }
        debug_assert!(validated_namespace_prepare_footprint.is_none());
    }
    *state.materialized_through_sequence = (*state.materialized_through_sequence).max(sequence);
    schedule_metadata_table_checkpoint_if_needed(
        root,
        state.tables,
        max_log_bytes,
        *state.table_log_offset,
        *state.table_log_journal_reference_replay_cost_bytes,
        *state.table_log_unpriced_namespace_prepare_references,
    )
}

fn validate_version_pin_removal_order(
    tables: &DfsMetadataTables,
    ops: &[DfsMetadataTableOp],
    preapplied: bool,
) -> Result<()> {
    let mut pins = tables.version_pins.keys().cloned().collect::<BTreeSet<_>>();
    if !preapplied {
        for op in ops {
            match op {
                DfsMetadataTableOp::PutVersionPin { pin } => {
                    pins.insert((
                        pin.object_id.clone(),
                        pin.version.clone(),
                        pin.pin_id.clone(),
                    ));
                }
                DfsMetadataTableOp::RemoveVersionPin {
                    object_id,
                    version,
                    pin_id,
                } => {
                    pins.remove(&(object_id.clone(), version.clone(), pin_id.clone()));
                }
                DfsMetadataTableOp::ReconcileCrossShardPinTransfer { transfer, .. } => {
                    if let Some(previous) = tables
                        .cross_shard_pin_transfers
                        .get(&(transfer.object_id.clone(), transfer.transfer_id.clone()))
                    {
                        for pin in &previous.copied_pins {
                            if !transfer.copied_pins.iter().any(|current| current == pin) {
                                pins.remove(&(
                                    pin.object_id.clone(),
                                    pin.version.clone(),
                                    pin.pin_id.clone(),
                                ));
                            }
                        }
                    }
                    for pin in &transfer.source_pins {
                        pins.insert((
                            pin.object_id.clone(),
                            pin.version.clone(),
                            pin.pin_id.clone(),
                        ));
                    }
                }
                DfsMetadataTableOp::FinalizeCrossShardSourceEviction { removed_pins, .. } => {
                    for pin in removed_pins {
                        pins.remove(&(
                            pin.object_id.clone(),
                            pin.version.clone(),
                            pin.pin_id.clone(),
                        ));
                    }
                }
                DfsMetadataTableOp::RemoveVersion { object_id, version }
                    if pins.iter().any(|(pin_object_id, pin_version, _)| {
                        pin_object_id == object_id && pin_version == version
                    }) =>
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata table batch cannot remove pinned version {object_id}/{version}"
                    )));
                }
                _ => {}
            }
        }
        return Ok(());
    }
    for op in ops {
        if let DfsMetadataTableOp::RemoveVersion { object_id, version } = op
            && pins.iter().any(|(pin_object_id, pin_version, _)| {
                pin_object_id == object_id && pin_version == version
            })
        {
            return Err(MetadataBackendError::Serde(format!(
                "preapplied metadata table batch cannot remove pinned version {object_id}/{version}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DfsFileSystemId;
    use crate::metadata_io::write_file_atomic;
    use crate::metadata_journal::write_local_journal_entry;
    use crate::metadata_read_snapshot::current_tables_snapshot;
    use crate::metadata_records::{
        DfsMetadataMutation, DfsMetadataShardId, DfsNamespacePrepareRecord,
        DfsNamespacePrepareStatus, DfsNamespaceTransaction, dfs_metadata_prefix_proof_seed,
    };
    use crate::metadata_table_store::{
        metadata_table_journal_reference_resolutions, metadata_table_snapshot_path,
        read_metadata_table_state, reset_metadata_table_journal_reference_resolutions,
    };
    use std::sync::{Arc, Mutex};
    use time::OffsetDateTime;

    fn namespace_transaction_op(transaction_id: &str) -> DfsMetadataTableOp {
        DfsMetadataTableOp::PutNamespaceTransaction {
            transaction: DfsNamespaceTransaction {
                file_version_pins: None,
                transaction_id: transaction_id.to_string(),
                file_system_id: DfsFileSystemId::from_legacy_named("fs_table_rollback")
                    .expect("filesystem"),
                objects: Vec::new(),
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
        }
    }

    fn namespace_prepare(
        operation_id: &str,
        status: DfsNamespacePrepareStatus,
    ) -> DfsNamespacePrepareRecord {
        let file_system_id =
            DfsFileSystemId::from_legacy_named("fs_table_semantic_prepare").expect("filesystem");
        DfsNamespacePrepareRecord {
            operation_id: operation_id.to_string(),
            file_system_id: file_system_id.clone(),
            shard_id: DfsMetadataShardId::new("shard_table_semantic_prepare"),
            owner_epoch: 7,
            operation_digest: format!("digest_{operation_id}"),
            expected_object_versions: Vec::new(),
            expected_path_occupants: Vec::new(),
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                file_version_pins: None,
                transaction_id: format!("txn_{operation_id}"),
                file_system_id,
                objects: Vec::new(),
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
            completed_request: None,
            completed_request_completion: None,
            status,
        }
    }

    fn namespace_prepare_commit_op(prepare: &DfsNamespacePrepareRecord) -> DfsMetadataTableOp {
        DfsMetadataTableOp::CommitNamespacePrepare {
            operation_id: prepare.operation_id.clone(),
            operation_digest: prepare.operation_digest.clone(),
            owner_epoch: prepare.owner_epoch,
            retained_at: Some(prepare.transaction.created_at),
        }
    }

    fn wait_for_checkpoint_worker(root: &Path) {
        wait_for_metadata_table_checkpoint_workers_for_test(root);
    }

    fn logical_checkpoint_worker_state(
        root: &Path,
        requested_at: Instant,
        replay_cost_bytes: u64,
        unpriced_namespace_prepare_references: u64,
        max_log_bytes: u64,
    ) -> MetadataTableCheckpointWorkerState {
        let mut worker = MetadataTableCheckpointWorkerState {
            revision: 1,
            requested_at,
            last_activity_at: requested_at,
            has_pending_namespace_prepares: false,
            physical_checkpoint_required: false,
            physical_checkpoint_not_before: requested_at,
            replay_pressure_boundary: None,
            replay_cost_bytes,
            unpriced_namespace_prepare_references,
            max_log_bytes,
            logical_checkpoint_stagger: metadata_table_logical_checkpoint_stagger(root),
            logical_hard_checkpoint_not_before: None,
            logical_emergency_pressure: false,
        };
        update_metadata_table_logical_checkpoint_pressure(
            root,
            &mut worker,
            requested_at,
            17,
            replay_cost_bytes,
            unpriced_namespace_prepare_references,
            max_log_bytes,
        );
        worker
    }

    #[test]
    fn no_pressure_retirement_preserves_a_concurrent_append_revision() {
        let root =
            sinomo_testdir::TestDir::new("sinomo-dfs-table-checkpoint-no-pressure-retirement-race");
        let registry = METADATA_TABLE_CHECKPOINT_WORKERS
            .get_or_init(MetadataTableCheckpointWorkerRegistry::default);
        let requested_at = Instant::now();
        registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                root.path().to_path_buf(),
                logical_checkpoint_worker_state(root.path(), requested_at, 101, 0, 100),
            );

        // Model the exact shutdown boundary: the worker sampled a clean active
        // suffix at revision 1, then a foreground append revised the existing
        // registry entry before the old worker could retire it.
        let sampled_revision = registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(root.path())
            .expect("registered checkpoint worker")
            .revision;
        let appended_revision = {
            let mut workers = registry
                .workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let worker = workers
                .get_mut(root.path())
                .expect("worker remains registered for concurrent append");
            worker.revision = worker.revision.saturating_add(1);
            worker.last_activity_at = Instant::now();
            worker.replay_cost_bytes = 301;
            worker.logical_emergency_pressure = true;
            worker.revision
        };

        assert!(
            !retire_metadata_table_checkpoint_worker_if_unchanged(
                registry,
                root.path(),
                sampled_revision,
            ),
            "a stale no-pressure sample must not unregister newer append work"
        );
        let worker = registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(root.path())
            .copied()
            .expect("newer append request remains registered");
        assert_eq!(worker.revision, appended_revision);
        assert_eq!(worker.replay_cost_bytes, 301);
        assert!(worker.logical_emergency_pressure);

        assert!(
            retire_metadata_table_checkpoint_worker_if_unchanged(
                registry,
                root.path(),
                appended_revision,
            ),
            "the worker may retire only after sampling the current revision"
        );
        assert!(
            !registry
                .workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .contains_key(root.path())
        );
    }

    #[test]
    fn failed_worker_attempt_keeps_a_concurrent_append_eligible_for_retry() {
        let root = sinomo_testdir::TestDir::new(
            "sinomo-dfs-table-checkpoint-failed-attempt-retirement-race",
        );
        let registry = METADATA_TABLE_CHECKPOINT_WORKERS
            .get_or_init(MetadataTableCheckpointWorkerRegistry::default);
        registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                root.path().to_path_buf(),
                logical_checkpoint_worker_state(root.path(), Instant::now(), 301, 0, 100),
            );

        let failed_attempt_revision =
            metadata_table_checkpoint_worker_revision(registry, root.path())
                .expect("registered failed attempt");
        let appended_revision = {
            let mut workers = registry
                .workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let worker = workers
                .get_mut(root.path())
                .expect("failed worker remains registered during append");
            worker.revision = worker.revision.saturating_add(1);
            worker.last_activity_at = Instant::now();
            worker.revision
        };

        assert!(
            !retire_metadata_table_checkpoint_worker_if_unchanged(
                registry,
                root.path(),
                failed_attempt_revision,
            ),
            "the failed attempt must retain ownership when an append revised its request"
        );
        let retry = next_metadata_table_checkpoint_worker_request(root.path())
            .expect("the existing worker can retry the concurrent append");
        assert_eq!(retry.revision, appended_revision);
        assert_eq!(
            retry.trigger,
            MetadataTableCheckpointTrigger::ReplayPressureEmergency
        );

        assert!(retire_metadata_table_checkpoint_worker_if_unchanged(
            registry,
            root.path(),
            appended_revision,
        ));
    }

    #[test]
    fn failed_worker_spawn_retries_after_a_concurrent_append_revision() {
        let root = sinomo_testdir::TestDir::new(
            "sinomo-dfs-table-checkpoint-failed-spawn-retirement-race",
        );
        let registry = METADATA_TABLE_CHECKPOINT_WORKERS
            .get_or_init(MetadataTableCheckpointWorkerRegistry::default);
        registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                root.path().to_path_buf(),
                logical_checkpoint_worker_state(root.path(), Instant::now(), 301, 0, 100),
            );

        let mut spawn_attempts = 0_usize;
        let mut appended_revision = None;
        start_metadata_table_checkpoint_worker_with(
            root.path(),
            100,
            |worker_root, _max_log_bytes| {
                spawn_attempts = spawn_attempts.saturating_add(1);
                if spawn_attempts == 1 {
                    let mut workers = registry
                        .workers
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let worker = workers
                        .get_mut(&worker_root)
                        .expect("worker remains registered during failed spawn");
                    worker.revision = worker.revision.saturating_add(1);
                    worker.last_activity_at = Instant::now();
                    appended_revision = Some(worker.revision);
                    return Err(io::Error::other("injected checkpoint worker spawn failure"));
                }
                Ok(())
            },
        );

        assert_eq!(spawn_attempts, 2);
        let appended_revision = appended_revision.expect("injected append revision");
        assert_eq!(
            metadata_table_checkpoint_worker_revision(registry, root.path()),
            Some(appended_revision),
            "the successful retry retains the exact append-revised worker state"
        );
        assert!(retire_metadata_table_checkpoint_worker_if_unchanged(
            registry,
            root.path(),
            appended_revision,
        ));
    }

    #[test]
    fn soft_logical_checkpoint_requires_idle_period_and_stable_node_stagger() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-logical-soft-admission");
        let requested_at = Instant::now();
        let worker = logical_checkpoint_worker_state(root.path(), requested_at, 101, 0, 100);
        let (trigger, eligible_at, reason) = metadata_table_logical_checkpoint_admission(&worker);
        assert_eq!(trigger, MetadataTableCheckpointTrigger::ReplayPressureSoft);
        assert_eq!(reason, "idle-plus-node-stagger");
        assert_eq!(
            eligible_at,
            requested_at
                .checked_add(METADATA_TABLE_LOGICAL_CHECKPOINT_QUIET_PERIOD)
                .and_then(|quiet| quiet.checked_add(worker.logical_checkpoint_stagger))
                .expect("bounded logical checkpoint admission")
        );
        assert_eq!(
            worker.logical_checkpoint_stagger,
            metadata_table_logical_checkpoint_stagger(root.path()),
            "a root must retain one deterministic stagger across foreground revisions"
        );
        assert!(worker.logical_checkpoint_stagger < METADATA_TABLE_LOGICAL_CHECKPOINT_STAGGER_MAX);
    }

    #[test]
    fn hard_logical_checkpoint_bypasses_pending_prepare_and_revision_churn() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-logical-hard-admission");
        let registry = METADATA_TABLE_CHECKPOINT_WORKERS
            .get_or_init(MetadataTableCheckpointWorkerRegistry::default);
        let requested_at = Instant::now()
            .checked_sub(METADATA_TABLE_LOGICAL_CHECKPOINT_HARD_STAGGER_MAX)
            .expect("recent hard-pressure crossing");
        let mut worker = logical_checkpoint_worker_state(root.path(), requested_at, 201, 0, 100);
        worker.has_pending_namespace_prepares = true;
        worker.logical_hard_checkpoint_not_before = Some(requested_at);
        registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(root.path().to_path_buf(), worker);

        let request =
            next_metadata_table_checkpoint_worker_request(root.path()).expect("hard request");
        assert_eq!(
            request.trigger,
            MetadataTableCheckpointTrigger::ReplayPressureHard
        );
        let activity_before_foreground = metadata_write_activity(root.path());
        let foreground_lock = crate::metadata_lock::metadata_write_lock(
            root.path().to_path_buf(),
            journal_write_lock_path(root.path()),
        )
        .expect("foreground writer activity");
        drop(foreground_lock);
        {
            let mut workers = registry
                .workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let worker = workers.get_mut(root.path()).expect("hard worker");
            worker.revision = worker.revision.saturating_add(1);
            worker.last_activity_at = Instant::now();
        }
        assert!(
            claim_metadata_table_checkpoint_worker_request(root.path(), request).is_some(),
            "the bounded restart-cost valve must not be starved by a stuck prepare or new revisions"
        );
        assert_eq!(
            metadata_table_checkpoint_preemption_reason(
                root.path(),
                request,
                activity_before_foreground,
            ),
            None,
            "hard admission must survive foreground waiter activity before rotation"
        );
        registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(root.path());
    }

    #[test]
    fn emergency_and_unpriced_pressure_bypass_stagger_and_process_spacing() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-logical-emergency");
        let requested_at = Instant::now();
        for worker in [
            logical_checkpoint_worker_state(root.path(), requested_at, 301, 0, 100),
            logical_checkpoint_worker_state(root.path(), requested_at, 0, 1, 100),
        ] {
            let (trigger, eligible_at, _) = metadata_table_logical_checkpoint_admission(&worker);
            assert_eq!(
                trigger,
                MetadataTableCheckpointTrigger::ReplayPressureEmergency
            );
            assert_eq!(eligible_at, requested_at);
            let future_process_gate = requested_at
                .checked_add(Duration::from_secs(60))
                .expect("future process gate");
            assert_eq!(
                metadata_table_checkpoint_apply_process_spacing(
                    trigger,
                    eligible_at,
                    Some(future_process_gate),
                ),
                (eligible_at, false),
                "emergency pressure must bypass normal fold spacing"
            );
        }
    }

    #[test]
    fn unsuccessful_refresh_preserves_hard_deadline_and_physical_bypasses_spacing() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-logical-hard-refresh");
        let requested_at = Instant::now();
        let mut worker = logical_checkpoint_worker_state(root.path(), requested_at, 201, 0, 100);
        let original_hard_not_before = worker
            .logical_hard_checkpoint_not_before
            .expect("hard deadline");
        worker.logical_hard_checkpoint_not_before = Some(
            original_hard_not_before
                .checked_add(Duration::from_secs(60))
                .expect("incorrectly reset deadline"),
        );
        preserve_metadata_table_logical_checkpoint_deadline(
            &mut worker,
            false,
            Some(original_hard_not_before),
            false,
        );
        assert_eq!(
            worker.logical_hard_checkpoint_not_before,
            Some(original_hard_not_before),
            "an unsuccessful fold must not move the restart-cost deadline"
        );

        let physical_eligible = requested_at;
        let future_process_gate = requested_at
            .checked_add(Duration::from_secs(60))
            .expect("future process gate");
        assert_eq!(
            metadata_table_checkpoint_apply_process_spacing(
                MetadataTableCheckpointTrigger::Physical,
                physical_eligible,
                Some(future_process_gate),
            ),
            (physical_eligible, false),
            "physical and crash-resumed work must never wait behind normal logical spacing"
        );
    }

    #[test]
    fn soft_checkpoint_preempts_before_rotation_when_foreground_revision_arrives() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-logical-soft-preemption");
        let shard_id = DfsMetadataShardId::new("shard_table_logical_soft_preemption");
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;
        let mut replay_cost = 0;
        let mut unpriced_references = 0;
        let mut materialized_through_sequence = 0;
        let mut snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &shard_id,
            1,
            "soft-preemption",
        ));
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes: &mut replay_cost,
                table_log_unpriced_namespace_prepare_references: &mut unpriced_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![namespace_transaction_op("txn_soft_preemption")],
            u64::MAX,
        )
        .expect("append soft-preemption state");
        let active_len_before = fs::metadata(metadata_table_path(root.path()))
            .expect("active table log")
            .len();

        let registry = METADATA_TABLE_CHECKPOINT_WORKERS
            .get_or_init(MetadataTableCheckpointWorkerRegistry::default);
        let requested_at = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("past soft request");
        let mut worker = logical_checkpoint_worker_state(root.path(), requested_at, 101, 0, 100);
        worker.logical_checkpoint_stagger = Duration::ZERO;
        worker.replay_pressure_boundary = Some(table_log_offset);
        registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(root.path().to_path_buf(), worker);
        let request =
            next_metadata_table_checkpoint_worker_request(root.path()).expect("soft request");
        let request = claim_metadata_table_checkpoint_worker_request(root.path(), request)
            .expect("claim soft request");

        let foreground_lock = crate::metadata_lock::metadata_write_lock(
            root.path().to_path_buf(),
            journal_write_lock_path(root.path()),
        )
        .expect("hold shard writer lock");
        let worker_root = root.path().to_path_buf();
        let handle = std::thread::spawn(move || {
            run_metadata_table_checkpoint_worker(&worker_root, 100, request)
                .expect("soft checkpoint attempt")
        });
        std::thread::sleep(Duration::from_millis(10));
        let revision_activity_at = Instant::now();
        {
            let mut workers = registry
                .workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let worker = workers.get_mut(root.path()).expect("soft worker");
            worker.revision = worker.revision.saturating_add(1);
            worker.last_activity_at = revision_activity_at;
        }
        drop(foreground_lock);
        let run = handle.join().expect("join soft checkpoint attempt");
        assert!(run.preempted);
        assert_eq!(run.preemption_reason, Some("foreground-revision"));
        assert!(!metadata_table_checkpoint_log_path(root.path()).exists());
        assert_eq!(
            fs::metadata(metadata_table_path(root.path()))
                .expect("unchanged active table log")
                .len(),
            active_len_before,
            "soft admission must cancel before rotating the active generation"
        );
        rearm_metadata_table_soft_checkpoint_after_preemption(root.path(), request);
        let worker = registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(root.path())
            .expect("remove soft worker");
        assert!(worker.last_activity_at >= revision_activity_at);
    }

    #[test]
    fn physical_checkpoint_stagger_is_deterministic_bounded_and_distributed() {
        let same = metadata_table_checkpoint_stagger_for_identity(
            b"node-a/shard-root",
            METADATA_TABLE_PHYSICAL_CHECKPOINT_STAGGER_MAX,
        );
        assert_eq!(
            same,
            metadata_table_checkpoint_stagger_for_identity(
                b"node-a/shard-root",
                METADATA_TABLE_PHYSICAL_CHECKPOINT_STAGGER_MAX,
            )
        );
        assert!(same < METADATA_TABLE_PHYSICAL_CHECKPOINT_STAGGER_MAX);

        let distinct = (0..128)
            .map(|index| {
                metadata_table_checkpoint_stagger_for_identity(
                    format!("node-{index}/shard-root").as_bytes(),
                    METADATA_TABLE_PHYSICAL_CHECKPOINT_STAGGER_MAX,
                )
            })
            .collect::<BTreeSet<_>>();
        assert!(
            distinct.len() >= 8,
            "independent local identities should spread threshold folds across many buckets"
        );
    }

    #[test]
    fn resumed_and_hard_pressure_physical_checkpoints_bypass_stagger() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-checkpoint-admission");
        let requested_at = Instant::now();
        let ordinary = metadata_table_physical_checkpoint_not_before(
            root.path(),
            requested_at,
            101,
            100,
            false,
        );
        assert!(ordinary >= requested_at);
        assert!(
            ordinary
                <= requested_at
                    .checked_add(METADATA_TABLE_PHYSICAL_CHECKPOINT_STAGGER_MAX)
                    .expect("bounded checkpoint stagger")
        );
        assert_eq!(
            metadata_table_physical_checkpoint_not_before(
                root.path(),
                requested_at,
                101,
                100,
                true,
            ),
            requested_at,
            "a crash-resumed rotated checkpoint must be admitted immediately"
        );
        assert_eq!(
            metadata_table_physical_checkpoint_not_before(
                root.path(),
                requested_at,
                200,
                100,
                false,
            ),
            requested_at,
            "two-times physical replay pressure must bypass jitter"
        );
    }

    #[test]
    fn due_physical_checkpoint_claim_survives_foreground_revision_updates() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-checkpoint-revision");
        let registry = METADATA_TABLE_CHECKPOINT_WORKERS
            .get_or_init(MetadataTableCheckpointWorkerRegistry::default);
        let requested_at = Instant::now();
        registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                root.path().to_path_buf(),
                MetadataTableCheckpointWorkerState {
                    revision: 1,
                    requested_at,
                    last_activity_at: requested_at,
                    has_pending_namespace_prepares: false,
                    physical_checkpoint_required: true,
                    physical_checkpoint_not_before: requested_at,
                    replay_pressure_boundary: None,
                    replay_cost_bytes: 0,
                    unpriced_namespace_prepare_references: 0,
                    max_log_bytes: 100,
                    logical_checkpoint_stagger: Duration::ZERO,
                    logical_hard_checkpoint_not_before: None,
                    logical_emergency_pressure: false,
                },
            );
        let request =
            next_metadata_table_checkpoint_worker_request(root.path()).expect("physical request");
        {
            let mut workers = registry
                .workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let worker = workers.get_mut(root.path()).expect("physical worker");
            worker.revision = worker.revision.saturating_add(1);
            worker.last_activity_at = Instant::now();
        }

        assert!(
            claim_metadata_table_checkpoint_worker_request(root.path(), request).is_some(),
            "monotonic physical pressure must not starve behind newer foreground revisions"
        );
        let worker = registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(root.path())
            .expect("remove claimed worker");
        assert!(!worker.physical_checkpoint_required);
    }

    #[test]
    fn crash_resumed_checkpoint_generation_is_admitted_immediately_and_reopens() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-checkpoint-resume");
        let shard_id = DfsMetadataShardId::new("shard_table_checkpoint_resume");
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;
        let mut table_log_journal_reference_replay_cost_bytes = 0;
        let mut table_log_unpriced_namespace_prepare_references = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &shard_id,
            1,
            "checkpoint-resume",
        ));
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![namespace_transaction_op("txn_checkpoint_resume")],
            u64::MAX,
        )
        .expect("append checkpoint resume state");
        assert!(
            rotate_metadata_table_log_for_checkpoint(root.path(), 0)
                .expect("publish crash-resumed checkpoint generation")
        );

        let fold = METADATA_TABLE_CHECKPOINT_FOLD
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        schedule_metadata_table_checkpoint_if_needed(root.path(), &tables, u64::MAX, 0, 0, 0)
            .expect("schedule crash-resumed checkpoint");
        let registry = METADATA_TABLE_CHECKPOINT_WORKERS
            .get()
            .expect("checkpoint registry");
        let workers = registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let worker = workers.get(root.path()).expect("resumed worker");
        assert!(worker.physical_checkpoint_required);
        assert_eq!(
            worker.physical_checkpoint_not_before, worker.requested_at,
            "an already-rotated generation must not receive threshold jitter"
        );
        drop(workers);
        drop(fold);

        wait_for_checkpoint_worker(root.path());
        assert!(
            !metadata_table_checkpoint_log_path(root.path()).exists(),
            "the resumed immutable generation should be folded and retired"
        );
        let reopened = read_metadata_table_state(root.path()).expect("reopen resumed checkpoint");
        assert!(
            reopened
                .tables
                .namespace_transactions
                .contains_key("txn_checkpoint_resume")
        );
    }

    #[test]
    fn compact_namespace_commit_reopens_from_materialized_table_log() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-semantic-reopen");
        let shard_id = DfsMetadataShardId::new("shard_table_semantic_prepare");
        let prepare = namespace_prepare(
            "operation_semantic_reopen",
            DfsNamespacePrepareStatus::Prepared,
        );
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;
        let mut table_log_journal_reference_replay_cost_bytes = 0;
        let mut table_log_unpriced_namespace_prepare_references = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &shard_id,
            1,
            "semantic-reopen-prepare",
        ));
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![DfsMetadataTableOp::put_namespace_prepare(prepare.clone())],
            u64::MAX,
        )
        .expect("append prepared body");

        prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &shard_id,
            2,
            "semantic-reopen-commit",
        ));
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            2,
            vec![namespace_prepare_commit_op(&prepare)],
            u64::MAX,
        )
        .expect("append compact commit marker");

        assert!(
            !tables
                .namespace_prepares
                .contains_key(&prepare.operation_id),
            "a committed prepare must not retain its full body"
        );
        assert!(
            !tables
                .namespace_transactions
                .contains_key(&prepare.transaction.transaction_id),
            "a committed transaction must not retain its full body"
        );
        assert_eq!(
            tables
                .namespace_prepare_receipts
                .get(&prepare.operation_id)
                .map(|receipt| &receipt.status),
            Some(&DfsNamespacePrepareStatus::Committed)
        );
        assert!(
            tables
                .namespace_transaction_receipts
                .get(&prepare.transaction.transaction_id)
                .expect("compact transaction receipt")
                .matches_transaction(&prepare.transaction)
                .expect("transaction identity")
        );

        let reopened =
            read_metadata_table_state(root.path()).expect("replay materialized table log");
        assert_eq!(reopened.tables, tables);
        assert_eq!(reopened.materialized_through_sequence, Some(2));
    }

    #[test]
    fn compact_prepare_replay_pressure_defers_until_terminal_commit() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-reference-replay-pressure");
        let shard_id = DfsMetadataShardId::new("shard_table_semantic_prepare");
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;
        let mut table_log_journal_reference_replay_cost_bytes = 0;
        let mut table_log_unpriced_namespace_prepare_references = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &shard_id,
            0,
            "empty-reference-generation",
        ));
        let mut next_sequence = 1;
        // Leave room for both compact table markers while keeping the full
        // authoritative prepare above the logical replay-pressure threshold.
        let max_replay_bytes = NAMESPACE_PREPARE_REPLAY_BASE_BYTES.saturating_add(512);
        let with_replay_pressure = |mut prepare: DfsNamespacePrepareRecord| {
            prepare.expected_path_occupants = (0..3)
                .map(|index| crate::DfsNamespacePathOccupantExpectation {
                    path: format!("/replay-pressure/{}/{index}", prepare.operation_id),
                    expected_object_id: None,
                })
                .collect();
            prepare
        };
        let prepare = with_replay_pressure(namespace_prepare(
            "operation_replay_pressure",
            DfsNamespacePrepareStatus::Prepared,
        ));
        let entry = write_local_journal_entry(
            root.path(),
            &shard_id,
            &mut next_sequence,
            DfsMetadataMutation::put_namespace_prepare(&prepare),
        )
        .expect("append authoritative namespace prepare");
        prefix_proof = Some(
            dfs_metadata_prefix_proof_after(
                prefix_proof.as_ref().expect("previous prefix proof"),
                &entry,
            )
            .expect("extend exact prefix proof"),
        );
        let reference = DfsMetadataTableOp::PutNamespacePrepareJournalReference {
            shard_id: shard_id.clone(),
            journal_sequence: entry.sequence,
            file_system_id: prepare.file_system_id.clone(),
            operation_id: prepare.operation_id.clone(),
            operation_digest: prepare.operation_digest.clone(),
            owner_epoch: prepare.owner_epoch,
            status: prepare.status.clone(),
        };
        append_and_apply_table_ops_with_persisted_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            entry.sequence,
            vec![reference],
            vec![DfsMetadataTableOp::put_namespace_prepare(prepare.clone())],
            None,
            max_replay_bytes,
        )
        .expect("append compact namespace prepare reference");

        assert!(
            table_log_offset <= max_replay_bytes,
            "the compact table log itself should remain below the byte trigger: offset={table_log_offset} threshold={max_replay_bytes}"
        );
        assert!(
            table_log_journal_reference_replay_cost_bytes > max_replay_bytes,
            "the compact reference must cross the reconstructed replay-cost trigger: logical={} threshold={max_replay_bytes}",
            table_log_journal_reference_replay_cost_bytes,
        );
        assert_eq!(table_log_unpriced_namespace_prepare_references, 0);

        wait_for_checkpoint_worker(root.path());
        assert!(
            !metadata_table_snapshot_path(root.path()).is_file(),
            "logical pressure must not checkpoint a still-pending prepare"
        );

        let commit_entry = write_local_journal_entry(
            root.path(),
            &shard_id,
            &mut next_sequence,
            DfsMetadataMutation::commit_namespace_prepare(&prepare),
        )
        .expect("append authoritative namespace commit");
        prefix_proof = Some(
            dfs_metadata_prefix_proof_after(
                prefix_proof.as_ref().expect("prepare prefix proof"),
                &commit_entry,
            )
            .expect("extend commit prefix proof"),
        );
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            commit_entry.sequence,
            vec![namespace_prepare_commit_op(&prepare)],
            max_replay_bytes,
        )
        .expect("append compact namespace commit");
        assert!(
            table_log_offset <= max_replay_bytes,
            "the commit must exercise logical rather than physical pressure: offset={table_log_offset} threshold={max_replay_bytes}"
        );

        wait_for_checkpoint_worker(root.path());
        assert!(
            metadata_table_snapshot_path(root.path()).is_file(),
            "the terminal commit must schedule a self-contained snapshot"
        );
        let first_snapshot =
            fs::read(metadata_table_snapshot_path(root.path())).expect("first snapshot bytes");

        let second_prepare = with_replay_pressure(namespace_prepare(
            "operation_replay_pressure_second_generation",
            DfsNamespacePrepareStatus::Prepared,
        ));
        let second_prepare_entry = write_local_journal_entry(
            root.path(),
            &shard_id,
            &mut next_sequence,
            DfsMetadataMutation::put_namespace_prepare(&second_prepare),
        )
        .expect("append second-generation authoritative prepare");
        prefix_proof = Some(
            dfs_metadata_prefix_proof_after(
                prefix_proof.as_ref().expect("first commit prefix proof"),
                &second_prepare_entry,
            )
            .expect("extend second-generation prepare prefix proof"),
        );
        append_and_apply_table_ops_with_persisted_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            second_prepare_entry.sequence,
            vec![DfsMetadataTableOp::PutNamespacePrepareJournalReference {
                shard_id: shard_id.clone(),
                journal_sequence: second_prepare_entry.sequence,
                file_system_id: second_prepare.file_system_id.clone(),
                operation_id: second_prepare.operation_id.clone(),
                operation_digest: second_prepare.operation_digest.clone(),
                owner_epoch: second_prepare.owner_epoch,
                status: second_prepare.status.clone(),
            }],
            vec![DfsMetadataTableOp::put_namespace_prepare(
                second_prepare.clone(),
            )],
            None,
            max_replay_bytes,
        )
        .expect("append second-generation compact prepare reference");
        assert_eq!(
            table_log_journal_reference_replay_cost_bytes,
            namespace_prepare_replay_cost_bytes(&second_prepare),
            "the live writer must rebase stale pressure counters onto the new active generation"
        );
        assert!(
            table_log_offset <= max_replay_bytes,
            "the second generation must remain below the physical trigger"
        );
        wait_for_checkpoint_worker(root.path());
        assert_eq!(
            fs::read(metadata_table_snapshot_path(root.path()))
                .expect("snapshot after pending second generation"),
            first_snapshot,
            "the second pending prepare must not replace the first checkpoint"
        );

        let second_commit_entry = write_local_journal_entry(
            root.path(),
            &shard_id,
            &mut next_sequence,
            DfsMetadataMutation::commit_namespace_prepare(&second_prepare),
        )
        .expect("append second-generation authoritative commit");
        prefix_proof = Some(
            dfs_metadata_prefix_proof_after(
                prefix_proof.as_ref().expect("second prepare prefix proof"),
                &second_commit_entry,
            )
            .expect("extend second-generation commit prefix proof"),
        );
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            second_commit_entry.sequence,
            vec![namespace_prepare_commit_op(&second_prepare)],
            max_replay_bytes,
        )
        .expect("append second-generation compact commit");
        wait_for_checkpoint_worker(root.path());
        assert_ne!(
            fs::read(metadata_table_snapshot_path(root.path()))
                .expect("second-generation snapshot bytes"),
            first_snapshot,
            "terminal second-generation pressure must publish another checkpoint"
        );

        reset_metadata_table_journal_reference_resolutions();
        let reopened = read_metadata_table_state(root.path()).expect("reopen checkpointed tables");
        assert_eq!(reopened.tables, tables);
        assert_eq!(
            metadata_table_journal_reference_resolutions(),
            0,
            "reopen must not resolve the checkpointed journal-reference suffix"
        );
        assert_eq!(
            reopened.table_log_journal_reference_replay_cost_bytes, 0,
            "the installed snapshot starts a fresh replay-pressure generation"
        );
    }

    #[test]
    fn legacy_unpriced_prepare_reference_is_checkpointed_conservatively() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-unpriced-reference");
        let shard_id = DfsMetadataShardId::new("shard_table_semantic_prepare");
        let prepare = namespace_prepare(
            "operation_legacy_unpriced_reference",
            DfsNamespacePrepareStatus::Committed,
        );
        let mut next_sequence = 1;
        let entry = write_local_journal_entry(
            root.path(),
            &shard_id,
            &mut next_sequence,
            DfsMetadataMutation::put_namespace_prepare(&prepare),
        )
        .expect("append authoritative namespace prepare");
        let prefix_proof = dfs_metadata_prefix_proof_after(
            &dfs_metadata_prefix_proof_seed(&shard_id, 0, "empty-unpriced-generation"),
            &entry,
        )
        .expect("extend exact prefix proof");
        let legacy_batch = DfsMetadataTableBatch {
            sequence: entry.sequence,
            ops: vec![DfsMetadataTableOp::PutNamespacePrepareJournalReference {
                shard_id,
                journal_sequence: entry.sequence,
                file_system_id: prepare.file_system_id.clone(),
                operation_id: prepare.operation_id.clone(),
                operation_digest: prepare.operation_digest.clone(),
                owner_epoch: prepare.owner_epoch,
                status: prepare.status.clone(),
            }],
            prefix_proof: Some(prefix_proof),
            // Zero is omitted from the encoded record, matching batches
            // written before replay-pressure accounting was introduced.
            journal_reference_replay_cost_bytes: 0,
        };
        append_metadata_table_batch(root.path(), &legacy_batch)
            .expect("append legacy compact table batch");

        let state = read_metadata_table_state(root.path()).expect("replay legacy reference");
        assert_eq!(state.table_log_journal_reference_replay_cost_bytes, 0);
        assert_eq!(
            state.table_log_unpriced_namespace_prepare_references, 1,
            "a missing legacy estimate must never mean zero replay work"
        );
        schedule_metadata_table_checkpoint_if_needed(
            root.path(),
            &state.tables,
            u64::MAX,
            state.table_log_offset,
            state.table_log_journal_reference_replay_cost_bytes,
            state.table_log_unpriced_namespace_prepare_references,
        )
        .expect("schedule conservative legacy checkpoint");
        wait_for_checkpoint_worker(root.path());

        reset_metadata_table_journal_reference_resolutions();
        let reopened = read_metadata_table_state(root.path()).expect("reopen legacy checkpoint");
        assert_eq!(reopened.tables, state.tables);
        assert_eq!(
            metadata_table_journal_reference_resolutions(),
            0,
            "legacy references must be absorbed into the self-contained snapshot"
        );
    }

    #[test]
    fn compact_namespace_commit_dependency_rejects_before_durable_append() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-semantic-missing-prepare");
        let prepare = namespace_prepare(
            "operation_semantic_missing",
            DfsNamespacePrepareStatus::Prepared,
        );
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;
        let mut table_log_journal_reference_replay_cost_bytes = 0;
        let mut table_log_unpriced_namespace_prepare_references = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &prepare.shard_id,
            1,
            "semantic-missing-prepare",
        ));

        let error = append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![namespace_prepare_commit_op(&prepare)],
            u64::MAX,
        )
        .expect_err("a semantic marker without its prepared body must fail");

        assert!(matches!(error, MetadataBackendError::Serde(_)));
        assert_eq!(table_log_offset, 0);
        assert_eq!(materialized_through_sequence, 0);
        assert_eq!(
            read_metadata_table_state(root.path())
                .expect("empty table state")
                .tables,
            tables
        );
    }

    #[test]
    fn compact_namespace_commit_fences_digest_epoch_and_aborted_prepare() {
        let prepare = namespace_prepare(
            "operation_semantic_fences",
            DfsNamespacePrepareStatus::Prepared,
        );
        let mut tables = DfsMetadataTables::default();
        tables.apply(DfsMetadataTableOp::put_namespace_prepare(prepare.clone()));

        let mut wrong_digest = namespace_prepare_commit_op(&prepare);
        let DfsMetadataTableOp::CommitNamespacePrepare {
            operation_digest, ..
        } = &mut wrong_digest
        else {
            unreachable!("compact marker")
        };
        *operation_digest = "wrong_digest".to_string();
        assert!(tables.validate_checked(&wrong_digest).is_err());

        let mut wrong_epoch = namespace_prepare_commit_op(&prepare);
        let DfsMetadataTableOp::CommitNamespacePrepare { owner_epoch, .. } = &mut wrong_epoch
        else {
            unreachable!("compact marker")
        };
        *owner_epoch += 1;
        assert!(tables.validate_checked(&wrong_epoch).is_err());

        let mut aborted = prepare.clone();
        aborted.status = DfsNamespacePrepareStatus::Aborted;
        tables.apply(DfsMetadataTableOp::put_namespace_prepare(aborted));
        assert!(
            tables
                .validate_checked(&namespace_prepare_commit_op(&prepare))
                .is_err()
        );
    }

    #[test]
    fn compact_namespace_commit_append_failure_rolls_back_materialization() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-semantic-append-failure");
        let blocked_root = root.path().join("not-a-directory");
        fs::write(&blocked_root, b"block metadata table directory").expect("blocking file");
        let prepare = namespace_prepare(
            "operation_semantic_rollback",
            DfsNamespacePrepareStatus::Prepared,
        );
        let mut tables = DfsMetadataTables::default();
        tables.apply(DfsMetadataTableOp::put_namespace_prepare(prepare.clone()));
        let before = tables.clone();
        let marker = namespace_prepare_commit_op(&prepare);
        let mut rollback = DfsMetadataTableRollback::default();
        tables
            .apply_checked_with_rollback(marker.clone(), &mut rollback)
            .expect("preapply compact commit");
        assert_ne!(tables, before);

        let mut table_log_offset = 0;
        let mut table_log_journal_reference_replay_cost_bytes = 0;
        let mut table_log_unpriced_namespace_prepare_references = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &prepare.shard_id,
            1,
            "semantic-append-failure",
        ));
        append_preapplied_table_ops(
            &blocked_root,
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![marker],
            u64::MAX,
            rollback,
        )
        .expect_err("failed marker append must roll back its materialized state");

        assert_eq!(tables, before);
        assert_eq!(table_log_offset, 0);
        assert_eq!(materialized_through_sequence, 0);
    }

    #[test]
    fn preapplied_batch_rolls_back_when_separate_apply_ops_are_rejected() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-preapply-separate-ops");
        let before = DfsMetadataTables::default();
        let mut tables = before.clone();
        let persisted_op = namespace_transaction_op("txn_preapply_persisted");
        let mut rollback = DfsMetadataTableRollback::default();
        tables.apply_with_rollback(persisted_op.clone(), &mut rollback);
        assert_ne!(tables, before);

        let mut table_log_offset = 0;
        let mut table_log_journal_reference_replay_cost_bytes = 0;
        let mut table_log_unpriced_namespace_prepare_references = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = None;
        let error = append_table_batch(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![persisted_op],
            Some(vec![namespace_transaction_op("txn_separate_apply")]),
            None,
            u64::MAX,
            Some(rollback),
            None,
        )
        .expect_err("preapplied rollback and separate apply operations must be rejected");

        assert!(
            error
                .to_string()
                .contains("cannot combine preapplied rollback")
        );
        assert_eq!(tables, before);
        assert_eq!(table_log_offset, 0);
        assert_eq!(materialized_through_sequence, 0);
        assert!(!metadata_table_path(root.path()).exists());
    }

    #[test]
    fn trailing_prepare_reference_rejects_wrong_journal_sequence() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-reference-wrong-sequence");
        let prepare = namespace_prepare(
            "operation_reference_wrong_sequence",
            DfsNamespacePrepareStatus::Prepared,
        );
        let before = DfsMetadataTables::default();
        let mut tables = before.clone();
        let prefix_op = namespace_transaction_op("txn_reference_wrong_sequence_prefix");
        let mut rollback = DfsMetadataTableRollback::default();
        tables.apply_with_rollback(prefix_op.clone(), &mut rollback);
        assert_ne!(tables, before);

        let reference = DfsMetadataTableOp::PutNamespacePrepareJournalReference {
            shard_id: prepare.shard_id.clone(),
            journal_sequence: 2,
            file_system_id: prepare.file_system_id.clone(),
            operation_id: prepare.operation_id.clone(),
            operation_digest: prepare.operation_digest.clone(),
            owner_epoch: prepare.owner_epoch,
            status: prepare.status.clone(),
        };
        let mut table_log_offset = 0;
        let mut table_log_journal_reference_replay_cost_bytes = 0;
        let mut table_log_unpriced_namespace_prepare_references = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = None;
        let error = append_preapplied_table_ops_then_apply_trailing_resolved_op(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![prefix_op, reference],
            DfsMetadataTableOp::put_namespace_prepare(prepare),
            1,
            u64::MAX,
            rollback,
        )
        .expect_err("a compact prepare reference must point at the table batch sequence");

        assert!(
            error
                .to_string()
                .contains("cannot combine preapplied rollback")
        );
        assert_eq!(tables, before);
        assert_eq!(table_log_offset, 0);
        assert_eq!(materialized_through_sequence, 0);
        assert!(!metadata_table_path(root.path()).exists());
    }

    #[test]
    fn preapplied_batch_rolls_back_when_measured_cost_has_no_reference() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-preapply-invalid-price");
        let before = DfsMetadataTables::default();
        let mut tables = before.clone();
        let op = namespace_transaction_op("txn_preapply_invalid_price");
        let mut rollback = DfsMetadataTableRollback::default();
        tables.apply_with_rollback(op.clone(), &mut rollback);
        assert_ne!(tables, before);

        let mut table_log_offset = 0;
        let mut table_log_journal_reference_replay_cost_bytes = 0;
        let mut table_log_unpriced_namespace_prepare_references = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = None;
        let error = append_table_batch(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![op],
            None,
            Some(1),
            u64::MAX,
            Some(rollback),
            None,
        )
        .expect_err("measured replay cost without a journal reference must be rejected");

        assert!(
            error
                .to_string()
                .contains("cannot price journal-reference replay")
        );
        assert_eq!(tables, before);
        assert_eq!(table_log_offset, 0);
        assert_eq!(materialized_through_sequence, 0);
        assert!(!metadata_table_path(root.path()).exists());
    }

    #[test]
    fn preapplied_batch_rolls_back_without_prefix_proof() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-preapply-no-proof");
        let before = DfsMetadataTables::default();
        let mut tables = before.clone();
        let op = namespace_transaction_op("txn_preapply_no_proof");
        let mut rollback = DfsMetadataTableRollback::default();
        tables.apply_with_rollback(op.clone(), &mut rollback);
        assert_ne!(tables, before);

        let mut table_log_offset = 0;
        let mut table_log_journal_reference_replay_cost_bytes = 0;
        let mut table_log_unpriced_namespace_prepare_references = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = None;
        let error = append_preapplied_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![op],
            u64::MAX,
            rollback,
        )
        .expect_err("missing proof must reject the preapplied batch");

        assert!(matches!(error, MetadataBackendError::Serde(_)));
        assert_eq!(tables, before);
        assert_eq!(table_log_offset, 0);
        assert_eq!(materialized_through_sequence, 0);
    }

    #[test]
    fn preapplied_batch_rolls_back_when_table_append_fails() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-preapply-append-failure");
        let blocked_root = root.path().join("not-a-directory");
        fs::write(&blocked_root, b"block metadata table directory").expect("blocking file");

        let before = DfsMetadataTables::default();
        let mut tables = before.clone();
        let op = namespace_transaction_op("txn_preapply_append_failure");
        let mut rollback = DfsMetadataTableRollback::default();
        tables.apply_with_rollback(op.clone(), &mut rollback);
        assert_ne!(tables, before);

        let mut table_log_offset = 0;
        let mut table_log_journal_reference_replay_cost_bytes = 0;
        let mut table_log_unpriced_namespace_prepare_references = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &DfsMetadataShardId::new("shard_table_rollback"),
            1,
            "preappend-state",
        ));
        append_preapplied_table_ops(
            &blocked_root,
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![op],
            u64::MAX,
            rollback,
        )
        .expect_err("table append through a file path must fail");

        assert_eq!(tables, before);
        assert_eq!(table_log_offset, 0);
        assert_eq!(materialized_through_sequence, 0);
    }

    #[test]
    fn reopen_preserves_unjournaled_history_across_snapshot_install_crash_window() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-snapshot-prefix-crash-window");
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;
        let mut table_log_journal_reference_replay_cost_bytes = 0;
        let mut table_log_unpriced_namespace_prepare_references = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &DfsMetadataShardId::new("shard_table_snapshot_crash"),
            1,
            "snapshot-crash-state",
        ));
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![namespace_transaction_op("txn_snapshot_crash")],
            u64::MAX,
        )
        .expect("append table prefix");

        let checkpoint_tables = tables.clone();
        let snapshot_bytes =
            encode_metadata_table_snapshot(&checkpoint_tables, Some(1), prefix_proof.as_ref())
                .expect("encode checkpoint");
        // Deliberately stop after the first half of snapshot+log publication:
        // the old sequence-1 table record is still present.
        write_file_atomic(&metadata_table_snapshot_path(root.path()), &snapshot_bytes)
            .expect("install snapshot without rewriting the table log");

        let reopened = read_metadata_table_state(root.path()).expect("reopen crash window");
        assert!(
            reopened
                .tables
                .namespace_transactions
                .contains_key("txn_snapshot_crash"),
            "checkpoint publication must preserve full history that was not removed by a journal mutation"
        );
        assert!(
            reopened.tables.namespace_transaction_receipts.is_empty(),
            "a checkpoint must not invent an unjournaled compact receipt"
        );
        assert_eq!(reopened.materialized_through_sequence, Some(1));
    }

    #[test]
    fn background_checkpoint_runs_off_the_append_path_and_installs_exact_state() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-background-checkpoint");
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;
        let mut table_log_journal_reference_replay_cost_bytes = 0;
        let mut table_log_unpriced_namespace_prepare_references = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &DfsMetadataShardId::new("shard_table_background"),
            1,
            "background-checkpoint-state",
        ));
        let mut op = namespace_transaction_op("txn_background_checkpoint");
        let DfsMetadataTableOp::PutNamespaceTransaction { transaction } = &mut op else {
            unreachable!("namespace transaction test op");
        };
        transaction.created_at = OffsetDateTime::now_utc();
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![op],
            0,
        )
        .expect("foreground append only schedules checkpoint");
        assert!(
            tables
                .namespace_transactions
                .contains_key("txn_background_checkpoint"),
            "the worker must not mutate the caller's live tables behind its lock"
        );

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let running = METADATA_TABLE_CHECKPOINT_WORKERS
                .get()
                .is_some_and(|registry| {
                    registry
                        .workers
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .contains_key(root.path())
                });
            if !running {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "background checkpoint did not finish"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let reopened = read_metadata_table_state(root.path()).expect("read checkpoint");
        assert!(
            reopened
                .tables
                .namespace_transactions
                .contains_key("txn_background_checkpoint"),
            "background checkpointing must preserve full history absent journaled GC"
        );
        assert!(
            reopened.tables.namespace_transaction_receipts.is_empty(),
            "background checkpointing must not invent compact receipts"
        );
        assert_eq!(reopened.materialized_through_sequence, Some(1));
        assert_eq!(
            fs::metadata(metadata_table_path(root.path()))
                .expect("table log")
                .len(),
            0,
            "a checkpoint with no concurrent suffix should rotate the table log"
        );
    }

    #[test]
    fn configured_checkpoint_coordination_lock_serializes_processes() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-host-coordination");
        let path = root.path().join("checkpoint.lock");
        let first = metadata_table_checkpoint_coordination_lock_at(&path)
            .expect("acquire first coordination lock");
        let (attempted_tx, attempted_rx) = std::sync::mpsc::channel();
        let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
        let contender_path = path.clone();
        let contender = std::thread::spawn(move || {
            attempted_tx.send(()).expect("announce lock attempt");
            let guard = metadata_table_checkpoint_coordination_lock_at(&contender_path)
                .expect("acquire contended coordination lock");
            acquired_tx.send(()).expect("announce lock acquisition");
            drop(guard);
        });

        attempted_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("contender started");
        assert!(
            acquired_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "a second process lock must remain blocked while the first is held"
        );
        drop(first);
        acquired_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("contender acquired released lock");
        contender.join().expect("coordination contender");
    }

    #[test]
    fn physical_and_logical_checkpoint_folds_are_serialized_per_process() {
        fn append_terminal_table_state(
            root: &Path,
            shard_id: &DfsMetadataShardId,
            transaction_id: &str,
        ) -> (DfsMetadataTables, u64) {
            let mut tables = DfsMetadataTables::default();
            let mut table_log_offset = 0;
            let mut table_log_journal_reference_replay_cost_bytes = 0;
            let mut table_log_unpriced_namespace_prepare_references = 0;
            let mut materialized_through_sequence = 0;
            let mut table_snapshot_fingerprint = None;
            let mut prefix_proof =
                Some(dfs_metadata_prefix_proof_seed(shard_id, 1, transaction_id));
            append_and_apply_table_ops(
                root,
                TableMaterializationState {
                    tables: &mut tables,
                    table_log_offset: &mut table_log_offset,
                    table_log_journal_reference_replay_cost_bytes:
                        &mut table_log_journal_reference_replay_cost_bytes,
                    table_log_unpriced_namespace_prepare_references:
                        &mut table_log_unpriced_namespace_prepare_references,
                    materialized_through_sequence: &mut materialized_through_sequence,
                    table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                    prefix_proof: &mut prefix_proof,
                },
                1,
                vec![namespace_transaction_op(transaction_id)],
                u64::MAX,
            )
            .expect("append terminal table state");
            (tables, table_log_offset)
        }

        let first = sinomo_testdir::TestDir::new("sinomo-dfs-table-logical-serial-first");
        let second = sinomo_testdir::TestDir::new("sinomo-dfs-table-logical-serial-second");
        let (first_tables, first_offset) = append_terminal_table_state(
            first.path(),
            &DfsMetadataShardId::new("shard_table_logical_serial_first"),
            "txn_logical_serial_first",
        );
        let (second_tables, second_offset) = append_terminal_table_state(
            second.path(),
            &DfsMetadataShardId::new("shard_table_logical_serial_second"),
            "txn_logical_serial_second",
        );

        let fold = METADATA_TABLE_CHECKPOINT_FOLD
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        schedule_metadata_table_checkpoint_if_needed(
            first.path(),
            &first_tables,
            first_offset.saturating_sub(1),
            first_offset,
            0,
            0,
        )
        .expect("schedule physical fold");
        schedule_metadata_table_checkpoint_if_needed(
            second.path(),
            &second_tables,
            u64::MAX - 1,
            second_offset,
            u64::MAX,
            0,
        )
        .expect("schedule logical fold");
        let registry = METADATA_TABLE_CHECKPOINT_WORKERS
            .get()
            .expect("checkpoint registry");
        let workers = registry
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(workers.contains_key(first.path()));
        assert!(workers.contains_key(second.path()));
        drop(workers);
        drop(fold);

        wait_for_checkpoint_worker(first.path());
        wait_for_checkpoint_worker(second.path());
        assert!(metadata_table_snapshot_path(first.path()).is_file());
        assert!(metadata_table_snapshot_path(second.path()).is_file());
        assert_eq!(
            METADATA_TABLE_CHECKPOINT_FOLDS_MAX.load(Ordering::SeqCst),
            1,
            "physical and logical snapshot folds must never overlap within one process"
        );
    }

    #[test]
    fn checkpoint_install_preserves_records_appended_after_captured_boundary() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-checkpoint-suffix");
        let shard_id = DfsMetadataShardId::new("shard_table_suffix");
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;
        let mut table_log_journal_reference_replay_cost_bytes = 0;
        let mut table_log_unpriced_namespace_prepare_references = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &shard_id,
            1,
            "checkpoint-suffix-one",
        ));
        let mut first_op = namespace_transaction_op("txn_checkpoint_prefix");
        let DfsMetadataTableOp::PutNamespaceTransaction { transaction } = &mut first_op else {
            unreachable!("namespace transaction test op");
        };
        transaction.created_at = OffsetDateTime::now_utc();
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![first_op],
            u64::MAX,
        )
        .expect("append captured prefix");
        rotate_metadata_table_log_for_checkpoint(root.path(), 0)
            .expect("rotate captured table generation");
        let capture =
            read_metadata_table_checkpoint_state(root.path()).expect("capture table boundary");
        let checkpoint_tables = capture.tables;
        let snapshot_bytes =
            encode_metadata_table_snapshot(&checkpoint_tables, Some(1), prefix_proof.as_ref())
                .expect("encode captured snapshot");

        prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &shard_id,
            2,
            "checkpoint-suffix-two",
        ));
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                table_log_journal_reference_replay_cost_bytes:
                    &mut table_log_journal_reference_replay_cost_bytes,
                table_log_unpriced_namespace_prepare_references:
                    &mut table_log_unpriced_namespace_prepare_references,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            2,
            vec![namespace_transaction_op("txn_checkpoint_suffix")],
            u64::MAX,
        )
        .expect("append newer suffix");

        // A long-lived backend can still hold a pre-rotation in-memory view
        // while another process owns the checkpoint worker. The checkpoint
        // generation is therefore part of the read-cache generation: a
        // lock-free read must replay both it and the active suffix instead of
        // trusting the active log's reset byte length.
        let stale_tables = DfsMetadataTables::default();
        let read_cache = Arc::new(Mutex::new(None));
        let during_checkpoint = current_tables_snapshot(
            root.path(),
            &stale_tables,
            0,
            0,
            &None,
            &None,
            &None,
            &read_cache,
        )
        .expect("read across checkpoint generation");
        assert!(
            during_checkpoint
                .namespace_transactions
                .contains_key("txn_checkpoint_prefix")
        );
        assert!(
            during_checkpoint
                .namespace_transactions
                .contains_key("txn_checkpoint_suffix")
        );

        install_encoded_metadata_table_snapshot(root.path(), &snapshot_bytes)
            .expect("install checkpoint with suffix");
        remove_metadata_table_checkpoint_log(root.path())
            .expect("retire captured table generation");

        let reopened = read_metadata_table_state(root.path()).expect("reopen checkpoint suffix");
        assert_eq!(reopened.materialized_through_sequence, Some(2));
        assert!(
            reopened
                .tables
                .namespace_transactions
                .contains_key("txn_checkpoint_prefix")
        );
        assert!(reopened.tables.namespace_transaction_receipts.is_empty());
        assert!(
            reopened
                .tables
                .namespace_transactions
                .contains_key("txn_checkpoint_suffix")
        );
    }
}
