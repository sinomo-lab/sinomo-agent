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
    prune_segmented_journal_before_checkpoint,
};
use crate::metadata_lock::{
    MetadataWriteActivity, metadata_checkpoint_write_lock, metadata_write_activity,
};
use crate::metadata_records::{DfsMetadataPrefixProof, DfsMetadataTableBatch, DfsMetadataTableOp};
#[cfg(test)]
use crate::metadata_table_store::rotate_metadata_table_log_for_checkpoint;
use crate::metadata_table_store::{
    append_metadata_table_batch, current_metadata_table_log_replay,
    metadata_table_checkpoint_log_path, metadata_table_path, metadata_table_snapshot_fingerprint,
    persist_metadata_table_snapshot, read_metadata_table_checkpoint_state,
    remove_metadata_table_checkpoint_log,
    rotate_metadata_table_log_for_checkpoint_at_replay_boundary,
};
use crate::metadata_tables::{
    DfsMetadataTableRollback, DfsMetadataTables, MetadataTableSnapshotFingerprint,
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
    has_pending_prepared_operations: bool,
    physical_checkpoint_required: bool,
    physical_checkpoint_not_before: Instant,
    replay_pressure_boundary: Option<u64>,
    replay_cost_bytes: u64,

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
    max_log_bytes: u64,
) -> Option<MetadataTableCheckpointTrigger> {
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
            "emergency-cap-immediate",
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
    max_log_bytes: u64,
) {
    worker.replay_pressure_boundary = Some(table_log_offset);
    worker.replay_cost_bytes = table_log_offset;

    worker.max_log_bytes = max_log_bytes;
    match metadata_table_logical_checkpoint_pressure(worker.replay_cost_bytes, max_log_bytes) {
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
    append_table_batch(root, state, sequence, ops, max_log_bytes, None)
}

/// Persist operations already checked and preapplied under the writer lock.
/// Append failure restores their exact rollback; successful append owns replay.
pub(crate) fn append_preapplied_table_ops(
    root: &Path,
    state: TableMaterializationState<'_>,
    sequence: u64,
    ops: Vec<DfsMetadataTableOp>,
    max_log_bytes: u64,
    rollback: DfsMetadataTableRollback,
) -> Result<()> {
    append_table_batch(root, state, sequence, ops, max_log_bytes, Some(rollback))
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
    if len <= max_log_bytes && !has_rotated_checkpoint {
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

pub(crate) fn schedule_metadata_table_checkpoint_if_needed(
    root: &Path,
    tables: &DfsMetadataTables,
    max_log_bytes: u64,
    table_log_offset: u64,
) -> Result<()> {
    let logical_pressure =
        metadata_table_logical_checkpoint_pressure(table_log_offset, max_log_bytes);
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
    let has_pending_prepared_operations = tables
        .prepared_operations_v7
        .values()
        .any(|record| record.status == crate::DfsPreparedOperationStatus::Prepared);
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
                worker.has_pending_prepared_operations = has_pending_prepared_operations;
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
            worker.has_pending_prepared_operations = has_pending_prepared_operations;
            worker.replay_cost_bytes = table_log_offset;

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
                    max_log_bytes,
                );
            }
            registry.changed.notify_all();
            return Ok(());
        }
        if !checkpoint_generation_present
            && !physical_checkpoint_required
            && has_pending_prepared_operations
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
            has_pending_prepared_operations,
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
            replay_cost_bytes: table_log_offset,
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
        if worker.has_pending_prepared_operations
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
            && (worker.revision != request.revision || worker.has_pending_prepared_operations))
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
        let replay_pressure_exceeded =
            metadata_table_logical_checkpoint_pressure(replay.offset, max_log_bytes).is_some();

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
        worker.replay_cost_bytes = replay.offset;

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
                    "metadata-table-checkpoint root={} trigger={} deferral_reason={} checkpointed={} preempted={} preemption_reason={} queued_ms={} eligible_queue_ms={} admission_wait_ms={} coordination_wait_ms={} coordination_lock_configured={} process_spacing_applied={} replay_cost_bytes={} max_log_bytes={} hard_threshold_bytes={} emergency_threshold_bytes={} checkpoint_log_bytes={} snapshot_bytes={} rotate_ms={} decode_ms={} encode_ms={} stage_ms={} install_lock_wait_ms={} publish_ms={} install_ms={} allocator_trimmed={} allocator_trim_ms={} total_ms={}",
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
    if worker.has_pending_prepared_operations {
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
    let staged = crate::metadata_snapshot::stage_snapshot(
        root,
        &tables,
        checkpoint_sequence,
        &prefix_proof,
    )?;
    let encode_ms = encode_started.elapsed().as_millis();
    let snapshot_len = usize::try_from(staged.len).unwrap_or(usize::MAX);
    let stage_ms = 0;
    let install_started = Instant::now();
    let install_lock_started = Instant::now();
    let _write_lock =
        metadata_checkpoint_write_lock(root.to_path_buf(), journal_write_lock_path(root))?;
    let install_lock_wait_ms = install_lock_started.elapsed().as_millis();
    let publish_started = Instant::now();
    if metadata_table_snapshot_fingerprint(root)? != snapshot_fingerprint
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
    let journal_admission =
        crate::metadata_journal::admission::capture_owned_checkpoint_admission(root)?;
    staged.install(root)?;
    drop(tables);
    remove_metadata_table_checkpoint_log(root)?;
    advance_activated_handoff_rebind_checkpoint(root, &prefix_proof)?;
    // The installed snapshot and newer table-log suffix are durable before
    // source history is pruned. Retaining the checkpoint journal segment
    // leaves a discontinuity that forces lagging replicas through snapshot
    // installation rather than reporting a false empty suffix.
    prune_segmented_journal_before_checkpoint(root, checkpoint_sequence)?;
    if let Some(admission) = journal_admission {
        admission.record_installed(&prefix_proof)?;
    }
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
    let mut reconstructed = freeze.rebind_prefix_proof.clone();
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
            let entries = entries
                .into_iter()
                .take_while(|entry| entry.sequence <= checkpoint.journal_sequence)
                .collect::<Vec<_>>();
            // The caller owns the journal writer for this entire cut. Reuse
            // the existing bounded exact-frame reader instead of reopening
            // and revalidating the same segment for every entry.
            let frames = crate::metadata_journal::read_segmented_journal_frames_at_sequences(
                root,
                &entries
                    .iter()
                    .map(|entry| entry.sequence)
                    .collect::<Vec<_>>(),
            )?;
            for (entry, frame) in entries.into_iter().zip(frames) {
                if entry.sequence != reconstructed.journal_sequence.saturating_add(1) {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata shard handoff {} at {} has a journal gap after sequence {}",
                        freeze.handoff_id,
                        root.display(),
                        reconstructed.journal_sequence
                    )));
                }
                let frame = frame.ok_or_else(|| {
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

    if &freeze.rebind_prefix_proof != checkpoint {
        freeze.rebind_prefix_proof = checkpoint.clone();
        write_sobj_atomic(&path, &freeze)?;
    }
    Ok(())
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

    *state.table_snapshot_fingerprint = snapshot_fingerprint;
    Ok(())
}

fn append_table_batch(
    root: &Path,
    mut state: TableMaterializationState<'_>,
    sequence: u64,
    persisted_ops: Vec<DfsMetadataTableOp>,

    max_log_bytes: u64,
    mut rollback: Option<DfsMetadataTableRollback>,
) -> Result<()> {
    if let Err(error) = reconcile_table_log_generation_after_background_checkpoint(root, &mut state)
    {
        if let Some(rollback) = rollback.take() {
            rollback.rollback(state.tables);
        }
        return Err(error);
    }
    let validation_ops = persisted_ops.as_slice();
    if let Err(error) =
        validate_version_pin_removal_order(state.tables, validation_ops, rollback.is_some())
    {
        if let Some(rollback) = rollback.take() {
            rollback.rollback(state.tables);
        }
        return Err(error);
    }
    let semantic_op_count = validation_ops
        .iter()
        .filter(|op| matches!(op, DfsMetadataTableOp::CommitPreparedOperationV7 { .. }))
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
    if rollback.is_none() {
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
    let apply_after_append = rollback.is_none();
    let batch = DfsMetadataTableBatch {
        sequence,
        ops: persisted_ops,
        prefix_proof: Some(prefix_proof.clone()),
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

    if apply_after_append {
        // The complete table batch now owns crash recovery for the trailing
        // semantic transition. Never retain or run the preappend rollback
        // after this boundary.
        drop(rollback.take());
        for op in batch.ops {
            state.tables.apply_checked(op)?;
        }
    }
    *state.materialized_through_sequence = (*state.materialized_through_sequence).max(sequence);
    schedule_metadata_table_checkpoint_if_needed(
        root,
        state.tables,
        max_log_bytes,
        *state.table_log_offset,
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
    use crate::metadata_read_snapshot::current_tables_snapshot;
    use crate::metadata_records::{
        DfsMetadataJournalEntry, DfsMetadataMutation, DfsMetadataShardId, DfsNamespaceTransaction,
        DfsNamespaceTransactionReceipt, DfsPreparedOperationDigest, DfsPreparedOperationPayload,
        DfsPreparedOperationRecord, DfsPreparedOperationScope, DfsPreparedOperationStatus,
        dfs_metadata_prefix_proof_genesis,
    };
    use crate::metadata_table_store::read_metadata_table_state;
    use std::sync::{Arc, Mutex};
    use time::OffsetDateTime;

    // Storage tests use exact bytes from current frames, with no publication
    // permit or fabricated cluster acknowledgement.
    fn current_frame_prefix(
        shard: &DfsMetadataShardId,
        sequence: u64,
        label: &str,
    ) -> DfsMetadataPrefixProof {
        let mut proof = dfs_metadata_prefix_proof_genesis(shard);
        for index in 1..=sequence {
            let fs = DfsFileSystemId::from_legacy_named("fs_table_prefix").expect("filesystem");
            let payload = DfsPreparedOperationPayload::for_metadata_effects(
                DfsPreparedOperationScope {
                    file_system_id: fs.clone(),
                    shard_id: shard.clone(),
                    owner_epoch: 0,
                    operation_id: format!("{label}:{index}"),
                },
                Vec::new(),
                Vec::new(),
                Vec::new(),
                OffsetDateTime::UNIX_EPOCH,
            )
            .expect("current prepare payload");
            let record = DfsPreparedOperationRecord {
                digest: DfsPreparedOperationDigest::at_sequence(&payload, index).expect("digest"),
                payload: Arc::new(payload),
                status: DfsPreparedOperationStatus::Prepared,
                committed_entry: None,
            };
            let entry = DfsMetadataJournalEntry {
                shard_id: shard.clone(),
                sequence: index,
                created_at: OffsetDateTime::UNIX_EPOCH,
                mutation: DfsMetadataMutation::PrepareOperationV7 {
                    file_system_id: fs,
                    operation_id: record.digest.operation_id.clone(),
                    record: Box::new(record),
                },
            };
            let frame = crate::metadata_journal::DfsMetadataJournalFrame::encode(&entry)
                .expect("current frame");
            proof =
                metadata_prefix_proof_after_exact_frame(&proof, &entry, shard, frame.exact_bytes())
                    .expect("exact frame prefix");
        }
        proof
    }

    fn namespace_transaction_op(transaction_id: &str) -> DfsMetadataTableOp {
        let transaction = DfsNamespaceTransaction {
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
            transaction_id: transaction_id.to_owned(),
            file_system_id: DfsFileSystemId::from_legacy_named("fs_table_rollback")
                .expect("filesystem"),
            objects: Vec::new(),
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::now_utc(),
        };
        DfsMetadataTableOp::PutNamespaceTransactionReceipt {
            receipt: DfsNamespaceTransactionReceipt::from_transaction(
                &transaction,
                transaction.created_at,
            )
            .expect("current receipt"),
        }
    }

    fn wait_for_checkpoint_worker(root: &Path) {
        wait_for_metadata_table_checkpoint_workers_for_test(root);
    }

    fn logical_checkpoint_worker_state(
        root: &Path,
        requested_at: Instant,
        replay_cost_bytes: u64,
        max_log_bytes: u64,
    ) -> MetadataTableCheckpointWorkerState {
        let mut worker = MetadataTableCheckpointWorkerState {
            revision: 1,
            requested_at,
            last_activity_at: requested_at,
            has_pending_prepared_operations: false,
            physical_checkpoint_required: false,
            physical_checkpoint_not_before: requested_at,
            replay_pressure_boundary: None,
            replay_cost_bytes,
            max_log_bytes,
            logical_checkpoint_stagger: metadata_table_logical_checkpoint_stagger(root),
            logical_hard_checkpoint_not_before: None,
            logical_emergency_pressure: false,
        };
        update_metadata_table_logical_checkpoint_pressure(
            root,
            &mut worker,
            requested_at,
            replay_cost_bytes,
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
                logical_checkpoint_worker_state(root.path(), requested_at, 101, 100),
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
                logical_checkpoint_worker_state(root.path(), Instant::now(), 301, 100),
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
                logical_checkpoint_worker_state(root.path(), Instant::now(), 301, 100),
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
        let worker = logical_checkpoint_worker_state(root.path(), requested_at, 101, 100);
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
        let mut worker = logical_checkpoint_worker_state(root.path(), requested_at, 201, 100);
        worker.has_pending_prepared_operations = true;
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
    fn emergency_byte_pressure_bypasses_stagger_and_process_spacing() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-logical-emergency");
        let requested_at = Instant::now();
        for worker in [logical_checkpoint_worker_state(
            root.path(),
            requested_at,
            301,
            100,
        )] {
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
        let mut worker = logical_checkpoint_worker_state(root.path(), requested_at, 201, 100);
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
    fn native_rebind_checkpoint_verifies_complete_history_with_bounded_segment_reads() {
        use crate::DfsMetadataBackend;
        fn copy_native_tree(from: &Path, to: &Path) {
            fs::create_dir_all(to).unwrap();
            for entry in fs::read_dir(from).unwrap() {
                let entry = entry.unwrap();
                let kind = entry.file_type().unwrap();
                assert!(!kind.is_symlink());
                let target = to.join(entry.file_name());
                if kind.is_dir() {
                    copy_native_tree(&entry.path(), &target);
                } else {
                    assert!(kind.is_file());
                    fs::copy(entry.path(), &target).unwrap();
                    assert_eq!(fs::read(entry.path()).unwrap(), fs::read(target).unwrap());
                }
            }
        }
        let (mut native, client, initial) = crate::test_support::current_native_metadata_service(
            "native-rebind-checkpoint-source-reads",
            2,
            "/services",
        )
        .unwrap();
        drop(client);
        let promoted = initial.replica_node_ids[0].clone();
        let current = native
            .reassign_metadata_shard(
                &initial.file_system_id,
                &initial.shard_id,
                promoted.clone(),
                vec![initial.primary_node_id.clone()],
            )
            .unwrap();
        // The retiring participant owns the activated checkpoint marker.
        // The promoted participant has a distinct promotion marker.
        let retiring = initial.primary_node_id.clone();
        let source = crate::default_dfs_metadata_shards_root(native.data_dir(&retiring))
            .join(&current.shard_id);
        let original_marker = read_sobj_optional::<DfsMetadataShardWriteFreeze>(
            &membership_handoff_write_freeze_path(&source),
        )
        .unwrap()
        .expect("actual activated retiring-primary handoff marker");
        assert_eq!(original_marker.activated_epoch, Some(current.epoch));
        let original_marker_bytes =
            fs::read(membership_handoff_write_freeze_path(&source)).unwrap();
        let mut writer = native.client(&promoted).unwrap();
        let mut written = Vec::new();
        for index in 0..64 {
            written.push(
                writer
                    .posix_write_file(
                        &format!("/services/rebind-source-{index}"),
                        format!("native record {index}").as_bytes(),
                        crate::DfsWriteOptions::replicated_eventual(vec![promoted.clone()]),
                    )
                    .unwrap(),
            );
        }
        // A normal full-shard prepare carries the preceding 64 commits to
        // the replica without replacing any receiver evidence.
        writer
            .posix_mkdir_all("/services/rebind-source-barrier")
            .unwrap();
        drop(writer);
        native.stop_node(&retiring).unwrap();
        let copy = sinomo_testdir::TestDir::new("native-rebind-checkpoint-source-copy");
        copy_native_tree(&source, copy.path());
        native.close().unwrap();
        let shard = DfsMetadataShardId::new(current.shard_id);
        // Replay only the actual accepted journal in this fresh copy; no
        // runtime owner or publication capability is created here.
        let inspected = crate::LocalSobjMetadataBackend::new(shard.clone(), copy.path()).unwrap();
        let target = inspected.metadata_prefix_proof().unwrap();
        for stat in &written {
            let object = DfsMetadataBackend::get_object(&inspected, &stat.object_id)
                .unwrap()
                .unwrap();
            assert_eq!(object.version, stat.version);
            assert_eq!(object.content_hash, stat.content_hash);
        }
        drop(inspected);
        let expected_frames =
            target.journal_sequence - original_marker.rebind_prefix_proof.journal_sequence;
        assert!(
            expected_frames >= 128,
            "all 64 native writes must be covered"
        );
        // A stale checkpoint marker with a later durable suffix is a real
        // recovery state. Restore only the saved ACTUAL marker in this fresh
        // copy; no native authority or successful publication is fabricated.
        fs::write(
            membership_handoff_write_freeze_path(copy.path()),
            original_marker_bytes,
        )
        .unwrap();
        let _writer = metadata_checkpoint_write_lock(
            copy.path().to_path_buf(),
            journal_write_lock_path(copy.path()),
        )
        .unwrap();
        crate::metadata_journal::reset_metadata_journal_exact_read_counters();
        advance_activated_handoff_rebind_checkpoint(copy.path(), &target).unwrap();
        let segment_opens = crate::metadata_journal::metadata_journal_exact_segment_opens();
        let frame_decodes = crate::metadata_journal::metadata_journal_exact_frame_decodes();
        let marker = read_sobj_optional::<DfsMetadataShardWriteFreeze>(
            &membership_handoff_write_freeze_path(copy.path()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(marker.rebind_prefix_proof, target);
        assert_eq!(marker.handoff_id, original_marker.handoff_id);
        assert_eq!(marker.retiring_epoch, original_marker.retiring_epoch);
        assert_eq!(marker.activated_epoch, original_marker.activated_epoch);
        assert_eq!(frame_decodes as u64, expected_frames);
        assert!(
            segment_opens <= 2 * expected_frames.div_ceil(1024) as usize,
            "bounded native history opened {segment_opens} segments for {expected_frames} frames"
        );
    }

    #[test]
    fn background_checkpoint_preserves_native_journal_admission_certificate() {
        fn copy_native_tree(from: &Path, to: &Path) {
            fs::create_dir_all(to).unwrap();
            for entry in fs::read_dir(from).unwrap() {
                let entry = entry.unwrap();
                let kind = entry.file_type().unwrap();
                assert!(!kind.is_symlink());
                let target = to.join(entry.file_name());
                if kind.is_dir() {
                    copy_native_tree(&entry.path(), &target);
                } else {
                    assert!(kind.is_file());
                    fs::copy(entry.path(), &target).unwrap();
                    assert_eq!(fs::read(entry.path()).unwrap(), fs::read(target).unwrap());
                }
            }
        }
        let (mut native, mut service, config) =
            crate::test_support::current_native_metadata_service(
                "native-background-checkpoint-certificate",
                1,
                "/services",
            )
            .unwrap();
        service
            .posix_mkdir_all("/services/checkpoint-source")
            .unwrap();
        let original_object = service
            .lookup_sifs_path("/services/checkpoint-source")
            .unwrap()
            .unwrap();
        let original_root =
            crate::default_dfs_metadata_shards_root(native.data_dir(&config.primary_node_id))
                .join(&config.shard_id);
        drop(service);
        native.stop_node(&config.primary_node_id).unwrap();
        let copy = sinomo_testdir::TestDir::new("native-checkpoint-certificate-copy");
        copy_native_tree(&original_root, copy.path());
        native.close().unwrap();
        let shard = DfsMetadataShardId::new(config.shard_id);
        let backend = crate::LocalSobjMetadataBackend::new(shard.clone(), copy.path()).unwrap();
        let before = backend.metadata_prefix_proof().unwrap();
        drop(backend);
        let next =
            crate::metadata_journal::admission::certify_for_write(copy.path(), &shard).unwrap();
        let active_bytes = fs::metadata(metadata_table_path(copy.path()))
            .unwrap()
            .len();
        assert!(active_bytes > 0);
        let now = Instant::now();
        // Force only this copied native source's existing storage checkpoint.
        // The production threshold and the original700-file case stay unchanged.
        let request = MetadataTableCheckpointWorkerRequest {
            revision: 0,
            requested_at: now,
            eligible_at: now,
            trigger: MetadataTableCheckpointTrigger::Physical,
            deferral_reason: "native-storage-transition-test",
            replay_pressure_boundary: None,
            replay_cost_bytes: active_bytes,
            max_log_bytes: 1,
            hard_threshold_bytes: 2,
            emergency_threshold_bytes: 3,
            process_spacing_applied: false,
        };
        let run = run_metadata_table_checkpoint_worker(copy.path(), 1, request).unwrap();
        assert!(run.checkpointed);
        struct ResetCertificationFault;
        impl Drop for ResetCertificationFault {
            fn drop(&mut self) {
                crate::metadata_journal::fail_journal_certifications_for_test(0);
            }
        }
        let _reset = ResetCertificationFault;
        let _writer = crate::metadata_lock::metadata_write_lock(
            copy.path().to_path_buf(),
            journal_write_lock_path(copy.path()),
        )
        .unwrap();
        crate::metadata_journal::fail_journal_certifications_for_test(1);
        let actual = crate::metadata_journal::admission::certify_for_write(copy.path(), &shard);
        assert!(
            actual.is_ok(),
            "a completed owned checkpoint must retain its certificate without a cold snapshot scan: {actual:?}"
        );
        assert_eq!(actual.unwrap(), next);
        assert_eq!(
            crate::metadata_journal::admission::current_owned_materialized_prefix(
                copy.path(),
                &shard
            )
            .unwrap(),
            Some(before.clone())
        );
        // An otherwise identical external replacement has no owned transition.
        // Stable file identity changes still require actual cold certification.
        let snapshot = crate::metadata_table_store::metadata_table_snapshot_path(copy.path());
        let replacement = copy.path().join("external-snapshot-replacement.sobj");
        fs::copy(&snapshot, &replacement).unwrap();
        fs::rename(replacement, snapshot).unwrap();
        assert!(
            crate::metadata_journal::admission::certify_for_write(copy.path(), &shard).is_err()
        );
        crate::metadata_journal::fail_journal_certifications_for_test(0);
        drop(_writer);
        crate::metadata_journal::admission::forget_for_test(copy.path());
        let reopened = crate::LocalSobjMetadataBackend::new(shard.clone(), copy.path()).unwrap();
        assert_eq!(reopened.metadata_prefix_proof().unwrap(), before);
        assert_eq!(
            crate::DfsMetadataBackend::get_object(&reopened, &original_object.object_id).unwrap(),
            Some(original_object)
        );
        assert_eq!(
            crate::metadata_journal::admission::certify_for_write(copy.path(), &shard).unwrap(),
            next
        );
    }

    #[test]
    fn soft_checkpoint_preempts_before_rotation_when_foreground_revision_arrives() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-logical-soft-preemption");
        let shard_id = DfsMetadataShardId::new("shard_table_logical_soft_preemption");
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;
        let mut replay_cost = 0;
        let mut materialized_through_sequence = 0;
        let mut snapshot_fingerprint = None;
        let mut prefix_proof = Some(current_frame_prefix(&shard_id, 1, "soft-preemption"));
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,

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
        let mut worker = logical_checkpoint_worker_state(root.path(), requested_at, 101, 100);
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
                    has_pending_prepared_operations: false,
                    physical_checkpoint_required: true,
                    physical_checkpoint_not_before: requested_at,
                    replay_pressure_boundary: None,
                    replay_cost_bytes: 0,

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

        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(current_frame_prefix(&shard_id, 1, "checkpoint-resume"));
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,

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
        schedule_metadata_table_checkpoint_if_needed(root.path(), &tables, u64::MAX, 0)
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
                .namespace_transaction_receipts
                .contains_key("txn_checkpoint_resume")
        );
    }

    #[test]
    fn preapplied_batch_rolls_back_when_prefix_names_another_sequence() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-preapply-invalid-prefix");
        let before = DfsMetadataTables::default();
        let mut tables = before.clone();
        let op = namespace_transaction_op("txn_preapply_invalid_prefix");
        let mut rollback = DfsMetadataTableRollback::default();
        tables.apply_with_rollback(op.clone(), &mut rollback);
        assert_ne!(tables, before);

        let mut table_log_offset = 0;

        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(current_frame_prefix(
            &DfsMetadataShardId::new("shard_table_preapply_invalid_prefix"),
            2,
            "preapply-invalid-prefix",
        ));
        let error = append_table_batch(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,

                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![op],
            u64::MAX,
            Some(rollback),
        )
        .expect_err("preapplied batch cannot append under another journal-prefix cut");

        assert!(matches!(error, MetadataBackendError::Serde(ref message)
            if message.contains("prefix proof names sequence 2")));
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

        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = None;
        let error = append_preapplied_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,

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

        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(current_frame_prefix(
            &DfsMetadataShardId::new("shard_table_rollback"),
            1,
            "preappend-state",
        ));
        append_preapplied_table_ops(
            &blocked_root,
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,

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

        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(current_frame_prefix(
            &DfsMetadataShardId::new("shard_table_snapshot_crash"),
            1,
            "snapshot-crash-state",
        ));
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,

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
        let stage = crate::metadata_snapshot::stage_snapshot(
            root.path(),
            &checkpoint_tables,
            1,
            prefix_proof.as_ref().unwrap(),
        )
        .expect("stage checkpoint");
        // Deliberately stop after the first half of snapshot+log publication:
        // the old sequence-1 table record is still present.
        stage
            .install(root.path())
            .expect("install snapshot without rewriting the table log");

        let reopened = read_metadata_table_state(root.path()).expect("reopen crash window");
        assert!(
            reopened
                .tables
                .namespace_transaction_receipts
                .contains_key("txn_snapshot_crash"),
            "checkpoint publication must preserve the explicit terminal receipt"
        );
        assert_eq!(reopened.materialized_through_sequence, Some(1));
    }

    #[test]
    fn background_checkpoint_runs_off_the_append_path_and_installs_exact_state() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-background-checkpoint");
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;

        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(current_frame_prefix(
            &DfsMetadataShardId::new("shard_table_background"),
            1,
            "background-checkpoint-state",
        ));
        let mut op = namespace_transaction_op("txn_background_checkpoint");
        let DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt } = &mut op else {
            unreachable!("namespace transaction test op");
        };
        receipt.created_at = OffsetDateTime::now_utc();
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,

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
                .namespace_transaction_receipts
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
                .namespace_transaction_receipts
                .contains_key("txn_background_checkpoint"),
            "background checkpointing must preserve the explicit terminal receipt"
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
    fn physical_checkpoint_folds_are_serialized_per_process() {
        fn append_terminal_table_state(
            root: &Path,
            shard_id: &DfsMetadataShardId,
            transaction_id: &str,
        ) -> (DfsMetadataTables, u64) {
            let mut tables = DfsMetadataTables::default();
            let mut table_log_offset = 0;

            let mut materialized_through_sequence = 0;
            let mut table_snapshot_fingerprint = None;
            let mut prefix_proof = Some(current_frame_prefix(shard_id, 1, transaction_id));
            append_and_apply_table_ops(
                root,
                TableMaterializationState {
                    tables: &mut tables,
                    table_log_offset: &mut table_log_offset,

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

        let first = sinomo_testdir::TestDir::new("sinomo-dfs-table-checkpoint-serial-first");
        let second = sinomo_testdir::TestDir::new("sinomo-dfs-table-checkpoint-serial-second");
        let (first_tables, first_offset) = append_terminal_table_state(
            first.path(),
            &DfsMetadataShardId::new("shard_table_checkpoint_serial_first"),
            "txn_checkpoint_serial_first",
        );
        let (second_tables, second_offset) = append_terminal_table_state(
            second.path(),
            &DfsMetadataShardId::new("shard_table_checkpoint_serial_second"),
            "txn_checkpoint_serial_second",
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
        )
        .expect("schedule physical fold");
        schedule_metadata_table_checkpoint_if_needed(
            second.path(),
            &second_tables,
            // Current materialization tracks physical append bytes. Use
            // genuine pressure instead of an obsolete inflated replay price.
            second_offset.saturating_sub(1),
            second_offset,
        )
        .expect("schedule second physical fold");
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
        assert!(crate::metadata_table_store::metadata_table_snapshot_path(first.path()).is_file());
        assert!(crate::metadata_table_store::metadata_table_snapshot_path(second.path()).is_file());
        assert_eq!(
            METADATA_TABLE_CHECKPOINT_FOLDS_MAX.load(Ordering::SeqCst),
            1,
            "snapshot folds for independent roots must never overlap within one process"
        );
    }

    #[test]
    fn checkpoint_install_preserves_records_appended_after_captured_boundary() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-checkpoint-suffix");
        let shard_id = DfsMetadataShardId::new("shard_table_suffix");
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;

        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(current_frame_prefix(&shard_id, 1, "checkpoint-suffix-one"));
        let mut first_op = namespace_transaction_op("txn_checkpoint_prefix");
        let DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt } = &mut first_op else {
            unreachable!("namespace transaction test op");
        };
        receipt.created_at = OffsetDateTime::now_utc();
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,

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
        let stage = crate::metadata_snapshot::stage_snapshot(
            root.path(),
            &checkpoint_tables,
            1,
            prefix_proof.as_ref().unwrap(),
        )
        .expect("stage captured snapshot");

        prefix_proof = Some(current_frame_prefix(&shard_id, 2, "checkpoint-suffix-two"));
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,

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
                .namespace_transaction_receipts
                .contains_key("txn_checkpoint_prefix")
        );
        assert!(
            during_checkpoint
                .namespace_transaction_receipts
                .contains_key("txn_checkpoint_suffix")
        );

        stage
            .install(root.path())
            .expect("install checkpoint with suffix");
        remove_metadata_table_checkpoint_log(root.path())
            .expect("retire captured table generation");

        let reopened = read_metadata_table_state(root.path()).expect("reopen checkpoint suffix");
        assert_eq!(reopened.materialized_through_sequence, Some(2));
        assert!(
            reopened
                .tables
                .namespace_transaction_receipts
                .contains_key("txn_checkpoint_prefix")
        );
        assert!(
            reopened
                .tables
                .namespace_transaction_receipts
                .contains_key("txn_checkpoint_suffix")
        );
    }
}
