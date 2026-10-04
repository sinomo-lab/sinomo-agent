//! Incremental durable storage for the node-local DFS repair queue.
//!
//! The metadata journal remains the authority for write intents and receipts.
//! This store is a bounded restart accelerator: foreground commits append only
//! their repair-queue delta, while maintenance occasionally folds the framed
//! log into an atomic snapshot.

use crate::DfsError;
use crate::maintenance::DfsRepairAttemptRecord;
#[cfg(test)]
use crate::metadata_io::write_sobj_atomic;
use crate::metadata_io::{encode_sobj, metadata_durable_file_error, write_file_atomic};
use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_core::storage::durable_file;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read as _, Seek as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

const DFS_REPAIR_QUEUE_LOG_MAX_RECORD_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_DFS_REPAIR_QUEUE_COMPACTION_LOG_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct DfsRepairQueueKey {
    pub intent_id: String,
    pub missing_replica_node_id: NodeId,
}

impl DfsRepairQueueKey {
    pub fn from_record(record: &DfsRepairAttemptRecord) -> Self {
        Self {
            intent_id: record.intent_id.clone(),
            missing_replica_node_id: record.missing_replica_node_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DfsRepairQueueOperation {
    /// Insert a newly committed repair without regressing existing retry state.
    EnqueueIfMissing { record: DfsRepairAttemptRecord },
    /// Replace the mutable retry state for one existing repair identity.
    Set { record: DfsRepairAttemptRecord },
    /// Remove a repair only after authoritative metadata has settled it.
    Remove { key: DfsRepairQueueKey },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct DfsRepairQueueLogBatch {
    sequence: u64,
    operations: Vec<DfsRepairQueueOperation>,
}

/// On-disk snapshot envelope. The defaulted floor reads the legacy
/// `DfsRepairAttemptSnapshot` representation without migration IO.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct DfsRepairQueueSnapshotFile {
    #[serde(default)]
    records: Vec<DfsRepairAttemptRecord>,
    #[serde(default)]
    applied_through_sequence: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsRepairQueueView {
    pub head_sequence: u64,
    pub records: Vec<DfsRepairAttemptRecord>,
}

/// Bounded operator projection from an already-materialized repair queue.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsRepairQueueStatusView {
    pub head_sequence: u64,
    pub record_count: usize,
    pub records: Vec<DfsRepairAttemptRecord>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsRepairQueueMaintenancePage {
    pub total_record_count: usize,
    pub records: Vec<DfsRepairAttemptRecord>,
    /// Same-tick owner captures merged with exact existing retry state.
    pub enrollment_records: Vec<DfsRepairAttemptRecord>,
    pub has_more: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct DfsRepairQueuePageObservation {
    queue: DfsRepairQueueObservation,
    advances_scan: bool,
    after: Option<DfsRepairQueueKey>,
    records: Vec<DfsRepairAttemptRecord>,
}

impl DfsRepairQueuePageObservation {
    pub(crate) fn record_count(&self) -> usize {
        self.records.len()
    }
}

pub(crate) enum DfsRepairQueuePageReconcileOutcome {
    Applied {
        removed_target_count: usize,
        operation_count: usize,
        has_more: bool,
    },
    GenerationChanged {
        current_head_sequence: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsRepairQueueBoundedStatus {
    Hydrated(DfsRepairQueueStatusView),
    NotHydrated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DfsRepairQueueReconcileOutcome {
    Applied {
        head_sequence: u64,
        operation_count: usize,
    },
    GenerationChanged {
        current_head_sequence: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsRepairQueueCompactionOptions {
    pub max_log_bytes: u64,
    pub force: bool,
}

impl Default for DfsRepairQueueCompactionOptions {
    fn default() -> Self {
        Self {
            max_log_bytes: DEFAULT_DFS_REPAIR_QUEUE_COMPACTION_LOG_BYTES,
            force: false,
        }
    }
}

impl DfsRepairQueueCompactionOptions {
    pub fn force() -> Self {
        Self {
            max_log_bytes: 0,
            force: true,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsRepairQueueCompactionReport {
    pub compacted: bool,
    pub applied_through_sequence: u64,
    pub retained_record_count: usize,
    pub log_bytes_before: u64,
    pub log_bytes_after: u64,
}

/// Process-local, store-bound observation; never serialized as authority.
#[derive(Clone, Debug)]
pub(crate) struct DfsRepairQueueObservation {
    owner: Arc<()>,
    revision: u64,
    pub(crate) head_sequence: u64,
}

pub(crate) enum DfsRepairQueueObservedReconcileOutcome {
    Applied(DfsRepairQueueObservation),
    GenerationChanged { current_head_sequence: u64 },
}

#[derive(Clone, Debug)]
struct RepairQueueFileFingerprint {
    present: bool,
    bytes: u64,
    hasher: blake3::Hasher,
}

impl RepairQueueFileFingerprint {
    fn absent() -> Self {
        Self {
            present: false,
            bytes: 0,
            hasher: blake3::Hasher::new(),
        }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(bytes);
        Self {
            present: true,
            bytes: bytes.len() as u64,
            hasher,
        }
    }

    fn append_frame(&mut self, bytes: &[u8]) {
        self.present = true;
        self.hasher.update(&(bytes.len() as u32).to_le_bytes());
        self.hasher.update(bytes);
        self.bytes = self.bytes.saturating_add(4 + bytes.len() as u64);
    }
}

impl PartialEq for RepairQueueFileFingerprint {
    fn eq(&self, other: &Self) -> bool {
        self.present == other.present
            && self.bytes == other.bytes
            && self.hasher.finalize() == other.hasher.finalize()
    }
}
impl Eq for RepairQueueFileFingerprint {}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RepairQueueDiskFingerprint {
    snapshot: RepairQueueFileFingerprint,
    log: RepairQueueFileFingerprint,
}

fn fingerprint_error(path: &Path, detail: impl std::fmt::Display) -> DfsError {
    DfsError::Metadata {
        message: format!(
            "could not fingerprint DFS repair queue {}: {detail}",
            path.display()
        ),
    }
}

fn same_file_observation(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    if !left.is_file()
        || !right.is_file()
        || left.len() != right.len()
        || left.modified().ok() != right.modified().ok()
    {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if (left.dev(), left.ino(), left.ctime(), left.ctime_nsec())
            != (right.dev(), right.ino(), right.ctime(), right.ctime_nsec())
        {
            return false;
        }
    }
    true
}

/// Fixed-memory, exact-byte check. Metadata only detects a read racing a
/// replacement; cache equality always requires the content digest as well.
fn fingerprint_repair_queue_file(path: &Path) -> Result<RepairQueueFileFingerprint, DfsError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RepairQueueFileFingerprint::absent());
        }
        Err(error) => return Err(fingerprint_error(path, error)),
    };
    let before = file
        .metadata()
        .map_err(|error| fingerprint_error(path, error))?;
    if !before.is_file() {
        return Err(fingerprint_error(path, "not a regular file"));
    }
    let mut fingerprint = RepairQueueFileFingerprint::from_bytes(&[]);
    let mut buffer = [0_u8; 64 * 1024];
    while fingerprint.bytes < before.len() {
        let limit = (before.len() - fingerprint.bytes).min(buffer.len() as u64) as usize;
        let read = file
            .read(&mut buffer[..limit])
            .map_err(|error| fingerprint_error(path, error))?;
        if read == 0 {
            return Err(fingerprint_error(
                path,
                "file shrank during fingerprint read",
            ));
        }
        fingerprint.hasher.update(&buffer[..read]);
        fingerprint.bytes += read as u64;
    }
    let mut extra = [0_u8; 1];
    if file
        .read(&mut extra)
        .map_err(|error| fingerprint_error(path, error))?
        != 0
    {
        return Err(fingerprint_error(path, "file grew during fingerprint read"));
    }
    let after = file
        .metadata()
        .map_err(|error| fingerprint_error(path, error))?;
    let current = fs::metadata(path).map_err(|error| fingerprint_error(path, error))?;
    if !same_file_observation(&before, &after) || !same_file_observation(&after, &current) {
        return Err(fingerprint_error(
            path,
            "file changed during fingerprint read",
        ));
    }
    Ok(fingerprint)
}

#[derive(Debug)]
struct DfsRepairQueueState {
    snapshot_path: PathBuf,
    active_log_path: PathBuf,
    loaded: bool,
    owner: Arc<()>,
    revision: u64,
    fingerprint: Option<RepairQueueDiskFingerprint>,
    snapshot_floor: u64,
    head_sequence: u64,
    active_log_bytes: u64,
    valid_active_log_bytes: u64,
    /// An append sync returned an error, so replayed bytes must be synced (or
    /// a torn suffix truncated and synced) before a retry may acknowledge an
    /// idempotent no-op.
    resync_on_load: bool,
    records: BTreeMap<DfsRepairQueueKey, DfsRepairAttemptRecord>,
    // Process-local position only; persisted records retain all recovery work.
    maintenance_cursor: Option<DfsRepairQueueKey>,
    maintenance_restart_pending: bool,
    maintenance_revisit_pending: bool,
}

impl DfsRepairQueueState {
    fn new(snapshot_path: PathBuf) -> Self {
        let active_log_path = repair_queue_active_log_path(&snapshot_path);
        Self {
            snapshot_path,
            active_log_path,
            loaded: false,
            owner: Arc::new(()),
            revision: 0,
            fingerprint: None,
            snapshot_floor: 0,
            head_sequence: 0,
            active_log_bytes: 0,
            valid_active_log_bytes: 0,
            resync_on_load: false,
            records: BTreeMap::new(),
            maintenance_cursor: None,
            maintenance_restart_pending: false,
            maintenance_revisit_pending: false,
        }
    }

    fn observation(&self) -> DfsRepairQueueObservation {
        DfsRepairQueueObservation {
            owner: Arc::clone(&self.owner),
            revision: self.revision,
            head_sequence: self.head_sequence,
        }
    }

    fn next_revision(&self) -> Result<u64, DfsError> {
        self.revision
            .checked_add(1)
            .ok_or_else(|| DfsError::Metadata {
                message: "DFS repair queue resident revision overflow".to_string(),
            })
    }

    fn disk_fingerprint(&self) -> Result<RepairQueueDiskFingerprint, DfsError> {
        Ok(RepairQueueDiskFingerprint {
            snapshot: fingerprint_repair_queue_file(&self.snapshot_path)?,
            log: fingerprint_repair_queue_file(&self.active_log_path)?,
        })
    }

    fn refresh_from_disk(&mut self) -> Result<(), DfsError> {
        if self.loaded && !self.resync_on_load {
            match self.disk_fingerprint() {
                Ok(current) if self.fingerprint.as_ref() == Some(&current) => return Ok(()),
                Ok(_) => {}
                Err(error) => {
                    self.loaded = false;
                    self.fingerprint = None;
                    return Err(error);
                }
            }
        }
        self.loaded = false;
        self.maintenance_restart_pending = true;
        self.ensure_loaded()
    }

    fn ensure_loaded(&mut self) -> Result<(), DfsError> {
        if self.loaded && !self.resync_on_load {
            return Ok(());
        }
        self.loaded = false;
        self.fingerprint = None;
        let revision = self.next_revision()?;
        #[cfg(test)]
        if let Some(hook) = REPAIR_QUEUE_BEFORE_REPLAY_HOOK.take() {
            hook();
        }
        #[cfg(test)]
        REPAIR_QUEUE_FULL_REPLAYS.set(REPAIR_QUEUE_FULL_REPLAYS.get().saturating_add(1));
        let (snapshot, snapshot_fingerprint) =
            read_repair_queue_snapshot_with_fingerprint(&self.snapshot_path)?;
        let mut records = records_by_key(snapshot.records)?;
        let replay = replay_repair_queue_log(
            &self.active_log_path,
            snapshot.applied_through_sequence,
            &mut records,
        )?;
        #[cfg(test)]
        if let Some(hook) = REPAIR_QUEUE_AFTER_REPLAY_HOOK.take() {
            hook();
        }
        let consumed = RepairQueueDiskFingerprint {
            snapshot: snapshot_fingerprint,
            log: replay.fingerprint.clone(),
        };
        let after = self.disk_fingerprint()?;
        if consumed != after {
            return Err(DfsError::Metadata {
                message: "DFS repair queue changed while loading its snapshot/log generation"
                    .to_string(),
            });
        }
        self.snapshot_floor = snapshot.applied_through_sequence;
        self.head_sequence = replay.head_sequence;
        self.records = records;
        self.active_log_bytes = replay.file_bytes;
        self.valid_active_log_bytes = replay.valid_bytes;
        if self.resync_on_load {
            self.resync_replayed_log()?;
            self.resync_on_load = false;
            // A repaired tail is a different physical generation. Reload it
            // only after the required sync succeeded, including no-op retries.
            return self.ensure_loaded();
        }
        self.revision = revision;
        self.fingerprint = Some(consumed);
        self.loaded = true;
        Ok(())
    }

    fn append_operations(
        &mut self,
        operations: Vec<DfsRepairQueueOperation>,
    ) -> Result<(u64, usize), DfsError> {
        self.ensure_loaded()?;
        if operations.is_empty() {
            return Ok((self.head_sequence, 0));
        }

        validate_repair_queue_operations(&self.records, &operations)?;
        let revision = self.next_revision()?;
        self.repair_torn_tail_before_append()?;

        let sequence = self
            .head_sequence
            .checked_add(1)
            .ok_or_else(|| DfsError::Metadata {
                message: "DFS repair queue log sequence overflow".to_string(),
            })?;
        let batch = DfsRepairQueueLogBatch {
            sequence,
            operations,
        };
        let bytes =
            sinomo_object_serde::to_bytes(&batch, 1, LengthMode::U32).map_err(|source| {
                DfsError::Serialization {
                    message: format!("could not encode DFS repair queue batch: {source}"),
                }
            })?;
        if bytes.len() > DFS_REPAIR_QUEUE_LOG_MAX_RECORD_BYTES {
            return Err(DfsError::Metadata {
                message: format!(
                    "DFS repair queue batch is too large: {} bytes exceeds {}",
                    bytes.len(),
                    DFS_REPAIR_QUEUE_LOG_MAX_RECORD_BYTES
                ),
            });
        }

        let append_result = (|| {
            let mut append = durable_file::open_file_for_append(&self.active_log_path)
                .map_err(metadata_durable_file_error)
                .map_err(DfsError::from)?;
            durable_file::append_length_prefixed_records(
                &mut append.file,
                &self.active_log_path,
                [bytes.as_slice()],
                durable_file::DurableRecordAppendOptions::new(
                    durable_file::DurableLengthEndian::Little,
                )
                .sync(durable_file::DurableFileSync::All),
            )
            .map_err(metadata_durable_file_error)
            .map_err(DfsError::from)
        })();
        let written = match append_result {
            Ok(written) => written,
            Err(error) => {
                // A failed sync can leave either no frame, a torn frame, or a
                // complete but uncertain frame. Force the next call to replay
                // and synchronize/truncate that tail before it can acknowledge
                // a no-op retry.
                self.loaded = false;
                self.resync_on_load = true;
                return Err(error);
            }
        };

        let rewind_scan = self.maintenance_cursor.as_ref().is_some_and(|cursor| {
            batch.operations.iter().any(|op| match op {
                DfsRepairQueueOperation::EnqueueIfMissing { record }
                | DfsRepairQueueOperation::Set { record } => {
                    DfsRepairQueueKey::from_record(record) <= *cursor
                }
                DfsRepairQueueOperation::Remove { .. } => false,
            })
        });
        if let Err(error) = apply_repair_queue_operations(&mut self.records, &batch.operations) {
            // Validation above makes this unreachable unless the operation
            // semantics change. Reload from the durable frame rather than
            // retaining a cache that disagrees with disk.
            self.loaded = false;
            return Err(error);
        }
        // Advance only the expected bytes we wrote. Reading an arbitrary
        // post-append disk hash here could silently adopt an external suffix.
        if let Some(fingerprint) = self.fingerprint.as_mut() {
            fingerprint.log.append_frame(&bytes);
        }
        if rewind_scan {
            self.maintenance_revisit_pending = true;
        }
        self.revision = revision;
        self.head_sequence = sequence;
        self.active_log_bytes = self.active_log_bytes.saturating_add(written);
        self.valid_active_log_bytes = self.active_log_bytes;
        Ok((sequence, batch.operations.len()))
    }

    fn repair_torn_tail_before_append(&mut self) -> Result<(), DfsError> {
        if self.valid_active_log_bytes >= self.active_log_bytes {
            return Ok(());
        }
        durable_file::truncate_file_and_sync(
            &self.active_log_path,
            self.valid_active_log_bytes,
            durable_file::DurableFileSync::All,
        )
        .map_err(metadata_durable_file_error)
        .map_err(DfsError::from)?;
        self.active_log_bytes = self.valid_active_log_bytes;
        self.fingerprint = None;
        Ok(())
    }

    fn resync_replayed_log(&mut self) -> Result<(), DfsError> {
        #[cfg(test)]
        if REPAIR_QUEUE_FAIL_RESYNC.replace(false) {
            return Err(DfsError::Metadata {
                message: "injected repair queue resync failure".into(),
            });
        }
        if self.valid_active_log_bytes < self.active_log_bytes {
            durable_file::truncate_file_and_sync(
                &self.active_log_path,
                self.valid_active_log_bytes,
                durable_file::DurableFileSync::All,
            )
            .map_err(metadata_durable_file_error)
            .map_err(DfsError::from)?;
            self.active_log_bytes = self.valid_active_log_bytes;
            return Ok(());
        }

        let mut append = durable_file::open_file_for_append(&self.active_log_path)
            .map_err(metadata_durable_file_error)
            .map_err(DfsError::from)?;
        durable_file::append_length_prefixed_records(
            &mut append.file,
            &self.active_log_path,
            std::iter::empty::<&[u8]>(),
            durable_file::DurableRecordAppendOptions::new(
                durable_file::DurableLengthEndian::Little,
            )
            .sync(durable_file::DurableFileSync::All),
        )
        .map_err(metadata_durable_file_error)
        .map_err(DfsError::from)?;
        Ok(())
    }
}

static DFS_REPAIR_QUEUE_STORES: OnceLock<
    Mutex<BTreeMap<PathBuf, Arc<Mutex<DfsRepairQueueState>>>>,
> = OnceLock::new();

#[cfg(test)]
thread_local! {
    static REPAIR_QUEUE_BEFORE_REPLAY_HOOK: std::cell::Cell<Option<Box<dyn FnOnce()>>> = const { std::cell::Cell::new(None) };
    static REPAIR_QUEUE_AFTER_REPLAY_HOOK: std::cell::Cell<Option<Box<dyn FnOnce()>>> = const { std::cell::Cell::new(None) };
    static REPAIR_QUEUE_FAIL_RESYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static REPAIR_QUEUE_FULL_REPLAYS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static BOUNDED_STATUS_RECORD_VISITS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[inline]
fn record_bounded_status_record_visit() {
    #[cfg(test)]
    BOUNDED_STATUS_RECORD_VISITS.set(BOUNDED_STATUS_RECORD_VISITS.get().saturating_add(1));
}

/// Cloneable handle to the process-local materialized repair queue.
///
/// Handles for the same snapshot path share one sequence allocator and map, so
/// daemon foreground and maintenance paths cannot issue conflicting frames.
#[derive(Clone, Debug)]
pub struct LocalDfsRepairQueueStore {
    inner: Arc<Mutex<DfsRepairQueueState>>,
}

impl LocalDfsRepairQueueStore {
    pub fn new(snapshot_path: impl Into<PathBuf>) -> Self {
        let snapshot_path = snapshot_path.into();
        let mut stores = DFS_REPAIR_QUEUE_STORES
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let inner = stores
            .entry(snapshot_path.clone())
            .or_insert_with(|| Arc::new(Mutex::new(DfsRepairQueueState::new(snapshot_path))))
            .clone();
        Self { inner }
    }

    /// Make the next access rebuild from the durable snapshot and active log.
    ///
    /// Tests use this to force cold recovery. Production maintenance uses
    /// exact content-fingerprint refresh instead of unconditional replay.
    #[cfg(test)]
    pub(crate) fn reload_from_disk_on_next_access(&self) {
        if let Ok(mut state) = self.inner.lock() {
            state.loaded = false;
        }
    }

    pub fn view(&self) -> Result<DfsRepairQueueView, DfsError> {
        let mut state = self.lock_state()?;
        state.ensure_loaded()?;
        Ok(DfsRepairQueueView {
            head_sequence: state.head_sequence,
            records: state.records.values().cloned().collect(),
        })
    }

    /// Maintenance observes external changes through exact streamed content
    /// fingerprints while retaining the hot decoded queue when bytes match.
    pub(crate) fn view_for_maintenance(
        &self,
    ) -> Result<(DfsRepairQueueView, DfsRepairQueueObservation), DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        Ok((
            DfsRepairQueueView {
                head_sequence: state.head_sequence,
                records: state.records.values().cloned().collect(),
            },
            state.observation(),
        ))
    }

    /// Observe one bounded window without cloning unrelated queue records.
    /// Total cardinality remains available to placement eligibility.
    pub(crate) fn maintenance_page(
        &self,
        max_records: usize,
        captured_records: &[DfsRepairAttemptRecord],
        expected: Option<&DfsRepairQueuePageObservation>,
    ) -> Result<(DfsRepairQueueMaintenancePage, DfsRepairQueuePageObservation), DfsError> {
        use std::ops::Bound;
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        if let Some(expected) = expected {
            if !Arc::ptr_eq(&state.owner, &expected.queue.owner)
                || state.revision != expected.queue.revision
                || state.head_sequence != expected.queue.head_sequence
                || state.maintenance_cursor != expected.after
            {
                return Err(DfsError::Metadata {
                    message: "repair queue generation changed before same-tick enrollment".into(),
                });
            }
        }
        if max_records > 0 && state.maintenance_restart_pending {
            state.maintenance_cursor = None;
            state.maintenance_restart_pending = false;
            state.maintenance_revisit_pending = false;
        }
        let after = state.maintenance_cursor.clone();
        let lower = after.as_ref().map_or(Bound::Unbounded, Bound::Excluded);
        let mut entries = state.records.range((lower, Bound::Unbounded));
        let records = entries
            .by_ref()
            .take(max_records)
            .map(|(_, record)| record.clone())
            .collect::<Vec<_>>();
        let has_more = entries.next().is_some();
        let observed = DfsRepairQueuePageObservation {
            queue: state.observation(),
            advances_scan: max_records > 0,
            after,
            records: records.clone(),
        };
        let mut selected = records;
        let mut enrollment_records = Vec::with_capacity(captured_records.len());
        let mut enrollment_remaining = false;
        let mut seen = std::collections::BTreeSet::new();
        for captured in captured_records {
            let key = DfsRepairQueueKey::from_record(captured);
            if !seen.insert(key.clone()) {
                continue;
            }
            if let Some(existing) = state.records.get(&key) {
                validate_stable_repair_identity(existing, captured)?;
                enrollment_records.push(existing.clone());
            } else {
                enrollment_records.push(captured.clone());
                if selected.len() < max_records {
                    selected.push(captured.clone());
                } else {
                    enrollment_remaining = true;
                }
            }
        }
        let page = DfsRepairQueueMaintenancePage {
            total_record_count: state.records.len(),
            records: selected,
            enrollment_records,
            has_more: has_more || enrollment_remaining,
        };
        Ok((page, observed))
    }

    /// Commit only observed work and owner-captured additions through the
    /// existing durable queue log. Cursor movement acknowledges scanning,
    /// never effect authority; it is atomic with the successful queue delta.
    pub(crate) fn reconcile_maintenance_page(
        &self,
        expected: &DfsRepairQueuePageObservation,
        examined_records: usize,
        reconciled_before: &[DfsRepairAttemptRecord],
        desired_records: &[DfsRepairAttemptRecord],
        retired_intents: &[DfsRepairAttemptRecord],
        enqueued_records: &[DfsRepairAttemptRecord],
    ) -> Result<DfsRepairQueuePageReconcileOutcome, DfsError> {
        use std::ops::Bound;
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        if !Arc::ptr_eq(&state.owner, &expected.queue.owner)
            || state.revision != expected.queue.revision
            || state.head_sequence != expected.queue.head_sequence
            || state.maintenance_cursor != expected.after
        {
            return Ok(DfsRepairQueuePageReconcileOutcome::GenerationChanged {
                current_head_sequence: state.head_sequence,
            });
        }
        if examined_records > expected.records.len() {
            return Err(DfsError::Metadata {
                message: "repair page examined beyond its observation".into(),
            });
        }
        let mut retired = BTreeMap::new();
        for record in retired_intents {
            if let Some(previous) = retired.insert(record.intent_id.clone(), record) {
                validate_stable_repair_intent_identity(previous, record)?;
            }
        }
        let is_retired = |record: &DfsRepairAttemptRecord| -> Result<bool, DfsError> {
            let Some(retired) = retired.get(&record.intent_id) else {
                return Ok(false);
            };
            validate_stable_repair_intent_identity(retired, record)?;
            Ok(true)
        };
        let mut desired = BTreeMap::new();
        for record in desired_records {
            if !is_retired(record)? {
                desired.insert(DfsRepairQueueKey::from_record(record), record);
            }
        }
        let mut before = BTreeMap::new();
        for record in expected
            .records
            .iter()
            .take(examined_records)
            .chain(reconciled_before)
        {
            let key = DfsRepairQueueKey::from_record(record);
            if let Some(existing) = state.records.get(&key) {
                validate_stable_repair_identity(existing, record)?;
            }
            before.insert(key, record);
        }
        let mut operations = BTreeMap::new();
        // Intent retirement covers all of its indexed target keys, including
        // siblings outside this candidate window. Bind every removal to the
        // exact object/version/hash which the service authoritatively retired.
        for record in retired.values() {
            let lower = DfsRepairQueueKey {
                intent_id: record.intent_id.clone(),
                missing_replica_node_id: NodeId::new("00000000000000000000000000")
                    .expect("minimum ULID"),
            };
            for (key, existing) in state
                .records
                .range(lower..)
                .take_while(|(key, _)| key.intent_id == record.intent_id)
            {
                validate_stable_repair_intent_identity(record, existing)?;
                operations.insert(
                    key.clone(),
                    DfsRepairQueueOperation::Remove { key: key.clone() },
                );
            }
        }
        for (key, old) in &before {
            match desired.get(key) {
                Some(record) => {
                    validate_stable_repair_identity(old, record)?;
                    if state.records.get(key) != Some(*record) {
                        operations.insert(
                            key.clone(),
                            DfsRepairQueueOperation::Set {
                                record: (*record).clone(),
                            },
                        );
                    }
                }
                None if state.records.contains_key(key) => {
                    operations.insert(
                        key.clone(),
                        DfsRepairQueueOperation::Remove { key: key.clone() },
                    );
                }
                None => {}
            }
        }
        // New work created by earlier phases participates in this exact
        // checkpoint. Captured old work cannot re-enqueue a retired identity.
        let mut rewind = false;
        let next_cursor = expected
            .records
            .get(examined_records.wrapping_sub(1))
            .map(DfsRepairQueueKey::from_record)
            .or_else(|| expected.after.clone());
        for captured in enqueued_records {
            if is_retired(captured)? {
                continue;
            }
            let key = DfsRepairQueueKey::from_record(captured);
            let record = desired.get(&key).copied().unwrap_or(captured);
            validate_stable_repair_identity(captured, record)?;
            if let Some(existing) = state.records.get(&key) {
                validate_stable_repair_identity(existing, record)?;
                continue;
            }
            rewind |= next_cursor.as_ref().is_some_and(|cursor| &key <= cursor);
            operations
                .entry(key)
                .or_insert_with(|| DfsRepairQueueOperation::EnqueueIfMissing {
                    record: record.clone(),
                });
        }
        let new_work = operations.iter().any(|(key, operation)| {
            !state.records.contains_key(key)
                && !matches!(operation, DfsRepairQueueOperation::Remove { .. })
        });
        let removed_target_count = operations
            .values()
            .filter(|op| matches!(op, DfsRepairQueueOperation::Remove { .. }))
            .count();
        let (_, operation_count) = state.append_operations(operations.into_values().collect())?;
        if !expected.advances_scan {
            // A completion-only delta cannot reopen an already completed
            // parked sweep. Only newly enrolled work requests continuation.
            return Ok(DfsRepairQueuePageReconcileOutcome::Applied {
                removed_target_count,
                operation_count,
                has_more: new_work,
            });
        }
        state.maintenance_revisit_pending |= rewind;
        let lower = next_cursor
            .as_ref()
            .map_or(Bound::Unbounded, Bound::Excluded);
        let suffix_remaining = state
            .records
            .range((lower, Bound::Unbounded))
            .next()
            .is_some();
        let wrap_remaining =
            !suffix_remaining && state.maintenance_revisit_pending && !state.records.is_empty();
        let has_more = suffix_remaining || wrap_remaining;
        state.maintenance_cursor = if suffix_remaining { next_cursor } else { None };
        if !suffix_remaining {
            state.maintenance_revisit_pending = false;
        }
        Ok(DfsRepairQueuePageReconcileOutcome::Applied {
            removed_target_count,
            operation_count,
            has_more,
        })
    }

    pub(crate) fn reconcile_observed(
        &self,
        expected: &DfsRepairQueueObservation,
        desired_records: &[DfsRepairAttemptRecord],
    ) -> Result<DfsRepairQueueObservedReconcileOutcome, DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        if !Arc::ptr_eq(&state.owner, &expected.owner)
            || state.revision != expected.revision
            || state.head_sequence != expected.head_sequence
        {
            return Ok(DfsRepairQueueObservedReconcileOutcome::GenerationChanged {
                current_head_sequence: state.head_sequence,
            });
        }
        let operations = reconcile_repair_queue_operations(&state.records, desired_records)?;
        state.append_operations(operations)?;
        Ok(DfsRepairQueueObservedReconcileOutcome::Applied(
            state.observation(),
        ))
    }

    /// Read queue cardinality plus a bounded deterministic sample without
    /// loading/replaying durable state or cloning the complete queue. If the
    /// process-local generation has never been materialized, status is
    /// explicitly unavailable rather than turning an operator probe into a
    /// cold recovery pass.
    pub fn bounded_status_view(
        &self,
        max_records: usize,
    ) -> Result<DfsRepairQueueBoundedStatus, DfsError> {
        let state = match self.inner.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(DfsError::Metadata {
                    message: "bounded DFS repair queue status unavailable while the resident queue is being updated"
                        .to_string(),
                });
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(DfsError::Metadata {
                    message: "bounded DFS repair queue status lock is poisoned".to_string(),
                });
            }
        };
        if !state.loaded {
            return Ok(DfsRepairQueueBoundedStatus::NotHydrated);
        }
        let records = state
            .records
            .values()
            .take(max_records)
            .inspect(|_| record_bounded_status_record_visit())
            .cloned()
            .collect();
        Ok(DfsRepairQueueBoundedStatus::Hydrated(
            DfsRepairQueueStatusView {
                head_sequence: state.head_sequence,
                record_count: state.records.len(),
                records,
            },
        ))
    }

    pub fn append_enqueues(
        &self,
        records: impl IntoIterator<Item = DfsRepairAttemptRecord>,
    ) -> Result<usize, DfsError> {
        let mut state = self.lock_state()?;
        state.ensure_loaded()?;
        let mut operations = Vec::new();
        let mut queued = BTreeMap::new();
        for record in records {
            let key = DfsRepairQueueKey::from_record(&record);
            if let Some(existing) = queued.get(&key) {
                validate_stable_repair_identity(existing, &record)?;
                continue;
            }
            queued.insert(key.clone(), record.clone());
            if let Some(existing) = state.records.get(&key) {
                validate_stable_repair_identity(existing, &record)?;
                continue;
            }
            operations.push(DfsRepairQueueOperation::EnqueueIfMissing { record });
        }
        let (_, operation_count) = state.append_operations(operations)?;
        Ok(operation_count)
    }

    pub fn append_operations(
        &self,
        operations: Vec<DfsRepairQueueOperation>,
    ) -> Result<usize, DfsError> {
        let mut state = self.lock_state()?;
        let (_, operation_count) = state.append_operations(operations)?;
        Ok(operation_count)
    }

    pub fn reconcile(
        &self,
        expected_head_sequence: u64,
        desired_records: &[DfsRepairAttemptRecord],
    ) -> Result<DfsRepairQueueReconcileOutcome, DfsError> {
        let mut state = self.lock_state()?;
        state.ensure_loaded()?;
        if state.head_sequence != expected_head_sequence {
            return Ok(DfsRepairQueueReconcileOutcome::GenerationChanged {
                current_head_sequence: state.head_sequence,
            });
        }
        let operations = reconcile_repair_queue_operations(&state.records, desired_records)?;
        let (head_sequence, operation_count) = state.append_operations(operations)?;
        Ok(DfsRepairQueueReconcileOutcome::Applied {
            head_sequence,
            operation_count,
        })
    }

    /// Atomically replace the materialized queue through the same sequenced
    /// log used by foreground deltas. This is retained for compatibility with
    /// callers that previously rewrote `DfsRepairAttemptSnapshot` directly.
    pub fn replace_records(
        &self,
        desired_records: &[DfsRepairAttemptRecord],
    ) -> Result<usize, DfsError> {
        let mut state = self.lock_state()?;
        state.ensure_loaded()?;
        let operations = reconcile_repair_queue_operations(&state.records, desired_records)?;
        let (_, operation_count) = state.append_operations(operations)?;
        Ok(operation_count)
    }

    pub fn compact_if_needed(
        &self,
        options: DfsRepairQueueCompactionOptions,
    ) -> Result<DfsRepairQueueCompactionReport, DfsError> {
        let mut state = self.lock_state()?;
        state.ensure_loaded()?;
        let log_bytes_before = state.active_log_bytes;
        if state.head_sequence == 0 || (!options.force && log_bytes_before < options.max_log_bytes)
        {
            return Ok(DfsRepairQueueCompactionReport {
                compacted: false,
                applied_through_sequence: state.snapshot_floor,
                retained_record_count: state.records.len(),
                log_bytes_before,
                log_bytes_after: log_bytes_before,
            });
        }

        let snapshot = DfsRepairQueueSnapshotFile {
            records: state.records.values().cloned().collect(),
            applied_through_sequence: state.head_sequence,
        };
        let snapshot_bytes = encode_sobj(&snapshot)?;
        if let Err(error) = write_file_atomic(&state.snapshot_path, &snapshot_bytes) {
            state.loaded = false;
            return Err(error.into());
        }
        let log_present = state.active_log_path.exists();
        if log_present
            && let Err(error) = durable_file::truncate_file_and_sync(
                &state.active_log_path,
                0,
                durable_file::DurableFileSync::All,
            )
        {
            // The published snapshot floor makes the retained old log safe to
            // replay. Reload it on retry instead of trusting cached lengths.
            state.loaded = false;
            return Err(DfsError::from(metadata_durable_file_error(error)));
        }
        // Owned compaction preserves the exact logical queue, so existing
        // maintenance observations remain valid across this physical rewrite.
        state.fingerprint = Some(RepairQueueDiskFingerprint {
            snapshot: RepairQueueFileFingerprint::from_bytes(&snapshot_bytes),
            log: if log_present {
                RepairQueueFileFingerprint::from_bytes(&[])
            } else {
                RepairQueueFileFingerprint::absent()
            },
        });
        state.snapshot_floor = state.head_sequence;
        state.active_log_bytes = 0;
        state.valid_active_log_bytes = 0;
        Ok(DfsRepairQueueCompactionReport {
            compacted: true,
            applied_through_sequence: state.head_sequence,
            retained_record_count: state.records.len(),
            log_bytes_before,
            log_bytes_after: 0,
        })
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, DfsRepairQueueState>, DfsError> {
        self.inner.lock().map_err(|_| DfsError::Metadata {
            message: "DFS repair queue store lock poisoned".to_string(),
        })
    }

    #[cfg(test)]
    fn evict_for_test(snapshot_path: &Path) {
        if let Some(stores) = DFS_REPAIR_QUEUE_STORES.get() {
            stores
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(snapshot_path);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DfsRepairQueueReplayReport {
    head_sequence: u64,
    file_bytes: u64,
    valid_bytes: u64,
    fingerprint: RepairQueueFileFingerprint,
}

fn replay_repair_queue_log(
    path: &Path,
    snapshot_floor: u64,
    records: &mut BTreeMap<DfsRepairQueueKey, DfsRepairAttemptRecord>,
) -> Result<DfsRepairQueueReplayReport, DfsError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DfsRepairQueueReplayReport {
                head_sequence: snapshot_floor,
                file_bytes: 0,
                valid_bytes: 0,
                fingerprint: RepairQueueFileFingerprint::absent(),
            });
        }
        Err(source) => {
            return Err(DfsError::Metadata {
                message: format!(
                    "could not open DFS repair queue log {}: {source}",
                    path.display()
                ),
            });
        }
    };
    let file_bytes = file
        .metadata()
        .map_err(|source| DfsError::Metadata {
            message: format!(
                "could not stat DFS repair queue log {}: {source}",
                path.display()
            ),
        })?
        .len();
    let mut fingerprint = RepairQueueFileFingerprint::from_bytes(&[]);
    let mut head_sequence = snapshot_floor;
    let mut expected_sequence = snapshot_floor.saturating_add(1);
    let valid_bytes = loop {
        let record_start = file
            .stream_position()
            .map_err(|source| DfsError::Metadata {
                message: format!(
                    "could not seek DFS repair queue log {}: {source}",
                    path.display()
                ),
            })?;
        let Some(record) = durable_file::read_length_prefixed_record(
            &mut file,
            path,
            durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Little)
                .max_record_bytes(DFS_REPAIR_QUEUE_LOG_MAX_RECORD_BYTES),
        )
        .map_err(metadata_durable_file_error)
        .map_err(DfsError::from)?
        else {
            // The shared framing reader rewinds to the incomplete record's
            // start. Hash its remaining physical bytes too: they do not apply
            // queue operations, but they belong to this exact generation.
            let mut buffer = [0_u8; 64 * 1024];
            while fingerprint.bytes < file_bytes {
                let limit = (file_bytes - fingerprint.bytes).min(buffer.len() as u64) as usize;
                file.read_exact(&mut buffer[..limit])
                    .map_err(|error| fingerprint_error(path, error))?;
                fingerprint.hasher.update(&buffer[..limit]);
                fingerprint.bytes += limit as u64;
            }
            break record_start;
        };
        if record.record_start != fingerprint.bytes || record.record_end > file_bytes {
            return Err(fingerprint_error(path, "log changed during framed replay"));
        }
        // U32 little-endian framing has one exact representation. Hash that
        // prefix and the very payload buffer decoded below, including frames
        // covered by the snapshot floor. A later re-open cannot stand in for
        // these consumed bytes (A -> B -> A would otherwise adopt B under A).
        fingerprint.append_frame(&record.bytes);
        let batch: DfsRepairQueueLogBatch = sinomo_object_serde::from_bytes(&record.bytes)
            .map_err(|source| DfsError::Serialization {
                message: format!(
                    "could not decode DFS repair queue log {} at byte {}: {source}",
                    path.display(),
                    record.record_start
                ),
            })?;
        if batch.sequence <= snapshot_floor {
            continue;
        }
        if batch.sequence != expected_sequence {
            return Err(DfsError::Metadata {
                message: format!(
                    "DFS repair queue log {} sequence gap: expected {}, found {}",
                    path.display(),
                    expected_sequence,
                    batch.sequence
                ),
            });
        }
        apply_repair_queue_operations(records, &batch.operations)?;
        head_sequence = batch.sequence;
        expected_sequence = batch
            .sequence
            .checked_add(1)
            .ok_or_else(|| DfsError::Metadata {
                message: "DFS repair queue log sequence overflow".to_string(),
            })?;
    };
    Ok(DfsRepairQueueReplayReport {
        head_sequence,
        file_bytes,
        valid_bytes,
        fingerprint,
    })
}

fn apply_repair_queue_operations(
    records: &mut BTreeMap<DfsRepairQueueKey, DfsRepairAttemptRecord>,
    operations: &[DfsRepairQueueOperation],
) -> Result<(), DfsError> {
    for operation in operations {
        match operation {
            DfsRepairQueueOperation::EnqueueIfMissing { record } => {
                let key = DfsRepairQueueKey::from_record(record);
                if let Some(existing) = records.get(&key) {
                    validate_stable_repair_identity(existing, record)?;
                } else {
                    records.insert(key, record.clone());
                }
            }
            DfsRepairQueueOperation::Set { record } => {
                let key = DfsRepairQueueKey::from_record(record);
                if let Some(existing) = records.get(&key) {
                    validate_stable_repair_identity(existing, record)?;
                }
                records.insert(key, record.clone());
            }
            DfsRepairQueueOperation::Remove { key } => {
                records.remove(key);
            }
        }
    }
    Ok(())
}

/// Validate a batch without cloning the entire accumulated queue. Foreground
/// writes normally touch only one or two repair identities, so this keeps
/// append cost proportional to the delta instead of the queue size.
fn validate_repair_queue_operations(
    records: &BTreeMap<DfsRepairQueueKey, DfsRepairAttemptRecord>,
    operations: &[DfsRepairQueueOperation],
) -> Result<(), DfsError> {
    let mut touched: BTreeMap<DfsRepairQueueKey, Option<DfsRepairAttemptRecord>> = BTreeMap::new();
    for operation in operations {
        let (key, candidate, removes) = match operation {
            DfsRepairQueueOperation::EnqueueIfMissing { record } => {
                (DfsRepairQueueKey::from_record(record), Some(record), false)
            }
            DfsRepairQueueOperation::Set { record } => {
                (DfsRepairQueueKey::from_record(record), Some(record), false)
            }
            DfsRepairQueueOperation::Remove { key } => (key.clone(), None, true),
        };
        let current = touched
            .entry(key.clone())
            .or_insert_with(|| records.get(&key).cloned());
        if removes {
            *current = None;
            continue;
        }
        let candidate = candidate.expect("non-remove repair queue operation has a record");
        if let Some(existing) = current.as_ref() {
            validate_stable_repair_identity(existing, candidate)?;
        }
        match operation {
            DfsRepairQueueOperation::EnqueueIfMissing { .. } if current.is_some() => {}
            _ => *current = Some(candidate.clone()),
        }
    }
    Ok(())
}

fn validate_stable_repair_intent_identity(
    expected: &DfsRepairAttemptRecord,
    actual: &DfsRepairAttemptRecord,
) -> Result<(), DfsError> {
    if expected.intent_id != actual.intent_id
        || expected.object_id != actual.object_id
        || expected.version != actual.version
        || expected.content_hash != actual.content_hash
    {
        return Err(DfsError::Metadata {
            message: "repair retirement identity changed".into(),
        });
    }
    Ok(())
}

fn validate_stable_repair_identity(
    existing: &DfsRepairAttemptRecord,
    candidate: &DfsRepairAttemptRecord,
) -> Result<(), DfsError> {
    if existing.object_id == candidate.object_id
        && existing.version == candidate.version
        && existing.content_hash == candidate.content_hash
    {
        return Ok(());
    }
    Err(DfsError::Metadata {
        message: format!(
            "DFS repair queue identity {} for target {} changed object, version, or content hash",
            existing.intent_id, existing.missing_replica_node_id
        ),
    })
}

fn reconcile_repair_queue_operations(
    current: &BTreeMap<DfsRepairQueueKey, DfsRepairAttemptRecord>,
    desired_records: &[DfsRepairAttemptRecord],
) -> Result<Vec<DfsRepairQueueOperation>, DfsError> {
    let desired = records_by_key(desired_records.to_vec())?;
    let mut operations = Vec::new();
    for (key, record) in &desired {
        match current.get(key) {
            Some(existing) if existing == record => {}
            Some(existing) => {
                validate_stable_repair_identity(existing, record)?;
                operations.push(DfsRepairQueueOperation::Set {
                    record: record.clone(),
                });
            }
            None => operations.push(DfsRepairQueueOperation::Set {
                record: record.clone(),
            }),
        }
    }
    for key in current.keys() {
        if !desired.contains_key(key) {
            operations.push(DfsRepairQueueOperation::Remove { key: key.clone() });
        }
    }
    Ok(operations)
}

fn records_by_key(
    records: Vec<DfsRepairAttemptRecord>,
) -> Result<BTreeMap<DfsRepairQueueKey, DfsRepairAttemptRecord>, DfsError> {
    let mut indexed = BTreeMap::new();
    for record in records {
        let key = DfsRepairQueueKey::from_record(&record);
        if let Some(existing) = indexed.get(&key) {
            validate_stable_repair_identity(existing, &record)?;
        }
        indexed.insert(key, record);
    }
    Ok(indexed)
}

#[cfg(test)]
fn read_repair_queue_snapshot(path: &Path) -> Result<DfsRepairQueueSnapshotFile, DfsError> {
    read_repair_queue_snapshot_with_fingerprint(path).map(|(snapshot, _)| snapshot)
}

fn read_repair_queue_snapshot_with_fingerprint(
    path: &Path,
) -> Result<(DfsRepairQueueSnapshotFile, RepairQueueFileFingerprint), DfsError> {
    match fs::read(path) {
        Ok(bytes) => {
            let snapshot = sinomo_object_serde::from_bytes(&bytes).map_err(|source| {
                DfsError::Serialization {
                    message: format!(
                        "DFS repair queue snapshot object error at {}: {source}",
                        path.display()
                    ),
                }
            })?;
            Ok((snapshot, RepairQueueFileFingerprint::from_bytes(&bytes)))
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok((
            DfsRepairQueueSnapshotFile::default(),
            RepairQueueFileFingerprint::absent(),
        )),
        Err(source) => Err(DfsError::Metadata {
            message: format!(
                "could not read DFS repair queue snapshot {}: {source}",
                path.display()
            ),
        }),
    }
}

fn repair_queue_active_log_path(snapshot_path: &Path) -> PathBuf {
    let mut file_name = snapshot_path
        .file_stem()
        .unwrap_or_else(|| std::ffi::OsStr::new("repair-attempts"))
        .to_os_string();
    file_name.push(".active.slog");
    snapshot_path.with_file_name(file_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maintenance::DfsRepairAttemptSnapshot;
    use crate::{DfsObjectId, DfsVersion};
    use std::fs::OpenOptions;
    use std::io::Write as _;

    fn record(name: &str, target: &str, attempts: u32) -> DfsRepairAttemptRecord {
        DfsRepairAttemptRecord {
            intent_id: format!("intent-{name}"),
            object_id: DfsObjectId::from_legacy_named(format!("object-{name}")).expect("object"),
            version: DfsVersion::from_legacy_named(format!("version-{name}")).expect("version"),
            content_hash: format!("blake3:{name}"),
            missing_replica_node_id: NodeId::from_legacy_named(target).expect("target"),
            attempts,
            in_flight: false,
            last_attempt_unix_nanos: u64::from(attempts),
            next_retry_unix_nanos: u64::from(attempts).saturating_add(10),
            transfer_request_id: None,
            last_error: (attempts > 0).then(|| "retry".to_string()),
        }
    }

    fn reopen(snapshot_path: &Path) -> LocalDfsRepairQueueStore {
        LocalDfsRepairQueueStore::evict_for_test(snapshot_path);
        LocalDfsRepairQueueStore::new(snapshot_path)
    }

    fn append_external_batch(path: &Path, sequence: u64, records: Vec<DfsRepairAttemptRecord>) {
        let bytes = encode_sobj(&DfsRepairQueueLogBatch {
            sequence,
            operations: records
                .into_iter()
                .map(|record| DfsRepairQueueOperation::EnqueueIfMissing { record })
                .collect(),
        })
        .unwrap();
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        durable_file::append_length_prefixed_records(
            &mut file,
            path,
            [bytes.as_slice()],
            durable_file::DurableRecordAppendOptions::new(
                durable_file::DurableLengthEndian::Little,
            )
            .sync(durable_file::DurableFileSync::All),
        )
        .unwrap();
    }

    #[test]
    fn repair_queue_page_rejection_preserves_records_and_next_observable_page() {
        let root = sinomo_testdir::TestDir::new("repair-page-atomic-rejection");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let alpha = record("alpha", "node-alpha", 3);
        let beta = record("beta", "node-beta", 4);
        let gamma = record("gamma", "node-gamma", 5);
        store
            .append_enqueues([alpha.clone(), beta.clone(), gamma.clone()])
            .unwrap();
        let (first, first_token) = store.maintenance_page(1, &[], None).unwrap();
        assert_eq!(first.records, vec![alpha.clone()]);
        assert!(matches!(
            store
                .reconcile_maintenance_page(&first_token, 1, &[], &first.records, &[], &[])
                .unwrap(),
            DfsRepairQueuePageReconcileOutcome::Applied {
                has_more: true,
                operation_count: 0,
                ..
            }
        ));
        let (page, stale_token) = store.maintenance_page(1, &[], None).unwrap();
        assert_eq!(page.records, vec![beta.clone()]);
        let delta = record("delta", "node-delta", 0);
        LocalDfsRepairQueueStore::new(&path)
            .append_enqueues([delta.clone()])
            .unwrap();
        let exact = store.view().unwrap();
        let unwanted = record("unwanted", "node-unwanted", 0);
        assert!(matches!(
            store
                .reconcile_maintenance_page(
                    &stale_token,
                    1,
                    &page.records,
                    &[],
                    &[beta.clone()],
                    &[unwanted.clone()]
                )
                .unwrap(),
            DfsRepairQueuePageReconcileOutcome::GenerationChanged { .. }
        ));
        assert_eq!(
            store.view().unwrap(),
            exact,
            "stale observation applies neither removal nor enqueue"
        );
        let (same, token) = store.maintenance_page(1, &[], None).unwrap();
        assert_eq!(
            same.records, page.records,
            "rejected reconciliation cannot advance past beta"
        );
        let mut incompatible = beta.clone();
        incompatible.version = DfsVersion::from_legacy_named("different-page-version").unwrap();
        assert!(
            store
                .reconcile_maintenance_page(
                    &token,
                    1,
                    &same.records,
                    &[incompatible],
                    &[alpha.clone()],
                    &[unwanted]
                )
                .is_err()
        );
        assert_eq!(
            store.view().unwrap(),
            exact,
            "a later identity error cannot partially apply the earlier retirement"
        );
        assert_eq!(
            store.maintenance_page(1, &[], None).unwrap().0.records,
            same.records
        );
        // The failed operation neither consumed nor forged the genuine token.
        assert!(matches!(
            store
                .reconcile_maintenance_page(&token, 1, &same.records, &same.records, &[], &[])
                .unwrap(),
            DfsRepairQueuePageReconcileOutcome::Applied {
                has_more: true,
                operation_count: 0,
                ..
            }
        ));
        assert_eq!(
            LocalDfsRepairQueueStore::new(&path)
                .maintenance_page(1, &[], None)
                .unwrap()
                .0
                .records,
            vec![delta]
        );
        assert_eq!(store.view().unwrap(), exact);
    }

    #[test]
    fn repair_queue_page_token_cannot_mutate_a_reopened_owner() {
        let root = sinomo_testdir::TestDir::new("repair-page-owner-identity");
        let path = root.join("repair.sobj");
        let original = LocalDfsRepairQueueStore::new(&path);
        let alpha = record("alpha", "node-alpha", 2);
        let beta = record("beta", "node-beta", 3);
        original
            .append_enqueues([alpha.clone(), beta.clone()])
            .unwrap();
        let (page, token) = original.maintenance_page(1, &[], None).unwrap();
        let reopened = reopen(&path);
        let exact = reopened.view().unwrap();
        assert!(matches!(
            reopened
                .reconcile_maintenance_page(
                    &token,
                    1,
                    &page.records,
                    &[],
                    &[alpha.clone()],
                    &[record("extra", "node-extra", 0)]
                )
                .unwrap(),
            DfsRepairQueuePageReconcileOutcome::GenerationChanged { .. }
        ));
        assert_eq!(reopened.view().unwrap(), exact);
        assert_eq!(
            reopened.maintenance_page(1, &[], None).unwrap().0.records,
            vec![alpha]
        );
    }

    #[test]
    fn repair_queue_completion_only_delta_does_not_reopen_finished_parked_sweep() {
        let root = sinomo_testdir::TestDir::new("repair-page-parked-followup");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let mut parked = vec![
            record("alpha", "node-alpha", 3),
            record("beta", "node-beta", 4),
        ];
        for record in &mut parked {
            record.next_retry_unix_nanos = u64::MAX - 1;
        }
        store.append_enqueues(parked.clone()).unwrap();
        let (page, token) = store.maintenance_page(2, &[], None).unwrap();
        assert_eq!(page.total_record_count, 2);
        assert!(matches!(
            store
                .reconcile_maintenance_page(&token, 2, &page.records, &page.records, &[], &[])
                .unwrap(),
            DfsRepairQueuePageReconcileOutcome::Applied {
                has_more: false,
                operation_count: 0,
                ..
            }
        ));
        let exact = store.view().unwrap();
        for _ in 0..3 {
            let (delta, observed) = LocalDfsRepairQueueStore::new(&path)
                .maintenance_page(0, &parked, None)
                .unwrap();
            assert!(delta.records.is_empty());
            assert_eq!(
                delta.total_record_count, 2,
                "empty delta window cannot erase total eligibility count"
            );
            assert!(
                matches!(
                    store
                        .reconcile_maintenance_page(
                            &observed,
                            0,
                            &parked,
                            &parked,
                            &[],
                            &delta.enrollment_records
                        )
                        .unwrap(),
                    DfsRepairQueuePageReconcileOutcome::Applied {
                        has_more: false,
                        operation_count: 0,
                        ..
                    }
                ),
                "unrelated completion with no delta must not manufacture another sweep"
            );
            assert_eq!(store.view().unwrap(), exact);
        }
        let ready = record("new-ready", "node-ready", 0);
        let (delta, observed) = store
            .maintenance_page(0, std::slice::from_ref(&ready), None)
            .unwrap();
        assert!(
            matches!(
                store
                    .reconcile_maintenance_page(
                        &observed,
                        0,
                        &[],
                        &[],
                        &[],
                        &delta.enrollment_records
                    )
                    .unwrap(),
                DfsRepairQueuePageReconcileOutcome::Applied {
                    has_more: true,
                    operation_count: 1,
                    ..
                }
            ),
            "a genuinely new owned repair retains immediate continuation"
        );
        assert!(store.view().unwrap().records.contains(&ready));
    }

    #[test]
    fn repair_queue_threshold_compaction_preserves_page_token_suffix_and_revisit() {
        use crate::maintenance::{
            DfsMaintenanceHost, LocalDfsMaintenanceHost, default_dfs_repair_attempt_snapshot_path,
        };
        let root = sinomo_testdir::TestDir::new("repair-page-threshold-cursor");
        let data_dir = sinomo_core::local::SinomoDataDir::new(root.path());
        let path = default_dfs_repair_attempt_snapshot_path(&data_dir);
        let log = repair_queue_active_log_path(&path);
        let store = LocalDfsRepairQueueStore::new(&path);
        // Real framed bytes cross the existing production threshold. No forced
        // compaction and no private cursor mutation substitutes for the owner.
        let records = (0..8)
            .map(|index| {
                let mut record = record(&format!("b{index}"), "node-target", 3);
                record.last_error = Some("x".repeat(600_000));
                record
            })
            .collect::<Vec<_>>();
        store.append_enqueues(records.clone()).unwrap();
        assert!(fs::metadata(&log).unwrap().len() >= DEFAULT_DFS_REPAIR_QUEUE_COMPACTION_LOG_BYTES);
        let mut host = LocalDfsMaintenanceHost::new(data_dir.clone());
        let page = host.load_repair_maintenance_page(1).unwrap();
        assert_eq!(page.records, records[..1]);
        let keys = std::collections::BTreeSet::from([(
            records[0].intent_id.clone(),
            records[0].missing_replica_node_id.clone(),
        )]);
        let checkpoint = host
            .checkpoint_repair_maintenance_page(&page, &keys, &[], &page.records)
            .unwrap();
        assert!(checkpoint.has_more);
        assert_eq!(checkpoint.removed_target_count, 1);
        assert_eq!(
            fs::metadata(&log).unwrap().len(),
            0,
            "normal page checkpoint retains threshold-based log compaction"
        );
        assert_eq!(store.view().unwrap().records, records[1..]);
        let early = record("a0", "node-earlier", 0);
        store.append_enqueues([early.clone()]).unwrap();
        let (next, next_token) = store.maintenance_page(1, &[], None).unwrap();
        assert_eq!(
            next.records,
            records[1..2],
            "earlier arrivals do not restart the old suffix"
        );
        let exact = store.view().unwrap();
        let bytes = fs::metadata(&log).unwrap().len();
        assert!(
            !store
                .compact_if_needed(DfsRepairQueueCompactionOptions {
                    max_log_bytes: bytes + 1,
                    force: false
                })
                .unwrap()
                .compacted
        );
        assert!(
            store
                .compact_if_needed(DfsRepairQueueCompactionOptions {
                    max_log_bytes: bytes,
                    force: false
                })
                .unwrap()
                .compacted
        );
        assert_eq!(
            store.view().unwrap(),
            exact,
            "physical rewrite preserves exact logical queue/head"
        );
        let same_owner = LocalDfsRepairQueueStore::new(&path);
        assert_eq!(
            same_owner.maintenance_page(1, &[], None).unwrap().0.records,
            next.records
        );
        assert!(
            matches!(
                same_owner
                    .reconcile_maintenance_page(
                        &next_token,
                        1,
                        &next.records,
                        &[],
                        &next.records,
                        &[]
                    )
                    .unwrap(),
                DfsRepairQueuePageReconcileOutcome::Applied { has_more: true, .. }
            ),
            "the real pre-compaction observation remains valid"
        );
        for expected in &records[2..] {
            let fresh_host_handle = LocalDfsRepairQueueStore::new(&path);
            let (page, observed) = fresh_host_handle.maintenance_page(1, &[], None).unwrap();
            assert_eq!(page.records, vec![expected.clone()]);
            assert!(matches!(
                fresh_host_handle
                    .reconcile_maintenance_page(
                        &observed,
                        1,
                        &page.records,
                        &[],
                        &page.records,
                        &[]
                    )
                    .unwrap(),
                DfsRepairQueuePageReconcileOutcome::Applied { has_more: true, .. }
            ));
        }
        assert_eq!(
            store.maintenance_page(1, &[], None).unwrap().0.records,
            vec![early.clone()],
            "the earlier arrival is revisited after the old tail completes"
        );
        let logical_remaining = store.view().unwrap();
        let cold = reopen(&path);
        assert_eq!(
            cold.view().unwrap(),
            logical_remaining,
            "cold recovery preserves completed removals and remaining work"
        );
        let (last, observed) = cold.maintenance_page(1, &[], None).unwrap();
        assert_eq!(last.records, vec![early]);
        assert!(matches!(
            cold.reconcile_maintenance_page(&observed, 1, &last.records, &[], &last.records, &[])
                .unwrap(),
            DfsRepairQueuePageReconcileOutcome::Applied {
                has_more: false,
                ..
            }
        ));
        assert!(reopen(&path).view().unwrap().records.is_empty());
    }

    #[test]
    fn repair_queue_maintenance_hosts_reuse_exact_fingerprint_and_preserve_backoff() {
        use crate::maintenance::{
            DfsMaintenanceHost, LocalDfsMaintenanceHost, default_dfs_repair_attempt_snapshot_path,
        };
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-hosts");
        let data_dir = sinomo_core::local::SinomoDataDir::new(root.path());
        let path = default_dfs_repair_attempt_snapshot_path(&data_dir);
        let store = LocalDfsRepairQueueStore::new(&path);
        let queued = record("alpha", "node-alpha", 3);
        store.append_enqueues([queued.clone()]).unwrap();
        REPAIR_QUEUE_FULL_REPLAYS.set(0);
        for _ in 0..4 {
            let mut host = LocalDfsMaintenanceHost::new(data_dir.clone());
            let records = host.load_repair_attempt_records().unwrap();
            assert_eq!(records, vec![queued.clone()]);
            host.store_repair_attempt_records(&records).unwrap();
        }
        assert_eq!(
            REPAIR_QUEUE_FULL_REPLAYS.get(),
            0,
            "unchanged maintenance generations must not decode historical batches"
        );
        let other = LocalDfsRepairQueueStore::new(&path);
        other
            .append_enqueues([record("beta", "node-beta", 0)])
            .unwrap();
        let mut host = LocalDfsMaintenanceHost::new(data_dir);
        let records = host.load_repair_attempt_records().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], queued);
        assert_eq!(
            REPAIR_QUEUE_FULL_REPLAYS.get(),
            0,
            "owned append must extend the expected fingerprint without replay"
        );
    }

    #[test]
    fn repair_queue_refresh_observes_external_append_and_rejects_stale_token() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-append");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let alpha = record("alpha", "node-alpha", 3);
        let beta = record("beta", "node-beta", 1);
        store.append_enqueues([alpha.clone()]).unwrap();
        let (_, observed) = store.view_for_maintenance().unwrap();
        append_external_batch(&repair_queue_active_log_path(&path), 2, vec![beta.clone()]);
        assert!(matches!(
            store.reconcile_observed(&observed, &[]).unwrap(),
            DfsRepairQueueObservedReconcileOutcome::GenerationChanged {
                current_head_sequence: 2
            }
        ));
        assert_eq!(
            store.view_for_maintenance().unwrap().0.records,
            vec![alpha, beta]
        );
    }

    #[test]
    fn repair_queue_refresh_same_size_same_head_snapshot_replacement_preserves_retry() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-same-head");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let queued = record("alpha", "node-alpha", 3);
        store.append_enqueues([queued.clone()]).unwrap();
        store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let (_, observed) = store.view_for_maintenance().unwrap();
        let before = fs::read(&path).unwrap();
        let mut snapshot = read_repair_queue_snapshot(&path).unwrap();
        snapshot.records[0].last_error = Some("later".into());
        let replacement = encode_sobj(&snapshot).unwrap();
        assert_eq!(before.len(), replacement.len());
        assert_ne!(before, replacement);
        write_file_atomic(&path, &replacement).unwrap();
        assert!(matches!(
            store.reconcile_observed(&observed, &[]).unwrap(),
            DfsRepairQueueObservedReconcileOutcome::GenerationChanged {
                current_head_sequence: 1
            }
        ));
        assert_eq!(
            store.view_for_maintenance().unwrap().0.records,
            snapshot.records
        );
    }

    #[test]
    fn repair_queue_owned_append_does_not_adopt_external_prefix_bytes() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-owned-append");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let mut alpha = record("alpha", "node-alpha", 3);
        store.append_enqueues([alpha.clone()]).unwrap();
        alpha.last_error = Some("later".into());
        let bytes = encode_sobj(&DfsRepairQueueLogBatch {
            sequence: 1,
            operations: vec![DfsRepairQueueOperation::EnqueueIfMissing {
                record: alpha.clone(),
            }],
        })
        .unwrap();
        let mut framed = (bytes.len() as u32).to_le_bytes().to_vec();
        framed.extend(bytes);
        let log = repair_queue_active_log_path(&path);
        assert_eq!(fs::metadata(&log).unwrap().len(), framed.len() as u64);
        write_file_atomic(&log, &framed).unwrap();
        let beta = record("beta", "node-beta", 0);
        store.append_enqueues([beta.clone()]).unwrap();
        REPAIR_QUEUE_FULL_REPLAYS.set(0);
        assert_eq!(
            store.view_for_maintenance().unwrap().0.records,
            vec![alpha, beta]
        );
        assert_eq!(
            REPAIR_QUEUE_FULL_REPLAYS.get(),
            1,
            "own append must hash its known prefix, not silently adopt a changed earlier frame"
        );
    }

    #[test]
    fn repair_queue_refresh_torn_suffix_and_compaction_keep_pending_records() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-torn");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let alpha = record("alpha", "node-alpha", 3);
        store.append_enqueues([alpha.clone()]).unwrap();
        let log = repair_queue_active_log_path(&path);
        OpenOptions::new()
            .append(true)
            .open(&log)
            .unwrap()
            .write_all(&[0xff, 0x01])
            .unwrap();
        assert_eq!(
            store.view_for_maintenance().unwrap().0.records,
            vec![alpha.clone()]
        );
        let beta = record("beta", "node-beta", 0);
        store.append_enqueues([beta.clone()]).unwrap();
        let (view, token) = store.view_for_maintenance().unwrap();
        assert_eq!(view.records, vec![alpha, beta]);
        store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        REPAIR_QUEUE_FULL_REPLAYS.set(0);
        assert!(matches!(
            store.reconcile_observed(&token, &view.records).unwrap(),
            DfsRepairQueueObservedReconcileOutcome::Applied(_)
        ));
        assert_eq!(
            REPAIR_QUEUE_FULL_REPLAYS.get(),
            0,
            "owned compaction publishes its exact expected bytes without a replay"
        );
        assert_eq!(
            reopen(&path).view_for_maintenance().unwrap().0.records,
            view.records
        );
    }

    #[test]
    fn repair_queue_refresh_corruption_cannot_reuse_resident_authority() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-corrupt");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        store
            .append_enqueues([record("alpha", "node-alpha", 1)])
            .unwrap();
        let log = repair_queue_active_log_path(&path);
        let mut bytes = fs::read(&log).unwrap();
        bytes[4..].fill(0xff);
        write_file_atomic(&log, &bytes).unwrap();
        assert!(store.view_for_maintenance().is_err());
        assert!(matches!(
            store.bounded_status_view(1).unwrap(),
            DfsRepairQueueBoundedStatus::NotHydrated
        ));
        assert!(
            store
                .append_enqueues([record("alpha", "node-alpha", 0)])
                .is_err()
        );
    }

    #[test]
    fn repair_queue_observation_cannot_authorize_another_store_or_new_resident_owner() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-token-owner");
        let path = root.join("repair.sobj");
        let first = LocalDfsRepairQueueStore::new(&path);
        first
            .append_enqueues([record("alpha", "node-alpha", 1)])
            .unwrap();
        let (_, token) = first.view_for_maintenance().unwrap();
        let other = LocalDfsRepairQueueStore::new(root.join("other.sobj"));
        other
            .append_enqueues([record("alpha", "node-alpha", 1)])
            .unwrap();
        assert!(matches!(
            other.reconcile_observed(&token, &[]).unwrap(),
            DfsRepairQueueObservedReconcileOutcome::GenerationChanged { .. }
        ));
        let reopened = reopen(&path);
        assert!(matches!(
            reopened.reconcile_observed(&token, &[]).unwrap(),
            DfsRepairQueueObservedReconcileOutcome::GenerationChanged { .. }
        ));
        assert_eq!(reopened.view().unwrap().records.len(), 1);
    }

    #[test]
    fn repair_queue_refresh_rejects_aba_substitution_of_decoded_snapshot_or_log() {
        for snapshot_case in [false, true] {
            let root = sinomo_testdir::TestDir::new(if snapshot_case {
                "dfs-repair-refresh-snapshot-aba"
            } else {
                "dfs-repair-refresh-log-aba"
            });
            let path = root.join("repair.sobj");
            let store = LocalDfsRepairQueueStore::new(&path);
            let original = record("alpha", "node-alpha", 3);
            store.append_enqueues([original.clone()]).unwrap();
            if snapshot_case {
                store
                    .compact_if_needed(DfsRepairQueueCompactionOptions::force())
                    .unwrap();
            }
            let changed_path = if snapshot_case {
                path.clone()
            } else {
                repair_queue_active_log_path(&path)
            };
            let original_bytes = fs::read(&changed_path).unwrap();
            let mut substituted = original.clone();
            substituted.last_error = Some("later".into());
            let substituted_bytes = if snapshot_case {
                encode_sobj(&DfsRepairQueueSnapshotFile {
                    records: vec![substituted],
                    applied_through_sequence: 1,
                })
                .unwrap()
            } else {
                let bytes = encode_sobj(&DfsRepairQueueLogBatch {
                    sequence: 1,
                    operations: vec![DfsRepairQueueOperation::EnqueueIfMissing {
                        record: substituted,
                    }],
                })
                .unwrap();
                let mut framed = (bytes.len() as u32).to_le_bytes().to_vec();
                framed.extend(bytes);
                framed
            };
            assert_eq!(original_bytes.len(), substituted_bytes.len());
            let original_fingerprint = store.inner.lock().unwrap().disk_fingerprint().unwrap();
            store.reload_from_disk_on_next_access();
            let before_path = changed_path.clone();
            REPAIR_QUEUE_BEFORE_REPLAY_HOOK.set(Some(Box::new(move || {
                write_file_atomic(&before_path, &substituted_bytes).unwrap();
            })));
            REPAIR_QUEUE_AFTER_REPLAY_HOOK.set(Some(Box::new(move || {
                write_file_atomic(&changed_path, &original_bytes).unwrap();
            })));
            let error = store
                .view_for_maintenance()
                .expect_err("B's decoded queue cannot be sealed by A's restored bytes");
            assert!(error.to_string().contains("changed while loading"));
            let state = store.inner.lock().unwrap();
            assert!(!state.loaded);
            assert!(state.fingerprint.is_none());
            assert_eq!(
                state.disk_fingerprint().unwrap(),
                original_fingerprint,
                "A is restored despite the rejected B replay"
            );
            drop(state);
            assert_eq!(
                store.view_for_maintenance().unwrap().0.records,
                vec![original]
            );
        }
    }

    #[test]
    fn repair_queue_refresh_fingerprint_includes_covered_frames_and_torn_body() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-consumed-bytes");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let queued = record("alpha", "node-alpha", 3);
        store.append_enqueues([queued.clone()]).unwrap();
        let log = repair_queue_active_log_path(&path);
        let mut covered_and_torn = fs::read(&log).unwrap();
        store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        // The complete old frame is covered by snapshot floor 1. The following
        // body is incomplete; both still contribute to the raw fingerprint.
        covered_and_torn.extend_from_slice(&77_u32.to_le_bytes());
        covered_and_torn.extend_from_slice(&[1, 2, 3]);
        write_file_atomic(&log, &covered_and_torn).unwrap();
        assert_eq!(
            store.view_for_maintenance().unwrap().0.records,
            vec![queued.clone()]
        );
        REPAIR_QUEUE_FULL_REPLAYS.set(0);
        assert_eq!(
            store.view_for_maintenance().unwrap().0.records,
            vec![queued]
        );
        assert_eq!(
            REPAIR_QUEUE_FULL_REPLAYS.get(),
            0,
            "the complete consumed byte generation must match on the next pass"
        );
        let state = store.inner.lock().unwrap();
        assert_eq!(
            state.fingerprint.as_ref(),
            Some(&state.disk_fingerprint().unwrap())
        );
        assert!(state.valid_active_log_bytes < state.active_log_bytes);
    }

    #[test]
    fn repair_queue_refresh_rejects_generation_changed_during_load() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-load-race");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        store
            .append_enqueues([record("alpha", "node-alpha", 1)])
            .unwrap();
        store.reload_from_disk_on_next_access();
        let log = repair_queue_active_log_path(&path);
        REPAIR_QUEUE_AFTER_REPLAY_HOOK.set(Some(Box::new(move || {
            append_external_batch(&log, 2, vec![record("beta", "node-beta", 0)]);
        })));
        assert!(store.view_for_maintenance().is_err());
        assert!(matches!(
            store.bounded_status_view(1).unwrap(),
            DfsRepairQueueBoundedStatus::NotHydrated
        ));
        assert_eq!(store.view_for_maintenance().unwrap().0.records.len(), 2);
    }

    #[test]
    fn repair_queue_refresh_never_bypasses_failed_resync_for_noop_retry() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-resync-failure");
        let store = LocalDfsRepairQueueStore::new(root.join("repair.sobj"));
        let queued = record("alpha", "node-alpha", 2);
        store.append_enqueues([queued.clone()]).unwrap();
        {
            let mut state = store.inner.lock().unwrap();
            state.loaded = false;
            state.resync_on_load = true;
        }
        REPAIR_QUEUE_FAIL_RESYNC.set(true);
        assert!(store.append_enqueues([queued.clone()]).is_err());
        {
            let state = store.inner.lock().unwrap();
            assert!(!state.loaded);
            assert!(state.resync_on_load);
        }
        assert_eq!(store.append_enqueues([queued.clone()]).unwrap(), 0);
        assert_eq!(
            store.view_for_maintenance().unwrap().0.records,
            vec![queued]
        );
        assert!(!store.inner.lock().unwrap().resync_on_load);
    }

    #[test]
    fn repair_queue_replays_enqueue_set_and_remove_in_order() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-replay");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let alpha = record("alpha", "node-alpha", 0);
        let beta = record("beta", "node-beta", 0);
        assert_eq!(
            store
                .append_enqueues([alpha.clone(), beta.clone()])
                .expect("enqueue"),
            2
        );
        let mut retried_alpha = alpha.clone();
        retried_alpha.attempts = 3;
        retried_alpha.last_error = Some("backoff".to_string());
        store
            .append_operations(vec![
                DfsRepairQueueOperation::Set {
                    record: retried_alpha.clone(),
                },
                DfsRepairQueueOperation::Remove {
                    key: DfsRepairQueueKey::from_record(&beta),
                },
            ])
            .expect("update and remove");

        let reopened = reopen(&snapshot_path);
        let view = reopened.view().expect("replay");
        assert_eq!(view.head_sequence, 2);
        assert_eq!(view.records, vec![retried_alpha]);
    }

    #[test]
    fn repair_queue_enqueue_if_missing_preserves_existing_backoff() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-preserve-backoff");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let existing = record("alpha", "node-alpha", 4);
        store
            .append_enqueues([existing.clone()])
            .expect("initial enqueue");
        assert_eq!(
            store
                .append_enqueues([record("alpha", "node-alpha", 0)])
                .expect("idempotent enqueue"),
            0
        );
        let view = reopen(&snapshot_path).view().expect("replay");
        assert_eq!(view.head_sequence, 1);
        assert_eq!(view.records, vec![existing]);
    }

    #[test]
    fn bounded_status_samples_large_resident_queue_without_replay_or_full_clone() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-bounded-status");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let mut records = (0..4096)
            .map(|index| {
                record(
                    &format!("bounded-{index:04}"),
                    &format!("node-bounded-{index:04}"),
                    0,
                )
            })
            .collect::<Vec<_>>();
        let sentinel = record("zzzz-sentinel", "node-zzzz-sentinel", 0);
        records.push(sentinel.clone());
        store.append_enqueues(records).expect("seed large queue");

        BOUNDED_STATUS_RECORD_VISITS.set(0);
        let DfsRepairQueueBoundedStatus::Hydrated(view) =
            store.bounded_status_view(32).expect("bounded hot view")
        else {
            panic!("materialized queue must return a hydrated bounded view");
        };
        assert_eq!(view.record_count, 4097);
        assert_eq!(view.records.len(), 32);
        assert!(!view.records.contains(&sentinel));
        assert_eq!(
            BOUNDED_STATUS_RECORD_VISITS.get(),
            32,
            "status may visit and clone only the requested sample, not the complete resident queue"
        );

        let cold = reopen(&snapshot_path);
        fs::write(&snapshot_path, b"not a repair queue snapshot")
            .expect("install disk replay sentinel");
        let cold_status = cold
            .bounded_status_view(32)
            .expect("cold bounded status must not replay disk");
        assert_eq!(cold_status, DfsRepairQueueBoundedStatus::NotHydrated);
    }

    #[test]
    fn repair_queue_enqueue_rejects_conflicting_duplicate_batch_identity() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-conflicting-batch");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let original = record("alpha", "node-alpha", 0);
        let mut conflicting = original.clone();
        conflicting.content_hash = "blake3:conflicting".to_string();

        assert!(
            store
                .append_enqueues([original, conflicting])
                .expect_err("conflicting duplicate identity must fail")
                .to_string()
                .contains("changed object, version, or content hash")
        );
        assert!(store.view().expect("unchanged queue").records.is_empty());
    }

    #[test]
    fn repair_queue_custom_snapshot_paths_have_distinct_active_logs() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-distinct-logs");
        let first = root.join("first.sobj");
        let second = root.join("second.sobj");

        assert_ne!(
            repair_queue_active_log_path(&first),
            repair_queue_active_log_path(&second)
        );
        assert_eq!(
            repair_queue_active_log_path(&first),
            root.join("first.active.slog")
        );
    }

    #[test]
    fn repair_queue_resyncs_uncertain_complete_frame_before_noop_retry() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-resync-noop");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let queued = record("alpha", "node-alpha", 0);
        store
            .append_enqueues([queued.clone()])
            .expect("initial enqueue");
        {
            let mut state = store.inner.lock().expect("queue state");
            // Model append_length_prefixed_records having written a complete
            // frame before its sync returned an error.
            state.loaded = false;
            state.resync_on_load = true;
        }

        assert_eq!(
            store
                .append_enqueues([queued.clone()])
                .expect("idempotent retry after resync"),
            0
        );
        let state = store.inner.lock().expect("queue state");
        assert!(state.loaded);
        assert!(!state.resync_on_load);
        assert_eq!(
            state.records.values().cloned().collect::<Vec<_>>(),
            vec![queued]
        );
    }

    #[test]
    fn repair_queue_reconcile_rejects_stale_head_without_losing_enqueue() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-cas");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let stale = store.view().expect("empty view");
        let queued = record("alpha", "node-alpha", 0);
        store
            .append_enqueues([queued.clone()])
            .expect("concurrent enqueue");

        assert_eq!(
            store.reconcile(stale.head_sequence, &[]).expect("CAS"),
            DfsRepairQueueReconcileOutcome::GenerationChanged {
                current_head_sequence: 1,
            }
        );
        assert_eq!(store.view().expect("current view").records, vec![queued]);
    }

    #[test]
    fn repair_queue_torn_tail_is_truncated_before_later_append() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-torn-tail");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let alpha = record("alpha", "node-alpha", 0);
        store
            .append_enqueues([alpha.clone()])
            .expect("first enqueue");
        let active_log_path = repair_queue_active_log_path(&snapshot_path);
        let valid_len = fs::metadata(&active_log_path).expect("valid log").len();
        drop(store);
        LocalDfsRepairQueueStore::evict_for_test(&snapshot_path);
        let mut file = OpenOptions::new()
            .append(true)
            .open(&active_log_path)
            .expect("open log");
        file.write_all(&64_u32.to_le_bytes()).expect("length");
        file.write_all(b"torn").expect("torn bytes");
        file.sync_all().expect("sync torn bytes");
        drop(file);

        let reopened = LocalDfsRepairQueueStore::new(&snapshot_path);
        assert_eq!(
            reopened.view().expect("read valid prefix").records,
            vec![alpha]
        );
        let beta = record("beta", "node-beta", 0);
        reopened
            .append_enqueues([beta.clone()])
            .expect("append after repair");
        assert!(fs::metadata(&active_log_path).expect("repaired log").len() > valid_len);
        assert_eq!(
            reopen(&snapshot_path)
                .view()
                .expect("replay repaired log")
                .records,
            vec![record("alpha", "node-alpha", 0), beta]
        );
    }

    #[test]
    fn repair_queue_empty_snapshot_floor_prevents_enqueue_resurrection() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-empty-compaction");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let queued = record("alpha", "node-alpha", 0);
        store.append_enqueues([queued.clone()]).expect("enqueue");
        store
            .append_operations(vec![DfsRepairQueueOperation::Remove {
                key: DfsRepairQueueKey::from_record(&queued),
            }])
            .expect("remove");
        let active_log_path = repair_queue_active_log_path(&snapshot_path);
        let old_log = fs::read(&active_log_path).expect("old log");
        let report = store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .expect("compact empty queue");
        assert!(report.compacted);
        assert_eq!(report.applied_through_sequence, 2);
        let snapshot = read_repair_queue_snapshot(&snapshot_path).expect("snapshot");
        assert_eq!(snapshot.applied_through_sequence, 2);
        assert!(snapshot.records.is_empty());

        // Model a crash after snapshot publication but before old-log pruning.
        fs::write(&active_log_path, old_log).expect("restore retained old log");
        let view = reopen(&snapshot_path).view().expect("replay floor");
        assert_eq!(view.head_sequence, 2);
        assert!(view.records.is_empty());
    }

    #[test]
    fn repair_queue_compaction_preserves_later_active_suffix() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-compaction-suffix");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let alpha = record("alpha", "node-alpha", 0);
        let beta = record("beta", "node-beta", 0);
        store
            .append_enqueues([alpha.clone()])
            .expect("enqueue before compaction");
        store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .expect("compact");
        store
            .append_enqueues([beta.clone()])
            .expect("enqueue suffix");

        let view = reopen(&snapshot_path)
            .view()
            .expect("replay snapshot and suffix");
        assert_eq!(view.head_sequence, 2);
        assert_eq!(view.records, vec![alpha, beta]);
    }

    #[test]
    fn repair_queue_reads_legacy_snapshot_at_sequence_zero() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-legacy-snapshot");
        let snapshot_path = root.join("repair-attempts.sobj");
        let legacy = record("legacy", "node-legacy", 2);
        write_sobj_atomic(
            &snapshot_path,
            &DfsRepairAttemptSnapshot {
                records: vec![legacy.clone()],
            },
        )
        .expect("legacy snapshot");
        let store = reopen(&snapshot_path);
        let initial = store.view().expect("legacy view");
        assert_eq!(initial.head_sequence, 0);
        assert_eq!(initial.records, vec![legacy.clone()]);
        let fresh = record("fresh", "node-fresh", 0);
        store
            .append_enqueues([fresh.clone()])
            .expect("append fresh");
        assert_eq!(
            reopen(&snapshot_path)
                .view()
                .expect("legacy plus log")
                .records,
            vec![fresh, legacy]
        );
    }

    #[test]
    fn repair_queue_explicit_reload_observes_external_snapshot_corruption() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-explicit-reload");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let queued = record("alpha", "node-alpha", 0);
        store
            .append_enqueues([queued.clone()])
            .expect("enqueue before compaction");
        store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .expect("compact queue");
        fs::write(&snapshot_path, b"invalid repair queue snapshot")
            .expect("replace snapshot externally");

        assert_eq!(
            store.view().expect("hot view remains materialized").records,
            vec![queued]
        );
        store.reload_from_disk_on_next_access();
        assert!(
            store.view().is_err(),
            "an explicit maintenance reload must observe external corruption"
        );
    }
}
