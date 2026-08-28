//! Incremental durable storage for the node-local DFS repair queue.
//!
//! The metadata journal remains the authority for write intents and receipts.
//! This store is a bounded restart accelerator: foreground commits append only
//! their repair-queue delta, while maintenance occasionally folds the framed
//! log into an atomic snapshot.

use crate::DfsError;
use crate::maintenance::DfsRepairAttemptRecord;
use crate::metadata_io::{metadata_durable_file_error, write_sobj_atomic};
use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_core::storage::durable_file;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Seek as _;
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

#[derive(Debug)]
struct DfsRepairQueueState {
    snapshot_path: PathBuf,
    active_log_path: PathBuf,
    loaded: bool,
    snapshot_floor: u64,
    head_sequence: u64,
    active_log_bytes: u64,
    valid_active_log_bytes: u64,
    /// An append sync returned an error, so replayed bytes must be synced (or
    /// a torn suffix truncated and synced) before a retry may acknowledge an
    /// idempotent no-op.
    resync_on_load: bool,
    records: BTreeMap<DfsRepairQueueKey, DfsRepairAttemptRecord>,
}

impl DfsRepairQueueState {
    fn new(snapshot_path: PathBuf) -> Self {
        let active_log_path = repair_queue_active_log_path(&snapshot_path);
        Self {
            snapshot_path,
            active_log_path,
            loaded: false,
            snapshot_floor: 0,
            head_sequence: 0,
            active_log_bytes: 0,
            valid_active_log_bytes: 0,
            resync_on_load: false,
            records: BTreeMap::new(),
        }
    }

    fn ensure_loaded(&mut self) -> Result<(), DfsError> {
        if self.loaded {
            return Ok(());
        }

        let snapshot = read_repair_queue_snapshot(&self.snapshot_path)?;
        self.snapshot_floor = snapshot.applied_through_sequence;
        self.head_sequence = snapshot.applied_through_sequence;
        self.records = records_by_key(snapshot.records)?;

        let replay = replay_repair_queue_log(
            &self.active_log_path,
            self.snapshot_floor,
            &mut self.records,
        )?;
        self.head_sequence = replay.head_sequence;
        self.active_log_bytes = replay.file_bytes;
        self.valid_active_log_bytes = replay.valid_bytes;
        if self.resync_on_load {
            self.resync_replayed_log()?;
            self.resync_on_load = false;
        }
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

        if let Err(error) = apply_repair_queue_operations(&mut self.records, &batch.operations) {
            // Validation above makes this unreachable unless the operation
            // semantics change. Reload from the durable frame rather than
            // retaining a cache that disagrees with disk.
            self.loaded = false;
            return Err(error);
        }
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
        Ok(())
    }

    fn resync_replayed_log(&mut self) -> Result<(), DfsError> {
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
    static BOUNDED_STATUS_RECORD_CLONES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
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
    /// Foreground appends deliberately keep the materialized queue hot. A new
    /// maintenance pass uses this explicit boundary to observe out-of-process
    /// replacement, corruption, or recovery of the node-local queue without
    /// adding a stat/replay to every write or reactive probe.
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
        let records = state.records.values().take(max_records).cloned().collect();
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
        if let Err(error) = write_sobj_atomic(&state.snapshot_path, &snapshot) {
            state.loaded = false;
            return Err(error.into());
        }
        if state.active_log_path.exists()
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DfsRepairQueueReplayReport {
    head_sequence: u64,
    file_bytes: u64,
    valid_bytes: u64,
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
                ..DfsRepairQueueReplayReport::default()
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
            break record_start;
        };
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

fn read_repair_queue_snapshot(path: &Path) -> Result<DfsRepairQueueSnapshotFile, DfsError> {
    match fs::read(path) {
        Ok(bytes) => {
            sinomo_object_serde::from_bytes(&bytes).map_err(|source| DfsError::Serialization {
                message: format!(
                    "DFS repair queue snapshot object error at {}: {source}",
                    path.display()
                ),
            })
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            Ok(DfsRepairQueueSnapshotFile::default())
        }
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

        BOUNDED_STATUS_RECORD_CLONES.set(0);
        let DfsRepairQueueBoundedStatus::Hydrated(view) =
            store.bounded_status_view(32).expect("bounded hot view")
        else {
            panic!("materialized queue must return a hydrated bounded view");
        };
        assert_eq!(view.record_count, 4097);
        assert_eq!(view.records.len(), 32);
        assert!(!view.records.contains(&sentinel));
        assert_eq!(
            BOUNDED_STATUS_RECORD_CLONES.get(),
            32,
            "status may clone only the requested sample, not the complete resident queue"
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
