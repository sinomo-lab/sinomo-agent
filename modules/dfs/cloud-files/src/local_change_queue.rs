//! Crash-safe node-local staging for Windows Cloud Files writeback.
//!
//! This store provides immutable staged bytes, caller-stable operation IDs,
//! append-before-send durability, and enough precondition/conflict evidence to
//! reconcile or replay an ambiguous DFS mutation. Provider activation remains
//! gated by the surrounding Windows adapter's ownership and startup checks.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fs2::FileExt as _;
use serde::{Deserialize, Serialize};
use sinomo_module_api::{decode_sobj, encode_sobj};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::storage::durable_file::{
    self, DurableAtomicFileOptions, DurableFileSync, DurableLengthEndian,
    DurableRecordAppendOptions, DurableRecordReadOptions,
};

use sinomo_dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_MODULE_ID;

#[cfg(windows)]
use std::os::windows::fs::MetadataExt as _;

const QUEUE_SCHEMA_VERSION: u16 = 1;
const QUEUE_RECORD_MAX_BYTES: usize = 1024 * 1024;
#[cfg(not(test))]
const QUEUE_JOURNAL_COMPACT_BYTES: u64 = 8 * 1024 * 1024;
#[cfg(test)]
const QUEUE_JOURNAL_COMPACT_BYTES: u64 = 256 * 1024;
#[cfg(not(test))]
const QUEUE_JOURNAL_MAX_BYTES: u64 = 32 * 1024 * 1024;
#[cfg(test)]
const QUEUE_JOURNAL_MAX_BYTES: u64 = 256 * 1024;
const QUEUE_JOURNAL_COMPACT_MIN_RECORDS: u64 = 128;
const QUEUE_DIR: &str = "local-changes";
const QUEUE_JOURNAL: &str = "operations.active.slog";
const QUEUE_COMPACTION_STAGE: &str = "operations.compact.staged";
const QUEUE_LOCK: &str = "operations.lock";
const QUEUE_BLOBS_DIR: &str = "blobs";
const QUEUE_RETIRED_LEDGER: &str = "operations.retired.sobj";
const QUEUE_RETIRED_LEDGER_TEMP_LABEL: &str = "cloud-files-retired";
const QUEUE_RETIRED_LEDGER_SCHEMA_VERSION: u16 = 1;
const QUEUE_RETIRED_LEDGER_MAX_BYTES: u64 = 32 * 1024 * 1024;
#[cfg(not(test))]
const QUEUE_RETIRED_TOMBSTONE_LIMIT: usize = 65_536;
#[cfg(test)]
const QUEUE_RETIRED_TOMBSTONE_LIMIT: usize = 64;
const BLOB_HASH_PREFIX: &str = "blake3:";
const MOUNT_SCOPE_DOMAIN: &[u8] = b"sinomo.dfs.cloud-files.mount-state.v1\0";

static STAGED_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CloudFilesLocalOperationKind {
    CreateFile,
    ReplaceFile,
    CreateDirectory,
    Rename,
    DeleteFile,
    DeleteDirectory,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesExpectedObjectState {
    pub object_id: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CloudFilesDestinationPrecondition {
    #[default]
    Unspecified,
    Absent,
    Exact(CloudFilesExpectedObjectState),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesLocalOperationPreconditions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<CloudFilesExpectedObjectState>,
    #[serde(default)]
    pub destination: CloudFilesDestinationPrecondition,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesStagedContent {
    pub content_hash: String,
    pub length: u64,
}

/// Cloneable, mount-scoped writer for immutable capture blobs. It exposes no
/// queue mutation: callers may publish bytes before enqueue, while the queue's
/// durable record remains the sole authority that makes them executable.
#[derive(Clone, Debug)]
pub struct CloudFilesContentStager {
    blobs_dir: PathBuf,
}

impl CloudFilesContentStager {
    pub fn stage_reader<R: Read>(
        &self,
        reader: R,
    ) -> Result<CloudFilesStagedContent, CloudFilesLocalChangeQueueError> {
        stage_reader_in_blobs_dir(&self.blobs_dir, reader)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesLocalOperationRequest {
    pub operation_id: String,
    /// Stable Windows file/USN observation that selected this operation ID.
    /// Legacy queue records predate this field and retain their exact IDs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_id: Option<String>,
    pub mount: String,
    pub root: String,
    pub kind: CloudFilesLocalOperationKind,
    pub source_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_path: Option<String>,
    #[serde(default)]
    pub preconditions: CloudFilesLocalOperationPreconditions,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_content: Option<CloudFilesStagedContent>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CloudFilesLocalOperationStatus {
    Queued,
    InFlight,
    Retrying,
    RemoteApplied,
    Completed,
    Conflict,
    Failed,
    Superseded,
}

impl CloudFilesLocalOperationStatus {
    fn retains_blob(&self) -> bool {
        !matches!(self, Self::Completed | Self::Superseded)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesConflictEvidence {
    pub code: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_object_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_content_hash: Option<String>,
}

/// Durable receipt retained between remote application and local Cloud Files
/// finalization. The operation ID is repeated deliberately so replay can
/// reject evidence accidentally attached to a different queued mutation.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesRemoteMutationEvidence {
    pub operation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesLocalOperation {
    pub request: CloudFilesLocalOperationRequest,
    /// Digest of the immutable request. The same operation ID with different
    /// paths, preconditions, or staged content is rejected during enqueue and
    /// replay.
    pub request_digest: String,
    pub status: CloudFilesLocalOperationStatus,
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict: Option<CloudFilesConflictEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence: Option<CloudFilesRemoteMutationEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CloudFilesLocalQueueSummary {
    pub pending_operations: u64,
    pub retrying_operations: u64,
    pub conflict_count: u64,
    pub failed_operations: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudFilesLocalOperationRetirement {
    pub operation: CloudFilesLocalOperation,
    pub blobs_pruned: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CloudFilesLocalQueueMaintenance {
    pub blobs_pruned: u64,
    pub journal_bytes: u64,
}

#[derive(Debug)]
pub struct CloudFilesLocalChangeQueueError {
    pub code: &'static str,
    pub detail: String,
}

impl std::fmt::Display for CloudFilesLocalChangeQueueError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for CloudFilesLocalChangeQueueError {}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct QueueJournalRecordBody {
    schema_version: u16,
    sequence: u64,
    operation: CloudFilesLocalOperation,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct QueueJournalRecord {
    body: QueueJournalRecordBody,
    checksum: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct ReconciledRetirementTombstone {
    operation_id: String,
    request_digest: String,
    ordinal: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct ReconciledRetirementLedgerSnapshot {
    schema_version: u16,
    next_ordinal: u64,
    tombstones: Vec<ReconciledRetirementTombstone>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReconciledRetirementLedger {
    next_ordinal: u64,
    order: VecDeque<String>,
    tombstones: BTreeMap<String, ReconciledRetirementTombstone>,
}

impl Default for ReconciledRetirementLedger {
    fn default() -> Self {
        Self {
            next_ordinal: 1,
            order: VecDeque::new(),
            tombstones: BTreeMap::new(),
        }
    }
}

/// One durable queue scoped to a single local mount and SIFS root.
pub struct CloudFilesLocalChangeQueue {
    mount: String,
    root: String,
    queue_dir: PathBuf,
    blobs_dir: PathBuf,
    journal_path: PathBuf,
    retired_ledger_path: PathBuf,
    _lock: File,
    journal: Option<File>,
    next_sequence: u64,
    journal_record_count: u64,
    operations: BTreeMap<String, CloudFilesLocalOperation>,
    retired_ledger: ReconciledRetirementLedger,
    usable: bool,
}

impl CloudFilesLocalChangeQueue {
    pub fn open(
        data_dir: &SinomoDataDir,
        mount: impl Into<String>,
        root: impl Into<String>,
    ) -> Result<Self, CloudFilesLocalChangeQueueError> {
        let mount = mount.into();
        if mount.trim().is_empty() {
            return Err(queue_error(
                "invalid-mount",
                "mount identity cannot be empty",
            ));
        }
        let root = canonical_sifs_path(&root.into())?;
        let runtime_dir = data_dir.module_runtime_dir();
        fs::create_dir_all(&runtime_dir).map_err(io_error("create module runtime directory"))?;
        let module_dir = runtime_dir.join(SINOMO_DFS_CLOUD_FILES_MODULE_ID);
        durable_file::ensure_private_directory(&module_dir)
            .map_err(storage_error("create Cloud Files module runtime directory"))?;
        let queue_root = module_dir.join(QUEUE_DIR);
        durable_file::ensure_private_directory(&queue_root)
            .map_err(storage_error("create Cloud Files local-change queue root"))?;
        let queue_dir = queue_root.join(mount_scope_directory(&mount, &root)?);
        durable_file::ensure_private_directory(&queue_dir)
            .map_err(storage_error("create Cloud Files local-change queue"))?;
        let blobs_dir = queue_dir.join(QUEUE_BLOBS_DIR);
        durable_file::ensure_private_directory(&blobs_dir)
            .map_err(storage_error("create Cloud Files staged-content directory"))?;
        let lock_path = queue_dir.join(QUEUE_LOCK);
        let lock = durable_file::open_file_for_append(&lock_path)
            .map_err(storage_error("open Cloud Files queue lock"))?
            .file;
        lock.try_lock_exclusive().map_err(|source| {
            queue_error(
                "queue-busy",
                format!(
                    "another Cloud Files writer owns queue {}: {source}",
                    queue_dir.display()
                ),
            )
        })?;
        let compaction_stage = queue_dir.join(QUEUE_COMPACTION_STAGE);
        durable_file::remove_file_if_exists_and_sync_parent(&compaction_stage).map_err(
            storage_error("remove abandoned Cloud Files queue compaction stage"),
        )?;
        let journal_path = queue_dir.join(QUEUE_JOURNAL);
        let retired_ledger_path = queue_dir.join(QUEUE_RETIRED_LEDGER);
        let append = durable_file::open_file_for_append(&journal_path)
            .map_err(storage_error("open Cloud Files local-change journal"))?;
        let mut queue = Self {
            mount,
            root,
            queue_dir,
            blobs_dir,
            journal_path,
            retired_ledger_path,
            _lock: lock,
            journal: Some(append.file),
            next_sequence: 1,
            journal_record_count: 0,
            operations: BTreeMap::new(),
            retired_ledger: ReconciledRetirementLedger::default(),
            usable: true,
        };
        queue.replay_and_repair_torn_tail()?;
        queue.load_reconciled_retirement_ledger()?;
        queue.requeue_ambiguous_operations()?;
        Ok(queue)
    }

    pub(crate) fn open_authorized_cursor_recovery(
        data_dir: &SinomoDataDir,
        mount: impl Into<String>,
        root: impl Into<String>,
    ) -> Result<Self, CloudFilesLocalChangeQueueError> {
        let mount = mount.into();
        if mount.trim().is_empty() {
            return Err(queue_error(
                "invalid-mount",
                "mount identity cannot be empty",
            ));
        }
        let root = canonical_sifs_path(&root.into())?;
        let guard = inspect_cursor_only_local_change_queue(data_dir, &mount, &root)?;
        if !guard.inspection().scope_present {
            return Err(queue_error(
                "cursor-recovery-queue-absent",
                "authorized recovery requires the canonical pre-existing queue",
            ));
        }
        // Transfer the exact pre-existing lock into the live queue. Calling the
        // ordinary opener here would drop this fence and then delete a
        // compaction stage, truncate a torn tail, or requeue ambiguous records
        // before we could discover that evidence appeared in the gap.
        let CloudFilesCursorOnlyQueueInspectionGuard {
            inspection: _,
            _lock: Some(lock),
        } = guard
        else {
            return Err(queue_error(
                "cursor-recovery-queue-lock",
                "authorized recovery lost its pre-existing queue lock",
            ));
        };
        let queue_dir = data_dir
            .module_runtime_dir()
            .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
            .join(QUEUE_DIR)
            .join(mount_scope_directory(&mount, &root)?);
        let blobs_dir = queue_dir.join(QUEUE_BLOBS_DIR);
        let journal_path = queue_dir.join(QUEUE_JOURNAL);
        let retired_ledger_path = queue_dir.join(QUEUE_RETIRED_LEDGER);
        let journal = fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(false)
            .open(&journal_path)
            .map_err(io_error("open exact cursor-recovery queue journal"))?;
        if journal
            .metadata()
            .map_err(io_error("recheck cursor-recovery queue journal"))?
            .len()
            != 0
        {
            return Err(queue_error(
                "cursor-recovery-queue-not-empty",
                "authorized recovery queue journal changed under its owner lock",
            ));
        }
        Ok(Self {
            mount,
            root,
            queue_dir,
            blobs_dir,
            journal_path,
            retired_ledger_path,
            _lock: lock,
            journal: Some(journal),
            next_sequence: 1,
            journal_record_count: 0,
            operations: BTreeMap::new(),
            retired_ledger: ReconciledRetirementLedger::default(),
            usable: true,
        })
    }

    pub fn queue_dir(&self) -> &Path {
        &self.queue_dir
    }

    pub fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    pub fn operation(&self, operation_id: &str) -> Option<&CloudFilesLocalOperation> {
        self.operations.get(operation_id)
    }

    pub fn operation_id_for_capture(&self, capture_id: &str) -> Option<&str> {
        self.operations.values().find_map(|operation| {
            (operation.request.capture_id.as_deref() == Some(capture_id))
                .then_some(operation.request.operation_id.as_str())
        })
    }

    pub fn operations(&self) -> impl Iterator<Item = &CloudFilesLocalOperation> {
        self.operations.values()
    }

    pub fn summary(&self) -> CloudFilesLocalQueueSummary {
        let mut summary = CloudFilesLocalQueueSummary::default();
        for operation in self.operations.values() {
            match operation.status {
                CloudFilesLocalOperationStatus::Queued => summary.pending_operations += 1,
                CloudFilesLocalOperationStatus::Retrying
                | CloudFilesLocalOperationStatus::InFlight => summary.retrying_operations += 1,
                CloudFilesLocalOperationStatus::Conflict => summary.conflict_count += 1,
                CloudFilesLocalOperationStatus::Failed => summary.failed_operations += 1,
                CloudFilesLocalOperationStatus::RemoteApplied
                | CloudFilesLocalOperationStatus::Completed
                | CloudFilesLocalOperationStatus::Superseded => {}
            }
        }
        summary
    }

    pub fn content_stager(&self) -> CloudFilesContentStager {
        CloudFilesContentStager {
            blobs_dir: self.blobs_dir.clone(),
        }
    }

    pub fn verify_staged_content(
        &self,
        content: &CloudFilesStagedContent,
    ) -> Result<(), CloudFilesLocalChangeQueueError> {
        self.ensure_usable()?;
        verify_blob(&self.blob_path(content)?, content)
    }

    /// Stage immutable content before the corresponding queue record is made
    /// visible. The returned hash/length can be embedded in a stable request.
    pub fn stage_content(
        &self,
        bytes: &[u8],
    ) -> Result<CloudFilesStagedContent, CloudFilesLocalChangeQueueError> {
        self.ensure_usable()?;
        stage_reader_in_blobs_dir(&self.blobs_dir, bytes)
    }

    pub fn enqueue(
        &mut self,
        mut request: CloudFilesLocalOperationRequest,
    ) -> Result<CloudFilesLocalOperation, CloudFilesLocalChangeQueueError> {
        self.validate_request(&mut request)?;
        let request_digest = request_digest(&request)?;
        if let Some(capture_id) = request.capture_id.as_deref()
            && let Some(existing) = self.operations.values().find(|operation| {
                operation.request.capture_id.as_deref() == Some(capture_id)
                    && operation.request.operation_id != request.operation_id
            })
        {
            return Err(queue_error(
                "capture-id-reused",
                format!(
                    "capture ID `{capture_id}` is already durably bound to operation `{}`",
                    existing.request.operation_id
                ),
            ));
        }
        if let Some(existing) = self.operations.get(&request.operation_id) {
            if existing.request_digest == request_digest && existing.request == request {
                return Ok(existing.clone());
            }
            return Err(queue_error(
                "operation-id-reused",
                format!(
                    "operation ID `{}` was already durably assigned to a different request",
                    request.operation_id
                ),
            ));
        }
        if let Some(retired) = self.retired_ledger.tombstones.get(&request.operation_id) {
            let (code, detail) = if retired.request_digest == request_digest {
                (
                    "operation-already-reconciled",
                    format!(
                        "operation ID `{}` was already reconciled through the authoritative namespace feed",
                        request.operation_id
                    ),
                )
            } else {
                (
                    "operation-id-reused",
                    format!(
                        "operation ID `{}` was already durably assigned to a reconciled request with a different digest",
                        request.operation_id
                    ),
                )
            };
            return Err(queue_error(code, detail));
        }
        if let Some(content) = request.staged_content.as_ref() {
            verify_blob(&self.blob_path(content)?, content)?;
        }
        let operation = CloudFilesLocalOperation {
            request,
            request_digest,
            status: CloudFilesLocalOperationStatus::Queued,
            attempts: 0,
            conflict: None,
            remote_evidence: None,
            last_error: None,
        };
        self.append_operation(operation.clone())?;
        Ok(operation)
    }

    pub fn mark_in_flight(
        &mut self,
        operation_id: &str,
    ) -> Result<CloudFilesLocalOperation, CloudFilesLocalChangeQueueError> {
        let mut operation = self.require_operation(operation_id)?.clone();
        operation.status = CloudFilesLocalOperationStatus::InFlight;
        operation.attempts = operation.attempts.saturating_add(1);
        operation.conflict = None;
        operation.remote_evidence = None;
        operation.last_error = None;
        self.append_operation(operation.clone())?;
        Ok(operation)
    }

    pub fn mark_remote_applied(
        &mut self,
        operation_id: &str,
    ) -> Result<CloudFilesLocalOperation, CloudFilesLocalChangeQueueError> {
        self.mark_remote_applied_with_evidence(CloudFilesRemoteMutationEvidence {
            operation_id: operation_id.to_string(),
            object_id: None,
            version: None,
            content_hash: None,
            content_size_bytes: None,
            kind: None,
            detail: "remote mutation applied".to_string(),
        })
    }

    pub fn mark_remote_applied_with_evidence(
        &mut self,
        evidence: CloudFilesRemoteMutationEvidence,
    ) -> Result<CloudFilesLocalOperation, CloudFilesLocalChangeQueueError> {
        let mut operation = self.require_operation(&evidence.operation_id)?.clone();
        operation.status = CloudFilesLocalOperationStatus::RemoteApplied;
        operation.conflict = None;
        operation.last_error = None;
        operation.remote_evidence = Some(evidence);
        self.append_operation(operation.clone())?;
        Ok(operation)
    }

    pub fn mark_completed(
        &mut self,
        operation_id: &str,
    ) -> Result<CloudFilesLocalOperation, CloudFilesLocalChangeQueueError> {
        self.transition(
            operation_id,
            CloudFilesLocalOperationStatus::Completed,
            None,
            None,
        )
    }

    pub fn mark_conflict(
        &mut self,
        operation_id: &str,
        evidence: CloudFilesConflictEvidence,
    ) -> Result<CloudFilesLocalOperation, CloudFilesLocalChangeQueueError> {
        let mut operation = self.require_operation(operation_id)?.clone();
        operation.status = CloudFilesLocalOperationStatus::Conflict;
        operation.conflict = Some(evidence);
        operation.remote_evidence = None;
        operation.last_error = None;
        self.append_operation(operation.clone())?;
        Ok(operation)
    }

    pub fn mark_retrying(
        &mut self,
        operation_id: &str,
        detail: impl Into<String>,
    ) -> Result<CloudFilesLocalOperation, CloudFilesLocalChangeQueueError> {
        let mut operation = self.require_operation(operation_id)?.clone();
        operation.status = CloudFilesLocalOperationStatus::Retrying;
        operation.conflict = None;
        operation.remote_evidence = None;
        operation.last_error = Some(detail.into());
        self.append_operation(operation.clone())?;
        Ok(operation)
    }

    pub fn mark_failed(
        &mut self,
        operation_id: &str,
        detail: impl Into<String>,
    ) -> Result<CloudFilesLocalOperation, CloudFilesLocalChangeQueueError> {
        self.transition(
            operation_id,
            CloudFilesLocalOperationStatus::Failed,
            None,
            Some(detail.into()),
        )
    }

    /// Retire an operation whose outcome has already been surfaced to the
    /// caller. Pending and remotely-applied operations are rejected because
    /// their staged bytes are still required for retry or local finalization.
    /// The latest terminal record remains in the compacted journal so a stable
    /// operation ID can never be reused after retirement.
    pub fn retire_terminal_operation(
        &mut self,
        operation_id: &str,
    ) -> Result<CloudFilesLocalOperationRetirement, CloudFilesLocalChangeQueueError> {
        self.ensure_usable()?;
        let current = self.require_operation(operation_id)?.clone();
        let operation = match current.status {
            CloudFilesLocalOperationStatus::Superseded => current,
            CloudFilesLocalOperationStatus::Completed
            | CloudFilesLocalOperationStatus::Conflict
            | CloudFilesLocalOperationStatus::Failed => {
                let mut retired = current;
                retired.status = CloudFilesLocalOperationStatus::Superseded;
                self.append_operation(retired.clone())?;
                retired
            }
            CloudFilesLocalOperationStatus::Queued
            | CloudFilesLocalOperationStatus::InFlight
            | CloudFilesLocalOperationStatus::Retrying
            | CloudFilesLocalOperationStatus::RemoteApplied => {
                return Err(queue_error(
                    "operation-not-terminal",
                    format!(
                        "operation `{operation_id}` is {:?}; pending work cannot be retired",
                        current.status
                    ),
                ));
            }
        };
        let blobs_pruned = self.prune_released_blobs()?;
        Ok(CloudFilesLocalOperationRetirement {
            operation,
            blobs_pruned,
        })
    }

    /// Move feed-reconciled successful operations out of the active journal.
    ///
    /// The caller may acknowledge an ID only after the authoritative namespace
    /// feed (or an exact full-scan boundary) has consumed its durable no-echo
    /// suppression. The compact tombstone is installed before the full record
    /// is removed, so every crash ordering retains at least one replay fence.
    /// Tombstones expire oldest-first only after this reconciliation proof;
    /// expired IDs no longer represent ambiguous remote outcomes.
    pub fn acknowledge_reconciled_retirements(
        &mut self,
        operation_ids: &[String],
    ) -> Result<u64, CloudFilesLocalChangeQueueError> {
        self.ensure_usable()?;
        if operation_ids.is_empty() {
            return Ok(0);
        }
        let unique = operation_ids.iter().collect::<BTreeSet<_>>();
        let acknowledged = unique.len() as u64;
        let mut ledger = self.retired_ledger.clone();
        let mut compacted = self.operations.clone();
        for operation_id in unique {
            let operation = self.require_operation(operation_id)?;
            if operation.status != CloudFilesLocalOperationStatus::Superseded
                || operation.remote_evidence.is_none()
                || operation.conflict.is_some()
                || operation.last_error.is_some()
            {
                return Err(queue_error(
                    "retirement-not-reconciled",
                    format!(
                        "operation `{operation_id}` is {:?} without retained successful remote evidence",
                        operation.status
                    ),
                ));
            }
            if let Some(existing) = ledger.tombstones.get(operation_id) {
                if existing.request_digest != operation.request_digest {
                    return Err(queue_error(
                        "operation-id-reused",
                        format!(
                            "reconciled operation `{operation_id}` conflicts with its retired digest"
                        ),
                    ));
                }
            } else {
                let tombstone = ReconciledRetirementTombstone {
                    operation_id: operation_id.clone(),
                    request_digest: operation.request_digest.clone(),
                    ordinal: ledger.next_ordinal,
                };
                ledger.next_ordinal = ledger.next_ordinal.saturating_add(1);
                ledger.order.push_back(operation_id.clone());
                ledger.tombstones.insert(operation_id.clone(), tombstone);
            }
            compacted.remove(operation_id);
        }
        while ledger.order.len() > QUEUE_RETIRED_TOMBSTONE_LIMIT {
            if let Some(expired) = ledger.order.pop_front() {
                ledger.tombstones.remove(&expired);
            }
        }
        self.persist_reconciled_retirement_ledger(&ledger)?;
        self.retired_ledger = ledger;
        self.install_compacted_operations(compacted)?;
        Ok(acknowledged)
    }

    /// Explicit production maintenance seam. It reclaims only blobs no
    /// durable operation can still need, then atomically snapshots the latest
    /// record for every stable operation ID when journal history has accrued.
    pub fn compact_and_prune(
        &mut self,
    ) -> Result<CloudFilesLocalQueueMaintenance, CloudFilesLocalChangeQueueError> {
        self.ensure_usable()?;
        let blobs_pruned = self.prune_released_blobs()?;
        let mut journal_bytes = fs::metadata(&self.journal_path)
            .map_err(io_error("inspect compacted Cloud Files queue journal"))?
            .len();
        if self.journal_record_count > u64::try_from(self.operations.len()).unwrap_or(u64::MAX)
            || journal_bytes >= QUEUE_JOURNAL_COMPACT_BYTES
        {
            self.install_compacted_operations(self.operations.clone())?;
            journal_bytes = fs::metadata(&self.journal_path)
                .map_err(io_error("inspect compacted Cloud Files queue journal"))?
                .len();
        }
        Ok(CloudFilesLocalQueueMaintenance {
            blobs_pruned,
            journal_bytes,
        })
    }

    pub fn read_staged_content(
        &self,
        operation_id: &str,
    ) -> Result<Vec<u8>, CloudFilesLocalChangeQueueError> {
        self.ensure_usable()?;
        let operation = self.require_operation(operation_id)?;
        let content = operation.request.staged_content.as_ref().ok_or_else(|| {
            queue_error("missing-staged-content", "operation has no staged bytes")
        })?;
        let path = self.blob_path(content)?;
        verify_blob(&path, content)?;
        let mut bytes = Vec::new();
        File::open(&path)
            .and_then(|mut file| file.read_to_end(&mut bytes))
            .map_err(io_error("read Cloud Files staged content"))?;
        Ok(bytes)
    }

    /// Open the immutable, hash-verified queue blob without materializing it.
    /// The returned handle is independent and may be consumed by a reader-
    /// backed DFS mutation while the queue retains the durable descriptor.
    pub fn open_staged_content(
        &self,
        operation_id: &str,
    ) -> Result<File, CloudFilesLocalChangeQueueError> {
        let path = self.staged_content_path(operation_id)?;
        File::open(&path).map_err(io_error("open Cloud Files staged content"))
    }

    pub fn staged_content_path(
        &self,
        operation_id: &str,
    ) -> Result<PathBuf, CloudFilesLocalChangeQueueError> {
        self.ensure_usable()?;
        let operation = self.require_operation(operation_id)?;
        let content = operation.request.staged_content.as_ref().ok_or_else(|| {
            queue_error("missing-staged-content", "operation has no staged bytes")
        })?;
        let path = self.blob_path(content)?;
        verify_blob(&path, content)?;
        Ok(path)
    }

    /// Remove content only after no durable operation still needs it for
    /// retry, local finalization, conflict resolution, or failure inspection.
    pub fn prune_released_blobs(&self) -> Result<u64, CloudFilesLocalChangeQueueError> {
        self.ensure_usable()?;
        let retained = self
            .operations
            .values()
            .filter(|operation| operation.status.retains_blob())
            .filter_map(|operation| operation.request.staged_content.as_ref())
            .map(|content| content.content_hash.clone())
            .collect::<BTreeSet<_>>();
        let mut removed = 0_u64;
        for entry in fs::read_dir(&self.blobs_dir)
            .map_err(io_error("list Cloud Files staged-content directory"))?
        {
            let entry = entry.map_err(io_error("read Cloud Files staged-content entry"))?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("blob") {
                continue;
            }
            let Some(hash_hex) = path.file_stem().and_then(|name| name.to_str()) else {
                continue;
            };
            if retained.contains(&format!("{BLOB_HASH_PREFIX}{hash_hex}")) {
                continue;
            }
            if durable_file::remove_file_if_exists_and_sync_parent(&path)
                .map_err(storage_error("remove released Cloud Files staged content"))?
            {
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn validate_request(
        &self,
        request: &mut CloudFilesLocalOperationRequest,
    ) -> Result<(), CloudFilesLocalChangeQueueError> {
        if request.operation_id.trim().is_empty() || request.operation_id.len() > 256 {
            return Err(queue_error(
                "invalid-operation-id",
                "operation ID must contain 1..=256 bytes",
            ));
        }
        if request
            .capture_id
            .as_ref()
            .is_some_and(|capture_id| capture_id.trim().is_empty() || capture_id.len() > 1024)
        {
            return Err(queue_error(
                "invalid-capture-id",
                "capture ID must be absent or contain 1..=1024 bytes",
            ));
        }
        if request.mount != self.mount {
            return Err(queue_error(
                "mount-mismatch",
                format!(
                    "operation mount `{}` does not match queue mount `{}`",
                    request.mount, self.mount
                ),
            ));
        }
        request.root = canonical_sifs_path(&request.root)?;
        if request.root != self.root {
            return Err(queue_error(
                "root-mismatch",
                format!(
                    "operation root `{}` does not match queue root `{}`",
                    request.root, self.root
                ),
            ));
        }
        request.source_path = canonical_sifs_path(&request.source_path)?;
        ensure_within_root(&request.source_path, &self.root)?;
        if let Some(destination) = request.destination_path.as_mut() {
            *destination = canonical_sifs_path(destination)?;
            ensure_within_root(destination, &self.root)?;
        }
        match request.kind {
            CloudFilesLocalOperationKind::Rename if request.destination_path.is_none() => {
                return Err(queue_error(
                    "invalid-rename",
                    "rename requires one canonical destination path",
                ));
            }
            CloudFilesLocalOperationKind::CreateFile
            | CloudFilesLocalOperationKind::ReplaceFile
                if request.staged_content.is_none() =>
            {
                return Err(queue_error(
                    "missing-staged-content",
                    "file create/replace requires immutable staged content",
                ));
            }
            _ => {}
        }
        Ok(())
    }

    fn require_operation(
        &self,
        operation_id: &str,
    ) -> Result<&CloudFilesLocalOperation, CloudFilesLocalChangeQueueError> {
        self.operations.get(operation_id).ok_or_else(|| {
            queue_error(
                "operation-not-found",
                format!("operation ID `{operation_id}` is not in this queue"),
            )
        })
    }

    fn transition(
        &mut self,
        operation_id: &str,
        status: CloudFilesLocalOperationStatus,
        conflict: Option<CloudFilesConflictEvidence>,
        last_error: Option<String>,
    ) -> Result<CloudFilesLocalOperation, CloudFilesLocalChangeQueueError> {
        let mut operation = self.require_operation(operation_id)?.clone();
        operation.status = status;
        operation.conflict = conflict;
        operation.last_error = last_error;
        self.append_operation(operation.clone())?;
        Ok(operation)
    }

    fn append_operation(
        &mut self,
        operation: CloudFilesLocalOperation,
    ) -> Result<(), CloudFilesLocalChangeQueueError> {
        self.ensure_usable()?;
        validate_operation_integrity(&operation)?;
        let body = QueueJournalRecordBody {
            schema_version: QUEUE_SCHEMA_VERSION,
            sequence: self.next_sequence,
            operation,
        };
        let record = QueueJournalRecord {
            checksum: journal_body_checksum(&body)?,
            body,
        };
        let bytes = encode_sobj(&record).map_err(sobj_error("encode queue journal record"))?;
        if bytes.len() > QUEUE_RECORD_MAX_BYTES {
            return Err(queue_error(
                "queue-record-too-large",
                format!(
                    "encoded queue record is {} bytes; max is {QUEUE_RECORD_MAX_BYTES}",
                    bytes.len()
                ),
            ));
        }
        let framed_bytes = 4_u64.saturating_add(bytes.len() as u64);
        let journal_bytes = self
            .journal_file()?
            .metadata()
            .map_err(io_error("inspect Cloud Files queue journal"))?
            .len();
        let projected_record_count = self.journal_record_count.saturating_add(1);
        let compact_for_churn = projected_record_count
            >= QUEUE_JOURNAL_COMPACT_MIN_RECORDS.max(
                u64::try_from(self.operations.len())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(4),
            );
        if journal_bytes.saturating_add(framed_bytes) > QUEUE_JOURNAL_MAX_BYTES
            || journal_bytes >= QUEUE_JOURNAL_COMPACT_BYTES
            || compact_for_churn
        {
            let mut compacted = self.operations.clone();
            compacted.insert(
                record.body.operation.request.operation_id.clone(),
                record.body.operation,
            );
            return self.install_compacted_operations(compacted);
        }
        let projected_bytes = journal_bytes.checked_add(framed_bytes).ok_or_else(|| {
            queue_error(
                "queue-journal-too-large",
                "Cloud Files queue journal length overflowed its hard bound",
            )
        })?;
        if projected_bytes > QUEUE_JOURNAL_MAX_BYTES {
            return Err(queue_error(
                "queue-journal-too-large",
                format!(
                    "queue journal would be {projected_bytes} bytes; max is {QUEUE_JOURNAL_MAX_BYTES}"
                ),
            ));
        }
        let append_result = durable_file::append_length_prefixed_records(
            self.journal.as_mut().ok_or_else(|| {
                queue_error(
                    "queue-reopen-required",
                    "Cloud Files queue journal is not open",
                )
            })?,
            &self.journal_path,
            [&bytes],
            DurableRecordAppendOptions::new(DurableLengthEndian::Little).sync(DurableFileSync::All),
        );
        if let Err(source) = append_result {
            self.usable = false;
            return Err(storage_error("append and fsync Cloud Files queue record")(
                source,
            ));
        }
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.journal_record_count = projected_record_count;
        self.operations.insert(
            record.body.operation.request.operation_id.clone(),
            record.body.operation,
        );
        Ok(())
    }

    fn install_compacted_operations(
        &mut self,
        operations: BTreeMap<String, CloudFilesLocalOperation>,
    ) -> Result<(), CloudFilesLocalChangeQueueError> {
        self.ensure_usable()?;
        let mut compacted = Vec::new();
        let mut sequence = 1_u64;
        for operation in operations.values() {
            validate_operation_integrity(operation)?;
            if operation.request.mount != self.mount || operation.request.root != self.root {
                return Err(queue_error(
                    "queue-scope-mismatch",
                    format!(
                        "operation `{}` does not belong to this mount/root scope",
                        operation.request.operation_id
                    ),
                ));
            }
            let body = QueueJournalRecordBody {
                schema_version: QUEUE_SCHEMA_VERSION,
                sequence,
                operation: operation.clone(),
            };
            let record = QueueJournalRecord {
                checksum: journal_body_checksum(&body)?,
                body,
            };
            let bytes =
                encode_sobj(&record).map_err(sobj_error("encode compacted queue record"))?;
            if bytes.len() > QUEUE_RECORD_MAX_BYTES {
                return Err(queue_error(
                    "queue-record-too-large",
                    format!(
                        "encoded compacted queue record is {} bytes; max is {QUEUE_RECORD_MAX_BYTES}",
                        bytes.len()
                    ),
                ));
            }
            let next_len = compacted
                .len()
                .checked_add(4)
                .and_then(|length| length.checked_add(bytes.len()))
                .ok_or_else(|| {
                    queue_error(
                        "queue-journal-too-large",
                        "compacted Cloud Files queue journal length overflowed",
                    )
                })?;
            if next_len as u64 > QUEUE_JOURNAL_MAX_BYTES {
                return Err(queue_error(
                    "queue-journal-too-large",
                    format!(
                        "compacted queue journal would be {next_len} bytes; max is {QUEUE_JOURNAL_MAX_BYTES}"
                    ),
                ));
            }
            compacted.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            compacted.extend_from_slice(&bytes);
            sequence = sequence.saturating_add(1);
        }

        let staged_path = self.queue_dir.join(QUEUE_COMPACTION_STAGE);
        durable_file::remove_file_if_exists_and_sync_parent(&staged_path).map_err(
            storage_error("remove stale Cloud Files queue compaction stage"),
        )?;
        let mut staged = durable_file::create_private_staged_file(&staged_path)
            .map_err(storage_error("create Cloud Files queue compaction stage"))?;
        if let Err(source) = staged
            .write_all(&compacted)
            .and_then(|()| staged.sync_all())
        {
            drop(staged);
            let _ = durable_file::remove_file_if_exists_and_sync_parent(&staged_path);
            return Err(io_error(
                "write and fsync Cloud Files queue compaction stage",
            )(source));
        }
        drop(staged);

        drop(self.journal.take());
        if let Err(source) =
            durable_file::install_staged_file(&staged_path, &self.journal_path, true)
        {
            self.usable = false;
            return Err(storage_error("install compacted Cloud Files queue journal")(source));
        }
        let reopened = match durable_file::open_file_for_append(&self.journal_path) {
            Ok(reopened) => reopened.file,
            Err(source) => {
                self.usable = false;
                return Err(storage_error("reopen compacted Cloud Files queue journal")(
                    source,
                ));
            }
        };
        self.journal = Some(reopened);
        self.next_sequence = sequence;
        self.journal_record_count = operations.len() as u64;
        self.operations = operations;
        Ok(())
    }

    fn journal_file(&self) -> Result<&File, CloudFilesLocalChangeQueueError> {
        self.journal.as_ref().ok_or_else(|| {
            queue_error(
                "queue-reopen-required",
                "Cloud Files queue journal is not open",
            )
        })
    }

    fn ensure_usable(&self) -> Result<(), CloudFilesLocalChangeQueueError> {
        if self.usable {
            Ok(())
        } else {
            Err(queue_error(
                "queue-reopen-required",
                "a prior durable queue write was uncertain; reopen before continuing",
            ))
        }
    }

    fn load_reconciled_retirement_ledger(&mut self) -> Result<(), CloudFilesLocalChangeQueueError> {
        if !self.retired_ledger_path.exists() {
            self.retired_ledger = ReconciledRetirementLedger::default();
            return Ok(());
        }
        let bytes = fs::read(&self.retired_ledger_path)
            .map_err(io_error("read Cloud Files reconciled-retirement ledger"))?;
        let snapshot = decode_sobj::<ReconciledRetirementLedgerSnapshot>(&bytes).map_err(
            sobj_error("decode Cloud Files reconciled-retirement ledger"),
        )?;
        if snapshot.schema_version != QUEUE_RETIRED_LEDGER_SCHEMA_VERSION {
            return Err(queue_error(
                "unsupported-retired-ledger-schema",
                format!(
                    "retired ledger schema {} is unsupported",
                    snapshot.schema_version
                ),
            ));
        }
        if snapshot.tombstones.len() > QUEUE_RETIRED_TOMBSTONE_LIMIT {
            return Err(queue_error(
                "retired-ledger-too-large",
                format!(
                    "retired ledger contains {} tombstones; max is {QUEUE_RETIRED_TOMBSTONE_LIMIT}",
                    snapshot.tombstones.len()
                ),
            ));
        }
        let mut ledger = ReconciledRetirementLedger {
            next_ordinal: snapshot.next_ordinal,
            ..ReconciledRetirementLedger::default()
        };
        let mut previous_ordinal = 0_u64;
        for tombstone in snapshot.tombstones {
            if tombstone.operation_id.trim().is_empty()
                || tombstone.operation_id.len() > 256
                || tombstone
                    .request_digest
                    .strip_prefix(BLOB_HASH_PREFIX)
                    .is_none()
                || tombstone.ordinal <= previous_ordinal
                || tombstone.ordinal >= ledger.next_ordinal
            {
                return Err(queue_error(
                    "invalid-retired-ledger",
                    format!(
                        "retired operation `{}` has invalid identity, digest, or ordinal",
                        tombstone.operation_id
                    ),
                ));
            }
            if let Some(active) = self.operations.get(&tombstone.operation_id)
                && active.request_digest != tombstone.request_digest
            {
                return Err(queue_error(
                    "operation-id-reused",
                    format!(
                        "active operation `{}` conflicts with its reconciled tombstone",
                        tombstone.operation_id
                    ),
                ));
            }
            previous_ordinal = tombstone.ordinal;
            ledger.order.push_back(tombstone.operation_id.clone());
            if ledger
                .tombstones
                .insert(tombstone.operation_id.clone(), tombstone)
                .is_some()
            {
                return Err(queue_error(
                    "invalid-retired-ledger",
                    "reconciled-retirement ledger contains a duplicate operation ID",
                ));
            }
        }
        self.retired_ledger = ledger;
        Ok(())
    }

    fn persist_reconciled_retirement_ledger(
        &mut self,
        ledger: &ReconciledRetirementLedger,
    ) -> Result<(), CloudFilesLocalChangeQueueError> {
        let snapshot = ReconciledRetirementLedgerSnapshot {
            schema_version: QUEUE_RETIRED_LEDGER_SCHEMA_VERSION,
            next_ordinal: ledger.next_ordinal,
            tombstones: ledger
                .order
                .iter()
                .filter_map(|operation_id| ledger.tombstones.get(operation_id).cloned())
                .collect(),
        };
        let bytes = encode_sobj(&snapshot).map_err(sobj_error(
            "encode Cloud Files reconciled-retirement ledger",
        ))?;
        if bytes.len() as u64 > QUEUE_RETIRED_LEDGER_MAX_BYTES {
            return Err(queue_error(
                "retired-ledger-too-large",
                format!(
                    "encoded retired ledger is {} bytes; max is {QUEUE_RETIRED_LEDGER_MAX_BYTES}",
                    bytes.len()
                ),
            ));
        }
        if let Err(source) = durable_file::write_file_atomic(
            &self.retired_ledger_path,
            &bytes,
            DurableAtomicFileOptions::new(QUEUE_RETIRED_LEDGER, QUEUE_RETIRED_LEDGER_TEMP_LABEL)
                .sync_all(),
        ) {
            self.usable = false;
            return Err(storage_error(
                "persist Cloud Files reconciled-retirement ledger",
            )(source));
        }
        Ok(())
    }

    fn replay_and_repair_torn_tail(&mut self) -> Result<(), CloudFilesLocalChangeQueueError> {
        let mut journal = self.journal.take().ok_or_else(|| {
            queue_error(
                "queue-reopen-required",
                "Cloud Files queue journal is not open",
            )
        })?;
        journal
            .seek(SeekFrom::Start(0))
            .map_err(io_error("rewind Cloud Files queue journal"))?;
        let mut expected_sequence = 1_u64;
        let mut last_valid_offset = 0_u64;
        loop {
            let record = match durable_file::read_length_prefixed_record(
                &mut journal,
                &self.journal_path,
                DurableRecordReadOptions::new(DurableLengthEndian::Little)
                    .max_record_bytes(QUEUE_RECORD_MAX_BYTES),
            ) {
                Ok(record) => record,
                Err(durable_file::DurableFileError::RecordTooLarge { record_bytes, .. }) => {
                    let file_len = journal
                        .metadata()
                        .map_err(io_error("inspect Cloud Files queue journal"))?
                        .len();
                    let declared_end = last_valid_offset
                        .saturating_add(4)
                        .saturating_add(u64::try_from(record_bytes).unwrap_or(u64::MAX));
                    if declared_end <= file_len {
                        return Err(queue_error(
                            "queue-record-too-large",
                            format!(
                                "complete queue record at offset {last_valid_offset} declares {record_bytes} bytes; max is {QUEUE_RECORD_MAX_BYTES}"
                            ),
                        ));
                    }
                    durable_file::truncate_open_file_and_sync(
                        &mut journal,
                        &self.journal_path,
                        last_valid_offset,
                        DurableFileSync::All,
                    )
                    .map_err(storage_error("truncate torn Cloud Files queue header"))?;
                    break;
                }
                Err(error) => {
                    return Err(storage_error("replay Cloud Files queue journal")(error));
                }
            };
            let Some(record) = record else {
                let file_len = journal
                    .metadata()
                    .map_err(io_error("inspect Cloud Files queue journal"))?
                    .len();
                if last_valid_offset < file_len {
                    durable_file::truncate_open_file_and_sync(
                        &mut journal,
                        &self.journal_path,
                        last_valid_offset,
                        DurableFileSync::All,
                    )
                    .map_err(storage_error("truncate torn Cloud Files queue tail"))?;
                }
                break;
            };
            let decoded = decode_sobj::<QueueJournalRecord>(&record.bytes)
                .map_err(sobj_error("decode complete Cloud Files queue record"))?;
            if decoded.body.schema_version != QUEUE_SCHEMA_VERSION {
                return Err(queue_error(
                    "unsupported-queue-schema",
                    format!(
                        "queue record schema {} is unsupported",
                        decoded.body.schema_version
                    ),
                ));
            }
            if decoded.body.sequence != expected_sequence {
                return Err(queue_error(
                    "queue-sequence-gap",
                    format!(
                        "expected queue sequence {expected_sequence}, observed {}",
                        decoded.body.sequence
                    ),
                ));
            }
            if decoded.checksum != journal_body_checksum(&decoded.body)? {
                return Err(queue_error(
                    "queue-record-checksum",
                    format!(
                        "complete queue record {} failed checksum validation",
                        decoded.body.sequence
                    ),
                ));
            }
            validate_operation_integrity(&decoded.body.operation)?;
            if decoded.body.operation.request.mount != self.mount
                || decoded.body.operation.request.root != self.root
            {
                return Err(queue_error(
                    "queue-scope-mismatch",
                    format!(
                        "replayed operation `{}` belongs to mount/root `{}` `{}`, not `{}` `{}`",
                        decoded.body.operation.request.operation_id,
                        decoded.body.operation.request.mount,
                        decoded.body.operation.request.root,
                        self.mount,
                        self.root
                    ),
                ));
            }
            if let Some(previous) = self
                .operations
                .get(&decoded.body.operation.request.operation_id)
                && previous.request_digest != decoded.body.operation.request_digest
            {
                return Err(queue_error(
                    "operation-id-reused",
                    format!(
                        "replayed operation ID `{}` has conflicting immutable digests",
                        decoded.body.operation.request.operation_id
                    ),
                ));
            }
            self.operations.insert(
                decoded.body.operation.request.operation_id.clone(),
                decoded.body.operation,
            );
            expected_sequence = expected_sequence.saturating_add(1);
            last_valid_offset = record.record_end;
        }
        self.next_sequence = expected_sequence;
        self.journal_record_count = expected_sequence.saturating_sub(1);
        journal
            .seek(SeekFrom::End(0))
            .map_err(io_error("position Cloud Files queue journal for append"))?;
        self.journal = Some(journal);
        Ok(())
    }

    fn requeue_ambiguous_operations(&mut self) -> Result<(), CloudFilesLocalChangeQueueError> {
        let ambiguous = self
            .operations
            .values()
            .filter(|operation| operation.status == CloudFilesLocalOperationStatus::InFlight)
            .map(|operation| operation.request.operation_id.clone())
            .collect::<Vec<_>>();
        for operation_id in ambiguous {
            self.transition(
                &operation_id,
                CloudFilesLocalOperationStatus::Retrying,
                None,
                Some(
                    "recovered an ambiguous in-flight operation; retry only with the same stable operation ID and staged bytes"
                        .to_string(),
                ),
            )?;
        }
        Ok(())
    }

    fn blob_path(
        &self,
        content: &CloudFilesStagedContent,
    ) -> Result<PathBuf, CloudFilesLocalChangeQueueError> {
        blob_path_in_dir(&self.blobs_dir, content)
    }
}

/// Read-only admission check for a brand-new v3 registration. `open` is not
/// usable here because it creates directories, repairs torn tails, and
/// requeues ambiguous operations. Any pre-existing exact scope is retained as
/// evidence and therefore blocks Fresh activation for a separate migration or
/// operator decision.
pub(crate) fn fresh_local_change_scope_is_absent(
    data_dir: &SinomoDataDir,
    mount: &str,
    root: &str,
) -> Result<bool, CloudFilesLocalChangeQueueError> {
    if mount.trim().is_empty() {
        return Err(queue_error(
            "invalid-mount",
            "mount identity cannot be empty",
        ));
    }
    let root = canonical_sifs_path(root)?;
    let scope = data_dir
        .module_runtime_dir()
        .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
        .join(QUEUE_DIR)
        .join(mount_scope_directory(mount, &root)?);
    match fs::symlink_metadata(&scope) {
        Ok(_) => Ok(false),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(source) => Err(io_error("inspect Cloud Files local-change scope")(source)),
    }
}

/// Exact raw evidence accepted by the explicit cursor-only recovery. Unlike
/// [`CloudFilesLocalChangeQueue::open`], this inspector never creates a scope,
/// lock, journal, or blob directory and never repairs or requeues anything.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct CloudFilesCursorOnlyQueueInspection {
    pub scope_present: bool,
    pub raw_files: BTreeMap<String, Vec<u8>>,
}

pub(crate) struct CloudFilesCursorOnlyQueueInspectionGuard {
    inspection: CloudFilesCursorOnlyQueueInspection,
    _lock: Option<File>,
}

impl CloudFilesCursorOnlyQueueInspectionGuard {
    pub(crate) fn inspection(&self) -> &CloudFilesCursorOnlyQueueInspection {
        &self.inspection
    }
}

pub(crate) fn inspect_cursor_only_local_change_queue(
    data_dir: &SinomoDataDir,
    mount: &str,
    root: &str,
) -> Result<CloudFilesCursorOnlyQueueInspectionGuard, CloudFilesLocalChangeQueueError> {
    if mount.trim().is_empty() {
        return Err(queue_error(
            "invalid-mount",
            "mount identity cannot be empty",
        ));
    }
    let root = canonical_sifs_path(root)?;
    let module_runtime = data_dir.module_runtime_dir();
    let module_dir = module_runtime.join(SINOMO_DFS_CLOUD_FILES_MODULE_ID);
    let queue_root = module_dir.join(QUEUE_DIR);
    let scope_name = mount_scope_directory(mount, &root)?;
    let scope = queue_root.join(&scope_name);
    for (label, path) in [
        ("module runtime", module_runtime.as_path()),
        ("Cloud Files module", module_dir.as_path()),
        ("local-change queue", queue_root.as_path()),
    ] {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_dir() && !metadata_is_reparse(&metadata) => {}
            Ok(_) => {
                return Err(queue_error(
                    "cursor-recovery-queue-topology",
                    format!("{label} path {} is not a plain directory", path.display()),
                ));
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                if label == "local-change queue" {
                    return Ok(CloudFilesCursorOnlyQueueInspectionGuard {
                        inspection: CloudFilesCursorOnlyQueueInspection::default(),
                        _lock: None,
                    });
                }
                return Err(io_error("inspect cursor-only queue parent")(source));
            }
            Err(source) => return Err(io_error("inspect cursor-only queue parent")(source)),
        }
    }
    let metadata = match fs::symlink_metadata(&scope) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            if fs::symlink_metadata(&queue_root).is_ok() {
                return Err(queue_error(
                    "cursor-recovery-queue-topology",
                    "local-change queue root exists without the exact mount/root scope",
                ));
            }
            return Ok(CloudFilesCursorOnlyQueueInspectionGuard {
                inspection: CloudFilesCursorOnlyQueueInspection::default(),
                _lock: None,
            });
        }
        Err(source) => return Err(io_error("inspect cursor-only queue scope")(source)),
    };
    if !metadata.file_type().is_dir() || metadata_is_reparse(&metadata) {
        return Err(queue_error(
            "cursor-recovery-queue-topology",
            format!("queue scope {} is not a plain directory", scope.display()),
        ));
    }
    for entry in fs::read_dir(&queue_root).map_err(io_error("list local-change queue root"))? {
        let entry = entry.map_err(io_error("read local-change queue root entry"))?;
        let name = entry.file_name().into_string().map_err(|_| {
            queue_error(
                "cursor-recovery-queue-topology",
                "local-change queue root contains a non-UTF-8 entry",
            )
        })?;
        let metadata = fs::symlink_metadata(entry.path())
            .map_err(io_error("inspect local-change queue root entry"))?;
        if name != scope_name || !metadata.file_type().is_dir() || metadata_is_reparse(&metadata) {
            return Err(queue_error(
                "cursor-recovery-queue-topology",
                format!("unexpected local-change queue root entry `{name}`"),
            ));
        }
    }

    let lock_path = scope.join(QUEUE_LOCK);
    let lock_metadata = fs::symlink_metadata(&lock_path)
        .map_err(io_error("inspect pre-existing cursor-only queue lock"))?;
    if !lock_metadata.file_type().is_file() || metadata_is_reparse(&lock_metadata) {
        return Err(queue_error(
            "cursor-recovery-queue-lock",
            "queue lock is not a plain pre-existing file",
        ));
    }
    let queue_lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(&lock_path)
        .map_err(io_error("open pre-existing cursor-only queue lock"))?;
    queue_lock.try_lock_exclusive().map_err(|source| {
        queue_error(
            "cursor-recovery-queue-live",
            format!("local-change queue is live: {source}"),
        )
    })?;

    let mut raw_files = BTreeMap::new();
    let mut saw_journal = false;
    let mut saw_lock = false;
    let mut saw_blobs = false;
    for entry in fs::read_dir(&scope).map_err(io_error("list cursor-only queue scope"))? {
        let entry = entry.map_err(io_error("read cursor-only queue scope entry"))?;
        let path = entry.path();
        let name = entry.file_name().into_string().map_err(|_| {
            queue_error(
                "cursor-recovery-queue-topology",
                "queue scope contains a non-UTF-8 entry",
            )
        })?;
        let metadata =
            fs::symlink_metadata(&path).map_err(io_error("inspect cursor-only queue entry"))?;
        if metadata_is_reparse(&metadata) {
            return Err(queue_error(
                "cursor-recovery-queue-topology",
                format!("queue scope contains symlink/reparse evidence `{name}`"),
            ));
        }
        match name.as_str() {
            QUEUE_JOURNAL if metadata.file_type().is_file() => {
                let bytes = fs::read(&path).map_err(io_error("read cursor-only queue journal"))?;
                if !bytes.is_empty() {
                    return Err(queue_error(
                        "cursor-recovery-queue-not-empty",
                        "local-change journal contains operation evidence",
                    ));
                }
                raw_files.insert(name, bytes);
                saw_journal = true;
            }
            QUEUE_LOCK if metadata.file_type().is_file() => {
                // Windows enforces the byte-range lock against I/O through a
                // second handle, including handles opened by this process.
                // Reinspect the already-owned handle instead of reopening it.
                let lock_metadata = queue_lock
                    .metadata()
                    .map_err(io_error("inspect locked cursor-only queue lock"))?;
                if lock_metadata.len() != 0 {
                    return Err(queue_error(
                        "cursor-recovery-queue-lock",
                        "queue lock contains unexpected state bytes",
                    ));
                }
                raw_files.insert(name, Vec::new());
                saw_lock = true;
            }
            QUEUE_BLOBS_DIR if metadata.file_type().is_dir() => {
                if fs::read_dir(&path)
                    .map_err(io_error("list cursor-only queue blobs"))?
                    .next()
                    .transpose()
                    .map_err(io_error("read cursor-only queue blob entry"))?
                    .is_some()
                {
                    return Err(queue_error(
                        "cursor-recovery-queue-not-empty",
                        "local-change blob directory contains retained content",
                    ));
                }
                saw_blobs = true;
            }
            QUEUE_RETIRED_LEDGER if metadata.file_type().is_file() => {
                let bytes = fs::read(&path)
                    .map_err(io_error("read cursor-only retired-operation ledger"))?;
                if bytes.len() as u64 > QUEUE_RETIRED_LEDGER_MAX_BYTES {
                    return Err(queue_error(
                        "cursor-recovery-queue-ledger",
                        "retired-operation ledger exceeds its bound",
                    ));
                }
                let ledger: ReconciledRetirementLedgerSnapshot = decode_sobj(&bytes)
                    .map_err(sobj_error("decode cursor-only retired-operation ledger"))?;
                if ledger.schema_version != QUEUE_RETIRED_LEDGER_SCHEMA_VERSION
                    || !ledger.tombstones.is_empty()
                    || ledger.next_ordinal != 1
                    || encode_sobj(&ledger)
                        .map_err(sobj_error("re-encode cursor-only retired-operation ledger"))?
                        != bytes
                {
                    return Err(queue_error(
                        "cursor-recovery-queue-not-empty",
                        "retired-operation ledger contains history or is malformed",
                    ));
                }
                raw_files.insert(name, bytes);
            }
            _ => {
                return Err(queue_error(
                    "cursor-recovery-queue-topology",
                    format!("unexpected queue recovery entry `{name}`"),
                ));
            }
        }
    }
    if !saw_journal || !saw_lock || !saw_blobs {
        return Err(queue_error(
            "cursor-recovery-queue-topology",
            "existing queue scope is missing its canonical journal, lock, or blobs directory",
        ));
    }
    Ok(CloudFilesCursorOnlyQueueInspectionGuard {
        inspection: CloudFilesCursorOnlyQueueInspection {
            scope_present: true,
            raw_files,
        },
        _lock: Some(queue_lock),
    })
}

#[cfg(windows)]
fn metadata_is_reparse(metadata: &fs::Metadata) -> bool {
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
fn metadata_is_reparse(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

fn stage_reader_in_blobs_dir<R: Read>(
    blobs_dir: &Path,
    mut reader: R,
) -> Result<CloudFilesStagedContent, CloudFilesLocalChangeQueueError> {
    let sequence = STAGED_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let staged = blobs_dir.join(format!(".stage-{}-{sequence}", std::process::id()));
    let mut file = durable_file::create_private_staged_file(&staged)
        .map_err(storage_error("create Cloud Files staged-content file"))?;
    let streamed = stream_reader_to_writer(&mut reader, &mut file);
    let (hash, length) = match streamed.and_then(|result| {
        file.sync_all()?;
        Ok(result)
    }) {
        Ok(result) => result,
        Err(source) => {
            drop(file);
            let _ = fs::remove_file(&staged);
            return Err(io_error("stream and fsync Cloud Files staged content")(
                source,
            ));
        }
    };
    drop(file);

    let hash_hex = hash.to_hex().to_string();
    let content = CloudFilesStagedContent {
        content_hash: format!("{BLOB_HASH_PREFIX}{hash_hex}"),
        length,
    };
    let destination = blobs_dir.join(format!("{hash_hex}.blob"));
    match durable_file::install_staged_file(&staged, &destination, false) {
        Ok(()) => {}
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&staged);
        }
        Err(source) => {
            let _ = fs::remove_file(&staged);
            return Err(storage_error("publish Cloud Files staged content")(source));
        }
    }
    verify_blob(&destination, &content)?;
    Ok(content)
}

fn stream_reader_to_writer<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> std::io::Result<(blake3::Hash, u64)> {
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        length = length
            .checked_add(read as u64)
            .ok_or_else(|| std::io::Error::other("staged content length overflowed u64"))?;
        hasher.update(&buffer[..read]);
        writer.write_all(&buffer[..read])?;
    }
    Ok((hasher.finalize(), length))
}

fn blob_path_in_dir(
    blobs_dir: &Path,
    content: &CloudFilesStagedContent,
) -> Result<PathBuf, CloudFilesLocalChangeQueueError> {
    let hash_hex = content
        .content_hash
        .strip_prefix(BLOB_HASH_PREFIX)
        .filter(|hash| hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| {
            queue_error(
                "invalid-content-hash",
                format!(
                    "staged content hash `{}` is not canonical BLAKE3",
                    content.content_hash
                ),
            )
        })?;
    Ok(blobs_dir.join(format!("{hash_hex}.blob")))
}

fn validate_operation_integrity(
    operation: &CloudFilesLocalOperation,
) -> Result<(), CloudFilesLocalChangeQueueError> {
    let digest = request_digest(&operation.request)?;
    if digest != operation.request_digest {
        return Err(queue_error(
            "operation-request-digest",
            format!(
                "operation `{}` request digest does not match its immutable request",
                operation.request.operation_id
            ),
        ));
    }
    if operation
        .remote_evidence
        .as_ref()
        .is_some_and(|evidence| evidence.operation_id != operation.request.operation_id)
    {
        return Err(queue_error(
            "operation-remote-evidence",
            format!(
                "operation `{}` carries remote evidence for a different operation ID",
                operation.request.operation_id
            ),
        ));
    }
    Ok(())
}

fn request_digest(
    request: &CloudFilesLocalOperationRequest,
) -> Result<String, CloudFilesLocalChangeQueueError> {
    let bytes = encode_sobj(request).map_err(sobj_error("encode operation request digest"))?;
    Ok(format!(
        "{BLOB_HASH_PREFIX}{}",
        blake3::hash(&bytes).to_hex()
    ))
}

fn journal_body_checksum(
    body: &QueueJournalRecordBody,
) -> Result<String, CloudFilesLocalChangeQueueError> {
    let bytes = encode_sobj(body).map_err(sobj_error("encode queue record checksum"))?;
    Ok(format!(
        "{BLOB_HASH_PREFIX}{}",
        blake3::hash(&bytes).to_hex()
    ))
}

fn mount_scope_directory(
    mount: &str,
    root: &str,
) -> Result<String, CloudFilesLocalChangeQueueError> {
    let identity = encode_sobj(&(mount, root)).map_err(sobj_error("encode mount scope"))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(MOUNT_SCOPE_DOMAIN);
    hasher.update(&identity);
    Ok(format!("scope-{}", hasher.finalize().to_hex()))
}

fn verify_blob(
    path: &Path,
    expected: &CloudFilesStagedContent,
) -> Result<(), CloudFilesLocalChangeQueueError> {
    let (hash, length) = durable_file::file_blake3_hash_and_size(path)
        .map_err(storage_error("verify Cloud Files staged content"))?;
    let observed_hash = format!("{BLOB_HASH_PREFIX}{}", hash.to_hex());
    if observed_hash != expected.content_hash || length != expected.length {
        return Err(queue_error(
            "staged-content-mismatch",
            format!(
                "staged blob {} is {observed_hash}/{length}, expected {}/{}",
                path.display(),
                expected.content_hash,
                expected.length
            ),
        ));
    }
    Ok(())
}

fn canonical_sifs_path(path: &str) -> Result<String, CloudFilesLocalChangeQueueError> {
    let path = path.trim();
    if path.is_empty() || !path.starts_with('/') || path.contains('\0') {
        return Err(queue_error(
            "invalid-path",
            format!("SIFS path `{path}` must be absolute, nonempty, and NUL-free"),
        ));
    }
    let mut components = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                if components.pop().is_none() {
                    return Err(queue_error(
                        "invalid-path",
                        format!("SIFS path `{path}` escapes its namespace root"),
                    ));
                }
            }
            component => components.push(component),
        }
    }
    if components.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", components.join("/")))
    }
}

fn ensure_within_root(path: &str, root: &str) -> Result<(), CloudFilesLocalChangeQueueError> {
    if root == "/" || path == root || path.starts_with(&format!("{root}/")) {
        return Ok(());
    }
    Err(queue_error(
        "path-outside-root",
        format!("SIFS path `{path}` is outside queue root `{root}`"),
    ))
}

fn queue_error(code: &'static str, detail: impl Into<String>) -> CloudFilesLocalChangeQueueError {
    CloudFilesLocalChangeQueueError {
        code,
        detail: detail.into(),
    }
}

fn io_error(
    operation: &'static str,
) -> impl FnOnce(std::io::Error) -> CloudFilesLocalChangeQueueError {
    move |source| queue_error("queue-io", format!("{operation}: {source}"))
}

fn storage_error(
    operation: &'static str,
) -> impl FnOnce(durable_file::DurableFileError) -> CloudFilesLocalChangeQueueError {
    move |source| queue_error("queue-storage", format!("{operation}: {source}"))
}

fn sobj_error(
    operation: &'static str,
) -> impl FnOnce(sinomo_module_api::NativeModuleApiError) -> CloudFilesLocalChangeQueueError {
    move |source| queue_error("queue-sobj", format!("{operation}: {source}"))
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{self, Read};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(_name: &str) -> Self {
            let suffix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            // Queue fixtures create nested immutable-blob and atomic temp
            // paths. Keep the root portable to legacy Windows MAX_PATH hosts.
            let path = std::env::temp_dir().join(format!("cfq-{}-{suffix}", std::process::id()));
            fs::create_dir_all(&path).expect("test root");
            Self(path)
        }

        fn data_dir(&self) -> SinomoDataDir {
            SinomoDataDir::new(&self.0)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn queue(root: &TestRoot) -> CloudFilesLocalChangeQueue {
        CloudFilesLocalChangeQueue::open(&root.data_dir(), "mount-a", "/services")
            .expect("open queue")
    }

    fn snapshot_tree(root: &Path) -> BTreeMap<String, Option<Vec<u8>>> {
        fn relative_name(root: &Path, path: &Path) -> String {
            path.strip_prefix(root)
                .expect("path beneath snapshot root")
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/")
        }

        fn visit(root: &Path, directory: &Path, snapshot: &mut BTreeMap<String, Option<Vec<u8>>>) {
            let mut entries = fs::read_dir(directory)
                .expect("read snapshot directory")
                .map(|entry| entry.expect("read snapshot entry").path())
                .collect::<Vec<_>>();
            entries.sort();
            for path in entries {
                let metadata = fs::symlink_metadata(&path).expect("snapshot metadata");
                let relative = relative_name(root, &path);
                if metadata.file_type().is_dir() {
                    snapshot.insert(format!("{relative}/"), None);
                    visit(root, &path, snapshot);
                } else {
                    snapshot.insert(relative, Some(fs::read(&path).expect("snapshot file")));
                }
            }
        }

        let mut snapshot = BTreeMap::new();
        visit(root, root, &mut snapshot);
        snapshot
    }

    fn canonical_queue_scope(root: &TestRoot) -> PathBuf {
        root.data_dir()
            .module_runtime_dir()
            .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
            .join(QUEUE_DIR)
            .join(mount_scope_directory("mount-a", "/services").expect("queue scope name"))
    }

    #[test]
    fn fresh_scope_probe_is_read_only_and_rejects_even_an_empty_scope() {
        let root = TestRoot::new("fresh-scope-probe");
        let data_dir = root.data_dir();
        let queue_root = data_dir
            .module_runtime_dir()
            .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
            .join(QUEUE_DIR);

        assert!(
            fresh_local_change_scope_is_absent(&data_dir, "mount-a", "/services")
                .expect("probe absent scope")
        );
        assert!(
            !queue_root.exists(),
            "read-only Fresh admission must not create queue directories"
        );

        let scope = queue_root
            .join(mount_scope_directory("mount-a", "/services").expect("exact queue scope name"));
        fs::create_dir_all(&scope).expect("empty retained queue scope");
        assert!(
            !fresh_local_change_scope_is_absent(&data_dir, "mount-a", "/services")
                .expect("probe retained scope")
        );
        assert!(scope.exists(), "probe must retain existing evidence");
    }

    #[test]
    fn cursor_only_queue_inspection_is_byte_read_only_for_canonical_empty_state() {
        let root = TestRoot::new("cursor-only-inspection-read-only");
        drop(queue(&root));
        let scope = canonical_queue_scope(&root);
        let before = snapshot_tree(&scope);

        let guard =
            inspect_cursor_only_local_change_queue(&root.data_dir(), "mount-a", "/services")
                .expect("inspect canonical empty queue");
        assert!(guard.inspection().scope_present);
        assert_eq!(
            guard.inspection().raw_files.get(QUEUE_JOURNAL),
            Some(&vec![])
        );
        assert_eq!(guard.inspection().raw_files.get(QUEUE_LOCK), Some(&vec![]));
        drop(guard);
        assert_eq!(snapshot_tree(&scope), before);

        let ledger = ReconciledRetirementLedgerSnapshot {
            schema_version: QUEUE_RETIRED_LEDGER_SCHEMA_VERSION,
            next_ordinal: 1,
            tombstones: Vec::new(),
        };
        fs::write(
            scope.join(QUEUE_RETIRED_LEDGER),
            encode_sobj(&ledger).expect("encode canonical empty ledger"),
        )
        .expect("write canonical empty ledger");
        let with_ledger_before = snapshot_tree(&scope);
        let guard =
            inspect_cursor_only_local_change_queue(&root.data_dir(), "mount-a", "/services")
                .expect("inspect canonical empty queue with ledger");
        assert!(
            guard
                .inspection()
                .raw_files
                .contains_key(QUEUE_RETIRED_LEDGER)
        );
        drop(guard);
        assert_eq!(snapshot_tree(&scope), with_ledger_before);
    }

    #[test]
    fn cursor_only_queue_inspection_rejects_history_and_unknown_topology_without_mutation() {
        let root = TestRoot::new("cursor-only-inspection-ledger-history");
        drop(queue(&root));
        let scope = canonical_queue_scope(&root);
        let historical = ReconciledRetirementLedgerSnapshot {
            schema_version: QUEUE_RETIRED_LEDGER_SCHEMA_VERSION,
            next_ordinal: 2,
            tombstones: Vec::new(),
        };
        fs::write(
            scope.join(QUEUE_RETIRED_LEDGER),
            encode_sobj(&historical).expect("encode historical ledger"),
        )
        .expect("write historical ledger");
        let historical_before = snapshot_tree(&scope);
        assert!(
            inspect_cursor_only_local_change_queue(&root.data_dir(), "mount-a", "/services",)
                .is_err()
        );
        assert_eq!(snapshot_tree(&scope), historical_before);

        let root = TestRoot::new("cursor-only-inspection-unknown-entry");
        drop(queue(&root));
        let scope = canonical_queue_scope(&root);
        fs::write(scope.join("operations.active.slog.stage"), b"torn evidence")
            .expect("write unknown queue evidence");
        let unknown_before = snapshot_tree(&scope);
        assert!(
            inspect_cursor_only_local_change_queue(&root.data_dir(), "mount-a", "/services",)
                .is_err()
        );
        assert_eq!(snapshot_tree(&scope), unknown_before);
    }

    #[test]
    fn authorized_cursor_recovery_transfers_the_exact_lock_without_repairing_evidence() {
        let root = TestRoot::new("cursor-only-authorized-lock-transfer");
        drop(queue(&root));
        let scope = canonical_queue_scope(&root);
        let before = snapshot_tree(&scope);
        let authorized = CloudFilesLocalChangeQueue::open_authorized_cursor_recovery(
            &root.data_dir(),
            "mount-a",
            "/services",
        )
        .expect("open canonical queue under the inspection lock");
        assert_eq!(snapshot_tree(&scope), before);
        assert_eq!(authorized.next_sequence, 1);
        assert_eq!(authorized.journal_record_count, 0);
        assert!(authorized.operations.is_empty());
        assert_eq!(authorized.retired_ledger.next_ordinal, 1);
        assert!(
            CloudFilesLocalChangeQueue::open(&root.data_dir(), "mount-a", "/services").is_err(),
            "the authorized queue must retain the same exclusive owner lock"
        );
        drop(authorized);
        assert_eq!(snapshot_tree(&scope), before);

        let root = TestRoot::new("cursor-only-authorized-does-not-repair");
        drop(queue(&root));
        let scope = canonical_queue_scope(&root);
        let compaction_stage = scope.join(QUEUE_COMPACTION_STAGE);
        fs::write(&compaction_stage, b"ambiguous compacted operation evidence")
            .expect("write abandoned compaction evidence");
        let before = snapshot_tree(&scope);
        assert!(
            CloudFilesLocalChangeQueue::open_authorized_cursor_recovery(
                &root.data_dir(),
                "mount-a",
                "/services",
            )
            .is_err(),
            "authorized recovery must reject instead of deleting a compaction stage"
        );
        assert_eq!(snapshot_tree(&scope), before);
        assert_eq!(
            fs::read(compaction_stage).expect("retained compaction evidence"),
            b"ambiguous compacted operation evidence"
        );
    }

    #[test]
    fn streaming_capture_exceeds_the_legacy_limit_with_bounded_memory() {
        struct GeneratedReader {
            remaining: u64,
            largest_buffer: usize,
        }

        impl Read for GeneratedReader {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                self.largest_buffer = self.largest_buffer.max(buffer.len());
                let read = usize::try_from(self.remaining.min(buffer.len() as u64))
                    .expect("bounded generated read");
                buffer[..read].fill(0x5a);
                self.remaining -= read as u64;
                Ok(read)
            }
        }

        let length = 256_u64 * 1024 * 1024 + 1;
        let mut reader = GeneratedReader {
            remaining: length,
            largest_buffer: 0,
        };
        let (hash, observed_length) =
            stream_reader_to_writer(&mut reader, &mut io::sink()).expect("stream generated file");
        assert_eq!(observed_length, length);
        assert_ne!(hash, blake3::hash(&[]));
        assert!(reader.largest_buffer <= 64 * 1024);
    }

    fn write_request(
        operation_id: &str,
        content: CloudFilesStagedContent,
    ) -> CloudFilesLocalOperationRequest {
        CloudFilesLocalOperationRequest {
            operation_id: operation_id.to_string(),
            capture_id: None,
            mount: "mount-a".to_string(),
            root: "/services".to_string(),
            kind: CloudFilesLocalOperationKind::ReplaceFile,
            source_path: "/services/report.txt".to_string(),
            destination_path: None,
            preconditions: CloudFilesLocalOperationPreconditions {
                source: Some(CloudFilesExpectedObjectState {
                    object_id: "01object".to_string(),
                    version: "01version".to_string(),
                    content_hash: Some("blake3:old".to_string()),
                }),
                destination: CloudFilesDestinationPrecondition::Unspecified,
            },
            staged_content: Some(content),
        }
    }

    fn directory_request(operation_id: &str, path: String) -> CloudFilesLocalOperationRequest {
        CloudFilesLocalOperationRequest {
            operation_id: operation_id.to_string(),
            capture_id: None,
            mount: "mount-a".to_string(),
            root: "/services".to_string(),
            kind: CloudFilesLocalOperationKind::CreateDirectory,
            source_path: path,
            destination_path: None,
            preconditions: CloudFilesLocalOperationPreconditions::default(),
            staged_content: None,
        }
    }

    #[test]
    fn fsynced_enqueue_replays_with_same_operation_and_staged_bytes() {
        let root = TestRoot::new("fsync-replay");
        let mut first = queue(&root);
        let content = first.stage_content(b"immutable bytes").expect("stage");
        first
            .enqueue(write_request("op-fsync", content))
            .expect("enqueue");
        drop(first);

        let reopened = queue(&root);
        assert_eq!(
            reopened.operation("op-fsync").expect("replayed").status,
            CloudFilesLocalOperationStatus::Queued
        );
        assert_eq!(
            reopened.read_staged_content("op-fsync").expect("bytes"),
            b"immutable bytes"
        );
    }

    #[test]
    fn only_one_writer_can_own_a_mount_queue() {
        let root = TestRoot::new("single-writer");
        let first = queue(&root);
        let error = CloudFilesLocalChangeQueue::open(&root.data_dir(), "mount-a", "/services")
            .err()
            .expect("second writer must fail");
        assert_eq!(error.code, "queue-busy");
        drop(first);
        queue(&root);
    }

    #[test]
    fn stable_id_is_idempotent_but_cannot_name_different_content() {
        let root = TestRoot::new("stable-id");
        let mut queue = queue(&root);
        let alpha = queue.stage_content(b"alpha").expect("alpha");
        let request = write_request("op-stable", alpha);
        let first = queue.enqueue(request.clone()).expect("first enqueue");
        let second = queue.enqueue(request).expect("idempotent enqueue");
        assert_eq!(first, second);

        let beta = queue.stage_content(b"beta").expect("beta");
        let error = queue
            .enqueue(write_request("op-stable", beta))
            .expect_err("same ID cannot change bytes");
        assert_eq!(error.code, "operation-id-reused");
    }

    #[test]
    fn torn_tail_is_truncated_before_a_later_durable_append() {
        let root = TestRoot::new("torn-tail");
        let mut first = queue(&root);
        let alpha = first.stage_content(b"alpha").expect("alpha");
        first
            .enqueue(write_request("op-alpha", alpha))
            .expect("enqueue alpha");
        let journal_path = first.journal_path().to_path_buf();
        drop(first);

        let mut journal = OpenOptions::new()
            .append(true)
            .open(&journal_path)
            .expect("append torn tail");
        journal.write_all(&64_u32.to_le_bytes()).expect("length");
        journal.write_all(b"torn").expect("partial record");
        journal.sync_all().expect("durable torn tail");
        drop(journal);

        let mut recovered = queue(&root);
        assert!(recovered.operation("op-alpha").is_some());
        let beta = recovered.stage_content(b"beta").expect("beta");
        recovered
            .enqueue(write_request("op-beta", beta))
            .expect("append after recovery");
        drop(recovered);

        let final_queue = queue(&root);
        assert!(final_queue.operation("op-alpha").is_some());
        assert!(final_queue.operation("op-beta").is_some());
    }

    #[test]
    fn torn_length_header_is_truncated_and_fsynced() {
        let root = TestRoot::new("torn-header");
        let mut first = queue(&root);
        let content = first.stage_content(b"header").expect("stage");
        first
            .enqueue(write_request("op-header", content))
            .expect("enqueue");
        let journal_path = first.journal_path().to_path_buf();
        drop(first);
        let valid_len = fs::metadata(&journal_path).expect("journal metadata").len();

        let mut journal = OpenOptions::new()
            .append(true)
            .open(&journal_path)
            .expect("append torn header");
        journal.write_all(&[0xff, 0xff]).expect("partial header");
        journal.sync_all().expect("sync torn header");
        drop(journal);

        let recovered = queue(&root);
        assert!(recovered.operation("op-header").is_some());
        drop(recovered);
        assert_eq!(
            fs::metadata(&journal_path).expect("journal metadata").len(),
            valid_len
        );
    }

    #[test]
    fn complete_corrupt_record_fails_closed_instead_of_being_truncated() {
        let root = TestRoot::new("complete-corrupt");
        let mut first = queue(&root);
        let content = first.stage_content(b"corrupt me").expect("stage");
        first
            .enqueue(write_request("op-corrupt", content))
            .expect("enqueue");
        let journal_path = first.journal_path().to_path_buf();
        drop(first);

        let mut journal = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&journal_path)
            .expect("open journal");
        let last = journal.metadata().expect("metadata").len() - 1;
        journal.seek(SeekFrom::Start(last)).expect("seek read");
        let mut byte = [0_u8; 1];
        journal.read_exact(&mut byte).expect("read byte");
        byte[0] ^= 0x5a;
        journal.seek(SeekFrom::Start(last)).expect("seek write");
        journal.write_all(&byte).expect("corrupt byte");
        journal.sync_all().expect("sync corruption");
        drop(journal);

        let error = CloudFilesLocalChangeQueue::open(&root.data_dir(), "mount-a", "/services")
            .err()
            .expect("complete corruption must fail closed");
        assert!(matches!(error.code, "queue-sobj" | "queue-record-checksum"));
    }

    #[test]
    fn ambiguous_in_flight_operation_replays_as_retry_with_same_id_and_blob() {
        let root = TestRoot::new("ambiguous-retry");
        let mut first = queue(&root);
        let content = first.stage_content(b"request body").expect("stage");
        first
            .enqueue(write_request("op-ambiguous", content))
            .expect("enqueue");
        first
            .mark_in_flight("op-ambiguous")
            .expect("mark in flight");
        drop(first);

        let recovered = queue(&root);
        let operation = recovered.operation("op-ambiguous").expect("operation");
        assert_eq!(operation.status, CloudFilesLocalOperationStatus::Retrying);
        assert_eq!(operation.attempts, 1);
        assert!(
            operation
                .last_error
                .as_deref()
                .is_some_and(|detail| detail.contains("same stable operation ID"))
        );
        assert_eq!(
            recovered
                .read_staged_content("op-ambiguous")
                .expect("staged bytes"),
            b"request body"
        );
    }

    #[test]
    fn rename_is_one_canonical_operation_with_source_and_destination_fences() {
        let root = TestRoot::new("rename");
        let mut queue = queue(&root);
        let request = CloudFilesLocalOperationRequest {
            operation_id: "op-rename".to_string(),
            capture_id: None,
            mount: "mount-a".to_string(),
            root: "/services/./".to_string(),
            kind: CloudFilesLocalOperationKind::Rename,
            source_path: "/services/old/../old.txt".to_string(),
            destination_path: Some("/services/new.txt".to_string()),
            preconditions: CloudFilesLocalOperationPreconditions {
                source: Some(CloudFilesExpectedObjectState {
                    object_id: "01object".to_string(),
                    version: "01version".to_string(),
                    content_hash: None,
                }),
                destination: CloudFilesDestinationPrecondition::Absent,
            },
            staged_content: None,
        };

        let operation = queue.enqueue(request).expect("enqueue rename");
        assert_eq!(operation.request.source_path, "/services/old.txt");
        assert_eq!(
            operation.request.destination_path.as_deref(),
            Some("/services/new.txt")
        );
        assert_eq!(operation.request.kind, CloudFilesLocalOperationKind::Rename);
        assert_eq!(queue.operations().count(), 1);
    }

    #[test]
    fn blobs_are_retained_for_retry_and_conflict_then_pruned_after_completion() {
        let root = TestRoot::new("blob-retention");
        let mut queue = queue(&root);
        let content = queue.stage_content(b"retain me").expect("stage");
        queue
            .enqueue(write_request("op-retain", content.clone()))
            .expect("enqueue");
        assert_eq!(queue.prune_released_blobs().expect("retain pending"), 0);
        queue
            .mark_conflict(
                "op-retain",
                CloudFilesConflictEvidence {
                    code: "version-changed".to_string(),
                    detail: "remote object advanced".to_string(),
                    observed_object_id: Some("01object".to_string()),
                    observed_version: Some("01newversion".to_string()),
                    observed_content_hash: Some("blake3:new".to_string()),
                },
            )
            .expect("conflict");
        assert_eq!(queue.prune_released_blobs().expect("retain conflict"), 0);
        assert_eq!(queue.summary().conflict_count, 1);
        queue.mark_completed("op-retain").expect("complete");
        assert_eq!(queue.prune_released_blobs().expect("prune"), 1);
        assert!(queue.read_staged_content("op-retain").is_err());
    }

    #[test]
    fn staged_bytes_do_not_follow_later_source_changes() {
        let root = TestRoot::new("immutable-stage");
        let source = root.0.join("source.txt");
        fs::write(&source, b"before").expect("source before");
        let mut queue = queue(&root);
        let content = queue
            .stage_content(&fs::read(&source).expect("read source"))
            .expect("stage source");
        queue
            .enqueue(write_request("op-source", content))
            .expect("enqueue");
        fs::write(&source, b"after").expect("source after");

        assert_eq!(
            queue.read_staged_content("op-source").expect("staged"),
            b"before"
        );
    }

    #[test]
    fn mount_and_root_replacements_use_isolated_durable_scopes() {
        let root = TestRoot::new("scope-isolation");
        let mut original = queue(&root);
        let original_dir = original.queue_dir().to_path_buf();
        let content = original.stage_content(b"original root").expect("stage");
        original
            .enqueue(write_request("op-original", content))
            .expect("enqueue");
        drop(original);

        let replacement =
            CloudFilesLocalChangeQueue::open(&root.data_dir(), "mount-a", "/replacement")
                .expect("open replacement root");
        assert_ne!(replacement.queue_dir(), original_dir);
        assert!(replacement.operation("op-original").is_none());
        drop(replacement);

        let moved_mount =
            CloudFilesLocalChangeQueue::open(&root.data_dir(), "mount-b", "/services")
                .expect("open replacement mount path");
        assert_ne!(moved_mount.queue_dir(), original_dir);
        assert!(moved_mount.operation("op-original").is_none());
        drop(moved_mount);

        let reopened = queue(&root);
        assert!(reopened.operation("op-original").is_some());
        assert_eq!(reopened.queue_dir(), original_dir);
    }

    #[test]
    fn bounded_churn_compacts_to_latest_records_and_restarts_cleanly() {
        let root = TestRoot::new("bounded-churn");
        let mut active = queue(&root);
        let content = active.stage_content(b"churn").expect("stage");
        active
            .enqueue(write_request("op-churn", content))
            .expect("enqueue");
        for attempt in 0..300 {
            active
                .mark_retrying("op-churn", format!("retry-{attempt}"))
                .expect("record retry");
        }
        assert!(active.journal_record_count < QUEUE_JOURNAL_COMPACT_MIN_RECORDS);
        assert!(
            fs::metadata(active.journal_path())
                .expect("journal metadata")
                .len()
                < 128 * 1024
        );
        drop(active);

        let reopened = queue(&root);
        let operation = reopened.operation("op-churn").expect("latest operation");
        assert_eq!(operation.status, CloudFilesLocalOperationStatus::Retrying);
        assert_eq!(operation.last_error.as_deref(), Some("retry-299"));
        assert_eq!(reopened.operations().count(), 1);
    }

    #[test]
    fn reconciled_high_churn_exceeds_journal_capacity_and_reopens_bounded() {
        let root = TestRoot::new("reconciled-high-churn");
        let mut active = queue(&root);
        let mut reconciled = Vec::new();
        const OPERATIONS: usize = 512;
        for index in 0..OPERATIONS {
            let operation_id = format!("op-reconciled-{index:05}");
            active
                .enqueue(directory_request(
                    &operation_id,
                    format!("/services/reconciled-{index:05}"),
                ))
                .expect("enqueue reconciled operation");
            active
                .mark_remote_applied_with_evidence(CloudFilesRemoteMutationEvidence {
                    operation_id: operation_id.clone(),
                    object_id: Some(format!("01object{index:05}")),
                    version: Some(format!("01version{index:05}")),
                    content_hash: None,
                    content_size_bytes: None,
                    kind: Some("directory".to_string()),
                    detail: "authoritative feed fixture".to_string(),
                })
                .expect("record remote evidence");
            active
                .mark_completed(&operation_id)
                .expect("mark completed");
            active
                .retire_terminal_operation(&operation_id)
                .expect("retire completed operation");
            reconciled.push(operation_id);
            if reconciled.len() == 16 {
                active
                    .acknowledge_reconciled_retirements(&reconciled)
                    .expect("acknowledge authoritative feed batch");
                reconciled.clear();
            }
        }
        assert!(reconciled.is_empty());
        assert!(active.operations.is_empty());
        assert_eq!(
            active.retired_ledger.tombstones.len(),
            QUEUE_RETIRED_TOMBSTONE_LIMIT
        );
        assert!(
            fs::metadata(active.journal_path())
                .expect("bounded journal metadata")
                .len()
                < QUEUE_JOURNAL_MAX_BYTES
        );
        drop(active);

        let mut reopened = queue(&root);
        assert!(reopened.operations.is_empty());
        assert_eq!(
            reopened.retired_ledger.tombstones.len(),
            QUEUE_RETIRED_TOMBSTONE_LIMIT
        );
        let newest = OPERATIONS - 1;
        let error = reopened
            .enqueue(directory_request(
                &format!("op-reconciled-{newest:05}"),
                format!("/services/reconciled-{newest:05}"),
            ))
            .expect_err("recent feed-reconciled ID remains fenced after restart");
        assert_eq!(error.code, "operation-already-reconciled");
    }

    #[test]
    fn crash_before_compaction_install_keeps_active_journal_authoritative() {
        let root = TestRoot::new("compaction-crash");
        let mut active = queue(&root);
        let content = active.stage_content(b"survive").expect("stage");
        active
            .enqueue(write_request("op-survive", content))
            .expect("enqueue");
        let active_len = fs::metadata(active.journal_path())
            .expect("active journal metadata")
            .len();
        let staged_path = active.queue_dir().join(QUEUE_COMPACTION_STAGE);
        let mut staged = durable_file::create_private_staged_file(&staged_path)
            .expect("create simulated compaction stage");
        staged
            .write_all(b"complete but not installed")
            .expect("write stage");
        staged.sync_all().expect("sync stage");
        drop(staged);
        drop(active);

        let reopened = queue(&root);
        assert!(reopened.operation("op-survive").is_some());
        assert_eq!(
            fs::metadata(reopened.journal_path())
                .expect("active journal metadata")
                .len(),
            active_len
        );
        assert!(!staged_path.exists());
    }

    #[test]
    fn terminal_retirement_rejects_pending_and_preserves_evidence_after_blob_prune() {
        let root = TestRoot::new("terminal-retirement");
        let mut active = queue(&root);
        let content = active.stage_content(b"retire me").expect("stage");
        active
            .enqueue(write_request("op-retire", content))
            .expect("enqueue");
        let error = active
            .retire_terminal_operation("op-retire")
            .expect_err("pending work must not retire");
        assert_eq!(error.code, "operation-not-terminal");
        assert_eq!(
            active.read_staged_content("op-retire").expect("retained"),
            b"retire me"
        );

        let evidence = CloudFilesConflictEvidence {
            code: "version-changed".to_string(),
            detail: "remote object advanced".to_string(),
            observed_object_id: Some("01object".to_string()),
            observed_version: Some("01newversion".to_string()),
            observed_content_hash: Some("blake3:new".to_string()),
        };
        active
            .mark_conflict("op-retire", evidence.clone())
            .expect("mark conflict");
        let retirement = active
            .retire_terminal_operation("op-retire")
            .expect("retire surfaced conflict");
        assert_eq!(
            retirement.operation.status,
            CloudFilesLocalOperationStatus::Superseded
        );
        assert_eq!(retirement.operation.conflict, Some(evidence.clone()));
        assert_eq!(retirement.blobs_pruned, 1);
        drop(active);

        let mut reopened = queue(&root);
        let retired = reopened.operation("op-retire").expect("retired identity");
        assert_eq!(retired.status, CloudFilesLocalOperationStatus::Superseded);
        assert_eq!(retired.conflict, Some(evidence));
        let replacement = reopened
            .stage_content(b"different")
            .expect("replacement stage");
        let error = reopened
            .enqueue(write_request("op-retire", replacement))
            .expect_err("retired stable ID remains reserved");
        assert_eq!(error.code, "operation-id-reused");

        reopened
            .enqueue(directory_request(
                "op-completed",
                "/services/completed".to_string(),
            ))
            .expect("enqueue completed operation");
        reopened
            .mark_completed("op-completed")
            .expect("mark completed");
        let retirement = reopened
            .retire_terminal_operation("op-completed")
            .expect("retire completed operation");
        assert_eq!(
            retirement.operation.status,
            CloudFilesLocalOperationStatus::Superseded
        );
    }

    #[test]
    fn hard_journal_bound_rejects_before_write_and_retains_all_pending_work() {
        let root = TestRoot::new("journal-bound");
        let mut active = queue(&root);
        let mut rejected = None;
        for index in 0..5000 {
            let operation_id = format!("op-bound-{index:05}");
            let request =
                directory_request(&operation_id, format!("/services/directory-{index:05}"));
            let before = fs::read(active.journal_path()).expect("journal before enqueue");
            match active.enqueue(request) {
                Ok(_) => {}
                Err(error) => {
                    assert_eq!(error.code, "queue-journal-too-large");
                    assert_eq!(
                        fs::read(active.journal_path()).expect("journal after rejection"),
                        before
                    );
                    assert!(active.operation(&operation_id).is_none());
                    rejected = Some(active.operations().count());
                    break;
                }
            }
        }
        let retained = rejected.expect("test hard bound must reject new work");
        assert!(retained > 0);
        drop(active);

        let reopened = queue(&root);
        assert_eq!(reopened.operations().count(), retained);
        assert!(
            reopened
                .operations()
                .all(|operation| operation.status == CloudFilesLocalOperationStatus::Queued)
        );
    }
}
